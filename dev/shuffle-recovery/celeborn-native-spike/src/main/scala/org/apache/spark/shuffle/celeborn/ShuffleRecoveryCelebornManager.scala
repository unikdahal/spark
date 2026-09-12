/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.celeborn

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark._
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.shuffle._
import org.apache.spark.storage.BlockManagerId

import org.apache.celeborn.client.StandaloneRetainedShuffleReader

/** Celeborn native recovery integration for this Spark branch. */
class ShuffleRecoveryCelebornManager(conf: SparkConf, isDriver: Boolean)
  extends BlockingShuffleManager with ShuffleRecoverySchedulerBackendProvider {
  private[celeborn] val delegate = new SparkShuffleManager(conf, isDriver)
  private val nativeConf = SparkUtils.fromSparkConf(conf)
  private val handles = new ConcurrentHashMap[Int, ShuffleRecoveryCelebornHandle[_, _, _]]()
  private val claims = new ConcurrentHashMap[BlockManagerId, ShuffleRecoveryCelebornLease]()
  private val adoption = if (isDriver) new ShuffleRecoveryNativeAdoption else null
  @volatile private var endpoint: RpcEndpointRef = _
  private var endpointEnv: RpcEnv = _

  override def shuffleRecoverySchedulerBackend: ShuffleRecoverySchedulerBackend = adoption

  private def driverEndpoint(): RpcEndpointRef = synchronized {
    require(isDriver)
    if (endpoint == null) {
      endpointEnv = SparkEnv.get.rpcEnv
      endpoint = endpointEnv.setupEndpoint("CelebornRecoveryBindings", new ThreadSafeRpcEndpoint {
        override val rpcEnv: RpcEnv = endpointEnv
        override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
          case ShuffleRecoveryCelebornBindingCurrent(shuffleId, location) =>
            val handle = handles.get(shuffleId)
            val lease = claims.get(location)
            context.reply(handle != null && lease != null && lease.isCurrent &&
              handle.current.exists(_.location == location) && adoption.isAdopted(shuffleId))
        }
      })
    }
    endpoint
  }

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    delegate.registerShuffle[K, V, C](shuffleId, dependency) match {
      case native: CelebornShuffleHandle[_, _, _] =>
        val handle = new ShuffleRecoveryCelebornHandle(
          native.asInstanceOf[CelebornShuffleHandle[K, V, C]])
        handles.put(shuffleId, handle)
        handle
      case other => other
    }
  }

  /** Register before discovery or provider I/O so ordinary execution can fence the work. */
  private[spark] def registerNativePreparation(
      request: ShuffleRecoveryPreparationRequest,
      manager: ShuffleRecoveryReservationManager,
      reservation: ShuffleRecoveryAdoptionReservation): Boolean = {
    require(isDriver && request != null)
    val handle = handles.get(request.target.targetShuffleId)
    handle != null && adoption.registerReservation(request, manager, reservation,
      handle.underlying.dependency)
  }

  /** Called on the preparation worker after full-identity manifest discovery. */
  private[spark] def offerNative(
      request: ShuffleRecoveryPreparationRequest,
      reservation: ShuffleRecoveryAdoptionReservation,
      manifest: ShuffleRecoveryManifest,
      ttlMillis: Long): Boolean = {
    require(isDriver && request != null && reservation != null &&
      ttlMillis >= 3000L && ttlMillis <= 3600000L)
    val handle = handles.get(request.target.targetShuffleId)
    require(handle != null)
    val dependency = handle.underlying.dependency
    require(dependency.aggregator.isEmpty && dependency.keyOrdering.isEmpty &&
      !dependency.mapSideCombine, "native recovery currently requires an unaggregated row shuffle")
    ShuffleRecoveryManifestCodec.validateManifest(manifest)
    val expected = request.identityInputs.identityFor(request.target)
    val compatibility = ShuffleRecoveryCelebornPublicationProvider.readFormatId(nativeConf)
    require(ShuffleRecoveryManifestCodec.identitiesMatch(manifest.identity, expected) &&
      expected.providerCompatibilityId == compatibility &&
      manifest.recoveryGroup == request.recoveryGroup &&
      manifest.generation < request.currentGeneration && manifest.nativeMapOutputs.isDefined)
    val descriptor = manifest.nativeDescriptor.get
    val native = new StandaloneRetainedShuffleReader(descriptor.toArray, ttlMillis, nativeConf)
    var transferred = false
    var lease: ShuffleRecoveryCelebornLease = null
    try {
      require(native.numMappers == request.target.mapperCount &&
        native.numReducers == request.target.reducerCount)
      lease = new ShuffleRecoveryCelebornLease(native, ttlMillis, () => true)
      val driver = driverEndpoint()
      val address = SparkEnv.get.blockManager.blockManagerId
      val location = BlockManagerId(
        "native-recovery-" + UUID.randomUUID(), address.host, address.port)
      val binding = ShuffleRecoveryCelebornBinding(descriptor, location, driver.address, driver.name,
        native.numMappers, native.numReducers, ttlMillis)
      val ownedLease = lease
      claims.put(location, ownedLease)
      val installation = new ShuffleRecoveryNativeInstallation {
        override val compatibilityId: String = compatibility
        override val descriptor: Vector[Byte] = binding.descriptor
        override val location: BlockManagerId = binding.location
        override def isCurrent: Boolean = ownedLease.isCurrent
        override def install(): Boolean = ownedLease.isCurrent && handle.install(binding)
        override def invalidate(): Unit = {
          handle.invalidate(binding)
          claims.remove(location, ownedLease)
          ownedLease.invalidate()
        }
        override def close(): Unit = {
          invalidate()
          ownedLease.close()
        }
      }
      transferred = true
      adoption.offerPrepared(reservation, manifest, installation)
    } finally {
      if (!transferred) {
        if (lease != null) lease.close() else native.close()
      }
    }
  }

  private[spark] def cancelNativePreparation(
      materialization: ShuffleRecoveryMaterializationId): Unit = {
    require(isDriver)
    adoption.cancel(materialization,
      SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster])
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    val ordinary = handle match {
      case wrapped: ShuffleRecoveryCelebornHandle[_, _, _] =>
        require(wrapped.current.isEmpty, "cannot write through an adopted native handle")
        wrapped.underlying
      case other => other
    }
    delegate.getWriter[K, V](ordinary, mapId, context, metrics)
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    handle match {
      case wrapped: ShuffleRecoveryCelebornHandle[_, _, _] =>
        wrapped.current match {
          case Some(binding) =>
            new ShuffleRecoveryCelebornReader[K, C](
              wrapped.asInstanceOf[ShuffleRecoveryCelebornHandle[K, _, C]], binding,
              startMapIndex, endMapIndex, startPartition, endPartition,
              context, nativeConf, metrics)
          case None => delegate.getReader[K, C](wrapped.underlying, startMapIndex, endMapIndex,
            startPartition, endPartition, context, metrics)
        }
      case other => delegate.getReader[K, C](other, startMapIndex, endMapIndex,
        startPartition, endPartition, context, metrics)
    }
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = delegate.shuffleBlockResolver()

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    if (isDriver) {
      adoption.unregisterShuffle(shuffleId,
        SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster])
    }
    handles.remove(shuffleId)
    delegate.unregisterShuffle(shuffleId)
  }

  override def stop(): Unit = {
    try {
      if (adoption != null) adoption.close()
      if (endpoint != null) endpointEnv.stop(endpoint)
      handles.clear()
    } finally delegate.stop()
  }
}
