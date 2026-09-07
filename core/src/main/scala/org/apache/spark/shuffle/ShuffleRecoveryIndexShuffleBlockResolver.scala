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

package org.apache.spark.shuffle

import java.io.IOException
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import org.apache.spark.{MapOutputTrackerMaster, ShuffleRecoverySchedulerAdoptionState, SparkConf}
import org.apache.spark.SparkEnv
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.storage.{BlockId, ShuffleBlockBatchId, ShuffleBlockId}
import org.apache.spark.util.collection.OpenHashSet

private[spark] final case class ShuffleRecoveryReadMetrics(
    blockReads: Long,
    nonEmptyBlockReads: Long,
    emptyBlockReads: Long,
    bytesRead: Long)

/**
 * Feasibility-only read indirection for an adopted reference-provider shuffle.
 *
 * The scheduler installs only an immutable current-shuffle-id binding. Provider access remains on
 * the ordinary shuffle fetch thread through [[getBlockData]], never on the DAGScheduler event
 * loop. A production provider integration will need an executor-visible compact descriptor rather
 * than this local reference-provider binding.
 */
private[spark] final class ShuffleRecoveryIndexShuffleBlockResolver(
    conf: SparkConf,
    taskIdMapsForShuffle: ConcurrentMap[Int, OpenHashSet[Long]])
  extends IndexShuffleBlockResolver(conf, null, taskIdMapsForShuffle) {

  private final class RecoveredReadBinding(
      val provider: ReferenceShuffleRecoveryClaimProvider,
      val binding: ShuffleRecoveryBinding,
      val mapperCount: Int,
      val reducerCount: Int,
      val localBindingGeneration: Long,
      val maps: Vector[ShuffleRecoveryPreparedMap]) {
    val usable = new AtomicBoolean(true)
  }

  private final class RecoveredReadCounters {
    val blockReads = new AtomicLong(0L)
    val nonEmptyBlockReads = new AtomicLong(0L)
    val emptyBlockReads = new AtomicLong(0L)
    val bytesRead = new AtomicLong(0L)
  }

  private val recoveredBindings = new ConcurrentHashMap[Int, RecoveredReadBinding]()
  private val recoveredReadCounters = new ConcurrentHashMap[Int, RecoveredReadCounters]()
  private val observedFailures =
    new ConcurrentHashMap[Int, ShuffleRecoveryObservedFetchFailure]()

  private[spark] val schedulerAdoption = new ShuffleRecoverySchedulerAdoptionState(this)

  private[spark] def installRecoveredBinding(
      targetShuffleId: Int,
      provider: ReferenceShuffleRecoveryClaimProvider,
      binding: ShuffleRecoveryBinding,
      mapperCount: Int,
      reducerCount: Int,
      localBindingGeneration: Long,
      maps: Vector[ShuffleRecoveryPreparedMap]): Boolean = {
    if (targetShuffleId < 0 || provider == null || binding == null || maps == null ||
        binding.targetShuffleId != targetShuffleId || mapperCount < 0 || reducerCount <= 0 ||
        localBindingGeneration <= 0L || maps.size != mapperCount ||
        maps.indices.exists(index => maps(index) == null || maps(index).mapIndex != index)) {
      false
    } else {
      val candidate = new RecoveredReadBinding(
        provider,
        binding,
        mapperCount,
        reducerCount,
        localBindingGeneration,
        maps)
      val existing = recoveredBindings.putIfAbsent(targetShuffleId, candidate)
      val installed = existing == null ||
        ((existing.provider eq provider) &&
          existing.binding == binding &&
          existing.mapperCount == mapperCount &&
          existing.reducerCount == reducerCount &&
          existing.localBindingGeneration == localBindingGeneration)
      if (installed) {
        observedFailures.remove(targetShuffleId)
        recoveredReadCounters.putIfAbsent(targetShuffleId, new RecoveredReadCounters)
      }
      installed
    }
  }

  private[spark] def invalidateRecoveredBinding(
      targetShuffleId: Int,
      binding: ShuffleRecoveryBinding,
      localBindingGeneration: Long): Boolean = {
    if (binding == null || localBindingGeneration <= 0L) {
      false
    } else {
      val existing = recoveredBindings.get(targetShuffleId)
      if (existing != null && existing.binding == binding &&
          existing.localBindingGeneration == localBindingGeneration) {
        existing.usable.set(false)
        recoveredBindings.remove(targetShuffleId, existing)
      } else {
        false
      }
    }
  }

  private[spark] def removeRecoveredBinding(
      targetShuffleId: Int,
      binding: ShuffleRecoveryBinding): Unit = {
    if (binding != null) {
      val existing = recoveredBindings.get(targetShuffleId)
      if (existing != null && existing.binding == binding &&
          recoveredBindings.remove(targetShuffleId, existing)) {
        existing.usable.set(false)
        recoveredReadCounters.remove(targetShuffleId)
        observedFailures.remove(targetShuffleId)
      }
    }
  }

  private[spark] def observedFetchFailure(
      targetShuffleId: Int): Option[ShuffleRecoveryObservedFetchFailure] =
    Option(observedFailures.get(targetShuffleId))

  private[spark] def clearRecoveryState(targetShuffleId: Int): Unit = {
    observedFailures.remove(targetShuffleId)
    recoveredReadCounters.remove(targetShuffleId)
  }

  private[spark] def isRecovered(targetShuffleId: Int): Boolean =
    recoveredBindings.containsKey(targetShuffleId)

  private[spark] def recoveredBindingCount: Int = recoveredBindings.size()

  private[spark] def recoveredReadMetrics(targetShuffleId: Int): ShuffleRecoveryReadMetrics = {
    val counters = recoveredReadCounters.get(targetShuffleId)
    if (counters == null) {
      ShuffleRecoveryReadMetrics(0L, 0L, 0L, 0L)
    } else {
      ShuffleRecoveryReadMetrics(
        counters.blockReads.get(),
        counters.nonEmptyBlockReads.get(),
        counters.emptyBlockReads.get(),
        counters.bytesRead.get())
    }
  }

  private[spark] def openBoundMapForPreparation(
      provider: ReferenceShuffleRecoveryClaimProvider,
      binding: ShuffleRecoveryBinding,
      mapIndex: Int): ReferenceShuffleResolvedMap = {
    provider.openBoundMap(binding, mapIndex)
  }

  override def getBlockData(
      blockId: BlockId,
      dirs: Option[Array[String]]): ManagedBuffer = {
    blockId match {
      case id: ShuffleBlockId =>
        val recovered = recoveredBindings.get(id.shuffleId)
        if (recovered == null) {
          super.getBlockData(blockId, dirs)
        } else {
          readRecoveredBlock(id, recovered)
        }

      case batch: ShuffleBlockBatchId if recoveredBindings.containsKey(batch.shuffleId) =>
        // The Phase 0 reference binding intentionally disables batch fetch so every provider read
        // retains exact reducer addressing. A scalable batched representation is a later design.
        throw new IOException("batch fetch is disabled for an adopted reference shuffle")

      case _ =>
        super.getBlockData(blockId, dirs)
    }
  }

  private def readRecoveredBlock(
      id: ShuffleBlockId,
      recovered: RecoveredReadBinding): ManagedBuffer = {
    if (!recovered.usable.get()) {
      recordFailure(id, recovered, ShuffleRecoveryAdoptedUnavailable)
      throw new IOException("recovered shuffle binding was invalidated before fetch")
    }
    if (id.mapId < 0L || id.mapId > Int.MaxValue.toLong) {
      recordFailure(id, recovered, ShuffleRecoveryAdoptedUnavailable)
      throw new IOException("recovered shuffle map id is outside the supported range")
    }
    val mapIndex = id.mapId.toInt
    if (mapIndex >= recovered.mapperCount ||
        id.reduceId < 0 || id.reduceId >= recovered.reducerCount) {
      recordFailure(id, recovered, ShuffleRecoveryAdoptedUnavailable)
      throw new IOException("recovered shuffle block coordinates are outside the binding")
    }

    val resolved = recovered.provider.openBoundMapForFetch(
      recovered.binding, mapIndex, recovered.maps(mapIndex)) match {
      case ShuffleRecoveryBoundMapOpened(value) => value
      case ShuffleRecoveryBoundMapFailed(failureClass) =>
        recordFailure(id, recovered, failureClass)
        throw new IOException(s"adopted shuffle provider read failed: $failureClass")
    }
    if (resolved.numReducers != recovered.reducerCount) {
      recordFailure(id, recovered, ShuffleRecoveryAdoptedCorrupt)
      throw new IOException("adopted shuffle reducer shape changed after validation")
    }

    val metadata = try {
      resolved.blockMetadata(id.reduceId)
    } catch {
      case _: IllegalArgumentException =>
        recordFailure(id, recovered, ShuffleRecoveryAdoptedCorrupt)
        throw new IOException("adopted shuffle block metadata is corrupt")
    }
    if (metadata.offset < 0L || metadata.length < 0L ||
        metadata.offset > resolved.dataLength ||
        metadata.length > resolved.dataLength - metadata.offset) {
      recordFailure(id, recovered, ShuffleRecoveryAdoptedCorrupt)
      throw new IOException("adopted shuffle block range is corrupt")
    }

    // Invalidation wins over a read that started earlier. A task that already received a buffer is
    // fenced at scheduler completion; one still inside this resolver is prevented from receiving A.
    if (!recovered.usable.get()) {
      recordFailure(id, recovered, ShuffleRecoveryAdoptedUnavailable)
      throw new IOException("recovered shuffle binding was invalidated during fetch")
    }

    val counters = recoveredReadCounters.get(id.shuffleId)
    if (counters != null) {
      counters.blockReads.incrementAndGet()
      if (metadata.isEmpty) {
        counters.emptyBlockReads.incrementAndGet()
      } else {
        counters.nonEmptyBlockReads.incrementAndGet()
        counters.bytesRead.addAndGet(metadata.length)
      }
    }
    resolved.getBlockData(id.reduceId).getOrElse {
      new NioManagedBuffer(ByteBuffer.allocate(0))
    }
  }

  private def recordFailure(
      id: ShuffleBlockId,
      recovered: RecoveredReadBinding,
      failureClass: ShuffleRecoveryAdoptedReadFailureClass): Unit = {
    val mapIndex = if (id.mapId >= 0L && id.mapId <= Int.MaxValue.toLong) {
      id.mapId.toInt
    } else {
      -1
    }
    val observed = ShuffleRecoveryObservedFetchFailure(
      recovered.localBindingGeneration,
      recovered.binding.bindingId,
      mapIndex,
      id.reduceId,
      failureClass)
    observedFailures.compute(id.shuffleId, (_, previous) => {
      if (previous == null ||
          recovered.localBindingGeneration > previous.localBindingGeneration ||
          (recovered.localBindingGeneration == previous.localBindingGeneration &&
            previous.bindingId == recovered.binding.bindingId &&
            !previous.failureClass.authorizesRetirement && failureClass.authorizesRetirement)) {
        observed
      } else {
        previous
      }
    })

    // The reference resolver runs in the driver's BlockManager for this feasibility path. By
    // invalidating here, the entire adopted tracker registration disappears before the ordinary
    // FetchFailed event can observe or remove one map at a time. Executor-side environments do not
    // have a MapOutputTrackerMaster and therefore cannot mutate driver recovery state.
    Option(SparkEnv.get).foreach { env =>
      env.mapOutputTracker match {
        case tracker: MapOutputTrackerMaster =>
          schedulerAdoption.invalidateObservedFetchFailure(tracker, id.shuffleId)
        case _ =>
      }
    }
  }
}
