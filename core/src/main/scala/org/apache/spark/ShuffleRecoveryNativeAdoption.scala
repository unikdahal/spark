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

package org.apache.spark

import java.util.concurrent.{ArrayBlockingQueue, RejectedExecutionException, ThreadFactory}
import java.util.concurrent.{ThreadPoolExecutor, TimeUnit}

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.ShuffleRecoveryNativeMapStatus
import org.apache.spark.shuffle._
import org.apache.spark.storage.BlockManagerId

/**
 * A prepared native claim. Metadata, liveness, install and invalidate are local operations.
 * Only close may perform provider I/O. Install atomically publishes a task-serializable descriptor
 * through the current shuffle handle. Invalidate must be idempotent, fence that exact installation,
 * and stop renewal scheduling locally before fresh execution or deferred resource cleanup.
 */
private[spark] trait ShuffleRecoveryNativeInstallation extends AutoCloseable {
  def compatibilityId: String
  def descriptor: Vector[Byte]
  def location: BlockManagerId
  def isCurrent: Boolean
  def install(): Boolean
  def invalidate(): Unit
}

/** Native adoption uses real scheduling estimates and an independently prepared reader binding. */
private[spark] final class ShuffleRecoveryNativeAdoption
  extends ShuffleRecoverySchedulerBackend with AutoCloseable with Logging {
  private final class Pending(
      val request: ShuffleRecoveryPreparationRequest,
      val manager: ShuffleRecoveryReservationManager,
      val reservation: ShuffleRecoveryAdoptionReservation,
      val dependency: ShuffleDependency[_, _, _]) {
    var ready: Option[Ready] = None
  }
  private case class Ready(
      installation: ShuffleRecoveryNativeInstallation,
      status: ShuffleStatus,
      location: BlockManagerId)
  private case class Adopted(pending: Pending, ready: Ready)
  private case class Invalidated(location: BlockManagerId, epoch: Long, var retry: Boolean)

  private val lock = new Object
  private val pending = mutable.HashMap.empty[Int, Pending]
  private val adopted = mutable.HashMap.empty[Int, Adopted]
  private val invalidated = mutable.HashMap.empty[Int, Invalidated]
  private val usedLocations = mutable.HashSet.empty[BlockManagerId]
  private var stopped = false
  private val cleanup = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
    new ArrayBlockingQueue[Runnable](64), new ThreadFactory {
      override def newThread(task: Runnable): Thread = {
        val thread = new Thread(task, "shuffle-recovery-native-release")
        thread.setDaemon(true)
        thread
      }
    }, new ThreadPoolExecutor.AbortPolicy())

  def registerReservation(
      request: ShuffleRecoveryPreparationRequest,
      manager: ShuffleRecoveryReservationManager,
      reservation: ShuffleRecoveryAdoptionReservation,
      dependency: ShuffleDependency[_, _, _]): Boolean = {
    require(request != null && request.target != null && manager != null &&
      reservation != null && dependency != null)
    val target = request.target
    require(target.targetShuffleId == dependency.shuffleId &&
      target.mapperCount == dependency.rdd.partitions.length &&
      target.reducerCount == dependency.partitioner.numPartitions &&
      reservation.targetShuffleId == target.targetShuffleId &&
      reservation.dependencyIdentity == target.dependencyIdentity &&
      reservation.materializationId == target.materializationId)
    lock.synchronized {
      if (stopped || pending.size + adopted.size + invalidated.size >= 256 ||
          pending.contains(dependency.shuffleId) || adopted.contains(dependency.shuffleId) ||
          !manager.isCurrent(reservation)) {
        false
      } else {
        pending.put(dependency.shuffleId, new Pending(request, manager, reservation, dependency))
        true
      }
    }
  }

  /** Ownership of installation transfers on entry, including rejected or stale offers. */
  def offerPrepared(
      reservation: ShuffleRecoveryAdoptionReservation,
      manifest: ShuffleRecoveryManifest,
      installation: ShuffleRecoveryNativeInstallation): Boolean = {
    require(reservation != null && installation != null)
    var accepted = false
    try {
      val selected = lock.synchronized { pending.get(reservation.targetShuffleId) }
      selected.exists { current =>
        require(current.reservation == reservation)
        ShuffleRecoveryManifestCodec.validateManifest(manifest)
        val request = current.request
        val expected = request.identityInputs.identityFor(request.target)
        val location = installation.location
        require(manifest.recoveryGroup == request.recoveryGroup &&
          manifest.generation < request.currentGeneration &&
          ShuffleRecoveryManifestCodec.identitiesMatch(manifest.identity, expected) &&
          manifest.identity.providerCompatibilityId == installation.compatibilityId &&
          manifest.nativeDescriptor.contains(installation.descriptor) &&
          manifest.nativeMapOutputs.isDefined && location != null &&
          manifest.mapperCount == request.target.mapperCount &&
          manifest.reducerCount == request.target.reducerCount)
        val replacement = new ShuffleStatus(manifest.mapperCount, manifest.reducerCount)
        manifest.nativeMapOutputs.get.zipWithIndex.foreach { case (output, index) =>
          replacement.addMapOutput(index, new ShuffleRecoveryNativeMapStatus(
            location, output.reducerBytes, output.mapTaskId))
        }
        lock.synchronized {
          accepted = !stopped && pending.get(reservation.targetShuffleId).contains(current) &&
            current.ready.isEmpty && current.manager.isCurrent(reservation) &&
            installation.isCurrent && usedLocations.size < 256 && !usedLocations.contains(location)
          if (accepted) {
            usedLocations.add(location)
            current.ready = Some(Ready(installation, replacement, location))
          }
          accepted
        }
      }
    } catch {
      case NonFatal(error) =>
        logWarning("Rejecting native shuffle recovery preparation", error)
        false
    } finally {
      if (!accepted) release(installation)
    }
  }

  override def beforeFindMissingPartitions(
      tracker: MapOutputTrackerMaster,
      dependency: ShuffleDependency[_, _, _],
      numPartitions: Int): Boolean = lock.synchronized {
    if (stopped || tracker == null || dependency == null) return false
    val shuffleId = dependency.shuffleId
    adopted.get(shuffleId) match {
      case Some(value) => return value.pending.dependency eq dependency
      case _ =>
    }
    pending.remove(shuffleId) match {
      case None => false
      case Some(value) =>
        var committed = false
        try {
          value.ready.foreach { ready =>
            val original = tracker.shuffleStatuses.get(shuffleId).orNull
            if ((value.dependency eq dependency) &&
                numPartitions == value.request.target.mapperCount && original != null &&
                original.numAvailableMapOutputs == 0 && ready.installation.isCurrent) {
              committed = value.manager.consumeIfCurrent(value.reservation) {
                if (!ready.installation.isCurrent || !ready.installation.install()) false
                else if (!tracker.shuffleStatuses.replace(shuffleId, original, ready.status)) false
                else {
                  adopted.put(shuffleId, Adopted(value, ready))
                  invalidated.remove(shuffleId)
                  tracker.incrementEpoch()
                  true
                }
              }
            }
          }
          committed
        } catch {
          case NonFatal(error) =>
            committed = adopted.get(shuffleId).exists { current =>
              (current.pending eq value) &&
                (tracker.shuffleStatuses.get(shuffleId).orNull eq current.ready.status)
            }
            logWarning("Native recovery local installation failed", error)
            committed
        } finally {
          if (!committed) {
            value.manager.ordinaryExecutionWon(value.reservation.materializationId)
            value.ready.foreach { ready =>
              ready.installation.invalidate()
              release(ready.installation)
            }
          }
        }
    }
  }

  override def handleFetchFailure(
      tracker: MapOutputTrackerMaster,
      dependency: ShuffleDependency[_, _, _],
      blockManagerId: BlockManagerId,
      taskEpoch: Long): ShuffleRecoveryFetchFailureAction = lock.synchronized {
    if (tracker == null || dependency == null || taskEpoch < 0L) {
      return ShuffleRecoveryFetchFailureNotAdopted
    }
    val shuffleId = dependency.shuffleId
    adopted.get(shuffleId) match {
      case Some(current) if current.pending.dependency ne dependency =>
        ShuffleRecoveryFetchFailureNotAdopted
      case Some(current) if current.ready.location != blockManagerId =>
        ShuffleRecoveryFetchFailureStale
      case Some(current) =>
        val binding = current.ready.installation
        binding.invalidate()
        adopted.remove(shuffleId)
        val target = current.pending.request.target
        val replacement = new ShuffleStatus(target.mapperCount, target.reducerCount)
        val cleared = tracker.shuffleStatuses.replace(shuffleId, current.ready.status, replacement)
        release(binding)
        if (!cleared) {
          invalidated.put(shuffleId,
            Invalidated(current.ready.location, tracker.getEpoch, retry = false))
          ShuffleRecoveryFetchFailureStale
        } else {
          current.ready.status.invalidateSerializedMapOutputStatusCache()
          current.ready.status.invalidateSerializedMergeOutputStatusCache()
          tracker.incrementEpoch()
          invalidated.put(shuffleId,
            Invalidated(current.ready.location, tracker.getEpoch, retry = true))
          ShuffleRecoveryFetchFailureInvalidated(true, ShuffleRecoveryAdoptedUnavailable)
        }
      case None =>
        invalidated.get(shuffleId) match {
          case Some(old) if old.location == blockManagerId || taskEpoch <= old.epoch =>
            ShuffleRecoveryFetchFailureStale
          case _ => ShuffleRecoveryFetchFailureNotAdopted
        }
    }
  }

  override def consumeWholeStageRetryRequirement(shuffleId: Int): Boolean = lock.synchronized {
    invalidated.get(shuffleId).exists { value =>
      val required = value.retry
      value.retry = false
      required
    }
  }

  override def isAdopted(shuffleId: Int): Boolean = lock.synchronized {
    adopted.contains(shuffleId)
  }

  def unregisterShuffle(shuffleId: Int, tracker: MapOutputTrackerMaster): Unit = lock.synchronized {
    pending.get(shuffleId).map(_.reservation.materializationId)
      .orElse(adopted.get(shuffleId).map(_.pending.reservation.materializationId))
      .foreach(cancel(_, tracker))
  }

  def cancel(
      materialization: ShuffleRecoveryMaterializationId,
      tracker: MapOutputTrackerMaster): Unit = lock.synchronized {
    pending.toVector.foreach { case (shuffleId, value) =>
      if (value.reservation.materializationId == materialization) {
        pending.remove(shuffleId)
        value.manager.cancel(materialization)
        value.ready.foreach(ready => release(ready.installation))
      }
    }
    adopted.values.toVector.foreach { value =>
      if (value.pending.reservation.materializationId == materialization) {
        require(tracker != null)
        handleFetchFailure(
          tracker, value.pending.dependency, value.ready.location, tracker.getEpoch)
      }
    }
  }

  override def close(): Unit = lock.synchronized {
    if (!stopped) {
      stopped = true
      pending.values.foreach { value =>
        value.manager.cancel(value.reservation.materializationId)
        value.ready.foreach(ready => release(ready.installation))
      }
      adopted.values.foreach { value =>
        value.ready.installation.invalidate()
        release(value.ready.installation)
      }
      pending.clear()
      adopted.clear()
      invalidated.clear()
      usedLocations.clear()
      cleanup.shutdown()
    }
  }

  private def release(installation: ShuffleRecoveryNativeInstallation): Unit = {
    installation.invalidate()
    try cleanup.execute(new Runnable {
      override def run(): Unit = {
        try installation.close() catch {
          case NonFatal(error) => logWarning("Native recovery claim release failed", error)
        }
      }
    }) catch {
      case _: RejectedExecutionException =>
        logWarning("Native recovery cleanup queue is full or stopped; claim must expire by lease")
    }
  }
}
