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

import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, RejectedExecutionException}
import java.util.concurrent.{ThreadFactory, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle._
import org.apache.spark.storage.BlockManagerId

private[spark] sealed trait ShuffleRecoveryFetchFailureAction
private[spark] case object ShuffleRecoveryFetchFailureNotAdopted
  extends ShuffleRecoveryFetchFailureAction
private[spark] case object ShuffleRecoveryFetchFailureStale
  extends ShuffleRecoveryFetchFailureAction
private[spark] final case class ShuffleRecoveryFetchFailureInvalidated(
    trackerCleared: Boolean,
    failureClass: ShuffleRecoveryAdoptedReadFailureClass)
  extends ShuffleRecoveryFetchFailureAction

/**
 * Per-driver state for the feasibility scheduler-adoption transaction.
 *
 * External recovery work ends in [[offerPrepared]] before the DAGScheduler can see the result.
 * The event-loop transaction itself only checks local fencing state, installs a prebuilt fetch
 * binding, atomically swaps a complete tracker status, records provenance, and bumps the tracker
 * epoch. It never calls a provider, filesystem, future, blocking queue, or other external service.
 */
private[spark] final class ShuffleRecoverySchedulerAdoptionState(
    resolver: ShuffleRecoveryIndexShuffleBlockResolver) extends Logging {

  private sealed trait Decision {
    def manager: ShuffleRecoveryReservationManager
    def reservation: ShuffleRecoveryAdoptionReservation
    def dependency: ShuffleDependency[_, _, _]
    def mapperCount: Int
    def reducerCount: Int
  }

  private final class Preparing(
      val manager: ShuffleRecoveryReservationManager,
      val reservation: ShuffleRecoveryAdoptionReservation,
      val dependency: ShuffleDependency[_, _, _],
      val mapperCount: Int,
      val reducerCount: Int) extends Decision

  private final class Ready(
      val manager: ShuffleRecoveryReservationManager,
      val reservation: ShuffleRecoveryAdoptionReservation,
      val dependency: ShuffleDependency[_, _, _],
      val mapperCount: Int,
      val reducerCount: Int,
      val provider: ReferenceShuffleRecoveryClaimProvider,
      val binding: ShuffleRecoveryBinding,
      val preparedMaps: Vector[ShuffleRecoveryPreparedMap],
      val localBindingGeneration: Long,
      val recoveredLocation: BlockManagerId,
      val incarnation: ShuffleRecoveryManifestIncarnation,
      val retirer: Option[ShuffleRecoveryIncarnationRetirer],
      val statuses: Vector[MapStatus]) extends Decision

  private final class Adopted(
      val dependency: ShuffleDependency[_, _, _],
      val provider: ReferenceShuffleRecoveryClaimProvider,
      val binding: ShuffleRecoveryBinding,
      val mapperCount: Int,
      val reducerCount: Int,
      val localBindingGeneration: Long,
      val recoveredLocation: BlockManagerId,
      val incarnation: ShuffleRecoveryManifestIncarnation,
      val retirer: Option[ShuffleRecoveryIncarnationRetirer],
      val trackerStatus: ShuffleStatus,
      val releaseQueued: AtomicBoolean = new AtomicBoolean(false),
      val retirementQueued: AtomicBoolean = new AtomicBoolean(false))

  private final class Invalidated(
      val provider: ReferenceShuffleRecoveryClaimProvider,
      val binding: ShuffleRecoveryBinding,
      val localBindingGeneration: Long,
      val recoveredLocation: BlockManagerId,
      val invalidatedAtEpoch: Long,
      val incarnation: ShuffleRecoveryManifestIncarnation,
      val retirer: Option[ShuffleRecoveryIncarnationRetirer],
      val releaseQueued: AtomicBoolean,
      val retirementQueued: AtomicBoolean)

  private val pending = new ConcurrentHashMap[Int, Decision]()
  private val adopted = new ConcurrentHashMap[Int, Adopted]()
  private val invalidated = new ConcurrentHashMap[Int, Invalidated]()
  private val nextBindingGeneration = new AtomicLong(0L)
  private val releaseThreadId = new AtomicInteger(0)
  private val releaseExecutor = new ThreadPoolExecutor(
    1,
    1,
    0L,
    TimeUnit.MILLISECONDS,
    new ArrayBlockingQueue[Runnable](64),
    new ThreadFactory {
      override def newThread(runnable: Runnable): Thread = {
        val thread = new Thread(
          runnable,
          s"shuffle-recovery-release-${releaseThreadId.incrementAndGet()}")
        thread.setDaemon(true)
        thread
      }
    },
    new ThreadPoolExecutor.AbortPolicy())

  /**
   * Registers the current reservation before asynchronous provider preparation starts.
   *
   * This call is local-only and gives ordinary task submission a deterministic object to fence if
   * it reaches the scheduler before [[offerPrepared]].
   */
  def registerReservation(
      manager: ShuffleRecoveryReservationManager,
      reservation: ShuffleRecoveryAdoptionReservation,
      dependency: ShuffleDependency[_, _, _],
      mapperCount: Int,
      reducerCount: Int): Either[String, Unit] = {
    if (manager == null || reservation == null || dependency == null) {
      Left("scheduler adoption reservation contains a null field")
    } else if (reservation.targetShuffleId != dependency.shuffleId ||
        mapperCount != dependency.rdd.partitions.length ||
        reducerCount != dependency.partitioner.numPartitions ||
        mapperCount < 0 || reducerCount <= 0) {
      Left("scheduler adoption reservation does not match the current shuffle dependency")
    } else if (!manager.isCurrent(reservation)) {
      Left("scheduler adoption reservation is no longer current")
    } else {
      val state = new Preparing(manager, reservation, dependency, mapperCount, reducerCount)
      val existing = pending.putIfAbsent(reservation.targetShuffleId, state)
      if (existing == null) Right(())
      else Left("target shuffle already has a recovery decision in flight")
    }
  }

  def offerPrepared(
      prepared: PreparedShuffleRecoveryAdoption,
      provider: ReferenceShuffleRecoveryClaimProvider,
      location: BlockManagerId): Either[String, Unit] = {
    offerPrepared(prepared, provider, location, None)
  }

  /**
   * Converts a validated provider binding into fully Spark-owned scheduler input off the
   * event loop.
   *
   * The exact reducer lengths are read here, not during scheduler installation. This deliberately
   * materializes O(M x R) runtime size metadata for the Phase 0 reference path; it is a correctness
   * proof, not a compact production representation.
   */
  def offerPrepared(
      prepared: PreparedShuffleRecoveryAdoption,
      provider: ReferenceShuffleRecoveryClaimProvider,
      location: BlockManagerId,
      retirer: Option[ShuffleRecoveryIncarnationRetirer]): Either[String, Unit] = {
    ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery scheduler preparation")
    if (prepared == null || provider == null || location == null || prepared.reservation == null ||
        prepared.binding == null || prepared.maps == null || retirer == null) {
      return Left("prepared scheduler adoption contains a null field")
    }
    val targetShuffleId = prepared.targetShuffleId
    val initial = pending.get(targetShuffleId)
    initial match {
      case preparing: Preparing
          if preparing.reservation == prepared.reservation &&
            preparing.mapperCount == prepared.mapperCount &&
            preparing.reducerCount == prepared.reducerCount =>
      case _ =>
        safeRelease(provider, prepared.binding)
        return Left("prepared scheduler adoption no longer has a matching in-flight reservation")
    }
    val preparing = initial.asInstanceOf[Preparing]
    if (!preparing.manager.isCurrent(prepared.reservation)) {
      pending.remove(targetShuffleId, preparing)
      safeRelease(provider, prepared.binding)
      return Left("prepared scheduler adoption reservation is stale")
    }

    val localBindingGeneration = try {
      val generation = nextBindingGeneration.incrementAndGet()
      if (generation <= 0L) {
        throw new IllegalStateException("shuffle recovery binding generation overflow")
      }
      generation
    } catch {
      case NonFatal(e) =>
        pending.remove(targetShuffleId, preparing)
        preparing.manager.invalidate(prepared.reservation.materializationId)
        safeRelease(provider, prepared.binding)
        return Left(e.getMessage)
    }
    val recoveredLocation = BlockManagerId(
      s"shuffle-recovery-$localBindingGeneration",
      location.host,
      location.port,
      location.topologyInfo)
    val incarnation = ShuffleRecoveryManifestIncarnation(
      prepared.recoveryGroup,
      prepared.publishingGeneration,
      prepared.incarnationId,
      prepared.feasibilityIdentityDigest)

    val statuses = try {
      buildStatuses(prepared, provider, recoveredLocation)
    } catch {
      case NonFatal(e) =>
        pending.remove(targetShuffleId, preparing)
        preparing.manager.invalidate(prepared.reservation.materializationId)
        safeRelease(provider, prepared.binding)
        return Left(s"unable to build recovered map status: ${e.getMessage}")
    }
    val ready = new Ready(
      preparing.manager,
      preparing.reservation,
      preparing.dependency,
      preparing.mapperCount,
      preparing.reducerCount,
      provider,
      prepared.binding,
      prepared.maps,
      localBindingGeneration,
      recoveredLocation,
      incarnation,
      retirer,
      statuses)
    if (!preparing.manager.isCurrent(prepared.reservation) ||
        !pending.replace(targetShuffleId, preparing, ready)) {
      pending.remove(targetShuffleId, ready)
      safeRelease(provider, prepared.binding)
      Left("ordinary execution or a newer recovery decision won during preparation")
    } else if (!preparing.manager.isCurrent(prepared.reservation)) {
      pending.remove(targetShuffleId, ready)
      safeRelease(provider, prepared.binding)
      Left("prepared scheduler adoption became stale before publication")
    } else {
      Right(())
    }
  }

  /** Called only from the scheduler's existing missing-partition path. */
  def beforeFindMissingPartitions(
      tracker: MapOutputTrackerMaster,
      dependency: ShuffleDependency[_, _, _],
      numPartitions: Int): Boolean = {
    if (tracker == null || dependency == null || numPartitions < 0) {
      return false
    }
    val shuffleId = dependency.shuffleId
    val already = adopted.get(shuffleId)
    if (already != null) {
      return already.dependency eq dependency
    }

    pending.get(shuffleId) match {
      case null => false
      case preparing: Preparing =>
        if (preparing.dependency ne dependency) {
          preparing.manager.dependencyReplaced(preparing.reservation.materializationId)
        } else {
          preparing.manager.ordinaryExecutionWon(preparing.reservation.materializationId)
        }
        pending.remove(shuffleId, preparing)
        false

      case ready: Ready =>
        if (ready.dependency ne dependency) {
          ready.manager.dependencyReplaced(ready.reservation.materializationId)
          if (pending.remove(shuffleId, ready)) {
            releaseAsync(ready.provider, ready.binding, new AtomicBoolean(false))
          }
          false
        } else if (ready.mapperCount != numPartitions ||
            ready.mapperCount != dependency.rdd.partitions.length ||
            ready.reducerCount != dependency.partitioner.numPartitions ||
            ready.reservation.targetShuffleId != shuffleId) {
          ready.manager.invalidate(ready.reservation.materializationId)
          if (pending.remove(shuffleId, ready)) {
            releaseAsync(ready.provider, ready.binding, new AtomicBoolean(false))
          }
          false
        } else {
          commitReady(tracker, ready)
        }
    }
  }

  /**
   * Invalidates an adopted binding without performing external recovery I/O on the scheduler.
   *
   * The binding becomes unusable before the tracker swap. The swap replaces the complete adopted
   * status object with an empty status, so no fresh map completion can be accepted alongside A.
   */
  def handleFetchFailure(
      tracker: MapOutputTrackerMaster,
      dependency: ShuffleDependency[_, _, _],
      blockManagerId: BlockManagerId,
      taskEpoch: Long): ShuffleRecoveryFetchFailureAction = {
    if (tracker == null || dependency == null || taskEpoch < 0L) {
      return ShuffleRecoveryFetchFailureNotAdopted
    }
    val shuffleId = dependency.shuffleId
    val current = adopted.get(shuffleId)
    if (current == null) {
      val old = invalidated.get(shuffleId)
      if (old == null) {
        return ShuffleRecoveryFetchFailureNotAdopted
      }
      maybeQueueLateRetirement(shuffleId, old)
      if (blockManagerId == old.recoveredLocation || taskEpoch <= old.invalidatedAtEpoch) {
        ShuffleRecoveryFetchFailureStale
      } else {
        ShuffleRecoveryFetchFailureNotAdopted
      }
    } else if (current.dependency ne dependency) {
      ShuffleRecoveryFetchFailureNotAdopted
    } else if (!adopted.remove(shuffleId, current)) {
      handleFetchFailure(tracker, dependency, blockManagerId, taskEpoch)
    } else {
      val observed = matchingObservedFailure(shuffleId, current)
      val failureClass = observed.map(_.failureClass)
        .getOrElse(ShuffleRecoveryAdoptedUnavailable)

      // Make provider reads fail before changing tracker visibility. No provider/store call occurs.
      resolver.invalidateRecoveredBinding(
        shuffleId, current.binding, current.localBindingGeneration)

      val replacement = new ShuffleStatus(current.mapperCount, current.reducerCount)
      val trackerCurrent = tracker.shuffleStatuses.get(shuffleId).orNull
      val trackerCleared = if (trackerCurrent eq current.trackerStatus) {
        if (tracker.shuffleStatuses.replace(shuffleId, current.trackerStatus, replacement)) {
          current.trackerStatus.invalidateSerializedMapOutputStatusCache()
          current.trackerStatus.invalidateSerializedMergeOutputStatusCache()
          tracker.incrementEpoch()
          true
        } else {
          false
        }
      } else {
        trackerCurrent != null && trackerCurrent.numAvailableMapOutputs == 0
      }
      val invalidatedAtEpoch = tracker.getEpoch
      val old = new Invalidated(
        current.provider,
        current.binding,
        current.localBindingGeneration,
        current.recoveredLocation,
        invalidatedAtEpoch,
        current.incarnation,
        current.retirer,
        current.releaseQueued,
        current.retirementQueued)
      invalidated.put(shuffleId, old)
      releaseAsync(current.provider, current.binding, current.releaseQueued)
      if (failureClass.authorizesRetirement) {
        retireAsync(current.incarnation, current.retirer, current.retirementQueued)
      }
      ShuffleRecoveryFetchFailureInvalidated(trackerCleared, failureClass)
    }
  }

  def isAdopted(shuffleId: Int): Boolean = adopted.containsKey(shuffleId)

  private[spark] def invalidatedBindingCount: Int = invalidated.size()

  def cancel(materializationId: ShuffleRecoveryMaterializationId): Unit = {
    if (materializationId != null) {
      val iterator = pending.entrySet().iterator()
      while (iterator.hasNext) {
        val entry = iterator.next()
        val decision = entry.getValue
        if (decision.reservation.materializationId == materializationId) {
          decision.manager.cancel(materializationId)
          if (pending.remove(entry.getKey, decision)) {
            decision match {
              case ready: Ready =>
                releaseAsync(ready.provider, ready.binding, new AtomicBoolean(false))
              case _ =>
            }
          }
        }
      }
    }
  }

  /**
   * Invalidates all local recovery decisions and releases aliases without touching healthy data.
   */
  def close(): Unit = {
    val managers = new java.util.HashSet[ShuffleRecoveryReservationManager]()
    val pendingIterator = pending.entrySet().iterator()
    while (pendingIterator.hasNext) {
      val entry = pendingIterator.next()
      val decision = entry.getValue
      managers.add(decision.manager)
      if (pending.remove(entry.getKey, decision)) {
        decision match {
          case ready: Ready =>
            releaseAsync(ready.provider, ready.binding, new AtomicBoolean(false))
          case _ =>
        }
      }
    }
    val adoptedIterator = adopted.entrySet().iterator()
    while (adoptedIterator.hasNext) {
      val entry = adoptedIterator.next()
      val value = entry.getValue
      if (adopted.remove(entry.getKey, value)) {
        resolver.removeRecoveredBinding(entry.getKey, value.binding)
        releaseAsync(value.provider, value.binding, value.releaseQueued)
      }
    }
    val invalidatedIterator = invalidated.entrySet().iterator()
    while (invalidatedIterator.hasNext) {
      val entry = invalidatedIterator.next()
      val value = entry.getValue
      maybeQueueLateRetirement(entry.getKey, value)
      releaseAsync(value.provider, value.binding, value.releaseQueued)
      invalidated.remove(entry.getKey, value)
      resolver.clearObservedFetchFailure(entry.getKey)
    }
    val managerIterator = managers.iterator()
    while (managerIterator.hasNext) {
      managerIterator.next().shutdown()
    }
    releaseExecutor.shutdown()
  }

  private def commitReady(tracker: MapOutputTrackerMaster, ready: Ready): Boolean = {
    val shuffleId = ready.dependency.shuffleId
    val currentStatus = tracker.shuffleStatuses.get(shuffleId).orNull
    if (currentStatus == null || currentStatus.numAvailableMapOutputs != 0) {
      ready.manager.ordinaryExecutionWon(ready.reservation.materializationId)
      if (pending.remove(shuffleId, ready)) {
        releaseAsync(ready.provider, ready.binding, new AtomicBoolean(false))
      }
      return false
    }

    // Build the complete replacement before taking the reservation fence. It is not published in
    // tracker state until the single CAS below succeeds.
    val replacement = new ShuffleStatus(ready.mapperCount, ready.reducerCount)
    var mapIndex = 0
    while (mapIndex < ready.statuses.size) {
      replacement.addMapOutput(mapIndex, ready.statuses(mapIndex))
      mapIndex += 1
    }

    val committed = try {
      ready.manager.consumeIfCurrent(ready.reservation) {
        val stillCurrent = pending.get(shuffleId) eq ready
        val trackerCurrent = tracker.shuffleStatuses.get(shuffleId).orNull
        if (!stillCurrent || (trackerCurrent ne currentStatus) ||
            trackerCurrent.numAvailableMapOutputs != 0 ||
            adopted.containsKey(shuffleId)) {
          false
        } else if (!resolver.installRecoveredBinding(
            shuffleId,
            ready.provider,
            ready.binding,
            ready.mapperCount,
            ready.reducerCount,
            ready.localBindingGeneration,
            ready.preparedMaps)) {
          false
        } else {
          val provenance = new Adopted(
            ready.dependency,
            ready.provider,
            ready.binding,
            ready.mapperCount,
            ready.reducerCount,
            ready.localBindingGeneration,
            ready.recoveredLocation,
            ready.incarnation,
            ready.retirer,
            replacement)
          if (adopted.putIfAbsent(shuffleId, provenance) != null) {
            resolver.removeRecoveredBinding(shuffleId, ready.binding)
            false
          } else if (!tracker.shuffleStatuses.replace(shuffleId, currentStatus, replacement)) {
            adopted.remove(shuffleId, provenance)
            resolver.removeRecoveredBinding(shuffleId, ready.binding)
            false
          } else {
            invalidated.remove(shuffleId)
            tracker.incrementEpoch()
            true
          }
        }
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"Recovered shuffle $shuffleId local installation failed", e)
        adopted.containsKey(shuffleId) &&
          (tracker.shuffleStatuses.get(shuffleId).orNull eq replacement)
    }

    pending.remove(shuffleId, ready)
    if (!committed) {
      resolver.removeRecoveredBinding(shuffleId, ready.binding)
      adopted.remove(shuffleId)
      releaseAsync(ready.provider, ready.binding, new AtomicBoolean(false))
    }
    committed
  }

  private def matchingObservedFailure(
      shuffleId: Int,
      current: Adopted): Option[ShuffleRecoveryObservedFetchFailure] = {
    resolver.observedFetchFailure(shuffleId).filter { observed =>
      observed.localBindingGeneration == current.localBindingGeneration &&
        observed.bindingId == current.binding.bindingId
    }
  }

  private def maybeQueueLateRetirement(shuffleId: Int, old: Invalidated): Unit = {
    resolver.observedFetchFailure(shuffleId).foreach { observed =>
      if (observed.localBindingGeneration == old.localBindingGeneration &&
          observed.bindingId == old.binding.bindingId &&
          observed.failureClass.authorizesRetirement) {
        retireAsync(old.incarnation, old.retirer, old.retirementQueued)
      }
    }
  }

  private def buildStatuses(
      prepared: PreparedShuffleRecoveryAdoption,
      provider: ReferenceShuffleRecoveryClaimProvider,
      location: BlockManagerId): Vector[MapStatus] = {
    if (prepared.maps.size != prepared.mapperCount) {
      throw new IllegalArgumentException("prepared adoption map count is inconsistent")
    }
    Vector.tabulate(prepared.mapperCount) { mapIndex =>
      val descriptor = prepared.maps(mapIndex)
      if (descriptor == null || descriptor.mapIndex != mapIndex) {
        throw new IllegalArgumentException("prepared adoption maps are not complete and ordered")
      }
      val resolved =
        resolver.openBoundMapForPreparation(provider, prepared.binding, mapIndex)
      if (resolved.numReducers != prepared.reducerCount ||
          resolved.dataLength != descriptor.dataLength ||
          resolved.indexBytes != descriptor.indexLength) {
        throw new IllegalArgumentException("bound provider map changed after validation")
      }
      val lengths = new Array[Long](prepared.reducerCount)
      var emptyBlocks = 0
      var nonEmptyBlocks = 0
      var physicalBytes = 0L
      var reduceId = 0
      while (reduceId < prepared.reducerCount) {
        val block = resolved.blockMetadata(reduceId)
        if (block.length < 0L || block.offset < 0L) {
          throw new IllegalArgumentException("bound provider contains invalid block metadata")
        }
        lengths(reduceId) = block.length
        if (block.length == 0L) emptyBlocks = Math.addExact(emptyBlocks, 1)
        else nonEmptyBlocks = Math.addExact(nonEmptyBlocks, 1)
        physicalBytes = Math.addExact(physicalBytes, block.length)
        reduceId += 1
      }
      if (emptyBlocks != descriptor.emptyBlockCount ||
          nonEmptyBlocks != descriptor.nonEmptyBlockCount ||
          physicalBytes != descriptor.physicalBlockBytes ||
          physicalBytes != descriptor.dataLength) {
        throw new IllegalArgumentException("bound provider block metadata changed after validation")
      }
      MapStatus(location, lengths, mapIndex.toLong)
    }
  }

  private def safeRelease(
      provider: ReferenceShuffleRecoveryClaimProvider,
      binding: ShuffleRecoveryBinding): Unit = {
    try {
      provider.release(binding)
    } catch {
      case NonFatal(e) =>
        logWarning("Unable to release shuffle recovery binding", e)
    }
  }

  private def safeRetire(
      incarnation: ShuffleRecoveryManifestIncarnation,
      retirer: ShuffleRecoveryIncarnationRetirer): Unit = {
    try {
      retirer.retireExact(incarnation) match {
        case ShuffleRecoveryIncarnationRetirementUnavailable =>
          logWarning("Unable to publish shuffle recovery incarnation retirement")
        case _ =>
      }
    } catch {
      case NonFatal(e) =>
        logWarning("Unable to retire dead shuffle recovery incarnation", e)
    }
  }

  private def releaseAsync(
      provider: ReferenceShuffleRecoveryClaimProvider,
      binding: ShuffleRecoveryBinding,
      queued: AtomicBoolean): Unit = {
    if (queued.compareAndSet(false, true)) {
      try {
        releaseExecutor.execute(new Runnable {
          override def run(): Unit = safeRelease(provider, binding)
        })
      } catch {
        case _: RejectedExecutionException =>
          queued.set(false)
          logWarning("Shuffle recovery release queue is unavailable; unbind remains pending")
      }
    }
  }

  private def retireAsync(
      incarnation: ShuffleRecoveryManifestIncarnation,
      retirer: Option[ShuffleRecoveryIncarnationRetirer],
      queued: AtomicBoolean): Unit = {
    retirer.foreach { exactRetirer =>
      if (queued.compareAndSet(false, true)) {
        try {
          releaseExecutor.execute(new Runnable {
            override def run(): Unit = safeRetire(incarnation, exactRetirer)
          })
        } catch {
          case _: RejectedExecutionException =>
            queued.set(false)
            logWarning("Shuffle recovery retirement queue is unavailable; retirement remains pending")
        }
      }
    }
  }
}

private[spark] object ShuffleRecoverySchedulerAdoption {
  private def state: Option[ShuffleRecoverySchedulerAdoptionState] = {
    Option(SparkEnv.get).flatMap { env =>
      env.shuffleManager match {
        case manager: BlockingShuffleManager =>
          manager.shuffleBlockResolver match {
            case resolver: ShuffleRecoveryIndexShuffleBlockResolver =>
              Some(resolver.schedulerAdoption)
            case _ => None
          }
        case _ => None
      }
    }
  }

  def beforeFindMissingPartitions(
      tracker: MapOutputTrackerMaster,
      dependency: ShuffleDependency[_, _, _],
      numPartitions: Int): Boolean = {
    state.exists(_.beforeFindMissingPartitions(tracker, dependency, numPartitions))
  }

  def handleFetchFailure(
      tracker: MapOutputTrackerMaster,
      dependency: ShuffleDependency[_, _, _],
      blockManagerId: BlockManagerId,
      taskEpoch: Long): ShuffleRecoveryFetchFailureAction = {
    state.map(_.handleFetchFailure(tracker, dependency, blockManagerId, taskEpoch))
      .getOrElse(ShuffleRecoveryFetchFailureNotAdopted)
  }

  def currentState: Option[ShuffleRecoverySchedulerAdoptionState] = state

  def isAdopted(shuffleId: Int): Boolean = state.exists(_.isAdopted(shuffleId))
}
