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

import scala.collection.mutable

/**
 * Local identity for one exchange materialization decision.
 *
 * These values fence asynchronous preparation inside one driver. They are deliberately not part
 * of the cross-driver feasibility identity and are never persisted as semantic cache keys.
 */
private[spark] final case class ShuffleRecoveryMaterializationId(
    exchangeId: Long,
    materializationId: Long)

private[spark] final case class ShuffleRecoveryAdoptionTarget(
    materializationId: ShuffleRecoveryMaterializationId,
    targetShuffleId: Int,
    dependencyIdentity: Long,
    mapperCount: Int,
    reducerCount: Int)

private[spark] final case class ShuffleRecoveryFeasibilityInputs(
    sourceToken: String,
    producerTag: String,
    rowEncoding: String,
    partitioningShape: String,
    resolvedLiteral: String) {

  def identityFor(target: ShuffleRecoveryAdoptionTarget): ShuffleRecoveryFeasibilityIdentity = {
    ShuffleRecoveryFeasibilityIdentity.create(
      sourceToken,
      producerTag,
      rowEncoding,
      partitioningShape,
      target.mapperCount,
      target.reducerCount,
      resolvedLiteral)
  }
}

private[spark] final case class ShuffleRecoveryPreparationRequest(
    recoveryGroup: String,
    currentGeneration: Long,
    target: ShuffleRecoveryAdoptionTarget,
    feasibility: ShuffleRecoveryFeasibilityInputs)

/**
 * Single-use local reservation for one asynchronous recovery decision.
 *
 * The monotonically increasing decision version is local fencing state. It is intentionally
 * separate from the durable publishing generation, which only orders cross-driver artifacts.
 */
private[spark] final case class ShuffleRecoveryAdoptionReservation(
    materializationId: ShuffleRecoveryMaterializationId,
    targetShuffleId: Int,
    dependencyIdentity: Long,
    decisionVersion: Long)

private[shuffle] sealed trait ShuffleRecoveryReservationState
private[shuffle] final case class ShuffleRecoveryReservationActive(
    reservation: ShuffleRecoveryAdoptionReservation) extends ShuffleRecoveryReservationState
private[shuffle] final case class ShuffleRecoveryReservationTerminal(
    decisionVersion: Long,
    reason: String) extends ShuffleRecoveryReservationState

/** Thread-safe local fencing for asynchronous adoption preparation. */
private[spark] final class ShuffleRecoveryReservationManager {
  private val states = mutable.HashMap.empty[
    ShuffleRecoveryMaterializationId,
    ShuffleRecoveryReservationState]
  private var nextDecisionVersion = 0L
  private var stopped = false

  def reserve(
      target: ShuffleRecoveryAdoptionTarget): Either[String, ShuffleRecoveryAdoptionReservation] =
    synchronized {
      validateTarget(target) match {
        case Some(reason) => Left(reason)
        case None if stopped => Left("recovery reservation manager is stopped")
        case None =>
          states.get(target.materializationId) match {
            case Some(_: ShuffleRecoveryReservationTerminal) =>
              Left("materialization already has a terminal recovery decision")
            case _ =>
              val version = advanceVersion()
              val reservation = ShuffleRecoveryAdoptionReservation(
                target.materializationId,
                target.targetShuffleId,
                target.dependencyIdentity,
                version)
              states.put(target.materializationId, ShuffleRecoveryReservationActive(reservation))
              Right(reservation)
          }
      }
    }

  def isCurrent(reservation: ShuffleRecoveryAdoptionReservation): Boolean = synchronized {
    !stopped && reservation != null &&
      states.get(reservation.materializationId).contains(
        ShuffleRecoveryReservationActive(reservation))
  }

  /** Consumes the exact current token at most once. */
  def consume(reservation: ShuffleRecoveryAdoptionReservation): Boolean = synchronized {
    if (!isCurrent(reservation)) {
      false
    } else {
      states.put(
        reservation.materializationId,
        ShuffleRecoveryReservationTerminal(
          advanceVersion(),
          "prepared adoption reservation was consumed"))
      true
    }
  }

  /** Drops an unconsumed current reservation after a preparation miss, allowing a later retry. */
  def abandon(reservation: ShuffleRecoveryAdoptionReservation): Unit = synchronized {
    if (isCurrent(reservation)) {
      states.remove(reservation.materializationId)
      advanceVersion()
    }
  }

  def cancel(materializationId: ShuffleRecoveryMaterializationId): Unit = synchronized {
    terminate(materializationId, "materialization was cancelled")
  }

  def ordinaryExecutionWon(materializationId: ShuffleRecoveryMaterializationId): Unit =
    synchronized {
      terminate(materializationId, "ordinary shuffle execution won the decision race")
    }

  def dependencyReplaced(materializationId: ShuffleRecoveryMaterializationId): Unit =
    synchronized {
      terminate(materializationId, "shuffle dependency or stage was replaced")
    }

  def invalidate(materializationId: ShuffleRecoveryMaterializationId): Unit = synchronized {
    terminate(materializationId, "recovery decision was explicitly invalidated")
  }

  def shutdown(): Unit = synchronized {
    if (!stopped) {
      stopped = true
      advanceVersion()
      states.clear()
    }
  }

  private def terminate(
      materializationId: ShuffleRecoveryMaterializationId,
      reason: String): Unit = {
    if (!stopped && materializationId != null) {
      states.put(
        materializationId,
        ShuffleRecoveryReservationTerminal(advanceVersion(), reason))
    }
  }

  private def validateTarget(target: ShuffleRecoveryAdoptionTarget): Option[String] = {
    if (target == null || target.materializationId == null) {
      Some("recovery target must not be null")
    } else if (target.materializationId.exchangeId < 0L ||
        target.materializationId.materializationId < 0L) {
      Some("recovery materialization identifiers must be non-negative")
    } else if (target.targetShuffleId < 0 || target.dependencyIdentity < 0L) {
      Some("recovery target shuffle and dependency identifiers must be non-negative")
    } else if (target.mapperCount < 0 ||
        target.mapperCount > ShuffleRecoveryManifestCodec.MaxMaps) {
      Some("recovery target mapper count is outside the Phase 0 bound")
    } else if (target.reducerCount <= 0 ||
        target.reducerCount > ShuffleRecoveryManifestCodec.MaxReducers) {
      Some("recovery target reducer count is outside the Phase 0 bound")
    } else {
      None
    }
  }

  private def advanceVersion(): Long = {
    try {
      nextDecisionVersion = Math.addExact(nextDecisionVersion, 1L)
      nextDecisionVersion
    } catch {
      case _: ArithmeticException =>
        stopped = true
        throw new IllegalStateException("shuffle recovery decision version overflow")
    }
  }
}

/**
 * Guard for calls that may touch a manifest store, provider, filesystem, or other external state.
 */
private[spark] object ShuffleRecoveryExternalCallGuard {
  private val DAGSchedulerThreadName = "dag-scheduler-event-loop"

  private[spark] def isDagSchedulerEventThread(thread: Thread): Boolean = {
    thread != null && DAGSchedulerThreadName == thread.getName
  }

  def assertAllowed(operation: String): Unit = {
    if (isDagSchedulerEventThread(Thread.currentThread())) {
      throw new IllegalStateException(
        s"$operation must not run on the DAGScheduler event-loop thread")
    }
  }
}

private[shuffle] trait ShuffleRecoveryCandidateLookup {
  def findCompatible(
      recoveryGroup: String,
      identity: ShuffleRecoveryFeasibilityIdentity,
      currentGeneration: Long): Option[ShuffleRecoveryManifest]
}

private[shuffle] final class ShuffleRecoveryManifestCandidateLookup(
    store: ShuffleRecoveryManifestStore) extends ShuffleRecoveryCandidateLookup {

  if (store == null) {
    throw new IllegalArgumentException("manifest store must not be null")
  }

  override def findCompatible(
      recoveryGroup: String,
      identity: ShuffleRecoveryFeasibilityIdentity,
      currentGeneration: Long): Option[ShuffleRecoveryManifest] = {
    store.findCompatible(recoveryGroup, identity, currentGeneration)
  }
}
