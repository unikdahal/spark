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

package org.apache.spark.sql.execution.exchange

import java.util.Properties

import scala.collection.mutable

import ShuffleRecoveryAccountingReason._

import org.apache.spark.Success
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerEvent,
  SparkListenerStageCompleted,
  SparkListenerStageSubmitted,
  SparkListenerTaskEnd,
  StageInfo}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

/** Runtime weight for the map outputs that constitute one completed physical shuffle. */
private[sql] case class ShuffleRecoveryStageRuntime(
    executionId: Long,
    stageId: Int,
    stageAttemptId: Int,
    shuffleId: Int,
    expectedMapTasks: Int,
    successfulMapTaskWinners: Int,
    shuffleWriteBytes: Long,
    executorRunTimeMs: Long,
    accumulatorIds: Set[Long],
    completionOrder: Long,
    complete: Boolean,
    invalidReason: Option[String],
    rddScopeIds: Set[String] = Set.empty,
    observedSuccessfulMapTaskCompletions: Long = 0L) {

  require(expectedMapTasks >= 0, "expected map task count must be non-negative")
  require(successfulMapTaskWinners >= 0, "successful winner count must be non-negative")
  require(shuffleWriteBytes >= 0L, "shuffle-write bytes must be non-negative")
  require(executorRunTimeMs >= 0L, "executor run time must be non-negative")
  require(rddScopeIds.forall(_.nonEmpty), "RDD scope IDs must be non-empty")
  require(
    observedSuccessfulMapTaskCompletions == 0L ||
      observedSuccessfulMapTaskCompletions >= successfulMapTaskWinners.toLong,
    "known observed successful map-task completions cannot be below accepted winner count")
}

/**
 * Accumulates one successful output candidate per logical map partition across stage retries.
 *
 * `StageInfo.numTasks` can be only the partitions submitted for one retry, so total RDD partition
 * count is supplied separately as the coverage denominator. Spark may recreate a previously
 * finished shuffle-map stage after output loss, so an attempt is identified by both stage ID and
 * attempt ID and ordered by listener submission order. A later submitted attempt replaces an
 * earlier candidate for the same map partition. Within one attempt, the latest successful
 * completion replaces an earlier duplicate, matching MapOutputTracker's map-output registration
 * semantics. The observed-success counter is deliberately separate from accepted winners so retry
 * and speculation reconciliation remains visible in evidence rather than being silently collapsed.
 */
private[exchange] final class ShuffleRecoveryStageAccumulator(
    val executionId: Long,
    val stageId: Int,
    val shuffleId: Int,
    val expectedMapTasks: Int) {

  private case class Winner(
      attemptOrder: Long,
      bytes: Long,
      executorRunTimeMs: Long)

  require(expectedMapTasks >= 0, "expected map task count must be non-negative")

  private val submittedAttempts = mutable.HashMap.empty[(Int, Int), Long]
  private val winners = mutable.HashMap.empty[Int, Winner]
  private val accumulatorIds = mutable.HashSet.empty[Long]
  private val rddScopeIds = mutable.HashSet.empty[String]
  private var observedSuccessfulMapTaskCompletions = 0L
  private var invalidReason: Option[String] = None

  def startAttempt(stageAttemptId: Int): Unit = {
    startAttempt(stageId, stageAttemptId, stageAttemptId.toLong)
  }

  def startAttempt(
      currentStageId: Int,
      stageAttemptId: Int,
      attemptOrder: Long): Unit = synchronized {
    if (stageAttemptId < 0) {
      invalidate(NegativeStageAttempt)
    } else {
      require(attemptOrder >= 0L, "attempt order must be non-negative")
      submittedAttempts.getOrElseUpdate((currentStageId, stageAttemptId), attemptOrder)
    }
  }

  def invalidate(reason: String): Unit = synchronized {
    if (invalidReason.isEmpty) {
      invalidReason = Some(reason)
    }
  }

  def recordAccumulatorIds(ids: Iterable[Long]): Unit = synchronized {
    accumulatorIds ++= ids
  }

  def recordRddScopeIds(ids: Iterable[String]): Unit = synchronized {
    ids.foreach { id =>
      require(id.nonEmpty, "RDD scope ID must be non-empty")
      rddScopeIds += id
    }
  }

  def recordSuccessfulTask(
      stageAttemptId: Int,
      mapPartitionId: Int,
      shuffleWriteBytes: Long,
      executorRunTimeMs: Long): Unit = {
    recordSuccessfulTask(
      stageId,
      stageAttemptId,
      mapPartitionId,
      shuffleWriteBytes,
      executorRunTimeMs)
  }

  def recordSuccessfulTask(
      currentStageId: Int,
      stageAttemptId: Int,
      mapPartitionId: Int,
      shuffleWriteBytes: Long,
      executorRunTimeMs: Long): Unit = synchronized {
    if (invalidReason.isEmpty) {
      submittedAttempts.get((currentStageId, stageAttemptId)) match {
        case None =>
          invalidate(TaskForUnknownStageAttempt)
        case Some(_) if mapPartitionId < 0 || mapPartitionId >= expectedMapTasks =>
          invalidate(MapPartitionOutOfRange)
        case Some(_) if shuffleWriteBytes < 0L || executorRunTimeMs < 0L =>
          invalidate(NegativeRuntimeMetric)
        case Some(attemptOrder) =>
          if (observedSuccessfulMapTaskCompletions == Long.MaxValue) {
            invalidate(RuntimeMetricOverflow)
          } else {
            observedSuccessfulMapTaskCompletions += 1L
            winners.get(mapPartitionId) match {
              case Some(current) if current.attemptOrder > attemptOrder =>
              case _ =>
                winners.update(
                  mapPartitionId,
                  Winner(attemptOrder, shuffleWriteBytes, executorRunTimeMs))
            }
          }
      }
    }
  }

  /** Atomically incorporates the successful attempt's accumulator IDs before sealing the stage. */
  def finish(
      successfulStageAttemptId: Int,
      finalAccumulatorIds: Iterable[Long],
      completionOrder: Long): ShuffleRecoveryStageRuntime = synchronized {
    finish(stageId, successfulStageAttemptId, finalAccumulatorIds, completionOrder)
  }

  def finish(
      successfulStageId: Int,
      successfulStageAttemptId: Int,
      finalAccumulatorIds: Iterable[Long],
      completionOrder: Long): ShuffleRecoveryStageRuntime = synchronized {
    accumulatorIds ++= finalAccumulatorIds
    finish(successfulStageId, successfulStageAttemptId, completionOrder)
  }

  def finish(
      successfulStageAttemptId: Int,
      completionOrder: Long): ShuffleRecoveryStageRuntime = synchronized {
    finish(stageId, successfulStageAttemptId, completionOrder)
  }

  def finish(
      successfulStageId: Int,
      successfulStageAttemptId: Int,
      completionOrder: Long): ShuffleRecoveryStageRuntime = synchronized {
    val totals = if (invalidReason.isEmpty) {
      checkedTotals()
    } else {
      Left(invalidReason.get)
    }
    val coverageComplete = winners.size == expectedMapTasks
    val finalInvalid = totals.left.toOption.orElse {
      if (coverageComplete) None else Some(IncompleteMapWinnerCoverage)
    }
    val (bytes, runTime) = totals.toOption.getOrElse((0L, 0L))
    ShuffleRecoveryStageRuntime(
      executionId,
      successfulStageId,
      successfulStageAttemptId,
      shuffleId,
      expectedMapTasks,
      winners.size,
      bytes,
      runTime,
      accumulatorIds.toSet,
      completionOrder,
      complete = finalInvalid.isEmpty,
      finalInvalid,
      rddScopeIds.toSet,
      observedSuccessfulMapTaskCompletions)
  }

  private def checkedTotals(): Either[String, (Long, Long)] = {
    try {
      Right(winners.valuesIterator.foldLeft((0L, 0L)) {
        case ((bytes, runTime), winner) =>
          (
            Math.addExact(bytes, winner.bytes),
            Math.addExact(runTime, winner.executorRunTimeMs))
      })
    } catch {
      case _: ArithmeticException => Left(RuntimeMetricOverflow)
    }
  }
}

/**
 * Listener-side runtime collector installed only by an explicit opportunity study.
 *
 * It performs no provider, manifest, file, or network I/O. All state is synchronized so report
 * threads cannot observe a partially updated stage. Listener failures become evidence state rather
 * than query failures wherever a stable accounting bucket can be preserved.
 */
private[sql] final class ShuffleRecoveryRuntimeWeightListener extends SparkListener {
  private case class StageKey(executionId: Long, shuffleId: Int)
  private case class AttemptKey(stageId: Int, stageAttemptId: Int)

  private val activeStages =
    mutable.HashMap.empty[StageKey, ShuffleRecoveryStageAccumulator]
  private val attemptToStage = mutable.HashMap.empty[AttemptKey, StageKey]
  private val completed = mutable.HashMap.empty[StageKey, ShuffleRecoveryStageRuntime]
  private var completionCounter = 0L
  private var attemptCounter = 0L

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = synchronized {
    val info = event.stageInfo
    for {
      executionId <- executionIdFrom(event.properties)
      shuffleId <- info.shuffleDepId
    } {
      val totalMapTasks = info.rddInfos.headOption
        .map(_.numPartitions)
        .getOrElse(info.numTasks)
      val stageKey = StageKey(executionId, shuffleId)
      val accumulator = activeStages.getOrElseUpdate(
        stageKey,
        new ShuffleRecoveryStageAccumulator(
          executionId,
          info.stageId,
          shuffleId,
          totalMapTasks))
      if (accumulator.expectedMapTasks != totalMapTasks) {
        accumulator.invalidate(MapperCountChangedAcrossAttempts)
      }
      accumulator.recordRddScopeIds(scopeIds(info))
      val attemptKey = AttemptKey(info.stageId, info.attemptNumber())
      attemptCounter = Math.addExact(attemptCounter, 1L)
      attemptToStage.put(attemptKey, stageKey)
      accumulator.startAttempt(info.stageId, info.attemptNumber(), attemptCounter)
    }
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = synchronized {
    if (event.reason == Success && event.taskMetrics != null) {
      val attemptKey = AttemptKey(event.stageId, event.stageAttemptId)
      attemptToStage.get(attemptKey).flatMap(activeStages.get).foreach { accumulator =>
        val partitionId = if (event.taskInfo.partitionId >= 0) {
          event.taskInfo.partitionId
        } else {
          event.taskInfo.index
        }
        accumulator.recordSuccessfulTask(
          event.stageId,
          event.stageAttemptId,
          partitionId,
          event.taskMetrics.shuffleWriteMetrics.bytesWritten,
          event.taskMetrics.executorRunTime)
      }
    }
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = synchronized {
    val info = event.stageInfo
    val attemptKey = AttemptKey(info.stageId, info.attemptNumber())
    attemptToStage.remove(attemptKey).flatMap(activeStages.get).foreach { accumulator =>
      accumulator.recordRddScopeIds(scopeIds(info))
      if (info.failureReason.isEmpty) {
        val stageKey = StageKey(accumulator.executionId, accumulator.shuffleId)
        completed.get(stageKey) match {
          case Some(runtime) if info.numTasks == 0 =>
            val additionalIds = info.accumulables.keys.toSet
            accumulator.recordAccumulatorIds(additionalIds)
            completed.update(
              stageKey,
              runtime.copy(
                accumulatorIds = runtime.accumulatorIds ++ additionalIds,
                rddScopeIds = runtime.rddScopeIds ++ scopeIds(info)))
          case _ =>
            completionCounter = Math.addExact(completionCounter, 1L)
            completed.update(
              stageKey,
              accumulator.finish(
                info.stageId,
                info.attemptNumber(),
                info.accumulables.keys,
                completionCounter))
        }
      } else {
        accumulator.recordAccumulatorIds(info.accumulables.keys)
      }
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case sqlEnd: SparkListenerSQLExecutionEnd =>
      releaseExecution(sqlEnd.executionId)
    case _ =>
  }

  def snapshot(): Seq[ShuffleRecoveryStageRuntime] = synchronized {
    completed.valuesIterator.toVector.sortBy { runtime =>
      (
        runtime.executionId,
        runtime.completionOrder,
        runtime.stageId,
        runtime.stageAttemptId)
    }
  }

  private def releaseExecution(executionId: Long): Unit = synchronized {
    activeStages.keysIterator
      .filter(_.executionId == executionId)
      .toVector
      .foreach(activeStages.remove)
    attemptToStage.filterInPlace { case (_, stageKey) =>
      stageKey.executionId != executionId
    }
  }

  private def executionIdFrom(properties: Properties): Option[Long] = {
    Option(properties)
      .flatMap(p => Option(p.getProperty(SQLExecution.EXECUTION_ID_KEY)))
      .flatMap { raw =>
        try {
          Some(raw.toLong)
        } catch {
          case _: NumberFormatException => None
        }
      }
  }

  private def scopeIds(info: StageInfo): Set[String] = {
    info.rddInfos.iterator.flatMap { rddInfo =>
      rddInfo.scope.iterator.flatMap(_.getAllScopes.iterator.map(_.id))
    }.filter(_.nonEmpty).toSet
  }
}
