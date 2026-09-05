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
  SparkListenerStageCompleted,
  SparkListenerStageSubmitted,
  SparkListenerTaskEnd}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}

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
    invalidReason: Option[String]) {

  require(expectedMapTasks >= 0, "expected map task count must be non-negative")
  require(successfulMapTaskWinners >= 0, "successful winner count must be non-negative")
  require(shuffleWriteBytes >= 0L, "shuffle-write bytes must be non-negative")
  require(executorRunTimeMs >= 0L, "executor run time must be non-negative")
}

/**
 * Accumulates one successful output candidate per logical map partition across stage retries.
 *
 * `StageInfo.numTasks` can be only the partitions submitted for one retry, so total RDD partition
 * count is supplied separately as the coverage denominator. A later attempt replaces an earlier
 * candidate for the same map partition. Within one attempt, the latest successful completion
 * replaces an earlier duplicate, matching MapOutputTracker's map-output registration semantics.
 */
private[exchange] final class ShuffleRecoveryStageAccumulator(
    val executionId: Long,
    val stageId: Int,
    val shuffleId: Int,
    val expectedMapTasks: Int) {

  private case class Winner(
      stageAttemptId: Int,
      bytes: Long,
      executorRunTimeMs: Long)

  require(expectedMapTasks >= 0, "expected map task count must be non-negative")

  private val submittedAttempts = mutable.HashSet.empty[Int]
  private val winners = mutable.HashMap.empty[Int, Winner]
  private val accumulatorIds = mutable.HashSet.empty[Long]
  private var invalidReason: Option[String] = None

  def startAttempt(stageAttemptId: Int): Unit = synchronized {
    if (stageAttemptId < 0) {
      invalidate(NegativeStageAttempt)
    } else {
      submittedAttempts += stageAttemptId
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

  def recordSuccessfulTask(
      stageAttemptId: Int,
      mapPartitionId: Int,
      shuffleWriteBytes: Long,
      executorRunTimeMs: Long): Unit = synchronized {
    if (invalidReason.isEmpty) {
      if (!submittedAttempts.contains(stageAttemptId)) {
        invalidate(TaskForUnknownStageAttempt)
      } else if (mapPartitionId < 0 || mapPartitionId >= expectedMapTasks) {
        invalidate(MapPartitionOutOfRange)
      } else if (shuffleWriteBytes < 0L || executorRunTimeMs < 0L) {
        invalidate(NegativeRuntimeMetric)
      } else {
        winners.get(mapPartitionId) match {
          case Some(current) if current.stageAttemptId > stageAttemptId =>
          case _ =>
            winners.update(
              mapPartitionId,
              Winner(stageAttemptId, shuffleWriteBytes, executorRunTimeMs))
        }
      }
    }
  }

  /** Atomically incorporates the successful attempt's accumulator IDs before sealing the stage. */
  def finish(
      successfulStageAttemptId: Int,
      finalAccumulatorIds: Iterable[Long],
      completionOrder: Long): ShuffleRecoveryStageRuntime = synchronized {
    accumulatorIds ++= finalAccumulatorIds
    finish(successfulStageAttemptId, completionOrder)
  }

  def finish(
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
      stageId,
      successfulStageAttemptId,
      shuffleId,
      expectedMapTasks,
      winners.size,
      bytes,
      runTime,
      accumulatorIds.toSet,
      completionOrder,
      complete = finalInvalid.isEmpty,
      finalInvalid)
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
  private case class StageKey(executionId: Long, stageId: Int, shuffleId: Int)
  private case class AttemptKey(stageId: Int, stageAttemptId: Int)

  private val activeStages =
    mutable.HashMap.empty[StageKey, ShuffleRecoveryStageAccumulator]
  private val attemptToStage = mutable.HashMap.empty[AttemptKey, StageKey]
  private val completed = mutable.ArrayBuffer.empty[ShuffleRecoveryStageRuntime]
  private var completionCounter = 0L

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = synchronized {
    val info = event.stageInfo
    for {
      executionId <- executionIdFrom(event.properties)
      shuffleId <- info.shuffleDepId
    } {
      val totalMapTasks = info.rddInfos.headOption
        .map(_.numPartitions)
        .getOrElse(info.numTasks)
      val stageKey = StageKey(executionId, info.stageId, shuffleId)
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
      val attemptKey = AttemptKey(info.stageId, info.attemptNumber())
      attemptToStage.put(attemptKey, stageKey)
      accumulator.startAttempt(info.attemptNumber())
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
      if (info.failureReason.isEmpty) {
        completionCounter = Math.addExact(completionCounter, 1L)
        completed += accumulator.finish(
          info.attemptNumber(),
          info.accumulables.keys,
          completionCounter)
        val stageKey = StageKey(
          accumulator.executionId,
          accumulator.stageId,
          accumulator.shuffleId)
        activeStages.remove(stageKey)
        attemptToStage.filterInPlace((_, key) => key != stageKey)
      } else {
        accumulator.recordAccumulatorIds(info.accumulables.keys)
      }
    }
  }

  def snapshot(): Seq[ShuffleRecoveryStageRuntime] = synchronized {
    completed.toVector.sortBy { runtime =>
      (
        runtime.executionId,
        runtime.completionOrder,
        runtime.stageId,
        runtime.stageAttemptId)
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
}
