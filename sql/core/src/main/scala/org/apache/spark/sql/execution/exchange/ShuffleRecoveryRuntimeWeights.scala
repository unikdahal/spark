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

import org.apache.spark.Success
import org.apache.spark.scheduler.{
  SparkListener, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd}
import org.apache.spark.sql.execution.{SQLExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.metric.SQLShuffleWriteMetricsReporter

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
 * Accumulates the successful map-output winner for each logical map partition across stage retries.
 *
 * StageInfo.numTasks is the number of tasks submitted for one attempt and can be smaller than the
 * shuffle's total mapper count on a determinate retry. The stage RDD's partition count is therefore
 * the coverage denominator. A later stage attempt replaces an earlier winner for the same map
 * partition; duplicate successes from the same attempt are ignored. This models the physical map
 * outputs that can constitute the completed shuffle without charging failed/speculative attempts as
 * reusable successful work.
 */
private[exchange] final class ShuffleRecoveryStageAccumulator(
    val executionId: Long,
    val stageId: Int,
    val shuffleId: Int,
    val expectedMapTasks: Int) {

  private case class Winner(stageAttemptId: Int, bytes: Long, executorRunTimeMs: Long)

  require(expectedMapTasks >= 0, "expected map task count must be non-negative")

  private val submittedAttempts = mutable.HashSet.empty[Int]
  private val winners = mutable.HashMap.empty[Int, Winner]
  private var invalid: Option[String] = None

  def startAttempt(stageAttemptId: Int): Unit = synchronized {
    if (stageAttemptId < 0) {
      invalid = Some("NEGATIVE_STAGE_ATTEMPT")
    } else {
      submittedAttempts += stageAttemptId
    }
  }

  def recordSuccessfulTask(
      stageAttemptId: Int,
      mapPartitionId: Int,
      shuffleWriteBytes: Long,
      executorRunTimeMs: Long): Unit = synchronized {
    if (invalid.isEmpty) {
      if (!submittedAttempts.contains(stageAttemptId)) {
        invalid = Some("TASK_FOR_UNKNOWN_STAGE_ATTEMPT")
      } else if (mapPartitionId < 0 || mapPartitionId >= expectedMapTasks) {
        invalid = Some("MAP_PARTITION_OUT_OF_RANGE")
      } else if (shuffleWriteBytes < 0L || executorRunTimeMs < 0L) {
        invalid = Some("NEGATIVE_RUNTIME_METRIC")
      } else {
        winners.get(mapPartitionId) match {
          case Some(current) if current.stageAttemptId > stageAttemptId =>
            // A delayed callback from an older attempt cannot replace a newer accepted candidate.
          case Some(current) if current.stageAttemptId == stageAttemptId =>
            // At most one successful candidate is charged for a speculative map partition.
          case _ =>
            winners.update(
              mapPartitionId,
              Winner(stageAttemptId, shuffleWriteBytes, executorRunTimeMs))
        }
      }
    }
  }

  def finish(
      successfulStageAttemptId: Int,
      accumulatorIds: Set[Long],
      completionOrder: Long): ShuffleRecoveryStageRuntime = synchronized {
    val totals = if (invalid.isEmpty) checkedTotals() else Left(invalid.get)
    val coverageComplete = winners.size == expectedMapTasks
    val finalInvalid = totals.left.toOption.orElse {
      if (coverageComplete) None else Some("INCOMPLETE_MAP_WINNER_COVERAGE")
    }
    val (bytes, runTime) = totals.toOption.getOrElse((0L, 0L))
    ShuffleRecoveryStageRuntime(
      executionId = executionId,
      stageId = stageId,
      stageAttemptId = successfulStageAttemptId,
      shuffleId = shuffleId,
      expectedMapTasks = expectedMapTasks,
      successfulMapTaskWinners = winners.size,
      shuffleWriteBytes = bytes,
      executorRunTimeMs = runTime,
      accumulatorIds = accumulatorIds,
      completionOrder = completionOrder,
      complete = finalInvalid.isEmpty,
      invalidReason = finalInvalid)
  }

  private def checkedTotals(): Either[String, (Long, Long)] = {
    try {
      val totals = winners.valuesIterator.foldLeft((0L, 0L)) {
        case ((bytes, runTime), winner) =>
          (Math.addExact(bytes, winner.bytes), Math.addExact(runTime, winner.executorRunTimeMs))
      }
      Right(totals)
    } catch {
      case _: ArithmeticException => Left("RUNTIME_METRIC_OVERFLOW")
    }
  }
}

/**
 * Listener-side collector used only while an opportunity study is explicitly installed.
 *
 * The collector performs no provider, manifest, file, or network I/O. Listener callbacks run off
 * the DAGScheduler event loop. All externally visible snapshots are synchronized so evidence can be
 * read concurrently without exposing a partially updated stage.
 */
private[sql] final class ShuffleRecoveryRuntimeWeightListener extends SparkListener {
  private case class StageKey(executionId: Long, stageId: Int, shuffleId: Int)
  private case class AttemptKey(stageId: Int, stageAttemptId: Int)

  private val activeStages = mutable.HashMap.empty[StageKey, ShuffleRecoveryStageAccumulator]
  private val attemptToStage = mutable.HashMap.empty[AttemptKey, StageKey]
  private val completed = mutable.ArrayBuffer.empty[ShuffleRecoveryStageRuntime]
  private var completionCounter = 0L

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = synchronized {
    val info = event.stageInfo
    for {
      executionId <- executionIdFrom(event.properties)
      shuffleId <- info.shuffleDepId
    } {
      val totalMapTasks = info.rddInfos.headOption.map(_.numPartitions).getOrElse(info.numTasks)
      val stageKey = StageKey(executionId, info.stageId, shuffleId)
      val accumulator = activeStages.getOrElseUpdate(
        stageKey,
        new ShuffleRecoveryStageAccumulator(executionId, info.stageId, shuffleId, totalMapTasks))
      if (accumulator.expectedMapTasks != totalMapTasks) {
        throw new IllegalStateException(
          s"shuffle $shuffleId changed mapper count across attempts: " +
            s"${accumulator.expectedMapTasks} != $totalMapTasks")
      }
      val attemptKey = AttemptKey(info.stageId, info.attemptNumber())
      attemptToStage.put(attemptKey, stageKey)
      accumulator.startAttempt(info.attemptNumber())
    }
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = synchronized {
    if (event.reason == Success && event.taskMetrics != null) {
      attemptToStage.get(AttemptKey(event.stageId, event.stageAttemptId)).foreach { stageKey =>
        activeStages.get(stageKey).foreach { accumulator =>
          val mapPartitionId = if (event.taskInfo.partitionId >= 0) {
            event.taskInfo.partitionId
          } else {
            event.taskInfo.index
          }
          accumulator.recordSuccessfulTask(
            event.stageAttemptId,
            mapPartitionId,
            event.taskMetrics.shuffleWriteMetrics.bytesWritten,
            event.taskMetrics.executorRunTime)
        }
      }
    }
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = synchronized {
    val info = event.stageInfo
    val attemptKey = AttemptKey(info.stageId, info.attemptNumber())
    attemptToStage.remove(attemptKey).flatMap(activeStages.get).foreach { accumulator =>
      if (info.failureReason.isEmpty) {
        completionCounter = Math.addExact(completionCounter, 1L)
        val stageKey = StageKey(accumulator.executionId, accumulator.stageId, accumulator.shuffleId)
        completed += accumulator.finish(
          info.attemptNumber(),
          info.accumulables.keySet.toSet,
          completionCounter)
        activeStages.remove(stageKey)
        attemptToStage.retain { case (_, key) => key != stageKey }
      }
    }
  }

  def snapshot(): Seq[ShuffleRecoveryStageRuntime] = synchronized {
    completed.toVector.sortBy(runtime =>
      (runtime.executionId, runtime.completionOrder, runtime.stageId, runtime.stageAttemptId))
  }

  private def executionIdFrom(properties: Properties): Option[Long] = {
    Option(properties)
      .flatMap(p => Option(p.getProperty(SQLExecution.EXECUTION_ID_KEY)))
      .flatMap { raw =>
        try Some(raw.toLong) catch {
          case _: NumberFormatException => None
        }
      }
  }
}

private[sql] case class ShuffleRecoveryExchangeRuntimeKey(
    exchangeOrdinal: Long,
    exchangePath: String,
    shuffleWriteMetricIds: Set[Long])

/** Produces runtime-correlation keys without forcing a shuffle dependency. */
private[exchange] object ShuffleRecoveryExchangeRuntimeKeys {
  private case class ChildRef(label: String, plan: SparkPlan)
  private case class Frame(plan: SparkPlan, pathReversed: List[String])

  private val writeMetricNames = Set(
    SQLShuffleWriteMetricsReporter.SHUFFLE_BYTES_WRITTEN,
    SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN,
    SQLShuffleWriteMetricsReporter.SHUFFLE_WRITE_TIME)

  def fromPlan(plan: SparkPlan): Seq[ShuffleRecoveryExchangeRuntimeKey] = {
    require(plan != null, "plan must not be null")
    val exchanges = mutable.ArrayBuffer.empty[(String, ShuffleExchangeExec)]
    val stack = mutable.ArrayBuffer(Frame(plan, Nil))
    while (stack.nonEmpty) {
      val frame = stack.remove(stack.length - 1)
      frame.plan match {
        case exchange: ShuffleExchangeExec =>
          exchanges += ((pathString(frame.pathReversed), exchange))
        case _ =>
      }
      val children = effectiveChildren(frame.plan)
      var index = children.length - 1
      while (index >= 0) {
        val child = children(index)
        stack += Frame(child.plan, child.label :: frame.pathReversed)
        index -= 1
      }
    }
    exchanges.zipWithIndex.map { case ((path, exchange), ordinal) =>
      val metricIds = exchange.metrics.iterator.collect {
        case (name, metric) if writeMetricNames.contains(name) => metric.id
      }.toSet
      ShuffleRecoveryExchangeRuntimeKey(ordinal.toLong, path, metricIds)
    }.toVector
  }

  private def effectiveChildren(plan: SparkPlan): Seq[ChildRef] = plan match {
    case adaptive: AdaptiveSparkPlanExec => Seq(ChildRef("c0", adaptive.executedPlan))
    case stage: QueryStageExec => Seq(ChildRef("c0", stage.plan))
    case reused: ReusedExchangeExec => Seq(ChildRef("c0", reused.child))
    case other =>
      other.children.zipWithIndex.map { case (child, index) => ChildRef(s"c$index", child) } ++
        other.subqueries.zipWithIndex.map { case (child, index) => ChildRef(s"s$index", child) }
  }

  private def pathString(pathReversed: List[String]): String = {
    if (pathReversed.isEmpty) "root" else pathReversed.reverseIterator.mkString(".")
  }
}

private[sql] sealed trait ShuffleRecoveryWeightDisposition { def code: String }
private[sql] object ShuffleRecoveryWeightDisposition {
  case object Weighted extends ShuffleRecoveryWeightDisposition { val code = "WEIGHTED" }
  case object Unweighted extends ShuffleRecoveryWeightDisposition { val code = "UNWEIGHTED" }
  case object Excluded extends ShuffleRecoveryWeightDisposition { val code = "EXCLUDED" }
}

private[sql] case class ShuffleRecoveryWeightedObservation(
    classification: ShuffleRecoveryExchangeObservation,
    disposition: ShuffleRecoveryWeightDisposition,
    accountingReason: Option[String],
    stageId: Option[Int],
    stageAttemptId: Option[Int],
    shuffleId: Option[Int],
    mapperCount: Option[Int],
    shuffleWriteBytes: Option[Long],
    executorRunTimeMs: Option[Long],
    completionOrder: Option[Long]) {

  def toJson: String = {
    val c = classification
    val fields = Seq(
      "schemaVersion" -> ShuffleRecoveryOpportunityRawIO.SchemaVersion.toString,
      "executionId" -> quote(c.executionId),
      "exchangeOrdinal" -> c.exchangeOrdinal.toString,
      "exchangePath" -> quote(c.exchangePath),
      "ruleSetName" -> quote(c.ruleSetName),
      "ruleSetVersion" -> c.ruleSetVersion.toString,
      "eligible" -> c.eligible.toString,
      "immediateMissReason" -> c.immediateMissReason.map(r => quote(r.code)).getOrElse("null"),
      "rootMissReason" -> c.rootMissReason.map(r => quote(r.code)).getOrElse("null"),
      "sourceTokenAvailability" -> quote(c.sourceTokenAvailability.code),
      "disposition" -> quote(disposition.code),
      "accountingReason" -> accountingReason.map(quote).getOrElse("null"),
      "stageId" -> int(stageId),
      "stageAttemptId" -> int(stageAttemptId),
      "shuffleId" -> int(shuffleId),
      "mapperCount" -> int(mapperCount),
      "shuffleWriteBytes" -> long(shuffleWriteBytes),
      "executorRunTimeMs" -> long(executorRunTimeMs),
      "completionOrder" -> long(completionOrder))
    fields.iterator.map { case (key, value) => s"${quote(key)}:$value" }
      .mkString("{", ",", "}")
  }

  private def int(value: Option[Int]): String = value.map(_.toString).getOrElse("null")
  private def long(value: Option[Long]): String = value.map(_.toString).getOrElse("null")

  private def quote(value: String): String = {
    val builder = new StringBuilder(value.length + 2).append('"')
    value.foreach {
      case '"' => builder.append("\\\"")
      case '\\' => builder.append("\\\\")
      case '\b' => builder.append("\\b")
      case '\f' => builder.append("\\f")
      case '\n' => builder.append("\\n")
      case '\r' => builder.append("\\r")
      case '\t' => builder.append("\\t")
      case c if c < ' ' => builder.append(f"\\u${c.toInt}%04x")
      case c => builder.append(c)
    }
    builder.append('"').toString()
  }
}

private[sql] object ShuffleRecoveryRuntimeCorrelator {
  import ShuffleRecoveryWeightDisposition._

  def correlate(
      observations: Seq[ShuffleRecoveryExchangeObservation],
      keys: Seq[ShuffleRecoveryExchangeRuntimeKey],
      stages: Seq[ShuffleRecoveryStageRuntime]): Seq[ShuffleRecoveryWeightedObservation] = {
    require(observations.size == keys.size,
      s"observation/key count mismatch: ${observations.size} != ${keys.size}")
    observations.zip(keys).foreach { case (observation, key) =>
      require(observation.exchangeOrdinal == key.exchangeOrdinal &&
        observation.exchangePath == key.exchangePath,
        s"observation/key path mismatch at ordinal ${observation.exchangeOrdinal}")
    }

    val seenPhysicalStages = mutable.HashSet.empty[(Long, Int, Int, Int)]
    observations.zip(keys).map { case (observation, key) =>
      val executionId = parseExecutionId(observation.executionId)
      val candidates = stages.filter { stage =>
        stage.executionId == executionId &&
          key.shuffleWriteMetricIds.nonEmpty &&
          key.shuffleWriteMetricIds.exists(stage.accumulatorIds.contains)
      }
      candidates match {
        case Seq(stage) if !stage.complete =>
          unweighted(observation, stage.invalidReason.getOrElse("INCOMPLETE_MAP_WINNER_COVERAGE"))
        case Seq(stage) =>
          val physicalKey =
            (stage.executionId, stage.stageId, stage.stageAttemptId, stage.shuffleId)
          if (!seenPhysicalStages.add(physicalKey)) {
            excluded(observation, "REUSED_PHYSICAL_WORK")
          } else {
            ShuffleRecoveryWeightedObservation(
              observation,
              Weighted,
              None,
              Some(stage.stageId),
              Some(stage.stageAttemptId),
              Some(stage.shuffleId),
              Some(stage.expectedMapTasks),
              Some(stage.shuffleWriteBytes),
              Some(stage.executorRunTimeMs),
              Some(stage.completionOrder))
          }
        case Seq() =>
          unweighted(observation,
            if (key.shuffleWriteMetricIds.isEmpty) "MISSING_WRITE_METRIC_KEY"
            else "NO_RUNTIME_CORRELATION")
        case _ => unweighted(observation, "AMBIGUOUS_RUNTIME_CORRELATION")
      }
    }
  }

  private def parseExecutionId(value: String): Long = {
    val prefix = "query-"
    require(value.startsWith(prefix), s"unexpected execution id: $value")
    value.substring(prefix.length).toLong
  }

  private def unweighted(
      observation: ShuffleRecoveryExchangeObservation,
      reason: String): ShuffleRecoveryWeightedObservation = {
    ShuffleRecoveryWeightedObservation(
      observation, Unweighted, Some(reason), None, None, None, None, None, None, None)
  }

  private def excluded(
      observation: ShuffleRecoveryExchangeObservation,
      reason: String): ShuffleRecoveryWeightedObservation = {
    ShuffleRecoveryWeightedObservation(
      observation, Excluded, Some(reason), None, None, None, None, None, None, None)
  }
}
