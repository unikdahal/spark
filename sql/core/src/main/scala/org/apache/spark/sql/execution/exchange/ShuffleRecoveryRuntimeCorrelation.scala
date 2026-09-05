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

import scala.collection.mutable

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.metric.SQLShuffleWriteMetricsReporter

private[sql] case class ShuffleRecoveryExchangeRuntimeKey(
    exchangeOrdinal: Long,
    exchangePath: String,
    shuffleWriteMetricIds: Set[Long],
    rddScopeId: Option[String] = None)

/** Produces correlation keys without forcing a shuffle dependency. */
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
      ShuffleRecoveryExchangeRuntimeKey(
        ordinal.toLong,
        path,
        metricIds,
        Some(exchange.rddScopeId))
    }.toVector
  }

  private def effectiveChildren(plan: SparkPlan): Seq[ChildRef] = plan match {
    case adaptive: AdaptiveSparkPlanExec =>
      Seq(ChildRef("c0", adaptive.executedPlan))
    case stage: QueryStageExec =>
      Seq(ChildRef("c0", stage.plan))
    case reused: ReusedExchangeExec =>
      Seq(ChildRef("c0", reused.child))
    case other =>
      val children = other.children.zipWithIndex.map { case (child, index) =>
        ChildRef(s"c$index", child)
      }
      val subqueries = other.subqueries.zipWithIndex.map { case (child, index) =>
        ChildRef(s"s$index", child)
      }
      children ++ subqueries
  }

  private def pathString(pathReversed: List[String]): String = {
    if (pathReversed.isEmpty) "root"
    else pathReversed.reverseIterator.mkString(".")
  }
}

private[sql] sealed trait ShuffleRecoveryWeightDisposition {
  def code: String
}

private[sql] object ShuffleRecoveryWeightDisposition {
  case object Weighted extends ShuffleRecoveryWeightDisposition {
    val code = "WEIGHTED"
  }
  case object Unweighted extends ShuffleRecoveryWeightDisposition {
    val code = "UNWEIGHTED"
  }
  case object Excluded extends ShuffleRecoveryWeightDisposition {
    val code = "EXCLUDED"
  }
}

/** Stable machine-readable reasons for non-weighted runtime accounting. */
private[sql] object ShuffleRecoveryAccountingReason {
  val NegativeStageAttempt = "NEGATIVE_STAGE_ATTEMPT"
  val TaskForUnknownStageAttempt = "TASK_FOR_UNKNOWN_STAGE_ATTEMPT"
  val MapPartitionOutOfRange = "MAP_PARTITION_OUT_OF_RANGE"
  val NegativeRuntimeMetric = "NEGATIVE_RUNTIME_METRIC"
  val MapperCountChangedAcrossAttempts = "MAPPER_COUNT_CHANGED_ACROSS_ATTEMPTS"
  val RuntimeMetricOverflow = "RUNTIME_METRIC_OVERFLOW"
  val IncompleteMapWinnerCoverage = "INCOMPLETE_MAP_WINNER_COVERAGE"
  val ReusedPhysicalWork = "REUSED_PHYSICAL_WORK"
  val MissingWriteMetricKey = "MISSING_WRITE_METRIC_KEY"
  val NoRuntimeCorrelation = "NO_RUNTIME_CORRELATION"
  val AmbiguousRuntimeCorrelation = "AMBIGUOUS_RUNTIME_CORRELATION"

  val All: Set[String] = Set(
    NegativeStageAttempt,
    TaskForUnknownStageAttempt,
    MapPartitionOutOfRange,
    NegativeRuntimeMetric,
    MapperCountChangedAcrossAttempts,
    RuntimeMetricOverflow,
    IncompleteMapWinnerCoverage,
    ReusedPhysicalWork,
    MissingWriteMetricKey,
    NoRuntimeCorrelation,
    AmbiguousRuntimeCorrelation)
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
      "childOperatorClass" -> quote(c.childOperatorClass),
      "partitioningClass" -> quote(c.partitioningClass),
      "partitionCount" -> int(c.partitionCount),
      "ruleSetName" -> quote(c.ruleSetName),
      "ruleSetVersion" -> c.ruleSetVersion.toString,
      "eligible" -> c.eligible.toString,
      "immediateMissReason" -> optionalReason(c.immediateMissReason),
      "rootMissReason" -> optionalReason(c.rootMissReason),
      "sourceTokenAvailability" -> quote(c.sourceTokenAvailability.code),
      "lineageDeterminism" -> quote(c.lineageDeterminism.code),
      "dppPresent" -> c.flags.dynamicPruning.toString,
      "runtimeFilterPresent" -> c.flags.runtimeFilter.toString,
      "subqueryPresent" -> c.flags.subquery.toString,
      "windowPresent" -> c.flags.window.toString,
      "expandPresent" -> c.flags.expand.toString,
      "cacheScanPresent" -> c.flags.cacheScan.toString,
      "adaptivePartitionSpecsPresent" -> c.flags.adaptivePartitionSpecs.toString,
      "pythonOrArrowPresent" -> c.flags.pythonOrArrow.toString,
      "reusedExchange" -> c.flags.reusedExchange.toString,
      "adaptivePlan" -> c.flags.adaptivePlan.toString,
      "pipelinedShuffle" -> c.pipelinedShuffle.toString,
      "pushBasedShuffleEnabled" -> c.pushBasedShuffleEnabled.toString,
      "mergedShuffleEnabled" -> c.mergedShuffleEnabled.toString,
      "incompatibleRuntimeFlags" -> stringArray(c.incompatibleRuntimeFlags),
      "disposition" -> quote(disposition.code),
      "accountingReason" -> optionalString(accountingReason),
      "stageId" -> int(stageId),
      "stageAttemptId" -> int(stageAttemptId),
      "shuffleId" -> int(shuffleId),
      "mapperCount" -> int(mapperCount),
      "shuffleWriteBytes" -> long(shuffleWriteBytes),
      "executorRunTimeMs" -> long(executorRunTimeMs),
      "completionOrder" -> long(completionOrder))
    fields.iterator
      .map { case (key, value) => s"${quote(key)}:$value" }
      .mkString("{", ",", "}")
  }

  private def optionalReason(
      reason: Option[ShuffleRecoveryMissReason]): String = {
    reason.map(value => quote(value.code)).getOrElse("null")
  }

  private def optionalString(value: Option[String]): String = {
    value.map(quote).getOrElse("null")
  }

  private def stringArray(values: Seq[String]): String = {
    values.distinct.sorted.iterator.map(quote).mkString("[", ",", "]")
  }

  private def int(value: Option[Int]): String = {
    value.map(_.toString).getOrElse("null")
  }

  private def long(value: Option[Long]): String = {
    value.map(_.toString).getOrElse("null")
  }

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

private[exchange] final class ShuffleRecoveryRuntimeStageIndex private (
    byAccumulatorId: Map[(Long, Long), Vector[ShuffleRecoveryStageRuntime]],
    byRddScopeId: Map[(Long, String), Vector[ShuffleRecoveryStageRuntime]]) {

  def candidatesByAccumulator(
      executionId: Long,
      accumulatorIds: Set[Long]): Seq[ShuffleRecoveryStageRuntime] = {
    val unique = mutable.LinkedHashSet.empty[ShuffleRecoveryStageRuntime]
    accumulatorIds.toSeq.sorted.foreach { accumulatorId =>
      byAccumulatorId.get((executionId, accumulatorId)).foreach { stages =>
        stages.foreach(unique += _)
      }
    }
    unique.toVector
  }

  def candidatesByRddScope(
      executionId: Long,
      rddScopeId: Option[String]): Seq[ShuffleRecoveryStageRuntime] = {
    rddScopeId.toSeq.flatMap { scopeId =>
      byRddScopeId.getOrElse((executionId, scopeId), Vector.empty)
    }
  }
}

private[exchange] object ShuffleRecoveryRuntimeStageIndex {
  def fromStages(stages: Seq[ShuffleRecoveryStageRuntime]): ShuffleRecoveryRuntimeStageIndex = {
    val accumulatorBuilders = mutable.HashMap.empty[
      (Long, Long), mutable.ArrayBuffer[ShuffleRecoveryStageRuntime]]
    val scopeBuilders = mutable.HashMap.empty[
      (Long, String), mutable.ArrayBuffer[ShuffleRecoveryStageRuntime]]
    stages.foreach { stage =>
      stage.accumulatorIds.foreach { accumulatorId =>
        accumulatorBuilders
          .getOrElseUpdate(
            (stage.executionId, accumulatorId),
            mutable.ArrayBuffer.empty[ShuffleRecoveryStageRuntime])
          .append(stage)
      }
      stage.rddScopeIds.foreach { scopeId =>
        scopeBuilders
          .getOrElseUpdate(
            (stage.executionId, scopeId),
            mutable.ArrayBuffer.empty[ShuffleRecoveryStageRuntime])
          .append(stage)
      }
    }
    val accumulatorIndex = accumulatorBuilders.iterator.map { case (key, values) =>
      key -> values.distinct.toVector
    }.toMap
    val scopeIndex = scopeBuilders.iterator.map { case (key, values) =>
      key -> values.distinct.toVector
    }.toMap
    new ShuffleRecoveryRuntimeStageIndex(accumulatorIndex, scopeIndex)
  }
}

private[sql] object ShuffleRecoveryRuntimeCorrelator {
  import ShuffleRecoveryAccountingReason._
  import ShuffleRecoveryWeightDisposition._

  def correlate(
      observations: Seq[ShuffleRecoveryExchangeObservation],
      keys: Seq[ShuffleRecoveryExchangeRuntimeKey],
      stages: Seq[ShuffleRecoveryStageRuntime]): Seq[ShuffleRecoveryWeightedObservation] = {
    require(
      observations.size == keys.size,
      s"observation/key count mismatch: ${observations.size} != ${keys.size}")
    observations.zip(keys).foreach { case (observation, key) =>
      require(
        observation.exchangeOrdinal == key.exchangeOrdinal &&
          observation.exchangePath == key.exchangePath,
        s"observation/key path mismatch at ordinal ${observation.exchangeOrdinal}")
    }

    val stageIndex = ShuffleRecoveryRuntimeStageIndex.fromStages(stages)
    val seenPhysicalStages = mutable.HashSet.empty[(Long, Int)]
    observations.zip(keys).map { case (observation, key) =>
      correlateOne(observation, key, stageIndex, seenPhysicalStages)
    }
  }

  private def correlateOne(
      observation: ShuffleRecoveryExchangeObservation,
      key: ShuffleRecoveryExchangeRuntimeKey,
      stageIndex: ShuffleRecoveryRuntimeStageIndex,
      seen: mutable.HashSet[(Long, Int)]): ShuffleRecoveryWeightedObservation = {
    val executionId = parseExecutionId(observation.executionId)
    val scopeCandidates = stageIndex.candidatesByRddScope(executionId, key.rddScopeId)
    val metricCandidates = if (key.shuffleWriteMetricIds.isEmpty) {
      Nil
    } else {
      stageIndex.candidatesByAccumulator(executionId, key.shuffleWriteMetricIds)
    }
    val candidates = chooseCandidates(scopeCandidates, metricCandidates)
    candidates match {
      case Seq(stage) if !stage.complete =>
        unweighted(
          observation,
          stage.invalidReason.getOrElse(IncompleteMapWinnerCoverage))
      case Seq(stage) =>
        val physicalKey = (stage.executionId, stage.shuffleId)
        if (!seen.add(physicalKey)) {
          excluded(observation, ReusedPhysicalWork)
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
        val reason = if (key.rddScopeId.isEmpty && key.shuffleWriteMetricIds.isEmpty) {
          MissingWriteMetricKey
        } else {
          NoRuntimeCorrelation
        }
        unweighted(observation, reason)
      case _ =>
        unweighted(observation, AmbiguousRuntimeCorrelation)
    }
  }

  private def chooseCandidates(
      scopeCandidates: Seq[ShuffleRecoveryStageRuntime],
      metricCandidates: Seq[ShuffleRecoveryStageRuntime]): Seq[ShuffleRecoveryStageRuntime] = {
    if (scopeCandidates.nonEmpty) {
      val scopeShuffleIds = scopeCandidates.iterator.map(_.shuffleId).toSet
      val metricConflicts = metricCandidates.exists(stage => !scopeShuffleIds.contains(stage.shuffleId))
      if (metricConflicts) scopeCandidates ++ metricCandidates else scopeCandidates
    } else {
      metricCandidates
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
      observation,
      Unweighted,
      Some(reason),
      None,
      None,
      None,
      None,
      None,
      None,
      None)
  }

  private def excluded(
      observation: ShuffleRecoveryExchangeObservation,
      reason: String): ShuffleRecoveryWeightedObservation = {
    ShuffleRecoveryWeightedObservation(
      observation,
      Excluded,
      Some(reason),
      None,
      None,
      None,
      None,
      None,
      None,
      None)
  }
}
