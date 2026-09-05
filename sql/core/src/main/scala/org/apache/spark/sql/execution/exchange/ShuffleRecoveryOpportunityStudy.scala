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
import scala.util.control.NonFatal

import org.apache.spark.Success
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{
  SparkListener, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd,
  StageInfo}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.metric.SQLShuffleWriteMetricsReporter
import org.apache.spark.sql.util.QueryExecutionListener

/**
 * Runtime weight for one successfully completed shuffle-map stage attempt.
 *
 * Only the first successful task attempt for each logical map index contributes. Failed attempts,
 * speculative losers, and failed stage attempts therefore never inflate reusable work. A stage is
 * considered weightable only when every expected logical map index has an accepted winner.
 */
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

private[exchange] final class ShuffleRecoveryStageAttemptAccumulator(
    val executionId: Long,
    val stageId: Int,
    val stageAttemptId: Int,
    val shuffleId: Int,
    val expectedMapTasks: Int) {
  require(expectedMapTasks >= 0, "expected map task count must be non-negative")

  private case class Winner(bytes: Long, executorRunTimeMs: Long)
  private val winners = mutable.HashMap.empty[Int, Winner]
  private var totalBytes = 0L
  private var totalRunTimeMs = 0L
  private var invalid: Option[String] = None

  def recordSuccessfulTask(
      taskIndex: Int,
      shuffleWriteBytes: Long,
      executorRunTimeMs: Long): Unit = synchronized {
    if (invalid.isEmpty && !winners.contains(taskIndex)) {
      if (taskIndex < 0 || taskIndex >= expectedMapTasks) {
        invalid = Some("TASK_INDEX_OUT_OF_RANGE")
      } else if (shuffleWriteBytes < 0L || executorRunTimeMs < 0L) {
        invalid = Some("NEGATIVE_RUNTIME_METRIC")
      } else {
        try {
          val nextBytes = Math.addExact(totalBytes, shuffleWriteBytes)
          val nextRunTime = Math.addExact(totalRunTimeMs, executorRunTimeMs)
          winners.put(taskIndex, Winner(shuffleWriteBytes, executorRunTimeMs))
          totalBytes = nextBytes
          totalRunTimeMs = nextRunTime
        } catch {
          case _: ArithmeticException => invalid = Some("RUNTIME_METRIC_OVERFLOW")
        }
      }
    }
  }

  def finish(accumulatorIds: Set[Long], completionOrder: Long): ShuffleRecoveryStageRuntime =
    synchronized {
      val complete = invalid.isEmpty && winners.size == expectedMapTasks
      ShuffleRecoveryStageRuntime(
        executionId = executionId,
        stageId = stageId,
        stageAttemptId = stageAttemptId,
        shuffleId = shuffleId,
        expectedMapTasks = expectedMapTasks,
        successfulMapTaskWinners = winners.size,
        shuffleWriteBytes = totalBytes,
        executorRunTimeMs = totalRunTimeMs,
        accumulatorIds = accumulatorIds,
        completionOrder = completionOrder,
        complete = complete,
        invalidReason = invalid.orElse {
          if (complete) None else Some("INCOMPLETE_SUCCESSFUL_STAGE")
        })
    }
}

/**
 * Listener-side runtime collector used only while an opportunity study is explicitly installed.
 *
 * It performs no storage or provider I/O and never runs on the DAGScheduler event loop. The live
 * listener bus serializes callbacks to this listener, while synchronized snapshots make test and
 * report threads safe. Stage retries are represented by separate attempt keys; only a successful
 * final attempt is retained as candidate reusable work.
 */
private[sql] final class ShuffleRecoveryRuntimeWeightListener extends SparkListener with Logging {
  private case class StageAttemptKey(stageId: Int, stageAttemptId: Int)

  private val active = mutable.HashMap.empty[StageAttemptKey, ShuffleRecoveryStageAttemptAccumulator]
  private val completed = mutable.ArrayBuffer.empty[ShuffleRecoveryStageRuntime]
  private var completionCounter = 0L

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = synchronized {
    val info = event.stageInfo
    for {
      executionId <- executionIdFrom(event.properties)
      shuffleId <- info.shuffleDepId
    } {
      val key = StageAttemptKey(info.stageId, info.attemptNumber())
      active.put(key, new ShuffleRecoveryStageAttemptAccumulator(
        executionId,
        info.stageId,
        info.attemptNumber(),
        shuffleId,
        info.numTasks))
    }
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = synchronized {
    if (event.reason == Success && event.taskMetrics != null) {
      active.get(StageAttemptKey(event.stageId, event.stageAttemptId)).foreach { attempt =>
        val taskMetrics = event.taskMetrics
        val writeMetrics = taskMetrics.shuffleWriteMetrics
        attempt.recordSuccessfulTask(
          event.taskInfo.index,
          if (writeMetrics == null) 0L else writeMetrics.bytesWritten,
          taskMetrics.executorRunTime)
      }
    }
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = synchronized {
    val info = event.stageInfo
    val key = StageAttemptKey(info.stageId, info.attemptNumber())
    active.remove(key).foreach { attempt =>
      if (info.failureReason.isEmpty) {
        completionCounter = Math.addExact(completionCounter, 1L)
        completed += attempt.finish(info.accumulables.keySet.toSet, completionCounter)
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

/**
 * Produces correlation keys without forcing a shuffle dependency or reading plan strings.
 *
 * SQL execution plan-info generation already materializes SQL metric identities. Correlation uses
 * only the three exchange-local shuffle-write metric IDs, so downstream shuffle-read metrics cannot
 * accidentally associate an upstream exchange with the wrong map stage.
 */
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
    fields.iterator.map { case (key, value) => s"${quote(key)}:$value" }.mkString("{", ",", "}")
  }

  private def int(value: Option[Int]): String = value.map(_.toString).getOrElse("null")
  private def long(value: Option[Long]): String = value.map(_.toString).getOrElse("null")
  private def quote(value: String): String = {
    val builder = new StringBuilder(value.length + 2).append('"')
    value.foreach {
      case '"' => builder.append("\\\"")
      case '\\' => builder.append("\\\\")
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
          unweighted(observation, stage.invalidReason.getOrElse("INCOMPLETE_SUCCESSFUL_STAGE"))
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
        case Seq() => unweighted(observation,
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

private[sql] case class ShuffleRecoveryStudyRuleSet(
    rules: ShuffleRecoveryEligibilityRules,
    curveRole: String)

private[sql] object ShuffleRecoveryStudyRuleSets {
  import ShuffleRecoverySourceTokenAvailability._

  val observedBaseline: ShuffleRecoveryStudyRuleSet = ShuffleRecoveryStudyRuleSet(
    ShuffleRecoveryEligibilityRules.conservative.copy(
      name = "observed-baseline-v1",
      version = 1),
    "observed")

  val exactSourceCounterfactual: ShuffleRecoveryStudyRuleSet = ShuffleRecoveryStudyRuleSet(
    observedBaseline.rules.copy(
      name = "exact-source-counterfactual-v1",
      acceptedSourceTokenCategories = Set(Exact, PrototypeSpecialCased, Unavailable)),
    "counterfactual-source")

  val dppAndRuntimeFilter: ShuffleRecoveryStudyRuleSet = ShuffleRecoveryStudyRuleSet(
    exactSourceCounterfactual.rules.copy(
      name = "exact-source-plus-dpp-runtime-filter-v1",
      allowDynamicPruning = true,
      allowRuntimeFilters = true,
      allowSubqueries = true),
    "scope-curve")

  val window: ShuffleRecoveryStudyRuleSet = ShuffleRecoveryStudyRuleSet(
    dppAndRuntimeFilter.rules.copy(
      name = "exact-source-plus-dpp-window-v1",
      allowWindow = true),
    "scope-curve")

  val additionalCandidates: Seq[ShuffleRecoveryStudyRuleSet] = Seq(
    ShuffleRecoveryStudyRuleSet(
      window.rules.copy(name = "candidate-plus-expand-v1", allowExpand = true),
      "additional-candidate"),
    ShuffleRecoveryStudyRuleSet(
      window.rules.copy(name = "candidate-plus-cache-scan-v1", allowCacheScan = true),
      "additional-candidate"),
    ShuffleRecoveryStudyRuleSet(
      window.rules.copy(
        name = "candidate-plus-adaptive-partition-specs-v1",
        allowAdaptivePartitionSpecs = true),
      "additional-candidate"))

  val all: Seq[ShuffleRecoveryStudyRuleSet] =
    Seq(observedBaseline, exactSourceCounterfactual, dppAndRuntimeFilter, window) ++
      additionalCandidates
}

private[exchange] case class ShuffleRecoveryPendingExecution(
    executionId: Long,
    byRule: Seq[(ShuffleRecoveryStudyRuleSet, Seq[ShuffleRecoveryExchangeObservation])],
    runtimeKeys: Seq[ShuffleRecoveryExchangeRuntimeKey])

/**
 * Explicit lifecycle wrapper for evidence collection. Ordinary Spark sessions never install these
 * listeners, and closing a study removes both listeners before returning.
 */
private[sql] final class ShuffleRecoveryOpportunityStudy(
    spark: SparkSession,
    ruleSets: Seq[ShuffleRecoveryStudyRuleSet] = ShuffleRecoveryStudyRuleSets.all)
  extends AutoCloseable with Logging {

  require(ruleSets.nonEmpty, "at least one rule set is required")
  require(ruleSets.map(r => (r.rules.name, r.rules.version)).distinct.size == ruleSets.size,
    "rule-set name/version pairs must be unique")

  private val runtimeListener = new ShuffleRecoveryRuntimeWeightListener
  private val pending = mutable.ArrayBuffer.empty[ShuffleRecoveryPendingExecution]
  private val failedExecutions = mutable.ArrayBuffer.empty[(Long, String)]
  private var installed = false

  private val queryListener = new QueryExecutionListener {
    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
      try {
        val executionId = qe.id
        val id = f"query-$executionId%020d"
        val byRule = ruleSets.map { ruleSet =>
          ruleSet -> ShuffleRecoveryOpportunityAnalyzer.analyze(
            qe.executedPlan, id, ruleSet.rules, runtimeState(qe))
        }
        val keys = ShuffleRecoveryExchangeRuntimeKeys.fromPlan(qe.executedPlan)
        ShuffleRecoveryOpportunityStudy.this.synchronized {
          pending += ShuffleRecoveryPendingExecution(executionId, byRule, keys)
        }
      } catch {
        case NonFatal(error) =>
          logWarning("Shuffle recovery opportunity study analysis failed; query is unaffected.", error)
      }
    }

    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
      ShuffleRecoveryOpportunityStudy.this.synchronized {
        failedExecutions += ((qe.id, exception.getClass.getName))
      }
    }
  }

  def install(): Unit = synchronized {
    require(!installed, "study is already installed")
    spark.sparkContext.addSparkListener(runtimeListener)
    spark.listenerManager.register(queryListener)
    installed = true
  }

  def snapshot(): ShuffleRecoveryOpportunityStudySnapshot = {
    require(installed, "study must be installed before taking a snapshot")
    spark.sparkContext.listenerBus.waitUntilEmpty()
    val stages = runtimeListener.snapshot()
    val (executions, failures) = synchronized { (pending.toVector, failedExecutions.toVector) }
    val weighted = executions.flatMap { execution =>
      execution.byRule.flatMap { case (_, observations) =>
        ShuffleRecoveryRuntimeCorrelator.correlate(observations, execution.runtimeKeys, stages)
      }
    }
    ShuffleRecoveryOpportunityStudySnapshot(
      weighted.sortBy(record => (
        record.classification.ruleSetName,
        record.classification.ruleSetVersion,
        record.classification.executionId,
        record.classification.exchangeOrdinal)),
      failures.sortBy(_._1),
      stages)
  }

  override def close(): Unit = synchronized {
    if (installed) {
      spark.listenerManager.unregister(queryListener)
      spark.sparkContext.removeSparkListener(runtimeListener)
      installed = false
    }
  }

  private def runtimeState(qe: QueryExecution): ShuffleRecoveryRuntimeState = {
    val sparkConf = qe.sparkSession.sparkContext.getConf
    val shuffleManager = sparkConf.get("spark.shuffle.manager", "sort")
    ShuffleRecoveryRuntimeState(
      pushBasedShuffleEnabled = sparkConf.getBoolean("spark.shuffle.push.enabled", false),
      incompatibleFlags = if (shuffleManager == "sort") Nil else Seq("CUSTOM_SHUFFLE_MANAGER"))
  }
}

private[sql] case class ShuffleRecoveryOpportunityStudySnapshot(
    records: Seq[ShuffleRecoveryWeightedObservation],
    failedExecutions: Seq[(Long, String)],
    stages: Seq[ShuffleRecoveryStageRuntime]) {

  def validateAccounting(): Unit = {
    records.groupBy(record =>
      (record.classification.ruleSetName, record.classification.ruleSetVersion)).foreach {
      case ((name, version), values) =>
        val bucketed = values.count(record =>
          record.disposition == ShuffleRecoveryWeightDisposition.Weighted ||
            record.disposition == ShuffleRecoveryWeightDisposition.Unweighted ||
            record.disposition == ShuffleRecoveryWeightDisposition.Excluded)
        require(bucketed == values.size,
          s"unreconciled exchange accounting for $name/$version")
    }
  }

  def deterministicJsonLines: Seq[String] = {
    validateAccounting()
    records.map(_.toJson)
  }
}
