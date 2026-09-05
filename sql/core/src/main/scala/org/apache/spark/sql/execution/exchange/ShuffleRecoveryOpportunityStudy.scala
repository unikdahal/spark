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
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

private[sql] case class ShuffleRecoveryStudyRuleSet(
    rules: ShuffleRecoveryEligibilityRules,
    curveRole: String)

/**
 * Preregistered semantic curve for the opportunity study.
 *
 * The observed row records only source-token capabilities that exist in the feasibility analyzer.
 * The counterfactual row grants an exact token to the controlled built-in batch sources while
 * retaining their observed source-token category in the observed row. Determinate lineage for
 * those sources is a separate semantic fact: the corpus materializes immutable batch input before
 * the study starts, so absence of a durable cross-driver token must not be conflated with unknown
 * RDD determinism.
 */
private[sql] object ShuffleRecoveryStudyRuleSets {
  import ShuffleRecoveryLineageDeterminism._
  import ShuffleRecoverySourceTokenAvailability._

  private val execution = "org.apache.spark.sql.execution."
  private val deterministicCorpusSources = Set(
    execution + "FileSourceScanExec",
    execution + "datasources.v2.BatchScanExec",
    execution + "LocalTableScanExec")

  private val observedLineage = ShuffleRecoveryEligibilityRules.conservative
    .lineageBySourceOperatorClassName ++ deterministicCorpusSources.iterator.map(_ -> Determinate)
  private val exactCounterfactualTokens = ShuffleRecoveryEligibilityRules.conservative
    .sourceTokenByOperatorClassName ++ deterministicCorpusSources.iterator.map(_ -> Exact)

  val observedBaseline: ShuffleRecoveryStudyRuleSet = ShuffleRecoveryStudyRuleSet(
    ShuffleRecoveryEligibilityRules.conservative.copy(
      name = "observed-baseline-v1",
      version = 1,
      lineageBySourceOperatorClassName = observedLineage),
    "observed")

  val exactSourceCounterfactual: ShuffleRecoveryStudyRuleSet = ShuffleRecoveryStudyRuleSet(
    observedBaseline.rules.copy(
      name = "exact-source-counterfactual-v1",
      sourceTokenByOperatorClassName = exactCounterfactualTokens),
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

  /**
   * Fixed before corpus execution. The report selects whichever of these assumptions unlocks the
   * most additional weighted task time over the Window row; it never invents a new rule after
   * observing the result.
   */
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
 * Explicit lifecycle wrapper for evidence collection.
 *
 * Ordinary Spark sessions never install these listeners. Query callbacks classify already executed
 * plans and read existing SQL metric identities; they do not create a shuffle dependency. Runtime
 * weighting comes solely from listener state, and closing the study removes both listeners.
 */
private[sql] final class ShuffleRecoveryOpportunityStudy(
    spark: SparkSession,
    ruleSets: Seq[ShuffleRecoveryStudyRuleSet] = ShuffleRecoveryStudyRuleSets.all)
  extends AutoCloseable with Logging {

  private case class AnalyzedPlan(
      byRule: Seq[(ShuffleRecoveryStudyRuleSet, Seq[ShuffleRecoveryExchangeObservation])],
      runtimeKeys: Seq[ShuffleRecoveryExchangeRuntimeKey])

  require(ruleSets.nonEmpty, "at least one rule set is required")
  require(ruleSets.map(r => (r.rules.name, r.rules.version)).distinct.size == ruleSets.size,
    "rule-set name/version pairs must be unique")

  private val runtimeListener = new ShuffleRecoveryRuntimeWeightListener
  private val completedExecutions = mutable.ArrayBuffer.empty[ShuffleRecoveryPendingExecution]
  private val failedExecutions = mutable.ArrayBuffer.empty[(Long, String)]
  private val analysisFailures = mutable.ArrayBuffer.empty[(Long, String)]
  private var installed = false

  private val queryListener = new SparkListener {
    override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
      case sqlEnd: SparkListenerSQLExecutionEnd if shouldReport(sqlEnd) =>
        recordExecution(sqlEnd)
      case _ =>
    }
  }

  def install(): Unit = synchronized {
    require(!installed, "study is already installed")
    spark.sparkContext.addSparkListener(runtimeListener)
    spark.sparkContext.addSparkListener(queryListener)
    installed = true
  }

  def snapshot(): ShuffleRecoveryOpportunityStudySnapshot = {
    require(installed, "study must be installed before taking a snapshot")
    spark.sparkContext.listenerBus.waitUntilEmpty()
    val stages = runtimeListener.snapshot()
    val stagesByExecution = stages.groupBy(_.executionId)
    val (executions, failures, analyzerFailures) = synchronized {
      (completedExecutions.toVector, failedExecutions.toVector, analysisFailures.toVector)
    }
    val weighted = executions.flatMap { execution =>
      val executionStages = stagesByExecution.getOrElse(execution.executionId, Nil)
      execution.byRule.flatMap { case (_, observations) =>
        ShuffleRecoveryRuntimeCorrelator.correlate(
          observations,
          execution.runtimeKeys,
          executionStages)
      }
    }
    val snapshot = ShuffleRecoveryOpportunityStudySnapshot(
      records = weighted.sortBy(record => (
        record.classification.ruleSetName,
        record.classification.ruleSetVersion,
        record.classification.executionId,
        record.classification.exchangeOrdinal)),
      completedExecutionIds = executions.map(_.executionId).sorted,
      failedExecutions = failures.sortBy(_._1),
      analysisFailures = analyzerFailures.sortBy(_._1),
      stages = stages)
    snapshot.validateAccounting(ruleSets)
    snapshot
  }

  override def close(): Unit = synchronized {
    if (installed) {
      spark.sparkContext.removeSparkListener(queryListener)
      spark.sparkContext.removeSparkListener(runtimeListener)
      installed = false
    }
  }

  private def shouldReport(event: SparkListenerSQLExecutionEnd): Boolean = {
    event.executionName.isDefined &&
      event.qe != null &&
      (event.qe.sparkSession eq spark)
  }

  private def recordExecution(event: SparkListenerSQLExecutionEnd): Unit = {
    val executionId = event.executionId
    event.executionFailure match {
      case Some(error) =>
        synchronized {
          failedExecutions += ((executionId, error.getClass.getName))
        }
      case None =>
        val qe = event.qe
        try {
          val stableId = f"query-$executionId%020d"
          val runtimeState = stateFor(qe)
          val current = analyzePlan(qe.executedPlan, stableId, runtimeState)
          val executionStages = runtimeListener.snapshot().filter(_.executionId == executionId)
          val captured = qe.executedPlan match {
            case adaptive: AdaptiveSparkPlanExec =>
              appendMaterializedInitialPlan(
                current,
                analyzePlan(adaptive.initialPlan, stableId, runtimeState),
                executionStages)
            case _ => current
          }
          synchronized {
            completedExecutions += ShuffleRecoveryPendingExecution(
              executionId,
              captured.byRule,
              captured.runtimeKeys)
          }
        } catch {
          case NonFatal(error) =>
            synchronized {
              analysisFailures += ((executionId, error.getClass.getName))
            }
            logWarning(
              "Shuffle recovery opportunity study analysis failed; query is unaffected.",
              error)
        }
    }
  }

  private def analyzePlan(
      plan: SparkPlan,
      stableId: String,
      runtimeState: ShuffleRecoveryRuntimeState): AnalyzedPlan = {
    val byRule = ruleSets.map { ruleSet =>
      ruleSet -> ShuffleRecoveryOpportunityAnalyzer.analyze(
        plan, stableId, ruleSet.rules, runtimeState)
    }
    val keys = ShuffleRecoveryExchangeRuntimeKeys.fromPlan(plan)
    require(byRule.forall(_._2.size == keys.size),
      s"classification/runtime-key count mismatch for $stableId")
    AnalyzedPlan(byRule, keys)
  }

  /**
   * AQE may complete shuffle query stages and later remove their exchanges from the final plan.
   * Such task time is materially executed and must remain in the correlation denominator. Append
   * only initial-plan exchanges that match completed runtime stages and are no longer represented
   * by the final plan. Planned-but-unmaterialized initial exchanges are deliberately not appended.
   */
  private def appendMaterializedInitialPlan(
      current: AnalyzedPlan,
      initial: AnalyzedPlan,
      stages: Seq[ShuffleRecoveryStageRuntime]): AnalyzedPlan = {
    val completedStages = stages.filter(_.complete)
    val unrepresentedStages = completedStages.filterNot { stage =>
      current.runtimeKeys.exists(key => runtimeKeyMatchesStage(key, stage))
    }
    val historicalIndices = initial.runtimeKeys.indices.filter { index =>
      unrepresentedStages.exists(stage => runtimeKeyMatchesStage(initial.runtimeKeys(index), stage))
    }
    if (historicalIndices.isEmpty) {
      current
    } else {
      val firstHistoricalOrdinal = current.runtimeKeys.size.toLong
      val historicalKeys = historicalIndices.zipWithIndex.map { case (initialIndex, offset) =>
        val key = initial.runtimeKeys(initialIndex)
        key.copy(
          exchangeOrdinal = firstHistoricalOrdinal + offset,
          exchangePath = s"historical-initial.${key.exchangePath}")
      }
      val mergedByRule = current.byRule.zip(initial.byRule).map {
        case ((currentRule, currentObservations), (initialRule, initialObservations)) =>
          require(currentRule == initialRule, "current/initial rule-set order mismatch")
          val historicalObservations = historicalIndices.zipWithIndex.map {
            case (initialIndex, offset) =>
              val observation = initialObservations(initialIndex)
              observation.copy(
                exchangeOrdinal = firstHistoricalOrdinal + offset,
                exchangePath = s"historical-initial.${observation.exchangePath}",
                flags = observation.flags.copy(adaptivePlan = true))
          }
          currentRule -> (currentObservations ++ historicalObservations)
      }
      AnalyzedPlan(mergedByRule, current.runtimeKeys ++ historicalKeys)
    }
  }

  private def runtimeKeyMatchesStage(
      key: ShuffleRecoveryExchangeRuntimeKey,
      stage: ShuffleRecoveryStageRuntime): Boolean = {
    key.rddScopeId.exists(stage.rddScopeIds.contains) ||
      key.shuffleWriteMetricIds.exists(stage.accumulatorIds.contains)
  }

  private def stateFor(qe: QueryExecution): ShuffleRecoveryRuntimeState = {
    val sparkConf = qe.sparkSession.sparkContext.getConf
    val shuffleManagerClass = ShuffleManager.getShuffleManagerClassName(sparkConf)
    val defaultShuffleManagerClass = classOf[SortShuffleManager].getName
    ShuffleRecoveryRuntimeState(
      pushBasedShuffleEnabled = sparkConf.getBoolean("spark.shuffle.push.enabled", false),
      incompatibleFlags =
        if (shuffleManagerClass == defaultShuffleManagerClass) Nil
        else Seq("CUSTOM_SHUFFLE_MANAGER"))
  }
}

private[sql] case class ShuffleRecoveryOpportunityStudySnapshot(
    records: Seq[ShuffleRecoveryWeightedObservation],
    completedExecutionIds: Seq[Long],
    failedExecutions: Seq[(Long, String)],
    analysisFailures: Seq[(Long, String)],
    stages: Seq[ShuffleRecoveryStageRuntime]) {

  def validateAccounting(ruleSets: Seq[ShuffleRecoveryStudyRuleSet]): Unit = {
    require(analysisFailures.isEmpty,
      s"opportunity analysis failed for executions: ${analysisFailures.map(_._1).mkString(",")}")
    require(completedExecutionIds.distinct.size == completedExecutionIds.size,
      "duplicate completed SQL execution callbacks")
    val expectedRules = ruleSets.map(r => (r.rules.name, r.rules.version)).toSet
    val byExecution = records.groupBy(_.classification.executionId)
    byExecution.foreach { case (executionId, executionRecords) =>
      val grouped = executionRecords.groupBy(record =>
        (record.classification.ruleSetName, record.classification.ruleSetVersion))
      require(grouped.keySet == expectedRules,
        s"rule-set accounting mismatch for $executionId")
      val counts = grouped.valuesIterator.map(_.size).toSet
      require(counts.size <= 1, s"rule-set exchange counts disagree for $executionId")
      grouped.foreach { case ((name, version), values) =>
        val ordinals = values.map(_.classification.exchangeOrdinal)
        require(ordinals.distinct.size == ordinals.size,
          s"duplicate exchange ordinal for $executionId/$name/$version")
        val weightedPhysical = values.filter(
          _.disposition == ShuffleRecoveryWeightDisposition.Weighted).flatMap { record =>
          for {
            stageId <- record.stageId
            attemptId <- record.stageAttemptId
            shuffleId <- record.shuffleId
          } yield (stageId, attemptId, shuffleId)
        }
        require(weightedPhysical.distinct.size == weightedPhysical.size,
          s"physical shuffle work counted more than once for $executionId/$name/$version")
      }
    }
    records.foreach { record =>
      record.disposition match {
        case ShuffleRecoveryWeightDisposition.Weighted =>
          require(record.accountingReason.isEmpty, "weighted record cannot have accounting reason")
          require(Seq(
            record.stageId,
            record.stageAttemptId,
            record.shuffleId,
            record.mapperCount,
            record.shuffleWriteBytes,
            record.executorRunTimeMs,
            record.completionOrder).forall(_.nonEmpty),
            "weighted record has incomplete runtime correlation")
        case ShuffleRecoveryWeightDisposition.Unweighted |
            ShuffleRecoveryWeightDisposition.Excluded =>
          require(record.accountingReason.nonEmpty,
            "unweighted/excluded record must have stable accounting reason")
      }
    }
  }

  def deterministicJsonLines(ruleSets: Seq[ShuffleRecoveryStudyRuleSet]): Seq[String] = {
    validateAccounting(ruleSets)
    records.map(_.toJson)
  }
}
