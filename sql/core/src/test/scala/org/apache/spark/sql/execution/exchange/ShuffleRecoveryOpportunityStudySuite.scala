/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ShuffleRecoveryOpportunityStudySuite extends SharedSparkSession {
  import ShuffleRecoveryLineageDeterminism._
  import ShuffleRecoveryMissReason._
  import ShuffleRecoverySourceTokenAvailability._
  import ShuffleRecoveryWeightDisposition._
  import testImplicits._

  private val gateRule = ShuffleRecoveryStudyRuleSets.exactSourceCounterfactual
  private val oneRule = Seq(gateRule)

  private def observation(
      executionId: Long = 1L,
      ordinal: Long = 0L,
      path: String = "root",
      eligible: Boolean = true,
      reason: ShuffleRecoveryMissReason = SourceTokenUnavailable,
      ruleName: String = gateRule.rules.name,
      ruleVersion: Int = gateRule.rules.version): ShuffleRecoveryExchangeObservation = {
    ShuffleRecoveryExchangeObservation(
      executionId = f"query-$executionId%020d",
      exchangeOrdinal = ordinal,
      exchangePath = path,
      childOperatorClass = "org.apache.spark.sql.execution.RangeExec",
      partitioningClass = "org.apache.spark.sql.catalyst.plans.physical.HashPartitioning",
      partitionCount = Some(4),
      ruleSetName = ruleName,
      ruleSetVersion = ruleVersion,
      eligible = eligible,
      immediateMissReason = if (eligible) None else Some(reason),
      rootMissReason = if (eligible) None else Some(reason),
      sourceTokenAvailability = Exact,
      lineageDeterminism = Determinate,
      flags = ShuffleRecoveryObservationFlags(),
      pipelinedShuffle = false,
      pushBasedShuffleEnabled = false,
      mergedShuffleEnabled = false,
      incompatibleRuntimeFlags = Nil)
  }

  private def stage(
      executionId: Long = 1L,
      stageId: Int = 10,
      attemptId: Int = 0,
      shuffleId: Int = 20,
      bytes: Long = 100L,
      runTimeMs: Long = 50L,
      accumulatorIds: Set[Long] = Set(7L),
      completionOrder: Long = 1L,
      complete: Boolean = true,
      invalidReason: Option[String] = None): ShuffleRecoveryStageRuntime = {
    ShuffleRecoveryStageRuntime(
      executionId,
      stageId,
      attemptId,
      shuffleId,
      expectedMapTasks = 4,
      successfulMapTaskWinners = if (complete) 4 else 3,
      bytes,
      runTimeMs,
      accumulatorIds,
      completionOrder,
      complete,
      invalidReason)
  }

  private def weighted(
      obs: ShuffleRecoveryExchangeObservation = observation(),
      stageId: Int = 10,
      attemptId: Int = 0,
      shuffleId: Int = 20,
      bytes: Long = 100L,
      runTimeMs: Long = 50L,
      completionOrder: Long = 1L): ShuffleRecoveryWeightedObservation = {
    ShuffleRecoveryWeightedObservation(
      obs,
      Weighted,
      accountingReason = None,
      Some(stageId),
      Some(attemptId),
      Some(shuffleId),
      mapperCount = Some(4),
      shuffleWriteBytes = Some(bytes),
      executorRunTimeMs = Some(runTimeMs),
      completionOrder = Some(completionOrder))
  }

  private def snapshot(
      records: Seq[ShuffleRecoveryWeightedObservation],
      completedExecutionIds: Seq[Long] = Seq(1L),
      failedExecutions: Seq[(Long, String)] = Nil): ShuffleRecoveryOpportunityStudySnapshot = {
    ShuffleRecoveryOpportunityStudySnapshot(
      records,
      completedExecutionIds,
      failedExecutions,
      analysisFailures = Nil,
      stages = Nil)
  }

  private def corpus(
      gateName: String = gateRule.rules.name,
      gateVersion: Int = gateRule.rules.version): ShuffleRecoveryCorpusDefinition = {
    ShuffleRecoveryCorpusDefinition(
      name = "unit",
      scale = "synthetic",
      baselineSha = ShuffleRecoveryOpportunityReportBuilder.FrozenBaselineSha,
      queries = Nil,
      sparkConfigs = Seq(SQLConf.SHUFFLE_PARTITIONS.key -> "4"),
      failureDistributionVersion = ShuffleRecoveryOpportunityReportBuilder.FailureDistributionVersion,
      gateRuleSetName = gateName,
      gateRuleSetVersion = gateVersion,
      gateThresholdBasisPoints = ShuffleRecoveryOpportunityReportBuilder.GateThresholdBasisPoints)
  }

  test("successful map winners are de-duplicated within an attempt and replaced by a later retry") {
    val accumulator = new ShuffleRecoveryStageAccumulator(
      executionId = 1L, stageId = 2, shuffleId = 3, expectedMapTasks = 2)
    accumulator.startAttempt(0)
    accumulator.recordSuccessfulTask(0, mapPartitionId = 0, shuffleWriteBytes = 10L, 11L)
    accumulator.recordSuccessfulTask(0, mapPartitionId = 0, shuffleWriteBytes = 999L, 999L)
    accumulator.startAttempt(1)
    accumulator.recordSuccessfulTask(1, mapPartitionId = 0, shuffleWriteBytes = 20L, 21L)
    accumulator.recordSuccessfulTask(1, mapPartitionId = 1, shuffleWriteBytes = 30L, 31L)

    val result = accumulator.finish(1, Set(7L), completionOrder = 1L)
    assert(result.complete)
    assert(result.successfulMapTaskWinners === 2)
    assert(result.shuffleWriteBytes === 50L)
    assert(result.executorRunTimeMs === 52L)
  }

  test("successful stage with incomplete map winner coverage is explicitly invalid") {
    val accumulator = new ShuffleRecoveryStageAccumulator(1L, 2, 3, expectedMapTasks = 2)
    accumulator.startAttempt(0)
    accumulator.recordSuccessfulTask(0, mapPartitionId = 0, shuffleWriteBytes = 10L, 5L)

    val result = accumulator.finish(0, Set.empty, completionOrder = 1L)
    assert(!result.complete)
    assert(result.invalidReason.contains("INCOMPLETE_MAP_WINNER_COVERAGE"))
  }

  test("runtime aggregation uses checked arithmetic") {
    val accumulator = new ShuffleRecoveryStageAccumulator(1L, 2, 3, expectedMapTasks = 2)
    accumulator.startAttempt(0)
    accumulator.recordSuccessfulTask(0, 0, Long.MaxValue, Long.MaxValue)
    accumulator.recordSuccessfulTask(0, 1, 1L, 1L)

    val result = accumulator.finish(0, Set.empty, completionOrder = 1L)
    assert(!result.complete)
    assert(result.invalidReason.contains("RUNTIME_METRIC_OVERFLOW"))
  }

  test("runtime correlation counts reused physical work once") {
    val observations = Seq(
      observation(ordinal = 0L, path = "c0"),
      observation(ordinal = 1L, path = "c1"))
    val keys = Seq(
      ShuffleRecoveryExchangeRuntimeKey(0L, "c0", Set(7L)),
      ShuffleRecoveryExchangeRuntimeKey(1L, "c1", Set(7L)))

    val result = ShuffleRecoveryRuntimeCorrelator.correlate(observations, keys, Seq(stage()))
    assert(result.map(_.disposition) === Seq(Weighted, Excluded))
    assert(result(1).accountingReason.contains("REUSED_PHYSICAL_WORK"))
  }

  test("missing and ambiguous runtime correlations are explicit unweighted buckets") {
    val obs = Seq(observation())
    val key = Seq(ShuffleRecoveryExchangeRuntimeKey(0L, "root", Set(7L)))
    val missing = ShuffleRecoveryRuntimeCorrelator.correlate(obs, key, Nil).head
    assert(missing.disposition === Unweighted)
    assert(missing.accountingReason.contains("NO_RUNTIME_CORRELATION"))

    val ambiguous = ShuffleRecoveryRuntimeCorrelator.correlate(
      obs,
      key,
      Seq(stage(stageId = 1), stage(stageId = 2, shuffleId = 21, completionOrder = 2L))).head
    assert(ambiguous.disposition === Unweighted)
    assert(ambiguous.accountingReason.contains("AMBIGUOUS_RUNTIME_CORRELATION"))
  }

  test("zero exchanges produce a valid empty report and N/A gate") {
    val report = ShuffleRecoveryOpportunityReportBuilder.build(snapshot(Nil), oneRule, corpus())
    assert(report.rules.size === 1)
    assert(report.rules.head.observedExchangeCount === 0L)
    assert(report.rules.head.byteRatio.render === "N/A")
    assert(report.rules.head.taskTimeRatio.render === "N/A")
    assert(report.valueGate.result.isEmpty)
  }

  test("zero byte and task-time denominators are N/A instead of zero percent") {
    val record = weighted(bytes = 0L, runTimeMs = 0L)
    val report = ShuffleRecoveryOpportunityReportBuilder.build(
      snapshot(Seq(record)), oneRule, corpus())
    assert(report.rules.head.countRatio.render === "100.0%")
    assert(report.rules.head.byteRatio.render === "N/A")
    assert(report.rules.head.taskTimeRatio.render === "N/A")
  }

  test("raw evidence parser rejects malformed and inconsistent records") {
    val json = weighted().toJson
    val parsed = ShuffleRecoveryOpportunityRawIO.parseLine(json)
    assert(parsed.schemaVersion === ShuffleRecoveryOpportunityRawIO.SchemaVersion)
    assert(parsed.disposition === "WEIGHTED")

    intercept[IllegalArgumentException] {
      ShuffleRecoveryOpportunityRawIO.parseLine(json.dropRight(1))
    }
    intercept[IllegalArgumentException] {
      ShuffleRecoveryOpportunityRawIO.parseLine(
        json.replace("\"shuffleWriteBytes\":100", "\"shuffleWriteBytes\":-1"))
    }
    intercept[IllegalArgumentException] {
      ShuffleRecoveryOpportunityRawIO.parseLine(
        json.dropRight(1) + ",\"unexpected\":1}")
    }
  }

  test("report generation is deterministic for input ordering") {
    val records = Seq(
      weighted(observation(ordinal = 0L), stageId = 10, completionOrder = 1L),
      weighted(observation(ordinal = 1L, eligible = false), stageId = 11,
        shuffleId = 21, completionOrder = 2L))
    val first = ShuffleRecoveryOpportunityReportBuilder.build(
      snapshot(records), oneRule, corpus()).toMarkdown
    val second = ShuffleRecoveryOpportunityReportBuilder.build(
      snapshot(records.reverse), oneRule, corpus()).toMarkdown
    assert(first === second)
  }

  test("10,000 weighted records aggregate without dense pairwise accounting") {
    val records = (0 until 10000).map { index =>
      weighted(
        observation(ordinal = index.toLong),
        stageId = index,
        shuffleId = index,
        bytes = 1L,
        runTimeMs = 1L,
        completionOrder = index.toLong + 1L)
    }
    val report = ShuffleRecoveryOpportunityReportBuilder.build(
      snapshot(records), oneRule, corpus())
    assert(report.rules.head.weightedExchangeCount === 10000L)
    assert(report.rules.head.weightedShuffleWriteBytes === 10000L)
    assert(report.rules.head.weightedExecutorRunTimeMs === 10000L)
  }

  test("rule-set version changes remain independently classified") {
    val v1 = gateRule
    val v2 = ShuffleRecoveryStudyRuleSet(
      gateRule.rules.copy(version = 2), gateRule.curveRole)
    val records = Seq(
      weighted(observation(ruleVersion = 1)),
      weighted(observation(ruleVersion = 2), stageId = 11, shuffleId = 21))
    val report = ShuffleRecoveryOpportunityReportBuilder.build(
      snapshot(records), Seq(v1, v2), corpus(gateVersion = 1))
    assert(report.rules.map(_.version) === Seq(1, 2))
  }

  test("failed SQL executions remain separate from completed opportunity") {
    val report = ShuffleRecoveryOpportunityReportBuilder.build(
      snapshot(Nil, completedExecutionIds = Nil,
        failedExecutions = Seq(9L -> "java.lang.RuntimeException")),
      oneRule,
      corpus())
    assert(report.failedExecutions.map(_._1) === Seq(9L))
    assert(report.rules.head.observedExchangeCount === 0L)
    assert(report.valueGate.result.isEmpty)
  }

  test("AQE on and off runtime correlation uses real successful shuffle task metrics") {
    val study = new ShuffleRecoveryOpportunityStudy(spark, oneRule)
    study.install()
    try {
      Seq(false, true).foreach { aqe =>
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
          SQLConf.SHUFFLE_PARTITIONS.key -> "4",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          spark.range(0, 2048, 1, 4)
            .selectExpr("id % 16 AS k", "id")
            .groupBy("k")
            .count()
            .orderBy("k")
            .collect()
        }
      }
      val result = study.snapshot()
      val weightedRecords = result.records.filter(_.disposition == Weighted)
      assert(result.completedExecutionIds.size === 2)
      assert(weightedRecords.nonEmpty)
      assert(weightedRecords.forall(_.shuffleWriteBytes.exists(_ >= 0L)))
      assert(weightedRecords.forall(_.executorRunTimeMs.exists(_ >= 0L)))
      assert(weightedRecords.exists(_.executorRunTimeMs.exists(_ > 0L)))
      assert(weightedRecords.map(r => (r.classification.executionId, r.stageId, r.shuffleId)).distinct.size ===
        weightedRecords.size)
    } finally {
      study.close()
    }
  }
}
