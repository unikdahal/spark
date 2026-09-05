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

import org.apache.spark.SparkFunSuite

class ShuffleRecoveryFailureDistributionSuite extends SparkFunSuite {
  import ShuffleRecoveryLineageDeterminism._
  import ShuffleRecoveryMissReason._
  import ShuffleRecoverySourceTokenAvailability._
  import ShuffleRecoveryWeightDisposition._

  private val gateRule = ShuffleRecoveryStudyRuleSets.exactSourceCounterfactual

  private def observation(ordinal: Long, eligible: Boolean): ShuffleRecoveryExchangeObservation = {
    ShuffleRecoveryExchangeObservation(
      executionId = "query-00000000000000000001",
      exchangeOrdinal = ordinal,
      exchangePath = s"c$ordinal",
      childOperatorClass = "org.apache.spark.sql.execution.RangeExec",
      partitioningClass = "org.apache.spark.sql.catalyst.plans.physical.HashPartitioning",
      partitionCount = Some(4),
      ruleSetName = gateRule.rules.name,
      ruleSetVersion = gateRule.rules.version,
      eligible = eligible,
      immediateMissReason = if (eligible) None else Some(WindowPresent),
      rootMissReason = if (eligible) None else Some(WindowPresent),
      sourceTokenAvailability = Exact,
      lineageDeterminism = Determinate,
      flags = ShuffleRecoveryObservationFlags(window = !eligible),
      pipelinedShuffle = false,
      pushBasedShuffleEnabled = false,
      mergedShuffleEnabled = false,
      incompatibleRuntimeFlags = Nil)
  }

  private def weighted(
      ordinal: Long,
      eligible: Boolean,
      runTimeMs: Long): ShuffleRecoveryWeightedObservation = {
    ShuffleRecoveryWeightedObservation(
      observation(ordinal, eligible),
      Weighted,
      accountingReason = None,
      stageId = Some(ordinal.toInt + 10),
      stageAttemptId = Some(0),
      shuffleId = Some(ordinal.toInt + 20),
      mapperCount = Some(4),
      shuffleWriteBytes = Some(runTimeMs),
      executorRunTimeMs = Some(runTimeMs),
      completionOrder = Some(ordinal + 1L))
  }

  private def corpus: ShuffleRecoveryCorpusDefinition = {
    ShuffleRecoveryCorpusDefinition(
      name = "evidence-unit",
      scale = "synthetic",
      baselineSha = ShuffleRecoveryOpportunityReportBuilder.FrozenBaselineSha,
      queries = Nil,
      sparkConfigs = Nil,
      failureDistributionVersion =
        ShuffleRecoveryOpportunityReportBuilder.EvidenceFailureDistributionVersion,
      gateRuleSetName = gateRule.rules.name,
      gateRuleSetVersion = gateRule.rules.version,
      gateThresholdBasisPoints = ShuffleRecoveryOpportunityReportBuilder.GateThresholdBasisPoints)
  }

  test("four-point evidence distribution adds mixed eligible and ineligible restart point") {
    val records = Seq(
      weighted(ordinal = 0L, eligible = true, runTimeMs = 10L),
      weighted(ordinal = 1L, eligible = false, runTimeMs = 20L),
      weighted(ordinal = 2L, eligible = true, runTimeMs = 30L))
    val snapshot = ShuffleRecoveryOpportunityStudySnapshot(
      records,
      completedExecutionIds = Seq(1L),
      failedExecutions = Nil,
      analysisFailures = Nil,
      stages = Nil)

    val report = ShuffleRecoveryOpportunityReportBuilder.build(snapshot, Seq(gateRule), corpus)
    val byPoint = report.failurePoints.map(point => point.point -> point).toMap
    assert(report.failurePoints.size === 4)
    assert(report.valueGate.distributionVersion === "equal-four-points-v2")

    val mixed = byPoint("AFTER_ELIGIBLE_INELIGIBLE_MIX")
    assert(mixed.applicableExecutions === 1L)
    assert(mixed.completedExchangeCount === 2L)
    assert(mixed.eligibleCompletedExchangeCount === 1L)
    assert(mixed.completedExecutorRunTimeMs === 30L)
    assert(mixed.avoidableExecutorRunTimeMs === 10L)

    assert(report.valueGate.completedExecutorRunTimeMs === 100L)
    assert(report.valueGate.reusableExecutorRunTimeMs === 40L)
    assert(report.valueGate.result.contains(true))
  }

  test("mixed restart point is N/A when an execution has no eligibility mixture") {
    val records = Seq(
      weighted(ordinal = 0L, eligible = true, runTimeMs = 10L),
      weighted(ordinal = 1L, eligible = true, runTimeMs = 20L))
    val snapshot = ShuffleRecoveryOpportunityStudySnapshot(
      records,
      completedExecutionIds = Seq(1L),
      failedExecutions = Nil,
      analysisFailures = Nil,
      stages = Nil)

    val report = ShuffleRecoveryOpportunityReportBuilder.build(snapshot, Seq(gateRule), corpus)
    val mixed = report.failurePoints.find(_.point == "AFTER_ELIGIBLE_INELIGIBLE_MIX").get
    assert(mixed.applicableExecutions === 0L)
    assert(mixed.taskTimeRatio.render === "N/A")
  }
}
