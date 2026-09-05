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

class ShuffleRecoveryOpportunityEvidenceSuite extends SparkFunSuite {
  import ShuffleRecoveryLineageDeterminism._
  import ShuffleRecoverySourceTokenAvailability._
  import ShuffleRecoveryWeightDisposition._

  private val gateRule = ShuffleRecoveryStudyRuleSets.exactSourceCounterfactual

  private def observation(
      ordinal: Long,
      eligible: Boolean,
      executionId: Long = 1L): ShuffleRecoveryExchangeObservation = {
    ShuffleRecoveryExchangeObservation(
      executionId = f"query-$executionId%020d",
      exchangeOrdinal = ordinal,
      exchangePath = s"c$ordinal",
      childOperatorClass = "org.apache.spark.sql.execution.RangeExec",
      partitioningClass = "org.apache.spark.sql.catalyst.plans.physical.HashPartitioning",
      partitionCount = Some(4),
      ruleSetName = gateRule.rules.name,
      ruleSetVersion = gateRule.rules.version,
      eligible = eligible,
      immediateMissReason = None,
      rootMissReason = None,
      sourceTokenAvailability = Exact,
      lineageDeterminism = Determinate,
      flags = ShuffleRecoveryObservationFlags(),
      pipelinedShuffle = false,
      pushBasedShuffleEnabled = false,
      mergedShuffleEnabled = false,
      incompatibleRuntimeFlags = Nil)
  }

  private def stage(
      shuffleId: Int,
      completionOrder: Long,
      runTimeMs: Long): ShuffleRecoveryStageRuntime = {
    ShuffleRecoveryStageRuntime(
      executionId = 1L,
      stageId = shuffleId + 100,
      stageAttemptId = 0,
      shuffleId = shuffleId,
      expectedMapTasks = 1,
      successfulMapTaskWinners = 1,
      shuffleWriteBytes = runTimeMs * 2L,
      executorRunTimeMs = runTimeMs,
      accumulatorIds = Set.empty,
      completionOrder = completionOrder,
      complete = true,
      invalidReason = None,
      rddScopeIds = Set(s"spark_plan_$shuffleId"),
      observedSuccessfulMapTaskCompletions = 1L)
  }

  private def weighted(
      ordinal: Long,
      shuffleId: Int,
      eligible: Boolean,
      runTimeMs: Long): ShuffleRecoveryWeightedObservation = {
    ShuffleRecoveryWeightedObservation(
      classification = observation(ordinal, eligible),
      disposition = Weighted,
      accountingReason = None,
      stageId = Some(shuffleId + 100),
      stageAttemptId = Some(0),
      shuffleId = Some(shuffleId),
      mapperCount = Some(1),
      shuffleWriteBytes = Some(runTimeMs * 2L),
      executorRunTimeMs = Some(runTimeMs),
      completionOrder = Some(ordinal + 1L))
  }

  private def snapshot(
      records: Seq[ShuffleRecoveryWeightedObservation],
      stages: Seq[ShuffleRecoveryStageRuntime]): ShuffleRecoveryOpportunityStudySnapshot = {
    ShuffleRecoveryOpportunityStudySnapshot(
      records = records,
      completedExecutionIds = Seq(1L),
      failedExecutions = Nil,
      analysisFailures = Nil,
      stages = stages)
  }

  private val query = Map(1L -> ShuffleRecoveryCorpusQuery("synthetic", "q", aqeEnabled = false))

  test("correlation gate denominator includes materialized stages with no correlated exchange") {
    val correlated = weighted(ordinal = 0L, shuffleId = 1, eligible = true, runTimeMs = 50L)
    val stages = Seq(stage(1, 1L, 50L), stage(2, 2L, 50L))

    val evidence = ShuffleRecoveryOpportunityEvidence.build(
      snapshot(Seq(correlated), stages), Seq(correlated), query)

    assert(evidence.correlation.materializedExchangeCount === 2L)
    assert(evidence.correlation.correlatedExchangeCount === 1L)
    assert(evidence.correlation.taskTimeRatio.render === "50.0%")
    assert(evidence.correlationGatePass.contains(false))
    assert(evidence.finalValueGateResult.isEmpty)
  }

  test("four-point distribution includes the preregistered mixed-work restart point") {
    val records = Seq(
      weighted(0L, 1, eligible = true, runTimeMs = 10L),
      weighted(1L, 2, eligible = false, runTimeMs = 20L),
      weighted(2L, 3, eligible = true, runTimeMs = 30L),
      weighted(3L, 4, eligible = false, runTimeMs = 40L))
    val stages = records.zipWithIndex.map { case (record, index) =>
      stage(record.shuffleId.get, index.toLong + 1L, record.executorRunTimeMs.get)
    }

    val evidence = ShuffleRecoveryOpportunityEvidence.build(
      snapshot(records, stages), records, query)
    val mixed = evidence.failurePoints.find(
      _.point == "AFTER_ELIGIBLE_INELIGIBLE_MIX").get

    assert(evidence.failureDistributionVersion === "equal-four-points-v2")
    assert(evidence.correlationGatePass.contains(true))
    assert(mixed.applicable)
    assert(mixed.completedExchangeCount === 2L)
    assert(mixed.reusableExchangeCount === 1L)
    assert(mixed.completedExecutorRunTimeMs === 30L)
    assert(mixed.reusableExecutorRunTimeMs === 10L)
    assert(evidence.finalValueGateResult.nonEmpty)
  }

  test("sensitivity analysis is deterministic and query-weighting is not exchange IID inference") {
    val records = Seq(
      weighted(0L, 1, eligible = true, runTimeMs = 70L),
      weighted(1L, 2, eligible = false, runTimeMs = 20L),
      weighted(2L, 3, eligible = true, runTimeMs = 10L))
    val stages = records.zipWithIndex.map { case (record, index) =>
      stage(record.shuffleId.get, index.toLong + 1L, record.executorRunTimeMs.get)
    }

    val first = ShuffleRecoveryOpportunityEvidence.build(snapshot(records, stages), records, query)
    val second = ShuffleRecoveryOpportunityEvidence.build(snapshot(records, stages), records, query)

    assert(first.toMarkdown === second.toMarkdown)
    assert(first.taskTimeOpportunity.render === "80.0%")
    assert(first.topExchangeTaskTimeShare.render === "70.0%")
    assert(first.withoutTopExchange.render === "33.3%")
    assert(first.toMarkdown.contains("not IID statistical samples"))
  }
}
