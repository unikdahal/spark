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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ShuffleRecoveryRuntimeCorrelationCoverageSuite extends SharedSparkSession {
  import ShuffleRecoveryLineageDeterminism._
  import ShuffleRecoverySourceTokenAvailability._
  import ShuffleRecoveryWeightDisposition._

  private val gateRule = ShuffleRecoveryStudyRuleSets.exactSourceCounterfactual

  private def observation(
      executionId: Long = 1L,
      ordinal: Long = 0L,
      path: String = "root"): ShuffleRecoveryExchangeObservation = {
    ShuffleRecoveryExchangeObservation(
      executionId = f"query-$executionId%020d",
      exchangeOrdinal = ordinal,
      exchangePath = path,
      childOperatorClass = "org.apache.spark.sql.execution.RangeExec",
      partitioningClass = "org.apache.spark.sql.catalyst.plans.physical.HashPartitioning",
      partitionCount = Some(4),
      ruleSetName = gateRule.rules.name,
      ruleSetVersion = gateRule.rules.version,
      eligible = true,
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
      stageId: Int,
      shuffleId: Int,
      accumulatorIds: Set[Long],
      rddScopeIds: Set[String]): ShuffleRecoveryStageRuntime = {
    ShuffleRecoveryStageRuntime(
      executionId = 1L,
      stageId = stageId,
      stageAttemptId = 0,
      shuffleId = shuffleId,
      expectedMapTasks = 1,
      successfulMapTaskWinners = 1,
      shuffleWriteBytes = 10L,
      executorRunTimeMs = 20L,
      accumulatorIds = accumulatorIds,
      completionOrder = stageId.toLong,
      complete = true,
      invalidReason = None,
      rddScopeIds = rddScopeIds)
  }

  test("RDD scope correlates a materialized exchange when SQL metric membership is absent") {
    val key = ShuffleRecoveryExchangeRuntimeKey(
      exchangeOrdinal = 0L,
      exchangePath = "root",
      shuffleWriteMetricIds = Set(7L),
      rddScopeId = "spark_plan_42")
    val runtime = stage(
      stageId = 10,
      shuffleId = 20,
      accumulatorIds = Set.empty,
      rddScopeIds = Set("spark_plan_42"))

    val result = ShuffleRecoveryRuntimeCorrelator.correlate(
      Seq(observation()), Seq(key), Seq(runtime)).head
    assert(result.disposition === Weighted)
    assert(result.stageId.contains(10))
    assert(result.shuffleId.contains(20))
  }

  test("conflicting RDD-scope and SQL-metric identities fail closed") {
    val key = ShuffleRecoveryExchangeRuntimeKey(
      exchangeOrdinal = 0L,
      exchangePath = "root",
      shuffleWriteMetricIds = Set(7L),
      rddScopeId = "spark_plan_42")
    val scopeStage = stage(10, 20, Set.empty, Set("spark_plan_42"))
    val metricStage = stage(11, 21, Set(7L), Set("spark_plan_99"))

    val result = ShuffleRecoveryRuntimeCorrelator.correlate(
      Seq(observation()), Seq(key), Seq(scopeStage, metricStage)).head
    assert(result.disposition === Unweighted)
    assert(result.accountingReason.contains("CORRELATION_KEY_CONFLICT"))
  }

  test("SQL metric correlation remains a fallback for synthetic records without RDD scopes") {
    val key = ShuffleRecoveryExchangeRuntimeKey(
      exchangeOrdinal = 0L,
      exchangePath = "root",
      shuffleWriteMetricIds = Set(7L))
    val runtime = stage(10, 20, Set(7L), Set.empty)

    val result = ShuffleRecoveryRuntimeCorrelator.correlate(
      Seq(observation()), Seq(key), Seq(runtime)).head
    assert(result.disposition === Weighted)
  }

  test("real multi-shuffle execution reconciles every completed shuffle stage") {
    val study = new ShuffleRecoveryOpportunityStudy(spark, Seq(gateRule))
    study.install()
    try {
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.SHUFFLE_PARTITIONS.key -> "4",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        val left = spark.range(0L, 64L, 1L, 4)
          .select((col("id") % 8).as("k"), col("id").as("left_id"))
        val right = spark.range(0L, 64L, 1L, 4)
          .select((col("id") % 8).as("k"), col("id").as("right_id"))
        left.join(right, "k")
          .groupBy("k")
          .agg(sum("left_id"), max("right_id"))
          .orderBy("k")
          .collect()
      }

      val snapshot = study.snapshot()
      assert(snapshot.stages.size >= 3, "test query must materialize multiple shuffle stages")
      val records = snapshot.records.filter(
        _.classification.ruleSetName == gateRule.rules.name)
      val weighted = records.filter(_.disposition == Weighted)
      val correlatedShuffleIds = weighted.flatMap(_.shuffleId).toSet
      val materializedShuffleIds = snapshot.stages.filter(_.complete).map(_.shuffleId).toSet
      assert(correlatedShuffleIds === materializedShuffleIds)
      assert(records.forall(_.disposition != Unweighted))
      assert(snapshot.stages.forall(_.rddScopeIds.nonEmpty))
    } finally {
      study.close()
    }
  }
}
