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

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ShuffleRecoveryRuntimeCorrelationSuite extends SharedSparkSession {
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
      sourceTokenAvailability = ShuffleRecoverySourceTokenAvailability.Exact,
      lineageDeterminism = ShuffleRecoveryLineageDeterminism.Determinate,
      flags = ShuffleRecoveryObservationFlags(),
      pipelinedShuffle = false,
      pushBasedShuffleEnabled = false,
      mergedShuffleEnabled = false,
      incompatibleRuntimeFlags = Nil)
  }

  private def stage(
      shuffleId: Int,
      scopeId: String,
      accumulatorIds: Set[Long] = Set.empty,
      bytes: Long = 0L): ShuffleRecoveryStageRuntime = {
    ShuffleRecoveryStageRuntime(
      executionId = 1L,
      stageId = shuffleId + 10,
      stageAttemptId = 0,
      shuffleId = shuffleId,
      expectedMapTasks = 2,
      successfulMapTaskWinners = 2,
      shuffleWriteBytes = bytes,
      executorRunTimeMs = 25L,
      accumulatorIds = accumulatorIds,
      completionOrder = shuffleId.toLong + 1L,
      complete = true,
      invalidReason = None,
      rddScopeIds = Set(scopeId))
  }

  test("RDD scope correlates a completed zero-byte shuffle with no SQL metric updates") {
    val key = ShuffleRecoveryExchangeRuntimeKey(
      exchangeOrdinal = 0L,
      exchangePath = "root",
      shuffleWriteMetricIds = Set(7L),
      rddScopeId = Some("spark_plan_42"))
    val result = ShuffleRecoveryRuntimeCorrelator.correlate(
      Seq(observation()),
      Seq(key),
      Seq(stage(shuffleId = 3, scopeId = "spark_plan_42"))).head

    assert(result.disposition === Weighted)
    assert(result.shuffleId.contains(3))
    assert(result.shuffleWriteBytes.contains(0L))
    assert(result.executorRunTimeMs.contains(25L))
  }

  test("RDD scope and SQL metric disagreement fails closed as ambiguous") {
    val key = ShuffleRecoveryExchangeRuntimeKey(
      exchangeOrdinal = 0L,
      exchangePath = "root",
      shuffleWriteMetricIds = Set(7L),
      rddScopeId = Some("spark_plan_42"))
    val result = ShuffleRecoveryRuntimeCorrelator.correlate(
      Seq(observation()),
      Seq(key),
      Seq(
        stage(shuffleId = 3, scopeId = "spark_plan_42"),
        stage(shuffleId = 4, scopeId = "other", accumulatorIds = Set(7L)))).head

    assert(result.disposition === Unweighted)
    assert(result.accountingReason.contains(
      ShuffleRecoveryAccountingReason.AmbiguousRuntimeCorrelation))
  }

  test("real zero-output shuffle is correlated without forcing a metric update") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "4") {
      val study = new ShuffleRecoveryOpportunityStudy(spark, Seq(gateRule))
      study.install()
      try {
        spark.range(0L, 8L, 1L, 2)
          .filter(col("id") < 0L)
          .repartition(4, col("id"))
          .collect()
        val snapshot = study.snapshot()
        assert(snapshot.stages.nonEmpty)
        val materializedZeroByteStages = snapshot.stages.filter { runtime =>
          runtime.complete && runtime.shuffleWriteBytes == 0L
        }
        assert(materializedZeroByteStages.nonEmpty)
        assert(snapshot.records.nonEmpty)
        assert(snapshot.records.forall { record =>
          record.disposition != Unweighted ||
            !record.accountingReason.contains(ShuffleRecoveryAccountingReason.NoRuntimeCorrelation)
        })
        assert(snapshot.records.exists { record =>
          record.disposition == Weighted && record.shuffleWriteBytes.contains(0L)
        })
      } finally {
        study.close()
      }
    }
  }
}
