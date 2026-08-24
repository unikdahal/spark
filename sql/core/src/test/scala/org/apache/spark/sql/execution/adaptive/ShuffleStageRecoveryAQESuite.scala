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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.MapOutputStatistics
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.execution.{CoalescedPartitionSpec, PartialReducerPartitionSpec}
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ShuffleStageRecoveryAQESuite
  extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  test("recovered statistics and map output are shared by reused shuffle stages") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "4") {
      val adaptive = spark.range(8).repartition(4).queryExecution.executedPlan
        .asInstanceOf[AdaptiveSparkPlanExec]
      val exchange = adaptive.initialPlan.collectFirst {
        case shuffle: ShuffleExchangeExec => shuffle
      }.getOrElse(fail("expected a shuffle exchange"))
      val stage = ShuffleQueryStageExec(1, exchange, exchange.canonicalized)
      val mapStats = new MapOutputStatistics(exchange.shuffleId, Array(11L, 22L, 33L, 44L))
      val runtimeStats =
        Statistics(sizeInBytes = BigInt(110L), rowCount = Some(BigInt(8L)), isRuntime = true)
      stage.resultOption.set(Some(mapStats))
      stage.setRecoveredRuntimeStatistics(runtimeStats)

      val reused = stage.newReuseInstance(2, stage.output.map(_.newInstance()))
        .asInstanceOf[ShuffleQueryStageExec]

      assert(reused.plan.isInstanceOf[ReusedExchangeExec])
      assert(reused.resultOption eq stage.resultOption)
      assert(reused.mapStats.contains(mapStats))
      assert(reused.getRuntimeStatistics === runtimeStats)
    }
  }

  test("AQE coalesced and skew reads retain a recovered shuffle stage") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "4") {
      val adaptive = spark.range(8).repartition(4).queryExecution.executedPlan
        .asInstanceOf[AdaptiveSparkPlanExec]
      val exchange = adaptive.initialPlan.collectFirst {
        case shuffle: ShuffleExchangeExec => shuffle
      }.getOrElse(fail("expected a shuffle exchange"))
      val stage = ShuffleQueryStageExec(1, exchange, exchange.canonicalized)
      val mapStats = new MapOutputStatistics(exchange.shuffleId, Array(10L, 20L, 30L, 40L))
      stage.resultOption.set(Some(mapStats))
      stage.setRecoveredRuntimeStatistics(
        Statistics(sizeInBytes = BigInt(100L), rowCount = Some(BigInt(8L)), isRuntime = true))

      val coalesced = AQEShuffleReadExec(
        stage,
        Seq(CoalescedPartitionSpec(0, 2, 30L), CoalescedPartitionSpec(2, 4, 70L)))
      assert(coalesced.hasCoalescedPartition)
      assert(!coalesced.hasSkewedPartition)
      assert(coalesced.child eq stage)

      val skewed = AQEShuffleReadExec(
        stage,
        Seq(
          PartialReducerPartitionSpec(2, 0, 2, 15L),
          PartialReducerPartitionSpec(2, 2, 4, 15L)))
      assert(skewed.hasSkewedPartition)
      assert(!skewed.hasCoalescedPartition)
      assert(skewed.child eq stage)
      assert(stage.mapStats.contains(mapStats))
    }
  }
}
