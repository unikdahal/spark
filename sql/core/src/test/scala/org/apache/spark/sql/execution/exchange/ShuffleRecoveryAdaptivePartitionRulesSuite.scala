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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ShuffleRecoveryAdaptivePartitionRulesSuite extends SharedSparkSession {
  import ShuffleRecoveryMissReason._
  import testImplicits._

  private val rules = ShuffleRecoveryEligibilityRules.conservative

  test("adaptive shuffle reads are gated only by the adaptive partition-spec rule") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      "spark.sql.adaptive.coalescePartitions.enabled" -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "8") {
      val df = spark.range(0, 64, 1, 4)
        .groupBy(($"id" % 4).as("bucket"))
        .count()
      df.collect()

      val strict = ShuffleRecoveryOpportunityAnalyzer.analyze(
        df.queryExecution.executedPlan,
        executionId = "adaptive-strict",
        rules = rules)
      val strictAdaptive = strict.filter(_.flags.adaptivePartitionSpecs)
      assert(strictAdaptive.nonEmpty, "expected AQE shuffle-read partition specs in final plan")
      assert(strictAdaptive.exists(_.rootMissReason.contains(AdaptivePartitionSpecPresent)))

      val relaxed = ShuffleRecoveryOpportunityAnalyzer.analyze(
        df.queryExecution.executedPlan,
        executionId = "adaptive-relaxed",
        rules = rules.copy(allowAdaptivePartitionSpecs = true))
      val relaxedByPath = relaxed.iterator.map(record => record.exchangePath -> record).toMap

      strictAdaptive.foreach { strictRecord =>
        val relaxedRecord = relaxedByPath.getOrElse(
          strictRecord.exchangePath,
          fail(s"missing relaxed observation for ${strictRecord.exchangePath}"))
        assert(!relaxedRecord.immediateMissReason.contains(AdaptivePartitionSpecPresent))
        assert(!relaxedRecord.rootMissReason.contains(AdaptivePartitionSpecPresent))
        assert(!relaxedRecord.immediateMissReason.contains(UnsupportedOperator))
        assert(!relaxedRecord.rootMissReason.contains(UnsupportedOperator))
      }
      assert(strictAdaptive.exists(record => relaxedByPath(record.exchangePath).eligible))
    }
  }
}
