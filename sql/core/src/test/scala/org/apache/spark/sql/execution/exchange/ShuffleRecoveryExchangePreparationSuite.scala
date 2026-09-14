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

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.{CoalescedPartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.test.SharedSparkSession

class ShuffleRecoveryExchangePreparationSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private def nodes(plan: SparkPlan): Seq[SparkPlan] = {
    val children = plan match {
      case adaptive: AdaptiveSparkPlanExec => Seq(adaptive.executedPlan)
      case stage: QueryStageExec => Seq(stage.plan)
      case other => other.children
    }
    plan +: children.flatMap(nodes)
  }

  for (coalesce <- Seq(false, true)) {
    test(s"prepare the actual AQE exchange once before materialization, coalescing=$coalesce") {
      withSQLConf(
        "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.shuffle.partitions" -> "4",
        "spark.sql.adaptive.coalescePartitions.enabled" -> coalesce.toString,
        "spark.sql.adaptive.coalescePartitions.parallelismFirst" -> "false",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize" -> "1") {
        val query = spark.range(0, 32, 1, 4).toDF("id").repartition($"id")
        val adaptive = query.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
        val initial = adaptive.executedPlan.collect { case e: ShuffleExchangeExec => e }.head
        val calls = new AtomicInteger
        val prepared = new AtomicReference[ShuffleExchangeExec]
        ShuffleRecoveryExchangePreparation.attach(initial) { exchange =>
          assert(!adaptive.isFinalPlan)
          assert(exchange.shuffleDependency.rdd.getNumPartitions == 4)
          prepared.set(exchange)
          calls.incrementAndGet()
        }
        assert(query.collect().map(_.getLong(0)).sorted.toSeq == (0L until 32L))
        assert(calls.get() == 1)
        val finalExchanges = nodes(adaptive).collect { case e: ShuffleExchangeExec => e }
        assert(finalExchanges.exists(_ eq prepared.get()))
        if (coalesce) {
          val reads = nodes(adaptive).collect { case r: AQEShuffleReadExec => r }
          assert(reads.flatMap(_.partitionSpecs).exists {
            case p: CoalescedPartitionSpec => p.endReducerIndex - p.startReducerIndex > 1
            case _ => false
          })
        }
        query.collect()
        assert(calls.get() == 1, "already materialized exchange prepared again")
      }
    }
  }
}
