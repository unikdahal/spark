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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.functions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * A recovery provider derives its durable stage key from the canonicalized plans handed to it in
 * [[ShuffleStageRecoveryInfo]]. Spark supplies collision-resistant fingerprints of both the
 * canonical query plan and canonical stage plan, so providers do not need to render plans.
 *
 * `TreeNode.treeString` renders at most `spark.sql.debug.maxToStringFields` (default 25)
 * elements of a sequence-like field and replaces the rest with a `"... N more fields"`
 * placeholder. On a wide
 * table, the elided elements are exactly the ones that distinguish two otherwise identical stages,
 * so a plan-string-derived key can collide across queries that produce different output. Adopting a
 * shuffle under a colliding key is a false-positive recovery: a replacement driver reuses output
 * that answers a different question.
 *
 * These tests pin that discrimination requirement down. They do not test a specific provider; they
 * test the property any provider must be able to rely on from the material Spark gives it.
 */
class RecoveryKeyDiscriminationSuite extends QueryTest with SharedSparkSession {

  private val numColumns = 40

  private def wideDf() = {
    val base = spark.range(0, 32).toDF("id")
    val columns = (0 until numColumns).map(i => (base("id") + i).as(s"c$i"))
    base.select(columns: _*)
  }

  /** A provider key assembled only from immutable values supplied by Spark. */
  private def recoveryKey(
      recoveryId: String,
      numMappers: Int,
      numPartitions: Int,
      canonicalizedQueryPlan: SparkPlan,
      canonicalizedPlan: SparkPlan): String = {
    recoveryId + "/" + spark.version + "/" + numMappers + "/" + numPartitions + "/" +
      ShuffleStageRecovery.fingerprint(canonicalizedQueryPlan) + "/" +
      ShuffleStageRecovery.fingerprint(canonicalizedPlan)
  }

  /**
   * A stage whose plan genuinely depends on every column: one aggregate expression per column, so
   * column pruning cannot collapse two different projections into the same plan. `tail` names the
   * expression for the last column, which is where the two variants differ -- past the
   * `maxToStringFields` cut-off of the plan string.
   */
  private def shuffleStagePlan(tail: String): SparkPlan = {
    val aggregates =
      ((1 until numColumns - 1).map(i => s"sum(c$i) as s$i") :+ tail).map(functions.expr)
    val df = wideDf().groupBy("c0").agg(aggregates.head, aggregates.tail: _*)
    df.queryExecution.sparkPlan
  }

  test("a canonicalized plan string truncates sequence-like fields") {
    val plan = shuffleStagePlan(s"sum(c${numColumns - 1}) as tail")
    val rendered = plan.canonicalized.treeString
    // Documents the mechanism the next test depends on: with more fields than
    // spark.sql.debug.maxToStringFields, the plan string is lossy.
    assert(SQLConf.get.maxToStringFields < numColumns)
    assert(rendered.contains("more fields"),
      s"expected a truncation placeholder in a $numColumns-column plan string:\n$rendered")
  }

  test("recovery keys must discriminate plans that differ past maxToStringFields") {
    val planA = shuffleStagePlan(s"sum(c${numColumns - 1}) as tail")
    val planB = shuffleStagePlan(s"sum(c${numColumns - 1} + 1) as tail")

    val keyA = recoveryKey("RUN-1", 8, 4, planA.canonicalized, planA.canonicalized)
    val keyB = recoveryKey("RUN-1", 8, 4, planB.canonicalized, planB.canonicalized)

    assert(keyA != keyB,
      "two stages whose only difference lies past spark.sql.debug.maxToStringFields produced the " +
        "same recovery key; a replacement driver would adopt shuffle output computed for a " +
        "different projection")
  }

  test("recovery fingerprints are independent of maxToStringFields") {
    val planA = shuffleStagePlan(s"sum(c${numColumns - 1}) as tail")
    val planB = shuffleStagePlan(s"sum(c${numColumns - 1} + 1) as tail")
    val truncatedA = ShuffleStageRecovery.fingerprint(planA.canonicalized)
    val truncatedB = ShuffleStageRecovery.fingerprint(planB.canonicalized)

    withSQLConf(SQLConf.MAX_TO_STRING_FIELDS.key -> Int.MaxValue.toString) {
      assert(ShuffleStageRecovery.fingerprint(planA.canonicalized) === truncatedA)
      assert(ShuffleStageRecovery.fingerprint(planB.canonicalized) === truncatedB)
    }
  }

  test("representative canonical plans have deterministic distinct recovery fingerprints") {
    val representativePlans = Seq[(String, () => SparkPlan)](
      "filter" -> (() => spark.range(32).where("id % 3 = 0").queryExecution.sparkPlan),
      "aggregate" -> (() => spark.range(32)
        .selectExpr("id % 4 AS bucket", "id")
        .groupBy("bucket")
        .sum("id")
        .queryExecution.sparkPlan),
      "shuffle" -> (() => spark.range(32).repartition(4).queryExecution.sparkPlan))

    val firstConstruction = representativePlans.map { case (name, createPlan) =>
      name -> ShuffleStageRecovery.fingerprint(createPlan().canonicalized)
    }
    val independentConstruction = representativePlans.map { case (name, createPlan) =>
      name -> ShuffleStageRecovery.fingerprint(createPlan().canonicalized)
    }

    assert(firstConstruction === independentConstruction)
    assert(firstConstruction.map(_._2).distinct.size === firstConstruction.size)
    firstConstruction.foreach { case (_, fingerprint) =>
      assert(fingerprint.matches("[0-9a-f]{64}"))
    }
    assert(ShuffleStageRecovery.PROTOCOL_VERSION === 1,
      "changing the canonical fingerprint contract requires a recovery protocol version bump")
  }
}
