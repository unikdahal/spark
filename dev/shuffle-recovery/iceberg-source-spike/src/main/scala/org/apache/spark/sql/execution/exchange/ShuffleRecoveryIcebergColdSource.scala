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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.source.ShuffleRecoveryIcebergSourceSpike

import org.apache.spark.shuffle.{ShuffleRecoveryCanonicalInputs, ShuffleRecoveryColdProcessSource, ShuffleRecoveryFeasibilityIdentity, ShuffleRecoveryIdentityInputs, ShuffleRecoveryStringValue}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

/** Connector-specific test adapter; the normal Spark source roots have no Iceberg dependency. */
class ShuffleRecoveryIcebergColdSource extends ShuffleRecoveryColdProcessSource {
  private var prepared: Option[(ShuffleExchangeExec, ShuffleRecoveryCanonicalInputs)] = None

  private var providerFormat = ShuffleRecoveryFeasibilityIdentity.ProviderCompatibilityId

  def configureProviderReadFormat(value: String): Unit = {
    require(prepared.isEmpty, "configure the provider before planning the query")
    require(value != null && value.nonEmpty)
    providerFormat = value
  }

  override def sessionOptions: Map[String, String] = Map(
    "spark.sql.catalog.cold" -> "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.cold.type" -> "hadoop",
    "spark.sql.catalog.cold.warehouse" -> sys.env("SPARK_SHUFFLE_RECOVERY_TEST_ICEBERG_WAREHOUSE"))

  override def buildQuery(
      spark: SparkSession,
      rows: Long,
      mappers: Int,
      reducers: Int,
      payloadBytes: Int): DataFrame = {
    require(rows == 32 && payloadBytes == 0, "Iceberg cold proof uses the sparse fixture")
    prepared = None
    val source = spark.table("cold.db.input")
    val producer = if (sys.env.get("SPARK_SHUFFLE_RECOVERY_TEST_PRODUCER_FILTER")
        .contains("true")) source.where(col("id") >= lit(0L)) else source
    val query = producer.select(col("id"), col("id").as("k"), col("payload"))
      .repartition(reducers, col("k"))
      .select(col("id"), col("k"), col("payload"))
    // Capture the planning event before inspecting the plan. Only this adapter instance keeps
    // the certificate; each replacement JVM independently resolves and certifies its read.
    val inputs = ShuffleRecoveryIcebergSourceSpike.canonicalInputs(
      query, providerFormat).asInstanceOf[ShuffleRecoveryCanonicalInputs]
    val exchanges = query.queryExecution.executedPlan.collect {
      case exchange: ShuffleExchangeExec => exchange
    }
    require(exchanges.size == 1, "Iceberg cold proof requires one exchange")
    prepared = Some(exchanges.head -> inputs)
    query
  }

  override def identityInputs(
      exchange: ShuffleExchangeExec,
      sourceToken: String,
      providerReadFormatId: String): ShuffleRecoveryIdentityInputs = {
    val (plannedExchange, inputs) = prepared.getOrElse {
      throw new IllegalStateException("Iceberg query must be certified before identity lookup")
    }
    require(plannedExchange eq exchange, "certificate belongs to another planned exchange")
    require(inputs.computation.compatibility.providerReadFormatId == providerReadFormatId)
    // Preserve the harness's synthetic source-token control independently of real snapshot tests.
    ShuffleRecoveryCanonicalInputs(inputs.computation.copy(resolvedValues = Vector(
      "cold-proof-source-token" -> ShuffleRecoveryStringValue(sourceToken))))
  }
}

/** Creates or rewrites the durable source in a separate JVM, outside measured query attempts. */
object ShuffleRecoveryIcebergColdSourceSetup {
  def main(args: Array[String]): Unit = {
    require(args.length == 2 && Set("create", "rewrite").contains(args(0)))
    val builder = SparkSession.builder().master("local[2]")
      .appName("shuffle-recovery-iceberg-cold-setup").config("spark.ui.enabled", "false")
    new ShuffleRecoveryIcebergColdSource().sessionOptions.foreach {
      case (key, value) => builder.config(key, value)
    }
    val spark = builder.getOrCreate()
    try {
      if (args(0) == "create") {
        spark.sql("CREATE NAMESPACE cold.db")
        spark.sql("CREATE TABLE cold.db.input (id BIGINT, payload STRING) USING iceberg")
      }
      val data = spark.range(0L, 32L, 1L, 4).select(col("id"), lit("").as("payload"))
      if (args(0) == "create") data.writeTo("cold.db.input").append()
      else data.writeTo("cold.db.input").overwrite(lit(true))
      val table = Spark3Util.loadIcebergTable(spark, "cold.db.input")
      table.refresh()
      val snapshot = table.currentSnapshot()
      require(snapshot != null, "setup must produce a committed snapshot")
      Files.write(Paths.get(args(1)), snapshot.snapshotId().toString
        .getBytes(StandardCharsets.UTF_8))
    } finally {
      spark.stop()
    }
  }
}
