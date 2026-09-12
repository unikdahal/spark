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

package org.apache.spark.shuffle

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.exchange.{ShuffleExchangeExec, ShuffleRecoveryCanonicalRangeInputs}
import org.apache.spark.sql.functions.{col, lit}

/**
 * Test-only source adapter loaded independently in each cold-process proof child. Implementations
 * must reconstruct reads from durable source facts and capture certificates while planning the
 * returned query, before the harness inspects its exchange. No adapter state crosses processes.
 */
private[spark] trait ShuffleRecoveryColdProcessSource {
  def sessionOptions: Map[String, String] = Map.empty

  /** Returns id: long, k: long, payload: string with exactly one shuffle on k. */
  def buildQuery(
      spark: SparkSession,
      rows: Long,
      mappers: Int,
      reducers: Int,
      payloadBytes: Int): DataFrame

  /** The source token argument is a harness discriminator used by the source-change control. */
  def identityInputs(
      exchange: ShuffleExchangeExec,
      sourceToken: String,
      providerReadFormatId: String): ShuffleRecoveryIdentityInputs
}

/** Exercises the adapter path in the existing remote-executor shared-filesystem proof. */
private[spark] final class ShuffleRecoveryColdRangeSource extends ShuffleRecoveryColdProcessSource {
  override def buildQuery(
      spark: SparkSession,
      rows: Long,
      mappers: Int,
      reducers: Int,
      payloadBytes: Int): DataFrame = {
    require(rows > 0 && payloadBytes == 0, "range adapter requires nonempty payload-free input")
    val range = spark.range(0L, rows, 1L, mappers)
    val producer = if (sys.env.get("SPARK_SHUFFLE_RECOVERY_TEST_PRODUCER_FILTER")
        .contains("true")) {
      range.where(col("id") >= lit(0L))
    } else {
      range
    }
    producer.select(col("id"), col("id").as("k"), lit("").as("payload"))
      .repartition(reducers, col("k"))
      .select(col("id"), col("k"), col("payload"))
  }

  override def identityInputs(
      exchange: ShuffleExchangeExec,
      sourceToken: String,
      providerReadFormatId: String): ShuffleRecoveryIdentityInputs = {
    ShuffleRecoveryCanonicalRangeInputs.build(exchange, sourceToken, providerReadFormatId)
  }
}
