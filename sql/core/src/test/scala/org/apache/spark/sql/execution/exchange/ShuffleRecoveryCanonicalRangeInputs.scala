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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.{ShuffleRecoveryCanonicalInputs, ShuffleRecoveryMapperDecomposition, ShuffleRecoveryMapperSplit, ShuffleRecoverySourceToken}
import org.apache.spark.sql.execution.RangeExec

/**
 * Test adapter for the deterministic built-in Range source. It feeds the same canonical identity
 * path intended for connector certificates; it does not introduce a source-name switch in Core.
 */
private[spark] object ShuffleRecoveryCanonicalRangeInputs {
  def build(
      exchange: ShuffleExchangeExec,
      source: String,
      providerReadFormatId: String): ShuffleRecoveryCanonicalInputs = {
    val ranges = exchange.child.collect { case range: RangeExec => range }
    require(ranges.size == 1, "canonical range proof requires exactly one Range source")
    val range = ranges.head
    val partitions = exchange.shuffleDependency.rdd.partitions
    require(partitions.length == range.numSlices,
      "canonical range proof requires a one-to-one source mapper decomposition")
    val decomposition = ShuffleRecoveryMapperDecomposition(partitions.length,
      partitions.toVector.map { partition =>
        ShuffleRecoveryMapperSplit.copyOf(0, partition.index, 1,
          ByteBuffer.allocate(4).putInt(partition.index).array())
      })
    val conf = SparkEnv.get.conf
    val inputs = ShuffleRecoveryResolvedIdentityInputs.create(
      Seq(range -> ShuffleRecoverySourceToken.forProtocol(
        "org.apache.spark.range", 1, source.getBytes(StandardCharsets.UTF_8))),
      decomposition,
      Map.empty,
      ShuffleRecoveryIdentitySemanticConfig(
        exchange.conf.ansiEnabled,
        exchange.conf.sessionLocalTimeZone,
        conf.getBoolean("spark.shuffle.compress", true),
        conf.get("spark.io.compression.codec", "lz4"),
        conf.getSizeAsBytes("spark.io.compression.lz4.blockSize", "32k").toInt,
        conf.getBoolean("spark.io.encryption.enabled", false)),
      providerReadFormatId)
    ShuffleRecoveryComputationIdentityBuilder.build(exchange, inputs) match {
      case ShuffleRecoveryIdentityBuilt(identity) => ShuffleRecoveryCanonicalInputs(identity)
      case ShuffleRecoveryIdentityRejected(reason) =>
        throw new IllegalArgumentException(s"canonical range proof was refused: $reason")
    }
  }
}
