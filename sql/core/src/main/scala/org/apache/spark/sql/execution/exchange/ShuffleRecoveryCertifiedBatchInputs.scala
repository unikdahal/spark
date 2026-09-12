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

import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.ShuffleRecoveryCanonicalInputs

/** Converts a certified planned batch read into inputs shared by publication and recovery. */
private[sql] object ShuffleRecoveryCertifiedBatchInputs {
  def build(
      exchange: ShuffleExchangeExec,
      binding: ShuffleRecoverySourceBinding,
      providerReadFormatId: String)
      : Either[ShuffleRecoveryMissReason, ShuffleRecoveryCanonicalInputs] = {
    if (exchange == null || binding == null || !binding.isCurrent(binding.plan)) {
      return Left(ShuffleRecoveryMissReason.SourceTokenUnavailable)
    }
    val conf = SparkEnv.get.conf
    val inputs = ShuffleRecoveryResolvedIdentityInputs.create(
      Nil, binding.decomposition, Map.empty,
      ShuffleRecoveryIdentitySemanticConfig(
        exchange.conf.ansiEnabled,
        exchange.conf.sessionLocalTimeZone,
        conf.getBoolean("spark.shuffle.compress", true),
        conf.get("spark.io.compression.codec", "lz4"),
        conf.getSizeAsBytes("spark.io.compression.lz4.blockSize", "32k").toInt,
        conf.getBoolean("spark.io.encryption.enabled", false)),
      providerReadFormatId, Some(binding))
    ShuffleRecoveryComputationIdentityBuilder.build(exchange, inputs) match {
      case ShuffleRecoveryIdentityRejected(reason) => Left(reason)
      case ShuffleRecoveryIdentityBuilt(identity) =>
        // Only resolve the dependency after semantic admission. Source planning failures remain
        // ordinary query errors; they must not be swallowed as recovery misses.
        if (exchange.shuffleDependency.rdd.partitions.length != binding.decomposition.mapperCount) {
          Left(ShuffleRecoveryMissReason.InvalidPartitionCount)
        } else {
          Right(ShuffleRecoveryCanonicalInputs(identity))
        }
    }
  }
}
