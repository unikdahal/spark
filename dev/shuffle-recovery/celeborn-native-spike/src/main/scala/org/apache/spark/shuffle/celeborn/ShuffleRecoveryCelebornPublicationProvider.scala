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

package org.apache.spark.shuffle.celeborn

import org.apache.spark.shuffle.{ShuffleRecoveryMapAttempt, ShuffleRecoveryNativePublicationProvider}

import org.apache.celeborn.common.CelebornConf

/** Integration code compiled against the Spark and Celeborn PoC forks, outside Spark Core. */
private[spark] final class ShuffleRecoveryCelebornPublicationProvider(
    manager: SparkShuffleManager,
    conf: CelebornConf,
    handoffTtlMillis: Long) extends ShuffleRecoveryNativePublicationProvider {
  require(manager != null && conf != null)
  require(handoffTtlMillis > 0 && handoffTtlMillis <= 3600000L)

  override val compatibilityId: String =
    ShuffleRecoveryCelebornPublicationProvider.readFormatId(conf)

  override def seal(
      shuffleId: Int,
      acceptedAttempts: Vector[ShuffleRecoveryMapAttempt]): Vector[Byte] = {
    require(acceptedAttempts != null && acceptedAttempts.nonEmpty)
    val encoded = acceptedAttempts.map { attempt =>
      require(attempt != null && attempt.stageAttemptId >= 0 && attempt.stageAttemptId <= 32767 &&
        attempt.taskAttemptNumber >= 0 && attempt.taskAttemptNumber <= 65535,
        "accepted Spark attempt cannot be represented by Celeborn's native attempt encoding")
      (attempt.stageAttemptId << 16) | attempt.taskAttemptNumber
    }.toArray
    val bytes = manager.publishRetainedShuffle(shuffleId, encoded, handoffTtlMillis)
    require(bytes != null && bytes.nonEmpty, "Celeborn returned an empty publication descriptor")
    bytes.toVector
  }
}

private[spark] object ShuffleRecoveryCelebornPublicationProvider {
  // Pin the native framing implementation. Spark row/serializer compatibility is represented
  // separately in the certified computation identity. Native decompression needs the same codec.
  def readFormatId(conf: CelebornConf): String = {
    require(conf != null)
    "celeborn-retained-4f0f787cb-v1-codec-" + conf.shuffleCompressionCodec.toString
  }
}
