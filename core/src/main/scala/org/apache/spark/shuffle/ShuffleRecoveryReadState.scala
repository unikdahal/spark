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

/**
 * Authoritative aggregate statistics owned by Spark after the recovery trust boundary.
 *
 * Absence is represented explicitly. In particular, an unavailable row count or reducer total is
 * never encoded as zero. Reducer aggregates are intentionally O(R); mapper-by-reducer
 * distributions are not retained by this representation.
 */
private[spark] final case class ShuffleRecoveryStatistics(
    totalDataSize: Option[Long],
    bytesByReducer: Option[Vector[Long]],
    numOutputRows: Option[Long]) {

  require(totalDataSize != null, "total shuffle size presence must not be null")
  require(bytesByReducer != null, "reducer statistics presence must not be null")
  require(numOutputRows != null, "output row-count presence must not be null")

  def reducerCount: Option[Int] = bytesByReducer.map(_.size)
}

private[spark] object ShuffleRecoveryStatistics {
  val Unknown: ShuffleRecoveryStatistics = ShuffleRecoveryStatistics(None, None, None)
}

/**
 * Capabilities proven for one adopted shuffle representation.
 *
 * These capabilities describe evidence available to a reader; they are not an `isRecovered`
 * shortcut. A caller must state what its concrete read requires and receive an explicit decision.
 */
private[spark] final case class ShuffleRecoveryReadCapabilities(
    reducerAggregateStatisticsAvailable: Boolean,
    exactMapperLocalDistributionAvailable: Boolean,
    partitionSpecCompatible: Boolean) {

  def evaluate(request: ShuffleRecoveryReadRequest): ShuffleRecoveryReadDecision = {
    if (request == null) {
      ShuffleRecoveryReadUnsupported("recovered read request is null")
    } else if (request.requiresReducerAggregateStatistics &&
        !reducerAggregateStatisticsAvailable) {
      ShuffleRecoveryReadUnsupported("authoritative reducer aggregate statistics are unavailable")
    } else if (request.requiresExactMapperLocalDistribution &&
        !exactMapperLocalDistributionAvailable) {
      ShuffleRecoveryReadUnsupported("exact mapper-local distribution is unavailable")
    } else if (request.requiresPartitionSpecCompatibility && !partitionSpecCompatible) {
      ShuffleRecoveryReadUnsupported("adaptive partition specification is incompatible")
    } else {
      ShuffleRecoveryReadSupported
    }
  }
}

private[spark] object ShuffleRecoveryReadCapabilities {
  val Conservative: ShuffleRecoveryReadCapabilities = ShuffleRecoveryReadCapabilities(
    reducerAggregateStatisticsAvailable = false,
    exactMapperLocalDistributionAvailable = false,
    partitionSpecCompatible = true)

  val ReducerAggregatesOnly: ShuffleRecoveryReadCapabilities = ShuffleRecoveryReadCapabilities(
    reducerAggregateStatisticsAvailable = true,
    exactMapperLocalDistributionAvailable = false,
    partitionSpecCompatible = true)
}

private[spark] final case class ShuffleRecoveryReadRequest(
    requiresReducerAggregateStatistics: Boolean,
    requiresExactMapperLocalDistribution: Boolean,
    requiresPartitionSpecCompatibility: Boolean)

private[spark] sealed trait ShuffleRecoveryReadDecision
private[spark] case object ShuffleRecoveryReadSupported extends ShuffleRecoveryReadDecision
private[spark] final case class ShuffleRecoveryReadUnsupported(reason: String)
  extends ShuffleRecoveryReadDecision

/** Exact provider-derived metadata for one non-empty recovered shuffle block. */
private[spark] final case class ShuffleRecoveryExactBlock(
    mapIndex: Int,
    reduceId: Int,
    length: Long)

/**
 * Bounded result of one exact recovered metadata lookup.
 *
 * Results are generation-fenced and are not cached on executors. A stale or unavailable result
 * therefore cannot repopulate recovery state after whole-shuffle invalidation.
 */
private[spark] sealed trait ShuffleRecoveryExactBlockQueryResult
private[spark] final case class ShuffleRecoveryExactBlocksAvailable(
    localBindingGeneration: Long,
    blocks: Vector[ShuffleRecoveryExactBlock]) extends ShuffleRecoveryExactBlockQueryResult
private[spark] case object ShuffleRecoveryExactBlocksNotAdopted
  extends ShuffleRecoveryExactBlockQueryResult
private[spark] final case class ShuffleRecoveryExactBlocksUnavailable(reason: String)
  extends ShuffleRecoveryExactBlockQueryResult
private[spark] final case class ShuffleRecoveryExactBlocksUnsupported(reason: String)
  extends ShuffleRecoveryExactBlockQueryResult
