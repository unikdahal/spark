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

private[spark] final case class ShuffleRecoveryClaimRequest(
    recoveryGroup: String,
    publishingGeneration: Long,
    incarnationId: String,
    providerCompatibilityId: String,
    targetShuffleId: Int,
    mapperCount: Int,
    reducerCount: Int,
    mapArtifacts: Vector[ShuffleRecoveryMapArtifact])

/** Attempt-local provider binding to the replacement driver's current shuffle id. */
private[spark] final case class ShuffleRecoveryBinding(
    bindingId: String,
    targetShuffleId: Int,
    recoveryGroup: String,
    publishingGeneration: Long,
    incarnationId: String)

/**
 * Provider-owned metadata is intentionally mutable at this boundary.
 *
 * The centralized validator below snapshots it before any scheduler-visible value can exist.
 */
private[spark] final case class ShuffleRecoveryClaimedMapDescriptor(
    mapIndex: Int,
    providerHandle: String,
    reducerCount: Int,
    dataLength: Long,
    indexLength: Long,
    exactIndexDigest: Array[Byte],
    emptyBlockCount: Int,
    nonEmptyBlockCount: Int,
    physicalBlockBytes: Long)

private[spark] final case class ShuffleRecoveryClaimDescriptor(
    recoveryGroup: String,
    publishingGeneration: Long,
    incarnationId: String,
    providerCompatibilityId: String,
    targetShuffleId: Int,
    descriptorVersion: Int,
    maps: Array[ShuffleRecoveryClaimedMapDescriptor])

private[spark] sealed trait ShuffleRecoveryClaimResult
private[spark] final case class ShuffleRecoveryClaimed(
    binding: ShuffleRecoveryBinding,
    descriptor: ShuffleRecoveryClaimDescriptor) extends ShuffleRecoveryClaimResult
private[spark] case object ShuffleRecoveryClaimMissing extends ShuffleRecoveryClaimResult
private[spark] case object ShuffleRecoveryClaimCorrupt extends ShuffleRecoveryClaimResult
private[spark] case object ShuffleRecoveryClaimUnavailable extends ShuffleRecoveryClaimResult
private[spark] final case class ShuffleRecoveryClaimRejected(reason: String)
  extends ShuffleRecoveryClaimResult

private[shuffle] trait ShuffleRecoveryClaimProvider {
  def compatibilityId: String
  def claim(request: ShuffleRecoveryClaimRequest): ShuffleRecoveryClaimResult
  def release(binding: ShuffleRecoveryBinding): Unit
}
