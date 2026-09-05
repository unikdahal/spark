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

import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.util.control.NonFatal

private[spark] final case class ShuffleRecoveryPreparedMap(
    mapIndex: Int,
    providerHandle: String,
    dataLength: Long,
    indexLength: Long,
    exactIndexDigest: String,
    emptyBlockCount: Int,
    nonEmptyBlockCount: Int,
    physicalBlockBytes: Long)

/**
 * Spark-owned immutable result of the one untrusted recovery boundary.
 *
 * Exact reducer addressing remains in the immutable reference-provider index and is revalidated
 * by the fetch path. The driver retains O(M) prepared map descriptors rather than an O(M x R)
 * block matrix.
 */
private[spark] final case class PreparedShuffleRecoveryAdoption(
    reservation: ShuffleRecoveryAdoptionReservation,
    binding: ShuffleRecoveryBinding,
    recoveryGroup: String,
    publishingGeneration: Long,
    incarnationId: String,
    feasibilityIdentityDigest: String,
    targetShuffleId: Int,
    mapperCount: Int,
    reducerCount: Int,
    maps: Vector[ShuffleRecoveryPreparedMap])

private[shuffle] final case class ShuffleRecoveryValidatedCandidate(
    recoveryGroup: String,
    publishingGeneration: Long,
    incarnationId: String,
    identity: ShuffleRecoveryFeasibilityIdentity,
    mapperCount: Int,
    reducerCount: Int,
    descriptorVersion: Int,
    mapArtifacts: Vector[ShuffleRecoveryMapArtifact])

private[shuffle] trait ShuffleRecoveryValidationObserver {
  def afterClaimSnapshot(): Unit
}

private[shuffle] object ShuffleRecoveryValidationObserver {
  val Noop: ShuffleRecoveryValidationObserver = new ShuffleRecoveryValidationObserver {
    override def afterClaimSnapshot(): Unit = {}
  }
}

/** Centralized conversion of manifest/provider input into Spark-owned immutable state. */
private[spark] final class ShuffleRecoveryUntrustedBoundary private[shuffle] (
    observer: ShuffleRecoveryValidationObserver = ShuffleRecoveryValidationObserver.Noop) {

  import ShuffleRecoveryUntrustedBoundary._

  if (observer == null) {
    throw new IllegalArgumentException("shuffle recovery validation observer must not be null")
  }

  def validateCandidate(
      request: ShuffleRecoveryPreparationRequest,
      expectedIdentity: ShuffleRecoveryFeasibilityIdentity,
      manifest: ShuffleRecoveryManifest): Either[String, ShuffleRecoveryValidatedCandidate] = {
    try {
      if (request == null || request.target == null || expectedIdentity == null ||
          manifest == null) {
        return Left("candidate validation received a null field")
      }
      if (request.recoveryGroup == null || request.recoveryGroup != manifest.recoveryGroup) {
        return Left("candidate recovery group does not match the current request")
      }
      if (request.currentGeneration <= 0L || manifest.generation <= 0L ||
          manifest.generation >= request.currentGeneration) {
        return Left("candidate generation is not strictly earlier than the current generation")
      }
      ShuffleRecoveryManifestCodec.validateManifest(manifest)
      if (manifest.identity.digest != expectedIdentity.digest ||
          manifest.identity.canonicalPayload != expectedIdentity.canonicalPayload) {
        return Left("candidate feasibility identity does not match the current exchange")
      }
      if (manifest.identity.providerCompatibilityId !=
          ShuffleRecoveryFeasibilityIdentity.ProviderCompatibilityId) {
        return Left("candidate provider compatibility id is unsupported")
      }
      if (manifest.mapperCount != request.target.mapperCount ||
          manifest.reducerCount != request.target.reducerCount) {
        return Left("candidate mapper or reducer shape does not match the current dependency")
      }
      if (manifest.descriptorVersion != ShuffleRecoveryManifest.DescriptorVersion) {
        return Left("candidate read descriptor version is unsupported")
      }
      if (manifest.mapArtifacts.size != manifest.mapperCount) {
        return Left("candidate does not contain exactly one descriptor per mapper")
      }
      var index = 0
      while (index < manifest.mapArtifacts.size) {
        val artifact = manifest.mapArtifacts(index)
        if (artifact == null || artifact.mapIndex != index) {
          return Left("candidate map descriptors are missing, duplicate, or out of order")
        }
        index += 1
      }
      Right(ShuffleRecoveryValidatedCandidate(
        manifest.recoveryGroup,
        manifest.generation,
        manifest.incarnationId,
        manifest.identity,
        manifest.mapperCount,
        manifest.reducerCount,
        manifest.descriptorVersion,
        manifest.mapArtifacts))
    } catch {
      case NonFatal(_) => Left("candidate manifest failed bounded validation")
    }
  }

  def validateClaim(
      request: ShuffleRecoveryPreparationRequest,
      reservation: ShuffleRecoveryAdoptionReservation,
      candidate: ShuffleRecoveryValidatedCandidate,
      claimed: ShuffleRecoveryClaimed): Either[String, PreparedShuffleRecoveryAdoption] = {
    try {
      if (request == null || reservation == null || candidate == null || claimed == null ||
          claimed.binding == null || claimed.descriptor == null) {
        return Left("claimed recovery metadata contains a null field")
      }
      val binding = claimed.binding
      val descriptor = claimed.descriptor
      if (reservation.materializationId != request.target.materializationId ||
          reservation.dependencyIdentity != request.target.dependencyIdentity ||
          reservation.targetShuffleId != request.target.targetShuffleId) {
        return Left("reservation does not match the current materialization decision")
      }
      if (binding.targetShuffleId != request.target.targetShuffleId ||
          binding.targetShuffleId != reservation.targetShuffleId ||
          binding.recoveryGroup != candidate.recoveryGroup ||
          binding.publishingGeneration != candidate.publishingGeneration ||
          binding.incarnationId != candidate.incarnationId) {
        return Left("provider binding does not correspond to the current reservation")
      }
      if (descriptor.recoveryGroup != candidate.recoveryGroup ||
          descriptor.publishingGeneration != candidate.publishingGeneration ||
          descriptor.incarnationId != candidate.incarnationId ||
          descriptor.providerCompatibilityId != candidate.identity.providerCompatibilityId ||
          descriptor.targetShuffleId != request.target.targetShuffleId ||
          descriptor.descriptorVersion != candidate.descriptorVersion) {
        return Left("provider claim descriptor does not correspond to the selected candidate")
      }

      val mapArray = descriptor.maps
      if (mapArray == null || mapArray.length != candidate.mapperCount ||
          mapArray.length > ShuffleRecoveryManifestCodec.MaxMaps) {
        return Left("provider claim contains an invalid map descriptor count")
      }
      val mapSnapshot = mapArray.clone()
      val snapshots = new Array[ClaimedMapSnapshot](mapSnapshot.length)
      var index = 0
      var metadataBytes = 0L
      while (index < mapSnapshot.length) {
        val untrusted = mapSnapshot(index)
        if (untrusted == null || untrusted.exactIndexDigest == null ||
            untrusted.exactIndexDigest.length != Sha256Bytes) {
          return Left("provider claim contains an invalid map descriptor")
        }
        val handleBytes = boundedUtf8(untrusted.providerHandle, MaxProviderHandleBytes)
        metadataBytes = checkedMetadataBytes(
          metadataBytes,
          Math.addExact(FixedMapMetadataBytes, handleBytes.length.toLong + Sha256Bytes))
        snapshots(index) = ClaimedMapSnapshot(
          untrusted.mapIndex,
          untrusted.providerHandle,
          untrusted.reducerCount,
          untrusted.dataLength,
          untrusted.indexLength,
          untrusted.exactIndexDigest.clone(),
          untrusted.emptyBlockCount,
          untrusted.nonEmptyBlockCount,
          untrusted.physicalBlockBytes)
        index += 1
      }
      observer.afterClaimSnapshot()

      val prepared = new Array[ShuffleRecoveryPreparedMap](snapshots.length)
      val seen = mutable.BitSet.empty
      index = 0
      while (index < snapshots.length) {
        val snapshot = snapshots(index)
        if (snapshot.mapIndex < 0 || snapshot.mapIndex >= candidate.mapperCount ||
            seen.contains(snapshot.mapIndex)) {
          return Left("provider map descriptors are missing, duplicate, or out of range")
        }
        seen += snapshot.mapIndex
        val expectedArtifact = candidate.mapArtifacts(snapshot.mapIndex)
        if (snapshot.providerHandle != expectedArtifact.providerHandle ||
            snapshot.reducerCount != candidate.reducerCount ||
            snapshot.dataLength != expectedArtifact.dataLength ||
            snapshot.indexLength != expectedArtifact.indexLength) {
          return Left("provider map descriptor does not match the selected manifest artifact")
        }
        if (snapshot.dataLength < 0L || snapshot.indexLength <= 0L ||
            snapshot.emptyBlockCount < 0 || snapshot.nonEmptyBlockCount < 0 ||
            snapshot.physicalBlockBytes < 0L) {
          return Left("provider map descriptor contains a negative numeric field")
        }
        val blockCount = try {
          Math.addExact(snapshot.emptyBlockCount, snapshot.nonEmptyBlockCount)
        } catch {
          case _: ArithmeticException => return Left("provider block-count arithmetic overflowed")
        }
        if (blockCount != candidate.reducerCount ||
            snapshot.physicalBlockBytes != snapshot.dataLength) {
          return Left("provider empty/non-empty block metadata is internally inconsistent")
        }
        if (!validExactIndexLength(candidate.reducerCount, snapshot.indexLength)) {
          return Left("provider exact block index length is malformed or oversized")
        }
        prepared(snapshot.mapIndex) = ShuffleRecoveryPreparedMap(
          snapshot.mapIndex,
          snapshot.providerHandle,
          snapshot.dataLength,
          snapshot.indexLength,
          toHex(snapshot.exactIndexDigest),
          snapshot.emptyBlockCount,
          snapshot.nonEmptyBlockCount,
          snapshot.physicalBlockBytes)
        index += 1
      }
      if (seen.size != candidate.mapperCount || prepared.exists(_ == null)) {
        return Left("provider claim does not cover every expected mapper exactly once")
      }
      Right(PreparedShuffleRecoveryAdoption(
        reservation,
        binding,
        candidate.recoveryGroup,
        candidate.publishingGeneration,
        candidate.incarnationId,
        candidate.identity.digest,
        request.target.targetShuffleId,
        candidate.mapperCount,
        candidate.reducerCount,
        prepared.toVector))
    } catch {
      case NonFatal(_) => Left("provider claim failed bounded validation")
    }
  }

  private def boundedUtf8(value: String, maximum: Int): Array[Byte] = {
    if (value == null || value.isEmpty) {
      throw new IllegalArgumentException("provider handle must not be empty")
    }
    val bytes = value.getBytes(StandardCharsets.UTF_8)
    if (bytes.length > maximum || value == "." || value == ".." ||
        !value.matches("[A-Za-z0-9._-]+")) {
      throw new IllegalArgumentException("provider handle is invalid")
    }
    bytes
  }

  private def checkedMetadataBytes(current: Long, additional: Long): Long = {
    val updated = Math.addExact(current, additional)
    if (updated > MaxClaimMetadataBytes) {
      throw new IllegalArgumentException("provider claim metadata exceeds the Phase 0 bound")
    }
    updated
  }

  private def validExactIndexLength(reducerCount: Int, length: Long): Boolean = {
    try {
      val offsets = Math.multiplyExact(reducerCount.toLong + 1L, 8L)
      val withoutChecksums = Math.addExact(32L, offsets)
      val withChecksums = Math.addExact(
        withoutChecksums,
        Math.multiplyExact(reducerCount.toLong, 8L))
      length == withoutChecksums || length == withChecksums
    } catch {
      case _: ArithmeticException => false
    }
  }

  private def toHex(bytes: Array[Byte]): String = {
    bytes.iterator.map(byte => f"${byte & 0xff}%02x").mkString
  }
}

private[shuffle] object ShuffleRecoveryUntrustedBoundary {
  private val Sha256Bytes = 32
  private val MaxProviderHandleBytes = 128
  private val FixedMapMetadataBytes = 72L
  private val MaxClaimMetadataBytes = 32L * 1024L * 1024L

  private final case class ClaimedMapSnapshot(
      mapIndex: Int,
      providerHandle: String,
      reducerCount: Int,
      dataLength: Long,
      indexLength: Long,
      exactIndexDigest: Array[Byte],
      emptyBlockCount: Int,
      nonEmptyBlockCount: Int,
      physicalBlockBytes: Long)
}
