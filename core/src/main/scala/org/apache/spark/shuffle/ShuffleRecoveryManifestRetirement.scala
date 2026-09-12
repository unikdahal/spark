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

import java.io.{ByteArrayOutputStream, DataOutputStream, IOException}
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, LinkOption, Path, StandardOpenOption}
import java.security.MessageDigest
import java.util.Base64

import scala.jdk.CollectionConverters._

/** Exact immutable incarnation selected by a replacement attempt. */
private[spark] final case class ShuffleRecoveryManifestIncarnation(
    recoveryGroup: String,
    generation: Long,
    incarnationId: String,
    identityDigest: String)

private[spark] sealed trait ShuffleRecoveryRetirementResult
private[spark] case object ShuffleRecoveryIncarnationRetired
  extends ShuffleRecoveryRetirementResult
private[spark] case object ShuffleRecoveryIncarnationAlreadyAbsent
  extends ShuffleRecoveryRetirementResult
private[spark] case object ShuffleRecoveryIncarnationRetirementRefused
  extends ShuffleRecoveryRetirementResult
private[spark] case object ShuffleRecoveryIncarnationRetirementUnavailable
  extends ShuffleRecoveryRetirementResult

/**
 * Narrow retirement surface for immutable recovery manifests.
 *
 * Retirement removes only the exact index reference examined by the caller. Manifest bodies and
 * provider bytes remain immutable. The reference bytes are compared with the complete expected
 * incarnation tuple before deletion, so a delayed retirement cannot name or mutate a successor.
 */
private[spark] trait ShuffleRecoveryIncarnationRetirer {
  def retireExact(
      incarnation: ShuffleRecoveryManifestIncarnation): ShuffleRecoveryRetirementResult
}

private[spark] final class ShuffleRecoveryManifestRetirer(root: Path)
  extends ShuffleRecoveryIncarnationRetirer {

  private val MaxReferenceBytes = 1024L
  private val normalizedRoot = Option(root).getOrElse {
    throw new IllegalArgumentException("manifest retirement root must not be null")
  }.toAbsolutePath.normalize()

  override def retireExact(
      incarnation: ShuffleRecoveryManifestIncarnation): ShuffleRecoveryRetirementResult = {
    ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery manifest retirement")
    if (!validIncarnation(incarnation)) {
      return ShuffleRecoveryIncarnationRetirementRefused
    }

    val reference = referencePath(incarnation)
    if (!safeExistingParent(reference.getParent)) {
      return ShuffleRecoveryIncarnationAlreadyAbsent
    }
    if (!Files.exists(reference, LinkOption.NOFOLLOW_LINKS)) {
      return ShuffleRecoveryIncarnationAlreadyAbsent
    }
    if (!Files.isRegularFile(reference, LinkOption.NOFOLLOW_LINKS) ||
        Files.isSymbolicLink(reference)) {
      return ShuffleRecoveryIncarnationRetirementRefused
    }

    try {
      if (Files.size(reference) > MaxReferenceBytes) {
        return ShuffleRecoveryIncarnationRetirementRefused
      }
      val expected = encodeReference(incarnation)
      val observed = Files.readAllBytes(reference)
      if (!MessageDigest.isEqual(observed, expected)) {
        return ShuffleRecoveryIncarnationRetirementRefused
      }
      Files.delete(reference)
      forceDirectory(reference.getParent)
      ShuffleRecoveryIncarnationRetired
    } catch {
      case _: java.nio.file.NoSuchFileException => ShuffleRecoveryIncarnationAlreadyAbsent
      case _: IOException => ShuffleRecoveryIncarnationRetirementUnavailable
      case _: SecurityException => ShuffleRecoveryIncarnationRetirementUnavailable
    }
  }

  private def validIncarnation(incarnation: ShuffleRecoveryManifestIncarnation): Boolean = {
    if (incarnation == null || incarnation.generation <= 0L ||
        incarnation.identityDigest == null ||
        !incarnation.identityDigest.matches("[0-9a-f]{64}")) {
      false
    } else {
      try {
        ShuffleRecoveryManifestCodec.validateIdentifier(
          incarnation.recoveryGroup, "recovery group")
        ShuffleRecoveryManifestCodec.validateIdentifier(
          incarnation.incarnationId, "incarnation id")
        true
      } catch {
        case _: IllegalArgumentException => false
      }
    }
  }

  private def referencePath(incarnation: ShuffleRecoveryManifestIncarnation): Path = {
    normalizedRoot.resolve("index")
      .resolve(encodeIdentifier(incarnation.recoveryGroup))
      .resolve(incarnation.identityDigest.substring(0, 2))
      .resolve(s"${incarnation.generation}-${encodeIdentifier(incarnation.incarnationId)}.ref")
      .normalize()
  }

  private def safeExistingParent(parent: Path): Boolean = {
    val normalized = parent.toAbsolutePath.normalize()
    if (!normalized.startsWith(normalizedRoot) ||
        !Files.isDirectory(normalizedRoot, LinkOption.NOFOLLOW_LINKS) ||
        Files.isSymbolicLink(normalizedRoot)) {
      return false
    }
    val relative = normalizedRoot.relativize(normalized)
    var current = normalizedRoot
    relative.iterator().asScala.forall { component =>
      current = current.resolve(component)
      Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(current)
    }
  }

  private def encodeIdentifier(value: String): String =
    Base64.getUrlEncoder.withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8))

  private def encodeReference(incarnation: ShuffleRecoveryManifestIncarnation): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    try {
      out.writeLong(incarnation.generation)
      ShuffleRecoveryManifestCodec.writeString(out, incarnation.incarnationId, "incarnation id")
      ShuffleRecoveryManifestCodec.writeString(out, incarnation.identityDigest, "identity digest")
      val bodyName =
        s"${incarnation.generation}-${encodeIdentifier(incarnation.incarnationId)}.manifest"
      ShuffleRecoveryManifestCodec.writeString(out, bodyName, "manifest body name")
      out.flush()
      bytes.toByteArray
    } finally {
      out.close()
    }
  }

  private def forceDirectory(directory: Path): Unit = {
    val channel = FileChannel.open(directory, StandardOpenOption.READ)
    try {
      channel.force(true)
    } finally {
      channel.close()
    }
  }
}
