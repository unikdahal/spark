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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream, IOException}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.nio.file.{FileAlreadyExistsException, Files, LinkOption, Path, StandardCopyOption, StandardOpenOption}
import java.security.MessageDigest
import java.util.{Base64, UUID}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

/**
 * Feasibility-only recovery identity used to prove cross-driver shuffle adoption.
 *
 * This encoding is intentionally narrower than the proposed production computation-identity
 * contract and must not be treated as a stable API. Every field is reviewed for the deterministic
 * feasibility workload; attempt-local scheduler identifiers are deliberately absent.
 */
private[spark] final case class ShuffleRecoveryFeasibilityIdentity(
    sourceToken: String,
    producerTag: String,
    rowEncoding: String,
    partitioningShape: String,
    mapperCount: Int,
    reducerCount: Int,
    resolvedLiteral: String,
    sparkCompatibilityId: String,
    providerCompatibilityId: String) {

  private[shuffle] lazy val canonicalPayload: Vector[Byte] =
    ShuffleRecoveryIdentityCodec.encode(this).toVector

  private[shuffle] lazy val digest: String =
    ShuffleRecoveryManifestCodec.sha256Hex(canonicalPayload.toArray)

  private[shuffle] lazy val lookupPrefix: String = digest.substring(0, 2)
}

private[spark] object ShuffleRecoveryFeasibilityIdentity {
  val EncodingVersion = 1

  val SparkCompatibilityId =
    "spark-2a7cfea06ba135cf0ddc62902eb0daf5a835c672-shuffle-recovery-phase0-v1"
  val ProviderCompatibilityId = "reference-shuffle-provider-v1"

  private[shuffle] val HashPartitioning = "hash-v1"
  private[shuffle] val SinglePartitioning = "single-v1"

  private[shuffle] def create(
      sourceToken: String,
      producerTag: String,
      rowEncoding: String,
      partitioningShape: String,
      mapperCount: Int,
      reducerCount: Int,
      resolvedLiteral: String): ShuffleRecoveryFeasibilityIdentity = {
    val identity = ShuffleRecoveryFeasibilityIdentity(
      sourceToken,
      producerTag,
      rowEncoding,
      partitioningShape,
      mapperCount,
      reducerCount,
      resolvedLiteral,
      SparkCompatibilityId,
      ProviderCompatibilityId)
    ShuffleRecoveryManifestCodec.validateIdentity(identity)
    identity
  }
}

private[spark] final case class ShuffleRecoveryMapArtifact(
    mapIndex: Int,
    mapTaskId: Long,
    providerHandle: String,
    dataLength: Long,
    indexLength: Long)

/**
 * Immutable, bounded description of one complete reference-provider shuffle incarnation.
 *
 * The manifest contains O(M) map handles and at most O(R) explicit reducer aggregates. It never
 * serializes Spark scheduler objects, MapStatus, or a dense mapper-by-reducer matrix.
 */
private[spark] final case class ShuffleRecoveryManifest(
    recoveryGroup: String,
    generation: Long,
    incarnationId: String,
    identity: ShuffleRecoveryFeasibilityIdentity,
    mapperCount: Int,
    reducerCount: Int,
    mapArtifacts: Vector[ShuffleRecoveryMapArtifact],
    descriptorVersion: Int,
    reducerBytes: Option[Vector[Long]],
    publicationTimestampMillis: Long)

private[spark] object ShuffleRecoveryManifest {
  val FormatVersion = 1
  val DescriptorVersion = 1
}

private[shuffle] object ShuffleRecoveryIdentityCodec {
  private val Magic = 0x53524931

  def encode(identity: ShuffleRecoveryFeasibilityIdentity): Array[Byte] = {
    ShuffleRecoveryManifestCodec.validateIdentityFields(identity)
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    out.writeInt(Magic)
    out.writeInt(ShuffleRecoveryFeasibilityIdentity.EncodingVersion)
    ShuffleRecoveryManifestCodec.writeString(out, identity.sourceToken, "source token")
    ShuffleRecoveryManifestCodec.writeString(out, identity.producerTag, "producer tag")
    ShuffleRecoveryManifestCodec.writeString(out, identity.rowEncoding, "row encoding")
    ShuffleRecoveryManifestCodec.writeString(
      out, identity.partitioningShape, "partitioning shape")
    out.writeInt(identity.mapperCount)
    out.writeInt(identity.reducerCount)
    ShuffleRecoveryManifestCodec.writeString(out, identity.resolvedLiteral, "resolved literal")
    ShuffleRecoveryManifestCodec.writeString(
      out, identity.sparkCompatibilityId, "Spark compatibility id")
    ShuffleRecoveryManifestCodec.writeString(
      out, identity.providerCompatibilityId, "provider compatibility id")
    out.flush()
    val result = bytes.toByteArray
    if (result.length > ShuffleRecoveryManifestCodec.MaxIdentityBytes) {
      throw new IllegalArgumentException(
        s"canonical feasibility identity exceeds ${ShuffleRecoveryManifestCodec.MaxIdentityBytes} bytes")
    }
    result
  }

  def decode(bytes: Array[Byte]): ShuffleRecoveryFeasibilityIdentity = {
    if (bytes == null || bytes.length > ShuffleRecoveryManifestCodec.MaxIdentityBytes) {
      throw new IOException("invalid feasibility identity size")
    }
    val in = new DataInputStream(new ByteArrayInputStream(bytes))
    try {
      if (in.readInt() != Magic) {
        throw new IOException("invalid feasibility identity magic")
      }
      val version = in.readInt()
      if (version != ShuffleRecoveryFeasibilityIdentity.EncodingVersion) {
        throw new IOException(s"unsupported feasibility identity version: $version")
      }
      val identity = ShuffleRecoveryFeasibilityIdentity(
        ShuffleRecoveryManifestCodec.readString(in, "source token"),
        ShuffleRecoveryManifestCodec.readString(in, "producer tag"),
        ShuffleRecoveryManifestCodec.readString(in, "row encoding"),
        ShuffleRecoveryManifestCodec.readString(in, "partitioning shape"),
        in.readInt(),
        in.readInt(),
        ShuffleRecoveryManifestCodec.readString(in, "resolved literal"),
        ShuffleRecoveryManifestCodec.readString(in, "Spark compatibility id"),
        ShuffleRecoveryManifestCodec.readString(in, "provider compatibility id"))
      if (in.available() != 0) {
        throw new IOException("trailing feasibility identity bytes")
      }
      ShuffleRecoveryManifestCodec.validateIdentity(identity)
      identity
    } catch {
      case e: IOException => throw e
      case NonFatal(e) => throw new IOException("malformed feasibility identity", e)
    }
  }
}

private[spark] object ShuffleRecoveryManifestCodec {
  private val Magic = 0x53524d31

  private[shuffle] val MaxManifestBytes = 4 * 1024 * 1024
  private[shuffle] val MaxIdentityBytes = 16 * 1024
  private[shuffle] val MaxStringBytes = 4096
  private[shuffle] val MaxIdentifierBytes = 128
  private[shuffle] val MaxMaps = 65536
  private[shuffle] val MaxReducers = ReferenceShuffleProvider.MaxReducers

  def encode(manifest: ShuffleRecoveryManifest): Array[Byte] = {
    validateManifest(manifest)
    val identityPayload = manifest.identity.canonicalPayload.toArray
    val estimated = estimateEncodedSize(manifest, identityPayload.length)
    if (estimated > MaxManifestBytes) {
      throw new IllegalArgumentException(s"manifest exceeds $MaxManifestBytes bytes")
    }

    val bytes = new ByteArrayOutputStream(estimated)
    val out = new DataOutputStream(bytes)
    out.writeInt(Magic)
    out.writeInt(ShuffleRecoveryManifest.FormatVersion)
    writeString(out, manifest.recoveryGroup, "recovery group")
    out.writeLong(manifest.generation)
    writeString(out, manifest.incarnationId, "incarnation id")
    out.writeInt(ShuffleRecoveryFeasibilityIdentity.EncodingVersion)
    writeString(out, manifest.identity.digest, "identity digest")
    writeBytes(out, identityPayload, MaxIdentityBytes, "identity payload")
    writeString(
      out, manifest.identity.providerCompatibilityId, "provider compatibility id")
    out.writeInt(manifest.mapperCount)
    out.writeInt(manifest.reducerCount)
    out.writeInt(manifest.descriptorVersion)
    out.writeInt(manifest.mapArtifacts.size)
    manifest.mapArtifacts.foreach { artifact =>
      out.writeInt(artifact.mapIndex)
      out.writeLong(artifact.mapTaskId)
      writeString(out, artifact.providerHandle, "provider handle")
      out.writeLong(artifact.dataLength)
      out.writeLong(artifact.indexLength)
    }
    manifest.reducerBytes match {
      case Some(values) =>
        out.writeBoolean(true)
        out.writeInt(values.size)
        values.foreach(out.writeLong)
      case None =>
        out.writeBoolean(false)
    }
    out.writeLong(manifest.publicationTimestampMillis)
    out.flush()
    val result = bytes.toByteArray
    if (result.length > MaxManifestBytes) {
      throw new IllegalArgumentException(s"manifest exceeds $MaxManifestBytes bytes")
    }
    result
  }

  def decode(bytes: Array[Byte]): ShuffleRecoveryManifest = {
    if (bytes == null || bytes.length > MaxManifestBytes) {
      throw new IOException("invalid manifest size")
    }
    val in = new DataInputStream(new ByteArrayInputStream(bytes))
    try {
      if (in.readInt() != Magic) {
        throw new IOException("invalid manifest magic")
      }
      val formatVersion = in.readInt()
      if (formatVersion != ShuffleRecoveryManifest.FormatVersion) {
        throw new IOException(s"unsupported manifest version: $formatVersion")
      }
      val recoveryGroup = readString(in, "recovery group")
      val generation = in.readLong()
      val incarnationId = readString(in, "incarnation id")
      val identityVersion = in.readInt()
      if (identityVersion != ShuffleRecoveryFeasibilityIdentity.EncodingVersion) {
        throw new IOException(s"unsupported feasibility identity version: $identityVersion")
      }
      val identityDigest = readString(in, "identity digest")
      val identityPayload = readBytes(in, MaxIdentityBytes, "identity payload")
      val actualDigest = sha256Hex(identityPayload)
      if (!constantTimeEquals(identityDigest, actualDigest)) {
        throw new IOException("feasibility identity digest mismatch")
      }
      val identity = ShuffleRecoveryIdentityCodec.decode(identityPayload)
      if (!constantTimeEquals(identity.digest, identityDigest)) {
        throw new IOException("canonical feasibility identity digest mismatch")
      }
      val providerCompatibilityId = readString(in, "provider compatibility id")
      if (providerCompatibilityId != identity.providerCompatibilityId) {
        throw new IOException("provider compatibility id disagrees with identity")
      }
      val mapperCount = in.readInt()
      val reducerCount = in.readInt()
      val descriptorVersion = in.readInt()
      val handleCount = in.readInt()
      validateCountsBeforeAllocation(mapperCount, reducerCount, handleCount)
      val artifacts = Vector.newBuilder[ShuffleRecoveryMapArtifact]
      var index = 0
      while (index < handleCount) {
        artifacts += ShuffleRecoveryMapArtifact(
          in.readInt(),
          in.readLong(),
          readString(in, "provider handle"),
          in.readLong(),
          in.readLong())
        index += 1
      }
      val reducerBytes = if (in.readBoolean()) {
        val count = in.readInt()
        if (count != reducerCount || count < 0 || count > MaxReducers) {
          throw new IOException("invalid reducer aggregate count")
        }
        val values = Vector.newBuilder[Long]
        var reduceId = 0
        while (reduceId < count) {
          values += in.readLong()
          reduceId += 1
        }
        Some(values.result())
      } else {
        None
      }
      val publicationTimestampMillis = in.readLong()
      if (in.available() != 0) {
        throw new IOException("trailing manifest bytes")
      }
      val manifest = ShuffleRecoveryManifest(
        recoveryGroup,
        generation,
        incarnationId,
        identity,
        mapperCount,
        reducerCount,
        artifacts.result(),
        descriptorVersion,
        reducerBytes,
        publicationTimestampMillis)
      validateManifest(manifest)
      manifest
    } catch {
      case e: IOException => throw e
      case e: IllegalArgumentException => throw new IOException("invalid manifest", e)
      case NonFatal(e) => throw new IOException("malformed manifest", e)
    }
  }

  private[shuffle] def validateManifest(manifest: ShuffleRecoveryManifest): Unit = {
    if (manifest == null) {
      throw new IllegalArgumentException("manifest must not be null")
    }
    validateIdentifier(manifest.recoveryGroup, "recovery group")
    validateIdentifier(manifest.incarnationId, "incarnation id")
    if (manifest.generation <= 0L) {
      throw new IllegalArgumentException("publishing generation must be positive")
    }
    validateIdentity(manifest.identity)
    validateCountsBeforeAllocation(
      manifest.mapperCount, manifest.reducerCount, manifest.mapArtifacts.size)
    if (manifest.mapperCount != manifest.identity.mapperCount ||
        manifest.reducerCount != manifest.identity.reducerCount) {
      throw new IllegalArgumentException("manifest shape disagrees with feasibility identity")
    }
    if (manifest.descriptorVersion != ShuffleRecoveryManifest.DescriptorVersion) {
      throw new IllegalArgumentException("unsupported provider descriptor version")
    }
    manifest.mapArtifacts.zipWithIndex.foreach { case (artifact, expectedMapIndex) =>
      if (artifact.mapIndex != expectedMapIndex) {
        throw new IllegalArgumentException("map artifacts must be complete and ordered")
      }
      if (artifact.mapTaskId < 0L) {
        throw new IllegalArgumentException("map task id must be non-negative")
      }
      validateIdentifier(artifact.providerHandle, "provider handle")
      if (artifact.dataLength < 0L || artifact.indexLength <= 0L) {
        throw new IllegalArgumentException("invalid provider artifact lengths")
      }
    }
    manifest.reducerBytes.foreach { values =>
      if (values.size != manifest.reducerCount || values.exists(_ < 0L)) {
        throw new IllegalArgumentException("invalid reducer aggregate statistics")
      }
    }
    if (manifest.publicationTimestampMillis < 0L) {
      throw new IllegalArgumentException("publication timestamp must be non-negative")
    }
  }

  private[shuffle] def validateIdentity(identity: ShuffleRecoveryFeasibilityIdentity): Unit = {
    if (identity == null) {
      throw new IllegalArgumentException("feasibility identity must not be null")
    }
    validateIdentityFields(identity)
    if (identity.canonicalPayload.size > MaxIdentityBytes) {
      throw new IllegalArgumentException("feasibility identity is too large")
    }
  }

  private[shuffle] def validateIdentityFields(
      identity: ShuffleRecoveryFeasibilityIdentity): Unit = {
    validateText(identity.sourceToken, "source token")
    validateText(identity.producerTag, "producer tag")
    validateText(identity.rowEncoding, "row encoding")
    validateText(identity.resolvedLiteral, "resolved literal")
    validateText(identity.sparkCompatibilityId, "Spark compatibility id")
    validateText(identity.providerCompatibilityId, "provider compatibility id")
    identity.partitioningShape match {
      case ShuffleRecoveryFeasibilityIdentity.HashPartitioning =>
      case ShuffleRecoveryFeasibilityIdentity.SinglePartitioning if identity.reducerCount == 1 =>
      case ShuffleRecoveryFeasibilityIdentity.SinglePartitioning =>
        throw new IllegalArgumentException("single partitioning requires exactly one reducer")
      case other =>
        throw new IllegalArgumentException(s"unsupported partitioning shape: $other")
    }
    if (identity.mapperCount < 0 || identity.mapperCount > MaxMaps) {
      throw new IllegalArgumentException("invalid mapper count")
    }
    if (identity.reducerCount <= 0 || identity.reducerCount > MaxReducers) {
      throw new IllegalArgumentException("invalid reducer count")
    }
    if (identity.sparkCompatibilityId != ShuffleRecoveryFeasibilityIdentity.SparkCompatibilityId) {
      throw new IllegalArgumentException("unexpected Spark compatibility id")
    }
    if (identity.providerCompatibilityId !=
        ShuffleRecoveryFeasibilityIdentity.ProviderCompatibilityId) {
      throw new IllegalArgumentException("unexpected provider compatibility id")
    }
  }

  private def validateCountsBeforeAllocation(
      mapperCount: Int,
      reducerCount: Int,
      handleCount: Int): Unit = {
    if (mapperCount < 0 || mapperCount > MaxMaps) {
      throw new IOException("invalid mapper count")
    }
    if (reducerCount <= 0 || reducerCount > MaxReducers) {
      throw new IOException("invalid reducer count")
    }
    if (handleCount != mapperCount || handleCount < 0 || handleCount > MaxMaps) {
      throw new IOException("invalid map artifact count")
    }
    try {
      val minimumBytes = Math.multiplyExact(handleCount.toLong, 32L)
      if (minimumBytes > MaxManifestBytes) {
        throw new IOException("map artifact count exceeds manifest allocation budget")
      }
    } catch {
      case _: ArithmeticException =>
        throw new IOException("map artifact count overflow")
    }
  }

  private def estimateEncodedSize(manifest: ShuffleRecoveryManifest, identityBytes: Int): Int = {
    var size = 64L
    def add(value: Long): Unit = {
      try {
        size = Math.addExact(size, value)
      } catch {
        case _: ArithmeticException => throw new IllegalArgumentException("manifest size overflow")
      }
      if (size > MaxManifestBytes) {
        throw new IllegalArgumentException(s"manifest exceeds $MaxManifestBytes bytes")
      }
    }
    add(encodedLength(manifest.recoveryGroup))
    add(encodedLength(manifest.incarnationId))
    add(encodedLength(manifest.identity.digest))
    add(4L + identityBytes)
    add(encodedLength(manifest.identity.providerCompatibilityId))
    manifest.mapArtifacts.foreach { artifact =>
      add(28L + encodedLength(artifact.providerHandle))
    }
    manifest.reducerBytes.foreach(values => add(4L + Math.multiplyExact(values.size.toLong, 8L)))
    size.toInt
  }

  private def encodedLength(value: String): Long = {
    if (value == null) {
      throw new IllegalArgumentException("string field must not be null")
    }
    4L + value.getBytes(StandardCharsets.UTF_8).length
  }

  private[shuffle] def writeString(
      out: DataOutputStream,
      value: String,
      field: String): Unit = {
    val bytes = validateText(value, field)
    writeBytes(out, bytes, MaxStringBytes, field)
  }

  private def writeBytes(
      out: DataOutputStream,
      bytes: Array[Byte],
      maximum: Int,
      field: String): Unit = {
    if (bytes == null || bytes.length > maximum) {
      throw new IllegalArgumentException(s"invalid $field size")
    }
    out.writeInt(bytes.length)
    out.write(bytes)
  }

  private[shuffle] def readString(in: DataInputStream, field: String): String = {
    val bytes = readBytes(in, MaxStringBytes, field)
    val decoder = StandardCharsets.UTF_8.newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    try {
      decoder.decode(ByteBuffer.wrap(bytes)).toString
    } catch {
      case NonFatal(e) => throw new IOException(s"invalid UTF-8 in $field", e)
    }
  }

  private def readBytes(
      in: DataInputStream,
      maximum: Int,
      field: String): Array[Byte] = {
    val length = in.readInt()
    if (length < 0 || length > maximum || length > in.available()) {
      throw new IOException(s"invalid $field length")
    }
    val bytes = new Array[Byte](length)
    in.readFully(bytes)
    bytes
  }

  private def validateText(value: String, field: String): Array[Byte] = {
    if (value == null || value.isEmpty) {
      throw new IllegalArgumentException(s"$field must not be empty")
    }
    val bytes = value.getBytes(StandardCharsets.UTF_8)
    if (bytes.length > MaxStringBytes) {
      throw new IllegalArgumentException(s"$field exceeds $MaxStringBytes bytes")
    }
    bytes
  }

  private[shuffle] def validateIdentifier(value: String, field: String): Unit = {
    val bytes = validateText(value, field)
    if (bytes.length > MaxIdentifierBytes || value == "." || value == ".." ||
        !value.matches("[A-Za-z0-9._-]+")) {
      throw new IllegalArgumentException(s"unsafe $field")
    }
  }

  private[shuffle] def sha256Hex(bytes: Array[Byte]): String = {
    val digest = MessageDigest.getInstance("SHA-256").digest(bytes)
    digest.iterator.map(byte => f"${byte & 0xff}%02x").mkString
  }

  private def constantTimeEquals(left: String, right: String): Boolean = {
    if (left == null || right == null) {
      false
    } else {
      MessageDigest.isEqual(
        left.getBytes(StandardCharsets.US_ASCII), right.getBytes(StandardCharsets.US_ASCII))
    }
  }
}

private[spark] sealed trait ShuffleRecoveryManifestPublishResult
private[spark] case object ShuffleRecoveryManifestPublished
  extends ShuffleRecoveryManifestPublishResult
private[spark] case object ShuffleRecoveryManifestAlreadyPublished
  extends ShuffleRecoveryManifestPublishResult

/**
 * Local-filesystem Phase 0 manifest store.
 *
 * Bodies and index references are immutable files. Publication commits the body first and only then
 * exposes an immutable index reference using a same-filesystem atomic rename. This is deliberately
 * a local filesystem mechanism; it does not claim that rename is a transaction primitive for
 * object stores. A failed index commit may leave an unreachable body, but can never expose a torn
 * manifest. Later retirement can remove an exact index reference without rewriting body bytes.
 */
private[spark] final class ShuffleRecoveryManifestStore(root: Path) extends Logging {
  import ShuffleRecoveryManifestStore._

  if (root == null) {
    throw new IllegalArgumentException("manifest root must not be null")
  }
  private val normalizedRoot = root.toAbsolutePath.normalize()

  def publish(manifest: ShuffleRecoveryManifest): ShuffleRecoveryManifestPublishResult = {
    ShuffleRecoveryManifestCodec.validateManifest(manifest)
    val bodyBytes = ShuffleRecoveryManifestCodec.encode(manifest)
    val layout = prepareLayout(manifest.recoveryGroup, manifest.identity.lookupPrefix)
    val body = layout.bodies.resolve(bodyName(manifest))
    val reference = layout.index.resolve(referenceName(manifest))

    val bodyResult = commitImmutable(body, bodyBytes)
    val referenceBytes = encodeReference(manifest, body.getFileName.toString)
    try {
      commitImmutable(reference, referenceBytes)
    } catch {
      case NonFatal(e) if bodyResult == ShuffleRecoveryManifestAlreadyPublished => throw e
      case NonFatal(e) => throw e
    }
    bodyResult
  }

  /**
   * Finds a full-payload-compatible candidate from a bounded short-digest namespace.
   * Same/future generations are ignored; malformed candidates are refused without mutation.
   */
  def findCompatible(
      recoveryGroup: String,
      identity: ShuffleRecoveryFeasibilityIdentity,
      currentGeneration: Long): Option[ShuffleRecoveryManifest] = {
    ShuffleRecoveryManifestCodec.validateIdentifier(recoveryGroup, "recovery group")
    ShuffleRecoveryManifestCodec.validateIdentity(identity)
    if (currentGeneration <= 0L) {
      throw new IllegalArgumentException("current generation must be positive")
    }
    val index = indexDirectory(recoveryGroup, identity.lookupPrefix)
    if (!Files.isDirectory(index, LinkOption.NOFOLLOW_LINKS)) {
      return None
    }

    boundedReferences(index).sortBy(reference => -reference.generation).iterator.flatMap { ref =>
      if (ref.generation >= currentGeneration) {
        Iterator.empty
      } else {
        try {
          val body = bodiesDirectory(recoveryGroup).resolve(ref.bodyName)
          val manifest = readManifestBody(body)
          val fullPayloadMatches = manifest.identity.digest == identity.digest &&
            manifest.identity.canonicalPayload == identity.canonicalPayload
          if (manifest.recoveryGroup == recoveryGroup &&
              manifest.generation == ref.generation &&
              manifest.incarnationId == ref.incarnationId &&
              fullPayloadMatches &&
              manifest.identity.providerCompatibilityId == identity.providerCompatibilityId) {
            Iterator.single(manifest)
          } else {
            Iterator.empty
          }
        } catch {
          case NonFatal(e) =>
            logWarning(s"Ignoring malformed shuffle recovery manifest candidate ${ref.bodyName}", e)
            Iterator.empty
        }
      }
    }.take(1).toSeq.headOption
  }

  private[shuffle] def readManifestBody(path: Path): ShuffleRecoveryManifest = {
    if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("manifest body is not a regular file")
    }
    val length = Files.size(path)
    if (length < 0L || length > ShuffleRecoveryManifestCodec.MaxManifestBytes) {
      throw new IOException("manifest body exceeds read bound")
    }
    ShuffleRecoveryManifestCodec.decode(Files.readAllBytes(path))
  }

  private[shuffle] def indexDirectoryFor(
      recoveryGroup: String,
      identity: ShuffleRecoveryFeasibilityIdentity): Path =
    indexDirectory(recoveryGroup, identity.lookupPrefix)

  private def prepareLayout(recoveryGroup: String, lookupPrefix: String): Layout = {
    val bodies = bodiesDirectory(recoveryGroup)
    val index = indexDirectory(recoveryGroup, lookupPrefix)
    createSafeDirectories(bodies)
    createSafeDirectories(index)
    Layout(bodies, index)
  }

  private def bodiesDirectory(recoveryGroup: String): Path =
    normalizedRoot.resolve(BodiesDirectory).resolve(encodeIdentifier(recoveryGroup))

  private def indexDirectory(recoveryGroup: String, lookupPrefix: String): Path = {
    if (!lookupPrefix.matches("[0-9a-f]{2}")) {
      throw new IllegalArgumentException("invalid lookup prefix")
    }
    normalizedRoot.resolve(IndexDirectory)
      .resolve(encodeIdentifier(recoveryGroup))
      .resolve(lookupPrefix)
  }

  private def createSafeDirectories(directory: Path): Unit = {
    if (!directory.normalize().startsWith(normalizedRoot)) {
      throw new IOException("manifest path escaped configured root")
    }
    val relative = normalizedRoot.relativize(directory.normalize())
    if (!Files.exists(normalizedRoot, LinkOption.NOFOLLOW_LINKS)) {
      Files.createDirectories(normalizedRoot)
    }
    if (!Files.isDirectory(normalizedRoot, LinkOption.NOFOLLOW_LINKS) ||
        Files.isSymbolicLink(normalizedRoot)) {
      throw new IOException("manifest root is not a safe directory")
    }
    var current = normalizedRoot
    relative.iterator().asScala.foreach { component =>
      current = current.resolve(component)
      try {
        Files.createDirectory(current)
      } catch {
        case _: FileAlreadyExistsException =>
      }
      if (!Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(current)) {
        throw new IOException("manifest namespace contains a non-directory or symbolic link")
      }
    }
  }

  private def commitImmutable(
      target: Path,
      bytes: Array[Byte]): ShuffleRecoveryManifestPublishResult = {
    val temp = target.resolveSibling(s".${target.getFileName}.tmp-${UUID.randomUUID()}")
    var channel: FileChannel = null
    try {
      channel = FileChannel.open(
        temp,
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE)
      val buffer = ByteBuffer.wrap(bytes)
      while (buffer.hasRemaining) {
        channel.write(buffer)
      }
      channel.force(true)
      channel.close()
      channel = null
      try {
        Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE)
        ShuffleRecoveryManifestPublished
      } catch {
        case _: FileAlreadyExistsException =>
          Files.deleteIfExists(temp)
          if (Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS) &&
              Files.size(target) == bytes.length &&
              MessageDigest.isEqual(Files.readAllBytes(target), bytes)) {
            ShuffleRecoveryManifestAlreadyPublished
          } else {
            throw new IOException("immutable manifest publication conflicts with existing bytes")
          }
      }
    } finally {
      if (channel != null) {
        channel.close()
      }
      Files.deleteIfExists(temp)
    }
  }

  private def boundedReferences(index: Path): Vector[IndexReference] = {
    val stream = Files.list(index)
    try {
      val paths = stream.iterator().asScala
        .filter(path => path.getFileName.toString.endsWith(ReferenceSuffix))
        .take(MaxCandidateReferences + 1)
        .toVector
      if (paths.size > MaxCandidateReferences) {
        throw new IOException("manifest candidate namespace exceeds configured bound")
      }
      paths.map(readReference)
    } finally {
      stream.close()
    }
  }

  private def readReference(path: Path): IndexReference = {
    if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) ||
        Files.size(path) > MaxReferenceBytes) {
      throw new IOException("invalid manifest index reference")
    }
    decodeReference(Files.readAllBytes(path))
  }
}

private[spark] object ShuffleRecoveryManifestStore {
  private val BodiesDirectory = "bodies"
  private val IndexDirectory = "index"
  private val ReferenceSuffix = ".ref"
  private val MaxCandidateReferences = 64
  private val MaxReferenceBytes = 1024

  private case class Layout(bodies: Path, index: Path)
  private case class IndexReference(
      generation: Long,
      incarnationId: String,
      identityDigest: String,
      bodyName: String)

  private def encodeIdentifier(value: String): String = {
    ShuffleRecoveryManifestCodec.validateIdentifier(value, "manifest identifier")
    Base64.getUrlEncoder.withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8))
  }

  private def bodyName(manifest: ShuffleRecoveryManifest): String =
    s"${manifest.generation}-${encodeIdentifier(manifest.incarnationId)}.manifest"

  private def referenceName(manifest: ShuffleRecoveryManifest): String =
    s"${manifest.generation}-${encodeIdentifier(manifest.incarnationId)}$ReferenceSuffix"

  private def encodeReference(manifest: ShuffleRecoveryManifest, bodyName: String): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    out.writeLong(manifest.generation)
    ShuffleRecoveryManifestCodec.writeString(out, manifest.incarnationId, "incarnation id")
    ShuffleRecoveryManifestCodec.writeString(out, manifest.identity.digest, "identity digest")
    ShuffleRecoveryManifestCodec.writeString(out, bodyName, "manifest body name")
    out.flush()
    val result = bytes.toByteArray
    if (result.length > MaxReferenceBytes) {
      throw new IllegalArgumentException("manifest index reference is too large")
    }
    result
  }

  private def decodeReference(bytes: Array[Byte]): IndexReference = {
    if (bytes == null || bytes.length > MaxReferenceBytes) {
      throw new IOException("invalid manifest index reference size")
    }
    val in = new DataInputStream(new ByteArrayInputStream(bytes))
    val reference = IndexReference(
      in.readLong(),
      ShuffleRecoveryManifestCodec.readString(in, "incarnation id"),
      ShuffleRecoveryManifestCodec.readString(in, "identity digest"),
      ShuffleRecoveryManifestCodec.readString(in, "manifest body name"))
    if (reference.generation <= 0L ||
        !reference.identityDigest.matches("[0-9a-f]{64}") ||
        !reference.bodyName.matches("[A-Za-z0-9._-]+")) {
      throw new IOException("malformed manifest index reference")
    }
    ShuffleRecoveryManifestCodec.validateIdentifier(reference.incarnationId, "incarnation id")
    if (in.available() != 0) {
      throw new IOException("trailing manifest index reference bytes")
    }
    reference
  }
}