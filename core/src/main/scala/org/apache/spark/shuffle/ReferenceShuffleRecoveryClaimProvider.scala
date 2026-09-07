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

import java.io.{BufferedInputStream, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.{AccessDeniedException, Files, LinkOption, NoSuchFileException, Path}
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest
import java.util.{Base64, UUID}
import java.util.concurrent.ConcurrentHashMap

import scala.util.control.NonFatal

import org.apache.spark.SparkConf

/**
 * Reference-provider claim adapter.
 *
 * A binding is an attempt-local alias only. It never moves, rewrites, revokes, or deletes the
 * immutable group-scoped artifacts that it references.
 */
private[spark] final class ReferenceShuffleRecoveryClaimProvider(
    providerRoot: Path,
    conf: SparkConf = new SparkConf(false)) extends ShuffleRecoveryClaimProvider {

  import ReferenceShuffleRecoveryClaimProvider._

  if (providerRoot == null || conf == null) {
    throw new IllegalArgumentException("provider root and SparkConf must not be null")
  }

  private final class ActiveBinding(
      val binding: ShuffleRecoveryBinding,
      val provider: ReferenceShuffleProvider)

  private val activeBindings = new ConcurrentHashMap[String, ActiveBinding]()

  override val compatibilityId: String =
    ShuffleRecoveryFeasibilityIdentity.ProviderCompatibilityId

  override def claim(request: ShuffleRecoveryClaimRequest): ShuffleRecoveryClaimResult = {
    ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery provider claim")
    validateRequest(request) match {
      case Some(reason) => return ShuffleRecoveryClaimRejected(reason)
      case None =>
    }

    val provider = openExistingProvider(request) match {
      case Right(value) => value
      case Left(result) => return result
    }

    val maps = new Array[ShuffleRecoveryClaimedMapDescriptor](request.mapperCount)
    var mapIndex = 0
    while (mapIndex < request.mapperCount) {
      inspectMap(provider, request, request.mapArtifacts(mapIndex)) match {
        case Left(result) => return result
        case Right(descriptor) => maps(mapIndex) = descriptor
      }
      mapIndex += 1
    }

    val binding = ShuffleRecoveryBinding(
      UUID.randomUUID().toString,
      request.targetShuffleId,
      request.recoveryGroup,
      request.publishingGeneration,
      request.incarnationId)
    val active = new ActiveBinding(binding, provider)
    if (activeBindings.putIfAbsent(binding.bindingId, active) != null) {
      return ShuffleRecoveryClaimUnavailable
    }
    ShuffleRecoveryClaimed(
      binding,
      ShuffleRecoveryClaimDescriptor(
        request.recoveryGroup,
        request.publishingGeneration,
        request.incarnationId,
        compatibilityId,
        request.targetShuffleId,
        ShuffleRecoveryManifest.DescriptorVersion,
        maps))
  }

  override def release(binding: ShuffleRecoveryBinding): Unit = {
    ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery provider release")
    if (binding == null) {
      throw new IllegalArgumentException("shuffle recovery binding must not be null")
    }
    val active = activeBindings.get(binding.bindingId)
    if (active != null && active.binding == binding) {
      activeBindings.remove(binding.bindingId, active)
    }
  }

  private[shuffle] def isBound(binding: ShuffleRecoveryBinding): Boolean = {
    if (binding == null) {
      false
    } else {
      val active = activeBindings.get(binding.bindingId)
      active != null && active.binding == binding
    }
  }

  private[shuffle] def openBoundMap(
      binding: ShuffleRecoveryBinding,
      mapIndex: Int): ReferenceShuffleResolvedMap = {
    ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery bound map open")
    if (binding == null) {
      throw new IllegalArgumentException("shuffle recovery binding must not be null")
    }
    val active = activeBindings.get(binding.bindingId)
    if (active == null || active.binding != binding) {
      throw new IOException("shuffle recovery binding is not active")
    }
    active.provider.openMap(mapIndex)
  }

  /**
   * Opens one map for an adopted fetch and classifies failure using the exact validated snapshot.
   *
   * This method runs on the shuffle-read path, never on the scheduler event loop. A generic I/O
   * failure is deliberately classified as unavailable unless a fresh examination proves that the
   * immutable winner, data, or exact index is missing or has changed.
   */
  private[shuffle] def openBoundMapForFetch(
      binding: ShuffleRecoveryBinding,
      mapIndex: Int,
      expected: ShuffleRecoveryPreparedMap): ShuffleRecoveryBoundMapReadResult = {
    ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery adopted map fetch")
    if (binding == null || expected == null || mapIndex < 0 || expected.mapIndex != mapIndex) {
      return ShuffleRecoveryBoundMapFailed(ShuffleRecoveryAdoptedUnavailable)
    }
    val active = activeBindings.get(binding.bindingId)
    if (active == null || active.binding != binding) {
      return ShuffleRecoveryBoundMapFailed(ShuffleRecoveryAdoptedUnavailable)
    }

    validateRuntimeArtifact(active.provider, expected) match {
      case Some(failureClass) => return ShuffleRecoveryBoundMapFailed(failureClass)
      case None =>
    }

    val resolved = try {
      active.provider.openMap(mapIndex)
    } catch {
      case _: AccessDeniedException =>
        return ShuffleRecoveryBoundMapFailed(ShuffleRecoveryAdoptedUnavailable)
      case _: NoSuchFileException =>
        return ShuffleRecoveryBoundMapFailed(ShuffleRecoveryAdoptedMissing)
      case _: IOException =>
        val failureClass = validateRuntimeArtifact(active.provider, expected)
          .getOrElse(ShuffleRecoveryAdoptedUnavailable)
        return ShuffleRecoveryBoundMapFailed(failureClass)
      case NonFatal(_) =>
        return ShuffleRecoveryBoundMapFailed(ShuffleRecoveryAdoptedUnavailable)
    }

    if (resolved.numReducers <= 0 ||
        resolved.dataLength != expected.dataLength ||
        resolved.indexBytes != expected.indexLength) {
      ShuffleRecoveryBoundMapFailed(ShuffleRecoveryAdoptedCorrupt)
    } else {
      ShuffleRecoveryBoundMapOpened(resolved)
    }
  }

  private def openExistingProvider(
      request: ShuffleRecoveryClaimRequest):
      Either[ShuffleRecoveryClaimResult, ReferenceShuffleProvider] = {
    val normalizedRoot = providerRoot.toAbsolutePath.normalize()
    val group = Base64.getUrlEncoder.withoutPadding().encodeToString(
      request.recoveryGroup.getBytes(StandardCharsets.UTF_8))
    val incarnation = Base64.getUrlEncoder.withoutPadding().encodeToString(
      request.incarnationId.getBytes(StandardCharsets.UTF_8))
    val incarnationDirectory = normalizedRoot
      .resolve(group)
      .resolve(request.publishingGeneration.toString)
      .resolve(incarnation)
    if (!incarnationDirectory.normalize().startsWith(normalizedRoot)) {
      return Left(ShuffleRecoveryClaimRejected(
        "provider claim namespace escapes the configured root"))
    }
    val requiredDirectories = Seq(
      incarnationDirectory,
      incarnationDirectory.resolve(".attempts"),
      incarnationDirectory.resolve("maps"))
    requiredDirectories.foreach { path =>
      validateExistingPath(path, expectDirectory = true) match {
        case Some(result) => return Left(result)
        case None =>
      }
    }
    try {
      Right(ReferenceShuffleProvider.open(
        providerRoot,
        request.recoveryGroup,
        request.publishingGeneration,
        request.incarnationId,
        conf))
    } catch {
      case _: AccessDeniedException => Left(ShuffleRecoveryClaimUnavailable)
      case _: NoSuchFileException => Left(ShuffleRecoveryClaimMissing)
      case _: IOException => Left(ShuffleRecoveryClaimUnavailable)
      case NonFatal(_) => Left(ShuffleRecoveryClaimUnavailable)
    }
  }

  private def validateExistingPath(
      path: Path,
      expectDirectory: Boolean): Option[ShuffleRecoveryClaimResult] = {
    try {
      val attributes = Files.readAttributes(
        path,
        classOf[BasicFileAttributes],
        LinkOption.NOFOLLOW_LINKS)
      if (attributes.isSymbolicLink ||
          (expectDirectory && !attributes.isDirectory) ||
          (!expectDirectory && !attributes.isRegularFile)) {
        Some(ShuffleRecoveryClaimCorrupt)
      } else {
        None
      }
    } catch {
      case _: NoSuchFileException => Some(ShuffleRecoveryClaimMissing)
      case _: AccessDeniedException => Some(ShuffleRecoveryClaimUnavailable)
      case _: IOException => Some(ShuffleRecoveryClaimUnavailable)
      case NonFatal(_) => Some(ShuffleRecoveryClaimUnavailable)
    }
  }

  private def classifyValidatedReadFailure(
      mapDirectory: Path,
      requiredFiles: Seq[Path]): ShuffleRecoveryClaimResult = {
    validateExistingPath(mapDirectory, expectDirectory = true) match {
      case Some(result) => return result
      case None =>
    }
    var unavailable = false
    requiredFiles.foreach { path =>
      validateExistingPath(path, expectDirectory = false) match {
        case Some(ShuffleRecoveryClaimMissing) => return ShuffleRecoveryClaimMissing
        case Some(ShuffleRecoveryClaimCorrupt) => return ShuffleRecoveryClaimCorrupt
        case Some(ShuffleRecoveryClaimUnavailable) => unavailable = true
        case Some(_) => unavailable = true
        case None =>
      }
    }
    if (unavailable) ShuffleRecoveryClaimUnavailable else ShuffleRecoveryClaimCorrupt
  }

  private def validateRuntimeArtifact(
      provider: ReferenceShuffleProvider,
      expected: ShuffleRecoveryPreparedMap):
      Option[ShuffleRecoveryAdoptedReadFailureClass] = {
    val mapDirectory = provider.committedMapDirectory(expected.mapIndex)
    val winner = mapDirectory.resolveSibling(s"map-${expected.mapIndex}.winner")
    val ready = mapDirectory.resolve(ReferenceShuffleProvider.ReadyFileName)
    val data = mapDirectory.resolve(ReferenceShuffleProvider.DataFileName)
    val index = mapDirectory.resolve(ReferenceShuffleProvider.IndexFileName)

    runtimePathFailure(mapDirectory, expectDirectory = true) match {
      case Some(failureClass) => return Some(failureClass)
      case None =>
    }
    Seq(winner, ready, data, index).foreach { path =>
      runtimePathFailure(path, expectDirectory = false) match {
        case Some(failureClass) => return Some(failureClass)
        case None =>
      }
    }

    try {
      val winnerSize = Files.size(winner)
      if (winnerSize <= 0L || winnerSize > MaxWinnerHandleBytes) {
        return Some(ShuffleRecoveryAdoptedCorrupt)
      }
      val winnerHandle = new String(Files.readAllBytes(winner), StandardCharsets.UTF_8).trim
      if (winnerHandle != expected.providerHandle ||
          Files.size(data) != expected.dataLength ||
          Files.size(index) != expected.indexLength ||
          Files.size(ready) != 1L) {
        return Some(ShuffleRecoveryAdoptedCorrupt)
      }
      val digest = toHex(digestExactIndex(index, expected.indexLength))
      if (digest != expected.exactIndexDigest) {
        Some(ShuffleRecoveryAdoptedCorrupt)
      } else {
        None
      }
    } catch {
      case _: NoSuchFileException => Some(ShuffleRecoveryAdoptedMissing)
      case _: AccessDeniedException => Some(ShuffleRecoveryAdoptedUnavailable)
      case _: IOException => Some(ShuffleRecoveryAdoptedUnavailable)
      case NonFatal(_) => Some(ShuffleRecoveryAdoptedUnavailable)
    }
  }

  private def runtimePathFailure(
      path: Path,
      expectDirectory: Boolean): Option[ShuffleRecoveryAdoptedReadFailureClass] = {
    validateExistingPath(path, expectDirectory) match {
      case Some(ShuffleRecoveryClaimMissing) => Some(ShuffleRecoveryAdoptedMissing)
      case Some(ShuffleRecoveryClaimCorrupt) => Some(ShuffleRecoveryAdoptedCorrupt)
      case Some(_) => Some(ShuffleRecoveryAdoptedUnavailable)
      case None => None
    }
  }

  private def validateRequest(request: ShuffleRecoveryClaimRequest): Option[String] = {
    if (request == null || request.recoveryGroup == null || request.incarnationId == null ||
        request.providerCompatibilityId == null || request.mapArtifacts == null) {
      Some("claim request contains a null field")
    } else if (!safeIdentifier(request.recoveryGroup) ||
        !safeIdentifier(request.incarnationId)) {
      Some("claim request contains an invalid provider namespace")
    } else if (request.providerCompatibilityId != compatibilityId) {
      Some("provider compatibility id does not match the reference provider")
    } else if (request.publishingGeneration <= 0L || request.targetShuffleId < 0) {
      Some("claim request contains an invalid generation or target shuffle id")
    } else if (request.mapperCount < 0 ||
        request.mapperCount > ShuffleRecoveryManifestCodec.MaxMaps ||
        request.mapArtifacts.size != request.mapperCount) {
      Some("claim request contains an invalid mapper shape")
    } else if (request.reducerCount <= 0 ||
        request.reducerCount > ShuffleRecoveryManifestCodec.MaxReducers) {
      Some("claim request contains an invalid reducer shape")
    } else {
      None
    }
  }

  private def safeIdentifier(value: String): Boolean = {
    try {
      ShuffleRecoveryManifestCodec.validateIdentifier(value, "provider claim identifier")
      true
    } catch {
      case NonFatal(_) => false
    }
  }

  private def inspectMap(
      provider: ReferenceShuffleProvider,
      request: ShuffleRecoveryClaimRequest,
      artifact: ShuffleRecoveryMapArtifact):
      Either[ShuffleRecoveryClaimResult, ShuffleRecoveryClaimedMapDescriptor] = {
    if (artifact == null || artifact.mapIndex < 0 || artifact.mapIndex >= request.mapperCount) {
      return Left(ShuffleRecoveryClaimRejected("claim request contains an invalid map artifact"))
    }
    val mapDirectory = provider.committedMapDirectory(artifact.mapIndex)
    val winner = mapDirectory.resolveSibling(s"map-${artifact.mapIndex}.winner")
    validateExistingPath(mapDirectory, expectDirectory = true) match {
      case Some(result) => return Left(result)
      case None =>
    }

    val requiredFiles = Seq(
      winner,
      mapDirectory.resolve(ReferenceShuffleProvider.ReadyFileName),
      mapDirectory.resolve(ReferenceShuffleProvider.DataFileName),
      mapDirectory.resolve(ReferenceShuffleProvider.IndexFileName))
    requiredFiles.foreach { path =>
      validateExistingPath(path, expectDirectory = false) match {
        case Some(result) => return Left(result)
        case None =>
      }
    }

    val winnerHandle = try {
      if (Files.size(winner) <= 0L || Files.size(winner) > MaxWinnerHandleBytes) {
        return Left(ShuffleRecoveryClaimCorrupt)
      }
      new String(Files.readAllBytes(winner), StandardCharsets.UTF_8).trim
    } catch {
      case _: AccessDeniedException => return Left(ShuffleRecoveryClaimUnavailable)
      case _: NoSuchFileException => return Left(ShuffleRecoveryClaimMissing)
      case _: IOException => return Left(ShuffleRecoveryClaimUnavailable)
    }
    if (winnerHandle != artifact.providerHandle) {
      return Left(ShuffleRecoveryClaimRejected(
        "manifest provider handle does not match the committed winner"))
    }

    val resolved = try {
      provider.openMap(artifact.mapIndex)
    } catch {
      case _: AccessDeniedException => return Left(ShuffleRecoveryClaimUnavailable)
      case _: NoSuchFileException => return Left(ShuffleRecoveryClaimMissing)
      case _: IOException =>
        return Left(classifyValidatedReadFailure(mapDirectory, requiredFiles))
    }
    if (resolved.numReducers != request.reducerCount ||
        resolved.dataLength != artifact.dataLength ||
        resolved.indexBytes != artifact.indexLength) {
      return Left(ShuffleRecoveryClaimRejected(
        "manifest artifact shape does not match immutable provider bytes"))
    }

    var emptyBlocks = 0
    var nonEmptyBlocks = 0
    var physicalBlockBytes = 0L
    var reduceId = 0
    try {
      while (reduceId < request.reducerCount) {
        val block = resolved.blockMetadata(reduceId)
        if (block.offset < 0L || block.length < 0L) {
          return Left(ShuffleRecoveryClaimCorrupt)
        }
        if (block.isEmpty) {
          emptyBlocks = Math.addExact(emptyBlocks, 1)
        } else {
          nonEmptyBlocks = Math.addExact(nonEmptyBlocks, 1)
          physicalBlockBytes = Math.addExact(physicalBlockBytes, block.length)
        }
        reduceId += 1
      }
    } catch {
      case _: ArithmeticException => return Left(ShuffleRecoveryClaimCorrupt)
      case _: IllegalArgumentException => return Left(ShuffleRecoveryClaimCorrupt)
    }
    if (physicalBlockBytes != resolved.dataLength) {
      return Left(ShuffleRecoveryClaimCorrupt)
    }

    val indexPath = mapDirectory.resolve(ReferenceShuffleProvider.IndexFileName)
    val digest = try {
      digestExactIndex(indexPath, resolved.indexBytes)
    } catch {
      case _: AccessDeniedException => return Left(ShuffleRecoveryClaimUnavailable)
      case _: NoSuchFileException => return Left(ShuffleRecoveryClaimMissing)
      case _: IOException =>
        return Left(classifyValidatedReadFailure(mapDirectory, requiredFiles))
    }
    Right(ShuffleRecoveryClaimedMapDescriptor(
      artifact.mapIndex,
      winnerHandle,
      resolved.numReducers,
      resolved.dataLength,
      resolved.indexBytes,
      digest,
      emptyBlocks,
      nonEmptyBlocks,
      physicalBlockBytes))
  }

  private def digestExactIndex(indexPath: Path, expectedLength: Long): Array[Byte] = {
    if (!Files.isRegularFile(indexPath, LinkOption.NOFOLLOW_LINKS) ||
        Files.isSymbolicLink(indexPath)) {
      throw new IOException("shuffle recovery index is not a safe regular file")
    }
    val actualLength = Files.size(indexPath)
    if (actualLength != expectedLength || actualLength <= 0L ||
        actualLength > MaxExactIndexBytes) {
      throw new IOException("shuffle recovery index length is outside the safe bound")
    }
    val digest = MessageDigest.getInstance("SHA-256")
    val in = new BufferedInputStream(Files.newInputStream(indexPath, LinkOption.NOFOLLOW_LINKS))
    try {
      val buffer = new Array[Byte](8192)
      var read = in.read(buffer)
      while (read >= 0) {
        if (read > 0) {
          digest.update(buffer, 0, read)
        }
        read = in.read(buffer)
      }
    } finally {
      in.close()
    }
    digest.digest()
  }

  private def toHex(bytes: Array[Byte]): String = {
    val builder = new StringBuilder(bytes.length * 2)
    bytes.foreach { value =>
      builder.append(f"${value & 0xff}%02x")
    }
    builder.result()
  }
}

private[shuffle] object ReferenceShuffleRecoveryClaimProvider {
  private val MaxWinnerHandleBytes = 256L
  private val MaxExactIndexBytes = 64L * 1024L * 1024L
}
