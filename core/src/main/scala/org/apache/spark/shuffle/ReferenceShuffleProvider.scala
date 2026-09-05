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

import java.io.{ByteArrayOutputStream, DataOutputStream, FilterOutputStream}
import java.io.{IOException, OutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{AtomicMoveNotSupportedException, FileAlreadyExistsException, LinkOption}
import java.nio.file.{Files, Path, StandardCopyOption, StandardOpenOption}
import java.util.Base64
import java.util.zip.CRC32

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.util.TransportConf
import org.apache.spark.shuffle.api.{ShuffleMapOutputWriter, ShufflePartitionWriter}
import org.apache.spark.shuffle.api.metadata.{MapOutputCommitMessage, MapOutputMetadata}

/**
 * Feasibility-only persistent shuffle storage used to prove fetch-correct cross-process reads.
 *
 * The store deliberately keeps attempt outputs separate from authoritative winner selection.
 * A caller must supply the exact committed attempt descriptor and map index. The store never
 * guesses which speculative or retried attempt won.
 *
 * Committed map artifacts are immutable directories containing one consolidated data file and
 * one exact reducer index. Empty reducers are represented by equal adjacent offsets. Non-empty
 * reducers are exposed as exact [[FileSegmentManagedBuffer]] ranges, matching the fundamental
 * addressing model used by [[IndexShuffleBlockResolver]].
 *
 * This is intentionally narrower than a production durable-shuffle provider contract. It uses
 * same-filesystem atomic directory rename semantics available on the local reference filesystem
 * and must not be generalized to object stores.
 */
private[spark] final class ReferenceShuffleProvider private (
    root: Path,
    recoveryGroup: String,
    generation: Long,
    artifactIncarnation: String,
    conf: SparkConf) {

  import ReferenceShuffleProvider._

  require(generation >= 0L, s"generation must be non-negative: $generation")

  private val normalizedRoot = root.toAbsolutePath.normalize()
  private val groupDirectory = normalizedRoot.resolve(encodeIdentifier(recoveryGroup))
  private val generationDirectory = groupDirectory.resolve(generation.toString)
  private val incarnationDirectory =
    generationDirectory.resolve(encodeIdentifier(artifactIncarnation))
  private val attemptsDirectory = incarnationDirectory.resolve(".attempts")
  private val mapsDirectory = incarnationDirectory.resolve("maps")

  ensureInsideRoot(incarnationDirectory)
  initializeDirectories()

  private val transportConf: TransportConf = {
    val securityManager = new SecurityManager(conf)
    SparkTransportConf.fromSparkConf(
      conf,
      "shuffle",
      sslOptions = Some(securityManager.getRpcSSLOptions()))
  }

  def createMapOutputWriter(
      mapTaskId: Long,
      numPartitions: Int): ShuffleMapOutputWriter = {
    require(mapTaskId >= 0L, s"mapTaskId must be non-negative: $mapTaskId")
    validateReducerCount(numPartitions)
    val candidateName = s"attempt-$mapTaskId-${java.util.UUID.randomUUID()}"
    val candidateDirectory = attemptsDirectory.resolve(candidateName)
    ensureInsideRoot(candidateDirectory)
    createDirectory(candidateDirectory)
    new ReferenceShuffleMapOutputWriter(
      candidateDirectory,
      candidateName,
      mapTaskId,
      numPartitions)
  }

  /**
   * Atomically binds one completed attempt output to an authoritative map index.
   *
   * The caller owns winner selection. Existing committed bytes are never replaced. If two
   * callers race to bind the same map index, a create-new winner claim fences all but one caller
   * before the same-filesystem atomic directory move. A loser cannot overwrite winner bytes.
   */
  def commitWinner(
      mapIndex: Int,
      output: ReferenceShuffleOutputDescriptor): Unit = {
    require(mapIndex >= 0, s"mapIndex must be non-negative: $mapIndex")
    if (output == null) {
      throw new IllegalArgumentException("output descriptor must not be null")
    }
    validateCandidateName(output.candidateName)
    validateReducerCount(output.numPartitions)
    require(output.dataLength >= 0L, s"negative data length: ${output.dataLength}")
    require(output.indexLength > 0L, s"invalid index length: ${output.indexLength}")

    val candidateDirectory = attemptsDirectory.resolve(output.candidateName)
    val targetDirectory = mapsDirectory.resolve(s"map-$mapIndex")
    val winnerFile = mapsDirectory.resolve(s"map-$mapIndex.winner")
    ensureInsideRoot(candidateDirectory)
    ensureInsideRoot(targetDirectory)
    ensureInsideRoot(winnerFile)

    if (!isDirectory(candidateDirectory) ||
        !isRegularFile(candidateDirectory.resolve(ReadyFileName))) {
      throw new IOException(s"candidate ${output.candidateName} is not fully committed")
    }

    if (!output.candidateName.startsWith(s"attempt-${output.mapTaskId}-")) {
      throw new IOException("candidate descriptor map task id does not match its namespace")
    }

    val index = ReferenceShuffleIndexCodec.read(
      candidateDirectory.resolve(IndexFileName),
      candidateDirectory.resolve(DataFileName))
    if (index.numReducers != output.numPartitions ||
        index.dataLength != output.dataLength ||
        Files.size(candidateDirectory.resolve(IndexFileName)) != output.indexLength) {
      throw new IOException("candidate descriptor does not match committed bytes")
    }

    if (Files.exists(targetDirectory, LinkOption.NOFOLLOW_LINKS)) {
      throw new FileAlreadyExistsException(targetDirectory.toString)
    }

    createWinnerClaim(winnerFile, output.candidateName)
    var moved = false
    try {
      Files.move(candidateDirectory, targetDirectory, StandardCopyOption.ATOMIC_MOVE)
      moved = true
    } catch {
      case e: AtomicMoveNotSupportedException =>
        throw new IOException(
          "reference provider requires same-filesystem atomic directory rename",
          e)
    } finally {
      if (!moved) {
        Files.deleteIfExists(winnerFile)
      }
    }
  }

  def openMap(mapIndex: Int): ReferenceShuffleResolvedMap = {
    require(mapIndex >= 0, s"mapIndex must be non-negative: $mapIndex")
    val mapDirectory = mapsDirectory.resolve(s"map-$mapIndex")
    val winnerFile = mapsDirectory.resolve(s"map-$mapIndex.winner")
    ensureInsideRoot(mapDirectory)
    ensureInsideRoot(winnerFile)
    if (!isRegularFile(winnerFile) ||
        !isDirectory(mapDirectory) ||
        !isRegularFile(mapDirectory.resolve(ReadyFileName))) {
      throw new IOException(s"map $mapIndex is not committed")
    }
    val dataFile = mapDirectory.resolve(DataFileName)
    val index = ReferenceShuffleIndexCodec.read(mapDirectory.resolve(IndexFileName), dataFile)
    new ReferenceShuffleResolvedMap(dataFile, index, transportConf)
  }

  def cleanupAttempt(output: ReferenceShuffleOutputDescriptor): Unit = {
    if (output == null) {
      throw new IllegalArgumentException("output descriptor must not be null")
    }
    validateCandidateName(output.candidateName)
    val candidateDirectory = attemptsDirectory.resolve(output.candidateName)
    ensureInsideRoot(candidateDirectory)
    deleteRecursively(candidateDirectory)
  }

  def cleanupGroup(): Unit = {
    deleteRecursively(groupDirectory)
  }

  private[shuffle] def committedMapDirectory(mapIndex: Int): Path = {
    mapsDirectory.resolve(s"map-$mapIndex")
  }

  private[shuffle] def attemptsPath: Path = attemptsDirectory

  private[shuffle] def committedMapCount: Int = {
    val stream = Files.list(mapsDirectory)
    try {
      var count = 0
      val iterator = stream.iterator()
      while (iterator.hasNext) {
        if (isDirectory(iterator.next())) {
          count += 1
        }
      }
      count
    } finally {
      stream.close()
    }
  }

  private def createWinnerClaim(path: Path, candidateName: String): Unit = {
    val channel = java.nio.channels.FileChannel.open(
      path,
      StandardOpenOption.CREATE_NEW,
      StandardOpenOption.WRITE)
    try {
      val bytes = ByteBuffer.wrap(candidateName.getBytes(StandardCharsets.UTF_8))
      while (bytes.hasRemaining) {
        channel.write(bytes)
      }
      channel.force(true)
    } catch {
      case e: IOException =>
        try {
          channel.close()
        } finally {
          Files.deleteIfExists(path)
        }
        throw e
    } finally {
      if (channel.isOpen) {
        channel.close()
      }
    }
  }

  private def initializeDirectories(): Unit = {
    Files.createDirectories(normalizedRoot)
    if (!isDirectory(normalizedRoot)) {
      throw new IOException(s"reference shuffle root is not a directory: $normalizedRoot")
    }
    createDirectory(groupDirectory)
    createDirectory(generationDirectory)
    createDirectory(incarnationDirectory)
    createDirectory(attemptsDirectory)
    createDirectory(mapsDirectory)
  }

  private def createDirectory(path: Path): Unit = {
    try {
      Files.createDirectory(path)
    } catch {
      case _: FileAlreadyExistsException if isDirectory(path) =>
    }
    if (!isDirectory(path)) {
      throw new IOException(s"reference shuffle path is not a directory: $path")
    }
  }

  private def isDirectory(path: Path): Boolean = {
    Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)
  }

  private def isRegularFile(path: Path): Boolean = {
    Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
  }

  private def ensureInsideRoot(path: Path): Unit = {
    if (!path.toAbsolutePath.normalize().startsWith(normalizedRoot)) {
      throw new IllegalArgumentException("resolved path escapes reference shuffle root")
    }
  }
}

private[spark] final case class ReferenceShuffleOutputDescriptor(
    candidateName: String,
    mapTaskId: Long,
    numPartitions: Int,
    dataLength: Long,
    indexLength: Long) extends MapOutputMetadata

private[spark] final case class ReferenceShuffleBlockMetadata(
    offset: Long,
    length: Long,
    checksum: Option[Long]) {
  def isEmpty: Boolean = length == 0L
}

private[spark] final class ReferenceShuffleResolvedMap(
    dataFile: Path,
    index: ReferenceShuffleMapIndex,
    transportConf: TransportConf) {

  def numReducers: Int = index.numReducers

  def dataLength: Long = index.dataLength

  def indexBytes: Long = index.serializedBytes

  def blockMetadata(reduceId: Int): ReferenceShuffleBlockMetadata = index.block(reduceId)

  def getBlockData(reduceId: Int): Option[ManagedBuffer] = {
    val metadata = index.block(reduceId)
    if (metadata.isEmpty) {
      None
    } else {
      Some(new FileSegmentManagedBuffer(
        transportConf,
        dataFile.toFile,
        metadata.offset,
        metadata.length))
    }
  }
}

private[spark] object ReferenceShuffleProvider {
  private val MaxIdentifierBytes = 128
  private val AllowedIdentifier = "[A-Za-z0-9._-]+".r

  private[shuffle] val DataFileName = "data"
  private[shuffle] val IndexFileName = "index"
  private[shuffle] val ReadyFileName = "READY"

  val MaxReducers = 1000000

  def open(
      root: Path,
      recoveryGroup: String,
      generation: Long,
      artifactIncarnation: String,
      conf: SparkConf = new SparkConf(false)): ReferenceShuffleProvider = {
    if (root == null) {
      throw new IllegalArgumentException("root must not be null")
    }
    if (conf == null) {
      throw new IllegalArgumentException("conf must not be null")
    }
    validateIdentifier(recoveryGroup, "recoveryGroup")
    validateIdentifier(artifactIncarnation, "artifactIncarnation")
    new ReferenceShuffleProvider(
      root,
      recoveryGroup,
      generation,
      artifactIncarnation,
      conf)
  }

  private[shuffle] def validateReducerCount(numReducers: Int): Unit = {
    if (numReducers <= 0 || numReducers > MaxReducers) {
      throw new IllegalArgumentException(
        s"numReducers must be between 1 and $MaxReducers: $numReducers")
    }
  }

  private def validateIdentifier(value: String, field: String): Unit = {
    if (value == null) {
      throw new IllegalArgumentException(s"$field must not be null")
    }
    val bytes = value.getBytes(StandardCharsets.UTF_8)
    if (bytes.isEmpty || bytes.length > MaxIdentifierBytes) {
      throw new IllegalArgumentException(s"invalid $field length: ${bytes.length}")
    }
    if (value == "." || value == ".." || !AllowedIdentifier.pattern.matcher(value).matches()) {
      throw new IllegalArgumentException(s"invalid $field")
    }
  }

  private[shuffle] def validateCandidateName(value: String): Unit = {
    validateIdentifier(value, "candidateName")
    if (!value.startsWith("attempt-")) {
      throw new IllegalArgumentException("invalid candidateName")
    }
  }

  private def encodeIdentifier(value: String): String = {
    Base64.getUrlEncoder.withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8))
  }

  private[shuffle] def deleteRecursively(path: Path): Unit = {
    if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
      return
    }
    val stream = Files.walk(path)
    try {
      val paths = new ArrayBuffer[Path]
      val iterator = stream.iterator()
      while (iterator.hasNext) {
        paths += iterator.next()
      }
      paths.sortBy(_.getNameCount).reverseIterator.foreach { current =>
        Files.deleteIfExists(current)
      }
    } finally {
      stream.close()
    }
  }
}

private final class ReferenceShuffleMapOutputWriter(
    candidateDirectory: Path,
    candidateName: String,
    mapTaskId: Long,
    numPartitions: Int) extends ShuffleMapOutputWriter {

  import ReferenceShuffleProvider._

  private val partitionWriters = new Array[ReferenceShufflePartitionWriter](numPartitions)
  private var lastReduceId = -1
  private var committed = false
  private var aborted = false

  override def getPartitionWriter(reducePartitionId: Int): ShufflePartitionWriter = synchronized {
    ensureOpen()
    if (reducePartitionId < 0 || reducePartitionId >= numPartitions) {
      throw new IllegalArgumentException(s"invalid reduce partition: $reducePartitionId")
    }
    if (reducePartitionId <= lastReduceId) {
      throw new IllegalArgumentException("reduce partitions must be requested in increasing order")
    }
    lastReduceId = reducePartitionId
    val writer = new ReferenceShufflePartitionWriter(
      candidateDirectory.resolve(s"part-$reducePartitionId.tmp"))
    partitionWriters(reducePartitionId) = writer
    writer
  }

  override def commitAllPartitions(checksums: Array[Long]): MapOutputCommitMessage = synchronized {
    ensureOpen()
    if (checksums == null) {
      throw new IllegalArgumentException("checksums must not be null")
    }
    if (checksums.nonEmpty && checksums.length != numPartitions) {
      throw new IllegalArgumentException("checksum count does not match reducer count")
    }

    closePartitionWriters()
    val lengths = new Array[Long](numPartitions)
    val dataFile = candidateDirectory.resolve(DataFileName)
    val out = Files.newOutputStream(
      dataFile,
      StandardOpenOption.CREATE_NEW,
      StandardOpenOption.WRITE)
    try {
      var reduceId = 0
      while (reduceId < numPartitions) {
        val writer = partitionWriters(reduceId)
        if (writer != null) {
          val part = writer.path
          lengths(reduceId) = Files.size(part)
          Files.copy(part, out)
        }
        reduceId += 1
      }
    } finally {
      out.close()
    }
    forceFile(dataFile)

    val indexFile = candidateDirectory.resolve(IndexFileName)
    ReferenceShuffleIndexCodec.write(indexFile, lengths, checksums)
    forceFile(indexFile)

    var reduceId = 0
    while (reduceId < numPartitions) {
      val writer = partitionWriters(reduceId)
      if (writer != null) {
        Files.deleteIfExists(writer.path)
      }
      reduceId += 1
    }

    Files.write(
      candidateDirectory.resolve(ReadyFileName),
      Array[Byte](1),
      StandardOpenOption.CREATE_NEW,
      StandardOpenOption.WRITE)
    forceFile(candidateDirectory.resolve(ReadyFileName))

    committed = true
    val descriptor = ReferenceShuffleOutputDescriptor(
      candidateName,
      mapTaskId,
      numPartitions,
      Files.size(dataFile),
      Files.size(indexFile))
    MapOutputCommitMessage.of(lengths.clone(), descriptor)
  }

  override def abort(error: Throwable): Unit = synchronized {
    if (!committed && !aborted) {
      try {
        closePartitionWriters()
      } finally {
        try {
          deleteRecursively(candidateDirectory)
        } finally {
          aborted = true
        }
      }
    }
  }

  private def closePartitionWriters(): Unit = {
    partitionWriters.foreach { writer =>
      if (writer != null) {
        writer.closeIfOpen()
      }
    }
  }

  private def ensureOpen(): Unit = {
    if (committed) {
      throw new IllegalStateException("map output is already committed")
    }
    if (aborted) {
      throw new IllegalStateException("map output is already aborted")
    }
  }

  private def forceFile(path: Path): Unit = {
    val channel = java.nio.channels.FileChannel.open(path, StandardOpenOption.WRITE)
    try {
      channel.force(true)
    } finally {
      channel.close()
    }
  }
}

private final class ReferenceShufflePartitionWriter(val path: Path) extends ShufflePartitionWriter {
  private var stream: CountingOutputStream = _

  override def openStream(): OutputStream = synchronized {
    if (stream != null) {
      throw new IllegalStateException("partition stream is already open")
    }
    val raw = Files.newOutputStream(
      path,
      StandardOpenOption.CREATE_NEW,
      StandardOpenOption.WRITE)
    stream = new CountingOutputStream(raw)
    stream
  }

  override def getNumBytesWritten(): Long = synchronized {
    if (stream == null) 0L else stream.count
  }

  def closeIfOpen(): Unit = synchronized {
    if (stream != null) {
      stream.close()
    }
  }
}

private final class CountingOutputStream(out: OutputStream) extends FilterOutputStream(out) {
  var count: Long = 0L

  override def write(value: Int): Unit = {
    out.write(value)
    count = Math.addExact(count, 1L)
  }

  override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = {
    out.write(bytes, offset, length)
    count = Math.addExact(count, length.toLong)
  }
}

private[spark] final case class ReferenceShuffleMapIndex(
    numReducers: Int,
    dataLength: Long,
    blocks: Vector[ReferenceShuffleBlockMetadata],
    serializedBytes: Long) {

  def block(reduceId: Int): ReferenceShuffleBlockMetadata = {
    if (reduceId < 0 || reduceId >= numReducers) {
      throw new IllegalArgumentException(s"invalid reduce partition: $reduceId")
    }
    blocks(reduceId)
  }
}

private object ReferenceShuffleIndexCodec {
  private val Magic = 0x53525031
  private val Version = 1
  private val HasChecksums = 1
  private val HeaderBytes = 24L
  private val FooterBytes = 8L
  private val MaxIndexBytes = 64L * 1024L * 1024L

  def write(indexFile: Path, lengths: Array[Long], checksums: Array[Long]): Unit = {
    ReferenceShuffleProvider.validateReducerCount(lengths.length)
    if (checksums.nonEmpty && checksums.length != lengths.length) {
      throw new IllegalArgumentException("checksum count does not match reducer count")
    }

    val offsets = new Array[Long](lengths.length + 1)
    var i = 0
    while (i < lengths.length) {
      if (lengths(i) < 0L) {
        throw new IllegalArgumentException(s"negative block length at reducer $i")
      }
      offsets(i + 1) = Math.addExact(offsets(i), lengths(i))
      i += 1
    }

    val bytes = new ByteArrayOutputStream(expectedSize(lengths.length, checksums.nonEmpty).toInt)
    val out = new DataOutputStream(bytes)
    out.writeInt(Magic)
    out.writeInt(Version)
    out.writeInt(lengths.length)
    out.writeInt(if (checksums.nonEmpty) HasChecksums else 0)
    out.writeLong(offsets.last)
    offsets.foreach(out.writeLong)
    if (checksums.nonEmpty) {
      checksums.foreach(out.writeLong)
    }
    out.flush()

    val payload = bytes.toByteArray
    val crc = new CRC32()
    crc.update(payload)
    val fileOut = new DataOutputStream(Files.newOutputStream(
      indexFile,
      StandardOpenOption.CREATE_NEW,
      StandardOpenOption.WRITE))
    try {
      fileOut.write(payload)
      fileOut.writeLong(crc.getValue)
    } finally {
      fileOut.close()
    }
  }

  def read(indexFile: Path, dataFile: Path): ReferenceShuffleMapIndex = {
    if (!Files.isRegularFile(indexFile, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException(s"missing shuffle index: $indexFile")
    }
    if (!Files.isRegularFile(dataFile, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException(s"missing shuffle data: $dataFile")
    }
    val indexSize = Files.size(indexFile)
    if (indexSize < HeaderBytes + FooterBytes || indexSize > MaxIndexBytes) {
      throw new IOException(s"invalid shuffle index size: $indexSize")
    }
    if (indexSize > Int.MaxValue) {
      throw new IOException(s"shuffle index is too large to read safely: $indexSize")
    }

    val bytes = Files.readAllBytes(indexFile)
    val buffer = ByteBuffer.wrap(bytes)
    if (buffer.getInt() != Magic) {
      throw new IOException("invalid shuffle index magic")
    }
    if (buffer.getInt() != Version) {
      throw new IOException("unsupported shuffle index version")
    }
    val numReducers = buffer.getInt()
    try {
      ReferenceShuffleProvider.validateReducerCount(numReducers)
    } catch {
      case e: IllegalArgumentException => throw new IOException(e.getMessage, e)
    }
    val flags = buffer.getInt()
    if ((flags & ~HasChecksums) != 0) {
      throw new IOException(s"unsupported shuffle index flags: $flags")
    }
    val hasChecksums = (flags & HasChecksums) != 0
    val dataLength = buffer.getLong()
    if (dataLength < 0L) {
      throw new IOException(s"negative data length in shuffle index: $dataLength")
    }

    val expected = expectedSize(numReducers, hasChecksums)
    if (indexSize != expected) {
      throw new IOException(s"shuffle index size $indexSize does not match expected $expected")
    }

    val crc = new CRC32()
    crc.update(bytes, 0, bytes.length - FooterBytes.toInt)
    val storedCrc = ByteBuffer.wrap(bytes, bytes.length - FooterBytes.toInt, FooterBytes.toInt)
      .getLong()
    if (crc.getValue != storedCrc) {
      throw new IOException("shuffle index checksum mismatch")
    }

    val offsets = new Array[Long](numReducers + 1)
    var i = 0
    while (i < offsets.length) {
      offsets(i) = buffer.getLong()
      i += 1
    }
    if (offsets(0) != 0L || offsets.last != dataLength) {
      throw new IOException("shuffle index offsets do not span the data file")
    }

    val checksums = if (hasChecksums) {
      val values = new Array[Long](numReducers)
      var index = 0
      while (index < numReducers) {
        values(index) = buffer.getLong()
        index += 1
      }
      Some(values)
    } else {
      None
    }

    val blocks = Vector.newBuilder[ReferenceShuffleBlockMetadata]
    i = 0
    while (i < numReducers) {
      val start = offsets(i)
      val end = offsets(i + 1)
      if (start < 0L || end < start) {
        throw new IOException(s"non-monotonic shuffle offsets at reducer $i")
      }
      blocks += ReferenceShuffleBlockMetadata(
        start,
        end - start,
        checksums.map(_(i)))
      i += 1
    }

    val physicalDataLength = Files.size(dataFile)
    if (physicalDataLength != dataLength) {
      throw new IOException(
        s"shuffle data length $physicalDataLength does not match index length $dataLength")
    }
    ReferenceShuffleMapIndex(numReducers, dataLength, blocks.result(), indexSize)
  }

  private def expectedSize(numReducers: Int, hasChecksums: Boolean): Long = {
    val offsetsBytes = Math.multiplyExact(numReducers.toLong + 1L, 8L)
    val checksumBytes = if (hasChecksums) Math.multiplyExact(numReducers.toLong, 8L) else 0L
    val size = Math.addExact(HeaderBytes, offsetsBytes)
    val withChecksums = Math.addExact(size, checksumBytes)
    val total = Math.addExact(withChecksums, FooterBytes)
    if (total > MaxIndexBytes) {
      throw new IllegalArgumentException(s"shuffle index exceeds safe bound: $total")
    }
    total
  }
}
