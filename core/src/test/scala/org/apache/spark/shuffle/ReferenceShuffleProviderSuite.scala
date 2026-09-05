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

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}
import java.nio.file.attribute.{PosixFileAttributeView, PosixFilePermissions}
import java.util.Base64
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.CRC32

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage

class ReferenceShuffleProviderSuite extends SparkFunSuite {

  test("exact index preserves empty, one-byte, sparse, skewed, and zero-total blocks") {
    withProvider("exact-index") { provider =>
      assert(provider.committedMapCount == 0)

      val reducers = 1024
      val oneByte = Array[Byte](7)
      val skewed = Array.fill[Byte](1024 * 1024)(11)
      val tail = "tail".getBytes(StandardCharsets.UTF_8)
      val blocks = Map(1 -> oneByte, 512 -> skewed, 1023 -> tail)
      val first = writeCandidate(provider, 11L, reducers, blocks)
      provider.commitWinner(0, first)

      val resolved = provider.openMap(0)
      assert(resolved.numReducers == reducers)
      assert(resolved.dataLength == oneByte.length + skewed.length + tail.length)
      assert(resolved.indexBytes == 40L + 16L * reducers)
      assert(resolved.blockMetadata(0).isEmpty)
      assert(resolved.getBlockData(0).isEmpty)
      assert(readBlock(resolved, 1).sameElements(oneByte))
      assert(readBlock(resolved, 512).sameElements(skewed))
      assert(readBlock(resolved, 1023).sameElements(tail))
      assert(resolved.blockMetadata(1).length == 1L)
      assert(resolved.blockMetadata(1).checksum.contains(crc32(oneByte)))
      assert(resolved.blockMetadata(512).length == skewed.length.toLong)

      var reduceId = 2
      while (reduceId < reducers) {
        if (reduceId != 512 && reduceId != 1023) {
          assert(resolved.blockMetadata(reduceId).isEmpty)
        }
        reduceId += 1
      }

      val empty = writeCandidate(provider, 12L, reducers, Map.empty[Int, Array[Byte]])
      provider.commitWinner(1, empty)
      val emptyMap = provider.openMap(1)
      assert(emptyMap.dataLength == 0L)
      assert((0 until reducers).forall(id => emptyMap.blockMetadata(id).isEmpty))
      assert((0 until reducers).forall(id => emptyMap.getBlockData(id).isEmpty))

      val single = writeCandidate(provider, 13L, 1, Map(0 -> Array[Byte](1, 2, 3)))
      provider.commitWinner(2, single)
      assert(readBlock(provider.openMap(2), 0).sameElements(Array[Byte](1, 2, 3)))

      val noChecksumWriter = provider.createMapOutputWriter(14L, 2)
      val noChecksumStream = noChecksumWriter.getPartitionWriter(1).openStream()
      noChecksumStream.write(Array[Byte](4, 5))
      noChecksumStream.close()
      val noChecksum = descriptorOf(noChecksumWriter.commitAllPartitions(Array.empty[Long]))
      provider.commitWinner(3, noChecksum)
      val withoutChecksums = provider.openMap(3)
      assert(withoutChecksums.indexBytes == 40L + 8L * 2L)
      assert(withoutChecksums.blockMetadata(0).checksum.isEmpty)
      assert(withoutChecksums.blockMetadata(1).checksum.isEmpty)

      intercept[IllegalArgumentException] {
        provider.createMapOutputWriter(15L, 0)
      }
      intercept[IllegalArgumentException] {
        provider.createMapOutputWriter(16L, ReferenceShuffleProvider.MaxReducers + 1)
      }
      intercept[IllegalArgumentException] {
        provider.createMapOutputWriter(17L, Int.MaxValue)
      }
    }
  }

  test("large block keeps its physical size for fetch accounting") {
    withProvider("large-block") { provider =>
      val writer = provider.createMapOutputWriter(21L, 1)
      val partition = writer.getPartitionWriter(0)
      val out = partition.openStream()
      val chunk = Array.fill[Byte](1024 * 1024)(5)
      var written = 0
      while (written < 64) {
        out.write(chunk)
        written += 1
      }
      out.close()
      assert(partition.getNumBytesWritten() == 64L * 1024L * 1024L)

      val expectedChecksum = crc32Repeated(chunk, 64)
      val message = writer.commitAllPartitions(Array(expectedChecksum))
      val descriptor = descriptorOf(message)
      provider.commitWinner(0, descriptor)
      val resolved = provider.openMap(0)
      val metadata = resolved.blockMetadata(0)
      assert(metadata.length == 64L * 1024L * 1024L)
      assert(!metadata.isEmpty)
      assert(metadata.checksum.contains(expectedChecksum))

      val (fetchedBytes, fetchedChecksum) = readBlockDigest(resolved, 0)
      assert(fetchedBytes == metadata.length)
      assert(fetchedChecksum == expectedChecksum)
    }
  }

  test("winner binding is separate from attempt commit and cleanup preserves durable maps") {
    withRoot { root =>
      val provider = openProvider(root, "winner-boundary")
      val descriptor = writeCandidate(provider, 31L, 2, Map(1 -> Array[Byte](9)))

      assert(provider.committedMapCount == 0)
      intercept[IOException] {
        provider.openMap(0)
      }

      provider.commitWinner(0, descriptor)
      assert(provider.committedMapCount == 1)
      provider.cleanupAttempt(descriptor)
      assert(readBlock(provider.openMap(0), 1).sameElements(Array[Byte](9)))

      provider.cleanupGroup()
      provider.cleanupGroup()
      assert(directoryIsEmpty(root))
    }
  }

  test("partial candidates, corrupt indexes, missing data, and mismatches fail closed") {
    withProvider("failure-cases") { provider =>
      val partial = provider.createMapOutputWriter(41L, 2)
      val partialWriter = partial.getPartitionWriter(0)
      val partialStream = partialWriter.openStream()
      partialStream.write(Array[Byte](1, 2, 3))
      partial.abort(new IOException("injected"))
      assert(directoryIsEmpty(provider.attemptsPath))

      val stale = provider.attemptsPath.resolve("attempt-stale")
      Files.createDirectory(stale)
      Files.write(stale.resolve(ReferenceShuffleProvider.DataFileName), Array[Byte](1))
      Files.write(stale.resolve(ReferenceShuffleProvider.IndexFileName), Array[Byte](1, 2, 3))
      assert(provider.committedMapCount == 0)
      ReferenceShuffleProvider.deleteRecursively(stale)

      val descriptor = writeCandidate(provider, 42L, 2, Map(0 -> Array[Byte](4)))
      val candidate = provider.attemptsPath.resolve(descriptor.candidateName)
      Files.write(
        candidate.resolve(ReferenceShuffleProvider.IndexFileName),
        Array[Byte](1),
        StandardOpenOption.TRUNCATE_EXISTING)
      intercept[IOException] {
        provider.commitWinner(0, descriptor)
      }
      assert(provider.committedMapCount == 0)
      provider.cleanupAttempt(descriptor)

      val corrupt = writeCandidate(provider, 43L, 2, Map(0 -> Array[Byte](5, 6)))
      provider.commitWinner(1, corrupt)
      val corruptIndex = provider.committedMapDirectory(1)
        .resolve(ReferenceShuffleProvider.IndexFileName)
      val indexBytes = Files.readAllBytes(corruptIndex)
      indexBytes(24) = (indexBytes(24) ^ 1).toByte
      Files.write(corruptIndex, indexBytes, StandardOpenOption.TRUNCATE_EXISTING)
      intercept[IOException] {
        provider.openMap(1)
      }

      val missing = writeCandidate(provider, 44L, 2, Map(0 -> Array[Byte](7)))
      provider.commitWinner(2, missing)
      Files.delete(provider.committedMapDirectory(2)
        .resolve(ReferenceShuffleProvider.DataFileName))
      intercept[IOException] {
        provider.openMap(2)
      }

      val mismatch = writeCandidate(provider, 45L, 2, Map(0 -> Array[Byte](8)))
      provider.commitWinner(3, mismatch)
      Files.write(
        provider.committedMapDirectory(3).resolve(ReferenceShuffleProvider.DataFileName),
        Array[Byte](99),
        StandardOpenOption.APPEND)
      intercept[IOException] {
        provider.openMap(3)
      }

      val noWinner = writeCandidate(provider, 46L, 1, Map(0 -> Array[Byte](10)))
      provider.commitWinner(4, noWinner)
      Files.delete(provider.committedMapDirectory(4).resolveSibling("map-4.winner"))
      intercept[IOException] {
        provider.openMap(4)
      }
    }
  }

  test("oversized index is rejected before allocation") {
    withProvider("oversized-index") { provider =>
      val descriptor = writeCandidate(provider, 47L, 1, Map(0 -> Array[Byte](1)))
      provider.commitWinner(0, descriptor)
      val index = provider.committedMapDirectory(0)
        .resolve(ReferenceShuffleProvider.IndexFileName)
      val channel = Files.newByteChannel(index, StandardOpenOption.WRITE)
      try {
        channel.position(64L * 1024L * 1024L)
        channel.write(ByteBuffer.wrap(Array[Byte](1)))
      } finally {
        channel.close()
      }
      intercept[IOException] {
        provider.openMap(0)
      }
    }
  }

  test("concurrent winner selection cannot overwrite immutable committed bytes") {
    withProvider("concurrent-winner") { provider =>
      val firstBytes = "first".getBytes(StandardCharsets.UTF_8)
      val secondBytes = "second".getBytes(StandardCharsets.UTF_8)
      val first = writeCandidate(provider, 51L, 1, Map(0 -> firstBytes))
      val second = writeCandidate(provider, 52L, 1, Map(0 -> secondBytes))
      val start = new CountDownLatch(1)
      val ready = new CountDownLatch(2)
      val successes = new AtomicInteger(0)
      val failures = new ArrayBuffer[Throwable]
      val executor = Executors.newFixedThreadPool(2)

      def submit(descriptor: ReferenceShuffleOutputDescriptor): Unit = {
        executor.submit(new Runnable {
          override def run(): Unit = {
            ready.countDown()
            start.await()
            try {
              provider.commitWinner(0, descriptor)
              successes.incrementAndGet()
            } catch {
              case error: IOException => failures.synchronized { failures += error }
            }
          }
        })
      }

      submit(first)
      submit(second)
      assert(ready.await(30, TimeUnit.SECONDS))
      start.countDown()
      executor.shutdown()
      assert(executor.awaitTermination(30, TimeUnit.SECONDS))
      assert(successes.get() == 1)
      assert(failures.synchronized(failures.size) == 1)

      val bytes = readBlock(provider.openMap(0), 0)
      assert(bytes.sameElements(firstBytes) || bytes.sameElements(secondBytes))
      provider.cleanupAttempt(first)
      provider.cleanupAttempt(second)
      assert(directoryIsEmpty(provider.attemptsPath))
    }
  }

  test("duplicate map-output commit is deterministically rejected") {
    withProvider("duplicate-commit") { provider =>
      val writer = provider.createMapOutputWriter(61L, 1)
      val out = writer.getPartitionWriter(0).openStream()
      out.write(1)
      out.close()
      writer.commitAllPartitions(Array(1L))
      intercept[IllegalStateException] {
        writer.commitAllPartitions(Array(1L))
      }
    }
  }

  test("unsafe identifiers and invalid roots cannot escape the configured namespace") {
    withRoot { root =>
      Seq("..", ".", "../escape", "/absolute", "a/b", "a\\b", "a:b", "").foreach { id =>
        intercept[IllegalArgumentException] {
          ReferenceShuffleProvider.open(root, id, 1L, "incarnation")
        }
        intercept[IllegalArgumentException] {
          ReferenceShuffleProvider.open(root, "group", 1L, id)
        }
      }
      intercept[IllegalArgumentException] {
        ReferenceShuffleProvider.open(root, null, 1L, "incarnation")
      }
      intercept[IllegalArgumentException] {
        ReferenceShuffleProvider.open(root, "group", 1L, null)
      }
      intercept[IllegalArgumentException] {
        ReferenceShuffleProvider.open(null, "group", 1L, "incarnation")
      }
      intercept[IllegalArgumentException] {
        ReferenceShuffleProvider.open(root, "group", -1L, "incarnation")
      }
      val provider = ReferenceShuffleProvider.open(root, "valid-group", 1L, "valid-inc")
      intercept[IllegalArgumentException] {
        provider.commitWinner(0, null)
      }
      intercept[IllegalArgumentException] {
        provider.cleanupAttempt(null)
      }

      val outside = Files.createTempDirectory("reference-shuffle-outside")
      try {
        val encoded = Base64.getUrlEncoder.withoutPadding().encodeToString(
          "linked-group".getBytes(StandardCharsets.UTF_8))
        try {
          Files.createSymbolicLink(root.resolve(encoded), outside)
          intercept[IOException] {
            ReferenceShuffleProvider.open(root, "linked-group", 1L, "incarnation")
          }
        } catch {
          case _: UnsupportedOperationException =>
        }
      } finally {
        ReferenceShuffleProvider.deleteRecursively(outside)
      }
    }

    val file = Files.createTempFile("reference-shuffle-root", ".file")
    try {
      intercept[IOException] {
        ReferenceShuffleProvider.open(file, "group", 1L, "incarnation")
      }
    } finally {
      Files.deleteIfExists(file)
    }
  }

  test("non-writable POSIX root is rejected when the filesystem exposes permissions") {
    withRoot { root =>
      if (Files.getFileStore(root).supportsFileAttributeView(classOf[PosixFileAttributeView])) {
        val original = Files.getPosixFilePermissions(root)
        try {
          Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("r-xr-xr-x"))
          if (!Files.isWritable(root)) {
            intercept[IOException] {
              ReferenceShuffleProvider.open(root.resolve("child"), "group", 1L, "incarnation")
            }
          }
        } finally {
          Files.setPosixFilePermissions(root, original)
        }
      }
    }
  }

  test("repeated open, fetch, attempt cleanup, and group cleanup leaves no temporary files") {
    withRoot { root =>
      val provider = openProvider(root, "repetition")
      var mapIndex = 0
      while (mapIndex < 20) {
        val bytes = Array.fill[Byte](4096)(mapIndex.toByte)
        val descriptor = writeCandidate(
          provider,
          100L + mapIndex,
          8,
          Map(mapIndex % 8 -> bytes))
        provider.commitWinner(mapIndex, descriptor)
        assert(readBlock(provider.openMap(mapIndex), mapIndex % 8).sameElements(bytes))
        mapIndex += 1
      }
      assert(directoryIsEmpty(provider.attemptsPath))
      provider.cleanupGroup()
      assert(directoryIsEmpty(root))
    }
  }

  private def withProvider(name: String)(body: ReferenceShuffleProvider => Unit): Unit = {
    withRoot { root =>
      val provider = openProvider(root, name)
      try {
        body(provider)
      } finally {
        provider.cleanupGroup()
      }
    }
  }

  private def withRoot(body: Path => Unit): Unit = {
    val root = Files.createTempDirectory("reference-shuffle-provider")
    try {
      body(root)
    } finally {
      ReferenceShuffleProvider.deleteRecursively(root)
    }
  }

  private def openProvider(root: Path, incarnation: String): ReferenceShuffleProvider = {
    ReferenceShuffleProvider.open(
      root,
      "test-group",
      1L,
      incarnation,
      new SparkConf(false))
  }

  private def writeCandidate(
      provider: ReferenceShuffleProvider,
      mapTaskId: Long,
      reducers: Int,
      blocks: Map[Int, Array[Byte]]): ReferenceShuffleOutputDescriptor = {
    val writer = provider.createMapOutputWriter(mapTaskId, reducers)
    blocks.toSeq.sortBy(_._1).foreach { case (reduceId, bytes) =>
      val partition = writer.getPartitionWriter(reduceId)
      val out = partition.openStream()
      out.write(bytes)
      out.close()
      assert(partition.getNumBytesWritten() == bytes.length.toLong)
    }
    val checksums = Array.tabulate(reducers) { reduceId =>
      crc32(blocks.getOrElse(reduceId, Array.empty[Byte]))
    }
    descriptorOf(writer.commitAllPartitions(checksums))
  }

  private def descriptorOf(
      message: MapOutputCommitMessage): ReferenceShuffleOutputDescriptor = {
    assert(message.getMapOutputMetadata.isPresent)
    message.getMapOutputMetadata.get().asInstanceOf[ReferenceShuffleOutputDescriptor]
  }

  private def readBlock(resolved: ReferenceShuffleResolvedMap, reduceId: Int): Array[Byte] = {
    val buffer = resolved.getBlockData(reduceId).getOrElse {
      fail(s"expected non-empty block $reduceId")
    }
    val in = buffer.createInputStream()
    try {
      in.readAllBytes()
    } finally {
      in.close()
    }
  }

  private def readBlockDigest(
      resolved: ReferenceShuffleResolvedMap,
      reduceId: Int): (Long, Long) = {
    val buffer = resolved.getBlockData(reduceId).getOrElse {
      fail(s"expected non-empty block $reduceId")
    }
    val in = buffer.createInputStream()
    val crc = new CRC32()
    val chunk = new Array[Byte](64 * 1024)
    var total = 0L
    try {
      var read = in.read(chunk)
      while (read >= 0) {
        if (read > 0) {
          crc.update(chunk, 0, read)
          total = Math.addExact(total, read.toLong)
        }
        read = in.read(chunk)
      }
      (total, crc.getValue)
    } finally {
      in.close()
    }
  }

  private def crc32(bytes: Array[Byte]): Long = {
    val crc = new CRC32()
    crc.update(bytes)
    crc.getValue
  }

  private def crc32Repeated(bytes: Array[Byte], repetitions: Int): Long = {
    val crc = new CRC32()
    var i = 0
    while (i < repetitions) {
      crc.update(bytes)
      i += 1
    }
    crc.getValue
  }

  private def directoryIsEmpty(path: Path): Boolean = {
    val stream = Files.list(path)
    try {
      stream.findAny().isEmpty
    } finally {
      stream.close()
    }
  }
}
