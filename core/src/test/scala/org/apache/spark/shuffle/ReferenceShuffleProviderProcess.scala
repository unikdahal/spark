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
import java.nio.file.{Files, Path, Paths}
import java.util.zip.CRC32

import org.apache.spark.SparkConf
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage

/**
 * Fresh-process proof utility for the persistent reference shuffle representation.
 *
 * The write and read modes are intentionally separate entry points. CI invokes them in distinct
 * JVMs against the same filesystem root, so no object, singleton, or static state can bridge the
 * process boundary.
 */
private[spark] object ReferenceShuffleProviderProcess {
  private val Group = "cross-jvm-group"
  private val Generation = 1L
  private val ProofIncarnation = "proof"
  private val LargeIncarnation = "proof-large"
  private val LargeChunkBytes = 1024 * 1024
  private val LargeChunkCount = 64
  private val LargeBytes = Math.multiplyExact(LargeChunkBytes.toLong, LargeChunkCount.toLong)

  def main(args: Array[String]): Unit = {
    require(args.length >= 2, "usage: <write|read> <root> [report]")
    args(0) match {
      case "write" => write(Paths.get(args(1)))
      case "read" =>
        require(args.length == 3, "read mode requires a report path")
        read(Paths.get(args(1)), Paths.get(args(2)))
      case other => throw new IllegalArgumentException(s"unknown mode: $other")
    }
  }

  private def write(root: Path): Unit = {
    val proof = provider(root, ProofIncarnation)
    val sparse = Map(
      1 -> Array[Byte](1),
      512 -> Array.fill[Byte](1024 * 1024)(2),
      1023 -> "tail".getBytes(StandardCharsets.UTF_8))
    commitMap(proof, 0, 1001L, 1024, sparse)
    commitMap(proof, 1, 1002L, 1024, Map.empty[Int, Array[Byte]])
    require(directoryIsEmpty(proof.attemptsPath), "attempt files remained after winner commit")

    writeLargeBlock(root)
    writeEmptyShape(root, "shape-m0", maps = 0, reducers = 1, taskBase = 1500L)
    writeEmptyShape(root, "shape-r1", maps = 1, reducers = 1, taskBase = 2000L)
    writeEmptyShape(root, "shape-r128", maps = 4, reducers = 128, taskBase = 3000L)
    writeEmptyShape(root, "shape-r4096", maps = 2, reducers = 4096, taskBase = 4000L)

    writeSuccessMarker("REFERENCE_SHUFFLE_WRITE_OK")
  }

  private def read(root: Path, report: Path): Unit = {
    val proof = provider(root, ProofIncarnation)
    val sparse = proof.openMap(0)
    require(sparse.numReducers == 1024)
    require(sparse.blockMetadata(0).isEmpty)
    require(sparse.getBlockData(0).isEmpty)
    require(readBlock(sparse, 1).sameElements(Array[Byte](1)))
    require(readBlock(sparse, 512).length == 1024 * 1024)
    require(new String(readBlock(sparse, 1023), StandardCharsets.UTF_8) == "tail")

    var reduceId = 2
    while (reduceId < 1024) {
      if (reduceId != 512 && reduceId != 1023) {
        require(sparse.blockMetadata(reduceId).isEmpty)
      }
      reduceId += 1
    }

    val zeroTotal = proof.openMap(1)
    require(zeroTotal.dataLength == 0L)
    require((0 until zeroTotal.numReducers).forall(id => zeroTotal.getBlockData(id).isEmpty))

    val large = provider(root, LargeIncarnation).openMap(0)
    val largeMetadata = large.blockMetadata(0)
    require(largeMetadata.length == LargeBytes)
    require(!largeMetadata.isEmpty)
    val expectedLargeChecksum = crc32Repeated(
      Array.fill[Byte](LargeChunkBytes)(5),
      LargeChunkCount)
    require(largeMetadata.checksum.contains(expectedLargeChecksum))
    val (largeFetchedBytes, largeFetchedChecksum) = readBlockDigest(large, 0)
    require(largeFetchedBytes == LargeBytes)
    require(largeFetchedChecksum == expectedLargeChecksum)

    val shapes = Seq(
      measureShape(root, "shape-m0", maps = 0, reducers = 1),
      measureShape(root, "shape-r1", maps = 1, reducers = 1),
      measureShape(root, "shape-r128", maps = 4, reducers = 128),
      measureShape(root, "shape-r4096", maps = 2, reducers = 4096))

    val reportText = renderReport(sparse.indexBytes, shapes)
    Option(report.getParent).foreach { parent =>
      Files.createDirectories(parent)
    }
    Files.write(report, reportText.getBytes(StandardCharsets.UTF_8))

    proof.cleanupGroup()
    proof.cleanupGroup()
    require(directoryIsEmpty(root), "explicit group cleanup left durable artifacts")
    writeSuccessMarker("REFERENCE_SHUFFLE_READ_OK")
  }

  private def writeLargeBlock(root: Path): Unit = {
    val store = provider(root, LargeIncarnation)
    val writer = store.createMapOutputWriter(1200L, 1)
    val partition = writer.getPartitionWriter(0)
    val out = partition.openStream()
    val chunk = Array.fill[Byte](LargeChunkBytes)(5)
    try {
      var written = 0
      while (written < LargeChunkCount) {
        out.write(chunk)
        written += 1
      }
    } finally {
      out.close()
    }
    require(partition.getNumBytesWritten() == LargeBytes)
    val checksum = crc32Repeated(chunk, LargeChunkCount)
    store.commitWinner(0, descriptorOf(writer.commitAllPartitions(Array(checksum))))
  }

  private def writeEmptyShape(
      root: Path,
      incarnation: String,
      maps: Int,
      reducers: Int,
      taskBase: Long): Unit = {
    val store = provider(root, incarnation)
    var mapIndex = 0
    while (mapIndex < maps) {
      commitMap(
        store,
        mapIndex,
        taskBase + mapIndex,
        reducers,
        Map.empty[Int, Array[Byte]])
      mapIndex += 1
    }
  }

  private def measureShape(
      root: Path,
      incarnation: String,
      maps: Int,
      reducers: Int): (Int, Int, Long) = {
    val store = provider(root, incarnation)
    require(store.committedMapCount == maps)
    var totalIndexBytes = 0L
    var mapIndex = 0
    while (mapIndex < maps) {
      val resolved = store.openMap(mapIndex)
      require(resolved.numReducers == reducers)
      var reduceId = 0
      while (reduceId < reducers) {
        require(resolved.blockMetadata(reduceId).isEmpty)
        require(resolved.getBlockData(reduceId).isEmpty)
        reduceId += 1
      }
      totalIndexBytes = Math.addExact(totalIndexBytes, resolved.indexBytes)
      mapIndex += 1
    }
    (maps, reducers, totalIndexBytes)
  }

  private def renderReport(
      proofIndexBytes: Long,
      shapes: Seq[(Int, Int, Long)]): String = {
    val rows = shapes.map { case (maps, reducers, bytes) =>
      s"| $maps | $reducers | $bytes | ${if (maps == 0) 0L else bytes / maps} |"
    }.mkString("\n|")
    s"""# Phase 0 persistent reference shuffle read representation
       |
       |## Selected Spark extension points
       |
       |The reference path implements `ShuffleMapOutputWriter` write semantics and resolves
       |non-empty blocks as exact `FileSegmentManagedBuffer` ranges, matching the consolidated
       |data-file/offset-index assumptions of `IndexShuffleBlockResolver`. It intentionally adds
       |no scheduler, `MapOutputTracker`, semantic identity, or manifest integration.
       |
       |## Data and index layout
       |
       |Artifacts live below an encoded recovery-group / generation / incarnation namespace.
       |Each authoritative map winner is one immutable directory containing `data`, `index`, and
       |`READY`, plus a create-new winner claim that fences concurrent promotion. Attempt candidates
       |stay under `.attempts` until the caller binds an exact winner. The local reference
       |filesystem must support same-filesystem atomic directory rename.
       |
       |The index stores reducer count, physical data length, all R+1 offsets, Spark-provided
       |per-reducer checksums, and an index CRC. With checksums its deterministic size is
       |`40 + 16R` bytes per map. This is deliberately M x R information and is separate from the
       |later compact recovery-manifest design.
       |
       |## Empty and fetch-accounting semantics
       |
       |A reducer is empty iff adjacent persisted offsets are equal. Empty blocks return no fetch
       |buffer. Every non-empty block returns a file segment whose length is exactly the physical
       |offset delta. No positive synthetic size or fabricated zero is used.
       |
       |The cross-JVM sparse proof used a 1024-reducer map whose index was $proofIndexBytes bytes.
       |The same fresh-process proof fetched and checksummed a $LargeBytes-byte large block.
       |
       || Maps (M) | Reducers (R) | Total index bytes | Bytes read per map open |
       || ---: | ---: | ---: | ---: |
       |$rows
       |
       |## Process-boundary proof
       |
       |A first JVM wrote and authoritatively selected immutable winners, then exited. A second
       |independent JVM reopened the same root, skipped genuine empty reducers, read the one-byte,
       |sparse, skewed, and 64 MiB blocks from exact physical ranges, verified a zero-total map,
       |opened wide indexes, and finally performed explicit idempotent group cleanup.
       |
       |## Limitations
       |
       |This is a local/reference mechanism, not a production provider SPI. It assumes one local
       |filesystem with atomic directory rename, keeps a dense per-map reducer index, has no remote
       |service durability or authorization model, and deliberately does not perform scheduler
       |adoption, semantic manifest lookup, AQE restoration, healing, or retention management.
       |""".stripMargin
  }

  private def commitMap(
      store: ReferenceShuffleProvider,
      mapIndex: Int,
      mapTaskId: Long,
      reducers: Int,
      blocks: Map[Int, Array[Byte]]): Unit = {
    val writer = store.createMapOutputWriter(mapTaskId, reducers)
    blocks.toSeq.sortBy(_._1).foreach { case (reduceId, bytes) =>
      val out = writer.getPartitionWriter(reduceId).openStream()
      out.write(bytes)
      out.close()
    }
    val checksums = Array.tabulate(reducers) { reduceId =>
      crc32(blocks.getOrElse(reduceId, Array.empty[Byte]))
    }
    store.commitWinner(mapIndex, descriptorOf(writer.commitAllPartitions(checksums)))
  }

  private def descriptorOf(message: MapOutputCommitMessage): ReferenceShuffleOutputDescriptor = {
    require(message.getMapOutputMetadata.isPresent)
    message.getMapOutputMetadata.get().asInstanceOf[ReferenceShuffleOutputDescriptor]
  }

  private def readBlock(map: ReferenceShuffleResolvedMap, reduceId: Int): Array[Byte] = {
    val buffer = map.getBlockData(reduceId).getOrElse {
      throw new IllegalStateException(s"expected non-empty reducer $reduceId")
    }
    val in = buffer.createInputStream()
    try {
      in.readAllBytes()
    } finally {
      in.close()
    }
  }

  private def readBlockDigest(
      map: ReferenceShuffleResolvedMap,
      reduceId: Int): (Long, Long) = {
    val buffer = map.getBlockData(reduceId).getOrElse {
      throw new IllegalStateException(s"expected non-empty reducer $reduceId")
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

  private def provider(root: Path, incarnation: String): ReferenceShuffleProvider = {
    ReferenceShuffleProvider.open(
      root,
      Group,
      Generation,
      incarnation,
      new SparkConf(false))
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

  private def writeSuccessMarker(marker: String): Unit = {
    System.out.write((marker + "\n").getBytes(StandardCharsets.UTF_8))
    System.out.flush()
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
