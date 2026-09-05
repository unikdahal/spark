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
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.CRC32

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage

/**
 * Fresh-process proof utility for the persistent reference shuffle representation and immutable
 * Phase 0 manifest control plane.
 *
 * CI invokes write and read in distinct JVMs against one filesystem root. Only explicit durable
 * configuration and files cross that boundary; no SparkContext, scheduler object, singleton, or
 * in-memory registry can bridge the processes.
 */
private[spark] object ReferenceShuffleProviderProcess {
  private val Group = "cross-jvm-group"
  private val Generation = 1L
  private val ProofIncarnation = "proof"
  private val LargeIncarnation = "proof-large"
  private val ShuffleId = 31
  private val StageId = 7
  private val Reducers = 1024
  private val MapTaskIds = Vector(1001L, 1002L)
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
    Files.createDirectories(root)
    val proof = provider(root, ProofIncarnation)
    val sparse = Map(
      1 -> Array[Byte](1),
      512 -> Array.fill[Byte](1024 * 1024)(2),
      1023 -> "tail".getBytes(StandardCharsets.UTF_8))
    commitMap(proof, 0, MapTaskIds(0), Reducers, sparse)
    commitMap(proof, 1, MapTaskIds(1), Reducers, Map.empty[Int, Array[Byte]])
    require(directoryIsEmpty(proof.attemptsPath), "attempt files remained after winner commit")

    writeLargeBlock(root)
    writeEmptyShape(root, "shape-m0", maps = 0, reducers = 1, taskBase = 1500L)
    writeEmptyShape(root, "shape-r1", maps = 1, reducers = 1, taskBase = 2000L)
    writeEmptyShape(root, "shape-r128", maps = 4, reducers = 128, taskBase = 3000L)
    writeEmptyShape(root, "shape-r4096", maps = 2, reducers = 4096, taskBase = 4000L)

    publishManifest(root)
    exerciseManifestStoreAdversarialCases(root)
    exercisePublisherFailureAndBackpressure()
    exerciseInvalidConfiguration(root)

    writeSuccessMarker("REFERENCE_SHUFFLE_WRITE_OK")
  }

  private def read(root: Path, report: Path): Unit = {
    val config = manifestConfig(root)
    val identity = config.identityFor(MapTaskIds.size)
    val manifest = new ShuffleRecoveryManifestStore(config.manifestRoot)
      .findCompatible(Group, identity, currentGeneration = 2L)
      .getOrElse(throw new IllegalStateException("fresh JVM could not discover published manifest"))
    require(manifest.generation == Generation)
    require(manifest.incarnationId == ProofIncarnation)
    require(manifest.identity.canonicalPayload == identity.canonicalPayload)
    require(manifest.mapArtifacts.map(_.mapTaskId) == MapTaskIds)
    require(manifest.mapArtifacts.map(_.mapIndex) == Vector(0, 1))
    require(manifest.mapArtifacts.forall(_.providerHandle.startsWith("attempt-")))
    require(
      new ShuffleRecoveryManifestStore(config.manifestRoot)
        .findCompatible(Group, identity, currentGeneration = Generation).isEmpty,
      "same-generation manifest must not be considered by a replacement reader")

    val proof = provider(root, ProofIncarnation)
    val sparse = proof.openMap(0)
    require(sparse.numReducers == Reducers)
    require(sparse.blockMetadata(0).isEmpty)
    require(sparse.getBlockData(0).isEmpty)
    require(readBlock(sparse, 1).sameElements(Array[Byte](1)))
    require(readBlock(sparse, 512).length == 1024 * 1024)
    require(new String(readBlock(sparse, 1023), StandardCharsets.UTF_8) == "tail")

    var reduceId = 2
    while (reduceId < Reducers) {
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

    val reportText = renderReport(sparse.indexBytes, shapes, manifest)
    Option(report.getParent).foreach(Files.createDirectories(_))
    Files.write(report, reportText.getBytes(StandardCharsets.UTF_8))

    proof.cleanupGroup()
    proof.cleanupGroup()
    ReferenceShuffleProvider.deleteRecursively(config.manifestRoot)
    Files.deleteIfExists(config.providerRoot)
    require(directoryIsEmpty(root), "explicit Phase 0 cleanup left durable artifacts")
    writeSuccessMarker("REFERENCE_SHUFFLE_READ_OK")
  }

  private def publishManifest(root: Path): Unit = {
    val config = manifestConfig(root)
    val publisher = new ShuffleRecoveryManifestPublisher(
      new FilesystemShuffleRecoveryPublicationBackend(config),
      queueCapacity = 4,
      workerName = "manifest-proof")
    // The fresh-process proof has no driver MapOutputTracker. Listener/tracker agreement is
    // validated separately; this path exercises durable publication after a frozen selection.
    val acceptCurrentSelection: ShuffleRecoveryPublication => Boolean = _ => true
    val coordinator = new ShuffleRecoveryPublicationCoordinator(
      publisher,
      config.reducerCount,
      Some(ShuffleId),
      acceptCurrentSelection)

    coordinator.stageSubmitted(StageId, 0, ShuffleId, MapTaskIds.size)
    coordinator.taskSucceeded(StageId, 0, 0, MapTaskIds(0))
    coordinator.taskSucceeded(StageId, 0, 1, MapTaskIds(1))
    coordinator.stageCompleted(StageId, 0, successful = true)
    // A speculative loser arriving after the stage-completion boundary cannot reopen the attempt.
    coordinator.taskSucceeded(StageId, 0, 0, 9999L)
    require(coordinator.trackedAttemptCount == 0)
    publisher.close()
    require(publisher.publishedCount == 1L)
    require(publisher.failedCount == 0L)
    require(publisher.isTerminated)

    val identity = config.identityFor(MapTaskIds.size)
    val store = new ShuffleRecoveryManifestStore(config.manifestRoot)
    val manifest = store.findCompatible(Group, identity, currentGeneration = 2L).get
    require(manifest.mapArtifacts.map(_.mapTaskId) == MapTaskIds)

    // A failed/incomplete stage must not enqueue another immutable publication.
    val secondPublisher = new ShuffleRecoveryManifestPublisher(
      new FilesystemShuffleRecoveryPublicationBackend(config),
      queueCapacity = 2,
      workerName = "manifest-incomplete-proof")
    val incomplete = new ShuffleRecoveryPublicationCoordinator(
      secondPublisher,
      config.reducerCount,
      Some(ShuffleId),
      acceptCurrentSelection)
    incomplete.stageSubmitted(StageId + 1, 0, ShuffleId, MapTaskIds.size)
    incomplete.taskSucceeded(StageId + 1, 0, 0, MapTaskIds(0))
    incomplete.stageCompleted(StageId + 1, 0, successful = true)
    secondPublisher.close()
    require(secondPublisher.publishedCount == 0L)
    require(secondPublisher.failedCount == 0L)
  }

  private def exerciseManifestStoreAdversarialCases(root: Path): Unit = {
    val config = manifestConfig(root)
    val store = new ShuffleRecoveryManifestStore(config.manifestRoot)

    // Exact duplicate bytes are idempotent; same immutable name with different bytes conflicts.
    val identity = zeroMapIdentity("idempotent")
    val manifest = zeroMapManifest("store-proof", identity, timestamp = 1L)
    require(store.publish(manifest) == ShuffleRecoveryManifestPublished)
    require(store.publish(manifest) == ShuffleRecoveryManifestAlreadyPublished)
    expectFailure[IOException] {
      store.publish(manifest.copy(publicationTimestampMillis = 2L))
    }

    // Two identities are guaranteed to share the two-hex-character lookup namespace after 257
    // distinct payloads (pigeonhole principle). Full canonical payload matching must still select
    // the right candidate.
    val byPrefix = mutable.HashMap.empty[String, ShuffleRecoveryFeasibilityIdentity]
    var collision: Option[
      (ShuffleRecoveryFeasibilityIdentity, ShuffleRecoveryFeasibilityIdentity)] = None
    var i = 0
    while (i <= 256 && collision.isEmpty) {
      val candidate = zeroMapIdentity(s"collision-$i")
      byPrefix.get(candidate.lookupPrefix) match {
        case Some(previous) if previous.canonicalPayload != candidate.canonicalPayload =>
          collision = Some(previous -> candidate)
        case _ => byPrefix(candidate.lookupPrefix) = candidate
      }
      i += 1
    }
    val (firstIdentity, secondIdentity) = collision.getOrElse {
      throw new IllegalStateException("failed to construct deterministic short-prefix collision")
    }
    val first = zeroMapManifest("collision-a", firstIdentity, timestamp = 11L)
    val second = zeroMapManifest("collision-b", secondIdentity, timestamp = 12L)
    store.publish(first)
    store.publish(second)
    require(store.findCompatible("collision-group", firstIdentity, 2L).contains(first))
    require(store.findCompatible("collision-group", secondIdentity, 2L).contains(second))

    // Oversized/malformed records fail before any attacker-controlled declared allocation occurs.
    expectFailure[IOException] {
      ShuffleRecoveryManifestCodec.decode(
        new Array[Byte](ShuffleRecoveryManifestCodec.MaxManifestBytes + 1))
    }
    val truncated = ShuffleRecoveryManifestCodec.encode(first).dropRight(1)
    expectFailure[IOException] {
      ShuffleRecoveryManifestCodec.decode(truncated)
    }
    val trailing = ShuffleRecoveryManifestCodec.encode(first) ++ Array[Byte](1)
    expectFailure[IOException] {
      ShuffleRecoveryManifestCodec.decode(trailing)
    }

    // Concurrent exact publication can create the immutable final name only once and cannot
    // tear it.
    val concurrentIdentity = zeroMapIdentity("concurrent")
    val concurrentManifest = zeroMapManifest("concurrent", concurrentIdentity, timestamp = 21L)
    val start = new CountDownLatch(1)
    val finished = new CountDownLatch(2)
    val failures = new AtomicInteger(0)
    (0 until 2).foreach { _ =>
      val thread = new Thread(() => {
        try {
          start.await()
          store.publish(concurrentManifest)
        } catch {
          case _: Throwable => failures.incrementAndGet()
        } finally {
          finished.countDown()
        }
      })
      thread.setDaemon(true)
      thread.start()
    }
    start.countDown()
    require(finished.await(30, TimeUnit.SECONDS))
    require(failures.get() == 0)
    require(
      store.findCompatible("collision-group", concurrentIdentity, 2L).contains(concurrentManifest))
  }

  private def exercisePublisherFailureAndBackpressure(): Unit = {
    val failing = new ShuffleRecoveryPublicationBackend {
      override def publish(publication: ShuffleRecoveryPublication): Unit =
        throw new IOException("injected publication failure")
    }
    val failedPublisher = new ShuffleRecoveryManifestPublisher(
      failing, queueCapacity = 1, workerName = "manifest-failure-proof")
    require(failedPublisher.submit(testPublication(1L)))
    failedPublisher.close()
    require(failedPublisher.failedCount == 1L)
    require(failedPublisher.publishedCount == 0L)
    require(failedPublisher.isTerminated)

    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val blocking = new ShuffleRecoveryPublicationBackend {
      override def publish(publication: ShuffleRecoveryPublication): Unit = {
        entered.countDown()
        require(release.await(30, TimeUnit.SECONDS))
      }
    }
    val bounded = new ShuffleRecoveryManifestPublisher(
      blocking, queueCapacity = 1, workerName = "manifest-backpressure-proof")
    require(bounded.submit(testPublication(10L)))
    require(entered.await(30, TimeUnit.SECONDS))
    require(bounded.submit(testPublication(11L)))
    require(!bounded.submit(testPublication(12L)))
    require(bounded.rejectedCount == 1L)
    release.countDown()
    bounded.close()
    require(bounded.publishedCount == 2L)
    require(bounded.isTerminated)
  }

  private def exerciseInvalidConfiguration(root: Path): Unit = {
    val disabled = new SparkConf(false)
      .set("spark.shuffle.recovery.phase0.manifest.enabled", "true")
      .set("spark.shuffle.recovery.phase0.provider.root", providerRoot(root).toString)
      .set("spark.shuffle.recovery.phase0.manifest.root", manifestRoot(root).toString)
      .set("spark.shuffle.recovery.phase0.group", Group)
      .set("spark.shuffle.recovery.phase0.generation", "-1")
      .set("spark.shuffle.recovery.phase0.incarnation", ProofIncarnation)
      .set("spark.shuffle.recovery.phase0.identity.sourceToken", "source-v1")
      .set("spark.shuffle.recovery.phase0.identity.producerTag", "hash-aggregate-v1")
      .set("spark.shuffle.recovery.phase0.identity.rowEncoding", "unsafe-row-v1")
      .set("spark.shuffle.recovery.phase0.identity.partitioning", "hash-v1")
      .set("spark.shuffle.recovery.phase0.identity.reducerCount", Reducers.toString)
      .set("spark.shuffle.recovery.phase0.identity.resolvedLiteral", "literal=7")
    require(ShuffleRecoveryManifestPublisher.configFromSparkConf(disabled).isEmpty)

    val oldFetch = validManifestSparkConf(root)
      .set("spark.shuffle.useOldFetchProtocol", "true")
    require(ShuffleRecoveryManifestPublisher.configFromSparkConf(oldFetch).isEmpty)
  }

  private def manifestConfig(root: Path): ShuffleRecoveryPublisherConfig =
    ShuffleRecoveryManifestPublisher.parseConfig(validManifestSparkConf(root))

  private def validManifestSparkConf(root: Path): SparkConf =
    new SparkConf(false)
      .set("spark.shuffle.recovery.phase0.manifest.enabled", "true")
      .set("spark.shuffle.recovery.phase0.provider.root", providerRoot(root).toString)
      .set("spark.shuffle.recovery.phase0.manifest.root", manifestRoot(root).toString)
      .set("spark.shuffle.recovery.phase0.group", Group)
      .set("spark.shuffle.recovery.phase0.generation", Generation.toString)
      .set("spark.shuffle.recovery.phase0.incarnation", ProofIncarnation)
      .set("spark.shuffle.recovery.phase0.identity.sourceToken", "source-v1")
      .set("spark.shuffle.recovery.phase0.identity.producerTag", "hash-aggregate-v1")
      .set("spark.shuffle.recovery.phase0.identity.rowEncoding", "unsafe-row-v1")
      .set("spark.shuffle.recovery.phase0.identity.partitioning", "hash-v1")
      .set("spark.shuffle.recovery.phase0.identity.reducerCount", Reducers.toString)
      .set("spark.shuffle.recovery.phase0.identity.resolvedLiteral", "literal=7")
      .set("spark.shuffle.recovery.phase0.publisher.queueCapacity", "4")
      .set("spark.shuffle.recovery.phase0.targetShuffleId", ShuffleId.toString)

  private def zeroMapIdentity(literal: String): ShuffleRecoveryFeasibilityIdentity =
    ShuffleRecoveryFeasibilityIdentity.create(
      "collision-source",
      "hash-aggregate-v1",
      "unsafe-row-v1",
      "hash-v1",
      mapperCount = 0,
      reducerCount = 1,
      literal)

  private def zeroMapManifest(
      incarnation: String,
      identity: ShuffleRecoveryFeasibilityIdentity,
      timestamp: Long): ShuffleRecoveryManifest =
    ShuffleRecoveryManifest(
      "collision-group",
      Generation,
      incarnation,
      identity,
      mapperCount = 0,
      reducerCount = 1,
      mapArtifacts = Vector.empty,
      ShuffleRecoveryManifest.DescriptorVersion,
      reducerBytes = None,
      publicationTimestampMillis = timestamp)

  private def testPublication(seed: Long): ShuffleRecoveryPublication =
    ShuffleRecoveryPublication(
      ShuffleId,
      StageId + seed.toInt,
      stageAttemptId = 0,
      winningMapTaskIds = Vector(seed),
      reducerCount = 1)

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
      shapes: Seq[(Int, Int, Long)],
      manifest: ShuffleRecoveryManifest): String = {
    val rows = shapes.map { case (maps, reducers, bytes) =>
      s"| $maps | $reducers | $bytes | ${if (maps == 0) 0L else bytes / maps} |"
    }.mkString("\n       |")
    s"""# Phase 0 persistent reference shuffle read representation
       |
       |## Selected Spark extension points
       |
       |The reference path implements `ShuffleMapOutputWriter` write semantics and resolves
       |non-empty blocks as exact `FileSegmentManagedBuffer` ranges. Manifest publication consumes
       |Spark's ordered task/stage listener stream and performs provider/store I/O only on a bounded
       |owned worker. It adds no scheduler adoption or `MapOutputTracker` mutation.
       |
       |## Data and index layout
       |
       |Artifacts live below an encoded recovery-group / generation / incarnation namespace.
       |Each authoritative map winner is one immutable directory containing `data`, `index`, and
       |`READY`, plus a create-new winner claim. The exact map-task winner vector is frozen on
       |successful stage completion before asynchronous manifest publication.
       |
       |The index stores reducer count, physical data length, all R+1 offsets, Spark-provided
       |per-reducer checksums, and an index CRC. With checksums its deterministic size is
       |`40 + 16R` bytes per map. This is deliberately M x R information and is separate from the
       |compact immutable manifest.
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
       |## Immutable manifest proof
       |
       |Process A published manifest `${manifest.incarnationId}` for ${manifest.mapperCount} exact
       |winning map attempts using a versioned canonical feasibility identity. Final manifest files
       |are created with a no-replace hard link only after temp bytes are fsynced. Process B decoded
       |the manifest, revalidated its SHA-256/full canonical identity payload, rejected same/future
       |generation lookup, and fetched the referenced immutable provider winners.
       |
       |## Process-boundary proof
       |
       |A first JVM wrote and selected immutable winners, then published the manifest and exited. A
       |second independent JVM discovered the committed manifest, reopened the same provider root,
       |skipped genuine empty reducers, read one-byte, sparse, skewed and 64 MiB blocks, verified a
       |zero-total map, opened wide indexes, and finally performed explicit idempotent cleanup.
       |
       |## Limitations
       |
       |This is a local/reference mechanism, not a production identity or provider SPI. It assumes
       |one local filesystem, keeps a dense per-map reducer index, uses a deliberately tiny semantic
       |identity, and does not perform claim/adoption, AQE restoration, healing, authorization, or
       |retention management. Stage/shuffle ids are diagnostics/routing only and are never identity
       |material.
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

  private def provider(root: Path, incarnation: String): ReferenceShuffleProvider =
    ReferenceShuffleProvider.open(
      providerRoot(root),
      Group,
      Generation,
      incarnation,
      new SparkConf(false))

  private def providerRoot(root: Path): Path = root.resolve("provider")
  private def manifestRoot(root: Path): Path = root.resolve("manifest")

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
    if (!Files.exists(path)) {
      return true
    }
    val stream = Files.list(path)
    try {
      stream.findAny().isEmpty
    } finally {
      stream.close()
    }
  }

  private def expectFailure[T <: Throwable : Manifest](body: => Unit): Unit = {
    var failed = false
    try {
      body
    } catch {
      case error: Throwable if implicitly[Manifest[T]].runtimeClass.isInstance(error) =>
        failed = true
    }
    require(failed, s"expected ${implicitly[Manifest[T]].runtimeClass.getName}")
  }
}
