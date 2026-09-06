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

import java.io.{InputStream, IOException, OutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.security.MessageDigest
import java.util.Base64
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._

import org.apache.spark.{MapOutputTrackerMaster, ShuffleRecoverySchedulerAdoption, SparkEnv}
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted, SparkListenerTaskStart}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.functions.{col, lit, pmod, repeat, substring, when}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.storage.ShuffleBlockId

/**
 * Child entry point for the cold-process shuffle recovery proof.
 *
 * Each invocation creates a fresh SparkSession and reconstructs the query from explicit durable
 * arguments. No Spark plan, RDD, scheduler object, static registry, or classloader state crosses
 * child-process boundaries.
 */
object ShuffleRecoveryColdProcessProcess {
  import ShuffleRecoveryColdProcessData._

  private val FrozenBaseline = "2a7cfea06ba135cf0ddc62902eb0daf5a835c672"
  private val ProviderGeneration = 1L
  private val ReplacementGeneration = 2L
  private val Incarnation = "cold-process-incarnation"
  private val LargeBlockThreshold = 2L * 1024L * 1024L
  private val SmallBlockThreshold = 64L * 1024L
  private val PayloadSeed =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  private val EmptyReads = ShuffleRecoveryReadMetrics(0L, 0L, 0L, 0L)

  private final case class ResultSummary(rowCount: Long, digest: String)

  private final case class EvidenceContext(
      role: String,
      scenarioValue: Scenario,
      control: String,
      testedCommit: String,
      identity: ShuffleRecoveryFeasibilityIdentity,
      group: String)

  private final case class EvidenceShuffle(
      generation: Long,
      publishingGeneration: Long,
      originShuffleId: Int,
      currentShuffleId: Int,
      listener: TargetTaskListener,
      adopted: Boolean,
      incarnation: String,
      reads: ShuffleRecoveryReadMetrics)

  private final case class EvidenceMetrics(
      emptyBlocks: Long,
      nonEmptyBlocks: Long,
      physicalBytes: Long,
      maxBlockBytes: Long,
      result: ResultSummary,
      started: Long,
      note: String)

  private final case class ProviderSummary(
      artifacts: Vector[ShuffleRecoveryMapArtifact],
      emptyBlocks: Long,
      nonEmptyBlocks: Long,
      physicalBytes: Long,
      maxBlockBytes: Long,
      reducerTotals: Vector[Long])

  private final class TargetTaskListener(targetRddId: Int) extends SparkListener {
    private val targetStages = ConcurrentHashMap.newKeySet[Integer]()
    private val targetTasks = new AtomicLong(0L)

    override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
      if (event.stageInfo.rddInfos.exists(_.id == targetRddId)) {
        targetStages.add(event.stageInfo.stageId)
      }
    }

    override def onTaskStart(event: SparkListenerTaskStart): Unit = {
      if (targetStages.contains(event.stageId)) {
        targetTasks.incrementAndGet()
      }
    }

    def taskCount: Long = targetTasks.get()

    def stageIds: String = {
      targetStages.asScala.toVector.map(_.intValue()).sorted.mkString(",")
    }
  }

  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, "cold-process mode is required")
    val mode = args.head
    val options = parseOptions(args.drop(1))
    val root = Paths.get(required(options, "root"))
    val scenarioValue = scenario(required(options, "scenario"))
    val group = required(options, "group")
    val testedCommit = required(options, "testedCommit")
    val evidencePath = Paths.get(required(options, "evidence"))
    Files.createDirectories(root)
    createParentDirectories(evidencePath)

    mode match {
      case "baseline" =>
        runBaseline(root, scenarioValue, group, testedCommit, evidencePath)
      case "producer" =>
        runProducer(
          root,
          scenarioValue,
          group,
          testedCommit,
          evidencePath,
          hold = false,
          marker = None)
      case "producer-hold" =>
        runProducer(
          root,
          scenarioValue,
          group,
          testedCommit,
          evidencePath,
          hold = true,
          marker = Some(Paths.get(required(options, "marker"))))
      case "replacement" =>
        runReplacement(
          root,
          scenarioValue,
          group,
          testedCommit,
          evidencePath,
          Paths.get(required(options, "baseline")),
          Paths.get(required(options, "producer")),
          options.getOrElse("control", "none"))
      case other =>
        throw new IllegalArgumentException(s"unknown cold-process mode: $other")
    }
  }

  private def runBaseline(
      root: Path,
      scenarioValue: Scenario,
      group: String,
      testedCommit: String,
      evidencePath: Path): Unit = {
    val started = System.nanoTime()
    val spark = createSpark(root, s"baseline-${scenarioValue.name}")
    try {
      val query = buildQuery(spark, scenarioValue)
      val exchange = onlyExchange(query)
      val listener = attachTargetListener(spark, exchange)
      val result = collectResult(query)
      drainListeners(spark)
      if (scenarioValue.rows > 0L) {
        require(listener.taskCount > 0L, "ordinary non-empty baseline ran no shuffle map tasks")
      }
      val identity = feasibility(scenarioValue, sourceToken(scenarioValue)).identityFor(
        target(exchange, materialization = 0L))
      writeEvidence(
        evidencePath,
        buildEvidence(
          EvidenceContext(
            role = "baseline",
            scenarioValue,
            control = "none",
            testedCommit,
            identity,
            group),
          EvidenceShuffle(
            generation = 0L,
            publishingGeneration = 0L,
            originShuffleId = exchange.shuffleId,
            currentShuffleId = exchange.shuffleId,
            listener,
            adopted = false,
            incarnation = "",
            EmptyReads),
          EvidenceMetrics(
            emptyBlocks = 0L,
            nonEmptyBlocks = 0L,
            physicalBytes = 0L,
            maxBlockBytes = 0L,
            result,
            started,
            note = "recovery-disabled semantic reference")))
    } finally {
      stopSpark(spark)
    }
  }

  private def runProducer(
      root: Path,
      scenarioValue: Scenario,
      group: String,
      testedCommit: String,
      evidencePath: Path,
      hold: Boolean,
      marker: Option[Path]): Unit = {
    val started = System.nanoTime()
    val spark = createSpark(root, s"producer-${scenarioValue.name}")
    try {
      val query = buildQuery(spark, scenarioValue)
      val exchange = onlyExchange(query)
      val listener = attachTargetListener(spark, exchange)

      // Publish while the completed map stage is still registered. A full SQL action is allowed
      // to release its shuffle tracker state once the query has consumed the output.
      spark.sparkContext.submitMapStage(exchange.shuffleDependency).get()
      drainListeners(spark)
      require(listener.taskCount > 0L, "producer did not execute the selected shuffle map stage")

      val provider = ReferenceShuffleProvider.open(
        providerRoot(root),
        group,
        ProviderGeneration,
        Incarnation,
        spark.sparkContext.conf)
      val summary = copyCompletedShuffle(exchange, provider)
      validateScenarioShape(scenarioValue, summary)

      val identity = feasibility(scenarioValue, sourceToken(scenarioValue)).identityFor(
        target(exchange, materialization = 0L))
      val manifest = ShuffleRecoveryManifest(
        group,
        ProviderGeneration,
        Incarnation,
        identity,
        exchange.numMappers,
        exchange.numPartitions,
        summary.artifacts,
        ShuffleRecoveryManifest.DescriptorVersion,
        None,
        publicationTimestampMillis = 1L)
      val store = new ShuffleRecoveryManifestStore(manifestRoot(root))
      require(store.publish(manifest) == ShuffleRecoveryManifestPublished)
      require(
        store.findCompatible(group, identity, ReplacementGeneration).contains(manifest),
        "published manifest was not durably readable")

      val result = collectResult(query)
      drainListeners(spark)
      writeEvidence(
        evidencePath,
        buildEvidence(
          EvidenceContext(
            role = "producer",
            scenarioValue,
            control = "none",
            testedCommit,
            identity,
            group),
          EvidenceShuffle(
            generation = ProviderGeneration,
            publishingGeneration = ProviderGeneration,
            originShuffleId = exchange.shuffleId,
            currentShuffleId = exchange.shuffleId,
            listener,
            adopted = false,
            incarnation = Incarnation,
            EmptyReads),
          EvidenceMetrics(
            summary.emptyBlocks,
            summary.nonEmptyBlocks,
            summary.physicalBytes,
            summary.maxBlockBytes,
            result,
            started,
            note = "immutable provider and manifest committed")))

      marker match {
        case Some(path) =>
          createParentDirectories(path)
          Files.write(
            path,
            "manifest committed\n".getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.WRITE)
        case None =>
      }
      if (hold) {
        new CountDownLatch(1).await()
      }
    } finally {
      stopSpark(spark)
    }
  }

  private def runReplacement(
      root: Path,
      scenarioValue: Scenario,
      group: String,
      testedCommit: String,
      evidencePath: Path,
      baselinePath: Path,
      producerPath: Path,
      control: String): Unit = {
    val started = System.nanoTime()
    val baseline = Evidence.read(baselinePath)
    val producer = Evidence.read(producerPath)
    require(baseline("scenario") == scenarioValue.name)
    require(producer("scenario") == scenarioValue.name)
    require(baseline("resultDigest") == producer("resultDigest"))
    require(baseline("rowCount") == producer("rowCount"))

    val spark = createSpark(root, s"replacement-${scenarioValue.name}-$control")
    try {
      consumeUnrelatedShuffleId(spark)
      val query = buildQuery(spark, scenarioValue)
      val exchange = onlyExchange(query)
      require(
        exchange.shuffleId != producer("originShuffleId").toInt,
        "replacement target accidentally reused the producer shuffle id")
      val listener = attachTargetListener(spark, exchange)
      val requestedSource = if (control == "source-token") {
        sourceToken(scenarioValue) + "-changed"
      } else {
        sourceToken(scenarioValue)
      }
      val adoptionOffered = prepareAdoption(
        spark,
        root,
        group,
        scenarioValue,
        exchange,
        feasibility(scenarioValue, requestedSource),
        control)

      val result = collectResult(query)
      drainListeners(spark)
      require(result.rowCount.toString == baseline("rowCount"))
      require(result.digest == baseline("resultDigest"))

      val adopted = ShuffleRecoverySchedulerAdoption.isAdopted(exchange.shuffleId)
      val resolver = SparkEnv.get.blockingShuffleManager.shuffleBlockResolver
        .asInstanceOf[ShuffleRecoveryIndexShuffleBlockResolver]
      val reads = if (adopted) {
        resolver.recoveredReadMetrics(exchange.shuffleId)
      } else {
        EmptyReads
      }

      if (control == "none") {
        validateHappyReplacement(producer, listener, reads, adoptionOffered, adopted)
      } else {
        validateMissReplacement(control, listener, reads, adopted)
      }

      val identity = feasibility(scenarioValue, sourceToken(scenarioValue)).identityFor(
        target(exchange, materialization = 0L))
      writeEvidence(
        evidencePath,
        buildEvidence(
          EvidenceContext(
            role = "replacement",
            scenarioValue,
            control,
            testedCommit,
            identity,
            group),
          EvidenceShuffle(
            generation = ReplacementGeneration,
            publishingGeneration = if (adopted) ProviderGeneration else 0L,
            originShuffleId = producer("originShuffleId").toInt,
            currentShuffleId = exchange.shuffleId,
            listener,
            adopted,
            incarnation = if (adopted) Incarnation else "",
            reads),
          EvidenceMetrics(
            emptyBlocks = producer("emptyBlocks").toLong,
            nonEmptyBlocks = producer("nonEmptyBlocks").toLong,
            physicalBytes = producer("physicalBytes").toLong,
            maxBlockBytes = producer("maxBlockBytes").toLong,
            result,
            started,
            note = replacementNote(control, adoptionOffered))))
    } finally {
      stopSpark(spark)
    }
  }

  private def validateHappyReplacement(
      producer: Evidence,
      listener: TargetTaskListener,
      reads: ShuffleRecoveryReadMetrics,
      adoptionOffered: Boolean,
      adopted: Boolean): Unit = {
    require(adoptionOffered, "happy-path recovery was not prepared for the scheduler")
    require(adopted, "prepared recovery was not installed by the scheduler")
    require(listener.taskCount == 0L, "adopted shuffle launched map tasks")
    require(reads.emptyBlockReads == 0L, "fetch requested a known-empty provider block")
    require(reads.nonEmptyBlockReads == producer("nonEmptyBlocks").toLong)
    require(reads.bytesRead == producer("physicalBytes").toLong)
    require(reads.blockReads == reads.nonEmptyBlockReads)
  }

  private def validateMissReplacement(
      control: String,
      listener: TargetTaskListener,
      reads: ShuffleRecoveryReadMetrics,
      adopted: Boolean): Unit = {
    require(!adopted, s"negative control $control unexpectedly adopted recovery")
    require(listener.taskCount > 0L, s"negative control $control did not recompute")
    require(reads.blockReads == 0L)
  }

  private def prepareAdoption(
      spark: SparkSession,
      root: Path,
      group: String,
      scenarioValue: Scenario,
      exchange: ShuffleExchangeExec,
      inputs: ShuffleRecoveryFeasibilityInputs,
      control: String): Boolean = {
    if (control == "disabled") {
      return false
    }

    val currentTarget = target(exchange, materialization = 1L)
    val requestGroup = if (control == "group-diff") group + "-other" else group
    val currentGeneration = if (control == "generation-not-later") {
      ProviderGeneration
    } else {
      ReplacementGeneration
    }
    val request = ShuffleRecoveryPreparationRequest(
      requestGroup,
      currentGeneration,
      currentTarget,
      inputs)
    val manager = new ShuffleRecoveryReservationManager
    val reservation = manager.reserve(currentTarget) match {
      case Right(value) => value
      case Left(reason) => throw new IllegalStateException(reason)
    }
    val scheduler = ShuffleRecoverySchedulerAdoption.currentState.getOrElse {
      throw new IllegalStateException("recovery-aware shuffle resolver is not installed")
    }
    scheduler.registerReservation(
      manager,
      reservation,
      exchange.shuffleDependency,
      exchange.numMappers,
      exchange.numPartitions) match {
      case Right(_) =>
      case Left(reason) => throw new IllegalStateException(reason)
    }

    val expectedIdentity = inputs.identityFor(currentTarget)
    val storeRoot = if (control == "manifest-absent") {
      root.resolve("manifest-absent")
    } else {
      manifestRoot(root)
    }
    val found = new ShuffleRecoveryManifestStore(storeRoot)
      .findCompatible(requestGroup, expectedIdentity, currentGeneration)

    if (control == "digest-collision") {
      val normalIdentity = feasibility(scenarioValue, sourceToken(scenarioValue))
        .identityFor(currentTarget)
      val manifest = new ShuffleRecoveryManifestStore(manifestRoot(root))
        .findCompatible(group, normalIdentity, ReplacementGeneration)
        .getOrElse {
          throw new IllegalStateException("collision control could not load manifest")
        }
      requireForcedDigestPayloadMismatchIsRejected(manifest)
      return false
    }

    val manifest = found match {
      case Some(value) if control == "provider-compat" =>
        value.copy(
          identity = value.identity.copy(providerCompatibilityId = "incompatible-v1"))
      case Some(value) => value
      case None => return false
    }
    val boundary = new ShuffleRecoveryUntrustedBoundary
    val candidate = boundary.validateCandidate(request, expectedIdentity, manifest) match {
      case Right(value) => value
      case Left(_) => return false
    }

    val provider = new ReferenceShuffleRecoveryClaimProvider(
      providerRoot(root), spark.sparkContext.conf)
    val claimRequest = ShuffleRecoveryClaimRequest(
      candidate.recoveryGroup,
      candidate.publishingGeneration,
      candidate.incarnationId,
      candidate.identity.providerCompatibilityId,
      currentTarget.targetShuffleId,
      candidate.mapperCount,
      candidate.reducerCount,
      candidate.mapArtifacts)
    val claimResult = claimForControl(
      provider, claimRequest, root, group, manifest, control)

    claimResult match {
      case claimed: ShuffleRecoveryClaimed =>
        boundary.validateClaim(request, reservation, candidate, claimed) match {
          case Right(prepared) =>
            if (control == "reservation-stale") {
              manager.invalidate(currentTarget.materializationId)
            }
            scheduler.offerPrepared(
              prepared,
              provider,
              SparkEnv.get.blockManager.blockManagerId).isRight
          case Left(_) =>
            provider.release(claimed.binding)
            false
        }
      case _ => false
    }
  }

  private def claimForControl(
      provider: ReferenceShuffleRecoveryClaimProvider,
      request: ShuffleRecoveryClaimRequest,
      root: Path,
      group: String,
      manifest: ShuffleRecoveryManifest,
      control: String): ShuffleRecoveryClaimResult = {
    if (control == "claim-unavailable") {
      ShuffleRecoveryClaimUnavailable
    } else if (control == "artifact-missing") {
      claimWithTemporarilyMissingIndex(provider, request, root, group, manifest)
    } else {
      provider.claim(request)
    }
  }

  private def claimWithTemporarilyMissingIndex(
      provider: ReferenceShuffleRecoveryClaimProvider,
      request: ShuffleRecoveryClaimRequest,
      root: Path,
      group: String,
      manifest: ShuffleRecoveryManifest): ShuffleRecoveryClaimResult = {
    val encodedGroup = Base64.getUrlEncoder.withoutPadding().encodeToString(
      group.getBytes(StandardCharsets.UTF_8))
    val encodedIncarnation = Base64.getUrlEncoder.withoutPadding().encodeToString(
      manifest.incarnationId.getBytes(StandardCharsets.UTF_8))
    val index = providerRoot(root)
      .resolve(encodedGroup)
      .resolve(manifest.generation.toString)
      .resolve(encodedIncarnation)
      .resolve("maps")
      .resolve("map-0")
      .resolve("index")
    val bytes = Files.readAllBytes(index)
    Files.delete(index)
    try {
      provider.claim(request)
    } finally {
      Files.write(
        index,
        bytes,
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE)
    }
  }

  private def requireForcedDigestPayloadMismatchIsRejected(
      manifest: ShuffleRecoveryManifest): Unit = {
    val expectedBytes = ShuffleRecoveryManifestCodec.encode(manifest)
    val wrong = manifest.copy(
      identity = manifest.identity.copy(sourceToken = manifest.identity.sourceToken + "-tampered"))
    val wrongBytes = ShuffleRecoveryManifestCodec.encode(wrong)
    val expectedDigest = firstHexDigest(expectedBytes)
    val wrongDigest = firstHexDigest(wrongBytes)
    val position = indexOf(wrongBytes, wrongDigest)
    require(position >= 0, "encoded manifest did not contain its identity digest")
    System.arraycopy(expectedDigest, 0, wrongBytes, position, expectedDigest.length)
    val rejected = try {
      ShuffleRecoveryManifestCodec.decode(wrongBytes)
      false
    } catch {
      case _: IOException => true
    }
    require(rejected, "full payload mismatch survived a forced serialized digest collision")
  }

  private def firstHexDigest(bytes: Array[Byte]): Array[Byte] = {
    var offset = 0
    while (offset <= bytes.length - 64) {
      var index = 0
      while (index < 64 && isLowerHex(bytes(offset + index))) {
        index += 1
      }
      if (index == 64) {
        return bytes.slice(offset, offset + 64)
      }
      offset += 1
    }
    throw new IllegalStateException("encoded manifest contains no SHA-256 digest")
  }

  private def isLowerHex(value: Byte): Boolean = {
    (value >= '0'.toByte && value <= '9'.toByte) ||
      (value >= 'a'.toByte && value <= 'f'.toByte)
  }

  private def indexOf(bytes: Array[Byte], target: Array[Byte]): Int = {
    var offset = 0
    while (offset <= bytes.length - target.length) {
      var index = 0
      while (index < target.length && bytes(offset + index) == target(index)) {
        index += 1
      }
      if (index == target.length) {
        return offset
      }
      offset += 1
    }
    -1
  }

  private def copyCompletedShuffle(
      exchange: ShuffleExchangeExec,
      provider: ReferenceShuffleProvider): ProviderSummary = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val status = tracker.shuffleStatuses.getOrElse(
      exchange.shuffleId,
      throw new IllegalStateException("producer shuffle is absent from MapOutputTracker"))
    val mapStatuses = status.mapStatuses.clone()
    require(mapStatuses.length == exchange.numMappers)
    require(mapStatuses.forall(_ != null), "producer shuffle is not completely materialized")
    val resolver = SparkEnv.get.blockingShuffleManager.shuffleBlockResolver
    val artifacts = Vector.newBuilder[ShuffleRecoveryMapArtifact]
    val reducerTotals = Array.fill[Long](exchange.numPartitions)(0L)
    var emptyBlocks = 0L
    var nonEmptyBlocks = 0L
    var physicalBytes = 0L
    var maxBlockBytes = 0L

    var mapIndex = 0
    while (mapIndex < mapStatuses.length) {
      val mapStatus = mapStatuses(mapIndex)
      val writer = provider.createMapOutputWriter(mapStatus.mapId, exchange.numPartitions)
      var reduceId = 0
      while (reduceId < exchange.numPartitions) {
        val buffer = resolver.getBlockData(
          ShuffleBlockId(exchange.shuffleId, mapStatus.mapId, reduceId),
          None)
        val size = buffer.size()
        try {
          if (size > 0L) {
            val partition = writer.getPartitionWriter(reduceId)
            val output = partition.openStream()
            val input = buffer.createInputStream()
            try {
              copy(input, output)
            } finally {
              input.close()
              output.close()
            }
            require(partition.getNumBytesWritten() == size)
          }
        } finally {
          buffer.release()
        }
        reduceId += 1
      }
      val commit = writer.commitAllPartitions(Array.empty[Long])
      require(commit.getMapOutputMetadata.isPresent)
      val descriptor = commit.getMapOutputMetadata.get()
        .asInstanceOf[ReferenceShuffleOutputDescriptor]
      provider.commitWinner(mapIndex, descriptor)
      artifacts += ShuffleRecoveryMapArtifact(
        mapIndex,
        mapStatus.mapId,
        descriptor.candidateName,
        descriptor.dataLength,
        descriptor.indexLength)

      val resolved = provider.openMap(mapIndex)
      reduceId = 0
      while (reduceId < exchange.numPartitions) {
        val block = resolved.blockMetadata(reduceId)
        if (block.isEmpty) {
          emptyBlocks = Math.addExact(emptyBlocks, 1L)
        } else {
          nonEmptyBlocks = Math.addExact(nonEmptyBlocks, 1L)
        }
        physicalBytes = Math.addExact(physicalBytes, block.length)
        reducerTotals(reduceId) = Math.addExact(reducerTotals(reduceId), block.length)
        maxBlockBytes = Math.max(maxBlockBytes, block.length)
        reduceId += 1
      }
      require(resolved.dataLength == descriptor.dataLength)
      mapIndex += 1
    }

    ProviderSummary(
      artifacts.result(),
      emptyBlocks,
      nonEmptyBlocks,
      physicalBytes,
      maxBlockBytes,
      reducerTotals.toVector)
  }

  private def validateScenarioShape(
      scenarioValue: Scenario,
      summary: ProviderSummary): Unit = {
    scenarioValue.name match {
      case "empty" =>
        require(summary.nonEmptyBlocks == 0L)
        require(summary.physicalBytes == 0L)
      case "adjacent" =>
        var adjacent = false
        var index = 1
        while (index < summary.reducerTotals.size && !adjacent) {
          val left = summary.reducerTotals(index - 1)
          val right = summary.reducerTotals(index)
          adjacent = (left == 0L) != (right == 0L)
          index += 1
        }
        require(adjacent, "adjacent scenario did not create neighboring empty/non-empty reducers")
      case "sparse" =>
        require(summary.emptyBlocks > summary.nonEmptyBlocks)
      case "skewed" =>
        val nonEmpty = summary.reducerTotals.filter(_ > 0L)
        require(nonEmpty.nonEmpty)
        require(nonEmpty.max > nonEmpty.min * 4L)
      case "small" =>
        require(summary.nonEmptyBlocks > 0L)
        require(summary.maxBlockBytes > 0L)
        require(summary.maxBlockBytes <= SmallBlockThreshold)
      case "large" =>
        require(summary.maxBlockBytes > LargeBlockThreshold)
      case "wide" =>
        require(scenarioValue.reducers >= 257)
        require(summary.emptyBlocks > 0L)
      case "negative" =>
        require(summary.nonEmptyBlocks > 0L)
      case other =>
        throw new IllegalArgumentException(s"unknown shape validation: $other")
    }
  }

  private def buildQuery(spark: SparkSession, scenarioValue: Scenario): DataFrame = {
    val range = if (scenarioValue.rows == 0L) {
      val schema = StructType(Seq(StructField("id", LongType, nullable = false)))
      val emptyRows = spark.sparkContext.parallelize(Seq.empty[Row], scenarioValue.mappers)
      spark.createDataFrame(emptyRows, schema)
    } else {
      spark.range(0L, scenarioValue.rows, 1L, scenarioValue.mappers)
    }
    val key = scenarioValue.shape match {
      case "skewed" =>
        val cutoff = scenarioValue.rows * 19L / 20L
        when(col("id") < lit(cutoff), lit(0L))
          .otherwise(
            pmod(col("id"), lit(math.max(1, scenarioValue.reducers - 1))) + lit(1L))
      case _ =>
        pmod(
          col("id") * lit(17L) + lit(3L),
          lit(scenarioValue.reducers.toLong * 4L))
    }
    val payload = if (scenarioValue.payloadBytes == 0) {
      lit("")
    } else {
      val repetitions = scenarioValue.payloadBytes / PayloadSeed.length + 1
      substring(repeat(lit(PayloadSeed), repetitions), 1, scenarioValue.payloadBytes)
    }
    range
      .select(col("id"), key.cast("long").as("k"), payload.as("payload"))
      .repartition(scenarioValue.reducers, col("k"))
      .select(col("id"), col("k"), col("payload"))
  }

  private def onlyExchange(query: DataFrame): ShuffleExchangeExec = {
    val exchanges = query.queryExecution.executedPlan.collect {
      case exchange: ShuffleExchangeExec => exchange
    }
    require(
      exchanges.size == 1,
      s"expected exactly one shuffle exchange, found ${exchanges.size}")
    exchanges.head
  }

  private def attachTargetListener(
      spark: SparkSession,
      exchange: ShuffleExchangeExec): TargetTaskListener = {
    val listener = new TargetTaskListener(exchange.shuffleDependency.rdd.id)
    spark.sparkContext.addSparkListener(listener)
    listener
  }

  private def consumeUnrelatedShuffleId(spark: SparkSession): Unit = {
    spark.sparkContext.newShuffleId()
  }

  private def collectResult(query: DataFrame): ResultSummary = {
    val rows = query.collect()
    ResultSummary(rows.length.toLong, digestRows(rows))
  }

  private def digestRows(rows: Array[Row]): String = {
    val canonical = rows.iterator.map { row =>
      val payload = row.getString(2)
      s"${row.getLong(0)}:${row.getLong(1)}:${payload.length}:$payload"
    }.toArray.sorted
    val digest = MessageDigest.getInstance("SHA-256")
    canonical.foreach { value =>
      val bytes = value.getBytes(StandardCharsets.UTF_8)
      digest.update(ByteBuffer.allocate(4).putInt(bytes.length).array())
      digest.update(bytes)
    }
    digest.digest().iterator.map(byte => f"${byte & 0xff}%02x").mkString
  }

  private def target(
      exchange: ShuffleExchangeExec,
      materialization: Long): ShuffleRecoveryAdoptionTarget = {
    ShuffleRecoveryAdoptionTarget(
      ShuffleRecoveryMaterializationId(exchange.id, materialization),
      exchange.shuffleId,
      exchange.shuffleDependency.rdd.id.toLong,
      exchange.numMappers,
      exchange.numPartitions)
  }

  private def feasibility(
      scenarioValue: Scenario,
      source: String): ShuffleRecoveryFeasibilityInputs = {
    ShuffleRecoveryFeasibilityInputs(
      source,
      "sql-repartition-v1",
      "unsafe-row-v1",
      "hash-v1",
      s"scenario=${scenarioValue.name};rows=${scenarioValue.rows};" +
        s"payload=${scenarioValue.payloadBytes}")
  }

  private def sourceToken(scenarioValue: Scenario): String = {
    s"cold-source-${scenarioValue.name}-v1"
  }

  private def createSpark(root: Path, name: String): SparkSession = {
    val local = root.resolve(s"spark-local-$name")
    val warehouse = root.resolve(s"warehouse-$name")
    Files.createDirectories(local)
    Files.createDirectories(warehouse)
    SparkSession.builder()
      .master("local[2]")
      .appName(s"shuffle-recovery-cold-$name")
      .config("spark.ui.enabled", "false")
      .config("spark.ui.showConsoleProgress", "false")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.shuffle.compress", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.port", "0")
      .config("spark.blockManager.port", "0")
      .config("spark.local.dir", local.toString)
      .config("spark.sql.warehouse.dir", warehouse.toUri.toString)
      .getOrCreate()
  }

  private def stopSpark(spark: SparkSession): Unit = {
    try {
      spark.stop()
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  private def drainListeners(spark: SparkSession): Unit = {
    spark.sparkContext.listenerBus.waitUntilEmpty(TimeUnit.SECONDS.toMillis(30))
  }

  private def providerRoot(root: Path): Path = root.resolve("provider")

  private def manifestRoot(root: Path): Path = root.resolve("manifests")

  private def copy(input: InputStream, output: OutputStream): Unit = {
    val bytes = new Array[Byte](64 * 1024)
    var read = input.read(bytes)
    while (read >= 0) {
      if (read > 0) {
        output.write(bytes, 0, read)
      }
      read = input.read(bytes)
    }
  }

  private def writeEvidence(path: Path, evidence: Evidence): Unit = {
    Files.write(
      path,
      evidence.render.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE_NEW,
      StandardOpenOption.WRITE)
  }

  private def buildEvidence(
      context: EvidenceContext,
      shuffle: EvidenceShuffle,
      metrics: EvidenceMetrics): Evidence = {
    Evidence(Vector(
      context.role,
      context.scenarioValue.name,
      context.control,
      FrozenBaseline,
      context.testedCommit,
      context.identity.sparkCompatibilityId,
      context.identity.providerCompatibilityId,
      "false",
      context.group,
      shuffle.generation.toString,
      shuffle.publishingGeneration.toString,
      shuffle.originShuffleId.toString,
      shuffle.currentShuffleId.toString,
      shuffle.listener.stageIds,
      shuffle.listener.taskCount.toString,
      shuffle.adopted.toString,
      shuffle.incarnation,
      shuffle.reads.blockReads.toString,
      shuffle.reads.nonEmptyBlockReads.toString,
      shuffle.reads.emptyBlockReads.toString,
      shuffle.reads.bytesRead.toString,
      metrics.emptyBlocks.toString,
      metrics.nonEmptyBlocks.toString,
      metrics.physicalBytes.toString,
      metrics.maxBlockBytes.toString,
      metrics.result.rowCount.toString,
      metrics.result.digest,
      elapsedMillis(metrics.started).toString,
      metrics.note))
  }

  private def replacementNote(control: String, offered: Boolean): String = {
    if (control == "none") {
      "prepared adoption installed; timings are mechanism diagnostics only"
    } else {
      s"cache miss control=$control offered=$offered; ordinary execution preserved"
    }
  }

  private def parseOptions(args: Array[String]): Map[String, String] = {
    args.iterator.map { arg =>
      val separator = arg.indexOf('=')
      require(separator > 0, s"invalid option: $arg")
      arg.substring(0, separator) -> arg.substring(separator + 1)
    }.toMap
  }

  private def required(options: Map[String, String], key: String): String = {
    options.getOrElse(key, throw new IllegalArgumentException(s"missing option: $key"))
  }

  private def createParentDirectories(path: Path): Unit = {
    val parent = path.getParent
    if (parent != null) {
      Files.createDirectories(parent)
    }
  }

  private def elapsedMillis(startedNanos: Long): Long = {
    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos)
  }
}
