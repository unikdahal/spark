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

package org.apache.spark.sql.execution.exchange

import java.io.{BufferedReader, InputStreamReader}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption, StandardWatchEventKinds}
import java.security.MessageDigest
import java.util.Base64
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.apache.spark.{MapOutputTrackerMaster, ShuffleRecoverySchedulerAdoption, SparkEnv}
import org.apache.spark.SparkFunSuite
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted, SparkListenerTaskStart}
import org.apache.spark.shuffle.{ReferenceShuffleOutputDescriptor, ReferenceShuffleProvider}
import org.apache.spark.shuffle.{ReferenceShuffleRecoveryClaimProvider, ShuffleRecoveryClaimRequest}
import org.apache.spark.shuffle.{ShuffleRecoveryClaimUnavailable, ShuffleRecoveryClaimed}
import org.apache.spark.shuffle.{ShuffleRecoveryFeasibilityInputs, ShuffleRecoveryManifest}
import org.apache.spark.shuffle.{ShuffleRecoveryManifestCodec, ShuffleRecoveryManifestPublished}
import org.apache.spark.shuffle.{ShuffleRecoveryManifestStore, ShuffleRecoveryMapArtifact}
import org.apache.spark.shuffle.{ShuffleRecoveryMaterializationId, ShuffleRecoveryAdoptionTarget}
import org.apache.spark.shuffle.{ShuffleRecoveryPreparationRequest, ShuffleRecoveryReadMetrics}
import org.apache.spark.shuffle.{ShuffleRecoveryReservationManager, ShuffleRecoveryUntrustedBoundary}
import org.apache.spark.shuffle.ShuffleRecoveryIndexShuffleBlockResolver
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, pmod, repeat, substring, when}

private object ShuffleRecoveryColdProcessData {
  final case class Scenario(
      name: String,
      mappers: Int,
      reducers: Int,
      rows: Long,
      payloadBytes: Int,
      shape: String)

  val scenarios: Vector[Scenario] = Vector(
    Scenario("sparse", 4, 64, 32L, 0, "sparse"),
    Scenario("empty", 1, 8, 0L, 0, "empty"),
    Scenario("adjacent", 1, 16, 2L, 0, "sparse"),
    Scenario("skewed", 4, 16, 2000L, 64, "skewed"),
    Scenario("small", 1, 4, 1L, 0, "sparse"),
    Scenario("large", 2, 4, 24L, 384 * 1024, "skewed"),
    Scenario("wide", 3, 257, 600L, 16, "sparse"))

  val negativeScenario: Scenario = Scenario("negative", 3, 32, 192L, 16, "sparse")

  val controls: Vector[String] = Vector(
    "disabled",
    "group-diff",
    "generation-not-later",
    "source-token",
    "provider-compat",
    "manifest-absent",
    "digest-collision",
    "artifact-missing",
    "claim-unavailable",
    "reservation-stale")

  def scenario(name: String): Scenario = {
    (scenarios :+ negativeScenario).find(_.name == name).getOrElse {
      throw new IllegalArgumentException(s"unknown cold-process scenario: $name")
    }
  }

  val evidenceHeader: Vector[String] = Vector(
    "role",
    "scenario",
    "control",
    "sparkBaseline",
    "sparkCompatibility",
    "providerCompatibility",
    "group",
    "generation",
    "publishingGeneration",
    "originShuffleId",
    "currentShuffleId",
    "targetStageIds",
    "mapTaskCount",
    "adopted",
    "incarnation",
    "providerBlockReads",
    "providerNonEmptyReads",
    "providerEmptyReads",
    "providerBytesRead",
    "emptyBlocks",
    "nonEmptyBlocks",
    "physicalBytes",
    "maxBlockBytes",
    "rowCount",
    "resultDigest",
    "elapsedMillis",
    "note")

  final case class Evidence(values: Vector[String]) {
    require(values.size == evidenceHeader.size)

    def apply(key: String): String = values(evidenceHeader.indexOf(key))

    def render: String = evidenceHeader.mkString("\t") + "\n" + values.mkString("\t") + "\n"
  }

  object Evidence {
    def read(path: Path): Evidence = {
      val lines = Files.readAllLines(path, StandardCharsets.UTF_8).asScala.toVector
      require(lines.size == 2, s"invalid evidence line count in $path")
      require(lines.head.split("\t", -1).toVector == evidenceHeader)
      Evidence(lines(1).split("\t", -1).toVector)
    }
  }
}

/**
 * Child entry point for the cold-process proof.
 *
 * Every invocation creates a new SparkSession and reconstructs the query from explicit durable
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
  private val PayloadSeed =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

  private final case class ResultSummary(rowCount: Long, digest: String)

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

    def stageIds: String = targetStages.asScala.toVector.map(_.intValue()).sorted.mkString(",")
  }

  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, "cold-process mode is required")
    val mode = args.head
    val options = parseOptions(args.drop(1))
    val root = Paths.get(required(options, "root"))
    val scenarioValue = scenario(required(options, "scenario"))
    val group = required(options, "group")
    val evidence = Paths.get(required(options, "evidence"))
    Files.createDirectories(root)
    Option(evidence.getParent).foreach(Files.createDirectories(_))

    mode match {
      case "baseline" => runBaseline(root, scenarioValue, group, evidence)
      case "producer" => runProducer(root, scenarioValue, group, evidence, hold = false, None)
      case "producer-hold" =>
        val marker = Paths.get(required(options, "marker"))
        runProducer(root, scenarioValue, group, evidence, hold = true, Some(marker))
      case "replacement" =>
        runReplacement(
          root,
          scenarioValue,
          group,
          evidence,
          Paths.get(required(options, "baseline")),
          Paths.get(required(options, "producer")),
          options.getOrElse("control", "none"))
      case other => throw new IllegalArgumentException(s"unknown cold-process mode: $other")
    }
  }

  private def runBaseline(
      root: Path,
      scenario: Scenario,
      group: String,
      evidencePath: Path): Unit = {
    val started = System.nanoTime()
    withSpark(root, s"baseline-${scenario.name}") { spark =>
      val query = buildQuery(spark, scenario)
      val exchange = onlyExchange(query)
      val listener = attachTargetListener(spark, exchange)
      val result = collectResult(query)
      drainListeners(spark)
      val taskCount = listener.taskCount
      if (scenario.rows > 0L) {
        require(taskCount > 0L, "ordinary non-empty baseline did not run shuffle map tasks")
      }
      val identity = feasibility(scenario, sourceToken(scenario)).identityFor(
        target(exchange, materialization = 0L))
      writeEvidence(
        evidencePath,
        Evidence(Vector(
          "baseline",
          scenario.name,
          "none",
          FrozenBaseline,
          identity.sparkCompatibilityId,
          identity.providerCompatibilityId,
          group,
          "0",
          "0",
          exchange.shuffleId.toString,
          exchange.shuffleId.toString,
          listener.stageIds,
          taskCount.toString,
          "false",
          "",
          "0",
          "0",
          "0",
          "0",
          "0",
          "0",
          "0",
          "0",
          result.rowCount.toString,
          result.digest,
          elapsedMillis(started).toString,
          "recovery-disabled semantic reference")))
    }
  }

  private def runProducer(
      root: Path,
      scenario: Scenario,
      group: String,
      evidencePath: Path,
      hold: Boolean,
      marker: Option[Path]): Unit = {
    val started = System.nanoTime()
    withSpark(root, s"producer-${scenario.name}") { spark =>
      val query = buildQuery(spark, scenario)
      val exchange = onlyExchange(query)
      val listener = attachTargetListener(spark, exchange)
      val result = collectResult(query)
      drainListeners(spark)
      require(listener.taskCount > 0L, "producer did not execute the selected shuffle map stage")

      val provider = ReferenceShuffleProvider.open(
        providerRoot(root), group, ProviderGeneration, Incarnation, spark.sparkContext.conf)
      val summary = copyCompletedShuffle(exchange, provider)
      validateScenarioShape(scenario, summary)

      val identity = feasibility(scenario, sourceToken(scenario)).identityFor(
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
      val publishResult = new ShuffleRecoveryManifestStore(manifestRoot(root)).publish(manifest)
      require(publishResult == ShuffleRecoveryManifestPublished)

      writeEvidence(
        evidencePath,
        Evidence(Vector(
          "producer",
          scenario.name,
          "none",
          FrozenBaseline,
          identity.sparkCompatibilityId,
          identity.providerCompatibilityId,
          group,
          ProviderGeneration.toString,
          ProviderGeneration.toString,
          exchange.shuffleId.toString,
          exchange.shuffleId.toString,
          listener.stageIds,
          listener.taskCount.toString,
          "false",
          Incarnation,
          "0",
          "0",
          "0",
          "0",
          summary.emptyBlocks.toString,
          summary.nonEmptyBlocks.toString,
          summary.physicalBytes.toString,
          summary.maxBlockBytes.toString,
          result.rowCount.toString,
          result.digest,
          elapsedMillis(started).toString,
          "immutable provider and manifest committed")))

      marker.foreach { path =>
        Option(path.getParent).foreach(Files.createDirectories(_))
        Files.write(
          path,
          "manifest committed\n".getBytes(StandardCharsets.UTF_8),
          StandardOpenOption.CREATE_NEW,
          StandardOpenOption.WRITE)
      }
      if (hold) {
        new CountDownLatch(1).await()
      }
    }
  }

  private def runReplacement(
      root: Path,
      scenario: Scenario,
      group: String,
      evidencePath: Path,
      baselinePath: Path,
      producerPath: Path,
      control: String): Unit = {
    val started = System.nanoTime()
    val baseline = Evidence.read(baselinePath)
    val producer = Evidence.read(producerPath)
    require(baseline("scenario") == scenario.name)
    require(producer("scenario") == scenario.name)
    require(baseline("resultDigest") == producer("resultDigest"))
    require(baseline("rowCount") == producer("rowCount"))

    withSpark(root, s"replacement-${scenario.name}-$control") { spark =>
      // Consume a distinct shuffle id before constructing the target exchange. The durable
      // identity intentionally excludes attempt-local numeric shuffle ids.
      spark.sparkContext.parallelize(0 until 8, 2)
        .map(value => (value % 3, value))
        .groupByKey(3)
        .count()

      val query = buildQuery(spark, scenario)
      val exchange = onlyExchange(query)
      require(
        exchange.shuffleId != producer("originShuffleId").toInt,
        "replacement target accidentally reused the producer shuffle id")
      val listener = attachTargetListener(spark, exchange)
      val requestedSource = if (control == "source-token") {
        sourceToken(scenario) + "-changed"
      } else {
        sourceToken(scenario)
      }
      val inputs = feasibility(scenario, requestedSource)
      val adoptionOffered = prepareAdoption(
        spark,
        root,
        group,
        scenario,
        exchange,
        inputs,
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
        ShuffleRecoveryReadMetrics(0L, 0L, 0L, 0L)
      }
      val expectedNonEmpty = producer("nonEmptyBlocks").toLong
      val expectedBytes = producer("physicalBytes").toLong

      if (control == "none") {
        require(adoptionOffered, "happy-path recovery was not prepared for the scheduler")
        require(adopted, "prepared recovery was not installed by the scheduler")
        require(listener.taskCount == 0L, "adopted shuffle launched map tasks")
        require(reads.emptyBlockReads == 0L, "fetch requested a known-empty provider block")
        require(reads.nonEmptyBlockReads == expectedNonEmpty)
        require(reads.bytesRead == expectedBytes)
        require(reads.blockReads == expectedNonEmpty)
      } else {
        require(!adopted, s"negative control $control unexpectedly adopted recovery")
        require(listener.taskCount > 0L, s"negative control $control did not recompute")
        require(reads.blockReads == 0L)
      }

      val identity = feasibility(scenario, sourceToken(scenario)).identityFor(
        target(exchange, materialization = 0L))
      writeEvidence(
        evidencePath,
        Evidence(Vector(
          "replacement",
          scenario.name,
          control,
          FrozenBaseline,
          identity.sparkCompatibilityId,
          identity.providerCompatibilityId,
          group,
          ReplacementGeneration.toString,
          (if (adopted) ProviderGeneration else 0L).toString,
          producer("originShuffleId"),
          exchange.shuffleId.toString,
          listener.stageIds,
          listener.taskCount.toString,
          adopted.toString,
          (if (adopted) Incarnation else ""),
          reads.blockReads.toString,
          reads.nonEmptyBlockReads.toString,
          reads.emptyBlockReads.toString,
          reads.bytesRead.toString,
          producer("emptyBlocks"),
          producer("nonEmptyBlocks"),
          producer("physicalBytes"),
          producer("maxBlockBytes"),
          result.rowCount.toString,
          result.digest,
          elapsedMillis(started).toString,
          replacementNote(control, adoptionOffered)))))
    }
  }

  private def prepareAdoption(
      spark: SparkSession,
      root: Path,
      group: String,
      scenario: Scenario,
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
    val reservation = manager.reserve(currentTarget).fold(
      reason => throw new IllegalStateException(reason), identity)
    val scheduler = ShuffleRecoverySchedulerAdoption.currentState.getOrElse {
      throw new IllegalStateException("recovery-aware shuffle resolver is not installed")
    }
    scheduler.registerReservation(
      manager,
      reservation,
      exchange.shuffleDependency,
      exchange.numMappers,
      exchange.numPartitions).fold(
        reason => throw new IllegalStateException(reason),
        _ => ())

    val expectedIdentity = inputs.identityFor(currentTarget)
    val storeRoot = if (control == "manifest-absent") {
      root.resolve("manifest-absent")
    } else {
      manifestRoot(root)
    }
    val found = new ShuffleRecoveryManifestStore(storeRoot)
      .findCompatible(requestGroup, expectedIdentity, currentGeneration)

    if (control == "digest-collision") {
      val manifest = new ShuffleRecoveryManifestStore(manifestRoot(root))
        .findCompatible(group, feasibility(scenario, sourceToken(scenario)).identityFor(currentTarget),
          ReplacementGeneration)
        .getOrElse(throw new IllegalStateException("collision control could not load manifest"))
      requireForcedDigestPayloadMismatchIsRejected(manifest, expectedIdentity)
      return false
    }

    val manifest = found match {
      case Some(value) if control == "provider-compat" =>
        value.copy(identity = value.identity.copy(providerCompatibilityId = "incompatible-v1"))
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

    val claimResult = if (control == "claim-unavailable") {
      ShuffleRecoveryClaimUnavailable
    } else if (control == "artifact-missing") {
      claimWithTemporarilyMissingIndex(provider, claimRequest, root, group, manifest)
    } else {
      provider.claim(claimRequest)
    }

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

  private def claimWithTemporarilyMissingIndex(
      provider: ReferenceShuffleRecoveryClaimProvider,
      request: ShuffleRecoveryClaimRequest,
      root: Path,
      group: String,
      manifest: ShuffleRecoveryManifest) = {
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
      Files.write(index, bytes, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
    }
  }

  private def requireForcedDigestPayloadMismatchIsRejected(
      manifest: ShuffleRecoveryManifest,
      expectedIdentity: org.apache.spark.shuffle.ShuffleRecoveryFeasibilityIdentity): Unit = {
    val wrong = manifest.copy(
      identity = manifest.identity.copy(sourceToken = manifest.identity.sourceToken + "-tampered"))
    val bytes = ShuffleRecoveryManifestCodec.encode(wrong)
    val wrongDigest = wrong.identity.digest.getBytes(StandardCharsets.UTF_8)
    val expectedDigest = expectedIdentity.digest.getBytes(StandardCharsets.UTF_8)
    val position = indexOf(bytes, wrongDigest)
    require(position >= 0, "encoded manifest did not contain its identity digest")
    System.arraycopy(expectedDigest, 0, bytes, position, expectedDigest.length)
    val rejected = try {
      ShuffleRecoveryManifestCodec.decode(bytes)
      false
    } catch {
      case _: java.io.IOException => true
    }
    require(rejected, "full payload mismatch survived a forced serialized digest collision")
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
            val out = partition.openStream()
            val in = buffer.createInputStream()
            try {
              copy(in, out)
            } finally {
              in.close()
              out.close()
            }
            require(partition.getNumBytesWritten() == size)
          }
        } finally {
          buffer.release()
        }
        reduceId += 1
      }
      val commit = writer.commitAllPartitions(Array.emptyLongArray)
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
        if (block.isEmpty) emptyBlocks = Math.addExact(emptyBlocks, 1L)
        else nonEmptyBlocks = Math.addExact(nonEmptyBlocks, 1L)
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

  private def validateScenarioShape(scenario: Scenario, summary: ProviderSummary): Unit = {
    scenario.name match {
      case "empty" =>
        require(summary.nonEmptyBlocks == 0L)
        require(summary.physicalBytes == 0L)
      case "adjacent" =>
        val adjacent = summary.reducerTotals.sliding(2).exists {
          case Vector(left, right) => (left == 0L) != (right == 0L)
          case _ => false
        }
        require(adjacent, "adjacent scenario did not create empty/non-empty neighboring reducers")
      case "sparse" =>
        require(summary.emptyBlocks > summary.nonEmptyBlocks)
      case "skewed" =>
        val nonEmpty = summary.reducerTotals.filter(_ > 0L)
        require(nonEmpty.nonEmpty)
        require(nonEmpty.max > nonEmpty.min * 4L)
      case "small" =>
        require(summary.nonEmptyBlocks > 0L)
      case "large" =>
        require(summary.maxBlockBytes > LargeBlockThreshold)
      case "wide" =>
        require(scenario.reducers >= 257)
        require(summary.emptyBlocks > 0L)
      case "negative" =>
        require(summary.nonEmptyBlocks > 0L)
      case other => throw new IllegalArgumentException(s"unknown shape validation: $other")
    }
  }

  private def buildQuery(spark: SparkSession, scenario: Scenario): DataFrame = {
    val range = spark.range(0L, scenario.rows, 1L, scenario.mappers)
    val key = scenario.shape match {
      case "skewed" =>
        val cutoff = scenario.rows * 19L / 20L
        when(col("id") < lit(cutoff), lit(0L))
          .otherwise(pmod(col("id"), lit(math.max(1, scenario.reducers - 1))) + lit(1L))
      case _ =>
        pmod(col("id") * lit(17L) + lit(3L), lit(scenario.reducers.toLong * 4L))
    }
    val payload = if (scenario.payloadBytes == 0) {
      lit("")
    } else {
      val repetitions = scenario.payloadBytes / PayloadSeed.length + 1
      substring(repeat(lit(PayloadSeed), repetitions), 1, scenario.payloadBytes)
    }
    range
      .select(col("id"), key.cast("long").as("k"), payload.as("payload"))
      .repartition(scenario.reducers, col("k"))
      .select(col("id"), col("k"), col("payload"))
  }

  private def onlyExchange(query: DataFrame): ShuffleExchangeExec = {
    val exchanges = query.queryExecution.executedPlan.collect {
      case exchange: ShuffleExchangeExec => exchange
    }
    require(exchanges.size == 1, s"expected exactly one shuffle exchange, found ${exchanges.size}")
    exchanges.head
  }

  private def attachTargetListener(
      spark: SparkSession,
      exchange: ShuffleExchangeExec): TargetTaskListener = {
    val listener = new TargetTaskListener(exchange.shuffleDependency.rdd.id)
    spark.sparkContext.addSparkListener(listener)
    listener
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
      scenario: Scenario,
      source: String): ShuffleRecoveryFeasibilityInputs = {
    ShuffleRecoveryFeasibilityInputs(
      source,
      "sql-repartition-v1",
      "unsafe-row-v1",
      "hash-v1",
      s"scenario=${scenario.name};rows=${scenario.rows};payload=${scenario.payloadBytes}")
  }

  private def sourceToken(scenario: Scenario): String = s"cold-source-${scenario.name}-v1"

  private def withSpark(root: Path, name: String)(body: SparkSession => Unit): Unit = {
    val local = root.resolve(s"spark-local-$name")
    val warehouse = root.resolve(s"warehouse-$name")
    Files.createDirectories(local)
    Files.createDirectories(warehouse)
    val spark = SparkSession.builder()
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
    try {
      body(spark)
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  private def drainListeners(spark: SparkSession): Unit = {
    spark.sparkContext.listenerBus.waitUntilEmpty(TimeUnit.SECONDS.toMillis(30))
  }

  private def providerRoot(root: Path): Path = root.resolve("provider")

  private def manifestRoot(root: Path): Path = root.resolve("manifests")

  private def copy(in: java.io.InputStream, out: java.io.OutputStream): Unit = {
    val bytes = new Array[Byte](64 * 1024)
    var read = in.read(bytes)
    while (read >= 0) {
      if (read > 0) out.write(bytes, 0, read)
      read = in.read(bytes)
    }
  }

  private def writeEvidence(path: Path, evidence: Evidence): Unit = {
    Files.write(
      path,
      evidence.render.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE_NEW,
      StandardOpenOption.WRITE)
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

  private def elapsedMillis(startedNanos: Long): Long = {
    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos)
  }
}

class ShuffleRecoveryColdProcessSuite extends SparkFunSuite {
  import ShuffleRecoveryColdProcessData._

  private val ChildTimeoutSeconds = 180L
  private val AbruptMarkerTimeoutSeconds = 120L

  test("cold independent JVMs adopt exact provider blocks and negative controls recompute") {
    val evidenceBase = sys.env.get("SPARK_SHUFFLE_RECOVERY_COLD_PROCESS_EVIDENCE_DIR")
      .map(Paths.get(_))
      .getOrElse(Files.createTempDirectory("shuffle-recovery-cold-evidence-"))
    Files.createDirectories(evidenceBase)
    val evidenceRun = Files.createTempDirectory(evidenceBase, "run-")
    val workRoot = Files.createTempDirectory("shuffle-recovery-cold-work-")
    try {
      scenarios.foreach { scenario =>
        runHappy(workRoot.resolve(s"happy-${scenario.name}"), evidenceRun, scenario, "happy")
      }
      runHappy(workRoot.resolve("repeat-sparse"), evidenceRun, scenario("sparse"), "repeat")
      runAbrupt(workRoot.resolve("abrupt-sparse"), evidenceRun, scenario("sparse"))
      runNegativeControls(workRoot.resolve("negative-controls"), evidenceRun)
      writeAggregate(evidenceRun, evidenceBase.resolve("cold-process-evidence.tsv"))
    } finally {
      deleteRecursively(workRoot)
    }
  }

  private def runHappy(
      root: Path,
      evidenceRun: Path,
      scenario: Scenario,
      prefix: String): Unit = {
    Files.createDirectories(root)
    val group = s"cold-$prefix-${scenario.name}"
    val baseline = evidenceRun.resolve(s"$prefix-${scenario.name}-baseline.tsv")
    val producer = evidenceRun.resolve(s"$prefix-${scenario.name}-producer.tsv")
    val replacement = evidenceRun.resolve(s"$prefix-${scenario.name}-replacement.tsv")
    try {
      runChild("baseline", root, scenario, group, baseline)
      runChild("producer", root, scenario, group, producer)
      runChild(
        "replacement",
        root,
        scenario,
        group,
        replacement,
        baseline = Some(baseline),
        producer = Some(producer))
      val replacementEvidence = Evidence.read(replacement)
      assert(replacementEvidence("adopted") == "true")
      assert(replacementEvidence("mapTaskCount") == "0")
      assert(replacementEvidence("providerEmptyReads") == "0")
    } finally {
      deleteRecursively(root)
    }
  }

  private def runAbrupt(root: Path, evidenceRun: Path, scenario: Scenario): Unit = {
    Files.createDirectories(root)
    val group = s"cold-abrupt-${scenario.name}"
    val baseline = evidenceRun.resolve(s"abrupt-${scenario.name}-baseline.tsv")
    val producer = evidenceRun.resolve(s"abrupt-${scenario.name}-producer.tsv")
    val replacement = evidenceRun.resolve(s"abrupt-${scenario.name}-replacement.tsv")
    val marker = root.resolve("manifest-committed.marker")
    try {
      runChild("baseline", root, scenario, group, baseline)
      runAbruptProducer(root, scenario, group, producer, marker)
      assert(Files.isRegularFile(marker))
      runChild(
        "replacement",
        root,
        scenario,
        group,
        replacement,
        baseline = Some(baseline),
        producer = Some(producer))
      assert(Evidence.read(replacement)("adopted") == "true")
    } finally {
      deleteRecursively(root)
    }
  }

  private def runNegativeControls(root: Path, evidenceRun: Path): Unit = {
    Files.createDirectories(root)
    val scenarioValue = negativeScenario
    val group = "cold-negative-controls"
    val baseline = evidenceRun.resolve("negative-baseline.tsv")
    val producer = evidenceRun.resolve("negative-producer.tsv")
    try {
      runChild("baseline", root, scenarioValue, group, baseline)
      runChild("producer", root, scenarioValue, group, producer)
      controls.foreach { control =>
        val evidence = evidenceRun.resolve(s"negative-$control.tsv")
        runChild(
          "replacement",
          root,
          scenarioValue,
          group,
          evidence,
          baseline = Some(baseline),
          producer = Some(producer),
          control = control)
        val result = Evidence.read(evidence)
        assert(result("adopted") == "false", control)
        assert(result("mapTaskCount").toLong > 0L, control)
        assert(result("resultDigest") == Evidence.read(baseline)("resultDigest"), control)
      }
    } finally {
      deleteRecursively(root)
    }
  }

  private def runChild(
      mode: String,
      root: Path,
      scenario: Scenario,
      group: String,
      evidence: Path,
      baseline: Option[Path] = None,
      producer: Option[Path] = None,
      control: String = "none"): Unit = {
    val command = childCommand(mode, root, scenario, group, evidence, baseline, producer, control)
    val log = evidence.resolveSibling(evidence.getFileName.toString + ".log")
    val process = startProcess(command, root)
    val drainer = drainProcess(process, log)
    val finished = process.waitFor(ChildTimeoutSeconds, TimeUnit.SECONDS)
    if (!finished) {
      terminate(process)
    }
    drainer.join(TimeUnit.SECONDS.toMillis(30))
    assert(finished, s"child timed out: ${command.mkString(" ")}; log=$log")
    assert(process.exitValue() == 0, s"child failed with ${process.exitValue()}; log=$log")
    assert(Files.isRegularFile(evidence), s"child produced no evidence: $evidence")
  }

  private def runAbruptProducer(
      root: Path,
      scenario: Scenario,
      group: String,
      evidence: Path,
      marker: Path): Unit = {
    val command = childCommand(
      "producer-hold",
      root,
      scenario,
      group,
      evidence,
      None,
      None,
      "none") :+ s"marker=$marker"
    val log = evidence.resolveSibling(evidence.getFileName.toString + ".log")
    val watcher = marker.getParent.getFileSystem.newWatchService()
    marker.getParent.register(watcher, StandardWatchEventKinds.ENTRY_CREATE)
    val process = startProcess(command, root)
    val drainer = drainProcess(process, log)
    try {
      val committed = waitForMarker(watcher, marker)
      assert(committed, s"producer never reached manifest commit; log=$log")
      process.destroyForcibly()
      assert(process.waitFor(30, TimeUnit.SECONDS), "abrupt producer did not terminate")
      assert(Files.isRegularFile(evidence), "abrupt producer did not flush pre-kill evidence")
    } finally {
      if (process.isAlive) terminate(process)
      watcher.close()
      drainer.join(TimeUnit.SECONDS.toMillis(30))
    }
  }

  private def childCommand(
      mode: String,
      root: Path,
      scenario: Scenario,
      group: String,
      evidence: Path,
      baseline: Option[Path],
      producer: Option[Path],
      control: String): Vector[String] = {
    val java = Paths.get(System.getProperty("java.home"), "bin", "java").toString
    val temporary = root.resolve(s"jvm-tmp-$mode-$control")
    Files.createDirectories(temporary)
    Vector(
      java,
      s"-Djava.io.tmpdir=$temporary",
      "-cp",
      System.getProperty("java.class.path"),
      "org.apache.spark.sql.execution.exchange.ShuffleRecoveryColdProcessProcess",
      mode,
      s"root=$root",
      s"scenario=${scenario.name}",
      s"group=$group",
      s"evidence=$evidence") ++
      baseline.map(path => s"baseline=$path") ++
      producer.map(path => s"producer=$path") ++
      (if (mode == "replacement") Vector(s"control=$control") else Vector.empty)
  }

  private def startProcess(command: Vector[String], root: Path): Process = {
    val builder = new ProcessBuilder(command: _*)
    builder.redirectErrorStream(true)
    builder.environment().put("SPARK_LOCAL_HOSTNAME", "localhost")
    builder.directory(root.toFile)
    builder.start()
  }

  private def drainProcess(process: Process, log: Path): Thread = {
    Option(log.getParent).foreach(Files.createDirectories(_))
    val thread = new Thread(() => {
      val reader = new BufferedReader(new InputStreamReader(
        process.getInputStream,
        StandardCharsets.UTF_8))
      val writer = Files.newBufferedWriter(
        log,
        StandardCharsets.UTF_8,
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE)
      try {
        var line = reader.readLine()
        while (line != null) {
          writer.write(line)
          writer.newLine()
          line = reader.readLine()
        }
      } finally {
        try reader.close() finally writer.close()
      }
    }, s"cold-process-drain-${process.pid()}")
    thread.setDaemon(true)
    thread.start()
    thread
  }

  private def waitForMarker(
      watcher: java.nio.file.WatchService,
      marker: Path): Boolean = {
    if (Files.isRegularFile(marker)) return true
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AbruptMarkerTimeoutSeconds)
    while (System.nanoTime() < deadline) {
      val remaining = deadline - System.nanoTime()
      val key = watcher.poll(math.max(1L, remaining), TimeUnit.NANOSECONDS)
      if (key != null) {
        key.pollEvents()
        key.reset()
        if (Files.isRegularFile(marker)) return true
      }
    }
    false
  }

  private def terminate(process: Process): Unit = {
    process.destroy()
    if (!process.waitFor(10, TimeUnit.SECONDS)) {
      process.destroyForcibly()
      process.waitFor(10, TimeUnit.SECONDS)
    }
  }

  private def writeAggregate(evidenceRun: Path, target: Path): Unit = {
    val stream = Files.list(evidenceRun)
    val evidenceFiles = try {
      stream.iterator().asScala
        .filter(path => path.getFileName.toString.endsWith(".tsv"))
        .toVector
        .sortBy(_.getFileName.toString)
    } finally {
      stream.close()
    }
    assert(evidenceFiles.nonEmpty)
    val rows = evidenceFiles.map(Evidence.read)
    val text = evidenceHeader.mkString("\t") + "\n" +
      rows.map(_.values.mkString("\t")).mkString("\n") + "\n"
    Files.write(
      target,
      text.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.WRITE)
  }

  private def deleteRecursively(path: Path): Unit = {
    if (!Files.exists(path)) return
    val stream = Files.walk(path)
    try {
      stream.iterator().asScala.toVector
        .sortBy(_.getNameCount)
        .reverseIterator
        .foreach(Files.deleteIfExists(_))
    } finally {
      stream.close()
    }
  }
}
