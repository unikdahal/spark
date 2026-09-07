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

import java.io.{BufferedReader, InputStream, InputStreamReader, OutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.security.MessageDigest
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._

import org.apache.spark.{MapOutputTrackerMaster, ShuffleRecoverySchedulerAdoption, SparkEnv}
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted, SparkListenerTaskStart}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.functions.{col, lit, pmod}
import org.apache.spark.storage.ShuffleBlockId

/**
 * Independent-JVM proof for the adopted failure -> all-fresh -> successor-adoption lifecycle.
 *
 * The parent runs three separate JVMs over one durable provider/manifest root:
 *
 *   1. generation 1 publishes A;
 *   2. generation 2 claims A, removes one already-bound provider data file before fetch, proves
 *      whole-shuffle fallback selected every mapper, waits for exact A retirement, then publishes B;
 *   3. generation 3 reconstructs the query in a fresh JVM and adopts B with zero map tasks.
 *
 * The destructive mutation is deliberately after claim/validation and before the first fetch, so
 * this is provider-read failure rather than an adoption cache miss.
 */
private[shuffle] object ShuffleRecoveryAdoptedFailureColdProcessProof {
  private val ChildTimeoutSeconds = 180L

  def run(testedCommit: String, evidenceBase: Path): Unit = {
    Files.createDirectories(evidenceBase)
    val evidenceRun = Files.createTempDirectory(evidenceBase, "adopted-failure-run-")
    val workRoot = Files.createTempDirectory("shuffle-recovery-adopted-failure-work-")
    val group = "cold-adopted-failure-healing"
    val producer = evidenceRun.resolve("producer.tsv")
    val failure = evidenceRun.resolve("failure.tsv")
    val healed = evidenceRun.resolve("healed.tsv")
    try {
      runChild("producer", workRoot, group, testedCommit, producer)
      runChild("failure", workRoot, group, testedCommit, failure, Some(producer))
      runChild("healed", workRoot, group, testedCommit, healed, Some(failure))

      val producerEvidence = readEvidence(producer)
      val failureEvidence = readEvidence(failure)
      val healedEvidence = readEvidence(healed)
      require(failureEvidence("resultDigest") == producerEvidence("resultDigest"))
      require(healedEvidence("resultDigest") == producerEvidence("resultDigest"))
      require(failureEvidence("adopted") == "false")
      require(failureEvidence("retiredA") == "true")
      require(failureEvidence("publishedGeneration") == "2")
      require(failureEvidence("mapTaskPartitions") == "0,1,2,3")
      require(healedEvidence("adopted") == "true")
      require(healedEvidence("mapTaskCount") == "0")
      require(healedEvidence("publishingGeneration") == "2")
    } finally {
      deleteRecursively(workRoot)
    }
  }

  private def runChild(
      mode: String,
      root: Path,
      group: String,
      testedCommit: String,
      evidence: Path,
      previous: Option[Path] = None): Unit = {
    val java = Paths.get(System.getProperty("java.home"), "bin", "java").toString
    val temporary = root.resolve(s"jvm-tmp-$mode")
    Files.createDirectories(temporary)
    val command = Vector(
      java,
      s"-Djava.io.tmpdir=$temporary",
      "-Dspark.shuffle.useOldFetchProtocol=true",
      "-cp",
      System.getProperty("java.class.path"),
      "org.apache.spark.shuffle.ShuffleRecoveryAdoptedFailureColdProcess",
      mode,
      s"root=$root",
      s"group=$group",
      s"testedCommit=$testedCommit",
      s"evidence=$evidence") ++ previous.map(path => s"previous=$path")
    val log = evidence.resolveSibling(evidence.getFileName.toString + ".log")
    val builder = new ProcessBuilder(command: _*)
    builder.redirectErrorStream(true)
    builder.environment().put("SPARK_LOCAL_HOSTNAME", "localhost")
    builder.directory(root.toFile)
    val process = builder.start()
    val drainer = drainProcess(process, log)
    val finished = process.waitFor(ChildTimeoutSeconds, TimeUnit.SECONDS)
    if (!finished) {
      process.destroyForcibly()
      process.waitFor(30, TimeUnit.SECONDS)
    }
    drainer.join(TimeUnit.SECONDS.toMillis(30))
    require(finished, s"$mode child timed out; log=$log")
    require(process.exitValue() == 0, s"$mode child failed with ${process.exitValue()}; log=$log")
    require(Files.isRegularFile(evidence), s"$mode child produced no evidence: $evidence")
  }

  private def drainProcess(process: Process, log: Path): Thread = {
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
    }, s"adopted-failure-drain-${process.pid()}")
    thread.setDaemon(true)
    thread.start()
    thread
  }

  private def readEvidence(path: Path): Map[String, String] = {
    Files.readAllLines(path, StandardCharsets.UTF_8).asScala.iterator.map { line =>
      val separator = line.indexOf('\t')
      require(separator > 0, s"invalid evidence line in $path: $line")
      line.substring(0, separator) -> line.substring(separator + 1)
    }.toMap
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

/** Child entry point used only by [[ShuffleRecoveryAdoptedFailureColdProcessProof]]. */
object ShuffleRecoveryAdoptedFailureColdProcess {
  private val ProviderA = 1L
  private val ProviderB = 2L
  private val Attempt2 = 2L
  private val Attempt3 = 3L
  private val IncarnationA = "adopted-failure-a"
  private val IncarnationB = "adopted-failure-b"
  private val Mappers = 4
  private val Reducers = 16
  private val Rows = 512L

  private final case class ResultSummary(rowCount: Long, digest: String)

  private final class TargetTaskListener(targetRddId: Int) extends SparkListener {
    private val targetStages = ConcurrentHashMap.newKeySet[Integer]()
    private val targetTasks = new AtomicLong(0L)
    private val targetPartitions = ConcurrentHashMap.newKeySet[Integer]()

    override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
      if (event.stageInfo.rddInfos.exists(_.id == targetRddId)) {
        targetStages.add(event.stageInfo.stageId)
      }
    }

    override def onTaskStart(event: SparkListenerTaskStart): Unit = {
      if (targetStages.contains(event.stageId)) {
        targetTasks.incrementAndGet()
        targetPartitions.add(event.taskInfo.partitionId)
      }
    }

    def taskCount: Long = targetTasks.get()

    def partitions: Vector[Int] = targetPartitions.asScala.iterator.map(_.intValue()).toVector.sorted
  }

  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, "mode is required")
    val mode = args.head
    val options = args.drop(1).iterator.map { arg =>
      val separator = arg.indexOf('=')
      require(separator > 0, s"invalid option: $arg")
      arg.substring(0, separator) -> arg.substring(separator + 1)
    }.toMap
    val root = Paths.get(required(options, "root"))
    val group = required(options, "group")
    val testedCommit = required(options, "testedCommit")
    val evidence = Paths.get(required(options, "evidence"))
    Files.createDirectories(root)
    Option(evidence.getParent).foreach(Files.createDirectories(_))

    mode match {
      case "producer" => runProducer(root, group, testedCommit, evidence)
      case "failure" =>
        runFailure(root, group, testedCommit, Paths.get(required(options, "previous")), evidence)
      case "healed" =>
        runHealed(root, group, testedCommit, Paths.get(required(options, "previous")), evidence)
      case other => throw new IllegalArgumentException(s"unknown mode: $other")
    }
  }

  private def runProducer(
      root: Path,
      group: String,
      testedCommit: String,
      evidence: Path): Unit = {
    val spark = createSpark(root, "producer")
    try {
      val query = buildQuery(spark)
      val exchange = onlyExchange(query)
      require(exchange.numMappers == Mappers)
      val listener = attachTargetListener(spark, exchange)
      spark.sparkContext.submitMapStage(exchange.shuffleDependency).get()
      drainListeners(spark)
      require(listener.partitions == (0 until Mappers).toVector)

      val provider = ReferenceShuffleProvider.open(
        providerRoot(root), group, ProviderA, IncarnationA, spark.sparkContext.conf)
      val artifacts = copyCompletedShuffle(exchange, provider)
      val identity = identityFor(exchange)
      publishManifest(root, group, ProviderA, IncarnationA, identity, exchange, artifacts)
      val result = collectResult(query)
      drainListeners(spark)
      writeEvidence(evidence, Vector(
        "role" -> "producer",
        "testedCommit" -> testedCommit,
        "resultDigest" -> result.digest,
        "rowCount" -> result.rowCount.toString,
        "mapperCount" -> exchange.numMappers.toString,
        "originShuffleId" -> exchange.shuffleId.toString,
        "publishedGeneration" -> ProviderA.toString))
    } finally {
      stopSpark(spark)
    }
  }

  private def runFailure(
      root: Path,
      group: String,
      testedCommit: String,
      producerEvidence: Path,
      evidence: Path): Unit = {
    val expected = readEvidence(producerEvidence)
    val spark = createSpark(root, "failure")
    try {
      spark.sparkContext.newShuffleId()
      val query = buildQuery(spark)
      val exchange = onlyExchange(query)
      require(exchange.numMappers == Mappers)
      val listener = attachTargetListener(spark, exchange)
      val identity = prepareAdoption(spark, root, group, exchange, Attempt2)

      // The claim and scheduler preparation have already validated A. Remove one exact bound map's
      // data file only now so the first provider fetch has authoritative Missing evidence.
      val providerA = ReferenceShuffleProvider.open(
        providerRoot(root), group, ProviderA, IncarnationA, spark.sparkContext.conf)
      val missingData = providerA.committedMapDirectory(0).resolve(ReferenceShuffleProvider.DataFileName)
      require(Files.deleteIfExists(missingData), s"failed to remove bound provider data: $missingData")

      val result = collectResult(query)
      drainListeners(spark)
      require(result.digest == expected("resultDigest"))
      require(!ShuffleRecoverySchedulerAdoption.isAdopted(exchange.shuffleId))
      require(
        listener.partitions == (0 until Mappers).toVector,
        s"whole-shuffle retry did not select every mapper: ${listener.partitions}")

      val retiredA = waitForRetirement(root, group, identity)
      require(retiredA, "authoritative Missing did not retire exact incarnation A")

      // Ensure the all-fresh shuffle is completely registered, then persist those fresh outputs as
      // successor B. If SQL cleanup already released them, submitMapStage recomputes ordinarily.
      spark.sparkContext.submitMapStage(exchange.shuffleDependency).get()
      drainListeners(spark)
      val providerB = ReferenceShuffleProvider.open(
        providerRoot(root), group, ProviderB, IncarnationB, spark.sparkContext.conf)
      val artifacts = copyCompletedShuffle(exchange, providerB)
      publishManifest(root, group, ProviderB, IncarnationB, identity, exchange, artifacts)
      val successor = new ShuffleRecoveryManifestStore(manifestRoot(root))
        .findCompatible(group, identity, Attempt3)
      require(successor.exists(_.generation == ProviderB))

      writeEvidence(evidence, Vector(
        "role" -> "failure",
        "testedCommit" -> testedCommit,
        "resultDigest" -> result.digest,
        "rowCount" -> result.rowCount.toString,
        "adopted" -> "false",
        "retiredA" -> retiredA.toString,
        "mapTaskCount" -> listener.taskCount.toString,
        "mapTaskPartitions" -> listener.partitions.mkString(","),
        "publishedGeneration" -> ProviderB.toString,
        "currentShuffleId" -> exchange.shuffleId.toString))
    } finally {
      stopSpark(spark)
    }
  }

  private def runHealed(
      root: Path,
      group: String,
      testedCommit: String,
      failureEvidence: Path,
      evidence: Path): Unit = {
    val expected = readEvidence(failureEvidence)
    val spark = createSpark(root, "healed")
    try {
      spark.sparkContext.newShuffleId()
      val query = buildQuery(spark)
      val exchange = onlyExchange(query)
      require(exchange.numMappers == Mappers)
      val listener = attachTargetListener(spark, exchange)
      prepareAdoption(spark, root, group, exchange, Attempt3)
      val result = collectResult(query)
      drainListeners(spark)
      require(result.digest == expected("resultDigest"))
      require(ShuffleRecoverySchedulerAdoption.isAdopted(exchange.shuffleId))
      require(listener.taskCount == 0L, s"healed successor launched ${listener.taskCount} map tasks")

      val identity = identityFor(exchange)
      val selected = new ShuffleRecoveryManifestStore(manifestRoot(root))
        .findCompatible(group, identity, Attempt3)
        .getOrElse(throw new IllegalStateException("generation 3 found no healed successor"))
      require(selected.generation == ProviderB)
      require(selected.incarnationId == IncarnationB)

      writeEvidence(evidence, Vector(
        "role" -> "healed",
        "testedCommit" -> testedCommit,
        "resultDigest" -> result.digest,
        "rowCount" -> result.rowCount.toString,
        "adopted" -> "true",
        "mapTaskCount" -> listener.taskCount.toString,
        "publishingGeneration" -> selected.generation.toString,
        "currentShuffleId" -> exchange.shuffleId.toString))
    } finally {
      stopSpark(spark)
    }
  }

  private def prepareAdoption(
      spark: SparkSession,
      root: Path,
      group: String,
      exchange: ShuffleExchangeExec,
      currentGeneration: Long): ShuffleRecoveryFeasibilityIdentity = {
    val currentTarget = target(exchange, materialization = currentGeneration)
    val inputs = feasibilityInputs
    val request = ShuffleRecoveryPreparationRequest(group, currentGeneration, currentTarget, inputs)
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

    val identity = inputs.identityFor(currentTarget)
    val manifest = new ShuffleRecoveryManifestStore(manifestRoot(root))
      .findCompatible(group, identity, currentGeneration)
      .getOrElse(throw new IllegalStateException("no compatible recovery manifest"))
    val boundary = new ShuffleRecoveryUntrustedBoundary
    val candidate = boundary.validateCandidate(request, identity, manifest) match {
      case Right(value) => value
      case Left(reason) => throw new IllegalStateException(reason.toString)
    }
    val provider = new ReferenceShuffleRecoveryClaimProvider(
      providerRoot(root), spark.sparkContext.conf)
    val claim = provider.claim(ShuffleRecoveryClaimRequest(
      candidate.recoveryGroup,
      candidate.publishingGeneration,
      candidate.incarnationId,
      candidate.identity.providerCompatibilityId,
      currentTarget.targetShuffleId,
      candidate.mapperCount,
      candidate.reducerCount,
      candidate.mapArtifacts)) match {
      case value: ShuffleRecoveryClaimed => value
      case other => throw new IllegalStateException(s"provider claim failed: $other")
    }
    val prepared = boundary.validateClaim(request, reservation, candidate, claim) match {
      case Right(value) => value
      case Left(reason) =>
        provider.release(claim.binding)
        throw new IllegalStateException(reason.toString)
    }
    scheduler.offerPrepared(
      prepared,
      provider,
      SparkEnv.get.blockManager.blockManagerId,
      Some(new ShuffleRecoveryManifestRetirer(manifestRoot(root)))) match {
      case Right(_) => identity
      case Left(reason) => throw new IllegalStateException(reason)
    }
  }

  private def waitForRetirement(
      root: Path,
      group: String,
      identity: ShuffleRecoveryFeasibilityIdentity): Boolean = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
    val store = new ShuffleRecoveryManifestStore(manifestRoot(root))
    while (System.nanoTime() < deadline) {
      if (store.findCompatible(group, identity, Attempt2).isEmpty) {
        return true
      }
      Thread.sleep(20L)
    }
    false
  }

  private def publishManifest(
      root: Path,
      group: String,
      generation: Long,
      incarnation: String,
      identity: ShuffleRecoveryFeasibilityIdentity,
      exchange: ShuffleExchangeExec,
      artifacts: Vector[ShuffleRecoveryMapArtifact]): Unit = {
    val manifest = ShuffleRecoveryManifest(
      group,
      generation,
      incarnation,
      identity,
      exchange.numMappers,
      exchange.numPartitions,
      artifacts,
      ShuffleRecoveryManifest.DescriptorVersion,
      None,
      publicationTimestampMillis = generation)
    val store = new ShuffleRecoveryManifestStore(manifestRoot(root))
    require(store.publish(manifest) == ShuffleRecoveryManifestPublished)
  }

  private def copyCompletedShuffle(
      exchange: ShuffleExchangeExec,
      provider: ReferenceShuffleProvider): Vector[ShuffleRecoveryMapArtifact] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val status = tracker.shuffleStatuses.getOrElse(
      exchange.shuffleId,
      throw new IllegalStateException("shuffle is absent from MapOutputTracker"))
    val statuses = status.mapStatuses.clone()
    require(statuses.length == exchange.numMappers)
    require(statuses.forall(_ != null), "shuffle is not completely materialized")
    val resolver = SparkEnv.get.blockingShuffleManager.shuffleBlockResolver
    val artifacts = Vector.newBuilder[ShuffleRecoveryMapArtifact]

    var mapIndex = 0
    while (mapIndex < statuses.length) {
      val mapStatus = statuses(mapIndex)
      val writer = provider.createMapOutputWriter(mapStatus.mapId, exchange.numPartitions)
      var reduceId = 0
      while (reduceId < exchange.numPartitions) {
        val buffer = resolver.getBlockData(
          ShuffleBlockId(exchange.shuffleId, mapStatus.mapId, reduceId),
          None)
        try {
          if (buffer.size() > 0L) {
            val partition = writer.getPartitionWriter(reduceId)
            val output = partition.openStream()
            val input = buffer.createInputStream()
            try copy(input, output) finally {
              try input.close() finally output.close()
            }
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
      mapIndex += 1
    }
    artifacts.result()
  }

  private def buildQuery(spark: SparkSession): DataFrame = {
    spark.range(0L, Rows, 1L, Mappers)
      .select(
        col("id"),
        pmod(col("id") * lit(17L) + lit(3L), lit(Reducers.toLong * 4L)).as("k"),
        lit("").as("payload"))
      .repartition(Reducers, col("k"))
      .select(col("id"), col("k"), col("payload"))
  }

  private def onlyExchange(query: DataFrame): ShuffleExchangeExec = {
    val exchanges = query.queryExecution.executedPlan.collect {
      case exchange: ShuffleExchangeExec => exchange
    }
    require(exchanges.size == 1, s"expected one exchange, found ${exchanges.size}")
    exchanges.head
  }

  private def collectResult(query: DataFrame): ResultSummary = {
    val rows = query.collect()
    ResultSummary(rows.length.toLong, digestRows(rows))
  }

  private def digestRows(rows: Array[Row]): String = {
    val canonical = rows.iterator.map { row =>
      s"${row.getLong(0)}:${row.getLong(1)}:${row.getString(2)}"
    }.toArray.sorted
    val digest = MessageDigest.getInstance("SHA-256")
    canonical.foreach { value =>
      val bytes = value.getBytes(StandardCharsets.UTF_8)
      digest.update(ByteBuffer.allocate(4).putInt(bytes.length).array())
      digest.update(bytes)
    }
    digest.digest().iterator.map(byte => f"${byte & 0xff}%02x").mkString
  }

  private def identityFor(exchange: ShuffleExchangeExec): ShuffleRecoveryFeasibilityIdentity = {
    feasibilityInputs.identityFor(target(exchange, materialization = 0L))
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

  private def feasibilityInputs: ShuffleRecoveryFeasibilityInputs = {
    ShuffleRecoveryFeasibilityInputs(
      "adopted-failure-source-v1",
      "sql-repartition-v1",
      "unsafe-row-v1",
      "hash-v1",
      s"rows=$Rows;mappers=$Mappers;reducers=$Reducers")
  }

  private def attachTargetListener(
      spark: SparkSession,
      exchange: ShuffleExchangeExec): TargetTaskListener = {
    val listener = new TargetTaskListener(exchange.shuffleDependency.rdd.id)
    spark.sparkContext.addSparkListener(listener)
    listener
  }

  private def createSpark(root: Path, name: String): SparkSession = {
    val local = root.resolve(s"spark-local-$name")
    val warehouse = root.resolve(s"warehouse-$name")
    Files.createDirectories(local)
    Files.createDirectories(warehouse)
    SparkSession.builder()
      .master("local[2]")
      .appName(s"shuffle-recovery-adopted-failure-$name")
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
    try spark.stop() finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  private def drainListeners(spark: SparkSession): Unit = {
    spark.sparkContext.listenerBus.waitUntilEmpty(TimeUnit.SECONDS.toMillis(30))
  }

  private def copy(input: InputStream, output: OutputStream): Unit = {
    val bytes = new Array[Byte](64 * 1024)
    var read = input.read(bytes)
    while (read >= 0) {
      if (read > 0) output.write(bytes, 0, read)
      read = input.read(bytes)
    }
  }

  private def writeEvidence(path: Path, values: Vector[(String, String)]): Unit = {
    val text = values.map { case (key, value) => s"$key\t$value" }.mkString("\n") + "\n"
    Files.write(
      path,
      text.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE_NEW,
      StandardOpenOption.WRITE)
  }

  private def readEvidence(path: Path): Map[String, String] = {
    Files.readAllLines(path, StandardCharsets.UTF_8).asScala.iterator.map { line =>
      val separator = line.indexOf('\t')
      require(separator > 0, s"invalid evidence line: $line")
      line.substring(0, separator) -> line.substring(separator + 1)
    }.toMap
  }

  private def required(options: Map[String, String], key: String): String = {
    options.getOrElse(key, throw new IllegalArgumentException(s"missing option: $key"))
  }

  private def providerRoot(root: Path): Path = root.resolve("provider")

  private def manifestRoot(root: Path): Path = root.resolve("manifests")
}
