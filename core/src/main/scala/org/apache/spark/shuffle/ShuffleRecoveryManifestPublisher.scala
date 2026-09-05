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
import java.nio.file.{FileAlreadyExistsException, Files, LinkOption, Path, Paths}
import java.util.concurrent.{ArrayBlockingQueue, RejectedExecutionException, ThreadFactory}
import java.util.concurrent.{ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.{MapOutputTrackerMaster, SparkConf, SparkEnv, Success}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.scheduler.{SparkListenerStageCompleted, SparkListenerStageSubmitted}
import org.apache.spark.scheduler.SparkListenerTaskEnd

/**
 * Immutable map-output selection frozen at a successful shuffle-stage completion boundary.
 *
 * `winningMapTaskIds` is ordered by map partition. Scheduler routing identifiers are diagnostic
 * only and never participate in the feasibility identity.
 */
private[spark] final case class ShuffleRecoveryPublication(
    shuffleId: Int,
    stageId: Int,
    stageAttemptId: Int,
    winningMapTaskIds: Vector[Long],
    reducerCount: Int)

private[shuffle] final case class ShuffleRecoveryPublisherConfig(
    providerRoot: Path,
    manifestRoot: Path,
    recoveryGroup: String,
    generation: Long,
    incarnationId: String,
    sourceToken: String,
    producerTag: String,
    rowEncoding: String,
    partitioningShape: String,
    reducerCount: Int,
    resolvedLiteral: String,
    queueCapacity: Int,
    targetShuffleId: Option[Int]) {

  def identityFor(mapperCount: Int): ShuffleRecoveryFeasibilityIdentity =
    ShuffleRecoveryFeasibilityIdentity.create(
      sourceToken,
      producerTag,
      rowEncoding,
      partitioningShape,
      mapperCount,
      reducerCount,
      resolvedLiteral)
}

private[shuffle] trait ShuffleRecoveryPublicationBackend {
  def publish(publication: ShuffleRecoveryPublication): Unit
}

private[shuffle] final class FilesystemShuffleRecoveryPublicationBackend(
    config: ShuffleRecoveryPublisherConfig) extends ShuffleRecoveryPublicationBackend {

  override def publish(publication: ShuffleRecoveryPublication): Unit = {
    validatePublication(publication)
    if (publication.reducerCount != config.reducerCount) {
      throw new IOException(
        "publication reducer count disagrees with configured feasibility identity")
    }
    if (config.targetShuffleId.exists(_ != publication.shuffleId)) {
      throw new IOException("publication does not belong to the configured target shuffle")
    }

    val identity = config.identityFor(publication.winningMapTaskIds.size)
    val provider = ReferenceShuffleProvider.open(
      config.providerRoot,
      config.recoveryGroup,
      config.generation,
      config.incarnationId)
    val artifacts = ShuffleRecoveryWinningSelection.certify(
      provider, publication.winningMapTaskIds, publication.reducerCount)
    val manifest = ShuffleRecoveryManifest(
      config.recoveryGroup,
      config.generation,
      config.incarnationId,
      identity,
      publication.winningMapTaskIds.size,
      publication.reducerCount,
      artifacts,
      ShuffleRecoveryManifest.DescriptorVersion,
      reducerBytes = None,
      publicationTimestampMillis = System.currentTimeMillis())
    new ShuffleRecoveryManifestStore(config.manifestRoot).publish(manifest)
  }

  private def validatePublication(publication: ShuffleRecoveryPublication): Unit = {
    if (publication == null) {
      throw new IllegalArgumentException("publication descriptor must not be null")
    }
    if (publication.shuffleId < 0 || publication.stageId < 0 ||
        publication.stageAttemptId < 0) {
      throw new IllegalArgumentException("invalid scheduler routing identifier")
    }
    if (publication.winningMapTaskIds == null ||
        publication.winningMapTaskIds.size > ShuffleRecoveryManifestCodec.MaxMaps ||
        publication.winningMapTaskIds.exists(_ < 0L) ||
        publication.winningMapTaskIds.distinct.size != publication.winningMapTaskIds.size) {
      throw new IllegalArgumentException("invalid winning map task selection")
    }
    if (publication.reducerCount <= 0 ||
        publication.reducerCount > ShuffleRecoveryManifestCodec.MaxReducers) {
      throw new IllegalArgumentException("invalid publication reducer count")
    }
  }
}

/**
 * Certifies the exact scheduler-frozen task-attempt selection against the reference provider.
 *
 * The provider never chooses a winner by shuffle id or by "latest" attempt. A missing, duplicate,
 * incomplete, or conflicting candidate fails publication rather than substituting another attempt.
 */
private[spark] object ShuffleRecoveryWinningSelection {
  private val MaxWinnerClaimBytes = 256L

  def certify(
      provider: ReferenceShuffleProvider,
      winningMapTaskIds: Vector[Long],
      reducerCount: Int): Vector[ShuffleRecoveryMapArtifact] = {
    if (provider == null || winningMapTaskIds == null) {
      throw new IllegalArgumentException("provider and winning selection must not be null")
    }
    if (winningMapTaskIds.size > ShuffleRecoveryManifestCodec.MaxMaps ||
        winningMapTaskIds.exists(_ < 0L) ||
        winningMapTaskIds.distinct.size != winningMapTaskIds.size) {
      throw new IllegalArgumentException("invalid winning map task selection")
    }
    if (reducerCount <= 0 || reducerCount > ReferenceShuffleProvider.MaxReducers) {
      throw new IllegalArgumentException("invalid reducer count")
    }
    winningMapTaskIds.zipWithIndex.map { case (mapTaskId, mapIndex) =>
      certifyMap(provider, mapIndex, mapTaskId, reducerCount)
    }
  }

  private def certifyMap(
      provider: ReferenceShuffleProvider,
      mapIndex: Int,
      mapTaskId: Long,
      reducerCount: Int): ShuffleRecoveryMapArtifact = {
    readCommitted(provider, mapIndex, mapTaskId, reducerCount).getOrElse {
      val candidate = findExactCandidate(provider, mapTaskId)
      val candidateName = candidate.getFileName.toString
      val descriptor = ReferenceShuffleOutputDescriptor(
        candidateName,
        mapTaskId,
        reducerCount,
        Files.size(candidate.resolve(ReferenceShuffleProvider.DataFileName)),
        Files.size(candidate.resolve(ReferenceShuffleProvider.IndexFileName)))
      try {
        provider.commitWinner(mapIndex, descriptor)
      } catch {
        case _: FileAlreadyExistsException =>
          return readCommitted(provider, mapIndex, mapTaskId, reducerCount).getOrElse {
            throw new IOException(
              "concurrent winner binding selected a different map attempt")
          }
      }
      readCommitted(provider, mapIndex, mapTaskId, reducerCount).getOrElse {
        throw new IOException("provider did not expose the committed winning map")
      }
    }
  }

  private def findExactCandidate(provider: ReferenceShuffleProvider, mapTaskId: Long): Path = {
    val attempts = provider.attemptsPath
    if (!Files.isDirectory(attempts, LinkOption.NOFOLLOW_LINKS) ||
        Files.isSymbolicLink(attempts)) {
      throw new IOException("provider attempt namespace is missing or unsafe")
    }
    val prefix = s"attempt-$mapTaskId-"
    val stream = Files.list(attempts)
    try {
      val matches = stream.iterator().asScala.filter { path =>
        val name = path.getFileName.toString
        name.startsWith(prefix) &&
          Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS) &&
          !Files.isSymbolicLink(path)
      }.take(2).toVector
      if (matches.size != 1) {
        throw new IOException(
          s"expected exactly one provider candidate for winning map task $mapTaskId")
      }
      val candidate = matches.head
      requireCompleteCandidate(candidate)
      candidate
    } finally {
      stream.close()
    }
  }

  private def requireCompleteCandidate(candidate: Path): Unit = {
    val ready = candidate.resolve(ReferenceShuffleProvider.ReadyFileName)
    val data = candidate.resolve(ReferenceShuffleProvider.DataFileName)
    val index = candidate.resolve(ReferenceShuffleProvider.IndexFileName)
    if (!Files.isRegularFile(ready, LinkOption.NOFOLLOW_LINKS) ||
        !Files.isRegularFile(data, LinkOption.NOFOLLOW_LINKS) ||
        !Files.isRegularFile(index, LinkOption.NOFOLLOW_LINKS) ||
        Files.isSymbolicLink(ready) ||
        Files.isSymbolicLink(data) ||
        Files.isSymbolicLink(index)) {
      throw new IOException("winning provider candidate is incomplete or unsafe")
    }
  }

  private def readCommitted(
      provider: ReferenceShuffleProvider,
      mapIndex: Int,
      mapTaskId: Long,
      reducerCount: Int): Option[ShuffleRecoveryMapArtifact] = {
    val mapDirectory = provider.committedMapDirectory(mapIndex)
    val winner = mapDirectory.resolveSibling(s"map-$mapIndex.winner")
    if (!Files.exists(mapDirectory, LinkOption.NOFOLLOW_LINKS) &&
        !Files.exists(winner, LinkOption.NOFOLLOW_LINKS)) {
      return None
    }
    if (!Files.isDirectory(mapDirectory, LinkOption.NOFOLLOW_LINKS) ||
        Files.isSymbolicLink(mapDirectory) ||
        !Files.isRegularFile(winner, LinkOption.NOFOLLOW_LINKS) ||
        Files.isSymbolicLink(winner) ||
        Files.size(winner) > MaxWinnerClaimBytes) {
      throw new IOException("invalid committed provider winner state")
    }
    val handle = new String(Files.readAllBytes(winner), StandardCharsets.UTF_8).trim
    val expectedPrefix = s"attempt-$mapTaskId-"
    if (!handle.startsWith(expectedPrefix) || !handle.matches("[A-Za-z0-9._-]+")) {
      throw new IOException("committed provider winner does not match frozen task selection")
    }
    val resolved = provider.openMap(mapIndex)
    if (resolved.numReducers != reducerCount) {
      throw new IOException("committed provider winner has incompatible reducer shape")
    }
    Some(ShuffleRecoveryMapArtifact(
      mapIndex,
      mapTaskId,
      handle,
      resolved.dataLength,
      resolved.indexBytes))
  }
}

/**
 * Bounded owned asynchronous publisher for optional recovery metadata.
 *
 * Submission is non-blocking. Provider/store/filesystem work runs only on the owned worker.
 * Publication failure is observable but can never fail an already successful Spark stage.
 */
private[spark] final class ShuffleRecoveryManifestPublisher private[shuffle] (
    backend: ShuffleRecoveryPublicationBackend,
    queueCapacity: Int,
    workerName: String = ShuffleRecoveryManifestPublisher.WorkerName) extends Logging {

  if (backend == null) {
    throw new IllegalArgumentException("publication backend must not be null")
  }
  if (queueCapacity <= 0 ||
      queueCapacity > ShuffleRecoveryManifestPublisher.MaxQueueCapacity) {
    throw new IllegalArgumentException("invalid manifest publisher queue capacity")
  }

  private val closed = new AtomicBoolean(false)
  private val published = new AtomicLong(0L)
  private val failed = new AtomicLong(0L)
  private val rejected = new AtomicLong(0L)
  private val threadCounter = new AtomicInteger(0)
  private val executor = new ThreadPoolExecutor(
    1,
    1,
    0L,
    TimeUnit.MILLISECONDS,
    new ArrayBlockingQueue[Runnable](queueCapacity),
    new ThreadFactory {
      override def newThread(runnable: Runnable): Thread = {
        val thread = new Thread(
          runnable,
          s"$workerName-${threadCounter.incrementAndGet()}")
        thread.setDaemon(true)
        thread
      }
    },
    new ThreadPoolExecutor.AbortPolicy())

  /** Returns false on queue saturation or after close; never waits for publication I/O. */
  def submit(publication: ShuffleRecoveryPublication): Boolean = {
    if (publication == null) {
      throw new IllegalArgumentException("publication descriptor must not be null")
    }
    if (closed.get()) {
      rejected.incrementAndGet()
      return false
    }
    try {
      executor.execute(new Runnable {
        override def run(): Unit = {
          try {
            backend.publish(publication)
            published.incrementAndGet()
          } catch {
            case NonFatal(e) =>
              failed.incrementAndGet()
              logWarning(
                s"Shuffle recovery manifest publication failed for shuffle " +
                  s"${publication.shuffleId}, stage " +
                  s"${publication.stageId}.${publication.stageAttemptId}",
                e)
          }
        }
      })
      true
    } catch {
      case _: RejectedExecutionException =>
        rejected.incrementAndGet()
        logWarning(
          s"Shuffle recovery manifest publisher queue is full; skipping shuffle " +
            s"${publication.shuffleId} publication")
        false
    }
  }

  /** Stops accepting work and deterministically drains the bounded queue. */
  def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      executor.shutdown()
      try {
        if (!executor.awaitTermination(
            ShuffleRecoveryManifestPublisher.CloseTimeoutSeconds,
            TimeUnit.SECONDS)) {
          executor.shutdownNow()
          executor.awaitTermination(
            ShuffleRecoveryManifestPublisher.CloseTimeoutSeconds,
            TimeUnit.SECONDS)
        }
      } catch {
        case _: InterruptedException =>
          executor.shutdownNow()
          Thread.currentThread().interrupt()
      }
    }
  }

  private[shuffle] def publishedCount: Long = published.get()
  private[shuffle] def failedCount: Long = failed.get()
  private[shuffle] def rejectedCount: Long = rejected.get()
  private[shuffle] def isTerminated: Boolean = executor.isTerminated
}

/**
 * Bounded listener-side bookkeeping for one shuffle stage across retries.
 *
 * Successful task events update the latest task id for their map partition. Failed stage attempts
 * keep their accepted winners so a later retry can supply only missing partitions. On a successful
 * completion, the vector is frozen and validated against Spark's current tracker state before it
 * is enqueued. If listener evidence and tracker state disagree, publication is skipped.
 */
private[shuffle] final class ShuffleRecoveryPublicationCoordinator(
    publisher: ShuffleRecoveryManifestPublisher,
    reducerCount: Int,
    targetShuffleId: Option[Int],
    currentSelection: ShuffleRecoveryPublication => Boolean) extends Logging {

  private case class StageState(
      shuffleId: Int,
      expectedMaps: Int,
      var latestAttemptId: Int,
      var active: Boolean,
      winners: mutable.Map[Int, Long])

  private val stages = mutable.HashMap.empty[Int, StageState]

  def stageSubmitted(
      stageId: Int,
      stageAttemptId: Int,
      shuffleId: Int,
      expectedMaps: Int): Unit = synchronized {
    if (stageId < 0 || stageAttemptId < 0 || shuffleId < 0 ||
        expectedMaps < 0 || expectedMaps > ShuffleRecoveryManifestCodec.MaxMaps) {
      logWarning("Ignoring invalid shuffle recovery stage-submission metadata")
      return
    }
    if (!targetShuffleId.forall(_ == shuffleId)) {
      return
    }

    stages.get(stageId) match {
      case Some(state)
          if state.shuffleId == shuffleId && state.expectedMaps == expectedMaps =>
        if (stageAttemptId > state.latestAttemptId) {
          state.latestAttemptId = stageAttemptId
          state.active = true
        } else if (stageAttemptId == state.latestAttemptId && !state.active) {
          state.active = true
        }
      case Some(_) =>
        stages.remove(stageId)
        logWarning(
          s"Disabling shuffle recovery tracking for stage $stageId after incompatible metadata")
      case None =>
        if (stages.size >= ShuffleRecoveryPublicationCoordinator.MaxTrackedStages) {
          logWarning(
            "Shuffle recovery publication tracking is at its bounded stage limit; " +
              s"skipping stage $stageId")
        } else {
          stages(stageId) = StageState(
            shuffleId,
            expectedMaps,
            stageAttemptId,
            active = true,
            mutable.HashMap.empty[Int, Long])
        }
    }
  }

  def taskSucceeded(
      stageId: Int,
      stageAttemptId: Int,
      partitionId: Int,
      mapTaskId: Long): Unit = synchronized {
    stages.get(stageId).foreach { state =>
      if (state.active &&
          state.latestAttemptId == stageAttemptId &&
          partitionId >= 0 &&
          partitionId < state.expectedMaps &&
          mapTaskId >= 0L) {
        // MapOutputTracker replaces an existing map status with a later accepted success.
        // Keep the latest listener-ordered success and validate it at the completion boundary.
        state.winners(partitionId) = mapTaskId
      }
    }
  }

  def stageCompleted(
      stageId: Int,
      stageAttemptId: Int,
      successful: Boolean): Unit = {
    val publication = synchronized {
      stages.get(stageId) match {
        case Some(state)
            if state.active && state.latestAttemptId == stageAttemptId && successful =>
          stages.remove(stageId)
          val complete = state.winners.size == state.expectedMaps &&
            (0 until state.expectedMaps).forall(state.winners.contains)
          if (complete) {
            val winners = Vector.tabulate(state.expectedMaps)(state.winners)
            Some(ShuffleRecoveryPublication(
              state.shuffleId,
              stageId,
              stageAttemptId,
              winners,
              reducerCount))
          } else {
            logWarning(
              s"Skipping shuffle recovery publication for stage $stageId.$stageAttemptId: " +
                s"only ${state.winners.size}/${state.expectedMaps} winners were observed")
            None
          }

        case Some(state)
            if state.active && state.latestAttemptId == stageAttemptId && !successful =>
          // Preserve winners that Spark may reuse in a later determinate retry.
          state.active = false
          None

        case _ =>
          None
      }
    }

    publication.foreach { frozen =>
      val accepted = try {
        currentSelection(frozen)
      } catch {
        case NonFatal(e) =>
          logWarning(
            s"Unable to validate shuffle recovery winner selection for stage " +
              s"$stageId.$stageAttemptId; skipping publication",
            e)
          false
      }
      if (accepted) {
        publisher.submit(frozen)
      } else {
        logWarning(
          s"Skipping shuffle recovery publication for stage $stageId.$stageAttemptId because " +
            "listener winners do not match Spark's current map-output state")
      }
    }
  }

  def clear(): Unit = synchronized {
    stages.clear()
  }

  private[shuffle] def trackedAttemptCount: Int = synchronized {
    stages.size
  }
}

private[shuffle] object ShuffleRecoveryPublicationCoordinator {
  private val MaxTrackedStages = 1024
}

/**
 * Phase 0 publication listener.
 *
 * Listener callbacks do bounded in-memory bookkeeping only. Provider, manifest-store, and
 * filesystem work is delegated to [[ShuffleRecoveryManifestPublisher]].
 */
private[spark] final class ShuffleRecoveryManifestListener private[shuffle] (
    conf: SparkConf,
    currentSelection: ShuffleRecoveryPublication => Boolean)
  extends SparkListener with Logging {

  def this(conf: SparkConf) =
    this(conf, ShuffleRecoveryManifestListener.currentSelectionMatchesTracker)

  private val configured = ShuffleRecoveryManifestPublisher.configFromSparkConf(conf)
  private val publisher = configured.map { config =>
    new ShuffleRecoveryManifestPublisher(
      new FilesystemShuffleRecoveryPublicationBackend(config),
      config.queueCapacity)
  }
  private val coordinator = configured.zip(publisher).headOption.map {
    case (config, worker) =>
      new ShuffleRecoveryPublicationCoordinator(
        worker,
        config.reducerCount,
        config.targetShuffleId,
        currentSelection)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val info = stageSubmitted.stageInfo
    info.shuffleDepId.foreach { shuffleId =>
      safely {
        val mapperCount = info.rddInfos.headOption
          .map(_.numPartitions)
          .getOrElse(info.numTasks)
        coordinator.foreach(_.stageSubmitted(
          info.stageId,
          info.attemptNumber(),
          shuffleId,
          mapperCount))
      }
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    if (taskEnd.taskType == "ShuffleMapTask" && taskEnd.reason == Success) {
      safely {
        coordinator.foreach(_.taskSucceeded(
          taskEnd.stageId,
          taskEnd.stageAttemptId,
          taskEnd.taskInfo.partitionId,
          taskEnd.taskInfo.taskId))
      }
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val info = stageCompleted.stageInfo
    if (info.shuffleDepId.isDefined) {
      safely {
        coordinator.foreach(_.stageCompleted(
          info.stageId,
          info.attemptNumber(),
          info.failureReason.isEmpty))
      }
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    coordinator.foreach(_.clear())
    publisher.foreach(_.close())
  }

  private def safely(body: => Unit): Unit = {
    try {
      body
    } catch {
      case NonFatal(e) =>
        logWarning("Ignoring Phase 0 shuffle recovery publication listener failure", e)
    }
  }
}

private[shuffle] object ShuffleRecoveryManifestListener extends Logging {
  private def currentSelectionMatchesTracker(
      publication: ShuffleRecoveryPublication): Boolean = {
    val env = SparkEnv.get
    if (env == null || publication == null) {
      return false
    }
    env.mapOutputTracker match {
      case tracker: MapOutputTrackerMaster =>
        publication.winningMapTaskIds.forall { mapTaskId =>
          tracker.getMapOutputLocation(publication.shuffleId, mapTaskId).isDefined
        }
      case _ =>
        logWarning(
          "Shuffle recovery publication requires the driver MapOutputTrackerMaster; " +
            "skipping publication")
        false
    }
  }
}

private[spark] object ShuffleRecoveryManifestPublisher extends Logging {
  private val EnabledKey = "spark.shuffle.recovery.phase0.manifest.enabled"
  private val ProviderRootKey = "spark.shuffle.recovery.phase0.provider.root"
  private val ManifestRootKey = "spark.shuffle.recovery.phase0.manifest.root"
  private val GroupKey = "spark.shuffle.recovery.phase0.group"
  private val GenerationKey = "spark.shuffle.recovery.phase0.generation"
  private val IncarnationKey = "spark.shuffle.recovery.phase0.incarnation"
  private val SourceTokenKey = "spark.shuffle.recovery.phase0.identity.sourceToken"
  private val ProducerTagKey = "spark.shuffle.recovery.phase0.identity.producerTag"
  private val RowEncodingKey = "spark.shuffle.recovery.phase0.identity.rowEncoding"
  private val PartitioningKey = "spark.shuffle.recovery.phase0.identity.partitioning"
  private val ReducerCountKey = "spark.shuffle.recovery.phase0.identity.reducerCount"
  private val ResolvedLiteralKey = "spark.shuffle.recovery.phase0.identity.resolvedLiteral"
  private val QueueCapacityKey = "spark.shuffle.recovery.phase0.publisher.queueCapacity"
  private val TargetShuffleIdKey = "spark.shuffle.recovery.phase0.targetShuffleId"
  private val OldFetchProtocolKey = "spark.shuffle.useOldFetchProtocol"

  private[shuffle] val WorkerName = "shuffle-recovery-manifest-publisher"
  private[shuffle] val MaxQueueCapacity = 1024
  private[shuffle] val CloseTimeoutSeconds = 5L
  private val DefaultQueueCapacity = 16

  /**
   * Returns None for disabled or malformed configuration. Invalid optional recovery configuration
   * must never prevent SparkContext construction.
   */
  def configFromSparkConf(conf: SparkConf): Option[ShuffleRecoveryPublisherConfig] = {
    if (conf == null) {
      return None
    }
    val enabled = conf.getOption(EnabledKey) match {
      case None | Some("false") => false
      case Some("true") => true
      case Some(other) =>
        logWarning(
          s"Ignoring invalid $EnabledKey=$other; shuffle recovery publication is disabled")
        false
    }
    if (!enabled) {
      return None
    }
    try {
      Some(parseConfig(conf))
    } catch {
      case NonFatal(e) =>
        logWarning(
          "Invalid Phase 0 shuffle recovery publication configuration; disabling it",
          e)
        None
    }
  }

  def fromSparkConf(conf: SparkConf): Option[ShuffleRecoveryManifestPublisher] =
    configFromSparkConf(conf).map { config =>
      new ShuffleRecoveryManifestPublisher(
        new FilesystemShuffleRecoveryPublicationBackend(config),
        config.queueCapacity)
    }

  private[shuffle] def parseConfig(conf: SparkConf): ShuffleRecoveryPublisherConfig = {
    def required(key: String): String =
      conf.getOption(key).filter(_.nonEmpty).getOrElse {
        throw new IllegalArgumentException(s"missing required configuration $key")
      }

    def positiveLong(key: String): Long = {
      val value = try {
        java.lang.Long.parseLong(required(key))
      } catch {
        case e: NumberFormatException =>
          throw new IllegalArgumentException(s"invalid numeric configuration $key", e)
      }
      if (value <= 0L) {
        throw new IllegalArgumentException(s"$key must be positive")
      }
      value
    }

    def positiveInt(key: String): Int = {
      val value = try {
        Integer.parseInt(required(key))
      } catch {
        case e: NumberFormatException =>
          throw new IllegalArgumentException(s"invalid numeric configuration $key", e)
      }
      if (value <= 0) {
        throw new IllegalArgumentException(s"$key must be positive")
      }
      value
    }

    // Under the old fetch protocol ShuffleMapTask uses partitionId as mapId while the recovery
    // proof is keyed to task-attempt ids. Refuse that mode rather than conflating identifiers.
    if (conf.getBoolean(OldFetchProtocolKey, false)) {
      throw new IllegalArgumentException(
        s"$OldFetchProtocolKey=true is outside the Phase 0 publication compatibility set")
    }

    val generation = positiveLong(GenerationKey)
    val reducerCount = positiveInt(ReducerCountKey)
    if (reducerCount > ShuffleRecoveryManifestCodec.MaxReducers) {
      throw new IllegalArgumentException("configured reducer count exceeds Phase 0 bound")
    }

    val queueCapacity = conf.getOption(QueueCapacityKey) match {
      case Some(value) =>
        val parsed = try {
          Integer.parseInt(value)
        } catch {
          case e: NumberFormatException =>
            throw new IllegalArgumentException("invalid manifest publisher queue capacity", e)
        }
        if (parsed <= 0 || parsed > MaxQueueCapacity) {
          throw new IllegalArgumentException(
            "manifest publisher queue capacity is out of bounds")
        }
        parsed
      case None =>
        DefaultQueueCapacity
    }

    val targetShuffleId = conf.getOption(TargetShuffleIdKey).map { raw =>
      val parsed = try {
        Integer.parseInt(raw)
      } catch {
        case e: NumberFormatException =>
          throw new IllegalArgumentException("invalid target shuffle id", e)
      }
      if (parsed < 0) {
        throw new IllegalArgumentException("target shuffle id must be non-negative")
      }
      parsed
    }

    val providerRoot = Paths.get(required(ProviderRootKey)).toAbsolutePath.normalize()
    val manifestRoot = Paths.get(required(ManifestRootKey)).toAbsolutePath.normalize()
    if (providerRoot == manifestRoot ||
        providerRoot.startsWith(manifestRoot) ||
        manifestRoot.startsWith(providerRoot)) {
      throw new IllegalArgumentException(
        "provider and manifest roots must be distinct non-nested Phase 0 namespaces")
    }

    val group = required(GroupKey)
    val incarnation = required(IncarnationKey)
    ShuffleRecoveryManifestCodec.validateIdentifier(group, "recovery group")
    ShuffleRecoveryManifestCodec.validateIdentifier(incarnation, "incarnation id")

    val config = ShuffleRecoveryPublisherConfig(
      providerRoot,
      manifestRoot,
      group,
      generation,
      incarnation,
      required(SourceTokenKey),
      required(ProducerTagKey),
      required(RowEncodingKey),
      required(PartitioningKey),
      reducerCount,
      required(ResolvedLiteralKey),
      queueCapacity,
      targetShuffleId)
    config.identityFor(0)
    config
  }
}
