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
import java.util.concurrent.{ArrayBlockingQueue, RejectedExecutionException, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
 * Immutable scheduler-side publication input.
 *
 * The map task ids are frozen while the DAGScheduler owns the authoritative completed stage. The
 * asynchronous publisher must use this exact vector and must never rediscover winners by shuffle
 * id, so a late speculative completion or a newer stage attempt cannot switch the selection.
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
    resolvedLiteral: String,
    queueCapacity: Int) {

  def identityFor(mapperCount: Int, reducerCount: Int): ShuffleRecoveryFeasibilityIdentity =
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
    if (publication == null) {
      throw new IllegalArgumentException("publication descriptor must not be null")
    }
    if (publication.winningMapTaskIds.size > ShuffleRecoveryManifestCodec.MaxMaps ||
        publication.winningMapTaskIds.exists(_ < 0L)) {
      throw new IllegalArgumentException("invalid winning map task selection")
    }
    if (publication.reducerCount <= 0 ||
        publication.reducerCount > ShuffleRecoveryManifestCodec.MaxReducers) {
      throw new IllegalArgumentException("invalid publication reducer count")
    }

    val identity = config.identityFor(
      publication.winningMapTaskIds.size, publication.reducerCount)
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
}

/**
 * Certifies and binds the exact task-attempt selection frozen by the scheduler.
 *
 * The reference provider deliberately does not choose winners. For each map index this helper finds
 * only the candidate belonging to the frozen task id, validates the provider descriptor through
 * [[ReferenceShuffleProvider.commitWinner]], and returns an immutable O(M) handle vector. A later
 * candidate for the same map index is irrelevant because its task id is not in the frozen vector.
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
        winningMapTaskIds.exists(_ < 0L)) {
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
            throw new IOException("concurrent winner binding selected a different map attempt")
          }
      }
      readCommitted(provider, mapIndex, mapTaskId, reducerCount).getOrElse {
        throw new IOException("provider did not expose the committed winning map")
      }
    }
  }

  private def findExactCandidate(provider: ReferenceShuffleProvider, mapTaskId: Long): Path = {
    val attempts = provider.attemptsPath
    if (!Files.isDirectory(attempts, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("provider attempt namespace is missing")
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
        !Files.isRegularFile(index, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("winning provider candidate is incomplete")
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
 * Submission is a non-blocking queue operation and performs no provider/store/filesystem access.
 * Publication failures are logged and counted on the worker; they never fail an already-successful
 * shuffle stage. Fatal JVM errors are not converted into cache misses.
 */
private[spark] final class ShuffleRecoveryManifestPublisher private[shuffle] (
    backend: ShuffleRecoveryPublicationBackend,
    queueCapacity: Int,
    workerName: String = ShuffleRecoveryManifestPublisher.WorkerName) extends Logging {

  if (backend == null) {
    throw new IllegalArgumentException("publication backend must not be null")
  }
  if (queueCapacity <= 0 || queueCapacity > ShuffleRecoveryManifestPublisher.MaxQueueCapacity) {
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
        val thread = new Thread(runnable, s"$workerName-${threadCounter.incrementAndGet()}")
        thread.setDaemon(true)
        thread
      }
    },
    new ThreadPoolExecutor.AbortPolicy())

  /** Returns false on queue saturation or after close; never blocks the scheduler event loop. */
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
                s"Shuffle recovery manifest publication failed for shuffle ${publication.shuffleId} " +
                  s"stage ${publication.stageId}.${publication.stageAttemptId}", e)
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

  /** Stops accepting work and deterministically drains bounded queued publication work. */
  def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      executor.shutdown()
      try {
        if (!executor.awaitTermination(
            ShuffleRecoveryManifestPublisher.CloseTimeoutSeconds, TimeUnit.SECONDS)) {
          executor.shutdownNow()
          executor.awaitTermination(
            ShuffleRecoveryManifestPublisher.CloseTimeoutSeconds, TimeUnit.SECONDS)
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
  private val ResolvedLiteralKey = "spark.shuffle.recovery.phase0.identity.resolvedLiteral"
  private val QueueCapacityKey = "spark.shuffle.recovery.phase0.publisher.queueCapacity"

  private[shuffle] val WorkerName = "shuffle-recovery-manifest-publisher"
  private[shuffle] val MaxQueueCapacity = 1024
  private[shuffle] val CloseTimeoutSeconds = 5L
  private val DefaultQueueCapacity = 16

  /**
   * Returns None for disabled or malformed Phase 0 configuration. Configuration problems disable
   * the optional cache path rather than preventing SparkContext construction.
   */
  def fromSparkConf(conf: SparkConf): Option[ShuffleRecoveryManifestPublisher] = {
    if (conf == null) {
      return None
    }
    val enabled = conf.getOption(EnabledKey) match {
      case None | Some("false") => false
      case Some("true") => true
      case Some(other) =>
        logWarning(s"Ignoring invalid $EnabledKey=$other; shuffle recovery publication is disabled")
        false
    }
    if (!enabled) {
      return None
    }
    try {
      val config = parseConfig(conf)
      val backend = new FilesystemShuffleRecoveryPublicationBackend(config)
      Some(new ShuffleRecoveryManifestPublisher(backend, config.queueCapacity))
    } catch {
      case NonFatal(e) =>
        logWarning("Invalid Phase 0 shuffle recovery publication configuration; disabling it", e)
        None
    }
  }

  private[shuffle] def parseConfig(conf: SparkConf): ShuffleRecoveryPublisherConfig = {
    def required(key: String): String = conf.getOption(key).filter(_.nonEmpty).getOrElse {
      throw new IllegalArgumentException(s"missing required configuration $key")
    }
    val generation = try {
      java.lang.Long.parseLong(required(GenerationKey))
    } catch {
      case e: NumberFormatException =>
        throw new IllegalArgumentException("invalid shuffle recovery generation", e)
    }
    if (generation <= 0L) {
      throw new IllegalArgumentException("shuffle recovery generation must be positive")
    }
    val queueCapacity = conf.getOption(QueueCapacityKey) match {
      case Some(value) =>
        try {
          Integer.parseInt(value)
        } catch {
          case e: NumberFormatException =>
            throw new IllegalArgumentException("invalid manifest publisher queue capacity", e)
        }
      case None => DefaultQueueCapacity
    }
    if (queueCapacity <= 0 || queueCapacity > MaxQueueCapacity) {
      throw new IllegalArgumentException("manifest publisher queue capacity is out of bounds")
    }

    val group = required(GroupKey)
    val incarnation = required(IncarnationKey)
    ShuffleRecoveryManifestCodec.validateIdentifier(group, "recovery group")
    ShuffleRecoveryManifestCodec.validateIdentifier(incarnation, "incarnation id")
    val partitioning = required(PartitioningKey)
    val config = ShuffleRecoveryPublisherConfig(
      Paths.get(required(ProviderRootKey)).toAbsolutePath.normalize(),
      Paths.get(required(ManifestRootKey)).toAbsolutePath.normalize(),
      group,
      generation,
      incarnation,
      required(SourceTokenKey),
      required(ProducerTagKey),
      required(RowEncodingKey),
      partitioning,
      required(ResolvedLiteralKey),
      queueCapacity)
    // Validate all stable text/compatibility fields without touching the filesystem. Shape is
    // revalidated against the actual completed stage before publication.
    config.identityFor(0, 1)
    config
  }
}