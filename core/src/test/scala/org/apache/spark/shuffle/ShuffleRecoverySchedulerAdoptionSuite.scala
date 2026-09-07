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
import java.nio.file.{Files, Path}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import org.apache.spark._
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.collection.OpenHashSet

class ShuffleRecoverySchedulerAdoptionSuite extends SparkFunSuite with LocalSparkContext {
  private val AwaitSeconds = 30L
  private val RecoveryGroup = "scheduler-adoption-race"
  private val IncarnationA = "incarnation-a"

  private final class RecordingRetirer(block: Boolean = false)
    extends ShuffleRecoveryIncarnationRetirer {
    val calls = new AtomicInteger(0)
    val entered = new CountDownLatch(1)
    val completed = new CountDownLatch(1)
    private val proceed = new CountDownLatch(if (block) 1 else 0)

    override def retireExact(
        incarnation: ShuffleRecoveryManifestIncarnation): ShuffleRecoveryRetirementResult = {
      calls.incrementAndGet()
      entered.countDown()
      try {
        if (!proceed.await(AwaitSeconds, TimeUnit.SECONDS)) {
          throw new IllegalStateException("retirement test barrier timed out")
        }
        ShuffleRecoveryIncarnationRetired
      } finally {
        completed.countDown()
      }
    }

    def allow(): Unit = proceed.countDown()
  }

  private final class Harness(
      val conf: SparkConf,
      val root: Path,
      val tracker: MapOutputTrackerMaster,
      val dependency: ShuffleDependency[Int, Int, Int],
      val resolver: ShuffleRecoveryIndexShuffleBlockResolver,
      val state: ShuffleRecoverySchedulerAdoptionState,
      val claimProvider: ReferenceShuffleRecoveryClaimProvider,
      val claimRequest: ShuffleRecoveryClaimRequest,
      val binding: ShuffleRecoveryBinding,
      val oldLocation: BlockManagerId,
      val localBindingGeneration: Long,
      val retirer: RecordingRetirer,
      val mapperCount: Int,
      val reducerCount: Int)

  test("concurrent reducer callbacks clear every adopted map and fence stale A") {
    val retirer = new RecordingRetirer(block = true)
    withHarness(mapperCount = 2, reducerCount = 2, retirer) { h =>
      val producerDecomposition = Vector(Set(0, 2), Set(1, 3))
      val replacementDecomposition = Vector(Set(0, 1), Set(2, 3))
      assert(producerDecomposition != replacementDecomposition)
      assert(producerDecomposition.indices.exists { index =>
        producerDecomposition(index) != replacementDecomposition(index)
      })

      val oldStatus = h.tracker.shuffleStatuses(h.dependency.shuffleId)
      assert(oldStatus.numAvailableMapOutputs == h.mapperCount)
      assert(h.resolver.recoveredBindingCount == 1)
      val oldEpoch = h.tracker.getEpoch

      val worker = new MapOutputTrackerWorker(h.conf)
      worker.trackerEndpoint = h.tracker.trackerEndpoint
      worker.updateEpoch(oldEpoch)
      assert(worker.getMapSizesByExecutorId(h.dependency.shuffleId, 0).nonEmpty)

      val ready = new CountDownLatch(2)
      val start = new CountDownLatch(1)
      val done = new CountDownLatch(2)
      val executor = Executors.newFixedThreadPool(2)
      try {
        (0 until 2).foreach { reduceId =>
          executor.submit(new Runnable {
            override def run(): Unit = {
              ready.countDown()
              start.await()
              try {
                h.resolver.recordObservedFailure(
                  h.dependency.shuffleId,
                  ShuffleRecoveryObservedFetchFailure(
                    h.localBindingGeneration,
                    h.binding.bindingId,
                    mapIndex = 0,
                    reduceId,
                    ShuffleRecoveryAdoptedMissing))
              } finally {
                done.countDown()
              }
            }
          })
        }
        assert(ready.await(AwaitSeconds, TimeUnit.SECONDS))
        start.countDown()
        assert(done.await(AwaitSeconds, TimeUnit.SECONDS))
      } finally {
        executor.shutdownNow()
      }

      assert(!h.state.isAdopted(h.dependency.shuffleId))
      assert(h.state.invalidatedBindingCount == 1)
      assert(h.resolver.recoveredBindingCount == 0)
      assert(h.tracker.getNumAvailableOutputs(h.dependency.shuffleId) == 0)
      assert(h.tracker.findMissingPartitions(h.dependency.shuffleId).contains(Seq(0, 1)))
      assert(h.tracker.getEpoch > oldEpoch)
      assert(h.tracker.shuffleStatuses(h.dependency.shuffleId) ne oldStatus)
      assert(h.state.consumeWholeStageRetryRequirement(h.dependency.shuffleId))
      assert(!h.state.consumeWholeStageRetryRequirement(h.dependency.shuffleId))

      worker.updateEpoch(h.tracker.getEpoch)
      intercept[FetchFailedException] {
        worker.getMapSizesByExecutorId(h.dependency.shuffleId, 0).toSeq
      }
      worker.stop()

      assert(retirer.entered.await(AwaitSeconds, TimeUnit.SECONDS))
      assert(retirer.calls.get() == 1)

      val fresh = BlockManagerId("fresh", "localhost", 7338)
      (0 until h.mapperCount).foreach { mapIndex =>
        h.tracker.registerMapOutput(
          h.dependency.shuffleId,
          mapIndex,
          MapStatus(fresh, Array.fill[Long](h.reducerCount)(1L), 100L + mapIndex))
      }
      assert(h.tracker.getNumAvailableOutputs(h.dependency.shuffleId) == h.mapperCount)

      val stale = h.state.handleFetchFailure(
        h.tracker,
        h.dependency,
        h.oldLocation,
        oldEpoch)
      assert(stale == ShuffleRecoveryFetchFailureStale)
      assert(h.tracker.getNumAvailableOutputs(h.dependency.shuffleId) == h.mapperCount)
      assert(h.tracker.shuffleStatuses(h.dependency.shuffleId).mapStatuses.forall { status =>
        status != null && status.location == fresh
      })

      h.resolver.recordObservedFailure(
        h.dependency.shuffleId,
        ShuffleRecoveryObservedFetchFailure(
          h.localBindingGeneration,
          h.binding.bindingId,
          mapIndex = 0,
          reduceId = 1,
          ShuffleRecoveryAdoptedCorrupt))
      assert(retirer.calls.get() == 1)
      retirer.allow()
      assert(retirer.completed.await(AwaitSeconds, TimeUnit.SECONDS))
      assert(retirer.calls.get() == 1)
    }
  }

  test("unavailable is non-destructive and later exact proof retires A once") {
    val retirer = new RecordingRetirer(block = true)
    withHarness(mapperCount = 2, reducerCount = 2, retirer) { h =>
      h.resolver.recordObservedFailure(
        h.dependency.shuffleId,
        ShuffleRecoveryObservedFetchFailure(
          h.localBindingGeneration,
          h.binding.bindingId,
          mapIndex = 0,
          reduceId = 0,
          ShuffleRecoveryAdoptedUnavailable))

      assert(!h.state.isAdopted(h.dependency.shuffleId))
      assert(h.tracker.getNumAvailableOutputs(h.dependency.shuffleId) == 0)
      assert(retirer.calls.get() == 0)
      assert(h.resolver.observedFetchFailure(h.dependency.shuffleId).exists { observed =>
        observed.failureClass == ShuffleRecoveryAdoptedUnavailable
      })

      h.resolver.recordObservedFailure(
        h.dependency.shuffleId,
        ShuffleRecoveryObservedFetchFailure(
          h.localBindingGeneration,
          h.binding.bindingId,
          mapIndex = 0,
          reduceId = 1,
          ShuffleRecoveryAdoptedMissing))
      assert(retirer.entered.await(AwaitSeconds, TimeUnit.SECONDS))
      assert(retirer.calls.get() == 1)

      h.resolver.recordObservedFailure(
        h.dependency.shuffleId,
        ShuffleRecoveryObservedFetchFailure(
          h.localBindingGeneration,
          h.binding.bindingId,
          mapIndex = 1,
          reduceId = 0,
          ShuffleRecoveryAdoptedCorrupt))
      assert(retirer.calls.get() == 1)
      retirer.allow()
      assert(retirer.completed.await(AwaitSeconds, TimeUnit.SECONDS))
      assert(retirer.calls.get() == 1)
    }
  }

  test("failure racing shutdown leaves no adopted state or recovered binding") {
    val retirer = new RecordingRetirer()
    withHarness(mapperCount = 2, reducerCount = 2, retirer, closeAfter = false) { h =>
      val ready = new CountDownLatch(2)
      val start = new CountDownLatch(1)
      val done = new CountDownLatch(2)
      val failures = new java.util.concurrent.ConcurrentLinkedQueue[Throwable]()
      val executor = Executors.newFixedThreadPool(2)
      try {
        executor.submit(new Runnable {
          override def run(): Unit = {
            ready.countDown()
            start.await()
            try {
              h.resolver.recordObservedFailure(
                h.dependency.shuffleId,
                ShuffleRecoveryObservedFetchFailure(
                  h.localBindingGeneration,
                  h.binding.bindingId,
                  mapIndex = 0,
                  reduceId = 0,
                  ShuffleRecoveryAdoptedMissing))
            } catch {
              case error: Throwable => failures.add(error)
            } finally {
              done.countDown()
            }
          }
        })
        executor.submit(new Runnable {
          override def run(): Unit = {
            ready.countDown()
            start.await()
            try h.state.close() catch {
              case error: Throwable => failures.add(error)
            } finally {
              done.countDown()
            }
          }
        })
        assert(ready.await(AwaitSeconds, TimeUnit.SECONDS))
        start.countDown()
        assert(done.await(AwaitSeconds, TimeUnit.SECONDS))
      } finally {
        executor.shutdownNow()
      }

      assert(failures.isEmpty, failures.asScala.mkString("; "))
      assert(!h.state.isAdopted(h.dependency.shuffleId))
      assert(h.resolver.recoveredBindingCount == 0)
      h.state.close()
    }
  }

  test("late release of A cannot unbind a later provider binding") {
    val retirer = new RecordingRetirer()
    withHarness(mapperCount = 2, reducerCount = 2, retirer) { h =>
      val callbackReady = new CountDownLatch(1)
      val allowCallback = new CountDownLatch(1)
      val callbackDone = new CountDownLatch(1)
      val callbackFailure = new java.util.concurrent.atomic.AtomicReference[Throwable]()
      val callback = new Thread(() => {
        callbackReady.countDown()
        try {
          allowCallback.await()
          h.claimProvider.release(h.binding)
        } catch {
          case error: Throwable => callbackFailure.set(error)
        } finally {
          callbackDone.countDown()
        }
      }, "delayed-recovery-release")
      callback.start()
      assert(callbackReady.await(AwaitSeconds, TimeUnit.SECONDS))

      h.resolver.recordObservedFailure(
        h.dependency.shuffleId,
        ShuffleRecoveryObservedFetchFailure(
          h.localBindingGeneration,
          h.binding.bindingId,
          mapIndex = 0,
          reduceId = 0,
          ShuffleRecoveryAdoptedUnavailable))
      assert(!h.state.isAdopted(h.dependency.shuffleId))
      assert(retirer.calls.get() == 0)

      val successor = h.claimProvider.claim(h.claimRequest) match {
        case value: ShuffleRecoveryClaimed => value
        case other => fail(s"successor provider claim failed: $other")
      }
      assert(h.claimProvider.isBound(successor.binding))

      allowCallback.countDown()
      assert(callbackDone.await(AwaitSeconds, TimeUnit.SECONDS))
      assert(callbackFailure.get() == null)
      assert(h.claimProvider.isBound(successor.binding))
      h.claimProvider.release(successor.binding)
    }
  }

  private def withHarness(
      mapperCount: Int,
      reducerCount: Int,
      retirer: RecordingRetirer,
      closeAfter: Boolean = true)(body: Harness => Unit): Unit = {
    val root = Files.createTempDirectory("shuffle-recovery-scheduler-adoption-")
    val conf = new SparkConf(false)
      .setMaster("local[2]")
      .setAppName("shuffle-recovery-scheduler-adoption-suite")
      .set("spark.ui.enabled", "false")
      .set("spark.shuffle.compress", "false")
    sc = new SparkContext(conf)

    val dependency = new ShuffleDependency[Int, Int, Int](
      sc.parallelize((0 until mapperCount).map(index => index -> index), mapperCount),
      new HashPartitioner(reducerCount))
    val tracker = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    if (!tracker.containsShuffle(dependency.shuffleId)) {
      tracker.registerShuffle(dependency.shuffleId, mapperCount, reducerCount)
    }

    val provider = ReferenceShuffleProvider.open(root, RecoveryGroup, 1L, IncarnationA, conf)
    val artifacts = Vector.tabulate(mapperCount) { mapIndex =>
      val mapTaskId = 100L + mapIndex
      val writer = provider.createMapOutputWriter(mapTaskId, reducerCount)
      (0 until reducerCount).foreach { reduceId =>
        val stream = writer.getPartitionWriter(reduceId).openStream()
        try {
          stream.write(s"$mapIndex:$reduceId".getBytes(StandardCharsets.UTF_8))
        } finally {
          stream.close()
        }
      }
      val descriptor = writer.commitAllPartitions(Array.empty[Long])
        .getMapOutputMetadata.get().asInstanceOf[ReferenceShuffleOutputDescriptor]
      provider.commitWinner(mapIndex, descriptor)
      ShuffleRecoveryMapArtifact(
        mapIndex,
        mapTaskId,
        descriptor.candidateName,
        descriptor.dataLength,
        descriptor.indexLength)
    }

    val target = ShuffleRecoveryAdoptionTarget(
      ShuffleRecoveryMaterializationId(1L, 1L),
      dependency.shuffleId,
      dependency.rdd.id.toLong,
      mapperCount,
      reducerCount)
    val feasibility = ShuffleRecoveryFeasibilityInputs(
      "scheduler-adoption-source",
      "scheduler-adoption-producer",
      "row-v1",
      "hash-v1",
      s"mappers=$mapperCount;reducers=$reducerCount")
    val identity = feasibility.identityFor(target)
    val request = ShuffleRecoveryPreparationRequest(RecoveryGroup, 2L, target, feasibility)
    val manifest = ShuffleRecoveryManifest(
      RecoveryGroup,
      1L,
      IncarnationA,
      identity,
      mapperCount,
      reducerCount,
      artifacts,
      ShuffleRecoveryManifest.DescriptorVersion,
      None,
      publicationTimestampMillis = 1L)
    val boundary = new ShuffleRecoveryUntrustedBoundary
    val candidate = boundary.validateCandidate(request, identity, manifest) match {
      case Right(value) => value
      case Left(reason) => fail(reason)
    }
    val claimProvider = new ReferenceShuffleRecoveryClaimProvider(root, conf)
    val claimRequest = ShuffleRecoveryClaimRequest(
      candidate.recoveryGroup,
      candidate.publishingGeneration,
      candidate.incarnationId,
      candidate.identity.providerCompatibilityId,
      dependency.shuffleId,
      mapperCount,
      reducerCount,
      artifacts)
    val claimed = claimProvider.claim(claimRequest) match {
      case value: ShuffleRecoveryClaimed => value
      case other => fail(s"provider claim failed: $other")
    }
    val manager = new ShuffleRecoveryReservationManager
    val reservation = manager.reserve(target) match {
      case Right(value) => value
      case Left(reason) => fail(reason)
    }
    val prepared = boundary.validateClaim(request, reservation, candidate, claimed) match {
      case Right(value) => value
      case Left(reason) => fail(reason)
    }

    val resolver = new ShuffleRecoveryIndexShuffleBlockResolver(
      conf,
      new ConcurrentHashMap[Int, OpenHashSet[Long]]())
    val state = resolver.schedulerAdoption
    assert(state.registerReservation(
      manager,
      reservation,
      dependency,
      mapperCount,
      reducerCount).isRight)
    assert(state.offerPrepared(
      prepared,
      claimProvider,
      BlockManagerId("recovery-host", "localhost", 7337),
      Some(retirer)).isRight)
    assert(state.beforeFindMissingPartitions(tracker, dependency, mapperCount))
    assert(state.isAdopted(dependency.shuffleId))

    val oldLocation = tracker.shuffleStatuses(dependency.shuffleId).mapStatuses(0).location
    val prefix = "shuffle-recovery-"
    assert(oldLocation.executorId.startsWith(prefix))
    val localBindingGeneration = oldLocation.executorId.substring(prefix.length).toLong
    val harness = new Harness(
      conf,
      root,
      tracker,
      dependency,
      resolver,
      state,
      claimProvider,
      claimRequest,
      claimed.binding,
      oldLocation,
      localBindingGeneration,
      retirer,
      mapperCount,
      reducerCount)

    try {
      body(harness)
    } finally {
      retirer.allow()
      if (closeAfter) state.close()
      try provider.cleanupGroup() catch {
        case _: Throwable =>
      }
      ReferenceShuffleProvider.deleteRecursively(root)
    }
  }
}
