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

import java.nio.file.Path

import org.apache.spark.{SparkConf, SparkFunSuite, Success}
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerStageCompleted}
import org.apache.spark.scheduler.{SparkListenerStageSubmitted, SparkListenerTaskEnd, StageInfo}
import org.apache.spark.scheduler.{TaskInfo, TaskLocality}

class ShuffleRecoveryManifestSuite extends SparkFunSuite {

  test("listener freezes exact winners only after a complete successful shuffle stage") {
    withTempDir { root =>
      val providerRoot = root.toPath.resolve("provider")
      val manifestRoot = root.toPath.resolve("manifest")
      val group = "listener-group"
      val incarnation = "listener-incarnation"
      val generation = 1L
      val shuffleId = 77
      val stageId = 9
      val taskIds = Vector(101L, 202L)

      val provider = ReferenceShuffleProvider.open(
        providerRoot,
        group,
        generation,
        incarnation,
        new SparkConf(false))
      taskIds.foreach { taskId =>
        val writer = provider.createMapOutputWriter(taskId, 1)
        writer.commitAllPartitions(Array(0L))
      }

      val conf = listenerConf(
        providerRoot, manifestRoot, group, generation, incarnation, shuffleId)
      val listener = new ShuffleRecoveryManifestListener(
        conf,
        publication => publication.winningMapTaskIds == taskIds)
      val stageInfo = shuffleStageInfo(stageId, shuffleId, taskIds.size)
      listener.onStageSubmitted(SparkListenerStageSubmitted(stageInfo))
      listener.onTaskEnd(successfulShuffleTask(stageId, 0, taskIds(0)))
      listener.onTaskEnd(successfulShuffleTask(stageId, 1, taskIds(1)))
      listener.onStageCompleted(SparkListenerStageCompleted(stageInfo))

      // Once StageCompleted closes the attempt, a late successful-looking event cannot replace
      // the frozen winner. Real speculative losers are TaskKilled by TaskSetManager; this extra
      // Success event is a stronger adapter-level fencing regression.
      listener.onTaskEnd(successfulShuffleTask(stageId, 0, 9999L))
      listener.onApplicationEnd(SparkListenerApplicationEnd(System.currentTimeMillis()))

      val config = ShuffleRecoveryManifestPublisher.parseConfig(conf)
      val identity = config.identityFor(taskIds.size)
      val manifest = new ShuffleRecoveryManifestStore(manifestRoot)
        .findCompatible(group, identity, currentGeneration = generation + 1L)
        .getOrElse(fail("listener did not publish a complete manifest"))
      assert(manifest.mapArtifacts.map(_.mapTaskId) == taskIds)
      assert(manifest.mapArtifacts.map(_.mapIndex) == Vector(0, 1))
      assert(provider.committedMapCount == taskIds.size)
    }
  }

  test("listener skips incomplete and failed shuffle-stage attempts") {
    withTempDir { root =>
      val providerRoot = root.toPath.resolve("provider")
      val manifestRoot = root.toPath.resolve("manifest")
      val group = "listener-negative"
      val incarnation = "listener-negative-incarnation"
      val generation = 1L
      val shuffleId = 88
      val conf = listenerConf(
        providerRoot, manifestRoot, group, generation, incarnation, shuffleId)
      val listener = new ShuffleRecoveryManifestListener(conf, _ => true)
      val provider = ReferenceShuffleProvider.open(
        providerRoot,
        group,
        generation,
        incarnation,
        new SparkConf(false))

      val incompleteTaskId = 301L
      provider.createMapOutputWriter(incompleteTaskId, 1).commitAllPartitions(Array(0L))
      val incomplete = shuffleStageInfo(stageId = 10, shuffleId, numTasks = 2)
      listener.onStageSubmitted(SparkListenerStageSubmitted(incomplete))
      listener.onTaskEnd(successfulShuffleTask(10, 0, incompleteTaskId))
      listener.onStageCompleted(SparkListenerStageCompleted(incomplete))

      val failedTaskId = 401L
      provider.createMapOutputWriter(failedTaskId, 1).commitAllPartitions(Array(0L))
      val failed = shuffleStageInfo(stageId = 11, shuffleId, numTasks = 1)
      listener.onStageSubmitted(SparkListenerStageSubmitted(failed))
      listener.onTaskEnd(successfulShuffleTask(11, 0, failedTaskId))
      failed.failureReason = Some("injected stage failure")
      listener.onStageCompleted(SparkListenerStageCompleted(failed))
      listener.onApplicationEnd(SparkListenerApplicationEnd(System.currentTimeMillis()))

      val config = ShuffleRecoveryManifestPublisher.parseConfig(conf)
      assert(new ShuffleRecoveryManifestStore(manifestRoot)
        .findCompatible(
          group,
          config.identityFor(2),
          currentGeneration = generation + 1L)
        .isEmpty)
      assert(new ShuffleRecoveryManifestStore(manifestRoot)
        .findCompatible(
          group,
          config.identityFor(1),
          currentGeneration = generation + 1L)
        .isEmpty)
      assert(provider.committedMapCount == 0)
    }
  }

  private def listenerConf(
      providerRoot: Path,
      manifestRoot: Path,
      group: String,
      generation: Long,
      incarnation: String,
      shuffleId: Int): SparkConf = {
    new SparkConf(false)
      .set("spark.shuffle.recovery.phase0.manifest.enabled", "true")
      .set("spark.shuffle.recovery.phase0.provider.root", providerRoot.toString)
      .set("spark.shuffle.recovery.phase0.manifest.root", manifestRoot.toString)
      .set("spark.shuffle.recovery.phase0.group", group)
      .set("spark.shuffle.recovery.phase0.generation", generation.toString)
      .set("spark.shuffle.recovery.phase0.incarnation", incarnation)
      .set("spark.shuffle.recovery.phase0.identity.sourceToken", "source-v1")
      .set("spark.shuffle.recovery.phase0.identity.producerTag", "hash-aggregate-v1")
      .set("spark.shuffle.recovery.phase0.identity.rowEncoding", "unsafe-row-v1")
      .set("spark.shuffle.recovery.phase0.identity.partitioning", "hash-v1")
      .set("spark.shuffle.recovery.phase0.identity.reducerCount", "1")
      .set("spark.shuffle.recovery.phase0.identity.resolvedLiteral", "literal=7")
      .set("spark.shuffle.recovery.phase0.publisher.queueCapacity", "4")
      .set("spark.shuffle.recovery.phase0.targetShuffleId", shuffleId.toString)
  }

  private def shuffleStageInfo(stageId: Int, shuffleId: Int, numTasks: Int): StageInfo = {
    new StageInfo(
      stageId,
      0,
      s"shuffle-stage-$stageId",
      numTasks,
      Seq.empty,
      Seq.empty,
      "shuffle recovery listener test",
      null,
      Seq.empty,
      Some(shuffleId),
      resourceProfileId = 0)
  }

  private def successfulShuffleTask(
      stageId: Int,
      partitionId: Int,
      taskId: Long): SparkListenerTaskEnd = {
    val taskInfo = new TaskInfo(
      taskId,
      partitionId,
      attemptNumber = 0,
      partitionId,
      launchTime = 1L,
      executorId = "executor",
      host = "localhost",
      TaskLocality.ANY,
      speculative = false)
    SparkListenerTaskEnd(
      stageId,
      stageAttemptId = 0,
      taskType = "ShuffleMapTask",
      reason = Success,
      taskInfo,
      taskExecutorMetrics = null,
      taskMetrics = null)
  }
}
