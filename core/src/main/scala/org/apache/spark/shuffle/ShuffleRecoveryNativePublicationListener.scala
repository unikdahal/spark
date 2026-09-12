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

import scala.util.control.NonFatal

import org.apache.spark.{MapOutputTrackerMaster, Success}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.scheduler.{SparkListenerStageCompleted, SparkListenerStageSubmitted}
import org.apache.spark.scheduler.SparkListenerTaskEnd

/** Attach before executing the certified exchange; provider work runs on the bounded worker. */
private[spark] final class ShuffleRecoveryNativePublicationListener(
    context: ShuffleRecoveryNativePublicationContext,
    provider: ShuffleRecoveryNativePublicationProvider,
    store: ShuffleRecoveryManifestStore,
    tracker: MapOutputTrackerMaster,
    queueCapacity: Int = 2) extends SparkListener with AutoCloseable with Logging {
  require(tracker != null)
  private val selectionMatches: ShuffleRecoveryPublication => Boolean = publication =>
    tracker.matchesMapOutputSelection(publication.shuffleId, publication.winningMapTaskIds)
  private val publisher = new ShuffleRecoveryManifestPublisher(
    new ShuffleRecoveryNativePublicationBackend(context, provider, store, selectionMatches,
      publication => tracker.captureNativeMapOutputs(publication.shuffleId,
        publication.winningMapTaskIds, publication.reducerCount)),
    queueCapacity)
  private val coordinator = new ShuffleRecoveryPublicationCoordinator(publisher,
    context.identity.reducerCount, Some(context.targetShuffleId), selectionMatches)

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = safely {
    val info = event.stageInfo
    if (info.shuffleDepId.contains(context.targetShuffleId)) {
      coordinator.stageSubmitted(info.stageId, info.attemptNumber(), context.targetShuffleId,
        context.identity.mapperCount)
    }
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = safely {
    if (event.taskType == "ShuffleMapTask" && event.reason == Success) {
      coordinator.taskSucceeded(event.stageId, event.stageAttemptId, event.taskInfo.partitionId,
        event.taskInfo.taskId, event.taskInfo.attemptNumber)
    }
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = safely {
    val info = event.stageInfo
    if (info.shuffleDepId.contains(context.targetShuffleId)) {
      coordinator.stageCompleted(info.stageId, info.attemptNumber(), info.failureReason.isEmpty)
    }
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = close()

  override def close(): Unit = {
    coordinator.clear()
    publisher.close()
  }

  private def safely(action: => Unit): Unit = {
    try action catch {
      case NonFatal(error) => logWarning("Skipping native shuffle recovery publication", error)
    }
  }
}
