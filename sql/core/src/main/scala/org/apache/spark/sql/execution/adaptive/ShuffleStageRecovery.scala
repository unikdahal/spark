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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.{MapOutputStatistics, ShuffleDependency}
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.shuffle.{RecoveredShuffleOutput, ShuffleStageRecoveryHandler}
import org.apache.spark.sql.catalyst.analysis.{RecoveryAnchorResolver, SourceRecoveryInfo}
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.metric.SQLShuffleWriteMetricsReporter

/** Immutable information identifying a shuffle stage considered for recovery. */
@DeveloperApi
@Experimental
case class ShuffleStageRecoveryInfo(
    stageId: Int,
    shuffleId: Int,
    numMappers: Int,
    numPartitions: Int,
    plan: SparkPlan,
    canonicalizedPlan: SparkPlan,
    canonicalizedQueryPlan: SparkPlan)

/**
 * Runtime statistics for a shuffle whose data was recovered by an external shuffle service.
 * The provider must make the shuffle readable under `ShuffleStageRecoveryInfo.shuffleId`
 * atomically before returning this value.
 */
@DeveloperApi
@Experimental
case class RecoveredShuffleStage(
    bytesByPartitionId: Seq[Long],
    dataSize: Long,
    rowCount: Option[Long])

/**
 * Session-scoped extension for recovering completed shuffle stages after a driver restart.
 *
 * Implementations must fail closed: `None` is an authoritative statement that no committed
 * recovery exists and permits Spark to materialize the stage normally. An unavailable provider,
 * an indeterminate lookup, corrupt metadata, or a failed adoption must throw; Spark fails the
 * query attempt rather than risk mixing an existing committed generation with recomputation.
 * This method runs synchronously before Spark submits any map task. A provider that wants to
 * recover a stage even if the driver dies immediately after external shuffle commit must register
 * its semantic completion intent before returning `None`; a post-completion callback alone leaves
 * an unsafe crash window. Such an intent must be idempotent and must not make an incomplete
 * shuffle readable.
 *
 * A returned recovery must already be readable through the active shuffle manager. Spark
 * validates the statistics and installs scheduler state only after this method returns
 * successfully. Providers may use `canonicalizedPlan` together with their durable execution ID
 * to derive a restart-stable key. The key should include `canonicalizedQueryPlan`, so a changed
 * query cannot recover only the structurally unchanged stages of an older execution and mix them
 * with newly computed stages. `stageId` and `shuffleId` are local to the current driver. A durable
 * execution ID must identify one immutable logical execution and must not be reused for a later
 * independent run. Providers must tolerate concurrent calls from independently executing plans.
 */
@DeveloperApi
@Experimental
trait ShuffleStageRecovery extends RecoveryAnchorResolver {

  /**
   * Durably resolve the input version for this execution. The first driver must atomically store
   * `currentAnchor`; replacement drivers must return that stored value. An unavailable or
   * conflicting lookup must throw.
   */
  def resolveSourceAnchor(info: SourceRecoveryInfo): String = {
    throw new UnsupportedOperationException(
      s"Recovery provider does not support source anchors for ${info.sourceId}")
  }

  def tryRecover(info: ShuffleStageRecoveryInfo): Option[RecoveredShuffleStage]

  /**
   * Roll back driver-local state from a recovery attempt that threw or that Spark could not
   * install. This method must be idempotent, must also accept an attempt that did not publish any
   * external state, and must not delete the authoritative committed recovery.
   */
  def abortRecovery(info: ShuffleStageRecoveryInfo, cause: Throwable): Unit = {}

  /**
   * Called after a stage materialized normally, once Spark has installed its result. This callback
   * may persist runtime statistics or mark an intent complete, but must not be the only record by
   * which already-committed external shuffle data can be discovered after a crash.
   */
  def onStageCompleted(
      info: ShuffleStageRecoveryInfo,
      result: RecoveredShuffleStage): Unit = {}
}

private[sql] object ShuffleStageRecovery {

  def install(
      exchange: ShuffleExchangeLike,
      dependency: ShuffleDependency[_, _, _],
      info: ShuffleStageRecoveryInfo,
      provider: ShuffleStageRecovery,
      onRecovered: Statistics => Unit = _ => ()): Unit = {
    if (dependency.stageRecoveryHandler.nonEmpty) {
      return
    }
    dependency.setStageRecoveryHandler(new ShuffleStageRecoveryHandler {
      override def tryRecover(
          shuffleId: Int,
          numMappers: Int,
          numPartitions: Int): Option[RecoveredShuffleOutput] = {
        provider.tryRecover(info).map { recovered =>
          recovered.rowCount.foreach { rowCount =>
            exchange.metrics(
              SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN).set(rowCount)
          }
          exchange.metrics("dataSize").set(recovered.dataSize)
          val runtimeStatistics =
            Statistics(recovered.dataSize, recovered.rowCount.map(BigInt(_)))
          onRecovered(runtimeStatistics)
          RecoveredShuffleOutput(
            recovered.bytesByPartitionId.toArray, recovered.dataSize, recovered.rowCount)
        }
      }

      override def onStageCompleted(
          shuffleId: Int,
          statistics: MapOutputStatistics): Unit = {
        val runtimeStatistics = exchange.runtimeStatistics
        provider.onStageCompleted(
          info,
          RecoveredShuffleStage(
            statistics.bytesByPartitionId.clone().toIndexedSeq,
            runtimeStatistics.sizeInBytes.toLong,
            runtimeStatistics.rowCount.map(_.toLong)))
      }

      override def abortRecovery(shuffleId: Int, cause: Throwable): Unit = {
        provider.abortRecovery(info, cause)
      }
    })
  }
}
