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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.{SparkConf, SparkFunSuite, TaskContext}
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleReadMetricsReporter, ShuffleReader,
  ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, LongType}

/**
 * Successful execution proof for an adopted non-empty shuffle.
 *
 * The production contract requires the provider to make adopted data readable through the active
 * shuffle manager before returning it. This test manager is that provider-side adapter: recovered
 * shuffle IDs read deterministic durable rows, while every non-recovered operation delegates to
 * Spark's sort shuffle manager. Any producer task would request a writer and fail the assertion.
 */
class ShuffleStageRecoveryExecutionSuite extends SparkFunSuite with AdaptiveSparkPlanHelper {

  test("non-empty AQE shuffle recovery executes coalesced consumers with zero producer tasks") {
    AQERecoveryTestShuffleState.reset()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    val providerLookups = new AtomicInteger()
    val configureExtensions: SparkSessionExtensions => Unit = { extensions =>
      extensions.injectShuffleStageRecovery { _ =>
        new ShuffleStageRecovery {
          override def protocolVersion: Int = ShuffleStageRecovery.PROTOCOL_VERSION

          override def tryRecover(
              info: ShuffleStageRecoveryInfo): Option[RecoveredShuffleStage] = {
            providerLookups.incrementAndGet()
            AQERecoveryTestShuffleState.adopt(info.shuffleId)
            val reducerBytes = Seq.tabulate(info.numPartitions) { partitionId =>
              if (partitionId == info.numPartitions / 2) 64L * 1024 * 1024 else 1L
            }
            Some(RecoveredShuffleStage(
              reducerBytes,
              reducerBytes.sum,
              Some(info.numPartitions.toLong),
              ShuffleStageRecovery.PROTOCOL_VERSION))
          }
        }
      }
    }

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(getClass.getSimpleName)
      .config("spark.ui.enabled", "false")
      .config("spark.shuffle.manager", classOf[AQERecoveryTestShuffleManager].getName)
      .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .config(SQLConf.COALESCE_PARTITIONS_ENABLED.key, "true")
      .config(SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST.key, "false")
      .config(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, (16L * 1024 * 1024).toString)
      .config(SQLConf.SKEW_JOIN_SKEWED_PARTITION_FACTOR.key, "2")
      .config(SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key, (16L * 1024 * 1024).toString)
      .config(SQLConf.SHUFFLE_PARTITIONS.key, "4")
      .withExtensions(configureExtensions)
      .getOrCreate()

    try {
      val result = spark.range(100)
        .selectExpr("id % 4 AS k")
        .groupBy("k")
        .count()
      val rows = result.collect().map(row => row.getLong(0) -> row.getLong(1)).toMap

      assert(rows === Map(0L -> 10L, 1L -> 10L, 2L -> 10L, 3L -> 10L))
      assert(providerLookups.get() === 1)
      assert(AQERecoveryTestShuffleState.readerCreations.get() > 0)
      assert(AQERecoveryTestShuffleState.writerCreations.get() === 0,
        "an adopted shuffle must submit no producer task that constructs a shuffle writer")

      val adaptive = result.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
      val recoveredExchanges = collect(adaptive.executedPlan) {
        case exchange: ShuffleExchangeExec
            if AQERecoveryTestShuffleState.isAdopted(exchange.shuffleId) => exchange
      }
      assert(recoveredExchanges.nonEmpty)
      val recoveredStages = collect(adaptive.executedPlan) {
        case stage: ShuffleQueryStageExec
            if AQERecoveryTestShuffleState.isAdopted(stage.shuffle.shuffleId) => stage
      }
      assert(recoveredStages.size === 1)
      val recoveredReducerBytes = recoveredStages.head.mapStats.get.bytesByPartitionId
      assert(recoveredReducerBytes.count(_ > 0L) === 4)
      // MapOutputTracker deliberately compresses block sizes, so the reconstructed value is an
      // upper approximation rather than the provider's byte-exact input. Preserve the property
      // AQE needs here: one reducer remains unambiguously large enough to drive skew decisions.
      val largestReducer = recoveredReducerBytes.max
      assert(largestReducer >= 64L * 1024 * 1024)
      assert(largestReducer > recoveredReducerBytes.sum - largestReducer)
      val reads = collect(adaptive.executedPlan) { case read: AQEShuffleReadExec => read }
      assert(reads.exists(_.hasCoalescedPartition),
        s"expected AQE to coalesce recovered reducer statistics, found $reads")
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      AQERecoveryTestShuffleState.reset()
    }
  }
}

private object AQERecoveryTestShuffleState {
  private val adoptedShuffleIds = ConcurrentHashMap.newKeySet[Int]()
  val readerCreations = new AtomicInteger()
  val writerCreations = new AtomicInteger()

  def reset(): Unit = {
    adoptedShuffleIds.clear()
    readerCreations.set(0)
    writerCreations.set(0)
  }

  def adopt(shuffleId: Int): Unit = adoptedShuffleIds.add(shuffleId)

  def isAdopted(shuffleId: Int): Boolean = adoptedShuffleIds.contains(shuffleId)

  def recoveredRows(startPartition: Int, endPartition: Int): Iterator[InternalRow] = {
    val projection = UnsafeProjection.create(Array[DataType](LongType, LongType))
    (startPartition until endPartition).iterator.map { partitionId =>
      projection(new GenericInternalRow(Array[Any](partitionId.toLong, 10L))).copy()
    }
  }
}

/** SparkEnv constructs the configured test shuffle manager reflectively. */
private class AQERecoveryTestShuffleManager(conf: SparkConf, _isDriver: Boolean)
  extends SortShuffleManager(conf) {

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    AQERecoveryTestShuffleState.writerCreations.incrementAndGet()
    super.getWriter(handle, mapId, context, metrics)
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    if (AQERecoveryTestShuffleState.isAdopted(handle.shuffleId)) {
      AQERecoveryTestShuffleState.readerCreations.incrementAndGet()
      new ShuffleReader[K, C] {
        override def read(): Iterator[Product2[K, C]] = {
          AQERecoveryTestShuffleState.recoveredRows(startPartition, endPartition).map { row =>
            (0.asInstanceOf[K], row.asInstanceOf[C])
          }
        }
      }
    } else {
      super.getReader(
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics)
    }
  }
}
