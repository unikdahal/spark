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
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.{ShuffleRecoverySchedulerAdoptionState, SparkConf}
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.storage.{BlockId, ShuffleBlockBatchId, ShuffleBlockId}
import org.apache.spark.util.collection.OpenHashSet

private[spark] final case class ShuffleRecoveryReadMetrics(
    blockReads: Long,
    nonEmptyBlockReads: Long,
    emptyBlockReads: Long,
    bytesRead: Long)

/**
 * Feasibility-only read indirection for an adopted reference-provider shuffle.
 *
 * The scheduler installs only an immutable current-shuffle-id binding. Provider access remains on
 * the ordinary shuffle fetch thread through [[getBlockData]], never on the DAGScheduler event
 * loop. A production provider integration will need an executor-visible compact descriptor rather
 * than this local reference-provider binding.
 */
private[spark] final class ShuffleRecoveryIndexShuffleBlockResolver(
    conf: SparkConf,
    taskIdMapsForShuffle: ConcurrentMap[Int, OpenHashSet[Long]])
  extends IndexShuffleBlockResolver(conf, null, taskIdMapsForShuffle) {

  private final class RecoveredReadBinding(
      val provider: ReferenceShuffleRecoveryClaimProvider,
      val binding: ShuffleRecoveryBinding,
      val mapperCount: Int,
      val reducerCount: Int)

  private final class RecoveredReadCounters {
    val blockReads = new AtomicLong(0L)
    val nonEmptyBlockReads = new AtomicLong(0L)
    val emptyBlockReads = new AtomicLong(0L)
    val bytesRead = new AtomicLong(0L)
  }

  private val recoveredBindings = new ConcurrentHashMap[Int, RecoveredReadBinding]()
  private val recoveredReadCounters = new ConcurrentHashMap[Int, RecoveredReadCounters]()

  private[spark] val schedulerAdoption = new ShuffleRecoverySchedulerAdoptionState(this)

  private[spark] def installRecoveredBinding(
      targetShuffleId: Int,
      provider: ReferenceShuffleRecoveryClaimProvider,
      binding: ShuffleRecoveryBinding,
      mapperCount: Int,
      reducerCount: Int): Boolean = {
    if (targetShuffleId < 0 || provider == null || binding == null ||
        binding.targetShuffleId != targetShuffleId || mapperCount < 0 || reducerCount <= 0) {
      false
    } else {
      val candidate = new RecoveredReadBinding(provider, binding, mapperCount, reducerCount)
      val existing = recoveredBindings.putIfAbsent(targetShuffleId, candidate)
      val installed = existing == null ||
        ((existing.provider eq provider) &&
          existing.binding == binding &&
          existing.mapperCount == mapperCount &&
          existing.reducerCount == reducerCount)
      if (installed) {
        recoveredReadCounters.putIfAbsent(targetShuffleId, new RecoveredReadCounters)
      }
      installed
    }
  }

  private[spark] def removeRecoveredBinding(
      targetShuffleId: Int,
      binding: ShuffleRecoveryBinding): Unit = {
    if (binding != null) {
      val existing = recoveredBindings.get(targetShuffleId)
      if (existing != null && existing.binding == binding &&
          recoveredBindings.remove(targetShuffleId, existing)) {
        recoveredReadCounters.remove(targetShuffleId)
      }
    }
  }

  private[spark] def isRecovered(targetShuffleId: Int): Boolean =
    recoveredBindings.containsKey(targetShuffleId)

  private[spark] def recoveredReadMetrics(targetShuffleId: Int): ShuffleRecoveryReadMetrics = {
    val counters = recoveredReadCounters.get(targetShuffleId)
    if (counters == null) {
      ShuffleRecoveryReadMetrics(0L, 0L, 0L, 0L)
    } else {
      ShuffleRecoveryReadMetrics(
        counters.blockReads.get(),
        counters.nonEmptyBlockReads.get(),
        counters.emptyBlockReads.get(),
        counters.bytesRead.get())
    }
  }

  private[spark] def openBoundMapForPreparation(
      provider: ReferenceShuffleRecoveryClaimProvider,
      binding: ShuffleRecoveryBinding,
      mapIndex: Int): ReferenceShuffleResolvedMap = {
    provider.openBoundMap(binding, mapIndex)
  }

  override def getBlockData(
      blockId: BlockId,
      dirs: Option[Array[String]]): ManagedBuffer = {
    blockId match {
      case id: ShuffleBlockId =>
        val recovered = recoveredBindings.get(id.shuffleId)
        if (recovered == null) {
          super.getBlockData(blockId, dirs)
        } else {
          if (id.mapId < 0L || id.mapId > Int.MaxValue.toLong) {
            throw new IOException("recovered shuffle map id is outside the supported range")
          }
          val mapIndex = id.mapId.toInt
          if (mapIndex >= recovered.mapperCount ||
              id.reduceId < 0 || id.reduceId >= recovered.reducerCount) {
            throw new IOException("recovered shuffle block coordinates are outside the binding")
          }
          val resolved = recovered.provider.openBoundMap(recovered.binding, mapIndex)
          val metadata = resolved.blockMetadata(id.reduceId)
          val counters = recoveredReadCounters.get(id.shuffleId)
          if (counters != null) {
            counters.blockReads.incrementAndGet()
            if (metadata.isEmpty) {
              counters.emptyBlockReads.incrementAndGet()
            } else {
              counters.nonEmptyBlockReads.incrementAndGet()
              counters.bytesRead.addAndGet(metadata.length)
            }
          }
          resolved.getBlockData(id.reduceId).getOrElse {
            new NioManagedBuffer(ByteBuffer.allocate(0))
          }
        }

      case batch: ShuffleBlockBatchId if recoveredBindings.containsKey(batch.shuffleId) =>
        // The Phase 0 reference binding intentionally disables batch fetch so every provider read
        // retains exact reducer addressing. A scalable batched representation is a later design.
        throw new IOException("batch fetch is disabled for an adopted reference shuffle")

      case _ =>
        super.getBlockData(blockId, dirs)
    }
  }
}
