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

import org.apache.spark.{ShuffleRecoverySchedulerAdoptionState, SparkConf}
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.storage.{BlockId, ShuffleBlockBatchId, ShuffleBlockId}
import org.apache.spark.util.collection.OpenHashSet

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

  private final case class RecoveredReadBinding(
      provider: ReferenceShuffleRecoveryClaimProvider,
      binding: ShuffleRecoveryBinding,
      mapperCount: Int,
      reducerCount: Int)

  private val recoveredBindings = new ConcurrentHashMap[Int, RecoveredReadBinding]()

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
      val candidate = RecoveredReadBinding(provider, binding, mapperCount, reducerCount)
      val existing = recoveredBindings.putIfAbsent(targetShuffleId, candidate)
      existing == null || existing == candidate
    }
  }

  private[spark] def removeRecoveredBinding(
      targetShuffleId: Int,
      binding: ShuffleRecoveryBinding): Unit = {
    if (binding != null) {
      val existing = recoveredBindings.get(targetShuffleId)
      if (existing != null && existing.binding == binding) {
        recoveredBindings.remove(targetShuffleId, existing)
      }
    }
  }

  private[spark] def isRecovered(targetShuffleId: Int): Boolean =
    recoveredBindings.containsKey(targetShuffleId)

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
