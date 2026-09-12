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

package org.apache.spark.shuffle.celeborn

import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import org.apache.spark.{InterruptibleIterator, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReader, ShuffleReadMetricsReporter}

import org.apache.celeborn.client.StandaloneRetainedShuffleReader
import org.apache.celeborn.client.read.MetricsCallback
import org.apache.celeborn.common.CelebornConf

/** Native SQL row reads; admission rejects RDD aggregation and key-ordering requirements. */
private[celeborn] final class ShuffleRecoveryCelebornReader[K, C](
    handle: ShuffleRecoveryCelebornHandle[K, _, C],
    binding: ShuffleRecoveryCelebornBinding,
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    conf: CelebornConf,
    metrics: ShuffleReadMetricsReporter) extends ShuffleReader[K, C] with Logging {

  override def read(): Iterator[Product2[K, C]] = {
    var currentPartition = startPartition
    def failed(error: Throwable): Nothing = {
      logWarning("Native retained reader failed before or during record consumption", error)
      throw new FetchFailedException(binding.location, handle.shuffleId, -1L, -1,
        currentPartition, "native retained shuffle read failed", error)
    }
    def protect[A](action: => A): A = {
      try action catch {
        case error: FetchFailedException => throw error
        case NonFatal(error) if !context.isInterrupted() => failed(error)
      }
    }
    val lastMap = math.min(endMapIndex, binding.mapperCount)
    protect {
      require(startMapIndex >= 0 && lastMap >= startMapIndex &&
        startPartition >= 0 && endPartition >= startPartition &&
        endPartition <= binding.reducerCount)
      require(handle.underlying.dependency.aggregator.isEmpty &&
        handle.underlying.dependency.keyOrdering.isEmpty &&
        !handle.underlying.dependency.mapSideCombine)
    }
    // Shuffle handles are deserialized by the task serializer, outside RpcEnv.deserialize.
    // Carry only an address/name and construct the endpoint reference in this executor's env.
    val driver = protect {
      SparkEnv.get.rpcEnv.setupEndpointRef(binding.driverAddress, binding.driverEndpointName)
    }
    def driverCurrent(): Boolean = driver.askSync[Boolean](
      ShuffleRecoveryCelebornBindingCurrent(handle.shuffleId, binding.location))
    protect { require(driverCurrent(), "native binding is no longer installed on the driver") }
    val native = protect {
      new StandaloneRetainedShuffleReader(binding.descriptor.toArray, binding.ttlMillis, conf)
    }
    val lease = try {
      require(native.numMappers == binding.mapperCount &&
        native.numReducers == binding.reducerCount)
      new ShuffleRecoveryCelebornLease(native, binding.ttlMillis, () => driverCurrent())
    } catch {
      case NonFatal(error) =>
        try native.close() catch {
          case NonFatal(cleanupError) => error.addSuppressed(cleanupError)
        }
        failed(error)
    }
    val completed = new AtomicBoolean(false)
    def close(): Unit = {
      if (completed.compareAndSet(false, true)) {
        try lease.close() finally context.taskMetrics().mergeShuffleReadMetrics()
      }
    }
    context.addTaskCompletionListener[Unit](_ => close())
    val serializer = protect { handle.underlying.dependency.serializer.newInstance() }
    val callback = new MetricsCallback {
      override def incBytesRead(bytes: Long): Unit = metrics.incRemoteBytesRead(bytes)
      override def incReadTime(time: Long): Unit = metrics.incFetchWaitTime(time)
    }
    val records = (startPartition until endPartition).iterator.flatMap { partition =>
      currentPartition = partition
      protect {
        if (!lease.isCurrent) throw new IOException("native reader lease is fenced")
        val stream = native.openPartition(
          partition, SparkCommonUtils.getEncodedAttemptNumber(context),
          context.taskAttemptId(), startMapIndex, lastMap, callback)
        val deserialized = serializer.deserializeStream(stream)
        val pairs = deserialized.asKeyValueIterator
        new Iterator[Product2[K, C]] {
          private var exhausted = false
          override def hasNext: Boolean = protect {
            if (!exhausted && !lease.isCurrent) {
              throw new IOException("native reader lease is fenced")
            }
            val more = !exhausted && pairs.hasNext
            if (!more && !exhausted) {
              exhausted = true
              deserialized.close()
            }
            more
          }
          override def next(): Product2[K, C] = protect {
            if (!lease.isCurrent) throw new IOException("native reader lease is fenced")
            val pair = pairs.next()
            if (!lease.isCurrent) {
              throw new IOException("native read expired during deserialization")
            }
            metrics.incRecordsRead(1L)
            pair.asInstanceOf[Product2[K, C]]
          }
        }
      }
    }
    val closing = new Iterator[Product2[K, C]] {
      override def hasNext: Boolean = protect {
        val more = !completed.get() && records.hasNext
        if (!more) close()
        more
      }
      override def next(): Product2[K, C] = {
        if (!hasNext) throw new NoSuchElementException("native shuffle reader exhausted")
        protect { records.next() }
      }
    }
    new InterruptibleIterator(context, closing)
  }
}
