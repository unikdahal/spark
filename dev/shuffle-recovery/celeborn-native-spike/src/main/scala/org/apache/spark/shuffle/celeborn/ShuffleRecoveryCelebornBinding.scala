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

import java.util.concurrent.{Executors, ScheduledFuture, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.util.control.NonFatal

import org.apache.spark.rpc.RpcAddress
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.storage.BlockManagerId

import org.apache.celeborn.client.StandaloneRetainedShuffleReader

private[celeborn] final case class ShuffleRecoveryCelebornBinding(
    descriptor: Vector[Byte],
    location: BlockManagerId,
    driverAddress: RpcAddress,
    driverEndpointName: String,
    mapperCount: Int,
    reducerCount: Int,
    ttlMillis: Long) extends Serializable

private[celeborn] final case class ShuffleRecoveryCelebornBindingCurrent(
    shuffleId: Int,
    location: BlockManagerId) extends Serializable

private[celeborn] final class ShuffleRecoveryCelebornHandle[K, V, C](
    val underlying: CelebornShuffleHandle[K, V, C]) extends ShuffleHandle(underlying.shuffleId) {
  private val binding = new AtomicReference[ShuffleRecoveryCelebornBinding]()
  def current: Option[ShuffleRecoveryCelebornBinding] = Option(binding.get())
  def install(value: ShuffleRecoveryCelebornBinding): Boolean = binding.compareAndSet(null, value)
  def invalidate(value: ShuffleRecoveryCelebornBinding): Unit = binding.compareAndSet(value, null)
}

/** Renewal and driver fencing run away from fetch threads; liveness checks never perform RPCs. */
private[celeborn] final class ShuffleRecoveryCelebornLease(
    val reader: StandaloneRetainedShuffleReader,
    ttlMillis: Long,
    bindingCurrent: () => Boolean) extends AutoCloseable {
  require(reader != null && ttlMillis >= 3000L && ttlMillis <= 3600000L)
  private val active = new AtomicBoolean(true)
  private val closed = new AtomicBoolean(false)
  private val executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
    override def newThread(task: Runnable): Thread = {
      val thread = new Thread(task, "celeborn-recovery-lease-renewal")
      thread.setDaemon(true)
      thread
    }
  })
  private val renewal: ScheduledFuture[_] = executor.scheduleWithFixedDelay(new Runnable {
    override def run(): Unit = {
      if (active.get()) {
        try {
          if (!bindingCurrent() || !reader.renew()) active.set(false)
        } catch {
          case NonFatal(_) => active.set(false)
        }
      }
    }
  }, ttlMillis / 3L, ttlMillis / 3L, TimeUnit.MILLISECONDS)

  def isCurrent: Boolean = active.get() && reader.isCurrent

  def invalidate(): Unit = {
    active.set(false)
    renewal.cancel(false)
    executor.shutdown()
  }

  override def close(): Unit = {
    invalidate()
    if (closed.compareAndSet(false, true)) reader.close()
  }
}
