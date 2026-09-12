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

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import org.apache.spark.{MapOutputTrackerMaster, SparkContext}
import org.apache.spark.shuffle.{ShuffleRecoveryManifest, ShuffleRecoveryManifestStore}
import org.apache.spark.shuffle.{ShuffleRecoveryNativePublicationContext, ShuffleRecoveryNativePublicationListener}

/** Install before materializing the exchange whose canonical identity was certified by SQL. */
private[spark] object ShuffleRecoveryCelebornPublication {
  def attach(
      sc: SparkContext,
      context: ShuffleRecoveryNativePublicationContext,
      manifestRoot: Path,
      handoffTtlMillis: Long): ShuffleRecoveryCelebornPublicationSession = {
    require(sc != null && !sc.isStopped && context != null)
    require(context.generation < Long.MaxValue)
    val manager = sc.env.shuffleManager match {
      case value: ShuffleRecoveryCelebornManager => value.delegate
      case value: SparkShuffleManager => value
      case _ => throw new IllegalArgumentException("active shuffle manager is not Celeborn")
    }
    val tracker = sc.env.mapOutputTracker match {
      case value: MapOutputTrackerMaster => value
      case _ => throw new IllegalArgumentException("publication must attach to the driver")
    }
    val conf = SparkUtils.fromSparkConf(sc.getConf)
    val provider = new ShuffleRecoveryCelebornPublicationProvider(manager, conf, handoffTtlMillis)
    val store = new ShuffleRecoveryManifestStore(manifestRoot)
    val listener = new ShuffleRecoveryNativePublicationListener(context, provider, store, tracker)
    try sc.addSparkListener(listener) catch {
      case NonFatal(error) =>
        listener.close()
        throw error
    }
    new ShuffleRecoveryCelebornPublicationSession(sc, context, store, listener)
  }
}

/** Call finish on the driver after the selected map-stage future/action has completed. */
private[spark] final class ShuffleRecoveryCelebornPublicationSession private[celeborn] (
    sc: SparkContext,
    context: ShuffleRecoveryNativePublicationContext,
    store: ShuffleRecoveryManifestStore,
    listener: ShuffleRecoveryNativePublicationListener) extends AutoCloseable {
  private val closed = new AtomicBoolean(false)

  def finish(): ShuffleRecoveryManifest = {
    close()
    store.findCompatible(context.recoveryGroup, context.identity, context.generation + 1L)
      .filter(value => value.generation == context.generation &&
        value.incarnationId == context.incarnationId && value.nativeDescriptor.isDefined)
      .getOrElse(throw new IllegalStateException("native shuffle publication did not complete"))
  }

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      try sc.listenerBus.waitUntilEmpty(30000L)
      finally {
        sc.removeSparkListener(listener)
        listener.close()
      }
    }
  }
}
