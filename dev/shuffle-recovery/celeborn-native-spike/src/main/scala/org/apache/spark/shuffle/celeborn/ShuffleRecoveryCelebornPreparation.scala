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

import org.apache.spark.SparkContext
import org.apache.spark.shuffle._

/** Invoke from the driver preparation worker before submitting the replacement exchange. */
private[spark] object ShuffleRecoveryCelebornPreparation {
  def prepare(
      sc: SparkContext,
      request: ShuffleRecoveryPreparationRequest,
      manifestRoot: Path,
      ttlMillis: Long): Either[String, ShuffleRecoveryCelebornAdoptionSession] = {
    require(sc != null && !sc.isStopped && request != null && request.target != null)
    val manager = sc.env.shuffleManager match {
      case value: ShuffleRecoveryCelebornManager => value
      case _ => return Left("active shuffle manager does not support native recovery")
    }
    val reservations = new ShuffleRecoveryReservationManager
    var offered = false
    var registered = false
    try {
      val reservation = reservations.reserve(request.target) match {
        case Right(value) => value
        case Left(reason) => return Left(reason)
      }
      if (!manager.registerNativePreparation(request, reservations, reservation)) {
        return Left("native recovery reservation lost to ordinary execution")
      }
      registered = true
      val identity = request.identityInputs.identityFor(request.target)
      require(identity.isInstanceOf[ShuffleRecoveryCanonicalManifestIdentity],
        "native preparation requires the certified computation identity")
      val manifest = new ShuffleRecoveryManifestStore(manifestRoot)
        .findCompatible(request.recoveryGroup, identity, request.currentGeneration) match {
        case Some(value) if value.nativeDescriptor.isDefined && value.nativeMapOutputs.isDefined =>
          value
        case _ => return Left("no compatible native recovery manifest")
      }
      if (!manager.offerNative(request, reservation, manifest, ttlMillis)) {
        Left("native provider claim or local preparation was rejected")
      } else {
        offered = true
        Right(new ShuffleRecoveryCelebornAdoptionSession(manager, request, reservations))
      }
    } catch {
      case NonFatal(error) => Left("native recovery preparation failed: " + error.getClass.getName)
    } finally {
      if (!offered) {
        if (registered) manager.cancelNativePreparation(request.target.materializationId)
        reservations.shutdown()
      }
    }
  }
}

/** Retain until all consumers complete; close fences the binding and schedules provider cleanup. */
private[spark] final class ShuffleRecoveryCelebornAdoptionSession private[celeborn] (
    manager: ShuffleRecoveryCelebornManager,
    request: ShuffleRecoveryPreparationRequest,
    reservations: ShuffleRecoveryReservationManager) extends AutoCloseable {
  private val closed = new AtomicBoolean(false)
  def isAdopted: Boolean = !closed.get() &&
    manager.shuffleRecoverySchedulerBackend.isAdopted(request.target.targetShuffleId)

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      try manager.cancelNativePreparation(request.target.materializationId)
      finally reservations.shutdown()
    }
  }
}
