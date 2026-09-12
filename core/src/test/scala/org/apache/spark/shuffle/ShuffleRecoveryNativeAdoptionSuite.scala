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

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.spark._
import org.apache.spark.storage.BlockManagerId

class ShuffleRecoveryNativeAdoptionSuite extends SparkFunSuite with LocalSparkContext {
  private final class Installation extends ShuffleRecoveryNativeInstallation {
    override val compatibilityId = ShuffleRecoveryFeasibilityIdentity.ProviderCompatibilityId
    override val descriptor = Vector[Byte](1, 2, 3)
    override val location = BlockManagerId("native-generation", "localhost", 19000)
    var installed = false
    var invalidated = false
    val released = new CountDownLatch(1)
    override def isCurrent: Boolean = !invalidated
    override def install(): Boolean = { installed = true; true }
    override def invalidate(): Unit = { invalidated = true; installed = false }
    override def close(): Unit = { released.countDown() }
  }

  private final class Harness extends AutoCloseable {
    sc = new SparkContext(new SparkConf().setMaster("local[1]")
      .setAppName("native-adoption-test").set("spark.ui.enabled", "false"))
    val dependency = new ShuffleDependency[Int, Int, Int](
      sc.parallelize(Seq((1, 1)), 1), new HashPartitioner(1))
    val tracker = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    tracker.registerShuffle(dependency.shuffleId, 1, 1)
    val target = ShuffleRecoveryAdoptionTarget(ShuffleRecoveryMaterializationId(1L, 1L),
      dependency.shuffleId, 1L, 1, 1)
    val inputs = ShuffleRecoveryFeasibilityInputs("source", "producer", "rows", "single-v1", "1")
    val request = ShuffleRecoveryPreparationRequest("group", 2L, target, inputs)
    val reservations = new ShuffleRecoveryReservationManager
    val reservation = reservations.reserve(target).toOption.get
    val state = new ShuffleRecoveryNativeAdoption
    val installation = new Installation
    val manifest = ShuffleRecoveryManifest("group", 1L, "incarnation", inputs.identityFor(target),
      1, 1, Vector.empty, ShuffleRecoveryManifest.DescriptorVersion, None, 1L,
      Some(installation.descriptor), Some(Vector(ShuffleRecoveryNativeMapOutput(11L, Vector(7L)))))
    assert(state.registerReservation(request, reservations, reservation, dependency))
    override def close(): Unit = state.close()
  }

  test("ordinary execution fences a native preparation that arrives late") {
    val h = new Harness
    try {
      assert(!h.state.beforeFindMissingPartitions(h.tracker, h.dependency, 1))
      assert(!h.state.offerPrepared(h.reservation, h.manifest, h.installation))
      assert(!h.installation.installed)
      assert(h.installation.released.await(10, TimeUnit.SECONDS))
      assert(h.tracker.getNumAvailableOutputs(h.dependency.shuffleId) == 0)
    } finally h.close()
  }

  test("native failure clears the whole adopted shuffle and requests rollback once") {
    val h = new Harness
    try {
      assert(h.state.offerPrepared(h.reservation, h.manifest, h.installation))
      assert(h.state.beforeFindMissingPartitions(h.tracker, h.dependency, 1))
      assert(h.installation.installed)
      assert(h.tracker.getNumAvailableOutputs(h.dependency.shuffleId) == 1)
      val stale = BlockManagerId("older-generation", "localhost", 19000)
      assert(h.state.handleFetchFailure(h.tracker, h.dependency, stale, h.tracker.getEpoch) ==
        ShuffleRecoveryFetchFailureStale)
      assert(h.state.isAdopted(h.dependency.shuffleId))
      assert(h.state.handleFetchFailure(h.tracker, h.dependency, h.installation.location,
        h.tracker.getEpoch).isInstanceOf[ShuffleRecoveryFetchFailureInvalidated])
      assert(h.tracker.getNumAvailableOutputs(h.dependency.shuffleId) == 0)
      assert(h.installation.invalidated)
      assert(h.state.consumeWholeStageRetryRequirement(h.dependency.shuffleId))
      assert(!h.state.consumeWholeStageRetryRequirement(h.dependency.shuffleId))
      assert(h.installation.released.await(10, TimeUnit.SECONDS))
    } finally h.close()
  }

  test("cancelled native preparation cannot install its descriptor") {
    val h = new Harness
    try {
      assert(h.state.offerPrepared(h.reservation, h.manifest, h.installation))
      h.state.cancel(h.target.materializationId, h.tracker)
      assert(!h.state.beforeFindMissingPartitions(h.tracker, h.dependency, 1))
      assert(!h.installation.installed)
      assert(h.installation.released.await(10, TimeUnit.SECONDS))
    } finally h.close()
  }
}
