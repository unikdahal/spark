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

/** Scheduling estimates from accepted outputs, distinct from physical stream offsets. */
private[spark] final case class ShuffleRecoveryNativeMapOutput(
    mapTaskId: Long,
    reducerBytes: Vector[Long])

private[spark] object ShuffleRecoveryNativeMapOutput {
  // Bound the dense scheduling metadata independently of native provider descriptor size.
  val MaxCells = 131072
}

/** Providers translate accepted attempt coordinates into their native encoding outside Core. */
private[spark] trait ShuffleRecoveryNativePublicationProvider {
  def compatibilityId: String
  def seal(
      shuffleId: Int,
      acceptedAttempts: Vector[ShuffleRecoveryMapAttempt]): Vector[Byte]
}

/** One certified exchange's immutable publication namespace and semantic identity. */
private[spark] final case class ShuffleRecoveryNativePublicationContext(
    recoveryGroup: String,
    generation: Long,
    incarnationId: String,
    targetShuffleId: Int,
    identity: ShuffleRecoveryCanonicalManifestIdentity)

/** Runs on the existing bounded publisher worker, never on a scheduler/listener callback. */
private[spark] final class ShuffleRecoveryNativePublicationBackend(
    context: ShuffleRecoveryNativePublicationContext,
    provider: ShuffleRecoveryNativePublicationProvider,
    store: ShuffleRecoveryManifestStore,
    currentSelection: ShuffleRecoveryPublication => Boolean,
    captureOutputs: ShuffleRecoveryPublication => Option[Vector[ShuffleRecoveryNativeMapOutput]])
  extends ShuffleRecoveryPublicationBackend {
  require(context != null && provider != null && store != null &&
    currentSelection != null && captureOutputs != null)
  ShuffleRecoveryManifestCodec.validateIdentifier(context.recoveryGroup, "recovery group")
  ShuffleRecoveryManifestCodec.validateIdentifier(context.incarnationId, "incarnation id")
  require(context.generation > 0L && context.targetShuffleId >= 0)
  ShuffleRecoveryManifestCodec.validateIdentity(context.identity)
  require(context.identity.providerCompatibilityId == provider.compatibilityId,
    "publication provider does not match the certified read format")

  override def publish(publication: ShuffleRecoveryPublication): Unit = {
    require(publication != null && publication.shuffleId == context.targetShuffleId,
      "publication does not belong to the certified exchange")
    val ids = publication.winningMapTaskIds
    val attempts = publication.winningMapAttempts
    require(ids != null && attempts != null && ids.nonEmpty &&
      ids.size == context.identity.mapperCount && attempts.size == ids.size &&
      publication.reducerCount == context.identity.reducerCount &&
      ids.forall(_ >= 0L) && ids.distinct.size == ids.size,
      "native publication requires a complete accepted mapper selection")
    require(attempts.zip(ids).forall { case (attempt, id) =>
      attempt != null && attempt.taskId == id && attempt.stageAttemptId >= 0 &&
        attempt.taskAttemptNumber >= 0
    }, "native publication attempt coordinates do not match selected tasks")
    require(currentSelection(publication), "mapper selection changed before native sealing")
    val descriptor = provider.seal(publication.shuffleId, attempts)
    require(descriptor != null && descriptor.nonEmpty &&
      descriptor.size <= ShuffleRecoveryManifestCodec.MaxNativeDescriptorBytes,
      "native publication returned an invalid descriptor")
    require(currentSelection(publication), "mapper selection changed during native sealing")
    val outputs = captureOutputs(publication)
      .getOrElse(throw new IllegalStateException("accepted native output statistics unavailable"))
    require(outputs.map(_.mapTaskId) == ids, "native output statistics changed mapper selection")
    val manifest = ShuffleRecoveryManifest(context.recoveryGroup, context.generation,
      context.incarnationId, context.identity, context.identity.mapperCount,
      context.identity.reducerCount, Vector.empty, ShuffleRecoveryManifest.DescriptorVersion,
      None, System.currentTimeMillis(), Some(descriptor), Some(outputs))
    store.publish(manifest)
  }
}
