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

/**
 * Provider-side evidence associated with an adopted read failure.
 *
 * A failed reduce fetch is not, by itself, authority to mutate durable recovery state. Only
 * evidence obtained while examining the exact immutable provider incarnation can grant that
 * authority. Transient or ambiguous failures remain non-destructive.
 */
private[spark] sealed trait ShuffleRecoveryAdoptedReadFailureClass {
  def authorizesRetirement: Boolean
}

private[spark] case object ShuffleRecoveryAdoptedMissing
  extends ShuffleRecoveryAdoptedReadFailureClass {
  override val authorizesRetirement: Boolean = true
}

private[spark] case object ShuffleRecoveryAdoptedCorrupt
  extends ShuffleRecoveryAdoptedReadFailureClass {
  override val authorizesRetirement: Boolean = true
}

private[spark] case object ShuffleRecoveryAdoptedUnavailable
  extends ShuffleRecoveryAdoptedReadFailureClass {
  override val authorizesRetirement: Boolean = false
}

/** Immutable failure observation copied out of the provider read path before scheduler handling. */
private[spark] final case class ShuffleRecoveryObservedFetchFailure(
    localBindingGeneration: Long,
    bindingId: String,
    mapIndex: Int,
    reduceId: Int,
    failureClass: ShuffleRecoveryAdoptedReadFailureClass)

private[shuffle] sealed trait ShuffleRecoveryBoundMapReadResult

private[shuffle] final case class ShuffleRecoveryBoundMapOpened(
    map: ReferenceShuffleResolvedMap) extends ShuffleRecoveryBoundMapReadResult

private[shuffle] final case class ShuffleRecoveryBoundMapFailed(
    failureClass: ShuffleRecoveryAdoptedReadFailureClass)
  extends ShuffleRecoveryBoundMapReadResult
