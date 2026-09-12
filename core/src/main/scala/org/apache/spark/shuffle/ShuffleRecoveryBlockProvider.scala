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

import org.apache.spark.network.buffer.ManagedBuffer

/**
 * Provider contract for the existing block-addressed adoption path. Claims bind immutable
 * retained output to an attempt-local shuffle ID. Release drops that local binding, not the
 * retained output; source artifacts can still serve other attempts. Providers must fence every
 * metadata and block access against the exact binding, including after release.
 *
 * Calls may perform I/O and must never run on the scheduler event loop. The claim descriptor
 * remains the authority checked by Spark's trust boundary. Implementations must classify missing,
 * changed, and unavailable artifacts against that validated descriptor on subsequent fetches.
 *
 * This internal compatibility path still requires exact map/reducer block metadata. It is not
 * the compact executor-native descriptor protocol needed by remote shuffle services.
 */
private[spark] trait ShuffleRecoveryBlockProvider extends ShuffleRecoveryClaimProvider {
  private[shuffle] def openBoundMap(
      binding: ShuffleRecoveryBinding,
      mapIndex: Int): ShuffleRecoveryResolvedMap

  private[shuffle] def openBoundMapForFetch(
      binding: ShuffleRecoveryBinding,
      mapIndex: Int,
      expected: ShuffleRecoveryPreparedMap): ShuffleRecoveryBoundMapReadResult
}

/** Immutable metadata view; logical offsets need not correspond to a local file. */
private[spark] trait ShuffleRecoveryBlockMetadata {
  def offset: Long
  def length: Long
  final def isEmpty: Boolean = length == 0L
}

/**
 * Bound provider map view. Sizes and logical offsets must agree with the validated claim.
 * A nonempty block must yield a buffer of exactly its certified length. None denotes an empty
 * block only. Ownership of a returned buffer transfers to the caller, who releases it on rejection
 * or through the ordinary shuffle transport lifecycle. Views must not hold uncloseable resources.
 */
private[spark] trait ShuffleRecoveryResolvedMap {
  def numReducers: Int
  def dataLength: Long
  def indexBytes: Long
  def blockMetadata(reduceId: Int): ShuffleRecoveryBlockMetadata
  def getBlockData(reduceId: Int): Option[ManagedBuffer]
}
