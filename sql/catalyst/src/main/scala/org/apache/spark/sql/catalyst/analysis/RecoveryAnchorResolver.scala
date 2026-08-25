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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{SupportsRecoveryAnchor, Table}
import org.apache.spark.sql.connector.recovery.RecoveryTaskCommitStore

/** Immutable source state considered while resolving a resumable batch query relation. */
@DeveloperApi
@Experimental
case class SourceRecoveryInfo(
    sourceId: String,
    currentAnchor: String,
    /**
     * Stable identity of the logical execution performing this read. Connectors use it to derive
     * deterministic recovery pins BEFORE the durable anchor is accepted, so a replacement driver
     * computes the same pin name and cannot adopt a different input state.
     */
    recoveryExecutionId: String)

/** Immutable information identifying a batch write considered for recovery. */
@DeveloperApi
@Experimental
case class WriteRecoveryInfo(
    sinkId: String,
    currentWriteId: String,
    operation: LogicalPlan,
    canonicalizedOperation: LogicalPlan)

/** Resolves the durable immutable state of every source used by a resumable execution. */
@DeveloperApi
@Experimental
trait RecoveryAnchorResolver {
  /**
   * Stable identity of this logical execution across replacement drivers.
   *
   * Providers that enable transactional write recovery must override this method. The value must
   * never be reused for a different logical execution.
   */
  def recoveryExecutionId: String = {
    throw new UnsupportedOperationException(
      "Recovery provider does not expose a stable execution identity")
  }

  def resolveSourceAnchor(info: SourceRecoveryInfo): String

  /** Atomically chooses one durable connector write ID for this logical execution and sink. */
  def resolveWriteId(info: WriteRecoveryInfo): String = {
    throw new UnsupportedOperationException(
      s"Recovery provider does not support writes to ${info.sinkId}")
  }

  /** Executor-capable durable task commit store for recoverable batch writes. */
  def taskCommitStore: Option[RecoveryTaskCommitStore] = None
}

private[sql] object RecoveryAnchorUtils {
  def resolveTable(table: Table, resolver: RecoveryAnchorResolver): Table = table match {
    case recoverable: SupportsRecoveryAnchor =>
      val sourceId = recoverable.recoverySourceId()
      require(sourceId != null && sourceId.nonEmpty,
        "A recovery source identity must not be empty")
      // Give the connector its execution identity BEFORE the selected state is read, so any
      // protection (snapshot pins) exists by the time the anchor names it and is durably
      // accepted below.
      recoverable.beforeRecoveryAnchor(resolver.recoveryExecutionId)
      val currentAnchor = recoverable.currentRecoveryAnchor()
      require(currentAnchor != null && currentAnchor.nonEmpty,
        s"Recovery source $sourceId did not select an immutable input state")
      val anchor = resolver.resolveSourceAnchor(
        SourceRecoveryInfo(
          sourceId,
          currentAnchor,
          resolver.recoveryExecutionId))
      require(anchor != null && anchor.nonEmpty,
        s"Recovery resolver returned an empty anchor for source $sourceId")
      recoverable.withRecoveryAnchor(anchor) match {
        case anchored: SupportsRecoveryAnchor =>
          require(anchored.recoverySourceId() == sourceId,
            s"Recovery source identity changed while applying anchor $anchor")
          require(anchored.currentRecoveryAnchor() == anchor,
            s"Recovery source $sourceId failed to apply anchor $anchor")
          anchored
        case other =>
          throw new IllegalStateException(
            s"Recovery source $sourceId returned non-recoverable table ${other.name()}")
      }
    case other =>
      throw new IllegalStateException(
        s"Recovery is enabled but table ${other.name()} does not implement " +
          classOf[SupportsRecoveryAnchor].getName)
  }
}
