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

package org.apache.spark.sql.connector.write;

import org.apache.spark.annotation.Evolving;

/**
 * A batch write that can recover durable task commits after a driver restart.
 *
 * <p>Spark records every successful data-writer commit in its configured recovery store. The
 * connector supplies a stable encoding of its commit message and retains responsibility for an
 * idempotent global commit. Recovery must fail rather than return ambiguous or incompatible
 * state. Spark does not invoke {@link #onDataWriterCommit(WriterCommitMessage)} for recovery
 * writes because a canonical message may be discovered after a lost response or from another
 * attempt. Implementations must derive global commit state only from the canonical messages passed
 * to {@link #commit(WriterCommitMessage[])}.</p>
 */
@Evolving
public interface SupportsBatchWriteRecovery extends BatchWrite {

  /** Stable codec used for task commit messages stored outside the connector. */
  RecoveryCommitMessageCodec commitMessageCodec();

  /**
   * Stable compatibility metadata bound before tasks start.
   *
   * <p>The payload must identify the sink and logical operation and include all connector state
   * that could change the meaning of a task commit, such as write mode, schema, partition spec,
   * and overwrite base state. It must use a stable, non-executable encoding.</p>
   */
  byte[] recoveryCompatibilityMetadata(PhysicalWriteInfo info);

  /** Loads connector-owned global state before Spark creates or launches data writers. */
  BatchWriteRecoveryState recover(PhysicalWriteInfo info);

  /**
   * Handles failure after recovery task execution has started.
   *
   * <p>This method must preserve all durable task commits so another driver can resume them. It
   * may clean up uncommitted attempts and other state that cannot be used by a later recovery.
   * Spark calls this method instead of {@link #abort(WriterCommitMessage[])} after entering task
   * execution even if no task commit existed when {@link #recover(PhysicalWriteInfo)} was called,
   * because commits from the current attempt must also remain recoverable. Spark does not call it
   * for manifest resolution, recovery-state lookup, or initial task-store lookup failures, since
   * those occur before writers can create output.</p>
   */
  void abortAfterRecovery(WriterCommitMessage[] messages);
}
