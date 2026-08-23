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

/** Durable state recovered for a batch write after a driver restart. */
@Evolving
public interface BatchWriteRecoveryState {

  /** Whether the connector has already committed the entire write. */
  boolean isCommitted();

  /**
   * One durable commit message per input partition. A null entry means that the partition has not
   * committed and Spark must run it. The array length must equal the physical partition count.
   * Entries may all be null when {@link #isCommitted()} is true and the connector has already
   * garbage-collected its task-level recovery ledger.
   */
  WriterCommitMessage[] commitMessages();

  /**
   * Durable output-row counts corresponding to {@link #commitMessages()}. Use -1 when a recovered
   * partition's count is unavailable. Non-null messages should normally have a non-negative count.
   */
  long[] numRows();

  /**
   * The durable total output-row count when the entire write is committed, or -1 if unavailable.
   * Spark ignores this value while {@link #isCommitted()} is false.
   */
  default long totalNumRows() {
    return -1L;
  }
}
