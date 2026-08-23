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

import java.io.Serializable;

import org.apache.spark.annotation.Evolving;

/**
 * Durable, fenced storage for recoverable batch task commits.
 *
 * <p>The store is created on the driver and serialized to executors. Implementations must use a
 * strongly consistent immutable compare-and-set for each {@code (recoveryId, partitionId)} key.
 * {@link #publish} must return the canonical value: either {@code value} when this attempt wins,
 * or the previously published value when another attempt won. It must never overwrite a value.
 * Lease loss, fencing, ambiguity, corruption, and unavailability must be surfaced as failures.</p>
 *
 * <p>Values are opaque Spark-owned envelopes. Implementations must preserve their bytes exactly.
 * A null result from {@link #load} means absent; all other null results are invalid.</p>
 */
@Evolving
public interface RecoveryTaskCommitStore extends Serializable {

  /**
   * Atomically chooses the immutable manifest for a write and returns the canonical value.
   * Implementations must return {@code proposedValue} byte-for-byte when it wins, or the existing
   * value when another driver won. This must complete before any data writer is created.
   */
  byte[] resolveWriteManifest(String recoveryId, byte[] proposedValue);

  /**
   * Loads immutable values for a batch of partitions.
   *
   * <p>The returned array must have the same length and order as {@code partitionIds}; a null
   * entry means absent. Spark calls this method in bounded batches so implementations can use
   * batched or paged RPCs instead of one round trip per partition.</p>
   */
  byte[][] load(String recoveryId, int[] partitionIds);

  /**
   * Atomically publishes a value and returns the canonical immutable value for this partition.
   */
  byte[] publish(
      String recoveryId,
      int partitionId,
      long taskAttemptId,
      int attemptNumber,
      byte[] value);
}
