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

package org.apache.spark.sql.connector.recovery;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.annotation.Experimental;

/**
 * Durable, fenced storage used by Spark to arbitrate recoverable batch task commits.
 *
 * <p>This is a recovery-provider SPI, not a data-source connector API. The store is created on the
 * driver and serialized to executors. Implementations must use a strongly consistent immutable
 * compare-and-set for every manifest and task key. Lease loss, fencing, ambiguity, corruption, and
 * unavailability must fail the operation; a successful empty lookup is the only authoritative
 * indication that a task commit is absent.</p>
 *
 * <p>Values are opaque Spark-owned envelopes and must be preserved byte-for-byte.</p>
 */
@DeveloperApi
@Experimental
public interface RecoveryTaskCommitStore extends Serializable {

  /** Semantics version required by this Spark implementation. */
  int SEMANTICS_VERSION = 1;

  /**
   * Immutable limits and semantics supported by this store instance.
   *
   * <p>Byte limits cover the complete Spark envelope, including headers and checksum. The batch
   * limit is a number of partition IDs. These values must be positive, remain unchanged for the
   * serialized store instance, and be available on executors without driver-local state.</p>
   */
  interface Capabilities extends Serializable {
    int semanticsVersion();
    int maxLoadBatchSize();
    int maxManifestBytes();
    int maxTaskCommitBytes();
  }

  /** Machine-readable reasons why an operation could not return authoritative state. */
  enum FailureReason {
    UNAVAILABLE,
    FENCED,
    AMBIGUOUS,
    CORRUPT,
    INCOMPATIBLE,
    RESOURCE_EXHAUSTED
  }

  /** A fail-closed store operation failure. */
  class StoreException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final FailureReason reason;

    public StoreException(FailureReason reason, String message) {
      super(message);
      this.reason = Objects.requireNonNull(reason, "reason");
    }

    public StoreException(FailureReason reason, String message, Throwable cause) {
      super(message, cause);
      this.reason = Objects.requireNonNull(reason, "reason");
    }

    public FailureReason reason() {
      return reason;
    }
  }

  /** Returns non-null immutable protocol and request limits. */
  Capabilities capabilities();

  /**
   * Atomically chooses the immutable manifest for a write and returns the canonical value.
   *
   * <p>Arguments and results must be non-null. The proposed value must not exceed
   * {@link Capabilities#maxManifestBytes()}. The returned bytes must equal either the proposal or
   * the existing canonical value. Implementations must not retain or return mutable shared array
   * storage. A non-authoritative outcome must throw {@link StoreException}.</p>
   */
  byte[] resolveWriteManifest(String recoveryId, byte[] proposedValue);

  /**
   * Loads immutable values in the same order as {@code partitionIds}.
   *
   * <p>Arguments and results must be non-null, and the request size must not exceed
   * {@link Capabilities#maxLoadBatchSize()}. The returned list must have the same size as the
   * request, contain no null elements, and use {@link Optional#empty()} only for an authoritative
   * absence. Present byte arrays must be defensively owned. A non-authoritative outcome must throw
   * {@link StoreException}.</p>
   */
  List<Optional<byte[]>> load(String recoveryId, int[] partitionIds);

  /**
   * Atomically publishes a value and returns the canonical immutable value for this partition.
   *
   * <p>Arguments and results must be non-null, and {@code value} must not exceed
   * {@link Capabilities#maxTaskCommitBytes()}. The returned bytes must equal either {@code value}
   * or the exact existing canonical value. Implementations must not retain or return mutable
   * shared array storage. A non-authoritative outcome must throw {@link StoreException}.</p>
   */
  byte[] publish(
      String recoveryId,
      int partitionId,
      long taskAttemptId,
      int attemptNumber,
      byte[] value);
}
