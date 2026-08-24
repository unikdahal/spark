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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.apache.spark.annotation.Evolving;

/** Durable state recovered for a batch write after a driver restart. */
@Evolving
public interface BatchWriteRecoveryState {

  /** Whether the connector has already committed the entire write. */
  boolean isCommitted();

  /**
   * The durable total output-row count when the entire write is committed, or -1 if unavailable.
   * Spark ignores this value while {@link #isCommitted()} is false.
   */
  default long totalNumRows() {
    return -1L;
  }

  /**
   * Durable totals for every metric declared by {@link SupportsRecoveryTaskMetrics} when the
   * entire write is committed. Spark ignores this value while {@link #isCommitted()} is false.
   * A committed write with a recovery metric schema must return every declared metric exactly
   * once and must not return unknown metrics.
   */
  default Map<String, Long> totalTaskMetrics() {
    return Collections.emptyMap();
  }

  /**
   * The authoritative row-level summary when the entire write is committed.
   *
   * <p>Spark ignores this value while {@link #isCommitted()} is false. A committed recoverable
   * row-level write must return a present value so Spark can reconstruct the command summary
   * without executing writer or source tasks again. Non-row-level writes return an empty value.</p>
   */
  default Optional<RowLevelTaskSummary> totalRowLevelSummary() {
    return Optional.empty();
  }
}
