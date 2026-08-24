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
import java.util.Collection;
import java.util.Objects;

import org.apache.spark.annotation.Evolving;

/**
 * Immutable additive counters produced by one physical row-level write task.
 *
 * <p>Every counter is exact and non-negative. A zero value means that the task performed no such
 * operation; unknown values are not representable. {@link #plus(RowLevelTaskSummary)} and
 * {@link #sum(Collection)} reject overflow instead of returning a corrupted summary.</p>
 *
 * <p>This value contains the counters needed to reconstruct {@link MergeSummary},
 * {@link UpdateSummary}, and {@link DeleteSummary}. It does not identify a command, write
 * generation, or task partition; those identities belong to the durable record that contains
 * this value. The scanned-row counter is retained separately because a group-replacement DELETE
 * may derive its deleted-row count from scanned rows minus copied rows; the deleted rows do not
 * reach the writer task as output records.</p>
 *
 * @since 4.2.0
 */
@Evolving
public final class RowLevelTaskSummary implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final RowLevelTaskSummary EMPTY =
      new RowLevelTaskSummary(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);

  private final long numTargetRowsScanned;
  private final long numTargetRowsCopied;
  private final long numTargetRowsDeleted;
  private final long numTargetRowsUpdated;
  private final long numTargetRowsInserted;
  private final long numTargetRowsMatchedUpdated;
  private final long numTargetRowsMatchedDeleted;
  private final long numTargetRowsNotMatchedBySourceUpdated;
  private final long numTargetRowsNotMatchedBySourceDeleted;

  /** Creates a task summary from exact non-negative additive counters. */
  public RowLevelTaskSummary(
      long numTargetRowsScanned,
      long numTargetRowsCopied,
      long numTargetRowsDeleted,
      long numTargetRowsUpdated,
      long numTargetRowsInserted,
      long numTargetRowsMatchedUpdated,
      long numTargetRowsMatchedDeleted,
      long numTargetRowsNotMatchedBySourceUpdated,
      long numTargetRowsNotMatchedBySourceDeleted) {
    this.numTargetRowsScanned = nonNegative(numTargetRowsScanned, "numTargetRowsScanned");
    this.numTargetRowsCopied = nonNegative(numTargetRowsCopied, "numTargetRowsCopied");
    this.numTargetRowsDeleted = nonNegative(numTargetRowsDeleted, "numTargetRowsDeleted");
    this.numTargetRowsUpdated = nonNegative(numTargetRowsUpdated, "numTargetRowsUpdated");
    this.numTargetRowsInserted = nonNegative(numTargetRowsInserted, "numTargetRowsInserted");
    this.numTargetRowsMatchedUpdated =
        nonNegative(numTargetRowsMatchedUpdated, "numTargetRowsMatchedUpdated");
    this.numTargetRowsMatchedDeleted =
        nonNegative(numTargetRowsMatchedDeleted, "numTargetRowsMatchedDeleted");
    this.numTargetRowsNotMatchedBySourceUpdated = nonNegative(
        numTargetRowsNotMatchedBySourceUpdated, "numTargetRowsNotMatchedBySourceUpdated");
    this.numTargetRowsNotMatchedBySourceDeleted = nonNegative(
        numTargetRowsNotMatchedBySourceDeleted, "numTargetRowsNotMatchedBySourceDeleted");
  }

  /** Returns a summary whose counters are all zero. */
  public static RowLevelTaskSummary empty() {
    return EMPTY;
  }

  /** Returns the number of target rows scanned by this task. */
  public long numTargetRowsScanned() {
    return numTargetRowsScanned;
  }

  /** Returns the number of target rows copied without modification. */
  public long numTargetRowsCopied() {
    return numTargetRowsCopied;
  }

  /** Returns the number of target rows deleted. */
  public long numTargetRowsDeleted() {
    return numTargetRowsDeleted;
  }

  /** Returns the number of target rows updated. */
  public long numTargetRowsUpdated() {
    return numTargetRowsUpdated;
  }

  /** Returns the number of target rows inserted. */
  public long numTargetRowsInserted() {
    return numTargetRowsInserted;
  }

  /** Returns the number of target rows updated by a matched clause. */
  public long numTargetRowsMatchedUpdated() {
    return numTargetRowsMatchedUpdated;
  }

  /** Returns the number of target rows deleted by a matched clause. */
  public long numTargetRowsMatchedDeleted() {
    return numTargetRowsMatchedDeleted;
  }

  /** Returns the number of target rows updated by a not-matched-by-source clause. */
  public long numTargetRowsNotMatchedBySourceUpdated() {
    return numTargetRowsNotMatchedBySourceUpdated;
  }

  /** Returns the number of target rows deleted by a not-matched-by-source clause. */
  public long numTargetRowsNotMatchedBySourceDeleted() {
    return numTargetRowsNotMatchedBySourceDeleted;
  }

  /** Returns the component-wise sum of this summary and {@code other}. */
  public RowLevelTaskSummary plus(RowLevelTaskSummary other) {
    Objects.requireNonNull(other, "other");
    return new RowLevelTaskSummary(
        Math.addExact(numTargetRowsScanned, other.numTargetRowsScanned),
        Math.addExact(numTargetRowsCopied, other.numTargetRowsCopied),
        Math.addExact(numTargetRowsDeleted, other.numTargetRowsDeleted),
        Math.addExact(numTargetRowsUpdated, other.numTargetRowsUpdated),
        Math.addExact(numTargetRowsInserted, other.numTargetRowsInserted),
        Math.addExact(numTargetRowsMatchedUpdated, other.numTargetRowsMatchedUpdated),
        Math.addExact(numTargetRowsMatchedDeleted, other.numTargetRowsMatchedDeleted),
        Math.addExact(
            numTargetRowsNotMatchedBySourceUpdated,
            other.numTargetRowsNotMatchedBySourceUpdated),
        Math.addExact(
            numTargetRowsNotMatchedBySourceDeleted,
            other.numTargetRowsNotMatchedBySourceDeleted));
  }

  /** Returns the component-wise sum of all non-null {@code summaries}. */
  public static RowLevelTaskSummary sum(Collection<RowLevelTaskSummary> summaries) {
    Objects.requireNonNull(summaries, "summaries");
    RowLevelTaskSummary result = EMPTY;
    for (RowLevelTaskSummary summary : summaries) {
      result = result.plus(Objects.requireNonNull(summary, "summary"));
    }
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof RowLevelTaskSummary)) {
      return false;
    }
    RowLevelTaskSummary that = (RowLevelTaskSummary) other;
    return numTargetRowsScanned == that.numTargetRowsScanned
        && numTargetRowsCopied == that.numTargetRowsCopied
        && numTargetRowsDeleted == that.numTargetRowsDeleted
        && numTargetRowsUpdated == that.numTargetRowsUpdated
        && numTargetRowsInserted == that.numTargetRowsInserted
        && numTargetRowsMatchedUpdated == that.numTargetRowsMatchedUpdated
        && numTargetRowsMatchedDeleted == that.numTargetRowsMatchedDeleted
        && numTargetRowsNotMatchedBySourceUpdated == that.numTargetRowsNotMatchedBySourceUpdated
        && numTargetRowsNotMatchedBySourceDeleted == that.numTargetRowsNotMatchedBySourceDeleted;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        numTargetRowsScanned,
        numTargetRowsCopied,
        numTargetRowsDeleted,
        numTargetRowsUpdated,
        numTargetRowsInserted,
        numTargetRowsMatchedUpdated,
        numTargetRowsMatchedDeleted,
        numTargetRowsNotMatchedBySourceUpdated,
        numTargetRowsNotMatchedBySourceDeleted);
  }

  @Override
  public String toString() {
    return "RowLevelTaskSummary(" +
        "numTargetRowsScanned=" + numTargetRowsScanned +
        ", numTargetRowsCopied=" + numTargetRowsCopied +
        ", numTargetRowsDeleted=" + numTargetRowsDeleted +
        ", numTargetRowsUpdated=" + numTargetRowsUpdated +
        ", numTargetRowsInserted=" + numTargetRowsInserted +
        ", numTargetRowsMatchedUpdated=" + numTargetRowsMatchedUpdated +
        ", numTargetRowsMatchedDeleted=" + numTargetRowsMatchedDeleted +
        ", numTargetRowsNotMatchedBySourceUpdated=" +
        numTargetRowsNotMatchedBySourceUpdated +
        ", numTargetRowsNotMatchedBySourceDeleted=" +
        numTargetRowsNotMatchedBySourceDeleted + ')';
  }

  private static long nonNegative(long value, String name) {
    if (value < 0L) {
      throw new IllegalArgumentException(name + " must be non-negative: " + value);
    }
    return value;
  }
}
