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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connector.write.RowLevelTaskSummary
import org.apache.spark.sql.execution.datasources.v2.RowLevelTaskSummaryAccumulator.ActionCode._

/** Contract tests for mapping Spark-owned row-action codes to durable task counters. */
class RowLevelTaskSummaryAccumulatorSuite extends SparkFunSuite {

  test("COPY INSERT UPDATE and DELETE map to their plain logical counters") {
    assert(record(COPY) === summary(scanned = 1L, copied = 1L))
    assert(record(INSERT) === summary(inserted = 1L))
    assert(record(UPDATE) === summary(scanned = 1L, updated = 1L))
    assert(record(DELETE) === summary(scanned = 1L, deleted = 1L))
  }

  test("matched MERGE actions increment total and matched counters once") {
    assert(record(MATCHED_UPDATE) ===
      summary(scanned = 1L, updated = 1L, matchedUpdated = 1L))
    assert(record(MATCHED_DELETE) ===
      summary(scanned = 1L, deleted = 1L, matchedDeleted = 1L))
  }

  test("not-matched-by-source MERGE actions increment their disjoint counters once") {
    assert(record(NOT_MATCHED_BY_SOURCE_UPDATE) ===
      summary(scanned = 1L, updated = 1L, notMatchedBySourceUpdated = 1L))
    assert(record(NOT_MATCHED_BY_SOURCE_DELETE) ===
      summary(scanned = 1L, deleted = 1L, notMatchedBySourceDeleted = 1L))
  }

  test("split update ignores its auxiliary delete and counts its reinsert exactly once") {
    val accumulator = new RowLevelTaskSummaryAccumulator
    accumulator.record(SPLIT_UPDATE_DELETE)
    assert(accumulator.result() === RowLevelTaskSummary.empty())

    accumulator.record(SPLIT_UPDATE_REINSERT)
    assert(accumulator.result() === summary(scanned = 1L, updated = 1L))
  }

  test("matched split update attributes only its reinsert to the matched clause") {
    val accumulator = new RowLevelTaskSummaryAccumulator
    accumulator.record(MATCHED_SPLIT_UPDATE_DELETE)
    accumulator.record(MATCHED_SPLIT_UPDATE_REINSERT)

    assert(accumulator.result() ===
      summary(scanned = 1L, updated = 1L, matchedUpdated = 1L))
  }

  test("not-matched-by-source split update attributes only its reinsert to that clause") {
    val accumulator = new RowLevelTaskSummaryAccumulator
    accumulator.record(NOT_MATCHED_BY_SOURCE_SPLIT_UPDATE_DELETE)
    accumulator.record(NOT_MATCHED_BY_SOURCE_SPLIT_UPDATE_REINSERT)

    assert(accumulator.result() ===
      summary(scanned = 1L, updated = 1L, notMatchedBySourceUpdated = 1L))
  }

  test("NO_WRITE records a scanned target row without inventing a writer operation") {
    assert(record(NO_WRITE) === summary(scanned = 1L))
  }

  test("a mixed action stream preserves exact disjoint totals") {
    val accumulator = new RowLevelTaskSummaryAccumulator
    Seq(
      COPY,
      INSERT,
      UPDATE,
      DELETE,
      MATCHED_UPDATE,
      MATCHED_DELETE,
      NOT_MATCHED_BY_SOURCE_UPDATE,
      NOT_MATCHED_BY_SOURCE_DELETE,
      SPLIT_UPDATE_DELETE,
      SPLIT_UPDATE_REINSERT,
      NO_WRITE).foreach(accumulator.record)

    assert(accumulator.result() === summary(
      scanned = 9L,
      copied = 1L,
      deleted = 3L,
      updated = 4L,
      inserted = 1L,
      matchedUpdated = 1L,
      matchedDeleted = 1L,
      notMatchedBySourceUpdated = 1L,
      notMatchedBySourceDeleted = 1L))
  }

  private def record(actionCode: Int): RowLevelTaskSummary = {
    val accumulator = new RowLevelTaskSummaryAccumulator
    accumulator.record(actionCode)
    accumulator.result()
  }

  private def summary(
      scanned: Long = 0L,
      copied: Long = 0L,
      deleted: Long = 0L,
      updated: Long = 0L,
      inserted: Long = 0L,
      matchedUpdated: Long = 0L,
      matchedDeleted: Long = 0L,
      notMatchedBySourceUpdated: Long = 0L,
      notMatchedBySourceDeleted: Long = 0L): RowLevelTaskSummary = {
    new RowLevelTaskSummary(
      scanned,
      copied,
      deleted,
      updated,
      inserted,
      matchedUpdated,
      matchedDeleted,
      notMatchedBySourceUpdated,
      notMatchedBySourceDeleted)
  }
}
