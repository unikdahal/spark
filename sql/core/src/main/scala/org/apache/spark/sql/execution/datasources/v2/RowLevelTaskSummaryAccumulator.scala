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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.util.RowDeltaUtils
import org.apache.spark.sql.catalyst.util.RowDeltaUtils._
import org.apache.spark.sql.connector.write.RowLevelTaskSummary

/**
 * Task-local interpreter for Spark-owned row-level semantic action codes.
 *
 * Semantic actions survive repartitioning, sorting, and AQE in the first integer field of every
 * row delivered to the final writer task. Control actions contribute to the durable summary but
 * deliberately produce no connector call. Split-update delete halves produce a connector delete
 * but contribute no logical action; the paired reinsert carries the one logical update count.
 */
private[v2] final class RowLevelTaskSummaryAccumulator {
  private var scanned = 0L
  private var copied = 0L
  private var deleted = 0L
  private var updated = 0L
  private var inserted = 0L
  private var matchedUpdated = 0L
  private var matchedDeleted = 0L
  private var notMatchedBySourceUpdated = 0L
  private var notMatchedBySourceDeleted = 0L

  /** Records one semantic action and returns the connector operation to perform, if any. */
  def record(operation: Int): Option[Int] = operation match {
    case COPY_OPERATION =>
      incrementScanned()
      copied = increment(copied)
      Some(COPY_OPERATION)
    case INSERT_OPERATION =>
      inserted = increment(inserted)
      Some(INSERT_OPERATION)
    case UPDATE_OPERATION =>
      incrementScanned()
      updated = increment(updated)
      Some(UPDATE_OPERATION)
    case DELETE_OPERATION =>
      incrementScanned()
      deleted = increment(deleted)
      Some(DELETE_OPERATION)
    case REINSERT_OPERATION =>
      incrementScanned()
      updated = increment(updated)
      Some(REINSERT_OPERATION)
    case SPLIT_UPDATE_REINSERT_OPERATION =>
      incrementScanned()
      updated = increment(updated)
      Some(REINSERT_OPERATION)
    case NO_WRITE_OPERATION =>
      incrementScanned()
      None
    case DELETE_CONTROL_OPERATION =>
      incrementScanned()
      deleted = increment(deleted)
      None
    case MATCHED_UPDATE_OPERATION =>
      incrementScanned()
      updated = increment(updated)
      matchedUpdated = increment(matchedUpdated)
      Some(UPDATE_OPERATION)
    case MATCHED_DELETE_OPERATION =>
      incrementScanned()
      deleted = increment(deleted)
      matchedDeleted = increment(matchedDeleted)
      Some(DELETE_OPERATION)
    case NOT_MATCHED_BY_SOURCE_UPDATE_OPERATION =>
      incrementScanned()
      updated = increment(updated)
      notMatchedBySourceUpdated = increment(notMatchedBySourceUpdated)
      Some(UPDATE_OPERATION)
    case NOT_MATCHED_BY_SOURCE_DELETE_OPERATION =>
      incrementScanned()
      deleted = increment(deleted)
      notMatchedBySourceDeleted = increment(notMatchedBySourceDeleted)
      Some(DELETE_OPERATION)
    case MATCHED_DELETE_CONTROL_OPERATION =>
      incrementScanned()
      deleted = increment(deleted)
      matchedDeleted = increment(matchedDeleted)
      None
    case NOT_MATCHED_BY_SOURCE_DELETE_CONTROL_OPERATION =>
      incrementScanned()
      deleted = increment(deleted)
      notMatchedBySourceDeleted = increment(notMatchedBySourceDeleted)
      None
    case SPLIT_UPDATE_DELETE_OPERATION |
        MATCHED_SPLIT_UPDATE_DELETE_OPERATION |
        NOT_MATCHED_BY_SOURCE_SPLIT_UPDATE_DELETE_OPERATION =>
      Some(DELETE_OPERATION)
    case MATCHED_SPLIT_UPDATE_REINSERT_OPERATION =>
      incrementScanned()
      updated = increment(updated)
      matchedUpdated = increment(matchedUpdated)
      Some(REINSERT_OPERATION)
    case NOT_MATCHED_BY_SOURCE_SPLIT_UPDATE_REINSERT_OPERATION =>
      incrementScanned()
      updated = increment(updated)
      notMatchedBySourceUpdated = increment(notMatchedBySourceUpdated)
      Some(REINSERT_OPERATION)
    case other =>
      throw new SparkException(s"Unexpected row-level semantic operation ID: $other")
  }

  def summary: RowLevelTaskSummary = new RowLevelTaskSummary(
    scanned,
    copied,
    deleted,
    updated,
    inserted,
    matchedUpdated,
    matchedDeleted,
    notMatchedBySourceUpdated,
    notMatchedBySourceDeleted)

  def result(): RowLevelTaskSummary = summary

  private def incrementScanned(): Unit = {
    scanned = increment(scanned)
  }

  private def increment(value: Long): Long = Math.addExact(value, 1L)
}

private[v2] object RowLevelTaskSummaryAccumulator {
  object ActionCode {
    val COPY: Int = COPY_OPERATION
    val INSERT: Int = INSERT_OPERATION
    val UPDATE: Int = UPDATE_OPERATION
    val DELETE: Int = DELETE_OPERATION
    val REINSERT: Int = REINSERT_OPERATION
    val DELETE_CONTROL: Int = DELETE_CONTROL_OPERATION
    val MATCHED_UPDATE: Int = MATCHED_UPDATE_OPERATION
    val MATCHED_DELETE: Int = MATCHED_DELETE_OPERATION
    val NOT_MATCHED_BY_SOURCE_UPDATE: Int = NOT_MATCHED_BY_SOURCE_UPDATE_OPERATION
    val NOT_MATCHED_BY_SOURCE_DELETE: Int = NOT_MATCHED_BY_SOURCE_DELETE_OPERATION
    val MATCHED_DELETE_CONTROL: Int = MATCHED_DELETE_CONTROL_OPERATION
    val NOT_MATCHED_BY_SOURCE_DELETE_CONTROL: Int =
      NOT_MATCHED_BY_SOURCE_DELETE_CONTROL_OPERATION
    val SPLIT_UPDATE_DELETE: Int = RowDeltaUtils.SPLIT_UPDATE_DELETE_OPERATION
    val SPLIT_UPDATE_REINSERT: Int = RowDeltaUtils.SPLIT_UPDATE_REINSERT_OPERATION
    val MATCHED_SPLIT_UPDATE_DELETE: Int = MATCHED_SPLIT_UPDATE_DELETE_OPERATION
    val MATCHED_SPLIT_UPDATE_REINSERT: Int = MATCHED_SPLIT_UPDATE_REINSERT_OPERATION
    val NOT_MATCHED_BY_SOURCE_SPLIT_UPDATE_DELETE: Int =
      NOT_MATCHED_BY_SOURCE_SPLIT_UPDATE_DELETE_OPERATION
    val NOT_MATCHED_BY_SOURCE_SPLIT_UPDATE_REINSERT: Int =
      NOT_MATCHED_BY_SOURCE_SPLIT_UPDATE_REINSERT_OPERATION
    val NO_WRITE: Int = NO_WRITE_OPERATION
  }
}
