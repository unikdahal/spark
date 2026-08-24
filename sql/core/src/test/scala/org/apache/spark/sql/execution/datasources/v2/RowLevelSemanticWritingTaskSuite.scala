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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkFunSuite, TaskContext}
import org.apache.spark.sql.catalyst.{InternalRow, ProjectingInternalRow}
import org.apache.spark.sql.catalyst.util.WriteDeltaProjections
import org.apache.spark.sql.catalyst.util.RowDeltaUtils._
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, DeltaWriter,
  DeltaWriterFactory, RowLevelTaskSummary, WriterCommitMessage}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class RowLevelSemanticWritingTaskSuite extends SparkFunSuite {

  test("group replacement skips controls and reports connector rows separately") {
    val state = new GroupWriterState
    val task = DataAndMetadataWritingSparkTask(
      projection(1), projection(2), Map.empty)
    val rows = Iterator(
      row(COPY_OPERATION, 10, 100),
      row(INSERT_OPERATION, 11, 101),
      row(MATCHED_UPDATE_OPERATION, 12, 102),
      row(MATCHED_DELETE_CONTROL_OPERATION, 13, 103),
      row(NOT_MATCHED_BY_SOURCE_DELETE_CONTROL_OPERATION, 14, 104),
      row(NO_WRITE_OPERATION, 15, 105))

    val result = task.run(
      new GroupWriterFactory(state), TaskContext.empty(), rows,
      useCommitCoordinator = false, Map.empty)

    assert(state.calls === Seq(
      "writeWithMetadata(100,10)",
      "write(11)",
      "writeWithMetadata(102,12)"))
    assert(result.numRows === 3L)
    assert(result.rowLevelSummary.contains(summary(
      scanned = 5L,
      copied = 1L,
      deleted = 2L,
      updated = 1L,
      inserted = 1L,
      matchedUpdated = 1L,
      matchedDeleted = 1L,
      notMatchedBySourceDeleted = 1L)))
  }

  test("group replacement without metadata maps plain and clause-specific updates") {
    val state = new GroupWriterState
    val task = DataWithProjectionWritingSparkTask(projection(1), Map.empty)
    val rows = Iterator(
      row(COPY_OPERATION, 40),
      row(UPDATE_OPERATION, 41),
      row(NOT_MATCHED_BY_SOURCE_UPDATE_OPERATION, 42),
      row(DELETE_CONTROL_OPERATION, 43),
      row(INSERT_OPERATION, 44))

    val result = task.run(
      new GroupWriterFactory(state), TaskContext.empty(), rows,
      useCommitCoordinator = false, Map.empty)

    assert(state.calls === Seq("write(40)", "write(41)", "write(42)", "write(44)"))
    assert(result.numRows === 4L)
    assert(result.rowLevelSummary.contains(summary(
      scanned = 4L,
      copied = 1L,
      deleted = 1L,
      updated = 2L,
      inserted = 1L,
      notMatchedBySourceUpdated = 1L)))
  }

  test("delta task maps semantic operations and never calls connector for controls") {
    val state = new DeltaWriterState
    val projections = WriteDeltaProjections(
      rowProjection = Some(projection(1)),
      rowIdProjection = projection(2),
      metadataProjection = Some(projection(3)))
    val task = DeltaWithMetadataWritingSparkTask(projections, Map.empty)
    val rows = Iterator(
      row(MATCHED_DELETE_OPERATION, 20, 200, 2000),
      row(NOT_MATCHED_BY_SOURCE_UPDATE_OPERATION, 21, 201, 2001),
      row(MATCHED_SPLIT_UPDATE_DELETE_OPERATION, 22, 202, 2002),
      row(MATCHED_SPLIT_UPDATE_REINSERT_OPERATION, 23, 203, 2003),
      row(MATCHED_DELETE_CONTROL_OPERATION, 24, 204, 2004),
      row(NO_WRITE_OPERATION, 25, 205, 2005),
      row(INSERT_OPERATION, 26, 206, 2006))

    val result = task.run(
      new SemanticTestDeltaWriterFactory(state), TaskContext.empty(), rows,
      useCommitCoordinator = false, Map.empty)

    assert(state.calls === Seq(
      "delete(2000,200)",
      "update(2001,201,21)",
      "delete(2002,202)",
      "reinsert(2003,23)",
      "insert(26)"))
    assert(result.numRows === 5L)
    assert(result.rowLevelSummary.contains(summary(
      scanned = 5L,
      deleted = 2L,
      updated = 2L,
      inserted = 1L,
      matchedUpdated = 1L,
      matchedDeleted = 2L,
      notMatchedBySourceUpdated = 1L)))
  }

  test("delta task without metadata exhaustively maps plain and split semantic operations") {
    val state = new DeltaWriterState
    val projections = WriteDeltaProjections(
      rowProjection = Some(projection(1)),
      rowIdProjection = projection(2),
      metadataProjection = None)
    val task = DeltaWritingSparkTask(projections, Map.empty)
    val rows = Iterator(
      row(DELETE_OPERATION, 30, 300),
      row(UPDATE_OPERATION, 31, 301),
      row(REINSERT_OPERATION, 32, 302),
      row(SPLIT_UPDATE_DELETE_OPERATION, 33, 303),
      row(SPLIT_UPDATE_REINSERT_OPERATION, 34, 304),
      row(NOT_MATCHED_BY_SOURCE_DELETE_OPERATION, 35, 305),
      row(NOT_MATCHED_BY_SOURCE_SPLIT_UPDATE_DELETE_OPERATION, 36, 306),
      row(NOT_MATCHED_BY_SOURCE_SPLIT_UPDATE_REINSERT_OPERATION, 37, 307),
      row(DELETE_CONTROL_OPERATION, 38, 308))

    val result = task.run(
      new SemanticTestDeltaWriterFactory(state), TaskContext.empty(), rows,
      useCommitCoordinator = false, Map.empty)

    assert(state.calls === Seq(
      "delete(null,300)",
      "update(null,301,31)",
      "reinsert(null,32)",
      "delete(null,303)",
      "reinsert(null,34)",
      "delete(null,305)",
      "delete(null,306)",
      "reinsert(null,37)"))
    assert(result.numRows === 8L)
    assert(result.rowLevelSummary.contains(summary(
      scanned = 7L,
      deleted = 3L,
      updated = 4L,
      notMatchedBySourceUpdated = 1L,
      notMatchedBySourceDeleted = 1L)))
  }

  private def projection(ordinal: Int): ProjectingInternalRow = {
    ProjectingInternalRow(
      StructType(Seq(StructField("value", IntegerType, nullable = false))), Seq(ordinal))
  }

  private def row(values: Int*): InternalRow = InternalRow(values: _*)

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

private case object SemanticWriterCommitMessage extends WriterCommitMessage

private class GroupWriterState {
  val calls: ArrayBuffer[String] = ArrayBuffer.empty
}

private class GroupWriterFactory(state: GroupWriterState) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new DataWriter[InternalRow] {
      override def write(record: InternalRow): Unit =
        state.calls += s"write(${record.getInt(0)})"
      override def write(metadata: InternalRow, record: InternalRow): Unit =
        state.calls += s"writeWithMetadata(${metadata.getInt(0)},${record.getInt(0)})"
      override def commit(): WriterCommitMessage = SemanticWriterCommitMessage
      override def abort(): Unit = {}
      override def close(): Unit = {}
    }
}

private class DeltaWriterState {
  val calls: ArrayBuffer[String] = ArrayBuffer.empty
}

private class SemanticTestDeltaWriterFactory(state: DeltaWriterState) extends DeltaWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DeltaWriter[InternalRow] =
    new DeltaWriter[InternalRow] {
      override def delete(metadata: InternalRow, id: InternalRow): Unit =
        state.calls += s"delete(${value(metadata)},${id.getInt(0)})"
      override def update(
          metadata: InternalRow,
          id: InternalRow,
          record: InternalRow): Unit = {
        state.calls +=
          s"update(${value(metadata)},${id.getInt(0)},${record.getInt(0)})"
      }
      override def reinsert(metadata: InternalRow, record: InternalRow): Unit =
        state.calls += s"reinsert(${value(metadata)},${record.getInt(0)})"
      override def insert(record: InternalRow): Unit =
        state.calls += s"insert(${record.getInt(0)})"
      override def commit(): WriterCommitMessage = SemanticWriterCommitMessage
      override def abort(): Unit = {}
      override def close(): Unit = {}

      private def value(metadata: InternalRow): String = {
        if (metadata == null) "null" else metadata.getInt(0).toString
      }
    }
}
