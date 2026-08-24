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

import java.util.{List => JList, Optional}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connector.recovery.RecoveryTaskCommitStore
import org.apache.spark.sql.connector.write.{BatchWriteRecoveryState, DeltaBatchWrite, DeltaWrite,
  DeltaWriterFactory, PhysicalWriteInfo, RecoveryCommitMessageCodec,
  RecoveryDeltaWriter, RecoveryDeltaWriterFactory, RecoveryTaskMetricDescriptor,
  RecoveryTaskMetricSchema,
  SupportsDeltaBatchWriteRecovery, SupportsRecoveryTaskMetrics, WriterCommitMessage}

class RecoveryRequiredDeltaWriteSuite extends SparkFunSuite {

  test("wrapper preserves delta recovery factory and metric schema types") {
    val delegate = new TestDeltaWrite(TestDeltaBatchWrite)
    val wrapped = wrap(delegate)
    val batch = wrapped.toBatch()

    assert(wrapped.isInstanceOf[DeltaWrite])
    assert(batch.isInstanceOf[DeltaBatchWrite])
    assert(batch.isInstanceOf[SupportsDeltaBatchWriteRecovery])
    assert(batch.isInstanceOf[SupportsRecoveryTaskMetrics])
    assert(batch.createBatchWriterFactory(TestPhysicalWriteInfo) eq TestDeltaWriterFactory)
    assert(batch.asInstanceOf[SupportsRecoveryTaskMetrics].recoveryTaskMetricSchema() eq
      TestDeltaMetrics.Schema)
    assert(batch.getClass.getMethod("recoveryId").invoke(batch) == "stable-write-id")
    assert(batch.getClass.getMethod("taskCommitStore").invoke(batch) eq TestTaskCommitStore)
  }

  test("wrapper rejects a delta batch that lacks the complete recovery contract") {
    val nonRecoverable = new DeltaBatchWrite {
      override def createBatchWriterFactory(info: PhysicalWriteInfo): DeltaWriterFactory = null
      override def commit(messages: Array[WriterCommitMessage]): Unit = ()
      override def abort(messages: Array[WriterCommitMessage]): Unit = ()
    }

    val error = intercept[IllegalStateException] {
      wrap(new TestDeltaWrite(nonRecoverable)).toBatch()
    }
    assert(error.getMessage.contains(classOf[SupportsDeltaBatchWriteRecovery].getName))
  }

  private def wrap(delegate: DeltaWrite): DeltaWrite = {
    val wrapperClass = Class.forName(
      "org.apache.spark.sql.execution.datasources.v2.RecoveryRequiredDeltaWrite")
    val constructor = wrapperClass.getDeclaredConstructor(
      classOf[DeltaWrite], classOf[String], classOf[String],
      classOf[RecoveryTaskCommitStore])
    constructor.setAccessible(true)
    constructor.newInstance(
      delegate, "test-table", "stable-write-id", TestTaskCommitStore)
      .asInstanceOf[DeltaWrite]
  }
}

private object TestPhysicalWriteInfo extends PhysicalWriteInfo {
  override def numPartitions(): Int = 1
}

private object TestDeltaMetrics {
  val Schema = new RecoveryTaskMetricSchema(
    "delta-recovery-metrics",
    1,
    Array(new RecoveryTaskMetricDescriptor(
      "rows",
      "row-count",
      RecoveryTaskMetricDescriptor.ADDITIVE_AGGREGATION,
      1,
      0L,
      Long.MaxValue)))
}

private object TestDeltaWriterFactory extends RecoveryDeltaWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): RecoveryDeltaWriter = null
}

private object TestDeltaBatchWrite extends SupportsDeltaBatchWriteRecovery {
  override def createBatchWriterFactory(info: PhysicalWriteInfo): RecoveryDeltaWriterFactory =
    TestDeltaWriterFactory

  override def recoveryTaskMetricSchema(): RecoveryTaskMetricSchema = TestDeltaMetrics.Schema

  override def commitMessageCodec(): RecoveryCommitMessageCodec = TestDeltaCommitCodec

  override def recoveryCompatibilityMetadata(info: PhysicalWriteInfo): Array[Byte] = Array(1.toByte)

  override def recover(info: PhysicalWriteInfo): BatchWriteRecoveryState =
    new BatchWriteRecoveryState {
      override def isCommitted(): Boolean = false
    }

  override def abortAfterRecovery(messages: Array[WriterCommitMessage]): Unit = ()
  override def commit(messages: Array[WriterCommitMessage]): Unit = ()
  override def abort(messages: Array[WriterCommitMessage]): Unit = ()
}

private object TestDeltaCommitCodec extends RecoveryCommitMessageCodec {
  override def codecId(): String = "delta-test-codec"
  override def version(): Int = 1
  override def encode(message: WriterCommitMessage): Array[Byte] = Array.emptyByteArray
  override def decode(version: Int, payload: Array[Byte]): WriterCommitMessage = null
}

private class TestDeltaWrite(batch: DeltaBatchWrite) extends DeltaWrite {
  override def toBatch(): DeltaBatchWrite = batch
}

private object TestTaskCommitStore extends RecoveryTaskCommitStore {
  override def capabilities(): RecoveryTaskCommitStore.Capabilities = TestStoreCapabilities

  override def resolveWriteManifest(recoveryId: String, proposedValue: Array[Byte]): Array[Byte] =
    proposedValue

  override def load(
      recoveryId: String,
      partitionIds: Array[Int]): JList[Optional[Array[Byte]]] =
    java.util.Collections.nCopies(partitionIds.length, Optional.empty[Array[Byte]]())

  override def publish(
      recoveryId: String,
      partitionId: Int,
      taskAttemptId: Long,
      attemptNumber: Int,
      value: Array[Byte]): Array[Byte] = value
}

private object TestStoreCapabilities extends RecoveryTaskCommitStore.Capabilities {
  override def semanticsVersion(): Int = RecoveryTaskCommitStore.SEMANTICS_VERSION
  override def maxLoadBatchSize(): Int = 16
  override def maxManifestBytes(): Int = 1024
  override def maxTaskCommitBytes(): Int = 1024
}
