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

import java.nio.ByteBuffer
import java.util
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.jdk.CollectionConverters._

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.transactions.Transaction
import org.apache.spark.sql.connector.recovery.RecoveryTaskCommitStore
import org.apache.spark.sql.connector.write.{BatchWriteRecoveryState, DataWriter,
  DataWriterFactory, PhysicalWriteInfo, RecoveryCommitMessageCodec, RecoveryDataWriter,
  RecoveryDataWriterFactory, RowLevelOperation, RowLevelTaskSummary, SupportsBatchWriteRecovery,
  UpdateSummary, Write, WriterCommitMessage}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class RowLevelTaskRecoveryStateSuite extends SharedSparkSession {

  override def beforeEach(): Unit = {
    super.beforeEach()
    RowLevelRecoveryTestStore.clear()
    RowLevelRecoveryWriterFactory.reset()
  }

  test("a globally committed row-level write requires an authoritative total summary") {
    val state = new FixedRowLevelRecoveryState(committed = true, Optional.empty())
    val exec = runRecoveryTask(new FixedRowLevelWritingTask(Some(RowLevelTaskSummary.empty())), state)

    val error = intercept[Exception] {
      exec.executeCollect()
    }
    assert(Utils.exceptionString(error).contains("no authoritative summary"))
    assert(exec.restored.get() == null)
  }

  test("a recovery task without its required row-level summary fails closed") {
    val state = new FixedRowLevelRecoveryState(committed = false, Optional.empty())
    val exec = runRecoveryTask(new FixedRowLevelWritingTask(None), state)

    val error = intercept[Exception] {
      exec.executeCollect()
    }
    assert(Utils.exceptionString(error).contains("row-level task summary presence false"))
    assert(exec.restored.get() == null)
  }

  test("row-level task summary aggregation rejects overflow") {
    val state = new FixedRowLevelRecoveryState(committed = false, Optional.empty())
    val exec = runRecoveryTask(OverflowRowLevelWritingTask, state)

    val error = intercept[Exception] {
      exec.executeCollect()
    }
    assert(Utils.exceptionString(error).contains(classOf[ArithmeticException].getName))
    assert(exec.restored.get() == null)
  }

  test("canonical per-partition row-level summaries are restored exactly once") {
    val state = new FixedRowLevelRecoveryState(committed = false, Optional.empty())
    val perTask = new RowLevelTaskSummary(10L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)
    val exec = runRecoveryTask(new FixedRowLevelWritingTask(Some(perTask)), state)

    exec.executeCollect()

    assert(exec.restored.get() === perTask.plus(perTask))
  }

  test("a globally committed authoritative row-level summary is restored exactly") {
    val total = new RowLevelTaskSummary(20L, 4L, 6L, 8L, 10L, 12L, 14L, 16L, 18L)
    val state = new FixedRowLevelRecoveryState(committed = true, Optional.of(total))
    val exec = runRecoveryTask(new FixedRowLevelWritingTask(None), state)

    exec.executeCollect()

    assert(exec.restored.get() === total)
  }

  test("a changed Spark-owned row-level generation manifest fails before task adoption") {
    val state = new FixedRowLevelRecoveryState(committed = false, Optional.empty())
    val summary = Some(RowLevelTaskSummary.empty())
    runRecoveryTask(
      new FixedRowLevelWritingTask(summary),
      state,
      Some(baseManifestInput)).executeCollect()

    val changed = baseManifestInput.copy(command = "UPDATE")
    val replacement = runRecoveryTask(new FixedRowLevelWritingTask(summary), state, Some(changed))
    val error = intercept[Exception] {
      replacement.executeCollect()
    }
    assert(Utils.exceptionString(error).contains("durable manifest"))
    assert(replacement.restored.get() == null)
  }

  test("a replacement row-level command adopts every canonical task without writer creation") {
    val state = new FixedRowLevelRecoveryState(committed = false, Optional.empty())
    val perTask = new RowLevelTaskSummary(5L, 2L, 0L, 3L, 0L, 0L, 0L, 0L, 0L)
    val query = spark.range(0L, 2L, 1L, 2).queryExecution.executedPlan

    val first = RowLevelRecoveryCommandTestExec(
      query,
      new RowLevelRecoveryTestWrite(state, Some(baseManifestInput.copy(command = "UPDATE"))),
      new FixedRowLevelWritingTask(Some(perTask)))
    first.executeCollect()
    assert(RowLevelRecoveryWriterFactory.writersCreated.get() === 2)
    assert(first.currentUpdateSummary === (6L, 4L))

    val replacement = RowLevelRecoveryCommandTestExec(
      query,
      new RowLevelRecoveryTestWrite(state, Some(baseManifestInput.copy(command = "UPDATE"))),
      new FixedRowLevelWritingTask(Some(perTask)))
    replacement.executeCollect()
    assert(RowLevelRecoveryWriterFactory.writersCreated.get() === 2,
      "replacement driver must not construct writers for canonical task commits")
    assert(replacement.currentUpdateSummary === (6L, 4L))
  }

  test("a physical row-level recovery command without a Spark manifest fails before writers") {
    val query = spark.range(0L, 2L, 1L, 2).queryExecution.executedPlan
    val exec = RowLevelRecoveryCommandTestExec(
      query,
      new RowLevelRecoveryTestWrite(
        new FixedRowLevelRecoveryState(committed = false, Optional.empty()), None),
      new FixedRowLevelWritingTask(Some(RowLevelTaskSummary.empty())))

    val error = intercept[Exception] {
      exec.executeCollect()
    }
    assert(Utils.exceptionString(error).contains("Spark-owned generation manifest"))
    assert(RowLevelRecoveryWriterFactory.writersCreated.get() === 0)
  }

  test("a row-level manifest with the wrong physical mode fails before writers") {
    val query = spark.range(0L, 2L, 1L, 2).queryExecution.executedPlan
    val wrongMode = baseManifestInput.copy(command = "UPDATE", physicalMode = "WRITE_DELTA")
    val exec = RowLevelRecoveryCommandTestExec(
      query,
      new RowLevelRecoveryTestWrite(
        new FixedRowLevelRecoveryState(committed = false, Optional.empty()), Some(wrongMode)),
      new FixedRowLevelWritingTask(Some(RowLevelTaskSummary.empty())))

    val error = intercept[Exception] {
      exec.executeCollect()
    }
    assert(Utils.exceptionString(error).contains("does not match physical mode"))
    assert(RowLevelRecoveryWriterFactory.writersCreated.get() === 0)
  }

  private def runRecoveryTask(
      task: WritingSparkTask[DataWriter[InternalRow]],
      state: BatchWriteRecoveryState,
      manifestInput: Option[RowLevelWriteManifestInput] = None): RowLevelRecoveryTestExec = {
    val query = spark.range(0L, 2L, 1L, 2).queryExecution.executedPlan
    RowLevelRecoveryTestExec(
      query, new RowLevelRecoveryTestBatch(state, manifestInput), task,
      new AtomicReference[RowLevelTaskSummary])
  }

  private def baseManifestInput: RowLevelWriteManifestInput = RowLevelWriteManifestInput(
    recoveryExecutionId = "execution-1",
    recoveryId = "row-level-recovery-test",
    generation = "generation-1",
    sinkId = "sink-1",
    catalogIdentity = "catalog",
    tableName = "catalog.ns.table",
    command = "DELETE",
    physicalMode = "REPLACE_DATA",
    canonicalOperationSha256 = Array.fill(32)(1.toByte),
    conditionSha256 = Array.fill(32)(2.toByte),
    conflictFilterSha256 = None,
    inputSchemaJson = Some("{\"type\":\"struct\",\"fields\":[]}"),
    outputSchemaJson = Some("{\"type\":\"struct\",\"fields\":[]}"),
    rowSchemaJson = Some("{\"type\":\"struct\",\"fields\":[]}"),
    rowIdSchemaJson = None,
    metadataSchemaJson = None,
    distributionDescription = "unspecified",
    distributionStrictlyRequired = false,
    requiredNumPartitions = 0,
    advisoryPartitionSizeInBytes = 0L,
    orderingDescriptions = Seq.empty,
    sourceAnchors = Seq(RowLevelWriteSourceAnchor("source-1", "snapshot-1")),
    transactionId = None)
}

private case class RowLevelRecoveryCommandTestExec(
    query: SparkPlan,
    write: Write,
    task: WritingSparkTask[DataWriter[InternalRow]],
    transaction: Option[Transaction] = None) extends RowLevelWriteExec {

  override val rowLevelCommand: RowLevelOperation.Command = RowLevelOperation.Command.UPDATE
  override private[v2] val rowLevelPhysicalMode: String = "REPLACE_DATA"
  override val refreshCache: () => Unit = () => {}
  override val tableName: String = "catalog.ns.table"
  override def writingTask: WritingSparkTask[_] = task
  override def withTransaction(txn: Option[Transaction]): RowLevelRecoveryCommandTestExec =
    copy(transaction = txn)
  override protected def withNewChildInternal(
      newChild: SparkPlan): RowLevelRecoveryCommandTestExec = copy(query = newChild)

  def currentUpdateSummary: (Long, Long) = {
    val summary = getWriteSummary().get.asInstanceOf[UpdateSummary]
    summary.numUpdatedRows() -> summary.numCopiedRows()
  }
}

private case class RowLevelRecoveryTestExec(
    query: SparkPlan,
    batchWrite: SupportsBatchWriteRecovery,
    task: WritingSparkTask[DataWriter[InternalRow]],
    restored: AtomicReference[RowLevelTaskSummary]) extends V2TableWriteExec {

  override def writingTask: WritingSparkTask[_] = task
  override protected def requiresRowLevelTaskSummary: Boolean = true
  override protected def restoreRowLevelTaskSummary(summary: RowLevelTaskSummary): Unit = {
    require(restored.compareAndSet(null, summary), "row-level summary was restored more than once")
  }
  override protected def run(): Seq[InternalRow] = writeWithV2(batchWrite)
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(query = newChild)
}

private class FixedRowLevelRecoveryState(
    committed: Boolean,
    summary: Optional[RowLevelTaskSummary]) extends BatchWriteRecoveryState {
  override def isCommitted(): Boolean = committed
  override def totalNumRows(): Long = if (committed) 2L else -1L
  override def totalRowLevelSummary(): Optional[RowLevelTaskSummary] = summary
}

private case class RowLevelRecoveryCommit(value: Int) extends WriterCommitMessage

private object RowLevelRecoveryCodec extends RecoveryCommitMessageCodec {
  override def codecId(): String = "row-level-recovery-test"
  override def version(): Int = 1
  override def encode(message: WriterCommitMessage): Array[Byte] =
    ByteBuffer.allocate(4).putInt(message.asInstanceOf[RowLevelRecoveryCommit].value).array()
  override def decode(version: Int, payload: Array[Byte]): WriterCommitMessage = {
    require(version == 1 && payload.length == 4)
    RowLevelRecoveryCommit(ByteBuffer.wrap(payload).getInt)
  }
}

private class RowLevelRecoveryTestBatch(
    state: BatchWriteRecoveryState,
    override val rowLevelManifestInput: Option[RowLevelWriteManifestInput] = None)
  extends SupportsBatchWriteRecovery with HasRecoveryTaskCommitStore {
  override val recoveryId: String = "row-level-recovery-test"
  override val taskCommitStore: RecoveryTaskCommitStore = RowLevelRecoveryTestStore
  override def commitMessageCodec(): RecoveryCommitMessageCodec = RowLevelRecoveryCodec
  override def recoveryCompatibilityMetadata(info: PhysicalWriteInfo): Array[Byte] =
    s"partitions=${info.numPartitions()}".getBytes("UTF-8")
  override def recover(info: PhysicalWriteInfo): BatchWriteRecoveryState = state
  override def abortAfterRecovery(messages: Array[WriterCommitMessage]): Unit = {}
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    RowLevelRecoveryWriterFactory
  override def commit(messages: Array[WriterCommitMessage]): Unit = {}
  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

private class RowLevelRecoveryTestWrite(
    state: BatchWriteRecoveryState,
    manifestInput: Option[RowLevelWriteManifestInput]) extends Write {
  override def toBatch(): SupportsBatchWriteRecovery =
    new RowLevelRecoveryTestBatch(state, manifestInput)
}

private object RowLevelRecoveryWriterFactory extends RecoveryDataWriterFactory {
  val writersCreated = new AtomicInteger()

  def reset(): Unit = writersCreated.set(0)

  override def createWriter(partitionId: Int, taskId: Long): RecoveryDataWriter =
    {
      writersCreated.incrementAndGet()
      new RecoveryDataWriter {
      override def write(record: InternalRow): Unit = {}
      override def commit(): WriterCommitMessage = RowLevelRecoveryCommit(partitionId)
      override def abort(): Unit = {}
      override def close(): Unit = {}
      override def discardCommittedOutput(committedMessage: WriterCommitMessage): Unit = {}
      }
    }
}

private class FixedRowLevelWritingTask(summary: Option[RowLevelTaskSummary])
  extends WritingSparkTask[DataWriter[InternalRow]] {
  override protected def write(
      writer: DataWriter[InternalRow],
      iter: java.util.Iterator[InternalRow]): Unit = while (iter.hasNext) writer.write(iter.next())
  override protected def rowLevelTaskSummary(): Option[RowLevelTaskSummary] = summary
}

private object OverflowRowLevelWritingTask extends WritingSparkTask[DataWriter[InternalRow]] {
  override protected def write(
      writer: DataWriter[InternalRow],
      iter: java.util.Iterator[InternalRow]): Unit = while (iter.hasNext) writer.write(iter.next())
  override protected def rowLevelTaskSummary(): Option[RowLevelTaskSummary] = {
    val scanned = if (TaskContext.get().partitionId() == 0) Long.MaxValue else 1L
    Some(new RowLevelTaskSummary(scanned, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L))
  }
}

private object RowLevelRecoveryTestStore extends RecoveryTaskCommitStore {
  private val records = new ConcurrentHashMap[Int, Array[Byte]]
  private val manifest = new AtomicReference[Array[Byte]]

  def clear(): Unit = {
    records.clear()
    manifest.set(null)
  }

  override def capabilities(): RecoveryTaskCommitStore.Capabilities =
    RowLevelRecoveryStoreCapabilities
  override def resolveWriteManifest(
      recoveryId: String,
      proposedValue: Array[Byte]): Array[Byte] = {
    manifest.compareAndSet(null, proposedValue.clone())
    manifest.get().clone()
  }
  override def load(
      recoveryId: String,
      partitionIds: Array[Int]): util.List[Optional[Array[Byte]]] = {
    partitionIds.iterator.map { id =>
      Option(records.get(id)).map(value => Optional.of(value.clone()))
        .getOrElse(Optional.empty[Array[Byte]]())
    }.toSeq.asJava
  }
  override def publish(
      recoveryId: String,
      partitionId: Int,
      taskAttemptId: Long,
      attemptNumber: Int,
      value: Array[Byte]): Array[Byte] = {
    records.putIfAbsent(partitionId, value.clone())
    records.get(partitionId).clone()
  }
}

private object RowLevelRecoveryStoreCapabilities extends RecoveryTaskCommitStore.Capabilities {
  override def semanticsVersion(): Int = RecoveryTaskCommitStore.SEMANTICS_VERSION
  override def maxLoadBatchSize(): Int = 1024
  override def maxManifestBytes(): Int = 2 * 1024 * 1024
  override def maxTaskCommitBytes(): Int = 32 * 1024 * 1024
}
