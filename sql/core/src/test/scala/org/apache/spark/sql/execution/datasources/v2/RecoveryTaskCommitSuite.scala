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
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.{List => JList, Optional}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import org.apache.spark.{SparkFunSuite, TaskContext, TaskKilledException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.recovery.RecoveryTaskCommitStore
import org.apache.spark.sql.connector.write.{RecoveryCommitMessageCodec, RecoveryDataWriter,
  RecoveryDataWriterFactory, RecoveryTaskMetricDescriptor, RecoveryTaskMetricSchema,
  RowLevelTaskSummary, WriterCommitMessage}
import org.apache.spark.util.Utils

class RecoveryTaskCommitSuite extends SparkFunSuite {

  private val context = RecoveryTaskCommitContext(
    NoopRecoveryTaskCommitStore, "write-17", IntegerCommitCodec)
  private val metricSchema = new RecoveryTaskMetricSchema(
    "writer-metrics",
    1,
    Array(
      new RecoveryTaskMetricDescriptor("bytes", "byte-count", "sum", 1, 0L, Long.MaxValue),
      new RecoveryTaskMetricDescriptor("files", "file-count", "sum", 1, 0L, 1000L)))
  private val metricContext = context.copy(metricSchema = Some(metricSchema))
  private val rowLevelContext = context.copy(rowLevelSummaryRequired = true)
  private val rowLevelSummary = new RowLevelTaskSummary(10L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)

  test("task commit envelope round trips stable connector payload and row count") {
    val encoded = RecoveryTaskCommitEnvelope.encode(context, 3, IntegerCommit(91), 42L)
    val decoded = RecoveryTaskCommitEnvelope.decode(context, 3, encoded)

    assert(decoded.message === IntegerCommit(91))
    assert(decoded.numRows === 42L)
    assert(decoded.payload.toSeq === IntegerCommitCodec.encode(IntegerCommit(91)).toSeq)
  }

  test("version 2 task envelope round trips ordered durable metrics") {
    val metrics = Seq(TestTaskMetric("files", 3L), TestTaskMetric("bytes", 1024L))
    val encoded = RecoveryTaskCommitEnvelope.encode(
      metricContext, 3, IntegerCommit(91), 42L, metrics)
    val decoded = RecoveryTaskCommitEnvelope.decode(metricContext, 3, encoded)

    assert(decoded.message === IntegerCommit(91))
    assert(decoded.numRows === 42L)
    assert(decoded.metrics === Map("bytes" -> 1024L, "files" -> 3L))
    assert(RecoveryTaskCommitEnvelope.encode(
      metricContext, 3, IntegerCommit(91), 42L, metrics.reverse).toSeq === encoded.toSeq)
  }

  test("task envelope versions fail closed against the required metric schema") {
    val version1 = RecoveryTaskCommitEnvelope.encode(context, 3, IntegerCommit(91), 42L)
    val version2 = RecoveryTaskCommitEnvelope.encode(
      metricContext,
      3,
      IntegerCommit(91),
      42L,
      Seq(TestTaskMetric("bytes", 10L), TestTaskMetric("files", 1L)))

    assert(intercept[IllegalArgumentException] {
      RecoveryTaskCommitEnvelope.decode(metricContext, 3, version1)
    }.getMessage.contains("does not match"))
    assert(intercept[IllegalArgumentException] {
      RecoveryTaskCommitEnvelope.decode(context, 3, version2)
    }.getMessage.contains("does not match"))
  }

  test("version 2 metric values reject missing, duplicate, unknown, and out-of-range data") {
    val validBytes = TestTaskMetric("bytes", 10L)
    val validFiles = TestTaskMetric("files", 1L)
    Seq(
      Seq(validBytes),
      Seq(validBytes, validFiles, TestTaskMetric("other", 1L)),
      Seq(validBytes, validFiles, TestTaskMetric("files", 2L)),
      Seq(validBytes, TestTaskMetric("files", 1001L))).foreach { metrics =>
      intercept[IllegalArgumentException] {
        RecoveryTaskCommitEnvelope.encode(
          metricContext, 3, IntegerCommit(91), 42L, metrics)
      }
    }
  }

  test("version 2 manifest binds complete ordered metric semantics") {
    val metadata = "sink=table-1".getBytes(StandardCharsets.UTF_8)
    val manifest = RecoveryTaskCommitEnvelope.writeManifest(metricContext, 8, metadata)
    val reordered = new RecoveryTaskMetricSchema(
      "writer-metrics", 1, metricSchema.descriptors().reverse)
    val changedSemantics = new RecoveryTaskMetricSchema(
      "writer-metrics",
      1,
      Array(
        new RecoveryTaskMetricDescriptor(
          "bytes", "compressed-byte-count", "sum", 1, 0L, Long.MaxValue),
        metricSchema.descriptors()(1)))

    assert(RecoveryTaskCommitEnvelope.writeManifest(
      metricContext, 8, metadata).toSeq === manifest.toSeq)
    assert(RecoveryTaskCommitEnvelope.writeManifest(
      metricContext.copy(metricSchema = Some(reordered)), 8, metadata).toSeq !== manifest.toSeq)
    assert(RecoveryTaskCommitEnvelope.writeManifest(
      metricContext.copy(metricSchema = Some(changedSemantics)),
      8,
      metadata).toSeq !== manifest.toSeq)
  }

  test("version 3 task envelope round trips an exact row-level task summary") {
    val encoded = RecoveryTaskCommitEnvelope.encode(
      rowLevelContext, 3, IntegerCommit(91), 42L,
      rowLevelSummary = Some(rowLevelSummary))
    val decoded = RecoveryTaskCommitEnvelope.decode(rowLevelContext, 3, encoded)

    assert(decoded.message === IntegerCommit(91))
    assert(decoded.numRows === 42L)
    assert(decoded.metrics.isEmpty)
    assert(decoded.rowLevelSummary.contains(rowLevelSummary))
  }

  test("version 3 task envelope composes row-level summaries with durable metrics") {
    val contextWithMetrics = rowLevelContext.copy(metricSchema = Some(metricSchema))
    val metrics = Seq(TestTaskMetric("files", 3L), TestTaskMetric("bytes", 1024L))
    val encoded = RecoveryTaskCommitEnvelope.encode(
      contextWithMetrics, 3, IntegerCommit(91), 42L, metrics, Some(rowLevelSummary))
    val decoded = RecoveryTaskCommitEnvelope.decode(contextWithMetrics, 3, encoded)

    assert(decoded.metrics === Map("bytes" -> 1024L, "files" -> 3L))
    assert(decoded.rowLevelSummary.contains(rowLevelSummary))
  }

  test("version 3 requires row-level summary presence exactly when declared") {
    intercept[IllegalArgumentException] {
      RecoveryTaskCommitEnvelope.encode(rowLevelContext, 3, IntegerCommit(91), 42L)
    }
    intercept[IllegalArgumentException] {
      RecoveryTaskCommitEnvelope.encode(
        context, 3, IntegerCommit(91), 42L, rowLevelSummary = Some(rowLevelSummary))
    }

    val version3 = RecoveryTaskCommitEnvelope.encode(
      rowLevelContext, 3, IntegerCommit(91), 42L,
      rowLevelSummary = Some(rowLevelSummary))
    assert(intercept[IllegalArgumentException] {
      RecoveryTaskCommitEnvelope.decode(context, 3, version3)
    }.getMessage.contains("does not match"))
  }

  test("version 3 manifest binds row-level summary layout and optional metric schema") {
    val metadata = "sink=table-1".getBytes(StandardCharsets.UTF_8)
    val version1 = RecoveryTaskCommitEnvelope.writeManifest(context, 8, metadata)
    val rowLevel = RecoveryTaskCommitEnvelope.writeManifest(rowLevelContext, 8, metadata)
    val rowLevelWithMetrics = RecoveryTaskCommitEnvelope.writeManifest(
      rowLevelContext.copy(metricSchema = Some(metricSchema)), 8, metadata)

    assert(rowLevel.toSeq !== version1.toSeq)
    assert(rowLevelWithMetrics.toSeq !== rowLevel.toSeq)
    assert(RecoveryTaskCommitEnvelope.writeManifest(
      rowLevelContext, 8, metadata).toSeq === rowLevel.toSeq)
  }

  test("version 4 write manifest binds Spark semantics separately from connector metadata") {
    val connectorMetadata = "connector-generation=7".getBytes(StandardCharsets.UTF_8)
    val sparkManifest = "spark-row-level-generation=9".getBytes(StandardCharsets.UTF_8)
    val manifest = RecoveryTaskCommitEnvelope.writeManifest(
      rowLevelContext, 8, connectorMetadata, Some(sparkManifest))

    assert(RecoveryTaskCommitEnvelope.writeManifest(
      rowLevelContext, 8, connectorMetadata, Some(sparkManifest)).toSeq === manifest.toSeq)
    assert(RecoveryTaskCommitEnvelope.writeManifest(
      rowLevelContext, 8, "connector-generation=8".getBytes(StandardCharsets.UTF_8),
      Some(sparkManifest)).toSeq !== manifest.toSeq)
    assert(RecoveryTaskCommitEnvelope.writeManifest(
      rowLevelContext, 8, connectorMetadata,
      Some("spark-row-level-generation=10".getBytes(StandardCharsets.UTF_8))).toSeq !==
        manifest.toSeq)
    intercept[IllegalArgumentException] {
      RecoveryTaskCommitEnvelope.writeManifest(
        context, 8, connectorMetadata, Some(sparkManifest))
    }
  }

  test("task commit envelope is bound to recovery ID, partition, and codec") {
    val encoded = RecoveryTaskCommitEnvelope.encode(context, 3, IntegerCommit(91), 42L)

    val wrongRecoveryId = context.copy(recoveryId = "write-18")
    assert(intercept[IllegalArgumentException] {
      RecoveryTaskCommitEnvelope.decode(wrongRecoveryId, 3, encoded)
    }.getMessage.contains("expected write-18"))
    assert(intercept[IllegalArgumentException] {
      RecoveryTaskCommitEnvelope.decode(context, 4, encoded)
    }.getMessage.contains("expected 4"))
    val wrongCodec = context.copy(codec = AlternateIntegerCommitCodec)
    assert(intercept[IllegalArgumentException] {
      RecoveryTaskCommitEnvelope.decode(wrongCodec, 3, encoded)
    }.getMessage.contains("expected alternate-integer"))
  }

  test("task commit envelope rejects corruption, truncation, and trailing bytes") {
    val encoded = RecoveryTaskCommitEnvelope.encode(context, 3, IntegerCommit(91), 42L)
    val corrupted = encoded.clone()
    // Every field before the final digest is protected, including the connector payload.
    corrupted(corrupted.length - 33) = (corrupted(corrupted.length - 33) ^ 1).toByte
    assert(intercept[IllegalArgumentException] {
      RecoveryTaskCommitEnvelope.decode(context, 3, corrupted)
    }.getMessage.contains("checksum mismatch"))

    assert(intercept[IllegalArgumentException] {
      RecoveryTaskCommitEnvelope.decode(context, 3, encoded.dropRight(1))
    }.getMessage.contains("checksum mismatch"))
    assert(intercept[IllegalArgumentException] {
      RecoveryTaskCommitEnvelope.decode(context, 3, encoded ++ Array[Byte](0))
    }.getMessage.contains("checksum mismatch"))
  }

  test("task commit envelope checksum protects identity and row-count headers") {
    val encoded = RecoveryTaskCommitEnvelope.encode(context, 3, IntegerCommit(91), 42L)
    val recoveryIdBytes = context.recoveryId.getBytes(StandardCharsets.UTF_8)
    val codecIdBytes = context.codec.codecId().getBytes(StandardCharsets.UTF_8)
    val partitionOffset = 12 + recoveryIdBytes.length
    val codecVersionOffset = partitionOffset + 8 + codecIdBytes.length
    val numRowsOffset = codecVersionOffset + 4

    Seq(partitionOffset, codecVersionOffset, numRowsOffset + 7).foreach { offset =>
      val corrupted = encoded.clone()
      corrupted(offset) = (corrupted(offset) ^ 1).toByte
      assert(intercept[IllegalArgumentException] {
        RecoveryTaskCommitEnvelope.decode(context, 3, corrupted)
      }.getMessage.contains("checksum mismatch"))
    }
  }

  test("recovery identities use strict UTF-8") {
    intercept[IllegalArgumentException] {
      val invalidSurrogate = Character.toString(0xd800.toChar)
      RecoveryTaskCommitEnvelope.validateContext(
        context.copy(recoveryId = s"bad${invalidSurrogate}id"))
    }

    val encoded = RecoveryTaskCommitEnvelope.encode(context, 3, IntegerCommit(91), 42L)
    val forged = encoded.clone()
    forged(12) = 0x80.toByte
    val coreLength = forged.length - 32
    val digest = MessageDigest.getInstance("SHA-256").digest(forged.take(coreLength))
    System.arraycopy(digest, 0, forged, coreLength, digest.length)
    assert(intercept[IllegalArgumentException] {
      RecoveryTaskCommitEnvelope.decode(context, 3, forged)
    }.getMessage.contains("Invalid UTF-8"))
  }

  test("write manifest is deterministic and binds all compatibility inputs") {
    val metadata = "sink=table-1;schema=7;spec=2".getBytes("UTF-8")
    val manifest = RecoveryTaskCommitEnvelope.writeManifest(context, 8, metadata)

    assert(RecoveryTaskCommitEnvelope.writeManifest(context, 8, metadata).toSeq === manifest.toSeq)
    assert(RecoveryTaskCommitEnvelope.writeManifest(context, 9, metadata).toSeq !== manifest.toSeq)
    assert(RecoveryTaskCommitEnvelope.writeManifest(
      context, 8, "sink=table-1;schema=8;spec=2".getBytes("UTF-8")).toSeq !== manifest.toSeq)
    assert(RecoveryTaskCommitEnvelope.writeManifest(
      context.copy(recoveryId = "write-18"), 8, metadata).toSeq !== manifest.toSeq)
    assert(RecoveryTaskCommitEnvelope.writeManifest(
      context.copy(codec = AlternateIntegerCommitCodec), 8, metadata).toSeq !== manifest.toSeq)
  }

  test("recovery envelope rejects invalid connector contracts before storage") {
    intercept[IllegalArgumentException] {
      RecoveryTaskCommitEnvelope.validateContext(context.copy(recoveryId = ""))
    }
    intercept[IllegalArgumentException] {
      RecoveryTaskCommitEnvelope.validateContext(context.copy(codec = ZeroVersionCommitCodec))
    }
    intercept[IllegalArgumentException] {
      RecoveryTaskCommitEnvelope.encode(context, -1, IntegerCommit(1), 0L)
    }
    intercept[IllegalArgumentException] {
      RecoveryTaskCommitEnvelope.encode(context, 0, IntegerCommit(1), -1L)
    }
    intercept[IllegalStateException] {
      RecoveryTaskCommitEnvelope.encode(context.copy(codec = NullEncodingCommitCodec),
        0, IntegerCommit(1), 0L)
    }
  }

  test("recovery store capabilities fail closed and survive executor serialization") {
    val restored = Utils.deserialize[RecoveryTaskCommitContext](Utils.serialize(context))
    assert(restored.recoveryId === context.recoveryId)
    assert(restored.store.capabilities().semanticsVersion() ===
      RecoveryTaskCommitStore.SEMANTICS_VERSION)

    val incompatible = new FixedCapabilitiesStore(new RecoveryTaskCommitStore.Capabilities {
      override def semanticsVersion(): Int = 2
      override def maxLoadBatchSize(): Int = 1024
      override def maxManifestBytes(): Int = 1024
      override def maxTaskCommitBytes(): Int = 1024
    })
    assert(intercept[IllegalArgumentException] {
      RecoveryTaskCommitEnvelope.validateContext(context.copy(store = incompatible))
    }.getMessage.contains("semantics version"))

    val invalidLimit = new FixedCapabilitiesStore(new RecoveryTaskCommitStore.Capabilities {
      override def semanticsVersion(): Int = RecoveryTaskCommitStore.SEMANTICS_VERSION
      override def maxLoadBatchSize(): Int = 0
      override def maxManifestBytes(): Int = 1024
      override def maxTaskCommitBytes(): Int = 1024
    })
    assert(intercept[IllegalArgumentException] {
      RecoveryTaskCommitEnvelope.validateContext(context.copy(store = invalidLimit))
    }.getMessage.contains("batch size"))
  }

  test("executor preflight neither creates a writer nor consumes upstream input") {
    val canonical = RecoveryTaskCommitEnvelope.encode(context, 0, IntegerCommit(91), 42L)
    val store = new TaskProtocolStore(Optional.of(canonical))
    val state = new TaskWriterState

    val result = DataWritingSparkTask.run(
      new TaskWriterFactory(state),
      TaskContext.empty(),
      new Iterator[InternalRow] {
        override def hasNext: Boolean =
          throw new IllegalStateException("executor preflight consumed upstream input")
        override def next(): InternalRow =
          throw new IllegalStateException("executor preflight consumed upstream input")
      },
      useCommitCoordinator = false,
      Map.empty,
      Some(context.copy(store = store)))

    assert(result.writerCommitMessage === IntegerCommit(91))
    assert(result.numRows === 42L)
    assert(state.created.get() === 0)
  }

  test("publish fencing fails closed before acceptance and adopts an accepted commit on retry") {
    val beforeAcceptance = new TaskProtocolStore()
    beforeAcceptance.publishFailure = Some(new RecoveryTaskCommitStore.StoreException(
      RecoveryTaskCommitStore.FailureReason.FENCED, "fenced before acceptance"))
    val firstState = new TaskWriterState
    intercept[RecoveryTaskCommitStore.StoreException] {
      runTask(beforeAcceptance, firstState)
    }
    assert(firstState.committed.get() === 1)
    assert(firstState.aborted.get() === 0,
      "DataWriter.abort has undefined semantics after a successful local commit")
    assert(firstState.closed.get() === 1)
    assert(!beforeAcceptance.loaded.isPresent)

    val acceptedThenFenced = new TaskProtocolStore()
    acceptedThenFenced.acceptBeforeFailure = true
    acceptedThenFenced.publishFailure = Some(new RecoveryTaskCommitStore.StoreException(
      RecoveryTaskCommitStore.FailureReason.FENCED, "fenced after acceptance"))
    val acceptedState = new TaskWriterState
    intercept[RecoveryTaskCommitStore.StoreException] {
      runTask(acceptedThenFenced, acceptedState)
    }
    assert(acceptedThenFenced.loaded.isPresent)

    acceptedThenFenced.publishFailure = None
    val retryState = new TaskWriterState
    val result = DataWritingSparkTask.run(
      new TaskWriterFactory(retryState),
      TaskContext.empty(),
      Iterator.single(InternalRow(1L)),
      useCommitCoordinator = false,
      Map.empty,
      Some(context.copy(store = acceptedThenFenced)))
    assert(result.writerCommitMessage === IntegerCommit(7))
    assert(retryState.created.get() === 0,
      "an accepted publish must be discovered before a retry creates a writer")
  }

  test("null, malformed, and wrong-partition publish responses fail closed") {
    val malformedResponses = Seq[Array[Byte]](
      null,
      Array[Byte](1, 2, 3),
      RecoveryTaskCommitEnvelope.encode(context, 1, IntegerCommit(7), 0L))

    malformedResponses.foreach { response =>
      val store = new TaskProtocolStore()
      if (response == null) {
        store.returnNullFromPublish = true
      } else {
        store.publishResponse = Some(response)
      }
      val state = new TaskWriterState
      intercept[Exception] {
        runTask(store, state)
      }
      assert(state.committed.get() === 1)
      assert(state.aborted.get() === 0)
      assert(state.closed.get() === 1)
      assert(state.discarded.get() === 0)
    }
  }

  test("a losing attempt surfaces discard failure without aborting a committed writer") {
    val canonical = RecoveryTaskCommitEnvelope.encode(context, 0, IntegerCommit(8), 0L)
    val store = new TaskProtocolStore()
    store.publishResponse = Some(canonical)
    val state = new TaskWriterState(failDiscard = true)

    val error = intercept[IllegalStateException] {
      runTask(store, state)
    }
    assert(error.getMessage.contains("injected discard failure"))
    assert(state.committed.get() === 1)
    assert(state.discarded.get() === 1)
    assert(state.aborted.get() === 0)
    assert(state.closed.get() === 1)
  }

  test("concurrent attempts use one canonical commit and discard the CAS loser") {
    val store = new ConcurrentCasTaskStore
    val firstState = new TaskWriterState(localCommitValue = 7)
    val secondState = new TaskWriterState(localCommitValue = 8)
    val firstResult = new AtomicReference[DataWritingSparkTaskResult]
    val secondResult = new AtomicReference[DataWritingSparkTaskResult]
    val firstFailure = new AtomicReference[Throwable]
    val secondFailure = new AtomicReference[Throwable]
    val first = taskThread(store, firstState, firstResult, firstFailure)
    val second = taskThread(store, secondState, secondResult, secondFailure)

    first.start()
    second.start()
    first.join(10000L)
    second.join(10000L)

    assert(!first.isAlive && !second.isAlive, "concurrent recovery attempts did not finish")
    Option(firstFailure.get()).foreach(failure => throw failure)
    Option(secondFailure.get()).foreach(failure => throw failure)
    val canonical = firstResult.get().writerCommitMessage
    assert(canonical === secondResult.get().writerCommitMessage)
    assert(canonical == IntegerCommit(7) || canonical == IntegerCommit(8))
    assert(firstState.committed.get() === 1 && secondState.committed.get() === 1)
    assert(firstState.discarded.get() + secondState.discarded.get() === 1)
    assert(firstState.aborted.get() + secondState.aborted.get() === 0)
    assert(firstState.closed.get() + secondState.closed.get() === 2)
  }

  test("cancellation before local commit aborts the writer and publishes nothing") {
    val store = new TaskProtocolStore()
    val state = new TaskWriterState(
      writeFailure = Some(new TaskKilledException("cancelled before commit")))

    intercept[TaskKilledException] {
      runTask(store, state, Iterator.single(InternalRow(1L)))
    }
    assert(state.created.get() === 1)
    assert(state.committed.get() === 0)
    assert(state.aborted.get() === 1)
    assert(state.closed.get() === 1)
    assert(store.publishes.get() === 0)
    assert(!store.loaded.isPresent)
  }

  test("cancellation after local commit does not call writer abort") {
    val store = new TaskProtocolStore()
    store.publishFailure = Some(new TaskKilledException("cancelled before publish acceptance"))
    val state = new TaskWriterState

    intercept[TaskKilledException] {
      runTask(store, state)
    }
    assert(state.committed.get() === 1)
    assert(state.aborted.get() === 0,
      "DataWriter.abort has undefined semantics after a successful local commit")
    assert(state.closed.get() === 1)
    assert(store.publishes.get() === 1)
    assert(!store.loaded.isPresent)
  }

  test("cancellation after accepted publish is recovered without recomputation") {
    val store = new TaskProtocolStore()
    store.acceptBeforeFailure = true
    store.publishFailure = Some(new TaskKilledException("cancelled after publish acceptance"))
    val firstState = new TaskWriterState
    intercept[TaskKilledException] {
      runTask(store, firstState)
    }
    assert(store.loaded.isPresent)
    assert(firstState.committed.get() === 1)
    assert(firstState.aborted.get() === 0)

    store.publishFailure = None
    val retryState = new TaskWriterState
    val result = DataWritingSparkTask.run(
      new TaskWriterFactory(retryState),
      TaskContext.empty(),
      new Iterator[InternalRow] {
        override def hasNext: Boolean =
          throw new IllegalStateException("retry consumed input after accepted publish")
        override def next(): InternalRow =
          throw new IllegalStateException("retry consumed input after accepted publish")
      },
      useCommitCoordinator = false,
      Map.empty,
      Some(context.copy(store = store)))
    assert(result.writerCommitMessage === IntegerCommit(7))
    assert(retryState.created.get() === 0)
  }

  private def runTask(
      store: TaskProtocolStore,
      state: TaskWriterState,
      input: Iterator[InternalRow] = Iterator.empty): DataWritingSparkTaskResult = {
    DataWritingSparkTask.run(
      new TaskWriterFactory(state),
      TaskContext.empty(),
      input,
      useCommitCoordinator = false,
      Map.empty,
      Some(context.copy(store = store)))
  }

  private def taskThread(
      store: RecoveryTaskCommitStore,
      state: TaskWriterState,
      result: AtomicReference[DataWritingSparkTaskResult],
      failure: AtomicReference[Throwable]): Thread = {
    new Thread(() => {
      try {
        result.set(DataWritingSparkTask.run(
          new TaskWriterFactory(state), TaskContext.empty(), Iterator.empty,
          useCommitCoordinator = false, Map.empty, Some(context.copy(store = store))))
      } catch {
        case thrown: Throwable => failure.set(thrown)
      }
    })
  }
}

private case class IntegerCommit(value: Int) extends WriterCommitMessage

private case class TestTaskMetric(metricName: String, metricValue: Long)
  extends CustomTaskMetric {
  override def name(): String = metricName
  override def value(): Long = metricValue
}

private class TaskWriterState(
    val failDiscard: Boolean = false,
    val localCommitValue: Int = 7,
    val writeFailure: Option[RuntimeException] = None) {
  val created = new AtomicInteger
  val committed = new AtomicInteger
  val aborted = new AtomicInteger
  val closed = new AtomicInteger
  val discarded = new AtomicInteger
}

private class TaskWriterFactory(state: TaskWriterState) extends RecoveryDataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): RecoveryDataWriter = {
    state.created.incrementAndGet()
    new RecoveryDataWriter {
      override def write(record: InternalRow): Unit =
        state.writeFailure.foreach(failure => throw failure)
      override def commit(): WriterCommitMessage = {
        state.committed.incrementAndGet()
        IntegerCommit(state.localCommitValue)
      }
      override def abort(): Unit = state.aborted.incrementAndGet()
      override def close(): Unit = state.closed.incrementAndGet()
      override def discardCommittedOutput(committedMessage: WriterCommitMessage): Unit = {
        state.discarded.incrementAndGet()
        if (state.failDiscard) {
          throw new IllegalStateException("injected discard failure")
        }
      }
    }
  }
}

private class TaskProtocolStore(
    var loaded: Optional[Array[Byte]] = Optional.empty[Array[Byte]]())
  extends RecoveryTaskCommitStore {
  var publishResponse: Option[Array[Byte]] = None
  var publishFailure: Option[RuntimeException] = None
  var acceptBeforeFailure: Boolean = false
  var returnNullFromPublish: Boolean = false
  val publishes = new AtomicInteger

  override def capabilities(): RecoveryTaskCommitStore.Capabilities =
    RecoveryTaskCommitTestStoreCapabilities
  override def resolveWriteManifest(
      recoveryId: String,
      proposedValue: Array[Byte]): Array[Byte] = proposedValue
  override def load(
      recoveryId: String,
      partitionIds: Array[Int]): JList[Optional[Array[Byte]]] =
    java.util.Collections.nCopies(partitionIds.length, loaded)
  override def publish(
      recoveryId: String,
      partitionId: Int,
      taskAttemptId: Long,
      attemptNumber: Int,
      value: Array[Byte]): Array[Byte] = {
    publishes.incrementAndGet()
    if (acceptBeforeFailure) {
      loaded = Optional.of(value.clone())
    }
    publishFailure.foreach(failure => throw failure)
    if (returnNullFromPublish) null else publishResponse.getOrElse(value)
  }
}

private class ConcurrentCasTaskStore extends RecoveryTaskCommitStore {
  private val preflight = new CountDownLatch(2)
  private val publishBarrier = new CountDownLatch(2)
  private val canonical = new AtomicReference[Array[Byte]]

  override def capabilities(): RecoveryTaskCommitStore.Capabilities =
    RecoveryTaskCommitTestStoreCapabilities
  override def resolveWriteManifest(
      recoveryId: String,
      proposedValue: Array[Byte]): Array[Byte] = proposedValue
  override def load(
      recoveryId: String,
      partitionIds: Array[Int]): JList[Optional[Array[Byte]]] = {
    preflight.countDown()
    require(preflight.await(10L, TimeUnit.SECONDS), "concurrent preflight timed out")
    val value = canonical.get()
    val result = if (value == null) {
      Optional.empty[Array[Byte]]()
    } else {
      Optional.of(value.clone())
    }
    java.util.Collections.nCopies(partitionIds.length, result)
  }
  override def publish(
      recoveryId: String,
      partitionId: Int,
      taskAttemptId: Long,
      attemptNumber: Int,
      value: Array[Byte]): Array[Byte] = {
    publishBarrier.countDown()
    require(publishBarrier.await(10L, TimeUnit.SECONDS), "concurrent publish timed out")
    canonical.compareAndSet(null, value.clone())
    canonical.get().clone()
  }
}

private object IntegerCommitCodec extends RecoveryCommitMessageCodec {
  override def codecId(): String = "integer"
  override def version(): Int = 1
  override def encode(message: WriterCommitMessage): Array[Byte] =
    ByteBuffer.allocate(4).putInt(message.asInstanceOf[IntegerCommit].value).array()
  override def decode(version: Int, payload: Array[Byte]): WriterCommitMessage = {
    require(version == 1 && payload.length == 4)
    IntegerCommit(ByteBuffer.wrap(payload).getInt)
  }
}

private object AlternateIntegerCommitCodec extends RecoveryCommitMessageCodec {
  override def codecId(): String = "alternate-integer"
  override def version(): Int = 1
  override def encode(message: WriterCommitMessage): Array[Byte] =
    IntegerCommitCodec.encode(message)
  override def decode(version: Int, payload: Array[Byte]): WriterCommitMessage =
    IntegerCommitCodec.decode(version, payload)
}

private object ZeroVersionCommitCodec extends RecoveryCommitMessageCodec {
  override def codecId(): String = "zero-version"
  override def version(): Int = 0
  override def encode(message: WriterCommitMessage): Array[Byte] = Array.emptyByteArray
  override def decode(version: Int, payload: Array[Byte]): WriterCommitMessage = IntegerCommit(0)
}

private object NullEncodingCommitCodec extends RecoveryCommitMessageCodec {
  override def codecId(): String = "null-encoding"
  override def version(): Int = 1
  override def encode(message: WriterCommitMessage): Array[Byte] = null
  override def decode(version: Int, payload: Array[Byte]): WriterCommitMessage = IntegerCommit(0)
}

private object NoopRecoveryTaskCommitStore extends RecoveryTaskCommitStore {
  override def capabilities(): RecoveryTaskCommitStore.Capabilities =
    RecoveryTaskCommitTestStoreCapabilities
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

private class FixedCapabilitiesStore(
    fixedCapabilities: RecoveryTaskCommitStore.Capabilities)
  extends RecoveryTaskCommitStore {
  override def capabilities(): RecoveryTaskCommitStore.Capabilities = fixedCapabilities
  override def resolveWriteManifest(
      recoveryId: String,
      proposedValue: Array[Byte]): Array[Byte] = proposedValue
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

private object RecoveryTaskCommitTestStoreCapabilities
  extends RecoveryTaskCommitStore.Capabilities {
  override def semanticsVersion(): Int = RecoveryTaskCommitStore.SEMANTICS_VERSION
  override def maxLoadBatchSize(): Int = 1024
  override def maxManifestBytes(): Int = 2 * 1024 * 1024
  override def maxTaskCommitBytes(): Int = 32 * 1024 * 1024
}
