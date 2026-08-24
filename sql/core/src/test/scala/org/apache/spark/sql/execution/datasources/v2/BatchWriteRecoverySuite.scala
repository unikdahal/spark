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

import java.io.{File, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{SourceRecoveryInfo, WriteRecoveryInfo}
import org.apache.spark.sql.connector.catalog.{SupportsRecoveryWrite, SupportsWrite, Table,
  TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.recovery.RecoveryTaskCommitStore
import org.apache.spark.sql.connector.write.{BatchWrite, BatchWriteRecoveryState, DataWriterFactory,
  LogicalWriteInfo, PhysicalWriteInfo, RecoveryCommitMessageCodec, RecoveryDataWriter,
  RecoveryDataWriterFactory, RecoveryTaskMetricDescriptor, RecoveryTaskMetricSchema,
  SupportsRecoveryTaskMetrics, Write, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.execution.{CommandResultExec, QueryExecution}
import org.apache.spark.sql.execution.adaptive.{RecoveredShuffleStage, ShuffleStageRecovery,
  ShuffleStageRecoveryInfo}
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, QueryExecutionListener}
import org.apache.spark.util.Utils

/**
 * End-to-end tests for recoverable V2 batch writes across a driver replacement.
 *
 * Each test runs one write in a session, discards that session entirely, and runs the same write
 * again in a brand new session that shares only durable state on disk: the recovery task commit
 * store and the sink directory. That is the boundary a replacement driver actually sees, since no
 * scheduler state, catalog, or in-memory commit message survives.
 *
 * The durable store used here is a real immutable compare-and-set over a directory, so a losing
 * attempt genuinely loses. Faults are injected through it, which is where every interesting failure
 * of the protocol occurs: a publish accepted but not acknowledged, a corrupt record, or a
 * replacement driver whose write no longer matches the manifest.
 *
 * A test cannot kill its own JVM, so driver death is modelled as a failed write followed by a
 * discarded session. Process-level kill coverage belongs in an integration test.
 */
class BatchWriteRecoverySuite extends SparkFunSuite {

  private var storeDir: File = _
  private var sinkDir: File = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    storeDir = Utils.createTempDir()
    sinkDir = Utils.createTempDir()
    // The sink creates its own directory, so a run cannot silently inherit files from a previous
    // test through a directory that already exists.
    Utils.deleteRecursively(sinkDir)
    TestWriterCounters.reset()
  }

  override def afterEach(): Unit = {
    try {
      Utils.deleteRecursively(storeDir)
      Utils.deleteRecursively(sinkDir)
    } finally {
      super.afterEach()
    }
  }

  private def withRecoveringSession[T](fault: StoreFault = NoFault)(body: SparkSession => T): T = {
    val store = new DirectoryRecoveryTaskCommitStore(storeDir.getAbsolutePath, fault)
    val session = SparkSession.builder()
      // Four task attempts: the lost-reply scenario depends on Spark retrying a failed task, and
      // a plain "local[N]" master allows exactly one attempt.
      .master("local[2, 4]")
      .appName("batch-write-recovery")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .withExtensions { extensions =>
        extensions.injectShuffleStageRecovery { _ =>
          new TestRecoveryProvider(storeDir.toPath, store)
        }
      }
      .getOrCreate()
    try {
      body(session)
    } finally {
      session.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  private def runWrite(
      session: SparkSession,
      numPartitions: Int,
      numRows: Long): V2TableWriteExec = {
    val captured = new AtomicReference[V2TableWriteExec]()
    val listener = new QueryExecutionListener {
      override def onSuccess(
          funcName: String, qe: QueryExecution, durationNs: Long): Unit = capture(qe)

      override def onFailure(
          funcName: String, qe: QueryExecution, exception: Exception): Unit = capture(qe)

      private def capture(qe: QueryExecution): Unit = {
        val commandPlan = qe.executedPlan match {
          case command: CommandResultExec => command.commandPhysicalPlan
          case plan => plan
        }
        commandPlan.collectFirst { case write: V2TableWriteExec => write }
          .foreach(write => captured.compareAndSet(null, write))
      }
    }
    session.listenerManager.register(listener)
    try {
      session.range(0, numRows)
        .repartition(numPartitions)
        .write
        .format(classOf[TestRecoverableSinkProvider].getName)
        .option("path", sinkDir.getAbsolutePath)
        .option("sinkId", "test-sink")
        .mode("append")
        .save()
      session.sparkContext.listenerBus.waitUntilEmpty()
      Option(captured.get()).getOrElse {
        fail("the query execution listener did not capture V2TableWriteExec")
      }
    } finally {
      session.listenerManager.unregister(listener)
    }
  }

  private def committedRows(): Long = {
    val marker = new File(sinkDir, TestSinkPaths.CommittedMarker)
    assert(marker.exists(), s"the sink never committed: ${marker.getAbsolutePath}")
    Files.readAllLines(marker.toPath).asScala
      .flatMap(_.split(" ").find(_.startsWith("rows=")))
      .map(_.stripPrefix("rows=").toLong)
      .sum
  }

  private def durableCommits(): Int =
    Option(storeDir.listFiles())
      .map(_.count(file => file.getName.endsWith(".record") && !file.getName.contains("manifest")))
      .getOrElse(0)

  test("a replacement driver writes only the partitions without a durable commit") {
    val accepted = 2
    intercept[Exception] {
      withRecoveringSession(FailAfterPublishes(accepted)) { session =>
        runWrite(session, numPartitions = 6, numRows = 600)
      }
    }
    assert(durableCommits() === accepted,
      "the failed write must leave exactly the accepted commits durable")

    TestWriterCounters.reset()
    val replacementExec = withRecoveringSession() { session =>
      runWrite(session, numPartitions = 6, numRows = 600)
    }
    assert(TestWriterCounters.writersCreated.get() === 6 - accepted,
      "the replacement driver must create writers only for partitions with no durable commit")
    assert(replacementExec.customMetrics(TestRecoveryMetrics.Rows).value === 600L,
      "recovered and newly written partition metrics must be aggregated exactly once")
    assert(committedRows() === 600L)
  }

  test("an executor adopts the canonical commit when its publish reply is lost") {
    val writeExec = withRecoveringSession(DropReplyAfterPublish(1)) { session =>
      runWrite(session, numPartitions = 4, numRows = 400)
    }
    assert(TestWriterCounters.writersCreated.get() === 4,
      "the retry of the partition whose reply was lost must not create a second writer")
    assert(TestWriterCounters.preflightHits.get() === 1,
      "the retry must observe the canonical commit before creating a writer")
    assert(TestWriterCounters.driverCommitCallbacks.get() === 0,
      "recovery writes must not replay non-durable per-task driver callbacks")
    assert(writeExec.customMetrics(TestRecoveryMetrics.Rows).value === 400L,
      "a retry that adopts an accepted commit must not add its metric twice")
    assert(committedRows() === 400L)
  }

  test("a fully committed write skips every writer task and the global commit") {
    withRecoveringSession() { session =>
      runWrite(session, numPartitions = 4, numRows = 400)
    }
    val marker = new File(sinkDir, TestSinkPaths.CommittedMarker).toPath
    val firstCommit = Files.readAllBytes(marker)

    TestWriterCounters.reset()
    withRecoveringSession() { session =>
      runWrite(session, numPartitions = 4, numRows = 400)
    }
    assert(TestWriterCounters.writersCreated.get() === 0,
      "an already committed write must not create any writer")
    assert(TestWriterCounters.globalCommits.get() === 0,
      "an already committed write must not commit again")
    assert(util.Arrays.equals(firstCommit, Files.readAllBytes(marker)))
  }

  test("a globally committed write does not depend on task-record lookup availability") {
    withRecoveringSession() { session =>
      runWrite(session, numPartitions = 4, numRows = 400)
    }

    TestWriterCounters.reset()
    val recoveredExec = withRecoveringSession(
        FailLoad(RecoveryTaskCommitStore.FailureReason.UNAVAILABLE)) { session =>
      runWrite(session, numPartitions = 4, numRows = 400)
    }
    assert(TestWriterCounters.writersCreated.get() === 0)
    assert(TestWriterCounters.globalCommits.get() === 0)
    assert(recoveredExec.customMetrics(TestRecoveryMetrics.Rows).value === 400L,
      "a globally committed write must restore connector-owned durable metric totals")
  }

  test("a globally committed write fails closed when durable metric totals are missing") {
    withRecoveringSession() { session =>
      runWrite(session, numPartitions = 4, numRows = 400)
    }

    TestWriterCounters.reset()
    TestWriterCounters.omitCommittedMetricTotals.set(true)
    val error = intercept[Exception] {
      withRecoveringSession() { session =>
        runWrite(session, numPartitions = 4, numRows = 400)
      }
    }
    assert(Utils.exceptionString(error).contains("do not match the durable schema"),
      "expected missing durable metric totals to fail closed, got: " +
        Utils.exceptionString(error))
    assert(TestWriterCounters.writersCreated.get() === 0)
    assert(TestWriterCounters.globalCommits.get() === 0)
  }

  test("a replacement driver with a different partition count fails closed") {
    intercept[Exception] {
      withRecoveringSession(FailAfterPublishes(2)) { session =>
        runWrite(session, numPartitions = 6, numRows = 600)
      }
    }
    val error = intercept[Exception] {
      withRecoveringSession() { session =>
        runWrite(session, numPartitions = 8, numRows = 600)
      }
    }
    assert(Utils.exceptionString(error).contains("manifest"),
      s"expected a manifest mismatch, got: ${Utils.exceptionString(error)}")
  }

  test("a corrupt durable record fails closed") {
    intercept[Exception] {
      withRecoveringSession(FailAfterPublishes(2)) { session =>
        runWrite(session, numPartitions = 6, numRows = 600)
      }
    }
    val error = intercept[Exception] {
      withRecoveringSession(CorruptOnRead) { session =>
        runWrite(session, numPartitions = 6, numRows = 600)
      }
    }
    assert(Utils.exceptionString(error).contains("checksum"),
      s"expected a checksum failure, got: ${Utils.exceptionString(error)}")
  }

  test("non-authoritative store failures never start a writer") {
    Seq(
      RecoveryTaskCommitStore.FailureReason.UNAVAILABLE,
      RecoveryTaskCommitStore.FailureReason.FENCED,
      RecoveryTaskCommitStore.FailureReason.AMBIGUOUS).foreach { reason =>
      TestWriterCounters.reset()
      intercept[Exception] {
        withRecoveringSession(FailLoad(reason)) { session =>
          runWrite(session, numPartitions = 2, numRows = 20)
        }
      }
      assert(TestWriterCounters.writersCreated.get() === 0,
        s"$reason store failure must happen before writer creation")
    }
  }
}

/** A fault injected into the durable store. None of these weaken what the store guarantees. */
private sealed trait StoreFault extends Serializable {
  def beforeLoad(): Unit = {}
  def beforePublish(partitionId: Int): Unit = {}
  def afterPublish(partitionId: Int): Unit = {}
  def onRead(partitionId: Int, value: Array[Byte]): Array[Byte] = value
}

private case object NoFault extends StoreFault

/** Fails the publish following `accepted` successful ones, leaving those accepted ones durable. */
private case class FailAfterPublishes(accepted: Int) extends StoreFault {
  override def beforePublish(partitionId: Int): Unit = {
    if (TestWriterCounters.publishes.get() >= accepted) {
      throw new IOException(s"injected publish failure after $accepted accepted commits")
    }
  }
}

/** Accepts the publish and then loses the answer, exactly once. */
private case class DropReplyAfterPublish(target: Int) extends StoreFault {
  override def afterPublish(partitionId: Int): Unit = {
    if (partitionId == target && TestWriterCounters.droppedReplies.getAndIncrement() == 0) {
      throw new IOException(s"injected lost reply after accepting partition $partitionId")
    }
  }
}

/** Corrupts a byte inside the connector payload, the region the envelope digest covers. */
private case object CorruptOnRead extends StoreFault {
  override def onRead(partitionId: Int, value: Array[Byte]): Array[Byte] = {
    val corrupted = value.clone()
    val index = math.max(0, corrupted.length - 33)
    corrupted(index) = (corrupted(index) ^ 0x01).toByte
    corrupted
  }
}

private case class FailLoad(reason: RecoveryTaskCommitStore.FailureReason) extends StoreFault {
  override def beforeLoad(): Unit = {
    throw new RecoveryTaskCommitStore.StoreException(reason, s"injected $reason lookup failure")
  }
}

/**
 * Counters shared by the driver and its local executors. They are JVM-global because a `local[N]`
 * executor runs in this JVM but not in this suite instance.
 */
private object TestWriterCounters {
  val writersCreated = new AtomicInteger(0)
  val preflightHits = new AtomicInteger(0)
  val globalCommits = new AtomicInteger(0)
  val publishes = new AtomicInteger(0)
  val droppedReplies = new AtomicInteger(0)
  val driverCommitCallbacks = new AtomicInteger(0)
  val omitCommittedMetricTotals = new AtomicBoolean(false)

  def reset(): Unit = {
    writersCreated.set(0)
    preflightHits.set(0)
    globalCommits.set(0)
    publishes.set(0)
    droppedReplies.set(0)
    driverCommitCallbacks.set(0)
    omitCommittedMetricTotals.set(false)
  }
}

/**
 * An immutable compare-and-set over a directory.
 *
 * The CAS primitive is `Files.createLink`, which is atomic and fails with
 * `FileAlreadyExistsException` when the target exists. `Files.move` is unusable here even with
 * `ATOMIC_MOVE`: it lowers to `rename(2)`, which replaces the target, and the existence check the
 * JDK performs without `REPLACE_EXISTING` is a separate stat, and therefore racy.
 */
private class DirectoryRecoveryTaskCommitStore(root: String, fault: StoreFault)
  extends RecoveryTaskCommitStore {

  override def capabilities(): RecoveryTaskCommitStore.Capabilities =
    DirectoryStoreCapabilities

  private def recordPath(recoveryId: String, partitionId: Int): Path =
    new File(root, s"${sanitize(recoveryId)}__$partitionId.record").toPath

  private def sanitize(id: String): String = id.replaceAll("[^A-Za-z0-9_.-]", "_")

  private def readIfPresent(target: Path): Option[Array[Byte]] =
    if (Files.exists(target)) Some(Files.readAllBytes(target)) else None

  private def compareAndSet(target: Path, value: Array[Byte]): Array[Byte] = {
    val existing = readIfPresent(target)
    if (existing.isDefined) {
      existing.get
    } else {
      Files.createDirectories(new File(root).toPath)
      val temp = new File(root, s".tmp-${UUID.randomUUID()}").toPath
      Files.write(temp, value, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
      try {
        Files.createLink(target, temp)
        value
      } catch {
        case _: java.nio.file.FileAlreadyExistsException =>
          readIfPresent(target).getOrElse(
            throw new IOException(s"lost the compare-and-set for $target with no readable record"))
      } finally {
        Files.deleteIfExists(temp)
      }
    }
  }

  override def resolveWriteManifest(recoveryId: String, proposedValue: Array[Byte]): Array[Byte] =
    compareAndSet(
      new File(root, s"${sanitize(recoveryId)}__manifest.record").toPath, proposedValue)

  override def load(
      recoveryId: String,
      partitionIds: Array[Int]): util.List[util.Optional[Array[Byte]]] = {
    fault.beforeLoad()
    val loaded = partitionIds.map { partitionId =>
      readIfPresent(recordPath(recoveryId, partitionId))
        .map(value => fault.onRead(partitionId, value))
        .map(util.Optional.of[Array[Byte]])
        .getOrElse(util.Optional.empty[Array[Byte]]())
    }
    if (partitionIds.length == 1 && loaded.head.isPresent) {
      // A single-key load only happens in the executor preflight.
      TestWriterCounters.preflightHits.incrementAndGet()
    }
    loaded.toList.asJava
  }

  override def publish(
      recoveryId: String,
      partitionId: Int,
      taskAttemptId: Long,
      attemptNumber: Int,
      value: Array[Byte]): Array[Byte] = {
    fault.beforePublish(partitionId)
    val canonical = compareAndSet(recordPath(recoveryId, partitionId), value)
    TestWriterCounters.publishes.incrementAndGet()
    fault.afterPublish(partitionId)
    canonical
  }
}

private object DirectoryStoreCapabilities extends RecoveryTaskCommitStore.Capabilities {
  override def semanticsVersion(): Int = RecoveryTaskCommitStore.SEMANTICS_VERSION
  override def maxLoadBatchSize(): Int = 1024
  override def maxManifestBytes(): Int = 2 * 1024 * 1024
  override def maxTaskCommitBytes(): Int = 32 * 1024 * 1024
}

/**
 * A recovery provider that implements write recovery only. `tryRecover` returns `None`, which the
 * SPI defines as authoritative permission to recompute the stage, so these tests exercise the write
 * protocol without depending on a shuffle service.
 */
private class TestRecoveryProvider(root: Path, store: RecoveryTaskCommitStore)
  extends ShuffleStageRecovery {

  override def protocolVersion: Int = ShuffleStageRecovery.PROTOCOL_VERSION

  override def tryRecover(info: ShuffleStageRecoveryInfo): Option[RecoveredShuffleStage] = None

  override def resolveSourceAnchor(info: SourceRecoveryInfo): String =
    TestRecoveryProvider.bind(root, s"source-${info.sourceId}", info.currentAnchor)

  override def resolveWriteId(info: WriteRecoveryInfo): String =
    TestRecoveryProvider.bind(root, s"write-${info.sinkId}", info.currentWriteId)

  override def taskCommitStore: Option[RecoveryTaskCommitStore] = Some(store)
}

private object TestRecoveryProvider {
  /** First writer wins, so both drivers derive one identity from the same logical write. */
  def bind(root: Path, key: String, proposed: String): String = {
    Files.createDirectories(root)
    val target = root.resolve(s"${key.replaceAll("[^A-Za-z0-9_.-]", "_")}.binding")
    if (Files.exists(target)) {
      new String(Files.readAllBytes(target), StandardCharsets.UTF_8)
    } else {
      Files.write(target, proposed.getBytes(StandardCharsets.UTF_8))
      proposed
    }
  }
}

private object TestSinkPaths {
  val CommittedMarker = "_COMMITTED"
}

private object TestRecoveryMetrics {
  val Rows = "recoveryRows"

  val Schema = new RecoveryTaskMetricSchema(
    "test-recoverable-sink-metrics",
    1,
    Array(new RecoveryTaskMetricDescriptor(
      Rows,
      "row-count",
      RecoveryTaskMetricDescriptor.ADDITIVE_AGGREGATION,
      1,
      0L,
      Long.MaxValue)))
}

class TestRecoveryRowsMetric extends CustomMetric {
  override def name(): String = TestRecoveryMetrics.Rows

  override def description(): String = "number of rows reported by the recovery test writer"

  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = taskMetrics.sum.toString
}

private case class TestRecoveryRowsTaskMetric(metricValue: Long) extends CustomTaskMetric {
  override def name(): String = TestRecoveryMetrics.Rows

  override def value(): Long = metricValue
}

private case class TestCommit(fileName: String, numRows: Long) extends WriterCommitMessage

/** A stable, non-executable encoding: recovery records outlive the class that wrote them. */
private class TestCommitCodec extends RecoveryCommitMessageCodec {
  override def codecId(): String = "test-recoverable-sink"

  override def version(): Int = 1

  override def encode(message: WriterCommitMessage): Array[Byte] = message match {
    case TestCommit(fileName, numRows) =>
      s"1\t$fileName\t$numRows".getBytes(StandardCharsets.UTF_8)
    case other =>
      throw new IllegalArgumentException(s"unsupported commit message ${other.getClass.getName}")
  }

  override def decode(version: Int, payload: Array[Byte]): WriterCommitMessage = {
    require(version == 1, s"unsupported test codec version: $version")
    val fields = new String(payload, StandardCharsets.UTF_8).split("\t")
    require(fields.length == 3 && fields(0) == "1", "malformed test commit payload")
    TestCommit(fields(1), fields(2).toLong)
  }
}

private class TestRecoveryDataWriter(root: String, partitionId: Int, taskId: Long)
  extends RecoveryDataWriter {

  private val fileName = f"part-$partitionId%05d-$taskId-${UUID.randomUUID()}.data"
  private val rows = new StringBuilder
  private var numRows = 0L

  override def write(record: InternalRow): Unit = {
    rows.append(record.getLong(0)).append('\n')
    numRows += 1
  }

  override def commit(): WriterCommitMessage = {
    val target = new File(root, fileName).toPath
    Files.createDirectories(target.getParent)
    Files.write(target, rows.toString.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
    TestCommit(fileName, numRows)
  }

  override def abort(): Unit = Files.deleteIfExists(new File(root, fileName).toPath)

  override def close(): Unit = {}

  override def currentMetricsValues(): Array[CustomTaskMetric] =
    Array(TestRecoveryRowsTaskMetric(numRows))

  override def discardCommittedOutput(committedMessage: WriterCommitMessage): Unit =
    Files.deleteIfExists(new File(root, committedMessage.asInstanceOf[TestCommit].fileName).toPath)
}

private class TestRecoveryDataWriterFactory(root: String) extends RecoveryDataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): RecoveryDataWriter = {
    TestWriterCounters.writersCreated.incrementAndGet()
    new TestRecoveryDataWriter(root, partitionId, taskId)
  }
}

private class TestRecoverableBatchWrite(root: String, sinkId: String)
  extends SupportsRecoveryTaskMetrics {

  override def recoveryTaskMetricSchema(): RecoveryTaskMetricSchema = TestRecoveryMetrics.Schema

  override def commitMessageCodec(): RecoveryCommitMessageCodec = new TestCommitCodec

  override def recoveryCompatibilityMetadata(info: PhysicalWriteInfo): Array[Byte] =
    s"sink=$sinkId;partitions=${info.numPartitions()};schema=id:bigint"
      .getBytes(StandardCharsets.UTF_8)

  override def recover(info: PhysicalWriteInfo): BatchWriteRecoveryState = {
    val committed = new File(root, TestSinkPaths.CommittedMarker).exists()
    // This connector keeps no task ledger of its own, which leaves the durable store as the sole
    // authority for per-partition state: the strictest configuration of the contract.
    new BatchWriteRecoveryState {
      override def isCommitted(): Boolean = committed

      override def totalNumRows(): Long = if (committed) committedRowCount() else -1L

      override def totalTaskMetrics(): util.Map[String, java.lang.Long] = {
        if (committed && !TestWriterCounters.omitCommittedMetricTotals.get()) {
          util.Collections.singletonMap(
            TestRecoveryMetrics.Rows, java.lang.Long.valueOf(committedRowCount()))
        } else {
          util.Collections.emptyMap[String, java.lang.Long]()
        }
      }
    }
  }

  private def committedRowCount(): Long = {
    Files.readAllLines(new File(root, TestSinkPaths.CommittedMarker).toPath).asScala
      .flatMap(_.split(" ").find(_.startsWith("rows=")))
      .map(_.stripPrefix("rows=").toLong)
      .sum
  }

  override def abortAfterRecovery(messages: Array[WriterCommitMessage]): Unit = {
    // Durable task commits must survive for the next driver, so there is nothing to undo.
  }

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    new TestRecoveryDataWriterFactory(root)

  override def onDataWriterCommit(message: WriterCommitMessage): Unit =
    TestWriterCounters.driverCommitCallbacks.incrementAndGet()

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val payload = messages.zipWithIndex.map { case (message, partitionId) =>
      val commit = message.asInstanceOf[TestCommit]
      s"partition=$partitionId file=${commit.fileName} rows=${commit.numRows}"
    }.mkString("\n").getBytes(StandardCharsets.UTF_8)
    val marker = new File(root, TestSinkPaths.CommittedMarker).toPath
    Files.createDirectories(marker.getParent)
    Files.write(marker, payload, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
    TestWriterCounters.globalCommits.incrementAndGet()
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

private class TestRecoverableTable(root: String, sinkId: String)
  extends Table with SupportsWrite with SupportsRecoveryWrite {

  override def name(): String = s"test-recoverable-sink($sinkId)"

  override def schema(): StructType = new StructType().add("id", LongType)

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_WRITE).asJava

  override def recoverySinkId(): String = sinkId

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new WriteBuilder {
    override def build(): Write = new Write {
      override def description(): String = s"test-recoverable-sink ${info.queryId()}"
      override def supportedCustomMetrics(): Array[CustomMetric] =
        Array(new TestRecoveryRowsMetric)
      override def toBatch: BatchWrite = new TestRecoverableBatchWrite(root, sinkId)
    }
  }
}

class TestRecoverableSinkProvider extends TableProvider {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    new StructType().add("id", LongType)

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val root = Option(options.get("path")).getOrElse(
      throw new IllegalArgumentException("test-recoverable-sink requires a path"))
    new TestRecoverableTable(root, Option(options.get("sinkId")).getOrElse(root))
  }

  override def supportsExternalMetadata(): Boolean = true
}
