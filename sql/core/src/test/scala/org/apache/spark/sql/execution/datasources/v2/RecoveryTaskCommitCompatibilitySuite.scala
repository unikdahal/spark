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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.{Base64, List => JList, Optional}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.recovery.RecoveryTaskCommitStore
import org.apache.spark.sql.connector.write.{RecoveryCommitMessageCodec,
  RecoveryTaskMetricDescriptor, RecoveryTaskMetricSchema, WriterCommitMessage}

/**
 * Golden-file compatibility test for the durable task commit envelope.
 *
 * Recovery records outlive the driver that wrote them, and a replacement driver may run a different
 * Spark build. This suite pins the version 1 layout: the checked-in fixture must still decode, and
 * the current code must still produce byte-identical output for the same inputs. A deliberate
 * format change is expected to fail here and to be accompanied by a readable-version set, not by
 * regenerating the fixture in silence.
 *
 * To regenerate after an intentional format change:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt \
 *     "sql/testOnly *RecoveryTaskCommitCompatibilitySuite"
 * }}}
 */
class RecoveryTaskCommitCompatibilitySuite extends SparkFunSuite {

  private val regenerate = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"
  private val fixtureName = "recovery/task-commit-envelope-v1.txt"
  private val fixtureV2Name = "recovery/task-commit-envelope-v2.txt"

  private val recoveryId = "compatibility-execution"
  private val partitionId = 3
  private val numRows = 4242L
  private val numPartitions = 8
  private val compatibilityMetadata =
    "sink=compatibility;schema=id:bigint".getBytes(StandardCharsets.UTF_8)
  private val message: WriterCommitMessage = FixedCommit("part-00003.data")

  private val context =
    RecoveryTaskCommitContext(UnusedStore, recoveryId, new FixedCommitCodec)

  private val metricSchema = new RecoveryTaskMetricSchema(
    "compatibility-writer-metrics",
    1,
    Array(
      new RecoveryTaskMetricDescriptor(
        "bytes", "byte-count", "sum", 1, 0L, Long.MaxValue),
      new RecoveryTaskMetricDescriptor(
        "files", "file-count", "sum", 1, 0L, 1000L)))
  private val metricContext = context.copy(metricSchema = Some(metricSchema))
  private val metricValues: Seq[CustomTaskMetric] = Seq(
    FixedTaskMetric("bytes", 8192L),
    FixedTaskMetric("files", 1L))

  private def fixturePath(name: String): java.nio.file.Path = {
    // Written back into the source tree, read from the classpath, in the usual golden-file style.
    val resourceRoot = Paths.get("sql", "core", "src", "test", "resources")
    val fromModule = resourceRoot.resolve(name)
    if (Files.exists(fromModule.getParent)) fromModule
    else Paths.get("src", "test", "resources").resolve(name)
  }

  private def readFixture(name: String): Map[String, Array[Byte]] = {
    val stream = Thread.currentThread().getContextClassLoader.getResourceAsStream(name)
    assert(stream != null, s"missing fixture $name; regenerate it, see the suite comment")
    try {
      new String(stream.readAllBytes(), StandardCharsets.UTF_8).linesIterator
        .filter(_.nonEmpty)
        .map(_.split("=", 2))
        .map(fields => fields(0) -> Base64.getDecoder.decode(fields(1)))
        .toMap
    } finally {
      stream.close()
    }
  }

  test("the version 1 envelope and manifest layouts are stable") {
    val envelope = RecoveryTaskCommitEnvelope.encode(context, partitionId, message, numRows)
    val manifest =
      RecoveryTaskCommitEnvelope.writeManifest(context, numPartitions, compatibilityMetadata)

    if (regenerate) {
      val encoder = Base64.getEncoder
      val text = s"envelope=${encoder.encodeToString(envelope)}\n" +
        s"manifest=${encoder.encodeToString(manifest)}\n"
      Files.createDirectories(fixturePath(fixtureName).getParent)
      Files.write(fixturePath(fixtureName), text.getBytes(StandardCharsets.UTF_8))
      logWarning(s"Regenerated $fixtureName")
    } else {
      val fixture = readFixture(fixtureName)

      val decoded = RecoveryTaskCommitEnvelope.decode(context, partitionId, fixture("envelope"))
      assert(decoded.message === message,
        "a record written by an earlier build no longer decodes to the same commit message")
      assert(decoded.numRows === numRows)

      assert(java.util.Arrays.equals(fixture("envelope"), envelope),
        "the envelope layout changed; every durable record written by an earlier build becomes " +
          "unreadable unless the decoder accepts a set of versions")
      assert(java.util.Arrays.equals(fixture("manifest"), manifest),
        "the write manifest layout changed; a replacement driver would fail its byte-for-byte " +
          "manifest comparison against records written by an earlier build")
    }
  }

  test("the version 2 metric envelope and manifest layouts are stable") {
    val envelope = RecoveryTaskCommitEnvelope.encode(
      metricContext, partitionId, message, numRows, metricValues)
    val manifest =
      RecoveryTaskCommitEnvelope.writeManifest(metricContext, numPartitions, compatibilityMetadata)

    if (regenerate) {
      val encoder = Base64.getEncoder
      val text = s"envelope=${encoder.encodeToString(envelope)}\n" +
        s"manifest=${encoder.encodeToString(manifest)}\n"
      Files.createDirectories(fixturePath(fixtureV2Name).getParent)
      Files.write(fixturePath(fixtureV2Name), text.getBytes(StandardCharsets.UTF_8))
      logWarning(s"Regenerated $fixtureV2Name")
    } else {
      val fixture = readFixture(fixtureV2Name)

      val decoded = RecoveryTaskCommitEnvelope.decode(
        metricContext, partitionId, fixture("envelope"))
      assert(decoded.message === message,
        "a version 2 record written by an earlier build no longer decodes to the same " +
          "commit message")
      assert(decoded.numRows === numRows)
      assert(decoded.metrics === Map("bytes" -> 8192L, "files" -> 1L))

      assert(java.util.Arrays.equals(fixture("envelope"), envelope),
        "the version 2 envelope layout changed; durable task metrics would no longer be " +
          "compatible")
      assert(java.util.Arrays.equals(fixture("manifest"), manifest),
        "the version 2 manifest layout or durable metric schema changed")
    }
  }
}

private case class FixedCommit(fileName: String) extends WriterCommitMessage

private case class FixedTaskMetric(name: String, value: Long) extends CustomTaskMetric

private class FixedCommitCodec extends RecoveryCommitMessageCodec {
  override def codecId(): String = "compatibility-codec"

  override def version(): Int = 1

  override def encode(message: WriterCommitMessage): Array[Byte] =
    message.asInstanceOf[FixedCommit].fileName.getBytes(StandardCharsets.UTF_8)

  override def decode(version: Int, payload: Array[Byte]): WriterCommitMessage = {
    require(version == 1, s"unsupported compatibility codec version: $version")
    FixedCommit(new String(payload, StandardCharsets.UTF_8))
  }
}

private object UnusedStore extends RecoveryTaskCommitStore {
  override def capabilities(): RecoveryTaskCommitStore.Capabilities =
    CompatibilityStoreCapabilities
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

private object CompatibilityStoreCapabilities extends RecoveryTaskCommitStore.Capabilities {
  override def semanticsVersion(): Int = RecoveryTaskCommitStore.SEMANTICS_VERSION
  override def maxLoadBatchSize(): Int = 1024
  override def maxManifestBytes(): Int = 2 * 1024 * 1024
  override def maxTaskCommitBytes(): Int = 32 * 1024 * 1024
}
