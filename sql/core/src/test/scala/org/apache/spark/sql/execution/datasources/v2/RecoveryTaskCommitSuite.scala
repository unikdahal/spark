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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connector.write.{RecoveryCommitMessageCodec, RecoveryTaskCommitStore,
  WriterCommitMessage}

class RecoveryTaskCommitSuite extends SparkFunSuite {

  private val context = RecoveryTaskCommitContext(
    NoopRecoveryTaskCommitStore, "write-17", IntegerCommitCodec)

  test("task commit envelope round trips stable connector payload and row count") {
    val encoded = RecoveryTaskCommitEnvelope.encode(context, 3, IntegerCommit(91), 42L)
    val decoded = RecoveryTaskCommitEnvelope.decode(context, 3, encoded)

    assert(decoded.message === IntegerCommit(91))
    assert(decoded.numRows === 42L)
    assert(decoded.payload.toSeq === IntegerCommitCodec.encode(IntegerCommit(91)).toSeq)
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
    // The connector payload is immediately before the 32-byte SHA-256 digest.
    corrupted(corrupted.length - 33) = (corrupted(corrupted.length - 33) ^ 1).toByte
    assert(intercept[IllegalArgumentException] {
      RecoveryTaskCommitEnvelope.decode(context, 3, corrupted)
    }.getMessage.contains("checksum mismatch"))

    intercept[java.io.EOFException] {
      RecoveryTaskCommitEnvelope.decode(context, 3, encoded.dropRight(1))
    }
    assert(intercept[IllegalArgumentException] {
      RecoveryTaskCommitEnvelope.decode(context, 3, encoded ++ Array[Byte](0))
    }.getMessage.contains("trailing bytes"))
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
}

private case class IntegerCommit(value: Int) extends WriterCommitMessage

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
  override def encode(message: WriterCommitMessage): Array[Byte] = IntegerCommitCodec.encode(message)
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
  override def resolveWriteManifest(recoveryId: String, proposedValue: Array[Byte]): Array[Byte] =
    proposedValue
  override def load(recoveryId: String, partitionIds: Array[Int]): Array[Array[Byte]] =
    Array.fill(partitionIds.length)(null)
  override def publish(
      recoveryId: String,
      partitionId: Int,
      taskAttemptId: Long,
      attemptNumber: Int,
      value: Array[Byte]): Array[Byte] = value
}
