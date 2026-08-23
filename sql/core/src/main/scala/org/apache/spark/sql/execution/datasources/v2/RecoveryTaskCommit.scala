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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import org.apache.spark.sql.connector.write.{RecoveryCommitMessageCodec, RecoveryTaskCommitStore, WriterCommitMessage}

private[sql] case class RecoveryTaskCommitContext(
    store: RecoveryTaskCommitStore,
    recoveryId: String,
    codec: RecoveryCommitMessageCodec) extends Serializable

private[sql] case class DecodedRecoveryTaskCommit(
    message: WriterCommitMessage,
    numRows: Long,
    payload: Array[Byte])

/** Versioned validation envelope owned by Spark, with an opaque connector payload. */
private[sql] object RecoveryTaskCommitEnvelope {
  private val Magic = 0x53525443 // SRTC
  private val FormatVersion = 1
  private val MaxIdentityBytes = 64 * 1024
  private val MaxCompatibilityMetadataBytes = 1024 * 1024
  private val MaxPayloadBytes = 16 * 1024 * 1024
  private val Sha256Bytes = 32

  def writeManifest(
      context: RecoveryTaskCommitContext,
      numPartitions: Int,
      compatibilityMetadata: Array[Byte]): Array[Byte] = {
    validateContext(context)
    require(numPartitions >= 0, s"Physical partition count must be non-negative: $numPartitions")
    require(compatibilityMetadata != null, "Recovery compatibility metadata must not be null")
    require(compatibilityMetadata.length <= MaxCompatibilityMetadataBytes,
      s"Recovery compatibility metadata is ${compatibilityMetadata.length} bytes; maximum is " +
        MaxCompatibilityMetadataBytes)
    val coreBytes = new ByteArrayOutputStream()
    val core = new DataOutputStream(coreBytes)
    core.writeInt(Magic)
    core.writeInt(FormatVersion)
    writeString(core, context.recoveryId)
    core.writeInt(numPartitions)
    writeString(core, context.codec.codecId())
    core.writeInt(context.codec.version())
    core.writeInt(compatibilityMetadata.length)
    core.write(compatibilityMetadata)
    core.flush()
    val outputBytes = new ByteArrayOutputStream()
    outputBytes.write(coreBytes.toByteArray)
    outputBytes.write(MessageDigest.getInstance("SHA-256").digest(coreBytes.toByteArray))
    outputBytes.toByteArray
  }

  def validateContext(context: RecoveryTaskCommitContext): Unit = {
    require(context.store != null, "Recovery task commit store must not be null")
    checkedString(context.recoveryId, "recovery ID")
    require(context.codec != null, "Recovery commit message codec must not be null")
    checkedString(context.codec.codecId(), "recovery commit codec ID")
    require(context.codec.version() > 0,
      s"Recovery commit codec version must be positive: ${context.codec.version()}")
  }

  def encode(
      context: RecoveryTaskCommitContext,
      partitionId: Int,
      message: WriterCommitMessage,
      numRows: Long): Array[Byte] = {
    validateContext(context)
    require(partitionId >= 0, s"Recovery partition ID must be non-negative: $partitionId")
    require(message != null, "Recovery writer commit message must not be null")
    require(numRows >= 0, s"Recovery output row count must be non-negative: $numRows")
    val payload = Option(context.codec.encode(message)).getOrElse {
      throw new IllegalStateException("Recovery commit message codec returned a null payload")
    }
    require(payload.length <= MaxPayloadBytes,
      s"Recovery commit payload is ${payload.length} bytes; maximum is $MaxPayloadBytes")
    val outputBytes = new ByteArrayOutputStream()
    val output = new DataOutputStream(outputBytes)
    output.writeInt(Magic)
    output.writeInt(FormatVersion)
    writeString(output, context.recoveryId)
    output.writeInt(partitionId)
    writeString(output, context.codec.codecId())
    output.writeInt(context.codec.version())
    output.writeLong(numRows)
    output.writeInt(payload.length)
    output.write(payload)
    output.write(MessageDigest.getInstance("SHA-256").digest(payload))
    output.flush()
    outputBytes.toByteArray
  }

  def decode(
      context: RecoveryTaskCommitContext,
      partitionId: Int,
      envelope: Array[Byte]): DecodedRecoveryTaskCommit = {
    validateContext(context)
    require(envelope != null, "Recovery task commit envelope must not be null")
    val input = new DataInputStream(new ByteArrayInputStream(envelope))
    require(input.readInt() == Magic, "Invalid recovery task commit envelope magic")
    val formatVersion = input.readInt()
    require(formatVersion == FormatVersion,
      s"Unsupported recovery task commit envelope version: $formatVersion")
    val storedRecoveryId = readString(input, "recovery ID")
    require(storedRecoveryId == context.recoveryId,
      s"Recovery task commit belongs to $storedRecoveryId, expected ${context.recoveryId}")
    val storedPartitionId = input.readInt()
    require(storedPartitionId == partitionId,
      s"Recovery task commit belongs to partition $storedPartitionId, expected $partitionId")
    val codecId = readString(input, "recovery commit codec ID")
    require(codecId == context.codec.codecId(),
      s"Recovery task commit uses codec $codecId, expected ${context.codec.codecId()}")
    val codecVersion = input.readInt()
    require(codecVersion > 0, s"Invalid recovery commit codec version: $codecVersion")
    val numRows = input.readLong()
    require(numRows >= 0, s"Invalid recovered output row count: $numRows")
    val payloadLength = input.readInt()
    require(payloadLength >= 0 && payloadLength <= MaxPayloadBytes,
      s"Invalid recovery commit payload length: $payloadLength")
    val payload = new Array[Byte](payloadLength)
    input.readFully(payload)
    val checksum = new Array[Byte](Sha256Bytes)
    input.readFully(checksum)
    require(input.read() == -1, "Recovery task commit envelope contains trailing bytes")
    val actualChecksum = MessageDigest.getInstance("SHA-256").digest(payload)
    require(MessageDigest.isEqual(checksum, actualChecksum),
      "Recovery task commit payload checksum mismatch")
    val message = Option(context.codec.decode(codecVersion, payload)).getOrElse {
      throw new IllegalStateException("Recovery commit message codec decoded a null message")
    }
    DecodedRecoveryTaskCommit(message, numRows, payload)
  }

  private def checkedString(value: String, label: String): Array[Byte] = {
    require(value != null && value.nonEmpty, s"$label must not be empty")
    val bytes = value.getBytes(StandardCharsets.UTF_8)
    require(bytes.length <= MaxIdentityBytes,
      s"$label is ${bytes.length} bytes; maximum is $MaxIdentityBytes")
    bytes
  }

  private def writeString(output: DataOutputStream, value: String): Unit = {
    val bytes = checkedString(value, "Recovery identity")
    output.writeInt(bytes.length)
    output.write(bytes)
  }

  private def readString(input: DataInputStream, label: String): String = {
    val length = input.readInt()
    require(length > 0 && length <= MaxIdentityBytes, s"Invalid $label length: $length")
    val bytes = new Array[Byte](length)
    input.readFully(bytes)
    new String(bytes, StandardCharsets.UTF_8)
  }
}
