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
import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.{CharacterCodingException, CodingErrorAction}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import org.apache.spark.sql.catalyst.util.RowDeltaUtils
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.recovery.RecoveryTaskCommitStore
import org.apache.spark.sql.connector.write.{RecoveryCommitMessageCodec,
  RecoveryTaskMetricDescriptor, RecoveryTaskMetricSchema, RowLevelTaskSummary,
  WriterCommitMessage}

private[sql] case class RecoveryTaskCommitContext(
    store: RecoveryTaskCommitStore,
    recoveryId: String,
    codec: RecoveryCommitMessageCodec,
    metricSchema: Option[RecoveryTaskMetricSchema] = None,
    rowLevelSummaryRequired: Boolean = false) extends Serializable

private[sql] case class DecodedRecoveryTaskCommit(
    message: WriterCommitMessage,
    numRows: Long,
    metrics: Map[String, Long],
    rowLevelSummary: Option[RowLevelTaskSummary],
    payload: Array[Byte])

/** Versioned validation envelope owned by Spark, with an opaque connector payload. */
private[sql] object RecoveryTaskCommitEnvelope {
  private val Magic = 0x53525443 // SRTC
  private val Version1 = 1
  private val Version2 = 2
  private val Version3 = 3
  private val RowLevelSummaryVersion = 1
  private val RowLevelCounterCount = 9
  private val MaxIdentityBytes = 64 * 1024
  private val MaxCompatibilityMetadataBytes = 1024 * 1024
  private val MaxPayloadBytes = 16 * 1024 * 1024
  private val MaxMetrics = 1024
  private val MaxMetricSchemaBytes = 1024 * 1024
  private val Sha256Bytes = 32
  private val MaxEnvelopeBytes =
    MaxPayloadBytes + (3 * MaxIdentityBytes) +
      ((MaxMetrics + RowLevelCounterCount) * java.lang.Long.BYTES) + 192

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
    val formatVersion = requiredFormatVersion(context)
    core.writeInt(formatVersion)
    writeString(core, context.recoveryId)
    core.writeInt(numPartitions)
    writeString(core, context.codec.codecId())
    core.writeInt(context.codec.version())
    if (formatVersion == Version3) core.writeBoolean(context.metricSchema.isDefined)
    context.metricSchema.foreach(writeMetricSchema(core, _))
    if (formatVersion == Version3) {
      core.writeInt(RowLevelSummaryVersion)
      core.writeInt(RowLevelCounterCount)
      core.writeInt(RowDeltaUtils.ROW_OPERATION_PROTOCOL_VERSION)
    }
    core.writeInt(compatibilityMetadata.length)
    core.write(compatibilityMetadata)
    core.flush()
    val outputBytes = new ByteArrayOutputStream()
    outputBytes.write(coreBytes.toByteArray)
    outputBytes.write(MessageDigest.getInstance("SHA-256").digest(coreBytes.toByteArray))
    outputBytes.toByteArray
  }

  def validateContext(context: RecoveryTaskCommitContext): Unit = {
    require(context != null, "Recovery task commit context must not be null")
    require(context.store != null, "Recovery task commit store must not be null")
    validatedCapabilities(context)
    checkedString(context.recoveryId, "recovery ID")
    require(context.codec != null, "Recovery commit message codec must not be null")
    checkedString(context.codec.codecId(), "recovery commit codec ID")
    require(context.codec.version() > 0,
      s"Recovery commit codec version must be positive: ${context.codec.version()}")
    require(context.metricSchema != null, "Recovery task metric schema option must not be null")
    context.metricSchema.foreach(validateMetricSchema)
  }

  def validatedCapabilities(
      context: RecoveryTaskCommitContext): RecoveryTaskCommitStore.Capabilities = {
    val capabilities = Option(context.store.capabilities()).getOrElse {
      throw new IllegalStateException("Recovery task commit store returned null capabilities")
    }
    require(capabilities.semanticsVersion() == RecoveryTaskCommitStore.SEMANTICS_VERSION,
      s"Unsupported recovery task store semantics version: ${capabilities.semanticsVersion()}")
    require(capabilities.maxLoadBatchSize() > 0,
      s"Invalid recovery task store load batch size: ${capabilities.maxLoadBatchSize()}")
    require(capabilities.maxManifestBytes() > 0,
      s"Invalid recovery task store manifest limit: ${capabilities.maxManifestBytes()}")
    require(capabilities.maxTaskCommitBytes() > 0,
      s"Invalid recovery task store commit limit: ${capabilities.maxTaskCommitBytes()}")
    capabilities
  }

  def validateManifestSize(context: RecoveryTaskCommitContext, manifest: Array[Byte]): Unit = {
    require(manifest != null, "Recovery write manifest must not be null")
    val maximum = validatedCapabilities(context).maxManifestBytes()
    require(manifest.length <= maximum,
      s"Recovery write manifest is ${manifest.length} bytes; store maximum is $maximum")
  }

  def validateTaskCommitSize(context: RecoveryTaskCommitContext, envelope: Array[Byte]): Unit = {
    require(envelope != null, "Recovery task commit envelope must not be null")
    val maximum = validatedCapabilities(context).maxTaskCommitBytes()
    require(envelope.length <= maximum,
      s"Recovery task commit is ${envelope.length} bytes; store maximum is $maximum")
  }

  def encode(
      context: RecoveryTaskCommitContext,
      partitionId: Int,
      message: WriterCommitMessage,
      numRows: Long,
      currentMetrics: Seq[CustomTaskMetric] = Seq.empty,
      rowLevelSummary: Option[RowLevelTaskSummary] = None): Array[Byte] = {
    validateContext(context)
    require(partitionId >= 0, s"Recovery partition ID must be non-negative: $partitionId")
    require(message != null, "Recovery writer commit message must not be null")
    require(numRows >= 0, s"Recovery output row count must be non-negative: $numRows")
    val payload = Option(context.codec.encode(message)).getOrElse {
      throw new IllegalStateException("Recovery commit message codec returned a null payload")
    }
    require(payload.length <= MaxPayloadBytes,
      s"Recovery commit payload is ${payload.length} bytes; maximum is $MaxPayloadBytes")
    val coreBytes = new ByteArrayOutputStream()
    val core = new DataOutputStream(coreBytes)
    core.writeInt(Magic)
    val formatVersion = requiredFormatVersion(context)
    core.writeInt(formatVersion)
    writeString(core, context.recoveryId)
    core.writeInt(partitionId)
    writeString(core, context.codec.codecId())
    core.writeInt(context.codec.version())
    if (formatVersion == Version3) core.writeBoolean(context.metricSchema.isDefined)
    context.metricSchema match {
      case Some(schema) =>
        val metricValues = validateMetricValues(schema, currentMetrics)
        writeString(core, schema.schemaId())
        core.writeInt(schema.version())
        core.writeInt(metricValues.length)
        metricValues.foreach(core.writeLong)
      case None =>
        require(currentMetrics.isEmpty,
          "Recovery task metrics require a durable metric schema")
    }
    require(rowLevelSummary != null, "Recovery row-level task summary option must not be null")
    require(rowLevelSummary.isDefined == context.rowLevelSummaryRequired,
      s"Recovery row-level task summary presence ${rowLevelSummary.isDefined} does not match " +
        s"the required state ${context.rowLevelSummaryRequired}")
    rowLevelSummary.foreach(writeRowLevelSummary(core, _))
    core.writeLong(numRows)
    core.writeInt(payload.length)
    core.write(payload)
    core.flush()
    val coreArray = coreBytes.toByteArray
    val outputBytes = new ByteArrayOutputStream(coreArray.length + Sha256Bytes)
    outputBytes.write(coreArray)
    outputBytes.write(MessageDigest.getInstance("SHA-256").digest(coreArray))
    outputBytes.toByteArray
  }

  def decode(
      context: RecoveryTaskCommitContext,
      partitionId: Int,
      envelope: Array[Byte]): DecodedRecoveryTaskCommit = {
    validateContext(context)
    require(envelope != null, "Recovery task commit envelope must not be null")
    require(envelope.length > Sha256Bytes && envelope.length <= MaxEnvelopeBytes,
      s"Invalid recovery task commit envelope length: ${envelope.length}")
    val coreLength = envelope.length - Sha256Bytes
    val coreBytes = java.util.Arrays.copyOfRange(envelope, 0, coreLength)
    val checksum = java.util.Arrays.copyOfRange(envelope, coreLength, envelope.length)
    val actualChecksum = MessageDigest.getInstance("SHA-256").digest(coreBytes)
    require(MessageDigest.isEqual(checksum, actualChecksum),
      "Recovery task commit envelope checksum mismatch")
    val input = new DataInputStream(new ByteArrayInputStream(coreBytes))
    require(input.readInt() == Magic, "Invalid recovery task commit envelope magic")
    val formatVersion = input.readInt()
    require(formatVersion == Version1 || formatVersion == Version2 || formatVersion == Version3,
      s"Unsupported recovery task commit envelope version: $formatVersion")
    require(formatVersion == requiredFormatVersion(context),
      s"Recovery task commit envelope version $formatVersion does not match required version " +
        requiredFormatVersion(context))
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
    if (formatVersion == Version3) {
      val hasMetricSchema = input.readBoolean()
      require(hasMetricSchema == context.metricSchema.isDefined,
        s"Recovery task commit metric schema presence $hasMetricSchema does not match required " +
          s"state ${context.metricSchema.isDefined}")
    }
    val metrics = context.metricSchema.map { schema =>
      val schemaId = readString(input, "recovery metric schema ID")
      require(schemaId == schema.schemaId(),
        s"Recovery task commit uses metric schema $schemaId, expected ${schema.schemaId()}")
      val schemaVersion = input.readInt()
      require(schemaVersion == schema.version(),
        s"Recovery task commit uses metric schema version $schemaVersion, " +
          s"expected ${schema.version()}")
      val descriptors = schema.descriptors()
      val metricCount = input.readInt()
      require(metricCount == descriptors.length,
        s"Recovery task commit contains $metricCount metrics, expected ${descriptors.length}")
      descriptors.iterator.map { descriptor =>
        val value = input.readLong()
        require(descriptor.accepts(value),
          s"Recovery task metric ${descriptor.name()} has out-of-range value $value")
        descriptor.name() -> value
      }.toMap
    }.getOrElse(Map.empty[String, Long])
    val rowLevelSummary =
      if (formatVersion == Version3) Some(readRowLevelSummary(input)) else None
    val numRows = input.readLong()
    require(numRows >= 0, s"Invalid recovered output row count: $numRows")
    val payloadLength = input.readInt()
    require(payloadLength >= 0 && payloadLength <= MaxPayloadBytes,
      s"Invalid recovery commit payload length: $payloadLength")
    val payload = new Array[Byte](payloadLength)
    input.readFully(payload)
    require(input.read() == -1, "Recovery task commit envelope contains trailing bytes")
    val message = Option(context.codec.decode(codecVersion, payload)).getOrElse {
      throw new IllegalStateException("Recovery commit message codec decoded a null message")
    }
    DecodedRecoveryTaskCommit(message, numRows, metrics, rowLevelSummary, payload)
  }

  private def requiredFormatVersion(context: RecoveryTaskCommitContext): Int = {
    if (context.rowLevelSummaryRequired) Version3
    else context.metricSchema.fold(Version1)(_ => Version2)
  }

  private def writeRowLevelSummary(
      output: DataOutputStream,
      summary: RowLevelTaskSummary): Unit = {
    output.writeInt(RowLevelSummaryVersion)
    output.writeInt(RowLevelCounterCount)
    output.writeLong(summary.numTargetRowsScanned())
    output.writeLong(summary.numTargetRowsCopied())
    output.writeLong(summary.numTargetRowsDeleted())
    output.writeLong(summary.numTargetRowsUpdated())
    output.writeLong(summary.numTargetRowsInserted())
    output.writeLong(summary.numTargetRowsMatchedUpdated())
    output.writeLong(summary.numTargetRowsMatchedDeleted())
    output.writeLong(summary.numTargetRowsNotMatchedBySourceUpdated())
    output.writeLong(summary.numTargetRowsNotMatchedBySourceDeleted())
  }

  private def readRowLevelSummary(input: DataInputStream): RowLevelTaskSummary = {
    val version = input.readInt()
    require(version == RowLevelSummaryVersion,
      s"Unsupported recovery row-level task summary version: $version")
    val counterCount = input.readInt()
    require(counterCount == RowLevelCounterCount,
      s"Recovery row-level task summary contains $counterCount counters, " +
        s"expected $RowLevelCounterCount")
    new RowLevelTaskSummary(
      input.readLong(), input.readLong(), input.readLong(),
      input.readLong(), input.readLong(), input.readLong(),
      input.readLong(), input.readLong(), input.readLong())
  }

  private def validateMetricSchema(schema: RecoveryTaskMetricSchema): Unit = {
    require(schema != null, "Recovery task metric schema must not be null")
    checkedString(schema.schemaId(), "recovery metric schema ID")
    require(schema.version() > 0,
      s"Recovery metric schema version must be positive: ${schema.version()}")
    val descriptors = Option(schema.descriptors()).getOrElse {
      throw new IllegalStateException("Recovery task metric schema returned null descriptors")
    }
    require(descriptors.nonEmpty && descriptors.length <= MaxMetrics,
      s"Recovery task metric count must be between 1 and $MaxMetrics: ${descriptors.length}")
    val names = scala.collection.mutable.HashSet.empty[String]
    var encodedBytes = checkedString(schema.schemaId(), "recovery metric schema ID").length
    descriptors.foreach { descriptor =>
      require(descriptor != null, "Recovery task metric descriptor must not be null")
      encodedBytes = Math.addExact(encodedBytes,
        checkedString(descriptor.name(), "recovery task metric name").length)
      encodedBytes = Math.addExact(encodedBytes,
        checkedString(descriptor.semanticType(), "recovery task metric semantic type").length)
      encodedBytes = Math.addExact(encodedBytes,
        checkedString(descriptor.aggregationId(), "recovery task metric aggregation ID").length)
      require(descriptor.aggregationId() == RecoveryTaskMetricDescriptor.ADDITIVE_AGGREGATION,
        s"Unsupported recovery task metric aggregation: ${descriptor.aggregationId()}")
      require(descriptor.version() > 0,
        s"Recovery task metric version must be positive: ${descriptor.version()}")
      require(descriptor.minimumValue() <= descriptor.maximumValue(),
        s"Invalid recovery task metric range for ${descriptor.name()}")
      require(descriptor.minimumValue() >= 0L,
        s"Recovery task metric ${descriptor.name()} must be non-negative")
      require(names.add(descriptor.name()),
        s"Duplicate recovery task metric name: ${descriptor.name()}")
    }
    require(encodedBytes <= MaxMetricSchemaBytes,
      s"Recovery task metric schema identifiers use $encodedBytes bytes; " +
        s"maximum is $MaxMetricSchemaBytes")
  }

  private def writeMetricSchema(
      output: DataOutputStream,
      schema: RecoveryTaskMetricSchema): Unit = {
    writeString(output, schema.schemaId())
    output.writeInt(schema.version())
    val descriptors = schema.descriptors()
    output.writeInt(descriptors.length)
    descriptors.foreach { descriptor =>
      writeString(output, descriptor.name())
      writeString(output, descriptor.semanticType())
      writeString(output, descriptor.aggregationId())
      output.writeInt(descriptor.version())
      output.writeLong(descriptor.minimumValue())
      output.writeLong(descriptor.maximumValue())
    }
  }

  private def validateMetricValues(
      schema: RecoveryTaskMetricSchema,
      currentMetrics: Seq[CustomTaskMetric]): Array[Long] = {
    require(currentMetrics != null, "Recovery task metrics must not be null")
    val values = scala.collection.mutable.HashMap.empty[String, Long]
    currentMetrics.foreach { metric =>
      require(metric != null, "Recovery task metric must not be null")
      val name = metric.name()
      checkedString(name, "recovery task metric name")
      require(!values.contains(name), s"Duplicate recovery task metric value: $name")
      values.put(name, metric.value())
    }
    val descriptors = schema.descriptors()
    val expectedNames = descriptors.iterator.map(_.name()).toSet
    val unknownNames = values.keySet.diff(expectedNames)
    require(unknownNames.isEmpty,
      s"Unknown recovery task metrics: ${unknownNames.toSeq.sorted.mkString(", ")}")
    descriptors.map { descriptor =>
      val value = values.getOrElse(descriptor.name(),
        throw new IllegalArgumentException(
          s"Missing recovery task metric: ${descriptor.name()}"))
      require(descriptor.accepts(value),
        s"Recovery task metric ${descriptor.name()} has out-of-range value $value")
      value
    }
  }

  def validateCurrentMetrics(
      context: RecoveryTaskCommitContext,
      currentMetrics: Seq[CustomTaskMetric]): Unit = {
    context.metricSchema match {
      case Some(schema) => validateMetricValues(schema, currentMetrics)
      case None => require(currentMetrics.isEmpty,
        "Recovery task metrics require a durable metric schema")
    }
  }

  private def checkedString(value: String, label: String): Array[Byte] = {
    require(value != null && value.nonEmpty, s"$label must not be empty")
    val bytes = encodeUtf8(value, label)
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
    decodeUtf8(bytes, label)
  }

  private def encodeUtf8(value: String, label: String): Array[Byte] = {
    val encoder = StandardCharsets.UTF_8.newEncoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    try {
      val encoded = encoder.encode(CharBuffer.wrap(value))
      val bytes = new Array[Byte](encoded.remaining())
      encoded.get(bytes)
      bytes
    } catch {
      case e: CharacterCodingException =>
        throw new IllegalArgumentException(s"$label is not valid Unicode", e)
    }
  }

  private def decodeUtf8(bytes: Array[Byte], label: String): String = {
    val decoder = StandardCharsets.UTF_8.newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    try {
      decoder.decode(ByteBuffer.wrap(bytes)).toString
    } catch {
      case e: CharacterCodingException =>
        throw new IllegalArgumentException(s"Invalid UTF-8 in $label", e)
    }
  }
}
