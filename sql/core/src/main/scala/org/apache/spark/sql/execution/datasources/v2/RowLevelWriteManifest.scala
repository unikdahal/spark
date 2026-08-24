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

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.CharBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.security.MessageDigest

private[sql] case class RowLevelWriteSourceAnchor(sourceId: String, anchor: String)

/** Fully resolved, immutable inputs to a Spark-owned row-level write generation manifest. */
private[sql] case class RowLevelWriteManifestInput(
    recoveryExecutionId: String,
    recoveryId: String,
    generation: String,
    sinkId: String,
    tableName: String,
    command: String,
    physicalMode: String,
    canonicalOperationSha256: Array[Byte],
    inputSchemaJson: Option[String],
    outputSchemaJson: Option[String],
    rowSchemaJson: Option[String],
    rowIdSchemaJson: Option[String],
    metadataSchemaJson: Option[String],
    distributionDescription: String,
    distributionStrictlyRequired: Boolean,
    requiredNumPartitions: Int,
    advisoryPartitionSizeInBytes: Long,
    orderingDescriptions: Seq[String],
    sourceAnchors: Seq[RowLevelWriteSourceAnchor],
    transactionId: Option[String],
    connectorCompatibilityMetadata: Array[Byte])

/** Deterministic binary encoding; connector bytes remain opaque inside the Spark-owned envelope. */
private[sql] object RowLevelWriteManifest {
  private val Magic = 0x5352574d // SRWM
  private val Version = 1
  private val Sha256Length = 32
  private val MaxStringBytes = 64 * 1024
  private val MaxEntries = 1024
  private val MaxConnectorMetadataBytes = 1024 * 1024
  private val MaxManifestBytes = 16 * 1024 * 1024
  private val Commands = Set("MERGE", "UPDATE", "DELETE")
  private val Modes = Set("REPLACE_DATA", "WRITE_DELTA")

  def encode(input: RowLevelWriteManifestInput): Array[Byte] = {
    require(input != null, "Row-level write manifest input must not be null")
    Seq(
      input.recoveryExecutionId -> "recovery execution ID",
      input.recoveryId -> "recovery ID",
      input.generation -> "generation",
      input.sinkId -> "sink ID",
      input.tableName -> "table name",
      input.distributionDescription -> "distribution description").foreach {
      case (value, name) => checkedString(value, name, requireNonEmpty = true)
    }
    require(Commands.contains(input.command), s"Unsupported row-level command: ${input.command}")
    require(Modes.contains(input.physicalMode),
      s"Unsupported row-level physical mode: ${input.physicalMode}")
    require(input.canonicalOperationSha256 != null &&
      input.canonicalOperationSha256.length == Sha256Length,
      "Canonical operation digest must contain exactly 32 bytes")
    require(input.requiredNumPartitions >= 0,
      s"Required partition count must be non-negative: ${input.requiredNumPartitions}")
    require(input.advisoryPartitionSizeInBytes >= 0,
      s"Advisory partition size must be non-negative: ${input.advisoryPartitionSizeInBytes}")
    require(input.orderingDescriptions != null, "Ordering descriptions must not be null")
    require(input.orderingDescriptions.size <= MaxEntries, "Too many ordering descriptions")
    input.orderingDescriptions.foreach(checkedString(_, "ordering description", requireNonEmpty = true))
    require(input.sourceAnchors != null, "Source anchors must not be null")
    require(input.sourceAnchors.size <= MaxEntries, "Too many source anchors")
    input.sourceAnchors.foreach { source =>
      require(source != null, "Source anchor must not be null")
      checkedString(source.sourceId, "source ID", requireNonEmpty = true)
      checkedString(source.anchor, "source anchor", requireNonEmpty = true)
    }
    val sortedSources = input.sourceAnchors.sortBy(source => (source.sourceId, source.anchor))
    require(sortedSources.map(_.sourceId).distinct.size == sortedSources.size,
      "Source anchor identities must be unique")
    require(input.connectorCompatibilityMetadata != null,
      "Connector compatibility metadata must not be null")
    require(input.connectorCompatibilityMetadata.length <= MaxConnectorMetadataBytes,
      "Connector compatibility metadata exceeds the maximum size")

    val coreBytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(coreBytes)
    out.writeInt(Magic)
    out.writeInt(Version)
    writeString(out, input.recoveryExecutionId)
    writeString(out, input.recoveryId)
    writeString(out, input.generation)
    writeString(out, input.sinkId)
    writeString(out, input.tableName)
    writeString(out, input.command)
    writeString(out, input.physicalMode)
    out.write(input.canonicalOperationSha256)
    Seq(input.inputSchemaJson, input.outputSchemaJson, input.rowSchemaJson,
      input.rowIdSchemaJson, input.metadataSchemaJson).foreach(writeOptionalString(out, _))
    writeString(out, input.distributionDescription)
    out.writeBoolean(input.distributionStrictlyRequired)
    out.writeInt(input.requiredNumPartitions)
    out.writeLong(input.advisoryPartitionSizeInBytes)
    out.writeInt(input.orderingDescriptions.size)
    input.orderingDescriptions.foreach(writeString(out, _))
    out.writeInt(sortedSources.size)
    sortedSources.foreach { source =>
      writeString(out, source.sourceId)
      writeString(out, source.anchor)
    }
    writeOptionalString(out, input.transactionId)
    out.writeInt(input.connectorCompatibilityMetadata.length)
    out.write(input.connectorCompatibilityMetadata)
    out.flush()

    val core = coreBytes.toByteArray
    require(core.length <= MaxManifestBytes - Sha256Length,
      s"Row-level write manifest exceeds $MaxManifestBytes bytes")
    val result = new ByteArrayOutputStream(core.length + Sha256Length)
    result.write(core)
    result.write(MessageDigest.getInstance("SHA-256").digest(core))
    result.toByteArray
  }

  private def writeOptionalString(out: DataOutputStream, value: Option[String]): Unit = {
    require(value != null, "Optional manifest string must not be null")
    out.writeBoolean(value.isDefined)
    value.foreach { present =>
      checkedString(present, "optional manifest string", requireNonEmpty = true)
      writeString(out, present)
    }
  }

  private def writeString(out: DataOutputStream, value: String): Unit = {
    val bytes = utf8(value)
    out.writeInt(bytes.length)
    out.write(bytes)
  }

  private def checkedString(value: String, name: String, requireNonEmpty: Boolean): Unit = {
    require(value != null, s"Row-level write $name must not be null")
    require(!requireNonEmpty || value.nonEmpty, s"Row-level write $name must not be empty")
    utf8(value)
  }

  private def utf8(value: String): Array[Byte] = {
    val encoded = StandardCharsets.UTF_8.newEncoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
      .encode(CharBuffer.wrap(value))
    require(encoded.remaining() <= MaxStringBytes,
      s"Row-level write manifest string exceeds $MaxStringBytes UTF-8 bytes")
    val bytes = new Array[Byte](encoded.remaining())
    encoded.get(bytes)
    bytes
  }
}
