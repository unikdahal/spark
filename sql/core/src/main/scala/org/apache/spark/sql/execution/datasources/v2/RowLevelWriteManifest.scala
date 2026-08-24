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
import java.util.Base64

import org.apache.spark.sql.connector.distributions.{ClusteredDistribution, Distribution,
  OrderedDistribution, UnspecifiedDistribution}
import org.apache.spark.sql.connector.expressions.{Cast, Expression, GeneralScalarExpression,
  Literal, NamedReference, SortOrder, Transform}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

private[sql] case class RowLevelWriteSourceAnchor(sourceId: String, anchor: String)

/** Fully resolved, immutable inputs to a Spark-owned row-level write generation manifest. */
private[sql] case class RowLevelWriteManifestInput(
    recoveryExecutionId: String,
    recoveryId: String,
    generation: String,
    sinkId: String,
    catalogIdentity: String,
    tableName: String,
    command: String,
    physicalMode: String,
    canonicalOperationSha256: Array[Byte],
    conditionSha256: Array[Byte],
    conflictFilterSha256: Option[Array[Byte]],
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
    transactionId: Option[String])

/** Deterministic binary encoding; connector bytes remain opaque inside the Spark-owned envelope. */
private[sql] object RowLevelWriteManifest {
  private val Magic = 0x5352574d // SRWM
  private val Version = 1
  private val Sha256Length = 32
  private val MaxStringBytes = 64 * 1024
  private val MaxEntries = 1024
  private val MaxManifestBytes = 16 * 1024 * 1024
  private val Commands = Set("MERGE", "UPDATE", "DELETE")
  private val Modes = Set("REPLACE_DATA", "WRITE_DELTA")

  /** Spark-owned generation fence independent of a provider-returned write ID. */
  def generation(
      recoveryExecutionId: String,
      recoveryId: String,
      sinkId: String,
      canonicalOperationSha256: Array[Byte]): String = {
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    writeString(out, "spark-row-level-generation-v1")
    Seq(recoveryExecutionId, recoveryId, sinkId).foreach { value =>
      checkedString(value, "generation identity", requireNonEmpty = true)
      writeString(out, value)
    }
    require(canonicalOperationSha256 != null &&
      canonicalOperationSha256.length == Sha256Length,
      "Canonical operation digest must contain exactly 32 bytes")
    out.write(canonicalOperationSha256.clone())
    out.flush()
    "spark-row-v1-" + Base64.getUrlEncoder.withoutPadding().encodeToString(
      MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray))
  }

  def encode(input: RowLevelWriteManifestInput): Array[Byte] = {
    require(input != null, "Row-level write manifest input must not be null")
    Seq(
      input.recoveryExecutionId -> "recovery execution ID",
      input.recoveryId -> "recovery ID",
      input.generation -> "generation",
      input.sinkId -> "sink ID",
      input.catalogIdentity -> "catalog identity",
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
    val canonicalOperationSha256 = input.canonicalOperationSha256.clone()
    require(input.conditionSha256 != null && input.conditionSha256.length == Sha256Length,
      "Row-level condition digest must contain exactly 32 bytes")
    val conditionSha256 = input.conditionSha256.clone()
    require(input.conflictFilterSha256 != null,
      "Row-level conflict-filter digest option must not be null")
    val conflictFilterSha256 = input.conflictFilterSha256.map { digest =>
      require(digest != null && digest.length == Sha256Length,
        "Row-level conflict-filter digest must contain exactly 32 bytes")
      digest.clone()
    }
    require(input.requiredNumPartitions >= 0,
      s"Required partition count must be non-negative: ${input.requiredNumPartitions}")
    require(input.advisoryPartitionSizeInBytes >= 0,
      s"Advisory partition size must be non-negative: ${input.advisoryPartitionSizeInBytes}")
    require(input.orderingDescriptions != null, "Ordering descriptions must not be null")
    val orderingDescriptions = input.orderingDescriptions.toVector
    require(orderingDescriptions.size <= MaxEntries, "Too many ordering descriptions")
    orderingDescriptions.foreach { description =>
      checkedString(description, "ordering description", requireNonEmpty = true)
    }
    require(input.sourceAnchors != null, "Source anchors must not be null")
    val sourceAnchors = input.sourceAnchors.toVector
    require(sourceAnchors.size <= MaxEntries, "Too many source anchors")
    sourceAnchors.foreach { source =>
      require(source != null, "Source anchor must not be null")
      checkedString(source.sourceId, "source ID", requireNonEmpty = true)
      checkedString(source.anchor, "source anchor", requireNonEmpty = true)
    }
    val sortedSources = sourceAnchors.sortBy(source => (source.sourceId, source.anchor))
    require(sortedSources.map(_.sourceId).distinct.size == sortedSources.size,
      "Source anchor identities must be unique")

    val coreBytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(coreBytes)
    out.writeInt(Magic)
    out.writeInt(Version)
    writeString(out, input.recoveryExecutionId)
    writeString(out, input.recoveryId)
    writeString(out, input.generation)
    writeString(out, input.sinkId)
    writeString(out, input.catalogIdentity)
    writeString(out, input.tableName)
    writeString(out, input.command)
    writeString(out, input.physicalMode)
    out.write(canonicalOperationSha256)
    out.write(conditionSha256)
    out.writeBoolean(conflictFilterSha256.isDefined)
    conflictFilterSha256.foreach(out.write(_))
    Seq(input.inputSchemaJson, input.outputSchemaJson, input.rowSchemaJson,
      input.rowIdSchemaJson, input.metadataSchemaJson).foreach(writeOptionalString(out, _))
    writeString(out, input.distributionDescription)
    out.writeBoolean(input.distributionStrictlyRequired)
    out.writeInt(input.requiredNumPartitions)
    out.writeLong(input.advisoryPartitionSizeInBytes)
    out.writeInt(orderingDescriptions.size)
    orderingDescriptions.foreach(writeString(out, _))
    out.writeInt(sortedSources.size)
    sortedSources.foreach { source =>
      writeString(out, source.sourceId)
      writeString(out, source.anchor)
    }
    writeOptionalString(out, input.transactionId)
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

/** Stable structural encoding for connector distribution and ordering requirements. */
private[sql] object RowLevelWriteRequirementEncoding {
  private val Version = 1
  private val MaxEntries = 1024
  private val MaxStringBytes = 64 * 1024

  def distribution(distribution: Distribution): String = encoded { out =>
    require(distribution != null, "Required distribution must not be null")
    out.writeInt(Version)
    distribution match {
      case _: UnspecifiedDistribution => out.writeByte(1)
      case clustered: ClusteredDistribution =>
        out.writeByte(2)
        writeExpressions(out, clustered.clustering())
      case ordered: OrderedDistribution =>
        out.writeByte(3)
        writeSortOrders(out, ordered.ordering())
      case other => throw new IllegalArgumentException(
        s"Unsupported durable distribution implementation: ${other.getClass.getName}")
    }
  }

  def ordering(order: SortOrder): String = encoded { out =>
    out.writeInt(Version)
    writeSortOrder(out, order)
  }

  private def encoded(write: DataOutputStream => Unit): String = {
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    write(out)
    out.flush()
    Base64.getUrlEncoder.withoutPadding().encodeToString(bytes.toByteArray)
  }

  private def writeSortOrders(out: DataOutputStream, orders: Array[SortOrder]): Unit = {
    require(orders != null, "Required ordering must not be null")
    require(orders.length <= MaxEntries, s"Required ordering exceeds $MaxEntries entries")
    out.writeInt(orders.length)
    orders.foreach(writeSortOrder(out, _))
  }

  private def writeSortOrder(out: DataOutputStream, order: SortOrder): Unit = {
    require(order != null, "Required sort order must not be null")
    writeString(out, Option(order.direction()).map(_.name()).orNull, "sort direction")
    writeString(out, Option(order.nullOrdering()).map(_.name()).orNull, "null ordering")
    writeExpression(out, order.expression())
  }

  private def writeExpressions(out: DataOutputStream, expressions: Array[Expression]): Unit = {
    require(expressions != null, "Required expressions must not be null")
    require(expressions.length <= MaxEntries,
      s"Required expressions exceed $MaxEntries entries")
    out.writeInt(expressions.length)
    expressions.foreach(writeExpression(out, _))
  }

  private def writeExpression(out: DataOutputStream, expression: Expression): Unit = {
    require(expression != null, "Required expression must not be null")
    expression match {
      case reference: NamedReference =>
        out.writeByte(1)
        val names = Option(reference.fieldNames()).getOrElse {
          throw new IllegalArgumentException("Named reference returned null field names")
        }
        require(names.nonEmpty && names.length <= MaxEntries,
          "Named reference must contain a bounded non-empty field path")
        out.writeInt(names.length)
        names.foreach(writeString(out, _, "reference field"))

      case literal: Literal[_] =>
        out.writeByte(2)
        writeString(out, Option(literal.dataType()).map(_.json).orNull, "literal data type")
        writeLiteral(out, literal.value())

      case transform: Transform =>
        out.writeByte(3)
        writeString(out, transform.name(), "transform name")
        writeExpressions(out, transform.arguments())

      case scalar: GeneralScalarExpression =>
        out.writeByte(4)
        writeString(out, scalar.name(), "scalar function name")
        writeExpressions(out, scalar.children())

      case cast: Cast =>
        out.writeByte(5)
        writeString(out, Option(cast.expressionDataType()).map(_.json).orNull,
          "cast input data type")
        writeString(out, Option(cast.dataType()).map(_.json).orNull, "cast output data type")
        writeExpression(out, cast.expression())

      case other => throw new IllegalArgumentException(
        s"Unsupported durable connector expression: ${other.getClass.getName}")
    }
  }

  private def writeLiteral(out: DataOutputStream, value: Any): Unit = value match {
    case null => out.writeByte(0)
    case value: Boolean =>
      out.writeByte(1)
      out.writeBoolean(value)
    case value: Byte =>
      out.writeByte(2)
      out.writeByte(value)
    case value: Short =>
      out.writeByte(3)
      out.writeShort(value)
    case value: Int =>
      out.writeByte(4)
      out.writeInt(value)
    case value: Long =>
      out.writeByte(5)
      out.writeLong(value)
    case value: Float =>
      out.writeByte(6)
      out.writeInt(java.lang.Float.floatToRawIntBits(value))
    case value: Double =>
      out.writeByte(7)
      out.writeLong(java.lang.Double.doubleToRawLongBits(value))
    case value: String =>
      out.writeByte(8)
      writeString(out, value, "literal string")
    case value: UTF8String =>
      out.writeByte(9)
      val bytes = value.getBytes
      out.writeInt(bytes.length)
      out.write(bytes)
    case value: Decimal =>
      out.writeByte(10)
      writeString(out, value.toJavaBigDecimal.toPlainString, "decimal literal")
    case value: Array[Byte] =>
      out.writeByte(11)
      out.writeInt(value.length)
      out.write(value)
    case other => throw new IllegalArgumentException(
      s"Unsupported durable connector literal: ${other.getClass.getName}")
  }

  private def writeString(
      out: DataOutputStream,
      value: String,
      label: String): Unit = {
    require(value != null && value.nonEmpty, s"$label must not be empty")
    val bytes = StandardCharsets.UTF_8.newEncoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
      .encode(CharBuffer.wrap(value))
    require(bytes.remaining() <= MaxStringBytes, s"$label exceeds $MaxStringBytes UTF-8 bytes")
    val copy = new Array[Byte](bytes.remaining())
    bytes.get(copy)
    out.writeInt(copy.length)
    out.write(copy)
  }
}
