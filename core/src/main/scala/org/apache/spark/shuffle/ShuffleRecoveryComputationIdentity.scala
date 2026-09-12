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

package org.apache.spark.shuffle

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.io.{EOFException, IOException}
import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.security.MessageDigest
import java.util.IdentityHashMap

import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * Versioned semantic identity for a shuffle materialization.
 *
 * The model is intentionally independent of SQL classes. SQL planning converts only an explicitly
 * supported semantic slice into these immutable values after source, decomposition, partitioning,
 * resolved-value, configuration, encoding, and parent-exchange facts are known. Unknown semantics
 * never have a generic serialization fallback.
 *
 * The SHA-256 digest is only an index key. A recovery candidate is compatible only when the full
 * canonical payload is byte-identical.
 */
private[spark] final case class ShuffleRecoveryComputationIdentity private[shuffle] (
    outputContract: ShuffleRecoveryOutputContract,
    producer: ShuffleRecoveryOperatorNode,
    partitioning: ShuffleRecoveryPartitioning,
    mapperDecomposition: ShuffleRecoveryMapperDecomposition,
    sourceTokens: Vector[ShuffleRecoverySourceToken],
    resolvedValues: Vector[(String, ShuffleRecoveryCanonicalValue)],
    semanticConfig: Vector[(String, String)],
    compatibility: ShuffleRecoveryCompatibility,
    parentExchangeIdentities: Vector[ShuffleRecoveryComputationIdentity]) {

  private[spark] lazy val canonicalPayload: Vector[Byte] =
    ShuffleRecoveryComputationIdentityCodec.encode(this).toVector

  private[spark] lazy val digest: String =
    ShuffleRecoveryComputationIdentityCodec.sha256Hex(canonicalPayload.toArray)

  private[spark] lazy val lookupPrefix: String = digest.substring(0, 2)
}

private[spark] object ShuffleRecoveryComputationIdentity {
  val EncodingVersion: Int = 2

  private[spark] val SparkCompatibilityId: Option[String] =
    buildCompatibilityId(org.apache.spark.SPARK_REVISION)

  private[shuffle] def buildCompatibilityId(revision: String): Option[String] = {
    if (revision != null && revision.matches("[0-9a-f]{40}|[0-9a-f]{64}")) {
      Some(s"spark-$revision-shuffle-recovery-identity-v2")
    } else {
      None
    }
  }

  private[spark] def create(
      outputContract: ShuffleRecoveryOutputContract,
      producer: ShuffleRecoveryOperatorNode,
      partitioning: ShuffleRecoveryPartitioning,
      mapperDecomposition: ShuffleRecoveryMapperDecomposition,
      sourceTokens: Seq[ShuffleRecoverySourceToken],
      resolvedValues: Map[String, ShuffleRecoveryCanonicalValue],
      semanticConfig: Map[String, String],
      compatibility: ShuffleRecoveryCompatibility,
      parentExchangeIdentities: Seq[ShuffleRecoveryComputationIdentity] = Nil)
      : ShuffleRecoveryComputationIdentity = {
    if (sourceTokens == null || resolvedValues == null || semanticConfig == null ||
        parentExchangeIdentities == null) {
      throw new IllegalArgumentException("identity collections must not be null")
    }
    val identity = ShuffleRecoveryComputationIdentity(
      outputContract,
      producer,
      partitioning,
      mapperDecomposition,
      sourceTokens.toVector,
      resolvedValues.toVector.sortBy(_._1),
      semanticConfig.toVector.sortBy(_._1),
      compatibility,
      parentExchangeIdentities.toVector)
    ShuffleRecoveryComputationIdentityCodec.validate(identity)
    identity
  }
}

private[spark] final case class ShuffleRecoveryOutputField private[shuffle] (
    dataType: String,
    nullable: Boolean,
    metadata: Vector[(String, String)])

private[spark] object ShuffleRecoveryOutputField {
  private[spark] def create(
      dataType: String,
      nullable: Boolean,
      metadata: Map[String, String] = Map.empty): ShuffleRecoveryOutputField = {
    if (metadata == null) {
      throw new IllegalArgumentException("output metadata must not be null")
    }
    ShuffleRecoveryOutputField(dataType, nullable, metadata.toVector.sortBy(_._1))
  }
}

private[spark] final case class ShuffleRecoveryOutputContract(
    fields: Vector[ShuffleRecoveryOutputField],
    rowEncodingVersion: String,
    serializerCompatibilityId: String,
    codecCompatibilityId: String)

private[spark] sealed trait ShuffleRecoveryOperatorKind {
  private[shuffle] def tag: Int
}

private[spark] object ShuffleRecoveryOperatorKind {
  case object Project extends ShuffleRecoveryOperatorKind { override val tag: Int = 1 }
  case object Filter extends ShuffleRecoveryOperatorKind { override val tag: Int = 2 }
  case object RangeSource extends ShuffleRecoveryOperatorKind { override val tag: Int = 3 }
  case object CertifiedBatchSource extends ShuffleRecoveryOperatorKind { override val tag: Int = 4 }

  private[shuffle] def fromTag(tag: Int): ShuffleRecoveryOperatorKind = tag match {
    case Project.tag => Project
    case Filter.tag => Filter
    case RangeSource.tag => RangeSource
    case CertifiedBatchSource.tag => CertifiedBatchSource
    case _ => throw new IOException(s"unknown recovery operator tag: $tag")
  }
}

private[spark] sealed trait ShuffleRecoveryOperatorChild
private[spark] final case class ShuffleRecoveryInlineOperator(
    node: ShuffleRecoveryOperatorNode) extends ShuffleRecoveryOperatorChild
private[spark] final case class ShuffleRecoveryParentExchange(
    parentIndex: Int) extends ShuffleRecoveryOperatorChild

private[spark] final case class ShuffleRecoveryOperatorNode(
    kind: ShuffleRecoveryOperatorKind,
    parameters: Vector[ShuffleRecoveryCanonicalValue],
    expressions: Vector[ShuffleRecoveryExpressionNode],
    children: Vector[ShuffleRecoveryOperatorChild])

private[spark] sealed trait ShuffleRecoveryExpressionKind {
  private[shuffle] def tag: Int
}

private[spark] object ShuffleRecoveryExpressionKind {
  case object Literal extends ShuffleRecoveryExpressionKind { override val tag: Int = 1 }
  case object Input extends ShuffleRecoveryExpressionKind { override val tag: Int = 2 }
  case object Alias extends ShuffleRecoveryExpressionKind { override val tag: Int = 3 }
  case object Add extends ShuffleRecoveryExpressionKind { override val tag: Int = 4 }
  case object Subtract extends ShuffleRecoveryExpressionKind { override val tag: Int = 5 }
  case object Multiply extends ShuffleRecoveryExpressionKind { override val tag: Int = 6 }
  case object EqualTo extends ShuffleRecoveryExpressionKind { override val tag: Int = 7 }
  case object EqualNullSafe extends ShuffleRecoveryExpressionKind { override val tag: Int = 8 }
  case object GreaterThan extends ShuffleRecoveryExpressionKind { override val tag: Int = 9 }
  case object GreaterThanOrEqual extends ShuffleRecoveryExpressionKind {
    override val tag: Int = 10
  }
  case object LessThan extends ShuffleRecoveryExpressionKind { override val tag: Int = 11 }
  case object LessThanOrEqual extends ShuffleRecoveryExpressionKind { override val tag: Int = 12 }
  case object And extends ShuffleRecoveryExpressionKind { override val tag: Int = 13 }
  case object Or extends ShuffleRecoveryExpressionKind { override val tag: Int = 14 }
  case object Not extends ShuffleRecoveryExpressionKind { override val tag: Int = 15 }
  case object IsNull extends ShuffleRecoveryExpressionKind { override val tag: Int = 16 }
  case object IsNotNull extends ShuffleRecoveryExpressionKind { override val tag: Int = 17 }
  case object Pmod extends ShuffleRecoveryExpressionKind { override val tag: Int = 18 }
  case object Murmur3Hash extends ShuffleRecoveryExpressionKind { override val tag: Int = 19 }

  private val values: Vector[ShuffleRecoveryExpressionKind] = Vector(
    Literal,
    Input,
    Alias,
    Add,
    Subtract,
    Multiply,
    EqualTo,
    EqualNullSafe,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    And,
    Or,
    Not,
    IsNull,
    IsNotNull,
    Pmod,
    Murmur3Hash)

  private[shuffle] def fromTag(tag: Int): ShuffleRecoveryExpressionKind = {
    values.find(_.tag == tag).getOrElse {
      throw new IOException(s"unknown recovery expression tag: $tag")
    }
  }
}

private[spark] final case class ShuffleRecoveryExpressionNode(
    kind: ShuffleRecoveryExpressionKind,
    dataType: String,
    nullable: Boolean,
    parameters: Vector[ShuffleRecoveryCanonicalValue],
    children: Vector[ShuffleRecoveryExpressionNode])

private[spark] sealed trait ShuffleRecoveryPartitioning
private[spark] final case class ShuffleRecoveryHashPartitioning(
    numPartitions: Int,
    expressions: Vector[ShuffleRecoveryExpressionNode],
    hashSeed: Int,
    hashCompatibilityId: String) extends ShuffleRecoveryPartitioning
private[spark] case object ShuffleRecoverySinglePartition extends ShuffleRecoveryPartitioning

private[spark] final case class ShuffleRecoverySourceToken(
    version: Int,
    payload: Vector[Byte])

private[spark] object ShuffleRecoverySourceToken {
  private val ProtocolMagic = 0x53524331 // SRC1
  private[shuffle] val MaxProtocolBytes = 1024

  /**
   * Frames a connector certificate with its certification protocol, separately from the opaque
   * connector bytes. Protocol IDs and versions describe semantics, not implementation classes or
   * credentials. Sharing a protocol is an explicit compatibility contract between connectors.
   * Legacy copyOf callers retain their original encoding.
   */
  private[spark] def forProtocol(
      protocolId: String,
      version: Int,
      certificate: Array[Byte]): ShuffleRecoverySourceToken = {
    require(protocolId != null && protocolId.nonEmpty && protocolId.length <= MaxProtocolBytes,
      "invalid source certification protocol identifier")
    require(version > 0, "source certification protocol version must be positive")
    require(certificate != null && certificate.nonEmpty &&
      certificate.length <= ShuffleRecoveryComputationIdentityCodec.MaxTokenBytes,
      "invalid source certificate size")
    val protocol = try {
      StandardCharsets.UTF_8.newEncoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
        .encode(CharBuffer.wrap(protocolId))
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException("invalid UTF-8 source certification protocol", e)
    }
    require(protocol.remaining() <= MaxProtocolBytes,
      "source certification protocol exceeds encoded size limit")
    val size = 12L + protocol.remaining() + certificate.length
    require(size <= ShuffleRecoveryComputationIdentityCodec.MaxTokenBytes,
      "framed source certificate exceeds token size limit")
    val framed = ByteBuffer.allocate(size.toInt)
      .putInt(ProtocolMagic)
      .putInt(protocol.remaining())
      .put(protocol)
      .putInt(certificate.length)
      .put(certificate)
    ShuffleRecoverySourceToken(version, framed.array().toVector)
  }

  private[spark] def copyOf(version: Int, payload: Array[Byte]): ShuffleRecoverySourceToken = {
    if (payload == null) {
      throw new IllegalArgumentException("source token payload must not be null")
    }
    ShuffleRecoverySourceToken(version, payload.toVector)
  }
}

private[spark] final case class ShuffleRecoveryMapperSplit(
    sourceOrdinal: Int,
    sourcePartitionOrdinal: Int,
    descriptorVersion: Int,
    descriptor: Vector[Byte])

private[spark] object ShuffleRecoveryMapperSplit {
  private[spark] def copyOf(
      sourceOrdinal: Int,
      sourcePartitionOrdinal: Int,
      descriptorVersion: Int,
      descriptor: Array[Byte]): ShuffleRecoveryMapperSplit = {
    if (descriptor == null) {
      throw new IllegalArgumentException("mapper split descriptor must not be null")
    }
    ShuffleRecoveryMapperSplit(
      sourceOrdinal,
      sourcePartitionOrdinal,
      descriptorVersion,
      descriptor.toVector)
  }
}

private[spark] final case class ShuffleRecoveryMapperDecomposition(
    mapperCount: Int,
    splits: Vector[ShuffleRecoveryMapperSplit])

private[spark] final case class ShuffleRecoveryCompatibility(
    sparkCompatibilityId: String,
    shuffleWriteFormatId: String,
    providerReadFormatId: String)

private[spark] sealed trait ShuffleRecoveryCanonicalValue
private[spark] case object ShuffleRecoveryNullValue extends ShuffleRecoveryCanonicalValue
private[spark] final case class ShuffleRecoveryBooleanValue(
    value: Boolean) extends ShuffleRecoveryCanonicalValue
private[spark] final case class ShuffleRecoveryIntValue(
    value: Int) extends ShuffleRecoveryCanonicalValue
private[spark] final case class ShuffleRecoveryLongValue(
    value: Long) extends ShuffleRecoveryCanonicalValue
private[spark] final case class ShuffleRecoveryFloatValue private[shuffle] (
    bits: Int) extends ShuffleRecoveryCanonicalValue
private[spark] final case class ShuffleRecoveryDoubleValue private[shuffle] (
    bits: Long) extends ShuffleRecoveryCanonicalValue
private[spark] final case class ShuffleRecoveryDecimalValue(
    scale: Int,
    unscaled: Vector[Byte]) extends ShuffleRecoveryCanonicalValue
private[spark] final case class ShuffleRecoveryStringValue(
    value: String) extends ShuffleRecoveryCanonicalValue
private[spark] final case class ShuffleRecoveryBinaryValue(
    value: Vector[Byte]) extends ShuffleRecoveryCanonicalValue

private[spark] object ShuffleRecoveryCanonicalValue {
  private[spark] def float(value: Float): ShuffleRecoveryFloatValue = {
    val bits = if (java.lang.Float.isNaN(value)) {
      java.lang.Float.floatToIntBits(Float.NaN)
    } else {
      java.lang.Float.floatToRawIntBits(value)
    }
    ShuffleRecoveryFloatValue(bits)
  }

  private[spark] def double(value: Double): ShuffleRecoveryDoubleValue = {
    val bits = if (java.lang.Double.isNaN(value)) {
      java.lang.Double.doubleToLongBits(Double.NaN)
    } else {
      java.lang.Double.doubleToRawLongBits(value)
    }
    ShuffleRecoveryDoubleValue(bits)
  }

  private[spark] def decimal(
      scale: Int,
      unscaled: Array[Byte]): ShuffleRecoveryDecimalValue = {
    if (unscaled == null || unscaled.isEmpty) {
      throw new IllegalArgumentException("decimal unscaled bytes must not be empty")
    }
    ShuffleRecoveryDecimalValue(scale, unscaled.toVector)
  }

  private[spark] def binary(value: Array[Byte]): ShuffleRecoveryBinaryValue = {
    if (value == null) {
      throw new IllegalArgumentException("binary value must not be null")
    }
    ShuffleRecoveryBinaryValue(value.toVector)
  }
}

/** Canonical binary codec and fail-closed compatibility check for computation identities. */
private[spark] object ShuffleRecoveryComputationIdentityCodec {
  private val Magic = 0x53524932 // SRI2
  private val HashSeed = 42
  private val HashCompatibilityId = "spark-murmur3-32-seed-42-v1"

  private[shuffle] val MaxIdentityBytes = 1024 * 1024
  private[shuffle] val MaxStringBytes = 16 * 1024
  private[shuffle] val MaxTokenBytes = 64 * 1024
  private[shuffle] val MaxBinaryValueBytes = 64 * 1024
  private[shuffle] val MaxMaps = 65536
  private[shuffle] val MaxSourceTokens = 4096
  private[shuffle] val MaxResolvedValues = 1024
  private[shuffle] val MaxSemanticConfigEntries = 128
  private[shuffle] val MaxParentIdentities = 128
  private[shuffle] val MaxNodes = 4096
  private[shuffle] val MaxDepth = 64

  private final class Budget {
    private var nodes: Int = 0

    def consumeNode(): Unit = {
      nodes = Math.addExact(nodes, 1)
      if (nodes > MaxNodes) {
        throw new IllegalArgumentException(s"identity exceeds $MaxNodes semantic nodes")
      }
    }
  }

  private final class BoundedByteArrayOutputStream(maximum: Int)
      extends ByteArrayOutputStream {
    private def requireCapacity(increment: Int): Unit = {
      if (increment < 0) {
        throw new IllegalArgumentException("negative identity write length")
      }
      val next = Math.addExact(count, increment)
      if (next > maximum) {
        throw new IllegalArgumentException(s"identity exceeds $maximum bytes")
      }
    }

    override def write(value: Int): Unit = {
      requireCapacity(1)
      super.write(value)
    }

    override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = {
      requireCapacity(length)
      super.write(bytes, offset, length)
    }
  }

  def encode(identity: ShuffleRecoveryComputationIdentity): Array[Byte] = {
    validate(identity)
    val bytes = new BoundedByteArrayOutputStream(MaxIdentityBytes)
    val out = new DataOutputStream(bytes)
    writeIdentity(out, identity, new Budget, depth = 0)
    out.flush()
    bytes.toByteArray
  }

  def decode(bytes: Array[Byte]): ShuffleRecoveryComputationIdentity = {
    if (bytes == null || bytes.isEmpty || bytes.length > MaxIdentityBytes) {
      throw new IOException("invalid computation identity size")
    }
    val in = new DataInputStream(new ByteArrayInputStream(bytes))
    try {
      val identity = readIdentity(in, new Budget, depth = 0)
      if (in.available() != 0) {
        throw new IOException("trailing computation identity bytes")
      }
      validate(identity)
      identity
    } catch {
      case e: EOFException => throw new IOException("truncated computation identity", e)
      case e: IOException => throw e
      case e: IllegalArgumentException =>
        throw new IOException("invalid computation identity", e)
      case NonFatal(e) => throw new IOException("malformed computation identity", e)
    }
  }

  private[shuffle] def validate(identity: ShuffleRecoveryComputationIdentity): Unit = {
    if (identity == null) {
      throw new IllegalArgumentException("computation identity must not be null")
    }
    validateIdentity(
      identity,
      new Budget,
      new IdentityHashMap[AnyRef, java.lang.Boolean](),
      depth = 0)
  }

  private def validateIdentity(
      identity: ShuffleRecoveryComputationIdentity,
      budget: Budget,
      active: IdentityHashMap[AnyRef, java.lang.Boolean],
      depth: Int): Unit = {
    requireDepth(depth)
    requireNonNull(identity.outputContract, "output contract")
    requireNonNull(identity.producer, "producer")
    requireNonNull(identity.partitioning, "partitioning")
    requireNonNull(identity.mapperDecomposition, "mapper decomposition")
    requireNonNull(identity.sourceTokens, "source tokens")
    requireNonNull(identity.resolvedValues, "resolved values")
    requireNonNull(identity.semanticConfig, "semantic configuration")
    requireNonNull(identity.parentExchangeIdentities, "parent exchange identities")
    if (active.put(identity, java.lang.Boolean.TRUE) != null) {
      throw new IllegalArgumentException("cyclic parent computation identity")
    }
    try {
      validateOutputContract(identity.outputContract)
      validateOperator(identity.producer, budget, active, depth + 1)
      validatePartitioning(identity.partitioning, budget, active, depth + 1)
      validateMapperDecomposition(identity.mapperDecomposition)
      requireCollection(identity.sourceTokens.size, MaxSourceTokens, "source tokens")
      identity.sourceTokens.foreach(validateSourceToken)
      identity.mapperDecomposition.splits.foreach { split =>
        if (split.sourceOrdinal >= identity.sourceTokens.size) {
          throw new IllegalArgumentException("mapper split source ordinal has no source token")
        }
      }
      validateResolvedValues(identity.resolvedValues)
      validateSemanticConfig(identity.semanticConfig)
      validateCompatibility(identity.compatibility)
      validateParents(identity, budget, active, depth)
    } finally {
      active.remove(identity)
    }
  }

  private def validateOutputContract(contract: ShuffleRecoveryOutputContract): Unit = {
    requireNonNull(contract.fields, "output fields")
    requireCollection(contract.fields.size, MaxSourceTokens, "output fields")
    validateText(contract.rowEncodingVersion, "row encoding version")
    validateText(contract.serializerCompatibilityId, "serializer compatibility id")
    validateText(contract.codecCompatibilityId, "codec compatibility id")
    contract.fields.foreach { field =>
      requireNonNull(field, "output field")
      requireNonNull(field.metadata, "output metadata")
      validateText(field.dataType, "output data type")
      requireCollection(field.metadata.size, 128, "output metadata")
      requireSortedDistinct(field.metadata.map(_._1), "output metadata")
      field.metadata.foreach { case (key, value) =>
        validateText(key, "output metadata key")
        validateText(value, "output metadata value", allowEmpty = true)
      }
    }
  }

  private def validateOperator(
      node: ShuffleRecoveryOperatorNode,
      budget: Budget,
      active: IdentityHashMap[AnyRef, java.lang.Boolean],
      depth: Int): Unit = {
    requireDepth(depth)
    requireNonNull(node, "operator node")
    requireNonNull(node.kind, "operator kind")
    requireNonNull(node.parameters, "operator parameters")
    requireNonNull(node.expressions, "operator expressions")
    requireNonNull(node.children, "operator children")
    budget.consumeNode()
    if (active.put(node, java.lang.Boolean.TRUE) != null) {
      throw new IllegalArgumentException("cyclic operator graph")
    }
    try {
      requireCollection(node.parameters.size, 128, "operator parameters")
      node.parameters.foreach(validateValue)
      requireCollection(node.expressions.size, 1024, "operator expressions")
      node.expressions.foreach(validateExpression(_, budget, active, depth + 1))
      requireCollection(node.children.size, 128, "operator children")
      node.children.foreach {
        case ShuffleRecoveryInlineOperator(child) =>
          validateOperator(child, budget, active, depth + 1)
        case ShuffleRecoveryParentExchange(index) if index >= 0 =>
        case ShuffleRecoveryParentExchange(_) =>
          throw new IllegalArgumentException("negative parent exchange index")
        case _ => throw new IllegalArgumentException("unsupported operator child")
      }
      validateOperatorShape(node)
    } finally {
      active.remove(node)
    }
  }

  private def validateOperatorShape(node: ShuffleRecoveryOperatorNode): Unit = node.kind match {
    case ShuffleRecoveryOperatorKind.Project =>
      requireExact(node.parameters.isEmpty, "project operator parameters")
      requireExact(node.children.size == 1, "project operator child count")
    case ShuffleRecoveryOperatorKind.Filter =>
      requireExact(node.parameters.isEmpty, "filter operator parameters")
      requireExact(node.expressions.size == 1, "filter operator expression count")
      requireExact(node.children.size == 1, "filter operator child count")
    case ShuffleRecoveryOperatorKind.CertifiedBatchSource =>
      requireExact(node.parameters.isEmpty, "certified batch source parameters")
      requireExact(node.children.isEmpty, "certified batch source children")
      node.expressions.zipWithIndex.foreach { case (field, ordinal) =>
        requireExact(field.kind == ShuffleRecoveryExpressionKind.Input &&
          field.parameters == Vector(ShuffleRecoveryIntValue(ordinal)),
          "certified batch source field ordinal")
      }
    case ShuffleRecoveryOperatorKind.RangeSource =>
      requireExact(node.expressions.isEmpty, "range source expressions")
      requireExact(node.children.isEmpty, "range source children")
      node.parameters match {
        case Vector(
            _: ShuffleRecoveryLongValue,
            _: ShuffleRecoveryLongValue,
            ShuffleRecoveryLongValue(step),
            ShuffleRecoveryIntValue(slices)) if step != 0L && slices > 0 =>
        case _ => throw new IllegalArgumentException("invalid range source parameters")
      }
  }

  private def validateExpression(
      expression: ShuffleRecoveryExpressionNode,
      budget: Budget,
      active: IdentityHashMap[AnyRef, java.lang.Boolean],
      depth: Int): Unit = {
    requireDepth(depth)
    requireNonNull(expression, "expression node")
    requireNonNull(expression.kind, "expression kind")
    requireNonNull(expression.parameters, "expression parameters")
    requireNonNull(expression.children, "expression children")
    budget.consumeNode()
    if (active.put(expression, java.lang.Boolean.TRUE) != null) {
      throw new IllegalArgumentException("cyclic expression graph")
    }
    try {
      validateText(expression.dataType, "expression data type")
      requireCollection(expression.parameters.size, 64, "expression parameters")
      expression.parameters.foreach(validateValue)
      requireCollection(expression.children.size, 64, "expression children")
      expression.children.foreach(validateExpression(_, budget, active, depth + 1))
      validateExpressionShape(expression)
    } finally {
      active.remove(expression)
    }
  }

  private def validateExpressionShape(expression: ShuffleRecoveryExpressionNode): Unit = {
    import ShuffleRecoveryExpressionKind._
    expression.kind match {
      case Literal =>
        requireExact(expression.parameters.size == 1, "literal parameter count")
        requireExact(expression.children.isEmpty, "literal child count")
      case Input =>
        expression.parameters match {
          case Vector(ShuffleRecoveryIntValue(ordinal)) if ordinal >= 0 =>
          case _ => throw new IllegalArgumentException("invalid input ordinal")
        }
        requireExact(expression.children.isEmpty, "input child count")
      case Alias =>
        requireExact(expression.parameters.isEmpty, "alias parameters")
        requireExact(expression.children.size == 1, "alias child count")
      case EqualTo | EqualNullSafe | GreaterThan | GreaterThanOrEqual |
          LessThan | LessThanOrEqual | And | Or =>
        requireExact(expression.parameters.isEmpty, "binary expression parameters")
        requireExact(expression.children.size == 2, "binary expression child count")
      case Not | IsNull | IsNotNull =>
        requireExact(expression.parameters.isEmpty, "unary expression parameters")
        requireExact(expression.children.size == 1, "unary expression child count")
      case Add | Subtract | Multiply | Pmod | Murmur3Hash =>
        throw new IllegalArgumentException("expression kind is not admitted by identity version 2")
    }
  }

  private def validatePartitioning(
      partitioning: ShuffleRecoveryPartitioning,
      budget: Budget,
      active: IdentityHashMap[AnyRef, java.lang.Boolean],
      depth: Int): Unit = partitioning match {
    case ShuffleRecoveryHashPartitioning(count, expressions, seed, compatibilityId) =>
      if (count <= 0 || count > ShuffleRecoveryManifestCodec.MaxReducers) {
        throw new IllegalArgumentException("invalid hash partition count")
      }
      if (seed != HashSeed || compatibilityId != HashCompatibilityId) {
        throw new IllegalArgumentException("unsupported hash partitioning compatibility")
      }
      if (expressions == null || expressions.isEmpty) {
        throw new IllegalArgumentException("hash partitioning requires expressions")
      }
      requireCollection(expressions.size, 1024, "hash expressions")
      expressions.foreach(validateExpression(_, budget, active, depth))
    case ShuffleRecoverySinglePartition =>
    case _ => throw new IllegalArgumentException("unsupported recovery partitioning")
  }

  private def validateMapperDecomposition(
      decomposition: ShuffleRecoveryMapperDecomposition): Unit = {
    if (decomposition.mapperCount < 0 || decomposition.mapperCount > MaxMaps ||
        decomposition.splits == null) {
      throw new IllegalArgumentException("invalid mapper decomposition")
    }
    if (decomposition.splits.size != decomposition.mapperCount) {
      throw new IllegalArgumentException("mapper split count must equal mapper count")
    }
    decomposition.splits.foreach { split =>
      if (split == null || split.sourceOrdinal < 0 || split.sourcePartitionOrdinal < 0 ||
          split.descriptorVersion <= 0) {
        throw new IllegalArgumentException("invalid mapper split descriptor")
      }
      validateBytes(split.descriptor, MaxTokenBytes, "mapper split descriptor")
    }
  }

  private def validateSourceToken(token: ShuffleRecoverySourceToken): Unit = {
    if (token == null || token.version <= 0) {
      throw new IllegalArgumentException("invalid source token version")
    }
    validateBytes(token.payload, MaxTokenBytes, "source token")
  }

  private def validateResolvedValues(
      resolvedValues: Vector[(String, ShuffleRecoveryCanonicalValue)]): Unit = {
    requireCollection(resolvedValues.size, MaxResolvedValues, "resolved values")
    requireSortedDistinct(resolvedValues.map(_._1), "resolved values")
    resolvedValues.foreach { case (key, value) =>
      validateText(key, "resolved value key")
      validateValue(value)
    }
  }

  private def validateSemanticConfig(semanticConfig: Vector[(String, String)]): Unit = {
    requireCollection(
      semanticConfig.size,
      MaxSemanticConfigEntries,
      "semantic configuration")
    requireSortedDistinct(semanticConfig.map(_._1), "semantic configuration")
    semanticConfig.foreach { case (key, value) =>
      validateText(key, "semantic configuration key")
      validateText(value, "semantic configuration value", allowEmpty = true)
    }
  }

  private def validateCompatibility(compatibility: ShuffleRecoveryCompatibility): Unit = {
    requireNonNull(compatibility, "compatibility")
    validateText(compatibility.sparkCompatibilityId, "Spark compatibility id")
    validateText(compatibility.shuffleWriteFormatId, "shuffle write format id")
    validateText(compatibility.providerReadFormatId, "provider read format id")
    // The codec may decode a manifest from an older build. Adoption compares its complete
    // identity with the current builder's revision-bound identity; parsing is structural only.
    if (!compatibility.sparkCompatibilityId.matches(
        "spark-(?:[0-9a-f]{40}|[0-9a-f]{64})-shuffle-recovery-identity-v2")) {
      throw new IllegalArgumentException("unexpected Spark compatibility id")
    }
  }

  private def validateValue(value: ShuffleRecoveryCanonicalValue): Unit = value match {
    case ShuffleRecoveryNullValue =>
    case _: ShuffleRecoveryBooleanValue =>
    case _: ShuffleRecoveryIntValue =>
    case _: ShuffleRecoveryLongValue =>
    case _: ShuffleRecoveryFloatValue =>
    case _: ShuffleRecoveryDoubleValue =>
    case ShuffleRecoveryDecimalValue(_, unscaled) =>
      validateBytes(unscaled, MaxBinaryValueBytes, "decimal value", allowEmpty = false)
    case ShuffleRecoveryStringValue(text) =>
      validateText(text, "string value", allowEmpty = true)
    case ShuffleRecoveryBinaryValue(bytes) =>
      validateBytes(bytes, MaxBinaryValueBytes, "binary value", allowEmpty = true)
    case _ => throw new IllegalArgumentException("unsupported canonical value")
  }

  private def validateParents(
      identity: ShuffleRecoveryComputationIdentity,
      budget: Budget,
      active: IdentityHashMap[AnyRef, java.lang.Boolean],
      depth: Int): Unit = {
    requireCollection(
      identity.parentExchangeIdentities.size,
      MaxParentIdentities,
      "parent exchange identities")
    identity.parentExchangeIdentities.foreach { parent =>
      validateIdentity(parent, budget, active, depth + 1)
    }
    val referenced = mutable.BitSet.empty
    collectParentReferences(
      identity.producer,
      identity.parentExchangeIdentities.size,
      referenced,
      new IdentityHashMap[ShuffleRecoveryOperatorNode, java.lang.Boolean]())
    if (referenced != mutable.BitSet((0 until identity.parentExchangeIdentities.size): _*)) {
      throw new IllegalArgumentException("parent identity list contains an unreferenced entry")
    }
  }

  private def collectParentReferences(
      node: ShuffleRecoveryOperatorNode,
      parentCount: Int,
      referenced: mutable.BitSet,
      seen: IdentityHashMap[ShuffleRecoveryOperatorNode, java.lang.Boolean]): Unit = {
    if (seen.put(node, java.lang.Boolean.TRUE) != null) {
      return
    }
    node.children.foreach {
      case ShuffleRecoveryInlineOperator(child) =>
        collectParentReferences(child, parentCount, referenced, seen)
      case ShuffleRecoveryParentExchange(index) if index < parentCount =>
        referenced += index
      case ShuffleRecoveryParentExchange(_) =>
        throw new IllegalArgumentException("parent exchange index exceeds parent identity count")
    }
  }

  private def writeIdentity(
      out: DataOutputStream,
      identity: ShuffleRecoveryComputationIdentity,
      budget: Budget,
      depth: Int): Unit = {
    requireDepth(depth)
    out.writeInt(Magic)
    out.writeInt(ShuffleRecoveryComputationIdentity.EncodingVersion)
    writeOutputContract(out, identity.outputContract)
    writeOperator(out, identity.producer, budget, depth + 1)
    writePartitioning(out, identity.partitioning, budget, depth + 1)
    writeMapperDecomposition(out, identity.mapperDecomposition)
    writeCollectionSize(out, identity.sourceTokens.size)
    identity.sourceTokens.foreach { token =>
      out.writeInt(token.version)
      writeBytes(out, token.payload)
    }
    writeCollectionSize(out, identity.resolvedValues.size)
    identity.resolvedValues.foreach { case (key, value) =>
      writeString(out, key)
      writeValue(out, value)
    }
    writeCollectionSize(out, identity.semanticConfig.size)
    identity.semanticConfig.foreach { case (key, value) =>
      writeString(out, key)
      writeString(out, value)
    }
    writeString(out, identity.compatibility.sparkCompatibilityId)
    writeString(out, identity.compatibility.shuffleWriteFormatId)
    writeString(out, identity.compatibility.providerReadFormatId)
    writeCollectionSize(out, identity.parentExchangeIdentities.size)
    identity.parentExchangeIdentities.foreach(writeIdentity(out, _, budget, depth + 1))
  }

  private def readIdentity(
      in: DataInputStream,
      budget: Budget,
      depth: Int): ShuffleRecoveryComputationIdentity = {
    requireDecodeDepth(depth)
    if (in.readInt() != Magic) {
      throw new IOException("invalid computation identity magic")
    }
    val version = in.readInt()
    if (version != ShuffleRecoveryComputationIdentity.EncodingVersion) {
      throw new IOException(s"unsupported computation identity version: $version")
    }
    val output = readOutputContract(in)
    val producer = readOperator(in, budget, depth + 1)
    val partitioning = readPartitioning(in, budget, depth + 1)
    val decomposition = readMapperDecomposition(in)
    val sourceCount = readCollectionSize(in, MaxSourceTokens, "source tokens")
    val sourceTokens = Vector.newBuilder[ShuffleRecoverySourceToken]
    var sourceIndex = 0
    while (sourceIndex < sourceCount) {
      sourceTokens += ShuffleRecoverySourceToken(
        in.readInt(),
        readBytes(in, MaxTokenBytes, "source token"))
      sourceIndex += 1
    }
    val resolvedCount = readCollectionSize(in, MaxResolvedValues, "resolved values")
    val resolved = Vector.newBuilder[(String, ShuffleRecoveryCanonicalValue)]
    var resolvedIndex = 0
    while (resolvedIndex < resolvedCount) {
      resolved += readString(in, "resolved value key") -> readValue(in)
      resolvedIndex += 1
    }
    val configCount = readCollectionSize(
      in,
      MaxSemanticConfigEntries,
      "semantic configuration")
    val config = Vector.newBuilder[(String, String)]
    var configIndex = 0
    while (configIndex < configCount) {
      config += readString(in, "semantic configuration key") ->
        readString(in, "semantic configuration value")
      configIndex += 1
    }
    val compatibility = ShuffleRecoveryCompatibility(
      readString(in, "Spark compatibility id"),
      readString(in, "shuffle write format id"),
      readString(in, "provider read format id"))
    val parentCount = readCollectionSize(in, MaxParentIdentities, "parent identities")
    val parents = Vector.newBuilder[ShuffleRecoveryComputationIdentity]
    var parentIndex = 0
    while (parentIndex < parentCount) {
      parents += readIdentity(in, budget, depth + 1)
      parentIndex += 1
    }
    ShuffleRecoveryComputationIdentity(
      output,
      producer,
      partitioning,
      decomposition,
      sourceTokens.result(),
      resolved.result(),
      config.result(),
      compatibility,
      parents.result())
  }

  private def writeOutputContract(
      out: DataOutputStream,
      contract: ShuffleRecoveryOutputContract): Unit = {
    writeCollectionSize(out, contract.fields.size)
    contract.fields.foreach { field =>
      writeString(out, field.dataType)
      out.writeBoolean(field.nullable)
      writeCollectionSize(out, field.metadata.size)
      field.metadata.foreach { case (key, value) =>
        writeString(out, key)
        writeString(out, value)
      }
    }
    writeString(out, contract.rowEncodingVersion)
    writeString(out, contract.serializerCompatibilityId)
    writeString(out, contract.codecCompatibilityId)
  }

  private def readOutputContract(in: DataInputStream): ShuffleRecoveryOutputContract = {
    val fieldCount = readCollectionSize(in, MaxSourceTokens, "output fields")
    val fields = Vector.newBuilder[ShuffleRecoveryOutputField]
    var fieldIndex = 0
    while (fieldIndex < fieldCount) {
      val dataType = readString(in, "output data type")
      val nullable = in.readBoolean()
      val metadataCount = readCollectionSize(in, 128, "output metadata")
      val metadata = Vector.newBuilder[(String, String)]
      var metadataIndex = 0
      while (metadataIndex < metadataCount) {
        metadata += readString(in, "output metadata key") ->
          readString(in, "output metadata value")
        metadataIndex += 1
      }
      fields += ShuffleRecoveryOutputField(dataType, nullable, metadata.result())
      fieldIndex += 1
    }
    ShuffleRecoveryOutputContract(
      fields.result(),
      readString(in, "row encoding version"),
      readString(in, "serializer compatibility id"),
      readString(in, "codec compatibility id"))
  }

  private def writeOperator(
      out: DataOutputStream,
      node: ShuffleRecoveryOperatorNode,
      budget: Budget,
      depth: Int): Unit = {
    requireDepth(depth)
    budget.consumeNode()
    out.writeInt(node.kind.tag)
    writeCollectionSize(out, node.parameters.size)
    node.parameters.foreach(writeValue(out, _))
    writeCollectionSize(out, node.expressions.size)
    node.expressions.foreach(writeExpression(out, _, budget, depth + 1))
    writeCollectionSize(out, node.children.size)
    node.children.foreach {
      case ShuffleRecoveryInlineOperator(child) =>
        out.writeByte(1)
        writeOperator(out, child, budget, depth + 1)
      case ShuffleRecoveryParentExchange(parentIndex) =>
        out.writeByte(2)
        out.writeInt(parentIndex)
    }
  }

  private def readOperator(
      in: DataInputStream,
      budget: Budget,
      depth: Int): ShuffleRecoveryOperatorNode = {
    requireDecodeDepth(depth)
    consumeDecodeNode(budget)
    val kind = ShuffleRecoveryOperatorKind.fromTag(in.readInt())
    val parameterCount = readCollectionSize(in, 128, "operator parameters")
    val parameters = Vector.newBuilder[ShuffleRecoveryCanonicalValue]
    var parameterIndex = 0
    while (parameterIndex < parameterCount) {
      parameters += readValue(in)
      parameterIndex += 1
    }
    val expressionCount = readCollectionSize(in, 1024, "operator expressions")
    val expressions = Vector.newBuilder[ShuffleRecoveryExpressionNode]
    var expressionIndex = 0
    while (expressionIndex < expressionCount) {
      expressions += readExpression(in, budget, depth + 1)
      expressionIndex += 1
    }
    val childCount = readCollectionSize(in, 128, "operator children")
    val children = Vector.newBuilder[ShuffleRecoveryOperatorChild]
    var childIndex = 0
    while (childIndex < childCount) {
      in.readByte() match {
        case 1 =>
          children += ShuffleRecoveryInlineOperator(readOperator(in, budget, depth + 1))
        case 2 => children += ShuffleRecoveryParentExchange(in.readInt())
        case tag => throw new IOException(s"unknown operator child tag: $tag")
      }
      childIndex += 1
    }
    ShuffleRecoveryOperatorNode(
      kind,
      parameters.result(),
      expressions.result(),
      children.result())
  }

  private def writeExpression(
      out: DataOutputStream,
      expression: ShuffleRecoveryExpressionNode,
      budget: Budget,
      depth: Int): Unit = {
    requireDepth(depth)
    budget.consumeNode()
    out.writeInt(expression.kind.tag)
    writeString(out, expression.dataType)
    out.writeBoolean(expression.nullable)
    writeCollectionSize(out, expression.parameters.size)
    expression.parameters.foreach(writeValue(out, _))
    writeCollectionSize(out, expression.children.size)
    expression.children.foreach(writeExpression(out, _, budget, depth + 1))
  }

  private def readExpression(
      in: DataInputStream,
      budget: Budget,
      depth: Int): ShuffleRecoveryExpressionNode = {
    requireDecodeDepth(depth)
    consumeDecodeNode(budget)
    val kind = ShuffleRecoveryExpressionKind.fromTag(in.readInt())
    val dataType = readString(in, "expression data type")
    val nullable = in.readBoolean()
    val parameterCount = readCollectionSize(in, 64, "expression parameters")
    val parameters = Vector.newBuilder[ShuffleRecoveryCanonicalValue]
    var parameterIndex = 0
    while (parameterIndex < parameterCount) {
      parameters += readValue(in)
      parameterIndex += 1
    }
    val childCount = readCollectionSize(in, 64, "expression children")
    val children = Vector.newBuilder[ShuffleRecoveryExpressionNode]
    var childIndex = 0
    while (childIndex < childCount) {
      children += readExpression(in, budget, depth + 1)
      childIndex += 1
    }
    ShuffleRecoveryExpressionNode(
      kind,
      dataType,
      nullable,
      parameters.result(),
      children.result())
  }

  private def writePartitioning(
      out: DataOutputStream,
      partitioning: ShuffleRecoveryPartitioning,
      budget: Budget,
      depth: Int): Unit = partitioning match {
    case ShuffleRecoveryHashPartitioning(count, expressions, seed, compatibilityId) =>
      out.writeByte(1)
      out.writeInt(count)
      out.writeInt(seed)
      writeString(out, compatibilityId)
      writeCollectionSize(out, expressions.size)
      expressions.foreach(writeExpression(out, _, budget, depth))
    case ShuffleRecoverySinglePartition => out.writeByte(2)
  }

  private def readPartitioning(
      in: DataInputStream,
      budget: Budget,
      depth: Int): ShuffleRecoveryPartitioning = in.readByte() match {
    case 1 =>
      val count = in.readInt()
      val seed = in.readInt()
      val compatibilityId = readString(in, "hash compatibility id")
      val expressionCount = readCollectionSize(in, 1024, "hash expressions")
      val expressions = Vector.newBuilder[ShuffleRecoveryExpressionNode]
      var expressionIndex = 0
      while (expressionIndex < expressionCount) {
        expressions += readExpression(in, budget, depth)
        expressionIndex += 1
      }
      ShuffleRecoveryHashPartitioning(count, expressions.result(), seed, compatibilityId)
    case 2 => ShuffleRecoverySinglePartition
    case tag => throw new IOException(s"unknown partitioning tag: $tag")
  }

  private def writeMapperDecomposition(
      out: DataOutputStream,
      decomposition: ShuffleRecoveryMapperDecomposition): Unit = {
    out.writeInt(decomposition.mapperCount)
    writeCollectionSize(out, decomposition.splits.size)
    decomposition.splits.foreach { split =>
      out.writeInt(split.sourceOrdinal)
      out.writeInt(split.sourcePartitionOrdinal)
      out.writeInt(split.descriptorVersion)
      writeBytes(out, split.descriptor)
    }
  }

  private def readMapperDecomposition(
      in: DataInputStream): ShuffleRecoveryMapperDecomposition = {
    val mapperCount = in.readInt()
    if (mapperCount < 0 || mapperCount > MaxMaps) {
      throw new IOException("invalid mapper count")
    }
    val splitCount = readCollectionSize(in, MaxMaps, "mapper splits")
    if (splitCount != mapperCount) {
      throw new IOException("mapper split count disagrees with mapper count")
    }
    val splits = Vector.newBuilder[ShuffleRecoveryMapperSplit]
    var splitIndex = 0
    while (splitIndex < splitCount) {
      splits += ShuffleRecoveryMapperSplit(
        in.readInt(),
        in.readInt(),
        in.readInt(),
        readBytes(in, MaxTokenBytes, "mapper split descriptor"))
      splitIndex += 1
    }
    ShuffleRecoveryMapperDecomposition(mapperCount, splits.result())
  }

  private def writeValue(out: DataOutputStream, value: ShuffleRecoveryCanonicalValue): Unit = {
    value match {
      case ShuffleRecoveryNullValue => out.writeByte(0)
      case ShuffleRecoveryBooleanValue(v) =>
        out.writeByte(1)
        out.writeBoolean(v)
      case ShuffleRecoveryIntValue(v) =>
        out.writeByte(2)
        out.writeInt(v)
      case ShuffleRecoveryLongValue(v) =>
        out.writeByte(3)
        out.writeLong(v)
      case ShuffleRecoveryFloatValue(bits) =>
        out.writeByte(4)
        out.writeInt(bits)
      case ShuffleRecoveryDoubleValue(bits) =>
        out.writeByte(5)
        out.writeLong(bits)
      case ShuffleRecoveryDecimalValue(scale, unscaled) =>
        out.writeByte(6)
        out.writeInt(scale)
        writeBytes(out, unscaled)
      case ShuffleRecoveryStringValue(v) =>
        out.writeByte(7)
        writeString(out, v)
      case ShuffleRecoveryBinaryValue(v) =>
        out.writeByte(8)
        writeBytes(out, v)
    }
  }

  private def readValue(in: DataInputStream): ShuffleRecoveryCanonicalValue = in.readByte() match {
    case 0 => ShuffleRecoveryNullValue
    case 1 => ShuffleRecoveryBooleanValue(in.readBoolean())
    case 2 => ShuffleRecoveryIntValue(in.readInt())
    case 3 => ShuffleRecoveryLongValue(in.readLong())
    case 4 => ShuffleRecoveryFloatValue(in.readInt())
    case 5 => ShuffleRecoveryDoubleValue(in.readLong())
    case 6 => ShuffleRecoveryDecimalValue(
      in.readInt(),
      readBytes(in, MaxBinaryValueBytes, "decimal value"))
    case 7 => ShuffleRecoveryStringValue(readString(in, "string value"))
    case 8 => ShuffleRecoveryBinaryValue(readBytes(in, MaxBinaryValueBytes, "binary value"))
    case tag => throw new IOException(s"unknown canonical value tag: $tag")
  }

  private def writeString(out: DataOutputStream, value: String): Unit = {
    val bytes = validatedTextBytes(value, "canonical string", allowEmpty = true)
    out.writeInt(bytes.length)
    out.write(bytes)
  }

  private def readString(in: DataInputStream, field: String): String = {
    val bytes = readByteArray(in, MaxStringBytes, field, allowEmpty = true)
    val decoder = StandardCharsets.UTF_8.newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    try {
      decoder.decode(ByteBuffer.wrap(bytes)).toString
    } catch {
      case NonFatal(e) => throw new IOException(s"invalid UTF-8 in $field", e)
    }
  }

  private def writeBytes(out: DataOutputStream, value: Vector[Byte]): Unit = {
    val bytes = value.toArray
    out.writeInt(bytes.length)
    out.write(bytes)
  }

  private def readBytes(
      in: DataInputStream,
      maximum: Int,
      field: String): Vector[Byte] = {
    readByteArray(in, maximum, field, allowEmpty = true).toVector
  }

  private def readByteArray(
      in: DataInputStream,
      maximum: Int,
      field: String,
      allowEmpty: Boolean): Array[Byte] = {
    val length = in.readInt()
    if (length < 0 || length > maximum || (!allowEmpty && length == 0) ||
        length > in.available()) {
      throw new IOException(s"invalid $field length")
    }
    val bytes = new Array[Byte](length)
    in.readFully(bytes)
    bytes
  }

  private def writeCollectionSize(out: DataOutputStream, size: Int): Unit = out.writeInt(size)

  private def readCollectionSize(
      in: DataInputStream,
      maximum: Int,
      field: String): Int = {
    val count = in.readInt()
    if (count < 0 || count > maximum) {
      throw new IOException(s"invalid $field count")
    }
    count
  }

  private def requireCollection(size: Int, maximum: Int, field: String): Unit = {
    if (size < 0 || size > maximum) {
      throw new IllegalArgumentException(s"invalid $field count")
    }
  }

  private def requireSortedDistinct(keys: Seq[String], field: String): Unit = {
    if (keys.exists(_ == null) || keys != keys.sorted || keys.distinct.size != keys.size) {
      throw new IllegalArgumentException(s"$field must be sorted with unique non-null keys")
    }
  }

  private def validateText(
      value: String,
      field: String,
      allowEmpty: Boolean = false): Unit = {
    validatedTextBytes(value, field, allowEmpty)
  }

  private def validatedTextBytes(
      value: String,
      field: String,
      allowEmpty: Boolean): Array[Byte] = {
    if (value == null || (!allowEmpty && value.isEmpty)) {
      throw new IllegalArgumentException(s"$field must not be empty")
    }
    val encoder = StandardCharsets.UTF_8.newEncoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    val encoded = try {
      encoder.encode(CharBuffer.wrap(value))
    } catch {
      case NonFatal(e) => throw new IllegalArgumentException(s"invalid UTF-8 source in $field", e)
    }
    val bytes = new Array[Byte](encoded.remaining())
    encoded.get(bytes)
    if (bytes.length > MaxStringBytes) {
      throw new IllegalArgumentException(s"$field exceeds $MaxStringBytes bytes")
    }
    bytes
  }

  private def validateBytes(
      value: Vector[Byte],
      maximum: Int,
      field: String,
      allowEmpty: Boolean = false): Unit = {
    if (value == null || value.size > maximum || (!allowEmpty && value.isEmpty)) {
      throw new IllegalArgumentException(s"invalid $field size")
    }
  }

  private def requireNonNull(value: AnyRef, field: String): Unit = {
    if (value == null) {
      throw new IllegalArgumentException(s"$field must not be null")
    }
  }

  private def requireExact(condition: Boolean, field: String): Unit = {
    if (!condition) {
      throw new IllegalArgumentException(s"invalid $field")
    }
  }

  private def requireDepth(depth: Int): Unit = {
    if (depth > MaxDepth) {
      throw new IllegalArgumentException(s"identity exceeds maximum depth $MaxDepth")
    }
  }

  private def requireDecodeDepth(depth: Int): Unit = {
    if (depth > MaxDepth) {
      throw new IOException(s"identity exceeds maximum depth $MaxDepth")
    }
  }

  private def consumeDecodeNode(budget: Budget): Unit = {
    try {
      budget.consumeNode()
    } catch {
      case e: IllegalArgumentException => throw new IOException(e.getMessage, e)
    }
  }

  private[shuffle] def sha256Hex(bytes: Array[Byte]): String = {
    if (bytes == null) {
      throw new IllegalArgumentException("digest input must not be null")
    }
    val digest = MessageDigest.getInstance("SHA-256").digest(bytes)
    val chars = new Array[Char](digest.length * 2)
    var index = 0
    while (index < digest.length) {
      val unsigned = digest(index) & 0xff
      chars(index * 2) = Character.forDigit(unsigned >>> 4, 16)
      chars(index * 2 + 1) = Character.forDigit(unsigned & 0xf, 16)
      index += 1
    }
    new String(chars)
  }

  private[spark] def compatibleAfterDigestHit(
      expected: ShuffleRecoveryComputationIdentity,
      candidate: ShuffleRecoveryComputationIdentity): Boolean = {
    if (expected == null || candidate == null || expected.digest != candidate.digest) {
      false
    } else {
      MessageDigest.isEqual(
        expected.canonicalPayload.toArray,
        candidate.canonicalPayload.toArray)
    }
  }

  private[shuffle] def fullPayloadMatches(
      expected: ShuffleRecoveryComputationIdentity,
      candidate: ShuffleRecoveryComputationIdentity): Boolean = {
    if (expected == null || candidate == null) {
      false
    } else {
      MessageDigest.isEqual(
        expected.canonicalPayload.toArray,
        candidate.canonicalPayload.toArray)
    }
  }
}
