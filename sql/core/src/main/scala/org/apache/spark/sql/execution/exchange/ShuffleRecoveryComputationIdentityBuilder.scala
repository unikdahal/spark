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

package org.apache.spark.sql.execution.exchange

import java.nio.charset.StandardCharsets
import java.util.{Arrays, IdentityHashMap}

import scala.collection.mutable

import org.apache.spark.shuffle.{
  ShuffleRecoveryBooleanValue,
  ShuffleRecoveryCanonicalValue,
  ShuffleRecoveryCompatibility,
  ShuffleRecoveryComputationIdentity,
  ShuffleRecoveryExpressionKind,
  ShuffleRecoveryExpressionNode,
  ShuffleRecoveryHashPartitioning,
  ShuffleRecoveryInlineOperator,
  ShuffleRecoveryIntValue,
  ShuffleRecoveryLongValue,
  ShuffleRecoveryMapperDecomposition,
  ShuffleRecoveryNullValue,
  ShuffleRecoveryOperatorKind,
  ShuffleRecoveryOperatorNode,
  ShuffleRecoveryOutputContract,
  ShuffleRecoveryOutputField,
  ShuffleRecoveryPartitioning,
  ShuffleRecoverySinglePartition,
  ShuffleRecoverySourceToken,
  ShuffleRecoveryStringValue}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  And,
  Attribute,
  AttributeReference,
  BinaryExpression,
  BoundReference,
  EqualNullSafe,
  EqualTo,
  Expression,
  ExprId,
  GreaterThan,
  GreaterThanOrEqual,
  IsNotNull,
  IsNull,
  LessThan,
  LessThanOrEqual,
  Literal,
  Nondeterministic,
  Not,
  Or}
import org.apache.spark.sql.catalyst.plans.physical.{
  HashPartitioning,
  Partitioning,
  RangePartitioning,
  SinglePartition}
import org.apache.spark.sql.execution.{
  ColumnarToRowExec,
  FilterExec,
  InputAdapter,
  ProjectExec,
  RangeExec,
  SparkPlan,
  WholeStageCodegenExec}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.types.{
  BinaryType,
  BooleanType,
  ByteType,
  DataType,
  DateType,
  Decimal,
  DecimalType,
  DoubleType,
  FixedLength,
  FloatType,
  IntegerType,
  LongType,
  MaxLength,
  Metadata,
  NoConstraint,
  ShortType,
  StringType,
  TimestampNTZType,
  TimestampType}
import org.apache.spark.unsafe.types.UTF8String

private[sql] sealed trait ShuffleRecoveryIdentityBuildResult
private[sql] final case class ShuffleRecoveryIdentityBuilt(
    identity: ShuffleRecoveryComputationIdentity) extends ShuffleRecoveryIdentityBuildResult
private[sql] final case class ShuffleRecoveryIdentityRejected(
    reason: ShuffleRecoveryMissReason) extends ShuffleRecoveryIdentityBuildResult

/**
 * Semantic and artifact-format facts resolved before computation-identity construction.
 *
 * Encryption-enabled shuffle bytes are deliberately outside this feasibility slice because an
 * attempt-local key is not a cross-driver compatibility certificate. Compression is admitted only
 * for the reviewed LZ4 format or for uncompressed shuffle output.
 */
private[sql] final case class ShuffleRecoveryIdentitySemanticConfig(
    ansiEnabled: Boolean,
    sessionTimeZone: String,
    shuffleCompressionEnabled: Boolean,
    shuffleCompressionCodec: String,
    shuffleCompressionBlockSize: Int,
    ioEncryptionEnabled: Boolean)

/**
 * Immutable facts consumed at the shuffle materialization boundary.
 *
 * Source-specific interpretation is deliberately absent. A source-read implementation supplies an
 * opaque, versioned token and an exact mapper-decomposition descriptor. Plan object identity is
 * used only for this in-process lookup and is never encoded into the computation identity.
 */
private[sql] final class ShuffleRecoveryResolvedIdentityInputs private (
    private val sourceTokens: IdentityHashMap[SparkPlan, ShuffleRecoverySourceToken],
    val mapperDecomposition: ShuffleRecoveryMapperDecomposition,
    val resolvedValues: Map[String, ShuffleRecoveryCanonicalValue],
    val semanticConfig: ShuffleRecoveryIdentitySemanticConfig,
    val providerReadFormatId: String,
    private val certifiedSource: Option[ShuffleRecoverySourceBinding]) {

  private[exchange] def sourceTokenFor(plan: SparkPlan): Option[ShuffleRecoverySourceToken] =
    Option(sourceTokens.get(plan))

  private[exchange] def certifiedSourceFor(
      plan: BatchScanExec): Option[ShuffleRecoverySourceBinding] =
    certifiedSource.filter(_.isCurrent(plan))
}

private[sql] object ShuffleRecoveryResolvedIdentityInputs {
  private[sql] def create(
      sourceTokens: Seq[(SparkPlan, ShuffleRecoverySourceToken)],
      mapperDecomposition: ShuffleRecoveryMapperDecomposition,
      resolvedValues: Map[String, ShuffleRecoveryCanonicalValue],
      semanticConfig: ShuffleRecoveryIdentitySemanticConfig,
      providerReadFormatId: String,
      certifiedSource: Option[ShuffleRecoverySourceBinding] = None)
      : ShuffleRecoveryResolvedIdentityInputs = {
    require(sourceTokens != null, "source token bindings must not be null")
    require(mapperDecomposition != null, "mapper decomposition must not be null")
    require(resolvedValues != null, "resolved values must not be null")
    require(semanticConfig != null, "semantic configuration must not be null")
    require(certifiedSource != null, "certified source option must not be null")
    certifiedSource.foreach { source =>
      require(source != null && source.isCurrent(source.plan),
        "source binding is no longer current")
      require(sourceTokens.isEmpty && mapperDecomposition == source.decomposition,
        "certified source must supply the complete source and mapper identity")
    }
    require(providerReadFormatId != null && providerReadFormatId.nonEmpty,
      "selected provider read format must not be empty")
    require(semanticConfig.sessionTimeZone != null && semanticConfig.sessionTimeZone.nonEmpty,
      "session time zone must not be empty")
    require(semanticConfig.shuffleCompressionCodec != null &&
      semanticConfig.shuffleCompressionCodec.nonEmpty,
      "shuffle compression codec must not be empty")
    require(semanticConfig.shuffleCompressionBlockSize > 0,
      "shuffle compression block size must be positive")

    val bindings = new IdentityHashMap[SparkPlan, ShuffleRecoverySourceToken]()
    sourceTokens.foreach { case (plan, token) =>
      require(plan != null && token != null, "source token binding must not contain null")
      require(bindings.put(plan, token) == null, "duplicate source plan token binding")
    }
    new ShuffleRecoveryResolvedIdentityInputs(
      bindings,
      mapperDecomposition,
      resolvedValues,
      semanticConfig,
      providerReadFormatId,
      certifiedSource)
  }
}

/**
 * Closed semantic policy for the currently reviewed computation-identity slice.
 *
 * The opportunity analyzer remains an outer observational gate. A class must additionally appear
 * in these exact-class sets and have a matching canonical encoder below. Unknown subclasses,
 * extension expressions, nondeterministic expressions, and semantics with captured state that is
 * not explicitly encoded are refused.
 */
private[sql] object ShuffleRecoveryComputationIdentityPolicy {
  import ShuffleRecoveryMissReason._

  private val supportedPlanClassNames = Set(
    classOf[ProjectExec].getName,
    classOf[FilterExec].getName,
    classOf[RangeExec].getName,
    classOf[BatchScanExec].getName,
    classOf[ColumnarToRowExec].getName,
    classOf[WholeStageCodegenExec].getName,
    classOf[InputAdapter].getName)

  private val supportedExpressionClassNames = Set(
    classOf[Alias].getName,
    classOf[AttributeReference].getName,
    classOf[BoundReference].getName,
    classOf[Literal].getName,
    classOf[EqualTo].getName,
    classOf[EqualNullSafe].getName,
    classOf[GreaterThan].getName,
    classOf[GreaterThanOrEqual].getName,
    classOf[LessThan].getName,
    classOf[LessThanOrEqual].getName,
    classOf[And].getName,
    classOf[Or].getName,
    classOf[Not].getName,
    classOf[IsNull].getName,
    classOf[IsNotNull].getName)

  private[exchange] def planMissReason(
      plan: SparkPlan,
      rules: ShuffleRecoveryEligibilityRules): Option[ShuffleRecoveryMissReason] = {
    if (plan == null) {
      Some(UnsupportedOperator)
    } else {
      val className = plan.getClass.getName
      if (!rules.allowedOperatorClassNames.contains(className)) {
        Some(CustomOperator)
      } else if (!supportedPlanClassNames.contains(className)) {
        Some(UnsupportedOperator)
      } else {
        None
      }
    }
  }

  private[exchange] def expressionMissReason(
      expression: Expression,
      rules: ShuffleRecoveryEligibilityRules): Option[ShuffleRecoveryMissReason] = {
    if (expression == null) {
      Some(UnsupportedExpression)
    } else {
      val className = expression.getClass.getName
      if (!expression.deterministic || expression.isInstanceOf[Nondeterministic]) {
        Some(NonDeterministic)
      } else if (ShuffleRecoveryOpportunityAnalyzer
          .isPythonOrArrowExpressionClassName(className)) {
        Some(PythonOrArrowPresent)
      } else if (!rules.allowedExpressionClassNames.contains(className) ||
          !supportedExpressionClassNames.contains(className)) {
        Some(UnsupportedExpression)
      } else {
        None
      }
    }
  }
}

/**
 * Builds a versioned identity only after materialization-boundary facts are resolved.
 *
 * No canonicalized-plan text, `toString`, reflection fallback, scheduler id, shuffle id, stage id,
 * task-attempt id, executor id, query execution id, or object identity is semantic authority.
 * Range partitioning, aggregates, Python/Arrow, context-bearing arithmetic, custom nodes, and
 * unknown semantics fail closed.
 */
private[sql] object ShuffleRecoveryComputationIdentityBuilder {
  import ShuffleRecoveryMissReason._

  private val MaxPlanDepth = 64
  private val MaxSemanticNodes = 4096
  private val MaxOutputFields = 4096
  private val MaxResolvedValues = 1024
  private val MaxCanonicalStringBytes = 16 * 1024
  private val MaxCompressionBlockBytes = 16 * 1024 * 1024
  private val HashSeed = 42
  private val HashCompatibilityId = "spark-murmur3-32-seed-42-v1"
  private val CanonicalSqlEncoderVersion = "spark-sql-canonical-encoder-v4"
  private val RowEncodingVersion = "unsafe-row-v1"
  private val SerializerCompatibilityId = "unsafe-row-serializer-v1"
  private val CodecCompatibilityId = "spark-internal-row-v1"
  private val ShuffleWriteFormatId = "spark-sort-shuffle-unsafe-row-v1"
  private val SupportedCompressionCodec = "lz4"

  private final class BuildContext(
      val inputs: ShuffleRecoveryResolvedIdentityInputs,
      val rules: ShuffleRecoveryEligibilityRules) {
    val activePlans = new IdentityHashMap[SparkPlan, java.lang.Boolean]()
    val sourceTokens = mutable.ArrayBuffer.empty[ShuffleRecoverySourceToken]
    var semanticNodes: Int = 0

    def consumeNode(): Unit = {
      semanticNodes = Math.addExact(semanticNodes, 1)
      if (semanticNodes > MaxSemanticNodes) {
        fail(DeterminismUnproven)
      }
    }
  }

  private final case class BuildFailure(reason: ShuffleRecoveryMissReason)
      extends RuntimeException(null, null, false, false)

  def build(
      exchange: ShuffleExchangeExec,
      inputs: ShuffleRecoveryResolvedIdentityInputs,
      rules: ShuffleRecoveryEligibilityRules = ShuffleRecoveryEligibilityRules.conservative)
      : ShuffleRecoveryIdentityBuildResult = {
    if (exchange == null || inputs == null || rules == null) {
      return ShuffleRecoveryIdentityRejected(DeterminismUnproven)
    }
    if (exchange.pipelined) {
      return ShuffleRecoveryIdentityRejected(UnsupportedShuffleMode)
    }

    try {
      validateRuntimeCompatibility(inputs.semanticConfig)
      if (inputs.resolvedValues.size > MaxResolvedValues) {
        fail(DeterminismUnproven)
      }
      val context = new BuildContext(inputs, rules)
      val outputContract = buildOutputContract(exchange.child.output)
      val producer = buildOperator(exchange.child, context, depth = 0)
      val partitioning = buildPartitioning(
        exchange.outputPartitioning,
        exchange.child.output,
        context)
      val runtime = inputs.semanticConfig
      val compressionCodec =
        if (runtime.shuffleCompressionEnabled) runtime.shuffleCompressionCodec else "none"
      val compressionBlockSize =
        if (runtime.shuffleCompressionEnabled) runtime.shuffleCompressionBlockSize.toString
        else "inactive"
      val semanticConfig = Map(
        "spark.shuffleRecovery.identityEncoder" -> CanonicalSqlEncoderVersion,
        "spark.sql.ansi.enabled" -> runtime.ansiEnabled.toString,
        "spark.sql.session.timeZone" -> runtime.sessionTimeZone,
        "spark.shuffle.compress" -> runtime.shuffleCompressionEnabled.toString,
        "spark.io.compression.codec" -> compressionCodec,
        "spark.io.compression.lz4.blockSize" -> compressionBlockSize,
        "spark.io.encryption.enabled" -> runtime.ioEncryptionEnabled.toString)
      val compatibility = ShuffleRecoveryCompatibility(
        ShuffleRecoveryComputationIdentity.SparkCompatibilityId
          .getOrElse(fail(UnsupportedShuffleMode)),
        ShuffleWriteFormatId,
        inputs.providerReadFormatId)
      val identity = ShuffleRecoveryComputationIdentity.create(
        outputContract,
        producer,
        partitioning,
        inputs.mapperDecomposition,
        context.sourceTokens.toVector,
        inputs.resolvedValues,
        semanticConfig,
        compatibility)
      ShuffleRecoveryIdentityBuilt(identity)
    } catch {
      case BuildFailure(reason) => ShuffleRecoveryIdentityRejected(reason)
      case _: IllegalArgumentException =>
        ShuffleRecoveryIdentityRejected(DeterminismUnproven)
    }
  }

  private def validateRuntimeCompatibility(config: ShuffleRecoveryIdentitySemanticConfig): Unit = {
    if (config.ioEncryptionEnabled) {
      fail(UnsupportedShuffleMode)
    }
    if (config.shuffleCompressionEnabled &&
        (config.shuffleCompressionCodec != SupportedCompressionCodec ||
          config.shuffleCompressionBlockSize > MaxCompressionBlockBytes)) {
      fail(UnsupportedShuffleMode)
    }
  }

  private def buildOutputContract(output: Seq[Attribute]): ShuffleRecoveryOutputContract = {
    if (output == null || output.size > MaxOutputFields) {
      fail(DeterminismUnproven)
    }
    val fields = output.map { attribute =>
      if (attribute == null || attribute.metadata != Metadata.empty) {
        // Metadata stays outside the initial slice until each semantic key has a reviewed encoding.
        fail(DeterminismUnproven)
      }
      ShuffleRecoveryOutputField.create(
        canonicalDataType(attribute.dataType),
        attribute.nullable)
    }.toVector
    ShuffleRecoveryOutputContract(
      fields,
      RowEncodingVersion,
      SerializerCompatibilityId,
      CodecCompatibilityId)
  }

  private def buildOperator(
      plan: SparkPlan,
      context: BuildContext,
      depth: Int): ShuffleRecoveryOperatorNode = {
    if (plan == null || depth > MaxPlanDepth) {
      fail(DeterminismUnproven)
    }
    context.consumeNode()
    if (context.activePlans.put(plan, java.lang.Boolean.TRUE) != null) {
      fail(DeterminismUnproven)
    }
    try {
      ShuffleRecoveryComputationIdentityPolicy.planMissReason(plan, context.rules).foreach(fail)
      plan match {
        case wrapper: WholeStageCodegenExec =>
          buildOperator(wrapper.child, context, depth + 1)

        case wrapper: InputAdapter =>
          buildOperator(wrapper.child, context, depth + 1)

        case wrapper: ColumnarToRowExec =>
          buildOperator(wrapper.child, context, depth + 1)

        case project: ProjectExec =>
          val ordinals = inputOrdinals(project.child.output)
          val expressions = project.projectList.map { expression =>
            buildExpression(expression, ordinals, context, depth + 1)
          }.toVector
          ShuffleRecoveryOperatorNode(
            ShuffleRecoveryOperatorKind.Project,
            Vector.empty,
            expressions,
            Vector(ShuffleRecoveryInlineOperator(
              buildOperator(project.child, context, depth + 1))))

        case filter: FilterExec =>
          val ordinals = inputOrdinals(filter.child.output)
          val condition = buildExpression(filter.condition, ordinals, context, depth + 1)
          ShuffleRecoveryOperatorNode(
            ShuffleRecoveryOperatorKind.Filter,
            Vector.empty,
            Vector(condition),
            Vector(ShuffleRecoveryInlineOperator(
              buildOperator(filter.child, context, depth + 1))))

        case scan: BatchScanExec =>
          val binding = context.inputs.certifiedSourceFor(scan).getOrElse {
            fail(SourceTokenUnavailable)
          }
          if (context.sourceTokens.nonEmpty ||
              binding.decomposition != context.inputs.mapperDecomposition) {
            fail(InvalidPartitionCount)
          }
          buildOutputContract(scan.output)
          val ordinals = inputOrdinals(scan.output)
          val fields = scan.output.map(buildExpression(_, ordinals, context, depth + 1)).toVector
          context.sourceTokens += binding.token
          ShuffleRecoveryOperatorNode(
            ShuffleRecoveryOperatorKind.CertifiedBatchSource,
            Vector.empty,
            fields,
            Vector.empty)

        case range: RangeExec =>
          val token = context.inputs.sourceTokenFor(range).getOrElse {
            fail(SourceTokenUnavailable)
          }
          context.sourceTokens += token
          ShuffleRecoveryOperatorNode(
            ShuffleRecoveryOperatorKind.RangeSource,
            Vector(
              ShuffleRecoveryLongValue(range.start),
              ShuffleRecoveryLongValue(range.end),
              ShuffleRecoveryLongValue(range.step),
              ShuffleRecoveryIntValue(range.numSlices)),
            Vector.empty,
            Vector.empty)

        case _ => fail(UnsupportedOperator)
      }
    } finally {
      context.activePlans.remove(plan)
    }
  }

  private def buildPartitioning(
      partitioning: Partitioning,
      input: Seq[Attribute],
      context: BuildContext): ShuffleRecoveryPartitioning = partitioning match {
    case HashPartitioning(expressions, count) =>
      if (count <= 0) {
        fail(InvalidPartitionCount)
      }
      val ordinals = inputOrdinals(input)
      val canonicalExpressions = expressions.map { expression =>
        if (!isHashStableType(expression.dataType)) {
          fail(UnsupportedExpression)
        }
        buildExpression(expression, ordinals, context, depth = 0)
      }.toVector
      ShuffleRecoveryHashPartitioning(
        count,
        canonicalExpressions,
        HashSeed,
        HashCompatibilityId)
    case SinglePartition => ShuffleRecoverySinglePartition
    case _: RangePartitioning => fail(RangePartitioningPresent)
    case _ => fail(UnsupportedPartitioning)
  }

  private def buildExpression(
      expression: Expression,
      inputOrdinals: Map[ExprId, Int],
      context: BuildContext,
      depth: Int): ShuffleRecoveryExpressionNode = {
    if (expression == null || depth > MaxPlanDepth) {
      fail(DeterminismUnproven)
    }
    context.consumeNode()
    ShuffleRecoveryComputationIdentityPolicy
      .expressionMissReason(expression, context.rules)
      .foreach(fail)
    val dataType = canonicalDataType(expression.dataType)

    expression match {
      case literal: Literal =>
        ShuffleRecoveryExpressionNode(
          ShuffleRecoveryExpressionKind.Literal,
          dataType,
          literal.nullable,
          Vector(canonicalLiteral(literal)),
          Vector.empty)

      case attribute: AttributeReference =>
        val ordinal = inputOrdinals.getOrElse(attribute.exprId, fail(DeterminismUnproven))
        ShuffleRecoveryExpressionNode(
          ShuffleRecoveryExpressionKind.Input,
          dataType,
          attribute.nullable,
          Vector(ShuffleRecoveryIntValue(ordinal)),
          Vector.empty)

      case bound: BoundReference =>
        if (bound.ordinal < 0) {
          fail(DeterminismUnproven)
        }
        ShuffleRecoveryExpressionNode(
          ShuffleRecoveryExpressionKind.Input,
          dataType,
          bound.nullable,
          Vector(ShuffleRecoveryIntValue(bound.ordinal)),
          Vector.empty)

      case alias: Alias =>
        ShuffleRecoveryExpressionNode(
          ShuffleRecoveryExpressionKind.Alias,
          dataType,
          alias.nullable,
          Vector.empty,
          Vector(buildExpression(alias.child, inputOrdinals, context, depth + 1)))

      case value: EqualTo => binary(
        ShuffleRecoveryExpressionKind.EqualTo, value, inputOrdinals, context, depth)
      case value: EqualNullSafe => binary(
        ShuffleRecoveryExpressionKind.EqualNullSafe, value, inputOrdinals, context, depth)
      case value: GreaterThan => binary(
        ShuffleRecoveryExpressionKind.GreaterThan, value, inputOrdinals, context, depth)
      case value: GreaterThanOrEqual => binary(
        ShuffleRecoveryExpressionKind.GreaterThanOrEqual,
        value,
        inputOrdinals,
        context,
        depth)
      case value: LessThan => binary(
        ShuffleRecoveryExpressionKind.LessThan, value, inputOrdinals, context, depth)
      case value: LessThanOrEqual => binary(
        ShuffleRecoveryExpressionKind.LessThanOrEqual,
        value,
        inputOrdinals,
        context,
        depth)
      case value: And => binary(
        ShuffleRecoveryExpressionKind.And, value, inputOrdinals, context, depth)
      case value: Or => binary(
        ShuffleRecoveryExpressionKind.Or, value, inputOrdinals, context, depth)

      case value: Not => unary(
        ShuffleRecoveryExpressionKind.Not,
        value.child,
        value,
        inputOrdinals,
        context,
        depth)
      case value: IsNull => unary(
        ShuffleRecoveryExpressionKind.IsNull,
        value.child,
        value,
        inputOrdinals,
        context,
        depth)
      case value: IsNotNull => unary(
        ShuffleRecoveryExpressionKind.IsNotNull,
        value.child,
        value,
        inputOrdinals,
        context,
        depth)

      case _ => fail(UnsupportedExpression)
    }
  }

  private def binary(
      kind: ShuffleRecoveryExpressionKind,
      expression: BinaryExpression,
      inputOrdinals: Map[ExprId, Int],
      context: BuildContext,
      depth: Int): ShuffleRecoveryExpressionNode = {
    ShuffleRecoveryExpressionNode(
      kind,
      canonicalDataType(expression.dataType),
      expression.nullable,
      Vector.empty,
      Vector(
        buildExpression(expression.left, inputOrdinals, context, depth + 1),
        buildExpression(expression.right, inputOrdinals, context, depth + 1)))
  }

  private def unary(
      kind: ShuffleRecoveryExpressionKind,
      child: Expression,
      expression: Expression,
      inputOrdinals: Map[ExprId, Int],
      context: BuildContext,
      depth: Int): ShuffleRecoveryExpressionNode = {
    ShuffleRecoveryExpressionNode(
      kind,
      canonicalDataType(expression.dataType),
      expression.nullable,
      Vector.empty,
      Vector(buildExpression(child, inputOrdinals, context, depth + 1)))
  }

  private def canonicalLiteral(literal: Literal): ShuffleRecoveryCanonicalValue = {
    if (literal.value == null) {
      return ShuffleRecoveryNullValue
    }
    literal.dataType match {
      case BooleanType => ShuffleRecoveryBooleanValue(literal.value.asInstanceOf[Boolean])
      case ByteType => ShuffleRecoveryIntValue(literal.value.asInstanceOf[Byte].toInt)
      case ShortType => ShuffleRecoveryIntValue(literal.value.asInstanceOf[Short].toInt)
      case IntegerType | DateType => ShuffleRecoveryIntValue(literal.value.asInstanceOf[Int])
      case LongType | TimestampType | TimestampNTZType =>
        ShuffleRecoveryLongValue(literal.value.asInstanceOf[Long])
      case FloatType =>
        ShuffleRecoveryCanonicalValue.float(literal.value.asInstanceOf[Float])
      case DoubleType =>
        ShuffleRecoveryCanonicalValue.double(literal.value.asInstanceOf[Double])
      case BinaryType =>
        ShuffleRecoveryCanonicalValue.binary(literal.value.asInstanceOf[Array[Byte]])
      case _: StringType => canonicalStringLiteral(literal.value)
      case _: DecimalType =>
        val decimal = literal.value.asInstanceOf[Decimal].toJavaBigDecimal
        ShuffleRecoveryCanonicalValue.decimal(
          decimal.scale(),
          decimal.unscaledValue().toByteArray)
      case _ => fail(UnsupportedExpression)
    }
  }

  private def canonicalStringLiteral(value: Any): ShuffleRecoveryCanonicalValue = value match {
    case utf8: UTF8String =>
      // UTF8String may directly own a caller-supplied mutable array. Copy before validation so the
      // accepted identity cannot race a later mutation of the literal's backing storage.
      val bytes = utf8.getBytes.clone()
      val ownedUtf8 = UTF8String.fromBytes(bytes)
      if (bytes.length > MaxCanonicalStringBytes || !ownedUtf8.isValid) {
        fail(UnsupportedExpression)
      }
      val decoded = new String(bytes, StandardCharsets.UTF_8)
      if (!Arrays.equals(bytes, decoded.getBytes(StandardCharsets.UTF_8))) {
        fail(UnsupportedExpression)
      }
      ShuffleRecoveryStringValue(decoded)
    case _ => fail(UnsupportedExpression)
  }

  private def canonicalDataType(dataType: DataType): String = dataType match {
    case BooleanType => "boolean-v1"
    case ByteType => "byte-v1"
    case ShortType => "short-v1"
    case IntegerType => "int-v1"
    case LongType => "long-v1"
    case FloatType => "float-v1"
    case DoubleType => "double-v1"
    case DateType => "date-days-v1"
    case TimestampType => "timestamp-micros-with-session-time-zone-v1"
    case TimestampNTZType => "timestamp-ntz-micros-v1"
    case BinaryType => "binary-v1"
    case decimal: DecimalType => s"decimal-v1-${decimal.precision}-${decimal.scale}"
    case string: StringType =>
      val constraint = string.constraint match {
        case NoConstraint => "none"
        case FixedLength(length) => s"fixed-$length"
        case MaxLength(length) => s"max-$length"
      }
      s"string-v1-collation-${string.collationId}-$constraint"
    case _ => fail(UnsupportedExpression)
  }

  private def isHashStableType(dataType: DataType): Boolean = dataType match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType => true
    case FloatType | DoubleType | DateType | TimestampType | TimestampNTZType => true
    case BinaryType | _: DecimalType => true
    // String hashing has additional collation/runtime compatibility switches. It stays excluded
    // until those switches are explicitly modeled instead of inferred from a type name.
    case _ => false
  }

  private def inputOrdinals(output: Seq[Attribute]): Map[ExprId, Int] = {
    if (output == null || output.size > MaxOutputFields) {
      fail(DeterminismUnproven)
    }
    val pairs = output.zipWithIndex.map { case (attribute, ordinal) =>
      if (attribute == null) {
        fail(DeterminismUnproven)
      }
      attribute.exprId -> ordinal
    }
    if (pairs.map(_._1).distinct.size != pairs.size) {
      fail(DeterminismUnproven)
    }
    pairs.toMap
  }

  private def fail(reason: ShuffleRecoveryMissReason): Nothing = throw BuildFailure(reason)
}
