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

import org.apache.spark.SparkArithmeticException
import org.apache.spark.shuffle.{
  ShuffleRecoveryAdoptionTarget,
  ShuffleRecoveryCanonicalValue,
  ShuffleRecoveryComputationIdentity,
  ShuffleRecoveryIntValue,
  ShuffleRecoveryMapperDecomposition,
  ShuffleRecoveryMapperSplit,
  ShuffleRecoveryMaterializationId,
  ShuffleRecoverySourceToken}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Ascending,
  AttributeReference,
  EvalMode,
  Expression,
  Literal,
  Murmur3Hash,
  Not,
  NumericEvalContext,
  Pmod,
  Rand,
  SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{
  HashPartitioning,
  RangePartitioning,
  SinglePartition}
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.connector.read.{Batch, HasPartitionKey, InputPartition, PartitionReader, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.{ProjectExec, RangeExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

class ShuffleRecoveryComputationIdentityBuilderSuite extends SharedSparkSession {
  import ShuffleRecoveryMissReason._

  private val DefaultCompressionBlockSize = 32 * 1024

  private case class BuildOverrides(
      encryptionEnabled: Boolean = false,
      resolvedValues: Map[String, ShuffleRecoveryCanonicalValue] = Map.empty,
      providerReadFormatId: String = "reference-shuffle-provider-v1")

  test("certified batch identities survive replanning but distinguish source and split facts") {
    def identity(protocol: String, certificate: Byte, descriptor: Byte)
        : ShuffleRecoveryComputationIdentity = {
      val plan = batchPlan()
      val binding = ShuffleRecoverySourceBinding.bind(plan, protocol, 1, Array(certificate),
        plan.inputPartitions.toVector.map(_ -> Array(descriptor))).toOption.get
      certifiedBuild(plan, binding) match {
        case ShuffleRecoveryIdentityBuilt(result) => result
        case other => fail(s"expected certified identity, got $other")
      }
    }
    val first = identity("example.source", 1, 2)
    assert(first.canonicalPayload === identity("example.source", 1, 2).canonicalPayload)
    assert(first.digest !== identity("other.source", 1, 2).digest)
    assert(first.digest !== identity("example.source", 3, 2).digest)
    assert(first.digest !== identity("example.source", 1, 4).digest)
  }

  test("source bindings require exact plan and partition objects and own descriptor bytes") {
    val plan = batchPlan()
    val descriptor = Array[Byte](1)
    val certificate = Array[Byte](2)
    val entries = plan.inputPartitions.toVector.map(_ -> descriptor)
    val binding = ShuffleRecoverySourceBinding.bind(
      plan, "example.source", 1, certificate, entries).toOption.get
    val before = certifiedBuild(plan, binding)
    descriptor(0) = 9
    certificate(0) = 9
    assert(certifiedBuild(plan, binding) === before)
    assert(certifiedBuild(plan.copy(), binding) ===
      ShuffleRecoveryIdentityRejected(SourceTokenUnavailable))
    val lookalikes = entries.map { case (_, bytes) => new InputPartition {} -> bytes }
    assert(ShuffleRecoverySourceBinding.bind(
      plan, "example.source", 1, certificate, lookalikes) === Left(SourceTokenUnavailable))
    assert(ShuffleRecoverySourceBinding.bind(
      plan, "example.source", 1, certificate, entries.reverse) === Left(SourceTokenUnavailable))
  }

  test("uncertified batch scans and unsupported partition layouts refuse identity") {
    val plan = batchPlan()
    val inputs = ShuffleRecoveryResolvedIdentityInputs.create(
      Seq(plan -> ShuffleRecoverySourceToken.forProtocol("example.source", 1, Array[Byte](1))),
      decomposition(), Map.empty, semanticConfig(), "test-provider-v1")
    assert(ShuffleRecoveryComputationIdentityBuilder.build(hashExchange(plan), inputs) ===
      ShuffleRecoveryIdentityRejected(SourceTokenUnavailable))
    val entries = plan.inputPartitions.toVector.map(_ -> Array[Byte](1))
    assert(ShuffleRecoverySourceBinding.bind(plan.copy(runtimeFilters = Seq(Literal(true))),
      "example.source", 1, Array[Byte](1), entries) === Left(RuntimeFilterPresent))
    assert(ShuffleRecoverySourceBinding.bind(plan.copy(keyGroupedPartitioning = Some(Nil)),
      "example.source", 1, Array[Byte](1), entries) === Left(UnsupportedPartitioning))
    assert(ShuffleRecoverySourceBinding.bind(plan, "example.source", 1, Array[Byte](1),
      entries.take(1)) === Left(InvalidPartitionCount))
    assert(ShuffleRecoverySourceBinding.bind(plan, "example.source", 1, Array[Byte](1),
      entries.map { case (part, _) => part -> new Array[Byte](65537) }) ===
      Left(SourceTokenUnavailable))
  }

  test("partition key capability alone does not imply a grouped source read") {
    val plan = batchPlan(hasPartitionKey = true)
    val entries = plan.inputPartitions.toVector.map(_ -> Array[Byte](1))
    val binding = ShuffleRecoverySourceBinding.bind(
      plan, "example.source", 1, Array[Byte](2), entries).toOption.get
    assert(certifiedBuild(plan, binding).isInstanceOf[ShuffleRecoveryIdentityBuilt])
  }

  test("certified batch inputs bind publication identity to the actual shuffle dependency") {
    val plan = batchPlan()
    val binding = ShuffleRecoverySourceBinding.bind(plan, "example.source", 1, Array[Byte](2),
      plan.inputPartitions.toVector.map(_ -> Array[Byte](1))).toOption.get
    val exchange = hashExchange(plan, 3)
    val inputs = ShuffleRecoveryCertifiedBatchInputs.build(
      exchange, binding, "test-provider-v1").toOption.get
    val dependency = exchange.shuffleDependency
    val target = ShuffleRecoveryAdoptionTarget(ShuffleRecoveryMaterializationId(1, 1),
      dependency.shuffleId, 1, dependency.rdd.partitions.length, 3)
    val manifestIdentity = inputs.identityFor(target)
    assert(manifestIdentity.mapperCount === 2)
    assert(manifestIdentity.reducerCount === 3)
    assert(inputs.computation.compatibility.providerReadFormatId === "test-provider-v1")
    intercept[IllegalArgumentException] {
      inputs.identityFor(target.copy(mapperCount = 3))
    }
    assert(ShuffleRecoveryCertifiedBatchInputs.build(hashExchange(plan.copy()), binding,
      "test-provider-v1") === Left(SourceTokenUnavailable))
  }

  test("certified input preparation preserves ordinary reader factory failures") {
    val failure = new IllegalStateException("source reader factory failed")
    val plan = batchPlan(readerFailure = Some(failure))
    val binding = ShuffleRecoverySourceBinding.bind(plan, "example.source", 1, Array[Byte](2),
      plan.inputPartitions.toVector.map(_ -> Array[Byte](1))).toOption.get
    val thrown = intercept[IllegalStateException] {
      ShuffleRecoveryCertifiedBatchInputs.build(hashExchange(plan), binding, "test-provider-v1")
    }
    assert(thrown eq failure)
  }

  test("ordinary source planning failures are not converted to certification misses") {
    val failure = new IllegalStateException("source planning failed")
    val plan = batchPlan(Some(failure))
    val thrown = intercept[IllegalStateException] {
      ShuffleRecoverySourceBinding.bind(plan, "example.source", 1, Array[Byte](1),
        Vector(new InputPartition {} -> Array[Byte](1)))
    }
    assert(thrown eq failure)
  }

  test("equivalent independently planned shuffles produce byte-identical identities") {
    val firstPlan = filteredRangePlan()
    val secondPlan = filteredRangePlan()
    val first = build(hashExchange(firstPlan), firstPlan)
    val second = build(hashExchange(secondPlan), secondPlan)

    assert(first.canonicalPayload === second.canonicalPayload)
    assert(first.digest === second.digest)
  }

  test("attempt-local plan and object identity do not enter the semantic identity") {
    val firstPlan = projectedLiteralRange(7)
    val secondPlan = projectedLiteralRange(7)
    val firstExchange = hashExchange(firstPlan)
    val secondExchange = hashExchange(secondPlan)

    assert(firstPlan ne secondPlan)
    assert(firstExchange ne secondExchange)
    assert(build(firstExchange, firstPlan).canonicalPayload ===
      build(secondExchange, secondPlan).canonicalPayload)
  }

  test("literal, output nullability, partitioning, source and decomposition mutations differ") {
    val literalSeven = projectedLiteralRange(7)
    val literalEight = projectedLiteralRange(8)
    assert(build(hashExchange(literalSeven), literalSeven).digest !==
      build(hashExchange(literalEight), literalEight).digest)

    val nullable = projectedNullRange()
    assert(build(hashExchange(nullable), nullable).digest !==
      build(hashExchange(literalSeven), literalSeven).digest)

    assert(build(hashExchange(literalSeven, 2), literalSeven).digest !==
      build(hashExchange(literalSeven, 3), literalSeven).digest)

    val sourceA = build(hashExchange(literalSeven), literalSeven, source = "source-A")
    val sourceB = build(hashExchange(literalSeven), literalSeven, source = "source-B")
    assert(sourceA.digest !== sourceB.digest)

    val splitA = build(hashExchange(literalSeven), literalSeven, firstSplit = "split-0")
    val splitB = build(hashExchange(literalSeven), literalSeven, firstSplit = "split-mutated")
    assert(splitA.digest !== splitB.digest)
  }

  test("ANSI, session timezone and reviewed runtime formats are identity discriminators") {
    val plan = projectedLiteralRange(7)
    val exchange = hashExchange(plan)
    val base = build(exchange, plan, ansi = false, timeZone = "UTC")

    assert(base.digest !== build(exchange, plan, ansi = true, timeZone = "UTC").digest)
    assert(base.digest !==
      build(exchange, plan, ansi = false, timeZone = "America/Los_Angeles").digest)
    assert(base.digest !== build(
      exchange,
      plan,
      compressionBlockSize = 64 * 1024).digest)
    assert(base.digest !== build(
      exchange,
      plan,
      compressionEnabled = false).digest)

    val uncompressedA = build(
      exchange,
      plan,
      compressionEnabled = false,
      compressionCodec = "lz4")
    val uncompressedB = build(
      exchange,
      plan,
      compressionEnabled = false,
      compressionCodec = "inactive-codec")
    assert(uncompressedA.canonicalPayload === uncompressedB.canonicalPayload)
  }

  test("unreviewed compression and encryption modes refuse identity construction") {
    val plan = projectedLiteralRange(7)
    val exchange = hashExchange(plan)

    assert(buildResult(exchange, plan, compressionCodec = "zstd") ===
      ShuffleRecoveryIdentityRejected(UnsupportedShuffleMode))
    assert(buildResult(exchange, plan, compressionBlockSize = 32 * 1024 * 1024) ===
      ShuffleRecoveryIdentityRejected(UnsupportedShuffleMode))
    assert(buildResult(
      exchange,
      plan,
      overrides = BuildOverrides(encryptionEnabled = true)) ===
      ShuffleRecoveryIdentityRejected(UnsupportedShuffleMode))
  }

  test("resolved runtime values are ordered canonically and remain semantic") {
    val plan = projectedLiteralRange(7)
    val exchange = hashExchange(plan)
    val first = build(
      exchange,
      plan,
      overrides = BuildOverrides(resolvedValues = Map(
        "z" -> ShuffleRecoveryIntValue(2),
        "a" -> ShuffleRecoveryIntValue(1))))
    val second = build(
      exchange,
      plan,
      overrides = BuildOverrides(resolvedValues = List(
        "a" -> ShuffleRecoveryIntValue(1),
        "z" -> ShuffleRecoveryIntValue(2)).toMap))
    val mutated = build(
      exchange,
      plan,
      overrides = BuildOverrides(resolvedValues = Map(
        "a" -> ShuffleRecoveryIntValue(1),
        "z" -> ShuffleRecoveryIntValue(3))))

    assert(first.canonicalPayload === second.canonicalPayload)
    assert(first.digest !== mutated.digest)
  }

  test("raw malformed UTF8 values cannot collapse to one accepted identity") {
    val firstUtf8 = UTF8String.fromBytes(Array(0x80.toByte))
    val secondUtf8 = UTF8String.fromBytes(Array(0x81.toByte))
    assert(firstUtf8.toString === secondUtf8.toString)
    assert(!firstUtf8.isValid)
    assert(!secondUtf8.isValid)

    val firstPlan = projectedUtf8(firstUtf8, StringType)
    val secondPlan = projectedUtf8(secondUtf8, StringType)
    assert(buildResult(singleExchange(firstPlan), firstPlan) ===
      ShuffleRecoveryIdentityRejected(UnsupportedExpression))
    assert(buildResult(singleExchange(secondPlan), secondPlan) ===
      ShuffleRecoveryIdentityRejected(UnsupportedExpression))
  }

  test("valid UTF8 bytes remain stable and collation remains semantic") {
    val text = new String(
      Array(
        0xcf.toByte,
        0x80.toByte,
        0x2d.toByte,
        0xf0.toByte,
        0x9f.toByte,
        0x99.toByte,
        0x82.toByte),
      StandardCharsets.UTF_8)
    val first = projectedUtf8(UTF8String.fromString(text), StringType)
    val second = projectedUtf8(UTF8String.fromString(text), StringType)
    assert(build(singleExchange(first), first).canonicalPayload ===
      build(singleExchange(second), second).canonicalPayload)

    val lcaseId = CollationFactory.collationNameToId("UTF8_LCASE")
    val lcase = projectedUtf8(UTF8String.fromString(text), StringType(lcaseId))
    assert(build(singleExchange(first), first).digest !==
      build(singleExchange(lcase), lcase).digest)
  }

  test("accepted UTF8 literals defensively own backing bytes") {
    val raw = "stable".getBytes(StandardCharsets.UTF_8)
    val plan = projectedUtf8(UTF8String.fromBytes(raw), StringType)
    val identity = build(singleExchange(plan), plan)

    raw(0) = 'X'.toByte

    val expected = projectedUtf8(UTF8String.fromString("stable"), StringType)
    assert(identity.canonicalPayload === build(singleExchange(expected), expected).canonicalPayload)
  }

  test("oversized UTF8 literals refuse identity construction within the canonical bound") {
    val oversized = UTF8String.fromBytes(Array.fill[Byte](16 * 1024 + 1)('a'.toByte))
    val plan = projectedUtf8(oversized, StringType)

    assert(buildResult(singleExchange(plan), plan) ===
      ShuffleRecoveryIdentityRejected(UnsupportedExpression))
  }

  test("captured Pmod evaluation contexts are refused instead of inferred from session state") {
    val legacy = Pmod(
      Literal(7),
      Literal(0),
      NumericEvalContext(EvalMode.LEGACY, allowDecimalPrecisionLoss = false))
    val ansi = Pmod(
      Literal(7),
      Literal(0),
      NumericEvalContext(EvalMode.ANSI, allowDecimalPrecisionLoss = true))

    assert(legacy.eval(InternalRow.empty) == null)
    intercept[SparkArithmeticException] {
      ansi.eval(InternalRow.empty)
    }

    val legacyPlan = projectedExpression(legacy)
    val ansiPlan = projectedExpression(ansi)
    assert(buildResult(hashExchange(legacyPlan), legacyPlan, ansi = false) ===
      ShuffleRecoveryIdentityRejected(UnsupportedExpression))
    assert(buildResult(hashExchange(ansiPlan), ansiPlan, ansi = false) ===
      ShuffleRecoveryIdentityRejected(UnsupportedExpression))
  }

  test("direct Murmur3Hash is outside the closed expression allowlist") {
    val child = rangePlan()
    val hash = Murmur3Hash(Seq(child.output.head), 42)
    val project = ProjectExec(Seq(Alias(hash, "hash")()), child)

    assert(buildResult(hashExchange(project), project) ===
      ShuffleRecoveryIdentityRejected(UnsupportedExpression))
  }

  test("non-empty output metadata is refused until semantic metadata keys are reviewed") {
    val child = rangePlan()
    val metadata = new MetadataBuilder().putString("semantic-key", "value").build()
    val project = ProjectExec(
      Seq(Alias(Literal(7), "value")(explicitMetadata = Some(metadata))),
      child)

    assert(buildResult(hashExchange(project), project) ===
      ShuffleRecoveryIdentityRejected(DeterminismUnproven))
  }

  test("deep expression graphs refuse identity construction before codec materialization") {
    val child = rangePlan()
    val deep = (0 until 70).foldLeft[Expression](Literal(true)) { case (expression, _) =>
      Not(expression)
    }
    val project = ProjectExec(Seq(Alias(deep, "deep")()), child)

    assert(buildResult(hashExchange(project), project) ===
      ShuffleRecoveryIdentityRejected(DeterminismUnproven))
  }

  test("wide intermediate outputs refuse before ordinal-map materialization") {
    val child = rangePlan()
    val wide = ProjectExec(
      (0 until 4097).map(index => Alias(Literal(index), s"value_$index")()),
      child)
    val hash = Murmur3Hash(Seq(wide.output.head), 42)
    val outer = ProjectExec(Seq(Alias(hash, "hash")()), wide)

    assert(buildResult(singleExchange(outer), outer) ===
      ShuffleRecoveryIdentityRejected(DeterminismUnproven))
  }

  test("identity binds the selected provider format without choosing a provider implementation") {
    val plan = projectedLiteralRange(7)
    val first = build(hashExchange(plan), plan,
      overrides = BuildOverrides(providerReadFormatId = "provider-a-format-v1"))
    val second = build(hashExchange(plan), plan,
      overrides = BuildOverrides(providerReadFormatId = "provider-b-format-v1"))
    assert(first.compatibility.providerReadFormatId == "provider-a-format-v1")
    assert(second.compatibility.providerReadFormatId == "provider-b-format-v1")
    assert(first.canonicalPayload != second.canonicalPayload)
    assert(first.digest != second.digest)
  }

  test("missing source identity fails closed") {
    val plan = projectedLiteralRange(7)
    val exchange = hashExchange(plan)
    val inputs = ShuffleRecoveryResolvedIdentityInputs.create(
      Nil,
      decomposition(),
      Map.empty,
      semanticConfig(),
      "reference-shuffle-provider-v1")

    assert(ShuffleRecoveryComputationIdentityBuilder.build(exchange, inputs) ===
      ShuffleRecoveryIdentityRejected(SourceTokenUnavailable))
  }

  test("nondeterministic expressions fail closed before identity creation") {
    val child = rangePlan()
    val project = ProjectExec(Seq(Alias(Rand(7L), "random")()), child)
    val exchange = ShuffleExchangeExec(HashPartitioning(project.output.take(1), 2), project)

    assert(buildResult(exchange, project) === ShuffleRecoveryIdentityRejected(NonDeterministic))
  }

  test("unmodeled operators fail closed even when the opportunity allowlist knows them") {
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      val aggregatePlan = spark.range(0, 32, 1, 4).groupBy().count().queryExecution.executedPlan
      val exchange =
        ShuffleExchangeExec(HashPartitioning(aggregatePlan.output.take(1), 2), aggregatePlan)

      assert(buildResult(exchange, aggregatePlan) ===
        ShuffleRecoveryIdentityRejected(UnsupportedOperator))
    }
  }

  test("range partitioning remains outside the supported identity slice") {
    val plan = rangePlan()
    val ordering = Seq(SortOrder(plan.output.head, Ascending))
    val exchange = ShuffleExchangeExec(RangePartitioning(ordering, 2), plan)

    assert(buildResult(exchange, plan) ===
      ShuffleRecoveryIdentityRejected(RangePartitioningPresent))
  }

  test("string hash partitioning fails closed until collation hashing switches are modeled") {
    val plan = spark.range(0, 8, 1, 2)
      .selectExpr("cast(id as string) as key")
      .queryExecution.executedPlan
    val exchange = ShuffleExchangeExec(HashPartitioning(plan.output.take(1), 2), plan)

    assert(buildResult(exchange, plan) ===
      ShuffleRecoveryIdentityRejected(UnsupportedExpression))
  }

  private def batchPlan(
      failure: Option[RuntimeException] = None,
      hasPartitionKey: Boolean = false,
      readerFailure: Option[RuntimeException] = None): BatchScanExec = {
    val source = new Scan with Batch {
      override def readSchema(): StructType = new StructType().add("id", IntegerType)
      override def toBatch: Batch = this
      override def planInputPartitions(): Array[InputPartition] = {
        failure.foreach(error => throw error)
        Array.tabulate[InputPartition](2) { _ =>
          if (hasPartitionKey) {
            new InputPartition with HasPartitionKey {
              override def partitionKey(): InternalRow =
                throw new IllegalStateException("ungrouped reads must not request partition keys")
            }
          } else {
            new InputPartition {}
          }
        }
      }
      override def createReaderFactory(): PartitionReaderFactory = {
        readerFailure.foreach(error => throw error)
        new PartitionReaderFactory {
          override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
            throw new UnsupportedOperationException("identity tests must not execute source reads")
        }
      }
    }
    val table = new Table {
      override def name(): String = "certified-identity-test"
      override def schema(): StructType = source.readSchema()
      override def capabilities(): java.util.Set[TableCapability] =
        java.util.Collections.singleton(TableCapability.BATCH_READ)
    }
    BatchScanExec(Seq(AttributeReference("id", IntegerType)()), source, Nil, table = table)
  }

  private def certifiedBuild(plan: BatchScanExec, binding: ShuffleRecoverySourceBinding)
      : ShuffleRecoveryIdentityBuildResult = {
    val inputs = ShuffleRecoveryResolvedIdentityInputs.create(
      Nil, binding.decomposition, Map.empty, semanticConfig(), "test-provider-v1", Some(binding))
    ShuffleRecoveryComputationIdentityBuilder.build(hashExchange(plan), inputs)
  }

  private def rangePlan(): SparkPlan =
    spark.range(0, 32, 1, 4).queryExecution.executedPlan

  private def filteredRangePlan(): SparkPlan =
    spark.range(0, 32, 1, 4).where("id > 7").queryExecution.executedPlan

  private def projectedLiteralRange(value: Int): SparkPlan =
    projectedExpression(Literal(value))

  private def projectedNullRange(): SparkPlan =
    projectedExpression(Literal.create(null, IntegerType))

  private def projectedExpression(expression: Expression): SparkPlan = {
    val child = rangePlan()
    ProjectExec(Seq(Alias(expression, "value")()), child)
  }

  private def projectedUtf8(value: UTF8String, dataType: StringType): SparkPlan = {
    val child = rangePlan()
    ProjectExec(Seq(Alias(Literal.create(value, dataType), "value")()), child)
  }

  private def hashExchange(child: SparkPlan, partitions: Int = 2): ShuffleExchangeExec =
    ShuffleExchangeExec(HashPartitioning(child.output.take(1), partitions), child)

  private def singleExchange(child: SparkPlan): ShuffleExchangeExec =
    ShuffleExchangeExec(SinglePartition, child)

  private def build(
      exchange: ShuffleExchangeExec,
      child: SparkPlan,
      source: String = "source-A",
      firstSplit: String = "split-0",
      ansi: Boolean = false,
      timeZone: String = "UTC",
      compressionEnabled: Boolean = true,
      compressionCodec: String = "lz4",
      compressionBlockSize: Int = DefaultCompressionBlockSize,
      overrides: BuildOverrides = BuildOverrides())
      : ShuffleRecoveryComputationIdentity = {
    buildResult(
      exchange,
      child,
      source,
      firstSplit,
      ansi,
      timeZone,
      compressionEnabled,
      compressionCodec,
      compressionBlockSize,
      overrides) match {
      case ShuffleRecoveryIdentityBuilt(identity) => identity
      case ShuffleRecoveryIdentityRejected(reason) => fail(s"identity rejected: ${reason.code}")
    }
  }

  private def buildResult(
      exchange: ShuffleExchangeExec,
      child: SparkPlan,
      source: String = "source-A",
      firstSplit: String = "split-0",
      ansi: Boolean = false,
      timeZone: String = "UTC",
      compressionEnabled: Boolean = true,
      compressionCodec: String = "lz4",
      compressionBlockSize: Int = DefaultCompressionBlockSize,
      overrides: BuildOverrides = BuildOverrides())
      : ShuffleRecoveryIdentityBuildResult = {
    val range = rangeLeaf(child)
    val inputs = ShuffleRecoveryResolvedIdentityInputs.create(
      Seq(range -> ShuffleRecoverySourceToken.copyOf(
        1, source.getBytes(StandardCharsets.UTF_8))),
      decomposition(firstSplit),
      overrides.resolvedValues,
      semanticConfig(
        ansi,
        timeZone,
        compressionEnabled,
        compressionCodec,
        compressionBlockSize,
        overrides.encryptionEnabled),
      overrides.providerReadFormatId)
    ShuffleRecoveryComputationIdentityBuilder.build(exchange, inputs)
  }

  private def semanticConfig(
      ansi: Boolean = false,
      timeZone: String = "UTC",
      compressionEnabled: Boolean = true,
      compressionCodec: String = "lz4",
      compressionBlockSize: Int = DefaultCompressionBlockSize,
      encryptionEnabled: Boolean = false): ShuffleRecoveryIdentitySemanticConfig = {
    ShuffleRecoveryIdentitySemanticConfig(
      ansi,
      timeZone,
      compressionEnabled,
      compressionCodec,
      compressionBlockSize,
      encryptionEnabled)
  }

  private def rangeLeaf(plan: SparkPlan): RangeExec = {
    plan.collectFirst { case range: RangeExec => range }
      .getOrElse(fail(s"expected RangeExec below ${plan.nodeName}"))
  }

  private def decomposition(firstSplit: String = "split-0"): ShuffleRecoveryMapperDecomposition = {
    ShuffleRecoveryMapperDecomposition(
      4,
      (0 until 4).map { index =>
        val descriptor = if (index == 0) firstSplit else s"split-$index"
        ShuffleRecoveryMapperSplit.copyOf(
          0,
          index,
          1,
          descriptor.getBytes(StandardCharsets.UTF_8))
      }.toVector)
  }
}
