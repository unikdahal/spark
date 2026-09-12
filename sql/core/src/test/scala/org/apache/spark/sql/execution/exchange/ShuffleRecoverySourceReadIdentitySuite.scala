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
import java.nio.file.{Files, Paths}
import java.util.Arrays

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  AttributeReference,
  BloomFilterMightContain,
  DynamicPruningExpression,
  Literal,
  UnsafeProjection}
import org.apache.spark.sql.execution.{LeafExecNode, RangeExec, SparkPlan}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

private[exchange] final case class ReferenceSnapshotScanExec(
    sourceId: String,
    sourceName: String,
    snapshotVersion: Long,
    schemaFingerprint: String,
    projection: Seq[String],
    filter: Option[String],
    readOptions: Map[String, String],
    selectedObjects: Seq[String],
    splits: Seq[String],
    retained: Boolean = true) extends LeafExecNode {

  override val output: Seq[Attribute] =
    Seq(AttributeReference("value", IntegerType, nullable = false)())

  override protected def doExecute(): RDD[InternalRow] = {
    if (splits.isEmpty) {
      sparkContext.emptyRDD[InternalRow]
    } else {
      sparkContext
        .parallelize(ReferenceSnapshotScanExec.rowsForSnapshot(snapshotVersion), splits.size)
        .mapPartitions[InternalRow] { values =>
          val projection = UnsafeProjection.create(output, output)
          values.map(value => projection(InternalRow(value)).copy())
        }
    }
  }
}

private[exchange] object ReferenceSnapshotScanExec {
  def rowsForSnapshot(snapshotVersion: Long): Seq[Int] = snapshotVersion match {
    case 1L => Seq(1, 2, 3)
    case 2L => Seq(10, 20, 30)
    case other => Seq(Math.floorMod(other, Int.MaxValue.toLong).toInt)
  }
}

private[exchange] final class ReferenceSnapshotSourceAdapter
  extends ShuffleRecoverySourceReadAdapter {

  import ShuffleRecoverySourceAdapterResult._

  override val planClass: Class[_ <: SparkPlan] = classOf[ReferenceSnapshotScanExec]
  override val adapterId: String = "spark.test.reference-snapshot.v1"

  private val maxMetadataBytes = 16 * 1024
  private val maxMetadataEntries = 4096

  override def sourceToken(plan: SparkPlan): ShuffleRecoverySourceAdapterResult = {
    val scan = plan.asInstanceOf[ReferenceSnapshotScanExec]
    if (!scan.retained ||
        scan.sourceId == null ||
        scan.sourceId.isEmpty ||
        scan.snapshotVersion <= 0 ||
        !boundedMetadata(scan)) {
      return Unavailable
    }

    val tokenBytes = ShuffleRecoverySourceReadIdentity.canonicalBytes { out =>
      out.writeInt(ShuffleRecoverySourceReadIdentity.CurrentTokenSchemaVersion)
      ShuffleRecoverySourceReadIdentity.writeUtf8(out, adapterId)
      ShuffleRecoverySourceReadIdentity.writeUtf8(out, scan.sourceId)
      out.writeLong(scan.snapshotVersion)
      ShuffleRecoverySourceReadIdentity.writeUtf8(out, scan.schemaFingerprint)
      writeStrings(out, scan.projection)
      writeOptional(out, scan.filter)
      val options = scan.readOptions.toSeq.sortBy { case (key, value) => (key, value) }
      out.writeInt(options.size)
      options.foreach { case (key, value) =>
        ShuffleRecoverySourceReadIdentity.writeUtf8(out, key)
        ShuffleRecoverySourceReadIdentity.writeUtf8(out, value)
      }
      writeStrings(out, scan.selectedObjects.sorted)
    }

    val decomposition = ShuffleRecoverySourceReadIdentity.sha256(
      ShuffleRecoverySourceReadIdentity.canonicalBytes { out =>
        ShuffleRecoverySourceReadIdentity.writeUtf8(
          out,
          "spark.test.reference-snapshot.decomposition.v1")
        writeStrings(out, scan.splits)
      })

    Candidate(ShuffleRecoverySourceTokenCandidate(
      schemaVersion = ShuffleRecoverySourceReadIdentity.CurrentTokenSchemaVersion,
      adapterId = adapterId,
      canonicalBytes = tokenBytes,
      diagnosticCategory = Some("reference-snapshot"),
      diagnosticSummary = Option(scan.sourceName).filter(_.nonEmpty),
      decompositionCertificate = Some(decomposition)))
  }

  private def boundedMetadata(scan: ReferenceSnapshotScanExec): Boolean = {
    val entries =
      scan.projection.size.toLong +
        scan.readOptions.size.toLong * 2L +
        scan.selectedObjects.size.toLong +
        scan.splits.size.toLong
    if (entries > maxMetadataEntries) {
      false
    } else {
      val strings =
        Seq(scan.sourceId, scan.schemaFingerprint) ++
          scan.projection ++
          scan.filter.toSeq ++
          scan.readOptions.iterator.flatMap { case (key, value) => Iterator(key, value) }.toSeq ++
          scan.selectedObjects ++
          scan.splits
      if (strings.exists(_ == null)) {
        false
      } else {
        var totalBytes = 0L
        strings.forall { value =>
          if (value.length > maxMetadataBytes) {
            false
          } else {
            totalBytes += value.getBytes(StandardCharsets.UTF_8).length.toLong
            totalBytes <= maxMetadataBytes
          }
        }
      }
    }
  }

  private def writeStrings(
      out: java.io.DataOutputStream,
      values: Seq[String]): Unit = {
    out.writeInt(values.size)
    values.foreach(value => ShuffleRecoverySourceReadIdentity.writeUtf8(out, value))
  }

  private def writeOptional(
      out: java.io.DataOutputStream,
      value: Option[String]): Unit = {
    value match {
      case None => out.writeBoolean(false)
      case Some(text) =>
        out.writeBoolean(true)
        ShuffleRecoverySourceReadIdentity.writeUtf8(out, text)
    }
  }
}

private[exchange] final class ThrowingReferenceSnapshotSourceAdapter
  extends ShuffleRecoverySourceReadAdapter {

  override val planClass: Class[_ <: SparkPlan] = classOf[ReferenceSnapshotScanExec]
  override val adapterId: String = "spark.test.throwing-reference.v1"

  override def sourceToken(plan: SparkPlan): ShuffleRecoverySourceAdapterResult = {
    throw new IllegalStateException("simulated transient source-token failure")
  }
}

class ShuffleRecoverySourceReadIdentitySuite extends SharedSparkSession {
  import ShuffleRecoveryMapperDecomposition._
  import ShuffleRecoveryResolvedValueDisposition._
  import ShuffleRecoverySourceIdentityMiss._
  import ShuffleRecoverySourceIdentityResult._

  private val referenceAdapter = new ReferenceSnapshotSourceAdapter
  private val referenceRegistry =
    ShuffleRecoverySourceReadIdentity.registry(Seq(referenceAdapter))

  private def scan(
      snapshotVersion: Long = 1L,
      sourceName: String = "orders",
      projection: Seq[String] = Seq("value"),
      filter: Option[String] = Some("value >= 0"),
      readOptions: Map[String, String] = Map("mode" -> "strict", "case" -> "sensitive"),
      selectedObjects: Seq[String] = Seq("part-000", "part-001"),
      splits: Seq[String] = Seq("part-000:0-9", "part-001:0-9"),
      retained: Boolean = true): ReferenceSnapshotScanExec = {
    ReferenceSnapshotScanExec(
      sourceId = "source-8d570c7e",
      sourceName = sourceName,
      snapshotVersion = snapshotVersion,
      schemaFingerprint = "schema-v1:id:int:not-null",
      projection = projection,
      filter = filter,
      readOptions = readOptions,
      selectedObjects = selectedObjects,
      splits = splits,
      retained = retained)
  }

  private def token(result: ShuffleRecoverySourceIdentityResult): ShuffleRecoverySourceToken = {
    result match {
      case Identified(value) => value
      case Miss(reason) => fail(s"expected source token but got ${reason.code}")
    }
  }

  private def decompositionBytes(token: ShuffleRecoverySourceToken): Array[Byte] = {
    token.decomposition match {
      case certified: Certified => certified.canonicalBytes
      case Uncertified => fail("expected certified mapper decomposition")
    }
  }

  test("same immutable snapshot and resolved options produce the same token") {
    val first = token(referenceRegistry.identify(scan()))
    val second = token(referenceRegistry.identify(scan(
      readOptions = List("case" -> "sensitive", "mode" -> "strict").toMap,
      selectedObjects = Seq("part-001", "part-000"))))

    assert(first === second)
    assert(Arrays.equals(first.canonicalBytes, second.canonicalBytes))
    assert(Arrays.equals(decompositionBytes(first), decompositionBytes(second)))
  }

  test("same source name resolving to a new immutable snapshot changes token and rows") {
    val oldPlan = scan(snapshotVersion = 1L)
    val newPlan = scan(snapshotVersion = 2L)
    val oldSnapshot = token(referenceRegistry.identify(oldPlan))
    val newSnapshot = token(referenceRegistry.identify(newPlan))

    assert(!Arrays.equals(oldSnapshot.canonicalBytes, newSnapshot.canonicalBytes))
    assert(oldPlan.executeCollect().map(_.getInt(0)).toSeq === Seq(1, 2, 3))
    assert(newPlan.executeCollect().map(_.getInt(0)).toSeq === Seq(10, 20, 30))
  }

  test("pinned old snapshot remains stable while the current snapshot advances") {
    val pinnedBefore = token(referenceRegistry.identify(scan(snapshotVersion = 1L)))
    val currentAfterAdvance = token(referenceRegistry.identify(scan(snapshotVersion = 2L)))
    val pinnedAfter = token(referenceRegistry.identify(scan(snapshotVersion = 1L)))

    assert(pinnedBefore === pinnedAfter)
    assert(pinnedBefore !== currentAfterAdvance)
  }

  test("projection filter and semantic read option changes alter the source token") {
    val base = token(referenceRegistry.identify(scan()))

    val changedProjection = token(referenceRegistry.identify(
      scan(projection = Seq("value", "derived"))))
    val changedFilter = token(referenceRegistry.identify(
      scan(filter = Some("value >= 10"))))
    val changedOption = token(referenceRegistry.identify(
      scan(readOptions = Map("mode" -> "permissive", "case" -> "sensitive"))))

    assert(base !== changedProjection)
    assert(base !== changedFilter)
    assert(base !== changedOption)
  }

  test("diagnostic namespace changes do not alter semantic source identity") {
    val first = token(referenceRegistry.identify(scan(sourceName = "orders")))
    val renamed = token(referenceRegistry.identify(scan(sourceName = "orders_alias")))

    assert(first === renamed)
    assert(first.diagnosticSummary.contains("orders"))
    assert(renamed.diagnosticSummary.contains("orders_alias"))
  }

  test("ordered split decomposition is separately certified") {
    val base = token(referenceRegistry.identify(scan()))
    val changedSet = token(referenceRegistry.identify(
      scan(splits = Seq("part-000:0-9", "part-001:10-19"))))
    val reordered = token(referenceRegistry.identify(
      scan(splits = Seq("part-001:0-9", "part-000:0-9"))))

    assert(Arrays.equals(base.canonicalBytes, changedSet.canonicalBytes))
    assert(!Arrays.equals(decompositionBytes(base), decompositionBytes(changedSet)))
    assert(!Arrays.equals(decompositionBytes(base), decompositionBytes(reordered)))
  }

  test("retained snapshot removal fails closed without fabricating a token") {
    assert(referenceRegistry.identify(scan(retained = false)) === Miss(SourceViewUnavailable))
  }

  test("mutable path-only and unknown leaf sources fail closed") {
    val pathOnly = MutablePathOnlyScanExec("/tmp/data", 12L, 99L)
    val unknown = UnknownSourceScanExec()

    assert(ShuffleRecoverySourceReadIdentity.identify(pathOnly) === Miss(UnknownSource))
    assert(ShuffleRecoverySourceReadIdentity.identify(unknown) === Miss(UnknownSource))
  }

  test("range source has a deterministic token and mapper certificate") {
    def rangeLeaf(partitions: Int): RangeExec = {
      spark
        .range(0, 100, 1, partitions)
        .queryExecution
        .executedPlan
        .collectFirst { case range: RangeExec => range }
        .get
    }

    val firstPlan = rangeLeaf(4)
    val secondPlan = rangeLeaf(4)
    val changedPlan = rangeLeaf(5)

    val first = token(ShuffleRecoverySourceReadIdentity.identify(firstPlan))
    val second = token(ShuffleRecoverySourceReadIdentity.identify(secondPlan))
    val changed = token(ShuffleRecoverySourceReadIdentity.identify(changedPlan))

    assert(first === second)
    assert(first !== changed)
  }

  test("selected source adapter cannot return another registered adapter id") {
    val otherAdapter = new ShuffleRecoverySourceReadAdapter {
      override val planClass: Class[_ <: SparkPlan] = classOf[RangeExec]
      override val adapterId: String = "spark.test.other-source.v1"

      override def sourceToken(plan: SparkPlan): ShuffleRecoverySourceAdapterResult = {
        ShuffleRecoverySourceAdapterResult.Unavailable
      }
    }
    val selectedAdapter = new ShuffleRecoverySourceReadAdapter {
      override val planClass: Class[_ <: SparkPlan] = classOf[ReferenceSnapshotScanExec]
      override val adapterId: String = "spark.test.selected-source.v1"

      override def sourceToken(plan: SparkPlan): ShuffleRecoverySourceAdapterResult = {
        ShuffleRecoverySourceAdapterResult.Candidate(
          ShuffleRecoverySourceTokenCandidate(
            ShuffleRecoverySourceReadIdentity.CurrentTokenSchemaVersion,
            otherAdapter.adapterId,
            Array[Byte](1),
            decompositionCertificate = Some(Array[Byte](2))))
      }
    }
    val registry = ShuffleRecoverySourceReadIdentity.registry(
      Seq(selectedAdapter, otherAdapter))

    assert(registry.identify(scan()) === Miss(UnknownAdapter))
  }

  test("adapter failure is a recovery miss and does not mutate ordinary execution") {
    val plan = scan()
    val throwingRegistry = ShuffleRecoverySourceReadIdentity.registry(
      Seq(new ThrowingReferenceSnapshotSourceAdapter))

    assert(throwingRegistry.identify(plan) === Miss(AdapterFailed))
    assert(plan.executeCollect().map(_.getInt(0)).toSeq === Seq(1, 2, 3))
  }

  test("untrusted token fields are bounded and fail closed before accepted-state copies") {
    val bounds = ShuffleRecoverySourceReadIdentity.DefaultBounds
    val validBytes = Array[Byte](1, 2, 3)
    val validDecomposition = Array[Byte](4, 5, 6)
    val valid = ShuffleRecoverySourceTokenCandidate(
      ShuffleRecoverySourceReadIdentity.CurrentTokenSchemaVersion,
      referenceAdapter.adapterId,
      validBytes,
      decompositionCertificate = Some(validDecomposition))

    def validate(
        candidate: ShuffleRecoverySourceTokenCandidate): ShuffleRecoverySourceIdentityResult = {
      ShuffleRecoverySourceReadIdentity.validateCandidate(
        candidate,
        Set(referenceAdapter.adapterId),
        bounds)
    }

    assert(validate(null) === Miss(NullCandidate))
    assert(validate(valid.copy(schemaVersion = 2)) === Miss(UnsupportedTokenVersion))
    assert(validate(valid.copy(adapterId = "other.adapter")) === Miss(UnknownAdapter))
    assert(validate(valid.copy(adapterId = "")) === Miss(InvalidAdapterId))
    assert(validate(valid.copy(canonicalBytes = Array.emptyByteArray)) === Miss(EmptyToken))
    assert(validate(valid.copy(
      canonicalBytes = new Array[Byte](bounds.maxTokenBytes + 1))) === Miss(OversizedToken))
    assert(validate(valid.copy(
      diagnosticSummary = Some("x" * (bounds.maxDiagnosticSummaryBytes + 1)))) ===
      Miss(InvalidDiagnostic))
    assert(validate(valid.copy(decompositionCertificate = null)) ===
      Miss(InvalidDecomposition))
    assert(validate(valid.copy(decompositionCertificate = None)) ===
      Miss(DecompositionUncertified))
    assert(validate(valid.copy(decompositionCertificate = Some(Array.emptyByteArray))) ===
      Miss(InvalidDecomposition))
    assert(validate(valid.copy(
      decompositionCertificate =
        Some(new Array[Byte](bounds.maxDecompositionBytes + 1)))) ===
      Miss(InvalidDecomposition))
  }

  test("accepted token and decomposition defensively own adapter buffers") {
    val tokenBytes = Array[Byte](7, 8, 9)
    val decomposition = Array[Byte](10, 11, 12)
    val accepted = token(ShuffleRecoverySourceReadIdentity.validateCandidate(
      ShuffleRecoverySourceTokenCandidate(
        ShuffleRecoverySourceReadIdentity.CurrentTokenSchemaVersion,
        referenceAdapter.adapterId,
        tokenBytes,
        decompositionCertificate = Some(decomposition)),
      Set(referenceAdapter.adapterId),
      ShuffleRecoverySourceReadIdentity.DefaultBounds))

    tokenBytes(0) = 99
    decomposition(0) = 99
    val exposedToken = accepted.canonicalBytes
    val exposedDecomposition = decompositionBytes(accepted)
    exposedToken(0) = 88
    exposedDecomposition(0) = 88

    assert(accepted.canonicalBytes.toSeq === Seq[Byte](7, 8, 9))
    assert(decompositionBytes(accepted).toSeq === Seq[Byte](10, 11, 12))
  }

  test("oversized reference metadata is refused before canonical token construction") {
    val oversizedAscii = scan(
      selectedObjects = Seq("x" * (16 * 1024 + 1)),
      splits = Seq("split"))
    val multibyte = 0x20ac.toChar.toString
    val oversizedUtf8 = scan(
      selectedObjects = Seq(multibyte * (6 * 1024)),
      splits = Seq("split"))

    assert(referenceRegistry.identify(oversizedAscii) === Miss(SourceViewUnavailable))
    assert(referenceRegistry.identify(oversizedUtf8) === Miss(SourceViewUnavailable))
  }

  test("resolved-value policy is explicit and keeps DPP and runtime filters unsupported") {
    val entries = ShuffleRecoveryResolvedValuePolicy.inventory.map { entry =>
      entry.name -> entry.disposition
    }.toMap

    assert(entries("resolved-literal") === IdentityMaterial)
    assert(entries("current-date-time-expression-before-folding") === Unsupported)
    assert(entries("scalar-subquery") === Unsupported)
    assert(entries("dynamic-partition-pruning") === Unsupported)
    assert(entries("runtime-filter") === Unsupported)
    assert(entries("session-time-zone") === IdentityMaterial)
    assert(entries("ansi-and-type-coercion-semantics") === IdentityMaterial)
    assert(entries("locale-and-collation-semantics") === IdentityMaterial)
    assert(entries("resolved-catalog-table-version") === IdentityMaterial)
    assert(entries("analysis-or-optimizer-produced-literal") === IdentityMaterial)
    assert(entries("nondeterministic-seed-or-process-session-state") === Unsupported)
    assert(entries("attempt-local-shuffle-stage-task-and-query-ids") === ProvablyNonSemantic)

    val dpp = DynamicPruningExpression(Literal(true))
    val runtimeFilter = BloomFilterMightContain(Literal(Array[Byte](1)), Literal(1L))
    assert(ShuffleRecoveryResolvedValuePolicy.classifyExpression(Literal(1)) === IdentityMaterial)
    assert(ShuffleRecoveryResolvedValuePolicy.classifyExpression(dpp) === Unsupported)
    assert(ShuffleRecoveryResolvedValuePolicy.classifyExpression(runtimeFilter) === Unsupported)
  }

  private case class MutablePathOnlyScanExec(path: String, length: Long, mtime: Long)
    extends LeafExecNode {

    override val output: Seq[Attribute] =
      Seq(AttributeReference("value", IntegerType, nullable = false)())

    override protected def doExecute(): RDD[InternalRow] = sparkContext.emptyRDD[InternalRow]
  }

  private case class UnknownSourceScanExec() extends LeafExecNode {
    override val output: Seq[Attribute] =
      Seq(AttributeReference("value", IntegerType, nullable = false)())

    override protected def doExecute(): RDD[InternalRow] = sparkContext.emptyRDD[InternalRow]
  }
}

object ShuffleRecoverySourceReadIdentityProcess {
  import ShuffleRecoveryMapperDecomposition.Certified
  import ShuffleRecoverySourceIdentityResult._

  def main(args: Array[String]): Unit = {
    require(args.length == 1, "usage: ShuffleRecoverySourceReadIdentityProcess <output>")
    val registry = ShuffleRecoverySourceReadIdentity.registry(
      Seq(new ReferenceSnapshotSourceAdapter))
    val plan = ReferenceSnapshotScanExec(
      sourceId = "source-cross-jvm",
      sourceName = "orders",
      snapshotVersion = 7L,
      schemaFingerprint = "schema-v1:id:int:not-null",
      projection = Seq("value"),
      filter = Some("value >= 0"),
      readOptions = Map("case" -> "sensitive", "mode" -> "strict"),
      selectedObjects = Seq("part-000", "part-001"),
      splits = Seq("part-000:0-9", "part-001:0-9"))

    val fingerprint = registry.identify(plan) match {
      case Identified(token) =>
        val decomposition = token.decomposition.asInstanceOf[Certified]
        Seq(
          token.schemaVersion.toString,
          token.adapterId,
          hex(token.canonicalBytes),
          hex(decomposition.canonicalBytes)).mkString("|")
      case Miss(reason) =>
        throw new IllegalStateException(s"reference source unexpectedly missed: ${reason.code}")
    }

    Files.write(
      Paths.get(args(0)),
      (fingerprint + "\n").getBytes(StandardCharsets.UTF_8))
  }

  private def hex(bytes: Array[Byte]): String = {
    val digits = "0123456789abcdef"
    val chars = new Array[Char](bytes.length * 2)
    var index = 0
    while (index < bytes.length) {
      val value = bytes(index) & 0xff
      chars(index * 2) = digits.charAt(value >>> 4)
      chars(index * 2 + 1) = digits.charAt(value & 0x0f)
      index += 1
    }
    new String(chars)
  }
}
