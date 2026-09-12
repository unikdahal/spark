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

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Arrays

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.expressions.{
  DynamicPruningExpression, Expression, Literal, Nondeterministic}
import org.apache.spark.sql.execution.{ExecSubqueryExpression, RangeExec, SparkPlan}

/**
 * Opaque, bounded source-read proof used by shuffle recovery identity construction.
 *
 * Generic recovery code may inspect only the envelope fields below. The bytes themselves are
 * source-specific and intentionally have no generic decoder. Adapters must describe the exact
 * immutable logical data view read by the already-resolved scan, not merely a mutable namespace.
 * A token is neither an authorization decision nor a lease over the underlying source.
 */
private[sql] final class ShuffleRecoverySourceToken private[exchange] (
    val schemaVersion: Int,
    val adapterId: String,
    tokenBytes: Array[Byte],
    val diagnosticCategory: Option[String],
    val diagnosticSummary: Option[String],
    val decomposition: ShuffleRecoveryMapperDecomposition) {

  private val ownedTokenBytes = tokenBytes.clone()

  def canonicalBytes: Array[Byte] = ownedTokenBytes.clone()

  override def equals(other: Any): Boolean = other match {
    case that: ShuffleRecoverySourceToken =>
      schemaVersion == that.schemaVersion &&
        adapterId == that.adapterId &&
        decomposition == that.decomposition &&
        Arrays.equals(ownedTokenBytes, that.ownedTokenBytes)
    case _ => false
  }

  override def hashCode(): Int = {
    var result = schemaVersion
    result = 31 * result + adapterId.hashCode
    result = 31 * result + decomposition.hashCode
    31 * result + Arrays.hashCode(ownedTokenBytes)
  }

  override def toString: String = {
    val summary = diagnosticSummary.getOrElse("")
    s"ShuffleRecoverySourceToken(v=$schemaVersion,adapter=$adapterId," +
      s"bytes=${ownedTokenBytes.length},category=${diagnosticCategory.getOrElse("")}," +
      s"summary=$summary,decomposition=${decomposition.code})"
  }
}

private[sql] sealed trait ShuffleRecoveryMapperDecomposition {
  def code: String
}

private[sql] object ShuffleRecoveryMapperDecomposition {
  case object Uncertified extends ShuffleRecoveryMapperDecomposition {
    override val code: String = "UNCERTIFIED"
  }

  final class Certified private[exchange] (certificateBytes: Array[Byte])
    extends ShuffleRecoveryMapperDecomposition {

    private val ownedCertificateBytes = certificateBytes.clone()

    override val code: String = "CERTIFIED"

    def canonicalBytes: Array[Byte] = ownedCertificateBytes.clone()

    override def equals(other: Any): Boolean = other match {
      case that: Certified =>
        Arrays.equals(ownedCertificateBytes, that.ownedCertificateBytes)
      case _ => false
    }

    override def hashCode(): Int = Arrays.hashCode(ownedCertificateBytes)
  }
}

private[sql] sealed trait ShuffleRecoverySourceIdentityMiss {
  def code: String
}

private[sql] object ShuffleRecoverySourceIdentityMiss {
  case object UnknownSource extends ShuffleRecoverySourceIdentityMiss {
    override val code: String = "UNKNOWN_SOURCE"
  }
  case object AdapterFailed extends ShuffleRecoverySourceIdentityMiss {
    override val code: String = "ADAPTER_FAILED"
  }
  case object NullCandidate extends ShuffleRecoverySourceIdentityMiss {
    override val code: String = "NULL_CANDIDATE"
  }
  case object UnsupportedTokenVersion extends ShuffleRecoverySourceIdentityMiss {
    override val code: String = "UNSUPPORTED_TOKEN_VERSION"
  }
  case object UnknownAdapter extends ShuffleRecoverySourceIdentityMiss {
    override val code: String = "UNKNOWN_ADAPTER"
  }
  case object InvalidAdapterId extends ShuffleRecoverySourceIdentityMiss {
    override val code: String = "INVALID_ADAPTER_ID"
  }
  case object EmptyToken extends ShuffleRecoverySourceIdentityMiss {
    override val code: String = "EMPTY_TOKEN"
  }
  case object OversizedToken extends ShuffleRecoverySourceIdentityMiss {
    override val code: String = "OVERSIZED_TOKEN"
  }
  case object InvalidDiagnostic extends ShuffleRecoverySourceIdentityMiss {
    override val code: String = "INVALID_DIAGNOSTIC"
  }
  case object DecompositionUncertified extends ShuffleRecoverySourceIdentityMiss {
    override val code: String = "DECOMPOSITION_UNCERTIFIED"
  }
  case object InvalidDecomposition extends ShuffleRecoverySourceIdentityMiss {
    override val code: String = "INVALID_DECOMPOSITION"
  }
  case object SourceViewUnavailable extends ShuffleRecoverySourceIdentityMiss {
    override val code: String = "SOURCE_VIEW_UNAVAILABLE"
  }
}

private[sql] sealed trait ShuffleRecoverySourceIdentityResult

private[sql] object ShuffleRecoverySourceIdentityResult {
  final case class Identified(token: ShuffleRecoverySourceToken)
    extends ShuffleRecoverySourceIdentityResult

  final case class Miss(reason: ShuffleRecoverySourceIdentityMiss)
    extends ShuffleRecoverySourceIdentityResult
}

/**
 * Untrusted adapter output. Validation copies byte arrays only after all size and shape checks
 * pass.
 */
private[sql] final case class ShuffleRecoverySourceTokenCandidate(
    schemaVersion: Int,
    adapterId: String,
    canonicalBytes: Array[Byte],
    diagnosticCategory: Option[String] = None,
    diagnosticSummary: Option[String] = None,
    decompositionCertificate: Option[Array[Byte]] = None)

private[sql] sealed trait ShuffleRecoverySourceAdapterResult

private[sql] object ShuffleRecoverySourceAdapterResult {
  final case class Candidate(value: ShuffleRecoverySourceTokenCandidate)
    extends ShuffleRecoverySourceAdapterResult

  case object Unavailable extends ShuffleRecoverySourceAdapterResult
}

private[sql] trait ShuffleRecoverySourceReadAdapter {
  def planClass: Class[_ <: SparkPlan]
  def adapterId: String

  /**
   * Resolve only facts already available at the materialization boundary.
   *
   * Implementations must not mutate the plan/source. Adapters that need future external I/O must
   * perform it before scheduler adoption and hand the resulting local token to recovery code.
   */
  def sourceToken(plan: SparkPlan): ShuffleRecoverySourceAdapterResult
}

/**
 * Exact-class adapter registry. There is deliberately no reflection or assignable-class fallback:
 * an unregistered source implementation is a recovery miss.
 */
private[sql] final class ShuffleRecoverySourceAdapterRegistry private[exchange] (
    adaptersByClass: Map[Class[_], ShuffleRecoverySourceReadAdapter],
    bounds: ShuffleRecoverySourceTokenBounds) {

  import ShuffleRecoverySourceIdentityMiss._
  import ShuffleRecoverySourceIdentityResult._

  def identify(plan: SparkPlan): ShuffleRecoverySourceIdentityResult = {
    if (plan == null) {
      Miss(UnknownSource)
    } else {
      adaptersByClass.get(plan.getClass) match {
        case None => Miss(UnknownSource)
        case Some(adapter) =>
          val adapterResult = try {
            adapter.sourceToken(plan)
          } catch {
            case NonFatal(_) => return Miss(AdapterFailed)
          }
          adapterResult match {
            case null => Miss(NullCandidate)
            case ShuffleRecoverySourceAdapterResult.Unavailable => Miss(SourceViewUnavailable)
            case ShuffleRecoverySourceAdapterResult.Candidate(candidate) =>
              ShuffleRecoverySourceReadIdentity.validateCandidate(
                candidate,
                Set(adapter.adapterId),
                bounds)
          }
      }
    }
  }
}

private[sql] final case class ShuffleRecoverySourceTokenBounds(
    maxTokenBytes: Int,
    maxDecompositionBytes: Int,
    maxAdapterIdBytes: Int,
    maxDiagnosticCategoryBytes: Int,
    maxDiagnosticSummaryBytes: Int) {

  require(maxTokenBytes > 0, "max token bytes must be positive")
  require(maxDecompositionBytes > 0, "max decomposition bytes must be positive")
  require(maxAdapterIdBytes > 0, "max adapter id bytes must be positive")
  require(maxDiagnosticCategoryBytes > 0, "max diagnostic category bytes must be positive")
  require(maxDiagnosticSummaryBytes > 0, "max diagnostic summary bytes must be positive")
}

private[sql] object ShuffleRecoverySourceReadIdentity {
  import ShuffleRecoveryMapperDecomposition._
  import ShuffleRecoverySourceIdentityMiss._
  import ShuffleRecoverySourceIdentityResult._

  private[exchange] val CurrentTokenSchemaVersion: Int = 1

  private[exchange] val DefaultBounds = ShuffleRecoverySourceTokenBounds(
    maxTokenBytes = 32 * 1024,
    maxDecompositionBytes = 4 * 1024,
    maxAdapterIdBytes = 128,
    maxDiagnosticCategoryBytes = 64,
    maxDiagnosticSummaryBytes = 256)

  private val rangeAdapter = new RangeSourceReadAdapter
  private val defaultRegistry = registry(Seq(rangeAdapter), DefaultBounds)

  def identify(plan: SparkPlan): ShuffleRecoverySourceIdentityResult = {
    defaultRegistry.identify(plan)
  }

  private[exchange] def registry(
      adapters: Seq[ShuffleRecoverySourceReadAdapter],
      bounds: ShuffleRecoverySourceTokenBounds = DefaultBounds)
      : ShuffleRecoverySourceAdapterRegistry = {
    require(adapters != null, "adapters must not be null")
    val pairs = adapters.map { adapter =>
      require(adapter != null, "adapter must not be null")
      require(adapter.planClass != null, "adapter plan class must not be null")
      require(validAdapterId(adapter.adapterId, bounds), "adapter id is invalid or oversized")
      adapter.planClass.asInstanceOf[Class[_]] -> adapter
    }
    require(pairs.map(_._1).distinct.size == pairs.size, "duplicate source adapter plan class")
    require(adapters.map(_.adapterId).distinct.size == adapters.size, "duplicate source adapter id")
    new ShuffleRecoverySourceAdapterRegistry(
      pairs.toMap,
      bounds)
  }

  private[exchange] def validateCandidate(
      candidate: ShuffleRecoverySourceTokenCandidate,
      knownAdapterIds: Set[String],
      bounds: ShuffleRecoverySourceTokenBounds): ShuffleRecoverySourceIdentityResult = {
    if (candidate == null) {
      return Miss(NullCandidate)
    }
    if (candidate.schemaVersion != CurrentTokenSchemaVersion) {
      return Miss(UnsupportedTokenVersion)
    }
    if (!validAdapterId(candidate.adapterId, bounds)) {
      return Miss(InvalidAdapterId)
    }
    if (!knownAdapterIds.contains(candidate.adapterId)) {
      return Miss(UnknownAdapter)
    }

    val tokenBytes = candidate.canonicalBytes
    if (tokenBytes == null || tokenBytes.isEmpty) {
      return Miss(EmptyToken)
    }
    if (tokenBytes.length > bounds.maxTokenBytes) {
      return Miss(OversizedToken)
    }
    if (!validOptionalText(
        candidate.diagnosticCategory,
        bounds.maxDiagnosticCategoryBytes,
        allowEmpty = false) ||
        !validOptionalText(
          candidate.diagnosticSummary,
          bounds.maxDiagnosticSummaryBytes,
          allowEmpty = false)) {
      return Miss(InvalidDiagnostic)
    }
    if (candidate.decompositionCertificate == null) {
      return Miss(InvalidDecomposition)
    }

    candidate.decompositionCertificate match {
      case None => Miss(DecompositionUncertified)
      case Some(certificate) if certificate == null || certificate.isEmpty =>
        Miss(InvalidDecomposition)
      case Some(certificate) if certificate.length > bounds.maxDecompositionBytes =>
        Miss(InvalidDecomposition)
      case Some(certificate) =>
        Identified(new ShuffleRecoverySourceToken(
          candidate.schemaVersion,
          candidate.adapterId,
          tokenBytes,
          candidate.diagnosticCategory,
          candidate.diagnosticSummary,
          new Certified(certificate)))
    }
  }

  private def validAdapterId(
      adapterId: String,
      bounds: ShuffleRecoverySourceTokenBounds): Boolean = {
    adapterId != null &&
      adapterId.nonEmpty &&
      adapterId.length <= bounds.maxAdapterIdBytes &&
      utf8Length(adapterId) <= bounds.maxAdapterIdBytes &&
      adapterId.forall { ch =>
        ch >= 'a' && ch <= 'z' ||
          ch >= 'A' && ch <= 'Z' ||
          ch >= '0' && ch <= '9' ||
          ch == '.' || ch == '_' || ch == '-'
      }
  }

  private def validOptionalText(
      value: Option[String],
      maximumBytes: Int,
      allowEmpty: Boolean): Boolean = {
    value != null && value.forall { text =>
      text != null &&
        (allowEmpty || text.nonEmpty) &&
        text.length <= maximumBytes &&
        utf8Length(text) <= maximumBytes
    }
  }

  private def utf8Length(value: String): Int = {
    value.getBytes(StandardCharsets.UTF_8).length
  }

  private final class RangeSourceReadAdapter extends ShuffleRecoverySourceReadAdapter {
    override val planClass: Class[_ <: SparkPlan] = classOf[RangeExec]
    override val adapterId: String = "spark.range.v1"

    override def sourceToken(plan: SparkPlan): ShuffleRecoverySourceAdapterResult = {
      val range = plan.asInstanceOf[RangeExec]
      if (range.numSlices <= 0) {
        return ShuffleRecoverySourceAdapterResult.Unavailable
      }

      val token = canonicalBytes { out =>
        out.writeInt(CurrentTokenSchemaVersion)
        writeUtf8(out, adapterId)
        out.writeLong(range.start)
        out.writeLong(range.end)
        out.writeLong(range.step)
        out.writeInt(range.numSlices)
      }
      val decomposition = sha256(canonicalBytes { out =>
        writeUtf8(out, "spark.range.decomposition.v1")
        out.writeInt(range.numSlices)
        out.writeLong(range.start)
        out.writeLong(range.end)
        out.writeLong(range.step)
      })

      ShuffleRecoverySourceAdapterResult.Candidate(
        ShuffleRecoverySourceTokenCandidate(
          schemaVersion = CurrentTokenSchemaVersion,
          adapterId = adapterId,
          canonicalBytes = token,
          diagnosticCategory = Some("synthetic-range"),
          diagnosticSummary = None,
          decompositionCertificate = Some(decomposition)))
    }
  }

  private[exchange] def canonicalBytes(
      write: DataOutputStream => Unit): Array[Byte] = {
    val buffer = new ByteArrayOutputStream()
    val out = new DataOutputStream(buffer)
    try {
      write(out)
      out.flush()
      buffer.toByteArray
    } finally {
      out.close()
    }
  }

  private[exchange] def writeUtf8(out: DataOutputStream, value: String): Unit = {
    val bytes = value.getBytes(StandardCharsets.UTF_8)
    out.writeInt(bytes.length)
    out.write(bytes)
  }

  private[exchange] def sha256(bytes: Array[Byte]): Array[Byte] = {
    MessageDigest.getInstance("SHA-256").digest(bytes)
  }
}

/**
 * Explicit policy inventory for driver-resolved values that can vary across replacement attempts.
 */
private[sql] sealed trait ShuffleRecoveryResolvedValueDisposition {
  def code: String
}

private[sql] object ShuffleRecoveryResolvedValueDisposition {
  case object IdentityMaterial extends ShuffleRecoveryResolvedValueDisposition {
    override val code: String = "IDENTITY_MATERIAL"
  }
  case object Unsupported extends ShuffleRecoveryResolvedValueDisposition {
    override val code: String = "UNSUPPORTED"
  }
  case object ProvablyNonSemantic extends ShuffleRecoveryResolvedValueDisposition {
    override val code: String = "PROVABLY_NON_SEMANTIC"
  }
}

private[sql] final case class ShuffleRecoveryResolvedValuePolicyEntry(
    name: String,
    disposition: ShuffleRecoveryResolvedValueDisposition,
    rationale: String)

private[sql] object ShuffleRecoveryResolvedValuePolicy {
  import ShuffleRecoveryResolvedValueDisposition._

  private val currentTimeLikeClassNames = Set(
    "org.apache.spark.sql.catalyst.expressions.CurrentDate",
    "org.apache.spark.sql.catalyst.expressions.CurrentTimestamp",
    "org.apache.spark.sql.catalyst.expressions.LocalTimestamp",
    "org.apache.spark.sql.catalyst.expressions.Now")

  private val runtimeFilterClassNames = Set(
    "org.apache.spark.sql.catalyst.expressions.BloomFilterMightContain")

  val inventory: Seq[ShuffleRecoveryResolvedValuePolicyEntry] = Seq(
    ShuffleRecoveryResolvedValuePolicyEntry(
      "resolved-literal",
      IdentityMaterial,
      "Resolved literal bytes and type semantics can change shuffle rows or bytes."),
    ShuffleRecoveryResolvedValuePolicyEntry(
      "current-date-time-expression-before-folding",
      Unsupported,
      "Recovery waits for normal analysis/optimization to resolve it to an identity literal."),
    ShuffleRecoveryResolvedValuePolicyEntry(
      "scalar-subquery",
      Unsupported,
      "The supported slice does not certify independently evaluated scalar-subquery state."),
    ShuffleRecoveryResolvedValuePolicyEntry(
      "dynamic-partition-pruning",
      Unsupported,
      "The final runtime pruning input set is not yet part of source identity."),
    ShuffleRecoveryResolvedValuePolicyEntry(
      "runtime-filter",
      Unsupported,
      "The final runtime filter state is not yet certified by the source adapter."),
    ShuffleRecoveryResolvedValuePolicyEntry(
      "session-time-zone",
      IdentityMaterial,
      "Time-zone-sensitive resolved values and casts must be represented by computation identity."),
    ShuffleRecoveryResolvedValuePolicyEntry(
      "ansi-and-type-coercion-semantics",
      IdentityMaterial,
      "ANSI/coercion changes can alter values, errors, and encoded shuffle rows."),
    ShuffleRecoveryResolvedValuePolicyEntry(
      "locale-and-collation-semantics",
      IdentityMaterial,
      "Any supported locale/collation semantics must be represented explicitly."),
    ShuffleRecoveryResolvedValuePolicyEntry(
      "resolved-catalog-table-version",
      IdentityMaterial,
      "A concrete immutable source version is semantic even when the namespace is unchanged."),
    ShuffleRecoveryResolvedValuePolicyEntry(
      "analysis-or-optimizer-produced-literal",
      IdentityMaterial,
      "The normally resolved value, not submitted SQL text, is the semantic input."),
    ShuffleRecoveryResolvedValuePolicyEntry(
      "nondeterministic-seed-or-process-session-state",
      Unsupported,
      "Process/session-derived state is not stable across replacement attempts."),
    ShuffleRecoveryResolvedValuePolicyEntry(
      "attempt-local-shuffle-stage-task-and-query-ids",
      ProvablyNonSemantic,
      "Attempt-local routing identifiers do not describe the producer computation."))

  def classifyExpression(expression: Expression): ShuffleRecoveryResolvedValueDisposition = {
    if (expression == null) {
      Unsupported
    } else if (expression.isInstanceOf[DynamicPruningExpression] ||
        runtimeFilterClassNames.contains(expression.getClass.getName) ||
        expression.isInstanceOf[ExecSubqueryExpression] ||
        expression.isInstanceOf[Nondeterministic]) {
      Unsupported
    } else if (currentTimeLikeClassNames.contains(expression.getClass.getName)) {
      Unsupported
    } else {
      expression match {
        case _: Literal => IdentityMaterial
        case _ => Unsupported
      }
    }
  }
}
