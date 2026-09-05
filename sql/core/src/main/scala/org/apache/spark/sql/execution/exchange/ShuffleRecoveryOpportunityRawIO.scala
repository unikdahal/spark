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

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}

/** Strict persisted shape for weighted opportunity evidence. */
private[sql] case class ShuffleRecoveryRawOpportunityRecord(
    schemaVersion: Int,
    executionId: String,
    exchangeOrdinal: Long,
    exchangePath: String,
    childOperatorClass: String,
    partitioningClass: String,
    partitionCount: Option[Int],
    ruleSetName: String,
    ruleSetVersion: Int,
    eligible: Boolean,
    immediateMissReason: Option[String],
    rootMissReason: Option[String],
    sourceTokenAvailability: String,
    lineageDeterminism: String,
    dppPresent: Boolean,
    runtimeFilterPresent: Boolean,
    subqueryPresent: Boolean,
    windowPresent: Boolean,
    expandPresent: Boolean,
    cacheScanPresent: Boolean,
    adaptivePartitionSpecsPresent: Boolean,
    pythonOrArrowPresent: Boolean,
    reusedExchange: Boolean,
    adaptivePlan: Boolean,
    pipelinedShuffle: Boolean,
    pushBasedShuffleEnabled: Boolean,
    mergedShuffleEnabled: Boolean,
    incompatibleRuntimeFlags: Seq[String],
    disposition: String,
    accountingReason: Option[String],
    stageId: Option[Int],
    stageAttemptId: Option[Int],
    shuffleId: Option[Int],
    mapperCount: Option[Int],
    shuffleWriteBytes: Option[Long],
    executorRunTimeMs: Option[Long],
    completionOrder: Option[Long])

/**
 * Treats persisted evidence as untrusted input.
 *
 * Parsing is bounded before Jackson allocation and then validates exact fields, numeric bounds,
 * enum-like codes, and disposition-specific shape. Malformed evidence therefore cannot silently
 * alter a denominator or become a permissive compatibility path.
 */
private[sql] object ShuffleRecoveryOpportunityRawIO {
  import ShuffleRecoveryLineageDeterminism._
  import ShuffleRecoveryMissReason._
  import ShuffleRecoverySourceTokenAvailability._

  val SchemaVersion = 1
  private val MaxRecordCharacters = 1024 * 1024

  private val mapper = new ObjectMapper()
    .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
  private val fields = Set(
    "schemaVersion",
    "executionId",
    "exchangeOrdinal",
    "exchangePath",
    "childOperatorClass",
    "partitioningClass",
    "partitionCount",
    "ruleSetName",
    "ruleSetVersion",
    "eligible",
    "immediateMissReason",
    "rootMissReason",
    "sourceTokenAvailability",
    "lineageDeterminism",
    "dppPresent",
    "runtimeFilterPresent",
    "subqueryPresent",
    "windowPresent",
    "expandPresent",
    "cacheScanPresent",
    "adaptivePartitionSpecsPresent",
    "pythonOrArrowPresent",
    "reusedExchange",
    "adaptivePlan",
    "pipelinedShuffle",
    "pushBasedShuffleEnabled",
    "mergedShuffleEnabled",
    "incompatibleRuntimeFlags",
    "disposition",
    "accountingReason",
    "stageId",
    "stageAttemptId",
    "shuffleId",
    "mapperCount",
    "shuffleWriteBytes",
    "executorRunTimeMs",
    "completionOrder")
  private val dispositions = Set("WEIGHTED", "UNWEIGHTED", "EXCLUDED")
  private val sourceTokenCodes = Set(
    Exact.code,
    PrototypeSpecialCased.code,
    Unavailable.code)
  private val lineageCodes = Set(
    Determinate.code,
    Unknown.code,
    Unordered.code,
    Indeterminate.code)
  private val missReasonCodes = Set(
    NonDeterministic.code,
    DeterminismUnproven.code,
    PythonOrArrowPresent.code,
    DynamicPruningPresent.code,
    RuntimeFilterPresent.code,
    SubqueryPresent.code,
    SourceTokenUnavailable.code,
    WindowPresent.code,
    ExpandPresent.code,
    CacheScanPresent.code,
    RangePartitioningPresent.code,
    AdaptivePartitionSpecPresent.code,
    UnsupportedShuffleMode.code,
    IncompatibleRuntimeFlag.code,
    InvalidPartitionCount.code,
    UnsupportedPartitioning.code,
    UnsupportedExpression.code,
    CustomOperator.code,
    UnsupportedOperator.code,
    UpstreamIneligible.code)

  def parseLine(line: String): ShuffleRecoveryRawOpportunityRecord = {
    require(line != null && line.nonEmpty, "raw opportunity record must be non-empty")
    require(
      line.length <= MaxRecordCharacters,
      s"raw opportunity record exceeds $MaxRecordCharacters characters")
    val node = try {
      mapper.readTree(line)
    } catch {
      case error: Exception =>
        throw new IllegalArgumentException("malformed raw opportunity JSON", error)
    }
    require(node != null && node.isObject, "raw opportunity record must be a JSON object")
    val present = node.fieldNames().asScala.toSet
    require(
      present == fields,
      "raw opportunity fields differ from schema: " +
        s"missing=${(fields -- present).toSeq.sorted.mkString(",")}; " +
        s"extra=${(present -- fields).toSeq.sorted.mkString(",")}")

    val record = ShuffleRecoveryRawOpportunityRecord(
      requiredInt(node, "schemaVersion"),
      requiredText(node, "executionId"),
      requiredLong(node, "exchangeOrdinal"),
      requiredText(node, "exchangePath"),
      requiredText(node, "childOperatorClass"),
      requiredText(node, "partitioningClass"),
      optionalInt(node, "partitionCount"),
      requiredText(node, "ruleSetName"),
      requiredInt(node, "ruleSetVersion"),
      requiredBoolean(node, "eligible"),
      optionalText(node, "immediateMissReason"),
      optionalText(node, "rootMissReason"),
      requiredText(node, "sourceTokenAvailability"),
      requiredText(node, "lineageDeterminism"),
      requiredBoolean(node, "dppPresent"),
      requiredBoolean(node, "runtimeFilterPresent"),
      requiredBoolean(node, "subqueryPresent"),
      requiredBoolean(node, "windowPresent"),
      requiredBoolean(node, "expandPresent"),
      requiredBoolean(node, "cacheScanPresent"),
      requiredBoolean(node, "adaptivePartitionSpecsPresent"),
      requiredBoolean(node, "pythonOrArrowPresent"),
      requiredBoolean(node, "reusedExchange"),
      requiredBoolean(node, "adaptivePlan"),
      requiredBoolean(node, "pipelinedShuffle"),
      requiredBoolean(node, "pushBasedShuffleEnabled"),
      requiredBoolean(node, "mergedShuffleEnabled"),
      requiredTextArray(node, "incompatibleRuntimeFlags"),
      requiredText(node, "disposition"),
      optionalText(node, "accountingReason"),
      optionalInt(node, "stageId"),
      optionalInt(node, "stageAttemptId"),
      optionalInt(node, "shuffleId"),
      optionalInt(node, "mapperCount"),
      optionalLong(node, "shuffleWriteBytes"),
      optionalLong(node, "executorRunTimeMs"),
      optionalLong(node, "completionOrder"))
    validate(record)
    record
  }

  def parseLines(lines: Seq[String]): Seq[ShuffleRecoveryRawOpportunityRecord] = {
    lines.zipWithIndex.map { case (line, index) =>
      try {
        parseLine(line)
      } catch {
        case error: IllegalArgumentException =>
          throw new IllegalArgumentException(
            s"invalid raw opportunity record at line ${index + 1}",
            error)
      }
    }
  }

  private def validate(record: ShuffleRecoveryRawOpportunityRecord): Unit = {
    require(
      record.schemaVersion == SchemaVersion,
      s"unsupported opportunity schema version ${record.schemaVersion}")
    require(record.executionId.matches("query-[0-9]{20}"), "invalid execution id")
    require(record.exchangeOrdinal >= 0L, "exchange ordinal must be non-negative")
    require(record.exchangePath.nonEmpty, "exchange path must be non-empty")
    require(record.childOperatorClass.nonEmpty, "child operator class must be non-empty")
    require(record.partitioningClass.nonEmpty, "partitioning class must be non-empty")
    require(record.ruleSetName.nonEmpty, "rule-set name must be non-empty")
    require(record.ruleSetVersion > 0, "rule-set version must be positive")
    require(
      sourceTokenCodes.contains(record.sourceTokenAvailability),
      s"invalid source-token category ${record.sourceTokenAvailability}")
    require(
      lineageCodes.contains(record.lineageDeterminism),
      s"invalid lineage determinism ${record.lineageDeterminism}")
    require(
      record.incompatibleRuntimeFlags == record.incompatibleRuntimeFlags.distinct.sorted,
      "incompatible runtime flags must be unique and sorted")
    require(
      dispositions.contains(record.disposition),
      s"invalid accounting disposition ${record.disposition}")

    nonNegative(record.partitionCount, "partition count")
    nonNegative(record.stageId, "stage id")
    nonNegative(record.stageAttemptId, "stage attempt id")
    nonNegative(record.shuffleId, "shuffle id")
    nonNegative(record.mapperCount, "mapper count")
    nonNegativeLong(record.shuffleWriteBytes, "shuffle bytes")
    nonNegativeLong(record.executorRunTimeMs, "executor run time")
    record.completionOrder.foreach { value =>
      require(value > 0L, "completion order must be positive")
    }
    record.immediateMissReason.foreach(validateMissReason)
    record.rootMissReason.foreach(validateMissReason)

    if (record.eligible) {
      require(
        record.immediateMissReason.isEmpty && record.rootMissReason.isEmpty,
        "eligible record cannot carry miss reasons")
    } else {
      require(
        record.immediateMissReason.nonEmpty && record.rootMissReason.nonEmpty,
        "ineligible record must carry immediate and root miss reasons")
    }

    record.disposition match {
      case "WEIGHTED" => validateWeighted(record)
      case "UNWEIGHTED" | "EXCLUDED" => validateNonWeighted(record)
      case _ =>
        throw new IllegalStateException("validated disposition became unreachable")
    }
  }

  private def validateWeighted(record: ShuffleRecoveryRawOpportunityRecord): Unit = {
    require(
      record.accountingReason.isEmpty,
      "weighted record cannot carry accounting reason")
    val runtimeFields = Seq(
      record.stageId,
      record.stageAttemptId,
      record.shuffleId,
      record.mapperCount,
      record.shuffleWriteBytes,
      record.executorRunTimeMs,
      record.completionOrder)
    require(
      runtimeFields.forall(_.nonEmpty),
      "weighted record must carry complete runtime correlation")
  }

  private def validateNonWeighted(record: ShuffleRecoveryRawOpportunityRecord): Unit = {
    require(
      record.accountingReason.nonEmpty,
      s"${record.disposition} record must carry a stable accounting reason")
    val accountingReason = record.accountingReason.get
    require(
      ShuffleRecoveryAccountingReason.All.contains(accountingReason),
      s"invalid accounting reason $accountingReason")
    val runtimeFields = Seq(
      record.stageId,
      record.stageAttemptId,
      record.shuffleId,
      record.mapperCount,
      record.shuffleWriteBytes,
      record.executorRunTimeMs,
      record.completionOrder)
    require(
      runtimeFields.forall(_.isEmpty),
      s"${record.disposition} record cannot carry runtime correlation fields")
  }

  private def validateMissReason(reason: String): Unit = {
    require(missReasonCodes.contains(reason), s"invalid miss reason $reason")
  }

  private def nonNegative(value: Option[Int], label: String): Unit = {
    value.foreach(v => require(v >= 0, s"$label must be non-negative"))
  }

  private def nonNegativeLong(value: Option[Long], label: String): Unit = {
    value.foreach(v => require(v >= 0L, s"$label must be non-negative"))
  }

  private def requiredText(node: JsonNode, field: String): String = {
    val value = node.get(field)
    require(
      value != null && value.isTextual && value.textValue().nonEmpty,
      s"$field must be a non-empty string")
    value.textValue()
  }

  private def requiredTextArray(node: JsonNode, field: String): Seq[String] = {
    val value = node.get(field)
    require(value != null && value.isArray, s"$field must be an array")
    value.elements().asScala.map { item =>
      require(
        item.isTextual && item.textValue().nonEmpty,
        s"$field entries must be non-empty strings")
      item.textValue()
    }.toVector
  }

  private def requiredBoolean(node: JsonNode, field: String): Boolean = {
    val value = node.get(field)
    require(value != null && value.isBoolean, s"$field must be boolean")
    value.booleanValue()
  }

  private def requiredInt(node: JsonNode, field: String): Int = {
    val value = node.get(field)
    require(
      value != null && value.isIntegralNumber && value.canConvertToInt,
      s"$field must be a bounded integer")
    value.intValue()
  }

  private def requiredLong(node: JsonNode, field: String): Long = {
    val value = node.get(field)
    require(
      value != null && value.isIntegralNumber && value.canConvertToLong,
      s"$field must be a bounded long")
    value.longValue()
  }

  private def optionalText(node: JsonNode, field: String): Option[String] = {
    val value = node.get(field)
    if (value == null || value.isNull) {
      None
    } else {
      require(
        value.isTextual && value.textValue().nonEmpty,
        s"$field must be null or a non-empty string")
      Some(value.textValue())
    }
  }

  private def optionalInt(node: JsonNode, field: String): Option[Int] = {
    val value = node.get(field)
    if (value == null || value.isNull) {
      None
    } else {
      require(
        value.isIntegralNumber && value.canConvertToInt,
        s"$field must be null or a bounded integer")
      Some(value.intValue())
    }
  }

  private def optionalLong(node: JsonNode, field: String): Option[Long] = {
    val value = node.get(field)
    if (value == null || value.isNull) {
      None
    } else {
      require(
        value.isIntegralNumber && value.canConvertToLong,
        s"$field must be null or a bounded long")
      Some(value.longValue())
    }
  }
}
