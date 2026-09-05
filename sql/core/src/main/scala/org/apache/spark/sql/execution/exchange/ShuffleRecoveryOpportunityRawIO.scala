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

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

/**
 * Strict persisted shape for weighted opportunity evidence.
 *
 * Durable evidence is treated as untrusted input even though the Phase 0-A tooling only reads its
 * own artifacts. The parser rejects unknown schema versions, missing fields, invalid dispositions,
 * negative weights, inconsistent weighted records, and extra fields rather than silently changing
 * denominator semantics when an artifact is malformed or produced by incompatible tooling.
 */
private[sql] case class ShuffleRecoveryRawOpportunityRecord(
    schemaVersion: Int,
    executionId: String,
    exchangeOrdinal: Long,
    exchangePath: String,
    ruleSetName: String,
    ruleSetVersion: Int,
    eligible: Boolean,
    immediateMissReason: Option[String],
    rootMissReason: Option[String],
    sourceTokenAvailability: String,
    disposition: String,
    accountingReason: Option[String],
    stageId: Option[Int],
    stageAttemptId: Option[Int],
    shuffleId: Option[Int],
    mapperCount: Option[Int],
    shuffleWriteBytes: Option[Long],
    executorRunTimeMs: Option[Long],
    completionOrder: Option[Long])

private[sql] object ShuffleRecoveryOpportunityRawIO {
  val SchemaVersion = 1

  private val mapper = new ObjectMapper()
  private val fields = Set(
    "schemaVersion",
    "executionId",
    "exchangeOrdinal",
    "exchangePath",
    "ruleSetName",
    "ruleSetVersion",
    "eligible",
    "immediateMissReason",
    "rootMissReason",
    "sourceTokenAvailability",
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

  def parseLine(line: String): ShuffleRecoveryRawOpportunityRecord = {
    require(line != null && line.nonEmpty, "raw opportunity record must be non-empty")
    val node = try mapper.readTree(line) catch {
      case error: Exception =>
        throw new IllegalArgumentException("malformed raw opportunity JSON", error)
    }
    require(node != null && node.isObject, "raw opportunity record must be a JSON object")
    val present = node.fieldNames().asScala.toSet
    require(present == fields,
      s"raw opportunity fields differ from schema: missing=${(fields -- present).toSeq.sorted.mkString(",")};" +
        s" extra=${(present -- fields).toSeq.sorted.mkString(",")}")

    val record = ShuffleRecoveryRawOpportunityRecord(
      schemaVersion = requiredInt(node, "schemaVersion"),
      executionId = requiredText(node, "executionId"),
      exchangeOrdinal = requiredLong(node, "exchangeOrdinal"),
      exchangePath = requiredText(node, "exchangePath"),
      ruleSetName = requiredText(node, "ruleSetName"),
      ruleSetVersion = requiredInt(node, "ruleSetVersion"),
      eligible = requiredBoolean(node, "eligible"),
      immediateMissReason = optionalText(node, "immediateMissReason"),
      rootMissReason = optionalText(node, "rootMissReason"),
      sourceTokenAvailability = requiredText(node, "sourceTokenAvailability"),
      disposition = requiredText(node, "disposition"),
      accountingReason = optionalText(node, "accountingReason"),
      stageId = optionalInt(node, "stageId"),
      stageAttemptId = optionalInt(node, "stageAttemptId"),
      shuffleId = optionalInt(node, "shuffleId"),
      mapperCount = optionalInt(node, "mapperCount"),
      shuffleWriteBytes = optionalLong(node, "shuffleWriteBytes"),
      executorRunTimeMs = optionalLong(node, "executorRunTimeMs"),
      completionOrder = optionalLong(node, "completionOrder"))

    validate(record)
    record
  }

  def parseLines(lines: Seq[String]): Seq[ShuffleRecoveryRawOpportunityRecord] = {
    lines.zipWithIndex.map { case (line, index) =>
      try parseLine(line) catch {
        case error: IllegalArgumentException =>
          throw new IllegalArgumentException(s"invalid raw opportunity record at line ${index + 1}", error)
      }
    }
  }

  private def validate(record: ShuffleRecoveryRawOpportunityRecord): Unit = {
    require(record.schemaVersion == SchemaVersion,
      s"unsupported opportunity schema version ${record.schemaVersion}")
    require(record.executionId.matches("query-[0-9]{20}"), "invalid execution id")
    require(record.exchangeOrdinal >= 0L, "exchange ordinal must be non-negative")
    require(record.exchangePath.nonEmpty, "exchange path must be non-empty")
    require(record.ruleSetName.nonEmpty, "rule-set name must be non-empty")
    require(record.ruleSetVersion > 0, "rule-set version must be positive")
    require(record.sourceTokenAvailability.nonEmpty, "source-token category must be non-empty")
    require(dispositions.contains(record.disposition),
      s"invalid accounting disposition ${record.disposition}")
    record.stageId.foreach(value => require(value >= 0, "stage id must be non-negative"))
    record.stageAttemptId.foreach(value => require(value >= 0, "stage attempt id must be non-negative"))
    record.shuffleId.foreach(value => require(value >= 0, "shuffle id must be non-negative"))
    record.mapperCount.foreach(value => require(value >= 0, "mapper count must be non-negative"))
    record.shuffleWriteBytes.foreach(value => require(value >= 0L, "shuffle bytes must be non-negative"))
    record.executorRunTimeMs.foreach(value => require(value >= 0L, "executor run time must be non-negative"))
    record.completionOrder.foreach(value => require(value > 0L, "completion order must be positive"))

    if (record.eligible) {
      require(record.immediateMissReason.isEmpty && record.rootMissReason.isEmpty,
        "eligible record cannot carry miss reasons")
    } else {
      require(record.immediateMissReason.nonEmpty && record.rootMissReason.nonEmpty,
        "ineligible record must carry immediate and root miss reasons")
    }

    record.disposition match {
      case "WEIGHTED" =>
        require(record.accountingReason.isEmpty, "weighted record cannot carry accounting reason")
        require(Seq(
          record.stageId,
          record.stageAttemptId,
          record.shuffleId,
          record.mapperCount,
          record.shuffleWriteBytes,
          record.executorRunTimeMs,
          record.completionOrder).forall(_.nonEmpty),
          "weighted record must carry complete runtime correlation")
      case "UNWEIGHTED" | "EXCLUDED" =>
        require(record.accountingReason.nonEmpty,
          s"${record.disposition} record must carry a stable accounting reason")
      case _ => throw new IllegalStateException("validated disposition became unreachable")
    }
  }

  private def requiredText(node: JsonNode, field: String): String = {
    val value = node.get(field)
    require(value != null && value.isTextual && value.textValue().nonEmpty,
      s"$field must be a non-empty string")
    value.textValue()
  }

  private def requiredBoolean(node: JsonNode, field: String): Boolean = {
    val value = node.get(field)
    require(value != null && value.isBoolean, s"$field must be boolean")
    value.booleanValue()
  }

  private def requiredInt(node: JsonNode, field: String): Int = {
    val value = node.get(field)
    require(value != null && value.isIntegralNumber && value.canConvertToInt,
      s"$field must be a bounded integer")
    value.intValue()
  }

  private def requiredLong(node: JsonNode, field: String): Long = {
    val value = node.get(field)
    require(value != null && value.isIntegralNumber && value.canConvertToLong,
      s"$field must be a bounded long")
    value.longValue()
  }

  private def optionalText(node: JsonNode, field: String): Option[String] = {
    val value = node.get(field)
    if (value == null || value.isNull) None
    else {
      require(value.isTextual && value.textValue().nonEmpty,
        s"$field must be null or a non-empty string")
      Some(value.textValue())
    }
  }

  private def optionalInt(node: JsonNode, field: String): Option[Int] = {
    val value = node.get(field)
    if (value == null || value.isNull) None
    else {
      require(value.isIntegralNumber && value.canConvertToInt,
        s"$field must be null or a bounded integer")
      Some(value.intValue())
    }
  }

  private def optionalLong(node: JsonNode, field: String): Option[Long] = {
    val value = node.get(field)
    if (value == null || value.isNull) None
    else {
      require(value.isIntegralNumber && value.canConvertToLong,
        s"$field must be null or a bounded long")
      Some(value.longValue())
    }
  }
}
