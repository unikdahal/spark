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

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{Expression, Expressions, NullOrdering,
  SortDirection}

class RowLevelWriteManifestSuite extends SparkFunSuite {
  private val digest = MessageDigest.getInstance("SHA-256").digest("operation".getBytes("UTF-8"))
  private val baseline = RowLevelWriteManifestInput(
    recoveryExecutionId = "execution-1",
    recoveryId = "write-1",
    generation = "generation-1",
    sinkId = "catalog.ns.table",
    catalogIdentity = "catalog",
    tableName = "catalog.ns.table",
    command = "MERGE",
    physicalMode = "REPLACE_DATA",
    canonicalOperationSha256 = digest,
    conditionSha256 = Array.fill[Byte](32)(2),
    conflictFilterSha256 = Some(Array.fill[Byte](32)(3)),
    inputSchemaJson = Some("{\"type\":\"struct\",\"fields\":[]}"),
    outputSchemaJson = Some("{\"type\":\"struct\",\"fields\":[{\"name\":\"id\"}]}"),
    rowSchemaJson = Some("{\"row\":1}"),
    rowIdSchemaJson = Some("{\"row-id\":1}"),
    metadataSchemaJson = Some("{\"metadata\":1}"),
    distributionDescription = "clustered(id)",
    distributionStrictlyRequired = true,
    requiredNumPartitions = 8,
    advisoryPartitionSizeInBytes = 67108864L,
    orderingDescriptions = Seq("id ASC NULLS FIRST", "version DESC NULLS LAST"),
    sourceAnchors = Seq(
      RowLevelWriteSourceAnchor("source-b", "snapshot-9"),
      RowLevelWriteSourceAnchor("source-a", "snapshot-4")),
    transactionId = Some("transaction-1"))

  test("row-level write manifest is deterministic and canonicalizes source order") {
    val encoded = RowLevelWriteManifest.encode(baseline)
    assert(RowLevelWriteManifest.encode(baseline).toSeq === encoded.toSeq)
    assert(RowLevelWriteManifest.encode(baseline.copy(
      sourceAnchors = baseline.sourceAnchors.reverse)).toSeq === encoded.toSeq)
    val core = encoded.dropRight(32)
    assert(encoded.takeRight(32).toSeq ===
      MessageDigest.getInstance("SHA-256").digest(core).toSeq)
  }

  test("Spark-owned generation is deterministic and fences provider write-ID reuse") {
    val generation = RowLevelWriteManifest.generation(
      baseline.recoveryExecutionId, baseline.recoveryId, baseline.sinkId, digest)
    assert(RowLevelWriteManifest.generation(
      baseline.recoveryExecutionId, baseline.recoveryId, baseline.sinkId, digest) === generation)
    assert(RowLevelWriteManifest.generation(
      baseline.recoveryExecutionId, baseline.recoveryId, baseline.sinkId,
      Array.fill[Byte](32)(7)) !== generation)
    assert(RowLevelWriteManifest.generation(
      baseline.recoveryExecutionId, "aliased-write", baseline.sinkId, digest) !== generation)
  }

  test("row-level write manifest binds every resolved generation field") {
    val encoded = RowLevelWriteManifest.encode(baseline).toSeq
    val changes = Seq(
      baseline.copy(recoveryExecutionId = "execution-2"),
      baseline.copy(recoveryId = "write-2"),
      baseline.copy(generation = "generation-2"),
      baseline.copy(sinkId = "other-sink"),
      baseline.copy(catalogIdentity = "other-catalog"),
      baseline.copy(tableName = "other-table"),
      baseline.copy(command = "UPDATE"),
      baseline.copy(physicalMode = "WRITE_DELTA"),
      baseline.copy(canonicalOperationSha256 = Array.fill[Byte](32)(7)),
      baseline.copy(conditionSha256 = Array.fill[Byte](32)(7)),
      baseline.copy(conflictFilterSha256 = None),
      baseline.copy(inputSchemaJson = None),
      baseline.copy(outputSchemaJson = None),
      baseline.copy(rowSchemaJson = None),
      baseline.copy(rowIdSchemaJson = None),
      baseline.copy(metadataSchemaJson = None),
      baseline.copy(distributionDescription = "unspecified"),
      baseline.copy(distributionStrictlyRequired = false),
      baseline.copy(requiredNumPartitions = 9),
      baseline.copy(advisoryPartitionSizeInBytes = 1L),
      baseline.copy(orderingDescriptions = baseline.orderingDescriptions.reverse),
      baseline.copy(sourceAnchors = baseline.sourceAnchors.updated(
        0, RowLevelWriteSourceAnchor("source-b", "snapshot-10"))),
      baseline.copy(transactionId = None))
    changes.foreach(changed => assert(RowLevelWriteManifest.encode(changed).toSeq !== encoded))
  }

  test("row-level write manifest preserves explicit schema presence") {
    val absent = RowLevelWriteManifest.encode(baseline.copy(metadataSchemaJson = None))
    val present = RowLevelWriteManifest.encode(baseline.copy(metadataSchemaJson = Some("{}")))
    assert(absent.toSeq !== present.toSeq)
  }

  test("row-level write manifest rejects invalid resolved inputs") {
    Seq(
      baseline.copy(recoveryExecutionId = ""),
      baseline.copy(command = "INSERT"),
      baseline.copy(physicalMode = "UNKNOWN"),
      baseline.copy(canonicalOperationSha256 = Array.emptyByteArray),
      baseline.copy(conditionSha256 = Array.emptyByteArray),
      baseline.copy(conflictFilterSha256 = Some(Array.emptyByteArray)),
      baseline.copy(requiredNumPartitions = -1),
      baseline.copy(advisoryPartitionSizeInBytes = -1L),
      baseline.copy(orderingDescriptions = null),
      baseline.copy(sourceAnchors = Seq(
        RowLevelWriteSourceAnchor("source-a", "snapshot-1"),
        RowLevelWriteSourceAnchor("source-a", "snapshot-2"))),
      baseline.copy(transactionId = Some(""))).foreach { invalid =>
      intercept[IllegalArgumentException](RowLevelWriteManifest.encode(invalid))
    }
  }

  test("row-level write manifest rejects malformed UTF-16 and oversized UTF-8") {
    intercept[java.nio.charset.CharacterCodingException] {
      RowLevelWriteManifest.encode(baseline.copy(tableName = "\ud800"))
    }
    intercept[IllegalArgumentException] {
      RowLevelWriteManifest.encode(baseline.copy(
        tableName = new String(new Array[Byte](64 * 1024 + 1), StandardCharsets.US_ASCII)))
    }
  }

  test("distribution and ordering requirements use structural fail-closed encodings") {
    val id = Expressions.column("nested.id")
    val clustered = Distributions.clustered(
      Array[Expression](id, Expressions.bucket(8, "bucket_col")))
    val clusteredEncoding = RowLevelWriteRequirementEncoding.distribution(clustered)
    assert(RowLevelWriteRequirementEncoding.distribution(clustered) === clusteredEncoding)
    assert(RowLevelWriteRequirementEncoding.distribution(
      Distributions.clustered(Array[Expression](Expressions.column("nested.other")))) !==
        clusteredEncoding)

    val ascending = Expressions.sort(id, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    val descending = Expressions.sort(id, SortDirection.DESCENDING, NullOrdering.NULLS_LAST)
    assert(RowLevelWriteRequirementEncoding.ordering(ascending) !==
      RowLevelWriteRequirementEncoding.ordering(descending))

    intercept[IllegalArgumentException] {
      RowLevelWriteRequirementEncoding.distribution(new Distribution {})
    }
  }
}
