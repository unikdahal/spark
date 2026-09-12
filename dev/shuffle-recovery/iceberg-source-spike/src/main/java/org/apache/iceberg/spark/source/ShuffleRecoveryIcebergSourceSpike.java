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
package org.apache.iceberg.spark.source;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionScanTask;
import org.apache.iceberg.Scan;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotScan;
import org.apache.iceberg.Table;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;
import org.apache.spark.sql.execution.exchange.ShuffleRecoveryIcebergIdentityBridge;

/**
 * Out-of-tree feasibility adapter for certifying the exact Apache Iceberg batch scan planned by
 * Spark. This source is deliberately outside Spark's normal source roots and is compiled only by
 * the dedicated conformance command with a pinned Iceberg runtime.
 *
 * <p>The adapter uses Iceberg package-private scan state because the pinned release does not expose
 * a stable connector-facing certification API. That is evidence for the extension-point design,
 * not an API that Spark should depend on.
 */
public final class ShuffleRecoveryIcebergSourceSpike {
  private static final String CATALOG = "recovery_iceberg";
  private static final String TABLE_NAME = CATALOG + ".db.items";
  private static final String PARTITIONED_TABLE_NAME = CATALOG + ".db.partitioned_items";

  private static final int MAX_TASK_GROUPS = 4096;
  private static final int MAX_FILE_TASKS = 16384;
  private static final int MAX_DELETES_PER_TASK = 4096;
  private static final int MAX_STRING_BYTES = 16 * 1024;
  private static final long MAX_HASHED_METADATA_BYTES = 64L * 1024L * 1024L;
  private static final int MAX_IDENTITY_BYTES = 32 * 1024;

  private static final ThreadLocal<List<ScanEvent>> CAPTURED_SCAN_EVENTS = new ThreadLocal<>();

  static {
    // ScanEvent is emitted synchronously by SnapshotScan.planFiles before the file tasks are
    // planned. A thread-local capture binds that event to the ordinary planning call below instead
    // of performing a second table lookup that could observe a newer snapshot.
    Listeners.register(
        event -> {
          List<ScanEvent> events = CAPTURED_SCAN_EVENTS.get();
          if (events != null) {
            events.add(event);
          }
        },
        ScanEvent.class);
  }

  private ShuffleRecoveryIcebergSourceSpike() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("expected one evidence output path");
    }

    Path evidencePath = Path.of(args[0]).toAbsolutePath();
    Path workspace = Files.createTempDirectory("shuffle-recovery-iceberg-source-");
    Path warehouse = workspace.resolve("warehouse");
    Files.createDirectories(warehouse);

    SparkSession spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("shuffle-recovery-iceberg-source-spike")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog." + CATALOG, "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog." + CATALOG + ".type", "hadoop")
            .config("spark.sql.catalog." + CATALOG + ".warehouse", warehouse.toUri().toString())
            .getOrCreate();

    List<String> evidence = new ArrayList<>();
    evidence.add("adapter\ticeberg-package-private-resolved-scan-v1");
    evidence.add("iceberg_runtime\t" + System.getenv().getOrDefault("ICEBERG_VERSION", "unknown"));

    try {
      runEvidence(spark, evidence);
      evidence.add("decision\tRESOLVED_SCAN_CERTIFICATION_FEASIBLE_WITH_PRIVATE_ICEBERG_HOOKS");
      evidence.add("result\tPASS");
    } catch (Exception | AssertionError failure) {
      evidence.add("result\tFAILED");
      evidence.add("failure_class\t" + failure.getClass().getName());
      throw failure;
    } finally {
      try {
        Files.createDirectories(evidencePath.getParent());
        Files.write(evidencePath, evidence, StandardCharsets.UTF_8);
      } finally {
        try {
          spark.stop();
        } finally {
          deleteRecursively(workspace);
        }
      }
    }
  }

  private static void runEvidence(SparkSession spark, List<String> evidence) throws Exception {
    verifyDigestBudget();
    evidence.add("digest_all_bytes_bounded\tPASS");
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + CATALOG + ".db");
    spark.sql(
        "CREATE TABLE "
            + TABLE_NAME
            + " (id BIGINT, payload STRING) USING iceberg "
            + "TBLPROPERTIES ('format-version'='2')");

    spark.range(0, 256)
        .repartition(8)
        .selectExpr("id", "concat('v-', cast(id as string)) AS payload")
        .writeTo(TABLE_NAME)
        .append();

    Table table = Spark3Util.loadIcebergTable(spark, TABLE_NAME);
    long snapshot1 = requireSnapshot(table).snapshotId();

    Dataset<Row> latest1 = latestProjection(spark);
    Certificate latestAtSnapshot1 = requireCertified(certify(latest1));
    require(latestAtSnapshot1.snapshotId == snapshot1, "latest scan did not bind snapshot 1");
    require(checkedRows(latest1, 0L, 256L) == 256L, "snapshot 1 ordinary result changed");

    Dataset<Row> repeatedLatest1 = latestProjection(spark);
    Certificate repeatedLatest1Certificate = requireCertified(certify(repeatedLatest1));
    require(
        repeatedLatest1Certificate.sameIdentity(latestAtSnapshot1),
        "same resolved snapshot did not reproduce the same identity");
    require(checkedRows(repeatedLatest1, 0L, 256L) == 256L, "repeated snapshot 1 result changed");

    Dataset<Row> firstShuffle = shuffledProjection(latestProjection(spark));
    String firstShuffleIdentity = canonicalIdentity(firstShuffle);
    Dataset<Row> repeatedShuffle = shuffledProjection(latestProjection(spark));
    require(firstShuffleIdentity.equals(canonicalIdentity(repeatedShuffle)),
        "independently planned Iceberg shuffles must reproduce canonical identity");
    require(checkedRows(firstShuffle, 0L, 256L) == 256L, "first shuffle result changed");
    require(checkedRows(repeatedShuffle, 0L, 256L) == 256L, "repeated shuffle result changed");
    evidence.add("canonical_shuffle_replanning\tPASS");

    spark.range(256, 512)
        .repartition(8)
        .selectExpr("id", "concat('v-', cast(id as string)) AS payload")
        .writeTo(TABLE_NAME)
        .append();
    table.refresh();
    long snapshot2 = requireSnapshot(table).snapshotId();
    require(snapshot2 != snapshot1, "append did not create a new snapshot");

    Dataset<Row> latest2 = latestProjection(spark);
    Certificate latestAtSnapshot2 = requireCertified(certify(latest2));
    require(latestAtSnapshot2.snapshotId == snapshot2, "latest scan did not advance to snapshot 2");
    require(
        !latestAtSnapshot1.sameIdentity(latestAtSnapshot2),
        "latest scan reused identity after the table advanced");
    require(checkedRows(latest2, 0L, 512L) == 512L, "snapshot 2 ordinary result changed");

    Dataset<Row> pinnedSnapshot1 = pinnedProjection(spark, snapshot1);
    Certificate pinnedSnapshot1Certificate = requireCertified(certify(pinnedSnapshot1));
    require(
        pinnedSnapshot1Certificate.sameIdentity(latestAtSnapshot1),
        "explicit snapshot 1 did not reproduce the original resolved scan identity");
    require(
        checkedRows(pinnedSnapshot1, 0L, 256L) == 256L,
        "pinned snapshot 1 ordinary result changed");

    Dataset<Row> advancedShuffle = shuffledProjection(latestProjection(spark));
    require(!firstShuffleIdentity.equals(canonicalIdentity(advancedShuffle)),
        "advanced Iceberg snapshot must change canonical shuffle identity");
    Dataset<Row> pinnedShuffle = shuffledProjection(pinnedProjection(spark, snapshot1));
    require(firstShuffleIdentity.equals(canonicalIdentity(pinnedShuffle)),
        "pinned Iceberg snapshot must reproduce canonical shuffle identity");
    require(checkedRows(advancedShuffle, 0L, 512L) == 512L, "advanced shuffle result changed");
    require(checkedRows(pinnedShuffle, 0L, 256L) == 256L, "pinned shuffle result changed");
    evidence.add("canonical_shuffle_snapshot_binding\tPASS");

    Dataset<Row> filtered = latestProjection(spark).where("id >= 128");
    Certificate filteredCertificate = requireCertified(certify(filtered));
    require(
        !filteredCertificate.sameIdentity(latestAtSnapshot2),
        "changed pushed filter did not change source identity");
    require(checkedRows(filtered, 128L, 512L) == 384L, "filtered ordinary result changed");

    Dataset<Row> differentlySplit =
        spark.read()
            .format("iceberg")
            .option("split-size", "1")
            .load(TABLE_NAME)
            .select("id", "payload");
    Certificate differentlySplitCertificate = requireCertified(certify(differentlySplit));
    require(
        !differentlySplitCertificate.sameIdentity(latestAtSnapshot2),
        "changed split option did not change source identity");
    require(
        !Arrays.equals(
            differentlySplitCertificate.decompositionDigest,
            latestAtSnapshot2.decompositionDigest),
        "changed split option did not change mapper decomposition");
    require(checkedRows(differentlySplit, 0L, 512L) == 512L, "split option changed ordinary rows");

    spark.sql("ALTER TABLE " + TABLE_NAME + " ADD COLUMN note STRING");
    Dataset<Row> sameProjectionAfterEvolution = latestProjection(spark);
    Certificate sameProjectionAfterEvolutionCertificate =
        requireCertified(certify(sameProjectionAfterEvolution));
    boolean sameCertificateAfterEvolution =
        sameProjectionAfterEvolutionCertificate.sameIdentity(latestAtSnapshot2);
    evidence.add("schema_evolution_certificate\t"
        + (sameCertificateAfterEvolution ? "STABLE" : "CONSERVATIVE_MISS"));
    require(
        checkedRows(sameProjectionAfterEvolution, 0L, 512L) == 512L,
        "schema addition changed old projection");

    Dataset<Row> evolvedProjection = spark.table(TABLE_NAME).select("id", "payload", "note");
    Certificate evolvedProjectionCertificate = requireCertified(certify(evolvedProjection));
    require(
        !evolvedProjectionCertificate.sameIdentity(latestAtSnapshot2),
        "projecting an evolved field did not change source identity");
    require(
        checkedRows(evolvedProjection, 0L, 512L) == 512L,
        "schema evolution changed ordinary row count");

    spark.sql(
        "CREATE TABLE "
            + PARTITIONED_TABLE_NAME
            + " (id BIGINT, bucket INT) USING iceberg PARTITIONED BY (bucket)");
    spark.sql(
        "INSERT INTO " + PARTITIONED_TABLE_NAME + " VALUES (1, 0), (2, 1), (3, 0), (4, 1)");
    Dataset<Row> unsupportedPartitioned =
        spark.table(PARTITIONED_TABLE_NAME).select("id", "bucket");
    CertificationResult unsupportedResult = certify(unsupportedPartitioned);
    require(!unsupportedResult.isCertified(), "partitioned source unexpectedly became eligible");
    require(
        rowCount(unsupportedPartitioned) == 4L,
        "unsupported source changed ordinary execution");

    table.refresh();
    table.expireSnapshots().expireSnapshotId(snapshot1).commit();
    table.refresh();
    spark.catalog().refreshTable(TABLE_NAME);
    Exception expiredFailure = captureFailure(() -> certify(pinnedProjection(spark, snapshot1)));
    require(expiredFailure != null, "expired explicit snapshot unexpectedly certified");
    require(
        containsMessage(expiredFailure, Long.toString(snapshot1)),
        "expired snapshot error lost the requested snapshot id");

    Exception missingTableFailure =
        captureFailure(
            () ->
                certify(
                    spark.read()
                        .format("iceberg")
                        .load(CATALOG + ".db.table_that_does_not_exist")));
    require(missingTableFailure != null, "missing table unexpectedly produced a certificate");

    // Exercise an actual row-level delete: a metadata-only whole-file removal would not test
    // the refusal of delete-bearing tasks. The exact refusal reason enforces that distinction.
    spark.sql("ALTER TABLE " + TABLE_NAME
        + " SET TBLPROPERTIES ('write.delete.mode'='merge-on-read')");
    spark.sql("DELETE FROM " + TABLE_NAME + " WHERE id = 0");
    Dataset<Row> deletedProjection = latestProjection(spark);
    CertificationResult deletedResult = certify(deletedProjection);
    require(!deletedResult.isCertified(), "delete-bearing source unexpectedly certified");
    require("delete-bearing-scan-unreviewed".equals(deletedResult.unsupportedReason),
        "row-level delete did not exercise delete-bearing tasks: "
            + deletedResult.unsupportedReason);
    require(checkedRows(deletedProjection, 1L, 512L) == 511L,
        "refused delete-bearing scan changed ordinary row values");
    evidence.add("delete_bearing_scan_refused_with_exact_values\tPASS");

    evidence.add("table_uuid\t" + latestAtSnapshot1.tableUuid);
    evidence.add("snapshot_1\t" + snapshot1);
    evidence.add("snapshot_2\t" + snapshot2);
    evidence.add("snapshot_1_mappers\t" + latestAtSnapshot1.mapperCount);
    evidence.add("snapshot_2_mappers\t" + latestAtSnapshot2.mapperCount);
    evidence.add("snapshot_2_file_tasks\t" + latestAtSnapshot2.fileTaskCount);
    evidence.add("snapshot_2_delete_files\t" + latestAtSnapshot2.deleteFileCount);
    evidence.add("split_option_changed_decomposition\tPASS");
    evidence.add("same_resolved_scan_reproduced_identity\tPASS");
    evidence.add("latest_advanced\tPASS");
    evidence.add("pinned_reproduced_identity\tPASS");
    evidence.add("pushed_filter_changed_identity\tPASS");
    evidence.add("split_option_changed_identity\tPASS");
    evidence.add("schema_evolution_preserved_values\tPASS");
    evidence.add("projected_schema_evolution_missed\tPASS");
    evidence.add("unsupported_partitioned_scan_preserved_execution\tPASS");
    evidence.add("expired_snapshot_preserved_ordinary_error\tPASS");
    evidence.add("missing_source_preserved_ordinary_error\tPASS");
  }

  private static Dataset<Row> latestProjection(SparkSession spark) {
    return spark.table(TABLE_NAME).select("id", "payload");
  }

  private static Dataset<Row> pinnedProjection(SparkSession spark, long snapshotId) {
    return spark.read()
        .format("iceberg")
        .option("versionAsOf", Long.toString(snapshotId))
        .load(TABLE_NAME)
        .select("id", "payload");
  }

  private static Dataset<Row> shuffledProjection(Dataset<Row> dataset) {
    return dataset.repartition(4, dataset.col("id"));
  }

  private static String canonicalIdentity(Dataset<Row> dataset) {
    Certificate certificate = requireCertified(certify(dataset));
    return ShuffleRecoveryIcebergIdentityBridge.identity(
        dataset.queryExecution().executedPlan(), certificate.plannedScan,
        certificate.identityBytes, certificate.decompositionDigest, certificate.mapperCount,
        "reference-shuffle-provider-v1");
  }

  /**
   * Out-of-tree handoff; certification must run before another caller plans this Dataset.
   * Keep the return opaque here: Spark's package-private Scala type is recovered by the adapter
   * inside the Spark package, without widening the Core API for mixed Java/Scala compilation.
   */
  public static Object canonicalInputs(
      Dataset<Row> dataset, String providerReadFormatId) {
    Certificate certificate = requireCertified(certify(dataset));
    return ShuffleRecoveryIcebergIdentityBridge.inputs(
        dataset.queryExecution().executedPlan(), certificate.plannedScan,
        certificate.identityBytes, certificate.decompositionDigest, certificate.mapperCount,
        providerReadFormatId);
  }

  /**
   * Certifies an ordinary Spark-planned Dataset without independently resolving its table or
   * snapshot. Source-planning exceptions deliberately escape this method unchanged.
   */
  static CertificationResult certify(Dataset<Row> dataset) {
    Objects.requireNonNull(dataset, "dataset");
    List<ScanEvent> events = new ArrayList<>();
    if (CAPTURED_SCAN_EVENTS.get() != null) {
      return CertificationResult.unsupported("nested-planning-capture");
    }

    CAPTURED_SCAN_EVENTS.set(events);
    try {
      SparkPlan plan = dataset.queryExecution().executedPlan();
      BatchScanExec batchScan = findSingleBatchScan(plan);
      if (batchScan == null) {
        return CertificationResult.unsupported("not-one-batch-scan");
      }
      if (!batchScan.runtimeFilters().isEmpty()) {
        return CertificationResult.unsupported("runtime-filtered-scan");
      }
      if (!(batchScan.scan() instanceof SparkBatchQueryScan)) {
        return CertificationResult.unsupported("not-iceberg-batch-query-scan");
      }

      SparkBatchQueryScan scan = (SparkBatchQueryScan) batchScan.scan();
      if (!scan.groupingKeyType().fields().isEmpty()) {
        return CertificationResult.unsupported("grouped-scan");
      }

      // This is the same cached task-group list that SparkBatch.planInputPartitions consumes.
      List<ScanTaskGroup<PartitionScanTask>> taskGroups = scan.taskGroups();
      ScanEvent event = matchingScanEvent(events, scan.table().name());
      if (event == null) {
        return CertificationResult.unsupported("resolved-snapshot-event-unavailable");
      }

      Scan<?, ? extends ScanTask, ? extends ScanTaskGroup<?>> icebergScan = scan.scan();
      if (!(icebergScan instanceof SnapshotScan)) {
        return CertificationResult.unsupported("not-snapshot-scan");
      }

      scala.collection.immutable.Seq<InputPartition> partitions =
          batchScan.inputPartitions();
      if (partitions.size() != taskGroups.size()) {
        return CertificationResult.unsupported("spark-partition-count-disagreement");
      }
      for (int index = 0; index < partitions.size(); index++) {
        if (!(partitions.apply(index) instanceof SparkInputPartition)
            || ((SparkInputPartition) partitions.apply(index)).<PartitionScanTask>taskGroup()
                != taskGroups.get(index)) {
          return CertificationResult.unsupported("spark-partition-task-group-disagreement");
        }
      }
      return buildCertificate(batchScan, scan, icebergScan, event, taskGroups);
    } catch (BoundExceededException e) {
      return CertificationResult.unsupported(e.code);
    } finally {
      CAPTURED_SCAN_EVENTS.remove();
    }
  }

  private static CertificationResult buildCertificate(
      BatchScanExec plannedScan,
      SparkBatchQueryScan sparkScan,
      Scan<?, ? extends ScanTask, ? extends ScanTaskGroup<?>> icebergScan,
      ScanEvent event,
      List<ScanTaskGroup<PartitionScanTask>> taskGroups) {
    try {
      UUID tableUuid = sparkScan.table().uuid();
      if (tableUuid == null) {
        return CertificationResult.unsupported("table-uuid-unavailable");
      }
      if (taskGroups == null || taskGroups.size() > MAX_TASK_GROUPS) {
        return CertificationResult.unsupported("task-group-bound");
      }

      String projectionJson = projectedSchemaJson(event.projection());
      String expectedSchemaJson = projectedSchemaJson(sparkScan.projection());
      if (!projectionJson.equals(expectedSchemaJson)) {
        return CertificationResult.unsupported("planning-projection-disagreement");
      }
      String filterJson = boundedExpressionJson(event.filter());
      if (!boundedString(projectionJson) || !boundedString(filterJson)) {
        return CertificationResult.unsupported("scan-fact-bound");
      }

      Decomposition decomposition = hashTaskGroups(taskGroups);
      if (decomposition.unsupportedReason != null) {
        return CertificationResult.unsupported(decomposition.unsupportedReason);
      }

      byte[] identityBytes =
          identityBytes(
              tableUuid,
              event.snapshotId(),
              projectionJson,
              filterJson,
              sparkScan.caseSensitive(),
              icebergScan.targetSplitSize(),
              icebergScan.splitLookback(),
              icebergScan.splitOpenFileCost(),
              taskGroups.size(),
              decomposition.fileTaskCount,
              decomposition.deleteFileCount,
              decomposition.digest);
      if (identityBytes.length > MAX_IDENTITY_BYTES) {
        return CertificationResult.unsupported("identity-bound");
      }

      return CertificationResult.certified(
          new Certificate(
              plannedScan,
              tableUuid,
              event.snapshotId(),
              identityBytes,
              decomposition.digest,
              taskGroups.size(),
              decomposition.fileTaskCount,
              decomposition.deleteFileCount));
    } catch (UnsupportedOperationException e) {
      return CertificationResult.unsupported("required-iceberg-fact-unavailable");
    } catch (BoundExceededException e) {
      return CertificationResult.unsupported(e.code);
    }
  }

  private static String projectedSchemaJson(Schema schema) {
    if (schema == null) {
      throw new BoundExceededException("null-projected-schema");
    }
    if (schema.columns().size() > 128) {
      throw new BoundExceededException("projected-field-count");
    }
    int characters = 0;
    for (Types.NestedField field : schema.columns()) {
      if (!boundedString(field.name()) || !field.type().isPrimitiveType()
          || field.initialDefaultLiteral() != null || field.writeDefaultLiteral() != null
          || (field.doc() != null && !boundedString(field.doc()))) {
        throw new BoundExceededException("unreviewed-projected-field");
      }
      characters += field.name().length() + (field.doc() == null ? 0 : field.doc().length());
      if (characters > MAX_STRING_BYTES / 8) {
        throw new BoundExceededException("projected-schema-character-budget");
      }
    }
    // SparkScanBuilder constructs projected schemas this way as well. Reconstructing from fields
    // removes an unrelated table schema-id while retaining field ids, names, nullability and types.
    return SchemaParser.toJson(new Schema(schema.columns()));
  }

  private static String boundedExpressionJson(Expression expression) {
    ArrayDeque<Expression> pending = new ArrayDeque<>();
    pending.push(Objects.requireNonNull(expression));
    int nodes = 0;
    int characters = 0;
    while (!pending.isEmpty()) {
      Expression current = pending.pop();
      if (++nodes > 128) {
        throw new BoundExceededException("filter-node-count");
      }
      if (current instanceof And) {
        pending.push(((And) current).left());
        pending.push(((And) current).right());
      } else if (current instanceof Or) {
        pending.push(((Or) current).left());
        pending.push(((Or) current).right());
      } else if (current instanceof Not) {
        pending.push(((Not) current).child());
      } else if (current.op() != Expression.Operation.TRUE
          && current.op() != Expression.Operation.FALSE) {
        if (!(current instanceof UnboundPredicate)) {
          throw new BoundExceededException("unreviewed-filter-predicate");
        }
        UnboundPredicate<?> predicate = (UnboundPredicate<?>) current;
        if (!(predicate.term() instanceof NamedReference)
            || !boundedString(predicate.ref().name())) {
          throw new BoundExceededException("unreviewed-filter-term");
        }
        characters += predicate.ref().name().length();
        if (predicate.literals() != null) {
          if (predicate.literals().size() > 128) {
            throw new BoundExceededException("filter-literal-count");
          }
          for (org.apache.iceberg.expressions.Literal<?> literal : predicate.literals()) {
            Object value = literal.value();
            if (!(value instanceof Long || value instanceof Integer || value instanceof Boolean
                || value instanceof String && boundedString((String) value))) {
              throw new BoundExceededException("unreviewed-filter-literal");
            }
            characters += value instanceof String ? ((String) value).length() : 24;
            if (characters > MAX_STRING_BYTES / 8) {
              throw new BoundExceededException("filter-character-budget");
            }
          }
        }
      }
      if (characters > MAX_STRING_BYTES / 8) {
        throw new BoundExceededException("filter-character-budget");
      }
    }
    String json = ExpressionParser.toJson(expression);
    if (!boundedString(json)) {
      throw new BoundExceededException("filter-encoded-size");
    }
    return json;
  }

  private static Decomposition hashTaskGroups(
      List<ScanTaskGroup<PartitionScanTask>> taskGroups) {
    MessageDigest digest = sha256();
    BoundedDigestOutputStream bounded = new BoundedDigestOutputStream(digest);
    try (DataOutputStream out = new DataOutputStream(bounded)) {
      writeString(out, bounded, "iceberg.mapper-decomposition.v1");
      out.writeInt(taskGroups.size());
      int fileTaskCount = 0;
      int deleteFileCount = 0;

      for (int groupIndex = 0; groupIndex < taskGroups.size(); groupIndex++) {
        ScanTaskGroup<PartitionScanTask> group = taskGroups.get(groupIndex);
        if (group == null) {
          return Decomposition.unsupported("null-task-group");
        }
        Collection<PartitionScanTask> tasks = group.tasks();
        if (tasks == null) {
          return Decomposition.unsupported("null-task-collection");
        }
        out.writeInt(groupIndex);
        out.writeInt(tasks.size());

        int taskIndex = 0;
        for (PartitionScanTask task : tasks) {
          if (!(task instanceof FileScanTask)) {
            return Decomposition.unsupported("non-file-scan-task");
          }
          FileScanTask fileTask = (FileScanTask) task;
          if (!fileTask.spec().isUnpartitioned()) {
            return Decomposition.unsupported("partitioned-scan-task");
          }
          if (++fileTaskCount > MAX_FILE_TASKS) {
            return Decomposition.unsupported("file-task-bound");
          }
          List<DeleteFile> deletes = fileTask.deletes();
          if (deletes == null || deletes.size() > MAX_DELETES_PER_TASK) {
            return Decomposition.unsupported("delete-file-bound");
          }
          if ((long) deleteFileCount + deletes.size() > Integer.MAX_VALUE) {
            return Decomposition.unsupported("delete-file-count-overflow");
          }
          if (!deletes.isEmpty()) {
            return Decomposition.unsupported("delete-bearing-scan-unreviewed");
          }
          deleteFileCount += deletes.size();

          out.writeInt(taskIndex++);
          writeContentFile(out, bounded, fileTask.file());
          out.writeLong(fileTask.start());
          out.writeLong(fileTask.length());
          writeString(out, bounded, boundedExpressionJson(fileTask.residual()));
          out.writeInt(deletes.size());
          for (DeleteFile delete : deletes) {
            if (delete == null) {
              return Decomposition.unsupported("null-delete-file");
            }
            writeContentFile(out, bounded, delete);
            writeNullableString(out, bounded, delete.referencedDataFile());
            writeNullableLong(out, delete.contentOffset());
            writeNullableLong(out, delete.contentSizeInBytes());
          }
        }
      }
      out.flush();
      return Decomposition.certified(digest.digest(), fileTaskCount, deleteFileCount);
    } catch (IOException e) {
      throw new IllegalStateException("unexpected in-memory certificate I/O failure", e);
    } catch (BoundExceededException e) {
      return Decomposition.unsupported(e.code);
    }
  }

  private static void writeContentFile(
      DataOutputStream out, BoundedDigestOutputStream bounded, ContentFile<?> file)
      throws IOException {
    if (file == null) {
      throw new BoundExceededException("null-content-file");
    }
    writeString(out, bounded, file.content().name());
    writeString(out, bounded, file.location());
    writeString(out, bounded, file.format().name());
    out.writeInt(file.specId());
    out.writeLong(file.recordCount());
    out.writeLong(file.fileSizeInBytes());
    writeNullableLong(out, file.dataSequenceNumber());
    writeNullableLong(out, file.fileSequenceNumber());
    writeNullableLong(out, file.firstRowId());
    writeNullableInt(out, file.sortOrderId());
    List<Integer> equalityIds = file.equalityFieldIds();
    if (equalityIds == null) {
      out.writeInt(-1);
    } else {
      out.writeInt(equalityIds.size());
      for (Integer fieldId : equalityIds) {
        if (fieldId == null) {
          throw new BoundExceededException("null-equality-field-id");
        }
        out.writeInt(fieldId);
      }
    }
  }

  private static byte[] identityBytes(
      UUID tableUuid,
      long snapshotId,
      String projectionJson,
      String filterJson,
      boolean caseSensitive,
      long targetSplitSize,
      int splitLookback,
      long splitOpenFileCost,
      int mapperCount,
      int fileTaskCount,
      int deleteFileCount,
      byte[] decompositionDigest) {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(buffer)) {
      writeBoundedIdentityString(out, "iceberg.resolved-scan.v1");
      writeBoundedIdentityString(out, tableUuid.toString());
      out.writeLong(snapshotId);
      writeBoundedIdentityString(out, projectionJson);
      writeBoundedIdentityString(out, filterJson);
      out.writeBoolean(caseSensitive);
      out.writeLong(targetSplitSize);
      out.writeInt(splitLookback);
      out.writeLong(splitOpenFileCost);
      out.writeInt(mapperCount);
      out.writeInt(fileTaskCount);
      out.writeInt(deleteFileCount);
      out.writeInt(decompositionDigest.length);
      out.write(decompositionDigest);
      out.flush();
      return buffer.toByteArray();
    } catch (IOException e) {
      throw new IllegalStateException("unexpected in-memory identity I/O failure", e);
    }
  }

  private static void writeBoundedIdentityString(DataOutputStream out, String value)
      throws IOException {
    if (value == null) {
      throw new BoundExceededException("null-identity-string");
    }
    if (value.length() > MAX_STRING_BYTES) {
      throw new BoundExceededException("metadata-string-bound");
    }
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    if (bytes.length > MAX_STRING_BYTES) {
      throw new BoundExceededException("identity-string-bound");
    }
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  private static void writeString(
      DataOutputStream out, BoundedDigestOutputStream bounded, String value) throws IOException {
    if (value == null) {
      throw new BoundExceededException("null-metadata-string");
    }
    if (value.length() > MAX_STRING_BYTES) {
      throw new BoundExceededException("metadata-string-bound");
    }
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    if (bytes.length > MAX_STRING_BYTES) {
      throw new BoundExceededException("metadata-string-bound");
    }
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  private static void writeNullableString(
      DataOutputStream out, BoundedDigestOutputStream bounded, String value) throws IOException {
    out.writeBoolean(value != null);
    if (value != null) {
      writeString(out, bounded, value);
    }
  }

  private static void writeNullableLong(DataOutputStream out, Long value) throws IOException {
    out.writeBoolean(value != null);
    if (value != null) {
      out.writeLong(value);
    }
  }

  private static void writeNullableInt(DataOutputStream out, Integer value) throws IOException {
    out.writeBoolean(value != null);
    if (value != null) {
      out.writeInt(value);
    }
  }

  private static boolean boundedString(String value) {
    return value != null && value.length() <= MAX_STRING_BYTES
        && value.getBytes(StandardCharsets.UTF_8).length <= MAX_STRING_BYTES;
  }

  private static ScanEvent matchingScanEvent(List<ScanEvent> events, String tableName) {
    ScanEvent selected = null;
    for (ScanEvent event : events) {
      if (event != null && Objects.equals(tableName, event.tableName())) {
        if (selected == null) {
          selected = event;
        } else if (selected.snapshotId() != event.snapshotId()
            || !projectedSchemaJson(selected.projection()).equals(
                projectedSchemaJson(event.projection()))
            || !boundedExpressionJson(selected.filter()).equals(
                boundedExpressionJson(event.filter()))) {
          return null;
        }
      }
    }
    return selected;
  }

  private static BatchScanExec findSingleBatchScan(SparkPlan root) {
    List<BatchScanExec> scans = new ArrayList<>();
    collectBatchScans(root, scans);
    return scans.size() == 1 ? scans.get(0) : null;
  }

  private static void collectBatchScans(SparkPlan plan, List<BatchScanExec> scans) {
    if (plan instanceof BatchScanExec) {
      scans.add((BatchScanExec) plan);
    }
    scala.collection.Iterator<SparkPlan> children = plan.children().iterator();
    while (children.hasNext()) {
      collectBatchScans(children.next(), scans);
    }
  }

  private static long checkedRows(Dataset<Row> dataset, long first, long end) {
    List<Row> rows = dataset.collectAsList();
    Set<Long> seen = new HashSet<>();
    for (Row row : rows) {
      require(!row.isNullAt(0), "unexpected null id");
      long id = row.getLong(0);
      require(id >= first && id < end && seen.add(id), "unexpected or duplicate id: " + id);
      require(("v-" + id).equals(row.getString(1)), "wrong payload for id " + id);
      if (row.size() == 3) {
        require(row.isNullAt(2), "evolved field must be null for existing rows");
      }
    }
    require(seen.size() == end - first, "missing expected rows");
    return rows.size();
  }

  private static long rowCount(Dataset<Row> dataset) {
    // collectAsList executes this Dataset's already-built QueryExecution. Using Dataset.count()
    // would construct a separate aggregate Dataset and would not prove that the certified scan was
    // the one subsequently executed.
    return dataset.collectAsList().size();
  }

  private static Snapshot requireSnapshot(Table table) {
    Snapshot snapshot = table.currentSnapshot();
    require(snapshot != null, "expected current Iceberg snapshot");
    return snapshot;
  }

  private static Certificate requireCertified(CertificationResult result) {
    require(result.isCertified(), "expected certified scan, got " + result.unsupportedReason);
    return result.certificate;
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }

  private static Exception captureFailure(ThrowingRunnable action) {
    try {
      action.run();
      return null;
    } catch (Exception e) {
      return e;
    }
  }

  private static boolean containsMessage(Exception exception, String expected) {
    Throwable current = exception;
    while (current != null) {
      if (current.getMessage() != null && current.getMessage().contains(expected)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  private static void verifyDigestBudget() throws IOException {
    BoundedDigestOutputStream output = new BoundedDigestOutputStream(sha256(), 12L);
    DataOutputStream data = new DataOutputStream(output);
    data.writeLong(7L);
    data.write(new byte[4]);
    require(output.metadataBytes == 12L, "scalar and array bytes must share the budget");
    try {
      data.writeByte(0);
      throw new AssertionError("digest exceeded its byte limit");
    } catch (BoundExceededException expected) {
      require("metadata-total-bound".equals(expected.code), "wrong budget refusal");
    }
  }

  private static void deleteRecursively(Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
      paths.sorted((left, right) -> right.compareTo(left))
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (IOException e) {
                  throw new DeleteFailure(e);
                }
              });
    } catch (DeleteFailure e) {
      throw e.cause;
    }
  }

  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  static final class CertificationResult {
    final Certificate certificate;
    final String unsupportedReason;

    private CertificationResult(Certificate certificate, String unsupportedReason) {
      this.certificate = certificate;
      this.unsupportedReason = unsupportedReason;
    }

    static CertificationResult certified(Certificate certificate) {
      return new CertificationResult(Objects.requireNonNull(certificate), null);
    }

    static CertificationResult unsupported(String reason) {
      return new CertificationResult(null, Objects.requireNonNull(reason));
    }

    boolean isCertified() {
      return certificate != null;
    }
  }

  static final class Certificate {
    final BatchScanExec plannedScan;
    final UUID tableUuid;
    final long snapshotId;
    final byte[] identityBytes;
    final byte[] decompositionDigest;
    final int mapperCount;
    final int fileTaskCount;
    final int deleteFileCount;

    Certificate(
        BatchScanExec plannedScan,
        UUID tableUuid,
        long snapshotId,
        byte[] identityBytes,
        byte[] decompositionDigest,
        int mapperCount,
        int fileTaskCount,
        int deleteFileCount) {
      this.plannedScan = plannedScan;
      this.tableUuid = tableUuid;
      this.snapshotId = snapshotId;
      this.identityBytes = identityBytes.clone();
      this.decompositionDigest = decompositionDigest.clone();
      this.mapperCount = mapperCount;
      this.fileTaskCount = fileTaskCount;
      this.deleteFileCount = deleteFileCount;
    }

    boolean sameIdentity(Certificate other) {
      return other != null && Arrays.equals(identityBytes, other.identityBytes);
    }
  }

  private static final class Decomposition {
    final byte[] digest;
    final int fileTaskCount;
    final int deleteFileCount;
    final String unsupportedReason;

    private Decomposition(
        byte[] digest, int fileTaskCount, int deleteFileCount, String unsupportedReason) {
      this.digest = digest;
      this.fileTaskCount = fileTaskCount;
      this.deleteFileCount = deleteFileCount;
      this.unsupportedReason = unsupportedReason;
    }

    static Decomposition certified(byte[] digest, int fileTaskCount, int deleteFileCount) {
      return new Decomposition(digest.clone(), fileTaskCount, deleteFileCount, null);
    }

    static Decomposition unsupported(String reason) {
      return new Decomposition(null, 0, 0, reason);
    }
  }

  private static final class BoundedDigestOutputStream extends DigestOutputStream {
    private long metadataBytes;
    private final long limit;

    BoundedDigestOutputStream(MessageDigest digest) {
      this(digest, MAX_HASHED_METADATA_BYTES);
    }

    BoundedDigestOutputStream(MessageDigest digest, long limit) {
      super(
          new OutputStream() {
            @Override
            public void write(int value) {}

            @Override
            public void write(byte[] bytes, int offset, int length) {}
          },
          digest);
      this.limit = limit;
    }

    @Override
    public void write(int value) throws IOException {
      account(1);
      super.write(value);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
      Objects.checkFromIndexSize(offset, length, bytes.length);
      account(length);
      super.write(bytes, offset, length);
    }

    private void account(int bytes) {
      if (bytes < 0 || metadataBytes > limit - bytes) {
        throw new BoundExceededException("metadata-total-bound");
      }
      metadataBytes += bytes;
    }
  }

  private static final class BoundExceededException extends RuntimeException {
    final String code;

    BoundExceededException(String code) {
      super(code);
      this.code = code;
    }
  }

  private static final class DeleteFailure extends RuntimeException {
    final IOException cause;

    DeleteFailure(IOException cause) {
      super(cause);
      this.cause = cause;
    }
  }
}
