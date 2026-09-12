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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Cheap linkage smoke test for the pinned out-of-tree Iceberg runtime.
 *
 * <p>This gate deliberately initializes the real catalog, plans a batch scan and reads one row
 * before the larger source-certification experiment runs. It exists to turn connector binary/API
 * mismatches into a small, attributable failure rather than allowing a long conformance run to fail
 * during unrelated setup.
 */
public final class ShuffleRecoveryIcebergCompatibilitySmoke {
  private static final String CATALOG = "recovery_iceberg_smoke";
  private static final String TABLE_NAME = CATALOG + ".db.items";

  private ShuffleRecoveryIcebergCompatibilitySmoke() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("expected one smoke evidence output path");
    }

    Path evidencePath = Path.of(args[0]).toAbsolutePath();
    Path workspace = Files.createTempDirectory("shuffle-recovery-iceberg-smoke-");
    Path warehouse = workspace.resolve("warehouse");
    Files.createDirectories(warehouse);
    List<String> evidence = new ArrayList<>();
    evidence.add("smoke\treal-catalog-and-batch-scan");
    evidence.add("iceberg_runtime\t" + System.getenv().getOrDefault("ICEBERG_VERSION", "unknown"));

    SparkSession spark = null;
    try {
      Class<?> sparkView = Class.forName("org.apache.spark.sql.connector.catalog.View");
      require(!sparkView.isInterface(), "candidate Spark View API unexpectedly has interface shape");
      Class.forName("org.apache.iceberg.spark.SparkCatalog");

      spark =
          SparkSession.builder()
              .master("local[1]")
              .appName("shuffle-recovery-iceberg-compatibility-smoke")
              .config("spark.ui.enabled", "false")
              .config("spark.sql.adaptive.enabled", "false")
              .config("spark.sql.catalog." + CATALOG, "org.apache.iceberg.spark.SparkCatalog")
              .config("spark.sql.catalog." + CATALOG + ".type", "hadoop")
              .config(
                  "spark.sql.catalog." + CATALOG + ".warehouse", warehouse.toUri().toString())
              .getOrCreate();

      spark.sql("CREATE NAMESPACE IF NOT EXISTS " + CATALOG + ".db");
      spark.sql("CREATE TABLE " + TABLE_NAME + " (id BIGINT, payload STRING) USING iceberg");
      spark.sql("INSERT INTO " + TABLE_NAME + " VALUES (1, 'smoke')");

      Dataset<Row> dataset = spark.table(TABLE_NAME).select("id", "payload");
      String plan = dataset.queryExecution().executedPlan().nodeName();
      List<Row> rows = dataset.collectAsList();
      require(rows.size() == 1, "smoke scan returned an unexpected row count");
      require(rows.get(0).getLong(0) == 1L, "smoke scan returned an unexpected id");
      require("smoke".equals(rows.get(0).getString(1)), "smoke scan returned an unexpected payload");

      evidence.add("spark_version\t" + spark.version());
      evidence.add("spark_view_api_shape\tclass");
      evidence.add("planned_root\t" + plan);
      evidence.add("catalog_initialization\tPASS");
      evidence.add("scan_planning\tPASS");
      evidence.add("scan_value_read\tPASS");
      evidence.add("result\tPASS");
      writeEvidence(evidencePath, evidence);
    } catch (Exception | LinkageError | AssertionError e) {
      evidence.add("failure_type\t" + e.getClass().getName());
      evidence.add("failure_message\t" + sanitize(e.getMessage()));
      evidence.add("result\tFAILED");
      writeEvidence(evidencePath, evidence);
      throw e;
    } finally {
      if (spark != null) {
        spark.stop();
      }
      deleteRecursively(workspace);
    }
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }

  private static String sanitize(String value) {
    if (value == null) {
      return "<none>";
    }
    return value.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ');
  }

  private static void writeEvidence(Path path, List<String> evidence) throws IOException {
    Files.createDirectories(path.getParent());
    Files.write(path, evidence, StandardCharsets.UTF_8);
  }

  private static void deleteRecursively(Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
      Path[] ordered = paths.sorted((left, right) -> right.compareTo(left)).toArray(Path[]::new);
      for (Path path : ordered) {
        Files.deleteIfExists(path);
      }
    }
  }
}
