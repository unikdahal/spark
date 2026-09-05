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
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import org.apache.spark.sql.{Column, TPCDSSchema}
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Executes the fast deterministic opportunity smoke corpus and the separately selected manual
 * evidence corpus using only Spark's in-tree TPC schemas and SQL resources.
 *
 * The generated rows are intentionally not presented as an official TPC scale factor. They keep
 * the study reproducible while the broad query matrix supplies plan-shape coverage. The manual
 * evidence mode is opt-in and must not become an unconditional per-PR cost.
 */
class ShuffleRecoveryOpportunityCorpusSuite extends SharedSparkSession with TPCDSSchema {

  private case class CorpusCase(family: String, resource: String, displayName: String, aqe: Boolean)

  private val smokeCases = Seq(
    CorpusCase("TPC-DS", "tpcds/q3.sql", "q3", aqe = false),
    CorpusCase("TPC-DS", "tpcds/q3.sql", "q3", aqe = true),
    CorpusCase("TPC-DS", "tpcds/q23a.sql", "q23a", aqe = true),
    CorpusCase("TPC-DS", "tpcds-v2.7.0/q51a.sql", "q51a-v2.7", aqe = true),
    CorpusCase("TPC-DS", "tpcds/q98.sql", "q98", aqe = false),
    CorpusCase("TPC-H", "tpch/q1.sql", "q1", aqe = false),
    CorpusCase("TPC-H", "tpch/q3.sql", "q3", aqe = true),
    CorpusCase("TPC-H", "tpch/q5.sql", "q5", aqe = true))

  // Spark's TPCDSBase uses this v1.4 set and excludes these six queries from normal golden tests
  // because q6/q75 can be flaky and the remaining entries are covered by the v2.7 corpus there.
  // Keeping the same practical exclusions avoids inventing a positive-result subset for this study.
  private val tpcdsEvidenceQueries = Seq(
    "q1", "q2", "q3", "q4", "q5", "q7", "q8", "q9", "q10", "q11",
    "q12", "q13", "q14a", "q14b", "q15", "q16", "q17", "q18", "q19", "q20",
    "q21", "q22", "q23a", "q23b", "q24a", "q24b", "q25", "q26", "q27", "q28", "q29",
    "q30", "q31", "q32", "q33", "q35", "q36", "q37", "q38", "q39a", "q39b", "q40",
    "q41", "q42", "q43", "q44", "q45", "q46", "q47", "q48", "q49", "q50",
    "q51", "q52", "q53", "q54", "q55", "q56", "q57", "q58", "q59", "q60",
    "q61", "q62", "q63", "q65", "q66", "q67", "q68", "q69", "q70",
    "q71", "q72", "q73", "q76", "q77", "q79", "q80",
    "q81", "q82", "q83", "q84", "q85", "q86", "q87", "q88", "q89", "q90",
    "q91", "q92", "q93", "q94", "q95", "q96", "q97", "q98", "q99")

  private val tpchEvidenceQueries = (1 to 22).map(index => s"q$index")

  private val evidenceCases = {
    val tpcds = tpcdsEvidenceQueries.flatMap { query =>
      Seq(false, true).map { aqe =>
        CorpusCase("TPC-DS", s"tpcds/$query.sql", query, aqe)
      }
    }
    val tpch = tpchEvidenceQueries.flatMap { query =>
      Seq(false, true).map { aqe =>
        CorpusCase("TPC-H", s"tpch/$query.sql", query, aqe)
      }
    }
    tpcds ++ tpch
  }

  private val tpchCreateTable = Map(
    "orders" ->
      """CREATE TABLE `orders` (`o_orderkey` BIGINT, `o_custkey` BIGINT,
        |`o_orderstatus` STRING, `o_totalprice` DECIMAL(10,0), `o_orderdate` DATE,
        |`o_orderpriority` STRING, `o_clerk` STRING, `o_shippriority` INT,
        |`o_comment` STRING) USING parquet""".stripMargin,
    "nation" ->
      """CREATE TABLE `nation` (`n_nationkey` BIGINT, `n_name` STRING,
        |`n_regionkey` BIGINT, `n_comment` STRING) USING parquet""".stripMargin,
    "region" ->
      """CREATE TABLE `region` (`r_regionkey` BIGINT, `r_name` STRING,
        |`r_comment` STRING) USING parquet""".stripMargin,
    "part" ->
      """CREATE TABLE `part` (`p_partkey` BIGINT, `p_name` STRING, `p_mfgr` STRING,
        |`p_brand` STRING, `p_type` STRING, `p_size` INT, `p_container` STRING,
        |`p_retailprice` DECIMAL(10,0), `p_comment` STRING) USING parquet""".stripMargin,
    "partsupp" ->
      """CREATE TABLE `partsupp` (`ps_partkey` BIGINT, `ps_suppkey` BIGINT,
        |`ps_availqty` INT, `ps_supplycost` DECIMAL(10,0), `ps_comment` STRING)
        |USING parquet""".stripMargin,
    "customer" ->
      """CREATE TABLE `customer` (`c_custkey` BIGINT, `c_name` STRING,
        |`c_address` STRING, `c_nationkey` BIGINT, `c_phone` STRING,
        |`c_acctbal` DECIMAL(10,0), `c_mktsegment` STRING, `c_comment` STRING)
        |USING parquet""".stripMargin,
    "supplier" ->
      """CREATE TABLE `supplier` (`s_suppkey` BIGINT, `s_name` STRING,
        |`s_address` STRING, `s_nationkey` BIGINT, `s_phone` STRING,
        |`s_acctbal` DECIMAL(10,0), `s_comment` STRING) USING parquet""".stripMargin,
    "lineitem" ->
      """CREATE TABLE `lineitem` (`l_orderkey` BIGINT, `l_partkey` BIGINT,
        |`l_suppkey` BIGINT, `l_linenumber` INT, `l_quantity` DECIMAL(10,0),
        |`l_extendedprice` DECIMAL(10,0), `l_discount` DECIMAL(10,0),
        |`l_tax` DECIMAL(10,0), `l_returnflag` STRING, `l_linestatus` STRING,
        |`l_shipdate` DATE, `l_commitdate` DATE, `l_receiptdate` DATE,
        |`l_shipinstruct` STRING, `l_shipmode` STRING, `l_comment` STRING)
        |USING parquet""".stripMargin)

  test("produce reconciled TPC-DS and TPC-H weighted opportunity evidence") {
    val mode = sys.env.getOrElse("SPARK_SHUFFLE_RECOVERY_CORPUS_MODE", "smoke")
    assert(Set("smoke", "evidence").contains(mode), s"unsupported opportunity corpus mode: $mode")
    val rowCount = if (mode == "evidence") 32 else 2
    val cases = if (mode == "evidence") evidenceCases else smokeCases

    val tpcdsCases = cases.filter(_.family == "TPC-DS")
    val tpchCases = cases.filter(_.family == "TPC-H")
    val tpcdsSnapshot = withTpcdsTables(rowCount) {
      runCases(tpcdsCases)
    }
    val tpchSnapshot = withTpchTables(rowCount) {
      runCases(tpchCases)
    }
    val combined = combine(tpcdsSnapshot, tpchSnapshot)
    combined.validateAccounting(ShuffleRecoveryStudyRuleSets.all)
    assert(combined.completedExecutionIds.size === cases.size)
    assert(combined.records.nonEmpty, "TPC corpus produced no shuffle observations")
    assert(combined.records.exists(_.disposition == ShuffleRecoveryWeightDisposition.Weighted),
      "TPC corpus produced no correlated physical shuffle work")

    val corpus = ShuffleRecoveryCorpusDefinition(
      name = s"spark-in-tree-tpc-$mode-v2",
      scale = s"$rowCount deterministic generated rows per table",
      baselineSha = ShuffleRecoveryOpportunityReportBuilder.FrozenBaselineSha,
      queries = cases.map(c => ShuffleRecoveryCorpusQuery(c.family, c.displayName, c.aqe)),
      sparkConfigs = Seq(
        SQLConf.SHUFFLE_PARTITIONS.key -> "4",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        "spark.master" -> spark.sparkContext.master),
      failureDistributionVersion =
        ShuffleRecoveryOpportunityReportBuilder.FailureDistributionVersion,
      gateRuleSetName = ShuffleRecoveryStudyRuleSets.exactSourceCounterfactual.rules.name,
      gateRuleSetVersion = ShuffleRecoveryStudyRuleSets.exactSourceCounterfactual.rules.version,
      gateThresholdBasisPoints = ShuffleRecoveryOpportunityReportBuilder.GateThresholdBasisPoints,
      reproductionCommands = Seq(reproductionCommand(mode)))
    val report = ShuffleRecoveryOpportunityReportBuilder.build(
      combined, ShuffleRecoveryStudyRuleSets.all, corpus)
    val gateRecords = combined.records.filter { record =>
      record.classification.ruleSetName == corpus.gateRuleSetName &&
        record.classification.ruleSetVersion == corpus.gateRuleSetVersion
    }
    val hasUnweightedGateEvidence = gateRecords.exists {
      _.disposition == ShuffleRecoveryWeightDisposition.Unweighted
    }
    assert(
      report.valueGate.result.nonEmpty ||
        hasUnweightedGateEvidence ||
        report.valueGate.completedExecutorRunTimeMs == 0L,
      "TPC corpus value gate may be N/A only for unweighted evidence or zero task time")
    val rawLines = combined.deterministicJsonLines(ShuffleRecoveryStudyRuleSets.all)
    assert(rawLines.nonEmpty)
    ShuffleRecoveryOpportunityRawIO.parseLines(rawLines)
    val rendered = report.toMarkdown
    val rebuilt = ShuffleRecoveryOpportunityReportBuilder.build(
      combined, ShuffleRecoveryStudyRuleSets.all, corpus).toMarkdown
    assert(rendered === rebuilt)
    assert(rendered.contains("## Reproduction"))
    assert(rendered.contains(s"SPARK_SHUFFLE_RECOVERY_CORPUS_MODE=$mode"))
    assert(rendered.contains("not benchmark-scale performance estimates"))

    if (mode == "evidence") {
      assert(cases.size >= 200, "manual evidence corpus must remain broad")
      assert(cases.count(_.family == "TPC-H") === 44, "TPC-H matrix must remain complete")
      assert(cases.exists(c => c.aqe) && cases.exists(c => !c.aqe),
        "manual evidence corpus must retain AQE on/off coverage")
    }

    writeArtifacts(mode, rawLines, rendered)
  }

  private def reproductionCommand(mode: String): String = {
    Seq(
      s"SPARK_SHUFFLE_RECOVERY_CORPUS_MODE=$mode \\",
      "SPARK_SHUFFLE_RECOVERY_OPPORTUNITY_DIR=\"$PWD/sql/core/target/" +
        "shuffle-recovery-phase0/opportunity\" \\",
      "./build/sbt -Phadoop-3 -Phive \\",
      "  \"sql/testOnly " +
        "org.apache.spark.sql.execution.exchange.ShuffleRecoveryOpportunityCorpusSuite\"")
      .mkString("\n")
  }

  private def runCases(cases: Seq[CorpusCase]): ShuffleRecoveryOpportunityStudySnapshot = {
    val study = new ShuffleRecoveryOpportunityStudy(spark)
    study.install()
    try {
      cases.foreach { corpusCase =>
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> corpusCase.aqe.toString,
          SQLConf.SHUFFLE_PARTITIONS.key -> "4",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true") {
          val query = resourceToString(
            corpusCase.resource,
            classLoader = Thread.currentThread().getContextClassLoader)
          spark.sql(query).collect()
        }
      }
      study.snapshot()
    } finally {
      study.close()
    }
  }

  private def combine(
      left: ShuffleRecoveryOpportunityStudySnapshot,
      right: ShuffleRecoveryOpportunityStudySnapshot): ShuffleRecoveryOpportunityStudySnapshot = {
    ShuffleRecoveryOpportunityStudySnapshot(
      records = left.records ++ right.records,
      completedExecutionIds = left.completedExecutionIds ++ right.completedExecutionIds,
      failedExecutions = left.failedExecutions ++ right.failedExecutions,
      analysisFailures = left.analysisFailures ++ right.analysisFailures,
      stages = left.stages ++ right.stages)
  }

  private def withTpcdsTables[T](rowCount: Int)(body: => T): T = {
    val tableNames = tableColumns.keys.toSeq.sorted
    try {
      tableNames.foreach { tableName =>
        val partitionClause = tablePartitionColumns.get(tableName) match {
          case Some(columns) if columns.nonEmpty => s"PARTITIONED BY (${columns.mkString(", ")})"
          case _ => ""
        }
        spark.sql(
          s"CREATE TABLE `$tableName` (${tableColumns(tableName)}) USING parquet $partitionClause")
      }
      tableNames.foreach(populate(_, rowCount))
      body
    } finally {
      tableNames.foreach(name => spark.sql(s"DROP TABLE IF EXISTS `$name`"))
    }
  }

  private def withTpchTables[T](rowCount: Int)(body: => T): T = {
    val tableNames = tpchCreateTable.keys.toSeq.sorted
    try {
      tableNames.foreach(name => spark.sql(tpchCreateTable(name)))
      tableNames.foreach(populate(_, rowCount))
      body
    } finally {
      tableNames.foreach(name => spark.sql(s"DROP TABLE IF EXISTS `$name`"))
    }
  }

  private def populate(tableName: String, rowCount: Int): Unit = {
    val schema = spark.table(tableName).schema
    val seed = spark.range(rowCount.toLong).toDF("_row_id")
    val columns = schema.fields.toIndexedSeq.map { field =>
      deterministicValue(col("_row_id"), field.name, field.dataType).as(field.name)
    }
    seed.select(columns: _*).write.mode("append").insertInto(tableName)
  }

  private def deterministicValue(rowId: Column, name: String, dataType: DataType): Column = {
    dataType match {
      case DateType =>
        date_add(lit("1998-01-01").cast(DateType), (rowId % 1460).cast(IntegerType))
      case TimestampType =>
        lit("2000-01-01 00:00:00").cast(TimestampType)
      case BooleanType =>
        rowId % 2 === 0
      case StringType =>
        deterministicString(rowId, name)
      case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
          _: DecimalType =>
        deterministicNumber(rowId, name).cast(dataType)
      case other =>
        lit(null).cast(other)
    }
  }

  private def deterministicNumber(rowId: Column, name: String): Column = {
    if (name.endsWith("_year") || name == "d_year") {
      lit(1998) + (rowId % 5)
    } else if (name.contains("month") || name.endsWith("_moy")) {
      (rowId % 12) + 1
    } else if (name.contains("hour")) {
      rowId % 24
    } else if (name.contains("minute")) {
      rowId % 60
    } else if (name.contains("quantity")) {
      (rowId % 20) + 1
    } else if (name.contains("discount") || name.contains("tax")) {
      rowId % 10
    } else {
      (rowId % 32) + 1
    }
  }

  private def deterministicString(rowId: Column, name: String): Column = {
    val values = if (name.contains("state")) {
      Seq("CA", "TX", "NY", "TN", "KY", "GA", "IL", "OH")
    } else if (name.contains("gender")) {
      Seq("M", "F")
    } else if (name.contains("marital")) {
      Seq("M", "S", "D", "W")
    } else if (name.contains("country")) {
      Seq("UNITED STATES", "CANADA", "JAPAN", "GERMANY")
    } else if (name.contains("shipmode")) {
      Seq("AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK")
    } else if (name.contains("segment")) {
      Seq("AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD")
    } else if (name.contains("returnflag")) {
      Seq("A", "N", "R")
    } else if (name.contains("linestatus")) {
      Seq("F", "O")
    } else {
      Seq("1", "2", "3", "4", "5", "6", "7", "8")
    }
    element_at(array(values.map(lit): _*), (rowId % values.size + 1).cast(IntegerType))
  }

  private def writeArtifacts(mode: String, rawLines: Seq[String], report: String): Unit = {
    val root = Paths.get(sys.env.getOrElse(
      "SPARK_SHUFFLE_RECOVERY_OPPORTUNITY_DIR",
      "target/shuffle-recovery-phase0/opportunity"))
    Files.createDirectories(root)
    write(root.resolve(s"opportunity-$mode.jsonl"), rawLines.mkString("\n") + "\n")
    write(root.resolve(s"opportunity-$mode.md"), report)
  }

  private def write(path: Path, content: String): Unit = {
    Files.write(
      path,
      content.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.WRITE)
  }
}
