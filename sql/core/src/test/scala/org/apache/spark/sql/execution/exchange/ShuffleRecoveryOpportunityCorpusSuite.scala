/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
 * Executes a deliberately tiny but real TPC-DS/TPC-H corpus using only Spark's in-tree schemas and
 * SQL resources. The tiny rows make the suite suitable for CI; the manual mode widens query and row
 * coverage without adding a benchmark-data dependency.
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

  private val largerOnlyCases = Seq(
    CorpusCase("TPC-DS", "tpcds/q7.sql", "q7", aqe = false),
    CorpusCase("TPC-DS", "tpcds/q19.sql", "q19", aqe = true),
    CorpusCase("TPC-DS", "tpcds/q42.sql", "q42", aqe = false),
    CorpusCase("TPC-DS", "tpcds/q52.sql", "q52", aqe = true),
    CorpusCase("TPC-DS", "tpcds/q73.sql", "q73", aqe = true),
    CorpusCase("TPC-H", "tpch/q12.sql", "q12", aqe = false),
    CorpusCase("TPC-H", "tpch/q18.sql", "q18", aqe = true))

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
    assert(Set("smoke", "larger").contains(mode), s"unsupported opportunity corpus mode: $mode")
    val rowCount = if (mode == "larger") 4 else 2
    val cases = if (mode == "larger") smokeCases ++ largerOnlyCases else smokeCases

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
      name = s"spark-in-tree-tpc-$mode-v1",
      scale = s"$rowCount deterministic generated rows per table",
      baselineSha = ShuffleRecoveryOpportunityReportBuilder.FrozenBaselineSha,
      queries = cases.map(c => ShuffleRecoveryCorpusQuery(c.family, c.displayName, c.aqe)),
      sparkConfigs = Seq(
        SQLConf.SHUFFLE_PARTITIONS.key -> "4",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        "spark.master" -> spark.sparkContext.master),
      failureDistributionVersion = ShuffleRecoveryOpportunityReportBuilder.FailureDistributionVersion,
      gateRuleSetName = ShuffleRecoveryStudyRuleSets.exactSourceCounterfactual.rules.name,
      gateRuleSetVersion = ShuffleRecoveryStudyRuleSets.exactSourceCounterfactual.rules.version,
      gateThresholdBasisPoints = ShuffleRecoveryOpportunityReportBuilder.GateThresholdBasisPoints)
    val report = ShuffleRecoveryOpportunityReportBuilder.build(
      combined, ShuffleRecoveryStudyRuleSets.all, corpus)
    val rawLines = combined.deterministicJsonLines(ShuffleRecoveryStudyRuleSets.all)
    assert(rawLines.nonEmpty)
    ShuffleRecoveryOpportunityRawIO.parseLines(rawLines)
    assert(report.toMarkdown === report.toMarkdown)

    writeArtifacts(mode, rawLines, report.toMarkdown)
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
    val columns = schema.fields.map { field =>
      deterministicValue(col("_row_id"), field.dataType).as(field.name)
    }
    seed.select(columns: _*).write.mode("append").insertInto(tableName)
  }

  private def deterministicValue(rowId: Column, dataType: DataType): Column = dataType match {
    case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType | _: DecimalType =>
      ((rowId % 3) + 1).cast(dataType)
    case StringType => concat(lit("value-"), (rowId % 3).cast(StringType))
    case DateType => date_add(lit("2000-01-01").cast(DateType), (rowId % 3).cast(IntegerType))
    case TimestampType => lit("2000-01-01 00:00:00").cast(TimestampType)
    case BooleanType => (rowId % 2 === 0)
    case other => lit(null).cast(other)
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
