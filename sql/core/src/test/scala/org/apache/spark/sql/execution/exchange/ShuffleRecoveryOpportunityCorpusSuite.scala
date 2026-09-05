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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{Column, DataFrame, TPCDSSchema}
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Runs the fast Phase 0-A smoke corpus and the separately selected manual evidence campaign.
 *
 * Smoke remains intentionally tiny and deterministic. Evidence mode broadens query/configuration
 * coverage without adding a benchmark-data dependency and is intended only for manual dispatch.
 */
class ShuffleRecoveryOpportunityCorpusSuite extends SharedSparkSession with TPCDSSchema {

  private case class CorpusCase(
      family: String,
      resource: String,
      displayName: String,
      aqe: Boolean)

  private case class ExecutedCase(
      family: String,
      displayName: String,
      aqe: Boolean,
      executionId: Long)

  private case class CorpusRun(
      snapshot: ShuffleRecoveryOpportunityStudySnapshot,
      executions: Seq[ExecutedCase])

  private val CorrelationGateBasisPoints = 9500L

  private val smokeCases = Seq(
    CorpusCase("TPC-DS", "tpcds/q3.sql", "q3", aqe = false),
    CorpusCase("TPC-DS", "tpcds/q3.sql", "q3", aqe = true),
    CorpusCase("TPC-DS", "tpcds/q23a.sql", "q23a", aqe = true),
    CorpusCase("TPC-DS", "tpcds-v2.7.0/q51a.sql", "q51a-v2.7", aqe = true),
    CorpusCase("TPC-DS", "tpcds/q98.sql", "q98", aqe = false),
    CorpusCase("TPC-H", "tpch/q1.sql", "q1", aqe = false),
    CorpusCase("TPC-H", "tpch/q3.sql", "q3", aqe = true),
    CorpusCase("TPC-H", "tpch/q5.sql", "q5", aqe = true))

  private val evidenceTpcdsQueries = Seq(
    "q1", "q3", "q7", "q10", "q13", "q14a", "q14b", "q16",
    "q17", "q19", "q23a", "q23b", "q24a", "q24b", "q27", "q34",
    "q39a", "q39b", "q42", "q43", "q46", "q52", "q53", "q59",
    "q61", "q64", "q68", "q73", "q77", "q79", "q88", "q98")

  private val evidenceTpcdsCases = evidenceTpcdsQueries.flatMap { query =>
    Seq(false, true).map { aqe =>
      CorpusCase("TPC-DS", s"tpcds/$query.sql", query, aqe)
    }
  }

  private val evidenceTpchCases = (1 to 22).flatMap { queryNumber =>
    Seq(false, true).map { aqe =>
      val query = s"q$queryNumber"
      CorpusCase("TPC-H", s"tpch/$query.sql", query, aqe)
    }
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
    assert(Set("smoke", "evidence").contains(mode),
      s"unsupported opportunity corpus mode: $mode")
    val evidenceMode = mode == "evidence"
    val rowCount = if (evidenceMode) 64 else 2
    val benchmarkCases = if (evidenceMode) {
      evidenceTpcdsCases ++ evidenceTpchCases
    } else {
      smokeCases
    }

    val tpcdsCases = benchmarkCases.filter(_.family == "TPC-DS")
    val tpchCases = benchmarkCases.filter(_.family == "TPC-H")
    val tpcdsRun = withTpcdsTables(rowCount) {
      runSqlCases(tpcdsCases)
    }
    val tpchRun = withTpchTables(rowCount) {
      runSqlCases(tpchCases)
    }
    val syntheticRun = if (evidenceMode) runSyntheticCases() else emptyRun
    val combined = combine(Seq(tpcdsRun, tpchRun, syntheticRun))

    combined.snapshot.validateAccounting(ShuffleRecoveryStudyRuleSets.all)
    assert(combined.snapshot.completedExecutionIds.size === combined.executions.size)
    assert(combined.snapshot.records.nonEmpty, "TPC corpus produced no shuffle observations")
    assert(combined.snapshot.records.exists {
      _.disposition == ShuffleRecoveryWeightDisposition.Weighted
    }, "TPC corpus produced no correlated physical shuffle work")

    val correlation = validateCorrelationQuality(combined.snapshot, evidenceMode)
    val corpus = ShuffleRecoveryCorpusDefinition(
      name = s"spark-in-tree-tpc-$mode-v2",
      scale = s"$rowCount deterministic generated rows per benchmark table",
      baselineSha = ShuffleRecoveryOpportunityReportBuilder.FrozenBaselineSha,
      queries = combined.executions.map { executed =>
        ShuffleRecoveryCorpusQuery(executed.family, executed.displayName, executed.aqe)
      },
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
      combined.snapshot, ShuffleRecoveryStudyRuleSets.all, corpus)
    val gateRecords = gateRuleRecords(combined.snapshot)
    val hasUnweightedGateEvidence = gateRecords.exists {
      _.disposition == ShuffleRecoveryWeightDisposition.Unweighted
    }
    assert(
      report.valueGate.result.nonEmpty ||
        hasUnweightedGateEvidence ||
        report.valueGate.completedExecutorRunTimeMs == 0L,
      "TPC corpus value gate may be N/A only for unweighted evidence or zero task time")
    val rawLines = combined.snapshot.deterministicJsonLines(ShuffleRecoveryStudyRuleSets.all)
    assert(rawLines.nonEmpty)
    ShuffleRecoveryOpportunityRawIO.parseLines(rawLines)
    val rendered = report.toMarkdown
    val rebuilt = ShuffleRecoveryOpportunityReportBuilder.build(
      combined.snapshot, ShuffleRecoveryStudyRuleSets.all, corpus).toMarkdown
    assert(rendered === rebuilt)
    assert(rendered.contains("## Reproduction"))
    assert(rendered.contains(s"SPARK_SHUFFLE_RECOVERY_CORPUS_MODE=$mode"))
    assert(rendered.contains("not benchmark-scale performance estimates"))

    writeArtifacts(
      mode,
      rawLines,
      rendered,
      renderCorrelationReport(mode, correlation, combined.snapshot),
      renderReconciliation(gateRecords, combined.snapshot.stages),
      renderSensitivity(combined.executions, gateRecords))
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

  private def runSqlCases(cases: Seq[CorpusCase]): CorpusRun = {
    if (cases.isEmpty) return emptyRun
    val study = new ShuffleRecoveryOpportunityStudy(spark)
    study.install()
    try {
      cases.foreach { corpusCase =>
        withEvidenceSqlConf(corpusCase.aqe) {
          val query = resourceToString(
            corpusCase.resource,
            classLoader = Thread.currentThread().getContextClassLoader)
          spark.sql(query).collect()
        }
      }
      val snapshot = study.snapshot()
      assert(snapshot.completedExecutionIds.size === cases.size,
        "each benchmark case must produce exactly one completed SQL execution")
      val executions = cases.zip(snapshot.completedExecutionIds).map { case (c, id) =>
        ExecutedCase(c.family, c.displayName, c.aqe, id)
      }
      CorpusRun(snapshot, executions)
    } finally {
      study.close()
    }
  }

  private def runSyntheticCases(): CorpusRun = {
    val cases = Seq(
      "sparse-shuffle" -> (() => sparseShuffle()),
      "wide-reducer-count" -> (() => wideReducerShuffle()),
      "many-mappers" -> (() => manyMapperShuffle()),
      "heavy-single-exchange" -> (() => heavySingleExchange()),
      "sequential-upstream-shuffles" -> (() => sequentialShuffles()),
      "reused-exchange" -> (() => reusedExchange()),
      "zero-output-shuffle" -> (() => zeroOutputShuffle()),
      "controlled-skew" -> (() => controlledSkew()))
    val expanded = Seq(false, true).flatMap { aqe =>
      cases.map { case (name, action) => (name, aqe, action) }
    }
    val study = new ShuffleRecoveryOpportunityStudy(spark)
    study.install()
    try {
      expanded.foreach { case (_, aqe, action) =>
        withEvidenceSqlConf(aqe) {
          action()
        }
      }
      val snapshot = study.snapshot()
      assert(snapshot.completedExecutionIds.size === expanded.size,
        "each synthetic case must produce exactly one completed SQL execution")
      val executions = expanded.zip(snapshot.completedExecutionIds).map {
        case ((name, aqe, _), id) => ExecutedCase("SYNTHETIC", name, aqe, id)
      }
      CorpusRun(snapshot, executions)
    } finally {
      study.close()
    }
  }

  private def withEvidenceSqlConf[T](aqe: Boolean)(body: => T): T = {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
      SQLConf.SHUFFLE_PARTITIONS.key -> "4",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true") {
      body
    }
  }

  private def sparseShuffle(): Unit = {
    spark.range(0L, 256L, 1L, 8)
      .filter(col("id") % 64L === 0L)
      .repartition(16, col("id"))
      .collect()
  }

  private def wideReducerShuffle(): Unit = {
    spark.range(0L, 1024L, 1L, 8)
      .repartition(64, col("id"))
      .groupBy(col("id") % 8L)
      .count()
      .collect()
  }

  private def manyMapperShuffle(): Unit = {
    spark.range(0L, 2048L, 1L, 32)
      .repartition(8, col("id"))
      .groupBy(col("id") % 16L)
      .count()
      .collect()
  }

  private def heavySingleExchange(): Unit = {
    spark.range(0L, 20000L, 1L, 16)
      .select(col("id"), (col("id") % 128L).as("k"))
      .repartition(16, col("k"))
      .groupBy("k")
      .count()
      .collect()
  }

  private def sequentialShuffles(): Unit = {
    val first = spark.range(0L, 4096L, 1L, 16)
      .groupBy((col("id") % 32L).as("k"))
      .count()
    first.repartition(8, col("k"))
      .groupBy((col("k") % 4L).as("bucket"))
      .sum("count")
      .collect()
  }

  private def reusedExchange(): Unit = {
    val left = spark.range(0L, 1024L, 1L, 8)
      .select((col("id") % 32L).as("k"), col("id").as("v"))
      .groupBy("k")
      .sum("v")
    left.as("a")
      .join(left.as("b"), col("a.k") === col("b.k"))
      .select(col("a.k"))
      .collect()
  }

  private def zeroOutputShuffle(): Unit = {
    spark.range(0L, 256L, 1L, 8)
      .filter(col("id") < 0L)
      .repartition(8, col("id"))
      .collect()
  }

  private def controlledSkew(): Unit = {
    spark.range(0L, 4096L, 1L, 16)
      .select(when(col("id") < 3800L, lit(0L)).otherwise(col("id")).as("k"))
      .repartition(8, col("k"))
      .groupBy("k")
      .count()
      .collect()
  }

  private def combine(runs: Seq[CorpusRun]): CorpusRun = {
    val nonEmpty = runs.filter(_.executions.nonEmpty)
    CorpusRun(
      ShuffleRecoveryOpportunityStudySnapshot(
        records = nonEmpty.flatMap(_.snapshot.records),
        completedExecutionIds = nonEmpty.flatMap(_.snapshot.completedExecutionIds),
        failedExecutions = nonEmpty.flatMap(_.snapshot.failedExecutions),
        analysisFailures = nonEmpty.flatMap(_.snapshot.analysisFailures),
        stages = nonEmpty.flatMap(_.snapshot.stages)),
      nonEmpty.flatMap(_.executions))
  }

  private def emptyRun: CorpusRun = {
    CorpusRun(
      ShuffleRecoveryOpportunityStudySnapshot(Nil, Nil, Nil, Nil, Nil),
      Nil)
  }

  private case class CorrelationQuality(
      weightedStageCount: Long,
      completedStageCount: Long,
      weightedBytes: Long,
      completedBytes: Long,
      weightedTaskTimeMs: Long,
      completedTaskTimeMs: Long,
      taskTimeBasisPoints: Long)

  private def validateCorrelationQuality(
      snapshot: ShuffleRecoveryOpportunityStudySnapshot,
      evidenceMode: Boolean): CorrelationQuality = {
    val gateRecords = gateRuleRecords(snapshot)
    val weightedPhysical = gateRecords.flatMap { record =>
      for {
        executionId <- executionNumber(record.classification.executionId)
        shuffleId <- record.shuffleId
        if record.disposition == ShuffleRecoveryWeightDisposition.Weighted
      } yield (executionId, shuffleId)
    }.toSet
    val completedStages = snapshot.stages.filter(_.complete)
    val correlatedStages = completedStages.filter { stage =>
      weightedPhysical.contains((stage.executionId, stage.shuffleId))
    }
    val completedTime = checkedSum(completedStages.map(_.executorRunTimeMs))
    val correlatedTime = checkedSum(correlatedStages.map(_.executorRunTimeMs))
    val completedBytes = checkedSum(completedStages.map(_.shuffleWriteBytes))
    val correlatedBytes = checkedSum(correlatedStages.map(_.shuffleWriteBytes))
    val basisPoints = ratioBasisPoints(correlatedTime, completedTime).getOrElse(10000L)
    assert(correlatedTime <= completedTime)
    assert(correlatedBytes <= completedBytes)
    assert(
      basisPoints >= CorrelationGateBasisPoints,
      s"runtime correlation coverage $basisPoints bp is below the frozen " +
        s"$CorrelationGateBasisPoints bp gate")
    if (evidenceMode) {
      assert(completedStages.size >= 100,
        "manual evidence campaign must materially execute at least 100 shuffle stages")
    }
    CorrelationQuality(
      correlatedStages.size,
      completedStages.size,
      correlatedBytes,
      completedBytes,
      correlatedTime,
      completedTime,
      basisPoints)
  }

  private def gateRuleRecords(
      snapshot: ShuffleRecoveryOpportunityStudySnapshot):
      Seq[ShuffleRecoveryWeightedObservation] = {
    val gate = ShuffleRecoveryStudyRuleSets.exactSourceCounterfactual.rules
    snapshot.records.filter { record =>
      record.classification.ruleSetName == gate.name &&
        record.classification.ruleSetVersion == gate.version
    }
  }

  private def renderCorrelationReport(
      mode: String,
      quality: CorrelationQuality,
      snapshot: ShuffleRecoveryOpportunityStudySnapshot): String = {
    val gateRecords = gateRuleRecords(snapshot)
    val weighted = gateRecords.count(_.disposition == ShuffleRecoveryWeightDisposition.Weighted)
    val unweighted = gateRecords.count(_.disposition == ShuffleRecoveryWeightDisposition.Unweighted)
    val excluded = gateRecords.count(_.disposition == ShuffleRecoveryWeightDisposition.Excluded)
    Seq(
      s"# Phase 0-A runtime-correlation quality ($mode)",
      "",
      s"- Frozen minimum completed-task-time coverage: 95.0%",
      s"- Result: ${renderBasisPoints(quality.taskTimeBasisPoints)}",
      s"- Completed shuffle stages: ${quality.completedStageCount}",
      s"- Correlated completed shuffle stages: ${quality.weightedStageCount}",
      s"- Completed shuffle-map task time (ms): ${quality.completedTaskTimeMs}",
      s"- Correlated shuffle-map task time (ms): ${quality.weightedTaskTimeMs}",
      s"- Completed shuffle-write bytes: ${quality.completedBytes}",
      s"- Correlated shuffle-write bytes: ${quality.weightedBytes}",
      s"- Observed gate-rule exchanges: ${gateRecords.size}",
      s"- Correlated/excluded/unweighted exchanges: $weighted/$excluded/$unweighted",
      "",
      "The denominator is materially executed, completed shuffle-map-stage work. " +
        "Merely planned exchanges do not add task time or bytes.",
      "")
      .mkString("\n")
  }

  private def renderReconciliation(
      gateRecords: Seq[ShuffleRecoveryWeightedObservation],
      stages: Seq[ShuffleRecoveryStageRuntime]): String = {
    val stageIndex = stages.map { stage =>
      (stage.executionId, stage.shuffleId) -> stage
    }.toMap
    val header = Seq(
      "executionId",
      "exchangeOrdinal",
      "exchangePath",
      "disposition",
      "reason",
      "stageId",
      "stageAttemptId",
      "shuffleId",
      "expectedMappers",
      "successfulMapWinners",
      "stageComplete",
      "shuffleWriteBytes",
      "executorRunTimeMs").mkString("\t")
    val rows = gateRecords.sortBy { record =>
      (record.classification.executionId, record.classification.exchangeOrdinal)
    }.map { record =>
      val stage = for {
        executionId <- executionNumber(record.classification.executionId)
        shuffleId <- record.shuffleId
        runtime <- stageIndex.get((executionId, shuffleId))
      } yield runtime
      Seq(
        record.classification.executionId,
        record.classification.exchangeOrdinal.toString,
        record.classification.exchangePath,
        record.disposition.code,
        record.accountingReason.getOrElse(""),
        record.stageId.map(_.toString).getOrElse(""),
        record.stageAttemptId.map(_.toString).getOrElse(""),
        record.shuffleId.map(_.toString).getOrElse(""),
        stage.map(_.expectedMapTasks.toString).getOrElse(""),
        stage.map(_.successfulMapTaskWinners.toString).getOrElse(""),
        stage.map(_.complete.toString).getOrElse(""),
        record.shuffleWriteBytes.map(_.toString).getOrElse(""),
        record.executorRunTimeMs.map(_.toString).getOrElse("")).mkString("\t")
    }
    (header +: rows).mkString("\n") + "\n"
  }

  private def renderSensitivity(
      executions: Seq[ExecutedCase],
      gateRecords: Seq[ShuffleRecoveryWeightedObservation]): String = {
    val executionIndex = executions.map(e => e.executionId -> e).toMap
    val weighted = gateRecords.filter(_.disposition == ShuffleRecoveryWeightDisposition.Weighted)
    val byExecution = weighted.groupBy { record =>
      executionNumber(record.classification.executionId).get
    }
    val queryRows = executions.map { executed =>
      val rows = byExecution.getOrElse(executed.executionId, Nil)
      val total = checkedSum(rows.flatMap(_.executorRunTimeMs))
      val eligible = checkedSum(rows.filter(_.classification.eligible).flatMap(_.executorRunTimeMs))
      (executed, total, eligible, ratioBasisPoints(eligible, total))
    }
    val totalTime = checkedSum(weighted.flatMap(_.executorRunTimeMs))
    val eligibleTime = checkedSum(
      weighted.filter(_.classification.eligible).flatMap(_.executorRunTimeMs))
    val sortedExchanges = weighted.sortBy(_.executorRunTimeMs.getOrElse(0L)).reverse
    val top1 = checkedSum(sortedExchanges.take(1).flatMap(_.executorRunTimeMs))
    val top5 = checkedSum(sortedExchanges.take(5).flatMap(_.executorRunTimeMs))
    val sortedQueries = queryRows.sortBy(_._2).reverse
    val topQuery = sortedQueries.headOption.map(_._2).getOrElse(0L)
    val top5Queries = checkedSum(sortedQueries.take(5).map(_._2))
    val withoutTop1 = opportunityWithout(sortedExchanges, 1)
    val withoutTop5 = opportunityWithout(sortedExchanges, 5)
    val queryRatios = queryRows.flatMap(_._4)
    val queryWeightedBasisPoints = if (queryRatios.isEmpty) None else {
      Some(queryRatios.sum / queryRatios.size)
    }

    val out = new StringBuilder
    out.append("# Phase 0-A sample-quality and sensitivity\n\n")
    out.append("Physical exchanges are not IID samples. Results are nested by query, ")
    out.append("AQE configuration, and benchmark family.\n\n")
    out.append(s"- Aggregate task-time opportunity: ${renderRatio(eligibleTime, totalTime)}\n")
    out.append(s"- Equal-query opportunity: ${renderOptional(queryWeightedBasisPoints)}\n")
    out.append(s"- Remove top 1 exchange: ${renderOptional(withoutTop1)}\n")
    out.append(s"- Remove top 5 exchanges: ${renderOptional(withoutTop5)}\n")
    out.append(s"- Top 1 exchange concentration: ${renderRatio(top1, totalTime)}\n")
    out.append(s"- Top 5 exchange concentration: ${renderRatio(top5, totalTime)}\n")
    out.append(s"- Top 1 query concentration: ${renderRatio(topQuery, totalTime)}\n")
    out.append(s"- Top 5 query concentration: ${renderRatio(top5Queries, totalTime)}\n")
    out.append(s"- Queries with zero eligible material work: ")
    out.append(queryRows.count { case (_, _, eligible, _) => eligible == 0L })
    out.append(s"/${queryRows.size}\n\n")
    appendGroupedSensitivity(out, "Benchmark family", queryRows.groupBy(_._1.family))
    appendGroupedSensitivity(out, "AQE mode", queryRows.groupBy(row => row._1.aqe.toString))
    out.append("## Per-query opportunity\n\n")
    out.append("| Family | Query | AQE | Task time (ms) | Eligible (ms) | Opportunity |\n")
    out.append("|---|---|---|---:|---:|---:|\n")
    queryRows.foreach { case (executed, total, eligible, ratio) =>
      out.append(s"| ${executed.family} | ${executed.displayName} | ${executed.aqe} | ")
      out.append(s"$total | $eligible | ${renderOptional(ratio)} |\n")
    }
    out.toString()
  }

  private def appendGroupedSensitivity(
      out: StringBuilder,
      title: String,
      groups: Map[String, Seq[(ExecutedCase, Long, Long, Option[Long])]]): Unit = {
    out.append(s"## $title split\n\n")
    out.append("| Group | Task time (ms) | Eligible (ms) | Opportunity |\n")
    out.append("|---|---:|---:|---:|\n")
    groups.toSeq.sortBy(_._1).foreach { case (group, rows) =>
      val total = checkedSum(rows.map(_._2))
      val eligible = checkedSum(rows.map(_._3))
      out.append(s"| $group | $total | $eligible | ${renderRatio(eligible, total)} |\n")
    }
    out.append("\n")
  }

  private def opportunityWithout(
      sorted: Seq[ShuffleRecoveryWeightedObservation],
      remove: Int): Option[Long] = {
    val remaining = sorted.drop(remove)
    val total = checkedSum(remaining.flatMap(_.executorRunTimeMs))
    val eligible = checkedSum(
      remaining.filter(_.classification.eligible).flatMap(_.executorRunTimeMs))
    ratioBasisPoints(eligible, total)
  }

  private def ratioBasisPoints(numerator: Long, denominator: Long): Option[Long] = {
    if (denominator == 0L) None
    else Some(((BigInt(numerator) * 10000) / BigInt(denominator)).toLong)
  }

  private def renderRatio(numerator: Long, denominator: Long): String = {
    renderOptional(ratioBasisPoints(numerator, denominator))
  }

  private def renderOptional(basisPoints: Option[Long]): String = {
    basisPoints.map(renderBasisPoints).getOrElse("N/A")
  }

  private def renderBasisPoints(basisPoints: Long): String = {
    f"${basisPoints / 100.0}%.1f%%"
  }

  private def checkedSum(values: Iterable[Long]): Long = {
    values.foldLeft(0L)(Math.addExact)
  }

  private def executionNumber(executionId: String): Option[Long] = {
    val prefix = "query-"
    if (!executionId.startsWith(prefix)) None
    else Some(executionId.substring(prefix.length).toLong)
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
      deterministicValue(tableName, col("_row_id"), field).as(field.name)
    }
    seed.select(columns: _*).write.mode("append").insertInto(tableName)
  }

  private def deterministicValue(
      tableName: String,
      rowId: Column,
      field: StructField): Column = {
    field.dataType match {
      case StringType => deterministicString(field.name, rowId)
      case DateType => deterministicDate(tableName, rowId)
      case TimestampType => lit("2000-01-01 00:00:00").cast(TimestampType)
      case BooleanType => rowId % 2L === 0L
      case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
          _: DecimalType =>
        ((rowId % 7L) + 1L).cast(field.dataType)
      case other => lit(null).cast(other)
    }
  }

  private def deterministicDate(tableName: String, rowId: Column): Column = {
    val base = if (tpchCreateTable.contains(tableName)) "1995-01-01" else "2000-01-01"
    date_add(lit(base).cast(DateType), (rowId % 31L).cast(IntegerType))
  }

  private def deterministicString(fieldName: String, rowId: Column): Column = {
    val fixed = fieldName match {
      case name if name.endsWith("_state") => Some("CA")
      case name if name.endsWith("_country") => Some("United States")
      case "c_mktsegment" => Some("BUILDING")
      case "l_shipmode" => Some("MAIL")
      case "l_shipinstruct" => Some("DELIVER IN PERSON")
      case "l_returnflag" => Some("R")
      case "l_linestatus" => Some("O")
      case "o_orderpriority" => Some("1-URGENT")
      case "o_orderstatus" => Some("O")
      case "p_brand" => Some("Brand#12")
      case "p_container" => Some("SM CASE")
      case "p_type" => Some("PROMO BURNISHED COPPER")
      case "r_name" => Some("AMERICA")
      case "n_name" => Some("UNITED STATES")
      case name if name.endsWith("_gender") => Some("M")
      case name if name.endsWith("_marital_status") => Some("M")
      case name if name.endsWith("_education_status") => Some("College")
      case name if name.endsWith("_buy_potential") => Some(">10000")
      case name if name.endsWith("_color") => Some("red")
      case name if name.endsWith("_category") => Some("Books")
      case _ => None
    }
    fixed.map(lit).getOrElse(((rowId % 7L) + 1L).cast(StringType))
  }

  private def writeArtifacts(
      mode: String,
      rawLines: Seq[String],
      report: String,
      correlation: String,
      reconciliation: String,
      sensitivity: String): Unit = {
    val root = Paths.get(sys.env.getOrElse(
      "SPARK_SHUFFLE_RECOVERY_OPPORTUNITY_DIR",
      "target/shuffle-recovery-phase0/opportunity"))
    Files.createDirectories(root)
    write(root.resolve(s"opportunity-$mode.jsonl"), rawLines.mkString("\n") + "\n")
    write(root.resolve(s"opportunity-$mode.md"), report)
    write(root.resolve(s"correlation-$mode.md"), correlation)
    write(root.resolve(s"reconciliation-$mode.tsv"), reconciliation)
    write(root.resolve(s"sensitivity-$mode.md"), sensitivity)
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