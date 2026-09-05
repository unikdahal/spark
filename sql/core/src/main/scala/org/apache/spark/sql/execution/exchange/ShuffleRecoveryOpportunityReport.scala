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

/** A ratio with an explicit undefined state for an empty denominator. */
private[sql] case class ShuffleRecoveryRatio(numerator: Long, denominator: Long) {
  require(numerator >= 0L, "ratio numerator must be non-negative")
  require(denominator >= 0L, "ratio denominator must be non-negative")
  require(
    numerator <= denominator,
    s"ratio numerator $numerator exceeds denominator $denominator")

  def basisPoints: Option[Long] = {
    if (denominator == 0L) None
    else Some(((BigInt(numerator) * 10000) / BigInt(denominator)).toLong)
  }

  def render: String = basisPoints match {
    case None => "N/A"
    case Some(value) => f"${value / 100.0}%.1f%%"
  }
}

private[sql] case class ShuffleRecoveryMissWeight(
    reason: String,
    exchangeCount: Long,
    executorRunTimeMs: Long)

private[sql] case class ShuffleRecoveryRuleSummary(
    name: String,
    version: Int,
    curveRole: String,
    observedExchangeCount: Long,
    excludedExchangeCount: Long,
    unweightedExchangeCount: Long,
    weightedExchangeCount: Long,
    eligibleExchangeCount: Long,
    weightedShuffleWriteBytes: Long,
    eligibleShuffleWriteBytes: Long,
    weightedExecutorRunTimeMs: Long,
    eligibleExecutorRunTimeMs: Long,
    correlationRatio: ShuffleRecoveryRatio,
    countRatio: ShuffleRecoveryRatio,
    byteRatio: ShuffleRecoveryRatio,
    taskTimeRatio: ShuffleRecoveryRatio,
    sourceTokenCounts: Seq[(String, Long)],
    accountingReasons: Seq[(String, Long)],
    misses: Seq[ShuffleRecoveryMissWeight])

private[sql] case class ShuffleRecoveryFailurePointRow(
    executionId: String,
    point: String,
    applicable: Boolean,
    allExchangeCount: Long,
    completedExchangeCount: Long,
    eligibleCompletedExchangeCount: Long,
    completedShuffleWriteBytes: Long,
    avoidableShuffleWriteBytes: Long,
    completedExecutorRunTimeMs: Long,
    avoidableExecutorRunTimeMs: Long)

private[sql] case class ShuffleRecoveryFailurePointSummary(
    point: String,
    applicableExecutions: Long,
    allExchangeCount: Long,
    completedExchangeCount: Long,
    eligibleCompletedExchangeCount: Long,
    completedShuffleWriteBytes: Long,
    avoidableShuffleWriteBytes: Long,
    completedExecutorRunTimeMs: Long,
    avoidableExecutorRunTimeMs: Long,
    byteRatio: ShuffleRecoveryRatio,
    taskTimeRatio: ShuffleRecoveryRatio)

private[sql] case class ShuffleRecoveryValueGate(
    ruleSetName: String,
    ruleSetVersion: Int,
    distributionVersion: String,
    thresholdBasisPoints: Long,
    reusableExecutorRunTimeMs: Long,
    completedExecutorRunTimeMs: Long,
    result: Option[Boolean]) {

  def renderedResult: String = result match {
    case Some(true) => "PASS"
    case Some(false) => "FAIL"
    case None => "N/A"
  }
}

private[sql] case class ShuffleRecoveryCorpusQuery(
    family: String,
    name: String,
    aqeEnabled: Boolean)

private[sql] case class ShuffleRecoveryCorpusDefinition(
    name: String,
    scale: String,
    baselineSha: String,
    queries: Seq[ShuffleRecoveryCorpusQuery],
    sparkConfigs: Seq[(String, String)],
    failureDistributionVersion: String,
    gateRuleSetName: String,
    gateRuleSetVersion: Int,
    gateThresholdBasisPoints: Long,
    reproductionCommands: Seq[String] = Nil) {

  require(name.nonEmpty, "corpus name must be non-empty")
  require(scale.nonEmpty, "corpus scale must be non-empty")
  require(
    baselineSha.matches("[0-9a-f]{40}"),
    "baseline SHA must be a 40-character hex SHA")
  require(gateRuleSetName.nonEmpty, "gate rule-set name must be non-empty")
  require(gateRuleSetVersion > 0, "gate rule-set version must be positive")
  require(
    gateThresholdBasisPoints >= 0L && gateThresholdBasisPoints <= 10000L,
    "gate threshold must be between 0 and 10000 basis points")
  require(
    reproductionCommands.forall(_.trim.nonEmpty),
    "reproduction commands must be non-empty")
  require(
    reproductionCommands.distinct.size == reproductionCommands.size,
    "reproduction commands must be unique")
}

private[sql] case class ShuffleRecoveryOpportunityReport(
    corpus: ShuffleRecoveryCorpusDefinition,
    rules: Seq[ShuffleRecoveryRuleSummary],
    selectedAdditionalRule: Option[(String, Int)],
    failurePointRows: Seq[ShuffleRecoveryFailurePointRow],
    failurePoints: Seq[ShuffleRecoveryFailurePointSummary],
    valueGate: ShuffleRecoveryValueGate,
    failedExecutions: Seq[(Long, String)]) {

  def toMarkdown: String = {
    val out = new StringBuilder
    line(out, "# Shuffle recovery Phase 0-A opportunity report")
    line(out, "")
    line(out, s"- Frozen Spark baseline: `${corpus.baselineSha}`")
    line(out, s"- Corpus: `${corpus.name}`")
    line(out, s"- Scale: `${corpus.scale}`")
    line(out, s"- Failure distribution: `${corpus.failureDistributionVersion}`")
    val threshold = f"${corpus.gateThresholdBasisPoints / 100.0}%.1f%%"
    line(
      out,
      s"- Value gate: `$threshold` of completed shuffle-map executor run time " +
        s"under `${corpus.gateRuleSetName}` v${corpus.gateRuleSetVersion}")
    val gateRatio = ShuffleRecoveryRatio(
      valueGate.reusableExecutorRunTimeMs,
      valueGate.completedExecutorRunTimeMs)
    line(out, s"- Gate result: **${valueGate.renderedResult}** (${gateRatio.render})")
    line(out, "")
    appendCorpus(out)
    appendReproduction(out)
    appendScopeCurve(out)
    appendFailurePoints(out)
    appendMissReasons(out)
    appendAccounting(out)
    appendLimitations(out)
    out.toString()
  }

  private def appendCorpus(out: StringBuilder): Unit = {
    line(out, "## Corpus")
    line(out, "")
    if (corpus.queries.isEmpty) {
      line(out, "No SQL corpus queries were declared.")
    } else {
      line(out, "| Family | Query | AQE |")
      line(out, "|---|---|---|")
      corpus.queries
        .sortBy(query => (query.family, query.name, query.aqeEnabled))
        .foreach { query =>
          val aqe = if (query.aqeEnabled) "on" else "off"
          line(out, s"| ${query.family} | ${query.name} | $aqe |")
        }
    }
    line(out, "")
    line(out, "Relevant Spark configs:")
    line(out, "")
    corpus.sparkConfigs.sortBy(_._1).foreach { case (key, value) =>
      line(out, s"- `$key=$value`")
    }
    line(out, "")
  }

  private def appendReproduction(out: StringBuilder): Unit = {
    line(out, "## Reproduction")
    line(out, "")
    if (corpus.reproductionCommands.isEmpty) {
      line(out, "No reproduction command was declared for this synthetic unit report.")
      line(out, "")
    } else {
      corpus.reproductionCommands.foreach { command =>
        line(out, "```bash")
        command.linesIterator.foreach(line(out, _))
        line(out, "```")
        line(out, "")
      }
    }
  }

  private def appendScopeCurve(out: StringBuilder): Unit = {
    line(out, "## Scope curve and correlation coverage")
    line(out, "")
    line(
      out,
      "| Rule set | Role | Exchanges | Correlated | Unweighted | Excluded | " +
        "Coverage | Eligible/count | Eligible/bytes | Eligible/task time |")
    line(
      out,
      "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    rules.foreach { rule =>
      val selected = selectedAdditionalRule.contains((rule.name, rule.version))
      val suffix = if (selected) " **(largest additional blocker)**" else ""
      line(
        out,
        s"| ${rule.name}$suffix v${rule.version} | ${rule.curveRole} | " +
          s"${rule.observedExchangeCount} | ${rule.weightedExchangeCount} | " +
          s"${rule.unweightedExchangeCount} | ${rule.excludedExchangeCount} | " +
          s"${rule.correlationRatio.render} | ${rule.countRatio.render} | " +
          s"${rule.byteRatio.render} | ${rule.taskTimeRatio.render} |")
    }
    if (selectedAdditionalRule.isEmpty) {
      line(out, "")
      line(
        out,
        "No preregistered additional candidate unlocked measurable opportunity " +
          "beyond the Window row.")
    }
    line(out, "")
    line(
      out,
      "Observed source-token categories remain separate from counterfactual capability.")
    rules.foreach { rule =>
      val rendered = if (rule.sourceTokenCounts.isEmpty) {
        "none"
      } else {
        rule.sourceTokenCounts
          .map { case (token, count) => s"$token=$count" }
          .mkString(", ")
      }
      line(out, s"- `${rule.name}` v${rule.version}: $rendered")
    }
    line(out, "")
  }

  private def appendFailurePoints(out: StringBuilder): Unit = {
    line(out, "## Failure-point opportunity")
    line(out, "")
    line(
      out,
      "| Point | Applicable executions | All exchanges | Completed | " +
        "Eligible completed | Avoidable bytes | Avoidable task time |")
    line(out, "|---|---:|---:|---:|---:|---:|---:|")
    failurePoints.foreach { point =>
      line(
        out,
        s"| ${point.point} | ${point.applicableExecutions} | " +
          s"${point.allExchangeCount} | ${point.completedExchangeCount} | " +
          s"${point.eligibleCompletedExchangeCount} | ${point.byteRatio.render} | " +
          s"${point.taskTimeRatio.render} |")
    }
    line(out, "")
    line(
      out,
      "`Avoidable` remains a projection until Phase 0-B proves real adoption.")
    line(out, "")
  }

  private def appendMissReasons(out: StringBuilder): Unit = {
    line(out, "## Top miss reasons")
    line(out, "")
    rules.foreach { rule =>
      line(out, s"### ${rule.name} v${rule.version}")
      line(out, "")
      if (rule.misses.isEmpty) {
        line(out, "No ineligible non-excluded exchanges.")
      } else {
        line(out, "| Root miss | Count | Weighted task time (ms) |")
        line(out, "|---|---:|---:|")
        rule.misses.foreach { miss =>
          line(
            out,
            s"| ${miss.reason} | ${miss.exchangeCount} | " +
              s"${miss.executorRunTimeMs} |")
        }
      }
      line(out, "")
    }
  }

  private def appendAccounting(out: StringBuilder): Unit = {
    line(out, "## Unweighted / excluded accounting")
    line(out, "")
    rules.foreach { rule =>
      val reasons = if (rule.accountingReasons.isEmpty) {
        "none"
      } else {
        rule.accountingReasons
          .map { case (reason, count) => s"$reason=$count" }
          .mkString(", ")
      }
      line(
        out,
        s"- `${rule.name}`: ${rule.unweightedExchangeCount} unweighted, " +
          s"${rule.excludedExchangeCount} excluded; reasons: $reasons.")
    }
    if (failedExecutions.nonEmpty) {
      line(out, "")
      line(
        out,
        "Failed/cancelled SQL executions are not represented as completed opportunity:")
      line(out, "")
      failedExecutions.sortBy(_._1).foreach { case (id, error) =>
        line(out, s"- execution `$id`: `$error`")
      }
    }
    line(out, "")
  }

  private def appendLimitations(out: StringBuilder): Unit = {
    line(out, "## Measurement semantics and limitations")
    line(out, "")
    line(
      out,
      s"- Corpus scale is `${corpus.scale}` and uses deterministic synthetic rows for " +
        "reproducibility. Task-time ratios include startup/scheduling noise and are feasibility " +
        "evidence, not benchmark-scale performance estimates.")
    line(
      out,
      "- Runtime weight is successful shuffle-map task executor run time in milliseconds, " +
        "summed once per logical map-output winner.")
    line(
      out,
      "- Shuffle bytes are accepted-winner " +
        "`TaskMetrics.shuffleWriteMetrics.bytesWritten`.")
    line(
      out,
      "- A later stage attempt replaces an earlier winner for the same map partition; " +
        "same-attempt duplicate successes are counted once.")
    line(
      out,
      "- Failed task work that does not survive into the completed shuffle is not " +
        "reusable successful work.")
    line(
      out,
      "- A successful stage without complete map-partition winner coverage is unweighted.")
    line(out, "- Zero-byte and zero-task-time denominators render as N/A.")
    line(
      out,
      "- Any unweighted gate-rule exchange makes the value gate N/A instead of " +
        "allowing a partial denominator to pass.")
    line(
      out,
      "- Counterfactual exact source identity is an opportunity assumption, not " +
        "observed support.")
    line(
      out,
      "- Executor run time is a coarse work proxy and may include executor-side " +
        "upstream fetch time; it is not CPU time.")
  }

  private def line(out: StringBuilder, value: String): Unit = {
    out.append(value).append('\n')
  }
}

private[sql] object ShuffleRecoveryOpportunityReportBuilder {
  import ShuffleRecoveryWeightDisposition._

  val FrozenBaselineSha = "2a7cfea06ba135cf0ddc62902eb0daf5a835c672"
  val FailureDistributionVersion = "equal-three-points-v1"
  val GateThresholdBasisPoints = 2000L

  private val AfterFirstEligible = "AFTER_FIRST_ELIGIBLE_COMPLETES"
  private val AfterMultipleUpstream = "AFTER_MULTIPLE_UPSTREAM_SHUFFLES_COMPLETE"
  private val BeforeMostExpensive = "BEFORE_MOST_EXPENSIVE_SHUFFLE_COMPLETES"
  private val failurePointOrder = Seq(
    AfterFirstEligible,
    AfterMultipleUpstream,
    BeforeMostExpensive)

  def build(
      snapshot: ShuffleRecoveryOpportunityStudySnapshot,
      ruleSets: Seq[ShuffleRecoveryStudyRuleSet],
      corpus: ShuffleRecoveryCorpusDefinition): ShuffleRecoveryOpportunityReport = {
    snapshot.validateAccounting(ruleSets)
    require(
      corpus.baselineSha == FrozenBaselineSha,
      s"opportunity corpus must use frozen baseline $FrozenBaselineSha")
    require(
      corpus.failureDistributionVersion == FailureDistributionVersion,
      s"unsupported failure distribution ${corpus.failureDistributionVersion}")
    require(
      corpus.gateThresholdBasisPoints == GateThresholdBasisPoints,
      s"value gate must remain at $GateThresholdBasisPoints basis points")
    val preregisteredGate = ShuffleRecoveryStudyRuleSets.exactSourceCounterfactual
    require(
      corpus.gateRuleSetName == preregisteredGate.rules.name &&
        corpus.gateRuleSetVersion == preregisteredGate.rules.version,
      "value gate must use the preregistered exact-source counterfactual rule")
    require(
      ruleSets.map(rule => (rule.rules.name, rule.rules.version)).distinct.size == ruleSets.size,
      "scope-curve rule-set name/version pairs must be unique")
    val gateRule = ruleSets.find { rule =>
      rule.rules.name == corpus.gateRuleSetName &&
        rule.rules.version == corpus.gateRuleSetVersion
    }
    require(gateRule.contains(preregisteredGate),
      "value gate must use the canonical preregistered rule definition")

    val summaries = ruleSets.map { ruleSet =>
      val records = matchingRecords(snapshot, ruleSet.rules.name, ruleSet.rules.version)
      summarizeRule(
        ruleSet.rules.name,
        ruleSet.rules.version,
        ruleSet.curveRole,
        records)
    }
    val selectedAdditional = selectAdditionalRule(summaries)
    val gateRecords = matchingRecords(
      snapshot,
      corpus.gateRuleSetName,
      corpus.gateRuleSetVersion)
    val rows = failureRows(gateRecords, snapshot.completedExecutionIds)
    val failureSummaries = summarizeFailurePoints(rows)
    val gate = buildGate(corpus, gateRecords, rows)

    ShuffleRecoveryOpportunityReport(
      corpus,
      summaries,
      selectedAdditional,
      rows,
      failureSummaries,
      gate,
      snapshot.failedExecutions)
  }

  private def matchingRecords(
      snapshot: ShuffleRecoveryOpportunityStudySnapshot,
      name: String,
      version: Int): Seq[ShuffleRecoveryWeightedObservation] = {
    snapshot.records.filter { record =>
      record.classification.ruleSetName == name &&
        record.classification.ruleSetVersion == version
    }
  }

  private def summarizeRule(
      name: String,
      version: Int,
      role: String,
      records: Seq[ShuffleRecoveryWeightedObservation]): ShuffleRecoveryRuleSummary = {
    val nonExcluded = records.filterNot(_.disposition == Excluded)
    val weighted = records.filter(_.disposition == Weighted)
    val eligible = nonExcluded.filter(_.classification.eligible)
    val eligibleWeighted = weighted.filter(_.classification.eligible)
    val totalBytes = checkedSum(
      weighted.flatMap(_.shuffleWriteBytes),
      s"$name bytes")
    val eligibleBytes = checkedSum(
      eligibleWeighted.flatMap(_.shuffleWriteBytes),
      s"$name eligible bytes")
    val totalTime = checkedSum(
      weighted.flatMap(_.executorRunTimeMs),
      s"$name task time")
    val eligibleTime = checkedSum(
      eligibleWeighted.flatMap(_.executorRunTimeMs),
      s"$name eligible time")
    val sourceTokens = nonExcluded
      .groupBy(_.classification.sourceTokenAvailability.code)
      .toSeq
      .map { case (code, values) => (code, values.size.toLong) }
      .sortBy(_._1)
    val accountingReasons = records
      .flatMap(_.accountingReason)
      .groupBy(identity)
      .toSeq
      .map { case (reason, values) => (reason, values.size.toLong) }
      .sortBy(_._1)
    val misses = nonExcluded
      .filterNot(_.classification.eligible)
      .groupBy { record =>
        record.classification.rootMissReason.map(_.code).getOrElse("UNKNOWN_MISS")
      }
      .toSeq
      .map { case (reason, values) =>
        val runTime = checkedSum(
          values.filter(_.disposition == Weighted).flatMap(_.executorRunTimeMs),
          s"$name/$reason")
        ShuffleRecoveryMissWeight(reason, values.size.toLong, runTime)
      }
      .sortBy(miss => (-miss.executorRunTimeMs, -miss.exchangeCount, miss.reason))

    ShuffleRecoveryRuleSummary(
      name,
      version,
      role,
      records.size.toLong,
      records.count(_.disposition == Excluded).toLong,
      records.count(_.disposition == Unweighted).toLong,
      weighted.size.toLong,
      eligible.size.toLong,
      totalBytes,
      eligibleBytes,
      totalTime,
      eligibleTime,
      ShuffleRecoveryRatio(weighted.size.toLong, nonExcluded.size.toLong),
      ShuffleRecoveryRatio(eligible.size.toLong, nonExcluded.size.toLong),
      ShuffleRecoveryRatio(eligibleBytes, totalBytes),
      ShuffleRecoveryRatio(eligibleTime, totalTime),
      sourceTokens,
      accountingReasons,
      misses)
  }

  private def selectAdditionalRule(
      summaries: Seq[ShuffleRecoveryRuleSummary]): Option[(String, Int)] = {
    val window = summaries.find(_.name == "exact-source-plus-dpp-window-v1")
    val baseTime = window.map(_.eligibleExecutorRunTimeMs).getOrElse(0L)
    val baseBytes = window.map(_.eligibleShuffleWriteBytes).getOrElse(0L)
    val baseCount = window.map(_.eligibleExchangeCount).getOrElse(0L)
    val candidates = summaries
      .filter(_.curveRole == "additional-candidate")
      .map { summary =>
        val marginalTime =
          math.max(0L, summary.eligibleExecutorRunTimeMs - baseTime)
        val marginalBytes =
          math.max(0L, summary.eligibleShuffleWriteBytes - baseBytes)
        val marginalCount =
          math.max(0L, summary.eligibleExchangeCount - baseCount)
        (marginalTime, marginalBytes, marginalCount, summary.name, summary.version)
      }
      .sortBy { case (time, bytes, count, name, version) =>
        (-time, -bytes, -count, name, version)
      }
    candidates.headOption.collect {
      case (time, bytes, count, name, version)
          if time > 0L || bytes > 0L || count > 0L =>
        (name, version)
    }
  }

  private def failureRows(
      records: Seq[ShuffleRecoveryWeightedObservation],
      completedExecutionIds: Seq[Long]): Seq[ShuffleRecoveryFailurePointRow] = {
    val byExecution = records.groupBy(_.classification.executionId)
    completedExecutionIds.distinct.sorted.flatMap { rawExecutionId =>
      val executionId = f"query-$rawExecutionId%020d"
      val executionRecords = byExecution.getOrElse(executionId, Nil)
      val physical = executionRecords
        .filter(_.disposition == Weighted)
        .sortBy { record =>
          (
            record.completionOrder.getOrElse(Long.MaxValue),
            record.classification.exchangeOrdinal)
        }
      val allCount = executionRecords.count(_.disposition != Excluded).toLong
      val firstEligible = physical
        .find(_.classification.eligible)
        .flatMap(_.completionOrder)
      val secondPhysical = if (physical.size >= 2) {
        physical(1).completionOrder
      } else {
        None
      }
      val expensive = physical
        .sortBy { record =>
          (
            -record.executorRunTimeMs.getOrElse(0L),
            -record.shuffleWriteBytes.getOrElse(0L),
            record.completionOrder.getOrElse(Long.MaxValue))
        }
        .headOption
        .flatMap(_.completionOrder)

      Seq(
        failureRow(
          executionId,
          AfterFirstEligible,
          allCount,
          physical,
          firstEligible,
          inclusive = true),
        failureRow(
          executionId,
          AfterMultipleUpstream,
          allCount,
          physical,
          secondPhysical,
          inclusive = true),
        failureRow(
          executionId,
          BeforeMostExpensive,
          allCount,
          physical,
          expensive,
          inclusive = false))
    }
  }

  private def failureRow(
      executionId: String,
      point: String,
      allExchangeCount: Long,
      physical: Seq[ShuffleRecoveryWeightedObservation],
      cutoff: Option[Long],
      inclusive: Boolean): ShuffleRecoveryFailurePointRow = {
    cutoff match {
      case None =>
        ShuffleRecoveryFailurePointRow(
          executionId,
          point,
          applicable = false,
          allExchangeCount,
          0L,
          0L,
          0L,
          0L,
          0L,
          0L)
      case Some(value) =>
        val completed = physical.filter { record =>
          record.completionOrder.exists { order =>
            if (inclusive) order <= value else order < value
          }
        }
        val eligible = completed.filter(_.classification.eligible)
        ShuffleRecoveryFailurePointRow(
          executionId,
          point,
          applicable = true,
          allExchangeCount,
          completed.size.toLong,
          eligible.size.toLong,
          checkedSum(
            completed.flatMap(_.shuffleWriteBytes),
            s"$executionId/$point bytes"),
          checkedSum(
            eligible.flatMap(_.shuffleWriteBytes),
            s"$executionId/$point eligible bytes"),
          checkedSum(
            completed.flatMap(_.executorRunTimeMs),
            s"$executionId/$point time"),
          checkedSum(
            eligible.flatMap(_.executorRunTimeMs),
            s"$executionId/$point eligible time"))
    }
  }

  private def summarizeFailurePoints(
      rows: Seq[ShuffleRecoveryFailurePointRow]): Seq[ShuffleRecoveryFailurePointSummary] = {
    val byPoint = rows.groupBy(_.point)
    failurePointOrder.map { point =>
      val applicable = byPoint.getOrElse(point, Nil).filter(_.applicable)
      val all = checkedSum(
        applicable.map(_.allExchangeCount),
        s"$point all exchanges")
      val completed = checkedSum(
        applicable.map(_.completedExchangeCount),
        s"$point completed")
      val eligible = checkedSum(
        applicable.map(_.eligibleCompletedExchangeCount),
        s"$point eligible")
      val completedBytes = checkedSum(
        applicable.map(_.completedShuffleWriteBytes),
        s"$point bytes")
      val avoidableBytes = checkedSum(
        applicable.map(_.avoidableShuffleWriteBytes),
        s"$point avoidable bytes")
      val completedTime = checkedSum(
        applicable.map(_.completedExecutorRunTimeMs),
        s"$point time")
      val avoidableTime = checkedSum(
        applicable.map(_.avoidableExecutorRunTimeMs),
        s"$point avoidable time")
      ShuffleRecoveryFailurePointSummary(
        point,
        applicable.size.toLong,
        all,
        completed,
        eligible,
        completedBytes,
        avoidableBytes,
        completedTime,
        avoidableTime,
        ShuffleRecoveryRatio(avoidableBytes, completedBytes),
        ShuffleRecoveryRatio(avoidableTime, completedTime))
    }
  }

  private def buildGate(
      corpus: ShuffleRecoveryCorpusDefinition,
      gateRecords: Seq[ShuffleRecoveryWeightedObservation],
      rows: Seq[ShuffleRecoveryFailurePointRow]): ShuffleRecoveryValueGate = {
    val applicable = rows.filter(_.applicable)
    val completed = checkedSum(
      applicable.map(_.completedExecutorRunTimeMs),
      "value-gate completed task time")
    val reusable = checkedSum(
      applicable.map(_.avoidableExecutorRunTimeMs),
      "value-gate reusable task time")
    val hasUnweighted = gateRecords.exists(_.disposition == Unweighted)
    val result = if (hasUnweighted) {
      None
    } else {
      ShuffleRecoveryRatio(reusable, completed)
        .basisPoints
        .map(_ >= corpus.gateThresholdBasisPoints)
    }
    ShuffleRecoveryValueGate(
      corpus.gateRuleSetName,
      corpus.gateRuleSetVersion,
      corpus.failureDistributionVersion,
      corpus.gateThresholdBasisPoints,
      reusable,
      completed,
      result)
  }

  private def checkedSum(values: Iterable[Long], label: String): Long = {
    values.foldLeft(0L) { (total, value) =>
      require(value >= 0L, s"negative value in $label")
      try {
        Math.addExact(total, value)
      } catch {
        case _: ArithmeticException =>
          throw new IllegalArgumentException(
            s"overflow while aggregating $label")
      }
    }
  }
}
