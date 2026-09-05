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

/** A ratio that deliberately renders an empty denominator as N/A instead of a fabricated zero. */
private[sql] case class ShuffleRecoveryRatio(numerator: Long, denominator: Long) {
  require(numerator >= 0L, "ratio numerator must be non-negative")
  require(denominator >= 0L, "ratio denominator must be non-negative")
  require(numerator <= denominator,
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
    gateThresholdBasisPoints: Long) {

  require(name.nonEmpty, "corpus name must be non-empty")
  require(scale.nonEmpty, "corpus scale must be non-empty")
  require(baselineSha.matches("[0-9a-f]{40}"), "baseline SHA must be a 40-character hex SHA")
  require(gateRuleSetName.nonEmpty, "gate rule-set name must be non-empty")
  require(gateRuleSetVersion > 0, "gate rule-set version must be positive")
  require(gateThresholdBasisPoints >= 0L && gateThresholdBasisPoints <= 10000L,
    "gate threshold must be between 0 and 10000 basis points")
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
    out.append("# Shuffle recovery Phase 0-A opportunity report\n\n")
    out.append(s"- Frozen Spark baseline: `${corpus.baselineSha}`\n")
    out.append(s"- Corpus: `${corpus.name}`\n")
    out.append(s"- Scale: `${corpus.scale}`\n")
    out.append(s"- Failure distribution: `${corpus.failureDistributionVersion}`\n")
    out.append(f"- Value gate: `${corpus.gateThresholdBasisPoints / 100.0}%.1f%%` of completed " +
      s"shuffle-map executor run time under `${corpus.gateRuleSetName}` v${corpus.gateRuleSetVersion}\n")
    val gateRatio = ShuffleRecoveryRatio(
      valueGate.reusableExecutorRunTimeMs, valueGate.completedExecutorRunTimeMs)
    out.append(s"- Gate result: **${valueGate.renderedResult}** (${gateRatio.render})\n\n")

    out.append("## Corpus\n\n")
    if (corpus.queries.isEmpty) {
      out.append("No SQL corpus queries were declared.\n")
    } else {
      out.append("| Family | Query | AQE |\n|---|---|---|\n")
      corpus.queries.sortBy(q => (q.family, q.name, q.aqeEnabled)).foreach { query =>
        out.append(s"| ${query.family} | ${query.name} | " +
          s"${if (query.aqeEnabled) "on" else "off"} |\n")
      }
    }
    out.append("\nRelevant Spark configs:\n\n")
    corpus.sparkConfigs.sortBy(_._1).foreach { case (key, value) =>
      out.append(s"- `$key=$value`\n")
    }

    out.append("\n## Scope curve and correlation coverage\n\n")
    out.append("| Rule set | Role | Exchanges | Correlated | Unweighted | Excluded | Coverage | " +
      "Eligible/count | Eligible/bytes | Eligible/task time |\n")
    out.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
    rules.foreach { rule =>
      val selected = selectedAdditionalRule.contains((rule.name, rule.version))
      val displayName = if (selected) s"${rule.name} **(largest additional blocker)**" else rule.name
      out.append(s"| $displayName v${rule.version} | ${rule.curveRole} | " +
        s"${rule.observedExchangeCount} | ${rule.weightedExchangeCount} | " +
        s"${rule.unweightedExchangeCount} | ${rule.excludedExchangeCount} | " +
        s"${rule.correlationRatio.render} | ${rule.countRatio.render} | " +
        s"${rule.byteRatio.render} | ${rule.taskTimeRatio.render} |\n")
    }
    if (selectedAdditionalRule.isEmpty) {
      out.append("\nNo preregistered additional candidate unlocked measurable count, byte, or task-time " +
        "opportunity beyond the Window row.\n")
    }

    out.append("\nObserved source-token categories remain separate from counterfactual capability.\n")
    rules.foreach { rule =>
      val rendered = if (rule.sourceTokenCounts.isEmpty) "none" else {
        rule.sourceTokenCounts.map { case (token, count) => s"$token=$count" }.mkString(", ")
      }
      out.append(s"- `${rule.name}` v${rule.version}: $rendered\n")
    }

    out.append("\n## Failure-point opportunity\n\n")
    out.append("| Point | Applicable executions | All exchanges | Completed | Eligible completed | " +
      "Avoidable bytes | Avoidable task time |\n")
    out.append("|---|---:|---:|---:|---:|---:|---:|\n")
    failurePoints.foreach { point =>
      out.append(s"| ${point.point} | ${point.applicableExecutions} | ${point.allExchangeCount} | " +
        s"${point.completedExchangeCount} | ${point.eligibleCompletedExchangeCount} | " +
        s"${point.byteRatio.render} | ${point.taskTimeRatio.render} |\n")
    }
    out.append("\n`Avoidable` remains a projection until Phase 0-B proves real adoption.\n")

    out.append("\n## Top miss reasons\n\n")
    rules.foreach { rule =>
      out.append(s"### ${rule.name} v${rule.version}\n\n")
      if (rule.misses.isEmpty) {
        out.append("No ineligible non-excluded exchanges.\n\n")
      } else {
        out.append("| Root miss | Count | Weighted task time (ms) |\n|---|---:|---:|\n")
        rule.misses.foreach { miss =>
          out.append(s"| ${miss.reason} | ${miss.exchangeCount} | ${miss.executorRunTimeMs} |\n")
        }
        out.append("\n")
      }
    }

    out.append("## Unweighted / excluded accounting\n\n")
    rules.foreach { rule =>
      val reasons = if (rule.accountingReasons.isEmpty) "none" else {
        rule.accountingReasons.map { case (reason, count) => s"$reason=$count" }.mkString(", ")
      }
      out.append(s"- `${rule.name}`: ${rule.unweightedExchangeCount} unweighted, " +
        s"${rule.excludedExchangeCount} excluded; reasons: $reasons.\n")
    }
    if (failedExecutions.nonEmpty) {
      out.append("\nFailed/cancelled SQL executions are not represented as completed opportunity:\n\n")
      failedExecutions.sortBy(_._1).foreach { case (id, error) =>
        out.append(s"- execution `$id`: `$error`\n")
      }
    }

    out.append("\n## Measurement semantics and limitations\n\n")
    out.append("- Runtime weight is successful shuffle-map task **executor run time in milliseconds**, " +
      "summed once per logical map-output winner that constitutes the completed shuffle.\n")
    out.append("- Shuffle bytes are accepted-winner `TaskMetrics.shuffleWriteMetrics.bytesWritten`.\n")
    out.append("- A later stage attempt replaces an earlier winner for the same map partition; " +
      "same-attempt duplicate successes are not charged twice.\n")
    out.append("- Failed task work and failed stage work that does not contribute a surviving map output " +
      "is not counted as reusable successful work.\n")
    out.append("- A successful stage without complete map-partition winner coverage is explicitly unweighted.\n")
    out.append("- Zero-byte and zero-task-time denominators render as N/A.\n")
    out.append("- Counterfactual exact source identity is an opportunity assumption, not observed support.\n")
    out.append("- Executor run time is a coarse work proxy and may include executor-side upstream fetch time; " +
      "it is intentionally not presented as CPU time.\n")
    out.toString()
  }
}

private[sql] object ShuffleRecoveryOpportunityReportBuilder {
  import ShuffleRecoveryWeightDisposition._

  val FrozenBaselineSha = "2a7cfea06ba135cf0ddc62902eb0daf5a835c672"
  val FailureDistributionVersion = "equal-three-points-v1"
  val GateThresholdBasisPoints = 2000L

  private val afterFirstEligible = "AFTER_FIRST_ELIGIBLE_COMPLETES"
  private val afterMultipleUpstream = "AFTER_MULTIPLE_UPSTREAM_SHUFFLES_COMPLETE"
  private val beforeMostExpensive = "BEFORE_MOST_EXPENSIVE_SHUFFLE_COMPLETES"
  private val failurePointOrder = Seq(afterFirstEligible, afterMultipleUpstream, beforeMostExpensive)

  def build(
      snapshot: ShuffleRecoveryOpportunityStudySnapshot,
      ruleSets: Seq[ShuffleRecoveryStudyRuleSet],
      corpus: ShuffleRecoveryCorpusDefinition): ShuffleRecoveryOpportunityReport = {
    snapshot.validateAccounting(ruleSets)
    require(corpus.baselineSha == FrozenBaselineSha,
      s"opportunity corpus must use frozen baseline $FrozenBaselineSha")
    require(corpus.failureDistributionVersion == FailureDistributionVersion,
      s"unsupported failure distribution ${corpus.failureDistributionVersion}")
    require(corpus.gateThresholdBasisPoints == GateThresholdBasisPoints,
      s"value gate must remain preregistered at $GateThresholdBasisPoints basis points")
    val gateRuleExists = ruleSets.exists { rule =>
      rule.rules.name == corpus.gateRuleSetName && rule.rules.version == corpus.gateRuleSetVersion
    }
    require(gateRuleExists, "gate rule set is not part of the declared scope curve")

    val summaries = ruleSets.map { ruleSet =>
      val records = snapshot.records.filter { record =>
        record.classification.ruleSetName == ruleSet.rules.name &&
          record.classification.ruleSetVersion == ruleSet.rules.version
      }
      summarizeRule(ruleSet.rules.name, ruleSet.rules.version, ruleSet.curveRole, records)
    }
    val selectedAdditional = selectAdditionalRule(summaries)
    val gateRecords = snapshot.records.filter { record =>
      record.classification.ruleSetName == corpus.gateRuleSetName &&
        record.classification.ruleSetVersion == corpus.gateRuleSetVersion
    }
    val rows = failureRows(gateRecords, snapshot.completedExecutionIds)
    val failureSummaries = summarizeFailurePoints(rows)
    val gate = buildGate(corpus, rows)

    ShuffleRecoveryOpportunityReport(
      corpus,
      summaries,
      selectedAdditional,
      rows,
      failureSummaries,
      gate,
      snapshot.failedExecutions)
  }

  private def summarizeRule(
      name: String,
      version: Int,
      role: String,
      records: Seq[ShuffleRecoveryWeightedObservation]): ShuffleRecoveryRuleSummary = {
    val nonExcluded = records.filterNot(_.disposition == Excluded)
    val weighted = records.filter(_.disposition == Weighted)
    val eligibleCount = nonExcluded.count(_.classification.eligible).toLong
    val totalBytes = checkedSum(weighted.flatMap(_.shuffleWriteBytes), s"$name bytes")
    val eligibleBytes = checkedSum(
      weighted.filter(_.classification.eligible).flatMap(_.shuffleWriteBytes), s"$name eligible bytes")
    val totalTime = checkedSum(weighted.flatMap(_.executorRunTimeMs), s"$name task time")
    val eligibleTime = checkedSum(
      weighted.filter(_.classification.eligible).flatMap(_.executorRunTimeMs), s"$name eligible time")
    val sourceTokens = nonExcluded.groupBy(_.classification.sourceTokenAvailability.code)
      .toSeq.map { case (code, values) => (code, values.size.toLong) }.sortBy(_._1)
    val accountingReasons = records.flatMap(_.accountingReason).groupBy(identity).toSeq
      .map { case (reason, values) => (reason, values.size.toLong) }.sortBy(_._1)
    val missGroups = nonExcluded.filterNot(_.classification.eligible).groupBy { record =>
      record.classification.rootMissReason.map(_.code).getOrElse("UNKNOWN_MISS")
    }
    val misses = missGroups.toSeq.map { case (reason, values) =>
      val time = checkedSum(
        values.filter(_.disposition == Weighted).flatMap(_.executorRunTimeMs), s"$name/$reason")
      ShuffleRecoveryMissWeight(reason, values.size.toLong, time)
    }.sortBy(miss => (-miss.executorRunTimeMs, -miss.exchangeCount, miss.reason))

    ShuffleRecoveryRuleSummary(
      name = name,
      version = version,
      curveRole = role,
      observedExchangeCount = records.size.toLong,
      excludedExchangeCount = records.count(_.disposition == Excluded).toLong,
      unweightedExchangeCount = records.count(_.disposition == Unweighted).toLong,
      weightedExchangeCount = weighted.size.toLong,
      eligibleExchangeCount = eligibleCount,
      weightedShuffleWriteBytes = totalBytes,
      eligibleShuffleWriteBytes = eligibleBytes,
      weightedExecutorRunTimeMs = totalTime,
      eligibleExecutorRunTimeMs = eligibleTime,
      correlationRatio = ShuffleRecoveryRatio(weighted.size.toLong, nonExcluded.size.toLong),
      countRatio = ShuffleRecoveryRatio(eligibleCount, nonExcluded.size.toLong),
      byteRatio = ShuffleRecoveryRatio(eligibleBytes, totalBytes),
      taskTimeRatio = ShuffleRecoveryRatio(eligibleTime, totalTime),
      sourceTokenCounts = sourceTokens,
      accountingReasons = accountingReasons,
      misses = misses)
  }

  private def selectAdditionalRule(
      summaries: Seq[ShuffleRecoveryRuleSummary]): Option[(String, Int)] = {
    val window = summaries.find(_.name == "exact-source-plus-dpp-window-v1")
    val baseTime = window.map(_.eligibleExecutorRunTimeMs).getOrElse(0L)
    val baseBytes = window.map(_.eligibleShuffleWriteBytes).getOrElse(0L)
    val baseCount = window.map(_.eligibleExchangeCount).getOrElse(0L)
    val candidates = summaries.filter(_.curveRole == "additional-candidate").map { summary =>
      val marginal = (
        math.max(0L, summary.eligibleExecutorRunTimeMs - baseTime),
        math.max(0L, summary.eligibleShuffleWriteBytes - baseBytes),
        math.max(0L, summary.eligibleExchangeCount - baseCount))
      (marginal, summary.name, summary.version)
    }.sortBy { case ((time, bytes, count), name, version) =>
      (-time, -bytes, -count, name, version)
    }
    candidates.headOption.collect {
      case ((time, bytes, count), name, version) if time > 0L || bytes > 0L || count > 0L =>
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
      val physical = executionRecords.filter(_.disposition == Weighted)
        .sortBy(record => (record.completionOrder.getOrElse(Long.MaxValue),
          record.classification.exchangeOrdinal))
      val allCount = executionRecords.count(_.disposition != Excluded).toLong
      val firstEligibleCutoff = physical.find(_.classification.eligible).flatMap(_.completionOrder)
      val multipleCutoff = if (physical.size >= 2) physical(1).completionOrder else None
      val expensive = physical.sortBy { record =>
        (-record.executorRunTimeMs.getOrElse(0L),
          -record.shuffleWriteBytes.getOrElse(0L),
          record.completionOrder.getOrElse(Long.MaxValue))
      }.headOption
      val beforeExpensiveCutoff = expensive.flatMap(_.completionOrder)

      Seq(
        failureRow(executionId, afterFirstEligible, allCount, physical,
          firstEligibleCutoff, inclusive = true),
        failureRow(executionId, afterMultipleUpstream, allCount, physical,
          multipleCutoff, inclusive = true),
        failureRow(executionId, beforeMostExpensive, allCount, physical,
          beforeExpensiveCutoff, inclusive = false))
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
      case None => ShuffleRecoveryFailurePointRow(
        executionId, point, applicable = false, allExchangeCount, 0L, 0L, 0L, 0L, 0L, 0L)
      case Some(value) =>
        val completed = physical.filter { record =>
          record.completionOrder.exists(order => if (inclusive) order <= value else order < value)
        }
        val eligible = completed.filter(_.classification.eligible)
        val completedBytes = checkedSum(completed.flatMap(_.shuffleWriteBytes), s"$executionId/$point bytes")
        val eligibleBytes = checkedSum(eligible.flatMap(_.shuffleWriteBytes), s"$executionId/$point eligible bytes")
        val completedTime = checkedSum(completed.flatMap(_.executorRunTimeMs), s"$executionId/$point time")
        val eligibleTime = checkedSum(eligible.flatMap(_.executorRunTimeMs), s"$executionId/$point eligible time")
        ShuffleRecoveryFailurePointRow(
          executionId,
          point,
          applicable = true,
          allExchangeCount,
          completed.size.toLong,
          eligible.size.toLong,
          completedBytes,
          eligibleBytes,
          completedTime,
          eligibleTime)
    }
  }

  private def summarizeFailurePoints(
      rows: Seq[ShuffleRecoveryFailurePointRow]): Seq[ShuffleRecoveryFailurePointSummary] = {
    val byPoint = rows.groupBy(_.point)
    failurePointOrder.map { point =>
      val applicable = byPoint.getOrElse(point, Nil).filter(_.applicable)
      val all = checkedSum(applicable.map(_.allExchangeCount), s"$point all exchanges")
      val completed = checkedSum(applicable.map(_.completedExchangeCount), s"$point completed")
      val eligible = checkedSum(applicable.map(_.eligibleCompletedExchangeCount), s"$point eligible")
      val completedBytes = checkedSum(applicable.map(_.completedShuffleWriteBytes), s"$point bytes")
      val avoidableBytes = checkedSum(applicable.map(_.avoidableShuffleWriteBytes), s"$point avoidable bytes")
      val completedTime = checkedSum(applicable.map(_.completedExecutorRunTimeMs), s"$point time")
      val avoidableTime = checkedSum(applicable.map(_.avoidableExecutorRunTimeMs), s"$point avoidable time")
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
      rows: Seq[ShuffleRecoveryFailurePointRow]): ShuffleRecoveryValueGate = {
    val applicable = rows.filter(_.applicable)
    val completed = checkedSum(
      applicable.map(_.completedExecutorRunTimeMs), "value-gate completed task time")
    val reusable = checkedSum(
      applicable.map(_.avoidableExecutorRunTimeMs), "value-gate reusable task time")
    val ratio = ShuffleRecoveryRatio(reusable, completed)
    ShuffleRecoveryValueGate(
      corpus.gateRuleSetName,
      corpus.gateRuleSetVersion,
      corpus.failureDistributionVersion,
      corpus.gateThresholdBasisPoints,
      reusable,
      completed,
      ratio.basisPoints.map(_ >= corpus.gateThresholdBasisPoints))
  }

  private def checkedSum(values: Iterable[Long], label: String): Long = {
    values.foldLeft(0L) { (total, value) =>
      require(value >= 0L, s"negative value in $label")
      try Math.addExact(total, value) catch {
        case _: ArithmeticException =>
          throw new IllegalArgumentException(s"overflow while aggregating $label")
      }
    }
  }
}
