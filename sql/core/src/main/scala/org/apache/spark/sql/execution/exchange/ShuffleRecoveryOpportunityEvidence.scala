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

private[sql] case class ShuffleRecoveryCorrelationCoverage(
    materializedExchangeCount: Long,
    correlatedExchangeCount: Long,
    materializedShuffleWriteBytes: Long,
    correlatedShuffleWriteBytes: Long,
    materializedExecutorRunTimeMs: Long,
    correlatedExecutorRunTimeMs: Long) {

  val countRatio: ShuffleRecoveryRatio =
    ShuffleRecoveryRatio(correlatedExchangeCount, materializedExchangeCount)
  val byteRatio: ShuffleRecoveryRatio =
    ShuffleRecoveryRatio(correlatedShuffleWriteBytes, materializedShuffleWriteBytes)
  val taskTimeRatio: ShuffleRecoveryRatio =
    ShuffleRecoveryRatio(correlatedExecutorRunTimeMs, materializedExecutorRunTimeMs)
}

private[sql] case class ShuffleRecoveryQueryOpportunity(
    executionId: Long,
    family: String,
    query: String,
    aqeEnabled: Boolean,
    completedExecutorRunTimeMs: Long,
    reusableExecutorRunTimeMs: Long) {

  val opportunity: ShuffleRecoveryRatio =
    ShuffleRecoveryRatio(reusableExecutorRunTimeMs, completedExecutorRunTimeMs)
}

private[sql] case class ShuffleRecoveryEvidenceFailurePoint(
    executionId: Long,
    point: String,
    applicable: Boolean,
    completedExchangeCount: Long,
    reusableExchangeCount: Long,
    completedShuffleWriteBytes: Long,
    reusableShuffleWriteBytes: Long,
    completedExecutorRunTimeMs: Long,
    reusableExecutorRunTimeMs: Long)

/**
 * Quality and sensitivity analysis for the manual Phase 0-A evidence campaign.
 *
 * Physical exchanges are nested inside queries and configurations, so the report deliberately does
 * not present exchange-count binomial confidence intervals. It reports query-level distributions,
 * concentration, and top-work sensitivity instead.
 */
private[sql] case class ShuffleRecoveryOpportunityEvidence(
    correlation: ShuffleRecoveryCorrelationCoverage,
    correlationGateThresholdBasisPoints: Long,
    failureDistributionVersion: String,
    failurePoints: Seq[ShuffleRecoveryEvidenceFailurePoint],
    finalValueGateThresholdBasisPoints: Long,
    queryOpportunity: Seq[ShuffleRecoveryQueryOpportunity],
    taskTimeOpportunity: ShuffleRecoveryRatio,
    queryWeightedOpportunityBasisPoints: Option[Long],
    withoutTopExchange: ShuffleRecoveryRatio,
    withoutTopFiveExchanges: ShuffleRecoveryRatio,
    topExchangeTaskTimeShare: ShuffleRecoveryRatio,
    topFiveExchangeTaskTimeShare: ShuffleRecoveryRatio,
    topQueryTaskTimeShare: ShuffleRecoveryRatio,
    topFiveQueryTaskTimeShare: ShuffleRecoveryRatio,
    zeroEligibleQueryCount: Long,
    materialQueryCount: Long,
    unweightedReasons: Seq[(String, Long)]) {

  def correlationGatePass: Option[Boolean] = correlation.taskTimeRatio.basisPoints.map {
    _ >= correlationGateThresholdBasisPoints
  }

  val finalValueGateRatio: ShuffleRecoveryRatio = {
    val applicable = failurePoints.filter(_.applicable)
    ShuffleRecoveryRatio(
      checkedSum(applicable.map(_.reusableExecutorRunTimeMs), "final reusable task time"),
      checkedSum(applicable.map(_.completedExecutorRunTimeMs), "final completed task time"))
  }

  def finalValueGateResult: Option[Boolean] = {
    if (!correlationGatePass.contains(true)) {
      None
    } else {
      finalValueGateRatio.basisPoints.map(_ >= finalValueGateThresholdBasisPoints)
    }
  }

  def toMarkdown: String = {
    val out = new StringBuilder
    line(out, "## Correlation quality gate")
    line(out, "")
    val correlationResult = renderResult(correlationGatePass)
    line(
      out,
      s"- Preregistered minimum: ${renderBasisPoints(correlationGateThresholdBasisPoints)} of " +
        "materially executed completed shuffle-map task time.")
    line(out, s"- Result: **$correlationResult**")
    line(out, s"- Exchange coverage: ${correlation.countRatio.render}")
    line(out, s"- Shuffle-write-byte coverage: ${correlation.byteRatio.render}")
    line(out, s"- Completed task-time coverage: ${correlation.taskTimeRatio.render}")
    line(out, "")
    if (unweightedReasons.nonEmpty) {
      line(out, "Unweighted observed exchanges:")
      line(out, "")
      unweightedReasons.foreach { case (reason, count) =>
        line(out, s"- `$reason`: $count")
      }
      line(out, "")
    }

    appendFailureDistribution(out)

    line(out, "## Sample quality and sensitivity")
    line(out, "")
    line(
      out,
      "Physical exchanges are not IID statistical samples; they are nested within queries, " +
        "AQE configurations, and benchmark families. No exchange-level binomial confidence " +
        "interval is reported.")
    line(out, "")
    line(out, s"- Aggregate task-time opportunity: ${taskTimeOpportunity.render}")
    line(
      out,
      s"- Equal-query-weight opportunity: ${renderOptionalBasisPoints(queryWeightedOpportunityBasisPoints)}")
    line(out, s"- Removing highest-task-time exchange: ${withoutTopExchange.render}")
    line(out, s"- Removing top five highest-task-time exchanges: ${withoutTopFiveExchanges.render}")
    line(out, s"- Largest exchange share of task time: ${topExchangeTaskTimeShare.render}")
    line(out, s"- Top five exchanges share of task time: ${topFiveExchangeTaskTimeShare.render}")
    line(out, s"- Largest query share of task time: ${topQueryTaskTimeShare.render}")
    line(out, s"- Top five queries share of task time: ${topFiveQueryTaskTimeShare.render}")
    line(out, s"- Queries with material work but zero eligible work: $zeroEligibleQueryCount")
    line(out, s"- Queries with material completed shuffle work: $materialQueryCount")
    line(out, "")

    appendSplit(out, "Benchmark-family split", _.family)
    appendSplit(out, "AQE split", query => if (query.aqeEnabled) "AQE on" else "AQE off")

    line(out, "### Per-query opportunity")
    line(out, "")
    line(out, "| Family | Query | AQE | Completed task time (ms) | Reusable task time (ms) | Opportunity |")
    line(out, "|---|---|---|---:|---:|---:|")
    queryOpportunity.sortBy(q => (q.family, q.query, q.aqeEnabled, q.executionId)).foreach { query =>
      val aqe = if (query.aqeEnabled) "on" else "off"
      line(
        out,
        s"| ${query.family} | ${query.query} | $aqe | ${query.completedExecutorRunTimeMs} | " +
          s"${query.reusableExecutorRunTimeMs} | ${query.opportunity.render} |")
    }
    line(out, "")
    out.toString()
  }

  private def appendFailureDistribution(out: StringBuilder): Unit = {
    line(out, "## Expanded failure-point opportunity and final value gate")
    line(out, "")
    line(out, s"- Failure distribution: `$failureDistributionVersion`")
    line(
      out,
      s"- Final value gate: **${renderResult(finalValueGateResult)}** " +
        s"(${finalValueGateRatio.render}; threshold " +
        s"${renderBasisPoints(finalValueGateThresholdBasisPoints)})")
    line(out, "")
    line(
      out,
      "The earlier three-point report remains a compatibility diagnostic. The final Phase 0-A " +
        "decision uses this preregistered four-point distribution.")
    line(out, "")
    line(
      out,
      "| Point | Applicable executions | Completed exchanges | Reusable exchanges | " +
        "Reusable bytes | Reusable task time |")
    line(out, "|---|---:|---:|---:|---:|---:|")
    ShuffleRecoveryOpportunityEvidence.FailurePointOrder.foreach { point =>
      val rows = failurePoints.filter(row => row.point == point && row.applicable)
      val completedCount = checkedSum(rows.map(_.completedExchangeCount), s"$point completed count")
      val reusableCount = checkedSum(rows.map(_.reusableExchangeCount), s"$point reusable count")
      val completedBytes = checkedSum(rows.map(_.completedShuffleWriteBytes), s"$point completed bytes")
      val reusableBytes = checkedSum(rows.map(_.reusableShuffleWriteBytes), s"$point reusable bytes")
      val completedTime = checkedSum(rows.map(_.completedExecutorRunTimeMs), s"$point completed time")
      val reusableTime = checkedSum(rows.map(_.reusableExecutorRunTimeMs), s"$point reusable time")
      line(
        out,
        s"| $point | ${rows.size} | $completedCount | $reusableCount | " +
          s"${ShuffleRecoveryRatio(reusableBytes, completedBytes).render} | " +
          s"${ShuffleRecoveryRatio(reusableTime, completedTime).render} |")
    }
    line(out, "")
    line(out, "`Reusable` remains a projection until the adoption mechanism is demonstrated.")
    line(out, "")
  }

  private def appendSplit(
      out: StringBuilder,
      title: String,
      key: ShuffleRecoveryQueryOpportunity => String): Unit = {
    line(out, s"### $title")
    line(out, "")
    line(out, "| Group | Completed task time (ms) | Reusable task time (ms) | Opportunity |")
    line(out, "|---|---:|---:|---:|")
    queryOpportunity.groupBy(key).toSeq.sortBy(_._1).foreach { case (name, queries) =>
      val completed = checkedSum(queries.map(_.completedExecutorRunTimeMs), s"$title/$name completed")
      val reusable = checkedSum(queries.map(_.reusableExecutorRunTimeMs), s"$title/$name reusable")
      line(out, s"| $name | $completed | $reusable | ${ShuffleRecoveryRatio(reusable, completed).render} |")
    }
    line(out, "")
  }

  private def checkedSum(values: Iterable[Long], label: String): Long = {
    try {
      values.foldLeft(0L)(Math.addExact)
    } catch {
      case _: ArithmeticException =>
        throw new IllegalArgumentException(s"overflow while summing $label")
    }
  }

  private def renderResult(result: Option[Boolean]): String = result match {
    case Some(true) => "PASS"
    case Some(false) => "FAIL"
    case None => "N/A"
  }

  private def renderOptionalBasisPoints(value: Option[Long]): String =
    value.map(renderBasisPoints).getOrElse("N/A")

  private def renderBasisPoints(value: Long): String = f"${value / 100.0}%.1f%%"

  private def line(out: StringBuilder, value: String): Unit = out.append(value).append('\n')
}

private[sql] object ShuffleRecoveryOpportunityEvidence {
  import ShuffleRecoveryWeightDisposition._

  val CorrelationGateThresholdBasisPoints = 9500L
  val FinalValueGateThresholdBasisPoints = 2000L
  val FailureDistributionVersion = "equal-four-points-v2"

  private val AfterFirstEligible = "AFTER_FIRST_ELIGIBLE_COMPLETES"
  private val AfterMultipleUpstream = "AFTER_MULTIPLE_UPSTREAM_SHUFFLES_COMPLETE"
  private val BeforeMostExpensive = "BEFORE_MOST_EXPENSIVE_SHUFFLE_COMPLETES"
  private val AfterMixedWork = "AFTER_ELIGIBLE_INELIGIBLE_MIX"

  val FailurePointOrder: Seq[String] = Seq(
    AfterFirstEligible,
    AfterMultipleUpstream,
    BeforeMostExpensive,
    AfterMixedWork)

  def build(
      snapshot: ShuffleRecoveryOpportunityStudySnapshot,
      gateRecords: Seq[ShuffleRecoveryWeightedObservation],
      queryByExecution: Map[Long, ShuffleRecoveryCorpusQuery]): ShuffleRecoveryOpportunityEvidence = {
    val materialized = snapshot.stages
    val stageKeys = materialized.map(stage => (stage.executionId, stage.shuffleId))
    require(stageKeys.distinct.size == stageKeys.size, "duplicate materialized shuffle runtime")

    val correlatedKeys = gateRecords.iterator
      .filter(_.disposition == Weighted)
      .flatMap { record =>
        record.shuffleId.map(shuffleId => (executionId(record.classification.executionId), shuffleId))
      }.toSet
    val correlated = materialized.filter(stage => correlatedKeys.contains((stage.executionId, stage.shuffleId)))
    val correlation = ShuffleRecoveryCorrelationCoverage(
      materialized.size.toLong,
      correlated.size.toLong,
      checkedSum(materialized.map(_.shuffleWriteBytes), "materialized shuffle bytes"),
      checkedSum(correlated.map(_.shuffleWriteBytes), "correlated shuffle bytes"),
      checkedSum(materialized.map(_.executorRunTimeMs), "materialized shuffle task time"),
      checkedSum(correlated.map(_.executorRunTimeMs), "correlated shuffle task time"))

    val weighted = gateRecords.filter(_.disposition == Weighted)
    val totalTime = checkedSum(weighted.flatMap(_.executorRunTimeMs), "weighted task time")
    val eligibleTime = checkedSum(
      weighted.filter(_.classification.eligible).flatMap(_.executorRunTimeMs),
      "eligible task time")
    val queryOpportunity = buildQueryOpportunity(weighted, queryByExecution)
    val materialQueries = queryOpportunity.filter(_.completedExecutorRunTimeMs > 0L)
    val queryWeighted = if (materialQueries.isEmpty) {
      None
    } else {
      val basisPoints = materialQueries.flatMap(_.opportunity.basisPoints)
      if (basisPoints.isEmpty) None else Some(basisPoints.sum / basisPoints.size)
    }
    val sortedExchanges = weighted.sortBy(record => -record.executorRunTimeMs.getOrElse(0L))
    val queryTimes = materialQueries.sortBy(query => -query.completedExecutorRunTimeMs)

    ShuffleRecoveryOpportunityEvidence(
      correlation,
      CorrelationGateThresholdBasisPoints,
      FailureDistributionVersion,
      buildFailurePoints(weighted, queryByExecution.keySet),
      FinalValueGateThresholdBasisPoints,
      queryOpportunity,
      ShuffleRecoveryRatio(eligibleTime, totalTime),
      queryWeighted,
      opportunityAfterRemoving(sortedExchanges, 1),
      opportunityAfterRemoving(sortedExchanges, 5),
      concentration(sortedExchanges.take(1).flatMap(_.executorRunTimeMs), totalTime),
      concentration(sortedExchanges.take(5).flatMap(_.executorRunTimeMs), totalTime),
      concentration(queryTimes.take(1).map(_.completedExecutorRunTimeMs), totalTime),
      concentration(queryTimes.take(5).map(_.completedExecutorRunTimeMs), totalTime),
      materialQueries.count(_.reusableExecutorRunTimeMs == 0L).toLong,
      materialQueries.size.toLong,
      gateRecords.filter(_.disposition == Unweighted)
        .flatMap(_.accountingReason)
        .groupBy(identity)
        .toSeq
        .map { case (reason, values) => reason -> values.size.toLong }
        .sortBy(_._1))
  }

  def reconciliationJsonLines(
      snapshot: ShuffleRecoveryOpportunityStudySnapshot,
      gateRecords: Seq[ShuffleRecoveryWeightedObservation]): Seq[String] = {
    val stages = snapshot.stages.map { stage =>
      (stage.executionId, stage.stageId, stage.stageAttemptId, stage.shuffleId) -> stage
    }.toMap
    gateRecords.sortBy(record => (
      record.classification.executionId,
      record.classification.exchangeOrdinal)).map { record =>
      val stage = for {
        stageId <- record.stageId
        attemptId <- record.stageAttemptId
        shuffleId <- record.shuffleId
        found <- stages.get((executionId(record.classification.executionId), stageId, attemptId, shuffleId))
      } yield found
      val fields = Seq(
        "sqlExecutionId" -> quote(record.classification.executionId),
        "exchangeOrdinal" -> record.classification.exchangeOrdinal.toString,
        "exchangePath" -> quote(record.classification.exchangePath),
        "shuffleId" -> optionalInt(record.shuffleId),
        "stageId" -> optionalInt(record.stageId),
        "stageAttemptId" -> optionalInt(record.stageAttemptId),
        "expectedMapperCount" -> optionalInt(stage.map(_.expectedMapTasks)),
        "acceptedWinnerCount" -> optionalInt(stage.map(_.successfulMapTaskWinners)),
        "stageComplete" -> optionalBoolean(stage.map(_.complete)),
        "shuffleWriteBytesAvailable" -> stage.isDefined.toString,
        "executorRunTimeAvailable" -> stage.isDefined.toString,
        "disposition" -> quote(record.disposition.code),
        "reason" -> optionalString(record.accountingReason))
      fields.map { case (key, value) => s"${quote(key)}:$value" }.mkString("{", ",", "}")
    }
  }

  private def buildFailurePoints(
      weighted: Seq[ShuffleRecoveryWeightedObservation],
      executionIds: Set[Long]): Seq[ShuffleRecoveryEvidenceFailurePoint] = {
    val byExecution = weighted.groupBy(record => executionId(record.classification.executionId))
    executionIds.toSeq.sorted.flatMap { id =>
      val physical = byExecution.getOrElse(id, Nil).sortBy { record =>
        (record.completionOrder.getOrElse(Long.MaxValue), record.classification.exchangeOrdinal)
      }
      val firstEligible = physical.find(_.classification.eligible).flatMap(_.completionOrder)
      val second = physical.lift(1).flatMap(_.completionOrder)
      val expensive = physical.sortBy { record =>
        (-record.executorRunTimeMs.getOrElse(0L), -record.shuffleWriteBytes.getOrElse(0L),
          record.completionOrder.getOrElse(Long.MaxValue))
      }.headOption.flatMap(_.completionOrder)
      val mixed = mixedCutoff(physical)
      Seq(
        failurePoint(id, AfterFirstEligible, physical, firstEligible, inclusive = true),
        failurePoint(id, AfterMultipleUpstream, physical, second, inclusive = true),
        failurePoint(id, BeforeMostExpensive, physical, expensive, inclusive = false),
        failurePoint(id, AfterMixedWork, physical, mixed, inclusive = true))
    }
  }

  private def mixedCutoff(
      physical: Seq[ShuffleRecoveryWeightedObservation]): Option[Long] = {
    if (physical.size < 3) {
      None
    } else {
      (1 until physical.size - 1).iterator.flatMap { index =>
        val prefix = physical.take(index + 1)
        val hasEligible = prefix.exists(_.classification.eligible)
        val hasIneligible = prefix.exists(record => !record.classification.eligible)
        if (hasEligible && hasIneligible) physical(index).completionOrder else None
      }.toSeq.headOption
    }
  }

  private def failurePoint(
      executionId: Long,
      point: String,
      physical: Seq[ShuffleRecoveryWeightedObservation],
      cutoff: Option[Long],
      inclusive: Boolean): ShuffleRecoveryEvidenceFailurePoint = {
    cutoff match {
      case None =>
        ShuffleRecoveryEvidenceFailurePoint(executionId, point, applicable = false,
          0L, 0L, 0L, 0L, 0L, 0L)
      case Some(value) =>
        val completed = physical.filter { record =>
          record.completionOrder.exists(order => if (inclusive) order <= value else order < value)
        }
        val reusable = completed.filter(_.classification.eligible)
        ShuffleRecoveryEvidenceFailurePoint(
          executionId,
          point,
          applicable = true,
          completed.size.toLong,
          reusable.size.toLong,
          checkedSum(completed.flatMap(_.shuffleWriteBytes), s"$executionId/$point bytes"),
          checkedSum(reusable.flatMap(_.shuffleWriteBytes), s"$executionId/$point reusable bytes"),
          checkedSum(completed.flatMap(_.executorRunTimeMs), s"$executionId/$point time"),
          checkedSum(reusable.flatMap(_.executorRunTimeMs), s"$executionId/$point reusable time"))
    }
  }

  private def buildQueryOpportunity(
      weighted: Seq[ShuffleRecoveryWeightedObservation],
      queryByExecution: Map[Long, ShuffleRecoveryCorpusQuery]): Seq[ShuffleRecoveryQueryOpportunity] = {
    val byExecution = weighted.groupBy(record => executionId(record.classification.executionId))
    queryByExecution.toSeq.sortBy(_._1).map { case (id, query) =>
      val records = byExecution.getOrElse(id, Nil)
      val completed = checkedSum(records.flatMap(_.executorRunTimeMs), s"query $id completed")
      val reusable = checkedSum(
        records.filter(_.classification.eligible).flatMap(_.executorRunTimeMs),
        s"query $id reusable")
      ShuffleRecoveryQueryOpportunity(id, query.family, query.name, query.aqeEnabled, completed, reusable)
    }
  }

  private def opportunityAfterRemoving(
      records: Seq[ShuffleRecoveryWeightedObservation],
      count: Int): ShuffleRecoveryRatio = {
    val remaining = records.drop(count)
    val completed = checkedSum(remaining.flatMap(_.executorRunTimeMs), s"remove top $count completed")
    val reusable = checkedSum(
      remaining.filter(_.classification.eligible).flatMap(_.executorRunTimeMs),
      s"remove top $count reusable")
    ShuffleRecoveryRatio(reusable, completed)
  }

  private def concentration(values: Iterable[Long], total: Long): ShuffleRecoveryRatio =
    ShuffleRecoveryRatio(checkedSum(values, "concentration"), total)

  private def executionId(value: String): Long = {
    val prefix = "query-"
    require(value.startsWith(prefix), s"unexpected execution id: $value")
    value.substring(prefix.length).toLong
  }

  private def checkedSum(values: Iterable[Long], label: String): Long = {
    try {
      values.foldLeft(0L)(Math.addExact)
    } catch {
      case _: ArithmeticException =>
        throw new IllegalArgumentException(s"overflow while summing $label")
    }
  }

  private def optionalInt(value: Option[Int]): String = value.map(_.toString).getOrElse("null")
  private def optionalBoolean(value: Option[Boolean]): String = value.map(_.toString).getOrElse("null")
  private def optionalString(value: Option[String]): String = value.map(quote).getOrElse("null")

  private def quote(value: String): String = {
    val out = new StringBuilder(value.length + 2).append('"')
    value.foreach {
      case '"' => out.append("\\\"")
      case '\\' => out.append("\\\\")
      case '\n' => out.append("\\n")
      case '\r' => out.append("\\r")
      case '\t' => out.append("\\t")
      case c if c < ' ' => out.append(f"\\u${c.toInt}%04x")
      case c => out.append(c)
    }
    out.append('"').toString()
  }
}
