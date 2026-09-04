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

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.rdd.DeterministicLevel
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{
  HashPartitioning, NullAwareHashPartitioning, Partitioning, RangePartitioning,
  RoundRobinPartitioning, SinglePartition}
import org.apache.spark.sql.execution.{ExecSubqueryExpression, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{
  AdaptiveSparkPlanExec, AQEShuffleReadExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.util.QueryExecutionListener

/**
 * Conservative, observation-only classification for completed SQL shuffle exchanges.
 *
 * The analyzer intentionally uses a closed rule set. Unknown operators, expressions,
 * partitionings, and source semantics fail closed so eligibility cannot silently expand when
 * Spark gains a new execution feature. The result is evidence about opportunity only; it does not
 * mutate scheduler state, shuffle state, query plans, or source state.
 */
private[sql] object ShuffleRecoveryEligibility {

  sealed trait MissReason {
    def code: String
    private[ShuffleRecoveryEligibility] def rootRank: Int
  }

  case object NON_DETERMINATE extends MissReason {
    override val code: String = "NON_DETERMINATE"
    override val rootRank: Int = 0
  }

  case object PYTHON_OR_ARROW extends MissReason {
    override val code: String = "PYTHON_OR_ARROW"
    override val rootRank: Int = 1
  }

  case object DPP_PRESENT extends MissReason {
    override val code: String = "DPP_PRESENT"
    override val rootRank: Int = 2
  }

  case object RUNTIME_FILTER_PRESENT extends MissReason {
    override val code: String = "RUNTIME_FILTER_PRESENT"
    override val rootRank: Int = 3
  }

  case object SUBQUERY_PRESENT extends MissReason {
    override val code: String = "SUBQUERY_PRESENT"
    override val rootRank: Int = 4
  }

  case object SOURCE_TOKEN_UNAVAILABLE extends MissReason {
    override val code: String = "SOURCE_TOKEN_UNAVAILABLE"
    override val rootRank: Int = 5
  }

  case object WINDOW_PRESENT extends MissReason {
    override val code: String = "WINDOW_PRESENT"
    override val rootRank: Int = 6
  }

  case object EXPAND_PRESENT extends MissReason {
    override val code: String = "EXPAND_PRESENT"
    override val rootRank: Int = 7
  }

  case object CACHE_SCAN_PRESENT extends MissReason {
    override val code: String = "CACHE_SCAN_PRESENT"
    override val rootRank: Int = 8
  }

  case object RANGE_PARTITIONING extends MissReason {
    override val code: String = "RANGE_PARTITIONING"
    override val rootRank: Int = 9
  }

  case object ADAPTIVE_PARTITION_SPECS extends MissReason {
    override val code: String = "ADAPTIVE_PARTITION_SPECS"
    override val rootRank: Int = 10
  }

  case object UNSUPPORTED_SHUFFLE_MODE extends MissReason {
    override val code: String = "UNSUPPORTED_SHUFFLE_MODE"
    override val rootRank: Int = 11
  }

  case object INVALID_PARTITION_COUNT extends MissReason {
    override val code: String = "INVALID_PARTITION_COUNT"
    override val rootRank: Int = 12
  }

  case object UNSUPPORTED_PARTITIONING extends MissReason {
    override val code: String = "UNSUPPORTED_PARTITIONING"
    override val rootRank: Int = 13
  }

  case object UNSUPPORTED_EXPRESSION extends MissReason {
    override val code: String = "UNSUPPORTED_EXPRESSION"
    override val rootRank: Int = 14
  }

  case object CUSTOM_OPERATOR extends MissReason {
    override val code: String = "CUSTOM_OPERATOR"
    override val rootRank: Int = 15
  }

  case object UNSUPPORTED_OPERATOR extends MissReason {
    override val code: String = "UNSUPPORTED_OPERATOR"
    override val rootRank: Int = 16
  }

  sealed trait SourceTokenAvailability {
    def code: String
    private[ShuffleRecoveryEligibility] def severity: Int
  }

  case object ExactTokenAvailable extends SourceTokenAvailability {
    override val code: String = "EXACT_TOKEN_AVAILABLE"
    override val severity: Int = 0
  }

  case object PrototypeSpecialCased extends SourceTokenAvailability {
    override val code: String = "PROTOTYPE_SPECIAL_CASED"
    override val severity: Int = 1
  }

  case object SourceTokenUnavailable extends SourceTokenAvailability {
    override val code: String = "UNAVAILABLE"
    override val severity: Int = 2
  }

  sealed trait LineageDeterminism {
    def code: String
  }

  case object DeterminateLineage extends LineageDeterminism {
    override val code: String = "DETERMINATE"
  }

  case object UnorderedLineage extends LineageDeterminism {
    override val code: String = "UNORDERED"
  }

  case object IndeterminateLineage extends LineageDeterminism {
    override val code: String = "INDETERMINATE"
  }

  case object UnknownLineage extends LineageDeterminism {
    override val code: String = "UNKNOWN"
  }

  final case class RuleSet(
      name: String,
      version: Int,
      allowedOperatorClassNames: Set[String],
      knownOperatorClassNames: Set[String],
      allowedExpressionClassNames: Set[String],
      allowedPartitioningClassNames: Set[String],
      acceptedSourceTokens: Set[SourceTokenAvailability],
      requireDeterminateLineage: Boolean = true,
      allowRangePartitioning: Boolean = false,
      allowPythonOrArrow: Boolean = false,
      allowDpp: Boolean = false,
      allowRuntimeFilters: Boolean = false,
      allowSubqueries: Boolean = false,
      allowWindow: Boolean = false,
      allowExpand: Boolean = false,
      allowCacheScan: Boolean = false,
      allowAdaptivePartitionSpecs: Boolean = false,
      allowPipelinedShuffle: Boolean = false,
      allowPushBasedShuffle: Boolean = false) {
    require(name.nonEmpty, "rule-set name must be non-empty")
    require(version >= 0, "rule-set version must be non-negative")
    require(allowedOperatorClassNames.subsetOf(knownOperatorClassNames),
      "allowed operators must be a subset of known operators")
  }

  object RuleSet {
    private val allowedOperators = Set(
      "org.apache.spark.sql.execution.CoalesceExec",
      "org.apache.spark.sql.execution.CollectLimitExec",
      "org.apache.spark.sql.execution.FilterExec",
      "org.apache.spark.sql.execution.GenerateExec",
      "org.apache.spark.sql.execution.GlobalLimitExec",
      "org.apache.spark.sql.execution.InputAdapter",
      "org.apache.spark.sql.execution.LocalLimitExec",
      "org.apache.spark.sql.execution.LocalTableScanExec",
      "org.apache.spark.sql.execution.ProjectExec",
      "org.apache.spark.sql.execution.RangeExec",
      "org.apache.spark.sql.execution.SampleExec",
      "org.apache.spark.sql.execution.SortExec",
      "org.apache.spark.sql.execution.TakeOrderedAndProjectExec",
      "org.apache.spark.sql.execution.UnionExec",
      "org.apache.spark.sql.execution.WholeStageCodegenExec",
      "org.apache.spark.sql.execution.aggregate.HashAggregateExec",
      "org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec",
      "org.apache.spark.sql.execution.aggregate.SortAggregateExec",
      "org.apache.spark.sql.execution.columnar.InMemoryTableScanExec",
      "org.apache.spark.sql.execution.datasources.FileSourceScanExec",
      "org.apache.spark.sql.execution.datasources.v2.BatchScanExec",
      "org.apache.spark.sql.execution.exchange.ShuffleExchangeExec",
      "org.apache.spark.sql.execution.joins.BroadcastHashJoinExec",
      "org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec",
      "org.apache.spark.sql.execution.joins.CartesianProductExec",
      "org.apache.spark.sql.execution.joins.ShuffledHashJoinExec",
      "org.apache.spark.sql.execution.joins.SortMergeJoinExec",
      "org.apache.spark.sql.execution.python.AggregateInPandasExec",
      "org.apache.spark.sql.execution.python.ArrowEvalPythonExec",
      "org.apache.spark.sql.execution.python.BatchEvalPythonExec",
      "org.apache.spark.sql.execution.python.FlatMapGroupsInPandasExec",
      "org.apache.spark.sql.execution.python.MapInPandasExec",
      "org.apache.spark.sql.execution.window.WindowExec",
      "org.apache.spark.sql.execution.ExpandExec")

    private val knownUnsupportedOperators = Set(
      "org.apache.spark.sql.execution.RDDScanExec",
      "org.apache.spark.sql.execution.RowDataSourceScanExec",
      "org.apache.spark.sql.execution.command.DataWritingCommandExec",
      "org.apache.spark.sql.execution.datasources.v2.V2CommandExec")

    private val allowedExpressions = Set(
      "org.apache.spark.sql.catalyst.expressions.Add",
      "org.apache.spark.sql.catalyst.expressions.Alias",
      "org.apache.spark.sql.catalyst.expressions.And",
      "org.apache.spark.sql.catalyst.expressions.AttributeReference",
      "org.apache.spark.sql.catalyst.expressions.BoundReference",
      "org.apache.spark.sql.catalyst.expressions.CaseWhen",
      "org.apache.spark.sql.catalyst.expressions.Cast",
      "org.apache.spark.sql.catalyst.expressions.Coalesce",
      "org.apache.spark.sql.catalyst.expressions.Concat",
      "org.apache.spark.sql.catalyst.expressions.ConcatWs",
      "org.apache.spark.sql.catalyst.expressions.Contains",
      "org.apache.spark.sql.catalyst.expressions.Divide",
      "org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression",
      "org.apache.spark.sql.catalyst.expressions.EndsWith",
      "org.apache.spark.sql.catalyst.expressions.EqualNullSafe",
      "org.apache.spark.sql.catalyst.expressions.EqualTo",
      "org.apache.spark.sql.catalyst.expressions.GreaterThan",
      "org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual",
      "org.apache.spark.sql.catalyst.expressions.If",
      "org.apache.spark.sql.catalyst.expressions.In",
      "org.apache.spark.sql.catalyst.expressions.InSet",
      "org.apache.spark.sql.catalyst.expressions.IsNotNull",
      "org.apache.spark.sql.catalyst.expressions.IsNull",
      "org.apache.spark.sql.catalyst.expressions.KnownNotNull",
      "org.apache.spark.sql.catalyst.expressions.Length",
      "org.apache.spark.sql.catalyst.expressions.LessThan",
      "org.apache.spark.sql.catalyst.expressions.LessThanOrEqual",
      "org.apache.spark.sql.catalyst.expressions.Like",
      "org.apache.spark.sql.catalyst.expressions.Literal",
      "org.apache.spark.sql.catalyst.expressions.Lower",
      "org.apache.spark.sql.catalyst.expressions.Multiply",
      "org.apache.spark.sql.catalyst.expressions.Not",
      "org.apache.spark.sql.catalyst.expressions.Or",
      "org.apache.spark.sql.catalyst.expressions.Pmod",
      "org.apache.spark.sql.catalyst.expressions.PythonUDF",
      "org.apache.spark.sql.catalyst.expressions.RLike",
      "org.apache.spark.sql.catalyst.expressions.Remainder",
      "org.apache.spark.sql.catalyst.expressions.SortOrder",
      "org.apache.spark.sql.catalyst.expressions.StartsWith",
      "org.apache.spark.sql.catalyst.expressions.Substring",
      "org.apache.spark.sql.catalyst.expressions.Subtract",
      "org.apache.spark.sql.catalyst.expressions.Upper",
      "org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression",
      "org.apache.spark.sql.catalyst.expressions.aggregate.Average",
      "org.apache.spark.sql.catalyst.expressions.aggregate.Count",
      "org.apache.spark.sql.catalyst.expressions.aggregate.First",
      "org.apache.spark.sql.catalyst.expressions.aggregate.Last",
      "org.apache.spark.sql.catalyst.expressions.aggregate.Max",
      "org.apache.spark.sql.catalyst.expressions.aggregate.Min",
      "org.apache.spark.sql.catalyst.expressions.aggregate.Sum",
      "org.apache.spark.sql.execution.InSubqueryExec",
      "org.apache.spark.sql.execution.ScalarSubquery")

    private val allowedPartitionings = Set(
      "org.apache.spark.sql.catalyst.plans.physical.HashPartitioning",
      "org.apache.spark.sql.catalyst.plans.physical.NullAwareHashPartitioning",
      "org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning",
      "org.apache.spark.sql.catalyst.plans.physical.SinglePartition$")

    val Conservative: RuleSet = RuleSet(
      name = "conservative-v1",
      version = 1,
      allowedOperatorClassNames = allowedOperators,
      knownOperatorClassNames = allowedOperators ++ knownUnsupportedOperators,
      allowedExpressionClassNames = allowedExpressions,
      allowedPartitioningClassNames = allowedPartitionings,
      acceptedSourceTokens = Set(ExactTokenAvailable))
  }

  final case class Flags(
      dpp: Boolean,
      runtimeFilter: Boolean,
      subquery: Boolean,
      window: Boolean,
      expand: Boolean,
      cacheScan: Boolean,
      pythonOrArrow: Boolean,
      adaptivePartitionSpecs: Boolean,
      reusedExchange: Boolean,
      adaptivePlan: Boolean)

  final case class Record(
      executionId: String,
      exchangeOrdinal: Long,
      exchangePath: String,
      operatorSummary: String,
      partitioningSummary: String,
      ruleSetName: String,
      ruleSetVersion: Int,
      eligible: Boolean,
      immediateReason: Option[MissReason],
      rootReason: Option[MissReason],
      flags: Flags,
      sourceTokenAvailability: SourceTokenAvailability,
      lineageDeterminism: LineageDeterminism,
      mapperCount: Option[Int],
      reducerCount: Int) {

    def toJson: String = {
      val fields = Seq(
        stringField("executionId", executionId),
        numberField("exchangeOrdinal", exchangeOrdinal),
        stringField("exchangePath", exchangePath),
        stringField("operatorSummary", operatorSummary),
        stringField("partitioningSummary", partitioningSummary),
        stringField("ruleSetName", ruleSetName),
        numberField("ruleSetVersion", ruleSetVersion),
        booleanField("eligible", eligible),
        optionalStringField("immediateReason", immediateReason.map(_.code)),
        optionalStringField("rootReason", rootReason.map(_.code)),
        flagsField(flags),
        stringField("sourceTokenAvailability", sourceTokenAvailability.code),
        stringField("lineageDeterminism", lineageDeterminism.code),
        optionalNumberField("mapperCount", mapperCount),
        numberField("reducerCount", reducerCount))
      fields.mkString("{", ",", "}")
    }
  }

  private final case class AnalysisRuntime(pushBasedShuffleEnabled: Boolean)

  private object AnalysisRuntime {
    val Empty: AnalysisRuntime = AnalysisRuntime(pushBasedShuffleEnabled = false)

    def from(qe: QueryExecution): AnalysisRuntime = {
      AnalysisRuntime(
        qe.sparkSession.sparkContext.getConf.getBoolean("spark.shuffle.push.enabled", false))
    }
  }

  private final case class PlanFrame(
      plan: SparkPlan,
      path: Vector[Int],
      reused: Boolean,
      adaptive: Boolean,
      adaptivePartitionSpecs: Boolean)

  private final case class Candidate(
      exchange: ShuffleExchangeExec,
      path: Vector[Int],
      reused: Boolean,
      adaptive: Boolean,
      adaptivePartitionSpecs: Boolean)

  private final case class Finding(reason: MissReason, path: Vector[Int], sequence: Long)

  private final class FlagAccumulator(
      var dpp: Boolean = false,
      var runtimeFilter: Boolean = false,
      var subquery: Boolean = false,
      var window: Boolean = false,
      var expand: Boolean = false,
      var cacheScan: Boolean = false,
      var pythonOrArrow: Boolean = false) {
    def result(candidate: Candidate): Flags = Flags(
      dpp = dpp,
      runtimeFilter = runtimeFilter,
      subquery = subquery,
      window = window,
      expand = expand,
      cacheScan = cacheScan,
      pythonOrArrow = pythonOrArrow,
      adaptivePartitionSpecs = candidate.adaptivePartitionSpecs,
      reusedExchange = candidate.reused,
      adaptivePlan = candidate.adaptive)
  }

  final class Analyzer(val ruleSet: RuleSet) {

    def analyze(
        executedPlan: SparkPlan,
        executionId: String): Seq[Record] = {
      analyze(executedPlan, executionId, AnalysisRuntime.Empty)
    }

    private def analyze(
        executedPlan: SparkPlan,
        executionId: String,
        runtime: AnalysisRuntime): Seq[Record] = {
      discoverExchanges(executedPlan).zipWithIndex.map { case (candidate, index) =>
        analyzeCandidate(candidate, executionId, index.toLong, runtime)
      }
    }

    private def analyzeCandidate(
        candidate: Candidate,
        executionId: String,
        ordinal: Long,
        runtime: AnalysisRuntime): Record = {
      val findings = mutable.ArrayBuffer.empty[Finding]
      val flags = new FlagAccumulator
      var findingSequence = 0L

      def addFinding(reason: MissReason, path: Vector[Int]): Unit = {
        findings += Finding(reason, path, findingSequence)
        findingSequence += 1L
      }

      inspectExchange(candidate, runtime, addFinding)
      val sourceToken = inspectSubtree(candidate.exchange.child, flags, addFinding)
      val lineage = lineageDeterminism(candidate.exchange)
      if (ruleSet.requireDeterminateLineage && lineage != DeterminateLineage) {
        addFinding(NON_DETERMINATE, candidate.path)
      }
      if (candidate.adaptivePartitionSpecs && !ruleSet.allowAdaptivePartitionSpecs) {
        addFinding(ADAPTIVE_PARTITION_SPECS, candidate.path)
      }
      if (!ruleSet.acceptedSourceTokens.contains(sourceToken)) {
        addFinding(SOURCE_TOKEN_UNAVAILABLE, candidate.path)
      }

      val immediate = findings.headOption.map(_.reason)
      val root = findings.sortBy { finding =>
        (finding.reason.rootRank, pathString(finding.path), finding.sequence)
      }.headOption.map(_.reason)
      val partitioning = candidate.exchange.outputPartitioning

      Record(
        executionId = executionId,
        exchangeOrdinal = ordinal,
        exchangePath = pathString(candidate.path),
        operatorSummary = candidate.exchange.child.getClass.getSimpleName,
        partitioningSummary = partitioningSummary(partitioning),
        ruleSetName = ruleSet.name,
        ruleSetVersion = ruleSet.version,
        eligible = findings.isEmpty,
        immediateReason = immediate,
        rootReason = root,
        flags = flags.result(candidate),
        sourceTokenAvailability = sourceToken,
        lineageDeterminism = lineage,
        mapperCount = mapperCount(candidate.exchange),
        reducerCount = partitioning.numPartitions)
    }

    private def inspectExchange(
        candidate: Candidate,
        runtime: AnalysisRuntime,
        addFinding: (MissReason, Vector[Int]) => Unit): Unit = {
      val exchange = candidate.exchange
      if (exchange.pipelined && !ruleSet.allowPipelinedShuffle) {
        addFinding(UNSUPPORTED_SHUFFLE_MODE, candidate.path)
      }
      if (runtime.pushBasedShuffleEnabled && !ruleSet.allowPushBasedShuffle) {
        addFinding(UNSUPPORTED_SHUFFLE_MODE, candidate.path)
      }

      val partitioning = exchange.outputPartitioning
      if (partitioning.numPartitions <= 0) {
        addFinding(INVALID_PARTITION_COUNT, candidate.path)
      }
      partitioning match {
        case _: RangePartitioning if !ruleSet.allowRangePartitioning =>
          addFinding(RANGE_PARTITIONING, candidate.path)
        case _ if !ruleSet.allowedPartitioningClassNames.contains(partitioning.getClass.getName) =>
          addFinding(UNSUPPORTED_PARTITIONING, candidate.path)
        case _ =>
      }
    }

    private def inspectSubtree(
        root: SparkPlan,
        flags: FlagAccumulator,
        addFinding: (MissReason, Vector[Int]) => Unit): SourceTokenAvailability = {
      val stack = mutable.ArrayBuffer(PlanFrame(root, Vector.empty, reused = false,
        adaptive = false, adaptivePartitionSpecs = false))
      var sourceToken = ExactTokenAvailable: SourceTokenAvailability

      while (stack.nonEmpty) {
        val frame = stack.remove(stack.length - 1)
        val plan = frame.plan
        val className = plan.getClass.getName

        if (pythonOperatorClassNames.contains(className)) {
          flags.pythonOrArrow = true
          if (!ruleSet.allowPythonOrArrow) {
            addFinding(PYTHON_OR_ARROW, frame.path)
          }
        }
        if (windowOperatorClassNames.contains(className)) {
          flags.window = true
          if (!ruleSet.allowWindow) {
            addFinding(WINDOW_PRESENT, frame.path)
          }
        }
        if (expandOperatorClassNames.contains(className)) {
          flags.expand = true
          if (!ruleSet.allowExpand) {
            addFinding(EXPAND_PRESENT, frame.path)
          }
        }
        if (cacheScanClassNames.contains(className)) {
          flags.cacheScan = true
          if (!ruleSet.allowCacheScan) {
            addFinding(CACHE_SCAN_PRESENT, frame.path)
          }
        }

        if (!ruleSet.allowedOperatorClassNames.contains(className)) {
          val reason = if (ruleSet.knownOperatorClassNames.contains(className)) {
            UNSUPPORTED_OPERATOR
          } else {
            CUSTOM_OPERATOR
          }
          addFinding(reason, frame.path)
        }

        inspectExpressions(plan.expressions, frame.path, flags, addFinding)

        val children = plan.children
        if (children.isEmpty) {
          val availability = sourceTokenFor(plan)
          if (availability.severity > sourceToken.severity) {
            sourceToken = availability
          }
        }
        children.indices.reverse.foreach { childIndex =>
          stack += PlanFrame(children(childIndex), frame.path :+ childIndex,
            reused = false, adaptive = false, adaptivePartitionSpecs = false)
        }
      }
      sourceToken
    }

    private def inspectExpressions(
        expressions: Seq[Expression],
        planPath: Vector[Int],
        flags: FlagAccumulator,
        addFinding: (MissReason, Vector[Int]) => Unit): Unit = {
      val stack = mutable.ArrayBuffer.empty[(Expression, Vector[Int])]
      expressions.indices.reverse.foreach { index =>
        stack += ((expressions(index), planPath :+ index))
      }

      while (stack.nonEmpty) {
        val (expression, path) = stack.remove(stack.length - 1)
        val className = expression.getClass.getName

        if (!expression.deterministic) {
          addFinding(NON_DETERMINATE, path)
        }
        if (pythonExpressionClassNames.contains(className)) {
          flags.pythonOrArrow = true
          if (!ruleSet.allowPythonOrArrow) {
            addFinding(PYTHON_OR_ARROW, path)
          }
        }
        if (dppExpressionClassNames.contains(className)) {
          flags.dpp = true
          if (!ruleSet.allowDpp) {
            addFinding(DPP_PRESENT, path)
          }
        }
        if (runtimeFilterExpressionClassNames.contains(className)) {
          flags.runtimeFilter = true
          if (!ruleSet.allowRuntimeFilters) {
            addFinding(RUNTIME_FILTER_PRESENT, path)
          }
        }
        if (expression.isInstanceOf[ExecSubqueryExpression]) {
          flags.subquery = true
          if (!ruleSet.allowSubqueries) {
            addFinding(SUBQUERY_PRESENT, path)
          }
        }
        if (!ruleSet.allowedExpressionClassNames.contains(className)) {
          addFinding(UNSUPPORTED_EXPRESSION, path)
        }

        expression.children.indices.reverse.foreach { childIndex =>
          stack += ((expression.children(childIndex), path :+ childIndex))
        }
      }
    }

    private def lineageDeterminism(exchange: ShuffleExchangeExec): LineageDeterminism = {
      try {
        exchange.shuffleDependency.rdd.outputDeterministicLevel match {
          case DeterministicLevel.DETERMINATE => DeterminateLineage
          case DeterministicLevel.UNORDERED => UnorderedLineage
          case DeterministicLevel.INDETERMINATE => IndeterminateLineage
        }
      } catch {
        case NonFatal(_) => UnknownLineage
      }
    }

    private def mapperCount(exchange: ShuffleExchangeExec): Option[Int] = {
      try {
        Some(exchange.numMappers)
      } catch {
        case NonFatal(_) => None
      }
    }

    private def sourceTokenFor(plan: SparkPlan): SourceTokenAvailability = {
      val className = plan.getClass.getName
      if (exactSourceClassNames.contains(className)) {
        ExactTokenAvailable
      } else if (prototypeSourceClassNames.contains(className)) {
        PrototypeSpecialCased
      } else {
        SourceTokenUnavailable
      }
    }
  }

  /**
   * Listener adapter for evidence runs. Registration is deliberately explicit so the disabled
   * path adds no listener, traversal, allocation, or synchronization to ordinary Spark queries.
   * Callback delivery is serialized because QueryExecutionListener may be invoked concurrently.
   */
  final class Listener(
      analyzer: Analyzer,
      onRecords: Seq[Record] => Unit) extends QueryExecutionListener {
    private val callbackLock = new Object

    override def onSuccess(
        funcName: String,
        qe: QueryExecution,
        durationNs: Long): Unit = {
      val records = analyzer.analyze(qe.executedPlan, qe.queryId.toString,
        AnalysisRuntime.from(qe))
      callbackLock.synchronized {
        onRecords(records)
      }
    }

    override def onFailure(
        funcName: String,
        qe: QueryExecution,
        exception: Exception): Unit = {}
  }

  private def discoverExchanges(plan: SparkPlan): Seq[Candidate] = {
    val result = mutable.ArrayBuffer.empty[Candidate]
    val stack = mutable.ArrayBuffer(PlanFrame(plan, Vector.empty, reused = false,
      adaptive = false, adaptivePartitionSpecs = false))

    while (stack.nonEmpty) {
      val frame = stack.remove(stack.length - 1)
      frame.plan match {
        case adaptive: AdaptiveSparkPlanExec =>
          stack += frame.copy(
            plan = adaptive.executedPlan,
            path = frame.path :+ 0,
            adaptive = true)

        case read: AQEShuffleReadExec =>
          stack += frame.copy(
            plan = read.child,
            path = frame.path :+ 0,
            adaptive = true,
            adaptivePartitionSpecs = true)

        case stage: ShuffleQueryStageExec =>
          stack += frame.copy(
            plan = stage.plan,
            path = frame.path :+ 0,
            reused = frame.reused || stage.plan.isInstanceOf[ReusedExchangeExec],
            adaptive = true)

        case stage: QueryStageExec =>
          stack += frame.copy(plan = stage.plan, path = frame.path :+ 0, adaptive = true)

        case reused: ReusedExchangeExec =>
          stack += frame.copy(
            plan = reused.child,
            path = frame.path :+ 0,
            reused = true)

        case exchange: ShuffleExchangeExec =>
          result += Candidate(
            exchange,
            frame.path,
            frame.reused,
            frame.adaptive,
            frame.adaptivePartitionSpecs)
          stack += frame.copy(plan = exchange.child, path = frame.path :+ 0)

        case other =>
          other.children.indices.reverse.foreach { childIndex =>
            stack += frame.copy(
              plan = other.children(childIndex),
              path = frame.path :+ childIndex)
          }
      }
    }
    result.toSeq
  }

  private val pythonOperatorClassNames = Set(
    "org.apache.spark.sql.execution.python.AggregateInPandasExec",
    "org.apache.spark.sql.execution.python.ArrowEvalPythonExec",
    "org.apache.spark.sql.execution.python.BatchEvalPythonExec",
    "org.apache.spark.sql.execution.python.CoGroupMapInPandasExec",
    "org.apache.spark.sql.execution.python.FlatMapGroupsInPandasExec",
    "org.apache.spark.sql.execution.python.MapInPandasExec",
    "org.apache.spark.sql.execution.python.WindowInPandasExec")

  private val pythonExpressionClassNames = Set(
    "org.apache.spark.sql.catalyst.expressions.PythonUDF")

  private val dppExpressionClassNames = Set(
    "org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression")

  private val runtimeFilterExpressionClassNames = Set(
    "org.apache.spark.sql.catalyst.expressions.BloomFilterMightContain")

  private val windowOperatorClassNames = Set(
    "org.apache.spark.sql.execution.window.WindowExec",
    "org.apache.spark.sql.execution.window.WindowGroupLimitExec")

  private val expandOperatorClassNames = Set(
    "org.apache.spark.sql.execution.ExpandExec")

  private val cacheScanClassNames = Set(
    "org.apache.spark.sql.execution.columnar.InMemoryTableScanExec")

  private val exactSourceClassNames = Set(
    "org.apache.spark.sql.execution.RangeExec")

  private val prototypeSourceClassNames = Set(
    "org.apache.spark.sql.execution.LocalTableScanExec",
    "org.apache.spark.sql.execution.datasources.FileSourceScanExec",
    "org.apache.spark.sql.execution.datasources.v2.BatchScanExec")

  private def partitioningSummary(partitioning: Partitioning): String = partitioning match {
    case _: HashPartitioning => s"HASH:${partitioning.numPartitions}"
    case _: NullAwareHashPartitioning => s"NULL_AWARE_HASH:${partitioning.numPartitions}"
    case _: RoundRobinPartitioning => s"ROUND_ROBIN:${partitioning.numPartitions}"
    case _: RangePartitioning => s"RANGE:${partitioning.numPartitions}"
    case SinglePartition => "SINGLE:1"
    case other => s"${other.getClass.getName}:${other.numPartitions}"
  }

  private def pathString(path: Vector[Int]): String = {
    if (path.isEmpty) "root" else path.mkString(".")
  }

  private def flagsField(flags: Flags): String = {
    val value = Seq(
      booleanField("dpp", flags.dpp),
      booleanField("runtimeFilter", flags.runtimeFilter),
      booleanField("subquery", flags.subquery),
      booleanField("window", flags.window),
      booleanField("expand", flags.expand),
      booleanField("cacheScan", flags.cacheScan),
      booleanField("pythonOrArrow", flags.pythonOrArrow),
      booleanField("adaptivePartitionSpecs", flags.adaptivePartitionSpecs),
      booleanField("reusedExchange", flags.reusedExchange),
      booleanField("adaptivePlan", flags.adaptivePlan)).mkString("{", ",", "}")
    s"\"flags\":$value"
  }

  private def stringField(name: String, value: String): String = {
    s"\"${jsonEscape(name)}\":\"${jsonEscape(value)}\""
  }

  private def optionalStringField(name: String, value: Option[String]): String = {
    value.map(stringField(name, _)).getOrElse(s"\"${jsonEscape(name)}\":null")
  }

  private def numberField(name: String, value: Long): String = {
    s"\"${jsonEscape(name)}\":$value"
  }

  private def optionalNumberField(name: String, value: Option[Int]): String = {
    value.map(v => numberField(name, v)).getOrElse(s"\"${jsonEscape(name)}\":null")
  }

  private def booleanField(name: String, value: Boolean): String = {
    s"\"${jsonEscape(name)}\":$value"
  }

  private def jsonEscape(value: String): String = {
    val builder = new StringBuilder(value.length + 16)
    value.foreach {
      case '"' => builder.append("\\\"")
      case '\\' => builder.append("\\\\")
      case '\b' => builder.append("\\b")
      case '\f' => builder.append("\\f")
      case '\n' => builder.append("\\n")
      case '\r' => builder.append("\\r")
      case '\t' => builder.append("\\t")
      case c if c < ' ' => builder.append(f"\\u${c.toInt}%04x")
      case c => builder.append(c)
    }
    builder.result()
  }
}