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

import java.util.IdentityHashMap
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{
  HashPartitioning, NullAwareHashPartitioning, Partitioning, RangePartitioning,
  RoundRobinPartitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.execution.{ExecSubqueryExpression, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.util.QueryExecutionListener

/** Stable machine-readable reasons emitted by the shuffle recovery opportunity analyzer. */
private[sql] sealed trait ShuffleRecoveryMissReason {
  def code: String
  private[exchange] def rootRank: Int
}

private[sql] object ShuffleRecoveryMissReason {
  case object NonDeterministic extends ShuffleRecoveryMissReason {
    override val code: String = "NON_DETERMINATE"
    override val rootRank: Int = 0
  }
  case object DeterminismUnproven extends ShuffleRecoveryMissReason {
    override val code: String = "DETERMINISM_UNPROVEN"
    override val rootRank: Int = 1
  }
  case object PythonOrArrowPresent extends ShuffleRecoveryMissReason {
    override val code: String = "PYTHON_OR_ARROW"
    override val rootRank: Int = 2
  }
  case object DynamicPruningPresent extends ShuffleRecoveryMissReason {
    override val code: String = "DPP_PRESENT"
    override val rootRank: Int = 3
  }
  case object RuntimeFilterPresent extends ShuffleRecoveryMissReason {
    override val code: String = "RUNTIME_FILTER_PRESENT"
    override val rootRank: Int = 4
  }
  case object SubqueryPresent extends ShuffleRecoveryMissReason {
    override val code: String = "SUBQUERY_PRESENT"
    override val rootRank: Int = 5
  }
  case object SourceTokenUnavailable extends ShuffleRecoveryMissReason {
    override val code: String = "SOURCE_TOKEN_UNAVAILABLE"
    override val rootRank: Int = 6
  }
  case object WindowPresent extends ShuffleRecoveryMissReason {
    override val code: String = "WINDOW_PRESENT"
    override val rootRank: Int = 7
  }
  case object ExpandPresent extends ShuffleRecoveryMissReason {
    override val code: String = "EXPAND_PRESENT"
    override val rootRank: Int = 8
  }
  case object CacheScanPresent extends ShuffleRecoveryMissReason {
    override val code: String = "CACHE_SCAN_PRESENT"
    override val rootRank: Int = 9
  }
  case object RangePartitioningPresent extends ShuffleRecoveryMissReason {
    override val code: String = "RANGE_PARTITIONING"
    override val rootRank: Int = 10
  }
  case object AdaptivePartitionSpecPresent extends ShuffleRecoveryMissReason {
    override val code: String = "ADAPTIVE_PARTITION_SPECS"
    override val rootRank: Int = 11
  }
  case object UnsupportedShuffleMode extends ShuffleRecoveryMissReason {
    override val code: String = "UNSUPPORTED_SHUFFLE_MODE"
    override val rootRank: Int = 12
  }
  case object IncompatibleRuntimeFlag extends ShuffleRecoveryMissReason {
    override val code: String = "INCOMPATIBLE_RUNTIME_FLAG"
    override val rootRank: Int = 13
  }
  case object InvalidPartitionCount extends ShuffleRecoveryMissReason {
    override val code: String = "INVALID_PARTITION_COUNT"
    override val rootRank: Int = 14
  }
  case object UnsupportedPartitioning extends ShuffleRecoveryMissReason {
    override val code: String = "UNSUPPORTED_PARTITIONING"
    override val rootRank: Int = 15
  }
  case object UnsupportedExpression extends ShuffleRecoveryMissReason {
    override val code: String = "UNSUPPORTED_EXPRESSION"
    override val rootRank: Int = 16
  }
  case object CustomOperator extends ShuffleRecoveryMissReason {
    override val code: String = "CUSTOM_OPERATOR"
    override val rootRank: Int = 17
  }
  case object UnsupportedOperator extends ShuffleRecoveryMissReason {
    override val code: String = "UNSUPPORTED_OPERATOR"
    override val rootRank: Int = 18
  }
  case object UpstreamIneligible extends ShuffleRecoveryMissReason {
    override val code: String = "UPSTREAM_INELIGIBLE"
    override val rootRank: Int = Int.MaxValue
  }
}

private[sql] sealed trait ShuffleRecoverySourceTokenAvailability {
  def code: String
  private[exchange] def severity: Int
}

private[sql] object ShuffleRecoverySourceTokenAvailability {
  case object Exact extends ShuffleRecoverySourceTokenAvailability {
    override val code: String = "EXACT_TOKEN_AVAILABLE"
    override val severity: Int = 0
  }
  case object PrototypeSpecialCased extends ShuffleRecoverySourceTokenAvailability {
    override val code: String = "PROTOTYPE_SPECIAL_CASED"
    override val severity: Int = 1
  }
  case object Unavailable extends ShuffleRecoverySourceTokenAvailability {
    override val code: String = "UNAVAILABLE"
    override val severity: Int = 2
  }
}

private[sql] sealed trait ShuffleRecoveryLineageDeterminism {
  def code: String
  private[exchange] def severity: Int
}

private[sql] object ShuffleRecoveryLineageDeterminism {
  case object Determinate extends ShuffleRecoveryLineageDeterminism {
    override val code: String = "DETERMINATE"
    override val severity: Int = 0
  }
  case object Unknown extends ShuffleRecoveryLineageDeterminism {
    override val code: String = "UNKNOWN"
    override val severity: Int = 1
  }
  case object Unordered extends ShuffleRecoveryLineageDeterminism {
    override val code: String = "UNORDERED"
    override val severity: Int = 2
  }
  case object Indeterminate extends ShuffleRecoveryLineageDeterminism {
    override val code: String = "INDETERMINATE"
    override val severity: Int = 3
  }
}

/**
 * Conservative, parameterized rules used only to measure recovery opportunity.
 *
 * Exact class-name allowlists are intentional. New Spark or extension classes fail closed instead
 * of inheriting eligibility through a package prefix or reflection-based fallback. Source lineage
 * facts are also explicit: the analyzer never materializes an RDD merely to inspect its
 * DeterministicLevel. This keeps analysis observational while still making the DETERMINATE proof
 * requirement independently configurable.
 */
private[sql] case class ShuffleRecoveryEligibilityRules(
    name: String,
    version: Int,
    allowedOperatorClassNames: Set[String],
    allowedExpressionClassNames: Set[String],
    allowedPartitioningClassNames: Set[String],
    sourceTokenByOperatorClassName: Map[String, ShuffleRecoverySourceTokenAvailability],
    acceptedSourceTokenCategories: Set[ShuffleRecoverySourceTokenAvailability],
    lineageBySourceOperatorClassName: Map[String, ShuffleRecoveryLineageDeterminism] = Map.empty,
    requireDeterminateLineage: Boolean = true,
    allowDynamicPruning: Boolean = false,
    allowRuntimeFilters: Boolean = false,
    allowSubqueries: Boolean = false,
    allowWindow: Boolean = false,
    allowExpand: Boolean = false,
    allowCacheScan: Boolean = false,
    allowAdaptivePartitionSpecs: Boolean = false,
    allowPythonOrArrow: Boolean = false,
    allowRangePartitioning: Boolean = false,
    allowMultiPartitionRoundRobin: Boolean = false,
    allowPipelinedShuffle: Boolean = false,
    allowPushBasedShuffle: Boolean = false,
    allowMergedShuffle: Boolean = false,
    allowIncompatibleRuntimeFlags: Boolean = false) {

  require(name.nonEmpty, "rule-set name must be non-empty")
  require(version > 0, "rule-set version must be positive")
}

private[sql] object ShuffleRecoveryEligibilityRules {
  import ShuffleRecoveryLineageDeterminism._
  import ShuffleRecoverySourceTokenAvailability._

  private val execution = "org.apache.spark.sql.execution."
  private val catalystExpressions = "org.apache.spark.sql.catalyst.expressions."
  private val aggregateExpressions = "org.apache.spark.sql.catalyst.expressions.aggregate."
  private val rangeExec = execution + "RangeExec"

  /**
   * Feasibility-only baseline. It is deliberately narrower than a production computation-identity
   * contract and must not be treated as a stable API or a claim that an allowlisted class is safe
   * outside the complete rule set below.
   */
  val conservative: ShuffleRecoveryEligibilityRules = ShuffleRecoveryEligibilityRules(
    name = "conservative-v1",
    version = 1,
    allowedOperatorClassNames = Set(
      execution + "CoalesceExec",
      execution + "CollectLimitExec",
      execution + "ColumnarToRowExec",
      execution + "ExpandExec",
      execution + "FilterExec",
      execution + "GenerateExec",
      execution + "GlobalLimitExec",
      execution + "InputAdapter",
      execution + "LocalLimitExec",
      execution + "LocalTableScanExec",
      execution + "ProjectExec",
      rangeExec,
      execution + "SampleExec",
      execution + "SortExec",
      execution + "TakeOrderedAndProjectExec",
      execution + "UnionExec",
      execution + "WholeStageCodegenExec",
      execution + "aggregate.HashAggregateExec",
      execution + "aggregate.ObjectHashAggregateExec",
      execution + "aggregate.SortAggregateExec",
      execution + "columnar.InMemoryTableScanExec",
      execution + "FileSourceScanExec",
      execution + "datasources.v2.BatchScanExec",
      execution + "exchange.BroadcastExchangeExec",
      execution + "exchange.ShuffleExchangeExec",
      execution + "joins.BroadcastHashJoinExec",
      execution + "joins.BroadcastNestedLoopJoinExec",
      execution + "joins.CartesianProductExec",
      execution + "joins.ShuffledHashJoinExec",
      execution + "joins.SortMergeJoinExec",
      execution + "python.AggregateInPandasExec",
      execution + "python.ArrowEvalPythonExec",
      execution + "python.BatchEvalPythonExec",
      execution + "python.CoGroupMapInPandasExec",
      execution + "python.FlatMapGroupsInPandasExec",
      execution + "python.MapInPandasExec",
      execution + "python.WindowInPandasExec",
      execution + "window.WindowExec",
      execution + "window.WindowGroupLimitExec"),
    allowedExpressionClassNames = Set(
      catalystExpressions + "Add",
      catalystExpressions + "Alias",
      catalystExpressions + "And",
      catalystExpressions + "AttributeReference",
      catalystExpressions + "BoundReference",
      catalystExpressions + "CaseWhen",
      catalystExpressions + "Cast",
      catalystExpressions + "Coalesce",
      catalystExpressions + "Concat",
      catalystExpressions + "ConcatWs",
      catalystExpressions + "Contains",
      catalystExpressions + "Divide",
      catalystExpressions + "DynamicPruningExpression",
      catalystExpressions + "EndsWith",
      catalystExpressions + "EqualNullSafe",
      catalystExpressions + "EqualTo",
      catalystExpressions + "GreaterThan",
      catalystExpressions + "GreaterThanOrEqual",
      catalystExpressions + "If",
      catalystExpressions + "In",
      catalystExpressions + "InSet",
      catalystExpressions + "IsNotNull",
      catalystExpressions + "IsNull",
      catalystExpressions + "KnownNotNull",
      catalystExpressions + "Length",
      catalystExpressions + "LessThan",
      catalystExpressions + "LessThanOrEqual",
      catalystExpressions + "Like",
      catalystExpressions + "Literal",
      catalystExpressions + "Lower",
      catalystExpressions + "Multiply",
      catalystExpressions + "Murmur3Hash",
      catalystExpressions + "Not",
      catalystExpressions + "Or",
      catalystExpressions + "Pmod",
      catalystExpressions + "PythonUDF",
      catalystExpressions + "RLike",
      catalystExpressions + "Remainder",
      catalystExpressions + "SortOrder",
      catalystExpressions + "StartsWith",
      catalystExpressions + "Substring",
      catalystExpressions + "Subtract",
      catalystExpressions + "UnaryMinus",
      catalystExpressions + "Upper",
      aggregateExpressions + "AggregateExpression",
      aggregateExpressions + "Average",
      aggregateExpressions + "Count",
      aggregateExpressions + "First",
      aggregateExpressions + "Last",
      aggregateExpressions + "Max",
      aggregateExpressions + "Min",
      aggregateExpressions + "Sum",
      execution + "InSubqueryExec",
      execution + "ScalarSubquery"),
    allowedPartitioningClassNames = Set(
      "org.apache.spark.sql.catalyst.plans.physical.HashPartitioning",
      "org.apache.spark.sql.catalyst.plans.physical.NullAwareHashPartitioning",
      "org.apache.spark.sql.catalyst.plans.physical.RangePartitioning",
      "org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning",
      "org.apache.spark.sql.catalyst.plans.physical.SinglePartition$"),
    sourceTokenByOperatorClassName = Map(rangeExec -> PrototypeSpecialCased),
    acceptedSourceTokenCategories = Set(Exact, PrototypeSpecialCased),
    lineageBySourceOperatorClassName = Map(rangeExec -> Determinate))
}

private[sql] case class ShuffleRecoveryRuntimeState(
    pushBasedShuffleEnabled: Boolean = false,
    mergedShuffleEnabled: Boolean = false,
    incompatibleFlags: Seq[String] = Nil)

private[sql] case class ShuffleRecoveryObservationFlags(
    dynamicPruning: Boolean = false,
    runtimeFilter: Boolean = false,
    subquery: Boolean = false,
    window: Boolean = false,
    expand: Boolean = false,
    cacheScan: Boolean = false,
    adaptivePartitionSpecs: Boolean = false,
    pythonOrArrow: Boolean = false,
    reusedExchange: Boolean = false,
    adaptivePlan: Boolean = false) {

  def merge(other: ShuffleRecoveryObservationFlags): ShuffleRecoveryObservationFlags = {
    ShuffleRecoveryObservationFlags(
      dynamicPruning = dynamicPruning || other.dynamicPruning,
      runtimeFilter = runtimeFilter || other.runtimeFilter,
      subquery = subquery || other.subquery,
      window = window || other.window,
      expand = expand || other.expand,
      cacheScan = cacheScan || other.cacheScan,
      adaptivePartitionSpecs = adaptivePartitionSpecs || other.adaptivePartitionSpecs,
      pythonOrArrow = pythonOrArrow || other.pythonOrArrow,
      reusedExchange = reusedExchange || other.reusedExchange,
      adaptivePlan = adaptivePlan || other.adaptivePlan)
  }
}

private[sql] case class ShuffleRecoveryExchangeObservation(
    executionId: String,
    exchangeOrdinal: Long,
    exchangePath: String,
    childOperatorClass: String,
    partitioningClass: String,
    partitionCount: Option[Int],
    ruleSetName: String,
    ruleSetVersion: Int,
    eligible: Boolean,
    immediateMissReason: Option[ShuffleRecoveryMissReason],
    rootMissReason: Option[ShuffleRecoveryMissReason],
    sourceTokenAvailability: ShuffleRecoverySourceTokenAvailability,
    lineageDeterminism: ShuffleRecoveryLineageDeterminism,
    flags: ShuffleRecoveryObservationFlags,
    pipelinedShuffle: Boolean,
    pushBasedShuffleEnabled: Boolean,
    mergedShuffleEnabled: Boolean,
    incompatibleRuntimeFlags: Seq[String],
    mapperCount: Option[Int] = None,
    runtimeStageId: Option[Int] = None,
    runtimeShuffleId: Option[Int] = None) {

  def toJson: String = {
    val fields = Seq(
      "executionId" -> jsonString(executionId),
      "exchangeOrdinal" -> exchangeOrdinal.toString,
      "exchangePath" -> jsonString(exchangePath),
      "childOperatorClass" -> jsonString(childOperatorClass),
      "partitioningClass" -> jsonString(partitioningClass),
      "partitionCount" -> optionalInt(partitionCount),
      "ruleSetName" -> jsonString(ruleSetName),
      "ruleSetVersion" -> ruleSetVersion.toString,
      "eligible" -> eligible.toString,
      "immediateMissReason" -> optionalReason(immediateMissReason),
      "rootMissReason" -> optionalReason(rootMissReason),
      "sourceTokenAvailability" -> jsonString(sourceTokenAvailability.code),
      "lineageDeterminism" -> jsonString(lineageDeterminism.code),
      "dppPresent" -> flags.dynamicPruning.toString,
      "runtimeFilterPresent" -> flags.runtimeFilter.toString,
      "subqueryPresent" -> flags.subquery.toString,
      "windowPresent" -> flags.window.toString,
      "expandPresent" -> flags.expand.toString,
      "cacheScanPresent" -> flags.cacheScan.toString,
      "adaptivePartitionSpecsPresent" -> flags.adaptivePartitionSpecs.toString,
      "pythonOrArrowPresent" -> flags.pythonOrArrow.toString,
      "reusedExchange" -> flags.reusedExchange.toString,
      "adaptivePlan" -> flags.adaptivePlan.toString,
      "pipelinedShuffle" -> pipelinedShuffle.toString,
      "pushBasedShuffleEnabled" -> pushBasedShuffleEnabled.toString,
      "mergedShuffleEnabled" -> mergedShuffleEnabled.toString,
      "incompatibleRuntimeFlags" -> jsonStringArray(incompatibleRuntimeFlags),
      "mapperCount" -> optionalInt(mapperCount),
      "runtimeStageId" -> optionalInt(runtimeStageId),
      "runtimeShuffleId" -> optionalInt(runtimeShuffleId))
    fields.iterator.map { case (key, value) => s"${jsonString(key)}:$value" }
      .mkString("{", ",", "}")
  }

  private def optionalReason(reason: Option[ShuffleRecoveryMissReason]): String = {
    reason.map(r => jsonString(r.code)).getOrElse("null")
  }

  private def optionalInt(value: Option[Int]): String = value.map(_.toString).getOrElse("null")

  private def jsonStringArray(values: Seq[String]): String = {
    values.distinct.sorted.iterator.map(jsonString).mkString("[", ",", "]")
  }

  private def jsonString(value: String): String = {
    val builder = new StringBuilder(value.length + 2)
    builder.append('"')
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
    builder.append('"')
    builder.toString()
  }
}

/**
 * Side-effect-free feasibility analyzer for executed SQL physical plans.
 *
 * In particular, this code does not read ShuffleExchangeExec.shuffleDependency, numMappers,
 * shuffleId, inputRDD, metrics, or other lazy execution state. Doing so could construct a shuffle
 * dependency and RangePartitioning can perform sampling while that dependency is prepared. Runtime
 * stage/shuffle identifiers and mapper counts therefore remain unpopulated at this boundary and are
 * reserved for later SparkListener/event-log correlation.
 */
private[sql] object ShuffleRecoveryOpportunityAnalyzer {
  import ShuffleRecoveryLineageDeterminism._
  import ShuffleRecoveryMissReason._
  import ShuffleRecoverySourceTokenAvailability._

  private sealed trait ReasonOrigin
  private case object LocalPlanReason extends ReasonOrigin
  private case object NestedExchangeReason extends ReasonOrigin

  private case class ReasonSummary(
      immediate: Option[ShuffleRecoveryMissReason],
      root: Option[ShuffleRecoveryMissReason],
      origin: Option[ReasonOrigin])

  private object ReasonSummary {
    val Empty: ReasonSummary = ReasonSummary(None, None, None)

    def from(
        reasons: Seq[ShuffleRecoveryMissReason],
        origin: ReasonOrigin = LocalPlanReason): ReasonSummary = {
      ReasonSummary(reasons.headOption, rootReason(reasons), reasons.headOption.map(_ => origin))
    }

    def combine(summaries: Seq[ReasonSummary]): ReasonSummary = {
      val first = summaries.iterator.find(_.immediate.nonEmpty)
      ReasonSummary(
        first.flatMap(_.immediate),
        rootReason(summaries.iterator.flatMap(_.root).toSeq),
        first.flatMap(_.origin))
    }

    private def rootReason(
        reasons: Seq[ShuffleRecoveryMissReason]): Option[ShuffleRecoveryMissReason] = {
      reasons.zipWithIndex.sortBy { case (reason, index) => (reason.rootRank, index) }
        .headOption.map(_._1)
    }
  }

  private case class SourceSummary(
      hasSource: Boolean = false,
      token: ShuffleRecoverySourceTokenAvailability = Exact,
      lineage: ShuffleRecoveryLineageDeterminism = Determinate) {

    def merge(other: SourceSummary): SourceSummary = {
      SourceSummary(
        hasSource = hasSource || other.hasSource,
        token = if (token.severity >= other.token.severity) token else other.token,
        lineage = if (lineage.severity >= other.lineage.severity) lineage else other.lineage)
    }

    def tokenCategory: ShuffleRecoverySourceTokenAvailability = {
      if (hasSource) token else Unavailable
    }

    def lineageCategory: ShuffleRecoveryLineageDeterminism = {
      if (hasSource) lineage else Unknown
    }
  }

  private case class PlanSummary(
      reasons: ReasonSummary,
      flags: ShuffleRecoveryObservationFlags,
      sources: SourceSummary)

  private case class ExpressionSummary(
      reasons: ReasonSummary,
      flags: ShuffleRecoveryObservationFlags)

  private case class ChildRef(label: String, plan: SparkPlan)

  private case class TraversalFrame(
      plan: SparkPlan,
      pathReversed: List[String],
      ancestorFlags: ShuffleRecoveryObservationFlags)

  private case class ExchangeOccurrence(
      exchange: ShuffleExchangeExec,
      path: String,
      ancestorFlags: ShuffleRecoveryObservationFlags)

  private case class PartitioningObservation(
      className: String,
      count: Option[Int],
      expressionRoots: Seq[Expression],
      range: Boolean,
      multiPartitionRoundRobin: Boolean,
      knownShape: Boolean)

  private val pythonOrArrowPlanClassNames = Set(
    "org.apache.spark.sql.execution.python.AggregateInPandasExec",
    "org.apache.spark.sql.execution.python.ArrowEvalPythonExec",
    "org.apache.spark.sql.execution.python.BatchEvalPythonExec",
    "org.apache.spark.sql.execution.python.CoGroupMapInPandasExec",
    "org.apache.spark.sql.execution.python.FlatMapGroupsInPandasExec",
    "org.apache.spark.sql.execution.python.MapInPandasExec",
    "org.apache.spark.sql.execution.python.WindowInPandasExec")

  private val pythonOrArrowExpressionClassNames = Set(
    "org.apache.spark.sql.catalyst.expressions.PythonUDF",
    "org.apache.spark.sql.catalyst.expressions.PythonUDAF")

  private val runtimeFilterExpressionClassNames = Set(
    "org.apache.spark.sql.catalyst.expressions.BloomFilterMightContain")

  private val windowPlanClassNames = Set(
    "org.apache.spark.sql.execution.window.WindowExec",
    "org.apache.spark.sql.execution.window.WindowGroupLimitExec")

  private val cacheScanPlanClassNames = Set(
    "org.apache.spark.sql.execution.columnar.InMemoryTableScanExec")

  private val adaptiveReadPlanClassNames = Set(
    "org.apache.spark.sql.execution.adaptive.AQEShuffleReadExec")

  private val expandPlanClassNames = Set("org.apache.spark.sql.execution.ExpandExec")

  def analyze(
      plan: SparkPlan,
      executionId: String,
      rules: ShuffleRecoveryEligibilityRules,
      runtimeState: ShuffleRecoveryRuntimeState = ShuffleRecoveryRuntimeState())
      : Seq[ShuffleRecoveryExchangeObservation] = {
    require(plan != null, "plan must not be null")
    require(executionId != null && executionId.nonEmpty, "execution id must be non-empty")

    val summaries = buildSummaries(plan, rules, runtimeState)
    exchangeOccurrences(plan).zipWithIndex.map { case (occurrence, ordinal) =>
      classifyExchange(occurrence, executionId, ordinal.toLong, rules, runtimeState, summaries)
    }
  }

  private def classifyExchange(
      occurrence: ExchangeOccurrence,
      executionId: String,
      ordinal: Long,
      rules: ShuffleRecoveryEligibilityRules,
      runtimeState: ShuffleRecoveryRuntimeState,
      summaries: IdentityHashMap[SparkPlan, PlanSummary])
      : ShuffleRecoveryExchangeObservation = {
    val exchange = occurrence.exchange
    val childSummary = summaries.get(exchange.child)
    val partitioning = observePartitioning(exchange.outputPartitioning)
    val partitionExpressions = summarizeExpressions(partitioning.expressionRoots, rules)
    val localReasons = exchangeLocalReasons(
      exchange,
      partitioning,
      occurrence.ancestorFlags.adaptivePartitionSpecs,
      childSummary.sources.lineageCategory,
      rules,
      runtimeState,
      partitionExpressions.reasons)
    val reasons = currentExchangeReasons(localReasons, childSummary.reasons)
    val flags = childSummary.flags
      .merge(partitionExpressions.flags)
      .merge(occurrence.ancestorFlags)

    ShuffleRecoveryExchangeObservation(
      executionId = executionId,
      exchangeOrdinal = ordinal,
      exchangePath = occurrence.path,
      childOperatorClass = exchange.child.getClass.getName,
      partitioningClass = partitioning.className,
      partitionCount = partitioning.count,
      ruleSetName = rules.name,
      ruleSetVersion = rules.version,
      eligible = reasons.immediate.isEmpty,
      immediateMissReason = reasons.immediate,
      rootMissReason = reasons.root,
      sourceTokenAvailability = childSummary.sources.tokenCategory,
      lineageDeterminism = childSummary.sources.lineageCategory,
      flags = flags,
      pipelinedShuffle = exchange.pipelined,
      pushBasedShuffleEnabled = runtimeState.pushBasedShuffleEnabled,
      mergedShuffleEnabled = runtimeState.mergedShuffleEnabled,
      incompatibleRuntimeFlags = runtimeState.incompatibleFlags.distinct.sorted)
  }

  private def currentExchangeReasons(
      local: ReasonSummary,
      child: ReasonSummary): ReasonSummary = {
    if (local.immediate.nonEmpty) {
      ReasonSummary.combine(Seq(local, child))
    } else if (child.origin.contains(NestedExchangeReason)) {
      ReasonSummary(Some(UpstreamIneligible), child.root, Some(LocalPlanReason))
    } else {
      child
    }
  }

  private def buildSummaries(
      root: SparkPlan,
      rules: ShuffleRecoveryEligibilityRules,
      runtimeState: ShuffleRecoveryRuntimeState): IdentityHashMap[SparkPlan, PlanSummary] = {
    val summaries = new IdentityHashMap[SparkPlan, PlanSummary]()
    val states = new IdentityHashMap[SparkPlan, java.lang.Byte]()
    val stack = mutable.ArrayBuffer((root, false))

    while (stack.nonEmpty) {
      val (plan, expanded) = stack.remove(stack.length - 1)
      if (expanded) {
        if (!summaries.containsKey(plan)) {
          val childSummaries = effectiveChildren(plan).map(child => summaries.get(child.plan))
          summaries.put(plan, summarizePlan(plan, childSummaries, rules, runtimeState))
          states.put(plan, 2.toByte)
        }
      } else if (!states.containsKey(plan)) {
        states.put(plan, 1.toByte)
        stack += ((plan, true))
        val children = effectiveChildren(plan)
        var index = children.length - 1
        while (index >= 0) {
          if (!states.containsKey(children(index).plan)) {
            stack += ((children(index).plan, false))
          }
          index -= 1
        }
      }
    }
    summaries
  }

  private def summarizePlan(
      plan: SparkPlan,
      childSummaries: Seq[PlanSummary],
      rules: ShuffleRecoveryEligibilityRules,
      runtimeState: ShuffleRecoveryRuntimeState): PlanSummary = {
    val children = effectiveChildren(plan)
    val localFlags = planFlags(plan)
    val expressionSummary = plan match {
      case exchange: ShuffleExchangeExec =>
        summarizeExpressions(observePartitioning(exchange.outputPartitioning).expressionRoots, rules)
      case _ if isStructuralWrapper(plan) =>
        ExpressionSummary(ReasonSummary.Empty, ShuffleRecoveryObservationFlags())
      case _ => summarizeExpressions(plan.expressions, rules)
    }
    val flags = childSummaries.foldLeft(localFlags.merge(expressionSummary.flags)) {
      case (acc, summary) => acc.merge(summary.flags)
    }
    val sources = if (children.isEmpty) {
      sourceSummary(plan, rules)
    } else {
      childSummaries.foldLeft(SourceSummary()) { case (acc, summary) => acc.merge(summary.sources) }
    }

    val semanticReasons = plan match {
      case exchange: ShuffleExchangeExec =>
        exchangeLocalReasons(
          exchange,
          observePartitioning(exchange.outputPartitioning),
          adaptivePartitionSpecsPresent = false,
          sources.lineageCategory,
          rules,
          runtimeState,
          expressionSummary.reasons)
      case _ if isStructuralWrapper(plan) => ReasonSummary.Empty
      case _ => ReasonSummary.from(planReasons(plan, rules))
    }
    val sourceReasons =
      if (!isStructuralWrapper(plan) && children.isEmpty) {
        leafSourceReasons(sources, rules)
      } else {
        ReasonSummary.Empty
      }
    val childReasons = childSummaries.map(_.reasons)
    val combined = ReasonSummary.combine(
      Seq(semanticReasons, expressionSummary.reasons, sourceReasons) ++ childReasons)
    val reasons = plan match {
      case _: ShuffleExchangeExec if combined.immediate.nonEmpty =>
        combined.copy(origin = Some(NestedExchangeReason))
      case _ => combined
    }

    PlanSummary(reasons, flags, sources)
  }

  private def leafSourceReasons(
      source: SourceSummary,
      rules: ShuffleRecoveryEligibilityRules): ReasonSummary = {
    val reasons = mutable.ArrayBuffer.empty[ShuffleRecoveryMissReason]
    if (rules.requireDeterminateLineage) {
      source.lineageCategory match {
        case Determinate =>
        case Unknown => reasons += DeterminismUnproven
        case Unordered | Indeterminate => reasons += NonDeterministic
      }
    }
    if (!rules.acceptedSourceTokenCategories.contains(source.tokenCategory)) {
      reasons += SourceTokenUnavailable
    }
    ReasonSummary.from(reasons.toSeq)
  }

  private def exchangeLocalReasons(
      exchange: ShuffleExchangeExec,
      partitioning: PartitioningObservation,
      adaptivePartitionSpecsPresent: Boolean,
      lineage: ShuffleRecoveryLineageDeterminism,
      rules: ShuffleRecoveryEligibilityRules,
      runtimeState: ShuffleRecoveryRuntimeState,
      partitionExpressionReasons: ReasonSummary): ReasonSummary = {
    val reasons = mutable.ArrayBuffer.empty[ShuffleRecoveryMissReason]

    if ((exchange.pipelined && !rules.allowPipelinedShuffle) ||
        (runtimeState.pushBasedShuffleEnabled && !rules.allowPushBasedShuffle) ||
        (runtimeState.mergedShuffleEnabled && !rules.allowMergedShuffle)) {
      reasons += UnsupportedShuffleMode
    }
    if (runtimeState.incompatibleFlags.nonEmpty && !rules.allowIncompatibleRuntimeFlags) {
      reasons += IncompatibleRuntimeFlag
    }
    if (rules.requireDeterminateLineage) {
      lineage match {
        case Determinate =>
        case Unknown => reasons += DeterminismUnproven
        case Unordered | Indeterminate => reasons += NonDeterministic
      }
    }
    if (partitioning.multiPartitionRoundRobin && !rules.allowMultiPartitionRoundRobin) {
      reasons += NonDeterministic
    }
    if (partitioning.range && !rules.allowRangePartitioning) {
      reasons += RangePartitioningPresent
    }
    partitioning.count.foreach { count =>
      if (count <= 0) reasons += InvalidPartitionCount
    }
    if (adaptivePartitionSpecsPresent && !rules.allowAdaptivePartitionSpecs) {
      reasons += AdaptivePartitionSpecPresent
    }
    if (!partitioning.knownShape ||
        !rules.allowedPartitioningClassNames.contains(partitioning.className)) {
      reasons += UnsupportedPartitioning
    }

    ReasonSummary.combine(Seq(ReasonSummary.from(reasons.toSeq), partitionExpressionReasons))
  }

  private def observePartitioning(partitioning: Partitioning): PartitioningObservation = {
    val className = partitioning.getClass.getName
    partitioning match {
      case HashPartitioning(expressions, count) =>
        PartitioningObservation(className, Some(count), expressions, range = false,
          multiPartitionRoundRobin = false, knownShape = true)
      case NullAwareHashPartitioning(expressions, count) =>
        PartitioningObservation(className, Some(count), expressions, range = false,
          multiPartitionRoundRobin = false, knownShape = true)
      case RangePartitioning(ordering, count) =>
        PartitioningObservation(className, Some(count), ordering, range = true,
          multiPartitionRoundRobin = false, knownShape = true)
      case RoundRobinPartitioning(count) =>
        PartitioningObservation(className, Some(count), Nil, range = false,
          multiPartitionRoundRobin = count > 1, knownShape = true)
      case SinglePartition =>
        PartitioningObservation(className, Some(1), Nil, range = false,
          multiPartitionRoundRobin = false, knownShape = true)
      case UnknownPartitioning(count) =>
        PartitioningObservation(className, Some(count), Nil, range = false,
          multiPartitionRoundRobin = false, knownShape = false)
      case _ =>
        // Unknown partitioning implementations are untrusted semantics. In particular, do not read
        // numPartitions or other overridable members merely to enrich an ineligible record.
        PartitioningObservation(className, None, Nil, range = false,
          multiPartitionRoundRobin = false, knownShape = false)
    }
  }

  private def planReasons(
      plan: SparkPlan,
      rules: ShuffleRecoveryEligibilityRules): Seq[ShuffleRecoveryMissReason] = {
    val className = plan.getClass.getName
    val reasons = mutable.ArrayBuffer.empty[ShuffleRecoveryMissReason]

    if (pythonOrArrowPlanClassNames.contains(className) && !rules.allowPythonOrArrow) {
      reasons += PythonOrArrowPresent
    }
    if (windowPlanClassNames.contains(className) && !rules.allowWindow) {
      reasons += WindowPresent
    }
    if (expandPlanClassNames.contains(className) && !rules.allowExpand) {
      reasons += ExpandPresent
    }
    if (cacheScanPlanClassNames.contains(className) && !rules.allowCacheScan) {
      reasons += CacheScanPresent
    }
    if (adaptiveReadPlanClassNames.contains(className) && !rules.allowAdaptivePartitionSpecs) {
      reasons += AdaptivePartitionSpecPresent
    }
    if (!rules.allowedOperatorClassNames.contains(className)) {
      if (className.startsWith("org.apache.spark.sql.execution.")) {
        reasons += UnsupportedOperator
      } else {
        reasons += CustomOperator
      }
    }
    reasons.toSeq
  }

  private def summarizeExpressions(
      roots: Seq[Expression],
      rules: ShuffleRecoveryEligibilityRules): ExpressionSummary = {
    val stack = mutable.ArrayBuffer.empty[Expression]
    roots.reverseIterator.foreach(stack += _)
    val reasons = mutable.ArrayBuffer.empty[ShuffleRecoveryMissReason]
    var flags = ShuffleRecoveryObservationFlags()

    // Expression.deterministic recursively visits descendants. Check it once per top-level root,
    // then use the iterative walk below for class/category validation to avoid quadratic recursion.
    if (roots.exists(root => !root.deterministic)) {
      reasons += NonDeterministic
    }

    while (stack.nonEmpty) {
      val expression = stack.remove(stack.length - 1)
      val className = expression.getClass.getName
      val isDpp = expression.isInstanceOf[DynamicPruningExpression]
      val isSubquery = expression.isInstanceOf[ExecSubqueryExpression]
      val isRuntimeFilter = runtimeFilterExpressionClassNames.contains(className)
      val isPythonOrArrow = pythonOrArrowExpressionClassNames.contains(className)

      flags = flags.merge(ShuffleRecoveryObservationFlags(
        dynamicPruning = isDpp,
        runtimeFilter = isRuntimeFilter,
        subquery = isSubquery,
        pythonOrArrow = isPythonOrArrow))

      if (isPythonOrArrow && !rules.allowPythonOrArrow) reasons += PythonOrArrowPresent
      if (isDpp && !rules.allowDynamicPruning) reasons += DynamicPruningPresent
      if (isRuntimeFilter && !rules.allowRuntimeFilters) reasons += RuntimeFilterPresent
      if (isSubquery && !rules.allowSubqueries) reasons += SubqueryPresent
      if (!rules.allowedExpressionClassNames.contains(className)) reasons += UnsupportedExpression

      expression.children.reverseIterator.foreach(stack += _)
    }

    ExpressionSummary(ReasonSummary.from(reasons.toSeq), flags)
  }

  private def sourceSummary(
      plan: SparkPlan,
      rules: ShuffleRecoveryEligibilityRules): SourceSummary = {
    val className = plan.getClass.getName
    SourceSummary(
      hasSource = true,
      token = rules.sourceTokenByOperatorClassName.getOrElse(className, Unavailable),
      lineage = rules.lineageBySourceOperatorClassName.getOrElse(className, Unknown))
  }

  private def planFlags(plan: SparkPlan): ShuffleRecoveryObservationFlags = {
    val className = plan.getClass.getName
    ShuffleRecoveryObservationFlags(
      window = windowPlanClassNames.contains(className),
      expand = expandPlanClassNames.contains(className),
      cacheScan = cacheScanPlanClassNames.contains(className),
      adaptivePartitionSpecs = adaptiveReadPlanClassNames.contains(className),
      pythonOrArrow = pythonOrArrowPlanClassNames.contains(className),
      reusedExchange = plan.isInstanceOf[ReusedExchangeExec],
      adaptivePlan = plan.isInstanceOf[AdaptiveSparkPlanExec])
  }

  private def ancestorObservationFlags(plan: SparkPlan): ShuffleRecoveryObservationFlags = {
    val className = plan.getClass.getName
    ShuffleRecoveryObservationFlags(
      adaptivePartitionSpecs = adaptiveReadPlanClassNames.contains(className),
      reusedExchange = plan.isInstanceOf[ReusedExchangeExec],
      adaptivePlan = plan.isInstanceOf[AdaptiveSparkPlanExec])
  }

  private def exchangeOccurrences(plan: SparkPlan): Seq[ExchangeOccurrence] = {
    val result = mutable.ArrayBuffer.empty[ExchangeOccurrence]
    val stack = mutable.ArrayBuffer(
      TraversalFrame(plan, Nil, ShuffleRecoveryObservationFlags()))

    while (stack.nonEmpty) {
      val frame = stack.remove(stack.length - 1)
      frame.plan match {
        case exchange: ShuffleExchangeExec =>
          result += ExchangeOccurrence(
            exchange,
            pathString(frame.pathReversed),
            frame.ancestorFlags)
        case _ =>
      }

      val inheritedFlags = frame.ancestorFlags.merge(ancestorObservationFlags(frame.plan))
      val children = effectiveChildren(frame.plan)
      var index = children.length - 1
      while (index >= 0) {
        val child = children(index)
        stack += TraversalFrame(child.plan, child.label :: frame.pathReversed, inheritedFlags)
        index -= 1
      }
    }
    result.toSeq
  }

  private def pathString(pathReversed: List[String]): String = {
    if (pathReversed.isEmpty) "root" else pathReversed.reverseIterator.mkString(".")
  }

  private def effectiveChildren(plan: SparkPlan): Seq[ChildRef] = plan match {
    case adaptive: AdaptiveSparkPlanExec => Seq(ChildRef("c0", adaptive.executedPlan))
    case stage: QueryStageExec => Seq(ChildRef("c0", stage.plan))
    case reused: ReusedExchangeExec => Seq(ChildRef("c0", reused.child))
    case other =>
      val children = other.children.zipWithIndex.map { case (child, index) =>
        ChildRef(s"c$index", child)
      }
      val subqueries = other.subqueries.zipWithIndex.map { case (subquery, index) =>
        ChildRef(s"s$index", subquery)
      }
      children ++ subqueries
  }

  private def isStructuralWrapper(plan: SparkPlan): Boolean = plan match {
    case _: AdaptiveSparkPlanExec | _: QueryStageExec | _: ReusedExchangeExec => true
    case _ => false
  }
}

/**
 * Explicit, thread-safe adapter for evidence runs.
 *
 * Registration is never automatic, so an analyzer that is not installed has no query-path cost.
 * The listener reads only plan/configuration state and serializes the user-supplied sink because
 * QueryExecutionListener callbacks may arrive concurrently. Duplicate callbacks for the same
 * QueryExecution are ignored deterministically.
 */
private[sql] final class ShuffleRecoveryOpportunityListener(
    rules: ShuffleRecoveryEligibilityRules,
    onRecords: Seq[ShuffleRecoveryExchangeObservation] => Unit,
    onError: Throwable => Unit = _ => ())
  extends QueryExecutionListener with Logging {

  private val callbackLock = new Object
  private val seenExecutions = new ConcurrentHashMap[java.lang.Long, java.lang.Boolean]()

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = observe(qe)

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = observe(qe)

  private def observe(qe: QueryExecution): Unit = {
    val key = java.lang.Long.valueOf(qe.id)
    if (seenExecutions.putIfAbsent(key, java.lang.Boolean.TRUE) == null) {
      try {
        val sparkConf = qe.sparkSession.sparkContext.getConf
        val shuffleManager = sparkConf.get("spark.shuffle.manager", "sort")
        val runtimeState = ShuffleRecoveryRuntimeState(
          pushBasedShuffleEnabled = sparkConf.getBoolean("spark.shuffle.push.enabled", false),
          incompatibleFlags = if (shuffleManager == "sort") Nil else Seq("CUSTOM_SHUFFLE_MANAGER"))
        val executionId = f"query-${qe.id}%020d"
        val records = ShuffleRecoveryOpportunityAnalyzer.analyze(
          qe.executedPlan, executionId, rules, runtimeState)
        callbackLock.synchronized {
          onRecords(records)
        }
      } catch {
        case NonFatal(error) =>
          seenExecutions.remove(key)
          reportError(error)
      }
    }
  }

  private def reportError(error: Throwable): Unit = {
    logWarning("Shuffle recovery opportunity analysis failed; query execution is unaffected.", error)
    try {
      callbackLock.synchronized {
        onError(error)
      }
    } catch {
      case NonFatal(sinkError) =>
        logWarning("Shuffle recovery opportunity error callback failed.", sinkError)
    }
  }
}
