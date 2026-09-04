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
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{
  Partitioning, RangePartitioning, RoundRobinPartitioning}
import org.apache.spark.sql.execution.{ExecSubqueryExpression, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.util.QueryExecutionListener

/**
 * Stable miss reasons emitted by the shuffle recovery opportunity analyzer.
 */
private[sql] sealed trait ShuffleRecoveryMissReason {
  def code: String
}

private[sql] object ShuffleRecoveryMissReason {
  case object NonDeterministic extends ShuffleRecoveryMissReason {
    override val code: String = "NON_DETERMINATE"
  }
  case object UnsupportedOperator extends ShuffleRecoveryMissReason {
    override val code: String = "UNSUPPORTED_OPERATOR"
  }
  case object CustomOperator extends ShuffleRecoveryMissReason {
    override val code: String = "CUSTOM_OPERATOR"
  }
  case object UnsupportedExpression extends ShuffleRecoveryMissReason {
    override val code: String = "UNSUPPORTED_EXPRESSION"
  }
  case object UnsupportedPartitioning extends ShuffleRecoveryMissReason {
    override val code: String = "UNSUPPORTED_PARTITIONING"
  }
  case object RangePartitioningPresent extends ShuffleRecoveryMissReason {
    override val code: String = "RANGE_PARTITIONING"
  }
  case object SourceTokenUnavailable extends ShuffleRecoveryMissReason {
    override val code: String = "SOURCE_TOKEN_UNAVAILABLE"
  }
  case object DynamicPruningPresent extends ShuffleRecoveryMissReason {
    override val code: String = "DPP_PRESENT"
  }
  case object RuntimeFilterPresent extends ShuffleRecoveryMissReason {
    override val code: String = "RUNTIME_FILTER_PRESENT"
  }
  case object SubqueryPresent extends ShuffleRecoveryMissReason {
    override val code: String = "SUBQUERY_PRESENT"
  }
  case object WindowPresent extends ShuffleRecoveryMissReason {
    override val code: String = "WINDOW_PRESENT"
  }
  case object ExpandPresent extends ShuffleRecoveryMissReason {
    override val code: String = "EXPAND_PRESENT"
  }
  case object CacheScanPresent extends ShuffleRecoveryMissReason {
    override val code: String = "CACHE_SCAN_PRESENT"
  }
  case object AdaptivePartitionSpecPresent extends ShuffleRecoveryMissReason {
    override val code: String = "ADAPTIVE_PARTITION_SPEC"
  }
  case object PythonOrArrowPresent extends ShuffleRecoveryMissReason {
    override val code: String = "PYTHON_OR_ARROW"
  }
  case object UnsupportedShuffleMode extends ShuffleRecoveryMissReason {
    override val code: String = "UNSUPPORTED_SHUFFLE_MODE"
  }
  case object IncompatibleRuntimeFlag extends ShuffleRecoveryMissReason {
    override val code: String = "INCOMPATIBLE_RUNTIME_FLAG"
  }
}

private[sql] sealed trait ShuffleRecoverySourceTokenAvailability {
  def code: String
}

private[sql] object ShuffleRecoverySourceTokenAvailability {
  case object Exact extends ShuffleRecoverySourceTokenAvailability {
    override val code: String = "EXACT"
  }
  case object Unavailable extends ShuffleRecoverySourceTokenAvailability {
    override val code: String = "UNAVAILABLE"
  }
  case object PrototypeSpecialCased extends ShuffleRecoverySourceTokenAvailability {
    override val code: String = "PROTOTYPE_SPECIAL_CASED"
  }
}

/**
 * Conservative, parameterized rules used only to measure recovery opportunity.
 *
 * Exact class-name allowlists are intentional. A new subclass cannot silently become eligible
 * through a package prefix, reflection fallback, or an inherited implementation.
 */
private[sql] case class ShuffleRecoveryEligibilityRules(
    name: String,
    version: Int,
    allowedOperatorClassNames: Set[String],
    allowedExpressionClassNames: Set[String],
    allowedPartitioningClassNames: Set[String],
    sourceTokenByOperatorClassName: Map[String, ShuffleRecoverySourceTokenAvailability],
    acceptedSourceTokenCategories: Set[ShuffleRecoverySourceTokenAvailability],
    allowDynamicPruning: Boolean = false,
    allowRuntimeFilters: Boolean = false,
    allowSubqueries: Boolean = false,
    allowWindow: Boolean = false,
    allowExpand: Boolean = false,
    allowCacheScan: Boolean = false,
    allowAdaptivePartitionSpecs: Boolean = false,
    allowMultiPartitionRoundRobin: Boolean = false) {

  require(name.nonEmpty, "rule-set name must be non-empty")
  require(version > 0, "rule-set version must be positive")
}

private[sql] object ShuffleRecoveryEligibilityRules {
  import ShuffleRecoverySourceTokenAvailability._

  private val execution = "org.apache.spark.sql.execution."
  private val catalystExpressions = "org.apache.spark.sql.catalyst.expressions."
  private val aggregateExpressions =
    "org.apache.spark.sql.catalyst.expressions.aggregate."

  /**
   * Feasibility-only baseline. It is deliberately smaller than any proposed production
   * computation-identity contract and is expected to grow only through explicit evidence.
   */
  val conservative: ShuffleRecoveryEligibilityRules = ShuffleRecoveryEligibilityRules(
    name = "conservative-v1",
    version = 1,
    allowedOperatorClassNames = Set(
      execution + "ColumnarToRowExec",
      execution + "FilterExec",
      execution + "InputAdapter",
      execution + "LocalTableScanExec",
      execution + "ProjectExec",
      execution + "RDDScanExec",
      execution + "RangeExec",
      execution + "SortExec",
      execution + "WholeStageCodegenExec",
      execution + "aggregate.HashAggregateExec",
      execution + "aggregate.SortAggregateExec",
      execution + "columnar.InMemoryTableScanExec",
      execution + "FileSourceScanExec",
      execution + "datasources.v2.BatchScanExec",
      execution + "exchange.BroadcastExchangeExec",
      execution + "exchange.ShuffleExchangeExec",
      execution + "window.WindowExec",
      execution + "window.WindowGroupLimitExec",
      execution + "ExpandExec",
      execution + "adaptive.AQEShuffleReadExec"),
    allowedExpressionClassNames = Set(
      catalystExpressions + "Add",
      catalystExpressions + "Alias",
      catalystExpressions + "And",
      catalystExpressions + "AttributeReference",
      catalystExpressions + "CaseWhen",
      catalystExpressions + "Cast",
      catalystExpressions + "Coalesce",
      catalystExpressions + "Divide",
      catalystExpressions + "DynamicPruningExpression",
      catalystExpressions + "BloomFilterMightContain",
      catalystExpressions + "EqualNullSafe",
      catalystExpressions + "EqualTo",
      catalystExpressions + "GreaterThan",
      catalystExpressions + "GreaterThanOrEqual",
      catalystExpressions + "If",
      catalystExpressions + "In",
      catalystExpressions + "InSet",
      catalystExpressions + "IsNotNull",
      catalystExpressions + "IsNull",
      catalystExpressions + "LessThan",
      catalystExpressions + "LessThanOrEqual",
      catalystExpressions + "Literal",
      catalystExpressions + "Multiply",
      catalystExpressions + "Not",
      catalystExpressions + "Or",
      catalystExpressions + "Pmod",
      catalystExpressions + "Remainder",
      catalystExpressions + "SortOrder",
      catalystExpressions + "Subtract",
      catalystExpressions + "UnaryMinus",
      catalystExpressions + "Murmur3Hash",
      aggregateExpressions + "AggregateExpression",
      aggregateExpressions + "Average",
      aggregateExpressions + "Count",
      aggregateExpressions + "First",
      aggregateExpressions + "Last",
      aggregateExpressions + "Max",
      aggregateExpressions + "Min",
      aggregateExpressions + "Sum"),
    allowedPartitioningClassNames = Set(
      "org.apache.spark.sql.catalyst.plans.physical.HashPartitioning",
      "org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning",
      "org.apache.spark.sql.catalyst.plans.physical.SinglePartition$"),
    sourceTokenByOperatorClassName = Map(
      execution + "RangeExec" -> PrototypeSpecialCased),
    acceptedSourceTokenCategories = Set(Exact, PrototypeSpecialCased))
}

private[sql] case class ShuffleRecoveryRuntimeState(
    pushBasedShuffleEnabled: Boolean = false,
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
    reusedExchange: Boolean = false) {

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
      reusedExchange = reusedExchange || other.reusedExchange)
  }
}

private[sql] case class ShuffleRecoveryExchangeObservation(
    executionId: String,
    exchangeOrdinal: Long,
    exchangePath: String,
    childOperatorClass: String,
    partitioningClass: String,
    partitionCount: Int,
    ruleSetName: String,
    ruleSetVersion: Int,
    eligible: Boolean,
    immediateMissReason: Option[ShuffleRecoveryMissReason],
    rootMissReason: Option[ShuffleRecoveryMissReason],
    sourceTokenAvailability: ShuffleRecoverySourceTokenAvailability,
    flags: ShuffleRecoveryObservationFlags,
    pipelinedShuffle: Boolean,
    pushBasedShuffleEnabled: Boolean,
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
      "partitionCount" -> partitionCount.toString,
      "ruleSetName" -> jsonString(ruleSetName),
      "ruleSetVersion" -> ruleSetVersion.toString,
      "eligible" -> eligible.toString,
      "immediateMissReason" -> optionalReason(immediateMissReason),
      "rootMissReason" -> optionalReason(rootMissReason),
      "sourceTokenAvailability" -> jsonString(sourceTokenAvailability.code),
      "dppPresent" -> flags.dynamicPruning.toString,
      "runtimeFilterPresent" -> flags.runtimeFilter.toString,
      "subqueryPresent" -> flags.subquery.toString,
      "windowPresent" -> flags.window.toString,
      "expandPresent" -> flags.expand.toString,
      "cacheScanPresent" -> flags.cacheScan.toString,
      "adaptivePartitionSpecsPresent" -> flags.adaptivePartitionSpecs.toString,
      "pythonOrArrowPresent" -> flags.pythonOrArrow.toString,
      "reusedExchange" -> flags.reusedExchange.toString,
      "pipelinedShuffle" -> pipelinedShuffle.toString,
      "pushBasedShuffleEnabled" -> pushBasedShuffleEnabled.toString,
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
    values.sorted.iterator.map(jsonString).mkString("[", ",", "]")
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
      case c if c < ' ' =>
        builder.append(f"\\u${c.toInt}%04x")
      case c => builder.append(c)
    }
    builder.append('"')
    builder.toString()
  }
}

private[sql] object ShuffleRecoveryOpportunityAnalyzer {
  import ShuffleRecoveryMissReason._
  import ShuffleRecoverySourceTokenAvailability._

  private case class SourceSummary(
      hasSource: Boolean = false,
      hasUnavailable: Boolean = false,
      hasPrototypeSpecialCase: Boolean = false) {

    def merge(other: SourceSummary): SourceSummary = {
      SourceSummary(
        hasSource = hasSource || other.hasSource,
        hasUnavailable = hasUnavailable || other.hasUnavailable,
        hasPrototypeSpecialCase =
          hasPrototypeSpecialCase || other.hasPrototypeSpecialCase)
    }

    def category: ShuffleRecoverySourceTokenAvailability = {
      if (!hasSource || hasUnavailable) {
        Unavailable
      } else if (hasPrototypeSpecialCase) {
        PrototypeSpecialCased
      } else {
        Exact
      }
    }
  }

  private case class PlanSummary(
      immediateReason: Option[ShuffleRecoveryMissReason],
      rootReason: Option[ShuffleRecoveryMissReason],
      flags: ShuffleRecoveryObservationFlags,
      sources: SourceSummary)

  private case class ExpressionSummary(
      reason: Option[ShuffleRecoveryMissReason],
      flags: ShuffleRecoveryObservationFlags)

  private val pythonOrArrowPlanClassNames = Set(
    "org.apache.spark.sql.execution.python.ArrowEvalPythonExec",
    "org.apache.spark.sql.execution.python.BatchEvalPythonExec",
    "org.apache.spark.sql.execution.python.MapInBatchExec",
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

  private val expandPlanClassNames = Set(
    "org.apache.spark.sql.execution.ExpandExec")

  def analyze(
      plan: SparkPlan,
      executionId: String,
      rules: ShuffleRecoveryEligibilityRules,
      runtimeState: ShuffleRecoveryRuntimeState = ShuffleRecoveryRuntimeState())
      : Seq[ShuffleRecoveryExchangeObservation] = {
    require(plan != null, "plan must not be null")
    require(executionId != null && executionId.nonEmpty, "execution id must be non-empty")

    val summaries = buildSummaries(plan, rules, runtimeState)
    val records = mutable.ArrayBuffer.empty[ShuffleRecoveryExchangeObservation]
    val stack = mutable.ArrayBuffer(
      (plan, List.empty[Int], ShuffleRecoveryObservationFlags()))
    var ordinal = 0L

    while (stack.nonEmpty) {
      val (current, pathReversed, ancestorFlags) = stack.remove(stack.length - 1)
      current match {
        case exchange: ShuffleExchangeExec =>
          records += classifyExchange(
            exchange,
            executionId,
            ordinal,
            pathReversed,
            ancestorFlags,
            rules,
            runtimeState,
            summaries)
          ordinal += 1L
        case _ =>
      }

      val inheritedFlags = ancestorFlags.merge(ancestorObservationFlags(current))
      val children = effectiveChildren(current)
      var index = children.length - 1
      while (index >= 0) {
        stack += ((children(index), index :: pathReversed, inheritedFlags))
        index -= 1
      }
    }

    records.toSeq
  }

  private def classifyExchange(
      exchange: ShuffleExchangeExec,
      executionId: String,
      ordinal: Long,
      pathReversed: List[Int],
      ancestorFlags: ShuffleRecoveryObservationFlags,
      rules: ShuffleRecoveryEligibilityRules,
      runtimeState: ShuffleRecoveryRuntimeState,
      summaries: IdentityHashMap[SparkPlan, PlanSummary])
      : ShuffleRecoveryExchangeObservation = {
    val childSummary = summaries.get(exchange.child)
    val partitioning = exchange.outputPartitioning
    val partitioningClass = partitioning.getClass.getName
    val partitionExpressionSummary = summarizePartitioningExpressions(partitioning, rules)
    val exchangeReason = exchangeLocalReason(
      exchange,
      ancestorFlags.adaptivePartitionSpecs,
      rules,
      runtimeState,
      partitionExpressionSummary.reason)

    val immediateReason = exchangeReason.orElse(childSummary.immediateReason)
    val rootReason = exchangeReason.orElse(childSummary.rootReason)
    val flags = childSummary.flags
      .merge(partitionExpressionSummary.flags)
      .merge(ancestorFlags)

    ShuffleRecoveryExchangeObservation(
      executionId = executionId,
      exchangeOrdinal = ordinal,
      exchangePath = pathReversed.reverseIterator.mkString("."),
      childOperatorClass = exchange.child.getClass.getName,
      partitioningClass = partitioningClass,
      partitionCount = partitioning.numPartitions,
      ruleSetName = rules.name,
      ruleSetVersion = rules.version,
      eligible = immediateReason.isEmpty,
      immediateMissReason = immediateReason,
      rootMissReason = rootReason,
      sourceTokenAvailability = childSummary.sources.category,
      flags = flags,
      pipelinedShuffle = exchange.pipelined,
      pushBasedShuffleEnabled = runtimeState.pushBasedShuffleEnabled,
      incompatibleRuntimeFlags = runtimeState.incompatibleFlags.distinct.sorted)
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
          val children = effectiveChildren(plan)
          val childSummaries = children.map(summaries.get)
          summaries.put(plan, summarizePlan(plan, childSummaries, rules, runtimeState))
          states.put(plan, 2.toByte)
        }
      } else if (!states.containsKey(plan)) {
        states.put(plan, 1.toByte)
        stack += ((plan, true))
        val children = effectiveChildren(plan)
        var index = children.length - 1
        while (index >= 0) {
          if (!states.containsKey(children(index))) {
            stack += ((children(index), false))
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
        summarizePartitioningExpressions(exchange.outputPartitioning, rules)
      case _ if isStructuralWrapper(plan) =>
        ExpressionSummary(None, ShuffleRecoveryObservationFlags())
      case _ =>
        summarizeExpressions(plan.expressions, rules)
    }
    val flags = childSummaries.foldLeft(localFlags.merge(expressionSummary.flags)) {
      case (acc, summary) => acc.merge(summary.flags)
    }
    val sources = if (children.isEmpty) {
      sourceSummary(plan, rules)
    } else {
      childSummaries.foldLeft(SourceSummary()) {
        case (acc, summary) => acc.merge(summary.sources)
      }
    }

    val semanticReason = plan match {
      case exchange: ShuffleExchangeExec =>
        exchangeLocalReason(
          exchange,
          adaptivePartitionSpecsPresent = false,
          rules,
          runtimeState,
          expressionSummary.reason)
      case _ if isStructuralWrapper(plan) =>
        None
      case _ =>
        planReason(plan, rules).orElse(expressionSummary.reason)
    }
    val sourceReason =
      if (!isStructuralWrapper(plan) && children.isEmpty &&
          !rules.acceptedSourceTokenCategories.contains(sources.category)) {
        Some(SourceTokenUnavailable)
      } else {
        None
      }

    val childImmediate = childSummaries.iterator.flatMap(_.immediateReason).take(1).toSeq.headOption
    val childRoot = childSummaries.iterator.flatMap(_.rootReason).take(1).toSeq.headOption
    val immediate = semanticReason.orElse(sourceReason).orElse(childImmediate)
    val root = semanticReason match {
      case Some(UnsupportedOperator) | Some(CustomOperator) | Some(UnsupportedExpression) =>
        sourceReason.orElse(childRoot).orElse(semanticReason)
      case Some(_) => semanticReason
      case None => sourceReason.orElse(childRoot)
    }

    PlanSummary(immediate, root, flags, sources)
  }

  private def summarizePartitioningExpressions(
      partitioning: Partitioning,
      rules: ShuffleRecoveryEligibilityRules): ExpressionSummary = {
    partitioning match {
      case expression: Expression =>
        summarizeExpressions(expression.children, rules)
      case _ =>
        ExpressionSummary(None, ShuffleRecoveryObservationFlags())
    }
  }

  private def exchangeLocalReason(
      exchange: ShuffleExchangeExec,
      adaptivePartitionSpecsPresent: Boolean,
      rules: ShuffleRecoveryEligibilityRules,
      runtimeState: ShuffleRecoveryRuntimeState,
      partitionExpressionReason: Option[ShuffleRecoveryMissReason])
      : Option[ShuffleRecoveryMissReason] = {
    val partitioning = exchange.outputPartitioning
    if (exchange.pipelined || runtimeState.pushBasedShuffleEnabled) {
      Some(UnsupportedShuffleMode)
    } else if (runtimeState.incompatibleFlags.nonEmpty) {
      Some(IncompatibleRuntimeFlag)
    } else if (partitioning.isInstanceOf[RoundRobinPartitioning] &&
        partitioning.numPartitions > 1 && !rules.allowMultiPartitionRoundRobin) {
      // Spark's multi-partition round-robin path can become order-sensitive when its input order
      // is not stable. The plan-only analyzer cannot safely materialize RDDs merely to prove
      // DeterministicLevel, so the conservative rule set refuses this shape.
      Some(NonDeterministic)
    } else if (partitioning.isInstanceOf[RangePartitioning]) {
      Some(RangePartitioningPresent)
    } else if (partitioning.numPartitions <= 0) {
      Some(UnsupportedPartitioning)
    } else if (adaptivePartitionSpecsPresent && !rules.allowAdaptivePartitionSpecs) {
      Some(AdaptivePartitionSpecPresent)
    } else if (!rules.allowedPartitioningClassNames.contains(partitioning.getClass.getName)) {
      Some(UnsupportedPartitioning)
    } else {
      partitionExpressionReason
    }
  }

  private def planReason(
      plan: SparkPlan,
      rules: ShuffleRecoveryEligibilityRules): Option[ShuffleRecoveryMissReason] = {
    val className = plan.getClass.getName
    if (pythonOrArrowPlanClassNames.contains(className)) {
      Some(PythonOrArrowPresent)
    } else if (windowPlanClassNames.contains(className) && !rules.allowWindow) {
      Some(WindowPresent)
    } else if (expandPlanClassNames.contains(className) && !rules.allowExpand) {
      Some(ExpandPresent)
    } else if (cacheScanPlanClassNames.contains(className) && !rules.allowCacheScan) {
      Some(CacheScanPresent)
    } else if (adaptiveReadPlanClassNames.contains(className) &&
        !rules.allowAdaptivePartitionSpecs) {
      Some(AdaptivePartitionSpecPresent)
    } else if (!rules.allowedOperatorClassNames.contains(className)) {
      if (className.startsWith("org.apache.spark.sql.execution.")) {
        Some(UnsupportedOperator)
      } else {
        Some(CustomOperator)
      }
    } else {
      None
    }
  }

  private def summarizeExpressions(
      roots: Seq[Expression],
      rules: ShuffleRecoveryEligibilityRules): ExpressionSummary = {
    val stack = mutable.ArrayBuffer.empty[Expression]
    roots.reverseIterator.foreach(stack += _)
    var reason: Option[ShuffleRecoveryMissReason] = None
    var flags = ShuffleRecoveryObservationFlags()

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

      if (reason.isEmpty) {
        reason =
          if (!expression.deterministic) {
            Some(NonDeterministic)
          } else if (isPythonOrArrow) {
            Some(PythonOrArrowPresent)
          } else if (isDpp && !rules.allowDynamicPruning) {
            Some(DynamicPruningPresent)
          } else if (isRuntimeFilter && !rules.allowRuntimeFilters) {
            Some(RuntimeFilterPresent)
          } else if (isSubquery && !rules.allowSubqueries) {
            Some(SubqueryPresent)
          } else if (!rules.allowedExpressionClassNames.contains(className)) {
            Some(UnsupportedExpression)
          } else {
            None
          }
      }

      expression.children.reverseIterator.foreach(stack += _)
    }

    ExpressionSummary(reason, flags)
  }

  private def sourceSummary(
      plan: SparkPlan,
      rules: ShuffleRecoveryEligibilityRules): SourceSummary = {
    rules.sourceTokenByOperatorClassName.get(plan.getClass.getName) match {
      case Some(Exact) =>
        SourceSummary(hasSource = true)
      case Some(PrototypeSpecialCased) =>
        SourceSummary(hasSource = true, hasPrototypeSpecialCase = true)
      case Some(Unavailable) | None =>
        SourceSummary(hasSource = true, hasUnavailable = true)
    }
  }

  private def planFlags(plan: SparkPlan): ShuffleRecoveryObservationFlags = {
    val className = plan.getClass.getName
    ShuffleRecoveryObservationFlags(
      window = windowPlanClassNames.contains(className),
      expand = expandPlanClassNames.contains(className),
      cacheScan = cacheScanPlanClassNames.contains(className),
      adaptivePartitionSpecs = adaptiveReadPlanClassNames.contains(className),
      pythonOrArrow = pythonOrArrowPlanClassNames.contains(className),
      reusedExchange = plan.isInstanceOf[ReusedExchangeExec])
  }

  private def ancestorObservationFlags(
      plan: SparkPlan): ShuffleRecoveryObservationFlags = {
    val className = plan.getClass.getName
    ShuffleRecoveryObservationFlags(
      adaptivePartitionSpecs = adaptiveReadPlanClassNames.contains(className),
      reusedExchange = plan.isInstanceOf[ReusedExchangeExec])
  }

  private def effectiveChildren(plan: SparkPlan): Seq[SparkPlan] = plan match {
    case adaptive: AdaptiveSparkPlanExec => Seq(adaptive.executedPlan)
    case stage: QueryStageExec => Seq(stage.plan)
    case reused: ReusedExchangeExec => Seq(reused.child)
    case other => other.children
  }

  private def isStructuralWrapper(plan: SparkPlan): Boolean = plan match {
    case _: AdaptiveSparkPlanExec | _: QueryStageExec | _: ReusedExchangeExec => true
    case _ => false
  }
}

/**
 * Thread-safe observation listener. It performs no filesystem, provider, scheduler, or tracker
 * mutation; callers explicitly register and unregister it with the session listener manager.
 */
private[sql] final class ShuffleRecoveryOpportunityListener(
    rules: ShuffleRecoveryEligibilityRules)
  extends QueryExecutionListener {

  private val nextExecutionId = new AtomicLong(0L)
  private val observations =
    new ConcurrentLinkedQueue[ShuffleRecoveryExchangeObservation]()

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    observe(qe)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    observe(qe)
  }

  def snapshot(): Seq[ShuffleRecoveryExchangeObservation] = {
    observations.iterator().asScala.toSeq.sortBy(record =>
      (record.executionId, record.exchangeOrdinal))
  }

  def clear(): Unit = observations.clear()

  private def observe(qe: QueryExecution): Unit = {
    val sequence = nextExecutionId.getAndIncrement()
    val executionId = f"query-$sequence%020d"
    val sparkConf = qe.sparkSession.sparkContext.getConf
    val runtimeState = ShuffleRecoveryRuntimeState(
      pushBasedShuffleEnabled = sparkConf.getBoolean("spark.shuffle.push.enabled", false))
    ShuffleRecoveryOpportunityAnalyzer
      .analyze(qe.executedPlan, executionId, rules, runtimeState)
      .foreach(observations.add)
  }
}
