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
import java.nio.file.{Files, Paths}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Ascending, Attribute, BloomFilterMightContain, Coalesce, DynamicPruningExpression, Expression,
  ExprId, Literal, PythonAggregate, PythonUDAF, PythonUDF, PythonUDTF, SortOrder,
  TranspiledPythonUDF, UnaryExpression, UnaryMinus, Unevaluable,
  UnresolvedPolymorphicPythonUDTF}
import org.apache.spark.sql.catalyst.plans.physical.{
  HashPartitioning, Partitioning, RangePartitioning, RoundRobinPartitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{
  ExpandExec, LeafExecNode, QueryExecution, ScalarSubquery, SparkPlan, UnaryExecNode, UnionExec}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, IntegerType, StructType}

class ShuffleRecoveryOpportunityAnalyzerSuite extends SharedSparkSession {
  import ShuffleRecoveryLineageDeterminism._
  import ShuffleRecoveryMissReason._
  import ShuffleRecoverySourceTokenAvailability._
  import testImplicits._

  private val rules = ShuffleRecoveryEligibilityRules.conservative

  private def rangePlan(): SparkPlan = {
    spark.range(0, 32, 1, 4).queryExecution.executedPlan
  }

  private def hashExchange(child: SparkPlan, partitions: Int = 4): ShuffleExchangeExec = {
    ShuffleExchangeExec(HashPartitioning(child.output.take(1), partitions), child)
  }

  private def analyze(
      plan: SparkPlan,
      executionId: String = "test",
      analyzerRules: ShuffleRecoveryEligibilityRules = rules,
      runtimeState: ShuffleRecoveryRuntimeState = ShuffleRecoveryRuntimeState())
      : Seq[ShuffleRecoveryExchangeObservation] = {
    ShuffleRecoveryOpportunityAnalyzer.analyze(plan, executionId, analyzerRules, runtimeState)
  }

  private def carrierRules(carrier: ExpressionCarrierExec): ShuffleRecoveryEligibilityRules = {
    rules.copy(
      allowedOperatorClassNames = rules.allowedOperatorClassNames + carrier.getClass.getName)
  }

  private def deepExpression(depth: Int): Expression = {
    (0 until depth).foldLeft[Expression](Literal(1)) { case (child, _) => UnaryMinus(child) }
  }

  test("zero exchanges produce no observation") {
    assert(analyze(rangePlan()).isEmpty)
  }

  test("a determinate hash exchange over the feasibility source is eligible") {
    val records = analyze(hashExchange(rangePlan()))

    assert(records.size === 1)
    val record = records.head
    assert(record.exchangeOrdinal === 0L)
    assert(record.exchangePath === "root")
    assert(record.partitionCount.contains(4))
    assert(record.eligible)
    assert(record.immediateMissReason.isEmpty)
    assert(record.rootMissReason.isEmpty)
    assert(record.sourceTokenAvailability === PrototypeSpecialCased)
    assert(record.lineageDeterminism === Determinate)
    assert(record.mapperCount.isEmpty)
    assert(record.runtimeStageId.isEmpty)
    assert(record.runtimeShuffleId.isEmpty)
  }

  test("unproven lineage fails closed without materializing shuffle state") {
    val source = TokenLeafExec(rangePlan().output)
    val sourceClass = source.getClass.getName
    val unprovenRules = rules.copy(
      allowedOperatorClassNames = rules.allowedOperatorClassNames + sourceClass,
      sourceTokenByOperatorClassName =
        rules.sourceTokenByOperatorClassName + (sourceClass -> Exact))

    val record = analyze(hashExchange(source), analyzerRules = unprovenRules).head

    assert(!record.eligible)
    assert(record.lineageDeterminism === Unknown)
    assert(record.immediateMissReason.contains(DeterminismUnproven))
    assert(record.rootMissReason.contains(DeterminismUnproven))
    assert(record.mapperCount.isEmpty)
  }

  test("nested exchanges propagate shuffle output determinism in stable occurrence order") {
    val inner = hashExchange(rangePlan(), partitions = 2)
    val outer = hashExchange(inner, partitions = 3)

    val strict = analyze(outer)

    assert(strict.map(_.exchangeOrdinal) === Seq(0L, 1L))
    assert(strict.map(_.exchangePath) === Seq("root", "c0"))
    assert(!strict.head.eligible)
    assert(strict.head.lineageDeterminism === Unordered)
    assert(strict.head.immediateMissReason.contains(NonDeterministic))
    assert(strict.head.rootMissReason.contains(NonDeterministic))
    assert(strict(1).eligible)
    assert(strict(1).lineageDeterminism === Determinate)

    val relaxed = analyze(
      outer,
      analyzerRules = rules.copy(requireDeterminateLineage = false))
    assert(relaxed.forall(_.eligible))
  }

  test("nested exchange misses use a cascade reason without losing the root cause") {
    val child = rangePlan()
    val inner = ShuffleExchangeExec(
      RangePartitioning(Seq(SortOrder(child.output.head, Ascending)), 2), child)
    val outer = hashExchange(inner, partitions = 3)
    val cascadeRules = rules.copy(requireDeterminateLineage = false)

    val records = analyze(outer, analyzerRules = cascadeRules)

    assert(records.head.immediateMissReason.contains(UpstreamIneligible))
    assert(records.head.rootMissReason.contains(RangePartitioningPresent))
    assert(records(1).immediateMissReason.contains(RangePartitioningPresent))
  }

  test("reused exchanges are unwrapped without object identity in the output") {
    val exchange = hashExchange(rangePlan())
    val reused = ReusedExchangeExec(exchange.output, exchange)

    val first = analyze(reused, executionId = "stable")
    val second = analyze(reused, executionId = "stable")

    assert(first.size === 1)
    assert(first.head.flags.reusedExchange)
    assert(first.map(_.toJson) === second.map(_.toJson))
  }

  test("unknown SparkPlan semantics fail closed") {
    val exchange = hashExchange(UnknownUnaryExec(rangePlan()))
    val record = analyze(exchange).head

    assert(!record.eligible)
    assert(record.immediateMissReason.contains(UnsupportedOperator))
    assert(record.rootMissReason.contains(UnsupportedOperator))
  }

  test("unknown expression semantics fail closed independently of operator allowlisting") {
    val carrier = ExpressionCarrierExec(UnknownExpression(Literal(1)), rangePlan())
    val record = analyze(hashExchange(carrier), analyzerRules = carrierRules(carrier)).head

    assert(!record.eligible)
    assert(record.immediateMissReason.contains(UnsupportedExpression))
    assert(record.rootMissReason.contains(UnsupportedExpression))
  }

  test("hostile unknown expressions are rejected before semantic access") {
    val carrier = ExpressionCarrierExec(HostileUnknownExpression(), rangePlan())
    val analyzerRules = carrierRules(carrier)
    val first = analyze(hashExchange(carrier), "hostile", analyzerRules).head
    val second = analyze(hashExchange(carrier), "hostile", analyzerRules).head

    assert(!first.eligible)
    assert(first.immediateMissReason.contains(UnsupportedExpression))
    assert(first.rootMissReason.contains(UnsupportedExpression))
    assert(first.toJson === second.toJson)
  }

  test("deep and wide trusted expression trees fail closed within explicit bounds") {
    val deep = ExpressionCarrierExec(
      deepExpression(ShuffleRecoveryOpportunityAnalyzer.maxExpressionDepth + 64),
      rangePlan())
    val deepRules = carrierRules(deep)
    val deepFirst = analyze(hashExchange(deep), "deep", deepRules).head
    val deepSecond = analyze(hashExchange(deep), "deep", deepRules).head

    assert(!deepFirst.eligible)
    assert(deepFirst.immediateMissReason.contains(DeterminismUnproven))
    assert(deepFirst.rootMissReason.contains(DeterminismUnproven))
    assert(deepFirst.toJson === deepSecond.toJson)

    val wide = ExpressionCarrierExec(
      Coalesce(Seq.fill(ShuffleRecoveryOpportunityAnalyzer.maxExpressionNodes + 1)(Literal(1))),
      rangePlan())
    val wideRecord = analyze(hashExchange(wide), analyzerRules = carrierRules(wide)).head
    assert(!wideRecord.eligible)
    assert(wideRecord.immediateMissReason.contains(DeterminismUnproven))
    assert(wideRecord.rootMissReason.contains(DeterminismUnproven))
  }

  test("shuffle partitioning expressions use the same hostile and bounded trust boundary") {
    val child = rangePlan()
    val hostileHash = ShuffleExchangeExec(
      HashPartitioning(Seq(HostileUnknownExpression()), 2), child)
    val hostileHashRecord = analyze(hostileHash).head
    assert(hostileHashRecord.immediateMissReason.contains(UnsupportedExpression))
    assert(hostileHashRecord.rootMissReason.contains(UnsupportedExpression))

    val hostileRange = ShuffleExchangeExec(
      RangePartitioning(Seq(SortOrder(HostileUnknownExpression(), Ascending)), 2), child)
    val hostileRangeRecord = analyze(
      hostileRange,
      analyzerRules = rules.copy(allowRangePartitioning = true)).head
    assert(hostileRangeRecord.immediateMissReason.contains(UnsupportedExpression))
    assert(hostileRangeRecord.rootMissReason.contains(UnsupportedExpression))

    val deepHash = ShuffleExchangeExec(
      HashPartitioning(
        Seq(deepExpression(ShuffleRecoveryOpportunityAnalyzer.maxExpressionDepth + 64)), 2),
      child)
    val deepHashFirst = analyze(deepHash, executionId = "partition-deep").head
    val deepHashSecond = analyze(deepHash, executionId = "partition-deep").head
    assert(deepHashFirst.immediateMissReason.contains(DeterminismUnproven))
    assert(deepHashFirst.rootMissReason.contains(DeterminismUnproven))
    assert(deepHashFirst.toJson === deepHashSecond.toJson)
  }

  test("frozen Python and Arrow families have negative-only dedicated classification") {
    val executionPrefix = "org.apache.spark.sql.execution.python."
    val frozenPlanFamilies = Seq(
      "ArrowAggregatePythonExec",
      "ArrowEvalPythonExec",
      "ArrowEvalPythonUDTFExec",
      "ArrowWindowPythonExec",
      "BatchEvalPythonExec",
      "BatchEvalPythonUDTFExec",
      "FlatMapCoGroupsInArrowExec",
      "FlatMapCoGroupsInBatchExec",
      "FlatMapCoGroupsInPandasExec",
      "FlatMapGroupsInArrowExec",
      "FlatMapGroupsInBatchExec",
      "FlatMapGroupsInPandasExec",
      "MapInArrowExec",
      "MapInBatchExec",
      "MapInPandasExec",
      "PythonIncrementalAggregateExec").map(executionPrefix + _)

    frozenPlanFamilies.foreach { className =>
      assert(
        ShuffleRecoveryOpportunityAnalyzer.isPythonOrArrowPlanClassName(className),
        s"missing Python/Arrow negative classification for $className")
    }
    assert(!ShuffleRecoveryOpportunityAnalyzer.isPythonOrArrowPlanClassName(
      executionPrefix + "AttachDistributedSequenceExec"))
    assert(!ShuffleRecoveryOpportunityAnalyzer.isPythonOrArrowPlanClassName(
      executionPrefix + "PythonWorkerLogsExec"))

    val pythonUdf = PythonUDF(
      name = "test",
      func = null,
      dataType = IntegerType,
      children = Seq(Literal(1)),
      evalType = 0,
      udfDeterministic = true)
    val pythonExpressions: Seq[Expression] = Seq(
      pythonUdf,
      PythonUDAF("test_udaf", null, IntegerType, Seq(Literal(1)), udfDeterministic = true),
      PythonAggregate(
        "test_aggregate",
        null,
        IntegerType,
        Seq(Literal(1)),
        udfDeterministic = true,
        bufferSchema = StructType(Nil)),
      PythonUDTF(
        "test_udtf",
        null,
        StructType(Nil),
        None,
        Seq(Literal(1)),
        evalType = 0,
        udfDeterministic = true),
      UnresolvedPolymorphicPythonUDTF(
        "test_unresolved_udtf",
        null,
        Seq(Literal(1)),
        evalType = 0,
        udfDeterministic = true,
        resolveElementMetadata = (_, _) =>
          throw new IllegalStateException(
            "Python UDTF analysis must not run in opportunity analysis")),
      TranspiledPythonUDF(
        name = "test_transpiled",
        pythonUDFExpr = pythonUdf,
        transpiledOptions = Nil))

    pythonExpressions.foreach { expression =>
      assert(ShuffleRecoveryOpportunityAnalyzer.isPythonOrArrowExpressionClassName(
        expression.getClass.getName))
      val carrier = ExpressionCarrierExec(expression, rangePlan())
      val strict = analyze(hashExchange(carrier), analyzerRules = carrierRules(carrier)).head
      assert(strict.flags.pythonOrArrow)
      assert(strict.immediateMissReason.contains(PythonOrArrowPresent))
      assert(strict.rootMissReason.contains(PythonOrArrowPresent))

      val relaxed = analyze(
        hashExchange(carrier),
        analyzerRules = carrierRules(carrier).copy(allowPythonOrArrow = true)).head
      assert(relaxed.flags.pythonOrArrow)
      assert(!relaxed.immediateMissReason.contains(PythonOrArrowPresent))
      assert(!relaxed.rootMissReason.contains(PythonOrArrowPresent))
      if (expression.getClass.getName == classOf[PythonUDF].getName) {
        assert(relaxed.eligible)
      } else {
        assert(relaxed.immediateMissReason.contains(UnsupportedExpression))
      }
    }
  }

  test("dynamic pruning is an independently parameterized miss") {
    val carrier = ExpressionCarrierExec(DynamicPruningExpression(Literal(true)), rangePlan())
    val analyzerRules = carrierRules(carrier)

    val refused = analyze(hashExchange(carrier), analyzerRules = analyzerRules).head
    val admitted = analyze(
      hashExchange(carrier),
      analyzerRules = analyzerRules.copy(allowDynamicPruning = true)).head

    assert(refused.immediateMissReason.contains(DynamicPruningPresent))
    assert(refused.flags.dynamicPruning)
    assert(admitted.eligible)
    assert(admitted.flags.dynamicPruning)
  }

  test("runtime filters are an independently parameterized miss") {
    val runtimeFilter = BloomFilterMightContain(Literal(Array[Byte](1)), Literal(1L))
    val carrier = ExpressionCarrierExec(runtimeFilter, rangePlan())
    val analyzerRules = carrierRules(carrier)

    val refused = analyze(hashExchange(carrier), analyzerRules = analyzerRules).head
    val admitted = analyze(
      hashExchange(carrier),
      analyzerRules = analyzerRules.copy(allowRuntimeFilters = true)).head

    assert(refused.immediateMissReason.contains(RuntimeFilterPresent))
    assert(refused.flags.runtimeFilter)
    assert(admitted.eligible)
    assert(admitted.flags.runtimeFilter)
  }

  test("source-token and lineage categories are independently parameterized") {
    val source = TokenLeafExec(rangePlan().output)
    val sourceClass = source.getClass.getName
    val determinateRules = rules.copy(
      allowedOperatorClassNames = rules.allowedOperatorClassNames + sourceClass,
      lineageBySourceOperatorClassName =
        rules.lineageBySourceOperatorClassName + (sourceClass -> Determinate))

    val unavailable = analyze(hashExchange(source), analyzerRules = determinateRules).head
    val exact = analyze(
      hashExchange(source),
      analyzerRules = determinateRules.copy(
        sourceTokenByOperatorClassName =
          determinateRules.sourceTokenByOperatorClassName + (sourceClass -> Exact))).head
    val prototype = analyze(
      hashExchange(source),
      analyzerRules = determinateRules.copy(
        sourceTokenByOperatorClassName =
          determinateRules.sourceTokenByOperatorClassName +
            (sourceClass -> PrototypeSpecialCased))).head

    assert(unavailable.immediateMissReason.contains(SourceTokenUnavailable))
    assert(unavailable.sourceTokenAvailability === Unavailable)
    assert(unavailable.lineageDeterminism === Determinate)
    assert(exact.eligible)
    assert(exact.sourceTokenAvailability === Exact)
    assert(prototype.eligible)
    assert(prototype.sourceTokenAvailability === PrototypeSpecialCased)
  }

  test("range partitioning is rejected without constructing shuffle runtime state") {
    val child = rangePlan()
    val partitioning = RangePartitioning(
      Seq(SortOrder(child.output.head, Ascending)), numPartitions = 2)
    val exchange = ShuffleExchangeExec(partitioning, child)

    val refused = analyze(exchange).head
    val admitted = analyze(
      exchange,
      analyzerRules = rules.copy(allowRangePartitioning = true)).head

    assert(refused.immediateMissReason.contains(RangePartitioningPresent))
    assert(refused.rootMissReason.contains(RangePartitioningPresent))
    assert(refused.mapperCount.isEmpty)
    assert(refused.runtimeShuffleId.isEmpty)
    assert(admitted.eligible)
  }

  test("non-deterministic expressions are rejected before generic expression misses") {
    val child = spark.range(32).selectExpr("rand() AS r").queryExecution.executedPlan
    val record = analyze(hashExchange(child)).head

    assert(!record.eligible)
    assert(record.immediateMissReason.contains(NonDeterministic))
    assert(record.rootMissReason.contains(NonDeterministic))
  }

  test("multi-partition round-robin is conservatively and independently rejected") {
    val child = rangePlan()
    val exchange = ShuffleExchangeExec(RoundRobinPartitioning(4), child)

    val refused = analyze(exchange).head
    val admitted = analyze(
      exchange,
      analyzerRules = rules.copy(allowMultiPartitionRoundRobin = true)).head

    assert(refused.immediateMissReason.contains(NonDeterministic))
    assert(admitted.eligible)
  }

  test("pipelined push and merged shuffle modes are independently parameterized") {
    val child = rangePlan()
    val pipelined = ShuffleExchangeExec(
      HashPartitioning(child.output.take(1), 2), child, pipelined = true)
    val ordinary = hashExchange(child, partitions = 2)

    assert(analyze(pipelined).head.immediateMissReason.contains(UnsupportedShuffleMode))
    assert(analyze(
      pipelined,
      analyzerRules = rules.copy(allowPipelinedShuffle = true)).head.eligible)

    val pushState = ShuffleRecoveryRuntimeState(pushBasedShuffleEnabled = true)
    assert(analyze(
      ordinary,
      runtimeState = pushState).head.immediateMissReason.contains(UnsupportedShuffleMode))
    assert(analyze(
      ordinary,
      analyzerRules = rules.copy(allowPushBasedShuffle = true),
      runtimeState = pushState).head.eligible)

    val mergedState = ShuffleRecoveryRuntimeState(mergedShuffleEnabled = true)
    assert(analyze(
      ordinary,
      runtimeState = mergedState).head.immediateMissReason.contains(UnsupportedShuffleMode))
    assert(analyze(
      ordinary,
      analyzerRules = rules.copy(allowMergedShuffle = true),
      runtimeState = mergedState).head.eligible)
  }

  test("incompatible runtime flags fail closed and remain separately parameterized") {
    val state = ShuffleRecoveryRuntimeState(incompatibleFlags = Seq("CUSTOM_SHUFFLE_MANAGER"))
    val exchange = hashExchange(rangePlan())

    val refused = analyze(exchange, runtimeState = state).head
    val admitted = analyze(
      exchange,
      analyzerRules = rules.copy(allowIncompatibleRuntimeFlags = true),
      runtimeState = state).head

    assert(refused.immediateMissReason.contains(IncompatibleRuntimeFlag))
    assert(admitted.eligible)
  }

  test("invalid and hostile partitioning shapes fail closed without unsafe enrichment") {
    val invalid = analyze(ShuffleExchangeExec(UnknownPartitioning(0), rangePlan())).head
    assert(!invalid.eligible)
    assert(invalid.partitionCount.contains(0))
    assert(invalid.immediateMissReason.contains(InvalidPartitionCount))

    val hostile = analyze(ShuffleExchangeExec(HostilePartitioning(), rangePlan())).head
    assert(!hostile.eligible)
    assert(hostile.partitionCount.isEmpty)
    assert(hostile.immediateMissReason.contains(UnsupportedPartitioning))
  }

  test("window presence is recorded independently from operator allowlisting") {
    val plan = hashExchange(WindowExec(Nil, Nil, Nil, rangePlan()))

    val refused = analyze(plan).head
    val admitted = analyze(plan, analyzerRules = rules.copy(allowWindow = true)).head

    assert(refused.flags.window)
    assert(refused.immediateMissReason.contains(WindowPresent))
    assert(refused.rootMissReason.contains(WindowPresent))
    assert(admitted.flags.window)
    assert(admitted.eligible)
    assert(admitted.immediateMissReason.isEmpty)
    assert(admitted.rootMissReason.isEmpty)
  }

  test("all independently configurable scope relaxations remove their dedicated blocker") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val child = rangePlan()
      val dppCarrier = ExpressionCarrierExec(DynamicPruningExpression(Literal(true)), child)
      val runtimeCarrier = ExpressionCarrierExec(
        BloomFilterMightContain(Literal(Array[Byte](1)), Literal(1L)), child)
      val subqueryCarrier = ExpressionCarrierExec(ScalarSubquery(null, ExprId(1)), child)
      val pythonCarrier = ExpressionCarrierExec(
        PythonUDF(
          "matrix_python",
          null,
          IntegerType,
          Seq(Literal(1)),
          evalType = 0,
          udfDeterministic = true),
        child)
      val window = WindowExec(Nil, Nil, Nil, child)
      val expand = ExpandExec(Seq(child.output), child.output, child)
      val range = ShuffleExchangeExec(
        RangePartitioning(Seq(SortOrder(child.output.head, Ascending)), 2), child)
      val roundRobin = ShuffleExchangeExec(RoundRobinPartitioning(4), child)
      val pipelined = ShuffleExchangeExec(
        HashPartitioning(child.output.take(1), 2), child, pipelined = true)
      val ordinary = hashExchange(child, partitions = 2)

      val source = TokenLeafExec(child.output)
      val sourceClass = source.getClass.getName
      val tokenRules = rules.copy(
        allowedOperatorClassNames = rules.allowedOperatorClassNames + sourceClass,
        lineageBySourceOperatorClassName =
          rules.lineageBySourceOperatorClassName + (sourceClass -> Determinate))
      val lineageRules = rules.copy(
        allowedOperatorClassNames = rules.allowedOperatorClassNames + sourceClass,
        sourceTokenByOperatorClassName =
          rules.sourceTokenByOperatorClassName + (sourceClass -> Exact))

      val cached = spark.range(8).cache()
      try {
        cached.count()
        val cachedPlan = cached.select($"id").queryExecution.executedPlan
        val cacheClass = "org.apache.spark.sql.execution.columnar.InMemoryTableScanExec"
        val cacheScan = cachedPlan.collectFirst {
          case plan if plan.getClass.getName == cacheClass => plan
        }.getOrElse(fail("expected an InMemoryTableScanExec fixture"))
        val cacheRules = rules.copy(
          sourceTokenByOperatorClassName =
            rules.sourceTokenByOperatorClassName + (cacheClass -> PrototypeSpecialCased),
          lineageBySourceOperatorClassName =
            rules.lineageBySourceOperatorClassName + (cacheClass -> Determinate))

        case class RuleCase(
            name: String,
            blocker: ShuffleRecoveryMissReason,
            strict: () => ShuffleRecoveryExchangeObservation,
            relaxed: () => ShuffleRecoveryExchangeObservation)

        val cases = Seq(
          RuleCase(
            "DPP",
            DynamicPruningPresent,
            () => analyze(hashExchange(dppCarrier), analyzerRules = carrierRules(dppCarrier)).head,
            () => analyze(
              hashExchange(dppCarrier),
              analyzerRules = carrierRules(dppCarrier).copy(allowDynamicPruning = true)).head),
          RuleCase(
            "runtime filter",
            RuntimeFilterPresent,
            () => analyze(
              hashExchange(runtimeCarrier), analyzerRules = carrierRules(runtimeCarrier)).head,
            () => analyze(
              hashExchange(runtimeCarrier),
              analyzerRules = carrierRules(runtimeCarrier).copy(allowRuntimeFilters = true)).head),
          RuleCase(
            "subquery",
            SubqueryPresent,
            () => analyze(
              hashExchange(subqueryCarrier), analyzerRules = carrierRules(subqueryCarrier)).head,
            () => analyze(
              hashExchange(subqueryCarrier),
              analyzerRules = carrierRules(subqueryCarrier).copy(allowSubqueries = true)).head),
          RuleCase(
            "Window",
            WindowPresent,
            () => analyze(hashExchange(window)).head,
            () => analyze(
              hashExchange(window),
              analyzerRules = rules.copy(allowWindow = true)).head),
          RuleCase(
            "Expand",
            ExpandPresent,
            () => analyze(hashExchange(expand)).head,
            () => analyze(
              hashExchange(expand),
              analyzerRules = rules.copy(allowExpand = true)).head),
          RuleCase(
            "cache scan",
            CacheScanPresent,
            () => analyze(hashExchange(cacheScan), analyzerRules = cacheRules).head,
            () => analyze(
              hashExchange(cacheScan),
              analyzerRules = cacheRules.copy(allowCacheScan = true)).head),
          RuleCase(
            "Python/Arrow",
            PythonOrArrowPresent,
            () => analyze(
              hashExchange(pythonCarrier), analyzerRules = carrierRules(pythonCarrier)).head,
            () => analyze(
              hashExchange(pythonCarrier),
              analyzerRules = carrierRules(pythonCarrier).copy(allowPythonOrArrow = true)).head),
          RuleCase(
            "RangePartitioning",
            RangePartitioningPresent,
            () => analyze(range).head,
            () => analyze(range, analyzerRules = rules.copy(allowRangePartitioning = true)).head),
          RuleCase(
            "multi-partition round robin",
            NonDeterministic,
            () => analyze(roundRobin).head,
            () => analyze(
              roundRobin,
              analyzerRules = rules.copy(allowMultiPartitionRoundRobin = true)).head),
          RuleCase(
            "pipelined shuffle",
            UnsupportedShuffleMode,
            () => analyze(pipelined).head,
            () => analyze(
              pipelined, analyzerRules = rules.copy(allowPipelinedShuffle = true)).head),
          RuleCase(
            "push-based shuffle",
            UnsupportedShuffleMode,
            () => analyze(
              ordinary,
              runtimeState = ShuffleRecoveryRuntimeState(pushBasedShuffleEnabled = true)).head,
            () => analyze(
              ordinary,
              analyzerRules = rules.copy(allowPushBasedShuffle = true),
              runtimeState = ShuffleRecoveryRuntimeState(pushBasedShuffleEnabled = true)).head),
          RuleCase(
            "merged shuffle",
            UnsupportedShuffleMode,
            () => analyze(
              ordinary,
              runtimeState = ShuffleRecoveryRuntimeState(mergedShuffleEnabled = true)).head,
            () => analyze(
              ordinary,
              analyzerRules = rules.copy(allowMergedShuffle = true),
              runtimeState = ShuffleRecoveryRuntimeState(mergedShuffleEnabled = true)).head),
          RuleCase(
            "incompatible runtime flags",
            IncompatibleRuntimeFlag,
            () => analyze(
              ordinary,
              runtimeState = ShuffleRecoveryRuntimeState(
                incompatibleFlags = Seq("CUSTOM_SHUFFLE_MANAGER"))).head,
            () => analyze(
              ordinary,
              analyzerRules = rules.copy(allowIncompatibleRuntimeFlags = true),
              runtimeState = ShuffleRecoveryRuntimeState(
                incompatibleFlags = Seq("CUSTOM_SHUFFLE_MANAGER"))).head),
          RuleCase(
            "source token",
            SourceTokenUnavailable,
            () => analyze(hashExchange(source), analyzerRules = tokenRules).head,
            () => analyze(
              hashExchange(source),
              analyzerRules = tokenRules.copy(
                sourceTokenByOperatorClassName =
                  tokenRules.sourceTokenByOperatorClassName + (sourceClass -> Exact))).head),
          RuleCase(
            "lineage",
            DeterminismUnproven,
            () => analyze(hashExchange(source), analyzerRules = lineageRules).head,
            () => analyze(
              hashExchange(source),
              analyzerRules = lineageRules.copy(requireDeterminateLineage = false)).head))

        cases.foreach { ruleCase =>
          val strict = ruleCase.strict()
          val relaxed = ruleCase.relaxed()
          assert(
            strict.immediateMissReason.contains(ruleCase.blocker) ||
              strict.rootMissReason.contains(ruleCase.blocker),
            s"${ruleCase.name} did not report ${ruleCase.blocker.code}: ${strict.toJson}")
          assert(
            !relaxed.immediateMissReason.contains(ruleCase.blocker) &&
              !relaxed.rootMissReason.contains(ruleCase.blocker),
            s"${ruleCase.name} blocker survived relaxation: ${relaxed.toJson}")
          assert(
            !relaxed.immediateMissReason.contains(UnsupportedOperator) &&
              !relaxed.rootMissReason.contains(UnsupportedOperator),
            s"${ruleCase.name} exposed an unrelated operator allowlist miss: ${relaxed.toJson}")
          assert(
            !relaxed.immediateMissReason.contains(UnsupportedExpression) &&
              !relaxed.rootMissReason.contains(UnsupportedExpression),
            s"${ruleCase.name} exposed an unrelated expression allowlist miss: ${relaxed.toJson}")
          assert(relaxed.eligible, s"${ruleCase.name} fixture was not isolated: ${relaxed.toJson}")
        }
      } finally {
        cached.unpersist(blocking = true)
      }
    }
  }

  test("empty query results remain observable without special-case mutation") {
    val df = spark.range(0).repartition(1)
    df.collect()

    val records = analyze(df.queryExecution.executedPlan, executionId = "empty")
    assert(records.nonEmpty)
    assert(records.forall(_.partitionCount.exists(_ > 0)))
  }

  test("AQE-on and AQE-off executed plans both expose shuffle observations") {
    val records = Seq(false, true).flatMap { adaptive =>
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = spark.range(0, 64, 1, 4).repartition(4, $"id")
        df.collect()
        val observed = analyze(
          df.queryExecution.executedPlan,
          executionId = if (adaptive) "aqe-on" else "aqe-off")
        assert(observed.nonEmpty)
        observed
      }
    }

    assert(records.map(_.executionId).toSet === Set("aqe-off", "aqe-on"))
    maybeWriteEvidence(records)
  }

  test("executed subquery plans participate in deterministic exchange discovery") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.sql(
        "SELECT id FROM range(4) WHERE id < (SELECT max(id) FROM range(8))")
      df.collect()
      val records = analyze(
        df.queryExecution.executedPlan,
        executionId = "subquery",
        analyzerRules = rules.copy(allowSubqueries = true))

      assert(records.nonEmpty)
      assert(records.exists(_.exchangePath.split("\\.").exists(_.startsWith("s"))))
    }
  }

  test("same plan produces byte-for-byte stable records for the same run identifier") {
    val plan = hashExchange(rangePlan())
    val first = analyze(plan, executionId = "stable").map(_.toJson)
    val second = analyze(plan, executionId = "stable").map(_.toJson)

    assert(first === second)
  }

  test("10,000 exchange occurrences stay ordered without identity deduplication") {
    val exchange = hashExchange(rangePlan(), partitions = 2)
    val plan = UnionExec(Seq.fill(10000)(exchange))

    val records = analyze(plan, executionId = "many")

    assert(records.size === 10000)
    assert(records.head.exchangeOrdinal === 0L)
    assert(records.last.exchangeOrdinal === 9999L)
    assert(records.map(_.exchangeOrdinal) === (0L until 10000L).toSeq)
    assert(records.map(_.exchangePath).distinct.size === 10000)
  }

  test("listener accounts hostile unsupported expressions instead of losing successful queries") {
    val baseline = spark.range(8).collect().toSeq
    assert(baseline === (0L until 8L).toSeq)

    val hostileCarrier = ExpressionCarrierExec(HostileUnknownExpression(), rangePlan())
    val hostilePlan = hashExchange(hostileCarrier)
    val captured = new ConcurrentLinkedQueue[Seq[ShuffleRecoveryExchangeObservation]]()
    val errors = new ConcurrentLinkedQueue[Throwable]()
    val listener = new ShuffleRecoveryOpportunityListener(
      carrierRules(hostileCarrier),
      batch => captured.add(batch),
      error => errors.add(error))
    val logical = spark.range(8).queryExecution.logical
    val qe = new QueryExecution(spark, logical) {
      override def executedPlan: SparkPlan = hostilePlan
    }

    listener.onSuccess("collect", qe, 0L)

    assert(errors.isEmpty)
    assert(captured.size() === 1)
    val records = captured.peek()
    assert(records.size === 1)
    assert(records.head.immediateMissReason.contains(UnsupportedExpression))
    assert(records.head.rootMissReason.contains(UnsupportedExpression))
    assert(spark.range(8).collect().toSeq === baseline)
  }

  test("listener registration is explicit and unregistering stops observations") {
    val captured = new ConcurrentLinkedQueue[Seq[ShuffleRecoveryExchangeObservation]]()
    val errors = new ConcurrentLinkedQueue[Throwable]()
    val listener = new ShuffleRecoveryOpportunityListener(
      rules,
      batch => captured.add(batch),
      error => errors.add(error))
    assert(!spark.listenerManager.listListeners().contains(listener))

    val baseline = spark.range(8).collect().toSeq
    assert(baseline === (0L until 8L).toSeq)

    spark.listenerManager.register(listener)
    try {
      spark.range(16).repartition(2, $"id").collect()
      spark.sparkContext.listenerBus.waitUntilEmpty()
      assert(!captured.isEmpty)
      assert(errors.isEmpty)
    } finally {
      spark.listenerManager.unregister(listener)
    }

    val observedCount = captured.size()
    spark.range(4).repartition(2, $"id").collect()
    spark.sparkContext.listenerBus.waitUntilEmpty()
    assert(captured.size() === observedCount)
  }

  test("duplicate concurrent listener callbacks are deterministic and deduplicated") {
    val captured = new ConcurrentLinkedQueue[Seq[ShuffleRecoveryExchangeObservation]]()
    val listener = new ShuffleRecoveryOpportunityListener(
      rules, batch => captured.add(batch))
    val qe = spark.range(16).repartition(2, $"id").queryExecution
    val executor = Executors.newFixedThreadPool(4)
    val start = new CountDownLatch(1)

    try {
      val futures = (0 until 16).map { _ =>
        executor.submit(new Runnable {
          override def run(): Unit = {
            start.await()
            listener.onSuccess("collect", qe, 0L)
          }
        })
      }
      start.countDown()
      futures.foreach(_.get(30, TimeUnit.SECONDS))

      assert(captured.size() === 1)
      val records = captured.peek()
      assert(records.nonEmpty)
      assert(records.map(_.executionId).distinct === Seq(f"query-${qe.id}%020d"))
    } finally {
      executor.shutdownNow()
      assert(executor.awaitTermination(30, TimeUnit.SECONDS))
    }
  }

  test("failed queries do not enter the completed opportunity corpus") {
    val captured = new ConcurrentLinkedQueue[Seq[ShuffleRecoveryExchangeObservation]]()
    val listener = new ShuffleRecoveryOpportunityListener(rules, batch => captured.add(batch))
    val qe = spark.range(4).repartition(2, $"id").queryExecution

    listener.onFailure("collect", qe, new RuntimeException("expected"))

    assert(captured.isEmpty)
  }

  test("listener retries an observation after the evidence sink fails") {
    val attempts = new AtomicInteger(0)
    val captured = new ConcurrentLinkedQueue[Seq[ShuffleRecoveryExchangeObservation]]()
    val errors = new ConcurrentLinkedQueue[Throwable]()
    val listener = new ShuffleRecoveryOpportunityListener(
      rules,
      batch => {
        if (attempts.getAndIncrement() == 0) {
          throw new RuntimeException("expected")
        }
        captured.add(batch)
      },
      error => errors.add(error))
    val qe = spark.range(4).repartition(2, $"id").queryExecution

    listener.onSuccess("collect", qe, 0L)
    listener.onSuccess("collect", qe, 0L)
    listener.onSuccess("collect", qe, 0L)

    assert(attempts.get() === 2)
    assert(captured.size() === 1)
    assert(errors.size() === 1)
  }

  test("listener dedupe protects in-flight ids from bounded history eviction") {
    val deduplicator = new ShuffleRecoveryExecutionDeduplicator()
    val protectedId = 1L

    assert(deduplicator.tryStart(protectedId))
    (2L to 5000L).foreach { executionId =>
      assert(deduplicator.tryStart(executionId))
      deduplicator.markCompleted(executionId)
    }
    assert(!deduplicator.tryStart(protectedId))

    deduplicator.markCompleted(protectedId)
    assert(!deduplicator.tryStart(protectedId))
    (5001L to 10000L).foreach { executionId =>
      assert(deduplicator.tryStart(executionId))
      deduplicator.markCompleted(executionId)
    }
    assert(deduplicator.tryStart(protectedId))

    deduplicator.markFailed(protectedId)
    assert(deduplicator.tryStart(protectedId))
    deduplicator.markFailed(protectedId)
  }

  private def maybeWriteEvidence(
      records: Seq[ShuffleRecoveryExchangeObservation]): Unit = {
    Option(System.getenv("SPARK_SHUFFLE_RECOVERY_OPPORTUNITY_OUTPUT")).foreach { rawPath =>
      val path = Paths.get(rawPath)
      Option(path.getParent).foreach(parent => Files.createDirectories(parent))
      val content = records.iterator.map(_.toJson).mkString("", "\n", "\n")
      Files.write(path, content.getBytes(StandardCharsets.UTF_8))
      assert(Files.size(path) > 0L)
    }
  }

  private case class UnknownUnaryExec(child: SparkPlan) extends UnaryExecNode {
    override def output: Seq[Attribute] = child.output

    override protected def doExecute(): RDD[InternalRow] = child.execute()

    override protected def withNewChildInternal(newChild: SparkPlan): UnknownUnaryExec = {
      copy(child = newChild)
    }
  }

  private case class ExpressionCarrierExec(
      expression: Expression,
      child: SparkPlan) extends UnaryExecNode {
    override def output: Seq[Attribute] = child.output

    override protected def doExecute(): RDD[InternalRow] = child.execute()

    override protected def withNewChildInternal(newChild: SparkPlan): ExpressionCarrierExec = {
      copy(child = newChild)
    }
  }

  private case class UnknownExpression(child: Expression)
    extends UnaryExpression with Unevaluable {
    override def dataType: DataType = child.dataType

    override def nullable: Boolean = child.nullable

    override protected def withNewChildInternal(newChild: Expression): UnknownExpression = {
      copy(child = newChild)
    }
  }

  private case class HostileUnknownExpression() extends Expression with Unevaluable {
    override lazy val deterministic: Boolean = {
      throw new IllegalStateException(
        "deterministic must not be invoked for an unknown expression")
    }

    override def children: Seq[Expression] = {
      throw new IllegalStateException("children must not be invoked for an unknown expression")
    }

    override def dataType: DataType = {
      throw new IllegalStateException("dataType must not be invoked for an unknown expression")
    }

    override def nullable: Boolean = {
      throw new IllegalStateException("nullable must not be invoked for an unknown expression")
    }

    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[Expression]): HostileUnknownExpression = {
      throw new IllegalStateException(
        "unknown expression must not be rewritten by opportunity analysis")
    }
  }

  private case class TokenLeafExec(override val output: Seq[Attribute]) extends LeafExecNode {
    override protected def doExecute(): RDD[InternalRow] = sparkContext.emptyRDD[InternalRow]
  }

  private case class HostilePartitioning() extends Partitioning {
    override lazy val numPartitions: Int = {
      throw new IllegalStateException("unknown partitioning must not be inspected")
    }
  }
}
