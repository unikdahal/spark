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
  Ascending, Attribute, BloomFilterMightContain, DynamicPruningExpression, Expression, Literal,
  SortOrder, UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.physical.{
  HashPartitioning, Partitioning, RangePartitioning, RoundRobinPartitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan, UnaryExecNode, UnionExec}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

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

  test("nested exchanges are observed in deterministic occurrence order") {
    val inner = hashExchange(rangePlan(), partitions = 2)
    val outer = hashExchange(inner, partitions = 3)

    val records = analyze(outer)

    assert(records.map(_.exchangeOrdinal) === Seq(0L, 1L))
    assert(records.map(_.exchangePath) === Seq("root", "c0"))
    assert(records.forall(_.eligible))
  }

  test("nested exchange misses use a cascade reason without losing the root cause") {
    val child = rangePlan()
    val inner = ShuffleExchangeExec(
      RangePartitioning(Seq(SortOrder(child.output.head, Ascending)), 2), child)
    val outer = hashExchange(inner, partitions = 3)

    val records = analyze(outer)

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
    val carrierRules = rules.copy(
      allowedOperatorClassNames = rules.allowedOperatorClassNames + carrier.getClass.getName)
    val record = analyze(hashExchange(carrier), analyzerRules = carrierRules).head

    assert(!record.eligible)
    assert(record.immediateMissReason.contains(UnsupportedExpression))
    assert(record.rootMissReason.contains(UnsupportedExpression))
  }

  test("dynamic pruning is an independently parameterized miss") {
    val carrier = ExpressionCarrierExec(DynamicPruningExpression(Literal(true)), rangePlan())
    val carrierRules = rules.copy(
      allowedOperatorClassNames = rules.allowedOperatorClassNames + carrier.getClass.getName)

    val refused = analyze(hashExchange(carrier), analyzerRules = carrierRules).head
    val admitted = analyze(
      hashExchange(carrier),
      analyzerRules = carrierRules.copy(allowDynamicPruning = true)).head

    assert(refused.immediateMissReason.contains(DynamicPruningPresent))
    assert(refused.flags.dynamicPruning)
    assert(admitted.eligible)
    assert(admitted.flags.dynamicPruning)
  }

  test("runtime filters are an independently parameterized miss") {
    val runtimeFilter = BloomFilterMightContain(Literal(Array[Byte](1)), Literal(1L))
    val carrier = ExpressionCarrierExec(runtimeFilter, rangePlan())
    val carrierRules = rules.copy(
      allowedOperatorClassNames = rules.allowedOperatorClassNames + carrier.getClass.getName)

    val refused = analyze(hashExchange(carrier), analyzerRules = carrierRules).head
    val admitted = analyze(
      hashExchange(carrier),
      analyzerRules = carrierRules.copy(allowRuntimeFilters = true)).head

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
    val df = spark.range(16)
      .select($"id", row_number().over(Window.orderBy($"id")).as("rn"))
      .repartition(2, $"rn")

    val refused = analyze(df.queryExecution.executedPlan)
      .find(_.flags.window).getOrElse(fail("expected a window-bearing exchange"))
    val admitted = analyze(
      df.queryExecution.executedPlan,
      analyzerRules = rules.copy(allowWindow = true))
      .find(_.exchangePath == refused.exchangePath).get

    assert(refused.rootMissReason.contains(WindowPresent))
    assert(!admitted.rootMissReason.contains(WindowPresent))
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

  test("listener registration is explicit and unregistering stops observations") {
    val captured = new ConcurrentLinkedQueue[Seq[ShuffleRecoveryExchangeObservation]]()
    val errors = new ConcurrentLinkedQueue[Throwable]()
    val listener = new ShuffleRecoveryOpportunityListener(
      rules,
      batch => captured.add(batch),
      error => errors.add(error))
    assert(!spark.listenerManager.listListeners().contains(listener))

    val baseline = spark.range(8).collect().map(_.getLong(0)).toSeq
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

  test("listener duplicate history is bounded") {
    val callbackCount = new AtomicInteger(0)
    val listener = new ShuffleRecoveryOpportunityListener(
      rules, _ => callbackCount.incrementAndGet())
    val first = spark.range(1).queryExecution

    listener.onSuccess("collect", first, 0L)
    (0 until 5000).foreach { index =>
      val qe = spark.range(index.toLong, index.toLong + 1L).queryExecution
      listener.onSuccess("collect", qe, 0L)
    }
    val beforeReplay = callbackCount.get()
    listener.onSuccess("collect", first, 0L)

    assert(callbackCount.get() === beforeReplay + 1)
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
    override def dataType: org.apache.spark.sql.types.DataType = child.dataType

    override def nullable: Boolean = child.nullable

    override protected def withNewChildInternal(newChild: Expression): UnknownExpression = {
      copy(child = newChild)
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
