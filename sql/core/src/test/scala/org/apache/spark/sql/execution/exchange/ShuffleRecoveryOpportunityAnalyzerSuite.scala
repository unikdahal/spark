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
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Ascending, Attribute, DynamicPruningExpression, Expression, Literal, SortOrder,
  UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.physical.{
  HashPartitioning, RangePartitioning, RoundRobinPartitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan, UnaryExecNode, UnionExec}
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
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

  /** Plan-only tests inject lineage explicitly so classification cannot materialize an RDD. */
  private def analyze(
      plan: SparkPlan,
      executionId: String = "test",
      analyzerRules: ShuffleRecoveryEligibilityRules = rules,
      runtimeState: ShuffleRecoveryRuntimeState = ShuffleRecoveryRuntimeState())
      : Seq[ShuffleRecoveryExchangeObservation] = {
    val discoveryRules = analyzerRules.copy(requireDeterminateLineage = false)
    val paths = ShuffleRecoveryOpportunityAnalyzer
      .analyze(plan, executionId, discoveryRules, runtimeState)
      .map(_.exchangePath)
    val syntheticLineage = paths.iterator
      .filterNot(runtimeState.lineageByExchangePath.contains)
      .map(_ -> Determinate)
      .toMap
    ShuffleRecoveryOpportunityAnalyzer.analyze(
      plan,
      executionId,
      analyzerRules,
      runtimeState.copy(
        lineageByExchangePath = syntheticLineage ++ runtimeState.lineageByExchangePath))
  }

  private def analyzeCompleted(
      plan: SparkPlan,
      executionId: String): Seq[ShuffleRecoveryExchangeObservation] = {
    val runtime = ShuffleRecoveryOpportunityAnalyzer.withCompletedExchangeRuntime(
      plan, ShuffleRecoveryRuntimeState())
    ShuffleRecoveryOpportunityAnalyzer.analyze(plan, executionId, rules, runtime)
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
    assert(record.partitionCount === 4)
    assert(record.eligible)
    assert(record.immediateMissReason.isEmpty)
    assert(record.rootMissReason.isEmpty)
    assert(record.sourceTokenAvailability === PrototypeSpecialCased)
    assert(record.lineageDeterminism === Determinate)
    assert(record.mapperCount.isEmpty)
    assert(record.runtimeStageId.isEmpty)
    assert(record.runtimeShuffleId.isEmpty)
  }

  test("unproven lineage fails closed") {
    val record = ShuffleRecoveryOpportunityAnalyzer
      .analyze(hashExchange(rangePlan()), "unknown-lineage", rules)
      .head

    assert(!record.eligible)
    assert(record.lineageDeterminism === Unknown)
    assert(record.immediateMissReason.contains(NonDeterministic))
    assert(record.rootMissReason.contains(NonDeterministic))
  }

  test("completed exchanges expose Spark DETERMINATE lineage and mapper shape") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.range(0, 32, 1, 4).repartition(4, $"id")
      df.collect()
      val record = analyzeCompleted(df.queryExecution.executedPlan, "completed").head

      assert(record.lineageDeterminism === Determinate)
      assert(record.mapperCount.contains(4))
      assert(record.partitionCount === 4)
    }
  }

  test("nested exchanges are observed in deterministic occurrence order") {
    val inner = hashExchange(rangePlan(), partitions = 2)
    val outer = hashExchange(inner, partitions = 3)

    val records = analyze(outer)

    assert(records.map(_.exchangeOrdinal) === Seq(0L, 1L))
    assert(records.map(_.exchangePath) === Seq("root", "0"))
    assert(records.forall(_.eligible))
  }

  test("reused exchange and query-stage wrappers remain observable without object ids") {
    val exchange = hashExchange(rangePlan())
    val reused = ReusedExchangeExec(exchange.output, exchange)
    val stage = ShuffleQueryStageExec(0, reused, exchange.canonicalized)

    val first = analyze(stage, executionId = "stable")
    val second = analyze(stage, executionId = "stable")

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
    val carrierClass = carrier.getClass.getName
    val baselineRules = rules.copy(
      allowedOperatorClassNames = rules.allowedOperatorClassNames + carrierClass)

    val refused = analyze(hashExchange(carrier), analyzerRules = baselineRules).head
    val admitted = analyze(
      hashExchange(carrier),
      analyzerRules = baselineRules.copy(allowDynamicPruning = true)).head

    assert(refused.immediateMissReason.contains(DynamicPruningPresent))
    assert(refused.flags.dynamicPruning)
    assert(admitted.eligible)
    assert(admitted.flags.dynamicPruning)
  }

  test("source-token categories are independently parameterized") {
    val source = TokenLeafExec(rangePlan().output)
    val sourceClass = source.getClass.getName
    val operatorRules = rules.copy(
      allowedOperatorClassNames = rules.allowedOperatorClassNames + sourceClass)

    val unavailable = analyze(hashExchange(source), analyzerRules = operatorRules).head
    val exact = analyze(
      hashExchange(source),
      analyzerRules = operatorRules.copy(
        sourceTokenByOperatorClassName =
          operatorRules.sourceTokenByOperatorClassName + (sourceClass -> Exact))).head
    val prototype = analyze(
      hashExchange(source),
      analyzerRules = operatorRules.copy(
        sourceTokenByOperatorClassName =
          operatorRules.sourceTokenByOperatorClassName +
            (sourceClass -> PrototypeSpecialCased))).head

    assert(unavailable.immediateMissReason.contains(SourceTokenUnavailable))
    assert(unavailable.rootMissReason.contains(SourceTokenUnavailable))
    assert(unavailable.sourceTokenAvailability === Unavailable)
    assert(exact.eligible)
    assert(exact.sourceTokenAvailability === Exact)
    assert(prototype.eligible)
    assert(prototype.sourceTokenAvailability === PrototypeSpecialCased)
  }

  test("range partitioning is rejected and can be relaxed without rewriting traversal") {
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

  test("pipelined and push-based shuffle rules are independently parameterized") {
    val child = rangePlan()
    val pipelined = ShuffleExchangeExec(
      HashPartitioning(child.output.take(1), 2), child, pipelined = true)
    val ordinary = hashExchange(child, partitions = 2)
    val pushState = ShuffleRecoveryRuntimeState(pushBasedShuffleEnabled = true)

    assert(analyze(pipelined).head.immediateMissReason.contains(UnsupportedShuffleMode))
    assert(analyze(
      pipelined,
      analyzerRules = rules.copy(allowPipelinedShuffle = true)).head.eligible)
    assert(analyze(
      ordinary,
      runtimeState = pushState).head.immediateMissReason.contains(UnsupportedShuffleMode))
    assert(analyze(
      ordinary,
      analyzerRules = rules.copy(allowPushBasedShuffle = true),
      runtimeState = pushState).head.eligible)
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

  test("invalid partition counts fail closed without allocation") {
    val record = analyze(ShuffleExchangeExec(UnknownPartitioning(0), rangePlan())).head

    assert(!record.eligible)
    assert(record.partitionCount === 0)
    assert(record.immediateMissReason.contains(InvalidPartitionCount))
  }

  test("window presence is recorded independently from generic operator allowlisting") {
    val df = spark.range(16)
      .select($"id", row_number().over(Window.orderBy($"id")).as("rn"))
      .repartition(2, $"rn")
    val baseline = analyze(df.queryExecution.executedPlan)
    val record = baseline.find(_.flags.window).getOrElse(fail("expected a window-bearing exchange"))
    val relaxed = analyze(
      df.queryExecution.executedPlan,
      analyzerRules = rules.copy(allowWindow = true))
      .find(_.exchangePath == record.exchangePath)
      .get

    assert(record.rootMissReason.contains(WindowPresent))
    assert(record.flags.window)
    assert(!relaxed.rootMissReason.contains(WindowPresent))
  }

  test("empty query results remain observable without special-case mutation") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.range(0).repartition(1)
      df.collect()

      val records = analyzeCompleted(df.queryExecution.executedPlan, "empty")
      assert(records.nonEmpty)
      assert(records.forall(_.partitionCount > 0))
    }
  }

  test("AQE-on and AQE-off completed plans both expose shuffle observations") {
    val aqeOff = completedRecords(adaptive = false)
    val aqeOn = completedRecords(adaptive = true)

    assert(aqeOff.nonEmpty)
    assert(aqeOn.nonEmpty)
    assert(aqeOff.forall(!_.flags.adaptivePlan))
    assert(aqeOn.exists(_.flags.adaptivePlan))
    assert((aqeOff ++ aqeOn).forall(_.lineageDeterminism != Unknown))

    maybeWriteEvidence(aqeOff ++ aqeOn)
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
    val batches = ArrayBuffer.empty[Seq[ShuffleRecoveryExchangeObservation]]
    val errors = ArrayBuffer.empty[Throwable]
    val listener = new ShuffleRecoveryOpportunityListener(
      rules,
      batch => batches.synchronized { batches += batch },
      error => errors.synchronized { errors += error })

    assert(!spark.listenerManager.listListeners().contains(listener))
    spark.range(8).repartition(2, $"id").collect()
    spark.sparkContext.listenerBus.waitUntilEmpty()
    assert(batches.synchronized(batches.isEmpty))

    spark.listenerManager.register(listener)
    try {
      spark.range(16).repartition(2, $"id").collect()
      spark.sparkContext.listenerBus.waitUntilEmpty()
      assert(batches.synchronized(batches.flatten.nonEmpty))
      assert(errors.synchronized(errors.isEmpty))
    } finally {
      spark.listenerManager.unregister(listener)
    }

    val observedCount = batches.synchronized(batches.flatten.size)
    spark.range(4).repartition(2, $"id").collect()
    spark.sparkContext.listenerBus.waitUntilEmpty()
    assert(batches.synchronized(batches.flatten.size) === observedCount)
  }

  test("listener serializes concurrent sinks without arbitrary sleeps") {
    val df = spark.range(16).repartition(2, $"id")
    df.collect()
    val qe = df.queryExecution
    val activeSinks = new AtomicInteger(0)
    val overlap = new AtomicInteger(0)
    val callbackCount = new AtomicInteger(0)
    val errorCount = new AtomicInteger(0)
    val firstSinkEntered = new CountDownLatch(1)
    val releaseFirstSink = new CountDownLatch(1)
    val start = new CountDownLatch(1)
    val completed = new CountDownLatch(4)

    val listener = new ShuffleRecoveryOpportunityListener(
      rules,
      _ => {
        if (activeSinks.incrementAndGet() != 1) {
          overlap.incrementAndGet()
        }
        if (callbackCount.get() == 0) {
          firstSinkEntered.countDown()
          assert(releaseFirstSink.await(10, TimeUnit.SECONDS))
        }
        callbackCount.incrementAndGet()
        activeSinks.decrementAndGet()
      },
      _ => errorCount.incrementAndGet())

    val executor = Executors.newFixedThreadPool(4)
    try {
      (0 until 4).foreach { _ =>
        executor.submit(new Runnable {
          override def run(): Unit = {
            try {
              start.await()
              listener.onSuccess("test", qe, 0L)
            } finally {
              completed.countDown()
            }
          }
        })
      }
      start.countDown()
      assert(firstSinkEntered.await(10, TimeUnit.SECONDS))
      releaseFirstSink.countDown()
      assert(completed.await(30, TimeUnit.SECONDS))
    } finally {
      executor.shutdownNow()
      assert(executor.awaitTermination(30, TimeUnit.SECONDS))
    }

    assert(callbackCount.get() === 4)
    assert(overlap.get() === 0)
    assert(errorCount.get() === 0)
  }

  test("failed queries are not reported as completed shuffle observations") {
    val callbackCount = new AtomicInteger(0)
    val listener = new ShuffleRecoveryOpportunityListener(
      rules, _ => callbackCount.incrementAndGet())
    val qe = spark.range(4).repartition(2, $"id").queryExecution

    listener.onFailure("test", qe, new RuntimeException("expected"))
    assert(callbackCount.get() === 0)
  }

  private def completedRecords(adaptive: Boolean): Seq[ShuffleRecoveryExchangeObservation] = {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
      val df = spark.range(0, 64, 1, 4).repartition(4, $"id")
      df.collect()
      analyzeCompleted(
        df.queryExecution.executedPlan,
        if (adaptive) "aqe-on" else "aqe-off")
    }
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

    override def dataType = child.dataType

    override def nullable: Boolean = child.nullable

    override protected def withNewChildInternal(newChild: Expression): UnknownExpression = {
      copy(child = newChild)
    }
  }

  private case class TokenLeafExec(override val output: Seq[Attribute]) extends LeafExecNode {
    override protected def doExecute(): RDD[InternalRow] = sparkContext.emptyRDD[InternalRow]
  }
}
