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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Ascending, Attribute, DynamicPruningExpression, Expression, Literal, SortOrder,
  UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.physical.{
  HashPartitioning, RangePartitioning, RoundRobinPartitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{
  LeafExecNode, SparkPlan, UnaryExecNode, UnionExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ShuffleRecoveryOpportunityAnalyzerSuite extends SharedSparkSession {
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
    ShuffleRecoveryOpportunityAnalyzer.analyze(
      plan, executionId, analyzerRules, runtimeState)
  }

  test("zero exchanges produce no observation") {
    assert(analyze(rangePlan()).isEmpty)
  }

  test("a deterministic hash exchange over the feasibility source is eligible") {
    val records = analyze(hashExchange(rangePlan()))

    assert(records.size === 1)
    val record = records.head
    assert(record.exchangeOrdinal === 0L)
    assert(record.exchangePath === "")
    assert(record.partitionCount === 4)
    assert(record.eligible)
    assert(record.immediateMissReason.isEmpty)
    assert(record.rootMissReason.isEmpty)
    assert(record.sourceTokenAvailability === PrototypeSpecialCased)
    assert(record.mapperCount.isEmpty)
    assert(record.runtimeStageId.isEmpty)
    assert(record.runtimeShuffleId.isEmpty)
  }

  test("nested exchanges are observed in deterministic occurrence order") {
    val inner = hashExchange(rangePlan(), partitions = 2)
    val outer = hashExchange(inner, partitions = 3)

    val records = analyze(outer)

    assert(records.map(_.exchangeOrdinal) === Seq(0L, 1L))
    assert(records.map(_.exchangePath) === Seq("", "0"))
    assert(records.forall(_.eligible))
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

  test("unknown operator semantics fail closed") {
    val exchange = hashExchange(UnknownUnaryExec(rangePlan()))
    val record = analyze(exchange).head

    assert(!record.eligible)
    assert(record.immediateMissReason.contains(UnsupportedOperator))
    assert(record.rootMissReason.contains(UnsupportedOperator))
  }

  test("unknown expression semantics fail closed independently of the operator allowlist") {
    val carrier = ExpressionCarrierExec(UnknownExpression(Literal(1)), rangePlan())
    val carrierRules = rules.copy(
      allowedOperatorClassNames = rules.allowedOperatorClassNames + carrier.getClass.getName)
    val record = analyze(hashExchange(carrier), analyzerRules = carrierRules).head

    assert(!record.eligible)
    assert(record.immediateMissReason.contains(UnsupportedExpression))
    assert(record.rootMissReason.contains(UnsupportedExpression))
  }

  test("dynamic pruning is a parameterized miss rather than a hard-coded traversal rule") {
    val carrier =
      ExpressionCarrierExec(DynamicPruningExpression(Literal(true)), rangePlan())
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

    val unavailable = analyze(
      hashExchange(source), analyzerRules = operatorRules).head
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

  test("range partitioning is rejected with a stable reason") {
    val child = rangePlan()
    val partitioning = RangePartitioning(
      Seq(SortOrder(child.output.head, Ascending)), numPartitions = 2)
    val record = analyze(ShuffleExchangeExec(partitioning, child)).head

    assert(!record.eligible)
    assert(record.immediateMissReason.contains(RangePartitioningPresent))
    assert(record.rootMissReason.contains(RangePartitioningPresent))
  }

  test("non-deterministic expressions are rejected before generic expression misses") {
    val child = spark.range(32).selectExpr("rand() AS r").queryExecution.executedPlan
    val record = analyze(hashExchange(child)).head

    assert(!record.eligible)
    assert(record.immediateMissReason.contains(NonDeterministic))
    assert(record.rootMissReason.contains(NonDeterministic))
  }

  test("multi-partition round-robin is conservatively rejected without RDD materialization") {
    val child = rangePlan()
    val exchange = ShuffleExchangeExec(RoundRobinPartitioning(4), child)

    val refused = analyze(exchange).head
    val admitted = analyze(
      exchange,
      analyzerRules = rules.copy(allowMultiPartitionRoundRobin = true)).head

    assert(refused.immediateMissReason.contains(NonDeterministic))
    assert(admitted.eligible)
  }

  test("pipelined and push-based shuffle modes are rejected") {
    val child = rangePlan()
    val pipelined = ShuffleExchangeExec(
      HashPartitioning(child.output.take(1), 2), child, pipelined = true)
    val ordinary = hashExchange(child, partitions = 2)

    assert(analyze(pipelined).head.immediateMissReason.contains(UnsupportedShuffleMode))
    assert(analyze(
      ordinary,
      runtimeState = ShuffleRecoveryRuntimeState(pushBasedShuffleEnabled = true))
      .head.immediateMissReason.contains(UnsupportedShuffleMode))
  }

  test("invalid partition counts fail closed without allocation") {
    val record = analyze(
      ShuffleExchangeExec(UnknownPartitioning(0), rangePlan())).head

    assert(!record.eligible)
    assert(record.partitionCount === 0)
    assert(record.immediateMissReason.contains(UnsupportedPartitioning))
  }

  test("empty query results remain observable without special-case mutation") {
    val df = spark.range(0).repartition(1)
    df.collect()

    val records = analyze(df.queryExecution.executedPlan, executionId = "empty")
    assert(records.nonEmpty)
    assert(records.forall(_.partitionCount > 0))
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

    val executionIds = records.map(_.executionId).toSet
    assert(executionIds === Set("aqe-off", "aqe-on"))
    maybeWriteEvidence(records)
  }

  test("10,000 exchange occurrences remain ordered and are not identity-deduplicated") {
    val exchange = hashExchange(rangePlan(), partitions = 2)
    val plan = UnionExec(Seq.fill(10000)(exchange))

    val records = analyze(plan, executionId = "many")

    assert(records.size === 10000)
    assert(records.head.exchangeOrdinal === 0L)
    assert(records.last.exchangeOrdinal === 9999L)
    assert(records.map(_.exchangeOrdinal) === (0L until 10000L).toSeq)
  }

  test("listener registration is explicit and unregistering stops observations") {
    val listener = new ShuffleRecoveryOpportunityListener(rules)
    assert(!spark.listenerManager.listListeners().contains(listener))

    val baseline = spark.range(8).collect().map(_.getLong(0)).toSeq
    assert(baseline === (0L until 8L).toSeq)

    spark.listenerManager.register(listener)
    try {
      spark.range(16).repartition(2, $"id").collect()
      spark.sparkContext.listenerBus.waitUntilEmpty()
      assert(listener.snapshot().nonEmpty)
    } finally {
      spark.listenerManager.unregister(listener)
    }

    val observedCount = listener.snapshot().size
    spark.range(4).repartition(2, $"id").collect()
    spark.sparkContext.listenerBus.waitUntilEmpty()
    assert(listener.snapshot().size === observedCount)
  }

  test("listener observation queue and run identifiers are thread-safe") {
    val listener = new ShuffleRecoveryOpportunityListener(rules)
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

      val snapshot = listener.snapshot()
      assert(snapshot.size === 16)
      assert(snapshot.map(_.executionId).distinct.size === 16)
      assert(snapshot.map(_.exchangeOrdinal).forall(_ === 0L))
    } finally {
      executor.shutdownNow()
      assert(executor.awaitTermination(30, TimeUnit.SECONDS))
    }
  }

  private def maybeWriteEvidence(
      records: Seq[ShuffleRecoveryExchangeObservation]): Unit = {
    Option(System.getenv("SPARK_SHUFFLE_RECOVERY_OPPORTUNITY_OUTPUT")).foreach { rawPath =>
      val path = Paths.get(rawPath)
      Option(path.getParent).foreach(parent => Files.createDirectories(parent))
      val content = records.iterator.map(_.toJson).mkString("", "\n", "\n")
      Files.write(path, content.getBytes(StandardCharsets.UTF_8))
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
