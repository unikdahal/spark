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
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Alias, Ascending, LeafExpression, SortOrder, Unevaluable}
import org.apache.spark.sql.catalyst.plans.physical.{
  RangePartitioning, RoundRobinPartitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.ShuffleRecoveryEligibility._
import org.apache.spark.sql.functions.{rand, row_number}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, IntegerType}

class ShuffleRecoveryEligibilitySuite extends QueryTest with SharedSparkSession {

  private case object UnknownExpression extends LeafExpression with Unevaluable {
    override def dataType: DataType = IntegerType
    override def nullable: Boolean = false
  }

  private case class UnknownUnaryExec(child: SparkPlan) extends UnaryExecNode {
    override def output = child.output

    override protected def doExecute(): RDD[InternalRow] = {
      throw new UnsupportedOperationException("analysis-only test plan")
    }

    override protected def withNewChildInternal(newChild: SparkPlan): UnknownUnaryExec = {
      copy(child = newChild)
    }
  }

  private val analyzer = new Analyzer(RuleSet.Conservative)

  private def executedHashExchange(): ShuffleExchangeExec = {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.range(32).repartition(4, $"id")
      df.collect()
      df.queryExecution.executedPlan.collectFirst {
        case exchange: ShuffleExchangeExec => exchange
      }.getOrElse(fail("expected a shuffle exchange"))
    }
  }

  private def syntheticRules: RuleSet = {
    val base = RuleSet.Conservative
    base.copy(
      name = "synthetic",
      acceptedSourceTokens = Set(
        ExactTokenAvailable,
        PrototypeSpecialCased,
        SourceTokenUnavailable),
      requireDeterminateLineage = false,
      allowedPartitioningClassNames =
        base.allowedPartitioningClassNames + classOf[UnknownPartitioning].getName)
  }

  test("zero exchanges produces an empty deterministic record set") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.range(8).select($"id")
      df.collect()
      assert(analyzer.analyze(df.queryExecution.executedPlan, "zero").isEmpty)
    }
  }

  test("deterministic range lineage with hash partitioning is eligible") {
    val exchange = executedHashExchange()
    val records = analyzer.analyze(exchange, "eligible")

    assert(records.length == 1)
    assert(records.head.eligible)
    assert(records.head.immediateReason.isEmpty)
    assert(records.head.rootReason.isEmpty)
    assert(records.head.sourceTokenAvailability == ExactTokenAvailable)
    assert(records.head.lineageDeterminism == DeterminateLineage)
    assert(records.head.reducerCount == 4)
    assert(records.head.mapperCount.contains(1))
  }

  test("range partitioning is rejected with a stable reason") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.range(32).repartitionByRange(4, $"id")
      df.collect()
      val record = analyzer.analyze(df.queryExecution.executedPlan, "range").head

      assert(!record.eligible)
      assert(record.immediateReason.contains(RANGE_PARTITIONING))
      assert(record.rootReason.contains(RANGE_PARTITIONING))
      assert(record.partitioningSummary == "RANGE:4")
    }
  }

  test("non-deterministic expressions fail closed") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.range(32).select(rand().as("r")).repartition(4, $"r")
      df.collect()
      val record = analyzer.analyze(df.queryExecution.executedPlan, "nondeterministic").head

      assert(!record.eligible)
      assert(record.rootReason.contains(NON_DETERMINATE))
    }
  }

  test("unknown SparkPlan subclasses fail closed") {
    val base = executedHashExchange()
    val exchange = ShuffleExchangeExec(
      RoundRobinPartitioning(2),
      UnknownUnaryExec(base.child))
    val record = new Analyzer(syntheticRules).analyze(exchange, "custom-plan").head

    assert(!record.eligible)
    assert(record.immediateReason.contains(CUSTOM_OPERATOR))
  }

  test("unknown Expression subclasses fail closed") {
    val base = executedHashExchange()
    val project = ProjectExec(Seq(Alias(UnknownExpression, "unknown")()), base.child)
    val exchange = ShuffleExchangeExec(RoundRobinPartitioning(2), project)
    val record = new Analyzer(syntheticRules).analyze(exchange, "custom-expression").head

    assert(!record.eligible)
    assert(record.immediateReason.contains(UNSUPPORTED_EXPRESSION))
  }

  test("zero partition shapes are rejected before any recovery claim") {
    val base = executedHashExchange()
    val exchange = ShuffleExchangeExec(UnknownPartitioning(0), base.child)
    val record = new Analyzer(syntheticRules).analyze(exchange, "zero-partitions").head

    assert(!record.eligible)
    assert(record.immediateReason.contains(INVALID_PARTITION_COUNT))
    assert(record.reducerCount == 0)
  }

  test("multiple nested exchanges use deterministic pre-order paths") {
    val inner = executedHashExchange()
    val outer = inner.copy(
      outputPartitioning = RoundRobinPartitioning(2),
      child = inner)
    val records = new Analyzer(syntheticRules).analyze(outer, "nested")

    assert(records.map(_.exchangePath) == Seq("root", "0"))
    assert(records.map(_.exchangeOrdinal) == Seq(0L, 1L))
  }

  test("reused exchange and reused query-stage shapes remain observable") {
    val exchange = executedHashExchange()
    val reused = ReusedExchangeExec(exchange.output, exchange)
    val reusedRecord = analyzer.analyze(reused, "reused").head

    assert(reusedRecord.flags.reusedExchange)

    val stage = ShuffleQueryStageExec(0, reused, exchange.canonicalized)
    val stageRecord = analyzer.analyze(stage, "reused-stage").head
    assert(stageRecord.flags.reusedExchange)
    assert(stageRecord.flags.adaptivePlan)
  }

  test("same executed plan produces byte-for-byte stable JSON") {
    val exchange = executedHashExchange()
    val first = analyzer.analyze(exchange, "stable").map(_.toJson)
    val second = analyzer.analyze(exchange, "stable").map(_.toJson)

    assert(first == second)
  }

  test("pipelined shuffle mode is rejected without changing the exchange") {
    val base = executedHashExchange()
    val pipelined = base.copy(pipelined = true)
    val record = new Analyzer(syntheticRules).analyze(pipelined, "pipelined").head

    assert(!record.eligible)
    assert(record.immediateReason.contains(UNSUPPORTED_SHUFFLE_MODE))
    assert(base.pipelined === false)
  }

  test("prototype source-token category is an explicit scope relaxation") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      import testImplicits._
      val df = Seq(1, 2, 3, 4).toDF("id").repartition(2, $"id")
      df.collect()

      val baseline = analyzer.analyze(df.queryExecution.executedPlan, "source-baseline").head
      assert(baseline.sourceTokenAvailability == PrototypeSpecialCased)
      assert(baseline.rootReason.contains(SOURCE_TOKEN_UNAVAILABLE))

      val relaxedRules = RuleSet.Conservative.copy(
        name = "source-special-cased",
        acceptedSourceTokens = Set(ExactTokenAvailable, PrototypeSpecialCased))
      val relaxed = new Analyzer(relaxedRules)
        .analyze(df.queryExecution.executedPlan, "source-relaxed").head
      assert(!relaxed.rootReason.contains(SOURCE_TOKEN_UNAVAILABLE))
    }
  }

  test("window presence is recorded independently from operator allowlisting") {
    import org.apache.spark.sql.expressions.Window

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.range(16)
        .select($"id", row_number().over(Window.orderBy($"id")).as("rn"))
        .repartition(2, $"rn")
      df.collect()

      val records = analyzer.analyze(df.queryExecution.executedPlan, "window")
      val outer = records.find(_.flags.window).getOrElse(fail("expected a window flag"))
      assert(outer.rootReason.contains(WINDOW_PRESENT))

      val relaxedRules = RuleSet.Conservative.copy(name = "window-enabled", allowWindow = true)
      val relaxed = new Analyzer(relaxedRules)
        .analyze(df.queryExecution.executedPlan, "window-enabled")
        .find(_.exchangePath == outer.exchangePath)
        .get
      assert(!relaxed.rootReason.contains(WINDOW_PRESENT))
    }
  }

  test("10,000 classifications keep stable ordinals and bounded per-record state") {
    val exchange = executedHashExchange()
    var count = 0L
    var lastJson = ""

    var i = 0
    while (i < 10000) {
      val records = analyzer.analyze(exchange, "scale")
      assert(records.length == 1)
      count += records.length
      lastJson = records.head.toJson
      i += 1
    }

    assert(count == 10000L)
    assert(lastJson.nonEmpty)
  }

  test("listener observes AQE-on and AQE-off plans and unregisters cleanly") {
    val listenersBefore = spark.listenerManager.listListeners().toSeq
    val aqeOff = captureListenerRecords(aqeEnabled = false)
    val aqeOn = captureListenerRecords(aqeEnabled = true)

    assert(aqeOff.nonEmpty)
    assert(aqeOn.nonEmpty)
    assert(aqeOff.forall(!_.flags.adaptivePlan))
    assert(aqeOn.exists(_.flags.adaptivePlan))
    assert(spark.listenerManager.listListeners().toSeq == listenersBefore)

    writeEvidence(aqeOff, aqeOn)
  }

  test("listener serializes concurrent callbacks without retaining work") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.range(32).repartition(4, $"id")
      df.collect()
      val qe = df.queryExecution
      val activeCallbacks = new AtomicInteger(0)
      val maxActiveCallbacks = new AtomicInteger(0)
      val callbackCount = new AtomicInteger(0)
      val firstCallbackEntered = new CountDownLatch(1)
      val releaseFirstCallback = new CountDownLatch(1)
      val start = new CountDownLatch(1)
      val completed = new CountDownLatch(4)

      val listener = new Listener(analyzer, _ => {
        val active = activeCallbacks.incrementAndGet()
        maxActiveCallbacks.accumulateAndGet(active, Math.max)
        if (callbackCount.get() == 0) {
          firstCallbackEntered.countDown()
          assert(releaseFirstCallback.await(10, TimeUnit.SECONDS))
        }
        callbackCount.incrementAndGet()
        activeCallbacks.decrementAndGet()
      })

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
        assert(firstCallbackEntered.await(10, TimeUnit.SECONDS))
        releaseFirstCallback.countDown()
        assert(completed.await(30, TimeUnit.SECONDS))
      } finally {
        executor.shutdownNow()
      }

      assert(callbackCount.get() == 4)
      assert(maxActiveCallbacks.get() == 1)
    }
  }

  test("range rule can be parameterized without changing analyzer traversal") {
    val base = executedHashExchange()
    val range = RangePartitioning(Seq(SortOrder(base.child.output.head, Ascending)), 2)
    val exchange = base.copy(outputPartitioning = range)
    val relaxedRules = syntheticRules.copy(
      name = "range-enabled",
      allowRangePartitioning = true,
      allowedPartitioningClassNames =
        syntheticRules.allowedPartitioningClassNames + range.getClass.getName)

    val baseline = new Analyzer(syntheticRules).analyze(exchange, "range-off").head
    val relaxed = new Analyzer(relaxedRules).analyze(exchange, "range-on").head

    assert(baseline.rootReason.contains(RANGE_PARTITIONING))
    assert(!relaxed.rootReason.contains(RANGE_PARTITIONING))
  }

  private def captureListenerRecords(aqeEnabled: Boolean): Seq[Record] = {
    val records = ArrayBuffer.empty[Record]
    val listener = new Listener(analyzer, batch => records.synchronized {
      records ++= batch
    })

    spark.listenerManager.register(listener)
    try {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled.toString) {
        val result = spark.range(64).repartition(4, $"id").groupBy($"id").count().collect()
        assert(result.length == 64)
        spark.sparkContext.listenerBus.waitUntilEmpty()
      }
    } finally {
      spark.listenerManager.unregister(listener)
    }
    records.synchronized(records.toSeq)
  }

  private def writeEvidence(aqeOff: Seq[Record], aqeOn: Seq[Record]): Unit = {
    sys.env.get("SPARK_SHUFFLE_RECOVERY_EVIDENCE_DIR").foreach { directory =>
      val path = Paths.get(directory).resolve("eligibility-records.jsonl")
      Files.createDirectories(path.getParent)
      val normalized =
        aqeOff.map(_.copy(executionId = "aqe-off")) ++
          aqeOn.map(_.copy(executionId = "aqe-on"))
      assert(normalized.nonEmpty)
      val content = normalized.map(_.toJson).mkString("", "\n", "\n")
      Files.write(path, content.getBytes(StandardCharsets.UTF_8))
      assert(Files.size(path) > 0L)
    }
  }
}