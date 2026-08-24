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
package org.apache.spark.sql

import java.nio.ByteBuffer
import java.util.{Locale, UUID}
import java.util.{List => JList, Optional}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

import org.apache.spark.{MapOutputStatistics, ShuffleDependency, SparkException, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskStart}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{SourceRecoveryInfo, UnresolvedAttribute,
  UnresolvedRelation, WriteRecoveryInfo}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Final, Max, Partial}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParserInterface, SqlStatementSplitResult}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, AggregateHint, ColumnStat, Limit, LocalRelation, LogicalPlan, Project, Range, Sort, SortHint, Statistics, UnresolvedHint}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.connector.{RangeInputPartition, SimpleBatchTable, SimpleScanBuilder,
  TestingV2Source}
import org.apache.spark.sql.connector.catalog.{CatalogManager, SupportsRecoveryAnchor,
  SupportsRecoveryWrite, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE}
import org.apache.spark.sql.connector.read.{InputPartition, ScanBuilder}
import org.apache.spark.sql.connector.recovery.RecoveryTaskCommitStore
import org.apache.spark.sql.connector.write.{BatchWrite, BatchWriteRecoveryState,
  DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, RecoveryCommitMessageCodec,
  RecoveryDataWriter, RecoveryDataWriterFactory, SupportsBatchWriteRecovery,
  WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper,
  AQEShuffleReadExec, QueryStageExec, RecoveredShuffleStage, ShuffleQueryStageExec,
  ShuffleStageRecovery, ShuffleStageRecoveryInfo}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources.{FileFormat, WriteFilesExec, WriteFilesExecBase, WriteFilesSpec}
import org.apache.spark.sql.execution.datasources.v2.{RecoveryTaskCommitContext,
  RecoveryTaskCommitEnvelope}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, BroadcastExchangeLike, ShuffleExchangeExec, ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.COLUMN_BATCH_SIZE
import org.apache.spark.sql.internal.StaticSQLConf.SPARK_SESSION_EXTENSIONS
import org.apache.spark.sql.types.{DataType, Decimal, IntegerType, LongType, Metadata, StructType}
import org.apache.spark.sql.vectorized.{ColumnarArray, ColumnarBatch, ColumnarMap, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Test cases for the [[SparkSessionExtensions]].
 */
class SparkSessionExtensionSuite extends PlanTest with AdaptiveSparkPlanHelper {
  private def create(
      builder: SparkSessionExtensionsProvider): Seq[SparkSessionExtensionsProvider] = Seq(builder)

  private def stop(spark: SparkSession): Unit = {
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def withSession(
      builders: Seq[SparkSessionExtensionsProvider])(f: SparkSession => Unit): Unit = {
    val builder = SparkSession.builder().master("local[1]")
    builders.foreach(builder.withExtensions)
    val spark = builder.getOrCreate()
    try f(spark) finally {
      stop(spark)
    }
  }

  private def withTable(spark: SparkSession, tableNames: String*)(f: => Unit): Unit = {
    try f finally {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  test("inject analyzer rule") {
    withSession(Seq(_.injectResolutionRule(MyRule))) { session =>
      assert(session.sessionState.analyzer.extendedResolutionRules.contains(MyRule(session)))
    }
  }

  test("inject analyzer rule - hidden column") {
    withSession(Seq(_.injectResolutionRule(MyHiddenColumn))) { session: SparkSession =>
      val rel = LocalRelation(
        AttributeReference("a", IntegerType)(),
        AttributeReference("b", IntegerType)())
      rel.setTagValue[Long](LogicalPlan.PLAN_ID_TAG, 0L)

      val u = UnresolvedAttribute("x")
      u.setTagValue[Long](LogicalPlan.PLAN_ID_TAG, 0L)
      val proj = Project(Seq(u), rel)

      val df = Dataset.ofRows(session, proj)
      assert(df.schema.fieldNames === Array("x"))
    }
  }

  test("inject post hoc resolution analyzer rule") {
    withSession(Seq(_.injectPostHocResolutionRule(MyRule))) { session =>
      assert(session.sessionState.analyzer.postHocResolutionRules.contains(MyRule(session)))
    }
  }

  test("inject check analysis rule") {
    withSession(Seq(_.injectCheckRule(MyCheckRule))) { session =>
      assert(session.sessionState.analyzer.extendedCheckRules.contains(MyCheckRule(session)))
    }
  }

  test("inject optimizer rule") {
    withSession(Seq(_.injectOptimizerRule(MyRule))) { session =>
      assert(session.sessionState.optimizer.batches.flatMap(_.rules).contains(MyRule(session)))
    }
  }

  test("SPARK-33621: inject a pre CBO rule") {
    withSession(Seq(_.injectPreCBORule(MyRule))) { session =>
      assert(session.sessionState.optimizer.preCBORules.contains(MyRule(session)))
    }
  }

  test("SPARK-57194: inject pre operator optimization rule") {
    withSession(Seq(_.injectPreOperatorOptimizationRule(MyRule))) { session =>
      assert(session.sessionState.optimizer.preOperatorOptimizationRules.contains(MyRule(session)))
    }
  }

  test("inject spark planner strategy") {
    withSession(Seq(_.injectPlannerStrategy(MySparkStrategy))) { session =>
      assert(session.sessionState.planner.strategies.contains(MySparkStrategy(session)))
    }
  }

  test("inject parser") {
    val extension = create { extensions =>
      extensions.injectParser((_: SparkSession, _: ParserInterface) => CatalystSqlParser)
    }
    withSession(extension) { session =>
      assert(session.sessionState.sqlParser === CatalystSqlParser)
    }
  }

  test("inject multiple rules") {
    withSession(Seq(_.injectOptimizerRule(MyRule),
        _.injectPlannerStrategy(MySparkStrategy))) { session =>
      assert(session.sessionState.optimizer.batches.flatMap(_.rules).contains(MyRule(session)))
      assert(session.sessionState.planner.strategies.contains(MySparkStrategy(session)))
    }
  }

  test("inject stacked parsers") {
    val extension = create { extensions =>
      extensions.injectParser((_: SparkSession, _: ParserInterface) => CatalystSqlParser)
      extensions.injectParser(MyParser)
      extensions.injectParser(MyParser)
    }
    withSession(extension) { session =>
      val parser = MyParser(session, MyParser(session, CatalystSqlParser))
      assert(session.sessionState.sqlParser === parser)
    }
  }

  test("inject function") {
    val extensions = create { extensions =>
      extensions.injectFunction(MyExtensions.myFunction)
    }
    withSession(extensions) { session =>
      // Extension functions are treated as built-ins (unqualified, 3-part key)
      val builtinIdent = FunctionIdentifier(
        MyExtensions.myFunction._1.funcName.toLowerCase(Locale.ROOT),
        Some(CatalogManager.BUILTIN_NAMESPACE),
        Some(CatalogManager.SYSTEM_CATALOG_NAME))
      assert(session.sessionState.functionRegistry.lookupFunction(builtinIdent).isDefined)
    }
  }

  test("inject table function") {
    val extensions = create { extensions =>
      extensions.injectTableFunction(MyExtensions.myTableFunction)
    }
    withSession(extensions) { session =>
      // Extension table functions are in the builtin namespace (3-part key)
      val builtinIdent = FunctionIdentifier(
        MyExtensions.myTableFunction._1.funcName.toLowerCase(Locale.ROOT),
        Some(CatalogManager.BUILTIN_NAMESPACE),
        Some(CatalogManager.SYSTEM_CATALOG_NAME))
      assert(session.sessionState.tableFunctionRegistry.lookupFunction(builtinIdent).isDefined)
      val rows = session.sql("SELECT * FROM myTableFunc()").collect()
      assert(rows.length === 2, s"expected 2 rows, got ${rows.length}")
      assert(rows(0).getLong(0) === 0L && rows(1).getLong(0) === 1L)
    }
  }

  case class MyHintRule(spark: SparkSession) extends Rule[LogicalPlan] {
    val MY_HINT_NAME = Set("CONVERT_TO_EMPTY")

    override def apply(plan: LogicalPlan): LogicalPlan =
      plan.resolveOperators {
      case h: UnresolvedHint if MY_HINT_NAME.contains(h.name.toUpperCase(Locale.ROOT)) =>
        LocalRelation(h.output, data = Seq.empty, isStreaming = h.isStreaming)
    }
  }

  test("inject custom hint rule") {
    withSession(Seq(_.injectHintResolutionRule(MyHintRule))) { session =>
      assert(
        session.range(1).hint("CONVERT_TO_EMPTY").logicalPlan.isInstanceOf[LocalRelation],
        "plan is expected to be a local relation"
      )
    }
  }

  test("inject adaptive query prep rule") {
    val extensions = create { extensions =>
      // inject rule that will run during AQE query stage preparation and will add custom tags
      // to the plan
      extensions.injectQueryStagePrepRule(session => MyQueryStagePrepRule())
      // inject rule that will run during AQE query stage optimization and will verify that the
      // custom tags were written in the preparation phase
      extensions.injectColumnar(session =>
        MyColumnarRule(MyNewQueryStageRule(), MyNewQueryStageRule()))
    }
    withSession(extensions) { session =>
      session.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, true)
      assert(session.sessionState.adaptiveRulesHolder.queryStagePrepRules
        .contains(MyQueryStagePrepRule()))
      assert(session.sessionState.columnarRules.contains(
        MyColumnarRule(MyNewQueryStageRule(), MyNewQueryStageRule())))
      import session.implicits._
      val data = Seq((100L), (200L), (300L)).toDF("vals").repartition(1)
      val df = data.selectExpr("vals + 1")
      df.collect()
    }
  }

  test("inject shuffle stage recovery provider") {
    val recoveryAttempts = new AtomicInteger()
    val completedStages = new AtomicInteger()
    val extensions = create { extensions =>
      extensions.injectShuffleStageRecovery { _ =>
        new ShuffleStageRecovery {
          override def protocolVersion: Int = ShuffleStageRecovery.PROTOCOL_VERSION

          override def tryRecover(
              info: ShuffleStageRecoveryInfo): Option[RecoveredShuffleStage] = {
            assert(info.protocolVersion === ShuffleStageRecovery.PROTOCOL_VERSION)
            assert(info.planFingerprint ===
              ShuffleStageRecovery.fingerprint(info.canonicalizedPlan))
            assert(info.queryPlanFingerprint ===
              ShuffleStageRecovery.fingerprint(info.canonicalizedQueryPlan))
            recoveryAttempts.incrementAndGet()
            None
          }

          override def onStageCompleted(
              info: ShuffleStageRecoveryInfo,
              result: RecoveredShuffleStage): Unit = {
            assert(result.bytesByPartitionId.length === info.numPartitions)
            completedStages.incrementAndGet()
          }
        }
      }
    }
    withSession(extensions) { session =>
      session.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, true)
      session.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 2)
      assert(session.range(10).repartition(2).groupBy().count().head().getLong(0) === 10L)
      assert(recoveryAttempts.get() > 0)
      assert(completedStages.get() > 0)
    }
  }

  test("invalid shuffle stage recovery is rolled back and fails closed") {
    val aborts = new AtomicInteger()
    val extensions = create { extensions =>
      extensions.injectShuffleStageRecovery { _ =>
        new ShuffleStageRecovery {
          override def protocolVersion: Int = ShuffleStageRecovery.PROTOCOL_VERSION

          override def tryRecover(
              info: ShuffleStageRecoveryInfo): Option[RecoveredShuffleStage] = {
            Some(RecoveredShuffleStage(
              Seq.empty, 0L, Some(0L), ShuffleStageRecovery.PROTOCOL_VERSION))
          }

          override def abortRecovery(info: ShuffleStageRecoveryInfo, cause: Throwable): Unit = {
            aborts.incrementAndGet()
          }
        }
      }
    }
    withSession(extensions) { session =>
      session.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, true)
      session.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 2)
      val error = intercept[SparkException] {
        session.range(10).repartition(2).groupBy().count().collect()
      }
      assert(error.getMessage.contains("refusing to recompute"))
      assert(aborts.get() > 0)
    }
  }

  test("incompatible recovered shuffle protocol is rolled back and fails closed") {
    val aborts = new AtomicInteger()
    val extensions = create { extensions =>
      extensions.injectShuffleStageRecovery { _ =>
        new ShuffleStageRecovery {
          override def protocolVersion: Int = ShuffleStageRecovery.PROTOCOL_VERSION

          override def tryRecover(
              info: ShuffleStageRecoveryInfo): Option[RecoveredShuffleStage] = {
            Some(RecoveredShuffleStage(
              Seq.fill(info.numPartitions)(0L),
              0L,
              Some(0L),
              info.protocolVersion + 1))
          }

          override def abortRecovery(info: ShuffleStageRecoveryInfo, cause: Throwable): Unit = {
            assert(cause.getMessage.contains("does not match request protocol"))
            aborts.incrementAndGet()
          }
        }
      }
    }
    withSession(extensions) { session =>
      session.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, true)
      session.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 2)
      val error = intercept[SparkException] {
        session.range(10).repartition(2).groupBy().count().collect()
      }
      assert(error.getMessage.contains("refusing to recompute"))
      assert(aborts.get() > 0)
    }
  }

  test("incompatible shuffle provider protocol is rejected before adoption") {
    val recoveryAttempts = new AtomicInteger()
    val aborts = new AtomicInteger()
    val extensions = create { extensions =>
      extensions.injectShuffleStageRecovery { _ =>
        new ShuffleStageRecovery {
          override def protocolVersion: Int = ShuffleStageRecovery.PROTOCOL_VERSION + 1

          override def tryRecover(
              info: ShuffleStageRecoveryInfo): Option[RecoveredShuffleStage] = {
            recoveryAttempts.incrementAndGet()
            None
          }

          override def abortRecovery(info: ShuffleStageRecoveryInfo, cause: Throwable): Unit = {
            assert(cause.getMessage.contains("provider protocol"))
            aborts.incrementAndGet()
          }
        }
      }
    }
    withSession(extensions) { session =>
      session.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, true)
      session.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 2)
      val error = intercept[SparkException] {
        session.range(10).repartition(2).groupBy().count().collect()
      }
      assert(error.getMessage.contains("refusing to recompute"))
      assert(recoveryAttempts.get() === 0)
      assert(aborts.get() > 0)
    }
  }

  test("shuffle stage recovery provider failure is aborted and fails closed") {
    val aborts = new AtomicInteger()
    val extensions = create { extensions =>
      extensions.injectShuffleStageRecovery { _ =>
        new ShuffleStageRecovery {
          override def protocolVersion: Int = ShuffleStageRecovery.PROTOCOL_VERSION

          override def tryRecover(
              info: ShuffleStageRecoveryInfo): Option[RecoveredShuffleStage] = {
            throw new IllegalStateException("partial external adoption")
          }

          override def abortRecovery(info: ShuffleStageRecoveryInfo, cause: Throwable): Unit = {
            assert(cause.getMessage === "partial external adoption")
            aborts.incrementAndGet()
          }
        }
      }
    }
    withSession(extensions) { session =>
      session.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, true)
      session.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 2)
      val error = intercept[SparkException] {
        session.range(10).repartition(2).groupBy().count().collect()
      }
      assert(error.getMessage.contains("refusing to recompute"))
      assert(aborts.get() > 0)
    }
  }

  test("shuffle stage recovery fails rather than mixing state when rollback fails") {
    val extensions = create { extensions =>
      extensions.injectShuffleStageRecovery { _ =>
        new ShuffleStageRecovery {
          override def protocolVersion: Int = ShuffleStageRecovery.PROTOCOL_VERSION

          override def tryRecover(
              info: ShuffleStageRecoveryInfo): Option[RecoveredShuffleStage] = {
            throw new IllegalStateException("partial external adoption")
          }

          override def abortRecovery(info: ShuffleStageRecoveryInfo, cause: Throwable): Unit = {
            throw new IllegalStateException("external rollback failed")
          }
        }
      }
    }
    withSession(extensions) { session =>
      session.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, true)
      session.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 2)
      val error = intercept[SparkException] {
        session.range(10).repartition(2).groupBy().count().collect()
      }
      assert(error.getMessage.contains("refusing to recompute"))
      assert(error.getCause.getSuppressed.exists(
        _.getMessage.contains("external rollback failed")))
    }
  }

  test("accepted empty shuffle recovery bypasses map tasks") {
    val recoveredStages = new AtomicInteger()
    val taskStarts = new AtomicInteger()
    val extensions = create { extensions =>
      extensions.injectShuffleStageRecovery { _ =>
        new ShuffleStageRecovery {
          override def protocolVersion: Int = ShuffleStageRecovery.PROTOCOL_VERSION

          override def tryRecover(
              info: ShuffleStageRecoveryInfo): Option[RecoveredShuffleStage] = {
            recoveredStages.incrementAndGet()
            Some(RecoveredShuffleStage(
              Seq.fill(info.numPartitions)(0L),
              0L,
              Some(0L),
              ShuffleStageRecovery.PROTOCOL_VERSION))
          }
        }
      }
    }
    withSession(extensions) { session =>
      val listener = new SparkListener {
        override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
          taskStarts.incrementAndGet()
        }
      }
      session.sparkContext.addSparkListener(listener)
      try {
        session.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, true)
        session.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 2)
        val empty = session.range(10).where("id < 0").repartition(2)
        val aggregate = empty.groupBy().count()
        assert(aggregate.head().getLong(0) === 0L)
        assert(recoveredStages.get() > 0)
        assert(taskStarts.get() === 1)
      } finally {
        session.sparkContext.removeSparkListener(listener)
      }
    }
  }

  test("non-AQE shuffle recovery bypasses map tasks") {
    val recoveredStages = new AtomicInteger()
    val taskStarts = new AtomicInteger()
    val extensions = create { extensions =>
      extensions.injectShuffleStageRecovery { _ =>
        new ShuffleStageRecovery {
          override def protocolVersion: Int = ShuffleStageRecovery.PROTOCOL_VERSION

          override def tryRecover(
              info: ShuffleStageRecoveryInfo): Option[RecoveredShuffleStage] = {
            assert(info.stageId === -1)
            recoveredStages.incrementAndGet()
            Some(RecoveredShuffleStage(
              Seq.fill(info.numPartitions)(0L),
              0L,
              Some(0L),
              ShuffleStageRecovery.PROTOCOL_VERSION))
          }
        }
      }
    }
    withSession(extensions) { session =>
      val listener = new SparkListener {
        override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
          taskStarts.incrementAndGet()
        }
      }
      session.sparkContext.addSparkListener(listener)
      try {
        session.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, false)
        session.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 2)
        val empty = session.range(10).where("id < 0").repartition(2)
        assert(empty.groupBy().count().head().getLong(0) === 0L)
        assert(recoveredStages.get() > 0)
        assert(taskStarts.get() === 1)
      } finally {
        session.sparkContext.removeSparkListener(listener)
      }
    }
  }

  test("recovery provider durably selects a V2 source anchor before scan build") {
    RecoveryAnchorDataSource.appliedAnchor.set(null)
    val extensions = create { extensions =>
      extensions.injectShuffleStageRecovery { _ =>
        new ShuffleStageRecovery {
          override def protocolVersion: Int = ShuffleStageRecovery.PROTOCOL_VERSION

          override def resolveSourceAnchor(info: SourceRecoveryInfo): String = {
            assert(info.sourceId === "recovery-anchor-source")
            assert(info.currentAnchor === "current-version")
            "stored-version"
          }

          override def tryRecover(
              info: ShuffleStageRecoveryInfo): Option[RecoveredShuffleStage] = None
        }
      }
    }
    withSession(extensions) { session =>
      val rows = session.read.format(classOf[RecoveryAnchorDataSource].getName).load().collect()
      assert(rows.length === 1)
      assert(RecoveryAnchorDataSource.appliedAnchor.get() === "stored-version")
    }
  }

  test("recovery provider durably selects a V2 connector write ID") {
    RecoveryWriteDataSource.reset()
    val extensions = create { extensions =>
      extensions.injectShuffleStageRecovery { _ =>
        new ShuffleStageRecovery {
          override def protocolVersion: Int = ShuffleStageRecovery.PROTOCOL_VERSION

          override def resolveSourceAnchor(info: SourceRecoveryInfo): String = info.currentAnchor

          override def resolveWriteId(info: WriteRecoveryInfo): String = {
            assert(info.sinkId === "recovery-write-sink")
            assert(info.currentWriteId.nonEmpty)
            assert(info.operation.canonicalized === info.canonicalizedOperation)
            "durable-write-id"
          }

          override def taskCommitStore: Option[RecoveryTaskCommitStore] =
            Some(new RecoveryTestTaskCommitStore)

          override def tryRecover(
              info: ShuffleStageRecoveryInfo): Option[RecoveredShuffleStage] = None
        }
      }
    }
    withSession(extensions) { session =>
      session.range(2).write.format(classOf[RecoveryWriteDataSource].getName).mode("append").save()
      assert(RecoveryWriteDataSource.resolvedWriteId.get() === "durable-write-id")
    }
  }

  test("recoverable V2 write runs only partitions without durable commit messages") {
    RecoveryWriteDataSource.reset(recoveredPartitions = Set(0, 2))
    withSession(recoveryWriteExtensions) { session =>
      session.range(8).repartition(4).write
        .format(classOf[RecoveryWriteDataSource].getName).mode("append").save()
      assert(RecoveryWriteDataSource.writerCommits.get() === 2)
      assert(RecoveryWriteDataSource.globalCommits.get() === 1)
      assert(RecoveryWriteDataSource.factoryCreations.get() === 1)
    }
  }

  test("recoverable V2 write runs no tasks or global commit when already committed") {
    RecoveryWriteDataSource.reset(committed = true)
    withSession(recoveryWriteExtensions) { session =>
      session.range(8).repartition(4).write
        .format(classOf[RecoveryWriteDataSource].getName).mode("append").save()
      assert(RecoveryWriteDataSource.writerCommits.get() === 0)
      assert(RecoveryWriteDataSource.globalCommits.get() === 0)
      assert(RecoveryWriteDataSource.factoryCreations.get() === 0)
    }
  }

  test("recovery-enabled V2 write fails closed for a non-recoverable batch writer") {
    NonRecoverableWriteDataSource.factoryCreations.set(0)
    withSession(recoveryWriteExtensions) { session =>
      val error = intercept[IllegalStateException] {
        session.range(2).write.format(classOf[NonRecoverableWriteDataSource].getName)
          .mode("append").save()
      }
      assert(error.getMessage.contains(classOf[SupportsBatchWriteRecovery].getName))
      assert(NonRecoverableWriteDataSource.factoryCreations.get() === 0)
    }
  }

  test("recoverable V2 write uses recovery-aware abort after a resumed failure") {
    RecoveryWriteDataSource.reset(recoveredPartitions = Set(0), failGlobalCommit = true)
    withSession(recoveryWriteExtensions) { session =>
      intercept[RuntimeException] {
        session.range(4).repartition(2).write
          .format(classOf[RecoveryWriteDataSource].getName).mode("append").save()
      }
      assert(RecoveryWriteDataSource.recoveryAborts.get() === 1)
      assert(RecoveryWriteDataSource.normalAborts.get() === 0)
    }
  }

  test("recoverable V2 write discards speculative output and commits the canonical winner") {
    RecoveryWriteDataSource.reset(forceCanonicalLoser = true)
    withSession(recoveryWriteExtensions) { session =>
      session.range(2).repartition(1).write
        .format(classOf[RecoveryWriteDataSource].getName).mode("append").save()
      assert(RecoveryWriteDataSource.writerCommits.get() === 1)
      assert(RecoveryWriteDataSource.discardedOutputs.get() === 1)
      assert(RecoveryWriteDataSource.committedPartitionIds.get().toSeq === Seq(1000))
    }
  }

  test("recoverable V2 task preflight observes a late winner without creating a writer") {
    RecoveryWriteDataSource.reset(publishBeforeExecutorPreflight = true)
    withSession(recoveryWriteExtensions) { session =>
      session.range(2).repartition(1).write
        .format(classOf[RecoveryWriteDataSource].getName).mode("append").save()
      assert(RecoveryWriteDataSource.factoryCreations.get() === 1)
      assert(RecoveryWriteDataSource.writerCreations.get() === 0)
      assert(RecoveryWriteDataSource.writerCommits.get() === 0)
      assert(RecoveryWriteDataSource.committedPartitionIds.get().toSeq === Seq(2000))
    }
  }

  test("recoverable V2 write rejects null connector global state before starting tasks") {
    withSession(recoveryWriteExtensions) { session =>
      RecoveryWriteDataSource.reset(malformedState = "null-state")
      intercept[RuntimeException] {
        session.range(4).repartition(2).write
          .format(classOf[RecoveryWriteDataSource].getName).mode("append").save()
      }
      assert(RecoveryWriteDataSource.factoryCreations.get() === 0)
    }
  }

  private def recoveryWriteExtensions: Seq[SparkSessionExtensionsProvider] = create { extensions =>
    extensions.injectShuffleStageRecovery { _ =>
      new ShuffleStageRecovery {
        override def protocolVersion: Int = ShuffleStageRecovery.PROTOCOL_VERSION

        override def resolveSourceAnchor(info: SourceRecoveryInfo): String = info.currentAnchor
        override def resolveWriteId(info: WriteRecoveryInfo): String = "durable-write-id"
        override def taskCommitStore: Option[RecoveryTaskCommitStore] =
          Some(new RecoveryTestTaskCommitStore)
        override def tryRecover(
            info: ShuffleStageRecoveryInfo): Option[RecoveredShuffleStage] = None
      }
    }
  }

  test("inject columnar AQE on") {
    testInjectColumnar(true)
  }

  test("inject columnar AQE off") {
    testInjectColumnar(false)
  }

  test("inject plan normalization rules") {
    val extensions = create { extensions =>
      extensions.injectPlanNormalizationRule { session =>
        org.apache.spark.sql.catalyst.optimizer.PushDownPredicates
      }
    }
    withSession(extensions) { session =>
      import session.implicits._
      val df = Seq((1, "a"), (2, "b")).toDF("i", "s")
      df.select("i").filter($"i" > 1).cache()
      assert(find(df.filter($"i" > 1).select("i").queryExecution.executedPlan) {
        case _: org.apache.spark.sql.execution.columnar.InMemoryTableScanExec => true
        case _ => false
      }.isDefined)
    }
  }

  test("SPARK-39991: AQE should retain column statistics from completed query stages") {
    val extensions = create { extensions =>
      extensions.injectColumnar(_ =>
        MyColumnarRule(PreRuleReplaceAddWithBrokenVersion(), MyPostRule()))
    }
    withSession(extensions) { session =>
      session.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, true)
      session.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      assert(session.sessionState.columnarRules.contains(
        MyColumnarRule(PreRuleReplaceAddWithBrokenVersion(), MyPostRule())))
      import session.implicits._
      // perform a join to inject a shuffle exchange
      val left = Seq((1, 50L), (2, 100L), (3, 150L)).toDF("l1", "l2")
      val right = Seq((1, 50L), (2, 100L), (3, 150L)).toDF("r1", "r2")
      val data = left.join(right, $"l1" === $"r1")
        // repartitioning avoids having the add operation pushed up into the LocalTableScan
        .repartition(1)
      val df = data.selectExpr("l2 + r2")
      // execute the plan so that the final adaptive plan is available
      df.collect()

      // check that column stats exist
      def findColumnStats(plan: SparkPlan,
          columnStats: ListBuffer[AttributeMap[ColumnStat]]): Unit = {
        plan match {
          case a: AdaptiveSparkPlanExec =>
            findColumnStats(stripAQEPlan(a), columnStats)
          case qs: ShuffleQueryStageExec =>
            columnStats += qs.computeStats().get.attributeStats
            findColumnStats(qs.plan, columnStats)
          case _ =>
            plan.children.foreach(findColumnStats(_, columnStats))
        }
      }

      // check for expected column stats (hard-coded in MyShuffleExchangeExec)
      val columnStats = ListBuffer[AttributeMap[ColumnStat]]()
      findColumnStats(df.queryExecution.executedPlan, columnStats)
      assert(columnStats.length == 3)
      assert(columnStats.forall(s => s.forall {
        case (_, columnStat) => columnStat.distinctCount.contains(BigInt(123))
      }))
    }
  }

  private def testInjectColumnar(enableAQE: Boolean): Unit = {
    def collectPlanSteps(plan: SparkPlan): Seq[Int] = plan match {
      case a: AdaptiveSparkPlanExec =>
        assert(a.toString.startsWith("AdaptiveSparkPlan isFinalPlan=true"))
        collectPlanSteps(a.executedPlan)
      case _ => plan.collect {
        case _: ReplacedRowToColumnarExec => 1
        case _: ColumnarProjectExec => 10
        case _: ColumnarToRowExec => 100
        case s: QueryStageExec => collectPlanSteps(s.plan).sum
        case _: MyShuffleExchangeExec => 1000
        case _: MyBroadcastExchangeExec => 10000
      }
    }

    val extensions = create { extensions =>
      extensions.injectColumnar(session =>
        MyColumnarRule(PreRuleReplaceAddWithBrokenVersion(), MyPostRule()))
    }
    withSession(extensions) { session =>
      session.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, enableAQE)
      assert(session.sessionState.columnarRules.contains(
        MyColumnarRule(PreRuleReplaceAddWithBrokenVersion(), MyPostRule())))
      import session.implicits._
      // perform a join to inject a broadcast exchange
      val left = Seq((1, 50L), (2, 100L), (3, 150L)).toDF("l1", "l2")
      val right = Seq((1, 50L), (2, 100L), (3, 150L)).toDF("r1", "r2")
      val data = left.join(right, $"l1" === $"r1")
        // repartitioning avoids having the add operation pushed up into the LocalTableScan
        .repartition(1)
      val df = data.selectExpr("l2 + r2")
      // execute the plan so that the final adaptive plan is available when AQE is on
      df.collect()
      val found = collectPlanSteps(df.queryExecution.executedPlan).sum
      // 1 MyBroadcastExchangeExec
      // 1 MyShuffleExchangeExec
      // 1 ColumnarToRowExec
      // 2 ColumnarProjectExec
      // 1 ReplacedRowToColumnarExec
      // so 11121 is expected.
      assert(found == 11121)

      // Verify that we get back the expected, wrong, result
      val result = df.collect()
      assert(result(0).getLong(0) == 101L) // Check that broken columnar Add was used.
      assert(result(1).getLong(0) == 201L)
      assert(result(2).getLong(0) == 301L)

      withTempPath { path =>
        val e = intercept[Exception](df.write.parquet(path.getCanonicalPath))
        assert(e.getMessage == "columnar write")
      }
    }
  }

  test("reset column vectors") {
    val session = SparkSession.builder()
      .master("local[1]")
      .config(COLUMN_BATCH_SIZE.key, 2)
      .withExtensions { extensions =>
        extensions.injectColumnar(session =>
          MyColumnarRule(PreRuleReplaceAddWithBrokenVersion(), MyPostRule())) }
      .getOrCreate()

    try {
      assert(session.sessionState.columnarRules.contains(
        MyColumnarRule(PreRuleReplaceAddWithBrokenVersion(), MyPostRule())))
      import session.implicits._

      val input = Seq((100L), (200L), (300L))
      val data = input.toDF("vals").repartition(1)
      val df = data.selectExpr("vals + 1")
      val result = df.collect()
      assert(result sameElements input.map(x => Row(x + 2)))
    } finally {
      stop(session)
    }
  }

  test("use custom class for extensions") {
    val session = SparkSession.builder()
      .master("local[1]")
      .config(SPARK_SESSION_EXTENSIONS.key, classOf[MyExtensions].getCanonicalName)
      .getOrCreate()
    try {
      assert(session.sessionState.planner.strategies.contains(MySparkStrategy(session)))
      assert(session.sessionState.analyzer.extendedResolutionRules.contains(MyRule(session)))
      assert(session.sessionState.analyzer.postHocResolutionRules.contains(MyRule(session)))
      assert(session.sessionState.analyzer.extendedCheckRules.contains(MyCheckRule(session)))
      assert(session.sessionState.optimizer.batches.flatMap(_.rules).contains(MyRule(session)))
      assert(session.sessionState.sqlParser.isInstanceOf[MyParser])
      // Extension functions are in the builtin namespace (3-part key)
      val builtinIdent = FunctionIdentifier(
        MyExtensions.myFunction._1.funcName.toLowerCase(Locale.ROOT),
        Some(CatalogManager.BUILTIN_NAMESPACE),
        Some(CatalogManager.SYSTEM_CATALOG_NAME))
      assert(session.sessionState.functionRegistry.lookupFunction(builtinIdent).isDefined)
      assert(session.sessionState.columnarRules.contains(
        MyColumnarRule(PreRuleReplaceAddWithBrokenVersion(), MyPostRule())))
    } finally {
      stop(session)
    }
  }

  test("use multiple custom class for extensions in the specified order") {
    val session = SparkSession.builder()
      .master("local[1]")
      .config(SPARK_SESSION_EXTENSIONS.key, Seq(
        classOf[MyExtensions2].getCanonicalName,
        classOf[MyExtensions].getCanonicalName).mkString(","))
      .getOrCreate()
    try {
      assert(session.sessionState.planner.strategies.containsSlice(
        Seq(MySparkStrategy2(session), MySparkStrategy(session))))
      val orderedRules = Seq(MyRule2(session), MyRule(session))
      val orderedCheckRules = Seq(MyCheckRule2(session), MyCheckRule(session))
      val parser = MyParser(session, CatalystSqlParser)
      assert(session.sessionState.analyzer.extendedResolutionRules.containsSlice(orderedRules))
      assert(session.sessionState.analyzer.postHocResolutionRules.containsSlice(orderedRules))
      assert(session.sessionState.analyzer.extendedCheckRules.containsSlice(orderedCheckRules))
      assert(session.sessionState.optimizer.batches.flatMap(_.rules).filter(orderedRules.contains)
        .containsSlice(orderedRules ++ orderedRules)) // The optimizer rules are duplicated
      assert(session.sessionState.sqlParser === parser)
      // Extension functions are in the builtin namespace (3-part key)
      val builtinIdent1 = FunctionIdentifier(
        MyExtensions.myFunction._1.funcName.toLowerCase(Locale.ROOT),
        Some(CatalogManager.BUILTIN_NAMESPACE),
        Some(CatalogManager.SYSTEM_CATALOG_NAME))
      val builtinIdent2 = FunctionIdentifier(
        MyExtensions2.myFunction._1.funcName.toLowerCase(Locale.ROOT),
        Some(CatalogManager.BUILTIN_NAMESPACE),
        Some(CatalogManager.SYSTEM_CATALOG_NAME))
      assert(session.sessionState.functionRegistry.lookupFunction(builtinIdent1).isDefined)
      assert(session.sessionState.functionRegistry.lookupFunction(builtinIdent2).isDefined)
    } finally {
      stop(session)
    }
  }

  test("allow an extension to be duplicated") {
    val session = SparkSession.builder()
      .master("local[1]")
      .config(SPARK_SESSION_EXTENSIONS.key, Seq(
        classOf[MyExtensions].getCanonicalName,
        classOf[MyExtensions].getCanonicalName).mkString(","))
      .getOrCreate()
    try {
      assert(session.sessionState.planner.strategies.count(_ === MySparkStrategy(session)) === 2)
      assert(session.sessionState.analyzer.extendedResolutionRules.count(_ === MyRule(session)) ===
        2)
      assert(session.sessionState.analyzer.postHocResolutionRules.count(_ === MyRule(session)) ===
        2)
      assert(session.sessionState.analyzer.extendedCheckRules.count(_ === MyCheckRule(session)) ===
        2)
      assert(session.sessionState.optimizer.batches.flatMap(_.rules)
        .count(_ === MyRule(session)) === 4) // The optimizer rules are duplicated
      val outerParser = session.sessionState.sqlParser
      assert(outerParser.isInstanceOf[MyParser])
      assert(outerParser.asInstanceOf[MyParser].delegate.isInstanceOf[MyParser])
      // Extension functions are in the builtin namespace (3-part key)
      val builtinIdent = FunctionIdentifier(
        MyExtensions.myFunction._1.funcName.toLowerCase(Locale.ROOT),
        Some(CatalogManager.BUILTIN_NAMESPACE),
        Some(CatalogManager.SYSTEM_CATALOG_NAME))
      assert(session.sessionState.functionRegistry.lookupFunction(builtinIdent).isDefined)
    } finally {
      stop(session)
    }
  }

  test("use the last registered function name when there are duplicates") {
    val session = SparkSession.builder()
      .master("local[1]")
      .config(SPARK_SESSION_EXTENSIONS.key, Seq(
        classOf[MyExtensions2].getCanonicalName,
        classOf[MyExtensions2Duplicate].getCanonicalName).mkString(","))
      .getOrCreate()
    try {
      // Extension functions are in the builtin namespace (3-part key); last registered wins
      val builtinIdent = FunctionIdentifier(
        "myfunction2",
        Some(CatalogManager.BUILTIN_NAMESPACE),
        Some(CatalogManager.SYSTEM_CATALOG_NAME))
      val lastRegistered = session.sessionState.functionRegistry.lookupFunctionBuilder(builtinIdent)
      assert(lastRegistered.isDefined)
      assert(lastRegistered.get !== MyExtensions2.myFunction._3)
      assert(lastRegistered.get === MyExtensions2Duplicate.myFunction._3)
    } finally {
      stop(session)
    }
  }

  test("SPARK-35380: Loading extensions from ServiceLoader") {
    val builder = SparkSession.builder().master("local[1]")

    Seq(None, Some(classOf[YourExtensions].getName)).foreach { ext =>
      ext.foreach(builder.config(SPARK_SESSION_EXTENSIONS.key, _))
      val session = builder.getOrCreate()
      try {
        assert(session.sql("select get_fake_app_name()").head().getString(0) === "Fake App Name")
      } finally {
        stop(session)
      }
    }
  }

  test("SPARK-35673: user-defined hint and unrecognized hint in subquery") {
    withSession(Seq(_.injectPostHocResolutionRule(MyHintRule))) { session =>
      // unrecognized hint
      QueryTest.checkAnswer(
        session.sql(
          """
            |SELECT *
            |FROM (
            |    SELECT /*+ some_random_hint_that_does_not_exist */ 42
            |)
            |""".stripMargin),
        Row(42) :: Nil)

      // user-defined hint
      QueryTest.checkAnswer(
        session.sql(
          """
            |SELECT *
            |FROM (
            |    SELECT /*+ CONVERT_TO_EMPTY */ 42
            |)
            |""".stripMargin),
        Nil)
    }
  }

  test("SPARK-37202: temp view refers a inject function") {
    val extensions = create { extensions =>
      extensions.injectFunction(MyExtensions.myFunction)
    }
    withSession(extensions) { session =>
      session.sql("CREATE TEMP VIEW v AS SELECT myFunction(a) FROM VALUES(1), (2) t(a)")
      session.sql("SELECT * FROM v")
    }
  }

  test("SPARK-38697: Extend SparkSessionExtensions to inject rules into AQE Optimizer") {
    def executedPlan(df: Dataset[java.lang.Long]): SparkPlan = {
      assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
      stripAQEPlan(df.queryExecution.executedPlan)
    }
    val extensions = create { extensions =>
      extensions.injectRuntimeOptimizerRule(_ => AddLimit)
    }
    withSession(extensions) { session =>
      assert(session.sessionState.adaptiveRulesHolder.runtimeOptimizerRules.contains(AddLimit))

      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
        val df = session.range(2).repartition()
        assert(!executedPlan(df).isInstanceOf[CollectLimitExec])
        df.collect()
        assert(executedPlan(df).isInstanceOf[CollectLimitExec])
      }
    }
  }

  test("SPARK-42963: Extend SparkSessionExtensions to inject rules into AQE query stage " +
    "optimizer") {
    val extensions = create { extensions =>
      extensions.injectQueryStageOptimizerRule(_ => RequireAtLeaseTwoPartitions)
    }
    withSession(extensions) { session =>
      assert(session.sessionState.adaptiveRulesHolder.queryStageOptimizerRules
        .contains(RequireAtLeaseTwoPartitions))
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "3") {
        val df = session.range(1).repartition()
        df.collect()
        assert(df.rdd.partitions.length == 3)
      }
    }
  }

  test("SPARK-46170: Support inject adaptive query post planner strategy rules in " +
    "SparkSessionExtensions") {
    val extensions = create { extensions =>
      extensions.injectQueryPostPlannerStrategyRule(_ => MyQueryPostPlannerStrategyRule)
    }
    withSession(extensions) { session =>
      assert(session.sessionState.adaptiveRulesHolder.queryPostPlannerStrategyRules
        .contains(MyQueryPostPlannerStrategyRule))
      import session.implicits._
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "3",
          SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false") {
        val input = Seq(10, 20, 10).toDF("c1")
        val df = input.groupBy("c1").count()
        df.collect()
        assert(df.rdd.partitions.length == 1)
        assert(collectFirst(df.queryExecution.executedPlan) {
          case s: ShuffleExchangeExec if s.outputPartitioning == SinglePartition => true
        }.isDefined)
        assert(collectFirst(df.queryExecution.executedPlan) {
          case _: SortExec => true
        }.isDefined)
      }
    }
  }

  test("custom aggregate hint") {
    // The custom hint allows us to replace the aggregate (without grouping keys) with just
    // Literal.
    withSession(Seq(_.injectHintResolutionRule(CustomAggregateHintResolutionRule),
      _.injectOptimizerRule(CustomAggregateRule))) { session =>
      val res = session.range(10).agg(max("id")).as("max_id")
        .hint("MAX_VALUE", "id", 10)
        .queryExecution.optimizedPlan
      assert(res.isInstanceOf[Aggregate])
      val expectedAlias = Alias(Literal(10L), "max(id)")()
      compareExpressions(expectedAlias, res.asInstanceOf[Aggregate].aggregateExpressions.head)
    }
  }

  test("custom sort hint") {
    // The custom hint allows us to replace the sort with its input
    withSession(Seq(_.injectHintResolutionRule(CustomSortHintResolutionRule),
      _.injectOptimizerRule(CustomSortRule))) { session =>
      val res = session.range(10).sort("id")
        .hint("INPUT_SORTED")
        .queryExecution.optimizedPlan
      assert(res.collect {case s: Sort => s}.isEmpty)
    }
  }

  test("early batch rule is applied on resolved IDENTIFIER") {
    var ruleApplied = false

    case class UnresolvedRelationRule(spark: SparkSession) extends Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
        case r: UnresolvedRelation =>
          ruleApplied = true
          r
      }
    }

    withSession(Seq(_.injectHintResolutionRule(UnresolvedRelationRule))) { session =>
      withTable(session, "my_table") {
        session.sql("CREATE TABLE IF NOT EXISTS my_table (col1 INT)")
        ruleApplied = false

        session.sql("SELECT * FROM IDENTIFIER('my_' || 'table')").collect()
        assert(ruleApplied)
      }
    }
  }
}

case class MyRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan
}

case class MyHiddenColumn(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case rel: LocalRelation if rel.output.size == 2 =>
      // rel.output.size == 2 for idempotence
      val newRel = rel.copy(
        output = rel.output :+ AttributeReference("x", IntegerType)()
      )
      assert(rel.getTagValue(LogicalPlan.PLAN_ID_TAG).contains(0L))
      newRel.setTagValue(LogicalPlan.PLAN_ID_TAG, 0L)
      newRel
  }
}

case class MyCheckRule(spark: SparkSession) extends (LogicalPlan => Unit) {
  override def apply(plan: LogicalPlan): Unit = { }
}

case class MySparkStrategy(spark: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = Seq.empty
}

case class MyParser(spark: SparkSession, delegate: ParserInterface) extends ParserInterface {
  override def parsePlan(sqlText: String): LogicalPlan =
    delegate.parsePlan(sqlText)

  override def parseExpression(sqlText: String): Expression =
    delegate.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    delegate.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    delegate.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType =
    delegate.parseDataType(sqlText)

  override def parseQuery(sqlText: String): LogicalPlan =
    delegate.parseQuery(sqlText)

  override def parseRoutineParam(sqlText: String): StructType = {
    delegate.parseRoutineParam(sqlText)
  }

  override def splitStatements(sqlText: String): SqlStatementSplitResult =
    delegate.splitStatements(sqlText)
}

object MyExtensions {

  val myFunction = (FunctionIdentifier("myFunction"),
    new ExpressionInfo(
      "noClass",
      "myDb",
      "myFunction",
      "usage",
      "extended usage",
      "    Examples:",
      """
       note
      """,
      "",
      "3.0.0",
      """
       deprecated
      """,
      ""),
    (_: Seq[Expression]) => Literal(5, IntegerType))

  val myTableFunction = (FunctionIdentifier("myTableFunc"),
    new ExpressionInfo(
      "noClass",
      "",
      "myTableFunc",
      "usage",
      "",
      "",
      "",
      "",
      "3.0.0",
      "",
      ""),
    (_: Seq[Expression]) => Range(0, 2, 1, None, Range.getOutputAttrs, isStreaming = false))
}

case class CloseableColumnBatchIterator(itr: Iterator[ColumnarBatch],
    f: ColumnarBatch => ColumnarBatch) extends Iterator[ColumnarBatch] {
  var cb: ColumnarBatch = null

  private def closeCurrentBatch(): Unit = {
    if (cb != null) {
      cb.close
      cb = null
    }
  }

  TaskContext.get().addTaskCompletionListener[Unit]((tc: TaskContext) => {
    closeCurrentBatch()
  })

  override def hasNext: Boolean = {
    closeCurrentBatch()
    itr.hasNext
  }

  override def next(): ColumnarBatch = {
    closeCurrentBatch()
    cb = f(itr.next())
    cb
  }
}

object NoCloseColumnVector extends Logging {
  def wrapIfNeeded(cv: ColumnVector): NoCloseColumnVector = cv match {
    case ref: NoCloseColumnVector =>
      ref
    case vec => NoCloseColumnVector(vec)
  }
}

/**
 * Provide a ColumnVector so ColumnarExpression can close temporary values without
 * having to guess what type it really is.
 */
case class NoCloseColumnVector(wrapped: ColumnVector) extends ColumnVector(wrapped.dataType) {
  private var refCount = 1

  /**
   * Don't actually close the ColumnVector this wraps.  The producer of the vector will take
   * care of that.
   */
  override def close(): Unit = {
    // Empty
  }

  override def hasNull: Boolean = wrapped.hasNull

  override def numNulls(): Int = wrapped.numNulls

  override def isNullAt(rowId: Int): Boolean = wrapped.isNullAt(rowId)

  override def getBoolean(rowId: Int): Boolean = wrapped.getBoolean(rowId)

  override def getByte(rowId: Int): Byte = wrapped.getByte(rowId)

  override def getShort(rowId: Int): Short = wrapped.getShort(rowId)

  override def getInt(rowId: Int): Int = wrapped.getInt(rowId)

  override def getLong(rowId: Int): Long = wrapped.getLong(rowId)

  override def getFloat(rowId: Int): Float = wrapped.getFloat(rowId)

  override def getDouble(rowId: Int): Double = wrapped.getDouble(rowId)

  override def getArray(rowId: Int): ColumnarArray = wrapped.getArray(rowId)

  override def getMap(ordinal: Int): ColumnarMap = wrapped.getMap(ordinal)

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal =
    wrapped.getDecimal(rowId, precision, scale)

  override def getUTF8String(rowId: Int): UTF8String = wrapped.getUTF8String(rowId)

  override def getBinary(rowId: Int): Array[Byte] = wrapped.getBinary(rowId)

  override def getChild(ordinal: Int): ColumnVector = wrapped.getChild(ordinal)
}

trait ColumnarExpression extends Expression with Serializable {
  /**
   * Returns true if this expression supports columnar processing through [[columnarEval]].
   */
  def supportsColumnar: Boolean = true

  /**
   * Returns the result of evaluating this expression on the entire
   * [[org.apache.spark.sql.vectorized.ColumnarBatch]]. The result of
   * calling this may be a single [[org.apache.spark.sql.vectorized.ColumnVector]] or a scalar
   * value. Scalar values typically happen if they are a part of the expression i.e. col("a") + 100.
   * In this case the 100 is a [[org.apache.spark.sql.catalyst.expressions.Literal]] that
   * [[org.apache.spark.sql.catalyst.expressions.Add]] would have to be able to handle.
   *
   * By convention any [[org.apache.spark.sql.vectorized.ColumnVector]] returned by [[columnarEval]]
   * is owned by the caller and will need to be closed by them. This can happen by putting it into
   * a [[org.apache.spark.sql.vectorized.ColumnarBatch]] and closing the batch or by closing the
   * vector directly if it is a temporary value.
   */
  def columnarEval(batch: ColumnarBatch): Any = {
    throw new IllegalStateException(s"Internal Error ${this.getClass} has column support mismatch")
  }

  // We need to override equals because we are subclassing a case class
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[ColumnarExpression]
  }

  override def hashCode(): Int = super.hashCode()
}

object ColumnarBindReferences extends Logging {

  // Mostly copied from BoundAttribute.scala so we can do columnar processing
  def bindReference[A <: ColumnarExpression](
      expression: A,
      input: AttributeSeq,
      allowFailures: Boolean = false): A = {
    expression.transform { case a: AttributeReference =>
      val ordinal = input.indexOf(a.exprId)
      if (ordinal == -1) {
        if (allowFailures) {
          a
        } else {
          sys.error(s"Couldn't find $a in ${input.attrs.mkString("[", ",", "]")}")
        }
      } else {
        new ColumnarBoundReference(ordinal, a.dataType, input(ordinal).nullable)
      }
    }.asInstanceOf[A]
  }

  /**
   * A helper function to bind given expressions to an input schema.
   */
  def bindReferences[A <: ColumnarExpression](
      expressions: Seq[A],
      input: AttributeSeq): Seq[A] = {
    expressions.map(ColumnarBindReferences.bindReference(_, input))
  }
}

class ColumnarBoundReference(ordinal: Int, dataType: DataType, nullable: Boolean)
  extends BoundReference(ordinal, dataType, nullable) with ColumnarExpression {

  override def columnarEval(batch: ColumnarBatch): Any = {
    // Because of the convention that the returned ColumnVector must be closed by the
    // caller we wrap this column vector so a close is a NOOP, and let the original source
    // of the vector close it.
    NoCloseColumnVector.wrapIfNeeded(batch.column(ordinal))
  }
}

class ColumnarAlias(child: ColumnarExpression, name: String)(
    override val exprId: ExprId = NamedExpression.newExprId,
    override val qualifier: Seq[String] = Seq.empty,
    override val explicitMetadata: Option[Metadata] = None,
    override val nonInheritableMetadataKeys: Seq[String] = Seq.empty)
  extends Alias(child, name)(exprId, qualifier, explicitMetadata, nonInheritableMetadataKeys)
  with ColumnarExpression {

  override def columnarEval(batch: ColumnarBatch): Any = child.columnarEval(batch)

  override protected def withNewChildInternal(newChild: Expression): ColumnarAlias =
    new ColumnarAlias(newChild.asInstanceOf[ColumnarExpression], name)(exprId, qualifier,
      explicitMetadata, nonInheritableMetadataKeys)
}

class ColumnarAttributeReference(
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    override val metadata: Metadata = Metadata.empty)(
    override val exprId: ExprId = NamedExpression.newExprId,
    override val qualifier: Seq[String] = Seq.empty[String])
  extends AttributeReference(name, dataType, nullable, metadata)(exprId, qualifier)
  with ColumnarExpression {

  // No columnar eval is needed because this must be bound before it is evaluated
}

class ColumnarLiteral (value: Any, dataType: DataType) extends Literal(value, dataType)
  with ColumnarExpression {
  override def columnarEval(batch: ColumnarBatch): Any = value
}

/**
 * A version of ProjectExec that adds in columnar support.
 */
class ColumnarProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
  extends ProjectExec(projectList, child) {

  override def supportsColumnar: Boolean =
    projectList.forall(_.asInstanceOf[ColumnarExpression].supportsColumnar)

  // Disable code generation
  override def supportCodegen: Boolean = false

  override def doExecuteColumnar() : RDD[ColumnarBatch] = {
    val boundProjectList: Seq[Any] =
      ColumnarBindReferences.bindReferences(
        projectList.asInstanceOf[Seq[ColumnarExpression]], child.output)
    val rdd = child.executeColumnar()
    rdd.mapPartitions((itr) => CloseableColumnBatchIterator(itr,
      (cb) => {
        val newColumns = boundProjectList.map(
          expr => expr.asInstanceOf[ColumnarExpression].columnarEval(cb).asInstanceOf[ColumnVector]
        ).toArray
        new ColumnarBatch(newColumns, cb.numRows())
      })
    )
  }

  // We have to override equals because subclassing a case class like ProjectExec is not that clean
  // One of the issues is that the generated equals will see ColumnarProjectExec and ProjectExec
  // as being equal and this can result in the withNewChildren method not actually replacing
  // anything
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[ColumnarProjectExec]
  }

  override def hashCode(): Int = super.hashCode()

  override def withNewChildInternal(newChild: SparkPlan): ColumnarProjectExec =
    new ColumnarProjectExec(projectList, newChild)
}

case class ColumnarWriteExec(
    child: SparkPlan,
    fileFormat: FileFormat,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    options: Map[String, String],
    staticPartitions: TablePartitionSpec) extends WriteFilesExecBase {

  override def supportsColumnar: Boolean = true

  override def doExecuteWrite(writeFilesSpec: WriteFilesSpec): RDD[WriterCommitMessage] = {
    assert(child.supportsColumnar)
    throw new Exception("columnar write")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarWriteExec =
    ColumnarWriteExec(
      newChild, fileFormat, partitionColumns, bucketSpec, options, staticPartitions)
}

/**
 * A version of add that supports columnar processing for longs.  This version is broken
 * on purpose so it adds the numbers plus 1 so that the tests can show that it was replaced.
 */
class BrokenColumnarAdd(
    left: ColumnarExpression,
    right: ColumnarExpression,
    failOnError: Boolean = false)
  extends Add(left, right, NumericEvalContext(EvalMode.fromBoolean(failOnError)))
    with ColumnarExpression {

  override def supportsColumnar: Boolean = left.supportsColumnar && right.supportsColumnar

  override def columnarEval(batch: ColumnarBatch): Any = {
    var lhs: Any = null
    var rhs: Any = null
    var ret: Any = null
    try {
      lhs = left.columnarEval(batch)
      rhs = right.columnarEval(batch)

      (lhs, rhs) match {
        case (null, null) =>
          ret = null
        case (l: ColumnVector, r: ColumnVector) =>
          val result = new OnHeapColumnVector(batch.numRows(), dataType)
          ret = result

          for (i <- 0 until batch.numRows()) {
            result.appendLong(l.getLong(i) + r.getLong(i) + 1) // BUG to show we replaced Add
          }
        case (l: Long, r: ColumnVector) =>
          val result = new OnHeapColumnVector(batch.numRows(), dataType)
          ret = result

          for (i <- 0 until batch.numRows()) {
            result.appendLong(l + r.getLong(i) + 1) // BUG to show we replaced Add
          }
        case (l: ColumnVector, r: Long) =>
          val result = new OnHeapColumnVector(batch.numRows(), dataType)
          ret = result

          for (i <- 0 until batch.numRows()) {
            result.appendLong(l.getLong(i) + r + 1) // BUG to show we replaced Add
          }
        case  (l, r) =>
          ret = nullSafeEval(l, r)
      }
    } finally {
      if (lhs != null && lhs.isInstanceOf[ColumnVector]) {
        lhs.asInstanceOf[ColumnVector].close()
      }
      if (rhs != null && rhs.isInstanceOf[ColumnVector]) {
        rhs.asInstanceOf[ColumnVector].close()
      }
    }
    ret
  }

  override def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): BrokenColumnarAdd =
    new BrokenColumnarAdd(
      left = newLeft.asInstanceOf[ColumnarExpression],
      right = newRight.asInstanceOf[ColumnarExpression], failOnError)
}

class CannotReplaceException(str: String) extends RuntimeException(str) {

}

case class PreRuleReplaceAddWithBrokenVersion() extends Rule[SparkPlan] {
  def replaceWithColumnarExpression(exp: Expression): ColumnarExpression = exp match {
    case a: Alias =>
      new ColumnarAlias(replaceWithColumnarExpression(a.child),
        a.name)(a.exprId, a.qualifier, a.explicitMetadata, a.nonInheritableMetadataKeys)
    case att: AttributeReference =>
      new ColumnarAttributeReference(att.name, att.dataType, att.nullable,
        att.metadata)(att.exprId, att.qualifier)
    case lit: Literal =>
      new ColumnarLiteral(lit.value, lit.dataType)
    case add: Add if (add.dataType == LongType) &&
      (add.left.dataType == LongType) &&
      (add.right.dataType == LongType) =>
      // Add only supports Longs for now.
      new BrokenColumnarAdd(replaceWithColumnarExpression(add.left),
        replaceWithColumnarExpression(add.right))
    case exp =>
      throw new CannotReplaceException(s"expression " +
        s"${exp.getClass} ${exp} is not currently supported.")
  }

  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan =
    try {
      plan match {
        case e: ShuffleExchangeExec =>
          // note that this is not actually columnar but demonstrates that exchanges can
          // be replaced.
          val replaced = e.withNewChildren(e.children.map(replaceWithColumnarPlan))
          MyShuffleExchangeExec(replaced.asInstanceOf[ShuffleExchangeExec])
        case e: BroadcastExchangeExec =>
          // note that this is not actually columnar but demonstrates that exchanges can
          // be replaced.
          val replaced = e.withNewChildren(e.children.map(replaceWithColumnarPlan))
          MyBroadcastExchangeExec(replaced.asInstanceOf[BroadcastExchangeExec])
        case plan: ProjectExec =>
          new ColumnarProjectExec(plan.projectList.map((exp) =>
            replaceWithColumnarExpression(exp).asInstanceOf[NamedExpression]),
            replaceWithColumnarPlan(plan.child))
        case write: WriteFilesExec =>
          ColumnarWriteExec(
            replaceWithColumnarPlan(write.child),
            write.fileFormat,
            write.partitionColumns,
            write.bucketSpec,
            write.options,
            write.staticPartitions)
        case p =>
          logWarning(s"Columnar processing for ${p.getClass} is not currently supported.")
          p.withNewChildren(p.children.map(replaceWithColumnarPlan))
      }
    } catch {
      case exp: CannotReplaceException =>
        logWarning(s"Columnar processing for ${plan.getClass} is not currently supported" +
          s"because ${exp.getMessage}")
        plan
    }

  override def apply(plan: SparkPlan): SparkPlan = replaceWithColumnarPlan(plan)
}

/**
 * Custom Exchange used in tests to demonstrate that shuffles can be replaced regardless of
 * whether AQE is enabled.
 */
case class MyShuffleExchangeExec(delegate: ShuffleExchangeExec) extends ShuffleExchangeLike {
  override def shuffleDependency: ShuffleDependency[_, _, _] = delegate.shuffleDependency
  override def numMappers: Int = delegate.numMappers
  override def numPartitions: Int = delegate.numPartitions
  override def advisoryPartitionSize: Option[Long] = delegate.advisoryPartitionSize
  override def shuffleOrigin: ShuffleOrigin = {
    delegate.shuffleOrigin
  }
  override def mapOutputStatisticsFuture: Future[MapOutputStatistics] =
    delegate.submitShuffleJob()
  override def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[_] =
    delegate.getShuffleRDD(partitionSpecs)
  override def runtimeStatistics: Statistics = {
    val stats = delegate.runtimeStatistics
    // add some mock column stats so we can test that AQE retains them in SPARK-39991
    val columnStats = ColumnStat(distinctCount = Some(BigInt(123)))
    val attributeStats = AttributeMap(Seq((child.output.head, columnStats)))
    Statistics(stats.sizeInBytes, stats.rowCount, attributeStats)
  }
  override def shuffleId: Int = delegate.shuffleId
  override def child: SparkPlan = delegate.child
  override protected def doExecute(): RDD[InternalRow] = delegate.execute()
  override def outputPartitioning: Partitioning = delegate.outputPartitioning
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    super.legacyWithNewChildren(Seq(newChild))
}

object RecoveryAnchorDataSource {
  val appliedAnchor = new AtomicReference[String]()
}

class RecoveryAnchorDataSource extends TestingV2Source {
  override def getTable(options: org.apache.spark.sql.util.CaseInsensitiveStringMap): Table = {
    new RecoveryAnchorTable("current-version")
  }
}

class RecoveryAnchorTable(anchor: String)
  extends SimpleBatchTable with SupportsRecoveryAnchor {

  override def recoverySourceId(): String = "recovery-anchor-source"

  override def currentRecoveryAnchor(): String = anchor

  override def withRecoveryAnchor(newAnchor: String): Table = {
    RecoveryAnchorDataSource.appliedAnchor.set(newAnchor)
    new RecoveryAnchorTable(newAnchor)
  }

  override def newScanBuilder(
      options: org.apache.spark.sql.util.CaseInsensitiveStringMap): ScanBuilder = {
    new SimpleScanBuilder {
      override def planInputPartitions(): Array[InputPartition] = {
        Array(RangeInputPartition(0, 1))
      }
    }
  }
}

object RecoveryWriteDataSource {
  val resolvedWriteId = new AtomicReference[String]()
  val writerCommits = new AtomicInteger()
  val globalCommits = new AtomicInteger()
  val factoryCreations = new AtomicInteger()
  val writerCreations = new AtomicInteger()
  val recoveryAborts = new AtomicInteger()
  val normalAborts = new AtomicInteger()
  val discardedOutputs = new AtomicInteger()
  val committedPartitionIds = new AtomicReference[Array[Int]](Array.emptyIntArray)
  val failGlobalCommit = new AtomicBoolean()
  val forceCanonicalLoser = new AtomicBoolean()
  val publishBeforeExecutorPreflight = new AtomicBoolean()
  @volatile var recoveredPartitions: Set[Int] = Set.empty
  @volatile var committed: Boolean = false
  @volatile var malformedState: String = ""

  def reset(
      recoveredPartitions: Set[Int] = Set.empty,
      committed: Boolean = false,
      failGlobalCommit: Boolean = false,
      forceCanonicalLoser: Boolean = false,
      publishBeforeExecutorPreflight: Boolean = false,
      malformedState: String = ""): Unit = {
    resolvedWriteId.set(null)
    writerCommits.set(0)
    globalCommits.set(0)
    factoryCreations.set(0)
    writerCreations.set(0)
    recoveryAborts.set(0)
    normalAborts.set(0)
    discardedOutputs.set(0)
    committedPartitionIds.set(Array.emptyIntArray)
    this.failGlobalCommit.set(failGlobalCommit)
    this.forceCanonicalLoser.set(forceCanonicalLoser)
    this.publishBeforeExecutorPreflight.set(publishBeforeExecutorPreflight)
    this.recoveredPartitions = recoveredPartitions
    this.committed = committed
    this.malformedState = malformedState
    RecoveryTestTaskCommitStore.clear()
  }
}

case class RecoveryWriterCommitMessage(partitionId: Int) extends WriterCommitMessage

object RecoveryTestCommitCodec extends RecoveryCommitMessageCodec {
  override def codecId(): String = "spark-test-recovery-commit"
  override def version(): Int = 1
  override def encode(message: WriterCommitMessage): Array[Byte] = {
    ByteBuffer.allocate(4).putInt(
      message.asInstanceOf[RecoveryWriterCommitMessage].partitionId).array()
  }
  override def decode(version: Int, payload: Array[Byte]): WriterCommitMessage = {
    require(version == 1)
    require(payload.length == 4)
    RecoveryWriterCommitMessage(ByteBuffer.wrap(payload).getInt)
  }
}

object RecoveryTestTaskCommitStore {
  private var manifest: Array[Byte] = _
  private val commits = scala.collection.mutable.Map.empty[Int, Array[Byte]]
  private var loadCalls = 0

  def clear(): Unit = synchronized {
    manifest = null
    commits.clear()
    loadCalls = 0
  }

  def resolve(proposed: Array[Byte]): Array[Byte] = synchronized {
    if (manifest == null) manifest = proposed.clone()
    manifest.clone()
  }

  def load(
      recoveryId: String,
      partitionIds: Array[Int]): JList[Optional[Array[Byte]]] = synchronized {
    loadCalls += 1
    partitionIds.map { partitionId =>
      if (!commits.contains(partitionId) && loadCalls > 1 &&
          RecoveryWriteDataSource.publishBeforeExecutorPreflight.get()) {
        val context = RecoveryTaskCommitContext(
          new RecoveryTestTaskCommitStore, recoveryId, RecoveryTestCommitCodec)
        commits(partitionId) = RecoveryTaskCommitEnvelope.encode(
          context, partitionId, RecoveryWriterCommitMessage(partitionId + 2000), 2L)
      }
      if (!commits.contains(partitionId) &&
          RecoveryWriteDataSource.recoveredPartitions.contains(partitionId)) {
        val context = RecoveryTaskCommitContext(
          new RecoveryTestTaskCommitStore, recoveryId, RecoveryTestCommitCodec)
        commits(partitionId) = RecoveryTaskCommitEnvelope.encode(
          context, partitionId, RecoveryWriterCommitMessage(partitionId), 1L)
      }
      Optional.ofNullable(commits.get(partitionId).map(_.clone()).orNull)
    }.toList.asJava
  }

  def publish(partitionId: Int, proposed: Array[Byte]): Array[Byte] = synchronized {
    if (!commits.contains(partitionId) && RecoveryWriteDataSource.forceCanonicalLoser.get()) {
      val context = RecoveryTaskCommitContext(
        new RecoveryTestTaskCommitStore, "durable-write-id", RecoveryTestCommitCodec)
      commits(partitionId) = RecoveryTaskCommitEnvelope.encode(
        context, partitionId, RecoveryWriterCommitMessage(partitionId + 1000), 2L)
    }
    commits.getOrElseUpdate(partitionId, proposed.clone()).clone()
  }
}

class RecoveryTestTaskCommitStore extends RecoveryTaskCommitStore {
  override def capabilities(): RecoveryTaskCommitStore.Capabilities =
    RecoveryTestStoreCapabilities
  override def resolveWriteManifest(recoveryId: String, proposedValue: Array[Byte]): Array[Byte] =
    RecoveryTestTaskCommitStore.resolve(proposedValue)
  override def load(
      recoveryId: String,
      partitionIds: Array[Int]): JList[Optional[Array[Byte]]] =
    RecoveryTestTaskCommitStore.load(recoveryId, partitionIds)
  override def publish(
      recoveryId: String,
      partitionId: Int,
      taskAttemptId: Long,
      attemptNumber: Int,
      value: Array[Byte]): Array[Byte] = RecoveryTestTaskCommitStore.publish(partitionId, value)
}

object RecoveryTestStoreCapabilities extends RecoveryTaskCommitStore.Capabilities {
  override def semanticsVersion(): Int = RecoveryTaskCommitStore.SEMANTICS_VERSION
  override def maxLoadBatchSize(): Int = 1024
  override def maxManifestBytes(): Int = 2 * 1024 * 1024
  override def maxTaskCommitBytes(): Int = 32 * 1024 * 1024
}

class RecoveryWriteDataSource extends TestingV2Source {
  override def getTable(options: org.apache.spark.sql.util.CaseInsensitiveStringMap): Table = {
    new RecoveryWriteTable
  }
}

class RecoveryWriteTable extends RecoveryAnchorTable("write-source-anchor")
  with SupportsWrite with SupportsRecoveryWrite {

  override def schema(): StructType = new StructType().add("id", LongType, nullable = false)

  override def recoverySinkId(): String = "recovery-write-sink"

  override def capabilities(): java.util.Set[TableCapability] = {
    java.util.EnumSet.of(BATCH_READ, BATCH_WRITE)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    RecoveryWriteDataSource.resolvedWriteId.set(info.queryId())
    new WriteBuilder {
      override def buildForBatch(): BatchWrite = new SupportsBatchWriteRecovery {
        override def commitMessageCodec(): RecoveryCommitMessageCodec = RecoveryTestCommitCodec
        override def recoveryCompatibilityMetadata(info: PhysicalWriteInfo): Array[Byte] =
          s"sink=recovery-write-sink;partitions=${info.numPartitions()}".getBytes("UTF-8")

        override def recover(info: PhysicalWriteInfo): BatchWriteRecoveryState = {
          if (RecoveryWriteDataSource.malformedState == "null-state") {
            return null
          }
          new BatchWriteRecoveryState {
            override def isCommitted(): Boolean = RecoveryWriteDataSource.committed
          }
        }

        override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
          RecoveryWriteDataSource.factoryCreations.incrementAndGet()
          new RecoveryDataWriterFactory {
            override def createWriter(
                partitionId: Int,
                taskId: Long): RecoveryDataWriter = {
              RecoveryWriteDataSource.writerCreations.incrementAndGet()
              new RecoveryDataWriter {
                override def write(record: InternalRow): Unit = {}
                override def commit(): WriterCommitMessage = {
                  RecoveryWriteDataSource.writerCommits.incrementAndGet()
                  RecoveryWriterCommitMessage(partitionId)
                }
                override def discardCommittedOutput(message: WriterCommitMessage): Unit = {
                  RecoveryWriteDataSource.discardedOutputs.incrementAndGet()
                }
                override def abort(): Unit = {}
                override def close(): Unit = {}
              }
            }
          }
        }

        override def commit(messages: Array[WriterCommitMessage]): Unit = {
          assert(messages.forall(_ != null))
          RecoveryWriteDataSource.committedPartitionIds.set(messages.map(
            _.asInstanceOf[RecoveryWriterCommitMessage].partitionId))
          RecoveryWriteDataSource.globalCommits.incrementAndGet()
          if (RecoveryWriteDataSource.failGlobalCommit.get()) {
            throw new RuntimeException("injected global commit failure")
          }
        }
        override def abortAfterRecovery(messages: Array[WriterCommitMessage]): Unit = {
          RecoveryWriteDataSource.recoveryAborts.incrementAndGet()
        }
        override def abort(messages: Array[WriterCommitMessage]): Unit = {
          RecoveryWriteDataSource.normalAborts.incrementAndGet()
        }
      }
    }
  }
}

object NonRecoverableWriteDataSource {
  val factoryCreations = new AtomicInteger()
}

class NonRecoverableWriteDataSource extends TestingV2Source {
  override def getTable(options: org.apache.spark.sql.util.CaseInsensitiveStringMap): Table = {
    new NonRecoverableWriteTable
  }
}

class NonRecoverableWriteTable extends RecoveryWriteTable {
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new WriteBuilder {
    override def buildForBatch(): BatchWrite = new BatchWrite {
      override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
        NonRecoverableWriteDataSource.factoryCreations.incrementAndGet()
        throw new IllegalStateException("a non-recoverable writer factory must not be created")
      }
      override def commit(messages: Array[WriterCommitMessage]): Unit = {}
      override def abort(messages: Array[WriterCommitMessage]): Unit = {}
    }
  }
}

/**
 * Custom Exchange used in tests to demonstrate that broadcasts can be replaced regardless of
 * whether AQE is enabled.
 */
case class MyBroadcastExchangeExec(delegate: BroadcastExchangeExec) extends BroadcastExchangeLike {
  override val runId: UUID = delegate.runId
  override def relationFuture: java.util.concurrent.Future[Broadcast[Any]] =
    delegate.relationFuture
  override def completionFuture: Future[Broadcast[Any]] = delegate.submitBroadcastJob()
  override def runtimeStatistics: Statistics = delegate.runtimeStatistics
  override def child: SparkPlan = delegate.child
  override protected def doPrepare(): Unit = delegate.prepare()
  override protected def doExecute(): RDD[InternalRow] = delegate.execute()
  override def doExecuteBroadcast[T](): Broadcast[T] = delegate.executeBroadcast()
  override def outputPartitioning: Partitioning = delegate.outputPartitioning

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    super.legacyWithNewChildren(Seq(newChild))
}

class ReplacedRowToColumnarExec(override val child: SparkPlan)
  extends RowToColumnarExec(child) {

  // We have to override equals because subclassing a case class like ProjectExec is not that clean
  // One of the issues is that the generated equals will see ColumnarProjectExec and ProjectExec
  // as being equal and this can result in the withNewChildren method not actually replacing
  // anything
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[ReplacedRowToColumnarExec]
  }

  override def hashCode(): Int = super.hashCode()

  override def withNewChildInternal(newChild: SparkPlan): ReplacedRowToColumnarExec =
    new ReplacedRowToColumnarExec(newChild)
}

case class MyPostRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan match {
    case rc: RowToColumnarExec => new ReplacedRowToColumnarExec(rc.child)
    case plan => plan.withNewChildren(plan.children.map(apply))
  }
}

case class MyColumnarRule(pre: Rule[SparkPlan], post: Rule[SparkPlan]) extends ColumnarRule {
  override def preColumnarTransitions: Rule[SparkPlan] = pre
  override def postColumnarTransitions: Rule[SparkPlan] = post
}

class MyExtensions extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectPlannerStrategy(MySparkStrategy)
    e.injectResolutionRule(MyRule)
    e.injectPostHocResolutionRule(MyRule)
    e.injectCheckRule(MyCheckRule)
    e.injectOptimizerRule(MyRule)
    e.injectParser(MyParser)
    e.injectFunction(MyExtensions.myFunction)
    e.injectColumnar(session => MyColumnarRule(PreRuleReplaceAddWithBrokenVersion(), MyPostRule()))
  }
}

object QueryPrepRuleHelper {
  val myPrepTag: TreeNodeTag[String] = TreeNodeTag[String]("myPrepTag")
  val myPrepTagValue: String = "myPrepTagValue"
}

// this rule will run during AQE query preparation and will write custom tags to each node
case class MyQueryStagePrepRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformDown {
    case plan =>
      plan.setTagValue(QueryPrepRuleHelper.myPrepTag, QueryPrepRuleHelper.myPrepTagValue)
      plan
  }
}

// this rule will run during AQE query stage optimization and will verify custom tags were
// already written during query preparation phase
case class MyNewQueryStageRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformDown {
    case plan if !plan.isInstanceOf[AdaptiveSparkPlanExec] =>
      assert(plan.getTagValue(QueryPrepRuleHelper.myPrepTag).get ==
          QueryPrepRuleHelper.myPrepTagValue)
      plan
  }
}

case class MyRule2(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan
}

case class MyCheckRule2(spark: SparkSession) extends (LogicalPlan => Unit) {
  override def apply(plan: LogicalPlan): Unit = { }
}

case class MySparkStrategy2(spark: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = Seq.empty
}

object MyExtensions2 {

  val myFunction = (FunctionIdentifier("myFunction2"),
    new ExpressionInfo(
      "noClass",
      "myDb",
      "myFunction2",
      "usage",
      "extended usage",
      "    Examples:",
      """
       note
      """,
      "",
      "3.0.0",
      """
       deprecated
      """,
      ""),
    (_: Seq[Expression]) => Literal(5, IntegerType))
}

class MyExtensions2 extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectPlannerStrategy(MySparkStrategy2)
    e.injectResolutionRule(MyRule2)
    e.injectPostHocResolutionRule(MyRule2)
    e.injectCheckRule(MyCheckRule2)
    e.injectOptimizerRule(MyRule2)
    e.injectParser((_: SparkSession, _: ParserInterface) => CatalystSqlParser)
    e.injectFunction(MyExtensions2.myFunction)
  }
}

object MyExtensions2Duplicate {

  val myFunction = (FunctionIdentifier("myFunction2"),
    new ExpressionInfo(
      "noClass",
      "myDb",
      "myFunction2",
      "usage",
      "extended usage",
      "    Examples:",
      """
       note
      """,
      "",
      "3.0.0",
      """
       deprecated
      """,
      ""),
    (_: Seq[Expression]) => Literal(5, IntegerType))
}

class MyExtensions2Duplicate extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectFunction(MyExtensions2Duplicate.myFunction)
  }
}

class YourExtensions extends SparkSessionExtensionsProvider {
  val getAppName = (FunctionIdentifier("get_fake_app_name"),
    new ExpressionInfo(
      "zzz.zzz.zzz",
      "",
      "get_fake_app_name"),
    (_: Seq[Expression]) => Literal("Fake App Name"))

  override def apply(v1: SparkSessionExtensions): Unit = {
    v1.injectFunction(getAppName)
  }
}

object AddLimit extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case Limit(_, _) => plan
    case _ => Limit(Literal(1), plan)
  }
}

object RequireAtLeaseTwoPartitions extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    val readOpt = plan.find(_.isInstanceOf[AQEShuffleReadExec])
    if (readOpt.exists(_.outputPartitioning.numPartitions == 1)) {
      plan.transform {
        case read: AQEShuffleReadExec => read.child
      }
    } else {
      plan
    }
  }
}

object MyQueryPostPlannerStrategyRule extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case h: HashAggregateExec if h.aggregateExpressions.map(_.mode).contains(Partial) =>
        ShuffleExchangeExec(SinglePartition, h)
      case h: HashAggregateExec if h.aggregateExpressions.map(_.mode).contains(Final) =>
        SortExec(h.groupingExpressions.map(k => SortOrder.apply(k, Ascending)), false, h)
    }
  }
}


// Example of an Aggregate hint that tells that 'attribute' values are no larger than 'max'.
// We will use them to rewrite MAX(attribute) with 'max' constant.
case class CustomAggHint(attribute: AttributeReference, max: Int) extends AggregateHint

// Attaches the CustomAggHint to the aggregate node without grouping keys if the aggregate
// function is MAX over the specified column.
case class CustomAggregateHintResolutionRule(spark: SparkSession) extends Rule[LogicalPlan] {
  val MY_HINT_NAME = Set("MAX_VALUE")

  def isMax(expr: NamedExpression, attribute: String): Option[AttributeReference] = {
    expr match {
      case Alias(AggregateExpression(Max(a @ AttributeReference(name, _, _, _)), _, _, _, _), _)
        if name.equalsIgnoreCase(attribute) =>
        Some(a)
      case _ => None
    }
  }

  private def applyMaxValueHint(
      plan: LogicalPlan,
      attribute: String,
      max: Int): LogicalPlan = {
    val newPlan = plan match {
      case a @ Aggregate(keys, aggs, _, None) if keys.isEmpty && aggs.size == 1 =>
        isMax(aggs.head, attribute) match {
          case Some(attr) => a.copy(hint = Some(CustomAggHint(attr, max)))
          case None => a
        }
      case _ => plan
    }
    newPlan.mapChildren { child =>
      applyMaxValueHint(child, attribute, max)
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case h: UnresolvedHint if MY_HINT_NAME.contains(h.name.toUpperCase(Locale.ROOT)) =>
      applyMaxValueHint(h.child, "id", 10)
  }
}

// Logical rule that replaces the MAX aggregation function (in Aggregates with CustomAggHint)
// with just the constant from the hint.
case class CustomAggregateRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformDown {
      case a @ Aggregate(groupingKeys, aggregates, _, Some(CustomAggHint(_, max)))
        if groupingKeys.isEmpty && aggregates.size == 1 =>
        a.copy(aggregateExpressions = Seq(Alias(Cast(Literal(max), aggregates.head.dataType),
          aggregates.head.name)()), hint = None)
    }
  }
}

// Example of a Sort hint that tells that the input is already sorted,
// and the rule that removes all Sort nodes based on such hint.
case class CustomSortHint(inputSorted: Boolean) extends SortHint

// Attaches the CustomSortHint to the sort node.
case class CustomSortHintResolutionRule(spark: SparkSession) extends Rule[LogicalPlan] {
  val MY_HINT_NAME = Set("INPUT_SORTED")

  private def applySortHint(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case s @ Sort(_, _, _, None) => s.copy(hint = Some(CustomSortHint(true)))
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case h: UnresolvedHint if MY_HINT_NAME.contains(h.name.toUpperCase(Locale.ROOT)) =>
      applySortHint(h.child)
  }
}

case class CustomSortRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case s @ Sort(_, _, _, Some(CustomSortHint(true))) => s.child
  }
}
