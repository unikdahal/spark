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

package org.apache.spark.sql.execution.datasources.v2

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, ProjectingInternalRow}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression, TableSpec, UnaryNode}
import org.apache.spark.sql.catalyst.transactions.TransactionUtils
import org.apache.spark.sql.catalyst.util.{removeInternalMetadata, CharVarcharUtils, ReplaceDataProjections, WriteDeltaProjections}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.{COPY_OPERATION, DELETE_OPERATION, INSERT_OPERATION, REINSERT_OPERATION, UPDATE_OPERATION}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column, Identifier, StagedTable, StagingTableCatalog, Table, TableCatalog, TableInfo, TableWritePrivilege}
import org.apache.spark.sql.connector.catalog.transactions.Transaction
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, DeleteSummaryImpl, DeltaWrite, DeltaWriter, InsertSummaryImpl, MergeSummaryImpl, PhysicalWriteInfoImpl, RecoveryDataWriterFactory, RowLevelOperation, RowLevelOperationTable, RowLevelTaskSummary, SupportsBatchWriteRecovery, SupportsRecoveryCommitDiscard, SupportsRecoveryTaskMetrics, UpdateSummaryImpl, Write, WriterCommitMessage, WriteSummary}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command._
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.{EmptyRDDWithPartitions, QueryExecution, SparkPlan, SQLExecution, UnaryExecNode}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.metric.{CustomMetrics, SQLLastAttemptMetric, SQLLastAttemptMetrics, SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.util.SchemaValidationMode.PROHIBIT_CHANGES
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * Deprecated logical plan for writing data into data source v2. This is being replaced by more
 * specific logical plans, like [[org.apache.spark.sql.catalyst.plans.logical.AppendData]].
 */
@deprecated("Use specific logical plans like AppendData instead", "2.4.0")
case class WriteToDataSourceV2(
    relation: Option[DataSourceV2Relation],
    batchWrite: BatchWrite,
    query: LogicalPlan,
    customMetrics: Seq[CustomMetric]) extends UnaryNode {
  override def child: LogicalPlan = query
  override def output: Seq[Attribute] = Nil
  override protected def withNewChildInternal(newChild: LogicalPlan): WriteToDataSourceV2 =
    copy(query = newChild)
}

/**
 * Physical plan node for v2 create table as select when the catalog does not support staging
 * the table creation.
 *
 * A new table will be created using the schema of the query, and rows from the query are appended.
 * If either table creation or the append fails, the table will be deleted. This implementation is
 * not atomic; for an atomic variant for catalogs that support the appropriate features, see
 * CreateTableAsSelectStagingExec.
 */
case class CreateTableAsSelectExec(
    catalog: TableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    query: LogicalPlan,
    tableSpec: TableSpec,
    writeOptions: Map[String, String],
    ifNotExists: Boolean,
    transaction: Option[Transaction] = None)
  extends V2CreateTableAsSelectBaseExec with TransactionalExec {

  override def withTransaction(txn: Option[Transaction]): CreateTableAsSelectExec =
    copy(transaction = txn)

  val properties = CatalogV2Util.convertTableProperties(tableSpec)

  override protected def run(): Seq[InternalRow] = {
    if (catalog.tableExists(ident)) {
      if (ifNotExists) {
        return Nil
      }
      throw QueryCompilationErrors.tableAlreadyExistsError(ident)
    }
    val tableInfo = new TableInfo.Builder()
      .withColumns(getV2Columns(query.schema, catalog.useNullableQuerySchema))
      .withPartitions(partitioning.toArray)
      .withProperties(properties.asJava)
      .build()
    val table = Option(catalog.createTable(ident, tableInfo))
      .getOrElse(loadTableForInsert(catalog, ident, writeOptions))
    val result = writeToTable(catalog, table, writeOptions, ident, query, overwrite = false)
    transaction.foreach(TransactionUtils.commit)
    result
  }
}

/**
 * Physical plan node for v2 create table as select, when the catalog is determined to support
 * staging table creation.
 *
 * A new table will be created using the schema of the query, and rows from the query are appended.
 * The CTAS operation is atomic. The creation of the table is staged and the commit of the write
 * should bundle the commitment of the metadata and the table contents in a single unit. If the
 * write fails, the table is instructed to roll back all staged changes.
 */
case class AtomicCreateTableAsSelectExec(
    catalog: StagingTableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    query: LogicalPlan,
    tableSpec: TableSpec,
    writeOptions: Map[String, String],
    ifNotExists: Boolean)
  extends V2CreateTableAsSelectBaseExec {

  val properties = CatalogV2Util.convertTableProperties(tableSpec)

  override val metrics: Map[String, SQLMetric] =
    DataSourceV2Utils.commitMetrics(sparkContext, catalog)

  override protected def run(): Seq[InternalRow] = {
    if (catalog.tableExists(ident)) {
      if (ifNotExists) {
        return Nil
      }
      throw QueryCompilationErrors.tableAlreadyExistsError(ident)
    }
    val tableInfo = new TableInfo.Builder()
      .withColumns(getV2Columns(query.schema, catalog.useNullableQuerySchema))
      .withPartitions(partitioning.toArray)
      .withProperties(properties.asJava)
      .build()
    val stagedTable = Option(catalog.stageCreate(ident, tableInfo)
    ).getOrElse(loadTableForInsert(catalog, ident, writeOptions))
    writeToTable(catalog, stagedTable, writeOptions, ident, query, overwrite = false)
  }
}

/**
 * Physical plan node for v2 replace table as select when the catalog does not support staging
 * table replacement.
 *
 * A new table will be created using the schema of the query, and rows from the query are appended.
 * If the table exists, its contents and schema should be replaced with the schema and the contents
 * of the query. This is a non-atomic implementation that drops the table and then runs non-atomic
 * CTAS. For an atomic implementation for catalogs with the appropriate support, see
 * ReplaceTableAsSelectStagingExec.
 */
case class ReplaceTableAsSelectExec(
    catalog: TableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    query: LogicalPlan,
    tableSpec: TableSpec,
    writeOptions: Map[String, String],
    orCreate: Boolean,
    invalidateCache: (TableCatalog, Identifier) => Unit,
    transaction: Option[Transaction] = None)
  extends V2CreateTableAsSelectBaseExec with TransactionalExec {

  override def withTransaction(txn: Option[Transaction]): ReplaceTableAsSelectExec =
    copy(transaction = txn)

  val properties = CatalogV2Util.convertTableProperties(tableSpec)

  override protected def run(): Seq[InternalRow] = {
    // Note that this operation is potentially unsafe, but these are the strict semantics of
    // RTAS if the catalog does not support atomic operations.
    //
    // There are numerous cases we concede to where the table will be dropped and irrecoverable:
    //
    // 1. Creating the new table fails,
    // 2. Writing to the new table fails,
    // 3. The table returned by catalog.createTable doesn't support writing.
    //
    // RTAS must refresh and pin versions in query to read from original table versions instead of
    // newly created empty table that is meant to serve as target for append/overwrite
    val refreshedQuery = V2TableRefreshUtil.refresh(
      session,
      query,
      versionedOnly = true,
      schemaValidationMode = PROHIBIT_CHANGES)
    if (catalog.tableExists(ident)) {
      invalidateCache(catalog, ident)
      catalog.dropTable(ident)
    } else if (!orCreate) {
      throw QueryCompilationErrors.cannotReplaceMissingTableError(
        ident,
        CatalogV2Util.searchPathForTableIdentifier(catalog, ident))
    }
    val tableInfo = new TableInfo.Builder()
      .withColumns(getV2Columns(refreshedQuery.schema, catalog.useNullableQuerySchema))
      .withPartitions(partitioning.toArray)
      .withProperties(properties.asJava)
      .build()
    val table = Option(catalog.createTable(ident, tableInfo))
      .getOrElse(loadTableForInsert(catalog, ident, writeOptions))
    val result = writeToTable(
      catalog, table, writeOptions, ident, refreshedQuery,
      overwrite = true, refreshPhaseEnabled = false)
    transaction.foreach(TransactionUtils.commit)
    result
  }
}

/**
 *
 * Physical plan node for v2 replace table as select when the catalog supports staging
 * table replacement.
 *
 * A new table will be created using the schema of the query, and rows from the query are appended.
 * If the table exists, its contents and schema should be replaced with the schema and the contents
 * of the query. This implementation is atomic. The table replacement is staged, and the commit
 * operation at the end should perform the replacement of the table's metadata and contents. If the
 * write fails, the table is instructed to roll back staged changes and any previously written table
 * is left untouched.
 */
case class AtomicReplaceTableAsSelectExec(
    catalog: StagingTableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    query: LogicalPlan,
    tableSpec: TableSpec,
    writeOptions: Map[String, String],
    orCreate: Boolean,
    invalidateCache: (TableCatalog, Identifier) => Unit)
  extends V2CreateTableAsSelectBaseExec {

  val properties = CatalogV2Util.convertTableProperties(tableSpec)

  override val metrics: Map[String, SQLMetric] =
    DataSourceV2Utils.commitMetrics(sparkContext, catalog)

  override protected def run(): Seq[InternalRow] = {
    val columns = getV2Columns(query.schema, catalog.useNullableQuerySchema)
    if (catalog.tableExists(ident)) {
      invalidateCache(catalog, ident)
    }
    val staged = if (orCreate) {
      val tableInfo = new TableInfo.Builder()
        .withColumns(columns)
        .withPartitions(partitioning.toArray)
        .withProperties(properties.asJava)
        .build()
      catalog.stageCreateOrReplace(ident, tableInfo)
    } else if (catalog.tableExists(ident)) {
      try {
        val tableInfo = new TableInfo.Builder()
          .withColumns(columns)
          .withPartitions(partitioning.toArray)
          .withProperties(properties.asJava)
          .build()
        catalog.stageReplace(ident, tableInfo)
      } catch {
        case e: NoSuchTableException =>
          throw QueryCompilationErrors.cannotReplaceMissingTableError(
            ident,
            CatalogV2Util.searchPathForTableIdentifier(catalog, ident),
            Some(e))
      }
    } else {
      throw QueryCompilationErrors.cannotReplaceMissingTableError(
        ident,
        CatalogV2Util.searchPathForTableIdentifier(catalog, ident))
    }
    val table = Option(staged).getOrElse(loadTableForInsert(catalog, ident, writeOptions))
    writeToTable(catalog, table, writeOptions, ident, query, overwrite = true)
  }
}

/**
 * Physical plan node for append into a v2 table.
 *
 * Rows in the output data set are appended.
 */
case class AppendDataExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write,
    tableName: String,
    transaction: Option[Transaction] = None) extends V2ExistingTableWriteExec {
  override def withTransaction(txn: Option[Transaction]): AppendDataExec = copy(transaction = txn)
  override protected def withNewChildInternal(newChild: SparkPlan): AppendDataExec =
    copy(query = newChild)

  override protected def getWriteSummary(): Option[WriteSummary] =
    Some(InsertSummaryImpl(numInsertedRows = numOutputRowsMetric.value))
}

/**
 * Physical plan for an insert-only MERGE rewrite. Behaves like [[AppendDataExec]] but emits a
 * [[org.apache.spark.sql.connector.write.MergeSummary]] so commit metadata reports the operation
 * as a MERGE, with all output rows accounted for as inserts.
 */
case class InsertOnlyMergeExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write,
    tableName: String,
    transaction: Option[Transaction] = None) extends V2ExistingTableWriteExec {
  override def withTransaction(txn: Option[Transaction]): InsertOnlyMergeExec =
    copy(transaction = txn)
  override protected def withNewChildInternal(newChild: SparkPlan): InsertOnlyMergeExec =
    copy(query = newChild)

  override protected def getWriteSummary(): Option[WriteSummary] =
    Some(MergeSummaryImpl(
      numTargetRowsCopied = 0L,
      numTargetRowsDeleted = 0L,
      numTargetRowsUpdated = 0L,
      numTargetRowsInserted = numOutputRowsMetric.value,
      numTargetRowsMatchedUpdated = 0L,
      numTargetRowsMatchedDeleted = 0L,
      numTargetRowsNotMatchedBySourceUpdated = 0L,
      numTargetRowsNotMatchedBySourceDeleted = 0L))
}

/**
 * Physical plan node for overwrite into a v2 table.
 *
 * Overwrites data in a table matched by a set of filters. Rows matching all of the filters will be
 * deleted and rows in the output data set are appended.
 *
 * This plan is used to implement SaveMode.Overwrite. The behavior of SaveMode.Overwrite is to
 * truncate the table -- delete all rows -- and append the output data set. This uses the filter
 * AlwaysTrue to delete all rows.
 */
case class OverwriteByExpressionExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write,
    tableName: String,
    transaction: Option[Transaction] = None) extends V2ExistingTableWriteExec {
  override def withTransaction(txn: Option[Transaction]): OverwriteByExpressionExec =
    copy(transaction = txn)
  override protected def withNewChildInternal(newChild: SparkPlan): OverwriteByExpressionExec =
    copy(query = newChild)
}

/**
 * Physical plan node for dynamic partition overwrite into a v2 table.
 *
 * Dynamic partition overwrite is the behavior of Hive INSERT OVERWRITE ... PARTITION queries, and
 * Spark INSERT OVERWRITE queries when spark.sql.sources.partitionOverwriteMode=dynamic. Each
 * partition in the output data set replaces the corresponding existing partition in the table or
 * creates a new partition. Existing partitions for which there is no data in the output data set
 * are not modified.
 */
case class OverwritePartitionsDynamicExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write,
    tableName: String,
    transaction: Option[Transaction] = None) extends V2ExistingTableWriteExec {
  override def withTransaction(txn: Option[Transaction]): OverwritePartitionsDynamicExec =
    copy(transaction = txn)
  override protected def withNewChildInternal(newChild: SparkPlan): OverwritePartitionsDynamicExec =
    copy(query = newChild)
}

/**
 * Physical plan node to replace data in existing tables.
 */
case class ReplaceDataExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    projections: ReplaceDataProjections,
    write: Write,
    rowLevelCommand: RowLevelOperation.Command,
    tableName: String,
    transaction: Option[Transaction] = None) extends RowLevelWriteExec {

  override private[v2] val rowLevelPhysicalMode: String = "REPLACE_DATA"

  override def writingTask: WritingSparkTask[_] = {
    projections.metadataProjection match {
      case Some(metadataProj) =>
        DataAndMetadataWritingSparkTask(projections.rowProjection, metadataProj, sparkMetrics)
      case None =>
        DataWithProjectionWritingSparkTask(projections.rowProjection, sparkMetrics)
    }
  }

  override def withTransaction(txn: Option[Transaction]): ReplaceDataExec = copy(transaction = txn)
  override protected def withNewChildInternal(newChild: SparkPlan): ReplaceDataExec = {
    copy(query = newChild)
  }

  override protected def getDeleteSummary(): Option[DeleteSummaryImpl] = {
    // DELETE ReplaceData plans filter out the deleted rows early in the plan, and they don't
    // reach this node. We need to calculate this value as numScannedRows - numCopiedRows.
    val numScannedRows = collectFirst(query) {
      case b: BatchScanExec if b.table.isInstanceOf[RowLevelOperationTable] =>
        getMetricValue(b.metrics, "numOutputRows")
    }
    val numCopiedRows = getMetricValue(sparkMetrics, "numCopiedRows")
    val numDeletedRows = if (numScannedRows.exists(_ >= 0) && numCopiedRows >= 0) {
      numScannedRows.get - numCopiedRows
    } else {
      // One of the metrics couldn't be found, also mark numDeletedRows as not found.
      -1L
    }

    // SQLMetric.set is a no-op if value is -1, leaving the metric in its invalid state.
    sparkMetrics("numDeletedRows").set(numDeletedRows)
    super.getDeleteSummary().map(_.copy(numDeletedRows = numDeletedRows))
  }
}

/**
 * Physical plan node to write a delta of rows to an existing table.
 */
case class WriteDeltaExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    projections: WriteDeltaProjections,
    write: DeltaWrite,
    rowLevelCommand: RowLevelOperation.Command,
    tableName: String,
    transaction: Option[Transaction] = None) extends RowLevelWriteExec {

  override private[v2] val rowLevelPhysicalMode: String = "WRITE_DELTA"

  override lazy val writingTask: WritingSparkTask[_] = {
    if (projections.metadataProjection.isDefined) {
      DeltaWithMetadataWritingSparkTask(projections, sparkMetrics)
    } else {
      DeltaWritingSparkTask(projections, sparkMetrics)
    }
  }

  override def withTransaction(txn: Option[Transaction]): WriteDeltaExec = copy(transaction = txn)
  override protected def withNewChildInternal(newChild: SparkPlan): WriteDeltaExec = {
    copy(query = newChild)
  }
}

case class WriteToDataSourceV2Exec(
    batchWrite: BatchWrite,
    refreshCache: () => Unit,
    query: SparkPlan,
    writeMetrics: Seq[CustomMetric],
    transaction: Option[Transaction] = None) extends V2TableWriteExec with TransactionalExec {

  override def withTransaction(txn: Option[Transaction]): WriteToDataSourceV2Exec =
    copy(transaction = txn)

  override protected def recoveryTransaction: Option[Transaction] = transaction

  override def stringArgs: Iterator[Any] = Iterator(batchWrite, query)

  override lazy val customMetrics: Map[String, SQLMetric] =
    createCustomMetrics(writeMetrics.toArray)

  override protected def run(): Seq[InternalRow] = {
    val writtenRows = writeWithV2(batchWrite)
    transaction.foreach(TransactionUtils.commit)
    refreshCache()
    writtenRows
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WriteToDataSourceV2Exec =
    copy(query = newChild)
}

/**
 * Trait for physical plan nodes that write to an existing table as part of a transaction.
 * The [[transaction]] is injected post-planning by [[QueryExecution]].
 */
trait TransactionalExec extends SparkPlan {
  def transaction: Option[Transaction]
  def withTransaction(txn: Option[Transaction]): SparkPlan
}

trait V2ExistingTableWriteExec extends V2TableWriteExec with TransactionalExec {
  def refreshCache: () => Unit
  def write: Write
  def tableName: String

  override protected def recoveryTransaction: Option[Transaction] = transaction

  override def stringArgs: Iterator[Any] = Iterator(query, write)

  override def nodeName: String = s"${super.nodeName} $tableName"

  override lazy val customMetrics: Map[String, SQLMetric] =
    createCustomMetrics(write.supportedCustomMetrics())

  override protected def run(): Seq[InternalRow] = {
    val writtenRows = try {
      writeWithV2(write.toBatch)
    } finally {
      postDriverMetrics(write.reportDriverMetrics())
    }
    transaction.foreach(TransactionUtils.commit)
    refreshCache()
    writtenRows
  }
}

/**
 * A trait for row-level write operations (UPDATE, DELETE, MERGE).
 */
trait RowLevelWriteExec extends V2ExistingTableWriteExec {
  def rowLevelCommand: RowLevelOperation.Command
  private[v2] def rowLevelPhysicalMode: String

  override protected def requiresRowLevelTaskSummary: Boolean = true

  override protected lazy val sparkMetrics: Map[String, SQLMetric] = super.sparkMetrics ++ (
    rowLevelCommand match {
      case UPDATE =>
        Map(
          "numUpdatedRows" ->
            SQLLastAttemptMetrics.createMetric(sparkContext, "number of updated rows"),
          "numCopiedRows" ->
            SQLLastAttemptMetrics.createMetric(sparkContext, "number of copied rows"))
      case DELETE =>
        Map(
          "numDeletedRows" ->
            SQLLastAttemptMetrics.createMetric(sparkContext, "number of deleted rows"),
          "numCopiedRows" ->
            SQLLastAttemptMetrics.createMetric(sparkContext, "number of copied rows"))
      case _ => Map.empty
    })

  /**
   * Returns the value of the named metric, or -1 if the metric is not found. For
   * [[SQLLastAttemptMetric]] values, prefers the last-attempt value so the result is stable across
   * stage retries; falls back to the regular accumulator value if the last-attempt value is
   * unavailable (e.g. the accumulator bailed out).
   */
  protected def getMetricValue(metrics: Map[String, SQLMetric], name: String): Long = {
    metrics.get(name).map {
      case slam: SQLLastAttemptMetric =>
        slam.lastAttemptValueForHighestRDDId().getOrElse(slam.value)
      case m => m.value
    }.getOrElse(-1L)
  }

  override protected def getWriteSummary(): Option[WriteSummary] = {
    rowLevelCommand match {
      case MERGE => getMergeSummary()
      case UPDATE => getUpdateSummary()
      case DELETE => getDeleteSummary()
    }
  }

  protected def getMergeSummary(): Option[MergeSummaryImpl] = {
    collectFirst(query) { case m: MergeRowsExec => m }.map { n =>
      val metrics = n.metrics
      MergeSummaryImpl(
        getMetricValue(metrics, "numTargetRowsCopied"),
        getMetricValue(metrics, "numTargetRowsDeleted"),
        getMetricValue(metrics, "numTargetRowsUpdated"),
        getMetricValue(metrics, "numTargetRowsInserted"),
        getMetricValue(metrics, "numTargetRowsMatchedUpdated"),
        getMetricValue(metrics, "numTargetRowsMatchedDeleted"),
        getMetricValue(metrics, "numTargetRowsNotMatchedBySourceUpdated"),
        getMetricValue(metrics, "numTargetRowsNotMatchedBySourceDeleted"))
    }
  }

  protected def getUpdateSummary(): Option[UpdateSummaryImpl] = {
    Some(UpdateSummaryImpl(
      getMetricValue(sparkMetrics, "numUpdatedRows"),
      getMetricValue(sparkMetrics, "numCopiedRows")))
  }

  protected def getDeleteSummary(): Option[DeleteSummaryImpl] = {
    Some(DeleteSummaryImpl(
      getMetricValue(sparkMetrics, "numDeletedRows"),
      getMetricValue(sparkMetrics, "numCopiedRows")))
  }

  override protected def restoreRowLevelTaskSummary(summary: RowLevelTaskSummary): Unit = {
    require(summary != null, "Recovered row-level task summary must not be null")
    rowLevelCommand match {
      case MERGE =>
        val merge = collectFirst(query) { case node: MergeRowsExec => node }.getOrElse {
          throw new IllegalStateException(
            "A recoverable MERGE plan has no MergeRowsExec metrics owner")
        }
        val values = Map(
          "numTargetRowsCopied" -> summary.numTargetRowsCopied(),
          "numTargetRowsDeleted" -> summary.numTargetRowsDeleted(),
          "numTargetRowsUpdated" -> summary.numTargetRowsUpdated(),
          "numTargetRowsInserted" -> summary.numTargetRowsInserted(),
          "numTargetRowsMatchedUpdated" -> summary.numTargetRowsMatchedUpdated(),
          "numTargetRowsMatchedDeleted" -> summary.numTargetRowsMatchedDeleted(),
          "numTargetRowsNotMatchedBySourceUpdated" ->
            summary.numTargetRowsNotMatchedBySourceUpdated(),
          "numTargetRowsNotMatchedBySourceDeleted" ->
            summary.numTargetRowsNotMatchedBySourceDeleted())
        values.foreach { case (name, value) =>
          merge.metrics.getOrElse(name,
            throw new IllegalStateException(s"A recoverable MERGE plan has no $name metric"))
            .set(value)
        }

      case UPDATE =>
        sparkMetrics("numUpdatedRows").set(summary.numTargetRowsUpdated())
        sparkMetrics("numCopiedRows").set(summary.numTargetRowsCopied())

      case DELETE =>
        sparkMetrics("numDeletedRows").set(summary.numTargetRowsDeleted())
        sparkMetrics("numCopiedRows").set(summary.numTargetRowsCopied())
        this match {
          case _: ReplaceDataExec =>
            require(summary.numTargetRowsScanned() >= summary.numTargetRowsCopied(),
              "Recovered DELETE scanned-row count is smaller than its copied-row count")
            require(
              summary.numTargetRowsScanned() - summary.numTargetRowsCopied() ==
                summary.numTargetRowsDeleted(),
              "Recovered DELETE summary has inconsistent scanned, copied, and deleted counts")
            val scan = collectFirst(query) {
              case node: BatchScanExec if node.table.isInstanceOf[RowLevelOperationTable] => node
            }.getOrElse {
              throw new IllegalStateException(
                "A recoverable ReplaceData DELETE plan has no row-level BatchScanExec")
            }
            scan.metrics.getOrElse("numOutputRows",
              throw new IllegalStateException(
                "A recoverable ReplaceData DELETE scan has no output-row metric"))
              .set(summary.numTargetRowsScanned())
          case _ =>
        }
    }
  }
}

/**
 * The base physical plan for writing data into data source v2.
 */
trait V2TableWriteExec
  extends V2CommandExec
  with UnaryExecNode
  with AdaptiveSparkPlanHelper
  with SupportsCustomDriverMetrics {

  def query: SparkPlan
  def writingTask: WritingSparkTask[_] = DataWritingSparkTask

  var commitProgress: Option[StreamWriterCommitProgress] = None

  override def child: SparkPlan = query
  override def output: Seq[Attribute] = Nil

  override def customMetrics: Map[String, SQLMetric] = Map.empty

  protected def recoveryTransaction: Option[Transaction] = None
  protected def recoveryUnsupportedReason: Option[String] = None
  protected def requiresRowLevelTaskSummary: Boolean = false

  protected lazy val numOutputRowsMetric: SQLMetric =
    SQLMetrics.createMetric(sparkContext, "number of output rows")

  override protected def sparkMetrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> numOutputRowsMetric)

  protected def writeWithV2(batchWrite: BatchWrite): Seq[InternalRow] = {
    val rdd: RDD[InternalRow] = {
      val tempRdd = query.execute()
      // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
      // partition rdd to make sure we at least set up one write task to write the metadata.
      if (tempRdd.partitions.length == 0) {
        new EmptyRDDWithPartitions(sparkContext, 1)
      } else {
        tempRdd
      }
    }
    val physicalWriteInfo = PhysicalWriteInfoImpl(rdd.getNumPartitions)
    if (batchWrite.isInstanceOf[SupportsBatchWriteRecovery]) {
      require(recoveryTransaction.isEmpty,
        "Batch write recovery inside a catalog transaction is not yet supported")
      recoveryUnsupportedReason.foreach(reason => throw new UnsupportedOperationException(reason))
    }
    val taskCommitContext = batchWrite match {
      case recoverable: SupportsBatchWriteRecovery =>
        val store = batchWrite match {
          case withStore: HasRecoveryTaskCommitStore => withStore.taskCommitStore
          case _ => throw new IllegalStateException(
            "A recoverable batch write has no durable task commit store")
        }
        val metricSchema = batchWrite match {
          case withMetrics: SupportsRecoveryTaskMetrics =>
            Some(Option(withMetrics.recoveryTaskMetricSchema()).getOrElse {
              throw new IllegalStateException(
                "A recoverable batch write returned a null task metric schema")
            })
          case _ =>
            require(customMetrics.isEmpty,
              "A recoverable batch write with custom metrics must implement " +
                classOf[SupportsRecoveryTaskMetrics].getName)
            None
        }
        metricSchema.foreach { schema =>
          val descriptors = schema.descriptors()
          val descriptorNames = descriptors.iterator.map(_.name()).toSeq
          require(descriptorNames.distinct.size == descriptorNames.size,
            "Recovery task metric schema contains duplicate names")
          require(descriptorNames.toSet == customMetrics.keySet,
            s"Recovery task metric schema names ${descriptorNames.sorted.mkString(", ")} " +
              "do not match supported custom metrics " +
              customMetrics.keys.toSeq.sorted.mkString(", "))
        }
        val context = RecoveryTaskCommitContext(
          store, batchWrite.asInstanceOf[HasRecoveryTaskCommitStore].recoveryId,
          recoverable.commitMessageCodec(), metricSchema, requiresRowLevelTaskSummary)
        if (requiresRowLevelTaskSummary && this.isInstanceOf[RowLevelWriteExec]) {
          val rowLevelExec = this.asInstanceOf[RowLevelWriteExec]
          val command = rowLevelExec.rowLevelCommand.toString
          val manifestInput =
            batchWrite.asInstanceOf[HasRecoveryTaskCommitStore].rowLevelManifestInput
          require(manifestInput.isDefined,
            s"Recovery of row-level ${rowLevelExec.rowLevelCommand} " +
              "requires a Spark-owned generation manifest")
          require(manifestInput.get.command == command,
            s"Row-level recovery manifest command ${manifestInput.get.command} does not match " +
              s"physical command $command")
          require(manifestInput.get.physicalMode == rowLevelExec.rowLevelPhysicalMode,
            s"Row-level recovery manifest mode ${manifestInput.get.physicalMode} does not match " +
              s"physical mode ${rowLevelExec.rowLevelPhysicalMode}")
        }
        RecoveryTaskCommitEnvelope.validateContext(context)
        val compatibilityMetadata = Option(
          recoverable.recoveryCompatibilityMetadata(physicalWriteInfo)).getOrElse {
          throw new IllegalStateException(
            "A recoverable batch write returned null compatibility metadata")
        }
        val rowLevelManifest =
          batchWrite.asInstanceOf[HasRecoveryTaskCommitStore].rowLevelManifestInput.map { input =>
            require(input.recoveryId == context.recoveryId,
              "Row-level write manifest and task commit context have different recovery IDs")
            RowLevelWriteManifest.encode(input)
          }
        val proposedManifest = RecoveryTaskCommitEnvelope.writeManifest(
          context, rdd.getNumPartitions, compatibilityMetadata, rowLevelManifest)
        RecoveryTaskCommitEnvelope.validateManifestSize(context, proposedManifest)
        val resolvedManifest = Option(
          store.resolveWriteManifest(context.recoveryId, proposedManifest)).getOrElse {
          throw new IllegalStateException("Recovery task commit store returned a null manifest")
        }
        require(java.security.MessageDigest.isEqual(proposedManifest, resolvedManifest),
          "Recovery write manifest differs from the durable manifest")
        Some(context)
      case _ => None
    }
    val recoveryState = batchWrite match {
      case recoverable: SupportsBatchWriteRecovery =>
        Some(Option(recoverable.recover(physicalWriteInfo)).getOrElse {
          throw new IllegalStateException("A recoverable batch write returned null recovery state")
        })
      case _ => None
    }
    val messages = new Array[WriterCommitMessage](rdd.partitions.length)
    val canonicalRows = Array.fill(messages.length)(-1L)
    val canonicalMetrics = Array.fill(messages.length)(Map.empty[String, Long])
    val canonicalRowLevelSummaries =
      Array.fill[Option[RowLevelTaskSummary]](messages.length)(None)
    val alreadyCommitted = recoveryState.exists(_.isCommitted())
    taskCommitContext.filter(_ => !alreadyCommitted).foreach { context =>
      val MaxLoadBatch = math.min(
        1024, RecoveryTaskCommitEnvelope.validatedCapabilities(context).maxLoadBatchSize())
      messages.indices.grouped(MaxLoadBatch).foreach { partitionIds =>
        val ids = partitionIds.toArray
        val stored = Option(context.store.load(context.recoveryId, ids)).getOrElse {
          throw new IllegalStateException("Recovery task commit store returned a null result")
        }
        require(stored.size() == ids.length,
          s"Recovery task commit store returned ${stored.size()} values for ${ids.length} keys")
        ids.indices.foreach { resultIndex =>
          val partitionId = ids(resultIndex)
          val storedValue = Option(stored.get(resultIndex)).getOrElse {
            throw new IllegalStateException(
              s"Recovery task commit store returned null lookup entry $resultIndex")
          }
          if (storedValue.isPresent) {
            val envelope = storedValue.get()
            RecoveryTaskCommitEnvelope.validateTaskCommitSize(context, envelope)
            val decoded = RecoveryTaskCommitEnvelope.decode(context, partitionId, envelope)
            messages(partitionId) = decoded.message
            canonicalRows(partitionId) = decoded.numRows
            canonicalMetrics(partitionId) = decoded.metrics
            canonicalRowLevelSummaries(partitionId) = decoded.rowLevelSummary
          }
        }
      }
    }
    val partitionsToWrite = messages.indices.filter(messages(_) == null)

    logInfo(log"Start processing data source write support: " +
      log"${MDC(LogKeys.BATCH_WRITE, batchWrite)}. The input RDD has " +
      log"${MDC(LogKeys.COUNT, messages.length)} partitions.")

    // Avoid object not serializable issue.
    val writeMetrics: Map[String, SQLMetric] = customMetrics

    try {
      if (!alreadyCommitted && partitionsToWrite.nonEmpty) {
        // introduce local vars to avoid serializing the whole physical plan
        val task = writingTask
        val writerFactory = batchWrite.createBatchWriterFactory(physicalWriteInfo)
        if (taskCommitContext.isDefined && !writerFactory.isInstanceOf[RecoveryDataWriterFactory]) {
          throw new IllegalStateException(
            s"Recovery writer factory ${writerFactory.getClass.getName} does not implement " +
              classOf[RecoveryDataWriterFactory].getName)
        }
        // The driver-local OutputCommitCoordinator cannot arbitrate a recovery write. If a writer
        // commits and its durable-store response is lost, a retry must still reach the immutable
        // CAS to discover the canonical value. A coordinator denial would wedge the write until
        // another driver restart. The durable task store is the coordinator here; losing attempts
        // are reclaimed through SupportsRecoveryCommitDiscard below.
        val useCommitCoordinator = batchWrite.useCommitCoordinator && taskCommitContext.isEmpty
        sparkContext.runJob(
          rdd,
          (context: TaskContext, iter: Iterator[InternalRow]) =>
            task.run(
              writerFactory,
              context,
              iter,
              useCommitCoordinator,
              writeMetrics,
              taskCommitContext),
          partitionsToWrite,
          (resultIndex, result: DataWritingSparkTaskResult) => {
            val partitionIndex = partitionsToWrite(resultIndex)
            val commitMessage = result.writerCommitMessage
            messages(partitionIndex) = commitMessage
            canonicalRows(partitionIndex) = result.numRows
            canonicalMetrics(partitionIndex) = result.recoveryMetrics
            canonicalRowLevelSummaries(partitionIndex) = result.rowLevelSummary
            // A store-authoritative commit can be returned by executor preflight, by a retry
            // after a publish response was lost, or by a losing speculative attempt. None of
            // those cases can support an exactly-once driver callback. Recovery-capable writes
            // must reconstruct all global state from the canonical messages passed to commit.
            if (taskCommitContext.isEmpty) {
              batchWrite.onDataWriterCommit(commitMessage)
            }
          }
        )
      }

      if (alreadyCommitted) {
        recoveryState.map(_.totalNumRows()).filter(_ >= 0).foreach(numOutputRowsMetric.add)
        taskCommitContext.flatMap(_.metricSchema).foreach { schema =>
          val durableTotals = Option(recoveryState.get.totalTaskMetrics()).getOrElse {
            throw new IllegalStateException(
              "A committed recovery state returned null task metric totals")
          }.asScala.toMap
          val descriptors = schema.descriptors()
          val expectedNames = descriptors.iterator.map(_.name()).toSet
          require(durableTotals.keySet == expectedNames,
            "Committed recovery task metric names " +
              durableTotals.keys.toSeq.sorted.mkString(", ") + " " +
              s"do not match the durable schema ${expectedNames.toSeq.sorted.mkString(", ")}")
          descriptors.foreach { descriptor =>
            val value = Option(durableTotals(descriptor.name())).map(_.longValue()).getOrElse {
              throw new IllegalStateException(
                s"Committed recovery task metric ${descriptor.name()} is null")
            }
            require(descriptor.accepts(value),
              s"Committed recovery task metric ${descriptor.name()} has out-of-range value $value")
            customMetrics(descriptor.name()).add(value)
          }
        }
        if (requiresRowLevelTaskSummary) {
          val durableSummary = Option(recoveryState.get.totalRowLevelSummary())
            .filter(_.isPresent)
            .map(_.get())
            .getOrElse {
              throw new IllegalStateException(
                "A committed row-level recovery state returned no authoritative summary")
            }
          restoreRowLevelTaskSummary(durableSummary)
        }
      } else {
        var totalRows = 0L
        val metricTotals = scala.collection.mutable.HashMap.empty[String, Long]
        canonicalRows.indices.foreach { partitionId =>
          require(canonicalRows(partitionId) >= 0,
            s"Missing canonical output row count for partition $partitionId")
          try {
            totalRows = Math.addExact(totalRows, canonicalRows(partitionId))
          } catch {
            case _: ArithmeticException =>
              throw new IllegalStateException("Recovered output row count overflow")
          }
          canonicalMetrics(partitionId).foreach { case (name, value) =>
            require(customMetrics.contains(name),
              s"Unknown canonical recovery task metric: $name")
            val current = metricTotals.getOrElse(name, 0L)
            try {
              metricTotals.put(name, Math.addExact(current, value))
            } catch {
              case _: ArithmeticException =>
                throw new IllegalStateException(
                  s"Recovered task metric $name overflow")
            }
          }
        }
        numOutputRowsMetric.add(totalRows)
        metricTotals.foreach { case (name, value) => customMetrics(name).add(value) }
        if (requiresRowLevelTaskSummary) {
          val summaries = canonicalRowLevelSummaries.indices.map { partitionId =>
            canonicalRowLevelSummaries(partitionId).getOrElse {
              throw new IllegalStateException(
                s"Missing canonical row-level summary for partition $partitionId")
            }
          }
          restoreRowLevelTaskSummary(RowLevelTaskSummary.sum(summaries.asJava))
        }
      }

      val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
      val recoveredCustomMetrics = taskCommitContext
        .filter(_.metricSchema.isDefined)
        .toSeq
        .flatMap(_ => customMetrics.values)
      SQLMetrics.postDriverMetricUpdates(
        sparkContext, executionId, Seq(numOutputRowsMetric) ++ recoveredCustomMetrics)

      if (!alreadyCommitted) {
        val writeSummary = getWriteSummary()
        logInfo(
          log"Data source write support ${MDC(LogKeys.BATCH_WRITE, batchWrite)} is committing.")
        writeSummary match {
          case Some(summary) => batchWrite.commit(messages, summary)
          case None => batchWrite.commit(messages)
        }
        logInfo(log"Data source write support ${MDC(LogKeys.BATCH_WRITE, batchWrite)} committed.")
      } else {
        logInfo(
          log"Data source write support ${MDC(LogKeys.BATCH_WRITE, batchWrite)} was already " +
            log"committed; skipping all writer tasks and the global commit.")
      }
      commitProgress = Some(StreamWriterCommitProgress(numOutputRowsMetric.value))
    } catch {
      case cause: Throwable =>
        logError(
          log"Data source write support ${MDC(LogKeys.BATCH_WRITE, batchWrite)} is aborting.")
        try {
          recoveryState match {
            case Some(_) =>
              batchWrite.asInstanceOf[SupportsBatchWriteRecovery].abortAfterRecovery(messages)
            case None => batchWrite.abort(messages)
          }
        } catch {
          case t: Throwable =>
            logError(log"Data source write support ${MDC(LogKeys.BATCH_WRITE, batchWrite)} " +
              log"failed to abort.")
            cause.addSuppressed(t)
            throw QueryExecutionErrors.writingJobFailedError(cause)
        }
        logError(log"Data source write support ${MDC(LogKeys.BATCH_WRITE, batchWrite)} aborted.")
        throw cause
    }

    Nil
  }

  protected def getWriteSummary(): Option[WriteSummary] = None

  /** Restores Spark-owned row-level metrics before the connector commit summary is constructed. */
  protected def restoreRowLevelTaskSummary(summary: RowLevelTaskSummary): Unit = {
    require(!requiresRowLevelTaskSummary,
      s"${getClass.getName} requires a row-level summary but does not restore it")
  }
}

trait WritingSparkTask[W <: DataWriter[InternalRow]] extends Logging with Serializable {

  protected def write(writer: W, iter: java.util.Iterator[InternalRow]): Unit

  /** Returns exact Spark-owned row-level counters produced by this invocation, when applicable. */
  protected def rowLevelTaskSummary(): Option[RowLevelTaskSummary] = None

  /** Returns connector records written, excluding Spark-owned summary-only control rows. */
  protected def numRowsWritten(numInputRows: Long): Long = numInputRows

  def run(
      writerFactory: DataWriterFactory,
      context: TaskContext,
      iter: Iterator[InternalRow],
      useCommitCoordinator: Boolean,
      customMetrics: Map[String, SQLMetric],
      recovery: Option[RecoveryTaskCommitContext] = None): DataWritingSparkTaskResult = {
    val stageId = context.stageId()
    val stageAttempt = context.stageAttemptNumber()
    val partId = context.partitionId()
    val taskId = context.taskAttemptId()
    val attemptId = context.attemptNumber()

    // Recheck immediately on the executor. The driver's recovery snapshot can race with a late
    // successful attempt, and a retry after an accepted-but-lost publish response must discover
    // that canonical commit without creating a writer or consuming its input again.
    recovery.foreach { recoveryContext =>
      val stored = Option(recoveryContext.store.load(
        recoveryContext.recoveryId, Array(partId))).getOrElse {
        throw new IllegalStateException(
          s"Recovery task commit store returned a null preflight result for partition $partId")
      }
      require(stored.size() == 1,
        s"Recovery task commit store returned ${stored.size()} preflight values for one key")
      val storedValue = Option(stored.get(0)).getOrElse {
        throw new IllegalStateException(
          s"Recovery task commit store returned a null preflight entry for partition $partId")
      }
      if (storedValue.isPresent) {
        val envelope = storedValue.get()
        RecoveryTaskCommitEnvelope.validateTaskCommitSize(recoveryContext, envelope)
        val decoded = RecoveryTaskCommitEnvelope.decode(recoveryContext, partId, envelope)
        logInfo(log"Using canonical recovery commit for partition " +
          log"${MDC(LogKeys.PARTITION_ID, partId)} before creating a data writer.")
        return DataWritingSparkTaskResult(
          decoded.numRows, decoded.message, decoded.metrics, decoded.rowLevelSummary)
      }
    }
    val dataWriter = writerFactory.createWriter(partId, taskId).asInstanceOf[W]
    var writerCommitted = false

    val updateMetricsOnExecutor = recovery.forall(_.metricSchema.isEmpty)
    val iterWithMetrics = IteratorWithMetrics(
      iter, dataWriter, customMetrics, updateMetricsOnExecutor)

    // write the data and commit this writer.
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      write(dataWriter, iterWithMetrics)
      val currentRowLevelSummary = rowLevelTaskSummary()
      val currentNumRows = numRowsWritten(iterWithMetrics.count)
      require(currentNumRows >= 0L && currentNumRows <= iterWithMetrics.count,
        s"Invalid connector output row count $currentNumRows for ${iterWithMetrics.count} inputs")

      val currentMetrics = Option(dataWriter.currentMetricsValues).getOrElse {
        throw new IllegalStateException("Data writer returned null current metrics")
      }.toImmutableArraySeq
      recovery.foreach(RecoveryTaskCommitEnvelope.validateCurrentMetrics(_, currentMetrics))
      if (updateMetricsOnExecutor) {
        CustomMetrics.updateMetrics(currentMetrics, customMetrics)
      }

      val msg = if (useCommitCoordinator) {
        val coordinator = SparkEnv.get.outputCommitCoordinator
        val commitAuthorized = coordinator.canCommit(stageId, stageAttempt, partId, attemptId)
        if (commitAuthorized) {
          logInfo(log"Commit authorized for partition ${MDC(LogKeys.PARTITION_ID, partId)} " +
            log"(task ${MDC(LogKeys.TASK_ID, taskId)}, " +
            log"attempt ${MDC(LogKeys.TASK_ATTEMPT_ID, attemptId)}, " +
            log"stage ${MDC(LogKeys.STAGE_ID, stageId)}." +
            log"${MDC(LogKeys.STAGE_ATTEMPT_ID, stageAttempt)})")

          dataWriter.commit()
        } else {
          val commitDeniedException = QueryExecutionErrors.commitDeniedError(
            partId, taskId, attemptId, stageId, stageAttempt)
          logInfo(log"${MDC(LogKeys.ERROR, commitDeniedException.getMessage)}")
          // throwing CommitDeniedException will trigger the catch block for abort
          throw commitDeniedException
        }

      } else {
        logInfo(log"Writer for partition ${MDC(LogKeys.PARTITION_ID, context.partitionId())} " +
          log"is committing.")
        dataWriter.commit()
      }
      writerCommitted = true

      logInfo(log"Committed partition ${MDC(LogKeys.PARTITION_ID, partId)} " +
        log"(task ${MDC(LogKeys.TASK_ID, taskId)}, " +
        log"attempt ${MDC(LogKeys.TASK_ATTEMPT_ID, attemptId)}, " +
        log"stage ${MDC(LogKeys.STAGE_ID, stageId)}." +
        log"${MDC(LogKeys.STAGE_ATTEMPT_ID, stageAttempt)})")

      val result = recovery.map { recoveryContext =>
        val proposed = RecoveryTaskCommitEnvelope.encode(
          recoveryContext,
          partId,
          msg,
          currentNumRows,
          currentMetrics,
          currentRowLevelSummary)
        RecoveryTaskCommitEnvelope.validateTaskCommitSize(recoveryContext, proposed)
        val canonical = Option(recoveryContext.store.publish(
          recoveryContext.recoveryId,
          partId,
          taskId,
          attemptId,
          proposed)).getOrElse {
          throw new IllegalStateException(
            s"Recovery task commit store returned null for partition $partId")
        }
        RecoveryTaskCommitEnvelope.validateTaskCommitSize(recoveryContext, canonical)
        val decoded = RecoveryTaskCommitEnvelope.decode(recoveryContext, partId, canonical)
        if (!java.util.Arrays.equals(proposed, canonical)) {
          dataWriter match {
            case discardable: SupportsRecoveryCommitDiscard =>
              discardable.discardCommittedOutput(msg)
            case _ => throw new IllegalStateException(
              s"Recovery writer ${dataWriter.getClass.getName} cannot discard losing output")
          }
        }
        DataWritingSparkTaskResult(
          decoded.numRows, decoded.message, decoded.metrics, decoded.rowLevelSummary)
      }.getOrElse(DataWritingSparkTaskResult(
        currentNumRows, msg, Map.empty, currentRowLevelSummary))

      result

    })(catchBlock = {
      if (!writerCommitted) {
        // DataWriter.abort has undefined semantics after commit and must not be used for cleanup.
        logError(log"Aborting commit for partition ${MDC(LogKeys.PARTITION_ID, partId)} " +
          log"(task ${MDC(LogKeys.TASK_ID, taskId)}, " +
          log"attempt ${MDC(LogKeys.TASK_ATTEMPT_ID, attemptId)}, " +
          log"stage ${MDC(LogKeys.STAGE_ID, stageId)}." +
          log"${MDC(LogKeys.STAGE_ATTEMPT_ID, stageAttempt)})")
        dataWriter.abort()
        logError(log"Aborted commit for partition ${MDC(LogKeys.PARTITION_ID, partId)} " +
          log"(task ${MDC(LogKeys.TASK_ID, taskId)}, " +
          log"attempt ${MDC(LogKeys.TASK_ATTEMPT_ID, attemptId)}, " +
          log"stage ${MDC(LogKeys.STAGE_ID, stageId)}." +
          log"${MDC(LogKeys.STAGE_ATTEMPT_ID, stageAttempt)})")
      }
    }, finallyBlock = {
      dataWriter.close()
    })
  }

  private case class IteratorWithMetrics(
      iter: Iterator[InternalRow],
      dataWriter: W,
      customMetrics: Map[String, SQLMetric],
      updateMetrics: Boolean) extends java.util.Iterator[InternalRow] {
    var count = 0L

    override def hasNext: Boolean = iter.hasNext

    override def next(): InternalRow = {
      if (updateMetrics && count % CustomMetrics.NUM_ROWS_PER_UPDATE == 0) {
        CustomMetrics.updateMetrics(
          dataWriter.currentMetricsValues.toImmutableArraySeq, customMetrics)
      }
      count += 1
      iter.next()
    }
  }
}

case class DataAndMetadataWritingSparkTask(
    dataProj: ProjectingInternalRow,
    metadataProj: ProjectingInternalRow,
    sparkMetrics: Map[String, SQLMetric])
  extends WritingSparkTask[DataWriter[InternalRow]] {

  private var completedSummary: Option[RowLevelTaskSummary] = None
  private var completedRows = 0L

  override protected def write(
      writer: DataWriter[InternalRow],
      iter: java.util.Iterator[InternalRow]): Unit = {
    val summary = new RowLevelTaskSummaryAccumulator
    completedSummary = None
    completedRows = 0L

    while (iter.hasNext) {
      val row = iter.next()
      val operation = summary.record(row.getInt(0))

      operation match {
        case Some(UPDATE_OPERATION) =>
          dataProj.project(row)
          metadataProj.project(row)
          writer.write(metadataProj, dataProj)
          completedRows = Math.addExact(completedRows, 1L)

        case Some(COPY_OPERATION) =>
          dataProj.project(row)
          metadataProj.project(row)
          writer.write(metadataProj, dataProj)
          completedRows = Math.addExact(completedRows, 1L)

        case Some(INSERT_OPERATION) =>
          dataProj.project(row)
          writer.write(dataProj)
          completedRows = Math.addExact(completedRows, 1L)

        case None =>

        case other =>
          throw new SparkException(s"Unexpected group-replacement operation: $other")
      }
    }

    completedSummary = Some(summary.summary)
  }

  override protected def rowLevelTaskSummary(): Option[RowLevelTaskSummary] = completedSummary
  override protected def numRowsWritten(numInputRows: Long): Long = completedRows
}

case class DataWithProjectionWritingSparkTask(
    dataProj: ProjectingInternalRow,
    sparkMetrics: Map[String, SQLMetric])
  extends WritingSparkTask[DataWriter[InternalRow]] {

  private var completedSummary: Option[RowLevelTaskSummary] = None
  private var completedRows = 0L

  override protected def write(
      writer: DataWriter[InternalRow],
      iter: java.util.Iterator[InternalRow]): Unit = {
    val summary = new RowLevelTaskSummaryAccumulator
    completedSummary = None
    completedRows = 0L

    while (iter.hasNext) {
      val row = iter.next()
      val operation = summary.record(row.getInt(0))

      operation match {
        case Some(UPDATE_OPERATION) =>
          dataProj.project(row)
          writer.write(dataProj)
          completedRows = Math.addExact(completedRows, 1L)

        case Some(COPY_OPERATION) =>
          dataProj.project(row)
          writer.write(dataProj)
          completedRows = Math.addExact(completedRows, 1L)

        case Some(INSERT_OPERATION) =>
          dataProj.project(row)
          writer.write(dataProj)
          completedRows = Math.addExact(completedRows, 1L)

        case None =>

        case other =>
          throw new SparkException(s"Unexpected group-replacement operation: $other")
      }
    }

    completedSummary = Some(summary.summary)
  }

  override protected def rowLevelTaskSummary(): Option[RowLevelTaskSummary] = completedSummary
  override protected def numRowsWritten(numInputRows: Long): Long = completedRows
}

object DataWritingSparkTask extends WritingSparkTask[DataWriter[InternalRow]] {
  override protected def write(
      writer: DataWriter[InternalRow],
      iter: java.util.Iterator[InternalRow]): Unit = {
    writer.writeAll(iter)
  }
}

case class DeltaWritingSparkTask(
    projections: WriteDeltaProjections,
    sparkMetrics: Map[String, SQLMetric])
  extends WritingSparkTask[DeltaWriter[InternalRow]] {

  private var completedSummary: Option[RowLevelTaskSummary] = None
  private var completedRows = 0L

  private lazy val rowProjection = projections.rowProjection.orNull
  private lazy val rowIdProjection = projections.rowIdProjection

  override protected def write(
      writer: DeltaWriter[InternalRow],
      iter: java.util.Iterator[InternalRow]): Unit = {
    val summary = new RowLevelTaskSummaryAccumulator
    completedSummary = None
    completedRows = 0L

    while (iter.hasNext) {
      val row = iter.next()
      val operation = summary.record(row.getInt(0))

      operation match {
        case Some(DELETE_OPERATION) =>
          rowIdProjection.project(row)
          writer.delete(null, rowIdProjection)
          completedRows = Math.addExact(completedRows, 1L)

        case Some(UPDATE_OPERATION) =>
          rowProjection.project(row)
          rowIdProjection.project(row)
          writer.update(null, rowIdProjection, rowProjection)
          completedRows = Math.addExact(completedRows, 1L)

        case Some(REINSERT_OPERATION) =>
          rowProjection.project(row)
          writer.reinsert(null, rowProjection)
          completedRows = Math.addExact(completedRows, 1L)

        case Some(INSERT_OPERATION) =>
          rowProjection.project(row)
          writer.insert(rowProjection)
          completedRows = Math.addExact(completedRows, 1L)

        case None =>

        case other =>
          throw new SparkException(s"Unexpected delta operation: $other")
      }
    }

    completedSummary = Some(summary.summary)
  }

  override protected def rowLevelTaskSummary(): Option[RowLevelTaskSummary] = completedSummary
  override protected def numRowsWritten(numInputRows: Long): Long = completedRows
}

case class DeltaWithMetadataWritingSparkTask(
    projections: WriteDeltaProjections,
    sparkMetrics: Map[String, SQLMetric])
  extends WritingSparkTask[DeltaWriter[InternalRow]] {

  private var completedSummary: Option[RowLevelTaskSummary] = None
  private var completedRows = 0L

  private lazy val rowProjection = projections.rowProjection.orNull
  private lazy val rowIdProjection = projections.rowIdProjection
  private lazy val metadataProjection = projections.metadataProjection.orNull

  override protected def write(
      writer: DeltaWriter[InternalRow],
      iter: java.util.Iterator[InternalRow]): Unit = {
    val summary = new RowLevelTaskSummaryAccumulator
    completedSummary = None
    completedRows = 0L

    while (iter.hasNext) {
      val row = iter.next()
      val operation = summary.record(row.getInt(0))

      operation match {
        case Some(DELETE_OPERATION) =>
          rowIdProjection.project(row)
          metadataProjection.project(row)
          writer.delete(metadataProjection, rowIdProjection)
          completedRows = Math.addExact(completedRows, 1L)

        case Some(UPDATE_OPERATION) =>
          rowProjection.project(row)
          rowIdProjection.project(row)
          metadataProjection.project(row)
          writer.update(metadataProjection, rowIdProjection, rowProjection)
          completedRows = Math.addExact(completedRows, 1L)

        case Some(REINSERT_OPERATION) =>
          rowProjection.project(row)
          metadataProjection.project(row)
          writer.reinsert(metadataProjection, rowProjection)
          completedRows = Math.addExact(completedRows, 1L)

        case Some(INSERT_OPERATION) =>
          rowProjection.project(row)
          writer.insert(rowProjection)
          completedRows = Math.addExact(completedRows, 1L)

        case None =>

        case other =>
          throw new SparkException(s"Unexpected delta operation: $other")
      }
    }

    completedSummary = Some(summary.summary)
  }

  override protected def rowLevelTaskSummary(): Option[RowLevelTaskSummary] = completedSummary
  override protected def numRowsWritten(numInputRows: Long): Long = completedRows
}

private[v2] trait V2CreateTableAsSelectBaseExec extends LeafV2CommandExec {
  override def output: Seq[Attribute] = Nil

  protected def loadTableForInsert(
      catalog: TableCatalog,
      ident: Identifier,
      writeOptions: Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(writeOptions.asJava)
    CatalogV2Util.loadTableForV2Write(
      catalog, ident, Set(TableWritePrivilege.INSERT), options)
  }

  protected def getV2Columns(schema: StructType, forceNullable: Boolean): Array[Column] = {
    val rawSchema = CharVarcharUtils.getRawSchema(removeInternalMetadata(schema), conf)
    val tableSchema = if (forceNullable) rawSchema.asNullable else rawSchema
    CatalogV2Util.structTypeToV2Columns(tableSchema, keepIds = false)
  }

  protected def writeToTable(
      catalog: TableCatalog,
      table: Table,
      writeOptions: Map[String, String],
      ident: Identifier,
      query: LogicalPlan,
      overwrite: Boolean,
      refreshPhaseEnabled: Boolean = true): Seq[InternalRow] = {
    Utils.tryWithSafeFinallyAndFailureCallbacks({
      val tableOptions = new CaseInsensitiveStringMap(writeOptions.asJava)
      val relation =
        DataSourceV2Relation.create(table, Some(catalog), Some(ident), tableOptions)
      val writeCommand = if (overwrite) {
        OverwriteByExpression.byPosition(relation, query, Literal.TrueLiteral, writeOptions)
      } else {
        AppendData.byPosition(relation, query, writeOptions)
      }
      QueryExecution.runCommand(
        session,
        writeCommand,
        "inner data writing for CTAS/RTAS",
        refreshPhaseEnabled)
      DataSourceV2Utils.commitStagedChanges(sparkContext, table, metrics)
      Nil
    })(catchBlock = {
      table match {
        // Failure rolls back the staged writes and metadata changes.
        case st: StagedTable => st.abortStagedChanges()
        case _ => catalog.dropTable(ident)
      }
    })
  }
}

private[v2] case class DataWritingSparkTaskResult(
    numRows: Long,
    writerCommitMessage: WriterCommitMessage,
    recoveryMetrics: Map[String, Long],
    rowLevelSummary: Option[RowLevelTaskSummary] = None)

/**
 * Sink progress information collected after commit.
 */
private[sql] case class StreamWriterCommitProgress(numOutputRows: Long)
