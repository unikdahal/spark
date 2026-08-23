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

import java.util.UUID

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.analysis.{RecoveryAnchorResolver, WriteRecoveryInfo}
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, InsertOnlyMerge, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic, ReplaceData, WriteDelta}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.catalyst.util.WriteDeltaProjections
import org.apache.spark.sql.connector.catalog.{SupportsRecoveryWrite, Table}
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.write.{BatchWrite, DeltaWriteBuilder, LogicalWriteInfoImpl,
  PhysicalWriteInfo, RecoveryTaskCommitStore, RequiresDistributionAndOrdering,
  SupportsBatchWriteRecovery, SupportsDynamicOverwrite, SupportsOverwriteV2, SupportsTruncate,
  Write, WriteBuilder, WriterCommitMessage, WriteSummary}
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.streaming.sources.{MicroBatchWrite, WriteToMicroBatchDataSource}
import org.apache.spark.sql.internal.connector.SupportsStreamingUpdateAsAppend
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ArrayImplicits._

/**
 * A rule that constructs logical writes.
 */
class V2Writes(recoveryResolver: () => Option[RecoveryAnchorResolver])
  extends Rule[LogicalPlan] with PredicateHelper {

  import DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Resolve this once per rule invocation. A provider can be backed by external state and must not
    // be observed inconsistently while constructing one logical write.
    val resolver = recoveryResolver()
    plan transformDown {
    case a @ AppendData(r: DataSourceV2Relation, query, options, _, _, None, _) =>
      val writeOptions = mergeOptions(options, r.options.asCaseSensitiveMap.asScala.toMap)
      val writeBuilder = newWriteBuilder(
        r.table, writeOptions, query.schema, operation = Some(a), resolver = resolver)
      val write = requireRecoverableWrite(r.table, writeBuilder.build(), resolver)
      val newQuery = DistributionAndOrderingUtils.prepareQuery(write, query, r.funCatalog)
      a.copy(write = Some(write), query = newQuery)

    case m @ InsertOnlyMerge(r: DataSourceV2Relation, query, None, _) =>
      val writeOptions = r.options.asCaseSensitiveMap.asScala.toMap
      val writeBuilder = newWriteBuilder(
        r.table, writeOptions, query.schema, operation = Some(m), resolver = resolver)
      val write = requireRecoverableWrite(r.table, writeBuilder.build(), resolver)
      val newQuery = DistributionAndOrderingUtils.prepareQuery(write, query, r.funCatalog)
      m.copy(write = Some(write), query = newQuery)

    case o @ OverwriteByExpression(
        r: DataSourceV2Relation, deleteExpr, query, options, _, _, None, _) =>
      // fail if any filter cannot be converted. correctness depends on removing all matching data.
      val predicates = splitConjunctivePredicates(deleteExpr).flatMap { pred =>
        val predicate = DataSourceV2Strategy.translateFilterV2(pred)
        if (predicate.isEmpty) {
          throw QueryCompilationErrors.cannotTranslateExpressionToSourceFilterError(pred)
        }
        predicate
      }.toArray

      val table = r.table
      val writeOptions = mergeOptions(options, r.options.asCaseSensitiveMap.asScala.toMap)
      val writeBuilder = newWriteBuilder(
        table, writeOptions, query.schema, operation = Some(o), resolver = resolver)
      val connectorWrite = writeBuilder match {
        case builder: SupportsTruncate if isTruncate(predicates) =>
          builder.truncate().build()
        case builder: SupportsOverwriteV2 if builder.canOverwrite(predicates) =>
          builder.overwrite(predicates).build()
        case _ =>
          throw QueryExecutionErrors.overwriteTableByUnsupportedExpressionError(table)
      }
      val write = requireRecoverableWrite(table, connectorWrite, resolver)

      val newQuery = DistributionAndOrderingUtils.prepareQuery(write, query, r.funCatalog)
      o.copy(write = Some(write), query = newQuery)

    case o @ OverwritePartitionsDynamic(r: DataSourceV2Relation, query, options, _, _, None) =>
      val table = r.table
      val writeOptions = mergeOptions(options, r.options.asCaseSensitiveMap.asScala.toMap)
      val writeBuilder = newWriteBuilder(
        table, writeOptions, query.schema, operation = Some(o), resolver = resolver)
      val connectorWrite = writeBuilder match {
        case builder: SupportsDynamicOverwrite =>
          builder.overwriteDynamicPartitions().build()
        case _ =>
          throw QueryExecutionErrors.dynamicPartitionOverwriteUnsupportedByTableError(table)
      }
      val write = requireRecoverableWrite(table, connectorWrite, resolver)
      val newQuery = DistributionAndOrderingUtils.prepareQuery(write, query, r.funCatalog)
      o.copy(write = Some(write), query = newQuery)

    case WriteToMicroBatchDataSource(
        r: DataSourceV2Relation, query, queryId, options, outputMode, Some(batchId)) =>
      val table = r.table
      val writeOptions = mergeOptions(options, r.options.asCaseSensitiveMap.asScala.toMap)
      val writeBuilder = newWriteBuilder(
        table, writeOptions, query.schema, queryId = queryId, resolver = resolver)
      val write = buildWriteForMicroBatch(table, writeBuilder, outputMode)
      val microBatchWrite = new MicroBatchWrite(batchId, write.toStreaming)
      val customMetrics = write.supportedCustomMetrics.toImmutableArraySeq
      val newQuery = DistributionAndOrderingUtils.prepareQuery(write, query, r.funCatalog)
      WriteToDataSourceV2(Some(r), microBatchWrite, newQuery, customMetrics)

    case rd @ ReplaceData(r: DataSourceV2Relation, _, query, _, projections, _, None) =>
      val rowSchema = projections.rowProjection.schema
      val metadataSchema = projections.metadataProjection.map(_.schema)
      val writeOptions = mergeOptions(Map.empty, r.options.asCaseSensitiveMap.asScala.toMap)
      val writeBuilder = newWriteBuilder(
        r.table, writeOptions, rowSchema, metadataSchema, operation = Some(rd), resolver = resolver)
      val write = requireRecoverableWrite(r.table, writeBuilder.build(), resolver)
      val newQuery = DistributionAndOrderingUtils.prepareQuery(write, query, r.funCatalog)
      rd.copy(write = Some(write), query = newQuery)

    case wd @ WriteDelta(r: DataSourceV2Relation, _, query, _, projections, _, None) =>
      val writeOptions = mergeOptions(Map.empty, r.options.asCaseSensitiveMap.asScala.toMap)
      val deltaWriteBuilder = newDeltaWriteBuilder(
        r.table, writeOptions, projections, operation = Some(wd), resolver = resolver)
      if (resolver.isDefined) {
        throw new UnsupportedOperationException(
          "Recovery of row-level delta writes requires durable custom task metrics")
      }
      val deltaWrite = deltaWriteBuilder.build()
      val newQuery = DistributionAndOrderingUtils.prepareQuery(deltaWrite, query, r.funCatalog)
      wd.copy(write = Some(deltaWrite), query = newQuery)
    }
  }

  private def mergeOptions(
      commandOptions: Map[String, String],
      dsOptions: Map[String, String]): Map[String, String] = {
    // for DataFrame API cases, same options are carried by both Command and DataSourceV2Relation
    // for SQL cases, options are only carried by DataSourceV2Relation
    assert(commandOptions == dsOptions || commandOptions.isEmpty || dsOptions.isEmpty)
    commandOptions ++ dsOptions
  }

  private def buildWriteForMicroBatch(
      table: Table,
      writeBuilder: WriteBuilder,
      outputMode: OutputMode): Write = {

    outputMode match {
      case Append =>
        writeBuilder.build()
      case Complete =>
        // TODO: we should do this check earlier when we have capability API.
        require(writeBuilder.isInstanceOf[SupportsTruncate],
          table.name + " does not support Complete mode.")
        writeBuilder.asInstanceOf[SupportsTruncate].truncate().build()
      case Update =>
        require(writeBuilder.isInstanceOf[SupportsStreamingUpdateAsAppend],
          table.name + " does not support Update mode.")
        writeBuilder.asInstanceOf[SupportsStreamingUpdateAsAppend].build()
    }
  }

  private def isTruncate(predicates: Array[Predicate]): Boolean = {
    predicates.length == 1 && predicates(0).name().equals("ALWAYS_TRUE")
  }

  private def newWriteBuilder(
      table: Table,
      writeOptions: Map[String, String],
      rowSchema: StructType,
      metadataSchema: Option[StructType] = None,
      queryId: String = UUID.randomUUID().toString,
      operation: Option[LogicalPlan] = None,
      resolver: Option[RecoveryAnchorResolver]): WriteBuilder = {

    val resolvedQueryId = resolveWriteId(table, queryId, operation, resolver)
    val info = LogicalWriteInfoImpl(
      resolvedQueryId,
      rowSchema,
      writeOptions.asOptions,
      rowIdSchema = None,
      metadataSchema = metadataSchema,
      isRecoveryEnabled = resolver.isDefined)
    table.asWritable.newWriteBuilder(info)
  }

  private def newDeltaWriteBuilder(
      table: Table,
      writeOptions: Map[String, String],
      projections: WriteDeltaProjections,
      queryId: String = UUID.randomUUID().toString,
      operation: Option[LogicalPlan] = None,
      resolver: Option[RecoveryAnchorResolver]): DeltaWriteBuilder = {

    val rowSchema = projections.rowProjection.map(_.schema).getOrElse(StructType(Nil))
    val rowIdSchema = Some(projections.rowIdProjection.schema)
    val metadataSchema = projections.metadataProjection.map(_.schema)

    val info = LogicalWriteInfoImpl(
      resolveWriteId(table, queryId, operation, resolver),
      rowSchema,
      writeOptions.asOptions,
      rowIdSchema,
      metadataSchema,
      isRecoveryEnabled = resolver.isDefined)

    val writeBuilder = table.asWritable.newWriteBuilder(info)
    assert(writeBuilder.isInstanceOf[DeltaWriteBuilder], s"$writeBuilder must be DeltaWriteBuilder")
    writeBuilder.asInstanceOf[DeltaWriteBuilder]
  }

  private def resolveWriteId(
      table: Table,
      currentWriteId: String,
      operation: Option[LogicalPlan],
      resolver: Option[RecoveryAnchorResolver]): String = {
    resolver match {
      case None => currentWriteId
      case Some(resolver) =>
        val sink = table match {
          case recoverable: SupportsRecoveryWrite => recoverable
          case _ => throw new IllegalStateException(
            s"Recovery is enabled but write table ${table.name()} does not implement " +
              classOf[SupportsRecoveryWrite].getName)
        }
        val logicalOperation = operation.getOrElse {
          throw new IllegalStateException(
            s"Recovery is enabled but write to ${sink.recoverySinkId()} has no logical identity")
        }
        val sinkId = sink.recoverySinkId()
        require(sinkId != null && sinkId.nonEmpty, "A recovery sink identity must not be empty")
        val resolved = resolver.resolveWriteId(WriteRecoveryInfo(
          sinkId, currentWriteId, logicalOperation, logicalOperation.canonicalized))
        require(resolved != null && resolved.nonEmpty,
          s"Recovery resolver returned an empty write ID for sink $sinkId")
        resolved
    }
  }

  private def requireRecoverableWrite(
      table: Table,
      write: Write,
      resolver: Option[RecoveryAnchorResolver]): Write = {
    resolver.map { recoveryResolver =>
      val store = recoveryResolver.taskCommitStore.getOrElse {
        throw new IllegalStateException(
          s"Recovery is enabled but no durable task commit store is configured for ${table.name()}")
      }
      RecoveryRequiredWrite(write, table.name(), store)
    }.getOrElse(write)
  }
}

/** Marks a batch write as recovery-required without calling `toBatch` during logical planning. */
private object RecoveryRequiredWrite {
  def apply(delegate: Write, tableName: String, store: RecoveryTaskCommitStore): Write =
      delegate match {
    case ordered: RequiresDistributionAndOrdering =>
      new OrderedRecoveryRequiredWrite(ordered, tableName, store)
    case _ => new RecoveryRequiredWrite(delegate, tableName, store)
  }
}

private class RecoveryRequiredWrite(
    protected val delegate: Write,
    tableName: String,
    store: RecoveryTaskCommitStore) extends Write {
  override def description(): String = delegate.description()
  override def supportedCustomMetrics(): Array[CustomMetric] = delegate.supportedCustomMetrics()
  override def reportDriverMetrics(): Array[CustomTaskMetric] = delegate.reportDriverMetrics()
  override def toStreaming(): StreamingWrite = delegate.toStreaming()

  override def toBatch(): BatchWrite = delegate.toBatch() match {
    case recoverable: SupportsBatchWriteRecovery =>
      new RecoveryRequiredBatchWrite(recoverable, store)
    case other => throw new IllegalStateException(
      s"Recovery is enabled but batch write ${other.getClass.getName} for table $tableName does " +
        s"not implement ${classOf[SupportsBatchWriteRecovery].getName}")
  }
}

private class OrderedRecoveryRequiredWrite(
    ordered: RequiresDistributionAndOrdering,
    tableName: String,
    store: RecoveryTaskCommitStore)
  extends RecoveryRequiredWrite(ordered, tableName, store) with RequiresDistributionAndOrdering {

  override def requiredDistribution(): Distribution = ordered.requiredDistribution()
  override def distributionStrictlyRequired(): Boolean = ordered.distributionStrictlyRequired()
  override def requiredNumPartitions(): Int = ordered.requiredNumPartitions()
  override def advisoryPartitionSizeInBytes(): Long = ordered.advisoryPartitionSizeInBytes()
  override def requiredOrdering(): Array[SortOrder] = ordered.requiredOrdering()
}

private[datasources] trait HasRecoveryTaskCommitStore {
  def taskCommitStore: RecoveryTaskCommitStore
}

private class RecoveryRequiredBatchWrite(
    delegate: SupportsBatchWriteRecovery,
    override val taskCommitStore: RecoveryTaskCommitStore)
  extends SupportsBatchWriteRecovery with HasRecoveryTaskCommitStore {

  override def recoveryId(): String = delegate.recoveryId()
  override def commitMessageCodec() = delegate.commitMessageCodec()
  override def recoveryCompatibilityMetadata(info: PhysicalWriteInfo) =
    delegate.recoveryCompatibilityMetadata(info)
  override def recover(info: PhysicalWriteInfo) = delegate.recover(info)
  override def abortAfterRecovery(messages: Array[WriterCommitMessage]) =
    delegate.abortAfterRecovery(messages)
  override def createBatchWriterFactory(info: PhysicalWriteInfo) =
    delegate.createBatchWriterFactory(info)
  override def useCommitCoordinator(): Boolean = delegate.useCommitCoordinator()
  override def onDataWriterCommit(message: WriterCommitMessage) =
    delegate.onDataWriterCommit(message)
  override def commit(messages: Array[WriterCommitMessage]) =
    delegate.commit(messages)
  override def commit(
      messages: Array[WriterCommitMessage],
      summary: WriteSummary) = delegate.commit(messages, summary)
  override def abort(messages: Array[WriterCommitMessage]) =
    delegate.abort(messages)
}

object V2Writes {
  val ruleName: String = classOf[V2Writes].getName
}
