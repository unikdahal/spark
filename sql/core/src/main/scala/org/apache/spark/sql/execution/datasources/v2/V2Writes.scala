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

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.UUID

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.analysis.{RecoveryAnchorResolver, WriteRecoveryInfo}
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, InsertOnlyMerge, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic, ReplaceData, WriteDelta}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.WriteDeltaProjections
import org.apache.spark.sql.connector.catalog.{SupportsRecoveryAnchor, SupportsRecoveryWrite, Table}
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.recovery.RecoveryTaskCommitStore
import org.apache.spark.sql.connector.write.{BatchWrite, BatchWriteRecoveryState,
  DataWriterFactory, DeltaBatchWrite, DeltaWrite, DeltaWriteBuilder, LogicalWriteInfoImpl,
  PhysicalWriteInfo, RecoveryCommitMessageCodec, RecoveryDeltaWriterFactory,
  RecoveryTaskMetricSchema, RequiresDistributionAndOrdering, RowLevelOperationTable,
  SupportsBatchWriteRecovery,
  SupportsDeltaBatchWriteRecovery, SupportsDynamicOverwrite, SupportsOverwriteV2,
  SupportsRecoveryTaskMetrics, SupportsTruncate, Write, WriteBuilder, WriterCommitMessage,
  WriteSummary}
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
    // Resolve this once per rule invocation. A provider can be backed by external state and must
    // not be observed inconsistently while constructing one logical write.
    val resolver = recoveryResolver()
    plan transformDown {
    case a @ AppendData(r: DataSourceV2Relation, query, options, _, _, None, _) =>
      val writeOptions = mergeOptions(options, r.options.asCaseSensitiveMap.asScala.toMap)
      val prepared = newWriteBuilder(
        r.table, writeOptions, query.schema, operation = Some(a), resolver = resolver)
      val write = requireRecoverableWrite(
        r.table, prepared.builder.build(), prepared.recoveryId, resolver)
      val newQuery = DistributionAndOrderingUtils.prepareQuery(write, query, r.funCatalog)
      a.copy(write = Some(write), query = newQuery)

    case m @ InsertOnlyMerge(r: DataSourceV2Relation, query, None, _) =>
      val writeOptions = r.options.asCaseSensitiveMap.asScala.toMap
      val prepared = newWriteBuilder(
        r.table, writeOptions, query.schema, operation = Some(m), resolver = resolver)
      val write = requireRecoverableWrite(
        r.table, prepared.builder.build(), prepared.recoveryId, resolver)
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
      val prepared = newWriteBuilder(
        table, writeOptions, query.schema, operation = Some(o), resolver = resolver)
      val connectorWrite = prepared.builder match {
        case builder: SupportsTruncate if isTruncate(predicates) =>
          builder.truncate().build()
        case builder: SupportsOverwriteV2 if builder.canOverwrite(predicates) =>
          builder.overwrite(predicates).build()
        case _ =>
          throw QueryExecutionErrors.overwriteTableByUnsupportedExpressionError(table)
      }
      val write = requireRecoverableWrite(
        table, connectorWrite, prepared.recoveryId, resolver)

      val newQuery = DistributionAndOrderingUtils.prepareQuery(write, query, r.funCatalog)
      o.copy(write = Some(write), query = newQuery)

    case o @ OverwritePartitionsDynamic(r: DataSourceV2Relation, query, options, _, _, None) =>
      val table = r.table
      val writeOptions = mergeOptions(options, r.options.asCaseSensitiveMap.asScala.toMap)
      val prepared = newWriteBuilder(
        table, writeOptions, query.schema, operation = Some(o), resolver = resolver)
      val connectorWrite = prepared.builder match {
        case builder: SupportsDynamicOverwrite =>
          builder.overwriteDynamicPartitions().build()
        case _ =>
          throw QueryExecutionErrors.dynamicPartitionOverwriteUnsupportedByTableError(table)
      }
      val write = requireRecoverableWrite(
        table, connectorWrite, prepared.recoveryId, resolver)
      val newQuery = DistributionAndOrderingUtils.prepareQuery(write, query, r.funCatalog)
      o.copy(write = Some(write), query = newQuery)

    case WriteToMicroBatchDataSource(
        r: DataSourceV2Relation, query, queryId, options, outputMode, Some(batchId)) =>
      val table = r.table
      val writeOptions = mergeOptions(options, r.options.asCaseSensitiveMap.asScala.toMap)
      val prepared = newWriteBuilder(
        table, writeOptions, query.schema, queryId = queryId, resolver = resolver)
      val write = buildWriteForMicroBatch(table, prepared.builder, outputMode)
      val microBatchWrite = new MicroBatchWrite(batchId, write.toStreaming)
      val customMetrics = write.supportedCustomMetrics.toImmutableArraySeq
      val newQuery = DistributionAndOrderingUtils.prepareQuery(write, query, r.funCatalog)
      WriteToDataSourceV2(Some(r), microBatchWrite, newQuery, customMetrics)

    case rd @ ReplaceData(r: DataSourceV2Relation, _, query, _, projections, _, None) =>
      val rowSchema = projections.rowProjection.schema
      val metadataSchema = projections.metadataProjection.map(_.schema)
      val writeOptions = mergeOptions(Map.empty, r.options.asCaseSensitiveMap.asScala.toMap)
      val prepared = newWriteBuilder(
        r.table, writeOptions, rowSchema, metadataSchema, operation = Some(rd), resolver = resolver)
      val connectorWrite = prepared.builder.build()
      val manifestInput = buildRowLevelManifestInput(
        rd, query, r.table, connectorWrite, prepared.recoveryId, resolver,
        physicalMode = "REPLACE_DATA",
        rowSchema = Some(rowSchema), rowIdSchema = None, metadataSchema = metadataSchema)
      val write = requireRecoverableWrite(
        r.table, connectorWrite, prepared.recoveryId, resolver, manifestInput)
      val newQuery = DistributionAndOrderingUtils.prepareQuery(write, query, r.funCatalog)
      rd.copy(write = Some(write), query = newQuery)

    case wd @ WriteDelta(r: DataSourceV2Relation, _, query, _, projections, _, None) =>
      val writeOptions = mergeOptions(Map.empty, r.options.asCaseSensitiveMap.asScala.toMap)
      val prepared = newDeltaWriteBuilder(
        r.table, writeOptions, projections, operation = Some(wd), resolver = resolver)
      val connectorWrite = prepared.builder.build()
      val manifestInput = buildRowLevelManifestInput(
        wd, query, r.table, connectorWrite, prepared.recoveryId, resolver,
        physicalMode = "WRITE_DELTA",
        rowSchema = projections.rowProjection.map(_.schema),
        rowIdSchema = Some(projections.rowIdProjection.schema),
        metadataSchema = projections.metadataProjection.map(_.schema))
      val deltaWrite = requireRecoverableDeltaWrite(
        r.table, connectorWrite, prepared.recoveryId, resolver, manifestInput)
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

  private case class PreparedWriteBuilder(
      builder: WriteBuilder,
      recoveryId: Option[String])

  private case class PreparedDeltaWriteBuilder(
      builder: DeltaWriteBuilder,
      recoveryId: Option[String])

  private def newWriteBuilder(
      table: Table,
      writeOptions: Map[String, String],
      rowSchema: StructType,
      metadataSchema: Option[StructType] = None,
      queryId: String = UUID.randomUUID().toString,
      operation: Option[LogicalPlan] = None,
      resolver: Option[RecoveryAnchorResolver]): PreparedWriteBuilder = {

    val resolvedQueryId = resolveWriteId(table, queryId, operation, resolver)
    val info = LogicalWriteInfoImpl(
      resolvedQueryId,
      rowSchema,
      writeOptions.asOptions,
      rowIdSchema = None,
      metadataSchema = metadataSchema,
      isRecoveryEnabled = resolver.isDefined)
    PreparedWriteBuilder(
      table.asWritable.newWriteBuilder(info),
      Option.when(resolver.isDefined)(resolvedQueryId))
  }

  private def newDeltaWriteBuilder(
      table: Table,
      writeOptions: Map[String, String],
      projections: WriteDeltaProjections,
      queryId: String = UUID.randomUUID().toString,
      operation: Option[LogicalPlan] = None,
      resolver: Option[RecoveryAnchorResolver]): PreparedDeltaWriteBuilder = {

    val rowSchema = projections.rowProjection.map(_.schema).getOrElse(StructType(Nil))
    val rowIdSchema = Some(projections.rowIdProjection.schema)
    val metadataSchema = projections.metadataProjection.map(_.schema)

    val resolvedQueryId = resolveWriteId(table, queryId, operation, resolver)
    val info = LogicalWriteInfoImpl(
      resolvedQueryId,
      rowSchema,
      writeOptions.asOptions,
      rowIdSchema,
      metadataSchema,
      isRecoveryEnabled = resolver.isDefined)

    val writeBuilder = table.asWritable.newWriteBuilder(info)
    assert(writeBuilder.isInstanceOf[DeltaWriteBuilder], s"$writeBuilder must be DeltaWriteBuilder")
    PreparedDeltaWriteBuilder(
      writeBuilder.asInstanceOf[DeltaWriteBuilder],
      Option.when(resolver.isDefined)(resolvedQueryId))
  }

  private def resolveWriteId(
      table: Table,
      currentWriteId: String,
      operation: Option[LogicalPlan],
      resolver: Option[RecoveryAnchorResolver]): String = {
    resolver match {
      case None => currentWriteId
      case Some(resolver) =>
        val sink = unwrapRowLevelTable(table) match {
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
      recoveryId: Option[String],
      resolver: Option[RecoveryAnchorResolver],
      rowLevelManifestInput: Option[RowLevelWriteManifestInput] = None): Write = {
    resolver.map { recoveryResolver =>
      val store = recoveryResolver.taskCommitStore.getOrElse {
        throw new IllegalStateException(
          s"Recovery is enabled but no durable task commit store is configured for ${table.name()}")
      }
      val stableRecoveryId = recoveryId.getOrElse {
        throw new IllegalStateException(
          s"Recovery is enabled but write to ${table.name()} has no durable identity")
      }
      RecoveryRequiredWrite(
        write, table.name(), stableRecoveryId, store, rowLevelManifestInput)
    }.getOrElse(write)
  }

  private def requireRecoverableDeltaWrite(
      table: Table,
      write: DeltaWrite,
      recoveryId: Option[String],
      resolver: Option[RecoveryAnchorResolver],
      rowLevelManifestInput: Option[RowLevelWriteManifestInput] = None): DeltaWrite = {
    resolver.map { recoveryResolver =>
      val store = recoveryResolver.taskCommitStore.getOrElse {
        throw new IllegalStateException(
          s"Recovery is enabled but no durable task commit store is configured for ${table.name()}")
      }
      val stableRecoveryId = recoveryId.getOrElse {
        throw new IllegalStateException(
          s"Recovery is enabled but write to ${table.name()} has no durable identity")
      }
      RecoveryRequiredDeltaWrite(
        write, table.name(), stableRecoveryId, store, rowLevelManifestInput)
    }.getOrElse(write)
  }

  private def buildRowLevelManifestInput(
      operation: LogicalPlan,
      query: LogicalPlan,
      table: Table,
      write: Write,
      recoveryId: Option[String],
      resolver: Option[RecoveryAnchorResolver],
      physicalMode: String,
      rowSchema: Option[StructType],
      rowIdSchema: Option[StructType],
      metadataSchema: Option[StructType]): Option[RowLevelWriteManifestInput] = {
    resolver.map { recoveryResolver =>
      val stableRecoveryId = recoveryId.getOrElse {
        throw new IllegalStateException("A recoverable row-level write has no durable identity")
      }
      val sinkTable = unwrapRowLevelTable(table)
      val sink = sinkTable match {
        case recoverable: SupportsRecoveryWrite => recoverable
        case _ => throw new IllegalStateException(
          s"Recovery sink ${sinkTable.name()} has no stable identity")
      }
      val rowLevelOperation = table match {
        case RowLevelOperationTable(_, configuredOperation) => configuredOperation
        case _ => throw new IllegalStateException(
          s"Row-level write table ${table.name()} has no row-level operation")
      }
      val ordered = write match {
        case requirements: RequiresDistributionAndOrdering => Some(requirements)
        case _ => None
      }
      val sourceTables = operation.collect {
        case relation: DataSourceV2Relation => unwrapRowLevelTable(relation.table)
      }
      val sourceAnchors = sourceTables.map {
        case source: SupportsRecoveryAnchor =>
          RowLevelWriteSourceAnchor(source.recoverySourceId(), source.currentRecoveryAnchor())
        case source => throw new IllegalStateException(
          s"Recovery source ${source.name()} has no immutable anchor")
      }.groupBy(_.sourceId).map { case (sourceId, anchors) =>
        require(sourceId != null && sourceId.nonEmpty, "Recovery source ID must not be empty")
        require(anchors.map(_.anchor).distinct.size == 1,
          s"Recovery source $sourceId has inconsistent immutable anchors")
        anchors.head
      }.toSeq
      val distribution = ordered.map { requirements =>
        Option(requirements.requiredDistribution()).getOrElse {
          throw new IllegalStateException("A row-level write returned null required distribution")
        }
      }
      val ordering = ordered.toSeq.flatMap { requirements =>
        Option(requirements.requiredOrdering()).getOrElse {
          throw new IllegalStateException("A row-level write returned null required ordering")
        }.map(_.describe())
      }
      val canonicalBytes = operation.canonicalized.asCode.getBytes(StandardCharsets.UTF_8)
      RowLevelWriteManifestInput(
        recoveryExecutionId = recoveryResolver.recoveryExecutionId,
        recoveryId = stableRecoveryId,
        generation = stableRecoveryId,
        sinkId = sink.recoverySinkId(),
        tableName = sinkTable.name(),
        command = rowLevelOperation.command().toString,
        physicalMode = physicalMode,
        canonicalOperationSha256 =
          MessageDigest.getInstance("SHA-256").digest(canonicalBytes),
        inputSchemaJson = Some(
          DataTypeUtils.fromAttributes(query.children.flatMap(_.output)).json),
        outputSchemaJson = Some(query.schema.json),
        rowSchemaJson = rowSchema.map(_.json),
        rowIdSchemaJson = rowIdSchema.map(_.json),
        metadataSchemaJson = metadataSchema.map(_.json),
        distributionDescription = distribution.map(_.toString).getOrElse("unspecified"),
        distributionStrictlyRequired = ordered.exists(_.distributionStrictlyRequired()),
        requiredNumPartitions = ordered.map(requirements =>
          Math.max(0, requirements.requiredNumPartitions())).getOrElse(0),
        advisoryPartitionSizeInBytes =
          ordered.map(requirements =>
            Math.max(0L, requirements.advisoryPartitionSizeInBytes())).getOrElse(0L),
        orderingDescriptions = ordering,
        sourceAnchors = sourceAnchors,
        transactionId = None,
        connectorCompatibilityMetadata = Array.emptyByteArray)
    }
  }

  private def unwrapRowLevelTable(table: Table): Table = table match {
    case RowLevelOperationTable(underlying, _) => underlying
    case other => other
  }
}

/** Marks a batch write as recovery-required without calling `toBatch` during logical planning. */
private object RecoveryRequiredWrite {
  def apply(
      delegate: Write,
      tableName: String,
      recoveryId: String,
      store: RecoveryTaskCommitStore,
      rowLevelManifestInput: Option[RowLevelWriteManifestInput]): Write =
      delegate match {
    case ordered: RequiresDistributionAndOrdering =>
      new OrderedRecoveryRequiredWrite(
        ordered, tableName, recoveryId, store, rowLevelManifestInput)
    case _ => new RecoveryRequiredWrite(
      delegate, tableName, recoveryId, store, rowLevelManifestInput)
  }
}

private class RecoveryRequiredWrite(
    protected val delegate: Write,
    tableName: String,
    recoveryId: String,
    store: RecoveryTaskCommitStore,
    rowLevelManifestInput: Option[RowLevelWriteManifestInput]) extends Write {
  override def description(): String = delegate.description()
  override def supportedCustomMetrics(): Array[CustomMetric] = delegate.supportedCustomMetrics()
  override def reportDriverMetrics(): Array[CustomTaskMetric] = delegate.reportDriverMetrics()
  override def toStreaming(): StreamingWrite = delegate.toStreaming()

  override def toBatch(): BatchWrite = delegate.toBatch() match {
    case recoverable: SupportsRecoveryTaskMetrics =>
      new RecoveryRequiredMetricBatchWrite(
        recoverable, recoveryId, store, rowLevelManifestInput)
    case recoverable: SupportsBatchWriteRecovery =>
      new RecoveryRequiredBatchWrite(recoverable, recoveryId, store, rowLevelManifestInput)
    case other => throw new IllegalStateException(
      s"Recovery is enabled but batch write ${other.getClass.getName} for table $tableName does " +
        s"not implement ${classOf[SupportsBatchWriteRecovery].getName}")
  }
}

private class OrderedRecoveryRequiredWrite(
    ordered: RequiresDistributionAndOrdering,
    tableName: String,
    recoveryId: String,
    store: RecoveryTaskCommitStore,
    rowLevelManifestInput: Option[RowLevelWriteManifestInput])
  extends RecoveryRequiredWrite(
    ordered, tableName, recoveryId, store, rowLevelManifestInput)
  with RequiresDistributionAndOrdering {

  override def requiredDistribution(): Distribution = ordered.requiredDistribution()
  override def distributionStrictlyRequired(): Boolean = ordered.distributionStrictlyRequired()
  override def requiredNumPartitions(): Int = ordered.requiredNumPartitions()
  override def advisoryPartitionSizeInBytes(): Long = ordered.advisoryPartitionSizeInBytes()
  override def requiredOrdering(): Array[SortOrder] = ordered.requiredOrdering()
}

private object RecoveryRequiredDeltaWrite {
  def apply(
      delegate: DeltaWrite,
      tableName: String,
      recoveryId: String,
      store: RecoveryTaskCommitStore,
      rowLevelManifestInput: Option[RowLevelWriteManifestInput]): DeltaWrite = delegate match {
    case ordered: DeltaWrite with RequiresDistributionAndOrdering =>
      new OrderedRecoveryRequiredDeltaWrite(
        ordered, tableName, recoveryId, store, rowLevelManifestInput)
    case _ => new RecoveryRequiredDeltaWrite(
      delegate, tableName, recoveryId, store, rowLevelManifestInput)
  }
}

private class RecoveryRequiredDeltaWrite(
    protected val deltaDelegate: DeltaWrite,
    tableName: String,
    recoveryId: String,
    store: RecoveryTaskCommitStore,
    rowLevelManifestInput: Option[RowLevelWriteManifestInput]) extends DeltaWrite {
  override def description(): String = deltaDelegate.description()
  override def supportedCustomMetrics(): Array[CustomMetric] =
    deltaDelegate.supportedCustomMetrics()
  override def reportDriverMetrics(): Array[CustomTaskMetric] =
    deltaDelegate.reportDriverMetrics()
  override def toStreaming(): StreamingWrite = deltaDelegate.toStreaming()

  override def toBatch(): DeltaBatchWrite = deltaDelegate.toBatch() match {
    case recoverable: SupportsDeltaBatchWriteRecovery =>
      new RecoveryRequiredDeltaBatchWrite(
        recoverable, recoveryId, store, rowLevelManifestInput)
    case other => throw new IllegalStateException(
      s"Recovery is enabled but delta batch write ${other.getClass.getName} for table $tableName " +
        s"does not implement ${classOf[SupportsDeltaBatchWriteRecovery].getName}")
  }
}

private class OrderedRecoveryRequiredDeltaWrite(
    ordered: DeltaWrite with RequiresDistributionAndOrdering,
    tableName: String,
    recoveryId: String,
    store: RecoveryTaskCommitStore,
    rowLevelManifestInput: Option[RowLevelWriteManifestInput])
  extends RecoveryRequiredDeltaWrite(
    ordered, tableName, recoveryId, store, rowLevelManifestInput)
  with RequiresDistributionAndOrdering {

  override def requiredDistribution(): Distribution = ordered.requiredDistribution()
  override def distributionStrictlyRequired(): Boolean = ordered.distributionStrictlyRequired()
  override def requiredNumPartitions(): Int = ordered.requiredNumPartitions()
  override def advisoryPartitionSizeInBytes(): Long = ordered.advisoryPartitionSizeInBytes()
  override def requiredOrdering(): Array[SortOrder] = ordered.requiredOrdering()
}

private[datasources] trait HasRecoveryTaskCommitStore {
  def recoveryId: String
  def taskCommitStore: RecoveryTaskCommitStore
  def rowLevelManifestInput: Option[RowLevelWriteManifestInput] = None
}

private class RecoveryRequiredBatchWrite(
    protected val delegate: SupportsBatchWriteRecovery,
    val recoveryId: String,
    override val taskCommitStore: RecoveryTaskCommitStore,
    override val rowLevelManifestInput: Option[RowLevelWriteManifestInput])
  extends SupportsBatchWriteRecovery with HasRecoveryTaskCommitStore {

  override def commitMessageCodec(): RecoveryCommitMessageCodec = delegate.commitMessageCodec()

  override def recoveryCompatibilityMetadata(info: PhysicalWriteInfo): Array[Byte] =
    delegate.recoveryCompatibilityMetadata(info)

  override def recover(info: PhysicalWriteInfo): BatchWriteRecoveryState = delegate.recover(info)

  override def abortAfterRecovery(messages: Array[WriterCommitMessage]): Unit =
    delegate.abortAfterRecovery(messages)

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    delegate.createBatchWriterFactory(info)

  override def useCommitCoordinator(): Boolean = delegate.useCommitCoordinator()

  override def onDataWriterCommit(message: WriterCommitMessage): Unit =
    delegate.onDataWriterCommit(message)

  override def commit(messages: Array[WriterCommitMessage]): Unit = delegate.commit(messages)

  override def commit(messages: Array[WriterCommitMessage], summary: WriteSummary): Unit =
    delegate.commit(messages, summary)

  override def abort(messages: Array[WriterCommitMessage]): Unit = delegate.abort(messages)
}

private class RecoveryRequiredMetricBatchWrite(
    metricDelegate: SupportsRecoveryTaskMetrics,
    recoveryId: String,
    taskCommitStore: RecoveryTaskCommitStore,
    rowLevelManifestInput: Option[RowLevelWriteManifestInput])
  extends RecoveryRequiredBatchWrite(
    metricDelegate, recoveryId, taskCommitStore, rowLevelManifestInput)
  with SupportsRecoveryTaskMetrics {

  override def recoveryTaskMetricSchema(): RecoveryTaskMetricSchema =
    metricDelegate.recoveryTaskMetricSchema()
}

private class RecoveryRequiredDeltaBatchWrite(
    deltaDelegate: SupportsDeltaBatchWriteRecovery,
    recoveryId: String,
    taskCommitStore: RecoveryTaskCommitStore,
    rowLevelManifestInput: Option[RowLevelWriteManifestInput])
  extends RecoveryRequiredBatchWrite(
    deltaDelegate, recoveryId, taskCommitStore, rowLevelManifestInput)
  with SupportsDeltaBatchWriteRecovery {

  override def createBatchWriterFactory(
      info: PhysicalWriteInfo): RecoveryDeltaWriterFactory =
    deltaDelegate.createBatchWriterFactory(info)

  override def recoveryTaskMetricSchema(): RecoveryTaskMetricSchema =
    deltaDelegate.recoveryTaskMetricSchema()
}

object V2Writes {
  val ruleName: String = classOf[V2Writes].getName
}
