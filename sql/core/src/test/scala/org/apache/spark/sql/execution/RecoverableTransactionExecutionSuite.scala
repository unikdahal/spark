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

package org.apache.spark.sql.execution

import java.util

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ResolvedTable, SourceRecoveryInfo}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafCommand, LogicalPlan, TransactionalWrite}
import org.apache.spark.sql.catalyst.transactions.TransactionUtils
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.connector.catalog.{BasicInMemoryTableCatalog, Identifier,
  SupportsTransactionRecovery, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.transactions.{Transaction, TransactionInfo,
  TransactionRecoveryInfo, TransactionRecoveryResult, TransactionRecoveryState}
import org.apache.spark.sql.execution.adaptive.{RecoveredShuffleStage, ShuffleStageRecovery,
  ShuffleStageRecoveryInfo}
import org.apache.spark.sql.execution.datasources.v2.{LeafV2CommandExec, TransactionalExec}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class RecoverableTransactionExecutionSuite extends SharedSparkSession {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.extensions", classOf[RecoverableTransactionTestExtensions].getName)

  override def beforeEach(): Unit = {
    super.beforeEach()
    RecoverableTransactionTestState.reset()
  }

  test("stable transaction identity is passed to the resolver") {
    val catalog = newCatalog(TransactionRecoveryState.OPEN)
    val command = testCommand(catalog)

    execute(command)
    catalog.forceState(TransactionRecoveryState.OPEN)
    execute(command)

    assert(catalog.recoveryInfos.length == 2)
    assert(catalog.recoveryInfos.map(_.recoveryExecutionId()).distinct ==
      Seq(RecoverableTransactionTestState.RecoveryExecutionId))
    assert(catalog.recoveryInfos.map(_.id()).distinct.length == 1)
    assert(catalog.recoveryInfos.head.id().startsWith("spark-txn-v1-"))
    assert(catalog.beginCalls == 0)
  }

  test("committed transaction short-circuits command execution and finalization") {
    val catalog = newCatalog(TransactionRecoveryState.COMMITTED)

    execute(testCommand(catalog))

    assert(catalog.resolveCalls == 1)
    assert(catalog.beginCalls == 0)
    assert(catalog.commitCalls == 0)
    assert(catalog.abortCalls == 0)
    assert(RecoverableTransactionTestState.writerCalls == 0)
  }

  for (state <- Seq(TransactionRecoveryState.ABORTED, TransactionRecoveryState.UNKNOWN)) {
    test(s"$state transaction fails closed without mutation") {
      val catalog = newCatalog(state)

      intercept[Throwable] {
        execute(testCommand(catalog))
      }

      assert(catalog.resolveCalls == 1)
      assert(catalog.beginCalls == 0)
      assert(catalog.commitCalls == 0)
      assert(catalog.abortCalls == 0)
      assert(RecoverableTransactionTestState.writerCalls == 0)
    }
  }

  test("open transaction executes the command and commits exactly once") {
    val catalog = newCatalog(TransactionRecoveryState.OPEN)

    execute(testCommand(catalog))

    assert(catalog.resolveCalls == 1)
    assert(catalog.beginCalls == 0)
    assert(catalog.commitCalls == 1)
    assert(catalog.abortCalls == 0)
    assert(catalog.durableState == TransactionRecoveryState.COMMITTED)
    assert(RecoverableTransactionTestState.writerCalls == 1)
  }

  test("accepted commit with a lost response is re-resolved and never aborted") {
    val catalog = newCatalog(TransactionRecoveryState.OPEN, loseCommitResponse = true)

    val error = intercept[Throwable] {
      execute(testCommand(catalog))
    }

    val errorMessages = Seq(error, error.getCause).filter(_ != null).map(_.getMessage)
    assert(errorMessages.exists(message => Option(message).exists(
      _.contains(RecoverableTransactionTestCatalog.LostCommitResponse))))
    assert(catalog.resolveCalls == 2)
    assert(catalog.recoveryInfos.map(_.id()).distinct.length == 1)
    assert(catalog.beginCalls == 0)
    assert(catalog.commitCalls == 1)
    assert(catalog.abortCalls == 0)
    assert(catalog.durableState == TransactionRecoveryState.COMMITTED)
    assert(RecoverableTransactionTestState.writerCalls == 1)
  }

  private def newCatalog(
      state: TransactionRecoveryState,
      loseCommitResponse: Boolean = false): RecoverableTransactionTestCatalog = {
    val catalog = new RecoverableTransactionTestCatalog(state, loseCommitResponse)
    catalog.initialize("recoverable_transaction_test", CaseInsensitiveStringMap.empty())
    catalog
  }

  private def testCommand(catalog: RecoverableTransactionTestCatalog): LogicalPlan = {
    val ident = Identifier.of(Array("ns"), "target")
    val target = ResolvedTable.create(catalog, ident, RecoverableTransactionTestTable)
    RecoverableTransactionTestCommand(target)
  }

  private def execute(command: LogicalPlan): Unit = {
    Dataset.ofRows(spark, command).collect()
  }
}

private object RecoverableTransactionTestState {
  val RecoveryExecutionId = "recoverable-transaction-execution"
  var writerCalls: Int = 0

  def reset(): Unit = {
    writerCalls = 0
  }
}

private object RecoverableTransactionTestTable extends Table {
  override def name(): String = "recoverable_transaction_test.ns.target"
  override def schema(): StructType = new StructType()
  override def capabilities(): util.Set[TableCapability] = util.Collections.emptySet()
}

private case class RecoverableTransactionTestCommand(table: LogicalPlan)
  extends LeafCommand with TransactionalWrite

private case class RecoverableTransactionTestExec(
    transaction: Option[Transaction] = None)
  extends LeafV2CommandExec with TransactionalExec {

  override def output: Seq[Attribute] = Nil

  override def withTransaction(txn: Option[Transaction]): RecoverableTransactionTestExec =
    copy(transaction = txn)

  override protected def run(): Seq[InternalRow] = {
    RecoverableTransactionTestState.writerCalls += 1
    TransactionUtils.commit(transaction.getOrElse {
      throw new IllegalStateException("recoverable command did not receive its open transaction")
    })
    Nil
  }
}

private object RecoverableTransactionTestStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case _: RecoverableTransactionTestCommand => RecoverableTransactionTestExec() :: Nil
    case _ => Nil
  }
}

class RecoverableTransactionTestExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy(_ => RecoverableTransactionTestStrategy)
    extensions.injectShuffleStageRecovery { _ =>
      new ShuffleStageRecovery {
        override def recoveryExecutionId: String =
          RecoverableTransactionTestState.RecoveryExecutionId

        override def protocolVersion: Int = ShuffleStageRecovery.PROTOCOL_VERSION

        override def resolveSourceAnchor(info: SourceRecoveryInfo): String = info.currentAnchor

        override def tryRecover(
            info: ShuffleStageRecoveryInfo): Option[RecoveredShuffleStage] = None
      }
    }
  }
}

private class RecoverableTransactionTestCatalog(
    initialState: TransactionRecoveryState,
    loseCommitResponse: Boolean)
  extends BasicInMemoryTableCatalog with SupportsTransactionRecovery {

  private var state = initialState
  private var openTransaction: Option[RecoverableTransactionTestTransaction] = None
  val recoveryInfos: ArrayBuffer[TransactionRecoveryInfo] = ArrayBuffer.empty
  var resolveCalls: Int = 0
  var beginCalls: Int = 0
  var commitCalls: Int = 0
  var abortCalls: Int = 0

  def durableState: TransactionRecoveryState = state

  def forceState(newState: TransactionRecoveryState): Unit = {
    state = newState
    openTransaction = None
  }

  override def beginTransaction(info: TransactionInfo): Transaction = {
    beginCalls += 1
    throw new AssertionError("recovery must not call ordinary beginTransaction")
  }

  override def resolveTransaction(info: TransactionRecoveryInfo): TransactionRecoveryResult = {
    resolveCalls += 1
    recoveryInfos += info
    state match {
      case TransactionRecoveryState.OPEN =>
        val txn = openTransaction.getOrElse {
          val created = new RecoverableTransactionTestTransaction(this)
          openTransaction = Some(created)
          created
        }
        TransactionRecoveryResult.open(txn)
      case TransactionRecoveryState.COMMITTED => TransactionRecoveryResult.committed()
      case TransactionRecoveryState.ABORTED => TransactionRecoveryResult.aborted()
      case TransactionRecoveryState.UNKNOWN => TransactionRecoveryResult.unknown()
    }
  }

  private def commitTransaction(): Unit = {
    commitCalls += 1
    state = TransactionRecoveryState.COMMITTED
    if (loseCommitResponse) {
      throw new RuntimeException(RecoverableTransactionTestCatalog.LostCommitResponse)
    }
  }

  private def abortTransaction(): Unit = {
    abortCalls += 1
    if (state == TransactionRecoveryState.OPEN) {
      state = TransactionRecoveryState.ABORTED
    }
  }

  private class RecoverableTransactionTestTransaction(owner: RecoverableTransactionTestCatalog)
    extends Transaction {
    override def catalog(): RecoverableTransactionTestCatalog = owner
    override def commit(): Unit = owner.commitTransaction()
    override def abort(): Unit = owner.abortTransaction()
    override def close(): Unit = ()
    override def registerScans(scans: Array[org.apache.spark.sql.connector.read.Scan]): Boolean =
      false
  }
}

private object RecoverableTransactionTestCatalog {
  val LostCommitResponse = "accepted transaction commit response was lost"
}
