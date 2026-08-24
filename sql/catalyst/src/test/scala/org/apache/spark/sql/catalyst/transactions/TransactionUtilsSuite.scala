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

package org.apache.spark.sql.catalyst.transactions

import java.nio.charset.StandardCharsets

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.connector.catalog.{
  CatalogPlugin,
  SupportsTransactionRecovery,
  TransactionalCatalogPlugin}
import org.apache.spark.sql.connector.catalog.transactions.{
  Transaction,
  TransactionInfo,
  TransactionRecoveryInfo,
  TransactionRecoveryResult,
  TransactionRecoveryState}
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class TransactionUtilsSuite extends SparkFunSuite {
  val testCatalogName = "test_catalog"

  // --- Helpers ---------------------------------------------------------------
  private def mockCatalog(catalogName: String): CatalogPlugin = new CatalogPlugin {
    override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = ()
    override def name(): String = catalogName
  }

  private val emptyFunction = () => ()
  private class TestTransaction(
      catalogName: String,
      onCommit: () => Unit = emptyFunction,
      onAbort: () => Unit = emptyFunction,
      onClose: () => Unit = emptyFunction) extends Transaction {
    var committed = false
    var aborted = false
    var closed = false

    override def catalog(): CatalogPlugin = mockCatalog(catalogName)
    override def commit(): Unit = { committed = true; onCommit() }
    override def abort(): Unit = { aborted = true; onAbort() }
    override def close(): Unit = { closed = true; onClose() }
    override def registerScans(scans: Array[Scan]): Boolean = false
  }

  private def mockTransactionalCatalog(
      catalogName: String,
      txnCatalogName: String = null): TransactionalCatalogPlugin = {
    val resolvedTxnCatalogName = Option(txnCatalogName).getOrElse(catalogName)
    new TransactionalCatalogPlugin {
      override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = ()
      override def name(): String = catalogName
      override def beginTransaction(info: TransactionInfo): Transaction =
        new TestTransaction(resolvedTxnCatalogName)
    }
  }

  private def bytes(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)

  private def mockRecoveryCatalog(
      result: TransactionRecoveryResult,
      catalogName: String = testCatalogName,
      onBegin: () => Unit = emptyFunction): SupportsTransactionRecovery = {
    new SupportsTransactionRecovery {
      override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = ()
      override def name(): String = catalogName
      override def beginTransaction(info: TransactionInfo): Transaction = {
        onBegin()
        new TestTransaction(catalogName)
      }
      override def resolveTransaction(info: TransactionRecoveryInfo): TransactionRecoveryResult = {
        result
      }
    }
  }

  // --- Commit ----------------------------------------------------------------
  test("commit: calls commit then close") {
    val txn = new TestTransaction(testCatalogName)
    TransactionUtils.commit(txn)
    assert(txn.committed)
    assert(txn.closed)
  }

  test("commit: close is called even if commit fails") {
    val txn = new TestTransaction(
      testCatalogName, onCommit = () => throw new RuntimeException("commit failed"))
    intercept[RuntimeException] { TransactionUtils.commit(txn) }
    assert(txn.closed)
  }

  // --- Abort -----------------------------------------------------------------
  test("abort: calls abort then close") {
    val txn = new TestTransaction(testCatalogName)
    TransactionUtils.abort(txn)
    assert(txn.aborted)
    assert(txn.closed)
  }

  test("abort: close is called even if abort fails") {
    val txn = new TestTransaction(testCatalogName,
      onAbort = () => throw new RuntimeException("abort failed"))
    intercept[RuntimeException] { TransactionUtils.abort(txn) }
    assert(txn.closed)
  }

  // --- Begin Transaction -----------------------------------------------------
  test("beginTransaction: returns transaction when catalog names match") {
    val catalog = mockTransactionalCatalog(testCatalogName)
    val txn = TransactionUtils.beginTransaction(catalog)
    assert(txn.catalog().name() == testCatalogName)
  }

  test("beginTransaction: fails when transaction catalog name does not match") {
    val catalog = mockTransactionalCatalog(catalogName = testCatalogName, txnCatalogName = "other")
    val e = intercept[SparkException] {
      TransactionUtils.beginTransaction(catalog)
    }
    assert(e.getMessage.contains("other"))
    assert(e.getMessage.contains(testCatalogName))
  }

  test("beginTransaction: aborts and closes transaction on catalog name mismatch") {
    var aborted = false
    var closed = false
    val catalog = new TransactionalCatalogPlugin {
      override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = ()
      override def name(): String = testCatalogName
      override def beginTransaction(info: TransactionInfo): Transaction =
        new TestTransaction(
          "other",
          onAbort = () => { aborted = true },
          onClose = () => { closed = true })
    }
    intercept[SparkException] { TransactionUtils.beginTransaction(catalog) }
    assert(aborted)
    assert(closed)
  }

  // --- Stable Transaction Identity ------------------------------------------
  test("stableTransactionInfo: is deterministic across driver incarnations") {
    val first = TransactionUtils.stableTransactionInfo(
      "execution-1", testCatalogName, bytes("MERGE(table=t,condition=id=source.id)"))
    val second = TransactionUtils.stableTransactionInfo(
      "execution-1", testCatalogName, bytes("MERGE(table=t,condition=id=source.id)"))

    assert(first == second)
    assert(first.id.startsWith("spark-txn-v1-"))
    assert(first.id.length == "spark-txn-v1-".length + 64)
    assert(first.canonicalOperationDigest.length == 64)
  }

  test("stableTransactionInfo: binds every identity component") {
    val baseline = TransactionUtils.stableTransactionInfo(
      "execution-1", testCatalogName, bytes("UPDATE(table=t,set=value+1)"))
    val differentExecution = TransactionUtils.stableTransactionInfo(
      "execution-2", testCatalogName, bytes("UPDATE(table=t,set=value+1)"))
    val differentCatalog = TransactionUtils.stableTransactionInfo(
      "execution-1", "other_catalog", bytes("UPDATE(table=t,set=value+1)"))
    val differentOperation = TransactionUtils.stableTransactionInfo(
      "execution-1", testCatalogName, bytes("DELETE(table=t,condition=true)"))

    assert(baseline.id != differentExecution.id)
    assert(baseline.id != differentCatalog.id)
    assert(baseline.id != differentOperation.id)
    assert(baseline.canonicalOperationDigest != differentOperation.canonicalOperationDigest)
  }

  test("stableTransactionInfo: length prefixes prevent field-boundary aliases") {
    val first = TransactionUtils.stableTransactionInfo("ab", "c", bytes("d"))
    val second = TransactionUtils.stableTransactionInfo("a", "bc", bytes("d"))

    assert(first.id != second.id)
  }

  test("stableTransactionInfo: rejects incomplete identity") {
    intercept[SparkException] {
      TransactionUtils.stableTransactionInfo(" ", testCatalogName, bytes("operation"))
    }
    intercept[SparkException] {
      TransactionUtils.stableTransactionInfo("execution-1", null, bytes("operation"))
    }
    intercept[SparkException] {
      TransactionUtils.stableTransactionInfo("execution-1", testCatalogName, Array.emptyByteArray)
    }
    intercept[SparkException] {
      TransactionUtils.stableTransactionInfo("execution-1", testCatalogName, null)
    }
  }

  // --- Recover Transaction --------------------------------------------------
  test("TransactionRecoveryResult: state determines transaction presence") {
    val txn = new TestTransaction(testCatalogName)
    val open = TransactionRecoveryResult.open(txn)

    assert(open.state() == TransactionRecoveryState.OPEN)
    assert(open.transaction().orElseThrow() eq txn)
    Seq(
      TransactionRecoveryResult.committed(),
      TransactionRecoveryResult.aborted(),
      TransactionRecoveryResult.unknown()).foreach { result =>
      assert(result.state() != TransactionRecoveryState.OPEN)
      assert(result.transaction().isEmpty)
    }
    intercept[NullPointerException] {
      TransactionRecoveryResult.open(null)
    }
  }

  test("resolveTransaction: returns authoritative open and terminal states") {
    val info = TransactionUtils.stableTransactionInfo(
      "execution-1", testCatalogName, bytes("operation"))
    val txn = new TestTransaction(testCatalogName)
    val open = TransactionUtils.resolveTransaction(
      mockRecoveryCatalog(TransactionRecoveryResult.open(txn)), info)
    val committed = TransactionUtils.resolveTransaction(
      mockRecoveryCatalog(TransactionRecoveryResult.committed()), info)
    val aborted = TransactionUtils.resolveTransaction(
      mockRecoveryCatalog(TransactionRecoveryResult.aborted()), info)

    assert(open.transaction().orElseThrow() eq txn)
    assert(committed.state() == TransactionRecoveryState.COMMITTED)
    assert(aborted.state() == TransactionRecoveryState.ABORTED)
  }

  test("resolveTransaction: unknown outcome fails without beginning a transaction") {
    var beginCalls = 0
    val catalog = mockRecoveryCatalog(
      TransactionRecoveryResult.unknown(), onBegin = () => beginCalls += 1)
    val info = TransactionUtils.stableTransactionInfo(
      "execution-1", testCatalogName, bytes("operation"))

    val error = intercept[SparkException] {
      TransactionUtils.resolveTransaction(catalog, info)
    }
    assert(error.getMessage.contains("unknown durable outcome"))
    assert(beginCalls == 0)
  }

  test("resolveTransaction: connector resolution failure does not begin a transaction") {
    var beginCalls = 0
    val catalog = new SupportsTransactionRecovery {
      override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = ()
      override def name(): String = testCatalogName
      override def beginTransaction(info: TransactionInfo): Transaction = {
        beginCalls += 1
        new TestTransaction(testCatalogName)
      }
      override def resolveTransaction(
          info: TransactionRecoveryInfo): TransactionRecoveryResult = {
        throw new RuntimeException("state store unavailable")
      }
    }
    val info = TransactionUtils.stableTransactionInfo(
      "execution-1", testCatalogName, bytes("operation"))

    val error = intercept[RuntimeException] {
      TransactionUtils.resolveTransaction(catalog, info)
    }
    assert(error.getMessage == "state store unavailable")
    assert(beginCalls == 0)
  }

  test("resolveTransaction: rejects null and mismatched connector results") {
    val info = TransactionUtils.stableTransactionInfo(
      "execution-1", testCatalogName, bytes("operation"))
    val nullResultCatalog = mockRecoveryCatalog(null)
    intercept[SparkException] {
      TransactionUtils.resolveTransaction(nullResultCatalog, info)
    }

    val mismatchedCatalog = mockRecoveryCatalog(
      TransactionRecoveryResult.committed(), catalogName = "other_catalog")
    intercept[SparkException] {
      TransactionUtils.resolveTransaction(mismatchedCatalog, info)
    }
  }

  test("resolveTransaction: closes an open transaction from a mismatched catalog") {
    val info = TransactionUtils.stableTransactionInfo(
      "execution-1", testCatalogName, bytes("operation"))
    val txn = new TestTransaction("other_catalog")

    intercept[SparkException] {
      TransactionUtils.resolveTransaction(
        mockRecoveryCatalog(TransactionRecoveryResult.open(txn)), info)
    }
    assert(txn.closed)
    assert(!txn.aborted)
  }

  test("resolveTransaction: closes an open transaction with a null catalog") {
    val info = TransactionUtils.stableTransactionInfo(
      "execution-1", testCatalogName, bytes("operation"))
    val txn = new TestTransaction(testCatalogName) {
      override def catalog(): CatalogPlugin = null
    }

    intercept[SparkException] {
      TransactionUtils.resolveTransaction(
        mockRecoveryCatalog(TransactionRecoveryResult.open(txn)), info)
    }
    assert(txn.closed)
    assert(!txn.aborted)
  }

  test("resolveTransaction: closes an open transaction with a null catalog name") {
    val info = TransactionUtils.stableTransactionInfo(
      "execution-1", testCatalogName, bytes("operation"))
    val txn = new TestTransaction(null)

    intercept[SparkException] {
      TransactionUtils.resolveTransaction(
        mockRecoveryCatalog(TransactionRecoveryResult.open(txn)), info)
    }
    assert(txn.closed)
    assert(!txn.aborted)
  }
}
