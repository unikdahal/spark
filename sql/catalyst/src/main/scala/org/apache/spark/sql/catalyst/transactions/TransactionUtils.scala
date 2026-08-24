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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.{HexFormat, UUID}

import org.apache.spark.SparkException
import org.apache.spark.sql.connector.catalog.{
  SupportsTransactionRecovery,
  TransactionalCatalogPlugin}
import org.apache.spark.sql.connector.catalog.transactions.{
  Transaction,
  TransactionInfoImpl,
  TransactionRecoveryInfo,
  TransactionRecoveryInfoImpl,
  TransactionRecoveryResult,
  TransactionRecoveryState}
import org.apache.spark.util.Utils

object TransactionUtils {
  private val TransactionIdDomain = "spark-transaction-recovery-v1"

  def commit(txn: Transaction): Unit = {
    Utils.tryWithSafeFinally {
      txn.commit()
    } {
      txn.close()
    }
  }

  def abort(txn: Transaction): Unit = {
    Utils.tryWithSafeFinally {
      txn.abort()
    } {
      txn.close()
    }
  }

  def beginTransaction(catalog: TransactionalCatalogPlugin): Transaction = {
    val info = TransactionInfoImpl(id = UUID.randomUUID.toString)
    val txn = catalog.beginTransaction(info)
    if (txn.catalog.name != catalog.name) {
      abort(txn)
      throw SparkException.internalError(
        s"Transaction catalog name (${txn.catalog.name}) " +
          s"must match original catalog name (${catalog.name}).")
    }
    txn
  }

  /**
   * Derives a transaction identity from stable recovery and logical-operation identities.
   * Length-prefixing prevents distinct field boundaries from producing the same digest input.
   */
  private[sql] def stableTransactionInfo(
      recoveryExecutionId: String,
      catalogIdentity: String,
      canonicalLogicalOperation: Array[Byte]): TransactionRecoveryInfoImpl = {
    requireNonBlank(recoveryExecutionId, "recovery execution ID")
    requireNonBlank(catalogIdentity, "catalog identity")
    if (canonicalLogicalOperation == null || canonicalLogicalOperation.isEmpty) {
      throw SparkException.internalError("Canonical logical operation must not be empty")
    }

    val operationDigest = sha256(canonicalLogicalOperation)
    val transactionDigest = MessageDigest.getInstance("SHA-256")
    addField(transactionDigest, TransactionIdDomain.getBytes(StandardCharsets.UTF_8))
    addField(transactionDigest, recoveryExecutionId.getBytes(StandardCharsets.UTF_8))
    addField(transactionDigest, catalogIdentity.getBytes(StandardCharsets.UTF_8))
    addField(transactionDigest, operationDigest)
    TransactionRecoveryInfoImpl(
      id = s"spark-txn-v1-${hex(transactionDigest.digest())}",
      recoveryExecutionId = recoveryExecutionId,
      catalogIdentity = catalogIdentity,
      canonicalOperationDigest = hex(operationDigest))
  }

  /** Resolves a transaction and validates every connector-controlled result fail-closed. */
  private[sql] def resolveTransaction(
      catalog: SupportsTransactionRecovery,
      info: TransactionRecoveryInfo): TransactionRecoveryResult = {
    if (catalog == null || info == null) {
      throw SparkException.internalError("Transaction recovery catalog and info must be non-null")
    }
    if (catalog.name() != info.catalogIdentity()) {
      throw SparkException.internalError(
        s"Transaction catalog name (${catalog.name()}) must match the stable catalog identity " +
          s"(${info.catalogIdentity()}).")
    }
    val result = Option(catalog.resolveTransaction(info)).getOrElse {
      throw SparkException.internalError("Transaction recovery returned a null result")
    }
    if (result.state() == TransactionRecoveryState.UNKNOWN) {
      throw SparkException.internalError(
        s"Transaction ${info.id} has an unknown durable outcome; recovery cannot continue")
    }
    result.transaction().ifPresent { txn =>
      val resolvedCatalog = Option(txn.catalog()).getOrElse {
        txn.close()
        throw SparkException.internalError(
          s"Open transaction ${info.id} returned a null catalog")
      }
      val resolvedCatalogName = Option(resolvedCatalog.name()).getOrElse {
        txn.close()
        throw SparkException.internalError(
          s"Open transaction ${info.id} returned a catalog with a null name")
      }
      if (resolvedCatalogName != catalog.name()) {
        txn.close()
        throw SparkException.internalError(
          s"Transaction catalog name ($resolvedCatalogName) " +
            s"must match original catalog name (${catalog.name()}).")
      }
    }
    result
  }

  private def addField(digest: MessageDigest, bytes: Array[Byte]): Unit = {
    digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array())
    digest.update(bytes)
  }

  private def sha256(bytes: Array[Byte]): Array[Byte] = {
    MessageDigest.getInstance("SHA-256").digest(bytes)
  }

  private def hex(bytes: Array[Byte]): String = HexFormat.of().formatHex(bytes)

  private def requireNonBlank(value: String, label: String): Unit = {
    if (value == null || value.trim.isEmpty) {
      throw SparkException.internalError(s"$label must not be blank")
    }
  }
}
