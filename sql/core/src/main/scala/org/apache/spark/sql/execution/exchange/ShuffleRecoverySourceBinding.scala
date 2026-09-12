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

import org.apache.spark.shuffle.{ShuffleRecoveryMapperDecomposition, ShuffleRecoveryMapperSplit, ShuffleRecoverySourceToken}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

/**
 * In-process association between connector-certified facts and the exact planned read. Object
 * identities are checked locally and never serialized. Connectors remain responsible for the
 * truth of their certificates and the immutability of the read represented by each partition.
 */
private[sql] final class ShuffleRecoverySourceBinding private (
    private[exchange] val plan: BatchScanExec,
    private val partitions: Vector[InputPartition],
    private[exchange] val token: ShuffleRecoverySourceToken,
    private[exchange] val decomposition: ShuffleRecoveryMapperDecomposition) {

  private[exchange] def isCurrent(candidate: BatchScanExec): Boolean = {
    if ((candidate ne plan) || candidate.runtimeFilters.nonEmpty ||
        candidate.keyGroupedPartitioning.nonEmpty) {
      false
    } else {
      val current = candidate.inputPartitions
      current.size == partitions.size &&
        current.iterator.zip(partitions.iterator).forall { case (left, right) => left eq right }
    }
  }
}

private[sql] object ShuffleRecoverySourceBinding {
  import ShuffleRecoveryMissReason._

  private val MaxPartitions = 4096
  private val MaxDescriptorBytes = 64 * 1024
  private val MaxCertificateBytes = 1024 * 1024

  /**
   * The connector supplies descriptors alongside the partition objects they certify, in order.
   * Ordinary planning failures escape unchanged. Invalid certificate facts refuse certification.
   * This initial contract admits only nonempty, ungrouped, one-partition-per-mapper batch reads.
   */
  def bind(
      plan: BatchScanExec,
      protocolId: String,
      protocolVersion: Int,
      certificate: Array[Byte],
      certifiedPartitions: Vector[(InputPartition, Array[Byte])])
      : Either[ShuffleRecoveryMissReason, ShuffleRecoverySourceBinding] = {
    if (plan == null || certifiedPartitions == null) {
      return Left(SourceTokenUnavailable)
    }
    if (plan.runtimeFilters.nonEmpty) {
      return Left(RuntimeFilterPresent)
    }
    if (plan.keyGroupedPartitioning.nonEmpty) {
      return Left(UnsupportedPartitioning)
    }
    if (certifiedPartitions.isEmpty || certifiedPartitions.size > MaxPartitions) {
      return Left(InvalidPartitionCount)
    }
    // Do not catch exceptions from the source's ordinary partition planning.
    val actual = plan.inputPartitions
    if (actual.size != certifiedPartitions.size) {
      return Left(InvalidPartitionCount)
    }
    val token = try {
      ShuffleRecoverySourceToken.forProtocol(protocolId, protocolVersion, certificate)
    } catch {
      case _: IllegalArgumentException => return Left(SourceTokenUnavailable)
    }
    var bytes = token.payload.size.toLong + 8L
    var index = 0
    while (index < actual.size) {
      val entry = certifiedPartitions(index)
      if (entry == null || actual(index) == null || (entry._1 ne actual(index)) ||
          entry._2 == null || entry._2.isEmpty || entry._2.length > MaxDescriptorBytes) {
        return Left(SourceTokenUnavailable)
      }
      bytes += 16L + entry._2.length
      if (bytes > MaxCertificateBytes) {
        return Left(SourceTokenUnavailable)
      }
      index += 1
    }
    val splits = certifiedPartitions.zipWithIndex.map { case ((_, descriptor), ordinal) =>
      ShuffleRecoveryMapperSplit.copyOf(0, ordinal, protocolVersion, descriptor)
    }
    Right(new ShuffleRecoverySourceBinding(plan, actual.toVector, token,
      ShuffleRecoveryMapperDecomposition(splits.size, splits)))
  }
}
