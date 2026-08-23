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

package org.apache.spark.shuffle

import org.apache.spark.MapOutputStatistics

/** Metadata for shuffle output which an external service has made readable to this driver. */
private[spark] case class RecoveredShuffleOutput(
    bytesByPartitionId: Array[Long],
    dataSize: Long,
    rowCount: Option[Long])

/**
 * Driver-side recovery callbacks attached to one [[org.apache.spark.ShuffleDependency]].
 *
 * The implementation owns the durable execution and stage identity. Local shuffle, stage and RDD
 * ids are deliberately not identity inputs: they are allocated again by a replacement driver.
 * `None` is authoritative permission to compute the stage. Any unavailable, indeterminate or
 * corrupt lookup must throw so that Spark fails closed instead of mixing generations.
 *
 * Before returning `Some`, the implementation must atomically adopt the physical shuffle under
 * `shuffleId` in the active shuffle manager. The DAGScheduler then installs scheduler metadata
 * and skips every map task. Implementations must durably record the stage intent before returning
 * `None`, so a driver failure cannot leave committed output which a later lookup mistakes for an
 * unrelated generation.
 */
private[spark] trait ShuffleStageRecoveryHandler {

  def tryRecover(
      shuffleId: Int,
      numMappers: Int,
      numPartitions: Int): Option[RecoveredShuffleOutput]

  /** Called synchronously after all real map outputs are registered and readable. */
  def onStageCompleted(shuffleId: Int, statistics: MapOutputStatistics): Unit

  /** Remove only driver-local state installed by a failed adoption attempt. */
  def abortRecovery(shuffleId: Int, cause: Throwable): Unit = {}
}
