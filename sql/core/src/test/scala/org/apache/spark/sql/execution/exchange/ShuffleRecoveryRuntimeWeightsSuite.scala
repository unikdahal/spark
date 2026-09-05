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

import org.apache.spark.SparkFunSuite

class ShuffleRecoveryRuntimeWeightsSuite extends SparkFunSuite {

  test("recreated shuffle stage preserves surviving winners and replaces recomputed maps") {
    val accumulator = new ShuffleRecoveryStageAccumulator(
      executionId = 1L, stageId = 2, shuffleId = 3, expectedMapTasks = 2)
    accumulator.startAttempt(currentStageId = 2, stageAttemptId = 0, attemptOrder = 1L)
    accumulator.recordSuccessfulTask(
      currentStageId = 2,
      stageAttemptId = 0,
      mapPartitionId = 0,
      shuffleWriteBytes = 10L,
      executorRunTimeMs = 11L)
    accumulator.recordSuccessfulTask(
      currentStageId = 2,
      stageAttemptId = 0,
      mapPartitionId = 1,
      shuffleWriteBytes = 30L,
      executorRunTimeMs = 31L)

    val first = accumulator.finish(
      successfulStageId = 2,
      successfulStageAttemptId = 0,
      finalAccumulatorIds = Set(7L),
      completionOrder = 1L)
    assert(first.complete)
    assert(first.shuffleWriteBytes === 40L)
    assert(first.executorRunTimeMs === 42L)

    accumulator.startAttempt(currentStageId = 9, stageAttemptId = 0, attemptOrder = 2L)
    accumulator.recordSuccessfulTask(
      currentStageId = 9,
      stageAttemptId = 0,
      mapPartitionId = 0,
      shuffleWriteBytes = 20L,
      executorRunTimeMs = 21L)

    val recomputed = accumulator.finish(
      successfulStageId = 9,
      successfulStageAttemptId = 0,
      finalAccumulatorIds = Set(8L),
      completionOrder = 2L)
    assert(recomputed.complete)
    assert(recomputed.stageId === 9)
    assert(recomputed.stageAttemptId === 0)
    assert(recomputed.successfulMapTaskWinners === 2)
    assert(recomputed.shuffleWriteBytes === 50L)
    assert(recomputed.executorRunTimeMs === 52L)
    assert(recomputed.accumulatorIds === Set(7L, 8L))
  }

  test("late success from an older stage incarnation cannot replace a newer winner") {
    val accumulator = new ShuffleRecoveryStageAccumulator(
      executionId = 1L, stageId = 2, shuffleId = 3, expectedMapTasks = 1)
    accumulator.startAttempt(currentStageId = 2, stageAttemptId = 0, attemptOrder = 1L)
    accumulator.startAttempt(currentStageId = 9, stageAttemptId = 0, attemptOrder = 2L)

    accumulator.recordSuccessfulTask(
      currentStageId = 9,
      stageAttemptId = 0,
      mapPartitionId = 0,
      shuffleWriteBytes = 20L,
      executorRunTimeMs = 21L)
    accumulator.recordSuccessfulTask(
      currentStageId = 2,
      stageAttemptId = 0,
      mapPartitionId = 0,
      shuffleWriteBytes = 999L,
      executorRunTimeMs = 999L)

    val result = accumulator.finish(
      successfulStageId = 9,
      successfulStageAttemptId = 0,
      completionOrder = 1L)
    assert(result.complete)
    assert(result.shuffleWriteBytes === 20L)
    assert(result.executorRunTimeMs === 21L)
  }
}
