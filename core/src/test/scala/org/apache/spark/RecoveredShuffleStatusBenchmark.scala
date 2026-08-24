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

package org.apache.spark

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.scheduler.RecoveredMapStatus

/**
 * Scale benchmark for the compact scheduler representation of an externally recovered shuffle.
 *
 * Every case uses the same mapper and reducer count. The previous eager representation attempted
 * to allocate their Cartesian product; the 1M case would therefore require 10^12 block-size
 * entries before object overhead. This benchmark registers only O(mappers + reducers) state and
 * samples one reducer lookup per mapper.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results are written to "benchmarks/RecoveredShuffleStatusBenchmark-results.txt".
 * }}}
 */
object RecoveredShuffleStatusBenchmark extends BenchmarkBase {

  private val scale = Seq(10000, 100000, 1000000)

  @volatile private var resultSink = 0L

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val conf = new SparkConf(false)
    val tracker = new MapOutputTrackerMaster(conf, new BroadcastManager(true, conf), isLocal = true)
    try {
      runBenchmark("Recovered shuffle compact registration") {
        scale.foreach { entries =>
          val benchmark = new Benchmark(
            s"$entries mappers x $entries reducers", entries.toLong, output = output)
          benchmark.addTimerCase("register", numIters = 1) { timer =>
            // Benchmark's time-based warmup repeatedly invokes a case. Keep warmup bounded while
            // retaining the exact 10K/100K/1M allocation in the recorded iteration.
            val measuredEntries = if (timer.iteration < 0) math.min(entries, 10000) else entries
            val reducerTotals = Array.tabulate(measuredEntries)(i => i.toLong + 1L)
            timer.startTiming()
            tracker.registerRecoveredShuffle(entries, measuredEntries, reducerTotals)
            timer.stopTiming()
            resultSink = tracker.getNumAvailableOutputs(entries)
            tracker.unregisterShuffle(entries)
          }
          benchmark.run()
        }
      }

      runBenchmark("Recovered shuffle compact point lookup") {
        scale.foreach { entries =>
          val reducerTotals = Array.tabulate(entries)(i => i.toLong + entries.toLong)
          tracker.registerRecoveredShuffle(entries, entries, reducerTotals)
          val statuses = tracker.shuffleStatuses(entries).mapStatuses.map {
            case status: RecoveredMapStatus => status
            case other => throw new IllegalStateException(
              s"Expected compact recovered status, found ${other.getClass.getName}")
          }
          require(statuses.length == entries)
          require(statuses.iterator.drop(1).forall(_.sharesRecoveryMetadata(statuses.head)))

          val benchmark = new Benchmark(
            s"$entries mappers sharing $entries reducer totals",
            entries.toLong,
            output = output)
          benchmark.addCase("one reducer lookup per mapper", numIters = 3) { _ =>
            var checksum = 0L
            var mapIndex = 0
            while (mapIndex < entries) {
              checksum += statuses(mapIndex).getSizeForBlock(mapIndex)
              mapIndex += 1
            }
            resultSink = checksum
          }
          benchmark.run()
          tracker.unregisterShuffle(entries)
        }
      }
    } finally {
      tracker.stop()
    }
  }
}
