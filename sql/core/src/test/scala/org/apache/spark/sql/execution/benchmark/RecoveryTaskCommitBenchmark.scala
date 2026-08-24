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

package org.apache.spark.sql.execution.benchmark

import java.nio.charset.StandardCharsets
import java.util.{List => JList, Optional}

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.connector.recovery.RecoveryTaskCommitStore
import org.apache.spark.sql.connector.write.{RecoveryCommitMessageCodec, WriterCommitMessage}
import org.apache.spark.sql.execution.datasources.v2.{RecoveryTaskCommitContext,
  RecoveryTaskCommitEnvelope}

/**
 * Benchmark for the durable task commit envelope used by recoverable batch writes.
 *
 * The numbers matter beyond throughput: the envelope size per partition is what a durable store
 * must hold for the whole write, and the batched load is work every replacement driver performs
 * before it can schedule anything.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/RecoveryTaskCommitBenchmark-results.txt".
 * }}}
 */
object RecoveryTaskCommitBenchmark extends BenchmarkBase {

  private val partitionCounts = Seq(1000, 10000, 100000)
  private val payloadSizes = Seq(512, 4096)

  private case class BenchmarkCommit(fileName: String) extends WriterCommitMessage

  private class PaddedCodec(payloadBytes: Int) extends RecoveryCommitMessageCodec {
    private val padding = "p" * math.max(0, payloadBytes - 64)

    override def codecId(): String = "benchmark-codec"

    override def version(): Int = 1

    override def encode(message: WriterCommitMessage): Array[Byte] =
      s"1\t${message.asInstanceOf[BenchmarkCommit].fileName}\t$padding"
        .getBytes(StandardCharsets.UTF_8)

    override def decode(version: Int, payload: Array[Byte]): WriterCommitMessage =
      BenchmarkCommit(new String(payload, StandardCharsets.UTF_8).split("\t")(1))
  }

  /** The envelope never calls the store, so a benchmark does not need a durable one. */
  private object NoopStore extends RecoveryTaskCommitStore {
    override def capabilities(): RecoveryTaskCommitStore.Capabilities =
      BenchmarkStoreCapabilities
    override def resolveWriteManifest(recoveryId: String, proposedValue: Array[Byte]): Array[Byte] =
      proposedValue
    override def load(
        recoveryId: String,
        partitionIds: Array[Int]): JList[Optional[Array[Byte]]] =
      java.util.Collections.nCopies(partitionIds.length, Optional.empty[Array[Byte]]())
    override def publish(
        recoveryId: String,
        partitionId: Int,
        taskAttemptId: Long,
        attemptNumber: Int,
        value: Array[Byte]): Array[Byte] = value
  }

  private object BenchmarkStoreCapabilities extends RecoveryTaskCommitStore.Capabilities {
    override def semanticsVersion(): Int = RecoveryTaskCommitStore.SEMANTICS_VERSION
    override def maxLoadBatchSize(): Int = 1024
    override def maxManifestBytes(): Int = 2 * 1024 * 1024
    override def maxTaskCommitBytes(): Int = 32 * 1024 * 1024
  }

  private def context(payloadBytes: Int): RecoveryTaskCommitContext =
    RecoveryTaskCommitContext(NoopStore, "benchmark-execution", new PaddedCodec(payloadBytes))

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    payloadSizes.foreach { payloadBytes =>
      val commitContext = context(payloadBytes)
      val message = BenchmarkCommit("part-00000-benchmark.data")
      val envelopeBytes =
        RecoveryTaskCommitEnvelope.encode(commitContext, 0, message, 1000L).length

      runBenchmark(s"Task commit envelope, $payloadBytes byte payload " +
        s"($envelopeBytes bytes on the wire per partition)") {
        partitionCounts.foreach { partitions =>
          val benchmark = new Benchmark(
            s"$partitions partitions", partitions.toLong, output = output)

          benchmark.addCase("encode") { _ =>
            var partition = 0
            while (partition < partitions) {
              RecoveryTaskCommitEnvelope.encode(commitContext, partition, message, 1000L)
              partition += 1
            }
          }

          val encoded = Array.tabulate(partitions) { partition =>
            RecoveryTaskCommitEnvelope.encode(commitContext, partition, message, 1000L)
          }

          benchmark.addCase("decode") { _ =>
            var partition = 0
            while (partition < partitions) {
              RecoveryTaskCommitEnvelope.decode(commitContext, partition, encoded(partition))
              partition += 1
            }
          }

          benchmark.addCase("decode a driver-side batch of 1024") { _ =>
            encoded.indices.grouped(1024).foreach { batch =>
              batch.foreach { partition =>
                RecoveryTaskCommitEnvelope.decode(commitContext, partition, encoded(partition))
              }
            }
          }

          benchmark.run()
        }
      }
    }

    runBenchmark("Write manifest") {
      val commitContext = context(512)
      val metadata = ("sink=benchmark;" + ("m" * 1024)).getBytes(StandardCharsets.UTF_8)
      val benchmark = new Benchmark("write manifest", 1000L, output = output)
      benchmark.addCase("build and digest") { _ =>
        var iteration = 0
        while (iteration < 1000) {
          RecoveryTaskCommitEnvelope.writeManifest(commitContext, 100000, metadata)
          iteration += 1
        }
      }
      benchmark.run()
    }
  }
}
