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

package org.apache.spark.shuffle.celeborn

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.{FetchFailed, SPARK_REVISION}
import org.apache.spark.scheduler._

import org.apache.celeborn.client.ShuffleRecoveryDescriptorEvidence
import org.apache.spark.shuffle._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.exchange.{ShuffleExchangeExec, ShuffleRecoveryIcebergColdSource}

/** Each invocation is one independent driver. The shell runner owns durable services and files. */
object ShuffleRecoveryCelebornColdProcess {
  private class TargetTasks(rddId: Int) extends SparkListener {
    private val stages = ConcurrentHashMap.newKeySet[Integer]()
    val count = new AtomicLong()
    val fetchFailures = new AtomicLong()
    val remoteBytesRead = new AtomicLong()
    override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
      if (event.reason.isInstanceOf[FetchFailed]) fetchFailures.incrementAndGet()
      if (event.taskMetrics != null) {
        remoteBytesRead.addAndGet(event.taskMetrics.shuffleReadMetrics.remoteBytesRead)
      }
    }
    override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
      if (event.stageInfo.rddInfos.exists(_.id == rddId)) stages.add(event.stageInfo.stageId)
    }
    override def onTaskStart(event: SparkListenerTaskStart): Unit = {
      if (stages.contains(event.stageId)) count.incrementAndGet()
    }
  }

  def main(args: Array[String]): Unit = {
    require(SPARK_REVISION == sys.env("SPARK_RECOVERY_TESTED_COMMIT"),
      "runtime build revision must match the tested candidate")
    require(args.length == 5, "role manifestRoot evidence group control")
    val Array(role, root, evidence, group, control) = args
    require(Set("baseline", "producer", "replacement").contains(role))
    require(Set("none", "source-token", "manifest-missing", "source-snapshot",
      "producer-filter", "artifact-loss", "concurrent", "lease-expiry", "owner-restart")
      .contains(control))
    require(role == "replacement" || control == "none")
    val source = new ShuffleRecoveryIcebergColdSource
    val builder = SparkSession.builder().master("local[2]")
      .appName("celeborn-cold-" + role)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.shuffle.useOldFetchProtocol", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
    source.sessionOptions.foreach { case (key, value) => builder.config(key, value) }
    if (role != "baseline") {
      builder.config("spark.shuffle.manager", classOf[ShuffleRecoveryCelebornManager].getName)
        .config("spark.celeborn.retainedShuffle.endpointFile",
          sys.env("CELEBORN_RETAINED_ENDPOINT_FILE"))
        .config("spark.celeborn.client.spark.shuffle.fallback.policy", "NEVER")
        .config("spark.celeborn.client.spark.stageRerun.enabled", "false")
        .config("spark.celeborn.client.spark.fetch.cleanFailedShuffle", "false")
        .config("spark.celeborn.client.adaptive.optimizeSkewedPartitionRead.enabled", "false")
        .config("spark.celeborn.client.spark.shuffle.getReducerFileGroup.broadcast.enabled",
          "false")
        .config("spark.celeborn.columnarShuffle.enabled", "false")
        .config("spark.io.encryption.enabled", "false")
        .config("spark.celeborn.client.shuffle.compression.codec", "lz4")
        .config("spark.celeborn.client.push.replicate.enabled", "false")
    }
    val spark = builder.getOrCreate()
    var publication: ShuffleRecoveryCelebornPublicationSession = null
    var adoption: ShuffleRecoveryCelebornAdoptionSession = null
    try {
      val sc = spark.sparkContext
      val format = ShuffleRecoveryCelebornPublicationProvider.readFormatId(
        SparkUtils.fromSparkConf(sc.getConf))
      source.configureProviderReadFormat(format)
      val query = source.buildQuery(spark, 32L, 4, 4, 0)
      val exchanges = query.queryExecution.executedPlan.collect {
        case exchange: ShuffleExchangeExec => exchange
      }
      require(exchanges.size == 1)
      val exchange = exchanges.head
      val dependency = exchange.shuffleDependency
      val target = ShuffleRecoveryAdoptionTarget(
        ShuffleRecoveryMaterializationId(exchange.id, 1L), exchange.shuffleId,
        dependency.rdd.id.toLong, exchange.numMappers, exchange.numPartitions)
      val inputs = source.identityInputs(exchange,
        if (control == "source-token") "different-source" else "fixture", format)
      val tasks = new TargetTasks(dependency.rdd.id)
      sc.addSparkListener(tasks)
      val preparationStarted = System.nanoTime()
      var offered = false
      var missReason = ""
      if (role == "producer") {
        val context = ShuffleRecoveryNativePublicationContext(group, 1L,
          UUID.randomUUID().toString, exchange.shuffleId,
          inputs.identityFor(target).asInstanceOf[ShuffleRecoveryCanonicalManifestIdentity])
        publication = ShuffleRecoveryCelebornPublication.attach(sc, context, Paths.get(root),
          3600000L)
      } else if (role == "replacement") {
        val manifestRoot = if (control == "manifest-missing") {
          Files.createTempDirectory(Paths.get(root), "empty-")
        } else Paths.get(root)
        ShuffleRecoveryCelebornPreparation.prepare(sc,
          ShuffleRecoveryPreparationRequest(group, 2L, target, inputs),
          manifestRoot, if (control == "lease-expiry") 3000L else 60000L) match {
          case Right(session) => adoption = session; offered = true
          case Left(reason) => missReason = reason
        }
      }
      val preparationNanos = System.nanoTime() - preparationStarted
      val executionStarted = System.nanoTime()
      sc.submitMapStage(dependency).get()
      if (publication != null) {
        val manifest = publication.finish()
        val (application, shuffle) = ShuffleRecoveryDescriptorEvidence.namespace(
          manifest.nativeDescriptor.get.toArray)
        Files.write(Paths.get(root, "producer-namespace"),
          s"$application\n$shuffle\n".getBytes(UTF_8), StandardOpenOption.CREATE_NEW)
      }
      val adoptedBeforeRead = adoption != null && adoption.isAdopted
      if (control == "concurrent") {
        require(adoptedBeforeRead, "concurrent reader must hold an adopted claim")
        Files.write(Paths.get(root, Paths.get(evidence).getFileName.toString + ".ready"),
          Array[Byte](1), StandardOpenOption.CREATE_NEW)
        val peers = Seq("concurrent-a", "concurrent-b").map { name =>
          Paths.get(root, name + ".properties.ready")
        }
        val deadline = System.nanoTime() + 120000000000L
        while (!peers.forall(Files.exists(_)) && System.nanoTime() < deadline) {
          Thread.sleep(100L)
        }
        require(peers.forall(Files.exists(_)), "both readers must hold claims concurrently")
      }
      if (control == "lease-expiry") {
        require(adoptedBeforeRead, "expiry fault must follow adoption")
        val controls = Paths.get(sys.env("CELEBORN_PROOF_CONTROL_ROOT"))
        def signalAndWait(request: String, acknowledgement: String): Unit = {
          Files.write(controls.resolve(request), Array[Byte](1), StandardOpenOption.CREATE_NEW)
          val deadline = System.nanoTime() + 30000000000L
          while (!Files.exists(controls.resolve(acknowledgement)) &&
              System.nanoTime() < deadline) {
            Thread.sleep(100L)
          }
          require(Files.exists(controls.resolve(acknowledgement)), "owner fault timed out")
        }
        signalAndWait("pause-owner", "owner-paused")
        // The service cannot answer renewals. Local lease expiry must remain permanent even
        // if an in-flight renewal returns successfully after the same owner resumes.
        Thread.sleep(5000L)
        signalAndWait("resume-owner", "owner-resumed")
      }
      if (control == "artifact-loss") {
        require(adoptedBeforeRead && tasks.count.get() == 0L,
          "fault must happen after adoption and before ordinary maps run")
        Files.write(Paths.get(root, "fault-ready"), Array[Byte](1),
          StandardOpenOption.CREATE_NEW)
        val deadline = System.nanoTime() + 120000000000L
        while (!Files.exists(Paths.get(root, "fault-applied")) &&
            System.nanoTime() < deadline) {
          Thread.sleep(100L)
        }
        require(Files.exists(Paths.get(root, "fault-applied")), "artifact fault timed out")
      }
      val rows = query.collect().map { row =>
        require(row.length == 3 && !row.anyNull)
        require(row.getLong(0) == row.getLong(1) && row.getString(2).isEmpty)
        row.getLong(0)
      }.sorted
      require(rows.toVector == (0L until 32L).toVector, "result differs from exact fixture")
      sc.listenerBus.waitUntilEmpty(30000L)
      val adoptedAfterRead = adoption != null && adoption.isAdopted
      // SQL execution cleanup may release the binding before collect returns. Successful reuse
      // is established by the installed binding and actual tasks/bytes, not post-query retention.
      val reused = adoptedBeforeRead && tasks.count.get() == 0L &&
        tasks.fetchFailures.get() == 0L && tasks.remoteBytesRead.get() > 0L
      val digest = MessageDigest.getInstance("SHA-256")
        .digest(rows.mkString("\n").getBytes(UTF_8)).map(b => f"${b & 0xff}%02x").mkString
      val process = ProcessHandle.current()
      val record = Seq("role" -> role, "control" -> control,
        "pid" -> process.pid().toString,
        "started" -> process.info().startInstant().get().toString,
        "testedCommit" -> sys.env("SPARK_RECOVERY_TESTED_COMMIT"),
        "rowCount" -> rows.length.toString, "resultDigest" -> digest,
        "mapTaskCount" -> tasks.count.get().toString,
        "fetchFailures" -> tasks.fetchFailures.get().toString,
        "remoteBytesRead" -> tasks.remoteBytesRead.get().toString,
        "preparationNanos" -> preparationNanos.toString,
        "executionNanos" -> (System.nanoTime() - executionStarted).toString,
        "adoptedBeforeRead" -> adoptedBeforeRead.toString,
        "bindingAfterRead" -> adoptedAfterRead.toString,
        "offered" -> offered.toString, "adopted" -> reused.toString,
        "missReason" -> missReason)
      Files.write(Paths.get(evidence), record.map { case (k, v) => s"$k=$v" }
        .mkString("", "\n", "\n").getBytes(UTF_8), StandardOpenOption.CREATE_NEW)
      if (role == "replacement" && Set("none", "concurrent").contains(control)) {
        require(offered && reused,
          s"native recovery gate failed: offered=$offered, before=$adoptedBeforeRead, " +
            s"after=$adoptedAfterRead, maps=${tasks.count.get()}, " +
            s"bytes=${tasks.remoteBytesRead.get()}")
      } else if (Set("artifact-loss", "lease-expiry").contains(control)) {
        require(offered && adoptedBeforeRead && !adoptedAfterRead &&
          tasks.count.get() > 0L && tasks.fetchFailures.get() > 0L,
          "unavailable native claim must cause fetch failure and whole-shuffle recomputation")
      } else {
        require(!adoptedBeforeRead && !adoptedAfterRead && tasks.count.get() > 0L,
          "baseline, producer and negative controls must execute target map tasks")
        if (role == "replacement") require(!offered, "negative control unexpectedly matched")
      }
    } finally {
      try {
        if (adoption != null) adoption.close()
        if (publication != null) publication.close()
      } finally spark.stop()
    }
  }
}
