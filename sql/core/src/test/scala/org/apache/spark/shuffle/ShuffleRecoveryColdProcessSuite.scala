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

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption, StandardWatchEventKinds}
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite

/** Parent harness for the independent-JVM cold-driver shuffle recovery proof. */
class ShuffleRecoveryColdProcessSuite extends SparkFunSuite {
  import ShuffleRecoveryColdProcessData._

  private val ChildTimeoutSeconds = 180L
  private val AbruptMarkerTimeoutSeconds = 120L
  private val TestedCommit = sys.env.getOrElse("GITHUB_SHA", "local")

  test("cold independent JVMs adopt exact provider blocks and negative controls recompute") {
    val evidenceBase = sys.env.get("SPARK_SHUFFLE_RECOVERY_COLD_PROCESS_EVIDENCE_DIR")
      .map(Paths.get(_))
      .getOrElse(Files.createTempDirectory("shuffle-recovery-cold-evidence-"))
    Files.createDirectories(evidenceBase)
    val evidenceRun = Files.createTempDirectory(evidenceBase, "run-")
    val workRoot = Files.createTempDirectory("shuffle-recovery-cold-work-")
    try {
      scenarios.foreach { scenarioValue =>
        runHappy(
          workRoot.resolve(s"happy-${scenarioValue.name}"),
          evidenceRun,
          scenarioValue,
          "happy")
      }
      runHappy(workRoot.resolve("repeat-sparse"), evidenceRun, scenario("sparse"), "repeat")
      runAbrupt(workRoot.resolve("abrupt-sparse"), evidenceRun, scenario("sparse"))
      runNegativeControls(workRoot.resolve("negative-controls"), evidenceRun)
      writeAggregate(evidenceRun, evidenceBase.resolve("cold-process-evidence.tsv"))
    } finally {
      deleteRecursively(workRoot)
    }
  }

  private def runHappy(
      root: Path,
      evidenceRun: Path,
      scenarioValue: Scenario,
      prefix: String): Unit = {
    Files.createDirectories(root)
    val group = s"cold-$prefix-${scenarioValue.name}"
    val baseline = evidenceRun.resolve(s"$prefix-${scenarioValue.name}-baseline.tsv")
    val producer = evidenceRun.resolve(s"$prefix-${scenarioValue.name}-producer.tsv")
    val replacement = evidenceRun.resolve(s"$prefix-${scenarioValue.name}-replacement.tsv")
    try {
      runChild("baseline", root, scenarioValue, group, baseline)
      runChild("producer", root, scenarioValue, group, producer)
      runChild(
        "replacement",
        root,
        scenarioValue,
        group,
        replacement,
        baseline = Some(baseline),
        producer = Some(producer))
      val replacementEvidence = Evidence.read(replacement)
      assert(replacementEvidence("adopted") == "true")
      assert(replacementEvidence("mapTaskCount") == "0")
      assert(replacementEvidence("providerEmptyReads") == "0")
    } finally {
      deleteRecursively(root)
    }
  }

  private def runAbrupt(root: Path, evidenceRun: Path, scenarioValue: Scenario): Unit = {
    Files.createDirectories(root)
    val group = s"cold-abrupt-${scenarioValue.name}"
    val baseline = evidenceRun.resolve(s"abrupt-${scenarioValue.name}-baseline.tsv")
    val producer = evidenceRun.resolve(s"abrupt-${scenarioValue.name}-producer.tsv")
    val replacement = evidenceRun.resolve(s"abrupt-${scenarioValue.name}-replacement.tsv")
    val marker = root.resolve("manifest-committed.marker")
    try {
      runChild("baseline", root, scenarioValue, group, baseline)
      runAbruptProducer(root, scenarioValue, group, producer, marker)
      assert(Files.isRegularFile(marker))
      runChild(
        "replacement",
        root,
        scenarioValue,
        group,
        replacement,
        baseline = Some(baseline),
        producer = Some(producer))
      assert(Evidence.read(replacement)("adopted") == "true")
    } finally {
      deleteRecursively(root)
    }
  }

  private def runNegativeControls(root: Path, evidenceRun: Path): Unit = {
    Files.createDirectories(root)
    val scenarioValue = negativeScenario
    val group = "cold-negative-controls"
    val baseline = evidenceRun.resolve("negative-baseline.tsv")
    val producer = evidenceRun.resolve("negative-producer.tsv")
    try {
      runChild("baseline", root, scenarioValue, group, baseline)
      runChild("producer", root, scenarioValue, group, producer)
      controls.foreach { control =>
        val evidence = evidenceRun.resolve(s"negative-$control.tsv")
        runChild(
          "replacement",
          root,
          scenarioValue,
          group,
          evidence,
          baseline = Some(baseline),
          producer = Some(producer),
          control = control)
        val result = Evidence.read(evidence)
        assert(result("adopted") == "false", control)
        assert(result("mapTaskCount").toLong > 0L, control)
        assert(result("resultDigest") == Evidence.read(baseline)("resultDigest"), control)
      }
    } finally {
      deleteRecursively(root)
    }
  }

  private def runChild(
      mode: String,
      root: Path,
      scenarioValue: Scenario,
      group: String,
      evidence: Path,
      baseline: Option[Path] = None,
      producer: Option[Path] = None,
      control: String = "none"): Unit = {
    val command = childCommand(
      mode,
      root,
      scenarioValue,
      group,
      evidence,
      baseline,
      producer,
      control)
    val log = evidence.resolveSibling(evidence.getFileName.toString + ".log")
    val process = startProcess(command, root)
    val drainer = drainProcess(process, log)
    val finished = process.waitFor(ChildTimeoutSeconds, TimeUnit.SECONDS)
    if (!finished) {
      terminate(process)
    }
    drainer.join(TimeUnit.SECONDS.toMillis(30))
    assert(finished, s"child timed out: ${command.mkString(" ")}; log=$log")
    assert(process.exitValue() == 0, s"child failed with ${process.exitValue()}; log=$log")
    assert(Files.isRegularFile(evidence), s"child produced no evidence: $evidence")
  }

  private def runAbruptProducer(
      root: Path,
      scenarioValue: Scenario,
      group: String,
      evidence: Path,
      marker: Path): Unit = {
    val command = childCommand(
      "producer-hold",
      root,
      scenarioValue,
      group,
      evidence,
      None,
      None,
      "none") :+ s"marker=$marker"
    val log = evidence.resolveSibling(evidence.getFileName.toString + ".log")
    val watcher = marker.getParent.getFileSystem.newWatchService()
    marker.getParent.register(watcher, StandardWatchEventKinds.ENTRY_CREATE)
    val process = startProcess(command, root)
    val drainer = drainProcess(process, log)
    try {
      val committed = waitForMarker(watcher, marker)
      assert(committed, s"producer never reached manifest commit; log=$log")
      process.destroyForcibly()
      assert(process.waitFor(30, TimeUnit.SECONDS), "abrupt producer did not terminate")
      assert(Files.isRegularFile(evidence), "abrupt producer did not flush pre-kill evidence")
    } finally {
      if (process.isAlive) {
        terminate(process)
      }
      watcher.close()
      drainer.join(TimeUnit.SECONDS.toMillis(30))
    }
  }

  private def childCommand(
      mode: String,
      root: Path,
      scenarioValue: Scenario,
      group: String,
      evidence: Path,
      baseline: Option[Path],
      producer: Option[Path],
      control: String): Vector[String] = {
    val java = Paths.get(System.getProperty("java.home"), "bin", "java").toString
    val temporary = root.resolve(s"jvm-tmp-$mode-$control")
    Files.createDirectories(temporary)
    Vector(
      java,
      s"-Djava.io.tmpdir=$temporary",
      "-cp",
      System.getProperty("java.class.path"),
      "org.apache.spark.shuffle.ShuffleRecoveryColdProcessProcess",
      mode,
      s"root=$root",
      s"scenario=${scenarioValue.name}",
      s"group=$group",
      s"testedCommit=$TestedCommit",
      s"evidence=$evidence") ++
      baseline.map(path => s"baseline=$path") ++
      producer.map(path => s"producer=$path") ++
      (if (mode == "replacement") Vector(s"control=$control") else Vector.empty)
  }

  private def startProcess(command: Vector[String], root: Path): Process = {
    val builder = new ProcessBuilder(command: _*)
    builder.redirectErrorStream(true)
    builder.environment().put("SPARK_LOCAL_HOSTNAME", "localhost")
    builder.directory(root.toFile)
    builder.start()
  }

  private def drainProcess(process: Process, log: Path): Thread = {
    createParentDirectories(log)
    val thread = new Thread(() => {
      val reader = new BufferedReader(new InputStreamReader(
        process.getInputStream,
        StandardCharsets.UTF_8))
      val writer = Files.newBufferedWriter(
        log,
        StandardCharsets.UTF_8,
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE)
      try {
        var line = reader.readLine()
        while (line != null) {
          writer.write(line)
          writer.newLine()
          line = reader.readLine()
        }
      } finally {
        try {
          reader.close()
        } finally {
          writer.close()
        }
      }
    }, s"cold-process-drain-${process.pid()}")
    thread.setDaemon(true)
    thread.start()
    thread
  }

  private def waitForMarker(
      watcher: java.nio.file.WatchService,
      marker: Path): Boolean = {
    if (Files.isRegularFile(marker)) {
      return true
    }
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AbruptMarkerTimeoutSeconds)
    while (System.nanoTime() < deadline) {
      val remaining = deadline - System.nanoTime()
      val key = watcher.poll(math.max(1L, remaining), TimeUnit.NANOSECONDS)
      if (key != null) {
        key.pollEvents()
        key.reset()
        if (Files.isRegularFile(marker)) {
          return true
        }
      }
    }
    false
  }

  private def terminate(process: Process): Unit = {
    process.destroy()
    if (!process.waitFor(10, TimeUnit.SECONDS)) {
      process.destroyForcibly()
      process.waitFor(10, TimeUnit.SECONDS)
    }
  }

  private def writeAggregate(evidenceRun: Path, target: Path): Unit = {
    val stream = Files.list(evidenceRun)
    val evidenceFiles = try {
      stream.iterator().asScala
        .filter(path => path.getFileName.toString.endsWith(".tsv"))
        .toVector
        .sortBy(_.getFileName.toString)
    } finally {
      stream.close()
    }
    assert(evidenceFiles.nonEmpty)
    val rows = evidenceFiles.map(Evidence.read)
    val text = evidenceHeader.mkString("\t") + "\n" +
      rows.map(_.values.mkString("\t")).mkString("\n") + "\n"
    Files.write(
      target,
      text.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.WRITE)
  }

  private def createParentDirectories(path: Path): Unit = {
    val parent = path.getParent
    if (parent != null) {
      Files.createDirectories(parent)
    }
  }

  private def deleteRecursively(path: Path): Unit = {
    if (!Files.exists(path)) {
      return
    }
    val stream = Files.walk(path)
    try {
      stream.iterator().asScala.toVector
        .sortBy(_.getNameCount)
        .reverseIterator
        .foreach(Files.deleteIfExists(_))
    } finally {
      stream.close()
    }
  }
}
