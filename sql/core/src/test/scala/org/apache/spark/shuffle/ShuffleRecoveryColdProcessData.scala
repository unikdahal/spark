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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._

private[shuffle] object ShuffleRecoveryColdProcessData {
  final case class Scenario(
      name: String,
      mappers: Int,
      reducers: Int,
      rows: Long,
      payloadBytes: Int,
      shape: String)

  val scenarios: Vector[Scenario] = Vector(
    Scenario("sparse", 4, 64, 32L, 0, "sparse"),
    Scenario("empty", 1, 8, 0L, 0, "empty"),
    Scenario("adjacent", 1, 16, 2L, 0, "sparse"),
    Scenario("skewed", 4, 16, 2000L, 64, "skewed"),
    Scenario("small", 1, 4, 1L, 0, "sparse"),
    Scenario("large", 2, 4, 24L, 384 * 1024, "skewed"),
    Scenario("wide", 3, 257, 600L, 16, "sparse"))

  val negativeScenario: Scenario = Scenario("negative", 3, 32, 192L, 16, "sparse")

  val controls: Vector[String] = Vector(
    "disabled",
    "group-diff",
    "generation-not-later",
    "source-token",
    "provider-compat",
    "manifest-absent",
    "digest-collision",
    "artifact-missing",
    "claim-unavailable",
    "reservation-stale")

  val evidenceHeader: Vector[String] = Vector(
    "role",
    "scenario",
    "control",
    "frozenBaseline",
    "testedCommit",
    "sparkCompatibility",
    "providerCompatibility",
    "aqeEnabled",
    "group",
    "generation",
    "publishingGeneration",
    "originShuffleId",
    "currentShuffleId",
    "targetStageIds",
    "mapTaskCount",
    "adopted",
    "incarnation",
    "providerBlockReads",
    "providerNonEmptyReads",
    "providerEmptyReads",
    "providerBytesRead",
    "emptyBlocks",
    "nonEmptyBlocks",
    "physicalBytes",
    "maxBlockBytes",
    "rowCount",
    "resultDigest",
    "elapsedMillis",
    "note")

  final case class Evidence(values: Vector[String]) {
    require(values.size == evidenceHeader.size)

    def apply(key: String): String = {
      val index = evidenceHeader.indexOf(key)
      require(index >= 0, s"unknown evidence field: $key")
      values(index)
    }

    def render: String = {
      evidenceHeader.mkString("\t") + "\n" + values.mkString("\t") + "\n"
    }
  }

  object Evidence {
    def read(path: Path): Evidence = {
      val lines = Files.readAllLines(path, StandardCharsets.UTF_8).asScala.toVector
      require(lines.size == 2, s"invalid evidence line count in $path")
      require(lines.head.split("\t", -1).toVector == evidenceHeader)
      Evidence(lines(1).split("\t", -1).toVector)
    }
  }

  def scenario(name: String): Scenario = {
    (scenarios :+ negativeScenario).find(_.name == name).getOrElse {
      throw new IllegalArgumentException(s"unknown cold-process scenario: $name")
    }
  }
}
