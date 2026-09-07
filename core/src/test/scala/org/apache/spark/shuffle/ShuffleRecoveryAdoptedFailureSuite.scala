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

import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite

class ShuffleRecoveryAdoptedFailureSuite extends SparkFunSuite {

  test("only authoritative adopted read failures authorize durable retirement") {
    assert(ShuffleRecoveryAdoptedMissing.authorizesRetirement)
    assert(ShuffleRecoveryAdoptedCorrupt.authorizesRetirement)
    assert(!ShuffleRecoveryAdoptedUnavailable.authorizesRetirement)
  }

  test("exact retirement cannot remove a newer compatible incarnation") {
    withTempDirectory { root =>
      val identity = feasibilityIdentity()
      val store = new ShuffleRecoveryManifestStore(root)
      val a = manifest(identity, generation = 1L, incarnation = "a")
      val b = manifest(identity, generation = 2L, incarnation = "b")

      assert(store.publish(a) == ShuffleRecoveryManifestPublished)
      assert(store.publish(b) == ShuffleRecoveryManifestPublished)
      assert(store.findCompatible("group", identity, currentGeneration = 3L).contains(b))

      val retirer = new ShuffleRecoveryManifestRetirer(root)
      val aIncarnation = ShuffleRecoveryManifestIncarnation(
        a.recoveryGroup,
        a.generation,
        a.incarnationId,
        a.identity.digest)
      assert(retirer.retireExact(aIncarnation) == ShuffleRecoveryIncarnationRetired)
      assert(store.findCompatible("group", identity, currentGeneration = 3L).contains(b))
      assert(retirer.retireExact(aIncarnation) == ShuffleRecoveryIncarnationAlreadyAbsent)
      assert(store.findCompatible("group", identity, currentGeneration = 3L).contains(b))
    }
  }

  test("retirement refuses a reference whose complete immutable tuple no longer matches") {
    withTempDirectory { root =>
      val identity = feasibilityIdentity()
      val store = new ShuffleRecoveryManifestStore(root)
      val a = manifest(identity, generation = 1L, incarnation = "a")
      assert(store.publish(a) == ShuffleRecoveryManifestPublished)

      val index = store.indexDirectoryFor(a.recoveryGroup, identity)
      val reference = onlyReference(index)
      val original = Files.readAllBytes(reference)
      val tampered = original.clone()
      tampered(tampered.length - 1) = (tampered.last ^ 1).toByte
      Files.write(reference, tampered)

      val result = new ShuffleRecoveryManifestRetirer(root).retireExact(
        ShuffleRecoveryManifestIncarnation(
          a.recoveryGroup,
          a.generation,
          a.incarnationId,
          a.identity.digest))
      assert(result == ShuffleRecoveryIncarnationRetirementRefused)
      assert(Files.isRegularFile(reference))
      assert(Files.readAllBytes(reference).sameElements(tampered))
    }
  }

  test("invalid retirement identity is non-destructive") {
    withTempDirectory { root =>
      val identity = feasibilityIdentity()
      val store = new ShuffleRecoveryManifestStore(root)
      val a = manifest(identity, generation = 1L, incarnation = "a")
      assert(store.publish(a) == ShuffleRecoveryManifestPublished)

      val result = new ShuffleRecoveryManifestRetirer(root).retireExact(
        ShuffleRecoveryManifestIncarnation(
          a.recoveryGroup,
          0L,
          a.incarnationId,
          a.identity.digest))
      assert(result == ShuffleRecoveryIncarnationRetirementRefused)
      assert(store.findCompatible("group", identity, currentGeneration = 2L).contains(a))
    }
  }

  private def feasibilityIdentity(): ShuffleRecoveryFeasibilityIdentity = {
    ShuffleRecoveryFeasibilityIdentity.create(
      sourceToken = "source-v1",
      producerTag = "producer-v1",
      rowEncoding = "unsafe-row-v1",
      partitioningShape = "hash-v1",
      mapperCount = 0,
      reducerCount = 1,
      resolvedLiteral = "literal-v1")
  }

  private def manifest(
      identity: ShuffleRecoveryFeasibilityIdentity,
      generation: Long,
      incarnation: String): ShuffleRecoveryManifest = {
    ShuffleRecoveryManifest(
      recoveryGroup = "group",
      generation = generation,
      incarnationId = incarnation,
      identity = identity,
      mapperCount = 0,
      reducerCount = 1,
      mapArtifacts = Vector.empty,
      descriptorVersion = ShuffleRecoveryManifest.DescriptorVersion,
      reducerBytes = Some(Vector(0L)),
      publicationTimestampMillis = generation)
  }

  private def onlyReference(index: Path): Path = {
    val stream = Files.list(index)
    try {
      val references = stream.iterator().asScala
        .filter(_.getFileName.toString.endsWith(".ref"))
        .toVector
      assert(references.size == 1)
      references.head
    } finally {
      stream.close()
    }
  }

  private def withTempDirectory(testBody: Path => Unit): Unit = {
    val root = Files.createTempDirectory("shuffle-recovery-adopted-failure-")
    try {
      testBody(root)
    } finally {
      val stream = Files.walk(root)
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
}
