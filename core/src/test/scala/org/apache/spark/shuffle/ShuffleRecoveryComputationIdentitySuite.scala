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

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.Base64

import org.apache.spark.SparkFunSuite

class ShuffleRecoveryComputationIdentitySuite extends SparkFunSuite {
  import ShuffleRecoveryComputationIdentityTestData._

  test("versioned canonical payload matches the golden vector and round trips") {
    val identity = baseIdentity()
    val payload = identity.canonicalPayload.toArray

    assert(identity.digest === GoldenDigest)
    assert(Base64.getEncoder.encodeToString(payload) === GoldenPayloadBase64)
    assert(ShuffleRecoveryComputationIdentityCodec.decode(payload) === identity)
  }

  test("runtime build compatibility refuses unknown revisions and separates builds") {
    val first = ShuffleRecoveryComputationIdentity.buildCompatibilityId("a" * 40).get
    val second = ShuffleRecoveryComputationIdentity.buildCompatibilityId("b" * 40).get
    assert(first != second)
    val base = baseIdentity()
    val firstBuild = base.copy(
      compatibility = base.compatibility.copy(sparkCompatibilityId = first))
    val secondBuild = base.copy(
      compatibility = base.compatibility.copy(sparkCompatibilityId = second))
    assert(firstBuild.canonicalPayload != secondBuild.canonicalPayload)
    Seq(null, "", "<unknown>", "abcdef", "a" * 40 + "-dirty").foreach { revision =>
      assert(ShuffleRecoveryComputationIdentity.buildCompatibilityId(revision).isEmpty)
    }
  }

  test("certified batch source operator survives canonical encoding") {
    val base = baseIdentity()
    val identity = base.copy(producer = ShuffleRecoveryOperatorNode(
      ShuffleRecoveryOperatorKind.CertifiedBatchSource,
      Vector.empty, Vector(inputExpression(0)), Vector.empty))
    assert(ShuffleRecoveryComputationIdentityCodec.decode(
      identity.canonicalPayload.toArray) === identity)
    assert(identity.digest !== base.digest)
  }

  test("certified batch sources reject non-leaf shapes and inconsistent field ordinals") {
    val base = baseIdentity()
    val source = ShuffleRecoveryOperatorNode(ShuffleRecoveryOperatorKind.CertifiedBatchSource,
      Vector.empty, Vector(inputExpression(0)), Vector.empty)
    val invalid = Seq(
      source.copy(parameters = Vector(ShuffleRecoveryIntValue(1))),
      source.copy(children = Vector(ShuffleRecoveryInlineOperator(base.producer))),
      source.copy(expressions = Vector(inputExpression(1))))
    invalid.foreach { producer =>
      intercept[IllegalArgumentException] {
        base.copy(producer = producer).canonicalPayload
      }
    }
  }

  test("semantic field mutations change the canonical identity") {
    val base = baseIdentity()
    def changed(candidate: ShuffleRecoveryComputationIdentity): Unit = {
      assert(candidate.canonicalPayload != base.canonicalPayload)
      assert(candidate.digest != base.digest)
    }

    changed(base.copy(
      outputContract = base.outputContract.copy(
        fields = Vector(ShuffleRecoveryOutputField.create("long-v1", nullable = false)))))
    changed(base.copy(
      outputContract = base.outputContract.copy(
        fields = Vector(ShuffleRecoveryOutputField.create("int-v1", nullable = true)))))
    changed(base.copy(
      outputContract = base.outputContract.copy(rowEncodingVersion = "unsafe-row-v2")))
    changed(base.copy(producer = base.producer.copy(kind = ShuffleRecoveryOperatorKind.Filter)))
    changed(base.copy(producer = base.producer.copy(
      expressions = Vector(base.producer.expressions.head.copy(
        kind = ShuffleRecoveryExpressionKind.Input)))))
    changed(base.copy(producer = base.producer.copy(
      expressions = Vector(base.producer.expressions.head.copy(
        parameters = Vector(ShuffleRecoveryIntValue(8)))))))
    changed(base.copy(partitioning = base.partitioning.asInstanceOf[ShuffleRecoveryHashPartitioning]
      .copy(numPartitions = 3)))
    changed(base.copy(partitioning = base.partitioning.asInstanceOf[ShuffleRecoveryHashPartitioning]
      .copy(expressions = Vector(inputExpression(1)))))
    changed(base.copy(sourceTokens = Vector(sourceToken("source-B"))))
    changed(base.copy(mapperDecomposition = decomposition("split-mutated")))
    changed(base.copy(resolvedValues = Vector("runtime-literal" -> ShuffleRecoveryIntValue(8))))
    changed(base.copy(semanticConfig = Vector(
      "spark.sql.ansi.enabled" -> "true",
      "spark.sql.session.timeZone" -> "UTC")))
    changed(base.copy(semanticConfig = Vector(
      "spark.sql.ansi.enabled" -> "false",
      "spark.sql.session.timeZone" -> "America/Los_Angeles")))
    changed(base.copy(compatibility = base.compatibility.copy(
      providerReadFormatId = "reference-shuffle-provider-v2")))

    val parentA = baseIdentity(resolvedLiteral = 11)
    val parentB = baseIdentity(resolvedLiteral = 12)
    changed(withParent(base, parentA))
    assert(withParent(base, parentA).canonicalPayload != withParent(base, parentB).canonicalPayload)
  }

  test("map insertion order cannot perturb the canonical payload") {
    val first = ShuffleRecoveryComputationIdentity.create(
      baseIdentity().outputContract,
      baseIdentity().producer,
      baseIdentity().partitioning,
      baseIdentity().mapperDecomposition,
      baseIdentity().sourceTokens,
      Map(
        "z-value" -> ShuffleRecoveryIntValue(9),
        "a-value" -> ShuffleRecoveryIntValue(1)),
      Map(
        "z-conf" -> "last",
        "a-conf" -> "first"),
      baseIdentity().compatibility)
    val second = ShuffleRecoveryComputationIdentity.create(
      baseIdentity().outputContract,
      baseIdentity().producer,
      baseIdentity().partitioning,
      baseIdentity().mapperDecomposition,
      baseIdentity().sourceTokens,
      List(
        "a-value" -> ShuffleRecoveryIntValue(1),
        "z-value" -> ShuffleRecoveryIntValue(9)).toMap,
      List(
        "a-conf" -> "first",
        "z-conf" -> "last").toMap,
      baseIdentity().compatibility)

    assert(first.canonicalPayload === second.canonicalPayload)
  }

  test("short digest collisions still require full payload equality") {
    val base = baseIdentity()
    val collision = (0 until 65536).iterator
      .filter(_ != 7)
      .map(value => base.copy(
        resolvedValues = Vector("runtime-literal" -> ShuffleRecoveryIntValue(value))))
      .find(_.lookupPrefix == base.lookupPrefix)
      .getOrElse(fail("did not find deterministic two-hex-digit digest collision"))

    assert(collision.digest != base.digest)
    assert(collision.lookupPrefix === base.lookupPrefix)
    assert(!ShuffleRecoveryComputationIdentityCodec.fullPayloadMatches(base, collision))
    assert(!ShuffleRecoveryComputationIdentityCodec.compatibleAfterDigestHit(base, collision))
  }

  test("unknown versions and malformed lengths fail before unsafe allocation") {
    val payload = baseIdentity().canonicalPayload.toArray
    val futureVersion = payload.clone()
    futureVersion(4) = 0
    futureVersion(5) = 0
    futureVersion(6) = 0
    futureVersion(7) = 99

    intercept[IOException] {
      ShuffleRecoveryComputationIdentityCodec.decode(futureVersion)
    }
    intercept[IOException] {
      ShuffleRecoveryComputationIdentityCodec.decode(payload.dropRight(1))
    }
    intercept[IOException] {
      ShuffleRecoveryComputationIdentityCodec.decode(
        new Array[Byte](ShuffleRecoveryComputationIdentityCodec.MaxIdentityBytes + 1))
    }
  }

  test("source, split and binary factories defensively own mutable input") {
    val tokenBytes = "source-A".getBytes(StandardCharsets.UTF_8)
    val splitBytes = "split-0".getBytes(StandardCharsets.UTF_8)
    val binaryBytes = Array[Byte](1, 2, 3)
    val token = ShuffleRecoverySourceToken.copyOf(1, tokenBytes)
    val split = ShuffleRecoveryMapperSplit.copyOf(0, 0, 1, splitBytes)
    val binary = ShuffleRecoveryCanonicalValue.binary(binaryBytes)

    tokenBytes(0) = 'X'.toByte
    splitBytes(0) = 'X'.toByte
    binaryBytes(0) = 99.toByte

    assert(new String(token.payload.toArray, StandardCharsets.UTF_8) === "source-A")
    assert(new String(split.descriptor.toArray, StandardCharsets.UTF_8) === "split-0")
    assert(binary.value === Vector[Byte](1, 2, 3))
  }

  test("connector protocol and version separate otherwise identical source certificates") {
    val bytes = Array[Byte](1, 2, 3)
    val first = ShuffleRecoverySourceToken.forProtocol("example.source-a", 1, bytes)
    val repeated = ShuffleRecoverySourceToken.forProtocol("example.source-a", 1, bytes)
    val otherProtocol = ShuffleRecoverySourceToken.forProtocol("example.source-b", 1, bytes)
    val otherVersion = ShuffleRecoverySourceToken.forProtocol("example.source-a", 2, bytes)
    assert(first == repeated)
    val identity = baseIdentity().copy(sourceTokens = Vector(first))
    for (other <- Seq(otherProtocol, otherVersion)) {
      val changed = identity.copy(sourceTokens = Vector(other))
      assert(changed.canonicalPayload != identity.canonicalPayload)
      assert(changed.digest != identity.digest)
    }
    assert(ShuffleRecoveryComputationIdentityCodec.decode(
      identity.canonicalPayload.toArray) == identity)
    bytes(0) = 99.toByte
    assert(first == repeated)
    assert(first != ShuffleRecoverySourceToken.forProtocol("example.source-a", 1, bytes))

    // Length framing distinguishes pairs that would collide under raw concatenation.
    val left = ShuffleRecoverySourceToken.forProtocol("a", 1, Array[Byte](98, 99))
    val right = ShuffleRecoverySourceToken.forProtocol("ab", 1, Array[Byte](99))
    assert(left != right)
  }

  test("source certificate framing includes all bytes in the token limit") {
    val maximum = ShuffleRecoveryComputationIdentityCodec.MaxTokenBytes
    val certificate = new Array[Byte](maximum - 13) // Three lengths/tags and one protocol byte.
    val token = ShuffleRecoverySourceToken.forProtocol("a", 1, certificate)
    assert(token.payload.size == maximum)
    val identity = baseIdentity().copy(sourceTokens = Vector(token))
    assert(ShuffleRecoveryComputationIdentityCodec.decode(
      identity.canonicalPayload.toArray) == identity)
    intercept[IllegalArgumentException] {
      ShuffleRecoverySourceToken.forProtocol("a", 1, new Array[Byte](certificate.length + 1))
    }
  }

  test("source certificate framing rejects missing facts and malformed protocol identifiers") {
    val bytes = Array[Byte](1)
    val malformed = new String(Array(0xd800.toChar))
    val multibyte = 0xe9.toChar.toString * ShuffleRecoverySourceToken.MaxProtocolBytes
    for (protocol <- Seq(null, "", malformed, multibyte,
        "x" * (ShuffleRecoverySourceToken.MaxProtocolBytes + 1))) {
      intercept[IllegalArgumentException] {
        ShuffleRecoverySourceToken.forProtocol(protocol, 1, bytes)
      }
    }
    for (version <- Seq(0, -1)) {
      intercept[IllegalArgumentException] {
        ShuffleRecoverySourceToken.forProtocol("example.source", version, bytes)
      }
    }
    for (certificate <- Seq(null, Array.emptyByteArray,
        new Array[Byte](ShuffleRecoveryComputationIdentityCodec.MaxTokenBytes + 1))) {
      intercept[IllegalArgumentException] {
        ShuffleRecoverySourceToken.forProtocol("example.source", 1, certificate)
      }
    }
  }

  test("NaN encoding is canonical while signed zero remains explicit") {
    val nanA = java.lang.Float.intBitsToFloat(0x7fc00001)
    val nanB = java.lang.Float.intBitsToFloat(0x7fffffff)
    assert(ShuffleRecoveryCanonicalValue.float(nanA) === ShuffleRecoveryCanonicalValue.float(nanB))
    assert(ShuffleRecoveryCanonicalValue.double(Double.NaN) ===
      ShuffleRecoveryCanonicalValue.double(java.lang.Double.longBitsToDouble(0x7ff8000000000001L)))
    assert(ShuffleRecoveryCanonicalValue.float(0.0f) !== ShuffleRecoveryCanonicalValue.float(-0.0f))
    assert(ShuffleRecoveryCanonicalValue.double(0.0d) !==
      ShuffleRecoveryCanonicalValue.double(-0.0d))
  }

  test("recursive and impossible identity graphs fail closed within hard bounds") {
    val base = baseIdentity()
    val invalidParentReference = base.copy(
      producer = base.producer.copy(children = Vector(ShuffleRecoveryParentExchange(1))),
      parentExchangeIdentities = Vector(base))
    intercept[IllegalArgumentException] {
      ShuffleRecoveryComputationIdentityCodec.validate(invalidParentReference)
    }

    intercept[IllegalArgumentException] {
      var nested = base
      var depth = 0
      while (depth <= ShuffleRecoveryComputationIdentityCodec.MaxDepth + 1) {
        nested = nested.copy(parentExchangeIdentities = Vector(nested))
        ShuffleRecoveryComputationIdentityCodec.validate(nested)
        depth += 1
      }
    }
  }
}

private[shuffle] object ShuffleRecoveryComputationIdentityTestData {
  val GoldenDigest = "4b21b6cac7b7ac69a098bef0ca8de2c94bf88188b67039c38f002f6e9fdc5362"
  val GoldenPayloadBase64 =
    "U1JJMgAAAAIAAAABAAAABmludC12MQAAAAAAAAAADXVuc2FmZS1yb3ctdjEAAAAYdW5zYWZlLXJv" +
      "dy1zZXJpYWxpemVyLXYxAAAAFXNwYXJrLWludGVybmFsLXJvdy12MQAAAAEAAAAAAAAAAQAAAAEA" +
      "AAAGaW50LXYxAAAAAAECAAAABwAAAAAAAAABAQAAAAMAAAAEAwAAAAAAAAAAAwAAAAAAAAAQAwAA" +
      "AAAAAAABAgAAAAIAAAAAAAAAAAEAAAACAAAAKgAAABtzcGFyay1tdXJtdXIzLTMyLXNlZWQtNDIt" +
      "djEAAAABAAAAAgAAAAZpbnQtdjEAAAAAAQIAAAAAAAAAAAAAAAIAAAACAAAAAAAAAAAAAAABAAAA" +
      "B3NwbGl0LTAAAAAAAAAAAQAAAAEAAAAHc3BsaXQtMQAAAAEAAAABAAAACHNvdXJjZS1BAAAAAQAA" +
      "AA9ydW50aW1lLWxpdGVyYWwCAAAABwAAAAIAAAAWc3Bhcmsuc3FsLmFuc2kuZW5hYmxlZAAAAAVm" +
      "YWxzZQAAABpzcGFyay5zcWwuc2Vzc2lvbi50aW1lWm9uZQAAAANVVEMAAABLc3BhcmstMmE3Y2Zl" +
      "YTA2YmExMzVjZjBkZGM2MjkwMmViMGRhZjVhODM1YzY3Mi1zaHVmZmxlLXJlY292ZXJ5LWlkZW50" +
      "aXR5LXYyAAAAIHNwYXJrLXNvcnQtc2h1ZmZsZS11bnNhZmUtcm93LXYxAAAAHXJlZmVyZW5jZS1z" +
      "aHVmZmxlLXByb3ZpZGVyLXYxAAAAAA=="

  def baseIdentity(resolvedLiteral: Int = 7): ShuffleRecoveryComputationIdentity = {
    ShuffleRecoveryComputationIdentity.create(
      ShuffleRecoveryOutputContract(
        Vector(ShuffleRecoveryOutputField.create("int-v1", nullable = false)),
        "unsafe-row-v1",
        "unsafe-row-serializer-v1",
        "spark-internal-row-v1"),
      ShuffleRecoveryOperatorNode(
        ShuffleRecoveryOperatorKind.Project,
        Vector.empty,
        Vector(ShuffleRecoveryExpressionNode(
          ShuffleRecoveryExpressionKind.Literal,
          "int-v1",
          nullable = false,
          Vector(ShuffleRecoveryIntValue(7)),
          Vector.empty)),
        Vector(ShuffleRecoveryInlineOperator(ShuffleRecoveryOperatorNode(
          ShuffleRecoveryOperatorKind.RangeSource,
          Vector(
            ShuffleRecoveryLongValue(0L),
            ShuffleRecoveryLongValue(16L),
            ShuffleRecoveryLongValue(1L),
            ShuffleRecoveryIntValue(2)),
          Vector.empty,
          Vector.empty)))),
      ShuffleRecoveryHashPartitioning(
        2,
        Vector(inputExpression(0)),
        42,
        "spark-murmur3-32-seed-42-v1"),
      decomposition("split-0"),
      Vector(sourceToken("source-A")),
      Map("runtime-literal" -> ShuffleRecoveryIntValue(resolvedLiteral)),
      Map(
        "spark.sql.session.timeZone" -> "UTC",
        "spark.sql.ansi.enabled" -> "false"),
      ShuffleRecoveryCompatibility(
        "spark-2a7cfea06ba135cf0ddc62902eb0daf5a835c672-shuffle-recovery-identity-v2",
        "spark-sort-shuffle-unsafe-row-v1",
        "reference-shuffle-provider-v1"))
  }

  def inputExpression(ordinal: Int): ShuffleRecoveryExpressionNode = {
    ShuffleRecoveryExpressionNode(
      ShuffleRecoveryExpressionKind.Input,
      "int-v1",
      nullable = false,
      Vector(ShuffleRecoveryIntValue(ordinal)),
      Vector.empty)
  }

  def sourceToken(value: String): ShuffleRecoverySourceToken =
    ShuffleRecoverySourceToken.copyOf(1, value.getBytes(StandardCharsets.UTF_8))

  def decomposition(firstDescriptor: String): ShuffleRecoveryMapperDecomposition = {
    ShuffleRecoveryMapperDecomposition(
      2,
      Vector(
        ShuffleRecoveryMapperSplit.copyOf(
          0, 0, 1, firstDescriptor.getBytes(StandardCharsets.UTF_8)),
        ShuffleRecoveryMapperSplit.copyOf(
          0, 1, 1, "split-1".getBytes(StandardCharsets.UTF_8))))
  }

  def withParent(
      identity: ShuffleRecoveryComputationIdentity,
      parent: ShuffleRecoveryComputationIdentity): ShuffleRecoveryComputationIdentity = {
    identity.copy(
      producer = identity.producer.copy(children = Vector(ShuffleRecoveryParentExchange(0))),
      parentExchangeIdentities = Vector(parent))
  }
}
