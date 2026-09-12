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
import java.nio.ByteBuffer

import org.apache.spark.SparkFunSuite

class ShuffleRecoveryNativeManifestSuite extends SparkFunSuite {
  private def manifest: ShuffleRecoveryManifest = {
    val identity = ShuffleRecoveryFeasibilityIdentity.create(
      "source", "producer", "rows", "hash-v1", 1, 2, "literal")
    ShuffleRecoveryManifest("group", 1L, "incarnation", identity, 1, 2,
      Vector.empty, ShuffleRecoveryManifest.DescriptorVersion, None, 1L,
      Some(Vector[Byte](1, 2, 3)))
  }

  test("native manifest preserves opaque bytes without fabricating map block indexes") {
    val value = manifest
    val encoded = ShuffleRecoveryManifestCodec.encode(value)
    assert(ByteBuffer.wrap(encoded).getInt(4) == ShuffleRecoveryManifest.NativeFormatVersion)
    assert(ShuffleRecoveryManifestCodec.decode(encoded) == value)
    withTempDir { dir =>
      val store = new ShuffleRecoveryManifestStore(dir.toPath)
      store.publish(value)
      assert(store.findCompatible("group", value.identity, 2L).contains(value))
    }
  }

  test("native and indexed descriptor modes cannot be mixed or silently omitted") {
    intercept[IOException] {
      ShuffleRecoveryManifestCodec.encode(manifest.copy(
        mapArtifacts = Vector(ShuffleRecoveryMapArtifact(0, 1L, "map", 10L, 24L))))
    }
    intercept[IllegalArgumentException] {
      ShuffleRecoveryManifestCodec.encode(manifest.copy(nativeDescriptor = Some(Vector.empty)))
    }
    intercept[IOException] {
      ShuffleRecoveryManifestCodec.encode(manifest.copy(nativeDescriptor = None))
    }
  }

  test("native scheduler statistics preserve zero and nonzero estimates without indexes") {
    val value = manifest.copy(nativeMapOutputs = Some(Vector(
      ShuffleRecoveryNativeMapOutput(101L, Vector(0L, 100L)))))
    val encoded = ShuffleRecoveryManifestCodec.encode(value)
    assert(ByteBuffer.wrap(encoded).getInt(4) ==
      ShuffleRecoveryManifest.NativeStatisticsFormatVersion)
    assert(ShuffleRecoveryManifestCodec.decode(encoded) == value)
    intercept[IllegalArgumentException] {
      ShuffleRecoveryManifestCodec.encode(value.copy(nativeMapOutputs = Some(Vector(
        ShuffleRecoveryNativeMapOutput(101L, Vector(-1L, 100L))))))
    }
    intercept[IllegalArgumentException] {
      ShuffleRecoveryManifestCodec.encode(value.copy(nativeMapOutputs = Some(Vector(
        ShuffleRecoveryNativeMapOutput(101L, Vector(100L))))))
    }
  }

  test("indexed manifests retain version one encoding") {
    val value = manifest.copy(nativeDescriptor = None,
      mapArtifacts = Vector(ShuffleRecoveryMapArtifact(0, 1L, "map", 10L, 24L)))
    val encoded = ShuffleRecoveryManifestCodec.encode(value)
    assert(ByteBuffer.wrap(encoded).getInt(4) == ShuffleRecoveryManifest.FormatVersion)
    assert(ShuffleRecoveryManifestCodec.decode(encoded) == value)
  }
}
