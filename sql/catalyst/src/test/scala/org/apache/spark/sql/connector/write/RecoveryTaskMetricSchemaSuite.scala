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

package org.apache.spark.sql.connector.write

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

class RecoveryTaskMetricSchemaSuite extends SparkFunSuite {

  test("durable metric capability is a batch write recovery capability") {
    assert(classOf[SupportsBatchWriteRecovery].isAssignableFrom(
      classOf[SupportsRecoveryTaskMetrics]))
  }

  private def descriptor(
      name: String = "bytes-written",
      semanticType: String = "byte-count",
      aggregationId: String = "sum",
      version: Int = 1,
      minimumValue: Long = 0L,
      maximumValue: Long = Long.MaxValue): RecoveryTaskMetricDescriptor = {
    new RecoveryTaskMetricDescriptor(
      name, semanticType, aggregationId, version, minimumValue, maximumValue)
  }

  test("descriptor exposes immutable identity and range metadata") {
    val metric = descriptor(maximumValue = 1024L)

    assert(metric.name() == "bytes-written")
    assert(metric.semanticType() == "byte-count")
    assert(metric.aggregationId() == "sum")
    assert(metric.version() == 1)
    assert(metric.minimumValue() == 0L)
    assert(metric.maximumValue() == 1024L)
    assert(metric.accepts(0L))
    assert(metric.accepts(1024L))
    assert(!metric.accepts(-1L))
    assert(!metric.accepts(1025L))
  }

  test("descriptor equality includes every semantic field and range boundary") {
    val base = descriptor(maximumValue = 10L)
    val same = descriptor(maximumValue = 10L)

    assert(base == same)
    assert(base.hashCode() == same.hashCode())
    assert(base != descriptor(name = "other", maximumValue = 10L))
    assert(base != descriptor(semanticType = "record-count", maximumValue = 10L))
    assert(base != descriptor(version = 2, maximumValue = 10L))
    assert(base != descriptor(minimumValue = 1L, maximumValue = 10L))
    assert(base != descriptor(maximumValue = 11L))
    assert(descriptor(minimumValue = 7L, maximumValue = 7L).accepts(7L))
    assert(descriptor(maximumValue = Long.MaxValue).accepts(Long.MaxValue))
  }

  test("descriptor rejects null and blank identifiers") {
    Seq[(String, String, String)](
      (null, "type", "sum"),
      ("name", null, "sum"),
      ("name", "type", null)).foreach { case (name, semanticType, aggregationId) =>
      intercept[NullPointerException] {
        descriptor(name, semanticType, aggregationId)
      }
    }

    Seq[(String, String, String)](
      (" ", "type", "sum"),
      ("name", "\t", "sum"),
      ("name", "type", "\n")).foreach { case (name, semanticType, aggregationId) =>
      intercept[IllegalArgumentException] {
        descriptor(name, semanticType, aggregationId)
      }
    }
  }

  test("descriptor rejects non-positive versions and inverted ranges") {
    Seq(0, -1).foreach { version =>
      intercept[IllegalArgumentException] {
        descriptor(version = version)
      }
    }
    intercept[IllegalArgumentException] {
      descriptor(minimumValue = 2L, maximumValue = 1L)
    }
    intercept[IllegalArgumentException] {
      descriptor(aggregationId = "max")
    }
    intercept[IllegalArgumentException] {
      descriptor(minimumValue = -1L)
    }
  }

  test("schema preserves descriptor order and owns its array") {
    val first = descriptor("first")
    val second = descriptor("second")
    val input = Array(first, second)
    val schema = new RecoveryTaskMetricSchema("writer-metrics", 2, input)

    input(0) = descriptor("replacement")
    assert(schema.descriptors().toSeq == Seq(first, second))

    val returned = schema.descriptors()
    returned(1) = descriptor("replacement")
    assert(schema.descriptors().toSeq == Seq(first, second))
    assert(schema.schemaId() == "writer-metrics")
    assert(schema.version() == 2)
  }

  test("schema rejects invalid identity, version, descriptors, and duplicates") {
    val metric = descriptor()
    intercept[NullPointerException] {
      new RecoveryTaskMetricSchema(null, 1, Array(metric))
    }
    intercept[IllegalArgumentException] {
      new RecoveryTaskMetricSchema(" ", 1, Array(metric))
    }
    Seq(0, -1).foreach { version =>
      intercept[IllegalArgumentException] {
        new RecoveryTaskMetricSchema("writer-metrics", version, Array(metric))
      }
    }
    intercept[NullPointerException] {
      new RecoveryTaskMetricSchema("writer-metrics", 1, null)
    }
    intercept[IllegalArgumentException] {
      new RecoveryTaskMetricSchema("writer-metrics", 1, Array.empty)
    }
    intercept[NullPointerException] {
      new RecoveryTaskMetricSchema(
        "writer-metrics", 1, Array[RecoveryTaskMetricDescriptor](metric, null))
    }
    intercept[IllegalArgumentException] {
      new RecoveryTaskMetricSchema(
        "writer-metrics", 1, Array(metric, descriptor(name = metric.name())))
    }
  }

  test("schema equality includes ordered descriptors and survives serialization") {
    val metrics = Array(descriptor("rows"), descriptor("bytes"))
    val schema = new RecoveryTaskMetricSchema("writer-metrics", 1, metrics)
    val same = new RecoveryTaskMetricSchema("writer-metrics", 1, metrics.reverse.reverse)
    val reordered = new RecoveryTaskMetricSchema("writer-metrics", 1, metrics.reverse)
    val restored = Utils.deserialize[RecoveryTaskMetricSchema](Utils.serialize(schema))

    assert(schema == same)
    assert(schema.hashCode() == same.hashCode())
    assert(schema != reordered)
    assert(schema != new RecoveryTaskMetricSchema("other", 1, metrics))
    assert(schema != new RecoveryTaskMetricSchema("writer-metrics", 2, metrics))
    assert(restored == schema)
    assert(restored.descriptors().toSeq == metrics.toSeq)
  }
}
