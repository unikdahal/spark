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

import java.util.{Arrays, Collections}

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

class RowLevelTaskSummarySuite extends SparkFunSuite {

  private def summary(values: Seq[Long] = 1L to 9L): RowLevelTaskSummary = {
    require(values.length == 9)
    new RowLevelTaskSummary(
      values(0), values(1), values(2), values(3),
      values(4), values(5), values(6), values(7), values(8))
  }

  test("summary exposes exact immutable counters") {
    val value = summary()

    assert(value.numTargetRowsScanned() == 1L)
    assert(value.numTargetRowsCopied() == 2L)
    assert(value.numTargetRowsDeleted() == 3L)
    assert(value.numTargetRowsUpdated() == 4L)
    assert(value.numTargetRowsInserted() == 5L)
    assert(value.numTargetRowsMatchedUpdated() == 6L)
    assert(value.numTargetRowsMatchedDeleted() == 7L)
    assert(value.numTargetRowsNotMatchedBySourceUpdated() == 8L)
    assert(value.numTargetRowsNotMatchedBySourceDeleted() == 9L)
  }

  test("summary rejects a negative value in every field") {
    (0 until 9).foreach { index =>
      val values = Array.fill(9)(0L)
      values(index) = -1L
      intercept[IllegalArgumentException] {
        summary(values.toSeq)
      }
    }
  }

  test("plus and sum aggregate every field") {
    val first = summary(0L to 8L)
    val second = summary(9L to 17L)
    val expected = summary(Seq(9L, 11L, 13L, 15L, 17L, 19L, 21L, 23L, 25L))

    assert(first.plus(second) == expected)
    assert(RowLevelTaskSummary.sum(Arrays.asList(first, second)) == expected)
    assert(RowLevelTaskSummary.sum(
      Collections.emptyList[RowLevelTaskSummary]()) == RowLevelTaskSummary.empty())
    assert(RowLevelTaskSummary.empty().plus(first) == first)
  }

  test("aggregation fails closed on overflow in every field") {
    (0 until 9).foreach { index =>
      val maximum = Array.fill(9)(0L)
      maximum(index) = Long.MaxValue
      val increment = Array.fill(9)(0L)
      increment(index) = 1L

      intercept[ArithmeticException] {
        summary(maximum.toSeq).plus(summary(increment.toSeq))
      }
      intercept[ArithmeticException] {
        RowLevelTaskSummary.sum(Arrays.asList(
          summary(maximum.toSeq), summary(increment.toSeq)))
      }
    }
  }

  test("aggregation rejects null inputs and elements") {
    intercept[NullPointerException] {
      summary().plus(null)
    }
    intercept[NullPointerException] {
      RowLevelTaskSummary.sum(null)
    }
    intercept[NullPointerException] {
      RowLevelTaskSummary.sum(Arrays.asList(summary(), null))
    }
  }

  test("value equality includes every field and survives serialization") {
    val value = summary()
    val same = summary()
    val restored = Utils.deserialize[RowLevelTaskSummary](Utils.serialize(value))

    assert(value == same)
    assert(value.hashCode() == same.hashCode())
    assert(restored == value)
    (0 until 9).foreach { index =>
      val different = (1L to 9L).toArray
      different(index) += 1L
      assert(value != summary(different.toSeq))
    }
  }
}
