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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite

class ShuffleRecoveryPublicationAttemptsSuite extends SparkFunSuite {
  private def collect(
      events: ShuffleRecoveryPublicationCoordinator => Unit): Vector[ShuffleRecoveryPublication] = {
    val results = new ConcurrentLinkedQueue[ShuffleRecoveryPublication]()
    val backend = new ShuffleRecoveryPublicationBackend {
      override def publish(publication: ShuffleRecoveryPublication): Unit = {
        results.add(publication)
      }
    }
    val publisher = new ShuffleRecoveryManifestPublisher(backend, 2)
    val coordinator = new ShuffleRecoveryPublicationCoordinator(publisher, 2, Some(7), _ => true)
    try events(coordinator) finally publisher.close()
    results.asScala.toVector
  }

  test("retry preserves the accepted attempt coordinates for maps reused from an earlier stage") {
    val results = collect { coordinator =>
      coordinator.stageSubmitted(1, 0, 7, 2)
      coordinator.taskSucceeded(1, 0, 0, 101L, 3)
      coordinator.stageCompleted(1, 0, successful = false)
      coordinator.stageSubmitted(1, 1, 7, 2)
      coordinator.taskSucceeded(1, 0, 0, 199L, 4)
      coordinator.taskSucceeded(1, 1, 1, 202L, 2)
      coordinator.stageCompleted(1, 1, successful = true)
    }
    assert(results.size == 1)
    assert(results.head.winningMapTaskIds == Vector(101L, 202L))
    assert(results.head.winningMapAttempts == Vector(
      ShuffleRecoveryMapAttempt(101L, 0, 3), ShuffleRecoveryMapAttempt(202L, 1, 2)))
  }

  test("a replacement winner replaces its attempt metadata in the same partition") {
    val results = collect { coordinator =>
      coordinator.stageSubmitted(1, 0, 7, 1)
      coordinator.taskSucceeded(1, 0, 0, 101L, 0)
      coordinator.taskSucceeded(1, 0, 0, 102L, 1)
      coordinator.stageCompleted(1, 0, successful = true)
    }
    assert(results.head.winningMapTaskIds == Vector(102L))
    assert(results.head.winningMapAttempts == Vector(ShuffleRecoveryMapAttempt(102L, 0, 1)))
  }

  test("missing attempt coordinates cannot inherit a previous winner's attempt number") {
    val results = collect { coordinator =>
      coordinator.stageSubmitted(1, 0, 7, 1)
      coordinator.taskSucceeded(1, 0, 0, 101L, 4)
      coordinator.taskSucceeded(1, 0, 0, 102L)
      coordinator.stageCompleted(1, 0, successful = true)
    }
    assert(results.head.winningMapTaskIds == Vector(102L))
    assert(results.head.winningMapAttempts.isEmpty)
  }
}
