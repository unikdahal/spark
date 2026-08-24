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

class RecoveryDeltaContractSuite extends SparkFunSuite {

  test("recovery delta writer preserves both parent writer types") {
    assert(classOf[DeltaWriter[_]].isAssignableFrom(classOf[RecoveryDeltaWriter]))
    assert(classOf[RecoveryDataWriter].isAssignableFrom(classOf[RecoveryDeltaWriter]))
  }

  test("recovery delta factory preserves both parent factory types") {
    assert(classOf[DeltaWriterFactory].isAssignableFrom(classOf[RecoveryDeltaWriterFactory]))
    assert(classOf[RecoveryDataWriterFactory]
      .isAssignableFrom(classOf[RecoveryDeltaWriterFactory]))
    assert(classOf[RecoveryDeltaWriter] ===
      classOf[RecoveryDeltaWriterFactory].getMethod(
        "createWriter", classOf[Int], classOf[Long]).getReturnType)
  }

  test("delta batch recovery requires durable task metrics") {
    assert(classOf[DeltaBatchWrite].isAssignableFrom(classOf[SupportsDeltaBatchWriteRecovery]))
    assert(classOf[SupportsBatchWriteRecovery]
      .isAssignableFrom(classOf[SupportsDeltaBatchWriteRecovery]))
    assert(classOf[SupportsRecoveryTaskMetrics]
      .isAssignableFrom(classOf[SupportsDeltaBatchWriteRecovery]))
    assert(classOf[RecoveryDeltaWriterFactory] ===
      classOf[SupportsDeltaBatchWriteRecovery].getMethod(
        "createBatchWriterFactory", classOf[PhysicalWriteInfo]).getReturnType)
  }
}
