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

package org.apache.spark.sql.connector.write;

import java.io.IOException;

import org.apache.spark.sql.catalyst.InternalRow;

/** Compile-time fixture for the Java recovery-delta covariance contract. */
public final class JavaRecoveryDeltaConformance {
  private JavaRecoveryDeltaConformance() {}

  /** A writer usable through both the delta and recovery writer APIs. */
  public abstract static class Writer implements RecoveryDeltaWriter {
    @Override
    public void write(InternalRow row) throws IOException {
      insert(row);
    }

    @Override
    public abstract void insert(InternalRow row) throws IOException;

    @Override
    public abstract void delete(InternalRow metadata, InternalRow id) throws IOException;

    @Override
    public abstract void update(
        InternalRow metadata, InternalRow id, InternalRow row) throws IOException;

    @Override
    public abstract WriterCommitMessage commit() throws IOException;

    @Override
    public abstract void abort() throws IOException;

    @Override
    public abstract void close() throws IOException;

    @Override
    public abstract void discardCommittedOutput(WriterCommitMessage committedMessage);
  }

  /** A factory whose return type satisfies both parent factory interfaces. */
  public abstract static class Factory implements RecoveryDeltaWriterFactory {
    @Override
    public abstract RecoveryDeltaWriter createWriter(int partitionId, long taskId);
  }

  /** A batch write that retains delta typing and requires durable metrics. */
  public abstract static class BatchWrite implements SupportsDeltaBatchWriteRecovery {
    @Override
    public abstract RecoveryDeltaWriterFactory createBatchWriterFactory(PhysicalWriteInfo info);

    @Override
    public abstract RecoveryTaskMetricSchema recoveryTaskMetricSchema();
  }

  /** Verifies source assignments through every covariant parent type. */
  public static void verifyAssignments(
      RecoveryDeltaWriter writer,
      RecoveryDeltaWriterFactory factory,
      SupportsDeltaBatchWriteRecovery batchWrite) {
    DeltaWriter<InternalRow> deltaWriter = writer;
    RecoveryDataWriter recoveryWriter = writer;
    DeltaWriterFactory deltaFactory = factory;
    RecoveryDataWriterFactory recoveryFactory = factory;
    DeltaBatchWrite deltaBatchWrite = batchWrite;
    SupportsBatchWriteRecovery recoveryBatchWrite = batchWrite;
    SupportsRecoveryTaskMetrics metricBatchWrite = batchWrite;

    if (deltaWriter == null || recoveryWriter == null || deltaFactory == null
        || recoveryFactory == null || deltaBatchWrite == null || recoveryBatchWrite == null
        || metricBatchWrite == null) {
      throw new AssertionError("unexpected null recovery-delta contract value");
    }
  }
}
