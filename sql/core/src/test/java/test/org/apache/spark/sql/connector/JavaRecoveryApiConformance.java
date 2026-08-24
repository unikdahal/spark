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

package test.org.apache.spark.sql.connector;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.SupportsRecoveryAnchor;
import org.apache.spark.sql.connector.catalog.SupportsRecoveryWrite;
import org.apache.spark.sql.connector.recovery.RecoveryTaskCommitStore;
import org.apache.spark.sql.connector.write.BatchWriteRecoveryState;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.RecoveryCommitMessageCodec;
import org.apache.spark.sql.connector.write.RecoveryDataWriter;
import org.apache.spark.sql.connector.write.RecoveryDataWriterFactory;
import org.apache.spark.sql.connector.write.RecoveryTaskMetricDescriptor;
import org.apache.spark.sql.connector.write.RecoveryTaskMetricSchema;
import org.apache.spark.sql.connector.write.SupportsRecoveryTaskMetrics;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

/**
 * Compile-time conformance fixture for recovery-capable Java connectors and task-store providers.
 *
 * <p>This class deliberately implements the public interfaces directly. Source incompatibilities
 * therefore fail SQL test compilation even when binary-compatibility tooling cannot see a newly
 * introduced API.</p>
 */
public final class JavaRecoveryApiConformance {
  private JavaRecoveryApiConformance() {}

  /** Exercises the recovery flag added to the existing logical write API. */
  public static boolean isRecoveryEnabled(LogicalWriteInfo info) {
    return info.isRecoveryEnabled();
  }

  /** A Java connector implementation of the complete recoverable batch-write contract. */
  public abstract static class RecoverableWrite implements SupportsRecoveryTaskMetrics {
    private static final RecoveryTaskMetricSchema METRIC_SCHEMA =
        new RecoveryTaskMetricSchema(
            "java-recovery-metrics",
            1,
            new RecoveryTaskMetricDescriptor[] {
                new RecoveryTaskMetricDescriptor(
                    "bytes",
                    "byte-count",
                    RecoveryTaskMetricDescriptor.ADDITIVE_AGGREGATION,
                    1,
                    0L,
                    Long.MAX_VALUE)
            });

    @Override
    public RecoveryCommitMessageCodec commitMessageCodec() {
      return new JavaCommitCodec();
    }

    @Override
    public byte[] recoveryCompatibilityMetadata(PhysicalWriteInfo info) {
      return ("partitions=" + info.numPartitions()).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public BatchWriteRecoveryState recover(PhysicalWriteInfo info) {
      return new BatchWriteRecoveryState() {
        @Override
        public boolean isCommitted() {
          return false;
        }

        @Override
        public long totalNumRows() {
          return -1L;
        }

        @Override
        public java.util.Map<String, Long> totalTaskMetrics() {
          return Collections.emptyMap();
        }
      };
    }

    @Override
    public void abortAfterRecovery(WriterCommitMessage[] messages) {}

    @Override
    public RecoveryTaskMetricSchema recoveryTaskMetricSchema() {
      return METRIC_SCHEMA;
    }
  }

  /** Exercises stable source and sink identities on a Java table implementation. */
  public abstract static class RecoverableTable
      implements SupportsRecoveryAnchor, SupportsRecoveryWrite {
    @Override
    public String recoverySourceId() {
      return "java-source";
    }

    @Override
    public String currentRecoveryAnchor() {
      return "snapshot-1";
    }

    @Override
    public SupportsRecoveryAnchor withRecoveryAnchor(String anchor) {
      return this;
    }

    @Override
    public String recoverySinkId() {
      return "java-sink";
    }
  }

  /** Exercises the recovery-specific Java writer and covariant factory contracts. */
  public abstract static class JavaRecoveryWriter implements RecoveryDataWriter {
    @Override
    public abstract void write(InternalRow record);

    @Override
    public abstract WriterCommitMessage commit();

    @Override
    public abstract void discardCommittedOutput(WriterCommitMessage committedMessage);
  }

  public abstract static class RecoverableDataWriterFactory
      implements RecoveryDataWriterFactory {
    @Override
    public abstract RecoveryDataWriter createWriter(int partitionId, long taskId);
  }

  /** A stable Java codec whose payload does not depend on Java serialization. */
  public static final class JavaCommitCodec implements RecoveryCommitMessageCodec {
    @Override
    public String codecId() {
      return "java-recovery-codec";
    }

    @Override
    public int version() {
      return 1;
    }

    @Override
    public byte[] encode(WriterCommitMessage message) {
      return ((JavaCommitMessage) message).value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public WriterCommitMessage decode(int version, byte[] payload) {
      if (version != 1) {
        throw new IllegalArgumentException("Unsupported codec version: " + version);
      }
      return new JavaCommitMessage(new String(payload, StandardCharsets.UTF_8));
    }
  }

  /** A minimal immutable commit message used by the codec fixture. */
  public static final class JavaCommitMessage implements WriterCommitMessage {
    private final String value;

    JavaCommitMessage(String value) {
      this.value = value;
    }
  }

  /** A Java provider implementation of the complete durable task-store contract. */
  public static final class JavaTaskCommitStore implements RecoveryTaskCommitStore {
    private static final Capabilities CAPABILITIES = new Capabilities() {
      @Override
      public int semanticsVersion() {
        return RecoveryTaskCommitStore.SEMANTICS_VERSION;
      }

      @Override
      public int maxLoadBatchSize() {
        return 1024;
      }

      @Override
      public int maxManifestBytes() {
        return 1024 * 1024;
      }

      @Override
      public int maxTaskCommitBytes() {
        return 16 * 1024 * 1024;
      }
    };

    @Override
    public Capabilities capabilities() {
      return CAPABILITIES;
    }

    @Override
    public byte[] resolveWriteManifest(String recoveryId, byte[] proposedValue) {
      return proposedValue.clone();
    }

    @Override
    public List<Optional<byte[]>> load(String recoveryId, int[] partitionIds) {
      List<Optional<byte[]>> results = new ArrayList<>(partitionIds.length);
      Arrays.stream(partitionIds).forEach(ignored -> results.add(Optional.empty()));
      return results;
    }

    @Override
    public byte[] publish(
        String recoveryId,
        int partitionId,
        long taskAttemptId,
        int attemptNumber,
        byte[] value) {
      return value.clone();
    }
  }

  /** Exercises the typed provider failure surface from Java. */
  public static RecoveryTaskCommitStore.StoreException unavailable(String message) {
    return new RecoveryTaskCommitStore.StoreException(
        RecoveryTaskCommitStore.FailureReason.UNAVAILABLE, message);
  }
}
