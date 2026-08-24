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

import org.apache.spark.annotation.Evolving;

/**
 * A recoverable batch write whose custom task metrics have a durable schema.
 *
 * <p>The schema must describe exactly the custom task metrics that every recovery data writer
 * reports in its {@link DataWriter#currentMetricsValues()} snapshot immediately after all input
 * records have been written and immediately before {@link DataWriter#commit()}. A recovery writer
 * must not change these values during commit. Missing, unknown,
 * duplicated, out-of-range, or schema-incompatible values cause recovery to fail. The schema is
 * bound into the immutable write manifest before Spark creates a writer.</p>
 *
 * <p>Implementations must return an equal schema for every driver incarnation of the same logical
 * write. They must not derive it from process-local state.</p>
 *
 * @since 4.2.0
 */
@Evolving
public interface SupportsRecoveryTaskMetrics extends SupportsBatchWriteRecovery {

  /** Returns the non-null immutable schema for durable additive custom task metrics. */
  RecoveryTaskMetricSchema recoveryTaskMetricSchema();
}
