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

import java.io.Serializable;
import java.util.Objects;

import org.apache.spark.annotation.Evolving;

/**
 * An immutable descriptor for one metric in a durable recovery task result.
 *
 * <p>The name identifies the corresponding custom task metric. The semantic type is a stable
 * connector-defined identifier, not a display string. The aggregation identifier must be
 * {@value #ADDITIVE_AGGREGATION}; recovery metrics are non-negative additive counters because
 * Spark can restore their exact total without recreating executor task events. Increment
 * {@link #version()} whenever the meaning or encoding changes.</p>
 *
 * <p>Spark validates every recovered value against the inclusive range before using it. A schema
 * intended for non-negative counters should use zero as its minimum.</p>
 *
 * @since 4.2.0
 */
@Evolving
public final class RecoveryTaskMetricDescriptor implements Serializable {
  private static final long serialVersionUID = 1L;

  /** The only aggregation that can be reconstructed exactly after driver recovery. */
  public static final String ADDITIVE_AGGREGATION = "sum";

  private final String name;
  private final String semanticType;
  private final String aggregationId;
  private final int version;
  private final long minimumValue;
  private final long maximumValue;

  /**
   * Creates a metric descriptor.
   *
   * @param name stable custom task metric name
   * @param semanticType stable identifier for the value's meaning
   * @param aggregationId stable identifier for the task-value aggregation semantics
   * @param version positive format version for this descriptor
   * @param minimumValue inclusive minimum accepted value
   * @param maximumValue inclusive maximum accepted value
   */
  public RecoveryTaskMetricDescriptor(
      String name,
      String semanticType,
      String aggregationId,
      int version,
      long minimumValue,
      long maximumValue) {
    this.name = requireIdentifier(name, "name");
    this.semanticType = requireIdentifier(semanticType, "semanticType");
    this.aggregationId = requireIdentifier(aggregationId, "aggregationId");
    if (!ADDITIVE_AGGREGATION.equals(this.aggregationId)) {
      throw new IllegalArgumentException(
          "Recovery task metrics must use additive aggregation: " + this.aggregationId);
    }
    if (version <= 0) {
      throw new IllegalArgumentException("Metric descriptor version must be positive: " + version);
    }
    if (minimumValue > maximumValue) {
      throw new IllegalArgumentException(
          "Metric minimum value must not exceed maximum value: " + minimumValue + " > " +
              maximumValue);
    }
    if (minimumValue < 0L) {
      throw new IllegalArgumentException(
          "Recovery task metric minimum must be non-negative: " + minimumValue);
    }
    this.version = version;
    this.minimumValue = minimumValue;
    this.maximumValue = maximumValue;
  }

  /** Returns the stable custom task metric name. */
  public String name() {
    return name;
  }

  /** Returns the stable identifier for the value's meaning. */
  public String semanticType() {
    return semanticType;
  }

  /** Returns the stable identifier for the task-value aggregation semantics. */
  public String aggregationId() {
    return aggregationId;
  }

  /** Returns the positive format version for this descriptor. */
  public int version() {
    return version;
  }

  /** Returns the inclusive minimum accepted value. */
  public long minimumValue() {
    return minimumValue;
  }

  /** Returns the inclusive maximum accepted value. */
  public long maximumValue() {
    return maximumValue;
  }

  /** Returns whether {@code value} is valid for this descriptor. */
  public boolean accepts(long value) {
    return value >= minimumValue && value <= maximumValue;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof RecoveryTaskMetricDescriptor)) {
      return false;
    }
    RecoveryTaskMetricDescriptor that = (RecoveryTaskMetricDescriptor) other;
    return version == that.version &&
        minimumValue == that.minimumValue &&
        maximumValue == that.maximumValue &&
        name.equals(that.name) &&
        semanticType.equals(that.semanticType) &&
        aggregationId.equals(that.aggregationId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name, semanticType, aggregationId, version, minimumValue, maximumValue);
  }

  private static String requireIdentifier(String value, String label) {
    Objects.requireNonNull(value, label);
    if (value.trim().isEmpty()) {
      throw new IllegalArgumentException("Metric descriptor " + label + " must not be blank");
    }
    return value;
  }
}
