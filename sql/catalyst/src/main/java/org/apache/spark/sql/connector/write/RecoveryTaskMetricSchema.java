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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.spark.annotation.Evolving;

/**
 * An immutable ordered schema for custom metrics stored in recovery task results.
 *
 * <p>Descriptor order is part of the durable format. Schema identifiers and versions must remain
 * stable while the ordered descriptors have the same meaning. A connector must use a new schema
 * version when descriptors are added, removed, reordered, or changed.</p>
 *
 * @since 4.2.0
 */
@Evolving
public final class RecoveryTaskMetricSchema implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String schemaId;
  private final int version;
  private final RecoveryTaskMetricDescriptor[] descriptors;

  /**
   * Creates a non-empty ordered metric schema.
   *
   * @param schemaId stable connector-defined schema identifier
   * @param version positive schema version
   * @param descriptors non-null ordered descriptors with unique metric names
   */
  public RecoveryTaskMetricSchema(
      String schemaId, int version, RecoveryTaskMetricDescriptor[] descriptors) {
    this.schemaId = requireSchemaId(schemaId);
    if (version <= 0) {
      throw new IllegalArgumentException("Recovery metric schema version must be positive: " +
          version);
    }
    Objects.requireNonNull(descriptors, "descriptors");
    if (descriptors.length == 0) {
      throw new IllegalArgumentException("Recovery metric schema must not be empty");
    }
    this.descriptors = descriptors.clone();
    Set<String> names = new HashSet<>();
    for (int index = 0; index < this.descriptors.length; index++) {
      RecoveryTaskMetricDescriptor descriptor = Objects.requireNonNull(
          this.descriptors[index], "descriptor at index " + index);
      if (!names.add(descriptor.name())) {
        throw new IllegalArgumentException(
            "Duplicate recovery task metric name: " + descriptor.name());
      }
    }
    this.version = version;
  }

  /** Returns the stable connector-defined schema identifier. */
  public String schemaId() {
    return schemaId;
  }

  /** Returns the positive schema version. */
  public int version() {
    return version;
  }

  /** Returns a defensive copy of the ordered metric descriptors. */
  public RecoveryTaskMetricDescriptor[] descriptors() {
    return descriptors.clone();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof RecoveryTaskMetricSchema)) {
      return false;
    }
    RecoveryTaskMetricSchema that = (RecoveryTaskMetricSchema) other;
    return version == that.version &&
        schemaId.equals(that.schemaId) &&
        Arrays.equals(descriptors, that.descriptors);
  }

  @Override
  public int hashCode() {
    return 31 * Objects.hash(schemaId, version) + Arrays.hashCode(descriptors);
  }

  private static String requireSchemaId(String schemaId) {
    Objects.requireNonNull(schemaId, "schemaId");
    if (schemaId.trim().isEmpty()) {
      throw new IllegalArgumentException("Recovery metric schema ID must not be blank");
    }
    return schemaId;
  }
}
