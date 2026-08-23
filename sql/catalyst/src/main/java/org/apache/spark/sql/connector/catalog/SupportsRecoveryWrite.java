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
package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Evolving;

/**
 * A table that can identify a write destination across replacement drivers.
 *
 * <p>The identity must include the destination selector, such as a branch, but must not contain a
 * driver-local query, stage, or task identifier. Spark passes a durably resolved write ID through
 * {@link org.apache.spark.sql.connector.write.LogicalWriteInfo#queryId()}.
 */
@Evolving
public interface SupportsRecoveryWrite extends Table {
  String recoverySinkId();
}
