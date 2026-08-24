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

package org.apache.spark.sql.connector.catalog.transactions;

import org.apache.spark.annotation.Evolving;

/**
 * Immutable identity metadata used to begin or recover a catalog transaction.
 *
 * <p>The transaction {@link #id()} is derived from all fields in this interface. A catalog must
 * durably resolve repeated requests with the same identity to the same logical transaction.</p>
 *
 * @since 4.2.0
 */
@Evolving
public interface TransactionRecoveryInfo extends TransactionInfo {

  /** Returns the stable identity of the recoverable Spark execution. */
  String recoveryExecutionId();

  /** Returns the stable catalog identity used when deriving {@link #id()}. */
  String catalogIdentity();

  /** Returns the SHA-256 digest of the canonical logical operation. */
  String canonicalOperationDigest();
}
