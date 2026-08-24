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
import org.apache.spark.sql.connector.catalog.transactions.TransactionRecoveryInfo;
import org.apache.spark.sql.connector.catalog.transactions.TransactionRecoveryResult;

/**
 * A transactional catalog that can durably resolve a stable transaction identity.
 *
 * <p>Implementations must resolve repeated calls for the same identity to the same logical
 * transaction. An unavailable, ambiguous, or inconsistent outcome must be returned as
 * {@code UNKNOWN}; it must not be treated as an absent transaction.</p>
 *
 * @since 4.2.0
 */
@Evolving
public interface SupportsTransactionRecovery extends TransactionalCatalogPlugin {

  /**
   * Creates or recovers the transaction identified by {@code info} and returns its authoritative
   * durable state. Implementations must not create a replacement transaction when the outcome is
   * unknown.
   */
  TransactionRecoveryResult resolveTransaction(TransactionRecoveryInfo info);
}
