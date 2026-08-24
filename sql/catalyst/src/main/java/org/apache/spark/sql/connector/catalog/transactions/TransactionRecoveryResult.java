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

import java.util.Objects;
import java.util.Optional;

import org.apache.spark.annotation.Evolving;

/**
 * An authoritative result of resolving a recoverable catalog transaction.
 *
 * <p>An {@link TransactionRecoveryState#OPEN OPEN} result always contains a transaction. Terminal
 * and {@link TransactionRecoveryState#UNKNOWN UNKNOWN} results never contain one. Use the static
 * factories to preserve this invariant.</p>
 *
 * @since 4.2.0
 */
@Evolving
public final class TransactionRecoveryResult {
  private final TransactionRecoveryState state;
  private final Optional<Transaction> transaction;

  private TransactionRecoveryResult(
      TransactionRecoveryState state, Optional<Transaction> transaction) {
    this.state = Objects.requireNonNull(state, "state");
    this.transaction = Objects.requireNonNull(transaction, "transaction");
  }

  /** Returns an open result attached to {@code transaction}. */
  public static TransactionRecoveryResult open(Transaction transaction) {
    return new TransactionRecoveryResult(
        TransactionRecoveryState.OPEN,
        Optional.of(Objects.requireNonNull(transaction, "transaction")));
  }

  /** Returns a terminal committed result. */
  public static TransactionRecoveryResult committed() {
    return withoutTransaction(TransactionRecoveryState.COMMITTED);
  }

  /** Returns a terminal aborted result. */
  public static TransactionRecoveryResult aborted() {
    return withoutTransaction(TransactionRecoveryState.ABORTED);
  }

  /** Returns a fail-closed result when the durable state cannot be determined. */
  public static TransactionRecoveryResult unknown() {
    return withoutTransaction(TransactionRecoveryState.UNKNOWN);
  }

  private static TransactionRecoveryResult withoutTransaction(TransactionRecoveryState state) {
    return new TransactionRecoveryResult(state, Optional.empty());
  }

  /** Returns the authoritative durable transaction state. */
  public TransactionRecoveryState state() {
    return state;
  }

  /** Returns the attached transaction exactly when {@link #state()} is {@code OPEN}. */
  public Optional<Transaction> transaction() {
    return transaction;
  }
}
