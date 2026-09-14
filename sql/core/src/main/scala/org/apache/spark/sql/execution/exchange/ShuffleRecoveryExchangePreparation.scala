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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.sql.catalyst.trees.TreeNodeTag

/**
 * Opt-in driver preparation for the existing recovery prototype. AQE copies an exchange while
 * applying stage rules, so bind the claim to the final exchange, not its pre-stage dependency.
 * TreeNode copies preserve the tag; execution occurs on the shuffle preparation thread before
 * map-stage submission. The callback must revalidate the final producer and own its cleanup.
 */
private[spark] object ShuffleRecoveryExchangePreparation {
  private val preparation =
    TreeNodeTag[ShuffleExchangeExec => Unit]("shuffleRecoveryExchangePreparation")

  def attach(exchange: ShuffleExchangeExec)(prepare: ShuffleExchangeExec => Unit): Unit = {
    require(exchange != null && prepare != null && !exchange.pipelined)
    require(exchange.getTagValue(preparation).isEmpty, "recovery preparation already attached")
    exchange.setTagValue(preparation, prepare)
  }

  def prepare(exchange: ShuffleExchangeExec): Unit = {
    if (!exchange.pipelined) exchange.getTagValue(preparation).foreach(_(exchange))
  }
}
