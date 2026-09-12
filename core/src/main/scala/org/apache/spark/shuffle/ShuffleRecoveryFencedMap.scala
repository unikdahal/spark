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

package org.apache.spark.shuffle

import java.io.IOException

import scala.util.control.NonFatal

import org.apache.spark.network.buffer.ManagedBuffer

/**
 * Keeps an opened map view bound to the exact live claim. A provider can use the same wrapper
 * for an active-binding check or a locally maintained retention lease deadline. The callback must
 * be a local, nonblocking check; renewing a remote lease belongs outside the fetch critical path.
 */
private[shuffle] final class ShuffleRecoveryFencedMap(
    delegate: ShuffleRecoveryResolvedMap,
    current: () => Boolean) extends ShuffleRecoveryResolvedMap {
  require(delegate != null && current != null)

  private def checkCurrent(): Unit = {
    if (!current()) {
      throw new IOException("retained shuffle binding is no longer current")
    }
  }

  private def checked[T](read: => T): T = {
    checkCurrent()
    val value = read
    checkCurrent()
    value
  }

  override def numReducers: Int = checked(delegate.numReducers)
  override def dataLength: Long = checked(delegate.dataLength)
  override def indexBytes: Long = checked(delegate.indexBytes)
  override def blockMetadata(reduceId: Int): ShuffleRecoveryBlockMetadata =
    checked(delegate.blockMetadata(reduceId))

  override def getBlockData(reduceId: Int): Option[ManagedBuffer] = {
    checkCurrent()
    val buffer = delegate.getBlockData(reduceId)
    try {
      checkCurrent()
      buffer
    } catch {
      case NonFatal(error) =>
        try {
          if (buffer != null) buffer.foreach { value =>
            if (value != null) value.release()
          }
        } catch {
          case NonFatal(cleanupError) => error.addSuppressed(cleanupError)
        }
        throw error
    }
  }
}
