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

import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.security.MessageDigest

/** Small forked-JVM smoke proof for canonical computation identity bytes. */
object ShuffleRecoveryComputationIdentityProcess {
  def main(args: Array[String]): Unit = {
    require(args.length == 2, "expected mode and output path")
    val mode = args(0)
    val path = Paths.get(args(1))
    val payload = ShuffleRecoveryComputationIdentityTestData.baseIdentity().canonicalPayload.toArray

    mode match {
      case "write" => write(path, payload)
      case "compare" =>
        val existing = Files.readAllBytes(path)
        require(MessageDigest.isEqual(existing, payload),
          "independent JVMs produced different canonical identity bytes")
      case other => throw new IllegalArgumentException(s"unsupported mode: $other")
    }
  }

  private def write(path: Path, payload: Array[Byte]): Unit = {
    Option(path.getParent).foreach(Files.createDirectories(_))
    Files.write(
      path,
      payload,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.WRITE)
  }
}
