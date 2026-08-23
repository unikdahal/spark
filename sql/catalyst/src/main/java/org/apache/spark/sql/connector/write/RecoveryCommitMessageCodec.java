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

import org.apache.spark.annotation.Evolving;

/**
 * A stable, non-executable encoding of a connector's writer commit messages.
 *
 * <p>Implementations must remain able to decode every version that may be present in the durable
 * recovery store during a rolling upgrade. Java or Kryo object serialization must not be used:
 * recovery records may outlive the classes and class loader that produced them.</p>
 */
@Evolving
public interface RecoveryCommitMessageCodec extends Serializable {

  /** Stable connector-defined identifier for the payload format. */
  String codecId();

  /** Positive version of the payload format produced by {@link #encode}. */
  int version();

  /** Encodes a non-null commit message. */
  byte[] encode(WriterCommitMessage message);

  /** Decodes a payload produced by this codec at the supplied format version. */
  WriterCommitMessage decode(int version, byte[] payload);
}
