#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Remove only the producer's files in the isolated worker after replacement adoption."""

import os
from pathlib import Path
import sys
import time

root = Path(sys.argv[1])
evidence = Path(sys.argv[2])
worker = Path(os.environ["CELEBORN_PROOF_WORKER_ROOT"]).resolve(strict=True)
deadline = time.monotonic() + 1200
while not (root / "fault-ready").exists():
    if time.monotonic() > deadline:
        raise TimeoutError("replacement never reached the adopted-read barrier")
    time.sleep(0.2)
application, shuffle = (root / "producer-namespace").read_text().splitlines()
assert application.startswith("cold-") and "/" not in application and ".." not in application
assert shuffle.isdigit()
namespace = (worker / application / shuffle).resolve(strict=True)
assert namespace.is_relative_to(worker) and namespace != worker
files = sorted(p for p in namespace.rglob("*") if p.is_file())
assert files and sum(p.stat().st_size for p in files) > 0, "no durable native files to remove"
records = []
for path in files:
    assert path.resolve().is_relative_to(namespace), "unexpected worker storage symlink"
    records.append(f"{path}\t{path.stat().st_size}")
    path.unlink()
evidence.write_text("\n".join(records) + "\n")
(root / "fault-applied").write_text("producer files removed; owner and worker remain alive\n")
