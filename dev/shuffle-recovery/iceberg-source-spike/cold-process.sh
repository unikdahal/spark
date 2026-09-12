#!/usr/bin/env bash
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

set -euo pipefail

: "${ICEBERG_RUNTIME_JAR:?pinned source-built Iceberg runtime is required}"
evidence_dir="${1:?evidence directory is required}"
mkdir -p "${evidence_dir}"
evidence_dir="$(cd "${evidence_dir}" && pwd)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/shuffle-recovery-iceberg-cold.XXXXXX")"
trap 'rm -rf "${work_dir}"' EXIT
candidate="$(git rev-parse HEAD)"
export SPARK_SHUFFLE_RECOVERY_TEST_ICEBERG_WAREHOUSE="file://${work_dir}/warehouse"
export SPARK_SHUFFLE_RECOVERY_TEST_SOURCE_ADAPTER=org.apache.spark.sql.execution.exchange.ShuffleRecoveryIcebergColdSource
export SPARK_SHUFFLE_RECOVERY_TEST_CANONICAL_IDENTITY=true
export SPARK_SHUFFLE_RECOVERY_TEST_PRODUCER_FILTER=false
export SPARK_SHUFFLE_RECOVERY_TEST_MASTER='local[2]'
export SPARK_SHUFFLE_RECOVERY_TEST_LOCAL_ROOT="${work_dir}/scratch"
export SPARK_LOCAL_DIRS="${work_dir}/scratch/executor-local"
entry=org.apache.spark.shuffle.ShuffleRecoveryColdProcessProcess
setup=org.apache.spark.sql.execution.exchange.ShuffleRecoveryIcebergColdSourceSetup
common="root=${work_dir}/retained scenario=sparse group=iceberg-cold testedCommit=${candidate}"

run_main() {
  local label="$1" command="$2"
  rm -rf "${work_dir}/scratch"
  mkdir -p "${work_dir}/scratch"
  timeout --kill-after=30s 15m ./build/sbt -Phadoop-3 -Phive 'project sql' \
    'set Test / unmanagedSourceDirectories ++= Seq("java", "scala").map(lang => file(sys.props("user.dir")) / "dev/shuffle-recovery/iceberg-source-spike/src/main" / lang)' \
    'set Test / unmanagedJars += file(sys.env("ICEBERG_RUNTIME_JAR"))' \
    'set Test / run / fork := true' \
    'set Test / javaOptions += "-Dspark.shuffle.useOldFetchProtocol=true"' \
    'set Test / javaOptions += "-Djava.io.tmpdir=" + sys.env("SPARK_SHUFFLE_RECOVERY_TEST_LOCAL_ROOT")' \
    "Test/runMain ${command}" 2>&1 | tee "${evidence_dir}/${label}.log"
}
run_child() {
  local label="$1" mode="$2"
  shift 2
  run_main "${label}" "${entry} ${mode} ${common} evidence=${evidence_dir}/${label}.tsv processEvidence=${evidence_dir}/${label}.process $*"
}
run_main create "${setup} create ${evidence_dir}/snapshot-before.txt"
run_child baseline baseline
run_child producer producer
run_child replacement replacement \
  "baseline=${evidence_dir}/baseline.tsv producer=${evidence_dir}/producer.tsv"
for control in source-token artifact-missing producer-filter; do
  export SPARK_SHUFFLE_RECOVERY_TEST_PRODUCER_FILTER=false
  if [[ "${control}" == producer-filter ]]; then
    export SPARK_SHUFFLE_RECOVERY_TEST_PRODUCER_FILTER=true
  fi
  run_child "${control}" replacement \
    "baseline=${evidence_dir}/baseline.tsv producer=${evidence_dir}/producer.tsv control=${control}"
done
export SPARK_SHUFFLE_RECOVERY_TEST_PRODUCER_FILTER=false
run_main rewrite "${setup} rewrite ${evidence_dir}/snapshot-after.txt"
run_child source-snapshot replacement \
  "baseline=${evidence_dir}/baseline.tsv producer=${evidence_dir}/producer.tsv control=source-snapshot"

python3 - "${evidence_dir}" "${candidate}" <<'CHECK'
import csv
import sys
from pathlib import Path
root = Path(sys.argv[1])
candidate = sys.argv[2]
processes = set()
def record(name):
    with (root / (name + '.tsv')).open() as stream:
        rows = list(csv.DictReader(stream, delimiter='\t'))
    assert len(rows) == 1, name
    row = rows[0]
    assert row['testedCommit'] == candidate, name
    assert row['scenario'] == 'sparse' and row['group'] == 'iceberg-cold', name
    assert row['rowCount'] == '32', name
    process = dict(line.split('=', 1) for line in
                   (root / (name + '.process')).read_text().splitlines())
    assert process['testedCommit'] == candidate and process['master'] == 'local[2]', name
    assert process['mode'] == row['role'], name
    assert process['sourceAdapter'] == (
        'org.apache.spark.sql.execution.exchange.ShuffleRecoveryIcebergColdSource'), name
    identity = (process['pid'], process['started'])
    assert identity not in processes, 'proof reused a child JVM'
    processes.add(identity)
    return row
baseline, producer, replacement = [record(n) for n in ['baseline', 'producer', 'replacement']]
assert [r['role'] for r in [baseline, producer, replacement]] == [
    'baseline', 'producer', 'replacement']
assert all(r['control'] == 'none' for r in [baseline, producer, replacement])
assert int(baseline['mapTaskCount']) > 0 and int(producer['mapTaskCount']) > 0
assert baseline['resultDigest'] == producer['resultDigest'] == replacement['resultDigest']
assert replacement['adopted'] == 'true' and replacement['mapTaskCount'] == '0'
assert int(replacement['providerBytesRead']) > 0
assert replacement['currentShuffleId'] != producer['originShuffleId']
for control in ['source-token', 'artifact-missing', 'producer-filter', 'source-snapshot']:
    missed = record(control)
    assert missed['role'] == 'replacement' and missed['control'] == control
    assert missed['adopted'] == 'false' and int(missed['mapTaskCount']) > 0
    assert missed['providerBytesRead'] == '0'
    assert missed['resultDigest'] == baseline['resultDigest']
assert (root / 'snapshot-before.txt').read_text() != (root / 'snapshot-after.txt').read_text()
(root / 'decision.txt').write_text('ICEBERG_COLD_PROCESS_PASS\n')
CHECK
