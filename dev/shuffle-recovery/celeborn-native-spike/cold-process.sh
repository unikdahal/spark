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

: "${ICEBERG_RUNTIME_JAR:?source-built Iceberg runtime jar is required}"
: "${CELEBORN_RUNTIME_JAR:?source-built Celeborn Spark 4 shaded jar is required}"
: "${CELEBORN_RETAINED_ENDPOINT_FILE:?running standalone owner endpoint file is required}"
[[ -f "$ICEBERG_RUNTIME_JAR" && -f "$CELEBORN_RUNTIME_JAR" ]]
[[ -f "$CELEBORN_RETAINED_ENDPOINT_FILE" ]]
evidence_dir="${1:?new evidence directory is required}"
mkdir "${evidence_dir}"
evidence_dir="$(cd "${evidence_dir}" && pwd)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/celeborn-cold.XXXXXX")"
# Preserve the warehouse and manifests for diagnosis, including on failure.
printf '%s\n' "${work_dir}" > "${evidence_dir}/work-directory.txt"
export SPARK_RECOVERY_TESTED_COMMIT="$(git rev-parse HEAD)"
export SPARK_SHUFFLE_RECOVERY_TEST_ICEBERG_WAREHOUSE="file://${work_dir}/warehouse"
export SPARK_SHUFFLE_RECOVERY_TEST_PRODUCER_FILTER=false
export SPARK_LOCAL_DIRS="${work_dir}/scratch"
mkdir "${work_dir}/manifests"
sha256sum "$ICEBERG_RUNTIME_JAR" "$CELEBORN_RUNTIME_JAR" > "${evidence_dir}/jars.sha256"
git diff --binary HEAD > "${evidence_dir}/working-tree.patch"
entry=org.apache.spark.shuffle.celeborn.ShuffleRecoveryCelebornColdProcess
setup=org.apache.spark.sql.execution.exchange.ShuffleRecoveryIcebergColdSourceSetup
group="native-$(basename "${work_dir}")"

export NATIVE_PROOF_CLASSPATH="${evidence_dir}/classpath.txt"
export NATIVE_PROOF_JAVA_OPTIONS="${evidence_dir}/java-options.txt"
timeout --kill-after=30s 45m ./build/sbt -Phadoop-3 -Phive 'project sql' \
  'set Compile / unmanagedSourceDirectories ++= Seq("java", "scala").map(lang => file(sys.props("user.dir")) / "dev/shuffle-recovery/iceberg-source-spike/src/main" / lang)' \
  'set Compile / unmanagedSourceDirectories += file(sys.props("user.dir")) / "dev/shuffle-recovery/celeborn-native-spike/src/main/scala"' \
  'set Compile / unmanagedSources += file(sys.props("user.dir")) / "sql/core/src/test/scala/org/apache/spark/shuffle/ShuffleRecoveryColdProcessSource.scala"' \
  'set Compile / unmanagedSources += file(sys.props("user.dir")) / "sql/core/src/test/scala/org/apache/spark/sql/execution/exchange/ShuffleRecoveryCanonicalRangeInputs.scala"' \
  'set Compile / unmanagedJars ++= Seq("ICEBERG_RUNTIME_JAR", "CELEBORN_RUNTIME_JAR").map(key => file(sys.env(key)))' \
  'set Test / javaOptions += "-Dspark.shuffle.useOldFetchProtocol=false"' \
  'set Global / commands += Command.command("exportNativeProof") { state => val ex = Project.extract(state); val (next, cp) = ex.runTask(Compile / fullClasspath, state); val (done, opts) = Project.extract(next).runTask(Test / javaOptions, next); IO.write(file(sys.env("NATIVE_PROOF_CLASSPATH")), cp.files.map(_.getAbsolutePath).mkString(java.io.File.pathSeparator)); IO.write(file(sys.env("NATIVE_PROOF_JAVA_OPTIONS")), opts.mkString("\n")); done }' \
  'Compile/compile' 'exportNativeProof' 2>&1 | tee "${evidence_dir}/compile.log"
[[ -s "$NATIVE_PROOF_CLASSPATH" && -s "$NATIVE_PROOF_JAVA_OPTIONS" ]]
mapfile -t java_options < "$NATIVE_PROOF_JAVA_OPTIONS"
classpath="$(cat "$NATIVE_PROOF_CLASSPATH")"
run_main() {
  local label="$1" command="$2"
  local -a arguments
  read -r -a arguments <<< "$command"
  # Separate scratch per child also permits concurrent replacement drivers.
  local scratch="${work_dir}/scratch/${label}"
  mkdir -p "$scratch"
  SPARK_LOCAL_DIRS="$scratch" timeout --kill-after=30s 15m \
    "${JAVA_HOME}/bin/java" "${java_options[@]}" "-Djava.io.tmpdir=${scratch}" \
    -cp "$classpath" "${arguments[@]}" 2>&1 | tee "${evidence_dir}/${label}.log"
  rm -rf "$scratch"
}
run_child() {
  local label="$1" role="$2" control="$3"
  run_main "$label" "${entry} ${role} ${work_dir}/manifests ${evidence_dir}/${label}.properties ${group} ${control}"
}
run_main create "${setup} create ${evidence_dir}/snapshot-before.txt"
run_child baseline baseline none
run_child producer producer none
run_child replacement replacement none
# Both readers independently claim the same descriptor; neither owns the other's lease.
run_child concurrent-a replacement concurrent &
first_reader=$!
run_child concurrent-b replacement concurrent &
second_reader=$!
first_status=0
second_status=0
wait "$first_reader" || first_status=$?
wait "$second_reader" || second_status=$?
[[ "$first_status" == 0 && "$second_status" == 0 ]]
for control in source-token manifest-missing producer-filter; do
  export SPARK_SHUFFLE_RECOVERY_TEST_PRODUCER_FILTER=false
  if [[ "$control" == producer-filter ]]; then
    export SPARK_SHUFFLE_RECOVERY_TEST_PRODUCER_FILTER=true
  fi
  run_child "$control" replacement "$control"
done
export SPARK_SHUFFLE_RECOVERY_TEST_PRODUCER_FILTER=false
run_child lease-expiry replacement lease-expiry
python3 dev/shuffle-recovery/celeborn-native-spike/artifact-loss.py \
  "${work_dir}/manifests" "${evidence_dir}/artifact-loss-files.txt" &
fault_pid=$!
if ! run_child artifact-loss replacement artifact-loss; then
  kill "$fault_pid" 2>/dev/null || true
  wait "$fault_pid" || true
  exit 1
fi
wait "$fault_pid"
run_main rewrite "${setup} rewrite ${evidence_dir}/snapshot-after.txt"
run_child source-snapshot replacement source-snapshot

# Publish a fresh artifact before restarting its owner; use a separate manifest namespace.
restart_root="${work_dir}/restart-manifests"
mkdir "$restart_root"
run_main restart-producer \
  "${entry} producer ${restart_root} ${evidence_dir}/restart-producer.properties ${group}-restart none"
python3 - "$CELEBORN_PROOF_CONTROL_ROOT" <<'RESTART'
import time
from pathlib import Path
import sys
control = Path(sys.argv[1])
(control / "restart-owner").write_text("restart the harness-owned lifecycle service\n")
deadline = time.monotonic() + 120
while not (control / "owner-restarted").exists():
    if time.monotonic() > deadline:
        raise TimeoutError("owner restart did not complete")
    time.sleep(0.2)
RESTART
run_main owner-restart \
  "${entry} replacement ${restart_root} ${evidence_dir}/owner-restart.properties ${group}-restart owner-restart"

python3 - "${evidence_dir}" <<'CHECK'
import sys
from pathlib import Path

root = Path(sys.argv[1])
processes = set()
records = {}
for name in ("baseline", "producer", "replacement", "concurrent-a", "concurrent-b",
             "source-token",
             "manifest-missing", "producer-filter", "source-snapshot", "artifact-loss", "lease-expiry",
             "restart-producer", "owner-restart"):
    row = dict(line.split("=", 1) for line in
               (root / (name + ".properties")).read_text().splitlines())
    process = (row["pid"], row["started"])
    assert process not in processes, "driver JVM was reused"
    processes.add(process)
    assert row["rowCount"] == "32", name
    records[name] = row
baseline = records["baseline"]
for name, row in records.items():
    assert row["resultDigest"] == baseline["resultDigest"], name
    assert row["testedCommit"] == baseline["testedCommit"], name
    if name in ("replacement", "concurrent-a", "concurrent-b"):
        assert row["adopted"] == "true" and row["offered"] == "true"
        assert row["mapTaskCount"] == "0" and int(row["remoteBytesRead"]) > 0
    else:
        assert row["adopted"] == "false" and int(row["mapTaskCount"]) > 0, name
        if name in ("artifact-loss", "lease-expiry"):
            assert row["adoptedBeforeRead"] == "true" and int(row["fetchFailures"]) > 0
assert (root / "snapshot-before.txt").read_text() != (
    root / "snapshot-after.txt").read_text()
(root / "decision.txt").write_text("NATIVE_COLD_PROCESS_AND_ARTIFACT_LOSS_PASS\n")
CHECK
