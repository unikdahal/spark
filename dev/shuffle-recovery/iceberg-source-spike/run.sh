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

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

readonly iceberg_source_commit="e76d63584d7f83b102026749e1ae0f91813cb78e"
readonly iceberg_artifact="iceberg-spark-runtime-4.2_2.13"
readonly iceberg_build_task=":iceberg-spark:iceberg-spark-runtime-4.2_2.13:shadowJar"

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/shuffle-recovery-iceberg-source.XXXXXX")"
evidence_path="${1:-${work_dir}/iceberg-source-evidence.tsv}"
mkdir -p "$(dirname "${evidence_path}")"
: > "${evidence_path}"

stage="initialize"
finalize() {
  local rc=$?
  if [[ ${rc} -ne 0 ]]; then
    for partial in "${smoke_evidence:-}" "${conformance_evidence:-}"; do
      if [[ -n "${partial}" && -s "${partial}" ]]; then
        cat "${partial}" >> "${evidence_path}"
      fi
    done
    {
      printf 'failure_stage\t%s\n' "${stage}"
      printf 'runner_exit_code\t%s\n' "${rc}"
      printf 'runner_result\tFAILED\n'
    } >> "${evidence_path}"
  fi
  rm -rf "${work_dir}"
  exit "${rc}"
}
trap finalize EXIT

spark_candidate_commit="$(git rev-parse HEAD)"
{
  printf 'spark_candidate_commit\t%s\n' "${spark_candidate_commit}"
  printf 'iceberg_source_commit\t%s\n' "${iceberg_source_commit}"
  printf 'iceberg_runtime_artifact\t%s\n' "${iceberg_artifact}"
  printf 'iceberg_build_task\t%s\n' "${iceberg_build_task}"
} >> "${evidence_path}"

stage="fetch-pinned-iceberg-source"
iceberg_src="${work_dir}/iceberg"
git init -q "${iceberg_src}"
git -C "${iceberg_src}" remote add origin https://github.com/apache/iceberg.git
git -C "${iceberg_src}" fetch --depth=1 origin "${iceberg_source_commit}"
git -C "${iceberg_src}" checkout -q --detach FETCH_HEAD
resolved_iceberg_commit="$(git -C "${iceberg_src}" rev-parse HEAD)"
test "${resolved_iceberg_commit}" = "${iceberg_source_commit}"

runtime_cache="${ICEBERG_RUNTIME_CACHE:-${work_dir}/runtime-cache}"
cache_identity="${iceberg_source_commit}:${iceberg_build_task}"
stage="validate-runtime-cache"
runtime_origin=source-build
if [[ -f "${runtime_cache}/runtime.jar" && -f "${runtime_cache}/source.txt" && \
      -f "${runtime_cache}/sha512.txt" ]] &&
    [[ "$(cat "${runtime_cache}/source.txt")" == "${cache_identity}" ]] &&
    [[ "$(sha512sum "${runtime_cache}/runtime.jar" | awk '{print $1}')" == \
       "$(cat "${runtime_cache}/sha512.txt")" ]]; then
  runtime_origin=verified-cache
else
  stage="build-pinned-iceberg-runtime"
  (
    cd "${iceberg_src}"
    ./gradlew --no-daemon -DsparkVersions=4.2 "${iceberg_build_task}"
  )

  mapfile -t runtime_jars < <(
    find "${iceberg_src}/spark/v4.2/spark-runtime/build/libs" -maxdepth 1 -type f \
      -name "${iceberg_artifact}-*.jar" \
      ! -name '*-sources.jar' ! -name '*-javadoc.jar' | sort
  )
  if [[ ${#runtime_jars[@]} -ne 1 ]]; then
    printf 'expected exactly one built Iceberg runtime jar, found %s\n' "${#runtime_jars[@]}" >&2
    exit 1
  fi
  mkdir -p "${runtime_cache}"
  cp "${runtime_jars[0]}" "${runtime_cache}/runtime.jar"
  printf '%s\n' "${cache_identity}" > "${runtime_cache}/source.txt"
  sha512sum "${runtime_cache}/runtime.jar" | awk '{print $1}' > "${runtime_cache}/sha512.txt"
fi

export ICEBERG_RUNTIME_JAR="${runtime_cache}/runtime.jar"
printf 'iceberg_runtime_origin\t%s\n' "${runtime_origin}" >> "${evidence_path}"
export ICEBERG_VERSION="source:${iceberg_source_commit}:spark-4.2"
iceberg_sha512="$(sha512sum "${ICEBERG_RUNTIME_JAR}" | awk '{print $1}')"
{
  printf 'iceberg_resolved_source_commit\t%s\n' "${resolved_iceberg_commit}"
  printf 'iceberg_runtime_sha512\t%s\n' "${iceberg_sha512}"
} >> "${evidence_path}"

smoke_evidence="${work_dir}/compatibility-smoke.tsv"
conformance_evidence="${work_dir}/resolved-scan-conformance.tsv"

stage="compile-and-run-compatibility-smoke"
./build/sbt -Phadoop-3 -Phive \
  "project sql" \
  'set Test / unmanagedSourceDirectories ++= Seq("java", "scala").map(lang => file(sys.props("user.dir")) / "dev/shuffle-recovery/iceberg-source-spike/src/main" / lang)' \
  'set Test / unmanagedJars += file(sys.env("ICEBERG_RUNTIME_JAR"))' \
  "Test / compile" \
  "Test / runMain org.apache.iceberg.spark.source.ShuffleRecoveryIcebergCompatibilitySmoke ${smoke_evidence}"

test -s "${smoke_evidence}"
grep -F $'result\tPASS' "${smoke_evidence}"

stage="run-resolved-scan-conformance"
./build/sbt -Phadoop-3 -Phive \
  "project sql" \
  'set Test / unmanagedSourceDirectories ++= Seq("java", "scala").map(lang => file(sys.props("user.dir")) / "dev/shuffle-recovery/iceberg-source-spike/src/main" / lang)' \
  'set Test / unmanagedJars += file(sys.env("ICEBERG_RUNTIME_JAR"))' \
  "Test / runMain org.apache.iceberg.spark.source.ShuffleRecoveryIcebergSourceSpike ${conformance_evidence}"

test -s "${conformance_evidence}"
grep -F $'result\tPASS' "${conformance_evidence}"
grep -Fx $'canonical_shuffle_replanning\tPASS' "${conformance_evidence}"
grep -Fx $'canonical_shuffle_snapshot_binding\tPASS' "${conformance_evidence}"
grep -F $'decision\tRESOLVED_SCAN_CERTIFICATION_FEASIBLE_WITH_PRIVATE_ICEBERG_HOOKS' \
  "${conformance_evidence}"
stage="run-iceberg-cold-process-recovery"
bash dev/shuffle-recovery/iceberg-source-spike/cold-process.sh "${evidence_path}.cold"
grep -Fx 'ICEBERG_COLD_PROCESS_PASS' "${evidence_path}.cold/decision.txt"
cat "${smoke_evidence}" "${conformance_evidence}" >> "${evidence_path}"
printf 'cold_process_recovery\tPASS\n' >> "${evidence_path}"
printf 'runner_result\tPASS\n' >> "${evidence_path}"
cat "${evidence_path}"
