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

module="${1:-}"
case "${module}" in core|sql) ;; *) echo "expected core or sql" >&2; exit 2 ;; esac
: "${CANDIDATE_SHA:?}"
: "${BASE_SHA:?}"
: "${VALIDATION_MODE:?}"
: "${IDENTITY_SOURCE:?}"
: "${PROVIDER:?}"
: "${ATTEMPT:?}"
: "${SCHEDULER:?}"
: "${HARNESS:?}"
: "${EVIDENCE_DIR:?}"

actual="$(git rev-parse HEAD)"
if [[ "${actual}" != "${CANDIDATE_SHA}" ]]; then
  echo "${module} checkout ${actual} does not match candidate ${CANDIDATE_SHA}" >&2
  exit 1
fi

if [[ "${EVIDENCE_DIR}" == "/" ]]; then
  echo "refusing to use filesystem root as evidence directory" >&2
  exit 2
fi
rm -rf "${EVIDENCE_DIR}"
mkdir -p "${EVIDENCE_DIR}/reports"
compile=not-started
tests=not-started
proof=not-started
process_evidence=not-required
finalized=false

write_status() {
  {
    echo "compilation=${compile}"
    echo "tests=${tests}"
    echo "proof=${proof}"
    echo "process_evidence=${process_evidence}"
  } > "${EVIDENCE_DIR}/status.env"
}

finalize_on_exit() {
  local status=$?
  trap - EXIT
  if [[ "${finalized}" != true ]]; then
    write_status
    bash .github/scripts/shuffle-recovery-ci-evidence.sh checksum-tree "${EVIDENCE_DIR}" || true
  fi
  exit "${status}"
}
trap finalize_on_exit EXIT

{
  echo "candidate_kind=experimental-tree"
  echo "candidate_sha=${actual}"
  echo "integration_base_sha=${BASE_SHA}"
  echo "validation_mode=${VALIDATION_MODE}"
  echo "workflow_run_id=${GITHUB_RUN_ID:-local}"
} > "${EVIDENCE_DIR}/provenance.env"
: > "${EVIDENCE_DIR}/commands.txt"
{
  java -version
  sed 's/^/build_property=/' project/build.properties
  echo "root_pom_sha256=$(sha256sum pom.xml | awk '{print $1}')"
} > "${EVIDENCE_DIR}/dependencies.txt" 2>&1

export SPARK_SHUFFLE_RECOVERY_TESTED_COMMIT="${CANDIDATE_SHA}"
cold_evidence_dir="${EVIDENCE_DIR}/cold-process"
if [[ "${module}" == sql && "${HARNESS}" == true ]]; then
  process_evidence=pending
  rm -rf "${cold_evidence_dir}"
  mkdir -p "${cold_evidence_dir}"
  export SPARK_SHUFFLE_RECOVERY_COLD_PROCESS_EVIDENCE_DIR="${cold_evidence_dir}"
  {
    echo "SPARK_SHUFFLE_RECOVERY_TESTED_COMMIT=${CANDIDATE_SHA}"
    echo "SPARK_SHUFFLE_RECOVERY_COLD_PROCESS_EVIDENCE_DIR=${cold_evidence_dir}"
  } >> "${EVIDENCE_DIR}/commands.txt"
fi

bash .github/scripts/shuffle-recovery-ci-suites.sh "${module}" \
  "${IDENTITY_SOURCE}" "${PROVIDER}" "${ATTEMPT}" "${SCHEDULER}" "${HARNESS}" > \
  "${EVIDENCE_DIR}/suites.txt"
if [[ ! -s "${EVIDENCE_DIR}/suites.txt" ]]; then
  echo "${module} routing selected no suites" >&2
  exit 1
fi
source_root="core"
profile=(-Phadoop-3)
project=core
if [[ "${module}" == sql ]]; then
  source_root="sql/core"
  profile=(-Phadoop-3 -Phive)
  project=sql
fi
while IFS= read -r suite; do
  bash .github/scripts/shuffle-recovery-ci-evidence.sh require-suite "${source_root}" "${suite}" >/dev/null
done < "${EVIDENCE_DIR}/suites.txt"

compile=failed
tests=skipped
proof=skipped
compile_command="${module}/Test/compile"
printf './build/sbt %s "show sbtVersion" "show scalaVersion" "%s"\n' \
  "${profile[*]}" "${compile_command}" >> "${EVIDENCE_DIR}/commands.txt"
set +e
./build/sbt "${profile[@]}" "show sbtVersion" "show scalaVersion" "${compile_command}" 2>&1 | \
  tee "${EVIDENCE_DIR}/compile.log"
compile_status=${PIPESTATUS[0]}
set -e
[[ ${compile_status} -ne 0 ]] || compile=success

if [[ "${compile}" == success ]]; then
  # Test reports are evidence for this invocation only. Delete any reports left by earlier local
  # runs before the command and require every selected report to be newer than this marker.
  echo "remove prior ${source_root} test reports before selected-suite execution" >> "${EVIDENCE_DIR}/commands.txt"
  find "${source_root}" -type f -path '*/test-reports/*.xml' -delete 2>/dev/null || true
  test_start="${EVIDENCE_DIR}/test-start.marker"
  : > "${test_start}"
  test_command="${module}/testOnly"
  while IFS= read -r suite; do test_command+=" ${suite}"; done < "${EVIDENCE_DIR}/suites.txt"
  printf './build/sbt %s "%s"\n' "${profile[*]}" "${test_command}" >> "${EVIDENCE_DIR}/commands.txt"
  set +e
  ./build/sbt "${profile[@]}" "${test_command}" 2>&1 | tee "${EVIDENCE_DIR}/tests.log"
  test_status=${PIPESTATUS[0]}
  set -e
  report_status=0
  while IFS= read -r suite; do
    short="${suite##*.}"
    exact="TEST-${suite}.xml"
    mapfile -d '' candidates < <(
      find "${source_root}" -type f -path '*/test-reports/*' -name "${exact}" -print0 \
        2>/dev/null | sort -z)
    if [[ ${#candidates[@]} -eq 1 ]]; then
      cp "${candidates[0]}" "${EVIDENCE_DIR}/reports/${short}.xml"
    elif [[ ${#candidates[@]} -gt 1 ]]; then
      index=0
      for candidate_report in "${candidates[@]}"; do
        cp "${candidate_report}" "${EVIDENCE_DIR}/reports/${short}.candidate-${index}.xml"
        index=$((index + 1))
      done
    fi
    report="$(bash .github/scripts/shuffle-recovery-ci-evidence.sh \
      locate-report "${source_root}" "${suite}" "${test_start}" \
      2>>"${EVIDENCE_DIR}/missing-reports.log")" || {
        report_status=1
        continue
      }
    echo "validate exact fresh report for ${suite}" >> "${EVIDENCE_DIR}/commands.txt"
    bash .github/scripts/shuffle-recovery-ci-evidence.sh validate-report \
      "${report}" "${suite}" > "${EVIDENCE_DIR}/reports/${short}.summary" \
      2> "${EVIDENCE_DIR}/reports/${short}.validation.log" || report_status=1
  done < "${EVIDENCE_DIR}/suites.txt"

  if [[ "${module}" == sql && "${HARNESS}" == true ]]; then
    echo "validate cold/healing process evidence for ${CANDIDATE_SHA}" >> "${EVIDENCE_DIR}/commands.txt"
    set +e
    bash .github/scripts/shuffle-recovery-ci-evidence.sh validate-cold-process \
      "${cold_evidence_dir}" "${CANDIDATE_SHA}" > \
      "${EVIDENCE_DIR}/cold-process-validation.txt" 2>&1
    cold_status=$?
    set -e
    if [[ ${cold_status} -eq 0 ]]; then
      process_evidence=success
    else
      process_evidence=failed
      report_status=1
    fi
  fi

  if [[ ${test_status} -eq 0 && ${report_status} -eq 0 ]]; then tests=success; else tests=failed; fi
fi

if [[ "${compile}" == success && "${tests}" == success ]]; then
  proof=success
  if [[ "${module}" == core && "${IDENTITY_SOURCE}" == true ]]; then
    vector="${RUNNER_TEMP:-/tmp}/shuffle-recovery-computation-identity.bin"
    rm -f "${vector}"
    echo 'Core identity independent-JVM write/compare proof' >> "${EVIDENCE_DIR}/commands.txt"
    set +e
    ./build/sbt "${profile[@]}" "project ${project}" "set Test / run / fork := true" \
      "Test/runMain org.apache.spark.shuffle.ShuffleRecoveryComputationIdentityProcess write ${vector}" \
      "Test/runMain org.apache.spark.shuffle.ShuffleRecoveryComputationIdentityProcess compare ${vector}" \
      2>&1 | tee "${EVIDENCE_DIR}/identity-process.log"
    status=${PIPESTATUS[0]}
    set -e
    if [[ ${status} -ne 0 || ! -s "${vector}" ]]; then proof=failed; else sha256sum "${vector}" > "${EVIDENCE_DIR}/identity-vector.sha256"; fi
  fi
  if [[ "${module}" == core && "${PROVIDER}" == true ]]; then
    proof_root="${RUNNER_TEMP:-/tmp}/shuffle-recovery-reference-provider-${GITHUB_RUN_ID:-local}"
    report="${EVIDENCE_DIR}/reference-provider-proof.md"
    rm -rf "${proof_root}"
    echo 'Reference provider independent-JVM write/read proof' >> "${EVIDENCE_DIR}/commands.txt"
    set +e
    ./build/sbt "${profile[@]}" "project ${project}" "set Test / run / fork := true" \
      "Test/runMain org.apache.spark.shuffle.ReferenceShuffleProviderProcess write ${proof_root}" \
      "Test/runMain org.apache.spark.shuffle.ReferenceShuffleProviderProcess read ${proof_root} ${report}" \
      2>&1 | tee "${EVIDENCE_DIR}/provider-process.log"
    status=${PIPESTATUS[0]}
    set -e
    if [[ ${status} -ne 0 || ! -s "${report}" || \
          -n "$(find "${proof_root}" -mindepth 1 -print -quit 2>/dev/null)" ]]; then proof=failed; fi
  fi
  if [[ "${module}" == sql && "${IDENTITY_SOURCE}" == true ]]; then
    first="${RUNNER_TEMP:-/tmp}/shuffle-recovery-source-first.txt"
    second="${RUNNER_TEMP:-/tmp}/shuffle-recovery-source-second.txt"
    rm -f "${first}" "${second}"
    echo 'Source identity independent-JVM stability proof' >> "${EVIDENCE_DIR}/commands.txt"
    set +e
    ./build/sbt "${profile[@]}" "project ${project}" "set Test / run / fork := true" \
      "Test/runMain org.apache.spark.sql.execution.exchange.ShuffleRecoverySourceReadIdentityProcess ${first}" \
      "Test/runMain org.apache.spark.sql.execution.exchange.ShuffleRecoverySourceReadIdentityProcess ${second}" \
      2>&1 | tee "${EVIDENCE_DIR}/source-identity-process.log"
    status=${PIPESTATUS[0]}
    set -e
    if [[ ${status} -ne 0 || ! -s "${first}" || ! -s "${second}" ]] || ! cmp "${first}" "${second}"; then
      proof=failed
    else
      sha256sum "${first}" > "${EVIDENCE_DIR}/source-identity.sha256"
    fi
  fi
fi

if [[ "${module}" == sql && "${SHARED_FILESYSTEM_PROOF:-false}" == true &&
      "${compile}" == success && "${tests}" == success && "${proof}" == success ]]; then
  proof=failed
  EVIDENCE_DIR="${EVIDENCE_DIR}/shared-filesystem" \
    bash dev/shuffle-recovery/shared-filesystem/run.sh
  proof=success
fi

write_status
bash .github/scripts/shuffle-recovery-ci-evidence.sh checksum-tree "${EVIDENCE_DIR}"
finalized=true
[[ "${compile}" == success && "${tests}" == success && "${proof}" == success ]]
