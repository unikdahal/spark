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

: "${CANDIDATE_SHA:?}"
: "${BASE_SHA:?}"
: "${VALIDATION_MODE:?}"
: "${RUN_STYLE:?}"
: "${RUN_LICENSE:?}"
: "${SCALA_CHANGED:?}"
: "${JAVA_CHANGED:?}"
: "${WORKFLOW_BUILD:?}"
: "${EVIDENCE_DIR:?}"

actual="$(git rev-parse HEAD)"
if [[ "${actual}" != "${CANDIDATE_SHA}" ]]; then
  echo "quality checkout ${actual} does not match candidate ${CANDIDATE_SHA}" >&2
  exit 1
fi

mkdir -p "${EVIDENCE_DIR}"
{
  echo "candidate_kind=experimental-tree"
  echo "candidate_sha=${actual}"
  echo "integration_base_sha=${BASE_SHA}"
  echo "validation_mode=${VALIDATION_MODE}"
  echo "workflow_run_id=${GITHUB_RUN_ID:-local}"
} > "${EVIDENCE_DIR}/provenance.env"
{
  java -version
  sed 's/^/build_property=/' project/build.properties
  echo "root_pom_sha256=$(sha256sum pom.xml | awk '{print $1}')"
} > "${EVIDENCE_DIR}/dependencies.txt" 2>&1
: > "${EVIDENCE_DIR}/commands.txt"

lint=skipped
if [[ "${RUN_STYLE}" == true ]]; then
  lint=success
  if [[ "${VALIDATION_MODE}" == full || "${VALIDATION_MODE}" == evidence || \
        "${SCALA_CHANGED}" == true || "${WORKFLOW_BUILD}" == true ]]; then
    echo './dev/scalastyle "-Phadoop-3 -Phive"' >> "${EVIDENCE_DIR}/commands.txt"
    set +e
    timeout --signal=TERM 20m ./dev/scalastyle "-Phadoop-3 -Phive" 2>&1 | \
      tee "${EVIDENCE_DIR}/scalastyle.log"
    status=${PIPESTATUS[0]}
    set -e
    [[ ${status} -eq 0 ]] || lint=failed
  fi
  if [[ "${VALIDATION_MODE}" == full || "${VALIDATION_MODE}" == evidence || \
        "${JAVA_CHANGED}" == true ]]; then
    echo './dev/lint-java' >> "${EVIDENCE_DIR}/commands.txt"
    set +e
    ./dev/lint-java 2>&1 | tee "${EVIDENCE_DIR}/java-lint.log"
    status=${PIPESTATUS[0]}
    set -e
    [[ ${status} -eq 0 ]] || lint=failed
  fi
fi

license=skipped
if [[ "${RUN_LICENSE}" == true ]]; then
  echo './dev/check-license' >> "${EVIDENCE_DIR}/commands.txt"
  set +e
  ./dev/check-license 2>&1 | tee "${EVIDENCE_DIR}/license.log"
  status=${PIPESTATUS[0]}
  set -e
  if [[ ${status} -eq 0 ]]; then license=success; else license=failed; fi
fi

{
  echo "lint=${lint}"
  echo "license=${license}"
} > "${EVIDENCE_DIR}/status.env"
bash .github/scripts/shuffle-recovery-ci-evidence.sh checksum-tree "${EVIDENCE_DIR}"

failed=false
[[ "${RUN_STYLE}" != true || "${lint}" == success ]] || failed=true
[[ "${RUN_LICENSE}" != true || "${license}" == success ]] || failed=true
[[ "${failed}" == false ]]
