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

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
router="${script_dir}/shuffle-recovery-ci-scope.sh"

value() {
  local output="$1"
  local key="$2"
  printf '%s\n' "${output}" | sed -n "s/^${key}=//p"
}

expect() {
  local output="$1"
  local key="$2"
  local expected="$3"
  local actual
  actual="$(value "${output}" "${key}")"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "expected ${key}=${expected}, got ${actual:-<missing>}" >&2
    echo "router output:" >&2
    printf '%s\n' "${output}" >&2
    exit 1
  fi
}

route() {
  bash "${router}" "$@"
}

analyzer="$(route focused opportunity \
  sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleRecoveryOpportunityAnalyzer.scala)"
expect "${analyzer}" run_opportunity true
expect "${analyzer}" run_core false
expect "${analyzer}" run_cold_process false

provider="$(route focused opportunity \
  core/src/main/scala/org/apache/spark/shuffle/ReferenceShuffleProvider.scala)"
expect "${provider}" run_core true
expect "${provider}" core_provider true
expect "${provider}" run_opportunity false
expect "${provider}" run_cold_process false

claim="$(route focused opportunity \
  core/src/main/scala/org/apache/spark/shuffle/ReferenceShuffleRecoveryClaimProvider.scala)"
expect "${claim}" claim true
expect "${claim}" core_provider true
expect "${claim}" run_core true
expect "${claim}" run_opportunity false
expect "${claim}" run_cold_process false

scheduler="$(route focused opportunity \
  core/src/main/scala/org/apache/spark/scheduler/ShuffleMapStage.scala)"
expect "${scheduler}" scheduler true
expect "${scheduler}" run_core true
expect "${scheduler}" run_opportunity false

cold="$(route focused opportunity \
  sql/core/src/test/scala/org/apache/spark/shuffle/ShuffleRecoveryColdProcessSuite.scala)"
expect "${cold}" cold_process true
expect "${cold}" run_cold_process true
expect "${cold}" run_opportunity false
expect "${cold}" run_core false

docs="$(route focused opportunity docs/shuffle-recovery/phase0/README.md)"
expect "${docs}" docs_only true
expect "${docs}" run_style false
expect "${docs}" run_core false
expect "${docs}" run_opportunity false
expect "${docs}" run_cold_process false

workflow="$(route focused opportunity .github/workflows/shuffle-recovery-phase0.yml)"
expect "${workflow}" workflow true
expect "${workflow}" run_style true
expect "${workflow}" run_core true
expect "${workflow}" run_opportunity true
expect "${workflow}" run_cold_process true

full="$(route full opportunity docs/shuffle-recovery/phase0/README.md)"
expect "${full}" validation_mode full
expect "${full}" scala true
expect "${full}" run_style true
expect "${full}" run_core true
expect "${full}" run_opportunity true
expect "${full}" run_cold_process true

evidence_opportunity="$(route evidence opportunity docs/shuffle-recovery/phase0/README.md)"
expect "${evidence_opportunity}" run_core false
expect "${evidence_opportunity}" run_opportunity true
expect "${evidence_opportunity}" run_cold_process false
expect "${evidence_opportunity}" run_evidence_opportunity true

evidence_cold="$(route evidence cold-process docs/shuffle-recovery/phase0/README.md)"
expect "${evidence_cold}" cold_process true
expect "${evidence_cold}" run_core false
expect "${evidence_cold}" run_opportunity false
expect "${evidence_cold}" run_cold_process true
expect "${evidence_cold}" run_evidence_cold_process true

evidence_all="$(route evidence all docs/shuffle-recovery/phase0/README.md)"
expect "${evidence_all}" cold_process true
expect "${evidence_all}" run_opportunity true
expect "${evidence_all}" run_cold_process true
expect "${evidence_all}" run_evidence_opportunity true
expect "${evidence_all}" run_evidence_cold_process true

unknown_source="$(route focused opportunity core/src/main/scala/org/apache/spark/FutureRecoveryHotPath.scala)"
expect "${unknown_source}" core_other true
expect "${unknown_source}" run_core true

if route unsupported opportunity docs/shuffle-recovery/README.md >/dev/null 2>&1; then
  echo "unsupported validation mode unexpectedly succeeded" >&2
  exit 1
fi
if route evidence unsupported docs/shuffle-recovery/README.md >/dev/null 2>&1; then
  echo "unsupported evidence target unexpectedly succeeded" >&2
  exit 1
fi

echo "shuffle recovery CI routing contract: PASS"
