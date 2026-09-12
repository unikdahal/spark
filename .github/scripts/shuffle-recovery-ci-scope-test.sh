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
evidence="${script_dir}/shuffle-recovery-ci-evidence.sh"
suites="${script_dir}/shuffle-recovery-ci-suites.sh"

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
    printf '%s\n' "${output}" >&2
    exit 1
  fi
}

route() { bash "${router}" "$@"; }

identity="$(route focused \
  core/src/main/scala/org/apache/spark/shuffle/ShuffleRecoveryComputationIdentity.scala \
  sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleRecoverySourceReadIdentity.scala)"
expect "${identity}" identity_source true
expect "${identity}" provider_publication_discovery false
expect "${identity}" run_core true
expect "${identity}" run_sql true
expect "${identity}" run_harness false

provider="$(route focused core/src/main/scala/org/apache/spark/shuffle/ReferenceShuffleProvider.scala)"
expect "${provider}" provider_publication_discovery true
expect "${provider}" identity_source false
expect "${provider}" run_core true
expect "${provider}" run_sql true

attempt="$(route focused core/src/main/scala/org/apache/spark/shuffle/ShuffleRecoveryPreparation.scala)"
expect "${attempt}" attempt_lifecycle true
expect "${attempt}" provider_publication_discovery true
expect "${attempt}" run_core true

scheduler="$(route focused core/src/main/scala/org/apache/spark/MapOutputTracker.scala)"
expect "${scheduler}" scheduler_tracker_reader_aqe_invalidation true
expect "${scheduler}" run_core true
expect "${scheduler}" run_sql true
expect "${scheduler}" identity_source false

harness="$(route focused sql/core/src/test/scala/org/apache/spark/shuffle/ShuffleRecoveryColdProcessSuite.scala)"
expect "${harness}" harness true
expect "${harness}" run_harness true
expect "${harness}" run_core false

hot="$(route focused \
  core/src/main/scala/org/apache/spark/shuffle/ReferenceShuffleRecoveryClaimProvider.scala \
  core/src/main/scala/org/apache/spark/ShuffleRecoverySchedulerAdoption.scala)"
expect "${hot}" provider_publication_discovery true
expect "${hot}" attempt_lifecycle true
expect "${hot}" scheduler_tracker_reader_aqe_invalidation true
expect "${hot}" run_core true
expect "${hot}" run_sql true

docs="$(route focused docs/shuffle-recovery/experimental/implementation-inventory.md)"
expect "${docs}" docs_only true
expect "${docs}" run_style false
expect "${docs}" run_license false
expect "${docs}" run_core false
expect "${docs}" run_sql false
expect "${docs}" run_harness false

unknown="$(route focused core/src/main/scala/org/apache/spark/FutureRecoveryBoundary.scala)"
expect "${unknown}" unknown_core_sql true
expect "${unknown}" identity_source true
expect "${unknown}" provider_publication_discovery true
expect "${unknown}" attempt_lifecycle true
expect "${unknown}" scheduler_tracker_reader_aqe_invalidation true
expect "${unknown}" harness true

workflow="$(route focused .github/workflows/shuffle-recovery-phase0.yml)"
expect "${workflow}" workflow_build true
expect "${workflow}" identity_source true
expect "${workflow}" provider_publication_discovery true
expect "${workflow}" attempt_lifecycle true
expect "${workflow}" scheduler_tracker_reader_aqe_invalidation true
expect "${workflow}" harness true
expect "${workflow}" run_style true
expect "${workflow}" run_license true

full="$(route full docs/shuffle-recovery/experimental/implementation-inventory.md)"
expect "${full}" validation_mode full
expect "${full}" identity_source true
expect "${full}" provider_publication_discovery true
expect "${full}" attempt_lifecycle true
expect "${full}" scheduler_tracker_reader_aqe_invalidation true
expect "${full}" harness true
expect "${full}" manual_evidence false

manual="$(route evidence docs/shuffle-recovery/experimental/implementation-inventory.md)"
expect "${manual}" validation_mode evidence
expect "${manual}" identity_source true
expect "${manual}" provider_publication_discovery true
expect "${manual}" scheduler_tracker_reader_aqe_invalidation true
expect "${manual}" harness true
expect "${manual}" manual_evidence true

core_scheduler_suites="$(bash "${suites}" core false false false true false)"
for required in \
  org.apache.spark.shuffle.ShuffleRecoverySchedulerAdoptionSuite \
  org.apache.spark.shuffle.ShuffleRecoveryAdoptedFailureSuite \
  org.apache.spark.scheduler.DAGSchedulerSuite \
  org.apache.spark.MapOutputTrackerSuite \
  org.apache.spark.shuffle.BlockStoreShuffleReaderSuite; do
  if ! grep -Fxq "${required}" <<< "${core_scheduler_suites}"; then
    echo "scheduler boundary omitted required suite ${required}" >&2
    exit 1
  fi
done
if grep -Fq 'ShuffleRecoveryComputationIdentitySuite' <<< "${core_scheduler_suites}"; then
  echo "scheduler-only boundary unexpectedly reduced to/selected identity coverage" >&2
  exit 1
fi

all_core_suites="$(bash "${suites}" core true true true true true)"
if [[ "$(grep -Fxc 'org.apache.spark.shuffle.ReferenceShuffleProviderSuite' <<< "${all_core_suites}")" != 1 ]]; then
  echo "suite selector did not deduplicate overlapping boundary coverage" >&2
  exit 1
fi

all_sql_suites="$(bash "${suites}" sql true true true true true)"
for required in \
  org.apache.spark.sql.execution.exchange.ShuffleRecoverySourceReadIdentitySuite \
  org.apache.spark.sql.execution.exchange.ShuffleRecoveryAdaptivePartitionRulesSuite \
  org.apache.spark.shuffle.ShuffleRecoveryColdProcessSuite; do
  if ! grep -Fxq "${required}" <<< "${all_sql_suites}"; then
    echo "SQL/harness boundary omitted required suite ${required}" >&2
    exit 1
  fi
done

sha=0123456789abcdef0123456789abcdef01234567
focused_mode="$(bash "${evidence}" resolve-mode pull_request feature-branch "${sha}")"
expect "${focused_mode}" validation_mode focused
full_mode="$(bash "${evidence}" resolve-mode push "shuffle-recovery-ci/full/${sha}" "${sha}")"
expect "${full_mode}" validation_mode full
evidence_mode="$(bash "${evidence}" resolve-mode push "shuffle-recovery-ci/evidence/${sha}" "${sha}")"
expect "${evidence_mode}" validation_mode evidence
if bash "${evidence}" resolve-mode push shuffle-recovery-ci/full/deadbeef deadbeef >/dev/null 2>&1; then
  echo "malformed full tag unexpectedly succeeded" >&2
  exit 1
fi
bash "${evidence}" validate-candidate full "${sha}" "${sha}"
bash "${evidence}" validate-candidate evidence "${sha}" "${sha}"
if bash "${evidence}" validate-candidate full deadbeef deadbeef >/dev/null 2>&1; then
  echo "malformed full-mode SHA unexpectedly succeeded" >&2
  exit 1
fi
if bash "${evidence}" validate-candidate evidence "${sha}" f123456789abcdef0123456789abcdef01234567 >/dev/null 2>&1; then
  echo "mismatched evidence candidate unexpectedly succeeded" >&2
  exit 1
fi

fixture="$(mktemp -d)"
trap 'rm -rf "${fixture}"' EXIT
valid_root="${fixture}/valid"
mkdir -p "${valid_root}/core/src/test/scala/org/apache/spark/shuffle" \
  "${valid_root}/core/target/test-reports"
touch "${valid_root}/core/src/test/scala/org/apache/spark/shuffle/PresentSuite.scala"
cat > "${valid_root}/core/target/test-reports/TEST-org.apache.spark.shuffle.PresentSuite.xml" <<'XML'
<testsuite name="org.apache.spark.shuffle.PresentSuite" tests="1" errors="0" failures="0" skipped="0">
  <testcase classname="org.apache.spark.shuffle.PresentSuite" name="executes"/>
</testsuite>
XML
fresh_marker="${valid_root}/fresh.marker"
touch -d '2000-01-01 UTC' "${fresh_marker}"
(
  cd "${valid_root}"
  bash "${evidence}" require-suite core org.apache.spark.shuffle.PresentSuite >/dev/null
  bash "${evidence}" require-report core org.apache.spark.shuffle.PresentSuite "${fresh_marker}" >/dev/null
  if bash "${evidence}" require-suite core org.apache.spark.shuffle.MissingSuite >/dev/null 2>&1; then
    echo "missing suite unexpectedly succeeded" >&2
    exit 1
  fi
  if bash "${evidence}" require-report core org.apache.spark.shuffle.MissingSuite "${fresh_marker}" >/dev/null 2>&1; then
    echo "missing test report unexpectedly succeeded" >&2
    exit 1
  fi
)

skip_report="${fixture}/map-output-tracker.xml"
cat > "${skip_report}" <<'XML'
<testsuite name="org.apache.spark.MapOutputTrackerSuite" tests="2" errors="0" failures="0" skipped="1">
  <testcase classname="org.apache.spark.MapOutputTrackerSuite" name="executes"/>
  <testcase classname="org.apache.spark.MapOutputTrackerSuite" name="SPARK-32210: serialize and deserialize over 2GB compressed mapStatuses"><skipped/></testcase>
</testsuite>
XML
bash "${evidence}" validate-report "${skip_report}" org.apache.spark.MapOutputTrackerSuite >/dev/null

wrong_known_skip="${fixture}/wrong-known-skip.xml"
cat > "${wrong_known_skip}" <<'XML'
<testsuite name="org.apache.spark.MapOutputTrackerSuite" tests="2" errors="0" failures="0" skipped="1">
  <testcase classname="org.apache.spark.MapOutputTrackerSuite" name="executes"/>
  <testcase classname="org.apache.spark.MapOutputTrackerSuite" name="different ignored case"><skipped/></testcase>
</testsuite>
XML
if bash "${evidence}" validate-report "${wrong_known_skip}" org.apache.spark.MapOutputTrackerSuite >/dev/null 2>&1; then
  echo "different MapOutputTracker ignored case unexpectedly succeeded" >&2
  exit 1
fi

unexpected_skip="${fixture}/unexpected-skip.xml"
cat > "${unexpected_skip}" <<'XML'
<testsuite name="org.apache.spark.shuffle.PresentSuite" tests="2" errors="0" failures="0" skipped="1">
  <testcase classname="org.apache.spark.shuffle.PresentSuite" name="executes"/>
  <testcase classname="org.apache.spark.shuffle.PresentSuite" name="ignored"><skipped/></testcase>
</testsuite>
XML
if bash "${evidence}" validate-report "${unexpected_skip}" org.apache.spark.shuffle.PresentSuite >/dev/null 2>&1; then
  echo "unapproved skipped test unexpectedly succeeded" >&2
  exit 1
fi

wrong_name="${fixture}/wrong-name.xml"
cat > "${wrong_name}" <<'XML'
<testsuite name="org.apache.spark.shuffle.OtherSuite" tests="1" errors="0" failures="0" skipped="0">
  <testcase classname="org.apache.spark.shuffle.OtherSuite" name="executes"/>
</testsuite>
XML
if bash "${evidence}" validate-report "${wrong_name}" org.apache.spark.shuffle.PresentSuite >/dev/null 2>&1; then
  echo "wrong report suite identity unexpectedly succeeded" >&2
  exit 1
fi

wrong_class="${fixture}/wrong-class.xml"
cat > "${wrong_class}" <<'XML'
<testsuite name="org.apache.spark.shuffle.PresentSuite" tests="1" errors="0" failures="0" skipped="0">
  <testcase classname="org.apache.spark.shuffle.OtherSuite" name="executes"/>
</testsuite>
XML
if bash "${evidence}" validate-report "${wrong_class}" org.apache.spark.shuffle.PresentSuite >/dev/null 2>&1; then
  echo "wrong testcase classname unexpectedly succeeded" >&2
  exit 1
fi

empty_xml="${fixture}/empty.xml"
: > "${empty_xml}"
if bash "${evidence}" validate-report "${empty_xml}" org.apache.spark.shuffle.PresentSuite >/dev/null 2>&1; then
  echo "empty XML unexpectedly succeeded" >&2
  exit 1
fi

zero_report="${fixture}/zero.xml"
printf '%s\n' '<testsuite name="org.apache.spark.shuffle.PresentSuite" tests="0" errors="0" failures="0" skipped="0"/>' > "${zero_report}"
if bash "${evidence}" validate-report "${zero_report}" org.apache.spark.shuffle.PresentSuite >/dev/null 2>&1; then
  echo "zero-execution report unexpectedly succeeded" >&2
  exit 1
fi

all_skipped="${fixture}/all-skipped.xml"
cat > "${all_skipped}" <<'XML'
<testsuite name="org.apache.spark.MapOutputTrackerSuite" tests="1" errors="0" failures="0" skipped="1">
  <testcase classname="org.apache.spark.MapOutputTrackerSuite" name="SPARK-32210: serialize and deserialize over 2GB compressed mapStatuses"><skipped/></testcase>
</testsuite>
XML
if bash "${evidence}" validate-report "${all_skipped}" org.apache.spark.MapOutputTrackerSuite >/dev/null 2>&1; then
  echo "all-skipped report unexpectedly succeeded" >&2
  exit 1
fi

invalid_xml="${fixture}/invalid.xml"
printf '%s\n' '<testsuite' > "${invalid_xml}"
if bash "${evidence}" validate-report "${invalid_xml}" org.apache.spark.shuffle.PresentSuite >/dev/null 2>&1; then
  echo "invalid XML unexpectedly succeeded" >&2
  exit 1
fi

stale_root="${fixture}/stale"
mkdir -p "${stale_root}/core/target/test-reports"
cp "${valid_root}/core/target/test-reports/TEST-org.apache.spark.shuffle.PresentSuite.xml" \
  "${stale_root}/core/target/test-reports/TEST-org.apache.spark.shuffle.PresentSuite.xml"
touch -d '2000-01-01 UTC' "${stale_root}/core/target/test-reports/TEST-org.apache.spark.shuffle.PresentSuite.xml"
touch "${stale_root}/marker"
if bash "${evidence}" require-report "${stale_root}/core" \
    org.apache.spark.shuffle.PresentSuite "${stale_root}/marker" >/dev/null 2>&1; then
  echo "stale test report unexpectedly succeeded" >&2
  exit 1
fi

ambiguous_root="${fixture}/ambiguous/core"
mkdir -p "${ambiguous_root}/one/test-reports" "${ambiguous_root}/two/test-reports"
cp "${valid_root}/core/target/test-reports/TEST-org.apache.spark.shuffle.PresentSuite.xml" \
  "${ambiguous_root}/one/test-reports/TEST-org.apache.spark.shuffle.PresentSuite.xml"
cp "${valid_root}/core/target/test-reports/TEST-org.apache.spark.shuffle.PresentSuite.xml" \
  "${ambiguous_root}/two/test-reports/TEST-org.apache.spark.shuffle.PresentSuite.xml"
if bash "${evidence}" locate-report "${ambiguous_root}" \
    org.apache.spark.shuffle.PresentSuite >/dev/null 2>&1; then
  echo "ambiguous exact reports unexpectedly succeeded" >&2
  exit 1
fi

missing_cold="${fixture}/missing-cold"
mkdir -p "${missing_cold}/run-partial" "${missing_cold}/adopted-failure-run-partial"
if bash "${evidence}" validate-cold-process "${missing_cold}" "${sha}" >/dev/null 2>&1; then
  echo "missing cold/healing child evidence unexpectedly succeeded" >&2
  exit 1
fi

if route unsupported docs/README.md >/dev/null 2>&1; then
  echo "unsupported validation mode unexpectedly succeeded" >&2
  exit 1
fi

echo "shuffle recovery CI routing and evidence contract: PASS"
