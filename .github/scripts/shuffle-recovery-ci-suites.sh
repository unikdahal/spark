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
identity_source="${2:-false}"
provider_publication_discovery="${3:-false}"
attempt_lifecycle="${4:-false}"
scheduler_tracker_reader_aqe_invalidation="${5:-false}"
harness="${6:-false}"

case "${module}" in core|sql) ;; *) echo "unsupported suite module: ${module:-<empty>}" >&2; exit 2 ;; esac
for value in "${identity_source}" "${provider_publication_discovery}" "${attempt_lifecycle}" \
    "${scheduler_tracker_reader_aqe_invalidation}" "${harness}"; do
  case "${value}" in true|false) ;; *) echo "suite selector requires boolean inputs" >&2; exit 2 ;; esac
done

suites=()
declare -A seen=()
add_suite() {
  local suite="$1"
  if [[ -z "${seen[${suite}]+x}" ]]; then
    suites+=("${suite}")
    seen["${suite}"]=1
  fi
}

if [[ "${module}" == core ]]; then
  if [[ "${identity_source}" == true ]]; then
    add_suite org.apache.spark.shuffle.ShuffleRecoveryComputationIdentitySuite
  fi

  if [[ "${provider_publication_discovery}" == true ]]; then
    add_suite org.apache.spark.shuffle.ReferenceShuffleProviderSuite
    add_suite org.apache.spark.shuffle.ShuffleRecoveryManifestSuite
    add_suite org.apache.spark.shuffle.ShuffleRecoveryNativeManifestSuite
    add_suite org.apache.spark.shuffle.ShuffleRecoveryPublicationAttemptsSuite
    add_suite org.apache.spark.shuffle.sort.IndexShuffleBlockResolverSuite
    add_suite org.apache.spark.shuffle.sort.SortShuffleManagerSuite
  fi

  if [[ "${attempt_lifecycle}" == true ]]; then
    # Attempt/lifecycle changes cross the provider binding and scheduler-adoption boundaries, so
    # both integrated regressions are required.
    add_suite org.apache.spark.shuffle.ReferenceShuffleProviderSuite
    add_suite org.apache.spark.shuffle.ShuffleRecoverySchedulerAdoptionSuite
  fi

  if [[ "${scheduler_tracker_reader_aqe_invalidation}" == true ]]; then
    add_suite org.apache.spark.shuffle.ShuffleRecoverySchedulerAdoptionSuite
    add_suite org.apache.spark.shuffle.ShuffleRecoveryAdoptedFailureSuite
    add_suite org.apache.spark.shuffle.ShuffleRecoveryNativeAdoptionSuite
    add_suite org.apache.spark.scheduler.MapStatusSuite
    add_suite org.apache.spark.scheduler.DAGSchedulerSuite
    add_suite org.apache.spark.MapOutputTrackerSuite
    add_suite org.apache.spark.shuffle.BlockStoreShuffleReaderSuite
  fi
else
  if [[ "${identity_source}" == true ]]; then
    add_suite org.apache.spark.sql.execution.exchange.ShuffleRecoverySourceReadIdentitySuite
    add_suite org.apache.spark.sql.execution.exchange.ShuffleRecoveryComputationIdentityBuilderSuite
    add_suite org.apache.spark.sql.execution.exchange.ShuffleRecoveryOpportunityAnalyzerSuite
  fi

  if [[ "${scheduler_tracker_reader_aqe_invalidation}" == true ]]; then
    add_suite org.apache.spark.sql.execution.exchange.ShuffleRecoveryAdaptivePartitionRulesSuite
  fi

  if [[ "${harness}" == true ]]; then
    add_suite org.apache.spark.shuffle.ShuffleRecoveryColdProcessSuite
  fi
fi

printf '%s\n' "${suites[@]}"
