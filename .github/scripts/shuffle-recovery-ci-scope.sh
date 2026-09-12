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

mode="${1:-focused}"
shift $(( $# > 0 ? 1 : 0 ))

case "${mode}" in
  focused|full|evidence) ;;
  *)
    echo "unsupported validation mode: ${mode}" >&2
    exit 2
    ;;
esac

paths=()
if [[ $# -gt 0 ]]; then
  paths=("$@")
else
  while IFS= read -r path; do
    [[ -n "${path}" ]] && paths+=("${path}")
  done
fi

workflow_build=false
docs=false
non_docs=false
scala=false
java=false
identity_source=false
provider_publication_discovery=false
attempt_lifecycle=false
scheduler_tracker_reader_aqe_invalidation=false
harness=false
unknown_core_sql=false

mark_all_boundaries() {
  identity_source=true
  provider_publication_discovery=true
  attempt_lifecycle=true
  scheduler_tracker_reader_aqe_invalidation=true
  harness=true
}

for path in "${paths[@]}"; do
  case "${path}" in
    *.scala) scala=true ;;
    *.java) java=true ;;
  esac

  case "${path}" in
    docs/shuffle-recovery/*.java|docs/shuffle-recovery/*.sh|docs/shuffle-recovery/*.py)
      non_docs=true
      workflow_build=true
      mark_all_boundaries
      ;;

    docs/*|*.md)
      docs=true
      ;;

    .github/workflows/shuffle-recovery-phase0.yml|\
    .github/scripts/shuffle-recovery-ci-*|\
    dev/*|project/*|build/*|pom.xml|*/pom.xml|*.sbt)
      workflow_build=true
      non_docs=true
      mark_all_boundaries
      ;;

    core/src/main/scala/org/apache/spark/shuffle/ShuffleRecoveryComputationIdentity*|\
    core/src/test/scala/org/apache/spark/shuffle/ShuffleRecoveryComputationIdentity*|\
    sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleRecoveryComputationIdentityBuilder*|\
    sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleRecoverySourceReadIdentity*|\
    sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleRecoveryOpportunity*|\
    sql/core/src/test/scala/org/apache/spark/sql/execution/exchange/ShuffleRecoveryComputationIdentityBuilder*|\
    sql/core/src/test/scala/org/apache/spark/sql/execution/exchange/ShuffleRecoverySourceReadIdentity*|\
    sql/core/src/test/scala/org/apache/spark/sql/execution/exchange/ShuffleRecoveryOpportunity*)
      identity_source=true
      non_docs=true
      ;;

    core/src/main/scala/org/apache/spark/shuffle/ReferenceShuffleProvider*|\
    core/src/test/scala/org/apache/spark/shuffle/ReferenceShuffleProvider*|\
    core/src/main/scala/org/apache/spark/shuffle/ShuffleRecoveryManifest*|\
    core/src/test/scala/org/apache/spark/shuffle/ShuffleRecoveryManifest*|\
    core/src/main/scala/org/apache/spark/shuffle/ShuffleRecoveryClaim*|\
    core/src/main/scala/org/apache/spark/shuffle/ShuffleRecoveryUntrustedBoundary*|\
    core/src/main/scala/org/apache/spark/shuffle/ShuffleRecoveryProvenance*|\
    core/src/main/scala/org/apache/spark/shuffle/ShuffleRecoveryIndexShuffleBlockResolver.scala|\
    core/src/main/scala/org/apache/spark/shuffle/IndexShuffleBlockResolver.scala|\
    core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleManager.scala|\
    core/src/test/scala/org/apache/spark/shuffle/sort/IndexShuffleBlockResolverSuite.scala|\
    core/src/test/scala/org/apache/spark/shuffle/sort/SortShuffleManagerSuite.scala)
      provider_publication_discovery=true
      scheduler_tracker_reader_aqe_invalidation=true
      non_docs=true
      ;;

    core/src/main/scala/org/apache/spark/shuffle/ShuffleRecoveryAttempt*|\
    core/src/test/scala/org/apache/spark/shuffle/ShuffleRecoveryAttempt*|\
    core/src/main/scala/org/apache/spark/shuffle/ShuffleRecoveryPreparation*|\
    core/src/main/scala/org/apache/spark/shuffle/ReferenceShuffleRecoveryClaimProvider.scala)
      attempt_lifecycle=true
      provider_publication_discovery=true
      scheduler_tracker_reader_aqe_invalidation=true
      non_docs=true
      ;;

    core/src/main/scala/org/apache/spark/ShuffleRecoverySchedulerAdoption*|\
    core/src/main/scala/org/apache/spark/shuffle/ShuffleRecoveryAdoptedFailure*|\
    core/src/main/scala/org/apache/spark/shuffle/ShuffleRecoveryReadState*|\
    core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala|\
    core/src/main/scala/org/apache/spark/scheduler/ShuffleMapStage.scala|\
    core/src/main/scala/org/apache/spark/MapOutputTracker.scala|\
    core/src/main/scala/org/apache/spark/shuffle/BlockStoreShuffleReader.scala|\
    core/src/test/scala/org/apache/spark/shuffle/ShuffleRecoverySchedulerAdoptionSuite.scala|\
    core/src/test/scala/org/apache/spark/shuffle/ShuffleRecoveryAdoptedFailureSuite.scala|\
    core/src/test/scala/org/apache/spark/MapOutputTrackerSuite.scala|\
    core/src/test/scala/org/apache/spark/scheduler/DAGSchedulerSuite.scala|\
    core/src/test/scala/org/apache/spark/shuffle/BlockStoreShuffleReaderSuite.scala|\
    sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/*|\
    sql/core/src/test/scala/org/apache/spark/sql/execution/adaptive/*|\
    sql/core/src/test/scala/org/apache/spark/sql/execution/exchange/ShuffleRecoveryAdaptivePartitionRulesSuite.scala)
      scheduler_tracker_reader_aqe_invalidation=true
      non_docs=true
      ;;

    sql/core/src/test/scala/org/apache/spark/shuffle/ShuffleRecoveryColdProcess*|\
    sql/core/src/test/scala/org/apache/spark/shuffle/ShuffleRecoveryAdoptedFailureColdProcess.scala|\
    core/src/test/scala/org/apache/spark/shuffle/ReferenceShuffleProviderProcess.scala)
      harness=true
      non_docs=true
      ;;

    core/src/main/*|core/src/test/*|sql/core/src/main/*|sql/core/src/test/*)
      unknown_core_sql=true
      non_docs=true
      mark_all_boundaries
      ;;

    *)
      # A non-document path that the prototype router does not understand is treated as build-risk.
      # This costs more CI but prevents new integration paths from inheriting a green gate from an
      # unrelated narrow lane.
      non_docs=true
      workflow_build=true
      mark_all_boundaries
      ;;
  esac
done

if [[ "${mode}" == full || "${mode}" == evidence ]]; then
  mark_all_boundaries
fi

docs_only=false
if [[ ${#paths[@]} -gt 0 && "${docs}" == true && "${non_docs}" == false ]]; then
  docs_only=true
fi

run_style=false
run_license=false
if [[ "${mode}" == full || "${mode}" == evidence || "${scala}" == true || \
      "${java}" == true || "${workflow_build}" == true ]]; then
  run_style=true
fi
if [[ "${mode}" == full || "${mode}" == evidence || "${non_docs}" == true ]]; then
  run_license=true
fi

run_core=false
run_sql=false
run_harness=false
if [[ "${identity_source}" == true || "${provider_publication_discovery}" == true || \
      "${attempt_lifecycle}" == true || \
      "${scheduler_tracker_reader_aqe_invalidation}" == true ]]; then
  run_core=true
fi
if [[ "${identity_source}" == true || \
      "${scheduler_tracker_reader_aqe_invalidation}" == true ]]; then
  run_sql=true
fi
if [[ "${harness}" == true ]]; then
  run_harness=true
fi

cat <<EOF2
validation_mode=${mode}
workflow_build=${workflow_build}
docs_only=${docs_only}
scala=${scala}
java=${java}
identity_source=${identity_source}
provider_publication_discovery=${provider_publication_discovery}
attempt_lifecycle=${attempt_lifecycle}
scheduler_tracker_reader_aqe_invalidation=${scheduler_tracker_reader_aqe_invalidation}
harness=${harness}
unknown_core_sql=${unknown_core_sql}
run_style=${run_style}
run_license=${run_license}
run_core=${run_core}
run_sql=${run_sql}
run_harness=${run_harness}
manual_evidence=$([[ "${mode}" == evidence ]] && echo true || echo false)
EOF2
