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
evidence_target="${2:-opportunity}"
shift $(( $# >= 2 ? 2 : $# ))

case "${mode}" in
  focused|full|evidence) ;;
  *)
    echo "unsupported validation mode: ${mode}" >&2
    exit 2
    ;;
esac
case "${evidence_target}" in
  opportunity|cold-process|all) ;;
  *)
    echo "unsupported evidence target: ${evidence_target}" >&2
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

workflow=false
docs=false
non_docs=false
scala=false
java=false
sql_opportunity=false
core_provider=false
manifest=false
claim=false
scheduler=false
cold_process=false
adopted_failure=false
core_other=false
sql_other=false

mark_core_other() {
  core_other=true
  non_docs=true
}

mark_sql_other() {
  sql_other=true
  non_docs=true
}

for path in "${paths[@]}"; do
  case "${path}" in
    *.scala) scala=true ;;
    *.java) java=true ;;
  esac

  case "${path}" in
    docs/shuffle-recovery/*)
      docs=true
      ;;
    .github/workflows/shuffle-recovery-phase0.yml|.github/scripts/shuffle-recovery-ci-*)
      workflow=true
      non_docs=true
      ;;
    sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleRecoveryOpportunity*|\
    sql/core/src/test/scala/org/apache/spark/sql/execution/exchange/ShuffleRecoveryOpportunity*|\
    sql/core/src/test/scala/org/apache/spark/sql/execution/exchange/ShuffleRecoveryRuntime*|\
    sql/core/src/test/scala/org/apache/spark/sql/execution/exchange/ShuffleRecoveryFailureDistributionSuite.scala|\
    sql/core/src/test/scala/org/apache/spark/sql/execution/exchange/ShuffleRecoveryAdaptivePartitionRulesSuite.scala)
      sql_opportunity=true
      non_docs=true
      ;;
    sql/core/src/test/scala/org/apache/spark/shuffle/ShuffleRecoveryColdProcess*)
      cold_process=true
      non_docs=true
      ;;
    core/src/main/scala/org/apache/spark/shuffle/ReferenceShuffleProvider*|\
    core/src/main/scala/org/apache/spark/shuffle/ShuffleRecoveryIndexShuffleBlockResolver.scala|\
    core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleManager.scala|\
    core/src/test/scala/org/apache/spark/shuffle/ReferenceShuffleProvider*|\
    core/src/test/scala/org/apache/spark/shuffle/sort/IndexShuffleBlockResolverSuite.scala|\
    core/src/test/scala/org/apache/spark/shuffle/sort/SortShuffleManagerSuite.scala)
      core_provider=true
      non_docs=true
      ;;
    core/src/main/scala/org/apache/spark/shuffle/ShuffleRecoveryManifest*|\
    core/src/test/scala/org/apache/spark/shuffle/ShuffleRecoveryManifestSuite.scala)
      manifest=true
      non_docs=true
      ;;
    core/src/main/scala/org/apache/spark/shuffle/ReferenceShuffleRecoveryClaimProvider.scala|\
    core/src/main/scala/org/apache/spark/shuffle/ShuffleRecoveryClaim.scala|\
    core/src/main/scala/org/apache/spark/shuffle/ShuffleRecoveryPreparation.scala|\
    core/src/main/scala/org/apache/spark/shuffle/ShuffleRecoveryUntrustedBoundary.scala)
      claim=true
      core_provider=true
      non_docs=true
      ;;
    core/src/main/scala/org/apache/spark/ShuffleRecoverySchedulerAdoption.scala|\
    core/src/main/scala/org/apache/spark/scheduler/ShuffleMapStage.scala|\
    core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala|\
    core/src/main/scala/org/apache/spark/MapOutputTracker.scala|\
    core/src/test/scala/org/apache/spark/scheduler/*|\
    core/src/test/scala/org/apache/spark/MapOutputTrackerSuite.scala)
      scheduler=true
      non_docs=true
      ;;
    core/src/main/scala/org/apache/spark/shuffle/*Failure*|\
    core/src/main/scala/org/apache/spark/shuffle/*Invalidat*|\
    core/src/test/scala/org/apache/spark/shuffle/*Failure*|\
    core/src/test/scala/org/apache/spark/shuffle/*Invalidat*)
      adopted_failure=true
      non_docs=true
      ;;
    core/src/main/*|core/src/test/*)
      mark_core_other
      ;;
    sql/core/src/main/*|sql/core/src/test/*)
      mark_sql_other
      ;;
    .github/*|dev/*|project/*|pom.xml|build/*)
      # Build/tooling changes can affect every prototype job. Fail conservatively.
      workflow=true
      non_docs=true
      ;;
    *)
      # Unknown production-ish paths are intentionally conservative. Pure documentation outside the
      # recovery docs is cheap enough to leave to generic repository CI, but any source/build path
      # that reaches this branch should not silently skip the prototype.
      case "${path}" in
        *.md|docs/*) docs=true ;;
        *) non_docs=true; core_other=true; sql_other=true ;;
      esac
      ;;
  esac
done

run_core=false
run_opportunity=false
run_cold_process=false
run_style=false
run_evidence_opportunity=false
run_evidence_cold_process=false

case "${mode}" in
  focused)
    if [[ "${workflow}" == true ]]; then
      # Workflow/router changes are the one focused-mode case that deliberately exercise every
      # accumulated correctness lane available at the candidate head.
      run_core=true
      run_opportunity=true
      run_cold_process=true
    else
      if [[ "${core_provider}" == true || "${manifest}" == true || "${claim}" == true || \
            "${scheduler}" == true || "${adopted_failure}" == true || "${core_other}" == true ]]; then
        run_core=true
      fi
      if [[ "${sql_opportunity}" == true || "${sql_other}" == true ]]; then
        run_opportunity=true
      fi
      if [[ "${cold_process}" == true ]]; then
        run_cold_process=true
      fi
    fi
    ;;
  full)
    # A full gate validates the complete candidate, not just the diff. Mark Scala validation
    # applicable so the existing style lane runs the repository scalastyle pass even for a
    # documentation-only final candidate.
    scala=true
    run_core=true
    run_opportunity=true
    run_cold_process=true
    ;;
  evidence)
    case "${evidence_target}" in
      opportunity)
        run_opportunity=true
        run_evidence_opportunity=true
        ;;
      cold-process)
        # An explicitly requested evidence lane is mandatory. Mark it applicable so the workflow
        # fails closed if the cold-process suite is not present at the selected candidate head.
        cold_process=true
        run_cold_process=true
        run_evidence_cold_process=true
        ;;
      all)
        cold_process=true
        run_opportunity=true
        run_cold_process=true
        run_evidence_opportunity=true
        run_evidence_cold_process=true
        ;;
    esac
    ;;
esac

if [[ "${mode}" == full || "${scala}" == true || "${java}" == true || "${workflow}" == true ]]; then
  run_style=true
fi

docs_only=false
if [[ ${#paths[@]} -gt 0 && "${docs}" == true && "${non_docs}" == false ]]; then
  docs_only=true
fi

cat <<EOF
validation_mode=${mode}
evidence_target=${evidence_target}
workflow=${workflow}
docs_only=${docs_only}
scala=${scala}
java=${java}
sql_opportunity=${sql_opportunity}
core_provider=${core_provider}
manifest=${manifest}
claim=${claim}
scheduler=${scheduler}
cold_process=${cold_process}
adopted_failure=${adopted_failure}
core_other=${core_other}
sql_other=${sql_other}
run_style=${run_style}
run_core=${run_core}
run_opportunity=${run_opportunity}
run_cold_process=${run_cold_process}
run_evidence_opportunity=${run_evidence_opportunity}
run_evidence_cold_process=${run_evidence_cold_process}
EOF
