# Phase 0-A weighted opportunity evidence

This directory contains the preregistered decision rule and the compact human-readable evidence retained with the prototype. Large raw JSONL and test logs are GitHub Actions artifacts rather than permanent repository history.

## Reproduce the focused accounting tests

```bash
./build/sbt -Phadoop-3 -Phive \
  "sql/testOnly org.apache.spark.sql.execution.exchange.ShuffleRecoveryOpportunityStudySuite"
```

## Reproduce the deterministic CI corpus

```bash
SPARK_SHUFFLE_RECOVERY_CORPUS_MODE=smoke \
SPARK_SHUFFLE_RECOVERY_OPPORTUNITY_DIR="$PWD/sql/core/target/shuffle-recovery-phase0/opportunity" \
./build/sbt -Phadoop-3 -Phive \
  "sql/testOnly org.apache.spark.sql.execution.exchange.ShuffleRecoveryOpportunityCorpusSuite"
```

The generated files are:

- `sql/core/target/shuffle-recovery-phase0/opportunity/opportunity-smoke.jsonl`
- `sql/core/target/shuffle-recovery-phase0/opportunity/opportunity-smoke.md`

## Reproduce the manual larger corpus

```bash
SPARK_SHUFFLE_RECOVERY_CORPUS_MODE=larger \
SPARK_SHUFFLE_RECOVERY_OPPORTUNITY_DIR="$PWD/sql/core/target/shuffle-recovery-phase0/opportunity" \
./build/sbt -Phadoop-3 -Phive \
  "sql/testOnly org.apache.spark.sql.execution.exchange.ShuffleRecoveryOpportunityCorpusSuite"
```

The dedicated `Shuffle recovery Phase 0` workflow exposes the same `opportunity-corpus-mode` choice for manual dispatch. Pull-request runs always use `smoke` so the Phase 0-A gate remains exercised without making every PR synchronization run the larger corpus.

## Interpreting the report

The value gate uses the preregistered `exact-source-counterfactual-v1` rule and the `equal-three-points-v1` restart distribution. It passes only at 20.0% or more of completed shuffle-map task executor run time. Exchange-count and shuffle-write-byte opportunity are diagnostic only.

`WEIGHTED`, `UNWEIGHTED`, and `EXCLUDED` are accounting buckets, not eligibility outcomes. An exchange can be semantically eligible but unweighted when runtime correlation cannot be proven. Conversely, an ineligible exchange can still be weighted so its real runtime cost remains in the completed-work denominator.

Observed source-token capability and the exact-source counterfactual are reported as different scope rows. The counterfactual does not claim that the frozen Spark baseline already has a durable file-snapshot identity implementation.

Failure-point `avoidable` work is a projection. Phase 0-A measures opportunity; it does not prove cross-driver reuse or zero map tasks.
