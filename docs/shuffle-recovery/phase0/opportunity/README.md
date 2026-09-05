# Phase 0-A weighted opportunity evidence

This directory separates fast regression evidence from the manual Phase 0-A value campaign. Large raw JSONL, reconciliation data, and test logs remain GitHub Actions artifacts rather than permanent repository history.

The frozen corpus, correlation gate, exclusion policy, failure distribution, and 20% value rule are defined in [`PREREGISTRATION.md`](PREREGISTRATION.md).

## Reproduce the focused accounting tests

```bash
./build/sbt -Phadoop-3 -Phive \
  "sql/testOnly \
    org.apache.spark.sql.execution.exchange.ShuffleRecoveryOpportunityStudySuite \
    org.apache.spark.sql.execution.exchange.ShuffleRecoveryRuntimeWeightsSuite \
    org.apache.spark.sql.execution.exchange.ShuffleRecoveryRuntimeCorrelationCoverageSuite \
    org.apache.spark.sql.execution.exchange.ShuffleRecoveryOpportunityEvidenceSuite"
```

These suites cover retry/speculation winner accounting, explicit correlation failure buckets, RDD-scope correlation, scope/metric conflict fail-closed behavior, observed-success versus accepted-winner reconciliation, the 95% correlation-quality denominator, and the four-point value distribution.

## Tier A: deterministic smoke/regression corpus

```bash
SPARK_SHUFFLE_RECOVERY_CORPUS_MODE=smoke \
SPARK_SHUFFLE_RECOVERY_OPPORTUNITY_DIR="$PWD/sql/core/target/shuffle-recovery-phase0/opportunity" \
./build/sbt -Phadoop-3 -Phive \
  "sql/testOnly org.apache.spark.sql.execution.exchange.ShuffleRecoveryOpportunityCorpusSuite"
```

The smoke files are:

- `opportunity-smoke.jsonl`
- `correlation-reconciliation-smoke.jsonl`
- `opportunity-smoke.md`

Smoke results validate instrumentation and accounting only. They must not be quoted as the final target-corpus opportunity estimate.

## Tier B: manual broad evidence campaign

```bash
SPARK_SHUFFLE_RECOVERY_CORPUS_MODE=evidence \
SPARK_SHUFFLE_RECOVERY_OPPORTUNITY_DIR="$PWD/sql/core/target/shuffle-recovery-phase0/opportunity" \
./build/sbt -Phadoop-3 -Phive \
  "sql/testOnly org.apache.spark.sql.execution.exchange.ShuffleRecoveryOpportunityCorpusSuite"
```

Broad mode executes the preregistered TPC-DS/TPC-H AQE matrix and writes separately labelled `evidence` artifacts. The dedicated `Shuffle recovery Phase 0` workflow exposes the same `opportunity-corpus-mode` input for manual dispatch. Ordinary pull-request synchronizations remain smoke-only; heavy evidence must be explicitly requested.

The broad campaign is accepted for a value decision only if at least 95% of materially executed completed shuffle-map task time is correlated. It has an engineering coverage target of at least 300 correlated materialized exchanges, but the full frozen query matrix runs regardless of when that target is crossed.

## Interpreting the report

`WEIGHTED`, `UNWEIGHTED`, and `EXCLUDED` are runtime-accounting buckets, not semantic eligibility outcomes. An exchange can be semantically eligible but unweighted when runtime correlation cannot be proven. Conversely, an ineligible exchange can be weighted so its real runtime cost remains in the denominator.

The report retains the earlier three-point calculation as a compatibility diagnostic, but the final Phase 0-A decision uses the preregistered `equal-four-points-v2` distribution and `exact-source-counterfactual-v1` semantic rule. The final gate passes only at 20.0% or more of completed shuffle-map task time, and it is emitted only after the correlation-quality gate passes.

The report also includes per-query opportunity, AQE and benchmark-family splits, equal-query versus task-time weighting, top-1/top-5 exchange sensitivity, and task-time concentration. Physical exchanges are explicitly not treated as IID statistical samples.

Observed source-token support and the exact-source counterfactual remain separate. Counterfactual exact source identity is an opportunity assumption, not a claim that the frozen Spark baseline already implements durable source snapshot identity.

Failure-point `reusable` work remains a projection. Phase 0-A measures opportunity; it does not prove cross-driver adoption or zero map tasks.
