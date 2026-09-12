# Phase 0-A reusable-shuffle opportunity preregistration

This document freezes the Phase 0-A value study **before** the final evidence run. Results may cause the SPIP to stop or re-scope, but they must not cause a correctness rule, corpus row, weighting metric, or failure distribution to be weakened after observation merely to cross the gate.

## Frozen code lineage

- Spark baseline: `2a7cfea06ba135cf0ddc62902eb0daf5a835c672`
- Prototype integration branch: `spip/shuffle-recovery-phase0`
- Runtime weight: successful shuffle-map task **executor run time in milliseconds** from `TaskMetrics.executorRunTime`.
- Secondary weight: successful shuffle-write bytes from `TaskMetrics.shuffleWriteMetrics.bytesWritten`.
- Count unit: one physical shuffle exchange after explicit reused-physical-work exclusion.

Executor run time is intentionally a coarse work proxy, not CPU time. It can include executor-side fetch time incurred by a map task with upstream dependencies. The metric is fixed here because the value gate is defined in task time rather than whichever metric produces the largest percentage.

## Physical-work winner rule

For one physical shuffle:

1. Retry accounting is keyed by `(SQL execution ID, shuffle ID)`, not stage ID. Spark can recreate a finished `ShuffleMapStage` after output loss with a new stage ID while retaining the same shuffle ID.
2. The coverage denominator is the stage RDD's total partition count, not `StageInfo.numTasks`, because a retry may submit only a subset of maps.
3. A successful task is keyed by the actual RDD/map `partitionId`; the historical task-set index fallback is used only when `partitionId` is unavailable.
4. Stage incarnations/attempts are ordered by their first `SparkListenerStageSubmitted` observation. Duplicate submission delivery preserves the original order. A successful result from a later submitted incarnation/attempt replaces the earlier candidate for that map partition. Successful outputs from an earlier attempt remain represented when Spark's determinate retry does not recompute them.
5. Within one stage incarnation/attempt, at most one successful result for a map partition contributes. The latest successful completion observed before stage completion replaces an earlier duplicate, matching Spark's replace-on-registration `MapOutputTracker` semantics for accepted duplicate successes. Speculative duplicates are therefore never double-counted.
6. A zero-task skipped resubmission of an already completed physical shuffle does not move its completion order or alter its winner weights. It may contribute accumulator IDs needed for correlation.
7. Failed task work is not reusable successful work. A failed stage attempt contributes only map winners that survive into the eventual completed shuffle under the rule above.
8. A successful stage without complete map-partition winner coverage is `UNWEIGHTED`, never partially weighted.
9. A repeated logical reference to the same correlated physical shuffle is `EXCLUDED/REUSED_PHYSICAL_WORK` after the first physical count. No other accounting reason is permitted to remove work from the denominator.
10. Missing or ambiguous exchange-to-stage correlation is `UNWEIGHTED` with a stable reason.

## Correlation rule

The study does not infer bytes, time, or shuffle IDs from plan strings and does not force `ShuffleExchangeExec.shuffleDependency` for measurement. A physical exchange is correlated with a completed shuffle stage only when an exchange-local shuffle-write SQL metric ID is present in that stage's accumulator IDs for the same SQL execution. Zero or multiple candidate stages are explicit unweighted outcomes.

## Scope curve

The same captured exchanges are classified under these named/versioned rows:

1. `observed-baseline-v1` — conservative built-in hash/single-partition semantics; only source-token capability actually available to the feasibility analyzer is accepted.
2. `exact-source-counterfactual-v1` — identical semantic/operator scope, but controlled immutable built-in batch sources are granted a counterfactual exact durable source token.
3. `exact-source-plus-dpp-runtime-filter-v1` — row 2 plus DPP/runtime-filter/subquery semantics.
4. `exact-source-plus-dpp-window-v1` — row 3 plus Window.
5. Fixed additional candidates, all defined before results:
   - `candidate-plus-expand-v1`
   - `candidate-plus-cache-scan-v1`
   - `candidate-plus-adaptive-partition-specs-v1`

The report selects the largest additional candidate by marginal eligible task time over the Window row, then bytes, then count; a candidate is highlighted only when at least one marginal measure is positive. No new semantic relaxation may be invented after seeing the corpus.

Observed support and counterfactual source opportunity are never combined. The controlled file-source corpus is materialized completely before study listeners are installed. Its batch lineage is treated as determinate for this study, while the observed row still records its durable source token as unavailable. The counterfactual row changes that token capability explicitly rather than treating token absence as nondeterminism.

## Deterministic CI corpus

All tables use in-tree Spark TPC schemas/query resources and a deterministic synthetic data generator. There is no Iceberg, Delta, or other added runtime dependency.

CI smoke scale: **2 generated rows per table**. Spark SQL shuffle partitions: **4**. Automatic broadcast joins: **disabled** so shuffle opportunity is exercised rather than optimized away. DPP remains enabled. Each row below is a separate SQL execution.

| Family | Query resource | AQE |
|---|---|---|
| TPC-DS | `tpcds/q3.sql` | off |
| TPC-DS | `tpcds/q3.sql` | on |
| TPC-DS | `tpcds/q23a.sql` | on |
| TPC-DS | `tpcds-v2.7.0/q51a.sql` | on |
| TPC-DS | `tpcds/q98.sql` | off |
| TPC-H | `tpch/q1.sql` | off |
| TPC-H | `tpch/q3.sql` | on |
| TPC-H | `tpch/q5.sql` | on |

TPC-H is included because the frozen baseline has an in-tree `TPCHBase` schema and `tpch/*.sql` resources; no external data kit is needed for the deterministic synthetic corpus.

## Manual-dispatch larger corpus

Manual larger mode uses **4 generated rows per table** and runs the complete CI smoke list plus:

| Family | Query resource | AQE |
|---|---|---|
| TPC-DS | `tpcds/q7.sql` | off |
| TPC-DS | `tpcds/q19.sql` | on |
| TPC-DS | `tpcds/q42.sql` | off |
| TPC-DS | `tpcds/q52.sql` | on |
| TPC-DS | `tpcds/q73.sql` | on |
| TPC-H | `tpch/q12.sql` | off |
| TPC-H | `tpch/q18.sql` | on |

The dedicated workflow records whether `smoke` or `larger` mode ran and uploads its raw JSONL plus rendered Markdown report.

## Failure/restart distribution

Distribution version: `equal-three-points-v1`.

For each successfully completed SQL execution, evaluate these fixed candidate restart points when applicable:

1. `AFTER_FIRST_ELIGIBLE_COMPLETES` — immediately after the first semantically eligible weighted shuffle completes.
2. `AFTER_MULTIPLE_UPSTREAM_SHUFFLES_COMPLETE` — immediately after the second physical shuffle completes. Executions with fewer than two correlated physical shuffles mark this point not applicable.
3. `BEFORE_MOST_EXPENSIVE_SHUFFLE_COMPLETES` — immediately before the correlated shuffle with the largest executor-run-time weight completes. Earlier completed shuffles remain in the denominator; the expensive shuffle itself does not. This is the negative-control point.

Each applicable `(SQL execution, failure point)` instance has equal probability in the declared distribution. The reported gate ratio is total projected reusable completed shuffle-map executor run time divided by total completed shuffle-map executor run time across those equally sampled instances. For every point the report separately exposes all exchanges, completed exchanges, semantically eligible completed exchanges, avoidable bytes, and avoidable task time.

`Avoidable` is a projection in Phase 0-A. It must not be described as demonstrated reuse until Phase 0-B proves actual zero-map-task adoption.

## Value gate

The value gate is evaluated on `exact-source-counterfactual-v1` because Phase 0-A is intended to answer the semantic opportunity question independently of whether a production snapshot-capable source adapter has already been implemented.

**PASS iff at least 20.0% of completed shuffle-map task executor run time at `equal-three-points-v1` is semantically reusable.**

A zero task-time denominator is `N/A`, not 0% and not PASS. Shuffle-write-byte and exchange-count percentages are also reported but cannot replace the preregistered task-time gate.

If this gate fails, the failure remains visible in the report and umbrella decision. No rule is relaxed post hoc to manufacture a pass.

## Required artifact/accounting properties

- Raw evidence schema version: `1`.
- Every observed exchange is `WEIGHTED`, stable-reason `UNWEIGHTED`, or documented `EXCLUDED`; only reused physical work may use the excluded bucket.
- Failed/cancelled SQL executions remain explicit and are never represented as completed opportunity.
- Zero exchanges are a valid empty report.
- Zero byte/time denominators are `N/A`.
- Aggregation uses checked `Long` arithmetic; malformed raw JSON/numbers are rejected.
- Rule-set name/version participates in classification and report grouping.
- Raw/report ordering is deterministic.
- Large raw logs remain workflow artifacts rather than repository history.
