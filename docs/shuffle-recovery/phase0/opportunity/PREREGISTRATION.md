# Phase 0-A reusable-shuffle opportunity preregistration

This document freezes the Phase 0-A value study **before** the final broad evidence run. Results may cause the SPIP to stop or re-scope, but they must not cause a correctness rule, corpus row, weighting metric, failure distribution, exclusion rule, or acceptance threshold to be weakened after observation merely to cross the gate.

## Frozen code lineage

- Spark baseline: `2a7cfea06ba135cf0ddc62902eb0daf5a835c672`
- Prototype integration branch: `spip/shuffle-recovery-phase0`
- Runtime weight: successful shuffle-map task **executor run time in milliseconds** from `TaskMetrics.executorRunTime`.
- Secondary weight: successful shuffle-write bytes from `TaskMetrics.shuffleWriteMetrics.bytesWritten`.
- Count unit: one materially executed physical shuffle after explicit reused-physical-work de-duplication.
- Primary value threshold: **20.0%** of completed shuffle-map task time at the declared failure distribution.
- Runtime-correlation acceptance threshold: **95.0%** of materially executed completed shuffle-map task time.

Executor run time is intentionally a coarse work proxy, not CPU time. It can include executor-side fetch time incurred by a map task with upstream dependencies. The metric is fixed here because the value gate is defined in task time rather than whichever metric produces the largest percentage.

## Correlation defect and frozen repair

The Issue #3 smoke artifact from workflow run `33946102450` contained 15 observed physical exchanges per rule set, but only 2 were runtime weighted. The remaining 13 were explicitly `UNWEIGHTED/NO_RUNTIME_CORRELATION`.

The defect was the correlation identity, not retry accounting. The original implementation treated exchange-local shuffle-write SQL metric IDs appearing in `StageInfo.accumulables` as the physical exchange-to-stage identity. That membership is not guaranteed to identify every materialized shuffle stage.

The repaired correlation rule is frozen before the broad value result is inspected:

1. `ShuffleExchangeExec`'s deterministic `SparkPlan.rddScopeId` is the primary identity. Spark already stamps this scope around shuffle-dependency RDD construction.
2. Listener-side `RDDInfo.scope` ancestry is captured for each shuffle stage and matched within the same SQL execution.
3. SQL shuffle-write metric accumulator IDs remain a consistency signal and fallback for synthetic/legacy records.
4. A scope/metric disagreement is `UNWEIGHTED/CORRELATION_KEY_CONFLICT`; it never chooses whichever answer improves coverage.
5. Correlation never forces `ShuffleExchangeExec.shuffleDependency` for a merely planned exchange.
6. Every observed exchange remains `WEIGHTED`, stable-reason `UNWEIGHTED`, or documented `EXCLUDED`.
7. Correlation coverage by bytes/task time uses all listener-observed materialized shuffle stages as the denominator, not only successfully joined exchanges.

This makes silent denominator loss impossible: an expensive completed stage that cannot be joined still remains in the correlation-quality denominator.

## Physical-work winner rule

For one physical shuffle:

1. Retry accounting is keyed by `(SQL execution ID, shuffle ID)`, not stage ID. Spark can recreate a finished `ShuffleMapStage` after output loss with a new stage ID while retaining the same shuffle ID.
2. The coverage denominator is the stage RDD's total partition count, not `StageInfo.numTasks`, because a retry may submit only a subset of maps.
3. A successful task is keyed by the actual RDD/map `partitionId`; the historical task-set index fallback is used only when `partitionId` is unavailable.
4. Stage incarnations/attempts are ordered by their first `SparkListenerStageSubmitted` observation. Duplicate submission delivery preserves the original order.
5. A successful result from a later submitted incarnation/attempt replaces the earlier candidate for that map partition. Successful outputs from an earlier attempt remain represented when Spark's determinate retry does not recompute them.
6. Within one stage incarnation/attempt, at most one successful result for a map partition contributes. The latest successful completion observed before stage completion replaces an earlier duplicate, matching Spark's replace-on-registration map-output semantics. Speculative duplicates are never double-counted.
7. A zero-task skipped resubmission of an already completed physical shuffle does not move its completion order or alter its winner weights. It may add correlation metadata.
8. Failed task work is not reusable successful work. A failed stage attempt contributes only map winners that survive into the eventual completed shuffle under the rule above.
9. A successful stage without complete map-partition winner coverage is `UNWEIGHTED`, never partially weighted.
10. A repeated logical reference to the same correlated physical shuffle is `EXCLUDED/REUSED_PHYSICAL_WORK` after the first physical count.

## Correlation-quality gate

The broad evidence campaign is accepted for a value decision only when all of the following hold:

- at least **95.0% of materially executed completed shuffle-map task time** is correlated and weighted;
- every observed exchange reconciles to `WEIGHTED`, explicit `UNWEIGHTED(reason)`, or documented exclusion;
- remaining unweighted reasons are reported prominently;
- coverage is reported separately by materialized exchange count, shuffle-write bytes, and completed shuffle-map task time.

If the 95% threshold fails, the final value gate is `N/A`; the study does not inspect a partial denominator and declare PASS/FAIL anyway. No alternative threshold will be selected after the value result is visible.

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

The report selects the largest additional candidate by marginal eligible task time over the Window row, then bytes, then count. No new semantic relaxation may be invented after seeing the corpus.

Observed support and counterfactual source opportunity are never combined. Counterfactual exact source identity remains an opportunity assumption, not an implemented source capability.

## Tier A — deterministic smoke/regression corpus

All tables use in-tree Spark TPC schemas/query resources and a deterministic synthetic data generator. There is no Iceberg, Delta, or other added runtime dependency.

Smoke scale: **2 generated rows per table**. Spark SQL shuffle partitions: **4**. Automatic broadcast joins: **disabled**. DPP remains enabled.

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

These smoke numbers validate instrumentation and regression behavior only. They are not the SPIP-facing value estimate.

## Tier B — manual broad evidence corpus

The broad mode is explicit/manual only and uses **32 deterministic generated rows per table**, four shuffle partitions, broadcast joins disabled, and DPP enabled. The generated rows are **not an official TPC scale factor**; benchmark resources supply broad query/plan-shape coverage while deterministic rows keep the campaign reproducible.

The frozen matrix contains **238 SQL executions**:

- 97 practical Spark TPC-DS v1.4 query resources, each with AQE off and AQE on = 194 executions;
- all 22 in-tree TPC-H query resources, each with AQE off and AQE on = 44 executions.

### Frozen TPC-DS query list

The list follows Spark's own practical v1.4 set and omits the same six names excluded by `TPCDSBase` in normal non-regeneration tests (`q6`, `q34`, `q64`, `q74`, `q75`, `q78`). That choice is frozen before broad opportunity results are inspected.

`q1, q2, q3, q4, q5, q7, q8, q9, q10, q11, q12, q13, q14a, q14b, q15, q16, q17, q18, q19, q20, q21, q22, q23a, q23b, q24a, q24b, q25, q26, q27, q28, q29, q30, q31, q32, q33, q35, q36, q37, q38, q39a, q39b, q40, q41, q42, q43, q44, q45, q46, q47, q48, q49, q50, q51, q52, q53, q54, q55, q56, q57, q58, q59, q60, q61, q62, q63, q65, q66, q67, q68, q69, q70, q71, q72, q73, q76, q77, q79, q80, q81, q82, q83, q84, q85, q86, q87, q88, q89, q90, q91, q92, q93, q94, q95, q96, q97, q98, q99`

### Frozen TPC-H query list

`q1` through `q22`, inclusive, each with AQE off and AQE on.

TPC-H is included because the frozen baseline has in-tree schema/query resources; no external benchmark kit or new runtime dependency is required.

### Coverage target

The broad run has an engineering coverage target of **at least 300 correlated materialized physical shuffle exchanges**. This is not a statistical significance threshold and the run must not stop once 300 is reached. The full frozen matrix is executed.

## AQE accounting

AQE-off and AQE-on are reported separately. For AQE-enabled executions the study must distinguish materialized query stages from exchanges that only appear in a plan. Reused physical shuffle work is counted once. Adaptive stage replacement/retry follows the physical winner rule above.

A merely planned exchange never contributes completed reusable work.

## Final failure/restart distribution

Final distribution version: `equal-four-points-v2`.

For each successfully completed SQL execution, evaluate these fixed restart points when applicable:

1. `AFTER_FIRST_ELIGIBLE_COMPLETES` — immediately after the first semantically eligible weighted shuffle completes.
2. `AFTER_MULTIPLE_UPSTREAM_SHUFFLES_COMPLETE` — immediately after the second physical shuffle completes.
3. `BEFORE_MOST_EXPENSIVE_SHUFFLE_COMPLETES` — immediately before the correlated shuffle with the largest executor-run-time weight completes; this is the negative control.
4. `AFTER_ELIGIBLE_INELIGIBLE_MIX` — the earliest non-final completion prefix that contains at least one eligible and at least one ineligible physical shuffle. Executions without such a prefix mark the point not applicable.

Each applicable `(SQL execution, failure point)` instance participates in the declared distribution. The reported final gate ratio is total projected reusable completed shuffle-map executor run time divided by total completed shuffle-map executor run time across those instances.

The Issue #3 `equal-three-points-v1` section is retained only as a compatibility diagnostic. **The final Phase 0-A decision uses `equal-four-points-v2`.**

`Reusable` remains a projection until Phase 0-B demonstrates actual zero-map-task adoption.

## Final value gate

The final value gate uses `exact-source-counterfactual-v1` and `equal-four-points-v2`.

**PASS iff at least 20.0% of completed shuffle-map task executor run time at the declared failure distribution is semantically reusable.**

The final result is emitted only after the 95% correlation-quality gate passes. A zero task-time denominator is `N/A`, not 0% and not PASS. Shuffle-write-byte and exchange-count percentages are secondary diagnostics and cannot replace the task-time gate.

If the gate fails, the failure remains visible. No query, semantic rule, source assumption, or failure point may be altered post hoc to manufacture a pass.

## Sample quality and uncertainty

Physical exchanges are not IID samples. They are nested within queries, benchmark families, and AQE configurations. The final report therefore does not use simplistic exchange-level binomial confidence intervals.

It must report at least:

- per-query opportunity;
- aggregate task-time-weighted opportunity;
- AQE-on/off split;
- benchmark-family split;
- sensitivity after removing the top 1 and top 5 highest-task-time exchanges;
- equal-query weighting versus task-time weighting;
- top-exchange and top-query task-time concentration;
- queries with material work but zero eligible work.

Any future bootstrap must resample at the query/workload unit, not individual exchanges.

## Exclusion and retry policy

- No query may be removed because it hurts the opportunity result.
- No positive query may be added because the gate misses.
- No semantic rule may be widened because the gate misses.
- A run invalidated by deterministic product/test failure remains a failed run until the root cause is fixed.
- An infrastructure-only failure may be rerun with the same commit, corpus, configs, and inputs; the failed run ID remains recorded.
- Failed/cancelled SQL executions are not silently represented as completed opportunity.
- Reused physical work is the only normal exchange-level exclusion in the raw accounting path.
- Large raw logs remain workflow artifacts rather than repository history.

## Required artifacts

Broad mode produces separately labelled artifacts containing at least:

- raw opportunity JSONL;
- per-exchange correlation-reconciliation JSONL;
- rendered human report with correlation gate, scope curve, four failure points, final value gate, AQE/family split, per-query results, and sensitivity/concentration analysis;
- focused test XML/log evidence;
- workflow run ID, artifact ID/name, head SHA, JDK/Scala/build configuration in the final PR/report record.

The dedicated workflow accepts `smoke` or `evidence` manual-dispatch input. Ordinary pull requests remain smoke-only. The broad evidence mode may also be explicitly opted into on a stable PR head; it is never unconditional on every push.
