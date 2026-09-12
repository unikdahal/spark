# Prospective completed-shuffle reuse value experiment v2

Status: **preregistered, not executed**

This document prospectively repairs the primary failure intervention registered in
`value-experiment-v1.md`. The v1 Markdown and JSON remain published unchanged as historical
preregistration evidence. No primary restart timing, no primary overhead timing, and no real
source/provider value campaign had been executed before this revision. No benchmark was run to
choose the revision.

The correction is deliberately narrow. Primary failures are selected from independent ordinary
failure-free runs and injected at fixed elapsed-time offsets that are identical in the
recovery-disabled and recovery-enabled arms. Recovery publication is observed after the fact; it is
never awaited at the failure boundary. The workload, deployment scenario, producer grammar, weights,
input scales, resource limits, cost rates, and prospective pass thresholds remain those registered in
v1.

Machine-readable companion: [`value-experiment-v2.json`](value-experiment-v2.json). The unchanged
workload and cost companions remain [`workload-scope-v1.md`](workload-scope-v1.md) and
[`cost-model-v1.md`](cost-model-v1.md).

## Why v2 exists

The v1 primary distribution used publication-dependent F0/F1 landmarks. A recovery-disabled control
performs no recovery publication, so those landmarks did not define the same intervention in both
arms. Waiting for publication in the recovery arm would also condition the failure on the
optimization already having produced a usable artifact and would hide slow or unsuccessful
publication as absent observations.

v2 removes that arm dependence. The primary controller never asks whether reuse succeeded and never
waits for a publication or provider event. It follows one frozen elapsed-time schedule derived from
ordinary recovery-disabled execution and applies the same schedule to both arms.

The older publication-boundary probes are retained only as optional mechanism controls with zero
primary scenario weight. They cannot enter the deployment-value effect, overhead gates, or economics.

## Historical boundary and measurement record

The frozen lineage remains:

- upstream baseline: `apache/spark@2a7cfea06ba135cf0ddc62902eb0daf5a835c672`;
- frozen Phase 0 evidence: `7888d1e26d32e69694083e367ee6a92b28277c48`;
- v1 preregistration base: `5f773c4669d06af57216b09748ec7d168412ad3c`;
- v2 integration base: `f907fd51066407ec3f599b846485b4ad027df7c1`.

The old Phase 0 opportunity run remains historical context only. Its stage shape and opportunity
figures do not select a v2 injection offset, do not contribute an observation, and do not enter a v2
weight, confidence interval, or economics calculation.

At the time v2 was registered:

- primary restart measurements observed: **no**;
- primary overhead measurements observed: **no**;
- real Iceberg/Celeborn value campaign executed: **no**;
- new benchmark run for this correction: **no**.

## Unchanged deployment, cohort, and producer scope

v2 keeps the v1 target deployment profile `decision-support-snapshot-rss-v1`: explicit-snapshot
Iceberg reads, Celeborn as the real durable-shuffle candidate, Kubernetes batch relaunch with a fresh
driver and fresh executors, the same six-query Window/join corpus, the four 16/64/256/512 GiB fact
scales, and the same scenario weights.

Exactly one exchange may be adopted. Its child remains a certified immutable scan plus optional
reviewed deterministic filter/project followed by hash or single partitioning. Joins, aggregates,
Window, subqueries, runtime filters, unsupported UDF/native expressions, sampled range partitioning,
and runtime-dependent scans remain excluded below the reusable producer. Default AQE and ordinary
Spark plan settings remain required for primary evidence.

The v1 workload weights are unchanged:

| Workload | Weight |
| --- | ---: |
| `W01` | 20% |
| `W02` | 15% |
| `S01` | 10% |
| `J01` | 25% |
| `J02` | 20% |
| `E01` | 10% |

Each of `I16`, `I64`, `I256`, and `I512` retains equal 25% input-scale weight.

## Independent failure-free dry-run schedule

Primary restart timing **must not begin** until a machine-readable
`primary-failure-schedule-v2.json` has been produced from the integrated candidate on the registered
deployment and frozen with the campaign evidence.

The schedule is derived from ordinary execution only:

- recovery is disabled;
- no driver failure is injected;
- no restart timing is collected;
- 5 valid dry runs are required for each of 6 workloads x 4 input scales;
- therefore calibration is 24 cells / **120 failure-free attempts**;
- cell execution order is deterministically shuffled with seed `557001`.

For every dry run, the external controller records monotonic elapsed time from immediately before
submitting the query action to these required ordinary observables:

1. completion of the target producer `ShuffleMapStage`;
2. complete correct-result verification.

Material downstream stage submission/completion timestamps are retained when present as diagnostics,
but they do not select or move a failure point. This avoids relying on a particular number or ordering
of downstream Spark stages.

Publication visibility, provider discovery, claim success, recovery eligibility, and every other
recovery-only state are forbidden schedule inputs.

For each workload/input-scale cell define:

```text
P = median(target producer completion elapsed ms)
Q = median(correct-result verification elapsed ms)
```

The controller freezes four integer-millisecond offsets:

| Failure point | Weight | Frozen offset rule |
| --- | ---: | --- |
| `F0` | 25% | `0.5 * P` |
| `F1` | 30% | `P` |
| `F2` | 30% | `P + 0.50 * (Q - P)` |
| `F3` | 15% | `P + 0.85 * (Q - P)` |

Offsets are rounded to the nearest integer millisecond, ties to even.

A schedule row is valid only if all five dry runs expose producer completion and correct-result
verification, `P > 0`, `Q > P`, and the four rounded offsets are strictly positive and strictly
increasing. All 24 rows must be valid and frozen before primary restart timing starts.

If calibration cannot produce that schedule, primary timing does not start. The failed dry-run
evidence is retained. A material workload/schedule redesign requires a new prospective campaign
version; it is not repaired by post-hoc weight redistribution.

The frozen artifact has exactly 24 workload/input-scale rows. Each row records workload id, scale id,
the five calibration attempt ids, producer-completion and result-verification elapsed times for every
attempt, diagnostic downstream stage timestamps when present, `P`, `Q`, rounded F0-F3 offsets, and
controller/build/deployment/source-snapshot/provider/cluster identity. The complete artifact also
carries a digest.

## Identical primary intervention in both arms

For every primary pair, control and recovery use the same frozen schedule row, source snapshots,
input scale, cluster shape, and controller semantics.

The controller:

1. captures a monotonic `queryStart` immediately before submitting the query action;
2. looks up the already-frozen `(workload, scale, failure point)` offset;
3. if the correct result has not already been verified, issues the driver-termination command when
   monotonic elapsed time reaches that offset;
4. if the correct result was already verified, does not terminate a completed query solely for
   measurement and records the zero-impact classification below;
5. never waits for publication, a Spark stage event, provider visibility, or reuse success in the
   primary trial.

The scheduled elapsed offset is therefore the intended intervention in both arms. There is no
recovery-disabled translation and no arm-specific failure landmark.

For an active query, the actual termination-command timestamp is recorded. Absolute error from the
frozen target must be at most **500 ms**. A larger error is
`HARNESS_INVALID_CONTROLLER_TIMING`: the raw record remains published and may be replaced only as a
predeclared harness fault with a new attempt id. Outcome, publication state, or observed speed never
determines whether a timing record is replaced.

A failed termination command, controller crash, or inability to observe the required correctness
boundary is likewise a harness fault rather than a recovery miss. Harness faults are reported
separately and cannot be reclassified from performance outcomes.

## Producer completion and early failures

Producer completion is determined after the run from ordinary Spark event timestamps. It never gates
or delays the primary failure command.

`F0` deliberately represents an early opportunity. A primary failure may occur before the target
producer completes. Such a trial remains in the registered cohort. No reusable artifact exists yet,
so recovery must miss/fall back if ordinary correctness permits. The failure is not dropped and its
scenario weight is not reassigned to a later failure point.

`F1` is intentionally a producer-boundary **elapsed-time** sample, not a post-publication sample. On
one repetition the producer may have completed before failure and on another it may not have. That
variation is evidence about the deployment timing distribution, not a reason to move the failure.

## Publication races are observations, not barriers

Primary failure injection performs no synchronous provider lookup and no publication check.
Publication state is classified after the run from asynchronous publication/provider audit telemetry
whose timestamp is independent of the injection path.

The recovery-arm state at the actual termination-command timestamp is one of:

- `VISIBLE_BEFORE_INJECTION`;
- `NOT_VISIBLE_BEFORE_INJECTION`;
- `PUBLICATION_FAILED_BEFORE_INJECTION`;
- `PUBLICATION_STATE_UNKNOWN`.

The recovery-disabled arm records `RECOVERY_DISABLED_NOT_APPLICABLE`.

A late publication, failed publication, or unavailable publication observation never removes a trial
from the cohort and never reschedules its failure. If publication is too late or fails and recovery
therefore misses, the full fallback time remains in the recovery arm. If telemetry is unavailable,
the publication classification is unknown but the timed outcome remains usable unless some separate
required correctness/timing evidence is missing.

For a publication event racing the failure, classification compares its audit timestamp with the
actual termination-command timestamp after the fact. There is no publication barrier in the query,
scheduler, or controller.

## Natural completion before the scheduled failure

A query may naturally produce and verify its correct result before a frozen late failure offset. That
is not a reason to shift the failure earlier or to drop the repetition.

The attempt is classified `CORRECT_RESULT_BEFORE_SCHEDULED_FAILURE`, receives a post-failure residual
of **0 ms**, is not replaced, and retains its registered scenario weight. Counts and scenario mass are
reported by arm, workload, input scale, and failure point.

For all attempts the primary residual is:

```text
if correct result was verified before the scheduled injection:
  0
else:
  max(0, correct_result_verification_time - actual_termination_command_time)
```

This keeps zero-impact late failure opportunities in the registered population instead of
conditioning the headline result on a query still being alive at a favorable recovery state. If the
aggregate control denominator is zero, the campaign is `INDETERMINATE` rather than inventing a ratio.

## Optional publication-boundary mechanism controls

The two v1 publication-dependent probes may still be useful to test mechanism behavior:

- `M0`: producer completed while durable publication is not yet visible;
- `M1`: durable publication visible before a material downstream stage completes.

They have **zero primary scenario weight**. They may use recovery-specific hooks or waits because they
are explicitly labelled mechanism diagnostics, but they cannot substitute for F0-F3, cannot enter the
headline restart estimate, cannot enter any overhead gate, and cannot enter deployment economics.

## Primary restart experiment counts and pairing

The primary restart campaign has:

```text
6 workloads x 4 input scales x 4 frozen failure offsets = 96 cells
96 cells x 12 paired repetitions = 1,152 pairs
1,152 pairs x 2 arms = 2,304 timed attempts
```

Each complete cell uses six control-first and six recovery-first pairs under seed `557003`. Pair order
is fixed before outcomes are observed. The natural-backend-warmth and no-outlier-deletion policies
from v1 remain unchanged.

A recovery miss is an observed recovery-arm result, not missing data. The hard per-attempt timeout
remains 1,800 seconds. Wrong row count or result digest is a correctness failure and the campaign
cannot pass.

Predetermined timeout scoring remains conservative, applied to the post-failure residual:

- recovery times out/fails while control is correct: keep the observed control residual and score
  recovery at 1,800 seconds;
- control fails/times out while recovery succeeds: score both residuals at 1,800 seconds for zero
  speedup credit, retain the actual recovery observation separately, and count the control failure;
- both fail/time out: score both at 1,800 seconds for zero speedup credit.

## Restart aggregation and confidence

For each pair, let `T_control` and `T_recovery` be the observed or predeclared-imputed post-failure
residuals.

Headline improvement is:

```text
1 - weighted_sum(T_recovery) / weighted_sum(T_control)
```

Weights remain workload weight x failure-point weight x equal 25% input-scale weight. No observed
publication state, recovery hit rate, producer-survival status, or natural-completion rate modifies a
weight.

The 95% interval remains a deterministic paired stratified bootstrap with 10,000 replicates and seed
`55003`: resample the 12 pairs within each fixed workload/input-scale/failure-point cell, preserve pair
dependence, keep the registered scenario composition fixed, and never treat exchanges/maps/reducers
as IID observations.

PASS still requires aggregate improvement >=20% and a lower 95% confidence bound >0%.

## Overhead experiments are separate campaigns

The 96 restart cells and 1,152 restart pairs do **not** cover the three no-failure overhead
comparisons. v2 preregisters those comparisons independently.

Each overhead comparison has 6 workloads x 4 input scales = **24 cells**, with 12 paired repetitions
per cell, hence **288 pairs / 576 timed attempts** per comparison.

| Id | No-failure comparison | Randomization seed | Bootstrap seed | Upper 95% gate |
| --- | --- | ---: | ---: | ---: |
| `OH_DISABLED` | recovery-disabled integrated candidate vs frozen upstream baseline | 557101 | 55101 | <=1% |
| `OH_ALL_MISS` | recovery-enabled naturally absent/incompatible candidates vs same candidate recovery-disabled | 557102 | 55102 | <=5% |
| `OH_PUBLICATION` | recovery-enabled candidate with publication enabled vs same candidate recovery-disabled | 557103 | 55103 | <=5% |

Across all three comparisons this is **72 registered cells / 864 pairs / 1,728 timed attempts**,
separate from the restart campaign.

Within every overhead cell, six pairs are reference-first and six are treatment-first according to
the comparison-specific seed. There is no failure injection. Cache warmth, 1,800-second timeout, and
no-outlier-deletion rules match the primary campaign.

For an overhead comparison:

```text
overhead =
  weighted_sum(treatment no-failure runtime) /
  weighted_sum(reference no-failure runtime) - 1
```

Weights are fixed workload weight x equal 25% input-scale weight. Confidence is a comparison-specific
paired stratified bootstrap over the 12 pairs in each workload/input-scale cell, with 10,000
replicates and the seed in the table. The upper 95% bound is the 95th percentile of the bootstrap
overhead distribution. No restart failure-point dimension appears in an overhead cell.

A non-harness timeout or failure in either overhead arm makes that overhead comparison
`INDETERMINATE`; the raw pair is retained and is not replaced. A wrong result prevents the campaign
from passing. Only a predeclared external harness fault may be replaced, with the invalid raw record
still published and the replacement assigned a new attempt id. This is deliberately separate from
the restart experiment's conservative timeout imputation.

The all-miss arm must use naturally absent or incompatible candidates; validation cannot be weakened
or short-circuited to manufacture a cheap miss.

`OH_PUBLICATION` measures the cost of **enabling and attempting publication**, not a survivor-only
successful-publication cohort. Every timed publication-arm outcome is retained, including successful,
late, failed, or publication-state-unknown outcomes. Publication success rate and any
successful-publication subset may be reported descriptively, but the <=5% gate uses every registered
publication-arm result.

## Failure probability and economics

The scenario driver-failure frequency remains 1.0% of query attempts. Economics applies that
probability to the **full frozen failure-opportunity distribution**, including pre-producer failures
and zero-impact outcomes whose correct result preceded the scheduled failure. It does not replace the
1.0% rate with a survivor-only probability conditioned on producer completion, successful
publication, or an observed recovery hit.

Existing-provider and greenfield accounting remain exactly as specified in `cost-model-v1.md`.
Ordinary shuffle writes are not double-counted as recovery publication cost, and the numerical
`$6.12304/hour` / `$0.306152/query` greenfield fixed allocation remains separate from the
existing-provider lens.

## Resource limits and correctness invariants

All v1 resource limits remain frozen, including 4 MiB manifests, 32 MiB Spark driver recovery state,
8 MiB provider discovery payload, four candidate incarnations, 512 MiB provider-index accounting,
3-second lookup / 10-second preparation deadlines, 128 concurrent provider reads, 512 GiB retained
artifact size, and 1,024 GiB-hours per query.

This revision changes no Spark runtime code. It therefore adds no scheduler event-loop I/O, no new
provider contract, no mutable trusted state, no lifecycle authority, and no public API. Later runtime
work must still preserve optional-cache semantics, best-effort publication, hostile-input validation,
all-or-nothing adoption, attempt/artifact lifetime separation, and no external recovery I/O on the
DAGScheduler event loop.

## Change control

v1 remains immutable historical evidence. v2 is the active prospective contract.

The dry-run derivation rule is frozen now; the numeric 24-row schedule is a required derived artifact
created from failure-free ordinary runs before primary timing. Once primary timing begins, changing a
schedule row, offset formula, workload/failure/scale weight, timeout/scoring rule, completion-before-
injection treatment, AQE/default-plan policy, resource limit, randomization/bootstrap seed,
confidence method, threshold, or economics assumption requires a new prospective version.

A publication miss, slow publication, unexpected hit rate, poor speedup, or unfavorable overhead is
an outcome. None grants permission to move failures, drop rows, redistribute weights, or rewrite the
campaign.
