# Prospective completed-shuffle reuse value experiment v1

Status: **preregistered, not executed**

This document freezes the first prospective value campaign for the experimental completed-shuffle
reuse prototype before restart-speedup measurements are observed. It defines the target deployment
scenario, exact cohort, failure distribution, run counts, resource limits, statistical method, and
economics rule.

These are author-side experiment gates, not Apache Spark or PMC acceptance requirements. Changing a
threshold, cohort member, weight, supported shape, timeout rule, exclusion rule, or cost assumption
after performance observation requires a new version. Historical failed or indeterminate evidence
remains published.

Companions:

- [`workload-scope-v1.md`](workload-scope-v1.md) freezes exact SQL, dataset provenance, producer and
  downstream scope, and primary map/reducer envelopes.
- [`cost-model-v1.md`](cost-model-v1.md) freezes common-unit existing-provider and numerical
  greenfield durable-storage economics.
- [`value-experiment-v1.json`](value-experiment-v1.json) is the machine-readable campaign contract.

## Lineage and historical evidence boundary

- Frozen upstream baseline:
  `apache/spark@2a7cfea06ba135cf0ddc62902eb0daf5a835c672`.
- Frozen Phase 0 evidence:
  `7888d1e26d32e69694083e367ee6a92b28277c48`.
- Experimental branch base used for this preregistration:
  `5f773c4669d06af57216b09748ec7d168412ad3c`.
- The frozen result remains **MECHANISM_FEASIBLE / VALUE_GATE_NOT_MET**.
- The formal old value result remains **N/A** with 18 explicitly unweighted observations.
- The reported 36.8% failure-point opportunity is a projection. The separate 19.4% eligible
  completed map-task-time share is not a measured restart speedup.

Nothing here relabels or replaces that result. This campaign measures failure-to-correct-result time
on a later integrated real-source/real-provider candidate.

## Target deployment profile and provenance

Profile id: `decision-support-snapshot-rss-v1`.

The workload weights are an **explicit scenario**, not production traces. No claim is made that they
represent a particular company or deployment. Real trace weights, if later available, require a new
prospective campaign rather than an in-place replacement.

The target pairing is:

- immutable source: Apache Iceberg tables pinned to explicit snapshot ids;
- durable shuffle provider: Apache Celeborn as the first real remote-shuffle candidate;
- retry mechanism: Kubernetes batch application relaunch with a fresh driver and fresh executors;
- driver: 4 vCPU, 16 GiB memory;
- executors: 32 pods, each 8 vCPU and 32 GiB memory;
- worker network assumption: 10 Gbit/s;
- fact input scales: 16, 64, 256 and 512 GiB of Iceberg data-file bytes, each +/-2%;
- nominal target fact snapshot: 256 GiB;
- dimension snapshot: 8 GiB +/-2%;
- retained completed-shuffle lifetime: 2 hours;
- scenario driver-failure frequency: 1.0% of query attempts;
- greenfield economics arrival rate: 20 primary read-only batch query attempts per hour.

Iceberg and Celeborn are targets, not presumed compatible implementations. Failure of a later source
or provider gate is a gate failure or reason for a new prospective profile; this file is not silently
retargeted.

The replacement application uses the same requested immutable source snapshots and authenticated
retry lineage, but it receives new Spark application/attempt state, a fresh driver, fresh executors,
and current shuffle ids. Old numeric shuffle ids are never reused as identity.

## Reusable producer and scope

Exactly one exchange may be adopted in a primary query. Its child is frozen to:

```text
certified immutable snapshot scan
  -> optional reviewed deterministic filter
  -> optional reviewed deterministic projection
  -> hash partitioning OR single partitioning
  -> target exchange
```

No join, partial/final aggregate, Window, subquery, runtime filter, Python/UDF/native expression,
sampled range partitioning, or runtime-dependent scan may appear below the target exchange.

Downstream consumers use ordinary current Spark semantics. The exact corpus uses Window and
sort-merge-join distribution requirements so the target exchange can retain the narrow producer
grammar; aggregates are downstream only. Later exchanges execute fresh.

Excluded behavior includes writes, streaming, incremental external result delivery, partial-map
reuse, generic RDD shuffle reuse, unsupported expressions, runtime-filter-dependent scans,
runtime-dependent source resolution, sampled range partitioning, and cross-build reuse.

Any uncertainty is a cache miss. Ordinary Spark execution remains available.

## Frozen primary cohort

Exact SQL and structural admission rules are in `workload-scope-v1.md`.

| Id | Weight | Target producer and downstream consumer |
| --- | ---: | --- |
| `W01` | 20% | scan/filter/project -> hash exchange -> tenant-partitioned Window -> small result reduction |
| `W02` | 15% | scan/filter/project -> hash exchange -> dimension-partitioned Window -> small result reduction |
| `S01` | 10% | selective scan/filter/project -> single-partition exchange -> global Window -> small result reduction |
| `J01` | 25% | fact scan/filter/project -> hash exchange -> sort-merge join -> global aggregate |
| `J02` | 20% | fact scan/filter/project -> hash exchange -> sort-merge join -> later fresh aggregate shuffle |
| `E01` | 10% | sparse scan/filter/project -> hash exchange -> Window, stressing empty blocks |

Weights sum to 100% and are scenario frequencies, not measurements from the frozen Phase 0 corpus.

Primary evidence uses the tested build's defaults for AQE, shuffle partition count, broadcast
thresholds, and adaptive thresholds. AQE-disabled runs, forced shuffle counts, disabled broadcast,
join/repartition hints, or similar plan-shaping controls are **mechanism experiments only** and are
excluded from headline value, overhead, and economics gates.

## Supported map/reducer envelopes

Primary mapper counts come from certified source decomposition, not a benchmark-side `repartition`.

| Scale | Fact bytes | Accepted M | Hash R | Single R |
| --- | ---: | ---: | ---: | ---: |
| `I16` | 16 GiB +/-2% | 96-160 | 200 | 1 |
| `I64` | 64 GiB +/-2% | 384-640 | 200 | 1 |
| `I256` | 256 GiB +/-2% | 1,536-2,560 | 200 | 1 |
| `I512` | 512 GiB +/-2% | 3,072-5,120 | 200 | 1 |

The 200 hash reducers are the ordinary default initial shuffle partition count in the frozen Spark
lineage; the campaign does not set that value. AQE stays enabled and may change read partition specs
after materialization. Those final specs are recorded evidence, not forced back to R.

A row outside its registered source-decomposition or initial-reducer envelope is
`NOT_APPLICABLE`; settings are not changed after observing performance to make it fit. Mechanism-only
forced probes may include 8,192 x 1,024 and 4,096 x 2,048, but never enter the primary aggregate.

## Independent failure-free pilot and failure points

No restart-speedup pilot is used to choose the cohort, weights, or thresholds.

Failure-point structure comes from the already frozen independent Phase 0-A benchmark execution:

- GitHub Actions run `33963836467`;
- artifact id `9969032965`;
- artifact digest
  `sha256:15bb5b3c2ebfbfa565c95163c9a2644cd9a147971f39c62d23583a84f6090013`.

The benchmark queries themselves completed without injected driver failures; the workflow's overall
failure was a disclosed post-measurement stale corpus-id validator. The run predates this campaign
and measured opportunity, not replacement restart acceleration. It is used **only** to justify
stage-relative early/middle/late failure landmarks. Its opportunity percentages do not enter this
campaign's effect estimate, weights, confidence interval, or economics.

Conditional on driver failure:

| Id | Weight | Injection rule |
| --- | ---: | --- |
| `F0` | 25% | producer completed but durable publication is not yet visible |
| `F1` | 30% | durable publication visible; no material downstream post-exchange stage completed |
| `F2` | 30% | first material downstream stage after target exchange completed |
| `F3` | 15% | immediately before the final material downstream stage is launched |

`F0` is the pre-publication negative control. Recovery must miss and fall back; that trial is never
dropped merely because reuse is impossible.

A structural failure-free admission pilot may verify source decomposition, physical-plan shape, and
stage landmarks, but it must not collect or use restart timing. A cell lacking its registered
landmark is `NOT_APPLICABLE` before performance execution and its weight is not redistributed.
Primary scales each carry weight 25%, so one cell's registered scenario mass is:

```text
workload weight x failure-point weight x 0.25 scale weight
```

More than 5% total registered scenario mass not applicable makes the campaign **INDETERMINATE**.

## Timing boundary

Primary restart time is **failure-to-correct-result** wall-clock time. It starts when the benchmark
controller injects driver failure and ends only after the replacement execution has produced the
complete result and the controller has verified row count and deterministic digest against the
failure-free control.

Timing includes, as applicable:

- failure detection;
- orchestration and retry delay;
- Kubernetes pod allocation;
- driver/executor startup;
- source resolution;
- planning and AQE planning;
- recovery eligibility/identity work;
- discovery and lookup;
- provider metadata paging;
- claim/binding;
- recovery reads and shuffle fetch;
- downstream execution;
- result verification.

No detection, allocation, planning, lookup, or read time is subtracted from the recovery arm.

## Run count, pairing, randomized order, cache warmth and outliers

Each applicable `(cohort id, input scale, failure point)` cell uses **12 paired repetitions**. A pair
contains one ordinary-control attempt and one recovery-enabled attempt against the same snapshots,
data scale, cluster shape, and failure landmark.

There are 6 workloads x 4 input scales x 4 failure points = **96 registered cells**. If all cells are
applicable, the primary campaign therefore contains **1,152 pairs / 2,304 timed attempts**.

Pair order is block-randomized with seed `557003`, with six control-first and six recovery-first pairs
per complete cell. The exact generated order is retained in campaign artifacts.

Primary cache policy is natural backend warmth:

- no manual source-cache or provider-page-cache flush between paired attempts;
- randomized order prevents assigning warmth systematically to one arm;
- each failed producer and replacement uses fresh driver/executor processes;
- externally observable provider/source cache state is recorded;
- a labelled cold-provider-cache sensitivity run may be reported but cannot replace primary results.

There is **no statistical outlier deletion, trimming, winsorization, or "obviously slow" rerun**. A
harness failure may invalidate a run only under a predeclared external-health condition, such as
failure to allocate the configured worker count before injection. The invalid raw record remains
published and any replacement gets a new attempt id.

## Misses, failures, timeouts and missing data

Hard per-attempt timeout: **1,800 seconds**.

Recovery misses remain in the recovery arm. A safe miss that recomputes normally is not missing data
and cannot be excluded from the speedup distribution.

Predetermined scoring/reporting rules:

- recovery times out or fails to produce a correct result while control succeeds: use the observed
  control time and score recovery as 1,800 seconds; retain the concrete failure reason;
- control times out or fails while recovery succeeds: score **both** `T_control` and `T_recovery` as
  1,800 seconds for the primary effect, giving the pair exactly zero speedup credit; separately
  report the actual recovery observation and count the control failure against reliability;
- both arms fail or time out: score both arms as 1,800 seconds for the primary effect, giving exactly
  zero speedup credit, and count the pair against reliability;
- result digest or row-count mismatch: correctness failure; campaign cannot PASS;
- provider/manifest uncertainty, timeout, malformed metadata, authorization failure, or ordinary
  recovery rejection: safe miss; retain full observed recovery time;
- required timing/correctness evidence missing for a non-harness reason: campaign is INDETERMINATE
  unless classified by a rule above.

The campaign cannot PASS if more than 1% of ordinary-control trials fail to reach a correct result,
or if any recovery trial produces a wrong result. The timeout imputations above are deliberately
conservative and make every non-harness pair numerically defined before aggregation.

## Aggregation and confidence method

For each scored pair, let `T_control` and `T_recovery` be the observed or predeclared-imputed
failure-to-correct-result times.

Headline improvement:

```text
1 - weighted_sum(T_recovery) / weighted_sum(T_control)
```

Weights are frozen cohort weight x conditional failure-point weight x equal 25% input-scale weight.
The four input scales are also reported individually; scale weights are never inferred from observed
results.

The 95% interval is a deterministic **paired stratified bootstrap over repetitions while keeping the
registered scenario composition fixed**:

1. each `(workload id, input scale, failure point)` cell remains present exactly once in every
   bootstrap replicate;
2. within each applicable cell, resample its 12 scored control/recovery pairs together with
   replacement, preserving pair dependence and randomized-order blocks;
3. compute the cell control/recovery sums and reapply the frozen workload, failure-point, and 25%
   input-scale weights;
4. do not resample workload identities: the six workloads are the registered scenario itself, not a
   random sample from a workload superpopulation;
5. use 10,000 bootstrap replicates with seed `55003`;
6. physical exchanges or individual map/reducer blocks are never treated as IID observations.

Raw paired observations, imputation classifications, bootstrap input, seed, and rendered interval
are retained as evidence.

## Prospective value and overhead gates

### Restart benefit

PASS requires:

- aggregate failure-to-correct-result improvement >= **20%**; and
- lower 95% confidence bound **> 0%**.

A positive point estimate with a non-positive lower bound does not pass.

### Disabled-path overhead

Compare the recovery-disabled candidate with frozen upstream
`2a7cfea06ba135cf0ddc62902eb0daf5a835c672` on the same backend, source snapshots,
JVM/compiler, cluster shape, no-failure corpus, and randomized paired order. Upper 95% confidence
bound on runtime overhead must be <= **1%**.

### Enabled all-miss overhead

Enable recovery but use predeclared naturally absent/incompatible candidates. Do not weaken
validation or add a provider shortcut to manufacture a miss. Relative to the same candidate with
recovery disabled, upper 95% confidence bound on no-failure runtime overhead must be <= **5%**.

### Publication / no-failure overhead

Successfully publish a reusable completed exchange without injecting driver failure. Relative to the
same candidate with recovery disabled, upper 95% confidence bound on no-failure runtime overhead must
be <= **5%**.

All overhead campaigns use the same backend and primary corpus. Mechanism-only forced plans cannot
substitute for default-AQE results.

## Resource and provider-index limits

| Resource | Limit |
| --- | ---: |
| complete manifest bytes | 4 MiB |
| Spark driver recovery metadata for one adoption | 32 MiB |
| provider discovery payload per query | 8 MiB |
| examined candidate incarnations per exchange | 4 |
| provider index bytes charged to one retained artifact | 512 MiB |
| provider metadata page size | 1 MiB |
| provider metadata pages per adoption | 256 |
| recovery lookup deadline | 3 seconds |
| total pre-scheduler recovery preparation deadline | 10 seconds |
| concurrent provider reads per replacement query | 128 |
| retained shuffle artifact bytes per query | 512 GiB |
| retention | 2 hours |
| retained artifact byte-hours per query | 1,024 GiB-hours |

Provider-index bytes are charged separately from shuffle payload bytes. Extra durable routing/index
metadata must be reported, not hidden inside service overhead. These are prospective campaign limits,
not claims that the current implementation already enforces them. A later implementation unable to
operate safely inside them fails the profile or requires a new prospective version.

## Cost model and deployment economics

All economics use **scenario USD** so compute, memory, durable storage, traffic, and provider
infrastructure share one unit. Rates are planning assumptions, not vendor quotes:

| Cost item | Scenario rate |
| --- | ---: |
| executor/driver vCPU | $0.040 per vCPU-hour |
| executor/driver memory | $0.005 per GiB-hour |
| durable retained storage | $0.000040 per GiB-hour |
| incremental provider read traffic/service | $0.010 per GiB |
| incremental provider write traffic/service | $0.010 per GiB |

For every query attempt record:

```text
control compute cost
recovery compute cost
ordinary shuffle payload bytes written
incremental recovery-publication bytes written
provider-index bytes written
retained payload GiB-hours
retained index GiB-hours
incremental provider bytes read on recovery
```

Ordinary shuffle writes are reported but are **not double-counted** as a recovery cost when an
already deployed durable provider would have written those bytes anyway. Publication write cost
charges only bytes/service work introduced to make the artifact reusable. If already-written
immutable blocks can simply be retained, incremental copy-byte cost is zero while retention,
index, and service cost remain visible.

Conditional-on-failure benefit is converted to expected per-query benefit using the frozen 1.0%
driver-failure frequency.

For an already deployed compatible provider:

```text
net_incremental_value =
  expected compute cost avoided
  - expected incremental recovery read cost
  - expected incremental publication write cost
  - retained payload storage cost
  - retained provider-index storage cost
  - measured incremental provider service cost
```

The deployment-economics gate requires `net_incremental_value > 0` and lower 95% confidence bound
above zero under frozen scenario weights.

The cost of **introducing** durable shuffle storage is separately frozen in `cost-model-v1.md` as
`greenfield_introduction_cost`: provider workers, control-plane capacity, reserved storage, and a
setup/operations allocation in the same scenario USD unit. The registered greenfield profile uses
20 query attempts/hour, a 24 TiB reservation, total fixed cost `$6.12304/hour`, and fixed allocation
`$0.306152/query` before separately billable incremental traffic.

The final report must show both:

1. economics when a compatible durable provider already exists; and
2. economics when durable storage must be introduced for this feature.

A positive existing-provider result may not be presented as positive greenfield economics.

## Change control and decisions

Once any primary performance timing is observed, these cannot change in place:

- SQL corpus, source-scale admission rules, cohort weights;
- failure-point membership or weights;
- primary M/R envelopes;
- timeout and missing-data treatment;
- AQE/default-plan policy;
- cache-warmth and outlier policy;
- resource limits;
- confidence method or seeds;
- value/overhead/economics thresholds;
- scenario cost rates and greenfield footprint/arrival rate.

A material change creates `v2` or later and is prospective. v1 raw data and its decision remain
published.

Decision values:

- `PASS`: every correctness, restart-benefit, overhead, resource, and economics requirement passes;
- `FAIL`: a preregistered threshold is missed or a correctness/resource requirement is violated;
- `INDETERMINATE`: evidence quality, missing-data, reliability, or applicability rules prevent a
  valid decision.

No adverse result is converted into a prerequisite PASS.
