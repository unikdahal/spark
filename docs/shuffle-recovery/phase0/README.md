# Shuffle recovery Phase 0 evidence

## Decision

**MECHANISM_FEASIBLE / VALUE_GATE_NOT_MET**

Phase 0 demonstrates a fetch-correct cold-driver adoption mechanism for the supported feasibility
slice, but it does **not** satisfy the preregistered value gate required to proceed directly to
Phase 1. The result must not be described as a GO.

The frozen upstream baseline is:

`apache/spark@2a7cfea06ba135cf0ddc62902eb0daf5a835c672`

The final evidence PR was cut from integration head:

`b7179691629718a35d7db322396aa910a60efb03`

The squash commit produced by this report cannot be embedded in its own contents without creating
an additional post-freeze commit. The final squash SHA is therefore recorded in the merged PR and
Issue #10 closeout; `summary.json` records the exact pre-merge candidate head used by the final CI
gate.

## What Phase 0 asked

Phase 0 separated two questions that must not be conflated:

1. **Opportunity:** under conservative deterministic semantics, is enough completed shuffle work
   reusable to justify continued engineering?
2. **Mechanism:** can a genuinely cold replacement driver bind immutable outputs to its *current*
   shuffle id, install only validated local state, launch zero replacement map tasks, fetch the
   original physical bytes correctly, and fall back safely on every miss/failure path?

The mechanism question is answered positively for the deliberately narrow reference-provider
slice. The value question is not.

## Opportunity evidence

The preregistered evidence campaign is documented in
`opportunity/ISSUE_26_FINAL_EVIDENCE.md`. The pinned benchmark artifact is Actions run
`33963836467`, artifact `shuffle-recovery-phase0-a-issue26-evidence-33963836467`, artifact id
`9969032965`, digest
`sha256:15bb5b3c2ebfbfa565c95163c9a2644cd9a147971f39c62d23583a84f6090013`.

The run's overall Actions conclusion is `failure` only because a temporary post-measurement
validator still expected the obsolete v1 corpus id. The benchmark, focused suites, AQE regression,
listener regression, and artifact upload succeeded. The corrected workflow subsequently validates
the v2 corpus id. This historical failure is intentionally disclosed rather than hidden.

### Opportunity table

| Measure | Result |
| --- | ---: |
| SQL executions | 108 |
| Observed gate-rule exchanges | 1,082 |
| Materially completed shuffle-map stages | 763 |
| Correlated completed stages | 757 |
| Explicitly unweighted final-plan observations | 18 |
| Excluded/reused observations | 307 |
| Completed-task-time correlation coverage | 99.6% |
| Frozen correlation-quality gate | >=95.0% — **PASS** |
| Exact-source eligible exchange count | 35.6% |
| Exact-source eligible shuffle-write bytes | 39.5% |
| Exact-source eligible completed map task time | **19.4%** |
| Frozen value threshold | >=20.0% |
| Formal frozen value-gate result | **N/A** |
| Failure-distribution projection | 36.8% |

The formal value result is N/A because the preregistered report refuses to label the gate PASS or
FAIL while gate-rule observations remain explicitly unweighted. That policy was frozen before the
result was observed. The correlated exact-source estimate is 19.4%, which is below the 20% target;
there is no preregistered revised corpus/scope that permits a post-hoc pass.

### Failure-point completed/reusable work

| Failure point | Applicable executions | Completed exchanges | Eligible completed | Avoidable bytes | Avoidable task time |
| --- | ---: | ---: | ---: | ---: | ---: |
| After first eligible completes | 96 | 140 | 96 | 52.1% | 44.5% |
| After multiple upstream shuffles complete | 93 | 186 | 110 | 50.6% | 39.0% |
| Before most expensive shuffle completes | 106 | 342 | 164 | 50.3% | 30.4% |
| After eligible/ineligible mix | 71 | 214 | 130 | 55.4% | 38.5% |

These are counterfactual projections, not measured saved runtime.

### Scope curve

| Rule set | Eligible/count | Eligible/bytes | Eligible/task time |
| --- | ---: | ---: | ---: |
| Observed source support | 0.0% | 0.0% | 0.0% |
| Exact-source counterfactual | 35.6% | 39.5% | 19.4% |
| + DPP/runtime-filter source support | 46.3% | 56.8% | 33.1% |
| + window source support | 46.3% | 56.8% | 33.1% |
| + adaptive partition-spec candidate | 46.8% | 57.6% | 33.3% |

Observed source support and counterfactual exact-token opportunity are deliberately separate. The
counterfactual rows are not evidence that a production source adapter already exists.

### Top exact-source miss reasons

| Root miss | Count | Weighted task time |
| --- | ---: | ---: |
| Non-determinate | 320 | 32,068 ms |
| DPP present | 83 | 7,639 ms |
| Unsupported expression | 73 | 5,020 ms |
| Adaptive partition specs | 5 | 177 ms |
| Determinism unproven | 18 | 65 ms |

## Cold-process mechanism evidence

The latest successful accumulated mechanism run before this report is Actions run `34070755581`
on PR head `62eebca9513704c36a5b883c9c5aa943ac031385`. Its cold-process artifact is
`shuffle-recovery-phase0-cold-process-34070755581`, artifact id `10000661195`, digest
`sha256:85a0a7ed05794cd85f709d322f99c94908f18f92774310f7c89aa8d43366ba78`.

The historical artifact's `testedCommit` column contains the pull-request event merge SHA rather
than the checked-out PR head because the harness inherited `GITHUB_SHA`. The final workflow sets
`SPARK_SHUFFLE_RECOVERY_TESTED_COMMIT` explicitly from the checked-out head so newly frozen evidence
uses the correct semantic label. The historical artifact remains valid evidence for the run it came
from; its label is not presented as the exact PR-head identifier.

### Cold-process table

All happy-path replacement rows run in a fresh JVM, use generation 2 against generation-1 artifacts,
bind origin shuffle id 0 to current shuffle id 1, and compare row count plus SHA-256 result digest to
ordinary recovery-disabled execution.

| Case | Replacement map tasks | Provider reads | Provider bytes | Empty / non-empty blocks | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| sparse | **0** | 30 | 1,152 | 226 / 30 | adopted; digest matches baseline |
| empty | **0** | 0 | 0 | 8 / 0 | adopted; exact empty result |
| adjacent non-empty | **0** | 2 | 72 | 14 / 2 | adopted; digest matches baseline |
| skewed | **0** | 13 | 200,000 | 51 / 13 | adopted; digest matches baseline |
| one-row small | **0** | 1 | 36 | 3 / 1 | adopted; digest matches baseline |
| large | **0** | 4 | 9,438,048 | 4 / 4 | adopted; digest matches baseline |
| wide | **0** | 424 | 31,200 | 347 / 424 | adopted; digest matches baseline |
| abrupt producer exit / sparse | **0** | 30 | 1,152 | 226 / 30 | adopted after producer JVM kill |
| repeat fresh namespace / sparse | **0** | 30 | 1,152 | 226 / 30 | adopted; repeatable |

Negative controls for disabled recovery, group mismatch, same/non-later generation, source-token
mismatch, provider compatibility mismatch, missing manifest, digest collision, missing artifact,
claim unavailability, and stale reservation all reject adoption, perform zero provider reads, run
fresh map tasks, and match the ordinary result digest.

## Exact fetch representation

The reference provider stores one immutable `data` file and one exact reducer-offset/checksum
`index` per map winner. A reducer is empty iff adjacent persisted offsets are equal. Non-empty reads
return a file segment whose length is exactly the physical offset delta. Empty blocks produce no
fetch buffer. No synthetic positive size or fabricated zero participates in fetch correctness.

The checksum-bearing index is exactly `40 + 16R` bytes per map, therefore the Phase 0 reference
index is intentionally **O(M x R)**:

| Maps M | Reducers R | Exact provider index bytes | Bytes read per map open |
| ---: | ---: | ---: | ---: |
| 0 | 1 | 0 | 0 |
| 1 | 1 | 56 | 56 |
| 4 | 128 | 8,352 | 2,088 |
| 2 | 4,096 | 131,152 | 65,576 |

The independent-JVM provider proof also fetched and checksummed a 64 MiB block.

The immutable manifest itself is bounded to 4 MiB and stores O(M + R) descriptors/optional reducer
aggregates, not a reducer vector per map. However, `ShuffleRecoverySchedulerAdoptionState` currently
materializes an exact `Array[Long](R)` for each of M maps before constructing Spark-owned
`MapStatus` values. The deterministic raw length-vector payload is therefore `8 * M * R` bytes,
excluding JVM/array/object overhead, and status construction is O(M x R). This is an explicit Phase
1 scalability requirement, not a production compactness claim.

Scheduler publication itself is one complete local transaction: construct a replacement
`ShuffleStatus`, add M Spark-owned statuses, install one resolver binding, CAS the tracker entry once,
and increment the tracker epoch once. All provider index opening and O(M x R) length construction
happen before the DAGScheduler can consume the prepared value.

## Scheduler reservation and no-I/O result

Preparation uses a single-use, monotonically versioned local reservation bound to the current
`ShuffleDependency.shuffleId`. Cancellation, ordinary execution, dependency replacement, or a newer
decision makes a late prepared result stale. The scheduler hook consumes only already prepared local
state from the existing missing-partition path.

Provider, manifest-store, filesystem, and blocking recovery work are guarded from the
`dag-scheduler-event-loop` thread and are exercised by deterministic scheduler tests. Adoption does
not resurrect the producer's old numeric shuffle id: the producer evidence uses origin shuffle id 0,
while the replacement binds the same durable incarnation to current shuffle id 1.

## Whole-shuffle failure, fencing, and healing

The adopted-failure cold-process proof records:

- generation-1 producer A: 4 maps, origin shuffle id 0;
- replacement failure: A is retired after authoritative failure, **all 4 map partitions are
  recomputed** under the current driver, and generation 2 is published;
- later cold replacement: generation 2 is adopted with **0 map tasks** and the same row-count/digest.

Concurrent/stale-event tests clear the complete adopted tracker state, advance the tracker epoch,
fence the old synthetic recovered location, preserve fresh local output from late callbacks, and
permit retirement only after authoritative missing/corrupt evidence. Transient unavailability is
non-destructive.

## Correctness matrix

| Invariant / risk | Executable evidence |
| --- | --- |
| Cache miss falls back | `ShuffleRecoveryColdProcessSuite` negative-control matrix |
| Publication failure is non-fatal | `ReferenceShuffleProviderSuite`; manifest publisher failure/queue cases |
| Exact emptiness and physical size | `ReferenceShuffleProviderSuite` exact-index + large-block tests; cold sparse/empty/large cases |
| Malformed/oversized metadata bounds | `ReferenceShuffleProviderSuite`; manifest/claim hostile-input tests |
| Mutable provider input snapshot ownership | claim/untrusted-boundary tests exercised by the Core full gate |
| Stale reservation rejection | `ShuffleRecoveryColdProcessSuite` `reservation-stale`; scheduler adoption suite |
| No external scheduler-event-loop I/O | `ShuffleRecoverySchedulerAdoptionSuite` and external-call guard regressions |
| Current shuffle-id binding | cold-process origin `0` / current `1` assertions |
| No partial tracker installation | `ShuffleRecoverySchedulerAdoptionSuite` complete replacement/CAS tests |
| Whole adopted invalidation | `ShuffleRecoverySchedulerAdoptionSuite`; `ShuffleRecoveryAdoptedFailureSuite` |
| Stale consumer/event fencing | `ShuffleRecoverySchedulerAdoptionSuite` concurrent/stale callback tests |
| Authoritative vs transient retirement | `ShuffleRecoverySchedulerAdoptionSuite` unavailable vs missing/corrupt test |
| A -> B healing and later adoption | cold-process adopted-failure/healing proof |
| Attempt cleanup preserves group artifacts | `ReferenceShuffleProviderSuite` winner/cleanup test |

The final exact-head Actions run and job ids are recorded in `summary.json` and the PR after the
candidate is frozen.

## Reproduction

Reference-provider exact fetch proof:

```bash
./build/sbt -Phadoop-3 \
  "core/testOnly org.apache.spark.shuffle.ReferenceShuffleProviderSuite"
```

Core manifest/claim/scheduler/failure correctness gate:

```bash
./build/sbt -Phadoop-3 \
  "core/testOnly org.apache.spark.shuffle.ShuffleRecoveryManifestSuite org.apache.spark.shuffle.ShuffleRecoverySchedulerAdoptionSuite org.apache.spark.shuffle.ShuffleRecoveryAdoptedFailureSuite"
```

Cold independent-JVM proof:

```bash
rm -rf sql/core/target/shuffle-recovery-phase0/cold-process
mkdir -p sql/core/target/shuffle-recovery-phase0/cold-process
SPARK_SHUFFLE_RECOVERY_COLD_PROCESS_EVIDENCE_DIR="$PWD/sql/core/target/shuffle-recovery-phase0/cold-process" \
  ./build/sbt -Phadoop-3 -Phive \
  "set sql / Test / fork := true" \
  "sql/testOnly org.apache.spark.shuffle.ShuffleRecoveryColdProcessSuite"
```

Opportunity smoke/report correctness:

```bash
SPARK_SHUFFLE_RECOVERY_CORPUS_MODE=smoke \
  ./build/sbt -Phadoop-3 -Phive \
  "sql/testOnly org.apache.spark.sql.execution.exchange.ShuffleRecoveryOpportunityCorpusSuite"
```

The broad opportunity corpus is evidence-mode work and must use the pinned campaign inputs; it is
not an unconditional development-push benchmark.

## Explicit limitations and Phase 1 backlog

A positive mechanism result does not make this SPIP vote-ready. Remaining work includes:

- production versioned computation identity;
- a trustworthy source-read token adapter;
- a private versioned provider capability/lifecycle contract;
- scalable recovered read/statistics state plus an explicit AQE capability contract;
- authenticated group/generation/attempt context and current authorization;
- an integrated disabled-by-default supported slice;
- manifest-index concurrency/ABA semantics;
- deadlines, budgets, worker bounds, and circuit breaking;
- fault, security, retention, and lifecycle hardening;
- one real source/provider cold-process conformance deployment; and
- final restart-benefit, no-failure overhead, and storage evidence.

Phase 0 additionally leaves an O(M x R) exact provider index and O(M x R) recovered length-vector
construction in the feasibility path. Those costs are disclosed, not described as O(M + R).

## History/freeze audit

Before this report, `spip/shuffle-recovery-phase0` is 12 commits ahead of the frozen baseline.
That exceeds the nine-issue final-history target if every follow-up/audit squash is counted as a
permanent issue-owned integration commit. The extra history includes separately reviewed Phase 0-A
evidence repair/finalization work, the scheduler wiring follow-up, and the explicitly authorized
#34 analyzer-hardening follow-up.

This report does **not** destructively rewrite those already-evidenced commits merely to make the
counter smaller. Such a rewrite would replace the exact commit lineage referenced by prior Actions
artifacts. The discrepancy is recorded as a freeze-history defect in `audit.md` and `summary.json`.
It is another reason not to claim an unconditional GO.
