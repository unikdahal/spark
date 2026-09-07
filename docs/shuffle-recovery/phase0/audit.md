# Phase 0 baseline-to-head audit

Audit range before the final evidence PR:

`2a7cfea06ba135cf0ddc62902eb0daf5a835c672..b7179691629718a35d7db322396aa910a60efb03`

The range changes 43 files across the dedicated workflow, Core recovery/provider/scheduler code,
SQL opportunity analysis, focused tests, and evidence documentation.

## Findings fixed by the final evidence PR

### Cold-process evidence mislabeled the checked-out commit

The cold harness defaulted `testedCommit` to `GITHUB_SHA`. On a `pull_request` Actions event that
variable identifies GitHub's event merge commit, while the dedicated workflow explicitly checks out
`github.event.pull_request.head.sha`. Run 34070755581 therefore executed the requested PR head but
wrote the event merge SHA into every TSV row.

The final workflow passes `SPARK_SHUFFLE_RECOVERY_TESTED_COMMIT` from the same expression used by
checkout. This changes evidence labeling only; it does not change recovery behavior.

### Final exact-head gate did not directly exercise every accumulated Core boundary

The accumulated workflow had reference-provider, cold-process, and opportunity jobs, but the final
freeze contract also requires direct execution of the manifest, claim/validation/reservation,
scheduler/tracker, and adopted-failure suites on the exact candidate head. The final workflow adds
an explicit full-gate Core job rather than weakening or replacing the existing focused jobs.

## Correctness review

### Eligibility and opportunity

- Observed source support remains separate from counterfactual exact-token capability.
- The report does not convert excluded/reused physical work into weighted opportunity.
- The 18 unweighted final-plan observations remain visible; the frozen gate is not relabeled PASS.
- The 20% target was not changed after observing the 19.4% correlated exact-source result.
- Non-determinate, DPP, unsupported-expression, adaptive-partition, and determinism-unproven misses
  remain distinct root causes rather than being silently broadened into eligibility.

### Provider and exact fetch metadata

- Empty blocks are derived from equal adjacent physical offsets; there is no guessed zero.
- Non-empty block length is the exact physical offset delta.
- The provider validates reducer counts, data/index lengths, CRC/checksums, winner readiness, and
  namespace components before exposing a committed map.
- Oversized indexes and malformed/truncated state fail before unbounded allocation.
- Immutable winner selection cannot replace already committed bytes.
- Attempt cleanup does not delete a committed group-scoped winner.
- The exact index is O(M x R); the report discloses this rather than claiming production O(M + R).

### Manifest and trust boundary

- The feasibility identity is canonical, versioned, and deliberately narrow; old shuffle id is not
  identity material.
- Prefix lookup is only candidate fanout; full digest plus canonical payload are required to match.
- Manifest size, string sizes, mapper/reducer counts, handle count, and arithmetic are bounded before
  allocation.
- Unknown/truncated/trailing bytes fail closed.
- Provider/manifests do not deserialize Spark `MapStatus`.
- Accepted mutable bytes/collections are copied into owned immutable values at the validation
  boundary.
- Publication remains best-effort after ordinary Spark stage success.

### Reservation and scheduler transaction

- The reservation is local, single-use, versioned, and bound to the current shuffle dependency.
- Ordinary execution, cancellation, dependency replacement, shutdown, or a newer decision fences a
  late prepared result.
- Provider/index reads and exact per-reducer length construction happen before scheduler
  installation and are rejected by the external-call guard on the DAGScheduler event-loop thread.
- Scheduler installation constructs a complete replacement tracker state and publishes it with one
  CAS; no per-map partial recovered state becomes visible.
- The durable producer's old numeric shuffle id is not resurrected. Cold evidence binds origin id 0
  to replacement current id 1.
- The disabled/miss path continues through ordinary Spark execution.

### Adopted failure and healing

- A current adopted failure invalidates the resolver binding before clearing the complete tracker
  registration.
- Tracker caches are invalidated and the epoch advances.
- Retry requires a whole fresh map-stage recomputation; surviving adopted maps are not mixed with
  new local output.
- Synthetic recovered locations and local binding generations fence stale callbacks/events.
- Transient unavailability does not authorize destructive retirement.
- Missing/corrupt evidence can queue conditional retirement of only the exact examined incarnation.
- The cold proof demonstrates A -> full fresh B -> later B adoption with matching digest.

## Concurrency/resource review

- Provider winner races use create-once/immutable selection rather than last-writer-wins mutation.
- Reservation races are synchronized around a local decision token; the scheduler callback inside
  that fence performs local state mutation only.
- Fetch-failure invalidation is serialized by the local invalidation lock and uses CAS against the
  exact tracker status being invalidated.
- Release/retirement use a bounded single-worker queue and do not block the event loop.
- Queue rejection does not grant destructive authority or fail the user query.
- Cold child processes have finite timeouts, deterministic termination, output draining, and temp
  directory cleanup; the CI job also has an EXIT cleanup trap.
- No new third-party dependency is introduced by the Phase 0 diff.

## API/scope review

The recovery types remain `private[spark]`/`private[shuffle]` or test-scoped. The branch does not add
a public provider SPI, generic plan serializer, reflection fallback, production source API,
authentication design, remote provider implementation, streaming/write recovery, or generic RDD
cache semantics.

Source comments explain invariants and feasibility limitations rather than carrying implementation
issue numbers or agent instructions. Historical issue/run identifiers are confined to evidence and
documentation where they are part of reproducibility.

## Representation-cost review

The compact immutable manifest and the exact fetch representation must not be conflated:

- manifest: bounded, one descriptor per map plus optional one value per reducer;
- provider index: `M * (40 + 16R)` bytes with checksums;
- replacement preparation: exact R-length vector per map, deterministic raw payload `8*M*R` bytes
  before Spark-owned status construction;
- scheduler publish: M local status additions, one binding install, one tracker CAS, one epoch
  increment.

The O(M x R) provider index and replacement preparation are accepted Phase 0 feasibility costs and
explicit Phase 1 design requirements.

## History finding

The pre-report integration branch is 12 commits ahead of the frozen baseline. That is larger than
the nine sequential issue-owned commits expected after the final report because separately reviewed
follow-up evidence/audit/scheduler commits were already merged into the same branch.

The audit does not erase that fact and does not force-rewrite commits that are referenced by prior
exact-head Actions evidence. The machine-readable summary marks the literal total-history target as
unsatisfied. A future evidence branch must either define commit accounting that explicitly includes
approved audit follow-ups or enforce one-squash-per-sequential-issue before those follow-ups land.

Because the value gate is independently not met, this history discrepancy cannot be used to justify
proceeding to Phase 1.

## Final decision check

Mechanism correctness conditions are supported by the accumulated tests and cold-process evidence.
The opportunity requirement for an unconditional GO is not met under the preregistered rule.
Therefore the only supported decision is:

**MECHANISM_FEASIBLE / VALUE_GATE_NOT_MET**

No Phase 1 implementation should begin from this decision without an explicit, prospective public
re-scope of the value target/corpus and a new evidence plan.
