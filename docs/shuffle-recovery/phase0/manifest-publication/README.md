# Phase 0-B immutable shuffle manifest publication

This Phase 0 mechanism records one complete reference-provider shuffle after Spark has accepted the
winning map tasks for a successful shuffle-map-stage attempt. It is intentionally narrower than the
proposed production recovery identity and does not perform lookup/claim adoption or scheduler
mutation.

## Publication boundary

Publication observes Spark's ordered scheduler-listener stream through the existing
`spark.extraListeners` extension point:

1. `StageSubmitted` opens state for one concrete shuffle-map-stage attempt.
2. The first successful `ShuffleMapTask` event for each map partition records that task attempt id.
   `TaskSetManager` reports a speculative copy that loses to an already-successful attempt as
   `TaskKilled`, so it cannot replace the recorded winner.
3. A successful `StageCompleted` freezes the complete ordered winner vector and removes the attempt
   from listener state. A late loser/event cannot reopen or mutate that vector.
4. The listener performs only bounded in-memory bookkeeping and a non-blocking queue offer. All
   provider, manifest-store, and filesystem work runs on one bounded owned publisher thread.

Stage id, stage-attempt id, and shuffle id are routing/diagnostic values only. They are not semantic
identity material.

The Phase 0 listener is loaded explicitly with Spark's existing extra-listener mechanism. The
manifest path remains disabled unless `spark.shuffle.recovery.phase0.manifest.enabled=true` and all
required private Phase 0 settings validate.

## Feasibility identity

The version-1 identity is deliberately closed over only the deterministic Phase 0 workload:

- exact test/source token;
- reviewed producer/operator tag;
- row-encoding discriminator;
- hash/single-partition shape;
- mapper and reducer counts;
- reviewed resolved literal;
- frozen Spark/prototype compatibility id; and
- reference-provider compatibility id.

The canonical payload is deterministically encoded and SHA-256 hashed. A two-hex-character digest
prefix is used only to bound/index candidate enumeration. A reader always verifies both the full
256-bit digest and the complete canonical payload, so a short-prefix collision cannot become a
match.

This is **not** the Phase 1 semantic identity. There is no generic `SparkPlan` serialization,
reflection fallback, raw `SQLConf`, or attempt-local stage/shuffle/task identity in the semantic
key.

## Exact provider selection

The frozen winner vector contains the exact Spark map-task ids ordered by map partition. The
`ShuffleExecutorComponents` contract defines each map task id as unique within a Spark application.
For every frozen id the publisher requires exactly one complete reference-provider candidate, then
conditionally binds that exact candidate to the corresponding map index. It never asks the provider
for the "latest" output or chooses a winner by shuffle id.

If a winner is missing, duplicated, incomplete, corrupt, or conflicts with an already-bound map,
manifest publication fails. That failure is optional-cache failure only and cannot fail the
already-successful Spark stage/query.

The old fetch protocol is excluded from this Phase 0 compatibility set because it passes partition
id rather than task-attempt id as the ShuffleDataIO map id. Publication fails configuration closed
instead of treating those identifiers as interchangeable.

## Immutable manifest and store

A manifest records:

- manifest and identity versions;
- recovery group and positive publishing generation;
- immutable incarnation id;
- full canonical identity payload plus digest;
- provider compatibility id;
- mapper/reducer shape;
- ordered exact provider winner handles/descriptors; and
- optional authoritative reducer aggregates (absent rather than fabricated in Phase 0).

Manifest bodies are bounded before allocation/decoding. Strings, mapper/reducer counts, handle
counts, cumulative encoded size and integer arithmetic are validated. Unknown/truncated/trailing
bytes fail decoding; mutable bytes do not cross the codec boundary.

For the local reference filesystem, immutable visibility uses this sequence:

```text
write unique temp -> fsync temp -> create final hard link (no replace) -> fsync directory -> unlink temp
```

Unlike `ATOMIC_MOVE`, hard-link creation is specified to fail if the final path already exists, so
a committed manifest body/reference can never be replaced in place. An exact duplicate is accepted
only after byte-for-byte comparison; different bytes at the same immutable name are a conflict.
Readers never enumerate temporary names.

This is a local-filesystem feasibility transaction. It must not be presented as an object-store
commit protocol.

## Generation semantics

Phase 0 injects a positive generation through private test/internal configuration. A reader only
considers manifests from strictly earlier generations. Malformed, zero, negative or overflowing
values disable/reject the optional recovery path; no invalid value becomes wildcard state.

Generation is ordering context, not authorization. Authentication and production lifecycle
ownership remain Phase 1 work.

## Failure and resource behavior

Publication is best effort after ordinary Spark success. Codec failure, missing provider winners,
queue saturation, store failure, immutable conflict, or publisher shutdown are observable misses;
they do not fail the successful stage/query.

The publisher owns exactly one daemon worker and a finite queue. Scheduler-listener callbacks never
wait for provider/storage I/O. Application end stops admission and deterministically drains the
bounded queue, falling back to interruption after a finite timeout.

## CI evidence

The existing Phase 0 reference-provider cross-JVM job now also proves the manifest control plane:

- process A writes exact sparse/empty/skewed provider winners and publishes an immutable manifest;
- process B starts in a fresh JVM, discovers the manifest, verifies digest plus full identity,
  rejects same/future generation lookup, and reads the referenced provider bytes;
- a late loser after the StageCompleted boundary cannot change the frozen vector;
- an incomplete winner vector publishes nothing;
- exact duplicate manifest bytes are idempotent and conflicting bytes cannot overwrite them;
- a deterministic short-prefix collision still resolves only by full identity;
- oversized/truncated/trailing manifest input fails closed;
- concurrent exact publication cannot expose torn bytes;
- backend failure is non-fatal;
- bounded queue saturation is a non-blocking rejection; and
- publisher shutdown terminates its owned worker.

The prior reference-provider suite remains the fetch-correctness gate for empty/non-empty blocks,
physical sizes, corruption, concurrent winner binding, namespace safety, cleanup and cross-JVM
storage behavior.

## Deliberate limitations

Phase 0 does not claim:

- production computation/source identity;
- public recovery APIs;
- manifest lookup/claim reservation semantics;
- `MapOutputTracker` or scheduler adoption;
- AQE/statistics restoration;
- authenticated group/attempt ownership;
- remote-service/object-store durability semantics;
- healing/retirement; or
- production retention/GC.

Those remain owned by the later staged issues. This issue only proves that Spark-authoritative
winning output can be captured, certified and described by an immutable bounded record that a fresh
JVM can decode safely.
