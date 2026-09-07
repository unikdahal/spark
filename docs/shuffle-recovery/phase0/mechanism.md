# Phase 0 mechanism

This document describes only the mechanism present on the frozen Phase 0 branch. It is a
feasibility implementation, not a production recovery API or semantic-identity contract.

## Reference provider and artifact layout

The local reference provider implements Spark shuffle-writer semantics and persists immutable map
winners below an encoded recovery-group / generation / incarnation namespace. Each committed map
winner has:

- an immutable `data` file;
- an exact `index` containing reducer offsets and optional checksums;
- a `READY` marker; and
- an immutable winner claim that prevents a later attempt from replacing the accepted bytes.

A non-empty reducer block is returned as the exact physical file segment between adjacent offsets.
Equal adjacent offsets mean the block is genuinely empty and no buffer is returned.

The checksum-bearing index costs `40 + 16R` bytes per map. Consequently this exact Phase 0 fetch
index is O(M x R). It intentionally prioritizes read correctness over the compact representation
required from a production provider.

## Feasibility-only identity

The cross-driver identity is deliberately closed over a tiny reviewed workload:

- exact source token;
- producer/operator tag;
- row-encoding discriminator;
- partitioning shape;
- mapper and reducer counts;
- reviewed resolved literal;
- frozen Spark/prototype compatibility id; and
- reference-provider compatibility id.

The identity is canonically encoded and SHA-256 hashed. A two-hex-character digest prefix is only a
candidate-enumeration fanout key; the full digest and complete canonical payload are revalidated
before a candidate can match.

Attempt-local stage ids, task ids, and old shuffle ids are not semantic identity. There is no generic
SparkPlan serialization, reflection fallback, public provider SPI, or production source identity.

## Winner selection and immutable manifest

The publication listener observes Spark's ordered scheduler-listener stream. For one successful
shuffle-map-stage attempt it freezes the exact successful map-task winner for every mapper only when
the stage completes successfully.

The listener performs bounded in-memory bookkeeping and a non-blocking queue offer. Provider and
manifest-store work runs on a bounded owned publisher worker, never synchronously on the scheduler
listener/event-loop path.

The immutable manifest records:

- format/identity/descriptor versions;
- recovery group and publishing generation;
- incarnation id;
- complete identity payload and digest;
- provider compatibility id;
- mapper/reducer shape;
- one exact provider handle/descriptor per mapper; and
- optional authoritative reducer aggregates.

Bodies, strings, counts, handle counts, and arithmetic are bounded before allocation. Truncated,
trailing, oversized, inconsistent, or malformed input fails closed. Final local-filesystem
publication uses fsynced temporary bytes followed by no-replace hard-link publication and directory
fsync; conflicting bytes cannot replace a committed immutable name.

## Typed claim and trust boundary

Provider claims return typed outcomes rather than throwing unstructured scheduler-facing failures.
Manifest/provider data remains untrusted until a single validation boundary has checked the group,
generation, full identity, provider compatibility, mapper/reducer shape, current target shuffle id,
handle ordering/counts, lengths, bounds, and provider descriptors.

Mutable input is copied/owned at the trust boundary. Durable metadata never deserializes or injects
Spark `MapStatus`; Spark constructs its own runtime state after validation.

## Reservation and prepared adoption

Each materialization uses a driver-local single-use reservation containing:

- materialization identity;
- **current** target shuffle id;
- dependency identity; and
- monotonically increasing decision version.

Asynchronous preparation may perform manifest/provider/filesystem work only before it offers an
immutable prepared result to scheduler-visible state. Cancellation, ordinary execution, dependency
replacement, shutdown, or a newer decision invalidates the reservation. A late result can therefore
lose safely without overwriting the winning execution path.

The explicit external-call guard rejects provider/store/filesystem preparation from the
`dag-scheduler-event-loop` thread.

## Current-shuffle binding and scheduler/tracker transaction

A replacement query creates its normal current `ShuffleDependency.shuffleId`. The recovery path
binds durable provider handles to that current id; it never resurrects the producer's numeric
shuffle id.

Before the scheduler sees a prepared adoption, the preparation thread opens every validated map and
constructs exact per-reducer physical lengths. This is deliberately O(M x R) in Phase 0 and happens
off the scheduler event loop.

Scheduler installation is then local only:

1. verify the dependency/reservation is still current and ordinary map output has not appeared;
2. build a complete replacement `ShuffleStatus` from Spark-owned `MapStatus` instances;
3. install one local resolver binding;
4. fence provenance for that binding generation;
5. CAS the old empty tracker status to the complete recovered status; and
6. increment the tracker epoch.

If any step loses a race, the binding is removed/released and ordinary execution remains available.
There is no partial recovered tracker install.

## Fetch failure, whole-shuffle invalidation, and fencing

Recovered bindings use a generation-specific synthetic block-manager location only as local runtime
provenance. A provider-side read failure is classified on the fetch thread before Spark's normal
`FetchFailed` reaches the DAGScheduler.

For the currently adopted binding, the failure path:

1. makes the old resolver binding unusable;
2. CASes the complete adopted `ShuffleStatus` to a completely empty status;
3. invalidates serialized tracker caches and increments the tracker epoch;
4. records one local invalidated-binding generation; and
5. queues release and, only when authorized, exact-incarnation retirement off the scheduler thread.

Fresh output is never mixed with surviving adopted maps. A retry after invalidation recomputes the
entire shuffle map stage.

Late callbacks/events name the old synthetic location and binding generation, so they cannot clear a
newly installed successor or fresh local map status. Duplicate callbacks are idempotently fenced.

## Authoritative versus transient retirement

Only authoritative missing/corrupt evidence authorizes conditional retirement of the exact examined
manifest incarnation. Unavailability or other transient uncertainty invalidates the local adoption
but has no destructive authority. A later authoritative observation for the same exact binding may
queue the retirement once.

Retirement is conditional on the exact group/generation/incarnation/digest tuple; it does not revoke
healthy concurrent readers or select a vague "latest" object.

## Healing

The cold-process failure proof exercises the full sequence:

- process A publishes generation 1;
- a replacement adopts A;
- injected authoritative failure invalidates the complete adoption;
- all mapper partitions recompute under the replacement driver;
- fresh generation 2 is published; and
- a later cold JVM adopts generation 2 with zero map tasks and identical result digest.

This proves successor healing for the narrow reference path without allowing A/fresh map mixing.

## Process lifecycle

The producer, replacement, negative controls, abrupt-exit producer, and later healed consumer are
independent JVMs. No Spark plan, RDD, scheduler object, classloader registry, or static in-process
state crosses the driver boundary.

Attempt cleanup/unbind removes the attempt-local alias only. Reference-provider group cleanup is an
explicit separate operation; ordinary attempt shutdown does not imply group-scoped artifact
destruction.

## AQE and statistics boundary

Phase 0 does not claim exact mapper-by-reducer statistics for AQE. The exact reducer lengths used to
make fetch-correct `MapStatus` values are routing/read metadata and must not be interpreted as a
production recovered-statistics contract. The manifest can carry optional authoritative reducer
aggregates, but the Phase 0 cold proof does not generalize them into broad AQE behavior.

Production work must define a scalable recovered statistics/read representation and capability
contract before enabling optimizations that require genuine mapper-level distributions, local
shuffle-reader locality, partial mapper reads, or skew splitting.

## Deferred decisions

Phase 0 deliberately defers:

- production computation identity and source token adapters;
- public or provider-vendor SPI commitments;
- authenticated group/generation/attempt authorization;
- object-store/remote-service publication semantics;
- O(M + R) exact read/statistics representation;
- broad AQE plan-shape parity;
- budgets, deadlines, circuit breakers, and production worker policies;
- durable retention/GC ownership;
- real remote provider/source conformance deployment;
- generic RDD, streaming, write, and cross-application shuffle recovery.

Those are design requirements, not capabilities implied by the Phase 0 proof.
