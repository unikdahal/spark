# Resumable Driver: Intermediate-Result Recovery Status

This document records the Spark-only boundary for recovery of completed intermediate results.
Celeborn and connector implementation details are outside its scope.

## Supported shuffle boundary

A regular, determinate shuffle can be adopted only when the recovery provider has made the whole
shuffle readable through the active shuffle manager. Spark installs complete synthetic map-output
state before submitting producer tasks. An absent record permits normal computation only when the
provider can authoritatively prove absence. Lookup ambiguity, invalid statistics, installation
failure, and a later fetch failure all fail the query instead of recomputing the adopted generation.

The current scheduler rejects these recovery-enabled producers before provider lookup and before
parent-stage creation:

- barrier stages, whose coordinated execution and possible external effects are not represented by
  shuffle metadata;
- statically indeterminate stages, for which a recomputed attempt need not produce the committed
  generation;
- push-merge-enabled shuffles, because the recovery record does not describe merge generation,
  merger locations, or per-reducer merge state; and
- pipelined shuffles, which do not have durable addressable output.

AQE stage reuse shares the adopted map-output result and recovered runtime statistics. Coalesced
and skewed AQE reads retain the adopted `ShuffleQueryStageExec`; they rearrange reads and must not
materialize another producer generation.

## Broadcast and table-cache results

Broadcast recovery is not implemented. A `TorrentBroadcast` is owned by the original driver:
its broadcast ID and metadata are not safely discoverable by a replacement driver even if some
executors still hold blocks. Safe recovery needs all of the following:

1. a durable, checksummed serialized-relation blob;
2. a semantic key binding the immutable execution, canonical query and broadcast plan,
   `BroadcastMode`, output schema, codec, encryption configuration, and Spark protocol version;
3. fenced first-writer publication and authoritative absence;
4. replacement-driver creation of a new local broadcast ID from the canonical blob;
5. bounded storage, retention, corruption handling, and losing-upload garbage collection; and
6. crash tests before blob publication, after publication, and after local broadcast creation.

Table-cache materialization is also not driver-restart recoverable. Its state can depend on the
cache serializer, storage level, schema, partitioning, source anchors, and surviving BlockManager
replicas. It needs a separate durable cache-generation protocol rather than reuse of shuffle
metadata.

The intended fail-closed boundary is: once an execution opts into a future strict
"recover every completed intermediate" mode, an unimplemented broadcast or table-cache stage must
be rejected before its producer job is submitted. This broad rejection is not enabled yet because
the present API advertises shuffle recovery only; applying it today would reject otherwise valid
queries without establishing that a broadcast or cache result had ever completed.

## Recovery protocol version proposal

The public API is being minimized and frozen separately. The smallest Spark-level version contract
proposed for that review is:

- `ShuffleStageRecoveryInfo.protocolVersion`, defaulting to Spark's current recovery protocol;
- `RecoveredShuffleStage.protocolVersion`, declaring the returned record's protocol; and
- exact equality validation before Spark mutates metrics or scheduler state.

Version 1 binds the meanings and ordering of mapper count, reducer count, reducer-byte totals,
data size, and optional row count. A mismatch is an adoption failure: Spark invokes
`abortRecovery` and refuses recomputation. Shuffle-manager-specific storage and codec versions stay
inside the provider's semantic key and catalog; the Spark protocol version does not replace them.

No version fields should be added until the public API freeze decides whether fields, capability
methods, or a versioned metadata envelope gives the best source and binary compatibility.

## Executor churn gap

The existing recovered-fetch-failure test proves that an unreadable adopted shuffle does not
resubmit map tasks. It does not yet prove behavior across real executor churn. Required external
evidence is:

1. adopt a non-empty shuffle through a real remote shuffle manager;
2. remove executors that ran the original producer and prove reads still succeed with zero map
   submissions;
3. remove a worker replica while retaining quorum and prove failover succeeds;
4. lose all canonical replicas and prove the consumer fails without map-stage resubmission; and
5. deliver late executor-loss and fetch-failure events from the old driver generation and prove
   they cannot invalidate or replace the fenced canonical generation.

These tests depend on the provider's real worker-backed storage and therefore are integration work,
not an in-process Spark scheduler test.

## Benchmark plan

Benchmarks must separate fingerprint cost, catalog lookup cost, and scheduler installation cost.

### Plan fingerprinting

- Plans: narrow, wide (100/1,000/10,000 expressions), deep, subquery-heavy, and exchange-reuse
  graphs.
- Compare canonical serialization plus SHA-256 with any proposed structured fingerprint.
- Record serialized bytes, allocation, throughput, p50/p95/p99 time, and sensitivity to
  `spark.sql.debug.maxToStringFields`.
- Verify semantically different plans never collide merely because a display string truncates.

### Large stage catalogs

- Catalog sizes: 10K, 100K, and 1M stage records.
- Recovery shapes: all hits, all misses, sparse hits, dense hits, corrupt entry, incompatible
  version, and provider timeout.
- Measure batch request/response size, lookup latency, driver heap, scheduler event-loop pause, and
  time until the first consumer task is submitted.
- Verify one provider call and one scheduler installation per unique shuffle despite AQE reuse.

### Scheduler installation

- Mapper/reducer matrices ranging from small stages to 1M synthetic map statuses.
- Measure `registerRecoveredShuffle`, status serialization, broadcast threshold crossings, heap,
  and MapOutputTracker epoch/cache behavior.
- Confirm benchmark setup never fetches synthetic locations as real local shuffle blocks.

All benchmark builds and runs must use the repository's two-CPU resource cap. Benchmark results are
not acceptance evidence until the real two-driver suite independently proves zero producer-task
submission.

## Focused verification inventory

Core scheduler tests:

- `recovered shuffle is available before any map task is submitted`
- `invalid recovered shuffle is aborted and not registered`
- `fetch failure from recovered shuffle fails without map-stage recomputation`
- `barrier shuffle recovery is rejected before provider lookup`
- `indeterminate shuffle recovery is rejected before provider lookup`
- `push-merged shuffle recovery is rejected before provider lookup`

SQL/AQE tests:

- `accepted empty shuffle recovery bypasses map tasks`
- `recovered statistics and map output are shared by reused shuffle stages`
- `AQE coalesced and skew reads retain a recovered shuffle stage`

The tests above are focused component evidence. They do not replace the external two-driver,
non-empty-shuffle, executor-churn, HA, compatibility, and scale gates.
