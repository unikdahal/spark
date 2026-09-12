# Early discussion: completed deterministic shuffle reuse across driver attempts

This note is an early design-discussion package. It asks for SQL, scheduler and shuffle-provider
feedback before the prototype freezes broad integration contracts. It does not claim Apache Spark
PMC endorsement, a shepherd, or a release commitment.

## Problem

A replacement Spark driver normally recomputes shuffle outputs even when a previous attempt
completed an equivalent deterministic shuffle and durable immutable bytes still exist. For
expensive read-only batch queries, that can duplicate substantial work after driver failure.

The proposal treats cross-driver reuse strictly as an optional cache. A replacement execution still
resolves its current query and source semantics normally. It may adopt one complete compatible
shuffle only when semantic identity, source view, authorization, provider durability/readability and
scheduler/read capabilities are all proven. Any uncertainty is a miss and ordinary Spark execution
remains available.

The frozen reference-provider experiment demonstrates a narrower mechanism: an independent JVM can
adopt one completed local durable shuffle, launch zero map tasks for it, read the retained bytes and
fall back as a whole shuffle after an adopted-read failure. That evidence is useful, but it does not
establish a production provider API, a scalable read representation, AQE compatibility, current
authorization, or a real remote-provider result.

## Initial scope

The revised proof-of-concept deliberately starts small:

- default off;
- read-only batch replay;
- ordinary current source/query semantics remain authoritative;
- one adopted exchange per query;
- one certified immutable scan, optionally reviewed projection/filter, followed by supported hash
  or single partitioning;
- complete-shuffle adoption only; never mix adopted maps with freshly recomputed maps;
- current authenticated authorization for reading older durable artifacts;
- bounded metadata, time, fanout, workers and provider I/O;
- AQE stays enabled, but an adopted shuffle may use only adaptive reads supported by explicit
  trustworthy capabilities.

Initially excluded are writes, streaming, externally delivered partial-result recovery, arbitrary
RDD reuse, cross-user sharing, broad UDF/Python/native expression support, runtime-filter-dependent
sources, sampled range partitioning and cross-build reuse.

## Provider risk to resolve early

The main external risk is not whether Spark can build a cache key. It is whether a real shuffle
provider can support the required lifetime and read semantics without brittle Spark-specific
assumptions.

A useful provider path needs to demonstrate, at minimum:

1. completed immutable bytes remain discoverable after the producer driver and executors are gone;
2. a later authenticated attempt may read those exact bytes without treating a group name or
   generation as a credential;
3. current-shuffle routing can bind to an older immutable artifact without revoking healthy readers;
4. fetch accounting uses authoritative physical lengths and distinguishes truly empty blocks;
5. attempt cleanup releases aliases/resources without destroying group-scoped artifacts;
6. provider unavailability or timeout is a cache miss, not destructive authority;
7. any destructive retirement is exact-incarnation/ABA safe;
8. metadata discovery and reads can be bounded without materializing an M×R driver matrix.

The reference provider cannot answer whether existing remote shuffle extension points expose those
semantics. The revised plan therefore puts a real-provider feasibility gate before defining the
minimal experimental provider contract.

## Competing implementation approaches

### A. Reconstruct ordinary `MapStatus` state in the replacement driver

This is the frozen feasibility mechanism. It is valuable because it reuses existing Spark stage
availability and fetch paths with a small scheduler hook.

Its limitation is fundamental for the target architecture: preparing every mapper's reducer-length
array is O(M×R), and ordinary compressed `MapStatus` values do not preserve exact provider physical
lengths for every fetch-accounting use. It remains a regression baseline, not the target scalable
representation.

### B. Compact recovered `MapStatus` markers plus exact metadata queries through `MapOutputTracker`

A current draft branch explores this direction: recovered statuses carry only a binding generation,
and tracker logic obtains exact block metadata before constructing fetch tuples. It avoids embedding
R reducer sizes in every mapper status.

The idea deserves comparison, but the current draft does not compile and has no reader-specific test
suite at its current head. It also risks making driver-mediated metadata query shape part of the
contract before a real provider demonstrates the right paging boundary.

### C. Provider-native adopted-read descriptor with bounded paging

The revised target is to let Spark own a compact, validated adopted-shuffle descriptor while the
provider supplies bounded exact metadata/read pages or another provider-native exact representation.
Spark still owns current semantic identity, scheduler adoption, failure fencing and fetch-accounting
requirements; the provider owns durable artifacts and provider-specific addressing.

This direction appears more compatible with O(M + R + descriptor) driver state, but Gate A should
measure a real provider before the private contract is frozen.

### D. Always recompute

This remains the correctness baseline and fallback. If the source/provider/identity/scheduler costs
make safe reuse too complex or the measured restart benefit is weak, the right result may be to stop
or narrow the proposal rather than weaken correctness.

## Questions for SQL and AQE maintainers

- What is the latest materialization boundary at which a supported producer's resolved source view,
  literals, expression-local semantic context and partitioning/decomposition facts can be certified
  without introducing another plan-serialization contract?
- Which adaptive read forms can be safely allowed from authoritative reducer aggregates alone, and
  where should a final partition-spec compatibility guard live?
- Is keeping local shuffle reader, mapper-local reads, partial mapper reads and skew splitting off for
  the first adopted-read slice a reasonable conservative boundary while AQE itself remains enabled?
- Are there existing exchange/query-stage lifecycle hooks that should own the "at most one adoption
  attempt per exchange" state instead of adding recovery-specific state elsewhere?

## Questions for scheduler and MapOutputTracker maintainers

- What is the smallest explicit transaction boundary for installing one complete adopted shuffle so
  stale lookup/claim callbacks, ordinary task submission, cancellation and invalidation cannot race
  into mixed state?
- Should an adopted shuffle be represented through a specialized tracker status/response, a distinct
  reader descriptor, or a narrower new internal path that bypasses ordinary `MapStatus` size
  semantics?
- Which existing epoch/cache invalidation mechanisms are sufficient for generation-fenced recovered
  metadata, and which need explicit incarnation/binding fencing?
- What evidence would be expected to show that provider/store I/O can never occur on the
  DAGScheduler event loop, including cancellation and cleanup paths?

## Questions for shuffle/provider maintainers

- Do current remote shuffle implementations retain complete immutable output in a form that remains
  addressable after producer driver/executor loss, and under what retention/authorization model?
- Is there an existing driver/executor extension boundary that can expose exact immutable-artifact
  discovery, authenticated later reads, current-shuffle binding and attempt-local unbind without
  creating a broad public SPI prematurely?
- Can exact physical block/range lengths be queried or paged with bounded fanout, and what provider
  index costs would that introduce?
- Can a provider distinguish authoritative missing/corrupt data from temporary unavailability well
  enough that Spark never treats ambiguity as deletion authority?
- If conditional exact-incarnation retirement is unavailable, is local whole-shuffle invalidation
  plus provider retention expiry an acceptable first contract?

## What feedback is useful now

Early feedback is most valuable on the architectural boundaries above: where identity becomes
complete, how much recovered-read specialization is acceptable, which provider semantics are
realistically available, and which scheduler/AQE hooks would be objectionable upstream.

A Spark PMC shepherd or experienced SQL/scheduler/shuffle maintainer willing to challenge the design
before the broad integration phase would be especially helpful. Seeking that feedback is not a claim
of sponsorship or endorsement. The next evidence gates intentionally allow a negative result if a
real provider/source pairing cannot satisfy the required contracts or if the measured value does not
justify the complexity.
