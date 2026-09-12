# Draft SPIP: Optional reuse of completed batch shuffle across driver attempts

Status: discussion draft backed by an experimental fork. No PMC endorsement,
shepherd, public API commitment or release target is implied. The native
Iceberg/Celeborn proof has passed cold-process reuse and its bounded fault-control
suite; the evidence below establishes prototype feasibility, not release readiness.

## Summary and decision requested

Allow a replacement read-only batch execution to reuse one complete shuffle
from an earlier driver attempt when Spark can independently establish the same
resolved computation and a provider can retain and serve the exact committed
output. Treat reuse as an optional cache: a miss executes the current query
normally, and an adopted-read failure invalidates the entire adopted shuffle
and dependent work before recomputation.

The proposed first decision is agreement on this narrow semantic and lifecycle
boundary, followed by a reviewable experimental implementation. It is not
approval of a general driver checkpoint system or a permanent provider SPI.

## Problem and expected benefit

Driver loss can discard knowledge of completed expensive shuffle producers even
when a remote shuffle service still has their bytes. Replaying source reads,
filters, projections and shuffle writes may dominate restart cost. Reuse can
avoid that work without restoring arbitrary driver memory.

The relevant benefit is avoided producer execution minus certification,
discovery, claim and installation cost. Retention consumes storage and control
resources, including for queries that never restart. A tiny correctness fixture
cannot establish an economic or performance benefit. The evaluation must report
producer cost, replacement latency, bytes retained and read, claim cost, and
fallback latency across increasing mapper/reducer counts and realistic source
sizes. If those costs are unfavorable, always recompute remains the right choice.

## Goals and initial scope

- Default off; unchanged ordinary behavior for applications that do not opt in.
- Read-only batch replay with one complete adopted exchange in the initial slice.
- Independent planning and certification by the replacement driver.
- Generic source and shuffle-provider responsibilities without product-name
  branches in Spark Core or the SQL identity encoder.
- An explicit local transaction between preparation and ordinary map submission.
- Bounded provider work away from scheduler and listener callbacks.
- Correct whole-shuffle fallback, including dependent outputs and late failures.

The first native harness disables AQE and admits unaggregated SQL row shuffles
with reviewed hash/single partitioning. It does not establish adaptive-read
support. Later adaptive integration must use an explicit capability check for
each partition-spec shape; unsupported shapes must decline reuse before install.

Writes, streaming, arbitrary RDD/UDF replay, driver heap restoration, externally
delivered partial results, cross-user sharing, owner restart recovery, cross-build
reuse and sampled range partitioning are outside the initial proposal.

## Semantic identity: the current execution stays authoritative

The replacement resolves its catalog and source view normally. Recovery must not
silently pin an old snapshot when the query asks for the current one, suppress a
current planning/authorization error, or use an earlier manifest as its query plan.

A connector certifies the actual planned scan and ordered input partitions. Its
opaque token represents the source semantics and physical decomposition that
matter to reproducible shuffle output. The proof's Iceberg adapter binds the
resolved snapshot, schema and exact task groups using private connector hooks;
that implementation is evidence for the contract, not a proposed Iceberg API.
Unreviewed deletes, runtime-filter-sensitive planning and unsupported schema or
expression features decline certification.

Spark builds a closed, versioned canonical representation of reviewed producer
operators, resolved literals and semantic configuration, source certificate,
partitioning, mapper/reducer shape, row/serializer compatibility and provider read
format. Unknown operators fail closed. A digest locates candidates; adoption
compares the complete canonical payload. Driver-local shuffle IDs, exchange IDs,
application IDs and lease tokens are not semantic cache keys.

False negatives cost recomputation. False positives violate query correctness and
are unacceptable. Expansion of the supported expression/source set requires
semantic tests, not just a wider plan fingerprint.

## Publication and retained lifetime

Spark records the successful mapper attempts actually accepted by its scheduler.
A bounded publication worker asks the provider to seal exactly those winners.
Task success alone is insufficient: native worker commit may still be pending.
The provider must verify complete immutable output and exact attempt selection.
Spark checks tracker selection around publication and persists a versioned
manifest only for the accepted complete output.

A manifest contains identity, publishing generation/incarnation, bounded
scheduling metadata and opaque provider descriptor bytes. It conveys neither a
credential nor ownership of the retained data. Discovery failure is a cache miss.

The provider independently owns durable output and cleanup. A producer-to-reader
handoff lease survives producer shutdown for a bounded interval. Each replacement
obtains its own lease; releasing one must not revoke another. Claims use exact
owner incarnation and artifact namespace. Expired tokens cannot be renewed back
to life. Process-local leases do not claim survival across provider-owner restart.

The Celeborn experiment extends its existing standalone LifecycleManager. It
allocates native shuffle IDs independently of each Spark driver's local IDs and
keeps heartbeats alive after driver exit. It serves sealed native file-group
metadata and reads worker files without copying them into a Spark-owned format.
Another provider may implement the same lifecycle with a different descriptor.

## Preparation, installation and reads

A driver-local reservation is established before asynchronous discovery or claim
work. It binds the exact current dependency and materialization. Cancellation or
ordinary map submission fences that reservation; a late successful lookup releases
its claim instead of altering a running stage.

Preparation validates identity, shape, generation, provider format and descriptor,
then obtains a live read claim and prepares local scheduling state. Installation
atomically binds the current shuffle handle and installs complete tracker state
before missing partitions are selected. The transaction performs no remote call.
No adopted/fresh mapper mixture is allowed.

Executors read through the provider's native descriptor. They validate the current
driver binding and maintain their own bounded read leases. Renewal happens outside
the fetch critical path; local liveness checks surround reads and deserialization.
A lease heartbeat is periodic fencing, not an instantaneous global revocation
primitive. Full failure rollback is therefore part of correctness.

The current native prototype captures dense mapper-by-reducer scheduling estimates
with a hard cell limit. Those values are estimates, not exact physical lengths.
Native reading uses the provider's actual format. This bounded representation is
adequate for a small proof but is explicitly not the scalable target. A release
requires measured metadata costs and a compact/paged representation or a justified
supported-size bound. Paging must not introduce provider calls on DAGScheduler.

## Failure and cleanup behavior

Before installation, missing metadata, source differences, expired claims,
unsupported reads and provider errors decline reuse. The current execution runs
normally. Errors from the current source still propagate normally.

After installation, an unavailable/corrupt/expired adopted read reports failure
with its exact binding generation. Spark fences the binding, clears all adopted
map availability, invalidates tracker caches/epoch and rolls back affected
succeeding work before retry. Late failures from an older binding cannot revoke
fresh output. Synthetic binding locations must not be treated as failed physical
executors for host-wide cleanup.

Cancellation and shutdown fence locally and queue bounded provider cleanup. If
cleanup cannot be queued or contacted, expiry supplies the eventual release
bound. A cache miss or local cancellation never grants authority to delete a
shared manifest or another reader's retained data.

## Security and operational boundaries

Existing source authorization must be checked for the replacement execution.
A production provider must independently authorize the replacement to read older
artifacts; a matching group name, identity digest or generation is not sufficient.

The current standalone Celeborn integration is an unauthenticated isolated proof
using a private test deployment. It does not establish cross-principal reuse or
production authorization. A stable enabled feature requires an explicit provider
authorization/tenant-isolation contract and tests. It must not inherit an old
driver's credentials through a serialized descriptor.

Retention TTL, admission count, descriptor size, preparation concurrency and
cleanup queue sizes must be bounded and observable. Production configuration
needs hit/miss reasons, publication failures, claim latency, adopted/fresh task
counts, lease failures and fallback counts without logging sensitive source tokens.

## Alternatives considered

Always recompute is simpler and remains the baseline. Restoring driver state is a
much larger problem with different lifecycle and side-effect guarantees. Persisting
materialized query results can avoid shuffle-specific integration but changes the
materialization boundary and may not serve multiple downstream consumers.

Copying ordinary Spark shuffle files into a separate durable format is useful as
a reference experiment, but adds publication I/O and duplicates native provider
storage. Reconstructing dense ordinary map statuses alone does not establish native
read semantics or scalable metadata. The proposed boundary preserves provider
ownership while keeping correctness and scheduling decisions in Spark.

## Evidence and acceptance gates

The reference-provider cold-process tests establish a baseline mechanism. The
native workflow builds pinned Iceberg and Celeborn sources and launches independent
producer/replacement JVMs against an independently owned lifecycle service.

Native candidate `93c048e1d379d274e074cd3a9e77a636c01fe29c` passed
[native run 34677866614](https://github.com/unikdahal/spark/actions/runs/34677866614)
and the Core, SQL/AQE, quality and pinned Iceberg jobs in
[general run 34677977967](https://github.com/unikdahal/spark/actions/runs/34677977967).
The cold replacement and two concurrent replacements each returned the exact
32-row baseline, ran zero target maps and read 730 native remote bytes without
fetch failures. Persisted worker-file loss and real lease expiry each triggered
fetch failure and correct whole-shuffle recomputation. Source-token, filter,
manifest, snapshot-rewrite and owner-restart controls each declined reuse.
All 13 independent driver roles returned the same digest. See the
[implementation evidence](owned-poc.md#completed-native-proof) for measurements,
provenance and the distinction between successful reuse and SQL binding cleanup.

The required native feasibility gates are:

1. Current Core/SQL regression, style and license gates.
2. Pinned source conformance and provider retention/descriptor tests.
3. Producer exit followed by equal results and zero target map tasks in a new JVM.
4. Changed source token, producer filter, absent manifest and real snapshot rewrite
   each causing ordinary recomputation with correct results.
5. Real persisted worker-file loss after adoption causing an observed fetch failure,
   whole-shuffle invalidation and correct fresh computation.
6. Distinct process identities, exact commit/jar provenance and raw retained evidence.

Additional gates before a stable feature include concurrent replacements, lease
expiry during reads, owner restart rejection, cancellation/late-callback races,
multiple dependent stages, authenticated access, supported AQE shapes, metadata
scale and representative benefit measurements. These are requirements, not claims
that a successful 32-row run answers them.

## Delivery and review plan

Keep source certification and provider integration private/experimental while
reviewing the semantics. Review identity/source binding separately from scheduler
transactions, then review native read/failure behavior with a concrete provider.
Do not promote implementation classes wholesale into a public SPI.

The initial upstream discussion should include the smallest mechanism patch,
explicit unsupported cases, exact evidence and a measured value study. Request
SQL/source, scheduler/AQE and shuffle-provider review on their respective
boundaries. Public extension stability, authorization and adaptive support are
separate decisions after the prototype evidence. Acceptance cannot be guaranteed;
a narrow claim with reproducible evidence makes the design assessable.
