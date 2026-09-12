# Generic source and shuffle-provider integration

This is the implementation direction for the owned PoC, not a published Spark API or a claim that
all provider paths are implemented. Iceberg and Celeborn are the primary source and storage targets.
Core must work through capabilities and validated contracts, without branching on either name.

## Separate responsibilities

| Component | Responsibility | Must not decide |
| --- | --- | --- |
| Source connector | Certify the actual resolved read and ordered input decomposition. | Which old shuffle incarnation to adopt. |
| Spark SQL | Encode supported producer semantics, output format and partitioning. | Connector snapshot selection or provider retention. |
| Spark scheduler | Select accepted map winners, fence local decisions and invalidate consumers. | Provider file layout or connector-specific identity fields. |
| Shuffle provider | Seal, retain, claim and serve completed output with its own format and authorization. | Current source semantics or current SQL computation identity. |
| Recovery coordinator | Match full identities and orchestrate bounded asynchronous publication/claim. | Reinterpret unsupported reads or suppress ordinary source errors. |

## Source certificate

Certification observes the same resolved scan and ordered input partitions used by the current
query. It must not independently resolve a table name, substitute an older snapshot, or create a
second planning result. The certificate binds opaque versioned source facts to the exact mapper
decomposition. Spark adds the reviewed SQL producer, output and shuffle-partitioning semantics.
The connector binding must also identify the certification protocol and version, so unrelated
connectors cannot accidentally equate identical opaque bytes. A token is not a provider locator.

The implementation adds `ShuffleRecoverySourceToken.forProtocol` to frame a protocol ID and
opaque certificate with explicit lengths and an envelope marker. The protocol version remains in
the source token's version field. Protocol identifiers use strict UTF-8 with a 1 KiB encoded bound;
the complete frame, including metadata, must fit the existing 64 KiB source-token bound. The
factory defensively owns the certificate bytes. Protocol IDs are compared exactly; no case folding
or Unicode normalization is implied. The Range fixture now uses this factory. Legacy `copyOf`
callers retain their existing bytes, so adoption of the protocol factory is explicit rather than
an assertion that every existing caller already supplies namespaced facts.

This framing identifies a compatibility protocol; it does not authenticate a connector or establish
that its claims are true. A future source binding must associate the certificate with the actual
configured connector and planned scan. The framing patch and its regression tests still require
exact-candidate compilation and execution before a passing conformance result is claimed.

The connector either provides immutable bounded facts or explicitly refuses certification. Normal
source resolution and authorization happen first; their failures retain ordinary query semantics.
A certificate establishes read reproducibility, not authority to access source or retained output.
Unknown expressions, runtime-dependent planning and unsupported source features remain ineligible.

The current Iceberg spike demonstrates these facts using private connector hooks. The next source
integration must adapt those facts into a connector-neutral binding tied to the actual scan object.
The built-in Range adapter currently exercises canonical publication/recovery without such hooks;
it is a test fixture, not the connector contract itself.

## Provider contract

The configured provider supplies a versioned read-format compatibility ID. That ID enters the
computation identity before publication or lookup. A manifest cannot choose or load a provider
implementation; the configured implementation must recognize and validate any offered descriptor.
Matching a format ID alone does not authorize reads or establish descriptor validity.

Publication describes one completed shuffle with the scheduler-accepted map attempts. The provider
must durably seal that incarnation before Spark publishes it as reusable. A claim must establish
availability and retention for the replacement attempt before the scheduler suppresses map work.
The claim binds the recovery group, publishing incarnation, current attempt and current target.

The eventual read contract must address logical mapper/reducer ranges without requiring every
provider to expose local data/index files. A provider may use files, remote objects or its own block
protocol. Opaque provider descriptors remain bounded, versioned and validated by the selected
provider. Core retains only the information it needs for scheduling, empty ranges, resource bounds
and invalidation. The existing reference descriptor/read path is an initial format, not a required
layout for Celeborn or other implementations.

Remote publication, discovery and claim operations must stay off the DAGScheduler event loop.
Timeouts, cancellation and pre-adoption unavailability produce ordinary recomputation. Failure
after adoption requires fencing the binding and invalidating every dependent consumer before
recomputation, including any AQE or partially consumed state. Mixing outputs from two incarnations
must not be an implicit fallback.

## Implementation order and evidence

1. Carry canonical identities through publication, discovery and adoption. Exercise independent
   processes, changed SQL semantics, changed source facts and changed provider-format IDs.
2. Bind a connector certificate to its actual Spark batch scan. Connect Iceberg through that binding,
   with ordinary latest/snapshot/error behavior preserved.
3. Replace reference-specific retained-read assumptions with a provider contract. Integrate Celeborn
   against verified seal, claim, retention and read capabilities; do not infer them from API names.
4. Exercise the same contract tests with a second source/provider fixture to detect hidden coupling.
5. Prove late-failure invalidation and AQE behavior, then measure first-attempt cost and recovery value.

The first item passed for candidate `4aa9889f9350bad49f9b873d1ec9017a281c5cfa` in
[run 34316394901](https://github.com/unikdahal/spark/actions/runs/34316394901). The subsequent
provider-format input refinement requires its own exact-candidate validation. Items two through five
remain outstanding. The provider-selected format ID is an explicit SQL-builder input; the manifest
identity envelope preserves complete canonical bytes and supports opaque connector tokens. These
changes do not by themselves finish a generic public SPI or a Celeborn-backed recovery deployment.

## Iceberg planning constraint for the next adapter

The existing spike captures a planning event while forcing the query's actual scan planning.
Calling it after a caller has already materialized that plan may produce no event and must remain
unsupported. The integration must arrange the capture before planning, or obtain a certificate
retained by the connector alongside its planned tasks.

Reading a mutable table's current snapshot after planning is not an adequate replacement. The
[pinned SnapshotScan implementation](https://github.com/apache/iceberg/blob/e76d63584d7f83b102026749e1ae0f91813cb78e/core/src/main/java/org/apache/iceberg/SnapshotScan.java)
resolves an unspecified snapshot through the table's current metadata. That getter does not itself
prove which snapshot produced an already cached task list. The adapter must bind the certificate
to the read already planned, not perform another latest-snapshot resolution.

### Binding certificates to planned batch reads

The internal `ShuffleRecoverySourceBinding` adapter associates a protocol certificate and
ordered split descriptors with one `BatchScanExec` and its exact `InputPartition` objects.
The canonical builder admits that batch source only through this binding. A raw source token
alone does not certify a batch scan. Object identities are local validation facts and never
enter persisted identity bytes, so equivalent independently planned reads can still match.

The initial adapter refuses runtime filters, grouped partitions, empty reads, mismatched
partition objects, and oversized descriptors. It copies certificate and descriptor bytes.
Ordinary partition-planning exceptions propagate. A connector must still guarantee that
its certificate describes the actual planned read and that partition semantics remain
immutable: object identity cannot establish either guarantee on its own.

This is an internal single-source integration boundary, not a released connector API.
The out-of-tree Iceberg conformance adapter now feeds actual planned scans into this
binding and the canonical SQL builder. Automatic publication/recovery wiring and
Celeborn-native retention and reads remain outstanding. SQL canonical encoding advances to v4 for this
additional source operator; previous SQL identities conservatively miss.

The Iceberg bridge checks that each Spark input partition owns the exact cached Iceberg
task-group object certified in that ordinal. Its split descriptor includes the complete
ordered decomposition digest and ordinal. Conformance compares independent planning,
snapshot advancement, and a pinned older snapshot at a real hash-shuffle boundary, with
exact result checks. Merely implementing `HasPartitionKey` does not mean a read is grouped;
the binding rejects planned grouping through `BatchScanExec.keyGroupedPartitioning`.
This distinction matters because the pinned Iceberg partition implementation exposes that
interface even for ungrouped reads.

`ShuffleRecoveryCertifiedBatchInputs` is the connector-neutral handoff to the existing
publication and recovery identity inputs. It reads the exchange's semantic settings,
requires canonical admission, and checks the certified mapper count against the actual
shuffle dependency before returning `ShuffleRecoveryCanonicalInputs`. The selected provider
format remains an explicit argument. The Iceberg conformance bridge delegates to this
handoff. Ordinary reader-factory and dependency-planning failures propagate; this helper
does not publish artifacts, reserve adoption, or suppress ordinary query errors.

### Cold-process source adapters

The test-only `ShuffleRecoveryColdProcessSource` contract lets an external connector reuse
the existing producer, durable manifest, replacement-driver, and negative-control harness.
`SPARK_SHUFFLE_RECOVERY_TEST_SOURCE_ADAPTER` selects a class from the test classpath, loaded
independently in each child JVM. The adapter supplies session options, reconstructs the
query from durable source facts, and returns its certified identity inputs. Certificate
capture must happen during query planning, before the harness requests its exchange.
The returned query follows the harness's `id`, `k`, `payload` result contract.

The shared-filesystem proof exercises this entry point with the deterministic Range adapter,
including source-token, missing-artifact, and producer-filter controls. Existing cold-process
suites retain their original built-in source when the adapter setting is absent. An actual
Iceberg adapter must additionally capture the exact planned scan, persist source data across
children, and avoid reconstructing certificates from a later `latest` snapshot. The out-of-tree `ShuffleRecoveryIcebergColdSource` now implements this contract;
its cross-driver execution requires validation on the new candidate.

The dedicated Iceberg runner now invokes `cold-process.sh` after source conformance. Setup,
baseline, producer, replacement, and each negative control run in separate forked JVMs.
Only the warehouse and retained artifacts survive scratch cleanup. The runner requires
32 identical result rows, distinct process identities, a different replacement shuffle ID,
zero selected map tasks and positive provider reads on adoption, and recomputation without
provider reads for every negative control. One control overwrites the Iceberg table with
the same values and verifies that the committed snapshot ID changes before recovery.
This real snapshot change is separate from the synthetic source-token discriminator.

This new Iceberg proof uses local executors and the reference provider. It does not prove
Celeborn retention, remote Iceberg executors, production storage semantics, or automatic
query integration. CI preserves raw child logs, result evidence, and both snapshot IDs.

### Block-provider integration path

`ShuffleRecoveryBlockProvider` now separates scheduler adoption and recovered-block resolution
from the concrete reference claim provider. Implementations supply claim/release and fenced
bound-map views through `ShuffleRecoveryResolvedMap`; the reference provider implements this
contract. A map exposes logical block offsets, exact sizes, and managed buffers without
requiring callers to know a local file path. Buffer ownership transfers to the reader.
The resolver rejects missing nonempty blocks and length mismatches and releases buffers
when invalidation wins during acquisition. These implementation changes await batch validation.

This is still the existing block-addressed compatibility path. Claim metadata still includes
exact index information, scheduler installation still reconstructs map statuses, and reads
still pass through the replacement driver's block resolver. It does not yet provide a compact
executor descriptor, provider retention lease renewal, or direct Celeborn reads. Implementing
this interface alone must not be reported as completing the native provider protocol.

The provider-side implementation is maintained on
[`unikdahal/celeborn`, `spip/retained-shuffle-poc`](https://github.com/unikdahal/celeborn/tree/spip/retained-shuffle-poc).
Its initial bounded lifecycle leases fence delayed cleanup. They are process-local and
must be owned by a lifecycle service independent of Spark attempts before they provide
cross-driver retention. Spark's reference provider now wraps opened map views with a
live-binding check, including buffer release when the binding expires during acquisition.
Both changes remain unvalidated pending the combined implementation batch.

### Native publication envelope

The manifest codec now has a version-two native descriptor mode. It carries bounded opaque
provider bytes alongside the complete identity, shape, publication generation, and incarnation.
Native manifests have no per-map file-index artifacts; mixed modes and empty native descriptors
are rejected. Existing indexed manifests still encode as version one. The overall manifest limit
is now 8 MiB, with at most 5 MiB of native provider bytes. The existing immutable filesystem store
and full-identity discovery logic support both modes. The indexed preparation validator explicitly
rejects native manifests rather than constructing synthetic block metadata.

`ShuffleRecoveryNativePublicationBackend` binds publication to a certified exchange's local
shuffle ID and canonical identity. It requires the provider's compatibility ID to match the
identity, complete scheduler attempt coordinates, and partition-ordered tracker agreement before
and after provider sealing. The generic provider callback receives task IDs, stage attempt IDs,
and task attempt numbers. Providers perform their native encoding outside Core. The listener
retains these coordinates across stage retries, including maps reused from earlier attempts.
Tracker validation now compares the full winner vector under one shuffle read lock.

This is the publication contract and storage implementation. A provider callback, native consumer
preparation and installation, SQL integration, and renewal/invalidation wiring are still required
for an executed native recovery path. The new regression suites are written, not run. No native
end-to-end success is claimed by the manifest codec or its publication backend.

### Celeborn publication attachment

The development integration under `dev/shuffle-recovery/celeborn-native-spike` implements the
native publication provider against the Celeborn fork. It checks that accepted stage/task attempt
coordinates fit Celeborn's 16-bit fields before encoding them. Its provider read-format ID pins
the native framing implementation and includes the configured compression codec; Spark row and
serializer compatibility remain separate certified identity fields.

`ShuffleRecoveryCelebornPublication.attach` takes the context produced for a certified exchange
and installs the generic native listener on the actual driver. The listener tracks only the
selected shuffle and delegates sealing and persistence to the bounded publisher worker.
After map-stage completion, the session's `finish` drains queued listener events and publication,
then requires a manifest matching this generation and incarnation. It cannot report an older
compatible manifest as this producer's successful publication. This attachment must run before
the map stage starts and finish before producer shutdown or shuffle cleanup.

The adapter sources deliberately live outside Core and require both fork artifacts when compiled.
They are not loaded by ordinary Spark execution. Native consumer installation and the cold-process
runner still need wiring; the attachment is not yet an executed integration test.

### Native manager and scheduler hook routing

This Spark branch requires the default shuffle manager to implement `BlockingShuffleManager`.
The Celeborn artifact still implements the older `ShuffleManager` contract. The development
adapter `org.apache.spark.shuffle.celeborn.ShuffleRecoveryCelebornManager` delegates ordinary
writes, reads, cleanup, and block resolution while satisfying the branch's manager interface.
Use this class for `spark.shuffle.manager` when running the native spike on this branch. The
publication attachment unwraps its Celeborn delegate explicitly.

Scheduler recovery dispatch now accepts `ShuffleRecoverySchedulerBackendProvider` on a manager.
The generic backend interface contains the existing local adoption, failure classification,
whole-stage rollback, and adoption-status hooks. The indexed implementation implements that
interface and remains reachable through its existing resolver. Its concrete preparation API stays
available to the reference harness. A native manager no longer needs to manufacture a reference
resolver merely to receive scheduler hooks.

The Celeborn wrapper currently delegates ordinary native reads. A native adoption backend,
validated scheduler output metadata, executor descriptor installation, and lease-driven failure
handling remain to be implemented. The wrapper and dispatch changes have not been compiled or run.

### Native scheduling metadata

Native publication now requires a bounded capture of map-output estimates from Spark's accepted
tracker state after sealing. Capture checks the partition-ordered winner vector under the same
shuffle read lock used to copy each map's reducer estimates. It preserves zero and nonzero values
and task IDs. These are Spark scheduling estimates, not assertions about native physical blocks.
The provider descriptor still determines the actual stream contents and native mapper filtering.

Manifests carrying these estimates use format three; formats one and two remain readable. Dense
scheduling metadata is capped at 131072 mapper/reducer cells independently of the provider byte
limit. This is a PoC size gate, not a scalable compact statistics protocol. Publication beyond the
gate is skipped rather than inventing sizes. `ShuffleRecoveryNativeMapStatus` restores the captured
estimates without applying another lossy compression pass and validates its serialized lengths.
The class is intended for the native adoption transaction; it is not yet installed by that path.
Tracker capture and manifest regression tests are written but remain unexecuted.

### Native adoption transaction

`ShuffleRecoveryNativeAdoption` implements the generic scheduler backend for prepared native
claims. Preparation must first register the exact dependency and materialization reservation.
An offer validates full identity encoding/digest/payload, earlier generation, provider format,
shape, descriptor bytes, and native scheduling metadata. It builds the replacement tracker status
before handing the result to the scheduler. A local installation callback publishes the task
binding under the same reservation fence as the tracker replacement. Ordinary execution that
arrives before preparation is ready consumes the opportunity; late preparation is released.

Every binding has a generation-specific location. A fetch failure naming that location fences
the binding, replaces the entire adopted tracker status with an empty status, advances the epoch,
and requests whole-stage rollback once. Failures naming older bindings cannot clear a newer
registration. Cancellation removes pending work and fences adopted bindings. Deferred cleanup uses
a bounded worker; local invalidation must stop renewal even when the cleanup queue is exhausted,
so eventual server lease expiry does not depend on that queue draining. The native adoption table
and generation-location history each have a 256-entry PoC admission limit.

The installation contract is local-only except for close. It does not grant provider implementations
permission to perform RPCs or wait for streams on the scheduler thread. The Celeborn handle/reader
implementation still needs to implement this contract. Regression tests cover late preparation,
cancellation, matching versus stale failures, and the one-shot rollback marker; they are written
but have not run. This transaction alone is not an end-to-end native read demonstration.

### Celeborn native consumer wiring

The development manager now wraps native shuffle handles with a serializable adoption binding.
Preparation registers the reservation before discovery, validates the canonical manifest and
provider read format, acquires an independent driver lease, and offers the local installation to
`ShuffleRecoveryNativeAdoption`. The preparation helper returns a session only after a successful
offer; the scheduler can still let ordinary execution win before commit. Closing the session
fences the materialization and queues provider cleanup.

Adopted executor reads deserialize Celeborn's native partition streams using the dependency's
serializer. The initial adapter admits unaggregated row shuffles without RDD key ordering or
map-side combine, including the targeted SQL exchange path. Unsupported dependencies fail
preparation and remain eligible for ordinary execution. Map ranges and coalesced reducer ranges
are checked against the sealed shape. Each reader obtains its own lease and renews away from
fetch threads. Renewal also checks a driver RPC endpoint for the exact installed binding; ordinary
stream consumption performs local liveness checks. This provides periodic fencing visibility,
not instantaneous cross-process revocation. Initial reader setup checks the driver before claiming.

Read/open/decode failures are reported with the adoption-specific location in `FetchFailed`.
DAGScheduler now calls manager-owned recovery failure hooks before its normal retry handling.
A matching native failure clears all adopted outputs and triggers whole-stage rollback even when
the checksum retry option is enabled. Stale binding failures are ignored, and synthetic recovery
locations do not trigger physical executor/host-loss cleanup. The reference resolver path retains
its existing fetch-thread invalidation mechanism.

The wiring is implemented but uncompiled. A pinned-fork cold-process runner, real Iceberg query
execution, failure controls, task-count assertions, and combined CI validation are still required
before reporting that the end-to-end native recovery path works.
