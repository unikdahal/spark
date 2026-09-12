# Owned completed-shuffle PoC

The active integration candidate is `spip/poc-owned-end-to-end`. Work is driven by executable
behavior and evidence, not by the old issue boundaries. The frozen Phase 0 evidence remains
unchanged. Open-PR source compatibility and evidence-validation work is preserved in merge history.

## Generic extension boundaries

Iceberg and Celeborn are the primary integration targets, in different roles: the source connector
certifies the resolved read, and the shuffle provider retains and serves completed output. Core
must not select behavior by either product name. Other connectors and providers should implement
the same eventual contracts. NFS and the built-in Range source are validation fixtures.

The current implementation connects the full canonical computation identity to the existing
manifest store and adoption-validation boundary. A versioned identity envelope retains the legacy
Phase 0 encoding and admits the bounded canonical encoding, including opaque source tokens and
provider format IDs. Discovery compares the full payload after a digest lookup. Preparation checks
that the supplied identity agrees with the current resolved inputs and dependency shape.
The SQL builder takes the selected provider's read-format ID explicitly; it does not instantiate or
choose a provider. A format change changes the identity even when the source and SQL plan agree.

The shared-filesystem fixture now reconstructs a Range/Project/Shuffle computation in each child
JVM and uses that canonical identity for publication and recovery. A changed producer filter must
miss even when the source token and expected rows are unchanged. Core tests also exercise a
non-reference provider format identifier; that proves generic identity handling, not an implemented
alternative provider read path. The retained map-descriptor format and runtime provider integration
still need to become a complete provider contract. No generic public SPI is declared finished here.

## What the current candidate is establishing

1. Exact-candidate Core and SQL correctness, including independently launched cold/healing JVMs.
2. An out-of-tree Iceberg runtime built at a pinned source revision, with catalog compatibility,
   exact row-value checks, resolved scan certificates and conservative unsupported results.
3. A shared-filesystem experiment using loopback NFS and separate Spark executor processes. The
   selected map winners are fetched from those executors through Spark's block transport, copied
   into the retained provider namespace, and read by a fresh replacement application.
4. Native Celeborn publication, cold-process reads and whole-shuffle fallback from the
   certified Iceberg producer, using the existing standalone LifecycleManager.

The shared-filesystem experiment is deliberately small. NFS `sync` exports and atomic filesystem
operations supply the storage substrate; the existing experimental provider supplies the format.
Only the shared namespace survives between producer and replacement; Spark scratch directories are
removed. The test uses a single ephemeral CI VM and does not establish cross-host performance,
Kerberos, server failover, authenticated recovery leases, or production retention behavior.

Publication copies and the driver-served adopted-read path remain explicit limitations of this
experiment. They must be replaced or fully costed before claiming a scalable implementation. The
experiment does not turn the reference provider into a completed production SPI.

## Source semantics

The unchanged resolved scan must reproduce its certificate. A changed scan, snapshot, projection,
filter or decomposition may refuse reuse. Schema evolution is checked against exact expected row
values; changing unprojected metadata may conservatively change the scan certificate. No unrelated
schema-evolution cache-hit guarantee is made.

The source spike rejects delete-bearing tasks, nested schemas, non-null field defaults and
unreviewed filter terms/literals. Its bounded digest counts scalar and array bytes together; source
planning memory is separate from additional certificate work. The real source and
reference-provider durable shuffle experiment now form one integrated replay query.
Native Celeborn uses the same certified source boundary
and has separate native end-to-end evidence below.

The delete refusal is exercised with an actual merge-on-read row deletion. Certification must
report a delete-bearing task; ordinary execution must return every remaining expected row with its
correct payload. A metadata-only whole-file removal does not satisfy that check.

## Earlier reference-provider and source evidence

Candidate `89f3c0673bf764f7a430c9886e45d39bcc978129` passed the complete
[Actions gate](https://github.com/unikdahal/spark/actions/runs/34274067280): routing, lint/license,
Core, SQL, pinned Iceberg conformance and the shared-filesystem mechanism experiment.

The downloaded SQL artifact validates 39 cold child records, including nine successful replacements
and ten negative controls, plus three healing child records against that exact candidate. Its NFS
experiment mounted NFS 4.2, recovered 32 rows with the baseline digest, ran zero selected map tasks,
and read 1,152 provider bytes across 30 nonempty blocks under a new shuffle ID. This is a small
mechanism result, not a performance measurement or proof of production durability.

The Iceberg artifact records a conservative certificate miss after unprojected schema evolution,
preserved exact values, repeated/pinned snapshot identity, latest-snapshot advancement, changed split
decomposition and preserved ordinary source errors. The earlier candidate `a109e7c7488` had failed
only the unconditional schema-evolution identity assertion; the corrected requirement now passes.

Candidate `84f72b10c972490e4af78542197b1c579c9b5cca` also passed the complete gate in
[run 34276458479](https://github.com/unikdahal/spark/actions/runs/34276458479), including row-delete
refusal, shared-filesystem negative controls, child provenance and scratch isolation. The subsequent
canonical-manifest integration passed in
[run 34316394901](https://github.com/unikdahal/spark/actions/runs/34316394901) for candidate
`4aa9889f9350bad49f9b873d1ec9017a281c5cfa`. Its canonical NFS replacement ran zero map tasks and
read 1,152 provider bytes. Source-token, missing-artifact and producer-filter controls each ran four
map tasks, read no provider bytes and returned the same 32 rows and digest as the baseline. Core
also passed the canonical manifest and alternate provider-format metadata tests. The selected provider-format input and planned Iceberg scan binding subsequently passed
the complete gate in [run 34354255853](https://github.com/unikdahal/spark/actions/runs/34354255853)
for candidate `80e3a36078c47490c8d8bb0e48e555f352295948`. The Iceberg evidence includes passing
`canonical_shuffle_replanning` and `canonical_shuffle_snapshot_binding` checks with exact query
results. This proves canonical identity construction for actual Iceberg shuffle producers;
it does not yet prove cross-driver adoption of their retained output. No production authorization,
AQE recovery, performance or SPIP acceptance claim follows from the completed checks. Ordinary
latest-source resolution and ordinary errors remain authoritative.

## Validation workflow

Pushes to the owned branch run the dedicated workflow on the exact commit. The unrelated generic
Spark build matrix is skipped for this branch only. All builds and Spark tests run in GitHub Actions;
local checks are limited to source inspection, shell syntax and patch hygiene.

The final candidate gate requires the source conformance job as well as Core/SQL and quality lanes.
Core/SQL artifacts include exact suite reports and cold/healing child evidence. The shared-filesystem
experiment records the mount, process logs, result digests, skipped maps and provider reads.
It also requires distinct child process identities and matching commit provenance, and checks that
a changed source token or missing provider index triggers recomputation with identical results.

The native implementation now connects certified SQL publication, descriptor discovery,
lease-backed adoption, native executor readers and whole-shuffle invalidation.
Its small scheduling-statistics matrix is bounded rather than scalable. Completion
of these code paths is separate from the native execution evidence below.

Candidate `7c52afc16cd` passed every job in
[run 34369849014](https://github.com/unikdahal/spark/actions/runs/34369849014), including the
generic cold-process source adapter path and certified dependency tests. Subsequent
Iceberg and native provider results are recorded below.

## Verified baseline and native validation

Candidate `c266372dda25c8ad3ad8359f11b3455e4a4eb94e` passed every experimental
job in [run 34525774352](https://github.com/unikdahal/spark/actions/runs/34525774352).
The downloaded Core reports include four native-manifest tests and three
native-adoption tests with no failures or skips, together with the existing
scheduler, tracker, reader, publication and identity suites.

Its actual Iceberg/reference-provider cold replacement returned the baseline's
32 rows, launched zero target map tasks and read 1,152 retained bytes. Source-token,
producer-filter, missing-artifact and real snapshot-rewrite controls each ran one
fresh map task, read no retained bytes and returned the same result digest.
That evidence proves source-certified recovery with the reference format; it does
not substitute for native Celeborn execution.

The native harness checks separate driver processes, exact results, zero target
map tasks plus positive remote bytes for adopted reads, simultaneous independent
claims, source/manifest misses, real lease expiry, persisted worker-file loss and
rejection after an owner restart. Incremental compilation caches regenerate build
metadata, and each native driver requires the runtime Spark revision to equal the
CI candidate. See the [SPIP draft](spip-draft.md) for proposed scope and release
gates; no production authorization, AQE, scalability or acceptance claim follows
from these prototype tests.

## Completed native proof

Candidate `93c048e1d379d274e074cd3a9e77a636c01fe29c` passed all jobs in
[native run 34677866614](https://github.com/unikdahal/spark/actions/runs/34677866614).
The same implementation candidate passed Core, SQL/AQE, lint/license and pinned
Iceberg source conformance in
[general run 34677977967](https://github.com/unikdahal/spark/actions/runs/34677977967).
The final documentation commit changes only these evidence notes and the proposal;
all executable code remains identical to this validated candidate.

The retained `native-proof-evidence` artifact contains the pass marker,
per-process measurements, native service logs, file-loss inventory, compile log,
jar checksums and exact candidate provenance. Celeborn is pinned to
`edb413ee3d5e77fbecf43afa7b1a33d6054ab569` in the user's fork.

| Scenario | Target map tasks | Fetch failures | Outcome |
| --- | ---: | ---: | --- |
| Cold replacement | 0 | 0 | Native reuse; 730 remote bytes |
| Concurrent reader A | 0 | 0 | Independent native claim; 730 remote bytes |
| Concurrent reader B | 0 | 0 | Independent native claim; 730 remote bytes |
| Persisted file loss after adoption | 1 | 2 | Whole-shuffle fallback |
| Lease expiry after adoption | 1 | 3 | Whole-shuffle fallback |
| Changed source token | 1 | 0 | Preparation miss |
| Changed producer filter | 1 | 0 | Preparation miss |
| Missing manifest | 1 | 0 | Preparation miss |
| Real Iceberg snapshot rewrite | 1 | 0 | Preparation miss |
| Owner restart | 1 | 0 | Old incarnation rejected |

All 13 driver roles, including the baseline and two producers, returned the same
32-row result and digest
`25a09c0e32dacd10d7ff9c20a605112c38ea0ecaecc0c3bca23f94cce13703b5`.
The file-loss control removed eight persisted files totaling 826 bytes while the
worker and owner remained alive. Remote bytes in fallback runs include ordinary
fresh shuffle reads and must not be counted as retained-data reuse.

Successful reuse is measured before SQL cleanup releases the binding: the
replacement had an installed native binding, executed no target maps, observed no
fetch failures and read native remote bytes. `bindingAfterRead=false` records
normal end-of-query cleanup rather than failed adoption. Independent concurrent
claims and the later fault controls demonstrate that one reader's cleanup did
not delete the shared producer artifact.

This is a correctness feasibility proof on a small isolated fixture with one
actual source mapper and four reducers. Drivers are separate JVMs on one host;
each uses local Spark executors, while Celeborn services are separate processes.
It does not establish multi-host executor or network-partition behavior. It is not a performance benchmark or a
production-readiness claim. Authenticated reuse, supported AQE shapes, metadata
scale and expiry during an already-open stream remain explicit follow-on gates.
