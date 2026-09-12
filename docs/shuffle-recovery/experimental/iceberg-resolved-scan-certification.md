# Iceberg resolved-scan certification spike

## Decision status

There are two separate questions in this experiment and they must not be conflated.

**Source inspection:** Apache Iceberg's Spark 4.2 connector line contains the information needed to
continue investigating a deliberately narrow resolved batch-scan certificate. The required scan
state is still package-private and the planning-listener mechanism used by this spike is not a stable
connector-facing API.

**Executed compatibility:** a positive result exists only when the dedicated exact-head job builds
the pinned Iceberg source revision below, passes the real-catalog/scan-planning smoke gate, executes
the conformance cases, and uploads `runner_result\tPASS` together with the source commit and runtime
artifact digest. A source-level inspection result by itself is not evidence that an Iceberg runtime is
binary-compatible with this Spark candidate.

The previously selected `iceberg-spark-runtime-4.0_2.13:1.11.0` pairing is explicitly rejected for
this branch. It fails at runtime because Iceberg's `SparkView` was compiled to implement
`org.apache.spark.sql.connector.catalog.View`, while this Spark candidate has the newer concrete
`View` class shape.

## Pinned compatibility candidate

The conformance runner builds Iceberg directly from:

- repository: `apache/iceberg`;
- source commit: `e76d63584d7f83b102026749e1ae0f91813cb78e`;
- source line: Spark 4.2 / Scala 2.13;
- Gradle task: `:iceberg-spark:iceberg-spark-runtime-4.2_2.13:shadowJar`;
- build invocation: `./gradlew --no-daemon -DsparkVersions=4.2
  :iceberg-spark:iceberg-spark-runtime-4.2_2.13:shadowJar`.

That commit includes the addition of Iceberg's Spark 4.2 connector plus the immediate build-cleanup
follow-up. In that line, `SparkView` converts Iceberg view metadata into Spark's concrete
`org.apache.spark.sql.connector.catalog.View` through `View.Builder` rather than implementing the old
interface. The exact runtime jar is built in the isolated conformance working directory, is added only
to the SQL test configuration for that invocation, and is never added to Spark's normal dependency
graph.

The runner records the resolved Iceberg source commit, the exact Spark candidate commit, the Gradle
build task and a SHA-512 digest of the generated runtime jar. A newer Iceberg main revision or a
released artifact must not be substituted without changing the recorded candidate and rerunning the
same gates.

CI may reuse the source-built jar from an exact-key cache scoped to the runner OS, architecture,
Zulu JDK 17 and runner-script hash (which includes the pinned source revision and build task).
The runner still fetches and verifies the pinned revision, checks the cached source/task identity
and SHA-512, and rebuilds if either check fails. Evidence records `source-build` or `verified-cache`
as `iceberg_runtime_origin`. A cache hit skips only the external runtime build; smoke and conformance
always execute against the current Spark candidate. This is build-cache provenance, not an
independent attestation of the cached artifact.

## Compatibility smoke gate

Before the larger conformance program runs,
`ShuffleRecoveryIcebergCompatibilitySmoke` performs a cheap executable check against the exact built
jar. It:

1. verifies the candidate Spark `View` API has the expected concrete-class shape;
2. loads the real `org.apache.iceberg.spark.SparkCatalog` class;
3. initializes a Hadoop-backed Iceberg catalog;
4. creates and writes a one-row Iceberg table;
5. forces Spark batch-scan planning; and
6. reads and checks the exact row value.

A linkage error, catalog initialization failure, planning failure or wrong value produces an explicit
FAILED smoke evidence record and a non-zero runner exit. The full certification experiment is not
allowed to manufacture a PASS by shadowing Spark catalog classes or suppressing the failure.

## Why the certificate is derived from the executed scan

Spark's `BatchScanExec` owns the resolved Data Source V2 `Scan`. The concrete Iceberg
`SparkBatchQueryScan` caches the file tasks and task groups it plans, and `SparkBatch` consumes those
same task groups when constructing Spark input partitions.

The spike enters through that `BatchScanExec.scan` object and reads the scan's cached task groups. It
does not load the table again to decide which snapshot or files should be read.

There is an important mutable-latest race to avoid. Iceberg's `SnapshotScan.planFiles()` chooses the
snapshot and emits a `ScanEvent` before planning files. Reading a separately resolved current snapshot
after planning could observe a newer table state. The spike therefore captures the synchronous
planning event on the planning thread and binds its snapshot ID, projection and pushed filter to the
same operation that produced the cached task groups.

Iceberg's listener registry has no unregister operation. The spike registers one process-life
listener and uses a `ThreadLocal` capture that is removed in `finally`. This is acceptable only in the
isolated conformance process and is not a recommended production extension point.

## Current certificate contents

For the currently exercised, unpartitioned, no-delete batch-scan slice, the identity includes:

- Iceberg table UUID from the resolved scan's table object;
- the concrete snapshot ID from the same planning event;
- projected Iceberg schema including field IDs and types;
- the pushed Iceberg filter expression including literal values;
- case-sensitivity policy;
- effective split size, split lookback and open-file cost;
- mapper/task counts; and
- a SHA-256 ordered mapper-decomposition digest.

The decomposition stream covers data-file identity and physical split facts, task residuals, sequence
facts and ordering. File locations contribute only to the digest and are not printed into CI evidence.
No dense mapper-by-reducer state is constructed by this source-certification spike.

## Coverage and resource limits

The owned candidate requires exact IDs and payloads for every positive snapshot, filter, split and
schema-evolution case, including uniqueness and complete expected membership. Projecting the added
nullable field also checks its null values. Unprojected schema evolution may retain the certificate
or conservatively miss; exact ordinary values are required in either case.

Delete-bearing tasks are explicitly refused. A merge-on-read deletion of one row must exercise that
specific refusal, after which ordinary execution must return the exact remaining rows. Position
deletes, equality deletes and deletion vectors are not certified reuse modes.

The decomposition digest limits every scalar and array write to 64 MiB. Schema and expression
serialization have separate field, node, literal and string limits applied before JSON encoding;
nested schemas, non-null defaults and unreviewed filter terms/literals are refused. These limits
bound additional certificate work, not Iceberg's own scan planning allocations or total JVM memory.

Deployment-specific authorization has not been exercised. Missing-table and expired-snapshot checks
establish preservation of those ordinary errors only. The adapter remains an out-of-tree experiment
and is not yet connected to scheduler adoption. Each case above is a required assertion, not a claim
that a candidate has passed before its exact-commit CI evidence is available.

## Ordinary query semantics

Recovery certification is an optional observation after ordinary source resolution. Source planning
exceptions are not converted into cache misses followed by different source semantics:

- when a valid resolved scan cannot produce the narrow certificate, recovery is unavailable and
  ordinary execution remains authoritative;
- when source resolution, snapshot selection or authorization fails, the ordinary Spark/source error
  remains authoritative; and
- recovery never selects an older snapshot on behalf of a mutable-latest query.

The experiment does not mutate scheduler state, provider state, `MapOutputTracker`, or durable
artifacts, and it performs no recovery I/O on the DAGScheduler event loop.

## Supported-feature matrix

| Feature | Executed/claimed disposition | Reason |
| --- | --- | --- |
| Real catalog initialization on this Spark candidate | Smoke-gated | Must pass against the exact built runtime before conformance. |
| Unpartitioned batch table scan without delete files | Feasibility candidate | Exact cached task groups map to Spark input partitions. |
| Mutable latest | Conformance case | Snapshot ID comes from the same planning event and advances with latest. |
| Explicit retained snapshot ID | Conformance case | Ordinary Iceberg planning resolves the requested snapshot. |
| Projection / field IDs | Identity input | Resolved projected schema is encoded. |
| Pushed filter | Identity input | Iceberg expression encoding includes literals. |
| Per-file residual | Decomposition input | Residual is included in ordered task hashing. |
| Split planning | Conformance case | Effective split controls and task ordering affect identity/decomposition. |
| Delete-bearing file tasks | Unsupported | A real row-delete case requires explicit refusal and correct ordinary values. |
| Positive-case value parity | Required assertion | Exact IDs, payloads, uniqueness and evolved null fields are checked. |
| Complete 64 MiB certification allocation bound | **Not claimed** | Source planning/JSON materialization are outside that narrow stream guardrail. |
| Partitioned/grouped scans | Unsupported | Partition/group-key canonicalization is unreviewed. |
| Runtime-filter-dependent scans | Unsupported | Runtime filters can re-plan Iceberg tasks. |
| Incremental/changelog/streaming scans | Unsupported | Outside batch snapshot scope. |
| Aggregate/local metadata scans | Unsupported | Outside the reviewed batch-query scan path. |
| Deployment-specific authorization | Not exercised | Authorization remains an ordinary source responsibility. |

## Deterministic conformance cases

`dev/shuffle-recovery/iceberg-source-spike/run.sh <evidence.tsv>` performs the pinned source build,
smoke gate and larger conformance run. The larger run checks:

1. the initial latest scan binds its actual current snapshot;
2. the same resolved snapshot reproduces identity;
3. appending data advances latest and changes identity;
4. explicitly reading the retained old snapshot reproduces the original identity;
5. a changed pushed filter changes identity while ordinary execution remains available;
6. a changed split option changes identity and mapper decomposition;
7. schema evolution is compared at the projected-field boundary;
8. an unsupported partitioned scan produces no certificate but still executes normally;
9. an expired explicit snapshot preserves its ordinary source failure;
10. a missing source preserves ordinary source resolution failure; and
11. a real delete-bearing scan refuses certification and preserves exact ordinary values.

The runner creates its evidence file before fetching/building Iceberg. Any failed stage appends
`failure_stage`, `runner_exit_code` and `runner_result\tFAILED`, so an early linkage/build failure
still produces a downloadable diagnostic artifact. A PASS requires the smoke and conformance outputs,
the exact source/runtime metadata and `runner_result\tPASS`. Missing success evidence remains a gate
failure.

## Gap to a stable adapter

The required information is available in the investigated Iceberg connector internals, but assembling
it still requires package-private access plus a process-global planning listener. Neither should
become a Spark dependency. A production source contract should let the exact resolved scan provide a
bounded immutable certificate after decomposition-affecting planning is fixed, with an explicit
unsupported result and ordinary source/auth errors kept distinct.

This experiment therefore establishes only the behavior actually executed by the pinned candidate.
It does not turn an unverified release label, source inspection, or descriptor encoding into a
compatibility/support claim.
