# Data Source V2 Transaction and Row-Level Write Recovery

## Status

This document defines the Spark-side contract required to resume transactional and row-level
Data Source V2 writes after a driver failure. It is a design contract, not a statement that these
paths are supported today.

Spark currently fails closed when recovery is enabled for either:

* a `SupportsBatchWriteRecovery` write attached to a catalog `Transaction`; or
* a row-level `ReplaceData` or `WriteDelta` execution.

Those checks must remain until every requirement below has an implementation and conformance test.
Removing either check in isolation can duplicate a catalog transaction, lose row-level summaries,
or combine task messages produced against incompatible table state.

The foundational Spark API now models durable transaction resolution as `OPEN`, `COMMITTED`,
`ABORTED`, or `UNKNOWN`. It derives a stable transaction identity from a recovery execution ID,
catalog identity, and canonical logical-operation bytes. Transaction execution is production-wired
for those states, including accepted-but-lost commit re-resolution, but recovery inside a batch
write remains guarded. The v3 task envelope and driver aggregation path can carry an exact
`RowLevelTaskSummary`; task-local production of every counter and the complete conformance matrix
below remain required before row-level execution is enabled.

Streaming and micro-batch writes are outside this contract. They require a distinct epoch and
checkpoint protocol.

## Safety properties

A recovered execution must satisfy all of these properties:

1. A logical transaction has one stable identity across driver incarnations.
2. A transaction outcome is immutable: `COMMITTED` and `ABORTED` are terminal.
3. An unavailable or ambiguous outcome is `UNKNOWN`, never `OPEN` or absent.
4. Transaction commit is idempotent for the stable transaction identity.
5. A task commit belongs to exactly one write generation and transaction identity.
6. A task commit is reusable only when the complete row-level compatibility manifest matches.
7. Task metrics and the commit message are one checksummed atomic record.
8. A recovered write summary is computed solely from authoritative durable task records.
9. Recovery never calls ordinary task or batch abort on authoritative committed task output.
10. A stale driver or executor cannot publish, commit, or abort after lease takeover.
11. A losing speculative attempt cleans only its own committed output.
12. Unsupported, corrupt, expired, or incompatible state fails before mutation.

## Transaction state machine

The transaction owner is the connector catalog. Spark supplies a stable transaction identity and
the current recovery fencing context. The connector returns one of four states:

```text
                 begin or recover
                       |
                       v
                    +------+
           +------->| OPEN |-------+
           |        +------+       |
           | commit    | abort     |
           v           v           v
      +-----------+ +---------+ +---------+
      | COMMITTED | | ABORTED | | UNKNOWN |
      +-----------+ +---------+ +---------+
```

`COMMITTED` and `ABORTED` are immutable. `UNKNOWN` means Spark cannot prove the outcome and must
stop. It must not create a replacement transaction or execute writer tasks.

The transaction identity must be derived from the stable recovery execution identity, catalog
identity, and canonical logical operation. It must not contain a `QueryExecution` ID, stage ID,
task attempt ID, process-local UUID, or driver incarnation.

Beginning or recovering a transaction must be an immutable resolve operation:

* no record: create `OPEN` under the current fence;
* existing `OPEN`: attach the replacement driver under a newer valid fence;
* existing `COMMITTED`: report the durable result and execute no writers or commit;
* existing `ABORTED`: fail with the durable abort reason;
* unavailable or inconsistent state: report `UNKNOWN` and fail closed.

The connector must make its transaction state durable independently of the driver. Merely passing
the same ID to a newly allocated transaction object is insufficient.

## Ordering of batch and transaction commit

For a write inside a catalog transaction, task publication, batch commit, and transaction commit
are distinct durable boundaries:

1. Resolve the stable transaction and write identities.
2. Resolve immutable transaction and write manifests.
3. Load authoritative task records.
4. Execute only missing partitions.
5. Publish each task envelope through fenced first-writer-wins CAS.
6. Invoke idempotent batch commit, which durably stages the write under the transaction identity.
7. Invoke idempotent transaction commit, which atomically makes all staged changes visible.
8. Persist or discover the terminal transaction result.

A replacement driver follows the same sequence. If batch commit previously succeeded, its
idempotency key makes step 6 a no-op. If transaction commit previously succeeded but its response
was lost, recovery observes `COMMITTED` and performs no write or finalize operation.

Spark must never infer a transaction outcome from an RPC exception. It must query the durable
transaction state. If the query cannot distinguish committed from uncommitted, the result is
`UNKNOWN` and execution stops.

## Immutable write generation

Every row-level write manifest must include, at minimum:

* stable recovery execution, transaction, sink, and write identities;
* command kind: `MERGE`, `UPDATE`, or `DELETE`;
* physical writer mode: `REPLACE_DATA` or `WRITE_DELTA`;
* catalog and table identity;
* table version or base snapshot and validation snapshot;
* row schema, row-ID schema, and metadata schema;
* partitioning, ordering, and physical partition count;
* canonical condition, assignments, and conflict filter;
* isolation level and validation settings;
* commit-message codec ID and version;
* metric schema ID and version;
* connector compatibility metadata;
* source anchor identities used by the operation.

Connector-specific manifests may add stronger bindings. For group replacement, that includes the
sorted set of replaced data files and delete vectors. For delta writes, it includes referenced data
file semantics and the stable encoding of data and delete files.

Spark must compare the complete manifest byte-for-byte through immutable store resolution before
it loads or executes a task. A mismatch is an incompatible write generation, not missing work.

## Durable task result

The recovery envelope evolves through additive formats rather than changing an existing version in
place. Version 1 contains the task payload and output-row count, version 2 adds connector custom
metrics, and version 3 adds the Spark-owned row-level summary. A row-level task record contains one
atomic payload with:

* partition ID;
* connector commit-message bytes;
* output row count;
* ordered custom task metric values;
* row-level counter values needed for the command summary;
* metric schema ID and version;
* checksum over identity, metadata, and payload.

Metric names alone are not a compatibility schema. The manifest binds an ordered metric descriptor
containing name, semantic type, additive aggregation identity, format version, and range. Versions
2 and 3 accept only non-negative additive counters: arbitrary averages, maxima, ratios, and connector
aggregation code cannot be reconstructed exactly without recreating the original executor task
events. Unknown, duplicated, missing, negative, out-of-range, or overflowed values fail closed.

Recovered metrics are installed in driver SQL metrics exactly once. Metrics from attempts that lose
the task CAS are not added. A retry that discovers a canonical task in executor preflight returns
the canonical message and metrics without creating a writer or consuming upstream input. Writers
snapshot these values after all records are written and immediately before `commit`; commit must not
change them. If global commit already completed, `BatchWriteRecoveryState` supplies the durable
totals so Spark need not make task-store availability part of global-commit recovery.

## Row-level summaries

Current row-level summaries partially read transient plan metrics, including `MergeRowsExec` and
`BatchScanExec` metrics. Those values do not survive driver replacement and cannot be the authority
for a recovered commit.

For recovery-enabled row-level writes, the durable task record must contain all counters used by:

* `MergeSummary`;
* `UpdateSummary`;
* `DeleteSummary`.

The driver builds the summary by validating and aggregating canonical records for every physical
partition. It must not mix durable values with current-driver accumulator values. Scan-derived
values such as group-replacement deleted-row counts must be recorded at the physical stage where
they are produced and survive any recovered exchange. A transient driver accumulator or a value
sampled only in the later writer task is not authoritative. Connector metadata may supply an
independently verifiable durable total, but opaque compatibility bytes alone are not proof.

## Abort behavior

Abort depends on durable state:

| Durable state | Spark behavior |
|---|---|
| No writer commit | ordinary writer abort is allowed |
| Canonical task commit exists | preserve canonical output |
| Local attempt lost task CAS | discard only local committed output |
| Batch staged, transaction open | recovery-aware batch abort may preserve reusable task output |
| Transaction committed | no abort; return committed result |
| Transaction aborted | no writer execution; report durable abort |
| Transaction unknown | no abort or retry mutation; fail closed |

The transaction abort API must be idempotent. It must not convert a committed transaction to an
aborted transaction. Failure while aborting is reported without deleting authoritative task state.

## Streaming boundary

The batch recovery resolver must not silently enable streaming recovery. A streaming protocol needs
stable query and epoch identities, coordination with the checkpoint commit log, sink epoch commit
status, source offsets, state-store versions, and watermark/state metadata. Until that protocol is
specified and implemented, recovery-enabled streaming write construction must fail explicitly or
continue to use only the existing streaming checkpoint semantics without batch task adoption.

## Connector conformance matrix

An implementation is not supported until automated tests prove each applicable row:

| Scenario | Required result |
|---|---|
| Driver dies before any task commit | replacement executes all tasks |
| Driver dies after some task commits | replacement executes only missing tasks |
| Task CAS accepted and response lost | retry preflight executes zero writer/input work |
| Speculative attempt loses CAS | local loser output is discarded; canonical output remains |
| Driver dies after all task commits | replacement executes zero writer tasks |
| Driver dies after batch stage commit | replacement does not create a second staged write |
| Transaction commit succeeds and response is lost | replacement observes `COMMITTED` |
| Transaction state is unavailable | fail closed before writer construction |
| Old driver/executor acts after takeover | mutation is rejected by fencing |
| Manifest or metric schema changes | fail closed before mutation |
| Recovered execution aborts | canonical task output is preserved as specified |
| MERGE, UPDATE, DELETE | summary equals uninterrupted control execution |
| ReplaceData and WriteDelta | exactly one visible global state transition |
| Streaming write sees batch recovery provider | no implicit batch-style adoption |

Tests must inspect writer creation, upstream row consumption, task submission, canonical task
records, connector-visible files, transaction state, and final table contents. Elapsed time and
successful query completion are not sufficient evidence.

## Implementation sequence

1. Freeze the transaction recovery and durable metric APIs.
2. Add plain-JVM state-machine, serialization, corruption, and compatibility tests.
3. Add transaction conformance tests with a durable in-memory test catalog that survives simulated
   driver instances.
4. Extend recovery execution to group replacement using the versioned task-result format.
5. Extend delta writer factories and tasks without adapting a delta write to an ordinary writer.
6. Add MERGE, UPDATE, and DELETE positive and fail-closed suites for both physical modes.
7. Add accepted-response-lost, speculation, cancellation, transaction-finalize, and fencing fault
   injection.
8. Enable support only after the full connector conformance suite passes.

## Non-goals

This design does not make arbitrary connector side effects recoverable. A connector that cannot
provide durable immutable task commits, idempotent batch staging, durable transaction status, or
loser cleanup must remain unsupported. Spark must not emulate these guarantees with driver-local
state or object-store existence checks.

## Granular implementation ledger

The estimates below are conservative engineering estimates for one agent with Spark builds limited
to two CPUs. They include implementation, focused review, and focused tests, but not connector
implementation, full-project CI, or Apache review time.

Dependency labels are:

* **X1**: depends only on the frozen version-1 batch recovery APIs and envelopes.
* **Spark**: independently implementable in Spark after X1 is frozen.
* **Connector contract**: Spark can define and test the API with an in-memory connector, but cannot
  enable production recovery until a real connector implements it.
* **Integration**: requires Iceberg and the durable task store in addition to Spark.

### A. Transaction recovery contract

| Item | Production files | Test files | Dependency | Estimate | Gate |
|---|---|---|---|---:|---|
| Define stable transaction recovery metadata and state enum | `sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/transactions/TransactionInfo.java`; new transaction recovery interfaces beside it | `sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/transactions/TransactionUtilsSuite.scala`; new serialization/state suite | X1, Spark | 2-3 days | Independently implementable |
| Derive a stable transaction ID from recovery execution, catalog, and canonical logical write | `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/transactions/TransactionUtils.scala`; `sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala` | `TransactionUtilsSuite.scala`; `sql/core/src/test/scala/org/apache/spark/sql/execution/QueryExecutionSuite.scala` | X1, Spark | 2-4 days | Independently implementable |
| Resolve begin/recover through `OPEN`, `COMMITTED`, `ABORTED`, and `UNKNOWN` | `TransactionalCatalogPlugin.java`; `Transaction.java`; `TransactionUtils.scala`; `QueryExecution.scala` | new `TransactionRecoverySuite.scala`; extend the in-memory transaction catalog | Connector contract | 4-7 days | API/test implementation independent; production enablement blocked |
| Bind transaction ID and connector generation into the immutable write manifest | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/RecoveryTaskCommit.scala`; `WriteToDataSourceV2Exec.scala` | `RecoveryTaskCommitSuite.scala`; `RecoveryTaskCommitCompatibilitySuite.scala` | X1, Spark | 2-3 days | Independently implementable after X1 fixture freeze; requires envelope version 2 |
| Make batch staging idempotent under a transaction | connector-facing additions beside `SupportsBatchWriteRecovery.java`; execution in `WriteToDataSourceV2Exec.scala` | `BatchWriteRecoverySuite.scala`; new transaction recovery conformance suite | Connector contract | 3-5 days Spark, plus connector work | Production enablement blocked |
| Discover commit outcome after response loss | `TransactionUtils.scala`; `QueryExecution.scala`; `WriteToDataSourceV2Exec.scala` | fault-injection cases in `TransactionRecoverySuite.scala` | Connector contract | 3-5 days Spark, plus connector work | Production enablement blocked |
| Implement recovery-aware, idempotent transaction abort | `Transaction.java`; `TransactionUtils.scala`; `QueryExecution.scala` | `TransactionUtilsSuite.scala`; `TransactionRecoverySuite.scala` | Connector contract | 2-4 days Spark, plus connector work | Production enablement blocked |
| Keep current transaction guard until all conformance gates pass | `WriteToDataSourceV2Exec.scala` | negative test in `BatchWriteRecoverySuite.scala` | X1, Spark | 0.5-1 day | Independently implementable |

Transaction API changes must be proposed and frozen with X1 before editing these production files.
The present random UUID behavior remains correct for non-recoverable transactions and must not be
changed globally. Stable IDs apply only when an active recovery provider resolves a logical write.

### B. Durable row-level task results and metrics

| Item | Production files | Test files | Dependency | Estimate | Gate |
|---|---|---|---|---:|---|
| Define a versioned metric descriptor and row-level counter schema | new Java interfaces beside `RecoveryTaskCommitStore.java`; implementations in `RecoveryTaskCommit.scala` | `RecoveryTaskCommitSuite.scala`; `RecoveryTaskCommitCompatibilitySuite.scala` | X1, Spark | 3-5 days | Independently implementable after API freeze |
| Add envelope version 2 containing commit payload, row count, custom metrics, and row-level counters | `RecoveryTaskCommit.scala` | recovery task unit/compatibility suites; add immutable version-2 fixture under `sql/core/src/test/resources/recovery/` | X1, Spark | 4-6 days | Independently implementable; version 1 must remain readable |
| Validate metric schema identity, duplicates, ranges, overflow, truncation, and checksum | `RecoveryTaskCommit.scala` | `RecoveryTaskCommitSuite.scala` | Spark | 2-4 days | Independently implementable |
| Restore canonical task metrics exactly once on the driver | `WriteToDataSourceV2Exec.scala` | `BatchWriteRecoverySuite.scala`; new `RowLevelWriteRecoverySuite.scala` | Spark | 3-5 days | Independently implementable after version 2 |
| Return canonical message and metrics from executor preflight without input consumption | `WriteToDataSourceV2Exec.scala` | `BatchWriteRecoverySuite.scala`; `RowLevelWriteRecoverySuite.scala` | Spark | 2-3 days | Independently implementable after version 2 |
| Ensure losing attempts contribute no metrics and discard only local output | `WriteToDataSourceV2Exec.scala` | `RowLevelWriteRecoverySuite.scala` | Spark plus connector discard support | 2-4 days | Framework independent; real file cleanup requires connector |
| Aggregate summaries entirely from canonical durable task records | `WriteToDataSourceV2Exec.scala` | `RowLevelWriteRecoverySuite.scala`; existing MERGE/UPDATE/DELETE suites | Spark | 4-7 days | Independently implementable for counters Spark can observe |
| Replace scan-derived DELETE counts with a durable proof | `BatchScanExec.scala`; `WriteToDataSourceV2Exec.scala`, or connector metadata API | `GroupBasedDeleteFromTableSuite.scala`; `RowLevelWriteRecoverySuite.scala` | Connector contract | 3-6 days Spark, plus connector work | Production enablement blocked for group replacement |

The existing version-1 compatibility fixture must not be regenerated to contain new fields.
Version 2 needs a separate golden fixture so replacement drivers can prove both backward decoding
and deterministic re-encoding.

### C. Group-based row-level recovery (`ReplaceData`)

| Item | Production files | Test files | Dependency | Estimate | Gate |
|---|---|---|---|---:|---|
| Bind command, condition, assignments, base state, schemas, partitioning, ordering, and source anchors | `V2Writes.scala`; row-level manifest helpers in a new dedicated file under `datasources/v2` | new `RowLevelWriteManifestSuite.scala` | X1, Spark | 4-7 days | Independently implementable after logical identity freeze |
| Preserve distribution and ordering wrapper behavior | `V2Writes.scala`; dedicated wrapper/helper file | manifest suite; existing group-based suites | Spark | 2-3 days | Independently implementable |
| Allow `ReplaceDataExec` to use version-2 recovery only after manifest/metric validation | `WriteToDataSourceV2Exec.scala` | new `RowLevelWriteRecoverySuite.scala` | Spark | 3-5 days | Independently implementable with in-memory connector |
| Recover MERGE counters and summary | `WriteToDataSourceV2Exec.scala`; row-level task helpers | `GroupBasedMergeIntoTableSuite.scala`; no-metadata variant; recovery suite | Spark | 3-5 days | Independent except connector file identity binding |
| Recover UPDATE counters and summary | same execution/helper files | `GroupBasedUpdateTableSuite.scala`; no-metadata variant; recovery suite | Spark | 2-4 days | Independent except connector file identity binding |
| Recover DELETE counters and summary | same execution/helper files | `GroupBasedDeleteFromTableSuite.scala`; no-metadata variant; recovery suite | Connector contract | 3-6 days | Blocked on durable scan/replaced-file proof |
| Bind the sorted replaced-file/delete-vector set and clean speculative losers | connector compatibility metadata consumed by Spark; no Iceberg source edits in Spark | Spark conformance test with an in-memory connector | Integration | 2-4 days Spark validation, plus Iceberg work | Production enablement blocked |
| Remove the `RowLevelWriteExec` fail-closed guard for supported group modes only | `WriteToDataSourceV2Exec.scala` | all group recovery suites and negative unsupported-mode cases | Integration | 1-2 days | Final integration gate |

### D. Delta-based row-level recovery (`WriteDelta`)

| Item | Production files | Test files | Dependency | Estimate | Gate |
|---|---|---|---|---:|---|
| Define recovery-capable delta write/factory/writer contracts without coercing them to ordinary writers | new interfaces beside `DeltaWrite.java`, `DeltaBatchWrite.java`, and `DeltaWriterFactory.java` | new Catalyst API conformance suite | X1, Spark | 4-7 days | Independently implementable after API review |
| Wrap recovery-required `DeltaWrite` while preserving distribution and ordering | `V2Writes.scala`; new dedicated delta recovery wrapper | `RowLevelWriteManifestSuite.scala` | Spark | 3-5 days | Independently implementable |
| Extend delta writing tasks with executor preflight, task CAS, and loser cleanup | `WriteToDataSourceV2Exec.scala`; preferably extracted recovery task helper | `RowLevelWriteRecoverySuite.scala` | Spark | 5-8 days | Independently implementable with test connector |
| Encode operation-specific data-file, delete-file, and referenced-file results | connector codec API consumed by Spark | recovery suite with synthetic messages | Connector contract | 2-4 days Spark validation, plus Iceberg work | Production enablement blocked |
| Recover MERGE/UPDATE/DELETE summaries for delta mode | execution/helper files | delta-based MERGE, UPDATE, DELETE and no-metadata suites | Spark | 4-7 days | Independent after durable metrics |
| Validate position-delta identity and referenced-data-file compatibility | manifest helper and connector metadata validation | `DeltaBased*` suites plus negative drift cases | Integration | 2-4 days Spark validation, plus Iceberg work | Production enablement blocked |
| Remove the current `WriteDelta` recovery rejection for explicitly supported connectors | `V2Writes.scala`; `WriteToDataSourceV2Exec.scala` | full delta recovery and unsupported connector suites | Integration | 1-2 days | Final integration gate |

### E. Spark-only conformance infrastructure

These tasks can proceed without Celeborn or Iceberg once X1 APIs are frozen. The test connector
must model durable state in a process-independent object owned by the suite, and separate connector
instances must represent replacement drivers.

| Item | Exact test files | Estimate |
|---|---|---:|
| Add durable in-memory transaction states, stable-ID resolution, and injected unknown outcomes | extend `sql/catalyst/src/test/scala/org/apache/spark/sql/connector/catalog/InMemoryRowLevelOperationTableCatalog.scala` or add an isolated recovery catalog beside it | 3-5 days |
| Add reusable counters for writer creation, input consumption, task publication, batch staging, finalize, abort, and cleanup | new helper under `sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/v2/` | 2-3 days |
| Add row-level manifest determinism and drift suite | new `RowLevelWriteManifestSuite.scala` | 2-4 days |
| Add ReplaceData task adoption and summary suite | new `RowLevelWriteRecoverySuite.scala` | 4-7 days |
| Add WriteDelta task adoption and summary cases | same suite or a dedicated `DeltaWriteRecoverySuite.scala` | 4-7 days |
| Add transaction begin/recover/finalize state-machine suite | new `sql/core/src/test/scala/org/apache/spark/sql/connector/TransactionRecoverySuite.scala` | 4-7 days |
| Add response-loss, speculation, cancellation, stale generation, corruption, and abort matrix | recovery suites above | 5-8 days |
| Add explicit streaming boundary tests | `StreamingTransactionSuite.scala` and a small recovery-provider suite | 2-3 days |

### F. Dependency gates and completion boundary

Spark-independent implementation can reach the following boundary:

1. reviewed and frozen public transaction/delta/metric recovery APIs;
2. stable identity derivation and immutable manifest construction;
3. version-2 envelope with backward-compatible version-1 decoding;
4. ReplaceData and WriteDelta recovery execution against durable in-memory test connectors;
5. deterministic summary restoration and recovery-aware abort semantics;
6. complete positive and fail-closed Spark conformance suites;
7. guards that enable recovery only when the connector advertises every required capability.

The following claims remain blocked until connector integration exists:

* a real catalog transaction can be recovered or its outcome discovered after driver death;
* transaction batch staging and finalization are globally idempotent;
* group-replacement file sets and delete vectors are durably bound;
* position-delta data/delete files and referenced-file semantics are recoverable;
* speculative loser files are safely deleted in an object store;
* MERGE, UPDATE, and DELETE produce one Iceberg state transition after a real process crash;
* stale driver/executor mutations are rejected by the production fenced store.

Spark must retain the current fail-closed checks for any connector that lacks those capabilities.
Passing an in-memory Spark conformance suite proves the framework, not production connector support.

### G. Consolidated estimate

| Scope | Conservative time |
|---|---:|
| Spark API and stable transaction identity | 1.5-2.5 weeks |
| Version-2 task metrics and summaries | 2-3 weeks |
| ReplaceData framework and Spark conformance | 2-3 weeks |
| WriteDelta framework and Spark conformance | 2.5-4 weeks |
| Fault injection, streaming boundary, compatibility, and cleanup | 1.5-2.5 weeks |
| **Spark-only X2 total with overlapping work** | **6-10 weeks** |
| Connector integration and real crash validation | **additional 3-6 weeks**, shared with connector owners |

These estimates assume X1 is frozen before public API work begins. Changing the version-1 envelope,
task-store semantics, or recovery identity after X2 starts adds a compatibility migration and should
be budgeted separately.
