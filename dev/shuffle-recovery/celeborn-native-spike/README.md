# Native Celeborn cold-process harness

`cold-process.sh <new-evidence-directory>` runs separate baseline, producer and
replacement driver JVMs against a running Celeborn standalone lifecycle owner.
The producer exits before the replacement starts. Driver scratch is cleared
between processes; the Iceberg warehouse, manifests and external worker storage
remain available.

Required environment:

- `ICEBERG_RUNTIME_JAR`: Iceberg Spark 4.2 Scala 2.13 runtime built from
  `e76d63584d7f83b102026749e1ae0f91813cb78e`.
- `CELEBORN_RUNTIME_JAR`: Celeborn Spark 4 shaded Scala 2.13 client built from
  fork commit `edb413ee3`. Build the servers with the same Scala profile.
- `CELEBORN_RETAINED_ENDPOINT_FILE`: endpoint file created by that fork's
  standalone LifecycleManager with the retained-shuffle extension enabled.

Run from the Spark repository root. Use paths without whitespace because sbt's
child command parser splits the arguments. The script compiles the application
and integration sources once with sbt, then exports its runtime classpath and
Spark's test JVM options. The normal Core/SQL suites run in their separate CI lane.
Each role then runs in a fresh Java process. Use CI for this expensive validation.
The validated candidate and its evidence are recorded below.

The baseline uses Spark's ordinary shuffle manager. The producer publishes its
native descriptor and the replacement must return the exact fixture with zero
target map task launches, an adopted binding before reading, positive remote bytes
and no fetch failures. Post-query binding state is recorded separately because SQL
execution cleanup can release a successfully consumed binding before collect returns.
Source token, producer filter, missing manifest and actual Iceberg snapshot
changes must all reject recovery and launch fresh map tasks. Every role's
result digest must match the baseline. Evidence includes process identities,
jar checksums, commit, working-tree diff, counts and raw logs. The work directory
is preserved for diagnosis; the operator owns service shutdown and cleanup.

`services.py <new-directory>` owns the master, worker and standalone owner,
waits for startup/registration, invokes the drivers, and shuts down all services
in a finally block. Set `CELEBORN_HOME` to the pinned distribution. The native CI
workflow builds both dependencies and runs this entry point.

The artifact-loss control pauses after adoption, removes the exact producer
namespace's persisted worker files, then releases the reader. It requires an
observed fetch failure, invalidation, fresh target map tasks and the baseline
result. The worker and owner stay alive so ordinary recomputation remains usable.
This requires `CELEBORN_PROOF_WORKER_ROOT`, supplied by the service runner.

Two concurrent replacement JVMs also hold independent adopted claims at a shared
barrier before reading. Each must report zero map tasks and positive remote bytes.
The lease-expiry control uses a three-second replacement lease. After adoption,
the supervisor pauses only its own lifecycle JVM for five seconds, then resumes
that same incarnation. Renewal replies cannot revive an expired local lease;
the read must fail through the binding check and trigger correct recomputation.
The supervisor resumes a paused owner even when the child fails.

The pass marker covers cold-process reuse, concurrent claims, identity controls,
lease expiry before reading, artifact loss and owner-restart rejection. The restart
control publishes into a separate namespace, restarts the owner on the same port
with a fresh application/incarnation, and requires ordinary recomputation.
Expiry during an already-open executor stream still requires additional evidence.
Provider unit tests exercise independent claim release, exact expiry, clock
wraparound and owner shutdown.

## AQE coverage

`SPARK_RECOVERY_AQE_MODE` selects `off` (the original control), `full` (AQE with
uncoalesced reducers), or `coalesced` (AQE must actually combine reducer ranges).
CI runs all three modes with the same positive, identity-miss and failure controls.
Each mode owns separate services and storage. Initial/final plans and the actual
adaptive read counts are saved beside each driver's evidence.

AQE preparation attaches to the initial exchange as a driver-only tag and runs on
the final exchange's shuffle preparation thread before map submission. The source
certificate remains tied to the actual planned scan; the final producer is encoded
again after stage rules. The harness lets AQE materialize its query stage and select
readers instead of submitting the initial exchange's potentially obsolete dependency.

## Verified AQE result

Candidate [`ab0052497c019d047fee2bea4b2dba1674ae5db4`](https://github.com/unikdahal/spark/commit/ab0052497c019d047fee2bea4b2dba1674ae5db4)
passed all three modes in the
[native proof run](https://github.com/unikdahal/spark/actions/runs/34881546887).
The [experimental CI run](https://github.com/unikdahal/spark/actions/runs/34881536279)
also passed the focused Core, SQL/AQE, Iceberg conformance and lint/license checks.

The downloaded `native-proof-evidence-{off,full,coalesced}` artifacts contain these
replacement-driver measurements:

| Mode | Rows | Target map tasks | Remote bytes read | Fetch failures | Merged reducer ranges |
| --- | ---: | ---: | ---: | ---: | ---: |
| AQE off | 32 | 0 | 730 | 0 | 0 |
| AQE full reducers | 32 | 0 | 730 | 0 | 0 |
| AQE coalesced | 32 | 0 | 730 | 0 | 1 |

All three replacement drivers reported adoption before reading and the same result
digest as their baseline. Both adaptive modes reported a final adaptive plan; the
coalesced final plan contains `AQEShuffleRead coalesced` over the four-reducer exchange.
Its final exchange differs from the initial exchange, exercising preparation after
AQE stage transformations.

Each mode passed all 13 driver invocations. Concurrent replacements each launched
zero map tasks. Source-token, source-snapshot, producer-filter, missing-manifest and
owner-restart controls rejected adoption and recomputed. Artifact-loss and lease-expiry
controls observed fetch failures, ran one fresh target map task and returned the same
32-row result. Raw properties and initial/final plans are retained in the artifacts.

This establishes native recovery with full and coalesced AQE readers for this fixture:
one host, `local[2]`, one source mapper and four reducers. It does not establish skew or
mapper-local reads, multi-host behavior, production-scale performance, or exact native
reducer byte statistics. The provider's map-status sizes remain scheduling estimates.
Adopted SQL runtime statistics report unknown row count because absent current-driver
write metrics must not be interpreted as proof that the retained exchange is empty.
