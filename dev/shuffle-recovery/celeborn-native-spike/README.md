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
Spark's test JVM options. The normal Core/SQL suites run in their separate CI lane. Each role then runs in a fresh Java process. Use CI for this
expensive validation. CI compilation and execution are in progress; a committed harness alone is not
evidence of successful native recovery.

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
