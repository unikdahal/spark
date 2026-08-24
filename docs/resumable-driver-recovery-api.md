# Resumable Driver Recovery API Artifact

Spark's resumable-driver integrations span Spark, a durable shuffle/task-state provider, and data
source connectors. Those repositories need a precise view of the proposed Spark API while the
changes are still under development. The artifact described here is a deterministic compile-time
overlay for that contract.

It is not a standalone Spark library and must not be installed as a runtime replacement jar. In
particular, it intentionally contains selected classes such as `SparkSessionExtensions` and
`LogicalWriteInfo` that must match the complete Spark build identified by the provenance file.

## Build

Compile Catalyst and SQL first with the project resource limits:

```bash
taskset -c 0,1 env \
  JAVA_HOME=/home/unik/.sdkman/candidates/java/17.0.11-tem \
  JAVA_TOOL_OPTIONS=-XX:ActiveProcessorCount=2 \
  SBT_OPTS='-XX:ActiveProcessorCount=2 -Dsbt.task.cpus=1' \
  ./build/sbt -Dsbt.task.cpus=1 'catalyst/Test/compile' 'sql/Test/compile'
```

Then create the artifact with the same Java 17 installation:

```bash
env JAVA_HOME=/home/unik/.sdkman/candidates/java/17.0.11-tem \
  PATH=/home/unik/.sdkman/candidates/java/17.0.11-tem/bin:$PATH \
  dev/create-recovery-api-artifact.sh /tmp/spark-recovery-api.jar
```

The script writes:

- `/tmp/spark-recovery-api.jar`: selected API bytecode with fixed entry timestamps and no manifest;
- `.classes`: the sorted exact bytecode inventory;
- `.signatures`: `javap -public -s -constants` output for each allowlisted API;
- `.sha256`, `.classes.sha256`, and `.signatures.sha256`: content checksums; and
- `.provenance`: Spark HEAD, complete dirty-worktree hash/status, Scala/JDK versions, and usage
  restriction.

`dev/recovery-api-public-classes.txt` is the reviewed top-level API allowlist. The generator adds
only the exact public case-class companions and nested Java types needed to compile against that
surface. It does not copy private implementation helpers or wildcard-selected anonymous classes.

## Consumer validation

Downstream compilation must put the overlay before the matching full Spark dependency classpath
and must verify every published checksum and provenance field. A successful compile validates only
source and binary linkage to this exact worktree. Runtime testing must use a complete Spark build
from the same source state; mixing the overlay with another Spark build is unsupported.

Any API or signature change requires a new artifact and downstream recompilation. Versioned wire
fixtures remain the compatibility authority for persisted manifests and task envelopes; the API
artifact does not replace those fixtures.
