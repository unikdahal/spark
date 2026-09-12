# Cold-process shuffle recovery proof

This directory documents the local reproduction command for the feasibility-only cold-driver
proof. The harness launches a clean baseline JVM, a producer JVM, and a replacement JVM for every
fetch-correctness scenario. It also repeats a happy path in a fresh namespace, kills one producer
process after the committed-manifest signal, and runs the cache-miss controls in fresh replacement
JVMs.

From the Spark repository root:

```bash
rm -rf sql/core/target/shuffle-recovery-phase0/cold-process
mkdir -p sql/core/target/shuffle-recovery-phase0/cold-process
SPARK_SHUFFLE_RECOVERY_COLD_PROCESS_EVIDENCE_DIR="$PWD/sql/core/target/shuffle-recovery-phase0/cold-process" \
  ./build/sbt -Phadoop-3 -Phive \
  "set sql / Test / fork := true" \
  "sql/testOnly org.apache.spark.shuffle.ShuffleRecoveryColdProcessSuite"
```

The aggregate evidence is written to
`sql/core/target/shuffle-recovery-phase0/cold-process/cold-process-evidence.tsv`. Child stdout and
stderr are drained into per-process log files beside the structured rows. Elapsed times in the
bundle are mechanism diagnostics only and are not performance claims.

The reference provider used here is intentionally local and feasibility-only. The replacement JVM
is genuinely cold, but local-mode executor tasks share that replacement JVM's blocking shuffle
manager. A production remote-executor integration needs an executor-visible compact provider
binding; this proof does not claim that public provider SPI or remote transport design.
