# Phase 0 persistent reference shuffle provider

This feasibility-only provider tests whether completed sort-shuffle bytes can survive one JVM and
be reopened by a fresh JVM with a fetch-correct representation. It is deliberately narrower than
the proposed production durable-shuffle contract.

## Spark boundaries used

The write side follows `ShuffleMapOutputWriter` / `ShufflePartitionWriter` semantics. Each map
attempt writes partition bytes, then commits one consolidated data file and one exact reducer
index. The read side resolves non-empty reducers as `FileSegmentManagedBuffer` ranges using the
same offset/length model as `IndexShuffleBlockResolver`.

The implementation is intentionally **not** configured as a general `ShuffleDataIO` plugin. The
current sort-shuffle plugin boundary can supply map writers, while ordinary reducer routing still
comes from Spark's tracker/resolver path. Wiring persistent bytes into that route before a validated
recovery decision would require scheduler/tracker coupling that this mechanism slice explicitly
does not add.

## Persistent layout and winner boundary

Identifiers are validated and URL-safe-base64 encoded before they become path components. The
reference layout is:

```text
<root>/<encoded-group>/<generation>/<encoded-incarnation>/
  .attempts/attempt-<map-task-id>-<nonce>/
    part-*.tmp
    data
    index
    READY
  maps/
    map-<map-index>.winner
    map-<map-index>/
      data
      index
      READY
```

A map-attempt commit creates only a candidate. It does not guess the scheduler winner. A separate
private `commitWinner(mapIndex, descriptor)` operation accepts the caller-selected output
descriptor, validates its committed bytes, creates a `CREATE_NEW` winner claim, and then performs a
same-filesystem atomic directory move. The winner bytes are never replaced. The winner claim is
retained as the fencing record. A crash after a claim but before promotion can cause a safe miss;
there is no lease or automatic destructive recovery in this reference mechanism.

The atomicity claim is limited to filesystems that provide `ATOMIC_MOVE` for a directory rename on
the same filesystem. It must not be generalized to object stores.

## Exact block index

The index is a bounded binary representation containing:

- magic and schema version;
- reducer count and flags;
- exact consolidated data-file length;
- all `R + 1` physical offsets;
- optional Spark-provided reducer checksums;
- a CRC over the index payload.

With checksums enabled, one map index occupies `40 + 16R` bytes. Without checksums it occupies
`40 + 8R` bytes. This is intentionally `M x R` information for the mechanism proof; its cost is
measured separately from the compact recovery manifest planned for later work.

A reducer block is genuinely empty iff its adjacent offsets are equal. Empty blocks return no
fetch buffer. Every non-empty block resolves to an exact physical file segment whose length is the
offset delta. No fabricated positive size and no false zero are used.

On reopen the codec validates schema/version, flags, reducer bounds, index size, CRC, monotonic
offsets, final offset, and exact physical data-file length before exposing any block.

## Reproduction

Focused storage, corruption, concurrency, lifecycle, and boundary tests:

```bash
./build/sbt -Phadoop-3 \
  "core/testOnly org.apache.spark.shuffle.ReferenceShuffleProviderSuite"
```

The process-boundary proof is intentionally two separate JVM invocations against the same root:

```bash
ROOT="$(mktemp -d)"
REPORT="core/target/shuffle-recovery-phase0/reference-provider/read-representation.md"

./build/sbt -Phadoop-3 \
  "core/Test/runMain org.apache.spark.shuffle.ReferenceShuffleProviderProcess write ${ROOT}"

./build/sbt -Phadoop-3 \
  "core/Test/runMain org.apache.spark.shuffle.ReferenceShuffleProviderProcess \
  read ${ROOT} ${REPORT}"
```

The second JVM verifies sparse and zero-total maps, one-byte and skewed non-empty blocks, wide
indexes, exact physical lengths, and explicit idempotent group cleanup. It also emits the compact
read-representation report consumed by the dedicated Phase 0 CI workflow.

## Explicit limitations

This mechanism does not add semantic computation identity, manifest lookup, scheduler adoption,
`MapOutputTracker` registration, AQE restoration, healing, remote-service durability,
authorization, leases, or retention management. It is a local/reference proof of persistent bytes,
exact reducer-block metadata, authoritative winner promotion, and cold-process resolution only.
