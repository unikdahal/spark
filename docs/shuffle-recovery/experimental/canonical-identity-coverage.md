# Canonical shuffle-recovery identity coverage

This document is the reviewed coverage matrix for the experimental computation-identity encoder.
The identity is a correctness boundary, not a generic Spark-plan serializer. Anything not listed as
accepted below is a cache miss and ordinary Spark execution remains available.

The SQL encoder identifies itself as `spark-sql-canonical-encoder-v3`. The Core canonical binary
container remains explicitly versioned and bounded; a digest is used only for lookup and acceptance
still requires equality of the complete canonical payload.

## Accepted operator and expression classes

Only exact runtime classes are admitted. Subclasses and extension nodes are not accepted through
pattern inheritance or reflection.

| Category | Accepted exact classes | Semantic state encoded | Why this slice is reviewable |
| --- | --- | --- | --- |
| Plan wrappers | `WholeStageCodegenExec`, `InputAdapter` | No wrapper-local state; child only | These wrappers do not change the produced rows in this slice. |
| Projection | `ProjectExec` | Ordered project expressions and child | Expression order is output order. Alias names and `ExprId`s are routing metadata, not values. |
| Filter | `FilterExec` | Condition and child | The condition is encoded recursively under the same closed expression policy. |
| Source leaf | `RangeExec` | Start, end, step, slice count plus an external versioned source certificate | Range construction is fully represented and the source token remains authoritative for source-specific certification. |
| Value references | `AttributeReference`, `BoundReference` | Input ordinal, data type and nullability | Attempt-local `ExprId`s are resolved to stable input ordinals and are never encoded. |
| Literals | `Literal` | Exact supported value plus full reviewed data type | Primitive bit patterns, decimal scale/unscaled bytes, binary bytes and validated UTF-8 are canonicalized without display text. |
| Naming | `Alias` | Child value, resulting type and nullability | Name, qualifier and `ExprId` do not change the produced value. Non-empty output metadata is not yet admitted. |
| Comparisons | `EqualTo`, `EqualNullSafe`, `GreaterThan`, `GreaterThanOrEqual`, `LessThan`, `LessThanOrEqual` | Exact kind, children, type and nullability | These admitted classes carry no additional evaluation context in the reviewed slice. |
| Boolean logic | `And`, `Or`, `Not`, `IsNull`, `IsNotNull` | Exact kind, ordered children, type and nullability | No hidden configuration is inferred from plan display text or the current session. |

`Pmod` is deliberately not admitted. It captures `NumericEvalContext` at expression construction,
including `evalMode` and `allowDecimalPrecisionLoss`. The current canonical binary expression schema
does not encode that context, so every `Pmod` instance is refused even when the current session has
matching SQL configuration. This prevents a legacy null-on-zero producer from being confused with an
ANSI error-on-zero computation. Direct `Murmur3Hash` expression nodes are also not admitted; shuffle
hash semantics are represented separately by the partitioning contract below.

Nondeterministic expressions, Python/Arrow expressions, unknown nodes, subclasses and extension
classes are refused before identity creation.

## Literal and type coverage

| Type | Canonical value | Additional policy |
| --- | --- | --- |
| Boolean, byte, short, int, long | Exact scalar value | Type has its own versioned discriminator. |
| Float, double | Canonical NaN bits; signed zero remains distinct | Avoids host string formatting and preserves meaningful IEEE distinctions. |
| Date | Exact day integer | Type discriminator distinguishes it from ordinary int. |
| Timestamp, timestamp without time zone | Exact microsecond long | Timestamp kind is explicit; session time zone is separately encoded. |
| Decimal | Declared precision/scale plus exact unscaled bytes | No decimal string round trip. |
| Binary | Exact owned bytes | Core factory defensively owns mutable input. |
| String | Exact valid UTF-8 byte sequence after a strict validity and round-trip check | Malformed UTF-8 and strings over 16 KiB are refused. Data type includes collation id and fixed/max-length constraint. |

A Spark `UTF8String` is inspected as bytes first. Invalid UTF-8 is never converted through a lossy
host `String`. For valid UTF-8, the encoder verifies that decoding and re-encoding yields the exact
same bytes before using the Core strict-string canonical value. UTF-8 has a unique valid encoding for
a code-point sequence, so accepted values preserve their original bytes; malformed representations
are bounded misses rather than normalized values.

Unsupported data types are refused. String-valued hash partition keys remain refused because the
additional collation/hash compatibility switches have not yet been reviewed as one closed contract.

## Other semantic categories

| Category | Encoded facts | Refusal boundary |
| --- | --- | --- |
| Output contract | Ordered field data type, nullability, row encoding, serializer compatibility and Core row-codec compatibility | Non-empty output metadata is refused until individual semantic metadata keys have reviewed encoders. |
| Source certificate | Ordered versioned source-token payloads | Missing source certificate or invalid token/decomposition state refuses identity creation. |
| Mapper decomposition | Mapper count and ordered source ordinal, source-partition ordinal, descriptor version and descriptor bytes for each split | Invalid counts, ordinals, versions and oversized descriptors are rejected by the bounded Core codec. |
| Partitioning | Hash vs single partition, partition count, ordered partition expressions, Murmur3 seed `42` and explicit hash compatibility id | Range and unknown partitioning are refused; string hash keys are refused. |
| Resolved values | Sorted key/value entries using the same canonical value ADT | Oversized collections or unsupported values are refused. |
| SQL semantics | ANSI flag and session time zone | These are conservative attempt-determined discriminators, not substitutes for expression-captured state. |
| SQL encoder | `spark-sql-canonical-encoder-v3` | Earlier SQL encoder payloads cannot compare equal to this repaired builder output. |
| Shuffle compression | Enabled flag; reviewed codec; LZ4 block size when active | Only uncompressed output or LZ4 with a positive block size no greater than 16 MiB is admitted. Inactive codec names are intentionally ignored. |
| Shuffle encryption | Explicit disabled value | Encryption-enabled shuffle is refused because cross-driver key compatibility is not certified by this prototype. |
| Spark/runtime compatibility | Frozen Spark compatibility id, UnsafeRow encoding/serializer/codec identifiers and sort-shuffle write-format id | A build/format change produces a different identity or is refused; no cross-version compatibility is claimed. |
| Provider compatibility | Explicit provider read-format id | Provider-format changes cannot match the complete canonical payload. |
| Parent exchanges | Ordered complete parent computation identities and explicit parent references | Cycles, invalid references and unused parents are rejected. |

## Bounds and acceptance

The SQL builder refuses an identity before constructing an unbounded semantic tree: depth is capped
at 64 and the combined operator/expression node budget at 4096. Output fields, resolved values and
string literals are separately bounded. The Core binary codec additionally caps total payload size,
collection sizes, token/binary sizes, parent count, depth and semantic nodes during both encoding and
decoding.

Lookup may use the SHA-256 digest and a short prefix for indexing, but neither is semantic authority.
After a digest hit, the complete bounded canonical bytes must compare equal. Unknown binary versions,
tags, malformed lengths, truncated inputs and trailing data are rejected.

This encoder is intentionally narrow. It does not claim compatibility across Spark builds, arbitrary
SQL expressions, arbitrary metadata, encryption-enabled shuffle, range partitioning, collated string
hashing, Python/Arrow execution, aggregates, streaming, writes or generic RDD shuffles.
