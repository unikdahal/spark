# Shuffle recovery experimental implementation inventory

This document records the reconciliation snapshot used to continue the completed-shuffle recovery
prototype without rewriting its frozen Phase 0 evidence. It is an inventory of code and evidence,
not a claim that every historical issue is integrated or production-ready.

## Immutable lineage snapshot

| Item | Immutable revision | Disposition |
| --- | --- | --- |
| Frozen upstream baseline | [`2a7cfea06ba135cf0ddc62902eb0daf5a835c672`](https://github.com/apache/spark/tree/2a7cfea06ba135cf0ddc62902eb0daf5a835c672) | Reference baseline; unchanged. |
| Frozen Phase 0 evidence | [`7888d1e26d32e69694083e367ee6a92b28277c48`](https://github.com/unikdahal/spark/tree/7888d1e26d32e69694083e367ee6a92b28277c48) | Preserved byte-for-byte on `spip/shuffle-recovery-phase0`; tree `fe2dd1b768ea130795eb071a82bd8bc2cc2817f6`. |
| Experimental audit starting head | [`df49f2a262d5fdb0c0acf06a6c1c18c464cd90fc`](https://github.com/unikdahal/spark/tree/df49f2a262d5fdb0c0acf06a6c1c18c464cd90fc) | P01 starts here. It is six commits ahead of the frozen Phase 0 commit and zero commits behind it. |
| Attempt-context candidate PR #49 | [`cc1a688e4ab659bdb83fe4f1ac18a8eae68261f0`](https://github.com/unikdahal/spark/tree/cc1a688e4ab659bdb83fe4f1ac18a8eae68261f0) | Competing candidate; not integrated. |
| Attempt-context candidate PR #50 | [`304823842b41bc43f17310a9d79728e0478730ab`](https://github.com/unikdahal/spark/tree/304823842b41bc43f17310a9d79728e0478730ab) | Selected salvage lineage for later re-review; not integrated. |
| Audit-repair candidate PR #51 | [`7c06f093929a9fcd01c17546cff4d6ec090e9b0b`](https://github.com/unikdahal/spark/tree/7c06f093929a9fcd01c17546cff4d6ec090e9b0b) | Mixed provider/read candidate; non-compiling at this exact head and not integrated. |
| Intended durable-provider branch | [`3649f400c06c11ae8b69c06928b24d232f1710fa`](https://github.com/unikdahal/spark/tree/3649f400c06c11ae8b69c06928b24d232f1710fa) | Useful salvage source; not integrated and not completion evidence. |

P01 has no predecessor implementation issue. The revised program begins here, so there is no
predecessor merge to satisfy before creating this branch.

The frozen Phase 0 commit is 13 commits ahead of the upstream baseline. The experimental starting
head is a direct descendant of the frozen Phase 0 commit. P01 does not modify, rebase, or retag the
frozen branch.

## Classification vocabulary

- **implemented-and-exercised**: production behavior exists and a test/evidence path actually
  exercised that behavior at an identified revision.
- **implemented-but-unverified**: implementation exists, but the relevant current-head behavior is
  not backed by an executed passing check sufficient for the claimed contract.
- **scaffold-only**: declarations or helper types exist but no production call path establishes the
  intended behavior.
- **absent**: the behavior is not present in the integrated experimental tree.

A historical green build does not upgrade a later changed head. A type definition or PR title does
not count as a production caller.

## Integrated behavior inventory at the experimental starting head

| Behavior | Classification | Production path and evidence | Next owner / limitation |
| --- | --- | --- | --- |
| Frozen local reference-provider publication, claim, current-shuffle binding, scheduler adoption, zero-map reuse and whole-shuffle fallback | **implemented-and-exercised** | The frozen implementation is in [`ReferenceShuffleRecoveryClaimProvider.scala`](https://github.com/unikdahal/spark/blob/7888d1e26d32e69694083e367ee6a92b28277c48/core/src/main/scala/org/apache/spark/shuffle/ReferenceShuffleRecoveryClaimProvider.scala), [`ShuffleRecoverySchedulerAdoption.scala`](https://github.com/unikdahal/spark/blob/7888d1e26d32e69694083e367ee6a92b28277c48/core/src/main/scala/org/apache/spark/ShuffleRecoverySchedulerAdoption.scala), and [`ShuffleRecoveryAdoptedFailure.scala`](https://github.com/unikdahal/spark/blob/7888d1e26d32e69694083e367ee6a92b28277c48/core/src/main/scala/org/apache/spark/shuffle/ShuffleRecoveryAdoptedFailure.scala). Frozen Core and cold-process suites exercised the narrow reference-provider mechanism. | Historical feasibility evidence only. It does not satisfy a real-provider gate, AQE conformance, current authorization, or the revised scalable reader contract. |
| Canonical computation identity codec and builder | **implemented-and-exercised** for the covered component cases; two correctness behaviors are **absent** | Integrated code: [`ShuffleRecoveryComputationIdentity.scala`](https://github.com/unikdahal/spark/blob/df49f2a262d5fdb0c0acf06a6c1c18c464cd90fc/core/src/main/scala/org/apache/spark/shuffle/ShuffleRecoveryComputationIdentity.scala) and [`ShuffleRecoveryComputationIdentityBuilder.scala`](https://github.com/unikdahal/spark/blob/df49f2a262d5fdb0c0acf06a6c1c18c464cd90fc/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleRecoveryComputationIdentityBuilder.scala), with component suites in Core and SQL. | Exact `UTF8String` literal-byte preservation and captured `Pmod` numeric-evaluation context are not represented. P06 / #58 owns repair and closed allowlist audit. |
| Source-read token/reference-source contract | **implemented-and-exercised** as a reference contract | Integrated [`ShuffleRecoverySourceReadIdentity.scala`](https://github.com/unikdahal/spark/blob/df49f2a262d5fdb0c0acf06a6c1c18c464cd90fc/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleRecoverySourceReadIdentity.scala) and [`ShuffleRecoverySourceReadIdentitySuite.scala`](https://github.com/unikdahal/spark/blob/df49f2a262d5fdb0c0acf06a6c1c18c464cd90fc/sql/core/src/test/scala/org/apache/spark/sql/execution/exchange/ShuffleRecoverySourceReadIdentitySuite.scala). | It is not yet the resolved real-source certification used by adoption. P05 / #57 proves a real source; P10 / #62 integrates certification and eligibility. |
| Versioned durable-provider capability replacing reference-provider-specific Spark callers | **absent** in the integrated tree | Integrated scheduler/resolver code still depends on `ReferenceShuffleRecoveryClaimProvider`. The historical provider milestone's squash commit [`288396539989bc54c2bdafeaeb18db342cfc8a5e`](https://github.com/unikdahal/spark/commit/288396539989bc54c2bdafeaeb18db342cfc8a5e) has the same tree as its parent (`fe2dd1b768ea130795eb071a82bd8bc2cc2817f6`). | P04 / #56 first proves real-provider feasibility. P09 / #61 defines the minimal contract from Gate A evidence. |
| Durable-provider capability implementation on the abandoned intended branch | **implemented-but-unverified** | [`DurableShuffleRecoveryProvider.scala`](https://github.com/unikdahal/spark/blob/3649f400c06c11ae8b69c06928b24d232f1710fa/core/src/main/scala/org/apache/spark/shuffle/DurableShuffleRecoveryProvider.scala) defines versioned capability negotiation, winner certification, attempt-local release, exact-incarnation retirement and exact read views. [`DurableShuffleRecoveryProviderSuite.scala`](https://github.com/unikdahal/spark/blob/3649f400c06c11ae8b69c06928b24d232f1710fa/core/src/test/scala/org/apache/spark/shuffle/DurableShuffleRecoveryProviderSuite.scala) is reusable test material. | The branch is not the integrated tree and predates the revised Gate A architecture. P09 / #61 must re-review and adapt useful pieces instead of merging it wholesale. |
| Recovered aggregate-statistics and read-capability declarations | **scaffold-only** | [`ShuffleRecoveryReadState.scala`](https://github.com/unikdahal/spark/blob/df49f2a262d5fdb0c0acf06a6c1c18c464cd90fc/core/src/main/scala/org/apache/spark/shuffle/ShuffleRecoveryReadState.scala) declares statistics, capability decisions and exact-block result ADTs, but the integrated scheduler/tracker/read path does not consume them. | P13 / #65 owns the provider-native bounded adopted-read representation; P15 / #67 owns AQE capability/final-spec guards. |
| Dense recovered `MapStatus` reconstruction | **implemented-and-exercised** as the Phase 0 mechanism, and explicitly **code to replace** | [`buildStatuses`](https://github.com/unikdahal/spark/blob/df49f2a262d5fdb0c0acf06a6c1c18c464cd90fc/core/src/main/scala/org/apache/spark/ShuffleRecoverySchedulerAdoption.scala#L638) opens every mapper, allocates an `R`-length array, enumerates every reducer block, then constructs ordinary `MapStatus`. | This is O(M×R) preparation/status detail and loses exact physical accounting through ordinary compressed status semantics. P13 / #65 replaces it; Gate B / #72 must prove bounded state. |
| Explicit scheduler adoption transaction independent of implicit tracker/provenance state | **absent** in the revised sense | The Phase 0 scheduler adoption manager has local reservations and atomic-looking installation, but the target program requires one explicit generation-fenced transaction spanning the selected new read/provider contracts. | P14 / #66 owns the transaction after the provider/read contracts exist. |
| Authenticated attempt context, generation allocator, current-use authorization and group-vs-attempt lifecycle | **absent** in the integrated tree | Current integrated preparation still accepts plain `recoveryGroup` and `currentGeneration` in [`ShuffleRecoveryPreparation.scala`](https://github.com/unikdahal/spark/blob/df49f2a262d5fdb0c0acf06a6c1c18c464cd90fc/core/src/main/scala/org/apache/spark/shuffle/ShuffleRecoveryPreparation.scala). | P08 / #60 owns integration after Gate A. PR #50 is the selected salvage source; it is not evidence that P08 is complete. |
| Exact old-byte read after producer driver/executor loss on a real durable provider | **absent** | Frozen cold-process tests use the local reference-provider slice. | P04 / #56 and Gate A / #59. Reference-provider evidence cannot substitute for this result. |
| Experimental focused/full/evidence CI with exact-head evidence gates | **absent** in the revised sense | The current dedicated workflow runs hygiene/style plus three identity jobs for every PR and checks only for report XML presence. It no longer carries the frozen workflow's focused/full/evidence modes or provider/read/cold-process coverage. | P02 / #54 restores exact-candidate focused CI and explicit evidence gates. |

## Current-head disposition of every audit finding

The earlier audit remains useful as a defect inventory, but its old issue ownership is superseded.
The following table records what is true at the P01 starting head rather than inferring status from
closed historical issues.

| Audit finding | Current-head disposition | Replacement work |
| --- | --- | --- |
| Historical provider-contract merge was an empty tree change | **Still valid.** No durable-provider-neutral production contract is integrated. The useful branch at `3649f400c06…` is salvage only. | Real-provider feasibility: #56; minimal experimental provider contract after Gate A: #61; integrated gate: #72. |
| Scalable recovered-read/AQE milestone contains declarations but not its selected representation | **Still valid.** `ShuffleRecoveryReadState` is scaffold-only; dense ordinary `MapStatus` reconstruction remains active. | Provider-native adopted read: #65; AQE/final partition-spec guards: #67; Gate B: #72. |
| String literal identity loses original `UTF8String` bytes via `toString` | **Still valid and unpatched at the integrated starting head.** | Canonical identity repair and allowlist: #58. |
| `Pmod` identity omits captured `NumericEvalContext` | **Still valid and unpatched at the integrated starting head.** | Canonical identity repair and allowlist: #58. |
| Experimental CI lost focused/full/evidence gate structure and positive executed-test accounting | **Still valid.** | #54. |
| Source token/decomposition contract is not wired into the supported adoption path | **Still unintegrated.** | Real source proof: #57; Gate A: #59; eligibility/source integration: #62. |
| Current capability/claims table needed | **Satisfied for the reconciliation snapshot by this inventory**, but final vote-grade claims remain future work. | Final audited package: #74. |
| Next value experiment must be prospective rather than rewriting the Phase 0 N/A result | **Historical result preserved; new experiment not yet run.** | Workload/preregistration: #55; measured economics: #73. |
| Real source/provider conformance and final integration evidence remain missing | **Still missing by design.** | #56, #57, Gate A #59, and Gate B #72. |

## Attempt-context overlap: PR #49 versus PR #50

PR #49 and PR #50 diverge from merge base
[`3b475622909276e9a021e137c23928bb0a50e57b`](https://github.com/unikdahal/spark/tree/3b475622909276e9a021e137c23928bb0a50e57b).
They must not both be merged.

### Selection

**Preserve PR #50 (`304823842b41…`) as the only attempt-context salvage lineage. Do not merge
PR #49. Do not merge PR #50 as P01 or as proof that P08 is done.**

PR #50 is the more coherent starting point because it separates an explicit lineage-incarnation
key from authorization, uses a bounded deterministic generation allocator, carries a local
revocation fence that can be checked without scheduler-thread external I/O, distinguishes attempt
stop from group finish, and contains hardening for authorization-revision exhaustion, allocator
capacity, hostile null `Option` state, external authority exceptions and redacted diagnostics.
Its exact-head dedicated workflow run `34161434454` completed successfully.

PR #49 has a narrower two-file implementation and a successful exact-head workflow run
`34159940077`, but it couples more discovery/claim/lifecycle behavior into one attempt-lifecycle
object and lacks the later hardening. Its useful design history remains available at immutable head
`cc1a688e4ab659…`.

Both implementations predate Gate A and therefore remain **implemented-but-unverified** candidates,
not accepted architecture. P08 / #60 must start from the then-current integration branch after Gate
A, re-review the selected #50 code against real source/provider evidence, and preserve commit author
metadata when salvaging code. No duplicate attempt-context implementation should survive in the
integration tree.

## PR #51 assessment against the revised architecture

PR #51's current head is not the implementation described by its draft body:

- its current changed-file set contains provider/read-path files but no canonical identity builder,
  identity regression test, or workflow file;
- therefore the audit's string-literal and `Pmod` identity fixes are **absent** from this PR head;
- it adds a compact recovered `MapStatus` marker plus a tracker bridge that requests exact block
  metadata instead of storing reducer sizes in every marker;
- it adds no read-specific test suite at the current head; the only new suite in the changed-file
  set is `DurableShuffleRecoveryProviderSuite`;
- exact-head workflow run `34160579407` fails before tests. Core compilation reports that
  `shuffleBlockResolver` is not a member of the `ShuffleManager` interface at both bridge call
  sites in `ShuffleRecoveryMapOutputTrackerSupport.scala`.

The reader idea is therefore **implemented-but-unverified and currently non-compiling**. It is also
not automatically the revised target: #52 asks P13 / #65 to evaluate a provider-native descriptor
with bounded provider-direct paging and explicit accounting rather than assuming this tracker bridge
is correct. Useful marker/query concepts may be compared there, but the PR must not be merged as a
bulk audit repair.

Its provider-contract material overlaps the `3649f400…` salvage branch and is useful source for
P09 / #61 only after Gate A. Identity correctness remains independently owned by #58.

## Durable-provider branch recovery

The intended provider branch was verified by tree difference, not by its historical merge message.
Relative to the frozen Phase 0 commit, `3649f400c06…` is seven commits ahead and changes six files:

- adds `core/src/main/scala/org/apache/spark/shuffle/DurableShuffleRecoveryProvider.scala`;
- substantially adapts `ReferenceShuffleRecoveryClaimProvider.scala`;
- makes small changes to adopted-failure, claim and index-resolver code;
- adds `core/src/test/scala/org/apache/spark/shuffle/DurableShuffleRecoveryProviderSuite.scala`.

Useful concepts to retain for P09 include bounded/versioned capability negotiation, exact winning
selection certification, binding-versus-artifact lifetime separation, optional exact-incarnation
retirement, explicit lifecycle authority and lazy exact block metadata. These concepts are not
accepted merely because the branch exists. Real-provider Gate A evidence may change the minimal
contract.

## Reusable tests and evidence

The following are worth preserving while keeping their claims narrow:

- frozen `ReferenceShuffleProviderSuite`, `ShuffleRecoverySchedulerAdoptionSuite` and
  `ShuffleRecoveryAdoptedFailureSuite` for reference-provider/trust/scheduler/fallback regressions;
- frozen independent-JVM `ShuffleRecoveryColdProcessSuite` and supporting processes for cold
  driver-replacement mechanics;
- current `ShuffleRecoveryComputationIdentitySuite` and
  `ShuffleRecoveryComputationIdentityBuilderSuite` as canonical-codec/builder regression bases;
- current `ShuffleRecoverySourceReadIdentitySuite` as a reference source-token contract suite;
- provider-branch `DurableShuffleRecoveryProviderSuite` as a capability/lifecycle test source to
  re-review under #61;
- PR #50 `ShuffleRecoveryAttemptContextSuite` and
  `ShuffleRecoveryAttemptContextHardeningSuite` as deterministic allocator/auth/lifecycle race
  tests to salvage under #60.

None of those test names alone prove the new real-provider, scalable-reader, AQE, authorization or
Gate B contracts. Future issues must link tests actually executed on their exact integrated heads.

## Temporary feasibility interfaces and migration plan

| Temporary / historical mechanism | Migration |
| --- | --- |
| `ShuffleRecoveryFeasibilityIdentity` and feasibility inputs | #58 replaces/repairs canonical identity policy; #62 connects resolved source certification at the correct materialization boundary. |
| Plain `recoveryGroup` / `currentGeneration` preparation inputs | #60 introduces the post-Gate-A authenticated attempt context and current-use authorization. |
| `ReferenceShuffleRecoveryClaimProvider` embedded in generic scheduler/resolver call sites | #56 first tests a real provider; #61 defines the evidence-derived private provider contract; later call sites become provider-neutral. |
| Dense `buildStatuses` ordinary `MapStatus` reconstruction | #65 replaces it with bounded provider-native adopted-read state and exact accounting. |
| Implicit scheduler/tracker/provenance installation state | #66 makes adoption one explicit generation-fenced scheduler transaction. |
| Early/boolean-only AQE capability declarations | #67 adds capability-aware final reader/partition-spec guards with AQE still globally enabled. |
| Reference source adapter/test token | #57 establishes a real immutable source certificate; #62 integrates the resolved certificate. |
| Frozen reference-provider cold-process harness | Retain as a regression harness; it cannot stand in for #56/#59 real-provider evidence or #72 integrated Gate B. |

## Experimental branch ownership and workflow

- `spip/shuffle-recovery-phase0` remains frozen at `7888d1e26d32…`.
- New work branches from the then-current `spip/shuffle-recovery-experimental` head.
- Changes reach the experimental branch through ordinary reviewable PRs and are reversible until
  merged.
- P01 itself is documentation/reconciliation only; it does not salvage production code from
  #49/#50/#51 or the durable-provider branch.
- Gate A (#59) is the stop point before broad integration. Attempt context, provider contract,
  source integration, adopted reader, scheduler transaction and AQE integration must not be treated
  as prerequisites already supplied by historical #13/#15/#16.
- Reference-provider evidence is retained as mechanism evidence, not promoted into a real-provider
  result.

## Validation actually performed for this inventory

This reconciliation used immutable GitHub trees, commit comparisons, production-source inspection,
changed-file lists and exact-head Actions results. In particular:

- frozen Phase 0 was confirmed at `7888d1e26d32…`;
- the experimental start was confirmed at `df49f2a262d5…` with the frozen commit as merge base;
- provider branch `3649f400c06…` was compared by tree, exposing its six changed files;
- PR #49 exact head `cc1a688e4ab6…` has successful dedicated run `34159940077`;
- PR #50 exact head `304823842b41…` has successful dedicated run `34161434454`;
- PR #51 exact head `7c06f093929a…` has failed dedicated run `34160579407`; the uploaded compile
  diagnostics show two deterministic Core compile errors at `ShuffleRecoveryMapOutputTrackerSupport`
  because `ShuffleManager` has no `shuffleBlockResolver` member;
- PR #49/#50/#51 have no submitted human reviews or inline review threads at reconciliation time.

No Spark build is claimed for P01 because this change is an inventory/discussion-document change.
The P01 PR's own exact-head checks are recorded in its PR after they run.
