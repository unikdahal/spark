# Shuffle recovery experimental CI evidence

The dedicated shuffle-recovery workflow validates the exact experimental candidate being proposed.
It does not use a successful run from the frozen Phase 0 branch as evidence for a later tree, and an
upstream-overlay compatibility experiment cannot satisfy this gate.

## Validation modes

`focused` is the default pull-request mode. It classifies the diff from the current experimental
integration merge base and runs every directly affected recovery boundary. Documentation-only
changes keep patch provenance but do not start correctness, compilation, or performance lanes.
Unknown Core/SQL source, build, workflow, and router changes are deliberately conservative and
select every recovery boundary available at that candidate.

`full` is an explicit tag-triggered correctness gate. Create a lightweight tag named
`shuffle-recovery-ci/full/<40-character-sha>` pointing at that same commit and push only that tag.
The workflow validates that the SHA encoded in the tag exactly equals the checked-out commit before
any expensive job starts.

`evidence` is the corresponding manual evidence mode, triggered by
`shuffle-recovery-ci/evidence/<40-character-sha>`. It has the same exact-SHA and correctness
requirements as `full` and labels every artifact `candidate_kind=experimental-tree` and
`validation_mode=evidence`. Heavy performance, large-corpus, or real-cluster campaigns remain
separate experiments; this mode does not manufacture performance evidence from a correctness run.

The tag trigger is intentional. GitHub only honors `workflow_dispatch` when the workflow file exists
on the repository default branch, while this prototype workflow is deliberately isolated to the
experimental lineage. A tag push executes the workflow version at the tagged experimental commit
without modifying the default or frozen branches.

Example invocation for an exact candidate `$SHA`:

```bash
git tag "shuffle-recovery-ci/full/${SHA}" "${SHA}"
git push origin "refs/tags/shuffle-recovery-ci/full/${SHA}"

git tag "shuffle-recovery-ci/evidence/${SHA}" "${SHA}"
git push origin "refs/tags/shuffle-recovery-ci/evidence/${SHA}"
```

Use a new tag for a new SHA. A malformed, abbreviated, mismatched, or moved tag fails. Pull-request
runs also re-resolve the current head before their final gate, so an older focused run cannot turn a
later PR head green. Historical artifacts remain tied to their recorded SHA.

## Focused boundary map

| Boundary | Representative checks |
| --- | --- |
| Identity / source | Core canonical-identity suite and independent-JVM vector proof; source-read, SQL identity-builder, and opportunity-analyzer suites; independent-JVM source-token proof |
| Provider / publication / discovery | Reference-provider, immutable-manifest, index-resolver, and sort-shuffle-manager suites plus independent-JVM provider read proof |
| Attempt / lifecycle | Current integrated provider lifecycle regressions and scheduler-adoption suite; later attempt-context implementation suites are required automatically once their paths exist and the selector is extended in the same change |
| Scheduler / tracker / reader / AQE / invalidation | Recovery scheduler-adoption and adopted-failure suites, `DAGSchedulerSuite`, `MapOutputTrackerSuite`, `BlockStoreShuffleReaderSuite`, and the SQL adaptive-partition recovery suite |
| Harness | Independent-process `ShuffleRecoveryColdProcessSuite` |

The suite selector deduplicates overlaps. Every selected suite must exist before the test command is
started and must produce a non-empty test report afterward. A missing suite, missing report, skipped
required job, failed process proof, or absent artifact is a failed recovery gate.

## Artifact schema

Every applicable job uploads a downloadable artifact with `checksums.sha256`. The exact contents
vary by lane, but the schema is intentionally regular:

- `provenance.env`: actual checkout SHA, experimental integration-base SHA, validation mode,
  candidate kind, workflow run identity, and lane routing where applicable;
- `dependencies.txt`: Java/build properties and immutable build-file hashes; compilation logs also
  record the resolved SBT and Scala versions;
- `commands.txt`: commands actually executed, not planned commands;
- `suites.txt`: fully qualified selected suites for Core or SQL/harness jobs;
- `compile.log`, `tests.log`, and focused `reports/*.xml` where those operations apply;
- process-boundary proof logs/results where the selected boundary requires them;
- `status.env`: separately records lint, license, compilation, test, proof, and job outcomes;
- `checksums.sha256`: SHA-256 for every other file in that artifact.

The preflight artifact additionally records changed paths and router output. The final gate artifact
records component-job outcomes and re-resolves the current candidate branch head. If that head no
longer equals the tested SHA, the run is marked stale and cannot be used as current evidence.

## Upstream-overlay compatibility

An upstream-overlay comparison is a distinct compatibility experiment. If one is run, its
provenance must identify the overlay/base separately and it must not reuse an
`experimental-tree` gate artifact or status. Only checks executed on the exact experimental tree can
satisfy this workflow's merge or evidence gate.
