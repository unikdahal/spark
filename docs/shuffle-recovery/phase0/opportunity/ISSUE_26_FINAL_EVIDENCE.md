# Issue #26 final Phase 0-A evidence record

This file records the result of the preregistered Phase 0-A evidence campaign from
`ISSUE_26_EVIDENCE_CAMPAIGN.md`. It is a result record, not a change to the frozen corpus,
semantic rule sets, correlation threshold, failure distribution, or value threshold.

## Immutable run identity

- Frozen upstream Phase-0 baseline: `apache/spark@2a7cfea06ba135cf0ddc62902eb0daf5a835c672`
- Evidence implementation head: `bb0f4d5b087fb732176871050249b19b82c9d571`
- GitHub Actions run: `33963836467`
- Evidence artifact: `shuffle-recovery-phase0-a-issue26-evidence-33963836467`
- Artifact ID: `9969032965`
- Artifact digest: `sha256:15bb5b3c2ebfbfa565c95163c9a2644cd9a147971f39c62d23583a84f6090013`
- Benchmark corpus: `spark-in-tree-tpc-evidence-v2`
- Synthetic supplement: `spark-controlled-synthetic-evidence-v1`
- Failure distribution: `equal-four-points-v2`
- Frozen correlation-quality threshold: `95.0%` of completed shuffle-map executor run time
- Frozen value threshold: `20.0%` under `exact-source-counterfactual-v1`

The Actions run has an overall `failure` conclusion only because the temporary post-measurement
validator checked for the stale corpus id `spark-in-tree-tpc-evidence-v1`. The benchmark corpus,
focused analyzer/runtime/correlation/report suites, AQE regression, listener-manager regression,
and evidence artifact upload all completed successfully. The permanent workflow now validates the
correct v2 corpus id.

## Corpus scale

The benchmark run executed the frozen 32-query TPC-DS subset and all 22 in-tree TPC-H queries,
with AQE disabled and enabled for every query: **108 SQL executions** total. Synthetic stress cases
ran separately and are not included in any benchmark headline denominator.

The benchmark produced:

- 1,082 observed gate-rule exchange records;
- 763 materially completed shuffle-map stages;
- 757 correlated completed shuffle-map stages;
- 307 documented excluded/reused exchange observations; and
- 18 explicitly unweighted final-plan exchange observations, all with
  `NO_RUNTIME_CORRELATION`.

The preferred engineering coverage target of at least 300 correlated materialized exchanges was
therefore exceeded substantially.

## Correlation-quality gate

| Metric | Correlated | Completed | Coverage |
|---|---:|---:|---:|
| Completed shuffle stages | 757 | 763 | 99.2% |
| Shuffle-write bytes | 198,816 | 200,545 | 99.1% |
| Shuffle-map executor run time | 55,784 ms | 56,022 ms | **99.6%** |

The preregistered **>=95.0% task-time correlation gate passes** without changing eligibility rules
or excluding unfavorable benchmark queries.

Every observed final-plan exchange is represented in `reconciliation-evidence.tsv` as weighted,
explicitly unweighted with a stable reason, or excluded. There is no silent observed-exchange loss.

### Six completed AQE stages not present in the final observed plan

The 238 ms / 1,729 byte residual completed-stage denominator is confined to six stages from four
AQE-on executions:

| Family | Query | SQL execution | Completed stage | Shuffle | Explanation |
|---|---|---:|---:|---:|---|
| TPC-DS | q1 | 49 | 38 | 13 | completed adaptive branch stage; final observed plan retains shuffles 10-12 |
| TPC-DS | q16 | 63 | 217 | 174 | completed adaptive branch stage; final observed plan retains shuffles 170-173 |
| TPC-DS | q16 | 63 | 218 | 175 | completed adaptive branch stage; final observed plan retains shuffles 170-173 |
| TPC-DS | q64 | 99 | 722 | 544 | completed adaptive branch stage; final observed plan retains shuffles 535-543 |
| TPC-DS | q64 | 99 | 723 | 545 | completed adaptive branch stage; final observed plan retains shuffles 535-543 |
| TPC-H | q8 | 167 | 1045 | 769 | completed adaptive branch stage; final observed plan retains shuffles 767-768 |

The scheduler log for each affected execution subsequently records sibling query-stage cancellation
with `The query stage is no longer referenced by the current adaptive plan`. Three of the four
executions have no unweighted final-plan exchange at all. This distinguishes the residual from a
failed stage-to-final-exchange identity join: AQE launched multiple candidate shuffle stages, some
finished before adaptive pruning, and the query listener's final physical plan no longer contains
those abandoned branch exchanges.

The six stages remain in the correlation denominator, so this limitation is conservative rather
than hidden. They account for only 0.4% of completed task time and do not require lowering the frozen
95% threshold.

## Opportunity result

The formal frozen value gate result is **N/A**, with a reported failure-distribution projection of
**36.8%**. It is intentionally N/A because 18 gate-rule exchange observations remain explicitly
unweighted; the preregistered report refuses to label the value gate PASS/FAIL when the gate-rule
observation set is not fully weighted. This policy was not changed after observing the result.

For interpretation only, among correlated benchmark work under `exact-source-counterfactual-v1`:

- eligible exchange count: 35.6%;
- eligible shuffle-write bytes: 39.5%;
- eligible completed shuffle-map task time: **19.4%**.

The four preregistered failure points report projected avoidable completed task time of:

- after first eligible completes: 44.5%;
- after multiple upstream shuffles complete: 39.0%;
- before the most expensive shuffle completes: 30.4%; and
- after an eligible/ineligible mix: 38.5%.

These are projections only. Phase 0-B must demonstrate real adoption before they can be described as
saved work.

## Sensitivity and sample quality

Physical exchanges are not IID observations. The evidence report therefore uses workload/query
sensitivity rather than binomial confidence claims.

Key sensitivity results:

- aggregate correlated task-time opportunity: 19.4%;
- equal-query opportunity: 42.6%;
- remove top 1 task-time exchange: 20.1%;
- remove top 5 task-time exchanges: 20.7%;
- TPC-DS task-time opportunity: 15.9%;
- TPC-H task-time opportunity: 35.8%;
- AQE-off task-time opportunity: 14.0%;
- AQE-on task-time opportunity: 53.0%;
- top-1 exchange task-time concentration: 3.4%;
- top-5 exchange task-time concentration: 6.2%;
- top-1 query task-time concentration: 7.6%;
- top-5 query task-time concentration: 28.1%; and
- queries with zero eligible material work: 12/108.

The deterministic benchmark tables contain 64 generated rows each. These measurements are Phase 0
feasibility evidence and are **not benchmark-scale performance estimates**; startup and scheduling
noise can materially affect the small absolute task times.

## Issue #26 conclusion

Issue #26 resolves the evidence-quality blocker raised by the original 15-exchange / 2-correlated
smoke result:

1. the dominant correlation defect was fixed by correlating SQL exchanges to the direct shuffle-map
   RDD scope, with SQL metric ids retained only as a consistency/fallback signal;
2. focused regressions cover zero-byte shuffles, retry/recomputation accounting, direct-scope
   identity, real multi-shuffle SQL, AQE/failure-distribution behavior, and listener lifecycle;
3. smoke and broad evidence modes are separate;
4. the final benchmark materially exceeds the preferred >=300 correlated-exchange target;
5. completed-task-time correlation is 99.6%, above the frozen 95% quality gate;
6. the remaining six completed-stage misses are explicitly attributable to AQE plan evolution and
   stay in the denominator; and
7. the value gate remains honestly **N/A** under the frozen completeness policy rather than being
   post-hoc reclassified.

This evidence is suitable input to the later Phase-0 GO/NO-GO package, with the explicit limitation
that the tiny deterministic generated data set measures feasibility and plan-shape opportunity, not
production-scale performance.
