# Phase 0-A Issue #26 evidence campaign

This file freezes the follow-up evidence campaign for Issue #26 before the final manual run.
It supplements `PREREGISTRATION.md`; it does not relax any semantic eligibility rule from #3.

## Why this follow-up exists

The Phase 0-A smoke workflow run `33946102450` produced 15 observed physical exchanges but only
2 runtime-weighted exchanges. The smoke corpus remains useful as a deterministic instrumentation
regression, but it is not a Spark-PMC-facing value study.

Root-cause analysis of that same artifact showed completed shuffle-map stages that did not correlate.
The old join depended on shuffle-write SQL accumulator IDs being present in `StageInfo.accumulables`.
A materially executed shuffle can successfully produce zero rows/bytes and therefore never update a
shuffle-write SQL accumulator. That loses the exchange-to-stage identity even though successful map
task time exists.

Issue #26 changes the observation join, not Spark recovery semantics: the primary runtime identity is
`(SQL execution id, SparkPlan.rddScopeId)`. `ShuffleExchangeExec` already creates its shuffle
dependency under that exact RDD scope. SQL metric IDs remain an independent consistency/fallback
signal, and disagreement fails closed.

## Frozen correlation-quality gate

Before inspecting the final value result, the campaign fixes these rules:

- denominator: successful task `executorRunTime` from every materially executed, completed
  shuffle-map stage in the selected benchmark corpus;
- numerator: the same task time belonging to a stage correlated to exactly one counted physical
  exchange under the gate rule set;
- minimum acceptable task-time coverage: **95.0%**;
- every observed exchange must be `WEIGHTED`, explicitly `UNWEIGHTED(reason)`, or a documented
  exclusion;
- coverage is also reported by completed shuffle-stage count and successful shuffle-write bytes;
- no semantic eligibility rule may be weakened to improve correlation or the value percentage;
- if the 95% gate fails, the value result is not accepted as Phase 0 evidence.

Retries/speculation retain #3's accepted-winner rules. Physical shuffle identity is scoped to SQL
execution plus Spark shuffle ID. A later stage incarnation can replace recomputed map winners without
double-counting surviving outputs.

## Smoke tier

Ordinary PR CI continues to use the existing eight-case smoke set at two generated rows per table.
It exists for analyzer, accounting, AQE, deterministic-output, zero-work, and listener regressions.
Smoke numbers must not be quoted as the Phase 0 value estimate.

## Manual benchmark evidence tier

Manual evidence mode uses **64 deterministic generated rows per benchmark table**, four Spark SQL
shuffle partitions, broadcast joins disabled, and DPP enabled. Inputs are completely materialized
before study listeners are installed.

The TPC-DS subset is fixed here before the final run. It was selected for broad physical-plan shape
coverage while keeping the no-new-dependency deterministic harness practical:

`q1, q3, q7, q10, q13, q14a, q14b, q16, q17, q19, q23a, q23b, q24a, q24b, q27, q34,
q39a, q39b, q42, q43, q46, q52, q53, q59, q61, q64, q68, q73, q77, q79, q88, q98`.

Every listed TPC-DS query runs once with AQE disabled and once with AQE enabled.

TPC-H uses all in-tree queries `q1` through `q22`, again once with AQE disabled and once with AQE
enabled. No external benchmark generator or runtime dependency is introduced.

The benchmark headline value gate is computed from TPC-DS and TPC-H only. The query set is not
stopped or changed when a favorable/unfavorable percentage is reached.

## Synthetic supplement

The following deterministic cases are run under AQE off and on, but are **not** mixed into the
benchmark headline value denominator:

- sparse shuffle;
- wide reducer count;
- many mappers;
- heavy single exchange;
- sequential upstream shuffles;
- reused exchange;
- zero-output shuffle;
- controlled skew.

They exist to stress identity/accounting boundaries that a tiny benchmark data set may underweight.

## Required final artifacts

The manual workflow must retain, at minimum:

- benchmark raw JSONL opportunity records;
- benchmark rendered opportunity report;
- runtime-correlation quality report;
- per-exchange reconciliation TSV;
- sample-quality/sensitivity report;
- separate synthetic raw/report material when synthetic cases run;
- focused Scala test reports and compile/style diagnostics;
- exact workflow run ID, artifact name, head SHA, and frozen baseline SHA.

The reconciliation table includes execution ID, exchange ordinal/path, disposition/reason, stage and
attempt when correlated, shuffle ID, expected mapper count, accepted successful map winners,
stage-completion state, shuffle-write bytes, and executor run time.

## Sample quality and sensitivity

Physical exchanges are not IID observations. The final benchmark report therefore includes:

- per-query opportunity;
- aggregate task-time opportunity;
- TPC-DS/TPC-H split;
- AQE on/off split;
- sensitivity after removing the top 1 and top 5 task-time exchanges;
- equal-query versus task-time weighting;
- top-exchange and top-query task-time concentration;
- number of queries with zero material eligible work.

Any future confidence interval must resample at the query/workload unit, never individual physical
exchanges.

## Value interpretation

The preregistered Phase 0-A value threshold remains 20% of completed shuffle-map executor run time at
the declared failure distribution under the exact-source counterfactual rule. Counterfactual source
identity remains explicitly separate from observed source-token support.

A pass means only that the measured opportunity is large enough to justify continuing the prototype.
It is not adoption evidence. A fail or N/A is a valid outcome and must be reported without changing
the corpus or semantic rules after observation.
