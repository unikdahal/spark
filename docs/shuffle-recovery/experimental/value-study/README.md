# Completed-shuffle recovery prospective value study

Status: **v2 preregistered only; no primary restart, overhead, or cost campaign has been executed**

This directory freezes the author-side value-study contract for the experimental completed-shuffle
recovery program. It does not change Spark runtime behavior and does not relabel the frozen Phase 0
`MECHANISM_FEASIBLE / VALUE_GATE_NOT_MET` result.

## Active prospective package

The active package is the v2 experiment plus its prospective economics correction. The correction
supersedes only the `Failure probability and economics` accounting inherited by the base v2
documents; all other v2 rules remain unchanged.

- [`value-experiment-v2.md`](value-experiment-v2.md): base deployment-value contract, including
  recovery-independent failure selection, matched disabled controls, primary accounting, run counts,
  overhead cells, confidence methods, gates, and change control.
- [`value-experiment-v2.json`](value-experiment-v2.json): machine-readable base experiment contract.
- [`cost-model-v2.md`](cost-model-v2.md): normative active economics model. It explicitly charges
  first-attempt Spark overhead, separates initial-submission and retry populations, preserves
  incremental-byte accounting, and keeps greenfield fixed capacity distinct from marginal costs.
- [`value-economics-v2.json`](value-economics-v2.json): machine-readable economics override,
  uncertainty contract, and deterministic worked ledgers.
- [`workload-scope-v1.md`](workload-scope-v1.md): unchanged deterministic dataset provenance, exact
  six-query corpus, supported producer grammar, downstream-consumer review, default-AQE policy, and
  primary mapper/reducer envelopes.
- [`cost-model-v1.md`](cost-model-v1.md): preserved historical v1 economics companion. It is not the
  active economics contract for v2 after the prospective correction above.

The machine-readable economics contract is checked with:

```text
python3 .github/scripts/shuffle-recovery-value-economics-test.py
```

## Preserved preregistration history

[`value-experiment-v1.md`](value-experiment-v1.md),
[`value-experiment-v1.json`](value-experiment-v1.json), and
[`cost-model-v1.md`](cost-model-v1.md) remain published unchanged. v1 was superseded prospectively
because its primary F0/F1 failure landmarks depended on recovery publication and did not define the
same intervention for the recovery-disabled control.

The economics correction is also prospective. Before it was registered:

- no primary restart timing had been observed;
- no primary overhead timing had been observed;
- no real Iceberg/Celeborn value campaign had been executed;
- no cost measurement had been executed or inferred for the correction;
- no new benchmark was run to choose the correction.

Historical Phase 0 opportunity evidence remains historical context only. It is not used to select v2
failure offsets or to estimate restart benefit.

## What v2 freezes before performance observation

The active package fixes:

- the same deployment scenario, workload cohort, producer grammar, default-AQE policy, input scales,
  weights, resource limits, pass thresholds, and scenario rates registered in v1;
- a mandatory independent failure-free calibration on the integrated candidate with recovery
  disabled: 24 workload/scale cells x 5 ordinary runs = 120 no-failure attempts;
- four primary failure offsets per workload/scale derived only from ordinary producer-completion and
  correct-result elapsed times, then frozen in `primary-failure-schedule-v2.json` before any restart
  timing begins;
- diagnostic downstream stage timestamps that cannot select or move a primary failure point;
- the exact same frozen elapsed-time offset in the recovery-disabled and recovery-enabled arms;
- no primary wait for publication, provider visibility, reuse success, or a recovery-only event;
- after-the-fact publication-state accounting so late/failed/unavailable publication remains an
  observed miss/outcome instead of disappearing from the cohort;
- explicit retention of failures before producer completion and zero-impact cases where a correct
  result completes before the scheduled failure, with no post-hoc weight redistribution;
- optional publication-boundary mechanism probes with zero primary scenario weight;
- 96 restart cells, 12 randomized pairs per cell, 1,152 pairs / 2,304 timed restart attempts;
- three separately registered no-failure overhead comparisons, each with 24 cells and 12 pairs per
  cell, totaling 72 cells / 864 pairs / 1,728 timed overhead attempts;
- comparison-specific overhead randomization and paired-bootstrap seeds;
- publication-overhead accounting that retains every publication-arm result rather than selecting
  only successful publications;
- failure-to-correct-result residual accounting, conservative timeout scoring, no outlier deletion,
  and fixed scenario composition;
- >=20% restart improvement with lower 95% confidence bound above zero;
- disabled <=1%, enabled-all-miss <=5%, and publication/no-failure <=5% upper-95% overhead gates;
- explicit first-attempt Spark cost `H` charged across the registered initial-submission population,
  including enabled misses, publication attempts, and partial work incurred before failed initial
  attempts;
- retry probability applied only to conditional replacement benefit/cost terms, never to `H`;
- incremental traffic measured against the same-provider control so ordinary control I/O is not
  charged twice;
- a complete-ledger economic bootstrap that uses the same frozen workload/scale weights and includes
  every relevant measured cost term in each net-value replicate;
- separate existing-provider and greenfield economics, with fixed greenfield capacity charged
  separately from marginal per-artifact costs.

The numeric 24-row failure schedule is intentionally not fabricated in this preregistration change.
It must be produced from the later integrated real deployment's independent ordinary dry runs and
frozen before primary timing. If those runs cannot produce the required schedule, the primary
campaign does not start under v2.

The economics worked ledgers are likewise illustrative specification checks rather than observations.
They intentionally include zero retries, zero publication copies, positive conditional retry saving
with negative net value, all-miss overhead, and existing-provider versus greenfield cases.

## Current implementation boundary

The experimental Spark tree is still being assembled toward the real source/provider candidate
required by the campaign. This package does not scaffold Iceberg certification, Celeborn discovery,
authenticated lifecycle, publication, adoption, AQE recovery state, or benchmark execution.

A later campaign may run only after the real integration proves the registered physical and semantic
boundary and emits the required failure schedule. Inability to do so is a gate result, not permission
to weaken the cohort after performance outcomes are visible.

This preserves recovery as an optional cache: uncertainty or incompatibility produces ordinary Spark
execution, publication failure remains best-effort, and no value-study setting weakens correctness
validation.
