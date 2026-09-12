# Completed-shuffle value-study cost model v2

Status: **prospective preregistration; no cost measurement was executed or inferred for this revision**

This is the normative economics companion for the active `value-experiment-v2` package. It replaces
only the economics accounting previously inherited from `cost-model-v1.md`; all non-economics v2
experiment rules remain unchanged. `cost-model-v1.md` stays unchanged because it is part of the
historical v1 preregistration.

The model keeps the same scenario rates and the same two deployment lenses:

1. incremental economics when a compatible durable shuffle provider already exists; and
2. greenfield economics when provider capacity is introduced for completed-shuffle recovery.

Both lenses use **scenario USD per registered initial query submission** as the common cost unit.

Machine-readable companion:
[`value-economics-v2.json`](value-economics-v2.json).

## Frozen scenario rates

The rates remain the v1 planning assumptions, not vendor quotes:

| Cost item | Frozen scenario rate |
| --- | ---: |
| vCPU | $0.040 per vCPU-hour |
| memory | $0.005 per GiB-hour |
| durable reserved/retained storage | $0.000040 per GiB-hour |
| incremental provider read traffic/service when separately billable | $0.010 per GiB |
| incremental provider write traffic/service when separately billable | $0.010 per GiB |

A later vendor quote or internal chargeback model is a new prospective campaign input. It does not
rewrite this package after performance results are known.

## Cost terms and populations

For workload/input-scale stratum `s`, define:

| Term | Population | Meaning |
| --- | --- | --- |
| `B_s` | replacement opportunity | conditional Spark compute cost saved by recovery versus the matched recovery-disabled replacement control, averaged over the frozen failure-point distribution |
| `H_s` | initial submission | incremental Spark driver/executor CPU, memory, and wait cost caused by recovery identity/certification, lookup, validation, scheduling bookkeeping, and publication-side Spark work |
| `S_s` | initial submission | marginal retained payload/index cost attributable to reusable artifacts |
| `T_initial,s` | initial submission | incremental publication/write/copy traffic cost attributable to recovery |
| `P_initial,s` | initial submission | marginal provider service cost attributable to recovery on the initial submission |
| `T_retry,s` | replacement opportunity | incremental recovery-read/retry traffic cost attributable to recovery |
| `P_retry,s` | replacement opportunity | marginal provider service cost attributable to recovery during replacement |
| `F` | accounting horizon | greenfield fixed provider-introduction/capacity allocation normalized to the same per-submission unit |

`B` is deliberately **gross of `H`** and gross of every provider, traffic, and storage charge. Provider
service work is not Spark work and cannot stand in for `H`.

The scenario driver-failure rate `p = 0.01` applies only to retry-conditional terms:
`B`, `T_retry`, and `P_retry`. It does not multiply `H` or any other initial-submission cost.

### Initial submissions, misses, and partial work

`H` is averaged over **all registered initial submissions on which recovery is enabled**, including:

- candidate hits;
- naturally absent/incompatible candidates;
- lookups or validation that end in an ordinary fallback;
- publication attempts that succeed, fail, finish late, or make no copy;
- failed or cancelled initial attempts.

For a failed or cancelled initial attempt, charge only recovery-specific work actually incurred before
the attempt ended. A component that was never reached contributes zero. The attempt itself is never
removed from the denominator merely because it failed before publication or query success.

A publication attempt with zero copied bytes therefore has zero publication-copy traffic, but its
identity, lookup, validation, scheduling, or publication-side Spark work still contributes to `H`.

Passing a no-failure overhead gate is a feasibility criterion only. It never sets `H` to zero, caps it
at the gate, or removes it from economic value.

## Byte accounting

Gross arm byte counters are retained as provenance when available. Economic traffic charges are
**incremental versus the same-provider matched control**.

Direct feature-attributable counters are preferred. If only arm totals are available, use the
prespecified paired enabled-minus-control difference and retain every pair, including negative/noisy
differences. Do not charge ordinary control reads or writes again merely because recovery also uses
the same provider.

This rule applies independently to initial publication traffic and replacement/recovery-read traffic.

## Existing-provider lens

For each workload/input-scale stratum:

```text
V_existing,s =
  p * (B_s - T_retry,s - P_retry,s)
  - H_s
  - S_s
  - T_initial,s
  - P_initial,s
```

The deployment estimate is:

```text
V_existing = sum_s w_s * V_existing,s
```

where `w_s` is the frozen workload weight times the frozen input-scale weight. `B_s`, `T_retry,s`,
and `P_retry,s` already average over the frozen failure-point weights; no hit-, publication-, or
survivor-conditioned reweighting is allowed.

The existing-provider economics gate requires a positive point estimate and a lower 95% confidence
bound above zero for this **complete** net ledger.

## Greenfield lens

The frozen provider footprint remains:

| Component | Count/capacity | Scenario hourly cost |
| --- | ---: | ---: |
| provider workers | 8 x (8 vCPU, 32 GiB) | $3.84000 |
| provider control plane | 3 x (2 vCPU, 4 GiB) | $0.30000 |
| reserved durable disk | 24 TiB = 24,576 GiB | $0.98304 |
| setup/operations allocation | fixed | $1.00000 |
| **total greenfield fixed cost** | | **$6.12304/hour** |

At the frozen 20 registered initial query attempts/hour:

```text
F = $6.12304 / 20 = $0.306152 per initial query submission
```

Greenfield accounting keeps fixed capacity separate from marginal per-artifact costs. If reserved
disk, worker service compute/memory, control-plane compute/memory, or traffic is already paid by `F`,
the corresponding greenfield marginal term is zero. A genuinely incremental external charge that is
not represented by `F` remains a marginal term.

Thus:

```text
V_greenfield,s =
  p * (B_s - T_retry,greenfield,s - P_retry,greenfield,s)
  - H_s
  - S_greenfield,s
  - T_initial,greenfield,s
  - P_initial,greenfield,s

V_greenfield =
  sum_s w_s * V_greenfield,s
  - F
```

`H` is identical in economic meaning across both deployment lenses and is never absorbed into `F`.

This preserves the original double-charging protection: fixed greenfield capacity is charged exactly
once, while truly marginal per-artifact work remains visible exactly once.

## Joint uncertainty for economic value

The economic confidence interval is not constructed by combining marginal confidence-interval
endpoints.

Use a deterministic stratified paired bootstrap with **10,000 replicates** and economics seed
`55083`. Keep the registered workload, input-scale, failure-point weights and `p = 0.01` fixed.

For every bootstrap replicate:

1. resample paired restart repetitions within each fixed workload/input-scale/failure-point cell;
2. resample paired no-failure repetitions within each fixed workload/input-scale cell;
3. keep primitive cost observations from the same attempt or pair together so covariance is
   preserved;
4. compute `B`, `H`, `S`, `T_initial`, `P_initial`, `T_retry`, and `P_retry` for that replicate;
5. convert every primitive term to scenario USD with the frozen rates;
6. apply the complete existing-provider formula;
7. apply the complete greenfield formula and subtract `F` exactly once.

The same workload/input-scale weights are used for the retry-benefit and no-failure-overhead
components. Failure-point weights enter only the retry-conditional component.

The 95% interval for each deployment lens is the 2.5th and 97.5th percentiles of its **complete
net-value replicate distribution**. `H` and every other measured cost term are therefore present in
every economic replicate.

If a required measured term or registered stratum is unavailable, the economic result is
`INDETERMINATE`. Do not substitute zero, drop the stratum, or renormalize the remaining weights.

Fixed scenario prices, `p`, workload/scale/failure weights, and `F` remain fixed in the primary
bootstrap. Prespecified price, utilization, or failure-rate sensitivity is reported separately and
cannot replace the primary result.

## Worked-ledger evidence

`value-economics-v2.json` contains deterministic illustrative ledgers for:

- zero retries;
- zero publication copies;
- positive conditional retry saving with negative net value;
- all-miss overhead;
- matched existing-provider and greenfield cases.

The examples use common abstract cost units and are not benchmark observations. In particular, the
positive-saving/negative-net ledger uses `p = 0.01`, `B = 40`, and `H = 1` against a 100-unit
no-failure control. Its 1% overhead passes a 5% feasibility threshold, but the economic result is
`0.01 * 40 - 1 = -0.60`. The threshold does not erase `H`.

The deterministic validator is:

```text
python3 .github/scripts/shuffle-recovery-value-economics-test.py
```

The validator recomputes every ledger and rejects contracts that remove `H`, apply `p` to
initial-submission charges, double-charge ordinary control I/O, omit a required acceptance case, or
form an economic confidence interval from less than the complete measured ledger.
