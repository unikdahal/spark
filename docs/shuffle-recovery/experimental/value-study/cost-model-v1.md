# Completed-shuffle value-study cost model v1

Status: **preregistered planning assumptions, not vendor quotes**

This file makes the two deployment-economics lenses in `value-experiment-v1.md` numerically explicit:

1. incremental economics when a compatible durable shuffle provider already exists; and
2. greenfield economics when durable shuffle storage must be introduced for this feature.

Both use scenario USD. They are intentionally separate so sunk/shared provider cost is not hidden in
the greenfield case and ordinary shuffle writes are not double-counted in the existing-provider case.

## Common rates

| Cost item | Frozen scenario rate |
| --- | ---: |
| vCPU | $0.040 per vCPU-hour |
| memory | $0.005 per GiB-hour |
| durable reserved/retained storage | $0.000040 per GiB-hour |
| incremental provider read traffic/service when separately billable | $0.010 per GiB |
| incremental provider write traffic/service when separately billable | $0.010 per GiB |

These are experiment-model inputs only. A later vendor quote or real internal chargeback model is a
new prospective campaign input; it does not rewrite v1 after performance results exist.

## Existing-provider lens

When a compatible provider is already deployed for ordinary shuffle, the feature is charged only for
incremental work caused by making an output reusable across driver attempts.

For each query record:

```text
ordinary shuffle payload bytes written
incremental recovery-publication bytes written
provider-index bytes written
retained payload GiB-hours
retained index GiB-hours
incremental provider bytes read on recovery
incremental provider service CPU/memory if separately measurable
```

Ordinary shuffle writes are not a recovery cost merely because their bytes are later retained. If
publication can pin already-written immutable blocks without copying them, incremental publication
copy bytes are zero; retention, index, metadata, and recovery-read costs remain visible.

The expected per-query value is:

```text
expected_existing_provider_value =
  expected compute cost avoided at the registered 1.0% driver-failure rate
  - incremental recovery read cost
  - incremental publication write/copy cost
  - retained payload storage cost
  - retained provider-index storage cost
  - measured incremental provider service cost
```

The existing-provider economics gate requires a positive point estimate and lower 95% confidence
bound above zero.

## Greenfield introduction scenario

The greenfield scenario assumes the service exists only because completed-shuffle recovery is being
introduced.

Frozen query arrival rate: **20 primary read-only batch query attempts per hour**.

Frozen service footprint:

| Component | Count/capacity | Scenario hourly cost |
| --- | ---: | ---: |
| provider workers | 8 x (8 vCPU, 32 GiB) | $3.84000 |
| provider control plane | 3 x (2 vCPU, 4 GiB) | $0.30000 |
| reserved durable disk | 24 TiB = 24,576 GiB | $0.98304 |
| setup/operations allocation | fixed | $1.00000 |
| **total greenfield fixed cost** | | **$6.12304/hour** |

At 20 query attempts/hour, the frozen fixed introduction allocation is:

```text
$6.12304 / 20 = $0.306152 per query attempt
```

The 24 TiB disk reservation is chosen prospectively from the registered worst-case retention
envelope: 20 attempts/hour x 2 hours x 512 GiB maximum retained artifact = 20 TiB, plus 20% reserved
headroom = 24 TiB. Actual retained bytes and index bytes are still recorded; exceeding the registered
per-query resource limits fails the campaign even if spare disk exists.

The worker/control-plane sizing is a scenario capacity assumption, not a Celeborn sizing
recommendation or project claim. The later real-provider campaign must report whether the selected
provider can actually satisfy the workload within this footprint. If it cannot, v1 fails the
registered greenfield scenario or a new prospective version is required.

## Greenfield accounting

Reserved disk, worker compute/memory, control-plane compute/memory, and the operations allocation are
already included in the $6.12304/hour fixed introduction cost. They must **not** also be charged as
per-artifact storage or provider-service cost in the same greenfield calculation.

External network/traffic charges that are genuinely incremental and not represented by those fixed
resources are added separately from measured bytes. The `$0.010/GiB` read/write scenario rate is used
only when such a separate charge applies; it is not automatically layered on top of self-hosted
worker service compute.

The greenfield expected per-query value is:

```text
expected_greenfield_value =
  expected compute cost avoided at the registered 1.0% driver-failure rate
  - $0.306152 fixed introduction allocation per query attempt
  - separately billable incremental recovery traffic cost
  - separately billable incremental publication traffic cost
```

The greenfield economics result is reported independently from the existing-provider result. A PASS
for the existing-provider lens does not imply a greenfield PASS.

For a positive greenfield decision, the point estimate and lower 95% confidence bound of
`expected_greenfield_value` must both be above zero under the same frozen workload/failure weights.

## Capacity and utilization sensitivity

The primary v1 decision uses exactly 20 query attempts/hour. Sensitivity may additionally report
10/hour and 40/hour to show fixed-cost amortization, but those rows are labelled sensitivity only and
cannot replace the registered 20/hour greenfield decision.

No observed speedup may be used to change the fixed provider footprint, query arrival rate, storage
headroom, or operations allocation in v1. A materially different deployment assumption requires a
new prospective campaign version.
