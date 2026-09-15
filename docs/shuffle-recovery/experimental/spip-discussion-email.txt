To: dev@spark.apache.org
Subject: [DISCUSS] Completed-shuffle reuse across driver attempts — proposal and working PoC

Hi Spark community,

I would like feedback on an opt-in mechanism that lets a replacement driver reuse a complete retained SQL shuffle when it independently establishes that the output matches its currently planned computation.

Remote shuffle storage can preserve bytes, but a new driver still needs to establish whether those bytes represent its own exchange before skipping producer tasks. In this prototype, the current source is resolved normally, a connector certifies the actual planned read, and Spark constructs a canonical computation identity. A compatible retained candidate can then be adopted through a local scheduler decision; an unavailable or incompatible candidate runs normally. No old source snapshot or driver state is restored.

I have prepared a short SPIP-style proposal and an implementation companion for early discussion:

Proposal (8 pages, PDF):
https://github.com/unikdahal/spark/blob/shuffle-recovery-discussion-20260915/docs/shuffle-recovery/experimental/spip-proposal.pdf

Implementation/API contracts and evidence (10 pages, PDF):
https://github.com/unikdahal/spark/blob/shuffle-recovery-discussion-20260915/docs/shuffle-recovery/experimental/spip-design-evidence.pdf

Markdown, editable Word files and package index:
https://github.com/unikdahal/spark/blob/shuffle-recovery-discussion-20260915/docs/shuffle-recovery/experimental/spip-draft.md

The documents describe the implemented approach: an immutable manifest store, scheduler-accepted map publication, tracker-backed adoption, provider-native reads and integration with Spark's fetch-failure recovery. The source and provider boundaries are generic; Iceberg and Celeborn are the concrete examples used in the native experiment. The integration surfaces are private and are not proposed as stable public APIs in their current form.

The native flow now passes with AQE disabled, AQE full-reducer reads and actual reducer coalescing. In each mode, a replacement driver returned the expected result with zero producer map tasks and native remote bytes read. Concurrent claims, identity misses, artifact loss, lease expiry and owner-restart controls also passed:
https://github.com/unikdahal/spark/actions/runs/34881546887

The fixture is deliberately small: one host, local[2], one source mapper, four reducers and 32 rows. These results establish mechanism feasibility, not production performance or broad workload coverage. Authentication/encryption, multi-host behavior, shared consumers and partial-result failure boundaries still need further work. The documents distinguish those gaps from the tested behavior.

I would particularly appreciate feedback on:

1. Whether real driver-retry workloads have enough retained producer cost to justify this mechanism.
2. Whether the tracker/handle adoption and existing recovery path are a suitable integration direction for Core and SQL.
3. The right source-certification and provider/manifest boundaries, alongside existing remote shuffle work.
4. Which consumer and AQE shapes would make a useful, manageable first upstream scope.

This is an early discussion request, not a vote or an announcement of an assigned SPIP JIRA. If the direction is useful, I would like to develop a formal SPIP with community feedback and a PMC shepherd. I will drive follow-up implementation and would welcome operators willing to share representative retry scenarios.

Thanks,
Unik Dahal
