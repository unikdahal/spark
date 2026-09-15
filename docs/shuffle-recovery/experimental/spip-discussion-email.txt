To: dev@spark.apache.org
Subject: [DISCUSS] Reusing completed shuffle output after a driver restart

Hi everyone,

When a batch application's driver fails after an expensive shuffle has finished, resubmitting the application can repeat the producer work even if a shuffle service still holds the output. I'd like to discuss whether Spark should be able to reuse that output when the new driver can establish that it needs the same computation.

The proposal is to resolve the new query's sources normally, compare a certified description of the read and producer with a retained exchange, and let the scheduler skip the producer maps only when the match and provider claim succeed. Otherwise Spark computes normally. Storage stays with the configured shuffle provider.

I've written up the proposal here:
https://github.com/unikdahal/spark/blob/shuffle-reuse-spip-discussion/docs/shuffle-recovery/experimental/spip-proposal.pdf

The proposal includes source, publication, discovery and reader-installation contracts with their Spark integration points. The companion specifies identity scope, admission rules, state transitions, failure outcomes, limits and conformance tests:
https://github.com/unikdahal/spark/blob/shuffle-reuse-spip-discussion/docs/shuffle-recovery/experimental/spip-design-evidence.pdf

Editable Word files and Markdown are linked from:
https://github.com/unikdahal/spark/blob/shuffle-reuse-spip-discussion/docs/shuffle-recovery/experimental/spip-draft.md

I'd start with a narrow batch SQL path: a certified scan with deterministic filters/projections, one completed blocking exchange, and a reviewed result consumer. Full and coalesced reducer reads are the first AQE targets. Writes, streaming and more complicated consumer graphs would need separate work.

The two questions I most want to understand are whether this captures enough real retry cost to be useful, and whether the adoption and recovery boundary fits Spark's scheduler without spreading complexity through unrelated paths. I'd also appreciate input from source and shuffle maintainers on certification, manifest ownership and access across applications.

If you've dealt with jobs repeating substantial producer work after driver loss, examples would be especially helpful in choosing the initial scope. I'm happy to drive the implementation and work through the design with interested maintainers. If there's support for the direction, I'd like to take it forward through the SPIP process with a PMC shepherd.

Thanks,
Unik Dahal
