# ADR-0021: Parallel canonical block preparation in fresh construction

Status: Accepted
Date: 2026-05-20

## Context

Fresh canonical construction does CPU-heavy per-block work before it loads a
block into the inactive canonical builder: block deserialization, identity
validation, transaction fact extraction, compact-block construction,
transparent-output indexing, and logical-action counting. That work reads only
the block's own bytes and the network-upgrade activations, so it is
independent per block. Commitment-tree positioning and the builder load are
not independent: each block's tree sizes and frontiers fold from its
predecessor, and the builder validates parent links in height order.

Running every block through one task leaves available CPU idle and makes
source prefetch less useful. Positioning or loading blocks concurrently would
break deterministic tree-size folding and parent-link validation.

## Decision

Fresh canonical construction splits per-block work into a parallel
preparation stage and an ordered positioning and load stage.

```text
height-ordered source blocks
  -> parallel prepare_canonical_block on the blocking pool
  -> prepared blocks yielded in height order
  -> ordered position_canonical_block fold
  -> load into the inactive canonical builder
```

`ingest.construction.block_prepare_concurrency` bounds the number of in-flight
block preparations. The default follows host parallelism and caps the worker
count so source I/O, the builder load, metrics, and the Tokio reactor retain
capacity. [ADR-0022](0022-resource-budgeted-bulk-catchup.md) defines that
default and the `block_prepare_memory_watermark_bytes` admission watermark
that bounds prepared blocks waiting for the builder.

The stage output is a typed `PreparedCanonicalBlock`. Preparation performs no
historical prevout, wallet, or canonical-store reads. One blocking loader task
positions each prepared block immediately before it fans the block into the
canonical column families, so the store observes source order and advances
tree-size counters in order. The store stays `BUILDING` until construction
validates and publishes its baseline.

After construction publishes the baseline, the canonical follower prepares and
positions one block per iteration on the blocking pool. The parallel stage
applies only to fresh construction.

## Consequences

CPU-bound block parsing and artifact construction scale across cores while
tree-size folding and the builder load stay serial and deterministic.

Back-pressure is explicit. When preparation is the bottleneck, the in-flight
prepare slots fill. When source fetch or the builder load is the bottleneck,
the source-fetch watermark or the prepare memory watermark fills instead.
`zinder_ingest_canonical_block_construction_stage_duration_seconds` reports
per-block preparation cost by stage, so that pressure is visible rather than
hidden behind the worker pool.
