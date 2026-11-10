# ADR-0022: Resource-budgeted bulk catchup and checkpoint tree state

## Status

Accepted.

## Context

Mainnet catchup does not have a stationary block profile. Pre-Sapling,
Sapling, NU5, and later branches differ enough that fixed block-count knobs
mis-size the real resources that bound sync speed. The failure mode is a batch
sized in blocks or transparent-spend references crossing a byte, CPU, or
memory boundary that configuration does not express.

Tree-state reads have the same issue in the storage contract. Storing
`z_gettreestate` for every block puts arbitrary per-height archival in the
canonical hot path even though the wallet-facing requirement is latest or
checkpoint tree state.

## Decision

Bulk catchup is a resource-budgeted staged pipeline:

```text
SourceFetchStage
  -> CanonicalBlockPrepareStage
  -> CanonicalPrevoutResolveStage
  -> CanonicalPositionStage
  -> SubtreeRootAttachmentStage
  -> CanonicalCommitStage
  -> CanonicalFlushStage
```

The source boundary is `SourceChainSegmentLimits { cursor,
max_connected_blocks, target_response_bytes, max_response_bytes }`.
`NodeSource::fetch_chain_segment` fetches raw block bytes only. Tree state is a
separate source call, `fetch_tree_state_for_block(block_id)`, and JSON-RPC
adapters reject a tree-state response whose height or hash does not match the
requested block.

Fresh canonical construction consumes one `CanonicalPipelineLimits` value.
The runtime resolves it from the container memory budget, logical CPU count,
and `node.max_response_bytes`:

- `source_segment_max_blocks = 64`
- `source_segment_target_response_bytes = min(node.max_response_bytes, 33554432)`
- `source_fetch_max_in_flight_requests = 12`
- `source_fetch_max_in_flight_bytes = max(node.max_response_bytes, clamp(memory / 64, 128 MiB, 384 MiB))`
- `block_prepare_concurrency = min(available_parallelism, 16)`
- `block_prepare_memory_watermark_bytes = clamp(memory / 64, 128 MiB, 512 MiB)`

The segment sizer uses observed response bytes per block, p95 density, overshoot
memory after split attempts, and network-upgrade resets. The JSON-RPC response
default is 64 MiB, so the default segment target is 32 MiB. Source fetch and
block prepare may complete out of order, but ordered reassembly is the only
place that releases blocks to the prevout and serial positioning boundaries.
Block prepare derives canonical artifacts. The ordered prevout resolver uses
same-window outputs and a recent-output cache before issuing one deduplicated
multi-get for the window's remaining cold outpoints; canonical commit still
performs the authoritative fallback lookup. The cache shares
`block_prepare_memory_watermark_bytes`, so cache entries yield to new
prepare work instead of creating another independent memory ceiling. Prepare
workers reserve a conservative peak estimate before parsing and retain the
larger of that peak or measured completed residency through ordered prevout
resolution. The reservation then resizes to resident commit-preparation data
and remains attached until commit reassembly takes ownership.
This is admission control rather than a hard allocator cap: one oversized block
or a measured resize can exceed the watermark, after which new work pauses until
the reservation falls below the configured limit. Completed out-of-order source
segments keep their measured-byte reservation until emitted.
The first density probe reserves `node.max_response_bytes`. Later requests
reserve the larger of the response target or 1.5 times the density prediction,
capped at `node.max_response_bytes`, then resize to the measured response after
decode. `source_fetch_max_in_flight_bytes` is therefore an admission watermark
over conservative predictions plus completed reassembly bytes, while
`source_fetch_max_in_flight_requests * node.max_response_bytes` remains the
absolute active-response bound. A larger-than-predicted response can
temporarily take the watermark over its admission limit; no more work is
admitted until retained bytes fall below the limit. Config validation still
requires enough room for the initial maximum-sized probe.

Canonical storage writes tree state only at checkpoints, not at every block.
Missing tree-state source capability does not block canonical construction,
but the wallet tree-state capabilities are unavailable until checkpoint rows
exist.

The native wallet API exposes checkpoint-oriented reads:

- `tree_state_checkpoint_at_or_before(max_height, at_epoch)`
- `latest_tree_state_checkpoint(at_epoch)`

The advertised capabilities are `wallet.read.tree_state_checkpoint_v1` and
`wallet.read.latest_tree_state_checkpoint_v1`.

Lightwalletd compatibility stays explicit: `GetLatestTreeState` returns the
latest checkpoint, and `GetTreeState(BlockID)` returns a tree state only when
the requested height is an exact stored checkpoint. Non-checkpoint heights
return `NOT_FOUND`.

## Consequences

Operators tune budgets in the units that bound throughput and memory, not by
guessing which chain height happens to be dense. Network upgrades can change
density without requiring per-height code patches.

Native clients read latest or checkpoint tree state. The native API does not
serve arbitrary per-height tree state.

The canonical construction vocabulary is `CanonicalPipelineLimits` and
`block_prepare_concurrency`. `materialized_view_replay_*` names are reserved for the async
materialized-view replay plane.

Bulk-catchup observability uses stage labels from this ADR:
`source_fetch`, `canonical_block_prepare`, `canonical_prevout_resolve`, `canonical_position`,
`subtree_root_attachment`, `checkpoint_tree_state`, `commit_reassembly`,
`canonical_commit`, and `canonical_flush`.

## Container-aware default queue caps

The queue-cap defaults now derive from the container memory budget at
startup. `services/zinder-ingest/src/memory_pressure.rs` exposes
`container_memory_budget_bytes()`, which returns `memory.high` (preferred)
or `memory.max` from cgroup v2. `CanonicalPipelineLimits` owns the source and
prepare policy. Each resource-aware bound uses
`container_budget / 64`, clamped to `[128 MiB, original fallback]`. On a
10 GiB container the source and prepare watermarks are both 160 MiB. On a
24 GiB container they are 384 MiB; on dev hosts without cgroup the fallback
constants apply.

The `ZINDER_INGEST__CONSTRUCTION__*_BYTES` env-var overrides still take
precedence over the auto-derived default. They are diagnostic overrides: the
tracked deployment examples omit them so the deployed runtime and the closed
storage-lifecycle certification resolve the same resource profile. The closed
lifecycle command does not accept independent source or prepare tuning flags;
its report validator recomputes the expected profile from the recorded CPU,
memory, and node response limits.
