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
canonical hot path, even though the tree state at any height can be derived
from an earlier checkpoint and the stored compact blocks.

## Decision

Fresh canonical construction is a resource-budgeted staged pipeline:

```text
source fetch of bounded connected segments
  -> parallel canonical block preparation
  -> ordered commitment-tree positioning and family load into the inactive builder
  -> exact subtree-root load
  -> validation and baseline publication
```

The source boundary is `SourceChainSegmentLimits { cursor,
max_connected_blocks, target_response_bytes, max_response_bytes }`.
`NodeSource::fetch_chain_segment` fetches raw block bytes only. Tree state is a
separate source call, `fetch_chain_checkpoint(height)` or
`fetch_chain_checkpoint_for_block(block_id)`, and the JSON-RPC adapter rejects
a tree-state response whose height or hash does not match the request.

Fresh canonical construction consumes one `CanonicalPipelineLimits` value.
The runtime resolves it from the container memory budget, logical CPU count,
and `node.max_response_bytes`. With `[node_fallbacks.N]` endpoints, startup can
make any endpoint active, so the formulas below use the largest
`max_response_bytes` in the ordered node list for the response limit and the
source watermark, and the smallest for the response target:

- `source_segment_max_blocks = 64`, and validation rejects values above 1000,
  the largest JSON-RPC batch a Zakura node accepts
- `source_segment_target_response_bytes = min(node.max_response_bytes, 33554432)`
- `source_fetch_max_in_flight_requests = 12`
- `source_fetch_max_in_flight_bytes = max(node.max_response_bytes, clamp(memory / 64, 128 MiB, 384 MiB))`
- `block_prepare_concurrency = min(available_parallelism, 16)`
- `block_prepare_memory_watermark_bytes = clamp(memory / 64, 128 MiB, 512 MiB)`

The segment sizer uses observed response bytes per block, p95 density, overshoot
memory after split attempts, and network-upgrade resets. The JSON-RPC response
default is 64 MiB, so the default segment target is 32 MiB. Source fetch may
complete out of order, but blocks leave source reassembly in height order.
Block preparation parses each block once and derives block-local canonical
facts without historical prevout or wallet reads. Preparation runs in parallel
and yields results in height order, so commitment-tree positioning and the
builder load stay serial. Each block reserves a conservative estimate derived
from its raw size against `block_prepare_memory_watermark_bytes` before parsing
and keeps that reservation until its ordered store write completes. A block
whose estimate exceeds the whole watermark reserves the entire watermark, so
outstanding reservations never exceed it. This is admission control over
estimates, not an allocator cap. Completed out-of-order source segments keep
their measured-byte reservation until emitted.
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

Construction writes a tree-state checkpoint every 100 blocks and at its build
tip, not at every block. It requires the node's tree-state capability, because
it checks its build tip against the node's tree state. Live following writes a
checkpoint for every block it appends.

The native wallet API serves tree state at any retained height:

- `TreeStateAtHeight` (`wallet.read.tree_state_at_height_v2`) returns the
  stored checkpoint at the height, or reconstructs it from the nearest
  checkpoint below and the stored compact blocks in between.
- `LatestTreeStateCheckpoint` (`wallet.read.latest_tree_state_checkpoint_v2`)
  returns the newest checkpoint at or below the visible tip.

Neither read contacts the node. Lightwalletd compatibility maps
`GetTreeState(BlockID)` onto `TreeStateAtHeight` and `GetLatestTreeState` onto
`LatestTreeStateCheckpoint`; `GetTreeState` refuses a height above the visible
tip with `INVALID_ARGUMENT`, as lightwalletd does.

## Consequences

Operators tune budgets in the units that bound throughput and memory, not by
guessing which chain height happens to be dense. Network upgrades can change
density without requiring per-height code patches.

Native clients read the tree state at any retained height. A height between
checkpoints costs a replay of at most 99 stored compact blocks, which each
query bounds to one concurrent reconstruction per available CPU.

The canonical construction vocabulary is `CanonicalPipelineLimits` and
`block_prepare_concurrency`. `materialized_view_replay_*` names are reserved for the async
materialized-view replay plane.

Construction observability labels source-lane queue, reorder-buffer, and
watermark metrics with `stage="source_fetch"`. Per-block construction work is
reported by `zinder_ingest_canonical_block_construction_stage_duration_seconds`
with the `block_parse`, `identity_validation`, `compact_artifacts`,
`transaction_facts`, `block_header_artifact`, `block_blob`, and `block_replay`
stages.

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

## Revision history

- 2026-11-18: With fallback node endpoints, the response limit and source
  watermark use the largest `max_response_bytes` in the ordered node list, and
  the response target the smallest.
- 2026-11-17: Tree state at a height between checkpoints is reconstructed
  from the nearest checkpoint below and the stored compact blocks, so
  `TreeStateAtHeight` and `GetTreeState` serve every retained height without
  the node. The checkpoint cadence is unchanged.
