# Bulk-catchup resource tuning

Use this runbook when canonical or wallet construction hits OOM, stalls, or runs slower than host hardware allows.

Both `zinder-ingest` and `zinder-projector` scale default construction budgets from the detected container cgroup memory limit and CPU count, so most deployments need no overrides. Inspect resolved values with `zinder config print` or `--print-config` ([Inspect a runtime container](deploying-on-a-vm.md#inspect-a-runtime-container)) and tune only the bottleneck shown in `/metrics`.

## Find the limiting resource

| Resource | Signals (`/metrics` and exit state) |
| --- | --- |
| **Memory** | Exit code `137` / cgroup OOM; `zinder_ingest_memory_pressure_ratio` approaches `1`. |
| **Node RPC** | High `zinder_ingest_source_request_duration_seconds`, rising `zinder_ingest_source_retry_total`, high `zinder_ingest_bulk_pipeline_head_of_line_wait_seconds`. |
| **CPU** | CPUs saturated while `zinder_ingest_source_fetch_queue_bytes` stays near its limit (blocks waiting for preparation). |
| **Disk I/O** | Saturated disk; `zinder_store_rocksdb_property{store_role="canonical_primary"}` shows rising `rocksdb.estimate-pending-compaction-bytes`, nonzero `rocksdb.actual-delayed-write-rate`, or `rocksdb.is-write-stopped == 1`. |

Change one setting at a time and compare throughput and peak memory over the same chain range.

## Construction settings

Ingest construction keys live under `[ingest.construction]` (`ZINDER_INGEST__CONSTRUCTION__*`), and projector build keys live under `[projector.build]` (`ZINDER_PROJECTOR__BUILD__*`). See [Configuration](../reference/configuration.md) for all variables.

| Symptom | Setting | Default | Effect |
| --- | --- | --- | --- |
| Ingest OOM | `block_prepare_concurrency` | `min(CPUs, 16)` | Lower first to reduce concurrent in-flight block preparation. |
| Ingest still OOM | `block_prepare_memory_watermark_bytes` | `clamp(container_mem / 64, 128 MiB, 512 MiB)` | Lower to bound prepared blocks waiting for write. |
| Ingest OOM with full fetch queue | `source_fetch_max_in_flight_bytes` | `max(node.max_response_bytes, clamp(container_mem / 64, 128 MiB, 384 MiB))` | Lower to hold fewer fetched node responses (must be ≥ largest `node.max_response_bytes` across fallbacks). |
| Projector OOM | `projector.build.max_outpoint_sort_memory_bytes` and `projector.build.max_secondary_sort_memory_bytes_per_sorter` | Cgroup-scaled: `40%` (`64 MiB..=4 GiB`) and `10%` (`32 MiB..=1 GiB`); `4 GiB` / `1 GiB` without a cgroup limit | Lower together so sorters spill sooner. After outpoint drain frees its buffer, address-transaction sort recycles up to half of `max_outpoint_sort_memory_bytes` (10 GiB defaults peak at ~3 GiB). Lowering also clamps concurrent key-prefix drain shards below a 256 MiB per-shard sort floor within `MAX_MERGE_FAN_IN`. |
| Idle CPUs during wallet build (`canonical_scan`, `*_drain`, or `cold_validation_family_readback` dominate) | `projector.build.accumulator_hashing_workers` | `min(logical CPUs, 4)` (1 if unknown) | Raise toward idle CPUs (up to 64; sharded key-prefix drains cap at 8 shards within the 256 MiB per-shard sort floor), or lower to reserve CPU for ingest/node. |
| Container ceiling too small for defaults | `ZINDER_INDEX_MEMORY_LIMIT`, `ZINDER_SERVE_MEMORY_LIMIT` | `20g` / `8g` (Mainnet Compose; single-volume `8g`+) | Raise ceiling; cgroup-scaled construction budgets scale up automatically (projector defaults cap at `4 GiB` / `1 GiB` / `512 MiB`). |
| Slow node or timeouts | `source_fetch_max_in_flight_requests` | `12` | Lower to reduce node pressure; raise only with spare node headroom. |
| Node rejects large batches | `source_segment_target_response_bytes` | `min(node.max_response_bytes, 32 MiB)` (smallest across fallbacks) | Keep below node limit (50 MiB default on Zebra 7.1.0 and Zakura 1.8.6). |
| Idle CPUs and fast node | `block_prepare_concurrency` | `min(CPUs, 16)` | Raise together with `block_prepare_memory_watermark_bytes`. |
| Idle CPUs and `tree_hashing_*` dominates `zinder_ingest_canonical_block_load_stage_duration_seconds` | `tree_hashing_workers` | `min(logical CPUs, 8)` for dense construction, else `1` | Raise toward idle CPUs or lower to leave headroom for preparation. |
| RocksDB compaction lags with spare CPU/IOPS | `storage.canonical.rocksdb.max_background_jobs` or `wallet.rocksdb.max_background_jobs` | `2` | Raise to parallelize flushes and compactions. |

`source_segment_max_blocks` defaults to `64` and rejects values above `1000` (Zakura's max JSON-RPC batch size).

<a id="first-sync-on-fewer-than-8-logical-cpus"></a>

### First sync on fewer than 8 logical CPUs

Wallet-serving ingest defaults to `every-block` tree-state checkpoint densification during fresh construction (`~40%` more writer CPU than sparse sync, meeting the 3-hour budget on 8 cores). On fewer than 8 logical CPUs, ingest logs `every_block_construction_below_qualified_cpus`.

Measured Mainnet fresh canonical construction to `READY` on dedicated writer cores (node on separate cores; a whole small VM sharing node and projector takes longer):

| Writer cores | `every-block` (default) | `commitment-budget` at `256` | Sparse (estimate) |
| ---: | --- | --- | --- |
| `4` | 2 h 34 min | 2 h 9 min | ~2.2–3.1 hours |
| `2` | 4 h 28 min *(over 3 h budget)* | 3 h 41 min *(over 3 h budget)* | ~3.4–5.3 hours |

To speed up initial construction on a smaller host (set before first sync):

- **`commitment-budget` cadence:** Set `ingest.tree_state_densification.selection = "commitment-budget"` and `ingest.tree_state_densification.commitment_budget = 256` (default `1024`; setting only `commitment_budget` selects `commitment-budget` automatically). Ingest writes a checkpoint whenever that many note commitments accumulate across pools, keeping `tree_hashing_workers` active while cutting writer CPU to within ~7% of sparse sync. After `READY`, the background pass (`workers = 2`, `max_blocks_per_second = 500`) maintains settled stride coverage.
- **Sparse construction:** Set `ingest.tree_state_densification.during_construction = false` to write checkpoints every 100 blocks and at the build tip, leaving densification to the background pass after `READY` (~4–5 CPU-hours on Mainnet).

## RocksDB, allocator, and recovery

- **RocksDB budgets:** `[storage.canonical.rocksdb]` and `[wallet.rocksdb]` defaults are documented in [Storage backend](../architecture/storage-backend.md). Raise the container memory ceiling rather than shrinking caches to mask an undersized container.
- **Allocator:** Linux binaries use `jemalloc`, tuned via `MALLOC_CONF` (for example `background_thread:true`; `MALLOC_ARENA_MAX` has no effect). See [Memory allocator](../architecture/service-operations.md#memory-allocator).
- **Recover after OOM:** Stop the restart loop, raise the container limit or lower the limiting construction setting, and restart against the same paths (unpublished construction staging restarts automatically; published stores replay WAL). Do not delete WAL or SST files by hand. Resource tuning does not fix `SchemaMismatch` or `ArtifactCorrupt`; rebuild into empty paths per [Initial sync](initial-sync.md).

See [ADR-0005](../adrs/0005-bounded-process-memory.md) and [ADR-0009](../adrs/0009-ingest-lifecycle-and-construction.md) for the bounding design.
