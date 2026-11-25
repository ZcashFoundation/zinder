# Bulk-catchup resource tuning

Use this runbook when canonical or wallet construction runs out of memory,
stalls, or is slower than the host should allow. It shows how to find the
resource that limits construction and which setting to change.

Ingest sizes its canonical construction budgets from the container's memory
limit. Wallet construction uses fixed default budgets. Most deployments need
no tuning. Change a setting only after a metric shows which resource is
short. To see the values a binary resolved, run it with `--print-config`. In
a runtime image, run the binary directly in the container, as
[Inspect a runtime container](deploying-on-a-vm.md#inspect-a-runtime-container)
shows.

## Find the limiting resource

Look at the process exit and the metrics just before it. Ingest exports its
metrics on the ops endpoint at `/metrics`.

| Resource | Signs |
| --- | --- |
| Memory | Exit code 137 or a cgroup out-of-memory event from the platform. `zinder_ingest_memory_pressure_ratio`, the non-reclaimable memory divided by the container limit, approaches 1 |
| Node RPC | `zinder_ingest_source_request_duration_seconds` is high, `zinder_ingest_source_retry_total` rises, and `zinder_ingest_bulk_pipeline_head_of_line_wait_seconds` shows construction waiting for the next segment from the node |
| CPU | The process uses all its CPUs while `zinder_ingest_source_fetch_queue_bytes` stays near its limit, so fetched blocks wait for preparation |
| Disk IO | Host disk utilization is saturated. After a canonical bulk load commits, `zinder_store_rocksdb_property{store_role="canonical_primary"}` reports a rising `rocksdb.estimate-pending-compaction-bytes`, a non-zero `rocksdb.actual-delayed-write-rate`, or `rocksdb.is-write-stopped` equal to 1 |

Change one setting at a time. Compare throughput and peak memory on the same
chain range before and after.

## Settings

Construction settings for ingest live under `[ingest.construction]`, and each
has a `ZINDER_INGEST__CONSTRUCTION__*` environment variable. Wallet
construction settings live under `[projector.build]`. Every key and its
environment variable are listed in
[Configuration](../reference/configuration.md).

| Symptom | Setting | Default | Effect of the change |
| --- | --- | --- | --- |
| Ingest runs out of memory | `block_prepare_concurrency` | `min(available CPUs, 16)` | Lower it first. Fewer blocks are prepared at once, so less memory is held |
| Ingest still runs out of memory | `block_prepare_memory_watermark_bytes` | container memory / 64, clamped to 128 MiB to 512 MiB | Lower it. Less prepared block data waits for its store write |
| Ingest runs out of memory with a full source queue | `source_fetch_max_in_flight_bytes` | the larger of `node.max_response_bytes` and container memory / 64 clamped to 128 MiB to 384 MiB | Lower it. Fewer node responses are held in memory. It cannot go below `node.max_response_bytes`, the largest across all endpoints when fallbacks are configured |
| The projector runs out of memory | `projector.build.max_outpoint_sort_memory_bytes` and `projector.build.max_secondary_sort_memory_bytes_per_sorter` | 4 GiB and 1 GiB | Lower them. Sorters spill to disk sooner, so the build needs more free disk |
| CPUs are idle while the projector builds and the `*_drain` or `cold_validation_family_readback` stages of `zinder_wallet_projection_build_stage_duration_seconds` dominate | `projector.build.accumulator_hashing_workers` | `min(available logical CPUs, 4)`, or 1 when the CPU count cannot be detected | Raise it toward the idle CPUs, or lower it to leave CPUs to ingest or a node on the same host. The build hashes wallet rows on that many threads, cold validation reads the primary output and address-transaction families in that many key ranges, and the published wallet does not change. It cannot shorten a build that has no idle CPU |
| The container limit is too small for any setting | `ZINDER_INGEST_MEMORY_LIMIT`, `ZINDER_PROJECTOR_MEMORY_LIMIT` (Compose) | 14g and 10g on Mainnet | Raise it. The ingest byte budgets above grow with it unless you set them explicitly. The projector's sort budgets stay fixed, so raise them yourself if the projector should use the extra memory |
| The node answers slowly or times out | `source_fetch_max_in_flight_requests` | 12 | Lower it to reduce load on the node. Raise it only when the node has spare capacity |
| The node rejects large responses | `source_segment_target_response_bytes` | `min(node.max_response_bytes, 32 MiB)`, using the smallest `max_response_bytes` across all endpoints when fallbacks are configured | Keep it below the node's own limit, which is 50 MiB by default on Zebra 7.1.0 and Zakura 1.8.6. It cannot exceed the largest `max_response_bytes` across all endpoints |
| CPUs are idle and the node keeps up | `block_prepare_concurrency` | `min(available CPUs, 16)` | Raise it together with `block_prepare_memory_watermark_bytes`. More slots without more bytes only add contention |
| CPUs are idle and the `tree_hashing_*` stages of `zinder_ingest_canonical_block_load_stage_duration_seconds` dominate block load | `tree_hashing_workers` | `min(available logical CPUs, 8)` when construction stores checkpoints beyond the cadence and build tip, otherwise 1. It is also 1 when the CPU count cannot be detected | Raise it toward the CPUs that preparation leaves idle, or lower it to leave CPUs to preparation. The loader hashes commitment trees on that many threads and stores the same rows. It cannot shorten a build that has no idle CPU |
| RocksDB compaction falls behind and the host has spare CPU and disk | `storage.canonical.rocksdb.max_background_jobs` or `wallet.rocksdb.max_background_jobs` | 2 | Raise it. More flush and compaction work runs in parallel |

`source_segment_max_blocks` caps the blocks in one node request. It defaults
to 64, and startup rejects values above 1000, the largest JSON-RPC batch
Zakura accepts. `node.max_response_bytes` (64 MiB by default) is the largest
response Zinder accepts, not a request size.

Startup rejects a configuration in which `source_fetch_max_in_flight_bytes` is
smaller than one node response, or `source_segment_target_response_bytes` is
larger than the largest `max_response_bytes` across all endpoints.

### First sync on fewer than 8 logical CPUs

Wallet-serving ingest stores a tree-state checkpoint at every height during a
fresh sync by default. On Mainnet that costs about 40% more writer CPU than a
sparse sync. The measured every-block build, with 6 hashing workers on 8
physical cores and one thread per core, reached READY within the three-hour
budget. A host with fewer logical CPUs takes longer, and ingest logs
`every_block_construction_below_qualified_cpus` when it starts such a build.
The warning counts logical CPUs, so a host with 4 physical cores and 8
hardware threads gets none, even though it is likely slower than the measured
host. Ingest does not change the policy on its own; the operator chooses.

The smallest writer measured to meet the three-hour budget has 4 cores. The
table gives fresh Mainnet construction to READY with the derived limits and
no overrides. The every-block and budget times are measured with the writer
on dedicated cores and the node on other cores, so a whole machine of that
size that also runs the node and projector takes longer. Sparse times are
estimates from the measured writer CPU and the core-count scaling of a
2,000-block Mainnet slice.

| Writer cores | Every block, the default | `commitment-budget` at 256 | Sparse (estimate) |
| ---: | --- | --- | --- |
| 4 | 2 h 34 min | 2 h 9 min | About 2.2 to 3.1 hours |
| 2 | 4 h 28 min, over the budget | 3 h 41 min, over the budget | About 3.4 to 5.3 hours |

To reach READY sooner on a small host, choose one of these before the first
sync. Both keep an existing store's checkpoints. Changing the selection also
changes what the background pass adds and restarts its coverage scan.

- Set `ingest.tree_state_densification.selection = "commitment-budget"`.
  Ingest then stores a checkpoint each time `commitment_budget` note
  commitments (1,024 by default) have accumulated, so a read between
  checkpoints replays fewer commitments than the budget. With a budget of 256,
  writer CPU was within 7% of a sparse sync, and hashing still runs on up to 8
  workers. Under wallet-serving coverage, setting only this selection or only
  `commitment_budget` now applies the budget before READY. It used to build a
  sparse store and leave the budget to the background pass.
- Set `ingest.tree_state_densification.during_construction = false` for a
  sparse store. Reads then replay up to 99 blocks until the background pass
  fills the history after READY, which takes an estimated 4 to 5 CPU-hours on
  Mainnet.

## RocksDB budgets

`[storage.canonical.rocksdb]` sets the canonical store's block cache, WAL
ceiling, open files, write buffers, memtable budget, and background jobs.
`[wallet.rocksdb]` sets the same for the wallet store. Each role has bounded
defaults, which [Storage backend](../architecture/storage-backend.md)
describes.

Raising caches or memtables to hide an undersized container moves the failure
point instead of removing it. Raise the container limit instead.

## Allocator

Tune the jemalloc allocator with `MALLOC_CONF`, for example
`background_thread:true`; see
[Service operations](../architecture/service-operations.md#memory-allocator).

## Recover after resource exhaustion

1. Stop the restart loop.
2. Change the container limit or the setting for the limiting resource.
3. Start the same runtime against the same storage paths.

Unpublished construction work is discarded on restart, so the build starts
over. A published store reopens and replays its WAL within the configured
budget. Do not delete WAL or SST files by hand.

Resource tuning does not fix `SchemaMismatch`, `ArtifactCorrupt`, or a network
mismatch at startup. For those, build into fresh, empty storage paths as in
[Initial sync](initial-sync.md), and keep the rejected store until the new one
is ready.

The design behind these bounds is in
[ADR-0005](../adrs/0005-bounded-process-memory.md) and
[ADR-0009](../adrs/0009-ingest-lifecycle-and-construction.md).
