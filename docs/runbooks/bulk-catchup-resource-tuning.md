# Bulk-catchup resource tuning

This runbook covers current memory, I/O, and concurrency controls for canonical
construction. Zinder bounds RocksDB resources and construction queues by
default; tune them only against observed pressure from the target deployment.
The resolved output from `zinder-ingest --print-config` is the authoritative
configuration for a running binary.

## Diagnose the constrained resource

Start with the process exit and the metrics immediately before it:

- Exit code 137 or a cgroup OOM event means the process exceeded the container
  memory boundary. Confirm with the platform event stream rather than inferring
  from a missing application error.
- `zinder_ingest_bulk_pipeline_queue_bytes{stage="source_fetch"}` and
  `zinder_ingest_bulk_pipeline_reorder_buffer_bytes{stage="source_fetch"}`
  identify in-flight source responses and out-of-order segment backlog.
- `zinder_store_block_cache_usage_bytes`, `zinder_store_wal_bytes`, and
  `zinder_store_rocksdb_property` separate cache pressure, WAL growth,
  memtables, compaction debt, and write stalls.

Do not tune all limits together. Record one pressured stage, change its owning
limit, and compare throughput and peak memory on the same chain range.

## Construction controls

The `[ingest.construction]` section owns canonical construction concurrency and
queue admission:

- `source_segment_max_blocks` is a hard request-shape ceiling. Startup
  rejects values above 1000, the largest JSON-RPC batch Zakura accepts.
- `source_segment_target_response_bytes` is the adaptive source-response target
  and must not exceed `node.max_response_bytes`. Keep it below the node's own
  response limit, which is 50 MiB by default on Zebra 7.0.0 and Zakura 1.8.6.
  The 64 MiB `node.max_response_bytes` default is a ceiling on what Zinder
  accepts, not a request size.
- `source_fetch_max_in_flight_requests` and
  `source_fetch_max_in_flight_bytes` bound active source requests and completed
  response backlog.
- `block_prepare_concurrency` bounds CPU preparation slots, while
  `block_prepare_memory_watermark_bytes` admits each block's estimated resident
  data until its ordered store write completes.

The byte watermarks use container-aware defaults when a cgroup v2 memory limit
is available. Explicit TOML or `ZINDER_INGEST__CONSTRUCTION__*` overrides win,
but they are diagnostic controls rather than required production settings. Keep
the source watermark at least as large as `node.max_response_bytes`; startup
rejects a configuration that cannot admit its first source request.

For memory pressure, first lower `block_prepare_concurrency`, then lower the
source or prepare watermark that metrics identify. Raising concurrency without raising the
corresponding byte budget increases contention and does not create additional
admission capacity.

## RocksDB budgets

`[storage.canonical.rocksdb]` configures canonical storage, while
`[wallet.rocksdb]` configures the wallet projection. Both merge role-specific
overrides onto bounded writer or reader defaults. The shared
`RocksDbResourceBudget` controls block cache, WAL ceiling, open files, write
buffers, aggregate memtable memory, and background jobs. Reader defaults are
smaller; canonical reader cache size is also container-aware.

Preserve these invariants:

- WAL stays enabled for crash recovery.
- point-in-time WAL recovery fails closed on corruption;
- writes remain ordered;
- cross-column-family flushes remain atomic;
- secondaries do not become compaction or flush owners.

Raise `max_background_jobs` only when pending compaction bytes or write-stall
metrics prove that compaction is the bottleneck and the host has spare CPU and
I/O. Raising caches or memtables to hide an undersized container merely moves
the failure boundary.

## Recover after resource exhaustion

Stop the restart loop, correct the container limit or the pressured stage's
configuration, and restart the same owner against the same storage paths. The
bounded RocksDB open path replays the WAL within the configured resource
envelope. Do not delete individual WAL or SST files.

If startup reports `SchemaMismatch`, `ArtifactCorrupt`, or a network mismatch,
resource tuning is not the remedy. Follow [Initial sync](initial-sync.md) and
create a fresh, empty storage path. Preserve the rejected path until the
replacement is ready.

## References

- [ADR-0020: Bounded RocksDB resource budget](../adrs/0020-bounded-rocksdb-resource-budget.md)
- [ADR-0022: Resource-budgeted bulk catchup](../adrs/0022-resource-budgeted-bulk-catchup.md)
- [Initial sync](initial-sync.md)
- [Storage backend](../architecture/storage-backend.md)
