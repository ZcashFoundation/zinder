# ADR-0020: Store-size-independent RocksDB memory

Status: Accepted
Date: 2026-05-20
Related: [ADR-0001](0001-rocksdb-canonical-store.md),
[ADR-0003](0003-canonical-storage-access-boundary.md),
[ADR-0015](0015-phase-driven-ingest.md),
[ADR-0022](0022-resource-budgeted-bulk-catchup.md)

## Context

Zinder's canonical and materialized-view stores are allowed to grow with chain history, but
their resident memory must not grow with on-disk store size. The storage layer
serves long sequential reads during startup catchup, secondary replay, and
wallet-facing scans. With buffered filesystem I/O, those reads can populate the
host page cache with large portions of the RocksDB store. In cgroup-enforced
deployments, that reclaimable page cache counts against the container memory
limit even though it is not Zinder heap or RocksDB-owned cache.

The operational invariant is therefore stricter than "set a smaller block
cache": resident memory owned by Zinder and RocksDB must be bounded by the
configured budget, and on-disk store size must not determine process RSS or the
memory-pressure signal that throttles rebuildable work.

Before this ADR revision, ADR-0020 focused on WAL growth and per-column-family
write buffers. Those controls are still required, but they are incomplete:

1. Buffered reads can fill the cgroup through the OS page cache.
2. Per-column-family write buffers do not bound total memtable memory across
   many column families.
3. A pressure signal based on `memory.current - inactive_file` still includes
   active file cache, which is reclaimable.
4. Startup catchup must not block a reader process indefinitely before
   `/readyz` can report replica lag.

## Decision

Every RocksDB user in the workspace routes through one bounded open path in
`zinder-store`: the canonical store (`RocksDbCanonicalStore` and
`RocksDbCanonicalSecondary`), the wallet store, and the materialized-view store
(`MaterializedViewStore`). The helper owns the block cache, write-buffer manager, direct
I/O resolution, and open retry policy.

### Direct I/O With Buffered Fallback

Every RocksDB open first attempts direct I/O:

- Primary and secondary opens set `use_direct_reads = true`.
- Primary opens also set `use_direct_io_for_flush_and_compaction = true` and
  `compaction_readahead_size = 2 MiB`.

If the direct-I/O open fails, Zinder logs
`event="rocksdb_direct_io_unsupported"` with the store path, role, and error,
then retries the same bounded open under buffered I/O. This keeps the default
path store-size-independent on filesystems that support direct I/O while
remaining portable on filesystems and development platforms that do not.

Successful opens log `event="rocksdb_io_mode"` with the resolved mode
(`direct` or `buffered`), store path, and role. The resolved mode is retained
beside the RocksDB handle for observability.

### Resource Budget

`RocksDbResourceBudget` is the single typed budget for canonical and materialized-view
stores. It carries:

| Knob | Canonical writer | Materialized-view writer | Canonical reader | Materialized-view reader | Effect |
| --- | --- | --- | --- | --- | --- |
| `block_cache_bytes` | 512 MiB | 256 MiB | 128 MiB | 64 MiB | Bounded LRU cache shared by data, index, and bloom blocks. |
| `max_wal_bytes` | 256 MiB | 256 MiB | 32 MiB | 16 MiB | Live WAL ceiling. Writer stores flush once the WAL crosses this limit. |
| `max_open_files` | 512 | 512 | 128 | 64 | Open SST handle cap so RocksDB does not pin metadata for every file. |
| `write_buffer_bytes` | 16 MiB | 16 MiB | 8 MiB | 4 MiB | Per-column-family mutable memtable size. |
| `max_write_buffer_count` | 2 | 4 | 2 | 2 | Per-column-family mutable plus immutable memtable count. |
| `max_background_jobs` | 2 | 2 | 2 (not applied) | 2 (not applied) | Primary-writer aggregate flush and compaction job cap owned by RocksDB. |
| `memtable_budget_bytes` | 256 MiB | 512 MiB | 16 MiB | 16 MiB | Total memtable memory budget across column families via `WriteBufferManager`. |

The materialized-view writer deliberately reserves more aggregate memtable headroom than
canonical because one ordered replay dispatch writes many consumer column
families. The shared manager still enforces the 512 MiB hard bound; the larger
envelope prevents hot families from stalling behind constant flush and
compaction turnover.

Local tests use the same bounded path with a smaller profile: 32 MiB block
cache, 16 MiB WAL ceiling, 64 open files, 4 MiB write buffers, two write
buffers, a two-job primary limit, and an 8 MiB total memtable budget.

The validation gate rejects values below the minimums:

- `MIN_BLOCK_CACHE_BYTES = 4 MiB`
- `MIN_MAX_WAL_BYTES = 4 MiB`
- `MIN_MAX_OPEN_FILES = 32`
- `MIN_WRITE_BUFFER_BYTES = 4 MiB`
- `MIN_MAX_WRITE_BUFFER_COUNT = 2`
- `MIN_MAX_BACKGROUND_JOBS = 2`
- `MIN_MEMTABLE_BUDGET_BYTES = 4 MiB`

`max_wal_bytes = 0` remains invalid because it disables RocksDB's WAL-size
flush trigger.

The background-job value is a primary-writer resource limit, not a flush-policy
switch. RocksDB dynamically schedules primary flush and compaction work within
the configured cap. Writer defaults remain at RocksDB's existing two-job
posture; operators can raise a primary store only after the per-column-family
pressure and byte ticker metrics demonstrate a maintenance backlog.
`OpenAsSecondary` disables automatic flushes and compactions, so secondary
profiles retain the field at `2` in the uniform budget type but neither apply it
nor export it as an effective limit.

Secondary stores set RocksDB's `max_file_opening_threads` to `1`. Periodic
secondary catch-up reloads table handlers for each column family; RocksDB's
default of `16` creates and joins a temporary thread group on every reload.
Serial table-handler loading keeps steady-state thread creation independent of
catch-up cadence and column-family count. This is a locked reader posture, not
an operator-tunable resource setting.

### Locked RocksDB Invariants

These options are storage-layer contracts and are not operator-configurable:

- WAL stays enabled.
- Point-in-time recovery stays enabled through RocksDB's default recovery mode.
- Atomic cross-CF flush stays enabled on primary stores.
- Ordered writes stay enabled.
- Index and bloom filter blocks are charged to the bounded block cache through
  `cache_index_and_filter_blocks = true` and
  `pin_l0_filter_and_index_blocks_in_cache = true`.
- The write-buffer manager handle is retained for the lifetime of the DB
  handle, alongside the block cache.

### Memory Pressure

Ingest memory-pressure backpressure uses non-reclaimable memory, not working
set. The materialized-view replay budget computes its pressure ratio from cgroup `anon`
memory divided by `memory.high` when set, otherwise `memory.max`. If cgroup
`anon` is unavailable, it falls back to process `RssAnon` from
`/proc/self/status`.

`working_set_bytes` remains exported as a diagnostic metric, but it does not
drive materialized-view replay throttling because active file cache is reclaimable.

### Startup Catchup

Reader startup performs bounded initial catchup. If a secondary does not
converge within `storage.initial_catchup_timeout_ms` (default 30 seconds), the
reader continues with the opened secondary view and the periodic catchup task
lets readiness report replica lag. Startup does not block indefinitely on a
far-behind secondary and does not crash-loop solely because catchup needs more
time.

## Consequences

- Store-size-independent memory is the storage contract. On supported
  filesystems, direct I/O prevents store reads from filling the cgroup through
  page cache. On unsupported platforms, the same bounded RocksDB-owned caches
  are used with buffered I/O.
- The canonical and materialized-view stores share the same open path, so direct-I/O
  resolution, block-cache setup, WBM setup, and fallback behavior cannot drift.
- Operators tune a curated budget surface instead of raw RocksDB options.
- Rebuildable materialized-view replay backs off only on non-reclaimable memory pressure.
  Reclaimable file cache does not pause indexing.
- Reader processes can start and expose `/readyz` while a secondary catches up.

## Alternatives considered

- **Expose a direct-I/O toggle.** Rejected. The correct operator contract is
  automatic direct-I/O detection with buffered fallback. A toggle would add a
  failure mode without changing the invariant.
- **Use only block-cache and open-file caps.** Rejected. Those bound RocksDB
  metadata and cached blocks, but do not stop buffered reads from filling the
  OS page cache.
- **Use per-column-family write buffers only.** Rejected. The total grows with
  the number of column families. `WriteBufferManager` is the cross-CF bound.
- **Keep using working-set pressure.** Rejected. `memory.current -
  inactive_file` still includes active file cache and can pause materialized-view replay
  while the kernel could reclaim the memory.
- **Fail startup when initial catchup is slow.** Rejected. Replica lag is a
  readiness condition, not a liveness failure.

## Store-size-independent memory invariant

RocksDB-owned memory is bounded by `RocksDbResourceBudget`, store
reads prefer direct I/O with buffered fallback, total memtable memory is bounded
by `WriteBufferManager`, and memory pressure is based on non-reclaimable
anonymous memory.
