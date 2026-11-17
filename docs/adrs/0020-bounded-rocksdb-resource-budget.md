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

Every production RocksDB open first attempts direct I/O:

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

`RocksDbResourceBudget::preferred_io_mode` names the mode the open attempts
first. Every profile and every configuration section leaves it at `direct`.
Only `zinder-bench rocksdb-tip-replay` sets it to `buffered`, which skips the
direct attempt so a benchmark can compare the two read paths on one filesystem.

### Resource Budget

`RocksDbResourceBudget` is the single typed budget for canonical and materialized-view
stores. It carries:

| Knob | Canonical writer | Materialized-view writer | Canonical reader | Materialized-view reader | Effect |
| --- | --- | --- | --- | --- | --- |
| `block_cache_bytes` | 512 MiB | 256 MiB | 128 MiB | 64 MiB | Bounded LRU cache shared by data and index blocks, and by filter blocks in families that configure a filter. A process that keeps several secondaries of one store open shares one cache of this size among them. |
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

The budget also carries `statistics_level`, which selects RocksDB statistics
collection. `off` installs no statistics object. `tickers`, the default,
collects ticker counters only (RocksDB's `kExceptHistogramOrTimers`). `full`
uses RocksDB's default level, which adds histograms and operation timers.
Zinder exports tickers only; histograms appear in RocksDB's periodic `LOG`
statistics dump.

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

### Block Caches Shared Across Secondaries

A block cache belongs to one store in one process. A process that keeps several
secondaries of one store open at the same time opens all of them on one
`SharedBlockCache` of the configured `block_cache_bytes`, so that value bounds
the process instead of each secondary:

- Wallet-serving `zinder-query` and `zinder-compat-lightwalletd` keep up to
  four generations of a canonical and a wallet secondary
  ([ADR-0038](0038-wallet-runtime-composition-and-capability-discovery.md)).
  Each store's generations share one cache, and the two stores keep separate
  caches, so one store's scans cannot evict the other's index and filter
  blocks.
- Compact-blocks `zinder-compat-lightwalletd` opens its two canonical
  generations on one cache.

The canonical-reader default, `min(cgroup limit / 8, 512 MiB)` with a 128 MiB
floor, therefore sizes one cache per process, and the wallet-reader default of
64 MiB is the process total as well.

Sharing works because RocksDB keys a cached block by the identity of the table
file that holds it: the file's `db_session_id` and original file number table
properties, plus the block offset. Every secondary of one primary computes the
same key for the same file, so the generations hold one copy of each block they
all read, and a generation opened or caught up later finds the blocks its
siblings loaded. Different files, and the files of different stores, never
share a key. Store tests pin the sharing for bulk-built, flushed, and compacted
files, so a RocksDB change to the key derivation fails them.

The open path takes the cache from
`RocksDbOpenResources::with_shared_block_cache` and rejects a budget whose
`block_cache_bytes` differs from the cache's capacity. Each instance keeps its
own reference, so the cache lives until the last instance and the owning
publisher drop it, and closing one instance releases only its pins.

These stay per instance, because neither RocksDB nor `rust-rocksdb` can share
them:

- The table cache: open table readers and file descriptors, up to
  `max_open_files` plus the handles that open and catch-up pin to file
  metadata. Table-reader memory is not charged to the block cache.
- The write-buffer manager and the memtables it records, which are not charged
  to the block cache either.
- The statistics object and its tickers.

A serving process exports the cache gauges once per cache and sums the
memtable and table-reader memory of its open instances. It samples that memory
at most once every 15 seconds and off its async tasks, because the table-reader
estimate looks up every table file's reader in the table cache, which costs
time in proportion to the file count and reorders the table cache's LRU list.

A shared cache stays a non-strict LRU. A strict limit fails reads and table
opens with `MemoryLimit` once pinned blocks fill the cache, and in a shared
cache one generation's pinned index and filter blocks would then fail another
generation's catch-up, which opens table files. Pinned usage can therefore
take the cache past its capacity. Index and filter blocks are inserted at high
priority into a pool of half the capacity, and data blocks at low priority, so
data reads cannot evict the metadata that table readers need. The pool does
not rank one generation's blocks above another's. The data blocks of every
generation compete by recency, so a retained or released pair that still
serves reads can evict data blocks the current pair read earlier. That
competition lasts only as long as an older pair serves reads: its retention
window and the read limit that follows
([ADR-0038](0038-wallet-runtime-composition-and-capability-discovery.md)).

Every canonical and wallet data family, and every store's `default` family,
pins RocksDB's `uncache_aggressiveness` at 0. A non-zero value makes a
secondary that drops a file at catch-up erase that file's cached blocks,
pinned ones included, while the other generations still read the file. RocksDB
documents that the default will likely change to about 300. With 0, unused
blocks age out of the LRU.

### Locked RocksDB Invariants

These options are storage-layer contracts and are not operator-configurable:

- WAL stays enabled.
- Point-in-time recovery stays enabled through RocksDB's default recovery mode.
- Atomic cross-CF flush stays enabled on primary stores.
- Ordered writes stay enabled.
- Every column family reads through the instance's bounded block cache,
  including each store's `default` family. The open path gives the `default`
  family the shared table factory, so RocksDB does not build the 32 MiB cache
  it otherwise creates for that family in every instance.
- Index blocks, and filter blocks where a family configures a filter, are
  charged to the bounded block cache through
  `cache_index_and_filter_blocks = true` and
  `pin_l0_filter_and_index_blocks_in_cache = true`.
- The shared `build_block_based_table_factory` sets no filter policy, so the
  canonical and materialized-view stores have no filter blocks. The wallet
  store adds partitioned whole-key hybrid-Ribbon filters, about 10 bits per
  key with a two-level index, to its four hash-keyed families:
  `transparent_unspent_output`, `transparent_spent_output`,
  `transparent_unspent_output_by_address`, and
  `transparent_address_transaction`. Their filter and index partitions are
  charged to the same bounded cache.
- Compression is fixed per family: Snappy for the canonical and wallet data
  families, ZSTD for every materialized-view family
  ([ADR-0034](0034-exclusive-index-build-stages-and-block-local-spend-replay.md)),
  and Snappy for each store's `default` family, which holds the canonical and
  wallet control rows. The open path gives the `default` family explicit
  options, so a change in RocksDB's default compression does not change the
  codec of newly written files.
- The write-buffer manager handle is retained for the lifetime of the DB
  handle, alongside the block cache.

Filter blocks stay inside the memory bound, but a filter working set larger
than the block cache turns rejected point reads back into disk reads. When
sizing the wallet `block_cache_bytes`, add the `filter block size` that
`rocksdb.aggregated-table-properties` reports for each filtered family to the
hot index and data blocks. The planner's keys are uniformly distributed
hashes, so a following wallet reads every filter partition over time. Plan for
the whole sum: about 0.9 bytes per row in those families, and 1.25 bytes for
rows still in L0. A rising `rocksdb.block.cache.filter.miss` count in the
wallet store's RocksDB statistics shows that the partitions no longer fit.

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
- A serving process's block-cache term is the configured canonical plus wallet
  cache size, not that sum times the number of open generations. Memtables
  and table readers remain per generation.

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
- **Divide each reader budget among the serving generations.** Rejected. Each
  generation would still cache its own copy of the blocks they all read, the
  current pair would get a quarter of the configured cache, and a generation
  that closes would lose its share.
- **One cache for the canonical and wallet stores.** Rejected. One store's
  scans would evict the other's index and filter blocks, and the two configured
  budgets would lose their meaning.

## Store-size-independent memory invariant

RocksDB-owned memory is bounded by `RocksDbResourceBudget`, store
reads prefer direct I/O with buffered fallback, total memtable memory is bounded
by `WriteBufferManager`, and memory pressure is based on non-reclaimable
anonymous memory.

## Revision history

- 2026-11-17: A process that keeps several secondaries of one store open
  shares one block cache of the configured size among them, per store.
  Sharing relies on RocksDB's file-derived cache keys, keeps the cache
  non-strict, and pins `uncache_aggressiveness` at 0 on the canonical and
  wallet data families and the `default` families. The table cache, memtables, and statistics stay per
  instance, and serving processes sample their summed memory at most every 15
  seconds. Each store's `default` family reads through the configured cache
  instead of a 32 MiB cache that RocksDB builds for every instance.
- 2026-11-16: `RocksDbResourceBudget::preferred_io_mode` lets the benchmark
  harness open a store buffered without a direct attempt. No configuration
  section exposes it, and production opens are unchanged.
- 2026-11-15: The shared table factory sets no filter policy, so the canonical
  and materialized-view stores have no filter blocks. The wallet store's four
  hash-keyed families carry partitioned hybrid-Ribbon whole-key filters charged
  to the bounded cache, and wallet cache sizing counts their filter working
  set.
- 2026-11-16: RocksDB 11.12 through `rust-rocksdb` 0.54. RocksDB 11.5 made LZ4
  the default compression, so the `default` family now pins Snappy. The
  `tickers` statistics level collects tickers only; earlier bindings mapped it
  to RocksDB's `kExceptTimers` level, which also recorded size histograms such
  as `rocksdb.bytes.per.write`.
