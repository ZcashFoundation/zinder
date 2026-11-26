# ADR-0005: Bound process memory independently of store size

Status: Accepted

## Context

Zinder's stores grow with chain history. Its processes run in containers with a memory limit, so resident memory must not grow with the size of the stores on disk.

Several mechanisms break that rule by default:

- Long sequential reads fill the host page cache when RocksDB uses buffered I/O. In a cgroup, that page cache counts against the container's memory limit.
- Per-column-family write buffers do not bound total memtable memory, because the total grows with the number of column families.
- A memory-pressure signal that includes active file cache throttles background work while the kernel could still reclaim that memory.
- A serving process keeps several secondary generations of a store open. A cache per generation multiplies memory by the generation count and caches the same blocks several times.

Allocation is the other half. Explorer view replay reads canonical data on up to 16 threads, and every read allocates in RocksDB and in Zinder. Glibc malloc gives each arena one lock, and containers often cap the arena count with `MALLOC_ARENA_MAX`. On a Mainnet deployment with a cap of two, replay advanced 16 to 37 blocks per minute and ingest spent about four times as much CPU in the kernel as in user space. A local reproduction advanced 1,816 blocks per minute with the cap and 39,835 without it. Zinder does not control that setting.

## Decision

1. **One bounded open path.** Canonical, wallet, and materialized-view stores use a common open path. A typed resource budget sets the block cache, WAL ceiling, open files, write buffers, and total memtable budget. Configuration validates each setting against a minimum. A read-only admission probe uses the caller's shared block cache or a private 4 MiB cache. It applies the open-file budget and opens table files on one thread.
2. **Direct I/O first.** Every production open tries direct reads, and primaries also use direct I/O for flush and compaction. If the direct open fails, the process logs the RocksDB error and retries once with buffered I/O. There is no operator toggle.
3. **One write-buffer manager per open database.** Each instance shares its manager across column families. On a primary, approaching the memtable budget triggers flushes. The wallet primary also caps its live WAL at 32 MiB so flushes bound the WAL backlog that retained secondary generations replay into memtables. A secondary replays the primary's WAL and can exceed its budget until it catches up with the primary's flush and releases the old read view.
4. **Metadata is charged to the cache.** Column families, including each store's default family, charge index and filter blocks to the bounded block cache. Canonical `block_hash_index` and `transaction_location` use partitioned hybrid Ribbon filters and two-level indexes whose small top-level index and filter blocks stay pinned in the block cache while partition leaf blocks remain evictable. Ingest's tree-state verification secondary bypasses that cache for the checkpoint and block-header families. Each read from their table files checks the data block's checksum again.
5. **One cache per store per process.** A process that keeps several secondaries of one store open shares one block cache of the configured size among them. The canonical and wallet stores keep separate caches. The cache stays non-strict, so one generation's pinned blocks cannot fail another generation's catch-up.
6. **Pressure means non-reclaimable memory.** Throttling of rebuildable work, such as materialized-view replay, uses anonymous memory against the cgroup limit, not working set.
7. **Container-scaled fresh wallet build budgets.** The projector derives its default external sort and reorg-undo ceilings from the detected cgroup v2 memory limit (40% outpoint sort clamped to 64 MiB..=4 GiB, 10% per secondary sorter clamped to 32 MiB..=1 GiB, and 5% reorg undo clamped to 16 MiB..=512 MiB; keeping 4 GiB, 1 GiB, and 512 MiB without a container limit). Parallel key-prefix drain shards are clamped by a 256 MiB per-shard sort-memory floor and active `MAX_MERGE_FAN_IN` headroom, and total pipelined SST writer queue memory across all concurrently open writers in a phase is capped at 16 MiB.
8. **Startup catch-up is bounded, and later lag is not a failure.** A wallet-serving reader waits up to `storage.initial_catchup_timeout_ms` to admit its first serving pair. If it cannot, it exits before it opens its public or ops listeners, so `/readyz` does not exist yet and the supervisor restarts it. After the first pair is published, a reader that falls behind keeps running, and readiness reports the lag.
9. **Service binaries allocate through jemalloc on Linux.** `zinder-runtime` links jemalloc with unprefixed symbols, so RocksDB's C allocations use it too, and every service binary installs it as the Rust global allocator. Operators tune it through `MALLOC_CONF`. Release builds fix jemalloc's page size and address width so one binary runs on every supported kernel. Other platforms keep the system allocator.

[Storage backend](../architecture/storage-backend.md) describes the budget, its defaults, and the fixed RocksDB options. [Configuration](../reference/configuration.md) lists the budget keys.

## Consequences

- Store reads cannot fill the cgroup through the page cache on filesystems that support direct I/O. Elsewhere, the same bounded RocksDB caches apply with buffered I/O.
- A serving process's block-cache memory is the configured canonical plus wallet cache size, not that sum times the number of generations. Memtables and table readers remain per generation. Because data-family index and filter blocks charge to the shared block cache, each cached `BlockBasedTable` reader holds only ~1.2–2.3 KiB in `zinder_store_table_reader_memory_bytes` (~287 KiB for Mainnet's 243 canonical SSTs; in the 243-block canonical secondary integration harness, raising `max_open_files` from 128 to 1,024–2,048 across 4 table-cache shards moves `zinder_store_table_reader_memory_bytes` from 277,371 B to 579,888 B, a +302,517 B / ~295 KiB delta, bounded below ~5 MiB per generation even if all 2,048 slots fill).
- Ingest's verification secondary keeps the index blocks of its open checkpoint and block-header table files in table-reader memory, outside `block_cache_bytes`. The open-file limit bounds that memory.
- With jemalloc and the arena cap still set, the local reproduction advanced 44,333 blocks per minute instead of 1,816, and its peak resident memory was 8 to 11 percent higher than glibc's with the cap.
- Glibc malloc variables such as `MALLOC_ARENA_MAX` have no effect on a Linux service binary, and glibc's memory reports no longer describe it.
- Building a service binary needs a C compiler and `make`. AArch64 builds use 64 KiB jemalloc pages, which can raise resident memory on 4 KiB-page hosts.

## Alternatives rejected

- **A direct-I/O toggle.** Automatic detection with buffered fallback keeps the same invariant, and a toggle adds a way to break it.
- **Only block-cache and open-file caps.** They bound RocksDB's own memory but not the page cache that buffered reads fill.
- **Exiting when a published reader falls behind.** After the first pair, replica lag is a readiness condition, not a liveness failure. Only the first admission has a deadline, because a reader with no pair has nothing to serve.
- **Waiting without a deadline for the first pair.** A reader that can never converge would stay silent, with no listener to report why.
- **Dividing a reader budget among generations.** Each generation would cache its own copy of the same blocks, and the current pair would get a fraction of the configured cache.
- **One cache for the canonical and wallet stores.** One store's scans would evict the other's index and filter blocks, and the two budgets would lose their meaning.
- **Verifying checkpoints through the shared cache with `fill_cache` off.** That flag only stops a read from inserting blocks. RocksDB still returns a block another read cached, so a verification read could pass a block whose table file has been damaged since.
- **Removing the arena cap from deployments.** Replay recovers, but throughput would still depend on a variable that operators set for unrelated reasons.
- **Fewer read threads.** It gives up the overlap of storage waits on network-attached volumes.
- **mimalloc.** It also avoids shared arena locks, but it was not measured. `tikv-jemallocator` is maintained by a project that runs RocksDB on it.

## Revision history

- 2026-11-20. Decisions 2 and 3 specify the buffered retry and distinguish primary flush thresholds from secondary WAL replay.
- 2026-11-20. Decision 1 applies the cache and open-file budgets to short-lived admission probes. Materialized-view secondaries validate the manifest through their own handle. [Storage backend](../architecture/storage-backend.md#resource-bounds).
- 2026-11-20. Decision 4 extends uncached verification reads to canonical block headers.
- 2026-11-25. Decision 3 caps the wallet primary live WAL at 32 MiB to bound WAL replay across retained secondary generations, [Storage backend](../architecture/storage-backend.md#resource-bounds) separates wallet defaults from materialized-view defaults (raising the wallet reader open-file cap to 1,024), and Decision 4 accounts for canonical point-lookup partitioned filters with pinned top-level index and filter blocks in the non-strict block cache.
- 2026-11-25. Decision 7 scales projector external-sort and reorg-undo defaults from the detected container memory limit, enforces a 256 MiB per-shard sort-memory floor and active `MAX_MERGE_FAN_IN` cap on key-prefix drain shards, and bounds cumulative SST writer pipeline queues at 16 MiB across open writers.
- 2026-11-26. Secondary and read-only probe opens use 4 table-cache shards (`table_cache_numshardbits = 2`), and [Storage backend](../architecture/storage-backend.md#resource-bounds) raises the canonical reader open-file default from 128–256 (128 without a container limit) to 1,024–2,048 scaled with the block cache (2,048 without a container limit; +302,517 B measured `zinder_store_table_reader_memory_bytes` delta across 243+ canonical SSTs).
