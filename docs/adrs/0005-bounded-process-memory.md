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

1. **One bounded open path.** Every RocksDB open—canonical, wallet, and materialized-view, whether primary, secondary, or a short-lived read-only admission probe—routes through one path in the storage layer. A typed resource budget sets the block cache, the WAL ceiling, open files, write buffers, and a total memtable budget. Configuration validates each value against a minimum. Operators tune the budget, not raw RocksDB options. A read-only admission probe that does not share a caller's block cache allocates the 4 MiB minimum block cache instead of a full writer or reader cache, caps open files with the budget, and serializes table-file opens.
2. **Direct I/O first.** Every production open tries direct reads, and primaries also use direct I/O for flush and compaction. When the filesystem refuses, the open logs the refusal and retries with buffered I/O. There is no operator toggle.
3. **Total memtable memory is bounded** across all column families of a store by one write-buffer manager.
4. **Metadata is charged to the cache.** Every column family, including each store's default family, reads through the bounded block cache, and index and filter blocks count against it. The one exception is the tree-state checkpoint family of the canonical secondary that ingest opens to verify stored checkpoints. It reads through no block cache, so every verification read takes its blocks from the table file and checks their checksums.
5. **One cache per store per process.** A process that keeps several secondaries of one store open shares one block cache of the configured size among them. The canonical and wallet stores keep separate caches. The cache stays non-strict, so one generation's pinned blocks cannot fail another generation's catch-up.
6. **Pressure means non-reclaimable memory.** Throttling of rebuildable work, such as materialized-view replay, uses anonymous memory against the cgroup limit, not working set.
7. **Startup catch-up is bounded, and later lag is not a failure.** A wallet-serving reader waits up to `storage.initial_catchup_timeout_ms` to admit its first serving pair. If it cannot, it exits before it opens its public or ops listeners, so `/readyz` does not exist yet and the supervisor restarts it. After the first pair is published, a reader that falls behind keeps running, and readiness reports the lag.
8. **Service binaries allocate through jemalloc on Linux.** `zinder-runtime` links jemalloc with unprefixed symbols, so RocksDB's C allocations use it too, and every service binary installs it as the Rust global allocator. Operators tune it through `MALLOC_CONF`. Release builds fix jemalloc's page size and address width so one binary runs on every supported kernel. Other platforms keep the system allocator.

[Storage backend](../architecture/storage-backend.md) describes the budget, its defaults, and the fixed RocksDB options. [Configuration](../reference/configuration.md) lists the budget keys.

## Consequences

- Store reads cannot fill the cgroup through the page cache on filesystems that support direct I/O. Elsewhere, the same bounded RocksDB caches apply with buffered I/O.
- A serving process's block-cache memory is the configured canonical plus wallet cache size, not that sum times the number of generations. Memtables and table readers remain per generation.
- Ingest's verification secondary keeps the index blocks of its open checkpoint table files in table-reader memory, outside `block_cache_bytes`. The open-file limit bounds that memory.
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

- 2026-11-20. Decision 1 covers short-lived read-only admission probes, using the 4 MiB minimum block cache unless a shared cache is supplied, and materialized-view secondaries validate through their open secondary handle instead of probing the live primary directory. [Storage backend](../architecture/storage-backend.md#resource-bounds).
