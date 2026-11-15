# ADR-0044: Service binaries allocate through jemalloc

| Field | Value |
| --- | --- |
| Status | Accepted |
| Product | Zinder |
| Domain | Process memory allocation in every service binary |
| Related | [ADR-0020](0020-bounded-rocksdb-resource-budget.md), [ADR-0034](0034-exclusive-index-build-stages-and-block-local-spend-replay.md), [Service operations](../architecture/service-operations.md#memory-allocator), [Bulk-catchup resource tuning](../runbooks/bulk-catchup-resource-tuning.md#allocator) |

## Context

Explorer view replay and the transparent-address ranking build each resolve
spent outputs through canonical reads on up to 16 threads
([ADR-0034](0034-exclusive-index-build-stages-and-block-local-spend-replay.md)).
Every read allocates. RocksDB allocates block buffers and a copy of the value,
and decoding a replay row allocates each transaction, input, output, and script
it holds.

Glibc malloc serves threads from a set of arenas, and each arena has one lock.
Container deployments often cap the arena count with `MALLOC_ARENA_MAX` to limit
resident memory. With a cap of two, up to 32 read threads from two walkers take
turns on two locks.

A Mainnet deployment running with `MALLOC_ARENA_MAX=2` measured this:

- Replay near height 361,000 advanced 16 to 37 blocks per minute.
- `zinder-ingest` used 4.1 to 4.6 times as much CPU in the kernel as in user
  space.
- Each read worker blocked about 29,000 times per second, while the store
  volume served only about 580 reads per second.

A local reproduction replayed a transparent-dense fixture shaped like that
history. With the cap it advanced 1,816 blocks per minute, and without it
39,835. A profile of the capped run put 81% of samples in `futex` calls from
glibc's internal lock wait and wake.

Zinder does not control the arena cap. It is a deployment setting, and lowering
glibc's arena count is common advice for containers, so replay throughput must
not depend on it.

## Decision

On Linux, `zinder-runtime` links jemalloc 5.3 through `tikv-jemallocator`,
built with unprefixed symbols. jemalloc therefore provides `malloc`, `free`, and
the other C allocation functions for every process that links `zinder-runtime`,
so RocksDB's native allocations use it too. Every Zinder service binary also
installs `zinder_runtime::RuntimeAllocator` as its global allocator, which sends
Rust allocations to jemalloc's sized allocation functions instead of through
the C `malloc` and `free`. Other platforms keep the system allocator.

jemalloc fixes its page size when it is built and aborts on a kernel whose page
size is larger. The release Dockerfile builds AArch64 binaries with 64 KiB
jemalloc pages, so they run on AArch64 kernels with 4, 16, or 64 KiB pages, as
the Armv8-A baseline of
[ADR-0036](0036-gnu-linux-binary-release-bundles.md) requires. x86-64 binaries use
4 KiB pages, the only size x86-64 Linux uses.

jemalloc gives each thread its own cache and spreads threads across many
arenas, so concurrent readers do not queue on a shared lock. By default, a
process that keeps allocating returns freed pages to the kernel within about
ten seconds.

Operators tune jemalloc through `MALLOC_CONF`, for example
`MALLOC_CONF=background_thread:true` to purge freed pages on a background
thread. Glibc malloc variables, such as `MALLOC_ARENA_MAX`,
`MALLOC_TRIM_THRESHOLD_`, and the `glibc.malloc.*` tunables, have no effect on a
Linux service binary.

Each service crate tests that its binary prints jemalloc statistics when run
with `MALLOC_CONF=stats_print:true`. jemalloc reads that variable only when it
owns the C allocation functions, so the test shows that RocksDB's allocations
use jemalloc in that binary. It does not detect a missing global allocator
declaration, because linking `zinder-runtime` alone gives jemalloc the C
allocation functions. The release workflows also run each archived binary with
that setting and check the page size jemalloc reports for its architecture.

## Consequences

- On the local reproduction with `MALLOC_ARENA_MAX=2` set, replay advanced
  44,333 blocks per minute instead of 1,816. With a second walker beside it and
  a 16 MiB block cache, it advanced 37,376 blocks per minute instead of 1,478.
  Without the cap, jemalloc matched or exceeded glibc's throughput.
- Peak resident memory in those runs stayed close to glibc's. With one walker
  it was 563 MiB, against 520 MiB with the two-arena cap and 544 MiB without
  it. With two walkers it was 388 MiB, against 347 MiB and 418 MiB. The cap
  kept peak memory 8% to 11% below jemalloc's at the cost of a 24- to 25-fold
  slowdown.
- Building a service binary needs a C compiler and `make` for jemalloc. The
  Rust image in the release Dockerfile provides both. A stripped binary grows
  by about 0.5 MB and gains no shared-library dependency.
- Glibc's own memory reports, such as `malloc_stats`, no longer describe a
  service binary. jemalloc's statistics do.
- Every Linux binary that links `zinder-runtime`, including test binaries,
  examples, and benchmarks, runs its C allocations on jemalloc, even without a
  global allocator declaration.
- AArch64 binaries manage jemalloc memory in 64 KiB pages, which can raise
  resident memory on 4 KiB-page hosts compared with a 4 KiB build.

## Alternatives considered

- **Remove the arena cap from deployments.** Rejected as the fix. Replay
  recovers with glibc's default arena limit, but throughput would still depend
  on a variable that operators set for unrelated memory reasons.
- **Allocate less in the read path.** Not sufficient alone. Fewer allocations
  per decoded row help under any allocator, but RocksDB still allocates on every
  read, so a capped glibc would still serialize the readers.
- **Run fewer read threads.** Rejected. It gives up the overlap of storage
  waits that ADR-0034 added for network-attached volumes.
- **mimalloc.** Not chosen. It also avoids shared arena locks but was not
  measured here. `tikv-jemallocator` is maintained by the TiKV project, which
  runs RocksDB on it, and it can replace the C allocator on Linux.
