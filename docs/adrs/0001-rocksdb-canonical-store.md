# ADR-0001: Store chain data in RocksDB

Status: Accepted

## Context

`zinder-ingest` keeps the durable copy of the chain that every other Zinder runtime reads: block headers, compact blocks, transaction locations, tree-state checkpoints, chain epochs, and retained chain events. The storage engine has to fit that workload:

- Writes are append-heavy and ordered by height, both during a first sync and while following the tip.
- Wallet sync reads long ranges of consecutive heights.
- A reorg replaces a short suffix of recent blocks.
- One block commit changes several logical tables at once. The commit must land atomically and survive a crash.
- A Mainnet deployment holds hundreds of gigabytes, so backup, checkpoint, compaction, and recovery tooling matter in production.

Each store has exactly one writer process, and readers run in other processes on the same host ([ADR-0003](0003-epoch-bound-reads-and-storage-roles.md)). Zinder therefore does not need several processes to write one database file. It does need an engine with a long production record.

The candidates were RocksDB through `rust-rocksdb`, `fjall`, `redb`, and LMDB through `heed`.

## Decision

1. Zinder stores its canonical chain data in RocksDB through `rust-rocksdb`. The wallet store and the materialized-view stores use the same engine.
2. Column families separate artifact families. One write batch commits everything a new chain epoch changes, so a crash leaves either the previous epoch or the new one. Canonical schema v1 binds the initial activation schedule in the same batch as READY. Fixed control fields carry the admitted schedule revision and digest and the effective reorg window, so admission can validate all three together.
3. RocksDB stays inside the storage crates. Runtimes and public APIs see chain epochs, typed artifacts, and typed errors, never RocksDB handles, options, or errors.
4. Store checkpoints use RocksDB's own checkpoint primitive. A wallet-serving backup binds a canonical and a wallet checkpoint together ([ADR-0004](0004-storage-and-deployment-topologies.md)). [Private backups](../runbooks/private-backups.md) describes capture and restore of the pair.

5. Canonical schema v1 keeps the serialized transaction digest in the transaction location index so readers can retrieve it with a point lookup. RocksDB uses fast compression for frequently accessed data, denser compression for cold history, and uncompressed hash indexes.

[Storage backend](../architecture/storage-backend.md) describes the stores, their column families, and their layout.

## Consequences

- Building Zinder needs a C++ toolchain. Cold builds take longer, and binaries are larger than with a pure-Rust engine.
- CI caches the native build. Each new release target needs its own native build validation.
- RocksDB tuning is part of Zinder's operational surface. Zinder keeps it small by exposing a curated resource budget instead of raw options ([ADR-0005](0005-bounded-process-memory.md)).
- Every new store API must be domain-shaped, so that RocksDB concepts do not leak into callers.

## Alternatives rejected

- **`fjall`.** It is the strongest pure-Rust challenger and separates large values well. Its file-format track record, backup story, and production evidence are weaker than RocksDB's. Zinder reconsiders it only if all of these hold: it passes the same real-fixture replay and crash-recovery tests, stays within twice RocksDB's disk footprint, matches RocksDB on bulk catch-up and reorg replacement, and the RocksDB native build blocks normal contributor or CI work.
- **`redb`.** It has a clean API and ACID transactions. Its evidence for append-heavy writes, reorg replacement, and disk efficiency is weaker than RocksDB's and `fjall`'s. It remains a candidate for small coordination state, not for chain data.
- **LMDB through `heed`.** It excels at memory-mapped, read-heavy access from several processes. Zinder does not need several processes to open one database as primary, and LMDB fits append-heavy, frequently replaced data less well.

## Revision history

- 2026-11-24. Added canonical transaction digests and per-family SST compression to decision 5.

- 2026-11-24. Decision 2 binds the initial activation schedule to READY in canonical schema v1.

- 2026-11-20. Updated the checkpoint decision to reference private recovery of the canonical and wallet pair.
