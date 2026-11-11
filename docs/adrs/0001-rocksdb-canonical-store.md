# ADR-0001: Use RocksDB for Canonical Storage

| Field | Value |
| ----- | ----- |
| Status | Accepted |
| Product | Zinder |
| Domain | Storage and persistence |
| Related | [Storage backend](../architecture/storage-backend.md), [Service boundaries](../architecture/service-boundaries.md) |

## Context

`zinder-ingest` needs a canonical embedded KV store that owns durable chain artifacts: block metadata, compact-block payloads, transaction lookup records, tree state, epoch pointers, and the reorg window.

The workload is not generic. It is append-heavy, range-read-heavy for wallet sync, and reorg-aware at the chain tip. It must support atomic per-`ChainEpoch` commits across multiple logical tables, explicit migrations, crash recovery, and a projected 100-200 GB storage footprint.

Service architecture matters too. `zinder-ingest` is the only canonical writer. Read replicas reach canonical state through `RocksDbCanonicalSecondary` and the `CanonicalReader` contract, not by holding their own write handle. Production recovery, checkpointing, and operational tooling matter more than native multi-process write sharing.

The candidates evaluated were RocksDB through `rust-rocksdb`, `fjall`, `redb`, and LMDB through `heed`.

## Decision

Zinder uses **RocksDB through `rust-rocksdb`** as the canonical KV store for `zinder-store`.

The storage boundary hides RocksDB behind Zinder-owned domain contracts. Public
APIs expose `ChainEpoch`, `BlockHeaderArtifact`, `BlockBlobArtifact`,
`CompactBlockArtifact`, transaction fact/location artifacts, transparent-output
artifacts, and `ReorgWindow`, never RocksDB types. RocksDB is owned by
`zinder-ingest`; other services read through epoch-pinned store readers or an
explicit query-owned store.

## Rationale

RocksDB has the strongest feature set for Zinder's canonical-store workload:

- **Append-heavy ingest:** the LSM design fits height-ordered chain bulk catchup and tip following.
- **Atomic multi-table commits:** `WriteBatch` plus column families maps cleanly to per-`ChainEpoch` commits across block metadata, compact blocks, transaction lookups, tree state, and epoch pointers.
- **Range serving:** iterators, prefix-oriented options, bloom filters, block cache controls, and snapshots support wallet `GetBlockRange` traffic and explorer-derived access.
- **Operational resilience:** checkpoints, backup tooling, compaction controls, WAL configuration, recovery modes, statistics, and a mature tuning surface matter more at 100-200 GB than pure-Rust dependency cleanliness.
- **Production maturity:** RocksDB has the deepest deployment history among the candidates and the richest operational literature.

The cost is developer experience. RocksDB adds a C++ build surface, longer cold-cache builds, larger binaries, and a larger tuning vocabulary. That cost is acceptable because canonical storage is the highest-blast-radius part of the project and the operational feature set materially reduces production risk.

## Consequences

Positive:

- The canonical store starts from the most operationally mature candidate.
- Column families partition artifact families without inventing table multiplexing.
- Backups and snapshots use established RocksDB primitives.
- One store supports wallet-facing range access and block-explorer-derived ingestion without changing engines.

Negative:

- Contributors need a working native toolchain for RocksDB.
- CI caches and monitors the native dependency build.
- Cross-compilation, especially MUSL targets, needs explicit validation.
- RocksDB tuning is part of Zinder's operational surface.
- The storage abstraction must keep RocksDB concepts out of public APIs.

Neutral:

- `fjall` remains a fallback if RocksDB DX cost becomes unacceptable.
- `redb` remains a candidate for small coordination state, not for canonical chain artifacts.
- LMDB remains a useful reference for read-heavy replicas, not for the canonical writer store.

## Switch Criteria

Switching from RocksDB to `fjall` requires all of:

- `fjall` passes the same real-fixture replay and crash-recovery tests.
- `fjall` stays within 2x of RocksDB's disk footprint at the measured fixture size.
- `fjall` matches or beats RocksDB on chain bulk catchup and reorg replacement within the accepted thresholds.
- RocksDB's native build or cross-compilation cost blocks normal contributor or CI workflows.

Switching to a third engine requires a new ADR.

## Alternatives Considered

### `fjall`

Strongest pure-Rust DX challenger and best key-value separation for large values. Rejected because operational maturity, file-format track record, backup story, and production evidence are weaker than RocksDB's.

### `redb`

Clean pure-Rust API, ACID transactions, and chain-indexing evidence through `ord`. Rejected for the canonical store because append-heavy, reorg-replacement, and disk-efficiency evidence is weaker than RocksDB and `fjall`. Viable for small coordination state.

### LMDB through `heed`

Excellent for mmap-backed read-heavy access and direct multi-process readers. Rejected because Zinder's service boundary does not rely on multiple processes opening the same database file as primary, and LMDB is less aligned with append-heavy mutable artifact storage and reorg replacement.
