# Storage backend

This page explains Zinder's three RocksDB stores: what each holds, who writes
it, how readers get a consistent view, and how memory stays bounded. It is for
contributors and for operators who size or recover a deployment.

Each store has one writer process, which opens it as the RocksDB primary.
Every other process opens it read-only as a RocksDB secondary on the same
filesystem. Stores never upgrade themselves in place: a store whose identity
or layout does not match the binary is refused, and you rebuild it.
[ADR-0004](../adrs/0004-storage-and-deployment-topologies.md) records why the
supported topology is one host. It also records the multi-host PostgreSQL
topology decided for hosted fleets, which is not implemented.
[ADR-0003](../adrs/0003-epoch-bound-reads-and-storage-roles.md) records the
epoch-bound read and secondary model.

## Stores at a glance

| Store | Writer | Location | Holds | Rebuildable from |
| --- | --- | --- | --- | --- |
| Canonical | `zinder-ingest` | `storage.path` | Chain truth and chain event history | The node |
| Wallet | `zinder-projector` | `wallet.path` | Transparent address indexes | The canonical store |
| Materialized views | `zinder-ingest`, only when `ingest.explorer_views = true` | `materialized-views` under `storage.path` | Explorer aggregates | The canonical store |

The canonical and wallet paths must be separate directories. Each reader also
needs its own secondary metadata path. For the canonical store, that is
`storage.secondary_path` in `zinder-query`, `zinder-compat-lightwalletd`, and
`zinder-explorer`. The projector uses `storage.canonical_secondary_path` and
finds the canonical store at `storage.canonical_path`. Wallet-serving readers
use `wallet.secondary_path` for the wallet store. The
[configuration reference](../reference/configuration.md) lists every key.

## Canonical store

`RocksDbCanonicalStore` is the writer's handle, and
`RocksDbCanonicalSecondary` is a reader's handle. The store's identity is
`canonical`, and `CANONICAL_STORE_SCHEMA_VERSION` versions its layout. Callers
use domain operations. Column family names and keys are private to
`zinder-store`.

| Column family | Contents |
| --- | --- |
| `block_replay` | Reversible `CanonicalBlockFacts` envelope per height, the source for every derived record |
| `block_header` | Header by height |
| `block_hash_index` | Height by block hash |
| `transaction_location` | Block position by transaction ID |
| `compact_block` | Compact block per height |
| `tree_state_checkpoint` | Commitment tree checkpoint at least every 100 blocks |
| `subtree_root` | Completed shielded subtree roots |
| `block_final_note_commitment_roots` | Final commitment roots per block (explorer workload only) |
| `daily_value_pool_balance` | Daily value pool snapshot (explorer workload only) |
| `chain_epoch` | Retained chain epochs |
| `chain_event` | Authenticated append and reorg history |
| `mempool_event` | Retained mempool history |
| `displaced_block_facts` | Blocks removed by a reorg |
| `block_blob`, `transaction_blob` | Raw bytes, kept according to `storage.raw_blob_policy` |

A control record ties the store to its network, workload (`wallet` or
`explorer`), raw-blob retention, reorg window, network-upgrade activations,
construction manifest, and current chain event. Admission checks all of them,
as described under [schema admission](#schema-admission).

A fresh store is built in an inactive staging path and published once as a
validated baseline. After that, each new block or reorg is one atomic write
batch. The batch advances the facts, indexes, chain epoch, chain event, and
sequence digest together, so a reader never sees part of a transition.
[Chain ingestion](chain-ingestion.md) describes construction and following.

## Wallet store

The wallet store holds transparent address data only. Shielded data, such as
compact blocks and tree states, stays in the canonical store. The wallet store
is versioned by `WALLET_ROCKSDB_SCHEMA_VERSION` for its layout and
`WALLET_PROJECTION_VALUE_ENCODING_VERSION` for its row values. Its control
record binds it to one canonical source identity and records the chain event it
has reached and a digest of its rows.

| Column family | Key | Filter |
| --- | --- | --- |
| `transparent_unspent_output` | `txid ‖ output_index_be` (36 bytes) | Partitioned hybrid Ribbon |
| `transparent_spent_output` | `txid ‖ output_index_be` (36 bytes) | Partitioned hybrid Ribbon |
| `transparent_unspent_output_by_address` | `address_script_hash ‖ height_be ‖ txid ‖ output_index_be` (72 bytes) | Partitioned hybrid Ribbon |
| `transparent_address_transaction` | `address_script_hash ‖ height_be ‖ tx_index_be` (40 bytes) | Partitioned hybrid Ribbon |
| `transparent_address_balance` | `address_script_hash` (32 bytes) | None |
| `reorg_undo` | `height_be` (4 bytes) | None |

The filters let the projector prove quickly that a new output, spend, or
history row does not exist yet. They use about 10 bits per key, for roughly a
1% false-positive rate. Flushed and level-0 files use Bloom filters, and deeper
files use Ribbon filters, which are about 30% smaller. Filters share the
store's block cache. The canonical and materialized-view stores use no filters.

**Construction.** The projector builds a new wallet store in an inactive path
from a pinned canonical event. Before it publishes the store, it reopens it
cold and reads every row back. The row digest and per-family row counts must
match what construction produced, and the balances must sum to the unspent
total. Otherwise the build is refused.

**Following.** The projector applies each canonical chain event as one atomic
write batch that also advances the wallet's source position. A transition that
would hold more than `projector.follow.max_transition_logical_bytes` is refused
before it writes anything, and the wallet stays unchanged.

**Reorgs.** `reorg_undo` rows cover exactly the unsettled blocks. Each row's
starting digest equals the previous row's resulting digest. The projector
checks the whole chain of undo rows on its first transition after opening the
store, after a failed write, on every rollback, and on every reconciliation.

## Materialized-view store

`MaterializedViewStore` holds explorer view rows, consumer cursors, coverage,
and schema metadata, separate from the canonical and wallet stores.
`MATERIALIZED_VIEW_STORE_FORMAT_VERSION` versions the container, and each
consumer versions its own rows through `MaterializedViewConsumerSchema`.
`zinder-explorer` reads it as a secondary. See the
[materialized-view plane](materialized-view-plane.md).

## Primary and secondary roles

A secondary is a read-only view of a primary that the reader process advances
by catching up. A secondary:

- never writes the primary;
- checks the store's identity and schema again after each catch-up;
- reports a failed catch-up, or lag beyond
  `storage.secondary_replica_lag_threshold_chain_epochs` (default 4), to
  readiness, as listed in the [readiness reference](../reference/readiness.md);
- belongs to one process and is never shared.

### How readers pin a consistent view

`zinder-query` and `zinder-compat-lightwalletd` in wallet mode each keep
canonical and wallet secondaries in pairs. To publish a new pair, the reader
catches both up, then checks that they share the network and reorg window,
that the wallet was built from this canonical store, and that both have reached
the same canonical chain event. Only then do new requests switch to the pair. A
request keeps the pair it started on until it finishes.

The reader rotates through four generation slots. The current pair holds one.
The others hold the next candidate, replaced pairs, or pairs still in use by
in-flight requests. While no slot is free, the reader does not publish, and
its next publication jumps to the writer's latest chain event. How long a
replaced pair stays readable is in
[Wallet data plane](wallet-data-plane.md#how-reads-stay-consistent).

A lightwalletd `GetBlockRange` or `GetBlockRangeNullifiers` stream holds a pair
only while it reads one chunk of at most 1,000 blocks and 2 MiB. The stream
ends if a chunk does not link by block hash to the previous one. In `compat.serving = "compact-blocks"` mode,
the adapter alternates two canonical secondaries and keeps no replaced pair.

All generations in one process share one block cache per store. The configured
`block_cache_bytes` therefore bounds the process, not each generation. Each
generation keeps its own table cache and memtables.

## Resource bounds

Every RocksDB open goes through `RocksDbResourceBudget`, which caps the block
cache, write-ahead log (WAL), open files, memtables, and background jobs. The
store's size on disk does not raise process memory beyond these caps.
[ADR-0005](../adrs/0005-bounded-process-memory.md) records the memory model.

| Role | Block cache | WAL | Open files | Memtable budget |
| --- | --- | --- | --- | --- |
| Canonical writer | 512 MiB | 256 MiB | 512 | 256 MiB |
| Canonical reader | 1/8 of the container memory limit, from 128 to 512 MiB | 32 MiB | 128 to 256, scaled with the block cache | 16 MiB |
| Wallet and materialized-view writer | 256 MiB | 256 MiB | 512 | 512 MiB |
| Wallet and materialized-view reader | 64 MiB | 16 MiB | 64 | 16 MiB |

You can override any field in `[storage.canonical.rocksdb]`,
`[storage.materialized_views.rocksdb]`, or `[wallet.rocksdb]`. The
[bulk catch-up tuning runbook](../runbooks/bulk-catchup-resource-tuning.md)
explains when to change them. Store memory metrics carry a `store_role` label;
the [metrics reference](../reference/metrics.md) lists them.

## Schema admission

Every open validates the store before it returns a handle. The open fails
without changing anything when any of these differ from what the process
expects:

- the store identity or layout version;
- the network;
- the canonical workload or raw-blob retention;
- the network-upgrade activations at or below the visible tip;
- the reorg window;
- for the wallet store, the canonical source it was built from.

Only the canonical writer records a new network-upgrade schedule, and readers
adopt it; see [ADR-0008](../adrs/0008-consensus-parameters-from-the-node.md).

The recorded reorg window can grow but never shrink; see
[Chain ingestion](chain-ingestion.md#reorgs-and-the-reorg-window).

Each format has its own version constant. Compare a version only with earlier
values of the same constant.

When a store fails admission, build a fresh store. A materialized-view store with the wrong format returns
`SchemaMismatch` and is left on disk unchanged.

## Checkpoints

A state bundle binds a canonical and a wallet checkpoint at one chain event,
because independently timed copies are not coherent. No shipped command
restores one; see
[Service operations](service-operations.md#rebuilding-is-the-only-restore).

## PostgreSQL benchmark arm

`zinder-bench` includes a PostgreSQL implementation of the canonical replay
storage benchmark, to compare it with RocksDB on the same captured corpus. It
is benchmark code only. Zinder has no PostgreSQL storage backend. The
[storage benchmark environment](../../deploy/storage-benchmark.md) runs it.
