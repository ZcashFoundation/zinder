# Storage backend

This page explains Zinder's three RocksDB stores: what each holds, who writes
it, how readers get a consistent view, and how memory stays bounded. It is for
contributors and for operators who size or recover a deployment.

Each store has one writer process, which opens it as the RocksDB primary.
Every other process opens it read-only as a RocksDB secondary on the same
filesystem. Stores never upgrade themselves in place: a store whose identity
or layout does not match the binary is refused, and you rebuild it.
[ADR-0004](../adrs/0004-storage-and-deployment-topologies.md) records why
Zinder deploys on one host.
[ADR-0003](../adrs/0003-epoch-bound-reads-and-storage-roles.md) records the
epoch-bound read and secondary model.

## Stores at a glance

| Store | Writer | Location | Holds | Rebuildable from |
| --- | --- | --- | --- | --- |
| Canonical | `zinder-ingest` | `storage.path` | Chain truth and chain event history | The node |
| Wallet | `zinder-projector` | `wallet.path` | Transparent address indexes | The canonical store |
| Materialized views | `zinder-ingest`, when `ingest.explorer_views = true` | `materialized-views` under `storage.path` | Explorer aggregates | The canonical store |

The canonical and wallet paths must be separate directories. Each reader also
needs its own secondary metadata path. For the canonical store, that is
`storage.secondary_path` in `zinder-query`, `zinder-compat-lightwalletd`, and
`zinder-explorer`. The projector uses `storage.canonical_secondary_path` and
finds the canonical store at `storage.canonical_path`. Wallet-serving readers
use `wallet.secondary_path` for the wallet store. The
[configuration reference](../reference/configuration.md) lists every key.

New RocksDB SSTs use block-based table format 7, the bundled engine's current
default. The explicit setting keeps SST output at format 7 if a future engine
changes its default.

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
| `daily_value_pool_balance` | Reserved for daily value-pool evidence. Explorer construction refuses READY publication pending authentication. |
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

Construction and every transition check a block's compact block against its
header before they write either row. A compact-block range read therefore
reads only `compact_block` and does not read `block_header`. The read first
checks every requested height against retained history. A height below the
first retained block or above the visible tip fails the read as an unavailable
artifact, even when a row is stored for it.

The read then fetches rows with batched point lookups (`MultiGet`):

- An unbounded range read fetches up to 1,024 heights per batch.
- The tree-state replay fetches up to 16 heights per batch and checks for cancellation after each block. Wallet queries at the same height and chain fence share a reconstruction. The last wallet to leave cancels the work. Each query runtime caches up to 256 settled answers, scoped to the epoch, sequence digest, and retained history.
- A byte-bounded read fetches one row first. Each later batch holds as many
  rows as the budget left fits at the size of the largest row read so far,
  from 1 to 16.

Each row must decode to the block at its key's height. A height missing inside
retained history fails the read as store damage.

The read never builds an iterator. `RocksDB` opens every level-0 table file of
a family to build an iterator, including files that hold no requested height.
A point lookup checks each level-0 file's key range before it opens the file,
unless level 0 is the only non-empty level and holds at most three files. A
point lookup can still open a file that does not hold its key. That happens
when the file's key range covers the key, or when `RocksDB` skips the range
check. On a secondary that the primary's compaction has overtaken, a range
read fails with the missing-table-file error only when one of its lookups
opens a deleted file. The `RocksDB` reader reports that error as an
invalidated reader, and `zinder-query` answers it by replacing the pair, as
[ADR-0015](../adrs/0015-wallet-runtime-composition.md) records.

## Wallet store

The wallet store holds transparent address data. Shielded data, such as
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

The row digest and the UTXO set commitment are sums of per-row hashes, so they
do not depend on the order rows are hashed in. Construction hashes rows on
`projector.build.accumulator_hashing_workers` threads, in batches, while the
build thread drains its sorters and writes SST files. Each batch is allocated
once with room for 256 KiB of keys, values, and scripts, 4,096 rows, and 2,048
unspent outputs, and is handed to a worker before an entry would overflow it.
A batch takes 512 KiB on 64-bit targets and less on 32-bit ones, whose row and
output descriptors hold smaller offsets. A row or script larger than a batch is hashed on the build
thread instead. At most twice the worker count of batches, never more than 64,
wait for or run on the workers or sit hashed in their replies, and the build
thread holds two more. With W workers, hashing therefore holds at most
(min(2W, 64) + 2) × 512 KiB of batches: at most 5 MiB at 4 workers and 33 MiB
at 64.
Each completed batch also carries two 2 KiB partial accumulators, and each
worker builds one row's hash input at a time, at most 256 KiB plus 45 bytes.
None of this grows with the number of rows, as
[ADR-0005](../adrs/0005-bounded-process-memory.md) requires. Cold validation reads the unspent and spent output families back in up
to the same number of outpoint key ranges, at most one per 65,536 of their
rows, each on its own thread, and checks every row as one scan would. The
setting is at most 64; a build that asks for more is refused before it creates
any wallet state. A thread that cannot start fails the build like any other
error, so its lease is released. Any worker count publishes the same rows and
evidence.

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
- checks the store's identity and schema again after each catch-up, including
  the manifest version and digest in the canonical READY evidence;
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
in-flight requests. A slot is reused only once no pair and no storage read in
flight holds its secondaries, because a read on the blocking pool can outlive
the request that started it. While no slot is free, the reader does not publish, and
its next publication jumps to the writer's latest chain event. How long a
replaced pair stays readable is in
[Wallet data plane](wallet-data-plane.md#how-reads-stay-consistent).

A lightwalletd `GetBlockRange` or `GetBlockRangeNullifiers` stream uses one
published view at a time while it reads its next chunk. The stream ends if a
chunk does not link by block hash to the previous one. In compact-blocks and
light-wallet modes, the publisher alternates between two canonical
secondaries. See [Block range methods](../reference/lightwalletd-compatibility.md#getblockrange-and-getblockrangenullifiers).

All generations in one process share one block cache per store. The configured
`block_cache_bytes` therefore bounds the process, not each generation. Each
generation keeps its own table cache and memtables.

Each secondary replays the primary's write-ahead log into its own memtables.
It can release those memtables after catch-up sees that the primary has flushed
their contents and any reads holding the old view finish. A secondary cannot
flush them itself. Its memory can therefore exceed `memtable_budget_bytes`.
For each store role, the reader reports the sum of the budgets and the sum of
usage across its open generations.

## Read interfaces

`zinder-query` and `zinder-compat-lightwalletd` read stored data only through
two asynchronous interfaces in `zinder-query`: `CanonicalReader` for canonical
data and `WalletProjectionReader` for the wallet projection. Each reader holds
one admitted fence and never moves it. Its accessors return what it admitted,
such as the network, the network-upgrade schedule, the event fence, the chain
epoch, or the wallet READY evidence, without reading storage. Every other
method reads storage.

The methods answer the questions the serving runtimes ask, so a request makes
one read where it needs several lookups:

- `mined_transaction` returns a transaction's location, its block's header,
  and its bytes, or proves the transaction absent.
- `block_header_by_hash` and `compact_block_by_hash` resolve a block hash
  through the best-chain index and read the block it names. A resolved hash
  whose compact block the store lacks fails `compact_block_by_hash` as that
  block's unavailable artifact, so it never reads as an unknown hash.
- `total_address_balance` sums up to 256 addresses.
- `outpoint_spend_statuses` classifies a batch of outpoints. On checkpointed
  history a missing outpoint fails the request, so there the read ends at the
  first one and reads none after it.

No signature names a storage engine, and an engine builds every value the
interfaces carry from what its writer stored, with constructors such as
`CanonicalEventFence::new`, `CanonicalActivationSchedule::first_revision`, and
`TreeStateReplay::new`. A tree-state rebuild built with `TreeStateReplay::new`
proves nothing about its seed, so a reader rebuilds it from the seed it reads
itself. Only a rebuild that a `RocksDB` lookup issued, on a store showing the
same visible chain, is replayed from the seed it names.

`RocksDbCanonicalReader` and `RocksDbWalletReader` implement the interfaces
over admitted secondaries. They run each read on the blocking pool inside the
caller's tracing span, because a direct-I/O cache miss is a synchronous
`pread`. A caller that already holds a blocking thread drives a read with
`read_on_blocking_thread`, and the reader then reads on that thread. Opening,
catching up, and retiring secondaries stays engine-specific: the
wallet-serving pair publisher and the two-generation compact-blocks refresh
own it. A compact-blocks range chunk therefore reads through
`CanonicalReader` on a blocking thread that captures the current generation
itself, drives the read there, and releases the generation before encoding
the chunk, so a chunk holds a generation only during its storage read.

A storage read can outlive the request that started it. A tree-state rebuild
therefore runs in a task of its own that holds its rebuild slot until the
rebuild ends, even when its request is cancelled.

A read that returns no value fails with `StorageReadError`. `QueryError`
converts each variant without changing the public outcome. A storage failure
becomes `QueryError::CanonicalStore` or `QueryError::WalletStore`, after the
interface that failed, so the `error_class` of the `zinder_query_request_*`
metrics stays `canonical_store` or `wallet_store`:

| `StorageReadError` | Meaning | Reason on the wire |
| --- | --- | --- |
| `ArtifactUnavailable` | The read named an artifact the reader does not hold, such as a height outside retained history | `ARTIFACT_UNAVAILABLE` |
| `CanonicalHistoryUnavailable` | A lookup missed in a store whose history starts after a checkpoint, so absence is unproven | `CANONICAL_HISTORY_UNAVAILABLE` |
| `ChainEventCursorExpired` | A chain-event cursor points before retained events | `CHAIN_EVENT_CURSOR_EXPIRED` |
| `ChainEventCursorInvalid` | A chain-event cursor is malformed or names another stream family | `CHAIN_EVENT_CURSOR_INVALID` |
| `ReaderInvalidated` | The reader's view of storage was invalidated under it; a reader admitted afresh can answer | `STORAGE_UNAVAILABLE` |
| `ReadTaskFailed` | The task running the read panicked or was shut down | `BLOCKING_TASK_FAILED` |
| `Unavailable` | Storage failed to answer | `STORAGE_UNAVAILABLE` |

A conformance suite in `services/zinder-query/tests/integration/read_conformance/`
defines how every implementation answers, including bounded pages, range
ends, proven absence, a reader that keeps its fence while the writer moves,
and reorg replacement. It is written only against the interfaces and a
harness that stores a chain the testkit describes. An engine runs it by
implementing the harness and instantiating the canonical and wallet cases,
which run separately. The `RocksDB` harness runs both in the default test
tiers, and a canonical reader that holds its chain in memory runs the
canonical cases, which shows the canonical interface can be implemented
without `RocksDB`.

## Resource bounds

The store budget sets the block cache, write-ahead log (WAL), open files,
write buffers, and background jobs. Primaries and secondaries apply the
configured write-buffer size and count to every column family, including
`default`. Read-only admission probes keep RocksDB's write-buffer defaults for
non-default families. The memtable budget applies to each open instance.
[ADR-0005](../adrs/0005-bounded-process-memory.md#decision) describes primary
flushes and secondary WAL replay.

| Role | Block cache | WAL | Open files | Memtable budget |
| --- | --- | --- | --- | --- |
| Canonical writer | 512 MiB | 256 MiB | 512 | 256 MiB |
| Canonical reader | 1/8 of the container memory limit, from 128 to 512 MiB | 32 MiB | 128 to 256, scaled with the block cache | 16 MiB |
| Wallet and materialized-view writer | 256 MiB | 256 MiB | 512 | 512 MiB |
| Wallet and materialized-view reader | 64 MiB | 16 MiB | 64 | 16 MiB |
| Read-only admission probe | 4 MiB (or caller's shared cache) | — | 32 (or caller's budget) | — |

Read-only admission probes open existing stores through the same bounded open
path with single-threaded table-file opens, index and filter blocks charged to
the probe cache, and direct reads with buffered fallback.

You can override any field in `[storage.canonical.rocksdb]`,
`[storage.materialized_views.rocksdb]`, or `[wallet.rocksdb]`. The
[bulk catch-up tuning runbook](../runbooks/bulk-catchup-resource-tuning.md)
explains when to change them. Store memory metrics carry a `store_role` label;
the [metrics reference](../reference/metrics.md) lists them.

## Schema admission

Every open validates the store before it returns a handle. Existing canonical and materialized-view stores use a bounded
read-only probe before a primary opens for writes. Their secondaries validate
through the bounded secondary handle. The open fails without changing anything when any of these
differ from what the process expects:

- the store identity or layout version;
- the network;
- the required retained data and its historical floor for a canonical reader;
- the exact canonical workload and raw-blob retention for a writer or recovery checkpoint;
- the network-upgrade activations at or below the visible tip;
- the reorg window;
- for the wallet store, the canonical source it was built from.

Schema v7 requires every family listed above plus the default control
family, including physically present families with no rows under the
admitted workload and blob policy. The construction manifest must bind the
same workload, blob policy, and build plan as the control record and have
evidence for the full family inventory. At construction, families outside
the stored set and families reserved for post-publication writes must report
zero rows.

`CanonicalStoredSet` derives retained block data and its history floor from
the build plan and workload. Readers can specify minimum data and a starting
height with `CanonicalStoreRequirements`. A richer store satisfies these
requirements when it covers the requested history. Missing data or an
insufficient history floor returns `CanonicalStoreError::MissingStoredData`.
The native query and compatibility listeners keep their first admitted
stored set across generation changes. A running signed-history publisher
also checks that set when it reopens its secondary. Restart the process to
admit a different stored set that meets its requirements.

An admitted stored set also carries authenticated subtree-root coverage from
the construction manifest. A reader can require a complete subtree-root
prefix from index zero in every pool. A retained-range load meets this
requirement when its predecessor has no completed roots. Otherwise, a
complete-prefix load is required. Insufficient coverage returns
`CanonicalStoreError::IncompleteSubtreeRootPrefix`. Light-wallet serving
requires this prefix and block history from height one at admission. Native
wallet query and full-wallet compatibility serve retained-range stores and
report an unavailable historical root on the affected request. Pool activation still
governs final roots, and event retention still governs event streams. Serving capabilities also depend on
the installed handlers and their admitted dependencies.

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
because independently timed copies can describe different chain states.
A private recovery archive binds every payload file to that bundle.
[Private backups](../runbooks/private-backups.md) describes how to verify and restore it.

## PostgreSQL benchmark arm

`zinder-bench` includes a PostgreSQL implementation of the canonical replay
storage benchmark, to compare it with RocksDB on the same captured corpus. It
is benchmark code; Zinder stores its data in RocksDB. The
[storage benchmark environment](../../deploy/storage-benchmark.md) runs it.
