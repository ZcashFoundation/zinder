# Storage backend

This page explains Zinder's three RocksDB stores: what each holds, who writes
it, how readers get a consistent view, and how memory stays bounded. It is for
contributors and for operators who size or recover a deployment.

Each store has one writer process, which opens it as the RocksDB primary.
Every other process opens it read-only as a RocksDB secondary on the same
filesystem. Stores never upgrade themselves in place: a store whose identity
or layout does not match the binary is refused, and you rebuild it.
[ADR-0004](../adrs/0004-storage-and-deployment-topologies.md) records why
Zinder deploys on one host with `rocksdb`, and defines the decided self-contained
`postgres` canonical (`canonical_settled_segment`), active mempool (`mempool_active_entry`),
and optional R2 settled-segment cache boundaries ahead of M3.
[ADR-0003](../adrs/0003-epoch-bound-reads-and-storage-roles.md) records the
epoch-bound read and secondary model.

## Stores at a glance

| Store | Writer | Location | Holds | Rebuildable from |
| --- | --- | --- | --- | --- |
| Canonical | `zinder-ingest` | `storage.path` | Chain truth and chain event history | The node |
| Wallet | `zinder-projector` | `wallet.path` | Transparent address indexes | The canonical store |
| Materialized views | `zinder-ingest`, when `ingest.explorer_views = true` | `materialized-views` under `storage.path` | Explorer aggregates | The canonical store |

The canonical and wallet paths must be separate directories. When running through the unified `zinder` CLI (`zinder serve` or `zinder index`), setting `storage.data_dir` (`ZINDER_STORAGE__DATA_DIR`, defaulting to `/var/lib/zinder`) automatically derives isolated subdirectories (`canonical`, `canonical-secondary/<role>`, `wallet`, `wallet-secondary/<role>`, `checkpoints`, and `materialized-views` under `canonical`). When configuring standalone binaries explicitly, each reader needs its own secondary metadata path: `storage.secondary_path` in `zinder-query`, `zinder-compat-lightwalletd`, and `zinder-explorer`, `storage.canonical_secondary_path` and `storage.canonical_path` in the projector, and `wallet.secondary_path` for wallet-serving readers. The [configuration reference](../reference/configuration.md) lists every key.

New RocksDB SSTs use block-based table format 7, the bundled engine's current
default. The explicit setting keeps SST output at format 7 if a future engine
changes its default.

## Canonical store

`RocksDbCanonicalStore` is the writer's handle, and
`RocksDbCanonicalSecondary` is a reader's handle. The store's identity is
`canonical`, and `CANONICAL_STORE_SCHEMA_VERSION = 2` versions its layout
(`Canonical Schema v2`). Callers use domain operations; column family names,
keys, and external segment formats are internal to `zinder-store`.

Schema v2 separates compact hot indexes and unsealed recent blocks (in RocksDB)
from immutable settled block payloads stored in external aligned segment files
under `<store_root>/segments/`:

| Column family | Contents |
| --- | --- |
| `default` | Store control (`store_control`: identity, schema `v2`, network, activation binding, retention, reorg window, manifest binding, READY evidence, cursor auth key) |
| `block_header` | Fixed `184`-byte header (including `32`-byte `commitment_bytes`) keyed by height BE |
| `block_hash_index` | `4`-byte height BE keyed by `32`-byte block hash |
| `transaction_location` | `72`-byte location and serialized-transaction SHA-256 digest keyed by `32`-byte txid |
| `live_tail_block` | Versioned `ZSLVTL01` (`V1`) record for unsealed heights `> sealed_tip`: pre-encoded `DEFAULT_RANGE` shielded wire payload, `PoolPresenceMask`, optional Tag-1/Tag-2 supplement, and optional `CanonicalBlockFacts` |
| `live_tail_raw_block` | Split versioned `ZSLVRB01` (`V1`) full block bytes and per-tx slice descriptors/SHA-256 digests for unsealed heights `> sealed_tip` when `storage.raw_blob_policy = "all"` (`0` rows when `none`) |
| `settled_segment_index` | `SettledSegmentIndexEntry` (`V1`) keyed by segment start height BE, pinning module flags (`supplement`, `raw_bodies`), external `ZSHRNG02`/`ZSTSUP01`/`ZSBLKB01` digests (`sha256`, `index_sha256`, optional `fs_verity_sha256`), and cumulative `end_sequence_checkpoint` |
| `tree_state_checkpoint` | Commitment tree frontier checkpoints (`V1`) stored at least every `100` blocks (`TREE_STATE_CHECKPOINT_STRIDE`) |
| `subtree_root` | Completed shielded subtree roots per pool (`Sapling`, `Orchard`, `Ironwood`) |
| `chain_epoch` | Retained chain epochs |
| `chain_event` | Authenticated append and reorg history |
| `mempool_event` | Retained mempool history |
| `displaced_block_facts` | Full `CanonicalBlockFacts` archived for blocks replaced by a reorg |

At `READY` publication, fresh construction stages SSTs for the seven bulk
families (`block_hash_index`, `block_header`, `live_tail_block`,
`live_tail_raw_block`, `settled_segment_index`, `transaction_location`,
`tree_state_checkpoint`), verifies `canonical-construction-manifest.v2.json`
(`CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION = 2`), and verifies zero
initial rows across the four post-publication families (`chain_epoch`,
`chain_event`, `mempool_event`, `displaced_block_facts`).

Canonical Schema v2 retains the `72`-byte value for each `32`-byte transaction
ID in `transaction_location`: `4`-byte big-endian block height, `32`-byte block
hash, `4`-byte big-endian transaction index, and the `32`-byte SHA-256 digest of
the exact serialized transaction bytes. A location lookup returns this digest
even when `storage.raw_blob_policy = "none"`.

RocksDB applies these codecs per data column family:

| Canonical column families | Upper levels | Bottommost level and bulk SSTs | Filter |
| --- | --- | --- | --- |
| `tree_state_checkpoint`, `displaced_block_facts`, `live_tail_raw_block` | LZ4 | Zstandard level 3 | None |
| `block_hash_index`, `transaction_location` | None | None | Partitioned hybrid Ribbon (`10` bits/key) + two-level index (top-level blocks pinned) |
| `chain_epoch`, `chain_event` | LZ4 | LZ4 | Full-table Bloom (`10` bits/key) |
| `block_header`, `subtree_root`, `live_tail_block`, `settled_segment_index`, `mempool_event` | LZ4 | LZ4 | None |

A control record binds the store to its network, raw-blob retention
(`none` or `all`; legacy `transactions` is removed and refused), reorg window,
network-upgrade activation schedule, `canonical-construction-manifest.v2.json`
digest, and current chain event. Admission checks all of them, as described under
[schema admission](#schema-admission).

A fresh store is built in an inactive staging path and published once as a
validated baseline. After that, each live block commit or reorg applies atomically:
any newly completed aligned `4,096`-block settled boundary is sealed into external
segment files under `segments/` first (staged in `segments/.staging`, fsynced,
renamed into `segments/{shielded,supplement,raw_block}/`, with parent directories
synced), and one RocksDB `WriteBatch` then inserts `settled_segment_index`,
deletes the newly sealed `live_tail_block` and `live_tail_raw_block` rows, and
advances indexes, chain epoch, chain event, and sequence digest together before
updating `segments-manifest.json` (`CANONICAL_SEGMENTS_MANIFEST_SCHEMA_VERSION = 2`).
Because `live_tail_block` holds only unsealed heights above `sealed_tip` — at most
`4,095` settled-but-unsealed blocks (up to `settled_tip`) plus the live reorg
suffix through the visible tip (at most `reorg_window_blocks + 4,095` physical rows
in total),
bounded compaction keeps unsealed tail storage small while reorg eligibility
remains strictly bounded by `settled_tip` / `reorg_window_blocks`. On primary
reopen, crash recovery cleans stale `segments/.staging` files and any
unreferenced segment files, then resynchronizes `segments-manifest.json` from
`settled_segment_index`. [Chain ingestion](chain-ingestion.md) describes
construction and following.

### External settled segments and cache

Settled blocks (`height <= sealed_tip` aligned to `DEFAULT_SETTLED_SEGMENT_BLOCKS = 4,096`)
reside outside RocksDB as immutable, `fs-verity`-compatible files under
`<store_root>/segments/` capped at `<= 256 MiB` (`DEFAULT_MAX_SETTLED_SEGMENT_BYTES`)
per segment across all three formats so each segment also fits single-object
PostgreSQL `BYTEA STORAGE EXTERNAL` and object-storage bounds (oversized strides
presplit deterministically):

- **`ZSHRNG02` (`segments/shielded/<start:010>-<end:010>.zshrng02`, always present):**
  128-byte fixed big-endian header, `Slab 0` containing 5-byte gRPC-framed
  pre-encoded `DEFAULT_RANGE` lightwalletd `CompactBlock` payloads, an `80`-byte
  per-block row index (block hash, `payload_sha256`, `block_time`, slab offset,
  `native_bytes` budget, `PoolPresenceMask` distinguishing Sapling spends from Sapling
  outputs plus Orchard and Ironwood, and row flags — with wire frame lengths derived
  by `frame_span` from adjacent slab offsets or the slab end), and a 32-byte whole-body SHA-256 trailer. Reader
  opening validates `index_sha256` in one positioned read (`O(index)`),
  retaining `12 B/block` compact rows (`+ 0.5 B/block` 64-row page digests when
  not backed by kernel `fs-verity`). Sans-I/O `ByteRange` planning groups short
  rows (`< 32 KiB`) into `512 KiB` slabs, isolates large rows (`>= 32 KiB`), skips
  `Slab 0` on unmatched/empty blocks for `GetBlockRangeNullifiers`, and projects
  narrowed pool subsets or nullifier wire payloads on the fly without decoding
  full blocks.
- **`ZSTSUP01` (`segments/supplement/<start:010>-<end:010>.zstsup01`, supplement module):**
  `16`-block `Zstd` level-3 (or uncompressed) compressed chunks of per-block
  tagged sections: Tag `1` compact transparent supplement (`tx.index`, `txid`,
  `fee_zat`, `vin`, `vout`) and Tag `2` extensible canonical block/tx metadata
  (ZIP 229/258/248/226/270/272 fields, explicit anchors,
  `zip317_logical_actions`, `paid_fee`, and `burn_zat`), with `O(index)`
  `index_sha256` reader opening and sans-I/O `ByteRange` planning. Together with
  `ZSHRNG02` `Slab 0`, `splice_compact_block_wire` yields byte-identical
  transparent-inclusive `CompactBlock` frames and
  `reconstruct_compact_block_artifact` / `reconstruct_canonical_block_facts`
  recover full canonical records for native query, wallet projection, and
  Explorer views.
- **`ZSBLKB01` (`segments/raw_block/<start:010>-<end:010>.zsblkb01`, when `raw_blob_policy = "all"`):**
  single-copy full consensus block vault storing each block once (`Zstd` level 3
  or uncompressed), a flat `40`-byte per-transaction slice table `(tx_offset,
  tx_len, tx_sha256)` read on demand without retaining an in-memory tx vector, a
  `128`-byte per-block row index, and a 32-byte trailer, with `O(index)`
  `index_sha256` reader opening and sans-I/O `ByteRange` planning. Setting
  `storage.full_blocks = true` (`storage.raw_blob_policy = "all"`) enables
  `ZSBLKB01` + `live_tail_raw_block` with one copy of full block bytes instead
  of separate transaction and block blob copies.

`SettledSegmentReaderCache` holds a thread-safe bounded-LRU lazy cache (default
`1,024` entries) keyed by `(start_height, SettledSegmentKind)` with miss
singleflight deduplication. Opening `ZSHRNG02` never touches `ZSTSUP01` or
`ZSBLKB01`, and opening `ZSTSUP01` never touches `ZSBLKB01`.

During cold store admission (`verify_pinned_segments_into_cache`), storage
admission checks `kernel_verified()` on each pinned `ZSHRNG02`, `ZSTSUP01`, and
`ZSBLKB01` segment reader. `O(index)` describes opening and index
authentication for an individual segment reader rather than total cold store
admission: when kernel `fs-verity` verification confirms a file digest,
per-file `verify_full_payload` scans are skipped, whereas without kernel
`fs-verity` admission falls back to full-payload verification across all three
segment formats. Cold admission also verifies cross-stride and cross-module
linkage across pinned segments (boundary block checks and, when both
`ZSTSUP01` and `ZSBLKB01` are present, `verify_raw_against_supplement_range`,
which reads and decodes every `ZSTSUP01` chunk to cross-check raw-block
digests).

### Canonical read path

Construction and every transition verify each block's compact block and
supplement against its canonical header before committing. Reads first validate
every requested height against retained history: a height below the first
retained block or above the visible tip fails as an unavailable artifact even
when an unsealed or historical row is present on disk.

Heights are routed in memory via `O(log N)` binary search over the contiguous
`settled_segment_index` vector (for settled heights `<= sealed_tip`) and batched
`MultiGet` point lookups into `live_tail_block` / `live_tail_raw_block` (for
unsealed heights `> sealed_tip`):

- Direct lightwalletd shielded and nullifier streaming
  (`encoded_compact_block_range_up_to`) reads `ZSHRNG02` `Slab 0` through
  `SettledSegmentReaderCache` (using sans-I/O `ByteRange` plans) and
  `live_tail_block`, verifying frame lengths, digests, and `(height, block_hash,
  previous_block_hash)` linkage across segment and tail boundaries.
- Full `CompactBlockArtifact` and `CanonicalBlockFacts` reads splice `ZSHRNG02`
  plus `ZSTSUP01` for settled heights and decode `ZSLVTL01` rows in `MultiGet`
  batches for unsealed tail heights:
  - An unbounded range read fetches up to `1,024` unsealed tail heights per batch.
  - Tree-state replay fetches up to `16` heights per batch and checks for
    cancellation after each block. Wallet queries at the same height and chain
    fence share a reconstruction; the last caller to leave cancels the work.
    Each query runtime caches up to `256` settled answers scoped to the epoch,
    sequence digest, and retained history.
  - A byte-bounded read fetches one row first, then adapts subsequent batch sizes
    from `1` to `16` within the remaining byte budget.

Reads verify block height linkage across every decoded row and fail on missing
rows inside retained history as store damage. Compact-block payload range reads
never construct RocksDB iterators over range data families (preventing L0 SST open
amplification during range sweeps and limiting secondary invalidation on those
reads to `MultiGet` lookups into compacted-away SSTs); checkpoint-at-or-before
lookups on `tree_state_checkpoint` still use a reverse RocksDB `iterator_cf` and
can therefore also trigger secondary invalidation, which reader-pair replacement
handles cleanly as recorded in [ADR-0015](../adrs/0015-wallet-runtime-composition.md).

A transaction byte lookup by ID reads the `72`-byte `transaction_location` row
once through the active handle (seeing one immutable chain view). When
`storage.raw_blob_policy = "all"`, it reads the exact transaction slice from the
indexed `ZSBLKB01` segment (for settled heights) or `live_tail_raw_block`
(`ZSLVRB01`, for unsealed heights) and verifies the slice's `tx_sha256`; when
`none`, callers fetch bytes from the verified primary node and compare against
the indexed `32`-byte SHA-256 digest. A caller-supplied location check compares
first against `transaction_location` and returns no bytes if the row is missing
or differs in height, block hash, index, or digest.

## Wallet store

The wallet store holds transparent address data. Shielded data, such as
compact blocks and tree states, stays in the canonical store. The wallet store
is versioned by `WALLET_ROCKSDB_SCHEMA_VERSION` (`WALLET_PROJECTION_SCHEMA_VERSION = 2`) for its layout and
`WALLET_PROJECTION_VALUE_ENCODING_VERSION` (`2`) for its row values; older schemas fail closed on open and require a fresh wallet rebuild. Its control
record binds it to one canonical source identity and records the chain event it
has reached and a digest of its rows.

| Column family | Key | Block size | Upper levels / bottommost & bulk SSTs | Filter |
| --- | --- | --- | --- | --- |
| `transparent_unspent_output` | `txid ‖ output_index_be` (36 bytes) | 8 KiB | LZ4 / Zstandard level 3 | Partitioned hybrid Ribbon (`10` bits/key) + two-level index (top-level blocks pinned) |
| `transparent_spent_output` | `txid ‖ output_index_be` (36 bytes) | 8 KiB | LZ4 / Zstandard level 3 | Partitioned hybrid Ribbon (`10` bits/key) + two-level index (top-level blocks pinned) |
| `transparent_unspent_output_by_address` | `address_script_hash ‖ height_be ‖ txid ‖ output_index_be` (72 bytes) | 16 KiB | LZ4 / Zstandard level 3 | Partitioned hybrid Ribbon (`10` bits/key) + two-level index (top-level blocks pinned) |
| `transparent_address_transaction` | `address_script_hash ‖ height_be ‖ tx_index_be` (40 bytes) | 16 KiB | LZ4 / Zstandard level 3 | Partitioned hybrid Ribbon (`10` bits/key) + two-level index (top-level blocks pinned) |
| `transparent_address_balance` | `address_script_hash` (32 bytes) | 4 KiB | LZ4 / Zstandard level 3 | None |
| `reorg_undo` | `height_be` (4 bytes) | 4 KiB | LZ4 / Zstandard level 3 | None |
| `block_hash_by_height` | `height_be` (4 bytes) | 4 KiB | None / None | None |

Schema v2 normalizes canonical block hashes out of per-output and per-address-transaction rows into `block_hash_by_height` (`4`-byte big-endian height to `32`-byte block hash). `transparent_address_transaction` stores only the `32`-byte `txid` (`WalletAddressTransaction`), while `transparent_unspent_output` (`StoredWalletUnspentOutput`) and `transparent_spent_output` (`StoredWalletSpentOutput`) store a `1`-byte flag header (script kind plus an explicit-address-hash bit), omit derived `address_script_hash` unless overridden, encode `value_zat`, heights, `tx_index_in_block`, and `input_index` as canonical LEB128 varints, and collapse standard P2PKH and P2SH scripts to `20`-byte `hash160` tags. Compressible wallet column families use LZ4 in upper levels and Zstandard level 3 (`window_bits = -14`) for bottommost compaction and bulk-loaded SSTs, while `block_hash_by_height` stays uncompressed (`None`). `RocksDbWalletStore` pairs `block_hash_by_height` with a bounded FIFO `BlockHashChunkCache` of `128`-block chunks protected by `parking_lot::RwLock` (budgeted at `clamp(block_cache_capacity / 4, 1 MiB, 128 MiB)`): warm page reads resolve cached block hashes under one shared read lock, while misses are sorted and grouped by `128`-block chunk (`resolve_missing_block_hashes`) and populated via forward ordered `iterator_cf` range scans over `block_hash_by_height` (`load_block_hash_chunk`, bounded to each chunk's `[start_height, end_height]` span; missing heights fail closed as `AdmissionChanged`). Transitions and secondary catch-up call `invalidate_from` at the previous settled tip height, evicting chunks from that chunk ID upward.

The filters let the projector prove quickly that a new output, spend, or
history row does not exist yet. They use about 10 bits per key, for roughly a
1% false-positive rate. Flushed and level-0 files use Bloom filters, and deeper
files use Ribbon filters, which are about 30% smaller. Point-lookup families
enable `TwoLevelIndexSearch`, partitioned filters, and pinned top-level index
and filter blocks; filters share the store's block cache. Materialized-view
stores and canonical families other than `block_hash_index`,
`transaction_location`, `chain_epoch`, and `chain_event` use no filters.

**Construction.** The projector builds a new wallet store in an inactive path
from a pinned canonical event. Before it publishes the store, it reopens it
cold and reads every row back. The row digest and per-family row counts must
match what construction produced, and the balances must sum to the unspent
total. Otherwise the build is refused.

**Row digest.** The row accumulator is a 2,048-byte `LtHash16` accumulator
over every row of the seven families (`WalletProjectionRowFamily` tags `1..=7`,
including `BlockHashByHeight = 7`): 1,024 little-endian 16-bit lanes, summed
modulo 2^16. The row digest is the 32-byte BLAKE2b-256 hash of it. Each row
contributes 2,048 bytes derived from its preimage. The preimage is the
domain `zinder:wallet-projection:row:v1\0`, the accumulator version
`WALLET_PROJECTION_ACCUMULATOR_VERSION` (1), the value encoding version
`WALLET_PROJECTION_VALUE_ENCODING_VERSION` (2), the
family tag, and the key and value, each prefixed with its 32-bit big-endian
length. An unkeyed 44-byte BLAKE2b hash of the preimage with the
personalization `ZinderWProjRoot1` supplies a ChaCha12 key (32 bytes) and
nonce (12 bytes). The first 2,048 keystream bytes are the row's lanes, and
removing a row subtracts the same lanes. The control record keeps the full
accumulator. The UTXO set commitment uses the same expansion over its own
element encoding and personalization.
[ADR-0017](../adrs/0017-transparent-wallet-queries.md) records the choice and
its security limits. A store written under another accumulator version carries
another schema version. The projector refuses to open it, and the wallet must
be rebuilt.

The row digest and the UTXO set commitment are sums of per-row hashes, so they
do not depend on the order rows are hashed in. Construction also scans the
settled canonical prefix (`ZSHRNG02` + `ZSTSUP01` segments) in `4,096`-block
height chunks across
`projector.build.accumulator_hashing_workers` threads into sharded outpoint
sorters that split `projector.build.max_outpoint_sort_memory_bytes` evenly
while sharing `projector.build.max_temporary_file_bytes_per_sorter`, bounding
in-flight chunk digests to at most `min(4W, 64)` chunks while verifying every
chunk boundary and the ordered sequence digest against the READY fence (and
scanning the unsettled suffix serially for `reorg_undo`). Construction hashes
rows on `projector.build.accumulator_hashing_workers` threads, in batches,
while the build thread drains its sorters and writes SST files (and, above 1 worker,
overlaps the secondary address-index/balance and address-transaction sort-and-drain
branches, sharing the build's hashing worker pool and global batch budget between them, while offloading `SstFileWriter`
writes and dual SHA-256 evidence hashing onto bounded-memory worker queues capped
at `clamp(sst_target_logical_bytes / 8, 64 KiB, 4 MiB)` per open bulk SST writer,
at most 12 MiB across the 3 concurrent secondary writers). Each batch is allocated
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
[ADR-0005](../adrs/0005-bounded-process-memory.md) requires. Cold validation reads the unspent and spent output families and the address-transaction family back in up
to the same number of hash-prefix key ranges, at most one per 65,536 of each
scan's rows, each on its own thread, and checks every row as one scan would. The
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

During active-writer following, the wallet projector retries a replay
transition once if its canonical secondary cannot see a live-tail row at or
below the admitted tip. It opens a fresh secondary, validates READY and the
previous fence, and checks the writer's attestation before retrying. If
admission or the retry fails, the projector stops without publishing the
transition.

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
  through `block_hash_index` and read the block (`ZSHRNG02` + `ZSTSUP01` or
  `live_tail_block`) it names. A resolved hash whose compact block the store
  lacks fails `compact_block_by_hash` as that block's unavailable artifact, so
  it never reads as an unknown hash.
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
| Canonical reader | 1/8 of the container memory limit, from 128 to 512 MiB | 32 MiB | 1024 to 2048, scaled with the block cache (2048 without a limit) | 16 MiB |
| Wallet writer | 256 MiB | 32 MiB | 512 | 512 MiB |
| Wallet reader | 64 MiB | 16 MiB | 1024 | 16 MiB |
| Materialized-view writer | 256 MiB | 256 MiB | 512 | 512 MiB |
| Materialized-view reader | 64 MiB | 16 MiB | 64 | 16 MiB |
| Read-only admission probe | 4 MiB (or caller's shared cache) | — | 32 (or caller's budget) | — |

The wallet writer caps its live WAL at 32 MiB so primary flushes bound WAL
replay memtables across retained serving secondary generations. Secondary and
read-only probe opens shard each store's RocksDB table cache into four shards
(`table_cache_numshardbits = 2` instead of RocksDB's 64-shard default) so hash
skew does not evict live SST handles while total capacity remains free. The
canonical reader keeps 1,024 to 2,048 SST handles open (2,048 without a
container limit; 254 to 510 table readers per shard) and the wallet reader
keeps up to 1,024 open (254 per shard), retaining Mainnet and Testnet SST files
while index and filter blocks remain bounded in each store's shared block
cache.

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

- the store identity or layout version (`CANONICAL_STORE_SCHEMA_VERSION = 2`);
- the network;
- the required retained data (`CanonicalStoreRequirements`) and its historical floor for a canonical reader;
- the exact `raw_blob_policy` (`none` or `all`) for a writer or recovery checkpoint (legacy `"transactions"` and Schema v1 stores fail closed);
- the network-upgrade activations at or below the visible tip;
- the reorg window;
- for the wallet store, the canonical source it was built from.

Canonical Schema v2 requires all 12 data column families plus `default` listed
above, `canonical-construction-manifest.v2.json`, and `segments-manifest.json`
together with every `settled_segment_index`-referenced external segment file
(`ZSHRNG02`, plus `ZSTSUP01` and `ZSBLKB01` according to the admitted
modules). The construction manifest must bind the same raw-blob retention and
build plan as the control record and carry evidence for the full family
inventory, including zero-row checks on post-publication families (`chain_epoch`,
`chain_event`, `mempool_event`, `displaced_block_facts`) and on
`live_tail_raw_block` when `raw_blob_policy = "none"`.

`CanonicalStoredSet` derives retained block data and its history floor from
the build plan. Readers specify minimum data and a starting height with
`CanonicalStoreRequirements` (checked by `CanonicalStoredSet::require`). A richer
store (such as `raw_blob_policy = "all"`) satisfies a `none` requirement when it
covers the requested history. Missing data or an insufficient history floor
returns `CanonicalStoreError::MissingStoredData`.
The native query and compatibility listeners keep their first admitted
stored set across generation changes. Restart the process to
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

The canonical writer publishes activation schedule revision 1 with the first
READY control record. Readers validate the schedule digest and revision in
control against the journal, then adopt the admitted schedule. Only the writer
records future revisions. See [ADR-0008](../adrs/0008-consensus-parameters-from-the-node.md).

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
[storage benchmark environment](../../deploy/storage-benchmark.md) and `deploy/bench/docker-compose.storage-benchmark.yml` run it.
