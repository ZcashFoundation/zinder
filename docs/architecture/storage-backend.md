# Storage backend

Zinder's supported topology uses separate RocksDB databases for canonical and
wallet state. Optional explorer materialized views use a third database. Each
database has one primary owner, exact identity and schema admission, bounded
resources, and read-only secondary roles.

[ADR-0035](../adrs/0035-canonical-storage-topologies.md) owns the topology.
[ADR-0003](../adrs/0003-canonical-storage-access-boundary.md) owns epoch-bound
reads and secondary behavior.

## Canonical RocksDB

The canonical store is implemented by `RocksDbCanonicalStore` and
`RocksDbCanonicalSecondary`. Its persisted identity is `canonical`, and its
physical layout is versioned by `CANONICAL_STORE_SCHEMA_VERSION`.

The ready control record binds:

- network and the construction network-upgrade activation fingerprint;
- after the writer's first admission, the latest network-upgrade schedule
  revision and its digest;
- `CanonicalStoreWorkload`;
- `CanonicalReorgPolicy`;
- history bounds and predecessor checkpoint;
- construction manifest identity;
- current `CanonicalEventFence` and ordered facts digest;
- build state and writer generation; and
- event-retention, projection-lease, and checkpoint-coordination state.

The physical families are domain-specific:

| Family | Purpose |
| --- | --- |
| `block_replay` | Reversible semantic `CanonicalBlockFacts` envelope by height |
| `block_header` | Direct height-to-header reads |
| `block_hash_index` | Direct hash-to-height resolution |
| `transaction_location` | Optional transaction-to-block location |
| `compact_block` | Wallet compact-block payloads |
| `tree_state_checkpoint` | Bounded commitment-tree recovery anchors |
| `block_final_note_commitment_roots` | Final per-block commitment roots |
| `subtree_root` | Subtree root ranges |
| `chain_epoch` | Retained chain epoch identities |
| `chain_event` | Authenticated append and replacement history |
| `mempool_event` | Retained typed mempool history |
| `displaced_block_facts` | Reorg-displaced block evidence |
| `block_blob`, `transaction_blob` | Optional raw bytes under the retention policy |
| `daily_value_pool_balance` | Highest canonical daily value-pool snapshot used by current consumers |

The store also contains singleton control and manifest records. Their keys are
private implementation details; public callers use domain operations rather
than RocksDB column families.

Canonical construction writes through `RocksDbCanonicalBuilder` in an inactive
staging path. Publication produces a validated baseline and ready control
record. Live append and replacement use atomic write batches that advance the
facts, indexes, epoch, event, and sequence commitment together.

## Wallet RocksDB

`RocksDbWalletBuildStore`, `RocksDbWalletFollowingStore`, and
`RocksDbWalletStore` separate construction, mutable following, and ready reads.
The physical layout is versioned by `WALLET_ROCKSDB_SCHEMA_VERSION` and the
stored values by `WALLET_PROJECTION_VALUE_ENCODING_VERSION`.

The wallet control record binds the store to one
`WalletCanonicalSourceIdentity`, projection source position, accumulator,
digest, build state, and lease generation. Wallet row families contain address
history, unspent outputs, spent outputs, balances, UTXO summaries, and bounded
reorg undo. They are query state, not canonical truth.

An inactive build is published only after its source identity and digest are
validated. Construction sorts the address-ordered families once while it writes
their SST files and emits the order-independent projection accumulator and the
six family row counts from those rows. Cold validation then reopens the flushed
store and reads every family back once in stored key order. It recomputes the
accumulator and row counts from the durable rows and requires both to equal the
construction evidence, so a row that was lost, altered, duplicated, or added
between SST writing and publication refuses promotion. The same scan decodes
every row, refuses an outpoint present in both output families, refuses a spent
output that does not follow its creation position, requires the balance rows to
sum to the unspent total, and reconstructs the retained reorg-undo suffix. The
primary output, address-index, and address-transaction scans run concurrently
and their partial accumulators are combined, which is valid because the
accumulator is order independent. The readback does not sort the address
families a second time, and the build report records the wall time of each
family scan.

The readback authenticates that the durable rows are the rows construction
produced. It does not re-derive the address-ordered families from the primary
output rows. Recovery admission of a restored checkpoint keeps that stricter
check: its READY evidence is stored beside the rows and cannot vouch for them,
so `cold_admit_recovery_checkpoint` rebuilds the expected address-index and
address-transaction rows through bounded external sorts and compares them with
the stored rows.

Following applies one canonical event transition atomically with the
new source position. Ready readers never repair or advance the store.

Within one transition, the planner remembers each output, address-index,
balance, and address-transaction row it reads, including a row it finds
missing, and reuses that answer until the transition writes the row. The
remembered rows count toward the transition's logical-byte limit
(`projector.follow.max_transition_logical_bytes`), but only in the room that
the write batch and pending writes leave. They are dropped when those need
the room, so the limit refuses the same transitions as it would without them.
While the limit leaves room for the remembered rows, spending an output or
creating one makes at most four point reads: its unspent-output, spent-output,
address-index, and balance rows. Each address a transaction touches adds one
read of its address-transaction row. A transition close to its limit may read
a row again after the planner drops what it remembered.

Reorg-undo rows cover exactly the unsettled suffix. Each row's starting source
digest equals the resulting digest of the row below it, and the top row matches
the wallet's source tip. A following handle reads and decodes every retained
undo row to check this:

- on its first transition after it opens, or after a transition that failed
  during or after its write;
- on every transition that rolls back a reorg; and
- on every reconciliation.

A transition that finds a missing, undecodable, or disconnected row fails
without writing. Other appends on the handle do not re-read the rest of the
retained suffix. Each one reads the successor row that binds a newly settled
tip, which it keeps, and reads each row it prunes once. It checks that the
pruned rows connect by source digest to that successor, or to the new source
tip when every row settles. The rows it writes come from the authenticated
replay and continue from the verified tip. When each append settles one block,
an append without transparent activity makes three point reads of the undo
family, whatever the reorg window.

A retained row changed outside the wallet so that it is missing, does not
decode, or breaks the digest chain is found no later than the handle's next
full check or the append that prunes it, whichever comes first. Neither check
compares the effects a row records with the rows they undo, so a change that
leaves a row decodable and its digests connected is not found this way.

## Materialized-view RocksDB

`MaterializedViewStore` is stored under the `materialized-views` subdirectory of
the configured canonical `storage.path`. Its shared container is versioned by
`MATERIALIZED_VIEW_STORE_FORMAT_VERSION`; each consumer separately versions its
owned row contract through `MaterializedViewConsumerSchema`.

Consumer rows, cursors, materialized-view state, coverage, and schema metadata remain
separate from both canonical and wallet storage. See
[Materialized-view plane](materialized-view-plane.md).

## Primary and secondary roles

Every store path has one primary owner. A reader in another process uses a
process-owned RocksDB secondary with a unique metadata path. Catch-up is
explicit and validated. A secondary:

- never writes the primary;
- revalidates identity and schema after catch-up;
- reports catch-up failure to readiness, and lag where the runtime defines a
  lag threshold, with the per-runtime causes in
  [Service operations](service-operations.md#runtime-specific-readiness);
- does not treat an open database as evidence of exact-fence agreement; and
- cannot be shared by two processes or two reader generations.

Every READY publication must be explained by the persisted replay rows from the
settled checkpoint through the visible tip. Opening a canonical primary or
secondary derives that whole unsettled suffix once. Afterwards the writer
authenticates only the rows each of its transitions wrote, and a secondary only
the rows the chain events since its last catch-up changed; a catch-up whose
READY evidence is unchanged reads no replay rows. Both re-derive the whole
suffix once their incrementally authenticated rows reach one reorg window, and
a secondary also does so when the intervening events are no longer retained or
its incremental check fails. Per-transition cost therefore follows the rows a
transition changes rather than the reorg window.

`zinder-query` and the transparent-serving `zinder-compat-lightwalletd` rotate
through four secondary generation slots: the current pair, the candidate the
reader catches up and validates before publishing it, and two for replaced
pairs. A replaced pair is either retained, still answering reads pinned to its
epoch, or retired but held by an in-flight request; a retained pair gives up
its slot when a held retired pair needs it. In-flight requests keep their
existing immutable pair, which pins its generation until they finish. A
transparent-output scan whose request is dropped stops at its next page and
releases its pair. The compact-blocks-only `zinder-compat-lightwalletd`
alternates two canonical secondary generations and retains no replaced pair.

## Resource bounds

All RocksDB opens use `RocksDbResourceBudget` and shared helpers from
`zinder-store`. Role-specific budgets cover block cache, memtables, WAL,
background jobs, open files, direct-I/O policy, statistics collection, and
flush behavior. Store size must not make process RSS unbounded.

The canonical secondary, materialized-view primary, and materialized-view
secondary export `zinder_store_*` resource metrics under distinct `store_role`
labels; the canonical primary and the wallet stores do not export them.
Operators should alert on memory pressure, live WAL bytes, pending compaction,
write stops, catch-up failure, and replica lag rather than infer health from
directory size.

Only the wallet store configures filters. Its four hash-keyed families answer
the projector's reads that prove a created output, spend, address-index row,
or history row is new, and those reads stop at the filter unless it reports a
false positive. Every open path and the bulk-build SST writer use the same
per-family options.

| Store | Family | Key layout | Filter |
| --- | --- | --- | --- |
| Wallet | `transparent_unspent_output`, `transparent_spent_output` | `txid ‖ output_index_be` (36 bytes) | Partitioned hybrid Ribbon |
| Wallet | `transparent_unspent_output_by_address` | `address_script_hash ‖ height_be ‖ txid ‖ output_index_be` (72 bytes) | Partitioned hybrid Ribbon |
| Wallet | `transparent_address_transaction` | `address_script_hash ‖ height_be ‖ tx_index_be` (40 bytes) | Partitioned hybrid Ribbon |
| Wallet | `transparent_address_balance` | `address_script_hash` (32 bytes) | None |
| Wallet | `reorg_undo` | `height_be` (4 bytes) | None |
| Wallet | default | Singleton control record | None |
| Canonical | Every family | Per family, private to `zinder-store` | None |
| Materialized views | Every family | Owned by each consumer | None |

The filters are whole-key filters at about 10 bits per key, about a 1%
false-positive rate. Flushed and L0 files use Bloom filters, and deeper files,
including bulk-built ones, use Ribbon filters, which are about 30% smaller.
Filter and index partitions share the store's bounded block cache. An existing
store gains filters only as RocksDB rewrites its files, so the full effect on a
store built by an earlier release needs a
[side-by-side wallet rebuild](../runbooks/deploying-on-a-vm.md#side-by-side-wallet-rebuild-and-cutover).
Files without filters stay readable, and a release without filters ignores
them. [ADR-0020](../adrs/0020-bounded-rocksdb-resource-budget.md) explains how
to count the filter working set when sizing the wallet block cache.

See [ADR-0020](../adrs/0020-bounded-rocksdb-resource-budget.md) for the memory
contract and [Service operations](service-operations.md) for metrics.

## Schema admission

Every opener validates exact identity and version before returning a role
handle. A newer or older unsupported physical layout, wrong network, wrong
workload, wrong construction activation fingerprint, an activation table that
differs from the admitted schedule at or below the visible tip, a reorg window
other than the store's effective window, or incompatible source identity fails
without mutation. Only the canonical writer journals a new schedule revision;
it may change only activations above the visible tip. A reader checks its
node's table only when it first opens a secondary. Catch-ups and later reader
generations follow the writer-admitted schedule; see
[ADR-0039](../adrs/0039-writer-admitted-network-upgrade-schedules.md).

The effective reorg window is the construction window until the canonical
writer opens the store with a larger one. The writer then appends a versioned
reorg-policy trailer after the schedule trailer in READY control, in one synced
write, and holds the settled tip so the replaceable suffix grows to the new
window. A smaller window is refused. The projector raises the wallet's
supported reorg depth to the canonical effective window when it resumes,
without backfilling undo rows. Readers require the exact effective window, and
releases before the trailer refuse a raised store. See
[ADR-0041](../adrs/0041-reorg-window-follows-the-node-bound.md).

Version domains are independent. Canonical physical schema, canonical replay
format, canonical digest, wallet physical schema, wallet value encoding,
materialized-view container format, and individual consumer schemas advance
only when their own bytes or semantics change.

When a layout has no explicit compatible path, the operator constructs a fresh
store or restores a certified coherent bundle. This includes every
materialized-view container mismatch: a store open returns `SchemaMismatch`
without deleting or rebuilding the existing directory. A reader never upgrades
a primary, and Zinder never adopts an unknown non-empty directory.

## Checkpoints and recovery

Physical RocksDB checkpoints are useful building blocks, not sufficient backup
evidence. A wallet-serving restore must authenticate canonical and wallet state
together at one source fence, restore them into fresh paths, and pass normal
owner and wallet-serving admission before traffic becomes ready.

Checkpoint preparation is owner-coordinated. Cold admission reopens the
checkpoint with bounded resources and compares its database identity and ready
evidence with the source captured before checkpoint creation. Independently
timed copies of primary directories are not a coherent restore contract.

A checkpoint of a store whose writer raised its reorg window enforces the
raised window. Owner-checkpoint evidence and state-bundle manifests record it
as `effective_reorg_window_blocks` beside the construction window, and omit it
for a store that still enforces its construction window. Restoring such a
bundle requires configuring every runtime with the recorded window.

## PostgreSQL diagnostics

`zinder-bench` contains PostgreSQL and RocksDB implementations of the canonical
block-facts persistence benchmark. Both consume the same captured corpus and
digest oracle. The PostgreSQL arm does not implement Zinder's store ownership,
wallet projection, serving admission, replication, backup, restore, or
readiness contracts and is not a deployable backend.
