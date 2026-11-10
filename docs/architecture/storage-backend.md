# Storage backend

Zinder's supported topology uses separate RocksDB databases for canonical and
wallet state. Optional explorer materialized views use a third database. Each
database has one primary owner, exact identity and schema admission, bounded
resources, and read-only secondary roles.

[ADR-0035](../adrs/0035-canonical-storage-topologies.md) owns the topology.
[ADR-0003](../adrs/0003-canonical-storage-access-boundary.md) owns epoch-bound
reads and secondary behavior.

## Canonical RocksDB

The release canonical store is implemented by `RocksDbCanonicalStore` and
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
validated. Following applies one canonical event transition atomically with the
new source position. Ready readers never repair or advance the store.

## Materialized-view RocksDB

`MaterializedViewStore` is stored under the `materialized-views` subdirectory of
the artifact-oriented canonical path. Its shared container is versioned by
`MATERIALIZED_VIEW_STORE_FORMAT_VERSION`; each consumer separately versions its
owned row contract through `MaterializedViewConsumerSchema`.

Consumer rows, cursors, materialized-view state, coverage, and schema metadata remain
separate from both release canonical and wallet storage. See
[Materialized-view plane](materialized-view-plane.md).

## Primary and secondary roles

Every store path has one primary owner. A reader in another process uses a
process-owned RocksDB secondary with a unique metadata path. Catch-up is
explicit and validated. A secondary:

- never writes the primary;
- revalidates identity and schema after catch-up;
- reports lag and catch-up failure to readiness;
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

`zinder-compat-lightwalletd` alternates secondary generations so it can catch up
and validate a replacement pair before publishing it. In-flight requests keep
their existing immutable pair.

The artifact-oriented `PrimaryChainStore`, `SecondaryChainStore`, and
`ChainEpochReader` remain the canonical contract used by optional explorer and
materialized-view components. They follow the same one-primary and epoch-bound
read rules but are not the release wallet-serving store.

## Resource bounds

All RocksDB opens use `RocksDbResourceBudget` and shared helpers from
`zinder-store`. Role-specific budgets cover block cache, memtables, WAL,
background jobs, open files, direct-I/O policy, statistics collection, and
flush behavior. Store size must not make process RSS unbounded.

Canonical primary, canonical secondary, wallet primary, wallet secondary,
materialized-view primary, and materialized-view secondary metrics use distinct
`store_role` labels. Operators should alert on memory pressure, live WAL bytes,
pending compaction, write stops, catch-up failure, and replica lag rather than
infer health from directory size.

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
