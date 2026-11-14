# Materialized-view plane

The materialized-view plane builds explorer-shaped indexes and aggregates from
canonical artifacts and retained events. It is optional, independently
versioned, and rebuildable. A materialized-view failure must not change
canonical truth or wallet projection state.

The implementation lives in `zinder-materialized-views`. The crate owns the
consumer SDK, the materialized-view RocksDB wrapper, bundled explorer
consumers, per-consumer schema manifests, cursors, coverage, and read
snapshots. `zinder-explorer` is the primary reader of those views.

This subsystem is not the wallet projection. Wallet state is owned by
`zinder-wallet-projection`, `zinder-wallet-rocksdb`, and `zinder-projector`.
Those crates do not depend on `zinder-materialized-views`.

## Deployment status

`zinder-ingest` builds the materialized views. It opens one in-process
canonical secondary at `<storage.path>.materialized-view-secondary`, hydrates
block contexts from canonical replay rows, and writes the view store nested
under the canonical path. The writer handle never serves these reads, so the
canonical writer's cross-block read counters stay at zero.

`zinder-explorer` reads those views as a RocksDB secondary. It is not part of
the release composition: the checked release topology does not start it or
expose its native query service. An operator who wants the explorer query
surface runs it separately against the same storage path.

## Runtime selection

The ingest runtime requests the optional Explorer materialized-view workload
with `[ingest].explorer_views = true`. The boolean defaults to `false` for
every coverage and deployment shape; coverage never enables Explorer views
implicitly. When false, the existing materialized-view directory remains
untouched and no materialized-view secondary, store, tailer, replay sampler,
or historical backfill starts. It does not change canonical-writer or
wallet-projection ownership or readiness.

`explorer_views = true` opens the Explorer view store with every bundled
consumer. Every bundled consumer is an Explorer view; there is no wallet
workload in the materialized-view store
([ADR-0043](../adrs/0043-owned-transparent-wallet-query-surface.md)). Wallet
projection and wallet-serving readiness remain owned by `zinder-projector`,
`zinder-wallet-rocksdb`, and the wallet readers.

When the field is true, the request is printed by `--print-config` and the
admitted state is emitted in bounded ingest startup evidence. Wallet-serving
deployment files set `explorer_views = false`; the Explorer Compose overlay
sets `ZINDER_INGEST__EXPLORER_VIEWS=true`.

An enabled store is admitted only after its persisted manifest matches this
release's bundled consumer set exactly. `MaterializedViewStore::open_explorer_views`
and `MaterializedViewStore::open_explorer_views_secondary` preflight the
manifest before the database opens. A recorded identity this release does not
bundle fails with `ConsumerNotDeclared`, and a recorded subset fails with
`ExplorerViewManifestRequiresFreshStore`. Neither failure mutates the store;
the operator rebuilds Explorer views on a fresh materialized-view path. A store
built before the `transparent_address_transaction_history` consumer was
retired records that identity and needs this rebuild. This selection does not introduce a storage-layout migration. Enabled
materialized views require genesis-complete canonical history or the
authenticated height-0 checkpoint: checkpoint heights above zero are not
admitted because cumulative address views can under-report when their producing
blocks precede the first retained height.

Readiness reports consumer identities in `materialized_view_identities` only
after enabled-store admission, and only from the process that admitted the
store. Disabled ingest therefore omits the field rather than claiming view
readiness. `ExplorerServerInfo.materialized_view_identities` carries the same
identities on the Explorer endpoint. Wallet endpoints carry none.

A store built with `ingest.run_overrides.checkpoint_height` at height 0 may host
enabled materialized views because height 0 is the authenticated predecessor
for the complete height-1 history. A checkpoint above height 0 does not host
materialized views: cumulative address views resolve spends against the
producing block, so a store whose first available height is above block 1
would silently under-report. The tailer refuses such a store instead.

## Ownership

A materialized view is appropriate when the value is a query-specific
aggregation, ordering, summary, or index that can be reconstructed from named
canonical inputs. Examples include transaction history, address activity,
block summaries, fee distributions, reorg incidents, value-pool history, and
time-indexed block production.

A value belongs in canonical storage when it is immutable source truth needed
by more than one consumer, required for wallet correctness, or required to
rebuild a projection without contacting the node. Consumer presentation,
ranking, rolling windows, and product-specific formulas do not belong in
canonical storage.

```text
Immutable reusable source fact?
├── yes -> canonical storage
└── no
    └── deterministic query-specific view? -> materialized-view consumer
```

Materialized-view consumers do not import `zinder-source` or call Zebra. If a
rebuild needs an upstream fact that canonical storage does not retain, the
canonical source and artifact contract must be extended first.

## Consumer contract

`MaterializedViewConsumer` is the event-level interface. It applies committed
and reorged chain events through a `MaterializedViewConsumerCtx` that owns the
pending RocksDB batch.

`BlockKeyedConsumer` is the standard per-block interface. Implementations apply
and revert one `BlockCommitContext`; the store's chain-event dispatch supplies
the event range loops and the batch hooks. `BlockCommitContext` carries the shared block identity,
time, transaction facts, and any hydrated spend facts required by the selected
consumers. The host hydrates a context once and shares it across consumers.

`MaterializedViewMempoolConsumer` handles typed mempool events for views that
include unconfirmed activity. Chain and mempool cursors remain separate because
chain state rewinds on reorg while the mempool event sequence does not.

Every consumer declares one `MaterializedViewConsumerSchema` containing:

- a stable `MaterializedViewConsumerName`;
- a monotonically increasing schema version;
- the complete set of owned column families; and
- the single row version admitted by the current reader.

Column-family ownership must be disjoint. Consumer names are persisted keys,
not display labels, so renaming one is a storage migration rather than a source
cleanup.

## Bundled consumer registry

`bundled_materialized_view_consumer_definitions()` is the single, closed list
of Explorer views bundled with a release. Each
`MaterializedViewConsumerDefinition` declares the schema and the
`MaterializedViewDispatch`:

| Dispatch | Receives | Recovery source |
| --- | --- | --- |
| `ChainEventBlocks`, join `Replay` | Every retained chain event with block contexts | Canonical chain events |
| `ChainEventBlocks`, join `CanonicalBackfill` | The live tail from the shared replay cursor | Canonical backfill plus chain events |
| `ChainEventBlocks`, join `CanonicalSnapshot` | Chain events once its snapshot is active | Canonical snapshot plus chain events |
| `ChainEventEnvelope` | Every retained chain event, envelope only | Canonical chain events |
| `MempoolEvents` | Mempool events, once a host drives them | Mempool events |
| `CanonicalBackfillOnly` | No events | Canonical backfill |

The store derives its exact consumer manifest and chain-event cursor lists from
the registry. The ingest replay host constructs each selected chain-event
consumer from its entry and derives its cursor-agreement and backfill-owned
sets from the join kind. Neither side keeps its own consumer list, so a
registered chain-event consumer cannot be left out of chain-event dispatch.
No bundled runtime drives `MempoolEvents` consumers yet.
`mempool_event_counts` is registered and included in the Explorer manifest, but
nothing calls `MaterializedViewStore::write_mempool_event`, so its column
family receives no rows.

### Publishing the canonical snapshot

The transparent-address ranking is the only `CanonicalSnapshot` consumer. Its
summaries are cumulative, so it cannot join the shared replay cursor partway
through history. The ingest replay host publishes its snapshot in one of two
ways:

- On a view store that has not materialized any height, the host activates an
  empty generation at genesis before the first rebuild. Its coverage is
  complete through height 0, and its lifetime history is the empty range that
  starts at height 1. The ranking then replays from height 1 with every other
  block consumer and follows the tip at the shared cursor.
- On a store whose views materialized heights without the ranking, the host
  builds an inactive generation from genesis. It starts only while the shared
  replay sits at the canonical fence. Each replay pass extends the build over
  settled history for at most 10 seconds, so the other views keep following
  the tip. The build resumes after a restart and adopts each pass's visible tip
  as its target.

The pass that reaches the settled tip reads the unsettled tail, at most one
reorg window long. One write applies that tail, activates the generation, and
adopts the shared cursor. Settled heights never change, so the inactive build
never holds a block that a reorg could replace. A pass that pauses or is
cancelled before that write leaves only settled progress behind. The ranking
store also checks that every tail block extends the block applied below it, so
a build left on a replaced branch fails instead of activating.

## Store contract

`MaterializedViewStore` is a separate RocksDB instance located under
`MATERIALIZED_VIEW_STORE_SUBDIR`, currently `materialized-views`, beneath the
configured canonical path. Writer and reader processes resolve that path
through `MaterializedViewStore::path_for_canonical`.

The primary stages consumer rows, materialized-view state, and cursor advances in one
write batch. A crash cannot publish a cursor beyond the rows it describes. A
secondary validates the container and every declared consumer after open and
after each catch-up; it never alters schemas or writes primary state.

`MaterializedViewStore::read_snapshot` binds materialized-view metadata and reads to
one store sequence. Primary stores use a RocksDB snapshot. Secondary stores
hold the shared side of the catch-up barrier for the snapshot lifetime, so a
catch-up cannot advance the underlying sequence halfway through a multi-read
response.

## Replay and coverage

The host feeds retained `ChainEventEnvelope` values in order, hydrates the
blocks named by each event, and calls
`MaterializedViewStore::write_chain_event`. Rebuild starts from the earliest
point covered by the consumer's declared recovery source, not automatically
from the oldest retained event.

Block-keyed consumers resume from their shared chain-event cursor. When that
cursor is expired, or when the consumers do not all hold one, retained events
cannot show whether a reorg replaced rows the view already holds. The host then
rebuilds from canonical heights instead. Before it rebuilds, it reverts the
reorg window below the view's newest materialized height. For a missing cursor
it skips that revert only when the block the view last checkpointed is still
canonical, because every row below a canonical block is canonical too. When the
view wrote its newest height, the settled tip was at least that height minus the
reorg window, so no replacement reaches below that window.

Consumers that make completeness claims persist `MaterializedViewState`
beside their rows. It records the canonical epoch, materialized-view tip, revision,
and optional contiguous `MaterializedViewCoverage`. Cursor position alone is
progress evidence and must not be presented as historical completeness.

A deterministic recovery source can be retained events, canonical artifacts,
or an explicit checkpoint. Before activating a replacement store, operators
must prove that the declared source covers the history being rebuilt. This
prevents a replacement from publishing partial history behind a fresh cursor.

## Schema lifecycle

`MATERIALIZED_VIEW_STORE_FORMAT_VERSION` versions shared container state:
manifest layout, cursor encoding, and metadata families. Every opener rejects a
container mismatch without mutation. Operators create a fresh materialized-view
path and rebuild from a certified recovery source; no service deletes an
existing materialized-view directory during open.

Individual row layouts use per-consumer versions. A fresh store records the
complete manifest atomically. Every later primary or secondary open requires
the exact consumer names, versions, owned column families, and physical
column-family set. Any divergence fails without mutation and requires a fresh
store rebuilt from a certified recovery source. The full decision is recorded
in [ADR-0028](../adrs/0028-materialized-view-schema-versioning.md).

## Key codecs and reorgs

Persisted keys use codecs from `zinder-core::wire`. Heights, positions,
timestamps, address script hashes, and outpoints are not encoded ad hoc inside
consumers.

Each block-keyed consumer must be able to delete exactly the rows produced by a
reverted block. Height-prefixed layouts use bounded range deletes. Layouts
whose primary key does not begin with height maintain a per-height index of the
keys written by that block. Reorg deletion and replacement rows share the same
batch as the cursor and materialized-view-state transition.

A reorg must leave the same bytes a fresh apply of the replacement chain
writes, independent of how blocks were grouped into batches. Reorg
conformance checks this for every registered chain-event consumer, in both
directions of a fork, and excludes only the reorg-incident log, which records
the reorg it observed. See
[ADR-0017](../adrs/0017-materialized-view-consumer-and-key-codec.md#rewind-contract).

## Query exposure

`zinder-explorer` exposes materialized views through `ExplorerQuery` and
advertises a capability only when its dependencies and coverage support the
method. An unavailable materialized-view store maps to the stable
`MATERIALIZED_VIEW_UNAVAILABLE` vocabulary. Missing data is never translated
into zero, an empty complete result, or canonical absence.

The Cipherscan adapter may translate those explorer methods into product
routes, but Cipherscan names and response shapes stop at the adapter boundary.
No materialized-view consumer may shape canonical storage around an external
product contract.

## Extension checklist

When adding a consumer:

1. identify the complete canonical recovery source and retention boundary;
2. choose a stable consumer name and owned column families;
3. add reusable key codecs to `zinder-core::wire`;
4. implement apply and revert behavior in one atomic batch, encoding index
   payloads in a deterministic order;
5. add one registry entry declaring the schema and dispatch;
6. persist truthful materialized-view coverage when the public method needs it;
7. define the fresh-store recovery behavior for a schema change;
8. expose the method under an explorer capability; and
9. test replay, crash recovery, secondary catch-up, and incomplete coverage
   refusal. The reorg conformance test runs every registry entry; extend its
   fixture chain until the consumer writes comparable rows, and list any
   path-dependent column family with its reason.

See [ADR-0017](../adrs/0017-materialized-view-consumer-and-key-codec.md)
for the consumer template and [Explorer plane](explorer-plane.md) for public
query behavior.
