# Materialized-view plane

This page explains how Zinder builds the Explorer's indexes and aggregates: who writes them, the contract each one follows, which ship, how they replay, and how their schemas change. It is for developers changing a view and operators running one. [ADR-0020](../adrs/0020-materialized-views.md) records why the plane works this way. [Explorer plane](explorer-plane.md) describes the queries that read it.

## What a materialized view is

A materialized view is a query-shaped index or aggregate that Zinder can rebuild at any time from the canonical chain. Block summaries, address activity, fee distributions, and reorg incidents are views. Each view is written by one consumer, a piece of code that receives committed blocks and writes rows for them.

A fact belongs in canonical storage instead when it is immutable, needed by more than one consumer, needed for wallet correctness, or needed to rebuild a view without the node. Rankings, rolling windows, presentation, and product-specific formulas belong in views.

```text
Immutable, reusable source fact?
├── yes -> canonical storage
└── no
    └── deterministic, query-specific? -> materialized-view consumer
```

A consumer never imports `zinder-source` or calls a node. If a view needs a fact that canonical storage lacks, the canonical artifact is extended first.

Views are optional and rebuildable. A view failure never changes canonical or wallet state. The wallet projection is a different system, owned by `zinder-projector` and `zinder-wallet-rocksdb`, and does not depend on `zinder-materialized-views`.

## Who writes the views and when

Only `zinder-ingest` writes views, and only when [the Explorer is turned on](explorer-plane.md#turn-it-on). Otherwise ingest opens no view store and leaves an existing one untouched.

When enabled, ingest works in this order:

1. It waits until its canonical store is published, then opens an in-process canonical secondary and the view store as a primary.
2. It admits the store only if the recorded consumers match this release's bundled set exactly (see [Schema versions](#schema-versions)).
3. After canonical sync reaches the tip, it replays committed blocks into every view and then follows new blocks.
4. Once replay reaches the tip, it runs the historical backfills for the conventional-fee distribution and the transaction-component summary.

The view store is a separate RocksDB instance at `<storage.path>/materialized-views`, on the canonical volume. Replay pauses when that volume runs low on space, as [What it needs](explorer-plane.md#what-it-needs) describes, and also slows or pauses under memory pressure.

`zinder-explorer` reads the store as a RocksDB secondary. A secondary validates the store after every catch-up and never writes.

## Consumer contract

Most views implement `BlockKeyedConsumer`. It applies and reverts one `BlockCommitContext`, which carries the block's identity, time, parsed transaction facts, and resolved spends. The host builds each context once and shares it across consumers. `MaterializedViewConsumer` is the event-level interface for views that need only the chain event, and `MaterializedViewMempoolConsumer` receives mempool events.

Each consumer declares a `MaterializedViewConsumerSchema`:

- a stable `MaterializedViewConsumerName`, persisted in the store, so renaming one is a storage change;
- a schema version;
- the column families it owns, which no other consumer may share; and
- the single row version its reader accepts.

These rules keep every view rebuildable and reorg-safe:

- One write batch holds a consumer's rows, its view state, and its cursor advance. A crash cannot publish a cursor past the rows it describes.
- Persisted keys use codecs from `zinder-core::wire` for heights, positions, timestamps, script hashes, and outpoints.
- Reverting a block deletes exactly the rows it wrote. Height-prefixed layouts use range deletes. Other layouts keep a per-height undo index, retained for one reorg window.
- A reorg leaves the same bytes as a fresh apply of the replacement chain, however blocks were batched. A conformance test checks this for every registered chain-event consumer except the reorg-incident log, which records the reorg itself.

## Bundled consumers

`bundled_materialized_view_consumer_definitions()` is the closed list of views a release ships. The store's manifest, its cursor lists, and the replay host's dispatch all derive from it, so a registered consumer cannot be left out of replay.

The dispatch column says how a consumer receives data. "Replay" consumers follow the shared replay cursor. "Backfill join" consumers follow the same cursor but track their own historical coverage. "Snapshot" means the consumer joins after its own build publishes a snapshot.

| Consumer | Dispatch | Read by |
| --- | --- | --- |
| `block_summary` | Replay | `BlockSummariesInRange`, `BlockDetail`, `FeeSummary`, `BlockActivityDistribution`, `OverviewSnapshot` |
| `recent_transactions` | Replay | `RecentTransactions` |
| `transaction_history` | Replay | `TransactionHistory`, `OverviewSnapshot` |
| `transaction_fees` | Replay | Paid fees and input values in `TransactionDetail` and history rows |
| `transparent_outpoint_spend` | Replay | `spent_by` in `TransactionDetail` |
| `transparent_address_activity` | Replay | `TransparentAddressActivity` |
| `transparent_address_deltas` | Replay | `TransparentAddressDeltas` |
| `transparent_address_ranking` | Snapshot | `TransparentAddressRanking`, the summary in `TransparentAddressActivity` |
| `ironwood_migration` | Replay | `MigrationOverview`, `MigrationCohorts`, `MigrationDenominations` |
| `conventional_fee_distribution` | Backfill join, with an ingest backfill | `ConventionalFeeDistribution` |
| `transaction_component_summary` | Backfill join, with an ingest backfill | `TransactionComponentSummary` |
| `value_pool_flow_history` | Backfill join | The `ValuePoolFlow*` methods |
| `block_production_time` | Backfill join | `BlockProductionInTimeRange`, which the Explorer does not advertise |
| `commitment_root_search` | Backfill join | `CommitmentRootSearch`, which the Explorer does not advertise |
| `paid_fee_distribution` | Backfill join | `PaidFeeDistribution`, which the Explorer does not advertise |
| `reorg_incidents` | Chain-event envelope only | `ChainReorgHistory` |
| `value_pool_balance_history` | Backfill only | `ValuePoolBalanceHistory` |
| `mempool_event_counts` | Mempool events | `MempoolEventCounts`, `OverviewSnapshot` |

Two registered views receive no rows in this release. No runtime writes mempool events, so `mempool_event_counts` stays empty. No backfill exists for `value_pool_balance_history`, so it stays empty too.

`reorg_incidents` reads chain-event envelopes directly. When first created, it backfills from the oldest retained chain event and then keeps incidents after chain-event retention drops them. It cannot recover incidents pruned before it existed.

## Replay and coverage

Replay feeds retained chain events in order, hydrates the blocks each event names, and writes them through `MaterializedViewStore::write_chain_event`. Hydration resolves each transparent input's previous output from the blocks in hand, then from an in-memory cache of recent outputs, and last from canonical storage. After a restart partway through history, the host first warms that cache from earlier blocks. Replay also derives each block's final note-commitment roots, which `commitment_root_search` indexes, from the stored tree state and the block's commitments. No other view reads roots, so hydration skips that derivation for the transaction-component and conventional-fee views, in both their historical backfill and their startup tail seed, and for the transparent-address ranking build described below.

When the shared cursor has expired, or the consumers disagree about it, retained events cannot show whether a reorg replaced rows the views hold. Replay then rebuilds from canonical heights instead. It first reverts the reorg window below the newest materialized height, unless the last checkpointed block is still canonical.

The transparent-address ranking cannot join partway through history, because its totals are cumulative. On a store with no materialized heights, ingest activates an empty ranking at genesis and it replays with the other views. On a store whose views were built without it, ingest builds the ranking from genesis beside replay and activates it once it reaches the other views' height. Settled heights only enter the build, so a reorg cannot leave the inactive build on a replaced branch.

A cursor position is progress, not proof of completeness. Views that make completeness claims persist a `MaterializedViewState` beside their rows: the canonical epoch, the view tip, a revision, and an optional contiguous `MaterializedViewCoverage`. The Explorer reports that coverage, and advertises some capabilities only once it exists. `MaterializedViewStore::read_snapshot` binds the state and the rows to one store sequence, so a multi-read response cannot mix data from before and after a catch-up.

## Schema versions

`MATERIALIZED_VIEW_STORE_FORMAT_VERSION` versions the shared container: manifest layout, cursor encoding, and metadata families. Each consumer versions its own rows.

A fresh store records its complete manifest at creation. Every later open, primary or secondary, requires the exact consumer names, versions, column families, and physical column-family set. Any difference fails without changing the store:

- A recorded consumer this release does not ship fails with `ConsumerNotDeclared`.
- A store that records only part of the bundled set fails with `ExplorerViewManifestRequiresFreshStore`.
- A container or consumer version difference fails with a schema mismatch.

No service deletes or migrates a view store. After a release that adds, removes, or changes a view, rebuild the views on a fresh path; [Add the Explorer](../runbooks/deploying-on-a-vm.md#add-the-explorer) gives the procedure.

A consumer may add a payload field within one version only when its reader still decodes every earlier row. An older release may then misread the new rows, so rolling back requires rebuilding the views.

## Adding a consumer

1. Name the complete canonical source the view rebuilds from, and its retention limit.
2. Choose a stable consumer name and the column families it owns.
3. Add any new key codec to `zinder-core::wire`.
4. Implement apply and revert in one atomic batch, with index payloads in a deterministic order.
5. Add one registry entry declaring the schema and dispatch.
6. Persist coverage if the public method makes a completeness claim.
7. Plan the fresh-store rebuild that the schema change requires.
8. Expose the query on `ExplorerQuery` under a new capability ([Capabilities](../reference/capabilities.md)).
9. Test replay, crash recovery, secondary catch-up, and refusal on incomplete coverage. Extend the reorg conformance fixture until the consumer writes comparable rows.
