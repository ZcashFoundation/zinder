# ADR-0029: Durable transparent-outpoint spend projection

| Field | Value |
| ----- | ----- |
| Status | Accepted |
| Product | Zinder |
| Domain | Materialized-view plane, transparent spend resolution |
| Related | [ADR-0003](0003-canonical-storage-access-boundary.md), [ADR-0017](0017-materialized-view-consumer-and-key-codec.md), [ADR-0026](0026-utxo-set-commitment.md), [ADR-0028](0028-materialized-view-schema-versioning.md), [ADR-0034](0034-exclusive-index-build-stages-and-block-local-spend-replay.md), [Wallet data plane §Transparent Reverse-Spend Resolution](../architecture/wallet-data-plane.md#transparent-reverse-spend-resolution) |

## Context

Wallet offline recovery and explorer transaction detail need the transaction
that spent a transparent output, however long ago the spend happened.
Canonical storage keeps block-local facts only: each block's replay row and the
transaction-location index. It has no spend-keyed family, so answering "who
spent this outpoint" from canonical rows alone would mean scanning every later
block. Canonical storage must also stay ignorant of the materialized-view plane
([ADR-0003](0003-canonical-storage-access-boundary.md)).

## Decision

A bundled materialized-view consumer, `transparent_outpoint_spend`, records
durable spender identity for every non-coinbase transparent input, keyed by the
spent outpoint. It is Zinder's only reverse-spend index.

### Authority split

Spent-versus-unspent is decided by the wallet projection's unspent-output rows,
the set the `LtHash16` commitment covers
([ADR-0026](0026-utxo-set-commitment.md)). This projection makes only *spender
identity* durable; it never answers spentness. A missing projection row means
"no spender recorded here", never "unspent".

### The `transparent_outpoint_spend` consumer

A `BlockKeyedConsumer` ([ADR-0017](0017-materialized-view-consumer-and-key-codec.md))
keys primary rows on the spent outpoint (creating transaction id plus big-endian
output index) valued with the spending transaction id, spending block hash,
spending height, and transparent input index. A per-height index column family,
written for every applied block even when empty, drives reorg rewind and reports
the projection's durable height through `last_materialized_height_ascending`.
The consumer also persists `MaterializedViewState` in the same write batch as
its rows and chain-event cursor. Its coverage starts at the first committed
height actually materialized, advances only across a contiguous commit or
connected reorg replacement, and cannot be inferred from the latest index key.
Coinbase inputs carry no prevout and never produce a row.

Spender identity comes from the child transaction's input and mined location.
Parent-output hydration supplies value and script facts to other consumers and
is not an input to this projection.

### Recovery

The projection is rebuildable from canonical storage alone. Canonical storage
deletes no best-chain block facts, and materialized-view replay admits only
canonical history that is complete from height 1
([ADR-0034](0034-exclusive-index-build-stages-and-block-local-spend-replay.md)),
so a fresh materialized-view store replays every spend from genesis. A consumer
schema change requires such a fresh store
([ADR-0028](0028-materialized-view-schema-versioning.md)).

The materialized-view store writes unsynced. A batch lost in a host crash loses
its chain-event cursor in the same batch, so the replay host re-derives those
rows from retained canonical events or, once the cursor falls behind the event
retention floor, from canonical block history. Canonical storage never deletes
data on the strength of materialized-view rows, so no write-ahead-log barrier
orders the two stores.

### Serving

`ExplorerQuery.TransactionDetail` reads the projection from one
materialized-view snapshot. It answers only when the consumer state matches the
requested chain epoch's visible tip and the verified coverage spans from the
transaction's mined height through that tip with a matching hash; otherwise it
refuses with `ARTIFACT_UNAVAILABLE`. Under that coverage the projection has
recorded every spend since the output was created, so the handler may treat an
absent row for an output it already holds as unspent at that epoch.

`WalletQuery.TransparentSpendsByOutpoint` keeps the `contract_revision` 1
meaning under `wallet.read.transparent_spends_by_outpoint_v1`: durable spender
identity, not scoped to the reorg window. The release serving-pair query does
not implement the method and does not advertise the capability.

## Consequences

- Spender identity for a transparent outpoint is durable, so explorer
  transaction detail resolves arbitrarily old spends.
- Canonical storage carries no spend-keyed rows and no retention coupling to the
  materialized-view plane, so canonical commits never wait on projection
  progress.
- A materialized-view store can always be rebuilt from genesis-complete
  canonical history. A canonical store bootstrapped from a checkpoint cannot
  host this projection.
- The projection stores one row per transparent input plus one index row per
  block, and that storage grows with chain history.
