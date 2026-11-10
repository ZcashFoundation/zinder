# ADR-0017: Materialized-view consumer and key-codec convention

Status: Accepted
Date: 2026-05-19
Related: [ADR-0009](0009-explorer-plane-as-product-surface.md),
[ADR-0011](0011-explorer-freshness-envelope.md),
[Materialized-view plane](../architecture/materialized-view-plane.md),
[Canonical and materialized-view architecture](../architecture/canonical-materialized-view-architecture.md)

The reusable replay host can apply bundled materialized-view writes from
canonical events. The durable contract in this ADR is the shared per-block fact context,
`BlockKeyedConsumer` range dispatch convention, key codecs in
`zinder-core::wire`, and atomic materialized-view cursor persistence. Current
implementations live in `crates/zinder-materialized-views`; the `zinder-ingest`
library exposes the replay composition, but the `zinder-ingest` executable does
not start or configure it.

## Context

The explorer plane materializes column-family projections through reusable
materialized-view consumers: `BlockSummaryConsumer`, `TransactionFeesConsumer`,
`TransparentAddressActivityConsumer`, and `TransactionHistoryConsumer`.
Each consumer needs (a) typed block-header and transaction facts at the height
being applied, (b) transparent spend facts for the block's transparent inputs
when the view computes fees or address deltas, and (c) a small per-height key
encoding for whichever column-family layout the consumer chose.

Letting each consumer roll its own block-context hydration, transparent-spend
hydration, key bytes, and range-loop scaffolding would multiply boilerplate by
N and let drift between consumers compound.

## Decision

### Per-block context shared across consumers

The reusable materialized-view replay host hydrates one `BlockCommitContext` per committed
block and passes shared references to every chain-event consumer observing that
height. The context carries block identity, block time, raw block size, ordered
`TransactionFactsArtifact` rows, and hydrated `TransparentSpendFact` rows when
the view needs transparent input values. Consumers do not fetch
`WalletQuery.FullBlock`, hold `WalletQueryClient` handles, parse raw block
bytes, or resolve transparent outputs over gRPC.

### Consumer trait split

`MaterializedViewConsumer` is the SDK-facing trait the materialized-view tailer calls; it has
`apply_chain_committed` and `apply_chain_reorged`.

`BlockKeyedConsumer` is the convention every production block consumer
implements. It has three required methods:

- `name(&self) -> MaterializedViewConsumerName`
- `apply_block(&mut self, &BlockCommitContext, &mut MaterializedViewConsumerCtx<'_>)`
- `revert_block(&mut self, BlockHeight, &mut MaterializedViewConsumerCtx<'_>)`

It also has three methods with no-op defaults, which consumers override when
they keep batch-local or event-aware state:

- `begin_batch` resets a batch-local overlay before the first block of a batch.
- `finish_batch` stages shared aggregate rows after every block in the batch.
- `stage_chain_event_checkpoint` stages coverage or read fences that need the
  chain event, in the same batch as the rows.

The store's chain-event dispatch supplies the range loop through
`apply_chain_committed_in_memory` and `apply_chain_reorged_in_memory`:
a committed range walks `start_height..=end_height`, pulls each
`BlockCommitContext` from the host-provided in-memory map, and calls
`apply_block`; a reorg walks the reverted range calling `revert_block` and then
walks the replacement range calling `apply_block`. One dispatch calls
`begin_batch`, the range loop, `finish_batch`, and
`stage_chain_event_checkpoint` for each consumer, then stages every cursor in
the same write batch.

Test-only consumers that don't fit the per-block shape implement
`MaterializedViewConsumer` directly without paying the per-block scaffolding tax.

### Key codec primitives

Key encoding lives in `crates/zinder-core/src/wire/`. Each primitive is
a `pub fn encode_*` and matching `decode_*`:

- `wire::height_key`: `encode_height_key_ascending(BlockHeight) -> [u8; 4]`
  (lexicographic = oldest-first), `encode_height_key_descending` (=
  `u32::MAX - height`, lexicographic = newest-first), and decoders.
- `wire::address_script_hash`:
  `encode_address_script_hash(TransparentAddressScriptHash) -> [u8; 32]`
  and decoder.
- `wire::in_block_position`:
  `encode_in_block_position(u32) -> [u8; 4]` for the per-block tx
  position component of composite keys.
- `wire::unix_seconds`:
  `encode_unix_seconds(u64) -> [u8; 8]` for time-bucketed projections.

Composite keys are concatenations of the above plus per-consumer
trailing tags. The conventions actually in use:

- per-block ascending: `[height_key_ascending(4)]` (`BlockSummary`,
  the `transaction_fees_index` family, the
  `transparent_address_activity_index` family)
- per-tx by id: `[transaction_id_internal(32)]`
  (`TransactionFeesConsumer` primary records)
- per-block-position descending: `[height_key_descending(4) | in_block_position(4)]`
  (`TransactionHistoryConsumer`)
- per-address descending: `[address_script_hash(32) | height_key_descending(4) | in_block_position(4)]`
  (`TransparentAddressActivityConsumer` primary records)
- per-second: `[unix_seconds(8)]`
  (`MempoolEventCountsConsumer`)

A consumer that needs a new shape adds the primitive to `wire/` and
documents the layout here, rather than encoding bytes inline. Inline
`.to_be_bytes()` on heights, positions, addresses, or unix seconds at
materialized-view boundaries is a forbidden pattern.

### Freshness helpers

`MaterializedViewStore` ships two helpers paired with the height-key primitives:

- `last_materialized_height_ascending(cf)` — decodes the last key as
  four-byte big-endian ascending height. Used by `BlockSummaryConsumer`
  and the explorer block-view handler.
- `last_materialized_height_descending(cf)` — decodes the first key
  (lowest reverse-height bytes correspond to highest height) as
  descending-height bytes. Used by every consumer that keys on
  reverse-height as the primary discriminator.

A consumer whose freshness signal is not chain-height (a mempool-driven
consumer, for example) defines its own helper on the store rather than
decoding bytes inline in a handler.

### Per-consumer schema versioning

`MaterializedViewConsumerSchema` binds a consumer's stable
`MaterializedViewConsumerName` to a `schema_version` and the column families it
owns. Requiring the version at construction makes it impossible to register a
column family without one. Every consumer starts at version 1.

Registration of a bundled consumer is one `MaterializedViewConsumerDefinition`
entry in the closed registry returned by
`bundled_materialized_view_consumer_definitions()`. The entry declares the
schema, product role, preset membership, and a `MaterializedViewDispatch` that
says how the replay host constructs and drives the consumer:

- `ChainEventBlocks` with a `ChainEventBlockJoin` of `Replay`,
  `CanonicalBackfill`, or `CanonicalSnapshot` (activation-gated);
- `ChainEventEnvelope` for consumers that read only the event envelope;
- `MempoolEvents`, which no bundled runtime drives yet; or
- `CanonicalBackfillOnly` for consumers that never receive events.

The recovery source is derived from the dispatch rather than declared
separately. `MaterializedViewStore::bundled_consumers()`, the preset schema
lists, the chain-event and event-only cursor lists, and the replay host's
dispatch, cursor-agreement, and backfill-owned cursor sets all derive from
the registry. Both enums are closed, so the replay host's exhaustive matches stop
compiling when a dispatch shape is added without handling it.

The persisted per-consumer manifest, exact-version admission policy, and
container-format version are defined in
[ADR-0028](0028-materialized-view-schema-versioning.md).

## Rewind contract

Every chain-events consumer implements `revert_block(height, ctx)`. The
shape of the implementation depends on the key layout:

- **Per-height ascending key** (`BlockSummary`): one `delete_cf` per
  height. `apply_chain_reorged_in_memory` walks the reverted range
  and calls `revert_block` per height.
- **Per-block-position descending composite**
  (`TransactionHistoryConsumer`): one `delete_range_cf` per height over
  the 4-byte `height_key_descending` prefix.
- **Composite where height is NOT a prefix**
  (`TransparentAddressActivityConsumer`,
  `TransactionFeesConsumer`): the consumer maintains a per-height
  *index* column family keyed by ascending height whose value is the
  concatenated list of secondary discriminators it wrote at that height
  (txids for fees, `(address, in_block_position)` pairs for
  address activity). On revert it reads the index for the reverted
  height, deletes each primary row by the reconstructed composite key,
  then deletes the index entry. All deletes go in the same batch as the
  cursor advance.

The two-CF index pattern is what makes rewind correct under reorg: the
canonical block at the reverted height has changed, so re-fetching from
the wallet would return the new block, not the one whose rows we
actually wrote. The persisted index captures what was actually
materialized at apply time.

Revert is held to path independence rather than to restoring an earlier
snapshot: after applying `A[1..n]` and reorging `A[k..n]` onto `B[k..m]`, every
column family a consumer declares must hold the same bytes as a fresh store
that applied `B[1..m]`, however the blocks were grouped into batches. The
reorg conformance test in `zinder-materialized-views` runs every registry
entry through that check, and back onto `A`, with a declared list of
path-dependent column families and the reason for each. Index payloads and undo
journals therefore encode their entries in a deterministic order and never
carry values that depend on batch grouping.

## Consequences

- A new consumer that fits the per-block shape implements the three
  required `BlockKeyedConsumer` methods plus a key codec and an error enum,
  and adds one registry entry. The store's dispatch gives it the range-loop
  scaffolding, and the replay host picks it up from the registry.
- A new wire-key shape requires one primitive in
  `crates/zinder-core/src/wire/`. Subsequent consumers reuse it.
- `wire_invariants.rs` extends with bans for inline
  `.to_be_bytes()` on materialized-view key fields when the temptation
  arises.
- Shared `BlockCommitContext` values bound materialized-view replay cost to one canonical
  fact hydration pass and one transparent-spend hydration pass per height, even
  as the consumer count grows.
- Composite-key consumers that don't have a height prefix accept the
  index-CF overhead (a few dozen bytes per row at apply time) in
  exchange for correct, single-batch rewinds.

## Revision history

- 2026-11-10: Registration moved to the closed bundled-consumer registry, which
  also declares dispatch. The trait description now lists all six
  `BlockKeyedConsumer` methods and the store dispatch functions that supply the
  range loop. Revert is specified as reorg path independence.
