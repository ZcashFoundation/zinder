# Explorer Plane

The explorer plane is implemented by the optional `zinder-explorer` workspace
service. The service compiles and is tested with the workspace, but the release
workflow does not build or publish an explorer image. Operators that need the
native `ExplorerQuery` surface can build its local image through the optional
Compose overlay.

The explorer plane is the Zinder product surface for block explorers, dashboards, and analytics consumers. It serves UI-ready, API-ready, and agent-ready views over canonical chain artifacts and replayable event streams, with explicit freshness, typed unavailability, and capability-gated panels. It is owned by `zinder-explorer`.

This document defines the boundary, wire vocabulary, capability namespace, freshness contract, and the rule that distinguishes explorer views from wallet views. It is the sibling document to [Wallet data plane](wallet-data-plane.md) and [Materialized-view plane](materialized-view-plane.md). The explorer plane exercises the materialized-view SDK; the materialized-view plane defines that SDK.

## Purpose

Wallets and explorers ask different questions. A wallet asks "did anything I care about happen?" and follows the chain by height. An explorer asks "what happened in this block, what is the status of this transaction, what is happening in the mempool right now, is this indexer fresh enough to trust." Those questions need typed responses with freshness metadata, capability strings, and stable pagination. They do not need raw compact blocks.

`ExplorerQuery` is the read surface for those questions. It owns block summaries, transaction details, typed search, mempool summaries, transparent address activity, fee summaries, value-pool summaries, and explorer freshness. `WalletQuery` keeps the wallet-correctness primitives.

The decisions that govern this plane:

- [ADR-0009](../adrs/0009-explorer-plane-as-product-surface.md) — service topology, capability namespace, dual-capability federation rule.
- [ADR-0010](../adrs/0010-transaction-public-facts.md) — single transaction parser feeding ingest, mempool, and explorer consumers.
- [ADR-0011](../adrs/0011-explorer-freshness-envelope.md) — freshness envelope embedded on every explorer response.
- [ADR-0012](../adrs/0012-typed-explorer-search-and-privacy-refusal.md) — typed search and structured privacy refusal.

## Boundary

`zinder-explorer` is an optional runtime over the shared query and storage
libraries. It:

- **Consumes** the explorer materialized-view store through a process-owned RocksDB secondary when available, plus `WalletQuery` over gRPC for federated read paths.
- **Owns** no primary RocksDB. `storage.path` identifies the canonical root whose `materialized-views` subdirectory ingest owns, while `storage.secondary_path/materialized-views` holds the explorer's secondary metadata. The explorer does not open the canonical store.
- **Produces** the `ExplorerQuery` gRPC service.
- **Does not** open any primary store, custody wallet secrets, or serve wallet balance RPCs. It may parse transaction bytes through `zinder-source` and attach an optional cached upstream-health observation to freshness; neither is an authoritative chain-fact read.

The boundary rules:

- A `zinder-explorer` crash does not stop ingest or wallet sync. The wallet plane keeps serving every `WalletQuery` primitive.
- The explorer never advertises a capability whose policy requires retained canonical facts. The methods that need canonical-store reads still exist on the wire: `BlockTransactions`, `BlockProductionSeries`, `BlockProductionInTimeRange`, `CommitmentRootSearch`, `PaidFeeDistribution`, `DisplacedBlockHistory`, and `DisplacedBlockDetail`. Each one fails with `FAILED_PRECONDITION` and the `DEPENDENCY_NOT_CONFIGURED` reason. A method that also reads materialized views first reports a missing materialized-view store with the same reason, and an attached store that lacks the exact Explorer view manifest with `UNIMPLEMENTED` and the `EXPLORER_METHOD_DISABLED` reason.
- The explorer plane never extends canonical artifact schemas. When a view needs an authoritative chain fact the canonical surface does not carry, the source boundary extends first, the canonical artifact or event gains the field, then the explorer subscribes.
- Server-side shielded address scanning, persisted viewing keys, and memo decryption are out of scope by product invariant.

## Wire surface

The native gRPC service is `ExplorerQuery` in `zinder.v1.explorer`. The service includes:

```proto
service ExplorerQuery {
  rpc ServerInfo(ServerInfoRequest) returns (ServerInfoResponse);

  rpc TransactionDetail(TransactionDetailRequest)
      returns (TransactionDetailResponse);
}
```

The same service also owns `BlockSummariesInRange`, `BlockDetail`, `Search`, `TransparentAddressActivity`, `MempoolSummary`, `MempoolSnapshot`, `MempoolActivity`, `FeeSummary`, and `ValuePoolSummary`. Every method follows the same shape rules:

- Response message field tag 1 is `ExplorerFreshness freshness` ([ADR-0011](../adrs/0011-explorer-freshness-envelope.md)).
- Streaming responses are chunked; each chunk carries its own `ExplorerFreshness` and an opaque `cursor: bytes`.
- Paginated requests accept `from_cursor: bytes` plus `max_entries: uint32`.
- Fields use unit suffixes per [Public interfaces §Method Naming Conventions](public-interfaces.md#method-naming-conventions): `_zat`, `_zec`, `_height`, `_count`, `_bytes`, `_millis`, `_seconds`.

## Transaction detail shape

`ExplorerQuery.TransactionDetail` resolves one transaction at an epoch-pinned wallet location. `WalletQuery.Transaction` supplies the mined or mempool location and retained raw transaction bytes. The explorer parses those bytes through the same `TransactionPublicFactSet` parser ingest uses and rejects a transaction-id mismatch. Mined inputs combine intrinsic outpoints with values projected by `TransactionFeesConsumer`. Their scripts stay absent, because the explorer does not read retained parent facts. Mined outputs carry exact intrinsic values and scripts, while `TransparentOutpointSpendConsumer` supplies canonical `spent_by` relations only after its state and historical coverage match the complete requested epoch. A missing or stale projection fails closed rather than turning unknown spent state into “unspent.” Mempool rows expose intrinsic inputs and outputs without claiming canonical parent resolution, canonical spent state, or paid fees.

Reverse-spend lookup remains epoch-pinned and bounded. Standard-address decoding is an edge concern, and shielded values are not implied. `intrinsic_value_balances` stays absent, and the explorer does not advertise `explorer.transaction.intrinsic_value_balances_v1`. The handler never writes materialized-view rows.

`TransactionFeesConsumer` materializes `paid_fee_zat` only when every
transparent prevout resolves and the canonical privacy shape is
`TransparentOnly`. A transparent delta inside a shielding or mixed transaction
is a transfer between pools, not a provable fee; shielded and unclassified rows
therefore retain resolved input values but leave `paid_fee_zat` absent.

An incompatible transaction-fee consumer layout requires a fresh
materialized-view store rebuilt from canonical history. Every read requires an independently
classified privacy shape and suppresses a paid-fee value unless that shape is
`TransparentOnly`. Transaction detail, recent transactions, and transaction
history read paid fees only from these projected rows. A missing or partial
fee row leaves `paid_fee_zat` absent. Readers never write materialized-view
rows.

Mempool transactions retain their location semantics. Mempool rows expose transaction-intrinsic transparent facts from their transient payload, but do not claim canonical parent resolution, canonical spent state, or actual paid fees.

The response composes parsed intrinsic facts with a durable reverse-spend relation. A spender remains visible beyond the first request batch, while epoch-pinned reads report the same outpoint as unspent before its spending epoch and spent afterwards.

## Block view shape

The block listing and block-detail surfaces share one materialized view: a `BlockSummaryRecord` per canonical block, keyed by big-endian block height. The wire shape splits read concerns across two RPCs without duplicating storage.

`BlockSummary` carries the per-block facts a listing page renders:

```proto
message BlockSummary {
  uint32 block_height = 1;
  string block_hash = 2;                 // RPC byte order, 64 lowercase hex chars
  int64 block_time_unix_seconds = 3;
  uint32 transaction_count = 4;          // includes coinbase
  string previous_block_hash = 5;        // RPC byte order, 64 lowercase hex chars
}
```

`ExplorerQuery.BlockSummariesInRange` returns a range of `BlockSummary` rows ordered by ascending height. The handler reads the materialized record from the consumer store, projects the summary fields, and skips the transaction-id payload so the wire response stays cheap on long ranges.

`ExplorerQuery.BlockDetail` resolves either a height or a hash to one `BlockSummary` plus the canonical-ordered list of transaction ids. It is the low-payload read for clients that only need block identity or transaction ids.

`ExplorerQuery.BlockTransactions`, `ExplorerQuery.BlockProductionSeries`, and
`ExplorerQuery.BlockProductionInTimeRange` join block summaries to canonical
transaction facts or headers. The explorer does not read the canonical store,
so these methods fail as described under [Boundary](#boundary). The
`BlockProductionTimeConsumer` time index from
[ADR-0033](../adrs/0033-time-indexed-block-production.md) is still
materialized.

The block-summary record is the base of every block view. `BlockDetail`
performs one RocksDB get.

Reorg rewind deletes every record in the reverted height range and re-fetches the replacement range before committing the cursor advance, so the view never advertises a stale BlockSummary for a height that no longer maps to the canonical chain.

## Block activity distribution

`ExplorerQuery.BlockActivityDistribution` is a bounded request-time aggregate
over the existing `BlockSummaryRecord` rows. The request names an inclusive
height range; the server caps that range at 20,000 blocks and returns the
requested bounds, materialized-row count, missing-row count, first and last
observed block times, total transaction count, and a complete 168-cell
weekday/hour grid. Weekdays use the explicit `Sunday = 0` order;
zero-valued cells are emitted so clients never need to infer absent activity
from an omitted bucket.

The response freshness describes the current `WalletQuery` chain view observed
while serving the read. It is not a historical `ChainEpoch` snapshot, and the
explicit coverage fields prevent a partial local materialized-view range from being
presented as complete activity history. This contract adds no materialized-view consumer,
column family, schema version, or replay requirement. A product that needs
unbounded historical activity must add a dedicated durable materialized view with its
own reorg, retention, and backfill semantics.

## Search shape

`ExplorerQuery.Search` accepts a raw user input string and returns a typed `SearchResponse` carrying zero or more `SearchCandidate` arms in confidence order. The classifier lives in `crates/zinder-core/src/explorer_search.rs` and is a pure function of `(query, network)`; it never touches the canonical store, the wallet plane, or the network. The handler in `services/zinder-explorer/src/grpc/search.rs` composes the classifier output with optional `WalletQuery` confirmations:

- Numeric input routes through `WalletQuery.BlockIdBySelector(height)` to confirm the block exists at that height before emitting `BlockMatch`.
- 64-character hex input routes through both `BlockIdBySelector(hash)` and `Transaction(hash)`; whichever resolves emits a candidate. If both resolve, each candidate carries `confidence = 0.5`; otherwise the single winner carries `confidence = 1.0`.
- Transparent (`t*/tm*`, `t3*/t2*`), ZIP-320 TEX (`tex*/textest*`), and ZIP-316 unified (`u*/utest*/uregtest*`) addresses decode locally via `zcash_address`; transparent and unified-with-transparent-receivers candidates emit at full confidence without storage probes.
- Sapling and Sprout shielded inputs (`zs*/ztestsapling*/zc*`) route to the typed `NotPubliclyIndexable` arm with `reason = NOT_PUBLICLY_INDEXABLE_REASON_SHIELDED_ADDRESS`; viewing keys (`uivk*/uview*/zxviews*/zviews*`) route to the typed `NotPubliclyIndexable` arm with `reason = NOT_PUBLICLY_INDEXABLE_REASON_VIEWING_KEY` and the `canonical_form` field omitted.
- Unknown ZIP-316 receiver typecodes inside a unified address surface as `UnifiedAddressReceiverKind::UNKNOWN` with a typed `NotPubliclyIndexable` body; the enclosing unified-address arm still routes any recognized transparent receivers normally.
- Empty, oversized, and unparseable inputs route to `UnclassifiedMatch` with an operator-readable hint.

The classifier short-circuits shielded forms before the handler issues any `WalletQuery` call, which is the structural invariant required by [ADR-0012](../adrs/0012-typed-explorer-search-and-privacy-refusal.md). The handler's only universal wallet call is `WalletQuery.VisibleTipBlock`, issued once at the end to build the `ExplorerFreshness` envelope (a property of every explorer response, not the search candidate).

Autocomplete indexes are separate materialized views and do not gate the `explorer.search_v1` capability; the local classifier is the capability's correctness boundary.

## Mempool views

The mempool surface is three request-time views over `WalletQuery.MempoolSnapshot`. None requires a materialized-view consumer: upstream snapshot reads are bounded by the hard cap of 4,096 entries, and every parsed entry uses `zinder_source::parse_transaction_public_fact_set` so privacy-shape and version classifiers stay in lockstep with `TransactionDetail`.

`ExplorerQuery.MempoolSummary` returns one aggregated page:

```proto
message MempoolSummaryResponse {
  ExplorerFreshness freshness = 1;
  uint32 transaction_count = 2;
  uint64 total_size_bytes = 3;
  repeated PrivacyShapeCount privacy_shape_distribution = 4;
  repeated TransactionVersionCount version_distribution = 5;
  uint64 oldest_entry_age_millis = 6;
  uint64 newest_entry_age_millis = 7;
}
```

The age fields are wall-clock deltas computed at response time against the entry's `first_seen_unix_millis`; they are zero when the snapshot is empty.

`ExplorerQuery.MempoolSnapshot` is the coherent page-ready view. It returns a `MempoolSnapshotSummary`, one bounded page of `MempoolActivityEntry` rows, and the usual opaque cursor from one `WalletQuery.MempoolSnapshot` response. Its summary and entries therefore cannot straddle a mine, eviction, or newly observed transaction. Consumers that display global statistics beside current rows must use this capability instead of combining `MempoolSummary` and `MempoolActivity` from separate requests.

`ExplorerQuery.MempoolActivity` paginates the same snapshot into typed entry rows sorted by newest-first observation time. Each row includes the parsed component counts and the sum of its transparent outputs in zatoshis. Both values are derived from the in-memory `MempoolEntry` at request time, so this surface neither requires a durable materialized view nor changes the chain-store schema. The cursor is opaque: 12 bytes packing `(first_seen_unix_millis, transaction_id_tail_4_bytes)` big-endian. Mempool state is transient, so subsequent pages may interleave with new arrivals; clients that need a consistent paged read should treat any single response as a snapshot and re-pin if needed.

## Transparent-address activity

`ExplorerQuery.TransparentAddressActivity` v2 is the single confirmed-address
read for explorer pages and compatibility adapters. It combines two
product-neutral sources without adding another materialized view:

1. `TransparentAddressActivityConsumer` supplies newest-first transaction rows
   and the opaque cursor. A bounded `offset` is also available for page-number
   adapters and is mutually exclusive with the cursor.
2. The atomically active `TransparentAddressRanking` generation supplies the
   current confirmed balance, lifetime received and sent totals, exact distinct
   transaction count, first and last activity timestamps, and explicit
   coverage. A valid address absent from the generation returns a typed zero
   summary rather than a not-found error.

Rows leave the canonical enrichment fields absent: transaction index, size,
component counts, address-specific input and output values, and scripts of
other transparent inputs and outputs. `input_facts_complete` is false, so a
client never reads a partial input sum as complete.

The handler pins its epoch through `WalletQuery.VisibleTipBlock` and answers
as of that epoch's visible tip. Ingest replays a block into the views before
the wallet query publishes it, so the ranking is often a few blocks ahead of
the epoch. The summary and coverage then come from the ranking's per-height
undo journals, which keep each touched address's summary from before every
applied block, and activity rows above the visible tip are left out. One
materialized-view snapshot holds both reads, so a block the views apply during
the request cannot split them. A ranking that has not reached the epoch's tip
answers from its current rows, and its coverage shows how far they reach.

The read fails with `FAILED_PRECONDITION` and the
`EXPLORER_PRECONDITION_UNSATISFIED` reason when the ranking applied a different
block at the epoch's tip height, as during a reorg only one side has seen. It
fails the same way when answering would rewind more than 1,000 blocks
(`TRANSPARENT_ADDRESS_RANKING_MAX_REWIND_BLOCKS`) or below the generation's
snapshot base. A retry at a newer epoch can succeed. `ExplorerFreshness` carries
the shared materialized-view indexed tip, including its hash, so adapters can
mark a same-height mismatch as degraded.

Offset paging and summary composition are request-time operations over the
activity and ranking materialized views. Mempool address activity remains a
separate surface, so confirmed pagination stays deterministic.

## Transparent-address deltas

`ExplorerQuery.TransparentAddressDeltas` is the per-event counterpart of the activity feed. Where the activity surface returns one net row per transaction, the deltas surface returns one row per received output and per resolved spend, ordered ascending by height for `getaddressdeltas` parity. Each entry carries `transaction_id`, `block_height`, `block_time_unix_seconds`, an `index` (the output index for a received output, the input index for a spent prevout), a signed `value_zat` (positive for a receive, negative for a spend), and an explicit `kind` (`RECEIVED` or `SPENT`) so a reader never infers direction from the sign alone.

Both surfaces fold the same per-event attribution. `TransparentAddressDeltasConsumer` and `TransparentAddressActivityConsumer` each call one shared decomposition that turns a block into per-address value events; the deltas consumer persists every event while the activity consumer sums the events for one `(address, transaction)` into `net_value_zat`. The net activity for any address and range therefore equals the sum of the deltas over the same range.

Received-output events are always exact. Spend events carry `spent_value_zat` from the canonical spend fact, so they need no prevout re-resolution at read time. A spend whose prevout cannot be resolved (or when transparent-spend hydration is off) produces no delta event rather than a wrong number; the activity surface's `prevout_resolution_status` reports the same partial state for that transaction. The range read is paged with the standard opaque cursor and capped at 256 rows per page.

## Transparent-address ranking

`ExplorerQuery.TransparentAddressRanking` returns positive-balance standard
transparent scripts ordered by `(balance_zat descending, script hash
ascending)`. The native boundary returns raw `script_pub_key` bytes because
network-specific address encoding belongs to clients and adapters. Pages are
offset-based, capped at 500 rows, and include the positive-address count,
positive-balance total, top-10 and top-100 totals, and explicit balance and
lifetime-history coverage. The response also carries ordered P2PKH and P2SH
aggregate rows. Their counts and balances must sum exactly to the generation
totals, so consumers can distinguish standard script templates without scanning
or decoding every ranked row.

The method is available only while an active materialized generation exists.
Its `ExplorerFreshness` must describe the same materialized-view cursor and canonical epoch
as that generation; an in-progress replacement generation is never visible.
This gives dashboards and agents a stable rank snapshot while ingest builds or
resumes an inactive generation.

Ingest publishes the generation without an operator step. A fresh Explorer
views build activates it at genesis, and its coverage then advances with the
shared replay like every other view's. A store whose views were built without
the ranking builds it from genesis in the background and activates it at the
shared replay cursor. [Publishing the canonical
snapshot](materialized-view-plane.md#publishing-the-canonical-snapshot)
describes both paths.

## Fee summary

`ExplorerQuery.FeeSummary` aggregates per-transaction [ZIP-317](https://zips.z.cash/zip-0317) conventional fee floors across an inclusive block range. The fee fields are the ZIP-317 floor `MARGINAL_FEE × max(logical_actions, GRACE_ACTIONS)`, not miner-collected fees: computing actual fees requires the value of every transparent input, and that resolution is intentionally out of scope for `v1`. The conventional-fee floor is the minimum a wallet should attach to a transaction with the given shape; aggregates over many blocks give an explorer page a useful approximation of fee floors without prevout resolution.

`logical_actions = max(transparent_input_count, transparent_output_count, max(sapling_spend_count, sapling_output_count), orchard_action_count) + ironwood_action_count`. The materialized-view plane computes component counts from `TransactionFactsArtifact` once and materializes per-block fee aggregates in `BlockSummaryRecord`. The handler scans those typed records; it does not request raw blocks or parse `zebra-chain` bytes on the read path. The fee helper lives on `zinder_core::TransactionComponentCounts::zip317_conventional_fee_zat` so the same formula is reusable from any handler that builds the count shape. The range cap is 256 blocks per request; coinbase transactions are excluded because they have no fee.

## Conventional fee distribution

`ExplorerQuery.ConventionalFeeDistribution` returns exact, sorted frequency
counts for ZIP-317 conventional fees grouped by UTC day over a half-open block
time range. It deliberately does not return percentiles, chart buckets, or
compatibility-adapter field names. Those are consumer policies derived losslessly from
the native frequencies. Coinbase transactions are excluded, zero-count rows
are absent, and transactions whose complete component shape is unavailable
are reported separately rather than silently omitted.

The response carries `ExplorerFreshness` and independent contiguous coverage.
The capability is advertised only after materialized-view coverage exists. A range is
complete only when its start is covered and the materialized view reaches the visible
tip or its time boundary. Full UTC days use aggregate rows; clipped boundary
days scan retained per-block contributions. This keeps 365-day reads bounded
by daily rows while preserving exact rolling-cutoff semantics.

## Value pool summary

`ExplorerQuery.ValuePoolSummary` wraps `WalletQuery.ChainValuePoolsAtTip` in the standard `ExplorerFreshness` envelope. It does not call upstream nodes directly and it does not project pool ids into fixed response fields. The response carries `source_tip: BlockTip` plus `repeated ChainValuePool pools`, preserving the height/hash identity and pool list from the same upstream observation. Existing UI can render known ids while additional consensus pools remain visible without a new explorer wire shape.

`ValuePoolSummary` reports the upstream node's `getblockchaininfo.valuePools` totals; it is not a Zinder-computed UTXO accounting. The explorer has no chain-wide transparent UTXO-set method ([ADR-0043](../adrs/0043-owned-transparent-wallet-query-surface.md)).

`ValuePoolSummary` is only the current source snapshot. Value-pool history and value-pool flow are outside this contract because cumulative balances and transaction movement have different inputs, coverage, and replay behavior.

## Transaction component summary

`ExplorerQuery.TransactionComponentSummary` returns exact component totals and
UTC-day rows for a half-open block-time range. The product-neutral fields
include transparent inputs and outputs, Sapling spends and outputs, Orchard
and Ironwood actions, Sprout JoinSplits, protocol transaction counts, and
explicitly named Sapling/Orchard classifications. Separate neutral
predicate totals use protocol-scoped identifiers, including
`sapling_orchard_or_ironwood_transaction_count` and three explicitly named
non-coinbase predicates. Unsupported
sections increment `transaction_predicate_unavailable_count` and contribute to
none of those predicates, so clients must require an unavailable count of zero
before claiming exact predicate totals. `totals_only` defaults to false and
returns UTC-day rows; true omits the rows.

The response carries the current `ChainEpoch` in `ExplorerFreshness` and a
separate contiguous coverage envelope. `requested_range_complete` is true only
after height-1 historical coverage has joined the cursor-seeded live tail and
reaches the visible tip. This deliberately conservative rule accounts for
non-monotonic block timestamps: the timestamp at the last indexed height alone
cannot prove that a missing later height falls outside the requested range.
Checkpoint-based stores therefore never claim full-chain completeness.

## Overview snapshot optional fields

`ExplorerQuery.OverviewSnapshot` requires an admitted wallet tip to anchor its coherent bundle, while its `mempool` and `value_pools` fields are optional. When WalletQuery returns the typed `ENDPOINT_CAPABILITY_UNAVAILABLE` precondition for the exact field capability, `mempool` stays absent or `value_pools` stays empty and `freshness.unavailable` records the corresponding `mempool` or `value_pools` field path with `UNAVAILABLE_UPSTREAM_NOT_SUPPORTED`. The unavailable marker distinguishes structural absence from an observed empty mempool or value-pool list. Transient upstream failures and preconditions whose structured reason, domain, violation type, or capability subject do not match continue to fail the Overview request.

## Capability namespace

The explorer plane uses the `explorer.*` capability prefix. The full namespace structure:

| Capability | Owner method | Always-on? |
| ---------- | ------------ | ---------- |
| `explorer.server_info_v1` | `ExplorerQuery.ServerInfo` | Yes |
| `explorer.transaction.detail_v4` | `ExplorerQuery.TransactionDetail` | When the wallet endpoint and explorer materialized-view store are configured |
| `explorer.block.summary_v1` | `ExplorerQuery.BlockSummariesInRange` + `BlockDetail` summary part | When the block-summary consumer is built and caught up |
| `explorer.block.production_series_v2` | `ExplorerQuery.BlockProductionSeries` | Never; requires canonical-store reads |
| `explorer.block.production_time_range_v1` | `ExplorerQuery.BlockProductionInTimeRange` | Never; requires canonical-store reads |
| `explorer.block.detail_v1` | `ExplorerQuery.BlockDetail` per-tx rows | When the block-detail consumer is built and caught up |
| `explorer.block.activity_distribution_v1` | `ExplorerQuery.BlockActivityDistribution` | When the block-summary consumer and wallet endpoint are available |
| `explorer.transparent_address.activity_v1` | `ExplorerQuery.TransparentAddressActivity` | When the wallet endpoint is configured |
| `explorer.transparent_address.activity_v2` | `ExplorerQuery.TransparentAddressActivity` | When the wallet endpoint plus explorer activity and ranking consumers are available, and the ranking has an active generation |
| `explorer.transparent_address.deltas_v1` | `ExplorerQuery.TransparentAddressDeltas` | When the wallet endpoint is configured |
| `explorer.transparent_address.ranking_v1` | `ExplorerQuery.TransparentAddressRanking` | When the ranking consumer has an active generation and the wallet endpoint is configured |
| `explorer.mempool.summary_v1` | `ExplorerQuery.MempoolSummary` | When the wallet endpoint is configured |
| `explorer.mempool.snapshot_v1` | `ExplorerQuery.MempoolSnapshot` | When the wallet endpoint is configured |
| `explorer.mempool.activity_v1` | `ExplorerQuery.MempoolActivity` | When the wallet endpoint is configured |
| `explorer.fee.summary_v1` | `ExplorerQuery.FeeSummary` | When the wallet endpoint is configured |
| `explorer.fee.conventional_distribution_v1` | `ExplorerQuery.ConventionalFeeDistribution` | When the conventional-fee materialized view has contiguous coverage and the wallet endpoint is configured |
| `explorer.fee.paid_distribution_v1` | `ExplorerQuery.PaidFeeDistribution` | Never; requires canonical-store reads |
| `explorer.value_pool.summary_v1` | `ExplorerQuery.ValuePoolSummary` | When the wallet endpoint is configured and `WalletQuery.ChainValuePoolsAtTip` is available |
| `explorer.transaction.component_summary_v2` | `ExplorerQuery.TransactionComponentSummary` | When the transaction-component consumer and wallet endpoint are available |
| `explorer.search_v1` | `ExplorerQuery.Search` | When the wallet endpoint is configured |

The naming follows `explorer.<noun>.<capability>_v{N}`. The noun is a domain category; the capability is the operation. New methods add new capability strings; wire-shape changes ship as `_vN` increments.

Every `explorer.*` capability is served by the explorer plane itself; clients reach these methods on `ExplorerQuery`, never through `WalletQuery`. Materialized-view consumers surfaced through the wallet client follow the federation rule in [ADR-0009](../adrs/0009-explorer-plane-as-product-surface.md): the capability lives in the consumer's product namespace, and the wallet plane advertises it only while the consumer's proxy is ready.

## Freshness envelope

Every explorer response embeds `ExplorerFreshness` at field tag 1. The shape and rationale live in [ADR-0011](../adrs/0011-explorer-freshness-envelope.md). The key fields:

- `chain_view`: the cross-plane chain-state envelope (chain epoch, the `{role}_tip` axes, materialized-view status). Identifies the snapshot the response was produced from. The upstream tip rides on `chain_view.upstream_tip`; the materialized-view replay ceiling on `chain_view.indexed_tip`. Index lag is `chain_view.chain_epoch.visible_tip.height - chain_view.indexed_tip.tip.height`.
- `snapshot_age_millis`: milliseconds since the current mempool source generation was certified, including a certified empty mempool; zero when the response does not touch mempool state.
- `capability_version`: exact capability string that produced the response.
- `unavailable`: repeated `UnavailableField` entries declaring specific field paths absent with structured reasons.

`UnavailableField` carries a `field_path` (dotted-path matching the response shape), a structured `reason` (enum), and a `human_reason` string from the canonical registry in `crates/zinder-core/src/explorer_reasons.rs`. Frontends can branch on `reason` or render `human_reason` verbatim; both come from the same source so the words match across surfaces.

## Privacy boundary

The explorer plane is a privacy surface. The non-negotiable rules:

- Search for a shielded address, viewing key, or unified-address shielded receiver returns the typed `NotPubliclyIndexable` arm per [ADR-0012](../adrs/0012-typed-explorer-search-and-privacy-refusal.md). The arm carries a canonical reason string. Empty match lists for shielded inputs are forbidden.
- The classifier never reaches storage for shielded inputs. A privacy regression test enforces this with a mock that records storage call counts.
- The explorer plane never receives viewing keys, spending keys, or seed phrases over any RPC. Search inputs that classify as viewing keys are echoed back only in their typed `NotPubliclyIndexable` form; the `canonical_form` field is omitted for viewing keys to avoid logging-layer leaks.
- Server-side shielded scanning is out of scope. The explorer plane does not implement, persist, or expose any shielded-address indexing.

The wallet plane's privacy invariants ([ADR-0005](../adrs/0005-consumer-neutral-wallet-data-plane.md)) apply identically to the explorer plane; the explorer adds the typed refusal vocabulary so a refusal is a structured response, not an error.

## Operator surface

`zinder-explorer` ships standard ops endpoints (`/healthz`, `/readyz`, `/metrics`) on the shared `[ops]` listener (default `127.0.0.1:9069` for the explorer). Prometheus metrics use the `zinder_explorer_*` prefix.

Configuration follows the canonical TOML conventions:

```toml
[ops]
listen_addr = "127.0.0.1:9069"   # shared section; "" disables the endpoint

[storage]
path = "/var/lib/zinder/store"
secondary_path = "/var/lib/zinder/explorer-secondary"

[explorer]
listen_addr = "127.0.0.1:9068"
bearer_token_path = "/run/secrets/zinder-explorer-token"
wallet_query_endpoint = "https://zinder.example:9102"   # optional zinder-query gRPC endpoint
```

`/readyz` refuses traffic while the latest materialized-view secondary
catch-up failed: `schema_mismatch` when the primary's store or consumer
identity no longer matches the binary, `storage_unavailable` otherwise. The
next successful catch-up restores readiness. Index lag behind the chain is a
per-response property carried in `ExplorerFreshness`, not a readiness cause.
See [Service operations](service-operations.md#explorer).

When `explorer.bearer_token_path` is set, the `ExplorerQuery` gRPC endpoint enforces the same shared-secret bearer-token interceptor as `IngestControl` per [ADR-0006](../adrs/0006-ingest-control-transport-security.md). The explorer's `wallet_query_endpoint` config points to an optional `zinder-query` endpoint for its wallet-composed reads (transaction detail, block views, search, mempool activity, and value pools).

Environment-variable mapping uses the `ZINDER_EXPLORER__*` prefix for explorer-specific fields, plus the shared `ZINDER_OPS__*` prefix for the universal operational endpoint:

- `ZINDER_EXPLORER__LISTEN_ADDR`
- `ZINDER_EXPLORER__BEARER_TOKEN_PATH`
- `ZINDER_EXPLORER__WALLET_QUERY_ENDPOINT`
- `ZINDER_STORAGE__PATH`
- `ZINDER_STORAGE__SECONDARY_PATH`
- `ZINDER_OPS__LISTEN_ADDR` (shared with every Zinder binary; default `127.0.0.1:9069` for the explorer)

## Failure isolation

The explorer plane fails independently from canonical state.

- An explorer service crash does not stop `zinder-ingest`. Ingest continues writing canonical artifacts and ChainEvents.
- An explorer service crash does not stop a separately deployed
  `zinder-query` runtime. Its admitted wallet primitives remain independent of
  explorer state.
- An explorer materialized view becoming inconsistent does not corrupt canonical state. Operators drop the materialized-view store and rebuild from retained canonical events. When the materialized-view store is absent or cannot be opened, `zinder-explorer` starts with materialized-view-backed capabilities omitted. A store whose recorded consumers do not match the bundled Explorer view manifest is different: `zinder-explorer` exits at startup with `ConsumerNotDeclared` or `ExplorerViewManifestRequiresFreshStore`, and the operator rebuilds the views on a fresh path ([Rebuild Explorer views after a manifest change](../runbooks/deploying-on-a-vm.md#rebuild-explorer-views-after-a-manifest-change)).
- Explorer readiness causes flow through the `/readyz` endpoint and `WalletQuery.ServerInfo` capability gating; they never propagate to the wallet plane's readiness.

## Cursor expiry contract

Every paginated explorer RPC that emits a `next_cursor`
(`BlockSummariesInRange` does not, but the streaming `RecentTransactions`
and the address-keyed `TransparentAddressActivity` do) treats the
returned bytes as **valid for the lifetime of the column-family snapshot
at issue time**. Concretely:

- A cursor that resolves to a height range still present in the consumer
  CF resumes cleanly on the next request.
- A cursor that references a height range the consumer has dropped (a
  rolling reorg removed it, or a deliberate operator rebuild started
  fresh) is rejected with
  `Status::FailedPrecondition` carrying a typed
  `CursorExpiredError` in the gRPC status details. The error carries
  `recommended_resume_height` (the recommended restart height) and `hint`
  (a short sentence a UI may render verbatim).

Consumers handle expiry by either re-issuing the request with an empty
cursor (page from the head) or jumping to
`recommended_resume_height`. Silently jumping past the rewound range is
explicitly _not_ the server's responsibility; masking the discontinuity
would lose the reorg signal a UI needs to render a "view refreshed" hint.

## Source-boundary extensions

The explorer may parse transaction bytes through `zinder-source` and poll an optional upstream-health observation outside the request path. Neither use may provide an authoritative chain fact, alter a pinned chain view, or substitute for canonical data. When a view needs an authoritative fact that is not in canonical artifacts or replayable events, the source boundary extends first:

1. New `NodeSource` method on `zinder-source` (e.g. `fetch_chain_value_pools`).
2. New `NodeCapability` variant identifying the surface.
3. The fact lands in `SourceBlock`, a typed `Source*` value, a source-backed control primitive, or a new canonical artifact family per [Extending artifacts](extending-artifacts.md).
4. The explorer consumer subscribes to the new event or artifact, or composes through the new `WalletQuery` primitive when the fact is intentionally live-source-backed.

Chain value pools (the `ValuePoolSummary` view) is the first source-boundary extension that stays live-source-backed. `zinder-source` parses `getblockchaininfo.valuePools` together with that response's `blocks` and `bestblockhash`; the admitted `WalletQuery` owns the same probed source handle that justified its capability claim and uses it to serve the hash-bound snapshot; and `ExplorerQuery.ValuePoolSummary` wraps the wallet response in `ExplorerFreshness`.

Final note-commitment roots use the durable variant of this pattern.
`zinder-source` parses the post-block Sapling, Orchard, and Ironwood roots from
`z_gettreestate`; `zinder-ingest` stores them as the canonical
`BlockFinalNoteCommitmentRoots` artifact; and
`CommitmentRootSearchConsumer` builds the product-neutral reverse index. The
explorer read paths for these roots need canonical-store reads, so it does not
advertise `explorer.block.final_note_commitment_roots_v1` or
`explorer.commitment_root.search_v1`.

## Materialized views

Explorer views use the materialized-view SDK and capability-gated optional fields. `BlockSummaryConsumer`, `TransactionFeesConsumer`, `MempoolEventCountsConsumer`, `TransparentAddressActivityConsumer`, `TransparentAddressDeltasConsumer`, `TransparentAddressRankingConsumer`, `TransactionHistoryConsumer`, `CommitmentRootSearchConsumer`, and `ReorgIncidentsConsumer` write product-specific rows in the materialized-view store while the canonical store remains the wallet-correctness boundary. `ReorgIncidentsConsumer` is an event-only chain-event consumer: it reads `ChainEventEnvelope` rows directly and never waits for block-context hydration. It backfills from the earliest retained chain event when first deployed and preserves incidents beyond chain-event retention; it cannot reconstruct incidents pruned before deployment. See [ADR-0017](../adrs/0017-materialized-view-consumer-and-key-codec.md) for the materialized-view consumer template and [ADR-0018](../adrs/0018-capability-gated-optional-payload-fields.md) for the optional-field convention.

`ExplorerQuery.TransactionHistory` is the reference read-fenced
materialized-view query. Consumer schema changes require a fresh
materialized-view store before the consumer records its atomic epoch, tip,
revision, and contiguous coverage state. A resumable replay verifier
establishes coverage from height 1 against canonical transaction facts. The
handler reads materialized-view state, rows, fee joins, and an optional exact
count from one `MaterializedViewStore` snapshot; on a secondary, the snapshot's
catch-up barrier prevents the response from mixing pre-catch-up and
post-catch-up state. The response returns the exact read fence and coverage.
Opaque cursors bind both the filter and fence, and a supplied stale fence fails
with `FAILED_PRECONDITION`.

Capability advertisement follows live materialized-view state. Capability v1
is available when the materialized view and its `WalletQuery` dependency are
online. Capability v2 also needs contiguous coverage from height 1 through the
current materialized-view tip with the same hash, and that position must match
the canonical tip. The explorer does not read the canonical tip, so it does not
advertise v2. Exact totals are returned only when the coverage condition holds,
and their scope is then `FULL_HISTORY`. Adapters that walk multiple pages or cache totals must
carry the fence through every request and include it in cache identity.

## Cross-references

- [Service boundaries](service-boundaries.md) — names `zinder-explorer` in the workspace inventory.
- [Materialized-view plane](materialized-view-plane.md) — the reusable SDK pattern the explorer plane exercises.
- [Wallet data plane](wallet-data-plane.md) — sibling boundary; the canonical wallet read surface.
- [Public interfaces](public-interfaces.md) — naming spine, capability discovery, error vocabulary, configuration conventions.
- [Service operations](service-operations.md) — readiness, metrics, lifecycle conventions the explorer service inherits.
- [Reference: error vocabulary](../reference/error-vocabulary.md) — explorer-specific `ErrorReason` variants and retry semantics.
- [ADR-0009](../adrs/0009-explorer-plane-as-product-surface.md), [ADR-0010](../adrs/0010-transaction-public-facts.md), [ADR-0011](../adrs/0011-explorer-freshness-envelope.md), [ADR-0012](../adrs/0012-typed-explorer-search-and-privacy-refusal.md) — the four decisions that govern this plane.
