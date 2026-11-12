//! Zinder capability strings and registry metadata.
//!
//! Every served RPC on `WalletQuery`, `ExplorerQuery`, and `IngestControl`
//! has one row in [`CAPABILITIES`]. Each row binds a capability string to its
//! surface, the fully qualified proto method it gates, and a declarative
//! [`AdvertisePolicy`]. The registry defines vocabulary, stable ordering, and
//! descriptive requirements; it does not discover the capabilities of a
//! composed wallet runtime. The `capability-table-vs-descriptor` CI guard
//! cross-checks the table against the compiled `FileDescriptorSet`. The full
//! protocol contract is in [Public interfaces §Capability
//! Discovery](../../docs/architecture/public-interfaces.md#capability-discovery).
//!
//! Capability naming follows `domain.subdomain.capability_name_v{N}`.
//! Versioned suffixes are part of the capability identity; a `_v2`
//! capability is a separate string from its `_v1` predecessor and may
//! coexist during a deprecation window.

use crate::v1::explorer::ExplorerServerInfo;
use crate::v1::ops::ServerInfo as OpsServerInfo;
use crate::v1::wallet::WalletServerInfo;

/// Capability advertised for `WalletQuery.VisibleTipBlock`.
pub const WALLET_READ_VISIBLE_TIP_BLOCK_V1: &str = "wallet.read.visible_tip_block_v1";
/// Capability advertised for `WalletQuery.SettledTipBlock`.
pub const WALLET_READ_SETTLED_TIP_BLOCK_V1: &str = "wallet.read.settled_tip_block_v1";
/// Capability advertised for `WalletQuery.BlockIdBySelector`.
pub const WALLET_READ_BLOCK_ID_BY_SELECTOR_V1: &str = "wallet.read.block_id_by_selector_v1";
/// Capability advertised for `WalletQuery.BlockHeaderBySelector`.
pub const WALLET_READ_BLOCK_HEADER_BY_SELECTOR_V1: &str = "wallet.read.block_header_by_selector_v1";
/// Capability advertised for `WalletQuery.CompactBlock`.
pub const WALLET_READ_COMPACT_BLOCK_AT_V2: &str = "wallet.read.compact_block_at_v2";
/// Capability advertised for `WalletQuery.CompactBlocksInRange`.
///
/// Since contract revision 7, the stream sends one leading `ChainView` header
/// and then one bare compact block per requested height.
pub const WALLET_READ_COMPACT_BLOCK_RANGE_V2: &str = "wallet.read.compact_block_range_v2";
/// Field capability gating native `ironwood_actions` and
/// `ironwood_commitment_tree_size` structured compact-block fields.
///
/// Present on every deployment running this Ironwood-aware binary: absence of
/// `ironwoodActions` on a block then means the block genuinely has no
/// Ironwood actions. A server that does not advertise this capability
/// predates Ironwood wallet-plane support, so a missing field is not
/// authoritative and must not be read as "no Ironwood activity".
pub const WALLET_READ_COMPACT_BLOCK_IRONWOOD_V2: &str = "wallet.read.compact_block_ironwood_v2";
/// Capability advertised for `WalletQuery.FullBlock`.
///
/// The serialized block bytes are present only when the writer deployment
/// retains block blobs (ingest `raw_blob_policy` is `all`). Reads for
/// unretained heights return `ArtifactUnavailable` (gRPC `NOT_FOUND`). A
/// wallet composition claims this capability only when its admitted read path
/// retains and serves block blobs.
pub const WALLET_READ_FULL_BLOCK_AT_V1: &str = "wallet.read.full_block_at_v1";
/// Capability advertised for `WalletQuery.FullBlocksInRange`.
///
/// Same block-blob retention requirement as
/// [`WALLET_READ_FULL_BLOCK_AT_V1`]: the stream yields serialized blocks only
/// when the writer deployment sets `raw_blob_policy = "all"`. A wallet
/// composition claims this capability only when its admitted range-read path
/// retains and serves block blobs.
pub const WALLET_READ_FULL_BLOCK_RANGE_V1: &str = "wallet.read.full_block_range_v1";
/// Capability advertised for `WalletQuery.TreeStateAtHeight`.
pub const WALLET_READ_TREE_STATE_AT_HEIGHT_V2: &str = "wallet.read.tree_state_at_height_v2";
/// Capability advertised for `WalletQuery.LatestTreeStateCheckpoint`.
pub const WALLET_READ_LATEST_TREE_STATE_CHECKPOINT_V2: &str =
    "wallet.read.latest_tree_state_checkpoint_v2";
/// Capability advertised for `WalletQuery.SubtreeRoots`.
pub const WALLET_READ_SUBTREE_ROOTS_IN_RANGE_V1: &str = "wallet.read.subtree_roots_in_range_v1";
/// Field capability gating the Ironwood protocol on `WalletQuery.SubtreeRoots`
/// (and the lightwalletd-compat `GetSubtreeRoots` ironwood arm).
///
/// A server that does not advertise this capability rejects Ironwood
/// subtree-root requests; clients must fall back to linear scanning for the
/// Ironwood tree rather than treating an empty response as authoritative.
pub const WALLET_READ_SUBTREE_ROOTS_IRONWOOD_V1: &str = "wallet.read.subtree_roots_ironwood_v1";
/// Capability advertised for `WalletQuery.Transaction`.
///
/// Covers the complete typed transaction-status response. A composition
/// advertises it only when its admitted canonical and live providers can
/// distinguish every supported status. The optional `raw_transaction_bytes`
/// field on the mined arm is gated separately by
/// [`WALLET_READ_TRANSACTION_BYTES_V1`].
pub const WALLET_READ_TRANSACTION_BY_ID_V2: &str = "wallet.read.transaction_by_id_v2";
/// Field capability gating `raw_transaction_bytes` on the mined arm of
/// `WalletQuery.Transaction`.
///
/// A composition may advertise this only together with
/// [`WALLET_READ_TRANSACTION_BY_ID_V2`] and when its admitted store retains
/// transaction blobs (ingest `raw_blob_policy` in `{transactions, all}`).
/// When absent the field is `None`; clients branch on presence rather than
/// empty-vs-populated bytes.
pub const WALLET_READ_TRANSACTION_BYTES_V1: &str = "wallet.read.transaction_bytes_v1";
/// Capability advertised for `WalletQuery.ServerInfo`.
pub const WALLET_READ_SERVER_INFO_V2: &str = "wallet.read.server_info_v2";
/// Capability advertised for `WalletQuery.NetworkUpgradeActivations`.
pub const WALLET_READ_NETWORK_UPGRADE_ACTIVATIONS_V1: &str =
    "wallet.read.network_upgrade_activations_v1";
/// Capability advertised for `WalletQuery.TransparentOutputsByOutpoint`.
///
/// A wallet composition claims this capability only when its admitted query
/// implements the canonical output resolver.
pub const WALLET_READ_TRANSPARENT_OUTPUTS_V1: &str =
    "wallet.read.transparent_outputs_by_outpoint_v1";
/// Capability advertised for `WalletQuery.TransparentSpendsByOutpoint`.
///
/// A wallet composition claims this capability only when its admitted query
/// implements the canonical (confirmed) reverse-spend resolver over the
/// canonical spend-fact index. The unmined half is
/// [`WALLET_MEMPOOL_TRANSPARENT_SPENDS_BY_OUTPOINT_V1`].
pub const WALLET_READ_TRANSPARENT_SPENDS_V1: &str = "wallet.read.transparent_spends_by_outpoint_v1";
/// Capability advertised for `WalletQuery.TransparentUnspentOutputsByOutpoint`.
///
/// A wallet composition claims this capability only when its admitted query
/// implements the canonical single-outpoint unspent probe
/// (gettxout-equivalent) over the canonical output and spend-fact indexes.
/// Mempool-aware unspent-ness composes with
/// [`WALLET_MEMPOOL_TRANSPARENT_SPENDS_BY_OUTPOINT_V1`].
pub const WALLET_READ_TRANSPARENT_UNSPENT_OUTPUTS_V1: &str =
    "wallet.read.transparent_unspent_outputs_by_outpoint_v1";
/// Capability advertised for `WalletQuery.ChainValuePoolsAtTip`.
///
/// The response binds the upstream pool snapshot to one source tip height and
/// hash.
pub const WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1: &str = "wallet.read.chain_value_pools_at_tip_v1";
/// Capability advertised for `WalletQuery.TransparentUtxoSetSummary`.
///
/// The count and total value are folded in-process from the canonical
/// current-UTXO projection at the settled tip. A composition advertises this
/// only when that projection and the bounded summary implementation are
/// concretely admitted. The release serving-pair query currently omits it.
pub const WALLET_READ_TRANSPARENT_UTXO_SET_SUMMARY_V1: &str =
    "wallet.read.transparent_utxo_set_summary_v1";
/// Field capability gating `commitment` on `TransparentUtxoSetSummaryResponse`.
///
/// The `LtHash16` commitment has real per-output CPU cost. No current production
/// composition admits this field, and a manual operator support flag is not
/// valid admission evidence. When absent the field is `None`.
pub const WALLET_READ_TRANSPARENT_UTXO_SET_COMMITMENT_V1: &str =
    "wallet.read.transparent_utxo_set_commitment_v1";
/// Capability advertised for `WalletQuery.BroadcastTransaction`.
pub const WALLET_BROADCAST_TRANSACTION_V1: &str = "wallet.broadcast.transaction_v1";
/// Capability advertised for `WalletQuery.ChainEvents`.
pub const WALLET_EVENTS_CHAIN_V1: &str = "wallet.events.chain_v1";
/// Capability advertised for `WalletQuery.MempoolSnapshot`.
pub const WALLET_SNAPSHOT_MEMPOOL_V3: &str = "wallet.snapshot.mempool_v3";
/// Capability advertised for `WalletQuery.MempoolEvents`.
pub const WALLET_EVENTS_MEMPOOL_V2: &str = "wallet.events.mempool_v2";
/// Capability advertised for `WalletQuery.TransparentMempoolOutputsByAddress`.
pub const WALLET_MEMPOOL_TRANSPARENT_OUTPUTS_BY_ADDRESS_V1: &str =
    "wallet.mempool.transparent_outputs_by_address_v1";
/// Capability advertised for `WalletQuery.TransparentMempoolSpendsByOutpoint`.
pub const WALLET_MEMPOOL_TRANSPARENT_SPENDS_BY_OUTPOINT_V1: &str =
    "wallet.mempool.transparent_spends_by_outpoint_v1";
/// Capability advertised for `WalletQuery.TransparentMempoolOutputsByOutpoint`.
pub const WALLET_MEMPOOL_TRANSPARENT_OUTPUTS_V1: &str =
    "wallet.mempool.transparent_outputs_by_outpoint_v1";
/// Capability advertised for `WalletQuery.TransparentAddressUnspentOutputs`.
pub const WALLET_ADDRESS_TRANSPARENT_UNSPENT_OUTPUTS_V1: &str =
    "wallet.address.transparent_unspent_outputs_v1";
/// Capability advertised for ascending
/// `WalletQuery.TransparentAddressTxIdsInRange` reads.
///
/// This capability does not imply that newest-first iteration is available.
pub const WALLET_ADDRESS_TRANSPARENT_HISTORY_V1: &str = "wallet.address.transparent_history_v1";
/// Capability advertised for `WalletQuery.TransparentAddressBalance`.
///
/// The endpoint must prove the complete balance semantics implemented by its
/// adapter. A wallet projection alone does not admit a native composite that
/// also reads live mempool state; compatibility adapters own their independent
/// support decision.
pub const WALLET_ADDRESS_TRANSPARENT_BALANCE_V1: &str = "wallet.address.transparent_balance_v1";
/// Capability advertised for `ExplorerQuery.ServerInfo`.
pub const EXPLORER_SERVER_INFO_V1: &str = "explorer.server_info_v1";
/// Capability advertised for `ExplorerQuery.TransactionDetail`.
///
/// Signals that the response carries the full `TransactionPublicFacts` shape
/// plus ordered transparent inputs and outputs parsed from wallet-retained
/// transaction bytes. The explorer materialized-view store supplies projected
/// input values, paid fees, and epoch-covered canonical output-spend state.
/// The wallet capability for typed transaction lookup remains
/// [`WALLET_READ_TRANSACTION_BY_ID_V2`], while mined bytes require
/// [`WALLET_READ_TRANSACTION_BYTES_V1`].
pub const EXPLORER_TRANSACTION_DETAIL_V4: &str = "explorer.transaction.detail_v4";
/// Capability advertised for `ExplorerQuery.BlockSummariesInRange`.
///
/// Signals that the explorer plane is materializing the `BlockSummary`
/// materialized view and the consumer has caught up far enough to serve the
/// summary shape (`block_height`, `block_hash`, `block_time_unix_seconds`,
/// `transaction_count`, `previous_block_hash`). The companion
/// [`EXPLORER_BLOCK_DETAIL_V1`] covers the per-block transaction-id list.
pub const EXPLORER_BLOCK_SUMMARY_V1: &str = "explorer.block.summary_v1";
/// Capability advertised for `ExplorerQuery.BlockProductionSeries`.
///
/// Signals that the explorer can join a bounded range of existing
/// `BlockSummary` rows to canonical block-header difficulty bits. The series is
/// assembled at request time and reports missing coverage explicitly; it does
/// not imply a new durable projection.
pub const EXPLORER_BLOCK_PRODUCTION_SERIES_V2: &str = "explorer.block.production_series_v2";
/// Capability advertised for `ExplorerQuery.BlockProductionInTimeRange`.
///
/// Signals that the explorer can page canonical block-production observations
/// through a half-open block-time range at one read-fenced epoch. Each page
/// reports covered and missing production facts explicitly; it does not imply
/// a new durable projection.
pub const EXPLORER_BLOCK_PRODUCTION_TIME_RANGE_V1: &str = "explorer.block.production_time_range_v1";
/// Capability advertised for `ExplorerQuery.BlockDetail`.
///
/// Signals that the explorer plane materialized the per-block transaction
/// id list alongside the summary fields. Coexists with
/// [`EXPLORER_BLOCK_SUMMARY_V1`]; both are advertised together by the same
/// `BlockSummaryConsumer` materialized view.
pub const EXPLORER_BLOCK_DETAIL_V1: &str = "explorer.block.detail_v1";
/// Capability advertised for `ExplorerQuery.BlockTransactions`.
///
/// Signals that the explorer plane can batch-read the public transaction facts
/// and transparent output scripts already retained for one materialized block.
/// The response avoids a consumer-side `BlockDetail` plus per-transaction
/// `TransactionDetail` fan-out and does not imply raw-byte retention.
pub const EXPLORER_BLOCK_TRANSACTIONS_V2: &str = "explorer.block.transactions_v2";
/// Field capability gating `BlockTransactionsResponse.final_note_commitment_roots`.
///
/// Presence means the explorer can read typed canonical root artifacts. The
/// field remains absent for historical heights whose additive backfill has not
/// reached them yet; individual pool fields remain absent before activation.
pub const EXPLORER_BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_V1: &str =
    "explorer.block.final_note_commitment_roots_v1";
/// Capability advertised for `ExplorerQuery.BlockActivityDistribution`.
///
/// Signals that the explorer aggregates a bounded height range of existing
/// materialized `BlockSummary` rows into a fixed weekday/hour activity grid.
/// The aggregate is a request-time read, not a durable time-series
/// projection; its response exposes missing materialized rows explicitly.
pub const EXPLORER_BLOCK_ACTIVITY_DISTRIBUTION_V1: &str = "explorer.block.activity_distribution_v1";
/// Capability advertised for `ExplorerQuery.Search`.
///
/// Signals that the explorer plane classifies a raw user input string
/// into typed search candidates per
/// [ADR-0012](../../../docs/adrs/0012-typed-explorer-search-and-privacy-refusal.md).
/// The classifier short-circuits shielded receivers and viewing keys
/// into the typed `NotPubliclyIndexable` refusal arm before any storage
/// read; gated on `wallet_query_endpoint.is_some()` because hash
/// disambiguation routes through `WalletQuery`.
pub const EXPLORER_SEARCH_V1: &str = "explorer.search_v1";
/// Capability advertised for `ExplorerQuery.CommitmentRootSearch`.
///
/// Signals that the explorer plane can search canonical post-block Sapling,
/// Orchard, and Ironwood note-commitment-tree roots through a durable reverse
/// index. Responses carry explicit historical coverage while the additive
/// projection backfills.
pub const EXPLORER_COMMITMENT_ROOT_SEARCH_V1: &str = "explorer.commitment_root.search_v1";
/// Field capability gating `CommitmentRootSearchResponse.displaced_matches`.
///
/// Presence means the root-search projection and writer-owned canonical archive
/// are both available. The field remains empty for a root with no retained
/// displaced match; callers use `displaced_coverage` to determine whether a
/// negative result is definitive within the activation-limited captured range.
pub const EXPLORER_COMMITMENT_ROOT_DISPLACED_MATCHES_V1: &str =
    "explorer.commitment_root.displaced_matches_v1";
/// Capability advertised for `ExplorerQuery.MempoolSummary`.
///
/// Signals that the explorer plane aggregates the live mempool snapshot
/// into the explorer-shaped page (total counts, privacy-shape and
/// version distributions, freshness extremes) at request time. Composed
/// from `WalletQuery.MempoolSnapshot`; no materialized-view consumer required.
pub const EXPLORER_MEMPOOL_SUMMARY_V1: &str = "explorer.mempool.summary_v1";
/// Capability advertised for `ExplorerQuery.MempoolSnapshot`.
///
/// Signals that the explorer derives one global summary and one requested page
/// from the same `WalletQuery.MempoolSnapshot` observation. This avoids the
/// cross-request race inherent in composing `MempoolSummary` and
/// `MempoolActivity` independently. No materialized-view consumer is required.
pub const EXPLORER_MEMPOOL_SNAPSHOT_V1: &str = "explorer.mempool.snapshot_v1";
/// Capability advertised for `ExplorerQuery.MempoolActivity`.
///
/// Signals that the explorer plane projects the live mempool entries
/// into the typed `MempoolActivityEntry` rows ordered by newest-first
/// observation time. Composed from `WalletQuery.MempoolSnapshot`.
pub const EXPLORER_MEMPOOL_ACTIVITY_V1: &str = "explorer.mempool.activity_v1";
/// Capability advertised for `ExplorerQuery.TransparentAddressActivity`.
///
/// Signals that the explorer serves cursor- or offset-paged confirmed address
/// activity with current balance/lifetime coverage from the active ranking
/// generation. A library composition with retained canonical transaction facts
/// may additionally enrich rows with block position, transaction shape, and
/// transparent counterparty facts without changing the persisted activity
/// schema.
pub const EXPLORER_TRANSPARENT_ADDRESS_ACTIVITY_V2: &str =
    "explorer.transparent_address.activity_v2";
/// Capability advertised for `ExplorerQuery.TransparentAddressDeltas`.
///
/// Signals that the explorer plane serves the per-event signed value series
/// for one transparent address over a height range, ordered ascending. The
/// rows are materialized by `TransparentAddressDeltasConsumer` from the same
/// per-event attribution the activity surface folds into one net row per
/// transaction.
pub const EXPLORER_TRANSPARENT_ADDRESS_DELTAS_V1: &str = "explorer.transparent_address.deltas_v1";
/// Capability advertised for `ExplorerQuery.FeeSummary`.
///
/// Signals that the explorer plane aggregates per-transaction
/// ZIP-317 conventional fee floors over a block range at request time.
/// The fee fields are ZIP-317 conventional fees, not miner-collected
/// fees: computing actual fees requires transparent-output resolution and is
/// materialized by the materialized-view plane.
pub const EXPLORER_FEE_SUMMARY_V1: &str = "explorer.fee.summary_v1";
/// Capability advertised for `ExplorerQuery.ConventionalFeeDistribution`.
///
/// Signals that the explorer serves exact, sorted per-UTC-day frequencies of
/// ZIP-317 conventional fees over half-open block-time ranges, with explicit
/// unavailable-transaction counts and projection coverage. It does not
/// describe miner-collected fees or consumer-specific percentiles.
pub const EXPLORER_CONVENTIONAL_FEE_DISTRIBUTION_V1: &str =
    "explorer.fee.conventional_distribution_v1";
/// Capability advertised for `ExplorerQuery.PaidFeeDistribution`.
///
/// Signals that the explorer serves exact miner-collected fee frequencies
/// from a dedicated materialized view. Missing intrinsic balances or
/// transparent prevouts remain explicit unavailable counts.
pub const EXPLORER_PAID_FEE_DISTRIBUTION_V1: &str = "explorer.fee.paid_distribution_v1";
/// Capability advertised for `ExplorerQuery.TransactionComponentSummary`.
///
/// Signals that the explorer can aggregate canonical transaction component
/// counts and protocol-scoped predicate totals over exact half-open block-time
/// ranges with UTC-day buckets and explicit contiguous historical coverage.
pub const EXPLORER_TRANSACTION_COMPONENT_SUMMARY_V2: &str =
    "explorer.transaction.component_summary_v2";
/// Capability advertised for `ExplorerQuery.TransparentAddressRanking`.
///
/// Signals that the explorer serves positive standard transparent scripts in
/// deterministic balance order with integer concentration totals and explicit
/// lifetime-history coverage.
pub const EXPLORER_TRANSPARENT_ADDRESS_RANKING_V1: &str = "explorer.transparent_address.ranking_v1";
/// Capability advertised for `ExplorerQuery.ValuePoolSummary`.
///
/// Signals that the explorer plane can surface upstream
/// `getblockchaininfo.valuePools` through the wallet-plane
/// `ChainValuePoolsAtTip` primitive. The response preserves upstream pool ids
/// instead of projecting into a fixed list of known pools and carries the
/// source tip height and hash from the same upstream observation.
pub const EXPLORER_VALUE_POOL_SUMMARY_V1: &str = "explorer.value_pool.summary_v1";
/// Capability advertised for `ExplorerQuery.NetworkUpgradeStatus`.
///
/// Signals that the explorer plane can surface the node-advertised
/// network-upgrade activation table alongside the canonical tip height, so a
/// consumer can render whether an upgrade is active and how far the tip is
/// from a not-yet-active activation. The tip height rides on the wallet-plane
/// `VisibleTipBlock` primitive.
pub const EXPLORER_NETWORK_UPGRADE_STATUS_V1: &str = "explorer.network_upgrade.status_v1";
/// Capability advertised for `ExplorerQuery.ValuePoolFlowHistory`.
///
/// Signals that the explorer serves bounded newest-first pages of canonical
/// transparent-to-shielded flow events with typed filters, opaque continuations,
/// optional exact counts, and explicit materialization coverage.
pub const EXPLORER_VALUE_POOL_FLOW_HISTORY_V1: &str = "explorer.value_pool.flow_history_v1";
/// Capability advertised for `ExplorerQuery.ValuePoolFlowEventsInRange`.
///
/// Signals that the explorer serves bounded canonical value-pool flow events
/// selected by half-open block-time range, direction, pool, and inclusive
/// amount bounds, with explicit scan, result, freshness, and coverage metadata.
pub const EXPLORER_VALUE_POOL_FLOW_EVENTS_IN_RANGE_V1: &str =
    "explorer.value_pool.flow_events_in_range_v1";
/// Capability advertised for `ExplorerQuery.ValuePoolFlowSummary`.
///
/// Signals that the explorer aggregates the canonical value-pool flow event
/// projection into UTC hour or day buckets over a half-open time range.
pub const EXPLORER_VALUE_POOL_FLOW_SUMMARY_V1: &str = "explorer.value_pool.flow_summary_v1";
/// Capability advertised for `ExplorerQuery.ValuePoolFlowAmountThresholdSummary`.
///
/// Signals that the explorer scans the canonical value-pool flow projection
/// once to return exact event counts and amount sums for up to 32 requested
/// minimum amounts over a half-open time range.
pub const EXPLORER_VALUE_POOL_FLOW_AMOUNT_THRESHOLD_SUMMARY_V1: &str =
    "explorer.value_pool.flow_amount_threshold_summary_v1";
/// Capability advertised for `ExplorerQuery.ValuePoolFlowRoundedAmountSummary`.
///
/// Signals that the explorer groups canonical value-pool flow events by a
/// caller-selected nearest raw-amount rounding quantum (with positive
/// exact-half ties upward) over a half-open time range, with optional
/// raw-amount and pool filters, frequency-ranked bounded rows, exact
/// shield/deshield counts, and explicit materialization coverage.
pub const EXPLORER_VALUE_POOL_FLOW_ROUNDED_AMOUNT_SUMMARY_V1: &str =
    "explorer.value_pool.flow_rounded_amount_summary_v1";
/// Capability advertised for `ExplorerQuery.ValuePoolBalanceHistory`.
///
/// Signals that the explorer serves authoritative cumulative pool balances
/// sampled at canonical UTC-day boundaries with explicit height coverage.
pub const EXPLORER_VALUE_POOL_BALANCE_HISTORY_V1: &str = "explorer.value_pool.balance_history_v1";
/// Capability advertised for `ExplorerQuery.UtxoSetSummary`.
///
/// Signals that the explorer plane can surface the chain-wide transparent
/// UTXO-set count and total value through the wallet-plane
/// `TransparentUtxoSetSummary` primitive. A request-time full scan of the
/// current-UTXO projection; the serialized-set hash and byte size are not
/// reported.
pub const EXPLORER_UTXO_SET_SUMMARY_V1: &str = "explorer.utxo_set.summary_v1";
/// Field capability gating `commitment` on `UtxoSetSummaryResponse`.
///
/// Mirrors [`WALLET_READ_TRANSPARENT_UTXO_SET_COMMITMENT_V1`]: the explorer
/// surfaces the wallet-plane commitment when a `WalletQuery` endpoint is wired.
/// The wallet endpoint populates the field only when its own commitment
/// capability is advertised, so a present field here always carries real bytes.
pub const EXPLORER_UTXO_SET_COMMITMENT_V1: &str = "explorer.utxo_set.commitment_v1";
/// Capability advertised for the per-transaction paid-fee surface.
///
/// Signals that the explorer plane has transparent-output facts online and
/// the `TransactionFeesConsumer` has materialized per-transaction fee rows.
/// When advertised, `TransactionDetail` responses populate resolved
/// `transparent_inputs[].value_zat`. They populate `paid_fee_zat` only for
/// transparent-only transactions because canonical facts do not retain the
/// value balances required to prove shielded fees. Consumers fall back to
/// `zip317_conventional_fee_zat` when the paid fee is absent and use
/// `prevout_resolution_status` for transparent-input resolution. Gates fields
/// on `TransactionDetail` rather than a dedicated RPC.
pub const EXPLORER_TRANSACTION_FEES_V1: &str = "explorer.transaction.fees_v1";
/// Capability advertised for `ExplorerQuery.RecentTransactions`.
///
/// Signals that the explorer plane materializes a time-descending projection
/// into the `recent_transactions` materialized-view column family and serves it as one
/// streaming RPC.
pub const EXPLORER_TRANSACTION_RECENT_V1: &str = "explorer.transaction.recent_v1";
/// Capability advertised for `ExplorerQuery.TransactionHistory`.
///
/// Signals that the explorer plane serves bounded, filter-aware,
/// bidirectional pages over its canonical transaction-history projection.
pub const EXPLORER_TRANSACTION_HISTORY_V1: &str = "explorer.transaction.history_v1";
/// Capability advertised for the additive read-fenced `ExplorerQuery.TransactionHistory` contract.
///
/// The v2 capability preserves the v1 RPC and entry fields while adding a
/// projection read fence, verified coverage, and count scope.
pub const EXPLORER_TRANSACTION_HISTORY_V2: &str = "explorer.transaction.history_v2";
/// Field capability gating transaction-detail and transaction-history intrinsic balances.
///
/// Advertised when the canonical secondary admits transaction-intrinsic value
/// balances. A retained canonical transaction blob may bridge an unsettled
/// artifact lag. Otherwise missing historical artifacts remain absent; clients
/// must not interpret absence as an all-zero balance.
pub const EXPLORER_TRANSACTION_INTRINSIC_VALUE_BALANCES_V1: &str =
    "explorer.transaction.intrinsic_value_balances_v1";
/// Capability advertised for `ExplorerQuery.MempoolEventCounts`.
///
/// Signals that the explorer plane materializes per-second counters of
/// `Added`, `Mined`, and `Invalidated` mempool events into a durable
/// materialized-view column family. The count surface survives consumer
/// restarts and works across horizontally scaled consumer replicas.
pub const EXPLORER_MEMPOOL_EVENT_COUNTS_V1: &str = "explorer.mempool.event_counts_v1";
/// Capability advertised for `ExplorerQuery.ChainReorgHistory`.
///
/// Signals that the explorer plane materializes durable reorg incident rows
/// in the materialized-view store. First deployment backfills retained chain events and
/// future incidents survive chain-event retention.
pub const EXPLORER_CHAIN_REORG_HISTORY_V1: &str = "explorer.chain.reorg_history_v1";
/// Capability advertised for `ExplorerQuery.DisplacedBlockHistory`.
///
/// Signals that the explorer can read the writer-owned archive of blocks
/// displaced by canonical replacement events. Coverage begins at the archive's
/// explicit activation event and never implies earlier historical completeness.
pub const EXPLORER_CHAIN_DISPLACED_BLOCK_HISTORY_V1: &str =
    "explorer.chain.displaced_block_history_v1";
/// Capability advertised for `ExplorerQuery.DisplacedBlockDetail`.
///
/// Resolves one displaced block by hash and joins its current canonical
/// counterpart at the former height against one pinned chain epoch.
pub const EXPLORER_CHAIN_DISPLACED_BLOCK_DETAIL_V1: &str =
    "explorer.chain.displaced_block_detail_v1";
/// Capability advertised for `ExplorerQuery.OverviewSnapshot`.
///
/// Signals that the explorer plane composes a single coherent overview
/// bundle — tip identity, mempool counts, fee summary, value pools,
/// recent blocks, recent transactions, mempool event counts — in one
/// read pass over the materialized-view store, sharing one `ExplorerFreshness`
/// across every sub-field. Consumers that render a dashboard avoid the
/// per-card fan-out (six independent RPCs whose freshness can diverge)
/// in favor of this single RPC. Gated on `materialized_view_store` and
/// `wallet_query_endpoint` both being online (same precondition as the
/// materialized-view-backed cards the bundle composes).
pub const EXPLORER_OVERVIEW_SNAPSHOT_V1: &str = "explorer.overview.snapshot_v1";
/// Capability advertised for `ExplorerQuery.MigrationOverview`.
///
/// Signals that the explorer plane materializes the Orchard-to-Ironwood
/// migration facts and can aggregate them into the two-sided pool audit
/// (`orchard_outflow_zat`, `ironwood_inflow_zat`) plus the migrated-value
/// total over a block range.
pub const EXPLORER_MIGRATION_OVERVIEW_V1: &str = "explorer.migration.overview_v1";
/// Capability advertised for `ExplorerQuery.MigrationCohorts`.
///
/// Signals that the explorer plane groups migrations by shared Orchard anchor
/// and reports per-cohort size, migrated value, and conformant-member share
/// alongside the average, smallest, and largest cohort sizes.
pub const EXPLORER_MIGRATION_COHORTS_V1: &str = "explorer.migration.cohorts_v1";
/// Capability advertised for `ExplorerQuery.MigrationDenominations`.
///
/// Signals that the explorer plane bins conformant migrations by the
/// power-of-ten magnitude of their Ironwood output amount.
pub const EXPLORER_MIGRATION_DENOMINATIONS_V1: &str = "explorer.migration.denominations_v1";

/// Capability advertised for `IngestControl.ServerInfo`.
pub const INGEST_CONTROL_SERVER_INFO_V1: &str = "ingest.control.server_info_v1";
/// Capability advertised for `IngestControl.WriterStatus`.
pub const INGEST_CONTROL_WRITER_STATUS_V1: &str = "ingest.control.writer_status_v1";
/// Capability advertised for `IngestControl.VisibleChainEvents`.
pub const INGEST_CONTROL_VISIBLE_CHAIN_EVENTS_V1: &str = "ingest.control.visible_chain_events_v1";
/// Capability advertised for `IngestControl.MempoolSnapshot`.
pub const INGEST_CONTROL_MEMPOOL_SNAPSHOT_V3: &str = "ingest.control.mempool_snapshot_v3";
/// Capability advertised for `IngestControl.MempoolTransaction`.
pub const INGEST_CONTROL_MEMPOOL_TRANSACTION_V2: &str = "ingest.control.mempool_transaction_v2";
/// Capability advertised for `IngestControl.MempoolEvents`.
pub const INGEST_CONTROL_MEMPOOL_EVENTS_V2: &str = "ingest.control.mempool_events_v2";
/// Capability advertised for `IngestControl.TransparentMempoolOutputsByAddress`.
pub const INGEST_CONTROL_TRANSPARENT_MEMPOOL_OUTPUTS_BY_ADDRESS_V1: &str =
    "ingest.control.transparent_mempool_outputs_by_address_v1";
/// Capability advertised for `IngestControl.TransparentMempoolSpendsByOutpoint`.
pub const INGEST_CONTROL_TRANSPARENT_MEMPOOL_SPENDS_BY_OUTPOINT_V1: &str =
    "ingest.control.transparent_mempool_spends_by_outpoint_v1";
/// Capability advertised for `IngestControl.TransparentMempoolOutputsByOutpoint`.
pub const INGEST_CONTROL_TRANSPARENT_MEMPOOL_PREVOUTS_V1: &str =
    "ingest.control.transparent_mempool_outputs_by_outpoint_v1";
/// Capability advertised for `IngestControl.ChainValuePoolsAtTip`.
///
/// The response binds the upstream pool snapshot to one source tip height and
/// hash.
pub const INGEST_CONTROL_CHAIN_VALUE_POOLS_AT_TIP_V1: &str =
    "ingest.control.chain_value_pools_at_tip_v1";

/// Capability for `IngestControl.WriterStatus.phase`.
///
/// Advertises the classifier-driven `zinder.v1.ingest.WriterPhase`
/// vocabulary wired in
/// [ADR-0015](../../../docs/adrs/0015-phase-driven-ingest.md). Gates a
/// field on `WriterStatus` rather than a dedicated RPC.
pub const INGEST_WRITER_PHASE_V1: &str = "ingest.writer.phase_v1";

/// The data plane a capability belongs to.
///
/// Selects which `ServerInfo` builder owns a [`CapabilitySpec`] and which
/// proto service the capability-table-vs-descriptor guard maps it against.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CapabilitySurface {
    /// Served by `zinder-query` on `WalletQuery`.
    Wallet,
    /// Served by `zinder-explorer` on `ExplorerQuery`.
    Explorer,
    /// Served by `zinder-ingest` on `IngestControl`.
    Ingest,
}

/// Declarative runtime requirement associated with a capability.
///
/// Each variant classifies evidence an owning runtime may require before it
/// advertises a capability. The registry records this metadata but does not
/// derive wallet runtime support from it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AdvertisePolicy {
    /// No additional deployment requirement.
    AlwaysOn,
    /// Wallet: requires a configured transaction broadcaster.
    RequiresBroadcaster,
    /// Wallet: requires a served chain-event stream.
    RequiresChainEvents,
    /// Advertised when the upstream node reports chain value-pool totals.
    ///
    /// Ingest resolves this against the source handle's
    /// `NodeCapability::ChainValuePools`; wallet compositions require the
    /// corresponding admitted upstream read path.
    RequiresChainValuePools,
    /// Explorer: advertised when a `WalletQuery` endpoint is wired.
    RequiresWalletQuery,
    /// Explorer: advertised when the canonical secondary store is online.
    RequiresCanonicalStore,
    /// Explorer: advertised when the materialized-view store is online.
    RequiresMaterializedViewStore,
    /// Explorer: advertised when both a `WalletQuery` endpoint and the
    /// canonical store are online.
    RequiresWalletQueryAndCanonicalStore,
    /// Explorer: advertised when both the materialized-view store and a `WalletQuery`
    /// endpoint are online.
    RequiresMaterializedViewStoreAndWalletQuery,
    /// Explorer: advertised when both the materialized-view store and canonical secondary
    /// store are online.
    RequiresMaterializedViewStoreAndCanonicalStore,
    /// Explorer: advertised when the materialized-view store, canonical secondary store,
    /// and `WalletQuery` endpoint are all online.
    RequiresMaterializedViewStoreWalletQueryAndCanonicalStore,
    /// Explorer: advertised when the transparent-output prevout resolution
    /// path is online (which itself requires a `WalletQuery` endpoint).
    RequiresPrevoutResolution,
    /// Explorer: advertised when transaction history has materialized a typed
    /// projection position and the wallet-query dependency is online.
    RequiresTransactionHistory,
    /// Explorer: advertised when transaction history has verified full
    /// coverage through its typed projection position and wallet-query is online.
    RequiresCompleteTransactionHistory,
    /// Wallet: requires a store that retains full block blobs (ingest
    /// `raw_blob_policy = all`).
    RequiresBlockBlobs,
    /// Wallet: requires a store that retains transaction blobs
    /// (ingest `raw_blob_policy` in `{transactions, all}`).
    RequiresTransactionBlobs,
    /// Wallet: requires an admitted transparent UTXO-set commitment fold.
    ///
    /// No release composition currently satisfies this policy.
    RequiresUtxoSetCommitment,
    /// Wallet: requires an available transparent-address history projection.
    RequiresTransparentAddressHistory,
    /// Wallet: requires an available durable transparent-spend projection.
    RequiresTransparentOutpointSpend,
}

impl AdvertisePolicy {
    /// Resolves an `ExplorerQuery` capability against the adapter's readiness.
    ///
    /// `prevout_resolution_online` is expected to already fold in the
    /// `WalletQuery`-endpoint precondition.
    #[must_use]
    pub fn explorer_satisfied(self, readiness: ExplorerReadiness) -> bool {
        match self {
            Self::AlwaysOn => true,
            Self::RequiresWalletQuery => readiness.wallet_query_online,
            Self::RequiresCanonicalStore => readiness.canonical_store_online,
            Self::RequiresMaterializedViewStore => readiness.materialized_view_store_online,
            Self::RequiresWalletQueryAndCanonicalStore => {
                readiness.wallet_query_online && readiness.canonical_store_online
            }
            Self::RequiresMaterializedViewStoreAndWalletQuery => {
                readiness.materialized_view_store_online && readiness.wallet_query_online
            }
            Self::RequiresMaterializedViewStoreAndCanonicalStore => {
                readiness.materialized_view_store_online && readiness.canonical_store_online
            }
            Self::RequiresMaterializedViewStoreWalletQueryAndCanonicalStore => {
                readiness.materialized_view_store_online
                    && readiness.wallet_query_online
                    && readiness.canonical_store_online
            }
            Self::RequiresPrevoutResolution => readiness.prevout_resolution_online,
            Self::RequiresTransactionHistory => {
                readiness.transaction_history_available && readiness.wallet_query_online
            }
            Self::RequiresCompleteTransactionHistory => {
                readiness.transaction_history_complete && readiness.wallet_query_online
            }
            Self::RequiresBroadcaster
            | Self::RequiresChainEvents
            | Self::RequiresChainValuePools
            | Self::RequiresBlockBlobs
            | Self::RequiresTransactionBlobs
            | Self::RequiresUtxoSetCommitment
            | Self::RequiresTransparentAddressHistory
            | Self::RequiresTransparentOutpointSpend => false,
        }
    }

    /// Resolves an `IngestControl` capability against the source handle.
    ///
    /// Ingest capabilities are always on except chain-value-pools, which is
    /// gated on the upstream node reporting the matching node capability.
    #[must_use]
    pub fn ingest_satisfied(self, chain_value_pools_supported: bool) -> bool {
        match self {
            Self::AlwaysOn => true,
            Self::RequiresChainValuePools => chain_value_pools_supported,
            Self::RequiresBroadcaster
            | Self::RequiresChainEvents
            | Self::RequiresCanonicalStore
            | Self::RequiresMaterializedViewStore
            | Self::RequiresWalletQuery
            | Self::RequiresWalletQueryAndCanonicalStore
            | Self::RequiresMaterializedViewStoreAndWalletQuery
            | Self::RequiresMaterializedViewStoreAndCanonicalStore
            | Self::RequiresMaterializedViewStoreWalletQueryAndCanonicalStore
            | Self::RequiresPrevoutResolution
            | Self::RequiresBlockBlobs
            | Self::RequiresTransactionBlobs
            | Self::RequiresUtxoSetCommitment
            | Self::RequiresTransparentAddressHistory
            | Self::RequiresTransparentOutpointSpend
            | Self::RequiresTransactionHistory
            | Self::RequiresCompleteTransactionHistory => false,
        }
    }
}

/// Readiness inputs the explorer plane resolves an [`AdvertisePolicy`] against.
///
/// Each field mirrors an online/offline gate the `ExplorerQuery` adapter
/// already tracks. `prevout_resolution_online` folds in the
/// `WalletQuery`-endpoint precondition at the call site. The explorer
/// constructs this directly per request, so it is not `#[non_exhaustive]`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "Each bool is an independent explorer readiness gate, not a state machine."
)]
pub struct ExplorerReadiness {
    /// A `WalletQuery` endpoint is wired.
    pub wallet_query_online: bool,
    /// The canonical secondary store is open.
    pub canonical_store_online: bool,
    /// The materialized-view store is open.
    pub materialized_view_store_online: bool,
    /// Transparent-output prevout resolution is online (implies
    /// `wallet_query_online`).
    pub prevout_resolution_online: bool,
    /// Transaction history has a typed materialized projection position.
    pub transaction_history_available: bool,
    /// Transaction history has verified complete coverage through its position.
    pub transaction_history_complete: bool,
}

/// One row binding a capability string to its surface, proto method, and
/// advertise policy.
///
/// `method` is the fully qualified proto method name as it appears in the
/// compiled `FileDescriptorSet`
/// (`zinder.v1.<package>.<Service>.<Method>`), or `None` when the capability
/// gates a field on another RPC rather than a method of its own.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct CapabilitySpec {
    /// The exact-match capability string advertised on the wire.
    pub string: &'static str,
    /// The data plane that owns this capability.
    pub surface: CapabilitySurface,
    /// The fully qualified proto method the capability gates, or `None` for a
    /// field-level capability that rides on another RPC.
    pub method: Option<&'static str>,
    /// Declarative runtime-requirement metadata for the capability.
    pub policy: AdvertisePolicy,
}

impl CapabilitySpec {
    const fn new(
        string: &'static str,
        surface: CapabilitySurface,
        method: Option<&'static str>,
        policy: AdvertisePolicy,
    ) -> Self {
        Self {
            string,
            surface,
            method,
            policy,
        }
    }
}

/// The single declarative capability contract.
///
/// Every served RPC on the three planes has a row here, plus the field-level
/// capabilities (`explorer.transaction.fees_v1`, `ingest.writer.phase_v1`)
/// that ride on an existing RPC. A row's presence does not prove runtime
/// availability. Owning runtimes advertise only capabilities proven by their
/// admitted composition. The CI drift guard cross-checks this vocabulary
/// against the compiled `FileDescriptorSet`.
pub const CAPABILITIES: &[CapabilitySpec] = &[
    CapabilitySpec::new(
        WALLET_READ_VISIBLE_TIP_BLOCK_V1,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.VisibleTipBlock"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_READ_SETTLED_TIP_BLOCK_V1,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.SettledTipBlock"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_READ_BLOCK_ID_BY_SELECTOR_V1,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.BlockIdBySelector"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_READ_BLOCK_HEADER_BY_SELECTOR_V1,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.BlockHeaderBySelector"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_READ_COMPACT_BLOCK_AT_V2,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.CompactBlock"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_READ_COMPACT_BLOCK_RANGE_V2,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.CompactBlocksInRange"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_READ_COMPACT_BLOCK_IRONWOOD_V2,
        CapabilitySurface::Wallet,
        None,
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_READ_FULL_BLOCK_AT_V1,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.FullBlock"),
        AdvertisePolicy::RequiresBlockBlobs,
    ),
    CapabilitySpec::new(
        WALLET_READ_FULL_BLOCK_RANGE_V1,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.FullBlocksInRange"),
        AdvertisePolicy::RequiresBlockBlobs,
    ),
    CapabilitySpec::new(
        WALLET_READ_TREE_STATE_AT_HEIGHT_V2,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.TreeStateAtHeight"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_READ_LATEST_TREE_STATE_CHECKPOINT_V2,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.LatestTreeStateCheckpoint"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_READ_SUBTREE_ROOTS_IN_RANGE_V1,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.SubtreeRoots"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_READ_SUBTREE_ROOTS_IRONWOOD_V1,
        CapabilitySurface::Wallet,
        None,
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_READ_TRANSACTION_BY_ID_V2,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.Transaction"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_READ_TRANSACTION_BYTES_V1,
        CapabilitySurface::Wallet,
        None,
        AdvertisePolicy::RequiresTransactionBlobs,
    ),
    CapabilitySpec::new(
        WALLET_READ_SERVER_INFO_V2,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.ServerInfo"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_READ_NETWORK_UPGRADE_ACTIVATIONS_V1,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.NetworkUpgradeActivations"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_READ_TRANSPARENT_OUTPUTS_V1,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.TransparentOutputsByOutpoint"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_READ_TRANSPARENT_SPENDS_V1,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.TransparentSpendsByOutpoint"),
        AdvertisePolicy::RequiresTransparentOutpointSpend,
    ),
    CapabilitySpec::new(
        WALLET_READ_TRANSPARENT_UNSPENT_OUTPUTS_V1,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.TransparentUnspentOutputsByOutpoint"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.ChainValuePoolsAtTip"),
        AdvertisePolicy::RequiresChainValuePools,
    ),
    CapabilitySpec::new(
        WALLET_BROADCAST_TRANSACTION_V1,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.BroadcastTransaction"),
        AdvertisePolicy::RequiresBroadcaster,
    ),
    CapabilitySpec::new(
        WALLET_EVENTS_CHAIN_V1,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.ChainEvents"),
        AdvertisePolicy::RequiresChainEvents,
    ),
    CapabilitySpec::new(
        WALLET_SNAPSHOT_MEMPOOL_V3,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.MempoolSnapshot"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_EVENTS_MEMPOOL_V2,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.MempoolEvents"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_MEMPOOL_TRANSPARENT_OUTPUTS_BY_ADDRESS_V1,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.TransparentMempoolOutputsByAddress"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_MEMPOOL_TRANSPARENT_SPENDS_BY_OUTPOINT_V1,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.TransparentMempoolSpendsByOutpoint"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_MEMPOOL_TRANSPARENT_OUTPUTS_V1,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.TransparentMempoolOutputsByOutpoint"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_ADDRESS_TRANSPARENT_UNSPENT_OUTPUTS_V1,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.TransparentAddressUnspentOutputs"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_ADDRESS_TRANSPARENT_HISTORY_V1,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.TransparentAddressTxIdsInRange"),
        AdvertisePolicy::RequiresTransparentAddressHistory,
    ),
    CapabilitySpec::new(
        WALLET_ADDRESS_TRANSPARENT_BALANCE_V1,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.TransparentAddressBalance"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_READ_TRANSPARENT_UTXO_SET_SUMMARY_V1,
        CapabilitySurface::Wallet,
        Some("zinder.v1.wallet.WalletQuery.TransparentUtxoSetSummary"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        WALLET_READ_TRANSPARENT_UTXO_SET_COMMITMENT_V1,
        CapabilitySurface::Wallet,
        None,
        AdvertisePolicy::RequiresUtxoSetCommitment,
    ),
    CapabilitySpec::new(
        EXPLORER_SERVER_INFO_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.ServerInfo"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        EXPLORER_TRANSACTION_DETAIL_V4,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.TransactionDetail"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_BLOCK_SUMMARY_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.BlockSummariesInRange"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_BLOCK_PRODUCTION_SERIES_V2,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.BlockProductionSeries"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndCanonicalStore,
    ),
    CapabilitySpec::new(
        EXPLORER_BLOCK_PRODUCTION_TIME_RANGE_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.BlockProductionInTimeRange"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndCanonicalStore,
    ),
    CapabilitySpec::new(
        EXPLORER_BLOCK_DETAIL_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.BlockDetail"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_BLOCK_TRANSACTIONS_V2,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.BlockTransactions"),
        AdvertisePolicy::RequiresMaterializedViewStoreWalletQueryAndCanonicalStore,
    ),
    CapabilitySpec::new(
        EXPLORER_BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_V1,
        CapabilitySurface::Explorer,
        None,
        AdvertisePolicy::RequiresMaterializedViewStoreWalletQueryAndCanonicalStore,
    ),
    CapabilitySpec::new(
        EXPLORER_BLOCK_ACTIVITY_DISTRIBUTION_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.BlockActivityDistribution"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_SEARCH_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.Search"),
        AdvertisePolicy::RequiresWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_COMMITMENT_ROOT_SEARCH_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.CommitmentRootSearch"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndCanonicalStore,
    ),
    CapabilitySpec::new(
        EXPLORER_COMMITMENT_ROOT_DISPLACED_MATCHES_V1,
        CapabilitySurface::Explorer,
        None,
        AdvertisePolicy::RequiresMaterializedViewStoreAndCanonicalStore,
    ),
    CapabilitySpec::new(
        EXPLORER_MEMPOOL_SUMMARY_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.MempoolSummary"),
        AdvertisePolicy::RequiresWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_MEMPOOL_SNAPSHOT_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.MempoolSnapshot"),
        AdvertisePolicy::RequiresWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_MEMPOOL_ACTIVITY_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.MempoolActivity"),
        AdvertisePolicy::RequiresWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_TRANSPARENT_ADDRESS_ACTIVITY_V2,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.TransparentAddressActivity"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_TRANSPARENT_ADDRESS_DELTAS_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.TransparentAddressDeltas"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_FEE_SUMMARY_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.FeeSummary"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_CONVENTIONAL_FEE_DISTRIBUTION_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.ConventionalFeeDistribution"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_PAID_FEE_DISTRIBUTION_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.PaidFeeDistribution"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_TRANSACTION_COMPONENT_SUMMARY_V2,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.TransactionComponentSummary"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_TRANSPARENT_ADDRESS_RANKING_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.TransparentAddressRanking"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_VALUE_POOL_SUMMARY_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.ValuePoolSummary"),
        AdvertisePolicy::RequiresWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_NETWORK_UPGRADE_STATUS_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.NetworkUpgradeStatus"),
        AdvertisePolicy::RequiresWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_VALUE_POOL_FLOW_HISTORY_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.ValuePoolFlowHistory"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_VALUE_POOL_FLOW_EVENTS_IN_RANGE_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.ValuePoolFlowEventsInRange"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_VALUE_POOL_FLOW_SUMMARY_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.ValuePoolFlowSummary"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_VALUE_POOL_FLOW_AMOUNT_THRESHOLD_SUMMARY_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.ValuePoolFlowAmountThresholdSummary"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_VALUE_POOL_FLOW_ROUNDED_AMOUNT_SUMMARY_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.ValuePoolFlowRoundedAmountSummary"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_VALUE_POOL_BALANCE_HISTORY_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.ValuePoolBalanceHistory"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_UTXO_SET_SUMMARY_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.UtxoSetSummary"),
        AdvertisePolicy::RequiresWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_UTXO_SET_COMMITMENT_V1,
        CapabilitySurface::Explorer,
        None,
        AdvertisePolicy::RequiresWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_CHAIN_REORG_HISTORY_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.ChainReorgHistory"),
        AdvertisePolicy::RequiresMaterializedViewStore,
    ),
    CapabilitySpec::new(
        EXPLORER_CHAIN_DISPLACED_BLOCK_HISTORY_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.DisplacedBlockHistory"),
        AdvertisePolicy::RequiresCanonicalStore,
    ),
    CapabilitySpec::new(
        EXPLORER_CHAIN_DISPLACED_BLOCK_DETAIL_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.DisplacedBlockDetail"),
        AdvertisePolicy::RequiresCanonicalStore,
    ),
    CapabilitySpec::new(
        EXPLORER_MEMPOOL_EVENT_COUNTS_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.MempoolEventCounts"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_TRANSACTION_FEES_V1,
        CapabilitySurface::Explorer,
        None,
        AdvertisePolicy::RequiresPrevoutResolution,
    ),
    CapabilitySpec::new(
        EXPLORER_TRANSACTION_RECENT_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.RecentTransactions"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_TRANSACTION_HISTORY_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.TransactionHistory"),
        AdvertisePolicy::RequiresTransactionHistory,
    ),
    CapabilitySpec::new(
        EXPLORER_TRANSACTION_HISTORY_V2,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.TransactionHistory"),
        AdvertisePolicy::RequiresCompleteTransactionHistory,
    ),
    CapabilitySpec::new(
        EXPLORER_TRANSACTION_INTRINSIC_VALUE_BALANCES_V1,
        CapabilitySurface::Explorer,
        None,
        AdvertisePolicy::RequiresMaterializedViewStoreWalletQueryAndCanonicalStore,
    ),
    CapabilitySpec::new(
        EXPLORER_OVERVIEW_SNAPSHOT_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.OverviewSnapshot"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_MIGRATION_OVERVIEW_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.MigrationOverview"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_MIGRATION_COHORTS_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.MigrationCohorts"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        EXPLORER_MIGRATION_DENOMINATIONS_V1,
        CapabilitySurface::Explorer,
        Some("zinder.v1.explorer.ExplorerQuery.MigrationDenominations"),
        AdvertisePolicy::RequiresMaterializedViewStoreAndWalletQuery,
    ),
    CapabilitySpec::new(
        INGEST_CONTROL_SERVER_INFO_V1,
        CapabilitySurface::Ingest,
        Some("zinder.v1.ingest.IngestControl.ServerInfo"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        INGEST_CONTROL_WRITER_STATUS_V1,
        CapabilitySurface::Ingest,
        Some("zinder.v1.ingest.IngestControl.WriterStatus"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        INGEST_CONTROL_VISIBLE_CHAIN_EVENTS_V1,
        CapabilitySurface::Ingest,
        Some("zinder.v1.ingest.IngestControl.VisibleChainEvents"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        INGEST_CONTROL_MEMPOOL_SNAPSHOT_V3,
        CapabilitySurface::Ingest,
        Some("zinder.v1.ingest.IngestControl.MempoolSnapshot"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        INGEST_CONTROL_MEMPOOL_TRANSACTION_V2,
        CapabilitySurface::Ingest,
        Some("zinder.v1.ingest.IngestControl.MempoolTransaction"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        INGEST_CONTROL_MEMPOOL_EVENTS_V2,
        CapabilitySurface::Ingest,
        Some("zinder.v1.ingest.IngestControl.MempoolEvents"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        INGEST_CONTROL_TRANSPARENT_MEMPOOL_OUTPUTS_BY_ADDRESS_V1,
        CapabilitySurface::Ingest,
        Some("zinder.v1.ingest.IngestControl.TransparentMempoolOutputsByAddress"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        INGEST_CONTROL_TRANSPARENT_MEMPOOL_SPENDS_BY_OUTPOINT_V1,
        CapabilitySurface::Ingest,
        Some("zinder.v1.ingest.IngestControl.TransparentMempoolSpendsByOutpoint"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        INGEST_CONTROL_TRANSPARENT_MEMPOOL_PREVOUTS_V1,
        CapabilitySurface::Ingest,
        Some("zinder.v1.ingest.IngestControl.TransparentMempoolOutputsByOutpoint"),
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        INGEST_WRITER_PHASE_V1,
        CapabilitySurface::Ingest,
        None,
        AdvertisePolicy::AlwaysOn,
    ),
    CapabilitySpec::new(
        INGEST_CONTROL_CHAIN_VALUE_POOLS_AT_TIP_V1,
        CapabilitySurface::Ingest,
        Some("zinder.v1.ingest.IngestControl.ChainValuePoolsAtTip"),
        AdvertisePolicy::RequiresChainValuePools,
    ),
];

/// Returns the capability rows owned by `surface`, in table order.
///
/// This registry query preserves vocabulary order and does not evaluate
/// [`AdvertisePolicy`] or discover runtime support.
pub fn capabilities_for_surface(
    surface: CapabilitySurface,
) -> impl Iterator<Item = &'static CapabilitySpec> {
    CAPABILITIES
        .iter()
        .filter(move |spec| spec.surface == surface)
}

/// Returns the always-on capability strings for `surface`.
///
/// This is a classification query over registry metadata, not evidence that a
/// particular runtime composed or admitted the corresponding methods.
#[must_use]
pub fn always_on_capability_strings(surface: CapabilitySurface) -> Vec<&'static str> {
    capabilities_for_surface(surface)
        .filter(|spec| matches!(spec.policy, AdvertisePolicy::AlwaysOn))
        .map(|spec| spec.string)
        .collect()
}

/// A typed wallet-plane capability a client probes before issuing a call.
///
/// Each variant names one advertised capability on the `WalletQuery` surface
/// whose presence a consumer checks to decide whether a feature is reachable.
/// [`Self::as_str`] returns the exact wire string, which is the same constant
/// the [`CAPABILITIES`] table advertises, so a typed probe and the wire stay
/// in lockstep. Prefer [`CapabilityDescriptor::supports`] over comparing raw
/// capability strings.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum Capability {
    /// Raw transaction broadcast (`wallet.broadcast.transaction_v1`).
    Broadcast,
    /// Cursor-resumable chain-event stream (`wallet.events.chain_v1`).
    ChainEvents,
    /// Tip-certified bounded mempool snapshot (`wallet.snapshot.mempool_v3`).
    MempoolSnapshot,
    /// Replayable mempool-event stream (`wallet.events.mempool_v2`).
    MempoolEvents,
    /// Chain value-pool totals at the upstream tip
    /// (`wallet.read.chain_value_pools_at_tip_v1`).
    ChainValuePools,
    /// Immutable network-upgrade activation metadata
    /// (`wallet.read.network_upgrade_activations_v1`).
    NetworkUpgradeActivations,
    /// Transparent-address balance (`wallet.address.transparent_balance_v1`).
    TransparentAddressBalance,
}

impl Capability {
    /// Returns the exact capability string this variant advertises.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Broadcast => WALLET_BROADCAST_TRANSACTION_V1,
            Self::ChainEvents => WALLET_EVENTS_CHAIN_V1,
            Self::MempoolSnapshot => WALLET_SNAPSHOT_MEMPOOL_V3,
            Self::MempoolEvents => WALLET_EVENTS_MEMPOOL_V2,
            Self::ChainValuePools => WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1,
            Self::NetworkUpgradeActivations => WALLET_READ_NETWORK_UPGRADE_ACTIVATIONS_V1,
            Self::TransparentAddressBalance => WALLET_ADDRESS_TRANSPARENT_BALANCE_V1,
        }
    }
}

/// Helpers for client-side capability discovery.
///
/// Implemented by every per-service descriptor (`WalletServerInfo`,
/// `ExplorerServerInfo`) plus the cross-service `ops::ServerInfo` they embed.
/// Capability discovery always reads from the embedded `ops::ServerInfo`;
/// per-service descriptors delegate.
pub trait CapabilityDescriptor {
    /// Returns true if the descriptor advertises `capability`.
    fn has(&self, capability: &str) -> bool;

    /// Returns true if the descriptor advertises the typed [`Capability`].
    ///
    /// Prefer this over [`Self::has`] with a raw string: the compiler checks
    /// the variant against the typed set, so a consumer cannot probe a
    /// capability string that no longer exists.
    fn supports(&self, capability: Capability) -> bool {
        self.has(capability.as_str())
    }
}

impl CapabilityDescriptor for OpsServerInfo {
    fn has(&self, capability: &str) -> bool {
        self.capabilities
            .iter()
            .any(|advertised| advertised == capability)
    }
}

impl CapabilityDescriptor for WalletServerInfo {
    fn has(&self, capability: &str) -> bool {
        self.common
            .as_ref()
            .is_some_and(|common| common.has(capability))
    }
}

impl CapabilityDescriptor for ExplorerServerInfo {
    fn has(&self, capability: &str) -> bool {
        self.common
            .as_ref()
            .is_some_and(|common| common.has(capability))
    }
}
