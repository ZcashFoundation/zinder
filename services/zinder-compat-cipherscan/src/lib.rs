//! Cipherscan REST compatibility adapter over Zinder native read APIs.
//!
//! This crate intentionally keeps Cipherscan's REST paths and JSON
//! field names at the service edge. Reusable chain facts still come from
//! `ExplorerQuery` and `WalletQuery`; missing product-neutral facts should
//! become native Zinder surfaces instead of growing a Cipherscan-shaped core.

mod blend_check;
mod market_price;
mod mining_pools;

pub use market_price::MarketPriceInitializationError;

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration as StdDuration, Instant},
};

use axum::{
    Json, Router,
    extract::{
        Path, Query, Request, State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code},
    },
    http::{
        HeaderValue, Method, StatusCode, Uri,
        header::{
            ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
            ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS, CACHE_CONTROL,
            CONTENT_TYPE,
        },
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{any, delete, get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;
use time::{Date, Duration, Month, OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::{Mutex, RwLock, broadcast};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tonic::Code;
use zcash_protocol::consensus::{BLOCKS_PER_HOUR, SECONDS_PER_BLOCK};
use zebra_chain::{
    amount::{Amount, NonNegative},
    block::Height as ZebraHeight,
    parameters::{
        Network as ZebraNetwork, NetworkUpgrade,
        subsidy::{
            self, FundingStreamReceiver, block_subsidy, funding_stream_values, height_for_halving,
            miner_subsidy,
        },
        testnet::RegtestParameters,
    },
    serialization::ZcashDeserializeInto,
    transaction::Transaction as ZebraTransaction,
    transparent,
    work::difficulty::CompactDifficulty,
};
use zinder_core::{
    Network, NetworkUpgradeActivations,
    TransactionComponentCounts as CoreTransactionComponentCounts,
    TransactionPublicFacts as CoreTransactionPublicFacts,
    wire::{
        encode_bip70_chain_name, encode_rpc_transaction_id_hex, encode_zinder_native_chain_name,
    },
};
use zinder_proto::capabilities::{
    EXPLORER_BLOCK_PRODUCTION_TIME_RANGE_V1, EXPLORER_CHAIN_DISPLACED_BLOCK_DETAIL_V1,
    EXPLORER_CHAIN_DISPLACED_BLOCK_HISTORY_V1, EXPLORER_COMMITMENT_ROOT_DISPLACED_MATCHES_V1,
    EXPLORER_PAID_FEE_DISTRIBUTION_V1, EXPLORER_TRANSACTION_COMPONENT_SUMMARY_V2,
    EXPLORER_TRANSACTION_HISTORY_V2, EXPLORER_TRANSACTION_INTRINSIC_VALUE_BALANCES_V1,
    EXPLORER_VALUE_POOL_BALANCE_HISTORY_V1, EXPLORER_VALUE_POOL_FLOW_AMOUNT_THRESHOLD_SUMMARY_V1,
    EXPLORER_VALUE_POOL_FLOW_EVENTS_IN_RANGE_V1, EXPLORER_VALUE_POOL_FLOW_HISTORY_V1,
    EXPLORER_VALUE_POOL_FLOW_ROUNDED_AMOUNT_SUMMARY_V1, EXPLORER_VALUE_POOL_FLOW_SUMMARY_V1,
};
use zinder_proto::v1::{
    explorer::{
        self, BlockActivityDistributionRequest, BlockDetailRequest,
        BlockProductionInTimeRangeRequest, BlockProductionSeriesRequest, ChainReorgHistoryRequest,
        CommitmentRootSearchRequest, ConventionalFeeDistributionRequest,
        ConventionalFeeDistributionResponse, DisplacedBlockDetailRequest,
        DisplacedBlockHistoryRequest, DisplacedBlockHistoryResponse, FeeSummaryRequest,
        MempoolSnapshotRequest, PaidFeeDistributionRequest, PaidFeeDistributionResponse,
        ServerInfoRequest, TransactionComponentSummaryRequest, TransactionComponentSummaryResponse,
        TransactionDetailRequest, TransactionHistoryAnchor, TransactionHistoryCountScope,
        TransactionHistoryDirection, TransactionHistoryFilter, TransactionHistoryReadFence,
        TransactionHistoryRequest, TransactionHistoryResponse, TransparentAddressActivityRequest,
        TransparentAddressRankingRequest, TransparentAddressRankingResponse,
        ValuePoolBalanceHistoryRequest, ValuePoolBalanceHistoryResponse,
        ValuePoolFlowAmountThresholdSummaryRequest, ValuePoolFlowAmountThresholdSummaryResponse,
        ValuePoolFlowDirection, ValuePoolFlowEventsInRangeRequest,
        ValuePoolFlowEventsInRangeResponse, ValuePoolFlowFilter, ValuePoolFlowHistoryRequest,
        ValuePoolFlowHistoryResponse, ValuePoolFlowPool, ValuePoolFlowRoundedAmountSummaryRequest,
        ValuePoolFlowRoundedAmountSummaryResponse, ValuePoolFlowSummaryRequest,
        ValuePoolFlowSummaryResolution, ValuePoolSummaryRequest, block_detail_request,
        explorer_query_client::ExplorerQueryClient, lock_time, transaction_history_request,
    },
    wallet::{
        self, AddressLookup, BlockSelectorRequest, BroadcastTransactionRequest,
        SettledTipBlockRequest, TransactionRequest, VisibleTipBlockRequest, address_lookup,
        broadcast_transaction_response, chain_event_envelope, event_stream_start,
        mempool_event_envelope, transaction_location, wallet_query_client::WalletQueryClient,
    },
};
use zinder_runtime::AuthenticatedChannel;

use crate::blend_check::{
    NearbyCandidateCount, SplitCandidateCount, blend_label, build_split_plans, compute_blend_score,
    nearby_popular_amounts, split_remainder_amounts,
};
use crate::market_price::{HistoricalMarketPriceLookup, MarketPriceClient, MarketPriceError};
use crate::mining_pools::get_pool_name;

const DEFAULT_LIMIT: u32 = 10;
const DEFAULT_ADDRESS_ACTIVITY_LIMIT: u32 = 25;
const DEFAULT_RECENT_TRANSACTION_LIMIT: u32 = 50;
const DEFAULT_MEMPOOL_LIMIT: u32 = 50;
const DEFAULT_NON_CANONICAL_BLOCK_LIMIT: u32 = 50;
const DEFAULT_REORG_FORK_LIMIT: u32 = 20;
const MAX_LIMIT: u32 = 100;
const MAX_NON_CANONICAL_BLOCK_LIMIT: u32 = 200;
const BLOCK_SUMMARY_PAGE_SIZE: u32 = 1_024;
const MAX_RAW_TRANSACTION_BATCH_SIZE: usize = 1_000;
const MAX_SCAN_RANGE_BLOCKS: u64 = 1_000_000;
const TARGET_BLOCKS_PER_DAY: u32 = BLOCKS_PER_HOUR * 24;
const MAX_ORCHARD_CANDIDATE_SCAN_BLOCKS: u32 = TARGET_BLOCKS_PER_DAY * 7;
const ORCHARD_CANDIDATE_SCAN_VIEWING_KEY_FIELDS: [&str; 6] = [
    "viewingKey",
    "viewing_key",
    "fullViewingKey",
    "incomingViewingKey",
    "fvk",
    "ivk",
];
const REORG_HISTORY_PAGE_SIZE: u32 = 1_024;
const MAX_REORG_HISTORY_PAGES: usize = 16;
const CIPHERSCAN_ADAPTER_SOURCE: &str = "zinder-compat-cipherscan";
const FEE_SUMMARY_WINDOW_BLOCKS: u32 = 256;
const ZIP317_MARGINAL_FEE_ZAT: i64 = 5_000;
const ZIP317_GRACE_ACTIONS: u32 = 2;
const ZIP317_SIMPLE_TX_FEE_ZAT: i64 = 10_000;
const ZIP317_TYPICAL_SHIELDED_TX_FEE_ZAT: i64 = 15_000;
const ZIP317_COMPLEX_TX_FEE_ZAT: i64 = 25_000;
const MAX_SUPPLY_ZEC: f64 = 21_000_000.0;
const ZATOSHIS_PER_ZEC: f64 = 100_000_000.0;
const SECONDS_PER_DAY: f64 = 86_400.0;
// Testnet block production can materially exceed the target spacing. This
// bound covers the default seven-day route from timestamps rather than from a
// target-rate estimate while keeping longer analytical windows bounded.
const MAX_MINING_PRODUCTION_BLOCKS: u32 = 50_000;
const MAX_NETWORK_STATS_BLOCKS: u32 = 20_000;
const DEFAULT_MINING_METRICS_WINDOW: i64 = 20;
const MIN_MINING_METRICS_WINDOW: i64 = 5;
const MAX_MINING_METRICS_WINDOW: i64 = 100;
const DEFAULT_MINING_METRICS_LIMIT: i64 = 120;
const MIN_MINING_METRICS_LIMIT: i64 = 20;
const MAX_MINING_METRICS_LIMIT: i64 = 500;
const DEFAULT_MINING_BLOCK_INTERVAL_SECONDS: u32 = SECONDS_PER_BLOCK;
const MAX_VALID_MINING_BLOCK_INTERVAL_SECONDS: u32 = 600;
const COMPONENT_SUMMARY_FUTURE_TIME_MARGIN_SECONDS: i64 = 7_200;
const PRIVACY_STATS_TREND_DAYS: i64 = 30;
const PRIVACY_STATS_TREND_POINT_LIMIT: usize = 30;
const MAX_USAGE_CLOCK_BLOCKS: u32 = 20_000;
const TRANSACTION_HISTORY_PAGE_SIZE: u32 = 256;
const MAX_TRANSACTION_HISTORY_OFFSET: u32 = 100_000;
const MAX_ORCHARD_CANDIDATE_SCAN_PAGES: usize = 32;
const MAX_ADDRESS_ACTIVITY_OFFSET: u32 = 100_000;
const VALUE_POOL_FLOW_NATIVE_PAGE_SIZE: u32 = 256;
const LINKABILITY_LOOKBACK_DAYS: i64 = 30;
const LINKABILITY_MAX_PAIR_AGE_SECONDS: i64 = 90 * 86_400;
const LINKABILITY_MINIMUM_AMOUNT_ZAT: u64 = 100_000;
const LINKABILITY_DEFAULT_TOLERANCE_ZEC: f64 = 0.001;
const LINKABILITY_MINIMUM_TOLERANCE_ZEC: f64 = 0.0001;
const LINKABILITY_MAXIMUM_TOLERANCE_ZEC: f64 = 0.1;
const LINKABILITY_NATIVE_EVENT_LIMIT: u32 = 1_024;
const LINKABILITY_MINIMUM_CONFIDENCE_SCORE: u32 = 35;
const LINKABILITY_HIGH_WARNING_SCORE: u32 = 75;
const LINKABILITY_MEDIUM_WARNING_SCORE: u32 = 55;
const CIPHERSCAN_FLOW_TRANSACTION_INDEX_FACTOR: u64 = 1_000_000;
const MAX_CIPHERSCAN_FLOW_TRANSACTION_INDEX: u32 = 999_999;
const CIPHERSCAN_VALUE_POOL_IDS: [&str; 6] = [
    "transparent",
    "sprout",
    "sapling",
    "orchard",
    "ironwood",
    "lockbox",
];
const VALUE_POOL_TOTALS_UNAVAILABLE: &str = "One or more monitored value-pool totals are unavailable, so incomplete supply fields are null.";
const UNKNOWN_VALUE_POOL_SEMANTICS_UNAVAILABLE: &str = "The current value-pool response contains a non-zero pool whose shielded semantics are unknown to Cipherscan.";
const TRANSACTION_HISTORY_COUNT_CACHE_TTL: StdDuration = StdDuration::from_secs(30);
const MINING_PRODUCTION_CACHE_TTL: StdDuration = StdDuration::from_mins(5);
const FEE_DISTRIBUTION_CACHE_TTL: StdDuration = StdDuration::from_hours(1);
const FLOW_ANALYTICS_CACHE_TTL: StdDuration = StdDuration::from_hours(1);
const COMMON_AMOUNTS_CACHE_TTL: StdDuration = StdDuration::from_mins(15);
const MAX_COMMON_AMOUNTS_CACHE_ENTRIES: usize = 256;
const COMMON_AMOUNTS_MINIMUM_ZAT: u64 = 1_000_000;
const COMMON_AMOUNTS_ROUNDING_QUANTUM_ZAT: u64 = 1_000_000;
const BLEND_CHECK_CACHE_TTL: StdDuration = StdDuration::from_mins(5);
const MAX_BLEND_CHECK_CACHE_ENTRIES: usize = 500;
const BLEND_MATCH_TOLERANCE_ZAT: u64 = 10_000;
const BLEND_ROUNDING_QUANTUM_ZAT: u64 = 1_000_000;
const MAX_AMOUNT_RANGES_PER_THRESHOLD_REQUEST: usize = 16;
const CIPHERSCAN_ANONYMITY_SET_THRESHOLDS_ZAT: [u64; 16] = [
    1_000,
    100_000,
    1_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    200_000_000,
    500_000_000,
    1_000_000_000,
    2_500_000_000,
    5_000_000_000,
    10_000_000_000,
    50_000_000_000,
    100_000_000_000,
    1_000_000_000_000,
];
const CIPHERSCAN_SHIELDING_DISTRIBUTION_BUCKETS: [CipherscanAmountBucket; 10] = [
    CipherscanAmountBucket::new(1, Some(100_000), "<0.001"),
    CipherscanAmountBucket::new(100_000, Some(1_000_000), "0.001-0.01"),
    CipherscanAmountBucket::new(1_000_000, Some(10_000_000), "0.01-0.1"),
    CipherscanAmountBucket::new(10_000_000, Some(100_000_000), "0.1-1"),
    CipherscanAmountBucket::new(100_000_000, Some(500_000_000), "1-5"),
    CipherscanAmountBucket::new(500_000_000, Some(1_000_000_000), "5-10"),
    CipherscanAmountBucket::new(1_000_000_000, Some(5_000_000_000), "10-50"),
    CipherscanAmountBucket::new(5_000_000_000, Some(10_000_000_000), "50-100"),
    CipherscanAmountBucket::new(10_000_000_000, Some(100_000_000_000), "100-1000"),
    CipherscanAmountBucket::new(100_000_000_000, None, "1000+"),
];
const MIGRATION_ANALYTICS_CACHE_TTL: StdDuration = StdDuration::from_secs(15);
const MAX_MIGRATION_HISTORY_ENTRIES: usize = 100_000;
const MAX_MIGRATION_SCANNED_HISTORY_ENTRIES: u64 = 5_000_000;
const UNIX_SECONDS_PER_DAY: i64 = 86_400;
const MIGRATION_BOUNDARY_MODULUS: u32 = 256;
const MAX_FORK_MONITOR_CHECK_HEIGHTS: usize = 10;
const FORK_MONITOR_ANCHORS: &[(u32, &str)] = &[
    (19_138, "BFT finalized"),
    (37_657, "fixed branch check"),
    (39_574, "split marker"),
    (41_898, "May 2 split"),
    (54_777, "OG fork point"),
    (57_298, "Roman drift"),
    (57_352, "May 7 last match"),
];
const FORK_MONITOR_SPLIT_HINTS: [&str; 4] = [
    "If h39573 matches and h39574 differs, your node is on an earlier observed split.",
    "If h40665 matches but h41898 differs, the node split later near the current tip.",
    "If a node is mining every block, treat it as partition risk until peers and tip hash match.",
    "Peer count alone does not determine correctness. Longest chain with valid PoW wins above finalized height.",
];
const REALTIME_EVENT_CHANNEL_CAPACITY: usize = 256;
const REALTIME_SEND_TIMEOUT: StdDuration = StdDuration::from_secs(5);
const REALTIME_RECONNECT_DELAY: StdDuration = StdDuration::from_secs(1);
const REALTIME_HYDRATION_RETRY_DELAY: StdDuration = StdDuration::from_millis(250);
const REALTIME_HYDRATION_ATTEMPTS: usize = 20;
const PRIVACY_STATS_EPOCH_RETRY_DELAY: StdDuration = StdDuration::from_millis(100);
const PRIVACY_STATS_EPOCH_ATTEMPTS: usize = 20;

#[derive(Clone)]
enum CipherscanRealtimeDispatch {
    Payload(Arc<str>),
    SourceUnavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RealtimeCommitStatus {
    AwaitingReader,
    Hydratable,
    Superseded,
}

struct CipherscanRealtimeBroadcaster {
    sender: broadcast::Sender<CipherscanRealtimeDispatch>,
    is_started: AtomicBool,
}

/// REST adapter state shared by every Cipherscan route handler.
#[derive(Clone)]
pub struct CipherscanRestAdapter {
    network: Network,
    explorer_channel: AuthenticatedChannel,
    wallet_channel: AuthenticatedChannel,
    realtime_broadcaster: Arc<CipherscanRealtimeBroadcaster>,
    realtime_cancel: CancellationToken,
    realtime_tasks: TaskTracker,
    transaction_history_count_cache:
        Arc<RwLock<BTreeMap<TransactionHistoryCountCacheKey, CachedTransactionHistoryCount>>>,
    mining_production_cache:
        Arc<RwLock<BTreeMap<MiningProductionPeriod, CachedMiningProductionWindow>>>,
    mining_production_refresh: Arc<Mutex<()>>,
    mining_pool_distribution_cache:
        Arc<RwLock<BTreeMap<String, CachedMiningPoolDistributionResponse>>>,
    fee_distribution_cache: Arc<RwLock<BTreeMap<String, CachedFeeDistributionResponse>>>,
    anonymity_set_cache: Arc<RwLock<BTreeMap<String, CachedFlowAnalyticsResponse>>>,
    shielding_distribution_cache: Arc<RwLock<BTreeMap<String, CachedFlowAnalyticsResponse>>>,
    common_amounts_cache: Arc<RwLock<BTreeMap<CommonAmountsCacheKey, CachedFlowAnalyticsResponse>>>,
    blend_check_cache: Arc<RwLock<BTreeMap<String, CachedFlowAnalyticsResponse>>>,
    migration_analytics_cache: Arc<RwLock<Option<CachedMigrationAnalytics>>>,
    migration_analytics_refresh: Arc<Mutex<()>>,
    market_price_client: MarketPriceClient,
}

/// External market endpoints used only by Cipherscan compatibility routes.
#[derive(Clone, Debug)]
pub struct CipherscanMarketPriceEndpoints {
    /// Endpoint for the current ZEC/USD price.
    pub current: reqwest::Url,
    /// Historical endpoint template containing one `{date}` placeholder.
    pub historical_template: String,
}

impl CipherscanMarketPriceEndpoints {
    /// Validates and groups the two external market endpoints.
    pub fn new(
        current: reqwest::Url,
        historical_template: String,
    ) -> Result<Self, MarketPriceInitializationError> {
        market_price::validate_historical_endpoint_template(&historical_template)?;
        Ok(Self {
            current,
            historical_template,
        })
    }
}

/// Exact note-commitment tree sizes tied to one visible chain epoch.
#[derive(Debug)]
struct VisibleTipCommitmentTreeSizes {
    chain_epoch_id: u64,
    block_height: u32,
    block_hash: String,
    sapling_commitment_tree_size: u32,
    orchard_commitment_tree_size: u32,
    ironwood_commitment_tree_size: u32,
}

struct TransactionHistoryWindow {
    entries: Vec<explorer::TransactionHistoryEntry>,
    has_more: bool,
    total_matching_transactions: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OrchardCandidateScanRange {
    start_height: u32,
    end_height: u32,
}

impl OrchardCandidateScanRange {
    fn total_blocks(self) -> u64 {
        u64::from(self.end_height) - u64::from(self.start_height) + 1
    }
}

#[derive(Default)]
struct OrchardCandidateScan {
    entries: Vec<explorer::TransactionHistoryEntry>,
    read_fence: Option<TransactionHistoryReadFence>,
    coverage: Option<explorer::TransactionHistoryCoverage>,
    seen_coordinates: HashSet<(u32, u32)>,
}

struct ShieldedFlowPage {
    events: Vec<explorer::ValuePoolFlowEvent>,
    total_matching_events: u64,
    has_older: bool,
    has_newer: bool,
    coverage: explorer::ValuePoolFlowCoverage,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
struct CipherscanFlowCoordinate {
    block_height: u32,
    transaction_index: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
struct CipherscanFlowCursor {
    block_time_unix_seconds: i64,
    coordinate: CipherscanFlowCoordinate,
}

impl CipherscanFlowCursor {
    fn from_event(event: &explorer::ValuePoolFlowEvent) -> Result<Self, CipherscanRestError> {
        Ok(Self {
            block_time_unix_seconds: event.block_time_unix_seconds,
            coordinate: CipherscanFlowCoordinate::from_event(event)?,
        })
    }
}

impl CipherscanFlowCoordinate {
    fn from_event(event: &explorer::ValuePoolFlowEvent) -> Result<Self, CipherscanRestError> {
        if event.transaction_index_in_block > MAX_CIPHERSCAN_FLOW_TRANSACTION_INDEX {
            return Err(CipherscanRestError::InvalidUpstreamField(
                "value_pool_flow_history.events.transaction_index_in_block",
            ));
        }
        Ok(Self {
            block_height: event.block_height,
            transaction_index: event.transaction_index_in_block,
        })
    }

    fn stable_id(self) -> u64 {
        u64::from(self.block_height) * CIPHERSCAN_FLOW_TRANSACTION_INDEX_FACTOR
            + u64::from(self.transaction_index)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShieldedFlowPageDirection {
    Older,
    Newer,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CipherscanFlowResolution {
    Hourly,
    Daily,
}

impl CipherscanFlowResolution {
    const fn native(self) -> i32 {
        match self {
            Self::Hourly => ValuePoolFlowSummaryResolution::Hour as i32,
            Self::Daily => ValuePoolFlowSummaryResolution::Day as i32,
        }
    }

    const fn response_name(self) -> &'static str {
        match self {
            Self::Hourly => "hourly",
            Self::Daily => "daily",
        }
    }

    const fn bucket_seconds(self) -> i64 {
        match self {
            Self::Hourly => 60 * 60,
            Self::Daily => UNIX_SECONDS_PER_DAY,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CipherscanFlowAmountFormat {
    Zatoshi,
    Zec,
}

impl CipherscanFlowAmountFormat {
    const fn response_name(self) -> &'static str {
        match self {
            Self::Zatoshi => "zatoshi",
            Self::Zec => "zec",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CipherscanPoolFlowRequest {
    period: &'static str,
    days: i64,
    pool: &'static str,
    pools: Vec<i32>,
    resolution: CipherscanFlowResolution,
    amount_format: CipherscanFlowAmountFormat,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TransactionHistoryCountCacheKey {
    is_coinbase: Option<bool>,
    privacy_shapes: Vec<i32>,
    contains_any_protocol: Vec<i32>,
    minimum_shielded_component_count: u32,
    read_fence: TransactionHistoryReadFenceCacheKey,
}

impl TransactionHistoryCountCacheKey {
    fn new(filter: &TransactionHistoryFilter, read_fence: &TransactionHistoryReadFence) -> Self {
        Self {
            is_coinbase: filter.is_coinbase,
            privacy_shapes: filter.privacy_shapes.clone(),
            contains_any_protocol: filter.contains_any_protocol.clone(),
            minimum_shielded_component_count: filter.minimum_shielded_component_count,
            read_fence: TransactionHistoryReadFenceCacheKey::from(read_fence),
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TransactionHistoryReadFenceCacheKey {
    chain_epoch_id: u64,
    materialized_view_revision: u64,
    materialized_view_tip_height: u32,
    materialized_view_tip_hash: String,
}

impl From<&TransactionHistoryReadFence> for TransactionHistoryReadFenceCacheKey {
    fn from(read_fence: &TransactionHistoryReadFence) -> Self {
        Self {
            chain_epoch_id: read_fence.chain_epoch_id,
            materialized_view_revision: read_fence.materialized_view_revision,
            materialized_view_tip_height: read_fence.materialized_view_tip_height,
            materialized_view_tip_hash: read_fence.materialized_view_tip_hash.clone(),
        }
    }
}

struct CachedTransactionHistoryCount {
    total: u64,
    expires_at: Instant,
}

fn require_full_transaction_history_count(
    response: &TransactionHistoryResponse,
) -> Result<u64, CipherscanRestError> {
    if TransactionHistoryCountScope::try_from(response.count_scope)
        != Ok(TransactionHistoryCountScope::FullHistory)
    {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "transaction_history.count_scope",
        ));
    }
    response
        .total_matching_transactions
        .ok_or(CipherscanRestError::MissingUpstreamField(
            "transaction_history.total_matching_transactions",
        ))
}

fn advance_transaction_history_read_fence(
    read_fence: &mut Option<TransactionHistoryReadFence>,
    response: &TransactionHistoryResponse,
) -> Result<(), CipherscanRestError> {
    let response_read_fence =
        response
            .read_fence
            .clone()
            .ok_or(CipherscanRestError::MissingUpstreamField(
                "transaction_history.read_fence",
            ))?;
    if read_fence
        .as_ref()
        .is_some_and(|expected| expected != &response_read_fence)
    {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "transaction_history.read_fence",
        ));
    }
    *read_fence = Some(response_read_fence);
    Ok(())
}

impl OrchardCandidateScan {
    fn observe_page(
        &mut self,
        response: TransactionHistoryResponse,
        range: OrchardCandidateScanRange,
    ) -> Result<(), CipherscanRestError> {
        advance_transaction_history_read_fence(&mut self.read_fence, &response)?;
        let read_fence =
            self.read_fence
                .as_ref()
                .ok_or(CipherscanRestError::MissingUpstreamField(
                    "transaction_history.read_fence",
                ))?;
        let coverage = response
            .coverage
            .ok_or(CipherscanRestError::CandidateScanUnavailable(
                "transaction-history coverage is unavailable",
            ))?;
        validate_orchard_candidate_scan_coverage(&coverage, read_fence, range)?;
        if self
            .coverage
            .as_ref()
            .is_some_and(|expected| expected != &coverage)
        {
            return Err(CipherscanRestError::InvalidUpstreamField(
                "transaction_history.coverage",
            ));
        }
        self.coverage = Some(coverage);

        if !transaction_history_entries_are_newest_first(&response.entries) {
            return Err(CipherscanRestError::InvalidUpstreamField(
                "transaction_history.entries.order",
            ));
        }
        for entry in response.entries {
            if entry
                .component_counts
                .as_ref()
                .is_none_or(|counts| counts.orchard_action_count == 0)
            {
                return Err(CipherscanRestError::InvalidUpstreamField(
                    "transaction_history.entries.component_counts.orchard_action_count",
                ));
            }
            if !self
                .seen_coordinates
                .insert((entry.block_height, entry.transaction_index))
            {
                return Err(CipherscanRestError::InvalidUpstreamField(
                    "transaction_history.entries.coordinate",
                ));
            }
            if entry.block_height >= range.start_height && entry.block_height <= range.end_height {
                self.entries.push(entry);
                if self.entries.len() > MAX_RAW_TRANSACTION_BATCH_SIZE {
                    return Err(CipherscanRestError::CandidateScanUnavailable(
                        "the requested range exceeds the bounded candidate result size",
                    ));
                }
            }
        }
        Ok(())
    }

    fn sort_newest_first(&mut self) {
        self.entries.sort_unstable_by(|left, right| {
            (right.block_height, right.transaction_index)
                .cmp(&(left.block_height, left.transaction_index))
        });
    }
}

fn validate_orchard_candidate_scan_coverage(
    coverage: &explorer::TransactionHistoryCoverage,
    read_fence: &TransactionHistoryReadFence,
    range: OrchardCandidateScanRange,
) -> Result<(), CipherscanRestError> {
    if coverage.complete_from_height > range.start_height
        || coverage.complete_through_height < range.end_height
    {
        return Err(CipherscanRestError::CandidateScanUnavailable(
            "transaction-history coverage does not include the requested range",
        ));
    }
    let coverage_range_is_valid = coverage.complete_from_height <= coverage.complete_through_height;
    let coverage_is_within_fence =
        coverage.complete_through_height <= read_fence.materialized_view_tip_height;
    let coverage_tip_matches_fence = coverage.complete_through_height
        != read_fence.materialized_view_tip_height
        || coverage.complete_through_hash == read_fence.materialized_view_tip_hash;
    if !coverage_range_is_valid || !coverage_is_within_fence || !coverage_tip_matches_fence {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "transaction_history.coverage",
        ));
    }
    Ok(())
}

struct CachedMiningProductionWindow {
    window: Arc<MiningProductionWindow>,
    expires_at: Instant,
}

struct CachedMiningPoolDistributionResponse {
    body: Value,
    expires_at: Instant,
}

struct CachedFeeDistributionResponse {
    body: Value,
    expires_at: Instant,
}

struct CachedFlowAnalyticsResponse {
    body: Value,
    expires_at: Instant,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CommonAmountsCacheKey {
    period: String,
    limit: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CommonAmountsPeriod {
    echoed: String,
    seconds: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CipherscanAmountBucket {
    minimum_amount_zat: u64,
    maximum_amount_zat: Option<u64>,
    label: &'static str,
}

impl CipherscanAmountBucket {
    const fn new(
        minimum_amount_zat: u64,
        maximum_amount_zat: Option<u64>,
        label: &'static str,
    ) -> Self {
        Self {
            minimum_amount_zat,
            maximum_amount_zat,
            label,
        }
    }
}

#[derive(Clone, Debug)]
struct CachedMigrationAnalytics {
    analytics: MigrationAnalytics,
    expires_at: Instant,
}

#[derive(Clone, Debug)]
enum MigrationAnalyticsState {
    Available(MigrationAnalytics),
    Unavailable(MigrationAnalyticsUnavailable),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MigrationAnalyticsUnavailable {
    ActivationUnknown,
    CapabilityUnavailable,
    IntrinsicValueBalanceUnavailable,
    HistoryCoverageIncomplete,
}

impl MigrationAnalyticsUnavailable {
    fn reason(self) -> &'static str {
        match self {
            Self::ActivationUnknown => {
                "Migration analytics are unavailable until the Ironwood activation height is known."
            }
            Self::CapabilityUnavailable => {
                "Migration analytics require explorer.transaction.intrinsic_value_balances_v1."
            }
            Self::IntrinsicValueBalanceUnavailable => {
                "At least one covered Ironwood transaction is missing its native intrinsic value balances. Missing balances are not interpreted as zero."
            }
            Self::HistoryCoverageIncomplete => {
                "The bounded native Ironwood transaction-history scan did not reach activation with complete coverage."
            }
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct MigrationAnalytics {
    total_migrated_zat: u64,
    transaction_count: u64,
    first_height: Option<u32>,
    last_height: Option<u32>,
    orchard_out_zat: u64,
    ironwood_in_zat: u64,
    cohorts: Vec<MigrationCohort>,
    denomination_bins: Vec<MigrationDenominationBin>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MigrationCohort {
    boundary: u32,
    boundary_start_height: u32,
    transaction_count: u64,
    volume_zat: u64,
    first_time_unix_seconds: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MigrationDenominationBin {
    power: i32,
    transaction_count: u64,
    volume_zat: u64,
}

#[derive(Debug, Default)]
struct MigrationCohortAccumulator {
    boundary_start_height: Option<u32>,
    transaction_count: u64,
    volume_zat: u64,
    first_time_unix_seconds: Option<i64>,
}

#[derive(Debug, Default)]
struct MigrationDenominationAccumulator {
    transaction_count: u64,
    volume_zat: u64,
}

impl CipherscanRestAdapter {
    /// Creates an adapter from already-authenticated Zinder gRPC channels.
    pub fn new(
        network: Network,
        explorer_channel: AuthenticatedChannel,
        wallet_channel: AuthenticatedChannel,
        market_price_endpoints: CipherscanMarketPriceEndpoints,
        realtime_cancel: CancellationToken,
    ) -> Result<Self, MarketPriceInitializationError> {
        let (realtime_sender, _) = broadcast::channel(REALTIME_EVENT_CHANNEL_CAPACITY);
        Ok(Self {
            network,
            explorer_channel,
            wallet_channel,
            realtime_broadcaster: Arc::new(CipherscanRealtimeBroadcaster {
                sender: realtime_sender,
                is_started: AtomicBool::new(false),
            }),
            realtime_cancel,
            realtime_tasks: TaskTracker::new(),
            transaction_history_count_cache: Arc::new(RwLock::new(BTreeMap::new())),
            mining_production_cache: Arc::new(RwLock::new(BTreeMap::new())),
            mining_production_refresh: Arc::new(Mutex::new(())),
            mining_pool_distribution_cache: Arc::new(RwLock::new(BTreeMap::new())),
            fee_distribution_cache: Arc::new(RwLock::new(BTreeMap::new())),
            anonymity_set_cache: Arc::new(RwLock::new(BTreeMap::new())),
            shielding_distribution_cache: Arc::new(RwLock::new(BTreeMap::new())),
            common_amounts_cache: Arc::new(RwLock::new(BTreeMap::new())),
            blend_check_cache: Arc::new(RwLock::new(BTreeMap::new())),
            migration_analytics_cache: Arc::new(RwLock::new(None)),
            migration_analytics_refresh: Arc::new(Mutex::new(())),
            market_price_client: MarketPriceClient::new(
                market_price_endpoints.current,
                market_price_endpoints.historical_template,
            )?,
        })
    }

    /// Builds the Cipherscan-compatible HTTP router.
    pub fn router(self) -> Router {
        let router = with_chain_routes(Router::new());
        let router = with_account_and_market_routes(router);
        let router = with_name_routes(router);
        let router = with_crosschain_routes(router);
        let router = with_crosslink_and_reorg_routes(router);
        let router = with_privacy_routes(router);

        with_cors_preflight(
            with_chain_analytics_routes(router)
                .route("/", get(realtime_websocket))
                .route("/api/{*path}", any(compat_fallback))
                .with_state(self),
        )
    }

    fn explorer_client(&self) -> ExplorerQueryClient<AuthenticatedChannel> {
        ExplorerQueryClient::new(self.explorer_channel.clone())
    }

    fn wallet_client(&self) -> WalletQueryClient<AuthenticatedChannel> {
        WalletQueryClient::new(self.wallet_channel.clone())
    }

    fn subscribe_realtime_events(&self) -> broadcast::Receiver<CipherscanRealtimeDispatch> {
        let receiver = self.realtime_broadcaster.sender.subscribe();
        if !self
            .realtime_broadcaster
            .is_started
            .swap(true, Ordering::AcqRel)
        {
            self.realtime_tasks.spawn(relay_chain_events(self.clone()));
            self.realtime_tasks
                .spawn(relay_mempool_events(self.clone()));
        }
        receiver
    }

    /// Cancels and joins native realtime relays and upgraded WebSocket tasks.
    pub async fn shutdown_realtime(&self) {
        self.realtime_cancel.cancel();
        self.realtime_tasks.close();
        self.realtime_tasks.wait().await;
    }

    async fn fetch_explorer_server_info(
        &self,
    ) -> Result<explorer::ServerInfoResponse, CipherscanRestError> {
        Ok(self
            .explorer_client()
            .server_info(ServerInfoRequest {})
            .await?
            .into_inner())
    }

    async fn fetch_coinbase_total_output_zat(
        &self,
        block_height: u32,
    ) -> Result<u64, CipherscanRestError> {
        let response = self
            .explorer_client()
            .block_detail(BlockDetailRequest {
                selector: Some(block_detail_request::Selector::BlockHeight(block_height)),
                at_epoch_id: None,
            })
            .await?
            .into_inner();
        let summary = response
            .summary
            .ok_or(CipherscanRestError::MissingUpstreamField("summary"))?;

        Ok(summary.coinbase_reward_zat)
    }

    async fn fetch_recent_blocks(
        &self,
        limit: u32,
        offset: u32,
    ) -> Result<(Vec<CipherscanBlockListEntry>, u64), CipherscanRestError> {
        let (tip, at_epoch_id) = self.fetch_visible_tip_block_context().await?;
        let total = u64::from(tip.height);
        if offset > tip.height {
            return Ok((Vec::new(), total));
        }

        let end_height = tip.height.saturating_sub(offset);
        let start_height = end_height.saturating_sub(limit.saturating_sub(1));
        let mut entries = self
            .fetch_block_list_entries_in_range(start_height, end_height, at_epoch_id)
            .await?;
        entries.reverse();
        Ok((entries, total))
    }

    async fn fetch_block_list_page(
        &self,
        limit: u32,
        page: u32,
        cursor: Option<u32>,
        direction: Option<&str>,
    ) -> Result<(Vec<CipherscanBlockListEntry>, u64), CipherscanRestError> {
        let (tip, at_epoch_id) = self.fetch_visible_tip_block_context().await?;
        let total = u64::from(tip.height);
        let Some(cursor) = cursor else {
            let offset = page.saturating_sub(1).saturating_mul(limit);
            if offset > tip.height {
                return Ok((Vec::new(), total));
            }

            let end_height = tip.height.saturating_sub(offset);
            let start_height = end_height.saturating_sub(limit.saturating_sub(1));
            let mut entries = self
                .fetch_block_list_entries_in_range(start_height, end_height, at_epoch_id)
                .await?;
            entries.reverse();
            return Ok((entries, total));
        };

        let is_previous_page = matches!(direction, Some("prev"));
        let (start_height, end_height) = if is_previous_page {
            if cursor >= tip.height {
                return Ok((Vec::new(), total));
            }

            (
                cursor.saturating_add(1),
                cursor.saturating_add(limit).min(tip.height),
            )
        } else {
            if cursor == 0 {
                return Ok((Vec::new(), total));
            }

            let end_height = cursor.saturating_sub(1);
            let start_height = end_height.saturating_sub(limit.saturating_sub(1));
            (start_height, end_height)
        };

        if start_height > end_height {
            return Ok((Vec::new(), total));
        }

        let mut entries = self
            .fetch_block_list_entries_in_range(start_height, end_height, at_epoch_id)
            .await?;
        entries.reverse();
        Ok((entries, total))
    }

    async fn fetch_block_list_entries_in_range(
        &self,
        start_height: u32,
        end_height: u32,
        at_epoch_id: Option<u64>,
    ) -> Result<Vec<CipherscanBlockListEntry>, CipherscanRestError> {
        let response = self
            .explorer_client()
            .block_production_series(BlockProductionSeriesRequest {
                start_height,
                end_height,
                at_epoch_id,
            })
            .await?
            .into_inner();
        response
            .points
            .into_iter()
            .map(|point| CipherscanBlockListEntry::try_from_point(self.network, point))
            .collect()
    }

    async fn fetch_canonical_block_production_entry(
        &self,
        block_height: u32,
        block_hash: &str,
        at_epoch_id: Option<u64>,
    ) -> Result<CipherscanBlockListEntry, CipherscanRestError> {
        let mut entries = self
            .fetch_block_list_entries_in_range(block_height, block_height, at_epoch_id)
            .await?;
        if entries.len() != 1 {
            return Err(CipherscanRestError::InvalidUpstreamField(
                "block_production_series.points",
            ));
        }
        let entry = entries
            .pop()
            .ok_or(CipherscanRestError::MissingUpstreamField(
                "block_production_series.points[0]",
            ))?;
        if entry.summary.block_height != block_height || entry.summary.block_hash != block_hash {
            return Err(CipherscanRestError::InvalidUpstreamField(
                "block_production_series.points[0].summary",
            ));
        }
        Ok(entry)
    }

    async fn fetch_network_activity_window(
        &self,
        tip_height: u32,
        at_epoch_id: u64,
        cutoff_unix_seconds: i64,
    ) -> Result<NetworkActivityWindow, CipherscanRestError> {
        let minimum_height = tip_height.saturating_sub(MAX_NETWORK_STATS_BLOCKS - 1);
        let mut end_height = tip_height;
        let mut entries = Vec::new();

        loop {
            let start_height = end_height
                .saturating_sub(BLOCK_SUMMARY_PAGE_SIZE - 1)
                .max(minimum_height);
            let page = self
                .fetch_block_list_entries_in_range(start_height, end_height, Some(at_epoch_id))
                .await?;
            let expected_count = end_height.saturating_sub(start_height).saturating_add(1);
            if u32::try_from(page.len()).ok() != Some(expected_count) {
                return Err(CipherscanRestError::InvalidUpstreamField(
                    "block_production_series.coverage",
                ));
            }
            let reached_cutoff = page
                .first()
                .is_some_and(|entry| entry.summary.block_time_unix_seconds < cutoff_unix_seconds);
            entries.extend(page);

            if reached_cutoff || start_height == 0 {
                break;
            }
            if start_height == minimum_height {
                return Err(CipherscanRestError::InvalidUpstreamField(
                    "network_stats.activity_window",
                ));
            }
            end_height = start_height.saturating_sub(1);
        }

        NetworkActivityWindow::from_entries(&entries, tip_height, cutoff_unix_seconds)
    }

    async fn fetch_mining_production_window(
        &self,
        period: MiningProductionPeriod,
        generated_at: OffsetDateTime,
    ) -> Result<MiningProductionWindow, CipherscanRestError> {
        let requested_cutoff_unix_seconds =
            period.cutoff_unix_seconds(generated_at.unix_timestamp());
        let requested_end_time_unix_seconds = generated_at.unix_timestamp().saturating_add(1);
        let (tip, at_epoch_id) = self.fetch_visible_tip_block_context().await?;
        let chain_epoch_id = at_epoch_id.ok_or(CipherscanRestError::MissingUpstreamField(
            "visible_tip_block.chain_view.chain_epoch",
        ))?;
        let minimum_height = tip
            .height
            .saturating_sub(MAX_MINING_PRODUCTION_BLOCKS.saturating_sub(1));
        let mut end_height = tip.height;
        let mut entries = Vec::new();
        let mut scanned_block_count = 0_u32;
        let mut covered_from_unix_seconds = None::<i64>;
        let mut covered_through_unix_seconds = None::<i64>;
        let mut coverage_complete = false;

        loop {
            let start_height = end_height
                .saturating_sub(BLOCK_SUMMARY_PAGE_SIZE.saturating_sub(1))
                .max(minimum_height);
            let page = self
                .fetch_block_list_entries_in_range(start_height, end_height, Some(chain_epoch_id))
                .await?;
            validate_mining_production_page(&page, start_height, end_height)?;

            scanned_block_count =
                scanned_block_count.saturating_add(u32::try_from(page.len()).unwrap_or(u32::MAX));
            for entry in &page {
                let summary = &entry.summary;
                covered_from_unix_seconds = Some(
                    covered_from_unix_seconds.map_or(summary.block_time_unix_seconds, |current| {
                        current.min(summary.block_time_unix_seconds)
                    }),
                );
                covered_through_unix_seconds = Some(
                    covered_through_unix_seconds
                        .map_or(summary.block_time_unix_seconds, |current| {
                            current.max(summary.block_time_unix_seconds)
                        }),
                );
            }

            let page_is_before_cutoff =
                mining_production_page_is_before_cutoff(&page, requested_cutoff_unix_seconds);
            entries.extend(page.into_iter().filter(|entry| {
                mining_production_entry_is_in_period(
                    entry,
                    requested_cutoff_unix_seconds,
                    requested_end_time_unix_seconds,
                )
            }));

            if page_is_before_cutoff || start_height == 0 {
                coverage_complete = true;
                break;
            }
            if start_height == minimum_height {
                break;
            }
            end_height = start_height.saturating_sub(1);
        }

        entries.sort_unstable_by_key(|entry| entry.summary.block_height);
        Ok(MiningProductionWindow {
            entries,
            requested_cutoff_unix_seconds,
            covered_from_unix_seconds,
            covered_through_unix_seconds,
            scanned_block_count,
            coverage_complete,
            generated_at,
        })
    }

    async fn mining_production_window(
        &self,
        period: MiningProductionPeriod,
    ) -> Result<Arc<MiningProductionWindow>, CipherscanRestError> {
        if let Some(window) = self.cached_mining_production_window(period).await {
            return Ok(window);
        }

        let _refresh_guard = self.mining_production_refresh.lock().await;
        if let Some(window) = self.cached_mining_production_window(period).await {
            return Ok(window);
        }
        let window = Arc::new(
            self.fetch_mining_production_window(period, OffsetDateTime::now_utc())
                .await?,
        );
        if window.coverage_complete {
            self.cache_mining_production_window(period, Arc::clone(&window))
                .await;
        }
        Ok(window)
    }

    async fn cached_mining_production_window(
        &self,
        period: MiningProductionPeriod,
    ) -> Option<Arc<MiningProductionWindow>> {
        let cache = self.mining_production_cache.read().await;
        cache
            .get(&period)
            .filter(|cached| cached.expires_at > Instant::now())
            .map(|cached| Arc::clone(&cached.window))
    }

    async fn cache_mining_production_window(
        &self,
        period: MiningProductionPeriod,
        window: Arc<MiningProductionWindow>,
    ) {
        let mut cache = self.mining_production_cache.write().await;
        cache.retain(|_, cached| cached.expires_at > Instant::now());
        cache.insert(
            period,
            CachedMiningProductionWindow {
                window,
                expires_at: Instant::now() + MINING_PRODUCTION_CACHE_TTL,
            },
        );
    }

    async fn fetch_mining_pool_distribution_window(
        &self,
        period: MiningProductionPeriod,
        generated_at: OffsetDateTime,
    ) -> Result<MiningPoolDistributionWindow, CipherscanRestError> {
        let start_time_unix_seconds = period
            .cutoff_unix_seconds(generated_at.unix_timestamp())
            .unwrap_or(i64::MIN);
        let end_time_unix_seconds = i64::MAX;
        let mut client = self.explorer_client();
        let mut cursor = Vec::new();
        let mut chain_epoch_id = None;
        let mut read_fence = None;
        let mut payout_by_address = BTreeMap::<Option<String>, MiningPayoutAggregate>::new();

        loop {
            let response = client
                .block_production_in_time_range(BlockProductionInTimeRangeRequest {
                    start_time_unix_seconds,
                    end_time_unix_seconds,
                    max_entries: 1_000,
                    from_cursor: cursor,
                    at_epoch_id: chain_epoch_id,
                })
                .await?
                .into_inner();
            validate_mining_pool_distribution_page(&response, read_fence.as_ref())?;
            let page_fence =
                response
                    .read_fence
                    .clone()
                    .ok_or(CipherscanRestError::MissingUpstreamField(
                        "block_production_in_time_range.read_fence",
                    ))?;
            chain_epoch_id = Some(page_fence.chain_epoch_id);
            read_fence = Some(page_fence);
            for point in response.points {
                let entry = CipherscanBlockListEntry::try_from_point(self.network, point)?;
                let paid_fee_zat = entry.summary.paid_fees_collected_zat.ok_or(
                    CipherscanRestError::MissingUpstreamField(
                        "block_production_in_time_range.points.summary.paid_fees_collected_zat",
                    ),
                )?;
                let payout = payout_by_address.entry(entry.miner_address).or_default();
                payout.block_count = payout.block_count.checked_add(1).ok_or(
                    CipherscanRestError::InvalidUpstreamField(
                        "block_production_in_time_range.covered_block_count",
                    ),
                )?;
                payout.total_fees_zat = payout.total_fees_zat.checked_add(paid_fee_zat).ok_or(
                    CipherscanRestError::InvalidUpstreamField(
                        "block_production_in_time_range.points.summary.paid_fees_collected_zat",
                    ),
                )?;
            }
            if response.next_cursor.is_empty() {
                break;
            }
            cursor = response.next_cursor;
        }

        Ok(MiningPoolDistributionWindow {
            payout_by_address,
            generated_at,
        })
    }

    async fn cached_mining_pool_distribution_response(&self, cache_key: &str) -> Option<Value> {
        let cache = self.mining_pool_distribution_cache.read().await;
        cache
            .get(cache_key)
            .filter(|cached| cached.expires_at > Instant::now())
            .map(|cached| cached.body.clone())
    }

    async fn cache_mining_pool_distribution_response(&self, cache_key: String, body: Value) {
        let mut cache = self.mining_pool_distribution_cache.write().await;
        cache.retain(|_, cached| cached.expires_at > Instant::now());
        cache.insert(
            cache_key,
            CachedMiningPoolDistributionResponse {
                body,
                expires_at: Instant::now() + MINING_PRODUCTION_CACHE_TTL,
            },
        );
    }

    async fn cached_fee_distribution_response(&self, cache_key: &str) -> Option<Value> {
        let cache = self.fee_distribution_cache.read().await;
        cache
            .get(cache_key)
            .filter(|cached| cached.expires_at > Instant::now())
            .map(|cached| cached.body.clone())
    }

    async fn cache_fee_distribution_response(&self, cache_key: String, body: Value) {
        let mut cache = self.fee_distribution_cache.write().await;
        cache.retain(|_, cached| cached.expires_at > Instant::now());
        cache.insert(
            cache_key,
            CachedFeeDistributionResponse {
                body,
                expires_at: Instant::now() + FEE_DISTRIBUTION_CACHE_TTL,
            },
        );
    }

    async fn cached_anonymity_set_response(&self, period: &str) -> Option<Value> {
        let cache = self.anonymity_set_cache.read().await;
        cache
            .get(period)
            .filter(|cached| cached.expires_at > Instant::now())
            .map(|cached| cached.body.clone())
    }

    async fn cache_anonymity_set_response(&self, period: String, body: Value) {
        let mut cache = self.anonymity_set_cache.write().await;
        cache.retain(|_, cached| cached.expires_at > Instant::now());
        cache.insert(
            period,
            CachedFlowAnalyticsResponse {
                body,
                expires_at: Instant::now() + FLOW_ANALYTICS_CACHE_TTL,
            },
        );
    }

    async fn cached_shielding_distribution_response(&self, period: &str) -> Option<Value> {
        let cache = self.shielding_distribution_cache.read().await;
        cache
            .get(period)
            .filter(|cached| cached.expires_at > Instant::now())
            .map(|cached| cached.body.clone())
    }

    async fn cache_shielding_distribution_response(&self, period: String, body: Value) {
        let mut cache = self.shielding_distribution_cache.write().await;
        cache.retain(|_, cached| cached.expires_at > Instant::now());
        cache.insert(
            period,
            CachedFlowAnalyticsResponse {
                body,
                expires_at: Instant::now() + FLOW_ANALYTICS_CACHE_TTL,
            },
        );
    }

    async fn cached_common_amounts_response(&self, key: &CommonAmountsCacheKey) -> Option<Value> {
        self.common_amounts_cache
            .read()
            .await
            .get(key)
            .filter(|cached| cached.expires_at > Instant::now())
            .map(|cached| cached.body.clone())
    }

    async fn cache_common_amounts_response(&self, key: CommonAmountsCacheKey, body: Value) {
        let mut cache = self.common_amounts_cache.write().await;
        cache.retain(|_, cached| cached.expires_at > Instant::now());
        if cache.len() >= MAX_COMMON_AMOUNTS_CACHE_ENTRIES
            && let Some(oldest_key) = cache
                .iter()
                .min_by_key(|(_, cached)| cached.expires_at)
                .map(|(key, _)| key.clone())
        {
            cache.remove(&oldest_key);
        }
        cache.insert(
            key,
            CachedFlowAnalyticsResponse {
                body,
                expires_at: Instant::now() + COMMON_AMOUNTS_CACHE_TTL,
            },
        );
    }

    async fn cached_blend_check_response(&self, cache_key: &str) -> Option<Value> {
        let cache = self.blend_check_cache.read().await;
        cache
            .get(cache_key)
            .filter(|cached| cached.expires_at > Instant::now())
            .map(|cached| cached.body.clone())
    }

    async fn cache_blend_check_response(&self, cache_key: String, body: Value) {
        let mut cache = self.blend_check_cache.write().await;
        cache.retain(|_, cached| cached.expires_at > Instant::now());
        if cache.len() >= MAX_BLEND_CHECK_CACHE_ENTRIES
            && let Some(oldest_key) = cache
                .iter()
                .min_by_key(|(_, cached)| cached.expires_at)
                .map(|(key, _)| key.clone())
        {
            cache.remove(&oldest_key);
        }
        cache.insert(
            cache_key,
            CachedFlowAnalyticsResponse {
                body,
                expires_at: Instant::now() + BLEND_CHECK_CACHE_TTL,
            },
        );
    }

    async fn fetch_migration_analytics(
        &self,
    ) -> Result<MigrationAnalyticsState, CipherscanRestError> {
        let Some(activation_height) = migration_activation_height(self.network) else {
            return Ok(MigrationAnalyticsState::Unavailable(
                MigrationAnalyticsUnavailable::ActivationUnknown,
            ));
        };
        let server_info = self.fetch_explorer_server_info().await?;
        if !explorer_supports_capability(
            &server_info,
            EXPLORER_TRANSACTION_INTRINSIC_VALUE_BALANCES_V1,
        ) || !explorer_supports_capability(&server_info, EXPLORER_TRANSACTION_HISTORY_V2)
        {
            return Ok(MigrationAnalyticsState::Unavailable(
                MigrationAnalyticsUnavailable::CapabilityUnavailable,
            ));
        }

        if let Some(analytics) = self.cached_migration_analytics().await {
            return Ok(MigrationAnalyticsState::Available(analytics));
        }

        let _refresh_guard = self.migration_analytics_refresh.lock().await;
        if let Some(analytics) = self.cached_migration_analytics().await {
            return Ok(MigrationAnalyticsState::Available(analytics));
        }

        let state = self.scan_migration_analytics(activation_height).await?;
        if let MigrationAnalyticsState::Available(analytics) = &state {
            *self.migration_analytics_cache.write().await = Some(CachedMigrationAnalytics {
                analytics: analytics.clone(),
                expires_at: Instant::now() + MIGRATION_ANALYTICS_CACHE_TTL,
            });
        }
        Ok(state)
    }

    async fn cached_migration_analytics(&self) -> Option<MigrationAnalytics> {
        self.migration_analytics_cache
            .read()
            .await
            .as_ref()
            .filter(|cached| cached.expires_at > Instant::now())
            .map(|cached| cached.analytics.clone())
    }

    async fn scan_migration_analytics(
        &self,
        activation_height: u32,
    ) -> Result<MigrationAnalyticsState, CipherscanRestError> {
        for attempt in 0..2 {
            match self.scan_migration_analytics_once(activation_height).await {
                Err(CipherscanRestError::Upstream(status))
                    if attempt == 0 && status.code() == Code::FailedPrecondition => {}
                outcome => return outcome,
            }
        }
        Err(CipherscanRestError::InvalidUpstreamField(
            "transaction_history.read_fence",
        ))
    }

    async fn scan_migration_analytics_once(
        &self,
        activation_height: u32,
    ) -> Result<MigrationAnalyticsState, CipherscanRestError> {
        let filter = TransactionHistoryFilter {
            contains_any_protocol: vec![explorer::ShieldedProtocol::Ironwood as i32],
            ..TransactionHistoryFilter::default()
        };
        let anchor_height =
            activation_height
                .checked_sub(1)
                .ok_or(CipherscanRestError::InvalidUpstreamField(
                    "migration.activation_height",
                ))?;
        let mut entries = Vec::new();
        let mut cursor = None;
        let mut read_fence = None;
        let mut scanned_entry_count = 0_u64;

        loop {
            let remaining_capacity = MAX_MIGRATION_HISTORY_ENTRIES.saturating_sub(entries.len());
            if remaining_capacity == 0 {
                return Ok(MigrationAnalyticsState::Unavailable(
                    MigrationAnalyticsUnavailable::HistoryCoverageIncomplete,
                ));
            }
            let page_size = TRANSACTION_HISTORY_PAGE_SIZE
                .min(u32::try_from(remaining_capacity).unwrap_or(TRANSACTION_HISTORY_PAGE_SIZE));
            let response = self
                .fetch_transaction_history(TransactionHistoryRequest {
                    page_size,
                    start: Some(cursor.clone().map_or_else(
                        || {
                            transaction_history_request::Start::Anchor(TransactionHistoryAnchor {
                                block_height: anchor_height,
                                transaction_index: 0,
                            })
                        },
                        transaction_history_request::Start::Cursor,
                    )),
                    direction: TransactionHistoryDirection::Newer as i32,
                    filter: Some(filter.clone()),
                    include_total_count: false,
                    read_fence: read_fence.clone(),
                })
                .await?;
            advance_transaction_history_read_fence(&mut read_fence, &response)?;

            scanned_entry_count = scanned_entry_count
                .checked_add(u64::from(response.scanned_entry_count))
                .ok_or(CipherscanRestError::InvalidUpstreamField(
                    "transaction_history.scanned_entry_count",
                ))?;
            if scanned_entry_count > MAX_MIGRATION_SCANNED_HISTORY_ENTRIES
                || response.entries.len() > remaining_capacity
            {
                return Ok(MigrationAnalyticsState::Unavailable(
                    MigrationAnalyticsUnavailable::HistoryCoverageIncomplete,
                ));
            }

            if !transaction_history_entries_are_newest_first(&response.entries) {
                return Err(CipherscanRestError::InvalidUpstreamField(
                    "transaction_history.entries.order",
                ));
            }
            entries.extend(
                response
                    .entries
                    .into_iter()
                    .filter(|entry| entry.block_height >= activation_height),
            );
            if !response.has_newer {
                return migration_analytics_from_entries(&entries);
            }
            if response.newer_cursor.is_empty() || cursor.as_ref() == Some(&response.newer_cursor) {
                return Err(CipherscanRestError::InvalidUpstreamField(
                    "transaction_history.newer_cursor",
                ));
            }
            cursor = Some(response.newer_cursor);
        }
    }

    async fn fetch_conventional_fee_distribution(
        &self,
        start_time_unix_seconds: i64,
        end_time_unix_seconds: i64,
    ) -> Result<ConventionalFeeDistributionResponse, CipherscanRestError> {
        Ok(self
            .explorer_client()
            .conventional_fee_distribution(ConventionalFeeDistributionRequest {
                start_time_unix_seconds,
                end_time_unix_seconds,
            })
            .await?
            .into_inner())
    }

    async fn fetch_paid_fee_distribution(
        &self,
        start_time_unix_seconds: i64,
        end_time_unix_seconds: i64,
    ) -> Result<PaidFeeDistributionResponse, CipherscanRestError> {
        Ok(self
            .explorer_client()
            .paid_fee_distribution(PaidFeeDistributionRequest {
                start_time_unix_seconds,
                end_time_unix_seconds,
            })
            .await?
            .into_inner())
    }

    async fn fetch_value_pool_flow_amount_threshold_summary(
        &self,
        request: ValuePoolFlowAmountThresholdSummaryRequest,
    ) -> Result<ValuePoolFlowAmountThresholdSummaryResponse, CipherscanRestError> {
        Ok(self
            .explorer_client()
            .value_pool_flow_amount_threshold_summary(request)
            .await?
            .into_inner())
    }

    async fn fetch_value_pool_flow_rounded_amount_summary(
        &self,
        request: ValuePoolFlowRoundedAmountSummaryRequest,
    ) -> Result<ValuePoolFlowRoundedAmountSummaryResponse, CipherscanRestError> {
        Ok(self
            .explorer_client()
            .value_pool_flow_rounded_amount_summary(request)
            .await?
            .into_inner())
    }

    async fn fetch_block_activity_distribution(
        &self,
        block_limit: u32,
    ) -> Result<explorer::BlockActivityDistributionResponse, CipherscanRestError> {
        let tip = self.fetch_visible_tip_block().await?;
        let start_height = tip.height.saturating_sub(block_limit.saturating_sub(1));
        Ok(self
            .explorer_client()
            .block_activity_distribution(BlockActivityDistributionRequest {
                start_height,
                end_height: tip.height,
            })
            .await?
            .into_inner())
    }

    async fn fetch_transaction_history(
        &self,
        request: TransactionHistoryRequest,
    ) -> Result<TransactionHistoryResponse, CipherscanRestError> {
        Ok(self
            .explorer_client()
            .transaction_history(request)
            .await?
            .into_inner())
    }

    async fn fetch_orchard_candidates(
        &self,
        range: OrchardCandidateScanRange,
    ) -> Result<OrchardCandidateScan, CipherscanRestError> {
        let filter = TransactionHistoryFilter {
            contains_any_protocol: vec![explorer::ShieldedProtocol::Orchard as i32],
            ..TransactionHistoryFilter::default()
        };
        let mut scan = OrchardCandidateScan::default();
        let mut cursor = None;
        let (direction, initial_start) = if range.start_height == 0 {
            (TransactionHistoryDirection::Older, None)
        } else {
            (
                TransactionHistoryDirection::Newer,
                Some(transaction_history_request::Start::Anchor(
                    TransactionHistoryAnchor {
                        block_height: range.start_height - 1,
                        transaction_index: 0,
                    },
                )),
            )
        };

        for _ in 0..MAX_ORCHARD_CANDIDATE_SCAN_PAGES {
            let response = self
                .fetch_transaction_history(TransactionHistoryRequest {
                    page_size: TRANSACTION_HISTORY_PAGE_SIZE,
                    start: cursor.clone().map_or_else(
                        || initial_start.clone(),
                        |cursor| Some(transaction_history_request::Start::Cursor(cursor)),
                    ),
                    direction: direction as i32,
                    filter: Some(filter.clone()),
                    include_total_count: false,
                    read_fence: scan.read_fence.clone(),
                })
                .await?;
            let (has_more, next_cursor) = if direction == TransactionHistoryDirection::Newer {
                (response.has_newer, response.newer_cursor.clone())
            } else {
                (response.has_older, response.older_cursor.clone())
            };
            scan.observe_page(response, range)?;
            if !has_more {
                scan.sort_newest_first();
                return Ok(scan);
            }
            if next_cursor.is_empty() || cursor.as_ref() == Some(&next_cursor) {
                return Err(CipherscanRestError::InvalidUpstreamField(
                    "transaction_history.cursor",
                ));
            }
            cursor = Some(next_cursor);
        }

        Err(CipherscanRestError::CandidateScanUnavailable(
            "the requested range exceeds the bounded native candidate scan",
        ))
    }

    async fn require_orchard_candidate_raw_bytes(
        &self,
        scan: &OrchardCandidateScan,
    ) -> Result<(), CipherscanRestError> {
        let read_fence =
            scan.read_fence
                .as_ref()
                .ok_or(CipherscanRestError::MissingUpstreamField(
                    "transaction_history.read_fence",
                ))?;
        let mut wallet_client = self.wallet_client();
        for entry in &scan.entries {
            let response = wallet_client
                .transaction(TransactionRequest {
                    transaction_id: entry.transaction_id.clone(),
                    at_epoch_id: Some(read_fence.chain_epoch_id),
                })
                .await?
                .into_inner();
            if raw_transaction_bytes(response.location.as_ref()).is_none_or(<[u8]>::is_empty) {
                return Err(CipherscanRestError::CandidateScanUnavailable(
                    "a candidate transaction lacks retained raw bytes",
                ));
            }
        }
        Ok(())
    }

    async fn require_explorer_capability(
        &self,
        capability: &'static str,
    ) -> Result<(), CipherscanRestError> {
        let server_info = self.fetch_explorer_server_info().await?;
        if explorer_supports_capability(&server_info, capability) {
            return Ok(());
        }
        Err(CipherscanRestError::MissingUpstreamField(capability))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Legacy forward and reverse cursor translation is one pagination state machine."
    )]
    async fn fetch_shielded_flow_page(
        &self,
        filter: ValuePoolFlowFilter,
        anchor: Option<CipherscanFlowCursor>,
        direction: ShieldedFlowPageDirection,
        limit: u32,
    ) -> Result<ShieldedFlowPage, CipherscanRestError> {
        if anchor.is_some_and(|cursor| {
            cursor.coordinate.transaction_index > MAX_CIPHERSCAN_FLOW_TRANSACTION_INDEX
        }) {
            return Err(CipherscanRestError::InvalidRequest(format!(
                "cursor_id must not exceed {MAX_CIPHERSCAN_FLOW_TRANSACTION_INDEX}"
            )));
        }

        let mut native_cursor = Vec::new();
        let mut previous_cursor = None;
        let mut newer_events = Vec::new();
        let mut selected_events = Vec::new();
        let mut anchor_found = anchor.is_none();
        let mut total_matching_events = None;
        let mut coverage = None;

        loop {
            let response = self
                .explorer_client()
                .value_pool_flow_history(ValuePoolFlowHistoryRequest {
                    page_size: VALUE_POOL_FLOW_NATIVE_PAGE_SIZE,
                    cursor: native_cursor.clone(),
                    filter: Some(filter.clone()),
                    include_total_count: total_matching_events.is_none(),
                })
                .await?
                .into_inner();
            let page_total = response.total_matching_events;
            if total_matching_events.is_none() {
                total_matching_events =
                    Some(page_total.ok_or(CipherscanRestError::MissingUpstreamField(
                        "value_pool_flow_history.total_matching_events",
                    ))?);
                coverage = Some(response.coverage.ok_or(
                    CipherscanRestError::MissingUpstreamField("value_pool_flow_history.coverage"),
                )?);
            }

            for event in response.events.iter().cloned() {
                validate_value_pool_flow_event(&event)?;
                let cursor = CipherscanFlowCursor::from_event(&event)?;
                if previous_cursor.is_some_and(|previous| previous <= cursor) {
                    return Err(CipherscanRestError::InvalidUpstreamField(
                        "value_pool_flow_history.events.order",
                    ));
                }
                previous_cursor = Some(cursor);

                if !anchor_found {
                    if Some(cursor) == anchor {
                        anchor_found = true;
                        if direction == ShieldedFlowPageDirection::Newer {
                            break;
                        }
                    } else {
                        newer_events.push(event);
                    }
                    continue;
                }

                if direction == ShieldedFlowPageDirection::Older {
                    selected_events.push(event);
                    if u32::try_from(selected_events.len()).unwrap_or(u32::MAX) > limit {
                        break;
                    }
                }
            }

            if !anchor_found && direction == ShieldedFlowPageDirection::Newer {
                if response.has_more {
                    native_cursor = next_value_pool_flow_cursor(&native_cursor, &response)?;
                    continue;
                }
                return Err(CipherscanRestError::InvalidRequest(
                    "cursor and cursor_id do not identify a matching flow".to_owned(),
                ));
            }

            if !anchor_found && !response.has_more {
                return Err(CipherscanRestError::InvalidRequest(
                    "cursor and cursor_id do not identify a matching flow".to_owned(),
                ));
            }

            if direction == ShieldedFlowPageDirection::Newer {
                let start = newer_events
                    .len()
                    .saturating_sub(usize::try_from(limit).unwrap_or(0));
                let events = newer_events.split_off(start);
                return Ok(ShieldedFlowPage {
                    has_newer: !newer_events.is_empty(),
                    has_older: anchor.is_some(),
                    events,
                    total_matching_events: total_matching_events.ok_or(
                        CipherscanRestError::MissingUpstreamField(
                            "value_pool_flow_history.total_matching_events",
                        ),
                    )?,
                    coverage: coverage.ok_or(CipherscanRestError::MissingUpstreamField(
                        "value_pool_flow_history.coverage",
                    ))?,
                });
            }

            if u32::try_from(selected_events.len()).unwrap_or(u32::MAX) > limit {
                selected_events.pop();
                return Ok(ShieldedFlowPage {
                    events: selected_events,
                    total_matching_events: total_matching_events.ok_or(
                        CipherscanRestError::MissingUpstreamField(
                            "value_pool_flow_history.total_matching_events",
                        ),
                    )?,
                    has_older: true,
                    has_newer: !newer_events.is_empty(),
                    coverage: coverage.ok_or(CipherscanRestError::MissingUpstreamField(
                        "value_pool_flow_history.coverage",
                    ))?,
                });
            }
            if !response.has_more {
                return Ok(ShieldedFlowPage {
                    events: selected_events,
                    total_matching_events: total_matching_events.ok_or(
                        CipherscanRestError::MissingUpstreamField(
                            "value_pool_flow_history.total_matching_events",
                        ),
                    )?,
                    has_older: false,
                    has_newer: !newer_events.is_empty(),
                    coverage: coverage.ok_or(CipherscanRestError::MissingUpstreamField(
                        "value_pool_flow_history.coverage",
                    ))?,
                });
            }
            native_cursor = next_value_pool_flow_cursor(&native_cursor, &response)?;
        }
    }

    async fn fetch_transaction_component_summary(
        &self,
        start_time_unix_seconds: i64,
        end_time_unix_seconds: i64,
        totals_only: bool,
    ) -> Result<TransactionComponentSummaryResponse, CipherscanRestError> {
        Ok(self
            .explorer_client()
            .transaction_component_summary(TransactionComponentSummaryRequest {
                start_time_unix_seconds,
                end_time_unix_seconds,
                totals_only,
            })
            .await?
            .into_inner())
    }

    async fn fetch_transaction_history_with_cached_count(
        &self,
        request: TransactionHistoryRequest,
    ) -> Result<TransactionHistoryResponse, CipherscanRestError> {
        for attempt in 0..2 {
            match self
                .fetch_transaction_history_with_cached_count_once(request.clone())
                .await
            {
                Err(CipherscanRestError::Upstream(status))
                    if attempt == 0 && status.code() == Code::FailedPrecondition => {}
                outcome => return outcome,
            }
        }
        Err(CipherscanRestError::InvalidUpstreamField(
            "transaction_history.read_fence",
        ))
    }

    async fn fetch_transaction_history_with_cached_count_once(
        &self,
        mut request: TransactionHistoryRequest,
    ) -> Result<TransactionHistoryResponse, CipherscanRestError> {
        let filter = request.filter.clone().unwrap_or_default();
        request.include_total_count = false;
        let mut response = self.fetch_transaction_history(request.clone()).await?;
        let read_fence =
            response
                .read_fence
                .clone()
                .ok_or(CipherscanRestError::MissingUpstreamField(
                    "transaction_history.read_fence",
                ))?;
        let cache_key = TransactionHistoryCountCacheKey::new(&filter, &read_fence);
        let cached_total = {
            let cache = self.transaction_history_count_cache.read().await;
            cache
                .get(&cache_key)
                .filter(|cached| cached.expires_at > Instant::now())
                .map(|cached| cached.total)
        };
        if let Some(total) = cached_total {
            response.total_matching_transactions = Some(total);
            response.count_scope = TransactionHistoryCountScope::FullHistory as i32;
            return Ok(response);
        }

        request.include_total_count = true;
        request.read_fence = Some(read_fence);
        let response = self.fetch_transaction_history(request).await?;
        let total = require_full_transaction_history_count(&response)?;
        self.transaction_history_count_cache.write().await.insert(
            cache_key,
            CachedTransactionHistoryCount {
                total,
                expires_at: Instant::now() + TRANSACTION_HISTORY_COUNT_CACHE_TTL,
            },
        );
        Ok(response)
    }

    async fn fetch_transaction_history_offset(
        &self,
        filter: TransactionHistoryFilter,
        offset: u32,
        limit: u32,
        include_total_count: bool,
    ) -> Result<TransactionHistoryWindow, CipherscanRestError> {
        for attempt in 0..2 {
            match self
                .fetch_transaction_history_offset_once(
                    filter.clone(),
                    offset,
                    limit,
                    include_total_count,
                )
                .await
            {
                Err(CipherscanRestError::Upstream(status))
                    if attempt == 0 && status.code() == Code::FailedPrecondition => {}
                outcome => return outcome,
            }
        }
        Err(CipherscanRestError::InvalidUpstreamField(
            "transaction_history.read_fence",
        ))
    }

    async fn fetch_transaction_history_offset_once(
        &self,
        filter: TransactionHistoryFilter,
        offset: u32,
        limit: u32,
        include_total_count: bool,
    ) -> Result<TransactionHistoryWindow, CipherscanRestError> {
        if offset > MAX_TRANSACTION_HISTORY_OFFSET {
            return Err(CipherscanRestError::InvalidRequest(format!(
                "offset must not exceed {MAX_TRANSACTION_HISTORY_OFFSET}"
            )));
        }

        let mut remaining_offset = offset;
        let mut entries =
            Vec::with_capacity(usize::try_from(limit.saturating_add(1)).unwrap_or(usize::MAX));
        let mut cursor = None;
        let mut read_fence = None;
        let mut has_older = true;
        let mut total_matching_transactions = None;

        while has_older && u32::try_from(entries.len()).unwrap_or(u32::MAX) <= limit {
            let remaining_entries = limit
                .saturating_add(1)
                .saturating_sub(u32::try_from(entries.len()).unwrap_or(u32::MAX));
            let page_size = remaining_offset
                .saturating_add(remaining_entries)
                .clamp(1, TRANSACTION_HISTORY_PAGE_SIZE);
            let request = TransactionHistoryRequest {
                page_size,
                start: cursor
                    .clone()
                    .map(transaction_history_request::Start::Cursor),
                direction: TransactionHistoryDirection::Older as i32,
                filter: Some(filter.clone()),
                include_total_count: include_total_count && total_matching_transactions.is_none(),
                read_fence: read_fence.clone(),
            };
            let response = self.fetch_transaction_history(request).await?;
            advance_transaction_history_read_fence(&mut read_fence, &response)?;
            if response.total_matching_transactions.is_some() {
                total_matching_transactions =
                    Some(require_full_transaction_history_count(&response)?);
            }
            let skipped =
                remaining_offset.min(u32::try_from(response.entries.len()).unwrap_or(u32::MAX));
            remaining_offset = remaining_offset.saturating_sub(skipped);
            entries.extend(
                response
                    .entries
                    .into_iter()
                    .skip(usize::try_from(skipped).unwrap_or(usize::MAX)),
            );
            has_older = response.has_older;
            if !has_older {
                break;
            }
            if response.older_cursor.is_empty() || cursor.as_ref() == Some(&response.older_cursor) {
                return Err(CipherscanRestError::InvalidUpstreamField(
                    "transaction_history.older_cursor",
                ));
            }
            cursor = Some(response.older_cursor);
        }

        let has_more = u32::try_from(entries.len()).unwrap_or(u32::MAX) > limit || has_older;
        entries.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
        Ok(TransactionHistoryWindow {
            entries,
            has_more,
            total_matching_transactions,
        })
    }

    async fn fetch_tip_height(&self) -> Result<u32, CipherscanRestError> {
        Ok(self.fetch_visible_tip_block().await?.height)
    }

    async fn fetch_visible_tip_block(&self) -> Result<wallet::BlockId, CipherscanRestError> {
        self.fetch_visible_tip_block_context()
            .await
            .map(|(tip, _at_epoch_id)| tip)
    }

    async fn fetch_visible_tip_block_context(
        &self,
    ) -> Result<(wallet::BlockId, Option<u64>), CipherscanRestError> {
        let visible_tip_block_response = self
            .wallet_client()
            .visible_tip_block(VisibleTipBlockRequest { at_epoch_id: None })
            .await?
            .into_inner();
        let at_epoch_id = visible_tip_block_response
            .chain_view
            .as_ref()
            .and_then(|chain_view| chain_view.chain_epoch.as_ref())
            .map(|epoch| epoch.chain_epoch_id);
        let tip = visible_tip_block_response.visible_tip_block.ok_or(
            CipherscanRestError::MissingUpstreamField("visible_tip_block"),
        )?;
        Ok((tip, at_epoch_id))
    }

    async fn fetch_visible_tip_commitment_tree_sizes(
        &self,
    ) -> Result<VisibleTipCommitmentTreeSizes, CipherscanRestError> {
        let visible_tip_block_response = self
            .wallet_client()
            .visible_tip_block(VisibleTipBlockRequest { at_epoch_id: None })
            .await?
            .into_inner();
        let tip = visible_tip_block_response.visible_tip_block.ok_or(
            CipherscanRestError::MissingUpstreamField("visible_tip_block"),
        )?;
        let chain_epoch = visible_tip_block_response
            .chain_view
            .and_then(|chain_view| chain_view.chain_epoch)
            .ok_or(CipherscanRestError::MissingUpstreamField("chain_epoch"))?;

        Ok(VisibleTipCommitmentTreeSizes {
            chain_epoch_id: chain_epoch.chain_epoch_id,
            block_height: tip.height,
            block_hash: tip.block_hash,
            sapling_commitment_tree_size: chain_epoch.sapling_commitment_tree_size,
            orchard_commitment_tree_size: chain_epoch.orchard_commitment_tree_size,
            ironwood_commitment_tree_size: chain_epoch.ironwood_commitment_tree_size,
        })
    }

    async fn fetch_chain_reorg_history(
        &self,
    ) -> Result<ChainReorgHistorySnapshot, CipherscanRestError> {
        let mut explorer_client = self.explorer_client();
        let mut from_cursor = Vec::new();
        let mut events = Vec::new();

        for _ in 0..MAX_REORG_HISTORY_PAGES {
            let response = match explorer_client
                .chain_reorg_history(ChainReorgHistoryRequest {
                    max_events: REORG_HISTORY_PAGE_SIZE,
                    from_cursor,
                })
                .await
            {
                Ok(response) => response.into_inner(),
                Err(status)
                    if events.is_empty()
                        && matches!(
                            status.code(),
                            Code::FailedPrecondition | Code::Unimplemented
                        ) =>
                {
                    return Ok(ChainReorgHistorySnapshot {
                        events: Vec::new(),
                        is_truncated: false,
                        is_projection_unavailable: true,
                    });
                }
                Err(status) => return Err(status.into()),
            };

            from_cursor = response.next_cursor;
            events.extend(response.events);
            if from_cursor.is_empty() {
                return Ok(ChainReorgHistorySnapshot {
                    events,
                    is_truncated: false,
                    is_projection_unavailable: false,
                });
            }
        }

        Ok(ChainReorgHistorySnapshot {
            events,
            is_truncated: true,
            is_projection_unavailable: false,
        })
    }
}

fn explorer_chain_epoch_id(freshness: Option<&explorer::ExplorerFreshness>) -> Option<u64> {
    freshness
        .and_then(|freshness| freshness.chain_view.as_ref())
        .and_then(|chain_view| chain_view.chain_epoch.as_ref())
        .map(|chain_epoch| chain_epoch.chain_epoch_id)
}

fn explorer_visible_tip(
    freshness: Option<&explorer::ExplorerFreshness>,
) -> Option<&wallet::BlockTip> {
    freshness
        .and_then(|freshness| freshness.chain_view.as_ref())
        .and_then(|chain_view| chain_view.chain_epoch.as_ref())
        .and_then(|chain_epoch| chain_epoch.visible_tip.as_ref())
}

fn explorer_settled_tip(
    freshness: Option<&explorer::ExplorerFreshness>,
) -> Option<&wallet::BlockTip> {
    freshness
        .and_then(|freshness| freshness.chain_view.as_ref())
        .and_then(|chain_view| chain_view.chain_epoch.as_ref())
        .and_then(|chain_epoch| chain_epoch.settled_tip.as_ref())
}

fn with_chain_routes(router: Router<CipherscanRestAdapter>) -> Router<CipherscanRestAdapter> {
    router
        .route("/api/info", get(chain_info))
        .route("/api/blockchain-info", get(blockchain_info))
        .route("/api/blocks", get(blocks))
        .route("/api/blocks/list", get(blocks_list))
        .route("/api/block/{block_id}", get(block_detail))
        .route("/api/search/anchor/{root}", get(anchor_root_search))
        .route("/api/tx/shielded", get(shielded_transactions))
        .route("/api/tx/broadcast", post(broadcast_transaction))
        .route("/api/scan/orchard", post(scan_orchard))
        .route("/api/tx/raw/batch", post(raw_transactions_batch))
        .route(
            "/api/tx/{transaction_id}/linkability",
            get(transaction_linkability),
        )
        .route("/api/tx/{transaction_id}/verbose", get(verbose_transaction))
        .route("/api/tx/{transaction_id}/raw", get(raw_transaction))
        .route("/api/tx/{transaction_id}", get(transaction_detail))
        .route("/api/transactions/list", get(transactions_list))
        .route("/api/shielded/list", get(shielded_flows))
}

fn with_account_and_market_routes(
    router: Router<CipherscanRestAdapter>,
) -> Router<CipherscanRestAdapter> {
    router
        .route("/api/address/{address}", get(address_detail))
        .route("/api/rich-list", get(rich_list))
        .route("/api/labels", get(labels))
        .route("/api/label/{address}", get(label_lookup))
        .route("/api/price/at", get(price_at))
        .route("/api/price", get(price))
        .route("/api/mempool/tx/{transaction_id}", get(mempool_transaction))
        .route("/api/mempool", get(mempool))
}

fn with_crosschain_routes(router: Router<CipherscanRestAdapter>) -> Router<CipherscanRestAdapter> {
    router
        .route("/api/crosschain/stats", get(crosschain_stats))
        .route("/api/crosschain/inflows", get(crosschain_inflows))
        .route("/api/crosschain/outflows", get(crosschain_outflows))
        .route("/api/crosschain/status", get(crosschain_status))
        .route("/api/crosschain/db-stats", get(crosschain_db_stats))
        .route("/api/crosschain/trends", get(crosschain_trends))
        .route("/api/crosschain/history", get(crosschain_history))
        .route(
            "/api/crosschain/volume-by-chain",
            get(crosschain_volume_by_chain),
        )
        .route("/api/crosschain/address/{address}", get(crosschain_address))
        .route(
            "/api/crosschain/popular-pairs",
            get(crosschain_popular_pairs),
        )
}

fn with_name_routes(router: Router<CipherscanRestAdapter>) -> Router<CipherscanRestAdapter> {
    router
        .route("/api/name/{name}/events", get(name_events))
        .route("/api/name/{name}", get(name_lookup))
}

fn with_crosslink_and_reorg_routes(
    router: Router<CipherscanRestAdapter>,
) -> Router<CipherscanRestAdapter> {
    router
        .route("/api/crosslink", get(crosslink_stats))
        .route("/api/crosslink/participation", get(crosslink_participation))
        .route("/api/crosslink/bft-chain", get(crosslink_bft_chain))
        .route("/api/crosslink/bft-tip", get(crosslink_bft_tip))
        .route(
            "/api/crosslink/bootstrap-info",
            get(crosslink_bootstrap_info),
        )
        .route(
            "/api/crosslink/divergence-history",
            get(crosslink_divergence_history),
        )
        .route("/api/finalizers", get(finalizers))
        .route(
            "/api/finalizer/{pubkey}/participation",
            get(finalizer_participation),
        )
        .route("/api/finalizer/{pubkey}", get(finalizer_detail))
        .route("/api/crosslink/fork-monitor", get(fork_monitor))
        .route("/api/uncles/stats", get(reorg_stats))
        .route("/api/uncles/forks", get(reorg_forks))
        .route("/api/uncles/nodes", get(reorg_nodes))
        .route("/api/uncles", get(non_canonical_blocks))
        .route("/api/uncle/report", post(non_canonical_block_report))
        .route("/api/uncle/{block_hash}", get(non_canonical_block_detail))
        .route(
            "/api/crosslink/fork-monitor/check",
            post(fork_monitor_check),
        )
        .route(
            "/api/crosslink/fork-monitor/report",
            post(fork_monitor_report),
        )
        .route(
            "/api/crosslink/fork-monitor/report/{node_name}",
            delete(fork_monitor_delete_report),
        )
        .route(
            "/api/crosslink/block-hash/{height}",
            get(crosslink_block_hash),
        )
}

fn with_privacy_routes(router: Router<CipherscanRestAdapter>) -> Router<CipherscanRestAdapter> {
    router
        .route("/api/privacy-stats", get(privacy_stats))
        .route("/api/privacy/risks", get(privacy_risks))
        .route("/api/privacy/linkage-edges", get(privacy_linkage_edges))
        .route("/api/privacy/batch-risks", get(privacy_batch_risks))
        .route("/api/privacy/clusters", get(privacy_clusters))
        .route("/api/privacy/graph/{transaction_id}", get(privacy_graph))
        .route(
            "/api/privacy/shield/{transaction_id}/batch",
            get(privacy_shield_batch),
        )
        .route("/api/privacy/patterns", get(privacy_patterns))
        .route("/api/privacy/common-amounts", get(privacy_common_amounts))
        .route(
            "/api/privacy/recommended-swap-amounts",
            get(privacy_recommended_swap_amounts),
        )
        .route("/api/blend-check/split", get(blend_check_split))
        .route("/api/blend-check", get(blend_check))
        .route("/api/stats/shielded-count", get(shielded_count))
        .route("/api/stats/shielded-daily", get(shielded_daily))
        .route("/api/analytics/anonymity-set", get(anonymity_set))
        .route(
            "/api/analytics/shielding-distribution",
            get(shielding_distribution),
        )
}

#[derive(Debug)]
struct ChainReorgHistorySnapshot {
    events: Vec<explorer::ChainReorgHistoryEvent>,
    is_truncated: bool,
    is_projection_unavailable: bool,
}

/// Errors raised by the Cipherscan REST adapter.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CipherscanRestError {
    /// Upstream Zinder gRPC call failed.
    #[error("Zinder upstream call failed: {0}")]
    Upstream(#[from] tonic::Status),
    /// A Zinder response omitted a field required to construct this REST shape.
    #[error("Zinder upstream response omitted required field: {0}")]
    MissingUpstreamField(&'static str),
    /// A Zinder response carried an invalid value for a required field.
    #[error("Zinder upstream response carried invalid field: {0}")]
    InvalidUpstreamField(&'static str),
    /// The Cipherscan request could not be translated into a valid Zinder request.
    #[error("invalid Cipherscan-compatible request: {0}")]
    InvalidRequest(String),
    /// A bounded client-side candidate scan cannot prove a complete decryptable result.
    #[error("Orchard candidate scan unavailable: {0}")]
    CandidateScanUnavailable(&'static str),
    /// The broadcast body did not contain valid transaction hex.
    #[error("invalid raw transaction hex: {0}")]
    InvalidRawTransactionHex(#[from] hex::FromHexError),
    /// Chain economics could not be derived from consensus parameters.
    #[error("chain economics unavailable: {0}")]
    ChainEconomicsUnavailable(String),
}

impl IntoResponse for CipherscanRestError {
    fn into_response(self) -> Response {
        let (status, code) = match &self {
            Self::Upstream(upstream) => match upstream.code() {
                Code::InvalidArgument => (StatusCode::BAD_REQUEST, "invalid_argument"),
                Code::NotFound => (StatusCode::NOT_FOUND, "not_found"),
                Code::Unavailable | Code::DeadlineExceeded => {
                    (StatusCode::SERVICE_UNAVAILABLE, "upstream_unavailable")
                }
                Code::Ok
                | Code::Cancelled
                | Code::Unknown
                | Code::AlreadyExists
                | Code::PermissionDenied
                | Code::ResourceExhausted
                | Code::FailedPrecondition
                | Code::Aborted
                | Code::OutOfRange
                | Code::Unimplemented
                | Code::Internal
                | Code::DataLoss
                | Code::Unauthenticated => (StatusCode::BAD_GATEWAY, "upstream_error"),
            },
            Self::MissingUpstreamField(_) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "upstream_field_unavailable",
            ),
            Self::InvalidUpstreamField(_) => (StatusCode::BAD_GATEWAY, "upstream_field_invalid"),
            Self::InvalidRequest(_) | Self::InvalidRawTransactionHex(_) => {
                (StatusCode::BAD_REQUEST, "invalid_request")
            }
            Self::CandidateScanUnavailable(_) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "candidate_scan_unavailable",
            ),
            Self::ChainEconomicsUnavailable(_) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "chain_economics_unavailable",
            ),
        };
        json_response(
            status,
            json!({
                "success": false,
                "source": CIPHERSCAN_ADAPTER_SOURCE,
                "code": code,
                "error": self.to_string(),
            }),
        )
    }
}

fn with_chain_analytics_routes(
    router: Router<CipherscanRestAdapter>,
) -> Router<CipherscanRestAdapter> {
    router
        .route("/api/network/stats", get(network_stats))
        .route("/api/network/health", get(network_health))
        .route("/api/network/blocks/recent", get(network_recent_blocks))
        .route("/api/network/halving", get(chain_halving))
        .route("/api/network/emission", get(chain_emission))
        .route("/api/network/mining-metrics", get(mining_metrics))
        .route(
            "/api/mining/pool-distribution",
            get(mining_pool_distribution),
        )
        .route("/api/mining/pool-ranking", get(mining_pool_ranking))
        .route("/api/mining/hashrate-share", get(mining_hashrate_share))
        .route("/api/mining/miner-behavior", get(miner_behavior))
        .route("/api/mining/zodl-leaderboard", get(zodl_leaderboard))
        .route("/api/mining/rewards", get(mining_rewards))
        .route("/api/pools/overview", get(pool_overview))
        .route("/api/pools/flows", get(pool_flows))
        .route("/api/pools/turnstile", get(pool_turnstile))
        .route("/api/migration/overview", get(migration_overview))
        .route("/api/migration/cohorts", get(migration_cohorts))
        .route("/api/migration/denominations", get(migration_denominations))
        .route("/api/network/pool-history", get(value_pool_history))
        .route("/api/network/chain-size-history", get(chain_size_history))
        .route("/api/network/fees", get(network_fees))
        .route("/api/network/fee-distribution", get(fee_distribution))
        .route("/api/network/protocol-stats", get(protocol_stats))
        .route("/api/analytics/usage-clock", get(usage_clock))
        .route("/api/network/peers", get(peers))
        .route("/api/network/nodes", get(nodes))
        .route("/api/network/nodes/stats", get(node_stats))
        .route("/api/network/node-history", get(node_history))
        .route("/api/supply", get(supply))
        .route("/api/circulating-supply", get(circulating_supply))
        .route(
            "/api/supply/transparent-breakdown",
            get(transparent_supply_breakdown),
        )
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct PageQuery {
    limit: Option<u32>,
    offset: Option<u32>,
    page: Option<u32>,
    cursor: Option<u32>,
    cursor_idx: Option<u32>,
    cursor_id: Option<u32>,
    direction: Option<String>,
    period: Option<String>,
    granularity: Option<String>,
    format: Option<String>,
    flow_type: Option<String>,
    pool: Option<String>,
    min_zec: Option<f64>,
    min_actions: Option<u32>,
    window: Option<u32>,
    skip_count: Option<String>,
    #[serde(rename = "type")]
    transaction_type: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct ShieldedTransactionQuery {
    limit: Option<String>,
    offset: Option<String>,
    pool: Option<String>,
    min_actions: Option<String>,
    skip_count: Option<String>,
    #[serde(rename = "type")]
    transaction_type: Option<String>,
}

/// Raw pagination parameters for the legacy reorg routes.
///
/// Express applies `parseInt` to query strings on these paths, so parsing
/// directly as unsigned integers would reject inputs that Cipherscan accepts.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct ReorgPageQuery {
    limit: Option<String>,
    offset: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct ShieldedFlowQuery {
    limit: Option<u32>,
    cursor: Option<i64>,
    cursor_id: Option<u64>,
    direction: Option<String>,
    flow_type: Option<String>,
    pool: Option<String>,
    min_zec: Option<f64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct TransactionLinkabilityQuery {
    limit: Option<String>,
    tolerance: Option<String>,
}

impl CipherscanPoolFlowRequest {
    fn from_query(query: &PageQuery) -> Result<Self, CipherscanRestError> {
        let (period, days) = match query.period.as_deref() {
            Some("7d") => ("7d", 7),
            Some("90d") => ("90d", 90),
            Some("1y") => ("1y", 365),
            None | Some(_) => ("30d", 30),
        };
        let (pool, pools) = match query.pool.as_deref() {
            None | Some("all") => ("all", Vec::new()),
            Some("sprout") => ("sprout", vec![ValuePoolFlowPool::Sprout as i32]),
            Some("sapling") => ("sapling", vec![ValuePoolFlowPool::Sapling as i32]),
            Some("orchard") => ("orchard", vec![ValuePoolFlowPool::Orchard as i32]),
            Some("ironwood") => ("ironwood", vec![ValuePoolFlowPool::Ironwood as i32]),
            Some("mixed") => ("mixed", vec![ValuePoolFlowPool::Mixed as i32]),
            Some(pool) => {
                return Err(CipherscanRestError::InvalidRequest(format!(
                    "unsupported value-pool flow pool: {pool}"
                )));
            }
        };
        let resolution = if query.granularity.as_deref() == Some("hourly") {
            CipherscanFlowResolution::Hourly
        } else {
            CipherscanFlowResolution::Daily
        };
        let amount_format = if query.format.as_deref() == Some("zatoshi") {
            CipherscanFlowAmountFormat::Zatoshi
        } else {
            CipherscanFlowAmountFormat::Zec
        };
        Ok(Self {
            period,
            days,
            pool,
            pools,
            resolution,
            amount_format,
        })
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct MiningMetricsQuery {
    window: Option<String>,
    limit: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RichListQuery {
    limit: Option<String>,
    offset: Option<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct ScanRange {
    start_height: u64,
    end_height: u64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ScanHeightField {
    Missing,
    Invalid,
    Present(u64),
}

#[derive(Debug, Deserialize)]
struct ForkMonitorCheckBody {
    heights: Vec<Value>,
}

#[derive(Debug, Default, Deserialize)]
struct CirculatingSupplyQuery {
    format: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct HistoricalPriceQuery {
    date: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ShieldedCountQuery {
    since: Option<String>,
    detailed: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ShieldedDailyQuery {
    since: Option<String>,
    until: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct UsageClockQuery {
    period: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct BlendAmountQuery {
    amount: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct PrivacyListQuery {
    limit: Option<u32>,
    offset: Option<u32>,
    period: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct PrivacyCommonAmountsQuery {
    limit: Option<String>,
    period: Option<String>,
    chain: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct RecommendedSwapAmountsQuery {
    chain: Option<String>,
    token: Option<String>,
}

async fn chain_info(
    State(adapter): State<CipherscanRestAdapter>,
) -> Result<Response, CipherscanRestError> {
    let height = adapter.fetch_tip_height().await?;
    Ok(json_response(StatusCode::OK, chain_info_json(height)))
}

async fn blockchain_info(
    State(adapter): State<CipherscanRestAdapter>,
) -> Result<Response, CipherscanRestError> {
    let tip = adapter.fetch_visible_tip_block().await?;
    Ok(json_response(
        StatusCode::OK,
        blockchain_info_json(adapter.network, &tip),
    ))
}

async fn blocks(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<PageQuery>,
) -> Result<Response, CipherscanRestError> {
    let limit = query_limit(query.limit, DEFAULT_LIMIT);
    let offset = query.offset.unwrap_or(0);
    let (entries, total) = adapter.fetch_recent_blocks(limit, offset).await?;
    let rows: Vec<Value> = entries.iter().map(block_list_row).collect();
    Ok(json_response(
        StatusCode::OK,
        json!({
            "blocks": rows,
            "pagination": offset_pagination(limit, offset, total),
        }),
    ))
}

async fn blocks_list(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<PageQuery>,
) -> Result<Response, CipherscanRestError> {
    let limit = query_limit(query.limit, DEFAULT_LIMIT);
    let page = query.page.unwrap_or(1).max(1);
    let (entries, total) = adapter
        .fetch_block_list_page(limit, page, query.cursor, query.direction.as_deref())
        .await?;
    let rows: Vec<Value> = entries.iter().map(block_list_row).collect();
    Ok(json_response(
        StatusCode::OK,
        json!({
            "success": true,
            "blocks": rows,
            "pagination": block_list_pagination(page, limit, total, &entries),
        }),
    ))
}

async fn network_recent_blocks(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<PageQuery>,
) -> Result<Response, CipherscanRestError> {
    let limit = query_limit(query.limit, DEFAULT_LIMIT);
    let (entries, _) = adapter.fetch_recent_blocks(limit, 0).await?;
    let rows: Vec<Value> = entries
        .iter()
        .map(|entry| {
            let summary = &entry.summary;
            let fee_zat = summary
                .paid_fees_collected_zat
                .unwrap_or(summary.fees_collected_zat);
            json!({
                "height": summary.block_height,
                "hash": summary.block_hash,
                "timestamp": summary.block_time_unix_seconds,
                "txCount": summary.transaction_count,
                "transaction_count": summary.transaction_count,
                "size": summary.total_size_bytes,
                "minerAddress": entry.miner_address,
                "minerReward": zec_from_unsigned_zatoshis(summary.coinbase_reward_zat),
                "fees": zec_from_unsigned_zatoshis(fee_zat),
            })
        })
        .collect();
    Ok(json_response(
        StatusCode::OK,
        json!({
            "success": true,
            "blocks": rows,
        }),
    ))
}

async fn block_detail(
    State(adapter): State<CipherscanRestAdapter>,
    Path(block_id): Path<String>,
) -> Result<Response, CipherscanRestError> {
    let hash_selector = is_rpc_hash(&block_id);
    let selector = block_selector(block_id.clone())?;
    let response = match adapter
        .explorer_client()
        .block_transactions(BlockDetailRequest {
            selector: Some(selector),
            at_epoch_id: None,
        })
        .await
    {
        Ok(response) => response.into_inner(),
        Err(status) if hash_selector && status.code() == tonic::Code::NotFound => {
            adapter
                .require_explorer_capability(EXPLORER_CHAIN_DISPLACED_BLOCK_DETAIL_V1)
                .await?;
            let detail = adapter
                .explorer_client()
                .displaced_block_detail(DisplacedBlockDetailRequest {
                    block_hash: block_id,
                })
                .await?
                .into_inner();
            return Ok(json_response(
                StatusCode::OK,
                displaced_block_page_json(adapter.network, &detail)?,
            ));
        }
        Err(status) => return Err(status.into()),
    };
    let body = cipherscan_block_detail_json(&adapter, response).await?;
    Ok(json_response(StatusCode::OK, body))
}

fn displaced_block_page_json(
    network: Network,
    detail: &explorer::DisplacedBlockDetailResponse,
) -> Result<Value, CipherscanRestError> {
    let block = detail
        .block
        .as_ref()
        .ok_or(CipherscanRestError::MissingUpstreamField(
            "displaced_block_detail.block",
        ))?;
    let miner_address = block
        .coinbase_outputs
        .iter()
        .find_map(|output| cipherscan_transparent_address(network, &output.script_pub_key));
    let canonical_block = detail.current_canonical_block.as_ref().map(|canonical| {
        let canonical_miner_address = canonical
            .coinbase_outputs
            .iter()
            .find_map(|output| cipherscan_transparent_address(network, &output.script_pub_key));
        json!({
            "height": canonical.block_height,
            "hash": canonical.block_hash,
            "timestamp": canonical.block_time_unix_seconds,
            "transaction_count": canonical.transaction_count,
            "size": canonical.total_size_bytes,
            "miner_address": canonical_miner_address,
            "miner_pool": Value::Null,
            "miner_pool_url": Value::Null,
            "miner_pool_region": Value::Null,
        })
    });
    Ok(json!({
        "height": block.block_height,
        "hash": block.block_hash,
        "timestamp": block.block_time_unix_seconds,
        "transaction_count": block.transaction_ids.len(),
        "transactionCount": block.transaction_ids.len(),
        "size": block.total_size_bytes,
        "difficulty": cipherscan_difficulty(network, block.difficulty_bits)?.to_string(),
        "previous_block_hash": block.previous_block_hash,
        "miner_address": miner_address,
        "isOrphaned": true,
        "orphanSource": CIPHERSCAN_ADAPTER_SOURCE,
        "orphanDetectedAt": rfc3339_millis(block.displaced_at_millis),
        "canonicalBlock": canonical_block,
        "transactions": [],
        "confirmations": 0,
        "miner_pool": Value::Null,
        "miner_pool_url": Value::Null,
        "miner_pool_region": Value::Null,
        "zinderUnavailable": [
            "Displaced-block transaction facts are archived as ordered transaction ids; the unchanged Cipherscan orphan page does not render transaction rows."
        ],
    }))
}

async fn cipherscan_block_detail_json(
    adapter: &CipherscanRestAdapter,
    response: explorer::BlockTransactionsResponse,
) -> Result<Value, CipherscanRestError> {
    let chain_epoch_id = explorer_chain_epoch_id(response.freshness.as_ref()).ok_or(
        CipherscanRestError::MissingUpstreamField("block_transactions.freshness.chain_epoch"),
    )?;
    let final_note_commitment_roots = response
        .final_note_commitment_roots
        .as_ref()
        .map(CipherscanFinalNoteCommitmentRoots::try_from)
        .transpose()?;
    let summary = response
        .summary
        .as_ref()
        .ok_or(CipherscanRestError::MissingUpstreamField("summary"))?;
    let header = adapter
        .wallet_client()
        .block_header_by_selector(BlockSelectorRequest {
            selector: Some(wallet::BlockSelector {
                selector: Some(wallet::block_selector::Selector::Height(
                    summary.block_height,
                )),
            }),
            at_epoch_id: Some(chain_epoch_id),
        })
        .await?
        .into_inner()
        .block_header
        .ok_or(CipherscanRestError::MissingUpstreamField("block_header"))?;
    let header_fields = cipherscan_block_header_fields(adapter.network, &header)?;
    let production_entry = adapter
        .fetch_canonical_block_production_entry(
            summary.block_height,
            &summary.block_hash,
            Some(chain_epoch_id),
        )
        .await?;
    let coinbase_miner_fields = match response.transactions.iter().find(|transaction| {
        transaction
            .public_facts
            .as_ref()
            .is_some_and(|facts| facts.is_coinbase)
    }) {
        Some(transaction) => {
            adapter
                .fetch_coinbase_miner_fields(&transaction.transaction_id, chain_epoch_id)
                .await?
        }
        None => None,
    };
    let mut transaction_rows = cipherscan_block_transaction_rows(
        adapter.network,
        &response.transactions,
        summary.block_height,
        summary.block_time_unix_seconds,
    );
    if production_entry.miner_address.is_none() {
        transaction_rows.unavailable.push(
            "Miner payout address is unavailable because canonical coinbase output 0 is absent or does not use a standard transparent script.",
        );
    }
    Ok(cipherscan_block_detail_response_json(
        CipherscanBlockDetailResponseInput {
            summary,
            header: &header,
            header_fields: &header_fields,
            transaction_rows: &transaction_rows,
            miner_address: production_entry.miner_address.as_deref(),
            final_note_commitment_roots: final_note_commitment_roots.as_ref(),
            coinbase_miner_fields: coinbase_miner_fields.as_ref(),
        },
    ))
}

#[derive(Debug, Eq, PartialEq)]
struct CipherscanCoinbaseMinerFields {
    miner_data_hex: String,
    miner_data_text: String,
}

impl CipherscanRestAdapter {
    async fn fetch_coinbase_miner_fields(
        &self,
        transaction_id: &str,
        at_epoch_id: u64,
    ) -> Result<Option<CipherscanCoinbaseMinerFields>, CipherscanRestError> {
        let response = self
            .wallet_client()
            .transaction(TransactionRequest {
                transaction_id: transaction_id.to_owned(),
                at_epoch_id: Some(at_epoch_id),
            })
            .await?
            .into_inner();
        let Some(raw_transaction_bytes) = raw_transaction_bytes(response.location.as_ref()) else {
            return Ok(None);
        };

        decode_cipherscan_coinbase_miner_fields(raw_transaction_bytes).map(Some)
    }
}

fn decode_cipherscan_coinbase_miner_fields(
    raw_transaction_bytes: &[u8],
) -> Result<CipherscanCoinbaseMinerFields, CipherscanRestError> {
    let transaction = raw_transaction_bytes
        .zcash_deserialize_into::<ZebraTransaction>()
        .map_err(|_| {
            CipherscanRestError::InvalidUpstreamField("location.mined.raw_transaction_bytes")
        })?;
    let inputs = transaction.inputs();
    let miner_data = inputs
        .iter()
        .find_map(transparent::Input::miner_data)
        .ok_or(CipherscanRestError::InvalidUpstreamField(
            "coinbase_transaction.transparent_input",
        ))?;
    let miner_data_text = miner_data
        .iter()
        .map(|byte| {
            if (0x20..=0x7e).contains(byte) {
                char::from(*byte)
            } else {
                '.'
            }
        })
        .collect();

    Ok(CipherscanCoinbaseMinerFields {
        miner_data_hex: hex::encode(miner_data),
        miner_data_text,
    })
}

struct CipherscanFinalNoteCommitmentRoots {
    sapling: Option<String>,
    orchard: Option<String>,
    ironwood: Option<String>,
}

impl TryFrom<&explorer::BlockFinalNoteCommitmentRoots> for CipherscanFinalNoteCommitmentRoots {
    type Error = CipherscanRestError;

    fn try_from(roots: &explorer::BlockFinalNoteCommitmentRoots) -> Result<Self, Self::Error> {
        Ok(Self {
            sapling: cipherscan_commitment_root_hex(
                roots.sapling.as_deref(),
                "final_note_commitment_roots.sapling",
            )?,
            orchard: cipherscan_commitment_root_hex(
                roots.orchard.as_deref(),
                "final_note_commitment_roots.orchard",
            )?,
            ironwood: cipherscan_commitment_root_hex(
                roots.ironwood.as_deref(),
                "final_note_commitment_roots.ironwood",
            )?,
        })
    }
}

fn cipherscan_commitment_root_hex(
    root: Option<&[u8]>,
    field: &'static str,
) -> Result<Option<String>, CipherscanRestError> {
    root.map(|bytes| {
        if bytes.len() != 32 {
            return Err(CipherscanRestError::InvalidUpstreamField(field));
        }
        Ok(hex::encode(bytes))
    })
    .transpose()
}

struct CipherscanBlockTransactionRows {
    rows: Vec<Value>,
    unavailable: Vec<&'static str>,
}

fn cipherscan_block_transaction_rows(
    network: Network,
    transactions: &[explorer::BlockTransaction],
    block_height: u32,
    block_time_unix_seconds: i64,
) -> CipherscanBlockTransactionRows {
    let input_values_are_fee_safe = transactions.iter().all(|transaction| {
        transaction.public_facts.as_ref().is_some_and(|facts| {
            facts.is_coinbase
                || facts.privacy_shape == explorer::PrivacyShape::TransparentOnly as i32
        })
    });
    let rows = transactions
        .iter()
        .map(|transaction| {
            cipherscan_block_transaction_json(
                network,
                transaction,
                block_height,
                block_time_unix_seconds,
                input_values_are_fee_safe,
            )
        })
        .collect::<Option<Vec<_>>>();
    let unavailable = match rows {
        Some(rows) => {
            let mut unavailable = vec!["Block solution is unavailable."];
            if !input_values_are_fee_safe
                && transactions
                    .iter()
                    .any(|transaction| !transaction.transparent_inputs.is_empty())
            {
                unavailable.push(
                    "Transparent input values are withheld when any block transaction has shielded components because unchanged Cipherscan would otherwise calculate a false partial block fee.",
                );
            }
            return CipherscanBlockTransactionRows { rows, unavailable };
        }
        None => vec![
            "Block solution is unavailable.",
            "At least one canonical transaction lacks public facts, so the Cipherscan table is withheld instead of inferring a false transaction type.",
        ],
    };
    CipherscanBlockTransactionRows {
        rows: Vec::new(),
        unavailable,
    }
}

#[derive(Clone, Copy)]
struct CipherscanBlockDetailResponseInput<'a> {
    summary: &'a explorer::BlockSummary,
    header: &'a wallet::BlockHeader,
    header_fields: &'a CipherscanBlockHeaderFields,
    transaction_rows: &'a CipherscanBlockTransactionRows,
    miner_address: Option<&'a str>,
    final_note_commitment_roots: Option<&'a CipherscanFinalNoteCommitmentRoots>,
    coinbase_miner_fields: Option<&'a CipherscanCoinbaseMinerFields>,
}

fn cipherscan_block_detail_response_json(input: CipherscanBlockDetailResponseInput<'_>) -> Value {
    let CipherscanBlockDetailResponseInput {
        summary,
        header,
        header_fields,
        transaction_rows,
        miner_address,
        final_note_commitment_roots,
        coinbase_miner_fields,
    } = input;
    json!({
        "height": summary.block_height.to_string(),
        "hash": summary.block_hash,
        "timestamp": summary.block_time_unix_seconds.to_string(),
        "transaction_count": summary.transaction_count,
        "transactionCount": summary.transaction_count,
        "size": summary.total_size_bytes,
        "difficulty": cipherscan_difficulty_string(header_fields.difficulty),
        "confirmations": summary.confirmations,
        "previous_block_hash": optional_string(&header.previous_block_hash),
        "next_block_hash": Value::Null,
        "version": header.version,
        "merkle_root": header.merkle_root_hash,
        "bits": header_fields.bits,
        "nonce": header_fields.nonce,
        "total_fees": summary.paid_fees_collected_zat.unwrap_or(summary.fees_collected_zat).to_string(),
        "coinbase_reward": summary.coinbase_reward_zat.to_string(),
        "miner_address": miner_address,
        "miner_pool": Value::Null,
        "miner_pool_url": Value::Null,
        "miner_pool_region": Value::Null,
        "sapling_output_count": summary.sapling_output_count,
        "orchard_action_count": summary.orchard_action_count,
        "ironwood_action_count": summary.ironwood_action_count,
        "final_sapling_root": final_note_commitment_roots
            .and_then(|roots| roots.sapling.as_deref()),
        "final_orchard_root": final_note_commitment_roots
            .and_then(|roots| roots.orchard.as_deref()),
        "final_ironwood_root": final_note_commitment_roots
            .and_then(|roots| roots.ironwood.as_deref()),
        "coinbase_hex": coinbase_miner_fields.map(|coinbase| coinbase.miner_data_hex.as_str()),
        "coinbase_text": coinbase_miner_fields.map(|coinbase| coinbase.miner_data_text.as_str()),
        "finality_status": Value::Null,
        "isOrphaned": !summary.is_canonical,
        "transactions": transaction_rows.rows,
        "zinderUnavailable": transaction_rows.unavailable,
    })
}

async fn anchor_root_search(
    State(adapter): State<CipherscanRestAdapter>,
    Path(root): Path<String>,
) -> Result<Response, CipherscanRestError> {
    if !is_rpc_hash(&root) {
        return Ok(json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "error": "Invalid anchor root (expected 64-char hex)",
            }),
        ));
    }

    let root = root.to_ascii_lowercase();
    let root_bytes = hex::decode(&root).map_err(|_| {
        CipherscanRestError::InvalidRequest(
            "anchor root must contain exactly 64 hexadecimal characters".to_owned(),
        )
    })?;
    let response = adapter
        .explorer_client()
        .commitment_root_search(CommitmentRootSearchRequest {
            root: root_bytes,
            max_matches: DEFAULT_LIMIT,
        })
        .await?
        .into_inner();
    let at_epoch_id = explorer_chain_epoch_id(response.freshness.as_ref());
    let coverage = response
        .coverage
        .ok_or(CipherscanRestError::MissingUpstreamField(
            "commitment_root_search.coverage",
        ))?;
    let server_info = adapter.fetch_explorer_server_info().await?;
    let displaced_root_capability =
        explorer_supports_capability(&server_info, EXPLORER_COMMITMENT_ROOT_DISPLACED_MATCHES_V1);
    let displaced_root_coverage = response.displaced_coverage.as_ref();
    let mut canonical = Vec::with_capacity(response.matches.len());
    for root_match in &response.matches {
        let production_entry = adapter
            .fetch_canonical_block_production_entry(
                root_match.block_height,
                &root_match.block_hash,
                at_epoch_id,
            )
            .await?;
        canonical.push(commitment_root_match_json(
            root_match,
            production_entry.miner_address.as_deref(),
            "canonical",
            None,
        )?);
    }
    let orphaned = if displaced_root_capability && displaced_root_coverage.is_some() {
        response
            .displaced_matches
            .iter()
            .map(|root_match| commitment_root_match_json(root_match, None, "orphaned", None))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        Vec::new()
    };

    Ok(json_response(
        StatusCode::OK,
        commitment_root_search_json(CommitmentRootSearchJsonInput {
            root: &root,
            canonical: &canonical,
            orphaned: &orphaned,
            canonical_coverage: &coverage,
            displaced_root_capability,
            displaced_root_coverage,
        }),
    ))
}

async fn transaction_detail(
    State(adapter): State<CipherscanRestAdapter>,
    Path(transaction_id): Path<String>,
) -> Result<Response, CipherscanRestError> {
    let response = adapter
        .explorer_client()
        .transaction_detail(TransactionDetailRequest {
            transaction_id,
            at_epoch_id: None,
        })
        .await?
        .into_inner();
    let Some(mined_transaction) = mined_location(response.location.as_ref()) else {
        return Ok(json_response(
            StatusCode::NOT_FOUND,
            json!({ "error": "Transaction not found" }),
        ));
    };
    let facts = response
        .facts
        .as_ref()
        .ok_or(CipherscanRestError::MissingUpstreamField("facts"))?;
    validate_transaction_detail_outputs(facts, &response)?;
    let coinbase_total_output_zat = if facts.is_coinbase {
        let mined_block_height = mined_transaction
            .location
            .as_ref()
            .map(|location| location.block_height);
        match mined_block_height {
            Some(block_height) => Some(
                adapter
                    .fetch_coinbase_total_output_zat(block_height)
                    .await?,
            ),
            None => None,
        }
    } else {
        None
    };
    let coinbase_miner_fields = if facts.is_coinbase {
        raw_transaction_bytes(response.location.as_ref())
            .map(decode_cipherscan_coinbase_miner_fields)
            .transpose()?
    } else {
        None
    };
    Ok(json_response(
        StatusCode::OK,
        transaction_detail_json(CipherscanTransactionDetailJsonInput {
            network: adapter.network,
            facts,
            location: response.location.as_ref(),
            response: &response,
            coinbase_total_output_zat,
            coinbase_miner_fields: coinbase_miner_fields.as_ref(),
        }),
    ))
}

fn validate_transaction_detail_outputs(
    facts: &explorer::TransactionPublicFacts,
    response: &explorer::TransactionDetailResponse,
) -> Result<(), CipherscanRestError> {
    let expected_output_count = facts
        .counts
        .as_ref()
        .map_or(0, |counts| counts.transparent_output_count);
    if usize::try_from(expected_output_count).unwrap_or(usize::MAX)
        != response.transparent_outputs.len()
    {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "transparent_outputs",
        ));
    }
    for (expected_output_index, transparent_output) in
        response.transparent_outputs.iter().enumerate()
    {
        if transparent_output.output_index
            != u32::try_from(expected_output_index).unwrap_or(u32::MAX)
        {
            return Err(CipherscanRestError::InvalidUpstreamField(
                "transparent_outputs.output_index",
            ));
        }
        transparent_output
            .output
            .as_ref()
            .ok_or(CipherscanRestError::MissingUpstreamField(
                "transparent_outputs.output",
            ))?;
        let Some(spend) = transparent_output.spent_by.as_ref() else {
            continue;
        };
        let spent_outpoint =
            spend
                .spent_outpoint
                .as_ref()
                .ok_or(CipherscanRestError::MissingUpstreamField(
                    "transparent_outputs.spent_by.spent_outpoint",
                ))?;
        if spent_outpoint.transaction_id != facts.transaction_id
            || spent_outpoint.output_index != transparent_output.output_index
            || !is_rpc_transaction_id(&spend.spending_transaction_id)
        {
            return Err(CipherscanRestError::InvalidUpstreamField(
                "transparent_outputs.spent_by",
            ));
        }
        let spending_block =
            spend
                .spending_block
                .as_ref()
                .ok_or(CipherscanRestError::MissingUpstreamField(
                    "transparent_outputs.spent_by.spending_block",
                ))?;
        if !is_rpc_hash(&spending_block.hash) {
            return Err(CipherscanRestError::InvalidUpstreamField(
                "transparent_outputs.spent_by.spending_block.hash",
            ));
        }
    }
    Ok(())
}

async fn raw_transaction(
    State(adapter): State<CipherscanRestAdapter>,
    Path(transaction_id): Path<String>,
) -> Result<Response, CipherscanRestError> {
    let response = adapter
        .wallet_client()
        .transaction(TransactionRequest {
            transaction_id: transaction_id.clone(),
            at_epoch_id: None,
        })
        .await?
        .into_inner();
    let raw_bytes = raw_transaction_bytes(response.location.as_ref()).ok_or(
        CipherscanRestError::MissingUpstreamField(
            "location.mined.raw_transaction_bytes or location.in_mempool.raw_transaction_bytes",
        ),
    )?;
    Ok(json_response(
        StatusCode::OK,
        json!({
            "txid": transaction_id,
            "hex": hex::encode(raw_bytes),
        }),
    ))
}

async fn transaction_linkability(
    State(adapter): State<CipherscanRestAdapter>,
    Path(transaction_id): Path<String>,
    Query(query): Query<TransactionLinkabilityQuery>,
) -> Result<Response, CipherscanRestError> {
    if !is_rpc_transaction_id(&transaction_id) {
        return Ok(invalid_txid_path_parameters_response());
    }

    let normalized_transaction_id = transaction_id.to_ascii_lowercase();
    adapter
        .require_explorer_capability(EXPLORER_VALUE_POOL_FLOW_EVENTS_IN_RANGE_V1)
        .await?;
    let request = CipherscanLinkabilityRequest::from_query(&query);
    adapter
        .fetch_transaction_linkability(
            &normalized_transaction_id,
            request,
            OffsetDateTime::now_utc().unix_timestamp(),
        )
        .await?
        .map_or_else(
            || {
                Ok(json_response(
                    StatusCode::NOT_FOUND,
                    transaction_linkability_not_found_json(),
                ))
            },
            |outcome| match outcome {
                CipherscanLinkabilityOutcome::Available(response) => {
                    Ok(json_response(StatusCode::OK, response))
                }
                CipherscanLinkabilityOutcome::Unavailable(response) => {
                    Ok(json_response(StatusCode::SERVICE_UNAVAILABLE, response))
                }
            },
        )
}

async fn verbose_transaction(
    State(adapter): State<CipherscanRestAdapter>,
    Path(transaction_id): Path<String>,
) -> Result<Response, CipherscanRestError> {
    if !is_rpc_transaction_id(&transaction_id) {
        return Ok(json_response(
            StatusCode::BAD_REQUEST,
            json!({ "error": "Invalid transaction ID" }),
        ));
    }

    let response = match adapter
        .wallet_client()
        .transaction(TransactionRequest {
            transaction_id: transaction_id.clone(),
            at_epoch_id: None,
        })
        .await
    {
        Ok(response) => response.into_inner(),
        Err(status) if status.code() == Code::NotFound => {
            return Ok(json_response(
                StatusCode::NOT_FOUND,
                json!({ "error": "Transaction not found" }),
            ));
        }
        Err(status) => return Err(CipherscanRestError::Upstream(status)),
    };

    let Some(raw_bytes) = raw_transaction_bytes(response.location.as_ref()) else {
        return Ok(json_response(
            StatusCode::NOT_FOUND,
            json!({ "error": "Transaction not found" }),
        ));
    };

    Ok(json_response(
        StatusCode::OK,
        verbose_transaction_json(&transaction_id, raw_bytes),
    ))
}

async fn raw_transactions_batch(
    State(adapter): State<CipherscanRestAdapter>,
    Json(body): Json<Value>,
) -> Result<Response, CipherscanRestError> {
    let txids = match parse_raw_transaction_batch_txids(&body) {
        Ok(txids) => txids,
        Err(error) => return Ok(error.into_response()),
    };
    let total = txids.len();
    let mut transactions = Vec::new();
    let mut failed = Vec::new();
    let mut wallet_client = adapter.wallet_client();

    for transaction_id in txids {
        let response = wallet_client
            .transaction(TransactionRequest {
                transaction_id: transaction_id.clone(),
                at_epoch_id: None,
            })
            .await;

        match response {
            Ok(response) => {
                let response = response.into_inner();
                if let Some(raw_bytes) = raw_transaction_bytes(response.location.as_ref()) {
                    transactions.push(raw_transaction_batch_row(&transaction_id, raw_bytes));
                } else {
                    failed.push(raw_transaction_batch_failure(
                        &transaction_id,
                        "Transaction raw bytes are unavailable",
                    ));
                }
            }
            Err(error) => {
                failed.push(raw_transaction_batch_failure(
                    &transaction_id,
                    error.message(),
                ));
            }
        }
    }

    Ok(json_response(
        StatusCode::OK,
        raw_transaction_batch_json(&transactions, &failed, total),
    ))
}

async fn scan_orchard(
    State(adapter): State<CipherscanRestAdapter>,
    Json(body): Json<Value>,
) -> Result<Response, CipherscanRestError> {
    let range = match parse_orchard_candidate_scan_range(&body) {
        Ok(range) => range,
        Err(error) => return Ok(scan_bad_request_response(error)),
    };
    adapter
        .require_explorer_capability(EXPLORER_TRANSACTION_HISTORY_V2)
        .await?;
    let scan = adapter.fetch_orchard_candidates(range).await?;
    adapter.require_orchard_candidate_raw_bytes(&scan).await?;
    Ok(json_response(
        StatusCode::OK,
        orchard_candidate_scan_json(range, &scan.entries),
    ))
}

async fn broadcast_transaction(
    State(adapter): State<CipherscanRestAdapter>,
    Json(body): Json<Value>,
) -> Result<Response, CipherscanRestError> {
    let raw_tx = match parse_broadcast_raw_transaction(&body) {
        Ok(raw_tx) => raw_tx,
        Err(error) => return Ok(error.into_response()),
    };
    let raw_transaction = hex::decode(raw_tx)?;
    let response = adapter
        .wallet_client()
        .broadcast_transaction(BroadcastTransactionRequest { raw_transaction })
        .await?
        .into_inner();
    Ok(broadcast_response(response.outcome))
}

async fn transactions_list(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<PageQuery>,
) -> Result<Response, CipherscanRestError> {
    adapter
        .require_explorer_capability(EXPLORER_TRANSACTION_HISTORY_V2)
        .await?;
    let limit = query_limit(query.limit, DEFAULT_RECENT_TRANSACTION_LIMIT);
    let direction = cipherscan_transaction_history_direction(query.direction.as_deref());
    let filter = transaction_list_history_filter(query.transaction_type.as_deref());
    let start = query.cursor.map(|block_height| {
        transaction_history_request::Start::Anchor(TransactionHistoryAnchor {
            block_height,
            transaction_index: query.cursor_idx.unwrap_or(0),
        })
    });
    let history = adapter
        .fetch_transaction_history_with_cached_count(TransactionHistoryRequest {
            page_size: limit,
            start,
            direction: direction as i32,
            filter: Some(filter),
            include_total_count: true,
            read_fence: None,
        })
        .await?;
    let first = history.entries.first();
    let last = history.entries.last();
    let rows: Vec<Value> = history.entries.iter().map(recent_transaction_row).collect();
    let total = history.total_matching_transactions.unwrap_or(0);
    let total_pages = total.div_ceil(u64::from(limit));
    Ok(json_response(
        StatusCode::OK,
        json!({
            "success": true,
            "transactions": rows,
            "pagination": {
                "total": total,
                "totalPages": total_pages,
                "limit": limit,
                "hasNext": history.has_older,
                "hasPrev": history.has_newer,
                "nextCursor": last.map(|entry| entry.block_height),
                "nextCursorIdx": last.map(|entry| entry.transaction_index),
                "prevCursor": first.map(|entry| entry.block_height),
                "prevCursorIdx": first.map(|entry| entry.transaction_index),
            },
            "degraded": true,
            "unavailable": [
                "Shielded value balances are encrypted, and recent rows use ZIP-317 conventional fees when actual paid fees are unavailable."
            ],
        }),
    ))
}

async fn shielded_transactions(
    State(adapter): State<CipherscanRestAdapter>,
    Query(raw_query): Query<ShieldedTransactionQuery>,
) -> Result<Response, CipherscanRestError> {
    adapter
        .require_explorer_capability(EXPLORER_TRANSACTION_HISTORY_V2)
        .await?;
    let query = match parse_shielded_transaction_query(raw_query) {
        Ok(query) => query,
        Err(details) => {
            return Ok(json_response(
                StatusCode::BAD_REQUEST,
                json!({
                    "error": "Invalid query parameters",
                    "details": details,
                }),
            ));
        }
    };
    let limit = query_limit(query.limit, DEFAULT_RECENT_TRANSACTION_LIMIT);
    let offset = query.offset.unwrap_or(0);
    let skip_count = query.skip_count.as_deref() == Some("true") || (offset == 0 && limit <= 10);
    let filter = shielded_transaction_history_filter(&query)?;
    let window = adapter
        .fetch_transaction_history_offset(filter, offset, limit, !skip_count)
        .await?;
    let rows: Vec<Value> = window
        .entries
        .iter()
        .map(shielded_transaction_row)
        .collect();
    let total = if skip_count {
        0
    } else {
        window
            .total_matching_transactions
            .ok_or(CipherscanRestError::MissingUpstreamField(
                "transaction_history.total_matching_transactions",
            ))?
    };
    let has_more = if skip_count {
        window.has_more
    } else {
        u64::from(offset).saturating_add(u64::from(limit)) < total
    };
    Ok(json_response(
        StatusCode::OK,
        json!({
            "transactions": rows,
            "pagination": {
                "total": total,
                "limit": limit,
                "offset": offset,
                "hasMore": has_more,
            },
            "filters": {
                "pool": query.pool.as_deref().unwrap_or("all"),
                "type": query.transaction_type.as_deref().unwrap_or("all"),
                "minActions": query.min_actions.unwrap_or(0),
            },
            "degraded": true,
            "unavailable": [
                "Actual paid fees and any non-retained intrinsic pool balances remain null; ZIP-317 conventional fees are exposed separately. Rows, filters, and exact pagination totals come from explorer.transaction.history_v2."
            ],
        }),
    ))
}

async fn shielded_flows(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<ShieldedFlowQuery>,
) -> Result<Response, CipherscanRestError> {
    let limit = query_limit(query.limit, DEFAULT_RECENT_TRANSACTION_LIMIT);
    adapter
        .require_explorer_capability(EXPLORER_VALUE_POOL_FLOW_HISTORY_V1)
        .await?;
    let filter = shielded_flow_filter(&query)?;
    let anchor = shielded_flow_anchor(&query)?;
    let direction = shielded_flow_page_direction(query.direction.as_deref());
    let page = adapter
        .fetch_shielded_flow_page(filter, anchor, direction, limit)
        .await?;
    let rows = page
        .events
        .iter()
        .map(shielded_flow_row)
        .collect::<Result<Vec<_>, _>>()?;
    let first = page
        .events
        .first()
        .map(CipherscanFlowCursor::from_event)
        .transpose()?;
    let last = page
        .events
        .last()
        .map(CipherscanFlowCursor::from_event)
        .transpose()?;
    let total_pages = page.total_matching_events.div_ceil(u64::from(limit));
    Ok(json_response(
        StatusCode::OK,
        json!({
            "success": true,
            "flows": rows,
            "pagination": {
                "total": page.total_matching_events,
                "totalPages": total_pages,
                "limit": limit,
                "hasNext": page.has_older,
                "hasPrev": page.has_newer,
                "nextCursor": last.map(|cursor| cursor.block_time_unix_seconds),
                "nextCursorId": last.map(|cursor| cursor.coordinate.stable_id()),
                "prevCursor": first.map(|cursor| cursor.block_time_unix_seconds),
                "prevCursorId": first.map(|cursor| cursor.coordinate.stable_id()),
            },
            "coverage": value_pool_flow_coverage_json(&page.coverage),
            "degraded": true,
            "unavailable": [
                "Transparent address attribution is not available for value-pool flow events. addresses is an empty array rather than inferred data."
            ],
        }),
    ))
}

async fn address_detail(
    State(adapter): State<CipherscanRestAdapter>,
    Path(address): Path<String>,
    Query(query): Query<PageQuery>,
) -> Result<Response, CipherscanRestError> {
    let (page, limit, offset) = address_activity_page(&query)?;
    if let Some(response) = private_address_response(&address) {
        return Ok(response);
    }
    let address_lookup = AddressLookup {
        selector: Some(address_lookup::Selector::Address(address.clone())),
    };
    let activity_result = adapter
        .explorer_client()
        .transparent_address_activity(TransparentAddressActivityRequest {
            address: Some(address_lookup),
            start_height: 0,
            end_height: u32::MAX,
            max_entries: limit,
            from_cursor: Vec::new(),
            at_epoch_id: None,
            offset: u64::from(offset),
        })
        .await;
    let activity = match activity_result {
        Ok(response) => response,
        Err(status) if status.code() == Code::InvalidArgument => {
            return Ok(json_response(
                StatusCode::NOT_FOUND,
                json!({
                    "error": "Invalid address format",
                }),
            ));
        }
        Err(status) => return Err(status.into()),
    }
    .into_inner();
    Ok(json_response(
        StatusCode::OK,
        address_detail_json(&CipherscanAddressDetailInput {
            network: adapter.network,
            address: &address,
            page,
            limit,
            activity: &activity,
        })?,
    ))
}

async fn rich_list(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<RichListQuery>,
) -> Result<Response, CipherscanRestError> {
    let limit = rich_list_limit(query.limit.as_deref());
    let offset = rich_list_offset(query.offset.as_deref());
    let ranking = adapter
        .explorer_client()
        .transparent_address_ranking(TransparentAddressRankingRequest { limit, offset })
        .await?
        .into_inner();

    Ok(json_response(
        StatusCode::OK,
        rich_list_json(adapter.network, limit, offset, &ranking)?,
    ))
}

fn rich_list_json(
    network: Network,
    limit: u32,
    offset: u64,
    ranking: &TransparentAddressRankingResponse,
) -> Result<Value, CipherscanRestError> {
    let lifetime_statistics_complete = ranking
        .coverage
        .as_ref()
        .is_some_and(|coverage| coverage.lifetime_statistics_complete);
    let mapped_entries = ranking
        .entries
        .iter()
        .map(|entry| rich_list_entry_json(network, lifetime_statistics_complete, entry))
        .collect::<Result<Vec<_>, CipherscanRestError>>()?;
    let has_missing_lifetime_fields = mapped_entries
        .iter()
        .any(|(_entry, has_missing_fields)| *has_missing_fields);
    let addresses = mapped_entries
        .into_iter()
        .map(|(entry, _has_missing_fields)| entry)
        .collect::<Vec<_>>();
    let unavailable = rich_list_unavailable(ranking, has_missing_lifetime_fields)?;
    let total_pages = ranking.positive_address_count.div_ceil(u64::from(limit));
    let page = offset.saturating_div(u64::from(limit)).saturating_add(1);

    Ok(json!({
        "success": true,
        "addresses": addresses,
        "concentration": {
            "top10": zec_from_unsigned_zatoshis(ranking.top_10_balance_zat),
            "top100": zec_from_unsigned_zatoshis(ranking.top_100_balance_zat),
            "totalTransparent": zec_from_unsigned_zatoshis(ranking.total_positive_balance_zat),
            "top10Pct": zatoshi_percentage(
                ranking.top_10_balance_zat,
                ranking.total_positive_balance_zat,
            ),
            "top100Pct": zatoshi_percentage(
                ranking.top_100_balance_zat,
                ranking.total_positive_balance_zat,
            ),
        },
        "pagination": {
            "total": ranking.positive_address_count,
            "limit": limit,
            "offset": offset,
            "totalPages": total_pages,
            "page": page,
            "hasNext": offset.saturating_add(u64::from(limit))
                < ranking.positive_address_count,
            "hasPrev": offset > 0,
        },
        "degraded": !unavailable.is_empty(),
        "unavailable": unavailable,
    }))
}

fn rich_list_entry_json(
    network: Network,
    lifetime_statistics_complete: bool,
    entry: &explorer::TransparentAddressRankingEntry,
) -> Result<(Value, bool), CipherscanRestError> {
    let address = cipherscan_transparent_address(network, &entry.script_pub_key).ok_or(
        CipherscanRestError::InvalidUpstreamField(
            "transparent_address_ranking.entries.script_pub_key",
        ),
    )?;
    let has_missing_lifetime_fields = lifetime_statistics_complete
        && (entry.total_received_zat.is_none()
            || entry.total_sent_zat.is_none()
            || entry.distinct_transaction_count.is_none()
            || entry.first_seen_unix_seconds.is_none()
            || entry.last_seen_unix_seconds.is_none());

    Ok((
        json!({
            "rank": entry.rank,
            "address": address,
            "balance": zec_from_unsigned_zatoshis(entry.balance_zat),
            "totalReceived": entry.total_received_zat
                .filter(|_| lifetime_statistics_complete)
                .map(zec_from_unsigned_zatoshis),
            "totalSent": entry.total_sent_zat
                .filter(|_| lifetime_statistics_complete)
                .map(zec_from_unsigned_zatoshis),
            "txCount": entry.distinct_transaction_count.filter(|_| lifetime_statistics_complete),
            "firstSeen": entry.first_seen_unix_seconds
                .filter(|_| lifetime_statistics_complete)
                .map(|seconds| seconds.to_string()),
            "lastSeen": entry.last_seen_unix_seconds
                .filter(|_| lifetime_statistics_complete)
                .map(|seconds| seconds.to_string()),
            "label": Value::Null,
            "category": Value::Null,
            "description": Value::Null,
            "verified": false,
            "logoUrl": Value::Null,
        }),
        has_missing_lifetime_fields,
    ))
}

fn rich_list_unavailable(
    ranking: &TransparentAddressRankingResponse,
    has_missing_lifetime_fields: bool,
) -> Result<Vec<&'static str>, CipherscanRestError> {
    let mut unavailable = Vec::new();
    let visible_tip_height = ranking
        .freshness
        .as_ref()
        .and_then(|freshness| freshness.chain_view.as_ref())
        .and_then(|chain_view| chain_view.chain_epoch.as_ref())
        .and_then(|chain_epoch| chain_epoch.visible_tip.as_ref())
        .map(|tip| tip.height);

    match (ranking.coverage.as_ref(), visible_tip_height) {
        (Some(coverage), Some(visible_tip_height))
            if coverage.balance_complete_through_height > visible_tip_height =>
        {
            return Err(CipherscanRestError::InvalidUpstreamField(
                "transparent_address_ranking.coverage.balance_complete_through_height",
            ));
        }
        (Some(coverage), Some(visible_tip_height))
            if coverage.balance_complete_through_height < visible_tip_height =>
        {
            unavailable.push(
                "Transparent address balances and concentration metrics are not complete through the visible chain tip.",
            );
        }
        (Some(_), Some(_)) => {}
        (None, _) => {
            unavailable.push("Transparent address ranking coverage metadata is unavailable.");
        }
        (Some(_), None) => unavailable.push(
            "The visible chain tip is unavailable, so current ranking coverage cannot be verified.",
        ),
    }

    if !ranking
        .coverage
        .as_ref()
        .is_some_and(|coverage| coverage.lifetime_statistics_complete)
    {
        unavailable.push(
            "Address lifetime totals, transaction counts, and first/last seen timestamps are unavailable because native history coverage is incomplete.",
        );
    } else if has_missing_lifetime_fields {
        unavailable.push(
            "One or more address lifetime totals, transaction counts, or first/last seen timestamps are unavailable.",
        );
    }
    if ranking
        .freshness
        .as_ref()
        .is_some_and(|freshness| !freshness.unavailable.is_empty())
    {
        unavailable.push("The native transparent address ranking reports unavailable fields.");
    }

    Ok(unavailable)
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Cipherscan concentration percentages are JSON floating-point numbers."
)]
fn zatoshi_percentage(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        return 0.0;
    }
    (numerator as f64 / denominator as f64) * 100.0
}

async fn labels() -> Response {
    sidecar_unavailable_response(
        "Address labels are unavailable",
        "Address-label registry data requires a Cipherscan sidecar.",
    )
}

async fn label_lookup(Path(_address): Path<String>) -> Response {
    sidecar_unavailable_response(
        "Address labels are unavailable",
        "Address-label registry data requires a Cipherscan sidecar.",
    )
}

async fn price_at(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<HistoricalPriceQuery>,
) -> Response {
    let Some(date) = query.date else {
        return historical_price_invalid_date_response();
    };
    if !has_iso8601_calendar_date_shape(&date) {
        return historical_price_invalid_date_response();
    }

    let lookup_date = historical_price_lookup_date(&date);
    match adapter
        .market_price_client
        .historical_price(&lookup_date)
        .await
    {
        Ok(HistoricalMarketPriceLookup::Price(mut historical_price)) => {
            if lookup_date != date {
                historical_price.date = date;
                historical_price.actual_date = Some(lookup_date);
                historical_price.exact = false;
            }
            json_response(
                StatusCode::OK,
                historical_market_price_json(historical_price),
            )
        }
        Ok(HistoricalMarketPriceLookup::NoPrice) => {
            json_response(StatusCode::OK, historical_price_json(&date))
        }
        Err(error) => {
            tracing::warn!(
                target: "zinder::compat::cipherscan",
                event = "historical_price_unavailable",
                requested_date = date,
                lookup_date,
                error = %error,
                "Historical market price is unavailable"
            );
            json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "error": "Failed to fetch price" }),
            )
        }
    }
}

async fn price(State(adapter): State<CipherscanRestAdapter>) -> Response {
    match adapter.market_price_client.current_price().await {
        Ok(current_price) => json_response(
            StatusCode::OK,
            json!({
                "price": current_price.price,
                "change24h": current_price.change_24h,
                "timestamp": current_price.timestamp,
            }),
        ),
        Err(MarketPriceError::UpstreamStatus(_)) => json_response(
            StatusCode::BAD_GATEWAY,
            json!({ "error": "Price service unavailable" }),
        ),
        Err(error) => {
            tracing::warn!(
                target: "zinder::compat::cipherscan",
                event = "current_price_unavailable",
                error = %error,
                "Current market price is unavailable"
            );
            json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "error": "Failed to fetch price" }),
            )
        }
    }
}

async fn mempool(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<PageQuery>,
) -> Result<Response, CipherscanRestError> {
    let limit = query_limit(query.limit, DEFAULT_MEMPOOL_LIMIT);
    let mut explorer_client = adapter.explorer_client();
    let snapshot = explorer_client
        .mempool_snapshot(MempoolSnapshotRequest {
            max_entries: limit,
            from_cursor: Vec::new(),
        })
        .await?
        .into_inner();
    let summary = snapshot
        .summary
        .as_ref()
        .ok_or(CipherscanRestError::MissingUpstreamField("summary"))?;
    let transactions: Vec<Value> = snapshot.entries.iter().map(mempool_row).collect();
    Ok(json_response(
        StatusCode::OK,
        json!({
            "success": true,
            "count": summary.transaction_count,
            "showing": transactions.len(),
            "transactions": transactions,
            "stats": mempool_stats_json(summary),
        }),
    ))
}

async fn mempool_transaction(
    State(adapter): State<CipherscanRestAdapter>,
    Path(transaction_id): Path<String>,
) -> Result<Response, CipherscanRestError> {
    if !is_rpc_transaction_id(&transaction_id) {
        return Ok(json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "success": false,
                "error": "Invalid txid",
            }),
        ));
    }

    let normalized_transaction_id = transaction_id.to_ascii_lowercase();
    let response = match adapter
        .explorer_client()
        .transaction_detail(TransactionDetailRequest {
            transaction_id: normalized_transaction_id,
            at_epoch_id: None,
        })
        .await
    {
        Ok(response) => response.into_inner(),
        Err(status) if status.code() == Code::NotFound => {
            return Ok(mempool_transaction_not_found_response());
        }
        Err(status) => return Err(status.into()),
    };

    let Some(mempool) = mempool_location(response.location.as_ref()) else {
        return Ok(mempool_transaction_not_found_response());
    };

    let facts = response
        .facts
        .as_ref()
        .ok_or(CipherscanRestError::MissingUpstreamField("facts"))?;
    validate_transaction_detail_outputs(facts, &response)?;

    Ok(json_response(
        StatusCode::OK,
        json!({
            "success": true,
            "inMempool": true,
            "transaction": mempool_transaction_json(adapter.network, facts, mempool, &response),
        }),
    ))
}

async fn crosschain_stats() -> Response {
    crosschain_sidecar_unavailable_response()
}

async fn crosschain_inflows() -> Response {
    crosschain_sidecar_unavailable_response()
}

async fn crosschain_outflows() -> Response {
    crosschain_sidecar_unavailable_response()
}

async fn crosschain_status() -> Response {
    crosschain_sidecar_unavailable_response()
}

async fn crosschain_db_stats() -> Response {
    crosschain_sidecar_unavailable_response()
}

async fn crosschain_trends() -> Response {
    crosschain_sidecar_unavailable_response()
}

async fn crosschain_history() -> Response {
    crosschain_sidecar_unavailable_response()
}

async fn crosschain_volume_by_chain() -> Response {
    crosschain_sidecar_unavailable_response()
}

async fn crosschain_address(Path(_address): Path<String>) -> Response {
    crosschain_sidecar_unavailable_response()
}

async fn crosschain_popular_pairs() -> Response {
    crosschain_sidecar_unavailable_response()
}

async fn name_lookup(Path(_name): Path<String>) -> Response {
    sidecar_unavailable_response(
        "ZNS registration data is unavailable",
        "ZNS availability and pricing require a Cipherscan ZNS sidecar.",
    )
}

async fn name_events(Path(name): Path<String>) -> Response {
    json_response(StatusCode::OK, name_events_json(&name))
}

async fn crosslink_stats() -> Response {
    crosslink_consensus_unavailable_response()
}

async fn crosslink_bft_chain() -> Response {
    crosslink_consensus_unavailable_response()
}

async fn crosslink_bft_tip() -> Response {
    crosslink_consensus_unavailable_response()
}

async fn crosslink_divergence_history() -> Response {
    json_response(StatusCode::OK, crosslink_divergence_history_json())
}

async fn crosslink_bootstrap_info() -> Response {
    json_response(StatusCode::OK, crosslink_bootstrap_info_json())
}

async fn finalizers() -> Response {
    crosslink_consensus_unavailable_response()
}

async fn finalizer_detail(Path(pubkey): Path<String>) -> Response {
    if !is_finalizer_pubkey(&pubkey) {
        return json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "success": false,
                "error": "Invalid finalizer pubkey",
            }),
        );
    }

    crosslink_consensus_unavailable_response()
}

async fn finalizer_participation(Path(pubkey): Path<String>) -> Response {
    if !is_finalizer_pubkey(&pubkey) {
        return json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "success": false,
                "error": "Invalid pubkey",
            }),
        );
    }

    crosslink_consensus_unavailable_response()
}

async fn crosslink_participation() -> Response {
    crosslink_consensus_unavailable_response()
}

async fn fork_monitor(
    State(adapter): State<CipherscanRestAdapter>,
) -> Result<Response, CipherscanRestError> {
    let mut wallet_client = adapter.wallet_client();
    let visible_tip_block = wallet_client
        .visible_tip_block(VisibleTipBlockRequest { at_epoch_id: None })
        .await?
        .into_inner()
        .visible_tip_block
        .ok_or(CipherscanRestError::MissingUpstreamField(
            "visible_tip_block",
        ))?;
    let settled_tip_height = wallet_client
        .settled_tip_block(SettledTipBlockRequest { at_epoch_id: None })
        .await?
        .into_inner()
        .settled_tip_block
        .map_or(0, |block| block.height);

    let mut anchors = Vec::new();
    for &(height, label) in FORK_MONITOR_ANCHORS {
        if height > visible_tip_block.height {
            continue;
        }
        let cipherscan_hash = canonical_block_hash_at_height(&adapter, height).await?;
        anchors.push(json!({
            "height": height,
            "label": label,
            "cipherscan_hash": cipherscan_hash,
            "ctaz_hash": Value::Null,
            "match": Value::Null,
        }));
    }

    Ok(json_response(
        StatusCode::OK,
        json!({
            "generated_at": current_rfc3339_timestamp(),
            "cipherscan": {
                "tip": visible_tip_block.height,
                "tip_hash": visible_tip_block.block_hash,
                "peers": 0,
                "finalized": settled_tip_height,
                "finality_gap": visible_tip_block.height.saturating_sub(settled_tip_height),
            },
            "ctaz": Value::Null,
            "status": "ctaz_unavailable",
            "first_divergence": Value::Null,
            "anchors": anchors,
            "nodes": [],
            "split_hints": FORK_MONITOR_SPLIT_HINTS,
            "degraded": true,
            "unavailable": [
                "cTAZ comparison and community node registry require a Cipherscan fork-monitor sidecar.",
                "Peer counts are not available from the storage-backed Zinder query plane."
            ],
        }),
    ))
}

async fn reorg_stats(
    State(adapter): State<CipherscanRestAdapter>,
) -> Result<Response, CipherscanRestError> {
    let snapshot = adapter.fetch_chain_reorg_history().await?;
    let archive = fetch_recent_displaced_blocks(&adapter).await?;
    Ok(json_response(
        StatusCode::OK,
        reorg_stats_json(
            &snapshot,
            archive.as_ref().map(|archive| archive.total_count),
        ),
    ))
}

async fn non_canonical_blocks(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<ReorgPageQuery>,
) -> Result<Response, CipherscanRestError> {
    let limit = reorg_page_limit(
        query.limit.as_deref(),
        DEFAULT_NON_CANONICAL_BLOCK_LIMIT,
        MAX_NON_CANONICAL_BLOCK_LIMIT,
    );
    let offset = reorg_page_offset(query.offset.as_deref());
    adapter
        .require_explorer_capability(EXPLORER_CHAIN_DISPLACED_BLOCK_HISTORY_V1)
        .await?;
    let page = fetch_displaced_block_page(&adapter, limit, offset).await?;
    Ok(json_response(
        StatusCode::OK,
        non_canonical_blocks_json(adapter.network, limit, offset, &page)?,
    ))
}

async fn reorg_forks(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<ReorgPageQuery>,
) -> Result<Response, CipherscanRestError> {
    let limit = reorg_page_limit(query.limit.as_deref(), DEFAULT_REORG_FORK_LIMIT, MAX_LIMIT);
    let offset = reorg_page_offset(query.offset.as_deref());
    let snapshot = adapter.fetch_chain_reorg_history().await?;
    let displaced_blocks = fetch_recent_displaced_blocks(&adapter).await?;
    Ok(json_response(
        StatusCode::OK,
        reorg_forks_with_archive_json(
            adapter.network,
            &snapshot,
            limit,
            offset,
            displaced_blocks.as_ref(),
        )?,
    ))
}

async fn fetch_recent_displaced_blocks(
    adapter: &CipherscanRestAdapter,
) -> Result<Option<DisplacedBlockHistoryResponse>, CipherscanRestError> {
    match adapter
        .explorer_client()
        .displaced_block_history(DisplacedBlockHistoryRequest {
            page_size: 4_096,
            cursor: Vec::new(),
        })
        .await
    {
        Ok(response) => Ok(Some(response.into_inner())),
        Err(status)
            if matches!(
                status.code(),
                tonic::Code::Unimplemented | tonic::Code::FailedPrecondition
            ) =>
        {
            Ok(None)
        }
        Err(status) => Err(status.into()),
    }
}

async fn reorg_nodes() -> Response {
    json_response(StatusCode::OK, reorg_nodes_json())
}

async fn non_canonical_block_detail(
    State(adapter): State<CipherscanRestAdapter>,
    Path(block_hash): Path<String>,
) -> Result<Response, CipherscanRestError> {
    if !is_rpc_hash(&block_hash) {
        return Ok(json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "error": "Invalid block hash",
            }),
        ));
    }
    adapter
        .require_explorer_capability(EXPLORER_CHAIN_DISPLACED_BLOCK_DETAIL_V1)
        .await?;
    let detail = match adapter
        .explorer_client()
        .displaced_block_detail(DisplacedBlockDetailRequest {
            block_hash: block_hash.clone(),
        })
        .await
    {
        Ok(response) => response.into_inner(),
        Err(status) if status.code() == tonic::Code::NotFound => {
            return Ok(json_response(
                StatusCode::NOT_FOUND,
                json!({
                    "success": false,
                    "error": "Orphaned block not found",
                }),
            ));
        }
        Err(status) => return Err(status.into()),
    };
    let snapshot = adapter.fetch_chain_reorg_history().await?;
    Ok(json_response(
        StatusCode::OK,
        non_canonical_block_detail_json(adapter.network, &detail, &snapshot)?,
    ))
}

async fn non_canonical_block_report() -> Response {
    json_response(
        StatusCode::SERVICE_UNAVAILABLE,
        json!({
            "success": false,
            "error": "Competing-tip reports require a Cipherscan fork-monitor sidecar",
            "degraded": true,
            "unavailable": [
                "Public fork-report ingestion is Cipherscan product registry behavior and is not stored in Zinder core."
            ],
        }),
    )
}

async fn fork_monitor_check(
    State(adapter): State<CipherscanRestAdapter>,
    Json(body): Json<ForkMonitorCheckBody>,
) -> Result<Response, CipherscanRestError> {
    if body.heights.is_empty() {
        return Ok(json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "success": false,
                "error": "heights must be a non-empty array",
            }),
        ));
    }
    if body.heights.len() > MAX_FORK_MONITOR_CHECK_HEIGHTS {
        return Ok(json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "success": false,
                "error": "max 10 heights per request",
            }),
        ));
    }

    let heights: Vec<u32> = body
        .heights
        .iter()
        .filter_map(parse_fork_monitor_height)
        .collect();
    if heights.is_empty() {
        return Ok(json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "success": false,
                "error": "no valid heights provided",
            }),
        ));
    }

    let mut results = Vec::with_capacity(heights.len());
    for height in heights {
        let cipherscan_hash = canonical_block_hash_at_height(&adapter, height).await?;
        results.push(json!({
            "height": height,
            "cipherscan_hash": cipherscan_hash,
            "ctaz_hash": Value::Null,
            "match": Value::Null,
        }));
    }

    Ok(json_response(
        StatusCode::OK,
        json!({
            "success": true,
            "results": results,
            "degraded": true,
            "unavailable": [
                "cTAZ comparison requires a Cipherscan fork-monitor sidecar."
            ],
        }),
    ))
}

async fn crosslink_block_hash(
    State(adapter): State<CipherscanRestAdapter>,
    Path(height): Path<String>,
) -> Result<Response, CipherscanRestError> {
    let Ok(height) = height.parse::<u32>() else {
        return Ok(json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "success": false,
                "error": "invalid height",
            }),
        ));
    };

    let Some(hash) = canonical_block_hash_at_height(&adapter, height).await? else {
        return Ok(json_response(
            StatusCode::NOT_FOUND,
            json!({
                "success": false,
                "error": "block not found",
            }),
        ));
    };

    Ok(json_response(
        StatusCode::OK,
        json!({
            "success": true,
            "height": height,
            "hash": hash,
        }),
    ))
}

async fn fork_monitor_report() -> Response {
    fork_monitor_registry_unavailable_response()
}

async fn fork_monitor_delete_report(Path(_node_name): Path<String>) -> Response {
    fork_monitor_registry_unavailable_response()
}

async fn privacy_stats(
    State(adapter): State<CipherscanRestAdapter>,
) -> Result<Response, CipherscanRestError> {
    Ok(json_response(
        StatusCode::OK,
        Box::pin(fetch_privacy_stats_json_with_epoch_retry(&adapter)).await?,
    ))
}

async fn fetch_privacy_stats_json_with_epoch_retry(
    adapter: &CipherscanRestAdapter,
) -> Result<Value, CipherscanRestError> {
    let mut attempt = 1;
    loop {
        match Box::pin(fetch_privacy_stats_json(adapter)).await {
            Err(error)
                if privacy_stats_epoch_changed(&error)
                    && attempt < PRIVACY_STATS_EPOCH_ATTEMPTS =>
            {
                tracing::debug!(
                    target: "zinder::compat::cipherscan",
                    event = "cipherscan_privacy_stats_epoch_retry",
                    attempt,
                    "retrying privacy stats after concurrent reader epoch movement"
                );
                attempt += 1;
                tokio::time::sleep(PRIVACY_STATS_EPOCH_RETRY_DELAY).await;
            }
            outcome => return outcome,
        }
    }
}

fn privacy_stats_epoch_changed(error: &CipherscanRestError) -> bool {
    matches!(
        error,
        CipherscanRestError::InvalidUpstreamField("privacy_stats.visible_tip")
    )
}

async fn fetch_privacy_stats_json(
    adapter: &CipherscanRestAdapter,
) -> Result<Value, CipherscanRestError> {
    adapter
        .require_explorer_capability(EXPLORER_TRANSACTION_COMPONENT_SUMMARY_V2)
        .await?;
    let generated_at = OffsetDateTime::now_utc();
    let now = generated_at.unix_timestamp();
    let current_day_start = calendar_date_start_unix_seconds(generated_at.date());
    let daily_start_time = current_day_start.saturating_sub(
        (PRIVACY_STATS_TREND_DAYS.saturating_sub(1)).saturating_mul(UNIX_SECONDS_PER_DAY),
    );
    let future_time = now.saturating_add(COMPONENT_SUMMARY_FUTURE_TIME_MARGIN_SECONDS);
    let seven_days_ago = now.saturating_sub(7 * UNIX_SECONDS_PER_DAY);
    let fourteen_days_ago = now.saturating_sub(14 * UNIX_SECONDS_PER_DAY);
    let mut value_pool_client = adapter.explorer_client();
    let mut history_client = adapter.explorer_client();
    let (value_pool_summary, value_pool_history) = tokio::try_join!(
        value_pool_client.value_pool_summary(ValuePoolSummaryRequest {}),
        history_client.value_pool_balance_history(ValuePoolBalanceHistoryRequest {
            page_size: u32::try_from(PRIVACY_STATS_TREND_POINT_LIMIT).unwrap_or(u32::MAX),
            cursor: Vec::new(),
        }),
    )?;
    let (
        all_history_summary,
        thirty_day_summary,
        daily_summary,
        recent_seven_day_summary,
        previous_seven_day_summary,
    ) = tokio::try_join!(
        adapter.fetch_transaction_component_summary(0, future_time, true),
        adapter.fetch_transaction_component_summary(
            now.saturating_sub(PRIVACY_STATS_TREND_DAYS * UNIX_SECONDS_PER_DAY),
            future_time,
            false,
        ),
        adapter.fetch_transaction_component_summary(daily_start_time, future_time, false,),
        adapter.fetch_transaction_component_summary(seven_days_ago, future_time, false),
        adapter.fetch_transaction_component_summary(fourteen_days_ago, seven_days_ago, false),
    )?;

    privacy_stats_json(
        &value_pool_summary.into_inner(),
        &value_pool_history.into_inner(),
        &all_history_summary,
        &thirty_day_summary,
        &daily_summary,
        &recent_seven_day_summary,
        &previous_seven_day_summary,
        generated_at,
    )
}

#[allow(
    clippy::too_many_lines,
    clippy::too_many_arguments,
    reason = "The Cipherscan contract is one nested JSON document whose source validation and unavailable markers are kept beside its field mapping."
)]
fn privacy_stats_json(
    value_pool_summary: &explorer::ValuePoolSummaryResponse,
    value_pool_history: &ValuePoolBalanceHistoryResponse,
    all_history_summary: &TransactionComponentSummaryResponse,
    thirty_day_summary: &TransactionComponentSummaryResponse,
    daily_summary: &TransactionComponentSummaryResponse,
    recent_seven_day_summary: &TransactionComponentSummaryResponse,
    previous_seven_day_summary: &TransactionComponentSummaryResponse,
    generated_at: OffsetDateTime,
) -> Result<Value, CipherscanRestError> {
    validate_value_pools(&value_pool_summary.pools)?;
    let visible_tip = verified_value_pool_source_tip(value_pool_summary)?;
    validate_privacy_summary_tip(
        value_pool_history.freshness.as_ref(),
        visible_tip,
        "value_pool_balance_history.freshness.chain_view.chain_epoch.visible_tip",
    )?;
    validate_privacy_summary_tip(
        all_history_summary.freshness.as_ref(),
        visible_tip,
        "transaction_component_summary.freshness.chain_view.chain_epoch.visible_tip",
    )?;
    validate_privacy_summary_tip(
        thirty_day_summary.freshness.as_ref(),
        visible_tip,
        "transaction_component_summary.freshness.chain_view.chain_epoch.visible_tip",
    )?;
    validate_privacy_summary_tip(
        daily_summary.freshness.as_ref(),
        visible_tip,
        "transaction_component_summary.freshness.chain_view.chain_epoch.visible_tip",
    )?;
    validate_privacy_summary_tip(
        recent_seven_day_summary.freshness.as_ref(),
        visible_tip,
        "transaction_component_summary.freshness.chain_view.chain_epoch.visible_tip",
    )?;
    validate_privacy_summary_tip(
        previous_seven_day_summary.freshness.as_ref(),
        visible_tip,
        "transaction_component_summary.freshness.chain_view.chain_epoch.visible_tip",
    )?;

    let transparent = value_pool_zat(&value_pool_summary.pools, "transparent");
    let sprout = value_pool_zat(&value_pool_summary.pools, "sprout");
    let sapling = value_pool_zat(&value_pool_summary.pools, "sapling");
    let orchard = value_pool_zat(&value_pool_summary.pools, "orchard");
    let ironwood = value_pool_zat(&value_pool_summary.pools, "ironwood");
    let has_unknown_pool = has_unknown_nonzero_value_pool(&value_pool_summary.pools);
    let shielded = if has_unknown_pool {
        None
    } else {
        total_value_pools_zat([sprout, sapling, orchard, ironwood])?
    };
    let chain_supply = complete_chain_supply_zat(&value_pool_summary.pools)?;
    let all_history_totals =
        all_history_summary
            .totals
            .as_ref()
            .ok_or(CipherscanRestError::MissingUpstreamField(
                "transaction_component_summary.totals",
            ))?;
    let thirty_day_totals =
        thirty_day_summary
            .totals
            .as_ref()
            .ok_or(CipherscanRestError::MissingUpstreamField(
                "transaction_component_summary.totals",
            ))?;
    let all_history_complete = transaction_component_coverage_complete(all_history_summary);
    let thirty_day_complete = transaction_component_coverage_complete(thirty_day_summary);
    let daily_coverage_complete = transaction_component_coverage_complete(daily_summary);
    let all_predicates_complete =
        all_history_complete && all_history_totals.transaction_predicate_unavailable_count == 0;
    let thirty_day_predicates_complete =
        thirty_day_complete && thirty_day_totals.transaction_predicate_unavailable_count == 0;
    let daily_predicates_complete = daily_coverage_complete
        && daily_summary
            .totals
            .as_ref()
            .is_some_and(|totals| totals.transaction_predicate_unavailable_count == 0);
    let all_time_total = all_history_complete.then_some(all_history_totals.transaction_count);
    let all_time_shielded = all_predicates_complete
        .then_some(all_history_totals.sapling_orchard_or_ironwood_transaction_count);
    let all_time_transparent = all_predicates_complete.then_some(
        all_history_totals.non_coinbase_without_sapling_orchard_or_ironwood_transaction_count,
    );
    let all_time_coinbase =
        all_predicates_complete.then_some(all_history_totals.coinbase_transaction_count);
    let all_time_mixed = all_predicates_complete.then_some(
        all_history_totals
            .non_coinbase_sapling_orchard_or_ironwood_with_transparent_inputs_and_outputs_transaction_count,
    );
    let all_time_fully_shielded = all_predicates_complete
        .then_some(
            all_history_totals
                .non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count,
        );
    if all_predicates_complete {
        validate_privacy_component_totals(all_history_totals)?;
    }
    let current_shielded_zat = shielded.and_then(|value_zat| u64::try_from(value_zat).ok());
    let current_chain_supply_zat = chain_supply.and_then(|value_zat| u64::try_from(value_zat).ok());
    let privacy_score = privacy_score(
        current_shielded_zat,
        current_chain_supply_zat,
        all_time_fully_shielded,
        all_time_shielded,
        all_time_total,
    );
    let adoption_trend =
        privacy_adoption_trend(recent_seven_day_summary, previous_seven_day_summary);
    let average_shielded_per_day = thirty_day_predicates_complete.then(|| {
        privacy_average_per_day(thirty_day_totals.sapling_orchard_or_ironwood_transaction_count)
    });
    let pool_history_complete = value_pool_history_is_historically_complete(value_pool_history)?;
    let current_day_start = calendar_date_start_unix_seconds(generated_at.date());
    let daily_start_time = current_day_start.saturating_sub(
        (PRIVACY_STATS_TREND_DAYS.saturating_sub(1)).saturating_mul(UNIX_SECONDS_PER_DAY),
    );
    let daily_pool_days_complete = pool_history_complete
        && (0..PRIVACY_STATS_TREND_POINT_LIMIT).all(|offset| {
            let day_start = daily_start_time.saturating_add(
                i64::try_from(offset)
                    .unwrap_or(i64::MAX)
                    .saturating_mul(UNIX_SECONDS_PER_DAY),
            );
            value_pool_history
                .points
                .iter()
                .any(|point| point.day_start_unix_seconds == day_start)
        });
    let daily = privacy_stats_daily_trends(
        daily_summary,
        value_pool_history,
        daily_coverage_complete,
        pool_history_complete,
        daily_start_time,
        all_time_total,
        all_time_shielded,
        all_time_fully_shielded,
    )?;
    let mut unavailable = Vec::new();
    if !all_history_complete {
        unavailable.push(
            "Transaction-component history is not complete through the visible tip, so all-time transaction metrics are null.",
        );
    }
    if !thirty_day_complete {
        unavailable.push(
            "The exact rolling 30-day transaction summary is incomplete, so avgShieldedPerDay is null.",
        );
    }
    if all_history_complete && !all_predicates_complete {
        unavailable.push(
            "Native transaction predicates are unavailable for one or more transactions, so predicate-derived all-time metrics are null.",
        );
    }
    if thirty_day_complete && !thirty_day_predicates_complete {
        unavailable.push(
            "Native transaction predicates are unavailable in the rolling 30-day range, so avgShieldedPerDay is null.",
        );
    }
    if !transaction_component_coverage_complete(recent_seven_day_summary)
        || !transaction_component_coverage_complete(previous_seven_day_summary)
    {
        unavailable.push(
            "One or more exact rolling 7-day transaction summaries are incomplete, so adoptionTrend is null.",
        );
    }
    if !daily_predicates_complete || daily_summary.days.iter().any(|day| day.totals.is_none()) {
        unavailable.push(
            "One or more UTC daily transaction buckets are incomplete or have unavailable predicates, so affected daily transaction metrics are null.",
        );
    }
    if !pool_history_complete {
        unavailable.push(
            "Cumulative value-pool history is still backfilling, so daily pool sizes are null.",
        );
    } else if !daily_pool_days_complete {
        unavailable.push(
            "One or more UTC daily value-pool snapshots are unavailable, so affected daily pool sizes and scores are null.",
        );
    }
    if !value_pools_are_complete(&value_pool_summary.pools) {
        unavailable.push(VALUE_POOL_TOTALS_UNAVAILABLE);
    }
    if has_unknown_pool {
        unavailable.push(UNKNOWN_VALUE_POOL_SEMANTICS_UNAVAILABLE);
    }

    Ok(json!({
        "totals": {
            "blocks": visible_tip.height,
            "shieldedTx": all_time_shielded,
            "transparentTx": all_time_transparent,
            "coinbaseTx": all_time_coinbase,
            "totalTx": all_time_total,
            "mixedTx": all_time_mixed,
            "fullyShieldedTx": all_time_fully_shielded,
        },
        "shieldedPool": {
            "currentSize": shielded.map(zec_from_zatoshis),
            "sprout": sprout.map(zec_from_zatoshis),
            "sapling": sapling.map(zec_from_zatoshis),
            "orchard": orchard.map(zec_from_zatoshis),
            "ironwood": ironwood.map(zec_from_zatoshis),
            "transparent": transparent.map(zec_from_zatoshis),
            "chainSupply": chain_supply.map(zec_from_zatoshis),
        },
        "metrics": {
            "shieldedPercentage": match (all_time_shielded, all_time_total) {
                (Some(shielded), Some(total)) => json!(privacy_percentage(shielded, total)),
                _ => Value::Null,
            },
            "privacyScore": privacy_score,
            "avgShieldedPerDay": average_shielded_per_day,
            "adoptionTrend": adoption_trend,
        },
        "trends": {
            "daily": daily,
        },
        "lastUpdated": rfc3339_timestamp(generated_at),
        "lastBlockScanned": visible_tip.height,
        "degraded": !unavailable.is_empty(),
        "source": CIPHERSCAN_ADAPTER_SOURCE,
        "unavailable": unavailable,
    }))
}

fn validate_privacy_summary_tip(
    freshness: Option<&explorer::ExplorerFreshness>,
    expected_tip: &wallet::BlockTip,
    field: &'static str,
) -> Result<(), CipherscanRestError> {
    let tip =
        explorer_visible_tip(freshness).ok_or(CipherscanRestError::MissingUpstreamField(field))?;
    if tip != expected_tip {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "privacy_stats.visible_tip",
        ));
    }
    Ok(())
}

fn validate_privacy_component_totals(
    totals: &explorer::TransactionComponentTotals,
) -> Result<(), CipherscanRestError> {
    if [
        totals.sapling_orchard_or_ironwood_transaction_count,
        totals
            .non_coinbase_without_sapling_orchard_or_ironwood_transaction_count,
        totals.coinbase_transaction_count,
        totals
            .non_coinbase_sapling_orchard_or_ironwood_with_transparent_inputs_and_outputs_transaction_count,
        totals
            .non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count,
    ]
    .into_iter()
    .any(|count| count > totals.transaction_count)
        || totals
            .non_coinbase_sapling_orchard_or_ironwood_with_transparent_inputs_and_outputs_transaction_count
            > totals.sapling_orchard_or_ironwood_transaction_count
        || totals
            .non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count
            > totals.sapling_orchard_or_ironwood_transaction_count
    {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "transaction_component_summary.totals",
        ));
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "The daily compatibility shape combines one native daily summary, one pool-history page, and the all-time ratios required by the public score."
)]
fn privacy_stats_daily_trends(
    component_summary: &TransactionComponentSummaryResponse,
    value_pool_history: &ValuePoolBalanceHistoryResponse,
    component_complete: bool,
    pool_history_complete: bool,
    daily_start_time: i64,
    all_time_total: Option<u64>,
    all_time_shielded: Option<u64>,
    all_time_fully_shielded: Option<u64>,
) -> Result<Vec<Value>, CipherscanRestError> {
    let mut component_days = BTreeMap::new();
    for day in &component_summary.days {
        if component_days
            .insert(day.day_start_unix_seconds, day)
            .is_some()
        {
            return Err(CipherscanRestError::InvalidUpstreamField(
                "transaction_component_summary.days.day_start_unix_seconds",
            ));
        }
    }
    let mut pool_days = BTreeMap::new();
    for point in &value_pool_history.points {
        validate_history_value_pools(&point.pools)?;
        if pool_days
            .insert(point.day_start_unix_seconds, point)
            .is_some()
        {
            return Err(CipherscanRestError::InvalidUpstreamField(
                "value_pool_balance_history.points.day_start_unix_seconds",
            ));
        }
    }

    let mut daily = Vec::with_capacity(PRIVACY_STATS_TREND_POINT_LIMIT);
    for offset in (0..PRIVACY_STATS_TREND_POINT_LIMIT).rev() {
        let day_start = daily_start_time.saturating_add(
            i64::try_from(offset)
                .unwrap_or(i64::MAX)
                .saturating_mul(UNIX_SECONDS_PER_DAY),
        );
        let (shielded, transparent, shielded_percentage) = if !component_complete {
            (None, None, None)
        } else if let Some(day) = component_days.get(&day_start) {
            match day.totals.as_ref() {
                Some(totals) if totals.transaction_predicate_unavailable_count == 0 => (
                    Some(totals.sapling_orchard_or_ironwood_transaction_count),
                    Some(totals.non_coinbase_without_sapling_orchard_or_ironwood_transaction_count),
                    Some(privacy_percentage(
                        totals.sapling_orchard_or_ironwood_transaction_count,
                        totals
                            .sapling_orchard_or_ironwood_transaction_count
                            .saturating_add(
                            totals
                                .non_coinbase_without_sapling_orchard_or_ironwood_transaction_count,
                        ),
                    )),
                ),
                Some(_) | None => (None, None, None),
            }
        } else {
            (Some(0), Some(0), Some(0.0))
        };
        let pool_point = pool_history_complete
            .then(|| pool_days.get(&day_start).copied())
            .flatten();
        let pool_size = pool_point
            .map(privacy_history_shielded_pool_zec)
            .transpose()?
            .flatten();
        let chain_supply = pool_point
            .map(privacy_history_chain_supply_zat)
            .transpose()?
            .flatten();
        let privacy_score = privacy_score(
            pool_point
                .map(privacy_history_shielded_pool_zat)
                .transpose()?
                .flatten(),
            chain_supply,
            all_time_fully_shielded,
            all_time_shielded,
            all_time_total,
        );
        daily.push(json!({
            "date": cipherscan_timestamp_from_unix_seconds(day_start),
            "shielded": shielded,
            "transparent": transparent,
            "shieldedPercentage": shielded_percentage,
            "poolSize": pool_size,
            "privacyScore": privacy_score,
        }));
    }
    Ok(daily)
}

fn privacy_history_shielded_pool_zec(
    point: &explorer::ValuePoolBalanceHistoryPoint,
) -> Result<Option<f64>, CipherscanRestError> {
    privacy_history_shielded_pool_zat(point).map(|total| total.map(zec_from_unsigned_zatoshis))
}

fn privacy_history_shielded_pool_zat(
    point: &explorer::ValuePoolBalanceHistoryPoint,
) -> Result<Option<u64>, CipherscanRestError> {
    if !privacy_history_pool_values_are_known(point)? {
        return Ok(None);
    }
    total_optional_u64(
        ["sprout", "sapling", "orchard", "ironwood"]
            .into_iter()
            .map(|id| history_pool_value(&point.pools, id)),
    )
}

fn privacy_history_chain_supply_zat(
    point: &explorer::ValuePoolBalanceHistoryPoint,
) -> Result<Option<u64>, CipherscanRestError> {
    if !privacy_history_pool_values_are_known(point)? {
        return Ok(None);
    }
    total_optional_u64(
        CIPHERSCAN_VALUE_POOL_IDS
            .into_iter()
            .map(|id| history_pool_value(&point.pools, id)),
    )
}

fn privacy_history_pool_values_are_known(
    point: &explorer::ValuePoolBalanceHistoryPoint,
) -> Result<bool, CipherscanRestError> {
    validate_history_value_pools(&point.pools)?;
    for pool in &point.pools {
        if CIPHERSCAN_VALUE_POOL_IDS.contains(&pool.id.as_str()) {
            continue;
        }
        if pool.value_zat.is_some_and(|value_zat| value_zat != 0)
            || (pool.monitored && pool.value_zat.is_none())
        {
            return Ok(false);
        }
    }
    Ok(true)
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "Cipherscan's compatibility score is represented as a JSON integer after floating-point percentage arithmetic."
)]
fn privacy_score(
    shielded_pool_zat: Option<u64>,
    chain_supply_zat: Option<u64>,
    fully_shielded: Option<u64>,
    shielded: Option<u64>,
    total: Option<u64>,
) -> Option<u64> {
    let shielded_pool_zat = shielded_pool_zat?;
    let chain_supply_zat = chain_supply_zat?;
    let fully_shielded = fully_shielded?;
    let shielded = shielded?;
    let total = total?;
    let supply_percentage = if chain_supply_zat == 0 {
        0.0
    } else {
        shielded_pool_zat as f64 / chain_supply_zat as f64 * 100.0
    };
    let fully_shielded_percentage = if shielded == 0 {
        0.0
    } else {
        fully_shielded as f64 / shielded as f64 * 100.0
    };
    let adoption_percentage = if total == 0 {
        0.0
    } else {
        shielded as f64 / total as f64 * 100.0
    };
    let supply_score = (supply_percentage * 0.4).min(40.0);
    let fully_shielded_score = (fully_shielded_percentage * 0.3).min(30.0);
    let adoption_score = (adoption_percentage * 0.3).min(30.0);
    Some(
        (supply_score + fully_shielded_score + adoption_score)
            .min(100.0)
            .round() as u64,
    )
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Cipherscan's adoption trend compares native u64 counts using the production percentage formula."
)]
fn privacy_adoption_trend(
    recent_summary: &TransactionComponentSummaryResponse,
    previous_summary: &TransactionComponentSummaryResponse,
) -> Option<&'static str> {
    if !transaction_component_coverage_complete(recent_summary)
        || !transaction_component_coverage_complete(previous_summary)
    {
        return None;
    }
    let recent_totals = recent_summary.totals.as_ref()?;
    let previous_totals = previous_summary.totals.as_ref()?;
    if recent_totals.transaction_predicate_unavailable_count != 0
        || previous_totals.transaction_predicate_unavailable_count != 0
    {
        return None;
    }
    let recent = recent_totals.sapling_orchard_or_ironwood_transaction_count;
    let previous = previous_totals.sapling_orchard_or_ironwood_transaction_count;
    if previous == 0 {
        return Some("stable");
    }
    let change_percentage = (recent as f64 - previous as f64) / previous as f64 * 100.0;
    if change_percentage > 10.0 {
        Some("growing")
    } else if change_percentage < -10.0 {
        Some("declining")
    } else {
        Some("stable")
    }
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Cipherscan's average transaction count is represented as a JSON floating-point number."
)]
fn privacy_average_per_day(transaction_count: u64) -> f64 {
    (transaction_count as f64 / PRIVACY_STATS_TREND_DAYS as f64 * 100.0).round() / 100.0
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Cipherscan's compatibility percentages are represented as JSON floating-point numbers rounded to the observed six-decimal wire scale."
)]
fn privacy_percentage(numerator: u64, denominator: u64) -> f64 {
    let percentage = zatoshi_percentage(numerator, denominator);
    (percentage * 1_000_000.0).round() / 1_000_000.0
}

async fn privacy_risks(Query(query): Query<PageQuery>) -> Response {
    json_response(
        StatusCode::OK,
        json!({
            "success": true,
            "degraded": true,
            "transactions": [],
            "stats": {
                "total": 0,
                "highRisk": 0,
                "mediumRisk": 0,
                "lowRisk": 0,
                "avgScore": Value::Null,
                "period": query.period.unwrap_or_else(|| String::from("unknown")),
            },
            "pagination": {
                "hasMore": false,
                "total": 0,
                "limit": query.limit.unwrap_or(DEFAULT_LIMIT),
                "offset": query.offset.unwrap_or(0),
            },
            "unavailable": [
                "Cipherscan privacy-risk scoring is sidecar/product logic and is not served by Zinder core."
            ],
        }),
    )
}

async fn privacy_linkage_edges(Query(query): Query<PrivacyListQuery>) -> Response {
    let limit = query_limit_with_max(query.limit, DEFAULT_LIMIT, MAX_LIMIT);
    let offset = query.offset.unwrap_or(0);
    json_response(StatusCode::OK, privacy_linkage_edges_json(limit, offset))
}

async fn privacy_batch_risks(Query(query): Query<PrivacyListQuery>) -> Response {
    let limit = query_limit_with_max(query.limit, DEFAULT_LIMIT, 50);
    let period = query.period.unwrap_or_else(|| String::from("30d"));
    json_response(StatusCode::OK, privacy_batch_risks_json(limit, &period))
}

async fn privacy_clusters(Query(query): Query<PrivacyListQuery>) -> Response {
    let limit = query_limit_with_max(query.limit, DEFAULT_LIMIT, 50);
    json_response(StatusCode::OK, privacy_clusters_json(limit))
}

async fn privacy_graph(Path(transaction_id): Path<String>) -> Response {
    if !is_rpc_transaction_id(&transaction_id) {
        return invalid_txid_path_parameters_response();
    }

    json_response(
        StatusCode::OK,
        privacy_graph_json(&transaction_id.to_ascii_lowercase()),
    )
}

async fn privacy_shield_batch(Path(transaction_id): Path<String>) -> Response {
    if !is_rpc_transaction_id(&transaction_id) {
        return invalid_txid_path_parameters_response();
    }

    json_response(
        StatusCode::NOT_FOUND,
        json!({
            "success": false,
            "error": "Shield transaction not found",
            "txid": transaction_id.to_ascii_lowercase(),
            "degraded": true,
            "unavailable": [
                "Batch-pattern detection requires Cipherscan shielded-flow sidecar analytics."
            ],
        }),
    )
}

async fn privacy_patterns(Query(query): Query<PrivacyListQuery>) -> Response {
    let limit = query_limit_with_max(query.limit, DEFAULT_LIMIT, 100);
    let offset = query.offset.unwrap_or(0);
    json_response(StatusCode::OK, privacy_patterns_json(limit, offset))
}

async fn privacy_common_amounts(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<PrivacyCommonAmountsQuery>,
) -> Result<Response, CipherscanRestError> {
    let limit = common_amounts_limit(query.limit.as_deref());
    let period = common_amounts_period(query.period.as_deref());
    let chain = query
        .chain
        .filter(|chain| !chain.is_empty())
        .map(|chain| chain.to_ascii_lowercase());
    if let Some(chain) = chain.as_deref() {
        return Ok(json_response(
            StatusCode::OK,
            privacy_common_amounts_json(&period.echoed, Some(chain)),
        ));
    }

    let cache_key = CommonAmountsCacheKey {
        period: period.echoed.clone(),
        limit,
    };
    if let Some(body) = adapter.cached_common_amounts_response(&cache_key).await {
        return Ok(json_response(StatusCode::OK, body));
    }
    require_blend_check_capabilities(&adapter).await?;

    let (start_time_unix_seconds, end_time_unix_seconds) =
        common_amounts_range(&period, OffsetDateTime::now_utc().unix_timestamp());
    let rounded = adapter
        .fetch_value_pool_flow_rounded_amount_summary(ValuePoolFlowRoundedAmountSummaryRequest {
            start_time_unix_seconds,
            end_time_unix_seconds,
            pools: Vec::new(),
            minimum_raw_amount_zat: COMMON_AMOUNTS_MINIMUM_ZAT,
            maximum_raw_amount_zat: None,
            rounding_quantum_zat: COMMON_AMOUNTS_ROUNDING_QUANTUM_ZAT,
            minimum_event_count: 0,
            max_rows: limit,
        })
        .await?;
    let threshold = adapter
        .fetch_value_pool_flow_amount_threshold_summary(
            ValuePoolFlowAmountThresholdSummaryRequest {
                start_time_unix_seconds,
                end_time_unix_seconds,
                pools: Vec::new(),
                minimum_amounts_zat: vec![COMMON_AMOUNTS_MINIMUM_ZAT],
            },
        )
        .await?;
    require_common_amounts_context(&rounded, &threshold)?;
    let body = common_amounts_json(&period.echoed, &rounded.rows, &threshold.thresholds)?;
    adapter
        .cache_common_amounts_response(cache_key, body.clone())
        .await;
    Ok(json_response(StatusCode::OK, body))
}

async fn privacy_recommended_swap_amounts(
    Query(query): Query<RecommendedSwapAmountsQuery>,
) -> Response {
    let chain = query.chain.filter(|chain| !chain.is_empty());
    let token = query.token.filter(|token| !token.is_empty());
    let (Some(chain), Some(token)) = (chain, token) else {
        return invalid_recommended_amounts_query_response();
    };

    json_response(
        StatusCode::OK,
        privacy_recommended_swap_amounts_json(
            &chain.to_ascii_lowercase(),
            &token.to_ascii_uppercase(),
        ),
    )
}

async fn blend_check(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<BlendAmountQuery>,
) -> Result<Response, CipherscanRestError> {
    let Some((amount, amount_zat)) = parse_blend_amount(query.amount.as_deref()) else {
        return Ok(blend_check_invalid_amount_response());
    };
    let cache_key = format!("blend:{amount_zat}");
    if let Some(body) = adapter.cached_blend_check_response(&cache_key).await {
        return Ok(json_response(StatusCode::OK, body));
    }
    require_blend_check_capabilities(&adapter).await?;

    let now = OffsetDateTime::now_utc().unix_timestamp();
    let mut expected_epoch_id = None;
    let periods =
        fetch_blend_period_counts(&adapter, amount_zat, now, &mut expected_epoch_id).await?;
    let count_30d = periods
        .get("30d")
        .and_then(|period| period.get("total"))
        .and_then(Value::as_u64)
        .ok_or(CipherscanRestError::InvalidUpstreamField(
            "blend_check.periods.30d.total",
        ))?;

    let (range_lower, range_upper) = cipherscan_nearby_raw_range(amount_zat);
    let rounded = fetch_blend_rounded_amounts(
        &adapter,
        BlendRoundedAmountRequest {
            start_time_unix_seconds: now.saturating_sub(30 * UNIX_SECONDS_PER_DAY),
            minimum_raw_amount_zat: range_lower.max(BLEND_ROUNDING_QUANTUM_ZAT),
            maximum_raw_amount_zat: range_upper.saturating_add(1),
            minimum_event_count: 3,
            max_rows: 20,
        },
        &mut expected_epoch_id,
    )
    .await?;
    let nearby_amounts = rounded
        .iter()
        .map(|row| row.rounded_amount_zat)
        .collect::<Vec<_>>();
    let nearby_counts = fetch_exact_blend_counts(
        &adapter,
        now.saturating_sub(30 * UNIX_SECONDS_PER_DAY),
        i64::MAX,
        &nearby_amounts,
        &mut expected_epoch_id,
    )
    .await?;
    let nearby_popular = nearby_popular_amounts(
        amount_zat,
        nearby_amounts
            .into_iter()
            .map(|candidate_amount_zat| NearbyCandidateCount {
                amount_zat: candidate_amount_zat,
                count_30d: nearby_counts
                    .get(&candidate_amount_zat)
                    .map_or(0, |counts| counts.total),
            }),
    );
    let blend_score = compute_blend_score(count_30d);
    let body = json!({
        "amount": amount,
        "amountZat": amount_zat,
        "periods": periods,
        "blendScore": blend_score,
        "blendLabel": blend_label(blend_score),
        "nearbyPopular": nearby_popular,
    });
    adapter
        .cache_blend_check_response(cache_key, body.clone())
        .await;
    Ok(json_response(StatusCode::OK, body))
}

async fn blend_check_split(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<BlendAmountQuery>,
) -> Result<Response, CipherscanRestError> {
    let Some((amount, amount_zat)) = parse_blend_amount(query.amount.as_deref()) else {
        return Ok(blend_check_invalid_amount_response());
    };
    let cache_key = format!("split:{amount_zat}");
    if let Some(body) = adapter.cached_blend_check_response(&cache_key).await {
        return Ok(json_response(StatusCode::OK, body));
    }
    require_blend_check_capabilities(&adapter).await?;

    let start_time_unix_seconds = OffsetDateTime::now_utc()
        .unix_timestamp()
        .saturating_sub(30 * UNIX_SECONDS_PER_DAY);
    let mut expected_epoch_id = None;
    let rounded = fetch_blend_rounded_amounts(
        &adapter,
        BlendRoundedAmountRequest {
            start_time_unix_seconds,
            minimum_raw_amount_zat: 100_000,
            maximum_raw_amount_zat: amount_zat.saturating_add(1),
            minimum_event_count: 10,
            max_rows: 50,
        },
        &mut expected_epoch_id,
    )
    .await?;
    let mut exact_amounts = rounded
        .iter()
        .map(|row| row.rounded_amount_zat)
        .collect::<Vec<_>>();
    exact_amounts.push(amount_zat);
    let exact_counts = fetch_exact_blend_counts(
        &adapter,
        start_time_unix_seconds,
        i64::MAX,
        &exact_amounts,
        &mut expected_epoch_id,
    )
    .await?;
    let original_count_30d = exact_counts
        .get(&amount_zat)
        .map_or(0, |counts| counts.total);
    let candidates = rounded
        .into_iter()
        .map(|row| SplitCandidateCount {
            amount_zat: row.rounded_amount_zat,
            count_30d: exact_counts
                .get(&row.rounded_amount_zat)
                .map_or(0, |counts| counts.total),
        })
        .collect::<Vec<_>>();
    let remainder_amounts = split_remainder_amounts(amount_zat, original_count_30d, &candidates);
    let remainder_counts = fetch_exact_blend_counts(
        &adapter,
        start_time_unix_seconds,
        i64::MAX,
        &remainder_amounts,
        &mut expected_epoch_id,
    )
    .await?;
    let plans = build_split_plans(
        amount_zat,
        original_count_30d,
        candidates,
        |remainder_amount_zat| {
            remainder_counts
                .get(&remainder_amount_zat)
                .map(|counts| counts.total)
        },
    );
    let body = json!({
        "amount": amount,
        "originalScore": compute_blend_score(original_count_30d),
        "plans": plans,
    });
    adapter
        .cache_blend_check_response(cache_key, body.clone())
        .await;
    Ok(json_response(StatusCode::OK, body))
}

async fn shielded_count(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<ShieldedCountQuery>,
) -> Result<Response, CipherscanRestError> {
    adapter
        .require_explorer_capability(EXPLORER_TRANSACTION_COMPONENT_SUMMARY_V2)
        .await?;
    let Some(since) = query.since else {
        return Ok(shielded_stats_missing_since_response());
    };
    let Some(since_date) = parse_iso8601_calendar_date(&since) else {
        return Ok(json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({
                "success": false,
                "error": "Invalid date format. Use ISO format: YYYY-MM-DD",
            }),
        ));
    };

    let is_detailed = query.detailed.as_deref() == Some("true");
    let queried_at = OffsetDateTime::now_utc();
    let summary = adapter
        .fetch_transaction_component_summary(
            calendar_date_start_unix_seconds(since_date),
            queried_at
                .unix_timestamp()
                .saturating_add(COMPONENT_SUMMARY_FUTURE_TIME_MARGIN_SECONDS),
            false,
        )
        .await?;
    Ok(json_response(
        StatusCode::OK,
        shielded_count_json(&since, is_detailed, &summary, queried_at),
    ))
}

async fn shielded_daily(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<ShieldedDailyQuery>,
) -> Result<Response, CipherscanRestError> {
    adapter
        .require_explorer_capability(EXPLORER_TRANSACTION_COMPONENT_SUMMARY_V2)
        .await?;
    let Some(since) = query.since else {
        return Ok(shielded_stats_missing_since_response());
    };
    let Some(since_date) = parse_iso8601_calendar_date(&since) else {
        return Ok(json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({
                "success": false,
                "error": "Invalid start date format. Use ISO format: YYYY-MM-DD",
            }),
        ));
    };
    let queried_at = OffsetDateTime::now_utc();
    let (until, end_time_unix_seconds) = match query.until {
        Some(until) if parse_iso8601_calendar_date(&until).is_some() => {
            let until_date = parse_iso8601_calendar_date(&until).ok_or(
                CipherscanRestError::InvalidUpstreamField("shielded_daily.until"),
            )?;
            (until, calendar_date_start_unix_seconds(until_date))
        }
        Some(_) => {
            return Ok(json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({
                    "success": false,
                    "error": "Invalid end date format. Use ISO format: YYYY-MM-DD",
                }),
            ));
        }
        None => (queried_at.date().to_string(), queried_at.unix_timestamp()),
    };
    let summary = adapter
        .fetch_transaction_component_summary(
            calendar_date_start_unix_seconds(since_date),
            end_time_unix_seconds,
            false,
        )
        .await?;

    Ok(json_response(
        StatusCode::OK,
        shielded_daily_json(&since, &until, &summary),
    ))
}

async fn anonymity_set(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<PageQuery>,
) -> Result<Response, CipherscanRestError> {
    let period = flow_analytics_period(query.period.as_deref());
    adapter
        .require_explorer_capability(EXPLORER_VALUE_POOL_FLOW_AMOUNT_THRESHOLD_SUMMARY_V1)
        .await?;
    if let Some(body) = adapter.cached_anonymity_set_response(&period.echoed).await {
        return Ok(json_response(StatusCode::OK, body));
    }

    let requested_at = OffsetDateTime::now_utc();
    let (start_time_unix_seconds, end_time_unix_seconds) =
        flow_analytics_range(period.days, requested_at.unix_timestamp());
    let summary = adapter
        .fetch_value_pool_flow_amount_threshold_summary(
            ValuePoolFlowAmountThresholdSummaryRequest {
                start_time_unix_seconds,
                end_time_unix_seconds,
                // The legacy route aggregates every shielded-flow attribution.
                pools: Vec::new(),
                minimum_amounts_zat: CIPHERSCAN_ANONYMITY_SET_THRESHOLDS_ZAT.to_vec(),
            },
        )
        .await?;
    if summary.freshness.is_none() {
        return Err(CipherscanRestError::MissingUpstreamField(
            "value_pool_flow_amount_threshold_summary.freshness",
        ));
    }
    let coverage = summary
        .coverage
        .as_ref()
        .ok_or(CipherscanRestError::MissingUpstreamField(
            "value_pool_flow_amount_threshold_summary.coverage",
        ))?;
    require_complete_flow_analytics_coverage(coverage.requested_range_complete)?;

    let thresholds = summary
        .thresholds
        .iter()
        .map(|threshold| {
            (
                threshold.minimum_amount_zat,
                threshold.shield_event_count,
                threshold.deshield_event_count,
            )
        })
        .collect::<Vec<_>>();
    let body = anonymity_set_json(&period.echoed, &thresholds, requested_at)?;
    adapter
        .cache_anonymity_set_response(period.echoed, body.clone())
        .await;

    Ok(json_response(StatusCode::OK, body))
}

async fn shielding_distribution(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<PageQuery>,
) -> Result<Response, CipherscanRestError> {
    let period = flow_analytics_period(query.period.as_deref());
    adapter
        .require_explorer_capability(EXPLORER_VALUE_POOL_FLOW_AMOUNT_THRESHOLD_SUMMARY_V1)
        .await?;
    if let Some(body) = adapter
        .cached_shielding_distribution_response(&period.echoed)
        .await
    {
        return Ok(json_response(StatusCode::OK, body));
    }

    let requested_at = OffsetDateTime::now_utc();
    let (start_time_unix_seconds, end_time_unix_seconds) =
        flow_analytics_range(period.days, requested_at.unix_timestamp());
    let minimum_amounts_zat = CIPHERSCAN_SHIELDING_DISTRIBUTION_BUCKETS
        .iter()
        .map(|bucket| bucket.minimum_amount_zat)
        .collect();
    let summary = adapter
        .fetch_value_pool_flow_amount_threshold_summary(
            ValuePoolFlowAmountThresholdSummaryRequest {
                start_time_unix_seconds,
                end_time_unix_seconds,
                pools: Vec::new(),
                minimum_amounts_zat,
            },
        )
        .await?;
    if summary.freshness.is_none() {
        return Err(CipherscanRestError::MissingUpstreamField(
            "value_pool_flow_amount_threshold_summary.freshness",
        ));
    }
    let coverage = summary
        .coverage
        .as_ref()
        .ok_or(CipherscanRestError::MissingUpstreamField(
            "value_pool_flow_amount_threshold_summary.coverage",
        ))?;
    require_complete_flow_analytics_coverage(coverage.requested_range_complete)?;

    let body = shielding_distribution_json(&period.echoed, &summary.thresholds, requested_at)?;
    adapter
        .cache_shielding_distribution_response(period.echoed, body.clone())
        .await;
    Ok(json_response(StatusCode::OK, body))
}

async fn fetch_value_pool_stats_anchor(
    adapter: &CipherscanRestAdapter,
    value_pool_summary: &explorer::ValuePoolSummaryResponse,
) -> Result<(u64, u32, i64), CipherscanRestError> {
    let at_epoch_id = explorer_chain_epoch_id(value_pool_summary.freshness.as_ref()).ok_or(
        CipherscanRestError::MissingUpstreamField("freshness.chain_view.chain_epoch"),
    )?;
    let source_tip = verified_value_pool_source_tip(value_pool_summary)?;
    let block_header = adapter
        .wallet_client()
        .block_header_by_selector(BlockSelectorRequest {
            selector: Some(wallet::BlockSelector {
                selector: Some(wallet::block_selector::Selector::Height(source_tip.height)),
            }),
            at_epoch_id: Some(at_epoch_id),
        })
        .await?
        .into_inner()
        .block_header
        .ok_or(CipherscanRestError::MissingUpstreamField("block_header"))?;
    validate_block_header_tip(&block_header, source_tip)?;
    Ok((at_epoch_id, source_tip.height, block_header.block_time))
}

async fn network_stats(
    State(adapter): State<CipherscanRestAdapter>,
) -> Result<Response, CipherscanRestError> {
    let value_pool_summary = adapter
        .explorer_client()
        .value_pool_summary(ValuePoolSummaryRequest {})
        .await?
        .into_inner();
    let (at_epoch_id, height, latest_block_time) =
        fetch_value_pool_stats_anchor(&adapter, &value_pool_summary).await?;
    let chain_subsidy = derive_chain_subsidy_summary(adapter.network, height)?;
    let activity = adapter
        .fetch_network_activity_window(
            height,
            at_epoch_id,
            OffsetDateTime::now_utc()
                .unix_timestamp()
                .saturating_sub(86_400),
        )
        .await?;
    Ok(json_response(
        StatusCode::OK,
        json!({
            "success": true,
            "mining": {
                "networkHashrate": cipherscan_hashrate_string(activity.network_hashrate_raw),
                "networkHashrateRaw": activity.network_hashrate_raw,
                "difficulty": activity.latest_difficulty,
                "avgBlockTime": activity.average_block_time_seconds,
                "blocks24h": activity.block_count,
                "blockReward": chain_subsidy.current_subsidy_zec,
                "minerReward": chain_subsidy.current_miner_subsidy_zec,
                "fundingStreams": chain_subsidy.current_funding_streams_zec,
                "lockbox": chain_subsidy.current_lockbox_zec,
                "dailyRevenue": f64::from(activity.block_count) * chain_subsidy.current_subsidy_zec,
                "dailyMinerRevenue": f64::from(activity.block_count) * chain_subsidy.current_miner_subsidy_zec,
            },
            "network": {
                "peers": 0,
                "height": height,
                "protocolVersion": 0,
                "subversion": "Zinder",
            },
            "blockchain": {
                "height": height,
                "latestBlockTime": latest_block_time,
                "syncProgress": 1.0,
                "sizeBytes": 0,
                "sizeGB": 0.0,
                "tx24h": activity.transaction_count,
            },
            "supply": network_supply_json(
                encode_zinder_native_chain_name(chain_subsidy.network),
                cipherscan_upgrade_name(chain_subsidy.active_upgrade),
                &value_pool_summary.pools,
            )?,
            "degraded": true,
            "unavailable": [
                "Peer inventory and chain-size facts require separate operational or native surfaces."
            ],
        }),
    ))
}

async fn network_health(
    State(adapter): State<CipherscanRestAdapter>,
) -> Result<Response, CipherscanRestError> {
    let explorer = adapter.fetch_explorer_server_info().await?;
    let wallet = adapter
        .wallet_client()
        .server_info(wallet::ServerInfoRequest {})
        .await?
        .into_inner();
    Ok(json_response(
        StatusCode::OK,
        network_health_json(&explorer, &wallet),
    ))
}

async fn chain_halving(
    State(adapter): State<CipherscanRestAdapter>,
) -> Result<Response, CipherscanRestError> {
    let current_height = adapter.fetch_tip_height().await?;
    let chain_subsidy = derive_chain_subsidy_summary(adapter.network, current_height)?;
    Ok(json_response(StatusCode::OK, halving_json(&chain_subsidy)))
}

async fn chain_emission(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<PageQuery>,
) -> Result<Response, CipherscanRestError> {
    adapter
        .require_explorer_capability(EXPLORER_VALUE_POOL_BALANCE_HISTORY_V1)
        .await?;
    let period = query
        .period
        .as_deref()
        .filter(|period| !period.is_empty())
        .unwrap_or("1y");
    let mut summary_client = adapter.explorer_client();
    let mut history_client = adapter.explorer_client();
    let (value_pool_summary, value_pool_history) = tokio::try_join!(
        summary_client.value_pool_summary(ValuePoolSummaryRequest {}),
        history_client.value_pool_balance_history(ValuePoolBalanceHistoryRequest {
            page_size: value_pool_history_page_size(period),
            cursor: Vec::new(),
        }),
    )?;
    let value_pool_summary = value_pool_summary.into_inner();
    validate_value_pools(&value_pool_summary.pools)?;
    let source_tip = verified_value_pool_source_tip(&value_pool_summary)?;
    let current_chain_supply_zat = complete_chain_supply_zat(&value_pool_summary.pools)?.ok_or(
        CipherscanRestError::MissingUpstreamField("value_pool_summary.pools.chain_value_zat"),
    )?;
    let chain_subsidy = derive_chain_subsidy_summary(adapter.network, source_tip.height)?;
    let chain_supply = chain_supply_summary_from_zats(current_chain_supply_zat)?;
    Ok(json_response(
        StatusCode::OK,
        emission_json(
            &chain_subsidy,
            &chain_supply,
            source_tip,
            &value_pool_history.into_inner(),
            period,
        )?,
    ))
}

async fn supply(
    State(adapter): State<CipherscanRestAdapter>,
) -> Result<Response, CipherscanRestError> {
    let response = adapter
        .explorer_client()
        .value_pool_summary(ValuePoolSummaryRequest {})
        .await?
        .into_inner();
    validate_value_pools(&response.pools)?;
    let pools: Vec<Value> = response.pools.iter().map(value_pool_row).collect();
    Ok(json_response(StatusCode::OK, json!(pools)))
}

async fn circulating_supply(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<CirculatingSupplyQuery>,
) -> Result<Response, CipherscanRestError> {
    let current_height = adapter.fetch_tip_height().await?;
    let chain_subsidy = derive_chain_subsidy_summary(adapter.network, current_height)?;
    let chain_supply = derive_chain_supply_summary(&chain_subsidy)?;

    if query.format.as_deref() == Some("json") {
        return Ok(json_response(
            StatusCode::OK,
            json!({
                "circulatingSupply": chain_supply.chain_supply_zec,
                "circulatingSupplyZat": chain_supply.chain_supply_zats.to_string(),
                "maxSupply": MAX_SUPPLY_ZEC,
                "unit": "ZEC",
                "source": CIPHERSCAN_ADAPTER_SOURCE,
                "degraded": false,
            }),
        ));
    }

    Ok(text_response(
        StatusCode::OK,
        chain_supply.chain_supply_zec.to_string(),
    ))
}

async fn transparent_supply_breakdown(
    State(adapter): State<CipherscanRestAdapter>,
) -> Result<Response, CipherscanRestError> {
    let mut ranking_client = adapter.explorer_client();
    let mut value_pool_client = adapter.explorer_client();
    let (ranking, value_pools) = tokio::try_join!(
        ranking_client.transparent_address_ranking(TransparentAddressRankingRequest {
            limit: 1,
            offset: 0,
        }),
        value_pool_client.value_pool_summary(ValuePoolSummaryRequest {}),
    )?;
    Ok(json_response(
        StatusCode::OK,
        transparent_supply_breakdown_json(
            adapter.network,
            &ranking.into_inner(),
            &value_pools.into_inner(),
        )?,
    ))
}

fn transparent_supply_breakdown_json(
    network: Network,
    ranking: &TransparentAddressRankingResponse,
    value_pools: &explorer::ValuePoolSummaryResponse,
) -> Result<Value, CipherscanRestError> {
    let p2pkh = transparent_script_type_summary(ranking, explorer::TransparentScriptType::P2pkh)?;
    let p2sh = transparent_script_type_summary(ranking, explorer::TransparentScriptType::P2sh)?;
    let classified_address_count = p2pkh
        .positive_address_count
        .checked_add(p2sh.positive_address_count)
        .ok_or(CipherscanRestError::InvalidUpstreamField(
            "transparent_address_ranking.script_type_summaries",
        ))?;
    let classified_balance_zat = p2pkh
        .total_positive_balance_zat
        .checked_add(p2sh.total_positive_balance_zat)
        .ok_or(CipherscanRestError::InvalidUpstreamField(
            "transparent_address_ranking.script_type_summaries",
        ))?;
    if classified_address_count != ranking.positive_address_count
        || classified_balance_zat != ranking.total_positive_balance_zat
    {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "transparent_address_ranking.script_type_summaries",
        ));
    }

    let value_pool_source_tip = value_pool_source_tip(value_pools)?;
    let transparent_total_zat = transparent_value_pool_zat(value_pools)?;
    let unattributed_transparent_zat = transparent_total_zat
        .checked_sub(classified_balance_zat)
        .ok_or(CipherscanRestError::InvalidUpstreamField(
            "transparent_address_ranking.total_positive_balance_zat",
        ))?;
    let ranking_tip =
        ranking_visible_tip(ranking).ok_or(CipherscanRestError::MissingUpstreamField(
            "transparent_address_ranking.freshness.chain_view.chain_epoch.visible_tip",
        ))?;
    if ranking_tip != value_pool_source_tip {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "value_pool_summary.source_tip",
        ));
    }
    let ranking_height = ranking_tip.height;
    let mut unavailable = vec!["Address category labels require a Cipherscan-owned label sidecar."];
    if !ranking_balance_coverage_complete(ranking) {
        unavailable.push("The native transparent address ranking has incomplete balance coverage.");
    }

    Ok(json!({
        "success": true,
        "cached": false,
        "timestamp": OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000,
        "transparentTotal": zec_from_unsigned_zatoshis(transparent_total_zat),
        "indexedStandardTotal": zec_from_unsigned_zatoshis(classified_balance_zat),
        "unattributedTransparent": zec_from_unsigned_zatoshis(unattributed_transparent_zat),
        "labeledTotal": 0.0,
        "labeledPercentage": 0.0,
        "addressTypes": [
            transparent_address_type_json(
                network,
                explorer::TransparentScriptType::P2pkh,
                p2pkh,
                transparent_total_zat,
            ),
            transparent_address_type_json(
                network,
                explorer::TransparentScriptType::P2sh,
                p2sh,
                transparent_total_zat,
            ),
        ],
        "categories": [{
            "category": "unlabeled",
            "addressCount": ranking.positive_address_count,
            "totalBalance": zec_from_unsigned_zatoshis(classified_balance_zat),
            "percentage": zatoshi_percentage(classified_balance_zat, transparent_total_zat),
        }],
        "coverage": {
            "rankingHeight": ranking_height,
            "valuePoolHeight": value_pool_source_tip.height,
            "balanceComplete": ranking_balance_coverage_complete(ranking),
        },
        "degraded": !unavailable.is_empty(),
        "unavailable": unavailable,
    }))
}

fn transparent_address_type_json(
    network: Network,
    script_type: explorer::TransparentScriptType,
    summary: &explorer::TransparentAddressScriptTypeSummary,
    transparent_total_zat: u64,
) -> Value {
    let (name, description) = match (network, script_type) {
        (Network::ZcashMainnet, explorer::TransparentScriptType::P2pkh) => {
            ("P2PKH", "Pay-to-Public-Key-Hash (t1...)")
        }
        (Network::ZcashMainnet, explorer::TransparentScriptType::P2sh) => {
            ("P2SH", "Pay-to-Script-Hash (t3..., multi-sig/custody)")
        }
        (_, explorer::TransparentScriptType::P2pkh) => ("P2PKH", "Pay-to-Public-Key-Hash (tm...)"),
        (_, explorer::TransparentScriptType::P2sh) => {
            ("P2SH", "Pay-to-Script-Hash (t2..., multi-sig/custody)")
        }
        _ => ("other", "Other"),
    };
    json!({
        "type": name,
        "description": description,
        "addressCount": summary.positive_address_count,
        "totalBalance": zec_from_unsigned_zatoshis(summary.total_positive_balance_zat),
        "percentage": zatoshi_percentage(
            summary.total_positive_balance_zat,
            transparent_total_zat,
        ),
    })
}

fn transparent_script_type_summary(
    ranking: &TransparentAddressRankingResponse,
    requested_type: explorer::TransparentScriptType,
) -> Result<&explorer::TransparentAddressScriptTypeSummary, CipherscanRestError> {
    let mut matches = ranking.script_type_summaries.iter().filter(|summary| {
        explorer::TransparentScriptType::try_from(summary.script_type).ok() == Some(requested_type)
    });
    let summary = matches
        .next()
        .ok_or(CipherscanRestError::InvalidUpstreamField(
            "transparent_address_ranking.script_type_summaries",
        ))?;
    if matches.next().is_some() {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "transparent_address_ranking.script_type_summaries",
        ));
    }
    Ok(summary)
}

fn ranking_visible_tip(ranking: &TransparentAddressRankingResponse) -> Option<&wallet::BlockTip> {
    explorer_visible_tip(ranking.freshness.as_ref())
}

fn ranking_visible_tip_height(ranking: &TransparentAddressRankingResponse) -> Option<u32> {
    ranking_visible_tip(ranking).map(|tip| tip.height)
}

fn ranking_balance_coverage_complete(ranking: &TransparentAddressRankingResponse) -> bool {
    ranking.coverage.as_ref().is_some_and(|coverage| {
        ranking_visible_tip_height(ranking) == Some(coverage.balance_complete_through_height)
    })
}

async fn mining_metrics(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<MiningMetricsQuery>,
) -> Result<Response, CipherscanRestError> {
    let window = mining_metrics_window(query.window.as_deref());
    let limit = mining_metrics_limit(query.limit.as_deref());
    let (tip, at_epoch_id) = adapter.fetch_visible_tip_block_context().await?;
    let start_height = tip.height.saturating_sub(limit.saturating_sub(1));
    let response = adapter
        .explorer_client()
        .block_production_series(BlockProductionSeriesRequest {
            start_height,
            end_height: tip.height,
            at_epoch_id,
        })
        .await?
        .into_inner();

    Ok(json_response(
        StatusCode::OK,
        mining_metrics_json(adapter.network, window, &response)?,
    ))
}

async fn mining_pool_distribution(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<PageQuery>,
) -> Result<Response, CipherscanRestError> {
    let period = query
        .period
        .filter(|period| !period.is_empty())
        .unwrap_or_else(|| String::from("7d"));
    let production_period = mining_production_period(&period);
    if let Some(body) = adapter
        .cached_mining_pool_distribution_response(&period)
        .await
    {
        return Ok(json_response(StatusCode::OK, body));
    }
    adapter
        .require_explorer_capability(EXPLORER_BLOCK_PRODUCTION_TIME_RANGE_V1)
        .await?;
    let window = adapter
        .fetch_mining_pool_distribution_window(production_period, OffsetDateTime::now_utc())
        .await?;
    let body = mining_pool_distribution_json(&period, &window)?;
    adapter
        .cache_mining_pool_distribution_response(period, body.clone())
        .await;
    Ok(json_response(StatusCode::OK, body))
}

async fn mining_pool_ranking(Query(query): Query<PageQuery>) -> Response {
    let period = query.period.unwrap_or_else(|| String::from("7d"));
    json_response(StatusCode::OK, mining_pool_ranking_json(&period))
}

async fn mining_hashrate_share(Query(query): Query<PageQuery>) -> Response {
    let period = query.period.unwrap_or_else(|| String::from("30d"));
    json_response(StatusCode::OK, mining_hashrate_share_json(&period))
}

async fn miner_behavior(Query(query): Query<PageQuery>) -> Response {
    let period = query.period.unwrap_or_else(|| String::from("90d"));
    json_response(StatusCode::OK, miner_behavior_json(&period))
}

async fn zodl_leaderboard(Query(query): Query<PageQuery>) -> Response {
    let period = query.period.unwrap_or_else(|| String::from("90d"));
    json_response(StatusCode::OK, zodl_leaderboard_json(&period))
}

async fn mining_rewards(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<PageQuery>,
) -> Result<Response, CipherscanRestError> {
    let period = query.period.unwrap_or_else(|| String::from("7d"));
    let window = adapter
        .mining_production_window(mining_production_period(&period))
        .await?;
    let body = mining_rewards_json(&period, &window);

    Ok(json_response(StatusCode::OK, body))
}

async fn pool_overview(
    State(adapter): State<CipherscanRestAdapter>,
) -> Result<Response, CipherscanRestError> {
    adapter
        .require_explorer_capability(EXPLORER_VALUE_POOL_BALANCE_HISTORY_V1)
        .await?;
    let mut summary_client = adapter.explorer_client();
    let mut history_client = adapter.explorer_client();
    let (summary, history) = tokio::try_join!(
        summary_client.value_pool_summary(ValuePoolSummaryRequest {}),
        history_client.value_pool_balance_history(ValuePoolBalanceHistoryRequest {
            page_size: 64,
            cursor: Vec::new(),
        }),
    )?;

    Ok(json_response(
        StatusCode::OK,
        pool_overview_json(&summary.into_inner(), &history.into_inner())?,
    ))
}

async fn pool_flows(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<PageQuery>,
) -> Result<Response, CipherscanRestError> {
    let request = CipherscanPoolFlowRequest::from_query(&query)?;
    adapter
        .require_explorer_capability(EXPLORER_VALUE_POOL_FLOW_SUMMARY_V1)
        .await?;
    let end_time_unix_seconds = OffsetDateTime::now_utc().unix_timestamp();
    let start_time_unix_seconds = cipherscan_flow_start_time(end_time_unix_seconds, &request)?;
    let summary = adapter
        .explorer_client()
        .value_pool_flow_summary(ValuePoolFlowSummaryRequest {
            start_time_unix_seconds,
            end_time_unix_seconds,
            pools: request.pools.clone(),
            resolution: request.resolution.native(),
        })
        .await?
        .into_inner();
    let coverage = summary
        .coverage
        .as_ref()
        .ok_or(CipherscanRestError::MissingUpstreamField(
            "value_pool_flow_summary.coverage",
        ))?;
    Ok(json_response(
        StatusCode::OK,
        pool_flows_json(&request, &summary, coverage)?,
    ))
}

async fn pool_turnstile() -> Response {
    json_response(
        StatusCode::SERVICE_UNAVAILABLE,
        pool_turnstile_building_json(),
    )
}

async fn migration_overview(
    State(adapter): State<CipherscanRestAdapter>,
) -> Result<Response, CipherscanRestError> {
    let tip_height = adapter.fetch_tip_height().await?;
    let analytics_state = adapter.fetch_migration_analytics().await?;
    let value_pool_summary = adapter
        .explorer_client()
        .value_pool_summary(ValuePoolSummaryRequest {})
        .await?
        .into_inner();

    Ok(json_response(
        StatusCode::OK,
        migration_overview_json(
            adapter.network,
            tip_height,
            &value_pool_summary,
            &analytics_state,
        )?,
    ))
}

async fn migration_cohorts(
    State(adapter): State<CipherscanRestAdapter>,
) -> Result<Response, CipherscanRestError> {
    let analytics_state = adapter.fetch_migration_analytics().await?;
    Ok(json_response(
        StatusCode::OK,
        migration_cohorts_json(adapter.network, &analytics_state),
    ))
}

async fn migration_denominations(
    State(adapter): State<CipherscanRestAdapter>,
) -> Result<Response, CipherscanRestError> {
    let analytics_state = adapter.fetch_migration_analytics().await?;
    Ok(json_response(
        StatusCode::OK,
        migration_denominations_json(adapter.network, &analytics_state),
    ))
}

async fn value_pool_history(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<PageQuery>,
) -> Result<Response, CipherscanRestError> {
    let period = query.period.unwrap_or_else(|| String::from("1y"));
    let format = query.format.unwrap_or_else(|| String::from("zec"));
    adapter
        .require_explorer_capability(EXPLORER_VALUE_POOL_BALANCE_HISTORY_V1)
        .await?;
    let history = adapter
        .explorer_client()
        .value_pool_balance_history(ValuePoolBalanceHistoryRequest {
            page_size: value_pool_history_page_size(&period),
            cursor: Vec::new(),
        })
        .await?
        .into_inner();
    Ok(json_response(
        StatusCode::OK,
        value_pool_history_json(&period, &format, &history)?,
    ))
}

async fn chain_size_history(Query(query): Query<PageQuery>) -> Response {
    let period = query.period.unwrap_or_else(|| String::from("90d"));
    json_response(StatusCode::OK, chain_size_history_json(&period))
}

async fn network_fees(
    State(adapter): State<CipherscanRestAdapter>,
) -> Result<Response, CipherscanRestError> {
    let current_height = adapter.fetch_tip_height().await?;
    let start_height = current_height.saturating_sub(FEE_SUMMARY_WINDOW_BLOCKS.saturating_sub(1));
    let summary = adapter
        .explorer_client()
        .fee_summary(FeeSummaryRequest {
            start_height,
            end_height: current_height,
        })
        .await?
        .into_inner();

    Ok(json_response(
        StatusCode::OK,
        network_fees_json(&summary, start_height, current_height),
    ))
}

async fn fee_distribution(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<PageQuery>,
) -> Result<Response, CipherscanRestError> {
    let period = fee_distribution_period(query.period.as_deref());
    let server_info = adapter.fetch_explorer_server_info().await?;
    let paid_fee_available =
        explorer_supports_capability(&server_info, EXPLORER_PAID_FEE_DISTRIBUTION_V1);
    let fee_basis = if paid_fee_available {
        "paid"
    } else {
        "conventional"
    };
    let cache_key = format!("{}:{fee_basis}", period.echoed);
    if let Some(body) = adapter.cached_fee_distribution_response(&cache_key).await {
        return Ok(json_response(StatusCode::OK, body));
    }

    let requested_at = OffsetDateTime::now_utc();
    let (start_time_unix_seconds, end_time_unix_seconds) =
        fee_distribution_range(period.days, requested_at.unix_timestamp());
    let generated_at = OffsetDateTime::now_utc();
    let body = if paid_fee_available {
        let distribution = adapter
            .fetch_paid_fee_distribution(start_time_unix_seconds, end_time_unix_seconds)
            .await?;
        paid_fee_distribution_json(&period.echoed, &distribution, generated_at)?
    } else {
        let distribution = adapter
            .fetch_conventional_fee_distribution(start_time_unix_seconds, end_time_unix_seconds)
            .await?;
        conventional_fee_distribution_json(&period.echoed, &distribution, generated_at)?
    };
    adapter
        .cache_fee_distribution_response(cache_key, body.clone())
        .await;

    Ok(json_response(StatusCode::OK, body))
}

async fn protocol_stats(
    State(adapter): State<CipherscanRestAdapter>,
) -> Result<Response, CipherscanRestError> {
    adapter
        .require_explorer_capability(EXPLORER_TRANSACTION_COMPONENT_SUMMARY_V2)
        .await?;
    let commitment_tree_sizes = adapter.fetch_visible_tip_commitment_tree_sizes().await?;
    let end_time_unix_seconds = OffsetDateTime::now_utc()
        .unix_timestamp()
        .saturating_add(COMPONENT_SUMMARY_FUTURE_TIME_MARGIN_SECONDS);
    let summary = adapter
        .fetch_transaction_component_summary(0, end_time_unix_seconds, false)
        .await?;

    Ok(json_response(
        StatusCode::OK,
        protocol_stats_json(&summary, &commitment_tree_sizes),
    ))
}

async fn usage_clock(
    State(adapter): State<CipherscanRestAdapter>,
    Query(query): Query<UsageClockQuery>,
) -> Result<Response, CipherscanRestError> {
    let period = query.period.unwrap_or_else(|| String::from("1y"));
    let block_limit = usage_clock_block_limit(&period);
    let distribution = adapter
        .fetch_block_activity_distribution(block_limit)
        .await?;

    Ok(json_response(
        StatusCode::OK,
        usage_clock_json(&period, &distribution),
    ))
}

async fn peers() -> Response {
    json_response(StatusCode::OK, peer_inventory_json())
}

async fn nodes() -> Response {
    json_response(StatusCode::OK, node_locations_json())
}

async fn node_stats() -> Response {
    json_response(StatusCode::OK, node_statistics_json())
}

async fn node_history(Query(query): Query<PageQuery>) -> Response {
    let period = query.period.unwrap_or_else(|| String::from("30d"));
    json_response(StatusCode::OK, node_history_json(&period))
}

async fn realtime_websocket(
    State(adapter): State<CipherscanRestAdapter>,
    websocket: WebSocketUpgrade,
) -> Response {
    let realtime_events = adapter.subscribe_realtime_events();
    let realtime_cancel = adapter.realtime_cancel;
    let realtime_tasks = adapter.realtime_tasks;
    websocket.on_upgrade(move |websocket| {
        realtime_tasks.track_future(forward_realtime_events(
            websocket,
            realtime_events,
            realtime_cancel,
        ))
    })
}

async fn forward_realtime_events(
    mut websocket: WebSocket,
    mut realtime_events: broadcast::Receiver<CipherscanRealtimeDispatch>,
    realtime_cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            () = realtime_cancel.cancelled() => {
                close_realtime_websocket(
                    &mut websocket,
                    close_code::AWAY,
                    "adapter shutting down",
                ).await;
                break;
            },
            client_message = websocket.recv() => match client_message {
                Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
            dispatch = realtime_events.recv() => match dispatch {
                Ok(CipherscanRealtimeDispatch::Payload(payload)) => {
                    let send = websocket.send(Message::Text(payload.as_ref().to_owned().into()));
                    if !matches!(
                        tokio::time::timeout(REALTIME_SEND_TIMEOUT, send).await,
                        Ok(Ok(()))
                    ) {
                        close_realtime_websocket(
                            &mut websocket,
                            close_code::AGAIN,
                            "realtime client too slow",
                        ).await;
                        break;
                    }
                }
                Ok(CipherscanRealtimeDispatch::SourceUnavailable) => {
                    close_realtime_websocket(
                        &mut websocket,
                        close_code::AGAIN,
                        "realtime source unavailable",
                    ).await;
                    break;
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    close_realtime_websocket(
                        &mut websocket,
                        close_code::AGAIN,
                        "realtime client lagged",
                    ).await;
                    break;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    }
}

async fn close_realtime_websocket(websocket: &mut WebSocket, code: u16, reason: &'static str) {
    let close = websocket.send(Message::Close(Some(CloseFrame {
        code,
        reason: reason.into(),
    })));
    let _ = tokio::time::timeout(REALTIME_SEND_TIMEOUT, close).await;
}

async fn relay_chain_events(adapter: CipherscanRestAdapter) {
    let realtime_sender = adapter.realtime_broadcaster.sender.clone();
    let mut resume_cursor = None;

    loop {
        let mut wallet_client = adapter.wallet_client();
        let response = tokio::select! {
            () = adapter.realtime_cancel.cancelled() => return,
            response = wallet_client.chain_events(wallet::ChainEventsRequest {
                start: Some(realtime_event_stream_start(resume_cursor.as_deref())),
                family: wallet::ChainEventStreamFamily::Visible as i32,
                address_filter: Vec::new(),
            }) => response,
        };
        let mut event_stream = match response {
            Ok(response) => response.into_inner(),
            Err(status) => {
                if status.code() == Code::FailedPrecondition {
                    resume_cursor = None;
                }
                broadcast_realtime_source_unavailable(&realtime_sender, "chain", &status);
                if !wait_for_realtime_retry(&adapter.realtime_cancel).await {
                    return;
                }
                continue;
            }
        };

        loop {
            let stream_message = tokio::select! {
                () = adapter.realtime_cancel.cancelled() => return,
                stream_message = event_stream.message() => stream_message,
            };
            let chain_event = match stream_message {
                Ok(Some(chain_event)) => chain_event,
                Ok(None) => {
                    broadcast_realtime_source_unavailable(
                        &realtime_sender,
                        "chain",
                        "native stream ended",
                    );
                    break;
                }
                Err(status) => {
                    if status.code() == Code::FailedPrecondition {
                        resume_cursor = None;
                    }
                    broadcast_realtime_source_unavailable(&realtime_sender, "chain", &status);
                    break;
                }
            };

            loop {
                let publish_result = tokio::select! {
                    () = adapter.realtime_cancel.cancelled() => return,
                    publish_result = publish_chain_event(&adapter, &realtime_sender, &chain_event) => publish_result,
                };
                match publish_result {
                    Ok(()) => {
                        resume_cursor = Some(chain_event.cursor.clone());
                        break;
                    }
                    Err(error) => {
                        broadcast_realtime_source_unavailable(
                            &realtime_sender,
                            "chain hydration",
                            &error,
                        );
                        if !wait_for_realtime_retry(&adapter.realtime_cancel).await {
                            return;
                        }
                    }
                }
            }
        }

        if !wait_for_realtime_retry(&adapter.realtime_cancel).await {
            return;
        }
    }
}

async fn relay_mempool_events(adapter: CipherscanRestAdapter) {
    let realtime_sender = adapter.realtime_broadcaster.sender.clone();
    let mut resume_cursor = None;

    loop {
        let mut wallet_client = adapter.wallet_client();
        let response = tokio::select! {
            () = adapter.realtime_cancel.cancelled() => return,
            response = wallet_client.mempool_events(wallet::MempoolEventsRequest {
                start: Some(realtime_event_stream_start(resume_cursor.as_deref())),
            }) => response,
        };
        let mut event_stream = match response {
            Ok(response) => response.into_inner(),
            Err(status) => {
                if status.code() == Code::FailedPrecondition {
                    resume_cursor = None;
                }
                broadcast_realtime_source_unavailable(&realtime_sender, "mempool", &status);
                if !wait_for_realtime_retry(&adapter.realtime_cancel).await {
                    return;
                }
                continue;
            }
        };

        loop {
            let stream_message = tokio::select! {
                () = adapter.realtime_cancel.cancelled() => return,
                stream_message = event_stream.message() => stream_message,
            };
            let mempool_event = match stream_message {
                Ok(Some(mempool_event)) => mempool_event,
                Ok(None) => {
                    broadcast_realtime_source_unavailable(
                        &realtime_sender,
                        "mempool",
                        "native stream ended",
                    );
                    break;
                }
                Err(status) => {
                    if status.code() == Code::FailedPrecondition {
                        resume_cursor = None;
                    }
                    broadcast_realtime_source_unavailable(&realtime_sender, "mempool", &status);
                    break;
                }
            };

            if let Err(error) = publish_mempool_event(&adapter, &realtime_sender, &mempool_event) {
                broadcast_realtime_source_unavailable(
                    &realtime_sender,
                    "mempool event decoding",
                    &error,
                );
            }
            resume_cursor = Some(mempool_event.cursor.clone());
        }

        if !wait_for_realtime_retry(&adapter.realtime_cancel).await {
            return;
        }
    }
}

async fn wait_for_realtime_retry(realtime_cancel: &CancellationToken) -> bool {
    tokio::select! {
        () = realtime_cancel.cancelled() => false,
        () = tokio::time::sleep(REALTIME_RECONNECT_DELAY) => true,
    }
}

fn realtime_event_stream_start(after_cursor: Option<&[u8]>) -> wallet::EventStreamStart {
    let position = after_cursor.map_or_else(
        || event_stream_start::Position::LiveTail(wallet::LiveTail {}),
        |after_cursor| event_stream_start::Position::AfterCursor(after_cursor.to_vec()),
    );
    wallet::EventStreamStart {
        position: Some(position),
    }
}

async fn publish_chain_event(
    adapter: &CipherscanRestAdapter,
    realtime_sender: &broadcast::Sender<CipherscanRealtimeDispatch>,
    chain_event: &wallet::ChainEventEnvelope,
) -> Result<(), CipherscanRestError> {
    let committed = match chain_event.event.as_ref() {
        Some(chain_event_envelope::Event::ChainCommitted(chain_committed)) => {
            chain_committed.committed.as_ref()
        }
        Some(chain_event_envelope::Event::ChainReorged(chain_reorged)) => {
            chain_reorged.committed.as_ref()
        }
        None => None,
    }
    .ok_or(CipherscanRestError::MissingUpstreamField(
        "chain_event.committed",
    ))?;
    let expected_visible_tip = committed
        .chain_epoch
        .as_ref()
        .and_then(|chain_epoch| chain_epoch.visible_tip.as_ref())
        .ok_or(CipherscanRestError::MissingUpstreamField(
            "chain_event.committed.chain_epoch.visible_tip",
        ))?;
    let committed_epoch_id = committed
        .chain_epoch
        .as_ref()
        .map(|chain_epoch| chain_epoch.chain_epoch_id)
        .ok_or(CipherscanRestError::MissingUpstreamField(
            "chain_event.committed.chain_epoch",
        ))?;
    if committed.start_height > committed.end_height {
        return Ok(());
    }
    if expected_visible_tip.height != committed.end_height {
        return Err(tonic::Status::data_loss(format!(
            "chain event committed range ends at height {}, but its visible tip is height {}",
            committed.end_height, expected_visible_tip.height,
        ))
        .into());
    }

    let Some(summaries) = hydrate_realtime_committed_blocks(
        adapter,
        committed.start_height,
        committed.end_height,
        committed_epoch_id,
        &expected_visible_tip.hash,
    )
    .await?
    else {
        tracing::debug!(
            event = "cipherscan_realtime_chain_event_superseded",
            start_height = committed.start_height,
            end_height = committed.end_height,
            expected_tip_hash = expected_visible_tip.hash,
            "skipping a chain event superseded by the current canonical chain"
        );
        return Ok(());
    };
    broadcast_realtime_blocks(realtime_sender, &summaries).await;
    publish_realtime_privacy_stats(adapter, realtime_sender).await;

    Ok(())
}

async fn hydrate_realtime_committed_blocks(
    adapter: &CipherscanRestAdapter,
    start_height: u32,
    end_height: u32,
    committed_epoch_id: u64,
    expected_tip_hash: &str,
) -> Result<Option<Vec<explorer::BlockSummary>>, CipherscanRestError> {
    for attempt in 0..REALTIME_HYDRATION_ATTEMPTS {
        let (current_tip, current_epoch_id) = adapter.fetch_visible_tip_block_context().await?;
        let current_epoch_id = current_epoch_id.ok_or(
            CipherscanRestError::MissingUpstreamField("visible_tip_block.chain_view.chain_epoch"),
        )?;
        match realtime_commit_status(
            committed_epoch_id,
            end_height,
            current_epoch_id,
            current_tip.height,
        ) {
            RealtimeCommitStatus::AwaitingReader => {
                if attempt + 1 < REALTIME_HYDRATION_ATTEMPTS {
                    tokio::time::sleep(REALTIME_HYDRATION_RETRY_DELAY).await;
                }
                continue;
            }
            RealtimeCommitStatus::Superseded => return Ok(None),
            RealtimeCommitStatus::Hydratable => {}
        }
        match fetch_realtime_block_summaries(adapter, start_height, end_height, current_epoch_id)
            .await
        {
            Ok(Some(summaries)) => {
                let final_summary =
                    summaries
                        .last()
                        .ok_or(CipherscanRestError::MissingUpstreamField(
                            "block_production_point.summary",
                        ))?;
                if final_summary.block_hash != expected_tip_hash {
                    return Ok(None);
                }
                return Ok(Some(summaries));
            }
            Ok(None) => {}
            Err(CipherscanRestError::Upstream(status))
                if matches!(status.code(), Code::NotFound | Code::FailedPrecondition) => {}
            Err(error) => return Err(error),
        }
        if attempt + 1 < REALTIME_HYDRATION_ATTEMPTS {
            tokio::time::sleep(REALTIME_HYDRATION_RETRY_DELAY).await;
        }
    }

    Err(CipherscanRestError::MissingUpstreamField(
        "block_production_series.coverage",
    ))
}

const fn realtime_commit_status(
    committed_epoch_id: u64,
    committed_end_height: u32,
    current_epoch_id: u64,
    current_tip_height: u32,
) -> RealtimeCommitStatus {
    if current_tip_height >= committed_end_height {
        return RealtimeCommitStatus::Hydratable;
    }
    if current_epoch_id > committed_epoch_id {
        return RealtimeCommitStatus::Superseded;
    }
    RealtimeCommitStatus::AwaitingReader
}

async fn fetch_realtime_block_summaries(
    adapter: &CipherscanRestAdapter,
    start_height: u32,
    end_height: u32,
    chain_epoch_id: u64,
) -> Result<Option<Vec<explorer::BlockSummary>>, CipherscanRestError> {
    let mut summaries = Vec::new();
    let mut chunk_start = start_height;
    loop {
        let chunk_end = end_height.min(
            chunk_start
                .saturating_add(BLOCK_SUMMARY_PAGE_SIZE)
                .saturating_sub(1),
        );
        let response = adapter
            .explorer_client()
            .block_production_series(BlockProductionSeriesRequest {
                start_height: chunk_start,
                end_height: chunk_end,
                at_epoch_id: Some(chain_epoch_id),
            })
            .await?
            .into_inner();
        let expected_count = inclusive_height_count(chunk_start, chunk_end);
        if response.covered_block_count != expected_count
            || response.points.len() != usize::try_from(expected_count).unwrap_or(usize::MAX)
        {
            return Ok(None);
        }
        for (offset, point) in response.points.into_iter().enumerate() {
            let summary = point
                .summary
                .ok_or(CipherscanRestError::MissingUpstreamField(
                    "block_production_point.summary",
                ))?;
            let expected_height =
                chunk_start.saturating_add(u32::try_from(offset).unwrap_or(u32::MAX));
            if summary.block_height != expected_height {
                return Err(tonic::Status::data_loss(format!(
                    "block production series returned height {} where {expected_height} was expected",
                    summary.block_height,
                ))
                .into());
            }
            summaries.push(summary);
        }
        if chunk_end == end_height {
            break;
        }
        chunk_start = chunk_end.saturating_add(1);
    }

    Ok(Some(summaries))
}

async fn broadcast_realtime_blocks(
    realtime_sender: &broadcast::Sender<CipherscanRealtimeDispatch>,
    summaries: &[explorer::BlockSummary],
) {
    for summary in summaries {
        broadcast_realtime_payload(realtime_sender, "new_block", &block_row(summary));
        tokio::task::yield_now().await;
    }
}

async fn publish_realtime_privacy_stats(
    adapter: &CipherscanRestAdapter,
    realtime_sender: &broadcast::Sender<CipherscanRealtimeDispatch>,
) {
    if realtime_sender.receiver_count() == 0 {
        return;
    }
    match Box::pin(fetch_privacy_stats_json(adapter)).await {
        Ok(stats) => broadcast_realtime_payload(realtime_sender, "privacy_stats", &stats),
        Err(error) => tracing::warn!(
            event = "cipherscan_realtime_privacy_stats_unavailable",
            %error,
            "skipping a realtime privacy_stats frame"
        ),
    }
}

fn publish_mempool_event(
    adapter: &CipherscanRestAdapter,
    realtime_sender: &broadcast::Sender<CipherscanRealtimeDispatch>,
    mempool_event: &wallet::MempoolEventEnvelope,
) -> Result<(), CipherscanRestError> {
    match mempool_event.event.as_ref() {
        Some(mempool_event_envelope::Event::Added(added)) => {
            let entry = added
                .entry
                .as_ref()
                .ok_or(CipherscanRestError::MissingUpstreamField(
                    "mempool_event.added.entry",
                ))?;
            let facts = parse_realtime_mempool_facts(adapter.network, entry)?;
            let transaction_data = mempool_added_json(&facts, entry);
            broadcast_realtime_payload(realtime_sender, "mempool_tx", &transaction_data);
        }
        Some(mempool_event_envelope::Event::Invalidated(invalidated)) => {
            let removal_data = json!({
                "txid": invalidated.transaction_id,
                "reason": "invalidated",
            });
            broadcast_realtime_payload(realtime_sender, "mempool_removed", &removal_data);
        }
        Some(mempool_event_envelope::Event::Mined(mined)) => {
            let removal_data = json!({
                "txid": mined.transaction_id,
                "reason": "mined",
                "minedHeight": mined.mined_height,
                "blockHash": mined.block_hash,
            });
            broadcast_realtime_payload(realtime_sender, "mempool_removed", &removal_data);
        }
        None => {
            return Err(CipherscanRestError::MissingUpstreamField(
                "mempool_event.event",
            ));
        }
    }

    Ok(())
}

fn parse_realtime_mempool_facts(
    network: Network,
    entry: &wallet::MempoolEntry,
) -> Result<CoreTransactionPublicFacts, CipherscanRestError> {
    let activations = NetworkUpgradeActivations::empty(network);
    let facts = zinder_source::parse_transaction_public_facts(
        &entry.raw_transaction_bytes,
        None,
        &activations,
    )
    .map_err(|error| {
        tonic::Status::data_loss(format!(
            "mempool event contains invalid transaction bytes: {error}"
        ))
    })?;
    let parsed_transaction_id = encode_rpc_transaction_id_hex(facts.transaction_id);
    if parsed_transaction_id != entry.transaction_id {
        return Err(tonic::Status::data_loss(format!(
            "mempool event transaction id {} does not match parsed transaction id {parsed_transaction_id}",
            entry.transaction_id,
        ))
        .into());
    }

    Ok(facts)
}

fn broadcast_realtime_payload(
    realtime_sender: &broadcast::Sender<CipherscanRealtimeDispatch>,
    event_type: &str,
    event_data: &Value,
) {
    let payload: Arc<str> = json!({
        "type": event_type,
        "data": event_data,
    })
    .to_string()
    .into();
    let _ = realtime_sender.send(CipherscanRealtimeDispatch::Payload(payload));
}

fn broadcast_realtime_source_unavailable(
    realtime_sender: &broadcast::Sender<CipherscanRealtimeDispatch>,
    source: &str,
    error: &(impl std::fmt::Display + ?Sized),
) {
    tracing::warn!(
        event = "cipherscan_realtime_source_unavailable",
        source,
        %error,
        "Cipherscan realtime source is unavailable"
    );
    let _ = realtime_sender.send(CipherscanRealtimeDispatch::SourceUnavailable);
}

async fn compat_fallback(method: Method, uri: Uri) -> Response {
    json_response(
        StatusCode::NOT_FOUND,
        json!({
            "success": false,
            "source": CIPHERSCAN_ADAPTER_SOURCE,
            "error": "Cipherscan REST route is not implemented by the Zinder compatibility adapter",
            "path": uri.path(),
            "method": method.as_str(),
        }),
    )
}

fn with_cors_preflight(router: Router) -> Router {
    router.layer(middleware::from_fn(handle_cors_preflight))
}

async fn handle_cors_preflight(request: Request, next: Next) -> Response {
    if request.method() == Method::OPTIONS {
        return preflight_response();
    }
    next.run(request).await
}

fn block_selector(block_id: String) -> Result<block_detail_request::Selector, CipherscanRestError> {
    match block_id.parse::<u32>() {
        Ok(height) => Ok(block_detail_request::Selector::BlockHeight(height)),
        Err(_) if block_id.len() == 64 && block_id.bytes().all(|byte| byte.is_ascii_hexdigit()) => {
            Ok(block_detail_request::Selector::BlockHash(block_id))
        }
        Err(_) => Err(CipherscanRestError::InvalidRequest(
            "block id must be a height or 64-character RPC block hash".to_owned(),
        )),
    }
}

async fn canonical_block_hash_at_height(
    adapter: &CipherscanRestAdapter,
    height: u32,
) -> Result<Option<String>, CipherscanRestError> {
    let response = match adapter
        .wallet_client()
        .block_id_by_selector(BlockSelectorRequest {
            selector: Some(wallet::BlockSelector {
                selector: Some(wallet::block_selector::Selector::Height(height)),
            }),
            at_epoch_id: None,
        })
        .await
    {
        Ok(response) => response.into_inner(),
        Err(status) if status.code() == Code::NotFound => return Ok(None),
        Err(status) => return Err(status.into()),
    };

    Ok(response.block_id.map(|block_id| block_id.block_hash))
}

fn parse_fork_monitor_height(height: &Value) -> Option<u32> {
    match height {
        Value::Number(number) => number
            .as_u64()
            .and_then(|height| u32::try_from(height).ok()),
        Value::String(text) => text.parse::<u32>().ok(),
        Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => None,
    }
}

fn fork_monitor_registry_unavailable_response() -> Response {
    json_response(
        StatusCode::SERVICE_UNAVAILABLE,
        json!({
            "success": false,
            "error": "Fork monitor reports require a Cipherscan registry sidecar",
            "degraded": true,
            "unavailable": [
                "Community fork-monitor reports are product registry data and are not stored in Zinder core."
            ],
        }),
    )
}

fn crosslink_consensus_unavailable_response() -> Response {
    sidecar_unavailable_response(
        "Crosslink consensus data is unavailable",
        "Crosslink finalized height, finality gap, finalizer roster, stake, and BFT decision data require a native Crosslink consensus surface or Cipherscan sidecar.",
    )
}

fn crosschain_sidecar_unavailable_response() -> Response {
    sidecar_unavailable_response(
        "Cross-chain analytics are unavailable",
        "Cross-chain swap, volume, latency, and bridge data require Cipherscan's NEAR Intents and bridge sidecars.",
    )
}

fn sidecar_unavailable_response(error: &str, unavailable: &str) -> Response {
    json_response(
        StatusCode::SERVICE_UNAVAILABLE,
        json!({
            "success": false,
            "error": error,
            "degraded": true,
            "unavailable": [unavailable],
        }),
    )
}

#[derive(Clone, Copy)]
struct CipherscanTransactionDetailJsonInput<'a> {
    network: Network,
    facts: &'a explorer::TransactionPublicFacts,
    location: Option<&'a wallet::TransactionLocation>,
    response: &'a explorer::TransactionDetailResponse,
    coinbase_total_output_zat: Option<u64>,
    coinbase_miner_fields: Option<&'a CipherscanCoinbaseMinerFields>,
}

struct CipherscanTransactionDetailZatoshiTotals {
    input: Option<u64>,
    output: Option<u64>,
    value_balance: Option<i64>,
}

fn cipherscan_transaction_detail_totals(
    response: &explorer::TransactionDetailResponse,
    coinbase_total_output_zat: Option<u64>,
) -> CipherscanTransactionDetailZatoshiTotals {
    let input_zat = response
        .transparent_inputs
        .iter()
        .try_fold(0_u64, |total, input| total.checked_add(input.value_zat?));
    let transparent_output_zat = response
        .transparent_outputs
        .iter()
        .try_fold(0_u64, |total, output| {
            total.checked_add(output.output.as_ref()?.value_zat)
        });
    let value_balance_zat = response
        .intrinsic_value_balances
        .as_ref()
        .and_then(|balances| {
            balances
                .sapling_zat
                .checked_add(balances.orchard_zat)?
                .checked_add(balances.ironwood_zat)
        });
    CipherscanTransactionDetailZatoshiTotals {
        input: input_zat,
        output: coinbase_total_output_zat.or(transparent_output_zat),
        value_balance: value_balance_zat,
    }
}

fn transaction_detail_json(input: CipherscanTransactionDetailJsonInput<'_>) -> Value {
    let CipherscanTransactionDetailJsonInput {
        network,
        facts,
        location,
        response,
        coinbase_total_output_zat,
        coinbase_miner_fields,
    } = input;
    let counts = facts.counts.as_ref();
    let fee = cipherscan_transaction_detail_fee(facts, response.paid_fee_zat);
    let mined = mined_location(location);
    let mempool = mempool_location(location);
    let block_location = mined.and_then(|mined_transaction| mined_transaction.location.as_ref());
    let mined_chain_context =
        mined.and_then(|mined_transaction| mined_transaction.chain_context.as_ref());
    let transaction_rows = cipherscan_transaction_detail_rows(network, facts, response);
    let intrinsic_value_balances = response.intrinsic_value_balances.as_ref();
    let totals = cipherscan_transaction_detail_totals(response, coinbase_total_output_zat);
    let cipherscan_fee = fee.amount_zec.filter(|amount| *amount > 0.0);
    let input_count = transaction_rows.inputs.len();
    let output_count = transaction_rows.outputs.len();
    let unavailable = transaction_rows.unavailable;

    let mut transaction = json!({
        "txid": facts.transaction_id,
        "blockHeight": block_location.map(|location| location.block_height.to_string()),
        "blockHash": block_location.map(|location| location.block_hash.as_str()),
        "blockTime": mined_chain_context.map(|chain_context| chain_context.block_time.to_string()),
        "confirmations": mined_chain_context.map(|chain_context| chain_context.confirmations),
        "mempoolTime": mempool.map(mempool_first_seen_unix_seconds),
        "status": transaction_status(location),
        "size": facts.size_bytes,
        "version": facts.version.as_ref().map(|version| version.effective_version),
        "versionKind": facts.version.as_ref().map(|version| version.kind),
        "locktime": cipherscan_lock_time_string(facts.lock_time.as_ref()),
        "expiryHeight": facts.expiry_height,
        "fee": cipherscan_fee,
        "feeSource": fee.source,
        "paid_fee_zat": fee.paid_fee_zat,
        "isCoinbase": facts.is_coinbase,
        "totalInput": totals.input.map(zec_from_unsigned_zatoshis),
        "totalOutput": totals.output.map(zec_from_unsigned_zatoshis),
        "hasSapling": counts.map(has_sapling_counts),
        "hasOrchard": counts.map(|counts| counts.orchard_action_count > 0),
        "hasIronwood": counts.map(|counts| counts.ironwood_action_count > 0),
        "hasSprout": counts.map(|counts| counts.sprout_joinsplit_count > 0),
        "shieldedSpends": counts.map(|counts| counts.sapling_spend_count),
        "shieldedOutputs": counts.map(|counts| counts.sapling_output_count),
        "orchardActions": counts.map(|counts| counts.orchard_action_count),
        "ironwoodActions": counts.map(|counts| counts.ironwood_action_count),
        "valueBalanceSapling": intrinsic_value_balances.map(|balances| zec_from_zatoshis(balances.sapling_zat)),
        "valueBalanceOrchard": intrinsic_value_balances.map(|balances| zec_from_zatoshis(balances.orchard_zat)),
        "valueBalanceIronwood": intrinsic_value_balances.map(|balances| zec_from_zatoshis(balances.ironwood_zat)),
        "vinCount": counts.map(|counts| counts.transparent_input_count),
        "voutCount": counts.map(|counts| counts.transparent_output_count),
        "privacyShape": facts.privacy_shape,
        "inputs": transaction_rows.inputs,
        "outputs": transaction_rows.outputs,
        "zinderUnavailable": unavailable,
    });
    if let Value::Object(fields) = &mut transaction {
        fields.insert(
            "valueBalance".to_owned(),
            json!(totals.value_balance.map(zec_from_zatoshis)),
        );
        fields.insert("inputCount".to_owned(), json!(input_count));
        fields.insert("outputCount".to_owned(), json!(output_count));
        fields.insert(
            "coinbaseHex".to_owned(),
            json!(coinbase_miner_fields.map(|coinbase| coinbase.miner_data_hex.as_str())),
        );
        fields.insert(
            "coinbaseText".to_owned(),
            json!(coinbase_miner_fields.map(|coinbase| coinbase.miner_data_text.as_str())),
        );
        fields.insert("bridge".to_owned(), Value::Null);
        fields.insert("stakingAction".to_owned(), Value::Null);
    }
    transaction
}

struct CipherscanTransactionDetailRows {
    inputs: Vec<Value>,
    outputs: Vec<Value>,
    unavailable: Vec<&'static str>,
}

fn cipherscan_transaction_detail_rows(
    network: Network,
    facts: &explorer::TransactionPublicFacts,
    response: &explorer::TransactionDetailResponse,
) -> CipherscanTransactionDetailRows {
    let inputs = response
        .transparent_inputs
        .iter()
        .map(|input| {
            let outpoint = input.spent_outpoint.as_ref();
            json!({
                "vout_index": input.input_index,
                "value": input.value_zat.map(|value_zat| value_zat.to_string()),
                "address": input.script_pub_key.as_ref().and_then(|script_pub_key| {
                    cipherscan_transparent_address(network, script_pub_key)
                }),
                "script_pubkey": input.script_pub_key.as_ref().map(hex::encode),
                "prev_txid": outpoint.map(|outpoint| outpoint.transaction_id.as_str()),
                "prev_vout": outpoint.map(|outpoint| outpoint.output_index),
            })
        })
        .collect();
    let outputs = cipherscan_transaction_detail_outputs(network, response);
    let unavailable = cipherscan_transaction_detail_unavailable(network, facts, response);
    CipherscanTransactionDetailRows {
        inputs,
        outputs,
        unavailable,
    }
}

fn cipherscan_transaction_detail_outputs(
    network: Network,
    response: &explorer::TransactionDetailResponse,
) -> Vec<Value> {
    response
        .transparent_outputs
        .iter()
        .map(|output| {
            let intrinsic = output.output.as_ref();
            json!({
                "vout_index": output.output_index,
                "value": intrinsic.map(|output| output.value_zat.to_string()),
                "script_pubkey": intrinsic.map(|output| hex::encode(&output.script_pub_key)),
                "address": intrinsic.and_then(|output| {
                    cipherscan_transparent_address(network, &output.script_pub_key)
                }),
                "spent": output.spent_by.is_some(),
            })
        })
        .collect()
}

fn cipherscan_transaction_detail_unavailable(
    network: Network,
    facts: &explorer::TransactionPublicFacts,
    response: &explorer::TransactionDetailResponse,
) -> Vec<&'static str> {
    let counts = facts.counts.as_ref();
    let mut unavailable = Vec::new();
    if !facts.is_coinbase
        && counts.is_some_and(|counts| counts.transparent_input_count > 0)
        && response.transparent_inputs.is_empty()
    {
        unavailable.push(
            "Transparent inputs are unavailable because this transaction has no canonical facts artifact.",
        );
    } else if response
        .transparent_inputs
        .iter()
        .any(|input| input.spent_outpoint.is_none())
    {
        unavailable.push("A transparent input is missing its canonical spent outpoint.");
    }
    if response
        .transparent_inputs
        .iter()
        .any(|input| input.value_zat.is_none() || input.script_pub_key.is_none())
    {
        unavailable.push(
            "A transparent input prevout is partially unavailable because its retained value or parent script is missing.",
        );
    }
    if response.transparent_inputs.iter().any(|input| {
        input.script_pub_key.as_ref().is_some_and(|script_pub_key| {
            cipherscan_transparent_address(network, script_pub_key).is_none()
        })
    }) {
        unavailable.push(
            "A transparent input prevout uses a nonstandard script, so no address is inferred from its scriptPubKey.",
        );
    }
    if response
        .transparent_outputs
        .iter()
        .any(|output| output.output.is_none())
    {
        unavailable.push("A transparent output is missing its intrinsic value and script.");
    }
    if response.transparent_outputs.iter().any(|output| {
        output.output.as_ref().is_some_and(|output| {
            cipherscan_transparent_address(network, &output.script_pub_key).is_none()
        })
    }) {
        unavailable.push(
            "A transparent output uses a nonstandard script, so no address is inferred from its scriptPubKey.",
        );
    }
    if counts.is_some_and(|counts| counts.transparent_output_count > 0)
        && response.transparent_outputs.is_empty()
    {
        unavailable.push(
            "Transparent outputs are unavailable because this transaction has no canonical facts artifact.",
        );
    }
    if cipherscan_transaction_detail_fee(facts, response.paid_fee_zat).source
        == "zip317-conventional"
    {
        unavailable.push(
            "Actual paid fees are unavailable for shielded transaction detail. fee is the ZIP-317 conventional fee.",
        );
    }
    unavailable
}

/// Projects a complete native block row into Cipherscan's existing REST shape.
///
/// The caller withholds the table when any row lacks public facts, because the
/// unchanged Cipherscan UI would otherwise infer a false coinbase status.
fn cipherscan_block_transaction_json(
    network: Network,
    transaction: &explorer::BlockTransaction,
    block_height: u32,
    block_time_unix_seconds: i64,
    expose_input_values: bool,
) -> Option<Value> {
    let facts = transaction.public_facts.as_ref()?;
    let counts = facts.counts.as_ref()?;
    let inputs: Vec<Value> = transaction
        .transparent_inputs
        .iter()
        .map(|input| cipherscan_block_transparent_input_json(network, input, expose_input_values))
        .collect();
    let outputs: Vec<Value> = transaction
        .transparent_outputs
        .iter()
        .enumerate()
        .map(|(output_index, output)| {
            json!({
                "vout_index": output_index,
                "value": output.value_zat.to_string(),
                "script_pubkey": hex::encode(&output.script_pub_key),
                "address": cipherscan_transparent_address(network, &output.script_pub_key),
            })
        })
        .collect();
    let mut unavailable = Vec::new();
    if !expose_input_values && !transaction.transparent_inputs.is_empty() {
        unavailable.push(
            "Transparent input values are withheld because this block contains shielded components and unchanged Cipherscan would calculate a false partial block fee.",
        );
    }
    if transaction.transparent_inputs.iter().any(|input| {
        input.value_zat.is_none()
            || input.script_pub_key.is_none()
            || input.spent_outpoint.is_none()
    }) {
        unavailable.push(
            "A transparent input prevout is partially unavailable because its retained outpoint, value, or parent script is missing.",
        );
    }
    if transaction.transparent_inputs.iter().any(|input| {
        input.script_pub_key.as_ref().is_some_and(|script_pub_key| {
            cipherscan_transparent_address(network, script_pub_key).is_none()
        })
    }) {
        unavailable.push(
            "A transparent input prevout uses a nonstandard script, so no address is inferred from its scriptPubKey.",
        );
    }
    if transaction
        .transparent_outputs
        .iter()
        .any(|output| cipherscan_transparent_address(network, &output.script_pub_key).is_none())
    {
        unavailable.push(
            "A transparent output uses a nonstandard script, so no address is inferred from its scriptPubKey.",
        );
    }
    if has_shielded_components(Some(counts)) {
        unavailable.push(
            "Shielded receiver addresses and shielded output values are encrypted and unavailable.",
        );
    }

    Some(json!({
        "txid": transaction.transaction_id,
        "tx_index": transaction.transaction_index,
        "block_height": block_height,
        "block_time": block_time_unix_seconds,
        "size": facts.size_bytes,
        "version": facts.version.as_ref().map(|version| version.effective_version),
        "is_coinbase": facts.is_coinbase,
        "has_sapling": has_sapling_counts(counts),
        "has_orchard": counts.orchard_action_count > 0,
        "has_ironwood": counts.ironwood_action_count > 0,
        "has_sprout": counts.sprout_joinsplit_count > 0,
        "shielded_spends": counts.sapling_spend_count,
        "shielded_outputs": counts.sapling_output_count,
        "orchard_actions": counts.orchard_action_count,
        "ironwood_actions": counts.ironwood_action_count,
        "transparent_input_count": counts.transparent_input_count,
        "transparent_output_count": counts.transparent_output_count,
        "inputs": inputs,
        "outputs": outputs,
        "zinderUnavailable": unavailable,
    }))
}

fn cipherscan_block_transparent_input_json(
    network: Network,
    input: &explorer::TransparentInput,
    expose_value: bool,
) -> Value {
    let outpoint = input.spent_outpoint.as_ref();
    let address = input
        .script_pub_key
        .as_ref()
        .and_then(|script_pub_key| cipherscan_transparent_address(network, script_pub_key));
    let mut unavailable = Vec::new();
    if outpoint.is_none() {
        unavailable.push("The transparent input is missing its canonical spent outpoint.");
    }
    if input.script_pub_key.is_none() {
        unavailable.push("The transparent input parent script is unavailable.");
    } else if address.is_none() {
        unavailable
            .push("The transparent input parent script is nonstandard, so no address is inferred.");
    }
    if input.value_zat.is_none() {
        unavailable.push("The transparent input parent value is unavailable.");
    } else if !expose_value {
        unavailable
            .push("The transparent input value is withheld to prevent a false partial block fee.");
    }
    json!({
        "txid": outpoint.map(|outpoint| outpoint.transaction_id.as_str()),
        "prev_txid": outpoint.map(|outpoint| outpoint.transaction_id.as_str()),
        "prev_vout": outpoint.map(|outpoint| outpoint.output_index),
        "vout_index": input.input_index,
        "value": expose_value.then_some(input.value_zat).flatten(),
        "script_pubkey": input.script_pub_key.as_ref().map(hex::encode),
        "address": address,
        "zinderUnavailable": unavailable,
    })
}

fn cipherscan_transparent_address(network: Network, script_pub_key: &[u8]) -> Option<String> {
    let network_kind = zebra_network_for(network).ok()?.t_addr_kind();
    if script_pub_key.len() == 25
        && script_pub_key[0..3] == [0x76, 0xa9, 0x14]
        && script_pub_key[23..] == [0x88, 0xac]
    {
        let public_key_hash = <[u8; 20]>::try_from(&script_pub_key[3..23]).ok()?;
        return Some(
            transparent::Address::from_pub_key_hash(network_kind, public_key_hash).to_string(),
        );
    }
    if script_pub_key.len() == 23
        && script_pub_key[0..2] == [0xa9, 0x14]
        && script_pub_key[22] == 0x87
    {
        let script_hash = <[u8; 20]>::try_from(&script_pub_key[2..22]).ok()?;
        return Some(transparent::Address::from_script_hash(network_kind, script_hash).to_string());
    }
    None
}

#[derive(Clone, Copy)]
struct CipherscanTransactionDetailFee {
    amount_zec: Option<f64>,
    paid_fee_zat: Option<u64>,
    source: &'static str,
}

fn cipherscan_transaction_detail_fee(
    facts: &explorer::TransactionPublicFacts,
    paid_fee_zat: Option<u64>,
) -> CipherscanTransactionDetailFee {
    if facts.is_coinbase {
        return CipherscanTransactionDetailFee {
            amount_zec: Some(0.0),
            paid_fee_zat: None,
            source: "coinbase",
        };
    }

    let is_transparent_only = facts.privacy_shape == explorer::PrivacyShape::TransparentOnly as i32;
    if is_transparent_only && let Some(paid_fee_zat) = paid_fee_zat {
        return CipherscanTransactionDetailFee {
            amount_zec: Some(zec_from_unsigned_zatoshis(paid_fee_zat)),
            paid_fee_zat: Some(paid_fee_zat),
            source: "paid-fee",
        };
    }

    let conventional_fee_zat = facts.counts.as_ref().map(|counts| {
        CoreTransactionComponentCounts {
            transparent_input_count: counts.transparent_input_count,
            transparent_output_count: counts.transparent_output_count,
            sapling_spend_count: counts.sapling_spend_count,
            sapling_output_count: counts.sapling_output_count,
            orchard_action_count: counts.orchard_action_count,
            sprout_joinsplit_count: counts.sprout_joinsplit_count,
            ironwood_action_count: counts.ironwood_action_count,
        }
        .zip317_conventional_fee_zat()
    });
    conventional_fee_zat.map_or(
        CipherscanTransactionDetailFee {
            amount_zec: None,
            paid_fee_zat: None,
            source: "unavailable",
        },
        |conventional_fee_zat| CipherscanTransactionDetailFee {
            amount_zec: Some(zec_from_unsigned_zatoshis(conventional_fee_zat)),
            paid_fee_zat: None,
            source: "zip317-conventional",
        },
    )
}

struct CipherscanBlockListEntry {
    summary: explorer::BlockSummary,
    difficulty: f64,
    miner_address: Option<String>,
}

struct NetworkActivityWindow {
    latest_difficulty: f64,
    network_hashrate_raw: f64,
    average_block_time_seconds: u32,
    block_count: u32,
    transaction_count: u64,
}

impl NetworkActivityWindow {
    fn from_entries(
        entries: &[CipherscanBlockListEntry],
        tip_height: u32,
        cutoff_unix_seconds: i64,
    ) -> Result<Self, CipherscanRestError> {
        let latest_difficulty = entries
            .iter()
            .find(|entry| entry.summary.block_height == tip_height)
            .map(|entry| entry.difficulty)
            .ok_or(CipherscanRestError::MissingUpstreamField(
                "network_stats.tip_difficulty",
            ))?;
        let mut block_count = 0_u32;
        let mut transaction_count = 0_u64;
        for entry in entries
            .iter()
            .filter(|entry| entry.summary.block_time_unix_seconds >= cutoff_unix_seconds)
        {
            block_count = block_count.saturating_add(1);
            transaction_count =
                transaction_count.saturating_add(u64::from(entry.summary.transaction_count));
        }
        let average_block_time_seconds = 86_400_u32
            .saturating_add(block_count / 2)
            .checked_div(block_count)
            .unwrap_or(DEFAULT_MINING_BLOCK_INTERVAL_SECONDS);
        let network_hashrate_raw = latest_difficulty / f64::from(average_block_time_seconds);
        Ok(Self {
            latest_difficulty,
            network_hashrate_raw,
            average_block_time_seconds,
            block_count,
            transaction_count,
        })
    }
}

impl CipherscanBlockListEntry {
    fn try_from_point(
        network: Network,
        point: explorer::BlockProductionPoint,
    ) -> Result<Self, CipherscanRestError> {
        if point
            .coinbase
            .as_ref()
            .is_some_and(|coinbase| !is_rpc_transaction_id(&coinbase.transaction_id))
        {
            return Err(CipherscanRestError::InvalidUpstreamField(
                "block_production_point.coinbase.transaction_id",
            ));
        }
        let miner_address = point
            .coinbase
            .as_ref()
            .and_then(|coinbase| coinbase.transparent_outputs.first())
            .and_then(|output| cipherscan_transparent_address(network, &output.script_pub_key));
        let summary = point
            .summary
            .ok_or(CipherscanRestError::MissingUpstreamField(
                "block_production_point.summary",
            ))?;
        Ok(Self {
            summary,
            difficulty: cipherscan_difficulty(network, point.bits)?,
            miner_address,
        })
    }
}

fn block_list_row(entry: &CipherscanBlockListEntry) -> Value {
    let summary = &entry.summary;
    let fee_zat = summary
        .paid_fees_collected_zat
        .unwrap_or(summary.fees_collected_zat);
    json!({
        "height": summary.block_height.to_string(),
        "hash": summary.block_hash,
        "timestamp": summary.block_time_unix_seconds.to_string(),
        "transaction_count": summary.transaction_count,
        "transactionCount": summary.transaction_count,
        "txCount": summary.transaction_count,
        "size": summary.total_size_bytes,
        "difficulty": cipherscan_difficulty_string(entry.difficulty),
        "miner_address": entry.miner_address,
        "miner_pool": Value::Null,
        "total_fees": fee_zat.to_string(),
        "coinbase_reward": summary.coinbase_reward_zat.to_string(),
        "finality_status": cipherscan_block_finality_status(summary),
        "is_canonical": summary.is_canonical,
    })
}

fn block_row(summary: &explorer::BlockSummary) -> Value {
    let fee_zat = summary
        .paid_fees_collected_zat
        .unwrap_or(summary.fees_collected_zat);
    json!({
        "height": summary.block_height.to_string(),
        "hash": summary.block_hash,
        "timestamp": summary.block_time_unix_seconds.to_string(),
        "transaction_count": summary.transaction_count,
        "transactionCount": summary.transaction_count,
        "txCount": summary.transaction_count,
        "size": summary.total_size_bytes,
        "difficulty": Value::Null,
        "miner_address": Value::Null,
        "miner_pool": Value::Null,
        "total_fees": fee_zat.to_string(),
        "coinbase_reward": summary.coinbase_reward_zat.to_string(),
        "finality_status": cipherscan_block_finality_status(summary),
        "is_canonical": summary.is_canonical,
    })
}

#[derive(Clone, Copy)]
struct CommitmentRootSearchJsonInput<'a> {
    root: &'a str,
    canonical: &'a [Value],
    orphaned: &'a [Value],
    canonical_coverage: &'a explorer::CommitmentRootSearchCoverage,
    displaced_root_capability: bool,
    displaced_root_coverage: Option<&'a explorer::CommitmentRootSearchDisplacedCoverage>,
}

fn displaced_root_coverage_note(
    coverage: Option<&explorer::CommitmentRootSearchDisplacedCoverage>,
) -> &'static str {
    match coverage {
        Some(coverage) if coverage.root_artifact_unavailable_count > 0 => {
            "The covered archive also has unavailable root artifacts."
        }
        Some(coverage) if coverage.activation_event_sequence.is_none() => {
            "The displaced-root archive has not activated because no post-deployment displacement has been captured."
        }
        Some(coverage) if coverage.captured_range_complete => {
            "The captured archive range has searchable root artifacts."
        }
        Some(_) | None => "The displaced-root archive coverage is incomplete.",
    }
}

fn commitment_root_search_json(input: CommitmentRootSearchJsonInput<'_>) -> Value {
    let CommitmentRootSearchJsonInput {
        root,
        canonical,
        orphaned,
        canonical_coverage: coverage,
        displaced_root_capability,
        displaced_root_coverage,
    } = input;
    let found = !canonical.is_empty() || !orphaned.is_empty();
    let displaced_root_supported = displaced_root_capability && displaced_root_coverage.is_some();
    let no_retained_displaced_match = orphaned.is_empty();
    let unavailable = if !displaced_root_supported {
        vec!["Non-canonical commitment-root matches are unavailable because the native displaced-root capability or activation coverage is absent.".to_owned()]
    } else if no_retained_displaced_match {
        let coverage_note = displaced_root_coverage_note(displaced_root_coverage);
        vec![format!(
            "No retained displaced commitment-root match exists in the covered archive since activation. {coverage_note} Pre-activation archive history remains unknown, so full historical orphan parity is unavailable."
        )]
    } else {
        vec![
            "The displaced commitment-root match is retained in the covered archive since activation. Pre-activation archive history remains unknown, so full historical orphan parity is unavailable.".to_owned(),
        ]
    };
    json!({
        "root": root.to_ascii_lowercase(),
        "found": found,
        "canonical": canonical,
        "orphaned": orphaned,
        "diagnosis": if found {
            if orphaned.is_empty() {
                "This anchor root is on the canonical chain."
            } else if canonical.is_empty() {
                if coverage.canonical_history_complete {
                    "This anchor root exists ONLY on retained orphaned fork(s). A wallet referencing this root is stuck on a dead fork and needs to rescan."
                } else {
                    "This anchor root has a retained match on an orphaned fork; canonical history is incomplete."
                }
            } else {
                "This anchor root exists on the canonical chain and one or more orphaned fork(s)."
            }
        } else if coverage.canonical_history_complete {
            if displaced_root_supported {
                "This anchor root has no canonical match and no retained displaced match in the covered archive since activation; pre-activation displaced history remains unknown."
            } else {
                "This anchor root is unknown to the complete canonical history, but displaced history is unavailable."
            }
        } else {
            "This anchor root has no canonical match in the currently indexed coverage; canonical history is incomplete, and displaced history is activation-limited."
        },
        "coverage": {
            "completeFromHeight": coverage.complete_from_height,
            "completeThroughHeight": coverage.complete_through_height,
            "latestIndexedHeight": coverage.latest_indexed_height,
            "canonicalHistoryComplete": coverage.canonical_history_complete,
        },
        "displacedCoverage": {
            "capabilityAdvertised": displaced_root_capability,
            "available": displaced_root_coverage.is_some(),
            "activationEventSequence": displaced_root_coverage.map(|coverage| coverage.activation_event_sequence),
            "activationEpochId": displaced_root_coverage.map(|coverage| coverage.activation_epoch_id),
            "activatedAtMillis": displaced_root_coverage.map(|coverage| coverage.activated_at_millis),
            "capturedBlockCount": displaced_root_coverage.map(|coverage| coverage.captured_block_count),
            "rootArtifactUnavailableCount": displaced_root_coverage.map(|coverage| coverage.root_artifact_unavailable_count),
            "capturedRangeComplete": displaced_root_coverage.map(|coverage| coverage.captured_range_complete),
            "returnedMatchCount": orphaned.len(),
            "historicalParity": "activation_limited",
        },
        "degraded": !unavailable.is_empty(),
        "unavailable": unavailable,
    })
}

fn commitment_root_match_json(
    root_match: &explorer::CommitmentRootMatch,
    miner_address: Option<&str>,
    chain: &str,
    detected_at: Option<&str>,
) -> Result<Value, CipherscanRestError> {
    let protocol = match explorer::ShieldedProtocol::try_from(root_match.protocol) {
        Ok(explorer::ShieldedProtocol::Sapling) => "sapling",
        Ok(explorer::ShieldedProtocol::Orchard) => "orchard",
        Ok(explorer::ShieldedProtocol::Ironwood) => "ironwood",
        Ok(explorer::ShieldedProtocol::Sprout | explorer::ShieldedProtocol::Unspecified)
        | Err(_) => {
            return Err(CipherscanRestError::InvalidUpstreamField(
                "commitment_root_search.matches.protocol",
            ));
        }
    };
    Ok(json!({
        "height": root_match.block_height,
        "hash": root_match.block_hash,
        "timestamp": root_match.block_time_unix_seconds,
        "matchedField": protocol,
        "minerAddress": miner_address,
        "minerPool": Value::Null,
        "chain": chain,
        "detectedAt": detected_at,
    }))
}

fn recent_transaction_row(entry: &explorer::TransactionHistoryEntry) -> Value {
    let counts = entry.component_counts.as_ref();
    json!({
        "txid": entry.transaction_id,
        "block_height": entry.block_height,
        "block_time": entry.block_time_unix_seconds,
        "blockHash": entry.block_hash,
        "tx_index": entry.transaction_index,
        "size": entry.size_bytes,
        "vin_count": counts.map(|counts| counts.transparent_input_count),
        "vout_count": counts.map(|counts| counts.transparent_output_count),
        "has_sapling": counts.map(has_sapling_counts),
        "has_orchard": counts.map(|counts| counts.orchard_action_count > 0),
        "has_ironwood": counts.map(|counts| counts.ironwood_action_count > 0),
        "has_sprout": counts.map(|counts| counts.sprout_joinsplit_count > 0),
        "is_coinbase": entry.is_coinbase,
        "value_balance": Value::Null,
        "value_balance_sapling": Value::Null,
        "value_balance_orchard": Value::Null,
        "value_balance_ironwood": Value::Null,
        "fee": entry.zip317_conventional_fee_zat.map(|fee_zat| fee_zat.to_string()),
        "feeSource": entry.zip317_conventional_fee_zat.map(|_| "zip317-conventional"),
        "zip317_conventional_fee_zat": entry.zip317_conventional_fee_zat,
        "logical_actions": entry.logical_actions,
        "privacy_shape": entry.privacy_shape,
        "flow_type": shielded_flow_type_or_none(entry),
        "zinderUnavailable": [
            "Actual paid fees are unavailable for recent transaction rows. fee is the ZIP-317 conventional fee."
        ],
    })
}

fn shielded_transaction_row(entry: &explorer::TransactionHistoryEntry) -> Value {
    let counts = entry.component_counts.as_ref();
    let intrinsic_value_balances = entry.intrinsic_value_balances.as_ref();
    json!({
        "txid": entry.transaction_id,
        "blockHeight": entry.block_height,
        "blockHash": entry.block_hash,
        "blockTime": entry.block_time_unix_seconds,
        "hasSapling": counts.map(has_sapling_counts),
        "hasOrchard": counts.map(|counts| counts.orchard_action_count > 0),
        "hasIronwood": counts.map(|counts| counts.ironwood_action_count > 0),
        "shieldedSpends": counts.map(|counts| counts.sapling_spend_count),
        "shieldedOutputs": counts.map(|counts| counts.sapling_output_count),
        "orchardActions": counts.map(|counts| counts.orchard_action_count),
        "ironwoodActions": counts.map(|counts| counts.ironwood_action_count),
        "vinCount": counts.map(|counts| counts.transparent_input_count),
        "voutCount": counts.map(|counts| counts.transparent_output_count),
        "size": entry.size_bytes,
        "fee": entry.paid_fee_zat.map(zec_from_unsigned_zatoshis),
        "feeSource": entry.paid_fee_zat.map(|_| "paid"),
        "zip317ConventionalFee": entry.zip317_conventional_fee_zat.map(zec_from_unsigned_zatoshis),
        "valueBalanceSapling": intrinsic_value_balances.map(|balances| zec_from_zatoshis(balances.sapling_zat)),
        "valueBalanceOrchard": intrinsic_value_balances.map(|balances| zec_from_zatoshis(balances.orchard_zat)),
        "valueBalanceIronwood": intrinsic_value_balances.map(|balances| zec_from_zatoshis(balances.ironwood_zat)),
        "type": if entry.privacy_shape == explorer::PrivacyShape::ShieldedOnly as i32 {
            "fully-shielded"
        } else {
            "partial"
        },
    })
}

fn shielded_flow_row(event: &explorer::ValuePoolFlowEvent) -> Result<Value, CipherscanRestError> {
    validate_value_pool_flow_event(event)?;
    let coordinate = CipherscanFlowCoordinate::from_event(event)?;
    let direction = cipherscan_flow_direction(event.direction)?;
    let pool = cipherscan_flow_pool(event.pool)?;
    Ok(json!({
        "id": coordinate.stable_id(),
        "txid": event.transaction_id,
        "blockHeight": event.block_height,
        "blockTime": event.block_time_unix_seconds,
        "flowType": direction,
        "amountZec": zec_from_unsigned_zatoshis(event.amount_zat),
        "pool": pool,
        "addresses": [],
        "zinderUnavailable": [
            "Transparent address attribution is unavailable for this value-pool flow event."
        ],
    }))
}

fn validate_value_pool_flow_event(
    event: &explorer::ValuePoolFlowEvent,
) -> Result<(), CipherscanRestError> {
    if !is_lowercase_rpc_transaction_id(&event.transaction_id) {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "value_pool_flow_history.events.transaction_id",
        ));
    }
    if event.amount_zat == 0 {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "value_pool_flow_history.events.amount_zat",
        ));
    }
    let _ = OffsetDateTime::from_unix_timestamp(event.block_time_unix_seconds).map_err(|_| {
        CipherscanRestError::InvalidUpstreamField(
            "value_pool_flow_history.events.block_time_unix_seconds",
        )
    })?;
    let _ = cipherscan_flow_direction(event.direction)?;
    let _ = cipherscan_flow_pool(event.pool)?;
    let _ = CipherscanFlowCoordinate::from_event(event)?;
    Ok(())
}

fn is_lowercase_rpc_transaction_id(transaction_id: &str) -> bool {
    transaction_id.len() == 64
        && transaction_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn cipherscan_flow_direction(encoded: i32) -> Result<&'static str, CipherscanRestError> {
    match ValuePoolFlowDirection::try_from(encoded) {
        Ok(ValuePoolFlowDirection::Shield) => Ok("shield"),
        Ok(ValuePoolFlowDirection::Deshield) => Ok("deshield"),
        Ok(ValuePoolFlowDirection::Unspecified) | Err(_) => Err(
            CipherscanRestError::InvalidUpstreamField("value_pool_flow_history.events.direction"),
        ),
    }
}

fn cipherscan_flow_pool(encoded: i32) -> Result<&'static str, CipherscanRestError> {
    match ValuePoolFlowPool::try_from(encoded) {
        Ok(ValuePoolFlowPool::Sprout) => Ok("sprout"),
        Ok(ValuePoolFlowPool::Sapling) => Ok("sapling"),
        Ok(ValuePoolFlowPool::Orchard) => Ok("orchard"),
        Ok(ValuePoolFlowPool::Ironwood) => Ok("ironwood"),
        Ok(ValuePoolFlowPool::Mixed) => Ok("mixed"),
        Ok(ValuePoolFlowPool::Unspecified) | Err(_) => Err(
            CipherscanRestError::InvalidUpstreamField("value_pool_flow_history.events.pool"),
        ),
    }
}

fn next_value_pool_flow_cursor(
    current_cursor: &[u8],
    response: &ValuePoolFlowHistoryResponse,
) -> Result<Vec<u8>, CipherscanRestError> {
    if response.next_cursor.is_empty() || response.next_cursor == current_cursor {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "value_pool_flow_history.next_cursor",
        ));
    }
    Ok(response.next_cursor.clone())
}

fn shielded_flow_filter(
    query: &ShieldedFlowQuery,
) -> Result<ValuePoolFlowFilter, CipherscanRestError> {
    let directions = match query.flow_type.as_deref() {
        None | Some("all") => Vec::new(),
        Some("shield") => vec![ValuePoolFlowDirection::Shield as i32],
        Some("deshield") => vec![ValuePoolFlowDirection::Deshield as i32],
        Some(flow_type) => {
            return Err(CipherscanRestError::InvalidRequest(format!(
                "unsupported shielded flow type: {flow_type}"
            )));
        }
    };
    let pools = match query.pool.as_deref() {
        None | Some("all") => Vec::new(),
        Some("sprout") => vec![ValuePoolFlowPool::Sprout as i32],
        Some("sapling") => vec![ValuePoolFlowPool::Sapling as i32],
        Some("orchard") => vec![ValuePoolFlowPool::Orchard as i32],
        Some("ironwood") => vec![ValuePoolFlowPool::Ironwood as i32],
        Some("mixed") => vec![ValuePoolFlowPool::Mixed as i32],
        Some(pool) => {
            return Err(CipherscanRestError::InvalidRequest(format!(
                "unsupported shielded flow pool: {pool}"
            )));
        }
    };
    Ok(ValuePoolFlowFilter {
        directions,
        pools,
        minimum_amount_zat: shielded_flow_minimum_zat(query.min_zec)?,
    })
}

fn shielded_flow_anchor(
    query: &ShieldedFlowQuery,
) -> Result<Option<CipherscanFlowCursor>, CipherscanRestError> {
    match (query.cursor, query.cursor_id) {
        (Some(block_time_unix_seconds), Some(stable_id)) => {
            let block_height = stable_id / CIPHERSCAN_FLOW_TRANSACTION_INDEX_FACTOR;
            let transaction_index = stable_id % CIPHERSCAN_FLOW_TRANSACTION_INDEX_FACTOR;
            Ok(Some(CipherscanFlowCursor {
                block_time_unix_seconds,
                coordinate: CipherscanFlowCoordinate {
                    block_height: u32::try_from(block_height).map_err(|_| {
                        CipherscanRestError::InvalidRequest(
                            "cursor_id contains an invalid block height".to_owned(),
                        )
                    })?,
                    transaction_index: u32::try_from(transaction_index).map_err(|_| {
                        CipherscanRestError::InvalidRequest(
                            "cursor_id contains an invalid transaction index".to_owned(),
                        )
                    })?,
                },
            }))
        }
        (None, None) => Ok(None),
        (Some(_), None) | (None, Some(_)) => Err(CipherscanRestError::InvalidRequest(
            "cursor and cursor_id must be provided together".to_owned(),
        )),
    }
}

fn shielded_flow_page_direction(direction: Option<&str>) -> ShieldedFlowPageDirection {
    if direction == Some("prev") {
        ShieldedFlowPageDirection::Newer
    } else {
        ShieldedFlowPageDirection::Older
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "Cipherscan accepts decimal ZEC query amounts and rounds them into zatoshis."
)]
fn shielded_flow_minimum_zat(minimum_zec: Option<f64>) -> Result<u64, CipherscanRestError> {
    let Some(minimum_zec) = minimum_zec else {
        return Ok(0);
    };
    if !minimum_zec.is_finite() || !(0.0..=MAX_SUPPLY_ZEC).contains(&minimum_zec) {
        return Err(CipherscanRestError::InvalidRequest(
            "min_zec must be a finite amount from 0 to 21000000".to_owned(),
        ));
    }
    Ok((minimum_zec * ZATOSHIS_PER_ZEC).round() as u64)
}

fn value_pool_flow_coverage_json(coverage: &explorer::ValuePoolFlowCoverage) -> Value {
    json!({
        "historicalFromHeight": coverage.historical_from_height,
        "historicalThroughHeight": coverage.historical_through_height,
        "historicalFromTime": coverage.historical_from_time_unix_seconds,
        "historicalThroughTime": coverage.historical_through_time_unix_seconds,
        "liveTailFromHeight": coverage.live_tail_from_height,
        "liveTailThroughHeight": coverage.live_tail_through_height,
        "liveTailThroughTime": coverage.live_tail_through_time_unix_seconds,
        "requestedRangeComplete": coverage.requested_range_complete,
    })
}

fn mempool_row(entry: &explorer::MempoolActivityEntry) -> Value {
    let counts = entry.component_counts.as_ref();
    let mut unavailable = Vec::new();
    if counts.is_none() {
        unavailable.push(
            "Mempool transaction component counts are unavailable from the connected Zinder explorer.",
        );
    }
    unavailable.push(
        "Sapling, Orchard, and Ironwood value balances are unavailable from the native mempool surface.",
    );
    json!({
        "txid": entry.transaction_id,
        "size": entry.size_bytes,
        "type": cipherscan_mempool_transaction_type(entry.privacy_shape),
        "time": entry.first_seen_unix_millis / 1_000,
        "vin": counts.map_or(0, |counts| counts.transparent_input_count),
        "vout": counts.map_or(0, |counts| counts.transparent_output_count),
        "vShieldedSpend": counts.map_or(0, |counts| counts.sapling_spend_count),
        "vShieldedOutput": counts.map_or(0, |counts| counts.sapling_output_count),
        "orchardActions": counts.map_or(0, |counts| counts.orchard_action_count),
        "ironwoodActions": counts.map_or(0, |counts| counts.ironwood_action_count),
        "hasSapling": counts.is_some_and(|counts| {
            counts.sapling_spend_count > 0 || counts.sapling_output_count > 0
        }),
        "hasOrchard": counts.is_some_and(|counts| counts.orchard_action_count > 0),
        "hasIronwood": counts.is_some_and(|counts| counts.ironwood_action_count > 0),
        "totalOutput": zec_from_unsigned_zatoshis(entry.transparent_output_total_zat),
        "valueBalanceSapling": Value::Null,
        "valueBalanceOrchard": Value::Null,
        "valueBalanceIronwood": Value::Null,
        "version": entry.version.as_ref().map(|version| version.effective_version),
        "zip317_conventional_fee_zat": entry.zip317_conventional_fee_zat,
        "paid_fee_zat": entry.paid_fee_zat,
        "logical_actions": entry.logical_actions,
        "zinderUnavailable": unavailable,
    })
}

fn mempool_added_json(facts: &CoreTransactionPublicFacts, entry: &wallet::MempoolEntry) -> Value {
    let counts = facts.counts;
    let transparent_output_total_zat = entry
        .transparent_outputs
        .iter()
        .map(|output| output.value_zat)
        .fold(0_u64, u64::saturating_add);
    json!({
        "txid": encode_rpc_transaction_id_hex(facts.transaction_id),
        "size": facts.size_bytes,
        "type": core_transaction_type(counts),
        "time": entry.first_seen_unix_millis / 1_000,
        "inputCount": counts.transparent_input_count,
        "outputCount": counts.transparent_output_count,
        "hasSapling": counts.sapling_spend_count > 0 || counts.sapling_output_count > 0,
        "hasOrchard": counts.orchard_action_count > 0,
        "hasIronwood": counts.ironwood_action_count > 0,
        "orchardActions": counts.orchard_action_count,
        "ironwoodActions": counts.ironwood_action_count,
        "totalOutput": zec_from_unsigned_zatoshis(transparent_output_total_zat),
        "version": facts.version.effective_version(),
        "zinderUnavailable": [
            "Actual paid fee and shielded value balances are unavailable on the realtime mempool event."
        ],
    })
}

fn core_transaction_type(counts: CoreTransactionComponentCounts) -> &'static str {
    match (
        counts.has_shielded_input() || counts.has_shielded_output(),
        counts.has_transparent_input() || counts.has_transparent_output(),
    ) {
        (true, true) => "mixed",
        (true, false) => "shielded",
        (false, _) => "transparent",
    }
}

fn mempool_stats_json(summary: &explorer::MempoolSnapshotSummary) -> Value {
    let shielded = summary
        .privacy_shape_distribution
        .iter()
        .filter(|shape| cipherscan_mempool_transaction_type(shape.shape) != "transparent")
        .fold(0_u32, |total, shape| total.saturating_add(shape.count));
    let transparent = summary.transaction_count.saturating_sub(shielded);
    json!({
        "total": summary.transaction_count,
        "shielded": shielded,
        "transparent": transparent,
        "shieldedPercentage": percentage(shielded, summary.transaction_count),
        "totalSizeBytes": summary.total_size_bytes,
        "oldestEntryAgeMillis": summary.oldest_entry_age_millis,
        "newestEntryAgeMillis": summary.newest_entry_age_millis,
    })
}

fn cipherscan_mempool_transaction_type(privacy_shape: i32) -> &'static str {
    match explorer::PrivacyShape::try_from(privacy_shape) {
        Ok(explorer::PrivacyShape::ShieldedOnly | explorer::PrivacyShape::ShieldedCoinbase) => {
            "shielded"
        }
        Ok(
            explorer::PrivacyShape::Shielding
            | explorer::PrivacyShape::Deshielding
            | explorer::PrivacyShape::Mixed,
        ) => "mixed",
        Ok(
            explorer::PrivacyShape::Unspecified
            | explorer::PrivacyShape::TransparentOnly
            | explorer::PrivacyShape::Coinbase
            | explorer::PrivacyShape::Unclassified,
        )
        | Err(_) => "transparent",
    }
}

fn mempool_transaction_not_found_response() -> Response {
    json_response(
        StatusCode::OK,
        json!({
            "success": true,
            "inMempool": false,
        }),
    )
}

fn mempool_transaction_json(
    network: Network,
    facts: &explorer::TransactionPublicFacts,
    mempool: &wallet::MempoolEntry,
    response: &explorer::TransactionDetailResponse,
) -> Value {
    let counts = facts.counts.as_ref();
    let outputs: Vec<Value> = response
        .transparent_outputs
        .iter()
        .filter_map(|transparent_output| {
            transparent_output.output.as_ref().map(|output| {
                json!({
                    "value": zec_from_unsigned_zatoshis(output.value_zat),
                    "n": transparent_output.output_index,
                    "address": cipherscan_transparent_address(network, &output.script_pub_key),
                })
            })
        })
        .collect();
    let transparent_output_total_zat = response
        .transparent_outputs
        .iter()
        .filter_map(|transparent_output| transparent_output.output.as_ref())
        .map(|output| output.value_zat)
        .fold(0_u64, u64::saturating_add);
    let has_nonstandard_output = response
        .transparent_outputs
        .iter()
        .any(|transparent_output| {
            transparent_output.output.as_ref().is_some_and(|output| {
                cipherscan_transparent_address(network, &output.script_pub_key).is_none()
            })
        });
    let mut unavailable = vec![
        "Sapling, Orchard, and Ironwood value balances are unavailable from the native transaction detail surface.",
    ];
    if has_nonstandard_output {
        unavailable.push(
            "A transparent output uses a nonstandard script, so no address is inferred from its scriptPubKey.",
        );
    }
    json!({
        "txid": facts.transaction_id,
        "size": facts.size_bytes,
        "type": compat_transaction_type(counts),
        "version": facts.version.as_ref().map(|version| version.effective_version),
        "versionKind": facts.version.as_ref().map(|version| version.kind),
        "locktime": lock_time_json(facts.lock_time.as_ref()),
        "firstSeen": mempool_first_seen_unix_seconds(mempool),
        "vinCount": counts.map(|counts| counts.transparent_input_count),
        "voutCount": counts.map(|counts| counts.transparent_output_count),
        "shieldedSpends": counts.map(|counts| counts.sapling_spend_count),
        "shieldedOutputs": counts.map(|counts| counts.sapling_output_count),
        "orchardActions": counts.map(|counts| counts.orchard_action_count),
        "ironwoodActions": counts.map(|counts| counts.ironwood_action_count),
        "hasIronwood": counts.map(|counts| counts.ironwood_action_count > 0),
        "valueBalanceIronwood": Value::Null,
        "privacyShape": facts.privacy_shape,
        "totalOutput": zec_from_unsigned_zatoshis(transparent_output_total_zat),
        "outputs": outputs,
        "zinderUnavailable": unavailable,
    })
}

fn shielded_stats_missing_since_response() -> Response {
    json_response(
        StatusCode::BAD_REQUEST,
        json!({
            "success": false,
            "error": "Missing required parameter: since (e.g., ?since=2024-01-01)",
        }),
    )
}

fn shielded_count_json(
    since: &str,
    is_detailed: bool,
    summary: &TransactionComponentSummaryResponse,
    queried_at: OffsetDateTime,
) -> Value {
    let totals = summary.totals.as_ref().copied().unwrap_or_default();
    let total_shielded = totals.sapling_or_orchard_transaction_count;
    let fully_shielded = totals.sapling_or_orchard_fully_shielded_transaction_count;
    let coverage_complete = transaction_component_coverage_complete(summary);
    let unavailable = (!coverage_complete)
        .then_some("The requested range extends beyond contiguous transaction-component history.");
    if is_detailed {
        return json!({
            "success": true,
            "since": since,
            "queriedAt": rfc3339_timestamp(queried_at),
            "totalShielded": total_shielded,
            "breakdown": {
                "saplingOnly": totals.sapling_without_orchard_transaction_count,
                "orchardOnly": totals.orchard_without_sapling_transaction_count,
                "bothPools": totals.sapling_and_orchard_transaction_count,
            },
            "fullyShielded": fully_shielded,
            "partiallyShielded": total_shielded.saturating_sub(fully_shielded),
            "timeRange": {
                "firstTx": summary.days.iter()
                    .filter_map(|day| day.first_sapling_or_orchard_transaction_time_unix_seconds)
                    .min()
                    .map(cipherscan_timestamp_from_unix_seconds),
                "lastTx": summary.days.iter()
                    .filter_map(|day| day.last_sapling_or_orchard_transaction_time_unix_seconds)
                    .max()
                    .map(cipherscan_timestamp_from_unix_seconds),
            },
            "coverage": transaction_component_coverage_json(summary),
            "degraded": !coverage_complete,
            "unavailable": unavailable,
        });
    }

    json!({
        "success": true,
        "since": since,
        "totalShielded": total_shielded,
        "coverage": transaction_component_coverage_json(summary),
        "degraded": !coverage_complete,
        "unavailable": unavailable,
    })
}

fn shielded_daily_json(
    since: &str,
    until: &str,
    summary: &TransactionComponentSummaryResponse,
) -> Value {
    let daily = summary
        .days
        .iter()
        .filter_map(|day| {
            let count = day
                .totals
                .as_ref()
                .map_or(0, |totals| totals.sapling_or_orchard_transaction_count);
            (count > 0).then(|| {
                json!({
                    "date": calendar_date_from_unix_seconds(day.day_start_unix_seconds),
                    "count": count,
                })
            })
        })
        .collect::<Vec<_>>();
    let total_shielded = summary
        .totals
        .as_ref()
        .map_or(0, |totals| totals.sapling_or_orchard_transaction_count);
    let coverage_complete = transaction_component_coverage_complete(summary);
    json!({
        "success": true,
        "since": since,
        "until": until,
        "totalDays": daily.len(),
        "totalShielded": total_shielded,
        "daily": daily,
        "coverage": transaction_component_coverage_json(summary),
        "degraded": !coverage_complete,
        "unavailable": (!coverage_complete).then_some(
            "The requested range extends beyond contiguous transaction-component history."
        ),
    })
}

fn transaction_component_coverage_complete(summary: &TransactionComponentSummaryResponse) -> bool {
    summary
        .coverage
        .as_ref()
        .is_some_and(|coverage| coverage.requested_range_complete)
}

fn transaction_component_coverage_json(summary: &TransactionComponentSummaryResponse) -> Value {
    summary.coverage.as_ref().map_or(Value::Null, |coverage| {
        json!({
            "completeFromHeight": coverage.complete_from_height,
            "completeThroughHeight": coverage.complete_through_height,
            "completeFromTime": cipherscan_timestamp_from_unix_seconds(
                coverage.complete_from_time_unix_seconds
            ),
            "completeThroughTime": cipherscan_timestamp_from_unix_seconds(
                coverage.complete_through_time_unix_seconds
            ),
            "requestedRangeComplete": coverage.requested_range_complete,
        })
    })
}

fn cipherscan_timestamp_from_unix_seconds(unix_seconds: i64) -> String {
    let Ok(timestamp) = OffsetDateTime::from_unix_timestamp(unix_seconds) else {
        return String::from("1970-01-01T00:00:00.000Z");
    };
    let timestamp = rfc3339_timestamp(timestamp);
    timestamp.strip_suffix('Z').map_or_else(
        || timestamp.clone(),
        |without_zone| {
            if without_zone.contains('.') {
                timestamp.clone()
            } else {
                format!("{without_zone}.000Z")
            }
        },
    )
}

fn calendar_date_from_unix_seconds(unix_seconds: i64) -> String {
    OffsetDateTime::from_unix_timestamp(unix_seconds).map_or_else(
        |_| String::from("1970-01-01"),
        |timestamp| timestamp.date().to_string(),
    )
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct CipherscanLinkabilityRequest {
    limit: u32,
    tolerance_zat: u64,
}

impl CipherscanLinkabilityRequest {
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "Cipherscan accepts a bounded decimal ZEC tolerance and rounds it into zatoshis."
    )]
    fn from_query(query: &TransactionLinkabilityQuery) -> Self {
        let tolerance_zec = query
            .tolerance
            .as_deref()
            .and_then(parse_javascript_float)
            .filter(|tolerance| tolerance.is_finite() && *tolerance != 0.0)
            .unwrap_or(LINKABILITY_DEFAULT_TOLERANCE_ZEC)
            .clamp(
                LINKABILITY_MINIMUM_TOLERANCE_ZEC,
                LINKABILITY_MAXIMUM_TOLERANCE_ZEC,
            );
        Self {
            limit: parse_cipherscan_bounded_integer(query.limit.as_deref(), 5, 1, 20),
            tolerance_zat: (tolerance_zec * ZATOSHIS_PER_ZEC).round() as u64,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct CipherscanLinkabilityFlow {
    transaction_id: String,
    direction: ValuePoolFlowDirection,
    amount_zat: u64,
    block_height: u32,
    block_time_unix_seconds: i64,
    pool: ValuePoolFlowPool,
    transparent_addresses: Vec<String>,
}

enum CipherscanLinkabilitySource {
    Flow(CipherscanLinkabilityFlow),
    NoFlow,
    Unavailable(&'static str),
}

enum CipherscanLinkabilityOutcome {
    Available(Value),
    Unavailable(Value),
}

impl CipherscanRestAdapter {
    async fn fetch_transaction_linkability(
        &self,
        transaction_id: &str,
        request: CipherscanLinkabilityRequest,
        now_unix_seconds: i64,
    ) -> Result<Option<CipherscanLinkabilityOutcome>, CipherscanRestError> {
        for _ in 0..3 {
            let detail = match self
                .explorer_client()
                .transaction_detail(TransactionDetailRequest {
                    transaction_id: transaction_id.to_owned(),
                    at_epoch_id: None,
                })
                .await
            {
                Ok(response) => response.into_inner(),
                Err(status) if status.code() == Code::NotFound => return Ok(None),
                Err(status) => return Err(status.into()),
            };
            let detail_epoch = explorer_chain_epoch_id(detail.freshness.as_ref()).ok_or(
                CipherscanRestError::MissingUpstreamField(
                    "transaction_detail.freshness.chain_epoch",
                ),
            )?;
            let source = match cipherscan_linkability_source(self.network, &detail)? {
                CipherscanLinkabilitySource::Flow(source) => source,
                CipherscanLinkabilitySource::NoFlow => {
                    return Ok(Some(CipherscanLinkabilityOutcome::Available(
                        transaction_linkability_no_flow_json(transaction_id),
                    )));
                }
                CipherscanLinkabilitySource::Unavailable(reason) => {
                    return Ok(Some(transaction_linkability_unavailable_outcome(
                        transaction_id,
                        None,
                        reason,
                    )));
                }
            };
            let cutoff =
                now_unix_seconds.saturating_sub(LINKABILITY_LOOKBACK_DAYS * UNIX_SECONDS_PER_DAY);
            if source.block_time_unix_seconds <= cutoff {
                return Ok(Some(transaction_linkability_unavailable_outcome(
                    transaction_id,
                    Some(&source),
                    "Transaction linkability is available only for the completely indexed rolling 30-day flow window.",
                )));
            }
            let minimum_amount_zat = source
                .amount_zat
                .saturating_sub(request.tolerance_zat)
                .max(LINKABILITY_MINIMUM_AMOUNT_ZAT);
            let maximum_amount_zat = source.amount_zat.saturating_add(request.tolerance_zat);
            let range = self
                .explorer_client()
                .value_pool_flow_events_in_range(ValuePoolFlowEventsInRangeRequest {
                    start_time_unix_seconds: cutoff.saturating_add(1),
                    end_time_unix_seconds: now_unix_seconds.saturating_add(1),
                    directions: Vec::new(),
                    pools: Vec::new(),
                    minimum_amount_zat,
                    maximum_amount_zat: Some(maximum_amount_zat),
                    max_events: LINKABILITY_NATIVE_EVENT_LIMIT,
                })
                .await?
                .into_inner();
            let range_epoch = explorer_chain_epoch_id(range.freshness.as_ref()).ok_or(
                CipherscanRestError::MissingUpstreamField(
                    "value_pool_flow_events_in_range.freshness.chain_epoch",
                ),
            )?;
            if detail_epoch != range_epoch {
                continue;
            }
            return Ok(Some(
                self.transaction_linkability_from_range(source, request, range, range_epoch)
                    .await?,
            ));
        }
        Ok(Some(transaction_linkability_unavailable_outcome(
            transaction_id,
            None,
            "Transaction detail and value-pool flow history did not stabilize on one chain epoch.",
        )))
    }

    async fn transaction_linkability_from_range(
        &self,
        source: CipherscanLinkabilityFlow,
        request: CipherscanLinkabilityRequest,
        range: ValuePoolFlowEventsInRangeResponse,
        chain_epoch_id: u64,
    ) -> Result<CipherscanLinkabilityOutcome, CipherscanRestError> {
        let coverage = range
            .coverage
            .as_ref()
            .ok_or(CipherscanRestError::MissingUpstreamField(
                "value_pool_flow_events_in_range.coverage",
            ))?;
        if !coverage.requested_range_complete
            || range.scan_limit_reached
            || range.event_limit_reached
        {
            return Ok(transaction_linkability_unavailable_outcome(
                &source.transaction_id,
                Some(&source),
                "The bounded value-pool flow range is incomplete or truncated.",
            ));
        }
        for event in &range.events {
            validate_value_pool_flow_event(event)?;
        }
        if !range
            .events
            .iter()
            .any(|event| cipherscan_linkability_event_matches_source(event, &source))
        {
            return Ok(transaction_linkability_unavailable_outcome(
                &source.transaction_id,
                Some(&source),
                "The source transaction is not present in the complete value-pool flow range.",
            ));
        }
        let candidates = cipherscan_linkability_candidates(
            &source,
            &range.events,
            request.tolerance_zat,
            request.limit,
        )?;
        let candidates = self
            .hydrate_linkability_candidate_addresses(candidates, chain_epoch_id)
            .await?;
        Ok(CipherscanLinkabilityOutcome::Available(
            transaction_linkability_json(&source, &candidates),
        ))
    }

    async fn hydrate_linkability_candidate_addresses(
        &self,
        candidates: Vec<CipherscanLinkabilityCandidate>,
        chain_epoch_id: u64,
    ) -> Result<Vec<CipherscanLinkabilityCandidate>, CipherscanRestError> {
        let mut hydrated = Vec::with_capacity(candidates.len());
        for mut candidate in candidates {
            let detail = self
                .explorer_client()
                .transaction_detail(TransactionDetailRequest {
                    transaction_id: candidate.flow.transaction_id.clone(),
                    at_epoch_id: Some(chain_epoch_id),
                })
                .await?
                .into_inner();
            if explorer_chain_epoch_id(detail.freshness.as_ref()) != Some(chain_epoch_id) {
                return Err(CipherscanRestError::InvalidUpstreamField(
                    "transaction_detail.freshness.chain_epoch",
                ));
            }
            candidate.flow.transparent_addresses =
                cipherscan_linkability_addresses(self.network, candidate.flow.direction, &detail);
            hydrated.push(candidate);
        }
        Ok(hydrated)
    }
}

fn transaction_linkability_unavailable_json(
    transaction_id: &str,
    source: Option<&CipherscanLinkabilityFlow>,
    has_shielded_activity: bool,
    reason: &'static str,
) -> Value {
    let mut response = json!({
        "success": false,
        "txid": transaction_id,
        "flowType": Value::Null,
        "amount": Value::Null,
        "amountZat": Value::Null,
        "blockHeight": Value::Null,
        "blockTime": Value::Null,
        "pool": Value::Null,
        "hasShieldedActivity": has_shielded_activity,
        "transparentAddresses": [],
        "linkedTransactions": [],
        "algorithm": {
            "version": "2.0",
            "note": "Scores combine amount similarity, timing, amount rarity, weird-amount detection, and ambiguity penalties.",
        },
        "degraded": true,
        "unavailable": [reason],
    });
    if let Some(source) = source {
        response["flowType"] =
            json!(cipherscan_flow_direction(source.direction as i32).unwrap_or("unknown"));
        response["amount"] = json!(zec_from_unsigned_zatoshis(source.amount_zat));
        response["amountZat"] = json!(source.amount_zat);
        response["blockHeight"] = json!(source.block_height);
        response["blockTime"] = json!(source.block_time_unix_seconds);
        response["pool"] = json!(cipherscan_flow_pool(source.pool as i32).unwrap_or("unknown"));
        response["transparentAddresses"] = json!(source.transparent_addresses);
    }
    response
}

fn transaction_linkability_unavailable_outcome(
    transaction_id: &str,
    source: Option<&CipherscanLinkabilityFlow>,
    reason: &'static str,
) -> CipherscanLinkabilityOutcome {
    CipherscanLinkabilityOutcome::Unavailable(transaction_linkability_unavailable_json(
        transaction_id,
        source,
        true,
        reason,
    ))
}

fn transaction_linkability_no_flow_json(transaction_id: &str) -> Value {
    json!({
        "success": true,
        "txid": transaction_id,
        "flowType": Value::Null,
        "hasShieldedActivity": false,
        "linkedTransactions": [],
        "message": "This transaction has no shielding or deshielding activity",
    })
}

fn cipherscan_linkability_source(
    network: Network,
    detail: &explorer::TransactionDetailResponse,
) -> Result<CipherscanLinkabilitySource, CipherscanRestError> {
    let facts = detail
        .facts
        .as_ref()
        .ok_or(CipherscanRestError::MissingUpstreamField(
            "transaction_detail.facts",
        ))?;
    let counts = facts
        .counts
        .as_ref()
        .ok_or(CipherscanRestError::MissingUpstreamField(
            "transaction_detail.facts.counts",
        ))?;
    let has_shielded = has_shielded_components(Some(counts));
    let has_transparent = has_transparent_components(Some(counts));
    if !has_shielded || !has_transparent || facts.is_coinbase {
        return Ok(CipherscanLinkabilitySource::NoFlow);
    }
    let Some(balances) = detail.intrinsic_value_balances.as_ref() else {
        return Ok(CipherscanLinkabilitySource::Unavailable(
            "Transaction-intrinsic value-pool balances are unavailable.",
        ));
    };
    let net_zat = balances
        .sprout_zat
        .checked_add(balances.sapling_zat)
        .and_then(|total| total.checked_add(balances.orchard_zat))
        .and_then(|total| total.checked_add(balances.ironwood_zat))
        .ok_or(CipherscanRestError::InvalidUpstreamField(
            "transaction_detail.intrinsic_value_balances",
        ))?;
    if net_zat == 0 {
        return Ok(CipherscanLinkabilitySource::NoFlow);
    }
    let mined = mined_location(detail.location.as_ref()).ok_or(
        CipherscanRestError::MissingUpstreamField("transaction_detail.location.mined"),
    )?;
    let block_location =
        mined
            .location
            .as_ref()
            .ok_or(CipherscanRestError::MissingUpstreamField(
                "transaction_detail.location.mined.location",
            ))?;
    let mined_chain_context =
        mined
            .chain_context
            .as_ref()
            .ok_or(CipherscanRestError::MissingUpstreamField(
                "transaction_detail.location.mined.chain_context",
            ))?;
    validate_transaction_detail_outputs(facts, detail)?;
    let direction = if net_zat < 0 {
        ValuePoolFlowDirection::Shield
    } else {
        ValuePoolFlowDirection::Deshield
    };
    let pool = cipherscan_linkability_pool(balances);
    Ok(CipherscanLinkabilitySource::Flow(
        CipherscanLinkabilityFlow {
            transaction_id: facts.transaction_id.clone(),
            direction,
            amount_zat: net_zat.unsigned_abs(),
            block_height: block_location.block_height,
            block_time_unix_seconds: mined_chain_context.block_time,
            pool,
            transparent_addresses: cipherscan_linkability_addresses(network, direction, detail),
        },
    ))
}

fn cipherscan_linkability_pool(
    balances: &explorer::TransactionIntrinsicValueBalances,
) -> ValuePoolFlowPool {
    let pools = [
        (balances.sprout_zat, ValuePoolFlowPool::Sprout),
        (balances.sapling_zat, ValuePoolFlowPool::Sapling),
        (balances.orchard_zat, ValuePoolFlowPool::Orchard),
        (balances.ironwood_zat, ValuePoolFlowPool::Ironwood),
    ];
    let mut nonzero_pools = pools
        .into_iter()
        .filter_map(|(balance, pool)| (balance != 0).then_some(pool));
    let first = nonzero_pools.next().unwrap_or(ValuePoolFlowPool::Mixed);
    if nonzero_pools.next().is_some() {
        ValuePoolFlowPool::Mixed
    } else {
        first
    }
}

fn cipherscan_linkability_addresses(
    network: Network,
    direction: ValuePoolFlowDirection,
    detail: &explorer::TransactionDetailResponse,
) -> Vec<String> {
    let scripts: Vec<&[u8]> = match direction {
        ValuePoolFlowDirection::Shield => detail
            .transparent_inputs
            .iter()
            .filter_map(|input| input.script_pub_key.as_deref())
            .collect(),
        ValuePoolFlowDirection::Deshield => detail
            .transparent_outputs
            .iter()
            .filter_map(|output| {
                output
                    .output
                    .as_ref()
                    .map(|output| output.script_pub_key.as_slice())
            })
            .collect(),
        ValuePoolFlowDirection::Unspecified => Vec::new(),
    };
    let mut seen = HashSet::new();
    scripts
        .into_iter()
        .filter_map(|script| cipherscan_transparent_address(network, script))
        .filter(|address| seen.insert(address.clone()))
        .collect()
}

fn cipherscan_linkability_event_matches_source(
    event: &explorer::ValuePoolFlowEvent,
    source: &CipherscanLinkabilityFlow,
) -> bool {
    event.transaction_id == source.transaction_id
        && event.direction == source.direction as i32
        && event.amount_zat == source.amount_zat
        && event.block_height == source.block_height
        && event.block_time_unix_seconds == source.block_time_unix_seconds
        && event.pool == source.pool as i32
}

#[derive(Clone, Debug, PartialEq)]
struct CipherscanLinkabilityScoreBreakdown {
    amount_similarity: u32,
    time_proximity: u32,
    amount_rarity: u32,
    weird_amount: u32,
    pool_match: u32,
    ambiguity_penalty: u32,
}

#[derive(Clone, Debug, PartialEq)]
struct CipherscanLinkabilityCandidate {
    flow: CipherscanLinkabilityFlow,
    time_delta_seconds: i64,
    linkability_score: u32,
    warning_level: &'static str,
    ambiguity_score: u32,
    confidence_margin: u32,
    score_breakdown: CipherscanLinkabilityScoreBreakdown,
}

#[derive(Clone, Copy)]
struct CipherscanLinkabilityPair<'a> {
    shield: &'a CipherscanLinkabilityFlow,
    deshield: &'a CipherscanLinkabilityFlow,
    preliminary_score: u32,
    occurrence_count: u32,
}

fn cipherscan_linkability_candidates(
    source: &CipherscanLinkabilityFlow,
    events: &[explorer::ValuePoolFlowEvent],
    tolerance_zat: u64,
    limit: u32,
) -> Result<Vec<CipherscanLinkabilityCandidate>, CipherscanRestError> {
    let flows = events
        .iter()
        .map(cipherscan_linkability_flow_from_event)
        .collect::<Result<Vec<_>, _>>()?;
    let occurrence_count_by_amount = flows.iter().fold(HashMap::new(), |mut counts, flow| {
        let count = counts.entry(flow.amount_zat).or_insert(0_u32);
        *count = count.saturating_add(1);
        counts
    });
    let shields = flows
        .iter()
        .filter(|flow| flow.direction == ValuePoolFlowDirection::Shield)
        .collect::<Vec<_>>();
    let deshields = flows
        .iter()
        .filter(|flow| flow.direction == ValuePoolFlowDirection::Deshield)
        .collect::<Vec<_>>();
    let mut candidates = Vec::new();
    for deshield in deshields {
        let mut pairs = shields
            .iter()
            .copied()
            .filter(|shield| {
                cipherscan_linkability_pair_is_eligible(shield, deshield, tolerance_zat)
            })
            .map(|shield| {
                let occurrence_count = occurrence_count_by_amount
                    .get(&shield.amount_zat)
                    .copied()
                    .unwrap_or(1);
                CipherscanLinkabilityPair {
                    shield,
                    deshield,
                    preliminary_score: cipherscan_linkability_preliminary_score(
                        shield,
                        deshield,
                        occurrence_count,
                    ),
                    occurrence_count,
                }
            })
            .collect::<Vec<_>>();
        pairs.sort_unstable_by(|left, right| {
            right
                .preliminary_score
                .cmp(&left.preliminary_score)
                .then_with(|| {
                    right
                        .shield
                        .block_time_unix_seconds
                        .cmp(&left.shield.block_time_unix_seconds)
                })
        });
        let pair_count = pairs.len();
        for (index, pair) in pairs
            .iter()
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
            .enumerate()
        {
            if pair.shield.transaction_id != source.transaction_id
                && pair.deshield.transaction_id != source.transaction_id
            {
                continue;
            }
            let next_score = pairs
                .get(index.saturating_add(1))
                .map_or(0, |next| next.preliminary_score);
            let margin = pair.preliminary_score.saturating_sub(next_score);
            if let Some(candidate) = cipherscan_linkability_candidate(
                source,
                *pair,
                u32::try_from(pair_count).unwrap_or(u32::MAX),
                margin,
            ) {
                candidates.push(candidate);
            }
        }
    }
    Ok(rank_cipherscan_linkability_candidates(candidates, limit))
}

fn rank_cipherscan_linkability_candidates(
    mut candidates: Vec<CipherscanLinkabilityCandidate>,
    limit: u32,
) -> Vec<CipherscanLinkabilityCandidate> {
    candidates.sort_unstable_by(|left, right| {
        right
            .linkability_score
            .cmp(&left.linkability_score)
            .then_with(|| {
                right
                    .flow
                    .block_time_unix_seconds
                    .cmp(&left.flow.block_time_unix_seconds)
            })
    });
    candidates.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
    candidates
}

fn cipherscan_linkability_flow_from_event(
    event: &explorer::ValuePoolFlowEvent,
) -> Result<CipherscanLinkabilityFlow, CipherscanRestError> {
    validate_value_pool_flow_event(event)?;
    Ok(CipherscanLinkabilityFlow {
        transaction_id: event.transaction_id.clone(),
        direction: ValuePoolFlowDirection::try_from(event.direction).map_err(|_| {
            CipherscanRestError::InvalidUpstreamField(
                "value_pool_flow_events_in_range.events.direction",
            )
        })?,
        amount_zat: event.amount_zat,
        block_height: event.block_height,
        block_time_unix_seconds: event.block_time_unix_seconds,
        pool: ValuePoolFlowPool::try_from(event.pool).map_err(|_| {
            CipherscanRestError::InvalidUpstreamField("value_pool_flow_events_in_range.events.pool")
        })?,
        transparent_addresses: Vec::new(),
    })
}

fn cipherscan_linkability_pair_is_eligible(
    shield: &CipherscanLinkabilityFlow,
    deshield: &CipherscanLinkabilityFlow,
    tolerance_zat: u64,
) -> bool {
    deshield.block_time_unix_seconds > shield.block_time_unix_seconds
        && deshield.block_time_unix_seconds
            < shield
                .block_time_unix_seconds
                .saturating_add(LINKABILITY_MAX_PAIR_AGE_SECONDS)
        && shield.amount_zat.abs_diff(deshield.amount_zat) <= tolerance_zat
}

fn cipherscan_linkability_preliminary_score(
    shield: &CipherscanLinkabilityFlow,
    deshield: &CipherscanLinkabilityFlow,
    occurrence_count: u32,
) -> u32 {
    cipherscan_linkability_amount_similarity_score(shield.amount_zat.abs_diff(deshield.amount_zat))
        + cipherscan_linkability_time_score(
            deshield
                .block_time_unix_seconds
                .saturating_sub(shield.block_time_unix_seconds),
        )
        + cipherscan_linkability_rarity_score(occurrence_count)
        + cipherscan_linkability_weird_amount_score(shield.amount_zat, occurrence_count)
        + u32::from(shield.pool == deshield.pool) * 4
}

fn cipherscan_linkability_candidate(
    source: &CipherscanLinkabilityFlow,
    pair: CipherscanLinkabilityPair<'_>,
    candidate_count: u32,
    margin: u32,
) -> Option<CipherscanLinkabilityCandidate> {
    let ambiguity_score = cipherscan_linkability_ambiguity_score(candidate_count, margin);
    let ambiguity_penalty = ambiguity_score.saturating_add(2) / 4;
    let linkability_score = pair
        .preliminary_score
        .saturating_sub(ambiguity_penalty)
        .min(100);
    if linkability_score < LINKABILITY_MINIMUM_CONFIDENCE_SCORE {
        return None;
    }
    let linked_flow = if source.direction == ValuePoolFlowDirection::Shield {
        pair.deshield
    } else {
        pair.shield
    };
    Some(CipherscanLinkabilityCandidate {
        flow: linked_flow.clone(),
        time_delta_seconds: linked_flow
            .block_time_unix_seconds
            .saturating_sub(source.block_time_unix_seconds),
        linkability_score,
        warning_level: cipherscan_linkability_warning_level(linkability_score),
        ambiguity_score,
        confidence_margin: margin,
        score_breakdown: CipherscanLinkabilityScoreBreakdown {
            amount_similarity: cipherscan_linkability_amount_similarity_score(
                pair.shield.amount_zat.abs_diff(pair.deshield.amount_zat),
            ),
            time_proximity: cipherscan_linkability_time_score(
                pair.deshield
                    .block_time_unix_seconds
                    .saturating_sub(pair.shield.block_time_unix_seconds),
            ),
            amount_rarity: cipherscan_linkability_rarity_score(pair.occurrence_count),
            weird_amount: cipherscan_linkability_weird_amount_score(
                pair.shield.amount_zat,
                pair.occurrence_count,
            ),
            pool_match: u32::from(pair.shield.pool == pair.deshield.pool) * 4,
            ambiguity_penalty,
        },
    })
}

fn transaction_linkability_json(
    source: &CipherscanLinkabilityFlow,
    candidates: &[CipherscanLinkabilityCandidate],
) -> Value {
    let highest_score = candidates
        .first()
        .map_or(0, |candidate| candidate.linkability_score);
    json!({
        "success": true,
        "txid": source.transaction_id,
        "flowType": cipherscan_flow_direction(source.direction as i32).unwrap_or("unknown"),
        "amount": zec_from_unsigned_zatoshis(source.amount_zat),
        "amountZat": source.amount_zat,
        "blockHeight": source.block_height,
        "blockTime": source.block_time_unix_seconds,
        "pool": cipherscan_flow_pool(source.pool as i32).unwrap_or("unknown"),
        "hasShieldedActivity": true,
        "transparentAddresses": source.transparent_addresses,
        "linkedTransactions": candidates.iter().map(cipherscan_linkability_candidate_json).collect::<Vec<_>>(),
        "totalMatches": candidates.len(),
        "warningLevel": cipherscan_linkability_warning_level(highest_score),
        "highestScore": highest_score,
        "algorithm": {
            "version": "2.0",
            "note": "Scores combine amount similarity, timing, amount rarity, weird-amount detection, and ambiguity penalties.",
        },
    })
}

fn cipherscan_linkability_candidate_json(candidate: &CipherscanLinkabilityCandidate) -> Value {
    json!({
        "txid": candidate.flow.transaction_id,
        "flowType": cipherscan_flow_direction(candidate.flow.direction as i32).unwrap_or("unknown"),
        "amount": zec_from_unsigned_zatoshis(candidate.flow.amount_zat),
        "amountZat": candidate.flow.amount_zat,
        "blockHeight": candidate.flow.block_height,
        "blockTime": candidate.flow.block_time_unix_seconds,
        "pool": cipherscan_flow_pool(candidate.flow.pool as i32).unwrap_or("unknown"),
        "timeDelta": cipherscan_linkability_time_delta(candidate.time_delta_seconds),
        "timeDeltaSeconds": candidate.time_delta_seconds,
        "linkabilityScore": candidate.linkability_score,
        "warningLevel": candidate.warning_level,
        "ambiguityScore": candidate.ambiguity_score,
        "confidenceMargin": candidate.confidence_margin,
        "transparentAddresses": candidate.flow.transparent_addresses,
        "scoreBreakdown": {
            "amountSimilarity": candidate.score_breakdown.amount_similarity,
            "timeProximity": candidate.score_breakdown.time_proximity,
            "amountRarity": candidate.score_breakdown.amount_rarity,
            "weirdAmount": candidate.score_breakdown.weird_amount,
            "poolMatch": candidate.score_breakdown.pool_match,
            "ambiguityPenalty": candidate.score_breakdown.ambiguity_penalty,
        },
    })
}

fn cipherscan_linkability_amount_similarity_score(amount_difference_zat: u64) -> u32 {
    match amount_difference_zat {
        0 => 35,
        1..=10_000 => 32,
        10_001..=50_000 => 28,
        50_001..=100_000 => 24,
        100_001..=250_000 => 16,
        250_001..=500_000 => 8,
        _ => 0,
    }
}

fn cipherscan_linkability_time_score(seconds: i64) -> u32 {
    match seconds {
        ..900 => 25,
        900..3_600 => 20,
        3_600..7_200 => 16,
        7_200..21_600 => 12,
        21_600..86_400 => 8,
        86_400..259_200 => 5,
        259_200..604_800 => 3,
        _ => 1,
    }
}

fn cipherscan_linkability_rarity_score(occurrence_count: u32) -> u32 {
    match occurrence_count {
        0 | 1 => 24,
        2 | 3 => 20,
        4..=10 => 15,
        11..=25 => 10,
        26..=100 => 4,
        _ => 0,
    }
}

fn cipherscan_linkability_weird_amount_score(amount_zat: u64, occurrence_count: u32) -> u32 {
    let roundness_score = cipherscan_linkability_roundness_score(amount_zat);
    match (occurrence_count, roundness_score) {
        (0..=3, 0) => 18,
        (4..=10, 0) => 12,
        (0..=3, _) => 6,
        (4..=10, _) => 2,
        _ => 0,
    }
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Cipherscan scores display-denominated round amounts with JavaScript numbers."
)]
fn cipherscan_linkability_roundness_score(amount_zat: u64) -> u32 {
    let amount_zec = amount_zat as f64 / ZATOSHIS_PER_ZEC;
    for (minimum, multiple, score) in [
        (1_000.0, 1_000.0, 14),
        (500.0, 500.0, 12),
        (100.0, 100.0, 10),
        (50.0, 50.0, 8),
        (10.0, 10.0, 6),
        (1.0, 1.0, 4),
    ] {
        let remainder = amount_zec.rem_euclid(multiple);
        if amount_zec >= minimum && remainder.min(multiple - remainder) <= 0.001 {
            return score;
        }
    }
    0
}

fn cipherscan_linkability_ambiguity_score(candidate_count: u32, margin: u32) -> u32 {
    let count_penalty = candidate_count.saturating_sub(1).saturating_mul(12);
    let margin_penalty = match margin {
        12.. => 0,
        8..=11 => 8,
        4..=7 => 16,
        _ => 24,
    };
    count_penalty.saturating_add(margin_penalty).min(100)
}

fn cipherscan_linkability_warning_level(score: u32) -> &'static str {
    if score >= LINKABILITY_HIGH_WARNING_SCORE {
        "HIGH"
    } else if score >= LINKABILITY_MEDIUM_WARNING_SCORE {
        "MEDIUM"
    } else {
        "LOW"
    }
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "Cipherscan formats human-readable time deltas with JavaScript Math.round."
)]
fn cipherscan_linkability_time_delta(seconds: i64) -> String {
    let absolute_seconds = seconds.saturating_abs();
    let suffix = if seconds > 0 { "after" } else { "before" };
    if absolute_seconds < 60 {
        return format!("{absolute_seconds} seconds {suffix}");
    }
    if absolute_seconds < 3_600 {
        return format!(
            "{} minutes {suffix}",
            (absolute_seconds as f64 / 60.0).round() as u64
        );
    }
    if absolute_seconds < 86_400 {
        return format!(
            "{} hours {suffix}",
            (absolute_seconds as f64 / 3_600.0).round() as u64
        );
    }
    if absolute_seconds < 604_800 {
        return format!("{:.1} days {suffix}", absolute_seconds as f64 / 86_400.0);
    }
    format!(
        "{} weeks {suffix}",
        (absolute_seconds as f64 / 604_800.0).round() as u64
    )
}

fn transaction_linkability_not_found_json() -> Value {
    json!({
        "error": "Transaction not found",
        "code": "TX_NOT_FOUND",
    })
}

fn privacy_linkage_edges_json(limit: u32, offset: u32) -> Value {
    json!({
        "success": true,
        "edges": [],
        "pagination": {
            "total": 0,
            "limit": limit,
            "offset": offset,
            "returned": 0,
            "hasMore": false,
        },
        "degraded": true,
        "unavailable": [
            "Privacy linkage edges require Cipherscan shielded-flow sidecar analytics."
        ],
    })
}

fn privacy_batch_risks_json(limit: u32, period: &str) -> Value {
    json!({
        "success": true,
        "patterns": [],
        "pagination": {
            "total": 0,
            "returned": 0,
            "hasMore": false,
            "nextCursor": Value::Null,
        },
        "stats": {
            "total": 0,
            "highRisk": 0,
            "mediumRisk": 0,
            "lowRisk": 0,
            "totalZecFlagged": 0.0,
            "period": period,
            "filteredTotal": 0,
        },
        "algorithm": {
            "version": "2.0",
            "description": "Precomputed batch clusters with amount, timing, conservation, and ambiguity scoring",
        },
        "limit": limit,
        "degraded": true,
        "unavailable": [
            "Batch-risk scoring requires Cipherscan shielded-flow sidecar analytics."
        ],
    })
}

fn privacy_clusters_json(limit: u32) -> Value {
    json!({
        "success": true,
        "clusters": [],
        "pagination": {
            "total": 0,
            "returned": 0,
            "hasMore": false,
            "nextCursor": Value::Null,
        },
        "limit": limit,
        "degraded": true,
        "unavailable": [
            "Privacy clusters require Cipherscan shielded-flow sidecar analytics."
        ],
    })
}

fn privacy_graph_json(transaction_id: &str) -> Value {
    json!({
        "success": true,
        "txid": transaction_id,
        "nodes": [],
        "edges": [],
        "clusters": [],
        "degraded": true,
        "unavailable": [
            "Privacy graph edges require Cipherscan shielded-flow sidecar analytics."
        ],
    })
}

fn privacy_patterns_json(limit: u32, offset: u32) -> Value {
    json!({
        "success": true,
        "patterns": [],
        "pagination": {
            "total": 0,
            "limit": limit,
            "offset": offset,
            "returned": 0,
            "hasMore": false,
        },
        "note": "Legacy detected_patterns view. Prefer /api/privacy/clusters for the new linkage pipeline.",
        "degraded": true,
        "unavailable": [
            "Legacy detected pattern storage is Cipherscan sidecar data and is not served by Zinder core."
        ],
    })
}

fn privacy_common_amounts_json(period: &str, chain: Option<&str>) -> Value {
    json!({
        "success": true,
        "period": period,
        "chain": chain,
        "totalTransactions": 0,
        "amounts": [],
        "tip": chain.map_or_else(
            || String::from(
                "Using common amounts helps you blend in with other transactions, making linkability analysis harder."
            ),
            |chain| format!(
                "Amounts that blend in on both the {} and Zcash sides require Cipherscan cross-chain sidecar analytics.",
                chain.to_ascii_uppercase()
            ),
        ),
        "degraded": true,
        "unavailable": [
            "Common-amount analytics require Cipherscan shielded-flow sidecar analytics."
        ],
    })
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "The legacy JSON contract represents ZEC amounts and percentages as decimal numbers."
)]
fn common_amounts_json(
    period: &str,
    rows: &[explorer::ValuePoolFlowRoundedAmountSummaryRow],
    thresholds: &[explorer::ValuePoolFlowAmountThresholdSummaryRow],
) -> Result<Value, CipherscanRestError> {
    let [threshold] = thresholds else {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "common_amounts.thresholds",
        ));
    };
    if threshold.minimum_amount_zat != COMMON_AMOUNTS_MINIMUM_ZAT {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "common_amounts.thresholds.minimum_amount_zat",
        ));
    }
    validate_blend_rounded_amount_rows(rows)?;
    let matching_events = threshold
        .shield_event_count
        .checked_add(threshold.deshield_event_count)
        .ok_or(CipherscanRestError::InvalidUpstreamField(
            "common_amounts.total_transactions",
        ))?;
    let total_transactions = matching_events.max(1);
    let amounts = rows
        .iter()
        .map(|row| {
            let count = row
                .shield_event_count
                .checked_add(row.deshield_event_count)
                .ok_or(CipherscanRestError::InvalidUpstreamField(
                    "common_amounts.amounts.tx_count",
                ))?;
            let percentage = count as f64 / total_transactions as f64 * 100.0;
            let blending_score = (count as f64 / total_transactions as f64 * 1_000.0)
                .round()
                .min(100.0) as u64;
            Ok(json!({
                "amountZec": row.rounded_amount_zat as f64 / ZATOSHIS_PER_ZEC,
                "txCount": count,
                "percentage": format!("{percentage:.1}"),
                "blendingScore": blending_score,
            }))
        })
        .collect::<Result<Vec<_>, CipherscanRestError>>()?;

    Ok(json!({
        "success": true,
        "period": period,
        "chain": Value::Null,
        "totalTransactions": total_transactions,
        "amounts": amounts,
        "tip": "Using common amounts helps you blend in with other transactions, making linkability analysis harder.",
    }))
}

fn require_common_amounts_context(
    rounded: &ValuePoolFlowRoundedAmountSummaryResponse,
    threshold: &ValuePoolFlowAmountThresholdSummaryResponse,
) -> Result<(), CipherscanRestError> {
    let rounded_epoch = explorer_chain_epoch_id(rounded.freshness.as_ref()).ok_or(
        CipherscanRestError::MissingUpstreamField("common_amounts.rounded.freshness.chain_epoch"),
    )?;
    let threshold_epoch = explorer_chain_epoch_id(threshold.freshness.as_ref()).ok_or(
        CipherscanRestError::MissingUpstreamField("common_amounts.threshold.freshness.chain_epoch"),
    )?;
    if rounded_epoch != threshold_epoch {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "common_amounts.freshness.chain_epoch",
        ));
    }
    for (coverage, field) in [
        (
            rounded.coverage.as_ref(),
            "common_amounts.rounded.coverage.requested_range_complete",
        ),
        (
            threshold.coverage.as_ref(),
            "common_amounts.threshold.coverage.requested_range_complete",
        ),
    ] {
        if !coverage.is_some_and(|coverage| coverage.requested_range_complete) {
            return Err(CipherscanRestError::MissingUpstreamField(field));
        }
    }
    Ok(())
}

fn privacy_recommended_swap_amounts_json(chain: &str, token: &str) -> Value {
    json!({
        "success": true,
        "chain": chain,
        "token": token,
        "recommendations": [],
        "tip": "Cross-chain swap data is being collected. Recommendations coming soon.",
        "degraded": true,
        "unavailable": [
            "Swap amount recommendations require Cipherscan cross-chain sidecar analytics."
        ],
    })
}

fn protocol_stats_json(
    summary: &TransactionComponentSummaryResponse,
    commitment_tree_sizes: &VisibleTipCommitmentTreeSizes,
) -> Value {
    let totals = summary.totals.as_ref().copied().unwrap_or_default();
    let mut totals_by_month = BTreeMap::<String, explorer::TransactionComponentTotals>::new();
    for day in &summary.days {
        let month = protocol_month_from_unix_seconds(day.day_start_unix_seconds);
        let month_totals = totals_by_month.entry(month).or_default();
        if let Some(day_totals) = day.totals.as_ref() {
            add_transaction_component_totals(month_totals, day_totals);
        }
    }
    let mut cumulative = explorer::TransactionComponentTotals::default();
    let history = totals_by_month
        .into_iter()
        .map(|(month, month_totals)| {
            add_transaction_component_totals(&mut cumulative, &month_totals);
            protocol_history_row_json(&month, &cumulative)
        })
        .collect::<Vec<_>>();
    let coverage_complete = transaction_component_coverage_complete(summary);
    json!({
        "success": true,
        "available": coverage_complete,
        "current": {
            "saplingCommitments": totals.sapling_output_count,
            "saplingNullifiers": totals.sapling_spend_count,
            "orchardCommitments": totals.orchard_action_count,
            "orchardNullifiers": totals.orchard_action_count,
            "ironwoodCommitments": totals.ironwood_action_count,
            "ironwoodNullifiers": totals.ironwood_action_count,
        },
        "history": history,
        "timestamp": current_unix_millis(),
        "visibleTipCommitmentTreeSizes": {
            "chainEpochId": commitment_tree_sizes.chain_epoch_id.to_string(),
            "blockHeight": commitment_tree_sizes.block_height,
            "blockHash": commitment_tree_sizes.block_hash,
            "sapling": commitment_tree_sizes.sapling_commitment_tree_size,
            "orchard": commitment_tree_sizes.orchard_commitment_tree_size,
            "ironwood": commitment_tree_sizes.ironwood_commitment_tree_size,
        },
        "coverage": transaction_component_coverage_json(summary),
        "source": CIPHERSCAN_ADAPTER_SOURCE,
        "degraded": !coverage_complete,
        "unavailable": (!coverage_complete).then_some(
            "Cumulative component history is still backfilling its contiguous canonical range."
        ),
    })
}

fn add_transaction_component_totals(
    target: &mut explorer::TransactionComponentTotals,
    source: &explorer::TransactionComponentTotals,
) {
    macro_rules! add_fields {
        ($($field:ident),+ $(,)?) => {
            $(target.$field = target.$field.saturating_add(source.$field);)+
        };
    }
    add_fields!(
        transaction_count,
        transparent_input_count,
        transparent_output_count,
        sapling_spend_count,
        sapling_output_count,
        orchard_action_count,
        ironwood_action_count,
        sprout_joinsplit_count,
        sapling_transaction_count,
        orchard_transaction_count,
        ironwood_transaction_count,
        sprout_transaction_count,
        sapling_or_orchard_transaction_count,
        sapling_without_orchard_transaction_count,
        orchard_without_sapling_transaction_count,
        sapling_and_orchard_transaction_count,
        sapling_or_orchard_fully_shielded_transaction_count,
    );
}

fn protocol_month_from_unix_seconds(unix_seconds: i64) -> String {
    let Ok(timestamp) = OffsetDateTime::from_unix_timestamp(unix_seconds) else {
        return String::from("1970-01-01T00:00:00.000Z");
    };
    let date = timestamp.date();
    format!(
        "{:04}-{:02}-01T00:00:00.000Z",
        date.year(),
        u8::from(date.month())
    )
}

fn protocol_history_row_json(month: &str, totals: &explorer::TransactionComponentTotals) -> Value {
    json!({
        "month": month,
        "saplingCommitments": totals.sapling_output_count,
        "saplingNullifiers": totals.sapling_spend_count,
        "orchardCommitments": totals.orchard_action_count,
        "orchardNullifiers": totals.orchard_action_count,
        "ironwoodCommitments": totals.ironwood_action_count,
        "ironwoodNullifiers": totals.ironwood_action_count,
    })
}

#[derive(Debug, Default, Clone, Copy)]
struct UsageClockBucket {
    transaction_count: u32,
    block_count: u32,
}

fn usage_clock_json(
    period: &str,
    distribution: &explorer::BlockActivityDistributionResponse,
) -> Value {
    let mut buckets = vec![UsageClockBucket::default(); 7 * 24];
    let mut hourly = [0_u32; 24];
    for bucket in &distribution.buckets {
        let Ok(day_of_week) = usize::try_from(bucket.weekday) else {
            continue;
        };
        let Ok(hour) = usize::try_from(bucket.hour) else {
            continue;
        };
        if day_of_week >= 7 || hour >= 24 {
            continue;
        }
        let bucket_index = day_of_week * 24 + hour;
        let transaction_count = u32::try_from(bucket.transaction_count).unwrap_or(u32::MAX);

        buckets[bucket_index].block_count = buckets[bucket_index]
            .block_count
            .saturating_add(bucket.block_count);
        buckets[bucket_index].transaction_count = buckets[bucket_index]
            .transaction_count
            .saturating_add(transaction_count);
        hourly[hour] = hourly[hour].saturating_add(transaction_count);
    }

    let peak_hour = usage_clock_peak_hour(&hourly);
    let low_hour = usage_clock_low_hour(&hourly);
    let peak_to_low_ratio = usage_clock_peak_to_low_ratio(hourly[peak_hour], hourly[low_hour]);
    let requested_block_count = distribution
        .end_height
        .saturating_sub(distribution.start_height)
        .saturating_add(1);
    let mut unavailable = vec![String::from(
        "The adapter aggregates the requested Zinder block-summary window; complete usage-clock history needs a durable block-activity materialized view.",
    )];
    if distribution.missing_block_count > 0 {
        unavailable.push(format!(
            "The requested height range contains {} unavailable block-summary rows.",
            distribution.missing_block_count
        ));
    }

    json!({
        "period": period,
        "dateRange": {
            "from": distribution
                .first_block_time_unix_seconds
                .and_then(iso8601_date_from_unix_seconds),
            "to": distribution
                .last_block_time_unix_seconds
                .and_then(iso8601_date_from_unix_seconds),
        },
        "totalBlocks": distribution.materialized_block_count,
        "totalTxs": distribution.transaction_count,
        "heatmap": usage_clock_heatmap_json(&buckets),
        "hourly": usage_clock_hourly_json(&hourly),
        "peakHour": peak_hour,
        "lowHour": low_hour,
        "peakToLowRatio": peak_to_low_ratio,
        "source": CIPHERSCAN_ADAPTER_SOURCE,
        "sampledBlockLimit": requested_block_count,
        "startHeight": distribution.start_height,
        "endHeight": distribution.end_height,
        "materializedBlockCount": distribution.materialized_block_count,
        "missingBlockCount": distribution.missing_block_count,
        "degraded": true,
        "unavailable": unavailable,
    })
}

fn usage_clock_heatmap_json(buckets: &[UsageClockBucket]) -> Vec<Value> {
    let mut cells = Vec::with_capacity(7 * 24);
    for day_of_week in 0_u8..7 {
        for hour in 0_u8..24 {
            let bucket = buckets[usize::from(day_of_week) * 24 + usize::from(hour)];
            cells.push(json!({
                "hour": hour,
                "dow": day_of_week,
                "txCount": bucket.transaction_count,
                "blockCount": bucket.block_count,
            }));
        }
    }
    cells
}

fn usage_clock_hourly_json(hourly: &[u32; 24]) -> Vec<Value> {
    (0_u8..24)
        .map(|hour| {
            json!({
                "hour": hour,
                "txCount": hourly[usize::from(hour)],
            })
        })
        .collect()
}

fn block_timestamp(block_time_unix_seconds: i64) -> Option<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp(block_time_unix_seconds).ok()
}

fn iso8601_date_from_unix_seconds(block_time_unix_seconds: i64) -> Option<String> {
    block_timestamp(block_time_unix_seconds).map(|timestamp| timestamp.date().to_string())
}

fn usage_clock_peak_hour(hourly: &[u32; 24]) -> usize {
    hourly
        .iter()
        .enumerate()
        .max_by_key(|(_, transaction_count)| *transaction_count)
        .map_or(0, |(hour, _)| hour)
}

fn usage_clock_low_hour(hourly: &[u32; 24]) -> usize {
    hourly
        .iter()
        .enumerate()
        .min_by_key(|(_, transaction_count)| *transaction_count)
        .map_or(0, |(hour, _)| hour)
}

fn usage_clock_peak_to_low_ratio(peak_transactions: u32, low_transactions: u32) -> f64 {
    if low_transactions == 0 {
        return 0.0;
    }

    ((f64::from(peak_transactions) / f64::from(low_transactions)) * 100.0).round() / 100.0
}

fn usage_clock_block_limit(period: &str) -> u32 {
    usage_clock_period_days(period).map_or(MAX_USAGE_CLOCK_BLOCKS, |days| {
        days.saturating_mul(TARGET_BLOCKS_PER_DAY)
            .clamp(1, MAX_USAGE_CLOCK_BLOCKS)
    })
}

fn usage_clock_period_days(period: &str) -> Option<u32> {
    match period {
        "30d" => Some(30),
        "90d" => Some(90),
        "6m" => Some(183),
        "all" => None,
        _ => Some(365),
    }
}

fn name_events_json(name: &str) -> Value {
    json!({
        "events": [],
        "total": 0,
        "name": name,
        "degraded": true,
        "unavailable": [
            "ZNS event history requires the Cipherscan ZNS sidecar."
        ],
    })
}

fn crosslink_divergence_history_json() -> Value {
    json!({
        "success": true,
        "count": 0,
        "openEvent": Value::Null,
        "events": [],
        "degraded": true,
        "unavailable": [
            "Crosslink divergence history requires Cipherscan sidecar telemetry or a native Crosslink consensus surface."
        ],
    })
}

fn crosslink_bootstrap_info_json() -> Value {
    json!({
        "success": true,
        "available": false,
        "degraded": true,
        "unavailable": [
            "Crosslink bootstrap snapshot metadata is Cipherscan deployment sidecar data and is not served by Zinder core."
        ],
    })
}

fn reorg_stats_json(
    snapshot: &ChainReorgHistorySnapshot,
    archived_block_count: Option<u64>,
) -> Value {
    let observed_reverted_blocks = reorg_observed_reverted_blocks(snapshot);
    let deepest_reorg = reorg_deepest_reorg(snapshot);

    json!({
        "success": true,
        "totalOrphanedBlocks": archived_block_count,
        "observedRevertedBlocks": observed_reverted_blocks,
        "totalForkEvents": reorg_total_fork_events(snapshot),
        "reportsLast24h": Value::Null,
        "deepestReorg": deepest_reorg,
        "degraded": true,
        "unavailable": reorg_stats_unavailable_reasons(snapshot, archived_block_count),
    })
}

async fn fetch_displaced_block_page(
    adapter: &CipherscanRestAdapter,
    limit: u32,
    offset: u32,
) -> Result<DisplacedBlockHistoryResponse, CipherscanRestError> {
    const NATIVE_PAGE_SIZE: u32 = 4_096;
    const MAX_NATIVE_PAGES: usize = 16;

    let mut cursor = Vec::new();
    let mut remaining_offset = u64::from(offset);
    let mut selected = Vec::new();
    let mut first_response = None;
    for _ in 0..MAX_NATIVE_PAGES {
        let response = adapter
            .explorer_client()
            .displaced_block_history(DisplacedBlockHistoryRequest {
                page_size: NATIVE_PAGE_SIZE,
                cursor: cursor.clone(),
            })
            .await?
            .into_inner();
        if first_response.is_none() {
            if remaining_offset >= response.total_count {
                return Ok(DisplacedBlockHistoryResponse {
                    entries: Vec::new(),
                    has_more: false,
                    next_cursor: Vec::new(),
                    ..response
                });
            }
            first_response = Some(response.clone());
        }
        for entry in response.entries {
            if remaining_offset > 0 {
                remaining_offset -= 1;
            } else if selected.len() < usize::try_from(limit).unwrap_or(usize::MAX) {
                selected.push(entry);
            }
        }
        if selected.len() >= usize::try_from(limit).unwrap_or(usize::MAX) || !response.has_more {
            let mut output = first_response.ok_or(CipherscanRestError::MissingUpstreamField(
                "displaced_block_history.first_response",
            ))?;
            output.entries = selected;
            output.has_more = u64::from(offset)
                .saturating_add(u64::try_from(output.entries.len()).unwrap_or(u64::MAX))
                < output.total_count;
            output.next_cursor = Vec::new();
            return Ok(output);
        }
        if response.next_cursor.is_empty() {
            return Err(CipherscanRestError::MissingUpstreamField(
                "displaced_block_history.next_cursor",
            ));
        }
        cursor = response.next_cursor;
    }
    Err(CipherscanRestError::InvalidRequest(
        "offset exceeds the displaced-block compatibility scan bound".to_owned(),
    ))
}

fn non_canonical_blocks_json(
    network: Network,
    limit: u32,
    offset: u32,
    history: &DisplacedBlockHistoryResponse,
) -> Result<Value, CipherscanRestError> {
    let page = offset.saturating_div(limit).saturating_add(1);
    let total_pages = history
        .total_count
        .saturating_add(u64::from(limit).saturating_sub(1))
        .checked_div(u64::from(limit))
        .unwrap_or(0);
    let blocks = history
        .entries
        .iter()
        .map(|entry| {
            let block = entry
                .block
                .as_ref()
                .ok_or(CipherscanRestError::MissingUpstreamField(
                    "displaced_block_history.entries.block",
                ))?;
            displaced_block_json(network, block, entry.current_canonical_block.as_ref())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(json!({
        "success": true,
        "orphanedBlocks": blocks,
        "pagination": {
            "total": history.total_count,
            "limit": limit,
            "offset": offset,
            "totalPages": total_pages,
            "page": page,
            "hasMore": history.has_more,
        },
        "degraded": true,
        "unavailable": [
            "Displaced-block archive coverage begins at its activation event; earlier reorged blocks cannot be reconstructed from the current best chain."
        ],
    }))
}

fn non_canonical_block_detail_json(
    network: Network,
    detail: &explorer::DisplacedBlockDetailResponse,
    reorgs: &ChainReorgHistorySnapshot,
) -> Result<Value, CipherscanRestError> {
    let block = detail
        .block
        .as_ref()
        .ok_or(CipherscanRestError::MissingUpstreamField(
            "displaced_block_detail.block",
        ))?;
    let event = reorgs
        .events
        .iter()
        .find(|event| event.event_sequence == block.displacement_event_sequence);
    let mut row = displaced_block_json(network, block, detail.current_canonical_block.as_ref())?;
    let object = row
        .as_object_mut()
        .ok_or(CipherscanRestError::InvalidUpstreamField(
            "displaced_block_detail.block",
        ))?;
    object.insert(
        "forkDepth".to_owned(),
        event
            .and_then(reorg_reverted_block_count)
            .map_or(Value::Null, Value::from),
    );
    object.insert(
        "forkDescription".to_owned(),
        event.map_or(Value::Null, |event| {
            reorg_description(event.reverted.as_ref(), event.committed.as_ref())
        }),
    );
    Ok(json!({
        "success": true,
        "block": row,
        "degraded": reorgs.is_projection_unavailable || reorgs.is_truncated,
        "unavailable": reorg_snapshot_unavailable_reasons(reorgs),
    }))
}

fn displaced_block_json(
    network: Network,
    block: &explorer::DisplacedBlockSummary,
    canonical: Option<&explorer::DisplacedBlockCanonicalCounterpart>,
) -> Result<Value, CipherscanRestError> {
    let miner_address = block
        .coinbase_outputs
        .iter()
        .find_map(|output| cipherscan_transparent_address(network, &output.script_pub_key));
    let canonical_block = canonical
        .map(|canonical| canonical_counterpart_json(network, canonical))
        .transpose()?;
    let canonical_hash = canonical.map(|canonical| canonical.block_hash.as_str());
    Ok(json!({
        "id": displaced_block_compat_id(block),
        "height": block.block_height,
        "hash": block.block_hash,
        "canonicalHash": canonical_hash,
        "timestamp": block.block_time_unix_seconds,
        "transactionCount": block.transaction_ids.len(),
        "size": block.total_size_bytes,
        "difficulty": cipherscan_difficulty(network, block.difficulty_bits)?.to_string(),
        "minerAddress": miner_address,
        "minerPool": Value::Null,
        "previousBlockHash": block.previous_block_hash,
        "source": CIPHERSCAN_ADAPTER_SOURCE,
        "reportedBy": Value::Null,
        "consensusValid": Value::Null,
        "detectedAt": rfc3339_millis(block.displaced_at_millis),
        "forkEventId": block.displacement_event_sequence,
        "canonicalBlock": canonical_block,
    }))
}

fn canonical_counterpart_json(
    network: Network,
    block: &explorer::DisplacedBlockCanonicalCounterpart,
) -> Result<Value, CipherscanRestError> {
    let miner_address = block
        .coinbase_outputs
        .iter()
        .find_map(|output| cipherscan_transparent_address(network, &output.script_pub_key));
    Ok(json!({
        "hash": block.block_hash,
        "height": block.block_height,
        "timestamp": block.block_time_unix_seconds,
        "transactionCount": block.transaction_count,
        "size": block.total_size_bytes,
        "difficulty": cipherscan_difficulty(network, block.difficulty_bits)?.to_string(),
        "minerAddress": miner_address,
        "minerPool": Value::Null,
        "minerPoolUrl": Value::Null,
        "minerPoolRegion": Value::Null,
    }))
}

fn displaced_block_compat_id(block: &explorer::DisplacedBlockSummary) -> u64 {
    block
        .displacement_event_sequence
        .saturating_mul(100_000_001)
        .saturating_add(u64::from(block.block_height))
}

fn reorg_forks_with_archive_json(
    network: Network,
    snapshot: &ChainReorgHistorySnapshot,
    limit: u32,
    offset: u32,
    archive: Option<&DisplacedBlockHistoryResponse>,
) -> Result<Value, CipherscanRestError> {
    let offset_usize = usize::try_from(offset).map_or(usize::MAX, |offset| offset);
    let limit_usize = usize::try_from(limit).map_or(usize::MAX, |limit| limit);
    let total = reorg_total_fork_events(snapshot);
    let forks: Vec<Value> = snapshot
        .events
        .iter()
        .rev()
        .skip(offset_usize)
        .take(limit_usize)
        .map(|event| reorg_fork_with_archive_json(network, event, archive))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(json!({
        "success": true,
        "forks": forks,
        "pagination": {
            "total": total,
            "limit": limit,
            "offset": offset,
            "hasMore": total.is_some_and(|total| offset_usize.saturating_add(limit_usize) < total)
                || snapshot.is_truncated,
        },
        "degraded": reorg_snapshot_is_degraded(snapshot),
        "unavailable": reorg_snapshot_unavailable_reasons(snapshot),
    }))
}

fn reorg_fork_with_archive_json(
    network: Network,
    event: &explorer::ChainReorgHistoryEvent,
    archive: Option<&DisplacedBlockHistoryResponse>,
) -> Result<Value, CipherscanRestError> {
    let reverted = event.reverted.as_ref();
    let committed = event.committed.as_ref();
    let fork_height = reverted
        .map(|range| range.start_height)
        .or_else(|| committed.map(|range| range.start_height));
    let depth = reorg_reverted_block_count(event);
    let canonical_tip = committed
        .map(|range| range.end_height)
        .or_else(|| event.visible_tip.as_ref().map(|tip| tip.height));

    let comparisons = archive.map_or_else(
        || Ok(Vec::new()),
        |archive| {
            archive
                .entries
                .iter()
                .filter_map(|entry| {
                    entry
                        .block
                        .as_ref()
                        .filter(|block| block.displacement_event_sequence == event.event_sequence)
                        .map(|block| (block, entry.current_canonical_block.as_ref()))
                })
                .map(|(block, canonical)| {
                    Ok(json!({
                        "height": block.block_height,
                        "orphaned": displaced_block_json(
                            network,
                            block,
                            canonical,
                        )?,
                        "canonical": canonical
                            .map(|block| canonical_counterpart_json(network, block))
                            .transpose()?,
                    }))
                })
                .collect::<Result<Vec<_>, CipherscanRestError>>()
        },
    )?;
    let comparisons_complete = u32::try_from(comparisons.len()).ok() == depth;
    Ok(json!({
        "id": event.event_sequence,
        "forkHeight": fork_height,
        "depth": depth,
        "canonicalTip": canonical_tip,
        "orphanedCount": depth,
        "source": CIPHERSCAN_ADAPTER_SOURCE,
        "description": reorg_description(reverted, committed),
        "detectedAt": rfc3339_millis(event.chain_epoch_created_at_millis),
        "resolvedAt": Value::Null,
        "comparisons": comparisons,
        "degraded": !comparisons_complete,
        "unavailable": if comparisons_complete {
            Vec::<&str>::new()
        } else {
            reorg_fork_unavailable_reasons(event, archive)
        },
    }))
}

fn reorg_nodes_json() -> Value {
    json!({
        "success": true,
        "nodes": [],
        "summary": {
            "total": 0,
            "online": 0,
            "forking": 0,
            "lastPoll": Value::Null,
        },
        "degraded": true,
        "unavailable": [
            "Monitored external node status is Cipherscan fork-monitor sidecar data and is not stored in Zinder core."
        ],
    })
}

fn reorg_reverted_block_count(event: &explorer::ChainReorgHistoryEvent) -> Option<u32> {
    event
        .reverted
        .as_ref()
        .map(|range| inclusive_height_count(range.start_height, range.end_height))
}

fn inclusive_height_count(start_height: u32, end_height: u32) -> u32 {
    if end_height < start_height {
        return 0;
    }

    end_height.saturating_sub(start_height).saturating_add(1)
}

fn reorg_description(
    reverted: Option<&wallet::ChainRangeReverted>,
    committed: Option<&wallet::ChainEpochCommitted>,
) -> Value {
    let Some(reverted) = reverted else {
        return Value::Null;
    };
    let Some(committed) = committed else {
        return json!(format!(
            "Chain reorg detected by Zinder: reverted heights {}-{}",
            reverted.start_height, reverted.end_height
        ));
    };

    json!(format!(
        "Chain reorg detected by Zinder: replaced heights {}-{} with canonical heights {}-{}",
        reverted.start_height, reverted.end_height, committed.start_height, committed.end_height
    ))
}

fn reorg_snapshot_is_degraded(snapshot: &ChainReorgHistorySnapshot) -> bool {
    snapshot.is_projection_unavailable || snapshot.is_truncated
}

fn reorg_observed_reverted_blocks(snapshot: &ChainReorgHistorySnapshot) -> Option<u32> {
    if snapshot.is_projection_unavailable || snapshot.is_truncated {
        return None;
    }

    snapshot.events.iter().try_fold(0_u32, |total, event| {
        reorg_reverted_block_count(event).map(|count| total.saturating_add(count))
    })
}

fn reorg_deepest_reorg(snapshot: &ChainReorgHistorySnapshot) -> Option<u32> {
    if snapshot.is_projection_unavailable || snapshot.is_truncated {
        return None;
    }

    snapshot.events.iter().try_fold(0_u32, |deepest, event| {
        reorg_reverted_block_count(event).map(|count| deepest.max(count))
    })
}

fn reorg_total_fork_events(snapshot: &ChainReorgHistorySnapshot) -> Option<usize> {
    (!snapshot.is_projection_unavailable && !snapshot.is_truncated).then_some(snapshot.events.len())
}

fn reorg_stats_unavailable_reasons(
    snapshot: &ChainReorgHistorySnapshot,
    archived_block_count: Option<u64>,
) -> Vec<&'static str> {
    let mut reasons = reorg_snapshot_unavailable_reasons(snapshot);
    reasons.push("Public tip-report activity is not retained by ChainReorgHistory.");
    if archived_block_count.is_none() {
        reasons.push("The displaced-block archive is not available from this deployment.");
    } else {
        reasons.push(
            "Displaced-block archive totals begin at archive activation and exclude earlier reorg incidents.",
        );
    }
    if snapshot.events.iter().any(|event| event.reverted.is_none()) {
        reasons.push("At least one reorg incident does not include its reverted height range.");
    }
    reasons
}

fn reorg_snapshot_unavailable_reasons(snapshot: &ChainReorgHistorySnapshot) -> Vec<&'static str> {
    let mut reasons = Vec::new();
    if snapshot.is_projection_unavailable {
        reasons.push("ChainReorgHistory is not available from this Zinder explorer deployment.");
    }
    if snapshot.is_truncated {
        reasons.push("This response is based on the first retained reorg-history page; totals may be lower than the full materialized view.");
    }
    reasons
}

fn reorg_fork_unavailable_reasons(
    event: &explorer::ChainReorgHistoryEvent,
    archive: Option<&DisplacedBlockHistoryResponse>,
) -> Vec<&'static str> {
    let archive_reason = match archive {
        None => "The displaced-block archive is not available from this deployment.",
        Some(archive) if archive.coverage.is_none() => {
            "The displaced-block archive is enabled but has not captured its activation event."
        }
        Some(archive)
            if archive.coverage.as_ref().is_some_and(|coverage| {
                event.event_sequence < coverage.activation_event_sequence
            }) =>
        {
            "This reorg incident predates displaced-block archive activation."
        }
        Some(_) => "The displaced-block archive does not contain every block from this incident.",
    };
    let mut reasons = vec![
        archive_reason,
        "Fork resolution time is not retained by ChainReorgHistory.",
    ];
    if event.reverted.is_none() {
        reasons.push("The incident does not include its reverted height range.");
    }
    reasons
}

fn network_fees_json(
    summary: &explorer::FeeSummaryResponse,
    start_height: u32,
    end_height: u32,
) -> Value {
    json!({
        "success": true,
        "fees": {
            "low": zec_from_zatoshis(ZIP317_SIMPLE_TX_FEE_ZAT),
            "standard": zec_from_zatoshis(ZIP317_TYPICAL_SHIELDED_TX_FEE_ZAT),
            "high": zec_from_zatoshis(ZIP317_COMPLEX_TX_FEE_ZAT),
        },
        "unit": "ZEC",
        "zip317": {
            "marginalFee": ZIP317_MARGINAL_FEE_ZAT,
            "graceActions": ZIP317_GRACE_ACTIONS,
            "p2pkhStandardFee": ZIP317_SIMPLE_TX_FEE_ZAT,
            "formula": "max(marginal_fee * max(grace_actions, logical_actions), p2pkh_standard_fee)",
        },
        "note": "Fees follow ZIP-317 proportional fee mechanism. Actual fee depends on the number of logical actions in the transaction.",
        "timestamp": OffsetDateTime::now_utc().unix_timestamp().saturating_mul(1_000),
        "observedZip317": {
            "startHeight": start_height,
            "endHeight": end_height,
            "blockCount": summary.block_count,
            "transactionCount": summary.transaction_count,
            "totalConventionalFeeZat": summary.total_zip317_conventional_fee_zat.to_string(),
            "minConventionalFeeZat": summary.min_zip317_conventional_fee_zat.to_string(),
            "maxConventionalFeeZat": summary.max_zip317_conventional_fee_zat.to_string(),
            "source": CIPHERSCAN_ADAPTER_SOURCE,
            "semantics": "ZIP-317 conventional fee floors, not miner-collected paid fees.",
        },
    })
}

fn peer_inventory_json() -> Value {
    json!({
        "success": true,
        "count": 0,
        "peers": [],
        "timestamp": OffsetDateTime::now_utc().unix_timestamp().saturating_mul(1_000),
        "degraded": true,
        "unavailable": [
            "Peer inventory is node-local crawler/RPC state and is not a Zinder core chain fact."
        ],
    })
}

fn node_locations_json() -> Value {
    json!({
        "success": true,
        "locations": [],
        "timestamp": OffsetDateTime::now_utc().unix_timestamp().saturating_mul(1_000),
        "degraded": true,
        "unavailable": [
            "Node locations are crawler sidecar data and are not a Zinder core chain fact."
        ],
    })
}

fn node_statistics_json() -> Value {
    json!({
        "success": true,
        "stats": {
            "activeNodes": 0,
            "totalNodes": 0,
            "countries": 0,
            "cities": 0,
            "avgPingMs": Value::Null,
            "torNodes": 0,
            "lastUpdated": Value::Null,
        },
        "trends": {
            "change24h": Value::Null,
            "change7d": Value::Null,
            "change30d": Value::Null,
        },
        "topCountries": [],
        "timestamp": OffsetDateTime::now_utc().unix_timestamp().saturating_mul(1_000),
        "degraded": true,
        "unavailable": [
            "Node statistics are crawler sidecar data and are not a Zinder core chain fact."
        ],
    })
}

fn node_history_json(period: &str) -> Value {
    json!({
        "success": true,
        "period": period,
        "snapshots": [],
        "timestamp": OffsetDateTime::now_utc().unix_timestamp().saturating_mul(1_000),
        "degraded": true,
        "unavailable": [
            "Node-count history is crawler sidecar data and is not a Zinder core chain fact."
        ],
    })
}

#[derive(Clone, Copy, Debug)]
struct MiningBlockSample {
    block_height: u32,
    block_time_unix_seconds: i64,
    difficulty: f64,
    transaction_fees_zec: f64,
    transaction_count: f64,
}

#[derive(Clone, Copy, Debug, Default)]
struct MiningMetricValues {
    solrate: f64,
    difficulty: f64,
    block_time_seconds: f64,
    transaction_fees_zec: f64,
    transaction_count: f64,
}

#[derive(Clone, Copy, Debug)]
struct MiningMetricPoint {
    block_height: u32,
    values: MiningMetricValues,
}

#[derive(Debug)]
struct MiningBlockSampleSet {
    samples: Vec<MiningBlockSample>,
    paid_fee_block_count: u32,
    conventional_fee_block_count: u32,
}

fn mining_metrics_json(
    network: Network,
    window: u32,
    response: &explorer::BlockProductionSeriesResponse,
) -> Result<Value, CipherscanRestError> {
    let sample_set = mining_block_samples(network, response)?;
    let points = rolling_mining_metric_points(&sample_set.samples, window);
    let latest = points.last().map_or_else(
        || MiningMetricValues {
            block_time_seconds: f64::from(DEFAULT_MINING_BLOCK_INTERVAL_SECONDS),
            ..Default::default()
        },
        |point| point.values,
    );
    let mut unavailable = Vec::new();
    if response.missing_block_count > 0 {
        unavailable.push(
            "Some requested block-production rows were unavailable or did not match the pinned canonical epoch.",
        );
    }
    if sample_set.conventional_fee_block_count > 0 {
        unavailable.push(
            "Some txFees points use ZIP-317 conventional fee floors because actual paid block fees are unavailable.",
        );
    }
    let is_degraded = !unavailable.is_empty();
    let requested_block_count = response
        .end_height
        .saturating_sub(response.start_height)
        .saturating_add(1);

    Ok(json!({
        "success": true,
        "window": window,
        "latest": mining_metric_values_json(latest),
        "points": points.into_iter().map(mining_metric_point_json).collect::<Vec<_>>(),
        "coverage": {
            "startHeight": response.start_height,
            "endHeight": response.end_height,
            "requestedBlocks": requested_block_count,
            "coveredBlocks": response.covered_block_count,
            "missingBlocks": response.missing_block_count,
            "paidFeeBlocks": sample_set.paid_fee_block_count,
            "conventionalFeeBlocks": sample_set.conventional_fee_block_count,
        },
        "degraded": is_degraded,
        "unavailable": unavailable,
    }))
}

fn mining_block_samples(
    network: Network,
    response: &explorer::BlockProductionSeriesResponse,
) -> Result<MiningBlockSampleSet, CipherscanRestError> {
    let mut samples = Vec::with_capacity(response.points.len());
    let mut paid_fee_block_count = 0_u32;
    let mut conventional_fee_block_count = 0_u32;
    for point in &response.points {
        let summary = point
            .summary
            .as_ref()
            .ok_or(CipherscanRestError::MissingUpstreamField(
                "block_production_point.summary",
            ))?;
        let transaction_fees_zat = summary.paid_fees_collected_zat.map_or_else(
            || {
                conventional_fee_block_count = conventional_fee_block_count.saturating_add(1);
                summary.fees_collected_zat
            },
            |paid_fees_collected_zat| {
                paid_fee_block_count = paid_fee_block_count.saturating_add(1);
                paid_fees_collected_zat
            },
        );
        samples.push(MiningBlockSample {
            block_height: summary.block_height,
            block_time_unix_seconds: summary.block_time_unix_seconds,
            difficulty: cipherscan_difficulty(network, point.bits)?,
            transaction_fees_zec: zec_from_unsigned_zatoshis(transaction_fees_zat),
            transaction_count: f64::from(summary.transaction_count),
        });
    }

    Ok(MiningBlockSampleSet {
        samples,
        paid_fee_block_count,
        conventional_fee_block_count,
    })
}

fn rolling_mining_metric_points(
    samples: &[MiningBlockSample],
    window: u32,
) -> Vec<MiningMetricPoint> {
    let window = usize::try_from(window).unwrap_or(usize::MAX);
    let mut sample_values = Vec::with_capacity(samples.len());
    for (index, sample) in samples.iter().enumerate() {
        let block_interval_seconds = index
            .checked_sub(1)
            .map(|previous_index| {
                sample
                    .block_time_unix_seconds
                    .saturating_sub(samples[previous_index].block_time_unix_seconds)
            })
            .and_then(|interval| u32::try_from(interval).ok())
            .filter(|interval| *interval > 0 && *interval < MAX_VALID_MINING_BLOCK_INTERVAL_SECONDS)
            .unwrap_or(DEFAULT_MINING_BLOCK_INTERVAL_SECONDS);
        sample_values.push(MiningMetricValues {
            solrate: sample.difficulty / f64::from(block_interval_seconds),
            difficulty: sample.difficulty,
            block_time_seconds: f64::from(block_interval_seconds),
            transaction_fees_zec: sample.transaction_fees_zec,
            transaction_count: sample.transaction_count,
        });
    }

    sample_values
        .iter()
        .enumerate()
        .map(|(index, _)| {
            let start = index.saturating_add(1).saturating_sub(window);
            MiningMetricPoint {
                block_height: samples[index].block_height,
                values: average_mining_metric_values(&sample_values[start..=index]),
            }
        })
        .collect()
}

fn average_mining_metric_values(values: &[MiningMetricValues]) -> MiningMetricValues {
    let sample_count = f64::from(u32::try_from(values.len()).unwrap_or(u32::MAX));
    let totals = values
        .iter()
        .fold(MiningMetricValues::default(), |mut totals, metric| {
            totals.solrate += metric.solrate;
            totals.difficulty += metric.difficulty;
            totals.block_time_seconds += metric.block_time_seconds;
            totals.transaction_fees_zec += metric.transaction_fees_zec;
            totals.transaction_count += metric.transaction_count;
            totals
        });
    MiningMetricValues {
        solrate: totals.solrate / sample_count,
        difficulty: totals.difficulty / sample_count,
        block_time_seconds: totals.block_time_seconds / sample_count,
        transaction_fees_zec: totals.transaction_fees_zec / sample_count,
        transaction_count: totals.transaction_count / sample_count,
    }
}

fn mining_metric_values_json(values: MiningMetricValues) -> Value {
    json!({
        "solrate": values.solrate,
        "difficulty": values.difficulty,
        "blockTime": values.block_time_seconds,
        "txFees": values.transaction_fees_zec,
        "txCount": values.transaction_count,
    })
}

fn mining_metric_point_json(point: MiningMetricPoint) -> Value {
    let mut body = mining_metric_values_json(point.values);
    if let Some(fields) = body.as_object_mut() {
        fields.insert("height".to_owned(), json!(point.block_height));
    }
    body
}

fn mining_metrics_window(window: Option<&str>) -> u32 {
    parse_cipherscan_bounded_integer(
        window,
        DEFAULT_MINING_METRICS_WINDOW,
        MIN_MINING_METRICS_WINDOW,
        MAX_MINING_METRICS_WINDOW,
    )
}

fn mining_metrics_limit(limit: Option<&str>) -> u32 {
    parse_cipherscan_bounded_integer(
        limit,
        DEFAULT_MINING_METRICS_LIMIT,
        MIN_MINING_METRICS_LIMIT,
        MAX_MINING_METRICS_LIMIT,
    )
}

fn parse_cipherscan_bounded_integer(
    raw_value: Option<&str>,
    default: i64,
    minimum: i64,
    maximum: i64,
) -> u32 {
    let parsed = raw_value
        .and_then(parse_cipherscan_integer)
        .unwrap_or(default);
    let nonzero = if parsed == 0 { default } else { parsed };
    u32::try_from(nonzero.clamp(minimum, maximum)).unwrap_or(u32::MAX)
}

fn parse_cipherscan_integer(raw_value: &str) -> Option<i64> {
    let trimmed = raw_value.trim_start();
    let (is_negative, digits) = match trimmed.as_bytes().first() {
        Some(b'-') => (true, &trimmed[1..]),
        Some(b'+') => (false, &trimmed[1..]),
        Some(_) => (false, trimmed),
        None => return None,
    };
    let mut digit_count = 0_usize;
    let magnitude =
        digits
            .bytes()
            .take_while(u8::is_ascii_digit)
            .fold(0_i64, |magnitude, digit| {
                digit_count = digit_count.saturating_add(1);
                magnitude
                    .saturating_mul(10)
                    .saturating_add(i64::from(digit - b'0'))
            });
    if digit_count == 0 {
        return None;
    }
    Some(if is_negative {
        magnitude.saturating_neg()
    } else {
        magnitude
    })
}

fn reorg_page_limit(raw_limit: Option<&str>, default_limit: u32, maximum_limit: u32) -> u32 {
    parse_cipherscan_bounded_integer(
        raw_limit,
        i64::from(default_limit),
        1,
        i64::from(maximum_limit),
    )
}

fn reorg_page_offset(raw_offset: Option<&str>) -> u32 {
    let offset = raw_offset.and_then(parse_cipherscan_integer).unwrap_or(0);
    u32::try_from(offset.max(0)).unwrap_or(u32::MAX)
}

fn rich_list_limit(limit: Option<&str>) -> u32 {
    parse_cipherscan_bounded_integer(limit, 100, 1, 500)
}

fn rich_list_offset(offset: Option<&str>) -> u64 {
    let parsed = offset.and_then(parse_cipherscan_integer).unwrap_or(0);
    u64::try_from(parsed.max(0)).unwrap_or(u64::MAX)
}

fn mining_pool_distribution_json(
    period: &str,
    window: &MiningPoolDistributionWindow,
) -> Result<Value, CipherscanRestError> {
    let mut payout_rows = window.payout_by_address.iter().collect::<Vec<_>>();
    payout_rows.sort_unstable_by(|(left_address, left), (right_address, right)| {
        right
            .block_count
            .cmp(&left.block_count)
            .then_with(|| left_address.cmp(right_address))
    });
    let mut pool_index_by_name = HashMap::<&'static str, usize>::new();
    let mut pool_aggregates = Vec::<CipherscanMiningPoolAggregate>::new();
    for (address, payout) in payout_rows {
        let name = get_pool_name(address.as_deref()).unwrap_or("Unknown");
        if let Some(index) = pool_index_by_name.get(name).copied() {
            let pool = &mut pool_aggregates[index];
            pool.block_count = pool.block_count.checked_add(payout.block_count).ok_or(
                CipherscanRestError::InvalidUpstreamField(
                    "block_production_in_time_range.covered_block_count",
                ),
            )?;
            pool.total_fees_zat = pool
                .total_fees_zat
                .checked_add(payout.total_fees_zat)
                .ok_or(CipherscanRestError::InvalidUpstreamField(
                    "block_production_in_time_range.points.summary.paid_fees_collected_zat",
                ))?;
        } else {
            pool_index_by_name.insert(name, pool_aggregates.len());
            pool_aggregates.push(CipherscanMiningPoolAggregate {
                address: address.clone(),
                name,
                block_count: payout.block_count,
                total_fees_zat: payout.total_fees_zat,
            });
        }
    }
    let total_blocks = pool_aggregates.iter().try_fold(0_u32, |total, pool| {
        total
            .checked_add(pool.block_count)
            .ok_or(CipherscanRestError::InvalidUpstreamField(
                "block_production_in_time_range.covered_block_count",
            ))
    })?;
    pool_aggregates.sort_unstable_by_key(|pool| std::cmp::Reverse(pool.block_count));
    let pools = pool_aggregates
        .into_iter()
        .map(|pool| {
            json!({
                "address": pool.address,
                "name": pool.name,
                "blocks": pool.block_count,
                "share": if total_blocks == 0 {
                    0.0
                } else {
                    f64::from(pool.block_count) / f64::from(total_blocks)
                },
                "totalFeesZat": pool.total_fees_zat.to_string(),
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "period": period,
        "totalBlocks": total_blocks,
        "pools": pools,
        "generatedAt": rfc3339_timestamp(window.generated_at),
    }))
}

fn mining_pool_ranking_json(period: &str) -> Value {
    json!({
        "period": period,
        "totalBlocks": 0,
        "ranking": [],
        "generatedAt": current_rfc3339_timestamp(),
        "degraded": true,
        "unavailable": [
            "Mining pool ranking is Cipherscan sidecar data and is not a Zinder core chain fact."
        ],
    })
}

fn mining_hashrate_share_json(period: &str) -> Value {
    json!({
        "period": period,
        "series": [],
        "allPools": [],
        "generatedAt": current_rfc3339_timestamp(),
        "degraded": true,
        "unavailable": [
            "Mining hashrate share is Cipherscan sidecar data and is not a Zinder core chain fact."
        ],
    })
}

fn miner_behavior_json(period: &str) -> Value {
    json!({
        "period": period,
        "series": [],
        "summary": Value::Null,
        "message": "Miner behavior data is being computed. Check back soon.",
        "generatedAt": current_rfc3339_timestamp(),
        "degraded": true,
        "unavailable": [
            "Miner behavior and reward movement classification are Cipherscan sidecar data and are not Zinder core chain facts."
        ],
    })
}

fn zodl_leaderboard_json(period: &str) -> Value {
    json!({
        "period": period,
        "pools": [],
        "summary": Value::Null,
        "message": "Miner behavior data is being computed. Check back soon.",
        "generatedAt": current_rfc3339_timestamp(),
        "degraded": true,
        "unavailable": [
            "ZODL leaderboard rankings are Cipherscan sidecar analytics and are not Zinder core chain facts."
        ],
    })
}

#[derive(Debug, Default)]
struct MiningRewardDay {
    blocks: u32,
    total_fees_zat: u64,
    total_coinbase_zat: u64,
}

#[derive(Debug, Default)]
struct MiningPayoutAggregate {
    block_count: u32,
    total_fees_zat: u64,
}

struct MiningPoolDistributionWindow {
    payout_by_address: BTreeMap<Option<String>, MiningPayoutAggregate>,
    generated_at: OffsetDateTime,
}

struct CipherscanMiningPoolAggregate {
    address: Option<String>,
    name: &'static str,
    block_count: u32,
    total_fees_zat: u64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct MiningProductionPeriod {
    days: Option<u32>,
}

impl MiningProductionPeriod {
    fn cutoff_unix_seconds(self, generated_at_unix_seconds: i64) -> Option<i64> {
        self.days.map(|days| {
            generated_at_unix_seconds
                .saturating_sub(i64::from(days).saturating_mul(UNIX_SECONDS_PER_DAY))
        })
    }
}

fn mining_production_period(period: &str) -> MiningProductionPeriod {
    MiningProductionPeriod {
        days: match period {
            "24h" => Some(1),
            "3d" => Some(3),
            "30d" => Some(30),
            "90d" => Some(90),
            "6m" => Some(180),
            "1y" => Some(365),
            // This includes `all`: Cipherscan maps it to null and then applies
            // `|| map['7d']`, so the production route uses seven days.
            _ => Some(7),
        },
    }
}

struct MiningProductionWindow {
    entries: Vec<CipherscanBlockListEntry>,
    requested_cutoff_unix_seconds: Option<i64>,
    covered_from_unix_seconds: Option<i64>,
    covered_through_unix_seconds: Option<i64>,
    scanned_block_count: u32,
    coverage_complete: bool,
    generated_at: OffsetDateTime,
}

fn validate_mining_pool_distribution_page(
    response: &explorer::BlockProductionInTimeRangeResponse,
    expected_fence: Option<&explorer::BlockProductionTimeRangeReadFence>,
) -> Result<(), CipherscanRestError> {
    let coverage = response
        .coverage
        .as_ref()
        .ok_or(CipherscanRestError::MissingUpstreamField(
            "block_production_in_time_range.coverage",
        ))?;
    let read_fence =
        response
            .read_fence
            .as_ref()
            .ok_or(CipherscanRestError::MissingUpstreamField(
                "block_production_in_time_range.read_fence",
            ))?;
    if !coverage.requested_range_complete
        || response.missing_block_count != 0
        || response.missing_coinbase_count != 0
        || response.missing_paid_fee_count != 0
        || u32::try_from(response.points.len()).ok() != Some(response.covered_block_count)
    {
        return Err(CipherscanRestError::MissingUpstreamField(
            "block_production_in_time_range.complete_points",
        ));
    }
    if expected_fence.is_some_and(|expected| expected != read_fence)
        || read_fence.materialized_view_tip.as_ref().is_none_or(|tip| {
            !is_rpc_hash(&tip.hash)
                || coverage.complete_through_height != Some(tip.height)
                || coverage
                    .complete_from_height
                    .is_none_or(|height| height > 1)
        })
    {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "block_production_in_time_range.read_fence",
        ));
    }
    Ok(())
}

fn mining_rewards_json(period: &str, window: &MiningProductionWindow) -> Value {
    let mut rewards_by_date = BTreeMap::<String, MiningRewardDay>::new();
    let mut paid_fee_block_count = 0_u32;
    let mut conventional_fee_block_count = 0_u32;

    for entry in &window.entries {
        let summary = &entry.summary;
        let Some(block_date) = mining_reward_block_date(summary.block_time_unix_seconds) else {
            continue;
        };
        let bucket = rewards_by_date.entry(block_date).or_default();
        bucket.blocks = bucket.blocks.saturating_add(1);
        let fee_zat = summary.paid_fees_collected_zat.map_or_else(
            || {
                conventional_fee_block_count = conventional_fee_block_count.saturating_add(1);
                summary.fees_collected_zat
            },
            |paid_fee_zat| {
                paid_fee_block_count = paid_fee_block_count.saturating_add(1);
                paid_fee_zat
            },
        );
        bucket.total_fees_zat = bucket.total_fees_zat.saturating_add(fee_zat);
        bucket.total_coinbase_zat = bucket
            .total_coinbase_zat
            .saturating_add(summary.coinbase_reward_zat);
    }

    let series: Vec<Value> = rewards_by_date
        .into_iter()
        .map(|(date, day)| {
            json!({
                "date": date,
                "blocks": day.blocks,
                "totalFeesZat": day.total_fees_zat.to_string(),
                "totalCoinbaseZat": day.total_coinbase_zat.to_string(),
            })
        })
        .collect();

    let fee_basis = match (paid_fee_block_count, conventional_fee_block_count) {
        (_, 0) => "paid",
        (0, _) => "zip317_conventional",
        _ => "mixed_paid_and_zip317_conventional",
    };
    let degraded = !window.coverage_complete || conventional_fee_block_count > 0;
    let mut unavailable = Vec::new();
    if !window.coverage_complete {
        unavailable
            .push("The requested period extends beyond the adapter's bounded block-summary scan.");
    }
    if conventional_fee_block_count > 0 {
        unavailable.push(
            "Actual paid fees are unavailable for some blocks; their ZIP-317 conventional fee floors are reported instead.",
        );
    }

    json!({
        "period": period,
        "series": series,
        "generatedAt": rfc3339_timestamp(window.generated_at),
        "source": CIPHERSCAN_ADAPTER_SOURCE,
        "coinbaseBasis": "transparent_outputs",
        "feeBasis": fee_basis,
        "coverage": {
            "requestedCutoff": window.requested_cutoff_unix_seconds.map(unix_timestamp_json),
            "coveredFrom": window.covered_from_unix_seconds.map(unix_timestamp_json),
            "coveredThrough": window.covered_through_unix_seconds.map(unix_timestamp_json),
            "scannedBlocks": window.scanned_block_count,
            "includedBlocks": window.entries.len(),
            "paidFeeBlocks": paid_fee_block_count,
            "conventionalFeeBlocks": conventional_fee_block_count,
            "complete": window.coverage_complete,
        },
        "degraded": degraded,
        "unavailable": unavailable,
    })
}

fn unix_timestamp_json(unix_seconds: i64) -> String {
    OffsetDateTime::from_unix_timestamp(unix_seconds)
        .map_or_else(|_| String::from("1970-01-01T00:00:00Z"), rfc3339_timestamp)
}

fn mining_reward_block_date(block_time_unix_seconds: i64) -> Option<String> {
    OffsetDateTime::from_unix_timestamp(block_time_unix_seconds)
        .ok()
        .map(|timestamp| timestamp.date().to_string())
}

fn validate_mining_production_page(
    page: &[CipherscanBlockListEntry],
    start_height: u32,
    end_height: u32,
) -> Result<(), CipherscanRestError> {
    let expected_count = end_height.saturating_sub(start_height).saturating_add(1);
    if u32::try_from(page.len()).ok() != Some(expected_count)
        || page.iter().enumerate().any(|(offset, entry)| {
            start_height.checked_add(u32::try_from(offset).unwrap_or(u32::MAX))
                != Some(entry.summary.block_height)
        })
    {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "block_production_series.coverage",
        ));
    }
    Ok(())
}

fn mining_production_entry_is_in_period(
    entry: &CipherscanBlockListEntry,
    requested_cutoff_unix_seconds: Option<i64>,
    requested_end_time_unix_seconds: i64,
) -> bool {
    requested_cutoff_unix_seconds
        .is_none_or(|cutoff| entry.summary.block_time_unix_seconds >= cutoff)
        && entry.summary.block_time_unix_seconds < requested_end_time_unix_seconds
}

fn mining_production_page_is_before_cutoff(
    page: &[CipherscanBlockListEntry],
    requested_cutoff_unix_seconds: Option<i64>,
) -> bool {
    requested_cutoff_unix_seconds.is_some_and(|cutoff| {
        page.iter()
            .all(|entry| entry.summary.block_time_unix_seconds < cutoff)
    })
}

fn pool_flows_json(
    request: &CipherscanPoolFlowRequest,
    summary: &explorer::ValuePoolFlowSummaryResponse,
    coverage: &explorer::ValuePoolFlowCoverage,
) -> Result<Value, CipherscanRestError> {
    let points = summary
        .buckets
        .iter()
        .map(|bucket| value_pool_flow_summary_point_json(bucket, request))
        .collect::<Result<Vec<_>, _>>()?;
    let degraded = !coverage.requested_range_complete;
    let unavailable = if degraded {
        vec![
            "The requested wall-clock range is not covered by one contiguous value-pool flow materialized view.",
        ]
    } else {
        Vec::new()
    };
    Ok(json!({
        "success": true,
        "period": request.period,
        "pool": request.pool,
        "granularity": request.resolution.response_name(),
        "format": request.amount_format.response_name(),
        "points": points,
        "coverage": value_pool_flow_coverage_json(coverage),
        "degraded": degraded,
        "unavailable": unavailable,
    }))
}

fn cipherscan_flow_start_time(
    end_time_unix_seconds: i64,
    request: &CipherscanPoolFlowRequest,
) -> Result<i64, CipherscanRestError> {
    let rolling_start_time_unix_seconds = end_time_unix_seconds
        .checked_sub(request.days.saturating_mul(UNIX_SECONDS_PER_DAY))
        .ok_or(CipherscanRestError::InvalidRequest(
            "value-pool flow period is outside the supported timestamp range".to_owned(),
        ))?;
    Ok(match request.resolution {
        CipherscanFlowResolution::Hourly => rolling_start_time_unix_seconds,
        CipherscanFlowResolution::Daily => {
            rolling_start_time_unix_seconds.div_euclid(UNIX_SECONDS_PER_DAY) * UNIX_SECONDS_PER_DAY
        }
    })
}

fn value_pool_flow_summary_point_json(
    bucket: &explorer::ValuePoolFlowSummaryBucket,
    request: &CipherscanPoolFlowRequest,
) -> Result<Value, CipherscanRestError> {
    if bucket
        .bucket_start_time_unix_seconds
        .rem_euclid(request.resolution.bucket_seconds())
        != 0
    {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "value_pool_flow_summary.buckets.bucket_start_time_unix_seconds",
        ));
    }
    let timestamp = OffsetDateTime::from_unix_timestamp(bucket.bucket_start_time_unix_seconds)
        .map_err(|_| {
            CipherscanRestError::InvalidUpstreamField(
                "value_pool_flow_summary.buckets.bucket_start_time_unix_seconds",
            )
        })?;
    let date = match request.resolution {
        CipherscanFlowResolution::Hourly => cipherscan_timestamp_with_millis(timestamp),
        CipherscanFlowResolution::Daily => timestamp.date().to_string(),
    };
    let net_zat = i128::from(bucket.shield_amount_zat) - i128::from(bucket.deshield_amount_zat);
    Ok(json!({
        "date": date,
        "shield": value_pool_flow_amount_json(bucket.shield_amount_zat, request.amount_format),
        "deshield": value_pool_flow_amount_json(bucket.deshield_amount_zat, request.amount_format),
        "shieldTx": bucket.shield_event_count,
        "deshieldTx": bucket.deshield_event_count,
        "net": value_pool_flow_net_amount_json(net_zat, request.amount_format),
    }))
}

fn value_pool_flow_amount_json(amount_zat: u64, format: CipherscanFlowAmountFormat) -> Value {
    match format {
        CipherscanFlowAmountFormat::Zatoshi => json!(amount_zat.to_string()),
        CipherscanFlowAmountFormat::Zec => json!(zec_from_unsigned_zatoshis(amount_zat)),
    }
}

#[allow(
    clippy::cast_precision_loss,
    reason = "The Cipherscan ZEC response contract uses JSON numbers for ZEC amounts."
)]
fn value_pool_flow_net_amount_json(net_zat: i128, format: CipherscanFlowAmountFormat) -> Value {
    match format {
        CipherscanFlowAmountFormat::Zatoshi => json!(net_zat.to_string()),
        CipherscanFlowAmountFormat::Zec => json!((net_zat as f64) / ZATOSHIS_PER_ZEC),
    }
}

fn value_pool_history_page_size(period: &str) -> u32 {
    match period {
        "7d" => 8,
        "30d" => 31,
        "1y" => 366,
        "all" => 4_096,
        _ => 91,
    }
}

fn emission_history_json_arrays(
    history: &ValuePoolBalanceHistoryResponse,
    period: &str,
) -> Result<(Vec<Value>, Vec<Value>), CipherscanRestError> {
    let cutoff_day = value_pool_history_cutoff_day(period)?;
    let mut history_points = history
        .points
        .iter()
        .filter(|point| cutoff_day.is_none_or(|cutoff| point.day_start_unix_seconds >= cutoff))
        .map(|point| {
            validate_history_value_pools(&point.pools)?;
            let chain_supply_zat = total_optional_u64(
                point.pools.iter().map(|pool| pool.value_zat),
            )?
            .ok_or(CipherscanRestError::MissingUpstreamField(
                "value_pool_balance_history.points.pools.value_zat",
            ))?;
            let date = OffsetDateTime::from_unix_timestamp(point.day_start_unix_seconds).map_err(
                |_| {
                    CipherscanRestError::InvalidUpstreamField(
                        "value_pool_balance_history.points.day_start_unix_seconds",
                    )
                },
            )?;
            Ok((
                cipherscan_timestamp_with_millis(date),
                point.block_height,
                chain_supply_zat,
            ))
        })
        .collect::<Result<Vec<_>, CipherscanRestError>>()?;
    history_points.reverse();

    let supply_history = history_points
        .iter()
        .map(|(date, block_height, chain_supply_zat)| {
            json!({
                "date": date,
                "circulating": zec_from_unsigned_zatoshis(*chain_supply_zat),
                "height": block_height,
            })
        })
        .collect::<Vec<_>>();
    let daily_emission = history_points
        .windows(2)
        .filter_map(|pair| {
            let (_, _, previous_supply_zat) = &pair[0];
            let (date, _, current_supply_zat) = &pair[1];
            current_supply_zat
                .checked_sub(*previous_supply_zat)
                .filter(|emission_zat| *emission_zat > 0)
                .map(|emission_zat| {
                    json!({
                        "date": date,
                        "emission": zec_from_unsigned_zatoshis(emission_zat),
                    })
                })
        })
        .collect::<Vec<_>>();

    Ok((supply_history, daily_emission))
}

fn emission_json(
    subsidy_summary: &ChainSubsidySummary,
    supply_summary: &ChainSupplySummary,
    current_tip: &wallet::BlockTip,
    history: &ValuePoolBalanceHistoryResponse,
    period: &str,
) -> Result<Value, CipherscanRestError> {
    let history_tip = explorer_visible_tip(history.freshness.as_ref()).ok_or(
        CipherscanRestError::MissingUpstreamField(
            "value_pool_balance_history.freshness.chain_view.chain_epoch.visible_tip",
        ),
    )?;
    if history_tip != current_tip {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "value_pool_balance_history.freshness.chain_view.chain_epoch.visible_tip",
        ));
    }

    let history_is_complete = value_pool_history_is_historically_complete(history)?;
    if !history_is_complete {
        return Ok(json!({
            "success": true,
            "degraded": true,
            "maxSupply": MAX_SUPPLY_ZEC,
            "circulating": supply_summary.chain_supply_zec,
            "remaining": supply_summary.remaining_supply_zec,
            "circulatingPct": supply_summary.circulating_pct,
            "dailyEmissionEstimate": subsidy_summary.daily_emission_estimate_zec,
            "supplyHistory": [],
            "dailyEmission": [],
            "hasChainSnapshots": false,
            "supplyHistorySource": "none",
            "unavailable": [
                "Historical emission arrays are incomplete while canonical value-pool history is backfilling."
            ],
        }));
    }
    let (supply_history, daily_emission) = emission_history_json_arrays(history, period)?;
    let supply_history_source = match supply_history.len() {
        0 => "none",
        1 => "partial",
        _ => "history",
    };

    Ok(json!({
        "success": true,
        "maxSupply": MAX_SUPPLY_ZEC,
        "circulating": supply_summary.chain_supply_zec,
        "remaining": supply_summary.remaining_supply_zec,
        "circulatingPct": supply_summary.circulating_pct,
        "dailyEmissionEstimate": subsidy_summary.daily_emission_estimate_zec,
        "supplyHistory": supply_history,
        "dailyEmission": daily_emission,
        "hasChainSnapshots": true,
        "supplyHistorySource": supply_history_source,
    }))
}

fn value_pool_history_json(
    period: &str,
    format: &str,
    history: &ValuePoolBalanceHistoryResponse,
) -> Result<Value, CipherscanRestError> {
    if !value_pool_history_is_historically_complete(history)? {
        return Ok(json!({
            "success": true,
            "period": period,
            "format": format,
            "points": [],
            "hasPoolBreakdown": false,
            "hasVerifiedPerPoolBreakdown": false,
            "degraded": true,
            "unavailable": [
                "Cumulative value-pool history is still backfilling the canonical height domain."
            ],
        }));
    }
    let cutoff_day = value_pool_history_cutoff_day(period)?;
    let mut points = history
        .points
        .iter()
        .filter(|point| cutoff_day.is_none_or(|cutoff| point.day_start_unix_seconds >= cutoff))
        .map(|point| value_pool_history_point_json(point, format))
        .collect::<Result<Vec<_>, _>>()?;
    points.reverse();
    let has_pool_breakdown = history
        .points
        .iter()
        .filter(|point| cutoff_day.is_none_or(|cutoff| point.day_start_unix_seconds >= cutoff))
        .all(value_pool_history_point_has_known_pool_breakdown);
    Ok(json!({
        "success": true,
        "period": period,
        "format": format,
        "points": points,
        "hasPoolBreakdown": has_pool_breakdown,
        "hasVerifiedPerPoolBreakdown": has_pool_breakdown,
    }))
}

fn value_pool_history_is_historically_complete(
    history: &ValuePoolBalanceHistoryResponse,
) -> Result<bool, CipherscanRestError> {
    let coverage = history
        .coverage
        .as_ref()
        .ok_or(CipherscanRestError::MissingUpstreamField(
            "value_pool_balance_history.coverage",
        ))?;
    let settled_tip = explorer_settled_tip(history.freshness.as_ref()).ok_or(
        CipherscanRestError::MissingUpstreamField(
            "value_pool_balance_history.freshness.chain_view.chain_epoch.settled_tip",
        ),
    )?;
    let historical_through = coverage.historical_through_height;
    let contiguous_live_tail = historical_through
        .and_then(|height| height.checked_add(1))
        .zip(coverage.live_tail_from_height)
        .is_some_and(|(next_historical_height, tail_from)| {
            next_historical_height >= tail_from && coverage.live_tail_through_height.is_some()
        });
    Ok(coverage.historical_from_height == Some(1)
        && (historical_through.is_some_and(|height| height >= settled_tip.height)
            || contiguous_live_tail))
}

fn value_pool_history_cutoff_day(period: &str) -> Result<Option<i64>, CipherscanRestError> {
    let days = match period {
        "7d" => Some(7_i64),
        "30d" => Some(30),
        "1y" => Some(365),
        "all" => None,
        _ => Some(90),
    };
    let current_day = calendar_date_start_unix_seconds(OffsetDateTime::now_utc().date());
    days.map(|days| {
        days.checked_mul(86_400)
            .and_then(|seconds| current_day.checked_sub(seconds))
            .ok_or(CipherscanRestError::InvalidUpstreamField(
                "value_pool_history.period",
            ))
    })
    .transpose()
}

fn value_pool_history_point_json(
    point: &explorer::ValuePoolBalanceHistoryPoint,
    format: &str,
) -> Result<Value, CipherscanRestError> {
    validate_history_value_pools(&point.pools)?;
    let sprout = history_pool_value(&point.pools, "sprout");
    let sapling = history_pool_value(&point.pools, "sapling");
    let orchard = history_pool_value(&point.pools, "orchard");
    let ironwood = history_pool_value(&point.pools, "ironwood");
    let transparent = history_pool_value(&point.pools, "transparent");
    let shielded = total_optional_u64([sprout, sapling, orchard, ironwood])?;
    let chain_supply = total_optional_u64(point.pools.iter().map(|pool| pool.value_zat))?;
    let shielded_supply_pct = shielded
        .zip(chain_supply)
        .and_then(|(shielded, supply)| (supply > 0).then(|| zatoshi_percentage(shielded, supply)));
    let timestamp =
        OffsetDateTime::from_unix_timestamp(point.day_start_unix_seconds).map_err(|_| {
            CipherscanRestError::InvalidUpstreamField("value_pool_history.day_start_unix_seconds")
        })?;
    let date = cipherscan_timestamp_with_millis(timestamp);
    let has_pool_breakdown = value_pool_history_point_has_known_pool_breakdown(point);
    if format == "zatoshi" {
        Ok(json!({
            "date": date,
            "shieldedZat": shielded.map(|amount_zat| amount_zat.to_string()),
            "sproutZat": sprout.map(|amount_zat| amount_zat.to_string()),
            "saplingZat": sapling.map(|amount_zat| amount_zat.to_string()),
            "orchardZat": orchard.map(|amount_zat| amount_zat.to_string()),
            "ironwoodZat": ironwood.map(|amount_zat| amount_zat.to_string()),
            "transparentZat": transparent.map(|amount_zat| amount_zat.to_string()),
            "chainSupplyZat": chain_supply.map(|amount_zat| amount_zat.to_string()),
            "shieldedSupplyPct": shielded_supply_pct,
            "hasPoolBreakdown": has_pool_breakdown,
        }))
    } else {
        Ok(json!({
            "date": date,
            "shielded": shielded.map(zec_from_unsigned_zatoshis),
            "sprout": sprout.map(zec_from_unsigned_zatoshis),
            "sapling": sapling.map(zec_from_unsigned_zatoshis),
            "orchard": orchard.map(zec_from_unsigned_zatoshis),
            "ironwood": ironwood.map(zec_from_unsigned_zatoshis),
            "transparent": transparent.map(zec_from_unsigned_zatoshis),
            "chainSupply": chain_supply.map(zec_from_unsigned_zatoshis),
            "shieldedSupplyPct": shielded_supply_pct,
            "hasPoolBreakdown": has_pool_breakdown,
        }))
    }
}

fn value_pool_history_point_has_known_pool_breakdown(
    point: &explorer::ValuePoolBalanceHistoryPoint,
) -> bool {
    ["transparent", "sprout", "sapling", "orchard", "ironwood"]
        .into_iter()
        .all(|id| history_pool_value(&point.pools, id).is_some())
}

fn validate_history_value_pools(
    pools: &[explorer::ValuePoolBalance],
) -> Result<(), CipherscanRestError> {
    let mut ids = HashSet::with_capacity(pools.len());
    for pool in pools {
        if pool.id.is_empty() || !ids.insert(pool.id.as_str()) {
            return Err(CipherscanRestError::InvalidUpstreamField(
                "value_pool_balance_history.points.pools.id",
            ));
        }
    }
    Ok(())
}

fn history_pool_value(pools: &[explorer::ValuePoolBalance], id: &str) -> Option<u64> {
    pools
        .iter()
        .find(|pool| pool.id == id)
        .and_then(|pool| pool.value_zat)
}

fn total_optional_u64(
    values: impl IntoIterator<Item = Option<u64>>,
) -> Result<Option<u64>, CipherscanRestError> {
    values
        .into_iter()
        .try_fold(Some(0_u64), |total, pool_value_zat| {
            match (total, pool_value_zat) {
                (Some(total), Some(pool_value_zat)) => total
                    .checked_add(pool_value_zat)
                    .map(Some)
                    .ok_or(CipherscanRestError::InvalidUpstreamField(
                        "value_pool_balance_history.points.pools.value_zat",
                    )),
                _ => Ok(None),
            }
        })
}

fn chain_size_history_json(period: &str) -> Value {
    json!({
        "success": true,
        "available": false,
        "period": period,
        "points": [],
        "degraded": true,
        "unavailable": [
            "Chain-size history is not exposed by current Zinder native APIs."
        ],
    })
}

#[derive(Debug, Eq, PartialEq)]
struct FeeDistributionPeriod {
    echoed: String,
    days: i64,
}

#[derive(Debug, Eq, PartialEq)]
struct FlowAnalyticsPeriod {
    echoed: String,
    days: Option<i64>,
}

fn flow_analytics_period(requested_period: Option<&str>) -> FlowAnalyticsPeriod {
    let echoed = requested_period
        .filter(|period| !period.is_empty())
        .unwrap_or("30d");
    let days = match echoed {
        "7d" => Some(7),
        "90d" => Some(90),
        "1y" => Some(365),
        "all" => None,
        _ => Some(30),
    };
    FlowAnalyticsPeriod {
        echoed: echoed.to_owned(),
        days,
    }
}

fn common_amounts_period(requested_period: Option<&str>) -> CommonAmountsPeriod {
    let echoed = requested_period
        .filter(|period| !period.is_empty())
        .unwrap_or("7d");
    let seconds = match echoed {
        "24h" => UNIX_SECONDS_PER_DAY,
        "30d" => 30 * UNIX_SECONDS_PER_DAY,
        "90d" => 90 * UNIX_SECONDS_PER_DAY,
        _ => 7 * UNIX_SECONDS_PER_DAY,
    };
    CommonAmountsPeriod {
        echoed: echoed.to_owned(),
        seconds,
    }
}

fn common_amounts_limit(requested_limit: Option<&str>) -> u32 {
    parse_cipherscan_bounded_integer(requested_limit, 10, 1, 50)
}

fn common_amounts_range(period: &CommonAmountsPeriod, now_unix_seconds: i64) -> (i64, i64) {
    let cutoff = now_unix_seconds.saturating_sub(period.seconds);
    // The native lower bound is inclusive; adding one preserves Cipherscan's
    // strict `block_time > cutoff` predicate.
    (cutoff.saturating_add(1), i64::MAX)
}

fn flow_analytics_range(days: Option<i64>, now_unix_seconds: i64) -> (i64, i64) {
    let start_time_unix_seconds = days.map_or(i64::MIN, |days| {
        now_unix_seconds.saturating_sub(days.saturating_mul(UNIX_SECONDS_PER_DAY))
    });
    // The legacy route has no upper timestamp predicate. The native range is
    // half-open, so i64::MAX preserves that unbounded upper range.
    (start_time_unix_seconds, i64::MAX)
}

fn require_complete_flow_analytics_coverage(
    requested_range_complete: bool,
) -> Result<(), CipherscanRestError> {
    if requested_range_complete {
        return Ok(());
    }
    Err(CipherscanRestError::InvalidUpstreamField(
        "value_pool_flow_amount_threshold_summary.coverage.requested_range_complete",
    ))
}

fn anonymity_set_json(
    period: &str,
    thresholds: &[(u64, u64, u64)],
    generated_at: OffsetDateTime,
) -> Result<Value, CipherscanRestError> {
    if thresholds.len() != CIPHERSCAN_ANONYMITY_SET_THRESHOLDS_ZAT.len()
        || thresholds
            .iter()
            .zip(CIPHERSCAN_ANONYMITY_SET_THRESHOLDS_ZAT)
            .any(|((minimum_amount_zat, _, _), expected)| *minimum_amount_zat != expected)
    {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "value_pool_flow_amount_threshold_summary.thresholds",
        ));
    }

    let thresholds = thresholds
        .iter()
        .map(
            |(minimum_amount_zat, shield_event_count, deshield_event_count)| {
                json!({
                    "thresholdZat": minimum_amount_zat,
                    "thresholdZec": zec_from_unsigned_zatoshis(*minimum_amount_zat),
                    "shieldCount": shield_event_count,
                    "deshieldCount": deshield_event_count,
                })
            },
        )
        .collect::<Vec<_>>();
    Ok(json!({
        "period": period,
        "thresholds": thresholds,
        "updatedAt": cipherscan_timestamp_with_millis(generated_at),
    }))
}

fn shielding_distribution_json(
    period: &str,
    thresholds: &[explorer::ValuePoolFlowAmountThresholdSummaryRow],
    generated_at: OffsetDateTime,
) -> Result<Value, CipherscanRestError> {
    if thresholds.len() != CIPHERSCAN_SHIELDING_DISTRIBUTION_BUCKETS.len()
        || thresholds
            .iter()
            .zip(CIPHERSCAN_SHIELDING_DISTRIBUTION_BUCKETS)
            .any(|(threshold, bucket)| threshold.minimum_amount_zat != bucket.minimum_amount_zat)
    {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "value_pool_flow_amount_threshold_summary.thresholds",
        ));
    }

    let buckets = thresholds
        .iter()
        .zip(CIPHERSCAN_SHIELDING_DISTRIBUTION_BUCKETS)
        .enumerate()
        .map(|(index, (lower, bucket))| {
            let upper = thresholds.get(index + 1);
            Ok(json!({
                "label": bucket.label,
                "minZat": bucket.minimum_amount_zat,
                "maxZat": bucket.maximum_amount_zat,
                "shieldCount": subtract_cumulative_flow_total(
                    lower.shield_event_count,
                    upper.map_or(0, |row| row.shield_event_count),
                )?,
                "deshieldCount": subtract_cumulative_flow_total(
                    lower.deshield_event_count,
                    upper.map_or(0, |row| row.deshield_event_count),
                )?,
                "shieldVolumeZat": subtract_cumulative_flow_total(
                    lower.shield_amount_zat,
                    upper.map_or(0, |row| row.shield_amount_zat),
                )?,
                "deshieldVolumeZat": subtract_cumulative_flow_total(
                    lower.deshield_amount_zat,
                    upper.map_or(0, |row| row.deshield_amount_zat),
                )?,
            }))
        })
        .collect::<Result<Vec<_>, CipherscanRestError>>()?;

    Ok(json!({
        "period": period,
        "buckets": buckets,
        "updatedAt": cipherscan_timestamp_with_millis(generated_at),
    }))
}

fn subtract_cumulative_flow_total(lower: u64, upper: u64) -> Result<u64, CipherscanRestError> {
    lower
        .checked_sub(upper)
        .ok_or(CipherscanRestError::InvalidUpstreamField(
            "value_pool_flow_amount_threshold_summary.thresholds.cumulative_totals",
        ))
}

fn fee_distribution_period(requested_period: Option<&str>) -> FeeDistributionPeriod {
    let echoed = requested_period
        .filter(|period| !period.is_empty())
        .unwrap_or("30d");
    let days = match echoed {
        "7d" => 7,
        "90d" => 90,
        "1y" => 365,
        _ => 30,
    };
    FeeDistributionPeriod {
        echoed: echoed.to_owned(),
        days,
    }
}

fn fee_distribution_range(days: i64, now_unix_seconds: i64) -> (i64, i64) {
    let start_time_unix_seconds =
        now_unix_seconds.saturating_sub(days.saturating_mul(UNIX_SECONDS_PER_DAY));
    // Public Cipherscan has no upper timestamp predicate. Canonical block times
    // can lead wall-clock time, so include the same bounded future-time window
    // used by the adapter's other timestamp-indexed materialized views. Native ranges are
    // half-open; the final second preserves inclusive integer timestamps.
    let end_time_unix_seconds = now_unix_seconds
        .saturating_add(COMPONENT_SUMMARY_FUTURE_TIME_MARGIN_SECONDS)
        .saturating_add(1);
    (start_time_unix_seconds, end_time_unix_seconds)
}

fn paid_fee_distribution_json(
    period: &str,
    distribution: &PaidFeeDistributionResponse,
    generated_at: OffsetDateTime,
) -> Result<Value, CipherscanRestError> {
    let mut days = distribution.days.iter().collect::<Vec<_>>();
    days.sort_unstable_by_key(|day| day.day_start_unix_seconds);
    let mut daily = Vec::with_capacity(days.len());
    for day in days {
        if let Some(row) = paid_fee_day_json(day)? {
            daily.push(row);
        }
    }
    let unavailable_transaction_count = distribution
        .days
        .iter()
        .try_fold(0_u64, |total, day| {
            total.checked_add(day.unavailable_transaction_count)
        })
        .ok_or(CipherscanRestError::InvalidUpstreamField(
            "paid_fee_distribution.days.unavailable_transaction_count",
        ))?;
    let coverage_complete = distribution
        .coverage
        .as_ref()
        .is_some_and(|coverage| coverage.requested_range_complete);
    let degraded = !coverage_complete || unavailable_transaction_count > 0;
    let mut unavailable = Vec::new();
    if !coverage_complete {
        unavailable.push("The requested range extends beyond contiguous actual paid-fee history.");
    }
    if unavailable_transaction_count > 0 {
        unavailable.push(
            "Actual paid fees are unavailable for some transactions and are excluded from the distribution.",
        );
    }

    Ok(json!({
        "period": period,
        "daily": daily,
        "updatedAt": cipherscan_timestamp_with_millis(generated_at),
        "source": CIPHERSCAN_ADAPTER_SOURCE,
        "feeBasis": "actual_paid",
        "coverage": distribution.coverage.as_ref().map(|coverage| json!({
            "completeFromHeight": coverage.complete_from_height,
            "completeThroughHeight": coverage.complete_through_height,
            "completeFromTime": coverage.complete_from_time_unix_seconds
                .map(cipherscan_timestamp_from_unix_seconds),
            "completeThroughTime": coverage.complete_through_time_unix_seconds
                .map(cipherscan_timestamp_from_unix_seconds),
            "requestedRangeComplete": coverage.requested_range_complete,
            "unavailableTransactionCount": unavailable_transaction_count,
        })),
        "degraded": degraded,
        "unavailable": unavailable,
    }))
}

fn paid_fee_day_json(
    day: &explorer::PaidFeeDistributionDay,
) -> Result<Option<Value>, CipherscanRestError> {
    let mut frequencies = BTreeMap::<u64, u64>::new();
    for frequency in &day.frequencies {
        if frequency.paid_fee_zat == 0 || frequency.transaction_count == 0 {
            continue;
        }
        let count = frequencies.entry(frequency.paid_fee_zat).or_default();
        *count = count.checked_add(frequency.transaction_count).ok_or(
            CipherscanRestError::InvalidUpstreamField(
                "paid_fee_distribution.days.frequencies.transaction_count",
            ),
        )?;
    }
    fee_distribution_day_json(day.day_start_unix_seconds, frequencies)
}

fn conventional_fee_distribution_json(
    period: &str,
    distribution: &ConventionalFeeDistributionResponse,
    generated_at: OffsetDateTime,
) -> Result<Value, CipherscanRestError> {
    let mut days = distribution.days.iter().collect::<Vec<_>>();
    days.sort_unstable_by_key(|day| day.day_start_unix_seconds);
    let mut daily = Vec::with_capacity(days.len());
    for day in days {
        if let Some(row) = conventional_fee_day_json(day)? {
            daily.push(row);
        }
    }
    let unavailable_transaction_count = distribution
        .days
        .iter()
        .try_fold(0_u64, |total, day| {
            total.checked_add(day.unavailable_transaction_count)
        })
        .ok_or(CipherscanRestError::InvalidUpstreamField(
            "conventional_fee_distribution.days.unavailable_transaction_count",
        ))?;
    let coverage_complete = distribution
        .coverage
        .as_ref()
        .is_some_and(|coverage| coverage.requested_range_complete);
    let degraded = true;
    let mut unavailable = vec![
        "Public Cipherscan aggregates actual paid fees. This fallback contains ZIP-317 conventional fees until Zinder retains the intrinsic value-balance facts required to prove shielded fees.",
    ];
    if !coverage_complete {
        unavailable.push(
            "The requested range extends beyond contiguous ZIP-317 conventional-fee history.",
        );
    }
    if unavailable_transaction_count > 0 {
        unavailable.push(
            "ZIP-317 conventional fees are unavailable for some transactions and are excluded from the distribution.",
        );
    }

    Ok(json!({
        "period": period,
        "daily": daily,
        "updatedAt": cipherscan_timestamp_with_millis(generated_at),
        "source": CIPHERSCAN_ADAPTER_SOURCE,
        "feeBasis": "zip317_conventional_fallback",
        "coverage": distribution.coverage.as_ref().map(|coverage| json!({
            "completeFromHeight": coverage.complete_from_height,
            "completeThroughHeight": coverage.complete_through_height,
            "completeFromTime": coverage.complete_from_time_unix_seconds
                .map(cipherscan_timestamp_from_unix_seconds),
            "completeThroughTime": coverage.complete_through_time_unix_seconds
                .map(cipherscan_timestamp_from_unix_seconds),
            "requestedRangeComplete": coverage.requested_range_complete,
            "unavailableTransactionCount": unavailable_transaction_count,
        })),
        "degraded": degraded,
        "unavailable": unavailable,
    }))
}

fn conventional_fee_day_json(
    day: &explorer::ConventionalFeeDistributionDay,
) -> Result<Option<Value>, CipherscanRestError> {
    let mut frequencies = BTreeMap::<u64, u64>::new();
    for frequency in &day.frequencies {
        if frequency.zip317_conventional_fee_zat == 0 || frequency.transaction_count == 0 {
            continue;
        }
        let count = frequencies
            .entry(frequency.zip317_conventional_fee_zat)
            .or_default();
        *count = count.checked_add(frequency.transaction_count).ok_or(
            CipherscanRestError::InvalidUpstreamField(
                "conventional_fee_distribution.days.frequencies.transaction_count",
            ),
        )?;
    }
    fee_distribution_day_json(day.day_start_unix_seconds, frequencies)
}

fn fee_distribution_day_json(
    day_start_unix_seconds: i64,
    frequencies: BTreeMap<u64, u64>,
) -> Result<Option<Value>, CipherscanRestError> {
    let frequencies = frequencies.into_iter().collect::<Vec<_>>();
    let Some(statistics) = fee_distribution_statistics(&frequencies)? else {
        return Ok(None);
    };

    Ok(Some(json!({
        "date": cipherscan_timestamp_from_unix_seconds(day_start_unix_seconds),
        "p10": statistics.p10,
        "p25": statistics.p25,
        "median": statistics.median,
        "p75": statistics.p75,
        "p90": statistics.p90,
        "avgFee": statistics.average,
        "txCount": statistics.transaction_count,
    })))
}

fn cipherscan_timestamp_with_millis(timestamp: OffsetDateTime) -> String {
    let whole_second = cipherscan_timestamp_from_unix_seconds(timestamp.unix_timestamp());
    let Some(without_millis) = whole_second.strip_suffix(".000Z") else {
        return whole_second;
    };
    format!(
        "{without_millis}.{:03}Z",
        timestamp.nanosecond() / 1_000_000
    )
}

#[derive(Debug, Eq, PartialEq)]
struct FeeDistributionStatistics {
    p10: u64,
    p25: u64,
    median: u64,
    p75: u64,
    p90: u64,
    average: u64,
    transaction_count: u64,
}

fn fee_distribution_statistics(
    frequencies: &[(u64, u64)],
) -> Result<Option<FeeDistributionStatistics>, CipherscanRestError> {
    let transaction_count = frequencies
        .iter()
        .try_fold(0_u64, |total, (_, count)| total.checked_add(*count));
    let Some(transaction_count) = transaction_count else {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "conventional_fee_distribution.transaction_count",
        ));
    };
    if transaction_count == 0 {
        return Ok(None);
    }
    let weighted_sum = frequencies.iter().try_fold(0_u128, |total, (fee, count)| {
        total.checked_add(u128::from(*fee).checked_mul(u128::from(*count))?)
    });
    let Some(weighted_sum) = weighted_sum else {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "conventional_fee_distribution.weighted_fee_sum",
        ));
    };
    let rounded_average = weighted_sum
        .checked_add(u128::from(transaction_count) / 2)
        .and_then(|sum| u64::try_from(sum / u128::from(transaction_count)).ok())
        .ok_or(CipherscanRestError::InvalidUpstreamField(
            "conventional_fee_distribution.average",
        ))?;

    Ok(Some(FeeDistributionStatistics {
        p10: fee_distribution_percentile(frequencies, transaction_count, 10)?,
        p25: fee_distribution_percentile(frequencies, transaction_count, 25)?,
        median: fee_distribution_percentile(frequencies, transaction_count, 50)?,
        p75: fee_distribution_percentile(frequencies, transaction_count, 75)?,
        p90: fee_distribution_percentile(frequencies, transaction_count, 90)?,
        average: rounded_average,
        transaction_count,
    }))
}

fn fee_distribution_percentile(
    frequencies: &[(u64, u64)],
    transaction_count: u64,
    percentile: u64,
) -> Result<u64, CipherscanRestError> {
    let rank_numerator = u128::from(transaction_count.saturating_sub(1))
        .checked_mul(u128::from(percentile))
        .ok_or(CipherscanRestError::InvalidUpstreamField(
            "conventional_fee_distribution.percentile_rank",
        ))?;
    let lower_rank = u64::try_from(rank_numerator / 100).map_err(|_| {
        CipherscanRestError::InvalidUpstreamField("conventional_fee_distribution.percentile_rank")
    })?;
    let interpolation = rank_numerator % 100;
    let upper_rank = lower_rank.saturating_add(u64::from(interpolation > 0));
    let lower_fee = fee_distribution_fee_at_rank(frequencies, lower_rank)?;
    let upper_fee = fee_distribution_fee_at_rank(frequencies, upper_rank)?;
    let interpolated_numerator = u128::from(lower_fee)
        .checked_mul(100)
        .and_then(|base| {
            u128::from(upper_fee.saturating_sub(lower_fee))
                .checked_mul(interpolation)
                .and_then(|increment| base.checked_add(increment))
        })
        .and_then(|interpolated_fee| interpolated_fee.checked_add(50))
        .ok_or(CipherscanRestError::InvalidUpstreamField(
            "conventional_fee_distribution.percentile",
        ))?;
    u64::try_from(interpolated_numerator / 100).map_err(|_| {
        CipherscanRestError::InvalidUpstreamField("conventional_fee_distribution.percentile")
    })
}

fn fee_distribution_fee_at_rank(
    frequencies: &[(u64, u64)],
    rank: u64,
) -> Result<u64, CipherscanRestError> {
    let mut first_rank_after_bucket = 0_u64;
    for (fee, count) in frequencies {
        first_rank_after_bucket = first_rank_after_bucket.checked_add(*count).ok_or(
            CipherscanRestError::InvalidUpstreamField(
                "conventional_fee_distribution.transaction_count",
            ),
        )?;
        if rank < first_rank_after_bucket {
            return Ok(*fee);
        }
    }
    Err(CipherscanRestError::InvalidUpstreamField(
        "conventional_fee_distribution.percentile_rank",
    ))
}

fn value_pool_source_tip(
    summary: &explorer::ValuePoolSummaryResponse,
) -> Result<&wallet::BlockTip, CipherscanRestError> {
    summary
        .source_tip
        .as_ref()
        .ok_or(CipherscanRestError::MissingUpstreamField(
            "value_pool_summary.source_tip",
        ))
}

fn verified_value_pool_source_tip(
    summary: &explorer::ValuePoolSummaryResponse,
) -> Result<&wallet::BlockTip, CipherscanRestError> {
    let source_tip = value_pool_source_tip(summary)?;
    let visible_tip = explorer_visible_tip(summary.freshness.as_ref()).ok_or(
        CipherscanRestError::MissingUpstreamField(
            "value_pool_summary.freshness.chain_view.chain_epoch.visible_tip",
        ),
    )?;
    if source_tip != visible_tip {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "value_pool_summary.source_tip",
        ));
    }
    Ok(source_tip)
}

fn validate_block_header_tip(
    block_header: &wallet::BlockHeader,
    expected_tip: &wallet::BlockTip,
) -> Result<(), CipherscanRestError> {
    let block_id =
        block_header
            .block_id
            .as_ref()
            .ok_or(CipherscanRestError::MissingUpstreamField(
                "block_header.block_id",
            ))?;
    let has_expected_height = block_id.height == expected_tip.height;
    let has_expected_hash = block_id.block_hash == expected_tip.hash;
    if !has_expected_height || !has_expected_hash {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "block_header.block_id",
        ));
    }
    Ok(())
}

fn pool_overview_json(
    summary: &explorer::ValuePoolSummaryResponse,
    history: &ValuePoolBalanceHistoryResponse,
) -> Result<Value, CipherscanRestError> {
    validate_value_pools(&summary.pools)?;
    let summary_tip = verified_value_pool_source_tip(summary)?;
    let history_tip = explorer_visible_tip(history.freshness.as_ref()).ok_or(
        CipherscanRestError::MissingUpstreamField(
            "value_pool_balance_history.freshness.chain_view.chain_epoch.visible_tip",
        ),
    )?;
    if history_tip != summary_tip {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "value_pool_balance_history.freshness.chain_view.chain_epoch.visible_tip",
        ));
    }
    let transparent = value_pool_zat(&summary.pools, "transparent");
    let sprout = value_pool_zat(&summary.pools, "sprout");
    let sapling = value_pool_zat(&summary.pools, "sapling");
    let orchard = value_pool_zat(&summary.pools, "orchard");
    let ironwood = value_pool_zat(&summary.pools, "ironwood");
    let shielded = total_value_pools_zat([sprout, sapling, orchard, ironwood])?;
    let chain_supply = complete_chain_supply_zat(&summary.pools)?;
    let history_complete = value_pool_history_is_historically_complete(history)?;
    let mut unavailable = Vec::new();
    if !history_complete {
        unavailable.push(
            "Cumulative value-pool history is still backfilling the canonical height domain.",
        );
    }
    if !value_pools_are_complete(&summary.pools) {
        unavailable.push(VALUE_POOL_TOTALS_UNAVAILABLE);
    }
    if has_unknown_nonzero_value_pool(&summary.pools) {
        unavailable.push(UNKNOWN_VALUE_POOL_SEMANTICS_UNAVAILABLE);
    }

    Ok(json!({
        "success": true,
        "current": {
            "sprout": sprout,
            "sapling": sapling,
            "orchard": orchard,
            "ironwood": ironwood,
            "transparent": transparent,
            "shielded": shielded,
            "chainSupply": chain_supply,
            "updatedAt": current_rfc3339_timestamp(),
        },
        "deltas": if history_complete {
            pool_overview_deltas_json(summary, history)?
        } else {
            pool_overview_unavailable_deltas()
        },
        "degraded": !unavailable.is_empty(),
        "unavailable": unavailable,
    }))
}

fn pool_overview_deltas_json(
    summary: &explorer::ValuePoolSummaryResponse,
    history: &ValuePoolBalanceHistoryResponse,
) -> Result<Value, CipherscanRestError> {
    let current = [
        ("sprout", value_pool_zat(&summary.pools, "sprout")),
        ("sapling", value_pool_zat(&summary.pools, "sapling")),
        ("orchard", value_pool_zat(&summary.pools, "orchard")),
        ("ironwood", value_pool_zat(&summary.pools, "ironwood")),
    ];
    let current_shielded =
        total_value_pools_zat(current.iter().map(|(_, pool_value_zat)| *pool_value_zat))?;
    let current_day = calendar_date_start_unix_seconds(OffsetDateTime::now_utc().date());
    let mut deltas = serde_json::Map::new();
    for (pool_id, current_value) in current {
        deltas.insert(
            pool_id.to_owned(),
            pool_delta_periods_from_history(current_value, history, current_day, |point| {
                Ok(history_pool_value(&point.pools, pool_id))
            })?,
        );
    }
    deltas.insert(
        "shielded".to_owned(),
        pool_delta_periods_from_history(current_shielded, history, current_day, |point| {
            total_optional_u64(
                ["sprout", "sapling", "orchard", "ironwood"]
                    .into_iter()
                    .map(|id| history_pool_value(&point.pools, id)),
            )
        })?,
    );
    Ok(Value::Object(deltas))
}

fn pool_delta_periods_from_history(
    current_value: Option<i64>,
    history: &ValuePoolBalanceHistoryResponse,
    current_day: i64,
    historical_value: impl Fn(
        &explorer::ValuePoolBalanceHistoryPoint,
    ) -> Result<Option<u64>, CipherscanRestError>,
) -> Result<Value, CipherscanRestError> {
    let mut periods = serde_json::Map::new();
    for (label, days) in [("24h", 1_i64), ("7d", 7), ("30d", 30)] {
        let target_day = days
            .checked_mul(86_400)
            .and_then(|seconds| current_day.checked_sub(seconds))
            .ok_or(CipherscanRestError::InvalidUpstreamField(
                "value_pool_balance_history.period",
            ))?;
        let Some(point) = history
            .points
            .iter()
            .find(|point| point.day_start_unix_seconds == target_day)
        else {
            continue;
        };
        let delta = match (current_value, historical_value(point)?) {
            (Some(current), Some(historical)) if historical > 0 => i64::try_from(historical)
                .ok()
                .and_then(|historical| current.checked_sub(historical))
                .map_or(Value::Null, Value::from),
            _ => Value::Null,
        };
        periods.insert(label.to_owned(), delta);
    }
    Ok(Value::Object(periods))
}

fn migration_analytics_from_entries(
    entries: &[explorer::TransactionHistoryEntry],
) -> Result<MigrationAnalyticsState, CipherscanRestError> {
    let mut analytics = MigrationAnalytics::default();
    let mut cohorts = BTreeMap::<u32, MigrationCohortAccumulator>::new();
    let mut denominations = BTreeMap::<i32, MigrationDenominationAccumulator>::new();

    for entry in entries {
        let Some(balances) = entry.intrinsic_value_balances.as_ref() else {
            return Ok(MigrationAnalyticsState::Unavailable(
                MigrationAnalyticsUnavailable::IntrinsicValueBalanceUnavailable,
            ));
        };
        if balances.ironwood_zat >= 0 {
            continue;
        }

        let ironwood_in_zat = balances
            .ironwood_zat
            .checked_abs()
            .and_then(|magnitude_zat| u64::try_from(magnitude_zat).ok())
            .ok_or(CipherscanRestError::InvalidUpstreamField(
                "transaction_history.entries.intrinsic_value_balances.ironwood_zat",
            ))?;
        analytics.total_migrated_zat = checked_migration_sum(
            analytics.total_migrated_zat,
            ironwood_in_zat,
            "migration.total_migrated_zat",
        )?;
        analytics.ironwood_in_zat = checked_migration_sum(
            analytics.ironwood_in_zat,
            ironwood_in_zat,
            "migration.supply_audit.ironwood_in_zat",
        )?;
        analytics.transaction_count = checked_migration_sum(
            analytics.transaction_count,
            1,
            "migration.transaction_count",
        )?;
        analytics.first_height = Some(
            analytics
                .first_height
                .map_or(entry.block_height, |height| height.min(entry.block_height)),
        );
        analytics.last_height = Some(
            analytics
                .last_height
                .map_or(entry.block_height, |height| height.max(entry.block_height)),
        );
        if balances.orchard_zat > 0 {
            let orchard_out_zat = u64::try_from(balances.orchard_zat).map_err(|_| {
                CipherscanRestError::InvalidUpstreamField(
                    "transaction_history.entries.intrinsic_value_balances.orchard_zat",
                )
            })?;
            analytics.orchard_out_zat = checked_migration_sum(
                analytics.orchard_out_zat,
                orchard_out_zat,
                "migration.supply_audit.orchard_out_zat",
            )?;
        }

        record_migration_cohort(&mut cohorts, entry, ironwood_in_zat)?;
        record_migration_denomination(&mut denominations, ironwood_in_zat)?;
    }

    analytics.cohorts = cohorts
        .into_iter()
        .map(|(boundary, cohort)| MigrationCohort {
            boundary,
            boundary_start_height: cohort.boundary_start_height.unwrap_or_default(),
            transaction_count: cohort.transaction_count,
            volume_zat: cohort.volume_zat,
            first_time_unix_seconds: cohort.first_time_unix_seconds.unwrap_or_default(),
        })
        .collect();
    analytics.denomination_bins = denominations
        .into_iter()
        .rev()
        .map(|(power, bin)| MigrationDenominationBin {
            power,
            transaction_count: bin.transaction_count,
            volume_zat: bin.volume_zat,
        })
        .collect();

    Ok(MigrationAnalyticsState::Available(analytics))
}

fn transaction_history_entries_are_newest_first(
    entries: &[explorer::TransactionHistoryEntry],
) -> bool {
    entries.windows(2).all(|pair| {
        (pair[0].block_height, pair[0].transaction_index)
            > (pair[1].block_height, pair[1].transaction_index)
    })
}

fn record_migration_cohort(
    cohorts: &mut BTreeMap<u32, MigrationCohortAccumulator>,
    entry: &explorer::TransactionHistoryEntry,
    ironwood_in_zat: u64,
) -> Result<(), CipherscanRestError> {
    let boundary = entry.block_height / MIGRATION_BOUNDARY_MODULUS;
    let cohort = cohorts.entry(boundary).or_default();
    cohort.boundary_start_height = Some(
        cohort
            .boundary_start_height
            .map_or(entry.block_height, |height| height.min(entry.block_height)),
    );
    cohort.first_time_unix_seconds = Some(
        cohort
            .first_time_unix_seconds
            .map_or(entry.block_time_unix_seconds, |time| {
                time.min(entry.block_time_unix_seconds)
            }),
    );
    cohort.transaction_count = checked_migration_sum(
        cohort.transaction_count,
        1,
        "migration.cohorts.transaction_count",
    )?;
    cohort.volume_zat = checked_migration_sum(
        cohort.volume_zat,
        ironwood_in_zat,
        "migration.cohorts.volume_zat",
    )?;
    Ok(())
}

fn record_migration_denomination(
    denominations: &mut BTreeMap<i32, MigrationDenominationAccumulator>,
    ironwood_in_zat: u64,
) -> Result<(), CipherscanRestError> {
    let power = i32::try_from(ironwood_in_zat.ilog10()).map_or(i32::MAX, |power| power - 8);
    let bin = denominations.entry(power).or_default();
    bin.transaction_count = checked_migration_sum(
        bin.transaction_count,
        1,
        "migration.denominations.transaction_count",
    )?;
    bin.volume_zat = checked_migration_sum(
        bin.volume_zat,
        ironwood_in_zat,
        "migration.denominations.volume_zat",
    )?;
    Ok(())
}

fn checked_migration_sum(
    total: u64,
    increment: u64,
    field: &'static str,
) -> Result<u64, CipherscanRestError> {
    total
        .checked_add(increment)
        .ok_or(CipherscanRestError::InvalidUpstreamField(field))
}

fn migration_overview_json(
    network: Network,
    tip_height: u32,
    summary: &explorer::ValuePoolSummaryResponse,
    analytics_state: &MigrationAnalyticsState,
) -> Result<Value, CipherscanRestError> {
    validate_value_pools(&summary.pools)?;
    let orchard = value_pool_zat(&summary.pools, "orchard");
    let current_ironwood = value_pool_zat(&summary.pools, "ironwood");
    let activation_height = migration_activation_height(network);
    let activated = activation_height.is_some_and(|height| tip_height >= height);
    let blocks_until_activation = activation_height.map_or(0, |height| {
        if activated {
            0
        } else {
            height.saturating_sub(tip_height)
        }
    });
    let (analytics, migration_unavailable) = match analytics_state {
        MigrationAnalyticsState::Available(analytics) => (Some(analytics), None),
        MigrationAnalyticsState::Unavailable(unavailable) => (None, Some(unavailable.reason())),
    };
    let current_migration_supply_zat = total_value_pools_zat([orchard, current_ironwood])?;
    let migrated_percent = match (current_ironwood, current_migration_supply_zat) {
        (Some(current_ironwood), Some(current_migration_supply_zat)) => {
            Some(progress_pct(current_ironwood, current_migration_supply_zat))
        }
        _ => None,
    };
    let mut unavailable =
        vec!["Reference node height and observed average block time are Cipherscan sidecar facts."];
    unavailable.extend(migration_unavailable);
    if orchard.is_none() || current_ironwood.is_none() {
        unavailable.push(VALUE_POOL_TOTALS_UNAVAILABLE);
    }

    Ok(json!({
        "success": true,
        "network": cipherscan_network_name(network),
        "activationHeight": activation_height,
        "tipHeight": tip_height,
        "activated": activated,
        "avgBlockTimeSecs": f64::from(SECONDS_PER_BLOCK),
        "referenceNode": Value::Null,
        "blocksUntilActivation": blocks_until_activation,
        "poolSizes": {
            "orchardZat": orchard,
            "ironwoodZat": current_ironwood,
            "updatedAt": current_rfc3339_timestamp(),
        },
        "migration": {
            "totalMigratedZat": analytics.map(|analytics| analytics.total_migrated_zat),
            "txCount": analytics.map(|analytics| analytics.transaction_count),
            "firstHeight": analytics.and_then(|analytics| analytics.first_height),
            "lastHeight": analytics.and_then(|analytics| analytics.last_height),
            "migratedPercent": migrated_percent,
        },
        "supplyAudit": {
            "orchardOutZat": analytics.map(|analytics| analytics.orchard_out_zat),
            "ironwoodInZat": analytics.map(|analytics| analytics.ironwood_in_zat),
            "balanced": analytics.map(|analytics| {
                analytics.orchard_out_zat == analytics.ironwood_in_zat
            }),
        },
        "degraded": true,
        "unavailable": unavailable,
    }))
}

fn migration_cohorts_json(network: Network, analytics_state: &MigrationAnalyticsState) -> Value {
    let analytics = match analytics_state {
        MigrationAnalyticsState::Available(analytics) => analytics,
        MigrationAnalyticsState::Unavailable(unavailable) => {
            return json!({
                "success": true,
                "network": cipherscan_network_name(network),
                "boundaryModulus": MIGRATION_BOUNDARY_MODULUS,
                "cohortCount": Value::Null,
                "avgAnonymitySet": Value::Null,
                "minAnonymitySet": Value::Null,
                "maxAnonymitySet": Value::Null,
                "cohorts": [],
                "degraded": true,
                "unavailable": [unavailable.reason()],
            });
        }
    };
    let cohort_count = analytics.cohorts.len();
    let average_anonymity_set = if cohort_count == 0 {
        0.0
    } else {
        let transaction_count = u32::try_from(analytics.transaction_count)
            .map_or_else(|_| f64::from(u32::MAX), f64::from);
        let cohort_count =
            u32::try_from(cohort_count).map_or_else(|_| f64::from(u32::MAX), f64::from);
        transaction_count / cohort_count
    };
    let minimum_anonymity_set = analytics
        .cohorts
        .iter()
        .map(|cohort| cohort.transaction_count)
        .min()
        .unwrap_or_default();
    let maximum_anonymity_set = analytics
        .cohorts
        .iter()
        .map(|cohort| cohort.transaction_count)
        .max()
        .unwrap_or_default();
    let cohorts = analytics
        .cohorts
        .iter()
        .map(|cohort| {
            json!({
                "boundary": cohort.boundary,
                "boundaryStartHeight": cohort.boundary_start_height,
                "txCount": cohort.transaction_count,
                "volumeZat": cohort.volume_zat,
                "firstTime": cohort.first_time_unix_seconds,
            })
        })
        .collect::<Vec<_>>();

    json!({
        "success": true,
        "network": cipherscan_network_name(network),
        "boundaryModulus": MIGRATION_BOUNDARY_MODULUS,
        "cohortCount": cohort_count,
        "avgAnonymitySet": average_anonymity_set,
        "minAnonymitySet": minimum_anonymity_set,
        "maxAnonymitySet": maximum_anonymity_set,
        "cohorts": cohorts,
        "degraded": false,
        "unavailable": [],
    })
}

fn migration_denominations_json(
    network: Network,
    analytics_state: &MigrationAnalyticsState,
) -> Value {
    let analytics = match analytics_state {
        MigrationAnalyticsState::Available(analytics) => analytics,
        MigrationAnalyticsState::Unavailable(unavailable) => {
            return json!({
                "success": true,
                "network": cipherscan_network_name(network),
                "totalTx": Value::Null,
                "bins": [],
                "degraded": true,
                "unavailable": [unavailable.reason()],
            });
        }
    };
    let bins = analytics
        .denomination_bins
        .iter()
        .map(|bin| {
            json!({
                "power": bin.power,
                "denomination": 10_f64.powi(bin.power),
                "label": migration_denomination_label(bin.power),
                "txCount": bin.transaction_count,
                "volumeZat": bin.volume_zat,
            })
        })
        .collect::<Vec<_>>();

    json!({
        "success": true,
        "network": cipherscan_network_name(network),
        "totalTx": analytics.transaction_count,
        "bins": bins,
        "degraded": false,
        "unavailable": [],
    })
}

fn migration_denomination_label(power: i32) -> String {
    if power >= 0 {
        return format!("{} ZEC", 10_f64.powi(power));
    }
    let zero_count = usize::try_from(power.unsigned_abs().saturating_sub(1)).unwrap_or_default();
    format!("0.{}1 ZEC", "0".repeat(zero_count))
}

fn pool_overview_unavailable_deltas() -> Value {
    json!({
        "sprout": pool_delta_periods_json(),
        "sapling": pool_delta_periods_json(),
        "orchard": pool_delta_periods_json(),
        "ironwood": pool_delta_periods_json(),
        "shielded": pool_delta_periods_json(),
    })
}

fn pool_delta_periods_json() -> Value {
    json!({
        "24h": Value::Null,
        "7d": Value::Null,
        "30d": Value::Null,
    })
}

fn validate_value_pools(pools: &[wallet::ChainValuePool]) -> Result<(), CipherscanRestError> {
    let mut pool_ids = HashSet::new();

    for pool in pools {
        if !pool_ids.insert(pool.id.as_str()) {
            return Err(CipherscanRestError::InvalidUpstreamField(
                "value_pool_summary.pools.id",
            ));
        }
        if pool
            .chain_value_zat
            .is_some_and(|chain_value_zat| chain_value_zat < 0)
        {
            return Err(CipherscanRestError::InvalidUpstreamField(
                "value_pool_summary.pools.chain_value_zat",
            ));
        }
    }

    Ok(())
}

fn value_pool_zat(pools: &[wallet::ChainValuePool], id: &str) -> Option<i64> {
    pools
        .iter()
        .find(|pool| pool.id == id)
        .and_then(|pool| pool.chain_value_zat)
}

fn required_value_pool_zat(
    pools: &[wallet::ChainValuePool],
    id: &'static str,
) -> Result<i64, CipherscanRestError> {
    value_pool_zat(pools, id).ok_or(CipherscanRestError::MissingUpstreamField(
        "value_pool_summary.pools.chain_value_zat",
    ))
}

fn transparent_value_pool_zat(
    value_pools: &explorer::ValuePoolSummaryResponse,
) -> Result<u64, CipherscanRestError> {
    validate_value_pools(&value_pools.pools)?;
    u64::try_from(value_pool_zat(&value_pools.pools, "transparent").ok_or(
        CipherscanRestError::MissingUpstreamField(
            "value_pool_summary.pools.transparent.chain_value_zat",
        ),
    )?)
    .map_err(|_| CipherscanRestError::InvalidUpstreamField("value_pool_summary.transparent"))
}

fn total_value_pools_zat(
    pool_values: impl IntoIterator<Item = Option<i64>>,
) -> Result<Option<i64>, CipherscanRestError> {
    let mut total_zat = 0_i64;
    for pool_value_zat in pool_values {
        let Some(pool_value_zat) = pool_value_zat else {
            return Ok(None);
        };
        total_zat = total_zat.checked_add(pool_value_zat).ok_or(
            CipherscanRestError::InvalidUpstreamField("value_pool_summary.pools.chain_value_zat"),
        )?;
    }
    Ok(Some(total_zat))
}

fn complete_chain_supply_zat(
    pools: &[wallet::ChainValuePool],
) -> Result<Option<i64>, CipherscanRestError> {
    if CIPHERSCAN_VALUE_POOL_IDS
        .iter()
        .any(|id| value_pool_zat(pools, id).is_none())
    {
        return Ok(None);
    }

    let mut chain_supply_zat = 0_i64;
    for pool in pools {
        let Some(pool_value_zat) = pool.chain_value_zat else {
            if CIPHERSCAN_VALUE_POOL_IDS.contains(&pool.id.as_str()) || pool.monitored {
                return Ok(None);
            }
            continue;
        };
        chain_supply_zat = chain_supply_zat.checked_add(pool_value_zat).ok_or(
            CipherscanRestError::InvalidUpstreamField("value_pool_summary.pools.chain_value_zat"),
        )?;
    }
    Ok(Some(chain_supply_zat))
}

fn value_pools_are_complete(pools: &[wallet::ChainValuePool]) -> bool {
    pools.iter().all(|pool| {
        pool.chain_value_zat.is_some()
            || (!CIPHERSCAN_VALUE_POOL_IDS.contains(&pool.id.as_str()) && !pool.monitored)
    }) && CIPHERSCAN_VALUE_POOL_IDS
        .iter()
        .all(|id| value_pool_zat(pools, id).is_some())
}

fn has_unknown_nonzero_value_pool(pools: &[wallet::ChainValuePool]) -> bool {
    pools.iter().any(|pool| {
        !CIPHERSCAN_VALUE_POOL_IDS.contains(&pool.id.as_str())
            && pool
                .chain_value_zat
                .is_some_and(|chain_value_zat| chain_value_zat != 0)
    })
}

fn pool_turnstile_building_json() -> Value {
    json!({
        "success": false,
        "error": "turnstile_daily view is rebuilding",
        "status": "building",
        "retryAfter": 60,
        "degraded": true,
        "unavailable": [
            "Turnstile classification is Cipherscan sidecar analytics and is not a Zinder core chain fact."
        ],
    })
}

fn blend_check_invalid_amount_response() -> Response {
    json_response(
        StatusCode::BAD_REQUEST,
        json!({
            "error": "Invalid amount. Must be > 0 and <= 21M ZEC.",
        }),
    )
}

fn invalid_txid_path_parameters_response() -> Response {
    json_response(
        StatusCode::BAD_REQUEST,
        json!({
            "error": "Invalid path parameters",
            "details": [
                {
                    "field": "txid",
                    "message": "Invalid transaction ID",
                }
            ],
        }),
    )
}

fn invalid_recommended_amounts_query_response() -> Response {
    json_response(
        StatusCode::BAD_REQUEST,
        json!({
            "error": "Invalid query parameters",
            "details": [
                {
                    "field": "chain",
                    "message": "Invalid input: expected string, received undefined",
                },
                {
                    "field": "token",
                    "message": "Invalid input: expected string, received undefined",
                }
            ],
        }),
    )
}

fn historical_price_invalid_date_response() -> Response {
    json_response(
        StatusCode::BAD_REQUEST,
        json!({
            "error": "date query param required (YYYY-MM-DD)",
        }),
    )
}

fn historical_price_json(date: &str) -> Value {
    json!({
        "date": date,
        "price_usd": Value::Null,
        "exact": false,
    })
}

fn historical_market_price_json(
    historical_price: crate::market_price::HistoricalMarketPrice,
) -> Value {
    let mut response = json!({
        "date": historical_price.date,
        "price_usd": historical_price.price_usd,
        "exact": historical_price.exact,
    });
    if let Some(actual_date) = historical_price.actual_date {
        response["actual_date"] = json!(actual_date);
    }
    response
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct BlendPeriodCounts {
    total: u64,
    shields: u64,
    deshields: u64,
}

fn blend_period_counts_json(counts: BlendPeriodCounts) -> Value {
    json!({
        "total": counts.total,
        "shields": counts.shields,
        "deshields": counts.deshields,
    })
}

async fn require_blend_check_capabilities(
    adapter: &CipherscanRestAdapter,
) -> Result<(), CipherscanRestError> {
    adapter
        .require_explorer_capability(EXPLORER_VALUE_POOL_FLOW_AMOUNT_THRESHOLD_SUMMARY_V1)
        .await?;
    adapter
        .require_explorer_capability(EXPLORER_VALUE_POOL_FLOW_ROUNDED_AMOUNT_SUMMARY_V1)
        .await
}

async fn fetch_blend_period_counts(
    adapter: &CipherscanRestAdapter,
    amount_zat: u64,
    now_unix_seconds: i64,
    expected_epoch_id: &mut Option<u64>,
) -> Result<serde_json::Map<String, Value>, CipherscanRestError> {
    let mut periods = serde_json::Map::new();
    for (name, start_time_unix_seconds) in [
        ("24h", now_unix_seconds.saturating_sub(UNIX_SECONDS_PER_DAY)),
        (
            "7d",
            now_unix_seconds.saturating_sub(7 * UNIX_SECONDS_PER_DAY),
        ),
        (
            "30d",
            now_unix_seconds.saturating_sub(30 * UNIX_SECONDS_PER_DAY),
        ),
        ("all", 0),
    ] {
        let counts = fetch_exact_blend_counts(
            adapter,
            start_time_unix_seconds,
            i64::MAX,
            &[amount_zat],
            expected_epoch_id,
        )
        .await?
        .remove(&amount_zat)
        .unwrap_or_default();
        periods.insert(name.to_owned(), blend_period_counts_json(counts));
    }
    Ok(periods)
}

async fn fetch_exact_blend_counts(
    adapter: &CipherscanRestAdapter,
    start_time_unix_seconds: i64,
    end_time_unix_seconds: i64,
    amounts_zat: &[u64],
    expected_epoch_id: &mut Option<u64>,
) -> Result<HashMap<u64, BlendPeriodCounts>, CipherscanRestError> {
    let mut amounts_zat = amounts_zat.to_vec();
    amounts_zat.sort_unstable();
    amounts_zat.dedup();
    let mut counts = HashMap::with_capacity(amounts_zat.len());
    for amount_chunk in amounts_zat.chunks(MAX_AMOUNT_RANGES_PER_THRESHOLD_REQUEST) {
        let mut thresholds = amount_chunk
            .iter()
            .flat_map(|amount_zat| {
                [
                    amount_zat.saturating_sub(BLEND_MATCH_TOLERANCE_ZAT),
                    amount_zat.saturating_add(BLEND_MATCH_TOLERANCE_ZAT + 1),
                ]
            })
            .collect::<Vec<_>>();
        thresholds.sort_unstable();
        thresholds.dedup();
        let response = adapter
            .fetch_value_pool_flow_amount_threshold_summary(
                ValuePoolFlowAmountThresholdSummaryRequest {
                    start_time_unix_seconds,
                    end_time_unix_seconds,
                    pools: Vec::new(),
                    minimum_amounts_zat: thresholds.clone(),
                },
            )
            .await?;
        require_blend_response_context(
            response.freshness.as_ref(),
            response.coverage.as_ref(),
            expected_epoch_id,
        )?;
        counts.extend(exact_blend_counts_from_thresholds(
            amount_chunk,
            &thresholds,
            &response.thresholds,
        )?);
    }
    Ok(counts)
}

fn exact_blend_counts_from_thresholds(
    amounts_zat: &[u64],
    expected_thresholds: &[u64],
    threshold_rows: &[explorer::ValuePoolFlowAmountThresholdSummaryRow],
) -> Result<Vec<(u64, BlendPeriodCounts)>, CipherscanRestError> {
    if threshold_rows.len() != expected_thresholds.len()
        || threshold_rows
            .iter()
            .zip(expected_thresholds)
            .any(|(row, threshold)| row.minimum_amount_zat != *threshold)
    {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "blend_check.thresholds",
        ));
    }
    let rows = threshold_rows
        .iter()
        .map(|row| (row.minimum_amount_zat, row))
        .collect::<HashMap<_, _>>();
    amounts_zat
        .iter()
        .map(|amount_zat| {
            let lower = amount_zat.saturating_sub(BLEND_MATCH_TOLERANCE_ZAT);
            let upper = amount_zat.saturating_add(BLEND_MATCH_TOLERANCE_ZAT + 1);
            let lower_row = rows
                .get(&lower)
                .ok_or(CipherscanRestError::InvalidUpstreamField(
                    "blend_check.thresholds.lower",
                ))?;
            let upper_row = rows
                .get(&upper)
                .ok_or(CipherscanRestError::InvalidUpstreamField(
                    "blend_check.thresholds.upper",
                ))?;
            let shields = subtract_cumulative_flow_total(
                lower_row.shield_event_count,
                upper_row.shield_event_count,
            )?;
            let deshields = subtract_cumulative_flow_total(
                lower_row.deshield_event_count,
                upper_row.deshield_event_count,
            )?;
            Ok((
                *amount_zat,
                BlendPeriodCounts {
                    total: shields.checked_add(deshields).ok_or(
                        CipherscanRestError::InvalidUpstreamField("blend_check.total"),
                    )?,
                    shields,
                    deshields,
                },
            ))
        })
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BlendRoundedAmountRequest {
    start_time_unix_seconds: i64,
    minimum_raw_amount_zat: u64,
    maximum_raw_amount_zat: u64,
    minimum_event_count: u64,
    max_rows: u32,
}

async fn fetch_blend_rounded_amounts(
    adapter: &CipherscanRestAdapter,
    request: BlendRoundedAmountRequest,
    expected_epoch_id: &mut Option<u64>,
) -> Result<Vec<explorer::ValuePoolFlowRoundedAmountSummaryRow>, CipherscanRestError> {
    if request.maximum_raw_amount_zat <= request.minimum_raw_amount_zat {
        return Ok(Vec::new());
    }
    let response = adapter
        .fetch_value_pool_flow_rounded_amount_summary(ValuePoolFlowRoundedAmountSummaryRequest {
            start_time_unix_seconds: request.start_time_unix_seconds,
            end_time_unix_seconds: i64::MAX,
            pools: Vec::new(),
            minimum_raw_amount_zat: request.minimum_raw_amount_zat,
            maximum_raw_amount_zat: Some(request.maximum_raw_amount_zat),
            rounding_quantum_zat: BLEND_ROUNDING_QUANTUM_ZAT,
            minimum_event_count: request.minimum_event_count,
            max_rows: request.max_rows,
        })
        .await?;
    require_blend_response_context(
        response.freshness.as_ref(),
        response.coverage.as_ref(),
        expected_epoch_id,
    )?;
    validate_blend_rounded_amount_rows(&response.rows)?;
    Ok(response.rows)
}

fn require_blend_response_context(
    freshness: Option<&explorer::ExplorerFreshness>,
    coverage: Option<&explorer::ValuePoolFlowCoverage>,
    expected_epoch_id: &mut Option<u64>,
) -> Result<(), CipherscanRestError> {
    let epoch_id = explorer_chain_epoch_id(freshness).ok_or(
        CipherscanRestError::MissingUpstreamField("blend_check.freshness.chain_epoch"),
    )?;
    if expected_epoch_id.is_some_and(|expected| expected != epoch_id) {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "blend_check.freshness.chain_epoch",
        ));
    }
    let coverage = coverage.ok_or(CipherscanRestError::MissingUpstreamField(
        "blend_check.coverage",
    ))?;
    if !coverage.requested_range_complete {
        return Err(CipherscanRestError::MissingUpstreamField(
            "blend_check.coverage.requested_range_complete",
        ));
    }
    *expected_epoch_id = Some(epoch_id);
    Ok(())
}

fn validate_blend_rounded_amount_rows(
    rows: &[explorer::ValuePoolFlowRoundedAmountSummaryRow],
) -> Result<(), CipherscanRestError> {
    let mut previous: Option<(u64, u64)> = None;
    let mut seen = HashSet::new();
    for row in rows {
        let total = row
            .shield_event_count
            .checked_add(row.deshield_event_count)
            .ok_or(CipherscanRestError::InvalidUpstreamField(
                "blend_check.rounded_rows.total",
            ))?;
        if !seen.insert(row.rounded_amount_zat)
            || previous.is_some_and(|(previous_total, previous_amount)| {
                total > previous_total
                    || (total == previous_total && row.rounded_amount_zat < previous_amount)
            })
        {
            return Err(CipherscanRestError::InvalidUpstreamField(
                "blend_check.rounded_rows.order",
            ));
        }
        previous = Some((total, row.rounded_amount_zat));
    }
    Ok(())
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "Cipherscan accepts decimal ZEC query amounts and rounds them into zatoshis."
)]
fn parse_blend_amount(amount: Option<&str>) -> Option<(f64, u64)> {
    let amount = parse_javascript_float(amount?)?;
    if !amount.is_finite() || amount <= 0.0 || amount > 21_000_000.0 {
        return None;
    }

    let amount_zat = (amount * ZATOSHIS_PER_ZEC).round();
    Some((amount, amount_zat as u64))
}

fn parse_javascript_float(input: &str) -> Option<f64> {
    let input = input.trim_start();
    let bytes = input.as_bytes();
    let mut cursor = usize::from(matches!(bytes.first(), Some(b'+' | b'-')));
    let integer_start = cursor;
    while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
        cursor += 1;
    }
    let mut digit_count = cursor.saturating_sub(integer_start);
    if bytes.get(cursor) == Some(&b'.') {
        cursor += 1;
        let fraction_start = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        digit_count = digit_count.saturating_add(cursor.saturating_sub(fraction_start));
    }
    if digit_count == 0 {
        return None;
    }
    let exponent_start = cursor;
    if matches!(bytes.get(cursor), Some(b'e' | b'E')) {
        cursor += 1;
        if matches!(bytes.get(cursor), Some(b'+' | b'-')) {
            cursor += 1;
        }
        let exponent_digits_start = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        if cursor == exponent_digits_start {
            cursor = exponent_start;
        }
    }
    input.get(..cursor)?.parse().ok()
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "Cipherscan computes nearby raw bounds with JavaScript numbers and Math.round."
)]
fn cipherscan_nearby_raw_range(amount_zat: u64) -> (u64, u64) {
    (
        ((amount_zat as f64) * 0.2).round() as u64,
        ((amount_zat as f64) * 5.0).round() as u64,
    )
}

fn has_iso8601_calendar_date_shape(date: &str) -> bool {
    let bytes = date.as_bytes();
    bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[..4].iter().all(u8::is_ascii_digit)
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[8..].iter().all(u8::is_ascii_digit)
}

fn parse_iso8601_calendar_date(date: &str) -> Option<Date> {
    if !has_iso8601_calendar_date_shape(date) {
        return None;
    }
    let (year, month, day) = date
        .get(..4)
        .and_then(|year| year.parse::<i32>().ok())
        .zip(date.get(5..7).and_then(|month| month.parse::<u8>().ok()))
        .zip(date.get(8..).and_then(|day| day.parse::<u8>().ok()))
        .map(|((year, month), day)| (year, month, day))?;
    Month::try_from(month)
        .ok()
        .and_then(|month| Date::from_calendar_date(year, month, day).ok())
}

fn historical_price_lookup_date(requested_date: &str) -> String {
    let Some(requested_date_value) = parse_iso8601_calendar_date(requested_date) else {
        return requested_date.to_owned();
    };
    let Some(latest_complete_date) = OffsetDateTime::now_utc().date().previous_day() else {
        return requested_date.to_owned();
    };
    requested_date_value.min(latest_complete_date).to_string()
}

fn calendar_date_start_unix_seconds(date: Date) -> i64 {
    date.midnight().assume_utc().unix_timestamp()
}

fn value_pool_row(pool: &wallet::ChainValuePool) -> Value {
    json!({
        "id": pool.id,
        "chainValue": pool.chain_value_zat.map(zec_from_zatoshis),
        "chainValueZat": pool.chain_value_zat.map(|zatoshis| zatoshis.to_string()),
        "monitored": pool.monitored,
    })
}

fn address_activity_page(query: &PageQuery) -> Result<(u32, u32, u32), CipherscanRestError> {
    let page = query.page.unwrap_or(1).max(1);
    let limit = match query.limit {
        None | Some(0) => DEFAULT_ADDRESS_ACTIVITY_LIMIT,
        Some(limit) => limit.min(MAX_LIMIT),
    };
    let offset = page
        .checked_sub(1)
        .and_then(|page_index| page_index.checked_mul(limit))
        .ok_or_else(|| {
            CipherscanRestError::InvalidRequest(
                "address activity page offset overflowed".to_owned(),
            )
        })?;
    if offset > MAX_ADDRESS_ACTIVITY_OFFSET {
        return Err(CipherscanRestError::InvalidRequest(format!(
            "address activity offset must not exceed {MAX_ADDRESS_ACTIVITY_OFFSET}",
        )));
    }
    Ok((page, limit, offset))
}

fn private_address_response(address: &str) -> Option<Response> {
    let note = if address.starts_with('u') {
        "Fully shielded unified address - balance and transactions are private"
    } else if address.starts_with("zs")
        || address.starts_with("zc")
        || address.starts_with("ztestsapling")
    {
        "Shielded address - balance and transactions are private"
    } else {
        return None;
    };
    Some(json_response(
        StatusCode::OK,
        json!({
            "address": address,
            "type": "shielded",
            "balance": Value::Null,
            "transactions": [],
            "note": note,
        }),
    ))
}

fn address_activity_chain_epoch(
    activity: &explorer::TransparentAddressActivityResponse,
) -> Result<&wallet::ChainEpoch, CipherscanRestError> {
    activity
        .freshness
        .as_ref()
        .and_then(|freshness| freshness.chain_view.as_ref())
        .and_then(|chain_view| chain_view.chain_epoch.as_ref())
        .ok_or(CipherscanRestError::MissingUpstreamField(
            "transparent_address_activity.freshness.chain_view.chain_epoch",
        ))
}

struct CipherscanAddressDetailInput<'a> {
    network: Network,
    address: &'a str,
    page: u32,
    limit: u32,
    activity: &'a explorer::TransparentAddressActivityResponse,
}

fn address_detail_json(
    input: &CipherscanAddressDetailInput<'_>,
) -> Result<Value, CipherscanRestError> {
    let &CipherscanAddressDetailInput {
        network,
        address,
        page,
        limit,
        activity,
    } = input;
    let chain_epoch = address_activity_chain_epoch(activity)?;
    let summary = activity
        .summary
        .as_ref()
        .ok_or(CipherscanRestError::MissingUpstreamField(
            "transparent_address_activity.summary",
        ))?;
    let coverage = activity
        .coverage
        .as_ref()
        .ok_or(CipherscanRestError::MissingUpstreamField(
            "transparent_address_activity.coverage",
        ))?;
    let mapped_rows = map_address_activity_rows(network, address, &activity.entries)?;
    let is_zero_history = address_summary_has_no_history(summary, coverage, &activity.entries);
    let total = if is_zero_history {
        Some(0)
    } else {
        summary.distinct_transaction_count
    };
    let total_pages = total.map(|total| total.div_ceil(u64::from(limit)));
    let mut unavailable = address_activity_unavailable(
        summary,
        coverage,
        chain_epoch,
        activity.freshness.as_ref(),
        is_zero_history,
    );
    if activity
        .freshness
        .as_ref()
        .is_some_and(|freshness| !freshness.unavailable.is_empty())
    {
        push_unique_reason(
            &mut unavailable,
            "The native transparent-address activity response reports unavailable fields.",
        );
    }
    for reason in mapped_rows.unavailable {
        push_unique_reason(&mut unavailable, reason);
    }

    Ok(json!({
        "address": address,
        "type": "transparent",
        "balance": summary.balance_zat,
        "totalReceived": if is_zero_history { Some(0) } else { summary.total_received_zat },
        "totalSent": if is_zero_history { Some(0) } else { summary.total_sent_zat },
        "txCount": total,
        "firstSeen": summary.first_seen_unix_seconds.map(|timestamp| timestamp.to_string()),
        "lastSeen": summary.last_seen_unix_seconds.map(|timestamp| timestamp.to_string()),
        "transactions": mapped_rows.rows,
        "pagination": {
            "page": if is_zero_history { 1 } else { page },
            "limit": limit,
            "total": total,
            "totalPages": total_pages,
            "hasNext": total_pages.map_or(!activity.next_cursor.is_empty(), |pages| {
                u64::from(page) < pages
            }),
            "hasPrev": !is_zero_history && page > 1,
        },
        "note": is_zero_history.then_some("This address has no transaction history yet."),
        "degraded": !unavailable.is_empty(),
        "unavailable": unavailable,
    }))
}

fn address_summary_has_no_history(
    summary: &explorer::TransparentAddressSummary,
    coverage: &explorer::TransparentAddressRankingCoverage,
    entries: &[explorer::TransparentAddressActivityEntry],
) -> bool {
    coverage.lifetime_statistics_complete
        && summary.balance_zat == 0
        && summary.total_received_zat.is_none()
        && summary.total_sent_zat.is_none()
        && summary.distinct_transaction_count.is_none()
        && summary.first_seen_unix_seconds.is_none()
        && summary.last_seen_unix_seconds.is_none()
        && entries.is_empty()
}

struct CipherscanAddressActivityRows {
    rows: Vec<Value>,
    unavailable: Vec<&'static str>,
}

fn map_address_activity_rows(
    network: Network,
    requested_address: &str,
    entries: &[explorer::TransparentAddressActivityEntry],
) -> Result<CipherscanAddressActivityRows, CipherscanRestError> {
    let mut rows = Vec::with_capacity(entries.len());
    let mut unavailable = Vec::new();
    for entry in entries {
        let (row, row_unavailable) = address_activity_row(network, requested_address, entry)?;
        for reason in row_unavailable {
            push_unique_reason(&mut unavailable, reason);
        }
        rows.push(row);
    }
    Ok(CipherscanAddressActivityRows { rows, unavailable })
}

fn address_activity_row(
    network: Network,
    requested_address: &str,
    entry: &explorer::TransparentAddressActivityEntry,
) -> Result<(Value, Vec<&'static str>), CipherscanRestError> {
    validate_address_activity_values(entry)?;
    let senders = other_transparent_addresses(
        network,
        requested_address,
        &entry.other_input_script_pub_keys,
    );
    let recipients = other_transparent_addresses(
        network,
        requested_address,
        &entry.other_output_script_pub_keys,
    );
    let mut unavailable = Vec::new();
    if !entry.input_facts_complete {
        unavailable.push(
            "Transparent input coverage is incomplete; senderCount is partial and a receiving counterparty is unavailable.",
        );
    }
    if entry.component_counts.is_none()
        || entry.transaction_index.is_none()
        || entry.size_bytes.is_none()
        || entry.output_value_zat.is_none()
    {
        unavailable.push(
            "Retained canonical transaction facts are unavailable for this address activity row.",
        );
    }
    let counterparty = match entry.net_value_zat {
        Some(net_change) if net_change > 0 && entry.input_facts_complete => senders.first(),
        Some(_) => recipients.first(),
        None => None,
    };
    let counts = entry.component_counts.as_ref();

    Ok((
        json!({
            "txid": entry.transaction_id,
            "blockHeight": entry.block_height,
            "blockTime": entry.block_time_unix_seconds.to_string(),
            "size": entry.size_bytes,
            "txIndex": entry.transaction_index,
            "hasSapling": counts.map(has_sapling_counts),
            "hasOrchard": counts.map(|counts| counts.orchard_action_count > 0),
            "hasIronwood": counts.map(|counts| counts.ironwood_action_count > 0),
            "inputValue": entry.input_value_zat,
            "outputValue": entry.output_value_zat,
            "netChange": entry.net_value_zat,
            "counterparty": counterparty,
            "senderCount": senders.len(),
            "recipientCount": recipients.len(),
            "zinderUnavailable": unavailable,
        }),
        unavailable,
    ))
}

fn validate_address_activity_values(
    entry: &explorer::TransparentAddressActivityEntry,
) -> Result<(), CipherscanRestError> {
    if let (Some(input), Some(output), Some(net_change)) = (
        entry.input_value_zat,
        entry.output_value_zat,
        entry.net_value_zat,
    ) {
        let expected = i128::from(output) - i128::from(input);
        if expected != i128::from(net_change) {
            return Err(CipherscanRestError::InvalidUpstreamField(
                "transparent_address_activity.entries.net_value_zat",
            ));
        }
    }
    Ok(())
}

fn other_transparent_addresses(
    network: Network,
    requested_address: &str,
    scripts: &[Vec<u8>],
) -> Vec<String> {
    let mut seen = HashSet::new();
    scripts
        .iter()
        .filter_map(|script| cipherscan_transparent_address(network, script))
        .filter(|address| address != requested_address)
        .filter(|address| seen.insert(address.clone()))
        .collect()
}

fn address_activity_unavailable(
    summary: &explorer::TransparentAddressSummary,
    coverage: &explorer::TransparentAddressRankingCoverage,
    chain_epoch: &wallet::ChainEpoch,
    freshness: Option<&explorer::ExplorerFreshness>,
    is_zero_history: bool,
) -> Vec<&'static str> {
    let mut unavailable = Vec::new();
    let visible_tip_height = chain_epoch.visible_tip.as_ref().map(|tip| tip.height);
    if visible_tip_height.is_none_or(|height| coverage.balance_complete_through_height < height) {
        unavailable.push("The native transparent-address summary has incomplete balance coverage.");
    }
    if !coverage.lifetime_statistics_complete {
        unavailable.push("Transparent-address lifetime history coverage is incomplete.");
    }
    let indexed_tip = freshness
        .and_then(|freshness| freshness.chain_view.as_ref())
        .and_then(|chain_view| chain_view.indexed_tip.as_ref())
        .and_then(|indexed_tip| indexed_tip.tip.as_ref());
    let indexed_tip_matches_epoch = chain_epoch
        .visible_tip
        .as_ref()
        .zip(indexed_tip)
        .is_some_and(|(visible_tip, indexed_tip)| {
            visible_tip.height == indexed_tip.height && visible_tip.hash == indexed_tip.hash
        });
    if !indexed_tip_matches_epoch {
        unavailable.push(
            "Transparent-address materialized views do not match the pinned canonical tip yet.",
        );
    }
    if !is_zero_history
        && (summary.total_received_zat.is_none()
            || summary.total_sent_zat.is_none()
            || summary.distinct_transaction_count.is_none()
            || summary.first_seen_unix_seconds.is_none()
            || summary.last_seen_unix_seconds.is_none())
    {
        unavailable
            .push("One or more transparent-address lifetime summary fields are unavailable.");
    }
    unavailable
}

fn push_unique_reason(reasons: &mut Vec<&'static str>, reason: &'static str) {
    if !reasons.contains(&reason) {
        reasons.push(reason);
    }
}

fn broadcast_response(outcome: Option<broadcast_transaction_response::Outcome>) -> Response {
    match outcome {
        Some(broadcast_transaction_response::Outcome::Accepted(accepted)) => json_response(
            StatusCode::OK,
            json!({
                "success": true,
                "txid": accepted.transaction_id,
            }),
        ),
        Some(broadcast_transaction_response::Outcome::Duplicate(duplicate)) => json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "success": false,
                "duplicate": true,
                "error": duplicate.message,
                "errorCode": duplicate.error_code,
                "reason": "duplicate",
            }),
        ),
        Some(broadcast_transaction_response::Outcome::Queued(queued)) => json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "success": false,
                "queued": true,
                "error": queued.message,
                "reason": "queued",
            }),
        ),
        Some(broadcast_transaction_response::Outcome::InvalidEncoding(invalid)) => json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "success": false,
                "error": invalid.message,
                "errorCode": invalid.error_code,
                "reason": "invalid_encoding",
            }),
        ),
        Some(broadcast_transaction_response::Outcome::Rejected(rejected)) => json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "success": false,
                "error": rejected.message,
                "errorCode": rejected.error_code,
                "reason": rejected.kind,
            }),
        ),
        Some(broadcast_transaction_response::Outcome::Unknown(unknown)) => json_response(
            StatusCode::BAD_GATEWAY,
            json!({
                "success": false,
                "error": unknown.message,
                "errorCode": unknown.error_code,
                "reason": "unknown",
            }),
        ),
        None => json_response(
            StatusCode::BAD_GATEWAY,
            json!({
                "success": false,
                "error": "Zinder returned no broadcast outcome",
            }),
        ),
    }
}

#[derive(Debug)]
struct InvalidRequestBodyDetail {
    field: String,
    message: String,
}

impl InvalidRequestBodyDetail {
    fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
        }
    }

    fn into_response(self) -> Response {
        json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "error": "Invalid request body",
                "details": [
                    {
                        "field": self.field,
                        "message": self.message,
                    }
                ],
            }),
        )
    }
}

fn parse_raw_transaction_batch_txids(
    body: &Value,
) -> Result<Vec<String>, InvalidRequestBodyDetail> {
    let Some(txids) = body.get("txids") else {
        return Err(InvalidRequestBodyDetail::new(
            "txids",
            "Invalid input: expected array, received undefined",
        ));
    };
    let Some(txids) = txids.as_array() else {
        return Err(InvalidRequestBodyDetail::new(
            "txids",
            format!(
                "Invalid input: expected array, received {}",
                json_type_name(txids)
            ),
        ));
    };

    if txids.is_empty() {
        return Err(InvalidRequestBodyDetail::new(
            "txids",
            "Too small: expected array to have >=1 items",
        ));
    }
    if txids.len() > MAX_RAW_TRANSACTION_BATCH_SIZE {
        return Err(InvalidRequestBodyDetail::new(
            "txids",
            "Too big: expected array to have <=1000 items",
        ));
    }

    let mut transaction_ids = Vec::with_capacity(txids.len());
    for (index, transaction_id) in txids.iter().enumerate() {
        let Some(transaction_id) = transaction_id.as_str() else {
            return Err(InvalidRequestBodyDetail::new(
                format!("txids.{index}"),
                "Invalid transaction ID",
            ));
        };
        if !is_rpc_transaction_id(transaction_id) {
            return Err(InvalidRequestBodyDetail::new(
                format!("txids.{index}"),
                "Invalid transaction ID",
            ));
        }
        transaction_ids.push(transaction_id.to_owned());
    }

    Ok(transaction_ids)
}

fn parse_broadcast_raw_transaction(body: &Value) -> Result<String, InvalidRequestBodyDetail> {
    let Some(raw_tx) = body.get("rawTx") else {
        return Err(InvalidRequestBodyDetail::new(
            "rawTx",
            "Invalid input: expected string, received undefined",
        ));
    };
    let Some(raw_tx) = raw_tx.as_str() else {
        return Err(InvalidRequestBodyDetail::new(
            "rawTx",
            format!(
                "Invalid input: expected string, received {}",
                json_type_name(raw_tx)
            ),
        ));
    };
    if raw_tx.is_empty() || !raw_tx.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(InvalidRequestBodyDetail::new(
            "rawTx",
            "rawTx must be a valid hex string",
        ));
    }

    Ok(raw_tx.to_owned())
}

fn parse_scan_range(body: &Value, require_end_height: bool) -> Result<ScanRange, &'static str> {
    let start_height = match scan_height_field(body, "startHeight") {
        ScanHeightField::Missing => {
            return Err(if require_end_height {
                "startHeight and endHeight are required"
            } else {
                "startHeight is required"
            });
        }
        ScanHeightField::Invalid => return Err("Invalid block heights"),
        ScanHeightField::Present(start_height) => start_height,
    };

    let end_height = match scan_height_field(body, "endHeight") {
        ScanHeightField::Missing if require_end_height => {
            return Err("startHeight and endHeight are required");
        }
        ScanHeightField::Missing => start_height,
        ScanHeightField::Invalid => return Err("Invalid block heights"),
        ScanHeightField::Present(end_height) => end_height,
    };

    if start_height > end_height {
        return Err("startHeight cannot be greater than endHeight");
    }
    if end_height.saturating_sub(start_height) > MAX_SCAN_RANGE_BLOCKS {
        return Err("Range too large (max 1 million blocks)");
    }

    Ok(ScanRange {
        start_height,
        end_height,
    })
}

fn parse_orchard_candidate_scan_range(
    body: &Value,
) -> Result<OrchardCandidateScanRange, &'static str> {
    if ORCHARD_CANDIDATE_SCAN_VIEWING_KEY_FIELDS
        .iter()
        .any(|field| body.get(*field).is_some())
    {
        return Err("Viewing keys are not accepted");
    }
    let range = parse_scan_range(body, true)?;
    if range.end_height.saturating_sub(range.start_height)
        > u64::from(MAX_ORCHARD_CANDIDATE_SCAN_BLOCKS)
    {
        return Err("Range too large (max 8064 blocks)");
    }
    Ok(OrchardCandidateScanRange {
        start_height: u32::try_from(range.start_height).map_err(|_| "Invalid block heights")?,
        end_height: u32::try_from(range.end_height).map_err(|_| "Invalid block heights")?,
    })
}

fn scan_height_field(body: &Value, field: &str) -> ScanHeightField {
    let Some(height_json) = body.get(field) else {
        return ScanHeightField::Missing;
    };
    scan_height_value(height_json).map_or(ScanHeightField::Invalid, ScanHeightField::Present)
}

fn scan_height_value(height_json: &Value) -> Option<u64> {
    if let Some(height) = height_json.as_u64() {
        return Some(height);
    }
    height_json
        .as_str()
        .and_then(|height| height.trim().parse::<u64>().ok())
}

fn scan_bad_request_response(error: &str) -> Response {
    json_response(StatusCode::BAD_REQUEST, json!({ "error": error }))
}

fn json_type_name(json_value: &Value) -> &'static str {
    match json_value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn raw_transaction_batch_row(transaction_id: &str, raw_bytes: &[u8]) -> Value {
    json!({
        "txid": transaction_id,
        "hex": hex::encode(raw_bytes),
    })
}

fn verbose_transaction_json(transaction_id: &str, raw_bytes: &[u8]) -> Value {
    json!({
        "txid": transaction_id,
        "hex": hex::encode(raw_bytes),
        "decoded": {
            "txid": transaction_id,
            "degraded": true,
            "source": CIPHERSCAN_ADAPTER_SOURCE,
            "unavailable": [
                "Zebra getrawtransaction verbosity=1 JSON is not exposed by Zinder's native read APIs.",
                "The adapter returns retained raw transaction bytes without inventing a native decoded JSON contract."
            ]
        },
        "degraded": true,
        "unavailable": [
            "decoded"
        ],
    })
}

fn raw_transaction_batch_failure(transaction_id: &str, error: &str) -> Value {
    json!({
        "txid": transaction_id,
        "error": error,
        "success": false,
    })
}

fn raw_transaction_batch_json(transactions: &[Value], failed: &[Value], total: usize) -> Value {
    let successful = transactions.len();
    let mut response = json!({
        "transactions": transactions,
        "total": total,
        "successful": successful,
    });
    if !failed.is_empty() {
        response["failed"] = json!(failed);
    }
    response
}

fn orchard_candidate_scan_json(
    range: OrchardCandidateScanRange,
    entries: &[explorer::TransactionHistoryEntry],
) -> Value {
    json!({
        "startHeight": range.start_height,
        "endHeight": range.end_height,
        "totalBlocks": range.total_blocks(),
        "orchardTransactions": entries.len(),
        "transactions": entries.iter().map(|entry| json!({
            "txid": entry.transaction_id,
            "block_height": entry.block_height.to_string(),
            "timestamp": entry.block_time_unix_seconds.to_string(),
        })).collect::<Vec<_>>(),
    })
}

fn chain_info_json(height: u32) -> Value {
    let height_string = height.to_string();
    json!({
        "blocks": height_string,
        "height": height_string,
    })
}

fn network_health_json(
    explorer: &explorer::ServerInfoResponse,
    wallet: &wallet::ServerInfoResponse,
) -> Value {
    json!({
        "success": true,
        "adapter": {
            "healthy": true,
            "ready": true,
        },
        "zinder": {
            "explorer": upstream_service_json(explorer.info.as_ref().and_then(|server_descriptor| server_descriptor.common.as_ref())),
            "wallet": upstream_service_json(wallet.info.as_ref().and_then(|server_descriptor| server_descriptor.common.as_ref())),
        },
        "zebra": {
            "healthy": true,
            "ready": true,
            "healthEndpointAvailable": false,
            "readyEndpointAvailable": false,
            "source": "zinder-query-plane",
            "unavailable": "The adapter derives this compatibility status from successful Zinder explorer and wallet reads, not direct Zebra health endpoints."
        },
        "timestamp": current_unix_millis(),
    })
}

fn explorer_supports_capability(
    server_info: &explorer::ServerInfoResponse,
    capability: &str,
) -> bool {
    server_info
        .info
        .as_ref()
        .and_then(|server_descriptor| server_descriptor.common.as_ref())
        .is_some_and(|common| {
            common
                .capabilities
                .iter()
                .any(|candidate| candidate == capability)
        })
}

fn blockchain_info_json(network: Network, tip: &wallet::BlockId) -> Value {
    json!({
        "chain": encode_bip70_chain_name(network),
        "blocks": tip.height,
        "headers": tip.height,
        "bestblockhash": tip.block_hash,
        "estimatedheight": tip.height,
        "verificationprogress": 1.0,
        "initialblockdownload": false,
        "pruned": false,
        "difficulty": 0.0,
        "chainwork": Value::Null,
        "size_on_disk": 0,
        "consensus": {
            "chaintip": Value::Null,
            "nextblock": Value::Null,
        },
        "upgrades": {},
        "valuePools": Value::Null,
        "degraded": true,
        "source": CIPHERSCAN_ADAPTER_SOURCE,
        "unavailable": [
            "The adapter exposes the current indexed tip in getblockchaininfo-compatible fields; Zebra RPC-only fields are not native Zinder explorer facts."
        ],
    })
}

fn raw_transaction_bytes(location: Option<&wallet::TransactionLocation>) -> Option<&[u8]> {
    match location.and_then(|location| location.location.as_ref()) {
        Some(transaction_location::Location::Mined(mined)) => {
            mined.raw_transaction_bytes.as_deref()
        }
        Some(transaction_location::Location::InMempool(mempool)) => {
            Some(mempool.raw_transaction_bytes.as_slice())
        }
        None => None,
    }
}

fn mined_location(
    location: Option<&wallet::TransactionLocation>,
) -> Option<&wallet::MinedTransaction> {
    match location.and_then(|location| location.location.as_ref()) {
        Some(transaction_location::Location::Mined(mined)) => Some(mined),
        Some(transaction_location::Location::InMempool(_)) | None => None,
    }
}

fn mempool_location(
    location: Option<&wallet::TransactionLocation>,
) -> Option<&wallet::MempoolEntry> {
    match location.and_then(|location| location.location.as_ref()) {
        Some(transaction_location::Location::InMempool(mempool)) => Some(mempool),
        Some(transaction_location::Location::Mined(_)) | None => None,
    }
}

const fn mempool_first_seen_unix_seconds(mempool: &wallet::MempoolEntry) -> u64 {
    mempool.first_seen_unix_millis / 1_000
}

fn transaction_status(location: Option<&wallet::TransactionLocation>) -> &'static str {
    match location.and_then(|location| location.location.as_ref()) {
        Some(transaction_location::Location::Mined(_)) => "mined",
        Some(transaction_location::Location::InMempool(_)) => "mempool",
        None => "unknown",
    }
}

fn lock_time_json(lock_time: Option<&explorer::LockTime>) -> Value {
    match lock_time.and_then(|lock_time| lock_time.kind.as_ref()) {
        Some(lock_time::Kind::Unlocked(_)) => json!(0),
        Some(lock_time::Kind::Height(height)) => json!(height),
        Some(lock_time::Kind::UnixSeconds(unix_seconds)) => json!(unix_seconds),
        None => Value::Null,
    }
}

fn cipherscan_lock_time_string(lock_time: Option<&explorer::LockTime>) -> Option<String> {
    match lock_time.and_then(|lock_time| lock_time.kind.as_ref()) {
        Some(lock_time::Kind::Unlocked(_)) => Some("0".to_owned()),
        Some(lock_time::Kind::Height(height)) => Some(height.to_string()),
        Some(lock_time::Kind::UnixSeconds(unix_seconds)) => Some(unix_seconds.to_string()),
        None => None,
    }
}

fn cipherscan_transaction_history_direction(
    direction: Option<&str>,
) -> TransactionHistoryDirection {
    if direction == Some("prev") {
        TransactionHistoryDirection::Newer
    } else {
        TransactionHistoryDirection::Older
    }
}

fn transaction_list_history_filter(requested_type: Option<&str>) -> TransactionHistoryFilter {
    match requested_type {
        Some("shielded") => TransactionHistoryFilter {
            contains_any_protocol: vec![
                explorer::ShieldedProtocol::Sapling as i32,
                explorer::ShieldedProtocol::Orchard as i32,
                explorer::ShieldedProtocol::Ironwood as i32,
            ],
            ..TransactionHistoryFilter::default()
        },
        Some("transparent") => TransactionHistoryFilter {
            is_coinbase: Some(false),
            privacy_shapes: vec![explorer::PrivacyShape::TransparentOnly as i32],
            ..TransactionHistoryFilter::default()
        },
        Some("coinbase") => TransactionHistoryFilter {
            is_coinbase: Some(true),
            ..TransactionHistoryFilter::default()
        },
        None | Some("all" | _) => TransactionHistoryFilter::default(),
    }
}

fn parse_shielded_transaction_query(
    raw_query: ShieldedTransactionQuery,
) -> Result<PageQuery, Vec<Value>> {
    let mut details = Vec::new();
    let limit = parse_bounded_integer_query(
        raw_query.limit.as_deref(),
        "limit",
        1,
        Some(100),
        &mut details,
    );
    let offset =
        parse_bounded_integer_query(raw_query.offset.as_deref(), "offset", 0, None, &mut details);
    if raw_query
        .pool
        .as_deref()
        .is_some_and(|pool| !matches!(pool, "sapling" | "orchard" | "ironwood"))
    {
        details.push(json!({
            "field": "pool",
            "message": "Invalid option: expected one of \"sapling\"|\"orchard\"|\"ironwood\"",
        }));
    }
    if raw_query
        .transaction_type
        .as_deref()
        .is_some_and(|transaction_type| !matches!(transaction_type, "fully-shielded" | "partial"))
    {
        details.push(json!({
            "field": "type",
            "message": "Invalid option: expected one of \"fully-shielded\"|\"partial\"",
        }));
    }
    let min_actions = parse_bounded_integer_query(
        raw_query.min_actions.as_deref(),
        "min_actions",
        0,
        None,
        &mut details,
    );
    if raw_query
        .skip_count
        .as_deref()
        .is_some_and(|skip_count| !matches!(skip_count, "true" | "false"))
    {
        details.push(json!({
            "field": "skip_count",
            "message": "Invalid option: expected one of \"true\"|\"false\"",
        }));
    }
    if !details.is_empty() {
        return Err(details);
    }

    Ok(PageQuery {
        limit,
        offset,
        pool: raw_query.pool,
        min_actions,
        skip_count: raw_query.skip_count,
        transaction_type: raw_query.transaction_type,
        ..PageQuery::default()
    })
}

fn parse_bounded_integer_query(
    raw_value: Option<&str>,
    field: &'static str,
    minimum: u32,
    maximum: Option<u32>,
    details: &mut Vec<Value>,
) -> Option<u32> {
    let raw_value = raw_value?.trim();
    let parsed_number = if raw_value.is_empty() {
        Some(0.0)
    } else if let Some(hexadecimal) = raw_value
        .strip_prefix("0x")
        .or_else(|| raw_value.strip_prefix("0X"))
    {
        u32::from_str_radix(hexadecimal, 16).ok().map(f64::from)
    } else {
        raw_value.parse::<f64>().ok()
    };
    let Some(number) = parsed_number else {
        details.push(json!({
            "field": field,
            "message": "Invalid input: expected number, received NaN",
        }));
        return None;
    };
    if !number.is_finite() {
        details.push(json!({
            "field": field,
            "message": "Invalid input: expected number, received NaN",
        }));
        return None;
    }
    if number.fract() != 0.0 {
        details.push(json!({
            "field": field,
            "message": "Invalid input: expected int, received number",
        }));
        return None;
    }
    if number < f64::from(minimum) {
        details.push(json!({
            "field": field,
            "message": format!("Too small: expected number to be >={minimum}"),
        }));
        return None;
    }
    if let Some(maximum) = maximum
        && number > f64::from(maximum)
    {
        details.push(json!({
            "field": field,
            "message": format!("Too big: expected number to be <={maximum}"),
        }));
        return None;
    }
    if number > f64::from(u32::MAX) {
        details.push(json!({
            "field": field,
            "message": format!("Too big: expected number to be <= {}", u32::MAX),
        }));
        return None;
    }
    number.to_string().parse().ok()
}

fn shielded_transaction_history_filter(
    query: &PageQuery,
) -> Result<TransactionHistoryFilter, CipherscanRestError> {
    let mut filter = shielded_protocol_history_filter(query.pool.as_deref())?;
    filter.privacy_shapes = match query.transaction_type.as_deref() {
        None | Some("all") => Vec::new(),
        Some("fully-shielded") => vec![explorer::PrivacyShape::ShieldedOnly as i32],
        Some("partial") => vec![
            explorer::PrivacyShape::Shielding as i32,
            explorer::PrivacyShape::Deshielding as i32,
            explorer::PrivacyShape::Mixed as i32,
            explorer::PrivacyShape::ShieldedCoinbase as i32,
        ],
        Some(requested_type) => {
            return Err(CipherscanRestError::InvalidRequest(format!(
                "unsupported shielded transaction type: {requested_type}"
            )));
        }
    };
    filter.minimum_shielded_component_count = query.min_actions.unwrap_or(0);
    Ok(filter)
}

fn shielded_protocol_history_filter(
    requested_pool: Option<&str>,
) -> Result<TransactionHistoryFilter, CipherscanRestError> {
    let contains_any_protocol = match requested_pool {
        None | Some("all") => vec![
            explorer::ShieldedProtocol::Sapling as i32,
            explorer::ShieldedProtocol::Orchard as i32,
            explorer::ShieldedProtocol::Ironwood as i32,
        ],
        Some("sapling") => vec![explorer::ShieldedProtocol::Sapling as i32],
        Some("orchard") => vec![explorer::ShieldedProtocol::Orchard as i32],
        Some("ironwood") => vec![explorer::ShieldedProtocol::Ironwood as i32],
        Some(requested_pool) => {
            return Err(CipherscanRestError::InvalidRequest(format!(
                "unsupported shielded pool: {requested_pool}"
            )));
        }
    };
    Ok(TransactionHistoryFilter {
        contains_any_protocol,
        ..TransactionHistoryFilter::default()
    })
}

fn has_shielded_components(counts: Option<&explorer::TransactionComponentCounts>) -> bool {
    counts.is_some_and(|counts| {
        counts.sapling_spend_count > 0
            || counts.sapling_output_count > 0
            || counts.orchard_action_count > 0
            || counts.ironwood_action_count > 0
            || counts.sprout_joinsplit_count > 0
    })
}

fn has_transparent_components(counts: Option<&explorer::TransactionComponentCounts>) -> bool {
    counts.is_some_and(|counts| {
        counts.transparent_input_count > 0 || counts.transparent_output_count > 0
    })
}

fn compat_transaction_type(counts: Option<&explorer::TransactionComponentCounts>) -> &'static str {
    match (
        has_shielded_components(counts),
        has_transparent_components(counts),
    ) {
        (true, true) => "mixed",
        (true, false) => "shielded",
        (false, _) => "transparent",
    }
}

fn shielded_flow_type(entry: &explorer::TransactionHistoryEntry) -> &'static str {
    let Some(counts) = entry.component_counts.as_ref() else {
        return "mixed";
    };
    let has_transparent_inputs = counts.transparent_input_count > 0;
    let has_transparent_outputs = counts.transparent_output_count > 0;

    match (has_transparent_inputs, has_transparent_outputs) {
        (true, false) => "shield",
        (false, true) => "deshield",
        (false, false) => "fully-shielded",
        (true, true) => "mixed",
    }
}

fn shielded_flow_type_or_none(entry: &explorer::TransactionHistoryEntry) -> Option<&'static str> {
    has_shielded_components(entry.component_counts.as_ref()).then(|| shielded_flow_type(entry))
}

fn has_sapling_counts(counts: &explorer::TransactionComponentCounts) -> bool {
    counts.sapling_spend_count > 0 || counts.sapling_output_count > 0
}

struct CipherscanBlockHeaderFields {
    difficulty: f64,
    bits: String,
    nonce: String,
}

fn cipherscan_block_header_fields(
    network: Network,
    header: &wallet::BlockHeader,
) -> Result<CipherscanBlockHeaderFields, CipherscanRestError> {
    let mut rpc_nonce = header.nonce.clone();
    rpc_nonce.reverse();

    Ok(CipherscanBlockHeaderFields {
        difficulty: cipherscan_difficulty(network, header.bits)?,
        bits: hex::encode(header.bits.to_be_bytes()),
        nonce: hex::encode(rpc_nonce),
    })
}

fn cipherscan_difficulty(network: Network, bits: u32) -> Result<f64, CipherscanRestError> {
    let compact_difficulty = CompactDifficulty::from_bytes_in_display_order(&bits.to_be_bytes())
        .map_err(chain_economics_error)?;
    Ok(compact_difficulty.relative_to_network(&zebra_network_for(network)?))
}

fn cipherscan_difficulty_string(difficulty: f64) -> String {
    const LEGACY_SIGNIFICANT_DIGITS: i32 = 15;
    let mut integer_digits = 0_i32;
    let mut remaining_integer = difficulty;
    while remaining_integer >= 1.0 && integer_digits < LEGACY_SIGNIFICANT_DIGITS {
        integer_digits += 1;
        remaining_integer /= 10.0;
    }
    let decimal_places = usize::try_from(
        LEGACY_SIGNIFICANT_DIGITS
            .saturating_sub(integer_digits)
            .max(0),
    )
    .unwrap_or(0);
    let mut formatted = format!("{difficulty:.decimal_places$}");
    if formatted.contains('.') {
        while formatted.ends_with('0') {
            formatted.pop();
        }
        if formatted.ends_with('.') {
            formatted.pop();
        }
    }
    formatted
}

fn cipherscan_hashrate_string(hashrate: f64) -> String {
    let (scaled, unit) = if hashrate >= 1e12 {
        (hashrate / 1e12, "TH/s")
    } else if hashrate >= 1e9 {
        (hashrate / 1e9, "GH/s")
    } else if hashrate >= 1e6 {
        (hashrate / 1e6, "MH/s")
    } else if hashrate >= 1e3 {
        (hashrate / 1e3, "KH/s")
    } else {
        (hashrate, "H/s")
    };
    format!("{scaled:.2} {unit}")
}

fn cipherscan_block_finality_status(summary: &explorer::BlockSummary) -> &'static str {
    if summary.confirmations >= 100 {
        "Finalized"
    } else {
        "NotYetFinalized"
    }
}

fn upstream_service_json(upstream_info: Option<&zinder_proto::v1::ops::ServerInfo>) -> Value {
    json!({
        "service": upstream_info.map(|server_info| server_info.service_name.as_str()),
        "version": upstream_info.map(|server_info| server_info.service_version.as_str()),
        "network": upstream_info.map(|server_info| server_info.network.as_str()),
        "capabilities": upstream_info.map_or_else(Vec::new, |server_info| server_info.capabilities.clone()),
    })
}

#[derive(Debug)]
struct ChainSubsidySummary {
    network: Network,
    zebra_network: ZebraNetwork,
    current_height: ZebraHeight,
    active_upgrade: NetworkUpgrade,
    current_subsidy_zec: f64,
    next_subsidy_zec: Option<f64>,
    current_miner_subsidy_zec: f64,
    next_miner_subsidy_zec: Option<f64>,
    current_funding_streams_zec: f64,
    current_lockbox_zec: f64,
    halving_block: Option<ZebraHeight>,
    blocks_remaining: Option<u32>,
    era_start_block: Option<ZebraHeight>,
    era_progress_pct: Option<f64>,
    estimated_seconds: Option<i64>,
    estimated_date: Option<String>,
    daily_emission_estimate_zec: f64,
}

#[derive(Debug)]
struct ChainSupplySummary {
    chain_supply_zats: i64,
    chain_supply_zec: f64,
    remaining_supply_zec: f64,
    circulating_pct: f64,
}

fn derive_chain_subsidy_summary(
    network: Network,
    current_height: u32,
) -> Result<ChainSubsidySummary, CipherscanRestError> {
    let zebra_network = zebra_network_for(network)?;
    let current_height = zebra_height(current_height)?;
    let active_upgrade = NetworkUpgrade::current(&zebra_network, current_height);
    let current_subsidy = chain_block_subsidy(current_height, &zebra_network)?;
    let current_miner_subsidy = miner_subsidy(current_height, &zebra_network, current_subsidy)
        .map_err(chain_economics_error)?;
    let funding_stream_values =
        funding_stream_values(current_height, &zebra_network, current_subsidy)
            .map_err(chain_economics_error)?;
    let current_lockbox_zec = funding_stream_values
        .get(&FundingStreamReceiver::Deferred)
        .map_or(0.0, |amount| zec_from_amount(*amount));
    let current_funding_streams_zec = funding_stream_values
        .iter()
        .filter(|(receiver, _)| **receiver != FundingStreamReceiver::Deferred)
        .map(|(_, amount)| zec_from_amount(*amount))
        .sum();
    let current_halving = subsidy::halving(current_height, &zebra_network);
    let next_halving = current_halving
        .checked_add(1)
        .ok_or_else(|| chain_economics_unavailable("halving index overflowed"))?;
    let era_start_block = height_for_halving(current_halving, &zebra_network);
    let halving_block = height_for_halving(next_halving, &zebra_network);
    let next_subsidy = halving_block
        .map(|height| chain_block_subsidy(height, &zebra_network))
        .transpose()?;
    let next_miner_subsidy = match (halving_block, next_subsidy) {
        (Some(height), Some(subsidy)) => {
            Some(miner_subsidy(height, &zebra_network, subsidy).map_err(chain_economics_error)?)
        }
        (Some(_), None) | (None, _) => None,
    };
    let blocks_remaining = halving_block.map(|height| height.0.saturating_sub(current_height.0));
    let era_progress_pct = match (era_start_block, halving_block) {
        (Some(start), Some(end)) if end > start => {
            Some(progress_pct(current_height - start, end - start))
        }
        (Some(_), Some(_) | None) | (None, _) => None,
    };
    let target_spacing_seconds = active_upgrade.target_spacing().num_seconds();
    let estimated_seconds =
        blocks_remaining.and_then(|blocks| i64::from(blocks).checked_mul(target_spacing_seconds));
    let estimated_date = estimated_seconds
        .map(|seconds| rfc3339_timestamp(OffsetDateTime::now_utc() + Duration::seconds(seconds)));
    let blocks_per_day = SECONDS_PER_DAY / seconds_to_f64(target_spacing_seconds);
    let current_subsidy_zec = zec_from_amount(current_subsidy);
    let current_miner_subsidy_zec = zec_from_amount(current_miner_subsidy);

    Ok(ChainSubsidySummary {
        network,
        zebra_network,
        current_height,
        active_upgrade,
        current_subsidy_zec,
        next_subsidy_zec: next_subsidy.map(zec_from_amount),
        current_miner_subsidy_zec,
        next_miner_subsidy_zec: next_miner_subsidy.map(zec_from_amount),
        current_funding_streams_zec,
        current_lockbox_zec,
        halving_block,
        blocks_remaining,
        era_start_block,
        era_progress_pct,
        estimated_seconds,
        estimated_date,
        daily_emission_estimate_zec: current_subsidy_zec * blocks_per_day,
    })
}

fn derive_chain_supply_summary(
    subsidy_summary: &ChainSubsidySummary,
) -> Result<ChainSupplySummary, CipherscanRestError> {
    let chain_supply_zats = chain_supply_zats(
        subsidy_summary.current_height,
        &subsidy_summary.zebra_network,
    )?;
    chain_supply_summary_from_zats(chain_supply_zats)
}

fn chain_supply_summary_from_zats(
    chain_supply_zats: i64,
) -> Result<ChainSupplySummary, CipherscanRestError> {
    if chain_supply_zats < 0 {
        return Err(CipherscanRestError::InvalidUpstreamField(
            "chain_supply_zats",
        ));
    }
    let chain_supply_zec = zec_from_zatoshis(chain_supply_zats);
    let remaining_supply_zec = (MAX_SUPPLY_ZEC - chain_supply_zec).max(0.0);
    let circulating_pct = if chain_supply_zec > 0.0 {
        (chain_supply_zec / MAX_SUPPLY_ZEC) * 100.0
    } else {
        0.0
    };

    Ok(ChainSupplySummary {
        chain_supply_zats,
        chain_supply_zec,
        remaining_supply_zec,
        circulating_pct,
    })
}

fn halving_json(subsidy_summary: &ChainSubsidySummary) -> Value {
    json!({
        "success": true,
        "cached": false,
        "currentHeight": subsidy_summary.current_height.0,
        "halvingBlock": subsidy_summary.halving_block.map(|height| height.0),
        "blocksRemaining": subsidy_summary.blocks_remaining,
        "eraStartBlock": subsidy_summary.era_start_block.map(|height| height.0),
        "eraProgress": subsidy_summary.era_progress_pct,
        "currentSubsidy": subsidy_summary.current_subsidy_zec,
        "nextSubsidy": subsidy_summary.next_subsidy_zec,
        "minerReward": subsidy_summary.current_miner_subsidy_zec,
        "nextMinerReward": subsidy_summary.next_miner_subsidy_zec,
        "fundingStreams": subsidy_summary.current_funding_streams_zec,
        "lockbox": subsidy_summary.current_lockbox_zec,
        "estimatedSeconds": subsidy_summary.estimated_seconds,
        "estimatedDate": subsidy_summary.estimated_date,
    })
}

fn network_supply_json(
    chain_name: &str,
    active_upgrade: &str,
    pools: &[wallet::ChainValuePool],
) -> Result<Value, CipherscanRestError> {
    validate_value_pools(pools)?;
    if !value_pools_are_complete(pools) {
        return Err(CipherscanRestError::MissingUpstreamField(
            "value_pool_summary.pools.chain_value_zat",
        ));
    }
    let transparent_zat = required_value_pool_zat(pools, "transparent")?;
    let sprout_zat = required_value_pool_zat(pools, "sprout")?;
    let sapling_zat = required_value_pool_zat(pools, "sapling")?;
    let orchard_zat = required_value_pool_zat(pools, "orchard")?;
    let ironwood_zat = required_value_pool_zat(pools, "ironwood")?;
    let lockbox_zat = required_value_pool_zat(pools, "lockbox")?;
    let total_shielded_zat = [sprout_zat, sapling_zat, orchard_zat, ironwood_zat]
        .into_iter()
        .try_fold(0_i64, |total_zat, pool_value_zat| {
            total_zat
                .checked_add(pool_value_zat)
                .ok_or(CipherscanRestError::InvalidUpstreamField(
                    "value_pool_summary.pools.chain_value_zat",
                ))
        })?;
    let chain_supply_zat = complete_chain_supply_zat(pools)?.ok_or(
        CipherscanRestError::MissingUpstreamField("value_pool_summary.pools.chain_value_zat"),
    )?;
    let total_shielded_zec = zec_from_zatoshis(total_shielded_zat);
    let chain_supply_zec = zec_from_zatoshis(chain_supply_zat);
    let shielded_percentage = if chain_supply_zec == 0.0 {
        0.0
    } else {
        (total_shielded_zec / chain_supply_zec) * 100.0
    };
    let has_unknown_nonzero_pool = has_unknown_nonzero_value_pool(pools);
    let is_degraded = has_unknown_nonzero_pool;
    let mut unavailable = Vec::new();
    if has_unknown_nonzero_pool {
        unavailable.push(UNKNOWN_VALUE_POOL_SEMANTICS_UNAVAILABLE);
    }

    Ok(json!({
        "chainSupply": chain_supply_zec,
        "transparent": zec_from_zatoshis(transparent_zat),
        "sprout": zec_from_zatoshis(sprout_zat),
        "sapling": zec_from_zatoshis(sapling_zat),
        "orchard": zec_from_zatoshis(orchard_zat),
        "ironwood": zec_from_zatoshis(ironwood_zat),
        "lockbox": zec_from_zatoshis(lockbox_zat),
        "totalShielded": total_shielded_zec,
        "shieldedPercentage": shielded_percentage,
        "activeUpgrade": active_upgrade,
        "chain": chain_name,
        "degraded": is_degraded,
        "unavailable": unavailable,
    }))
}

fn chain_supply_zats(
    current_height: ZebraHeight,
    network: &ZebraNetwork,
) -> Result<i64, CipherscanRestError> {
    let mut chain_supply_zats = 0_i64;
    let end_exclusive = current_height
        .0
        .checked_add(1)
        .ok_or_else(|| chain_economics_unavailable("height range overflowed"))?;
    let slow_start_end = network.slow_start_interval().0.min(end_exclusive);

    for height in 0..slow_start_end {
        let subsidy_zats = chain_block_subsidy(ZebraHeight(height), network)?.zatoshis();
        add_chain_supply_zats(&mut chain_supply_zats, subsidy_zats)?;
    }

    let mut range_start = slow_start_end;
    while range_start < end_exclusive {
        let range_start_height = ZebraHeight(range_start);
        let range_subsidy_zats = chain_block_subsidy(range_start_height, network)?.zatoshis();
        let range_end = next_subsidy_boundary(range_start_height, end_exclusive, network)?;
        let block_count = range_end
            .checked_sub(range_start)
            .ok_or_else(|| chain_economics_unavailable("chain supply range underflowed"))?;
        let range_supply_zats = range_subsidy_zats
            .checked_mul(i64::from(block_count))
            .ok_or_else(|| chain_economics_unavailable("chain supply range overflowed"))?;
        add_chain_supply_zats(&mut chain_supply_zats, range_supply_zats)?;
        range_start = range_end;
    }

    Ok(chain_supply_zats)
}

fn next_subsidy_boundary(
    range_start: ZebraHeight,
    end_exclusive: u32,
    network: &ZebraNetwork,
) -> Result<u32, CipherscanRestError> {
    let current_halving = subsidy::halving(range_start, network);
    let next_halving = current_halving
        .checked_add(1)
        .and_then(|halving| height_for_halving(halving, network))
        .map_or(end_exclusive, |height| height.0);
    let next_blossom = NetworkUpgrade::Blossom
        .activation_height(network)
        .filter(|height| *height > range_start)
        .map_or(end_exclusive, |height| height.0);
    let range_end = next_halving.min(next_blossom).min(end_exclusive);

    if range_end <= range_start.0 {
        return Err(chain_economics_unavailable(
            "chain supply range did not advance",
        ));
    }

    Ok(range_end)
}

fn add_chain_supply_zats(total_zats: &mut i64, zats: i64) -> Result<(), CipherscanRestError> {
    *total_zats = total_zats
        .checked_add(zats)
        .ok_or_else(|| chain_economics_unavailable("chain supply overflowed"))?;
    Ok(())
}

fn chain_block_subsidy(
    height: ZebraHeight,
    network: &ZebraNetwork,
) -> Result<Amount<NonNegative>, CipherscanRestError> {
    block_subsidy(height, network).map_err(chain_economics_error)
}

fn zebra_network_for(network: Network) -> Result<ZebraNetwork, CipherscanRestError> {
    Ok(match network {
        Network::ZcashMainnet => ZebraNetwork::Mainnet,
        Network::ZcashTestnet => ZebraNetwork::new_default_testnet(),
        Network::ZcashRegtest => ZebraNetwork::new_regtest(RegtestParameters::default()),
        _ => return Err(chain_economics_unavailable("unsupported Zinder network")),
    })
}

fn zebra_height(height: u32) -> Result<ZebraHeight, CipherscanRestError> {
    ZebraHeight::try_from(height)
        .map_err(|_| chain_economics_unavailable("height is outside Zebra's supported range"))
}

fn migration_activation_height(network: Network) -> Option<u32> {
    let zebra_network = zebra_network_for(network).ok()?;
    NetworkUpgrade::Nu6_3
        .activation_height(&zebra_network)
        .map(|height| height.0)
}

fn cipherscan_network_name(network: Network) -> &'static str {
    match network {
        Network::ZcashMainnet => "mainnet",
        Network::ZcashTestnet => "testnet",
        Network::ZcashRegtest => "regtest",
        _ => "unknown",
    }
}

fn cipherscan_upgrade_name(upgrade: NetworkUpgrade) -> &'static str {
    match upgrade {
        NetworkUpgrade::Genesis => "Genesis",
        NetworkUpgrade::BeforeOverwinter => "BeforeOverwinter",
        NetworkUpgrade::Overwinter => "Overwinter",
        NetworkUpgrade::Sapling => "Sapling",
        NetworkUpgrade::Blossom => "Blossom",
        NetworkUpgrade::Heartwood => "Heartwood",
        NetworkUpgrade::Canopy => "Canopy",
        NetworkUpgrade::Nu5 => "NU5",
        NetworkUpgrade::Nu6 => "NU6",
        NetworkUpgrade::Nu6_1 => "NU6.1",
        NetworkUpgrade::Nu6_2 => "NU6.2",
        NetworkUpgrade::Nu6_3 => "NU6.3",
        NetworkUpgrade::Nu7 => "NU7",
    }
}

fn chain_economics_error(error: impl std::fmt::Display) -> CipherscanRestError {
    chain_economics_unavailable(error.to_string())
}

fn chain_economics_unavailable(reason: impl Into<String>) -> CipherscanRestError {
    CipherscanRestError::ChainEconomicsUnavailable(reason.into())
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Cipherscan's compatibility contract represents ZEC amounts as JSON floating-point numbers."
)]
fn zec_from_amount(amount: Amount<NonNegative>) -> f64 {
    zec_from_zatoshis(amount.zatoshis())
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Cipherscan's compatibility contract represents ZEC amounts as JSON floating-point numbers."
)]
fn zec_from_zatoshis(zatoshis: i64) -> f64 {
    zatoshis as f64 / ZATOSHIS_PER_ZEC
}

fn zec_from_unsigned_zatoshis(zatoshis: u64) -> f64 {
    i64::try_from(zatoshis).map_or(0.0, zec_from_zatoshis)
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Cipherscan's compatibility contract represents progress percentages as JSON floating-point numbers."
)]
fn progress_pct(numerator: i64, denominator: i64) -> f64 {
    if denominator <= 0 {
        return 0.0;
    }

    ((numerator as f64) / (denominator as f64) * 100.0).clamp(0.0, 100.0)
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Daily emission is a UI estimate serialized as a Cipherscan-compatible JSON float."
)]
fn seconds_to_f64(seconds: i64) -> f64 {
    seconds as f64
}

fn rfc3339_timestamp(timestamp: OffsetDateTime) -> String {
    timestamp
        .format(&Rfc3339)
        .unwrap_or_else(|_| String::from("1970-01-01T00:00:00Z"))
}

fn rfc3339_millis(timestamp_millis: u64) -> Value {
    let Ok(timestamp_seconds) = i64::try_from(timestamp_millis / 1_000) else {
        return Value::Null;
    };
    let Ok(timestamp) = OffsetDateTime::from_unix_timestamp(timestamp_seconds) else {
        return Value::Null;
    };

    json!(rfc3339_timestamp(timestamp))
}

fn current_rfc3339_timestamp() -> String {
    rfc3339_timestamp(OffsetDateTime::now_utc())
}

fn current_unix_millis() -> i64 {
    OffsetDateTime::now_utc()
        .unix_timestamp()
        .saturating_mul(1_000)
}

fn offset_pagination(limit: u32, offset: u32, total: u64) -> Value {
    let next_offset = offset.saturating_add(limit);
    json!({
        "limit": limit,
        "offset": offset,
        "total": total.to_string(),
        "hasMore": u64::from(next_offset) < total,
    })
}

fn block_list_pagination(
    page: u32,
    limit: u32,
    total: u64,
    entries: &[CipherscanBlockListEntry],
) -> Value {
    let limit_u64 = u64::from(limit);
    let total_pages = if total == 0 {
        0
    } else {
        total.saturating_add(limit_u64.saturating_sub(1)) / limit_u64
    };
    let first_height = entries.first().map(|entry| entry.summary.block_height);
    let last_height = entries.last().map(|entry| entry.summary.block_height);
    let page_number = first_height.map_or_else(
        || u64::from(page),
        |height| total.saturating_sub(u64::from(height)) / limit_u64 + 1,
    );
    json!({
        "page": page_number,
        "limit": limit,
        "totalPages": total_pages,
        "total": total,
        "hasNext": last_height.is_some_and(|height| height > 1),
        "hasPrev": first_height.is_some_and(|height| u64::from(height) < total),
        "nextCursor": last_height,
        "prevCursor": first_height,
    })
}

fn query_limit(limit: Option<u32>, default_limit: u32) -> u32 {
    query_limit_with_max(limit, default_limit, MAX_LIMIT)
}

fn query_limit_with_max(limit: Option<u32>, default_limit: u32, max_limit: u32) -> u32 {
    limit.unwrap_or(default_limit).clamp(1, max_limit)
}

fn optional_string(text: &str) -> Value {
    if text.is_empty() {
        Value::Null
    } else {
        json!(text)
    }
}

fn is_rpc_transaction_id(transaction_id: &str) -> bool {
    is_rpc_hash(transaction_id)
}

fn is_finalizer_pubkey(pubkey: &str) -> bool {
    is_rpc_hash(pubkey)
}

fn is_rpc_hash(hash: &str) -> bool {
    hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn percentage(numerator: u32, denominator: u32) -> Value {
    if denominator == 0 {
        return json!(0.0_f64);
    }
    json!((f64::from(numerator) / f64::from(denominator)) * 100.0)
}

fn json_response(status: StatusCode, body: Value) -> Response {
    let mut response = (status, Json(body)).into_response();
    insert_cors_headers(response.headers_mut());
    response
}

fn text_response(status: StatusCode, body: String) -> Response {
    let mut response = (status, body).into_response();
    insert_cors_headers(response.headers_mut());
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
    response
}

fn preflight_response() -> Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    insert_cors_headers(response.headers_mut());
    response
}

fn insert_cors_headers(headers: &mut axum::http::HeaderMap) {
    headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    headers.insert(
        ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET,POST,DELETE,OPTIONS"),
    );
    headers.insert(
        ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("content-type,authorization"),
    );
    headers.insert(
        ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static("content-type"),
    );
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::Request as HttpRequest,
    };
    use tower::ServiceExt;

    const SAMPLE_TRANSACTION_ID: &str =
        "73af76ccff3d661fb028d04325f2e93d88efe22525b01c6e8fab5e680a9cbbc8";
    const SAMPLE_BLOCK_HASH: &str =
        "00000000000000000000000000000000000000000000000000000000000000ab";

    fn value_pool_test_source_tip(height: u32) -> wallet::BlockTip {
        wallet::BlockTip {
            height,
            hash: SAMPLE_BLOCK_HASH.to_owned(),
        }
    }

    fn value_pool_history_test_response(
        visible_tip_height: u32,
        complete: bool,
        points: Vec<explorer::ValuePoolBalanceHistoryPoint>,
    ) -> ValuePoolBalanceHistoryResponse {
        ValuePoolBalanceHistoryResponse {
            freshness: Some(rich_list_freshness(visible_tip_height)),
            points,
            coverage: Some(explorer::ValuePoolBalanceHistoryCoverage {
                historical_from_height: Some(1),
                historical_through_height: complete.then_some(visible_tip_height),
                complete_through_visible_tip: complete,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn migration_history_entry(
        block_height: u32,
        block_time_unix_seconds: i64,
        is_coinbase: bool,
        orchard_zat: i64,
        ironwood_zat: i64,
    ) -> explorer::TransactionHistoryEntry {
        explorer::TransactionHistoryEntry {
            block_height,
            block_time_unix_seconds,
            is_coinbase,
            intrinsic_value_balances: Some(explorer::TransactionIntrinsicValueBalances {
                orchard_zat,
                ironwood_zat,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn assert_f64_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-12,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn conventional_fee_statistics_preserve_duplicate_frequencies()
    -> Result<(), CipherscanRestError> {
        let statistics = fee_distribution_statistics(&[(10_000, 2), (10_000, 3), (20_000, 1)])?
            .ok_or(CipherscanRestError::InvalidUpstreamField("test.statistics"))?;

        assert_eq!(statistics.transaction_count, 6);
        assert_eq!(statistics.median, 10_000);
        assert_eq!(statistics.average, 11_667);
        Ok(())
    }

    #[test]
    fn conventional_fee_percentiles_cover_singletons_and_even_interpolation()
    -> Result<(), CipherscanRestError> {
        let singleton = fee_distribution_statistics(&[(10_000, 1)])?
            .ok_or(CipherscanRestError::InvalidUpstreamField("test.singleton"))?;
        assert_eq!(singleton.p10, 10_000);
        assert_eq!(singleton.median, 10_000);
        assert_eq!(singleton.p90, 10_000);
        assert_eq!(singleton.average, 10_000);

        let even = fee_distribution_statistics(&[(10_000, 1), (20_000, 1)])?
            .ok_or(CipherscanRestError::InvalidUpstreamField("test.even"))?;
        assert_eq!(even.median, 15_000);
        assert_eq!(even.average, 15_000);
        Ok(())
    }

    #[test]
    fn conventional_fee_statistics_round_positive_halves_up() -> Result<(), CipherscanRestError> {
        let statistics = fee_distribution_statistics(&[(10_000, 1), (10_001, 1)])?
            .ok_or(CipherscanRestError::InvalidUpstreamField("test.statistics"))?;

        assert_eq!(statistics.median, 10_001);
        assert_eq!(statistics.average, 10_001);
        Ok(())
    }

    #[test]
    fn conventional_fee_period_coercion_echoes_unsupported_nonempty_values() {
        assert_eq!(
            fee_distribution_period(None),
            FeeDistributionPeriod {
                echoed: String::from("30d"),
                days: 30,
            }
        );
        assert_eq!(
            fee_distribution_period(Some("")),
            FeeDistributionPeriod {
                echoed: String::from("30d"),
                days: 30,
            }
        );
        assert_eq!(
            fee_distribution_period(Some("7D")),
            FeeDistributionPeriod {
                echoed: String::from("7D"),
                days: 30,
            }
        );
        assert_eq!(fee_distribution_period(Some("7d")).days, 7);
        assert_eq!(fee_distribution_period(Some("90d")).days, 90);
        assert_eq!(fee_distribution_period(Some("1y")).days, 365);
    }

    #[test]
    fn flow_analytics_period_coercion_preserves_the_raw_effective_cache_key() {
        assert_eq!(
            flow_analytics_period(None),
            FlowAnalyticsPeriod {
                echoed: String::from("30d"),
                days: Some(30),
            }
        );
        assert_eq!(
            flow_analytics_period(Some("")),
            FlowAnalyticsPeriod {
                echoed: String::from("30d"),
                days: Some(30),
            }
        );
        assert_eq!(
            flow_analytics_period(Some("7D")),
            FlowAnalyticsPeriod {
                echoed: String::from("7D"),
                days: Some(30),
            }
        );
        assert_eq!(flow_analytics_period(Some("7d")).days, Some(7));
        assert_eq!(flow_analytics_period(Some("90d")).days, Some(90));
        assert_eq!(flow_analytics_period(Some("1y")).days, Some(365));
        assert_eq!(flow_analytics_period(Some("all")).days, None);
    }

    #[test]
    fn flow_analytics_ranges_are_lower_inclusive_and_unbounded_above() {
        let now = 1_751_392_345;
        assert_eq!(
            flow_analytics_range(Some(7), now),
            (now - 7 * UNIX_SECONDS_PER_DAY, i64::MAX)
        );
        assert_eq!(flow_analytics_range(None, now), (i64::MIN, i64::MAX));
    }

    #[test]
    fn anonymity_set_json_maps_thresholds_in_legacy_ascending_order()
    -> Result<(), CipherscanRestError> {
        let thresholds = CIPHERSCAN_ANONYMITY_SET_THRESHOLDS_ZAT
            .iter()
            .map(|minimum_amount_zat| (*minimum_amount_zat, 10, 20))
            .collect::<Vec<_>>();
        let response = anonymity_set_json(
            "7d",
            &thresholds,
            OffsetDateTime::from_unix_timestamp(1_751_392_345)
                .unwrap_or(OffsetDateTime::UNIX_EPOCH),
        )?;

        assert_eq!(response["period"], json!("7d"));
        assert_eq!(response["thresholds"].as_array().map(Vec::len), Some(16));
        assert_eq!(response["thresholds"][0]["thresholdZat"], json!(1_000));
        assert_eq!(response["thresholds"][0]["thresholdZec"], json!(0.00001));
        assert_eq!(response["thresholds"][0]["shieldCount"], json!(10));
        assert_eq!(response["thresholds"][0]["deshieldCount"], json!(20));
        assert_eq!(
            response["thresholds"][15]["thresholdZat"],
            json!(1_000_000_000_000_u64)
        );
        Ok(())
    }

    #[test]
    fn flow_analytics_reject_incomplete_coverage() {
        assert!(require_complete_flow_analytics_coverage(false).is_err());
        assert!(require_complete_flow_analytics_coverage(true).is_ok());
    }

    #[test]
    fn anonymity_set_rejects_missing_or_misordered_native_thresholds() {
        let missing = CIPHERSCAN_ANONYMITY_SET_THRESHOLDS_ZAT
            .iter()
            .copied()
            .take(15)
            .map(|minimum_amount_zat| (minimum_amount_zat, 0, 0))
            .collect::<Vec<_>>();
        assert!(anonymity_set_json("30d", &missing, OffsetDateTime::UNIX_EPOCH).is_err());

        let mut misordered = CIPHERSCAN_ANONYMITY_SET_THRESHOLDS_ZAT
            .iter()
            .copied()
            .map(|minimum_amount_zat| (minimum_amount_zat, 0, 0))
            .collect::<Vec<_>>();
        misordered.swap(0, 1);
        assert!(anonymity_set_json("30d", &misordered, OffsetDateTime::UNIX_EPOCH).is_err());
    }

    #[test]
    fn shielding_distribution_subtracts_adjacent_cumulative_thresholds()
    -> Result<(), CipherscanRestError> {
        let thresholds = CIPHERSCAN_SHIELDING_DISTRIBUTION_BUCKETS
            .iter()
            .enumerate()
            .map(|(index, bucket)| {
                let remaining = u64::try_from(10 - index).unwrap_or(0);
                explorer::ValuePoolFlowAmountThresholdSummaryRow {
                    minimum_amount_zat: bucket.minimum_amount_zat,
                    shield_event_count: remaining * 10,
                    deshield_event_count: remaining * 20,
                    shield_amount_zat: remaining * 100,
                    deshield_amount_zat: remaining * 200,
                }
            })
            .collect::<Vec<_>>();
        let response = shielding_distribution_json(
            "30d",
            &thresholds,
            OffsetDateTime::from_unix_timestamp(1_751_392_345)
                .unwrap_or(OffsetDateTime::UNIX_EPOCH),
        )?;

        assert_eq!(response["period"], json!("30d"));
        assert_eq!(response["buckets"].as_array().map(Vec::len), Some(10));
        assert_eq!(response["buckets"][0]["label"], json!("<0.001"));
        assert_eq!(response["buckets"][0]["minZat"], json!(1));
        assert_eq!(response["buckets"][0]["maxZat"], json!(100_000));
        assert_eq!(response["buckets"][0]["shieldCount"], json!(10));
        assert_eq!(response["buckets"][0]["deshieldCount"], json!(20));
        assert_eq!(response["buckets"][0]["shieldVolumeZat"], json!(100));
        assert_eq!(response["buckets"][0]["deshieldVolumeZat"], json!(200));
        assert_eq!(response["buckets"][9]["label"], json!("1000+"));
        assert_eq!(response["buckets"][9]["maxZat"], Value::Null);
        assert_eq!(response["buckets"][9]["shieldCount"], json!(10));
        Ok(())
    }

    #[test]
    fn shielding_distribution_rejects_malformed_or_increasing_cumulative_rows() {
        let mut thresholds = CIPHERSCAN_SHIELDING_DISTRIBUTION_BUCKETS
            .iter()
            .map(|bucket| explorer::ValuePoolFlowAmountThresholdSummaryRow {
                minimum_amount_zat: bucket.minimum_amount_zat,
                shield_event_count: 10,
                deshield_event_count: 10,
                shield_amount_zat: 10,
                deshield_amount_zat: 10,
            })
            .collect::<Vec<_>>();
        thresholds.pop();
        assert!(
            shielding_distribution_json("30d", &thresholds, OffsetDateTime::UNIX_EPOCH).is_err()
        );

        thresholds.push(explorer::ValuePoolFlowAmountThresholdSummaryRow {
            minimum_amount_zat: 100_000_000_000,
            shield_event_count: 11,
            deshield_event_count: 10,
            shield_amount_zat: 10,
            deshield_amount_zat: 10,
        });
        assert!(
            shielding_distribution_json("30d", &thresholds, OffsetDateTime::UNIX_EPOCH).is_err()
        );
    }

    #[test]
    fn conventional_fee_range_and_daily_rows_preserve_boundary_day_shape()
    -> Result<(), CipherscanRestError> {
        let now = 1_751_392_345;
        assert_eq!(
            fee_distribution_range(7, now),
            (
                now - 7 * UNIX_SECONDS_PER_DAY,
                now + COMPONENT_SUMMARY_FUTURE_TIME_MARGIN_SECONDS + 1,
            )
        );
        let distribution = explorer::ConventionalFeeDistributionResponse {
            days: vec![
                conventional_fee_day(1_751_414_400, &[(20_000, 2)], 1),
                conventional_fee_day(1_751_328_000, &[(10_000, 3)], 0),
            ],
            coverage: Some(explorer::ConventionalFeeDistributionCoverage {
                complete_from_height: None,
                complete_through_height: None,
                complete_from_time_unix_seconds: None,
                complete_through_time_unix_seconds: None,
                requested_range_complete: false,
            }),
            ..Default::default()
        };
        let generated_at =
            OffsetDateTime::from_unix_timestamp(now).unwrap_or(OffsetDateTime::UNIX_EPOCH);
        let response = conventional_fee_distribution_json("7d", &distribution, generated_at)?;

        assert_eq!(response["daily"].as_array().map(Vec::len), Some(2));
        assert_eq!(
            response["daily"][0]["date"],
            json!("2025-07-01T00:00:00.000Z")
        );
        assert_eq!(
            response["daily"][1]["date"],
            json!("2025-07-02T00:00:00.000Z")
        );
        assert_eq!(response["daily"][1]["txCount"], json!(2));
        assert_eq!(response["degraded"], json!(true));
        assert_eq!(
            response["coverage"]["unavailableTransactionCount"],
            json!(1)
        );
        let unavailable =
            response["unavailable"]
                .as_array()
                .ok_or(CipherscanRestError::InvalidUpstreamField(
                    "test.unavailable",
                ))?;
        assert_eq!(unavailable.len(), 3);
        assert!(
            unavailable[0]
                .as_str()
                .is_some_and(|reason| reason.contains("actual paid fees"))
        );
        Ok(())
    }

    #[test]
    fn conventional_fee_statistics_match_weighted_reference_vector()
    -> Result<(), CipherscanRestError> {
        let statistics = fee_distribution_statistics(&[
            (10_000, 24),
            (15_000, 28),
            (25_000, 79),
            (30_000, 131),
            (40_000, 130),
            (65_000, 129),
        ])?
        .ok_or(CipherscanRestError::InvalidUpstreamField("test.statistics"))?;

        assert_eq!(statistics.p10, 25_000);
        assert_eq!(statistics.p25, 25_000);
        assert_eq!(statistics.median, 30_000);
        assert_eq!(statistics.p75, 40_000);
        assert_eq!(statistics.p90, 65_000);
        assert_eq!(statistics.average, 38_676);
        assert_eq!(statistics.transaction_count, 521);
        Ok(())
    }

    #[test]
    fn paid_fee_distribution_is_complete_only_with_full_coverage_and_no_missing_fees()
    -> Result<(), CipherscanRestError> {
        let generated_at = OffsetDateTime::from_unix_timestamp(1_751_392_345)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);
        let response = paid_fee_distribution_json(
            "7d",
            &explorer::PaidFeeDistributionResponse {
                days: vec![paid_fee_day(1_751_328_000, &[(10_000, 1), (20_000, 1)], 0)],
                coverage: Some(explorer::PaidFeeDistributionCoverage {
                    complete_from_height: Some(100),
                    complete_through_height: Some(200),
                    complete_from_time_unix_seconds: Some(1_750_000_000),
                    complete_through_time_unix_seconds: Some(1_751_500_000),
                    requested_range_complete: true,
                }),
                ..Default::default()
            },
            generated_at,
        )?;

        assert_eq!(response["feeBasis"], json!("actual_paid"));
        assert_eq!(response["degraded"], json!(false));
        assert_eq!(response["unavailable"], json!([]));
        assert_eq!(response["daily"][0]["median"], json!(15_000));
        assert_eq!(response["daily"][0]["avgFee"], json!(15_000));
        assert_eq!(response["daily"][0]["txCount"], json!(2));
        Ok(())
    }

    #[test]
    fn paid_fee_distribution_declares_partial_or_missing_fee_data()
    -> Result<(), CipherscanRestError> {
        let response = paid_fee_distribution_json(
            "30d",
            &explorer::PaidFeeDistributionResponse {
                days: vec![paid_fee_day(1_751_328_000, &[(20_000, 2)], 3)],
                coverage: Some(explorer::PaidFeeDistributionCoverage {
                    requested_range_complete: false,
                    ..Default::default()
                }),
                ..Default::default()
            },
            OffsetDateTime::UNIX_EPOCH,
        )?;

        assert_eq!(response["degraded"], json!(true));
        assert_eq!(
            response["coverage"]["unavailableTransactionCount"],
            json!(3)
        );
        assert_eq!(response["unavailable"].as_array().map(Vec::len), Some(2));
        Ok(())
    }

    fn conventional_fee_day(
        day_start_unix_seconds: i64,
        frequencies: &[(u64, u64)],
        unavailable_transaction_count: u64,
    ) -> explorer::ConventionalFeeDistributionDay {
        explorer::ConventionalFeeDistributionDay {
            day_start_unix_seconds,
            frequencies: frequencies
                .iter()
                .map(|(fee, count)| explorer::ConventionalFeeFrequency {
                    zip317_conventional_fee_zat: *fee,
                    transaction_count: *count,
                })
                .collect(),
            unavailable_transaction_count,
        }
    }

    fn paid_fee_day(
        day_start_unix_seconds: i64,
        frequencies: &[(u64, u64)],
        unavailable_transaction_count: u64,
    ) -> explorer::PaidFeeDistributionDay {
        explorer::PaidFeeDistributionDay {
            day_start_unix_seconds,
            frequencies: frequencies
                .iter()
                .map(|(fee, count)| explorer::PaidFeeFrequency {
                    paid_fee_zat: *fee,
                    transaction_count: *count,
                })
                .collect(),
            unavailable_transaction_count,
        }
    }

    #[tokio::test]
    async fn cors_preflight_handles_registered_get_routes() -> Result<(), Box<dyn std::error::Error>>
    {
        let router = with_cors_preflight(Router::new().route(
            "/api/network/mining-metrics",
            get(|| async { StatusCode::OK }),
        ));
        let response = router
            .oneshot(
                HttpRequest::builder()
                    .method(Method::OPTIONS)
                    .uri("/api/network/mining-metrics")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&HeaderValue::from_static("*"))
        );
        assert_eq!(
            response.headers().get(ACCESS_CONTROL_ALLOW_METHODS),
            Some(&HeaderValue::from_static("GET,POST,DELETE,OPTIONS"))
        );
        Ok(())
    }

    #[test]
    fn realtime_event_stream_start_resumes_after_the_last_native_cursor() {
        let live_tail = realtime_event_stream_start(None);
        assert!(matches!(
            live_tail.position,
            Some(event_stream_start::Position::LiveTail(_))
        ));

        let after_cursor = realtime_event_stream_start(Some(&[1, 2, 3]));
        assert!(matches!(
            after_cursor.position,
            Some(event_stream_start::Position::AfterCursor(cursor)) if cursor == vec![1, 2, 3]
        ));
    }

    #[test]
    fn realtime_commit_status_distinguishes_reader_lag_from_supersession() {
        assert_eq!(
            realtime_commit_status(11, 101, 10, 100),
            RealtimeCommitStatus::AwaitingReader,
        );
        assert_eq!(
            realtime_commit_status(11, 101, 11, 101),
            RealtimeCommitStatus::Hydratable,
        );
        assert_eq!(
            realtime_commit_status(11, 101, 12, 100),
            RealtimeCommitStatus::Superseded,
        );
        assert_eq!(
            realtime_commit_status(11, 101, 12, 101),
            RealtimeCommitStatus::Hydratable,
        );
    }

    #[test]
    fn realtime_payload_preserves_cipherscan_type_and_data_envelope()
    -> Result<(), Box<dyn std::error::Error>> {
        let (realtime_sender, mut realtime_events) = broadcast::channel(1);
        let block_data = json!({
            "height": "4157230",
            "hash": SAMPLE_BLOCK_HASH,
            "timestamp": "1783677045",
            "transaction_count": 2,
            "size": 13_194,
        });
        broadcast_realtime_payload(&realtime_sender, "new_block", &block_data);

        let dispatch = realtime_events.try_recv()?;
        let CipherscanRealtimeDispatch::Payload(payload) = dispatch else {
            return Err(std::io::Error::other("expected one realtime payload").into());
        };
        let message = serde_json::from_str::<Value>(&payload)?;
        assert_eq!(message["type"], json!("new_block"));
        assert_eq!(message["data"]["height"], json!("4157230"));
        assert_eq!(message["data"]["transaction_count"], json!(2));
        assert_eq!(message["data"]["size"], json!(13_194));
        Ok(())
    }

    #[tokio::test]
    async fn realtime_block_fanout_preserves_order_beyond_channel_capacity()
    -> Result<(), Box<dyn std::error::Error>> {
        let (realtime_sender, mut realtime_events) =
            broadcast::channel(REALTIME_EVENT_CHANNEL_CAPACITY);
        let summaries = (0..=REALTIME_EVENT_CHANNEL_CAPACITY)
            .map(|offset| explorer::BlockSummary {
                block_height: 100_u32.saturating_add(u32::try_from(offset).unwrap_or(u32::MAX)),
                block_hash: format!("block-{offset}"),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let expected_message_count = summaries.len();
        let receive_blocks = async move {
            let mut heights = Vec::with_capacity(expected_message_count);
            for _ in 0..expected_message_count {
                let CipherscanRealtimeDispatch::Payload(payload) = realtime_events.recv().await?
                else {
                    return Err(std::io::Error::other("expected one realtime block payload").into());
                };
                let message = serde_json::from_str::<Value>(&payload)?;
                if message["type"] != json!("new_block") {
                    return Err(std::io::Error::other("expected a new_block payload").into());
                }
                heights.push(message["data"]["height"].clone());
            }
            Ok::<Vec<Value>, Box<dyn std::error::Error>>(heights)
        };

        let ((), heights) = tokio::join!(
            broadcast_realtime_blocks(&realtime_sender, &summaries),
            receive_blocks,
        );
        let heights = heights?;
        assert_eq!(heights.len(), expected_message_count);
        assert_eq!(heights.first(), Some(&json!("100")));
        assert_eq!(heights.last(), Some(&json!("356")));
        Ok(())
    }

    #[test]
    fn mempool_added_json_uses_immutable_event_facts_and_output_total()
    -> Result<(), Box<dyn std::error::Error>> {
        let raw_transaction_bytes = transparent_transaction_bytes();
        let activations = NetworkUpgradeActivations::empty(Network::ZcashRegtest);
        let parsed_facts = zinder_source::parse_transaction_public_facts(
            &raw_transaction_bytes,
            None,
            &activations,
        )?;
        let transaction_id = encode_rpc_transaction_id_hex(parsed_facts.transaction_id);
        let entry = wallet::MempoolEntry {
            transaction_id: transaction_id.clone(),
            raw_transaction_bytes,
            first_seen_unix_millis: 1_783_677_045_999,
            transparent_outputs: vec![
                wallet::TransparentMempoolOutput {
                    value_zat: 25_000,
                    ..Default::default()
                },
                wallet::TransparentMempoolOutput {
                    value_zat: 75_000,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut facts = parse_realtime_mempool_facts(Network::ZcashRegtest, &entry)?;
        facts.size_bytes = 2_878;
        facts.counts = CoreTransactionComponentCounts {
            transparent_input_count: 6,
            transparent_output_count: 2,
            sapling_spend_count: 0,
            sapling_output_count: 2,
            orchard_action_count: 1,
            ironwood_action_count: 3,
            sprout_joinsplit_count: 0,
        };

        let transaction = mempool_added_json(&facts, &entry);

        assert_eq!(transaction["txid"], json!(transaction_id));
        assert_eq!(transaction["size"], json!(2_878));
        assert_eq!(transaction["time"], json!(1_783_677_045));
        assert_eq!(transaction["inputCount"], json!(6));
        assert_eq!(transaction["outputCount"], json!(2));
        assert_eq!(transaction["hasSapling"], json!(true));
        assert_eq!(transaction["hasOrchard"], json!(true));
        assert_eq!(transaction["hasIronwood"], json!(true));
        assert_eq!(transaction["orchardActions"], json!(1));
        assert_eq!(transaction["ironwoodActions"], json!(3));
        assert_eq!(transaction["totalOutput"], json!(0.001));
        assert!(transaction.get("fee").is_none());
        Ok(())
    }

    fn transparent_transaction_bytes() -> Vec<u8> {
        let mut bytes = vec![1, 0, 0, 0, 1];
        bytes.extend_from_slice(&[0xA5; 32]);
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        bytes.push(2);
        for (value_zat, script_pub_key) in [(1_000_u64, 0x51_u8), (2_500_u64, 0x52_u8)] {
            bytes.extend_from_slice(&value_zat.to_le_bytes());
            bytes.extend_from_slice(&[1, script_pub_key]);
        }
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes
    }

    fn sample_reorg_event() -> explorer::ChainReorgHistoryEvent {
        explorer::ChainReorgHistoryEvent {
            event_sequence: 42,
            cursor: vec![1, 2, 3],
            chain_epoch_id: 7,
            chain_epoch_created_at_millis: 1_700_000_000_000,
            visible_tip: Some(wallet::BlockTip {
                height: 101,
                hash: SAMPLE_BLOCK_HASH.to_owned(),
            }),
            settled_tip: Some(wallet::BlockTip {
                height: 95,
                hash: SAMPLE_BLOCK_HASH.to_owned(),
            }),
            reverted: Some(wallet::ChainRangeReverted {
                chain_epoch: None,
                start_height: 98,
                end_height: 100,
            }),
            committed: Some(wallet::ChainEpochCommitted {
                chain_epoch: None,
                start_height: 98,
                end_height: 101,
            }),
        }
    }

    #[test]
    fn mempool_transaction_json_preserves_parsed_counts() {
        let mut script_pub_key = vec![0x76, 0xa9, 0x14];
        script_pub_key.extend_from_slice(&[0x42; 20]);
        script_pub_key.extend_from_slice(&[0x88, 0xac]);
        let facts = explorer::TransactionPublicFacts {
            transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
            version: Some(explorer::TransactionVersion {
                kind: explorer::TransactionVersionKind::V5 as i32,
                effective_version: 5,
                version_group_id: Some(0x26a7_270a),
            }),
            size_bytes: 1234,
            counts: Some(explorer::TransactionComponentCounts {
                transparent_input_count: 1,
                transparent_output_count: 2,
                sapling_spend_count: 3,
                sapling_output_count: 4,
                orchard_action_count: 5,
                sprout_joinsplit_count: 0,
                ironwood_action_count: 6,
            }),
            privacy_shape: explorer::PrivacyShape::Mixed as i32,
            ..Default::default()
        };
        let mempool = wallet::MempoolEntry {
            raw_transaction_bytes: vec![0, 1],
            first_seen_unix_millis: 1_700_000_000_000,
            ..Default::default()
        };
        let response = explorer::TransactionDetailResponse {
            transparent_outputs: vec![
                explorer::TransparentOutput {
                    output_index: 0,
                    output: Some(wallet::TransparentOutput {
                        value_zat: 25_000_000,
                        script_pub_key,
                    }),
                    spent_by: None,
                },
                explorer::TransparentOutput {
                    output_index: 1,
                    output: Some(wallet::TransparentOutput {
                        value_zat: 75_000_000,
                        script_pub_key: vec![0x51],
                    }),
                    spent_by: None,
                },
            ],
            ..Default::default()
        };

        let transaction =
            mempool_transaction_json(Network::ZcashTestnet, &facts, &mempool, &response);

        assert_eq!(transaction["txid"], json!(SAMPLE_TRANSACTION_ID));
        assert_eq!(transaction["size"], json!(1234));
        assert_eq!(transaction["type"], json!("mixed"));
        assert_eq!(transaction["version"], json!(5));
        assert_eq!(transaction["firstSeen"], json!(1_700_000_000));
        assert_eq!(transaction["vinCount"], json!(1));
        assert_eq!(transaction["voutCount"], json!(2));
        assert_eq!(transaction["shieldedSpends"], json!(3));
        assert_eq!(transaction["shieldedOutputs"], json!(4));
        assert_eq!(transaction["orchardActions"], json!(5));
        assert_eq!(transaction["ironwoodActions"], json!(6));
        assert_eq!(transaction["totalOutput"], json!(1.0));
        assert_eq!(transaction["outputs"][0]["value"], json!(0.25));
        assert_eq!(transaction["outputs"][0]["n"], json!(0));
        assert!(transaction["outputs"][0]["address"].is_string());
        assert_eq!(transaction["outputs"][1]["value"], json!(0.75));
        assert_eq!(transaction["outputs"][1]["address"], Value::Null);
        assert!(
            transaction["zinderUnavailable"]
                .as_array()
                .is_some_and(|fields| fields.len() == 2)
        );
    }

    #[test]
    fn confirmed_transaction_location_rejects_mempool_transactions() {
        let unconfirmed_location = wallet::TransactionLocation {
            location: Some(transaction_location::Location::InMempool(
                wallet::MempoolEntry {
                    raw_transaction_bytes: vec![0, 1],
                    first_seen_unix_millis: 1_700_000_000_000,
                    ..Default::default()
                },
            )),
        };
        let confirmed_location = wallet::TransactionLocation {
            location: Some(transaction_location::Location::Mined(
                wallet::MinedTransaction::default(),
            )),
        };

        assert!(mined_location(Some(&unconfirmed_location)).is_none());
        assert!(mined_location(Some(&confirmed_location)).is_some());
    }

    #[test]
    fn transaction_detail_json_uses_the_block_coinbase_total() {
        let facts = explorer::TransactionPublicFacts {
            transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
            is_coinbase: true,
            counts: Some(explorer::TransactionComponentCounts {
                transparent_input_count: 1,
                ..Default::default()
            }),
            ..Default::default()
        };
        let response = explorer::TransactionDetailResponse::default();
        let coinbase_miner_fields = CipherscanCoinbaseMinerFields {
            miner_data_hex: "04f09f8c".to_owned(),
            miner_data_text: "....".to_owned(),
        };

        let transaction = transaction_detail_json(CipherscanTransactionDetailJsonInput {
            network: Network::ZcashTestnet,
            facts: &facts,
            location: None,
            response: &response,
            coinbase_total_output_zat: Some(137_500_000),
            coinbase_miner_fields: Some(&coinbase_miner_fields),
        });

        assert_eq!(transaction["totalOutput"], json!(1.375));
        assert_eq!(transaction["totalInput"], json!(0.0));
        assert_eq!(transaction["fee"], Value::Null);
        assert_eq!(transaction["feeSource"], json!("coinbase"));
        assert_eq!(transaction["inputCount"], json!(0));
        assert_eq!(transaction["outputCount"], json!(0));
        assert_eq!(transaction["coinbaseHex"], json!("04f09f8c"));
        assert_eq!(transaction["coinbaseText"], json!("...."));
        assert_eq!(transaction["bridge"], Value::Null);
        assert_eq!(transaction["stakingAction"], Value::Null);
        assert_eq!(transaction["inputs"], json!([]));
        assert_eq!(transaction["outputs"], json!([]));
        assert_eq!(transaction["zinderUnavailable"], json!([]));
    }

    #[test]
    fn transaction_detail_json_preserves_canonical_transparent_rows() {
        let script_pub_key = vec![
            0x76, 0xa9, 0x14, 0x3f, 0x1d, 0x70, 0x7e, 0xae, 0x92, 0x97, 0x98, 0x36, 0x95, 0xaa,
            0x5d, 0xbf, 0x98, 0x3e, 0x03, 0xb6, 0x38, 0x53, 0x0c, 0x88, 0xac,
        ];
        let facts = explorer::TransactionPublicFacts {
            transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
            counts: Some(explorer::TransactionComponentCounts {
                transparent_input_count: 1,
                transparent_output_count: 1,
                ..Default::default()
            }),
            ..Default::default()
        };
        let response = canonical_spent_transaction_detail_response(&script_pub_key);

        let transaction = transaction_detail_json(CipherscanTransactionDetailJsonInput {
            network: Network::ZcashTestnet,
            facts: &facts,
            location: None,
            response: &response,
            coinbase_total_output_zat: None,
            coinbase_miner_fields: None,
        });

        assert_eq!(
            transaction["inputs"][0]["prev_txid"],
            json!("ab".repeat(32))
        );
        assert_eq!(transaction["inputs"][0]["prev_vout"], json!(4));
        assert_eq!(transaction["inputs"][0]["value"], json!("200000000"));
        assert_eq!(
            transaction["inputs"][0]["address"],
            json!("tmFU5Ak942B7SciQpZCh3xH76QV3UmJgnDd")
        );
        assert_eq!(
            transaction["inputs"][0]["script_pubkey"],
            json!(hex::encode(&script_pub_key))
        );
        assert_eq!(transaction["outputs"][0]["vout_index"], json!(0));
        assert_eq!(transaction["outputs"][0]["value"], json!("125000000"));
        assert_eq!(transaction["outputs"][0]["spent"], json!(true));
        assert_eq!(
            transaction["outputs"][0]["address"],
            json!("tmFU5Ak942B7SciQpZCh3xH76QV3UmJgnDd")
        );
        assert_eq!(
            transaction["outputs"][0]["script_pubkey"],
            json!(hex::encode(script_pub_key))
        );
        assert_eq!(
            transaction["zinderUnavailable"].as_array().map(Vec::len),
            Some(1)
        );
        assert!(
            transaction["zinderUnavailable"]
                .as_array()
                .is_some_and(|reasons| reasons.iter().all(|reason| {
                    !reason
                        .as_str()
                        .unwrap_or_default()
                        .contains("transparent input")
                }))
        );
    }

    fn canonical_spent_transaction_detail_response(
        script_pub_key: &[u8],
    ) -> explorer::TransactionDetailResponse {
        explorer::TransactionDetailResponse {
            transparent_inputs: vec![explorer::TransparentInput {
                input_index: 0,
                spent_outpoint: Some(wallet::OutPoint {
                    transaction_id: "ab".repeat(32),
                    output_index: 4,
                }),
                value_zat: Some(200_000_000),
                script_pub_key: Some(script_pub_key.to_vec()),
            }],
            transparent_outputs: vec![explorer::TransparentOutput {
                output_index: 0,
                output: Some(wallet::TransparentOutput {
                    value_zat: 125_000_000,
                    script_pub_key: script_pub_key.to_vec(),
                }),
                spent_by: Some(wallet::TransparentSpend {
                    spent_outpoint: Some(wallet::OutPoint {
                        transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
                        output_index: 0,
                    }),
                    spending_transaction_id: "cd".repeat(32),
                    input_index: 1,
                    spending_block: Some(wallet::BlockTip {
                        height: 43,
                        hash: "ef".repeat(32),
                    }),
                }),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn transaction_detail_validation_rejects_missing_intrinsic_output() {
        let script_pub_key = [0x51];
        let mut response = canonical_spent_transaction_detail_response(&script_pub_key);
        response.transparent_outputs[0].output = None;
        let facts = explorer::TransactionPublicFacts {
            transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
            counts: Some(explorer::TransactionComponentCounts {
                transparent_output_count: 1,
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(matches!(
            validate_transaction_detail_outputs(&facts, &response),
            Err(CipherscanRestError::MissingUpstreamField(
                "transparent_outputs.output"
            ))
        ));
    }

    #[test]
    fn transaction_detail_validation_rejects_malformed_spender() {
        let script_pub_key = [0x51];
        let mut response = canonical_spent_transaction_detail_response(&script_pub_key);
        if let Some(spend) = response.transparent_outputs[0].spent_by.as_mut() {
            spend.spending_transaction_id = "not-a-transaction-id".to_owned();
        }
        let facts = explorer::TransactionPublicFacts {
            transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
            counts: Some(explorer::TransactionComponentCounts {
                transparent_output_count: 1,
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(matches!(
            validate_transaction_detail_outputs(&facts, &response),
            Err(CipherscanRestError::InvalidUpstreamField(
                "transparent_outputs.spent_by"
            ))
        ));
    }

    #[test]
    fn transaction_detail_json_marks_absent_spender_as_unspent() {
        let script_pub_key = [0x51];
        let mut response = canonical_spent_transaction_detail_response(&script_pub_key);
        response.transparent_outputs[0].spent_by = None;
        let facts = explorer::TransactionPublicFacts {
            transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
            counts: Some(explorer::TransactionComponentCounts {
                transparent_output_count: 1,
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(validate_transaction_detail_outputs(&facts, &response).is_ok());
        let transaction = transaction_detail_json(CipherscanTransactionDetailJsonInput {
            network: Network::ZcashTestnet,
            facts: &facts,
            location: None,
            response: &response,
            coinbase_total_output_zat: None,
            coinbase_miner_fields: None,
        });
        assert_eq!(transaction["outputs"][0]["spent"], json!(false));
    }

    #[test]
    fn transaction_detail_json_marks_an_unresolved_input_prevout_without_losing_its_outpoint() {
        let facts = explorer::TransactionPublicFacts {
            transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
            counts: Some(explorer::TransactionComponentCounts {
                transparent_input_count: 1,
                ..Default::default()
            }),
            ..Default::default()
        };
        let response = explorer::TransactionDetailResponse {
            transparent_inputs: vec![explorer::TransparentInput {
                input_index: 0,
                spent_outpoint: Some(wallet::OutPoint {
                    transaction_id: "ab".repeat(32),
                    output_index: 4,
                }),
                value_zat: None,
                script_pub_key: None,
            }],
            ..Default::default()
        };

        let transaction = transaction_detail_json(CipherscanTransactionDetailJsonInput {
            network: Network::ZcashTestnet,
            facts: &facts,
            location: None,
            response: &response,
            coinbase_total_output_zat: None,
            coinbase_miner_fields: None,
        });

        assert_eq!(
            transaction["inputs"][0]["prev_txid"],
            json!("ab".repeat(32))
        );
        assert_eq!(transaction["inputs"][0]["prev_vout"], json!(4));
        assert_eq!(transaction["inputs"][0]["value"], Value::Null);
        assert_eq!(transaction["inputs"][0]["address"], Value::Null);
        assert!(
            transaction["zinderUnavailable"]
                .as_array()
                .is_some_and(|reasons| reasons.iter().any(|reason| {
                    reason
                        .as_str()
                        .is_some_and(|reason| reason.contains("partially unavailable"))
                }))
        );
    }

    #[test]
    fn transaction_detail_json_uses_conventional_fee_for_shielding() {
        let facts = explorer::TransactionPublicFacts {
            transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
            counts: Some(explorer::TransactionComponentCounts {
                transparent_input_count: 5,
                sapling_output_count: 2,
                ..Default::default()
            }),
            privacy_shape: explorer::PrivacyShape::Shielding as i32,
            ..Default::default()
        };
        let response = explorer::TransactionDetailResponse {
            paid_fee_zat: Some(625_010_000),
            ..Default::default()
        };

        let transaction = transaction_detail_json(CipherscanTransactionDetailJsonInput {
            network: Network::ZcashTestnet,
            facts: &facts,
            location: None,
            response: &response,
            coinbase_total_output_zat: None,
            coinbase_miner_fields: None,
        });

        assert_eq!(transaction["fee"], json!(0.00035));
        assert_eq!(transaction["feeSource"], json!("zip317-conventional"));
        assert_eq!(transaction["paid_fee_zat"], Value::Null);
        assert!(
            transaction["zinderUnavailable"]
                .as_array()
                .is_some_and(|fields| fields.len() == 2)
        );
    }

    #[test]
    fn transaction_detail_json_exposes_signed_pool_balances_for_page_classification() {
        let facts = explorer::TransactionPublicFacts {
            transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
            counts: Some(explorer::TransactionComponentCounts {
                transparent_input_count: 1,
                transparent_output_count: 1,
                sapling_output_count: 1,
                orchard_action_count: 1,
                ironwood_action_count: 1,
                ..Default::default()
            }),
            ..Default::default()
        };
        let response = explorer::TransactionDetailResponse {
            intrinsic_value_balances: Some(explorer::TransactionIntrinsicValueBalances {
                sprout_zat: 0,
                sapling_zat: -125_000_000,
                orchard_zat: 25_000_000,
                ironwood_zat: 50_000_000,
            }),
            ..Default::default()
        };

        let transaction = transaction_detail_json(CipherscanTransactionDetailJsonInput {
            network: Network::ZcashTestnet,
            facts: &facts,
            location: None,
            response: &response,
            coinbase_total_output_zat: None,
            coinbase_miner_fields: None,
        });

        assert_eq!(transaction["valueBalanceSapling"], json!(-1.25));
        assert_eq!(transaction["valueBalanceOrchard"], json!(0.25));
        assert_eq!(transaction["valueBalanceIronwood"], json!(0.5));
        assert!(transaction.get("type").is_none());
    }

    #[test]
    fn compat_transaction_type_uses_cipherscan_labels() {
        let transparent = explorer::TransactionComponentCounts {
            transparent_output_count: 1,
            ..Default::default()
        };
        let shielded = explorer::TransactionComponentCounts {
            orchard_action_count: 1,
            ..Default::default()
        };
        let mixed = explorer::TransactionComponentCounts {
            transparent_input_count: 1,
            sapling_output_count: 1,
            ..Default::default()
        };

        assert_eq!(compat_transaction_type(Some(&transparent)), "transparent");
        assert_eq!(compat_transaction_type(Some(&shielded)), "shielded");
        assert_eq!(compat_transaction_type(Some(&mixed)), "mixed");
        assert_eq!(compat_transaction_type(None), "transparent");
    }

    #[test]
    fn transaction_list_filters_map_to_product_neutral_history_fields() {
        let shielded = transaction_list_history_filter(Some("shielded"));
        assert_eq!(shielded.is_coinbase, None);
        assert_eq!(
            shielded.contains_any_protocol,
            vec![
                explorer::ShieldedProtocol::Sapling as i32,
                explorer::ShieldedProtocol::Orchard as i32,
                explorer::ShieldedProtocol::Ironwood as i32,
            ]
        );

        let transparent = transaction_list_history_filter(Some("transparent"));
        assert_eq!(transparent.is_coinbase, Some(false));
        assert_eq!(
            transparent.privacy_shapes,
            vec![explorer::PrivacyShape::TransparentOnly as i32]
        );

        let coinbase = transaction_list_history_filter(Some("coinbase"));
        assert_eq!(coinbase.is_coinbase, Some(true));
    }

    #[test]
    fn transaction_history_count_cache_is_bound_to_the_projection_fence() {
        let filter = TransactionHistoryFilter::default();
        let first = transaction_history_read_fence(7);
        let second = transaction_history_read_fence(8);

        assert_ne!(
            TransactionHistoryCountCacheKey::new(&filter, &first),
            TransactionHistoryCountCacheKey::new(&filter, &second)
        );
    }

    #[test]
    fn transaction_history_count_requires_full_history_scope() {
        let mut response = TransactionHistoryResponse {
            total_matching_transactions: Some(42),
            ..Default::default()
        };
        assert!(require_full_transaction_history_count(&response).is_err());

        response.count_scope = TransactionHistoryCountScope::FullHistory as i32;
        assert!(matches!(
            require_full_transaction_history_count(&response),
            Ok(42)
        ));
    }

    #[test]
    fn transaction_history_page_fence_rejects_revision_changes() {
        let mut read_fence = None;
        let first = TransactionHistoryResponse {
            read_fence: Some(transaction_history_read_fence(7)),
            ..Default::default()
        };
        assert!(advance_transaction_history_read_fence(&mut read_fence, &first).is_ok());

        let changed = TransactionHistoryResponse {
            read_fence: Some(transaction_history_read_fence(8)),
            ..Default::default()
        };
        assert!(advance_transaction_history_read_fence(&mut read_fence, &changed).is_err());
    }

    fn transaction_history_read_fence(revision: u64) -> TransactionHistoryReadFence {
        TransactionHistoryReadFence {
            chain_epoch_id: 11,
            materialized_view_revision: revision,
            materialized_view_tip_height: 42,
            materialized_view_tip_hash: "00".repeat(32),
        }
    }

    #[test]
    fn shielded_transaction_filters_preserve_pool_type_and_action_threshold()
    -> Result<(), CipherscanRestError> {
        let query = PageQuery {
            pool: Some("orchard".to_owned()),
            transaction_type: Some("partial".to_owned()),
            min_actions: Some(3),
            ..PageQuery::default()
        };

        let filter = shielded_transaction_history_filter(&query)?;
        assert_eq!(
            filter.contains_any_protocol,
            vec![explorer::ShieldedProtocol::Orchard as i32]
        );
        assert_eq!(
            filter.privacy_shapes,
            vec![
                explorer::PrivacyShape::Shielding as i32,
                explorer::PrivacyShape::Deshielding as i32,
                explorer::PrivacyShape::Mixed as i32,
                explorer::PrivacyShape::ShieldedCoinbase as i32,
            ]
        );
        assert_eq!(filter.minimum_shielded_component_count, 3);
        Ok(())
    }

    #[test]
    fn shielded_transaction_query_preserves_cipherscan_validation_and_count_controls()
    -> Result<(), Box<dyn std::error::Error>> {
        let query = parse_shielded_transaction_query(ShieldedTransactionQuery {
            limit: Some("0x19".to_owned()),
            offset: Some("50".to_owned()),
            pool: Some("ironwood".to_owned()),
            min_actions: Some("3".to_owned()),
            skip_count: Some("false".to_owned()),
            transaction_type: Some("partial".to_owned()),
        });
        assert!(query.is_ok());
        let query = query.unwrap_or_default();
        assert_eq!(query.limit, Some(25));
        assert_eq!(query.offset, Some(50));
        assert_eq!(query.skip_count.as_deref(), Some("false"));

        let empty_limit = parse_shielded_transaction_query(ShieldedTransactionQuery {
            limit: Some(String::new()),
            ..ShieldedTransactionQuery::default()
        });
        let details = empty_limit
            .err()
            .ok_or("an empty limit must coerce to zero and fail its lower bound")?;
        assert_eq!(details[0]["field"], json!("limit"));
        assert_eq!(
            details[0]["message"],
            json!("Too small: expected number to be >=1")
        );

        let invalid = parse_shielded_transaction_query(ShieldedTransactionQuery {
            limit: Some("101".to_owned()),
            pool: Some("all".to_owned()),
            skip_count: Some("yes".to_owned()),
            ..ShieldedTransactionQuery::default()
        });
        let details = invalid
            .err()
            .ok_or("the invalid compatibility query must be rejected")?;
        assert_eq!(details.len(), 3);
        assert_eq!(details[0]["field"], json!("limit"));
        assert_eq!(details[1]["field"], json!("pool"));
        assert_eq!(details[2]["field"], json!("skip_count"));
        Ok(())
    }

    #[test]
    fn shielded_transaction_row_keeps_paid_and_conventional_fees_distinct() {
        let entry = explorer::TransactionHistoryEntry {
            paid_fee_zat: None,
            zip317_conventional_fee_zat: Some(15_000),
            intrinsic_value_balances: Some(explorer::TransactionIntrinsicValueBalances {
                sapling_zat: -125_000_000,
                orchard_zat: 250_000_000,
                ironwood_zat: 0,
                ..Default::default()
            }),
            ..Default::default()
        };

        let row = shielded_transaction_row(&entry);

        assert_eq!(row["fee"], Value::Null);
        assert_eq!(row["feeSource"], Value::Null);
        assert_eq!(row["zip317ConventionalFee"], json!(0.00015));
        assert_eq!(row["valueBalanceSapling"], json!(-1.25));
        assert_eq!(row["valueBalanceOrchard"], json!(2.5));
        assert_eq!(row["valueBalanceIronwood"], json!(0.0));
    }

    #[test]
    fn transaction_history_rows_expose_the_real_in_block_cursor() {
        let entry = explorer::TransactionHistoryEntry {
            transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
            block_height: 42,
            block_hash: SAMPLE_BLOCK_HASH.to_owned(),
            transaction_index: 7,
            ..Default::default()
        };

        let row = recent_transaction_row(&entry);
        assert_eq!(row["block_height"], json!(42));
        assert_eq!(row["tx_index"], json!(7));
    }

    #[test]
    fn mempool_row_uses_cipherscan_labels_and_native_component_counts() {
        let transparent = explorer::MempoolActivityEntry {
            privacy_shape: explorer::PrivacyShape::TransparentOnly as i32,
            ..Default::default()
        };
        let shielded = explorer::MempoolActivityEntry {
            privacy_shape: explorer::PrivacyShape::ShieldedOnly as i32,
            ..Default::default()
        };
        let mixed = explorer::MempoolActivityEntry {
            privacy_shape: explorer::PrivacyShape::Shielding as i32,
            component_counts: Some(explorer::TransactionComponentCounts {
                transparent_input_count: 2,
                transparent_output_count: 3,
                sapling_spend_count: 5,
                sapling_output_count: 7,
                orchard_action_count: 11,
                ironwood_action_count: 13,
                ..Default::default()
            }),
            transparent_output_total_zat: 123_456_789,
            ..Default::default()
        };

        assert_eq!(mempool_row(&transparent)["type"], json!("transparent"));
        assert_eq!(mempool_row(&shielded)["type"], json!("shielded"));
        assert_eq!(mempool_row(&mixed)["type"], json!("mixed"));
        assert_eq!(mempool_row(&mixed)["vin"], json!(2));
        assert_eq!(mempool_row(&mixed)["vout"], json!(3));
        assert_eq!(mempool_row(&mixed)["vShieldedSpend"], json!(5));
        assert_eq!(mempool_row(&mixed)["vShieldedOutput"], json!(7));
        assert_eq!(mempool_row(&mixed)["orchardActions"], json!(11));
        assert_eq!(mempool_row(&mixed)["ironwoodActions"], json!(13));
        assert_eq!(mempool_row(&mixed)["hasSapling"], json!(true));
        assert_eq!(mempool_row(&mixed)["hasOrchard"], json!(true));
        assert_eq!(mempool_row(&mixed)["hasIronwood"], json!(true));
        assert_eq!(mempool_row(&mixed)["totalOutput"], json!(1.234_567_89));
        assert!(
            mempool_row(&mixed)["zinderUnavailable"]
                .as_array()
                .is_some_and(|fields| fields.len() == 1)
        );
        assert!(
            mempool_row(&transparent)["zinderUnavailable"]
                .as_array()
                .is_some_and(|fields| fields.len() == 2)
        );
    }

    #[test]
    fn mempool_stats_use_the_full_summary_distribution() {
        let summary = explorer::MempoolSnapshotSummary {
            transaction_count: 10,
            total_size_bytes: 42_000,
            privacy_shape_distribution: vec![
                explorer::PrivacyShapeCount {
                    shape: explorer::PrivacyShape::TransparentOnly as i32,
                    count: 3,
                },
                explorer::PrivacyShapeCount {
                    shape: explorer::PrivacyShape::ShieldedOnly as i32,
                    count: 2,
                },
                explorer::PrivacyShapeCount {
                    shape: explorer::PrivacyShape::Shielding as i32,
                    count: 1,
                },
                explorer::PrivacyShapeCount {
                    shape: explorer::PrivacyShape::Deshielding as i32,
                    count: 1,
                },
                explorer::PrivacyShapeCount {
                    shape: explorer::PrivacyShape::Mixed as i32,
                    count: 2,
                },
                explorer::PrivacyShapeCount {
                    shape: explorer::PrivacyShape::Unclassified as i32,
                    count: 1,
                },
            ],
            oldest_entry_age_millis: 200,
            newest_entry_age_millis: 10,
            ..Default::default()
        };

        let stats = mempool_stats_json(&summary);

        assert_eq!(stats["total"], json!(10));
        assert_eq!(stats["shielded"], json!(6));
        assert_eq!(stats["transparent"], json!(4));
        assert_eq!(stats["shieldedPercentage"], json!(60.0));
        assert_eq!(stats["totalSizeBytes"], json!(42_000));
    }

    #[test]
    fn transaction_routes_keep_paid_and_conventional_fee_semantics_explicit() {
        let entry = explorer::TransactionHistoryEntry {
            transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
            privacy_shape: explorer::PrivacyShape::Shielding as i32,
            zip317_conventional_fee_zat: Some(35_000),
            paid_fee_zat: Some(625_010_000),
            ..Default::default()
        };

        let transaction = recent_transaction_row(&entry);
        let shielded = shielded_transaction_row(&entry);

        assert_eq!(transaction["fee"], json!("35000"));
        assert_eq!(transaction["feeSource"], json!("zip317-conventional"));
        assert!(
            transaction["zinderUnavailable"]
                .as_array()
                .is_some_and(|fields| !fields.is_empty())
        );
        assert_eq!(shielded["fee"], json!(6.2501));
        assert_eq!(shielded["feeSource"], json!("paid"));
        assert_eq!(shielded["zip317ConventionalFee"], json!(0.00035));
    }

    #[test]
    fn block_transaction_row_preserves_outpoints_outputs_and_standard_addresses() {
        let public_key_hash = [0x42; 20];
        let mut script_pub_key = vec![0x76, 0xa9, 0x14];
        script_pub_key.extend_from_slice(&public_key_hash);
        script_pub_key.extend_from_slice(&[0x88, 0xac]);
        let transaction = explorer::BlockTransaction {
            transaction_index: 1,
            transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
            public_facts: Some(explorer::TransactionPublicFacts {
                transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
                size_bytes: 1_536,
                counts: Some(explorer::TransactionComponentCounts {
                    transparent_input_count: 1,
                    transparent_output_count: 1,
                    orchard_action_count: 1,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            transparent_outputs: vec![wallet::TransparentOutput {
                value_zat: 137_500_000,
                script_pub_key: script_pub_key.clone(),
            }],
            transparent_inputs: vec![explorer::TransparentInput {
                input_index: 0,
                spent_outpoint: Some(wallet::OutPoint {
                    transaction_id: "ab".repeat(32),
                    output_index: 3,
                }),
                value_zat: Some(150_000_000),
                script_pub_key: Some(script_pub_key),
            }],
        };

        let row =
            cipherscan_block_transaction_json(Network::ZcashTestnet, &transaction, 42, 100, false)
                .unwrap_or_default();

        assert_eq!(row["txid"], json!(SAMPLE_TRANSACTION_ID));
        assert_eq!(row["tx_index"], json!(1));
        assert_eq!(row["block_height"], json!(42));
        assert_eq!(row["size"], json!(1_536));
        assert_eq!(row["inputs"][0]["prev_txid"], json!("ab".repeat(32)));
        assert_eq!(row["inputs"][0]["prev_vout"], json!(3));
        assert_eq!(row["inputs"][0]["value"], Value::Null);
        assert!(
            row["inputs"][0]["address"]
                .as_str()
                .is_some_and(|address| address.starts_with("tm"))
        );
        assert_eq!(row["outputs"][0]["vout_index"], json!(0));
        assert_eq!(row["outputs"][0]["value"], json!("137500000"));
        assert!(
            row["outputs"][0]["address"]
                .as_str()
                .is_some_and(|address| address.starts_with("tm"))
        );
        assert_eq!(row["has_orchard"], json!(true));
        assert!(
            row["zinderUnavailable"]
                .as_array()
                .is_some_and(|reasons| reasons.len() >= 2)
        );
    }

    #[test]
    fn block_transaction_rows_expose_values_only_for_fully_transparent_blocks() {
        let mut script_pub_key = vec![0x76, 0xa9, 0x14];
        script_pub_key.extend_from_slice(&[0x42; 20]);
        script_pub_key.extend_from_slice(&[0x88, 0xac]);
        let transparent = explorer::BlockTransaction {
            transaction_index: 1,
            transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
            public_facts: Some(explorer::TransactionPublicFacts {
                transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
                privacy_shape: explorer::PrivacyShape::TransparentOnly as i32,
                counts: Some(explorer::TransactionComponentCounts {
                    transparent_input_count: 1,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            transparent_inputs: vec![explorer::TransparentInput {
                input_index: 0,
                spent_outpoint: Some(wallet::OutPoint {
                    transaction_id: "ab".repeat(32),
                    output_index: 3,
                }),
                value_zat: Some(150_000_000),
                script_pub_key: Some(script_pub_key),
            }],
            ..Default::default()
        };
        let transparent_rows = cipherscan_block_transaction_rows(
            Network::ZcashTestnet,
            std::slice::from_ref(&transparent),
            42,
            100,
        );
        assert_eq!(
            transparent_rows.rows[0]["inputs"][0]["value"],
            json!(150_000_000)
        );

        let shielded = explorer::BlockTransaction {
            transaction_index: 2,
            transaction_id: "cd".repeat(32),
            public_facts: Some(explorer::TransactionPublicFacts {
                transaction_id: "cd".repeat(32),
                privacy_shape: explorer::PrivacyShape::ShieldedOnly as i32,
                counts: Some(explorer::TransactionComponentCounts {
                    sapling_output_count: 1,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mixed_rows = cipherscan_block_transaction_rows(
            Network::ZcashTestnet,
            &[transparent, shielded],
            42,
            100,
        );
        assert_eq!(mixed_rows.rows[0]["inputs"][0]["value"], Value::Null);
        assert!(mixed_rows.unavailable.iter().any(|reason| {
            reason.contains("withheld") && reason.contains("false partial block fee")
        }));
    }

    #[test]
    fn block_detail_exposes_the_validated_miner_address() {
        let summary = explorer::BlockSummary {
            block_height: 42,
            block_hash: SAMPLE_BLOCK_HASH.to_owned(),
            ..Default::default()
        };
        let header = wallet::BlockHeader::default();
        let header_fields = CipherscanBlockHeaderFields {
            difficulty: 1.0,
            bits: "1f07ffff".to_owned(),
            nonce: "00".repeat(32),
        };
        let transaction_rows = CipherscanBlockTransactionRows {
            rows: Vec::new(),
            unavailable: Vec::new(),
        };

        let response = cipherscan_block_detail_response_json(CipherscanBlockDetailResponseInput {
            summary: &summary,
            header: &header,
            header_fields: &header_fields,
            transaction_rows: &transaction_rows,
            miner_address: Some("tmUcufCrN94ZXNuffjzWPdB3PSAYpc2KmSw"),
            final_note_commitment_roots: None,
            coinbase_miner_fields: None,
        });

        assert_eq!(
            response["miner_address"],
            json!("tmUcufCrN94ZXNuffjzWPdB3PSAYpc2KmSw")
        );
        assert_eq!(response["height"], json!("42"));
        assert_eq!(response["timestamp"], json!("0"));
        assert_eq!(response["difficulty"], json!("1"));
        assert_eq!(response["total_fees"], json!("0"));
        assert_eq!(response["miner_pool"], Value::Null);
        assert_eq!(response["miner_pool_url"], Value::Null);
        assert_eq!(response["miner_pool_region"], Value::Null);
        assert_eq!(response["finality_status"], Value::Null);
    }

    #[test]
    fn block_detail_exposes_validated_final_note_commitment_roots()
    -> std::result::Result<(), CipherscanRestError> {
        let summary = explorer::BlockSummary {
            block_height: 42,
            block_hash: SAMPLE_BLOCK_HASH.to_owned(),
            ..Default::default()
        };
        let header = wallet::BlockHeader::default();
        let header_fields = CipherscanBlockHeaderFields {
            difficulty: 1.0,
            bits: "1f07ffff".to_owned(),
            nonce: "00".repeat(32),
        };
        let transaction_rows = CipherscanBlockTransactionRows {
            rows: Vec::new(),
            unavailable: Vec::new(),
        };
        let roots = CipherscanFinalNoteCommitmentRoots::try_from(
            &explorer::BlockFinalNoteCommitmentRoots {
                sapling: Some(vec![0x11; 32]),
                orchard: Some(vec![0x22; 32]),
                ironwood: Some(vec![0x33; 32]),
            },
        )?;

        let response = cipherscan_block_detail_response_json(CipherscanBlockDetailResponseInput {
            summary: &summary,
            header: &header,
            header_fields: &header_fields,
            transaction_rows: &transaction_rows,
            miner_address: None,
            final_note_commitment_roots: Some(&roots),
            coinbase_miner_fields: None,
        });

        assert_eq!(response["final_sapling_root"], json!("11".repeat(32)));
        assert_eq!(response["final_orchard_root"], json!("22".repeat(32)));
        assert_eq!(response["final_ironwood_root"], json!("33".repeat(32)));
        Ok(())
    }

    #[test]
    fn block_detail_decodes_coinbase_miner_fields() -> Result<(), CipherscanRestError> {
        let raw_transaction = hex::decode(
            "0600008098b684d85b16a5370000000050743f00010000000000000000000000000000000000000000000000000000000000000000ffffffff160350743f04f09f8cb87a6b636c61756465636f646572ffffffff0240597307000000001976a9143f1d707eae9297983695aa5dbf983e03b638530c88ac20bcbe000000000017a9147a86d6c7eb12ce0aa309d7391a6f338eba3c242b8700000000",
        )?;

        let coinbase_miner_fields = decode_cipherscan_coinbase_miner_fields(&raw_transaction)?;

        assert_eq!(
            coinbase_miner_fields,
            CipherscanCoinbaseMinerFields {
                miner_data_hex: "04f09f8cb87a6b636c61756465636f646572".to_owned(),
                miner_data_text: ".....zkclaudecoder".to_owned(),
            }
        );
        Ok(())
    }

    #[test]
    fn block_detail_rejects_malformed_final_note_commitment_root() {
        let error = CipherscanFinalNoteCommitmentRoots::try_from(
            &explorer::BlockFinalNoteCommitmentRoots {
                sapling: Some(vec![0x11; 31]),
                orchard: None,
                ironwood: None,
            },
        )
        .err();

        assert!(matches!(
            error,
            Some(CipherscanRestError::InvalidUpstreamField(
                "final_note_commitment_roots.sapling"
            ))
        ));
    }

    #[test]
    fn transparent_address_decoder_rejects_nonstandard_scripts() {
        assert!(cipherscan_transparent_address(Network::ZcashTestnet, &[0x51]).is_none());
    }

    #[test]
    fn is_rpc_transaction_id_accepts_64_hex_characters() {
        assert!(is_rpc_transaction_id(SAMPLE_TRANSACTION_ID));
        assert!(is_finalizer_pubkey(SAMPLE_TRANSACTION_ID));
        assert!(is_rpc_transaction_id(
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        ));
        assert!(is_finalizer_pubkey(
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        ));
        assert!(!is_rpc_transaction_id("not-a-txid"));
        assert!(!is_finalizer_pubkey("not-a-pubkey"));
        assert!(!is_rpc_transaction_id(
            "gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg"
        ));
        assert!(!is_finalizer_pubkey(
            "gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg"
        ));
    }

    #[test]
    fn commitment_root_search_json_marks_empty_partial_coverage_as_indeterminate() {
        let root = "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789";

        let result = commitment_root_search_json(CommitmentRootSearchJsonInput {
            root,
            canonical: &[],
            orphaned: &[],
            canonical_coverage: &explorer::CommitmentRootSearchCoverage {
                complete_from_height: Some(280_000),
                complete_through_height: Some(1_000_000),
                latest_indexed_height: Some(4_158_484),
                canonical_history_complete: false,
            },
            displaced_root_capability: false,
            displaced_root_coverage: None,
        });

        assert_eq!(
            result["root"],
            json!("abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")
        );
        assert_eq!(result["found"], json!(false));
        assert_eq!(result["canonical"], json!([]));
        assert_eq!(result["orphaned"], json!([]));
        assert_eq!(
            result["diagnosis"],
            json!(
                "This anchor root has no canonical match in the currently indexed coverage; canonical history is incomplete, and displaced history is activation-limited."
            )
        );
        assert_eq!(result["degraded"], json!(true));
        assert_eq!(result["coverage"]["completeFromHeight"], json!(280_000));
        assert_eq!(
            result["coverage"]["completeThroughHeight"],
            json!(1_000_000)
        );
        assert_eq!(result["coverage"]["latestIndexedHeight"], json!(4_158_484));
        assert!(
            result["unavailable"]
                .as_array()
                .is_some_and(|fields| !fields.is_empty())
        );
    }

    #[test]
    fn commitment_root_search_json_marks_empty_complete_coverage_as_unknown() {
        let result = commitment_root_search_json(CommitmentRootSearchJsonInput {
            root: "ab".repeat(32).as_str(),
            canonical: &[],
            orphaned: &[],
            canonical_coverage: &explorer::CommitmentRootSearchCoverage {
                complete_from_height: Some(280_000),
                complete_through_height: Some(4_158_484),
                latest_indexed_height: Some(4_158_484),
                canonical_history_complete: true,
            },
            displaced_root_capability: true,
            displaced_root_coverage: Some(&explorer::CommitmentRootSearchDisplacedCoverage {
                activation_event_sequence: Some(33738),
                activation_epoch_id: Some(42),
                activated_at_millis: Some(1_783_726_289_000),
                captured_block_count: 3,
                root_artifact_unavailable_count: 1,
                captured_range_complete: false,
            }),
        });

        assert_eq!(
            result["diagnosis"],
            json!(
                "This anchor root has no canonical match and no retained displaced match in the covered archive since activation; pre-activation displaced history remains unknown."
            )
        );
        assert_eq!(result["degraded"], json!(true));
        assert_eq!(
            result["displacedCoverage"]["activationEventSequence"],
            json!(33738)
        );
        assert_eq!(result["displacedCoverage"]["capturedBlockCount"], json!(3));
        assert_eq!(
            result["displacedCoverage"]["rootArtifactUnavailableCount"],
            json!(1)
        );
        assert_eq!(result["displacedCoverage"]["returnedMatchCount"], json!(0));
    }

    #[test]
    fn commitment_root_search_json_distinguishes_unactivated_displaced_coverage() {
        let result = commitment_root_search_json(CommitmentRootSearchJsonInput {
            root: &"ab".repeat(32),
            canonical: &[],
            orphaned: &[],
            canonical_coverage: &explorer::CommitmentRootSearchCoverage {
                complete_from_height: Some(280_000),
                complete_through_height: Some(4_158_484),
                latest_indexed_height: Some(4_158_484),
                canonical_history_complete: true,
            },
            displaced_root_capability: true,
            displaced_root_coverage: Some(&explorer::CommitmentRootSearchDisplacedCoverage {
                activation_event_sequence: None,
                activation_epoch_id: None,
                activated_at_millis: None,
                captured_block_count: 0,
                root_artifact_unavailable_count: 0,
                captured_range_complete: false,
            }),
        });

        assert_eq!(result["degraded"], json!(true));
        assert_eq!(
            result["unavailable"],
            json!([
                "No retained displaced commitment-root match exists in the covered archive since activation. The displaced-root archive has not activated because no post-deployment displacement has been captured. Pre-activation archive history remains unknown, so full historical orphan parity is unavailable."
            ])
        );
    }

    #[test]
    fn commitment_root_search_json_maps_retained_orphan_match_without_miner_inference() {
        let orphaned = vec![
            commitment_root_match_json(
                &explorer::CommitmentRootMatch {
                    block_height: 100,
                    block_hash: "cd".repeat(32),
                    block_time_unix_seconds: 1_783_726_289,
                    protocol: explorer::ShieldedProtocol::Orchard as i32,
                },
                None,
                "orphaned",
                None,
            )
            .unwrap_or_default(),
        ];
        let result = commitment_root_search_json(CommitmentRootSearchJsonInput {
            root: &"cd".repeat(32),
            canonical: &[],
            orphaned: &orphaned,
            canonical_coverage: &explorer::CommitmentRootSearchCoverage {
                complete_from_height: Some(280_000),
                complete_through_height: Some(4_158_484),
                latest_indexed_height: Some(4_158_484),
                canonical_history_complete: true,
            },
            displaced_root_capability: true,
            displaced_root_coverage: Some(&explorer::CommitmentRootSearchDisplacedCoverage {
                activation_event_sequence: Some(33738),
                activation_epoch_id: Some(42),
                activated_at_millis: Some(1_783_726_289_000),
                captured_block_count: 3,
                root_artifact_unavailable_count: 0,
                captured_range_complete: true,
            }),
        });

        assert_eq!(result["found"], json!(true));
        assert_eq!(result["orphaned"][0]["chain"], json!("orphaned"));
        assert!(result["orphaned"][0]["minerAddress"].is_null());
        assert_eq!(result["degraded"], json!(true));
        assert!(
            result["unavailable"]
                .as_array()
                .is_some_and(|fields| !fields.is_empty())
        );
    }

    #[test]
    fn commitment_root_match_json_maps_ironwood_without_product_inference() {
        let result = commitment_root_match_json(
            &explorer::CommitmentRootMatch {
                block_height: 4_158_484,
                block_hash: "ab".repeat(32),
                block_time_unix_seconds: 1_783_726_289,
                protocol: explorer::ShieldedProtocol::Ironwood as i32,
            },
            Some("tmExampleMinerAddress"),
            "canonical",
            None,
        );

        assert!(result.is_ok());
        let result = result.unwrap_or_default();
        assert_eq!(result["height"], json!(4_158_484));
        assert_eq!(result["matchedField"], json!("ironwood"));
        assert_eq!(result["chain"], json!("canonical"));
        assert_eq!(result["minerAddress"], json!("tmExampleMinerAddress"));
        assert!(result["minerPool"].is_null());
    }

    #[test]
    fn parse_raw_transaction_batch_txids_accepts_valid_batch() {
        let parsed = parse_raw_transaction_batch_txids(&json!({
            "txids": [SAMPLE_TRANSACTION_ID]
        }));

        assert!(parsed.is_ok());
        assert_eq!(
            parsed.unwrap_or_default(),
            vec![SAMPLE_TRANSACTION_ID.to_owned()]
        );
    }

    #[test]
    fn parse_raw_transaction_batch_txids_rejects_invalid_batch_shapes() {
        assert!(parse_raw_transaction_batch_txids(&json!({})).is_err());
        assert!(parse_raw_transaction_batch_txids(&json!({ "txids": [] })).is_err());
        assert!(parse_raw_transaction_batch_txids(&json!({ "txids": ["not-a-txid"] })).is_err());
    }

    #[test]
    fn parse_broadcast_raw_transaction_matches_cipherscan_validation() {
        assert_eq!(
            parse_broadcast_raw_transaction(&json!({ "rawTx": "00" })).ok(),
            Some(String::from("00"))
        );

        let missing = parse_broadcast_raw_transaction(&json!({})).err();
        assert!(missing.as_ref().is_some_and(|error| {
            error.field == "rawTx"
                && error.message == "Invalid input: expected string, received undefined"
        }));

        let non_string = parse_broadcast_raw_transaction(&json!({ "rawTx": 1 })).err();
        assert!(non_string.as_ref().is_some_and(|error| {
            error.field == "rawTx"
                && error.message == "Invalid input: expected string, received number"
        }));

        for raw_tx in ["", "not-hex", "0x00"] {
            let error = parse_broadcast_raw_transaction(&json!({ "rawTx": raw_tx })).err();
            assert!(error.as_ref().is_some_and(|error| {
                error.field == "rawTx" && error.message == "rawTx must be a valid hex string"
            }));
        }
    }

    #[tokio::test]
    async fn broadcast_response_preserves_cipherscan_success_duplicate_and_queued_contracts()
    -> Result<(), Box<dyn std::error::Error>> {
        let accepted = broadcast_response(Some(broadcast_transaction_response::Outcome::Accepted(
            wallet::BroadcastAccepted {
                transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
            },
        )));
        let (accepted_status, accepted_body) = read_json_response(accepted).await?;
        assert_eq!(accepted_status, StatusCode::OK);
        assert_eq!(accepted_body["success"], json!(true));
        assert_eq!(accepted_body["txid"], json!(SAMPLE_TRANSACTION_ID));

        let duplicate = broadcast_response(Some(
            broadcast_transaction_response::Outcome::Duplicate(wallet::BroadcastDuplicate {
                error_code: Some(-27),
                message: String::from("transaction already in block chain"),
            }),
        ));
        let (duplicate_status, duplicate_body) = read_json_response(duplicate).await?;
        assert_eq!(duplicate_status, StatusCode::BAD_REQUEST);
        assert_eq!(duplicate_body["success"], json!(false));
        assert_eq!(duplicate_body["duplicate"], json!(true));
        assert_eq!(
            duplicate_body["error"],
            json!("transaction already in block chain")
        );
        assert_eq!(duplicate_body["reason"], json!("duplicate"));
        assert_eq!(duplicate_body["errorCode"], json!(-27));
        assert!(duplicate_body.get("txid").is_none());

        let queued = broadcast_response(Some(broadcast_transaction_response::Outcome::Queued(
            wallet::BroadcastQueued {
                message: String::from("transaction is already queued"),
            },
        )));
        let (queued_status, queued_body) = read_json_response(queued).await?;
        assert_eq!(queued_status, StatusCode::BAD_REQUEST);
        assert_eq!(queued_body["success"], json!(false));
        assert_eq!(queued_body["queued"], json!(true));
        assert_eq!(queued_body["error"], json!("transaction is already queued"));
        assert_eq!(queued_body["reason"], json!("queued"));
        assert!(queued_body.get("txid").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn broadcast_response_preserves_rejection_and_unavailable_contracts()
    -> Result<(), Box<dyn std::error::Error>> {
        let invalid = broadcast_response(Some(
            broadcast_transaction_response::Outcome::InvalidEncoding(
                wallet::BroadcastInvalidEncoding {
                    error_code: Some(-22),
                    message: String::from("TX decode failed"),
                },
            ),
        ));
        let (invalid_status, invalid_body) = read_json_response(invalid).await?;
        assert_eq!(invalid_status, StatusCode::BAD_REQUEST);
        assert_eq!(invalid_body["success"], json!(false));
        assert_eq!(invalid_body["reason"], json!("invalid_encoding"));

        let rejected = broadcast_response(Some(broadcast_transaction_response::Outcome::Rejected(
            wallet::BroadcastRejected {
                error_code: Some(-26),
                message: String::from("bad-txns"),
                kind: wallet::BroadcastRejectionReason::InvalidSignature as i32,
            },
        )));
        let (rejected_status, rejected_body) = read_json_response(rejected).await?;
        assert_eq!(rejected_status, StatusCode::BAD_REQUEST);
        assert_eq!(rejected_body["success"], json!(false));
        assert_eq!(
            rejected_body["reason"],
            json!(wallet::BroadcastRejectionReason::InvalidSignature as i32),
        );

        let unknown = broadcast_response(Some(broadcast_transaction_response::Outcome::Unknown(
            wallet::BroadcastUnknown {
                error_code: Some(-1),
                message: String::from("unclassified node response"),
            },
        )));
        let (unknown_status, unknown_body) = read_json_response(unknown).await?;
        assert_eq!(unknown_status, StatusCode::BAD_GATEWAY);
        assert_eq!(unknown_body["success"], json!(false));
        assert_eq!(unknown_body["reason"], json!("unknown"));

        let missing = broadcast_response(None);
        let (missing_status, missing_body) = read_json_response(missing).await?;
        assert_eq!(missing_status, StatusCode::BAD_GATEWAY);
        assert_eq!(missing_body["success"], json!(false));
        assert_eq!(
            missing_body["error"],
            json!("Zinder returned no broadcast outcome"),
        );
        Ok(())
    }

    async fn read_json_response(
        response: Response,
    ) -> Result<(StatusCode, Value), Box<dyn std::error::Error>> {
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await?;
        Ok((status, serde_json::from_slice(&bytes)?))
    }

    #[test]
    fn raw_transaction_batch_json_uses_cipherscan_scanner_shape() {
        let transaction = raw_transaction_batch_row(SAMPLE_TRANSACTION_ID, &[0xab, 0xcd]);
        let transactions = vec![transaction];
        let failed = Vec::new();
        let response = raw_transaction_batch_json(&transactions, &failed, 1);

        assert_eq!(
            response["transactions"][0]["txid"],
            json!(SAMPLE_TRANSACTION_ID)
        );
        assert_eq!(response["transactions"][0]["hex"], json!("abcd"));
        assert_eq!(response["total"], json!(1));
        assert_eq!(response["successful"], json!(1));
        assert!(response.get("failed").is_none());

        let transactions = Vec::new();
        let failed = vec![raw_transaction_batch_failure(
            SAMPLE_TRANSACTION_ID,
            "not found",
        )];
        let failed_response = raw_transaction_batch_json(&transactions, &failed, 1);

        assert_eq!(failed_response["transactions"], json!([]));
        assert_eq!(
            failed_response["failed"][0]["txid"],
            json!(SAMPLE_TRANSACTION_ID)
        );
        assert_eq!(failed_response["failed"][0]["error"], json!("not found"));
        assert_eq!(failed_response["failed"][0]["success"], json!(false));
        assert_eq!(failed_response["successful"], json!(0));
    }

    #[test]
    fn parse_scan_range_matches_cipherscan_validation() {
        assert!(parse_scan_range(&json!({}), true).is_err());
        assert!(parse_scan_range(&json!({}), false).is_err());
        assert!(parse_scan_range(&json!({ "startHeight": 10 }), true).is_err());
        assert!(parse_scan_range(&json!({ "startHeight": "abc", "endHeight": 11 }), true).is_err());
        assert!(parse_scan_range(&json!({ "startHeight": 12, "endHeight": 11 }), true).is_err());
        assert!(
            parse_scan_range(
                &json!({ "startHeight": 1, "endHeight": MAX_SCAN_RANGE_BLOCKS + 2 }),
                true,
            )
            .is_err()
        );

        assert_eq!(
            parse_scan_range(&json!({ "startHeight": "10", "endHeight": "12" }), true).ok(),
            Some(ScanRange {
                start_height: 10,
                end_height: 12,
            })
        );
        assert_eq!(
            parse_scan_range(&json!({ "startHeight": 10 }), false).ok(),
            Some(ScanRange {
                start_height: 10,
                end_height: 10,
            })
        );
    }

    #[test]
    fn orchard_candidate_scan_range_preserves_validation_and_bounds() {
        assert_eq!(
            parse_orchard_candidate_scan_range(&json!({ "startHeight": 10, "endHeight": 12 })).ok(),
            Some(OrchardCandidateScanRange {
                start_height: 10,
                end_height: 12,
            })
        );
        assert_eq!(
            parse_orchard_candidate_scan_range(&json!({
                "startHeight": 1,
                "endHeight": MAX_ORCHARD_CANDIDATE_SCAN_BLOCKS + 1,
            }))
            .ok(),
            Some(OrchardCandidateScanRange {
                start_height: 1,
                end_height: 8_065,
            })
        );
        assert_eq!(
            parse_orchard_candidate_scan_range(&json!({
                "startHeight": 1,
                "endHeight": MAX_ORCHARD_CANDIDATE_SCAN_BLOCKS + 2,
            }))
            .err(),
            Some("Range too large (max 8064 blocks)")
        );
        assert_eq!(
            parse_orchard_candidate_scan_range(&json!({
                "startHeight": u64::from(u32::MAX) + 1,
                "endHeight": u64::from(u32::MAX) + 1,
            }))
            .err(),
            Some("Invalid block heights")
        );
        assert_eq!(
            parse_orchard_candidate_scan_range(&json!({
                "startHeight": 10,
                "endHeight": 12,
                "viewingKey": "secret",
            }))
            .err(),
            Some("Viewing keys are not accepted")
        );
    }

    #[test]
    fn orchard_candidate_scan_maps_complete_newest_first_pages() -> Result<(), CipherscanRestError>
    {
        let range = OrchardCandidateScanRange {
            start_height: 40,
            end_height: 42,
        };
        let coverage = explorer::TransactionHistoryCoverage {
            complete_from_height: 1,
            complete_through_height: 42,
            complete_through_hash: "00".repeat(32),
        };
        let entry = |height: u32, transaction_index: u32, transaction_id: &str| {
            explorer::TransactionHistoryEntry {
                transaction_id: transaction_id.to_owned(),
                block_height: height,
                block_time_unix_seconds: i64::from(height) * 75,
                transaction_index,
                component_counts: Some(explorer::TransactionComponentCounts {
                    orchard_action_count: 1,
                    ..Default::default()
                }),
                ..Default::default()
            }
        };
        let mut scan = OrchardCandidateScan::default();
        scan.observe_page(
            TransactionHistoryResponse {
                entries: vec![entry(42, 2, "a"), entry(41, 1, "b")],
                read_fence: Some(transaction_history_read_fence(7)),
                coverage: Some(coverage.clone()),
                ..Default::default()
            },
            range,
        )?;
        scan.observe_page(
            TransactionHistoryResponse {
                entries: vec![entry(40, 3, "c"), entry(39, 1, "d")],
                read_fence: Some(transaction_history_read_fence(7)),
                coverage: Some(coverage),
                ..Default::default()
            },
            range,
        )?;
        scan.sort_newest_first();

        let response = orchard_candidate_scan_json(range, &scan.entries);
        assert_eq!(response["startHeight"], json!(40));
        assert_eq!(response["endHeight"], json!(42));
        assert_eq!(response["totalBlocks"], json!(3));
        assert_eq!(response["orchardTransactions"], json!(3));
        assert_eq!(
            response["transactions"][0],
            json!({
                "txid": "a",
                "block_height": "42",
                "timestamp": "3150",
            })
        );
        assert_eq!(response["transactions"][2]["txid"], json!("c"));
        Ok(())
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "This fixture covers the three page-safety guards that share a fenced Orchard range."
    )]
    fn orchard_candidate_scan_rejects_fence_coverage_and_protocol_drift() {
        let range = OrchardCandidateScanRange {
            start_height: 40,
            end_height: 42,
        };
        let valid_coverage = explorer::TransactionHistoryCoverage {
            complete_from_height: 1,
            complete_through_height: 42,
            complete_through_hash: "00".repeat(32),
        };
        let orchard_entry = explorer::TransactionHistoryEntry {
            block_height: 42,
            transaction_index: 1,
            component_counts: Some(explorer::TransactionComponentCounts {
                orchard_action_count: 1,
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut scan = OrchardCandidateScan::default();
        assert!(
            scan.observe_page(
                TransactionHistoryResponse {
                    entries: vec![orchard_entry.clone()],
                    read_fence: Some(transaction_history_read_fence(7)),
                    coverage: Some(valid_coverage.clone()),
                    ..Default::default()
                },
                range,
            )
            .is_ok()
        );
        assert!(
            scan.observe_page(
                TransactionHistoryResponse {
                    entries: vec![explorer::TransactionHistoryEntry {
                        block_height: 41,
                        transaction_index: 1,
                        component_counts: Some(explorer::TransactionComponentCounts {
                            orchard_action_count: 1,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    read_fence: Some(transaction_history_read_fence(8)),
                    coverage: Some(valid_coverage),
                    ..Default::default()
                },
                range,
            )
            .is_err()
        );

        let incomplete_coverage = explorer::TransactionHistoryCoverage {
            complete_from_height: 1,
            complete_through_height: 41,
            complete_through_hash: "00".repeat(31) + "29",
        };
        assert!(
            OrchardCandidateScan::default()
                .observe_page(
                    TransactionHistoryResponse {
                        entries: vec![orchard_entry],
                        read_fence: Some(transaction_history_read_fence(7)),
                        coverage: Some(incomplete_coverage),
                        ..Default::default()
                    },
                    range,
                )
                .is_err()
        );

        let non_orchard_entry = explorer::TransactionHistoryEntry {
            block_height: 42,
            transaction_index: 1,
            component_counts: Some(explorer::TransactionComponentCounts::default()),
            ..Default::default()
        };
        assert!(
            OrchardCandidateScan::default()
                .observe_page(
                    TransactionHistoryResponse {
                        entries: vec![non_orchard_entry],
                        read_fence: Some(transaction_history_read_fence(7)),
                        coverage: Some(explorer::TransactionHistoryCoverage {
                            complete_from_height: 1,
                            complete_through_height: 42,
                            complete_through_hash: "00".repeat(32),
                        }),
                        ..Default::default()
                    },
                    range,
                )
                .is_err()
        );
    }

    #[tokio::test]
    async fn orchard_candidate_scan_upstream_errors_preserve_adapter_envelope()
    -> Result<(), Box<dyn std::error::Error>> {
        let (status, body) = read_json_response(
            CipherscanRestError::Upstream(tonic::Status::unavailable("explorer offline"))
                .into_response(),
        )
        .await?;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["success"], json!(false));
        assert_eq!(body["code"], json!("upstream_unavailable"));
        Ok(())
    }

    #[test]
    fn verbose_transaction_json_preserves_degraded_cipherscan_shape() {
        let response = verbose_transaction_json(SAMPLE_TRANSACTION_ID, &[0xab, 0xcd]);

        assert_eq!(response["txid"], json!(SAMPLE_TRANSACTION_ID));
        assert_eq!(response["hex"], json!("abcd"));
        assert_eq!(response["decoded"]["txid"], json!(SAMPLE_TRANSACTION_ID));
        assert_eq!(response["decoded"]["degraded"], json!(true));
        assert!(
            response["decoded"]["unavailable"]
                .as_array()
                .is_some_and(|fields| !fields.is_empty())
        );
        assert_eq!(response["degraded"], json!(true));
        assert_eq!(response["unavailable"], json!(["decoded"]));
    }

    #[test]
    fn parse_fork_monitor_height_accepts_numbers_and_strings() {
        assert_eq!(parse_fork_monitor_height(&json!(19138)), Some(19_138));
        assert_eq!(parse_fork_monitor_height(&json!("19138")), Some(19_138));
        assert_eq!(parse_fork_monitor_height(&json!(-1)), None);
        assert_eq!(parse_fork_monitor_height(&json!("not-a-height")), None);
        assert_eq!(
            parse_fork_monitor_height(&json!(u64::from(u32::MAX) + 1)),
            None
        );
    }

    #[test]
    fn reorg_stats_json_separates_archive_and_incident_counts() {
        let snapshot = ChainReorgHistorySnapshot {
            events: vec![sample_reorg_event()],
            is_truncated: false,
            is_projection_unavailable: false,
        };

        let stats = reorg_stats_json(&snapshot, Some(3));

        assert_eq!(stats["success"], json!(true));
        assert_eq!(stats["totalOrphanedBlocks"], json!(3));
        assert_eq!(stats["observedRevertedBlocks"], json!(3));
        assert_eq!(stats["totalForkEvents"], json!(1));
        assert_eq!(stats["reportsLast24h"], Value::Null);
        assert_eq!(stats["deepestReorg"], json!(3));
        assert_eq!(stats["degraded"], json!(true));
        assert_eq!(
            stats["unavailable"],
            json!([
                "Public tip-report activity is not retained by ChainReorgHistory.",
                "Displaced-block archive totals begin at archive activation and exclude earlier reorg incidents."
            ])
        );
    }

    #[test]
    fn reorg_forks_json_preserves_cipherscan_fork_shape() -> Result<(), CipherscanRestError> {
        let snapshot = ChainReorgHistorySnapshot {
            events: vec![sample_reorg_event()],
            is_truncated: false,
            is_projection_unavailable: false,
        };

        let forks = reorg_forks_with_archive_json(Network::ZcashTestnet, &snapshot, 20, 0, None)?;

        assert_eq!(forks["success"], json!(true));
        assert_eq!(forks["forks"][0]["id"], json!(42));
        assert_eq!(forks["forks"][0]["forkHeight"], json!(98));
        assert_eq!(forks["forks"][0]["depth"], json!(3));
        assert_eq!(forks["forks"][0]["canonicalTip"], json!(101));
        assert_eq!(forks["forks"][0]["orphanedCount"], json!(3));
        assert_eq!(
            forks["forks"][0]["source"],
            json!(CIPHERSCAN_ADAPTER_SOURCE)
        );
        assert_eq!(forks["forks"][0]["comparisons"], json!([]));
        assert_eq!(forks["pagination"]["total"], json!(1));
        assert_eq!(forks["pagination"]["limit"], json!(20));
        assert_eq!(forks["pagination"]["offset"], json!(0));
        assert_eq!(forks["pagination"]["hasMore"], json!(false));
        assert_eq!(forks["degraded"], json!(false));
        assert_eq!(forks["unavailable"], json!([]));
        assert_eq!(forks["forks"][0]["degraded"], json!(true));
        assert_eq!(
            forks["forks"][0]["unavailable"][0],
            json!("The displaced-block archive is not available from this deployment.")
        );

        let enabled_archive = DisplacedBlockHistoryResponse::default();
        let enabled_forks = reorg_forks_with_archive_json(
            Network::ZcashTestnet,
            &snapshot,
            20,
            0,
            Some(&enabled_archive),
        )?;
        assert_eq!(
            enabled_forks["forks"][0]["unavailable"][0],
            json!(
                "The displaced-block archive is enabled but has not captured its activation event."
            )
        );

        let archive = DisplacedBlockHistoryResponse {
            coverage: Some(explorer::DisplacedBlockArchiveCoverage {
                activation_event_sequence: 43,
                activation_epoch_id: 1,
                activated_at_millis: 1,
            }),
            ..DisplacedBlockHistoryResponse::default()
        };
        let pre_activation_forks =
            reorg_forks_with_archive_json(Network::ZcashTestnet, &snapshot, 20, 0, Some(&archive))?;
        assert_eq!(
            pre_activation_forks["forks"][0]["unavailable"][0],
            json!("This reorg incident predates displaced-block archive activation.")
        );
        Ok(())
    }

    #[test]
    fn reorg_serializers_mark_only_missing_projection_facts_unavailable()
    -> Result<(), CipherscanRestError> {
        let unavailable = ChainReorgHistorySnapshot {
            events: Vec::new(),
            is_truncated: false,
            is_projection_unavailable: true,
        };
        let stats = reorg_stats_json(&unavailable, None);
        let forks =
            reorg_forks_with_archive_json(Network::ZcashTestnet, &unavailable, 20, 0, None)?;

        for field in [
            "totalOrphanedBlocks",
            "totalForkEvents",
            "reportsLast24h",
            "deepestReorg",
        ] {
            assert_eq!(stats[field], Value::Null);
        }
        assert_eq!(stats["degraded"], json!(true));
        assert!(stats["unavailable"].as_array().is_some_and(|fields| {
            fields.iter().any(|field| {
                field == "ChainReorgHistory is not available from this Zinder explorer deployment."
            })
        }));
        assert_eq!(forks["forks"], json!([]));
        assert_eq!(forks["pagination"]["total"], Value::Null);
        assert_eq!(forks["degraded"], json!(true));
        Ok(())
    }

    #[test]
    fn reorg_serializers_do_not_label_partial_history_as_totals() -> Result<(), CipherscanRestError>
    {
        let truncated = ChainReorgHistorySnapshot {
            events: vec![sample_reorg_event()],
            is_truncated: true,
            is_projection_unavailable: false,
        };
        let stats = reorg_stats_json(&truncated, None);
        let forks = reorg_forks_with_archive_json(Network::ZcashTestnet, &truncated, 20, 0, None)?;

        assert_eq!(stats["totalOrphanedBlocks"], Value::Null);
        assert_eq!(stats["totalForkEvents"], Value::Null);
        assert_eq!(stats["deepestReorg"], Value::Null);
        assert_eq!(forks["pagination"]["total"], Value::Null);
        assert_eq!(forks["pagination"]["hasMore"], json!(true));
        assert!(forks["unavailable"].as_array().is_some_and(|fields| {
            fields.iter().any(|field| {
                field
                    .as_str()
                    .is_some_and(|field| field.contains("totals may be lower"))
            })
        }));
        Ok(())
    }

    #[test]
    fn uncles_route_query_matches_cipherscan_coercion_and_bounds() {
        for default_limit in [DEFAULT_NON_CANONICAL_BLOCK_LIMIT, DEFAULT_REORG_FORK_LIMIT] {
            assert_eq!(
                reorg_page_limit(None, default_limit, MAX_LIMIT),
                default_limit
            );
            assert_eq!(
                reorg_page_limit(Some("0"), default_limit, MAX_LIMIT),
                default_limit
            );
            assert_eq!(
                reorg_page_limit(Some("not-a-number"), default_limit, MAX_LIMIT),
                default_limit
            );
            assert_eq!(reorg_page_limit(Some("-4"), default_limit, MAX_LIMIT), 1);
            assert_eq!(
                reorg_page_limit(Some("999999999999999999999"), default_limit, MAX_LIMIT),
                MAX_LIMIT
            );
        }
        assert_eq!(
            reorg_page_limit(
                Some("999"),
                DEFAULT_NON_CANONICAL_BLOCK_LIMIT,
                MAX_NON_CANONICAL_BLOCK_LIMIT
            ),
            MAX_NON_CANONICAL_BLOCK_LIMIT
        );
        assert_eq!(reorg_page_offset(None), 0);
        assert_eq!(reorg_page_offset(Some("0")), 0);
        assert_eq!(reorg_page_offset(Some("not-a-number")), 0);
        assert_eq!(reorg_page_offset(Some("-4")), 0);
        assert_eq!(reorg_page_offset(Some("999999999999999999999")), u32::MAX);
    }

    #[test]
    fn non_canonical_blocks_json_preserves_empty_archive_shape() -> Result<(), CipherscanRestError>
    {
        let blocks = non_canonical_blocks_json(
            Network::ZcashTestnet,
            50,
            50,
            &DisplacedBlockHistoryResponse::default(),
        )?;

        assert_eq!(blocks["success"], json!(true));
        assert_eq!(blocks["orphanedBlocks"], json!([]));
        assert_eq!(blocks["pagination"]["total"], json!(0));
        assert_eq!(blocks["pagination"]["limit"], json!(50));
        assert_eq!(blocks["pagination"]["offset"], json!(50));
        assert_eq!(blocks["pagination"]["totalPages"], json!(0));
        assert_eq!(blocks["pagination"]["page"], json!(2));
        assert_eq!(blocks["pagination"]["hasMore"], json!(false));
        assert_eq!(blocks["degraded"], json!(true));
        assert!(
            blocks["unavailable"]
                .as_array()
                .is_some_and(|fields| !fields.is_empty())
        );
        Ok(())
    }

    #[test]
    fn displaced_block_json_populates_list_and_block_page() -> Result<(), CipherscanRestError> {
        let block = explorer::DisplacedBlockSummary {
            block_height: 100,
            block_hash: SAMPLE_BLOCK_HASH.to_owned(),
            previous_block_hash: "11".repeat(32),
            block_time_unix_seconds: 1_700_000_000,
            total_size_bytes: 1_642,
            difficulty_bits: 0x1f07_ffff,
            transaction_ids: vec![SAMPLE_TRANSACTION_ID.to_owned()],
            coinbase_outputs: vec![explorer::DisplacedBlockCoinbaseOutput {
                output_index: 0,
                value_zat: 625_000_000,
                script_pub_key: p2pkh_script(0x42),
            }],
            displacement_event_sequence: 42,
            displacement_epoch_id: 43,
            displaced_at_millis: 1_700_000_001_000,
        };
        let canonical = explorer::DisplacedBlockCanonicalCounterpart {
            block_height: 100,
            block_hash: "22".repeat(32),
            previous_block_hash: "11".repeat(32),
            block_time_unix_seconds: 1_700_000_002,
            total_size_bytes: 1_621,
            difficulty_bits: 0x1f07_ffff,
            transaction_count: 1,
            coinbase_outputs: vec![explorer::DisplacedBlockCoinbaseOutput {
                output_index: 0,
                value_zat: 625_000_000,
                script_pub_key: p2pkh_script(0x24),
            }],
        };
        let history = DisplacedBlockHistoryResponse {
            entries: vec![explorer::DisplacedBlockHistoryEntry {
                block: Some(block.clone()),
                current_canonical_block: Some(canonical.clone()),
            }],
            total_count: 1,
            ..Default::default()
        };

        let list = non_canonical_blocks_json(Network::ZcashTestnet, 50, 0, &history)?;
        assert_eq!(list["orphanedBlocks"][0]["height"], json!(100));
        assert_eq!(
            list["orphanedBlocks"][0]["canonicalHash"],
            json!(canonical.block_hash)
        );
        assert!(list["orphanedBlocks"][0]["minerAddress"].is_string());

        let page = displaced_block_page_json(
            Network::ZcashTestnet,
            &explorer::DisplacedBlockDetailResponse {
                block: Some(block),
                current_canonical_block: Some(canonical),
                ..Default::default()
            },
        )?;
        assert_eq!(page["isOrphaned"], json!(true));
        assert_eq!(page["transactions"], json!([]));
        assert_eq!(page["canonicalBlock"]["height"], json!(100));
        Ok(())
    }

    #[test]
    fn reorg_nodes_json_preserves_empty_monitor_shape() {
        let nodes = reorg_nodes_json();

        assert_eq!(nodes["success"], json!(true));
        assert_eq!(nodes["nodes"], json!([]));
        assert_eq!(nodes["summary"]["total"], json!(0));
        assert_eq!(nodes["summary"]["online"], json!(0));
        assert_eq!(nodes["summary"]["forking"], json!(0));
        assert_eq!(nodes["summary"]["lastPoll"], Value::Null);
        assert_eq!(nodes["degraded"], json!(true));
    }

    #[test]
    fn chain_info_json_matches_cipherscan_height_aliases() {
        let info = chain_info_json(42);

        assert_eq!(info, json!({ "blocks": "42", "height": "42" }));
    }

    #[test]
    fn network_health_json_marks_the_zinder_query_plane_as_ready() {
        let health = network_health_json(
            &explorer::ServerInfoResponse::default(),
            &wallet::ServerInfoResponse::default(),
        );

        assert_eq!(health["success"], json!(true));
        assert_eq!(health["adapter"]["healthy"], json!(true));
        assert_eq!(health["zebra"]["healthy"], json!(true));
        assert_eq!(health["zebra"]["ready"], json!(true));
        assert_eq!(health["zebra"]["healthEndpointAvailable"], json!(false));
        assert_eq!(health["zebra"]["readyEndpointAvailable"], json!(false));
        assert_eq!(health["zebra"]["source"], json!("zinder-query-plane"));
        assert!(
            health["timestamp"]
                .as_i64()
                .is_some_and(|timestamp| timestamp > 0)
        );
    }

    #[test]
    fn blockchain_info_json_preserves_getblockchaininfo_shape() {
        let tip = wallet::BlockId {
            height: 42,
            block_hash: SAMPLE_BLOCK_HASH.to_owned(),
        };

        let info = blockchain_info_json(Network::ZcashTestnet, &tip);

        assert_eq!(info["chain"], json!("test"));
        assert_eq!(info["blocks"], json!(42));
        assert_eq!(info["headers"], json!(42));
        assert_eq!(info["bestblockhash"], json!(SAMPLE_BLOCK_HASH));
        assert_eq!(info["estimatedheight"], json!(42));
        assert_eq!(info["verificationprogress"], json!(1.0));
        assert_eq!(info["initialblockdownload"], json!(false));
        assert_eq!(info["pruned"], json!(false));
        assert_eq!(info["consensus"]["chaintip"], Value::Null);
        assert_eq!(info["upgrades"], json!({}));
        assert_eq!(info["degraded"], json!(true));
        assert_eq!(info["source"], json!(CIPHERSCAN_ADAPTER_SOURCE));
    }

    #[test]
    fn block_list_pagination_preserves_cipherscan_cursors() {
        let entries = vec![
            block_list_entry(100, 1.0),
            block_list_entry(99, 1.0),
            block_list_entry(98, 1.0),
        ];

        let pagination = block_list_pagination(1, 3, 100, &entries);

        assert_eq!(pagination["page"], json!(1));
        assert_eq!(pagination["totalPages"], json!(34));
        assert_eq!(pagination["total"], json!(100));
        assert_eq!(pagination["hasNext"], json!(true));
        assert_eq!(pagination["hasPrev"], json!(false));
        assert_eq!(pagination["nextCursor"], json!(98));
        assert_eq!(pagination["prevCursor"], json!(100));
        assert_eq!(json!(zec_from_unsigned_zatoshis(137_500_000)), json!(1.375));

        let cursor_page = block_list_pagination(1, 3, 100, &[block_list_entry(94, 1.0)]);

        assert_eq!(cursor_page["page"], json!(3));
    }

    #[test]
    fn block_list_row_exposes_network_relative_difficulty() {
        let mut entry = block_list_entry(42, 39.485_703_005_553_46);
        entry.miner_address = Some("tmUcufCrN94ZXNuffjzWPdB3PSAYpc2KmSw".to_owned());
        let row = block_list_row(&entry);

        assert_eq!(row["difficulty"], json!("39.4857030055535"));
        assert_eq!(
            row["miner_address"],
            json!("tmUcufCrN94ZXNuffjzWPdB3PSAYpc2KmSw")
        );
        assert_eq!(cipherscan_difficulty_string(42.0), "42");
    }

    #[test]
    fn block_list_entry_decodes_the_first_standard_coinbase_output()
    -> Result<(), CipherscanRestError> {
        let mut script_pub_key = vec![0x76, 0xa9, 0x14];
        script_pub_key.extend_from_slice(&[0x42; 20]);
        script_pub_key.extend_from_slice(&[0x88, 0xac]);
        let entry = CipherscanBlockListEntry::try_from_point(
            Network::ZcashTestnet,
            explorer::BlockProductionPoint {
                summary: Some(explorer::BlockSummary::default()),
                bits: 0x1f34_bb90,
                coinbase: Some(explorer::CoinbaseTransactionSummary {
                    transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
                    transparent_outputs: vec![wallet::TransparentOutput {
                        value_zat: 137_500_000,
                        script_pub_key,
                    }],
                    has_shielded_outputs: Some(false),
                }),
            },
        )?;

        assert!(
            entry
                .miner_address
                .is_some_and(|address| address.starts_with("tm"))
        );
        Ok(())
    }

    #[test]
    fn network_activity_window_matches_cipherscan_24_hour_formulas()
    -> Result<(), CipherscanRestError> {
        let entries = vec![
            network_activity_entry(3, 1_100, 3, 72.0),
            network_activity_entry(2, 1_050, 4, 70.0),
            network_activity_entry(1, 999, 9, 68.0),
        ];

        let activity = NetworkActivityWindow::from_entries(&entries, 3, 1_000)?;

        assert_eq!(activity.block_count, 2);
        assert_eq!(activity.transaction_count, 7);
        assert_eq!(activity.average_block_time_seconds, 43_200);
        assert!((activity.latest_difficulty - 72.0).abs() < f64::EPSILON);
        assert!((activity.network_hashrate_raw - (72.0 / 43_200.0)).abs() < f64::EPSILON);
        assert_eq!(cipherscan_hashrate_string(1.829_992), "1.83 H/s");
        Ok(())
    }

    fn network_activity_entry(
        block_height: u32,
        block_time_unix_seconds: i64,
        transaction_count: u32,
        difficulty: f64,
    ) -> CipherscanBlockListEntry {
        CipherscanBlockListEntry {
            summary: explorer::BlockSummary {
                block_height,
                block_time_unix_seconds,
                transaction_count,
                ..Default::default()
            },
            difficulty,
            miner_address: None,
        }
    }

    fn block_list_entry(block_height: u32, difficulty: f64) -> CipherscanBlockListEntry {
        CipherscanBlockListEntry {
            summary: explorer::BlockSummary {
                block_height,
                ..Default::default()
            },
            difficulty,
            miner_address: None,
        }
    }

    #[test]
    fn block_row_matches_cipherscan_list_field_types() {
        let row = block_row(&explorer::BlockSummary {
            block_height: 42,
            block_hash: SAMPLE_BLOCK_HASH.to_owned(),
            block_time_unix_seconds: 1_700_000_000,
            transaction_count: 3,
            total_size_bytes: 1_234,
            paid_fees_collected_zat: Some(10_000),
            fees_collected_zat: 20_000,
            coinbase_reward_zat: 137_500_000,
            confirmations: 100,
            is_canonical: true,
            ..Default::default()
        });

        assert_eq!(row["height"], json!("42"));
        assert_eq!(row["timestamp"], json!("1700000000"));
        assert_eq!(row["transaction_count"], json!(3));
        assert_eq!(row["total_fees"], json!("10000"));
        assert_eq!(row["coinbase_reward"], json!("137500000"));
        assert_eq!(row["difficulty"], Value::Null);
        assert_eq!(row["miner_address"], Value::Null);
        assert_eq!(row["miner_pool"], Value::Null);
        assert_eq!(row["finality_status"], json!("Finalized"));
    }

    #[test]
    fn block_header_fields_preserve_cipherscan_difficulty_encoding() {
        let header = wallet::BlockHeader {
            bits: 0x1f34_bb90,
            nonce: vec![0x67, 0x00, 0x01],
            ..Default::default()
        };

        let fields = cipherscan_block_header_fields(Network::ZcashTestnet, &header);

        assert!(fields.is_ok());
        if let Ok(fields) = fields {
            assert_eq!(fields.bits, "1f34bb90");
            assert_eq!(fields.nonce, "010067");
            assert!(
                (fields.difficulty - 38.837_332_691_337_22).abs() < 1e-12,
                "unexpected difficulty: {}",
                fields.difficulty
            );
        }
    }

    #[test]
    fn network_fees_json_preserves_cipherscan_zip317_contract() {
        let summary = explorer::FeeSummaryResponse {
            freshness: None,
            block_count: 12,
            transaction_count: 34,
            total_zip317_conventional_fee_zat: 560_000,
            min_zip317_conventional_fee_zat: 10_000,
            max_zip317_conventional_fee_zat: 25_000,
        };

        let fees = network_fees_json(&summary, 100, 355);

        assert_eq!(fees["success"], json!(true));
        assert_eq!(fees["fees"]["low"], json!(0.000_1));
        assert_eq!(fees["fees"]["standard"], json!(0.000_15));
        assert_eq!(fees["fees"]["high"], json!(0.000_25));
        assert_eq!(fees["unit"], json!("ZEC"));
        assert_eq!(fees["zip317"]["marginalFee"], json!(5_000));
        assert_eq!(fees["zip317"]["graceActions"], json!(2));
        assert_eq!(fees["zip317"]["p2pkhStandardFee"], json!(10_000));
        assert!(
            fees["timestamp"]
                .as_i64()
                .is_some_and(|timestamp| timestamp > 0)
        );
        assert_eq!(fees["observedZip317"]["startHeight"], json!(100));
        assert_eq!(fees["observedZip317"]["endHeight"], json!(355));
        assert_eq!(fees["observedZip317"]["blockCount"], json!(12));
        assert_eq!(fees["observedZip317"]["transactionCount"], json!(34));
        assert_eq!(
            fees["observedZip317"]["totalConventionalFeeZat"],
            json!("560000")
        );
        assert_eq!(
            fees["observedZip317"]["minConventionalFeeZat"],
            json!("10000")
        );
        assert_eq!(
            fees["observedZip317"]["maxConventionalFeeZat"],
            json!("25000")
        );
        assert_eq!(
            fees["observedZip317"]["source"],
            json!(CIPHERSCAN_ADAPTER_SOURCE)
        );
    }

    #[test]
    fn peer_inventory_json_preserves_empty_cipherscan_shape() {
        let inventory = peer_inventory_json();

        assert_eq!(inventory["success"], json!(true));
        assert_eq!(inventory["count"], json!(0));
        assert_eq!(inventory["peers"], json!([]));
        assert!(
            inventory["timestamp"]
                .as_i64()
                .is_some_and(|timestamp| timestamp > 0)
        );
        assert_eq!(inventory["degraded"], json!(true));
        assert!(
            inventory["unavailable"]
                .as_array()
                .is_some_and(|fields| !fields.is_empty())
        );
    }

    #[test]
    fn node_locations_json_preserves_cipherscan_locations_shape() {
        let locations = node_locations_json();

        assert_eq!(locations["success"], json!(true));
        assert_eq!(locations["locations"], json!([]));
        assert!(
            locations["timestamp"]
                .as_i64()
                .is_some_and(|timestamp| timestamp > 0)
        );
        assert_eq!(locations["degraded"], json!(true));
        assert!(
            locations["unavailable"]
                .as_array()
                .is_some_and(|fields| !fields.is_empty())
        );
    }

    #[test]
    fn node_statistics_json_preserves_cipherscan_stats_shape() {
        let statistics = node_statistics_json();

        assert_eq!(statistics["success"], json!(true));
        assert_eq!(statistics["stats"]["activeNodes"], json!(0));
        assert_eq!(statistics["stats"]["totalNodes"], json!(0));
        assert_eq!(statistics["stats"]["countries"], json!(0));
        assert_eq!(statistics["stats"]["cities"], json!(0));
        assert_eq!(statistics["stats"]["avgPingMs"], Value::Null);
        assert_eq!(statistics["stats"]["torNodes"], json!(0));
        assert_eq!(statistics["stats"]["lastUpdated"], Value::Null);
        assert_eq!(statistics["trends"]["change24h"], Value::Null);
        assert_eq!(statistics["trends"]["change7d"], Value::Null);
        assert_eq!(statistics["trends"]["change30d"], Value::Null);
        assert_eq!(statistics["topCountries"], json!([]));
        assert!(
            statistics["timestamp"]
                .as_i64()
                .is_some_and(|timestamp| timestamp > 0)
        );
        assert_eq!(statistics["degraded"], json!(true));
    }

    #[test]
    fn node_history_json_preserves_cipherscan_snapshot_shape() {
        let history = node_history_json("7d");

        assert_eq!(history["success"], json!(true));
        assert_eq!(history["period"], json!("7d"));
        assert_eq!(history["snapshots"], json!([]));
        assert!(
            history["timestamp"]
                .as_i64()
                .is_some_and(|timestamp| timestamp > 0)
        );
        assert_eq!(history["degraded"], json!(true));
        assert!(
            history["unavailable"]
                .as_array()
                .is_some_and(|fields| !fields.is_empty())
        );
    }

    #[test]
    fn mining_metrics_json_preserves_cipherscan_chart_shape() -> Result<(), CipherscanRestError> {
        let response = explorer::BlockProductionSeriesResponse {
            start_height: 100,
            end_height: 100,
            covered_block_count: 1,
            missing_block_count: 0,
            points: vec![explorer::BlockProductionPoint {
                summary: Some(explorer::BlockSummary {
                    block_height: 100,
                    block_time_unix_seconds: 1_700_000_000,
                    transaction_count: 3,
                    fees_collected_zat: 65_000,
                    ..Default::default()
                }),
                bits: 0x1f34_bb90,
                coinbase: None,
            }],
            ..Default::default()
        };

        let metrics = mining_metrics_json(Network::ZcashTestnet, 20, &response)?;

        assert_eq!(metrics["success"], json!(true));
        assert_eq!(metrics["window"], json!(20));
        assert_eq!(metrics["latest"]["blockTime"], json!(75.0));
        assert_eq!(metrics["latest"]["txFees"], json!(0.000_65));
        assert_eq!(metrics["latest"]["txCount"], json!(3.0));
        assert_eq!(metrics["points"].as_array().map(Vec::len), Some(1));
        assert_eq!(metrics["points"][0]["height"], json!(100));
        for field in ["solrate", "difficulty", "blockTime", "txFees", "txCount"] {
            assert_eq!(metrics["latest"][field], metrics["points"][0][field]);
        }
        assert_eq!(metrics["coverage"]["coveredBlocks"], json!(1));
        assert_eq!(metrics["coverage"]["missingBlocks"], json!(0));
        assert_eq!(metrics["coverage"]["paidFeeBlocks"], json!(0));
        assert_eq!(metrics["coverage"]["conventionalFeeBlocks"], json!(1));
        assert_eq!(metrics["degraded"], json!(true));
        Ok(())
    }

    #[test]
    fn mining_metrics_query_matches_cipherscan_coercion_and_bounds() {
        assert_eq!(mining_metrics_window(None), 20);
        assert_eq!(mining_metrics_window(Some("bad")), 20);
        assert_eq!(mining_metrics_window(Some("0")), 20);
        assert_eq!(mining_metrics_window(Some("-9")), 5);
        assert_eq!(mining_metrics_window(Some("7.9xyz")), 7);
        assert_eq!(mining_metrics_window(Some("999")), 100);
        assert_eq!(mining_metrics_limit(None), 120);
        assert_eq!(mining_metrics_limit(Some("bad")), 120);
        assert_eq!(mining_metrics_limit(Some("0")), 120);
        assert_eq!(mining_metrics_limit(Some("-9")), 20);
        assert_eq!(mining_metrics_limit(Some("21.8xyz")), 21);
        assert_eq!(mining_metrics_limit(Some("999")), 500);
    }

    #[test]
    fn address_summary_maps_native_lifetime_facts_and_public_types()
    -> Result<(), CipherscanRestError> {
        let epoch = address_activity_epoch(7, 200);
        let activity = address_activity_response(
            epoch,
            explorer::TransparentAddressSummary {
                balance_zat: 150_000_000,
                total_received_zat: Some(250_000_000),
                total_sent_zat: Some(100_000_000),
                distinct_transaction_count: Some(2),
                first_seen_unix_seconds: Some(1_700_000_000),
                last_seen_unix_seconds: Some(1_700_000_075),
                ..Default::default()
            },
            Vec::new(),
        );

        let response = address_detail_json(&CipherscanAddressDetailInput {
            network: Network::ZcashTestnet,
            address: "tmFkhJaNXuoMeKKBHp8EE9oiFW4uXKAPWnH",
            page: 1,
            limit: 25,
            activity: &activity,
        })?;

        assert_eq!(response["balance"], json!(150_000_000));
        assert_eq!(response["totalReceived"], json!(250_000_000));
        assert_eq!(response["totalSent"], json!(100_000_000));
        assert_eq!(response["txCount"], json!(2));
        assert_eq!(response["firstSeen"], json!("1700000000"));
        assert_eq!(response["lastSeen"], json!("1700000075"));
        assert_eq!(response["degraded"], json!(false));
        Ok(())
    }

    #[test]
    fn address_summary_marks_a_same_height_indexed_tip_mismatch_as_degraded()
    -> Result<(), CipherscanRestError> {
        let epoch = address_activity_epoch(7, 200);
        let mut activity = address_activity_response(
            epoch,
            explorer::TransparentAddressSummary {
                balance_zat: 1,
                total_received_zat: Some(1),
                total_sent_zat: Some(0),
                distinct_transaction_count: Some(1),
                first_seen_unix_seconds: Some(1),
                last_seen_unix_seconds: Some(1),
                ..Default::default()
            },
            Vec::new(),
        );
        let indexed_tip = activity
            .freshness
            .as_mut()
            .and_then(|freshness| freshness.chain_view.as_mut())
            .and_then(|chain_view| chain_view.indexed_tip.as_mut())
            .and_then(|indexed_tip| indexed_tip.tip.as_mut())
            .ok_or(CipherscanRestError::MissingUpstreamField(
                "test.indexed_tip",
            ))?;
        indexed_tip.hash = "22".repeat(32);

        let response = address_detail_json(&CipherscanAddressDetailInput {
            network: Network::ZcashTestnet,
            address: "tmFkhJaNXuoMeKKBHp8EE9oiFW4uXKAPWnH",
            page: 1,
            limit: 25,
            activity: &activity,
        })?;

        assert_eq!(response["degraded"], json!(true));
        assert!(response["unavailable"].as_array().is_some_and(|reasons| {
            reasons.iter().any(|reason| {
                reason
                    == "Transparent-address materialized views do not match the pinned canonical tip yet."
            })
        }));
        Ok(())
    }

    #[test]
    fn address_page_two_uses_checked_offset_and_exact_pagination() -> Result<(), CipherscanRestError>
    {
        let query = PageQuery {
            page: Some(2),
            limit: Some(25),
            ..Default::default()
        };
        assert_eq!(address_activity_page(&query)?, (2, 25, 25));

        let epoch = address_activity_epoch(7, 200);
        let activity = address_activity_response(
            epoch,
            explorer::TransparentAddressSummary {
                balance_zat: 1,
                total_received_zat: Some(51),
                total_sent_zat: Some(50),
                distinct_transaction_count: Some(51),
                first_seen_unix_seconds: Some(1),
                last_seen_unix_seconds: Some(2),
                ..Default::default()
            },
            Vec::new(),
        );
        let response = address_detail_json(&CipherscanAddressDetailInput {
            network: Network::ZcashTestnet,
            address: "tmFkhJaNXuoMeKKBHp8EE9oiFW4uXKAPWnH",
            page: 2,
            limit: 25,
            activity: &activity,
        })?;

        assert_eq!(
            response["pagination"],
            json!({
                "page": 2,
                "limit": 25,
                "total": 51,
                "totalPages": 3,
                "hasNext": true,
                "hasPrev": true,
            })
        );
        Ok(())
    }

    #[test]
    fn unused_transparent_address_returns_public_zero_history_shape()
    -> Result<(), CipherscanRestError> {
        let epoch = address_activity_epoch(7, 200);
        let activity = address_activity_response(
            epoch,
            explorer::TransparentAddressSummary::default(),
            Vec::new(),
        );

        let response = address_detail_json(&CipherscanAddressDetailInput {
            network: Network::ZcashTestnet,
            address: "tmFkhJaNXuoMeKKBHp8EE9oiFW4uXKAPWnH",
            page: 2,
            limit: 25,
            activity: &activity,
        })?;

        assert_eq!(response["balance"], json!(0));
        assert_eq!(response["totalReceived"], json!(0));
        assert_eq!(response["totalSent"], json!(0));
        assert_eq!(response["txCount"], json!(0));
        assert_eq!(response["firstSeen"], Value::Null);
        assert_eq!(response["lastSeen"], Value::Null);
        assert_eq!(response["transactions"], json!([]));
        assert_eq!(response["pagination"]["page"], json!(1));
        assert_eq!(response["pagination"]["totalPages"], json!(0));
        assert_eq!(
            response["note"],
            json!("This address has no transaction history yet.")
        );
        assert_eq!(response["degraded"], json!(false));
        Ok(())
    }

    #[test]
    fn address_activity_row_maps_deterministic_standard_counterparties()
    -> Result<(), CipherscanRestError> {
        let requested_script = p2pkh_script(0x42);
        let requested_address =
            cipherscan_transparent_address(Network::ZcashTestnet, &requested_script)
                .ok_or(CipherscanRestError::InvalidUpstreamField("test.address"))?;
        let sender_script = p2pkh_script(0x11);
        let recipient_script = p2pkh_script(0x22);
        let expected_sender = cipherscan_transparent_address(Network::ZcashTestnet, &sender_script)
            .ok_or(CipherscanRestError::InvalidUpstreamField("test.sender"))?;
        let entry = complete_address_activity_entry(
            vec![
                sender_script.clone(),
                requested_script.clone(),
                sender_script,
                vec![0x51],
            ],
            vec![recipient_script, requested_script],
        );

        let (row, unavailable) =
            address_activity_row(Network::ZcashTestnet, &requested_address, &entry)?;

        assert_eq!(row["blockTime"], json!("1700000000"));
        assert_eq!(row["size"], json!(250));
        assert_eq!(row["txIndex"], json!(3));
        assert_eq!(row["hasSapling"], json!(true));
        assert_eq!(row["hasOrchard"], json!(true));
        assert_eq!(row["hasIronwood"], json!(true));
        assert_eq!(row["inputValue"], json!(50));
        assert_eq!(row["outputValue"], json!(70));
        assert_eq!(row["netChange"], json!(20));
        assert_eq!(row["counterparty"], json!(expected_sender));
        assert_eq!(row["senderCount"], json!(1));
        assert_eq!(row["recipientCount"], json!(1));
        assert!(unavailable.is_empty());
        Ok(())
    }

    #[test]
    fn incomplete_input_coverage_withholds_receiving_counterparty()
    -> Result<(), CipherscanRestError> {
        let mut entry = complete_address_activity_entry(vec![p2pkh_script(0x11)], Vec::new());
        entry.input_facts_complete = false;
        entry.input_value_zat = None;

        let (row, unavailable) = address_activity_row(
            Network::ZcashTestnet,
            "tmFkhJaNXuoMeKKBHp8EE9oiFW4uXKAPWnH",
            &entry,
        )?;

        assert_eq!(row["inputValue"], Value::Null);
        assert_eq!(row["counterparty"], Value::Null);
        assert_eq!(row["senderCount"], json!(1));
        assert!(
            unavailable
                .iter()
                .any(|reason| reason.contains("senderCount is partial"))
        );
        assert!(
            row["zinderUnavailable"]
                .as_array()
                .is_some_and(|reasons| !reasons.is_empty())
        );
        Ok(())
    }

    #[test]
    fn address_activity_page_enforces_defaults_bounds_and_overflow() {
        assert_eq!(
            address_activity_page(&PageQuery::default()).ok(),
            Some((1, 25, 0))
        );
        assert_eq!(
            address_activity_page(&PageQuery {
                page: Some(0),
                limit: Some(0),
                ..Default::default()
            })
            .ok(),
            Some((1, 25, 0))
        );
        assert_eq!(
            address_activity_page(&PageQuery {
                page: Some(2),
                limit: Some(500),
                ..Default::default()
            })
            .ok(),
            Some((2, 100, 100))
        );
        assert!(
            address_activity_page(&PageQuery {
                page: Some(1_002),
                limit: Some(100),
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            address_activity_page(&PageQuery {
                page: Some(u32::MAX),
                limit: Some(100),
                ..Default::default()
            })
            .is_err()
        );
    }

    #[test]
    fn address_activity_requires_its_own_chain_epoch() {
        let epoch = address_activity_epoch(7, 200);
        let mut activity = address_activity_response(
            epoch,
            explorer::TransparentAddressSummary {
                balance_zat: 10,
                ..Default::default()
            },
            Vec::new(),
        );
        if let Some(chain_view) = activity
            .freshness
            .as_mut()
            .and_then(|freshness| freshness.chain_view.as_mut())
        {
            chain_view.chain_epoch = None;
        }
        assert!(matches!(
            address_activity_chain_epoch(&activity),
            Err(CipherscanRestError::MissingUpstreamField(
                "transparent_address_activity.freshness.chain_view.chain_epoch"
            ))
        ));
    }

    fn address_activity_epoch(chain_epoch_id: u64, height: u32) -> wallet::ChainEpoch {
        wallet::ChainEpoch {
            chain_epoch_id,
            visible_tip: Some(wallet::BlockTip {
                height,
                hash: SAMPLE_BLOCK_HASH.to_owned(),
            }),
            ..Default::default()
        }
    }

    fn address_activity_response(
        chain_epoch: wallet::ChainEpoch,
        summary: explorer::TransparentAddressSummary,
        entries: Vec<explorer::TransparentAddressActivityEntry>,
    ) -> explorer::TransparentAddressActivityResponse {
        let height = chain_epoch.visible_tip.as_ref().map_or(0, |tip| tip.height);
        explorer::TransparentAddressActivityResponse {
            freshness: Some(explorer::ExplorerFreshness {
                chain_view: Some(wallet::ChainView {
                    indexed_tip: chain_epoch
                        .visible_tip
                        .clone()
                        .map(|tip| wallet::IndexedTip {
                            tip: Some(tip),
                            block_time_unix_seconds: 0,
                        }),
                    chain_epoch: Some(chain_epoch),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            entries,
            summary: Some(summary),
            coverage: Some(explorer::TransparentAddressRankingCoverage {
                balance_complete_through_height: height,
                history_complete_from_height: Some(0),
                history_complete_through_height: Some(height),
                lifetime_statistics_complete: true,
            }),
            ..Default::default()
        }
    }

    fn complete_address_activity_entry(
        other_input_script_pub_keys: Vec<Vec<u8>>,
        other_output_script_pub_keys: Vec<Vec<u8>>,
    ) -> explorer::TransparentAddressActivityEntry {
        explorer::TransparentAddressActivityEntry {
            transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
            block_height: 100,
            block_time_unix_seconds: 1_700_000_000,
            net_value_zat: Some(20),
            input_count: 1,
            output_count: 1,
            transaction_index: Some(3),
            size_bytes: Some(250),
            component_counts: Some(explorer::TransactionComponentCounts {
                sapling_spend_count: 1,
                orchard_action_count: 1,
                ironwood_action_count: 1,
                ..Default::default()
            }),
            input_value_zat: Some(50),
            output_value_zat: Some(70),
            other_input_script_pub_keys,
            other_output_script_pub_keys,
            input_facts_complete: true,
            ..Default::default()
        }
    }

    fn p2pkh_script(byte: u8) -> Vec<u8> {
        let mut script = vec![0x76, 0xa9, 0x14];
        script.extend_from_slice(&[byte; 20]);
        script.extend_from_slice(&[0x88, 0xac]);
        script
    }

    #[test]
    fn rich_list_query_matches_cipherscan_coercion_and_bounds() {
        assert_eq!(rich_list_limit(None), 100);
        assert_eq!(rich_list_limit(Some("bad")), 100);
        assert_eq!(rich_list_limit(Some("0")), 100);
        assert_eq!(rich_list_limit(Some("-9")), 1);
        assert_eq!(rich_list_limit(Some("25entries")), 25);
        assert_eq!(rich_list_limit(Some("999")), 500);
        assert_eq!(rich_list_offset(None), 0);
        assert_eq!(rich_list_offset(Some("bad")), 0);
        assert_eq!(rich_list_offset(Some("-1")), 0);
        assert_eq!(rich_list_offset(Some("25entries")), 25);
    }

    #[test]
    fn rich_list_maps_native_ranking_to_cipherscan_shape() -> Result<(), CipherscanRestError> {
        let ranking = complete_rich_list_ranking();

        let response = rich_list_json(Network::ZcashTestnet, 100, 100, &ranking)?;

        assert_eq!(response["success"], json!(true));
        assert_eq!(response["addresses"][0]["rank"], json!(101));
        assert_eq!(
            response["addresses"][0]["address"],
            json!("tmFkhJaNXuoMeKKBHp8EE9oiFW4uXKAPWnH")
        );
        assert_eq!(response["addresses"][0]["balance"], json!(1.5));
        assert_eq!(response["addresses"][0]["totalReceived"], json!(2.5));
        assert_eq!(response["addresses"][0]["totalSent"], json!(1.0));
        assert_eq!(response["addresses"][0]["txCount"], json!(7));
        assert_eq!(response["addresses"][0]["firstSeen"], json!("1700000000"));
        assert_eq!(
            response["addresses"][1]["address"],
            json!("t29qubbGSKwqZBeexC4uKjncdKSn8Pv3s6v")
        );
        for field in ["label", "category", "description", "logoUrl"] {
            assert_eq!(response["addresses"][0][field], Value::Null);
        }
        assert_eq!(response["addresses"][0]["verified"], json!(false));
        assert_eq!(
            response["concentration"],
            json!({
                "top10": 25_000_000.0,
                "top100": 50_000_000.0,
                "totalTransparent": 80_000_000.0,
                "top10Pct": 31.25,
                "top100Pct": 62.5,
            })
        );
        assert_eq!(
            response["pagination"],
            json!({
                "total": 250,
                "limit": 100,
                "offset": 100,
                "totalPages": 3,
                "page": 2,
                "hasNext": true,
                "hasPrev": true,
            })
        );
        assert_eq!(response["degraded"], json!(false));
        assert_eq!(response["unavailable"], json!([]));
        Ok(())
    }

    #[test]
    fn rich_list_marks_incomplete_native_coverage_unavailable() -> Result<(), CipherscanRestError> {
        let mut script_pub_key = vec![0x76, 0xa9, 0x14];
        script_pub_key.extend_from_slice(&[0x42; 20]);
        script_pub_key.extend_from_slice(&[0x88, 0xac]);
        let ranking = TransparentAddressRankingResponse {
            freshness: Some(rich_list_freshness(200)),
            entries: vec![explorer::TransparentAddressRankingEntry {
                rank: 1,
                script_pub_key,
                balance_zat: 150_000_000,
                total_received_zat: Some(250_000_000),
                total_sent_zat: Some(100_000_000),
                distinct_transaction_count: Some(7),
                first_seen_unix_seconds: Some(1_700_000_000),
                last_seen_unix_seconds: Some(1_700_000_075),
            }],
            positive_address_count: 1,
            total_positive_balance_zat: 150_000_000,
            top_10_balance_zat: 150_000_000,
            top_100_balance_zat: 150_000_000,
            coverage: Some(explorer::TransparentAddressRankingCoverage {
                balance_complete_through_height: 199,
                history_complete_from_height: Some(100),
                history_complete_through_height: Some(199),
                lifetime_statistics_complete: false,
            }),
            script_type_summaries: Vec::new(),
        };

        let response = rich_list_json(Network::ZcashTestnet, 100, 0, &ranking)?;

        assert_eq!(response["degraded"], json!(true));
        for field in [
            "totalReceived",
            "totalSent",
            "txCount",
            "firstSeen",
            "lastSeen",
        ] {
            assert_eq!(response["addresses"][0][field], Value::Null);
        }
        assert!(response["unavailable"].as_array().is_some_and(|items| {
            items.iter().any(|item| {
                item.as_str()
                    .is_some_and(|reason| reason.contains("visible chain tip"))
            }) && items.iter().any(|item| {
                item.as_str()
                    .is_some_and(|reason| reason.contains("history coverage is incomplete"))
            })
        }));
        Ok(())
    }

    #[test]
    fn rich_list_rejects_nonstandard_native_scripts() {
        let ranking = TransparentAddressRankingResponse {
            freshness: Some(rich_list_freshness(200)),
            entries: vec![explorer::TransparentAddressRankingEntry {
                rank: 1,
                script_pub_key: vec![0x51],
                balance_zat: 1,
                ..Default::default()
            }],
            coverage: Some(explorer::TransparentAddressRankingCoverage {
                balance_complete_through_height: 200,
                history_complete_from_height: Some(0),
                history_complete_through_height: Some(200),
                lifetime_statistics_complete: true,
            }),
            ..Default::default()
        };

        assert!(matches!(
            rich_list_json(Network::ZcashTestnet, 100, 0, &ranking),
            Err(CipherscanRestError::InvalidUpstreamField(
                "transparent_address_ranking.entries.script_pub_key"
            ))
        ));
    }

    fn rich_list_freshness(visible_tip_height: u32) -> explorer::ExplorerFreshness {
        explorer::ExplorerFreshness {
            chain_view: Some(wallet::ChainView {
                chain_epoch: Some(wallet::ChainEpoch {
                    visible_tip: Some(wallet::BlockTip {
                        height: visible_tip_height,
                        hash: SAMPLE_BLOCK_HASH.to_owned(),
                    }),
                    settled_tip: Some(wallet::BlockTip {
                        height: visible_tip_height,
                        hash: SAMPLE_BLOCK_HASH.to_owned(),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn complete_rich_list_ranking() -> TransparentAddressRankingResponse {
        let mut public_key_hash_script = vec![0x76, 0xa9, 0x14];
        public_key_hash_script.extend_from_slice(&[0x42; 20]);
        public_key_hash_script.extend_from_slice(&[0x88, 0xac]);
        let mut script_hash_script = vec![0xa9, 0x14];
        script_hash_script.extend_from_slice(&[0x24; 20]);
        script_hash_script.push(0x87);

        TransparentAddressRankingResponse {
            freshness: Some(rich_list_freshness(200)),
            entries: vec![
                explorer::TransparentAddressRankingEntry {
                    rank: 101,
                    script_pub_key: public_key_hash_script,
                    balance_zat: 150_000_000,
                    total_received_zat: Some(250_000_000),
                    total_sent_zat: Some(100_000_000),
                    distinct_transaction_count: Some(7),
                    first_seen_unix_seconds: Some(1_700_000_000),
                    last_seen_unix_seconds: Some(1_700_000_075),
                },
                explorer::TransparentAddressRankingEntry {
                    rank: 102,
                    script_pub_key: script_hash_script,
                    balance_zat: 50_000_000,
                    total_received_zat: Some(75_000_000),
                    total_sent_zat: Some(25_000_000),
                    distinct_transaction_count: Some(2),
                    first_seen_unix_seconds: Some(1_700_000_150),
                    last_seen_unix_seconds: Some(1_700_000_225),
                },
            ],
            positive_address_count: 250,
            total_positive_balance_zat: 8_000_000_000_000_000,
            top_10_balance_zat: 2_500_000_000_000_000,
            top_100_balance_zat: 5_000_000_000_000_000,
            coverage: Some(explorer::TransparentAddressRankingCoverage {
                balance_complete_through_height: 200,
                history_complete_from_height: Some(0),
                history_complete_through_height: Some(200),
                lifetime_statistics_complete: true,
            }),
            script_type_summaries: vec![
                explorer::TransparentAddressScriptTypeSummary {
                    script_type: explorer::TransparentScriptType::P2pkh as i32,
                    positive_address_count: 200,
                    total_positive_balance_zat: 6_000_000_000_000_000,
                },
                explorer::TransparentAddressScriptTypeSummary {
                    script_type: explorer::TransparentScriptType::P2sh as i32,
                    positive_address_count: 50,
                    total_positive_balance_zat: 2_000_000_000_000_000,
                },
            ],
        }
    }

    #[test]
    fn transparent_supply_breakdown_uses_native_script_and_value_pool_totals()
    -> Result<(), CipherscanRestError> {
        let ranking = complete_rich_list_ranking();
        let value_pools = explorer::ValuePoolSummaryResponse {
            freshness: None,
            pools: vec![wallet::ChainValuePool {
                id: String::from("transparent"),
                monitored: true,
                chain_value_zat: Some(8_500_000_000_000_000),
            }],
            source_tip: Some(value_pool_test_source_tip(200)),
        };

        let response =
            transparent_supply_breakdown_json(Network::ZcashTestnet, &ranking, &value_pools)?;

        assert_eq!(response["success"], json!(true));
        assert_eq!(response["transparentTotal"], json!(85_000_000.0));
        assert_eq!(response["indexedStandardTotal"], json!(80_000_000.0));
        assert_eq!(response["unattributedTransparent"], json!(5_000_000.0));
        assert_eq!(response["addressTypes"][0]["type"], json!("P2PKH"));
        assert_eq!(response["addressTypes"][0]["addressCount"], json!(200));
        assert_eq!(
            response["addressTypes"][0]["totalBalance"],
            json!(60_000_000.0)
        );
        assert_eq!(response["addressTypes"][1]["type"], json!("P2SH"));
        assert_eq!(response["addressTypes"][1]["addressCount"], json!(50));
        assert_eq!(
            response["addressTypes"][1]["totalBalance"],
            json!(20_000_000.0)
        );
        assert_eq!(response["categories"][0]["category"], json!("unlabeled"));
        assert_eq!(response["categories"][0]["addressCount"], json!(250));
        assert_eq!(response["coverage"]["balanceComplete"], json!(true));
        assert_eq!(response["coverage"]["rankingHeight"], json!(200));
        assert_eq!(response["coverage"]["valuePoolHeight"], json!(200));
        assert_eq!(response["degraded"], json!(true));
        assert!(response["unavailable"].as_array().is_some_and(|reasons| {
            reasons.iter().any(|reason| {
                reason
                    .as_str()
                    .is_some_and(|reason| reason.contains("label sidecar"))
            })
        }));
        Ok(())
    }

    #[test]
    fn transparent_supply_breakdown_rejects_inconsistent_script_totals() {
        let mut ranking = complete_rich_list_ranking();
        ranking.script_type_summaries[0].positive_address_count = 199;
        let value_pools = explorer::ValuePoolSummaryResponse::default();

        assert!(matches!(
            transparent_supply_breakdown_json(Network::ZcashTestnet, &ranking, &value_pools),
            Err(CipherscanRestError::InvalidUpstreamField(
                "transparent_address_ranking.script_type_summaries"
            ))
        ));
    }

    #[test]
    fn transparent_supply_breakdown_rejects_classified_value_above_pool_total() {
        let ranking = complete_rich_list_ranking();
        let value_pools = explorer::ValuePoolSummaryResponse {
            freshness: None,
            pools: vec![wallet::ChainValuePool {
                id: String::from("transparent"),
                monitored: true,
                chain_value_zat: Some(7_999_999_999_999_999),
            }],
            source_tip: Some(value_pool_test_source_tip(200)),
        };

        assert!(matches!(
            transparent_supply_breakdown_json(Network::ZcashTestnet, &ranking, &value_pools),
            Err(CipherscanRestError::InvalidUpstreamField(
                "transparent_address_ranking.total_positive_balance_zat"
            ))
        ));
    }

    #[test]
    fn transparent_supply_breakdown_rejects_missing_value_pool_source_tip() {
        let ranking = complete_rich_list_ranking();
        let value_pools = explorer::ValuePoolSummaryResponse::default();

        assert!(matches!(
            transparent_supply_breakdown_json(Network::ZcashTestnet, &ranking, &value_pools),
            Err(CipherscanRestError::MissingUpstreamField(
                "value_pool_summary.source_tip"
            ))
        ));
    }

    #[test]
    fn transparent_supply_breakdown_rejects_absent_transparent_pool_value() {
        let ranking = complete_rich_list_ranking();
        let value_pools = explorer::ValuePoolSummaryResponse {
            freshness: None,
            pools: vec![wallet::ChainValuePool {
                id: String::from("transparent"),
                monitored: true,
                chain_value_zat: None,
            }],
            source_tip: Some(value_pool_test_source_tip(200)),
        };

        assert!(matches!(
            transparent_supply_breakdown_json(Network::ZcashTestnet, &ranking, &value_pools),
            Err(CipherscanRestError::MissingUpstreamField(
                "value_pool_summary.pools.transparent.chain_value_zat"
            ))
        ));
    }

    #[test]
    fn verified_value_pool_source_tip_rejects_height_and_hash_mismatches() {
        let mut summary = explorer::ValuePoolSummaryResponse {
            freshness: Some(rich_list_freshness(200)),
            source_tip: Some(value_pool_test_source_tip(201)),
            ..Default::default()
        };
        assert!(matches!(
            verified_value_pool_source_tip(&summary),
            Err(CipherscanRestError::InvalidUpstreamField(
                "value_pool_summary.source_tip"
            ))
        ));

        summary.source_tip = Some(wallet::BlockTip {
            height: 200,
            hash: "ff".repeat(32),
        });
        assert!(matches!(
            verified_value_pool_source_tip(&summary),
            Err(CipherscanRestError::InvalidUpstreamField(
                "value_pool_summary.source_tip"
            ))
        ));

        summary.source_tip = Some(value_pool_test_source_tip(200));
        assert!(verified_value_pool_source_tip(&summary).is_ok_and(|tip| tip.height == 200));
    }

    #[test]
    fn transparent_supply_breakdown_rejects_mismatched_chain_tips() {
        let ranking = complete_rich_list_ranking();
        let value_pools = explorer::ValuePoolSummaryResponse {
            pools: vec![wallet::ChainValuePool {
                id: String::from("transparent"),
                monitored: true,
                chain_value_zat: Some(8_500_000_000_000_000),
            }],
            source_tip: Some(wallet::BlockTip {
                height: 200,
                hash: "ff".repeat(32),
            }),
            ..Default::default()
        };

        assert!(matches!(
            transparent_supply_breakdown_json(Network::ZcashTestnet, &ranking, &value_pools),
            Err(CipherscanRestError::InvalidUpstreamField(
                "value_pool_summary.source_tip"
            ))
        ));
    }

    #[test]
    fn block_header_tip_validation_rejects_same_height_reorg_hash() {
        let expected_tip = value_pool_test_source_tip(200);
        let matching_header = wallet::BlockHeader {
            block_id: Some(wallet::BlockId {
                height: 200,
                block_hash: expected_tip.hash.clone(),
            }),
            ..Default::default()
        };
        assert!(validate_block_header_tip(&matching_header, &expected_tip).is_ok());

        let reorged_header = wallet::BlockHeader {
            block_id: Some(wallet::BlockId {
                height: 200,
                block_hash: "ff".repeat(32),
            }),
            ..Default::default()
        };
        assert!(matches!(
            validate_block_header_tip(&reorged_header, &expected_tip),
            Err(CipherscanRestError::InvalidUpstreamField(
                "block_header.block_id"
            ))
        ));
    }

    #[test]
    fn mining_metrics_rolling_windows_match_cipherscan_interval_rules() {
        let samples = [
            MiningBlockSample {
                block_height: 1,
                block_time_unix_seconds: 1_000,
                difficulty: 10.0,
                transaction_fees_zec: 1.0,
                transaction_count: 2.0,
            },
            MiningBlockSample {
                block_height: 2,
                block_time_unix_seconds: 1_100,
                difficulty: 20.0,
                transaction_fees_zec: 3.0,
                transaction_count: 4.0,
            },
            MiningBlockSample {
                block_height: 3,
                block_time_unix_seconds: 1_700,
                difficulty: 30.0,
                transaction_fees_zec: 5.0,
                transaction_count: 6.0,
            },
        ];

        let points = rolling_mining_metric_points(&samples, 2);

        assert_eq!(points.len(), 3);
        assert_f64_close(points[0].values.block_time_seconds, 75.0);
        assert_f64_close(points[0].values.solrate, 10.0 / 75.0);
        assert_f64_close(points[1].values.difficulty, 15.0);
        assert_f64_close(points[1].values.block_time_seconds, 87.5);
        assert_f64_close(
            points[1].values.solrate,
            f64::midpoint(10.0 / 75.0, 20.0 / 100.0),
        );
        assert_f64_close(points[2].values.difficulty, 25.0);
        assert_f64_close(points[2].values.block_time_seconds, 87.5);
        assert_f64_close(points[2].values.solrate, 0.3);
        assert_f64_close(points[2].values.transaction_fees_zec, 4.0);
        assert_f64_close(points[2].values.transaction_count, 5.0);
    }

    #[test]
    fn mining_pool_distribution_groups_known_addresses_and_keeps_exact_fees()
    -> Result<(), Box<dyn std::error::Error>> {
        let generated_at = OffsetDateTime::from_unix_timestamp(1_735_862_400)?;
        let window = mining_pool_distribution_test_window(generated_at);
        let distribution = mining_pool_distribution_json("bad", &window)?;

        assert_eq!(distribution["period"], json!("bad"));
        assert_eq!(distribution["totalBlocks"], json!(3));
        assert_eq!(distribution["pools"][0]["name"], json!("AntPool"));
        assert_eq!(distribution["pools"][0]["blocks"], json!(2));
        assert_f64_close(
            distribution["pools"][0]["share"]
                .as_f64()
                .unwrap_or_default(),
            2.0 / 3.0,
        );
        assert_eq!(distribution["pools"][0]["totalFeesZat"], json!("17"));
        assert_eq!(distribution["pools"][1]["name"], json!("Unknown"));
        assert_eq!(distribution["pools"][1]["blocks"], json!(1));
        assert_eq!(distribution["pools"][1]["totalFeesZat"], json!("3"));
        assert_eq!(distribution["generatedAt"], json!("2025-01-03T00:00:00Z"));
        assert!(distribution.get("degraded").is_none());
        assert!(distribution.get("unavailable").is_none());
        Ok(())
    }

    fn mining_pool_distribution_test_window(
        generated_at: OffsetDateTime,
    ) -> MiningPoolDistributionWindow {
        MiningPoolDistributionWindow {
            payout_by_address: BTreeMap::from([
                (
                    Some("t1ZVi2YGk98tEGYcNpXYnJFWCoLG2oYwv3J".to_owned()),
                    MiningPayoutAggregate {
                        block_count: 1,
                        total_fees_zat: 7,
                    },
                ),
                (
                    Some("t1L2b66MXbgpVMXDfUa94GCBFAN4dCxGohM".to_owned()),
                    MiningPayoutAggregate {
                        block_count: 1,
                        total_fees_zat: 10,
                    },
                ),
                (
                    Some("tmDominant".to_owned()),
                    MiningPayoutAggregate {
                        block_count: 1,
                        total_fees_zat: 3,
                    },
                ),
            ]),
            generated_at,
        }
    }

    #[test]
    fn mining_pool_ranking_json_preserves_cipherscan_table_shape() {
        let ranking = mining_pool_ranking_json("bad");

        assert_eq!(ranking["period"], json!("bad"));
        assert_eq!(ranking["totalBlocks"], json!(0));
        assert_eq!(ranking["ranking"], json!([]));
        assert!(
            ranking["generatedAt"]
                .as_str()
                .is_some_and(|at| at.contains('T') && at.ends_with('Z'))
        );
        assert_eq!(ranking["degraded"], json!(true));
        assert!(
            ranking["unavailable"]
                .as_array()
                .is_some_and(|fields| !fields.is_empty())
        );
    }

    #[test]
    fn mining_hashrate_share_json_preserves_cipherscan_series_shape() {
        let share = mining_hashrate_share_json("bad");

        assert_eq!(share["period"], json!("bad"));
        assert_eq!(share["series"], json!([]));
        assert_eq!(share["allPools"], json!([]));
        assert!(
            share["generatedAt"]
                .as_str()
                .is_some_and(|at| at.contains('T') && at.ends_with('Z'))
        );
        assert_eq!(share["degraded"], json!(true));
        assert!(
            share["unavailable"]
                .as_array()
                .is_some_and(|fields| !fields.is_empty())
        );
    }

    #[test]
    fn miner_behavior_json_preserves_cipherscan_computing_shape() {
        let behavior = miner_behavior_json("bad");

        assert_eq!(behavior["period"], json!("bad"));
        assert_eq!(behavior["series"], json!([]));
        assert_eq!(behavior["summary"], Value::Null);
        assert_eq!(
            behavior["message"],
            json!("Miner behavior data is being computed. Check back soon.")
        );
        assert!(
            behavior["generatedAt"]
                .as_str()
                .is_some_and(|at| at.contains('T') && at.ends_with('Z'))
        );
        assert_eq!(behavior["degraded"], json!(true));
        assert!(
            behavior["unavailable"]
                .as_array()
                .is_some_and(|fields| !fields.is_empty())
        );
    }

    #[test]
    fn zodl_leaderboard_json_preserves_cipherscan_computing_shape() {
        let leaderboard = zodl_leaderboard_json("bad");

        assert_eq!(leaderboard["period"], json!("bad"));
        assert_eq!(leaderboard["pools"], json!([]));
        assert_eq!(leaderboard["summary"], Value::Null);
        assert_eq!(
            leaderboard["message"],
            json!("Miner behavior data is being computed. Check back soon.")
        );
        assert!(
            leaderboard["generatedAt"]
                .as_str()
                .is_some_and(|at| at.contains('T') && at.ends_with('Z'))
        );
        assert_eq!(leaderboard["degraded"], json!(true));
        assert!(
            leaderboard["unavailable"]
                .as_array()
                .is_some_and(|fields| !fields.is_empty())
        );
    }

    #[test]
    fn mining_rewards_json_preserves_legacy_series_and_declares_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let generated_at = OffsetDateTime::from_unix_timestamp(1_735_862_400)?;
        let window = MiningProductionWindow {
            entries: vec![
                CipherscanBlockListEntry {
                    summary: explorer::BlockSummary {
                        block_height: 1,
                        block_time_unix_seconds: 1_735_689_600,
                        fees_collected_zat: 10,
                        paid_fees_collected_zat: Some(7),
                        coinbase_reward_zat: 100,
                        ..Default::default()
                    },
                    difficulty: 1.0,
                    miner_address: None,
                },
                CipherscanBlockListEntry {
                    summary: explorer::BlockSummary {
                        block_height: 2,
                        block_time_unix_seconds: 1_735_693_200,
                        fees_collected_zat: 20,
                        coinbase_reward_zat: 200,
                        ..Default::default()
                    },
                    difficulty: 1.0,
                    miner_address: None,
                },
                CipherscanBlockListEntry {
                    summary: explorer::BlockSummary {
                        block_height: 3,
                        block_time_unix_seconds: 1_735_776_000,
                        fees_collected_zat: 30,
                        paid_fees_collected_zat: Some(29),
                        coinbase_reward_zat: 300,
                        ..Default::default()
                    },
                    difficulty: 1.0,
                    miner_address: None,
                },
            ],
            requested_cutoff_unix_seconds: Some(1_735_689_600),
            covered_from_unix_seconds: Some(1_735_600_000),
            covered_through_unix_seconds: Some(1_735_776_000),
            scanned_block_count: 4,
            coverage_complete: true,
            generated_at,
        };
        let rewards = mining_rewards_json("bad", &window);

        assert_eq!(rewards["period"], json!("bad"));
        assert_eq!(rewards["series"][0]["date"], json!("2025-01-01"));
        assert_eq!(rewards["series"][0]["blocks"], json!(2));
        assert_eq!(rewards["series"][0]["totalFeesZat"], json!("27"));
        assert_eq!(rewards["series"][0]["totalCoinbaseZat"], json!("300"));
        assert_eq!(rewards["series"][1]["date"], json!("2025-01-02"));
        assert_eq!(rewards["series"][1]["blocks"], json!(1));
        assert_eq!(rewards["series"][1]["totalFeesZat"], json!("29"));
        assert_eq!(rewards["series"][1]["totalCoinbaseZat"], json!("300"));
        assert_eq!(rewards["generatedAt"], json!("2025-01-03T00:00:00Z"));
        assert_eq!(rewards["coinbaseBasis"], json!("transparent_outputs"));
        assert_eq!(
            rewards["feeBasis"],
            json!("mixed_paid_and_zip317_conventional")
        );
        assert_eq!(
            rewards["coverage"]["requestedCutoff"],
            json!("2025-01-01T00:00:00Z")
        );
        assert_eq!(rewards["coverage"]["scannedBlocks"], json!(4));
        assert_eq!(rewards["coverage"]["includedBlocks"], json!(3));
        assert_eq!(rewards["coverage"]["paidFeeBlocks"], json!(2));
        assert_eq!(rewards["coverage"]["conventionalFeeBlocks"], json!(1));
        assert_eq!(rewards["coverage"]["complete"], json!(true));
        assert_eq!(rewards["degraded"], json!(true));
        assert!(
            rewards["unavailable"]
                .as_array()
                .is_some_and(|fields| !fields.is_empty())
        );

        Ok(())
    }

    #[test]
    fn mining_production_cutoff_matches_cipherscan_period_rules() {
        let generated_at = 1_735_862_400;

        assert_eq!(
            mining_production_period("24h").cutoff_unix_seconds(generated_at),
            Some(generated_at - 86_400)
        );
        assert_eq!(
            mining_production_period("7d").cutoff_unix_seconds(generated_at),
            Some(generated_at - 7 * 86_400)
        );
        assert_eq!(
            mining_production_period("bad").cutoff_unix_seconds(generated_at),
            Some(generated_at - 7 * 86_400)
        );
        assert_eq!(
            mining_production_period("all").cutoff_unix_seconds(generated_at),
            Some(generated_at - 7 * UNIX_SECONDS_PER_DAY)
        );
    }

    #[test]
    fn mining_production_page_requires_contiguous_height_coverage() {
        let complete_page = vec![
            CipherscanBlockListEntry {
                summary: explorer::BlockSummary {
                    block_height: 10,
                    ..Default::default()
                },
                difficulty: 1.0,
                miner_address: None,
            },
            CipherscanBlockListEntry {
                summary: explorer::BlockSummary {
                    block_height: 11,
                    ..Default::default()
                },
                difficulty: 1.0,
                miner_address: None,
            },
        ];
        let missing_height_page = vec![CipherscanBlockListEntry {
            summary: explorer::BlockSummary {
                block_height: 11,
                ..Default::default()
            },
            difficulty: 1.0,
            miner_address: None,
        }];

        assert!(validate_mining_production_page(&complete_page, 10, 11).is_ok());
        assert!(validate_mining_production_page(&missing_height_page, 10, 11).is_err());
    }

    #[test]
    fn mining_production_cutoff_is_inclusive_and_requires_a_full_prior_page_to_stop() {
        let cutoff = 1_000;
        let before = CipherscanBlockListEntry {
            summary: explorer::BlockSummary {
                block_time_unix_seconds: cutoff - 1,
                ..Default::default()
            },
            difficulty: 1.0,
            miner_address: None,
        };
        let at_cutoff = CipherscanBlockListEntry {
            summary: explorer::BlockSummary {
                block_time_unix_seconds: cutoff,
                ..Default::default()
            },
            difficulty: 1.0,
            miner_address: None,
        };
        let at_end = CipherscanBlockListEntry {
            summary: explorer::BlockSummary {
                block_time_unix_seconds: cutoff + 100,
                ..Default::default()
            },
            difficulty: 1.0,
            miner_address: None,
        };

        assert!(!mining_production_entry_is_in_period(
            &before,
            Some(cutoff),
            cutoff + 100
        ));
        assert!(mining_production_entry_is_in_period(
            &at_cutoff,
            Some(cutoff),
            cutoff + 100
        ));
        assert!(!mining_production_entry_is_in_period(
            &at_end,
            Some(cutoff),
            cutoff + 100
        ));
        assert!(mining_production_page_is_before_cutoff(
            std::slice::from_ref(&before),
            Some(cutoff)
        ));
        assert!(!mining_production_page_is_before_cutoff(
            &[before, at_cutoff],
            Some(cutoff)
        ));
        assert!(!mining_production_page_is_before_cutoff(&[], None));
    }

    #[test]
    fn pool_flows_json_maps_native_buckets_without_fabricating_points()
    -> Result<(), CipherscanRestError> {
        let request = CipherscanPoolFlowRequest {
            period: "7d",
            days: 7,
            pool: "orchard",
            pools: vec![ValuePoolFlowPool::Orchard as i32],
            resolution: CipherscanFlowResolution::Hourly,
            amount_format: CipherscanFlowAmountFormat::Zatoshi,
        };
        let coverage = explorer::ValuePoolFlowCoverage {
            requested_range_complete: true,
            ..Default::default()
        };
        let summary = explorer::ValuePoolFlowSummaryResponse {
            buckets: vec![explorer::ValuePoolFlowSummaryBucket {
                bucket_start_time_unix_seconds: 1_735_689_600,
                shield_event_count: 12,
                deshield_event_count: 8,
                shield_amount_zat: 14_250_000_000,
                deshield_amount_zat: 8_930_000_000,
            }],
            ..Default::default()
        };
        let flows = pool_flows_json(&request, &summary, &coverage)?;

        assert_eq!(flows["success"], json!(true));
        assert_eq!(flows["period"], json!("7d"));
        assert_eq!(flows["pool"], json!("orchard"));
        assert_eq!(flows["granularity"], json!("hourly"));
        assert_eq!(flows["format"], json!("zatoshi"));
        assert_eq!(
            flows["points"][0]["date"],
            json!("2025-01-01T00:00:00.000Z")
        );
        assert_eq!(flows["points"][0]["shield"], json!("14250000000"));
        assert_eq!(flows["points"][0]["deshield"], json!("8930000000"));
        assert_eq!(flows["points"][0]["net"], json!("5320000000"));
        assert_eq!(flows["points"][0]["shieldTx"], json!(12));
        assert_eq!(flows["points"][0]["deshieldTx"], json!(8));
        assert_eq!(flows["degraded"], json!(false));
        assert!(flows["unavailable"].as_array().is_some_and(Vec::is_empty));
        Ok(())
    }

    #[test]
    fn shielded_flow_filter_reaches_native_before_paging() -> Result<(), CipherscanRestError> {
        let query = ShieldedFlowQuery {
            flow_type: Some("shield".to_owned()),
            pool: Some("orchard".to_owned()),
            min_zec: Some(2.5),
            ..Default::default()
        };
        let filter = shielded_flow_filter(&query)?;

        assert_eq!(
            filter.directions,
            vec![ValuePoolFlowDirection::Shield as i32]
        );
        assert_eq!(filter.pools, vec![ValuePoolFlowPool::Orchard as i32]);
        assert_eq!(filter.minimum_amount_zat, 250_000_000);
        Ok(())
    }

    #[test]
    fn shielded_flow_cursor_translates_legacy_time_and_stable_id() -> Result<(), CipherscanRestError>
    {
        let query = ShieldedFlowQuery {
            cursor: Some(1_735_689_600),
            cursor_id: Some(4_200_000_000_012),
            direction: Some("prev".to_owned()),
            ..ShieldedFlowQuery::default()
        };

        assert_eq!(
            shielded_flow_anchor(&query)?,
            Some(CipherscanFlowCursor {
                block_time_unix_seconds: 1_735_689_600,
                coordinate: CipherscanFlowCoordinate {
                    block_height: 4_200_000,
                    transaction_index: 12,
                },
            })
        );
        assert_eq!(
            shielded_flow_page_direction(query.direction.as_deref()),
            ShieldedFlowPageDirection::Newer
        );
        assert!(
            shielded_flow_anchor(&ShieldedFlowQuery {
                cursor: Some(1_735_689_600),
                ..ShieldedFlowQuery::default()
            })
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn shielded_flow_cursor_orders_by_time_before_canonical_coordinate() {
        let newer_time_lower_height = CipherscanFlowCursor {
            block_time_unix_seconds: 200,
            coordinate: CipherscanFlowCoordinate {
                block_height: 10,
                transaction_index: 0,
            },
        };
        let older_time_higher_height = CipherscanFlowCursor {
            block_time_unix_seconds: 199,
            coordinate: CipherscanFlowCoordinate {
                block_height: 11,
                transaction_index: 0,
            },
        };
        let same_time_later_transaction = CipherscanFlowCursor {
            block_time_unix_seconds: 200,
            coordinate: CipherscanFlowCoordinate {
                block_height: 10,
                transaction_index: 1,
            },
        };

        assert!(newer_time_lower_height > older_time_higher_height);
        assert!(same_time_later_transaction > newer_time_lower_height);
    }

    #[test]
    fn shielded_flow_row_uses_stable_numeric_event_coordinates() -> Result<(), CipherscanRestError>
    {
        let event = explorer::ValuePoolFlowEvent {
            transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
            block_height: 4_200_000,
            block_time_unix_seconds: 1_735_689_600,
            transaction_index_in_block: 12,
            direction: ValuePoolFlowDirection::Shield as i32,
            pool: ValuePoolFlowPool::Orchard as i32,
            amount_zat: 250_000_000,
            pool_balances: None,
        };
        let row = shielded_flow_row(&event)?;

        assert_eq!(row["id"], json!(4_200_000_000_012_u64));
        assert_eq!(row["txid"], json!(SAMPLE_TRANSACTION_ID));
        assert_eq!(row["blockHeight"], json!(4_200_000));
        assert_eq!(row["blockTime"], json!(1_735_689_600));
        assert_eq!(row["flowType"], json!("shield"));
        assert_eq!(row["amountZec"], json!(2.5));
        assert_eq!(row["pool"], json!("orchard"));
        assert_eq!(row["addresses"], json!([]));
        assert!(
            row["zinderUnavailable"]
                .as_array()
                .is_some_and(|fields| !fields.is_empty())
        );
        Ok(())
    }

    #[test]
    fn malformed_native_flow_event_fails_closed() {
        let event = explorer::ValuePoolFlowEvent {
            transaction_id: "not-a-canonical-transaction-id".to_owned(),
            block_time_unix_seconds: i64::MAX,
            direction: ValuePoolFlowDirection::Unspecified as i32,
            ..Default::default()
        };

        assert!(shielded_flow_row(&event).is_err());
    }

    #[test]
    fn pool_flow_request_normalizes_period_granularity_and_format()
    -> Result<(), CipherscanRestError> {
        let query = PageQuery {
            period: Some("all".to_owned()),
            granularity: Some("hourly".to_owned()),
            format: Some("zatoshi".to_owned()),
            pool: Some("sapling".to_owned()),
            ..PageQuery::default()
        };
        let request = CipherscanPoolFlowRequest::from_query(&query)?;

        assert_eq!(request.period, "30d");
        assert_eq!(request.days, 30);
        assert_eq!(request.resolution, CipherscanFlowResolution::Hourly);
        assert_eq!(request.amount_format, CipherscanFlowAmountFormat::Zatoshi);
        assert_eq!(request.pools, vec![ValuePoolFlowPool::Sapling as i32]);
        Ok(())
    }

    #[test]
    fn pool_flow_daily_range_includes_full_utc_cutoff_day() -> Result<(), CipherscanRestError> {
        let mut request = CipherscanPoolFlowRequest {
            period: "7d",
            days: 7,
            pool: "all",
            pools: Vec::new(),
            resolution: CipherscanFlowResolution::Daily,
            amount_format: CipherscanFlowAmountFormat::Zec,
        };
        let end_time_unix_seconds = 10 * UNIX_SECONDS_PER_DAY + 3_600;

        assert_eq!(
            cipherscan_flow_start_time(end_time_unix_seconds, &request)?,
            3 * UNIX_SECONDS_PER_DAY
        );
        request.resolution = CipherscanFlowResolution::Hourly;
        assert_eq!(
            cipherscan_flow_start_time(end_time_unix_seconds, &request)?,
            3 * UNIX_SECONDS_PER_DAY + 3_600
        );
        Ok(())
    }

    #[test]
    fn malformed_native_flow_bucket_fails_closed() {
        let request = CipherscanPoolFlowRequest {
            period: "30d",
            days: 30,
            pool: "all",
            pools: Vec::new(),
            resolution: CipherscanFlowResolution::Daily,
            amount_format: CipherscanFlowAmountFormat::Zec,
        };
        let bucket = explorer::ValuePoolFlowSummaryBucket {
            bucket_start_time_unix_seconds: 1,
            ..Default::default()
        };

        assert!(value_pool_flow_summary_point_json(&bucket, &request).is_err());
    }

    #[test]
    fn history_chart_json_preserves_public_metadata_shape() -> Result<(), CipherscanRestError> {
        let pool_history = value_pool_history_json(
            "30d",
            "zatoshi",
            &value_pool_history_test_response(10, false, Vec::new()),
        )?;
        let chain_size = chain_size_history_json("1y");
        let fees = conventional_fee_distribution_json(
            "90d",
            &explorer::ConventionalFeeDistributionResponse::default(),
            OffsetDateTime::UNIX_EPOCH,
        )?;

        assert_eq!(pool_history["success"], json!(true));
        assert_eq!(pool_history["period"], json!("30d"));
        assert_eq!(pool_history["format"], json!("zatoshi"));
        assert_eq!(pool_history["points"], json!([]));
        assert_eq!(pool_history["hasPoolBreakdown"], json!(false));
        assert_eq!(pool_history["hasVerifiedPerPoolBreakdown"], json!(false));
        assert_eq!(pool_history["degraded"], json!(true));

        assert_eq!(chain_size["success"], json!(true));
        assert_eq!(chain_size["available"], json!(false));
        assert_eq!(chain_size["period"], json!("1y"));
        assert_eq!(chain_size["points"], json!([]));
        assert_eq!(chain_size["degraded"], json!(true));

        assert_eq!(fees["success"], Value::Null);
        assert_eq!(fees["period"], json!("90d"));
        assert_eq!(fees["daily"], json!([]));
        assert!(
            fees["updatedAt"]
                .as_str()
                .is_some_and(|at| at.contains('T') && at.ends_with('Z'))
        );
        assert_eq!(fees["degraded"], json!(true));
        Ok(())
    }

    #[test]
    fn pool_history_remains_available_during_live_tail_lag() -> Result<(), CipherscanRestError> {
        let mut history = value_pool_history_test_response(10, true, Vec::new());
        if let Some(coverage) = history.coverage.as_mut() {
            coverage.historical_through_height = Some(9);
            coverage.live_tail_from_height = Some(10);
            coverage.live_tail_through_height = Some(10);
            coverage.complete_through_visible_tip = false;
        }

        let response = value_pool_history_json("7d", "zec", &history)?;

        assert_eq!(response["success"], json!(true));
        assert!(response.get("degraded").is_none());
        assert_eq!(response["points"], json!([]));
        Ok(())
    }

    #[test]
    fn pool_overview_json_uses_current_value_pool_balances() -> Result<(), CipherscanRestError> {
        let summary = explorer::ValuePoolSummaryResponse {
            freshness: Some(rich_list_freshness(10)),
            pools: vec![
                wallet::ChainValuePool {
                    id: String::from("transparent"),
                    monitored: true,
                    chain_value_zat: Some(1_000),
                },
                wallet::ChainValuePool {
                    id: String::from("sprout"),
                    monitored: true,
                    chain_value_zat: Some(20),
                },
                wallet::ChainValuePool {
                    id: String::from("sapling"),
                    monitored: true,
                    chain_value_zat: Some(30),
                },
                wallet::ChainValuePool {
                    id: String::from("orchard"),
                    monitored: true,
                    chain_value_zat: Some(40),
                },
                wallet::ChainValuePool {
                    id: String::from("ironwood"),
                    monitored: true,
                    chain_value_zat: Some(50),
                },
                wallet::ChainValuePool {
                    id: String::from("lockbox"),
                    monitored: true,
                    chain_value_zat: Some(60),
                },
            ],
            source_tip: Some(value_pool_test_source_tip(10)),
        };

        let overview = pool_overview_json(
            &summary,
            &value_pool_history_test_response(10, false, Vec::new()),
        )?;

        assert_eq!(overview["success"], json!(true));
        assert_eq!(overview["current"]["transparent"], json!(1_000));
        assert_eq!(overview["current"]["sprout"], json!(20));
        assert_eq!(overview["current"]["sapling"], json!(30));
        assert_eq!(overview["current"]["orchard"], json!(40));
        assert_eq!(overview["current"]["ironwood"], json!(50));
        assert_eq!(overview["current"]["shielded"], json!(140));
        assert_eq!(overview["current"]["chainSupply"], json!(1_200));
        assert_eq!(overview["deltas"]["sapling"]["24h"], Value::Null);
        assert_eq!(overview["degraded"], json!(true));

        let network_supply = network_supply_json("zcash-testnet", "NU6.3", &summary.pools)?;
        assert_eq!(network_supply["chainSupply"], json!(0.000_012));
        assert_eq!(network_supply["transparent"], json!(0.000_01));
        assert_eq!(network_supply["sprout"], json!(0.000_000_2));
        assert_eq!(network_supply["sapling"], json!(0.000_000_3));
        assert_eq!(network_supply["orchard"], json!(0.000_000_4));
        assert_eq!(network_supply["ironwood"], json!(0.000_000_5));
        assert_eq!(network_supply["lockbox"], json!(0.000_000_6));
        assert_eq!(network_supply["totalShielded"], json!(0.000_001_4));
        assert_eq!(network_supply["activeUpgrade"], json!("NU6.3"));
        assert_eq!(network_supply["chain"], json!("zcash-testnet"));
        assert_eq!(network_supply["degraded"], json!(false));
        assert_eq!(network_supply["unavailable"], json!([]));
        assert!(
            network_supply["shieldedPercentage"]
                .as_f64()
                .is_some_and(
                    |percentage| (percentage - 11.666_666_666_666_666).abs() < f64::EPSILON
                )
        );
        Ok(())
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one fixture proves both history formatting and calendar delta semantics"
    )]
    fn pool_history_and_overview_use_exact_calendar_snapshots() -> Result<(), CipherscanRestError> {
        let current_day = calendar_date_start_unix_seconds(OffsetDateTime::now_utc().date());
        let history = value_pool_history_test_response(
            10,
            true,
            [0_i64, 1, 7, 30]
                .into_iter()
                .map(|days| explorer::ValuePoolBalanceHistoryPoint {
                    day_start_unix_seconds: current_day - days * 86_400,
                    block_height: 10_u32.saturating_sub(u32::try_from(days).unwrap_or_default()),
                    block_hash: SAMPLE_BLOCK_HASH.to_owned(),
                    block_time_unix_seconds: current_day - days * 86_400 + 1,
                    pools: vec![
                        explorer::ValuePoolBalance {
                            id: "transparent".to_owned(),
                            monitored: true,
                            value_zat: Some(1_000),
                        },
                        explorer::ValuePoolBalance {
                            id: "sprout".to_owned(),
                            monitored: true,
                            value_zat: Some(20),
                        },
                        explorer::ValuePoolBalance {
                            id: "sapling".to_owned(),
                            monitored: true,
                            value_zat: Some(u64::try_from(100 - days).unwrap_or_default()),
                        },
                        explorer::ValuePoolBalance {
                            id: "orchard".to_owned(),
                            monitored: true,
                            value_zat: Some(40),
                        },
                        explorer::ValuePoolBalance {
                            id: "ironwood".to_owned(),
                            monitored: true,
                            value_zat: Some(50),
                        },
                        explorer::ValuePoolBalance {
                            id: "lockbox".to_owned(),
                            monitored: true,
                            value_zat: Some(60),
                        },
                    ],
                })
                .collect(),
        );
        let summary = explorer::ValuePoolSummaryResponse {
            freshness: Some(rich_list_freshness(10)),
            pools: vec![
                wallet::ChainValuePool {
                    id: "transparent".to_owned(),
                    monitored: true,
                    chain_value_zat: Some(1_000),
                },
                wallet::ChainValuePool {
                    id: "sprout".to_owned(),
                    monitored: true,
                    chain_value_zat: Some(20),
                },
                wallet::ChainValuePool {
                    id: "sapling".to_owned(),
                    monitored: true,
                    chain_value_zat: Some(100),
                },
                wallet::ChainValuePool {
                    id: "orchard".to_owned(),
                    monitored: true,
                    chain_value_zat: Some(40),
                },
                wallet::ChainValuePool {
                    id: "ironwood".to_owned(),
                    monitored: true,
                    chain_value_zat: Some(50),
                },
                wallet::ChainValuePool {
                    id: "lockbox".to_owned(),
                    monitored: true,
                    chain_value_zat: Some(60),
                },
            ],
            source_tip: Some(value_pool_test_source_tip(10)),
        };

        let chart = value_pool_history_json("7d", "zatoshi", &history)?;
        assert_eq!(chart["hasVerifiedPerPoolBreakdown"], json!(true));
        assert_eq!(chart["points"].as_array().map(Vec::len), Some(3));
        assert_eq!(chart["points"][2]["saplingZat"], json!("100"));
        assert_eq!(chart["points"][2]["chainSupplyZat"], json!("1270"));

        let overview = pool_overview_json(&summary, &history)?;
        assert_eq!(overview["deltas"]["sapling"]["24h"], json!(1));
        assert_eq!(overview["deltas"]["sapling"]["7d"], json!(7));
        assert_eq!(overview["deltas"]["sapling"]["30d"], json!(30));
        assert_eq!(overview["deltas"]["shielded"]["7d"], json!(7));
        assert_eq!(overview["degraded"], json!(false));
        Ok(())
    }

    #[test]
    fn emission_history_uses_exact_daily_chain_supply_deltas() -> Result<(), CipherscanRestError> {
        let current_day = calendar_date_start_unix_seconds(OffsetDateTime::now_utc().date());
        let point = |days_ago: i64, block_height: u32, chain_supply_zat: u64| {
            explorer::ValuePoolBalanceHistoryPoint {
                day_start_unix_seconds: current_day - days_ago * UNIX_SECONDS_PER_DAY,
                block_height,
                block_hash: SAMPLE_BLOCK_HASH.to_owned(),
                block_time_unix_seconds: current_day - days_ago * UNIX_SECONDS_PER_DAY + 1,
                pools: vec![explorer::ValuePoolBalance {
                    id: "transparent".to_owned(),
                    monitored: true,
                    value_zat: Some(chain_supply_zat),
                }],
            }
        };
        let history = value_pool_history_test_response(
            10,
            true,
            vec![point(0, 10, 1_300), point(1, 9, 1_200), point(2, 8, 1_000)],
        );
        let subsidy = derive_chain_subsidy_summary(Network::ZcashTestnet, 10)?;
        let supply = chain_supply_summary_from_zats(1_300)?;
        let response = emission_json(
            &subsidy,
            &supply,
            &value_pool_test_source_tip(10),
            &history,
            "7d",
        )?;

        assert_eq!(response["circulating"], json!(0.000_013));
        assert_eq!(response["supplyHistory"].as_array().map(Vec::len), Some(3));
        assert_eq!(response["supplyHistory"][0]["height"], json!(8));
        assert_eq!(response["supplyHistory"][0]["circulating"], json!(0.000_01));
        assert_eq!(response["supplyHistory"][2]["height"], json!(10));
        assert_eq!(response["dailyEmission"].as_array().map(Vec::len), Some(2));
        assert_eq!(response["dailyEmission"][0]["emission"], json!(0.000_002));
        assert_eq!(response["dailyEmission"][1]["emission"], json!(0.000_001));
        assert_eq!(response["hasChainSnapshots"], json!(true));
        assert_eq!(response["supplyHistorySource"], json!("history"));
        assert!(response.get("degraded").is_none());
        Ok(())
    }

    #[test]
    fn emission_history_rejects_epoch_drift_and_withholds_partial_arrays()
    -> Result<(), CipherscanRestError> {
        let subsidy = derive_chain_subsidy_summary(Network::ZcashTestnet, 10)?;
        let supply = chain_supply_summary_from_zats(1_300)?;
        let incomplete = value_pool_history_test_response(10, false, Vec::new());
        let response = emission_json(
            &subsidy,
            &supply,
            &value_pool_test_source_tip(10),
            &incomplete,
            "1y",
        )?;
        assert_eq!(response["supplyHistory"], json!([]));
        assert_eq!(response["dailyEmission"], json!([]));
        assert_eq!(response["hasChainSnapshots"], json!(false));
        assert_eq!(response["degraded"], json!(true));

        assert!(
            emission_json(
                &subsidy,
                &supply,
                &value_pool_test_source_tip(11),
                &incomplete,
                "1y",
            )
            .is_err()
        );
        assert!(chain_supply_summary_from_zats(-1).is_err());
        Ok(())
    }

    #[test]
    fn pool_overview_json_preserves_zero_and_marks_missing_known_values_unavailable()
    -> Result<(), CipherscanRestError> {
        let summary = explorer::ValuePoolSummaryResponse {
            freshness: Some(rich_list_freshness(10)),
            pools: vec![
                wallet::ChainValuePool {
                    id: String::from("transparent"),
                    monitored: true,
                    chain_value_zat: Some(0),
                },
                wallet::ChainValuePool {
                    id: String::from("sprout"),
                    monitored: true,
                    chain_value_zat: None,
                },
                wallet::ChainValuePool {
                    id: String::from("sapling"),
                    monitored: true,
                    chain_value_zat: Some(0),
                },
                wallet::ChainValuePool {
                    id: String::from("orchard"),
                    monitored: true,
                    chain_value_zat: Some(0),
                },
                wallet::ChainValuePool {
                    id: String::from("ironwood"),
                    monitored: true,
                    chain_value_zat: Some(0),
                },
                wallet::ChainValuePool {
                    id: String::from("lockbox"),
                    monitored: true,
                    chain_value_zat: Some(0),
                },
            ],
            source_tip: Some(value_pool_test_source_tip(10)),
        };

        let overview = pool_overview_json(
            &summary,
            &value_pool_history_test_response(10, false, Vec::new()),
        )?;

        assert_eq!(overview["current"]["transparent"], json!(0));
        assert_eq!(overview["current"]["sprout"], Value::Null);
        assert_eq!(overview["current"]["shielded"], Value::Null);
        assert_eq!(overview["current"]["chainSupply"], Value::Null);
        assert!(overview["unavailable"].as_array().is_some_and(|reasons| {
            reasons
                .iter()
                .any(|reason| reason == VALUE_POOL_TOTALS_UNAVAILABLE)
        }));
        assert!(matches!(
            network_supply_json("zcash-testnet", "NU6.3", &summary.pools),
            Err(CipherscanRestError::MissingUpstreamField(
                "value_pool_summary.pools.chain_value_zat"
            ))
        ));
        Ok(())
    }

    #[test]
    fn network_supply_json_includes_valued_unknown_pools() -> Result<(), CipherscanRestError> {
        let pools = vec![
            wallet::ChainValuePool {
                id: String::from("transparent"),
                monitored: true,
                chain_value_zat: Some(1_000),
            },
            wallet::ChainValuePool {
                id: String::from("sprout"),
                monitored: true,
                chain_value_zat: Some(0),
            },
            wallet::ChainValuePool {
                id: String::from("sapling"),
                monitored: true,
                chain_value_zat: Some(0),
            },
            wallet::ChainValuePool {
                id: String::from("orchard"),
                monitored: true,
                chain_value_zat: Some(0),
            },
            wallet::ChainValuePool {
                id: String::from("ironwood"),
                monitored: true,
                chain_value_zat: Some(0),
            },
            wallet::ChainValuePool {
                id: String::from("lockbox"),
                monitored: true,
                chain_value_zat: Some(0),
            },
            wallet::ChainValuePool {
                id: String::from("future-pool"),
                monitored: true,
                chain_value_zat: Some(200),
            },
        ];

        let supply = network_supply_json("zcash-testnet", "NU6.3", &pools)?;
        let overview = pool_overview_json(
            &explorer::ValuePoolSummaryResponse {
                freshness: Some(rich_list_freshness(10)),
                pools,
                source_tip: Some(value_pool_test_source_tip(10)),
            },
            &value_pool_history_test_response(10, false, Vec::new()),
        )?;

        assert_eq!(supply["chainSupply"], json!(0.000_012));
        assert_eq!(supply["degraded"], json!(true));
        assert!(
            supply["unavailable"]
                .as_array()
                .is_some_and(|fields| !fields.is_empty())
        );
        assert!(overview["unavailable"].as_array().is_some_and(|reasons| {
            reasons
                .iter()
                .any(|reason| reason == UNKNOWN_VALUE_POOL_SEMANTICS_UNAVAILABLE)
        }));
        Ok(())
    }

    #[test]
    fn network_supply_json_rejects_unvalued_monitored_unknown_pool() {
        let pools = vec![
            wallet::ChainValuePool {
                id: String::from("transparent"),
                monitored: true,
                chain_value_zat: Some(1_000),
            },
            wallet::ChainValuePool {
                id: String::from("sprout"),
                monitored: true,
                chain_value_zat: Some(0),
            },
            wallet::ChainValuePool {
                id: String::from("sapling"),
                monitored: true,
                chain_value_zat: Some(0),
            },
            wallet::ChainValuePool {
                id: String::from("orchard"),
                monitored: true,
                chain_value_zat: Some(0),
            },
            wallet::ChainValuePool {
                id: String::from("ironwood"),
                monitored: true,
                chain_value_zat: Some(0),
            },
            wallet::ChainValuePool {
                id: String::from("lockbox"),
                monitored: true,
                chain_value_zat: Some(0),
            },
            wallet::ChainValuePool {
                id: String::from("future-pool"),
                monitored: true,
                chain_value_zat: None,
            },
        ];

        assert!(matches!(
            network_supply_json("zcash-testnet", "NU6.3", &pools),
            Err(CipherscanRestError::MissingUpstreamField(
                "value_pool_summary.pools.chain_value_zat"
            ))
        ));
    }

    #[test]
    fn value_pool_validation_rejects_duplicate_ids_and_negative_values() {
        let duplicate_ids = vec![
            wallet::ChainValuePool {
                id: String::from("transparent"),
                monitored: true,
                chain_value_zat: Some(0),
            },
            wallet::ChainValuePool {
                id: String::from("transparent"),
                monitored: true,
                chain_value_zat: Some(1),
            },
        ];
        let negative_value = vec![wallet::ChainValuePool {
            id: String::from("transparent"),
            monitored: true,
            chain_value_zat: Some(-1),
        }];

        assert!(matches!(
            validate_value_pools(&duplicate_ids),
            Err(CipherscanRestError::InvalidUpstreamField(
                "value_pool_summary.pools.id"
            ))
        ));
        assert!(matches!(
            validate_value_pools(&negative_value),
            Err(CipherscanRestError::InvalidUpstreamField(
                "value_pool_summary.pools.chain_value_zat"
            ))
        ));
    }

    #[test]
    fn migration_analytics_include_coinbase_and_match_public_predicates()
    -> Result<(), CipherscanRestError> {
        let entries = vec![
            migration_history_entry(4_134_256, 300, false, 10_000_000_000, -10_000_000_000),
            migration_history_entry(4_134_020, 200, true, 0, -125_000_000),
            migration_history_entry(4_134_010, 100, false, 100_000_000, -100_000_000),
            migration_history_entry(4_134_005, 50, false, 50_000_000, 50_000_000),
        ];

        let MigrationAnalyticsState::Available(analytics) =
            migration_analytics_from_entries(&entries)?
        else {
            return Err(CipherscanRestError::MissingUpstreamField(
                "test.migration_analytics",
            ));
        };

        assert_eq!(analytics.transaction_count, 3);
        assert_eq!(analytics.total_migrated_zat, 10_225_000_000);
        assert_eq!(analytics.orchard_out_zat, 10_100_000_000);
        assert_eq!(analytics.ironwood_in_zat, 10_225_000_000);
        assert_eq!(analytics.first_height, Some(4_134_010));
        assert_eq!(analytics.last_height, Some(4_134_256));
        assert_eq!(analytics.cohorts.len(), 2);
        assert_eq!(analytics.cohorts[0].boundary, 4_134_010 / 256);
        assert_eq!(analytics.cohorts[0].boundary_start_height, 4_134_010);
        assert_eq!(analytics.cohorts[0].transaction_count, 2);
        assert_eq!(analytics.cohorts[0].volume_zat, 225_000_000);
        assert_eq!(analytics.cohorts[0].first_time_unix_seconds, 100);
        assert_eq!(analytics.denomination_bins.len(), 2);
        assert_eq!(analytics.denomination_bins[0].power, 2);
        assert_eq!(analytics.denomination_bins[0].transaction_count, 1);
        assert_eq!(analytics.denomination_bins[1].power, 0);
        assert_eq!(analytics.denomination_bins[1].transaction_count, 2);
        Ok(())
    }

    #[test]
    fn migration_analytics_do_not_interpret_missing_balances_as_zero()
    -> Result<(), CipherscanRestError> {
        let entries = vec![explorer::TransactionHistoryEntry {
            block_height: 4_134_001,
            intrinsic_value_balances: None,
            ..Default::default()
        }];

        let state = migration_analytics_from_entries(&entries)?;

        assert!(matches!(
            state,
            MigrationAnalyticsState::Unavailable(
                MigrationAnalyticsUnavailable::IntrinsicValueBalanceUnavailable
            )
        ));
        Ok(())
    }

    #[test]
    fn migration_denomination_powers_use_integer_zatoshi_boundaries()
    -> Result<(), CipherscanRestError> {
        let entries = vec![
            migration_history_entry(4_134_004, 4, false, 0, -100_000_000),
            migration_history_entry(4_134_003, 3, false, 0, -99_999_999),
            migration_history_entry(4_134_002, 2, false, 0, -10),
            migration_history_entry(4_134_001, 1, false, 0, -1),
        ];

        let MigrationAnalyticsState::Available(analytics) =
            migration_analytics_from_entries(&entries)?
        else {
            return Err(CipherscanRestError::MissingUpstreamField(
                "test.migration_denominations",
            ));
        };
        let powers = analytics
            .denomination_bins
            .iter()
            .map(|bin| bin.power)
            .collect::<Vec<_>>();

        assert_eq!(powers, vec![0, -1, -7, -8]);
        Ok(())
    }

    #[test]
    fn migration_history_pages_require_newest_first_entries() {
        let entry = |block_height, transaction_index| explorer::TransactionHistoryEntry {
            block_height,
            transaction_index,
            ..Default::default()
        };

        assert!(transaction_history_entries_are_newest_first(&[
            entry(20, 2),
            entry(20, 1),
            entry(19, 3),
        ]));
        assert!(!transaction_history_entries_are_newest_first(&[
            entry(19, 3),
            entry(20, 1),
        ]));
        assert!(!transaction_history_entries_are_newest_first(&[
            entry(20, 1),
            entry(20, 1),
        ]));
    }

    #[test]
    fn migration_json_uses_native_aggregates_and_current_pool_progress()
    -> Result<(), CipherscanRestError> {
        let tip_height = 4_134_100;
        let summary = explorer::ValuePoolSummaryResponse {
            freshness: None,
            pools: vec![
                wallet::ChainValuePool {
                    id: String::from("orchard"),
                    monitored: true,
                    chain_value_zat: Some(800),
                },
                wallet::ChainValuePool {
                    id: String::from("ironwood"),
                    monitored: true,
                    chain_value_zat: Some(200),
                },
            ],
            source_tip: Some(value_pool_test_source_tip(tip_height)),
        };
        let analytics_state = migration_analytics_from_entries(&[
            migration_history_entry(4_134_020, 200, true, 0, -125),
            migration_history_entry(4_134_010, 100, false, 100, -10),
        ])?;

        let overview = migration_overview_json(
            Network::ZcashTestnet,
            tip_height,
            &summary,
            &analytics_state,
        )?;
        let cohorts = migration_cohorts_json(Network::ZcashTestnet, &analytics_state);
        let denominations = migration_denominations_json(Network::ZcashTestnet, &analytics_state);

        assert_eq!(overview["success"], json!(true));
        assert_eq!(overview["network"], json!("testnet"));
        assert_eq!(overview["activationHeight"], json!(4_134_000));
        assert_eq!(overview["tipHeight"], json!(4_134_100));
        assert_eq!(overview["activated"], json!(true));
        assert_eq!(overview["blocksUntilActivation"], json!(0));
        assert_eq!(overview["poolSizes"]["orchardZat"], json!(800));
        assert_eq!(overview["poolSizes"]["ironwoodZat"], json!(200));
        assert_eq!(overview["migration"]["txCount"], json!(2));
        assert_eq!(overview["migration"]["totalMigratedZat"], json!(135));
        assert_eq!(overview["migration"]["firstHeight"], json!(4_134_010));
        assert_eq!(overview["migration"]["lastHeight"], json!(4_134_020));
        assert_eq!(overview["migration"]["migratedPercent"], json!(20.0));
        assert_eq!(overview["supplyAudit"]["orchardOutZat"], json!(100));
        assert_eq!(overview["supplyAudit"]["ironwoodInZat"], json!(135));
        assert_eq!(overview["supplyAudit"]["balanced"], json!(false));
        assert_eq!(overview["degraded"], json!(true));
        assert_eq!(cohorts["cohortCount"], json!(1));
        assert_eq!(cohorts["cohorts"][0]["txCount"], json!(2));
        assert_eq!(cohorts["degraded"], json!(false));
        assert_eq!(denominations["totalTx"], json!(2));
        assert_eq!(denominations["bins"][0]["power"], json!(-6));
        assert_eq!(denominations["bins"][1]["power"], json!(-7));
        assert_eq!(denominations["degraded"], json!(false));
        Ok(())
    }

    #[test]
    fn migration_overview_json_returns_null_progress_for_incomplete_pool_values()
    -> Result<(), CipherscanRestError> {
        let tip_height = 4_133_990;
        let summary = explorer::ValuePoolSummaryResponse {
            freshness: None,
            pools: Vec::new(),
            source_tip: Some(value_pool_test_source_tip(tip_height)),
        };

        let overview = migration_overview_json(
            Network::ZcashTestnet,
            tip_height,
            &summary,
            &MigrationAnalyticsState::Available(MigrationAnalytics::default()),
        )?;

        assert_eq!(overview["activated"], json!(false));
        assert_eq!(overview["blocksUntilActivation"], json!(10));
        assert_eq!(overview["poolSizes"]["orchardZat"], Value::Null);
        assert_eq!(overview["poolSizes"]["ironwoodZat"], Value::Null);
        assert_eq!(overview["migration"]["migratedPercent"], Value::Null);
        assert!(overview["unavailable"].as_array().is_some_and(|reasons| {
            reasons
                .iter()
                .any(|reason| reason == VALUE_POOL_TOTALS_UNAVAILABLE)
        }));
        Ok(())
    }

    #[test]
    fn migration_overview_json_uses_mainnet_activation_height() -> Result<(), CipherscanRestError> {
        let tip_height = 3_500_000;
        let summary = explorer::ValuePoolSummaryResponse {
            freshness: None,
            pools: Vec::new(),
            source_tip: Some(value_pool_test_source_tip(tip_height)),
        };

        let overview = migration_overview_json(
            Network::ZcashMainnet,
            tip_height,
            &summary,
            &MigrationAnalyticsState::Available(MigrationAnalytics::default()),
        )?;

        assert_eq!(overview["network"], json!("mainnet"));
        assert_eq!(overview["activationHeight"], json!(3_428_143));
        assert_eq!(overview["activated"], json!(true));
        assert_eq!(overview["blocksUntilActivation"], json!(0));
        Ok(())
    }

    #[test]
    fn migration_json_preserves_explicit_unavailability_without_zero_substitution()
    -> Result<(), CipherscanRestError> {
        let unavailable = MigrationAnalyticsState::Unavailable(
            MigrationAnalyticsUnavailable::CapabilityUnavailable,
        );
        let cohorts = migration_cohorts_json(Network::ZcashTestnet, &unavailable);
        let denominations = migration_denominations_json(Network::ZcashTestnet, &unavailable);
        let overview = migration_overview_json(
            Network::ZcashTestnet,
            4_134_100,
            &explorer::ValuePoolSummaryResponse::default(),
            &unavailable,
        )?;

        assert_eq!(cohorts["success"], json!(true));
        assert_eq!(cohorts["network"], json!("testnet"));
        assert_eq!(cohorts["boundaryModulus"], json!(256));
        assert_eq!(cohorts["cohortCount"], Value::Null);
        assert_eq!(cohorts["avgAnonymitySet"], Value::Null);
        assert_eq!(cohorts["minAnonymitySet"], Value::Null);
        assert_eq!(cohorts["maxAnonymitySet"], Value::Null);
        assert_eq!(cohorts["cohorts"], json!([]));
        assert_eq!(cohorts["degraded"], json!(true));
        assert_eq!(denominations["totalTx"], Value::Null);
        assert_eq!(denominations["bins"], json!([]));
        assert_eq!(denominations["degraded"], json!(true));
        assert_eq!(overview["migration"]["totalMigratedZat"], Value::Null);
        assert_eq!(overview["migration"]["txCount"], Value::Null);
        assert_eq!(overview["supplyAudit"]["orchardOutZat"], Value::Null);
        assert_eq!(overview["supplyAudit"]["ironwoodInZat"], Value::Null);
        assert_eq!(overview["supplyAudit"]["balanced"], Value::Null);
        assert!(
            overview["unavailable"]
                .as_array()
                .is_some_and(|fields| fields.iter().any(|field| field
                    .as_str()
                    .is_some_and(|reason| reason.contains("intrinsic_value_balances_v1"))))
        );
        Ok(())
    }

    #[test]
    fn shielded_count_json_preserves_simple_and_detailed_shapes() {
        let summary = explorer::TransactionComponentSummaryResponse {
            totals: Some(explorer::TransactionComponentTotals {
                sapling_or_orchard_transaction_count: 12,
                sapling_without_orchard_transaction_count: 7,
                orchard_without_sapling_transaction_count: 3,
                sapling_and_orchard_transaction_count: 2,
                sapling_or_orchard_fully_shielded_transaction_count: 5,
                ..Default::default()
            }),
            days: vec![explorer::TransactionComponentDay {
                day_start_unix_seconds: 1_751_328_000,
                totals: None,
                first_sapling_or_orchard_transaction_time_unix_seconds: Some(1_751_328_016),
                last_sapling_or_orchard_transaction_time_unix_seconds: Some(1_751_331_600),
            }],
            coverage: Some(explorer::TransactionComponentCoverage {
                complete_from_height: 1,
                complete_through_height: 100,
                complete_from_time_unix_seconds: 1_467_331_200,
                complete_through_time_unix_seconds: 1_751_331_600,
                requested_range_complete: true,
            }),
            ..Default::default()
        };
        let queried_at = OffsetDateTime::from_unix_timestamp(1_751_331_700)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);
        let simple = shielded_count_json("2026-07-01", false, &summary, queried_at);
        let detailed = shielded_count_json("2026-07-01", true, &summary, queried_at);

        assert_eq!(simple["success"], json!(true));
        assert_eq!(simple["since"], json!("2026-07-01"));
        assert_eq!(simple["totalShielded"], json!(12));
        assert_eq!(simple["breakdown"], Value::Null);
        assert_eq!(simple["degraded"], json!(false));

        assert_eq!(detailed["success"], json!(true));
        assert_eq!(detailed["totalShielded"], json!(12));
        assert_eq!(detailed["breakdown"]["saplingOnly"], json!(7));
        assert_eq!(detailed["breakdown"]["orchardOnly"], json!(3));
        assert_eq!(detailed["breakdown"]["bothPools"], json!(2));
        assert_eq!(detailed["fullyShielded"], json!(5));
        assert_eq!(detailed["partiallyShielded"], json!(7));
        assert_eq!(
            detailed["timeRange"]["firstTx"],
            json!("2025-07-01T00:00:16.000Z")
        );
        assert_eq!(detailed["degraded"], json!(false));
    }

    #[test]
    fn shielded_daily_json_preserves_cipherscan_daily_shape() {
        let summary = explorer::TransactionComponentSummaryResponse {
            totals: Some(explorer::TransactionComponentTotals {
                sapling_or_orchard_transaction_count: 9,
                ..Default::default()
            }),
            days: vec![
                explorer::TransactionComponentDay {
                    day_start_unix_seconds: 1_751_328_000,
                    totals: Some(explorer::TransactionComponentTotals {
                        sapling_or_orchard_transaction_count: 4,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                explorer::TransactionComponentDay {
                    day_start_unix_seconds: 1_751_414_400,
                    totals: Some(explorer::TransactionComponentTotals {
                        sapling_or_orchard_transaction_count: 5,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ],
            coverage: Some(explorer::TransactionComponentCoverage {
                requested_range_complete: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        let daily = shielded_daily_json("2025-07-01", "2025-07-03", &summary);

        assert_eq!(daily["success"], json!(true));
        assert_eq!(daily["since"], json!("2025-07-01"));
        assert_eq!(daily["until"], json!("2025-07-03"));
        assert_eq!(daily["totalDays"], json!(2));
        assert_eq!(daily["totalShielded"], json!(9));
        assert_eq!(daily["daily"][0]["date"], json!("2025-07-01"));
        assert_eq!(daily["daily"][0]["count"], json!(4));
        assert_eq!(daily["daily"][1]["count"], json!(5));
        assert_eq!(daily["degraded"], json!(false));
    }

    #[test]
    fn transaction_linkability_unavailable_json_preserves_degraded_shape() {
        let source = CipherscanLinkabilityFlow {
            transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
            direction: ValuePoolFlowDirection::Shield,
            amount_zat: 1_124_945_000,
            block_height: 4_157_695,
            block_time_unix_seconds: 1_783_697_187,
            pool: ValuePoolFlowPool::Sapling,
            transparent_addresses: vec!["tmFU5Ak942B7SciQpZCh3xH76QV3UmJgnDd".to_owned()],
        };
        let response = transaction_linkability_unavailable_json(
            SAMPLE_TRANSACTION_ID,
            Some(&source),
            true,
            "range unavailable",
        );

        assert_eq!(response["success"], json!(false));
        assert_eq!(response["txid"], json!(SAMPLE_TRANSACTION_ID));
        assert_eq!(response["flowType"], json!("shield"));
        assert_eq!(response["amountZat"], json!(1_124_945_000_u64));
        assert_eq!(response["hasShieldedActivity"], json!(true));
        assert_eq!(response["linkedTransactions"], json!([]));
        assert!(response.get("totalMatches").is_none());
        assert!(response.get("warningLevel").is_none());
        assert!(response.get("highestScore").is_none());
        assert_eq!(response["degraded"], json!(true));
    }

    #[test]
    fn transaction_linkability_query_matches_cipherscan_coercion() {
        assert_eq!(
            CipherscanLinkabilityRequest::from_query(&TransactionLinkabilityQuery::default()),
            CipherscanLinkabilityRequest {
                limit: 5,
                tolerance_zat: 100_000,
            }
        );
        assert_eq!(
            CipherscanLinkabilityRequest::from_query(&TransactionLinkabilityQuery {
                limit: Some("-2suffix".to_owned()),
                tolerance: Some("-1suffix".to_owned()),
            }),
            CipherscanLinkabilityRequest {
                limit: 1,
                tolerance_zat: 10_000,
            }
        );
    }

    #[test]
    fn transaction_linkability_scoring_matches_cipherscan_boundaries() {
        assert_eq!(cipherscan_linkability_amount_similarity_score(0), 35);
        assert_eq!(cipherscan_linkability_amount_similarity_score(10_000), 32);
        assert_eq!(cipherscan_linkability_amount_similarity_score(10_001), 28);
        assert_eq!(cipherscan_linkability_time_score(899), 25);
        assert_eq!(cipherscan_linkability_time_score(900), 20);
        assert_eq!(cipherscan_linkability_rarity_score(3), 20);
        assert_eq!(cipherscan_linkability_rarity_score(4), 15);
        assert_eq!(cipherscan_linkability_ambiguity_score(1, 12), 0);
        assert_eq!(cipherscan_linkability_ambiguity_score(3, 3), 48);
    }

    #[test]
    fn transaction_linkability_complete_zero_match_preserves_public_shape() {
        let source = CipherscanLinkabilityFlow {
            transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
            direction: ValuePoolFlowDirection::Shield,
            amount_zat: 1_124_945_000,
            block_height: 4_157_695,
            block_time_unix_seconds: 1_783_697_187,
            pool: ValuePoolFlowPool::Sapling,
            transparent_addresses: vec!["tmFU5Ak942B7SciQpZCh3xH76QV3UmJgnDd".to_owned()],
        };
        let response = transaction_linkability_json(&source, &[]);

        assert_eq!(response["flowType"], json!("shield"));
        assert_eq!(response["amount"], json!(11.24945));
        assert_eq!(response["amountZat"], json!(1_124_945_000_u64));
        assert_eq!(response["blockHeight"], json!(4_157_695));
        assert_eq!(response["pool"], json!("sapling"));
        assert_eq!(response["hasShieldedActivity"], json!(true));
        assert_eq!(response["linkedTransactions"], json!([]));
        assert_eq!(response["warningLevel"], json!("LOW"));
        assert_eq!(response["highestScore"], json!(0));
        assert!(response.get("degraded").is_none());
    }

    #[test]
    fn transaction_linkability_ranks_an_exact_fast_same_pool_pair()
    -> Result<(), CipherscanRestError> {
        let source = CipherscanLinkabilityFlow {
            transaction_id: SAMPLE_TRANSACTION_ID.to_owned(),
            direction: ValuePoolFlowDirection::Shield,
            amount_zat: 1_124_945_000,
            block_height: 100,
            block_time_unix_seconds: 1_000,
            pool: ValuePoolFlowPool::Sapling,
            transparent_addresses: Vec::new(),
        };
        let events = vec![
            explorer::ValuePoolFlowEvent {
                transaction_id: source.transaction_id.clone(),
                block_height: source.block_height,
                block_time_unix_seconds: source.block_time_unix_seconds,
                transaction_index_in_block: 1,
                direction: source.direction as i32,
                pool: source.pool as i32,
                amount_zat: source.amount_zat,
                pool_balances: None,
            },
            explorer::ValuePoolFlowEvent {
                transaction_id: "ab".repeat(32),
                block_height: 101,
                block_time_unix_seconds: 1_600,
                transaction_index_in_block: 1,
                direction: ValuePoolFlowDirection::Deshield as i32,
                pool: ValuePoolFlowPool::Sapling as i32,
                amount_zat: source.amount_zat,
                pool_balances: None,
            },
        ];

        let candidates = cipherscan_linkability_candidates(&source, &events, 100_000, 5)?;

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].flow.transaction_id, "ab".repeat(32));
        assert_eq!(candidates[0].linkability_score, 100);
        assert_eq!(candidates[0].warning_level, "HIGH");
        assert_eq!(candidates[0].ambiguity_score, 0);
        assert_eq!(candidates[0].time_delta_seconds, 600);
        assert_eq!(cipherscan_linkability_time_delta(600), "10 minutes after");
        Ok(())
    }

    #[test]
    fn privacy_linkage_edges_json_preserves_empty_page_shape() {
        let response = privacy_linkage_edges_json(20, 40);

        assert_eq!(response["success"], json!(true));
        assert_eq!(response["edges"], json!([]));
        assert_eq!(response["pagination"]["total"], json!(0));
        assert_eq!(response["pagination"]["limit"], json!(20));
        assert_eq!(response["pagination"]["offset"], json!(40));
        assert_eq!(response["pagination"]["returned"], json!(0));
        assert_eq!(response["pagination"]["hasMore"], json!(false));
        assert_eq!(response["degraded"], json!(true));
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "The fixture spells out every native pool and Cipherscan response field used by this contract test."
    )]
    fn privacy_stats_json_uses_exact_counts_score_trend_and_daily_wire_shape()
    -> Result<(), Box<dyn std::error::Error>> {
        let oldest_day_start = 1_751_328_000;
        let current_day_start = oldest_day_start + 29 * UNIX_SECONDS_PER_DAY;
        let value_pool_summary = explorer::ValuePoolSummaryResponse {
            freshness: Some(rich_list_freshness(200)),
            pools: vec![
                wallet::ChainValuePool {
                    id: "transparent".to_owned(),
                    monitored: true,
                    chain_value_zat: Some(800_000_000),
                },
                wallet::ChainValuePool {
                    id: "sprout".to_owned(),
                    monitored: true,
                    chain_value_zat: Some(10_000_000),
                },
                wallet::ChainValuePool {
                    id: "sapling".to_owned(),
                    monitored: true,
                    chain_value_zat: Some(20_000_000),
                },
                wallet::ChainValuePool {
                    id: "orchard".to_owned(),
                    monitored: true,
                    chain_value_zat: Some(30_000_000),
                },
                wallet::ChainValuePool {
                    id: "ironwood".to_owned(),
                    monitored: true,
                    chain_value_zat: Some(0),
                },
                wallet::ChainValuePool {
                    id: "lockbox".to_owned(),
                    monitored: true,
                    chain_value_zat: Some(40_000_000),
                },
            ],
            source_tip: Some(value_pool_test_source_tip(200)),
        };
        let history = value_pool_history_test_response(
            200,
            true,
            vec![explorer::ValuePoolBalanceHistoryPoint {
                day_start_unix_seconds: current_day_start,
                block_height: 200,
                block_hash: SAMPLE_BLOCK_HASH.to_owned(),
                block_time_unix_seconds: current_day_start + 1,
                pools: vec![
                    explorer::ValuePoolBalance {
                        id: "transparent".to_owned(),
                        monitored: true,
                        value_zat: Some(800_000_000),
                    },
                    explorer::ValuePoolBalance {
                        id: "sprout".to_owned(),
                        monitored: true,
                        value_zat: Some(10_000_000),
                    },
                    explorer::ValuePoolBalance {
                        id: "sapling".to_owned(),
                        monitored: true,
                        value_zat: Some(20_000_000),
                    },
                    explorer::ValuePoolBalance {
                        id: "orchard".to_owned(),
                        monitored: true,
                        value_zat: Some(30_000_000),
                    },
                    explorer::ValuePoolBalance {
                        id: "ironwood".to_owned(),
                        monitored: true,
                        value_zat: Some(0),
                    },
                    explorer::ValuePoolBalance {
                        id: "lockbox".to_owned(),
                        monitored: true,
                        value_zat: Some(40_000_000),
                    },
                ],
            }],
        );
        let all_time_totals = explorer::TransactionComponentTotals {
            transaction_count: 100,
            sapling_orchard_or_ironwood_transaction_count: 20,
            non_coinbase_without_sapling_orchard_or_ironwood_transaction_count: 70,
            non_coinbase_sapling_orchard_or_ironwood_with_transparent_inputs_and_outputs_transaction_count: 5,
            non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count: 8,
            coinbase_transaction_count: 10,
            ..Default::default()
        };
        let daily_totals = explorer::TransactionComponentTotals {
            transaction_count: 10,
            sapling_orchard_or_ironwood_transaction_count: 4,
            non_coinbase_without_sapling_orchard_or_ironwood_transaction_count: 6,
            ..Default::default()
        };
        let summary = |totals, days| TransactionComponentSummaryResponse {
            freshness: Some(rich_list_freshness(200)),
            totals: Some(totals),
            days,
            coverage: Some(explorer::TransactionComponentCoverage {
                requested_range_complete: true,
                ..Default::default()
            }),
        };
        let all_history_summary = summary(all_time_totals, Vec::new());
        let thirty_day_summary = summary(
            explorer::TransactionComponentTotals {
                sapling_orchard_or_ironwood_transaction_count: 60,
                ..Default::default()
            },
            Vec::new(),
        );
        let daily_summary = summary(
            daily_totals,
            vec![explorer::TransactionComponentDay {
                day_start_unix_seconds: current_day_start,
                totals: Some(daily_totals),
                ..Default::default()
            }],
        );
        let recent_summary = summary(
            explorer::TransactionComponentTotals {
                sapling_orchard_or_ironwood_transaction_count: 11,
                ..Default::default()
            },
            Vec::new(),
        );
        let previous_summary = summary(
            explorer::TransactionComponentTotals {
                sapling_orchard_or_ironwood_transaction_count: 10,
                ..Default::default()
            },
            Vec::new(),
        );
        let generated_at = OffsetDateTime::from_unix_timestamp(current_day_start + 10)?;

        let stats = privacy_stats_json(
            &value_pool_summary,
            &history,
            &all_history_summary,
            &thirty_day_summary,
            &daily_summary,
            &recent_summary,
            &previous_summary,
            generated_at,
        )?;

        assert_eq!(stats["totals"]["blocks"], json!(200));
        assert_eq!(stats["totals"]["shieldedTx"], json!(20));
        assert_eq!(stats["totals"]["transparentTx"], json!(70));
        assert_eq!(stats["totals"]["coinbaseTx"], json!(10));
        assert_eq!(stats["totals"]["mixedTx"], json!(5));
        assert_eq!(stats["totals"]["fullyShieldedTx"], json!(8));
        assert_eq!(stats["shieldedPool"]["currentSize"], json!(0.6));
        assert_eq!(stats["shieldedPool"]["chainSupply"], json!(9.0));
        assert_eq!(stats["metrics"]["shieldedPercentage"], json!(20.0));
        assert_eq!(stats["metrics"]["privacyScore"], json!(21));
        assert_eq!(stats["metrics"]["avgShieldedPerDay"], json!(2.0));
        assert_eq!(stats["metrics"]["adoptionTrend"], json!("stable"));
        assert_eq!(stats["trends"]["daily"].as_array().map(Vec::len), Some(30));
        assert_eq!(
            stats["trends"]["daily"][0]["date"],
            json!(cipherscan_timestamp_from_unix_seconds(current_day_start))
        );
        assert_eq!(stats["trends"]["daily"][0]["shielded"], json!(4));
        assert_eq!(stats["trends"]["daily"][0]["transparent"], json!(6));
        assert_eq!(
            stats["trends"]["daily"][0]["shieldedPercentage"],
            json!(40.0)
        );
        assert_eq!(stats["trends"]["daily"][0]["poolSize"], json!(0.6));
        assert_eq!(stats["trends"]["daily"][0]["privacyScore"], json!(21));
        assert_eq!(stats["trends"]["daily"][29]["shielded"], json!(0));
        assert_eq!(stats["lastBlockScanned"], json!(200));
        assert_eq!(stats["degraded"], json!(true));
        Ok(())
    }

    #[test]
    fn privacy_stats_daily_trends_preserve_zero_denominators()
    -> Result<(), Box<dyn std::error::Error>> {
        let day_start = 1_751_328_000;
        let component_summary = TransactionComponentSummaryResponse {
            days: vec![explorer::TransactionComponentDay {
                day_start_unix_seconds: day_start + 29 * UNIX_SECONDS_PER_DAY,
                totals: Some(explorer::TransactionComponentTotals {
                    transaction_count: 0,
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        let history = value_pool_history_test_response(200, true, Vec::new());

        let daily = privacy_stats_daily_trends(
            &component_summary,
            &history,
            true,
            true,
            day_start,
            Some(0),
            Some(0),
            Some(0),
        )?;

        assert_eq!(daily.len(), 30);
        assert_eq!(daily[0]["shielded"], json!(0));
        assert_eq!(daily[0]["transparent"], json!(0));
        assert_eq!(daily[0]["shieldedPercentage"], json!(0.0));
        assert_eq!(
            daily[0]["date"],
            json!(cipherscan_timestamp_from_unix_seconds(
                day_start + 29 * UNIX_SECONDS_PER_DAY
            ))
        );
        assert_eq!(daily[0]["privacyScore"], Value::Null);
        Ok(())
    }

    #[test]
    fn privacy_stats_daily_predicate_unavailability_is_scoped_to_one_bucket()
    -> Result<(), Box<dyn std::error::Error>> {
        let daily_start = 1_751_328_000;
        let unavailable_day = daily_start + 29 * UNIX_SECONDS_PER_DAY;
        let component_summary = TransactionComponentSummaryResponse {
            days: vec![explorer::TransactionComponentDay {
                day_start_unix_seconds: unavailable_day,
                totals: Some(explorer::TransactionComponentTotals {
                    transaction_predicate_unavailable_count: 1,
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        let history = value_pool_history_test_response(200, true, Vec::new());

        let daily = privacy_stats_daily_trends(
            &component_summary,
            &history,
            true,
            true,
            daily_start,
            Some(100),
            Some(20),
            Some(5),
        )?;

        assert_eq!(daily[0]["shielded"], Value::Null);
        assert_eq!(daily[0]["transparent"], Value::Null);
        assert_eq!(daily[0]["shieldedPercentage"], Value::Null);
        assert_eq!(daily[1]["shielded"], json!(0));
        assert_eq!(daily[1]["transparent"], json!(0));
        assert_eq!(daily[1]["shieldedPercentage"], json!(0.0));
        Ok(())
    }

    #[test]
    fn privacy_stats_retries_only_reader_epoch_movement() {
        assert!(privacy_stats_epoch_changed(
            &CipherscanRestError::InvalidUpstreamField("privacy_stats.visible_tip")
        ));
        assert!(!privacy_stats_epoch_changed(
            &CipherscanRestError::InvalidUpstreamField("value_pool_summary.pools")
        ));
        assert!(!privacy_stats_epoch_changed(
            &CipherscanRestError::MissingUpstreamField("privacy_stats.visible_tip")
        ));
    }

    #[test]
    fn privacy_score_and_wire_rounding_preserve_zero_denominators() {
        assert_eq!(
            privacy_score(None, Some(100), Some(1), Some(1), Some(1)),
            None
        );
        assert_eq!(
            privacy_score(Some(0), Some(0), Some(0), Some(0), Some(0)),
            Some(0)
        );
        assert_f64_close(privacy_average_per_day(42), 1.4);
        assert_f64_close(privacy_percentage(1, 3), 33.333_333);
    }

    #[test]
    fn privacy_adoption_trend_uses_strict_ten_percent_boundaries() {
        let summary = |shielded_transaction_count| TransactionComponentSummaryResponse {
            totals: Some(explorer::TransactionComponentTotals {
                sapling_orchard_or_ironwood_transaction_count: shielded_transaction_count,
                ..Default::default()
            }),
            coverage: Some(explorer::TransactionComponentCoverage {
                requested_range_complete: true,
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(
            privacy_adoption_trend(&summary(111), &summary(100)),
            Some("growing")
        );
        assert_eq!(
            privacy_adoption_trend(&summary(110), &summary(100)),
            Some("stable")
        );
        assert_eq!(
            privacy_adoption_trend(&summary(89), &summary(100)),
            Some("declining")
        );
        assert_eq!(
            privacy_adoption_trend(&summary(100), &summary(0)),
            Some("stable")
        );
    }

    #[test]
    fn privacy_stats_realtime_frame_preserves_the_rest_body()
    -> Result<(), Box<dyn std::error::Error>> {
        let (realtime_sender, mut realtime_events) = broadcast::channel(1);
        let stats = json!({
            "totals": { "totalTx": 123 },
            "metrics": { "privacyScore": Value::Null },
        });

        broadcast_realtime_payload(&realtime_sender, "privacy_stats", &stats);

        let CipherscanRealtimeDispatch::Payload(payload) = realtime_events.try_recv()? else {
            return Err(std::io::Error::other("expected one realtime payload").into());
        };
        let frame = serde_json::from_str::<Value>(&payload)?;
        assert_eq!(frame["type"], json!("privacy_stats"));
        assert_eq!(frame["data"], stats);
        Ok(())
    }

    #[test]
    fn privacy_batch_json_preserves_empty_pattern_shapes() {
        let batch = privacy_batch_risks_json(20, "30d");
        let clusters = privacy_clusters_json(20);

        assert_eq!(batch["success"], json!(true));
        assert_eq!(batch["patterns"], json!([]));
        assert_eq!(batch["pagination"]["nextCursor"], Value::Null);
        assert_eq!(batch["stats"]["period"], json!("30d"));
        assert_eq!(batch["stats"]["totalZecFlagged"], json!(0.0));
        assert_eq!(batch["algorithm"]["version"], json!("2.0"));
        assert_eq!(batch["degraded"], json!(true));

        assert_eq!(clusters["success"], json!(true));
        assert_eq!(clusters["clusters"], json!([]));
        assert_eq!(clusters["pagination"]["nextCursor"], Value::Null);
        assert_eq!(clusters["degraded"], json!(true));
    }

    #[test]
    fn privacy_graph_and_patterns_json_preserve_empty_shapes() {
        let graph = privacy_graph_json(SAMPLE_TRANSACTION_ID);
        let patterns = privacy_patterns_json(3, 6);

        assert_eq!(graph["success"], json!(true));
        assert_eq!(graph["txid"], json!(SAMPLE_TRANSACTION_ID));
        assert_eq!(graph["nodes"], json!([]));
        assert_eq!(graph["edges"], json!([]));
        assert_eq!(graph["clusters"], json!([]));
        assert_eq!(graph["degraded"], json!(true));

        assert_eq!(patterns["success"], json!(true));
        assert_eq!(patterns["patterns"], json!([]));
        assert_eq!(patterns["pagination"]["limit"], json!(3));
        assert_eq!(patterns["pagination"]["offset"], json!(6));
        assert_eq!(
            patterns["note"],
            json!(
                "Legacy detected_patterns view. Prefer /api/privacy/clusters for the new linkage pipeline."
            )
        );
        assert_eq!(patterns["degraded"], json!(true));
    }

    #[test]
    fn privacy_amount_json_preserves_empty_shapes() {
        let common = privacy_common_amounts_json("7d", None);
        let recommended = privacy_recommended_swap_amounts_json("eth", "USDC");

        assert_eq!(common["success"], json!(true));
        assert_eq!(common["period"], json!("7d"));
        assert_eq!(common["chain"], Value::Null);
        assert_eq!(common["totalTransactions"], json!(0));
        assert_eq!(common["amounts"], json!([]));
        assert_eq!(common["degraded"], json!(true));

        assert_eq!(recommended["success"], json!(true));
        assert_eq!(recommended["chain"], json!("eth"));
        assert_eq!(recommended["token"], json!("USDC"));
        assert_eq!(recommended["recommendations"], json!([]));
        assert_eq!(recommended["degraded"], json!(true));
    }

    #[test]
    fn common_amounts_query_normalizes_legacy_limit_and_period_inputs() {
        assert_eq!(common_amounts_limit(None), 10);
        assert_eq!(common_amounts_limit(Some("")), 10);
        assert_eq!(common_amounts_limit(Some("garbage")), 10);
        assert_eq!(common_amounts_limit(Some("0")), 10);
        assert_eq!(common_amounts_limit(Some(" -2rows")), 1);
        assert_eq!(common_amounts_limit(Some("8rows")), 8);
        assert_eq!(common_amounts_limit(Some("500")), 50);
        assert_eq!(common_amounts_limit(Some("999999999999999999999")), 50);

        assert_eq!(
            common_amounts_period(None),
            CommonAmountsPeriod {
                echoed: String::from("7d"),
                seconds: 7 * UNIX_SECONDS_PER_DAY,
            }
        );
        assert_eq!(common_amounts_period(Some("")), common_amounts_period(None));
        assert_eq!(
            common_amounts_period(Some("24h")).seconds,
            UNIX_SECONDS_PER_DAY
        );
        assert_eq!(
            common_amounts_period(Some("unknown")),
            CommonAmountsPeriod {
                echoed: String::from("unknown"),
                seconds: 7 * UNIX_SECONDS_PER_DAY,
            }
        );
        assert_eq!(
            common_amounts_range(&common_amounts_period(Some("7d")), 1_000_000),
            (1_000_000 - 7 * UNIX_SECONDS_PER_DAY + 1, i64::MAX)
        );
    }

    #[test]
    fn common_amounts_maps_ranked_rows_and_uses_full_threshold_denominator()
    -> Result<(), CipherscanRestError> {
        let response = common_amounts_json(
            "7d",
            &[
                explorer::ValuePoolFlowRoundedAmountSummaryRow {
                    rounded_amount_zat: 100_000_000,
                    shield_event_count: 12,
                    deshield_event_count: 3,
                },
                explorer::ValuePoolFlowRoundedAmountSummaryRow {
                    rounded_amount_zat: 1_000_000,
                    shield_event_count: 4,
                    deshield_event_count: 1,
                },
            ],
            &[explorer::ValuePoolFlowAmountThresholdSummaryRow {
                minimum_amount_zat: COMMON_AMOUNTS_MINIMUM_ZAT,
                shield_event_count: 30,
                deshield_event_count: 20,
                ..Default::default()
            }],
        )?;

        assert_eq!(response["success"], json!(true));
        assert_eq!(response["period"], json!("7d"));
        assert_eq!(response["chain"], Value::Null);
        assert_eq!(response["totalTransactions"], json!(50));
        assert_eq!(response["amounts"][0]["amountZec"], json!(1.0));
        assert_eq!(response["amounts"][0]["txCount"], json!(15));
        assert_eq!(response["amounts"][0]["percentage"], json!("30.0"));
        assert_eq!(response["amounts"][0]["blendingScore"], json!(100));
        assert_eq!(response["amounts"][1]["amountZec"], json!(0.01));
        assert_eq!(response["amounts"][1]["percentage"], json!("10.0"));
        assert_eq!(response["amounts"][1]["blendingScore"], json!(100));
        assert_eq!(
            response["tip"],
            json!(
                "Using common amounts helps you blend in with other transactions, making linkability analysis harder."
            )
        );
        Ok(())
    }

    #[test]
    fn common_amounts_uses_one_when_no_flow_events_match() -> Result<(), CipherscanRestError> {
        let response = common_amounts_json(
            "90d",
            &[],
            &[explorer::ValuePoolFlowAmountThresholdSummaryRow {
                minimum_amount_zat: COMMON_AMOUNTS_MINIMUM_ZAT,
                ..Default::default()
            }],
        )?;

        assert_eq!(response["totalTransactions"], json!(1));
        assert_eq!(response["amounts"], json!([]));
        Ok(())
    }

    #[test]
    fn common_amounts_requires_complete_matching_native_context() {
        let response = |epoch_id| explorer::ExplorerFreshness {
            chain_view: Some(wallet::ChainView {
                chain_epoch: Some(wallet::ChainEpoch {
                    chain_epoch_id: epoch_id,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let rounded = |epoch_id, complete| ValuePoolFlowRoundedAmountSummaryResponse {
            freshness: Some(response(epoch_id)),
            coverage: Some(explorer::ValuePoolFlowCoverage {
                requested_range_complete: complete,
                ..Default::default()
            }),
            ..Default::default()
        };
        let threshold = |epoch_id, complete| ValuePoolFlowAmountThresholdSummaryResponse {
            freshness: Some(response(epoch_id)),
            coverage: Some(explorer::ValuePoolFlowCoverage {
                requested_range_complete: complete,
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(require_common_amounts_context(&rounded(7, true), &threshold(7, true)).is_ok());
        assert!(require_common_amounts_context(&rounded(7, true), &threshold(8, true)).is_err());
        assert!(require_common_amounts_context(&rounded(7, false), &threshold(7, true)).is_err());
        assert!(require_common_amounts_context(&rounded(7, true), &threshold(7, false)).is_err());
    }

    #[test]
    fn parse_blend_amount_preserves_cipherscan_amount_bounds() {
        assert_eq!(parse_blend_amount(None), None);
        assert_eq!(parse_blend_amount(Some("abc")), None);
        assert_eq!(parse_blend_amount(Some("0")), None);
        assert_eq!(parse_blend_amount(Some("21000000.1")), None);
        assert_eq!(
            parse_blend_amount(Some("1.23456789")),
            Some((1.234_567_89, 123_456_789))
        );
        assert_eq!(
            parse_blend_amount(Some("  +1.25zec")),
            Some((1.25, 125_000_000))
        );
        assert_eq!(parse_blend_amount(Some("1e-10")), Some((1e-10, 0)));
        assert_eq!(parse_blend_amount(Some("1e")), Some((1.0, 100_000_000)));
    }

    #[test]
    fn blend_period_counts_preserve_cipherscan_field_names() {
        assert_eq!(
            blend_period_counts_json(BlendPeriodCounts {
                total: 7,
                shields: 5,
                deshields: 2,
            }),
            json!({ "total": 7, "shields": 5, "deshields": 2 })
        );
    }

    #[test]
    fn protocol_stats_json_builds_cumulative_monthly_history() {
        let summary = explorer::TransactionComponentSummaryResponse {
            totals: Some(explorer::TransactionComponentTotals {
                sapling_output_count: 11,
                sapling_spend_count: 7,
                orchard_action_count: 5,
                ironwood_action_count: 3,
                ..Default::default()
            }),
            days: vec![
                explorer::TransactionComponentDay {
                    day_start_unix_seconds: 1_735_689_600,
                    totals: Some(explorer::TransactionComponentTotals {
                        sapling_output_count: 4,
                        sapling_spend_count: 2,
                        orchard_action_count: 1,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                explorer::TransactionComponentDay {
                    day_start_unix_seconds: 1_738_368_000,
                    totals: Some(explorer::TransactionComponentTotals {
                        sapling_output_count: 7,
                        sapling_spend_count: 5,
                        orchard_action_count: 4,
                        ironwood_action_count: 3,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ],
            coverage: Some(explorer::TransactionComponentCoverage {
                requested_range_complete: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        let stats = protocol_stats_json(
            &summary,
            &VisibleTipCommitmentTreeSizes {
                chain_epoch_id: 42,
                block_height: 100,
                block_hash: "0".repeat(64),
                sapling_commitment_tree_size: 9,
                orchard_commitment_tree_size: 14,
                ironwood_commitment_tree_size: 18,
            },
        );

        assert_eq!(stats["success"], json!(true));
        assert_eq!(stats["available"], json!(true));
        assert_eq!(stats["current"]["saplingCommitments"], json!(11));
        assert_eq!(stats["current"]["saplingNullifiers"], json!(7));
        assert_eq!(stats["current"]["orchardCommitments"], json!(5));
        assert_eq!(stats["current"]["orchardNullifiers"], json!(5));
        assert_eq!(stats["current"]["ironwoodCommitments"], json!(3));
        assert_eq!(stats["current"]["ironwoodNullifiers"], json!(3));
        assert_eq!(stats["history"].as_array().map(Vec::len), Some(2));
        assert_eq!(
            stats["history"][0]["month"],
            json!("2025-01-01T00:00:00.000Z")
        );
        assert_eq!(stats["history"][0]["saplingCommitments"], json!(4));
        assert_eq!(stats["history"][1]["saplingCommitments"], json!(11));
        assert_eq!(
            stats["visibleTipCommitmentTreeSizes"]["chainEpochId"],
            json!("42")
        );
        assert_eq!(
            stats["visibleTipCommitmentTreeSizes"]["blockHeight"],
            json!(100)
        );
        assert_eq!(stats["visibleTipCommitmentTreeSizes"]["sapling"], json!(9));
        assert_eq!(stats["visibleTipCommitmentTreeSizes"]["orchard"], json!(14));
        assert_eq!(
            stats["visibleTipCommitmentTreeSizes"]["ironwood"],
            json!(18)
        );
        assert_eq!(stats["degraded"], json!(false));
        assert_eq!(stats["unavailable"], Value::Null);
    }

    #[test]
    fn usage_clock_json_preserves_heatmap_and_hourly_shapes() {
        let distribution = explorer::BlockActivityDistributionResponse {
            start_height: 100,
            end_height: 103,
            materialized_block_count: 4,
            first_block_time_unix_seconds: Some(1_736_035_200),
            last_block_time_unix_seconds: Some(1_736_121_600),
            transaction_count: 17,
            buckets: vec![
                explorer::BlockActivityBucket {
                    weekday: 0,
                    hour: 0,
                    transaction_count: 5,
                    block_count: 2,
                },
                explorer::BlockActivityBucket {
                    weekday: 0,
                    hour: 1,
                    transaction_count: 5,
                    block_count: 1,
                },
                explorer::BlockActivityBucket {
                    weekday: 1,
                    hour: 0,
                    transaction_count: 7,
                    block_count: 1,
                },
            ],
            ..Default::default()
        };
        let clock = usage_clock_json("90d", &distribution);

        assert_eq!(clock["period"], json!("90d"));
        assert_eq!(clock["dateRange"]["from"], json!("2025-01-05"));
        assert_eq!(clock["dateRange"]["to"], json!("2025-01-06"));
        assert_eq!(clock["totalBlocks"], json!(4));
        assert_eq!(clock["totalTxs"], json!(17));
        assert_eq!(clock["heatmap"].as_array().map(Vec::len), Some(168));
        assert_eq!(clock["hourly"].as_array().map(Vec::len), Some(24));
        assert_eq!(clock["heatmap"][0]["hour"], json!(0));
        assert_eq!(clock["heatmap"][0]["dow"], json!(0));
        assert_eq!(clock["heatmap"][0]["txCount"], json!(5));
        assert_eq!(clock["heatmap"][0]["blockCount"], json!(2));
        assert_eq!(clock["heatmap"][1]["txCount"], json!(5));
        assert_eq!(clock["heatmap"][24]["dow"], json!(1));
        assert_eq!(clock["heatmap"][24]["txCount"], json!(7));
        assert_eq!(clock["hourly"][0]["txCount"], json!(12));
        assert_eq!(clock["hourly"][1]["txCount"], json!(5));
        assert_eq!(clock["hourly"][23]["hour"], json!(23));
        assert_eq!(clock["peakHour"], json!(0));
        assert_eq!(clock["lowHour"], json!(2));
        assert_eq!(clock["peakToLowRatio"], json!(0.0));
        assert_eq!(clock["sampledBlockLimit"], json!(4));
        assert_eq!(clock["startHeight"], json!(100));
        assert_eq!(clock["endHeight"], json!(103));
        assert_eq!(clock["materializedBlockCount"], json!(4));
        assert_eq!(clock["missingBlockCount"], json!(0));
        assert_eq!(clock["degraded"], json!(true));

        let mut hourly = [1_u32; 24];
        hourly[3] = 10;
        hourly[4] = 5;
        assert_eq!(usage_clock_peak_hour(&hourly), 3);
        assert_eq!(usage_clock_low_hour(&hourly), 0);
        assert_eq!(json!(usage_clock_peak_to_low_ratio(10, 5)), json!(2.0));
        assert_eq!(usage_clock_block_limit("all"), MAX_USAGE_CLOCK_BLOCKS);
        assert_eq!(usage_clock_block_limit("30d"), MAX_USAGE_CLOCK_BLOCKS);
    }

    #[test]
    fn usage_clock_json_reports_missing_materialized_blocks() {
        let distribution = explorer::BlockActivityDistributionResponse {
            start_height: 100,
            end_height: 103,
            materialized_block_count: 3,
            missing_block_count: 1,
            first_block_time_unix_seconds: Some(1_736_035_200),
            last_block_time_unix_seconds: Some(1_736_121_600),
            transaction_count: 17,
            buckets: vec![explorer::BlockActivityBucket {
                weekday: 0,
                hour: 0,
                transaction_count: 2,
                block_count: 1,
            }],
            ..Default::default()
        };

        let clock = usage_clock_json("90d", &distribution);

        assert_eq!(clock["missingBlockCount"], json!(1));
        assert_eq!(clock["degraded"], json!(true));
        assert!(clock["unavailable"].as_array().is_some_and(|items| {
            items.iter().any(|item| {
                item == "The requested height range contains 1 unavailable block-summary rows."
            })
        }));
    }

    #[tokio::test]
    async fn crosslink_routes_do_not_fabricate_finality_or_finalizer_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let responses = [
            crosslink_stats().await,
            crosslink_bft_chain().await,
            crosslink_bft_tip().await,
            finalizers().await,
            finalizer_detail(Path(String::from(SAMPLE_TRANSACTION_ID))).await,
            finalizer_participation(Path(String::from(SAMPLE_TRANSACTION_ID))).await,
            crosslink_participation().await,
        ];

        for response in responses {
            let (status, body) = read_json_response(response).await?;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(body["success"], json!(false));
            assert_eq!(
                body["error"],
                json!("Crosslink consensus data is unavailable")
            );
            assert!(body.get("finalizedHeight").is_none());
            assert!(body.get("finalityGap").is_none());
            assert!(body.get("finalizers").is_none());
            assert!(body.get("totalStakeZec").is_none());
        }
        Ok(())
    }

    #[test]
    fn crosslink_divergence_history_json_preserves_empty_event_shape() {
        let response = crosslink_divergence_history_json();

        assert_eq!(response["success"], json!(true));
        assert_eq!(response["count"], json!(0));
        assert_eq!(response["openEvent"], Value::Null);
        assert_eq!(response["events"], json!([]));
        assert_eq!(response["degraded"], json!(true));
        assert!(
            response["unavailable"]
                .as_array()
                .is_some_and(|fields| !fields.is_empty())
        );
    }

    #[test]
    fn crosslink_bootstrap_info_json_preserves_no_snapshot_shape() {
        let response = crosslink_bootstrap_info_json();

        assert_eq!(response["success"], json!(true));
        assert_eq!(response["available"], json!(false));
        assert_eq!(response["degraded"], json!(true));
        assert!(
            response["unavailable"]
                .as_array()
                .is_some_and(|fields| !fields.is_empty())
        );
    }

    #[tokio::test]
    async fn crosschain_routes_report_unavailable_without_zero_totals()
    -> Result<(), Box<dyn std::error::Error>> {
        let responses = [
            crosschain_stats().await,
            crosschain_db_stats().await,
            crosschain_inflows().await,
            crosschain_outflows().await,
            crosschain_status().await,
            crosschain_trends().await,
            crosschain_history().await,
            crosschain_volume_by_chain().await,
            crosschain_address(Path(String::from("t1abc"))).await,
            crosschain_popular_pairs().await,
        ];

        for response in responses {
            let (status, body) = read_json_response(response).await?;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(body["success"], json!(false));
            assert_eq!(
                body["error"],
                json!("Cross-chain analytics are unavailable")
            );
            assert!(body.get("totalVolume24h").is_none());
            assert!(body.get("totalSwapsAllTime").is_none());
            assert!(body.get("total").is_none());
        }
        Ok(())
    }

    #[tokio::test]
    async fn name_and_label_routes_do_not_invent_absence_or_pricing()
    -> Result<(), Box<dyn std::error::Error>> {
        let responses = [
            name_lookup(Path(String::from("satoshi"))).await,
            labels().await,
            label_lookup(Path(String::from("t1abc"))).await,
        ];

        for response in responses {
            let (status, body) = read_json_response(response).await?;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(body["success"], json!(false));
            assert!(body.get("pricing").is_none());
            assert!(body.get("labels").is_none());
            assert!(body.get("label").is_none());
        }
        Ok(())
    }

    #[test]
    fn name_events_json_preserves_empty_history_shape() {
        let response = name_events_json("satoshi");

        assert_eq!(response["events"], json!([]));
        assert_eq!(response["total"], json!(0));
        assert_eq!(response["name"], json!("satoshi"));
        assert_eq!(response["degraded"], json!(true));
    }

    #[test]
    fn pool_turnstile_building_json_matches_cipherscan_rebuild_shape() {
        let turnstile = pool_turnstile_building_json();

        assert_eq!(turnstile["success"], json!(false));
        assert_eq!(
            turnstile["error"],
            json!("turnstile_daily view is rebuilding")
        );
        assert_eq!(turnstile["status"], json!("building"));
        assert_eq!(turnstile["retryAfter"], json!(60));
        assert_eq!(turnstile["degraded"], json!(true));
        assert!(
            turnstile["unavailable"]
                .as_array()
                .is_some_and(|fields| !fields.is_empty())
        );
    }

    #[test]
    fn historical_price_json_preserves_no_market_data_shape() {
        let price = historical_price_json("2024-01-01");

        assert_eq!(price["date"], json!("2024-01-01"));
        assert_eq!(price["price_usd"], Value::Null);
        assert_eq!(price["exact"], json!(false));
        assert_eq!(price.as_object().map(serde_json::Map::len), Some(3));
    }

    #[test]
    fn historical_price_date_shape_matches_cipherscan_lexical_validation() {
        assert!(has_iso8601_calendar_date_shape("2024-01-01"));
        assert!(has_iso8601_calendar_date_shape("2024-02-29"));
        assert!(has_iso8601_calendar_date_shape("2023-02-29"));
        assert!(has_iso8601_calendar_date_shape("2024-99-99"));
        assert!(!has_iso8601_calendar_date_shape("2024-1-1"));
        assert!(!has_iso8601_calendar_date_shape("not-a-date"));
    }

    #[test]
    fn historical_price_lookup_clamps_future_dates_to_latest_complete_day()
    -> Result<(), Box<dyn std::error::Error>> {
        let latest_complete_date = OffsetDateTime::now_utc()
            .date()
            .previous_day()
            .ok_or("latest completed date is outside the supported range")?;

        assert_eq!(
            historical_price_lookup_date("9999-12-31"),
            latest_complete_date.to_string()
        );
        assert_eq!(historical_price_lookup_date("2024-01-01"), "2024-01-01");
        assert_eq!(historical_price_lookup_date("2024-99-99"), "2024-99-99");
        Ok(())
    }

    #[test]
    fn value_pool_row_uses_cipherscan_supply_shape() {
        let pool = wallet::ChainValuePool {
            id: "orchard".to_owned(),
            monitored: true,
            chain_value_zat: Some(123_456_789),
        };

        let row = value_pool_row(&pool);

        assert_eq!(row["id"], json!("orchard"));
        assert_eq!(row["chainValue"], json!(1.234_567_89));
        assert_eq!(row["chainValueZat"], json!("123456789"));
        assert_eq!(row["monitored"], json!(true));
    }

    #[test]
    fn value_pool_row_preserves_absent_pool_values() {
        let pool = wallet::ChainValuePool {
            id: "future-pool".to_owned(),
            monitored: false,
            chain_value_zat: None,
        };

        let row = value_pool_row(&pool);

        assert_eq!(row["id"], json!("future-pool"));
        assert_eq!(row["chainValue"], Value::Null);
        assert_eq!(row["chainValueZat"], Value::Null);
        assert_eq!(row["monitored"], json!(false));
    }
}
