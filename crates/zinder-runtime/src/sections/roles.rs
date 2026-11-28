//! Role-specific configuration sections belonging to the unified [`crate::ZinderConfig`] schema.

use std::path::PathBuf;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

use crate::{
    BrowserOrigins, GrpcRangeAdmission,
    config::{RedactedCidrs, RedactedUrl, SecretInput, Toggle},
};

/// Coverage policy applied to `ingest.run_overrides.coverage`.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum IngestCoverage {
    /// Use explicitly supplied modifier heights as-is.
    #[default]
    Explicit,
    /// Derive the historical floor needed by native wallet serving.
    WalletServing,
}

impl IngestCoverage {
    /// Returns the kebab-case configuration representation of this coverage mode.
    #[must_use]
    pub const fn as_kebab_case(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::WalletServing => "wallet-serving",
        }
    }
}

/// Configured name of a tree-state checkpoint selection policy.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TreeStateSelectionName {
    /// Store tree-state checkpoints at every settled block.
    EveryBlock,
    /// Spread tree-state checkpoints by commitment budget.
    CommitmentBudget,
}

/// Raw `[ingest]` configuration section.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestSection {
    /// Canonical source-adapter selector (`zebra-json-rpc` or `zebra-indexer-grpc`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Whether the bundled Explorer materialized-view workload is requested.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explorer_views: Option<bool>,
    /// Available bytes on the store volume below which Explorer view replay pauses. Zero disables.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explorer_views_min_free_bytes: Option<u64>,
    /// Reorg-window depth below the upstream tip for settlement.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reorg_window_blocks: Option<u32>,
    /// Phase classifier knobs for `zinder-ingest probe`.
    #[serde(
        default,
        skip_serializing_if = "IngestPhaseClassificationSection::is_empty"
    )]
    pub phase_classification: IngestPhaseClassificationSection,
    /// Source-fetch and block-preparation knobs for canonical construction.
    #[serde(default, skip_serializing_if = "IngestConstructionSection::is_empty")]
    pub construction: IngestConstructionSection,
    /// Live mempool admission and durable reconciliation limits.
    #[serde(default, skip_serializing_if = "IngestMempoolSection::is_empty")]
    pub mempool: IngestMempoolSection,
    /// Canonical follower knobs.
    #[serde(default, skip_serializing_if = "IngestFollowSection::is_empty")]
    pub follow: IngestFollowSection,
    /// One-shot `run_overrides` for the ingest run.
    #[serde(default, skip_serializing_if = "IngestRunOverridesSection::is_empty")]
    pub run_overrides: IngestRunOverridesSection,
    /// Background densification of settled tree-state checkpoints.
    #[serde(
        default,
        skip_serializing_if = "IngestTreeStateDensificationSection::is_empty"
    )]
    pub tree_state_densification: IngestTreeStateDensificationSection,
    /// Background verification of stored tree-state checkpoints.
    #[serde(
        default,
        skip_serializing_if = "IngestTreeStateVerificationSection::is_empty"
    )]
    pub tree_state_verification: IngestTreeStateVerificationSection,
}

impl IngestSection {
    /// Reports whether every field and subsection in `[ingest]` is unset.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.source.is_none()
            && self.explorer_views.is_none()
            && self.explorer_views_min_free_bytes.is_none()
            && self.reorg_window_blocks.is_none()
            && self.phase_classification.is_empty()
            && self.construction.is_empty()
            && self.mempool.is_empty()
            && self.follow.is_empty()
            && self.run_overrides.is_empty()
            && self.tree_state_densification.is_empty()
            && self.tree_state_verification.is_empty()
    }
}

/// Raw `[ingest.tree_state_densification]` configuration subsection.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestTreeStateDensificationSection {
    /// Enable background densification of tree-state checkpoints.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Allow densification during bulk canonical construction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub during_construction: Option<bool>,
    /// Checkpoint selection policy name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selection: Option<TreeStateSelectionName>,
    /// Commitment budget per interval when `selection = "commitment-budget"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commitment_budget: Option<u32>,
    /// Worker concurrency for densification.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workers: Option<u32>,
    /// Rate limit in blocks per second.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_blocks_per_second: Option<u32>,
}

impl IngestTreeStateDensificationSection {
    /// Reports whether every field is unset.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.enabled.is_none()
            && self.during_construction.is_none()
            && self.selection.is_none()
            && self.commitment_budget.is_none()
            && self.workers.is_none()
            && self.max_blocks_per_second.is_none()
    }
}

/// Raw `[ingest.tree_state_verification]` configuration subsection.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestTreeStateVerificationSection {
    /// Enable background verification of stored tree-state checkpoints.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Rate limit in rows per second.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_rows_per_second: Option<u32>,
}

impl IngestTreeStateVerificationSection {
    /// Reports whether every field is unset.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.enabled.is_none() && self.max_rows_per_second.is_none()
    }
}

/// Raw `[ingest.mempool]` configuration subsection.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestMempoolSection {
    /// Maximum unmined mempool transactions retained.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_transaction_count: Option<u32>,
    /// Maximum raw bytes across retained unmined mempool transactions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_total_raw_transaction_bytes: Option<u64>,
    /// Target raw transaction bytes per durable reconciliation batch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reconciliation_batch_target_raw_transaction_bytes: Option<u64>,
}

impl IngestMempoolSection {
    /// Reports whether every field is unset.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.max_transaction_count.is_none()
            && self.max_total_raw_transaction_bytes.is_none()
            && self
                .reconciliation_batch_target_raw_transaction_bytes
                .is_none()
    }
}

/// Raw `[ingest.phase_classification]` configuration subsection.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestPhaseClassificationSection {
    /// Catch-up distance threshold in blocks for phase classification.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catchup_threshold_blocks: Option<u32>,
}

impl IngestPhaseClassificationSection {
    /// Reports whether every field is unset.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.catchup_threshold_blocks.is_none()
    }
}

/// Raw `[ingest.construction]` configuration subsection.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestConstructionSection {
    /// Maximum blocks per source segment request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_segment_max_blocks: Option<u32>,
    /// Target byte size per source segment response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_segment_target_response_bytes: Option<u64>,
    /// Maximum concurrent in-flight source fetch requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_fetch_max_in_flight_requests: Option<u32>,
    /// Maximum aggregate bytes across in-flight source fetches.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_fetch_max_in_flight_bytes: Option<u64>,
    /// Concurrency for block preparation workers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_prepare_concurrency: Option<u32>,
    /// High-water memory byte limit for block preparation queues.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_prepare_memory_watermark_bytes: Option<u64>,
    /// Worker count for commitment-tree hashing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tree_hashing_workers: Option<u32>,
}

impl IngestConstructionSection {
    /// Reports whether every field is unset.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.source_segment_max_blocks.is_none()
            && self.source_segment_target_response_bytes.is_none()
            && self.source_fetch_max_in_flight_requests.is_none()
            && self.source_fetch_max_in_flight_bytes.is_none()
            && self.block_prepare_concurrency.is_none()
            && self.block_prepare_memory_watermark_bytes.is_none()
            && self.tree_hashing_workers.is_none()
    }
}

/// Raw `[ingest.follow]` configuration subsection.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestFollowSection {
    /// Follower tip polling cadence in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub poll_interval_ms: Option<u64>,
    /// Block-lag threshold before reporting follower lag.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lag_threshold_blocks: Option<u64>,
    /// Maximum accounted byte size of a reorg replacement batch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_replacement_bytes: Option<u64>,
}

impl IngestFollowSection {
    /// Reports whether every field is unset.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.poll_interval_ms.is_none()
            && self.lag_threshold_blocks.is_none()
            && self.max_replacement_bytes.is_none()
    }
}

/// Raw `[ingest.run_overrides]` configuration subsection.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestRunOverridesSection {
    /// Optional target block height override.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_height: Option<u32>,
    /// Optional checkpoint floor height override.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_height: Option<u32>,
    /// Coverage policy applied during bootstrap.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coverage: Option<IngestCoverage>,
}

impl IngestRunOverridesSection {
    /// Reports whether every field is unset.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.target_height.is_none() && self.checkpoint_height.is_none() && self.coverage.is_none()
    }
}

/// Raw `[projector]` configuration section.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProjectorSection {
    /// Expected canonical store reorg window in blocks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reorg_window_blocks: Option<u32>,
    /// Fixed 16-byte lowercase hex build-owner identifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub build_owner_hex: Option<String>,
    /// Lease duration in seconds for canonical build pinning.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_duration_seconds: Option<u64>,
    /// Convergence timeout in seconds when acquiring a canonical fence.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fence_convergence_timeout_seconds: Option<u64>,
    /// Bulk wallet-projection construction tuning.
    #[serde(default, skip_serializing_if = "ProjectorBuildSection::is_empty")]
    pub build: ProjectorBuildSection,
    /// Incremental wallet-projection follower tuning.
    #[serde(default, skip_serializing_if = "ProjectorFollowSection::is_empty")]
    pub follow: ProjectorFollowSection,
}

impl ProjectorSection {
    /// Reports whether every field and subsection in `[projector]` is unset.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.reorg_window_blocks.is_none()
            && self.build_owner_hex.is_none()
            && self.lease_duration_seconds.is_none()
            && self.fence_convergence_timeout_seconds.is_none()
            && self.build.is_empty()
            && self.follow.is_empty()
    }
}

/// Raw `[projector.build]` configuration subsection.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProjectorBuildSection {
    /// Maximum in-memory bytes for primary outpoint sorting.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_outpoint_sort_memory_bytes: Option<u64>,
    /// Maximum in-memory bytes per secondary sorter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_secondary_sort_memory_bytes_per_sorter: Option<u64>,
    /// Maximum temporary spill file bytes per sorter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_temporary_file_bytes_per_sorter: Option<u64>,
    /// Target logical byte size per emitted SST file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sst_target_logical_bytes: Option<u64>,
    /// Maximum accounted byte size of reorg undo state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_accounted_reorg_undo_bytes: Option<u64>,
    /// Worker concurrency for accumulator hashing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accumulator_hashing_workers: Option<u32>,
}

impl ProjectorBuildSection {
    /// Reports whether every field is unset.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.max_outpoint_sort_memory_bytes.is_none()
            && self.max_secondary_sort_memory_bytes_per_sorter.is_none()
            && self.max_temporary_file_bytes_per_sorter.is_none()
            && self.sst_target_logical_bytes.is_none()
            && self.max_accounted_reorg_undo_bytes.is_none()
            && self.accumulator_hashing_workers.is_none()
    }
}

/// Raw `[projector.follow]` configuration subsection.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProjectorFollowSection {
    /// Maximum logical bytes processed in a single incremental transition.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_transition_logical_bytes: Option<u64>,
}

impl ProjectorFollowSection {
    /// Reports whether every field is unset.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.max_transition_logical_bytes.is_none()
    }
}

/// Raw `[query]` configuration section.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct QuerySection {
    /// Socket address on which `WalletQuery` serves gRPC.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub listen_addr: Option<String>,
    /// CORS origin allowlist for browser gRPC-Web clients (`"none"` or `[]` disables).
    #[serde(default)]
    pub browser_origins: BrowserOrigins,
    /// Expected canonical store reorg window in blocks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reorg_window_blocks: Option<u32>,
    /// Convergence retry budget across canonical/wallet serving pairs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pair_convergence_attempts: Option<u8>,
    /// Seconds an exposed epoch continues honoring pins after replacement (`60..=300`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch_pin_retention_seconds: Option<u64>,
    /// Maximum milliseconds a tree-state query waits for background replay.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tree_state_replay_wait_ms: Option<u64>,
    /// Maximum concurrent waiters per tree-state replay slot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tree_state_replay_waiters_per_slot: Option<u32>,
    /// Maximum concurrent reads forwarded to upstream node.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub forwarded_read_max_concurrent: Option<u32>,
    /// Maximum milliseconds a forwarded read waits for an upstream slot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub forwarded_read_wait_ms: Option<u64>,
    /// Maximum queued waiters per forwarded-read slot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub forwarded_read_waiters_per_slot: Option<u32>,
    /// Byte capacity for caching forwarded raw transactions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub forwarded_transaction_cache_bytes: Option<u64>,
    /// Whether HTTP proxy headers supply the client address for admission.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trust_proxy_client_address: Option<bool>,
    /// Trusted PROXY-protocol v1/v2 source CIDR allowlist (`"none"` to disable).
    #[serde(default, skip_serializing_if = "RedactedCidrs::is_defaulted")]
    pub proxy_protocol_trusted_cidrs: RedactedCidrs,
    /// Block-range admission limits.
    #[serde(default)]
    pub range_admission: GrpcRangeAdmission,
}

impl QuerySection {
    /// Reports whether every field in `[query]` equals its omitted default.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.listen_addr.is_none()
            && self.browser_origins.is_empty()
            && self.reorg_window_blocks.is_none()
            && self.pair_convergence_attempts.is_none()
            && self.epoch_pin_retention_seconds.is_none()
            && self.tree_state_replay_wait_ms.is_none()
            && self.tree_state_replay_waiters_per_slot.is_none()
            && self.forwarded_read_max_concurrent.is_none()
            && self.forwarded_read_wait_ms.is_none()
            && self.forwarded_read_waiters_per_slot.is_none()
            && self.forwarded_transaction_cache_bytes.is_none()
            && self.trust_proxy_client_address.is_none()
            && self.proxy_protocol_trusted_cidrs.is_defaulted()
            && self.range_admission.is_default()
    }
}

/// Serving contract mode selectable on `zinder-compat-lightwalletd`.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum CompatServing {
    /// Full wallet-compatible serving over an admitted canonical and wallet pair.
    #[default]
    Wallet,
    /// Canonical compact-block serving without wallet storage.
    CompactBlocks,
    /// Canonical wallet reads with node-backed transparent methods.
    LightWallet,
}

impl CompatServing {
    /// Returns the kebab-case configuration representation of this serving mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Wallet => "wallet",
            Self::CompactBlocks => "compact-blocks",
            Self::LightWallet => "light-wallet",
        }
    }
}

/// Raw `[compat]` configuration section.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CompatSection {
    /// Socket address on which `CompactTxStreamer` serves gRPC / gRPC-Web.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub listen_addr: Option<String>,
    /// CORS origin allowlist for browser gRPC-Web clients (`"none"` or `[]` disables).
    #[serde(default)]
    pub browser_origins: BrowserOrigins,
    /// Expected canonical store reorg window in blocks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reorg_window_blocks: Option<u32>,
    /// Convergence retry budget across canonical/wallet serving pairs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pair_convergence_attempts: Option<u8>,
    /// Maximum concurrent block-range streams open globally.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrent_block_range_streams: Option<u32>,
    /// Maximum milliseconds a tree-state call waits for background replay.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tree_state_replay_wait_ms: Option<u64>,
    /// Maximum concurrent waiters per tree-state replay slot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tree_state_replay_waiters_per_slot: Option<u32>,
    /// Maximum concurrent reads forwarded to upstream node.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub forwarded_read_max_concurrent: Option<u32>,
    /// Maximum milliseconds a forwarded read waits for an upstream slot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub forwarded_read_wait_ms: Option<u64>,
    /// Maximum queued waiters per forwarded-read slot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub forwarded_read_waiters_per_slot: Option<u32>,
    /// Byte capacity for caching forwarded raw transactions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub forwarded_transaction_cache_bytes: Option<u64>,
    /// Maximum age in milliseconds of an unrefreshed tip before readiness degrades.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale_tip_limit_ms: Option<u64>,
    /// Maximum time in milliseconds a waiting block-notification stream sleeps on tip advance.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tip_wait_ms: Option<u64>,
    /// Maximum block advance per waited tip notification.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tip_wait_max_blocks: Option<u32>,
    /// Maximum concurrent streams waiting on tip advance.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tip_wait_max_waiters: Option<u32>,
    /// Explicit acknowledgment required to enable `Ping` when enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ping_very_insecure: Option<bool>,
    /// Serving contract mode (`wallet`, `compact-blocks`, or `light-wallet`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serving: Option<CompatServing>,
    /// Whether HTTP proxy headers supply the client address for admission.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trust_proxy_client_address: Option<bool>,
    /// Trusted PROXY-protocol v1/v2 source CIDR allowlist (`"none"` to disable).
    #[serde(default, skip_serializing_if = "RedactedCidrs::is_defaulted")]
    pub proxy_protocol_trusted_cidrs: RedactedCidrs,
    /// Block-range admission limits.
    #[serde(default)]
    pub range_admission: GrpcRangeAdmission,
}

impl CompatSection {
    /// Reports whether every field in `[compat]` equals its omitted default.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.listen_addr.is_none()
            && self.browser_origins.is_empty()
            && self.reorg_window_blocks.is_none()
            && self.pair_convergence_attempts.is_none()
            && self.max_concurrent_block_range_streams.is_none()
            && self.tree_state_replay_wait_ms.is_none()
            && self.tree_state_replay_waiters_per_slot.is_none()
            && self.forwarded_read_max_concurrent.is_none()
            && self.forwarded_read_wait_ms.is_none()
            && self.forwarded_read_waiters_per_slot.is_none()
            && self.forwarded_transaction_cache_bytes.is_none()
            && self.stale_tip_limit_ms.is_none()
            && self.tip_wait_ms.is_none()
            && self.tip_wait_max_blocks.is_none()
            && self.tip_wait_max_waiters.is_none()
            && self.ping_very_insecure.is_none()
            && self.serving.is_none()
            && self.trust_proxy_client_address.is_none()
            && self.proxy_protocol_trusted_cidrs.is_defaulted()
            && self.range_admission.is_default()
    }
}

/// Raw `[explorer]` configuration section.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExplorerSection {
    /// Socket address on which `ExplorerQuery` serves gRPC.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub listen_addr: Option<String>,
    /// CORS origin allowlist for browser gRPC-Web clients (`"none"` or `[]` disables).
    #[serde(default)]
    pub browser_origins: BrowserOrigins,
    /// File path containing the bearer token sent on `WalletQuery` calls.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bearer_token_path: Option<PathBuf>,
    /// Inline bearer token sent on `WalletQuery` calls (mutually exclusive with `bearer_token_path`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bearer_token: Option<SecretInput>,
    /// Optional `WalletQuery` endpoint URL (`"none"` disables forwarded wallet lookups).
    #[serde(default, skip_serializing_if = "Toggle::is_defaulted")]
    pub wallet_query_endpoint: Toggle<RedactedUrl>,
    /// Whether HTTP proxy headers supply the client address for admission.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trust_proxy_client_address: Option<bool>,
}

impl ExplorerSection {
    /// Reports whether every field in `[explorer]` is unset.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.listen_addr.is_none()
            && self.browser_origins.is_empty()
            && self.bearer_token_path.is_none()
            && self.bearer_token.is_none()
            && self.wallet_query_endpoint.is_defaulted()
            && self.trust_proxy_client_address.is_none()
    }
}

/// Raw `[cipherscan]` configuration section.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CipherscanSection {
    /// Socket address on which Cipherscan HTTP REST / WebSocket routes serve.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub listen_addr: Option<String>,
    /// Upstream `ExplorerQuery` gRPC endpoint URL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explorer_query_endpoint: Option<RedactedUrl>,
    /// Upstream `WalletQuery` gRPC endpoint URL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wallet_query_endpoint: Option<RedactedUrl>,
    /// Upstream HTTP endpoint URL for spot market prices.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_price_endpoint: Option<RedactedUrl>,
    /// Upstream HTTP endpoint URL template for historical market prices.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub historical_price_endpoint_template: Option<RedactedUrl>,
    /// File path containing the bearer token for upstream gRPC calls.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bearer_token_path: Option<PathBuf>,
    /// Inline bearer token for upstream gRPC calls (mutually exclusive with `bearer_token_path`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bearer_token: Option<SecretInput>,
}

impl CipherscanSection {
    /// Reports whether every field in `[cipherscan]` is unset.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.listen_addr.is_none()
            && self.explorer_query_endpoint.is_none()
            && self.wallet_query_endpoint.is_none()
            && self.current_price_endpoint.is_none()
            && self.historical_price_endpoint_template.is_none()
            && self.bearer_token_path.is_none()
            && self.bearer_token.is_none()
    }
}
