//! Historical best-chain blocks fetched from Zebra's binary indexer RPC.
//!
//! This source replaces only raw block retrieval. Atomic tip observation,
//! tree checkpoints, subtree roots, value pools, and health stay on the
//! configured [`ZebraJsonRpcSource`], because Zebra's indexer protocol does
//! not expose those canonical-construction facts. A gRPC failure never falls
//! back to JSON-RPC block reads.

use std::{
    collections::BTreeMap,
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures_util::{StreamExt, stream::FuturesUnordered};
use prost::Message;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::{Code, Request, Status, transport::Channel};
use zinder_core::{
    BlockHeight, BlockId, BlockValuePoolBalances, ChainValuePools, CommitmentTreeCheckpoint,
    Network, NetworkUpgradeActivations, ShieldedProtocol, SubtreeRootIndex, SubtreeRootRange,
};
use zinder_proto::external::zebra_indexer_rpc::{
    BlockAndHash, BlockRequest, indexer_client::IndexerClient,
};

use crate::{
    MempoolSource, MempoolSourceAdmissionLimits, MempoolSourceFactory, NodeCapabilities,
    NodeCapability, NodeSource, NodeStorageMode, NodeTarget, SourceBlock, SourceChainSegment,
    SourceChainSegmentLimits, SourceChainSegmentStats, SourceChainUpdate, SourceError,
    SourceSubtreeRoots, UpstreamHealthSnapshot, ZebraIndexerChannelOptions,
    ZebraIndexerSourceTarget, ZebraJsonRpcSource, connect_zebra_indexer_channel,
    connect_zebra_indexer_channel_lazy,
    source_chain_update::SourceChainCursorPosition,
    zebra_json_rpc::{is_unreadable_stored_block_refusal, validate_source_block_links},
};

const ZEBRA_INDEXER_BLOCK_SOURCE_LABEL: &str = "zebra_indexer_grpc";
const GET_BLOCK_METHOD_LABEL: &str = "get_block";

/// Runtime options for [`ZebraIndexerBlockSource`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ZebraIndexerBlockSourceOptions {
    /// Deadline for establishing the shared HTTP/2 channel.
    pub connect_timeout: Duration,
    /// Per-unary-request deadline.
    pub request_timeout: Duration,
    /// Global actual unary request limit shared by every source clone.
    pub max_in_flight_requests: NonZeroU32,
}

impl Default for ZebraIndexerBlockSourceOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            max_in_flight_requests: NonZeroU32::new(12).unwrap_or(NonZeroU32::MIN),
        }
    }
}

/// Zebra source that fetches raw blocks through unary binary gRPC.
///
/// The JSON-RPC source is an explicit control-plane dependency, not a block
/// fallback. The shared semaphore bounds actual `GetBlock` calls even when
/// canonical construction runs several source-segment requests concurrently.
#[derive(Clone)]
pub struct ZebraIndexerBlockSource {
    network: Network,
    control_plane: ZebraJsonRpcSource,
    channel: Channel,
    request_permits: Arc<Semaphore>,
    queued_requests: Arc<AtomicU64>,
    active_requests: Arc<AtomicU64>,
}

impl ZebraIndexerBlockSource {
    /// Connects eagerly to the indexer endpoint paired with `control_plane`.
    pub async fn connect(
        target: ZebraIndexerSourceTarget,
        control_plane: ZebraJsonRpcSource,
        options: ZebraIndexerBlockSourceOptions,
    ) -> Result<Self, SourceError> {
        let channel = connect_zebra_indexer_channel(
            &target,
            ZebraIndexerChannelOptions {
                connect_timeout: options.connect_timeout,
                request_timeout: options.request_timeout,
            },
        )
        .await
        .map_err(|error| SourceError::NodeUnavailable {
            reason: format!("Zebra indexer GetBlock channel could not connect: {error}"),
        })?;
        Ok(Self::with_channel(control_plane, channel, options))
    }

    /// Builds a lazily connecting indexer source paired with `control_plane`.
    ///
    /// Validates the endpoint URL and TLS material synchronously while
    /// deferring the HTTP/2 connection until the first request.
    pub fn connect_lazy(
        target: &ZebraIndexerSourceTarget,
        control_plane: ZebraJsonRpcSource,
        options: ZebraIndexerBlockSourceOptions,
    ) -> Result<Self, SourceError> {
        let channel = connect_zebra_indexer_channel_lazy(
            target,
            ZebraIndexerChannelOptions {
                connect_timeout: options.connect_timeout,
                request_timeout: options.request_timeout,
            },
        )
        .map_err(|error| SourceError::NodeUnavailable {
            reason: format!("Zebra indexer GetBlock channel could not be configured: {error}"),
        })?;
        Ok(Self::with_channel(control_plane, channel, options))
    }

    /// Builds an indexer block source and its JSON-RPC control plane from one
    /// resolved [`NodeTarget`].
    pub fn from_node_target_lazy(target: &NodeTarget) -> Result<Self, SourceError> {
        let control_plane = ZebraJsonRpcSource::from_node_target(target)?;
        let Some(indexer_target) = ZebraIndexerSourceTarget::from_node_target(target) else {
            return Err(SourceError::NodeUnavailable {
                reason: "node.indexer_grpc_addr is required for zebra-indexer-grpc source"
                    .to_owned(),
            });
        };
        let options = ZebraIndexerBlockSourceOptions {
            connect_timeout: Self::default_connect_timeout(),
            request_timeout: target.request_timeout,
            max_in_flight_requests: target.max_concurrent_requests,
        };
        Self::connect_lazy(&indexer_target, control_plane, options)
    }

    fn default_connect_timeout() -> Duration {
        ZebraIndexerBlockSourceOptions::default().connect_timeout
    }

    fn with_channel(
        control_plane: ZebraJsonRpcSource,
        channel: Channel,
        options: ZebraIndexerBlockSourceOptions,
    ) -> Self {
        let max_in_flight_requests =
            usize::try_from(options.max_in_flight_requests.get()).unwrap_or(usize::MAX);
        Self {
            network: control_plane.network(),
            control_plane,
            channel,
            request_permits: Arc::new(Semaphore::new(max_in_flight_requests)),
            queued_requests: Arc::new(AtomicU64::new(0)),
            active_requests: Arc::new(AtomicU64::new(0)),
        }
    }

    #[allow(
        clippy::significant_drop_tightening,
        reason = "the owned semaphore permit intentionally spans the complete unary HTTP/2 request"
    )]
    async fn fetch_indexer_block_at(
        &self,
        height: BlockHeight,
    ) -> Result<SourceBlock, SourceError> {
        let queued_request = QueuedUnaryRequest::new(
            Arc::clone(&self.queued_requests),
            ZEBRA_INDEXER_BLOCK_SOURCE_LABEL,
            GET_BLOCK_METHOD_LABEL,
        );
        let wait_started_at = Instant::now();
        let permit = Arc::clone(&self.request_permits)
            .acquire_owned()
            .await
            .map_err(|_| SourceError::NodeUnavailable {
                reason: "Zebra indexer GetBlock request limiter closed".to_owned(),
            })?;
        drop(queued_request);
        metrics::histogram!(
            "zinder_node_request_admission_wait_seconds",
            "source" => ZEBRA_INDEXER_BLOCK_SOURCE_LABEL,
            "method" => GET_BLOCK_METHOD_LABEL
        )
        .record(wait_started_at.elapsed());
        let active_request = ActiveUnaryRequest::new(
            permit,
            Arc::clone(&self.active_requests),
            ZEBRA_INDEXER_BLOCK_SOURCE_LABEL,
            GET_BLOCK_METHOD_LABEL,
        );

        let request_started_at = Instant::now();
        let mut client = IndexerClient::new(self.channel.clone());
        let response = client
            .get_block(Request::new(BlockRequest {
                hash_or_height: height.value().to_be_bytes().to_vec(),
            }))
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| map_get_block_status(height, &status));
        drop(active_request);
        record_get_block_outcome(request_started_at, &response);
        let response = response?;
        record_response_bytes(&response);

        let header_started_at = Instant::now();
        let block = SourceBlock::from_raw_block_bytes(self.network, height, response.data)?;
        metrics::histogram!(
            "zinder_node_block_decode_stage_duration_seconds",
            "source" => ZEBRA_INDEXER_BLOCK_SOURCE_LABEL,
            "stage" => "block_header"
        )
        .record(header_started_at.elapsed());
        let response_hash = zinder_core::wire::decode_rpc_block_hash_bytes(&response.hash)
            .map_err(|_| SourceError::SourceProtocolMismatch {
                reason: "Zebra indexer GetBlock hash was not 32 display-order bytes",
            })?;
        if response_hash != block.hash {
            return Err(SourceError::SourceProtocolMismatch {
                reason: "Zebra indexer GetBlock hash differs from the raw block header",
            });
        }
        Ok(block)
    }

    async fn fetch_indexer_block_range(
        &self,
        start_height: BlockHeight,
        end_height: BlockHeight,
        max_response_bytes: u64,
    ) -> Result<(Vec<SourceBlock>, SourceChainSegmentStats), SourceError> {
        let mut in_flight = FuturesUnordered::new();
        for height_value in start_height.value()..=end_height.value() {
            let height = BlockHeight::new(height_value);
            in_flight.push(async move { (height, self.fetch_indexer_block_at(height).await) });
        }
        let mut blocks_by_height = BTreeMap::new();
        while let Some((height, outcome)) = in_flight.next().await {
            blocks_by_height.insert(height.value(), outcome?);
        }
        let blocks = blocks_by_height.into_values().collect::<Vec<_>>();
        let response_payload_bytes = blocks.iter().fold(0_u64, |bytes, block| {
            bytes.saturating_add(u64::try_from(block.raw_block_bytes.len()).unwrap_or(u64::MAX))
        });
        if blocks.iter().any(|block| {
            u64::try_from(block.raw_block_bytes.len()).unwrap_or(u64::MAX) > max_response_bytes
        }) {
            return Err(SourceError::SourceResponseTooLarge {
                operation: "indexer_get_block_segment",
                max_response_bytes,
            });
        }
        let stats = SourceChainSegmentStats::from_response_payload_bytes(response_payload_bytes)
            .with_connected_blocks(blocks.len());
        Ok((blocks, stats))
    }

    async fn fetch_settled_segment_without_tip_poll(
        &self,
        limits: SourceChainSegmentLimits,
        range: SettledIndexerRange,
    ) -> Result<SourceChainSegment, SourceError> {
        let (blocks, stats) = match self
            .fetch_indexer_block_range(
                range.start_height,
                range.end_height,
                limits.max_response_bytes,
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(error)
                if matches!(
                    error,
                    SourceError::BlockUnavailable { .. }
                        | SourceError::HistoricalBlockBodyUnavailable { .. }
                ) =>
            {
                let observed_tip_id = self.control_plane.tip_id().await?;
                if observed_tip_id.height < range.end_height {
                    return self
                        .fetch_chain_segment_from_observed_tip(limits, observed_tip_id)
                        .await;
                }
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        if let (Some(expected_parent_id), Some(first_block)) =
            (range.expected_parent_id, blocks.first())
            && first_block.parent_hash != expected_parent_id.hash
        {
            let _observed_tip_id = self.control_plane.tip_id().await?;
            return Ok(SourceChainSegment::new([
                SourceChainUpdate::reverted_block(expected_parent_id),
            ]));
        }
        if let Err(error) = validate_source_block_links(&blocks) {
            let _observed_tip_id = self.control_plane.tip_id().await?;
            return Err(error);
        }
        Ok(SourceChainSegment::connected_blocks_with_stats(
            blocks, stats,
        ))
    }

    async fn fetch_chain_segment_from_observed_tip(
        &self,
        limits: SourceChainSegmentLimits,
        observed_tip_id: BlockId,
    ) -> Result<SourceChainSegment, SourceError> {
        let (start_height, expected_parent_id) = match limits.cursor.position() {
            SourceChainCursorPosition::BeforeHeight(height) => (height, None),
            SourceChainCursorPosition::AtBlock(block_id) => {
                if observed_tip_id.height < block_id.height
                    || (observed_tip_id.height == block_id.height
                        && observed_tip_id.hash != block_id.hash)
                {
                    return Ok(SourceChainSegment::new([
                        SourceChainUpdate::reverted_block(block_id),
                    ]));
                }
                if observed_tip_id.height == block_id.height {
                    return Ok(SourceChainSegment::default());
                }
                let Some(next_height) = block_id.height.next() else {
                    return Ok(SourceChainSegment::default());
                };
                (next_height, Some(block_id))
            }
        };
        if observed_tip_id.height < start_height {
            return Ok(SourceChainSegment::default());
        }
        let end_height = BlockHeight::new(
            start_height
                .value()
                .saturating_add(limits.max_connected_blocks.get().saturating_sub(1))
                .min(observed_tip_id.height.value()),
        );
        let (blocks, stats) = self
            .fetch_indexer_block_range(start_height, end_height, limits.max_response_bytes)
            .await?;
        if let (Some(expected_parent_id), Some(first_block)) = (expected_parent_id, blocks.first())
            && first_block.parent_hash != expected_parent_id.hash
        {
            return Ok(SourceChainSegment::new([
                SourceChainUpdate::reverted_block(expected_parent_id),
            ]));
        }
        validate_source_block_links(&blocks)?;
        Ok(SourceChainSegment::connected_blocks_with_stats(
            blocks, stats,
        ))
    }
}

impl MempoolSourceFactory for ZebraIndexerBlockSource {
    fn mempool_source(
        &self,
        admission_limits: MempoolSourceAdmissionLimits,
    ) -> Arc<dyn MempoolSource> {
        self.control_plane.mempool_source(admission_limits)
    }
}

#[async_trait]
impl NodeSource for ZebraIndexerBlockSource {
    fn capabilities(&self) -> NodeCapabilities {
        self.control_plane.capabilities()
    }

    async fn probe_capabilities(&self) -> Result<NodeCapabilities, SourceError> {
        self.control_plane.probe_capabilities().await
    }

    async fn fetch_network_upgrade_activations(
        &self,
    ) -> Result<NetworkUpgradeActivations, SourceError> {
        self.control_plane.fetch_network_upgrade_activations().await
    }

    async fn fetch_storage_mode(&self) -> Result<NodeStorageMode, SourceError> {
        self.control_plane.fetch_storage_mode().await
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        self.fetch_indexer_block_at(height).await
    }

    async fn fetch_chain_segment(
        &self,
        limits: SourceChainSegmentLimits,
    ) -> Result<SourceChainSegment, SourceError> {
        if let Some(range) = settled_indexer_segment_bounds_without_tip_poll(limits) {
            return self
                .fetch_settled_segment_without_tip_poll(limits, range)
                .await;
        }
        let observed_tip_id = self.control_plane.tip_id().await?;
        self.fetch_chain_segment_from_observed_tip(limits, observed_tip_id)
            .await
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        self.control_plane
            .fetch_chain_checkpoint(height, activations)
            .await
    }

    async fn fetch_chain_checkpoint_for_block(
        &self,
        block_id: BlockId,
        activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        self.control_plane
            .fetch_chain_checkpoint_for_block(block_id, activations)
            .await
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        self.control_plane.tip_id().await
    }

    async fn probe_tip_id(&self) -> Result<BlockId, SourceError> {
        self.control_plane.probe_tip_id().await
    }

    async fn fetch_subtree_roots(
        &self,
        protocol: ShieldedProtocol,
        start_index: SubtreeRootIndex,
        max_entries: NonZeroU32,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        self.control_plane
            .fetch_subtree_roots(protocol, start_index, max_entries)
            .await
    }

    async fn fetch_subtree_root_range(
        &self,
        range: SubtreeRootRange,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        self.control_plane.fetch_subtree_root_range(range).await
    }

    async fn fetch_chain_value_pools_at_tip(&self) -> Result<ChainValuePools, SourceError> {
        self.control_plane.fetch_chain_value_pools_at_tip().await
    }

    async fn fetch_block_value_pool_balances(
        &self,
        block_id: BlockId,
    ) -> Result<BlockValuePoolBalances, SourceError> {
        self.control_plane
            .fetch_block_value_pool_balances(block_id)
            .await
    }

    async fn poll_upstream_health(&self) -> Result<UpstreamHealthSnapshot, SourceError> {
        self.control_plane.poll_upstream_health().await
    }
}

#[derive(Clone, Copy)]
struct SettledIndexerRange {
    start_height: BlockHeight,
    expected_parent_id: Option<BlockId>,
    end_height: BlockHeight,
}

fn settled_indexer_segment_bounds_without_tip_poll(
    limits: SourceChainSegmentLimits,
) -> Option<SettledIndexerRange> {
    let known_tip = limits.known_tip?;
    let (start_height, expected_parent_id) = match limits.cursor.position() {
        SourceChainCursorPosition::BeforeHeight(height) => (height, None),
        SourceChainCursorPosition::AtBlock(block_id) => (block_id.height.next()?, Some(block_id)),
    };
    let settled_upper_bound =
        known_tip.settled_upper_bound_below_window(start_height, limits.max_connected_blocks)?;
    let end_height = BlockHeight::new(
        start_height
            .value()
            .saturating_add(limits.max_connected_blocks.get().saturating_sub(1))
            .min(settled_upper_bound.value()),
    );
    Some(SettledIndexerRange {
        start_height,
        expected_parent_id,
        end_height,
    })
}

struct ActiveUnaryRequest {
    _permit: OwnedSemaphorePermit,
    active_requests: Arc<AtomicU64>,
    source: &'static str,
    method: &'static str,
}

struct QueuedUnaryRequest {
    queued_requests: Arc<AtomicU64>,
    source: &'static str,
    method: &'static str,
}

impl QueuedUnaryRequest {
    fn new(queued_requests: Arc<AtomicU64>, source: &'static str, method: &'static str) -> Self {
        let queued = queued_requests.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::gauge!("zinder_node_queued_requests", "source" => source, "method" => method)
            .set(u64_to_f64(queued));
        Self {
            queued_requests,
            source,
            method,
        }
    }
}

impl Drop for QueuedUnaryRequest {
    fn drop(&mut self) {
        let queued = self
            .queued_requests
            .fetch_sub(1, Ordering::Relaxed)
            .saturating_sub(1);
        metrics::gauge!(
            "zinder_node_queued_requests",
            "source" => self.source,
            "method" => self.method
        )
        .set(u64_to_f64(queued));
    }
}

impl ActiveUnaryRequest {
    fn new(
        permit: OwnedSemaphorePermit,
        active_requests: Arc<AtomicU64>,
        source: &'static str,
        method: &'static str,
    ) -> Self {
        let active = active_requests.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::gauge!("zinder_node_active_requests", "source" => source, "method" => method)
            .set(u64_to_f64(active));
        Self {
            _permit: permit,
            active_requests,
            source,
            method,
        }
    }
}

impl Drop for ActiveUnaryRequest {
    fn drop(&mut self) {
        let active = self
            .active_requests
            .fetch_sub(1, Ordering::Relaxed)
            .saturating_sub(1);
        metrics::gauge!(
            "zinder_node_active_requests",
            "source" => self.source,
            "method" => self.method
        )
        .set(u64_to_f64(active));
    }
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "future gRPC status codes are conservatively retryable node-unavailable failures"
)]
fn map_get_block_status(height: BlockHeight, status: &Status) -> SourceError {
    match status.code() {
        Code::NotFound => SourceError::BlockUnavailable {
            height,
            reason: status.message().to_owned(),
        },
        Code::Unimplemented => SourceError::NodeCapabilityMissing {
            capability: NodeCapability::BestChainBlocks,
        },
        Code::InvalidArgument | Code::DataLoss => SourceError::SourceProtocolMismatch {
            reason: "Zebra indexer rejected a valid height-keyed GetBlock request",
        },
        // Zebra answers a block-read refusal with `Unavailable`, the code a
        // transport failure also carries, so only the refusal text tells the
        // two apart.
        Code::Unavailable if is_unreadable_stored_block_refusal(status.message()) => {
            SourceError::HistoricalBlockBodyUnavailable {
                height,
                reason: status.message().to_owned(),
            }
        }
        _ => SourceError::NodeUnavailable {
            reason: format!("Zebra indexer GetBlock failed: {status}"),
        },
    }
}

fn record_get_block_outcome(started_at: Instant, outcome: &Result<BlockAndHash, SourceError>) {
    let (status, error_class) = outcome.as_ref().map_or_else(
        |error| ("error", error.upstream_classification().label()),
        |_| ("ok", "none"),
    );
    metrics::histogram!(
        "zinder_node_request_duration_seconds",
        "source" => ZEBRA_INDEXER_BLOCK_SOURCE_LABEL,
        "method" => GET_BLOCK_METHOD_LABEL,
        "status" => status,
        "error_class" => error_class
    )
    .record(started_at.elapsed());
    metrics::counter!(
        "zinder_node_request_total",
        "source" => ZEBRA_INDEXER_BLOCK_SOURCE_LABEL,
        "method" => GET_BLOCK_METHOD_LABEL,
        "status" => status,
        "error_class" => error_class
    )
    .increment(1);
}

fn record_response_bytes(response: &BlockAndHash) {
    metrics::counter!(
        "zinder_node_response_bytes_total",
        "source" => ZEBRA_INDEXER_BLOCK_SOURCE_LABEL,
        "method" => GET_BLOCK_METHOD_LABEL,
        "encoding" => "raw_block"
    )
    .increment(u64::try_from(response.data.len()).unwrap_or(u64::MAX));
    metrics::counter!(
        "zinder_node_response_bytes_total",
        "source" => ZEBRA_INDEXER_BLOCK_SOURCE_LABEL,
        "method" => GET_BLOCK_METHOD_LABEL,
        "encoding" => "protobuf_message"
    )
    .increment(u64::try_from(response.encoded_len()).unwrap_or(u64::MAX));
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Prometheus gauges use f64 samples; request counts are diagnostic magnitudes"
)]
fn u64_to_f64(sample: u64) -> f64 {
    sample as f64
}
