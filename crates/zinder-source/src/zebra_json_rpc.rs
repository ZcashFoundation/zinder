//! Zebra JSON-RPC source adapter.

use std::collections::HashSet;
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use jsonrpsee::core::ClientError;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::core::params::{ArrayParams, BatchRequestBuilder};
use jsonrpsee::http_client::{HeaderMap, HeaderValue, HttpClient};
use jsonrpsee::types::{ErrorObject, ErrorObjectOwned};
use parking_lot::Mutex;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use serde_json::{Map, Value};
use zinder_core::{
    BlockFinalNoteCommitmentRoots, BlockHash, BlockHeight, BlockId, BlockValuePoolBalances,
    BroadcastAccepted, BroadcastDuplicate, BroadcastInvalidEncoding, BroadcastQueued,
    BroadcastRejected, BroadcastRejectionReason, BroadcastUnknown, ChainValuePool, ChainValuePools,
    CommitmentTreeCheckpoint, CommitmentTreeFrontier, CommitmentTreeFrontierValidationError,
    CommitmentTreeFrontiers, ConsensusBranchId, FinalNoteCommitmentRoot, Network,
    NetworkUpgradeActivation, NetworkUpgradeActivations, RawTransactionBytes, ShieldedProtocol,
    SubtreeRootHash, SubtreeRootIndex, SubtreeRootRange, TransactionBroadcastOutcome,
    TransactionId, ValuePoolBalance,
};

use crate::{
    CookieSource, CookieSourceError, NodeAuth, NodeCapabilities, NodeCapability, NodeHealthConfig,
    NodeSource, ResilientClient, SourceBlock, SourceChainCursor, SourceChainSegment,
    SourceChainSegmentLimits, SourceChainSegmentStats, SourceChainUpdate, SourceError,
    SourceSubtreeRoot, SourceSubtreeRoots, SourceTreeState, TransactionBroadcaster,
    TreeStateUpstream, UPSTREAM_HEALTH_REASON_ESTIMATED_GAP_ABOVE_FLOOR,
    UPSTREAM_HEALTH_REASON_VERIFICATION_PROGRESS_BELOW_FLOOR,
    UPSTREAM_HEALTH_SOURCE_VERIFICATION_PROGRESS_FALLBACK, UpstreamHealthSnapshot,
    ZEBRA_REBUILD_THRESHOLD, decode_rpc_block_hash, encode_rpc_block_hash,
    source_block::wire_error_to_transaction_id_error,
    source_chain_update::SourceChainCursorPosition, zebra_ready_endpoint::ZebraReadyClient,
};

/// Result of looking up a transaction at the upstream node.
///
/// Used by the polling mempool source to classify a txid that disappeared
/// from successive `getrawmempool` snapshots: a txid still present but
/// reported as mined produces a [`crate::MempoolSourceEvent::Mined`]; a
/// txid that the node no longer recognizes produces
/// [`crate::MempoolSourceEvent::Invalidated`] with reason
/// [`zinder_core::MempoolEvictionReason::Unknown`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum UpstreamTransactionLookup {
    /// Transaction is still visible in the upstream mempool.
    InMempool,
    /// Transaction was mined into a block on the upstream best chain.
    Mined {
        /// Height at which the upstream node reports the transaction is mined.
        mined_height: BlockHeight,
        /// Hash of the block that mined the transaction, as reported by the
        /// upstream node.
        block_hash: BlockHash,
    },
    /// Transaction is not in the upstream mempool or main chain.
    NotFound,
}

/// Default capability set assumed for Zebra JSON-RPC sources.
///
/// Used until [`ZebraJsonRpcSource::probe_capabilities`] runs. Operators
/// should treat this as a baseline; the probed value is the source of truth
/// at runtime.
fn default_zebra_capabilities() -> NodeCapabilities {
    NodeCapabilities::from_trusted([
        NodeCapability::JsonRpc,
        NodeCapability::BestChainBlocks,
        NodeCapability::SourceChainSegments,
        NodeCapability::TipId,
        NodeCapability::TreeState,
        NodeCapability::SubtreeRoots,
        NodeCapability::TransactionBroadcast,
        NodeCapability::ChainValuePools,
        NodeCapability::BlockValuePoolBalances,
    ])
}

/// Returns `capabilities` with [`NodeCapability::ReadinessProbe`] added when
/// `enabled` is true, or with it cleared otherwise.
///
/// Operators opt in to the probe by setting `[node.health].addr`; sources
/// without that configuration must not advertise the capability because
/// the background probe is the only mechanism that exercises it.
fn with_readiness_probe_capability(
    capabilities: NodeCapabilities,
    enabled: bool,
) -> NodeCapabilities {
    let mut entries: Vec<NodeCapability> = capabilities
        .iter()
        .filter(|capability| *capability != NodeCapability::ReadinessProbe)
        .collect();
    if enabled {
        entries.push(NodeCapability::ReadinessProbe);
    }
    NodeCapabilities::from_trusted(entries)
}

/// Default maximum JSON-RPC response body size.
pub const DEFAULT_MAX_JSON_RPC_RESPONSE_BYTES: NonZeroU64 =
    NonZeroU64::MIN.saturating_add((64 * 1024 * 1024) - 1);

/// JSON-RPC error code returned for invalid transaction encodings.
const JSON_RPC_INVALID_ENCODING_CODE: i32 = -22;
/// JSON-RPC error code returned for transactions already in the mempool.
const JSON_RPC_DUPLICATE_TRANSACTION_CODE: i32 = -27;
/// JSON-RPC error code returned when a txid is not in mempool or main chain.
const JSON_RPC_INVALID_ADDRESS_OR_KEY_CODE: i32 = -5;
/// JSON-RPC error code for general transaction verification failures.
///
/// Zebra collapses every mempool rejection into this code; the
/// `classify_broadcast_error` unit tests use it as the call code.
#[cfg(test)]
const JSON_RPC_VERIFY_CODE: i32 = -25;

/// Peer label for this source's [`ResilientClient`].
///
/// Surfaces on `zinder_transport_reconnect_total{peer}` and on
/// `zinder::transport` log events. Matches the `source` label used by
/// per-RPC metrics so operators can correlate the two.
const ZEBRA_JSON_RPC_PEER_LABEL: &str = "zebra_json_rpc";

/// Node source backed by Zebra's JSON-RPC API.
#[derive(Clone)]
pub struct ZebraJsonRpcSource {
    network: Network,
    client: ResilientClient<HttpClient>,
    max_response_bytes: NonZeroU64,
    broadcast_timeout: Option<Duration>,
    cached_capabilities: Arc<Mutex<NodeCapabilities>>,
    health_config: Option<NodeHealthConfig>,
    ready_http_client: Option<ZebraReadyClient>,
}

/// Runtime options for [`ZebraJsonRpcSource`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ZebraJsonRpcSourceOptions {
    /// Maximum total time spent on one JSON-RPC request.
    pub request_timeout: Duration,
    /// Maximum JSON-RPC response body size accepted from the node.
    pub max_response_bytes: NonZeroU64,
    /// Timeout applied only to `sendrawtransaction` calls.
    ///
    /// Zebra can stall for the full 30s `request_timeout` during a block-solve
    /// spike while a broadcast is queued. A tighter per-broadcast timeout lets
    /// callers retry sooner without shrinking the global timeout for block
    /// fetches, which legitimately run long. `None` falls back to
    /// `request_timeout`.
    pub broadcast_timeout: Option<Duration>,
}

impl Default for ZebraJsonRpcSourceOptions {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(30),
            max_response_bytes: DEFAULT_MAX_JSON_RPC_RESPONSE_BYTES,
            broadcast_timeout: None,
        }
    }
}

impl ZebraJsonRpcSource {
    /// Returns the network whose blocks this source decodes.
    #[must_use]
    pub const fn network(&self) -> Network {
        self.network
    }

    /// Returns the static baseline capability set Zebra JSON-RPC sources
    /// are assumed to support before runtime discovery runs.
    ///
    /// Production code should prefer [`ZebraJsonRpcSource::probe_capabilities`]
    /// (which writes the discovered set into the per-source cache); this
    /// constant exists for tests and for compile-time defaults.
    #[must_use]
    pub fn baseline_capabilities() -> NodeCapabilities {
        default_zebra_capabilities()
    }

    /// Fetches the next source-chain update after `cursor`.
    ///
    /// This keeps JSON-RPC catch-up behind the shared
    /// [`SourceChainUpdate`] boundary used by source adapters.
    /// The JSON-RPC adapter can emit connected blocks and cursor-divergence
    /// reverts; it does not emit [`SourceChainUpdate::SettledTip`] because
    /// Zebra JSON-RPC does not expose an ordered finality feed.
    pub async fn fetch_chain_update_after(
        &self,
        cursor: SourceChainCursor,
    ) -> Result<Option<SourceChainUpdate>, SourceError> {
        let observed_tip_id = self.tip_id().await?;

        match cursor.position() {
            SourceChainCursorPosition::BeforeHeight(height) => {
                if observed_tip_id.height < height {
                    return Ok(None);
                }
                self.fetch_connected_block_update(height).await.map(Some)
            }
            SourceChainCursorPosition::AtBlock(block_id) => {
                if observed_tip_id.height < block_id.height
                    || (observed_tip_id.height == block_id.height
                        && observed_tip_id.hash != block_id.hash)
                {
                    return Ok(Some(SourceChainUpdate::reverted_block(block_id)));
                }

                if observed_tip_id.height == block_id.height {
                    return Ok(None);
                }

                let Some(next_height) = block_id.height.next() else {
                    return Ok(None);
                };
                let next_block = self.fetch_block_at(next_height).await?;
                if next_block.parent_hash != block_id.hash {
                    return Ok(Some(SourceChainUpdate::reverted_block(block_id)));
                }

                Ok(Some(SourceChainUpdate::connected_block(next_block)))
            }
        }
    }

    /// Fetches the chain checkpoint identity, block time, and commitment-tree
    /// frontiers at `height` from the node.
    ///
    /// This is the data Zinder needs to bootstrap canonical storage from a
    /// recent height instead of replaying the chain from genesis. The values
    /// come from one height-keyed `z_gettreestate` request. Frontier presence
    /// is validated against `network_upgrade_activations`, including custom
    /// Testnet and Regtest schedules.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError::BlockUnavailable`] when the node does not
    /// have the requested height (e.g. because it is still syncing) and
    /// [`SourceError::SourceProtocolMismatch`] when the response identity is
    /// malformed or does not match `height`. Capability discovery
    /// ([`ZebraJsonRpcSource::probe_capabilities`]) does not gate this call.
    pub async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        if network_upgrade_activations.network() != self.network {
            return Err(SourceError::SourceProtocolMismatch {
                reason: "checkpoint activation table network does not match the node source",
            });
        }
        if network_upgrade_activations
            .activation_height_by_name("Sapling")
            .is_none()
        {
            return Err(SourceError::SourceProtocolMismatch {
                reason: "checkpoint activation table is missing Sapling",
            });
        }
        let block_unavailable = |error: JsonRpcCallError| SourceError::BlockUnavailable {
            height,
            reason: error.message,
        };

        let tree_state: ZebraGetTreestate = self
            .call_typed(
                "z_gettreestate",
                positional_params([Value::from(height.value().to_string())])?,
                block_unavailable,
            )
            .await?;
        if tree_state.height != height.value() {
            return Err(SourceError::SourceProtocolMismatch {
                reason: "z_gettreestate height does not match requested checkpoint height",
            });
        }
        let block_hash = decode_rpc_block_hash(&tree_state.hash)?;
        let block_id = BlockId::new(height, block_hash);
        let block_time_seconds = tree_state.time;
        let frontiers = decode_zebra_commitment_tree_frontiers(
            tree_state,
            height,
            network_upgrade_activations,
        )?;

        Ok(CommitmentTreeCheckpoint::new(
            block_id,
            block_time_seconds,
            frontiers,
        ))
    }

    async fn fetch_connected_block_update(
        &self,
        height: BlockHeight,
    ) -> Result<SourceChainUpdate, SourceError> {
        self.fetch_block_at(height)
            .await
            .map(SourceChainUpdate::connected_block)
    }

    /// Fetches the node-advertised network upgrade activations.
    ///
    /// The values come from Zebra's `getblockchaininfo.upgrades` field so
    /// custom Testnet and Regtest activation schedules stay node-owned, per
    /// [`../../../docs/architecture/chain-ingestion.md`][cha]. The returned
    /// table carries the same `network` identifier as this source.
    ///
    /// [cha]: ../../../docs/architecture/chain-ingestion.md
    pub async fn fetch_network_upgrade_activations(
        &self,
    ) -> Result<NetworkUpgradeActivations, SourceError> {
        let blockchain_info: ZebraGetBlockchainInfoUpgrades = self
            .call_typed("getblockchaininfo", ArrayParams::new(), |error| {
                SourceError::NodeUnavailable {
                    reason: error.message,
                }
            })
            .await?;

        let activations = blockchain_info
            .upgrades
            .iter()
            .map(|(branch_id_hex, upgrade)| {
                let branch_id_value = u32::from_str_radix(branch_id_hex, 16).map_err(|_| {
                    SourceError::SourceProtocolMismatch {
                        reason: "getblockchaininfo upgrades carried a non-hex branch id key",
                    }
                })?;
                Ok(NetworkUpgradeActivation {
                    branch_id: ConsensusBranchId::new(branch_id_value),
                    activation_height: BlockHeight::new(upgrade.activation_height),
                    name: upgrade.name.clone(),
                })
            })
            .collect::<Result<Vec<_>, SourceError>>()?;

        NetworkUpgradeActivations::new(self.network, activations).map_err(|error| {
            tracing::warn!(
                target: "zinder::source",
                event = "network_upgrade_activations_duplicate_branch_id",
                error = %error,
                "Zebra getblockchaininfo upgrades advertised a duplicate branch id"
            );
            SourceError::SourceProtocolMismatch {
                reason: "getblockchaininfo upgrades advertised duplicate consensus branch ids",
            }
        })
    }

    /// Discovers the node-advertised upgrade activations and wraps them for
    /// process-wide sharing.
    ///
    /// Convenience wrapper that calls
    /// [`Self::fetch_network_upgrade_activations`], wraps the result in an
    /// [`Arc`], and emits a structured `network_upgrade_activations_discovered`
    /// log event tagged with `target`. The four service binaries
    /// (`zinder-ingest`, `zinder-query`, `zinder-compat-lightwalletd`,
    /// `zinder-explorer`) share this one entry point so the discovery event
    /// shape stays consistent across the workspace.
    pub async fn discover_network_upgrade_activations(
        &self,
        target: &'static str,
    ) -> Result<Arc<NetworkUpgradeActivations>, SourceError> {
        let activations = Arc::new(self.fetch_network_upgrade_activations().await?);
        tracing::info!(
            target: "zinder::source",
            event = "network_upgrade_activations_discovered",
            service = target,
            network = ?activations.network(),
            advertised = activations.activations().len(),
            "discovered network upgrade activations from running node"
        );
        Ok(activations)
    }

    /// Probes the node's `rpc.discover` (`OpenRPC`) endpoint and updates
    /// the cached capability set returned by
    /// [`NodeSource::capabilities`].
    ///
    /// On probe success, the cache reflects the methods the node
    /// advertises plus [`NodeCapability::JsonRpc`] and
    /// [`NodeCapability::OpenRpcDiscovery`].
    ///
    /// Missing discovery fails closed because assuming a method set would let
    /// startup accept a node that cannot satisfy the source contract.
    pub async fn probe_capabilities(&self) -> Result<NodeCapabilities, SourceError> {
        let openrpc_response: Value = self
            .call_typed("rpc.discover", ArrayParams::new(), |_error| {
                SourceError::NodeCapabilityMissing {
                    capability: NodeCapability::OpenRpcDiscovery,
                }
            })
            .await?;
        let json_rpc_capabilities = parse_openrpc_capabilities(&openrpc_response);

        let probed_capabilities =
            with_readiness_probe_capability(json_rpc_capabilities, self.health_config.is_some());
        *self.cached_capabilities.lock() = probed_capabilities;
        Ok(probed_capabilities)
    }

    /// Creates a Zebra JSON-RPC source.
    pub fn new(
        network: Network,
        json_rpc_addr: impl Into<String>,
        node_auth: NodeAuth,
        request_timeout: Duration,
    ) -> Result<Self, SourceError> {
        Self::with_options(
            network,
            json_rpc_addr,
            node_auth,
            ZebraJsonRpcSourceOptions {
                request_timeout,
                ..ZebraJsonRpcSourceOptions::default()
            },
        )
    }

    /// Creates a Zebra JSON-RPC source with explicit runtime options.
    pub fn with_options(
        network: Network,
        json_rpc_addr: impl Into<String>,
        node_auth: NodeAuth,
        options: ZebraJsonRpcSourceOptions,
    ) -> Result<Self, SourceError> {
        let json_rpc_addr: String = json_rpc_addr.into();
        let request_timeout = options.request_timeout;
        let max_response_bytes = options.max_response_bytes;
        let broadcast_timeout = options.broadcast_timeout;

        let initial_headers = derive_authorization_headers(&node_auth)?;
        let initial_client = crate::transport::build_zebra_json_rpc_client(
            &json_rpc_addr,
            request_timeout,
            max_response_bytes,
            initial_headers,
        )
        .map_err(|error| SourceError::NodeUnavailable {
            reason: error.to_string(),
        })?;

        let rebuilder_addr = json_rpc_addr;
        let rebuilder_auth = node_auth;
        let client = ResilientClient::new(
            initial_client,
            ZEBRA_JSON_RPC_PEER_LABEL,
            ZEBRA_REBUILD_THRESHOLD,
            move || {
                let addr = rebuilder_addr.clone();
                let auth = rebuilder_auth.clone();
                async move {
                    let headers = derive_authorization_headers(&auth).map_err(|error| {
                        crate::transport::ZebraTransportError::ClientBuildFailed(error.to_string())
                    })?;
                    crate::transport::build_zebra_json_rpc_client(
                        &addr,
                        request_timeout,
                        max_response_bytes,
                        headers,
                    )
                }
            },
        );

        Ok(Self {
            network,
            client,
            max_response_bytes,
            broadcast_timeout,
            cached_capabilities: Arc::new(Mutex::new(default_zebra_capabilities())),
            health_config: None,
            ready_http_client: None,
        })
    }

    /// Installs the upstream-health probe configuration.
    ///
    /// Pass the resolved [`NodeHealthConfig`] from
    /// [`NodeTarget::health`](crate::NodeTarget::health). With it set, the
    /// trait method [`NodeSource::poll_upstream_health`] hits Zebra's
    /// `/ready` endpoint; without it, the same method falls back to
    /// `getblockchaininfo.verificationprogress` per
    /// [ADR-0015 §Upstream sync detection].
    ///
    /// [ADR-0015 §Upstream sync detection]:
    ///     ../../../docs/adrs/0015-phase-driven-ingest.md#upstream-sync-detection
    #[must_use]
    pub fn with_health_config(mut self, health_config: Option<NodeHealthConfig>) -> Self {
        self.ready_http_client = health_config
            .as_ref()
            .map(|config| ZebraReadyClient::new(config.poll_interval));
        self.health_config = health_config;
        let mut cache = self.cached_capabilities.lock();
        *cache = with_readiness_probe_capability(*cache, self.health_config.is_some());
        drop(cache);
        self
    }

    async fn call_typed<Response>(
        &self,
        method: &'static str,
        params: ArrayParams,
        map_call_error: impl FnOnce(JsonRpcCallError) -> SourceError,
    ) -> Result<Response, SourceError>
    where
        Response: for<'de> Deserialize<'de>,
    {
        let started_at = Instant::now();
        let client_result = self
            .client
            .snapshot()
            .request::<Response, _>(method, params)
            .await;
        self.client.record_outcome(&jsonrpsee_transport_signal(
            &client_result,
            method,
            self.max_response_bytes,
        ));
        let rpc_outcome = match client_result {
            Ok(response) => Ok(response),
            Err(ClientError::Call(error)) => Err(map_call_error(JsonRpcCallError::from(error))),
            Err(error) => Err(map_transport_error(&error, method, self.max_response_bytes)),
        };
        record_json_rpc_source_outcome(method, started_at, &rpc_outcome);

        rpc_outcome
    }

    async fn fetch_blocks_at_batch(
        &self,
        start_height: BlockHeight,
        end_height: BlockHeight,
    ) -> Result<BatchedSourceBlocks, SourceError> {
        let mut pending = vec![(start_height, end_height)];
        let mut blocks = Vec::with_capacity(block_height_span_len(start_height, end_height));
        let mut stats = SourceChainSegmentStats::default();

        while let Some((range_start, range_end)) = pending.pop() {
            let started_at = Instant::now();
            let outcome = self
                .fetch_blocks_at_batch_inner(range_start, range_end)
                .await;
            record_json_rpc_source_outcome("batch_getblock", started_at, &outcome);
            match outcome {
                Ok(mut range_blocks) => {
                    stats = stats.with_added_response_payload_bytes(
                        range_blocks.stats.response_payload_bytes(),
                    );
                    blocks.append(&mut range_blocks.blocks);
                }
                Err(error) if source_response_too_large(&error) => {
                    let Some(((left_start, left_end), (right_start, right_end))) =
                        split_inclusive_height_range(range_start, range_end)
                    else {
                        return Err(error);
                    };
                    let retry_accounting =
                        offending_range_split_retry_accounting(range_start, range_end);
                    // A child that is still oversized returns through this branch,
                    // so recursive splits add their own requests and block attempts.
                    metrics::counter!(
                        "zinder_node_source_segment_split_total",
                        "source" => "zebra_json_rpc",
                        "reason" => "response_too_large"
                    )
                    .increment(1);
                    metrics::counter!(
                        "zinder_node_source_segment_offending_range_retry_requests_total",
                        "source" => "zebra_json_rpc",
                        "reason" => "response_too_large"
                    )
                    .increment(retry_accounting.request_count);
                    metrics::counter!(
                        "zinder_node_source_segment_offending_range_retry_blocks_total",
                        "source" => "zebra_json_rpc",
                        "reason" => "response_too_large"
                    )
                    .increment(retry_accounting.block_count);
                    stats = stats.with_added_splits(1);
                    tracing::warn!(
                        target: "zinder::source",
                        event = "source_chain_segment_split",
                        start_height = range_start.value(),
                        end_height = range_end.value(),
                        left_start_height = left_start.value(),
                        left_end_height = left_end.value(),
                        right_start_height = right_start.value(),
                        right_end_height = right_end.value(),
                        max_response_bytes = self.max_response_bytes.get(),
                        "source-chain segment exceeded the configured JSON-RPC response limit; splitting the range"
                    );
                    pending.push((right_start, right_end));
                    pending.push((left_start, left_end));
                }
                Err(error) => return Err(error),
            }
        }

        stats = stats.with_connected_blocks(blocks.len());
        Ok(BatchedSourceBlocks { blocks, stats })
    }

    async fn fetch_blocks_at_batch_inner(
        &self,
        start_height: BlockHeight,
        end_height: BlockHeight,
    ) -> Result<BatchedSourceBlocks, SourceError> {
        let heights = block_heights_inclusive(start_height, end_height);
        if heights.is_empty() {
            return Ok(BatchedSourceBlocks {
                blocks: Vec::new(),
                stats: SourceChainSegmentStats::default(),
            });
        }

        let mut batch = BatchRequestBuilder::new();
        for height in &heights {
            let height_param = Value::from(height.value().to_string());
            batch
                .insert(
                    "getblock",
                    positional_params([height_param, Value::from(0)])?,
                )
                .map_err(|source| SourceError::SourcePayloadEncodingFailed { source })?;
        }

        let batch_result = self.client.snapshot().batch_request::<Value>(batch).await;
        self.client.record_outcome(&jsonrpsee_transport_signal(
            &batch_result,
            "batch_getblock",
            self.max_response_bytes,
        ));
        let batch_response = match batch_result {
            Ok(response) => response,
            Err(error) => {
                return Err(map_transport_error(
                    &error,
                    "batch_getblock",
                    self.max_response_bytes,
                ));
            }
        };

        let mut responses = batch_response.into_iter();
        let mut blocks = Vec::with_capacity(heights.len());
        let mut response_payload_bytes = 0_u64;
        for height in heights {
            let raw_block_value = next_batch_value(&mut responses, height, "getblock")?;
            let raw_block_hex = raw_block_value.as_str().ok_or({
                SourceError::SourceProtocolMismatch {
                    reason: "batched getblock response is not a hex string",
                }
            })?;
            response_payload_bytes =
                response_payload_bytes.saturating_add(usize_to_u64_saturating(raw_block_hex.len()));
            let decode_started_at = Instant::now();
            let raw_block_bytes_outcome = hex::decode(raw_block_hex)
                .map_err(|source| SourceError::InvalidRawBlockHex { source })?;
            record_block_decode_stage("hex_decode", decode_started_at);
            let header_started_at = Instant::now();
            let source_block =
                SourceBlock::from_raw_block_bytes(self.network, height, raw_block_bytes_outcome)?;
            record_block_decode_stage("block_header", header_started_at);
            blocks.push(source_block);
        }

        if responses.next().is_some() {
            return Err(SourceError::SourceProtocolMismatch {
                reason: "batched block response contained more entries than requested",
            });
        }

        Ok(BatchedSourceBlocks {
            blocks,
            stats: SourceChainSegmentStats::from_response_payload_bytes(response_payload_bytes),
        })
    }

    /// Fetches the upstream node's current mempool transaction identifiers.
    ///
    /// Uses `getrawmempool` with `verbose=false`, which returns the bare
    /// txid list. Zinder's polling backend diffs successive snapshots to
    /// derive mempool change events; a verbose response would trigger
    /// expensive descendant-graph computation on the node and is not
    /// needed by the indexer.
    pub async fn fetch_raw_mempool_transaction_ids(
        &self,
    ) -> Result<Vec<TransactionId>, SourceError> {
        let txid_hex_list: Vec<String> = self
            .call_typed("getrawmempool", ArrayParams::new(), map_node_unavailable)
            .await?;
        let mut transaction_ids = Vec::with_capacity(txid_hex_list.len());
        for txid_hex in txid_hex_list {
            transaction_ids.push(decode_display_transaction_id(&txid_hex)?);
        }
        Ok(transaction_ids)
    }

    /// Looks up the upstream node's view of a transaction by identifier.
    ///
    /// Returns:
    ///
    /// - [`UpstreamTransactionLookup::Mined`] when the node reports the
    ///   transaction is in a block on its best chain.
    /// - [`UpstreamTransactionLookup::InMempool`] when the node still has
    ///   the transaction in its mempool but no confirming block.
    /// - [`UpstreamTransactionLookup::NotFound`] when Zebra returns the
    ///   `-5` (`InvalidAddressOrKey`) error for the txid.
    ///
    /// The polling [`crate::JsonRpcMempoolSource`] uses this to classify a
    /// disappeared txid into a [`crate::MempoolSourceEvent::Mined`] or
    /// [`crate::MempoolSourceEvent::Invalidated`].
    pub async fn fetch_upstream_transaction_lookup(
        &self,
        transaction_id: TransactionId,
    ) -> Result<UpstreamTransactionLookup, SourceError> {
        let params = positional_params([
            Value::from(display_order_transaction_id_hex(transaction_id)),
            Value::from(1),
        ])?;

        let started_at = Instant::now();
        let response = self
            .client
            .snapshot()
            .request::<Value, _>("getrawtransaction", params)
            .await;
        record_json_rpc_client_result("getrawtransaction", started_at, &response);
        self.client.record_outcome(&jsonrpsee_transport_signal(
            &response,
            "getrawtransaction",
            self.max_response_bytes,
        ));

        match response {
            Ok(verbose_response) => {
                let height_value = verbose_response.get("height").and_then(Value::as_u64);
                let Some(height_value) = height_value else {
                    return Ok(UpstreamTransactionLookup::InMempool);
                };
                let mined_height = BlockHeight::new(u32::try_from(height_value).map_err(|_| {
                    SourceError::SourceProtocolMismatch {
                        reason: "verbose getrawtransaction height does not fit u32",
                    }
                })?);
                let block_hash_hex = verbose_response
                    .get("blockhash")
                    .and_then(Value::as_str)
                    .ok_or(SourceError::SourceProtocolMismatch {
                        reason: "verbose getrawtransaction reports a height without a blockhash",
                    })?;
                let block_hash = decode_rpc_block_hash(block_hash_hex)?;
                Ok(UpstreamTransactionLookup::Mined {
                    mined_height,
                    block_hash,
                })
            }
            Err(ClientError::Call(error)) => {
                let call_error = JsonRpcCallError::from(error);
                if call_error.is_not_found() {
                    Ok(UpstreamTransactionLookup::NotFound)
                } else {
                    Err(SourceError::NodeUnavailable {
                        reason: call_error.message,
                    })
                }
            }
            Err(error) => Err(map_transport_error(
                &error,
                "getrawtransaction",
                self.max_response_bytes,
            )),
        }
    }

    /// Fetches raw serialized transaction bytes by identifier.
    ///
    /// Returns `Ok(None)` when the upstream node reports the transaction
    /// is not present in either the mempool or the main chain (Zebra error
    /// code -5). This is not an error: a hydration race can remove a
    /// transaction between an `Added` observation and the follow-up
    /// fetch. Callers increment `zinder_mempool_hydration_failures_total`
    /// with reason `not_found` when they observe `None`.
    pub async fn fetch_raw_transaction_bytes(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<RawTransactionBytes>, SourceError> {
        let params = positional_params([
            Value::from(display_order_transaction_id_hex(transaction_id)),
            Value::from(0),
        ])?;

        let started_at = Instant::now();
        let response = self
            .client
            .snapshot()
            .request::<String, _>("getrawtransaction", params)
            .await;
        record_json_rpc_client_result("getrawtransaction", started_at, &response);
        self.client.record_outcome(&jsonrpsee_transport_signal(
            &response,
            "getrawtransaction",
            self.max_response_bytes,
        ));

        match response {
            Ok(raw_transaction_hex) => {
                let raw_bytes = hex::decode(raw_transaction_hex)
                    .map_err(|source| SourceError::InvalidRawTransactionHex { source })?;
                Ok(Some(RawTransactionBytes::new(raw_bytes)))
            }
            Err(ClientError::Call(error)) => {
                let call_error = JsonRpcCallError::from(error);
                if call_error.is_not_found() {
                    Ok(None)
                } else {
                    Err(SourceError::MempoolHydrationFailed {
                        transaction_id,
                        reason: call_error.message,
                    })
                }
            }
            Err(error) => Err(map_transport_error(
                &error,
                "getrawtransaction",
                self.max_response_bytes,
            )),
        }
    }

    async fn fetch_bounded_subtree_roots(
        &self,
        protocol: ShieldedProtocol,
        start_index: SubtreeRootIndex,
        max_entries: NonZeroU32,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        let subtree_response: ZebraSubtreeRootsByIndex = self
            .call_typed(
                "z_getsubtreesbyindex",
                positional_params([
                    Value::from(protocol.rpc_pool_name()),
                    Value::from(start_index.value()),
                    Value::from(max_entries.get()),
                ])?,
                |error| SourceError::SubtreeRootsUnavailable {
                    protocol,
                    start_index,
                    reason: error.message,
                },
            )
            .await?;

        if subtree_response.pool != protocol.rpc_pool_name() {
            return Err(SourceError::SourceProtocolMismatch {
                reason: "subtree roots pool does not match requested protocol",
            });
        }
        if subtree_response.start_index != start_index.value() {
            return Err(SourceError::SourceProtocolMismatch {
                reason: "subtree roots start index does not match requested index",
            });
        }
        let response_entry_count =
            u32::try_from(subtree_response.subtrees.len()).map_err(|_| {
                SourceError::SourceProtocolMismatch {
                    reason: "subtree roots response has too many entries",
                }
            })?;
        if response_entry_count > max_entries.get() {
            return Err(SourceError::SourceProtocolMismatch {
                reason: "subtree roots response exceeds the requested bound",
            });
        }

        let mut subtree_roots = Vec::with_capacity(subtree_response.subtrees.len());
        let mut previous_completing_block_height = None;
        for (offset, subtree) in subtree_response.subtrees.into_iter().enumerate() {
            let offset =
                u32::try_from(offset).map_err(|_| SourceError::SourceProtocolMismatch {
                    reason: "subtree roots response has too many entries",
                })?;
            let subtree_index = start_index
                .value()
                .checked_add(offset)
                .map(SubtreeRootIndex::new)
                .ok_or(SourceError::SourceProtocolMismatch {
                    reason: "subtree roots response exceeds the SubtreeRootIndex range",
                })?;
            let completing_block_height = BlockHeight::new(subtree.end_height);
            if previous_completing_block_height
                .is_some_and(|previous_height| completing_block_height < previous_height)
            {
                return Err(SourceError::SourceProtocolMismatch {
                    reason: "subtree root completion heights are not ascending",
                });
            }
            previous_completing_block_height = Some(completing_block_height);
            subtree_roots.push(SourceSubtreeRoot::new(
                subtree_index,
                decode_subtree_root_hash(&subtree.root)?,
                completing_block_height,
            ));
        }

        Ok(SourceSubtreeRoots::new(
            protocol,
            start_index,
            subtree_roots,
        ))
    }
}

#[async_trait]
impl NodeSource for ZebraJsonRpcSource {
    fn capabilities(&self) -> NodeCapabilities {
        *self.cached_capabilities.lock()
    }

    async fn fetch_chain_segment(
        &self,
        limits: SourceChainSegmentLimits,
    ) -> Result<SourceChainSegment, SourceError> {
        let observed_tip_id = self.tip_id().await?;
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

        let end_height = bounded_segment_end_height(
            start_height,
            observed_tip_id.height,
            limits.max_connected_blocks,
        );
        let batched_blocks = self.fetch_blocks_at_batch(start_height, end_height).await?;
        if let (Some(expected_parent_id), Some(first_block)) =
            (expected_parent_id, batched_blocks.blocks.first())
            && first_block.parent_hash != expected_parent_id.hash
        {
            return Ok(SourceChainSegment::new([
                SourceChainUpdate::reverted_block(expected_parent_id),
            ]));
        }
        validate_source_block_links(&batched_blocks.blocks)?;

        Ok(SourceChainSegment::connected_blocks_with_stats(
            batched_blocks.blocks,
            batched_blocks.stats,
        ))
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        let block_unavailable = |error: JsonRpcCallError| SourceError::BlockUnavailable {
            height,
            reason: error.message,
        };

        let raw_block_hex = self
            .call_typed::<String>(
                "getblock",
                positional_params([Value::from(height.value().to_string()), Value::from(0)])?,
                block_unavailable,
            )
            .await?;

        let raw_block_bytes = hex::decode(raw_block_hex)
            .map_err(|source| SourceError::InvalidRawBlockHex { source })?;

        SourceBlock::from_raw_block_bytes(self.network, height, raw_block_bytes)
    }

    async fn fetch_tree_state_for_block(
        &self,
        block_id: BlockId,
    ) -> Result<SourceTreeState, SourceError> {
        let tree_state = self
            .call_typed::<Value>(
                "z_gettreestate",
                positional_params([Value::from(block_id.height.value().to_string())])?,
                |error| SourceError::BlockUnavailable {
                    height: block_id.height,
                    reason: error.message,
                },
            )
            .await?;
        let final_note_commitment_roots =
            parse_zebra_final_note_commitment_roots(&tree_state, block_id)?;
        let (block_time_seconds, payload_bytes) = normalize_zebra_tree_state_payload(&tree_state)?;
        Ok(SourceTreeState::with_final_note_commitment_roots(
            final_note_commitment_roots,
            block_time_seconds,
            payload_bytes,
        ))
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        Self::fetch_chain_checkpoint(self, height, network_upgrade_activations).await
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        let observed_tip: ZebraBestBlockHeightAndHash = self
            .call_typed(
                "getbestblockheightandhash",
                ArrayParams::new(),
                map_node_unavailable,
            )
            .await?;
        Ok(BlockId::new(
            BlockHeight::new(observed_tip.height),
            BlockHash::from_bytes(observed_tip.hash),
        ))
    }

    async fn fetch_subtree_roots(
        &self,
        protocol: ShieldedProtocol,
        start_index: SubtreeRootIndex,
        max_entries: NonZeroU32,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        self.fetch_bounded_subtree_roots(protocol, start_index, max_entries)
            .await
    }

    async fn fetch_subtree_root_range(
        &self,
        range: SubtreeRootRange,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        let subtree_roots = self
            .fetch_bounded_subtree_roots(range.protocol, range.start_index, range.max_entries)
            .await?;
        let actual_count = u32::try_from(subtree_roots.subtree_roots.len()).map_err(|_| {
            SourceError::SourceProtocolMismatch {
                reason: "subtree roots response has too many entries",
            }
        })?;
        if actual_count != range.max_entries.get() {
            return Err(SourceError::SubtreeRootsUnavailable {
                protocol: range.protocol,
                start_index: range.start_index,
                reason: format!(
                    "expected {} subtree roots, got {actual_count}",
                    range.max_entries
                ),
            });
        }

        Ok(subtree_roots)
    }

    async fn fetch_chain_value_pools_at_tip(&self) -> Result<ChainValuePools, SourceError> {
        let blockchain_info: ZebraGetBlockchainInfoValuePools = self
            .call_typed("getblockchaininfo", ArrayParams::new(), |error| {
                SourceError::NodeUnavailable {
                    reason: error.message,
                }
            })
            .await?;
        if blockchain_info.value_pools.is_empty() {
            return Err(SourceError::NodeCapabilityMissing {
                capability: NodeCapability::ChainValuePools,
            });
        }
        let pools = blockchain_info
            .value_pools
            .into_iter()
            .map(|entry| ChainValuePool::new(entry.id, entry.monitored, entry.chain_value_zat))
            .collect();
        Ok(ChainValuePools::new(
            BlockId::new(
                BlockHeight::new(blockchain_info.blocks),
                decode_rpc_block_hash(&blockchain_info.best_block_hash)?,
            ),
            pools,
        ))
    }

    async fn fetch_block_value_pool_balances(
        &self,
        block_id: BlockId,
    ) -> Result<BlockValuePoolBalances, SourceError> {
        let block_response: ZebraGetBlockValuePools = self
            .call_typed(
                "getblock",
                positional_params([
                    Value::from(encode_rpc_block_hash(block_id.hash)),
                    Value::from(1),
                ])?,
                |error| SourceError::BlockUnavailable {
                    height: block_id.height,
                    reason: error.message,
                },
            )
            .await?;
        let response_hash = block_response
            .hash
            .ok_or(SourceError::SourceProtocolMismatch {
                reason: "verbose getblock response is missing hash",
            })?;
        let response_height = block_response
            .height
            .ok_or(SourceError::SourceProtocolMismatch {
                reason: "verbose getblock response is missing height",
            })?;
        let response_time = block_response
            .time
            .ok_or(SourceError::SourceProtocolMismatch {
                reason: "verbose getblock response is missing time",
            })?;
        let value_pools = block_response
            .value_pools
            .ok_or(SourceError::NodeCapabilityMissing {
                capability: NodeCapability::BlockValuePoolBalances,
            })?;

        if response_height != block_id.height.value() {
            return Err(SourceError::SourceProtocolMismatch {
                reason: "verbose getblock height does not match requested block",
            });
        }
        if decode_rpc_block_hash(&response_hash)? != block_id.hash {
            return Err(SourceError::SourceProtocolMismatch {
                reason: "verbose getblock hash does not match requested block",
            });
        }
        if value_pools.is_empty() {
            return Err(SourceError::SourceProtocolMismatch {
                reason: "verbose getblock valuePools is empty",
            });
        }

        let mut pool_ids = HashSet::with_capacity(value_pools.len());
        let mut pools = Vec::with_capacity(value_pools.len());
        for entry in value_pools {
            if entry.id.is_empty() {
                return Err(SourceError::SourceProtocolMismatch {
                    reason: "verbose getblock valuePools contains an empty pool id",
                });
            }
            if !pool_ids.insert(entry.id.clone()) {
                return Err(SourceError::SourceProtocolMismatch {
                    reason: "verbose getblock valuePools contains a duplicate pool id",
                });
            }
            let value_zat = entry
                .chain_value_zat
                .map(u64::try_from)
                .transpose()
                .map_err(|_| SourceError::SourceProtocolMismatch {
                    reason: "verbose getblock valuePools contains a negative balance",
                })?;
            pools.push(ValuePoolBalance::new(entry.id, entry.monitored, value_zat));
        }

        Ok(BlockValuePoolBalances::new(block_id, response_time, pools))
    }

    async fn poll_upstream_health(&self) -> Result<UpstreamHealthSnapshot, SourceError> {
        if let (Some(config), Some(client)) =
            (self.health_config.as_ref(), self.ready_http_client.as_ref())
        {
            match client.probe(&config.addr).await {
                Ok(snapshot) => return Ok(snapshot),
                Err(error) => {
                    tracing::warn!(
                        target: "zinder::source",
                        event = "upstream_health_endpoint_unreachable",
                        addr = config.addr.as_str(),
                        reason = %error,
                        "ready endpoint probe failed; falling back to verificationprogress"
                    );
                }
            }
        }

        self.poll_upstream_health_from_blockchain_info().await
    }
}

impl ZebraJsonRpcSource {
    async fn poll_upstream_health_from_blockchain_info(
        &self,
    ) -> Result<UpstreamHealthSnapshot, SourceError> {
        let snapshot_fields: ZebraGetBlockchainInfoHealth = self
            .call_typed("getblockchaininfo", ArrayParams::new(), |error| {
                SourceError::NodeUnavailable {
                    reason: error.message,
                }
            })
            .await?;

        let (progress_floor, gap_floor) =
            self.health_config
                .as_ref()
                .map_or(NodeHealthConfig::default_floors(), |config| {
                    (
                        config.verification_progress_floor,
                        config.estimated_gap_floor_blocks,
                    )
                });

        let progress = snapshot_fields.verification_progress;
        let committed = snapshot_fields.blocks;
        let estimated = snapshot_fields.estimated_height.unwrap_or(committed);
        if self.network == Network::ZcashRegtest {
            // Regtest has no external network tip to synchronize against.
            // Zebra can still report a Mainnet-shaped estimated height here,
            // so applying public-network gap thresholds would permanently
            // reject an otherwise usable operator-controlled chain.
            return Ok(UpstreamHealthSnapshot::ready(
                UPSTREAM_HEALTH_SOURCE_VERIFICATION_PROGRESS_FALLBACK,
                Some(committed),
                Some(estimated),
                progress,
            ));
        }
        let gap = estimated.saturating_sub(committed);

        if let Some(progress_value) = progress
            && progress_value < progress_floor
        {
            return Ok(UpstreamHealthSnapshot::not_ready(
                UPSTREAM_HEALTH_SOURCE_VERIFICATION_PROGRESS_FALLBACK,
                UPSTREAM_HEALTH_REASON_VERIFICATION_PROGRESS_BELOW_FLOOR,
                Some(committed),
                Some(estimated),
                progress,
            ));
        }
        if gap > gap_floor {
            return Ok(UpstreamHealthSnapshot::not_ready(
                UPSTREAM_HEALTH_SOURCE_VERIFICATION_PROGRESS_FALLBACK,
                UPSTREAM_HEALTH_REASON_ESTIMATED_GAP_ABOVE_FLOOR,
                Some(committed),
                Some(estimated),
                progress,
            ));
        }

        Ok(UpstreamHealthSnapshot::ready(
            UPSTREAM_HEALTH_SOURCE_VERIFICATION_PROGRESS_FALLBACK,
            Some(committed),
            Some(estimated),
            progress,
        ))
    }
}

#[async_trait]
impl TransactionBroadcaster for ZebraJsonRpcSource {
    async fn broadcast_transaction(
        &self,
        raw_transaction: RawTransactionBytes,
    ) -> Result<TransactionBroadcastOutcome, SourceError> {
        let raw_transaction_hex = hex::encode(raw_transaction.as_slice());
        let params = positional_params([Value::from(raw_transaction_hex)])?;

        let started_at = Instant::now();
        let client_snapshot = self.client.snapshot();
        let broadcast_fut = client_snapshot.request::<String, _>("sendrawtransaction", params);

        let response = if let Some(timeout_duration) = self.broadcast_timeout {
            match tokio::time::timeout(timeout_duration, broadcast_fut).await {
                Ok(outcome) => outcome,
                Err(_elapsed) => Err(ClientError::Transport(
                    std::io::Error::other(format!(
                        "sendrawtransaction timed out after {}ms",
                        timeout_duration.as_millis()
                    ))
                    .into(),
                )),
            }
        } else {
            broadcast_fut.await
        };
        record_json_rpc_client_result("sendrawtransaction", started_at, &response);
        self.client.record_outcome(&jsonrpsee_transport_signal(
            &response,
            "sendrawtransaction",
            self.max_response_bytes,
        ));

        match response {
            Ok(transaction_id_hex) => {
                Ok(TransactionBroadcastOutcome::Accepted(BroadcastAccepted {
                    transaction_id: decode_display_transaction_id(&transaction_id_hex)?,
                }))
            }
            Err(ClientError::Call(error)) => {
                Ok(classify_broadcast_error(JsonRpcCallError::from(error)))
            }
            Err(error) => Err(map_transport_error(
                &error,
                "sendrawtransaction",
                self.max_response_bytes,
            )),
        }
    }
}

#[async_trait]
impl TreeStateUpstream for ZebraJsonRpcSource {
    async fn fetch_tree_state_for_block(
        &self,
        block_id: BlockId,
    ) -> Result<SourceTreeState, SourceError> {
        <Self as NodeSource>::fetch_tree_state_for_block(self, block_id).await
    }
}

fn map_node_unavailable(error: JsonRpcCallError) -> SourceError {
    SourceError::NodeUnavailable {
        reason: error.message,
    }
}

struct BatchedSourceBlocks {
    blocks: Vec<SourceBlock>,
    stats: SourceChainSegmentStats,
}

fn bounded_segment_end_height(
    start_height: BlockHeight,
    tip_height: BlockHeight,
    max_connected_blocks: NonZeroU32,
) -> BlockHeight {
    let last_requested_height = start_height
        .value()
        .saturating_add(max_connected_blocks.get().saturating_sub(1));
    BlockHeight::new(last_requested_height.min(tip_height.value()))
}

fn block_heights_inclusive(start_height: BlockHeight, end_height: BlockHeight) -> Vec<BlockHeight> {
    if end_height < start_height {
        return Vec::new();
    }
    let capacity = usize::try_from(
        end_height
            .value()
            .saturating_sub(start_height.value())
            .saturating_add(1),
    )
    .unwrap_or(usize::MAX);
    let mut heights = Vec::with_capacity(capacity);
    let mut next_height = Some(start_height);
    while let Some(height) = next_height {
        if height > end_height {
            break;
        }
        heights.push(height);
        next_height = height.next();
    }
    heights
}

fn block_height_span_len(start_height: BlockHeight, end_height: BlockHeight) -> usize {
    if end_height < start_height {
        return 0;
    }
    usize::try_from(
        end_height
            .value()
            .saturating_sub(start_height.value())
            .saturating_add(1),
    )
    .unwrap_or(usize::MAX)
}

type InclusiveHeightRange = (BlockHeight, BlockHeight);

fn split_inclusive_height_range(
    start_height: BlockHeight,
    end_height: BlockHeight,
) -> Option<(InclusiveHeightRange, InclusiveHeightRange)> {
    if start_height >= end_height {
        return None;
    }
    let midpoint = start_height
        .value()
        .saturating_add(end_height.value().saturating_sub(start_height.value()) / 2);
    let left_end = BlockHeight::new(midpoint);
    let right_start = left_end.next()?;
    Some(((start_height, left_end), (right_start, end_height)))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OffendingRangeSplitRetryAccounting {
    request_count: u64,
    block_count: u64,
}

fn offending_range_split_retry_accounting(
    start_height: BlockHeight,
    end_height: BlockHeight,
) -> OffendingRangeSplitRetryAccounting {
    let block_count = if end_height < start_height {
        0
    } else {
        u64::from(
            end_height
                .value()
                .saturating_sub(start_height.value())
                .saturating_add(1),
        )
    };
    OffendingRangeSplitRetryAccounting {
        request_count: 2,
        block_count,
    }
}

fn next_batch_value<'a>(
    responses: &mut impl Iterator<Item = Result<Value, ErrorObject<'a>>>,
    height: BlockHeight,
    method: &'static str,
) -> Result<Value, SourceError> {
    match responses.next() {
        Some(Ok(response_value)) => Ok(response_value),
        Some(Err(error)) => Err(SourceError::BlockUnavailable {
            height,
            reason: batch_call_error_message(method, &error),
        }),
        None => Err(SourceError::SourceProtocolMismatch {
            reason: "batched block response contained fewer entries than requested",
        }),
    }
}

fn batch_call_error_message(method: &str, error: &ErrorObject<'_>) -> String {
    format!("{method}: {}", error.message())
}

pub(crate) fn validate_source_block_links(blocks: &[SourceBlock]) -> Result<(), SourceError> {
    for pair in blocks.windows(2) {
        let [previous, current] = pair else {
            continue;
        };
        if current.height.value() != previous.height.value().saturating_add(1)
            || current.parent_hash != previous.hash
        {
            return Err(SourceError::BlockReorgDuringFetch {
                height: current.height,
                reason: "batched source blocks are not parent-linked",
            });
        }
    }
    Ok(())
}

fn usize_to_u64_saturating(amount: usize) -> u64 {
    u64::try_from(amount).unwrap_or(u64::MAX)
}

fn record_block_decode_stage(stage: &'static str, started_at: Instant) {
    metrics::histogram!(
        "zinder_node_block_decode_stage_duration_seconds",
        "source" => "zebra_json_rpc",
        "stage" => stage
    )
    .record(started_at.elapsed());
}

fn record_json_rpc_source_outcome<Response>(
    method: &'static str,
    started_at: Instant,
    rpc_outcome: &Result<Response, SourceError>,
) {
    metrics::histogram!(
        "zinder_node_request_duration_seconds",
        "source" => "zebra_json_rpc",
        "method" => method,
        "status" => outcome_status(rpc_outcome),
        "error_class" => source_error_class(rpc_outcome.as_ref().err())
    )
    .record(started_at.elapsed());
    metrics::counter!(
        "zinder_node_request_total",
        "source" => "zebra_json_rpc",
        "method" => method,
        "status" => outcome_status(rpc_outcome),
        "error_class" => source_error_class(rpc_outcome.as_ref().err())
    )
    .increment(1);
}

fn record_json_rpc_client_result<Response>(
    method: &'static str,
    started_at: Instant,
    rpc_outcome: &Result<Response, ClientError>,
) {
    metrics::histogram!(
        "zinder_node_request_duration_seconds",
        "source" => "zebra_json_rpc",
        "method" => method,
        "status" => outcome_status(rpc_outcome),
        "error_class" => client_error_class(rpc_outcome.as_ref().err())
    )
    .record(started_at.elapsed());
    metrics::counter!(
        "zinder_node_request_total",
        "source" => "zebra_json_rpc",
        "method" => method,
        "status" => outcome_status(rpc_outcome),
        "error_class" => client_error_class(rpc_outcome.as_ref().err())
    )
    .increment(1);
}

const fn outcome_status<T, E>(outcome: &Result<T, E>) -> &'static str {
    if outcome.is_ok() { "ok" } else { "error" }
}

fn source_error_class(error: Option<&SourceError>) -> &'static str {
    match error {
        None => "none",
        Some(SourceError::NodeUnavailable { .. }) => "node_unavailable",
        Some(SourceError::SourceResponseTooLarge { .. }) => "source_response_too_large",
        Some(SourceError::BlockUnavailable { .. }) => "block_unavailable",
        Some(SourceError::BlockReorgDuringFetch { .. }) => "block_reorg_during_fetch",
        Some(SourceError::SubtreeRootsUnavailable { .. }) => "subtree_roots_unavailable",
        Some(SourceError::SourceProtocolMismatch { .. }) => "source_protocol_mismatch",
        Some(SourceError::SourcePayloadEncodingFailed { .. }) => "source_payload_encoding_failed",
        Some(SourceError::NodeCapabilityMissing { .. }) => "node_capability_missing",
        Some(SourceError::TransactionBroadcastDisabled) => "transaction_broadcast_disabled",
        Some(SourceError::UnsupportedNodeAuth { .. }) => "unsupported_node_auth",
        Some(SourceError::MempoolStreamUnavailable { .. }) => "mempool_stream_unavailable",
        Some(SourceError::MempoolHydrationFailed { .. }) => "mempool_hydration_failed",
        Some(SourceError::MempoolTransactionCountLimitExceeded { .. }) => {
            "mempool_transaction_count_limit_exceeded"
        }
        Some(SourceError::MempoolRawTransactionBytesLimitExceeded { .. }) => {
            "mempool_raw_transaction_bytes_limit_exceeded"
        }
        Some(SourceError::ChainTipStreamUnavailable { .. }) => "chain_tip_stream_unavailable",
        Some(
            SourceError::InvalidBlockHashHex { .. }
            | SourceError::InvalidRawBlockHex { .. }
            | SourceError::InvalidRawTransactionHex { .. }
            | SourceError::InvalidBlockHashLength { .. }
            | SourceError::InvalidTransactionIdHex { .. }
            | SourceError::InvalidTransactionIdLength { .. }
            | SourceError::InvalidSubtreeRootHex { .. }
            | SourceError::InvalidSubtreeRootLength { .. }
            | SourceError::MalformedFinalNoteCommitmentRoot { .. }
            | SourceError::InvalidFinalNoteCommitmentRootHex { .. }
            | SourceError::InvalidFinalNoteCommitmentRootLength { .. }
            | SourceError::MalformedCommitmentTreeFrontier { .. }
            | SourceError::InvalidCommitmentTreeFrontierHex { .. }
            | SourceError::CommitmentTreeFrontierTooLarge { .. }
            | SourceError::InvalidCommitmentTreeFrontierEncoding { .. }
            | SourceError::CommitmentTreeSizeOutOfRange { .. }
            | SourceError::CommitmentTreeFrontierRootMismatch { .. }
            | SourceError::CommitmentTreeFrontierActivationMismatch { .. }
            | SourceError::RawBlockParseFailed { .. }
            | SourceError::RawTransactionParseFailed { .. }
            | SourceError::TransactionComponentIndexOverflow { .. }
            | SourceError::RawBlockTimeOutOfRange,
        ) => "source_decode_failed",
    }
}

fn client_error_class(error: Option<&ClientError>) -> &'static str {
    match error {
        None => "none",
        Some(ClientError::Call(_)) => "json_rpc_call_error",
        Some(error) if client_error_is_response_too_large(error) => "response_too_large",
        Some(_) => "transport_error",
    }
}

/// Parses an `OpenRPC` `rpc.discover` response into a
/// [`NodeCapabilities`] set.
///
/// The probe always grants [`NodeCapability::JsonRpc`] and
/// [`NodeCapability::OpenRpcDiscovery`] on success; remaining variants
/// are granted when the corresponding RPC methods appear in the response.
fn parse_openrpc_capabilities(openrpc_response: &Value) -> NodeCapabilities {
    let mut probed_capabilities = vec![NodeCapability::JsonRpc, NodeCapability::OpenRpcDiscovery];

    let method_names = openrpc_method_names(openrpc_response);
    if method_names.contains(&"getblock") {
        probed_capabilities.push(NodeCapability::BestChainBlocks);
        probed_capabilities.push(NodeCapability::BlockValuePoolBalances);
    }
    if method_names.contains(&"getblock") {
        probed_capabilities.push(NodeCapability::SourceChainSegments);
    }
    if method_names.contains(&"getbestblockheightandhash") {
        probed_capabilities.push(NodeCapability::TipId);
    }
    if method_names.contains(&"z_gettreestate") {
        probed_capabilities.push(NodeCapability::TreeState);
    }
    if method_names.contains(&"z_getsubtreesbyindex") {
        probed_capabilities.push(NodeCapability::SubtreeRoots);
    }
    if method_names.contains(&"sendrawtransaction") {
        probed_capabilities.push(NodeCapability::TransactionBroadcast);
    }
    if method_names.contains(&"getblockchaininfo") {
        probed_capabilities.push(NodeCapability::ChainValuePools);
    }

    NodeCapabilities::from_trusted(probed_capabilities)
}

fn openrpc_method_names(openrpc_response: &Value) -> Vec<&str> {
    openrpc_response
        .get("methods")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|method| method.get("name").and_then(Value::as_str))
        .collect()
}

/// Builds the `Authorization: Basic ...` header value from node credentials.
/// Re-reads cookie credentials (when applicable) and assembles the
/// `authorization` header used by every JSON-RPC call.
///
/// Called once at construction and again from the
/// [`ResilientClient`] rebuilder closure. Re-reading on rebuild is
/// load-bearing: Zebra rotates the cookie file on restart, so a
/// rebuilder that captured headers once at startup would replay a stale
/// credential and every newly-built client would hit 401 forever. The
/// rebuilder owns a `NodeAuth` clone and re-derives the header each
/// time the wrapper rebuilds.
fn derive_authorization_headers(node_auth: &NodeAuth) -> Result<HeaderMap, SourceError> {
    let authorization = match node_auth {
        NodeAuth::None => None,
        NodeAuth::Basic { username, password } => {
            Some(basic_authorization_header(username, password))
        }
        NodeAuth::Cookie(source) => Some(cookie_authorization_header(source)?),
    };
    let mut headers = HeaderMap::new();
    if let Some(authorization) = authorization {
        let header_value =
            HeaderValue::from_str(&authorization).map_err(|_| SourceError::NodeUnavailable {
                reason: "node authorization header is not a valid HTTP header value".to_owned(),
            })?;
        headers.insert("authorization", header_value);
    }
    Ok(headers)
}

fn basic_authorization_header(username: &str, password: &SecretString) -> String {
    let credentials = format!("{}:{}", username, password.expose_secret());
    basic_authorization_header_from_credentials(&credentials)
}

/// Builds the `Authorization: Basic ...` header value from a cookie source.
fn cookie_authorization_header(source: &CookieSource) -> Result<String, SourceError> {
    let credentials = source
        .read_credentials()
        .map_err(|error| SourceError::NodeUnavailable {
            reason: match error {
                CookieSourceError::Unreadable { .. } => {
                    "node cookie source could not be read".to_owned()
                }
                CookieSourceError::Empty => "node cookie credentials are empty".to_owned(),
            },
        })?;
    Ok(basic_authorization_header_from_credentials(
        credentials.expose_secret(),
    ))
}

/// Builds the `Authorization: Basic ...` header value from raw `username:password` credentials.
fn basic_authorization_header_from_credentials(credentials: &str) -> String {
    format!("Basic {}", BASE64_STANDARD.encode(credentials))
}

/// Builds positional JSON-RPC parameters from an iterator of pre-typed JSON values.
///
/// `ArrayParams::insert` returns an error only on serialization failure. Inputs
/// are already `serde_json::Value`, so the error path is logically unreachable;
/// any unexpected failure is surfaced through `SourcePayloadEncodingFailed`.
fn positional_params(
    param_values: impl IntoIterator<Item = Value>,
) -> Result<ArrayParams, SourceError> {
    let mut params = ArrayParams::new();
    for param_value in param_values {
        params
            .insert(param_value)
            .map_err(|source| SourceError::SourcePayloadEncodingFailed { source })?;
    }
    Ok(params)
}

fn validate_zebra_tree_state(
    tree_state: &Value,
    requested_height: BlockHeight,
    block_hash: BlockHash,
) -> Result<(), SourceError> {
    let tree_state_hash = tree_state
        .get("hash")
        .and_then(Value::as_str)
        .ok_or(SourceError::SourceProtocolMismatch {
            reason: "tree-state response is missing block hash",
        })
        .and_then(decode_rpc_block_hash)?;
    if tree_state_hash != block_hash {
        // Mid-flight reorg: `z_gettreestate` and the parsed `getblock`
        // bytes observed different blocks at the same height.
        return Err(SourceError::BlockReorgDuringFetch {
            height: requested_height,
            reason: "tree-state hash disagrees with the parsed raw block hash",
        });
    }

    let tree_state_height = tree_state.get("height").and_then(Value::as_u64).ok_or(
        SourceError::SourceProtocolMismatch {
            reason: "tree-state response is missing block height",
        },
    )?;
    if tree_state_height != u64::from(requested_height.value()) {
        return Err(SourceError::SourceProtocolMismatch {
            reason: "tree-state height does not match requested height",
        });
    }

    Ok(())
}

fn parse_zebra_final_note_commitment_roots(
    tree_state: &Value,
    block_id: BlockId,
) -> Result<BlockFinalNoteCommitmentRoots, SourceError> {
    validate_zebra_tree_state(tree_state, block_id.height, block_id.hash)?;

    Ok(BlockFinalNoteCommitmentRoots::new(
        block_id.height,
        block_id.hash,
        parse_zebra_final_note_commitment_root(tree_state, ShieldedProtocol::Sapling)?,
        parse_zebra_final_note_commitment_root(tree_state, ShieldedProtocol::Orchard)?,
        parse_zebra_final_note_commitment_root(tree_state, ShieldedProtocol::Ironwood)?,
    ))
}

fn normalize_zebra_tree_state_payload(tree_state: &Value) -> Result<(u32, Vec<u8>), SourceError> {
    let block_time_seconds = tree_state
        .get("time")
        .and_then(Value::as_u64)
        .and_then(|time| u32::try_from(time).ok())
        .ok_or(SourceError::SourceProtocolMismatch {
            reason: "tree-state response is missing a valid block time",
        })?;
    let mut payload = Map::new();
    for pool_name in ["sapling", "orchard", "ironwood"] {
        let Some(final_state) = tree_state
            .get(pool_name)
            .and_then(|pool| pool.get("commitments"))
            .and_then(|commitments| commitments.get("finalState"))
        else {
            continue;
        };
        let final_state = final_state
            .as_str()
            .ok_or(SourceError::SourceProtocolMismatch {
                reason: "tree-state finalState must be a string",
            })?;
        payload.insert(
            pool_name.to_owned(),
            serde_json::json!({"commitments": {"finalState": final_state}}),
        );
    }
    serde_json::to_vec(&Value::Object(payload))
        .map(|payload_bytes| (block_time_seconds, payload_bytes))
        .map_err(|source| SourceError::SourcePayloadEncodingFailed { source })
}

fn parse_zebra_final_note_commitment_root(
    tree_state: &Value,
    protocol: ShieldedProtocol,
) -> Result<Option<FinalNoteCommitmentRoot>, SourceError> {
    let Some(pool) = tree_state.get(protocol.rpc_pool_name()) else {
        return Ok(None);
    };
    let Some(commitments) = pool.get("commitments") else {
        return Ok(None);
    };
    let Some(final_root) = commitments.get("finalRoot") else {
        return Ok(None);
    };
    if final_root.is_null() {
        return Ok(None);
    }
    let root_hex = final_root
        .as_str()
        .ok_or(SourceError::MalformedFinalNoteCommitmentRoot {
            protocol,
            reason: "finalRoot must be a hex string or null",
        })?;
    decode_final_note_commitment_root_hex(protocol, root_hex).map(Some)
}

fn decode_zebra_commitment_tree_frontiers(
    tree_state: ZebraGetTreestate,
    height: BlockHeight,
    network_upgrade_activations: &NetworkUpgradeActivations,
) -> Result<CommitmentTreeFrontiers, SourceError> {
    let sapling = decode_zebra_commitment_tree_frontier(
        ShieldedProtocol::Sapling,
        tree_state.sapling,
        height,
        network_upgrade_activations,
    )?;
    let orchard = decode_zebra_commitment_tree_frontier(
        ShieldedProtocol::Orchard,
        tree_state.orchard,
        height,
        network_upgrade_activations,
    )?;
    let ironwood = decode_zebra_commitment_tree_frontier(
        ShieldedProtocol::Ironwood,
        tree_state.ironwood,
        height,
        network_upgrade_activations,
    )?;

    Ok(CommitmentTreeFrontiers::from_validated_parts(
        sapling, orchard, ironwood,
    ))
}

fn decode_zebra_commitment_tree_frontier(
    protocol: ShieldedProtocol,
    tree_state: Option<ZebraTreestate>,
    height: BlockHeight,
    network_upgrade_activations: &NetworkUpgradeActivations,
) -> Result<Option<CommitmentTreeFrontier>, SourceError> {
    let commitments = tree_state.and_then(|state| state.commitments);
    let (final_root_hex, final_state_hex) = commitments.map_or((None, None), |commitments| {
        (commitments.final_root, commitments.final_state)
    });
    let is_active = commitment_tree_is_active(network_upgrade_activations, protocol, height);

    let (final_root_hex, final_state_hex) = match (final_root_hex, final_state_hex, is_active) {
        (None, None, false) => return Ok(None),
        (Some(_), Some(_), false) => {
            return Err(SourceError::CommitmentTreeFrontierActivationMismatch {
                protocol,
                height,
                reason: "frontier is present before the pool activation height",
            });
        }
        (None, None, true) => {
            return Err(SourceError::CommitmentTreeFrontierActivationMismatch {
                protocol,
                height,
                reason: "frontier is absent at or after the pool activation height",
            });
        }
        (Some(final_root_hex), Some(final_state_hex), true) => (final_root_hex, final_state_hex),
        _ => {
            return Err(SourceError::MalformedCommitmentTreeFrontier {
                protocol,
                reason: "finalRoot and finalState must both be present or both be absent",
            });
        }
    };

    let final_root = decode_final_note_commitment_root_hex(protocol, &final_root_hex)?;
    let final_state_bytes = hex::decode(final_state_hex)
        .map_err(|source| SourceError::InvalidCommitmentTreeFrontierHex { protocol, source })?;
    CommitmentTreeFrontier::from_canonical_final_state(protocol, final_root, final_state_bytes)
        .map(Some)
        .map_err(|source| map_frontier_validation_error(protocol, source))
}

const fn map_frontier_validation_error(
    protocol: ShieldedProtocol,
    source: CommitmentTreeFrontierValidationError,
) -> SourceError {
    match source {
        CommitmentTreeFrontierValidationError::TooLarge {
            byte_count,
            max_byte_count,
        } => SourceError::CommitmentTreeFrontierTooLarge {
            protocol,
            byte_count,
            max_byte_count,
        },
        CommitmentTreeFrontierValidationError::InvalidEncoding { reason } => {
            SourceError::InvalidCommitmentTreeFrontierEncoding { protocol, reason }
        }
        CommitmentTreeFrontierValidationError::TreeSizeOutOfRange { tree_size } => {
            SourceError::CommitmentTreeSizeOutOfRange {
                protocol,
                tree_size,
            }
        }
        CommitmentTreeFrontierValidationError::RootMismatch => {
            SourceError::CommitmentTreeFrontierRootMismatch { protocol }
        }
    }
}

fn commitment_tree_is_active(
    network_upgrade_activations: &NetworkUpgradeActivations,
    protocol: ShieldedProtocol,
    height: BlockHeight,
) -> bool {
    network_upgrade_activations
        .activation_height_by_name(protocol.activation_upgrade_name())
        .is_some_and(|activation_height| activation_height <= height)
}

fn decode_final_note_commitment_root_hex(
    protocol: ShieldedProtocol,
    root_hex: &str,
) -> Result<FinalNoteCommitmentRoot, SourceError> {
    let root_bytes = hex::decode(root_hex)
        .map_err(|source| SourceError::InvalidFinalNoteCommitmentRootHex { protocol, source })?;
    let byte_count = root_bytes.len();
    let root_bytes = <[u8; 32]>::try_from(root_bytes.as_slice()).map_err(|_| {
        SourceError::InvalidFinalNoteCommitmentRootLength {
            protocol,
            byte_count,
        }
    })?;
    Ok(FinalNoteCommitmentRoot::from_bytes(root_bytes))
}

fn decode_display_transaction_id(
    display_transaction_id: &str,
) -> Result<TransactionId, SourceError> {
    zinder_core::wire::decode_rpc_transaction_id_hex(display_transaction_id)
        .map_err(wire_error_to_transaction_id_error)
}

/// Encodes a [`TransactionId`] as a Zebra-display-order hex string for
/// JSON-RPC requests.
///
/// Internal Zinder storage holds canonical (network-order) txid bytes;
/// Zebra's RPC surface accepts and returns display-order (reversed) hex.
fn display_order_transaction_id_hex(transaction_id: TransactionId) -> String {
    zinder_core::wire::encode_rpc_transaction_id_hex(transaction_id)
}

fn decode_subtree_root_hash(root_hash: &str) -> Result<SubtreeRootHash, SourceError> {
    let root_hash_bytes =
        hex::decode(root_hash).map_err(|source| SourceError::InvalidSubtreeRootHex { source })?;
    let byte_count = root_hash_bytes.len();
    let root_hash_bytes = <[u8; 32]>::try_from(root_hash_bytes.as_slice())
        .map_err(|_| SourceError::InvalidSubtreeRootLength { byte_count })?;

    Ok(SubtreeRootHash::from_bytes(root_hash_bytes))
}

fn classify_broadcast_error(error: JsonRpcCallError) -> TransactionBroadcastOutcome {
    let JsonRpcCallError { code, message } = error;

    let Some(numeric_error_code) = code else {
        return TransactionBroadcastOutcome::Unknown(BroadcastUnknown {
            error_code: None,
            message,
        });
    };

    let error_code = Some(numeric_error_code);
    match i32::try_from(numeric_error_code).ok() {
        Some(JSON_RPC_INVALID_ENCODING_CODE) => {
            TransactionBroadcastOutcome::InvalidEncoding(BroadcastInvalidEncoding {
                error_code,
                message,
            })
        }
        Some(JSON_RPC_DUPLICATE_TRANSACTION_CODE) => {
            TransactionBroadcastOutcome::Duplicate(BroadcastDuplicate {
                error_code,
                message,
            })
        }
        _ if is_duplicate_transaction_message(&message) => {
            TransactionBroadcastOutcome::Duplicate(BroadcastDuplicate {
                error_code,
                message,
            })
        }
        _ => {
            if is_already_queued_message(&message) {
                TransactionBroadcastOutcome::Queued(BroadcastQueued { message })
            } else {
                let kind = classify_rejection_reason(&message);
                TransactionBroadcastOutcome::Rejected(BroadcastRejected {
                    kind,
                    error_code,
                    message,
                })
            }
        }
    }
}

/// Returns whether the upstream node reported a transaction already in its mempool.
///
/// Zebra versions do not consistently use the legacy `-27` code for this state.
/// Current nodes can return general code `-1` with this message, so the source
/// boundary classifies the stable semantic phrase before downstream consumers see it.
fn is_duplicate_transaction_message(message: &str) -> bool {
    let lowercased_message = message.to_ascii_lowercase();
    lowercased_message.contains("already exists in mempool")
        || lowercased_message.contains("already in mempool")
        || lowercased_message.contains("transaction is already in state")
}

/// Returns whether the upstream node reported the broadcast as queued.
///
/// Zebra emits this state through `MempoolError::AlreadyQueued`, whose
/// `Display` impl produces `"already queued for download"`. Detection is
/// case-insensitive on the distinctive `queued for download` substring so
/// future Zebra wording shifts (uppercase, prefixed sentence) keep matching.
fn is_already_queued_message(message: &str) -> bool {
    message.to_ascii_lowercase().contains("queued for download")
}

/// Maps the upstream rejection message to a typed reason.
///
/// The submitter is the only crate allowed to peek at Zebra's free-form
/// message strings (per ADR-0004); downstream consumers match on the typed
/// [`BroadcastRejectionReason`] instead.
fn classify_rejection_reason(message: &str) -> BroadcastRejectionReason {
    let lowercased_message = message.to_ascii_lowercase();
    // Zebra: `transaction version {0} not supported by the network upgrade {1:?}`.
    if lowercased_message.contains("not supported by the network upgrade") {
        return BroadcastRejectionReason::UnsupportedTransactionVersion;
    }
    // Zebra: `... exceeding the next block's ZIP 218 limit of {limit}`. Zakura:
    // `... exceeds the per-block limit of {limit}` and
    // `... exceeds the per-block global shielded budget of {limit}`.
    if lowercased_message.contains("zip 218 limit")
        || lowercased_message.contains("exceeds the per-block limit")
        || lowercased_message.contains("exceeds the per-block global shielded budget")
    {
        return BroadcastRejectionReason::ExceedsBlockActionLimit;
    }
    if lowercased_message.contains("mempool is full")
        || (lowercased_message.contains("mempool") && lowercased_message.contains("full"))
    {
        return BroadcastRejectionReason::MempoolFull;
    }
    if lowercased_message.contains("consensus branch") || lowercased_message.contains("branch id") {
        return BroadcastRejectionReason::BadConsensusBranch;
    }
    if lowercased_message.contains("expiry") || lowercased_message.contains("expired") {
        return BroadcastRejectionReason::BadExpiryHeight;
    }
    if lowercased_message.contains("invalid signature")
        || lowercased_message.contains("bad signature")
        || lowercased_message.contains("signature is invalid")
    {
        return BroadcastRejectionReason::InvalidSignature;
    }
    BroadcastRejectionReason::Unknown
}

/// Domain-shaped JSON-RPC `error` object after we strip jsonrpsee internals.
struct JsonRpcCallError {
    code: Option<i64>,
    message: String,
}

impl JsonRpcCallError {
    /// Returns whether the call error reports the requested resource was
    /// not found at the source.
    ///
    /// Zebra returns code -5 (`InvalidAddressOrKey`) both for unknown
    /// txids and for malformed input. Mempool hydration callers check
    /// this only after they have constructed the request from a typed
    /// [`TransactionId`], so a -5 response unambiguously means "not in
    /// mempool or main chain."
    fn is_not_found(&self) -> bool {
        matches!(
            self.code,
            Some(code) if i32::try_from(code).ok() == Some(JSON_RPC_INVALID_ADDRESS_OR_KEY_CODE)
        )
    }
}

impl From<ErrorObjectOwned> for JsonRpcCallError {
    fn from(error: ErrorObjectOwned) -> Self {
        Self {
            code: Some(i64::from(error.code())),
            message: error.message().to_owned(),
        }
    }
}

fn map_transport_error(
    error: &ClientError,
    operation: &'static str,
    max_response_bytes: NonZeroU64,
) -> SourceError {
    if source_response_too_large_for_operation(error, operation) {
        SourceError::SourceResponseTooLarge {
            operation,
            max_response_bytes: max_response_bytes.get(),
        }
    } else {
        SourceError::NodeUnavailable {
            reason: error.to_string(),
        }
    }
}

fn source_response_too_large(error: &SourceError) -> bool {
    matches!(error, SourceError::SourceResponseTooLarge { .. })
}

fn client_error_is_response_too_large(error: &ClientError) -> bool {
    matches!(error, ClientError::Transport(_))
        && error.to_string().contains("HTTP message was too big")
}

fn source_response_too_large_for_operation(error: &ClientError, operation: &'static str) -> bool {
    client_error_is_response_too_large(error)
        || (operation == "batch_getblock"
            && matches!(error, ClientError::ParseError(_))
            && error
                .to_string()
                .contains("invalid type: map, expected a sequence"))
}

/// Bridges a raw jsonrpsee result into the transport-class signal
/// [`ResilientClient::record_outcome`] consumes.
///
/// A server-side JSON-RPC error ([`ClientError::Call`]) means the wire
/// is fine but the server rejected the request; that must not advance
/// the rebuild counter. Every other [`ClientError`] variant is a
/// transport-layer failure (closed connection, deserialization on the
/// transport layer, dispatch failure) and surfaces as
/// [`SourceError::NodeUnavailable`] so the wrapper can react.
fn jsonrpsee_transport_signal<Response>(
    outcome: &Result<Response, ClientError>,
    operation: &'static str,
    max_response_bytes: NonZeroU64,
) -> Result<(), SourceError> {
    match outcome {
        Ok(_) | Err(ClientError::Call(_)) => Ok(()),
        Err(error) if source_response_too_large_for_operation(error, operation) => Ok(()),
        Err(error) => Err(map_transport_error(error, operation, max_response_bytes)),
    }
}

#[derive(Deserialize)]
struct ZebraBestBlockHeightAndHash {
    height: u32,
    hash: [u8; 32],
}

#[derive(Deserialize)]
struct ZebraSubtreeRootsByIndex {
    pool: String,
    start_index: u32,
    subtrees: Vec<ZebraSubtreeRoot>,
}

#[derive(Deserialize)]
struct ZebraGetTreestate {
    hash: String,
    height: u32,
    time: u32,
    #[serde(default)]
    sapling: Option<ZebraTreestate>,
    #[serde(default)]
    orchard: Option<ZebraTreestate>,
    #[serde(default)]
    ironwood: Option<ZebraTreestate>,
}

#[derive(Deserialize)]
struct ZebraTreestate {
    #[serde(default)]
    commitments: Option<ZebraCommitments>,
}

#[derive(Deserialize)]
struct ZebraCommitments {
    #[serde(rename = "finalRoot", default)]
    final_root: Option<String>,
    #[serde(rename = "finalState", default)]
    final_state: Option<String>,
}

#[derive(Deserialize)]
struct ZebraGetBlockValuePools {
    #[serde(default)]
    hash: Option<String>,
    #[serde(default)]
    height: Option<u32>,
    #[serde(default)]
    time: Option<i64>,
    #[serde(rename = "valuePools", default)]
    value_pools: Option<Vec<ZebraValuePoolEntry>>,
}

#[derive(Deserialize)]
struct ZebraGetBlockchainInfoUpgrades {
    // Preserve the node's advertised order. `getblockchaininfo` lists upgrades
    // in activation sequence, so when several share an activation height
    // (regtest activates every upgrade at height 1) the last entry is the
    // active one; a `BTreeMap` would reorder by branch-id hex and resolve the
    // tie to the wrong upgrade.
    #[serde(deserialize_with = "deserialize_upgrades_in_advertised_order")]
    upgrades: Vec<(String, ZebraNetworkUpgradeActivation)>,
}

fn deserialize_upgrades_in_advertised_order<'de, D>(
    deserializer: D,
) -> Result<Vec<(String, ZebraNetworkUpgradeActivation)>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct AdvertisedOrderVisitor;

    impl<'de> serde::de::Visitor<'de> for AdvertisedOrderVisitor {
        type Value = Vec<(String, ZebraNetworkUpgradeActivation)>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a map of consensus branch id to network upgrade info")
        }

        fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
        where
            M: serde::de::MapAccess<'de>,
        {
            let mut entries = Vec::with_capacity(map.size_hint().unwrap_or_default());
            while let Some(entry) = map.next_entry()? {
                entries.push(entry);
            }
            Ok(entries)
        }
    }

    deserializer.deserialize_map(AdvertisedOrderVisitor)
}

#[derive(Deserialize)]
struct ZebraGetBlockchainInfoValuePools {
    blocks: u32,
    #[serde(rename = "bestblockhash")]
    best_block_hash: String,
    #[serde(rename = "valuePools", default)]
    value_pools: Vec<ZebraValuePoolEntry>,
}

#[derive(Deserialize)]
struct ZebraGetBlockchainInfoHealth {
    blocks: u32,
    #[serde(rename = "estimatedheight", default)]
    estimated_height: Option<u32>,
    #[serde(rename = "verificationprogress", default)]
    verification_progress: Option<f64>,
}

#[derive(Deserialize)]
struct ZebraValuePoolEntry {
    id: String,
    monitored: bool,
    #[serde(rename = "chainValueZat")]
    chain_value_zat: Option<i64>,
}

#[derive(Deserialize)]
struct ZebraNetworkUpgradeActivation {
    name: String,
    #[serde(rename = "activationheight")]
    activation_height: u32,
}

#[derive(Deserialize)]
struct ZebraSubtreeRoot {
    root: String,
    end_height: u32,
}

#[cfg(test)]
mod tests {
    #![allow(
        missing_docs,
        reason = "Unit test names describe the behavior under test."
    )]

    use super::*;

    #[test]
    fn tree_state_payload_normalization_keeps_only_pool_frontiers() -> Result<(), SourceError> {
        let source_payload = serde_json::json!({
            "network": "regtest",
            "height": 42,
            "hash": "11".repeat(32),
            "time": 1_774_668_700,
            "sapling": {"commitments": {"finalRoot": "22".repeat(32), "finalState": "aa"}},
            "orchard": {"commitments": {"finalState": "bb"}},
            "ironwood": {"commitments": {"finalState": "cc"}},
        });

        let (block_time_seconds, payload_bytes) =
            normalize_zebra_tree_state_payload(&source_payload)?;
        let payload: Value = serde_json::from_slice(&payload_bytes)
            .map_err(|source| SourceError::SourcePayloadEncodingFailed { source })?;

        assert_eq!(block_time_seconds, 1_774_668_700);
        assert_eq!(
            payload,
            serde_json::json!({
                "sapling": {"commitments": {"finalState": "aa"}},
                "orchard": {"commitments": {"finalState": "bb"}},
                "ironwood": {"commitments": {"finalState": "cc"}},
            })
        );
        assert!(payload.get("height").is_none());
        assert!(payload.get("hash").is_none());
        assert!(payload.get("time").is_none());

        Ok(())
    }

    #[test]
    fn batch_response_error_object_is_treated_as_splittable_response_size() {
        let Err(parse_error) = serde_json::from_str::<Vec<Value>>("{}") else {
            return;
        };
        let error = ClientError::ParseError(parse_error);

        let source_error = map_transport_error(
            &error,
            "batch_getblock",
            DEFAULT_MAX_JSON_RPC_RESPONSE_BYTES,
        );

        assert!(matches!(
            source_error,
            SourceError::SourceResponseTooLarge {
                operation: "batch_getblock",
                max_response_bytes,
            } if max_response_bytes == DEFAULT_MAX_JSON_RPC_RESPONSE_BYTES.get()
        ));
    }

    #[test]
    fn offending_range_split_retry_accounting_counts_child_requests_and_union_blocks() {
        let parent =
            offending_range_split_retry_accounting(BlockHeight::new(10), BlockHeight::new(15));
        assert_eq!(parent.request_count, 2);
        assert_eq!(parent.block_count, 6);

        let recursively_split_child =
            offending_range_split_retry_accounting(BlockHeight::new(10), BlockHeight::new(12));
        assert_eq!(
            parent.request_count + recursively_split_child.request_count,
            4
        );
        assert_eq!(parent.block_count + recursively_split_child.block_count, 9);
    }

    #[test]
    fn upgrades_deserialize_in_advertised_order_not_hex_order() -> Result<(), eyre::Report> {
        // Heartwood precedes Canopy in the node's advertised order, but its
        // branch-id hex sorts after Canopy's; a hex-keyed map would swap them
        // and make Heartwood the active same-height upgrade.
        let json = r#"{
            "upgrades": {
                "f5b9230b": { "name": "Heartwood", "activationheight": 1 },
                "e9ff75a6": { "name": "Canopy", "activationheight": 1 }
            }
        }"#;
        let parsed: ZebraGetBlockchainInfoUpgrades = serde_json::from_str(json)?;
        let advertised: Vec<(&str, &str)> = parsed
            .upgrades
            .iter()
            .map(|(branch_id_hex, upgrade)| (branch_id_hex.as_str(), upgrade.name.as_str()))
            .collect();
        assert_eq!(
            advertised,
            [("f5b9230b", "Heartwood"), ("e9ff75a6", "Canopy")]
        );
        Ok(())
    }

    #[test]
    fn cookie_auth_builds_basic_authorization_from_file() -> Result<(), eyre::Report> {
        use std::io::Write;
        let mut cookie_file = tempfile::NamedTempFile::new()?;
        writeln!(cookie_file, "zebra:secret")?;

        let source = CookieSource::File(cookie_file.path().to_path_buf());
        let authorization = cookie_authorization_header(&source)?;

        assert_eq!(authorization, "Basic emVicmE6c2VjcmV0");
        Ok(())
    }

    #[test]
    fn cookie_auth_builds_basic_authorization_from_inline() -> Result<(), SourceError> {
        let source = CookieSource::Inline(SecretString::from("zebra:secret"));
        let authorization = cookie_authorization_header(&source)?;

        assert_eq!(authorization, "Basic emVicmE6c2VjcmV0");
        Ok(())
    }

    #[test]
    fn derive_authorization_headers_rereads_cookie_file_on_each_call() -> Result<(), eyre::Report> {
        use std::io::{Seek, SeekFrom, Write};

        let mut cookie_file = tempfile::NamedTempFile::new()?;
        writeln!(cookie_file, "first:secret-one")?;

        let source = CookieSource::File(cookie_file.path().to_path_buf());
        let auth = NodeAuth::Cookie(source);

        let headers_first = derive_authorization_headers(&auth)?;
        let initial_value = headers_first
            .get("authorization")
            .ok_or_else(|| eyre::eyre!("expected authorization header on first read"))?
            .to_str()?
            .to_owned();

        // Simulate Zebra rotating the cookie on restart by overwriting the
        // file at the same path. A rebuilder that captured headers at
        // construction would still emit `initial_value`; the helper must
        // re-read the file and surface the rotated credential.
        let mut handle = cookie_file.reopen()?;
        handle.set_len(0)?;
        handle.seek(SeekFrom::Start(0))?;
        writeln!(handle, "second:secret-two")?;
        handle.sync_all()?;

        let headers_second = derive_authorization_headers(&auth)?;
        let rotated_value = headers_second
            .get("authorization")
            .ok_or_else(|| eyre::eyre!("expected authorization header on second read"))?
            .to_str()?
            .to_owned();

        assert_ne!(
            initial_value, rotated_value,
            "rebuilder must re-read the cookie file so a Zebra-side rotation \
             surfaces in subsequent requests; captured header was reused instead",
        );
        Ok(())
    }

    #[test]
    fn rpc_basic_auth_allows_basic_or_no_auth() -> Result<(), SourceError> {
        ZebraJsonRpcSource::new(
            Network::ZcashRegtest,
            "http://127.0.0.1:18232",
            NodeAuth::basic("zebra", "zebra"),
            Duration::from_secs(1),
        )?;
        ZebraJsonRpcSource::new(
            Network::ZcashRegtest,
            "http://127.0.0.1:18232",
            NodeAuth::None,
            Duration::from_secs(1),
        )?;

        Ok(())
    }

    #[test]
    fn baseline_capabilities_omit_readiness_probe() {
        assert!(
            !ZebraJsonRpcSource::baseline_capabilities().supports(NodeCapability::ReadinessProbe)
        );
    }

    #[test]
    fn with_health_config_grants_readiness_probe_capability() -> Result<(), SourceError> {
        let source = ZebraJsonRpcSource::new(
            Network::ZcashRegtest,
            "http://127.0.0.1:18232",
            NodeAuth::None,
            Duration::from_secs(1),
        )?
        .with_health_config(Some(NodeHealthConfig::new(
            "http://127.0.0.1:18233/ready".to_owned(),
            Duration::from_secs(30),
            0.999,
            10,
        )));
        assert!(
            source
                .capabilities()
                .supports(NodeCapability::ReadinessProbe)
        );
        Ok(())
    }

    #[test]
    fn classify_broadcast_error_maps_already_queued_to_queued_variant() {
        let result = classify_broadcast_error(JsonRpcCallError {
            code: Some(i64::from(JSON_RPC_VERIFY_CODE)),
            message: "transaction was already queued for download".to_owned(),
        });

        assert!(matches!(
            result,
            TransactionBroadcastOutcome::Queued(BroadcastQueued { ref message })
                if message == "transaction was already queued for download"
        ));
    }

    #[test]
    fn classify_broadcast_error_maps_invalid_signature_to_typed_kind() {
        let result = classify_broadcast_error(JsonRpcCallError {
            code: Some(i64::from(JSON_RPC_VERIFY_CODE)),
            message: "transaction signature is invalid".to_owned(),
        });

        assert!(matches!(
            result,
            TransactionBroadcastOutcome::Rejected(BroadcastRejected {
                kind: BroadcastRejectionReason::InvalidSignature,
                ..
            })
        ));
    }

    #[test]
    fn classify_broadcast_error_maps_bad_expiry_height_to_typed_kind() {
        let result = classify_broadcast_error(JsonRpcCallError {
            code: Some(i64::from(JSON_RPC_VERIFY_CODE)),
            message: "transaction expiry height is past tip".to_owned(),
        });

        assert!(matches!(
            result,
            TransactionBroadcastOutcome::Rejected(BroadcastRejected {
                kind: BroadcastRejectionReason::BadExpiryHeight,
                ..
            })
        ));
    }

    #[test]
    fn classify_broadcast_error_maps_bad_consensus_branch_to_typed_kind() {
        let result = classify_broadcast_error(JsonRpcCallError {
            code: Some(i64::from(JSON_RPC_VERIFY_CODE)),
            message: "transaction consensus branch id does not match".to_owned(),
        });

        assert!(matches!(
            result,
            TransactionBroadcastOutcome::Rejected(BroadcastRejected {
                kind: BroadcastRejectionReason::BadConsensusBranch,
                ..
            })
        ));
    }

    #[test]
    fn classify_broadcast_error_maps_mempool_full_to_typed_kind() {
        let result = classify_broadcast_error(JsonRpcCallError {
            code: Some(i64::from(JSON_RPC_VERIFY_CODE)),
            message: "mempool is full".to_owned(),
        });

        assert!(matches!(
            result,
            TransactionBroadcastOutcome::Rejected(BroadcastRejected {
                kind: BroadcastRejectionReason::MempoolFull,
                ..
            })
        ));
    }

    fn assert_verify_rejection_kind(message: &str, expected_kind: BroadcastRejectionReason) {
        let result = classify_broadcast_error(JsonRpcCallError {
            code: Some(i64::from(JSON_RPC_VERIFY_CODE)),
            message: message.to_owned(),
        });

        assert!(
            matches!(
                result,
                TransactionBroadcastOutcome::Rejected(BroadcastRejected { kind, .. })
                    if kind == expected_kind
            ),
            "message {message:?} did not classify as {expected_kind:?}"
        );
    }

    #[test]
    fn classify_broadcast_error_maps_zebra_v4_after_nu7_to_unsupported_transaction_version() {
        // Zebra v7.0.0: `transaction version {0} not supported by the network upgrade {1:?}`.
        assert_verify_rejection_kind(
            "transaction did not pass consensus validation: transaction version 4 not supported by the network upgrade Nu7",
            BroadcastRejectionReason::UnsupportedTransactionVersion,
        );
        // The mempool prefixes cached rejections on later submissions.
        assert_verify_rejection_kind(
            "the transaction will be rejected from the mempool until the next chain tip block: transaction did not pass consensus validation: transaction version 4 not supported by the network upgrade Nu7",
            BroadcastRejectionReason::UnsupportedTransactionVersion,
        );
    }

    #[test]
    fn classify_broadcast_error_maps_zebra_zip_218_limit_to_exceeds_block_action_limit() {
        // Zebra v7.0.0: `mempool transaction has {count} {pool}, exceeding the next block's
        // ZIP 218 limit of {limit}`.
        for (pool, count, limit) in [
            ("Orchard actions", 331, 330),
            ("Ironwood actions", 331, 330),
            ("Sapling inputs and outputs", 301, 300),
            ("Sprout JoinSplits", 21, 20),
            ("shielded actions across all pools", 331, 330),
        ] {
            assert_verify_rejection_kind(
                &format!(
                    "transaction did not pass consensus validation: mempool transaction has {count} {pool}, exceeding the next block's ZIP 218 limit of {limit}"
                ),
                BroadcastRejectionReason::ExceedsBlockActionLimit,
            );
        }
    }

    #[test]
    fn classify_broadcast_error_maps_zakura_action_limits_to_exceeds_block_action_limit() {
        // Zakura v1.6.0 and v1.7.0: `{pool} count {count} exceeds the per-block limit of {limit}`
        // and `shielded cost {cost} (...) exceeds the per-block global shielded budget of {limit}`.
        for zakura_message in [
            "Orchard action count 331 exceeds the per-block limit of 330",
            "Ironwood action count 331 exceeds the per-block limit of 330",
            "Sapling spends + outputs count 301 exceeds the per-block limit of 300",
            "Sprout JoinSplit count 21 exceeds the per-block limit of 20",
            "shielded cost 331 (Orchard actions + Ironwood actions + Sapling spends + Sapling outputs + 2 * Sprout JoinSplits) exceeds the per-block global shielded budget of 330",
        ] {
            assert_verify_rejection_kind(
                &format!("transaction did not pass consensus validation: {zakura_message}"),
                BroadcastRejectionReason::ExceedsBlockActionLimit,
            );
        }
    }

    #[test]
    fn classify_broadcast_error_maps_zakura_v4_after_nu7_to_unsupported_transaction_version() {
        // Zakura reuses Zebra's `UnsupportedByNetworkUpgrade` message.
        assert_verify_rejection_kind(
            "transaction did not pass consensus validation: transaction version 4 not supported by the network upgrade Nu7",
            BroadcastRejectionReason::UnsupportedTransactionVersion,
        );
    }

    #[test]
    fn classify_broadcast_error_keeps_adjacent_zebra_messages_in_their_existing_kinds() {
        assert_verify_rejection_kind(
            "transaction did not pass consensus validation: transaction uses an incorrect consensus branch id",
            BroadcastRejectionReason::BadConsensusBranch,
        );
        // The version-floor rule is distinct from the network-upgrade rule and stays unclassified.
        assert_verify_rejection_kind(
            "transaction did not pass consensus validation: transaction version number MUST be >= 4",
            BroadcastRejectionReason::Unknown,
        );
    }

    #[test]
    fn classify_broadcast_error_defaults_unrecognized_rejection_to_unknown_kind() {
        let result = classify_broadcast_error(JsonRpcCallError {
            code: Some(i64::from(JSON_RPC_VERIFY_CODE)),
            message: "bad-txns-invalid".to_owned(),
        });

        assert!(matches!(
            result,
            TransactionBroadcastOutcome::Rejected(BroadcastRejected {
                kind: BroadcastRejectionReason::Unknown,
                ..
            })
        ));
    }

    #[test]
    fn with_health_config_clears_readiness_probe_when_unset() -> Result<(), SourceError> {
        let source = ZebraJsonRpcSource::new(
            Network::ZcashRegtest,
            "http://127.0.0.1:18232",
            NodeAuth::None,
            Duration::from_secs(1),
        )?
        .with_health_config(Some(NodeHealthConfig::new(
            "http://127.0.0.1:18233/ready".to_owned(),
            Duration::from_secs(30),
            0.999,
            10,
        )))
        .with_health_config(None);
        assert!(
            !source
                .capabilities()
                .supports(NodeCapability::ReadinessProbe)
        );
        Ok(())
    }
}
