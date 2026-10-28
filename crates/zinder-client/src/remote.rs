//! Remote gRPC implementation of the chain-index contract.

use std::collections::HashSet;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use tokio_stream::StreamExt as _;
use tonic::{
    Request,
    transport::{Channel, Endpoint},
};
use tonic_types::StatusExt as _;
use tracing::warn;
use zinder_core::wire::{
    decode_rpc_block_hash_hex, decode_rpc_merkle_root_hex, decode_rpc_transaction_id_hex,
    decode_zinder_native_chain_name, encode_rpc_block_hash_hex, encode_rpc_transaction_id_hex,
    encode_zinder_native_chain_name,
};
use zinder_core::{
    ArtifactSchemaVersion, BlockBlobArtifact, BlockHash, BlockHeader, BlockHeight,
    BlockHeightRange, BlockSelector, ChainEpoch, ChainEpochId, ChainValuePool,
    ChainValuePoolsAtTip, CompactBlockArtifact, ConsensusBranchId, MempoolEntry,
    MempoolEvictionReason, MinedTransaction, MinedTransactionChainContext, Network,
    NetworkUpgradeActivation, NetworkUpgradeActivations, RawTransactionBytes, ShieldedProtocol,
    SubtreeRootArtifact, SubtreeRootHash, SubtreeRootIndex, SubtreeRootRange,
    TransactionBroadcastOutcome, TransactionId, TransactionLocation, TransparentAddressBalance,
    TransparentAddressScriptHash, TransparentAddressTxIndexArtifact, TransparentMempoolOutput,
    TransparentMempoolOutputsRequest, TransparentMempoolSpend, TransparentOutPoint,
    TransparentOutputsByOutpointResponse, TransparentSpendEntry,
    TransparentSpendsByOutpointResponse, TransparentUnspentOutput,
    TransparentUnspentOutputsByOutpointResponse, TreeStateArtifact, TxStatus,
};
use zinder_proto::v1::wallet::{self, wallet_query_client::WalletQueryClient};
use zinder_proto::wire::{
    TransactionAbsence, WalletWireDecodeError, chain_epoch_from_message,
    compact_block_from_message as compact_block_from_wire_message,
    decode_transparent_utxo_set_commitment, mempool_entry_from_message, outpoint_message,
    transparent_mempool_output_from_message as transparent_mempool_output_from_message_shared,
    transparent_mempool_spend_from_message as transparent_mempool_spend_from_message_shared,
};

use crate::error::ZINDER_ERROR_DOMAIN;
use crate::{
    BlockId, Capability, CapabilityDescriptor, ChainEpochCommitted, ChainEvent, ChainEventCursor,
    ChainEventEnvelope, ChainEventStream, ChainEventStreamFamily, ChainIndex, ChainRangeReverted,
    EndpointBackedIndex, EventStreamStart, IndexStream, IndexerError, MempoolEvent,
    MempoolEventCursor, MempoolEventEnvelope, MempoolEventStream, MempoolSnapshotCursor,
    MempoolSnapshotRequest, MempoolSnapshotView, NodeServerInfo, ServerInfo,
    TransparentAddressTransactionChunk, TransparentAddressTxIdsQuery,
    TransparentAddressTxIdsStream, TransparentAddressUnspentOutputsQuery,
    TransparentAddressUnspentOutputsStream, TransparentHistoryCursor,
    TransparentUnspentOutputChunk, TransparentUtxoSetSummaryView,
};

/// Options for opening a remote chain index over the native wallet gRPC API.
#[derive(Clone, Debug)]
pub struct RemoteOpenOptions {
    /// Native `WalletQuery` endpoint URI.
    pub endpoint: String,
    /// Expected network served by the endpoint.
    pub network: Network,
}

/// Remote chain index backed by the native wallet gRPC API.
///
/// `RemoteChainIndex` is the recommended baseline for the Zallet-with-Zinder
/// operator recipe documented in
/// [Service operations §Zallet with Zinder](../../../docs/architecture/service-operations.md#zallet-with-zinder).
/// Zinder services that read storage directly use the service-internal
/// `WalletServingQuery` composition rather than this public client surface.
///
/// The underlying tonic `Channel` is configured with HTTP/2 keepalive and a
/// lazy connect: the connection is established on the first call and
/// re-established automatically after a transport failure. A half-open
/// connection is detected by the keepalive PING within
/// `KEEP_ALIVE_INTERVAL + KEEP_ALIVE_TIMEOUT`, so a stalled call errors out
/// instead of hanging forever. The channel multiplexes concurrent calls over
/// one HTTP/2 connection.
///
/// The channel is held behind an [`ArcSwap`] so the index can self-heal from
/// the tonic 0.14 failure mode where a stream-level h2 error leaves the
/// internal multiplexer in a state where every subsequent call returns an
/// empty `Code::Unavailable` without re-attempting the TCP/h2 handshake.
/// When a call surfaces that exact signature (Unavailable with no
/// `zinder.dev` `ErrorInfo` trailer), the index atomically swaps in a fresh
/// lazy channel built from the saved [`Endpoint`]; the next call dials a new
/// connection. Cloning the type via [`Clone`] is cheap and intentionally
/// shares the swappable channel slot.
#[derive(Clone)]
pub struct RemoteChainIndex {
    client: Arc<ArcSwap<WalletQueryClient<Channel>>>,
    endpoint: Endpoint,
    network: Network,
}

/// Cadence at which HTTP/2 PING frames are sent over the channel; with
/// [`KEEP_ALIVE_TIMEOUT`], this is what makes a half-open connection
/// detectable instead of hanging in-flight calls forever.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(20);

/// Time the channel waits for a PONG to a keepalive PING before deciding the
/// connection is dead and tearing it down so the next call re-dials.
const KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// Upper bound on the initial TCP+HTTP/2 handshake when the channel
/// establishes (or re-establishes) the connection lazily.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// OS-level TCP keepalive idle interval. Belt-and-braces alongside the
/// application-level HTTP/2 PING: detects connections dropped silently by
/// intermediaries that don't surface the failure to userspace.
const TCP_KEEPALIVE: Duration = Duration::from_mins(1);

/// Oldest native wallet contract revision this client can safely consume.
pub const MIN_SUPPORTED_CONTRACT_REVISION: u32 = 5;

impl RemoteChainIndex {
    /// Builds a remote-chain-index handle pointed at a `WalletQuery` endpoint.
    ///
    /// The channel is constructed lazily: the TCP+HTTP/2 connection is
    /// established on the first gRPC call, not here. Only URI parsing can
    /// fail at this stage; transport errors surface at first use.
    pub fn connect(options: RemoteOpenOptions) -> Result<Self, IndexerError> {
        // URI scheme is the TLS signal: `https://` negotiates rustls with system roots.
        let use_tls = options.endpoint.starts_with("https://");
        let mut endpoint = Channel::from_shared(options.endpoint)
            .map_err(|error| IndexerError::invalid_request(error.to_string()))?;
        if use_tls {
            endpoint = endpoint
                .tls_config(tonic::transport::ClientTlsConfig::new().with_native_roots())
                .map_err(|error| IndexerError::invalid_request(error.to_string()))?;
        }
        let endpoint = endpoint
            .keep_alive_while_idle(true)
            .http2_keep_alive_interval(KEEP_ALIVE_INTERVAL)
            .keep_alive_timeout(KEEP_ALIVE_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .tcp_keepalive(Some(TCP_KEEPALIVE));
        let channel = endpoint.connect_lazy();

        Ok(Self {
            client: Arc::new(ArcSwap::new(Arc::new(WalletQueryClient::new(channel)))),
            endpoint,
            network: options.network,
        })
    }

    /// Returns a per-call `WalletQueryClient` clone backed by the current
    /// swappable channel slot. Cloning the client is cheap and does not
    /// allocate a new connection; concurrent callers share the underlying
    /// HTTP/2 multiplexer until [`Self::rebuild_channel`] swaps it out.
    fn client(&self) -> WalletQueryClient<Channel> {
        (**self.client.load()).clone()
    }

    /// Maps a tonic [`Status`](tonic::Status) into the typed [`IndexerError`]
    /// and rebuilds the underlying gRPC channel when the status carries the
    /// "poisoned multiplexer" signature, i.e. `Code::Unavailable` (or
    /// `Code::Unknown`, which tonic uses for some transport-level failures)
    /// with no `zinder.dev` `ErrorInfo` trailer. Application-level errors
    /// like `Code::InvalidArgument` always preserve the channel even when
    /// the upstream omitted the trailer, since those are caller bugs that
    /// reconnecting would not fix.
    fn map_status(&self, status: tonic::Status) -> IndexerError {
        let poisoned_transport = matches!(
            status.code(),
            tonic::Code::Unavailable | tonic::Code::Unknown
        ) && status
            .get_error_details()
            .error_info()
            .is_none_or(|error_info| error_info.domain != ZINDER_ERROR_DOMAIN);
        let err = IndexerError::from_status(status);
        if poisoned_transport {
            self.rebuild_channel();
        }
        err
    }

    /// Maps a failed transaction lookup to [`TxStatus::NotFound`] only for the
    /// typed `TRANSACTION_NOT_FOUND` absence signal.
    fn tx_status_from_lookup_failure(
        &self,
        status: tonic::Status,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TxStatus, IndexerError> {
        match TransactionAbsence::from_status(&status) {
            Ok(Some(absence)) => {
                verify_transaction_absence(absence, at_epoch_id).map(|()| TxStatus::NotFound)
            }
            Ok(None) if is_unverified_transaction_absence(&status) => {
                Err(IndexerError::TransactionAbsenceUnverified {
                    message: status.message().to_owned(),
                })
            }
            Ok(None) => Err(self.map_status(status)),
            Err(metadata_error) => Err(IndexerError::malformed(
                "error_info.metadata",
                metadata_error.to_string(),
            )),
        }
    }

    fn rebuild_channel(&self) {
        let fresh = self.endpoint.connect_lazy();
        self.client.store(Arc::new(WalletQueryClient::new(fresh)));
        warn!(
            target: "zinder_client",
            event = "remote_channel_rebuilt",
            "rebuilt remote chain index gRPC channel after poisoned-transport failure"
        );
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "The remote implementation mirrors the public ChainIndex trait one method at a time."
)]
#[async_trait]
impl ChainIndex for RemoteChainIndex {
    async fn current_epoch(&self) -> Result<ChainEpoch, IndexerError> {
        let response = self
            .client()
            .visible_tip_block(Request::new(wallet::VisibleTipBlockRequest {
                at_epoch_id: None,
            }))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();

        chain_epoch_from_chain_view_with_network(self.network, response.chain_view)
    }

    async fn network_upgrade_activations(&self) -> Result<NetworkUpgradeActivations, IndexerError> {
        let server_info = EndpointBackedIndex::server_info(self).await?;
        ensure_advertised_capability(&server_info, Capability::NetworkUpgradeActivations)?;

        let response = self
            .client()
            .network_upgrade_activations(Request::new(wallet::NetworkUpgradeActivationsRequest {}))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();

        network_upgrade_activations_from_message(self.network, response)
    }

    async fn visible_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockId, IndexerError> {
        let response = self
            .client()
            .visible_tip_block(Request::new(wallet::VisibleTipBlockRequest {
                at_epoch_id: at_epoch_id.map(ChainEpochId::value),
            }))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();
        let chain_epoch =
            chain_epoch_from_chain_view_with_pin(self.network, at_epoch_id, response.chain_view)?;
        let visible_tip_block = response
            .visible_tip_block
            .ok_or_else(|| IndexerError::malformed("visible_tip_block", "field is missing"))?;
        let block = BlockId {
            height: BlockHeight::new(visible_tip_block.height),
            hash: block_hash_from_rpc_hex(
                "visible_tip_block.block_hash",
                &visible_tip_block.block_hash,
            )?,
        };
        if block.height != chain_epoch.visible_tip_height
            || block.hash != chain_epoch.visible_tip_hash
        {
            return Err(IndexerError::malformed(
                "visible_tip_block",
                "block does not match chain_view.chain_epoch.visible_tip",
            ));
        }
        Ok(block)
    }

    async fn settled_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockId, IndexerError> {
        let response = self
            .client()
            .settled_tip_block(Request::new(wallet::SettledTipBlockRequest {
                at_epoch_id: at_epoch_id.map(ChainEpochId::value),
            }))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();
        let chain_epoch =
            chain_epoch_from_chain_view_with_pin(self.network, at_epoch_id, response.chain_view)?;
        let settled_tip_block = response
            .settled_tip_block
            .ok_or_else(|| IndexerError::malformed("settled_tip_block", "field is missing"))?;

        let block = BlockId {
            height: BlockHeight::new(settled_tip_block.height),
            hash: block_hash_from_rpc_hex(
                "settled_tip_block.block_hash",
                &settled_tip_block.block_hash,
            )?,
        };
        if block.height != chain_epoch.settled_tip_height
            || block.hash != chain_epoch.settled_tip_hash
        {
            return Err(IndexerError::malformed(
                "settled_tip_block",
                "block does not match chain_view.chain_epoch.settled_tip",
            ));
        }
        Ok(block)
    }

    async fn block_id_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockId, IndexerError> {
        let response = self
            .client()
            .block_id_by_selector(Request::new(wallet::BlockSelectorRequest {
                selector: Some(block_selector_to_message(selector)?),
                at_epoch_id: at_epoch_id.map(ChainEpochId::value),
            }))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();
        block_id_from_selector_response(self.network, at_epoch_id, selector, response)
    }

    async fn block_header_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockHeader, IndexerError> {
        let response = self
            .client()
            .block_header_by_selector(Request::new(wallet::BlockSelectorRequest {
                selector: Some(block_selector_to_message(selector)?),
                at_epoch_id: at_epoch_id.map(ChainEpochId::value),
            }))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();
        block_header_from_selector_response(self.network, at_epoch_id, selector, response)
    }

    async fn compact_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockArtifact, IndexerError> {
        let response = self
            .client()
            .compact_block(Request::new(wallet::CompactBlockRequest {
                height: height.value(),
                at_epoch_id: at_epoch_id.map(ChainEpochId::value),
            }))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();
        let chain_epoch =
            chain_epoch_from_chain_view_with_pin(self.network, at_epoch_id, response.chain_view)?;
        let artifact = compact_block_from_message(
            response
                .compact_block
                .ok_or_else(|| IndexerError::malformed("compact_block", "field is missing"))?,
        )?;
        if artifact.height() != height || artifact.height() > chain_epoch.visible_tip_height {
            return Err(IndexerError::malformed(
                "compact_block.height",
                "compact block identity does not match the request and response chain view",
            ));
        }
        Ok(artifact)
    }

    async fn compact_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<IndexStream<CompactBlockArtifact>, IndexerError> {
        let response = self
            .client()
            .compact_blocks_in_range(Request::new(wallet::CompactBlocksInRangeRequest {
                start_height: block_range.start.value(),
                end_height: block_range.end.value(),
                at_epoch_id: at_epoch_id.map(ChainEpochId::value),
            }))
            .await
            .map_err(|status| self.map_status(status))?;
        let recovery = self.clone();
        let expected_network = self.network;
        let mut streamed_epoch = None;
        let next_height = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(u64::from(
            block_range.start.value(),
        )));
        let next_height_while_decoding = std::sync::Arc::clone(&next_height);
        let stream = response.into_inner().map(move |chunk_result| {
            let chunk = chunk_result.map_err(|status| recovery.map_status(status))?;
            let chain_epoch = chain_epoch_from_chain_view_with_pin(
                expected_network,
                at_epoch_id,
                chunk.chain_view,
            )?;
            match streamed_epoch {
                None => streamed_epoch = Some(chain_epoch),
                Some(epoch) if epoch == chain_epoch => {}
                Some(_) => {
                    return Err(IndexerError::malformed(
                        "chain_view.chain_epoch.chain_epoch_id",
                        "compact block stream changed chain epoch",
                    ));
                }
            }
            let artifact =
                compact_block_from_message(chunk.compact_block.ok_or_else(|| {
                    IndexerError::malformed("compact_block", "field is missing")
                })?)?;
            if artifact.height() > chain_epoch.visible_tip_height {
                return Err(IndexerError::malformed(
                    "compact_block.height",
                    "compact block exceeds chain-view visible tip",
                ));
            }
            let expected_value =
                next_height_while_decoding.load(std::sync::atomic::Ordering::Acquire);
            let expected = u32::try_from(expected_value)
                .map(BlockHeight::new)
                .map_err(|_| {
                    IndexerError::malformed(
                        "compact_block.height",
                        "compact block stream exceeded requested end height",
                    )
                })?;
            if artifact.height() != expected {
                return Err(IndexerError::malformed(
                    "compact_block.height",
                    format!(
                        "expected streamed height {}, observed {}",
                        expected.value(),
                        artifact.height().value()
                    ),
                ));
            }
            let following_height = if expected == block_range.end {
                u64::from(u32::MAX) + 1
            } else {
                u64::from(expected.value()) + 1
            };
            next_height_while_decoding
                .store(following_height, std::sync::atomic::Ordering::Release);
            Ok(artifact)
        });
        let terminal = futures_util::StreamExt::filter_map(
            futures_util::stream::once(async move {
                incomplete_compact_block_stream_error(
                    next_height.load(std::sync::atomic::Ordering::Acquire),
                    block_range.end,
                )
                .map(Err)
            }),
            |terminal_result| async move { terminal_result },
        );

        Ok(Box::pin(futures_util::StreamExt::chain(stream, terminal)))
    }

    async fn full_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockBlobArtifact, IndexerError> {
        let response = self
            .client()
            .full_block(Request::new(wallet::FullBlockRequest {
                height: height.value(),
                at_epoch_id: at_epoch_id.map(ChainEpochId::value),
            }))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();
        let chain_epoch =
            chain_epoch_from_chain_view_with_pin(self.network, at_epoch_id, response.chain_view)?;
        let artifact = full_block_from_message(
            response
                .full_block
                .ok_or_else(|| IndexerError::malformed("full_block", "field is missing"))?,
        )?;
        if artifact.height != height || artifact.height > chain_epoch.visible_tip_height {
            return Err(IndexerError::malformed(
                "full_block.height",
                "full block identity does not match the request and response chain view",
            ));
        }
        Ok(artifact)
    }

    async fn full_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<IndexStream<BlockBlobArtifact>, IndexerError> {
        let response = self
            .client()
            .full_blocks_in_range(Request::new(wallet::FullBlocksInRangeRequest {
                start_height: block_range.start.value(),
                end_height: block_range.end.value(),
                at_epoch_id: at_epoch_id.map(ChainEpochId::value),
            }))
            .await
            .map_err(|status| self.map_status(status))?;
        let recovery = self.clone();
        let expected_network = self.network;
        let mut streamed_epoch = None;
        let next_height = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(u64::from(
            block_range.start.value(),
        )));
        let next_height_while_decoding = std::sync::Arc::clone(&next_height);
        let stream = response.into_inner().map(move |chunk_result| {
            let chunk = chunk_result.map_err(|status| recovery.map_status(status))?;
            let chain_epoch = chain_epoch_from_chain_view_with_pin(
                expected_network,
                at_epoch_id,
                chunk.chain_view,
            )?;
            match streamed_epoch {
                None => streamed_epoch = Some(chain_epoch),
                Some(epoch) if epoch == chain_epoch => {}
                Some(_) => {
                    return Err(IndexerError::malformed(
                        "chain_view.chain_epoch.chain_epoch_id",
                        "full block stream changed chain epoch",
                    ));
                }
            }
            let artifact = full_block_from_message(
                chunk
                    .full_block
                    .ok_or_else(|| IndexerError::malformed("full_block", "field is missing"))?,
            )?;
            let expected_value =
                next_height_while_decoding.load(std::sync::atomic::Ordering::Acquire);
            let expected = u32::try_from(expected_value)
                .map(BlockHeight::new)
                .map_err(|_| {
                    IndexerError::malformed(
                        "full_block.height",
                        "full block stream exceeded requested end height",
                    )
                })?;
            if artifact.height != expected || artifact.height > chain_epoch.visible_tip_height {
                return Err(IndexerError::malformed(
                    "full_block.height",
                    "full block stream identity does not match the request and response chain view",
                ));
            }
            let following_height = if expected == block_range.end {
                u64::from(u32::MAX) + 1
            } else {
                u64::from(expected.value()) + 1
            };
            next_height_while_decoding
                .store(following_height, std::sync::atomic::Ordering::Release);
            Ok(artifact)
        });
        let terminal = futures_util::StreamExt::filter_map(
            futures_util::stream::once(async move {
                incomplete_full_block_stream_error(
                    next_height.load(std::sync::atomic::Ordering::Acquire),
                    block_range.end,
                )
                .map(Err)
            }),
            |terminal_result| async move { terminal_result },
        );

        Ok(Box::pin(futures_util::StreamExt::chain(stream, terminal)))
    }

    async fn tree_state_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeStateArtifact, IndexerError> {
        let response = self
            .client()
            .tree_state_at_height(Request::new(wallet::TreeStateAtHeightRequest {
                height: height.value(),
                at_epoch_id: at_epoch_id.map(ChainEpochId::value),
            }))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();
        let chain_epoch = chain_epoch_from_chain_view_with_pin(
            self.network,
            at_epoch_id,
            response.chain_view.clone(),
        )?;
        let artifact = tree_state_from_response(response)?;
        if artifact.height != height {
            return Err(IndexerError::malformed(
                "tree_state.height",
                format!(
                    "expected requested height {}, observed {}",
                    height.value(),
                    artifact.height.value()
                ),
            ));
        }
        if artifact.height > chain_epoch.visible_tip_height {
            return Err(IndexerError::malformed(
                "tree_state.height",
                "tree state exceeds chain-view visible tip",
            ));
        }
        Ok(artifact)
    }

    async fn latest_tree_state_checkpoint(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeStateArtifact, IndexerError> {
        let response = self
            .client()
            .latest_tree_state_checkpoint(Request::new(wallet::LatestTreeStateCheckpointRequest {
                at_epoch_id: at_epoch_id.map(ChainEpochId::value),
            }))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();
        let chain_epoch = chain_epoch_from_chain_view_with_pin(
            self.network,
            at_epoch_id,
            response.chain_view.clone(),
        )?;
        let artifact = tree_state_from_response(response)?;
        if artifact.height > chain_epoch.visible_tip_height {
            return Err(IndexerError::malformed(
                "tree_state.height",
                "tree-state checkpoint exceeds chain-view visible tip",
            ));
        }
        Ok(artifact)
    }

    async fn subtree_roots_in_range(
        &self,
        subtree_root_range: SubtreeRootRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<Vec<SubtreeRootArtifact>, IndexerError> {
        let response = self
            .client()
            .subtree_roots(Request::new(wallet::SubtreeRootsRequest {
                shielded_protocol: shielded_protocol_to_message(subtree_root_range.protocol)?
                    as i32,
                start_index: subtree_root_range.start_index.value(),
                max_entries: subtree_root_range.max_entries.get(),
                at_epoch_id: at_epoch_id.map(ChainEpochId::value),
            }))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();
        subtree_roots_from_response(self.network, at_epoch_id, subtree_root_range, response)
    }

    async fn transaction_by_id(
        &self,
        transaction_id: TransactionId,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TxStatus, IndexerError> {
        let response = match self
            .client()
            .transaction(Request::new(wallet::TransactionRequest {
                transaction_id: encode_rpc_transaction_id_hex(transaction_id),
                at_epoch_id: at_epoch_id.map(ChainEpochId::value),
            }))
            .await
        {
            Ok(response) => response.into_inner(),
            Err(status) => return self.tx_status_from_lookup_failure(status, at_epoch_id),
        };
        tx_status_from_message(self.network, transaction_id, at_epoch_id, response)
    }

    async fn transparent_address_unspent_outputs(
        &self,
        query: TransparentAddressUnspentOutputsQuery,
    ) -> Result<TransparentAddressUnspentOutputsStream, IndexerError> {
        let request = wallet::TransparentAddressUnspentOutputsRequest {
            address: Some(wallet::AddressLookup {
                selector: Some(wallet::address_lookup::Selector::ScriptHash(
                    query.address_script_hash.as_bytes().to_vec(),
                )),
            }),
            start_height: query.start_height.value(),
            at_epoch_id: query.at_epoch_id.map(ChainEpochId::value),
        };
        let response = self
            .client()
            .transparent_address_unspent_outputs(Request::new(request))
            .await
            .map_err(|status| self.map_status(status))?;
        let expected_network = self.network;
        let expected_epoch_id = query.at_epoch_id;
        let recovery = self.clone();
        // The leading header pins the chain epoch for the whole stream; the
        // closure captures it and drops the header (yielding no item).
        let mut pinned_chain_epoch: Option<ChainEpoch> = None;
        let header_seen = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let header_seen_while_decoding = std::sync::Arc::clone(&header_seen);
        let stream = response.into_inner().filter_map(move |message_result| {
            message_result
                .map_err(|status| recovery.map_status(status))
                .and_then(|message| {
                    transparent_unspent_output_stream_item(
                        expected_network,
                        expected_epoch_id,
                        &mut pinned_chain_epoch,
                        &header_seen_while_decoding,
                        message,
                    )
                })
                .transpose()
        });
        let terminal = futures_util::StreamExt::filter_map(
            futures_util::stream::once(async move {
                missing_transparent_unspent_header_error(
                    header_seen.load(std::sync::atomic::Ordering::Acquire),
                )
                .map(Err)
            }),
            |terminal_result| async move { terminal_result },
        );
        Ok(Box::pin(futures_util::StreamExt::chain(stream, terminal)))
    }

    async fn transparent_address_tx_ids_in_range(
        &self,
        query: TransparentAddressTxIdsQuery,
    ) -> Result<TransparentAddressTxIdsStream, IndexerError> {
        let address_script_hash = query.address_script_hash;
        let expected_epoch_id = query.at_epoch_id;
        let request = wallet::TransparentAddressTxIdsInRangeRequest {
            address: Some(wallet::AddressLookup {
                selector: Some(wallet::address_lookup::Selector::ScriptHash(
                    address_script_hash.as_bytes().to_vec(),
                )),
            }),
            start_height: query.start_height.value(),
            end_height: query.end_height.value(),
            max_entries: query.max_entries.map(NonZeroU32::get).unwrap_or_default(),
            from_cursor: query
                .from_cursor
                .as_ref()
                .map(|cursor| cursor.as_bytes().to_vec())
                .unwrap_or_default(),
            descending: query.descending,
        };
        let response = self
            .client()
            .transparent_address_tx_ids_in_range(Request::new(request))
            .await
            .map_err(|status| self.map_status(status))?;
        let expected_network = self.network;
        let recovery = self.clone();
        let header_seen = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stream_failed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let history_stream_state = TransparentHistoryStreamState {
            expected_network,
            expected_epoch_id,
            address_script_hash,
            pinned_chain_epoch: None,
            header_seen: std::sync::Arc::clone(&header_seen),
            stream_failed: std::sync::Arc::clone(&stream_failed),
        };
        let wire_stream =
            tokio_stream::StreamExt::map(response.into_inner(), move |chunk_result| {
                chunk_result.map_err(|status| recovery.map_status(status))
            });
        Ok(transparent_address_tx_ids_stream(
            Box::pin(wire_stream),
            history_stream_state,
        ))
    }

    async fn transparent_address_balance(
        &self,
        addresses: &[TransparentAddressScriptHash],
    ) -> Result<TransparentAddressBalance, IndexerError> {
        let wire_addresses = addresses
            .iter()
            .map(|script_hash| wallet::AddressLookup {
                selector: Some(wallet::address_lookup::Selector::ScriptHash(
                    script_hash.as_bytes().to_vec(),
                )),
            })
            .collect();
        let request = wallet::TransparentAddressBalanceRequest {
            addresses: wire_addresses,
            at_epoch_id: None,
        };
        let response = self
            .client()
            .transparent_address_balance(Request::new(request))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();
        transparent_address_balance_from_message(self.network, response)
    }

    async fn transparent_outputs_by_outpoint(
        &self,
        outpoints: &[TransparentOutPoint],
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentOutputsByOutpointResponse, IndexerError> {
        let wire_outpoints = outpoints.iter().map(outpoint_message).collect();
        let request = wallet::TransparentOutputsByOutpointRequest {
            outpoints: wire_outpoints,
            at_epoch_id: at_epoch_id.map(ChainEpochId::value),
        };
        let response = self
            .client()
            .transparent_outputs_by_outpoint(Request::new(request))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();
        transparent_outputs_by_outpoint_response_from_message(self.network, response)
    }

    async fn transparent_spends_by_outpoint(
        &self,
        outpoints: &[TransparentOutPoint],
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentSpendsByOutpointResponse, IndexerError> {
        let wire_outpoints = outpoints.iter().map(outpoint_message).collect();
        let request = wallet::TransparentSpendsByOutpointRequest {
            outpoints: wire_outpoints,
            at_epoch_id: at_epoch_id.map(ChainEpochId::value),
        };
        let response = self
            .client()
            .transparent_spends_by_outpoint(Request::new(request))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();
        transparent_spends_by_outpoint_response_from_message(self.network, response)
    }

    async fn transparent_unspent_outputs_by_outpoint(
        &self,
        outpoints: &[TransparentOutPoint],
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentUnspentOutputsByOutpointResponse, IndexerError> {
        let wire_outpoints = outpoints.iter().map(outpoint_message).collect();
        let request = wallet::TransparentUnspentOutputsByOutpointRequest {
            outpoints: wire_outpoints,
            at_epoch_id: at_epoch_id.map(ChainEpochId::value),
        };
        let response = self
            .client()
            .transparent_unspent_outputs_by_outpoint(Request::new(request))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();
        transparent_unspent_outputs_by_outpoint_response_from_message(self.network, response)
    }

    async fn transparent_utxo_set_summary(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentUtxoSetSummaryView, IndexerError> {
        let request = wallet::TransparentUtxoSetSummaryRequest {
            at_epoch_id: at_epoch_id.map(ChainEpochId::value),
        };
        let response = self
            .client()
            .transparent_utxo_set_summary(Request::new(request))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();
        let chain_epoch =
            chain_epoch_from_chain_view_with_network(self.network, response.chain_view)?;
        let commitment = response
            .commitment
            .map(|message| {
                decode_transparent_utxo_set_commitment(&message)
                    .map_err(|error| IndexerError::malformed("commitment", error.to_string()))
            })
            .transpose()?;
        Ok(TransparentUtxoSetSummaryView {
            chain_epoch,
            summarized_height: BlockHeight::new(response.summarized_height),
            utxo_count: response.utxo_count,
            total_value_zat: response.total_value_zat,
            commitment,
        })
    }
}

fn missing_transparent_unspent_header_error(header_seen: bool) -> Option<IndexerError> {
    (!header_seen).then(|| {
        IndexerError::malformed(
            "transparent_unspent_outputs.header",
            "stream ended before the required chain-view header",
        )
    })
}

fn missing_transparent_history_header_error(
    header_seen: bool,
    stream_failed: bool,
) -> Option<IndexerError> {
    (!header_seen && !stream_failed).then(|| {
        IndexerError::malformed(
            "transparent_address_tx_ids.header",
            "stream ended before the required chain-view header",
        )
    })
}

fn incomplete_compact_block_stream_error(
    next_height: u64,
    end_height: BlockHeight,
) -> Option<IndexerError> {
    u32::try_from(next_height).is_ok().then(|| {
        IndexerError::malformed(
            "compact_block.height",
            format!(
                "compact block stream ended before requested height {}; next expected height was {next_height}",
                end_height.value()
            ),
        )
    })
}

fn incomplete_full_block_stream_error(
    next_height: u64,
    end_height: BlockHeight,
) -> Option<IndexerError> {
    u32::try_from(next_height).is_ok().then(|| {
        IndexerError::malformed(
            "full_block.height",
            format!(
                "full block stream ended before requested height {}; next expected height was {next_height}",
                end_height.value()
            ),
        )
    })
}

fn subtree_roots_from_response(
    expected_network: Network,
    expected_epoch_id: Option<ChainEpochId>,
    subtree_root_range: SubtreeRootRange,
    response: wallet::SubtreeRootsResponse,
) -> Result<Vec<SubtreeRootArtifact>, IndexerError> {
    let _chain_epoch = chain_epoch_from_chain_view_with_pin(
        expected_network,
        expected_epoch_id,
        response.chain_view.clone(),
    )?;
    let protocol = shielded_protocol_from_message(response.shielded_protocol)?;
    if protocol != subtree_root_range.protocol {
        return Err(IndexerError::malformed(
            "shielded_protocol",
            "response protocol differs from request",
        ));
    }
    if response.start_index != subtree_root_range.start_index.value() {
        return Err(IndexerError::malformed(
            "start_index",
            "response start index differs from request",
        ));
    }
    if response.subtree_roots.len()
        > usize::try_from(subtree_root_range.max_entries.get()).unwrap_or(usize::MAX)
    {
        return Err(IndexerError::malformed(
            "subtree_roots",
            "response exceeds requested maximum entry count",
        ));
    }
    response
        .subtree_roots
        .into_iter()
        .enumerate()
        .map(|(offset, root)| {
            let offset = u32::try_from(offset).map_err(|_| {
                IndexerError::malformed("subtree_roots", "root offset exceeds u32::MAX")
            })?;
            let expected_index = subtree_root_range
                .start_index
                .value()
                .checked_add(offset)
                .ok_or_else(|| {
                    IndexerError::malformed("subtree_roots", "root index exceeds u32::MAX")
                })?;
            if root.subtree_index != expected_index {
                return Err(IndexerError::malformed(
                    "subtree_roots.subtree_index",
                    format!(
                        "expected subtree index {expected_index}, observed {}",
                        root.subtree_index
                    ),
                ));
            }
            subtree_root_from_message(protocol, root)
        })
        .collect()
}

#[async_trait]
impl EndpointBackedIndex for RemoteChainIndex {
    async fn server_info(&self) -> Result<ServerInfo, IndexerError> {
        let response = self
            .client()
            .server_info(Request::new(wallet::ServerInfoRequest {}))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();
        let wallet_info = response
            .info
            .ok_or_else(|| IndexerError::malformed("info", "field is missing"))?;
        server_info_from_message(self.network, wallet_info)
    }

    async fn chain_value_pools_at_tip(&self) -> Result<ChainValuePoolsAtTip, IndexerError> {
        let response = self
            .client()
            .chain_value_pools_at_tip(Request::new(wallet::ChainValuePoolsAtTipRequest {}))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();
        chain_value_pools_at_tip_from_message(self.network, response)
    }

    async fn broadcast_transaction(
        &self,
        raw_transaction: RawTransactionBytes,
    ) -> Result<TransactionBroadcastOutcome, IndexerError> {
        let response = self
            .client()
            .broadcast_transaction(Request::new(wallet::BroadcastTransactionRequest {
                raw_transaction: raw_transaction.as_slice().to_vec(),
            }))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();
        transaction_broadcast_outcome_from_message(response)
    }

    async fn chain_events_for_family(
        &self,
        start: EventStreamStart<ChainEventCursor>,
        family: ChainEventStreamFamily,
    ) -> Result<ChainEventStream, IndexerError> {
        self.chain_events_with_filter(start, family, Vec::new())
            .await
    }

    async fn chain_events_with_filter(
        &self,
        start: EventStreamStart<ChainEventCursor>,
        family: ChainEventStreamFamily,
        address_filter: Vec<String>,
    ) -> Result<ChainEventStream, IndexerError> {
        let response = self
            .client()
            .chain_events(Request::new(wallet::ChainEventsRequest {
                start: Some(event_stream_start_to_message(&start, |cursor| {
                    cursor.as_bytes()
                })),
                family: chain_event_stream_family_to_message(family) as i32,
                address_filter,
            }))
            .await
            .map_err(|status| self.map_status(status))?;
        let expected_network = self.network;
        let recovery = self.clone();
        let mut stream_state = ChainEventStreamState::default();
        let stream = response.into_inner().map(move |event_result| {
            let event = event_result.map_err(|status| recovery.map_status(status))?;
            let envelope = chain_event_envelope_from_message(expected_network, event)?;
            stream_state.validate(&envelope)?;
            Ok(envelope)
        });

        Ok(Box::pin(stream))
    }

    async fn mempool_snapshot(
        &self,
        request: MempoolSnapshotRequest,
    ) -> Result<MempoolSnapshotView, IndexerError> {
        let from_cursor = request
            .from_cursor
            .map(|cursor| cursor.as_bytes().to_vec())
            .unwrap_or_default();
        let response = self
            .client()
            .mempool_snapshot(Request::new(wallet::MempoolSnapshotRequest {
                max_entries: request.max_entries,
                from_cursor,
            }))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();
        mempool_snapshot_view_from_message(self.network, response)
    }

    async fn mempool_events(
        &self,
        start: EventStreamStart<MempoolEventCursor>,
    ) -> Result<MempoolEventStream, IndexerError> {
        let response = self
            .client()
            .mempool_events(Request::new(wallet::MempoolEventsRequest {
                start: Some(event_stream_start_to_message(&start, |cursor| {
                    cursor.as_bytes()
                })),
            }))
            .await
            .map_err(|status| self.map_status(status))?;
        let expected_network = self.network;
        let recovery = self.clone();
        let stream = response.into_inner().map(move |event_result| {
            let envelope_message = event_result.map_err(|status| recovery.map_status(status))?;
            mempool_event_envelope_from_message(expected_network, envelope_message)
        });
        Ok(Box::pin(stream))
    }

    async fn is_in_mempool(&self, transaction_id: TransactionId) -> Result<bool, IndexerError> {
        // Defers to `transaction_by_id`, which the canonical chain consults
        // first and falls through to the mempool when no mined record
        // exists. This avoids a second round-trip and reuses the writer's
        // typed `TxStatus::InMempool` answer.
        let outcome = self.transaction_by_id(transaction_id, None).await?;
        Ok(matches!(outcome, TxStatus::InMempool(_)))
    }

    async fn transparent_mempool_outputs_by_address(
        &self,
        request: TransparentMempoolOutputsRequest,
    ) -> Result<Vec<TransparentMempoolOutput>, IndexerError> {
        let wire_request = wallet::TransparentMempoolOutputsByAddressRequest {
            address: Some(wallet::AddressLookup {
                selector: Some(wallet::address_lookup::Selector::ScriptHash(
                    request.address_script_hash.as_bytes().to_vec(),
                )),
            }),
            max_entries: Some(request.max_entries),
        };
        let response = self
            .client()
            .transparent_mempool_outputs_by_address(Request::new(wire_request))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();
        response
            .outputs
            .into_iter()
            .map(transparent_mempool_output_from_message)
            .collect::<Result<Vec<_>, IndexerError>>()
    }

    async fn transparent_mempool_spends_by_outpoint(
        &self,
        outpoints: &[TransparentOutPoint],
    ) -> Result<Vec<TransparentMempoolSpend>, IndexerError> {
        let wire_request = wallet::TransparentMempoolSpendsByOutpointRequest {
            outpoints: outpoints.iter().map(outpoint_message).collect(),
        };
        let response = self
            .client()
            .transparent_mempool_spends_by_outpoint(Request::new(wire_request))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();
        response
            .spends
            .into_iter()
            .map(transparent_mempool_spend_from_message)
            .collect()
    }

    async fn transparent_mempool_outputs_by_outpoint(
        &self,
        outpoints: &[TransparentOutPoint],
    ) -> Result<TransparentOutputsByOutpointResponse, IndexerError> {
        let wire_outpoints = outpoints.iter().map(outpoint_message).collect();
        let request = wallet::TransparentMempoolOutputsByOutpointRequest {
            outpoints: wire_outpoints,
        };
        let response = self
            .client()
            .transparent_mempool_outputs_by_outpoint(Request::new(request))
            .await
            .map_err(|status| self.map_status(status))?
            .into_inner();
        transparent_outputs_by_outpoint_response_from_message(self.network, response)
    }
}

fn ensure_supported_contract_revision(contract_revision: u32) -> Result<(), IndexerError> {
    if contract_revision < MIN_SUPPORTED_CONTRACT_REVISION {
        return Err(IndexerError::FailedPrecondition {
            reason: format!(
                "wallet contract revision {contract_revision} is older than required revision {MIN_SUPPORTED_CONTRACT_REVISION}"
            ),
        });
    }
    Ok(())
}

fn ensure_advertised_capability(
    descriptor: &impl CapabilityDescriptor,
    required_capability: Capability,
) -> Result<(), IndexerError> {
    let required_name = required_capability.as_str().to_owned();
    if descriptor.supports(required_capability) {
        return Ok(());
    }
    Err(IndexerError::FailedPrecondition {
        reason: format!(
            "remote wallet service does not advertise required capability {required_name}"
        ),
    })
}

fn server_info_from_message(
    expected_network: Network,
    wallet_info: wallet::WalletServerInfo,
) -> Result<ServerInfo, IndexerError> {
    let common = wallet_info
        .common
        .ok_or_else(|| IndexerError::malformed("info.common", "field is missing"))?;
    ensure_network_name(expected_network, &common.network)?;
    ensure_supported_contract_revision(common.contract_revision)?;
    let schema_version = u16::try_from(wallet_info.schema_version).map_err(|_| {
        IndexerError::malformed(
            "info.schema_version",
            "artifact schema version exceeds u16::MAX",
        )
    })?;

    Ok(ServerInfo {
        network: expected_network,
        service_name: common.service_name,
        service_version: common.service_version,
        capabilities: common
            .capabilities
            .into_iter()
            .map(Capability::from_wire_name)
            .collect(),
        contract_revision: common.contract_revision,
        materialized_view_preset: (!common.materialized_view_preset.is_empty())
            .then_some(common.materialized_view_preset),
        materialized_view_identities: common.materialized_view_identities,
        build_git_commit: common.build_git_commit,
        schema_version: ArtifactSchemaVersion::new(schema_version),
        reorg_window_blocks: wallet_info.reorg_window_blocks,
        node: wallet_info.node.map(|node| NodeServerInfo {
            version: node.version,
            capabilities: node.capabilities,
        }),
    })
}

fn network_upgrade_activations_from_message(
    network: Network,
    response: wallet::NetworkUpgradeActivationsResponse,
) -> Result<NetworkUpgradeActivations, IndexerError> {
    let activations = response
        .activations
        .into_iter()
        .enumerate()
        .map(|(index, activation)| {
            if activation.name.trim().is_empty() {
                return Err(IndexerError::malformed(
                    "activations.name",
                    format!("activation at index {index} has an empty name"),
                ));
            }
            Ok(NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(activation.consensus_branch_id),
                activation_height: BlockHeight::new(activation.activation_height),
                name: activation.name,
            })
        })
        .collect::<Result<Vec<_>, IndexerError>>()?;

    NetworkUpgradeActivations::new(network, activations)
        .map_err(|error| IndexerError::malformed("activations", error.to_string()))
}

#[cfg(test)]
mod network_upgrade_activation_tests {
    use super::*;

    #[test]
    fn exact_network_upgrade_capability_preflight_accepts_only_advertised_capability() {
        struct StubDescriptor(Vec<String>);
        impl CapabilityDescriptor for StubDescriptor {
            fn has(&self, capability: &str) -> bool {
                self.0.iter().any(|advertised| advertised == capability)
            }
        }

        let advertised = StubDescriptor(vec![
            Capability::NetworkUpgradeActivations.as_str().to_owned(),
        ]);
        assert!(
            ensure_advertised_capability(&advertised, Capability::NetworkUpgradeActivations)
                .is_ok()
        );

        let error = ensure_advertised_capability(
            &StubDescriptor(vec![
                "wallet.read.network_upgrade_activations_v2".to_owned(),
            ]),
            Capability::NetworkUpgradeActivations,
        );
        assert!(matches!(
            error,
            Err(IndexerError::FailedPrecondition { .. })
        ));
    }

    #[test]
    fn duplicate_branch_ids_are_rejected_as_malformed_responses() -> Result<(), &'static str> {
        let response = wallet::NetworkUpgradeActivationsResponse {
            activations: vec![
                wallet::NetworkUpgradeActivation {
                    consensus_branch_id: 0x5ba8_1b19,
                    name: "Overwinter".to_owned(),
                    activation_height: 1,
                },
                wallet::NetworkUpgradeActivation {
                    consensus_branch_id: 0x5ba8_1b19,
                    name: "Duplicate".to_owned(),
                    activation_height: 2,
                },
            ],
        };

        let error = network_upgrade_activations_from_message(Network::ZcashRegtest, response)
            .err()
            .ok_or("duplicate branch id must be rejected")?;

        assert!(matches!(
            error,
            IndexerError::MalformedResponse {
                field: "activations",
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn whitespace_only_names_are_rejected_as_malformed_responses() -> Result<(), &'static str> {
        let response = wallet::NetworkUpgradeActivationsResponse {
            activations: vec![wallet::NetworkUpgradeActivation {
                consensus_branch_id: 0x5ba8_1b19,
                name: " \t".to_owned(),
                activation_height: 1,
            }],
        };

        let error = network_upgrade_activations_from_message(Network::ZcashRegtest, response)
            .err()
            .ok_or("whitespace-only name must be rejected")?;

        assert!(matches!(
            error,
            IndexerError::MalformedResponse {
                field: "activations.name",
                ..
            }
        ));
        Ok(())
    }
}

#[cfg(test)]
mod server_info_conversion_tests {
    use zinder_proto::v1::ops;

    use super::*;

    #[test]
    fn maximum_domain_schema_version_is_accepted() -> Result<(), IndexerError> {
        let server_info = server_info_from_message(
            Network::ZcashRegtest,
            wallet_server_info(u32::from(u16::MAX)),
        )?;

        assert_eq!(server_info.schema_version.value(), u16::MAX);
        Ok(())
    }

    #[test]
    fn schema_version_above_domain_range_is_rejected() {
        let error = server_info_from_message(
            Network::ZcashRegtest,
            wallet_server_info(u32::from(u16::MAX) + 1),
        );

        assert!(matches!(
            error,
            Err(IndexerError::MalformedResponse {
                field: "info.schema_version",
                ..
            })
        ));
    }

    #[test]
    fn unknown_capabilities_survive_server_info_conversion() -> Result<(), IndexerError> {
        let mut message = wallet_server_info(1);
        let common = message
            .common
            .as_mut()
            .ok_or_else(|| IndexerError::malformed("info.common", "test field is missing"))?;
        common.capabilities = vec![
            Capability::FullBlock.as_str().to_owned(),
            Capability::VisibleTipBlock.as_str().to_owned(),
            "wallet.read.future_v7".to_owned(),
        ];

        let server_info = server_info_from_message(Network::ZcashRegtest, message)?;

        assert!(server_info.supports(Capability::FullBlock));
        assert!(server_info.supports(Capability::VisibleTipBlock));
        assert!(server_info.has("wallet.read.future_v7"));
        assert!(
            server_info
                .capabilities
                .contains(&Capability::Unknown("wallet.read.future_v7".to_owned()))
        );
        Ok(())
    }

    fn wallet_server_info(schema_version: u32) -> wallet::WalletServerInfo {
        wallet::WalletServerInfo {
            common: Some(ops::ServerInfo {
                network: "zcash-regtest".to_owned(),
                service_name: "zinder-query".to_owned(),
                service_version: "0.5.0".to_owned(),
                contract_revision: MIN_SUPPORTED_CONTRACT_REVISION,
                ..ops::ServerInfo::default()
            }),
            schema_version,
            ..wallet::WalletServerInfo::default()
        }
    }
}

fn transparent_outputs_by_outpoint_response_from_message(
    expected_network: Network,
    message: wallet::TransparentOutputsByOutpointResponse,
) -> Result<TransparentOutputsByOutpointResponse, IndexerError> {
    let chain_epoch =
        chain_epoch_from_chain_view_with_network(expected_network, message.chain_view)?;
    let entries = message
        .entries
        .into_iter()
        .map(transparent_output_entry_from_message)
        .collect::<Result<Vec<_>, IndexerError>>()?;
    Ok(TransparentOutputsByOutpointResponse {
        chain_epoch,
        entries,
    })
}

fn transparent_unspent_outputs_by_outpoint_response_from_message(
    expected_network: Network,
    message: wallet::TransparentUnspentOutputsByOutpointResponse,
) -> Result<TransparentUnspentOutputsByOutpointResponse, IndexerError> {
    let chain_epoch =
        chain_epoch_from_chain_view_with_network(expected_network, message.chain_view)?;
    let entries = message
        .entries
        .into_iter()
        .map(transparent_output_entry_from_message)
        .collect::<Result<Vec<_>, IndexerError>>()?;
    Ok(TransparentUnspentOutputsByOutpointResponse {
        chain_epoch,
        entries,
    })
}

fn transparent_spends_by_outpoint_response_from_message(
    expected_network: Network,
    message: wallet::TransparentSpendsByOutpointResponse,
) -> Result<TransparentSpendsByOutpointResponse, IndexerError> {
    let chain_epoch =
        chain_epoch_from_chain_view_with_network(expected_network, message.chain_view)?;
    let spends = message
        .spends
        .into_iter()
        .map(transparent_spend_from_message)
        .collect::<Result<Vec<_>, IndexerError>>()?;
    Ok(TransparentSpendsByOutpointResponse {
        chain_epoch,
        spends,
    })
}

fn transparent_spend_from_message(
    message: wallet::TransparentSpend,
) -> Result<TransparentSpendEntry, IndexerError> {
    let outpoint_message = message.spent_outpoint.ok_or_else(|| {
        IndexerError::malformed("transparent_spend.spent_outpoint", "field is missing")
    })?;
    let spent_transaction_id = transaction_id_from_rpc_hex(
        "transparent_spend.spent_outpoint.transaction_id",
        &outpoint_message.transaction_id,
    )?;
    let spent_outpoint =
        TransparentOutPoint::new(spent_transaction_id, outpoint_message.output_index);
    let spending_transaction_id = transaction_id_from_rpc_hex(
        "transparent_spend.spending_transaction_id",
        &message.spending_transaction_id,
    )?;
    let spending_block = message.spending_block.ok_or_else(|| {
        IndexerError::malformed("transparent_spend.spending_block", "field is missing")
    })?;
    let spending_block_hash = block_hash_from_rpc_hex(
        "transparent_spend.spending_block.hash",
        &spending_block.hash,
    )?;
    Ok(TransparentSpendEntry {
        spent_outpoint,
        spending_transaction_id,
        input_index: message.input_index,
        spending_block_height: BlockHeight::new(spending_block.height),
        spending_block_hash,
    })
}

fn chain_value_pools_at_tip_from_message(
    expected_network: Network,
    message: wallet::ChainValuePoolsAtTipResponse,
) -> Result<ChainValuePoolsAtTip, IndexerError> {
    let chain_epoch =
        chain_epoch_from_chain_view_with_network(expected_network, message.chain_view)?;
    let source_tip = message
        .source_tip
        .ok_or_else(|| IndexerError::malformed("source_tip", "field is missing"))?;
    let source_tip_hash = block_hash_from_rpc_hex("source_tip.hash", &source_tip.hash)?;
    let pools = message
        .pools
        .into_iter()
        .map(|pool| ChainValuePool::new(pool.id, pool.monitored, pool.chain_value_zat))
        .collect();
    Ok(ChainValuePoolsAtTip {
        chain_epoch,
        source_tip: BlockId::new(BlockHeight::new(source_tip.height), source_tip_hash),
        pools,
    })
}

fn transparent_output_entry_from_message(
    message: wallet::TransparentOutputEntry,
) -> Result<zinder_core::TransparentOutputEntry, IndexerError> {
    let outpoint_message = message.outpoint.ok_or_else(|| {
        IndexerError::malformed("transparent_output_entry.outpoint", "field is missing")
    })?;
    let transaction_id = transaction_id_from_rpc_hex(
        "transparent_output_entry.outpoint.transaction_id",
        &outpoint_message.transaction_id,
    )?;
    let outpoint = TransparentOutPoint::new(transaction_id, outpoint_message.output_index);
    let prevout = message
        .output
        .map(|prevout_message| zinder_core::TransparentOutput {
            value_zat: prevout_message.value_zat,
            script_pub_key: prevout_message.script_pub_key,
        });
    Ok(zinder_core::TransparentOutputEntry {
        outpoint,
        output: prevout,
    })
}

fn transparent_address_balance_from_message(
    expected_network: Network,
    message: wallet::TransparentAddressBalanceResponse,
) -> Result<TransparentAddressBalance, IndexerError> {
    let chain_epoch =
        chain_epoch_from_chain_view_with_network(expected_network, message.chain_view)?;
    Ok(TransparentAddressBalance {
        confirmed_zat: message.confirmed_zat,
        unconfirmed_delta_zat: message.unconfirmed_delta_zat,
        address_count: message.address_count,
        chain_epoch,
    })
}

fn transparent_mempool_output_from_message(
    message: wallet::TransparentMempoolOutput,
) -> Result<TransparentMempoolOutput, IndexerError> {
    transparent_mempool_output_from_message_shared(message)
        .map_err(wallet_decode_error_to_indexer_error)
}

fn transparent_mempool_spend_from_message(
    message: wallet::TransparentMempoolSpend,
) -> Result<TransparentMempoolSpend, IndexerError> {
    transparent_mempool_spend_from_message_shared(message)
        .map_err(wallet_decode_error_to_indexer_error)
}

fn ensure_network_name(expected: Network, actual_name: &str) -> Result<(), IndexerError> {
    let Some(actual) = decode_zinder_native_chain_name(actual_name).ok() else {
        return Err(IndexerError::NetworkMismatch {
            expected,
            actual: actual_name.to_owned(),
        });
    };
    if actual != expected {
        return Err(IndexerError::NetworkMismatch {
            expected,
            actual: actual_name.to_owned(),
        });
    }
    Ok(())
}

fn chain_epoch_from_message_with_network(
    expected_network: Network,
    message: wallet::ChainEpoch,
) -> Result<ChainEpoch, IndexerError> {
    ensure_network_name(expected_network, &message.network_name)?;
    let chain_epoch =
        chain_epoch_from_message(message).map_err(wallet_decode_error_to_indexer_error)?;
    if chain_epoch.id.value() == 0 {
        return Err(IndexerError::malformed(
            "chain_epoch.chain_epoch_id",
            "epoch id must be greater than zero",
        ));
    }
    if chain_epoch.artifact_schema_version.value() == 0 {
        return Err(IndexerError::malformed(
            "chain_epoch.artifact_schema_version",
            "artifact schema version must be greater than zero",
        ));
    }
    Ok(chain_epoch)
}

/// Decodes the chain epoch from a response's [`wallet::ChainView`] envelope,
/// asserting the network matches the endpoint the client opened against.
fn chain_epoch_from_chain_view_with_network(
    expected_network: Network,
    chain_view: Option<wallet::ChainView>,
) -> Result<ChainEpoch, IndexerError> {
    let chain_view =
        chain_view.ok_or_else(|| IndexerError::malformed("chain_view", "field is missing"))?;
    let chain_epoch = chain_view
        .chain_epoch
        .ok_or_else(|| IndexerError::malformed("chain_view.chain_epoch", "field is missing"))?;
    chain_epoch_from_message_with_network(expected_network, chain_epoch)
}

fn chain_epoch_from_chain_view_with_pin(
    expected_network: Network,
    expected_epoch: Option<ChainEpochId>,
    chain_view: Option<wallet::ChainView>,
) -> Result<ChainEpoch, IndexerError> {
    let chain_epoch = chain_epoch_from_chain_view_with_network(expected_network, chain_view)?;
    if expected_epoch.is_some_and(|expected| expected != chain_epoch.id) {
        return Err(IndexerError::malformed(
            "chain_view.chain_epoch.chain_epoch_id",
            "response chain epoch differs from request pin",
        ));
    }
    Ok(chain_epoch)
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "Used as a Result::map_err callback so the value-passing signature is required."
)]
fn wallet_decode_error_to_indexer_error(error: WalletWireDecodeError) -> IndexerError {
    IndexerError::malformed(error.field(), error.to_string())
}

fn compact_block_from_message(
    message: wallet::CompactBlock,
) -> Result<CompactBlockArtifact, IndexerError> {
    compact_block_from_wire_message(message).map_err(wallet_decode_error_to_indexer_error)
}

fn full_block_from_message(message: wallet::FullBlock) -> Result<BlockBlobArtifact, IndexerError> {
    Ok(BlockBlobArtifact::new(
        BlockHeight::new(message.height),
        block_hash_from_rpc_hex("full_block.block_hash", &message.block_hash)?,
        block_hash_from_rpc_hex("full_block.parent_block_hash", &message.parent_block_hash)?,
        message.payload_bytes,
    ))
}

fn tree_state_from_response(
    response: wallet::TreeStateResponse,
) -> Result<TreeStateArtifact, IndexerError> {
    let block_time_seconds = response.block_time_seconds.ok_or_else(|| {
        IndexerError::malformed(
            "tree_state.block_time_seconds",
            "field is missing from a contract revision 2 response",
        )
    })?;
    Ok(TreeStateArtifact::new(
        BlockHeight::new(response.height),
        block_hash_from_rpc_hex("tree_state.block_hash", &response.block_hash)?,
        block_time_seconds,
        response.payload_bytes,
    ))
}

fn subtree_root_from_message(
    protocol: ShieldedProtocol,
    message: wallet::SubtreeRoot,
) -> Result<SubtreeRootArtifact, IndexerError> {
    Ok(SubtreeRootArtifact::new(
        protocol,
        SubtreeRootIndex::new(message.subtree_index),
        subtree_root_hash_from_bytes("subtree_root.root_hash", message.root_hash)?,
        BlockHeight::new(message.completing_block_height),
        block_hash_from_rpc_hex(
            "subtree_root.completing_block_hash",
            &message.completing_block_hash,
        )?,
    ))
}

/// Decodes one message of the `TransparentAddressTxIdsInRange` stream.
///
/// The leading header carries the chain epoch pinned for the whole stream; it
/// is captured in `pinned_chain_epoch` and yields no item (`Ok(None)`). Every
/// later item is bound to that captured epoch. An item before the header, or a
/// second header, is a protocol violation.
struct TransparentHistoryStreamState {
    expected_network: Network,
    expected_epoch_id: Option<ChainEpochId>,
    address_script_hash: TransparentAddressScriptHash,
    pinned_chain_epoch: Option<ChainEpoch>,
    header_seen: std::sync::Arc<std::sync::atomic::AtomicBool>,
    stream_failed: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

fn transparent_address_tx_ids_stream(
    wire_stream: IndexStream<wallet::TransparentAddressTxIdsChunk>,
    state: TransparentHistoryStreamState,
) -> TransparentAddressTxIdsStream {
    let header_seen = std::sync::Arc::clone(&state.header_seen);
    let stream_failed = std::sync::Arc::clone(&state.stream_failed);
    let decoded = futures_util::stream::unfold(
        (wire_stream, state, false),
        |(mut wire_stream, mut state, is_terminated)| async move {
            if is_terminated {
                return None;
            }
            let message_result = futures_util::StreamExt::next(&mut wire_stream).await?;
            let decoded = message_result
                .and_then(|message| transparent_address_tx_ids_stream_item(&mut state, message))
                .transpose();
            let is_terminated = decoded.as_ref().is_some_and(Result::is_err);
            if is_terminated {
                state
                    .stream_failed
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            Some((decoded, (wire_stream, state, is_terminated)))
        },
    );
    let items = futures_util::StreamExt::filter_map(decoded, |decoded| async move { decoded });
    let terminal = futures_util::StreamExt::filter_map(
        futures_util::stream::once(async move {
            missing_transparent_history_header_error(
                header_seen.load(std::sync::atomic::Ordering::Acquire),
                stream_failed.load(std::sync::atomic::Ordering::Acquire),
            )
            .map(Err)
        }),
        |terminal_outcome| async move { terminal_outcome },
    );
    Box::pin(futures_util::StreamExt::chain(items, terminal))
}

fn transparent_address_tx_ids_stream_item(
    state: &mut TransparentHistoryStreamState,
    message: wallet::TransparentAddressTxIdsChunk,
) -> Result<Option<TransparentAddressTransactionChunk>, IndexerError> {
    match message.body.ok_or_else(|| {
        IndexerError::malformed("transparent_address_tx_ids_chunk.body", "field is missing")
    })? {
        wallet::transparent_address_tx_ids_chunk::Body::Header(chain_view) => {
            stream_header_chain_epoch(
                state.expected_network,
                &mut state.pinned_chain_epoch,
                Some(chain_view),
            )?;
            if state.expected_epoch_id.is_some_and(|expected| {
                state.pinned_chain_epoch.as_ref().map(|epoch| epoch.id) != Some(expected)
            }) {
                return Err(IndexerError::ChainEpochPinUnavailable);
            }
            state
                .header_seen
                .store(true, std::sync::atomic::Ordering::Release);
            Ok(None)
        }
        wallet::transparent_address_tx_ids_chunk::Body::Item(entry) => {
            let chain_epoch = stream_item_chain_epoch(state.pinned_chain_epoch.as_ref())?;
            let transaction_id =
                transaction_id_from_rpc_hex("transaction_id", &entry.transaction_id)?;
            let block_hash = block_hash_from_rpc_hex("block_hash", &entry.block_hash)?;
            let artifact = TransparentAddressTxIndexArtifact::new(
                state.address_script_hash,
                BlockHeight::new(entry.block_height),
                entry.tx_index_in_block,
                transaction_id,
                block_hash,
            );
            let cursor = if entry.cursor.is_empty() {
                None
            } else {
                Some(TransparentHistoryCursor::from_bytes(entry.cursor))
            };
            Ok(Some(TransparentAddressTransactionChunk {
                chain_epoch,
                artifact,
                cursor,
            }))
        }
    }
}

/// Decodes one message of the `TransparentAddressUnspentOutputs` stream.
///
/// The leading header carries the chain epoch pinned for the whole stream; it
/// is captured in `pinned_chain_epoch` and yields no item (`Ok(None)`). Every
/// later item is bound to that captured epoch. An item before the header, or a
/// second header, is a protocol violation.
fn transparent_unspent_output_stream_item(
    expected_network: Network,
    expected_epoch_id: Option<ChainEpochId>,
    pinned_chain_epoch: &mut Option<ChainEpoch>,
    header_seen: &std::sync::atomic::AtomicBool,
    message: wallet::TransparentUnspentOutputsChunk,
) -> Result<Option<TransparentUnspentOutputChunk>, IndexerError> {
    match message.body.ok_or_else(|| {
        IndexerError::malformed("transparent_unspent_outputs_chunk.body", "field is missing")
    })? {
        wallet::transparent_unspent_outputs_chunk::Body::Header(chain_view) => {
            stream_header_chain_epoch(expected_network, pinned_chain_epoch, Some(chain_view))?;
            if let Some(expected) = expected_epoch_id
                && pinned_chain_epoch.as_ref().map(|epoch| epoch.id) != Some(expected)
            {
                return Err(IndexerError::malformed(
                    "transparent_unspent_outputs.header.chain_epoch_id",
                    "response epoch does not match requested epoch pin",
                ));
            }
            header_seen.store(true, std::sync::atomic::Ordering::Release);
            Ok(None)
        }
        wallet::transparent_unspent_outputs_chunk::Body::Item(output_message) => {
            let chain_epoch = stream_item_chain_epoch(pinned_chain_epoch.as_ref())?;
            let address_script_hash_bytes = fixed_32_bytes(
                "transparent_unspent_output.address_script_hash",
                output_message.address_script_hash,
            )?;
            let outpoint_message = output_message.outpoint.ok_or_else(|| {
                IndexerError::malformed("transparent_unspent_output.outpoint", "field is missing")
            })?;
            let transaction_id = transaction_id_from_rpc_hex(
                "transparent_unspent_output.outpoint.transaction_id",
                &outpoint_message.transaction_id,
            )?;
            let block_hash = block_hash_from_rpc_hex(
                "transparent_unspent_output.block_hash",
                &output_message.block_hash,
            )?;
            let output = TransparentUnspentOutput::new(
                TransparentAddressScriptHash::from_bytes(address_script_hash_bytes),
                output_message.script_pub_key,
                TransparentOutPoint::new(transaction_id, outpoint_message.output_index),
                output_message.value_zat,
                BlockHeight::new(output_message.block_height),
                block_hash,
            );
            Ok(Some(TransparentUnspentOutputChunk {
                chain_epoch,
                output,
            }))
        }
    }
}

/// Records the pinned chain epoch from a stream's leading header.
///
/// Rejects a second header: the stream-header contract sends exactly one.
fn stream_header_chain_epoch(
    expected_network: Network,
    pinned_chain_epoch: &mut Option<ChainEpoch>,
    chain_view: Option<wallet::ChainView>,
) -> Result<(), IndexerError> {
    if pinned_chain_epoch.is_some() {
        return Err(IndexerError::malformed(
            "stream.header",
            "stream sent more than one header",
        ));
    }
    let chain_epoch = chain_epoch_from_chain_view_with_network(expected_network, chain_view)?;
    *pinned_chain_epoch = Some(chain_epoch);
    Ok(())
}

/// Returns the chain epoch a stream item binds to, rejecting an item that
/// arrives before the leading header.
fn stream_item_chain_epoch(
    pinned_chain_epoch: Option<&ChainEpoch>,
) -> Result<ChainEpoch, IndexerError> {
    pinned_chain_epoch.copied().ok_or_else(|| {
        IndexerError::malformed("stream.item", "stream sent an item before its header")
    })
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "MinedBlockLocation is a tiny POD; taking by value matches the symmetric encoder."
)]
fn mined_block_location_from_message(
    message: wallet::MinedBlockLocation,
) -> Result<TransactionLocation, IndexerError> {
    Ok(TransactionLocation::new(
        transaction_id_from_rpc_hex("mined.location.transaction_id", &message.transaction_id)?,
        BlockHeight::new(message.block_height),
        block_hash_from_rpc_hex("mined_block_location.block_hash", &message.block_hash)?,
        message.tx_index_in_block,
    ))
}

fn transaction_broadcast_outcome_from_message(
    response: wallet::BroadcastTransactionResponse,
) -> Result<TransactionBroadcastOutcome, IndexerError> {
    use wallet::broadcast_transaction_response::Outcome;
    use zinder_core::{
        BroadcastAccepted, BroadcastDuplicate, BroadcastInvalidEncoding, BroadcastQueued,
        BroadcastRejected, BroadcastUnknown,
    };

    let outcome = response
        .outcome
        .ok_or_else(|| IndexerError::malformed("outcome", "field is missing"))?;
    match outcome {
        Outcome::Accepted(accepted) => {
            Ok(TransactionBroadcastOutcome::Accepted(BroadcastAccepted {
                transaction_id: transaction_id_from_rpc_hex(
                    "accepted.transaction_id",
                    &accepted.transaction_id,
                )?,
            }))
        }
        Outcome::Duplicate(duplicate) => {
            Ok(TransactionBroadcastOutcome::Duplicate(BroadcastDuplicate {
                error_code: duplicate.error_code,
                message: duplicate.message,
            }))
        }
        Outcome::InvalidEncoding(invalid_encoding) => Ok(
            TransactionBroadcastOutcome::InvalidEncoding(BroadcastInvalidEncoding {
                error_code: invalid_encoding.error_code,
                message: invalid_encoding.message,
            }),
        ),
        Outcome::Queued(queued) => Ok(TransactionBroadcastOutcome::Queued(BroadcastQueued {
            message: queued.message,
        })),
        Outcome::Rejected(rejected) => {
            Ok(TransactionBroadcastOutcome::Rejected(BroadcastRejected {
                kind: broadcast_rejection_reason_from_message(rejected.kind)?,
                error_code: rejected.error_code,
                message: rejected.message,
            }))
        }
        Outcome::Unknown(unknown) => Ok(TransactionBroadcastOutcome::Unknown(BroadcastUnknown {
            error_code: unknown.error_code,
            message: unknown.message,
        })),
    }
}

fn broadcast_rejection_reason_from_message(
    code: i32,
) -> Result<zinder_core::BroadcastRejectionReason, IndexerError> {
    use zinder_core::BroadcastRejectionReason;

    match wallet::BroadcastRejectionReason::try_from(code) {
        Ok(wallet::BroadcastRejectionReason::InvalidSignature) => {
            Ok(BroadcastRejectionReason::InvalidSignature)
        }
        Ok(wallet::BroadcastRejectionReason::BadExpiryHeight) => {
            Ok(BroadcastRejectionReason::BadExpiryHeight)
        }
        Ok(wallet::BroadcastRejectionReason::BadConsensusBranch) => {
            Ok(BroadcastRejectionReason::BadConsensusBranch)
        }
        Ok(wallet::BroadcastRejectionReason::MempoolFull) => {
            Ok(BroadcastRejectionReason::MempoolFull)
        }
        Ok(wallet::BroadcastRejectionReason::Unknown) => Ok(BroadcastRejectionReason::Unknown),
        Ok(wallet::BroadcastRejectionReason::Unspecified) | Err(_) => Err(IndexerError::malformed(
            "rejected.kind",
            format!("unsupported broadcast rejection reason discriminant {code}"),
        )),
    }
}

fn chain_event_envelope_from_message(
    expected_network: Network,
    message: wallet::ChainEventEnvelope,
) -> Result<ChainEventEnvelope, IndexerError> {
    if message.cursor.is_empty() {
        return Err(IndexerError::malformed("cursor", "field is empty"));
    }
    if message.event_sequence == 0 {
        return Err(IndexerError::malformed(
            "event_sequence",
            "sequence must be greater than zero",
        ));
    }
    let chain_epoch =
        chain_epoch_from_chain_view_with_network(expected_network, message.chain_view)?;
    let event = match message
        .event
        .ok_or_else(|| IndexerError::malformed("event", "field is missing"))?
    {
        wallet::chain_event_envelope::Event::ChainCommitted(chain_committed) => {
            let committed = chain_epoch_committed_from_message(
                expected_network,
                chain_committed.committed.ok_or_else(|| {
                    IndexerError::malformed("chain_committed.committed", "field is missing")
                })?,
            )?;
            if committed.chain_epoch != chain_epoch {
                return Err(IndexerError::malformed(
                    "chain_committed.committed.chain_epoch",
                    "committed event epoch does not match envelope chain view",
                ));
            }
            validate_committed_range(&committed)?;
            ChainEvent::ChainCommitted { committed }
        }
        wallet::chain_event_envelope::Event::ChainReorged(chain_reorged) => {
            let reverted = chain_range_reverted_from_message(
                expected_network,
                chain_reorged.reverted.ok_or_else(|| {
                    IndexerError::malformed("chain_reorged.reverted", "field is missing")
                })?,
            )?;
            let committed = chain_epoch_committed_from_message(
                expected_network,
                chain_reorged.committed.ok_or_else(|| {
                    IndexerError::malformed("chain_reorged.committed", "field is missing")
                })?,
            )?;
            if committed.chain_epoch != chain_epoch {
                return Err(IndexerError::malformed(
                    "chain_reorged.committed.chain_epoch",
                    "reorg committed epoch does not match envelope chain view",
                ));
            }
            validate_reorg_relationship(&reverted, &committed)?;
            ChainEvent::ChainReorged {
                reverted,
                committed,
            }
        }
    };

    Ok(ChainEventEnvelope {
        cursor: ChainEventCursor::from_bytes(message.cursor),
        event_sequence: message.event_sequence,
        settled_tip_height: chain_epoch.settled_tip_height,
        chain_epoch,
        event,
    })
}

fn validate_committed_range(committed: &ChainEpochCommitted) -> Result<(), IndexerError> {
    let range = committed.block_range;
    let visible_tip = committed.chain_epoch.visible_tip_height;
    if range.start <= range.end {
        if range.end != visible_tip {
            return Err(IndexerError::malformed(
                "committed.block_range",
                "non-empty committed range must end at its epoch visible tip",
            ));
        }
    } else if range != BlockHeightRange::empty_at(visible_tip) {
        return Err(IndexerError::malformed(
            "committed.block_range",
            "empty committed range must be the exact marker after the visible tip",
        ));
    }
    Ok(())
}

fn validate_reorg_relationship(
    reverted: &ChainRangeReverted,
    committed: &ChainEpochCommitted,
) -> Result<(), IndexerError> {
    validate_committed_range(committed)?;
    if reverted.block_range.start > reverted.block_range.end {
        return Err(IndexerError::malformed(
            "chain_reorged.reverted.block_range",
            "reverted range must not be empty or reversed",
        ));
    }
    if committed.block_range.start > committed.block_range.end {
        return Err(IndexerError::malformed(
            "chain_reorged.committed.block_range",
            "reorg committed range must not be empty or reversed",
        ));
    }
    if reverted.block_range.start != committed.block_range.start {
        return Err(IndexerError::malformed(
            "chain_reorged",
            "reverted and committed ranges must start at the same fork height",
        ));
    }
    if reverted.block_range.end != reverted.chain_epoch.visible_tip_height {
        return Err(IndexerError::malformed(
            "chain_reorged.reverted.block_range",
            "reverted range must end at its historical epoch visible tip",
        ));
    }
    if reverted.chain_epoch.network != committed.chain_epoch.network
        || reverted.chain_epoch.artifact_schema_version
            != committed.chain_epoch.artifact_schema_version
    {
        return Err(IndexerError::malformed(
            "chain_reorged.reverted.chain_epoch",
            "reverted and committed epochs must share network and artifact schema",
        ));
    }
    if reverted.chain_epoch.id >= committed.chain_epoch.id {
        return Err(IndexerError::malformed(
            "chain_reorged.reverted.chain_epoch.chain_epoch_id",
            "reverted epoch must precede the committed epoch",
        ));
    }
    if reverted.chain_epoch.settled_tip_height != committed.chain_epoch.settled_tip_height
        || reverted.chain_epoch.settled_tip_hash != committed.chain_epoch.settled_tip_hash
    {
        return Err(IndexerError::malformed(
            "chain_reorged.reverted.chain_epoch.settled_tip",
            "reorg must preserve the settled tip identity",
        ));
    }
    if reverted.block_range.start <= committed.chain_epoch.settled_tip_height {
        return Err(IndexerError::malformed(
            "chain_reorged.reverted.block_range.start",
            "reorg range must begin above the settled tip",
        ));
    }
    Ok(())
}

// Retain every accepted cursor for this stream so a server cannot move the
// resume position back to any previously delivered event. Both limits make
// the retention cost explicit and bounded. Reaching either limit fails the
// stream before accepting another event; callers can reconnect from the last
// accepted cursor to start with fresh validation state.
const CHAIN_EVENT_CURSOR_COUNT_LIMIT: usize = 65_536;
const CHAIN_EVENT_CURSOR_BYTES_LIMIT: usize = 8 * 1024 * 1024;

struct ChainEventStreamState {
    previous_sequence: Option<u64>,
    seen_cursors: HashSet<Vec<u8>>,
    retained_cursor_bytes: usize,
    cursor_count_limit: usize,
    cursor_bytes_limit: usize,
}

impl Default for ChainEventStreamState {
    fn default() -> Self {
        Self {
            previous_sequence: None,
            seen_cursors: HashSet::new(),
            retained_cursor_bytes: 0,
            cursor_count_limit: CHAIN_EVENT_CURSOR_COUNT_LIMIT,
            cursor_bytes_limit: CHAIN_EVENT_CURSOR_BYTES_LIMIT,
        }
    }
}

impl ChainEventStreamState {
    fn validate(&mut self, envelope: &ChainEventEnvelope) -> Result<(), IndexerError> {
        if self
            .previous_sequence
            .is_some_and(|previous| envelope.event_sequence <= previous)
        {
            return Err(IndexerError::malformed(
                "event_sequence",
                "chain event stream sequence must increase monotonically",
            ));
        }
        let cursor = envelope.cursor.as_bytes();
        if self.seen_cursors.contains(cursor) {
            return Err(IndexerError::malformed(
                "cursor",
                "chain event stream repeated a previously delivered cursor",
            ));
        }

        let retained_cursor_bytes = self
            .retained_cursor_bytes
            .checked_add(cursor.len())
            .ok_or_else(|| {
                IndexerError::malformed(
                    "cursor",
                    "chain event cursor retention capacity is exhausted; reconnect from the last accepted cursor",
                )
            })?;
        if self.seen_cursors.len() >= self.cursor_count_limit
            || retained_cursor_bytes > self.cursor_bytes_limit
        {
            return Err(IndexerError::malformed(
                "cursor",
                "chain event cursor retention capacity is exhausted; reconnect from the last accepted cursor",
            ));
        }

        self.previous_sequence = Some(envelope.event_sequence);
        self.seen_cursors.insert(cursor.to_vec());
        self.retained_cursor_bytes = retained_cursor_bytes;
        Ok(())
    }
}

fn chain_epoch_committed_from_message(
    expected_network: Network,
    message: wallet::ChainEpochCommitted,
) -> Result<ChainEpochCommitted, IndexerError> {
    Ok(ChainEpochCommitted {
        chain_epoch: chain_epoch_from_message_with_network(
            expected_network,
            message
                .chain_epoch
                .ok_or_else(|| IndexerError::malformed("chain_epoch", "field is missing"))?,
        )?,
        block_range: BlockHeightRange::inclusive(
            BlockHeight::new(message.start_height),
            BlockHeight::new(message.end_height),
        ),
    })
}

fn chain_range_reverted_from_message(
    expected_network: Network,
    message: wallet::ChainRangeReverted,
) -> Result<ChainRangeReverted, IndexerError> {
    Ok(ChainRangeReverted {
        chain_epoch: chain_epoch_from_message_with_network(
            expected_network,
            message
                .chain_epoch
                .ok_or_else(|| IndexerError::malformed("chain_epoch", "field is missing"))?,
        )?,
        block_range: BlockHeightRange::inclusive(
            BlockHeight::new(message.start_height),
            BlockHeight::new(message.end_height),
        ),
    })
}

fn shielded_protocol_to_message(
    protocol: ShieldedProtocol,
) -> Result<wallet::ShieldedProtocol, IndexerError> {
    match protocol {
        ShieldedProtocol::Sapling => Ok(wallet::ShieldedProtocol::Sapling),
        ShieldedProtocol::Orchard => Ok(wallet::ShieldedProtocol::Orchard),
        ShieldedProtocol::Ironwood => Ok(wallet::ShieldedProtocol::Ironwood),
        _ => Err(IndexerError::invalid_request(
            "shielded protocol is unsupported by the native wallet protocol",
        )),
    }
}

fn shielded_protocol_from_message(protocol: i32) -> Result<ShieldedProtocol, IndexerError> {
    match wallet::ShieldedProtocol::try_from(protocol) {
        Ok(wallet::ShieldedProtocol::Sapling) => Ok(ShieldedProtocol::Sapling),
        Ok(wallet::ShieldedProtocol::Orchard) => Ok(ShieldedProtocol::Orchard),
        Ok(wallet::ShieldedProtocol::Ironwood) => Ok(ShieldedProtocol::Ironwood),
        Ok(wallet::ShieldedProtocol::Unspecified) => Err(IndexerError::malformed(
            "shielded_protocol",
            "protocol is unspecified",
        )),
        Err(_) => Err(IndexerError::malformed(
            "shielded_protocol",
            "protocol is unknown",
        )),
    }
}

fn chain_event_stream_family_to_message(
    family: ChainEventStreamFamily,
) -> wallet::ChainEventStreamFamily {
    match family {
        ChainEventStreamFamily::Visible => wallet::ChainEventStreamFamily::Visible,
        ChainEventStreamFamily::Settled => wallet::ChainEventStreamFamily::Settled,
    }
}

fn block_hash_from_rpc_hex(field: &'static str, rpc_hex: &str) -> Result<BlockHash, IndexerError> {
    decode_rpc_block_hash_hex(rpc_hex)
        .map_err(|error| IndexerError::malformed(field, error.to_string()))
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "BlockSelector is #[non_exhaustive]; new selector variants need wire opt-in before they can be sent"
)]
fn block_selector_to_message(
    selector: BlockSelector,
) -> Result<wallet::BlockSelector, IndexerError> {
    let inner = match selector {
        BlockSelector::Height(height) => wallet::block_selector::Selector::Height(height.value()),
        BlockSelector::Hash(hash) => {
            wallet::block_selector::Selector::Hash(encode_rpc_block_hash_hex(hash))
        }
        _ => {
            return Err(IndexerError::invalid_request(
                "unsupported block selector variant",
            ));
        }
    };
    Ok(wallet::BlockSelector {
        selector: Some(inner),
    })
}

/// Checks that a typed absence answers the lookup the caller made.
///
/// A pinned lookup must be answered at the pinned epoch; an unpinned lookup
/// must also have searched the live mempool; and the covered range must start
/// at height 1, so a store with a later history floor never yields an absence.
fn verify_transaction_absence(
    absence: TransactionAbsence,
    at_epoch_id: Option<ChainEpochId>,
) -> Result<(), IndexerError> {
    if absence.first_covered_height > BlockHeight::new(1) {
        return Err(IndexerError::CanonicalHistoryUnavailable {
            first_available_height: absence.first_covered_height,
        });
    }
    match at_epoch_id {
        Some(pinned_epoch_id) if absence.chain_epoch_id != pinned_epoch_id => {
            Err(IndexerError::malformed(
                "error_info.metadata.chain_epoch_id",
                "absence epoch does not match requested epoch pin",
            ))
        }
        None if !absence.covers_mempool => Err(IndexerError::malformed(
            "error_info.metadata.covers_mempool",
            "an unpinned absence must cover the live mempool",
        )),
        Some(_) | None => Ok(()),
    }
}

/// Returns whether `status` is a `NOT_FOUND` that a server below contract
/// revision 6 used for every transaction miss, which cannot be told apart
/// from a storage or history failure.
fn is_unverified_transaction_absence(status: &tonic::Status) -> bool {
    if status.code() != tonic::Code::NotFound {
        return false;
    }
    let details = status.get_error_details();
    match details
        .error_info()
        .filter(|error_info| error_info.domain == ZINDER_ERROR_DOMAIN)
    {
        None => true,
        Some(error_info) => {
            error_info.reason == crate::ErrorReason::ArtifactUnavailable.as_str()
                && details.resource_info().is_none()
        }
    }
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "TransactionLocation oneof is non-exhaustive in the protobuf-generated enum; new variants are a deliberate wire change."
)]
fn tx_status_from_message(
    expected_network: Network,
    expected_transaction_id: TransactionId,
    expected_epoch_id: Option<ChainEpochId>,
    response: wallet::TransactionStatusResponse,
) -> Result<TxStatus, IndexerError> {
    let chain_epoch_message = response
        .chain_view
        .and_then(|chain_view| chain_view.chain_epoch)
        .ok_or_else(|| IndexerError::malformed("chain_view.chain_epoch", "field is missing"))?;
    let chain_epoch = chain_epoch_from_message_with_network(expected_network, chain_epoch_message)?;
    if expected_epoch_id.is_some_and(|expected| expected != chain_epoch.id) {
        return Err(IndexerError::malformed(
            "chain_view.chain_epoch.chain_epoch_id",
            "response epoch does not match requested epoch pin",
        ));
    }
    let location = response
        .location
        .and_then(|location| location.location)
        .ok_or_else(|| IndexerError::malformed("location.location", "field is missing"))?;
    match location {
        wallet::transaction_location::Location::Mined(mined) => {
            let location =
                mined_block_location_from_message(mined.location.ok_or_else(|| {
                    IndexerError::malformed("mined.location", "field is missing")
                })?)?;
            if location.transaction_id != expected_transaction_id {
                return Err(IndexerError::malformed(
                    "mined.location.transaction_id",
                    "response transaction id does not match requested transaction id",
                ));
            }
            let chain_context_message = mined.chain_context.ok_or_else(|| {
                IndexerError::malformed("mined.chain_context", "field is missing")
            })?;
            let chain_context = MinedTransactionChainContext {
                consensus_branch_id: ConsensusBranchId::new(
                    chain_context_message.consensus_branch_id,
                ),
                block_time: chain_context_message.block_time,
                confirmations: chain_context_message.confirmations,
            };
            Ok(TxStatus::Mined(MinedTransaction::new(
                location,
                chain_context,
                mined.raw_transaction_bytes,
            )))
        }
        wallet::transaction_location::Location::InMempool(in_mempool) => {
            let entry = mempool_entry_from_message(in_mempool)
                .map_err(wallet_decode_error_to_indexer_error)?;
            ensure_mempool_entry_network(expected_network, &entry)?;
            if entry.transaction_id() != expected_transaction_id {
                return Err(IndexerError::malformed(
                    "location.in_mempool.transaction_id",
                    "response transaction id does not match requested transaction id",
                ));
            }
            Ok(TxStatus::InMempool(entry))
        }
    }
}

fn block_id_from_message(block_id: Option<wallet::BlockId>) -> Result<BlockId, IndexerError> {
    let block_id_message =
        block_id.ok_or_else(|| IndexerError::malformed("block_id", "field is missing"))?;
    let block_hash = block_hash_from_rpc_hex("block_id.block_hash", &block_id_message.block_hash)?;
    Ok(BlockId::new(
        BlockHeight::new(block_id_message.height),
        block_hash,
    ))
}

fn block_id_from_selector_response(
    expected_network: Network,
    expected_epoch_id: Option<ChainEpochId>,
    requested_selector: BlockSelector,
    response: wallet::BlockIdResponse,
) -> Result<BlockId, IndexerError> {
    let chain_epoch = chain_epoch_from_chain_view_with_pin(
        expected_network,
        expected_epoch_id,
        response.chain_view,
    )?;
    let block_id = block_id_from_message(response.block_id)?;
    ensure_block_id_matches_selector_and_visible_tip(
        "block_id",
        requested_selector,
        block_id,
        BlockId::new(chain_epoch.visible_tip_height, chain_epoch.visible_tip_hash),
    )?;
    Ok(block_id)
}

fn block_header_from_selector_response(
    expected_network: Network,
    expected_epoch_id: Option<ChainEpochId>,
    requested_selector: BlockSelector,
    response: wallet::BlockHeaderResponse,
) -> Result<BlockHeader, IndexerError> {
    let chain_epoch = chain_epoch_from_chain_view_with_pin(
        expected_network,
        expected_epoch_id,
        response.chain_view,
    )?;
    let header_message = response
        .block_header
        .ok_or_else(|| IndexerError::malformed("block_header", "field is missing"))?;
    let header = block_header_from_message(header_message)?;
    ensure_block_id_matches_selector_and_visible_tip(
        "block_header.block_id",
        requested_selector,
        header.block_id,
        BlockId::new(chain_epoch.visible_tip_height, chain_epoch.visible_tip_hash),
    )?;
    Ok(header)
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "BlockSelector is #[non_exhaustive]; new selector variants require an explicit response-validation contract"
)]
fn ensure_block_id_matches_selector_and_visible_tip(
    field: &'static str,
    requested_selector: BlockSelector,
    block_id: BlockId,
    visible_tip: BlockId,
) -> Result<(), IndexerError> {
    let matches_selector = match requested_selector {
        BlockSelector::Height(requested_height) => block_id.height == requested_height,
        BlockSelector::Hash(requested_hash) => block_id.hash == requested_hash,
        _ => false,
    };
    let is_at_or_below_visible_tip = block_id.height <= visible_tip.height;
    let has_coherent_visible_tip_identity =
        (block_id.height == visible_tip.height) == (block_id.hash == visible_tip.hash);
    if !matches_selector || !is_at_or_below_visible_tip || !has_coherent_visible_tip_identity {
        return Err(IndexerError::malformed(
            field,
            "resolved block identity does not match the requested selector or response visible tip",
        ));
    }
    Ok(())
}

fn block_header_from_message(message: wallet::BlockHeader) -> Result<BlockHeader, IndexerError> {
    let block_id = block_id_from_message(message.block_id)?;
    let previous_block_hash = block_hash_from_rpc_hex(
        "block_header.previous_block_hash",
        &message.previous_block_hash,
    )?;
    let merkle_root_hash =
        merkle_root_hash_from_rpc_hex("block_header.merkle_root_hash", &message.merkle_root_hash)?;
    let commitment_bytes =
        fixed_32_bytes("block_header.commitment_bytes", message.commitment_bytes)?;
    let nonce = fixed_32_bytes("block_header.nonce", message.nonce)?;
    Ok(BlockHeader::new(
        block_id,
        previous_block_hash,
        merkle_root_hash,
        commitment_bytes,
        message.block_time,
        message.bits,
        nonce,
        message.version,
    ))
}

fn subtree_root_hash_from_bytes(
    field: &'static str,
    bytes: Vec<u8>,
) -> Result<SubtreeRootHash, IndexerError> {
    let bytes = fixed_32_bytes(field, bytes)?;
    Ok(SubtreeRootHash::from_bytes(bytes))
}

fn transaction_id_from_rpc_hex(
    field: &'static str,
    rpc_hex: &str,
) -> Result<TransactionId, IndexerError> {
    decode_rpc_transaction_id_hex(rpc_hex)
        .map_err(|error| IndexerError::malformed(field, error.to_string()))
}

fn merkle_root_hash_from_rpc_hex(
    field: &'static str,
    rpc_hex: &str,
) -> Result<[u8; 32], IndexerError> {
    decode_rpc_merkle_root_hex(rpc_hex)
        .map_err(|error| IndexerError::malformed(field, error.to_string()))
}

fn fixed_32_bytes(field: &'static str, bytes: Vec<u8>) -> Result<[u8; 32], IndexerError> {
    let len = bytes.len();
    bytes
        .try_into()
        .map_err(|_| IndexerError::malformed(field, format!("expected 32 bytes, got {len} bytes")))
}

fn mempool_snapshot_view_from_message(
    expected_network: Network,
    message: wallet::MempoolSnapshotResponse,
) -> Result<MempoolSnapshotView, IndexerError> {
    let chain_epoch_message = message
        .chain_view
        .and_then(|chain_view| chain_view.chain_epoch)
        .ok_or_else(|| IndexerError::malformed("chain_view.chain_epoch", "field is missing"))?;
    let chain_epoch = chain_epoch_from_message_with_network(expected_network, chain_epoch_message)?;
    let source_tip_message = message
        .source_tip
        .ok_or_else(|| IndexerError::malformed("source_tip", "field is missing"))?;
    let source_tip = BlockId::new(
        BlockHeight::new(source_tip_message.height),
        block_hash_from_rpc_hex("source_tip.hash", &source_tip_message.hash)?,
    );
    let visible_tip = BlockId::new(chain_epoch.visible_tip_height, chain_epoch.visible_tip_hash);
    if source_tip != visible_tip {
        return Err(IndexerError::malformed(
            "source_tip",
            "does not match chain_view.chain_epoch.visible_tip",
        ));
    }
    let entries = message
        .entries
        .into_iter()
        .map(|entry| mempool_entry_from_message_with_network(expected_network, entry))
        .collect::<Result<Vec<MempoolEntry>, IndexerError>>()?;
    let next_cursor = if message.next_cursor.is_empty() {
        None
    } else {
        Some(MempoolSnapshotCursor::from_bytes(message.next_cursor))
    };
    let events_resume_cursor = if message.events_resume_cursor.is_empty() {
        None
    } else {
        Some(MempoolEventCursor::from_bytes(message.events_resume_cursor))
    };
    Ok(MempoolSnapshotView {
        chain_epoch,
        source_tip,
        events_resume_cursor,
        snapshot_age_millis: message.snapshot_age_millis,
        entries,
        next_cursor,
    })
}

fn mempool_entry_from_message_with_network(
    expected_network: Network,
    message: wallet::MempoolEntry,
) -> Result<MempoolEntry, IndexerError> {
    let entry =
        mempool_entry_from_message(message).map_err(wallet_decode_error_to_indexer_error)?;
    ensure_mempool_entry_network(expected_network, &entry)?;
    Ok(entry)
}

fn ensure_mempool_entry_network(
    expected_network: Network,
    entry: &MempoolEntry,
) -> Result<(), IndexerError> {
    ensure_network_name(
        expected_network,
        encode_zinder_native_chain_name(entry.first_seen_chain_epoch().network),
    )
}

/// Encodes a typed client start position into the wire `EventStreamStart`,
/// extracting the opaque cursor bytes with `cursor_bytes`.
fn event_stream_start_to_message<Cursor>(
    start: &EventStreamStart<Cursor>,
    cursor_bytes: impl for<'cursor> Fn(&'cursor Cursor) -> &'cursor [u8],
) -> wallet::EventStreamStart {
    let position = match start {
        EventStreamStart::AfterCursor(cursor) => {
            wallet::event_stream_start::Position::AfterCursor(cursor_bytes(cursor).to_vec())
        }
        EventStreamStart::EarliestRetained => {
            wallet::event_stream_start::Position::EarliestRetained(wallet::EarliestRetained {})
        }
        EventStreamStart::LiveTail => {
            wallet::event_stream_start::Position::LiveTail(wallet::LiveTail {})
        }
    };
    wallet::EventStreamStart {
        position: Some(position),
    }
}

fn mempool_event_envelope_from_message(
    expected_network: Network,
    message: wallet::MempoolEventEnvelope,
) -> Result<MempoolEventEnvelope, IndexerError> {
    let event = match message.event.ok_or_else(|| {
        IndexerError::malformed("mempool_event_envelope.event", "field is missing")
    })? {
        wallet::mempool_event_envelope::Event::Added(added) => {
            let entry = mempool_entry_from_message(added.entry.ok_or_else(|| {
                IndexerError::malformed("mempool_event_envelope.added.entry", "field is missing")
            })?)
            .map_err(wallet_decode_error_to_indexer_error)?;
            ensure_mempool_entry_network(expected_network, &entry)?;
            MempoolEvent::Added { entry }
        }
        wallet::mempool_event_envelope::Event::Invalidated(invalidated) => {
            MempoolEvent::Invalidated {
                transaction_id: transaction_id_from_rpc_hex(
                    "mempool_event_envelope.invalidated.transaction_id",
                    &invalidated.transaction_id,
                )?,
                reason: mempool_eviction_reason_from_message(invalidated.reason)?,
            }
        }
        wallet::mempool_event_envelope::Event::Mined(mined) => MempoolEvent::Mined {
            transaction_id: transaction_id_from_rpc_hex(
                "mempool_event_envelope.mined.transaction_id",
                &mined.transaction_id,
            )?,
            mined_height: BlockHeight::new(mined.mined_height),
            block_hash: block_hash_from_rpc_hex(
                "mempool_event_envelope.mined.block_hash",
                &mined.block_hash,
            )?,
        },
    };
    Ok(MempoolEventEnvelope {
        cursor: MempoolEventCursor::from_bytes(message.cursor),
        event_sequence: message.event_sequence,
        source_observed_unix_millis: message.source_observed_unix_millis,
        event,
    })
}

fn mempool_eviction_reason_from_message(
    encoded: i32,
) -> Result<MempoolEvictionReason, IndexerError> {
    match wallet::MempoolEvictionReason::try_from(encoded) {
        Ok(wallet::MempoolEvictionReason::Conflict) => Ok(MempoolEvictionReason::Conflict),
        Ok(wallet::MempoolEvictionReason::Expired) => Ok(MempoolEvictionReason::Expired),
        Ok(wallet::MempoolEvictionReason::LowFee) => Ok(MempoolEvictionReason::LowFee),
        Ok(wallet::MempoolEvictionReason::NodeRejected) => Ok(MempoolEvictionReason::NodeRejected),
        Ok(wallet::MempoolEvictionReason::Unknown) => Ok(MempoolEvictionReason::Unknown),
        Ok(wallet::MempoolEvictionReason::Unspecified) | Err(_) => Err(IndexerError::malformed(
            "mempool_event_envelope.invalidated.reason",
            format!("unknown mempool eviction reason {encoded}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ErrorReason, IndexerError};
    use tonic::{Code, Status};
    use tonic_types::ErrorDetails;

    const EXPECTED_NETWORK: Network = Network::ZcashRegtest;
    const MISMATCHED_NETWORK: Network = Network::ZcashMainnet;

    #[allow(
        clippy::expect_used,
        reason = "syntactically valid URI; connect_lazy cannot fail here"
    )]
    fn build_index() -> RemoteChainIndex {
        RemoteChainIndex::connect(RemoteOpenOptions {
            endpoint: "http://127.0.0.1:1".to_owned(),
            network: Network::ZcashRegtest,
        })
        .expect("connect_lazy never errors for a syntactically valid URI")
    }

    fn current_client_ptr(index: &RemoteChainIndex) -> usize {
        Arc::as_ptr(&index.client.load_full()) as usize
    }

    fn synthetic_chain_epoch(network: Network) -> wallet::ChainEpoch {
        wallet::ChainEpoch {
            chain_epoch_id: 7,
            network_name: encode_zinder_native_chain_name(network).to_owned(),
            artifact_schema_version: 1,
            created_at_millis: 1_774_670_400_000,
            visible_tip: Some(wallet::BlockTip {
                height: 42,
                hash: "11".repeat(32),
            }),
            settled_tip: Some(wallet::BlockTip {
                height: 40,
                hash: "22".repeat(32),
            }),
            sapling_commitment_tree_size: 0,
            orchard_commitment_tree_size: 0,
            ironwood_commitment_tree_size: 0,
        }
    }

    fn synthetic_chain_view(network: Network) -> wallet::ChainView {
        wallet::ChainView {
            chain_epoch: Some(synthetic_chain_epoch(network)),
            indexed_tip: None,
            upstream_tip: None,
            materialized_views: None,
        }
    }

    fn synthetic_block_id(height: u32, hash_byte: u8) -> wallet::BlockId {
        wallet::BlockId {
            height,
            block_hash: format!("{hash_byte:02x}").repeat(32),
        }
    }

    fn synthetic_block_id_response(
        chain_view: wallet::ChainView,
        block_id: wallet::BlockId,
    ) -> wallet::BlockIdResponse {
        wallet::BlockIdResponse {
            chain_view: Some(chain_view),
            block_id: Some(block_id),
        }
    }

    fn synthetic_block_header_response(
        chain_view: wallet::ChainView,
        block_id: wallet::BlockId,
    ) -> wallet::BlockHeaderResponse {
        wallet::BlockHeaderResponse {
            chain_view: Some(chain_view),
            block_header: Some(wallet::BlockHeader {
                block_id: Some(block_id),
                previous_block_hash: "55".repeat(32),
                merkle_root_hash: "66".repeat(32),
                commitment_bytes: vec![0x77; 32],
                block_time: 1_774_670_400,
                bits: 0x1f07_ffff,
                nonce: vec![0x88; 32],
                version: 4,
            }),
        }
    }

    fn synthetic_selector_response_results(
        expected_network: Network,
        expected_epoch_id: Option<ChainEpochId>,
        requested_selector: BlockSelector,
        chain_view: wallet::ChainView,
        block_id: wallet::BlockId,
    ) -> (
        Result<BlockId, IndexerError>,
        Result<BlockHeader, IndexerError>,
    ) {
        (
            block_id_from_selector_response(
                expected_network,
                expected_epoch_id,
                requested_selector,
                synthetic_block_id_response(chain_view.clone(), block_id.clone()),
            ),
            block_header_from_selector_response(
                expected_network,
                expected_epoch_id,
                requested_selector,
                synthetic_block_header_response(chain_view, block_id),
            ),
        )
    }

    fn synthetic_mempool_entry(network: Network) -> wallet::MempoolEntry {
        wallet::MempoolEntry {
            transaction_id: "33".repeat(32),
            auth_digest: String::new(),
            raw_transaction_bytes: vec![0x01, 0x02, 0x03],
            compact_transaction_data: Some(wallet::CompactTransactionData::default()),
            first_seen_unix_millis: 1_774_670_400_000,
            first_seen_chain_epoch: Some(synthetic_chain_epoch(network)),
            transparent_outputs: Vec::new(),
            transparent_spends: Vec::new(),
        }
    }

    fn transaction_in_mempool_message(network: Network) -> wallet::TransactionStatusResponse {
        wallet::TransactionStatusResponse {
            chain_view: Some(synthetic_chain_view(network)),
            location: Some(wallet::TransactionLocation {
                location: Some(wallet::transaction_location::Location::InMempool(
                    synthetic_mempool_entry(network),
                )),
            }),
        }
    }

    fn mined_transaction_message(network: Network) -> wallet::TransactionStatusResponse {
        wallet::TransactionStatusResponse {
            chain_view: Some(synthetic_chain_view(network)),
            location: Some(wallet::TransactionLocation {
                location: Some(wallet::transaction_location::Location::Mined(
                    wallet::MinedTransaction {
                        location: Some(wallet::MinedBlockLocation {
                            transaction_id: "33".repeat(32),
                            block_height: 40,
                            block_hash: "22".repeat(32),
                            tx_index_in_block: 0,
                        }),
                        chain_context: Some(wallet::MinedTransactionChainContext {
                            consensus_branch_id: 1,
                            block_time: 1_774_670_400,
                            confirmations: 3,
                        }),
                        raw_transaction_bytes: None,
                    },
                )),
            }),
        }
    }

    fn mempool_snapshot_message(
        chain_view_network: Network,
        entry_network: Network,
    ) -> wallet::MempoolSnapshotResponse {
        wallet::MempoolSnapshotResponse {
            chain_view: Some(synthetic_chain_view(chain_view_network)),
            events_resume_cursor: Vec::new(),
            snapshot_age_millis: 0,
            entries: vec![synthetic_mempool_entry(entry_network)],
            next_cursor: Vec::new(),
            source_tip: Some(wallet::BlockTip {
                height: 42,
                hash: "11".repeat(32),
            }),
        }
    }

    fn mempool_added_event_message(entry_network: Network) -> wallet::MempoolEventEnvelope {
        wallet::MempoolEventEnvelope {
            cursor: Vec::new(),
            event_sequence: 1,
            source_observed_unix_millis: 1_774_670_400_000,
            event: Some(wallet::mempool_event_envelope::Event::Added(
                wallet::MempoolAddedEvent {
                    entry: Some(synthetic_mempool_entry(entry_network)),
                },
            )),
        }
    }

    fn committed_event_message(sequence: u64, cursor: Vec<u8>) -> wallet::ChainEventEnvelope {
        let epoch = synthetic_chain_epoch(EXPECTED_NETWORK);
        wallet::ChainEventEnvelope {
            cursor,
            event_sequence: sequence,
            chain_view: Some(synthetic_chain_view(EXPECTED_NETWORK)),
            event: Some(wallet::chain_event_envelope::Event::ChainCommitted(
                wallet::ChainCommitted {
                    committed: Some(wallet::ChainEpochCommitted {
                        chain_epoch: Some(epoch),
                        start_height: 41,
                        end_height: 42,
                    }),
                },
            )),
        }
    }

    fn committed_payload_mut(
        message: &mut wallet::ChainEventEnvelope,
    ) -> Option<&mut wallet::ChainEpochCommitted> {
        match message.event.as_mut()? {
            wallet::chain_event_envelope::Event::ChainCommitted(event) => event.committed.as_mut(),
            wallet::chain_event_envelope::Event::ChainReorged(_) => None,
        }
    }

    fn reorg_event_message() -> wallet::ChainEventEnvelope {
        let mut committed_epoch = synthetic_chain_epoch(EXPECTED_NETWORK);
        committed_epoch.visible_tip = Some(wallet::BlockTip {
            height: 43,
            hash: "33".repeat(32),
        });
        let mut reverted_epoch = synthetic_chain_epoch(EXPECTED_NETWORK);
        reverted_epoch.chain_epoch_id = 6;
        wallet::ChainEventEnvelope {
            cursor: vec![1],
            event_sequence: 7,
            chain_view: Some(wallet::ChainView {
                chain_epoch: Some(committed_epoch.clone()),
                indexed_tip: None,
                upstream_tip: None,
                materialized_views: None,
            }),
            event: Some(wallet::chain_event_envelope::Event::ChainReorged(
                wallet::ChainReorged {
                    reverted: Some(wallet::ChainRangeReverted {
                        chain_epoch: Some(reverted_epoch),
                        start_height: 41,
                        end_height: 42,
                    }),
                    committed: Some(wallet::ChainEpochCommitted {
                        chain_epoch: Some(committed_epoch),
                        start_height: 41,
                        end_height: 43,
                    }),
                },
            )),
        }
    }

    #[test]
    fn contract_revision_five_is_the_minimum() {
        assert!(matches!(
            ensure_supported_contract_revision(4),
            Err(IndexerError::FailedPrecondition { .. })
        ));
        assert!(ensure_supported_contract_revision(5).is_ok());
        assert!(ensure_supported_contract_revision(6).is_ok());
    }

    #[test]
    fn block_selector_responses_reject_the_wrong_network() {
        let (block_id_result, block_header_result) = synthetic_selector_response_results(
            EXPECTED_NETWORK,
            None,
            BlockSelector::Height(BlockHeight::new(41)),
            synthetic_chain_view(MISMATCHED_NETWORK),
            synthetic_block_id(41, 0x33),
        );
        assert!(matches!(
            block_id_result,
            Err(IndexerError::NetworkMismatch { expected, .. })
                if expected == EXPECTED_NETWORK
        ));
        assert!(matches!(
            block_header_result,
            Err(IndexerError::NetworkMismatch { expected, .. })
                if expected == EXPECTED_NETWORK
        ));
    }

    #[test]
    fn block_selector_responses_reject_the_wrong_pinned_epoch() {
        let (block_id_result, block_header_result) = synthetic_selector_response_results(
            EXPECTED_NETWORK,
            Some(ChainEpochId::new(8)),
            BlockSelector::Height(BlockHeight::new(41)),
            synthetic_chain_view(EXPECTED_NETWORK),
            synthetic_block_id(41, 0x33),
        );
        assert!(matches!(
            block_id_result,
            Err(IndexerError::MalformedResponse {
                field: "chain_view.chain_epoch.chain_epoch_id",
                ..
            })
        ));
        assert!(matches!(
            block_header_result,
            Err(IndexerError::MalformedResponse {
                field: "chain_view.chain_epoch.chain_epoch_id",
                ..
            })
        ));
    }

    #[test]
    fn block_selector_responses_reject_the_wrong_requested_height() {
        let (block_id_result, block_header_result) = synthetic_selector_response_results(
            EXPECTED_NETWORK,
            None,
            BlockSelector::Height(BlockHeight::new(41)),
            synthetic_chain_view(EXPECTED_NETWORK),
            synthetic_block_id(40, 0x33),
        );
        assert!(matches!(
            block_id_result,
            Err(IndexerError::MalformedResponse {
                field: "block_id",
                ..
            })
        ));
        assert!(matches!(
            block_header_result,
            Err(IndexerError::MalformedResponse {
                field: "block_header.block_id",
                ..
            })
        ));
    }

    #[test]
    fn block_selector_responses_reject_the_wrong_requested_hash() {
        let (block_id_result, block_header_result) = synthetic_selector_response_results(
            EXPECTED_NETWORK,
            None,
            BlockSelector::Hash(BlockHash::from_bytes([0x44; 32])),
            synthetic_chain_view(EXPECTED_NETWORK),
            synthetic_block_id(41, 0x33),
        );
        assert!(matches!(
            block_id_result,
            Err(IndexerError::MalformedResponse {
                field: "block_id",
                ..
            })
        ));
        assert!(matches!(
            block_header_result,
            Err(IndexerError::MalformedResponse {
                field: "block_header.block_id",
                ..
            })
        ));
    }

    #[test]
    fn block_selector_responses_reject_an_identity_above_the_visible_tip() {
        let (block_id_result, block_header_result) = synthetic_selector_response_results(
            EXPECTED_NETWORK,
            None,
            BlockSelector::Height(BlockHeight::new(43)),
            synthetic_chain_view(EXPECTED_NETWORK),
            synthetic_block_id(43, 0x33),
        );
        assert!(matches!(
            block_id_result,
            Err(IndexerError::MalformedResponse {
                field: "block_id",
                ..
            })
        ));
        assert!(matches!(
            block_header_result,
            Err(IndexerError::MalformedResponse {
                field: "block_header.block_id",
                ..
            })
        ));
    }

    #[test]
    fn block_selector_responses_reject_a_conflicting_visible_tip_hash() {
        for selector in [
            BlockSelector::Height(BlockHeight::new(42)),
            BlockSelector::Hash(BlockHash::from_bytes([0x33; 32])),
        ] {
            let (block_id_result, block_header_result) = synthetic_selector_response_results(
                EXPECTED_NETWORK,
                None,
                selector,
                synthetic_chain_view(EXPECTED_NETWORK),
                synthetic_block_id(42, 0x33),
            );
            assert!(matches!(
                block_id_result,
                Err(IndexerError::MalformedResponse {
                    field: "block_id",
                    ..
                })
            ));
            assert!(matches!(
                block_header_result,
                Err(IndexerError::MalformedResponse {
                    field: "block_header.block_id",
                    ..
                })
            ));
        }
    }

    #[test]
    fn block_selector_responses_reject_the_visible_tip_hash_at_a_lower_height() {
        for selector in [
            BlockSelector::Height(BlockHeight::new(41)),
            BlockSelector::Hash(BlockHash::from_bytes([0x11; 32])),
        ] {
            let (block_id_result, block_header_result) = synthetic_selector_response_results(
                EXPECTED_NETWORK,
                None,
                selector,
                synthetic_chain_view(EXPECTED_NETWORK),
                synthetic_block_id(41, 0x11),
            );
            assert!(matches!(
                block_id_result,
                Err(IndexerError::MalformedResponse {
                    field: "block_id",
                    ..
                })
            ));
            assert!(matches!(
                block_header_result,
                Err(IndexerError::MalformedResponse {
                    field: "block_header.block_id",
                    ..
                })
            ));
        }
    }

    #[test]
    fn block_selector_responses_accept_matching_height_and_hash_identities()
    -> Result<(), IndexerError> {
        for (selector, wire_block_id, expected_block_id) in [
            (
                BlockSelector::Height(BlockHeight::new(41)),
                synthetic_block_id(41, 0x33),
                BlockId::new(BlockHeight::new(41), BlockHash::from_bytes([0x33; 32])),
            ),
            (
                BlockSelector::Hash(BlockHash::from_bytes([0x11; 32])),
                synthetic_block_id(42, 0x11),
                BlockId::new(BlockHeight::new(42), BlockHash::from_bytes([0x11; 32])),
            ),
        ] {
            let (block_id_result, block_header_result) = synthetic_selector_response_results(
                EXPECTED_NETWORK,
                Some(ChainEpochId::new(7)),
                selector,
                synthetic_chain_view(EXPECTED_NETWORK),
                wire_block_id,
            );
            assert_eq!(block_id_result?, expected_block_id);
            assert_eq!(block_header_result?.block_id, expected_block_id);
        }
        Ok(())
    }

    #[test]
    fn chain_event_decoder_rejects_empty_cursor_and_zero_sequence() {
        assert!(matches!(
            chain_event_envelope_from_message(
                EXPECTED_NETWORK,
                committed_event_message(1, Vec::new())
            ),
            Err(IndexerError::MalformedResponse {
                field: "cursor",
                ..
            })
        ));
        assert!(matches!(
            chain_event_envelope_from_message(
                EXPECTED_NETWORK,
                committed_event_message(0, vec![1])
            ),
            Err(IndexerError::MalformedResponse {
                field: "event_sequence",
                ..
            })
        ));
    }

    #[test]
    fn chain_event_decoder_rejects_tip_and_empty_range_mismatches() {
        let mut wrong_tip = committed_event_message(1, vec![1]);
        assert!(committed_payload_mut(&mut wrong_tip).is_some());
        if let Some(committed) = committed_payload_mut(&mut wrong_tip) {
            committed.end_height = 41;
        }
        assert!(chain_event_envelope_from_message(EXPECTED_NETWORK, wrong_tip).is_err());

        let mut malformed_empty = committed_event_message(1, vec![1]);
        assert!(committed_payload_mut(&mut malformed_empty).is_some());
        if let Some(committed) = committed_payload_mut(&mut malformed_empty) {
            committed.start_height = 44;
            committed.end_height = 42;
        }
        assert!(chain_event_envelope_from_message(EXPECTED_NETWORK, malformed_empty).is_err());
    }

    #[test]
    fn chain_event_decoder_accepts_the_max_height_empty_range_marker() {
        let mut message = committed_event_message(1, vec![1]);
        let max_height = u32::MAX;
        let mut chain_epoch = synthetic_chain_epoch(EXPECTED_NETWORK);
        chain_epoch.visible_tip = Some(wallet::BlockTip {
            height: max_height,
            hash: "44".repeat(32),
        });
        message.chain_view = Some(wallet::ChainView {
            chain_epoch: Some(chain_epoch.clone()),
            indexed_tip: None,
            upstream_tip: None,
            materialized_views: None,
        });
        if let Some(committed) = committed_payload_mut(&mut message) {
            committed.chain_epoch = Some(chain_epoch);
            committed.start_height = max_height;
            committed.end_height = max_height - 1;
        }

        assert!(chain_event_envelope_from_message(EXPECTED_NETWORK, message).is_ok());
    }

    #[test]
    fn chain_event_stream_requires_increasing_sequences_and_unique_cursors_but_allows_gaps()
    -> Result<(), IndexerError> {
        let first = chain_event_envelope_from_message(
            EXPECTED_NETWORK,
            committed_event_message(1, vec![1]),
        )?;
        let gap = chain_event_envelope_from_message(
            EXPECTED_NETWORK,
            committed_event_message(3, vec![3]),
        )?;
        let duplicate_sequence = chain_event_envelope_from_message(
            EXPECTED_NETWORK,
            committed_event_message(3, vec![4]),
        )?;
        let nonadjacent_repeated_cursor = chain_event_envelope_from_message(
            EXPECTED_NETWORK,
            committed_event_message(4, vec![1]),
        )?;
        let event_after_rejections = chain_event_envelope_from_message(
            EXPECTED_NETWORK,
            committed_event_message(4, vec![4]),
        )?;
        let mut state = ChainEventStreamState::default();

        assert!(state.validate(&first).is_ok());
        assert!(state.validate(&gap).is_ok());
        assert!(state.validate(&duplicate_sequence).is_err());
        assert!(state.validate(&nonadjacent_repeated_cursor).is_err());
        assert!(state.validate(&event_after_rejections).is_ok());
        Ok(())
    }

    #[test]
    fn chain_event_stream_fails_closed_before_cursor_retention_capacity_is_exceeded()
    -> Result<(), IndexerError> {
        let first = chain_event_envelope_from_message(
            EXPECTED_NETWORK,
            committed_event_message(1, vec![1]),
        )?;
        let over_capacity = chain_event_envelope_from_message(
            EXPECTED_NETWORK,
            committed_event_message(2, vec![2]),
        )?;
        let mut state = ChainEventStreamState {
            cursor_count_limit: 1,
            cursor_bytes_limit: usize::MAX,
            ..ChainEventStreamState::default()
        };

        assert!(state.validate(&first).is_ok());
        assert!(matches!(
            state.validate(&over_capacity),
            Err(IndexerError::MalformedResponse {
                field: "cursor",
                ..
            })
        ));
        assert_eq!(state.previous_sequence, Some(1));
        assert_eq!(state.seen_cursors.len(), 1);
        Ok(())
    }

    #[test]
    fn chain_event_decoder_enforces_reorg_range_epoch_and_settlement_relationships() {
        assert!(chain_event_envelope_from_message(EXPECTED_NETWORK, reorg_event_message()).is_ok());

        let mut misaligned = reorg_event_message();
        if let Some(wallet::chain_event_envelope::Event::ChainReorged(reorg)) =
            misaligned.event.as_mut()
            && let Some(committed) = reorg.committed.as_mut()
        {
            committed.start_height = 42;
        }
        assert!(chain_event_envelope_from_message(EXPECTED_NETWORK, misaligned).is_err());

        let mut settlement_changed = reorg_event_message();
        if let Some(wallet::chain_event_envelope::Event::ChainReorged(reorg)) =
            settlement_changed.event.as_mut()
            && let Some(reverted_epoch) = reorg
                .reverted
                .as_mut()
                .and_then(|reverted| reverted.chain_epoch.as_mut())
        {
            reverted_epoch.settled_tip = Some(wallet::BlockTip {
                height: 39,
                hash: "55".repeat(32),
            });
        }
        assert!(chain_event_envelope_from_message(EXPECTED_NETWORK, settlement_changed).is_err());

        let mut zero_epoch = committed_event_message(1, vec![1]);
        if let Some(chain_epoch) = zero_epoch
            .chain_view
            .as_mut()
            .and_then(|view| view.chain_epoch.as_mut())
        {
            chain_epoch.chain_epoch_id = 0;
        }
        assert!(chain_event_envelope_from_message(EXPECTED_NETWORK, zero_epoch).is_err());
    }

    #[test]
    fn transaction_in_mempool_rejects_mismatched_network() {
        let outcome = tx_status_from_message(
            EXPECTED_NETWORK,
            TransactionId::from_bytes([0x33; 32]),
            None,
            transaction_in_mempool_message(MISMATCHED_NETWORK),
        );

        assert!(matches!(
            outcome,
            Err(IndexerError::NetworkMismatch {
                expected: EXPECTED_NETWORK,
                ref actual,
            }) if actual == encode_zinder_native_chain_name(MISMATCHED_NETWORK)
        ));
    }

    #[test]
    fn transaction_in_mempool_accepts_matching_network() {
        let outcome = tx_status_from_message(
            EXPECTED_NETWORK,
            TransactionId::from_bytes([0x33; 32]),
            None,
            transaction_in_mempool_message(EXPECTED_NETWORK),
        );

        assert!(matches!(
            outcome,
            Ok(TxStatus::InMempool(entry))
                if entry.first_seen_chain_epoch().network == EXPECTED_NETWORK
        ));
    }

    #[test]
    fn transaction_in_mempool_rejects_a_different_transaction_id() {
        let outcome = tx_status_from_message(
            EXPECTED_NETWORK,
            TransactionId::from_bytes([0x44; 32]),
            None,
            transaction_in_mempool_message(EXPECTED_NETWORK),
        );

        assert!(matches!(
            outcome,
            Err(IndexerError::MalformedResponse {
                field: "location.in_mempool.transaction_id",
                ..
            })
        ));
    }

    #[test]
    fn mined_transaction_rejects_mismatched_network() {
        let outcome = tx_status_from_message(
            EXPECTED_NETWORK,
            TransactionId::from_bytes([0x33; 32]),
            None,
            mined_transaction_message(MISMATCHED_NETWORK),
        );

        assert!(matches!(outcome, Err(IndexerError::NetworkMismatch { .. })));
    }

    #[test]
    fn mined_transaction_rejects_a_different_transaction_id() {
        let outcome = tx_status_from_message(
            EXPECTED_NETWORK,
            TransactionId::from_bytes([0x44; 32]),
            None,
            mined_transaction_message(EXPECTED_NETWORK),
        );

        assert!(matches!(
            outcome,
            Err(IndexerError::MalformedResponse {
                field: "mined.location.transaction_id",
                ..
            })
        ));
    }

    #[test]
    fn transaction_status_rejects_a_response_from_another_epoch() {
        let outcome = tx_status_from_message(
            EXPECTED_NETWORK,
            TransactionId::from_bytes([0x33; 32]),
            Some(ChainEpochId::new(8)),
            transaction_in_mempool_message(EXPECTED_NETWORK),
        );

        assert!(matches!(
            outcome,
            Err(IndexerError::MalformedResponse { .. })
        ));
    }

    #[test]
    fn transparent_unspent_header_rejects_a_response_from_another_epoch() {
        let mut pinned_epoch = None;
        let header_seen = std::sync::atomic::AtomicBool::new(false);
        let outcome = transparent_unspent_output_stream_item(
            EXPECTED_NETWORK,
            Some(ChainEpochId::new(8)),
            &mut pinned_epoch,
            &header_seen,
            wallet::TransparentUnspentOutputsChunk {
                body: Some(wallet::transparent_unspent_outputs_chunk::Body::Header(
                    synthetic_chain_view(EXPECTED_NETWORK),
                )),
            },
        );

        assert!(matches!(
            outcome,
            Err(IndexerError::MalformedResponse { .. })
        ));
        assert!(!header_seen.load(std::sync::atomic::Ordering::Acquire));
    }

    #[test]
    fn transparent_history_header_rejects_a_response_from_another_epoch() {
        let header_seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut state = TransparentHistoryStreamState {
            expected_network: EXPECTED_NETWORK,
            expected_epoch_id: Some(ChainEpochId::new(8)),
            address_script_hash: TransparentAddressScriptHash::from_bytes([0x51; 32]),
            pinned_chain_epoch: None,
            header_seen: Arc::clone(&header_seen),
            stream_failed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let outcome = transparent_address_tx_ids_stream_item(
            &mut state,
            wallet::TransparentAddressTxIdsChunk {
                body: Some(wallet::transparent_address_tx_ids_chunk::Body::Header(
                    synthetic_chain_view(EXPECTED_NETWORK),
                )),
            },
        );

        assert!(matches!(
            &outcome,
            Err(IndexerError::ChainEpochPinUnavailable)
        ));
        assert_eq!(
            outcome.as_ref().err().map(IndexerError::retry_policy),
            Some(crate::RetryPolicy::RefreshChainEpoch)
        );
        assert!(!header_seen.load(std::sync::atomic::Ordering::Acquire));
    }

    #[tokio::test]
    async fn transparent_history_stream_terminates_after_epoch_mismatch() {
        let state = TransparentHistoryStreamState {
            expected_network: EXPECTED_NETWORK,
            expected_epoch_id: Some(ChainEpochId::new(8)),
            address_script_hash: TransparentAddressScriptHash::from_bytes([0x51; 32]),
            pinned_chain_epoch: None,
            header_seen: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            stream_failed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let mismatched_header = wallet::TransparentAddressTxIdsChunk {
            body: Some(wallet::transparent_address_tx_ids_chunk::Body::Header(
                synthetic_chain_view(EXPECTED_NETWORK),
            )),
        };
        let later_item = wallet::TransparentAddressTxIdsChunk {
            body: Some(wallet::transparent_address_tx_ids_chunk::Body::Item(
                wallet::TransparentAddressTxId {
                    transaction_id: encode_rpc_transaction_id_hex(TransactionId::from_bytes(
                        [0x31; 32],
                    )),
                    block_height: 42,
                    tx_index_in_block: 0,
                    block_hash: "11".repeat(32),
                    cursor: Vec::new(),
                },
            )),
        };
        let wire_stream = futures_util::stream::iter([Ok(mismatched_header), Ok(later_item)]);
        let mut history_stream = transparent_address_tx_ids_stream(Box::pin(wire_stream), state);

        assert!(matches!(
            history_stream.next().await,
            Some(Err(IndexerError::ChainEpochPinUnavailable))
        ));
        assert!(
            history_stream.next().await.is_none(),
            "an epoch mismatch must terminate before later items or terminal checks"
        );
    }

    #[test]
    fn empty_transparent_history_stream_requires_one_valid_header() {
        assert!(missing_transparent_history_header_error(false, false).is_some());
        assert!(missing_transparent_history_header_error(false, true).is_none());

        let header_seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut state = TransparentHistoryStreamState {
            expected_network: EXPECTED_NETWORK,
            expected_epoch_id: Some(ChainEpochId::new(7)),
            address_script_hash: TransparentAddressScriptHash::from_bytes([0x51; 32]),
            pinned_chain_epoch: None,
            header_seen: Arc::clone(&header_seen),
            stream_failed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let outcome = transparent_address_tx_ids_stream_item(
            &mut state,
            wallet::TransparentAddressTxIdsChunk {
                body: Some(wallet::transparent_address_tx_ids_chunk::Body::Header(
                    synthetic_chain_view(EXPECTED_NETWORK),
                )),
            },
        );

        assert!(matches!(outcome, Ok(None)));
        assert!(header_seen.load(std::sync::atomic::Ordering::Acquire));
        assert!(missing_transparent_history_header_error(true, false).is_none());
    }

    #[test]
    fn empty_transparent_unspent_stream_requires_one_valid_header() {
        assert!(missing_transparent_unspent_header_error(false).is_some());

        let mut pinned_epoch = None;
        let header_seen = std::sync::atomic::AtomicBool::new(false);
        let outcome = transparent_unspent_output_stream_item(
            EXPECTED_NETWORK,
            Some(ChainEpochId::new(7)),
            &mut pinned_epoch,
            &header_seen,
            wallet::TransparentUnspentOutputsChunk {
                body: Some(wallet::transparent_unspent_outputs_chunk::Body::Header(
                    synthetic_chain_view(EXPECTED_NETWORK),
                )),
            },
        );

        assert!(matches!(outcome, Ok(None)));
        assert!(header_seen.load(std::sync::atomic::Ordering::Acquire));
        assert!(missing_transparent_unspent_header_error(true).is_none());
    }

    #[test]
    fn compact_stream_rejects_empty_and_truncated_success() {
        assert!(
            incomplete_compact_block_stream_error(10, BlockHeight::new(12)).is_some(),
            "an empty stream still owes its first requested height"
        );
        assert!(
            incomplete_compact_block_stream_error(12, BlockHeight::new(12)).is_some(),
            "a truncated stream still owes its final requested height"
        );
        assert!(
            incomplete_compact_block_stream_error(
                u64::from(u32::MAX) + 1,
                BlockHeight::new(u32::MAX),
            )
            .is_none(),
            "the terminal sentinel must represent the complete full-domain range"
        );
    }

    #[test]
    fn subtree_response_rejects_wrong_echo_and_noncontiguous_indices() {
        let range = SubtreeRootRange::new(
            ShieldedProtocol::Sapling,
            SubtreeRootIndex::new(4),
            std::num::NonZeroU32::new(2).unwrap_or(std::num::NonZeroU32::MIN),
        );
        let response = |protocol, start_index, indices: &[u32]| wallet::SubtreeRootsResponse {
            chain_view: Some(synthetic_chain_view(EXPECTED_NETWORK)),
            shielded_protocol: protocol,
            start_index,
            subtree_roots: indices
                .iter()
                .map(|index| wallet::SubtreeRoot {
                    subtree_index: *index,
                    root_hash: vec![0; 32],
                    completing_block_hash: "22".repeat(32),
                    completing_block_height: 40,
                })
                .collect(),
        };

        assert!(
            subtree_roots_from_response(
                EXPECTED_NETWORK,
                Some(ChainEpochId::new(7)),
                range,
                response(wallet::ShieldedProtocol::Orchard as i32, 4, &[4]),
            )
            .is_err()
        );
        assert!(
            subtree_roots_from_response(
                EXPECTED_NETWORK,
                Some(ChainEpochId::new(7)),
                range,
                response(wallet::ShieldedProtocol::Sapling as i32, 3, &[3]),
            )
            .is_err()
        );
        assert!(
            subtree_roots_from_response(
                EXPECTED_NETWORK,
                Some(ChainEpochId::new(7)),
                range,
                response(wallet::ShieldedProtocol::Sapling as i32, 4, &[4, 6]),
            )
            .is_err()
        );
        assert!(matches!(
            subtree_roots_from_response(
                EXPECTED_NETWORK,
                Some(ChainEpochId::new(7)),
                range,
                response(wallet::ShieldedProtocol::Sapling as i32, 4, &[4, 5]),
            ),
            Ok(roots) if roots.len() == 2
        ));
    }

    #[test]
    fn mempool_snapshot_rejects_mismatched_chain_view_network() {
        let outcome = mempool_snapshot_view_from_message(
            EXPECTED_NETWORK,
            mempool_snapshot_message(MISMATCHED_NETWORK, EXPECTED_NETWORK),
        );

        assert!(matches!(
            outcome,
            Err(IndexerError::NetworkMismatch {
                expected: EXPECTED_NETWORK,
                ref actual,
            }) if actual == encode_zinder_native_chain_name(MISMATCHED_NETWORK)
        ));
    }

    #[test]
    fn mempool_snapshot_rejects_mismatched_entry_network() {
        let mut message = mempool_snapshot_message(EXPECTED_NETWORK, MISMATCHED_NETWORK);
        message
            .entries
            .insert(0, synthetic_mempool_entry(EXPECTED_NETWORK));
        let outcome = mempool_snapshot_view_from_message(EXPECTED_NETWORK, message);

        assert!(matches!(
            outcome,
            Err(IndexerError::NetworkMismatch {
                expected: EXPECTED_NETWORK,
                ref actual,
            }) if actual == encode_zinder_native_chain_name(MISMATCHED_NETWORK)
        ));
    }

    #[test]
    fn mempool_snapshot_rejects_a_missing_source_tip() {
        let mut message = mempool_snapshot_message(EXPECTED_NETWORK, EXPECTED_NETWORK);
        message.source_tip = None;

        assert!(mempool_snapshot_view_from_message(EXPECTED_NETWORK, message).is_err());
    }

    #[test]
    fn mempool_snapshot_rejects_a_source_tip_that_differs_from_the_chain_view() {
        let mut message = mempool_snapshot_message(EXPECTED_NETWORK, EXPECTED_NETWORK);
        message.source_tip = Some(wallet::BlockTip {
            height: 42,
            hash: "22".repeat(32),
        });

        assert!(mempool_snapshot_view_from_message(EXPECTED_NETWORK, message).is_err());
    }

    #[test]
    fn mempool_snapshot_accepts_matching_networks() {
        let outcome = mempool_snapshot_view_from_message(
            EXPECTED_NETWORK,
            mempool_snapshot_message(EXPECTED_NETWORK, EXPECTED_NETWORK),
        );

        assert!(matches!(
            outcome,
            Ok(snapshot)
                if snapshot.chain_epoch.network == EXPECTED_NETWORK
                    && snapshot.entries.len() == 1
                    && snapshot.entries[0].first_seen_chain_epoch().network == EXPECTED_NETWORK
        ));
    }

    #[test]
    fn mempool_added_event_rejects_mismatched_entry_network() {
        let outcome = mempool_event_envelope_from_message(
            EXPECTED_NETWORK,
            mempool_added_event_message(MISMATCHED_NETWORK),
        );

        assert!(matches!(
            outcome,
            Err(IndexerError::NetworkMismatch {
                expected: EXPECTED_NETWORK,
                ref actual,
            }) if actual == encode_zinder_native_chain_name(MISMATCHED_NETWORK)
        ));
    }

    #[test]
    fn mempool_added_event_accepts_matching_entry_network() {
        let outcome = mempool_event_envelope_from_message(
            EXPECTED_NETWORK,
            mempool_added_event_message(EXPECTED_NETWORK),
        );

        assert!(matches!(
            outcome,
            Ok(MempoolEventEnvelope {
                event: MempoolEvent::Added { entry },
                ..
            }) if entry.first_seen_chain_epoch().network == EXPECTED_NETWORK
        ));
    }

    #[tokio::test]
    async fn map_status_swaps_channel_on_poisoned_unavailable() {
        let index = build_index();
        let before = current_client_ptr(&index);

        let err = index.map_status(Status::new(Code::Unavailable, ""));

        let after = current_client_ptr(&index);
        assert!(
            matches!(
                &err,
                IndexerError::ServiceUnavailable { reason }
                    if reason.starts_with("missing zinder.dev ErrorInfo")
            ),
            "expected ServiceUnavailable with poisoned-transport reason, got {err:?}"
        );
        assert_ne!(
            before, after,
            "empty Code::Unavailable without zinder.dev ErrorInfo must swap the channel"
        );
    }

    #[tokio::test]
    async fn map_status_keeps_channel_on_invalid_argument() {
        let index = build_index();
        let before = current_client_ptr(&index);

        let _ = index.map_status(Status::new(Code::InvalidArgument, "bad cursor"));

        let after = current_client_ptr(&index);
        assert_eq!(
            before, after,
            "application-level errors with InvalidArgument must not rebuild the channel"
        );
    }

    #[tokio::test]
    async fn map_status_keeps_channel_on_typed_readiness_unavailable() {
        let index = build_index();
        let before = current_client_ptr(&index);
        let status = Status::with_error_details(
            Code::Unavailable,
            "service readiness is closed",
            ErrorDetails::with_error_info(
                ErrorReason::ServiceNotReady.as_str(),
                ZINDER_ERROR_DOMAIN,
                std::collections::HashMap::from([(
                    "readiness_cause".to_owned(),
                    "ingest_control_unavailable".to_owned(),
                )]),
            ),
        );

        let error = index.map_status(status);

        let after = current_client_ptr(&index);
        assert_eq!(
            before, after,
            "typed readiness failures must not be mistaken for poisoned transport"
        );
        assert!(matches!(
            error,
            IndexerError::RemoteFailure {
                reason: ErrorReason::ServiceNotReady,
                retry_policy: crate::RetryPolicy::RetryWithBackoff,
                ..
            }
        ));
    }
}
