//! Native protobuf encoders for [`WalletQueryApi`] reads.
//!
//! These free functions translate epoch-bound query responses into
//! `zinder_proto::v1::wallet` messages. They take any `WalletQueryApi`
//! implementation by reference and never call upstream nodes or open storage
//! directly. Splitting these out as free functions instead of a blanket trait
//! impl keeps the [`WalletQueryApi`] boundary free of `zinder_proto` types.

use zebra_chain::transparent::Address as ZebraTransparentAddress;
use zinder_core::{
    BlockBlobArtifact, BlockHeight, BroadcastAccepted, BroadcastDuplicate,
    BroadcastInvalidEncoding, BroadcastQueued, BroadcastRejected, BroadcastRejectionReason,
    BroadcastUnknown, ChainEpoch, ChainEpochId, CompactBlockArtifact, MinedTransactionChainContext,
    Network, RawTransactionBytes, ShieldedProtocol, SubtreeRootArtifact, SubtreeRootRange,
    TransactionBroadcastOutcome, TransactionId, TransactionLocation, TransparentAddressScriptHash,
    TransparentAddressTxIndexArtifact, TransparentOutPoint, TransparentOutputsByOutpointResponse,
    TransparentSpendsByOutpointResponse, TransparentUnspentOutput,
    TransparentUnspentOutputsByOutpointResponse, TransparentUtxoSetSummary, TxStatus,
    wire::{encode_rpc_block_hash_hex, encode_rpc_merkle_root_hex, encode_rpc_transaction_id_hex},
};
use zinder_materialized_views::MaterializedViewPreset;
use zinder_proto::v1::{ops, wallet};
use zinder_proto::wire::{
    compact_block_message, encode_transparent_utxo_set_commitment, mempool_entry_message,
};
use zinder_source::transparent_address_matches_network;

use crate::{
    BlockHeaderAtEpoch, BlockIdAtEpoch, ChainEvents, CompactBlock, FullBlock,
    NativeWalletEndpointCapabilities, QueryError, SettledTipBlock, SubtreeRoots, TransactionStatus,
    TransparentAddressTxIds, TransparentAddressTxIdsInRangeRequest,
    TransparentAddressUnspentOutputs, TransparentAddressUnspentOutputsRequest, TreeState,
    UpstreamNodeCapabilities, VisibleTipBlock, WalletQueryApi,
};
pub(crate) use zinder_store::chain_view_message as build_chain_view_message;
use zinder_store::{
    ChainEventEncodeError, ChainEventStreamFamily, StreamCursorTokenV1,
    chain_event_envelope_message, outpoint_message, transparent_output_entry_message,
    transparent_spend_message,
};

/// Encodes the configured node's advertised network-upgrade schedule.
pub async fn network_upgrade_activations_response<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
) -> Result<wallet::NetworkUpgradeActivationsResponse, QueryError> {
    let activations = query_api.network_upgrade_activations().await?;
    Ok(wallet::NetworkUpgradeActivationsResponse {
        activations: activations
            .activations()
            .iter()
            .map(|activation| wallet::NetworkUpgradeActivation {
                consensus_branch_id: activation.branch_id.value(),
                name: activation.name.clone(),
                activation_height: activation.activation_height.value(),
            })
            .collect(),
    })
}

/// Reads and encodes chain-wide value-pool totals from the query's admitted
/// node source.
pub async fn chain_value_pools_at_tip_response<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
) -> Result<wallet::ChainValuePoolsAtTipResponse, QueryError> {
    let response = query_api.chain_value_pools_at_tip().await?;
    Ok(wallet::ChainValuePoolsAtTipResponse {
        chain_view: Some(build_chain_view_message(response.chain_epoch)),
        source_tip: Some(wallet::BlockTip {
            height: response.source_tip.height.value(),
            hash: encode_rpc_block_hash_hex(response.source_tip.hash),
        }),
        pools: response
            .pools
            .into_iter()
            .map(|pool| wallet::ChainValuePool {
                id: pool.id,
                monitored: pool.monitored,
                chain_value_zat: pool.chain_value_zat,
            })
            .collect(),
    })
}

/// Descriptive endpoint metadata used to build `WalletServerInfo`.
///
/// Structural support is deliberately absent. The admitted query owns the
/// immutable capability set used by both native and operational discovery.
#[derive(Clone, Debug)]
pub struct WalletEndpointMetadata {
    /// Network identifier such as `"zcash-mainnet"` or `"zcash-regtest"`.
    pub network: String,
    /// Semver of the running binary, sourced from `CARGO_PKG_VERSION`.
    pub service_version: String,
    /// Git commit embedded by the build.
    pub build_git_commit: String,
    /// Canonical artifact schema version reported by `PrimaryChainStore`.
    pub schema_version: u32,
    /// Configured reorg window depth in blocks.
    pub reorg_window_blocks: u32,
    /// Closed materialized-view workload when this service has an attached store.
    pub materialized_view_preset: Option<MaterializedViewPreset>,
}

impl Default for WalletEndpointMetadata {
    /// Returns development-mode defaults safe for tests and local composition.
    ///
    /// Production deployments replace these through the runtime config loader,
    /// whose default canonical reorg window is bounded.
    fn default() -> Self {
        Self {
            network: "zcash-regtest".to_owned(),
            service_version: env!("CARGO_PKG_VERSION").to_owned(),
            build_git_commit: zinder_runtime::BUILD_GIT_COMMIT.to_owned(),
            schema_version: u32::from(zinder_store::CURRENT_ARTIFACT_SCHEMA_VERSION.value()),
            reorg_window_blocks: 100,
            materialized_view_preset: None,
        }
    }
}

/// Builds the `WalletServerInfo` descriptor from admitted query evidence.
///
/// The capability set is passed through verbatim from the query that owns the
/// concrete handlers. Metadata cannot add or suppress support.
#[must_use]
pub fn build_wallet_server_info(
    metadata: WalletEndpointMetadata,
    capabilities: &NativeWalletEndpointCapabilities,
    upstream: Option<&UpstreamNodeCapabilities>,
) -> wallet::WalletServerInfo {
    let materialized_view_preset = metadata.materialized_view_preset;
    let common = ops::ServerInfo {
        network: metadata.network,
        service_name: env!("CARGO_PKG_NAME").to_owned(),
        service_version: metadata.service_version,
        build_git_commit: metadata.build_git_commit,
        contract_revision: zinder_proto::CONTRACT_REVISION,
        capabilities: capabilities.iter().map(str::to_owned).collect(),
        materialized_view_preset: materialized_view_preset
            .map_or_else(String::new, |preset| preset.as_str().to_owned()),
        materialized_view_identities: materialized_view_preset.map_or_else(Vec::new, |preset| {
            preset
                .consumer_schemas()
                .iter()
                .map(|schema| schema.name.as_str().to_owned())
                .collect()
        }),
    };

    wallet::WalletServerInfo {
        common: Some(common),
        schema_version: metadata.schema_version,
        reorg_window_blocks: metadata.reorg_window_blocks,
        node: Some(build_node_capabilities_descriptor(upstream)),
    }
}

fn build_node_capabilities_descriptor(
    upstream: Option<&UpstreamNodeCapabilities>,
) -> wallet::NodeCapabilitiesDescriptor {
    upstream.map_or_else(
        || wallet::NodeCapabilitiesDescriptor {
            version: None,
            capabilities: Vec::new(),
        },
        |capabilities| wallet::NodeCapabilitiesDescriptor {
            version: capabilities.version.clone(),
            capabilities: capabilities.names().map(str::to_owned).collect(),
        },
    )
}

/// Reads the visible-tip block and encodes the native wallet response.
pub async fn visible_tip_block_response<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    at_epoch_id: Option<ChainEpochId>,
) -> Result<wallet::VisibleTipBlockResponse, QueryError> {
    query_api
        .visible_tip_block(at_epoch_id)
        .await
        .map(build_visible_tip_block_response)
}

/// Reads the block at the chain epoch's settled tip and encodes the native
/// wallet response.
pub(super) async fn settled_tip_block_response<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    at_epoch_id: Option<ChainEpochId>,
) -> Result<wallet::SettledTipBlockResponse, QueryError> {
    query_api
        .settled_tip_block(at_epoch_id)
        .await
        .map(build_settled_tip_block_response)
}

/// Aggregates the chain-wide transparent UTXO set at the settled tip and
/// encodes the native wallet response.
///
/// `commitment_enabled` drives both the store-side fold and whether the
/// optional commitment field is populated, so the capability and the field stay
/// in lockstep.
pub(super) async fn transparent_utxo_set_summary_response<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    at_epoch_id: Option<ChainEpochId>,
) -> Result<wallet::TransparentUtxoSetSummaryResponse, QueryError> {
    query_api
        .transparent_utxo_set_summary(at_epoch_id)
        .await
        .and_then(|summary| build_transparent_utxo_set_summary_response(&summary))
}

fn build_transparent_utxo_set_summary_response(
    summary: &TransparentUtxoSetSummary,
) -> Result<wallet::TransparentUtxoSetSummaryResponse, QueryError> {
    let commitment = summary
        .commitment
        .as_ref()
        .map(encode_transparent_utxo_set_commitment)
        .transpose()
        .map_err(|_| QueryError::UnsupportedWalletEncoding {
            value_kind: "utxo-set commitment scheme",
        })?;
    Ok(wallet::TransparentUtxoSetSummaryResponse {
        chain_view: Some(build_chain_view_message(summary.chain_epoch)),
        utxo_count: summary.utxo_count,
        total_value_zat: summary.total_value_zat,
        summarized_height: summary.summarized_height.value(),
        commitment,
    })
}

/// Reads the compact block at `height` and encodes the native wallet response.
pub async fn compact_block_response<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    height: BlockHeight,
    at_epoch_id: Option<ChainEpochId>,
) -> Result<wallet::CompactBlockResponse, QueryError> {
    query_api
        .compact_block_at(height, at_epoch_id)
        .await
        .map(|compact_block| build_compact_block_response(&compact_block))
}

/// Reads the full block at `height` and encodes the native wallet response.
pub async fn full_block_response<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    height: BlockHeight,
    at_epoch_id: Option<ChainEpochId>,
) -> Result<wallet::FullBlockResponse, QueryError> {
    query_api
        .full_block_at(height, at_epoch_id)
        .await
        .map(build_full_block_response)
}

/// Resolves a typed block selector and encodes the native wallet response.
pub async fn block_id_by_selector_response<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    selector: zinder_core::BlockSelector,
    at_epoch_id: Option<ChainEpochId>,
) -> Result<wallet::BlockIdResponse, QueryError> {
    query_api
        .block_id_by_selector(selector, at_epoch_id)
        .await
        .map(build_block_id_response)
}

/// Reads the typed block-header read model at a typed block selector and
/// encodes the native wallet response.
pub async fn block_header_by_selector_response<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    selector: zinder_core::BlockSelector,
    at_epoch_id: Option<ChainEpochId>,
) -> Result<wallet::BlockHeaderResponse, QueryError> {
    query_api
        .block_header_by_selector(selector, at_epoch_id)
        .await
        .map(|response| build_block_header_response(&response))
}

/// Native answer to a canonical transaction-status lookup.
#[derive(Clone, Debug, PartialEq)]
pub enum TransactionStatusLookup {
    /// The transaction has a typed location the wire oneof can carry.
    Located(wallet::TransactionStatusResponse),
    /// The transaction is absent from the canonical chain of `chain_epoch`,
    /// whose retained history is complete.
    Absent {
        /// Epoch whose canonical chain was searched.
        chain_epoch: ChainEpoch,
    },
}

/// Reads the typed transaction status at `transaction_id` from the canonical
/// chain and encodes the native wallet response.
///
/// [`TransactionStatusLookup::Absent`] is a proven canonical absence at the
/// returned epoch. The adapter decides whether to consult the live mempool
/// and encodes the absence as a `TRANSACTION_NOT_FOUND` status, because the
/// wire oneof carries only located transactions.
pub async fn transaction_response<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    transaction_id: TransactionId,
    at_epoch_id: Option<ChainEpochId>,
) -> Result<TransactionStatusLookup, QueryError> {
    let status = query_api.transaction(transaction_id, at_epoch_id).await?;
    build_transaction_status_response(status)
}

/// Reads the tree-state at exactly `height` and encodes the native wallet response.
pub async fn tree_state_at_response<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    height: BlockHeight,
    at_epoch_id: Option<ChainEpochId>,
) -> Result<wallet::TreeStateResponse, QueryError> {
    query_api
        .tree_state_at(height, at_epoch_id)
        .await
        .map(build_tree_state_response)
}

/// Reads the latest tree-state and encodes the native wallet response.
pub async fn latest_tree_state_checkpoint_response<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    at_epoch_id: Option<ChainEpochId>,
) -> Result<wallet::TreeStateResponse, QueryError> {
    query_api
        .latest_tree_state_checkpoint(at_epoch_id)
        .await
        .map(build_tree_state_response)
}

/// Reads subtree roots in `subtree_root_range` and encodes the native wallet response.
pub async fn subtree_roots_response<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    subtree_root_range: SubtreeRootRange,
    at_epoch_id: Option<ChainEpochId>,
) -> Result<wallet::SubtreeRootsResponse, QueryError> {
    query_api
        .subtree_roots(subtree_root_range, at_epoch_id)
        .await
        .and_then(|subtree_roots| build_subtree_roots_response(&subtree_roots))
}

/// Broadcasts `raw_transaction` and encodes the native wallet response.
pub async fn broadcast_transaction_response<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    raw_transaction: RawTransactionBytes,
) -> Result<wallet::BroadcastTransactionResponse, QueryError> {
    query_api
        .broadcast_transaction(raw_transaction)
        .await
        .and_then(build_broadcast_transaction_response)
}

/// Reads one bounded chain-event page and encodes the native wallet messages.
pub async fn chain_events_response<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    from_cursor: Option<StreamCursorTokenV1>,
    family: ChainEventStreamFamily,
) -> Result<Vec<wallet::ChainEventEnvelope>, QueryError> {
    query_api
        .chain_events(from_cursor, family)
        .await
        .and_then(|chain_events| build_chain_events_response(&chain_events))
}

/// Resolves a batch of canonical-chain transparent outpoints to their
/// referenced outputs and encodes the native wallet response.
///
pub async fn transparent_outputs_by_outpoint_response<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    outpoints: Vec<TransparentOutPoint>,
    at_epoch_id: Option<ChainEpochId>,
) -> Result<wallet::TransparentOutputsByOutpointResponse, QueryError> {
    query_api
        .transparent_outputs_by_outpoint(outpoints, at_epoch_id)
        .await
        .map(build_transparent_outputs_by_outpoint_response)
}

/// Resolves a batch of canonical-chain transparent outpoints to their spends
/// and encodes the native wallet response.
///
pub async fn transparent_spends_by_outpoint_response<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    outpoints: Vec<TransparentOutPoint>,
    at_epoch_id: Option<ChainEpochId>,
) -> Result<wallet::TransparentSpendsByOutpointResponse, QueryError> {
    query_api
        .transparent_spends_by_outpoint(outpoints, at_epoch_id)
        .await
        .map(|response| build_transparent_spends_by_outpoint_response(&response))
}

/// Resolves a batch of canonical-chain transparent outpoints to their unspent
/// referenced outputs (null-if-spent) and encodes the native wallet response.
pub async fn transparent_unspent_outputs_by_outpoint_response<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    outpoints: Vec<TransparentOutPoint>,
    at_epoch_id: Option<ChainEpochId>,
) -> Result<wallet::TransparentUnspentOutputsByOutpointResponse, QueryError> {
    query_api
        .transparent_unspent_outputs_by_outpoint(outpoints, at_epoch_id)
        .await
        .map(build_transparent_unspent_outputs_by_outpoint_response)
}

/// Reads the complete unspent transparent output set for `request` at one
/// pinned chain epoch. The gRPC adapter streams the returned set one
/// message per output.
pub async fn transparent_address_unspent_outputs_response<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    request: TransparentAddressUnspentOutputsRequest,
    at_epoch_id: Option<ChainEpochId>,
) -> Result<TransparentAddressUnspentOutputs, QueryError> {
    query_api
        .transparent_address_unspent_outputs(request, at_epoch_id)
        .await
}

/// Resolves a `wallet::AddressLookup` oneof to the typed
/// `TransparentAddressScriptHash`. String addresses are parsed against
/// `network` and SHA-256-hashed; raw script-hash bytes are taken verbatim.
pub fn address_lookup_to_script_hash(
    address: Option<wallet::AddressLookup>,
    network: Network,
) -> Result<TransparentAddressScriptHash, QueryError> {
    let lookup = address.ok_or(QueryError::InvalidAddress {
        reason: "address selector is required",
    })?;
    let selector = lookup.selector.ok_or(QueryError::InvalidAddress {
        reason: "address selector is empty",
    })?;
    match selector {
        wallet::address_lookup::Selector::ScriptHash(bytes) => {
            let hash_bytes: [u8; 32] =
                bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| QueryError::InvalidAddress {
                        reason: "script_hash must be 32 bytes",
                    })?;
            Ok(TransparentAddressScriptHash::from_bytes(hash_bytes))
        }
        wallet::address_lookup::Selector::Address(address_text) => {
            let zebra_address = address_text
                .parse::<ZebraTransparentAddress>()
                .map_err(|_| QueryError::InvalidAddress {
                    reason: "transparent address could not be parsed",
                })?;
            if !transparent_address_matches_network(zebra_address.network_kind(), network) {
                return Err(QueryError::InvalidAddress {
                    reason: "transparent address network does not match server network",
                });
            }
            let script_pub_key = zebra_address.script().as_raw_bytes().to_vec();
            if script_pub_key.is_empty() {
                return Err(QueryError::InvalidAddress {
                    reason: "transparent address does not produce a receivable script",
                });
            }
            Ok(TransparentAddressScriptHash::of_script_pub_key(
                &script_pub_key,
            ))
        }
    }
}

/// Reads a page of transparent-address tx-history index artifacts and
/// surfaces them through the typed query layer for the gRPC adapter to
/// chunk into a server-streamed response.
pub async fn transparent_address_tx_ids_response<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    request: TransparentAddressTxIdsInRangeRequest,
) -> Result<TransparentAddressTxIds, QueryError> {
    query_api.transparent_address_tx_ids_in_range(request).await
}

/// Builds the leading header chunk carrying the chain epoch pinned for the
/// whole tx-history stream.
#[must_use]
pub fn build_transparent_address_tx_ids_header(
    chain_epoch: ChainEpoch,
) -> wallet::TransparentAddressTxIdsChunk {
    wallet::TransparentAddressTxIdsChunk {
        body: Some(wallet::transparent_address_tx_ids_chunk::Body::Header(
            build_chain_view_message(chain_epoch),
        )),
    }
}

/// Builds one streamed tx-history item chunk.
#[must_use]
pub fn build_transparent_address_tx_ids_chunk(
    artifact: &TransparentAddressTxIndexArtifact,
    cursor: Vec<u8>,
) -> wallet::TransparentAddressTxIdsChunk {
    wallet::TransparentAddressTxIdsChunk {
        body: Some(wallet::transparent_address_tx_ids_chunk::Body::Item(
            wallet::TransparentAddressTxId {
                transaction_id: encode_rpc_transaction_id_hex(artifact.transaction_id),
                block_height: artifact.block_height.value(),
                tx_index_in_block: artifact.tx_index_in_block,
                block_hash: encode_rpc_block_hash_hex(artifact.block_hash),
                cursor,
            },
        )),
    }
}

/// Builds the leading header chunk carrying the chain epoch pinned for the
/// whole unspent-output stream.
#[must_use]
pub fn build_transparent_unspent_outputs_header(
    chain_epoch: ChainEpoch,
) -> wallet::TransparentUnspentOutputsChunk {
    wallet::TransparentUnspentOutputsChunk {
        body: Some(wallet::transparent_unspent_outputs_chunk::Body::Header(
            build_chain_view_message(chain_epoch),
        )),
    }
}

/// Builds one streamed unspent-output item chunk.
#[must_use]
pub fn build_transparent_unspent_output_message(
    output: &TransparentUnspentOutput,
) -> wallet::TransparentUnspentOutputsChunk {
    wallet::TransparentUnspentOutputsChunk {
        body: Some(wallet::transparent_unspent_outputs_chunk::Body::Item(
            wallet::TransparentUnspentOutput {
                address_script_hash: output.address_script_hash.as_bytes().to_vec(),
                script_pub_key: output.script_pub_key.clone(),
                outpoint: Some(outpoint_message(&output.outpoint)),
                value_zat: output.value_zat,
                block_height: output.block_height.value(),
                block_hash: encode_rpc_block_hash_hex(output.block_hash),
            },
        )),
    }
}

fn build_transparent_outputs_by_outpoint_response(
    response: TransparentOutputsByOutpointResponse,
) -> wallet::TransparentOutputsByOutpointResponse {
    wallet::TransparentOutputsByOutpointResponse {
        chain_view: Some(build_chain_view_message(response.chain_epoch)),
        entries: response
            .entries
            .into_iter()
            .map(transparent_output_entry_message)
            .collect(),
    }
}

fn build_transparent_spends_by_outpoint_response(
    response: &TransparentSpendsByOutpointResponse,
) -> wallet::TransparentSpendsByOutpointResponse {
    wallet::TransparentSpendsByOutpointResponse {
        chain_view: Some(build_chain_view_message(response.chain_epoch)),
        spends: response
            .spends
            .iter()
            .map(transparent_spend_message)
            .collect(),
    }
}

fn build_transparent_unspent_outputs_by_outpoint_response(
    response: TransparentUnspentOutputsByOutpointResponse,
) -> wallet::TransparentUnspentOutputsByOutpointResponse {
    wallet::TransparentUnspentOutputsByOutpointResponse {
        chain_view: Some(build_chain_view_message(response.chain_epoch)),
        entries: response
            .entries
            .into_iter()
            .map(transparent_output_entry_message)
            .collect(),
    }
}

fn build_visible_tip_block_response(
    visible_tip_block: VisibleTipBlock,
) -> wallet::VisibleTipBlockResponse {
    wallet::VisibleTipBlockResponse {
        chain_view: Some(build_chain_view_message(visible_tip_block.chain_epoch)),
        visible_tip_block: Some(build_block_id_message(
            visible_tip_block.height,
            visible_tip_block.block_hash,
        )),
    }
}

fn build_settled_tip_block_response(
    settled_tip_block: SettledTipBlock,
) -> wallet::SettledTipBlockResponse {
    wallet::SettledTipBlockResponse {
        chain_view: Some(build_chain_view_message(settled_tip_block.chain_epoch)),
        settled_tip_block: Some(build_block_id_message(
            settled_tip_block.height,
            settled_tip_block.block_hash,
        )),
    }
}

fn build_block_id_response(response: BlockIdAtEpoch) -> wallet::BlockIdResponse {
    wallet::BlockIdResponse {
        chain_view: Some(build_chain_view_message(response.chain_epoch)),
        block_id: Some(build_block_id_message(
            response.block_id.height,
            response.block_id.hash,
        )),
    }
}

fn build_block_header_response(response: &BlockHeaderAtEpoch) -> wallet::BlockHeaderResponse {
    let header = &response.block_header;
    wallet::BlockHeaderResponse {
        chain_view: Some(build_chain_view_message(response.chain_epoch)),
        block_header: Some(wallet::BlockHeader {
            block_id: Some(build_block_id_message(
                header.block_id.height,
                header.block_id.hash,
            )),
            previous_block_hash: encode_rpc_block_hash_hex(header.previous_block_hash),
            merkle_root_hash: encode_rpc_merkle_root_hex(header.merkle_root_hash),
            commitment_bytes: header.commitment_bytes.to_vec(),
            block_time: header.block_time,
            bits: header.bits,
            nonce: header.nonce.to_vec(),
            version: header.version,
        }),
    }
}

fn build_compact_block_response(compact_block: &CompactBlock) -> wallet::CompactBlockResponse {
    wallet::CompactBlockResponse {
        chain_view: Some(build_chain_view_message(compact_block.chain_epoch)),
        compact_block: Some(build_compact_block_message(&compact_block.compact_block)),
    }
}

fn build_full_block_response(full_block: FullBlock) -> wallet::FullBlockResponse {
    wallet::FullBlockResponse {
        chain_view: Some(build_chain_view_message(full_block.chain_epoch)),
        full_block: Some(build_full_block_message(full_block.block_blob)),
    }
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "TxStatus is #[non_exhaustive]; new arms must be wired into the proto oneof in a deliberate change, not folded into a default branch."
)]
fn build_transaction_status_response(
    status: TransactionStatus,
) -> Result<TransactionStatusLookup, QueryError> {
    let chain_epoch = status.chain_epoch;
    let location = match status.status {
        TxStatus::Mined(mined) => {
            wallet::transaction_location::Location::Mined(wallet::MinedTransaction {
                location: Some(build_mined_block_location_message(mined.location)),
                chain_context: Some(build_mined_transaction_chain_context_message(
                    mined.chain_context,
                )),
                raw_transaction_bytes: mined.raw_transaction_bytes,
            })
        }
        TxStatus::InMempool(entry) => {
            wallet::transaction_location::Location::InMempool(mempool_entry_message(&entry))
        }
        TxStatus::NotFound => return Ok(TransactionStatusLookup::Absent { chain_epoch }),
        _ => {
            return Err(QueryError::UnsupportedTransactionStatus {
                reason: "transaction status variant has no wire representation",
            });
        }
    };
    Ok(TransactionStatusLookup::Located(
        wallet::TransactionStatusResponse {
            chain_view: Some(build_chain_view_message(chain_epoch)),
            location: Some(wallet::TransactionLocation {
                location: Some(location),
            }),
        },
    ))
}

fn build_mined_transaction_chain_context_message(
    chain_context: MinedTransactionChainContext,
) -> wallet::MinedTransactionChainContext {
    wallet::MinedTransactionChainContext {
        consensus_branch_id: chain_context.consensus_branch_id.value(),
        block_time: chain_context.block_time,
        confirmations: chain_context.confirmations,
    }
}

fn build_mined_block_location_message(
    transaction: TransactionLocation,
) -> wallet::MinedBlockLocation {
    wallet::MinedBlockLocation {
        transaction_id: encode_rpc_transaction_id_hex(transaction.transaction_id),
        block_height: transaction.block_height.value(),
        block_hash: encode_rpc_block_hash_hex(transaction.block_hash),
        tx_index_in_block: transaction.tx_index_in_block,
    }
}

fn build_tree_state_response(tree_state: TreeState) -> wallet::TreeStateResponse {
    wallet::TreeStateResponse {
        chain_view: Some(build_chain_view_message(tree_state.chain_epoch)),
        height: tree_state.height.value(),
        block_hash: encode_rpc_block_hash_hex(tree_state.block_hash),
        payload_bytes: tree_state.payload_bytes,
        block_time_seconds: Some(tree_state.block_time_seconds),
    }
}

fn build_subtree_roots_response(
    subtree_roots: &SubtreeRoots,
) -> Result<wallet::SubtreeRootsResponse, QueryError> {
    Ok(wallet::SubtreeRootsResponse {
        chain_view: Some(build_chain_view_message(subtree_roots.chain_epoch)),
        shielded_protocol: native_shielded_protocol(subtree_roots.protocol)? as i32,
        start_index: subtree_roots.start_index.value(),
        subtree_roots: subtree_roots
            .subtree_roots
            .iter()
            .map(build_subtree_root_message)
            .collect(),
    })
}

fn build_broadcast_transaction_response(
    broadcast_outcome: TransactionBroadcastOutcome,
) -> Result<wallet::BroadcastTransactionResponse, QueryError> {
    use wallet::broadcast_transaction_response::Outcome;

    let outcome = match broadcast_outcome {
        TransactionBroadcastOutcome::Accepted(accepted) => {
            Outcome::Accepted(build_broadcast_accepted_message(accepted))
        }
        TransactionBroadcastOutcome::Duplicate(duplicate) => {
            Outcome::Duplicate(build_broadcast_duplicate_message(duplicate))
        }
        TransactionBroadcastOutcome::InvalidEncoding(invalid_encoding) => {
            Outcome::InvalidEncoding(build_broadcast_invalid_encoding_message(invalid_encoding))
        }
        TransactionBroadcastOutcome::Queued(queued) => {
            Outcome::Queued(build_broadcast_queued_message(queued))
        }
        TransactionBroadcastOutcome::Rejected(rejected) => {
            Outcome::Rejected(build_broadcast_rejected_message(rejected)?)
        }
        TransactionBroadcastOutcome::Unknown(unknown) => {
            Outcome::Unknown(build_broadcast_unknown_message(unknown))
        }
        _ => {
            return Err(QueryError::UnsupportedWalletEncoding {
                value_kind: "transaction broadcast outcome",
            });
        }
    };

    Ok(wallet::BroadcastTransactionResponse {
        outcome: Some(outcome),
    })
}

fn build_broadcast_accepted_message(accepted: BroadcastAccepted) -> wallet::BroadcastAccepted {
    wallet::BroadcastAccepted {
        transaction_id: encode_rpc_transaction_id_hex(accepted.transaction_id),
    }
}

fn build_broadcast_duplicate_message(duplicate: BroadcastDuplicate) -> wallet::BroadcastDuplicate {
    wallet::BroadcastDuplicate {
        error_code: duplicate.error_code,
        message: duplicate.message,
    }
}

fn build_broadcast_invalid_encoding_message(
    invalid_encoding: BroadcastInvalidEncoding,
) -> wallet::BroadcastInvalidEncoding {
    wallet::BroadcastInvalidEncoding {
        error_code: invalid_encoding.error_code,
        message: invalid_encoding.message,
    }
}

fn build_broadcast_rejected_message(
    rejected: BroadcastRejected,
) -> Result<wallet::BroadcastRejected, QueryError> {
    Ok(wallet::BroadcastRejected {
        error_code: rejected.error_code,
        message: rejected.message,
        kind: broadcast_rejection_reason_to_message(rejected.kind)? as i32,
    })
}

fn build_broadcast_queued_message(queued: BroadcastQueued) -> wallet::BroadcastQueued {
    wallet::BroadcastQueued {
        message: queued.message,
    }
}

#[allow(
    unreachable_patterns,
    reason = "BroadcastRejectionReason is non-exhaustive; the encoder fails closed for future variants."
)]
fn broadcast_rejection_reason_to_message(
    kind: BroadcastRejectionReason,
) -> Result<wallet::BroadcastRejectionReason, QueryError> {
    match kind {
        BroadcastRejectionReason::InvalidSignature => {
            Ok(wallet::BroadcastRejectionReason::InvalidSignature)
        }
        BroadcastRejectionReason::BadExpiryHeight => {
            Ok(wallet::BroadcastRejectionReason::BadExpiryHeight)
        }
        BroadcastRejectionReason::BadConsensusBranch => {
            Ok(wallet::BroadcastRejectionReason::BadConsensusBranch)
        }
        BroadcastRejectionReason::MempoolFull => Ok(wallet::BroadcastRejectionReason::MempoolFull),
        BroadcastRejectionReason::Unknown => Ok(wallet::BroadcastRejectionReason::Unknown),
        _ => Err(QueryError::UnsupportedWalletEncoding {
            value_kind: "transaction broadcast rejection reason",
        }),
    }
}

fn build_broadcast_unknown_message(unknown: BroadcastUnknown) -> wallet::BroadcastUnknown {
    wallet::BroadcastUnknown {
        error_code: unknown.error_code,
        message: unknown.message,
    }
}

fn build_chain_events_response(
    chain_events: &ChainEvents,
) -> Result<Vec<wallet::ChainEventEnvelope>, QueryError> {
    chain_events
        .event_envelopes
        .iter()
        .map(|event_envelope| {
            chain_event_envelope_message(event_envelope).map_err(map_chain_event_encode_error)
        })
        .collect()
}

fn map_chain_event_encode_error(error: ChainEventEncodeError) -> QueryError {
    match error {
        ChainEventEncodeError::UnsupportedChainEvent { event } => {
            QueryError::UnsupportedChainEvent { event }
        }
        ChainEventEncodeError::UnsupportedMempoolEvictionReason => {
            QueryError::UnsupportedWalletEncoding {
                value_kind: "mempool eviction reason",
            }
        }
        _ => QueryError::UnsupportedWalletEncoding {
            value_kind: "chain event",
        },
    }
}

fn build_block_id_message(
    height: BlockHeight,
    block_hash: zinder_core::BlockHash,
) -> wallet::BlockId {
    wallet::BlockId {
        height: height.value(),
        block_hash: encode_rpc_block_hash_hex(block_hash),
    }
}

pub(crate) fn build_compact_block_message(
    compact_block: &CompactBlockArtifact,
) -> wallet::CompactBlock {
    compact_block_message(compact_block)
}

pub(crate) fn build_full_block_message(block_blob: BlockBlobArtifact) -> wallet::FullBlock {
    wallet::FullBlock {
        height: block_blob.height.value(),
        block_hash: encode_rpc_block_hash_hex(block_blob.block_hash),
        payload_bytes: block_blob.raw_block_bytes,
        parent_block_hash: encode_rpc_block_hash_hex(block_blob.parent_hash),
    }
}

fn build_subtree_root_message(subtree_root: &SubtreeRootArtifact) -> wallet::SubtreeRoot {
    wallet::SubtreeRoot {
        subtree_index: subtree_root.subtree_index.value(),
        root_hash: subtree_root.root_hash.as_bytes().into(),
        completing_block_hash: encode_rpc_block_hash_hex(subtree_root.completing_block_hash),
        completing_block_height: subtree_root.completing_block_height.value(),
    }
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "non-exhaustive core protocols must fail closed until the native proto supports them"
)]
fn native_shielded_protocol(
    protocol: ShieldedProtocol,
) -> Result<wallet::ShieldedProtocol, QueryError> {
    match protocol {
        ShieldedProtocol::Sapling => Ok(wallet::ShieldedProtocol::Sapling),
        ShieldedProtocol::Orchard => Ok(wallet::ShieldedProtocol::Orchard),
        ShieldedProtocol::Ironwood => Ok(wallet::ShieldedProtocol::Ironwood),
        _ => Err(QueryError::UnsupportedShieldedProtocol { protocol }),
    }
}

#[cfg(test)]
mod server_info_tests {
    use zinder_proto::capabilities::{
        WALLET_BROADCAST_TRANSACTION_V1, WALLET_EVENTS_MEMPOOL_V2,
        WALLET_MEMPOOL_TRANSPARENT_OUTPUTS_BY_ADDRESS_V1,
        WALLET_MEMPOOL_TRANSPARENT_SPENDS_BY_OUTPOINT_V1, WALLET_READ_BLOCK_HEADER_BY_SELECTOR_V1,
        WALLET_READ_BLOCK_ID_BY_SELECTOR_V1, WALLET_READ_COMPACT_BLOCK_RANGE_V2,
        WALLET_READ_FULL_BLOCK_AT_V1, WALLET_READ_FULL_BLOCK_RANGE_V1,
        WALLET_READ_NETWORK_UPGRADE_ACTIVATIONS_V1, WALLET_READ_SERVER_INFO_V2,
        WALLET_READ_SETTLED_TIP_BLOCK_V1, WALLET_READ_TRANSACTION_BY_ID_V2,
        WALLET_READ_TRANSACTION_BYTES_V1, WALLET_READ_TRANSPARENT_OUTPUTS_V1,
        WALLET_READ_TRANSPARENT_UTXO_SET_SUMMARY_V1, WALLET_READ_TREE_STATE_AT_HEIGHT_V2,
        WALLET_READ_VISIBLE_TIP_BLOCK_V1, WALLET_SNAPSHOT_MEMPOOL_V3,
    };
    use zinder_source::{NodeCapabilities, NodeCapability};
    use zinder_store::RawBlobRetention;
    use zinder_testkit::IngestControlFixture;

    use super::{
        MaterializedViewPreset, NativeWalletEndpointCapabilities, UpstreamNodeCapabilities,
        WalletEndpointMetadata, build_wallet_server_info,
    };
    use crate::AdmittedIngestControl;

    #[tokio::test]
    async fn build_wallet_server_info_populates_node_when_upstream_known()
    -> Result<(), Box<dyn std::error::Error>> {
        let ingest_fixture =
            IngestControlFixture::spawn(zinder_core::Network::ZcashRegtest).await?;
        let admitted_ingest_control = AdmittedIngestControl::connect(
            ingest_fixture.endpoint(),
            None,
            zinder_core::Network::ZcashRegtest,
        )
        .await?;
        let metadata = WalletEndpointMetadata::default();
        let capabilities = NativeWalletEndpointCapabilities::for_admitted_native_wallet_query(
            RawBlobRetention::Transactions,
            NodeCapabilities::default(),
            &admitted_ingest_control,
        );
        let mut upstream = UpstreamNodeCapabilities::from_probed(NodeCapabilities::new([
            NodeCapability::SubtreeRoots,
            NodeCapability::TransactionBroadcast,
        ])?);
        upstream.version = Some("2.4.0".to_owned());

        let descriptor = build_wallet_server_info(metadata, &capabilities, Some(&upstream));
        let Some(node) = descriptor.node else {
            unreachable!("node field must always be set")
        };
        assert_eq!(node.version.as_deref(), Some("2.4.0"));
        assert_eq!(node.capabilities.len(), 2);
        assert!(
            node.capabilities
                .iter()
                .any(|cap| cap == NodeCapability::TransactionBroadcast.name())
        );

        let Some(common) = descriptor.common else {
            unreachable!("common ops.ServerInfo field must always be set")
        };
        assert_eq!(common.service_name, env!("CARGO_PKG_NAME"));
        assert_eq!(common.build_git_commit, zinder_runtime::BUILD_GIT_COMMIT);
        assert!(!common.capabilities.is_empty());
        assert!(common.materialized_view_preset.is_empty());
        assert!(common.materialized_view_identities.is_empty());
        ingest_fixture.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn server_info_reports_the_effective_wallet_workload()
    -> Result<(), Box<dyn std::error::Error>> {
        let ingest_fixture =
            IngestControlFixture::spawn(zinder_core::Network::ZcashRegtest).await?;
        let admitted_ingest_control = AdmittedIngestControl::connect(
            ingest_fixture.endpoint(),
            None,
            zinder_core::Network::ZcashRegtest,
        )
        .await?;
        let metadata = WalletEndpointMetadata {
            materialized_view_preset: Some(MaterializedViewPreset::Wallet),
            ..WalletEndpointMetadata::default()
        };
        let capabilities = NativeWalletEndpointCapabilities::for_admitted_native_wallet_query(
            RawBlobRetention::Transactions,
            NodeCapabilities::default(),
            &admitted_ingest_control,
        );
        let common = build_wallet_server_info(metadata, &capabilities, None)
            .common
            .unwrap_or_default();

        assert_eq!(common.materialized_view_preset, "wallet");
        assert_eq!(
            common.materialized_view_identities,
            MaterializedViewPreset::Wallet
                .consumer_schemas()
                .iter()
                .map(|schema| schema.name.as_str().to_owned())
                .collect::<Vec<_>>()
        );
        ingest_fixture.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn build_wallet_server_info_emits_empty_node_when_no_upstream()
    -> Result<(), Box<dyn std::error::Error>> {
        let ingest_fixture =
            IngestControlFixture::spawn(zinder_core::Network::ZcashRegtest).await?;
        let admitted_ingest_control = AdmittedIngestControl::connect(
            ingest_fixture.endpoint(),
            None,
            zinder_core::Network::ZcashRegtest,
        )
        .await?;
        let metadata = WalletEndpointMetadata::default();
        let capabilities = NativeWalletEndpointCapabilities::for_admitted_native_wallet_query(
            RawBlobRetention::Transactions,
            NodeCapabilities::default(),
            &admitted_ingest_control,
        );
        let descriptor = build_wallet_server_info(metadata, &capabilities, None);
        let Some(node) = descriptor.node else {
            unreachable!("node field must always be set")
        };
        assert!(node.version.is_none());
        assert!(node.capabilities.is_empty());

        let Some(common) = descriptor.common else {
            unreachable!("common ops.ServerInfo field must always be set")
        };
        assert_eq!(common.network, "zcash-regtest");
        ingest_fixture.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn wallet_serving_capabilities_come_from_composed_service_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let ingest_fixture =
            IngestControlFixture::spawn(zinder_core::Network::ZcashRegtest).await?;
        let admitted_ingest_control = AdmittedIngestControl::connect(
            ingest_fixture.endpoint(),
            None,
            zinder_core::Network::ZcashRegtest,
        )
        .await?;
        let transactions = NativeWalletEndpointCapabilities::for_admitted_native_wallet_query(
            RawBlobRetention::Transactions,
            NodeCapabilities::default(),
            &admitted_ingest_control,
        );
        for always_supported in [
            WALLET_READ_VISIBLE_TIP_BLOCK_V1,
            WALLET_READ_SETTLED_TIP_BLOCK_V1,
            WALLET_READ_BLOCK_ID_BY_SELECTOR_V1,
            WALLET_READ_COMPACT_BLOCK_RANGE_V2,
            WALLET_READ_SERVER_INFO_V2,
            WALLET_READ_NETWORK_UPGRADE_ACTIVATIONS_V1,
            WALLET_READ_TRANSACTION_BY_ID_V2,
            WALLET_READ_TRANSACTION_BYTES_V1,
            WALLET_SNAPSHOT_MEMPOOL_V3,
            WALLET_EVENTS_MEMPOOL_V2,
            WALLET_MEMPOOL_TRANSPARENT_OUTPUTS_BY_ADDRESS_V1,
            WALLET_MEMPOOL_TRANSPARENT_SPENDS_BY_OUTPOINT_V1,
        ] {
            assert!(transactions.contains(always_supported));
        }
        for structurally_absent in [
            WALLET_READ_FULL_BLOCK_AT_V1,
            WALLET_READ_FULL_BLOCK_RANGE_V1,
            WALLET_READ_TREE_STATE_AT_HEIGHT_V2,
            WALLET_BROADCAST_TRANSACTION_V1,
            WALLET_READ_BLOCK_HEADER_BY_SELECTOR_V1,
            WALLET_READ_TRANSPARENT_OUTPUTS_V1,
            WALLET_READ_TRANSPARENT_UTXO_SET_SUMMARY_V1,
        ] {
            assert!(!transactions.contains(structurally_absent));
        }

        let probed_node = NodeCapabilities::new([
            NodeCapability::OpenRpcDiscovery,
            NodeCapability::TreeState,
            NodeCapability::TransactionBroadcast,
        ])?;
        let all = NativeWalletEndpointCapabilities::for_admitted_native_wallet_query(
            RawBlobRetention::All,
            probed_node,
            &admitted_ingest_control,
        );
        for evidence_backed in [
            WALLET_READ_FULL_BLOCK_AT_V1,
            WALLET_READ_FULL_BLOCK_RANGE_V1,
            WALLET_READ_TREE_STATE_AT_HEIGHT_V2,
            WALLET_BROADCAST_TRANSACTION_V1,
            WALLET_READ_TRANSACTION_BY_ID_V2,
            WALLET_READ_TRANSACTION_BYTES_V1,
            WALLET_SNAPSHOT_MEMPOOL_V3,
            WALLET_EVENTS_MEMPOOL_V2,
            WALLET_MEMPOOL_TRANSPARENT_OUTPUTS_BY_ADDRESS_V1,
            WALLET_MEMPOOL_TRANSPARENT_SPENDS_BY_OUTPOINT_V1,
        ] {
            assert!(all.contains(evidence_backed));
        }
        for still_absent in [
            WALLET_READ_TRANSPARENT_OUTPUTS_V1,
            WALLET_READ_TRANSPARENT_UTXO_SET_SUMMARY_V1,
        ] {
            assert!(!all.contains(still_absent));
        }
        ingest_fixture.shutdown().await?;
        Ok(())
    }
}
