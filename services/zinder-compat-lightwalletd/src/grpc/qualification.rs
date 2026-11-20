//! gRPC routes for canonical and node-backed light-wallet qualification.

use std::{collections::HashSet, num::NonZeroU32, time::Duration};

use tokio_stream::{self as stream, Stream, StreamExt as _};
use tonic::{Request, Response, Status};
use zebra_chain::transparent::Address;
use zinder_core::{
    BlockHeight, ChainEpochId, Network, RawTransactionBytes, SubtreeRootRange, TransactionId,
    wire::{
        decode_internal_transaction_id, decode_rpc_block_hash_bytes, encode_internal_transaction_id,
    },
};
use zinder_proto::compat::lightwalletd::{self, compact_tx_streamer_server};
use zinder_query::{
    ArtifactKey, CanonicalReader, MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES,
    QualificationWalletQuery, QueryError, StorageReadError, SubtreeRoots, status_from_query_error,
};
use zinder_source::{
    SourceError, SourceTransparentUnspentOutput, TransactionBroadcaster as _,
    TransparentAddressLookup, TransparentAddressRowLimit, UpstreamTransactionLookup,
    ZebraJsonRpcSource, parse_transparent_address, transparent_address_matches_network,
};
use zinder_store::{ArtifactFamily, BlockHashLookup};

use super::{
    CanonicalTransactionMiss, GrpcStream, LightwalletdGrpcAdapter,
    SUBTREE_ROOT_STREAM_CHANNEL_CAPACITY, SubtreeRootReads, SubtreeRootsStream,
    lightwalletd_tree_state, reject_future_tree_state_height, send_response_from_broadcast_outcome,
    shielded_protocol_from_request, spawn_response_stream, standard_fee_response,
    transparent_address_tx_history_request, tree_state_block_lookup_status, tree_state_status,
    u32_to_usize, wire_decode_error_to_status,
};
use crate::CanonicalReaderSlot;
use crate::block_methods::{self, RequestedBlock, requested_block};
use crate::compact_block_messages::CompactBlockShape;

/// The feature-gated canonical reader and forwarded wallet methods used in qualification.
#[derive(Clone)]
pub struct QualificationWallet {
    slot: CanonicalReaderSlot,
    query: QualificationWalletQuery,
}

impl LightwalletdGrpcAdapter<QualificationWallet> {
    /// Serves local block data, retained transactions, and node-backed wallet lookups without a projector.
    #[must_use]
    pub fn for_light_wallet_qualification(
        slot: CanonicalReaderSlot,
        node: ZebraJsonRpcSource,
        replay_wait: Duration,
        replay_waiters_per_slot: NonZeroU32,
    ) -> Self {
        let query = QualificationWalletQuery::new(node, replay_wait, replay_waiters_per_slot);
        Self::new(QualificationWallet { slot, query })
    }

    async fn qualification_transaction(
        &self,
        filter: lightwalletd::TxFilter,
    ) -> Result<lightwalletd::RawTransaction, Status> {
        if filter.hash.is_empty() {
            return Err(Status::invalid_argument("GetTransaction: specify a txid"));
        }
        let id = decode_internal_transaction_id(&filter.hash)
            .map_err(|e| wire_decode_error_to_status(&e))?;
        let reader = self.query_api.slot.capture();
        if !reader.raw_blob_retention().retains_transaction_blobs() {
            let node = self
                .query_api
                .query
                .verified_node(&reader)
                .await
                .map_err(node_status)?;
            return forwarded_transaction(node, id, None).await;
        }
        let miss = match reader.mined_transaction(id).await {
            Ok(Some(mined)) => {
                let blob = mined.transaction_blob.ok_or_else(|| {
                    status_from_query_error(&QueryError::ArtifactUnavailable {
                        family: ArtifactFamily::TransactionBlob,
                        key: ArtifactKey::TransactionId(id),
                    })
                })?;
                return Ok(lightwalletd::RawTransaction {
                    height: u64::from(mined.location.block_height.value()),
                    data: blob.raw_transaction_bytes,
                });
            }
            Ok(None) => CanonicalTransactionMiss::NotInChain {
                at_epoch_id: reader.chain_epoch().id,
            },
            Err(error @ StorageReadError::CanonicalHistoryUnavailable { .. }) => {
                CanonicalTransactionMiss::BelowHistoryFloor(storage_status(error))
            }
            Err(error) => return Err(storage_status(error)),
        };
        drop(reader);
        self.pending_transaction(id, miss).await
    }
}

impl QualificationWallet {
    async fn node(&self) -> Result<&ZebraJsonRpcSource, Status> {
        self.query
            .verified_node(&self.slot.capture())
            .await
            .map_err(node_status)
    }

    async fn balance(
        &self,
        mut addresses: impl Stream<Item = Result<String, Status>> + Unpin,
    ) -> Result<lightwalletd::Balance, Status> {
        let network = self.slot.capture().network();
        let mut distinct = HashSet::new();
        while let Some(address) = addresses.next().await {
            distinct.insert(indexed_address(&address?, network)?);
            if distinct.len() > u32_to_usize(MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES) {
                return Err(status_from_query_error(
                    &QueryError::TransparentBalanceAddressCountExceeded {
                        requested: distinct.len(),
                        maximum: MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES,
                    },
                ));
            }
        }
        let lookup = TransparentAddressLookup::new(network, distinct)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let balance = self
            .node()
            .await?
            .fetch_transparent_address_balance(&lookup)
            .await
            .map_err(node_status)?;
        Ok(lightwalletd::Balance {
            value_zat: i64::try_from(balance.balance_zat)
                .map_err(|_| Status::out_of_range("transparent address balance exceeds i64"))?,
        })
    }

    async fn utxos(
        &self,
        request: lightwalletd::GetAddressUtxosArg,
    ) -> Result<std::vec::IntoIter<SourceTransparentUnspentOutput>, Status> {
        if request.addresses.is_empty() {
            return Ok(Vec::new().into_iter());
        }
        let network = self.slot.capture().network();
        let addresses = request
            .addresses
            .iter()
            .map(|text| indexed_address(text, network))
            .collect::<Result<Vec<_>, _>>()?;
        let lookup = TransparentAddressLookup::new(network, addresses)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let height = BlockHeight::new(
            u32::try_from(request.start_height)
                .map_err(|_| Status::invalid_argument("startHeight exceeds u32"))?,
        );
        let node = self.node().await?;
        let mut rows = wallet_ordered_utxos(node, &lookup, height, request.max_entries).await?;
        rows.sort_unstable_by_key(|row| {
            (
                row.block_height,
                row.outpoint.transaction_id,
                row.outpoint.output_index,
            )
        });
        if request.max_entries != 0 {
            rows.truncate(u32_to_usize(request.max_entries));
        }
        Ok(rows.into_iter())
    }

    async fn history(
        &self,
        filter: lightwalletd::TransparentAddressBlockFilter,
    ) -> Result<GrpcStream<lightwalletd::RawTransaction>, Status> {
        let epoch = self.slot.capture().chain_epoch();
        let range = transparent_address_tx_history_request(
            &filter,
            epoch.network,
            epoch.visible_tip_height,
        )?;
        let lookup = TransparentAddressLookup::new(
            epoch.network,
            [indexed_address(&filter.address, epoch.network)?],
        )
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let node = self.node().await?;
        let ids = node
            .fetch_transparent_address_transaction_ids(
                &lookup,
                range.start_height,
                range.end_height,
            )
            .await
            .map_err(node_status)?;
        Ok(history_stream(
            node.clone(),
            ids,
            range.start_height,
            range.end_height,
        ))
    }
}

fn indexed_address(text: &str, network: Network) -> Result<Address, Status> {
    let address =
        parse_transparent_address(text).map_err(|e| Status::invalid_argument(e.to_string()))?;
    if !transparent_address_matches_network(address.network_kind(), network) {
        return Err(Status::invalid_argument(
            "transparent address network does not match server network",
        ));
    }
    Ok(address)
}

/// The node orders same-height rows by block position, the wallet by txid.
/// Read past the requested prefix until its last height is complete before sorting it.
async fn wallet_ordered_utxos(
    node: &ZebraJsonRpcSource,
    lookup: &TransparentAddressLookup,
    height: BlockHeight,
    max_entries: u32,
) -> Result<Vec<SourceTransparentUnspentOutput>, Status> {
    if let Some(last) = max_entries.checked_sub(1) {
        let mut prefix = max_entries.saturating_add(1);
        while let Some(limit) = TransparentAddressRowLimit::new(prefix) {
            let rows = node
                .fetch_transparent_address_utxo_prefix(lookup, height, limit)
                .await
                .map_err(node_status)?;
            if rows.len() < u32_to_usize(prefix)
                || rows[u32_to_usize(last)].block_height != rows[rows.len() - 1].block_height
            {
                return Ok(rows);
            }
            prefix = prefix.saturating_mul(2);
        }
    }
    node.fetch_transparent_address_utxos(lookup, height)
        .await
        .map_err(node_status)
}

fn utxo_reply(
    row: SourceTransparentUnspentOutput,
) -> Result<lightwalletd::GetAddressUtxosReply, Status> {
    Ok(lightwalletd::GetAddressUtxosReply {
        address: row.address.to_string(),
        txid: encode_internal_transaction_id(row.outpoint.transaction_id).to_vec(),
        index: i32::try_from(row.outpoint.output_index)
            .map_err(|_| Status::data_loss("transparent output index exceeds i32"))?,
        script: row.script_pub_key,
        value_zat: i64::try_from(row.value_zat)
            .map_err(|_| Status::data_loss("transparent output value exceeds i64"))?,
        height: u64::from(row.block_height.value()),
    })
}

fn history_stream(
    node: ZebraJsonRpcSource,
    ids: Vec<TransactionId>,
    start: BlockHeight,
    end: BlockHeight,
) -> GrpcStream<lightwalletd::RawTransaction> {
    Box::pin(stream::iter(ids).then(move |id| {
        let node = node.clone();
        async move { forwarded_transaction(&node, id, Some((start, end))).await }
    }))
}

async fn forwarded_transaction(
    node: &ZebraJsonRpcSource,
    id: TransactionId,
    range: Option<(BlockHeight, BlockHeight)>,
) -> Result<lightwalletd::RawTransaction, Status> {
    let missing = || {
        if range.is_some() {
            Status::unavailable(
                "node address history changed while reading transactions; retry with backoff",
            )
        } else {
            Status::not_found("transaction was not found")
        }
    };
    let bytes = node
        .fetch_raw_transaction_bytes(id)
        .await
        .map_err(node_status)?
        .ok_or_else(missing)?;
    #[allow(
        clippy::wildcard_enum_match_arm,
        reason = "Only a confirmed height or the mempool sentinel can be encoded."
    )]
    let height = match node
        .fetch_upstream_transaction_lookup(id)
        .await
        .map_err(node_status)?
    {
        UpstreamTransactionLookup::Mined { mined_height, .. }
            if range.is_none_or(|(start, end)| start <= mined_height && mined_height <= end) =>
        {
            mined_height.value()
        }
        UpstreamTransactionLookup::InMempool if range.is_none() => 0,
        _ => return Err(missing()),
    };
    Ok(lightwalletd::RawTransaction {
        data: bytes.as_slice().to_vec(),
        height: u64::from(height),
    })
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "Other node errors follow the shared query status policy."
)]
fn node_status(error: SourceError) -> Status {
    match error {
        SourceError::TransparentAddressIndexesUnavailable { .. } => {
            Status::failed_precondition(format!("transparent address index unavailable: {error}"))
        }
        SourceError::BlockCommittedDuringRead { .. }
        | SourceError::ChainTipChangedDuringFetch { .. } => {
            Status::unavailable(format!("{error}; retry with backoff"))
        }
        SourceError::SourceProtocolMismatch { .. }
        | SourceError::InvalidRawTransactionHex { .. } => Status::data_loss(error.to_string()),
        SourceError::TransparentAddressRowLimitExceeded { .. }
        | SourceError::NodeResponseTooLarge { .. }
        | SourceError::SourceResponseTooLarge { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        _ => status_from_query_error(&QueryError::Node(error)),
    }
}

fn storage_status(error: StorageReadError) -> Status {
    status_from_query_error(&QueryError::canonical_read(error))
}

impl SubtreeRootReads for QualificationWallet {
    async fn read_subtree_roots(
        &self,
        range: SubtreeRootRange,
        epoch: ChainEpochId,
    ) -> Result<SubtreeRoots, QueryError> {
        let reader = self.slot.capture();
        if reader.chain_epoch().id != epoch {
            return Err(QueryError::ChainEpochPinUnavailable {
                chain_epoch_id: epoch,
            });
        }
        QualificationWalletQuery::subtree_roots(&reader, range).await
    }
}

#[tonic::async_trait]
impl compact_tx_streamer_server::CompactTxStreamer
    for LightwalletdGrpcAdapter<QualificationWallet>
{
    async fn get_latest_block(
        &self,
        _request: Request<lightwalletd::ChainSpec>,
    ) -> Result<Response<lightwalletd::BlockId>, Status> {
        self.block_methods
            .latest_block(&self.query_api.slot)
            .await
            .map(Response::new)
    }

    async fn get_block(
        &self,
        request: Request<lightwalletd::BlockId>,
    ) -> Result<Response<lightwalletd::CompactBlock>, Status> {
        block_methods::compact_block(
            &self.query_api.slot,
            &request.into_inner(),
            CompactBlockShape::FULL_BLOCK,
        )
        .await
        .map(Response::new)
    }

    async fn get_block_nullifiers(
        &self,
        request: Request<lightwalletd::BlockId>,
    ) -> Result<Response<lightwalletd::CompactBlock>, Status> {
        block_methods::compact_block(
            &self.query_api.slot,
            &request.into_inner(),
            CompactBlockShape::BLOCK_NULLIFIERS,
        )
        .await
        .map(Response::new)
    }

    type GetBlockRangeStream = GrpcStream<lightwalletd::CompactBlock>;

    async fn get_block_range(
        &self,
        request: Request<lightwalletd::BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeStream>, Status> {
        let request = request.into_inner();
        self.block_methods
            .block_range(&self.query_api.slot, &request, CompactBlockShape::for_range)
            .await
            .map(Response::new)
    }

    type GetBlockRangeNullifiersStream = GrpcStream<lightwalletd::CompactBlock>;

    async fn get_block_range_nullifiers(
        &self,
        request: Request<lightwalletd::BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeNullifiersStream>, Status> {
        let request = request.into_inner();
        self.block_methods
            .block_range(
                &self.query_api.slot,
                &request,
                CompactBlockShape::for_nullifier_range,
            )
            .await
            .map(Response::new)
    }

    async fn get_transaction(
        &self,
        request: Request<lightwalletd::TxFilter>,
    ) -> Result<Response<lightwalletd::RawTransaction>, Status> {
        self.qualification_transaction(request.into_inner())
            .await
            .map(Response::new)
    }

    async fn send_transaction(
        &self,
        request: Request<lightwalletd::RawTransaction>,
    ) -> Result<Response<lightwalletd::SendResponse>, Status> {
        let reader = self.query_api.slot.capture();
        let outcome = self
            .query_api
            .query
            .broadcast(&reader, RawTransactionBytes::new(request.into_inner().data))
            .await
            .map_err(|e| status_from_query_error(&e))?;
        Ok(Response::new(send_response_from_broadcast_outcome(outcome)))
    }

    type GetTaddressTxidsStream = GrpcStream<lightwalletd::RawTransaction>;

    async fn get_taddress_txids(
        &self,
        request: Request<lightwalletd::TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTxidsStream>, Status> {
        self.get_taddress_transactions(request).await
    }

    type GetTaddressTransactionsStream = GrpcStream<lightwalletd::RawTransaction>;

    async fn get_taddress_transactions(
        &self,
        request: Request<lightwalletd::TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTransactionsStream>, Status> {
        self.query_api
            .history(request.into_inner())
            .await
            .map(Response::new)
    }

    async fn get_taddress_balance(
        &self,
        request: Request<lightwalletd::AddressList>,
    ) -> Result<Response<lightwalletd::Balance>, Status> {
        self.query_api
            .balance(stream::iter(
                request.into_inner().addresses.into_iter().map(Ok),
            ))
            .await
            .map(Response::new)
    }

    async fn get_taddress_balance_stream(
        &self,
        request: Request<tonic::Streaming<lightwalletd::Address>>,
    ) -> Result<Response<lightwalletd::Balance>, Status> {
        self.query_api
            .balance(
                request
                    .into_inner()
                    .map(|row| row.map(|address| address.address)),
            )
            .await
            .map(Response::new)
    }

    type GetMempoolTxStream = GrpcStream<lightwalletd::CompactTx>;

    async fn get_mempool_tx(
        &self,
        request: Request<lightwalletd::GetMempoolTxRequest>,
    ) -> Result<Response<Self::GetMempoolTxStream>, Status> {
        self.mempool_transactions(request).await
    }

    type GetMempoolStreamStream = GrpcStream<lightwalletd::RawTransaction>;

    async fn get_mempool_stream(
        &self,
        _request: Request<lightwalletd::Empty>,
    ) -> Result<Response<Self::GetMempoolStreamStream>, Status> {
        self.mempool_stream().await
    }

    async fn get_tree_state(
        &self,
        request: Request<lightwalletd::BlockId>,
    ) -> Result<Response<lightwalletd::TreeState>, Status> {
        let reader = self.query_api.slot.capture();
        let height = match requested_block(&request.into_inner(), decode_rpc_block_hash_bytes)? {
            RequestedBlock::Height(height) => height,
            RequestedBlock::Hash(hash) => match reader
                .block_hash_lookup(hash)
                .await
                .map_err(storage_status)?
            {
                BlockHashLookup::Resolved(id) => id.height,
                BlockHashLookup::NotIndexed | BlockHashLookup::NotInBestChain => {
                    return Err(tree_state_block_lookup_status(
                        &QueryError::BlockNotInBestChain,
                    ));
                }
            },
        };
        reject_future_tree_state_height(height, reader.chain_epoch().visible_tip_height)?;
        let state = self
            .query_api
            .query
            .tree_state(reader, Some(height))
            .await
            .map_err(|error| tree_state_status(&error))?;
        Ok(Response::new(lightwalletd_tree_state(&state)))
    }

    async fn get_latest_tree_state(
        &self,
        _request: Request<lightwalletd::Empty>,
    ) -> Result<Response<lightwalletd::TreeState>, Status> {
        let state = self
            .query_api
            .query
            .tree_state(self.query_api.slot.capture(), None)
            .await
            .map_err(|error| tree_state_status(&error))?;
        Ok(Response::new(lightwalletd_tree_state(&state)))
    }

    type GetSubtreeRootsStream = GrpcStream<lightwalletd::SubtreeRoot>;

    async fn get_subtree_roots(
        &self,
        request: Request<lightwalletd::GetSubtreeRootsArg>,
    ) -> Result<Response<Self::GetSubtreeRootsStream>, Status> {
        let epoch = self.query_api.slot.capture().chain_epoch().id;
        let request = request.into_inner();
        let source = SubtreeRootsStream::start(
            self.query_api.clone(),
            shielded_protocol_from_request(request.shielded_protocol)?,
            &request,
            self.response_limits.max_subtree_roots,
            epoch,
        )
        .await?;
        Ok(Response::new(spawn_response_stream(
            source,
            SUBTREE_ROOT_STREAM_CHANNEL_CAPACITY,
        )))
    }

    async fn get_address_utxos(
        &self,
        request: Request<lightwalletd::GetAddressUtxosArg>,
    ) -> Result<Response<lightwalletd::GetAddressUtxosReplyList>, Status> {
        let address_utxos = self
            .query_api
            .utxos(request.into_inner())
            .await?
            .map(utxo_reply)
            .collect::<Result<_, _>>()?;
        Ok(Response::new(lightwalletd::GetAddressUtxosReplyList {
            address_utxos,
        }))
    }

    type GetAddressUtxosStreamStream = GrpcStream<lightwalletd::GetAddressUtxosReply>;

    async fn get_address_utxos_stream(
        &self,
        request: Request<lightwalletd::GetAddressUtxosArg>,
    ) -> Result<Response<Self::GetAddressUtxosStreamStream>, Status> {
        let outputs = self.query_api.utxos(request.into_inner()).await?;
        Ok(Response::new(Box::pin(stream::iter(
            outputs.map(utxo_reply),
        ))))
    }

    async fn get_lightd_info(
        &self,
        _request: Request<lightwalletd::Empty>,
    ) -> Result<Response<lightwalletd::LightdInfo>, Status> {
        self.block_methods
            .lightd_info(&self.query_api.slot, true)
            .await
            .map(Response::new)
    }

    async fn get_standard_fee(
        &self,
        _request: Request<lightwalletd::Empty>,
    ) -> Result<Response<lightwalletd::StandardFee>, Status> {
        let node = self.query_api.node().await?;
        standard_fee_response(node.standard_fee().await.map_err(QueryError::Node))
    }

    async fn ping(
        &self,
        request: Request<lightwalletd::Duration>,
    ) -> Result<Response<lightwalletd::PingResponse>, Status> {
        self.ping
            .respond(request.into_inner())
            .await
            .map(Response::new)
    }
}
