//! gRPC routes for canonical and node-backed light-wallet qualification.

use std::{collections::HashSet, num::NonZeroU32, sync::Arc, time::Duration, vec::IntoIter};

use futures_util::{TryStreamExt as _, stream::try_unfold};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_stream::{self as stream, Stream, StreamExt as _};
use tonic::{Request, Response, Status};
use zebra_chain::transparent::Address;
use zinder_core::{
    BlockHeight, BlockId, ChainEpoch, Network, RawTransactionBytes, SubtreeRootRange,
    TransactionId,
    wire::{
        decode_internal_transaction_id, decode_rpc_block_hash_bytes, encode_internal_transaction_id,
    },
};
use zinder_proto::compat::lightwalletd::{self, compact_tx_streamer_server};
use zinder_query::{
    ArtifactKey, CanonicalReader, MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES,
    QualificationWalletQuery, QueryError, StorageReadError, SubtreeRoots,
    parse_receivable_transparent_address, status_from_query_error,
};
use zinder_source::{
    SourceError, SourceTransparentUnspentOutput, SourceTransparentUtxoPage,
    TransactionBroadcaster as _, TransparentAddressLookup, TransparentAddressRowLimit,
    ZebraJsonRpcSource, parse_transparent_address, transparent_address_matches_network,
};
use zinder_store::{ArtifactFamily, BlockHashLookup};

use super::{
    CanonicalTransactionMiss, GrpcStream, LightwalletdGrpcAdapter,
    SUBTREE_ROOT_STREAM_CHANNEL_CAPACITY, SubtreeRootReads, SubtreeRootsStream,
    address_utxo_limit_exceeded, lightwalletd_tree_state, reject_future_tree_state_height,
    reject_genesis_tree_state, require_sapling_tree_state, send_response_from_broadcast_outcome,
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
    utxo_slots: Arc<Semaphore>,
    history_slots: Arc<Semaphore>,
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
        let readers = slot.clone();
        let query = QualificationWalletQuery::new(
            move || readers.capture(),
            node,
            replay_wait,
            replay_waiters_per_slot,
        );
        Self::new(QualificationWallet {
            slot,
            query,
            utxo_slots: Arc::new(Semaphore::new(1)),
            history_slots: Arc::new(Semaphore::new(1)),
        })
    }

    /// Observes the node against the same canonical tip and schedule used for forwarding.
    #[must_use]
    pub fn spawn_node_readiness_probe(
        &self,
        readiness: zinder_query::WalletServingReadiness,
        interval: Duration,
        cancel: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        self.query_api
            .query
            .clone()
            .spawn_node_readiness_probe(readiness, interval, cancel)
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
        let forwarded = !reader.raw_blob_retention().retains_transaction_blobs();
        let miss = match reader.mined_transaction(id).await {
            Ok(Some(mined)) if forwarded => {
                let header = mined.block_header.ok_or_else(|| {
                    status_from_query_error(&QueryError::ArtifactUnavailable {
                        family: ArtifactFamily::BlockHeader,
                        key: ArtifactKey::BlockHeight(mined.location.block_height),
                    })
                })?;
                drop(reader);
                let node = self.query_api.node().await?;
                let block = BlockId::new(mined.location.block_height, header.block_hash);
                return forwarded_transaction(node, id, block).await;
            }
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
        if forwarded {
            // A forwarded lookup reports an unverified node the same way for
            // every transaction, including ones the served view does not mine.
            self.query_api.node().await?;
        }
        self.pending_transaction(id, miss).await
    }
}

impl QualificationWallet {
    async fn node(&self) -> Result<&ZebraJsonRpcSource, Status> {
        self.query
            .verified_node()
            .await
            .map_err(|error| status_from_query_error(&QueryError::Node(error)))
    }

    async fn balance(
        &self,
        mut addresses: impl Stream<Item = Result<String, Status>> + Unpin,
    ) -> Result<lightwalletd::Balance, Status> {
        let view = served_view(self.slot.capture().chain_epoch());
        let network = view.network;
        let mut distinct = HashSet::new();
        while let Some(address) = addresses.next().await {
            let parsed = parse_receivable_transparent_address(&address?, network)
                .map_err(|error| status_from_query_error(&error))?;
            distinct.insert(parsed);
            if distinct.len() > u32_to_usize(MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES) {
                return Err(status_from_query_error(
                    &QueryError::TransparentBalanceAddressCountExceeded {
                        requested: distinct.len(),
                        maximum: MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES,
                    },
                ));
            }
        }
        if distinct.is_empty() {
            return Err(Status::invalid_argument(super::EMPTY_BALANCE_ADDRESSES));
        }
        let lookup = TransparentAddressLookup::new(network, distinct)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        // The node's balance RPC does not report the tip it answered at, so
        // the balance is summed from outputs the node attests at the served tip.
        let node = self.node().await?;
        let unspent = served_utxo_page(node, &lookup, BlockHeight::new(0), None, view.tip).await?;
        let exceeds = || Status::out_of_range("transparent address balance exceeds i64");
        let balance_zat = unspent
            .outputs
            .iter()
            .try_fold(0_u64, |sum, output| sum.checked_add(output.value_zat))
            .ok_or_else(exceeds)?;
        Ok(lightwalletd::Balance {
            value_zat: i64::try_from(balance_zat).map_err(|_| exceeds())?,
        })
    }

    async fn utxos(
        &self,
        request: lightwalletd::GetAddressUtxosArg,
        response_limit: NonZeroU32,
    ) -> Result<GrpcStream<lightwalletd::GetAddressUtxosReply>, Status> {
        if request.addresses.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }
        let view = served_view(self.slot.capture().chain_epoch());
        let network = view.network;
        let mut addresses = Vec::with_capacity(request.addresses.len());
        for text in request.addresses {
            let parsed = indexed_address(&text, network)?;
            if matches!(parsed, Address::Tex { .. }) {
                return Err(Status::invalid_argument(super::UNRECEIVABLE_ADDRESS));
            }
            addresses.push(parsed);
        }
        let lookup = TransparentAddressLookup::new(network, addresses)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let height = BlockHeight::new(
            u32::try_from(request.start_height)
                .map_err(|_| Status::invalid_argument("startHeight exceeds u32"))?,
        );
        let permit = Arc::clone(&self.utxo_slots)
            .try_acquire_owned()
            .map_err(|_| {
                Status::resource_exhausted(
                    "a node UTXO response is in progress; retry with backoff",
                )
            })?;
        let node = self.node().await?;
        let mut rows =
            wallet_ordered_utxos(node, &lookup, height, request.max_entries, view.tip).await?;
        let count = wallet_utxo_response_len(rows.len(), request.max_entries, response_limit)?;
        if count == 0 {
            return Ok(Box::pin(stream::empty()));
        }
        rows.sort_unstable_by_key(|row| {
            (
                row.block_height,
                row.outpoint.transaction_id,
                row.outpoint.output_index,
            )
        });
        rows.truncate(count);
        rows.shrink_to_fit();
        Ok(Box::pin(try_unfold(
            (rows.into_iter(), permit),
            |(mut rows, permit)| async move {
                let Some(row) = rows.next() else {
                    return Ok(None);
                };
                Ok(Some((utxo_reply(row)?, (rows, permit))))
            },
        )))
    }

    async fn history(
        &self,
        filter: lightwalletd::TransparentAddressBlockFilter,
    ) -> Result<GrpcStream<lightwalletd::RawTransaction>, Status> {
        let reader = self.slot.capture();
        let epoch = reader.chain_epoch();
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
        let permit = Arc::clone(&self.history_slots)
            .try_acquire_owned()
            .map_err(|_| {
                Status::resource_exhausted(
                    "a node address history response is in progress; retry with backoff",
                )
            })?;
        let end_hash = reader
            .block_header_at(range.end_height)
            .await
            .map_err(storage_status)?
            .ok_or_else(|| {
                status_from_query_error(&QueryError::ArtifactUnavailable {
                    family: ArtifactFamily::BlockHeader,
                    key: ArtifactKey::BlockHeight(range.end_height),
                })
            })?
            .block_hash;
        drop(reader);
        let node = self.node().await?;
        NodeAddressHistory {
            node: node.clone(),
            lookup,
            pending: vec![(range.start_height, range.end_height)],
            anchor: BlockId::new(range.end_height, end_hash),
            page: IntoIter::default(),
            page_range: (range.start_height, range.end_height),
            _permit: permit,
        }
        .into_stream()
        .await
    }
}

/// The served tip a forwarded request answers for, captured once per request.
struct ServedView {
    network: Network,
    tip: BlockId,
}

fn served_view(epoch: ChainEpoch) -> ServedView {
    ServedView {
        network: epoch.network,
        tip: BlockId::new(epoch.visible_tip_height, epoch.visible_tip_hash),
    }
}

fn wallet_utxo_response_len(
    available: usize,
    requested: u32,
    limit: NonZeroU32,
) -> Result<usize, Status> {
    let count =
        NonZeroU32::new(requested).map_or(available, |max| available.min(u32_to_usize(max.get())));
    let limit = u32_to_usize(limit.get());
    if count > limit {
        return Err(address_utxo_limit_exceeded(limit));
    }
    Ok(count)
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

/// Reads one `getaddressutxos` page and keeps it only when the node reports
/// answering at the served tip.
///
/// A complete page is read when `limit` is unset.
async fn served_utxo_page(
    node: &ZebraJsonRpcSource,
    lookup: &TransparentAddressLookup,
    height: BlockHeight,
    limit: Option<TransparentAddressRowLimit>,
    served: BlockId,
) -> Result<SourceTransparentUtxoPage, Status> {
    let page = match limit {
        None => node.fetch_transparent_address_utxos(lookup, height).await,
        Some(limit) => {
            node.fetch_transparent_address_utxo_page(lookup, height, limit)
                .await
        }
    }
    .map_err(node_status)?;
    if page.tip == served {
        Ok(page)
    } else {
        Err(outside_served_view(
            "node answered at another tip than the served tip",
        ))
    }
}

/// The node orders same-height outputs by block position, the wallet by txid.
///
/// Complete the last height before sorting a bounded prefix. A second lookup
/// starts at that height so earlier outputs cannot exhaust its row budget.
async fn wallet_ordered_utxos(
    node: &ZebraJsonRpcSource,
    lookup: &TransparentAddressLookup,
    height: BlockHeight,
    max_entries: u32,
    served: BlockId,
) -> Result<Vec<SourceTransparentUnspentOutput>, Status> {
    let Some(requested) = TransparentAddressRowLimit::new(max_entries) else {
        return Ok(served_utxo_page(node, lookup, height, None, served)
            .await?
            .outputs);
    };
    let limit = TransparentAddressRowLimit::new(requested.get() + 1)
        .unwrap_or(TransparentAddressRowLimit::MAX);
    let page = served_utxo_page(node, lookup, height, Some(limit), served).await?;
    let mut rows = page.outputs;
    if rows.len() < u32_to_usize(requested.get()) {
        return Ok(rows);
    }
    let cutoff = rows[u32_to_usize(requested.get() - 1)].block_height;
    let through_cutoff = rows.partition_point(|row| row.block_height <= cutoff);
    if page.complete || through_cutoff < rows.len() {
        rows.truncate(through_cutoff);
        return Ok(rows);
    }
    let before_cutoff = rows.partition_point(|row| row.block_height < cutoff);
    let mut at_cutoff = rows.len() - before_cutoff;
    rows.truncate(before_cutoff);
    let mut tail_limit = limit;
    let mut tail = loop {
        if at_cutoff == u32_to_usize(TransparentAddressRowLimit::MAX.get()) {
            // The RPC has no end height. Later outputs can push this read over
            // the node limit and leave the cutoff block's final order unknown.
            break served_utxo_page(node, lookup, cutoff, None, served)
                .await?
                .outputs;
        }
        tail_limit = TransparentAddressRowLimit::new(tail_limit.get() * 2)
            .unwrap_or(TransparentAddressRowLimit::MAX);
        let page = served_utxo_page(node, lookup, cutoff, Some(tail_limit), served).await?;
        at_cutoff = page
            .outputs
            .partition_point(|row| row.block_height == cutoff);
        if page.complete || at_cutoff < page.outputs.len() {
            break page.outputs;
        }
    };
    tail.truncate(tail.partition_point(|row| row.block_height == cutoff));
    rows.append(&mut tail);
    Ok(rows)
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

struct NodeAddressHistory {
    node: ZebraJsonRpcSource,
    lookup: TransparentAddressLookup,
    pending: Vec<(BlockHeight, BlockHeight)>,
    anchor: BlockId,
    page: IntoIter<TransactionId>,
    page_range: (BlockHeight, BlockHeight),
    _permit: OwnedSemaphorePermit,
}

impl NodeAddressHistory {
    async fn into_stream(mut self) -> Result<GrpcStream<lightwalletd::RawTransaction>, Status> {
        self.read_page().await?;
        if self.page.as_slice().is_empty() && self.pending.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }
        Ok(Box::pin(try_unfold(self, Self::next)))
    }

    async fn read_page(&mut self) -> Result<(), Status> {
        self.page = IntoIter::default();
        while let Some((start, end)) = self.pending.pop() {
            match self
                .node
                .fetch_transparent_address_transaction_ids(&self.lookup, start, end)
                .await
            {
                Ok(ids) => {
                    self.verify_anchor().await?;
                    if ids.iter().collect::<HashSet<_>>().len() != ids.len() {
                        return Err(Status::data_loss(
                            "node address history repeated a transaction ID",
                        ));
                    }
                    self.page = ids.into_iter();
                    self.page_range = (start, end);
                    break;
                }
                Err(
                    SourceError::TransparentAddressRowLimitExceeded { .. }
                    | SourceError::NodeResponseTooLarge { .. }
                    | SourceError::SourceResponseTooLarge { .. },
                ) if start < end => {
                    let mid = start.value().midpoint(end.value());
                    self.pending.push((BlockHeight::new(mid + 1), end));
                    self.pending.push((start, BlockHeight::new(mid)));
                }
                Err(error) => return Err(node_status(error)),
            }
        }
        Ok(())
    }

    async fn verify_anchor(&self) -> Result<(), Status> {
        let hash = self
            .node
            .fetch_block_hash(self.anchor.height)
            .await
            .map_err(node_status)?;
        if hash == self.anchor.hash {
            Ok(())
        } else {
            Err(history_changed())
        }
    }

    async fn next(mut self) -> Result<Option<(lightwalletd::RawTransaction, Self)>, Status> {
        loop {
            if let Some(id) = self.page.next() {
                let (block, bytes) = self
                    .node
                    .fetch_mined_transaction_bytes(id)
                    .await
                    .map_err(node_status)?
                    .ok_or_else(history_changed)?;
                let (start, end) = self.page_range;
                if block.height < start || block.height > end {
                    return Err(history_changed());
                }
                self.verify_anchor().await?;
                self.page_range.0 = block.height;
                return Ok(Some((
                    lightwalletd::RawTransaction {
                        data: bytes,
                        height: u64::from(block.height.value()),
                    },
                    self,
                )));
            }
            if self.pending.is_empty() {
                self.verify_anchor().await?;
                return Ok(None);
            }
            self.read_page().await?;
        }
    }
}

fn history_changed() -> Status {
    Status::unavailable(
        "node address history changed while reading transactions; retry with backoff",
    )
}

/// Reads the bytes of a transaction the served view mines in `block`.
///
/// The node reply has no block position, so the transaction ID and block
/// identity bind it to the served location.
async fn forwarded_transaction(
    node: &ZebraJsonRpcSource,
    id: TransactionId,
    block: BlockId,
) -> Result<lightwalletd::RawTransaction, Status> {
    match node
        .fetch_mined_transaction_bytes(id)
        .await
        .map_err(node_status)?
    {
        Some((mined, bytes)) if mined == block => Ok(lightwalletd::RawTransaction {
            data: bytes,
            height: u64::from(block.height.value()),
        }),
        Some(_) | None => Err(outside_served_view(
            "node does not mine the transaction in the served block",
        )),
    }
}

/// Refuses a forwarded answer the node cannot give for the served chain view.
fn outside_served_view(reason: &str) -> Status {
    status_from_query_error(&QueryError::Node(SourceError::NodeUnavailable {
        reason: format!("{reason}; retry with backoff"),
    }))
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
        epoch: ChainEpoch,
    ) -> Result<SubtreeRoots, QueryError> {
        self.query.subtree_roots(epoch, range).await
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
        let outcome = self
            .query_api
            .query
            .broadcast(RawTransactionBytes::new(request.into_inner().data))
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
        let block = requested_block(&request.into_inner(), decode_rpc_block_hash_bytes)?;
        reject_genesis_tree_state(block, reader.network())?;
        let height = match block {
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
        require_sapling_tree_state(&state)?;
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
        require_sapling_tree_state(&state)?;
        Ok(Response::new(lightwalletd_tree_state(&state)))
    }

    type GetSubtreeRootsStream = GrpcStream<lightwalletd::SubtreeRoot>;

    async fn get_subtree_roots(
        &self,
        request: Request<lightwalletd::GetSubtreeRootsArg>,
    ) -> Result<Response<Self::GetSubtreeRootsStream>, Status> {
        let epoch = self.query_api.slot.capture().chain_epoch();
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
            .utxos(request.into_inner(), self.response_limits.max_address_utxos)
            .await?
            .try_collect()
            .await?;
        Ok(Response::new(lightwalletd::GetAddressUtxosReplyList {
            address_utxos,
        }))
    }

    type GetAddressUtxosStreamStream = GrpcStream<lightwalletd::GetAddressUtxosReply>;

    async fn get_address_utxos_stream(
        &self,
        request: Request<lightwalletd::GetAddressUtxosArg>,
    ) -> Result<Response<Self::GetAddressUtxosStreamStream>, Status> {
        let outputs = self
            .query_api
            .utxos(request.into_inner(), self.response_limits.max_address_utxos)
            .await?;
        Ok(Response::new(outputs))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualification_utxo_limit_applies_to_the_requested_response() {
        let limit = NonZeroU32::MIN.saturating_add(9);
        for (available, requested, expected) in [(20, 4, 4), (20, 10, 10), (10, 0, 10), (7, 11, 7)]
        {
            assert_eq!(
                wallet_utxo_response_len(available, requested, limit).ok(),
                Some(expected)
            );
        }
        for requested in [0, 11] {
            assert!(
                matches!(wallet_utxo_response_len(20, requested, limit), Err(status) if status.code() == tonic::Code::ResourceExhausted)
            );
        }
    }
}
