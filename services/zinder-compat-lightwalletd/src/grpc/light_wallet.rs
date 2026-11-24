//! gRPC routes for canonical and node-backed light-wallet serving.

use std::{
    collections::HashSet,
    convert::Infallible,
    num::NonZeroU32,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
    vec::IntoIter,
};

use futures_util::stream::try_unfold;
use http_body::{Body as HttpBody, Frame, SizeHint};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    time::{Instant, timeout_at},
};
use tokio_stream::{self as stream, Stream, StreamExt as _};
use tonic::{Request, Response, Status, body::Body, server::NamedService, service::LayerExt as _};
use tower::{Service, util::MapResponseLayer};
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
    ArtifactKey, CanonicalReader, LightWalletNodeQuery, MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES,
    MinedTransactionDigest, QueryError, RocksDbCanonicalReader, StorageReadError, SubtreeRoots,
    VerifiedNodeTarget, parse_receivable_transparent_address, read_on_blocking_thread,
    status_from_query_error,
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

const FORWARDED_TRANSACTION_SLOTS: usize = 8;
const FORWARDED_HISTORY_SLOTS: usize = 2;
const FORWARDED_HISTORY_DEADLINE: Duration = Duration::from_secs(60);
const FORWARDED_UTXO_RESPONSE_SLOTS: usize = 2;

/// The canonical reader and node-backed methods for light-wallet serving.
#[derive(Clone)]
pub struct NodeBackedLightWallet {
    slot: CanonicalReaderSlot,
    query: LightWalletNodeQuery,
    utxo_slots: Arc<Semaphore>,
    utxo_response_slots: Arc<Semaphore>,
    history_slots: Arc<Semaphore>,
    transaction_slots: Arc<Semaphore>,
}

impl LightwalletdGrpcAdapter<NodeBackedLightWallet> {
    /// Serves local block data, retained transactions, and node-backed wallet lookups without a projector.
    #[must_use]
    pub fn for_light_wallet(
        slot: CanonicalReaderSlot,
        node: ZebraJsonRpcSource,
        replay_wait: Duration,
        replay_waiters_per_slot: NonZeroU32,
    ) -> Self {
        let readers = slot.clone();
        let query = LightWalletNodeQuery::new(
            move || readers.capture(),
            node,
            replay_wait,
            replay_waiters_per_slot,
        );
        Self::new(NodeBackedLightWallet {
            slot,
            query,
            utxo_slots: Arc::new(Semaphore::new(1)),
            utxo_response_slots: Arc::new(Semaphore::new(FORWARDED_UTXO_RESPONSE_SLOTS)),
            history_slots: Arc::new(Semaphore::new(FORWARDED_HISTORY_SLOTS)),
            transaction_slots: Arc::new(Semaphore::new(FORWARDED_TRANSACTION_SLOTS)),
        })
    }

    /// Wraps this adapter in a tonic service with request field bounds and
    /// body-owned admission for unary unspent-output replies.
    #[must_use]
    pub fn into_server(
        self,
    ) -> impl Service<
        http::Request<Body>,
        Response = http::Response<Body>,
        Error = Infallible,
        Future: Send + 'static,
    > + NamedService
    + Clone
    + Send
    + Sync
    + 'static {
        MapResponseLayer::new(reserve_utxo_response_body)
            .named_layer(self.into_field_guarded_server())
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

    async fn light_wallet_transaction(
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
            return self.forwarded_transaction(reader, id).await;
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

    /// Answers `GetTransaction` from the served chain's location and recorded
    /// digest, with only the bytes read from the node.
    async fn forwarded_transaction(
        &self,
        reader: Arc<RocksDbCanonicalReader>,
        id: TransactionId,
    ) -> Result<lightwalletd::RawTransaction, Status> {
        let permit = Arc::clone(&self.query_api.transaction_slots)
            .try_acquire_owned()
            .map_err(|_| {
                Status::resource_exhausted(
                    "forwarded transaction lookup limit reached; retry with backoff",
                )
            })?;
        let view = served_view(&reader)?;
        let span = tracing::Span::current();
        // The storage read owns admission even if the caller drops the
        // request. Only the caller can proceed from evidence to the node.
        let (evidence, _permit) = tokio::task::spawn_blocking(move || {
            let evidence = span
                .in_scope(|| read_on_blocking_thread(forwarded_transaction_evidence(&reader, id)));
            (evidence, permit)
        })
        .await
        .map_err(|error| {
            Status::unavailable(format!("transaction evidence read failed: {error}"))
        })?;
        let evidence = evidence?;
        let node = self.query_api.node(&view).await?;
        match evidence {
            ForwardedTransactionEvidence::Mined(mined) => {
                let reported = node
                    .fetch_mined_transaction_bytes(id)
                    .await
                    .map_err(node_status)?
                    .ok_or_else(|| {
                        outside_served_view(
                            "node does not mine the transaction in the served block",
                        )
                    })?;
                served_transaction(mined, reported)
            }
            // A forwarded lookup reports an unverified node the same way for
            // every transaction, including ones the served view does not mine.
            ForwardedTransactionEvidence::Missing(miss) => self.pending_transaction(id, miss).await,
        }
    }
}

enum ForwardedTransactionEvidence {
    Mined(MinedTransactionDigest),
    Missing(CanonicalTransactionMiss),
}

async fn forwarded_transaction_evidence(
    reader: &RocksDbCanonicalReader,
    id: TransactionId,
) -> Result<ForwardedTransactionEvidence, Status> {
    let miss = match reader.mined_transaction_location(id).await {
        Ok(Some(location)) => {
            let digest = MinedTransactionDigest::from_location(location)
                .map_err(|error| status_from_query_error(&error))?;
            return Ok(ForwardedTransactionEvidence::Mined(digest));
        }
        Ok(None) => CanonicalTransactionMiss::NotInChain {
            at_epoch_id: reader.chain_epoch().id,
        },
        Err(error @ StorageReadError::CanonicalHistoryUnavailable { .. }) => {
            CanonicalTransactionMiss::BelowHistoryFloor(storage_status(error))
        }
        Err(error) => return Err(storage_status(error)),
    };
    Ok(ForwardedTransactionEvidence::Missing(miss))
}

impl NodeBackedLightWallet {
    async fn node(&self, view: &ServedView) -> Result<&ZebraJsonRpcSource, Status> {
        self.query
            .verified_node(&view.target)
            .await
            .map_err(|error| status_from_query_error(&QueryError::Node(error)))
    }

    /// Admits one node UTXO lookup per instance, including those behind a balance.
    fn utxo_permit(&self) -> Result<OwnedSemaphorePermit, Status> {
        Arc::clone(&self.utxo_slots)
            .try_acquire_owned()
            .map_err(|_| {
                Status::resource_exhausted(
                    "a node UTXO response is in progress; retry with backoff",
                )
            })
    }

    async fn balance(
        &self,
        mut addresses: impl Stream<Item = Result<String, Status>> + Unpin,
    ) -> Result<lightwalletd::Balance, Status> {
        let network = self.slot.capture().network();
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
        let _permit = self.utxo_permit()?;
        let view = served_view(&self.slot.capture())?;
        let node = self.node(&view).await?;
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
    ) -> Result<Option<(Vec<SourceTransparentUnspentOutput>, OwnedSemaphorePermit)>, Status> {
        if request.addresses.is_empty() {
            return Ok(None);
        }
        let view = served_view(&self.slot.capture())?;
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
        let response_permit = Arc::clone(&self.utxo_response_slots)
            .try_acquire_owned()
            .map_err(|_| {
                Status::resource_exhausted("node UTXO response limit reached; retry with backoff")
            })?;
        let mut rows = {
            let _node_permit = self.utxo_permit()?;
            let node = self.node(&view).await?;
            wallet_ordered_utxos(node, &lookup, height, request.max_entries, view.tip).await?
        };
        let count = wallet_utxo_response_len(rows.len(), request.max_entries, response_limit)?;
        if count == 0 {
            return Ok(None);
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
        Ok(Some((rows, response_permit)))
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
                    "forwarded address history limit reached; retry with backoff",
                )
            })?;
        let view = served_view(&reader)?;
        drop(reader);
        let deadline = Instant::now() + FORWARDED_HISTORY_DEADLINE;
        let history = timeout_at(deadline, async {
            let node = self.node(&view).await?;
            let mut history = NodeAddressHistory {
                node: node.clone(),
                slot: self.slot.clone(),
                lookup,
                pending: vec![(range.start_height, range.end_height)],
                served: view.tip,
                page: IntoIter::default(),
                page_range: (range.start_height, range.end_height),
                last_position: None,
            };
            history.read_page().await?;
            Ok::<_, Status>(history)
        })
        .await
        .map_err(|_| history_deadline_exceeded())??;
        if history.page.as_slice().is_empty() && history.pending.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }
        Ok(history.into_stream(permit, deadline))
    }
}

/// The served tip a forwarded request answers for, captured once per request
/// with the node admission target of the same reader.
struct ServedView {
    network: Network,
    tip: BlockId,
    target: VerifiedNodeTarget,
}

fn served_view(reader: &RocksDbCanonicalReader) -> Result<ServedView, Status> {
    let epoch = reader.chain_epoch();
    Ok(ServedView {
        network: epoch.network,
        tip: BlockId::new(epoch.visible_tip_height, epoch.visible_tip_hash),
        target: VerifiedNodeTarget::of(reader)
            .map_err(|error| status_from_query_error(&QueryError::Node(error)))?,
    })
}

/// Keeps node bytes for a served transaction only when the node mines it in
/// the served block and they are the bytes ingest committed there.
fn served_transaction(
    mined: MinedTransactionDigest,
    (reported, bytes): (BlockId, Vec<u8>),
) -> Result<lightwalletd::RawTransaction, Status> {
    let location = mined.location;
    mined
        .verify_node_bytes(reported, &bytes)
        .map_err(|error| status_from_query_error(&error))?;
    Ok(lightwalletd::RawTransaction {
        data: bytes,
        height: u64::from(location.block_height.value()),
    })
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
    // Every reread answers at the served tip, so it must agree with every
    // earlier page: it repeats, in order, the longest run of cutoff-height
    // outputs seen so far, and names no output an earlier page placed below
    // the cutoff.
    let mut verified = rows.split_off(before_cutoff);
    let earlier: HashSet<_> = rows.iter().map(|row| row.outpoint).collect();
    let mut tail_limit = limit;
    loop {
        // The RPC has no end height. Later outputs can push a bounded read
        // over the node limit and leave the cutoff block's final order unknown.
        let unbounded = verified.len() == u32_to_usize(TransparentAddressRowLimit::MAX.get());
        let page = if unbounded {
            served_utxo_page(node, lookup, cutoff, None, served).await?
        } else {
            tail_limit = TransparentAddressRowLimit::new(tail_limit.get() * 2)
                .unwrap_or(TransparentAddressRowLimit::MAX);
            served_utxo_page(node, lookup, cutoff, Some(tail_limit), served).await?
        };
        let at_cutoff = page
            .outputs
            .partition_point(|row| row.block_height == cutoff);
        let agrees = page.outputs[..at_cutoff].starts_with(&verified)
            && !page
                .outputs
                .iter()
                .any(|row| earlier.contains(&row.outpoint));
        if !agrees {
            return Err(outside_served_view(
                "node UTXO pages at the served tip disagree",
            ));
        }
        let finished = unbounded || page.complete || at_cutoff < page.outputs.len();
        verified = page.outputs;
        verified.truncate(at_cutoff);
        if finished {
            break;
        }
    }
    rows.append(&mut verified);
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

/// A forwarded address-history stream bound to the served tip it started at.
///
/// Every page request proves the node holds that tip, and every row must be a
/// served transaction, in canonical position order, whose node bytes match the
/// digest ingest committed.
struct NodeAddressHistory {
    node: ZebraJsonRpcSource,
    slot: CanonicalReaderSlot,
    lookup: TransparentAddressLookup,
    pending: Vec<(BlockHeight, BlockHeight)>,
    served: BlockId,
    page: IntoIter<TransactionId>,
    page_range: (BlockHeight, BlockHeight),
    last_position: Option<(BlockHeight, u32)>,
}

#[derive(Clone)]
struct HistoryAdmission(Arc<parking_lot::Mutex<Option<Arc<OwnedSemaphorePermit>>>>);

impl HistoryAdmission {
    fn reserve_evidence(&self) -> Result<Arc<OwnedSemaphorePermit>, Status> {
        self.0
            .lock()
            .clone()
            .ok_or_else(|| Status::cancelled("forwarded address history cancelled"))
    }
}

impl Drop for HistoryAdmission {
    fn drop(&mut self) {
        self.0.lock().take();
    }
}

impl NodeAddressHistory {
    fn into_stream(
        mut self,
        permit: OwnedSemaphorePermit,
        deadline: Instant,
    ) -> GrpcStream<lightwalletd::RawTransaction> {
        let (demand, mut requests) = mpsc::channel(1);
        let (rows, received) = mpsc::channel(1);
        // Either owner can release stream admission. A blocking evidence
        // read keeps its own reference until the read actually finishes.
        let admission = HistoryAdmission(Arc::new(parking_lot::Mutex::new(Some(Arc::new(permit)))));
        let worker_admission = admission.clone();
        let task = tokio::spawn(async move {
            let admission = worker_admission;
            timeout_at(deadline, async {
                while requests.recv().await.is_some() {
                    let row = tokio::select! {
                        biased;
                        () = rows.closed() => return Ok::<_, Status>(()),
                        row = self.next(&admission) => row?,
                    };
                    let Some(row) = row else {
                        return Ok(());
                    };
                    if rows.send(row).await.is_err() {
                        return Ok(());
                    }
                }
                Ok(())
            })
            .await
            .map_err(|_| history_deadline_exceeded())?
        });
        Box::pin(try_unfold(
            (demand, received, task, admission),
            |(demand, mut received, task, admission)| async move {
                let _ = demand.send(()).await;
                if let Some(row) = received.recv().await {
                    return Ok(Some((row, (demand, received, task, admission))));
                }
                task.await
                    .map_err(|error| Status::internal(error.to_string()))??;
                Ok(None)
            },
        ))
    }

    async fn read_page(&mut self) -> Result<(), Status> {
        self.page = IntoIter::default();
        while let Some((start, end)) = self.pending.pop() {
            match self
                .node
                .fetch_transparent_address_transaction_ids(&self.lookup, start, end, self.served)
                .await
            {
                Ok(ids) => {
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

    async fn verify_served_tip(&self) -> Result<(), Status> {
        let hash = self
            .node
            .fetch_block_hash(self.served.height)
            .await
            .map_err(node_status)?;
        if hash == self.served.hash {
            Ok(())
        } else {
            Err(history_changed())
        }
    }

    /// Reads the canonical evidence without keeping the reader across node calls.
    async fn served_location(
        &self,
        id: TransactionId,
        admission: &HistoryAdmission,
    ) -> Result<MinedTransactionDigest, Status> {
        let permit = admission.reserve_evidence()?;
        let reader = self.slot.capture();
        let served = self.served;
        let span = tracing::Span::current();
        let task = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            span.in_scope(|| {
                read_on_blocking_thread(history_transaction_evidence(&reader, served, id))
            })
        });
        tracing::trace!(
            target: "zinder::compat_lightwalletd",
            "queued forwarded address history evidence"
        );
        task.await.map_err(|error| {
            Status::unavailable(format!("address history evidence read failed: {error}"))
        })?
    }

    async fn next(
        &mut self,
        admission: &HistoryAdmission,
    ) -> Result<Option<lightwalletd::RawTransaction>, Status> {
        loop {
            if let Some(id) = self.page.next() {
                let reported = self
                    .node
                    .fetch_mined_transaction_bytes(id)
                    .await
                    .map_err(node_status)?
                    .ok_or_else(history_changed)?;
                let (start, end) = self.page_range;
                if reported.0.height < start || reported.0.height > end {
                    return Err(history_changed());
                }
                let mined = self.served_location(id, admission).await?;
                let position = (
                    mined.location.block_height,
                    mined.location.tx_index_in_block,
                );
                let row = served_transaction(mined, reported)?;
                if self.last_position.is_some_and(|last| last >= position) {
                    return Err(Status::data_loss(
                        "node address history is out of canonical transaction order",
                    ));
                }
                self.verify_served_tip().await?;
                self.last_position = Some(position);
                self.page_range.0 = position.0;
                return Ok(Some(row));
            }
            if self.pending.is_empty() {
                self.verify_served_tip().await?;
                return Ok(None);
            }
            self.read_page().await?;
        }
    }
}

async fn history_transaction_evidence(
    reader: &RocksDbCanonicalReader,
    served: BlockId,
    id: TransactionId,
) -> Result<MinedTransactionDigest, Status> {
    let epoch = reader.chain_epoch();
    if BlockId::new(epoch.visible_tip_height, epoch.visible_tip_hash) != served
        && reader
            .block_hash_lookup(served.hash)
            .await
            .map_err(storage_status)?
            != BlockHashLookup::Resolved(served)
    {
        return Err(history_changed());
    }
    let location = reader
        .mined_transaction_location(id)
        .await
        .map_err(storage_status)?
        .ok_or_else(history_changed)?;
    MinedTransactionDigest::from_location(location).map_err(|error| status_from_query_error(&error))
}

fn history_deadline_exceeded() -> Status {
    Status::deadline_exceeded("forwarded address history deadline exceeded; retry with backoff")
}

fn history_changed() -> Status {
    Status::unavailable(
        "node address history changed while reading transactions; retry with backoff",
    )
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

impl SubtreeRootReads for NodeBackedLightWallet {
    async fn read_subtree_roots(
        &self,
        range: SubtreeRootRange,
        epoch: ChainEpoch,
    ) -> Result<SubtreeRoots, QueryError> {
        self.query.subtree_roots(epoch, range).await
    }
}

#[tonic::async_trait]
impl lightwalletd::EncodedBlockRange for LightwalletdGrpcAdapter<NodeBackedLightWallet> {
    async fn get_encoded_block_range(
        &self,
        request: Request<lightwalletd::BlockRange>,
    ) -> Result<Response<lightwalletd::EncodedBlockRangeStream>, Status> {
        let request = request.into_inner();
        self.block_methods
            .await_range_tip(&self.query_api.slot, "get_encoded_block_range", &request)
            .await?;
        let range = self.published_history.as_ref().map_or_else(
            || (None, self.live_tail.clone()),
            |history| history.encoded_range(),
        );
        self.block_methods
            .published_block_range(
                &self.query_api.slot,
                &request,
                range,
                CompactBlockShape::for_range,
            )
            .await
            .map(Response::new)
    }
}

#[tonic::async_trait]
impl lightwalletd::EncodedBlockRangeNullifiers for LightwalletdGrpcAdapter<NodeBackedLightWallet> {
    async fn get_encoded_block_range_nullifiers(
        &self,
        request: Request<lightwalletd::BlockRange>,
    ) -> Result<Response<lightwalletd::EncodedBlockRangeStream>, Status> {
        let request = request.into_inner();
        self.block_methods
            .await_range_tip(
                &self.query_api.slot,
                "get_encoded_block_range_nullifiers",
                &request,
            )
            .await?;
        let range = self.published_history.as_ref().map_or_else(
            || (None, self.live_tail.clone()),
            |history| history.encoded_range(),
        );
        self.block_methods
            .published_block_range(
                &self.query_api.slot,
                &request,
                range,
                CompactBlockShape::for_nullifier_range,
            )
            .await
            .map(Response::new)
    }
}

#[tonic::async_trait]
impl compact_tx_streamer_server::CompactTxStreamer
    for LightwalletdGrpcAdapter<NodeBackedLightWallet>
{
    fn encoded_block_range(&self) -> Option<&dyn lightwalletd::EncodedBlockRange> {
        Some(self)
    }

    fn encoded_block_range_nullifiers(
        &self,
    ) -> Option<&dyn lightwalletd::EncodedBlockRangeNullifiers> {
        Some(self)
    }

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
        let block_id = request.into_inner();
        self.block_methods
            .await_block_tip(&self.query_api.slot, "get_block", &block_id)
            .await?;
        block_methods::compact_block(
            &self.query_api.slot,
            &block_id,
            CompactBlockShape::FULL_BLOCK,
        )
        .await
        .map(Response::new)
    }

    async fn get_block_nullifiers(
        &self,
        request: Request<lightwalletd::BlockId>,
    ) -> Result<Response<lightwalletd::CompactBlock>, Status> {
        let block_id = request.into_inner();
        self.block_methods
            .await_block_tip(&self.query_api.slot, "get_block_nullifiers", &block_id)
            .await?;
        block_methods::compact_block(
            &self.query_api.slot,
            &block_id,
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
            .await_range_tip(&self.query_api.slot, "get_block_range", &request)
            .await?;
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
            .await_range_tip(&self.query_api.slot, "get_block_range_nullifiers", &request)
            .await?;
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
        self.light_wallet_transaction(request.into_inner())
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
        let block = requested_block(&request.into_inner(), decode_rpc_block_hash_bytes)?;
        if let RequestedBlock::Height(height) = block {
            self.block_methods
                .await_tip(&self.query_api.slot, "get_tree_state", height)
                .await?;
        }
        let reader = self.query_api.slot.capture();
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
        let outputs = self
            .query_api
            .utxos(request.into_inner(), self.response_limits.max_address_utxos)
            .await?;
        let Some((rows, permit)) = outputs else {
            return Ok(Response::new(
                lightwalletd::GetAddressUtxosReplyList::default(),
            ));
        };
        let address_utxos = rows.into_iter().map(utxo_reply).collect::<Result<_, _>>()?;
        let mut response = Response::new(lightwalletd::GetAddressUtxosReplyList { address_utxos });
        response.extensions_mut().insert(UtxoResponsePermit {
            _permit: Arc::new(permit),
        });
        Ok(response)
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
        let outputs: Self::GetAddressUtxosStreamStream = match outputs {
            None => Box::pin(stream::empty()),
            Some((rows, permit)) => Box::pin(try_unfold(
                (rows.into_iter(), permit),
                |(mut rows, permit)| async move {
                    let Some(row) = rows.next() else {
                        return Ok(None);
                    };
                    Ok(Some((utxo_reply(row)?, (rows, permit))))
                },
            )),
        };
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
        let view = served_view(&self.query_api.slot.capture())?;
        let node = self.query_api.node(&view).await?;
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

#[derive(Clone)]
struct UtxoResponsePermit {
    _permit: Arc<OwnedSemaphorePermit>,
}

fn reserve_utxo_response_body(mut response: http::Response<Body>) -> http::Response<Body> {
    let Some(permit) = response.extensions_mut().remove::<UtxoResponsePermit>() else {
        return response;
    };
    response.map(|body| {
        Body::new(ReservedUtxoBody {
            body,
            permit: Some(permit),
        })
    })
}

struct ReservedUtxoBody {
    body: Body,
    permit: Option<UtxoResponsePermit>,
}

impl HttpBody for ReservedUtxoBody {
    type Data = <Body as HttpBody>::Data;
    type Error = <Body as HttpBody>::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let frame = Pin::new(&mut self.body).poll_frame(cx);
        // The outer send-stall layer polls for trailers only after handing
        // all pieces of the previous data frame to the transport.
        if matches!(&frame, Poll::Ready(None))
            || matches!(&frame, Poll::Ready(Some(Ok(frame))) if frame.is_trailers())
        {
            self.permit.take();
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn a_send_stall_releases_unary_utxo_admission_while_a_piece_is_still_held()
    -> Result<(), Box<dyn std::error::Error>> {
        use http_body_util::{BodyExt as _, Full};
        use tower::{Layer as _, ServiceExt as _};
        use zinder_runtime::GrpcSendStallLayer;

        let slots = Arc::new(Semaphore::new(1));
        let permit = UtxoResponsePermit {
            _permit: Arc::new(Arc::clone(&slots).try_acquire_owned()?),
        };
        let data = vec![0_u8; 256 * 1024];
        let data_len = data.len();
        let mut response =
            http::Response::new(Body::new(Full::new(<Body as HttpBody>::Data::from(data))));
        response.extensions_mut().insert(permit);
        let mut response = Some(response);
        let service = tower::service_fn(move |_: http::Request<Body>| {
            std::future::ready(
                response
                    .take()
                    .map(reserve_utxo_response_body)
                    .ok_or("unexpected second call"),
            )
        });
        let mut body = GrpcSendStallLayer::new(Duration::from_secs(5))
            .layer(service)
            .oneshot(http::Request::new(Body::empty()))
            .await?
            .into_body();

        let piece = body.frame().await.ok_or("missing first piece")??;
        assert!(piece.data_ref().is_some_and(|data| data.len() < data_len));
        assert_eq!(slots.available_permits(), 0);
        let returned_permit =
            tokio::time::timeout(Duration::from_secs(10), Arc::clone(&slots).acquire_owned())
                .await??;

        let status = body.frame().await.ok_or("missing stall status")??;
        assert_eq!(
            status
                .trailers_ref()
                .and_then(Status::from_header_map)
                .map(|status| status.code()),
            Some(tonic::Code::DeadlineExceeded)
        );
        drop((returned_permit, body, piece));
        assert_eq!(slots.available_permits(), 1);
        Ok(())
    }

    #[test]
    fn light_wallet_utxo_limit_applies_to_the_requested_response() {
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
