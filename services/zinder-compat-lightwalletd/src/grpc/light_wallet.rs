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
    sync::mpsc,
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
use zinder_proto::{
    compat::lightwalletd::{self, compact_tx_streamer_server},
    status_for_reason,
    v1::ops::ErrorReason,
};
use zinder_query::{
    ArtifactKey, CanonicalReader, FORWARDED_HISTORY_DEADLINE, ForwardedReadKind,
    ForwardedReadLimit, ForwardedReadRefused, ForwardedResponseKind, ForwardedResponsePermit,
    ForwardedTransactionError, LightWalletNodeQuery, MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES,
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
    address_utxo_limit_exceeded, lightwalletd_tree_state, lightwalletd_tree_state_from_hex,
    reject_future_tree_state_height, reject_genesis_tree_state, require_sapling_tree_state,
    send_response_from_broadcast_outcome, shielded_protocol_from_request, spawn_response_stream,
    standard_fee_response, transparent_address_tx_history_request, tree_state_block_lookup_status,
    tree_state_status, u32_to_usize, wire_decode_error_to_status,
};
use crate::CanonicalReaderSlot;
use crate::block_methods::{self, RequestedBlock, requested_block};
use crate::compact_block_messages::CompactBlockShape;
use crate::tip_wait::{TipWait, TipWaitProgress};

/// The canonical reader and node-backed methods for light-wallet serving.
#[derive(Clone)]
pub struct NodeBackedLightWallet {
    slot: CanonicalReaderSlot,
    query: LightWalletNodeQuery,
    reads: ForwardedReadLimit,
}

impl LightwalletdGrpcAdapter<NodeBackedLightWallet> {
    /// Serves local block data, retained transactions, and node-backed wallet
    /// lookups without a projector.
    ///
    /// Reads forwarded to `node` wait for slots of `reads` and share its
    /// verified transaction byte cache.
    #[must_use]
    pub fn for_light_wallet(
        slot: CanonicalReaderSlot,
        node: ZebraJsonRpcSource,
        replay_wait: Duration,
        replay_waiters_per_slot: NonZeroU32,
        reads: ForwardedReadLimit,
    ) -> Self {
        let readers = slot.clone();
        let current_check = slot.clone();
        let query = LightWalletNodeQuery::new(
            move || readers.capture(),
            node,
            replay_wait,
            replay_waiters_per_slot,
        )
        .with_current_reader_check(move |reader| current_check.is_current(reader));
        Self::new(NodeBackedLightWallet { slot, query, reads })
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
    ///
    /// The node reports the block that mines the transaction and its bytes
    /// must match the recorded digest, so a mined transaction needs no
    /// separate served-tip check. A canonical miss checks the tip before the
    /// mempool answers.
    async fn forwarded_transaction(
        &self,
        reader: Arc<RocksDbCanonicalReader>,
        id: TransactionId,
    ) -> Result<lightwalletd::RawTransaction, Status> {
        let view = served_view(&reader)?;
        let span = tracing::Span::current();
        let evidence = tokio::task::spawn_blocking(move || {
            span.in_scope(|| read_on_blocking_thread(forwarded_transaction_evidence(&reader, id)))
        })
        .await
        .map_err(|error| {
            Status::unavailable(format!("transaction evidence read failed: {error}"))
        })??;
        match evidence {
            ForwardedTransactionEvidence::Mined(mined) => {
                let reads = &self.query_api.reads;
                let height = u64::from(mined.location.block_height.value());
                let bytes = if let Some(bytes) = reads.cached_transaction(&mined) {
                    bytes
                } else {
                    let deadline = reads.deadline();
                    let node = self.query_api.schedule_node(&view).await?;
                    reads
                        .mined_transaction_bytes(
                            node,
                            mined,
                            ForwardedReadKind::Transaction,
                            deadline,
                        )
                        .await
                        .map_err(forwarded_transaction_status)?
                };
                Ok(lightwalletd::RawTransaction {
                    data: bytes.to_vec(),
                    height,
                })
            }
            // A forwarded lookup reports an unverified node the same way for
            // every transaction, including ones the served view does not mine.
            ForwardedTransactionEvidence::Missing(miss) => {
                let reads = &self.query_api.reads;
                let deadline = reads.deadline();
                self.query_api
                    .query
                    .verify_admitted_node(
                        &view.target,
                        reads,
                        ForwardedReadKind::Transaction,
                        deadline,
                    )
                    .await
                    .map_err(|error| super::forwarded_read_status(&error))?;
                self.pending_transaction(id, miss).await
            }
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
    /// Verifies the node's schedule and that it holds the served tip.
    async fn node(&self, view: &ServedView) -> Result<&ZebraJsonRpcSource, Status> {
        self.query
            .verified_node(&view.target)
            .await
            .map_err(|error| status_from_query_error(&QueryError::Node(error)))
    }

    /// Verifies only the node's schedule, for a read whose reply binds
    /// itself to the served tip.
    async fn schedule_node(&self, view: &ServedView) -> Result<&ZebraJsonRpcSource, Status> {
        self.query
            .schedule_verified_node(&view.target)
            .await
            .map_err(|error| status_from_query_error(&QueryError::Node(error)))
    }

    async fn balance(
        &self,
        mut addresses: impl Stream<Item = Result<String, Status>> + Unpin,
        method: &'static str,
        tip_wait: &TipWait,
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
        let deadline = self.reads.deadline();
        let unspent = self
            .run_with_tip_wait(method, tip_wait, deadline, || async {
                let view = served_view(&self.slot.capture()).map_err(UtxoAttemptError::Status)?;
                let reads = NodeReads {
                    node: self
                        .schedule_node(&view)
                        .await
                        .map_err(UtxoAttemptError::Status)?,
                    limit: &self.reads,
                    kind: ForwardedReadKind::Utxo,
                    deadline,
                };
                served_utxo_page(&reads, &lookup, BlockHeight::new(0), None, view.tip).await
            })
            .await?;
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
        method: &'static str,
        tip_wait: &TipWait,
    ) -> Result<Option<(Vec<SourceTransparentUnspentOutput>, ForwardedResponsePermit)>, Status>
    {
        if request.addresses.is_empty() {
            return Ok(None);
        }
        let network = self.slot.capture().network();
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
        // The response slot is taken before any node-call slot, so this
        // request never holds a node-call slot while it waits, and it is
        // dropped before TipWait waits on a mismatch.
        let deadline = self.reads.deadline();
        let (mut rows, response_permit) = self
            .run_with_tip_wait(method, tip_wait, deadline, || async {
                let response_permit = self
                    .reads
                    .admit_response(ForwardedResponseKind::UtxoResponse, deadline)
                    .await
                    .map_err(forwarded_read_refused)
                    .map_err(UtxoAttemptError::Status)?;
                let view = served_view(&self.slot.capture()).map_err(UtxoAttemptError::Status)?;
                let reads = NodeReads {
                    node: self
                        .schedule_node(&view)
                        .await
                        .map_err(UtxoAttemptError::Status)?,
                    limit: &self.reads,
                    kind: ForwardedReadKind::Utxo,
                    deadline,
                };
                let rows =
                    wallet_ordered_utxos(&reads, &lookup, height, request.max_entries, view.tip)
                        .await?;
                Ok((rows, response_permit))
            })
            .await?;
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

    async fn run_with_tip_wait<T, Attempt, Fut>(
        &self,
        method: &'static str,
        tip_wait: &TipWait,
        deadline: Instant,
        mut attempt: Attempt,
    ) -> Result<T, Status>
    where
        Attempt: FnMut() -> Fut,
        Fut: Future<Output = Result<T, UtxoAttemptError>> + Send,
    {
        match attempt().await {
            Ok(answer) => Ok(answer),
            Err(UtxoAttemptError::TipMismatch {
                served_tip,
                answered_tip,
            }) if answered_tip.height > served_tip.height => {
                let progress = tip_wait
                    .await_height_until(&self.slot, method, answered_tip.height, Some(deadline))
                    .await?;
                if progress != TipWaitProgress::Reached {
                    return Err(served_tip_mismatch(TIP_MISMATCH_MESSAGE));
                }
                attempt().await.map_err(UtxoAttemptError::into_status)
            }
            Err(error) => Err(error.into_status()),
        }
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
        let view = served_view(&reader)?;
        drop(reader);
        let deadline = Instant::now() + FORWARDED_HISTORY_DEADLINE;
        let permit = self
            .reads
            .admit_response(
                ForwardedResponseKind::HistoryStream,
                self.reads.deadline().min(deadline),
            )
            .await
            .map_err(forwarded_read_refused)?;
        let history = timeout_at(deadline, async {
            // Every page is bracketed by its own tip checks, so the stream
            // starts without a separate served-tip check.
            let node = self.schedule_node(&view).await?;
            let mut history = NodeAddressHistory {
                node: node.clone(),
                query: self.query.clone(),
                reads: self.reads.clone(),
                slot: self.slot.clone(),
                lookup,
                pending: vec![(range.start_height, range.end_height)],
                served: view.tip,
                page: IntoIter::default(),
                page_range: (range.start_height, range.end_height),
                last_position: None,
                deadline,
            };
            history.read_page().await?;
            Ok::<_, Status>(history)
        })
        .await
        .map_err(|_| history_deadline_exceeded())??;
        if history.page.as_slice().is_empty() && history.pending.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }
        Ok(history.into_stream(permit))
    }
}

/// The served tip a forwarded request answers for, captured once per request
/// with the node admission target of the same reader.
struct ServedView {
    tip: BlockId,
    target: VerifiedNodeTarget,
}

fn served_view(reader: &RocksDbCanonicalReader) -> Result<ServedView, Status> {
    let epoch = reader.chain_epoch();
    Ok(ServedView {
        tip: BlockId::new(epoch.visible_tip_height, epoch.visible_tip_hash),
        target: VerifiedNodeTarget::of(reader)
            .map_err(|error| status_from_query_error(&QueryError::Node(error)))?,
    })
}

/// One request's node calls of a single kind, each run under its own
/// node-call slot so no slot is held between calls.
struct NodeReads<'a> {
    node: &'a ZebraJsonRpcSource,
    limit: &'a ForwardedReadLimit,
    kind: ForwardedReadKind,
    deadline: Instant,
}

impl NodeReads<'_> {
    async fn call<T>(
        &self,
        call: impl Future<Output = Result<T, SourceError>>,
    ) -> Result<T, Status> {
        self.limit
            .node_call(self.kind, self.deadline, call)
            .await
            .map_err(forwarded_read_refused)?
            .map_err(node_status)
    }
}

/// Reports a forwarded read refused for want of a slot as `UNAVAILABLE`.
///
/// A full block-range or tree-state rebuild limit returns the same code, so a
/// wallet retries every admission limit the same way. `RESOURCE_EXHAUSTED`
/// stays reserved for deterministic size and row caps.
fn forwarded_read_refused(refused: ForwardedReadRefused) -> Status {
    Status::unavailable(refused.to_string())
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "Other failures follow the shared query status policy."
)]
fn forwarded_query_status(error: QueryError) -> Status {
    match error {
        QueryError::ForwardedReadLimitReached { .. } => Status::unavailable(error.to_string()),
        QueryError::Node(error) => node_status(error),
        error => status_from_query_error(&error),
    }
}

fn forwarded_transaction_status(error: ForwardedTransactionError) -> Status {
    forwarded_query_status(QueryError::from(error))
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
    reads: &NodeReads<'_>,
    lookup: &TransparentAddressLookup,
    height: BlockHeight,
    limit: Option<TransparentAddressRowLimit>,
    served: BlockId,
) -> Result<SourceTransparentUtxoPage, UtxoAttemptError> {
    let node = reads.node;
    let page = match limit {
        None => {
            reads
                .call(node.fetch_transparent_address_utxos(lookup, height))
                .await
        }
        Some(limit) => {
            reads
                .call(node.fetch_transparent_address_utxo_page(lookup, height, limit))
                .await
        }
    }
    .map_err(UtxoAttemptError::Status)?;
    if page.tip == served {
        Ok(page)
    } else {
        Err(UtxoAttemptError::TipMismatch {
            served_tip: served,
            answered_tip: page.tip,
        })
    }
}

/// The node orders same-height outputs by block position, the wallet by txid.
///
/// Complete the last height before sorting a bounded prefix. A second lookup
/// starts at that height so earlier outputs cannot exhaust its row budget.
async fn wallet_ordered_utxos(
    node: &NodeReads<'_>,
    lookup: &TransparentAddressLookup,
    height: BlockHeight,
    max_entries: u32,
    served: BlockId,
) -> Result<Vec<SourceTransparentUnspentOutput>, UtxoAttemptError> {
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
            return Err(UtxoAttemptError::PagesDisagreed);
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
    query: LightWalletNodeQuery,
    reads: ForwardedReadLimit,
    slot: CanonicalReaderSlot,
    lookup: TransparentAddressLookup,
    pending: Vec<(BlockHeight, BlockHeight)>,
    served: BlockId,
    page: IntoIter<TransactionId>,
    page_range: (BlockHeight, BlockHeight),
    last_position: Option<(BlockHeight, u32)>,
    /// When the whole request, stream included, must end.
    deadline: Instant,
}

/// The stream slot of a forwarded address history, which the response stream
/// and its worker share.
///
/// Dropping either owner releases the slot unless the worker is in the
/// middle of an iteration or a blocking evidence read, which each keep their
/// own reference until they end.
#[derive(Clone)]
struct HistoryAdmission(Arc<parking_lot::Mutex<Option<Arc<ForwardedResponsePermit>>>>);

impl HistoryAdmission {
    fn new(permit: ForwardedResponsePermit) -> Self {
        Self(Arc::new(parking_lot::Mutex::new(Some(Arc::new(permit)))))
    }

    /// Returns a reference to the slot for work that must keep it until the
    /// work ends, or `CANCELLED` once the stream has been dropped.
    fn reserve_evidence(&self) -> Result<Arc<ForwardedResponsePermit>, Status> {
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
        permit: ForwardedResponsePermit,
    ) -> GrpcStream<lightwalletd::RawTransaction> {
        let deadline = self.deadline;
        let (demand, mut requests) = mpsc::channel(1);
        let (rows, received) = mpsc::channel(1);
        let admission = HistoryAdmission::new(permit);
        let worker_admission = admission.clone();
        let task = tokio::spawn(async move {
            let admission = worker_admission;
            timeout_at(deadline, async {
                while requests.recv().await.is_some() {
                    // An iteration keeps the slot until it ends even when the
                    // client drops the stream meanwhile. A worker idle
                    // between client polls holds no reference, so dropping
                    // a paused stream frees the slot at once.
                    let Ok(iteration) = admission.reserve_evidence() else {
                        return Ok::<_, Status>(());
                    };
                    let row = tokio::select! {
                        biased;
                        () = rows.closed() => return Ok(()),
                        row = self.next(&admission) => row?,
                    };
                    drop(iteration);
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

    /// Returns when a node call starting now stops waiting for a slot.
    fn call_deadline(&self) -> Instant {
        self.reads.deadline().min(self.deadline)
    }

    async fn read_page(&mut self) -> Result<(), Status> {
        self.page = IntoIter::default();
        while let Some((start, end)) = self.pending.pop() {
            match self
                .reads
                .node_call(
                    ForwardedReadKind::History,
                    self.call_deadline(),
                    self.node.fetch_transparent_address_transaction_ids(
                        &self.lookup,
                        start,
                        end,
                        self.served,
                    ),
                )
                .await
                .map_err(forwarded_read_refused)?
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

    /// Checks that the node still holds the served tip, charging one history
    /// slot and sharing the call with concurrent requests checking the same
    /// tip.
    async fn verify_served_tip(&self) -> Result<(), Status> {
        if self
            .query
            .node_holds_block_admitted(
                self.served,
                &self.reads,
                ForwardedReadKind::History,
                self.call_deadline(),
            )
            .await
            .map_err(forwarded_query_status)?
        {
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

    /// Returns the next row: the served location and digest of the next
    /// listed transaction, checked against the page range and canonical
    /// order, with its bytes from the verified cache or the node.
    ///
    /// Each page is bracketed by its own tip checks and every row binds to the
    /// served view through its local location and committed digest, so rows
    /// need no tip check of their own; the stream checks the tip once more
    /// at its end.
    async fn next(
        &mut self,
        admission: &HistoryAdmission,
    ) -> Result<Option<lightwalletd::RawTransaction>, Status> {
        loop {
            if let Some(id) = self.page.next() {
                let mined = self.served_location(id, admission).await?;
                let position = (
                    mined.location.block_height,
                    mined.location.tx_index_in_block,
                );
                let (start, end) = self.page_range;
                if position.0 < start || position.0 > end {
                    return Err(history_changed());
                }
                if self.last_position.is_some_and(|last| last >= position) {
                    return Err(Status::data_loss(
                        "node address history is out of canonical transaction order",
                    ));
                }
                let bytes = self
                    .reads
                    .mined_transaction_bytes(
                        &self.node,
                        mined,
                        ForwardedReadKind::History,
                        self.call_deadline(),
                    )
                    .await
                    .map_err(history_transaction_status)?;
                self.last_position = Some(position);
                self.page_range.0 = position.0;
                return Ok(Some(lightwalletd::RawTransaction {
                    data: bytes.to_vec(),
                    height: u64::from(position.0.value()),
                }));
            }
            if self.pending.is_empty() {
                self.verify_served_tip().await?;
                return Ok(None);
            }
            self.read_page().await?;
        }
    }
}

/// A row the node no longer mines in its served block means the history
/// changed under the stream.
#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "Every other failure maps as a GetTransaction failure does."
)]
fn history_transaction_status(error: ForwardedTransactionError) -> Status {
    match error {
        ForwardedTransactionError::OutsideServedView => history_changed(),
        error => forwarded_transaction_status(error),
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

/// Retry pushback hint in milliseconds attached to `SERVED_TIP_MISMATCH`
/// refusals as `grpc-retry-pushback-ms` metadata.
const SERVED_TIP_MISMATCH_RETRY_PUSHBACK_MS: u64 = 1_000;
const GRPC_RETRY_PUSHBACK_MS_HEADER: &str = "grpc-retry-pushback-ms";
const TIP_MISMATCH_MESSAGE: &str =
    "node answered at another tip than the served tip; retry shortly";
const PAGES_DISAGREED_MESSAGE: &str = "node UTXO pages at the served tip disagree; retry shortly";

enum UtxoAttemptError {
    TipMismatch {
        served_tip: BlockId,
        answered_tip: BlockId,
    },
    PagesDisagreed,
    Status(Status),
}

impl UtxoAttemptError {
    fn into_status(self) -> Status {
        match self {
            Self::TipMismatch { .. } => served_tip_mismatch(TIP_MISMATCH_MESSAGE),
            Self::PagesDisagreed => served_tip_mismatch(PAGES_DISAGREED_MESSAGE),
            Self::Status(status) => status,
        }
    }
}

/// Refuses a forwarded UTXO or balance answer whose tip differs from the served
/// view (or whose pages disagreed) with `SERVED_TIP_MISMATCH` and retry
/// pushback guidance.
fn served_tip_mismatch(message: &str) -> Status {
    let mut status = status_for_reason(ErrorReason::ServedTipMismatch, message);
    status.metadata_mut().insert(
        GRPC_RETRY_PUSHBACK_MS_HEADER,
        tonic::metadata::MetadataValue::from(SERVED_TIP_MISMATCH_RETRY_PUSHBACK_MS),
    );
    status
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
            .balance(
                stream::iter(request.into_inner().addresses.into_iter().map(Ok)),
                "get_taddress_balance",
                self.block_methods.tip_wait(),
            )
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
                "get_taddress_balance_stream",
                self.block_methods.tip_wait(),
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
        let mut reader = self.query_api.slot.capture();
        if let RequestedBlock::Height(height) = block
            && height > reader.chain_epoch().visible_tip_height
        {
            drop(reader);
            self.block_methods
                .await_tip(&self.query_api.slot, "get_tree_state", height)
                .await?;
            reader = self.query_api.slot.capture();
        }
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
            .tree_state_hex(reader, Some(height))
            .await
            .map_err(|error| tree_state_status(&error))?;
        lightwalletd_tree_state_from_hex(state).map(Response::new)
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
            .utxos(
                request.into_inner(),
                self.response_limits.max_address_utxos,
                "get_address_utxos",
                self.block_methods.tip_wait(),
            )
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
            .utxos(
                request.into_inner(),
                self.response_limits.max_address_utxos,
                "get_address_utxos_stream",
                self.block_methods.tip_wait(),
            )
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
    _permit: Arc<ForwardedResponsePermit>,
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
        // A body error is terminal too, so the slot is released even while
        // the consumer keeps the failed body.
        if matches!(&frame, Poll::Ready(None | Some(Err(_))))
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

    /// Takes every free response slot of `kind`, so the next request must
    /// wait for one of the returned permits to drop.
    async fn take_every_slot(
        reads: &ForwardedReadLimit,
        kind: ForwardedResponseKind,
    ) -> Vec<ForwardedResponsePermit> {
        let mut held = Vec::new();
        while let Ok(permit) = reads.admit_response(kind, Instant::now()).await {
            held.push(permit);
        }
        held
    }

    async fn slot_free(reads: &ForwardedReadLimit, kind: ForwardedResponseKind) -> bool {
        reads.admit_response(kind, Instant::now()).await.is_ok()
    }

    #[tokio::test(start_paused = true)]
    async fn a_send_stall_releases_unary_utxo_admission_while_a_piece_is_still_held()
    -> Result<(), Box<dyn std::error::Error>> {
        use http_body_util::{BodyExt as _, Full};
        use tower::{Layer as _, ServiceExt as _};
        use zinder_runtime::GrpcSendStallLayer;

        let reads = ForwardedReadLimit::default();
        let kind = ForwardedResponseKind::UtxoResponse;
        let mut held = take_every_slot(&reads, kind).await;
        let permit = UtxoResponsePermit {
            _permit: Arc::new(held.pop().ok_or("no response slot")?),
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
        assert!(!slot_free(&reads, kind).await);
        let returned_permit = reads
            .admit_response(kind, Instant::now() + Duration::from_secs(10))
            .await?;

        let status = body.frame().await.ok_or("missing stall status")??;
        assert_eq!(
            status
                .trailers_ref()
                .and_then(Status::from_header_map)
                .map(|status| status.code()),
            Some(tonic::Code::DeadlineExceeded)
        );
        drop((returned_permit, body, piece));
        assert!(slot_free(&reads, kind).await);
        Ok(())
    }

    #[tokio::test]
    async fn a_body_error_releases_unary_utxo_admission_while_the_body_is_kept()
    -> Result<(), Box<dyn std::error::Error>> {
        use http_body_util::{BodyExt as _, StreamBody};

        let reads = ForwardedReadLimit::default();
        let kind = ForwardedResponseKind::UtxoResponse;
        let mut held = take_every_slot(&reads, kind).await;
        let permit = UtxoResponsePermit {
            _permit: Arc::new(held.pop().ok_or("no response slot")?),
        };
        let failing = StreamBody::new(tokio_stream::iter([Err::<
            Frame<<Body as HttpBody>::Data>,
            Status,
        >(Status::internal(
            "encoding failed",
        ))]));
        let mut response = http::Response::new(Body::new(failing));
        response.extensions_mut().insert(permit);
        let mut body = reserve_utxo_response_body(response).into_body();

        assert!(!slot_free(&reads, kind).await);
        assert!(matches!(body.frame().await, Some(Err(_))));
        assert!(
            slot_free(&reads, kind).await,
            "a failed body kept its response slot"
        );
        drop(body);
        Ok(())
    }

    #[tokio::test]
    async fn a_stream_dropped_during_an_iteration_keeps_its_slot_until_the_iteration_ends()
    -> Result<(), Box<dyn std::error::Error>> {
        let reads = ForwardedReadLimit::default();
        let kind = ForwardedResponseKind::HistoryStream;
        let mut held = take_every_slot(&reads, kind).await;
        let stream = HistoryAdmission::new(held.pop().ok_or("no stream slot")?);
        let worker = stream.clone();

        let iteration = worker.reserve_evidence()?;
        drop(stream);
        assert!(
            !slot_free(&reads, kind).await,
            "a replacement entered while the dropped stream's iteration still ran"
        );
        drop(iteration);
        assert!(slot_free(&reads, kind).await);
        assert!(worker.reserve_evidence().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn an_idle_dropped_stream_frees_its_slot_at_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let reads = ForwardedReadLimit::default();
        let kind = ForwardedResponseKind::HistoryStream;
        let mut held = take_every_slot(&reads, kind).await;
        let stream = HistoryAdmission::new(held.pop().ok_or("no stream slot")?);
        let _worker_waiting_for_demand = stream.clone();

        drop(stream);
        assert!(slot_free(&reads, kind).await);
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

    #[test]
    fn a_rate_limited_node_is_unavailable_with_its_own_reason() {
        let status = node_status(SourceError::NodeRateLimited {
            operation: "getaddressutxos",
        });

        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(
            tonic_types::StatusExt::get_details_error_info(&status)
                .map(|error_info| error_info.reason),
            Some("NODE_RATE_LIMITED".to_owned())
        );
    }
}
