//! gRPC adapter for compact-blocks serving, which answers the block methods
//! of lightwalletd `CompactTxStreamer` from the canonical reader of each
//! compact-blocks generation alone.

use std::{num::NonZeroU32, sync::Arc, time::Duration, time::Instant};

use arc_swap::ArcSwap;
use tokio::sync::watch;
use tonic::{Request, Response, Status};
use zinder_core::{ChainEpoch, CompactBlockArtifact, NetworkUpgradeActivations};
use zinder_proto::compat::lightwalletd::{self, compact_tx_streamer_server};
use zinder_query::{
    ArtifactKey, CanonicalReader, QueryError, RocksDbCanonicalReader, StorageReadError,
    read_on_blocking_thread, status_from_query_error,
};
use zinder_runtime::RepeatedFieldGuard;
use zinder_store::ArtifactFamily;

use crate::block_methods::{
    self, BlockMethods, ChunkRead, ChunkReadFailure, ChunkReadOutcome, ChunkView,
    CompactBlockReads, RequestedBlock, view_reaches,
};
use crate::compact_block_messages::CompactBlockShape;
use crate::grpc::GrpcStream;
use crate::node_tip_witness::NodeTipWitness;
use crate::ping::LightwalletdPing;
use crate::request_limits::COMPACT_TX_STREAMER_FIELD_LIMITS;
use crate::stale_tip::StaleTipLimit;
use crate::tip_wait::TipWait;

/// The canonical reader compact-blocks serving answers from, which its owner
/// replaces with the reader of each newer generation.
///
/// Clones share one slot. A request captures the current reader when it
/// starts and holds it until it no longer reads, so a replaced reader stays
/// open until its last read has ended. A block-range chunk after the first
/// captures the reader on the blocking thread that reads it and drops it
/// before its blocks are converted, so the stream holds a generation only
/// during that storage read.
#[derive(Clone)]
pub struct CanonicalReaderSlot {
    current: Arc<ArcSwap<RocksDbCanonicalReader>>,
    /// Changes on every publication, so a read can wait for a newer reader.
    publication: Arc<watch::Sender<()>>,
}

impl CanonicalReaderSlot {
    /// Creates a slot that serves `reader`.
    #[must_use]
    pub fn new(reader: Arc<RocksDbCanonicalReader>) -> Self {
        Self {
            current: Arc::new(ArcSwap::from(reader)),
            publication: Arc::new(watch::Sender::new(())),
        }
    }

    /// Returns the reader the slot serves now.
    #[must_use]
    pub fn capture(&self) -> Arc<RocksDbCanonicalReader> {
        self.current.load_full()
    }

    /// Returns whether `reader` is still the generation currently published in
    /// this slot, without cloning or dropping its reference count.
    #[must_use]
    pub fn is_current(&self, reader: &Arc<RocksDbCanonicalReader>) -> bool {
        let current = self.current.load();
        Arc::ptr_eq(&current, reader)
    }

    /// Returns the visible tip height of the currently published reader without
    /// cloning or dropping its reference count.
    #[must_use]
    pub fn peek_visible_tip_height(&self) -> zinder_core::BlockHeight {
        self.current.load().chain_epoch().visible_tip_height
    }

    /// Serves `reader` from now on and returns the reader it replaces, which
    /// requests that captured it may still hold.
    #[must_use]
    pub fn publish(&self, reader: Arc<RocksDbCanonicalReader>) -> Arc<RocksDbCanonicalReader> {
        let replaced = self.current.swap(reader);
        self.publication.send_replace(());
        replaced
    }
}

/// Compact-blocks serving reads at the reader a request captures, so every
/// read of one view answers at that reader's fence.
impl CompactBlockReads for CanonicalReaderSlot {
    type View = Arc<RocksDbCanonicalReader>;

    fn capture_view(
        &self,
    ) -> impl Future<Output = Result<Arc<RocksDbCanonicalReader>, Status>> + Send {
        std::future::ready(Ok(self.capture()))
    }

    fn subscribe_publications(&self) -> Option<watch::Receiver<()>> {
        Some(self.publication.subscribe())
    }

    fn peek_visible_tip_height(
        &self,
    ) -> impl Future<Output = Result<zinder_core::BlockHeight, Status>> + Send {
        std::future::ready(Ok(Self::peek_visible_tip_height(self)))
    }

    fn range_fence(
        &self,
        view: &Self::View,
    ) -> Option<(
        zinder_store::CanonicalEventFence,
        zinder_core::CanonicalHistoryBounds,
    )> {
        Some((view.event_fence(), view.history_bounds()))
    }

    fn tail_source(&self, view: &Self::View) -> Option<zinder_query::CompactBlockFence> {
        Some(zinder_query::CompactBlockFence {
            event_fence: view.event_fence(),
            history_bounds: view.history_bounds(),
            source: zinder_query::CompactBlockSource::of(view),
        })
    }

    fn view_chain_epoch(view: &Arc<RocksDbCanonicalReader>) -> ChainEpoch {
        view.chain_epoch()
    }

    fn view_activations(
        &self,
        view: &Arc<RocksDbCanonicalReader>,
    ) -> impl Future<Output = Result<NetworkUpgradeActivations, Status>> + Send {
        std::future::ready(
            view.network_upgrade_schedule()
                .map(|schedule| schedule.activations().clone())
                .ok_or_else(|| {
                    Status::failed_precondition(
                        "canonical reader carries no writer-admitted network-upgrade schedule",
                    )
                }),
        )
    }

    async fn tip_block_time(&self, view: &Arc<RocksDbCanonicalReader>) -> Result<i64, Status> {
        view.block_header_at(view.chain_epoch().visible_tip_height)
            .await
            .map_err(storage_status)?
            .map(|header| header.block_time)
            .ok_or_else(|| Status::unavailable("the served tip has no stored block header"))
    }

    async fn read_compact_block(
        &self,
        view: &Arc<RocksDbCanonicalReader>,
        block: RequestedBlock,
    ) -> Result<CompactBlockArtifact, Status> {
        match block {
            RequestedBlock::Height(height) => view
                .compact_block_at(height)
                .await
                .map_err(storage_status)?
                .ok_or_else(|| {
                    status_from_query_error(&QueryError::ArtifactUnavailable {
                        family: ArtifactFamily::CompactBlock,
                        key: ArtifactKey::BlockHeight(height),
                    })
                }),
            // The reader resolves the hash and reads its block in one read.
            RequestedBlock::Hash(block_hash) => view
                .compact_block_by_hash(block_hash)
                .await
                .map_err(storage_status)?
                .ok_or_else(|| status_from_query_error(&QueryError::BlockNotInBestChain)),
        }
    }

    async fn block_hash_at(
        &self,
        view: &Self::View,
        height: zinder_core::BlockHeight,
    ) -> Result<zinder_core::BlockHash, Status> {
        view.block_header_at(height)
            .await
            .map_err(storage_status)?
            .map(|row| row.block_hash)
            .ok_or_else(|| Status::failed_precondition("missing settled shielded range anchor"))
    }

    async fn read_compact_block_rows_with_lengths(
        &self,
        view: &Self::View,
        range: zinder_core::BlockHeightRange,
        max_bytes: usize,
    ) -> Result<Option<Vec<(CompactBlockArtifact, usize)>>, Status> {
        view.compact_block_rows_with_lengths(range, max_bytes)
            .await
            .map(Some)
            .map_err(storage_status)
    }

    async fn read_compact_block_chunk<Output, Convert>(
        &self,
        read: ChunkRead<Arc<RocksDbCanonicalReader>>,
        convert: Convert,
    ) -> ChunkReadOutcome<Output>
    where
        Output: Send + 'static,
        Convert: FnOnce(ChainEpoch, Vec<CompactBlockArtifact>) -> Output + Send + 'static,
    {
        let slot = self.clone();
        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            span.in_scope(|| read_chunk_on_this_thread(&slot, read, convert))
        })
        .await
        .unwrap_or_else(|error| {
            ChunkReadOutcome::unread(ChunkReadFailure::Failed(status_from_query_error(
                &QueryError::BlockingTaskFailed {
                    reason: error.to_string(),
                },
            )))
        })
    }
}

/// Reads one chunk through the canonical read interface on the calling
/// blocking thread and converts its blocks there.
///
/// A later chunk captures the current generation here, on the thread that
/// reads it, so a read waiting for a blocking thread holds no generation.
/// The generation is dropped before the blocks are converted, and the read
/// stage leaves out the wait for this thread.
fn read_chunk_on_this_thread<Output>(
    slot: &CanonicalReaderSlot,
    read: ChunkRead<Arc<RocksDbCanonicalReader>>,
    convert: impl FnOnce(ChainEpoch, Vec<CompactBlockArtifact>) -> Output,
) -> ChunkReadOutcome<Output> {
    let ChunkRead {
        chunk,
        order,
        max_bytes,
        view,
        cancel,
    } = read;
    if cancel.is_cancelled() {
        return ChunkReadOutcome::unread(ChunkReadFailure::Failed(Status::cancelled(
            "block-range stream cancelled",
        )));
    }
    let reader = match view {
        ChunkView::Captured(reader) => reader,
        ChunkView::Current => slot.capture(),
    };
    let read_at = reader.chain_epoch();
    if !view_reaches(&read_at, chunk) {
        return ChunkReadOutcome::unread(ChunkReadFailure::TipBelowChunk(read_at));
    }
    let started_at = Instant::now();
    let blocks =
        read_on_blocking_thread(reader.compact_blocks_in_range_up_to(chunk, order, max_bytes));
    let read_duration = started_at.elapsed();
    drop(reader);
    if cancel.is_cancelled() {
        return ChunkReadOutcome {
            read_duration: Some(read_duration),
            converted: Err(ChunkReadFailure::Failed(Status::cancelled(
                "block-range stream cancelled",
            ))),
        };
    }
    ChunkReadOutcome {
        read_duration: Some(read_duration),
        converted: blocks
            .map(|blocks| convert(read_at, blocks))
            .map_err(|error| ChunkReadFailure::Failed(storage_status(error))),
    }
}

fn storage_status(error: StorageReadError) -> Status {
    status_from_query_error(&QueryError::canonical_read(error))
}

/// gRPC adapter that serves lightwalletd `CompactTxStreamer` block methods
/// from the canonical reader a [`CanonicalReaderSlot`] serves.
///
/// `GetLightdInfo`, `GetLatestBlock`, `GetBlock`, `GetBlockRange`, and `Ping`
/// answer as [`crate::LightwalletdGrpcAdapter`] answers them, except that
/// `taddrSupport` is `false`. Every other method returns `UNIMPLEMENTED`. The
/// adapter never contacts a node.
#[derive(Clone)]
pub struct CompactBlocksGrpcAdapter {
    slot: CanonicalReaderSlot,
    block_methods: BlockMethods,
    published_history: Option<Arc<crate::PublishedShieldedHistory>>,
    live_tail: crate::published_range::ShieldedTailBuffer,
    ping: LightwalletdPing,
}

impl CompactBlocksGrpcAdapter {
    /// Serves matching settled shielded ranges from the history admitted for each request.
    #[must_use]
    pub fn with_published_shielded_history(
        mut self,
        history: Arc<crate::PublishedShieldedHistory>,
    ) -> Self {
        self.published_history = Some(history);
        self
    }

    /// Creates an adapter that serves the reader `slot` serves at each
    /// request.
    #[must_use]
    pub fn new(slot: CanonicalReaderSlot) -> Self {
        Self {
            slot,
            block_methods: BlockMethods::default(),
            published_history: None,
            live_tail: crate::published_range::ShieldedTailBuffer::default(),
            ping: LightwalletdPing::default(),
        }
    }

    /// Limits how many `GetBlockRange` streams this adapter and its clones
    /// serve at once.
    ///
    /// A request past the limit waits for a slot before any read, and fails
    /// with `UNAVAILABLE` when none returns in time. The default is
    /// [`crate::DEFAULT_MAX_CONCURRENT_BLOCK_RANGE_STREAMS`].
    #[must_use]
    pub fn with_max_concurrent_block_range_streams(mut self, max_streams: NonZeroU32) -> Self {
        self.block_methods = self
            .block_methods
            .with_max_concurrent_block_range_streams(max_streams);
        self
    }

    /// Sets how long a block-range request past the limit waits for a slot
    /// before it fails with `UNAVAILABLE`. The default is
    /// [`crate::DEFAULT_BLOCK_RANGE_ADMISSION_WAIT`].
    #[must_use]
    pub fn with_block_range_admission_wait(mut self, max_wait: Duration) -> Self {
        self.block_methods = self.block_methods.with_block_range_admission_wait(max_wait);
        self
    }

    /// Sets how old the served tip may be before `GetLatestBlock` and
    /// `GetLightdInfo` fail with `UNAVAILABLE`. The default is
    /// [`StaleTipLimit::DISABLED`].
    #[must_use]
    pub const fn with_stale_tip_limit(mut self, stale_tip_limit: StaleTipLimit) -> Self {
        self.block_methods.stale_tip_limit = stale_tip_limit;
        self
    }

    /// Sets the witness whose report of the writer's node decides whether an
    /// old served tip is the chain tip or a stale one.
    ///
    /// Without one, no tip is vouched for, so the stale-tip limit refuses
    /// every tip older than the limit.
    #[must_use]
    pub fn with_node_tip_witness(mut self, node_tip_witness: NodeTipWitness) -> Self {
        self.block_methods = self.block_methods.with_node_tip_witness(node_tip_witness);
        self
    }

    /// Lets a block read that names a height just past the visible tip wait
    /// briefly for that height instead of failing at once.
    ///
    /// Applies to `GetBlock`, `GetBlockNullifiers`, `GetBlockRange`,
    /// `GetBlockRangeNullifiers`, the encoded range methods, and, where this
    /// adapter serves it, `GetTreeState`, when they select by height. The
    /// adapter defaults to [`TipWait::disabled`]; the
    /// `zinder-compat-lightwalletd` binary sets the configured wait.
    #[must_use]
    pub fn with_tip_wait(mut self, tip_wait: TipWait) -> Self {
        self.block_methods = self.block_methods.with_tip_wait(tip_wait);
        self
    }

    /// Sets how this adapter answers `Ping`. The default refuses it with
    /// `FAILED_PRECONDITION`, as lightwalletd does without
    /// `--ping-very-insecure`.
    #[must_use]
    pub fn with_ping(mut self, ping: LightwalletdPing) -> Self {
        self.ping = ping;
        self
    }

    /// Wraps this adapter in the generated tonic server type, behind the
    /// pre-decode bounds of [`COMPACT_TX_STREAMER_FIELD_LIMITS`].
    #[must_use]
    pub fn into_server(
        self,
    ) -> RepeatedFieldGuard<compact_tx_streamer_server::CompactTxStreamerServer<Self>> {
        RepeatedFieldGuard::new(
            compact_tx_streamer_server::CompactTxStreamerServer::new(self)
                .max_decoding_message_size(zinder_runtime::MAX_DECODING_MESSAGE_BYTES),
            COMPACT_TX_STREAMER_FIELD_LIMITS,
        )
    }
}

fn unsupported(method: &'static str) -> Status {
    Status::unimplemented(format!(
        "compact-blocks compatibility does not implement {method}"
    ))
}

#[tonic::async_trait]
impl lightwalletd::EncodedBlockRange for CompactBlocksGrpcAdapter {
    async fn get_encoded_block_range(
        &self,
        request: Request<lightwalletd::BlockRange>,
    ) -> Result<Response<lightwalletd::EncodedBlockRangeStream>, Status> {
        let request = request.into_inner();
        self.block_methods
            .await_range_tip(&self.slot, "get_encoded_block_range", &request)
            .await?;
        let selection = self.published_history.as_ref().map_or_else(
            || {
                crate::published_range::PublishedRangeSelection::standalone(
                    self.live_tail.clone(),
                    None,
                )
            },
            |history| history.encoded_range_selection(None),
        );
        self.block_methods
            .published_block_range(
                &self.slot,
                &request,
                selection,
                CompactBlockShape::for_range,
            )
            .await
            .map(Response::new)
    }

    fn supports_encoded_frames(&self) -> bool {
        true
    }

    async fn get_encoded_block_range_frames(
        &self,
        request: Request<lightwalletd::BlockRange>,
    ) -> Result<Response<lightwalletd::EncodedBlockFrameStream>, Status> {
        let request = request.into_inner();
        self.block_methods
            .await_range_tip(&self.slot, "get_encoded_block_range", &request)
            .await?;
        let selection = self.published_history.as_ref().map_or_else(
            || {
                crate::published_range::PublishedRangeSelection::standalone(
                    self.live_tail.clone(),
                    None,
                )
            },
            |history| history.encoded_range_selection(None),
        );
        self.block_methods
            .published_block_range_frames(
                &self.slot,
                &request,
                selection,
                CompactBlockShape::for_range,
            )
            .await
            .map(Response::new)
    }
}

#[tonic::async_trait]
impl compact_tx_streamer_server::CompactTxStreamer for CompactBlocksGrpcAdapter {
    fn encoded_block_range(&self) -> Option<&dyn lightwalletd::EncodedBlockRange> {
        Some(self)
    }

    type GetBlockRangeStream = GrpcStream<lightwalletd::CompactBlock>;
    type GetBlockRangeNullifiersStream = GrpcStream<lightwalletd::CompactBlock>;
    type GetTaddressTxidsStream = GrpcStream<lightwalletd::RawTransaction>;
    type GetTaddressTransactionsStream = GrpcStream<lightwalletd::RawTransaction>;
    type GetMempoolTxStream = GrpcStream<lightwalletd::CompactTx>;
    type GetMempoolStreamStream = GrpcStream<lightwalletd::RawTransaction>;
    type GetSubtreeRootsStream = GrpcStream<lightwalletd::SubtreeRoot>;
    type GetAddressUtxosStreamStream = GrpcStream<lightwalletd::GetAddressUtxosReply>;

    async fn get_lightd_info(
        &self,
        _request: Request<lightwalletd::Empty>,
    ) -> Result<Response<lightwalletd::LightdInfo>, Status> {
        self.block_methods
            .lightd_info(&self.slot, false)
            .await
            .map(Response::new)
    }

    async fn get_latest_block(
        &self,
        _request: Request<lightwalletd::ChainSpec>,
    ) -> Result<Response<lightwalletd::BlockId>, Status> {
        self.block_methods
            .latest_block(&self.slot)
            .await
            .map(Response::new)
    }

    async fn get_block(
        &self,
        request: Request<lightwalletd::BlockId>,
    ) -> Result<Response<lightwalletd::CompactBlock>, Status> {
        let block_id = request.into_inner();
        self.block_methods
            .await_block_tip(&self.slot, "get_block", &block_id)
            .await?;
        block_methods::compact_block(&self.slot, &block_id, CompactBlockShape::FULL_BLOCK)
            .await
            .map(Response::new)
    }

    async fn get_block_range(
        &self,
        request: Request<lightwalletd::BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeStream>, Status> {
        let request = request.into_inner();
        self.block_methods
            .await_range_tip(&self.slot, "get_block_range", &request)
            .await?;
        self.block_methods
            .block_range(&self.slot, &request, CompactBlockShape::for_range)
            .await
            .map(Response::new)
    }

    async fn get_block_nullifiers(
        &self,
        _request: Request<lightwalletd::BlockId>,
    ) -> Result<Response<lightwalletd::CompactBlock>, Status> {
        Err(unsupported("GetBlockNullifiers"))
    }

    async fn get_block_range_nullifiers(
        &self,
        _request: Request<lightwalletd::BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeNullifiersStream>, Status> {
        Err(unsupported("GetBlockRangeNullifiers"))
    }

    async fn get_transaction(
        &self,
        _request: Request<lightwalletd::TxFilter>,
    ) -> Result<Response<lightwalletd::RawTransaction>, Status> {
        Err(unsupported("GetTransaction"))
    }

    async fn send_transaction(
        &self,
        _request: Request<lightwalletd::RawTransaction>,
    ) -> Result<Response<lightwalletd::SendResponse>, Status> {
        Err(unsupported("SendTransaction"))
    }

    async fn get_taddress_txids(
        &self,
        _request: Request<lightwalletd::TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTxidsStream>, Status> {
        Err(unsupported("GetTaddressTxids"))
    }

    async fn get_taddress_transactions(
        &self,
        _request: Request<lightwalletd::TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTransactionsStream>, Status> {
        Err(unsupported("GetTaddressTransactions"))
    }

    async fn get_taddress_balance(
        &self,
        _request: Request<lightwalletd::AddressList>,
    ) -> Result<Response<lightwalletd::Balance>, Status> {
        Err(unsupported("GetTaddressBalance"))
    }

    async fn get_taddress_balance_stream(
        &self,
        _request: Request<tonic::Streaming<lightwalletd::Address>>,
    ) -> Result<Response<lightwalletd::Balance>, Status> {
        Err(unsupported("GetTaddressBalanceStream"))
    }

    async fn get_mempool_tx(
        &self,
        _request: Request<lightwalletd::GetMempoolTxRequest>,
    ) -> Result<Response<Self::GetMempoolTxStream>, Status> {
        Err(unsupported("GetMempoolTx"))
    }

    async fn get_mempool_stream(
        &self,
        _request: Request<lightwalletd::Empty>,
    ) -> Result<Response<Self::GetMempoolStreamStream>, Status> {
        Err(unsupported("GetMempoolStream"))
    }

    async fn get_tree_state(
        &self,
        _request: Request<lightwalletd::BlockId>,
    ) -> Result<Response<lightwalletd::TreeState>, Status> {
        Err(unsupported("GetTreeState"))
    }

    async fn get_latest_tree_state(
        &self,
        _request: Request<lightwalletd::Empty>,
    ) -> Result<Response<lightwalletd::TreeState>, Status> {
        Err(unsupported("GetLatestTreeState"))
    }

    async fn get_subtree_roots(
        &self,
        _request: Request<lightwalletd::GetSubtreeRootsArg>,
    ) -> Result<Response<Self::GetSubtreeRootsStream>, Status> {
        Err(unsupported("GetSubtreeRoots"))
    }

    async fn get_address_utxos(
        &self,
        _request: Request<lightwalletd::GetAddressUtxosArg>,
    ) -> Result<Response<lightwalletd::GetAddressUtxosReplyList>, Status> {
        Err(unsupported("GetAddressUtxos"))
    }

    async fn get_address_utxos_stream(
        &self,
        _request: Request<lightwalletd::GetAddressUtxosArg>,
    ) -> Result<Response<Self::GetAddressUtxosStreamStream>, Status> {
        Err(unsupported("GetAddressUtxosStream"))
    }

    async fn get_standard_fee(
        &self,
        _request: Request<lightwalletd::Empty>,
    ) -> Result<Response<lightwalletd::StandardFee>, Status> {
        Err(unsupported("GetStandardFee"))
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
