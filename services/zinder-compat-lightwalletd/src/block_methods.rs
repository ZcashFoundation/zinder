//! The block methods of `CompactTxStreamer` that both serving modes answer:
//! `GetLightdInfo`, `GetLatestBlock`, `GetBlock`, and `GetBlockRange`, and the
//! nullifier variants that wallet serving adds.
//!
//! Each mode supplies only its canonical reads, through
//! [`CompactBlockReads`]: wallet serving reads through `WalletQueryApi` at its
//! serving pair, and compact-blocks serving through the
//! `RocksDbCanonicalReader` of its published generation. A request captures
//! one chain view and makes every read at it, so one response never mixes two
//! chain epochs. A block-range stream reads its first chunk at that view and
//! each later chunk at the view current when it is read, as the `block_range`
//! module describes.
//!
//! A chunk's lightwalletd messages are built on the blocking pool, and a
//! chunk whose blocks cannot all be converted sends none of them. Tonic
//! encodes the messages to protobuf on the async runtime as it sends them.

use std::{
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, Instant},
};

use tonic::Status;
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, ChainEpoch, CompactBlockArtifact,
    NetworkUpgradeActivations,
    wire::{encode_bip70_chain_name, encode_branch_id_hex, encode_internal_block_hash},
};
use zinder_proto::compat::lightwalletd::{self, LIGHTWALLETD_PROTOCOL_VERSION};
use zinder_store::HeightOrder;

use crate::block_range::{
    BlockRangeChunks, BlockRangeContinuity, BlockRangeResponse, BlockRangeSink, BlockRangeStage,
    BlockRangeStreamLimit, DEFAULT_MAX_CONCURRENT_BLOCK_RANGE_STREAMS,
};
use crate::compact_block_messages::{CompactBlockShape, compact_block_to_lightwalletd};
use crate::network_tip_estimate::estimated_network_height;
use crate::node_tip_witness::NodeTipWitness;
use crate::stale_tip::{StaleTipLimit, TipBlockTime, TipBlockTimeCache, unix_now_seconds};

/// The chain view a chunk read answers at.
pub(crate) enum ChunkView<View> {
    /// The view the request captured, which a stream's first chunk reads so
    /// it answers the bounds the request was checked against.
    Captured(View),
    /// The view current when the read runs, which each later chunk reads:
    /// the hash link, not a pin, proves that it continues the stream.
    Current,
}

/// One chunk read of a block-range stream.
pub(crate) struct ChunkRead<View> {
    /// Heights the read may cover.
    pub(crate) chunk: BlockHeightRange,
    /// The end of `chunk` the read starts from.
    pub(crate) order: HeightOrder,
    /// The view the read answers at.
    pub(crate) view: ChunkView<View>,
}

/// Returns whether `read_at` still serves every height of `chunk`.
///
/// A chunk read checks it before it reads, so a reorg that lowered the
/// visible tip ends a stream with lightwalletd's `OUT_OF_RANGE` rather than
/// the `NOT_FOUND` of a read above the tip.
pub(crate) const fn view_reaches(read_at: &ChainEpoch, chunk: BlockHeightRange) -> bool {
    chunk.end.value() <= read_at.visible_tip_height.value()
}

/// What a chunk read hands back to the stream's task, which records it.
pub(crate) struct ChunkReadOutcome<Output> {
    /// How long the chunk's storage read took, or `None` when none ran.
    pub(crate) read_duration: Option<Duration>,
    /// What the conversion returned, or why no conversion ran.
    pub(crate) converted: Result<Output, ChunkReadFailure>,
}

impl<Output> ChunkReadOutcome<Output> {
    /// A read that ended before its storage read ran.
    pub(crate) const fn unread(failure: ChunkReadFailure) -> Self {
        Self {
            read_duration: None,
            converted: Err(failure),
        }
    }
}

/// Why a chunk read converted no block.
pub(crate) enum ChunkReadFailure {
    /// The view the read would answer at no longer serves every height of
    /// the chunk, so nothing was read.
    TipBelowChunk(ChainEpoch),
    /// The read, or the task that ran it, failed with this status.
    Failed(Status),
}

/// The block a lightwalletd `BlockId` names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestedBlock {
    /// The best-chain block at this height.
    Height(BlockHeight),
    /// The best-chain block with this hash.
    Hash(BlockHash),
}

/// The canonical reads the block methods make, as one serving mode answers
/// them.
///
/// Each read fails with the status its request fails with. A read that
/// touches storage keeps that work off the async runtime.
pub(crate) trait CompactBlockReads: Clone + Send + Sync + 'static {
    /// One captured chain view, which every read of one request answers at.
    type View: Send + Sync + 'static;

    /// Blocks a block-range stream queues ahead of a slow client, on top of
    /// the chunk the stream's reader is sending.
    const BLOCK_RANGE_QUEUE_CAPACITY: usize;

    /// Captures the chain view current now, without a storage read.
    fn capture_view(&self) -> impl Future<Output = Result<Self::View, Status>> + Send;

    /// Returns the chain epoch `view` reads.
    fn view_chain_epoch(view: &Self::View) -> ChainEpoch;

    /// Returns the network-upgrade activations the canonical writer admitted
    /// with `view` or with a newer view.
    fn view_activations(
        &self,
        view: &Self::View,
    ) -> impl Future<Output = Result<NetworkUpgradeActivations, Status>> + Send;

    /// Reads the block time in the header of `view`'s visible tip.
    fn tip_block_time(&self, view: &Self::View)
    -> impl Future<Output = Result<i64, Status>> + Send;

    /// Reads the compact block `block` names in `view`'s best chain, at or
    /// below its visible tip.
    fn read_compact_block(
        &self,
        view: &Self::View,
        block: RequestedBlock,
    ) -> impl Future<Output = Result<CompactBlockArtifact, Status>> + Send;

    /// Reads the blocks of `read.chunk` that fit
    /// [`BLOCK_RANGE_CHUNK_MAX_BYTES`](crate::BLOCK_RANGE_CHUNK_MAX_BYTES),
    /// from the end `read.order` visits first, and runs `convert` on the
    /// blocking pool with the chain epoch they were read at and the blocks in
    /// ascending height order.
    ///
    /// The read first checks [`view_reaches`] at the view it answers at, and
    /// reads nothing when that view does not reach the chunk.
    /// It drops the view it captured before `convert` runs.
    fn read_compact_block_chunk<Output, Convert>(
        &self,
        read: ChunkRead<Self::View>,
        convert: Convert,
    ) -> impl Future<Output = ChunkReadOutcome<Output>> + Send
    where
        Output: Send + 'static,
        Convert: FnOnce(ChainEpoch, Vec<CompactBlockArtifact>) -> Output + Send + 'static;
}

/// The Unix time in seconds that tip calls judge a tip's age at.
type UnixClock = Arc<dyn Fn() -> i64 + Send + Sync>;

/// What the block methods of one adapter and its clones share.
#[derive(Clone)]
pub(crate) struct BlockMethods {
    block_range_streams: BlockRangeStreamLimit,
    /// How old the served tip may be before the tip calls refuse it.
    pub(crate) stale_tip_limit: StaleTipLimit,
    tip_block_times: TipBlockTimeCache,
    node_tip_witness: NodeTipWitness,
    unix_clock: UnixClock,
}

impl std::fmt::Debug for BlockMethods {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlockMethods")
            .field(
                "max_concurrent_block_range_streams",
                &self.block_range_streams.max_streams(),
            )
            .field("stale_tip_limit", &self.stale_tip_limit.limit())
            .field("tip_block_times", &self.tip_block_times)
            .field("node_tip_witness", &self.node_tip_witness)
            .finish_non_exhaustive()
    }
}

impl Default for BlockMethods {
    fn default() -> Self {
        Self {
            block_range_streams: BlockRangeStreamLimit::new(
                DEFAULT_MAX_CONCURRENT_BLOCK_RANGE_STREAMS,
            ),
            stale_tip_limit: StaleTipLimit::DISABLED,
            tip_block_times: TipBlockTimeCache::default(),
            node_tip_witness: NodeTipWitness::default(),
            unix_clock: Arc::new(unix_now_seconds),
        }
    }
}

/// The served tip a tip call answers with, the view it was read at, the times
/// of its block, and when it was read.
struct ServedTip<View> {
    view: View,
    chain_epoch: ChainEpoch,
    times: TipBlockTime,
    now_unix_seconds: i64,
}

impl BlockMethods {
    /// Limits how many block-range streams these methods serve at once,
    /// keeping the admission wait.
    pub(crate) fn with_max_concurrent_block_range_streams(
        mut self,
        max_streams: NonZeroU32,
    ) -> Self {
        self.block_range_streams = BlockRangeStreamLimit::new(max_streams)
            .with_max_wait(self.block_range_streams.max_wait());
        self
    }

    /// Sets how long a block-range request past the limit waits for a slot.
    pub(crate) fn with_block_range_admission_wait(mut self, max_wait: Duration) -> Self {
        self.block_range_streams = self.block_range_streams.with_max_wait(max_wait);
        self
    }

    /// Sets the witness whose report of the writer's node decides whether an
    /// old served tip is the chain tip or a stale one.
    pub(crate) fn with_node_tip_witness(mut self, node_tip_witness: NodeTipWitness) -> Self {
        self.node_tip_witness = node_tip_witness;
        self
    }

    /// Judges tip ages at the Unix time `unix_clock` reports.
    #[cfg(test)]
    fn with_unix_clock(mut self, unix_clock: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.unix_clock = Arc::new(unix_clock);
        self
    }

    /// Answers `GetLightdInfo` from the served tip.
    pub(crate) async fn lightd_info<Reads: CompactBlockReads>(
        &self,
        reads: &Reads,
        transparent_address_support: bool,
    ) -> Result<lightwalletd::LightdInfo, Status> {
        let served_tip = self.served_tip(reads, "get_lightd_info").await?;
        // Read after the tip: a later view can only carry the same or a newer
        // admitted schedule, and revisions never change activations at or
        // below the tip they were admitted at.
        let activations = reads.view_activations(&served_tip.view).await?;
        if served_tip.chain_epoch.network != activations.network() {
            return Err(Status::failed_precondition(
                "network upgrade activations do not match the chain epoch network",
            ));
        }
        let tip_height = served_tip.chain_epoch.visible_tip_height;
        let estimated_height = estimated_network_height(
            &activations,
            tip_height,
            served_tip.times.block_time_unix_seconds,
            served_tip.now_unix_seconds,
        );
        Ok(lightd_info(
            &activations,
            tip_height,
            estimated_height,
            transparent_address_support,
        ))
    }

    /// Answers `GetLatestBlock` with the served tip, its hash in internal
    /// byte order.
    pub(crate) async fn latest_block<Reads: CompactBlockReads>(
        &self,
        reads: &Reads,
    ) -> Result<lightwalletd::BlockId, Status> {
        let tip = self
            .served_tip(reads, "get_latest_block")
            .await?
            .chain_epoch;
        Ok(lightwalletd::BlockId {
            height: u64::from(tip.visible_tip_height.value()),
            hash: encode_internal_block_hash(tip.visible_tip_hash).to_vec(),
        })
    }

    /// Streams the compact blocks `request` names, shaped by `shape`.
    ///
    /// lightwalletd puts no width bound on `BlockRange`, so
    /// [`BlockRangeChunks`] splits the range into chunks read in wire order,
    /// and at most one bounded chunk is materialized at a time. The first
    /// chunk is read at the view the request's bounds were checked against,
    /// before the response, so an unreadable range fails the call rather than
    /// a stream the client has already opened. Later chunks follow
    /// [`BlockRangeContinuity`].
    ///
    /// The stream holds a slot of the block-range limit from admission until
    /// the response is dropped and no chunk read of the stream runs, even
    /// when the client cancels the call.
    pub(crate) async fn block_range<Reads: CompactBlockReads>(
        &self,
        reads: &Reads,
        request: &lightwalletd::BlockRange,
        shape: CompactBlockShape,
    ) -> Result<BlockRangeResponse<Result<lightwalletd::CompactBlock, Status>>, Status> {
        let (block_range, is_descending) = block_range_from_request(request)?;
        let stream_slot = self.block_range_streams.open().await?;
        let view = reads.capture_view().await?;
        reject_future_block_height(
            block_range.end,
            Reads::view_chain_epoch(&view).visible_tip_height,
        )?;
        let conversion = ChunkConversion {
            shape,
            is_descending,
        };
        let (first_messages, later_chunks) = stream_slot
            .hold_during(read_first_chunk(
                reads.clone(),
                view,
                BlockRangeChunks::new(block_range, is_descending),
                conversion,
            ))
            .await?;
        let Some((chunks, continuity)) = later_chunks else {
            return Ok(stream_slot.hold_for(first_messages));
        };
        let reads = reads.clone();
        Ok(stream_slot.hold_with_reader(
            first_messages,
            Reads::BLOCK_RANGE_QUEUE_CAPACITY,
            |sink| stream_later_chunks(reads, chunks, continuity, conversion, sink),
        ))
    }

    /// Captures the current view, reads the times of its tip for the tip call
    /// `method`, and refuses the tip when it is older than the stale-tip
    /// limit and the writer's node does not vouch for it.
    async fn served_tip<Reads: CompactBlockReads>(
        &self,
        reads: &Reads,
        method: &'static str,
    ) -> Result<ServedTip<Reads::View>, Status> {
        let view = reads.capture_view().await?;
        let chain_epoch = Reads::view_chain_epoch(&view);
        let tip_height = chain_epoch.visible_tip_height;
        let times = self
            .tip_block_times
            .block_time(chain_epoch.id, tip_height, || reads.tip_block_time(&view))
            .await?;
        // Sampled after the header read, which can wait for a blocking
        // thread, so the tip's age is the one it has when the call answers.
        let now_unix_seconds = (self.unix_clock)();
        self.stale_tip_limit.check(
            method,
            times.aged_from_unix_seconds,
            now_unix_seconds,
            self.node_tip_witness.vouches_for(tip_height),
        )?;
        Ok(ServedTip {
            view,
            chain_epoch,
            times,
            now_unix_seconds,
        })
    }
}

/// Answers `GetBlock` with the stored block `block_id` names, every pool
/// kept, read at one captured view.
pub(crate) async fn compact_block<Reads: CompactBlockReads>(
    reads: &Reads,
    block_id: &lightwalletd::BlockId,
) -> Result<lightwalletd::CompactBlock, Status> {
    let block = requested_block(block_id)?;
    let view = reads.capture_view().await?;
    if let RequestedBlock::Height(height) = block {
        reject_future_block_height(height, Reads::view_chain_epoch(&view).visible_tip_height)?;
    }
    let compact_block = reads.read_compact_block(&view, block).await?;
    let compact_block = compact_block_to_lightwalletd(&compact_block)?;
    if !block_id.hash.is_empty() && block_id.hash != compact_block.hash {
        return Err(Status::not_found(
            "requested block hash does not match indexed block",
        ));
    }
    Ok(compact_block)
}

/// Returns the block a lightwalletd `BlockId` names.
///
/// `BlockId { height, hash }` takes three shapes: height-only (`hash` empty),
/// hash-only (`height = 0` with a 32-byte `hash`), and height-with-hash, whose
/// hash the caller checks after its read. Height 0 with an empty hash is an
/// unspecified identifier and returns `INVALID_ARGUMENT`, as reference
/// lightwalletd does for unary block lookups.
pub(crate) fn requested_block(block_id: &lightwalletd::BlockId) -> Result<RequestedBlock, Status> {
    if block_id.height != 0 {
        return block_range_height_from_id(block_id).map(RequestedBlock::Height);
    }
    if block_id.hash.is_empty() {
        return Err(Status::invalid_argument("block identifier is unspecified"));
    }
    let hash_bytes: [u8; 32] = block_id
        .hash
        .as_slice()
        .try_into()
        .map_err(|_| Status::invalid_argument("block hash must be 32 bytes"))?;
    Ok(RequestedBlock::Hash(BlockHash::from_bytes(hash_bytes)))
}

/// Refuses a height above the visible tip with lightwalletd's
/// `OUT_OF_RANGE`.
pub(crate) fn reject_future_block_height(
    requested_height: BlockHeight,
    visible_tip_height: BlockHeight,
) -> Result<(), Status> {
    if requested_height > visible_tip_height {
        return Err(newer_than_latest_block(
            requested_height,
            visible_tip_height,
        ));
    }
    Ok(())
}

fn newer_than_latest_block(
    requested_height: BlockHeight,
    visible_tip_height: BlockHeight,
) -> Status {
    Status::out_of_range(format!(
        "block {} is newer than latest block {}",
        requested_height.value(),
        visible_tip_height.value(),
    ))
}

fn block_range_from_request(
    request: &lightwalletd::BlockRange,
) -> Result<(BlockHeightRange, bool), Status> {
    let start = request
        .start
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("range.start is required"))
        .and_then(block_range_height_from_id)?;
    let end = request
        .end
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("range.end is required"))
        .and_then(block_range_height_from_id)?;
    let is_descending = start > end;
    let block_range = if is_descending {
        BlockHeightRange::inclusive(end, start)
    } else {
        BlockHeightRange::inclusive(start, end)
    };

    Ok((block_range, is_descending))
}

fn block_range_height_from_id(block_id: &lightwalletd::BlockId) -> Result<BlockHeight, Status> {
    let height = u32::try_from(block_id.height)
        .map_err(|_| Status::invalid_argument("block height exceeds u32"))?;
    Ok(BlockHeight::new(height))
}

/// Builds `LightdInfo` for a served tip at `tip_height` while the network is
/// estimated to be at `estimated_height`.
fn lightd_info(
    activations: &NetworkUpgradeActivations,
    tip_height: BlockHeight,
    estimated_height: BlockHeight,
    transparent_address_support: bool,
) -> lightwalletd::LightdInfo {
    let consensus_branch_id = activations.active_at(tip_height).map_or_else(
        || "00000000".to_owned(),
        |activation| encode_branch_id_hex(activation.branch_id),
    );
    // The protocol defines `upgradeName` and `upgradeHeight` as the next
    // pending upgrade, not the active one.
    let next_pending = activations
        .activations()
        .iter()
        .find(|activation| activation.activation_height > tip_height);
    let upgrade_name = next_pending.map_or_default(|activation| activation.name.clone());
    let upgrade_height = next_pending.map_or(0, |activation| {
        u64::from(activation.activation_height.value())
    });
    let sapling_activation_height = activations
        .activation_height_by_name("Sapling")
        .map_or(0, |height| u64::from(height.value()));

    lightwalletd::LightdInfo {
        version: env!("CARGO_PKG_VERSION").to_owned(),
        vendor: "Zinder".to_owned(),
        taddr_support: transparent_address_support,
        chain_name: encode_bip70_chain_name(activations.network()).to_owned(),
        sapling_activation_height,
        consensus_branch_id,
        block_height: u64::from(tip_height.value()),
        git_commit: zinder_runtime::BUILD_GIT_COMMIT.to_owned(),
        branch: String::new(),
        build_date: String::new(),
        build_user: String::new(),
        estimated_height: u64::from(estimated_height.max(tip_height).value()),
        // Deliberately empty: Zinder is not zcashd; impersonating a build
        // version misleads operators inspecting the field.
        zcashd_build: String::new(),
        zcashd_subversion: String::new(),
        // Operator-configured in lightwalletd-go. Zinder has no donation
        // address config, so the empty string preserves the unset convention.
        donation_address: String::new(),
        upgrade_name,
        upgrade_height,
        lightwallet_protocol_version: LIGHTWALLETD_PROTOCOL_VERSION.to_owned(),
    }
}

/// How a block-range stream shapes and orders the blocks it sends.
#[derive(Clone, Copy)]
struct ChunkConversion {
    shape: CompactBlockShape,
    is_descending: bool,
}

/// The messages of one chunk, as the response stream yields them. Each is
/// `Ok`, because a chunk that cannot be converted fails whole.
type ChunkMessages = Vec<Result<lightwalletd::CompactBlock, Status>>;

/// The chunks after the first, and the continuity the next must follow.
type LaterChunks = Option<(BlockRangeChunks, BlockRangeContinuity)>;

impl ChunkConversion {
    /// Converts a chunk's blocks, in ascending height order, to the messages
    /// the stream sends, in its order.
    ///
    /// # Errors
    ///
    /// Returns the status of the first block that cannot be converted, and
    /// no message, so a stream never sends part of a chunk.
    fn messages(self, mut blocks: Vec<CompactBlockArtifact>) -> Result<ChunkMessages, Status> {
        if self.is_descending {
            blocks.reverse();
        }
        blocks
            .iter()
            .map(|block| self.shape.message(block).map(Ok))
            .collect()
    }
}

/// How long one chunk spent reading and converting, for the stages that
/// ran.
///
/// The read and the conversion measure their stages where they run, and the
/// stream's task records them, so the samples reach the recorder of the task
/// that serves the stream.
#[derive(Clone, Copy, Debug, Default)]
struct ChunkTimings {
    read: Option<Duration>,
    convert: Option<Duration>,
}

impl ChunkTimings {
    /// Runs `convert` and keeps how long it took as the convert stage.
    fn time_convert<Output>(&mut self, convert: impl FnOnce() -> Output) -> Output {
        let started_at = Instant::now();
        let output = convert();
        self.convert = Some(started_at.elapsed());
        output
    }

    fn record(self) {
        if let Some(read) = self.read {
            BlockRangeStage::Read.record(read);
        }
        if let Some(convert) = self.convert {
            BlockRangeStage::Convert.record(convert);
        }
    }
}

/// Reads and converts the first chunk of a range at `view`, the view the
/// request's bounds were checked against.
///
/// When heights remain after the blocks the read kept, also returns the plan
/// and the continuity that later chunks must follow. The caller runs this
/// under [`crate::block_range::BlockRangeStreamSlot::hold_during`], because
/// the read and its conversion finish on the blocking pool even when the
/// client cancels the request.
async fn read_first_chunk<Reads: CompactBlockReads>(
    reads: Reads,
    view: Reads::View,
    mut chunks: BlockRangeChunks,
    conversion: ChunkConversion,
) -> Result<(ChunkMessages, LaterChunks), Status> {
    let Some(chunk) = chunks.next_chunk() else {
        return Ok((Vec::new(), None));
    };
    let read = ChunkRead {
        chunk,
        order: chunks.order(),
        view: ChunkView::Captured(view),
    };
    read_chunk(&reads, read, move |read_at, blocks, timings| {
        chunks.advance(blocks.len());
        let later_chunks = chunks
            .has_remaining()
            .then(|| {
                BlockRangeContinuity::after_first_chunk(
                    &blocks,
                    read_at.id,
                    conversion.is_descending,
                )
                .map(|continuity| (chunks, continuity))
            })
            .transpose()?;
        let messages = timings.time_convert(|| conversion.messages(blocks))?;
        Ok((messages, later_chunks))
    })
    .await
    .map_err(|failure| match failure {
        ChunkReadFailure::TipBelowChunk(read_at) => {
            newer_than_latest_block(chunk.end, read_at.visible_tip_height)
        }
        ChunkReadFailure::Failed(status) => status,
    })
}

/// Streams every chunk after the first, each read at the view current when
/// it is read.
///
/// The task holds no view while it waits for a read or for the client, so a
/// slow client cannot keep a serving pair or a generation from being
/// replaced. A read failure, a reorg ending, or a chunk that cannot be
/// converted ends the stream with that status, and a client that stops
/// draining ends it on the next send.
///
/// Before it reads the third chunk, the task waits until the client has
/// taken every message of the first, so a stream reads at most two chunks
/// ahead of a client that has not started, however few blocks each holds.
async fn stream_later_chunks<Reads: CompactBlockReads>(
    reads: Reads,
    mut chunks: BlockRangeChunks,
    mut continuity: BlockRangeContinuity,
    conversion: ChunkConversion,
    mut sink: BlockRangeSink<Result<lightwalletd::CompactBlock, Status>>,
) {
    while let Some(chunk) = chunks.next_chunk() {
        let messages = match read_later_chunk(&reads, chunk, chunks, continuity, conversion).await {
            Ok((next_chunks, next_continuity, messages)) => {
                chunks = next_chunks;
                continuity = next_continuity;
                messages
            }
            Err(status) => {
                let _ = sink.send(Err(status)).await;
                return;
            }
        };
        let sent_chunk = BlockRangeStage::Send
            .time_async(async {
                for message in messages {
                    if sink.send(message).await.is_err() {
                        return false;
                    }
                }
                sink.first_chunk_taken().await
            })
            .await;
        if !sent_chunk {
            return;
        }
    }
}

/// Reads `chunk`, the next chunk of `chunks`, at the view current when the
/// read runs.
///
/// The chunk is accepted only when it continues the chain the stream has
/// sent, and is converted only then. The tip check and the hash link run
/// where the chunk is read, and their outcome is recorded here, on the
/// stream's task. Returns the plan and the continuity moved past the chunk,
/// with its messages.
async fn read_later_chunk<Reads: CompactBlockReads>(
    reads: &Reads,
    chunk: BlockHeightRange,
    mut chunks: BlockRangeChunks,
    mut continuity: BlockRangeContinuity,
    conversion: ChunkConversion,
) -> Result<(BlockRangeChunks, BlockRangeContinuity, ChunkMessages), Status> {
    let read = ChunkRead {
        chunk,
        order: chunks.order(),
        view: ChunkView::Current,
    };
    let sent = continuity;
    let (link, kept_blocks, messages) = read_chunk(reads, read, move |read_at, blocks, timings| {
        let link = sent.link(chunk, read_at.id, &blocks);
        let kept_blocks = blocks.len();
        let messages = if link.continues() {
            timings.time_convert(|| conversion.messages(blocks))?
        } else {
            Vec::new()
        };
        Ok((link, kept_blocks, messages))
    })
    .await
    .map_err(|failure| match failure {
        ChunkReadFailure::TipBelowChunk(read_at) => continuity.end_below_tip(chunk, &read_at),
        ChunkReadFailure::Failed(status) => status,
    })?;
    continuity.accept(link)?;
    chunks.advance(kept_blocks);
    Ok((chunks, continuity, messages))
}

/// Reads one chunk and hands its blocks to `convert` on the blocking pool,
/// then records how long each stage that ran took.
///
/// The read stage is the storage read the serving mode reports, and is
/// recorded for a read that fails too.
async fn read_chunk<Reads, Output>(
    reads: &Reads,
    read: ChunkRead<Reads::View>,
    convert: impl FnOnce(
        ChainEpoch,
        Vec<CompactBlockArtifact>,
        &mut ChunkTimings,
    ) -> Result<Output, Status>
    + Send
    + 'static,
) -> Result<Output, ChunkReadFailure>
where
    Reads: CompactBlockReads,
    Output: Send + 'static,
{
    let outcome = reads
        .read_compact_block_chunk(read, move |read_at, blocks| {
            let mut timings = ChunkTimings::default();
            let output = convert(read_at, blocks, &mut timings);
            (output, timings.convert)
        })
        .await;
    let mut timings = ChunkTimings {
        read: outcome.read_duration,
        convert: None,
    };
    let converted = outcome.converted.map(|(output, convert)| {
        timings.convert = convert;
        output
    });
    timings.record();
    converted?.map_err(ChunkReadFailure::Failed)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicI64, Ordering},
        },
        time::Duration,
    };

    use tonic::{Code, Status};
    use zinder_core::{
        ArtifactSchemaVersion, BlockHash, BlockHeight, ChainEpoch, ChainEpochId, ChainTipMetadata,
        CompactBlockArtifact, Network, NetworkUpgradeActivations, UnixTimestampMillis,
        wire::encode_branch_id_hex,
    };

    use super::{
        BlockMethods, ChunkRead, ChunkReadFailure, ChunkReadOutcome, CompactBlockReads,
        RequestedBlock, lightd_info,
    };
    use crate::stale_tip::StaleTipLimit;

    /// The block time in the header of the served tip in the slow-header
    /// test.
    const TIP_BLOCK_TIME: i64 = 1_700_000_000;

    /// Reads whose tip header read lasts while `clock` advances from one
    /// minute after [`TIP_BLOCK_TIME`] to 30 minutes after it.
    #[derive(Clone)]
    struct SlowTipHeaderReads {
        clock: Arc<AtomicI64>,
    }

    impl SlowTipHeaderReads {
        fn new() -> Self {
            Self {
                clock: Arc::new(AtomicI64::new(TIP_BLOCK_TIME + 60)),
            }
        }

        /// Block methods that refuse a tip older than 25 minutes, judged at
        /// this reads' clock.
        fn methods(&self) -> BlockMethods {
            let clock = Arc::clone(&self.clock);
            BlockMethods {
                stale_tip_limit: StaleTipLimit::after(Duration::from_mins(25)),
                ..BlockMethods::default()
            }
            .with_unix_clock(move || clock.load(Ordering::SeqCst))
        }
    }

    impl CompactBlockReads for SlowTipHeaderReads {
        type View = ChainEpoch;

        const BLOCK_RANGE_QUEUE_CAPACITY: usize = 1;

        fn capture_view(&self) -> impl Future<Output = Result<ChainEpoch, Status>> + Send {
            std::future::ready(Ok(ChainEpoch {
                id: ChainEpochId::new(1),
                network: Network::ZcashRegtest,
                visible_tip_height: BlockHeight::new(1),
                visible_tip_hash: BlockHash::from_bytes([1; 32]),
                settled_tip_height: BlockHeight::new(1),
                settled_tip_hash: BlockHash::from_bytes([1; 32]),
                artifact_schema_version: ArtifactSchemaVersion::new(1),
                tip_metadata: ChainTipMetadata::new(0, 0, 0),
                created_at: UnixTimestampMillis::new(0),
            }))
        }

        fn view_chain_epoch(view: &ChainEpoch) -> ChainEpoch {
            *view
        }

        fn view_activations(
            &self,
            _view: &ChainEpoch,
        ) -> impl Future<Output = Result<NetworkUpgradeActivations, Status>> + Send {
            std::future::ready(Ok(zinder_testkit::sample_regtest_upgrade_activations()))
        }

        fn tip_block_time(
            &self,
            _view: &ChainEpoch,
        ) -> impl Future<Output = Result<i64, Status>> + Send {
            let clock = Arc::clone(&self.clock);
            async move {
                clock.store(TIP_BLOCK_TIME + 30 * 60, Ordering::SeqCst);
                Ok(TIP_BLOCK_TIME)
            }
        }

        fn read_compact_block(
            &self,
            _view: &ChainEpoch,
            _block: RequestedBlock,
        ) -> impl Future<Output = Result<CompactBlockArtifact, Status>> + Send {
            std::future::ready(Err(Status::unimplemented("the test serves no block")))
        }

        fn read_compact_block_chunk<Output, Convert>(
            &self,
            _read: ChunkRead<ChainEpoch>,
            _convert: Convert,
        ) -> impl Future<Output = ChunkReadOutcome<Output>> + Send
        where
            Output: Send + 'static,
            Convert: FnOnce(ChainEpoch, Vec<CompactBlockArtifact>) -> Output + Send + 'static,
        {
            std::future::ready(ChunkReadOutcome::unread(ChunkReadFailure::Failed(
                Status::unimplemented("the test serves no block range"),
            )))
        }
    }

    /// A tip call judges the tip's age when its header read ends, not when
    /// the call starts.
    ///
    /// The clock shows the tip one minute old when each call starts and
    /// advances past the 25-minute stale-tip limit while the call reads the
    /// tip's header.
    #[tokio::test]
    async fn a_tip_call_judges_the_tip_age_after_its_header_read() {
        let latest_block_reads = SlowTipHeaderReads::new();
        let latest_block = latest_block_reads
            .methods()
            .latest_block(&latest_block_reads)
            .await;
        assert_eq!(
            latest_block.err().map(|status| status.code()),
            Some(Code::Unavailable),
            "GetLatestBlock must see the tip 30 minutes old when its header read ends"
        );

        let lightd_info_reads = SlowTipHeaderReads::new();
        let lightd_info_response = lightd_info_reads
            .methods()
            .lightd_info(&lightd_info_reads, true)
            .await;
        assert_eq!(
            lightd_info_response.err().map(|status| status.code()),
            Some(Code::Unavailable),
            "GetLightdInfo must see the tip 30 minutes old when its header read ends"
        );
    }

    #[test]
    fn lightd_info_reports_the_active_branch_and_the_next_pending_upgrade() {
        let activations = zinder_testkit::sample_regtest_upgrade_activations();
        let branch_hex = |name: &str| {
            activations
                .activations()
                .iter()
                .find(|activation| activation.name == name)
                .map(|activation| encode_branch_id_hex(activation.branch_id))
        };

        let before_nu5 = lightd_info(&activations, BlockHeight::new(1), BlockHeight::new(1), true);
        assert_eq!(Some(before_nu5.consensus_branch_id), branch_hex("Canopy"));
        assert_eq!(before_nu5.upgrade_name, "NU5");
        assert_eq!(before_nu5.upgrade_height, 2);

        let before_nu6_3 = lightd_info(
            &activations,
            BlockHeight::new(602),
            BlockHeight::new(602),
            true,
        );
        assert_eq!(Some(before_nu6_3.consensus_branch_id), branch_hex("NU6.2"));
        assert_eq!(before_nu6_3.upgrade_name, "NU6.3");
        assert_eq!(before_nu6_3.upgrade_height, 603);

        let before_nu7 = lightd_info(
            &activations,
            BlockHeight::new(603),
            BlockHeight::new(603),
            true,
        );
        assert_eq!(Some(before_nu7.consensus_branch_id), branch_hex("NU6.3"));
        assert_eq!(before_nu7.upgrade_name, "NU7");
        assert_eq!(before_nu7.upgrade_height, 703);

        let none_pending = lightd_info(
            &activations,
            BlockHeight::new(703),
            BlockHeight::new(703),
            true,
        );
        assert_eq!(Some(none_pending.consensus_branch_id), branch_hex("NU7"));
        assert_eq!(none_pending.upgrade_name, "");
        assert_eq!(none_pending.upgrade_height, 0);
    }

    #[test]
    fn lightd_info_reports_the_shared_build_identity() {
        let activations = zinder_testkit::sample_regtest_upgrade_activations();
        let info = lightd_info(&activations, BlockHeight::new(1), BlockHeight::new(1), true);

        assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(info.git_commit, zinder_runtime::BUILD_GIT_COMMIT);
    }
}
