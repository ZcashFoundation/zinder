//! Two-generation secondary publishers, swappable wallet-serving query, and
//! supervised single-server lifecycle.

use std::{
    collections::VecDeque,
    net::SocketAddr,
    num::{NonZeroU16, NonZeroU32},
    path::{Path, PathBuf},
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::{
    net::TcpListener,
    sync::{oneshot, watch},
    task::JoinHandle,
};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use zinder_compat_lightwalletd::{
    CanonicalReaderSlot, LightwalletdGrpcAdapter, SharedMempoolSurface, SharedTipChangeWatcher,
    TipWait,
};
use zinder_core::{
    BlockHeight, BlockHeightRange, BlockSelector, CanonicalHistoryBounds, ChainEpoch, ChainEpochId,
    ChainValuePoolsAtTip, Network, NetworkUpgradeActivations, RawTransactionBytes, StandardFee,
    SubtreeRootRange, TransactionBroadcastOutcome, TransactionId, TransparentAddressBalance,
    TransparentAddressScriptHash, TransparentOutPoint,
};
use zinder_query::{
    BlockHeaderAtEpoch, BlockIdAtEpoch, CanonicalReader as _, ChainEvents, CompactBlock,
    CompactBlockRange, CompactBlockStream, ForwardedReadLimit, FullBlock, FullBlockStream,
    NativeWalletEndpointCapabilities, QueryError, RawTransaction, RocksDbCanonicalReader,
    RocksDbWalletReader, ServedTipSnapshot, ServedTreeStateHex, SettledTipBlock, StorageReadError,
    SubtreeRoots, Transaction, TransactionStatus, TransparentAddressTxIds,
    TransparentAddressTxIdsInRangeRequest, TransparentAddressUnspentOutputs,
    TransparentAddressUnspentOutputsRequest, TransparentOutpointSpendStatuses, TreeState,
    UpstreamNodeCapabilities, VisibleTipBlock, WalletQueryApi, WalletServingPairSlot,
    WalletServingQuery, WalletServingReadPair,
};
use zinder_source::{NodeAuth, ZebraJsonRpcSource};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreError, CanonicalStoreRequirements, ChainEventStreamFamily,
    ChainEventStreamResume, EncodedCompactBlockRange, EncodedCompactBlockRangeRequest,
    EventStreamStartPosition, HeightOrder, RawBlobRetention, RocksDbCanonicalSecondary,
    RocksDbResourceBudget, SharedSecondaryResources, StreamCursorTokenV1,
};
use zinder_wallet_rocksdb::{RocksDbWalletError, RocksDbWalletSecondary};

use super::report::{StoreTickerDelta, TickerCounterSnapshot};
use crate::BenchError;

const TWO_GENERATION_SLOTS: usize = 2;
const WALLET_GENERATION_SLOTS: usize = 4;
const MAX_RETAINED_WALLET_GENERATIONS: usize = 2;
const MAX_MISSING_TABLE_RETRIES: usize = 8;

/// Retries [`RocksDbCanonicalSecondary::try_catch_up`] and
/// [`RocksDbCanonicalSecondary::chain_epoch`] while writer compaction deletes SST
/// files referenced by the closing manifest.
pub(crate) fn catch_up_canonical_secondary(
    sec: &mut RocksDbCanonicalSecondary,
) -> Result<ChainEpoch, CanonicalStoreError> {
    for attempt in 0..=MAX_MISSING_TABLE_RETRIES {
        match sec.try_catch_up().and_then(|_| sec.chain_epoch()) {
            Ok(epoch) => return Ok(epoch),
            Err(err) if err.is_missing_table_file() && attempt < MAX_MISSING_TABLE_RETRIES => {
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(err) => return Err(err),
        }
    }
    sec.try_catch_up().and_then(|_| sec.chain_epoch())
}

/// Retries [`RocksDbWalletSecondary::try_catch_up`] while projector compaction
/// deletes SST files referenced by the closing manifest.
pub(crate) fn catch_up_wallet_secondary(
    sec: &mut RocksDbWalletSecondary,
) -> Result<(), RocksDbWalletError> {
    for attempt in 0..=MAX_MISSING_TABLE_RETRIES {
        match sec.try_catch_up() {
            Ok(_) => return Ok(()),
            Err(err) if err.is_missing_table_file() && attempt < MAX_MISSING_TABLE_RETRIES => {
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(err) => return Err(err),
        }
    }
    sec.try_catch_up().map(|_| ())
}

/// Returns whether a query read or reader construction failed because writer or
/// projector compaction removed an SST file needed by a stale secondary view.
pub(crate) const fn is_storage_read_invalidated(err: &StorageReadError) -> bool {
    matches!(err, StorageReadError::ReaderInvalidated { .. })
}

struct PublishedWalletViews {
    current_epoch: ChainEpochId,
    current_query: WalletServingQuery<()>,
    current_pair: Arc<WalletServingReadPair>,
    retained: VecDeque<(ChainEpochId, WalletServingQuery<()>)>,
}

/// Replaceable [`WalletQueryApi`] delegating across bounded generations.
///
/// Serves each request from the latest [`WalletServingQuery`] (or a retained
/// previous generation when pinned by `ChainEpochId`) and notifies tip-wait
/// subscribers.
#[derive(Clone)]
pub(crate) struct SwappableWalletQuery {
    capabilities: NativeWalletEndpointCapabilities,
    views: Arc<Mutex<PublishedWalletViews>>,
    publications: Arc<watch::Sender<()>>,
}

impl SwappableWalletQuery {
    pub(crate) fn new(
        first: WalletServingQuery<()>,
        initial_pair: Arc<WalletServingReadPair>,
    ) -> Self {
        let current_epoch = initial_pair.chain_epoch().id;
        Self {
            capabilities: first.native_endpoint_capabilities().clone(),
            views: Arc::new(Mutex::new(PublishedWalletViews {
                current_epoch,
                current_query: first,
                current_pair: initial_pair,
                retained: VecDeque::with_capacity(MAX_RETAINED_WALLET_GENERATIONS),
            })),
            publications: Arc::new(watch::Sender::new(())),
        }
    }

    pub(crate) fn publish(
        &self,
        next: WalletServingQuery<()>,
        next_pair: Arc<WalletServingReadPair>,
        max_retained: usize,
    ) {
        let (prev_pair, evicted) = swap_published_views(&self.views, next, next_pair, max_retained);
        drop(prev_pair);
        drop(evicted);
        self.publications.send_replace(());
    }

    pub(crate) fn clear_retained(&self) {
        let drained: Vec<_> = self.views.lock().retained.drain(..).collect();
        drop(drained);
    }

    pub(crate) fn capture_pair(&self) -> Arc<WalletServingReadPair> {
        Arc::clone(&self.views.lock().current_pair)
    }

    fn now(&self) -> WalletServingQuery<()> {
        self.views.lock().current_query.clone()
    }

    fn for_epoch(&self, at_epoch_id: Option<ChainEpochId>) -> WalletServingQuery<()> {
        let guard = self.views.lock();
        let target_opt = at_epoch_id.filter(|id| *id != guard.current_epoch);
        let selected = target_opt
            .and_then(|target| {
                guard
                    .retained
                    .iter()
                    .find(|(epoch, _)| *epoch == target)
                    .map(|(_, query)| query.clone())
            })
            .unwrap_or_else(|| guard.current_query.clone());
        drop(guard);
        selected
    }
}

fn swap_published_views(
    views: &Mutex<PublishedWalletViews>,
    next: WalletServingQuery<()>,
    next_pair: Arc<WalletServingReadPair>,
    max_retained: usize,
) -> (Arc<WalletServingReadPair>, Vec<WalletServingQuery<()>>) {
    let next_epoch = next_pair.chain_epoch().id;
    let mut evicted = Vec::new();
    let mut guard = views.lock();
    let prev_epoch = guard.current_epoch;
    let prev_query = std::mem::replace(&mut guard.current_query, next);
    let prev_pair = std::mem::replace(&mut guard.current_pair, next_pair);
    guard.current_epoch = next_epoch;
    if max_retained > 0 && next_epoch != prev_epoch {
        for dup_epoch in [next_epoch, prev_epoch] {
            if let Some(pos) = guard.retained.iter().position(|(ep, _)| *ep == dup_epoch)
                && let Some((_, dup)) = guard.retained.remove(pos)
            {
                evicted.push(dup);
            }
        }
        guard.retained.push_front((prev_epoch, prev_query));
        while guard.retained.len() > max_retained {
            if let Some((_, dropped)) = guard.retained.pop_back() {
                evicted.push(dropped);
            }
        }
    } else {
        if max_retained == 0 {
            evicted.extend(guard.retained.drain(..).map(|(_, q)| q));
        }
        evicted.push(prev_query);
    }
    drop(guard);
    (prev_pair, evicted)
}

#[async_trait]
impl WalletQueryApi for SwappableWalletQuery {
    async fn encoded_compact_block_range_up_to(
        &self,
        request: EncodedCompactBlockRangeRequest,
        at_epoch_id: Option<ChainEpochId>,
        cancel: Option<Arc<AtomicBool>>,
    ) -> Result<Option<EncodedCompactBlockRange>, QueryError> {
        self.for_epoch(at_epoch_id)
            .encoded_compact_block_range_up_to(request, at_epoch_id, cancel)
            .await
    }

    fn subscribe_publications(&self) -> Option<watch::Receiver<()>> {
        Some(self.publications.subscribe())
    }

    fn current_visible_tip_height(&self) -> Option<BlockHeight> {
        self.now().current_visible_tip_height()
    }

    fn current_chain_epoch(&self) -> Option<ChainEpoch> {
        self.now().current_chain_epoch()
    }

    fn canonical_history_bounds(&self) -> Option<CanonicalHistoryBounds> {
        self.now().canonical_history_bounds()
    }

    fn native_endpoint_capabilities(&self) -> &NativeWalletEndpointCapabilities {
        &self.capabilities
    }

    fn upstream_node_capabilities(&self) -> Option<UpstreamNodeCapabilities> {
        self.now().upstream_node_capabilities()
    }

    async fn network_upgrade_activations(&self) -> Result<NetworkUpgradeActivations, QueryError> {
        self.now().network_upgrade_activations().await
    }

    async fn chain_value_pools_at_tip(&self) -> Result<ChainValuePoolsAtTip, QueryError> {
        self.now().chain_value_pools_at_tip().await
    }

    async fn visible_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<VisibleTipBlock, QueryError> {
        self.for_epoch(at_epoch_id)
            .visible_tip_block(at_epoch_id)
            .await
    }

    async fn served_tip_snapshot(&self) -> Result<ServedTipSnapshot, QueryError> {
        self.now().served_tip_snapshot().await
    }

    async fn settled_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<SettledTipBlock, QueryError> {
        self.for_epoch(at_epoch_id)
            .settled_tip_block(at_epoch_id)
            .await
    }

    async fn block_id_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockIdAtEpoch, QueryError> {
        self.for_epoch(at_epoch_id)
            .block_id_by_selector(selector, at_epoch_id)
            .await
    }

    async fn block_header_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockHeaderAtEpoch, QueryError> {
        self.for_epoch(at_epoch_id)
            .block_header_by_selector(selector, at_epoch_id)
            .await
    }

    async fn compact_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlock, QueryError> {
        self.for_epoch(at_epoch_id)
            .compact_block_at(height, at_epoch_id)
            .await
    }

    async fn encoded_compact_block_at(
        &self,
        height: BlockHeight,
        selection: zinder_store::CompactBlockSpliceSelection,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<Option<zinder_query::EncodedCompactBlock>, QueryError> {
        self.for_epoch(at_epoch_id)
            .encoded_compact_block_at(height, selection, at_epoch_id)
            .await
    }

    async fn compact_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockRange, QueryError> {
        self.for_epoch(at_epoch_id)
            .compact_blocks_in_range(block_range, at_epoch_id)
            .await
    }

    async fn compact_block_stream(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockStream, QueryError> {
        self.for_epoch(at_epoch_id)
            .compact_block_stream(block_range, at_epoch_id)
            .await
    }

    async fn compact_blocks_in_range_up_to(
        &self,
        block_range: BlockHeightRange,
        order: HeightOrder,
        max_bytes: usize,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockRange, QueryError> {
        self.for_epoch(at_epoch_id)
            .compact_blocks_in_range_up_to(block_range, order, max_bytes, at_epoch_id)
            .await
    }

    async fn full_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<FullBlock, QueryError> {
        self.for_epoch(at_epoch_id)
            .full_block_at(height, at_epoch_id)
            .await
    }

    async fn full_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<FullBlockStream, QueryError> {
        self.for_epoch(at_epoch_id)
            .full_blocks_in_range(block_range, at_epoch_id)
            .await
    }

    async fn transaction(
        &self,
        transaction_id: TransactionId,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransactionStatus, QueryError> {
        self.for_epoch(at_epoch_id)
            .transaction(transaction_id, at_epoch_id)
            .await
    }

    async fn transactions_in_order(
        &self,
        transaction_ids: Vec<TransactionId>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<zinder_query::TransactionsInOrder, QueryError>
    where
        Self: Sized + Clone,
    {
        self.for_epoch(at_epoch_id)
            .transactions_in_order(transaction_ids, at_epoch_id)
            .await
    }

    async fn transaction_at_block_index(
        &self,
        height: BlockHeight,
        tx_index: u64,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<Transaction, QueryError> {
        self.for_epoch(at_epoch_id)
            .transaction_at_block_index(height, tx_index, at_epoch_id)
            .await
    }

    async fn raw_transaction(
        &self,
        transaction_id: TransactionId,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<RawTransaction, QueryError> {
        self.for_epoch(at_epoch_id)
            .raw_transaction(transaction_id, at_epoch_id)
            .await
    }

    async fn transparent_address_unspent_outputs(
        &self,
        request: TransparentAddressUnspentOutputsRequest,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressUnspentOutputs, QueryError> {
        self.for_epoch(at_epoch_id)
            .transparent_address_unspent_outputs(request, at_epoch_id)
            .await
    }

    async fn transparent_address_unspent_outputs_on_demand(
        &self,
        request: TransparentAddressUnspentOutputsRequest,
        first_page_size: NonZeroU16,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressUnspentOutputs, QueryError> {
        self.for_epoch(at_epoch_id)
            .transparent_address_unspent_outputs_on_demand(request, first_page_size, at_epoch_id)
            .await
    }

    async fn transparent_address_tx_ids_in_range(
        &self,
        request: TransparentAddressTxIdsInRangeRequest,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressTxIds, QueryError> {
        self.for_epoch(at_epoch_id)
            .transparent_address_tx_ids_in_range(request, at_epoch_id)
            .await
    }

    async fn transparent_outpoint_spend_status(
        &self,
        outpoints: Vec<TransparentOutPoint>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentOutpointSpendStatuses, QueryError> {
        self.for_epoch(at_epoch_id)
            .transparent_outpoint_spend_status(outpoints, at_epoch_id)
            .await
    }

    async fn transparent_address_balance(
        &self,
        addresses: Vec<TransparentAddressScriptHash>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressBalance, QueryError> {
        self.for_epoch(at_epoch_id)
            .transparent_address_balance(addresses, at_epoch_id)
            .await
    }

    async fn tree_state_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeState, QueryError> {
        self.for_epoch(at_epoch_id)
            .tree_state_at(height, at_epoch_id)
            .await
    }

    async fn tree_state_hex_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<ServedTreeStateHex, QueryError> {
        self.for_epoch(at_epoch_id)
            .tree_state_hex_at(height, at_epoch_id)
            .await
    }

    async fn latest_tree_state_checkpoint(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeState, QueryError> {
        self.for_epoch(at_epoch_id)
            .latest_tree_state_checkpoint(at_epoch_id)
            .await
    }

    async fn latest_tree_state_checkpoint_hex(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<ServedTreeStateHex, QueryError> {
        self.for_epoch(at_epoch_id)
            .latest_tree_state_checkpoint_hex(at_epoch_id)
            .await
    }

    async fn subtree_roots(
        &self,
        subtree_root_range: SubtreeRootRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<SubtreeRoots, QueryError> {
        self.for_epoch(at_epoch_id)
            .subtree_roots(subtree_root_range, at_epoch_id)
            .await
    }

    async fn chain_events(
        &self,
        from_cursor: Option<StreamCursorTokenV1>,
        family: ChainEventStreamFamily,
    ) -> Result<ChainEvents, QueryError> {
        self.now().chain_events(from_cursor, family).await
    }

    async fn resolve_chain_events_start(
        &self,
        start: EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, QueryError> {
        self.now()
            .resolve_chain_events_start(start, requested_family)
            .await
    }

    async fn broadcast_transaction(
        &self,
        raw_transaction: RawTransactionBytes,
    ) -> Result<TransactionBroadcastOutcome, QueryError> {
        self.now().broadcast_transaction(raw_transaction).await
    }

    async fn standard_fee(&self) -> Result<StandardFee, QueryError> {
        self.now().standard_fee().await
    }
}

/// Strictly 2-generation canonical secondary publisher for `ingest_active`
/// (serving at most 1 active generation and holding at most 1 retired generation).
pub(crate) struct TwoGenCanonicalPublisher {
    slot: CanonicalReaderSlot,
    _resources: SharedSecondaryResources,
    state: Mutex<TwoGenCanonicalState>,
    invalidation_requested: AtomicBool,
    pub(crate) io_modes: Vec<String>,
}

struct TwoGenCanonicalState {
    standby: Option<RocksDbCanonicalSecondary>,
    retired: Option<Arc<RocksDbCanonicalReader>>,
    baseline: TickerCounterSnapshot,
    generation_seq: u64,
}

impl TwoGenCanonicalPublisher {
    /// Opens 2 canonical secondary instances on a shared bounded cache.
    pub(crate) fn open(
        canonical_dir: &Path,
        secondary_dirs: &[PathBuf],
        reorg_window_blocks: u32,
        budget: RocksDbResourceBudget,
    ) -> Result<Arc<Self>, BenchError> {
        if secondary_dirs.len() < TWO_GENERATION_SLOTS {
            return Err(BenchError::invalid_argument(
                "two-generation canonical publisher requires 2 secondary directories",
            ));
        }
        let resources = SharedSecondaryResources::new(budget);
        let reorg_policy = CanonicalReorgPolicy::new(reorg_window_blocks)?;
        let requirements = CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::All);

        let mut secondaries = Vec::with_capacity(TWO_GENERATION_SLOTS);
        let mut io_modes = Vec::with_capacity(TWO_GENERATION_SLOTS);
        for dir in &secondary_dirs[..TWO_GENERATION_SLOTS] {
            let sec = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
                canonical_dir,
                dir,
                Network::ZcashRegtest,
                requirements.clone(),
                reorg_policy,
                &resources,
            )?;
            io_modes.push(sec.io_mode().as_str().to_owned());
            secondaries.push(sec);
        }

        let active_sec = secondaries.remove(0);
        let standby = secondaries.remove(0);
        let reader = RocksDbCanonicalReader::new(active_sec)
            .map_err(|err| BenchError::canonical_read(err.to_string()))?;
        let baseline = capture_canonical_sec_tickers(reader.secondary());
        let slot = CanonicalReaderSlot::new(Arc::new(reader));

        Ok(Arc::new(Self {
            slot,
            _resources: resources,
            state: Mutex::new(TwoGenCanonicalState {
                standby: Some(standby),
                retired: None,
                baseline,
                generation_seq: 1,
            }),
            invalidation_requested: AtomicBool::new(false),
            io_modes,
        }))
    }

    pub(crate) fn capture_slot(&self) -> CanonicalReaderSlot {
        self.slot.clone()
    }

    pub(crate) fn request_rotation(&self) {
        self.invalidation_requested.store(true, Ordering::Release);
    }

    pub(crate) fn has_rotation_request(&self) -> bool {
        self.invalidation_requested.load(Ordering::Acquire)
    }

    /// Rotates a fresh generation cleanly to the current canonical tip.
    pub(crate) async fn force_rotate_to_latest(self: &Arc<Self>) -> Result<(), BenchError> {
        let this = Arc::clone(self);
        tokio::task::spawn_blocking(move || this.catch_up_blocking(true).map(|_| ()))
            .await
            .map_err(|err| BenchError::tip_replay(err.to_string()))?
    }

    /// Catches up the standby secondary and swaps it into `CanonicalReaderSlot`
    /// once any previous retired generation has drained.
    pub(crate) async fn try_catch_up_and_publish(
        self: &Arc<Self>,
    ) -> Result<(u32, ChainEpoch, u64), BenchError> {
        let this = Arc::clone(self);
        tokio::task::spawn_blocking(move || this.catch_up_blocking(false))
            .await
            .map_err(|err| BenchError::tip_replay(err.to_string()))?
    }

    /// Swaps to a standby generation even at an unchanged epoch (used by
    /// `canonical_quiescent_control` so cold tail refills start on a fresh
    /// `CompactBlockSource`).
    pub(crate) async fn rotate_generation_for_cold_tail(
        self: &Arc<Self>,
        cancel: &CancellationToken,
    ) -> Result<(u32, ChainEpoch, u64), BenchError> {
        let this = Arc::clone(self);
        let stop = cancel.clone();
        tokio::task::spawn_blocking(move || {
            let start_seq = this.state.lock().generation_seq;
            let mut last = {
                let epoch = this.slot.capture().chain_epoch();
                (epoch.visible_tip_height.value(), epoch, 0)
            };
            while !stop.is_cancelled() {
                let res = this.catch_up_blocking(true)?;
                if res.2 > start_seq {
                    return Ok(res);
                }
                last = (res.0, res.1, 0);
                std::thread::sleep(Duration::from_millis(2));
            }
            Ok(last)
        })
        .await
        .map_err(|err| BenchError::tip_replay(err.to_string()))?
    }

    /// Captures the cumulative `RocksDB` ticker delta on the shared canonical
    /// pool statistics and resets the baseline to the current instant.
    pub(crate) fn snapshot_and_reset_tickers(&self) -> StoreTickerDelta {
        let mut guard = self.state.lock();
        reclaim_canonical_retired(&mut guard);
        let active_reader = self.slot.capture();
        let now = capture_canonical_sec_tickers(active_reader.secondary());
        let delta = now.delta_since(&guard.baseline);
        guard.baseline = now;
        drop(guard);
        delta
    }

    pub(crate) async fn wait_reclaim_all(self: &Arc<Self>) {
        let this = Arc::clone(self);
        let _ = tokio::task::spawn_blocking(move || {
            for _ in 0..200 {
                let mut guard = this.state.lock();
                reclaim_canonical_retired(&mut guard);
                if guard.retired.is_none() {
                    return;
                }
                drop(guard);
                std::thread::sleep(Duration::from_millis(5));
            }
        })
        .await;
    }

    fn catch_up_blocking(&self, force_rotate: bool) -> Result<(u32, ChainEpoch, u64), BenchError> {
        let should_force = force_rotate || self.invalidation_requested.load(Ordering::Acquire);
        let (mut standby, current_epoch, current_seq) = {
            let mut guard = self.state.lock();
            reclaim_canonical_retired(&mut guard);
            let active = self.slot.capture();
            let current_epoch = active.chain_epoch();
            if guard.retired.is_some() {
                return Ok((
                    current_epoch.visible_tip_height.value(),
                    current_epoch,
                    guard.generation_seq,
                ));
            }
            let Some(standby) = guard.standby.take() else {
                return Ok((
                    current_epoch.visible_tip_height.value(),
                    current_epoch,
                    guard.generation_seq,
                ));
            };
            (standby, current_epoch, guard.generation_seq)
        };

        let caught_up_epoch = match catch_up_canonical_secondary(&mut standby) {
            Ok(epoch) => epoch,
            Err(err) => {
                let missing_table = err.is_missing_table_file();
                self.state.lock().standby = Some(standby);
                if missing_table {
                    return Ok((
                        current_epoch.visible_tip_height.value(),
                        current_epoch,
                        current_seq,
                    ));
                }
                return Err(err.into());
            }
        };
        if !should_force && caught_up_epoch.id == current_epoch.id {
            self.state.lock().standby = Some(standby);
            return Ok((
                current_epoch.visible_tip_height.value(),
                current_epoch,
                current_seq,
            ));
        }

        self.publish_canonical_standby(standby, current_epoch, caught_up_epoch, current_seq)
    }

    fn publish_canonical_standby(
        &self,
        standby: RocksDbCanonicalSecondary,
        current_epoch: ChainEpoch,
        caught_up_epoch: ChainEpoch,
        current_seq: u64,
    ) -> Result<(u32, ChainEpoch, u64), BenchError> {
        let sec_arc = Arc::new(standby);
        let new_reader = match RocksDbCanonicalReader::new(Arc::clone(&sec_arc)) {
            Ok(reader) => {
                drop(sec_arc);
                Arc::new(reader)
            }
            Err(err) => {
                let invalidated = is_storage_read_invalidated(&err);
                if let Ok(sec) = Arc::try_unwrap(sec_arc) {
                    self.state.lock().standby = Some(sec);
                }
                if invalidated {
                    return Ok((
                        current_epoch.visible_tip_height.value(),
                        current_epoch,
                        current_seq,
                    ));
                }
                return Err(BenchError::canonical_read(err.to_string()));
            }
        };
        let old_reader = self.slot.publish(new_reader);
        self.invalidation_requested.store(false, Ordering::Release);
        let new_seq = {
            let mut guard = self.state.lock();
            guard.retired = Some(old_reader);
            reclaim_canonical_retired(&mut guard);
            guard.generation_seq = guard.generation_seq.saturating_add(1);
            guard.generation_seq
        };
        Ok((
            caught_up_epoch.visible_tip_height.value(),
            caught_up_epoch,
            new_seq,
        ))
    }
}

fn reclaim_canonical_retired(guard: &mut TwoGenCanonicalState) {
    let Some(retired) = guard.retired.take() else {
        return;
    };
    match Arc::try_unwrap(retired).map(RocksDbCanonicalReader::into_secondary) {
        Ok(Ok(sec)) => guard.standby = Some(sec),
        Ok(Err(reader)) => guard.retired = Some(Arc::new(reader)),
        Err(shared) => guard.retired = Some(shared),
    }
}

/// Bounded 4-generation `(canonical, wallet)` pair publisher matching production
/// `WalletServingPairPublisher` (`SECONDARY_GENERATION_COUNT = 4`), backing
/// `SwappableWalletQuery` with retained epoch-pin lookup.
pub(crate) struct TwoGenWalletPairPublisher {
    query: SwappableWalletQuery,
    _can_resources: SharedSecondaryResources,
    _wal_resources: SharedSecondaryResources,
    state: Mutex<TwoGenWalletPairState>,
    invalidation_requested: AtomicBool,
    pub(crate) canonical_io_modes: Vec<String>,
    pub(crate) wallet_io_modes: Vec<String>,
}

struct StandbyPairHandles {
    canonical: RocksDbCanonicalSecondary,
    wallet: RocksDbWalletSecondary,
}

struct ActiveOrRetiredWalletHandles {
    canonical: Arc<RocksDbCanonicalSecondary>,
    wallet: Arc<RocksDbWalletSecondary>,
    pair_weak: Weak<WalletServingReadPair>,
}

struct TwoGenWalletPairState {
    active: Option<ActiveOrRetiredWalletHandles>,
    standby: VecDeque<StandbyPairHandles>,
    retired: VecDeque<ActiveOrRetiredWalletHandles>,
    can_baseline: TickerCounterSnapshot,
    wal_baseline: TickerCounterSnapshot,
    generation_seq: u64,
}

/// Directories and storage policy needed to open [`TwoGenWalletPairPublisher`].
#[derive(Clone, Copy)]
pub(crate) struct WalletPairOpenConfig<'a> {
    pub(crate) canonical_dir: &'a Path,
    pub(crate) wallet_dir: &'a Path,
    pub(crate) secondary_dirs: &'a [PathBuf],
    pub(crate) wallet_secondary_dirs: &'a [PathBuf],
    pub(crate) reorg_window_blocks: u32,
    pub(crate) budget: RocksDbResourceBudget,
}

impl TwoGenWalletPairPublisher {
    /// Opens 4 canonical and 4 wallet secondary generations on shared bounded caches.
    pub(crate) fn open(cfg: WalletPairOpenConfig<'_>) -> Result<Arc<Self>, BenchError> {
        if cfg.secondary_dirs.len() < WALLET_GENERATION_SLOTS
            || cfg.wallet_secondary_dirs.len() < WALLET_GENERATION_SLOTS
        {
            return Err(BenchError::invalid_argument(
                "four-generation wallet pair publisher requires 4 canonical and 4 wallet secondary directories",
            ));
        }
        let can_resources = SharedSecondaryResources::new(cfg.budget);
        let wal_resources = SharedSecondaryResources::new(cfg.budget);
        let mut opened = open_wallet_slots(cfg, &can_resources, &wal_resources)?;

        let active_slot = opened.slots.remove(0);
        let active_can = Arc::new(active_slot.canonical);
        let active_wal = Arc::new(active_slot.wallet);
        let can_baseline = capture_canonical_sec_tickers(&active_can);
        let wal_baseline = capture_wallet_sec_tickers(&active_wal);
        let initial_pair = build_wallet_serving_pair(&active_can, &active_wal)?;
        let pair_weak = Arc::downgrade(&initial_pair);
        let initial_query = WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(
            Arc::clone(&initial_pair),
        ));

        Ok(Arc::new(Self {
            query: SwappableWalletQuery::new(initial_query, initial_pair),
            _can_resources: can_resources,
            _wal_resources: wal_resources,
            state: Mutex::new(TwoGenWalletPairState {
                active: Some(ActiveOrRetiredWalletHandles {
                    canonical: active_can,
                    wallet: active_wal,
                    pair_weak,
                }),
                standby: VecDeque::from(opened.slots),
                retired: VecDeque::new(),
                can_baseline,
                wal_baseline,
                generation_seq: 1,
            }),
            invalidation_requested: AtomicBool::new(false),
            canonical_io_modes: opened.canonical_io_modes,
            wallet_io_modes: opened.wallet_io_modes,
        }))
    }

    pub(crate) fn query(&self) -> SwappableWalletQuery {
        self.query.clone()
    }

    pub(crate) fn request_rotation(&self) {
        self.invalidation_requested.store(true, Ordering::Release);
    }

    pub(crate) fn has_rotation_request(&self) -> bool {
        self.invalidation_requested.load(Ordering::Acquire)
    }

    /// Rotates a fresh aligned `(canonical, wallet)` pair cleanly to the latest tip.
    pub(crate) async fn force_rotate_to_latest(self: &Arc<Self>) -> Result<(), BenchError> {
        let this = Arc::clone(self);
        tokio::task::spawn_blocking(move || this.catch_up_blocking(true).map(|_| ()))
            .await
            .map_err(|err| BenchError::tip_replay(err.to_string()))?
    }

    /// Catches up a standby canonical + wallet secondary pair to an aligned
    /// fence and swaps a new `WalletServingQuery` into [`SwappableWalletQuery`].
    pub(crate) async fn try_catch_up_and_publish(
        self: &Arc<Self>,
    ) -> Result<(u32, ChainEpoch, u64), BenchError> {
        let this = Arc::clone(self);
        tokio::task::spawn_blocking(move || this.catch_up_blocking(false))
            .await
            .map_err(|err| BenchError::tip_replay(err.to_string()))?
    }

    /// Swaps to a standby generation even at an unchanged epoch (used by
    /// quiescent controls so cold tail refills start on a fresh `CompactBlockSource`).
    pub(crate) async fn rotate_generation_for_cold_tail(
        self: &Arc<Self>,
        cancel: &CancellationToken,
    ) -> Result<(u32, ChainEpoch, u64), BenchError> {
        let this = Arc::clone(self);
        let stop = cancel.clone();
        tokio::task::spawn_blocking(move || {
            let mut last = {
                let epoch = this.query.capture_pair().chain_epoch();
                (epoch.visible_tip_height.value(), epoch, 0)
            };
            while !stop.is_cancelled() {
                let res = this.catch_up_blocking(true)?;
                if res.2 > 0 {
                    return Ok(res);
                }
                last = res;
                std::thread::sleep(Duration::from_millis(2));
            }
            Ok(last)
        })
        .await
        .map_err(|err| BenchError::tip_replay(err.to_string()))?
    }

    /// Captures and resets cumulative ticker deltas once per shared canonical
    /// and wallet secondary resource pool.
    pub(crate) fn snapshot_and_reset_tickers(&self) -> (StoreTickerDelta, StoreTickerDelta) {
        let mut guard = self.state.lock();
        reclaim_wallet_retired(&mut guard);
        let (can_delta, wal_delta) = if let Some((can_now, wal_now)) =
            guard.active.as_ref().map(|active| {
                (
                    capture_canonical_sec_tickers(&active.canonical),
                    capture_wallet_sec_tickers(&active.wallet),
                )
            }) {
            let cd = can_now.delta_since(&guard.can_baseline);
            let wd = wal_now.delta_since(&guard.wal_baseline);
            guard.can_baseline = can_now;
            guard.wal_baseline = wal_now;
            (cd, wd)
        } else {
            (StoreTickerDelta::default(), StoreTickerDelta::default())
        };
        drop(guard);
        (can_delta, wal_delta)
    }

    pub(crate) async fn wait_reclaim_all(self: &Arc<Self>) {
        self.query.clear_retained();
        let this = Arc::clone(self);
        let _ = tokio::task::spawn_blocking(move || {
            for _ in 0..200 {
                let mut guard = this.state.lock();
                reclaim_wallet_retired(&mut guard);
                if guard.retired.is_empty() {
                    return;
                }
                drop(guard);
                std::thread::sleep(Duration::from_millis(5));
            }
        })
        .await;
    }

    fn catch_up_blocking(&self, force_rotate: bool) -> Result<(u32, ChainEpoch, u64), BenchError> {
        let should_force = force_rotate || self.invalidation_requested.load(Ordering::Acquire);
        let mut standby = {
            let mut guard = self.state.lock();
            reclaim_wallet_retired(&mut guard);
            let current_pair = self.query.capture_pair();
            let current_epoch = current_pair.chain_epoch();
            let Some(standby) = guard.standby.pop_front() else {
                let seq = guard.generation_seq;
                drop(guard);
                return Ok((
                    current_epoch.visible_tip_height.value(),
                    current_epoch,
                    if force_rotate { 0 } else { seq },
                ));
            };
            drop(guard);
            standby
        };

        let current_epoch = self.query.capture_pair().chain_epoch();
        let current_seq = self.state.lock().generation_seq;

        let aligned = match converge_aligned_standby(&mut standby) {
            Ok(aligned) => aligned,
            Err(err) => {
                self.state.lock().standby.push_back(standby);
                return Err(err);
            }
        };
        let Some(new_epoch) = aligned else {
            self.state.lock().standby.push_back(standby);
            return Ok((
                current_epoch.visible_tip_height.value(),
                current_epoch,
                if force_rotate { 0 } else { current_seq },
            ));
        };
        if !should_force && new_epoch.id == current_epoch.id {
            self.state.lock().standby.push_back(standby);
            return Ok((
                current_epoch.visible_tip_height.value(),
                current_epoch,
                current_seq,
            ));
        }

        self.publish_wallet_standby(
            standby,
            WalletPublishMeta {
                current_epoch,
                new_epoch,
                current_seq,
                force_rotate,
            },
        )
    }

    fn publish_wallet_standby(
        &self,
        standby: StandbyPairHandles,
        meta: WalletPublishMeta,
    ) -> Result<(u32, ChainEpoch, u64), BenchError> {
        let active_can = Arc::new(standby.canonical);
        let active_wal = Arc::new(standby.wallet);
        let new_pair = match try_build_wallet_serving_pair(&active_can, &active_wal)? {
            BuildPairOutcome::Ready(pair) => pair,
            BuildPairOutcome::ReaderInvalidated => {
                if let (Ok(can), Ok(wal)) =
                    (Arc::try_unwrap(active_can), Arc::try_unwrap(active_wal))
                {
                    self.state.lock().standby.push_back(StandbyPairHandles {
                        canonical: can,
                        wallet: wal,
                    });
                }
                return Ok((
                    meta.current_epoch.visible_tip_height.value(),
                    meta.current_epoch,
                    if meta.force_rotate {
                        0
                    } else {
                        meta.current_seq
                    },
                ));
            }
        };
        let pair_weak = Arc::downgrade(&new_pair);
        let next_query = WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(
            Arc::clone(&new_pair),
        ));
        let max_retained = if meta.force_rotate {
            0
        } else {
            MAX_RETAINED_WALLET_GENERATIONS
        };
        self.query.publish(next_query, new_pair, max_retained);
        self.invalidation_requested.store(false, Ordering::Release);

        let new_seq = {
            let mut guard = self.state.lock();
            if let Some(prev_active) = guard.active.replace(ActiveOrRetiredWalletHandles {
                canonical: active_can,
                wallet: active_wal,
                pair_weak,
            }) {
                guard.retired.push_back(prev_active);
            }
            reclaim_wallet_retired(&mut guard);
            guard.generation_seq = guard.generation_seq.saturating_add(1);
            guard.generation_seq
        };
        Ok((
            meta.new_epoch.visible_tip_height.value(),
            meta.new_epoch,
            new_seq,
        ))
    }
}

#[derive(Clone, Copy)]
struct WalletPublishMeta {
    current_epoch: ChainEpoch,
    new_epoch: ChainEpoch,
    current_seq: u64,
    force_rotate: bool,
}

struct OpenedWalletSlots {
    slots: Vec<StandbyPairHandles>,
    canonical_io_modes: Vec<String>,
    wallet_io_modes: Vec<String>,
}

fn open_wallet_slots(
    cfg: WalletPairOpenConfig<'_>,
    can_resources: &SharedSecondaryResources,
    wal_resources: &SharedSecondaryResources,
) -> Result<OpenedWalletSlots, BenchError> {
    let reorg_policy = CanonicalReorgPolicy::new(cfg.reorg_window_blocks)?;
    let requirements = CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::All);
    let mut slots = Vec::with_capacity(WALLET_GENERATION_SLOTS);
    let mut canonical_io_modes = Vec::with_capacity(WALLET_GENERATION_SLOTS);
    let mut wallet_io_modes = Vec::with_capacity(WALLET_GENERATION_SLOTS);
    for slot_idx in 0..WALLET_GENERATION_SLOTS {
        let can = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
            cfg.canonical_dir,
            &cfg.secondary_dirs[slot_idx],
            Network::ZcashRegtest,
            requirements.clone(),
            reorg_policy,
            can_resources,
        )?;
        let wal = RocksDbWalletSecondary::open_ready(
            cfg.wallet_dir,
            &cfg.wallet_secondary_dirs[slot_idx],
            Network::ZcashRegtest,
            wal_resources,
        )?;
        canonical_io_modes.push(can.io_mode().as_str().to_owned());
        wallet_io_modes.push(wal.io_mode().as_str().to_owned());
        slots.push(StandbyPairHandles {
            canonical: can,
            wallet: wal,
        });
    }
    Ok(OpenedWalletSlots {
        slots,
        canonical_io_modes,
        wallet_io_modes,
    })
}

fn converge_aligned_standby(
    standby: &mut StandbyPairHandles,
) -> Result<Option<ChainEpoch>, BenchError> {
    for _ in 0..4 {
        let wal_seq = standby
            .wallet
            .ready_evidence()
            .source_position
            .event_sequence;
        let can_seq = standby.canonical.event_fence().chain_event_sequence();
        match catch_up_wallet_secondary(&mut standby.wallet) {
            Ok(()) => {}
            Err(err) if err.is_missing_table_file() => return Ok(None),
            Err(err) => return Err(err.into()),
        }
        if wal_seq >= can_seq {
            match catch_up_canonical_secondary(&mut standby.canonical) {
                Ok(_) => {}
                Err(err) if err.is_missing_table_file() => return Ok(None),
                Err(err) => return Err(err.into()),
            }
        }
        let wal_after = standby
            .wallet
            .ready_evidence()
            .source_position
            .event_sequence;
        let can_after = standby.canonical.event_fence().chain_event_sequence();
        if wal_after > can_after {
            match catch_up_canonical_secondary(&mut standby.canonical) {
                Ok(_) => {}
                Err(err) if err.is_missing_table_file() => return Ok(None),
                Err(err) => return Err(err.into()),
            }
        }
        let can_epoch = match standby.canonical.chain_epoch() {
            Ok(epoch) => epoch,
            Err(err) if err.is_missing_table_file() => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        if wal_after == standby.canonical.event_fence().chain_event_sequence()
            && standby
                .wallet
                .ready_evidence()
                .source_position
                .chain_epoch_id
                == can_epoch.id
        {
            return Ok(Some(can_epoch));
        }
    }
    Ok(None)
}

fn reclaim_wallet_retired(guard: &mut TwoGenWalletPairState) {
    let pending = guard.retired.len();
    for _ in 0..pending {
        let Some(retired) = guard.retired.pop_front() else {
            break;
        };
        if retired.pair_weak.strong_count() > 0 {
            guard.retired.push_back(retired);
            continue;
        }
        match (
            Arc::try_unwrap(retired.canonical),
            Arc::try_unwrap(retired.wallet),
        ) {
            (Ok(can), Ok(wal)) => {
                guard.standby.push_back(StandbyPairHandles {
                    canonical: can,
                    wallet: wal,
                });
            }
            (can_res, wal_res) => {
                let can = can_res.map_or_else(|arc| arc, Arc::new);
                let wal = wal_res.map_or_else(|arc| arc, Arc::new);
                guard.retired.push_back(ActiveOrRetiredWalletHandles {
                    canonical: can,
                    wallet: wal,
                    pair_weak: retired.pair_weak,
                });
            }
        }
    }
}

enum BuildPairOutcome {
    Ready(Arc<WalletServingReadPair>),
    ReaderInvalidated,
}

fn try_build_wallet_serving_pair(
    canonical_sec: &Arc<RocksDbCanonicalSecondary>,
    wallet_sec: &Arc<RocksDbWalletSecondary>,
) -> Result<BuildPairOutcome, BenchError> {
    let canonical_reader = match RocksDbCanonicalReader::new(Arc::clone(canonical_sec)) {
        Ok(reader) => reader,
        Err(err) if is_storage_read_invalidated(&err) => {
            return Ok(BuildPairOutcome::ReaderInvalidated);
        }
        Err(err) => return Err(BenchError::canonical_read(err.to_string())),
    };
    let wallet_reader = RocksDbWalletReader::new(Arc::clone(wallet_sec));
    let pair = WalletServingReadPair::new(Arc::new(canonical_reader), Arc::new(wallet_reader))
        .map_err(|err| BenchError::canonical_read(err.to_string()))?;
    Ok(BuildPairOutcome::Ready(Arc::new(pair)))
}

fn build_wallet_serving_pair(
    canonical_sec: &Arc<RocksDbCanonicalSecondary>,
    wallet_sec: &Arc<RocksDbWalletSecondary>,
) -> Result<Arc<WalletServingReadPair>, BenchError> {
    let canonical_reader = RocksDbCanonicalReader::new(Arc::clone(canonical_sec))
        .map_err(|err| BenchError::canonical_read(err.to_string()))?;
    let wallet_reader = RocksDbWalletReader::new(Arc::clone(wallet_sec));
    let pair = WalletServingReadPair::new(Arc::new(canonical_reader), Arc::new(wallet_reader))
        .map_err(|err| BenchError::canonical_read(err.to_string()))?;
    Ok(Arc::new(pair))
}

/// Supervised single gRPC listener per mode with clean shutdown and exit check.
pub(crate) struct SupervisedGrpcServer {
    pub(crate) addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: JoinHandle<Result<(), BenchError>>,
}

impl SupervisedGrpcServer {
    /// Spawns one canonical-only compatibility listener (`ingest_active`).
    pub(crate) async fn spawn_canonical_only(
        slot: CanonicalReaderSlot,
        mempool: SharedMempoolSurface,
        tip_watcher: SharedTipChangeWatcher,
    ) -> Result<Self, BenchError> {
        let dummy_node = ZebraJsonRpcSource::new(
            Network::ZcashRegtest,
            "http://127.0.0.1:1",
            NodeAuth::None,
            Duration::from_secs(5),
        )?;
        let adapter = LightwalletdGrpcAdapter::for_light_wallet(
            slot,
            dummy_node,
            Duration::from_secs(2),
            NonZeroU32::new(4).unwrap_or(NonZeroU32::MIN),
            ForwardedReadLimit::default(),
        )
        .with_mempool_surface(mempool)
        .with_tip_change_watcher(tip_watcher)
        .with_tip_wait(TipWait::default());
        let (addr, incoming, shutdown_tx, shutdown_rx) = bind_listener().await?;
        let service = adapter.into_server();
        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .map_err(|err| BenchError::canonical_read(format!("grpc server exited: {err}")))
        });
        Ok(Self {
            addr,
            shutdown_tx: Some(shutdown_tx),
            handle,
        })
    }

    /// Spawns one full wallet-projection compatibility listener.
    pub(crate) async fn spawn_wallet_serving(
        query: SwappableWalletQuery,
        mempool: SharedMempoolSurface,
        tip_watcher: SharedTipChangeWatcher,
    ) -> Result<Self, BenchError> {
        let adapter = LightwalletdGrpcAdapter::new(query)
            .with_mempool_surface(mempool)
            .with_tip_change_watcher(tip_watcher)
            .with_tip_wait(TipWait::default());
        let (addr, incoming, shutdown_tx, shutdown_rx) = bind_listener().await?;
        let service = adapter.into_server();
        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .map_err(|err| BenchError::canonical_read(format!("grpc server exited: {err}")))
        });
        Ok(Self {
            addr,
            shutdown_tx: Some(shutdown_tx),
            handle,
        })
    }

    /// Signals graceful shutdown and verifies the server exited without error.
    pub(crate) async fn shutdown(mut self) -> Result<(), BenchError> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.handle
            .await
            .map_err(|err| BenchError::canonical_read(err.to_string()))?
    }
}

async fn bind_listener() -> Result<
    (
        SocketAddr,
        impl tokio_stream::Stream<Item = std::io::Result<tokio::net::TcpStream>> + Send,
        oneshot::Sender<()>,
        oneshot::Receiver<()>,
    ),
    BenchError,
> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|source| BenchError::io("127.0.0.1:0", source))?;
    let addr = listener
        .local_addr()
        .map_err(|source| BenchError::io("127.0.0.1:0", source))?;
    let incoming = tokio_stream::StreamExt::map(TcpListenerStream::new(listener), |accepted| {
        if let Ok(ref stream) = accepted {
            let _ = stream.set_nodelay(true);
        }
        accepted
    });
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    Ok((addr, incoming, shutdown_tx, shutdown_rx))
}

pub(crate) fn capture_canonical_sec_tickers(
    sec: &RocksDbCanonicalSecondary,
) -> TickerCounterSnapshot {
    TickerCounterSnapshot::capture(|ticker| sec.rocksdb_ticker_count(ticker))
}

pub(crate) fn capture_wallet_sec_tickers(sec: &RocksDbWalletSecondary) -> TickerCounterSnapshot {
    TickerCounterSnapshot::capture(|ticker| sec.rocksdb_ticker_count(ticker))
}

/// Ensures cancellation aborts clean-up safely when dropped early.
pub(crate) struct CancelGuard(pub(crate) CancellationToken);

impl Drop for CancelGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, num::NonZeroU32, path::PathBuf, time::Duration};

    use tokio_util::sync::CancellationToken;
    use zinder_core::{BlockHeight, BlockHeightRange, ChainEpoch, TransparentAddressScriptHash};
    use zinder_query::{CanonicalReader as _, WalletQueryApi as _};
    use zinder_store::{
        EncodedCompactBlockOutput, EncodedCompactBlockRangeRequest, HeightOrder, RocksDbIoMode,
        RocksDbResourceBudget, ShieldedProjectionSelection,
    };

    use super::{
        TwoGenCanonicalPublisher, TwoGenWalletPairPublisher, WalletPairOpenConfig,
        capture_canonical_sec_tickers, capture_wallet_sec_tickers,
    };
    use crate::{
        BenchError,
        active_writer_publication::{
            ActiveWriterPublicationConfig,
            chain_fixture::{
                ChainSpecConfig, GeneratedChain, TemplateStores, create_scratch_tempdir,
            },
        },
    };

    struct TestPublisherLayout {
        _root: tempfile::TempDir,
        config: ActiveWriterPublicationConfig,
        can_dir: PathBuf,
        wal_dir: PathBuf,
        sec_dirs: Vec<PathBuf>,
        wal_sec_dirs: Vec<PathBuf>,
    }

    async fn setup_test_publishers() -> Result<TestPublisherLayout, Box<dyn std::error::Error>> {
        let config = ActiveWriterPublicationConfig {
            base_blocks: 16,
            live_blocks: 4,
            reorg_window_blocks: 2,
            segment_blocks: NonZeroU32::new(16).unwrap_or(NonZeroU32::MIN),
            dense_block_cadence: 4,
            dense_txs_per_block: 1,
            io_mode: RocksDbIoMode::Buffered,
            ..ActiveWriterPublicationConfig::default()
        };
        let chain = GeneratedChain::build(ChainSpecConfig {
            base_blocks: config.base_blocks,
            live_blocks: config.live_blocks,
            reorg_window_blocks: config.reorg_window_blocks,
            dense_block_cadence: config.dense_block_cadence,
            dense_txs_per_block: config.dense_txs_per_block,
            dense_orchard_actions_per_tx: 2,
        })?;
        let template = TemplateStores::build(&chain, &config).await?;
        let root = create_scratch_tempdir(None, "two-gen-test")?;
        let can_dir = root.path().join("canonical");
        let wal_dir = root.path().join("wallet");
        template.clone_to(&can_dir, &wal_dir)?;
        let sec_dirs: Vec<_> = (0..4)
            .map(|i| root.path().join(format!("can-sec-{i}")))
            .collect();
        let wal_sec_dirs: Vec<_> = (0..4)
            .map(|i| root.path().join(format!("wal-sec-{i}")))
            .collect();
        for dir in sec_dirs.iter().chain(wal_sec_dirs.iter()) {
            fs::create_dir_all(dir)?;
        }
        Ok(TestPublisherLayout {
            _root: root,
            config,
            can_dir,
            wal_dir,
            sec_dirs,
            wal_sec_dirs,
        })
    }

    #[tokio::test]
    async fn two_gen_wallet_publisher_rotates_and_reclaims_retired_pair_when_released()
    -> Result<(), Box<dyn std::error::Error>> {
        let layout = setup_test_publishers().await?;
        let publisher = TwoGenWalletPairPublisher::open(WalletPairOpenConfig {
            canonical_dir: &layout.can_dir,
            wallet_dir: &layout.wal_dir,
            secondary_dirs: &layout.sec_dirs,
            wallet_secondary_dirs: &layout.wal_sec_dirs,
            reorg_window_blocks: layout.config.reorg_window_blocks,
            budget: RocksDbResourceBudget::for_local_tests(),
        })?;
        let cancel = CancellationToken::new();
        let mut held_pairs = Vec::new();
        let mut last_seq = 1;
        for _ in 0..3 {
            held_pairs.push(publisher.query().capture_pair());
            let (_, _, seq) = publisher.rotate_generation_for_cold_tail(&cancel).await?;
            assert!(seq > last_seq);
            last_seq = seq;
        }
        // All 3 standby slots were rotated and remain pinned by `held_pairs`.
        let (_, _, blocked) = tokio::time::timeout(
            Duration::from_millis(250),
            publisher.try_catch_up_and_publish(),
        )
        .await
        .map_err(|err| BenchError::tip_replay(err.to_string()))??;
        assert_eq!(blocked, last_seq);

        drop(held_pairs);
        let (_, _, seq_after) = publisher.rotate_generation_for_cold_tail(&cancel).await?;
        assert!(seq_after > last_seq);
        publisher.wait_reclaim_all().await;
        Ok(())
    }

    #[tokio::test]
    async fn two_gen_canonical_publisher_rotates_at_unchanged_epoch_on_request()
    -> Result<(), Box<dyn std::error::Error>> {
        let layout = setup_test_publishers().await?;
        let publisher = TwoGenCanonicalPublisher::open(
            &layout.can_dir,
            &layout.sec_dirs,
            layout.config.reorg_window_blocks,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert!(!publisher.has_rotation_request());
        let (_, _, unchanged_seq) = publisher.try_catch_up_and_publish().await?;
        assert_eq!(unchanged_seq, 1);

        publisher.request_rotation();
        assert!(publisher.has_rotation_request());
        let (_, _, rotated_seq) = publisher.try_catch_up_and_publish().await?;
        assert!(rotated_seq > unchanged_seq);
        assert!(!publisher.has_rotation_request());
        publisher.wait_reclaim_all().await;
        Ok(())
    }

    #[tokio::test]
    async fn two_gen_wallet_publisher_rotates_at_unchanged_epoch_keeping_retained_pins()
    -> Result<(), Box<dyn std::error::Error>> {
        let layout = setup_test_publishers().await?;
        let publisher = TwoGenWalletPairPublisher::open(WalletPairOpenConfig {
            canonical_dir: &layout.can_dir,
            wallet_dir: &layout.wal_dir,
            secondary_dirs: &layout.sec_dirs,
            wallet_secondary_dirs: &layout.wal_sec_dirs,
            reorg_window_blocks: layout.config.reorg_window_blocks,
            budget: RocksDbResourceBudget::for_local_tests(),
        })?;
        let query = publisher.query();
        let initial_epoch: ChainEpoch = query.capture_pair().chain_epoch();
        publisher.request_rotation();
        assert!(publisher.has_rotation_request());

        let (_, _, rotated_seq) = publisher.try_catch_up_and_publish().await?;
        assert!(rotated_seq > 1);
        assert!(!publisher.has_rotation_request());
        let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(16));
        assert!(
            query
                .encoded_compact_block_range_up_to(
                    EncodedCompactBlockRangeRequest {
                        range,
                        order: HeightOrder::Ascending,
                        selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
                        include_transparent: false,
                        output: EncodedCompactBlockOutput::Blocks,
                        max_bytes: 1024 * 1024,
                        enforce_wire_fee_limit: true,
                    },
                    Some(initial_epoch.id),
                    None,
                )
                .await?
                .is_some()
        );
        publisher.wait_reclaim_all().await;
        Ok(())
    }

    #[tokio::test]
    async fn canonical_shared_secondary_tickers_counted_once_across_rotation()
    -> Result<(), Box<dyn std::error::Error>> {
        let layout = setup_test_publishers().await?;
        let publisher = TwoGenCanonicalPublisher::open(
            &layout.can_dir,
            &layout.sec_dirs,
            layout.config.reorg_window_blocks,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let _ = publisher.snapshot_and_reset_tickers();
        let before = capture_canonical_sec_tickers(publisher.capture_slot().capture().secondary());

        let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(16));
        let _ = publisher
            .capture_slot()
            .capture()
            .compact_blocks_in_range(range)
            .await?;
        let cancel = CancellationToken::new();
        let (_, _, seq) = publisher.rotate_generation_for_cold_tail(&cancel).await?;
        assert!(seq > 1);
        let rotated = publisher.capture_slot().capture();
        let _ = rotated.compact_blocks_in_range(range).await?;

        let after = capture_canonical_sec_tickers(rotated.secondary());
        let expected = after.delta_since(&before);
        let observed = publisher.snapshot_and_reset_tickers();
        assert_eq!(observed, expected);
        assert!(observed.bytes_read > 0);
        publisher.wait_reclaim_all().await;
        Ok(())
    }

    #[tokio::test]
    async fn wallet_shared_secondary_tickers_counted_once_across_rotation()
    -> Result<(), Box<dyn std::error::Error>> {
        let layout = setup_test_publishers().await?;
        let publisher = TwoGenWalletPairPublisher::open(WalletPairOpenConfig {
            canonical_dir: &layout.can_dir,
            wallet_dir: &layout.wal_dir,
            secondary_dirs: &layout.sec_dirs,
            wallet_secondary_dirs: &layout.wal_sec_dirs,
            reorg_window_blocks: layout.config.reorg_window_blocks,
            budget: RocksDbResourceBudget::for_local_tests(),
        })?;
        let _ = publisher.snapshot_and_reset_tickers();
        let (before_can, before_wal) = wallet_publisher_raw_tickers(&publisher)?;

        let mut spk = vec![0x76_u8, 0xa9, 0x14];
        spk.extend_from_slice(&[0x42_u8; 19]);
        spk.push(1);
        spk.extend_from_slice(&[0x88_u8, 0xac]);
        let script_hash = TransparentAddressScriptHash::of_script_pub_key(&spk);
        let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(16));
        let _ = publisher
            .query()
            .compact_blocks_in_range(range, None)
            .await?;
        let _ = publisher
            .query()
            .transparent_address_balance(vec![script_hash], None)
            .await?;

        let cancel = CancellationToken::new();
        let (_, _, seq) = publisher.rotate_generation_for_cold_tail(&cancel).await?;
        assert!(seq > 1);
        let _ = publisher
            .query()
            .compact_blocks_in_range(range, None)
            .await?;
        let _ = publisher
            .query()
            .transparent_address_balance(vec![script_hash], None)
            .await?;

        let (after_can, after_wal) = wallet_publisher_raw_tickers(&publisher)?;
        let expected = (
            after_can.delta_since(&before_can),
            after_wal.delta_since(&before_wal),
        );
        let observed = publisher.snapshot_and_reset_tickers();
        assert_eq!(observed, expected);
        assert!(observed.0.bytes_read > 0);
        assert!(
            observed.1.bytes_read > 0
                || observed.1.block_cache_data_hit > 0
                || observed.1.block_cache_data_miss > 0
        );
        publisher.wait_reclaim_all().await;
        Ok(())
    }

    fn wallet_publisher_raw_tickers(
        publisher: &TwoGenWalletPairPublisher,
    ) -> Result<
        (
            super::super::report::TickerCounterSnapshot,
            super::super::report::TickerCounterSnapshot,
        ),
        BenchError,
    > {
        let guard = publisher.state.lock();
        let active = guard
            .active
            .as_ref()
            .ok_or_else(|| BenchError::tip_replay("active wallet pair missing"))?;
        let snapshots = (
            capture_canonical_sec_tickers(&active.canonical),
            capture_wallet_sec_tickers(&active.wallet),
        );
        drop(guard);
        Ok(snapshots)
    }
}
