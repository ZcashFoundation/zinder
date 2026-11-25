//! Single-scan external construction of wallet projection SSTs.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    mem::size_of,
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    time::{Duration, Instant},
};

use parking_lot::{Condvar, Mutex};
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, CanonicalBlockFacts,
    CanonicalBlockFactsDigest, CanonicalBlockFactsSequenceDigest,
    CanonicalBlockFactsSequenceDigestBuilder, CanonicalBlockFactsSequenceDigestVersion, Network,
    TransactionId, ValidatedCanonicalBlockReplay,
};
use zinder_rocksdb_bulk_load::{
    BulkLoadError, OrderedSstWriter, ParallelVariableValueSorter, SortedVariableValues, SstFileSet,
    VariableValueSortDurations, VariableValueSortEvidence, VariableValueSortShard,
    VariableValueSorter,
};
use zinder_store::{
    CanonicalReplaySliceSource, CanonicalStoreError, ObservedCanonicalReplaySequence,
};
use zinder_wallet_projection::{
    WalletAddressBalance, WalletAddressTransaction, WalletAddressTransactionKey,
    WalletAddressUnspentOutputKey, WalletOutpointKey, WalletProjectionAccumulator,
    WalletProjectionContractError, WalletProjectionDigest, WalletProjectionFamilyRowCounts,
    WalletProjectionRowFamily, WalletReorgUndo, WalletSpentOutput, WalletTransactionPosition,
    WalletUnspentOutput, WalletUtxoSetSummary,
};

use crate::{
    RocksDbWalletError, WalletProjectionReplaySource, WalletSecondaryRowDerivationPhaseDurations,
    WalletSstWriteDurations,
    accumulator_hashing::ConstructionAccumulators,
    store::{
        REORG_UNDO_COLUMN_FAMILY, TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY,
        TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY, TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY,
        TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
        TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY, WalletColumnFamilyOptions,
    },
};

const OUTPOINT_EVENT_KEY_BYTES: usize = 37;
const ADDRESS_UNSPENT_KEY_BYTES: usize = 72;
const ADDRESS_TRANSACTION_KEY_BYTES: usize = 40;
const SPEND_TRAILER_BYTES: usize = 76;
const OUTPUT_EVENT_TAG: u8 = 0;
const SPEND_EVENT_TAG: u8 = 1;

/// Default height span scanned by each parallel canonical-scan chunk.
pub(crate) const DEFAULT_CANONICAL_SCAN_CHUNK_BLOCKS: NonZeroU32 = match NonZeroU32::new(4_096) {
    Some(blocks) => blocks,
    None => NonZeroU32::MIN,
};

/// Minimum accounted outpoint-sort memory required per parallel scan shard.
///
/// Keeps each shard well above any valid transparent output record (~10 KiB
/// consensus script cap plus metadata) so splitting memory across workers
/// never refuses a record that would fit under a smaller worker count.
const MIN_SHARD_OUTPOINT_SORT_MEMORY_BYTES: u64 = 64 * 1024;

/// Maximum height chunks claimed ahead of ordered sequence-digest verification.
const MAX_IN_FLIGHT_SCAN_CHUNKS: usize = 64;

/// Resource limits for one external projection construction.
#[derive(Clone, Copy)]
pub(crate) struct WalletProjectionLoadConfig<'options> {
    pub(crate) staging_path: &'options Path,
    pub(crate) options: &'options WalletColumnFamilyOptions,
    pub(crate) network: Network,
    pub(crate) first_retained_block: BlockId,
    pub(crate) settled_tip: BlockId,
    pub(crate) supported_reorg_depth: u32,
    pub(crate) max_outpoint_sort_memory_bytes: u64,
    pub(crate) max_secondary_sort_memory_bytes_per_sorter: u64,
    pub(crate) max_temporary_file_bytes_per_sorter: u64,
    pub(crate) sst_target_logical_bytes: u64,
    pub(crate) max_accounted_reorg_undo_bytes: u64,
    pub(crate) accumulator_hashing_workers: NonZeroU32,
    pub(crate) scan_chunk_blocks: NonZeroU32,
}

/// Counts proving the build used one authenticated scan and no prevout reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalletProjectionLoadCounters {
    pub(crate) scanned_block_count: u64,
    pub(crate) scanned_transaction_count: u64,
    pub(crate) staged_output_count: u64,
    pub(crate) staged_spend_count: u64,
    pub(crate) historical_prevout_read_count: u64,
    pub(crate) peak_accounted_reorg_undo_bytes: u64,
    pub(crate) max_accounted_reorg_undo_bytes: u64,
}

/// Phase timings exposed by the fixed-tip build report.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct WalletProjectionLoadPhaseDurations {
    pub(crate) canonical_scan: Duration,
    pub(crate) outpoint_sort: Duration,
    pub(crate) outpoint_merge: Duration,
    pub(crate) secondary_row_derivation: Duration,
    pub(crate) secondary_row_derivation_phases: WalletSecondaryRowDerivationPhaseDurations,
    pub(crate) sst_write: WalletSstWriteDurations,
    pub(crate) logical_evidence: Duration,
}

/// One prepared physical family ready for external-file ingestion.
pub(crate) struct PreparedWalletColumnFamily {
    pub(crate) name: &'static str,
    pub(crate) paths: Vec<PathBuf>,
}

/// Complete version-1 SST artifacts and the evidence derived while writing them.
pub(crate) struct PreparedWalletProjectionLoad {
    pub(crate) network: Network,
    pub(crate) supported_reorg_depth: u32,
    pub(crate) first_block: BlockId,
    pub(crate) tip: BlockId,
    pub(crate) source_sequence_digest: CanonicalBlockFactsSequenceDigest,
    pub(crate) projection_digest: WalletProjectionDigest,
    pub(crate) projection_accumulator: WalletProjectionAccumulator,
    pub(crate) row_counts: WalletProjectionFamilyRowCounts,
    pub(crate) utxo_summary: WalletUtxoSetSummary,
    pub(crate) counters: WalletProjectionLoadCounters,
    pub(crate) phase_durations: WalletProjectionLoadPhaseDurations,
    pub(crate) outpoint_sort_evidence: VariableValueSortEvidence,
    pub(crate) address_index_sort_evidence: VariableValueSortEvidence,
    pub(crate) address_transaction_sort_evidence: VariableValueSortEvidence,
    pub(crate) outpoint_sort_durations: VariableValueSortDurations,
    pub(crate) address_index_sort_durations: VariableValueSortDurations,
    pub(crate) address_transaction_sort_durations: VariableValueSortDurations,
    pub(crate) logical_row_bytes: u64,
    pub(crate) sst_file_bytes: u64,
    pub(crate) sst_file_count: u64,
    pub(crate) families: Vec<PreparedWalletColumnFamily>,
}

#[derive(Clone, Copy)]
struct StagedSpend {
    spent_at: WalletTransactionPosition,
    input_index: u32,
}

#[derive(Default)]
struct WalletProjectionSstEvidence {
    logical_row_bytes: u64,
}

impl WalletProjectionSstEvidence {
    fn add_row(&mut self, key: &[u8], encoded_value: &[u8]) -> Result<(), RocksDbWalletError> {
        let row_bytes = u64::try_from(key.len())
            .ok()
            .and_then(|key_bytes| {
                u64::try_from(encoded_value.len())
                    .ok()
                    .and_then(|value_bytes| key_bytes.checked_add(value_bytes))
            })
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        self.logical_row_bytes = self
            .logical_row_bytes
            .checked_add(row_bytes)
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        Ok(())
    }
}

struct RetainedUndo {
    records: VecDeque<WalletReorgUndo>,
    supported_depth: usize,
    settled_tip: BlockId,
    tracking_current_block: bool,
    accounted_bytes: u64,
    peak_accounted_bytes: u64,
    max_accounted_bytes: u64,
}

impl RetainedUndo {
    fn new(supported_depth: u32, settled_tip: BlockId, max_accounted_bytes: u64) -> Self {
        Self {
            records: VecDeque::new(),
            supported_depth: usize::try_from(supported_depth).unwrap_or(usize::MAX),
            settled_tip,
            tracking_current_block: false,
            accounted_bytes: 0,
            peak_accounted_bytes: 0,
            max_accounted_bytes,
        }
    }

    fn begin_block(
        &mut self,
        block: BlockId,
        parent_hash: BlockHash,
        source_sequence_digest_before: CanonicalBlockFactsSequenceDigest,
        source_sequence_digest_after: CanonicalBlockFactsSequenceDigest,
    ) -> Result<(), RocksDbWalletError> {
        self.tracking_current_block = false;
        if block.height < self.settled_tip.height {
            return Ok(());
        }
        if block.height == self.settled_tip.height {
            if block != self.settled_tip {
                return Err(RocksDbWalletError::CanonicalSourceFenceMismatch {
                    reason: "canonical replay settled-height block differs from the admitted settled tip",
                });
            }
            return Ok(());
        }
        if self.supported_depth == 0 || self.records.len() >= self.supported_depth {
            return Err(RocksDbWalletError::WalletProjectionRebuildRequired {
                reason: "canonical unsettled suffix exceeds the wallet's configured undo capacity",
            });
        }
        self.reserve(
            u64::try_from(size_of::<WalletReorgUndo>())
                .map_err(|_| RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?,
        )?;
        self.records.push_back(WalletReorgUndo {
            block,
            parent_hash,
            source_sequence_digest_before,
            source_sequence_digest_after,
            created_outpoints: Vec::new(),
            spent_outpoints: Vec::new(),
            address_transaction_keys: Vec::new(),
        });
        self.tracking_current_block = true;
        Ok(())
    }

    fn add_created_outpoint(&mut self, key: WalletOutpointKey) -> Result<(), RocksDbWalletError> {
        if !self.tracking_current_block {
            return Ok(());
        }
        self.reserve(
            u64::try_from(size_of::<WalletOutpointKey>())
                .map_err(|_| RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?,
        )?;
        self.records
            .back_mut()
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?
            .created_outpoints
            .push(key);
        Ok(())
    }

    fn add_spent_outpoint(&mut self, key: WalletOutpointKey) -> Result<(), RocksDbWalletError> {
        if !self.tracking_current_block {
            return Ok(());
        }
        self.reserve(
            u64::try_from(size_of::<WalletOutpointKey>())
                .map_err(|_| RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?,
        )?;
        self.records
            .back_mut()
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?
            .spent_outpoints
            .push(key);
        Ok(())
    }

    fn finish_block(&mut self) -> Result<(), RocksDbWalletError> {
        if !self.tracking_current_block {
            return Ok(());
        }
        let undo = self
            .records
            .back_mut()
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        undo.created_outpoints.sort_unstable();
        undo.spent_outpoints.sort_unstable();
        Ok(())
    }

    fn reserve_transient(&mut self, additional_bytes: u64) -> Result<(), RocksDbWalletError> {
        if !self.tracking_current_block {
            return Ok(());
        }
        self.reserve(additional_bytes)
    }

    fn release_transient(&mut self, bytes: u64) -> Result<(), RocksDbWalletError> {
        if !self.tracking_current_block {
            return Ok(());
        }
        self.accounted_bytes = self
            .accounted_bytes
            .checked_sub(bytes)
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        Ok(())
    }

    fn add_address_transaction(
        &mut self,
        key: WalletAddressTransactionKey,
    ) -> Result<(), RocksDbWalletError> {
        let Some(undo_index) = self
            .records
            .iter()
            .position(|undo| undo.block.height == key.block_height())
        else {
            return Ok(());
        };
        self.reserve(
            u64::try_from(size_of::<WalletAddressTransactionKey>())
                .map_err(|_| RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?,
        )?;
        self.records[undo_index].address_transaction_keys.push(key);
        Ok(())
    }

    fn reserve(&mut self, additional_bytes: u64) -> Result<(), RocksDbWalletError> {
        let required_bytes = self
            .accounted_bytes
            .checked_add(additional_bytes)
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        if required_bytes > self.max_accounted_bytes {
            return Err(RocksDbWalletError::AccountedReorgUndoMemoryLimit {
                limit_bytes: self.max_accounted_bytes,
                required_bytes,
            });
        }
        self.accounted_bytes = required_bytes;
        self.peak_accounted_bytes = self.peak_accounted_bytes.max(required_bytes);
        Ok(())
    }
}

trait OutpointEventSink {
    fn push_event(
        &mut self,
        key: [u8; OUTPOINT_EVENT_KEY_BYTES],
        encoded_value: &[u8],
    ) -> Result<(), BulkLoadError>;
}

impl OutpointEventSink for VariableValueSorter<OUTPOINT_EVENT_KEY_BYTES> {
    fn push_event(
        &mut self,
        key: [u8; OUTPOINT_EVENT_KEY_BYTES],
        encoded_value: &[u8],
    ) -> Result<(), BulkLoadError> {
        self.push(key, encoded_value)
    }
}

impl OutpointEventSink for VariableValueSortShard<'_, OUTPOINT_EVENT_KEY_BYTES> {
    fn push_event(
        &mut self,
        key: [u8; OUTPOINT_EVENT_KEY_BYTES],
        encoded_value: &[u8],
    ) -> Result<(), BulkLoadError> {
        self.push(key, encoded_value)
    }
}

struct CompletedCanonicalScan {
    first_block: BlockId,
    tip: BlockId,
    source_sequence_digest: CanonicalBlockFactsSequenceDigest,
    retained_undo: RetainedUndo,
    scanned_block_count: u64,
    scanned_transaction_count: u64,
    staged_output_count: u64,
    staged_spend_count: u64,
    canonical_scan: Duration,
    outpoint_sort: Duration,
    sorted_outpoints: SortedVariableValues<OUTPOINT_EVENT_KEY_BYTES>,
    outpoint_sort_evidence: VariableValueSortEvidence,
    outpoint_sort_durations: VariableValueSortDurations,
}

#[derive(Default)]
struct ScanCounters {
    blocks: u64,
    transactions: u64,
    staged_outputs: u64,
    staged_spends: u64,
}

impl ScanCounters {
    fn combine(&mut self, other: &Self) -> Result<(), RocksDbWalletError> {
        self.blocks = add_counter(self.blocks, other.blocks)?;
        self.transactions = add_counter(self.transactions, other.transactions)?;
        self.staged_outputs = add_counter(self.staged_outputs, other.staged_outputs)?;
        self.staged_spends = add_counter(self.staged_spends, other.staged_spends)?;
        Ok(())
    }
}

struct CompletedReplayChunk {
    chunk_index: usize,
    first_block: BlockId,
    first_parent_hash: BlockHash,
    last_block: BlockId,
    counters: ScanCounters,
    digests: Vec<CanonicalBlockFactsDigest>,
}

struct ChunkWindowState {
    next_to_claim: usize,
    next_to_verify: usize,
    stopped: bool,
}

struct ChunkWindow {
    chunks: Vec<BlockHeightRange>,
    max_in_flight: usize,
    stop_flag: AtomicBool,
    #[cfg(test)]
    panic_on_chunk: std::sync::atomic::AtomicUsize,
    state: Mutex<ChunkWindowState>,
    available: Condvar,
}

impl ChunkWindow {
    fn new(chunks: Vec<BlockHeightRange>, max_in_flight: usize) -> Self {
        Self {
            chunks,
            max_in_flight: max_in_flight.max(1),
            stop_flag: AtomicBool::new(false),
            #[cfg(test)]
            panic_on_chunk: std::sync::atomic::AtomicUsize::new(
                NEXT_WINDOW_PANIC_CHUNK.take().unwrap_or(usize::MAX),
            ),
            state: Mutex::new(ChunkWindowState {
                next_to_claim: 0,
                next_to_verify: 0,
                stopped: false,
            }),
            available: Condvar::new(),
        }
    }

    fn is_stopped(&self) -> bool {
        self.stop_flag.load(Ordering::Relaxed)
    }

    fn stop(&self) {
        self.stop_flag.store(true, Ordering::Relaxed);
        self.state.lock().stopped = true;
        self.available.notify_all();
    }

    fn claim_next(&self) -> Option<(usize, BlockHeightRange)> {
        let mut state = self.state.lock();
        loop {
            if state.stopped || state.next_to_claim >= self.chunks.len() {
                return None;
            }
            if state.next_to_claim.saturating_sub(state.next_to_verify) < self.max_in_flight {
                let chunk_index = state.next_to_claim;
                state.next_to_claim = chunk_index.saturating_add(1);
                drop(state);
                return Some((chunk_index, self.chunks[chunk_index]));
            }
            self.available.wait(&mut state);
        }
    }

    fn mark_verified(&self, next_to_verify: usize) {
        self.state.lock().next_to_verify = next_to_verify;
        self.available.notify_all();
    }
}

#[cfg(test)]
thread_local! {
    /// Test-only counter: when `Some(n)`, the `n`th canonical-scan worker spawn
    /// on this thread returns a refused-thread error.
    static REFUSED_CANONICAL_SCAN_THREAD: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };

    /// Test-only one-shot arming slot consumed by [`ChunkWindow::new`] on the
    /// spawning thread so concurrent tests do not share a process-global hook.
    static NEXT_WINDOW_PANIC_CHUNK: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
struct PanicChunkArmGuard;

#[cfg(test)]
impl Drop for PanicChunkArmGuard {
    fn drop(&mut self) {
        NEXT_WINDOW_PANIC_CHUNK.set(None);
    }
}

#[cfg(test)]
fn arm_next_window_panic_chunk(chunk_index: usize) -> PanicChunkArmGuard {
    NEXT_WINDOW_PANIC_CHUNK.set(Some(chunk_index));
    PanicChunkArmGuard
}

#[cfg(test)]
impl ChunkWindow {
    #[expect(
        clippy::panic,
        reason = "intentional unwind injection for testing worker-panic recovery"
    )]
    fn maybe_inject_worker_unwind(&self, chunk_index: usize) {
        if self
            .panic_on_chunk
            .compare_exchange(chunk_index, usize::MAX, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            panic!("injected canonical scan worker unwind on chunk {chunk_index}");
        }
    }
}

/// Stops and wakes the shared [`ChunkWindow`] if a scan worker unwinds so
/// sibling workers exit instead of filling the claim window and deadlocking.
struct StopWindowOnUnwind<'a>(&'a ChunkWindow);

impl Drop for StopWindowOnUnwind<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.stop();
        }
    }
}

/// Spawns one scoped canonical-scan worker, converting OS thread-start
/// failures into [`RocksDbWalletError::CanonicalScanThreadUnavailable`].
fn spawn_canonical_scan_worker<'scope, 'env, F, T>(
    scope: &'scope std::thread::Scope<'scope, 'env>,
    worker_index: usize,
    task: F,
) -> Result<std::thread::ScopedJoinHandle<'scope, T>, RocksDbWalletError>
where
    F: FnOnce() -> T + Send + 'scope,
    T: Send + 'scope,
{
    #[cfg(test)]
    if let Some(remaining) = REFUSED_CANONICAL_SCAN_THREAD.get() {
        if remaining == 0 {
            REFUSED_CANONICAL_SCAN_THREAD.set(None);
            return Err(RocksDbWalletError::CanonicalScanThreadUnavailable {
                reason: "injected thread start refusal".to_owned(),
            });
        }
        REFUSED_CANONICAL_SCAN_THREAD.set(Some(remaining - 1));
    }
    std::thread::Builder::new()
        .name(format!("zinder-wallet-scan-{worker_index}"))
        .spawn_scoped(scope, task)
        .map_err(
            |source| RocksDbWalletError::CanonicalScanThreadUnavailable {
                reason: source.to_string(),
            },
        )
}

/// Splits `[first_height ..= settled_height]` into contiguous inclusive height
/// chunks of at most `chunk_blocks` blocks each.
fn partition_settled_height_chunks(
    first_height: BlockHeight,
    settled_height: BlockHeight,
    chunk_blocks: NonZeroU32,
) -> Vec<BlockHeightRange> {
    if first_height > settled_height {
        return Vec::new();
    }
    let span = chunk_blocks.get();
    let mut chunks = Vec::new();
    let mut current = first_height.value();
    let last = settled_height.value();
    loop {
        let chunk_end = current.saturating_add(span - 1).min(last);
        chunks.push(BlockHeightRange::inclusive(
            BlockHeight::new(current),
            BlockHeight::new(chunk_end),
        ));
        if chunk_end == last {
            break;
        }
        current = chunk_end + 1;
    }
    chunks
}

/// Effective worker count for parallel canonical replay scanning.
///
/// Caps workers by the configured worker limit, the number of settled chunks,
/// and the per-shard minimum sort memory so each worker can keep typical
/// transparent outputs buffered before spilling (while oversized outputs within
/// the global budget spill directly as single-record runs).
fn parallel_scan_worker_count(
    requested_workers: NonZeroU32,
    chunk_count: usize,
    max_outpoint_sort_memory_bytes: u64,
) -> NonZeroU32 {
    let max_for_memory = max_outpoint_sort_memory_bytes / MIN_SHARD_OUTPOINT_SORT_MEMORY_BYTES;
    let workers = u64::from(requested_workers.get())
        .min(u64::try_from(chunk_count).unwrap_or(u64::MAX))
        .min(max_for_memory)
        .min(u64::from(crate::MAX_ACCUMULATOR_HASHING_WORKERS));
    NonZeroU32::new(u32::try_from(workers).unwrap_or(1)).unwrap_or(NonZeroU32::MIN)
}

/// Writes all six wallet families from a replay source, scanning the settled
/// prefix in parallel height chunks when the source supports slices and more
/// than one worker is active.
pub(crate) fn write_projection_ssts_from_source<Source>(
    config: WalletProjectionLoadConfig<'_>,
    canonical_store: &Source,
) -> Result<PreparedWalletProjectionLoad, RocksDbWalletError>
where
    Source: WalletProjectionReplaySource + ?Sized,
{
    if let Some(slice_source) = canonical_store.wallet_projection_slice_source() {
        let chunks = partition_settled_height_chunks(
            config.first_retained_block.height,
            config.settled_tip.height,
            config.scan_chunk_blocks,
        );
        let workers = parallel_scan_worker_count(
            config.accumulator_hashing_workers,
            chunks.len(),
            config.max_outpoint_sort_memory_bytes,
        );
        if workers.get() > 1 {
            let scanned = scan_canonical_replay_parallel(config, &slice_source, chunks, workers)?;
            return finish_projection_ssts(config, scanned);
        }
    }
    let blocks = canonical_store
        .wallet_projection_scan()
        .map_err(|source| RocksDbWalletError::CanonicalReplay { source })?;
    write_projection_ssts(config, blocks)
}

/// Writes all six wallet families from one authenticated canonical replay scan.
pub(crate) fn write_projection_ssts(
    config: WalletProjectionLoadConfig<'_>,
    blocks: impl IntoIterator<Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>>,
) -> Result<PreparedWalletProjectionLoad, RocksDbWalletError> {
    let scanned = scan_canonical_replay_serial(config, blocks)?;
    finish_projection_ssts(config, scanned)
}

fn scan_canonical_replay_serial(
    config: WalletProjectionLoadConfig<'_>,
    blocks: impl IntoIterator<Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>>,
) -> Result<CompletedCanonicalScan, RocksDbWalletError> {
    let scan_started = Instant::now();
    let mut outpoint_sorter = VariableValueSorter::<OUTPOINT_EVENT_KEY_BYTES>::new(
        config.staging_path,
        "wallet-outpoint-events",
        config.max_outpoint_sort_memory_bytes,
        config.max_temporary_file_bytes_per_sorter,
    )?;
    let mut retained_undo = RetainedUndo::new(
        config.supported_reorg_depth,
        config.settled_tip,
        config.max_accounted_reorg_undo_bytes,
    );
    let mut sequence_digest =
        CanonicalBlockFactsSequenceDigestBuilder::new(CanonicalBlockFactsSequenceDigestVersion::V1);
    let mut first_block = None;
    let mut previous_block = None;
    let mut counters = ScanCounters::default();

    for replay in blocks {
        let replay = replay.map_err(|source| RocksDbWalletError::CanonicalReplay { source })?;
        let block = stage_serial_block(
            config,
            replay,
            &mut first_block,
            previous_block,
            &mut sequence_digest,
            &mut retained_undo,
            &mut outpoint_sorter,
            &mut counters,
        )?;
        previous_block = Some(block);
    }
    let first_block = first_block.ok_or(RocksDbWalletError::EmptyCanonicalHistory)?;
    let tip = previous_block.ok_or(RocksDbWalletError::EmptyCanonicalHistory)?;
    let canonical_scan = scan_started.elapsed();

    let phase_started = Instant::now();
    let sorted_outpoints = outpoint_sorter.finish()?;
    let outpoint_sort_evidence = sorted_outpoints.evidence();
    let outpoint_sort_durations = sorted_outpoints.durations();
    let outpoint_sort = phase_started.elapsed();

    Ok(CompletedCanonicalScan {
        first_block,
        tip,
        source_sequence_digest: sequence_digest.finish(),
        retained_undo,
        scanned_block_count: counters.blocks,
        scanned_transaction_count: counters.transactions,
        staged_output_count: counters.staged_outputs,
        staged_spend_count: counters.staged_spends,
        canonical_scan,
        outpoint_sort,
        sorted_outpoints,
        outpoint_sort_evidence,
        outpoint_sort_durations,
    })
}

struct ParallelSettledScan<'sorter> {
    shards: Vec<VariableValueSortShard<'sorter, OUTPOINT_EVENT_KEY_BYTES>>,
    first_block: Option<BlockId>,
    previous_block: Option<BlockId>,
    counters: ScanCounters,
}

fn scan_canonical_replay_parallel(
    config: WalletProjectionLoadConfig<'_>,
    slice_source: &CanonicalReplaySliceSource<'_>,
    chunks: Vec<BlockHeightRange>,
    worker_count: NonZeroU32,
) -> Result<CompletedCanonicalScan, RocksDbWalletError> {
    let scan_started = Instant::now();
    let parallel_sorter = ParallelVariableValueSorter::<OUTPOINT_EVENT_KEY_BYTES>::new(
        config.staging_path,
        "wallet-outpoint-events",
        worker_count,
        config.max_outpoint_sort_memory_bytes,
        config.max_temporary_file_bytes_per_sorter,
    )?;
    let mut retained_undo = RetainedUndo::new(
        config.supported_reorg_depth,
        config.settled_tip,
        config.max_accounted_reorg_undo_bytes,
    );
    let mut sequence_digest =
        CanonicalBlockFactsSequenceDigestBuilder::new(CanonicalBlockFactsSequenceDigestVersion::V1);
    let mut settled = run_parallel_settled_chunks(
        &parallel_sorter,
        slice_source,
        chunks,
        config.first_retained_block,
        &mut sequence_digest,
    )?;
    if settled.previous_block != Some(config.settled_tip) {
        return Err(RocksDbWalletError::CanonicalSourceFenceMismatch {
            reason: "canonical replay settled-height block differs from the admitted settled tip",
        });
    }
    scan_unsettled_suffix(
        config,
        slice_source,
        &mut settled,
        &mut sequence_digest,
        &mut retained_undo,
    )?;

    let ParallelSettledScan {
        shards,
        first_block,
        previous_block,
        counters,
    } = settled;
    let first_block = first_block.ok_or(RocksDbWalletError::EmptyCanonicalHistory)?;
    let tip = previous_block.ok_or(RocksDbWalletError::EmptyCanonicalHistory)?;
    let final_sequence_digest = sequence_digest.finish();
    slice_source
        .verify_complete_sequence(ObservedCanonicalReplaySequence {
            first_block,
            tip,
            block_count: counters.blocks,
            sequence_digest: final_sequence_digest,
        })
        .map_err(|source| RocksDbWalletError::CanonicalReplay { source })?;
    let canonical_scan = scan_started.elapsed();

    let phase_started = Instant::now();
    let staged = parallel_sorter.flush_shards(shards)?;
    let sorted_outpoints = parallel_sorter.finish(staged)?;
    let outpoint_sort_evidence = sorted_outpoints.evidence();
    let outpoint_sort_durations = sorted_outpoints.durations();
    let outpoint_sort = phase_started.elapsed();

    Ok(CompletedCanonicalScan {
        first_block,
        tip,
        source_sequence_digest: final_sequence_digest,
        retained_undo,
        scanned_block_count: counters.blocks,
        scanned_transaction_count: counters.transactions,
        staged_output_count: counters.staged_outputs,
        staged_spend_count: counters.staged_spends,
        canonical_scan,
        outpoint_sort,
        sorted_outpoints,
        outpoint_sort_evidence,
        outpoint_sort_durations,
    })
}

fn scan_unsettled_suffix(
    config: WalletProjectionLoadConfig<'_>,
    slice_source: &CanonicalReplaySliceSource<'_>,
    settled: &mut ParallelSettledScan<'_>,
    sequence_digest: &mut CanonicalBlockFactsSequenceDigestBuilder,
    retained_undo: &mut RetainedUndo,
) -> Result<(), RocksDbWalletError> {
    let visible_tip = slice_source.ready_evidence().visible_tip;
    let Some(suffix_start) = config.settled_tip.height.next() else {
        return Ok(());
    };
    if suffix_start > visible_tip.height {
        return Ok(());
    }
    let suffix_shard = settled
        .shards
        .first_mut()
        .ok_or(RocksDbWalletError::CanonicalScanWorkerStopped)?;
    let suffix_scan = slice_source
        .scan_slice(BlockHeightRange::inclusive(
            suffix_start,
            visible_tip.height,
        ))
        .map_err(|source| RocksDbWalletError::CanonicalReplay { source })?;
    for replay in suffix_scan {
        let replay = replay.map_err(|source| RocksDbWalletError::CanonicalReplay { source })?;
        let block = stage_serial_block(
            config,
            replay,
            &mut settled.first_block,
            settled.previous_block,
            sequence_digest,
            retained_undo,
            suffix_shard,
            &mut settled.counters,
        )?;
        settled.previous_block = Some(block);
    }
    Ok(())
}

fn run_parallel_settled_chunks<'sorter>(
    parallel_sorter: &'sorter ParallelVariableValueSorter<OUTPOINT_EVENT_KEY_BYTES>,
    slice_source: &CanonicalReplaySliceSource<'_>,
    chunks: Vec<BlockHeightRange>,
    first_retained_block: BlockId,
    sequence_digest: &mut CanonicalBlockFactsSequenceDigestBuilder,
) -> Result<ParallelSettledScan<'sorter>, RocksDbWalletError> {
    let initial_shards = parallel_sorter.open_shards();
    let total_chunks = chunks.len();
    let worker_threads = initial_shards.len();
    let max_in_flight = worker_threads
        .saturating_mul(4)
        .clamp(worker_threads, MAX_IN_FLIGHT_SCAN_CHUNKS);
    let window = ChunkWindow::new(chunks, max_in_flight);
    let window_ref = &window;
    let (completed_tx, completed_rx) = mpsc::sync_channel::<
        Result<CompletedReplayChunk, RocksDbWalletError>,
    >(max_in_flight.saturating_add(worker_threads));
    std::thread::scope(|scope| {
        let mut workers = Vec::with_capacity(initial_shards.len());
        let mut start_failure = None;
        for (worker_index, shard) in initial_shards.into_iter().enumerate() {
            let sender = completed_tx.clone();
            match spawn_canonical_scan_worker(scope, worker_index, move || {
                scan_worker_chunks(slice_source, window_ref, shard, &sender)
            }) {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    window.stop();
                    start_failure = Some(error);
                    break;
                }
            }
        }
        drop(completed_tx);

        let verify_outcome = if start_failure.is_none() {
            verify_completed_chunks(
                &window,
                &completed_rx,
                first_retained_block,
                total_chunks,
                sequence_digest,
            )
        } else {
            Ok((None, None, ScanCounters::default()))
        };
        if verify_outcome.is_err() {
            window.stop();
        }

        let mut returned_shards = Vec::with_capacity(workers.len());
        let mut worker_stopped = false;
        for worker in workers {
            match worker.join() {
                Ok(shard) => returned_shards.push(shard),
                Err(_) => worker_stopped = true,
            }
        }
        if let Some(error) = start_failure {
            return Err(error);
        }
        let (first_block, previous_block, counters) = verify_outcome?;
        if worker_stopped {
            return Err(RocksDbWalletError::CanonicalScanWorkerStopped);
        }
        Ok(ParallelSettledScan {
            shards: returned_shards,
            first_block,
            previous_block,
            counters,
        })
    })
}

fn scan_worker_chunks<'sorter>(
    slice_source: &CanonicalReplaySliceSource<'_>,
    window: &ChunkWindow,
    mut shard: VariableValueSortShard<'sorter, OUTPOINT_EVENT_KEY_BYTES>,
    completed_tx: &SyncSender<Result<CompletedReplayChunk, RocksDbWalletError>>,
) -> VariableValueSortShard<'sorter, OUTPOINT_EVENT_KEY_BYTES> {
    let _stop_on_unwind = StopWindowOnUnwind(window);
    while let Some((chunk_index, range)) = window.claim_next() {
        #[cfg(test)]
        window.maybe_inject_worker_unwind(chunk_index);
        let outcome = scan_one_settled_chunk(slice_source, window, chunk_index, range, &mut shard);
        let failed = outcome.is_err();
        if failed {
            window.stop();
        }
        if completed_tx.send(outcome).is_err() || failed {
            break;
        }
    }
    shard
}

fn scan_one_settled_chunk(
    slice_source: &CanonicalReplaySliceSource<'_>,
    window: &ChunkWindow,
    chunk_index: usize,
    range: BlockHeightRange,
    shard: &mut VariableValueSortShard<'_, OUTPOINT_EVENT_KEY_BYTES>,
) -> Result<CompletedReplayChunk, RocksDbWalletError> {
    let expected_blocks =
        usize::try_from(u64::from(range.end.value() - range.start.value()) + 1).unwrap_or(0);
    let mut digests = Vec::with_capacity(expected_blocks);
    let mut first_block = None;
    let mut first_parent_hash = None;
    let mut last_block = None;
    let mut counters = ScanCounters::default();
    let scan = slice_source
        .scan_slice(range)
        .map_err(|source| RocksDbWalletError::CanonicalReplay { source })?;
    for replay in scan {
        if window.is_stopped() {
            return Err(RocksDbWalletError::CanonicalScanWorkerStopped);
        }
        let replay = replay.map_err(|source| RocksDbWalletError::CanonicalReplay { source })?;
        let reference_digest = replay.reference_digest();
        let facts = replay.into_facts();
        let block = BlockId::new(facts.block_header.height, facts.block_header.block_hash);
        if first_block.is_none() {
            first_block = Some(block);
            first_parent_hash = Some(facts.block_header.parent_hash);
        }
        last_block = Some(block);
        digests.push(reference_digest);
        counters.blocks = increment(counters.blocks)?;
        stage_settled_block_transactions(&facts, block, shard, &mut counters)?;
    }
    let first_block = first_block.ok_or(RocksDbWalletError::EmptyCanonicalHistory)?;
    let first_parent_hash = first_parent_hash.ok_or(RocksDbWalletError::EmptyCanonicalHistory)?;
    let last_block = last_block.ok_or(RocksDbWalletError::EmptyCanonicalHistory)?;
    Ok(CompletedReplayChunk {
        chunk_index,
        first_block,
        first_parent_hash,
        last_block,
        counters,
        digests,
    })
}

fn verify_completed_chunks(
    window: &ChunkWindow,
    completed_rx: &Receiver<Result<CompletedReplayChunk, RocksDbWalletError>>,
    first_retained_block: BlockId,
    total_chunks: usize,
    sequence_digest: &mut CanonicalBlockFactsSequenceDigestBuilder,
) -> Result<(Option<BlockId>, Option<BlockId>, ScanCounters), RocksDbWalletError> {
    let mut reorder = BTreeMap::new();
    let mut next_to_verify = 0_usize;
    let mut first_block = None;
    let mut previous_block = None;
    let mut counters = ScanCounters::default();
    while next_to_verify < total_chunks {
        let received = completed_rx
            .recv()
            .map_err(|_| RocksDbWalletError::CanonicalScanWorkerStopped)??;
        reorder.insert(received.chunk_index, received);
        while let Some(ready) = reorder.remove(&next_to_verify) {
            validate_next_block(
                first_retained_block,
                previous_block,
                ready.first_block,
                ready.first_parent_hash,
            )?;
            first_block.get_or_insert(ready.first_block);
            for digest in ready.digests {
                sequence_digest.try_append(digest)?;
            }
            counters.combine(&ready.counters)?;
            previous_block = Some(ready.last_block);
            next_to_verify = next_to_verify.saturating_add(1);
            window.mark_verified(next_to_verify);
        }
    }
    Ok((first_block, previous_block, counters))
}

fn stage_settled_block_transactions(
    facts: &CanonicalBlockFacts,
    block: BlockId,
    sink: &mut impl OutpointEventSink,
    counters: &mut ScanCounters,
) -> Result<(), RocksDbWalletError> {
    for (transaction_index, transaction) in facts.transactions.iter().enumerate() {
        let tx_index_in_block = u32::try_from(transaction_index)
            .map_err(|_| WalletProjectionContractError::FactIndexOverflow)?;
        counters.transactions = increment(counters.transactions)?;
        let transaction_position = WalletTransactionPosition::new(
            transaction.public_facts.transaction_id,
            tx_index_in_block,
            block,
        );
        for (input_position, input) in transaction.transparent_inputs.iter().enumerate() {
            let expected_input_index = u32::try_from(input_position)
                .map_err(|_| WalletProjectionContractError::FactIndexOverflow)?;
            if input.input_index != expected_input_index {
                return Err(WalletProjectionContractError::FactIndexMismatch.into());
            }
            if input.spent_outpoint.is_coinbase_sentinel() {
                continue;
            }
            let outpoint_key = WalletOutpointKey::new(input.spent_outpoint);
            sink.push_event(
                outpoint_event_key(outpoint_key, SPEND_EVENT_TAG),
                &encode_spend_trailer(StagedSpend {
                    spent_at: transaction_position,
                    input_index: input.input_index,
                }),
            )?;
            counters.staged_spends = increment(counters.staged_spends)?;
        }
        for (output_position, output) in transaction.transparent_outputs.iter().enumerate() {
            let expected_output_index = u32::try_from(output_position)
                .map_err(|_| WalletProjectionContractError::FactIndexOverflow)?;
            if output.output_index != expected_output_index {
                return Err(WalletProjectionContractError::FactIndexMismatch.into());
            }
            let outpoint = zinder_core::TransparentOutPoint::new(
                transaction.public_facts.transaction_id,
                output.output_index,
            );
            let outpoint_key = WalletOutpointKey::new(outpoint);
            let wallet_output = WalletUnspentOutput::new(
                outpoint,
                output.address_script_hash,
                output.value_zat,
                output.script_pub_key.clone(),
                transaction_position,
            )?;
            sink.push_event(
                outpoint_event_key(outpoint_key, OUTPUT_EVENT_TAG),
                &wallet_output.encode_value()?,
            )?;
            counters.staged_outputs = increment(counters.staged_outputs)?;
        }
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "serial block staging updates sequence digest, retained undo, outpoint sink, and counters together"
)]
#[allow(
    clippy::too_many_lines,
    reason = "serial block staging keeps input/output validation and transient undo accounting in one loop"
)]
fn stage_serial_block(
    config: WalletProjectionLoadConfig<'_>,
    replay: ValidatedCanonicalBlockReplay,
    first_block: &mut Option<BlockId>,
    previous_block: Option<BlockId>,
    sequence_digest: &mut CanonicalBlockFactsSequenceDigestBuilder,
    retained_undo: &mut RetainedUndo,
    sink: &mut impl OutpointEventSink,
    counters: &mut ScanCounters,
) -> Result<BlockId, RocksDbWalletError> {
    let reference_digest = replay.reference_digest();
    let facts = replay.into_facts();
    let block = BlockId::new(facts.block_header.height, facts.block_header.block_hash);
    validate_next_block(
        config.first_retained_block,
        previous_block,
        block,
        facts.block_header.parent_hash,
    )?;
    first_block.get_or_insert(block);
    let source_sequence_digest_before = sequence_digest.clone().finish();
    sequence_digest.try_append(reference_digest)?;
    let source_sequence_digest_after = sequence_digest.clone().finish();
    counters.blocks = increment(counters.blocks)?;

    retained_undo.begin_block(
        block,
        facts.block_header.parent_hash,
        source_sequence_digest_before,
        source_sequence_digest_after,
    )?;
    let mut block_created_outpoints = BTreeSet::new();
    for (transaction_index, transaction) in facts.transactions.iter().enumerate() {
        let tx_index_in_block = u32::try_from(transaction_index)
            .map_err(|_| WalletProjectionContractError::FactIndexOverflow)?;
        counters.transactions = increment(counters.transactions)?;
        let transaction_position = WalletTransactionPosition::new(
            transaction.public_facts.transaction_id,
            tx_index_in_block,
            block,
        );
        for (input_position, input) in transaction.transparent_inputs.iter().enumerate() {
            let expected_input_index = u32::try_from(input_position)
                .map_err(|_| WalletProjectionContractError::FactIndexOverflow)?;
            if input.input_index != expected_input_index {
                return Err(WalletProjectionContractError::FactIndexMismatch.into());
            }
            if input.spent_outpoint.is_coinbase_sentinel() {
                continue;
            }
            let outpoint_key = WalletOutpointKey::new(input.spent_outpoint);
            sink.push_event(
                outpoint_event_key(outpoint_key, SPEND_EVENT_TAG),
                &encode_spend_trailer(StagedSpend {
                    spent_at: transaction_position,
                    input_index: input.input_index,
                }),
            )?;
            counters.staged_spends = increment(counters.staged_spends)?;
            if !block_created_outpoints.contains(&outpoint_key) {
                retained_undo.add_spent_outpoint(outpoint_key)?;
            }
        }
        for (output_position, output) in transaction.transparent_outputs.iter().enumerate() {
            let expected_output_index = u32::try_from(output_position)
                .map_err(|_| WalletProjectionContractError::FactIndexOverflow)?;
            if output.output_index != expected_output_index {
                return Err(WalletProjectionContractError::FactIndexMismatch.into());
            }
            let outpoint = zinder_core::TransparentOutPoint::new(
                transaction.public_facts.transaction_id,
                output.output_index,
            );
            let outpoint_key = WalletOutpointKey::new(outpoint);
            let wallet_output = WalletUnspentOutput::new(
                outpoint,
                output.address_script_hash,
                output.value_zat,
                output.script_pub_key.clone(),
                transaction_position,
            )?;
            sink.push_event(
                outpoint_event_key(outpoint_key, OUTPUT_EVENT_TAG),
                &wallet_output.encode_value()?,
            )?;
            counters.staged_outputs = increment(counters.staged_outputs)?;
            if config.supported_reorg_depth != 0 && !block_created_outpoints.contains(&outpoint_key)
            {
                retained_undo
                    .reserve_transient(u64::try_from(size_of::<WalletOutpointKey>()).map_err(
                        |_| RocksDbWalletError::WalletProjectionLoadAccountingOverflow,
                    )?)?;
                block_created_outpoints.insert(outpoint_key);
            }
            retained_undo.add_created_outpoint(outpoint_key)?;
        }
    }
    retained_undo.release_transient(
        u64::try_from(block_created_outpoints.len())
            .ok()
            .and_then(|count| {
                u64::try_from(size_of::<WalletOutpointKey>())
                    .ok()
                    .and_then(|bytes| count.checked_mul(bytes))
            })
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?,
    )?;
    retained_undo.finish_block()?;
    Ok(block)
}

#[allow(
    clippy::too_many_lines,
    reason = "the loader keeps the external drains and evidence fence in visible order"
)]
fn finish_projection_ssts(
    config: WalletProjectionLoadConfig<'_>,
    scanned: CompletedCanonicalScan,
) -> Result<PreparedWalletProjectionLoad, RocksDbWalletError> {
    let CompletedCanonicalScan {
        first_block,
        tip,
        source_sequence_digest,
        mut retained_undo,
        scanned_block_count,
        scanned_transaction_count,
        staged_output_count,
        staged_spend_count,
        canonical_scan,
        outpoint_sort,
        mut sorted_outpoints,
        outpoint_sort_evidence,
        outpoint_sort_durations,
    } = scanned;

    let phase_started = Instant::now();
    let mut address_index_sorter = VariableValueSorter::<ADDRESS_UNSPENT_KEY_BYTES>::new(
        config.staging_path,
        "wallet-address-unspent",
        config.max_secondary_sort_memory_bytes_per_sorter,
        config.max_temporary_file_bytes_per_sorter,
    )?;
    let mut address_transaction_sorter = VariableValueSorter::<ADDRESS_TRANSACTION_KEY_BYTES>::new(
        config.staging_path,
        "wallet-address-transactions",
        config.max_secondary_sort_memory_bytes_per_sorter,
        config.max_temporary_file_bytes_per_sorter,
    )?;
    let mut unspent_writer = ordered_writer(
        config,
        WalletProjectionRowFamily::TransparentUnspentOutput,
        "wallet-unspent-output",
    )?;
    let mut spent_writer = ordered_writer(
        config,
        WalletProjectionRowFamily::TransparentSpentOutput,
        "wallet-spent-output",
    )?;
    let mut accumulators =
        ConstructionAccumulators::start(config.network, config.accumulator_hashing_workers)?;
    let mut sst_evidence = WalletProjectionSstEvidence::default();
    let mut utxo_count = 0_u64;
    let mut total_value_zat = 0_u64;
    drain_outpoint_events(
        &mut sorted_outpoints,
        &mut unspent_writer,
        &mut spent_writer,
        &mut address_index_sorter,
        &mut address_transaction_sorter,
        &mut accumulators,
        &mut sst_evidence,
        &mut utxo_count,
        &mut total_value_zat,
    )?;
    accumulators.settle()?;
    let (unspent_files, unspent_write) = unspent_writer.finish()?;
    let (spent_files, spent_write) = spent_writer.finish()?;
    let outpoint_merge = phase_started.elapsed();

    let phase_started = Instant::now();
    let step_started = Instant::now();
    let mut sorted_address_index = address_index_sorter.finish()?;
    let address_index_sort_evidence = sorted_address_index.evidence();
    let address_index_sort_durations = sorted_address_index.durations();
    let address_index_sort = step_started.elapsed();
    let step_started = Instant::now();
    let mut address_index_writer = ordered_writer(
        config,
        WalletProjectionRowFamily::TransparentUnspentOutputByAddress,
        "wallet-address-unspent-index",
    )?;
    let mut address_balance_writer = ordered_writer(
        config,
        WalletProjectionRowFamily::TransparentAddressBalance,
        "wallet-address-balance",
    )?;
    drain_address_index(
        &mut sorted_address_index,
        &mut address_index_writer,
        &mut address_balance_writer,
        &mut accumulators,
        &mut sst_evidence,
    )?;
    accumulators.settle()?;
    let (address_index_files, address_index_write) = address_index_writer.finish()?;
    let (address_balance_files, address_balance_write) = address_balance_writer.finish()?;
    let address_index_drain = step_started.elapsed();

    let step_started = Instant::now();
    let mut sorted_address_transactions = address_transaction_sorter.finish()?;
    let address_transaction_sort_evidence = sorted_address_transactions.evidence();
    let address_transaction_sort_durations = sorted_address_transactions.durations();
    let address_transaction_sort = step_started.elapsed();
    let step_started = Instant::now();
    let mut address_transaction_writer = ordered_writer(
        config,
        WalletProjectionRowFamily::TransparentAddressTransaction,
        "wallet-address-transaction",
    )?;
    drain_address_transactions(
        &mut sorted_address_transactions,
        &mut address_transaction_writer,
        &mut retained_undo,
        &mut accumulators,
        &mut sst_evidence,
    )?;
    accumulators.settle()?;
    let (address_transaction_files, address_transaction_write) =
        address_transaction_writer.finish()?;
    let address_transaction_drain = step_started.elapsed();

    let step_started = Instant::now();
    let mut reorg_undo_writer = ordered_writer(
        config,
        WalletProjectionRowFamily::ReorgUndo,
        "wallet-reorg-undo",
    )?;
    for mut undo in retained_undo.records {
        undo.address_transaction_keys.sort_unstable();
        let key = undo.encode_key();
        let encoded_value = undo.encode_value()?;
        append_row(
            &mut reorg_undo_writer,
            &mut accumulators,
            &mut sst_evidence,
            WalletProjectionRowFamily::ReorgUndo,
            &key,
            &encoded_value,
        )?;
    }
    let (reorg_undo_files, reorg_undo_write) = reorg_undo_writer.finish()?;
    let reorg_undo_write_step = step_started.elapsed();
    let secondary_row_derivation = phase_started.elapsed();

    let phase_started = Instant::now();
    let (digest, commitment) = accumulators.finish()?;
    let row_counts = digest.row_counts();
    let (projection_accumulator, projection_digest) = digest.finish_with_accumulator();
    let logical_evidence = phase_started.elapsed();
    let artifacts = [
        (TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY, unspent_files),
        (
            TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
            address_index_files,
        ),
        (TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY, spent_files),
        (
            TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY,
            address_transaction_files,
        ),
        (
            TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY,
            address_balance_files,
        ),
        (REORG_UNDO_COLUMN_FAMILY, reorg_undo_files),
    ];
    let (families, sst_file_bytes, sst_file_count) = prepare_families(artifacts)?;

    Ok(PreparedWalletProjectionLoad {
        network: config.network,
        supported_reorg_depth: config.supported_reorg_depth,
        first_block,
        tip,
        source_sequence_digest,
        projection_digest,
        projection_accumulator,
        row_counts,
        utxo_summary: WalletUtxoSetSummary {
            utxo_count,
            total_value_zat,
            commitment,
        },
        counters: WalletProjectionLoadCounters {
            scanned_block_count,
            scanned_transaction_count,
            staged_output_count,
            staged_spend_count,
            historical_prevout_read_count: 0,
            peak_accounted_reorg_undo_bytes: retained_undo.peak_accounted_bytes,
            max_accounted_reorg_undo_bytes: retained_undo.max_accounted_bytes,
        },
        phase_durations: WalletProjectionLoadPhaseDurations {
            canonical_scan,
            outpoint_sort,
            outpoint_merge,
            secondary_row_derivation,
            secondary_row_derivation_phases: WalletSecondaryRowDerivationPhaseDurations {
                address_index_sort,
                address_index_drain,
                address_transaction_sort,
                address_transaction_drain,
                reorg_undo_write: reorg_undo_write_step,
            },
            sst_write: WalletSstWriteDurations {
                primary_outputs: unspent_write.saturating_add(spent_write),
                address_index_and_balance: address_index_write
                    .saturating_add(address_balance_write),
                address_transaction: address_transaction_write,
                reorg_undo: reorg_undo_write,
            },
            logical_evidence,
        },
        outpoint_sort_evidence,
        address_index_sort_evidence,
        address_transaction_sort_evidence,
        outpoint_sort_durations,
        address_index_sort_durations,
        address_transaction_sort_durations,
        logical_row_bytes: sst_evidence.logical_row_bytes,
        sst_file_bytes,
        sst_file_count,
        families,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "the streaming drain owns three output sinks and exact evidence accumulators"
)]
fn drain_outpoint_events(
    sorted: &mut SortedVariableValues<OUTPOINT_EVENT_KEY_BYTES>,
    unspent_writer: &mut TimedSstWriter<'_>,
    spent_writer: &mut TimedSstWriter<'_>,
    address_index_sorter: &mut VariableValueSorter<ADDRESS_UNSPENT_KEY_BYTES>,
    address_transaction_sorter: &mut VariableValueSorter<ADDRESS_TRANSACTION_KEY_BYTES>,
    accumulators: &mut ConstructionAccumulators,
    sst_evidence: &mut WalletProjectionSstEvidence,
    utxo_count: &mut u64,
    total_value_zat: &mut u64,
) -> Result<(), RocksDbWalletError> {
    let mut pending = sorted.next_record()?;
    while let Some(first) = pending.take() {
        let outpoint_key = WalletOutpointKey::decode(&first.key[..36])?;
        let mut output = None;
        let mut spend = None;
        accept_outpoint_event(&first.key, &first.encoded_value, &mut output, &mut spend)?;
        loop {
            let next = sorted.next_record()?;
            let Some(next_record) = next else {
                pending = None;
                break;
            };
            if next_record.key[..36] != first.key[..36] {
                pending = Some(next_record);
                break;
            }
            accept_outpoint_event(
                &next_record.key,
                &next_record.encoded_value,
                &mut output,
                &mut spend,
            )?;
        }
        let output = output.ok_or(WalletProjectionContractError::MissingTransparentPredecessor)?;
        stage_address_transaction(&output, output.created_at, address_transaction_sorter)?;
        if let Some(spend) = spend {
            if !precedes(output.created_at, spend.spent_at) {
                return Err(WalletProjectionContractError::MissingTransparentPredecessor.into());
            }
            let spent_output = WalletSpentOutput::new(output, spend.spent_at, spend.input_index);
            stage_address_transaction(
                &spent_output.output,
                spent_output.spent_at,
                address_transaction_sorter,
            )?;
            let encoded_value = spent_output.encode_value()?;
            append_row(
                spent_writer,
                accumulators,
                sst_evidence,
                WalletProjectionRowFamily::TransparentSpentOutput,
                outpoint_key.as_bytes(),
                &encoded_value,
            )?;
        } else {
            let address_key = WalletAddressUnspentOutputKey::new(&output);
            address_index_sorter.push(*address_key.as_bytes(), &output.value_zat.to_be_bytes())?;
            *utxo_count = increment(*utxo_count)?;
            *total_value_zat = total_value_zat
                .checked_add(output.value_zat)
                .ok_or(WalletProjectionContractError::UtxoValueOverflow)?;
            accumulators.insert_unspent_output(&output)?;
            let encoded_value = output.encode_value()?;
            append_row(
                unspent_writer,
                accumulators,
                sst_evidence,
                WalletProjectionRowFamily::TransparentUnspentOutput,
                outpoint_key.as_bytes(),
                &encoded_value,
            )?;
        }
    }
    Ok(())
}

fn accept_outpoint_event(
    key: &[u8; OUTPOINT_EVENT_KEY_BYTES],
    encoded_value: &[u8],
    output: &mut Option<WalletUnspentOutput>,
    spend: &mut Option<StagedSpend>,
) -> Result<(), RocksDbWalletError> {
    let outpoint_key = WalletOutpointKey::decode(&key[..36])?;
    match key[36] {
        OUTPUT_EVENT_TAG => {
            if output.is_some() {
                return Err(WalletProjectionContractError::DuplicateOutput.into());
            }
            *output = Some(WalletUnspentOutput::decode_value(
                outpoint_key,
                encoded_value,
            )?);
        }
        SPEND_EVENT_TAG => {
            if spend.is_some() {
                return Err(WalletProjectionContractError::DuplicateSpend.into());
            }
            *spend = Some(decode_spend_trailer(encoded_value)?);
        }
        _ => {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "outpoint event sort contains an unsupported tag",
            });
        }
    }
    Ok(())
}

fn drain_address_index(
    sorted: &mut SortedVariableValues<ADDRESS_UNSPENT_KEY_BYTES>,
    index_writer: &mut TimedSstWriter<'_>,
    balance_writer: &mut TimedSstWriter<'_>,
    accumulators: &mut ConstructionAccumulators,
    evidence: &mut WalletProjectionSstEvidence,
) -> Result<(), RocksDbWalletError> {
    let mut current_address = None;
    let mut current_balance = 0_u64;
    while let Some(record) = sorted.next_record()? {
        if record.encoded_value.len() != 8 {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "address-index staging value is not an exact u64",
            });
        }
        let address_key = WalletAddressUnspentOutputKey::decode(&record.key)?;
        let address = address_key.address_script_hash();
        if current_address.is_some_and(|current| current != address) {
            write_balance(
                current_address.ok_or(WalletProjectionContractError::AddressBalanceUnderflow)?,
                current_balance,
                balance_writer,
                accumulators,
                evidence,
            )?;
            current_balance = 0;
        }
        current_address = Some(address);
        let value_zat = u64::from_be_bytes(record.encoded_value.try_into().map_err(|_| {
            RocksDbWalletError::AdmissionChanged {
                reason: "address-index staging value changed after length validation",
            }
        })?);
        current_balance = current_balance
            .checked_add(value_zat)
            .ok_or(WalletProjectionContractError::AddressBalanceOverflow)?;
        append_row(
            index_writer,
            accumulators,
            evidence,
            WalletProjectionRowFamily::TransparentUnspentOutputByAddress,
            &record.key,
            &[],
        )?;
    }
    if let Some(address) = current_address {
        write_balance(
            address,
            current_balance,
            balance_writer,
            accumulators,
            evidence,
        )?;
    }
    Ok(())
}

fn write_balance(
    address: zinder_core::TransparentAddressScriptHash,
    balance_zat: u64,
    writer: &mut TimedSstWriter<'_>,
    accumulators: &mut ConstructionAccumulators,
    evidence: &mut WalletProjectionSstEvidence,
) -> Result<(), RocksDbWalletError> {
    if balance_zat == 0 {
        return Ok(());
    }
    let balance = WalletAddressBalance {
        address_script_hash: address,
        balance_zat,
    };
    append_row(
        writer,
        accumulators,
        evidence,
        WalletProjectionRowFamily::TransparentAddressBalance,
        &balance.encode_key(),
        &balance.encode_value(),
    )
}

fn drain_address_transactions(
    sorted: &mut SortedVariableValues<ADDRESS_TRANSACTION_KEY_BYTES>,
    writer: &mut TimedSstWriter<'_>,
    retained_undo: &mut RetainedUndo,
    accumulators: &mut ConstructionAccumulators,
    evidence: &mut WalletProjectionSstEvidence,
) -> Result<(), RocksDbWalletError> {
    let mut pending = sorted.next_record()?;
    while let Some(first) = pending.take() {
        if first.encoded_value.len() != 64 {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "address-transaction staging value is not exact64",
            });
        }
        let mut canonical_value = first.encoded_value;
        loop {
            let next = sorted.next_record()?;
            let Some(next_record) = next else {
                pending = None;
                break;
            };
            if next_record.key != first.key {
                pending = Some(next_record);
                break;
            }
            if next_record.encoded_value != canonical_value {
                return Err(RocksDbWalletError::AddressTransactionConflict);
            }
            canonical_value = next_record.encoded_value;
        }
        let key = WalletAddressTransactionKey::decode(&first.key)?;
        WalletAddressTransaction::decode_value(key, &canonical_value)?;
        retained_undo.add_address_transaction(key)?;
        append_row(
            writer,
            accumulators,
            evidence,
            WalletProjectionRowFamily::TransparentAddressTransaction,
            &first.key,
            &canonical_value,
        )?;
    }
    Ok(())
}

fn stage_address_transaction(
    output: &WalletUnspentOutput,
    position: WalletTransactionPosition,
    sorter: &mut VariableValueSorter<ADDRESS_TRANSACTION_KEY_BYTES>,
) -> Result<(), RocksDbWalletError> {
    let key = WalletAddressTransactionKey::new(
        output.address_script_hash,
        position.block.height,
        position.tx_index_in_block,
    );
    let transaction =
        WalletAddressTransaction::new(key, position.transaction_id, position.block.hash);
    sorter.push(*key.as_bytes(), &transaction.encode_value())?;
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "one row must update its physical writer and both pieces of logical evidence together"
)]
fn append_row(
    writer: &mut TimedSstWriter<'_>,
    accumulators: &mut ConstructionAccumulators,
    evidence: &mut WalletProjectionSstEvidence,
    family: WalletProjectionRowFamily,
    key: &[u8],
    encoded_value: &[u8],
) -> Result<(), RocksDbWalletError> {
    writer.put(key, encoded_value)?;
    accumulators.append_row(family, key, encoded_value)?;
    evidence.add_row(key, encoded_value)
}

fn ordered_writer<'options>(
    config: WalletProjectionLoadConfig<'options>,
    family: WalletProjectionRowFamily,
    artifact_label: &'static str,
) -> Result<TimedSstWriter<'options>, RocksDbWalletError> {
    Ok(TimedSstWriter {
        writer: OrderedSstWriter::new(
            config.staging_path,
            artifact_label,
            config.options.for_family(family),
            config.sst_target_logical_bytes,
        )?,
        write_duration: Duration::ZERO,
    })
}

/// Ordered SST writer that keeps the wall time spent inside it.
///
/// The time covers each row's encoding, file rotation, and the final file
/// close, so the build can separate SST writing from the drain that feeds it.
struct TimedSstWriter<'options> {
    writer: OrderedSstWriter<'options>,
    write_duration: Duration,
}

impl TimedSstWriter<'_> {
    fn put(&mut self, key: &[u8], encoded_value: &[u8]) -> Result<(), BulkLoadError> {
        let started = Instant::now();
        let outcome = self.writer.put(key, encoded_value);
        self.write_duration = self.write_duration.saturating_add(started.elapsed());
        outcome
    }

    fn finish(self) -> Result<(SstFileSet, Duration), BulkLoadError> {
        let started = Instant::now();
        let files = self.writer.finish()?;
        Ok((files, self.write_duration.saturating_add(started.elapsed())))
    }
}

fn prepare_families(
    artifacts: [(&'static str, SstFileSet); 6],
) -> Result<(Vec<PreparedWalletColumnFamily>, u64, u64), RocksDbWalletError> {
    let mut families = Vec::with_capacity(artifacts.len());
    let mut sst_file_bytes = 0_u64;
    let mut sst_file_count = 0_u64;
    for (name, files) in artifacts {
        sst_file_bytes = sst_file_bytes
            .checked_add(files.file_bytes)
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        sst_file_count = sst_file_count
            .checked_add(
                u64::try_from(files.paths.len())
                    .map_err(|_| RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?,
            )
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        families.push(PreparedWalletColumnFamily {
            name,
            paths: files.paths,
        });
    }
    Ok((families, sst_file_bytes, sst_file_count))
}

fn validate_next_block(
    first_retained_block: BlockId,
    previous: Option<BlockId>,
    block: BlockId,
    parent_hash: BlockHash,
) -> Result<(), WalletProjectionContractError> {
    match previous {
        None if block == first_retained_block => Ok(()),
        Some(previous)
            if previous.height.next() == Some(block.height) && previous.hash == parent_hash =>
        {
            Ok(())
        }
        None | Some(_) => Err(WalletProjectionContractError::NonContiguousBlock),
    }
}

fn precedes(created_at: WalletTransactionPosition, spent_at: WalletTransactionPosition) -> bool {
    if created_at.block.height < spent_at.block.height {
        return true;
    }
    created_at.block.height == spent_at.block.height
        && created_at.block.hash == spent_at.block.hash
        && created_at.tx_index_in_block < spent_at.tx_index_in_block
}

fn outpoint_event_key(key: WalletOutpointKey, tag: u8) -> [u8; OUTPOINT_EVENT_KEY_BYTES] {
    let mut encoded = [0; OUTPOINT_EVENT_KEY_BYTES];
    encoded[..36].copy_from_slice(key.as_bytes());
    encoded[36] = tag;
    encoded
}

fn encode_spend_trailer(spend: StagedSpend) -> [u8; SPEND_TRAILER_BYTES] {
    let mut encoded = [0; SPEND_TRAILER_BYTES];
    encoded[..32].copy_from_slice(&spend.spent_at.transaction_id.as_bytes());
    encoded[32..36].copy_from_slice(&spend.input_index.to_be_bytes());
    encoded[36..40].copy_from_slice(&spend.spent_at.tx_index_in_block.to_be_bytes());
    encoded[40..44].copy_from_slice(&spend.spent_at.block.height.value().to_be_bytes());
    encoded[44..].copy_from_slice(&spend.spent_at.block.hash.as_bytes());
    encoded
}

fn decode_spend_trailer(encoded: &[u8]) -> Result<StagedSpend, RocksDbWalletError> {
    let encoded: [u8; SPEND_TRAILER_BYTES] =
        encoded
            .try_into()
            .map_err(|_| RocksDbWalletError::AdmissionChanged {
                reason: "spend staging value is not the exact version-1 76-byte trailer",
            })?;
    let mut transaction_id = [0; 32];
    transaction_id.copy_from_slice(&encoded[..32]);
    let mut input_index = [0; 4];
    input_index.copy_from_slice(&encoded[32..36]);
    let mut tx_index = [0; 4];
    tx_index.copy_from_slice(&encoded[36..40]);
    let mut height = [0; 4];
    height.copy_from_slice(&encoded[40..44]);
    let mut block_hash = [0; 32];
    block_hash.copy_from_slice(&encoded[44..]);
    Ok(StagedSpend {
        spent_at: WalletTransactionPosition::new(
            TransactionId::from_bytes(transaction_id),
            u32::from_be_bytes(tx_index),
            BlockId::new(
                BlockHeight::new(u32::from_be_bytes(height)),
                BlockHash::from_bytes(block_hash),
            ),
        ),
        input_index: u32::from_be_bytes(input_index),
    })
}

fn increment(counter: u64) -> Result<u64, RocksDbWalletError> {
    add_counter(counter, 1)
}

fn add_counter(counter: u64, delta: u64) -> Result<u64, RocksDbWalletError> {
    counter
        .checked_add(delta)
        .ok_or(RocksDbWalletError::BuildCounterOverflow)
}

#[cfg(test)]
mod tests {
    use std::{error::Error, path::Path};

    use rust_rocksdb::Cache;
    use tempfile::TempDir;
    use zinder_core::{
        BlockHeaderArtifact, CanonicalBlockFacts, CanonicalBlockFactsDigestVersion,
        CanonicalBlockReplayFormatVersion, CanonicalTransactionFacts, LockTime, PrivacyShape,
        SerializedBytesDigest, TransactionComponentCounts, TransactionIntrinsicValueBalances,
        TransactionPublicFacts, TransactionVersion, TransparentAddressScriptHash,
        TransparentInputFact, TransparentOutPoint, TransparentOutputFact,
        decode_canonical_block_replay, encode_canonical_block_replay,
    };
    use zinder_rocksdb_bulk_load::BulkLoadError;
    use zinder_store::RocksDbResourceBudget;
    use zinder_wallet_projection::WalletProjectionSerialOracle;

    use super::*;

    const TEST_SORT_MEMORY_BYTES: u64 = 1024 * 1024;
    const TEST_TEMPORARY_FILE_BYTES: u64 = 16 * 1024 * 1024;
    const TEST_SST_LOGICAL_BYTES: u64 = 1024 * 1024;
    const TEST_REORG_UNDO_BYTES: u64 = 1024 * 1024;
    const TEST_BLOCK_CACHE_BYTES: usize = 1024 * 1024;

    #[derive(Clone, Copy)]
    struct TestByteLimits {
        outpoint_sort: u64,
        secondary_sort_per_sorter: u64,
        temporary_files_per_sorter: u64,
    }

    impl Default for TestByteLimits {
        fn default() -> Self {
            Self {
                outpoint_sort: TEST_SORT_MEMORY_BYTES,
                secondary_sort_per_sorter: TEST_SORT_MEMORY_BYTES,
                temporary_files_per_sorter: TEST_TEMPORARY_FILE_BYTES,
            }
        }
    }

    #[test]
    fn retained_undo_accounting_bounds_a_thousand_block_unsettled_suffix()
    -> Result<(), Box<dyn Error>> {
        const DEPTH: u32 = 1_000;
        // The projector's default `build.max_accounted_reorg_undo_bytes`.
        const DEFAULT_UNDO_CAP_BYTES: u64 = 512 * 1024 * 1024;
        // Created and spent outpoints per block in a transparent-heavy suffix.
        const OUTPOINTS_PER_BLOCK: u32 = 64;
        let settled_tip = BlockId::new(BlockHeight::new(10_000), BlockHash::from_bytes([1; 32]));
        let digest = CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            CanonicalBlockFactsSequenceDigestVersion::V1,
            1,
            [2; 32],
        );
        let fill = |undo: &mut RetainedUndo, block_count: u32| {
            for offset in 1..=block_count {
                let height = BlockHeight::new(settled_tip.height.value() + offset);
                let hash = BlockHash::from_bytes([u8::try_from(offset % 251).unwrap_or(0); 32]);
                undo.begin_block(BlockId::new(height, hash), hash, digest, digest)?;
                for output_index in 0..OUTPOINTS_PER_BLOCK {
                    let mut transaction_id = [0; 32];
                    transaction_id[..4].copy_from_slice(&offset.to_be_bytes());
                    undo.add_created_outpoint(WalletOutpointKey::new(TransparentOutPoint::new(
                        TransactionId::from_bytes(transaction_id),
                        output_index,
                    )))?;
                    transaction_id[4] = 1;
                    undo.add_spent_outpoint(WalletOutpointKey::new(TransparentOutPoint::new(
                        TransactionId::from_bytes(transaction_id),
                        output_index,
                    )))?;
                }
                undo.finish_block()?;
            }
            Ok::<_, RocksDbWalletError>(())
        };
        let per_block_bytes = u64::try_from(
            size_of::<WalletReorgUndo>()
                + 2 * usize::try_from(OUTPOINTS_PER_BLOCK)? * size_of::<WalletOutpointKey>(),
        )?;
        let suffix_bytes = u64::from(DEPTH) * per_block_bytes;

        let mut undo = RetainedUndo::new(DEPTH, settled_tip, DEFAULT_UNDO_CAP_BYTES);
        fill(&mut undo, DEPTH)?;
        assert_eq!(undo.records.len(), usize::try_from(DEPTH)?);
        assert_eq!(undo.accounted_bytes, suffix_bytes);
        assert_eq!(undo.peak_accounted_bytes, suffix_bytes);
        assert!(suffix_bytes < DEFAULT_UNDO_CAP_BYTES / 64);
        let next_height = BlockHeight::new(settled_tip.height.value() + DEPTH + 1);
        assert!(matches!(
            undo.begin_block(
                BlockId::new(next_height, BlockHash::from_bytes([0xfe; 32])),
                BlockHash::from_bytes([0xfd; 32]),
                digest,
                digest,
            ),
            Err(RocksDbWalletError::WalletProjectionRebuildRequired { .. })
        ));

        // A cap below the complete suffix fails closed instead of growing.
        let mut capped = RetainedUndo::new(DEPTH, settled_tip, suffix_bytes - 1);
        let error = fill(&mut capped, DEPTH)
            .err()
            .ok_or("a suffix above the accounting cap must be refused")?;
        assert!(
            matches!(
                error,
                RocksDbWalletError::AccountedReorgUndoMemoryLimit {
                    limit_bytes,
                    required_bytes,
                } if limit_bytes == suffix_bytes - 1 && required_bytes == suffix_bytes
            ),
            "{error:?}"
        );
        Ok(())
    }

    #[test]
    fn external_loader_accepts_an_authenticated_checkpoint_successor_as_its_first_block()
    -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = test_column_family_options();
        let first_retained = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0xc2; 32]));
        let tip = BlockId::new(BlockHeight::new(3), BlockHash::from_bytes([0xc3; 32]));
        let blocks = vec![
            block_facts(2, [0xc1; 32], [0xc2; 32], Vec::new()),
            block_facts(3, [0xc2; 32], [0xc3; 32], Vec::new()),
        ];

        let prepared = write_projection_ssts(
            test_config_with_bounds(
                temporary.path(),
                &options,
                TestByteLimits::default(),
                first_retained,
                tip,
            ),
            canonical_scan(validated_replays(&blocks)?),
        )?;

        assert_eq!(prepared.first_block, first_retained);
        assert_eq!(prepared.tip, tip);
        assert_eq!(prepared.counters.scanned_block_count, 2);
        Ok(())
    }

    #[test]
    fn external_loader_rejects_a_first_block_outside_the_authenticated_retained_anchor()
    -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = test_column_family_options();
        let expected_first = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0xd2; 32]));
        let observed_first = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0xc2; 32]));
        let blocks = vec![block_facts(2, [0xc1; 32], [0xc2; 32], Vec::new())];

        let result = write_projection_ssts(
            test_config_with_bounds(
                temporary.path(),
                &options,
                TestByteLimits::default(),
                expected_first,
                observed_first,
            ),
            canonical_scan(validated_replays(&blocks)?),
        );

        assert!(matches!(
            result,
            Err(RocksDbWalletError::Contract(
                WalletProjectionContractError::NonContiguousBlock
            ))
        ));
        Ok(())
    }

    #[test]
    fn external_loader_rejects_duplicate_output_and_spend_keys() -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = test_column_family_options();
        let address = TransparentAddressScriptHash::from_bytes([0xa1; 32]);
        let transaction = TransactionId::from_bytes([0xb1; 32]);
        let outpoint = TransparentOutPoint::new(transaction, 0);
        let duplicate_outputs = vec![block_facts(
            1,
            [0; 32],
            [0xc1; 32],
            vec![
                transaction_facts(
                    transaction,
                    Vec::new(),
                    vec![TransparentOutputFact::new(0, 1, [0x51], address)],
                ),
                transaction_facts(
                    transaction,
                    Vec::new(),
                    vec![TransparentOutputFact::new(0, 1, [0x51], address)],
                ),
            ],
        )];
        let duplicate_output_replays = validated_replays(&duplicate_outputs)?;
        let duplicate_output_result = write_projection_ssts(
            test_config(temporary.path(), &options, TestByteLimits::default()),
            canonical_scan(duplicate_output_replays),
        );
        assert!(matches!(
            duplicate_output_result,
            Err(RocksDbWalletError::Contract(
                WalletProjectionContractError::DuplicateOutput
            ))
        ));

        let duplicate_spends = vec![
            block_facts(
                1,
                [0; 32],
                [0xc1; 32],
                vec![transaction_facts(
                    transaction,
                    Vec::new(),
                    vec![TransparentOutputFact::new(0, 1, [0x51], address)],
                )],
            ),
            block_facts(
                2,
                [0xc1; 32],
                [0xc2; 32],
                vec![transaction_facts(
                    TransactionId::from_bytes([0xb2; 32]),
                    vec![
                        TransparentInputFact::new(0, outpoint),
                        TransparentInputFact::new(1, outpoint),
                    ],
                    Vec::new(),
                )],
            ),
        ];
        let duplicate_spend_replays = validated_replays(&duplicate_spends)?;
        let duplicate_spend_result = write_projection_ssts(
            test_config_with_settled_tip(
                temporary.path(),
                &options,
                TestByteLimits::default(),
                BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0xc2; 32])),
            ),
            canonical_scan(duplicate_spend_replays),
        );
        assert!(matches!(
            duplicate_spend_result,
            Err(RocksDbWalletError::Contract(
                WalletProjectionContractError::DuplicateSpend
            ))
        ));
        Ok(())
    }

    #[test]
    fn external_loader_rejects_missing_and_future_predecessors() -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = test_column_family_options();
        let missing = TransparentOutPoint::new(TransactionId::from_bytes([0xee; 32]), 0);
        let missing_blocks = vec![block_facts(
            1,
            [0; 32],
            [0xc1; 32],
            vec![transaction_facts(
                TransactionId::from_bytes([0xb1; 32]),
                vec![TransparentInputFact::new(0, missing)],
                Vec::new(),
            )],
        )];
        assert_missing_predecessor(
            temporary.path(),
            &options,
            validated_replays(&missing_blocks)?,
        );

        let later_transaction = TransactionId::from_bytes([0xb3; 32]);
        let future = TransparentOutPoint::new(later_transaction, 0);
        let future_blocks = vec![block_facts(
            1,
            [0; 32],
            [0xc1; 32],
            vec![
                transaction_facts(
                    TransactionId::from_bytes([0xb2; 32]),
                    vec![TransparentInputFact::new(0, future)],
                    Vec::new(),
                ),
                transaction_facts(
                    later_transaction,
                    Vec::new(),
                    vec![TransparentOutputFact::new(
                        0,
                        1,
                        [0x51],
                        TransparentAddressScriptHash::from_bytes([0xa1; 32]),
                    )],
                ),
            ],
        )];
        assert_missing_predecessor(
            temporary.path(),
            &options,
            validated_replays(&future_blocks)?,
        );
        Ok(())
    }

    #[test]
    fn external_loader_preserves_zero_only_and_mixed_zero_balances() -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = test_column_family_options();
        let address = TransparentAddressScriptHash::from_bytes([0xa1; 32]);
        let zero_blocks = vec![block_facts(
            1,
            [0; 32],
            [0xc1; 32],
            vec![transaction_facts(
                TransactionId::from_bytes([0xb1; 32]),
                Vec::new(),
                vec![TransparentOutputFact::new(0, 0, [0x51], address)],
            )],
        )];
        let zero = write_projection_ssts(
            test_config(temporary.path(), &options, TestByteLimits::default()),
            canonical_scan(validated_replays(&zero_blocks)?),
        )?;
        let zero_oracle = oracle_for(&zero_blocks)?;
        assert_eq!(zero.row_counts, zero_oracle.row_counts());
        assert_eq!(zero.utxo_summary, zero_oracle.utxo_summary());
        assert_eq!(zero.projection_digest, zero_oracle.projection_digest()?);
        assert_eq!(
            zero.projection_accumulator,
            zero_oracle.projection_accumulator()?
        );
        assert_eq!(zero.row_counts.transparent_unspent_output_count, 1);
        assert_eq!(zero.row_counts.transparent_address_balance_count, 0);

        let mixed_blocks = vec![block_facts(
            1,
            [0; 32],
            [0xd1; 32],
            vec![transaction_facts(
                TransactionId::from_bytes([0xb2; 32]),
                Vec::new(),
                vec![
                    TransparentOutputFact::new(0, 0, [0x51], address),
                    TransparentOutputFact::new(1, 7, [0x52], address),
                ],
            )],
        )];
        let mixed_tip = BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0xd1; 32]));
        let mixed = write_projection_ssts(
            test_config_with_bounds(
                temporary.path(),
                &options,
                TestByteLimits::default(),
                mixed_tip,
                mixed_tip,
            ),
            canonical_scan(validated_replays(&mixed_blocks)?),
        )?;
        let mixed_oracle = oracle_for(&mixed_blocks)?;
        assert_eq!(mixed.row_counts, mixed_oracle.row_counts());
        assert_eq!(mixed.utxo_summary, mixed_oracle.utxo_summary());
        assert_eq!(mixed.projection_digest, mixed_oracle.projection_digest()?);
        assert_eq!(
            mixed.projection_accumulator,
            mixed_oracle.projection_accumulator()?
        );
        assert_eq!(mixed.row_counts.transparent_unspent_output_count, 2);
        assert_eq!(mixed.row_counts.transparent_address_balance_count, 1);
        assert_eq!(mixed.utxo_summary.total_value_zat, 7);
        Ok(())
    }

    #[test]
    fn external_loader_merges_multiple_outpoint_runs() -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = test_column_family_options();
        let address = TransparentAddressScriptHash::from_bytes([0xa1; 32]);
        let blocks = vec![block_facts(
            1,
            [0; 32],
            [0xc1; 32],
            vec![transaction_facts(
                TransactionId::from_bytes([0xb1; 32]),
                Vec::new(),
                vec![
                    TransparentOutputFact::new(0, 1, [0x51], address),
                    TransparentOutputFact::new(1, 2, [0x52], address),
                    TransparentOutputFact::new(2, 3, [0x53], address),
                ],
            )],
        )];
        let prepared = write_projection_ssts(
            test_config(
                temporary.path(),
                &options,
                TestByteLimits {
                    outpoint_sort: 160,
                    ..TestByteLimits::default()
                },
            ),
            canonical_scan(validated_replays(&blocks)?),
        )?;

        assert!(prepared.outpoint_sort_evidence.initial_run_count > 1);
        // At most 64 runs merge inside the reader without a written pass.
        assert_eq!(prepared.outpoint_sort_evidence.merge_pass_count, 0);
        assert_eq!(prepared.row_counts.transparent_unspent_output_count, 3);
        assert_eq!(prepared.utxo_summary.total_value_zat, 6);
        Ok(())
    }

    /// Three blocks whose outputs go to two addresses, with spends of
    /// outputs from earlier blocks.
    fn three_block_spend_chain() -> Vec<CanonicalBlockFacts> {
        let first = TransparentAddressScriptHash::from_bytes([0xa1; 32]);
        let second = TransparentAddressScriptHash::from_bytes([0xa2; 32]);
        let funding = TransactionId::from_bytes([0xb1; 32]);
        let change = TransactionId::from_bytes([0xb2; 32]);
        vec![
            block_facts(
                1,
                [0; 32],
                [0xc1; 32],
                vec![transaction_facts(
                    funding,
                    Vec::new(),
                    vec![
                        TransparentOutputFact::new(0, 10, [0x51], first),
                        TransparentOutputFact::new(1, 20, [0x52, 0x53], second),
                        TransparentOutputFact::new(2, 30, [0x54], first),
                    ],
                )],
            ),
            block_facts(
                2,
                [0xc1; 32],
                [0xc2; 32],
                vec![transaction_facts(
                    change,
                    vec![TransparentInputFact::new(
                        0,
                        TransparentOutPoint::new(funding, 0),
                    )],
                    vec![
                        TransparentOutputFact::new(0, 4, [0x55], second),
                        TransparentOutputFact::new(1, 6, [0x56], first),
                    ],
                )],
            ),
            block_facts(
                3,
                [0xc2; 32],
                [0xc3; 32],
                vec![transaction_facts(
                    TransactionId::from_bytes([0xb3; 32]),
                    vec![
                        TransparentInputFact::new(0, TransparentOutPoint::new(funding, 1)),
                        TransparentInputFact::new(1, TransparentOutPoint::new(change, 0)),
                    ],
                    vec![TransparentOutputFact::new(0, 24, [0x57], first)],
                )],
            ),
        ]
    }

    #[test]
    fn every_hashing_worker_count_publishes_the_serial_oracle_evidence()
    -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = test_column_family_options();
        let blocks = three_block_spend_chain();
        let oracle = oracle_for(&blocks)?;
        let tip = BlockId::new(BlockHeight::new(3), BlockHash::from_bytes([0xc3; 32]));
        for workers in [1, 2, 4, 8] {
            let config = WalletProjectionLoadConfig {
                accumulator_hashing_workers: NonZeroU32::new(workers).ok_or("nonzero workers")?,
                ..test_config_with_settled_tip(
                    temporary.path(),
                    &options,
                    TestByteLimits::default(),
                    tip,
                )
            };
            let prepared =
                write_projection_ssts(config, canonical_scan(validated_replays(&blocks)?))?;
            assert_eq!(
                prepared.row_counts,
                oracle.row_counts(),
                "{workers} workers"
            );
            assert_eq!(
                prepared.utxo_summary,
                oracle.utxo_summary(),
                "{workers} workers"
            );
            assert_eq!(
                prepared.projection_digest,
                oracle.projection_digest()?,
                "{workers} workers"
            );
            assert_eq!(
                prepared.projection_accumulator,
                oracle.projection_accumulator()?,
                "{workers} workers"
            );
            assert_eq!(prepared.row_counts.transparent_spent_output_count, 3);
            assert_eq!(prepared.utxo_summary.total_value_zat, 60);
        }
        Ok(())
    }

    #[test]
    fn external_loader_refuses_memory_and_temporary_file_limit_crossings()
    -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = test_column_family_options();
        let blocks = vec![block_facts(
            1,
            [0; 32],
            [0xc1; 32],
            vec![transaction_facts(
                TransactionId::from_bytes([0xb1; 32]),
                Vec::new(),
                vec![TransparentOutputFact::new(
                    0,
                    1,
                    [0x51],
                    TransparentAddressScriptHash::from_bytes([0xa1; 32]),
                )],
            )],
        )];
        let memory_result = write_projection_ssts(
            test_config(
                temporary.path(),
                &options,
                TestByteLimits {
                    outpoint_sort: 1,
                    ..TestByteLimits::default()
                },
            ),
            canonical_scan(validated_replays(&blocks)?),
        );
        assert!(matches!(
            memory_result,
            Err(RocksDbWalletError::BulkLoad(
                BulkLoadError::AccountedMemoryLimit {
                    limit_bytes: 1,
                    required_bytes: _
                }
            ))
        ));

        let disk_result = write_projection_ssts(
            test_config(
                temporary.path(),
                &options,
                TestByteLimits {
                    temporary_files_per_sorter: 1,
                    ..TestByteLimits::default()
                },
            ),
            canonical_scan(validated_replays(&blocks)?),
        );
        assert!(matches!(
            disk_result,
            Err(RocksDbWalletError::BulkLoad(
                BulkLoadError::TemporaryFileLimit {
                    limit_bytes: 1,
                    required_bytes: _
                }
            ))
        ));
        Ok(())
    }

    fn test_column_family_options() -> WalletColumnFamilyOptions {
        WalletColumnFamilyOptions::new(
            &Cache::new_lru_cache(TEST_BLOCK_CACHE_BYTES),
            RocksDbResourceBudget::for_local_tests(),
        )
    }

    fn test_config<'options>(
        staging_path: &'options Path,
        options: &'options WalletColumnFamilyOptions,
        limits: TestByteLimits,
    ) -> WalletProjectionLoadConfig<'options> {
        test_config_with_settled_tip(
            staging_path,
            options,
            limits,
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0xc1; 32])),
        )
    }

    fn test_config_with_settled_tip<'options>(
        staging_path: &'options Path,
        options: &'options WalletColumnFamilyOptions,
        limits: TestByteLimits,
        settled_tip: BlockId,
    ) -> WalletProjectionLoadConfig<'options> {
        test_config_with_bounds(
            staging_path,
            options,
            limits,
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0xc1; 32])),
            settled_tip,
        )
    }

    fn test_config_with_bounds<'options>(
        staging_path: &'options Path,
        options: &'options WalletColumnFamilyOptions,
        limits: TestByteLimits,
        first_retained_block: BlockId,
        settled_tip: BlockId,
    ) -> WalletProjectionLoadConfig<'options> {
        WalletProjectionLoadConfig {
            staging_path,
            options,
            network: Network::ZcashRegtest,
            first_retained_block,
            settled_tip,
            supported_reorg_depth: 0,
            max_outpoint_sort_memory_bytes: limits.outpoint_sort,
            max_secondary_sort_memory_bytes_per_sorter: limits.secondary_sort_per_sorter,
            max_temporary_file_bytes_per_sorter: limits.temporary_files_per_sorter,
            sst_target_logical_bytes: TEST_SST_LOGICAL_BYTES,
            max_accounted_reorg_undo_bytes: TEST_REORG_UNDO_BYTES,
            accumulator_hashing_workers: NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN),
            scan_chunk_blocks: DEFAULT_CANONICAL_SCAN_CHUNK_BLOCKS,
        }
    }

    fn canonical_scan(
        replays: Vec<ValidatedCanonicalBlockReplay>,
    ) -> impl Iterator<Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>> {
        replays.into_iter().map(Ok)
    }

    fn validated_replays(
        blocks: &[CanonicalBlockFacts],
    ) -> Result<Vec<ValidatedCanonicalBlockReplay>, Box<dyn Error>> {
        blocks
            .iter()
            .map(|facts| {
                let envelope = encode_canonical_block_replay(
                    facts,
                    CanonicalBlockReplayFormatVersion::V1,
                    CanonicalBlockFactsDigestVersion::V1,
                );
                decode_canonical_block_replay(envelope.as_bytes())
                    .map_err(|error| Box::new(error) as Box<dyn Error>)
            })
            .collect()
    }

    fn oracle_for(
        blocks: &[CanonicalBlockFacts],
    ) -> Result<WalletProjectionSerialOracle, WalletProjectionContractError> {
        let mut oracle = WalletProjectionSerialOracle::new(Network::ZcashRegtest);
        for block in blocks {
            oracle.apply_block(block)?;
        }
        Ok(oracle)
    }

    fn assert_missing_predecessor(
        staging_path: &Path,
        options: &WalletColumnFamilyOptions,
        replays: Vec<ValidatedCanonicalBlockReplay>,
    ) {
        assert!(matches!(
            write_projection_ssts(
                test_config(staging_path, options, TestByteLimits::default()),
                canonical_scan(replays),
            ),
            Err(RocksDbWalletError::Contract(
                WalletProjectionContractError::MissingTransparentPredecessor
            ))
        ));
    }

    fn block_facts(
        height: u32,
        parent_hash: [u8; 32],
        block_hash: [u8; 32],
        transactions: Vec<CanonicalTransactionFacts>,
    ) -> CanonicalBlockFacts {
        CanonicalBlockFacts {
            block_header: BlockHeaderArtifact::new(
                BlockHeight::new(height),
                BlockHash::from_bytes(block_hash),
                BlockHash::from_bytes(parent_hash),
                [0; 32],
                [0; 32],
                i64::from(height),
                0,
                [0; 32],
                0,
                0,
            ),
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&block_hash),
            transactions,
        }
    }

    fn transaction_facts(
        transaction_id: TransactionId,
        transparent_inputs: Vec<TransparentInputFact>,
        transparent_outputs: Vec<TransparentOutputFact>,
    ) -> CanonicalTransactionFacts {
        let counts = TransactionComponentCounts {
            transparent_input_count: u32::try_from(transparent_inputs.len()).unwrap_or(u32::MAX),
            transparent_output_count: u32::try_from(transparent_outputs.len()).unwrap_or(u32::MAX),
            ..TransactionComponentCounts::EMPTY
        };
        CanonicalTransactionFacts {
            public_facts: TransactionPublicFacts {
                transaction_id,
                auth_digest: None,
                wtxid: None,
                version: TransactionVersion::V4,
                consensus_branch_id: None,
                lock_time: LockTime::Unlocked,
                expiry_height: None,
                size_bytes: 32,
                counts,
                orchard_value_balance_zat: None,
                orchard_anchor: None,
                ironwood_value_balance_zat: None,
                privacy_shape: PrivacyShape::Unclassified,
                is_coinbase: false,
                unsupported_sections: Vec::new(),
            },
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
                &transaction_id.as_bytes(),
            ),
            intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
            transparent_inputs,
            transparent_outputs,
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "test fixture constructs a complete certified canonical store in one helper"
    )]
    fn build_ready_test_canonical_store(
        path: &Path,
        blocks: &[CanonicalBlockFacts],
        settled_index: usize,
        reorg_window: u32,
    ) -> Result<zinder_store::RocksDbCanonicalStore, Box<dyn Error>> {
        use zinder_core::{
            ChainTipMetadata, CommitmentTreeCheckpoint, CommitmentTreeFrontiers,
            CompactBlockArtifact, CompactChainMetadata, ConsensusBranchId,
            NetworkUpgradeActivation, NetworkUpgradeActivations, TransactionBlobArtifact,
            TransactionLocation, UnixTimestampMillis,
        };
        use zinder_store::{
            CanonicalBaselinePublication, CanonicalBuildBlock, CanonicalReorgPolicy,
            CanonicalStoreBuildPlan, CanonicalStoreWorkload, RawBlobRetention,
            RocksDbCanonicalBuilder,
        };
        let activations_vec = [
            "Overwinter",
            "Sapling",
            "Blossom",
            "Heartwood",
            "Canopy",
            "NU5",
            "NU6",
            "NU6.1",
            "NU6.2",
            "NU6.3",
        ]
        .into_iter()
        .enumerate()
        .map(|(index, name)| NetworkUpgradeActivation {
            branch_id: ConsensusBranchId::new(u32::try_from(index).unwrap_or(u32::MAX) + 1),
            activation_height: BlockHeight::new(100 + u32::try_from(index).unwrap_or(u32::MAX)),
            name: name.to_owned(),
        })
        .collect();
        let activations = NetworkUpgradeActivations::new(Network::ZcashRegtest, activations_vec)?;
        let tip_facts = blocks.last().ok_or("test blocks must not be empty")?;
        let tip = BlockId::new(
            tip_facts.block_header.height,
            tip_facts.block_header.block_hash,
        );
        let settled_facts = blocks
            .get(settled_index)
            .ok_or("settled index out of range")?;
        let settled_tip = BlockId::new(
            settled_facts.block_header.height,
            settled_facts.block_header.block_hash,
        );
        let plan = CanonicalStoreBuildPlan::complete(
            &activations,
            0,
            tip,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(reorg_window)?,
        )?;
        let mut builder = RocksDbCanonicalBuilder::create_fresh(
            path,
            CanonicalStoreWorkload::Wallet,
            plan,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let total = blocks.len();
        let build_blocks = blocks.iter().enumerate().map(|(index, facts)| {
            let height = facts.block_header.height;
            let block_hash = facts.block_header.block_hash;
            let parent_hash = facts.block_header.parent_hash;
            let is_tip = index + 1 == total;
            let compact_block = CompactBlockArtifact::empty(
                BlockId::new(height, block_hash),
                parent_hash,
                u32::try_from(facts.block_header.block_time).unwrap_or(u32::MAX),
                CompactChainMetadata {
                    sapling_commitment_tree_size: 0,
                    orchard_commitment_tree_size: 0,
                    ironwood_commitment_tree_size: 0,
                },
            );
            let transaction_blobs = facts
                .transactions
                .iter()
                .enumerate()
                .map(|(tx_idx, transaction)| {
                    TransactionBlobArtifact::new(
                        TransactionLocation::new(
                            transaction.public_facts.transaction_id,
                            height,
                            block_hash,
                            u32::try_from(tx_idx).unwrap_or(u32::MAX),
                        ),
                        transaction.public_facts.transaction_id.as_bytes(),
                    )
                })
                .collect();
            let tree_state_checkpoint = is_tip.then(|| {
                CommitmentTreeCheckpoint::new(
                    BlockId::new(height, block_hash),
                    u32::try_from(facts.block_header.block_time).unwrap_or(u32::MAX),
                    CommitmentTreeFrontiers::default(),
                )
            });
            let replay_envelope = encode_canonical_block_replay(
                facts,
                CanonicalBlockReplayFormatVersion::V1,
                CanonicalBlockFactsDigestVersion::V1,
            );
            Ok::<_, std::io::Error>(CanonicalBuildBlock {
                facts: facts.clone(),
                replay_envelope,
                compact_block,
                tip_metadata: ChainTipMetadata::new(0, 0, 0),
                tree_state_checkpoint,
                block_final_note_commitment_roots: None,
                transaction_blobs,
                block_blob: None,
            })
        });
        builder.bulk_load_blocks(&activations, build_blocks)?;
        builder.load_subtree_roots(std::iter::empty())?;
        let tip_checkpoint = CommitmentTreeCheckpoint::new(
            tip,
            u32::try_from(tip_facts.block_header.block_time).unwrap_or(u32::MAX),
            CommitmentTreeFrontiers::default(),
        );
        builder.confirm_source_tip_checkpoint(&tip_checkpoint)?;
        let validated = builder.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
            settled_tip,
            UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        Ok(validated.publish_baseline(publication)?)
    }

    fn multi_chunk_canonical_blocks() -> Vec<CanonicalBlockFacts> {
        let mut blocks = Vec::new();
        let mut parent_hash = Network::ZcashRegtest.genesis_hash().as_bytes();
        let mut created_outpoints = Vec::new();
        for height in 1_u32..=15 {
            let mut block_hash = [0_u8; 32];
            block_hash[0] = u8::try_from(height).unwrap_or(0);
            block_hash[31] = 0xAA;
            let mut tx_id_bytes = [0_u8; 32];
            tx_id_bytes[0] = u8::try_from(height).unwrap_or(0);
            tx_id_bytes[1] = 0x10;
            let tx_id = TransactionId::from_bytes(tx_id_bytes);
            let mut address_bytes = [0_u8; 32];
            address_bytes[0] = u8::try_from(height % 4).unwrap_or(0);
            let address = TransparentAddressScriptHash::from_bytes(address_bytes);
            let mut coinbase = transaction_facts(
                tx_id,
                vec![TransparentInputFact::new(
                    0,
                    TransparentOutPoint::COINBASE_SENTINEL,
                )],
                vec![TransparentOutputFact::new(
                    0,
                    1_000 * u64::from(height),
                    vec![0x76, 0xa9],
                    address,
                )],
            );
            coinbase.public_facts.is_coinbase = true;
            created_outpoints.push(TransparentOutPoint::new(tx_id, 0));
            let mut transactions = vec![coinbase];
            // Spend an output from 3 blocks earlier (exercises cross-chunk and
            // settled-to-unsettled spends) on every height >= 4.
            if height >= 4 {
                let mut spend_tx_bytes = [0_u8; 32];
                spend_tx_bytes[0] = u8::try_from(height).unwrap_or(0);
                spend_tx_bytes[1] = 0x20;
                let spend_tx_id = TransactionId::from_bytes(spend_tx_bytes);
                let spent = created_outpoints[usize::try_from(height - 4).unwrap_or(0)];
                let spend_tx = transaction_facts(
                    spend_tx_id,
                    vec![TransparentInputFact::new(0, spent)],
                    vec![TransparentOutputFact::new(
                        0,
                        500 * u64::from(height),
                        vec![0x51],
                        TransparentAddressScriptHash::from_bytes([0xbb; 32]),
                    )],
                );
                transactions.push(spend_tx);
            }
            blocks.push(block_facts(height, parent_hash, block_hash, transactions));
            parent_hash = block_hash;
        }
        blocks
    }

    #[test]
    fn partition_settled_height_chunks_covers_every_height_once() -> Result<(), Box<dyn Error>> {
        let empty = partition_settled_height_chunks(
            BlockHeight::new(5),
            BlockHeight::new(4),
            NonZeroU32::new(4).ok_or("zero chunk")?,
        );
        assert_eq!(empty, Vec::new());

        for span in [1_u32, 2, 3, 4, 7, 16] {
            let chunks = partition_settled_height_chunks(
                BlockHeight::new(1),
                BlockHeight::new(15),
                NonZeroU32::new(span).ok_or("zero chunk")?,
            );
            assert_eq!(
                chunks.first().map(|chunk| chunk.start),
                Some(BlockHeight::new(1))
            );
            assert_eq!(
                chunks.last().map(|chunk| chunk.end),
                Some(BlockHeight::new(15))
            );
            for pair in chunks.windows(2) {
                assert_eq!(pair[0].end.next(), Some(pair[1].start));
            }
            for chunk in &chunks {
                let len = u64::from(chunk.end.value() - chunk.start.value()) + 1;
                assert!(len >= 1 && len <= u64::from(span));
            }
        }

        // u32::MAX upper bound must not overflow.
        let top_chunks = partition_settled_height_chunks(
            BlockHeight::new(u32::MAX - 2),
            BlockHeight::new(u32::MAX),
            NonZeroU32::new(2).ok_or("zero chunk")?,
        );
        assert_eq!(
            top_chunks,
            vec![
                BlockHeightRange::inclusive(
                    BlockHeight::new(u32::MAX - 2),
                    BlockHeight::new(u32::MAX - 1),
                ),
                BlockHeightRange::inclusive(BlockHeight::new(u32::MAX), BlockHeight::new(u32::MAX),),
            ]
        );
        Ok(())
    }

    #[test]
    fn parallel_scan_worker_count_respects_chunks_workers_and_memory_floor()
    -> Result<(), Box<dyn Error>> {
        let four = NonZeroU32::new(4).ok_or("zero")?;
        let eight = NonZeroU32::new(8).ok_or("zero")?;
        assert_eq!(
            parallel_scan_worker_count(four, 10, 4 * MIN_SHARD_OUTPOINT_SORT_MEMORY_BYTES).get(),
            4
        );
        assert_eq!(
            parallel_scan_worker_count(eight, 3, 16 * MIN_SHARD_OUTPOINT_SORT_MEMORY_BYTES).get(),
            3
        );
        assert_eq!(
            parallel_scan_worker_count(eight, 10, 2 * MIN_SHARD_OUTPOINT_SORT_MEMORY_BYTES).get(),
            2
        );
        assert_eq!(
            parallel_scan_worker_count(eight, 10, MIN_SHARD_OUTPOINT_SORT_MEMORY_BYTES - 1).get(),
            1
        );
        assert_eq!(
            parallel_scan_worker_count(eight, 0, 16 * MIN_SHARD_OUTPOINT_SORT_MEMORY_BYTES).get(),
            1
        );
        Ok(())
    }

    #[test]
    fn parallel_canonical_scan_matches_serial_with_settled_and_unsettled_suffix()
    -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let blocks = multi_chunk_canonical_blocks();
        let canonical =
            build_ready_test_canonical_store(&temporary.path().join("canonical"), &blocks, 11, 5)?;
        let ready = canonical.ready_evidence();
        let options = test_column_family_options();
        let serial_staging = TempDir::new()?;
        let mut serial_config = test_config_with_bounds(
            serial_staging.path(),
            &options,
            TestByteLimits::default(),
            ready.first_retained_block,
            ready.sequence_checkpoint.through(),
        );
        serial_config.supported_reorg_depth = 5;
        serial_config.accumulator_hashing_workers = NonZeroU32::MIN;
        let serial = write_projection_ssts_from_source(serial_config, &canonical)?;

        for workers in [2_u32, 4, 8] {
            for chunk_blocks in [2_u32, 3, 5] {
                let staging = TempDir::new()?;
                let mut config = serial_config;
                config.staging_path = staging.path();
                config.accumulator_hashing_workers = NonZeroU32::new(workers).ok_or("zero")?;
                config.scan_chunk_blocks = NonZeroU32::new(chunk_blocks).ok_or("zero")?;
                let parallel = write_projection_ssts_from_source(config, &canonical)?;
                assert_eq!(parallel.first_block, serial.first_block);
                assert_eq!(parallel.tip, serial.tip);
                assert_eq!(
                    parallel.source_sequence_digest,
                    serial.source_sequence_digest
                );
                assert_eq!(parallel.projection_digest, serial.projection_digest);
                assert_eq!(
                    parallel.projection_accumulator,
                    serial.projection_accumulator
                );
                assert_eq!(parallel.row_counts, serial.row_counts);
                assert_eq!(parallel.utxo_summary, serial.utxo_summary);
                assert_eq!(parallel.counters, serial.counters);
                assert_eq!(parallel.logical_row_bytes, serial.logical_row_bytes);
                assert_eq!(
                    parallel.row_counts.reorg_undo_count, 3,
                    "workers={workers} chunk={chunk_blocks}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn parallel_canonical_scan_rejects_refused_worker_thread_with_typed_error()
    -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let blocks = multi_chunk_canonical_blocks();
        let canonical =
            build_ready_test_canonical_store(&temporary.path().join("canonical"), &blocks, 11, 5)?;
        let ready = canonical.ready_evidence();
        let options = test_column_family_options();
        for refused_index in [0_usize, 1, 3] {
            let staging = TempDir::new()?;
            let mut config = test_config_with_bounds(
                staging.path(),
                &options,
                TestByteLimits::default(),
                ready.first_retained_block,
                ready.sequence_checkpoint.through(),
            );
            config.supported_reorg_depth = 5;
            config.accumulator_hashing_workers = NonZeroU32::new(4).ok_or("zero")?;
            config.scan_chunk_blocks = NonZeroU32::new(2).ok_or("zero")?;
            REFUSED_CANONICAL_SCAN_THREAD.set(Some(refused_index));
            let outcome = write_projection_ssts_from_source(config, &canonical);
            REFUSED_CANONICAL_SCAN_THREAD.set(None);
            assert!(
                matches!(
                    outcome,
                    Err(RocksDbWalletError::CanonicalScanThreadUnavailable { .. })
                ),
                "refused index {refused_index}"
            );
        }
        Ok(())
    }

    #[test]
    fn parallel_canonical_scan_stops_window_when_worker_unwinds_past_window_capacity()
    -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let blocks = multi_chunk_canonical_blocks();
        // 12 settled blocks with chunk_blocks=1 gives 12 chunks, exceeding the
        // 2-worker claim window (max_in_flight = 8) so siblings would deadlock
        // if an unwinding worker on chunk 0 did not stop and wake the window.
        let canonical =
            build_ready_test_canonical_store(&temporary.path().join("canonical"), &blocks, 12, 5)?;
        let ready = canonical.ready_evidence();
        let options = test_column_family_options();
        let staging = TempDir::new()?;
        let mut config = test_config_with_bounds(
            staging.path(),
            &options,
            TestByteLimits::default(),
            ready.first_retained_block,
            ready.sequence_checkpoint.through(),
        );
        config.supported_reorg_depth = 5;
        config.accumulator_hashing_workers = NonZeroU32::new(2).ok_or("zero")?;
        config.scan_chunk_blocks = NonZeroU32::MIN;
        let _panic_guard = arm_next_window_panic_chunk(0);
        let outcome = write_projection_ssts_from_source(config, &canonical);
        assert!(
            matches!(outcome, Err(RocksDbWalletError::CanonicalScanWorkerStopped)),
            "expected CanonicalScanWorkerStopped after worker unwind, got {:?}",
            outcome.err()
        );
        Ok(())
    }

    #[test]
    fn parallel_canonical_scan_admits_script_larger_than_single_shard_budget()
    -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let mut blocks = multi_chunk_canonical_blocks();
        // Give one settled output a 100 KiB script: exceeds one 64 KiB shard
        // (256 KiB / 4 workers) while fitting well inside the 256 KiB global
        // outpoint-sort memory limit that the serial path accepts.
        blocks[2].transactions[0].transparent_outputs[0].script_pub_key = vec![0x6a_u8; 100 * 1024];
        let canonical =
            build_ready_test_canonical_store(&temporary.path().join("canonical"), &blocks, 12, 5)?;
        let ready = canonical.ready_evidence();
        let options = test_column_family_options();
        let limits = TestByteLimits {
            outpoint_sort: 256 * 1024,
            ..TestByteLimits::default()
        };
        let serial_staging = TempDir::new()?;
        let mut serial_config = test_config_with_bounds(
            serial_staging.path(),
            &options,
            limits,
            ready.first_retained_block,
            ready.sequence_checkpoint.through(),
        );
        serial_config.supported_reorg_depth = 5;
        serial_config.accumulator_hashing_workers = NonZeroU32::MIN;
        let serial = write_projection_ssts_from_source(serial_config, &canonical)?;

        let parallel_staging = TempDir::new()?;
        let mut parallel_config = serial_config;
        parallel_config.staging_path = parallel_staging.path();
        parallel_config.accumulator_hashing_workers = NonZeroU32::new(4).ok_or("zero")?;
        parallel_config.scan_chunk_blocks = NonZeroU32::new(3).ok_or("zero")?;
        let parallel = write_projection_ssts_from_source(parallel_config, &canonical)?;
        assert_eq!(parallel.projection_digest, serial.projection_digest);
        assert_eq!(
            parallel.projection_accumulator,
            serial.projection_accumulator
        );
        assert_eq!(
            parallel.source_sequence_digest,
            serial.source_sequence_digest
        );
        assert_eq!(parallel.row_counts, serial.row_counts);
        assert_eq!(parallel.utxo_summary, serial.utxo_summary);
        assert!(
            parallel.outpoint_sort_evidence.peak_accounted_memory_bytes <= limits.outpoint_sort
        );
        Ok(())
    }
}
