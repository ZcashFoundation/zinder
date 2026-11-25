//! Accumulator hashing for fresh wallet construction.
//!
//! The projection row accumulator and the UTXO set commitment are both sums of
//! per-row `LtHash16` expansions, so rows hashed into separate partial
//! accumulators on different threads combine into the same bytes as one serial
//! pass. With one worker, every row is hashed on the calling thread. With more,
//! the calling thread copies rows into batches of fixed capacity, and each
//! batch is hashed into its own partial accumulators on a worker.
//!
//! Memory held for hashing is fixed by the worker count. A batch is allocated
//! once with room for [`BATCH_PAYLOAD_BYTES`] of keys, values, and scripts,
//! [`MAX_BATCH_ROWS`] row descriptors, and [`MAX_BATCH_UNSPENT_OUTPUTS`] output
//! descriptors, and is handed to a worker before an entry would overflow it. A
//! row or script larger than a whole batch is hashed on the calling thread
//! instead of being copied. At most twice the worker count of batches, never
//! more than [`MAX_PENDING_BATCHES`], wait for or run on the workers, or sit
//! hashed in their reply channels, and the calling thread holds one batch it
//! is filling and one emptied batch for reuse.

use std::{
    collections::VecDeque,
    num::NonZeroU32,
    sync::{
        Arc,
        mpsc::{Receiver, SyncSender, sync_channel},
    },
};

use zinder_core::{
    BlockHeight, Network, TransparentOutPoint, TransparentUtxoSetCommitment,
    wire::UtxoSetCommitmentElement,
};
use zinder_wallet_projection::{
    WalletProjectionDigestBuilder, WalletProjectionRowFamily, WalletUnspentOutput,
};

use crate::RocksDbWalletError;

/// Key, value, and script bytes one batch holds.
const BATCH_PAYLOAD_BYTES: usize = 256 * 1024;

/// Rows one batch describes; enough for the smallest rows, 40-byte address
/// balances, to fill most of the payload.
const MAX_BATCH_ROWS: usize = 4096;

/// Unspent outputs one batch describes. Each comes with an unspent-output row
/// of at least 120 bytes, so a full payload holds about 2,000 of them.
const MAX_BATCH_UNSPENT_OUTPUTS: usize = 2048;

/// Most batches that can wait for or run on the workers, whatever the worker
/// count.
const MAX_PENDING_BATCHES: usize = 64;

/// The wallet build's two order-independent accumulators.
pub(crate) struct ConstructionAccumulators {
    network: Network,
    digest: WalletProjectionDigestBuilder,
    commitment: TransparentUtxoSetCommitment,
    workers: Option<HashingWorkers>,
}

impl ConstructionAccumulators {
    /// Starts empty accumulators that hash on `workers` threads, or on the
    /// calling thread when `workers` is one.
    pub(crate) fn start(network: Network, workers: NonZeroU32) -> Result<Self, RocksDbWalletError> {
        Self::start_with_payload_capacity(network, workers, BATCH_PAYLOAD_BYTES)
    }

    fn start_with_payload_capacity(
        network: Network,
        workers: NonZeroU32,
        payload_capacity: usize,
    ) -> Result<Self, RocksDbWalletError> {
        Ok(Self {
            network,
            digest: WalletProjectionDigestBuilder::new(),
            commitment: TransparentUtxoSetCommitment::empty(),
            workers: HashingWorkers::start(workers, network, payload_capacity)?,
        })
    }

    /// Adds one durable row to the projection accumulator.
    pub(crate) fn append_row(
        &mut self,
        family: WalletProjectionRowFamily,
        key: &[u8],
        encoded_value: &[u8],
    ) -> Result<(), RocksDbWalletError> {
        let Some(workers) = self.workers.as_mut() else {
            self.digest.append_row(family, key, encoded_value)?;
            return Ok(());
        };
        let payload_bytes = key.len().saturating_add(encoded_value.len());
        if workers.make_room(
            payload_bytes,
            BatchEntry::Row,
            &mut self.digest,
            &mut self.commitment,
        )? {
            workers.filling.push_row(family, key, encoded_value);
        } else {
            self.digest.append_row(family, key, encoded_value)?;
        }
        Ok(())
    }

    /// Adds one unspent output to the UTXO set commitment.
    pub(crate) fn insert_unspent_output(
        &mut self,
        output: &WalletUnspentOutput,
    ) -> Result<(), RocksDbWalletError> {
        let fits_a_batch = match self.workers.as_mut() {
            Some(workers) => workers.make_room(
                output.script_pub_key.len(),
                BatchEntry::UnspentOutput,
                &mut self.digest,
                &mut self.commitment,
            )?,
            None => false,
        };
        if fits_a_batch && let Some(workers) = self.workers.as_mut() {
            workers.filling.push_unspent_output(output);
        } else {
            self.commitment
                .insert(&utxo_element(self.network, output, &output.script_pub_key));
        }
        Ok(())
    }

    /// Waits until every row and output added so far is hashed and combined.
    ///
    /// The build settles at the end of each drain, so a drain's stage time
    /// includes its own hashing.
    pub(crate) fn settle(&mut self) -> Result<(), RocksDbWalletError> {
        if let Some(workers) = self.workers.as_mut() {
            workers.submit(&mut self.digest, &mut self.commitment)?;
            workers.drain(&mut self.digest, &mut self.commitment)?;
        }
        Ok(())
    }

    /// Settles in-flight batches, releases primary's working batches, and
    /// extracts the shared worker pool so secondary branches reuse the same
    /// threads without exceeding the build-wide worker or batch ceiling.
    pub(crate) fn take_branch_hashing_pool(
        &mut self,
    ) -> Result<Option<BranchHashingPool>, RocksDbWalletError> {
        self.settle()?;
        Ok(self.workers.take().map(HashingWorkers::into_branch_pool))
    }

    /// Creates a branch accumulator sharing an existing worker pool and batch
    /// budget, or an inline accumulator when `branch_config` is `None`.
    pub(crate) fn start_optional_branch(
        network: Network,
        branch_config: Option<BranchHashingConfig>,
    ) -> Self {
        Self {
            network,
            digest: WalletProjectionDigestBuilder::new(),
            commitment: TransparentUtxoSetCommitment::empty(),
            workers: branch_config
                .map(|config| HashingWorkers::from_branch_config(network, config)),
        }
    }

    /// Re-attaches a fresh [`HashingWorkers`] backed by `pool` after concurrent
    /// branches have finished and dropped their own batches.
    pub(crate) fn restore_branch_hashing_pool(&mut self, pool: BranchHashingPool) {
        let max_pending = max_pending_for_workers(pool.worker_count);
        self.workers = Some(HashingWorkers::from_shared_pool(
            pool.pool,
            pool.worker_count,
            network_for_restore(self.network),
            pool.payload_capacity,
            max_pending,
        ));
    }

    /// Settles every batch and returns the combined accumulators.
    pub(crate) fn finish(
        mut self,
    ) -> Result<(WalletProjectionDigestBuilder, TransparentUtxoSetCommitment), RocksDbWalletError>
    {
        self.settle()?;
        Ok((self.digest, self.commitment))
    }

    /// Settles every batch and returns opaque finished branch accumulators.
    pub(crate) fn finish_branch(self) -> Result<FinishedBranchAccumulators, RocksDbWalletError> {
        let (digest, commitment) = self.finish()?;
        Ok(FinishedBranchAccumulators { digest, commitment })
    }

    /// Combines finished branch accumulators into this instance after settling.
    pub(crate) fn combine_finished(
        &mut self,
        branch_digest: &WalletProjectionDigestBuilder,
        branch_commitment: &TransparentUtxoSetCommitment,
    ) -> Result<(), RocksDbWalletError> {
        self.settle()?;
        self.digest.combine(branch_digest)?;
        self.commitment.combine(branch_commitment)?;
        Ok(())
    }

    /// Combines finished branch accumulators returned by [`Self::finish_branch`].
    pub(crate) fn combine_branch(
        &mut self,
        branch: &FinishedBranchAccumulators,
    ) -> Result<(), RocksDbWalletError> {
        self.combine_finished(&branch.digest, &branch.commitment)
    }
}

const fn network_for_restore(network: Network) -> Network {
    network
}

const fn max_pending_for_workers(workers: NonZeroU32) -> usize {
    let threads = workers.get() as usize;
    let doubled = threads.saturating_mul(2);
    if doubled <= MAX_PENDING_BATCHES {
        doubled
    } else {
        MAX_PENDING_BATCHES
    }
}

/// Shared worker pool retired from primary hashing for concurrent secondary
/// derivation branches and later restored for final `reorg_undo` hashing.
pub(crate) struct BranchHashingPool {
    pool: Arc<rayon::ThreadPool>,
    worker_count: NonZeroU32,
    payload_capacity: usize,
}

impl BranchHashingPool {
    const BATCHES_HELD_PER_SLICE: usize = 2;
    const MIN_BATCHES_PER_SLICE: usize = Self::BATCHES_HELD_PER_SLICE + 1;

    const fn global_batch_budget(worker_count: NonZeroU32) -> usize {
        max_pending_for_workers(worker_count).saturating_add(Self::BATCHES_HELD_PER_SLICE)
    }

    fn config_with_total_batches(&self, allocated_total: usize) -> BranchHashingConfig {
        BranchHashingConfig {
            pool: Arc::clone(&self.pool),
            worker_count: self.worker_count,
            payload_capacity: self.payload_capacity,
            max_pending: allocated_total
                .saturating_sub(Self::BATCHES_HELD_PER_SLICE)
                .max(1),
        }
    }

    /// Splits the build-wide batch budget across two concurrent branches while
    /// keeping total `pending + filling + spare` batches within
    /// `min(2 * workers, MAX_PENDING_BATCHES) + 2`.
    #[cfg(test)]
    pub(crate) fn split_two(&self) -> (BranchHashingConfig, BranchHashingConfig) {
        let total_batches = Self::global_batch_budget(self.worker_count);
        let first_total = total_batches / 2;
        let second_total = total_batches.saturating_sub(first_total);
        (
            self.config_with_total_batches(first_total),
            self.config_with_total_batches(second_total),
        )
    }

    /// Splits the build-wide batch budget across up to `slice_count` concurrent
    /// branches or shards while keeping total `pending + filling + spare`
    /// batches within `min(2 * workers, MAX_PENDING_BATCHES) + 2`. Overflow
    /// slices fall back to `None` (inline hashing on their own thread).
    pub(crate) fn split_many(&self, slice_count: usize) -> Vec<Option<BranchHashingConfig>> {
        if slice_count == 0 {
            return Vec::new();
        }
        let total_batches = Self::global_batch_budget(self.worker_count);
        let active_slices = (total_batches / Self::MIN_BATCHES_PER_SLICE).min(slice_count);
        if active_slices == 0 {
            return (0..slice_count).map(|_| None).collect();
        }
        let base = total_batches / active_slices;
        let remainder = total_batches % active_slices;
        let mut configs = Vec::with_capacity(slice_count);
        for slice_index in 0..slice_count {
            if slice_index < active_slices {
                let extra = usize::from(slice_index < remainder);
                configs.push(Some(self.config_with_total_batches(base + extra)));
            } else {
                configs.push(None);
            }
        }
        configs
    }

    /// Splits the build-wide batch budget across the concurrent address-index
    /// branch and `tx_shard_count` address-transaction shards while keeping
    /// total `pending + filling + spare` batches within
    /// `min(2 * workers, MAX_PENDING_BATCHES) + 2`. Overflow shards fall back
    /// to `None` (inline hashing on their own shard thread).
    pub(crate) fn split_secondary(
        &self,
        tx_shard_count: usize,
    ) -> (
        Option<BranchHashingConfig>,
        Vec<Option<BranchHashingConfig>>,
    ) {
        let mut slices = self
            .split_many(tx_shard_count.saturating_add(1))
            .into_iter();
        let index_config = slices.next().flatten();
        let mut tx_configs: Vec<_> = slices.take(tx_shard_count).collect();
        while tx_configs.len() < tx_shard_count {
            tx_configs.push(None);
        }
        (index_config, tx_configs)
    }
}

/// Per-branch slice of the shared [`BranchHashingPool`] and global batch cap.
pub(crate) struct BranchHashingConfig {
    pool: Arc<rayon::ThreadPool>,
    worker_count: NonZeroU32,
    payload_capacity: usize,
    max_pending: usize,
}

/// Settled partial accumulators produced by an independent derivation branch.
pub(crate) struct FinishedBranchAccumulators {
    digest: WalletProjectionDigestBuilder,
    commitment: TransparentUtxoSetCommitment,
}

fn utxo_element<'script>(
    network: Network,
    output: &WalletUnspentOutput,
    script_pub_key: &'script [u8],
) -> UtxoSetCommitmentElement<'script> {
    UtxoSetCommitmentElement {
        network_id: network.id(),
        outpoint: output.outpoint,
        value_zat: output.value_zat,
        script_pub_key,
        block_height: output.created_at.block.height,
    }
}

/// Hashing threads and the batches handed to them, oldest first.
///
/// Dropping it before every batch is combined abandons the pending batches:
/// the threads finish the batches already handed to them, discard the
/// results, and exit.
struct HashingWorkers {
    pool: Arc<rayon::ThreadPool>,
    worker_count: NonZeroU32,
    network: Network,
    filling: RowBatch,
    /// One emptied batch kept for reuse, so steady-state batches do not
    /// allocate.
    spare: Option<RowBatch>,
    pending: VecDeque<Receiver<HashedBatch>>,
    max_pending: usize,
    payload_capacity: usize,
}

/// The kind of entry a batch must make room for.
#[derive(Clone, Copy)]
enum BatchEntry {
    Row,
    UnspentOutput,
}

impl HashingWorkers {
    /// Starts `workers` hashing threads, or returns `None` for one worker,
    /// which hashes on the calling thread.
    fn start(
        workers: NonZeroU32,
        network: Network,
        payload_capacity: usize,
    ) -> Result<Option<Self>, RocksDbWalletError> {
        if workers.get() > crate::MAX_ACCUMULATOR_HASHING_WORKERS {
            return Err(RocksDbWalletError::AccumulatorHashingWorkersOutOfRange {
                workers: workers.get(),
                maximum: crate::MAX_ACCUMULATOR_HASHING_WORKERS,
            });
        }
        let threads = usize::try_from(workers.get()).unwrap_or(usize::MAX);
        if threads == 1 {
            return Ok(None);
        }
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|index| format!("zinder-wallet-hash-{index}"))
            // Without a handler, a panicking job aborts the process. The panic
            // hook has already reported it, and the job's dropped result
            // sender fails the build with a typed error.
            .panic_handler(|_panic| {})
            .build()
            .map_err(
                |source| RocksDbWalletError::AccumulatorHashingWorkersUnavailable {
                    workers: workers.get(),
                    reason: source.to_string(),
                },
            )?;
        Ok(Some(Self::from_shared_pool(
            Arc::new(pool),
            workers,
            network,
            payload_capacity,
            max_pending_for_workers(workers),
        )))
    }

    fn from_shared_pool(
        pool: Arc<rayon::ThreadPool>,
        worker_count: NonZeroU32,
        network: Network,
        payload_capacity: usize,
        max_pending: usize,
    ) -> Self {
        Self {
            pool,
            worker_count,
            network,
            filling: RowBatch::with_capacity(payload_capacity),
            spare: None,
            pending: VecDeque::new(),
            max_pending,
            payload_capacity,
        }
    }

    fn from_branch_config(network: Network, config: BranchHashingConfig) -> Self {
        Self::from_shared_pool(
            config.pool,
            config.worker_count,
            network,
            config.payload_capacity,
            config.max_pending,
        )
    }

    fn into_branch_pool(self) -> BranchHashingPool {
        BranchHashingPool {
            pool: self.pool,
            worker_count: self.worker_count,
            payload_capacity: self.payload_capacity,
        }
    }

    /// Makes room in the filling batch for one more entry of `payload_bytes`,
    /// handing the batch to a worker first when the entry would overflow it.
    ///
    /// Returns `false` for an entry larger than a whole batch, which the caller
    /// hashes on its own thread instead.
    fn make_room(
        &mut self,
        payload_bytes: usize,
        entry: BatchEntry,
        digest: &mut WalletProjectionDigestBuilder,
        commitment: &mut TransparentUtxoSetCommitment,
    ) -> Result<bool, RocksDbWalletError> {
        if payload_bytes > self.payload_capacity {
            return Ok(false);
        }
        let payload_fits = self
            .filling
            .bytes
            .len()
            .checked_add(payload_bytes)
            .is_some_and(|bytes| bytes <= self.payload_capacity);
        let descriptor_fits = match entry {
            BatchEntry::Row => self.filling.rows.len() < MAX_BATCH_ROWS,
            BatchEntry::UnspentOutput => {
                self.filling.unspent_outputs.len() < MAX_BATCH_UNSPENT_OUTPUTS
            }
        };
        if !(payload_fits && descriptor_fits) {
            self.submit(digest, commitment)?;
        }
        Ok(true)
    }

    /// Hands the filling batch to a worker.
    fn submit(
        &mut self,
        digest: &mut WalletProjectionDigestBuilder,
        commitment: &mut TransparentUtxoSetCommitment,
    ) -> Result<(), RocksDbWalletError> {
        if self.filling.is_empty() {
            return Ok(());
        }
        let next = self
            .spare
            .take()
            .unwrap_or_else(|| RowBatch::with_capacity(self.payload_capacity));
        let batch = std::mem::replace(&mut self.filling, next);
        let network = self.network;
        self.spawn(digest, commitment, move || batch.hash(network))
    }

    /// Runs `job` on a worker, first combining the oldest batch when the
    /// pending queue is full.
    fn spawn(
        &mut self,
        digest: &mut WalletProjectionDigestBuilder,
        commitment: &mut TransparentUtxoSetCommitment,
        job: impl FnOnce() -> HashedBatch + Send + 'static,
    ) -> Result<(), RocksDbWalletError> {
        if self.pending.len() >= self.max_pending
            && let Some(oldest) = self.pending.pop_front()
        {
            self.combine(&oldest, digest, commitment)?;
        }
        let (sender, receiver) = sync_channel(1);
        self.pool.spawn(move || send_hashed(&sender, job()));
        self.pending.push_back(receiver);
        Ok(())
    }

    /// Waits for every pending batch and combines it.
    fn drain(
        &mut self,
        digest: &mut WalletProjectionDigestBuilder,
        commitment: &mut TransparentUtxoSetCommitment,
    ) -> Result<(), RocksDbWalletError> {
        while let Some(oldest) = self.pending.pop_front() {
            self.combine(&oldest, digest, commitment)?;
        }
        Ok(())
    }

    fn combine(
        &mut self,
        receiver: &Receiver<HashedBatch>,
        digest: &mut WalletProjectionDigestBuilder,
        commitment: &mut TransparentUtxoSetCommitment,
    ) -> Result<(), RocksDbWalletError> {
        let HashedBatch { partial, mut batch } = receiver
            .recv()
            .map_err(|_| RocksDbWalletError::AccumulatorHashingWorkerStopped)?;
        let partial = partial?;
        digest.combine(&partial.digest)?;
        commitment.combine(&partial.commitment)?;
        batch.clear();
        self.spare.get_or_insert(batch);
        Ok(())
    }
}

fn send_hashed(sender: &SyncSender<HashedBatch>, hashed: HashedBatch) {
    // The calling thread drops the receiver only when the build stops early.
    let _build_stopped = sender.send(hashed);
}

/// A batch's partial accumulators, returned with the batch so its buffers can
/// be reused.
struct HashedBatch {
    partial: Result<PartialAccumulators, RocksDbWalletError>,
    batch: RowBatch,
}

struct PartialAccumulators {
    digest: WalletProjectionDigestBuilder,
    commitment: TransparentUtxoSetCommitment,
}

/// Rows and unspent outputs copied out of a drain into one byte buffer.
///
/// Its buffers are allocated once at their full capacity and never grow.
struct RowBatch {
    bytes: Vec<u8>,
    rows: Vec<BatchedRow>,
    unspent_outputs: Vec<BatchedUnspentOutput>,
}

struct BatchedRow {
    family: WalletProjectionRowFamily,
    start: usize,
    key_len: usize,
    value_len: usize,
}

struct BatchedUnspentOutput {
    outpoint: TransparentOutPoint,
    value_zat: u64,
    block_height: BlockHeight,
    script_start: usize,
    script_len: usize,
}

impl RowBatch {
    fn with_capacity(payload_capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(payload_capacity),
            rows: Vec::with_capacity(MAX_BATCH_ROWS),
            unspent_outputs: Vec::with_capacity(MAX_BATCH_UNSPENT_OUTPUTS),
        }
    }

    fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.unspent_outputs.is_empty()
    }

    fn clear(&mut self) {
        self.bytes.clear();
        self.rows.clear();
        self.unspent_outputs.clear();
    }

    fn push_row(&mut self, family: WalletProjectionRowFamily, key: &[u8], encoded_value: &[u8]) {
        let start = self.bytes.len();
        self.bytes.extend_from_slice(key);
        self.bytes.extend_from_slice(encoded_value);
        self.rows.push(BatchedRow {
            family,
            start,
            key_len: key.len(),
            value_len: encoded_value.len(),
        });
    }

    fn push_unspent_output(&mut self, output: &WalletUnspentOutput) {
        let script_start = self.bytes.len();
        self.bytes.extend_from_slice(&output.script_pub_key);
        self.unspent_outputs.push(BatchedUnspentOutput {
            outpoint: output.outpoint,
            value_zat: output.value_zat,
            block_height: output.created_at.block.height,
            script_start,
            script_len: output.script_pub_key.len(),
        });
    }

    fn hash(self, network: Network) -> HashedBatch {
        HashedBatch {
            partial: self.hash_partial(network),
            batch: self,
        }
    }

    fn hash_partial(&self, network: Network) -> Result<PartialAccumulators, RocksDbWalletError> {
        let mut digest = WalletProjectionDigestBuilder::new();
        for row in &self.rows {
            let key_end = row.start + row.key_len;
            digest.append_row(
                row.family,
                &self.bytes[row.start..key_end],
                &self.bytes[key_end..key_end + row.value_len],
            )?;
        }
        let mut commitment = TransparentUtxoSetCommitment::empty();
        for output in &self.unspent_outputs {
            commitment.insert(&UtxoSetCommitmentElement {
                network_id: network.id(),
                outpoint: output.outpoint,
                value_zat: output.value_zat,
                script_pub_key: &self.bytes
                    [output.script_start..output.script_start + output.script_len],
                block_height: output.block_height,
            });
        }
        Ok(PartialAccumulators { digest, commitment })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        mem::size_of,
        time::{Duration, Instant},
    };

    use zinder_core::{BlockHash, BlockId, TransactionId, TransparentAddressScriptHash};
    use zinder_wallet_projection::WalletTransactionPosition;

    use super::*;

    const NETWORK: Network = Network::ZcashTestnet;

    type SampleRow = (WalletProjectionRowFamily, Vec<u8>, Vec<u8>);

    fn workers(count: u32) -> NonZeroU32 {
        NonZeroU32::new(count).unwrap_or(NonZeroU32::MIN)
    }

    /// Deterministic rows across every family and unspent outputs with
    /// scripts of varying length.
    fn sample_input() -> (Vec<SampleRow>, Vec<WalletUnspentOutput>) {
        use WalletProjectionRowFamily::{
            ReorgUndo, TransparentAddressBalance, TransparentAddressTransaction,
            TransparentSpentOutput, TransparentUnspentOutput, TransparentUnspentOutputByAddress,
        };
        let families = [
            TransparentUnspentOutput,
            TransparentUnspentOutputByAddress,
            TransparentSpentOutput,
            TransparentAddressTransaction,
            TransparentAddressBalance,
            ReorgUndo,
        ];
        let rows = (0..240u32)
            .map(|index| {
                let seed = index.to_be_bytes();
                let family = families[usize::try_from(index).unwrap_or(0) % families.len()];
                let key = [seed.as_slice(), &[0x5a; 32]].concat();
                let value_len = usize::try_from(index % 200).unwrap_or(0);
                let encoded_value = seed.iter().copied().cycle().take(value_len).collect();
                (family, key, encoded_value)
            })
            .collect();
        let outputs = (0..90u32)
            .filter_map(|index| {
                let mut txid = [0x11; 32];
                txid[..4].copy_from_slice(&index.to_be_bytes());
                let height = BlockHeight::new(index + 1);
                let position = WalletTransactionPosition::new(
                    TransactionId::from_bytes(txid),
                    index % 7,
                    BlockId::new(height, BlockHash::from_bytes([0x22; 32])),
                );
                WalletUnspentOutput::new(
                    TransparentOutPoint::new(TransactionId::from_bytes(txid), index % 3),
                    TransparentAddressScriptHash::from_bytes([0x33; 32]),
                    u64::from(index) * 1_000,
                    vec![0x76; usize::try_from(index % 40).unwrap_or(0)],
                    position,
                )
                .ok()
            })
            .collect();
        (rows, outputs)
    }

    fn serial_reference(
        rows: &[SampleRow],
        outputs: &[WalletUnspentOutput],
    ) -> Result<(WalletProjectionDigestBuilder, TransparentUtxoSetCommitment), RocksDbWalletError>
    {
        let mut digest = WalletProjectionDigestBuilder::new();
        for (family, key, encoded_value) in rows {
            digest.append_row(*family, key, encoded_value)?;
        }
        let mut commitment = TransparentUtxoSetCommitment::empty();
        for output in outputs {
            commitment.insert(&utxo_element(NETWORK, output, &output.script_pub_key));
        }
        Ok((digest, commitment))
    }

    #[test]
    fn every_worker_count_and_batch_size_matches_the_serial_accumulators()
    -> Result<(), RocksDbWalletError> {
        let (rows, outputs) = sample_input();
        assert_eq!(outputs.len(), 90);
        let (reference_digest, reference_commitment) = serial_reference(&rows, &outputs)?;
        for worker_count in [1, 2, 3, 4, 8] {
            for payload_capacity in [1, 100, 4_096, BATCH_PAYLOAD_BYTES] {
                let mut accumulators = ConstructionAccumulators::start_with_payload_capacity(
                    NETWORK,
                    workers(worker_count),
                    payload_capacity,
                )?;
                assert_eq!(accumulators.workers.is_none(), worker_count == 1);
                let mut outputs = outputs.iter();
                for (index, (family, key, encoded_value)) in rows.iter().enumerate() {
                    accumulators.append_row(*family, key, encoded_value)?;
                    if index % 3 == 0
                        && let Some(output) = outputs.next()
                    {
                        accumulators.insert_unspent_output(output)?;
                    }
                    if index % 97 == 0 {
                        accumulators.settle()?;
                    }
                }
                for output in outputs {
                    accumulators.insert_unspent_output(output)?;
                }
                let (digest, commitment) = accumulators.finish()?;
                let case = format!("{worker_count} workers, {payload_capacity}-byte batches");
                assert_eq!(digest.row_counts(), reference_digest.row_counts(), "{case}");
                assert_eq!(
                    digest.accumulator(),
                    reference_digest.accumulator(),
                    "{case}"
                );
                assert_eq!(commitment, reference_commitment, "{case}");
            }
        }
        Ok(())
    }

    #[test]
    fn batches_never_grow_past_their_fixed_capacity() -> Result<(), RocksDbWalletError> {
        const PAYLOAD_BYTES: usize = BATCH_PAYLOAD_BYTES;
        let check = |accumulators: &ConstructionAccumulators| {
            let Some(workers) = accumulators.workers.as_ref() else {
                return;
            };
            assert!(workers.pending.len() <= workers.max_pending);
            for batch in std::iter::once(&workers.filling).chain(workers.spare.as_ref()) {
                assert_eq!(batch.bytes.capacity(), PAYLOAD_BYTES);
                assert_eq!(batch.rows.capacity(), MAX_BATCH_ROWS);
                assert_eq!(batch.unspent_outputs.capacity(), MAX_BATCH_UNSPENT_OUTPUTS);
            }
        };
        let (_, outputs) = sample_input();
        let oversized_value = vec![0x42; PAYLOAD_BYTES + 1];
        let mut rows: Vec<SampleRow> = (0..10_000u32)
            .map(|index| {
                (
                    WalletProjectionRowFamily::TransparentAddressBalance,
                    index.to_be_bytes().to_vec(),
                    Vec::new(),
                )
            })
            .collect();
        rows.push((
            WalletProjectionRowFamily::ReorgUndo,
            7u32.to_be_bytes().to_vec(),
            vec![0x24; PAYLOAD_BYTES - 4],
        ));
        rows.push((
            WalletProjectionRowFamily::ReorgUndo,
            8u32.to_be_bytes().to_vec(),
            oversized_value,
        ));
        let (reference_digest, reference_commitment) = serial_reference(&rows, &outputs)?;
        let mut accumulators = ConstructionAccumulators::start(NETWORK, workers(2))?;
        for (family, key, encoded_value) in &rows {
            accumulators.append_row(*family, key, encoded_value)?;
            check(&accumulators);
        }
        for output in &outputs {
            accumulators.insert_unspent_output(output)?;
            check(&accumulators);
        }
        let (digest, commitment) = accumulators.finish()?;
        assert_eq!(digest.row_counts(), reference_digest.row_counts());
        assert_eq!(digest.accumulator(), reference_digest.accumulator());
        assert_eq!(commitment, reference_commitment);
        Ok(())
    }

    #[test]
    fn one_batch_allocates_at_most_half_a_mebibyte() {
        // The configuration reference and the storage architecture page state
        // this bound. The descriptors hold pointer-sized offsets, so a batch is
        // exactly 512 KiB on 64-bit targets and smaller on 32-bit ones.
        let batch = RowBatch::with_capacity(BATCH_PAYLOAD_BYTES);
        let batch_allocation_bytes = batch.bytes.capacity()
            + batch.rows.capacity() * size_of::<BatchedRow>()
            + batch.unspent_outputs.capacity() * size_of::<BatchedUnspentOutput>();
        assert!(
            batch_allocation_bytes <= 512 * 1024,
            "{batch_allocation_bytes} bytes"
        );
        if cfg!(target_pointer_width = "64") {
            assert_eq!(batch_allocation_bytes, 512 * 1024);
        }
    }

    #[test]
    fn the_pending_queue_is_twice_the_workers_up_to_its_cap() -> Result<(), RocksDbWalletError> {
        for (worker_count, expected) in [(2, 4), (4, 8), (31, 62), (32, 64), (33, 64), (64, 64)] {
            let accumulators = ConstructionAccumulators::start(NETWORK, workers(worker_count))?;
            let max_pending = accumulators
                .workers
                .as_ref()
                .map_or(0, |workers| workers.max_pending);
            assert_eq!(max_pending, expected, "{worker_count} workers");
        }
        Ok(())
    }

    #[test]
    fn pending_batches_never_exceed_twice_the_worker_count() -> Result<(), RocksDbWalletError> {
        let (rows, _) = sample_input();
        let mut accumulators =
            ConstructionAccumulators::start_with_payload_capacity(NETWORK, workers(3), 100)?;
        for (family, key, encoded_value) in &rows {
            accumulators.append_row(*family, key, encoded_value)?;
            let pending = accumulators
                .workers
                .as_ref()
                .map_or(0, |workers| workers.pending.len());
            assert!(pending <= 6, "{pending} pending batches");
        }
        accumulators.finish()?;
        Ok(())
    }

    #[test]
    fn a_panicking_worker_fails_with_a_typed_error() -> Result<(), RocksDbWalletError> {
        let mut accumulators = ConstructionAccumulators::start(NETWORK, workers(2))?;
        let Some(hashers) = accumulators.workers.as_mut() else {
            return Err(RocksDbWalletError::AccumulatorHashingWorkerStopped);
        };
        hashers.spawn(
            &mut accumulators.digest,
            &mut accumulators.commitment,
            || -> HashedBatch { std::panic::resume_unwind(Box::new("injected worker panic")) },
        )?;
        assert!(matches!(
            accumulators.finish(),
            Err(RocksDbWalletError::AccumulatorHashingWorkerStopped)
        ));
        Ok(())
    }

    #[test]
    fn a_worker_error_fails_the_build_with_that_error() -> Result<(), RocksDbWalletError> {
        let mut accumulators = ConstructionAccumulators::start(NETWORK, workers(2))?;
        let Some(hashers) = accumulators.workers.as_mut() else {
            return Err(RocksDbWalletError::AccumulatorHashingWorkerStopped);
        };
        hashers.spawn(
            &mut accumulators.digest,
            &mut accumulators.commitment,
            || HashedBatch {
                partial: Err(RocksDbWalletError::BuildCounterOverflow),
                batch: RowBatch::with_capacity(0),
            },
        )?;
        assert!(matches!(
            accumulators.finish(),
            Err(RocksDbWalletError::BuildCounterOverflow)
        ));
        Ok(())
    }

    #[test]
    fn dropping_with_pending_batches_does_not_wait_for_them() -> Result<(), RocksDbWalletError> {
        let mut accumulators = ConstructionAccumulators::start(NETWORK, workers(2))?;
        let Some(hashers) = accumulators.workers.as_mut() else {
            return Err(RocksDbWalletError::AccumulatorHashingWorkerStopped);
        };
        for _ in 0..4 {
            hashers.spawn(
                &mut accumulators.digest,
                &mut accumulators.commitment,
                || {
                    std::thread::sleep(Duration::from_millis(200));
                    RowBatch::with_capacity(0).hash(NETWORK)
                },
            )?;
        }
        let started = Instant::now();
        drop(accumulators);
        assert!(started.elapsed() < Duration::from_millis(150));
        Ok(())
    }

    #[test]
    fn branch_hashing_pool_retires_primary_and_bounds_global_threads_and_batches()
    -> Result<(), RocksDbWalletError> {
        let mut serial = ConstructionAccumulators::start(NETWORK, workers(1))?;
        assert!(serial.take_branch_hashing_pool()?.is_none());
        let inline_branch = ConstructionAccumulators::start_optional_branch(NETWORK, None);
        assert!(inline_branch.workers.is_none());

        for worker_count in [2, 3, 4, 8, 31, 32, 33, 64] {
            let mut primary = ConstructionAccumulators::start(NETWORK, workers(worker_count))?;
            let primary_pool = primary
                .workers
                .as_ref()
                .map(|w| Arc::clone(&w.pool))
                .ok_or(RocksDbWalletError::AccumulatorHashingWorkerStopped)?;
            let branch_pool = primary
                .take_branch_hashing_pool()?
                .ok_or(RocksDbWalletError::AccumulatorHashingWorkerStopped)?;
            assert!(
                primary.workers.is_none(),
                "primary must drop its pool and batches on retire"
            );

            let (index_config, tx_config) = branch_pool.split_two();
            assert!(Arc::ptr_eq(&index_config.pool, &primary_pool));
            assert!(Arc::ptr_eq(&tx_config.pool, &primary_pool));
            assert_eq!(
                primary_pool.current_num_threads(),
                usize::try_from(worker_count).unwrap_or(usize::MAX)
            );
            let total_pending_cap = index_config.max_pending + tx_config.max_pending;
            assert!(
                total_pending_cap <= MAX_PENDING_BATCHES,
                "{worker_count} workers exceeded global pending ceiling: {total_pending_cap}"
            );
            let total_batch_cap = (index_config.max_pending + 2) + (tx_config.max_pending + 2);
            let expected_cap = (usize::try_from(worker_count).unwrap_or(usize::MAX) * 2)
                .min(MAX_PENDING_BATCHES)
                + 2;
            assert!(
                total_batch_cap <= expected_cap,
                "{worker_count} workers total batch cap {total_batch_cap} > {expected_cap}"
            );

            drop(index_config);
            drop(tx_config);
            for tx_shard_count in [0, 1, 4, 8, 16] {
                let (shard_idx_cfg, shard_tx_cfgs) = branch_pool.split_secondary(tx_shard_count);
                assert_eq!(shard_tx_cfgs.len(), tx_shard_count);
                let mut total_slice_batches = 0_usize;
                for cfg in std::iter::once(&shard_idx_cfg)
                    .chain(shard_tx_cfgs.iter())
                    .flatten()
                {
                    assert!(Arc::ptr_eq(&cfg.pool, &primary_pool));
                    total_slice_batches += cfg.max_pending + 2;
                }
                assert!(
                    total_slice_batches <= expected_cap,
                    "{worker_count} workers / {tx_shard_count} shards exceeded budget: {total_slice_batches} > {expected_cap}"
                );
            }
            for slice_count in [0, 1, 2, 4, 8, 16] {
                let slice_cfgs = branch_pool.split_many(slice_count);
                assert_eq!(slice_cfgs.len(), slice_count);
                let mut total_slice_batches = 0_usize;
                for cfg in slice_cfgs.iter().flatten() {
                    assert!(Arc::ptr_eq(&cfg.pool, &primary_pool));
                    total_slice_batches += cfg.max_pending + 2;
                }
                assert!(
                    total_slice_batches <= expected_cap,
                    "{worker_count} workers / {slice_count} split_many slices exceeded budget: {total_slice_batches} > {expected_cap}"
                );
            }
            primary.restore_branch_hashing_pool(branch_pool);
            let restored = primary
                .workers
                .as_ref()
                .ok_or(RocksDbWalletError::AccumulatorHashingWorkerStopped)?;
            assert!(Arc::ptr_eq(&restored.pool, &primary_pool));
        }
        Ok(())
    }

    #[test]
    fn shared_branch_pools_match_serial_accumulators_across_phases()
    -> Result<(), RocksDbWalletError> {
        let (rows, outputs) = sample_input();
        let (reference_digest, reference_commitment) = serial_reference(&rows, &outputs)?;
        let (primary_rows, rest_rows) = rows.split_at(80);
        let (index_rows, middle_rows) = rest_rows.split_at(60);
        let (tx_rows, undo_rows) = middle_rows.split_at(60);

        let mut primary =
            ConstructionAccumulators::start_with_payload_capacity(NETWORK, workers(4), 100)?;
        for (family, key, value) in primary_rows {
            primary.append_row(*family, key, value)?;
        }
        for output in &outputs {
            primary.insert_unspent_output(output)?;
        }

        let branch_pool = primary
            .take_branch_hashing_pool()?
            .ok_or(RocksDbWalletError::AccumulatorHashingWorkerStopped)?;
        assert!(primary.workers.is_none());
        let (index_cfg, tx_cfg) = branch_pool.split_two();

        let (index_finished, tx_finished) = std::thread::scope(|scope| {
            let tx_handle = scope.spawn(
                move || -> Result<FinishedBranchAccumulators, RocksDbWalletError> {
                    let mut branch =
                        ConstructionAccumulators::start_optional_branch(NETWORK, Some(tx_cfg));
                    for (family, key, value) in tx_rows {
                        branch.append_row(*family, key, value)?;
                    }
                    branch.finish_branch()
                },
            );
            let mut index_branch =
                ConstructionAccumulators::start_optional_branch(NETWORK, Some(index_cfg));
            for (family, key, value) in index_rows {
                index_branch.append_row(*family, key, value)?;
            }
            let index_res = index_branch.finish_branch();
            let tx_res = tx_handle
                .join()
                .map_err(|_| RocksDbWalletError::AccumulatorHashingWorkerStopped)?;
            Ok::<_, RocksDbWalletError>((index_res?, tx_res?))
        })?;

        primary.restore_branch_hashing_pool(branch_pool);
        primary.combine_branch(&index_finished)?;
        primary.combine_branch(&tx_finished)?;
        for (family, key, value) in undo_rows {
            primary.append_row(*family, key, value)?;
        }
        let (digest, commitment) = primary.finish()?;
        assert_eq!(digest.row_counts(), reference_digest.row_counts());
        assert_eq!(digest.accumulator(), reference_digest.accumulator());
        assert_eq!(commitment, reference_commitment);
        Ok(())
    }
}
