//! Accumulator hashing for fresh wallet construction.
//!
//! The projection row accumulator and the UTXO set commitment are both sums of
//! per-row `LtHash16` expansions, so rows hashed into separate partial
//! accumulators on different threads combine into the same bytes as one serial
//! pass. With one worker, every row is hashed on the calling thread. With more,
//! the calling thread copies rows into bounded batches, and each batch is
//! hashed into its own partial accumulators on a worker.

use std::{
    collections::VecDeque,
    num::NonZeroU32,
    sync::mpsc::{Receiver, SyncSender, sync_channel},
};

use zinder_core::{
    BlockHeight, Network, TransparentOutPoint, TransparentUtxoSetCommitment,
    wire::UtxoSetCommitmentElement,
};
use zinder_wallet_projection::{
    WalletProjectionDigestBuilder, WalletProjectionRowFamily, WalletUnspentOutput,
};

use crate::RocksDbWalletError;

/// Row bytes after which a batch is handed to a worker.
///
/// A batch holds at most this many bytes plus its final row.
const BATCH_TARGET_BYTES: usize = 256 * 1024;

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
        Self::start_with_batch_target(network, workers, BATCH_TARGET_BYTES)
    }

    fn start_with_batch_target(
        network: Network,
        workers: NonZeroU32,
        batch_target_bytes: usize,
    ) -> Result<Self, RocksDbWalletError> {
        Ok(Self {
            network,
            digest: WalletProjectionDigestBuilder::new(),
            commitment: TransparentUtxoSetCommitment::empty(),
            workers: HashingWorkers::start(workers, network, batch_target_bytes)?,
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
        workers.filling.push_row(family, key, encoded_value);
        workers.submit_if_full(&mut self.digest, &mut self.commitment)
    }

    /// Adds one unspent output to the UTXO set commitment.
    pub(crate) fn insert_unspent_output(
        &mut self,
        output: &WalletUnspentOutput,
    ) -> Result<(), RocksDbWalletError> {
        let Some(workers) = self.workers.as_mut() else {
            self.commitment
                .insert(&utxo_element(self.network, output, &output.script_pub_key));
            return Ok(());
        };
        workers.filling.push_unspent_output(output);
        workers.submit_if_full(&mut self.digest, &mut self.commitment)
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

    /// Settles every batch and returns the combined accumulators.
    pub(crate) fn finish(
        mut self,
    ) -> Result<(WalletProjectionDigestBuilder, TransparentUtxoSetCommitment), RocksDbWalletError>
    {
        self.settle()?;
        Ok((self.digest, self.commitment))
    }
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
    pool: rayon::ThreadPool,
    network: Network,
    filling: RowBatch,
    /// One emptied batch kept for reuse, so steady-state batches do not
    /// allocate.
    spare: Option<RowBatch>,
    pending: VecDeque<Receiver<HashedBatch>>,
    max_pending: usize,
    batch_target_bytes: usize,
}

impl HashingWorkers {
    /// Starts `workers` hashing threads, or returns `None` for one worker,
    /// which hashes on the calling thread.
    fn start(
        workers: NonZeroU32,
        network: Network,
        batch_target_bytes: usize,
    ) -> Result<Option<Self>, RocksDbWalletError> {
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
        Ok(Some(Self {
            pool,
            network,
            filling: RowBatch::default(),
            spare: None,
            pending: VecDeque::new(),
            max_pending: threads.saturating_mul(2).min(MAX_PENDING_BATCHES),
            batch_target_bytes,
        }))
    }

    fn submit_if_full(
        &mut self,
        digest: &mut WalletProjectionDigestBuilder,
        commitment: &mut TransparentUtxoSetCommitment,
    ) -> Result<(), RocksDbWalletError> {
        if self.filling.bytes.len() < self.batch_target_bytes {
            return Ok(());
        }
        self.submit(digest, commitment)
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
        let batch = std::mem::replace(&mut self.filling, self.spare.take().unwrap_or_default());
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
        commitment.combine(&partial.commitment);
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
#[derive(Default)]
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
    use std::time::{Duration, Instant};

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
                let value = seed.iter().copied().cycle().take(value_len).collect();
                (family, key, value)
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
        for (family, key, value) in rows {
            digest.append_row(*family, key, value)?;
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
            for batch_target_bytes in [1, 100, 4_096, BATCH_TARGET_BYTES] {
                let mut accumulators = ConstructionAccumulators::start_with_batch_target(
                    NETWORK,
                    workers(worker_count),
                    batch_target_bytes,
                )?;
                assert_eq!(accumulators.workers.is_none(), worker_count == 1);
                let mut outputs = outputs.iter();
                for (index, (family, key, value)) in rows.iter().enumerate() {
                    accumulators.append_row(*family, key, value)?;
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
                let case = format!("{worker_count} workers, {batch_target_bytes}-byte batches");
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
    fn pending_batches_never_exceed_twice_the_worker_count() -> Result<(), RocksDbWalletError> {
        let (rows, _) = sample_input();
        let mut accumulators =
            ConstructionAccumulators::start_with_batch_target(NETWORK, workers(3), 1)?;
        for (family, key, value) in &rows {
            accumulators.append_row(*family, key, value)?;
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
                batch: RowBatch::default(),
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
                    RowBatch::default().hash(NETWORK)
                },
            )?;
        }
        let started = Instant::now();
        drop(accumulators);
        assert!(started.elapsed() < Duration::from_millis(150));
        Ok(())
    }
}
