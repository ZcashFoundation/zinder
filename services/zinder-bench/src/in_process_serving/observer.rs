//! Synced-batch observer: write timing plus synthetic density ballast.
//!
//! The ballast arm scales each real batch by copying real row values (read
//! from the same stores at startup) into a dedicated column family of the same
//! database, inside the same synced batch. Canonical ballast keys are
//! append-only like height keys. Wallet ballast keys are hash-distributed like
//! outpoint keys. A quarter of each wallet write's keys are deleted 100 writes
//! later and another quarter `long_delete_lag_writes` later, so the wallet
//! ballast reproduces both quick spends and spends of outputs created long
//! before a reader's snapshot. Only the long-lag deletes can pin garbage for
//! the length of a long snapshot hold. Keys are derived from a counter, so the
//! delete schedule needs only the counter range of each write.

use std::{
    collections::{HashMap, VecDeque, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use rust_rocksdb::{DB, FlushOptions, WriteBatch, WriteOptions};
use serde::Serialize;
use zinder_store::{SpikeBatchKind, SpikeBatchObserver};

pub(crate) const BALLAST_COLUMN_FAMILY: &str = "spike_density_ballast";
const WALLET_BALLAST_SHORT_DELETE_LAG_WRITES: usize = 100;
/// Default long delete lag: 3,600 writes is 30 minutes at 500 ms pacing.
pub(crate) const DEFAULT_LONG_DELETE_LAG_WRITES: usize = 3_600;

/// The last observed synced write of one kind.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct LastWrite {
    pub(crate) base_bytes: usize,
    pub(crate) ballast_bytes: usize,
    pub(crate) final_bytes: usize,
    pub(crate) write_seconds: f64,
}

/// Running totals of ballast keys written and deleted.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub(crate) struct BallastCounts {
    pub(crate) canonical_inserted_keys: u64,
    pub(crate) canonical_ballast_bytes: u64,
    pub(crate) wallet_inserted_keys: u64,
    pub(crate) wallet_deleted_keys: u64,
    pub(crate) wallet_short_lag_deleted_keys: u64,
    pub(crate) wallet_long_lag_deleted_keys: u64,
    pub(crate) wallet_ballast_bytes: u64,
    pub(crate) wallet_writes: u64,
}

impl BallastCounts {
    /// Wallet ballast keys that must still be readable.
    pub(crate) fn wallet_live_keys(&self) -> u64 {
        self.wallet_inserted_keys
            .saturating_sub(self.wallet_deleted_keys)
    }
}

/// Evidence of the setup-time preseed that fills the long delete window.
#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct PreseedRecord {
    pub(crate) writes: usize,
    pub(crate) bytes_per_write: usize,
    pub(crate) seconds: f64,
    pub(crate) flush_and_compact_seconds: f64,
    pub(crate) counts_after: BallastCounts,
}

#[derive(Default)]
struct BallastState {
    canonical_pool: Vec<Vec<u8>>,
    wallet_pool: Vec<Vec<u8>>,
    canonical_counter: u64,
    wallet_counter: u64,
    cursor: usize,
    /// Counter range `(first, count)` of each recent wallet ballast write, oldest first.
    wallet_history: VecDeque<(u64, u64)>,
    counts: BallastCounts,
}

pub(crate) struct SpikeObserver {
    multiplier_bits: AtomicU64,
    long_delete_lag_writes: usize,
    pending: Mutex<HashMap<SpikeBatchKind, (usize, usize)>>,
    last: Mutex<HashMap<SpikeBatchKind, LastWrite>>,
    ballast: Mutex<BallastState>,
}

impl SpikeObserver {
    pub(crate) fn new(
        canonical_pool: Vec<Vec<u8>>,
        wallet_pool: Vec<Vec<u8>>,
        long_delete_lag_writes: usize,
    ) -> Self {
        Self {
            multiplier_bits: AtomicU64::new(1.0_f64.to_bits()),
            long_delete_lag_writes: long_delete_lag_writes.max(WALLET_BALLAST_SHORT_DELETE_LAG_WRITES + 1),
            pending: Mutex::new(HashMap::new()),
            last: Mutex::new(HashMap::new()),
            ballast: Mutex::new(BallastState {
                canonical_pool,
                wallet_pool,
                ..BallastState::default()
            }),
        }
    }

    pub(crate) fn set_multiplier(&self, multiplier: f64) {
        self.multiplier_bits
            .store(multiplier.to_bits(), Ordering::SeqCst);
    }

    fn multiplier(&self) -> f64 {
        f64::from_bits(self.multiplier_bits.load(Ordering::SeqCst))
    }

    /// Returns and clears the last write of `kind`.
    pub(crate) fn take_last(&self, kind: SpikeBatchKind) -> Option<LastWrite> {
        self.last.lock().remove(&kind)
    }

    pub(crate) fn pool_sizes(&self) -> (usize, usize) {
        let ballast = self.ballast.lock();
        (ballast.canonical_pool.len(), ballast.wallet_pool.len())
    }

    pub(crate) fn counts(&self) -> BallastCounts {
        self.ballast.lock().counts
    }

    pub(crate) fn long_delete_lag_writes(&self) -> usize {
        self.long_delete_lag_writes
    }

    /// Fills the wallet ballast family with `writes` unsynced writes of about
    /// `bytes_per_write`, applying the normal delete schedule, then flushes and
    /// compacts the family so measured phases start from a settled tree.
    pub(crate) fn preseed_wallet(
        &self,
        db: &DB,
        writes: usize,
        bytes_per_write: usize,
    ) -> Result<PreseedRecord, String> {
        let family = db
            .cf_handle(BALLAST_COLUMN_FAMILY)
            .ok_or("wallet ballast family is absent")?;
        let started = Instant::now();
        let mut write_options = WriteOptions::default();
        write_options.set_sync(false);
        for _ in 0..writes {
            let mut batch = WriteBatch::default();
            {
                let mut ballast = self.ballast.lock();
                wallet_ballast(&mut ballast, self.long_delete_lag_writes, db, &mut batch, bytes_per_write);
            }
            db.write_opt(&batch, &write_options)
                .map_err(|error| format!("preseed write failed: {error}"))?;
        }
        let seconds = started.elapsed().as_secs_f64();
        let started = Instant::now();
        let mut flush = FlushOptions::default();
        flush.set_wait(true);
        db.flush_cf_opt(&family, &flush)
            .map_err(|error| format!("preseed flush failed: {error}"))?;
        db.compact_range_cf(&family, None::<&[u8]>, None::<&[u8]>);
        Ok(PreseedRecord {
            writes,
            bytes_per_write,
            seconds,
            flush_and_compact_seconds: started.elapsed().as_secs_f64(),
            counts_after: self.counts(),
        })
    }

    fn add_ballast(
        &self,
        kind: SpikeBatchKind,
        db: &DB,
        batch: &mut WriteBatch,
        target: usize,
    ) -> usize {
        let mut ballast = self.ballast.lock();
        if kind == SpikeBatchKind::WalletTransition && !ballast.wallet_pool.is_empty() {
            return wallet_ballast(&mut ballast, self.long_delete_lag_writes, db, batch, target);
        }
        let Some(family) = db.cf_handle(BALLAST_COLUMN_FAMILY) else {
            return 0;
        };
        let pool_len = ballast.canonical_pool.len();
        if pool_len == 0 {
            return 0;
        }
        let mut added = 0;
        while added < target {
            ballast.canonical_counter += 1;
            ballast.cursor = (ballast.cursor + 1) % pool_len;
            let key = ballast.canonical_counter.to_be_bytes();
            let value = &ballast.canonical_pool[ballast.cursor];
            batch.put_cf(&family, key, value);
            added += key.len() + value.len();
            ballast.counts.canonical_inserted_keys += 1;
        }
        ballast.counts.canonical_ballast_bytes += added as u64;
        added
    }
}

/// Adds hash-keyed wallet ballast plus the scheduled deletes to `batch`.
fn wallet_ballast(
    ballast: &mut BallastState,
    long_lag: usize,
    db: &DB,
    batch: &mut WriteBatch,
    target: usize,
) -> usize {
    let Some(family) = db.cf_handle(BALLAST_COLUMN_FAMILY) else {
        return 0;
    };
    let pool_len = ballast.wallet_pool.len();
    if pool_len == 0 {
        return 0;
    }
    let first = ballast.wallet_counter + 1;
    let mut added = 0;
    while added < target {
        ballast.wallet_counter += 1;
        ballast.cursor = (ballast.cursor + 1) % pool_len;
        let key = hashed_key(ballast.wallet_counter);
        let value = &ballast.wallet_pool[ballast.cursor];
        batch.put_cf(&family, key, value);
        added += key.len() + value.len();
    }
    let count = ballast.wallet_counter + 1 - first;
    ballast.counts.wallet_inserted_keys += count;
    ballast.counts.wallet_ballast_bytes += added as u64;
    ballast.counts.wallet_writes += 1;
    ballast.wallet_history.push_back((first, count));
    let len = ballast.wallet_history.len();
    let mut delete = |range: (u64, u64), offset: u64| -> u64 {
        let (start, count) = range;
        let mut deleted = 0;
        let mut index = offset;
        while index < count {
            batch.delete_cf(&family, hashed_key(start + index));
            deleted += 1;
            index += 4;
        }
        deleted
    };
    if len > WALLET_BALLAST_SHORT_DELETE_LAG_WRITES {
        let range = ballast.wallet_history[len - 1 - WALLET_BALLAST_SHORT_DELETE_LAG_WRITES];
        let deleted = delete(range, 0);
        ballast.counts.wallet_short_lag_deleted_keys += deleted;
        ballast.counts.wallet_deleted_keys += deleted;
    }
    if len > long_lag {
        let range = ballast.wallet_history[len - 1 - long_lag];
        let deleted = delete(range, 2);
        ballast.counts.wallet_long_lag_deleted_keys += deleted;
        ballast.counts.wallet_deleted_keys += deleted;
    }
    while ballast.wallet_history.len() > long_lag + 1 {
        ballast.wallet_history.pop_front();
    }
    added
}

fn hashed_key(counter: u64) -> [u8; 32] {
    let mut key = [0_u8; 32];
    for (lane, chunk) in key.chunks_mut(8).enumerate() {
        let mut hasher = DefaultHasher::new();
        (counter, lane).hash(&mut hasher);
        chunk.copy_from_slice(&hasher.finish().to_be_bytes());
    }
    key
}

impl SpikeBatchObserver for SpikeObserver {
    fn before_write(&self, kind: SpikeBatchKind, db: &DB, batch: &mut WriteBatch) {
        let base = batch.size_in_bytes();
        let multiplier = self.multiplier();
        let ballast = if multiplier > 1.0 {
            let target = ((multiplier - 1.0) * base as f64) as usize;
            self.add_ballast(kind, db, batch, target)
        } else {
            0
        };
        self.pending.lock().insert(kind, (base, ballast));
    }

    fn after_write(&self, kind: SpikeBatchKind, batch_bytes: usize, write_elapsed: Duration) {
        let (base_bytes, ballast_bytes) = self.pending.lock().remove(&kind).unwrap_or_default();
        self.last.lock().insert(
            kind,
            LastWrite {
                base_bytes,
                ballast_bytes,
                final_bytes: batch_bytes,
                write_seconds: write_elapsed.as_secs_f64(),
            },
        );
    }
}
