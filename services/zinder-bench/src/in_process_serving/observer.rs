//! Synced-batch observer: write timing plus synthetic density ballast.
//!
//! The ballast arm scales each real batch by copying real row values (read
//! from the same stores at startup) into a dedicated column family of the same
//! database, inside the same synced batch. Canonical ballast keys are
//! append-only like height keys; wallet ballast keys are hash-distributed like
//! outpoint keys and half of them are deleted again a fixed number of writes
//! later, which reproduces the wallet's insert-then-delete churn.

use std::{
    collections::{HashMap, VecDeque, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use parking_lot::Mutex;
use rust_rocksdb::{DB, WriteBatch};
use zinder_store::{SpikeBatchKind, SpikeBatchObserver};

pub(crate) const BALLAST_COLUMN_FAMILY: &str = "spike_density_ballast";
const WALLET_BALLAST_DELETE_LAG_WRITES: usize = 100;

/// The last observed synced write of one kind.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct LastWrite {
    pub(crate) base_bytes: usize,
    pub(crate) ballast_bytes: usize,
    pub(crate) final_bytes: usize,
    pub(crate) write_seconds: f64,
}

#[derive(Default)]
struct BallastState {
    canonical_pool: Vec<Vec<u8>>,
    wallet_pool: Vec<Vec<u8>>,
    counter: u64,
    cursor: usize,
    wallet_recent: VecDeque<Vec<[u8; 32]>>,
}

pub(crate) struct SpikeObserver {
    multiplier_bits: AtomicU64,
    pending: Mutex<HashMap<SpikeBatchKind, (usize, usize)>>,
    last: Mutex<HashMap<SpikeBatchKind, LastWrite>>,
    ballast: Mutex<BallastState>,
}

impl SpikeObserver {
    pub(crate) fn new(canonical_pool: Vec<Vec<u8>>, wallet_pool: Vec<Vec<u8>>) -> Self {
        Self {
            multiplier_bits: AtomicU64::new(1.0_f64.to_bits()),
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

    fn add_ballast(
        &self,
        kind: SpikeBatchKind,
        db: &DB,
        batch: &mut WriteBatch,
        target: usize,
    ) -> usize {
        let Some(family) = db.cf_handle(BALLAST_COLUMN_FAMILY) else {
            return 0;
        };
        let mut ballast = self.ballast.lock();
        let wallet = kind == SpikeBatchKind::WalletTransition;
        let pool_len = if wallet && !ballast.wallet_pool.is_empty() {
            ballast.wallet_pool.len()
        } else {
            ballast.canonical_pool.len()
        };
        if pool_len == 0 {
            return 0;
        }
        let mut added = 0;
        let mut inserted = Vec::new();
        while added < target {
            ballast.counter += 1;
            ballast.cursor = (ballast.cursor + 1) % pool_len;
            let cursor = ballast.cursor;
            let counter = ballast.counter;
            let value = if wallet && !ballast.wallet_pool.is_empty() {
                &ballast.wallet_pool[cursor]
            } else {
                &ballast.canonical_pool[cursor]
            };
            if wallet {
                let key = hashed_key(counter);
                batch.put_cf(&family, key, value);
                added += key.len() + value.len();
                inserted.push(key);
            } else {
                let key = counter.to_be_bytes();
                batch.put_cf(&family, key, value);
                added += key.len() + value.len();
            }
        }
        if wallet {
            ballast.wallet_recent.push_back(inserted);
            if ballast.wallet_recent.len() > WALLET_BALLAST_DELETE_LAG_WRITES
                && let Some(old) = ballast.wallet_recent.pop_front()
            {
                for key in old.iter().step_by(2) {
                    batch.delete_cf(&family, key);
                }
            }
        }
        added
    }
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
