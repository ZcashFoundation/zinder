//! Mirror lanes: synthetic row volume derived from each real block's rows.
//!
//! A lane replays every real canonical row and every real wallet row
//! operation (put, delete, and planner point read) into `mirror_<family>`
//! column families with the same options, with key byte 0 XOR-ed by the lane
//! id. The density arm runs lanes inline in the commit batch, so the synced
//! batch carries `multiplier` times the real rows. Arm 3 runs lanes in a
//! background writer (unsynced, WAL on) to add compaction debt.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        mpsc::{Receiver, SyncSender, sync_channel},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use rust_rocksdb::{DB, WriteBatch, WriteOptions};
use serde::Serialize;
use zinder_bench::BenchError;
use zinder_wallet_projection::WalletProjectionDigestBuilder;
use zinder_wallet_rocksdb::{
    SpikeMirrorPlanOverlay, SpikeWalletPlanOps, spike_wallet_data_column_family_names,
};

use super::{
    layout::{CANONICAL_DATA_FAMILIES, mirror_family},
    spike_error,
};

/// The real rows of one committed block, copied for mirroring.
#[derive(Default)]
pub(crate) struct MirrorWork {
    pub(crate) canonical_rows: Vec<(&'static str, Vec<u8>, Vec<u8>)>,
    pub(crate) wallet: Option<SpikeWalletPlanOps>,
}

/// Which real rows a lane mirrors.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, clap::ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum MirrorFamilies {
    /// Canonical rows and wallet row operations.
    All,
    /// Wallet row operations only.
    Wallet,
}

/// Counters for mirrored operations.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub(crate) struct MirrorCounts {
    pub(crate) puts: u64,
    pub(crate) deletes: u64,
    pub(crate) reads: u64,
    pub(crate) read_hits: u64,
}

impl MirrorCounts {
    fn add(&mut self, other: Self) {
        self.puts += other.puts;
        self.deletes += other.deletes;
        self.reads += other.reads;
        self.read_hits += other.read_hits;
    }
}

/// Mirror family names resolved once.
#[derive(Clone)]
pub(crate) struct MirrorFamilyNames {
    names: Arc<HashMap<&'static str, String>>,
}

impl MirrorFamilyNames {
    pub(crate) fn new() -> Self {
        let names = CANONICAL_DATA_FAMILIES
            .into_iter()
            .chain(spike_wallet_data_column_family_names())
            .map(|name| (name, mirror_family(name)))
            .collect();
        Self {
            names: Arc::new(names),
        }
    }

    pub(crate) fn get(&self, family: &'static str) -> Result<&str, BenchError> {
        self.names
            .get(family)
            .map(String::as_str)
            .ok_or_else(|| BenchError::spike(format!("no mirror family for {family}")))
    }
}

pub(crate) fn lane_key(key: &[u8], lane: u8) -> Vec<u8> {
    if key.is_empty() {
        return vec![lane];
    }
    let mut mirrored = key.to_vec();
    mirrored[0] ^= lane;
    mirrored
}

/// Applies `lanes` mirror lanes of `work` to `batch`.
///
/// With `reads`, each recorded planner point read is repeated against the
/// lane's mirror family. With `accumulator`, each mirrored wallet row is also
/// folded into a scratch projection accumulator, as the planner does. With
/// `plan_overlay`, each mirrored wallet row is also charged and held under the
/// planner's overlay and logical-byte rules.
pub(crate) fn apply_lanes(
    db: &DB,
    names: &MirrorFamilyNames,
    batch: &mut WriteBatch,
    work: &MirrorWork,
    lanes: std::ops::RangeInclusive<u8>,
    families: MirrorFamilies,
    reads: bool,
    mut accumulator: Option<&mut WalletProjectionDigestBuilder>,
    mut plan_overlay: Option<&mut SpikeMirrorPlanOverlay>,
) -> Result<MirrorCounts, BenchError> {
    let mut counts = MirrorCounts::default();
    for lane in lanes {
        if families == MirrorFamilies::All {
            for (family, key, value) in &work.canonical_rows {
                let handle = db
                    .cf_handle(names.get(family)?)
                    .ok_or_else(|| BenchError::spike(format!("missing mirror of {family}")))?;
                batch.put_cf(&handle, lane_key(key, lane), value);
                counts.puts += 1;
            }
        }
        let Some(wallet) = work.wallet.as_ref() else {
            continue;
        };
        if reads {
            for (family, key) in &wallet.reads {
                let handle = db
                    .cf_handle(names.get(family)?)
                    .ok_or_else(|| BenchError::spike(format!("missing mirror of {family}")))?;
                let found = db
                    .get_cf(&handle, lane_key(key, lane))
                    .map_err(|source| spike_error("mirror point read", source))?;
                counts.reads += 1;
                counts.read_hits += u64::from(found.is_some());
            }
        }
        for op in &wallet.row_ops {
            let handle = db
                .cf_handle(names.get(op.column_family)?)
                .ok_or_else(|| BenchError::spike("missing mirror wallet family"))?;
            let key = lane_key(&op.key, lane);
            if let Some(accumulator) = accumulator.as_deref_mut() {
                if op.insert {
                    accumulator
                        .append_row(op.family, &key, &op.value)
                        .map_err(|source| spike_error("mirror accumulator", source))?;
                } else {
                    accumulator
                        .remove_row(op.family, &key, &op.value)
                        .map_err(|source| spike_error("mirror accumulator", source))?;
                }
            }
            if let Some(plan_overlay) = plan_overlay.as_deref_mut() {
                if op.insert {
                    plan_overlay
                        .put(lane, op.column_family, &key, &op.value)
                        .map_err(|source| spike_error("mirror plan overlay", source))?;
                } else {
                    plan_overlay
                        .delete(lane, op.column_family, &key)
                        .map_err(|source| spike_error("mirror plan overlay", source))?;
                }
            }
            if op.insert {
                batch.put_cf(&handle, key, &op.value);
                counts.puts += 1;
            } else {
                batch.delete_cf(&handle, key);
                counts.deletes += 1;
            }
        }
    }
    Ok(counts)
}

/// Background pressure writer statistics.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub(crate) struct PressureStats {
    pub(crate) lanes: u32,
    pub(crate) blocks: u64,
    pub(crate) batches: u64,
    pub(crate) bytes: u64,
    pub(crate) counts: MirrorCounts,
    pub(crate) write_seconds: f64,
    pub(crate) committer_blocked_seconds: f64,
}

/// Background writer that mirrors each committed block `lanes` times.
pub(crate) struct PressureWriter {
    sender: Option<SyncSender<MirrorWork>>,
    handle: Option<JoinHandle<Result<PressureStats, BenchError>>>,
    blocked: Duration,
}

impl PressureWriter {
    pub(crate) fn spawn(
        db: Arc<DB>,
        names: MirrorFamilyNames,
        first_lane: u8,
        lanes: u8,
        families: MirrorFamilies,
        queue_blocks: usize,
    ) -> Self {
        let (sender, receiver) = sync_channel::<MirrorWork>(queue_blocks);
        let handle = std::thread::spawn(move || {
            pressure_loop(&db, &names, &receiver, first_lane, lanes, families)
        });
        Self {
            sender: Some(sender),
            handle: Some(handle),
            blocked: Duration::ZERO,
        }
    }

    /// Queues one block; blocks the committer while the queue is full.
    pub(crate) fn submit(&mut self, work: MirrorWork) -> Result<(), BenchError> {
        let started = Instant::now();
        if let Some(sender) = self.sender.as_ref() {
            sender
                .send(work)
                .map_err(|_| BenchError::spike("pressure writer stopped"))?;
        }
        self.blocked += started.elapsed();
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<PressureStats, BenchError> {
        drop(self.sender.take());
        let mut stats = self
            .handle
            .take()
            .ok_or_else(|| BenchError::spike("pressure writer already joined"))?
            .join()
            .map_err(|_| BenchError::spike("pressure writer panicked"))??;
        stats.committer_blocked_seconds = self.blocked.as_secs_f64();
        Ok(stats)
    }
}

fn pressure_loop(
    db: &DB,
    names: &MirrorFamilyNames,
    receiver: &Receiver<MirrorWork>,
    first_lane: u8,
    lanes: u8,
    families: MirrorFamilies,
) -> Result<PressureStats, BenchError> {
    let mut stats = PressureStats {
        lanes: u32::from(lanes),
        ..PressureStats::default()
    };
    let mut write_options = WriteOptions::default();
    write_options.set_sync(false);
    write_options.disable_wal(false);
    while let Ok(work) = receiver.recv() {
        stats.blocks += 1;
        for lane in first_lane..first_lane.saturating_add(lanes) {
            let mut batch = WriteBatch::default();
            let counts = apply_lanes(
                db,
                names,
                &mut batch,
                &work,
                lane..=lane,
                families,
                false,
                None,
                None,
            )?;
            stats.counts.add(counts);
            if batch.is_empty() {
                continue;
            }
            stats.bytes += batch.size_in_bytes() as u64;
            let started = Instant::now();
            db.write_opt(&batch, &write_options)
                .map_err(|source| spike_error("pressure write", source))?;
            stats.write_seconds += started.elapsed().as_secs_f64();
            stats.batches += 1;
        }
    }
    Ok(stats)
}
