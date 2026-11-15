//! Arm 2: rebuild one wallet family from local canonical data, attach it by
//! SST ingest while the writer commits, catch it up, switch to it atomically,
//! and drop the old column family while readers still hold snapshots on it.

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

use rust_rocksdb::{
    DB, IngestExternalFileOptions, IteratorMode, Options, WriteBatch, WriteOptions,
};
use serde::Serialize;
use zinder_core::{BlockHeight, Network};
use zinder_store::{
    CanonicalReplayScan, CanonicalStoreError, CanonicalStoreReadyEvidence, SpikeSnapshot,
    spike_block_replay_at, spike_read_options, spike_replay_scan_at_snapshot,
};
use zinder_wallet_projection::WalletOutpointKey;
use zinder_wallet_rocksdb::{
    RocksDbWalletBuildOptions, WalletProjectionReplaySource, spike_set_family_alias,
    spike_wallet_ready_evidence_at, spike_write_projection_ssts,
};

use super::{
    observer::BALLAST_COLUMN_FAMILY,
    plan::IndexAttachSpec,
    readers::MANIFEST_COLUMN_FAMILY,
    stats::{process_memory, sst_files_on_disk},
    writer::{CanonicalSnapshotGrant, Shared, WriterRequest, WriterState},
};

const SPENT_FAMILY: &str = "transparent_spent_output";

struct SnapshotReplaySource<'db> {
    db: &'db DB,
    snapshot: SpikeSnapshot<'db>,
    ready: CanonicalStoreReadyEvidence,
    network: Network,
}

impl WalletProjectionReplaySource for SnapshotReplaySource<'_> {
    fn wallet_projection_network(&self) -> Network {
        self.network
    }

    fn wallet_projection_ready_evidence(&self) -> CanonicalStoreReadyEvidence {
        self.ready
    }

    fn wallet_projection_scan(&self) -> Result<CanonicalReplayScan<'_>, CanonicalStoreError> {
        spike_replay_scan_at_snapshot(self.db, &self.ready, &self.snapshot)
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct CatchUpRound {
    pub(crate) t: f64,
    pub(crate) from_height: u32,
    pub(crate) to_height: u32,
    pub(crate) rows: u64,
    pub(crate) seconds: f64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct IndexAttachRecord {
    pub(crate) family: String,
    pub(crate) old_column_family: String,
    pub(crate) new_column_family: String,
    pub(crate) snapshot_granted_t: f64,
    pub(crate) snapshot_tip: u32,
    pub(crate) sst_build_started_t: f64,
    pub(crate) sst_build_seconds: f64,
    pub(crate) sst_scanned_blocks: u64,
    pub(crate) sst_scanned_transactions: u64,
    pub(crate) sst_historical_prevout_reads: u64,
    pub(crate) sst_all_families_file_bytes: u64,
    pub(crate) sst_all_families_file_count: u64,
    pub(crate) family_sst_files: usize,
    pub(crate) family_sst_bytes: u64,
    pub(crate) family_rows_at_build: u64,
    pub(crate) peak_rss_after_build_bytes: Option<u64>,
    pub(crate) create_cf_seconds: f64,
    pub(crate) ingest_started_t: f64,
    pub(crate) ingest_seconds: f64,
    pub(crate) ingest_ended_t: f64,
    pub(crate) catch_up_rounds: Vec<CatchUpRound>,
    pub(crate) catch_up_blocks_after_ingest: u32,
    pub(crate) final_switch_t: f64,
    pub(crate) final_switch_writer_blocked_seconds: f64,
    pub(crate) final_switch_delta_blocks: u32,
    pub(crate) final_switch_rows: u64,
    pub(crate) final_tip: u32,
    pub(crate) verification_rows_old: u64,
    pub(crate) verification_rows_new: u64,
    pub(crate) verification_mismatches: u64,
    pub(crate) verification_seconds: f64,
    pub(crate) rebuilt_family_matches_live: bool,
    pub(crate) old_family_bytes_before_drop: u64,
    pub(crate) old_family_files_before_drop: usize,
    pub(crate) wallet_disk_sst_bytes_before_drop: u64,
    pub(crate) drop_t: f64,
    pub(crate) drop_seconds: f64,
    pub(crate) wallet_disk_sst_bytes_after_drop: u64,
    /// Old-family SST files still on disk right after `drop_cf`.
    pub(crate) old_family_files_on_disk_after_drop: usize,
    /// Seconds after `drop_cf` until every old-family SST file was deleted, if observed.
    pub(crate) old_family_files_deleted_after_s: Option<f64>,
    pub(crate) wallet_disk_sst_bytes_after_old_files_deleted: Option<u64>,
    /// Run-clock time when every old-family SST file was gone, if observed.
    pub(crate) old_family_files_deleted_t: Option<f64>,
    /// Synced-write probe of the wallet database around the ingest call.
    pub(crate) ingest_probe: Option<IngestProbe>,
    /// Filled after the phase from reader cycles that held the old family at the drop.
    pub(crate) old_family_holders_at_drop: Vec<OldFamilyHolder>,
    pub(crate) last_old_family_holder_release_t: Option<f64>,
    /// Seconds from the last holder's release to the old files leaving disk.
    pub(crate) files_deleted_after_last_holder_release_s: Option<f64>,
    pub(crate) error: Option<String>,
}

/// Latency of tiny unsynced writes to the same wallet database around the ingest.
#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct IngestProbeWindow {
    pub(crate) writes: usize,
    pub(crate) p50_s: f64,
    pub(crate) p99_s: f64,
    pub(crate) max_s: f64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct IngestProbe {
    /// Writes that finished before the ingest call started.
    pub(crate) before: IngestProbeWindow,
    /// Writes whose interval overlapped the ingest call.
    pub(crate) overlapping: IngestProbeWindow,
    /// Writes that started after the ingest call returned.
    pub(crate) after: IngestProbeWindow,
    /// Longest single probe write that overlapped the ingest call.
    pub(crate) max_overlapping_write_s: f64,
}

/// A reader that held the old family when it was dropped.
#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct OldFamilyHolder {
    pub(crate) reader: String,
    pub(crate) t_start: f64,
    pub(crate) t_release: f64,
    pub(crate) violation_count: u64,
    pub(crate) error: Option<String>,
    pub(crate) family_dropped_before_release: Option<bool>,
}

fn probe_window(values: &[f64]) -> IngestProbeWindow {
    let summary = super::stats::LatencySummary::from_values(values);
    IngestProbeWindow {
        writes: summary.count,
        p50_s: summary.p50,
        p99_s: summary.p99,
        max_s: summary.max,
    }
}

/// Runs `ingest` while a probe thread issues tiny synced writes to `db`, one
/// second before and after it, and classifies each probe write by overlap.
fn probe_ingest<T>(db: &DB, ingest: impl FnOnce() -> T) -> (T, IngestProbe) {
    let stop = AtomicBool::new(false);
    let origin = Instant::now();
    std::thread::scope(|scope| {
        let probe = scope.spawn(|| {
            let mut writes = Vec::new();
            let Some(family) = db.cf_handle(BALLAST_COLUMN_FAMILY) else {
                return writes;
            };
            // Unsynced and paced: a synced tight loop would itself slow the writer's
            // WAL syncs. A write blocked by the ingest still waits for the write thread.
            let mut options = WriteOptions::default();
            options.set_sync(false);
            let mut counter = 0_u64;
            while !stop.load(Ordering::SeqCst) {
                counter += 1;
                let mut key = b"probe/".to_vec();
                key.extend_from_slice(&counter.to_be_bytes());
                let started = origin.elapsed().as_secs_f64();
                let mut batch = WriteBatch::default();
                batch.put_cf(&family, &key, b"p");
                let _ = db.write_opt(&batch, &options);
                writes.push((started, origin.elapsed().as_secs_f64()));
                std::thread::sleep(Duration::from_micros(500));
            }
            writes
        });
        std::thread::sleep(Duration::from_secs(1));
        let ingest_started = origin.elapsed().as_secs_f64();
        let output = ingest();
        let ingest_ended = origin.elapsed().as_secs_f64();
        std::thread::sleep(Duration::from_secs(1));
        stop.store(true, Ordering::SeqCst);
        let writes = probe.join().unwrap_or_default();
        let latency = |(start, end): &(f64, f64)| end - start;
        let before = writes
            .iter()
            .filter(|(_, end)| *end <= ingest_started)
            .map(latency)
            .collect::<Vec<_>>();
        let overlapping = writes
            .iter()
            .filter(|(start, end)| *start < ingest_ended && *end > ingest_started)
            .map(latency)
            .collect::<Vec<_>>();
        let after = writes
            .iter()
            .filter(|(start, _)| *start >= ingest_ended)
            .map(latency)
            .collect::<Vec<_>>();
        let probe = IngestProbe {
            before: probe_window(&before),
            max_overlapping_write_s: overlapping.iter().copied().fold(0.0, f64::max),
            overlapping: probe_window(&overlapping),
            after: probe_window(&after),
        };
        (output, probe)
    })
}

pub(crate) struct AttachContext<'run> {
    pub(crate) wallet_cf_options: &'run Options,
    pub(crate) build_options: RocksDbWalletBuildOptions,
    pub(crate) attach_index: u32,
}

/// Spent outpoint keys created by the transparent inputs of one canonical block.
fn spent_keys_at(db: &DB, height: u32) -> Result<Vec<WalletOutpointKey>, String> {
    let replay = spike_block_replay_at(db, None, BlockHeight::new(height))
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("canonical replay at {height} is absent"))?;
    let mut keys = Vec::new();
    for transaction in &replay.facts().transactions {
        for input in &transaction.transparent_inputs {
            if !input.spent_outpoint.is_coinbase_sentinel() {
                keys.push(WalletOutpointKey::new(input.spent_outpoint));
            }
        }
    }
    Ok(keys)
}

/// Copies the spent rows of `from..=to` from the live family into `batch`.
fn copy_delta(
    canonical_db: &DB,
    wallet_db: &DB,
    snapshot: Option<&SpikeSnapshot<'_>>,
    old_family: &str,
    new_family: &str,
    from: u32,
    to: u32,
    batch: &mut WriteBatch,
) -> Result<u64, String> {
    let old = wallet_db
        .cf_handle(old_family)
        .ok_or_else(|| format!("{old_family} is unavailable"))?;
    let new = wallet_db
        .cf_handle(new_family)
        .ok_or_else(|| format!("{new_family} is unavailable"))?;
    let read_options = spike_read_options(snapshot, true);
    let mut rows = 0;
    for height in from..=to {
        for key in spent_keys_at(canonical_db, height)? {
            let value = wallet_db
                .get_cf_opt(&old, key.as_bytes(), &read_options)
                .map_err(|error| error.to_string())?
                .ok_or_else(|| format!("spent row for a block-{height} input is absent"))?;
            batch.put_cf(&new, key.as_bytes(), value);
            rows += 1;
        }
    }
    Ok(rows)
}

struct FinalSwitch<'db> {
    blocked_seconds: f64,
    delta_blocks: u32,
    rows: u64,
    final_tip: u32,
    snapshot: SpikeSnapshot<'db>,
    error: Option<String>,
}

/// Runs the whole attach sequence; never panics the phase on failure.
pub(crate) fn run_index_attach<'db>(
    shared: &Shared<'db>,
    spec: &IndexAttachSpec,
    context: &AttachContext<'_>,
    requests: &mpsc::Sender<WriterRequest<'db>>,
    phase_started: Instant,
) -> IndexAttachRecord {
    let mut record = IndexAttachRecord {
        family: spec.family.clone(),
        ..IndexAttachRecord::default()
    };
    let wait = Duration::from_secs_f64(spec.at_seconds).saturating_sub(phase_started.elapsed());
    std::thread::sleep(wait);
    if let Err(error) = attach_body(shared, spec, context, requests, &mut record) {
        record.error = Some(error);
    }
    shared.attach_stage.store(0, Ordering::SeqCst);
    shared.clock.event(
        shared.outputs,
        "index_attach_finished",
        serde_json::to_value(&record).unwrap_or_default(),
    );
    record
}

#[allow(clippy::too_many_lines, reason = "spike sequence kept in one place")]
fn attach_body<'db>(
    shared: &Shared<'db>,
    spec: &IndexAttachSpec,
    context: &AttachContext<'_>,
    requests: &mpsc::Sender<WriterRequest<'db>>,
    record: &mut IndexAttachRecord,
) -> Result<(), String> {
    let wallet_db = shared
        .wallet_db
        .ok_or("index attach needs a wallet store")?;
    let wallet_dir = shared
        .wallet_dir
        .clone()
        .ok_or("index attach needs a wallet store")?;
    let old_family = zinder_wallet_rocksdb::spike_resolve_family(SPENT_FAMILY);
    let new_family = format!("{SPENT_FAMILY}__rebuild_{}", context.attach_index);
    record.old_column_family = old_family.clone();
    record.new_column_family = new_family.clone();

    // 1. A canonical snapshot granted by the writer between commits.
    let (reply, granted) = mpsc::channel::<CanonicalSnapshotGrant<'db>>();
    requests
        .send(WriterRequest::CanonicalSnapshot(reply))
        .map_err(|_| "writer stopped before granting a snapshot")?;
    let grant = granted
        .recv()
        .map_err(|_| "writer stopped before granting a snapshot")?;
    record.snapshot_granted_t = shared.clock.t();
    record.snapshot_tip = grant.ready.visible_tip.height.value();

    // 2. Real projection SSTs from that snapshot, while the writer commits.
    shared.attach_stage.store(1, Ordering::SeqCst);
    let staging = shared
        .staging_dir
        .join(format!("attach-{}", context.attach_index));
    std::fs::create_dir_all(staging.join("projection")).map_err(|error| error.to_string())?;
    let source = SnapshotReplaySource {
        db: shared.canonical_db,
        snapshot: grant.snapshot,
        ready: grant.ready,
        network: grant.network,
    };
    record.sst_build_started_t = shared.clock.t();
    let started = Instant::now();
    let built = spike_write_projection_ssts(
        &source,
        &staging.join("projection"),
        context.build_options,
        context.wallet_cf_options,
    )
    .map_err(|error| format!("projection SST build failed: {error}"))?;
    drop(source);
    record.sst_build_seconds = started.elapsed().as_secs_f64();
    record.peak_rss_after_build_bytes = process_memory().peak_rss_bytes;
    record.sst_scanned_blocks = built.scanned_block_count;
    record.sst_scanned_transactions = built.scanned_transaction_count;
    record.sst_historical_prevout_reads = built.historical_prevout_read_count;
    record.sst_all_families_file_bytes = built.sst_file_bytes;
    record.sst_all_families_file_count = built.sst_file_count;
    record.family_rows_at_build = built.row_counts.transparent_spent_output_count;
    let built_tip = built.tip.height.value();
    let paths = built
        .families
        .iter()
        .find(|(name, _)| *name == SPENT_FAMILY)
        .map(|(_, paths)| paths.clone())
        .unwrap_or_default();
    record.family_sst_files = paths.len();
    record.family_sst_bytes = paths
        .iter()
        .filter_map(|path| std::fs::metadata(path).ok())
        .map(|metadata| metadata.len())
        .sum();

    // 3. A new column family with production options, then external ingest.
    shared.attach_stage.store(2, Ordering::SeqCst);
    let started = Instant::now();
    wallet_db
        .create_cf(&new_family, context.wallet_cf_options)
        .map_err(|error| format!("create_cf failed: {error}"))?;
    record.create_cf_seconds = started.elapsed().as_secs_f64();
    let new_handle = wallet_db
        .cf_handle(&new_family)
        .ok_or("new column family vanished")?;
    let mut ingest = IngestExternalFileOptions::default();
    ingest.set_move_files(true);
    ingest.set_snapshot_consistency(true);
    ingest.set_allow_global_seqno(true);
    ingest.set_allow_blocking_flush(false);
    let mut ingest_timing = (0.0, 0.0, 0.0);
    let (ingested, probe) = probe_ingest(wallet_db, || {
        let started_t = shared.clock.t();
        let started = Instant::now();
        let outcome = if paths.is_empty() {
            Ok(())
        } else {
            wallet_db
                .ingest_external_file_cf_opts(&new_handle, &ingest, paths)
                .map_err(|error| format!("ingest failed: {error}"))
        };
        ingest_timing = (started_t, started.elapsed().as_secs_f64(), shared.clock.t());
        outcome
    });
    record.ingest_started_t = ingest_timing.0;
    record.ingest_seconds = ingest_timing.1;
    record.ingest_ended_t = ingest_timing.2;
    record.ingest_probe = Some(probe);
    ingested?;
    drop(new_handle);
    let _ = std::fs::remove_dir_all(&staging);

    // 4. Concurrent catch-up from the build tip toward the live wallet tip.
    shared.attach_stage.store(3, Ordering::SeqCst);
    let mut caught_up = built_tip;
    let tip_after_ingest = spike_wallet_ready_evidence_at(wallet_db, None)
        .map_err(|error| error.to_string())?
        .source_position
        .tip
        .height
        .value();
    record.catch_up_blocks_after_ingest = tip_after_ingest.saturating_sub(built_tip);
    loop {
        let snapshot = wallet_db.snapshot();
        let tip = spike_wallet_ready_evidence_at(wallet_db, Some(&snapshot))
            .map_err(|error| error.to_string())?
            .source_position
            .tip
            .height
            .value();
        if tip.saturating_sub(caught_up) <= spec.final_gap_blocks {
            break;
        }
        let started = Instant::now();
        let mut batch = WriteBatch::default();
        let rows = copy_delta(
            shared.canonical_db,
            wallet_db,
            Some(&snapshot),
            &old_family,
            &new_family,
            caught_up + 1,
            tip,
            &mut batch,
        )?;
        wallet_db
            .write(&batch)
            .map_err(|error| format!("catch-up write failed: {error}"))?;
        record.catch_up_rounds.push(CatchUpRound {
            t: shared.clock.t(),
            from_height: caught_up + 1,
            to_height: tip,
            rows,
            seconds: started.elapsed().as_secs_f64(),
        });
        caught_up = tip;
    }

    // 5. Final delta, manifest key, and alias switch, run by the writer between commits.
    shared.attach_stage.store(4, Ordering::SeqCst);
    let (reply, switched) = mpsc::channel::<FinalSwitch<'db>>();
    let canonical_db = shared.canonical_db;
    let old_for_switch = old_family.clone();
    let new_for_switch = new_family.clone();
    requests
        .send(WriterRequest::Run(Box::new(
            move |state: &mut WriterState| {
                let started = Instant::now();
                let outcome = (|| -> Result<(u32, u32, u64), String> {
                    let wallet = state.wallet.as_ref().ok_or("wallet stage is detached")?;
                    let tip = wallet.ready_evidence().source_position.tip.height.value();
                    let mut batch = WriteBatch::default();
                    let rows = if tip > caught_up {
                        copy_delta(
                            canonical_db,
                            wallet_db,
                            None,
                            &old_for_switch,
                            &new_for_switch,
                            caught_up + 1,
                            tip,
                            &mut batch,
                        )?
                    } else {
                        0
                    };
                    let manifest = wallet_db
                        .cf_handle(MANIFEST_COLUMN_FAMILY)
                        .ok_or("manifest column family is absent")?;
                    batch.put_cf(
                        &manifest,
                        SPENT_FAMILY.as_bytes(),
                        new_for_switch.as_bytes(),
                    );
                    let mut write_options = WriteOptions::default();
                    write_options.set_sync(true);
                    wallet_db
                        .write_opt(&batch, &write_options)
                        .map_err(|error| format!("switch write failed: {error}"))?;
                    spike_set_family_alias(SPENT_FAMILY, Some(new_for_switch.clone()));
                    Ok((tip, tip.saturating_sub(caught_up), rows))
                })();
                let snapshot = wallet_db.snapshot();
                let blocked_seconds = started.elapsed().as_secs_f64();
                let switch = match outcome {
                    Ok((final_tip, delta_blocks, rows)) => FinalSwitch {
                        blocked_seconds,
                        delta_blocks,
                        rows,
                        final_tip,
                        snapshot,
                        error: None,
                    },
                    Err(error) => FinalSwitch {
                        blocked_seconds,
                        delta_blocks: 0,
                        rows: 0,
                        final_tip: 0,
                        snapshot,
                        error: Some(error),
                    },
                };
                let _ = reply.send(switch);
            },
        )))
        .map_err(|_| "writer stopped before the switch")?;
    let switch = switched
        .recv()
        .map_err(|_| "writer stopped before the switch")?;
    record.final_switch_t = shared.clock.t();
    record.final_switch_writer_blocked_seconds = switch.blocked_seconds;
    record.final_switch_delta_blocks = switch.delta_blocks;
    record.final_switch_rows = switch.rows;
    record.final_tip = switch.final_tip;
    if let Some(error) = switch.error {
        return Err(error);
    }
    shared.attach_stage.store(6, Ordering::SeqCst);

    // 6. Verify old and new families are identical at the switch point.
    let started = Instant::now();
    let (old_rows, new_rows, mismatches) =
        compare_families(wallet_db, &switch.snapshot, &old_family, &new_family)?;
    drop(switch.snapshot);
    record.verification_rows_old = old_rows;
    record.verification_rows_new = new_rows;
    record.verification_mismatches = mismatches;
    record.verification_seconds = started.elapsed().as_secs_f64();
    record.rebuilt_family_matches_live = old_rows == new_rows && mismatches == 0;

    // 7. Drop the old family while readers may still hold snapshots on it.
    std::thread::sleep(Duration::from_secs_f64(spec.drop_after_seconds));
    shared.attach_stage.store(5, Ordering::SeqCst);
    if let Some(old_handle) = wallet_db.cf_handle(&old_family) {
        let metadata = wallet_db.get_column_family_metadata_cf(&old_handle);
        record.old_family_bytes_before_drop = metadata.size;
        record.old_family_files_before_drop = metadata.file_count;
    }
    let old_files = wallet_db
        .live_files()
        .map(|files| {
            files
                .into_iter()
                .filter(|file| file.column_family_name == old_family)
                .map(|file| wallet_dir.join(file.name.trim_start_matches('/')))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    record.wallet_disk_sst_bytes_before_drop = sst_files_on_disk(&wallet_dir).0;
    record.drop_t = shared.clock.t();
    let started = Instant::now();
    wallet_db
        .drop_cf(&old_family)
        .map_err(|error| format!("drop_cf failed: {error}"))?;
    record.drop_seconds = started.elapsed().as_secs_f64();
    record.old_family_files_on_disk_after_drop =
        old_files.iter().filter(|path| path.exists()).count();
    record.wallet_disk_sst_bytes_after_drop = sst_files_on_disk(&wallet_dir).0;
    // Readers holding the old handle keep its files; wait for their release.
    let wait_started = Instant::now();
    let wait_limit = Duration::from_secs_f64(spec.wait_for_old_files_seconds);
    while wait_started.elapsed() < wait_limit {
        if old_files.iter().all(|path| !path.exists()) {
            record.old_family_files_deleted_after_s = Some(wait_started.elapsed().as_secs_f64());
            record.old_family_files_deleted_t = Some(shared.clock.t());
            record.wallet_disk_sst_bytes_after_old_files_deleted =
                Some(sst_files_on_disk(&wallet_dir).0);
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    Ok(())
}

fn compare_families(
    db: &DB,
    snapshot: &SpikeSnapshot<'_>,
    old_family: &str,
    new_family: &str,
) -> Result<(u64, u64, u64), String> {
    let old = db.cf_handle(old_family).ok_or("old family unavailable")?;
    let new = db.cf_handle(new_family).ok_or("new family unavailable")?;
    let mut old_rows = db.iterator_cf_opt(
        &old,
        spike_read_options(Some(snapshot), false),
        IteratorMode::Start,
    );
    let mut new_rows = db.iterator_cf_opt(
        &new,
        spike_read_options(Some(snapshot), false),
        IteratorMode::Start,
    );
    let (mut old_count, mut new_count, mut mismatches) = (0, 0, 0);
    loop {
        let left = old_rows
            .next()
            .transpose()
            .map_err(|error| error.to_string())?;
        let right = new_rows
            .next()
            .transpose()
            .map_err(|error| error.to_string())?;
        match (left, right) {
            (None, None) => break,
            (left, right) => {
                old_count += u64::from(left.is_some());
                new_count += u64::from(right.is_some());
                let digest = |row: &Option<(Box<[u8]>, Box<[u8]>)>| {
                    let mut hasher = DefaultHasher::new();
                    row.hash(&mut hasher);
                    hasher.finish()
                };
                if digest(&left) != digest(&right) {
                    mismatches += 1;
                }
            }
        }
    }
    Ok((old_count, new_count, mismatches))
}
