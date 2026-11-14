//! Periodic `RocksDB` property sampling for space, compaction, and stalls.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use rust_rocksdb::{DB, Options, statistics::Ticker};
use serde::Serialize;

use super::{
    stats::{process_memory, sst_files_on_disk},
    writer::Shared,
};

/// One database the sampler watches.
pub(crate) struct DbProbe<'db> {
    pub(crate) name: &'static str,
    pub(crate) db: &'db DB,
    pub(crate) dir: PathBuf,
    pub(crate) statistics: Arc<Options>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct DbSample {
    pub(crate) live_sst_bytes: u64,
    pub(crate) total_sst_bytes: u64,
    pub(crate) obsolete_sst_bytes: Option<u64>,
    pub(crate) disk_sst_bytes: u64,
    pub(crate) disk_sst_files: u64,
    pub(crate) live_files_bytes: Option<u64>,
    pub(crate) live_files_count: Option<u64>,
    pub(crate) pending_compaction_bytes: u64,
    pub(crate) max_cf_pending_compaction_bytes: u64,
    pub(crate) max_cf_pending_compaction_family: String,
    pub(crate) max_l0_files: u64,
    pub(crate) num_live_versions: u64,
    pub(crate) mem_table_bytes: u64,
    pub(crate) estimate_live_data_bytes: u64,
    pub(crate) num_snapshots: u64,
    pub(crate) oldest_snapshot_time: u64,
    pub(crate) oldest_snapshot_age_s: Option<u64>,
    pub(crate) is_write_stopped: u64,
    pub(crate) actual_delayed_write_rate: u64,
    pub(crate) running_compactions: u64,
    pub(crate) running_flushes: u64,
    pub(crate) background_errors: u64,
    pub(crate) stall_micros: u64,
    pub(crate) cf_total_stops: u64,
    pub(crate) cf_total_delays: u64,
    pub(crate) db_stall_counts: BTreeMap<String, u64>,
}

#[derive(Serialize)]
pub(crate) struct SampleRecord {
    pub(crate) t: f64,
    pub(crate) phase: String,
    pub(crate) canonical: DbSample,
    pub(crate) wallet: Option<DbSample>,
    pub(crate) rss_bytes: Option<u64>,
    pub(crate) peak_rss_bytes: Option<u64>,
    pub(crate) active_readers: u32,
    pub(crate) attach_stage: u32,
    pub(crate) source_revealed_height: u32,
    pub(crate) source_cached_blocks: usize,
}

/// Parses `Write Stall (count): key: value, key: value` property text.
pub(crate) fn parse_stall_counts(text: &str) -> BTreeMap<String, u64> {
    let body = text.split_once(':').map_or(text, |(_, rest)| rest).trim();
    body.split(',')
        .filter_map(|pair| {
            let (key, value) = pair.trim().rsplit_once(':')?;
            Some((key.trim().to_owned(), value.trim().parse().ok()?))
        })
        .collect()
}

/// Per-column-family stall counts, summed over all families.
pub(crate) fn cf_stall_counts(db: &DB) -> BTreeMap<String, u64> {
    let mut totals = BTreeMap::new();
    for name in db.cf_names() {
        let Some(family) = db.cf_handle(&name) else {
            continue;
        };
        if let Ok(Some(text)) = db.property_value_cf(&family, "rocksdb.cf-write-stall-stats") {
            for (key, value) in parse_stall_counts(&text) {
                *totals.entry(key).or_insert(0) += value;
            }
        }
    }
    totals
}

fn int(db: &DB, name: &str) -> u64 {
    db.property_int_value(name).ok().flatten().unwrap_or(0)
}

pub(crate) fn sample_db(probe: &DbProbe<'_>, include_live_files: bool) -> DbSample {
    let db = probe.db;
    let mut sample = DbSample::default();
    for name in db.cf_names() {
        let Some(family) = db.cf_handle(&name) else {
            continue;
        };
        let cf_int = |property: &str| {
            db.property_int_value_cf(&family, property)
                .ok()
                .flatten()
                .unwrap_or(0)
        };
        sample.live_sst_bytes += cf_int("rocksdb.live-sst-files-size");
        sample.total_sst_bytes += cf_int("rocksdb.total-sst-files-size");
        let pending = cf_int("rocksdb.estimate-pending-compaction-bytes");
        sample.pending_compaction_bytes += pending;
        if pending > sample.max_cf_pending_compaction_bytes {
            sample.max_cf_pending_compaction_bytes = pending;
            sample.max_cf_pending_compaction_family = name.clone();
        }
        sample.max_l0_files = sample
            .max_l0_files
            .max(cf_int("rocksdb.num-files-at-level0"));
        sample.num_live_versions += cf_int("rocksdb.num-live-versions");
        sample.mem_table_bytes += cf_int("rocksdb.cur-size-all-mem-tables");
        sample.estimate_live_data_bytes += cf_int("rocksdb.estimate-live-data-size");
        if let Ok(Some(text)) = db.property_value_cf(&family, "rocksdb.cf-write-stall-stats") {
            let counts = parse_stall_counts(&text);
            sample.cf_total_stops += counts.get("total-stops").copied().unwrap_or(0);
            sample.cf_total_delays += counts.get("total-delays").copied().unwrap_or(0);
        }
    }
    sample.obsolete_sst_bytes = db
        .property_int_value("rocksdb.obsolete-sst-files-size")
        .ok()
        .flatten();
    let (disk_bytes, disk_files) = sst_files_on_disk(&probe.dir);
    sample.disk_sst_bytes = disk_bytes;
    sample.disk_sst_files = disk_files;
    if include_live_files && let Ok(files) = db.live_files() {
        sample.live_files_count = Some(files.len() as u64);
        sample.live_files_bytes = Some(files.iter().map(|file| file.size as u64).sum());
    }
    sample.num_snapshots = int(db, "rocksdb.num-snapshots");
    sample.oldest_snapshot_time = int(db, "rocksdb.oldest-snapshot-time");
    if sample.oldest_snapshot_time > 0 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_secs());
        sample.oldest_snapshot_age_s = Some(now.saturating_sub(sample.oldest_snapshot_time));
    }
    sample.is_write_stopped = int(db, "rocksdb.is-write-stopped");
    sample.actual_delayed_write_rate = int(db, "rocksdb.actual-delayed-write-rate");
    sample.running_compactions = int(db, "rocksdb.num-running-compactions");
    sample.running_flushes = int(db, "rocksdb.num-running-flushes");
    sample.background_errors = int(db, "rocksdb.background-errors");
    sample.stall_micros = probe.statistics.get_ticker_count(Ticker::StallMicros);
    if let Ok(Some(text)) = db.property_value("rocksdb.db-write-stall-stats") {
        sample.db_stall_counts = parse_stall_counts(&text);
    }
    sample
}

pub(crate) fn take_sample(
    shared: &Shared<'_>,
    canonical: &DbProbe<'_>,
    wallet: Option<&DbProbe<'_>>,
    include_live_files: bool,
) -> SampleRecord {
    let memory = process_memory();
    SampleRecord {
        t: shared.clock.t(),
        phase: shared.clock.phase().to_owned(),
        canonical: sample_db(canonical, include_live_files),
        wallet: wallet.map(|probe| sample_db(probe, include_live_files)),
        rss_bytes: memory.rss_bytes,
        peak_rss_bytes: memory.peak_rss_bytes,
        active_readers: shared.active_readers.load(Ordering::SeqCst),
        attach_stage: shared.attach_stage.load(Ordering::SeqCst),
        source_revealed_height: shared.source.revealed_height(),
        source_cached_blocks: shared.source.cached_blocks(),
    }
}

/// Samples until `stop`; keeps every record for phase summaries.
pub(crate) fn run_sampler(
    shared: &Shared<'_>,
    canonical: &DbProbe<'_>,
    wallet: Option<&DbProbe<'_>>,
    interval: Duration,
    live_files_every: u32,
    stop: &AtomicBool,
    keep: &parking_lot::Mutex<Vec<SampleRecord>>,
) {
    let mut count = 0_u32;
    let mut next = Instant::now();
    while !stop.load(Ordering::SeqCst) {
        let include_live_files = live_files_every > 0 && count % live_files_every == 0;
        let record = take_sample(shared, canonical, wallet, include_live_files);
        shared.outputs.samples.write(&record);
        keep.lock().push(record);
        count = count.wrapping_add(1);
        next += interval;
        let now = Instant::now();
        if next > now {
            std::thread::sleep(next - now);
        } else {
            next = now;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_stall_counts;

    #[test]
    fn parses_rocksdb_stall_text() {
        let counts = parse_stall_counts(
            "Write Stall (count): total-delays: 3, total-stops: 0, memtable-limit-stops: 0\n",
        );
        assert_eq!(counts.get("total-delays"), Some(&3));
        assert_eq!(counts.get("total-stops"), Some(&0));
        assert_eq!(counts.len(), 3);
    }
}
