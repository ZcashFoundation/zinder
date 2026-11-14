//! Snapshot-holding readers and the mixed serving load.
//!
//! Every reader takes one primary snapshot, reads through it with the real
//! row codecs, and checks invariants that a consistent view must satisfy:
//!
//! - compact range: every height present, header and compact row agree, each
//!   parent hash links to the previous row, the last row is the epoch's
//!   visible tip, no row exists above that tip, and the epoch, event head,
//!   control record, and displaced-archive count are unchanged at the end;
//! - wallet family: the scanned row count equals the READY control's count
//!   for that family (and UTXO count and value equal the UTXO summary), and
//!   the READY evidence is unchanged at the end.
//!
//! A reader with `snapshot: false` runs the same checks against the live
//! database, so it is the negative control that shows the checks can fail.

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use rust_rocksdb::{DB, IteratorMode};
use serde::Serialize;
use zinder_core::{BlockHeight, BlockHeightRange, TransactionId, TransparentAddressScriptHash};
use zinder_store::{
    SpikeCompactBlockCursor, SpikeCompactBlockRead, SpikeSnapshot, spike_canonical_epoch_tip,
    spike_canonical_view_fingerprint, spike_compact_block_exists, spike_compact_blocks,
    spike_read_options, spike_transaction_lookup,
};
use zinder_wallet_projection::{
    WalletAddressBalance, WalletAddressTransaction, WalletAddressTransactionKey,
    WalletAddressUnspentOutputKey, WalletOutpointKey, WalletProjectionReadyEvidence,
    WalletReorgUndo, WalletSpentOutput, WalletUnspentOutput,
};
use zinder_wallet_rocksdb::spike_wallet_ready_evidence_at;

use super::{plan::LoadSpec, stats::LatencySummary, writer::Shared};

pub(crate) const MANIFEST_COLUMN_FAMILY: &str = "spike_serving_manifest";
const MAX_RECORDED_VIOLATIONS: usize = 20;

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct ReaderCycleRecord {
    pub(crate) phase: String,
    pub(crate) reader: String,
    pub(crate) kind: &'static str,
    pub(crate) snapshot: bool,
    pub(crate) held_iterator: bool,
    pub(crate) t_start: f64,
    pub(crate) t_end: f64,
    pub(crate) snapshot_sequence: Option<u64>,
    pub(crate) hold_target_s: f64,
    pub(crate) hold_actual_s: f64,
    pub(crate) hold_truncated: bool,
    pub(crate) pass1_seconds: f64,
    pub(crate) pass1_rows: u64,
    pub(crate) pass1_bytes: u64,
    pub(crate) pass1_rows_per_s: f64,
    pub(crate) pass1_mib_per_s: f64,
    pub(crate) paced_reads: u64,
    pub(crate) paced_rows: u64,
    pub(crate) range_start: Option<u32>,
    pub(crate) range_end: Option<u32>,
    pub(crate) family: Option<String>,
    pub(crate) physical_family: Option<String>,
    pub(crate) expected_rows: Option<u64>,
    pub(crate) family_dropped_before_release: Option<bool>,
    pub(crate) violation_count: u64,
    pub(crate) violations: Vec<String>,
    pub(crate) error: Option<String>,
}

impl ReaderCycleRecord {
    fn violate(&mut self, message: String) {
        self.violation_count += 1;
        if self.violations.len() < MAX_RECORDED_VIOLATIONS {
            self.violations.push(message);
        }
    }

    fn finish(&mut self, started: Instant, shared: &Shared<'_>) {
        self.t_end = shared.clock.t();
        self.hold_actual_s = started.elapsed().as_secs_f64();
        if self.pass1_seconds > 0.0 {
            self.pass1_rows_per_s = self.pass1_rows as f64 / self.pass1_seconds;
            self.pass1_mib_per_s = self.pass1_bytes as f64 / self.pass1_seconds / 1_048_576.0;
        }
    }
}

/// Parameters of one compact-range reader.
#[derive(Clone, Copy)]
pub(crate) struct CompactReaderParams {
    pub(crate) hold: Duration,
    pub(crate) range_blocks: u32,
    pub(crate) chunk_blocks: u32,
    pub(crate) held_iterator: bool,
    pub(crate) snapshot: bool,
    pub(crate) paced_interval: Duration,
}

/// Parameters of one wallet-family reader.
#[derive(Clone)]
pub(crate) struct WalletReaderParams {
    pub(crate) hold: Duration,
    pub(crate) family: String,
    pub(crate) snapshot: bool,
    pub(crate) paced_interval: Duration,
}

/// Runs hold cycles until `stop`, starting a cycle only if it fits before `deadline`.
pub(crate) fn run_reader_loop(
    shared: &Shared<'_>,
    name: String,
    stop: &AtomicBool,
    deadline: Option<Instant>,
    hold: Duration,
    mut cycle: impl FnMut(&str) -> ReaderCycleRecord,
) {
    let mut first = true;
    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        if !first && deadline.is_some_and(|deadline| Instant::now() + hold > deadline) {
            break;
        }
        first = false;
        shared.active_readers.fetch_add(1, Ordering::SeqCst);
        let record = cycle(&name);
        shared.active_readers.fetch_sub(1, Ordering::SeqCst);
        shared.outputs.reader_cycles.write(&record);
        if record.violation_count > 0 || record.error.is_some() {
            tracing::warn!(
                target: "zinder::bench::spike",
                reader = %name,
                snapshot = record.snapshot,
                violations = record.violation_count,
                error = ?record.error,
                "reader cycle observed violations"
            );
        }
        if record.error.is_some() {
            std::thread::sleep(Duration::from_millis(500));
        }
    }
}

fn chunk_hash(rows: &[(u32, u64, [u8; 32])]) -> u64 {
    let mut hasher = DefaultHasher::new();
    rows.hash(&mut hasher);
    hasher.finish()
}

/// Reads one chunk; returns (chunk identity rows, bytes).
fn read_compact_chunk(
    shared: &Shared<'_>,
    snapshot: Option<&SpikeSnapshot<'_>>,
    cursor: Option<&mut SpikeCompactBlockCursor<'_>>,
    range: BlockHeightRange,
    previous_hash: &mut Option<[u8; 32]>,
    check_links: bool,
    record: &mut ReaderCycleRecord,
) -> Result<(Vec<(u32, u64, [u8; 32])>, u64), String> {
    let reads = match cursor {
        Some(cursor) => cursor.read_range(range),
        None => spike_compact_blocks(shared.canonical_db, snapshot, range),
    }
    .map_err(|error| error.to_string())?;
    let mut rows = Vec::with_capacity(reads.len());
    let mut bytes = 0;
    for read in reads {
        match read {
            SpikeCompactBlockRead::Present(row) => {
                if check_links
                    && let Some(previous) = previous_hash
                    && row.parent_hash != *previous
                {
                    record.violate(format!(
                        "height {} parent does not link to the previous row",
                        row.height
                    ));
                }
                *previous_hash = Some(row.block_hash);
                bytes += row.encoded_bytes as u64;
                rows.push((row.height, row.value_hash, row.block_hash));
            }
            SpikeCompactBlockRead::CompactAbsent(height) => {
                record.violate(format!("compact row at {height} absent inside the view"));
            }
            SpikeCompactBlockRead::HeaderAbsent(height) => {
                record.violate(format!("header row at {height} absent inside the view"));
            }
            SpikeCompactBlockRead::HeaderMismatch(height) => {
                record.violate(format!("header and compact row disagree at {height}"));
            }
        }
    }
    Ok((rows, bytes))
}

/// One compact-block range stream held through one snapshot.
pub(crate) fn compact_range_cycle(
    shared: &Shared<'_>,
    name: &str,
    params: CompactReaderParams,
    stop: &AtomicBool,
) -> ReaderCycleRecord {
    let started = Instant::now();
    let mut record = ReaderCycleRecord {
        phase: shared.clock.phase().to_owned(),
        reader: name.to_owned(),
        kind: "compact-range",
        snapshot: params.snapshot,
        held_iterator: params.held_iterator,
        t_start: shared.clock.t(),
        hold_target_s: params.hold.as_secs_f64(),
        ..ReaderCycleRecord::default()
    };
    let snapshot = params.snapshot.then(|| shared.canonical_db.snapshot());
    record.snapshot_sequence = snapshot
        .as_ref()
        .map(rust_rocksdb::SnapshotWithThreadMode::sequence_number);
    if let Err(error) = compact_range_body(
        shared,
        snapshot.as_ref(),
        params,
        stop,
        &mut record,
        started,
    ) {
        record.error = Some(error);
    }
    drop(snapshot);
    record.finish(started, shared);
    record
}

fn compact_range_body(
    shared: &Shared<'_>,
    snapshot: Option<&SpikeSnapshot<'_>>,
    params: CompactReaderParams,
    stop: &AtomicBool,
    record: &mut ReaderCycleRecord,
    started: Instant,
) -> Result<(), String> {
    let db = shared.canonical_db;
    let fingerprint_start = spike_canonical_view_fingerprint(db, shared.network, snapshot)
        .map_err(|error| error.to_string())?;
    let tip = fingerprint_start.visible_tip_height;
    let start = tip
        .saturating_sub(params.range_blocks.saturating_sub(1))
        .max(shared.first_retained_height.max(1));
    record.range_start = Some(start);
    record.range_end = Some(tip);
    let mut cursor = if params.held_iterator {
        Some(SpikeCompactBlockCursor::open(db, snapshot).map_err(|error| error.to_string())?)
    } else {
        None
    };
    let chunk_ranges = (start..=tip)
        .step_by(params.chunk_blocks as usize)
        .map(|chunk_start| {
            BlockHeightRange::inclusive(
                BlockHeight::new(chunk_start),
                BlockHeight::new((chunk_start + params.chunk_blocks - 1).min(tip)),
            )
        })
        .collect::<Vec<_>>();

    let pass_started = Instant::now();
    let mut previous_hash = None;
    let mut chunk_hashes = Vec::with_capacity(chunk_ranges.len());
    for range in &chunk_ranges {
        let (rows, bytes) = read_compact_chunk(
            shared,
            snapshot,
            cursor.as_mut(),
            *range,
            &mut previous_hash,
            true,
            record,
        )?;
        record.pass1_rows += rows.len() as u64;
        record.pass1_bytes += bytes;
        chunk_hashes.push(chunk_hash(&rows));
    }
    record.pass1_seconds = pass_started.elapsed().as_secs_f64();
    if previous_hash != Some(fingerprint_start.visible_tip_hash) {
        record.violate("last row is not the epoch's visible tip".to_owned());
    }
    if spike_compact_block_exists(db, snapshot, tip + 1).map_err(|error| error.to_string())? {
        record.violate(format!(
            "a compact row above the epoch tip {tip} is visible"
        ));
    }

    let mut index = 0;
    while started.elapsed() < params.hold && !chunk_ranges.is_empty() {
        if stop.load(Ordering::SeqCst) {
            record.hold_truncated = true;
            break;
        }
        let remaining = params.hold.saturating_sub(started.elapsed());
        std::thread::sleep(params.paced_interval.min(remaining));
        if started.elapsed() >= params.hold {
            break;
        }
        let chunk = index % chunk_ranges.len();
        let mut chunk_previous = None;
        let (rows, _) = read_compact_chunk(
            shared,
            snapshot,
            cursor.as_mut(),
            chunk_ranges[chunk],
            &mut chunk_previous,
            true,
            record,
        )?;
        if chunk_hash(&rows) != chunk_hashes[chunk] {
            record.violate(format!(
                "chunk {}..={} changed during the hold",
                chunk_ranges[chunk].start.value(),
                chunk_ranges[chunk].end.value()
            ));
        }
        record.paced_reads += 1;
        record.paced_rows += rows.len() as u64;
        index += 1;
    }
    let fingerprint_end = spike_canonical_view_fingerprint(db, shared.network, snapshot)
        .map_err(|error| error.to_string())?;
    if fingerprint_end != fingerprint_start {
        record.violate(format!(
            "view fingerprint changed: epoch {}->{}, tip {}->{}, event {}->{}, displaced {}->{}",
            fingerprint_start.epoch_id,
            fingerprint_end.epoch_id,
            fingerprint_start.visible_tip_height,
            fingerprint_end.visible_tip_height,
            fingerprint_start.latest_event_sequence,
            fingerprint_end.latest_event_sequence,
            fingerprint_start.displaced_archive_rows,
            fingerprint_end.displaced_archive_rows
        ));
    }
    if spike_compact_block_exists(db, snapshot, tip + 1).map_err(|error| error.to_string())? {
        record.violate(format!(
            "a compact row above the epoch tip {tip} is visible at release"
        ));
    }
    drop(cursor);
    Ok(())
}

/// Resolves the physical column family serving a logical wallet family in one view.
pub(crate) fn resolve_physical_family(
    db: &DB,
    snapshot: Option<&SpikeSnapshot<'_>>,
    logical: &str,
) -> Result<String, String> {
    let Some(manifest) = db.cf_handle(MANIFEST_COLUMN_FAMILY) else {
        return Ok(logical.to_owned());
    };
    let value = db
        .get_cf_opt(
            &manifest,
            logical.as_bytes(),
            &spike_read_options(snapshot, true),
        )
        .map_err(|error| error.to_string())?;
    Ok(value.map_or_else(
        || logical.to_owned(),
        |bytes| String::from_utf8_lossy(&bytes).into_owned(),
    ))
}

fn expected_family_rows(evidence: &WalletProjectionReadyEvidence, logical: &str) -> Option<u64> {
    let counts = evidence.row_counts;
    Some(match logical {
        "transparent_unspent_output" => counts.transparent_unspent_output_count,
        "transparent_unspent_output_by_address" => {
            counts.transparent_unspent_output_by_address_count
        }
        "transparent_spent_output" => counts.transparent_spent_output_count,
        "transparent_address_transaction" => counts.transparent_address_transaction_count,
        "transparent_address_balance" => counts.transparent_address_balance_count,
        "reorg_undo" => counts.reorg_undo_count,
        _ => return None,
    })
}

/// Decodes one wallet row with the real codec; returns the unspent value if any.
pub(crate) fn decode_wallet_row(
    logical: &str,
    key: &[u8],
    value: &[u8],
) -> Result<Option<u64>, String> {
    let error = |error: zinder_wallet_projection::WalletProjectionContractError| error.to_string();
    match logical {
        "transparent_unspent_output" => {
            let key = WalletOutpointKey::decode(key).map_err(error)?;
            let output = WalletUnspentOutput::decode_value(key, value).map_err(error)?;
            Ok(Some(output.value_zat))
        }
        "transparent_unspent_output_by_address" => {
            WalletAddressUnspentOutputKey::decode(key).map_err(error)?;
            if value.is_empty() {
                Ok(None)
            } else {
                Err("address unspent index value is not empty".to_owned())
            }
        }
        "transparent_spent_output" => {
            let key = WalletOutpointKey::decode(key).map_err(error)?;
            WalletSpentOutput::decode_value(key, value).map_err(error)?;
            Ok(None)
        }
        "transparent_address_transaction" => {
            let key = WalletAddressTransactionKey::decode(key).map_err(error)?;
            WalletAddressTransaction::decode_value(key, value).map_err(error)?;
            Ok(None)
        }
        "transparent_address_balance" => {
            WalletAddressBalance::decode(key, value).map_err(error)?;
            Ok(None)
        }
        "reorg_undo" => {
            WalletReorgUndo::decode(key, value).map_err(error)?;
            Ok(None)
        }
        other => Err(format!("unknown wallet family {other}")),
    }
}

#[derive(Default, PartialEq, Eq)]
struct FamilyScan {
    rows: u64,
    bytes: u64,
    value_zat: u64,
    hash: u64,
}

/// One full wallet-family scan held through one snapshot.
pub(crate) fn wallet_family_cycle(
    shared: &Shared<'_>,
    name: &str,
    params: &WalletReaderParams,
    stop: &AtomicBool,
) -> ReaderCycleRecord {
    let started = Instant::now();
    let mut record = ReaderCycleRecord {
        phase: shared.clock.phase().to_owned(),
        reader: name.to_owned(),
        kind: "wallet-family",
        snapshot: params.snapshot,
        held_iterator: true,
        t_start: shared.clock.t(),
        hold_target_s: params.hold.as_secs_f64(),
        family: Some(params.family.clone()),
        ..ReaderCycleRecord::default()
    };
    let Some(db) = shared.wallet_db else {
        record.error = Some("no wallet store".to_owned());
        record.finish(started, shared);
        return record;
    };
    let snapshot = params.snapshot.then(|| db.snapshot());
    record.snapshot_sequence = snapshot
        .as_ref()
        .map(rust_rocksdb::SnapshotWithThreadMode::sequence_number);
    if let Err(error) = wallet_family_body(
        shared,
        db,
        snapshot.as_ref(),
        params,
        stop,
        &mut record,
        started,
    ) {
        record.error = Some(error);
    }
    drop(snapshot);
    record.finish(started, shared);
    record
}

fn wallet_family_body(
    _shared: &Shared<'_>,
    db: &DB,
    snapshot: Option<&SpikeSnapshot<'_>>,
    params: &WalletReaderParams,
    stop: &AtomicBool,
    record: &mut ReaderCycleRecord,
    started: Instant,
) -> Result<(), String> {
    let evidence_start =
        spike_wallet_ready_evidence_at(db, snapshot).map_err(|error| error.to_string())?;
    let physical = resolve_physical_family(db, snapshot, &params.family)?;
    record.physical_family = Some(physical.clone());
    let family = db
        .cf_handle(&physical)
        .ok_or_else(|| format!("column family {physical} is unavailable"))?;
    let expected = expected_family_rows(&evidence_start, &params.family);
    record.expected_rows = expected;

    let scan = |iterator_mode| -> Result<FamilyScan, String> {
        let mut scan = FamilyScan::default();
        let mut hasher = DefaultHasher::new();
        for row in db.iterator_cf_opt(&family, spike_read_options(snapshot, false), iterator_mode) {
            let (key, value) = row.map_err(|error| error.to_string())?;
            if let Some(value_zat) = decode_wallet_row(&params.family, &key, &value)? {
                scan.value_zat = scan.value_zat.wrapping_add(value_zat);
            }
            scan.rows += 1;
            scan.bytes += (key.len() + value.len()) as u64;
            (&key[..], &value[..]).hash(&mut hasher);
        }
        scan.hash = hasher.finish();
        Ok(scan)
    };

    let pass_started = Instant::now();
    let first = scan(IteratorMode::Start)?;
    record.pass1_seconds = pass_started.elapsed().as_secs_f64();
    record.pass1_rows = first.rows;
    record.pass1_bytes = first.bytes;
    if let Some(expected) = expected
        && expected != first.rows
    {
        record.violate(format!(
            "scanned {} rows; READY control in the same view says {expected}",
            first.rows
        ));
    }
    if params.family == "transparent_unspent_output" {
        let summary = &evidence_start.utxo_summary;
        if summary.utxo_count != first.rows || summary.total_value_zat != first.value_zat {
            record.violate(format!(
                "UTXO scan {} rows / {} zat differs from summary {} / {}",
                first.rows, first.value_zat, summary.utxo_count, summary.total_value_zat
            ));
        }
    }

    // Paced re-scan through one held raw iterator for the rest of the hold.
    let mut iterator = db.raw_iterator_cf_opt(&family, spike_read_options(snapshot, false));
    iterator.seek_to_first();
    let mut rescan = FamilyScan::default();
    let mut rescan_hasher = DefaultHasher::new();
    while started.elapsed() < params.hold {
        if stop.load(Ordering::SeqCst) {
            record.hold_truncated = true;
            break;
        }
        let remaining = params.hold.saturating_sub(started.elapsed());
        std::thread::sleep(params.paced_interval.min(remaining));
        let remaining = params.hold.saturating_sub(started.elapsed()).as_secs_f64();
        let ticks_left = (remaining / params.paced_interval.as_secs_f64()).max(1.0);
        let rows_left = first.rows.saturating_sub(rescan.rows) as f64;
        let rows_this_tick = (rows_left / ticks_left).ceil().max(1.0) as u64;
        for _ in 0..rows_this_tick {
            if !iterator.valid() {
                iterator.status().map_err(|error| error.to_string())?;
                rescan.hash = rescan_hasher.finish();
                if rescan.rows != first.rows || rescan.hash != first.hash {
                    record.violate(format!(
                        "paced rescan saw {} rows (hash differs: {}); first pass saw {}",
                        rescan.rows,
                        rescan.hash != first.hash,
                        first.rows
                    ));
                }
                record.paced_reads += 1;
                rescan = FamilyScan::default();
                rescan_hasher = DefaultHasher::new();
                iterator.seek_to_first();
                break;
            }
            let (Some(key), Some(value)) = (iterator.key(), iterator.value()) else {
                break;
            };
            decode_wallet_row(&params.family, key, value)?;
            (key, value).hash(&mut rescan_hasher);
            rescan.rows += 1;
            record.paced_rows += 1;
            iterator.next();
        }
    }
    drop(iterator);
    let evidence_end =
        spike_wallet_ready_evidence_at(db, snapshot).map_err(|error| error.to_string())?;
    if evidence_end != evidence_start {
        record.violate(format!(
            "READY evidence changed during the hold: tip {} -> {}",
            evidence_start.source_position.tip.height.value(),
            evidence_end.source_position.tip.height.value()
        ));
    }
    record.family_dropped_before_release = Some(db.cf_handle(&physical).is_none());
    Ok(())
}

/// Small deterministic generator for load-request choices.
pub(crate) struct XorShift(u64);

impl XorShift {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    pub(crate) fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    pub(crate) fn below(&mut self, bound: usize) -> usize {
        if bound == 0 {
            0
        } else {
            (self.next() % bound as u64) as usize
        }
    }
}

/// Samples drawn at startup for the serving load.
#[derive(Default)]
pub(crate) struct LoadSamples {
    pub(crate) addresses: Vec<[u8; 32]>,
    pub(crate) transaction_ids: Vec<TransactionId>,
}

#[derive(Default)]
pub(crate) struct LoadLatencies {
    pub(crate) compact: Mutex<Vec<f64>>,
    pub(crate) wallet: Mutex<Vec<f64>>,
    pub(crate) lookup: Mutex<Vec<f64>>,
    pub(crate) errors: Mutex<Vec<String>>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct LoadSummary {
    pub(crate) compact: LatencySummary,
    pub(crate) wallet: LatencySummary,
    pub(crate) lookup: LatencySummary,
    pub(crate) compact_requests_per_s: f64,
    pub(crate) wallet_requests_per_s: f64,
    pub(crate) lookup_requests_per_s: f64,
    pub(crate) error_count: usize,
    pub(crate) first_errors: Vec<String>,
}

impl LoadLatencies {
    pub(crate) fn summary(&self, seconds: f64) -> LoadSummary {
        let compact = self.compact.lock();
        let wallet = self.wallet.lock();
        let lookup = self.lookup.lock();
        let errors = self.errors.lock();
        LoadSummary {
            compact: LatencySummary::from_values(&compact),
            wallet: LatencySummary::from_values(&wallet),
            lookup: LatencySummary::from_values(&lookup),
            compact_requests_per_s: compact.len() as f64 / seconds.max(f64::EPSILON),
            wallet_requests_per_s: wallet.len() as f64 / seconds.max(f64::EPSILON),
            lookup_requests_per_s: lookup.len() as f64 / seconds.max(f64::EPSILON),
            error_count: errors.len(),
            first_errors: errors.iter().take(10).cloned().collect(),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum LoadKind {
    Compact,
    Wallet,
    Lookup,
}

/// One load worker issuing request-scoped snapshot reads until `stop`.
pub(crate) fn run_load_worker(
    shared: &Shared<'_>,
    kind: LoadKind,
    spec: &LoadSpec,
    samples: &LoadSamples,
    latencies: &LoadLatencies,
    stop: &AtomicBool,
    seed: u64,
) {
    let mut random = XorShift::new(seed);
    while !stop.load(Ordering::SeqCst) {
        let started = Instant::now();
        let outcome = match kind {
            LoadKind::Compact => compact_request(shared, spec, &mut random),
            LoadKind::Wallet => wallet_request(shared, samples, &mut random),
            LoadKind::Lookup => lookup_request(shared, samples, &mut random),
        };
        let seconds = started.elapsed().as_secs_f64();
        match outcome {
            Ok(()) => match kind {
                LoadKind::Compact => latencies.compact.lock().push(seconds),
                LoadKind::Wallet => latencies.wallet.lock().push(seconds),
                LoadKind::Lookup => latencies.lookup.lock().push(seconds),
            },
            Err(error) => {
                let mut errors = latencies.errors.lock();
                if errors.len() < 1_000 {
                    errors.push(error);
                }
            }
        }
        if spec.think_time_ms > 0 {
            std::thread::sleep(Duration::from_millis(spec.think_time_ms));
        }
    }
}

fn compact_request(
    shared: &Shared<'_>,
    spec: &LoadSpec,
    random: &mut XorShift,
) -> Result<(), String> {
    let snapshot = shared.canonical_db.snapshot();
    let (tip, _) = spike_canonical_epoch_tip(shared.canonical_db, shared.network, Some(&snapshot))
        .map_err(|error| error.to_string())?;
    let window = 10_000_u32.max(spec.compact_range_blocks);
    let lowest = tip
        .saturating_sub(window)
        .max(shared.first_retained_height.max(1));
    let span = tip
        .saturating_sub(lowest)
        .saturating_sub(spec.compact_range_blocks) as usize;
    let start = lowest + random.below(span + 1) as u32;
    let end = (start + spec.compact_range_blocks - 1).min(tip);
    let reads = spike_compact_blocks(
        shared.canonical_db,
        Some(&snapshot),
        BlockHeightRange::inclusive(BlockHeight::new(start), BlockHeight::new(end)),
    )
    .map_err(|error| error.to_string())?;
    if reads
        .iter()
        .any(|read| !matches!(read, SpikeCompactBlockRead::Present(_)))
    {
        return Err(format!("compact request {start}..={end} saw an absent row"));
    }
    Ok(())
}

fn wallet_request(
    shared: &Shared<'_>,
    samples: &LoadSamples,
    random: &mut XorShift,
) -> Result<(), String> {
    let Some(db) = shared.wallet_db else {
        return Err("no wallet store".to_owned());
    };
    if samples.addresses.is_empty() {
        return Err("no sampled addresses".to_owned());
    }
    let address = samples.addresses[random.below(samples.addresses.len())];
    let snapshot = db.snapshot();
    let address_hash = TransparentAddressScriptHash::from_bytes(address);
    let history_family =
        resolve_physical_family(db, Some(&snapshot), "transparent_address_transaction")?;
    let history = db
        .cf_handle(&history_family)
        .ok_or_else(|| "address transaction family unavailable".to_owned())?;
    let start = WalletAddressTransactionKey::first_at_or_after(address_hash, BlockHeight::new(0));
    let mut rows = 0;
    for row in db.iterator_cf_opt(
        &history,
        spike_read_options(Some(&snapshot), true),
        IteratorMode::From(start.as_bytes(), rust_rocksdb::Direction::Forward),
    ) {
        let (key, value) = row.map_err(|error| error.to_string())?;
        if !key.starts_with(&address) || rows >= 100 {
            break;
        }
        decode_wallet_row("transparent_address_transaction", &key, &value)?;
        rows += 1;
    }
    let index = db
        .cf_handle(&resolve_physical_family(
            db,
            Some(&snapshot),
            "transparent_unspent_output_by_address",
        )?)
        .ok_or_else(|| "address unspent index unavailable".to_owned())?;
    let unspent = db
        .cf_handle(&resolve_physical_family(
            db,
            Some(&snapshot),
            "transparent_unspent_output",
        )?)
        .ok_or_else(|| "unspent family unavailable".to_owned())?;
    let mut outpoints = Vec::new();
    for row in db.iterator_cf_opt(
        &index,
        spike_read_options(Some(&snapshot), true),
        IteratorMode::From(&address, rust_rocksdb::Direction::Forward),
    ) {
        let (key, _) = row.map_err(|error| error.to_string())?;
        if !key.starts_with(&address) || outpoints.len() >= 100 {
            break;
        }
        let key = WalletAddressUnspentOutputKey::decode(&key).map_err(|error| error.to_string())?;
        outpoints.push(WalletOutpointKey::new(key.outpoint()));
    }
    let read_options = spike_read_options(Some(&snapshot), true);
    for (outpoint, row) in outpoints.iter().zip(db.batched_multi_get_cf_opt(
        &unspent,
        outpoints.iter().map(|key| key.as_bytes().as_slice()),
        false,
        &read_options,
    )) {
        let row = row
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "address index names a missing unspent output".to_owned())?;
        WalletUnspentOutput::decode_value(*outpoint, &row).map_err(|error| error.to_string())?;
    }
    let balance_family =
        resolve_physical_family(db, Some(&snapshot), "transparent_address_balance")?;
    let balance = db
        .cf_handle(&balance_family)
        .ok_or_else(|| "balance family unavailable".to_owned())?;
    let _ = db
        .get_pinned_cf_opt(&balance, address, &read_options)
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn lookup_request(
    shared: &Shared<'_>,
    samples: &LoadSamples,
    random: &mut XorShift,
) -> Result<(), String> {
    if samples.transaction_ids.is_empty() {
        return Err("no sampled transaction ids".to_owned());
    }
    let snapshot = shared.canonical_db.snapshot();
    let transaction_id = samples.transaction_ids[random.below(samples.transaction_ids.len())];
    let found = spike_transaction_lookup(shared.canonical_db, Some(&snapshot), transaction_id)
        .map_err(|error| error.to_string())?;
    let Some((location, _blob)) = found else {
        return Ok(());
    };
    let reads = spike_compact_blocks(
        shared.canonical_db,
        Some(&snapshot),
        BlockHeightRange::inclusive(location.block_height, location.block_height),
    )
    .map_err(|error| error.to_string())?;
    if !matches!(reads.first(), Some(SpikeCompactBlockRead::Present(_))) {
        return Err("located transaction's block is absent".to_owned());
    }
    Ok(())
}
