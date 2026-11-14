//! Arm 2: repeated deep replacements of the visible suffix at the tip.
//!
//! Each replacement displaces the newest `depth` blocks and commits the same
//! blocks again, re-derived from the fixture on top of the replacement
//! parent's reconstructed checkpoint. The canonical side runs the production
//! replacement (displaced capture, deletions, permanent archive, replacement
//! rows, reorg event, READY control) and the wallet side runs the production
//! reorg transition (`rollback_range` over the undo suffix, then replay), both
//! in one synced batch. Re-committing identical content keeps the final state
//! comparable with the reference wallet while carrying the full byte volume.
//!
//! With `--density-multiplier N`, the same synced batch also carries N - 1
//! mirror lanes of every per-block operation of the replacement: displaced-row
//! deletions, replacement rows, permanent archive rows, and the wallet
//! rollback-and-reapply row operations (with their planner point reads, digest
//! folds, and overlay accounting). The lane copies stay in memory until the
//! write returns, as N times larger prepared rows would.

use std::{
    num::NonZeroU64,
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

use clap::Args;
use rust_rocksdb::WriteBatch;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use zinder_bench::{BenchError, fixture::FixtureNodeSource};
use zinder_core::{BlockHeight, BlockId, UnixTimestampMillis};
use zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES;
use zinder_store::{CanonicalLiveReplacement, CanonicalReplacementBlock, RawBlobRetention};
use zinder_wallet_projection::WalletCanonicalSourceIdentity;
use zinder_wallet_rocksdb::{
    MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES, RocksDbWalletStore, SpikeMirrorPlanOverlay,
};

use super::{
    CommitMode, CommonArgs, NETWORK_LABEL_TESTNET,
    layout::{
        BudgetView, CopyStats, Layout, absolute, copy_closed_store, ensure_column_families,
        open_canonical, open_secondary, wallet_family_budget, writer_budget,
    },
    metrics::{
        PropertySampler, PropertySamples, StallRecorder, StallSummary, current_rss_bytes,
        install_stall_listener, peak_rss_bytes, ticker_delta, ticker_snapshot,
    },
    mirror::{MirrorCounts, MirrorFamilies, MirrorFamilyNames, MirrorWork, apply_lanes, lane_key},
    pipeline::derive_block,
    replay::{FenceView, ThresholdCheck, WalletView, finish_secondary, mirror_scratch_accumulator},
    secondary_follow::SecondaryChild,
    spike_error,
};

const GIB: u64 = 1024 * 1024 * 1024;
const PROJECTOR_DEFAULT_FOLLOW_MAX_TRANSITION_BYTES: u64 = 512 * 1024 * 1024;
const REPLACEMENT_BATCH_CEILING_BYTES: u64 = 5 * GIB / 2;

/// Reorg arm inputs.
#[derive(Args)]
pub(crate) struct ReorgArgs {
    #[command(flatten)]
    pub(crate) common: CommonArgs,
    /// Store at the tip to replace in place (usually a replay arm's work store).
    #[arg(long)]
    store: Option<PathBuf>,
    /// Golden store to copy into `--work` instead of using `--store` in place.
    #[arg(long)]
    golden: Option<PathBuf>,
    /// Fresh path for the copy when `--golden` is given.
    #[arg(long)]
    work: Option<PathBuf>,
    /// Canonical-only or combined.
    #[arg(long, value_enum)]
    mode: CommitMode,
    /// Number of replacements.
    #[arg(long, default_value_t = 5)]
    count: u32,
    /// Displaced and replacement depth in blocks.
    #[arg(long, default_value_t = 1000)]
    depth: u32,
    /// Directory for a canonical secondary caught up after each replacement.
    #[arg(long)]
    secondary_dir: Option<PathBuf>,
    /// Run the secondary inside the writer process instead of a child process.
    #[arg(long, default_value_t = false)]
    secondary_in_process: bool,
    /// Wallet transition logical-byte ceiling passed to the planner.
    #[arg(long, default_value_t = MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES)]
    wallet_max_transition_bytes: u64,
    /// Per-block row volume multiplier: N - 1 mirror lanes ride in each replacement batch.
    #[arg(long, default_value_t = 1)]
    density_multiplier: u32,
    /// After the last replacement, cold-recompute the combined wallet rows and
    /// require them to equal the READY evidence.
    #[arg(long, default_value_t = false)]
    cold_readback: bool,
}

/// One measured replacement.
#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct ReplacementSample {
    pub(crate) index: u32,
    pub(crate) first_height: u32,
    pub(crate) tip_height: u32,
    pub(crate) blocks: u32,
    pub(crate) source_and_derive_seconds: f64,
    /// `ingest.follow.max_replacement_bytes` accounting (source + prepared bytes).
    pub(crate) ingest_held_bytes: u64,
    pub(crate) canonical_prepare_seconds: f64,
    pub(crate) wallet_plan_seconds: f64,
    pub(crate) canonical_put_seconds: f64,
    pub(crate) synced_write_seconds: f64,
    pub(crate) canonical_readback_seconds: f64,
    pub(crate) wallet_adopt_seconds: f64,
    /// Canonical prepare through wallet adopt.
    pub(crate) commit_seconds: f64,
    pub(crate) batch_bytes: u64,
    pub(crate) batch_operations: u64,
    pub(crate) canonical_batch_bytes: u64,
    pub(crate) wallet_batch_bytes: u64,
    pub(crate) wallet_plan_batch_logical_bytes: u64,
    pub(crate) wallet_plan_overlay_logical_bytes: u64,
    pub(crate) wallet_point_reads: u64,
    /// Real per-block canonical operations mirrored per lane (deletions, rows, archive).
    pub(crate) scaled_canonical_ops_per_lane: u64,
    pub(crate) mirror_seconds: f64,
    pub(crate) mirror_canonical_batch_bytes: u64,
    pub(crate) mirror_wallet_batch_bytes: u64,
    pub(crate) mirror_counts: MirrorCounts,
    /// Mirror wallet rows charged under the planner's batch and overlay rules.
    pub(crate) mirror_wallet_plan_batch_logical_bytes: u64,
    pub(crate) mirror_wallet_plan_overlay_logical_bytes: u64,
    /// Real plus mirror wallet plan logical bytes (batch plus overlay).
    pub(crate) scaled_wallet_plan_logical_bytes: u64,
    /// Bytes of the lane copies held in memory through the write.
    pub(crate) retained_lane_bytes: u64,
    /// batch_bytes / (batch_bytes - mirror bytes).
    pub(crate) effective_byte_multiplier: f64,
    /// ingest_held_bytes times the nominal multiplier (estimate; source bytes are not mirrored).
    pub(crate) scaled_ingest_held_bytes_estimate: u64,
    pub(crate) rss_before_bytes: Option<u64>,
    pub(crate) rss_after_prepare_bytes: Option<u64>,
    pub(crate) rss_after_commit_bytes: Option<u64>,
    pub(crate) peak_vmhwm_after_bytes: Option<u64>,
    pub(crate) secondary_catch_up_seconds: Option<f64>,
    pub(crate) secondary_fence_matches_primary: Option<bool>,
    pub(crate) secondary_wallet_control_matches_fence: Option<bool>,
    pub(crate) resulting_fence: Option<FenceView>,
}

/// Report of the reorg arm.
#[derive(Serialize)]
pub(crate) struct ReorgReport {
    pub(crate) spike: &'static str,
    pub(crate) action: &'static str,
    pub(crate) network_label: &'static str,
    pub(crate) label: Option<String>,
    pub(crate) mode: CommitMode,
    pub(crate) writer_budget: BudgetView,
    pub(crate) store: String,
    pub(crate) copy: Option<CopyStats>,
    pub(crate) depth: u32,
    pub(crate) density_multiplier: u32,
    pub(crate) wallet_supported_reorg_depth: Option<u32>,
    pub(crate) replacements: Vec<ReplacementSample>,
    pub(crate) secondary_open_seconds: Option<f64>,
    pub(crate) peak_vmhwm_bytes: Option<u64>,
    /// VmHWM plus (multiplier - 1) times the largest ingest_held_bytes, an upper
    /// estimate that adds the unmirrored source and derived block bytes.
    pub(crate) peak_rss_with_unmirrored_source_estimate_bytes: Option<u64>,
    pub(crate) tickers: std::collections::BTreeMap<&'static str, u64>,
    pub(crate) stalls: StallSummary,
    pub(crate) properties: PropertySamples,
    pub(crate) final_fence: FenceView,
    pub(crate) wallet_before: Option<WalletView>,
    pub(crate) wallet_final: Option<WalletView>,
    /// Seconds of the cold readback that matched the READY evidence.
    pub(crate) cold_readback_seconds: Option<f64>,
    pub(crate) ceilings: Vec<ThresholdCheck>,
}

/// Runs the reorg arm.
pub(crate) async fn run_reorg(args: ReorgArgs) -> Result<ReorgReport, BenchError> {
    let network = args.common.network()?;
    let (manifest, activations) = args.common.manifest_and_activations()?;
    let activations = Arc::new(activations);
    let combined = args.mode == CommitMode::Combined;
    if args.density_multiplier == 0 {
        return Err(BenchError::invalid_argument("--density-multiplier must be >= 1"));
    }
    let density_lanes = u8::try_from(args.density_multiplier - 1)
        .map_err(|_| BenchError::invalid_argument("density multiplier too large"))?;
    let layout = Layout {
        wallet: combined,
        mirrors: false,
    };
    let (path, copy) = match (&args.store, &args.golden, &args.work) {
        (Some(store), None, None) => (store.clone(), None),
        (None, Some(golden), Some(work)) => (work.clone(), Some(copy_closed_store(golden, work)?)),
        _ => {
            return Err(BenchError::invalid_argument(
                "pass either --store, or --golden with --work",
            ));
        }
    };
    let wallet_budget = wallet_family_budget();
    let budget = writer_budget(
        combined,
        args.common.budget_profile,
        args.common.max_background_jobs,
        args.common.statistics_level,
    );
    let existing = rust_rocksdb::DB::list_cf(&rust_rocksdb::Options::default(), &path)
        .map_err(|source| spike_error("list column families", source))?;
    let layout = Layout {
        mirrors: density_lanes > 0 || existing.iter().any(|name| name.starts_with("mirror_")),
        ..layout
    };
    ensure_column_families(&path, &layout.extra_names())?;
    layout.register(wallet_budget);
    let stalls = StallRecorder::new();
    install_stall_listener(&stalls);
    let mut store = Some(open_canonical(
        &path,
        &activations,
        args.common.reorg_window_blocks,
        budget,
    )?);
    let mut wallet = if combined {
        let shared = store
            .as_ref()
            .ok_or_else(|| BenchError::spike("store lost"))?
            .spike_bounded_open()
            .clone();
        Some(RocksDbWalletStore::spike_open_shared(shared, network)?)
    } else {
        None
    };
    let db = Arc::clone(
        &store
            .as_ref()
            .ok_or_else(|| BenchError::spike("store lost"))?
            .spike_bounded_open()
            .db,
    );
    let statistics = Arc::clone(
        &store
            .as_ref()
            .ok_or_else(|| BenchError::spike("store lost"))?
            .spike_bounded_open()
            .statistics,
    );
    let wallet_before = wallet
        .as_ref()
        .map(|wallet| WalletView::from(wallet.ready_evidence()));
    let sampler = PropertySampler::spawn(
        Arc::clone(&db),
        db.cf_names(),
        std::time::Duration::from_millis(250),
    );
    let tickers_before = ticker_snapshot(&statistics);

    let mut secondary_open_seconds = None;
    let mut secondary = None;
    let mut secondary_child = None;
    if let Some(dir) = args.secondary_dir.as_ref() {
        let started = Instant::now();
        if args.secondary_in_process {
            secondary = Some(open_secondary(&path, dir, &activations, args.common.reorg_window_blocks)?);
        } else {
            secondary_child = Some(SecondaryChild::spawn(
                &args.common.fixture,
                &args.common.network,
                &path,
                dir,
                0,
                args.common.reorg_window_blocks,
            )?);
        }
        secondary_open_seconds = Some(started.elapsed().as_secs_f64());
    }

    let source = FixtureNodeSource::open(&args.common.fixture, &manifest)?;
    let cancel = CancellationToken::new();
    let wallet_max = NonZeroU64::new(args.wallet_max_transition_bytes)
        .ok_or_else(|| BenchError::invalid_argument("wallet ceiling must be non-zero"))?;
    let mut replacements = Vec::new();
    let mirror_names = MirrorFamilyNames::new();
    let mut accumulator = mirror_scratch_accumulator();
    for index in 0..args.count {
        let current = store.take().ok_or_else(|| BenchError::spike("store lost"))?;
        let mut sample = ReplacementSample {
            index,
            rss_before_bytes: current_rss_bytes(),
            ..ReplacementSample::default()
        };
        let anchor = current.append_anchor()?;
        let tip = anchor.event_fence().visible_tip();
        let settled = anchor.settled_tip();
        let first_height = tip
            .height
            .value()
            .saturating_sub(args.depth.saturating_sub(1))
            .max(settled.height.value() + 1);
        let parent_height = BlockHeight::new(first_height - 1);
        let parent_hash = current
            .block_header_at(parent_height)?
            .ok_or_else(|| BenchError::spike("replacement parent header absent"))?
            .block_hash;
        let parent = BlockId::new(parent_height, parent_hash);
        if manifest.from_height > first_height || manifest.to_height < tip.height.value() {
            return Err(BenchError::invalid_argument(
                "fixture does not cover the replacement range",
            ));
        }
        sample.first_height = first_height;
        sample.tip_height = tip.height.value();
        let derive_started = Instant::now();
        let mut predecessor = current.replacement_parent_checkpoint(parent, &activations)?;
        let mut blocks = Vec::new();
        let mut held_bytes = 0_u64;
        for height in first_height..=tip.height.value() {
            let (derived, checkpoint) = derive_block(
                &source,
                BlockHeight::new(height),
                &predecessor,
                &activations,
                RawBlobRetention::Transactions,
                &cancel,
            )
            .await?;
            held_bytes += derived.prepared_bytes;
            predecessor = checkpoint;
            blocks.push(CanonicalReplacementBlock::new(
                derived.block,
                derived.subtree_roots,
            ));
        }
        sample.blocks = u32::try_from(blocks.len()).unwrap_or(u32::MAX);
        sample.ingest_held_bytes = held_bytes;
        sample.source_and_derive_seconds = derive_started.elapsed().as_secs_f64();
        sample.rss_after_prepare_bytes = current_rss_bytes();
        let replacement = CanonicalLiveReplacement::new(
            anchor.event_fence(),
            blocks,
            UnixTimestampMillis::now(),
        );

        sample.scaled_ingest_held_bytes_estimate =
            held_bytes.saturating_mul(u64::from(args.density_multiplier));
        let wallet_taken = wallet.take();
        let commit_activations = Arc::clone(&activations);
        let lanes = Lanes {
            density_lanes,
            names: mirror_names.clone(),
            accumulator,
        };
        let (next_store, next_wallet, sample_out, lanes_out) =
            tokio::task::spawn_blocking(move || {
                commit_replacement(
                    current,
                    wallet_taken,
                    replacement,
                    &commit_activations,
                    wallet_max,
                    sample,
                    lanes,
                )
            })
            .await
            .map_err(|source| spike_error("replacement join", source))??;
        store = Some(next_store);
        wallet = next_wallet;
        sample = sample_out;
        accumulator = lanes_out.accumulator;
        sample.rss_after_commit_bytes = current_rss_bytes();
        sample.peak_vmhwm_after_bytes = peak_rss_bytes();
        let primary_fence = store
            .as_ref()
            .ok_or_else(|| BenchError::spike("store lost"))?
            .event_fence();
        sample.resulting_fence = Some(FenceView::from(primary_fence));
        if let Some(secondary) = secondary.as_mut() {
            let (seconds, _fence, matches, wallet_matches) =
                finish_secondary(secondary, primary_fence, combined)?;
            sample.secondary_catch_up_seconds = Some(seconds);
            sample.secondary_fence_matches_primary = Some(matches);
            sample.secondary_wallet_control_matches_fence = wallet_matches;
        }
        if let Some(child) = secondary_child.as_mut() {
            let response = child.request_catch_up()?;
            sample.secondary_catch_up_seconds = Some(response.seconds);
            sample.secondary_fence_matches_primary = Some(
                response.tip_height == primary_fence.visible_tip().height.value()
                    && response.event_sequence == primary_fence.chain_event_sequence(),
            );
            sample.secondary_wallet_control_matches_fence = response.wallet_control_matches_fence;
        }
        tracing::info!(
            target: "zinder::bench::spike",
            event = "spike_replacement_committed",
            index,
            first_height,
            tip_height = sample.tip_height,
            batch_bytes = sample.batch_bytes,
            commit_seconds = sample.commit_seconds,
            "spike replacement committed"
        );
        replacements.push(sample);
    }
    let store = store.ok_or_else(|| BenchError::spike("store lost"))?;
    let has_secondary = secondary.is_some() || secondary_child.is_some();
    if let Some(child) = secondary_child {
        child.finish()?;
    }
    let properties = sampler.finish();
    let tickers = ticker_delta(&tickers_before, &ticker_snapshot(&statistics));
    let peak = peak_rss_bytes();
    let mut cold_readback_seconds = None;
    if let (true, Some(wallet)) = (args.cold_readback, wallet.as_ref()) {
        let readback = wallet.spike_cold_readback(512 * 1024 * 1024)?;
        cold_readback_seconds = Some(readback.elapsed.as_secs_f64());
    }
    let max_wallet_logical = replacements
        .iter()
        .map(|sample| sample.scaled_wallet_plan_logical_bytes)
        .max()
        .unwrap_or(0);
    let max_scaled_held = replacements
        .iter()
        .map(|sample| sample.scaled_ingest_held_bytes_estimate)
        .max()
        .unwrap_or(0);
    let max_batch = replacements.iter().map(|sample| sample.batch_bytes).max().unwrap_or(0);
    let max_held = replacements
        .iter()
        .map(|sample| sample.ingest_held_bytes)
        .max()
        .unwrap_or(0);
    let max_catch_up = replacements
        .iter()
        .filter_map(|sample| sample.secondary_catch_up_seconds)
        .fold(0.0_f64, f64::max);
    let mut ceilings = vec![
        ThresholdCheck {
            check: "replacement batch at most 2.5 GiB",
            threshold: format!("<= {REPLACEMENT_BATCH_CEILING_BYTES} bytes"),
            observed: format!("{max_batch} bytes"),
            passed: max_batch <= REPLACEMENT_BATCH_CEILING_BYTES,
        },
        ThresholdCheck {
            check: "writer RSS at most 6 GiB",
            threshold: "<= 6 GiB".to_owned(),
            observed: format!("{} bytes", peak.unwrap_or(0)),
            passed: peak.is_some_and(|bytes| bytes <= 6 * GIB),
        },
        ThresholdCheck {
            check: "zero stop stalls",
            threshold: "0 stop entries".to_owned(),
            observed: format!(
                "{} stop entries, {} stopped samples",
                stalls.summary().db_stop_entries,
                properties.write_stopped_samples
            ),
            passed: stalls.summary().db_stop_entries == 0
                && properties.write_stopped_samples == 0,
        },
        ThresholdCheck {
            check: "ingest replacement memory within ingest.follow.max_replacement_bytes",
            threshold: format!("<= {} bytes", DEFAULT_MAX_REPLACEMENT_BYTES.get()),
            observed: if density_lanes > 0 {
                format!("{max_held} bytes real; {max_scaled_held} bytes scaled estimate")
            } else {
                format!("{max_held} bytes")
            },
            passed: max_scaled_held.max(max_held) <= DEFAULT_MAX_REPLACEMENT_BYTES.get(),
        },
    ];
    let rss_estimate = peak.map(|peak| {
        peak.saturating_add(max_held.saturating_mul(u64::from(density_lanes)))
    });
    if density_lanes > 0 {
        ceilings.push(ThresholdCheck {
            check: "writer RSS at most 6 GiB, adding unmirrored source bytes (estimate)",
            threshold: "<= 6 GiB".to_owned(),
            observed: format!("{} bytes", rss_estimate.unwrap_or(0)),
            passed: rss_estimate.is_some_and(|bytes| bytes <= 6 * GIB),
        });
    }
    if combined {
        ceilings.push(ThresholdCheck {
            check: "wallet plan within MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES (1 GiB)",
            threshold: format!("<= {MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES} bytes"),
            observed: format!("{max_wallet_logical} bytes"),
            passed: max_wallet_logical <= MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES,
        });
        ceilings.push(ThresholdCheck {
            check: "wallet plan within projector default follow ceiling (512 MiB)",
            threshold: format!("<= {PROJECTOR_DEFAULT_FOLLOW_MAX_TRANSITION_BYTES} bytes"),
            observed: format!("{max_wallet_logical} bytes"),
            passed: max_wallet_logical <= PROJECTOR_DEFAULT_FOLLOW_MAX_TRANSITION_BYTES,
        });
    }
    if has_secondary {
        ceilings.push(ThresholdCheck {
            check: "secondary catch-up after each replacement under 30 s",
            threshold: "< 30 s".to_owned(),
            observed: format!("{max_catch_up:.3} s max"),
            passed: max_catch_up < 30.0,
        });
    }
    Ok(ReorgReport {
        spike: "A-combined-commit",
        action: "reorg",
        network_label: NETWORK_LABEL_TESTNET,
        label: args.common.label.clone(),
        mode: args.mode,
        writer_budget: BudgetView::from(budget),
        store: absolute(&path).display().to_string(),
        copy,
        depth: args.depth,
        density_multiplier: args.density_multiplier,
        wallet_supported_reorg_depth: wallet
            .as_ref()
            .map(RocksDbWalletStore::spike_supported_reorg_depth),
        replacements,
        secondary_open_seconds,
        peak_vmhwm_bytes: peak,
        peak_rss_with_unmirrored_source_estimate_bytes: rss_estimate,
        tickers,
        stalls: stalls.summary(),
        properties,
        final_fence: FenceView::from(store.event_fence()),
        wallet_before,
        wallet_final: wallet
            .as_ref()
            .map(|wallet| WalletView::from(wallet.ready_evidence())),
        cold_readback_seconds,
        ceilings,
    })
}

/// Mirror-lane state carried across replacements.
struct Lanes {
    density_lanes: u8,
    names: MirrorFamilyNames,
    accumulator: zinder_wallet_projection::WalletProjectionDigestBuilder,
}

type LaneRow = (&'static str, Vec<u8>, Option<Vec<u8>>);

fn commit_replacement(
    store: zinder_store::RocksDbCanonicalStore,
    mut wallet: Option<RocksDbWalletStore>,
    replacement: CanonicalLiveReplacement,
    activations: &zinder_core::NetworkUpgradeActivations,
    wallet_max: NonZeroU64,
    mut sample: ReplacementSample,
    mut lanes: Lanes,
) -> Result<
    (
        zinder_store::RocksDbCanonicalStore,
        Option<RocksDbWalletStore>,
        ReplacementSample,
        Lanes,
    ),
    BenchError,
> {
    let started = Instant::now();
    let prepared = store.spike_prepare_live_replacement(replacement, activations)?;
    let canonical_prepared = Instant::now();
    sample.canonical_prepare_seconds = (canonical_prepared - started).as_secs_f64();
    let record_ops = lanes.density_lanes > 0;
    let mut batch = WriteBatch::default();
    let mut pending = None;
    let mut wallet_ops = None;
    if let Some(wallet) = wallet.as_ref() {
        let event = prepared.retained_event()?;
        let replays = prepared.committed_replays()?;
        let expected_source =
            WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
        let planned = wallet.spike_plan_canonical_event_range(
            expected_source,
            event,
            prepared.resulting_fence(),
            prepared.settled_tip(),
            wallet_max,
            replays.into_iter().map(Ok),
            batch,
            record_ops,
        )?;
        batch = planned.batch;
        sample.wallet_plan_batch_logical_bytes = planned.stats.batch_logical_bytes;
        sample.wallet_plan_overlay_logical_bytes = planned.stats.overlay_logical_bytes;
        sample.wallet_point_reads = planned.stats.point_reads;
        pending = Some(planned.pending);
        wallet_ops = planned.ops;
    }
    let header_bytes = WriteBatch::default().size_in_bytes() as u64;
    let after_wallet = batch.size_in_bytes() as u64;
    sample.wallet_batch_bytes = after_wallet - header_bytes;
    let wallet_planned = Instant::now();
    sample.wallet_plan_seconds = (wallet_planned - canonical_prepared).as_secs_f64();
    store.spike_put_prepared_live_replacement(&prepared, &mut batch)?;
    let after_canonical = batch.size_in_bytes() as u64;
    sample.canonical_batch_bytes = after_canonical - after_wallet;
    let put_done = Instant::now();
    sample.canonical_put_seconds = (put_done - wallet_planned).as_secs_f64();

    // Density lanes, inside the same synced batch.
    let mut retained: Vec<LaneRow> = Vec::new();
    let mut mirror_overlay = None;
    if lanes.density_lanes > 0 {
        let db = Arc::clone(&store.spike_bounded_open().db);
        let mut real: Vec<LaneRow> = Vec::new();
        prepared.for_each_block_scaled_op(|family, key, encoded| {
            real.push((family, key.to_vec(), encoded.map(<[u8]>::to_vec)));
        });
        sample.scaled_canonical_ops_per_lane = real.len() as u64;
        retained.reserve(real.len() * usize::from(lanes.density_lanes));
        for lane in 1..=lanes.density_lanes {
            for (family, key, encoded) in &real {
                retained.push((*family, lane_key(key, lane), encoded.clone()));
            }
        }
        drop(real);
        let mut handles = std::collections::HashMap::new();
        for (family, key, encoded) in &retained {
            sample.retained_lane_bytes +=
                (key.len() + encoded.as_ref().map_or(0, Vec::len)) as u64;
            if !handles.contains_key(family) {
                let handle = db
                    .cf_handle(lanes.names.get(*family)?)
                    .ok_or_else(|| BenchError::spike(format!("missing mirror of {family}")))?;
                handles.insert(*family, handle);
            }
            let handle = handles
                .get(family)
                .ok_or_else(|| BenchError::spike("mirror handle lost"))?;
            match encoded {
                Some(encoded) => {
                    batch.put_cf(handle, key, encoded);
                    sample.mirror_counts.puts += 1;
                }
                None => {
                    batch.delete_cf(handle, key);
                    sample.mirror_counts.deletes += 1;
                }
            }
        }
        drop(handles);
        let after_canonical_lanes = batch.size_in_bytes() as u64;
        sample.mirror_canonical_batch_bytes = after_canonical_lanes - after_canonical;
        if let Some(ops) = wallet_ops.take() {
            let overlay = mirror_overlay.insert(SpikeMirrorPlanOverlay::default());
            let work = MirrorWork {
                canonical_rows: Vec::new(),
                wallet: Some(ops),
            };
            let counts = apply_lanes(
                &db,
                &lanes.names,
                &mut batch,
                &work,
                1..=lanes.density_lanes,
                MirrorFamilies::Wallet,
                true,
                Some(&mut lanes.accumulator),
                Some(&mut *overlay),
            )?;
            sample.mirror_counts.puts += counts.puts;
            sample.mirror_counts.deletes += counts.deletes;
            sample.mirror_counts.reads += counts.reads;
            sample.mirror_counts.read_hits += counts.read_hits;
            sample.mirror_wallet_batch_bytes = batch.size_in_bytes() as u64 - after_canonical_lanes;
            sample.mirror_wallet_plan_batch_logical_bytes = overlay.batch_logical_bytes();
            sample.mirror_wallet_plan_overlay_logical_bytes = overlay.overlay_logical_bytes();
            // Held until the write returns, like the planner's own row copies.
            wallet_ops = work.wallet;
        }
    }
    let mirrored = Instant::now();
    sample.mirror_seconds = (mirrored - put_done).as_secs_f64();
    sample.scaled_wallet_plan_logical_bytes = sample.wallet_plan_batch_logical_bytes
        + sample.wallet_plan_overlay_logical_bytes
        + sample.mirror_wallet_plan_batch_logical_bytes
        + sample.mirror_wallet_plan_overlay_logical_bytes;
    sample.batch_bytes = batch.size_in_bytes() as u64;
    sample.batch_operations = batch.len() as u64;
    let mirror_bytes = sample.mirror_canonical_batch_bytes + sample.mirror_wallet_batch_bytes;
    sample.effective_byte_multiplier =
        sample.batch_bytes as f64 / (sample.batch_bytes - mirror_bytes).max(1) as f64;
    let (store, _fence, timing) = store.spike_commit_prepared_live_replacement(prepared, batch)?;
    drop(retained);
    drop(mirror_overlay);
    drop(wallet_ops);
    sample.synced_write_seconds = timing.write.as_secs_f64();
    sample.canonical_readback_seconds = timing.readback.as_secs_f64();
    let adopt_started = Instant::now();
    if let (Some(wallet), Some(pending)) = (wallet.as_mut(), pending) {
        wallet.spike_complete_planned_transition(pending)?;
    }
    let finished = Instant::now();
    sample.wallet_adopt_seconds = (finished - adopt_started).as_secs_f64();
    sample.commit_seconds = (finished - started).as_secs_f64();
    Ok((store, wallet, sample, lanes))
}
