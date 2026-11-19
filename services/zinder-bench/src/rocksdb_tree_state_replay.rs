//! Tree-state reconstruction latency at the heights wallets anchor on.
//!
//! A wallet that scans in 100-block rounds anchors one block below each
//! construction checkpoint, at `k * 100 - 1`. A tree state there has no stored
//! checkpoint, so the reader replays the 99 compact blocks above the
//! checkpoint below it, the most any reconstruction replays. This command
//! measures that case through the admitted canonical secondary API, over the
//! whole retained range or a chosen window such as a dense spending range.

use std::{
    fs,
    path::PathBuf,
    time::{Duration, Instant},
};

use clap::Args;
use serde::Serialize;
use zinder_bench::{BenchError, fixture::FixtureManifest};
use zinder_core::{BlockHeight, ChainTipMetadata, NetworkUpgradeActivations};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalSecondary,
    RocksDbResourceBudget, TREE_STATE_CHECKPOINT_STRIDE,
};

const DEFAULT_SUPPORTED_REORG_DEPTH: u32 = zinder_store::DEFAULT_REORG_WINDOW_BLOCKS;
const DEFAULT_MAX_ANCHORS: u32 = 64;

/// CLI contract for measuring tree-state reconstructions from one READY store.
#[derive(Args)]
pub(crate) struct RocksDbTreeStateReplayArgs {
    /// Directory containing the fixture manifest used to construct the store.
    #[arg(long)]
    fixture: PathBuf,
    /// Existing READY canonical primary store.
    #[arg(long = "canonical-store")]
    canonical_store: PathBuf,
    /// Absent directory used for process-local secondary metadata.
    #[arg(long = "secondary-root")]
    secondary_root: PathBuf,
    /// Exact source revision of the measured binary.
    #[arg(long = "software-revision")]
    software_revision: String,
    /// Persisted canonical replacement depth.
    #[arg(long, default_value_t = DEFAULT_SUPPORTED_REORG_DEPTH)]
    supported_reorg_depth: u32,
    /// Lowest height an anchor may take. Defaults to the first retained block.
    #[arg(long = "from-height")]
    from_height: Option<u32>,
    /// Highest height an anchor may take. Defaults to the visible tip.
    #[arg(long = "to-height")]
    to_height: Option<u32>,
    /// Most anchors measured, spread evenly over the window.
    #[arg(long = "max-anchors", default_value_t = DEFAULT_MAX_ANCHORS)]
    max_anchors: u32,
    /// Write the JSON report to this path instead of stdout.
    #[arg(long)]
    report: Option<PathBuf>,
}

/// Report and optional output path produced by the reconstruction measurement.
pub(crate) struct RocksDbTreeStateReplayOutput {
    pub(crate) report: RocksDbTreeStateReplayReport,
    pub(crate) report_path: Option<PathBuf>,
}

/// Reconstruction latency at every measured anchor of one READY store.
#[derive(Serialize)]
pub(crate) struct RocksDbTreeStateReplayReport {
    contract_identity: &'static str,
    report_format_version: u16,
    software_revision: String,
    fixture_manifest_digest_sha256: String,
    canonical_store: String,
    visible_tip_height: u32,
    #[serde(flatten)]
    measurement: TreeStateReplayMeasurement,
}

/// Anchors measured in one window and their latency summaries.
#[derive(Debug, Serialize)]
pub(crate) struct TreeStateReplayMeasurement {
    window_start_height: u32,
    window_end_height: u32,
    /// Anchors in the window that hold a stored checkpoint, as every
    /// live-followed block does, and so need no reconstruction.
    stored_checkpoint_anchors_skipped: u32,
    cold_seconds: LatencySeconds,
    warm_seconds: LatencySeconds,
    anchors: Vec<AnchorMeasurement>,
}

/// Nearest-rank latency percentiles over the measured anchors, in seconds.
#[derive(Debug, Serialize)]
struct LatencySeconds {
    p50: f64,
    p90: f64,
    p99: f64,
    max: f64,
}

#[derive(Debug, Serialize)]
struct AnchorMeasurement {
    height: u32,
    seed_checkpoint_height: u32,
    replayed_blocks: u32,
    replayed_note_commitments: u64,
    cold_seconds: f64,
    warm_seconds: f64,
}

/// Measures reconstructions at the anchors of one admitted READY store.
pub(crate) fn run_rocksdb_tree_state_replay(
    args: RocksDbTreeStateReplayArgs,
) -> Result<RocksDbTreeStateReplayOutput, BenchError> {
    if args.software_revision.trim().is_empty() {
        return Err(BenchError::invalid_argument(
            "--software-revision must not be empty",
        ));
    }
    let manifest = FixtureManifest::read(&args.fixture)?;
    let activations = manifest.activations_typed()?;
    let canonical_store = fs::canonicalize(&args.canonical_store)
        .map_err(|source| BenchError::io(&args.canonical_store, source))?;
    if args.secondary_root.exists() {
        return Err(BenchError::invalid_argument(
            "--secondary-root must name an absent directory",
        ));
    }
    let secondary = RocksDbCanonicalSecondary::open_ready(
        &canonical_store,
        &args.secondary_root,
        &activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(args.supported_reorg_depth)?,
        RocksDbResourceBudget::canonical_reader_defaults(),
    )?;
    let ready = secondary.ready_evidence();
    let measurement = measure_tree_state_replays(
        &secondary,
        &activations,
        ReplayWindow {
            from_height: args.from_height,
            to_height: args.to_height,
            max_anchors: args.max_anchors,
        },
    )?;
    Ok(RocksDbTreeStateReplayOutput {
        report: RocksDbTreeStateReplayReport {
            contract_identity: "rocksdb-tree-state-replay",
            report_format_version: 1,
            software_revision: args.software_revision,
            fixture_manifest_digest_sha256: manifest.digest_sha256()?,
            canonical_store: canonical_store.to_string_lossy().into_owned(),
            visible_tip_height: ready.visible_tip.height.value(),
            measurement,
        },
        report_path: args.report,
    })
}

/// Heights an anchor may take, and how many anchors to measure.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReplayWindow {
    pub(crate) from_height: Option<u32>,
    pub(crate) to_height: Option<u32>,
    pub(crate) max_anchors: u32,
}

/// Reconstructs the tree state at each anchor of `window` twice in a row.
///
/// The first reconstruction reads the anchor's compact blocks cold, and the
/// second, run at once, reads them from the cache the first one filled.
/// Measuring the warm replay before the next anchor keeps later replays from
/// evicting the blocks of an earlier one when the sampled ranges exceed the
/// secondary's block cache.
pub(crate) fn measure_tree_state_replays(
    secondary: &RocksDbCanonicalSecondary,
    activations: &NetworkUpgradeActivations,
    window: ReplayWindow,
) -> Result<TreeStateReplayMeasurement, BenchError> {
    let ready = secondary.ready_evidence();
    let window_start = window
        .from_height
        .unwrap_or(0)
        .max(ready.first_retained_block.height.value());
    let window_end = window
        .to_height
        .unwrap_or(u32::MAX)
        .min(ready.visible_tip.height.value());
    let mut replays = Vec::new();
    let mut stored_checkpoint_anchors_skipped = 0_u32;
    for height in anchor_heights(window_start, window_end) {
        let seed = secondary
            .tree_state_checkpoint_at_or_before(BlockHeight::new(height))?
            .ok_or_else(|| {
                BenchError::report_format(format!("anchor {height} has no checkpoint below it"))
            })?;
        let seed_height = seed.block_id().height.value();
        if seed_height == height {
            stored_checkpoint_anchors_skipped = stored_checkpoint_anchors_skipped.saturating_add(1);
        } else {
            replays.push((height, seed_height, seed.tip_metadata()));
        }
    }
    let replays = spread_evenly(replays, window.max_anchors);
    if replays.is_empty() {
        return Err(BenchError::invalid_argument(
            "the window holds no anchor that needs a tree-state reconstruction",
        ));
    }
    let mut anchors = Vec::with_capacity(replays.len());
    for (height, seed_checkpoint_height, seed_sizes) in &replays {
        let (cold, sizes) = time_reconstruction(secondary, activations, *height)?;
        let (warm, _) = time_reconstruction(secondary, activations, *height)?;
        anchors.push(AnchorMeasurement {
            height: *height,
            seed_checkpoint_height: *seed_checkpoint_height,
            replayed_blocks: height.saturating_sub(*seed_checkpoint_height),
            replayed_note_commitments: commitments_between(*seed_sizes, sizes),
            cold_seconds: cold.as_secs_f64(),
            warm_seconds: warm.as_secs_f64(),
        });
    }
    Ok(TreeStateReplayMeasurement {
        window_start_height: window_start,
        window_end_height: window_end,
        stored_checkpoint_anchors_skipped,
        cold_seconds: LatencySeconds::of(anchors.iter().map(|anchor| anchor.cold_seconds)),
        warm_seconds: LatencySeconds::of(anchors.iter().map(|anchor| anchor.warm_seconds)),
        anchors,
    })
}

fn time_reconstruction(
    secondary: &RocksDbCanonicalSecondary,
    activations: &NetworkUpgradeActivations,
    height: u32,
) -> Result<(Duration, ChainTipMetadata), BenchError> {
    let started = Instant::now();
    let checkpoint = secondary
        .tree_state_checkpoint_at(BlockHeight::new(height), activations)?
        .ok_or_else(|| {
            BenchError::report_format(format!("anchor {height} has no tree state to reconstruct"))
        })?;
    Ok((started.elapsed(), checkpoint.tip_metadata()))
}

/// Heights one block below each checkpoint stride boundary within
/// `start..=end`.
fn anchor_heights(start: u32, end: u32) -> impl Iterator<Item = u32> {
    (start.div_ceil(TREE_STATE_CHECKPOINT_STRIDE)..)
        .map_while(|stride| stride.checked_mul(TREE_STATE_CHECKPOINT_STRIDE))
        .filter_map(|boundary| boundary.checked_sub(1))
        .skip_while(move |anchor| *anchor < start)
        .take_while(move |anchor| *anchor <= end)
}

/// Keeps at most `max` entries, evenly spaced and starting with the first.
fn spread_evenly<Entry>(entries: Vec<Entry>, max: u32) -> Vec<Entry> {
    let max = usize::try_from(max).unwrap_or(usize::MAX);
    let len = entries.len();
    if len <= max {
        return entries;
    }
    let kept = (0..max)
        .map(|slot| slot.saturating_mul(len) / max)
        .collect::<Vec<_>>();
    entries
        .into_iter()
        .enumerate()
        .filter(|(index, _)| kept.binary_search(index).is_ok())
        .map(|(_, entry)| entry)
        .collect()
}

fn commitments_between(before: ChainTipMetadata, after: ChainTipMetadata) -> u64 {
    [
        after
            .sapling_commitment_tree_size
            .saturating_sub(before.sapling_commitment_tree_size),
        after
            .orchard_commitment_tree_size
            .saturating_sub(before.orchard_commitment_tree_size),
        after
            .ironwood_commitment_tree_size
            .saturating_sub(before.ironwood_commitment_tree_size),
    ]
    .into_iter()
    .map(u64::from)
    .sum()
}

impl LatencySeconds {
    fn of(samples: impl Iterator<Item = f64>) -> Self {
        let mut sorted = samples.collect::<Vec<_>>();
        sorted.sort_by(f64::total_cmp);
        let nearest_rank = |percentile: usize| {
            let rank = sorted.len().saturating_mul(percentile).div_ceil(100);
            sorted
                .get(rank.saturating_sub(1))
                .copied()
                .unwrap_or_default()
        };
        Self {
            p50: nearest_rank(50),
            p90: nearest_rank(90),
            p99: nearest_rank(99),
            max: sorted.last().copied().unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use zinder_core::Network;
    use zinder_testkit::{
        CanonicalBaseline, CanonicalWriterFixture, ChainFixture,
        sample_regtest_upgrade_activations, with_note_commitments,
    };

    use super::{ReplayWindow, anchor_heights, measure_tree_state_replays, spread_evenly};

    #[test]
    fn anchors_sit_one_block_below_each_checkpoint_stride() {
        assert_eq!(anchor_heights(1, 350).collect::<Vec<_>>(), [99, 199, 299]);
        assert_eq!(anchor_heights(150, 199).collect::<Vec<_>>(), [199]);
        assert_eq!(anchor_heights(200, 298).count(), 0);
    }

    #[test]
    fn spreading_keeps_evenly_spaced_entries_up_to_the_limit() {
        let entries = (0..10).collect::<Vec<_>>();
        assert_eq!(spread_evenly(entries.clone(), 20), entries);
        assert_eq!(spread_evenly(entries.clone(), 5), [0, 2, 4, 6, 8]);
        assert_eq!(spread_evenly(entries.clone(), 1), [0]);
        assert_eq!(spread_evenly(entries, 0), [] as [i32; 0]);
    }

    #[test]
    fn measurement_replays_the_full_stride_below_each_construction_checkpoint()
    -> Result<(), Box<dyn std::error::Error>> {
        let activations = sample_regtest_upgrade_activations();
        let chain = with_note_commitments(
            &ChainFixture::new(Network::ZcashRegtest).extend_blocks(250),
            &activations,
        )?;
        let mut store =
            CanonicalWriterFixture::publish(&CanonicalBaseline::new(&chain), &activations)?
                .into_serving_store_fixture()?;
        let (canonical, _wallet) = store.take_readers()?;

        let measurement = measure_tree_state_replays(
            &canonical,
            &activations,
            ReplayWindow {
                from_height: None,
                to_height: None,
                max_anchors: 64,
            },
        )?;

        assert_eq!(measurement.stored_checkpoint_anchors_skipped, 0);
        assert_eq!(
            measurement
                .anchors
                .iter()
                .map(|anchor| (anchor.height, anchor.replayed_blocks))
                .collect::<Vec<_>>(),
            [(99, 99), (199, 99)]
        );
        assert!(
            measurement
                .anchors
                .iter()
                .all(|anchor| anchor.replayed_note_commitments > 0)
        );
        Ok(())
    }
}
