//! Tree-state reconstruction latency at the heights wallets anchor on.
//!
//! A wallet that scans in 100-block rounds anchors one block below each
//! construction checkpoint, at `k * 100 - 1`. A tree state there has no stored
//! checkpoint, so the reader replays the 99 compact blocks above the
//! checkpoint below it, the most any reconstruction replays. This command
//! measures that case through the admitted canonical secondary API. An explicit
//! height file instead measures every selected read, including stored states.

use std::{fs, path::PathBuf};

use clap::Args;
use serde::Serialize;
use sha2::{Digest, Sha256};
use zinder_bench::BenchError;
use zinder_core::{BlockHeight, ChainTipMetadata, NetworkUpgradeActivations};
use zinder_store::{
    RocksDbCanonicalSecondary, RocksDbStatisticsLevel, TREE_STATE_CHECKPOINT_STRIDE,
    TreeStateLookup, TreeStateReplayOutcome,
};

use crate::rocksdb_read_bench::{ReadCounters, ReadEnvironment, ReadStoreArgs, TimedRead, measure};

const DEFAULT_MAX_ANCHORS: u32 = 64;

/// CLI contract for measuring tree-state reconstructions from one READY store.
#[derive(Args)]
pub(crate) struct RocksDbTreeStateReplayArgs {
    #[command(flatten)]
    pub(crate) store: ReadStoreArgs,
    /// Lowest height an anchor may take. Defaults to the first retained block.
    #[arg(long = "from-height")]
    from_height: Option<u32>,
    /// Highest height an anchor may take. Defaults to the visible tip.
    #[arg(long = "to-height")]
    to_height: Option<u32>,
    /// Most anchors measured, spread evenly over the window.
    #[arg(long = "max-anchors", default_value_t = DEFAULT_MAX_ANCHORS)]
    max_anchors: u32,
    /// Measure every listed height in file order, including stored checkpoints.
    #[arg(long, conflicts_with_all = ["from_height", "to_height", "max_anchors"])]
    heights_file: Option<PathBuf>,
}

/// Reconstruction latency at every measured anchor of one READY store.
#[derive(Serialize)]
pub(crate) struct RocksDbTreeStateReplayReport {
    contract_identity: &'static str,
    report_format_version: u16,
    #[serde(flatten)]
    environment: ReadEnvironment,
    visible_tip_height: u32,
    heights_file_sha256: Option<String>,
    #[serde(flatten)]
    measurement: TreeStateReplayMeasurement,
}

/// Anchors measured in one window and their latency summaries.
#[derive(Debug, Serialize)]
pub(crate) struct TreeStateReplayMeasurement {
    read_path: &'static str,
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
    source: TreeStateSource,
    seed_checkpoint_height: u32,
    replayed_blocks: u32,
    replayed_note_commitments: u64,
    cold_seconds: f64,
    warm_seconds: f64,
    cold_counters: ReadCounters,
    warm_counters: ReadCounters,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TreeStateSource {
    Stored,
    Replayed,
}

/// Measures reconstructions at the anchors of one admitted READY store.
pub(crate) fn run_rocksdb_tree_state_replay(
    args: &RocksDbTreeStateReplayArgs,
) -> Result<RocksDbTreeStateReplayReport, BenchError> {
    let explicit = args
        .heights_file
        .as_ref()
        .map(|path| {
            let text = fs::read_to_string(path).map_err(|source| BenchError::io(path, source))?;
            Ok::<_, BenchError>((
                parse_heights(&text)?,
                hex::encode(Sha256::digest(text.as_bytes())),
            ))
        })
        .transpose()?;
    let (store, environment) = args.store.prepare()?;
    let (secondary, _) = store.open("tree-state", None)?;
    let ready = secondary.ready_evidence();
    let measurement = measure_tree_state_replays(
        &secondary,
        &store.activations,
        store.budget.statistics_level,
        ReplayWindow {
            from_height: args.from_height,
            to_height: args.to_height,
            max_anchors: args.max_anchors,
        },
        explicit.as_ref().map(|(heights, _)| heights.as_slice()),
    )?;
    Ok(RocksDbTreeStateReplayReport {
        contract_identity: "rocksdb-tree-state-replay",
        report_format_version: 3,
        environment,
        visible_tip_height: ready.visible_tip.height.value(),
        heights_file_sha256: explicit.map(|(_, digest)| digest),
        measurement,
    })
}

/// Heights an anchor may take, and how many anchors to measure.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReplayWindow {
    pub(crate) from_height: Option<u32>,
    pub(crate) to_height: Option<u32>,
    pub(crate) max_anchors: u32,
}

/// Reads the tree state at sampled or explicit heights twice in a row.
///
/// The first read uses the secondary's current cache state. The second read,
/// run at once, can reuse the cache the first one filled.
/// Measuring the warm replay before the next anchor keeps later replays from
/// evicting the blocks of an earlier one when the sampled ranges exceed the
/// secondary's block cache.
pub(crate) fn measure_tree_state_replays(
    secondary: &RocksDbCanonicalSecondary,
    activations: &NetworkUpgradeActivations,
    level: RocksDbStatisticsLevel,
    window: ReplayWindow,
    explicit: Option<&[u32]>,
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
    let (replays, stored_checkpoint_anchors_skipped) = select_anchors(
        secondary,
        window_start,
        window_end,
        window.max_anchors,
        explicit,
    )?;
    if replays.is_empty() {
        return Err(BenchError::invalid_argument(
            "the window holds no anchor that needs a tree-state reconstruction",
        ));
    }
    let mut anchors = Vec::with_capacity(replays.len());
    for anchor in &replays {
        let cold = time_anchor(secondary, activations, level, anchor)?;
        let warm = time_anchor(secondary, activations, level, anchor)?;
        anchors.push(AnchorMeasurement {
            height: anchor.height,
            source: cold.rows.source,
            seed_checkpoint_height: cold.rows.seed_checkpoint_height,
            replayed_blocks: cold.rows.replayed_blocks,
            replayed_note_commitments: cold.rows.replayed_note_commitments,
            cold_seconds: cold.seconds,
            warm_seconds: warm.seconds,
            cold_counters: cold.counters,
            warm_counters: warm.counters,
        });
    }
    Ok(TreeStateReplayMeasurement {
        read_path: if explicit.is_some() {
            "wallet-query-store"
        } else {
            "validated-checkpoint"
        },
        window_start_height: window_start,
        window_end_height: window_end,
        stored_checkpoint_anchors_skipped,
        cold_seconds: LatencySeconds::of(anchors.iter().map(|anchor| anchor.cold_seconds)),
        warm_seconds: LatencySeconds::of(anchors.iter().map(|anchor| anchor.warm_seconds)),
        anchors,
    })
}

fn parse_heights(text: &str) -> Result<Vec<u32>, BenchError> {
    let heights = text
        .lines()
        .map(|line| {
            line.trim().parse::<u32>().map_err(|_| {
                BenchError::invalid_argument("--heights-file requires one u32 height per line")
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if heights.is_empty() {
        return Err(BenchError::invalid_argument(
            "--heights-file must not be empty",
        ));
    }
    Ok(heights)
}

struct SelectedAnchor {
    height: u32,
    validated_seed: Option<(u32, ChainTipMetadata)>,
}

struct MeasuredTreeState {
    source: TreeStateSource,
    seed_checkpoint_height: u32,
    replayed_blocks: u32,
    replayed_note_commitments: u64,
}

fn select_anchors(
    secondary: &RocksDbCanonicalSecondary,
    start: u32,
    end: u32,
    max: u32,
    explicit: Option<&[u32]>,
) -> Result<(Vec<SelectedAnchor>, u32), BenchError> {
    let heights = explicit.map_or_else(|| anchor_heights(start, end).collect(), <[u32]>::to_vec);
    if (explicit.is_some() && heights.is_empty())
        || heights.iter().any(|height| !(start..=end).contains(height))
    {
        return Err(BenchError::invalid_argument(
            "selected heights must be nonempty and within the retained window",
        ));
    }
    if explicit.is_some() {
        return Ok((
            heights
                .into_iter()
                .map(|height| SelectedAnchor {
                    height,
                    validated_seed: None,
                })
                .collect(),
            0,
        ));
    }
    let mut selected = Vec::new();
    let mut skipped = 0_u32;
    for height in heights {
        let seed = secondary
            .tree_state_checkpoint_at_or_before(BlockHeight::new(height))?
            .ok_or_else(|| {
                BenchError::report_format(format!("anchor {height} has no checkpoint below it"))
            })?;
        let seed_height = seed.block_id().height.value();
        if explicit.is_none() && seed_height == height {
            skipped = skipped.saturating_add(1);
        } else {
            selected.push(SelectedAnchor {
                height,
                validated_seed: Some((seed_height, seed.tip_metadata())),
            });
        }
    }
    Ok((spread_evenly(selected, max), skipped))
}

fn time_anchor(
    secondary: &RocksDbCanonicalSecondary,
    activations: &NetworkUpgradeActivations,
    level: RocksDbStatisticsLevel,
    anchor: &SelectedAnchor,
) -> Result<TimedRead<MeasuredTreeState>, BenchError> {
    if let Some((seed_checkpoint_height, seed_sizes)) = anchor.validated_seed {
        let sample = time_reconstruction(secondary, activations, anchor.height, level)?;
        return Ok(TimedRead {
            rows: MeasuredTreeState {
                source: TreeStateSource::Replayed,
                seed_checkpoint_height,
                replayed_blocks: anchor.height.saturating_sub(seed_checkpoint_height),
                replayed_note_commitments: commitments_between(seed_sizes, sample.rows),
            },
            seconds: sample.seconds,
            counters: sample.counters,
        });
    }
    measure(secondary, level, || {
        let lookup = secondary
            .tree_state_lookup(BlockHeight::new(anchor.height), activations)?
            .ok_or_else(|| BenchError::report_format("selected height has no served tree state"))?;
        let outcome = match lookup {
            TreeStateLookup::Stored(stored) => TreeStateReplayOutcome::Stored(stored),
            TreeStateLookup::Replay(replay) => secondary
                .replay_tree_state(&replay, activations)?
                .ok_or_else(|| {
                BenchError::report_format("selected replay has no served tree state")
            })?,
        };
        Ok(match outcome {
            TreeStateReplayOutcome::Stored(stored) => MeasuredTreeState {
                source: TreeStateSource::Stored,
                seed_checkpoint_height: stored.block_id().height.value(),
                replayed_blocks: 0,
                replayed_note_commitments: 0,
            },
            TreeStateReplayOutcome::Replayed { replay, tree_state } => MeasuredTreeState {
                source: TreeStateSource::Replayed,
                seed_checkpoint_height: replay.seed().block_id.height.value(),
                replayed_blocks: replay.replayed_blocks(),
                replayed_note_commitments: commitments_between(
                    replay.seed().tip_metadata(),
                    tree_state.tip_metadata(),
                ),
            },
        })
    })
}

fn time_reconstruction(
    secondary: &RocksDbCanonicalSecondary,
    activations: &NetworkUpgradeActivations,
    height: u32,
    level: RocksDbStatisticsLevel,
) -> Result<TimedRead<ChainTipMetadata>, BenchError> {
    measure(secondary, level, || {
        secondary
            .tree_state_checkpoint_at(BlockHeight::new(height), activations)?
            .map(|checkpoint| checkpoint.tip_metadata())
            .ok_or_else(|| {
                BenchError::report_format(format!(
                    "anchor {height} has no tree state to reconstruct"
                ))
            })
    })
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
    use rust_rocksdb::{PerfMetric, statistics::Ticker};
    use zinder_core::{BlockHeight, Network, NetworkUpgradeActivations};
    use zinder_store::{
        RocksDbCanonicalStore, ServedTreeState, TreeStateCheckpointSelection, TreeStateLookup,
    };
    use zinder_testkit::{
        CanonicalBaseline, CanonicalWriterFixture, ChainFixture,
        sample_regtest_upgrade_activations, with_note_commitments,
    };

    use super::{
        ReplayWindow, TreeStateSource, anchor_heights, measure_tree_state_replays, parse_heights,
        spread_evenly,
    };

    #[test]
    fn explicit_height_file_preserves_order_and_rejects_invalid_input()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(parse_heights("199\n100\n199\n")?, [199, 100, 199]);
        for invalid in ["", "\n", "1\n\n2", "-1", "4294967296", "99 100"] {
            assert!(parse_heights(invalid).is_err());
        }
        Ok(())
    }

    #[test]
    fn explicit_sparse_and_mixed_reads_retain_attribution() -> Result<(), Box<dyn std::error::Error>>
    {
        assert_explicit_fixture(false)
    }

    #[test]
    fn explicit_dense_and_mixed_reads_retain_attribution() -> Result<(), Box<dyn std::error::Error>>
    {
        assert_explicit_fixture(true)
    }

    fn assert_explicit_fixture(dense: bool) -> Result<(), Box<dyn std::error::Error>> {
        let activations = sample_regtest_upgrade_activations();
        let chain = with_note_commitments(
            &ChainFixture::new(Network::ZcashRegtest).extend_blocks(250),
            &activations,
        )?;
        let writer =
            CanonicalWriterFixture::publish(&CanonicalBaseline::new(&chain), &activations)?;
        if dense {
            densify_selected_fixture_heights(writer.canonical_primary()?, &activations)?;
        }
        let mut store = writer.into_serving_store_fixture()?;
        let (canonical, _wallet) = store.take_readers()?;
        for heights in [
            &[99, 199][..],
            &[100, 200][..],
            &[199, 100, 99, 250, 100][..],
        ] {
            let measurement = measure_tree_state_replays(
                &canonical,
                &activations,
                zinder_store::RocksDbStatisticsLevel::Tickers,
                ReplayWindow {
                    from_height: None,
                    to_height: None,
                    max_anchors: 1,
                },
                Some(heights),
            )?;
            assert_explicit_reads(&measurement, heights, dense)?;
        }
        assert!(
            measure_tree_state_replays(
                &canonical,
                &activations,
                zinder_store::RocksDbStatisticsLevel::Tickers,
                ReplayWindow {
                    from_height: None,
                    to_height: None,
                    max_anchors: 64
                },
                Some(&[251]),
            )
            .is_err()
        );
        Ok(())
    }

    fn densify_selected_fixture_heights(
        store: &RocksDbCanonicalStore,
        activations: &NetworkUpgradeActivations,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let checkpoints = [99, 100, 199, 200, 250]
            .into_iter()
            .map(|height| store.tree_state_checkpoint_at(BlockHeight::new(height), activations))
            .collect::<Result<Vec<_>, _>>()?;
        let selection = TreeStateCheckpointSelection::EveryBlock;
        let mut from = BlockHeight::new(0);
        let mut batches = 0;
        let mut rows = 0;
        while let Some(stride) = store.next_tree_state_stride(from, selection)? {
            let densified = store.densify_tree_state_stride(stride, selection, activations)?;
            rows += store.commit_tree_state_stride(&densified)?.rows_written;
            batches += 1;
            from = stride.end_height();
        }
        assert_eq!((batches, rows), (2, 198));
        for checkpoint in checkpoints {
            let checkpoint = checkpoint.ok_or("fixture checkpoint is absent")?;
            let Some(TreeStateLookup::Stored(stored)) =
                store.tree_state_lookup(checkpoint.block_id.height, activations)?
            else {
                return Err("selected height was not densified".into());
            };
            assert_eq!(stored, ServedTreeState::from(&checkpoint));
        }
        Ok(())
    }

    fn assert_explicit_reads(
        measurement: &super::TreeStateReplayMeasurement,
        heights: &[u32],
        dense: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(measurement.read_path, "wallet-query-store");
        assert_eq!(measurement.stored_checkpoint_anchors_skipped, 0);
        assert_eq!(
            measurement
                .anchors
                .iter()
                .map(|a| a.height)
                .collect::<Vec<_>>(),
            heights
        );
        for anchor in &measurement.anchors {
            let stored = dense || anchor.height % 100 == 0 || anchor.height == 250;
            assert_eq!(
                anchor.source,
                if stored {
                    TreeStateSource::Stored
                } else {
                    TreeStateSource::Replayed
                }
            );
            assert_eq!(anchor.replayed_blocks == 0, stored);
            assert_eq!(anchor.replayed_note_commitments == 0, stored);
            let encoded = serde_json::to_value(anchor)?;
            assert!(
                encoded["cold_counters"]["ticker_deltas"][Ticker::NumberKeysRead.name()].is_u64()
            );
            assert!(
                encoded["cold_counters"]["perf_context"][PerfMetric::BlockReadCount.name()]
                    .is_u64()
            );
            assert!(
                encoded["warm_counters"]["perf_context"][PerfMetric::BlockReadByte.name()].is_u64()
            );
        }
        Ok(())
    }

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
            zinder_store::RocksDbStatisticsLevel::Tickers,
            ReplayWindow {
                from_height: None,
                to_height: None,
                max_anchors: 64,
            },
            None,
        )?;

        assert_eq!(measurement.read_path, "validated-checkpoint");
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
