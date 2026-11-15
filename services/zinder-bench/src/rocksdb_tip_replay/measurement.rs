//! Block workload counts, workload strata, and nearest-rank latency summaries.

use serde::{Deserialize, Serialize};
use zinder_core::CanonicalBlockFacts;

/// Percentile method named in every report and comparison.
pub(super) const PERCENTILE_METHOD: &str = "nearest-rank";

/// A block is dense when it has more than this many transparent inputs or outputs.
const DENSE_BLOCK_TRANSPARENT_ROWS: u32 = 1_000;
/// Heights after a dense block whose near-empty blocks form their own stratum.
const AFTER_DENSE_WINDOW_HEIGHTS: u32 = 1_000;
/// Most non-coinbase transparent inputs plus outputs a near-empty block holds.
const NEAR_EMPTY_MAX_TRANSPARENT_ROWS: u32 = 10;
/// Fewest non-coinbase transparent inputs in an input-heavy block.
const INPUT_HEAVY_MIN_INPUTS: u32 = 1_000;
/// Fewest transparent outputs in a fan-out block, which also has no inputs.
const FAN_OUT_MIN_OUTPUTS: u32 = 1_000;

/// Transparent workload of one block, counted from its canonical facts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct BlockWorkload {
    /// Transactions, the coinbase included.
    pub(super) transactions: u32,
    /// Non-coinbase transparent inputs (a coinbase input is not a transparent input).
    pub(super) transparent_inputs: u32,
    /// Transparent outputs, coinbase outputs included.
    pub(super) transparent_outputs: u32,
    /// Transparent outputs of the coinbase transaction.
    pub(super) coinbase_transparent_outputs: u32,
}

impl BlockWorkload {
    pub(super) fn from_facts(facts: &CanonicalBlockFacts) -> Self {
        let mut workload = Self {
            transactions: saturating_u32(facts.transactions.len()),
            ..Self::default()
        };
        for transaction in &facts.transactions {
            let outputs = saturating_u32(transaction.transparent_outputs.len());
            workload.transparent_inputs = workload
                .transparent_inputs
                .saturating_add(saturating_u32(transaction.transparent_inputs.len()));
            workload.transparent_outputs = workload.transparent_outputs.saturating_add(outputs);
            if transaction.public_facts.is_coinbase {
                workload.coinbase_transparent_outputs = workload
                    .coinbase_transparent_outputs
                    .saturating_add(outputs);
            }
        }
        workload
    }

    const fn non_coinbase_transparent_rows(self) -> u32 {
        self.transparent_inputs.saturating_add(
            self.transparent_outputs
                .saturating_sub(self.coinbase_transparent_outputs),
        )
    }

    const fn is_dense(self) -> bool {
        self.transparent_inputs > DENSE_BLOCK_TRANSPARENT_ROWS
            || self.transparent_outputs > DENSE_BLOCK_TRANSPARENT_ROWS
    }
}

/// Workload strata a replayed block can belong to; a block may be in several.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Stratum {
    /// No non-coinbase transparent inputs or outputs.
    NoTransparentActivity,
    /// At most 10 non-coinbase transparent inputs plus outputs, within 1,000
    /// heights after a block with more than 1,000 transparent inputs or outputs.
    NearEmptyAfterDense,
    /// At least 1,000 non-coinbase transparent inputs.
    InputHeavy,
    /// At least 1,000 transparent outputs and no non-coinbase transparent inputs.
    FanOut,
}

/// Numeric stratum rules, recorded in every report.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct StratumRules {
    pub(super) dense_block_more_than_transparent_inputs_or_outputs: u32,
    pub(super) after_dense_window_heights: u32,
    pub(super) near_empty_max_non_coinbase_transparent_rows: u32,
    pub(super) input_heavy_min_transparent_inputs: u32,
    pub(super) fan_out_min_transparent_outputs: u32,
}

pub(super) const STRATUM_RULES: StratumRules = StratumRules {
    dense_block_more_than_transparent_inputs_or_outputs: DENSE_BLOCK_TRANSPARENT_ROWS,
    after_dense_window_heights: AFTER_DENSE_WINDOW_HEIGHTS,
    near_empty_max_non_coinbase_transparent_rows: NEAR_EMPTY_MAX_TRANSPARENT_ROWS,
    input_heavy_min_transparent_inputs: INPUT_HEAVY_MIN_INPUTS,
    fan_out_min_transparent_outputs: FAN_OUT_MIN_OUTPUTS,
};

/// Classifies replayed blocks, in ascending height order.
///
/// `history` holds the retained blocks just before the replay, so a dense
/// block shortly before the first replayed height still marks the near-empty
/// blocks after it.
pub(super) fn classify_strata(
    history: &[(u32, BlockWorkload)],
    replayed: &[(u32, BlockWorkload)],
) -> Vec<Vec<Stratum>> {
    let mut last_dense_height = history
        .iter()
        .rev()
        .find(|(_, workload)| workload.is_dense())
        .map(|(height, _)| *height);
    replayed
        .iter()
        .map(|(height, workload)| {
            let non_coinbase_rows = workload.non_coinbase_transparent_rows();
            let mut strata = Vec::new();
            if non_coinbase_rows == 0 {
                strata.push(Stratum::NoTransparentActivity);
            }
            if non_coinbase_rows <= NEAR_EMPTY_MAX_TRANSPARENT_ROWS
                && last_dense_height.is_some_and(|dense_height| {
                    height.saturating_sub(dense_height) <= AFTER_DENSE_WINDOW_HEIGHTS
                })
            {
                strata.push(Stratum::NearEmptyAfterDense);
            }
            if workload.transparent_inputs >= INPUT_HEAVY_MIN_INPUTS {
                strata.push(Stratum::InputHeavy);
            }
            if workload.transparent_outputs >= FAN_OUT_MIN_OUTPUTS
                && workload.transparent_inputs == 0
            {
                strata.push(Stratum::FanOut);
            }
            if workload.is_dense() {
                last_dense_height = Some(*height);
            }
            strata
        })
        .collect()
}

/// Per-block timers, in milliseconds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(super) struct BlockTimings {
    /// One production follow iteration: fixture source reads, settlement
    /// selection, block preparation, subtree roots, commit, and readiness.
    pub(super) canonical_follow_step: f64,
    /// `zinder_ingest_canonical_live_prepare_seconds` recorded by that iteration.
    pub(super) canonical_prepare: f64,
    /// `zinder_ingest_canonical_live_commit_seconds` recorded by that iteration.
    pub(super) canonical_commit: f64,
    /// Canonical secondary catch-up to the new writer fence.
    pub(super) canonical_secondary_catch_up: f64,
    /// The production wallet transition, including its replay-row reads.
    pub(super) wallet_apply: f64,
    /// Catch-up, retained-event and epoch reads, and the wallet transition.
    pub(super) wallet_step: f64,
}

/// One replayed block: its workload, strata, and timers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct BlockSample {
    pub(super) height: u32,
    pub(super) workload: BlockWorkload,
    pub(super) strata: Vec<Stratum>,
    pub(super) timings_ms: BlockTimings,
}

/// Nearest-rank summary of one timer, in milliseconds.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct LatencySummary {
    pub(super) count: u32,
    pub(super) mean_ms: f64,
    pub(super) p50_ms: f64,
    pub(super) p99_ms: f64,
    pub(super) p999_ms: f64,
    pub(super) max_ms: f64,
    pub(super) total_ms: f64,
}

impl LatencySummary {
    /// Summarizes samples; `None` when there are none.
    pub(super) fn from_samples(samples_ms: &[f64]) -> Option<Self> {
        let mut sorted = samples_ms.to_vec();
        sorted.sort_by(f64::total_cmp);
        let count = u32::try_from(sorted.len()).unwrap_or(u32::MAX);
        let max_ms = *sorted.last()?;
        let total_ms: f64 = sorted.iter().sum();
        Some(Self {
            count,
            mean_ms: total_ms / f64::from(count),
            p50_ms: nearest_rank(&sorted, 5_000)?,
            p99_ms: nearest_rank(&sorted, 9_900)?,
            p999_ms: nearest_rank(&sorted, 9_990)?,
            max_ms,
            total_ms,
        })
    }
}

/// Returns the sample at 1-based rank `ceil(n * q / 10,000)` of ascending samples.
fn nearest_rank(sorted: &[f64], quantile_per_ten_thousand: u64) -> Option<f64> {
    let sample_count = u64::try_from(sorted.len()).ok()?;
    let rank = sample_count
        .checked_mul(quantile_per_ten_thousand)?
        .div_ceil(10_000)
        .max(1);
    sorted
        .get(usize::try_from(rank.checked_sub(1)?).ok()?)
        .copied()
}

/// Summaries of every timer over one set of blocks.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct TimerSummaries {
    pub(super) canonical_follow_step: LatencySummary,
    pub(super) canonical_prepare: LatencySummary,
    pub(super) canonical_commit: LatencySummary,
    pub(super) canonical_secondary_catch_up: LatencySummary,
    pub(super) wallet_apply: LatencySummary,
    pub(super) wallet_step: LatencySummary,
    /// The canonical follow step plus the wallet step.
    pub(super) block_total: LatencySummary,
}

impl TimerSummaries {
    fn from_samples(samples: &[&BlockSample]) -> Option<Self> {
        let summarize = |timer: fn(&BlockTimings) -> f64| {
            let values: Vec<f64> = samples
                .iter()
                .map(|sample| timer(&sample.timings_ms))
                .collect();
            LatencySummary::from_samples(&values)
        };
        Some(Self {
            canonical_follow_step: summarize(|timings| timings.canonical_follow_step)?,
            canonical_prepare: summarize(|timings| timings.canonical_prepare)?,
            canonical_commit: summarize(|timings| timings.canonical_commit)?,
            canonical_secondary_catch_up: summarize(|timings| {
                timings.canonical_secondary_catch_up
            })?,
            wallet_apply: summarize(|timings| timings.wallet_apply)?,
            wallet_step: summarize(|timings| timings.wallet_step)?,
            block_total: summarize(|timings| timings.canonical_follow_step + timings.wallet_step)?,
        })
    }
}

/// Block count and timer summaries for one stratum; `timers` is `null` when empty.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct StratumSummary {
    pub(super) block_count: u32,
    pub(super) timers: Option<TimerSummaries>,
}

impl StratumSummary {
    fn from_samples(samples: &[&BlockSample]) -> Self {
        Self {
            block_count: u32::try_from(samples.len()).unwrap_or(u32::MAX),
            timers: TimerSummaries::from_samples(samples),
        }
    }
}

/// Timer summaries over all blocks and over each workload stratum.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct TimerReport {
    pub(super) all_blocks: StratumSummary,
    pub(super) no_transparent_activity: StratumSummary,
    pub(super) near_empty_after_dense: StratumSummary,
    pub(super) input_heavy: StratumSummary,
    pub(super) fan_out: StratumSummary,
}

impl TimerReport {
    pub(super) fn from_samples(samples: &[&BlockSample]) -> Self {
        let stratum = |stratum: Stratum| {
            let members: Vec<&BlockSample> = samples
                .iter()
                .copied()
                .filter(|sample| sample.strata.contains(&stratum))
                .collect();
            StratumSummary::from_samples(&members)
        };
        Self {
            all_blocks: StratumSummary::from_samples(samples),
            no_transparent_activity: stratum(Stratum::NoTransparentActivity),
            near_empty_after_dense: stratum(Stratum::NearEmptyAfterDense),
            input_heavy: stratum(Stratum::InputHeavy),
            fan_out: stratum(Stratum::FanOut),
        }
    }

    /// Every summary paired with its stable stratum name.
    pub(super) const fn named_strata(&self) -> [(&'static str, &StratumSummary); 5] {
        [
            ("all_blocks", &self.all_blocks),
            ("no_transparent_activity", &self.no_transparent_activity),
            ("near_empty_after_dense", &self.near_empty_after_dense),
            ("input_heavy", &self.input_heavy),
            ("fan_out", &self.fan_out),
        ]
    }
}

impl TimerSummaries {
    /// Every timer summary paired with its stable timer name.
    pub(super) const fn named_timers(&self) -> [(&'static str, &LatencySummary); 7] {
        [
            ("canonical_follow_step", &self.canonical_follow_step),
            ("canonical_prepare", &self.canonical_prepare),
            ("canonical_commit", &self.canonical_commit),
            (
                "canonical_secondary_catch_up",
                &self.canonical_secondary_catch_up,
            ),
            ("wallet_apply", &self.wallet_apply),
            ("wallet_step", &self.wallet_step),
            ("block_total", &self.block_total),
        ]
    }
}

fn saturating_u32(count: usize) -> u32 {
    u32::try_from(count).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::{BlockWorkload, LatencySummary, Stratum, classify_strata, nearest_rank};

    fn workload(inputs: u32, non_coinbase_outputs: u32) -> BlockWorkload {
        BlockWorkload {
            transactions: 1,
            transparent_inputs: inputs,
            transparent_outputs: non_coinbase_outputs.saturating_add(1),
            coinbase_transparent_outputs: 1,
        }
    }

    fn bits(sample: Option<f64>) -> Option<u64> {
        sample.map(f64::to_bits)
    }

    #[test]
    fn nearest_rank_selects_the_ceiling_rank_sample() {
        let sorted: Vec<f64> = (1..=1_000).map(f64::from).collect();
        assert_eq!(bits(nearest_rank(&sorted, 5_000)), bits(Some(500.0)));
        assert_eq!(bits(nearest_rank(&sorted, 9_900)), bits(Some(990.0)));
        assert_eq!(bits(nearest_rank(&sorted, 9_990)), bits(Some(999.0)));
        assert_eq!(bits(nearest_rank(&[7.0], 9_990)), bits(Some(7.0)));
        assert_eq!(nearest_rank(&[], 5_000), None);
    }

    #[test]
    fn latency_summary_reports_nearest_rank_tails_and_totals() -> Result<(), Box<dyn Error>> {
        let summary = LatencySummary::from_samples(&[4.0, 1.0, 3.0, 2.0])
            .ok_or("four samples must summarize")?;

        assert_eq!(summary.count, 4);
        assert_eq!(summary.p50_ms.to_bits(), 2.0_f64.to_bits());
        assert_eq!(summary.p99_ms.to_bits(), 4.0_f64.to_bits());
        assert_eq!(summary.p999_ms.to_bits(), 4.0_f64.to_bits());
        assert_eq!(summary.max_ms.to_bits(), 4.0_f64.to_bits());
        assert_eq!(summary.total_ms.to_bits(), 10.0_f64.to_bits());
        assert_eq!(summary.mean_ms.to_bits(), 2.5_f64.to_bits());
        assert!(LatencySummary::from_samples(&[]).is_none());
        Ok(())
    }

    #[test]
    fn strata_follow_dense_blocks_across_the_replay_boundary() {
        let history = [(100, workload(1_001, 0)), (101, workload(0, 0))];
        let replayed = [
            (1_100, workload(0, 0)),
            (1_101, workload(0, 0)),
            (1_102, workload(0, 1_500)),
            (1_103, workload(2, 3)),
            (1_104, workload(1_000, 4)),
            (2_102, workload(0, 0)),
            (2_103, workload(0, 0)),
        ];

        let strata = classify_strata(&history, &replayed);

        assert_eq!(
            strata,
            vec![
                vec![Stratum::NoTransparentActivity, Stratum::NearEmptyAfterDense],
                vec![Stratum::NoTransparentActivity],
                vec![Stratum::FanOut],
                vec![Stratum::NearEmptyAfterDense],
                vec![Stratum::InputHeavy],
                vec![Stratum::NoTransparentActivity, Stratum::NearEmptyAfterDense],
                vec![Stratum::NoTransparentActivity],
            ]
        );
    }
}
