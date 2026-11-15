//! Phase plan for one in-process serving run.

use serde::{Deserialize, Serialize};

/// Ordered phases of one run.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Plan {
    pub(crate) phases: Vec<Phase>,
}

/// One measured phase. The writer stops at `blocks` or `duration_seconds`,
/// whichever comes first; at least one must be set.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Phase {
    pub(crate) name: String,
    /// Milliseconds between revealed source blocks; 0 reveals as fast as the writer commits.
    pub(crate) block_interval_ms: u64,
    #[serde(default)]
    pub(crate) blocks: Option<u32>,
    #[serde(default)]
    pub(crate) duration_seconds: Option<u64>,
    #[serde(default)]
    pub(crate) readers: Vec<ReaderSpec>,
    #[serde(default)]
    pub(crate) load: Option<LoadSpec>,
    #[serde(default)]
    pub(crate) replacements: Vec<ReplacementSpec>,
    #[serde(default)]
    pub(crate) index_attach: Option<IndexAttachSpec>,
    /// Synthetic density multiplier for this phase (1.0 = real Testnet rows only).
    #[serde(default = "one")]
    pub(crate) density_multiplier: f64,
    /// Run a full manual compaction of both databases after the phase.
    #[serde(default)]
    pub(crate) compact_all_at_end: bool,
    /// Phase whose commit p99 this phase is compared with; defaults to the run baseline.
    #[serde(default)]
    pub(crate) baseline_phase: Option<String>,
    /// No-reader phase after the treatment, for an A-B-A comparison.
    #[serde(default)]
    pub(crate) after_baseline_phase: Option<String>,
}

fn one() -> f64 {
    1.0
}

fn default_true() -> bool {
    true
}

/// One group of identical snapshot-holding readers.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) enum ReaderSpec {
    /// A compact-block range stream through one snapshot.
    CompactRange {
        count: u32,
        hold_seconds: f64,
        range_blocks: u32,
        #[serde(default = "default_chunk_blocks")]
        chunk_blocks: u32,
        /// Keep one raw iterator open for the whole hold (pins its version).
        #[serde(default = "default_true")]
        held_iterator: bool,
        /// `false` reads the live database: the negative control for torn views.
        #[serde(default = "default_true")]
        snapshot: bool,
        #[serde(default = "default_paced_interval_ms")]
        paced_interval_ms: u64,
    },
    /// A full scan of one wallet family through one snapshot.
    WalletFamily {
        count: u32,
        hold_seconds: f64,
        family: String,
        #[serde(default = "default_true")]
        snapshot: bool,
        #[serde(default = "default_paced_interval_ms")]
        paced_interval_ms: u64,
    },
}

fn default_chunk_blocks() -> u32 {
    100
}

fn default_paced_interval_ms() -> u64 {
    250
}

/// Mixed request load with request-scoped snapshots.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LoadSpec {
    pub(crate) compact_workers: u32,
    pub(crate) wallet_workers: u32,
    pub(crate) lookup_workers: u32,
    #[serde(default = "default_load_range_blocks")]
    pub(crate) compact_range_blocks: u32,
    #[serde(default)]
    pub(crate) think_time_ms: u64,
}

fn default_load_range_blocks() -> u32 {
    500
}

/// One canonical suffix replacement during a phase.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplacementSpec {
    pub(crate) at_seconds: f64,
    /// Blocks displaced from the visible tip downward.
    pub(crate) displaced: u32,
    /// Replacement blocks committed (equal to `displaced` keeps the tip).
    pub(crate) kept: u32,
}

/// Rebuild one wallet family into a new column family and switch to it.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexAttachSpec {
    pub(crate) at_seconds: f64,
    /// Only `transparent_spent_output` has a catch-up implementation.
    pub(crate) family: String,
    /// Seconds between the switch and dropping the old column family.
    #[serde(default = "default_drop_after_seconds")]
    pub(crate) drop_after_seconds: f64,
    /// Hand off to the writer once the concurrent catch-up gap is at most this many blocks.
    #[serde(default = "default_final_gap_blocks")]
    pub(crate) final_gap_blocks: u32,
    /// Seconds to wait after the drop for the old family's files to disappear.
    #[serde(default = "default_wait_for_old_files_seconds")]
    pub(crate) wait_for_old_files_seconds: f64,
}

fn default_wait_for_old_files_seconds() -> f64 {
    600.0
}

fn default_drop_after_seconds() -> f64 {
    5.0
}

fn default_final_gap_blocks() -> u32 {
    2
}

impl Phase {
    pub(crate) fn pacing_label(&self) -> String {
        match self.block_interval_ms {
            0 => "unpaced".to_owned(),
            25_000 => "real-25s".to_owned(),
            other => format!("accelerated-{other}ms"),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.blocks.is_none() && self.duration_seconds.is_none() {
            return Err(format!(
                "phase {} needs blocks or duration_seconds",
                self.name
            ));
        }
        if self.density_multiplier < 1.0 {
            return Err(format!(
                "phase {} density_multiplier must be >= 1",
                self.name
            ));
        }
        for replacement in &self.replacements {
            if replacement.kept == 0 || replacement.kept > replacement.displaced {
                return Err(format!(
                    "phase {} replacement kept must be in 1..=displaced",
                    self.name
                ));
            }
        }
        if let Some(attach) = &self.index_attach
            && attach.family != "transparent_spent_output"
        {
            return Err("index_attach supports only transparent_spent_output".to_owned());
        }
        Ok(())
    }
}

fn hold_readers(count: u32, hold_seconds: f64, range_blocks: u32) -> Vec<ReaderSpec> {
    vec![
        ReaderSpec::CompactRange {
            count,
            hold_seconds,
            range_blocks,
            chunk_blocks: 100,
            held_iterator: true,
            snapshot: true,
            paced_interval_ms: 250,
        },
        ReaderSpec::WalletFamily {
            count: 1,
            hold_seconds,
            family: "transparent_unspent_output".to_owned(),
            snapshot: true,
            paced_interval_ms: 250,
        },
        ReaderSpec::WalletFamily {
            count: 1,
            hold_seconds,
            family: "transparent_address_transaction".to_owned(),
            snapshot: true,
            paced_interval_ms: 250,
        },
    ]
}

fn base_phase(name: &str, block_interval_ms: u64) -> Phase {
    Phase {
        name: name.to_owned(),
        block_interval_ms,
        blocks: None,
        duration_seconds: None,
        readers: Vec::new(),
        load: None,
        replacements: Vec::new(),
        index_attach: None,
        density_multiplier: 1.0,
        compact_all_at_end: false,
        baseline_phase: None,
        after_baseline_phase: None,
    }
}

/// Compressed plan that exercises every arm in a few minutes.
pub(crate) fn smoke_plan() -> Plan {
    let mut baseline = base_phase("baseline-no-readers", 100);
    baseline.blocks = Some(150);
    let mut hold_1s = base_phase("hold-1s", 100);
    hold_1s.duration_seconds = Some(15);
    hold_1s.readers = hold_readers(2, 1.0, 1_000);
    let mut hold_long = base_phase("hold-20s-with-replacements", 100);
    hold_long.duration_seconds = Some(45);
    hold_long.readers = hold_readers(2, 20.0, 1_000);
    hold_long.readers.push(ReaderSpec::CompactRange {
        count: 1,
        hold_seconds: 20.0,
        range_blocks: 1_000,
        chunk_blocks: 100,
        held_iterator: false,
        snapshot: false,
        paced_interval_ms: 250,
    });
    hold_long.readers.push(ReaderSpec::WalletFamily {
        count: 1,
        hold_seconds: 20.0,
        family: "transparent_unspent_output".to_owned(),
        snapshot: false,
        paced_interval_ms: 250,
    });
    hold_long.replacements = vec![
        ReplacementSpec {
            at_seconds: 5.0,
            displaced: 1_000,
            kept: 1_000,
        },
        ReplacementSpec {
            at_seconds: 12.0,
            displaced: 1_000,
            kept: 500,
        },
    ];
    let mut post = base_phase("post-release", 100);
    post.duration_seconds = Some(10);
    post.compact_all_at_end = true;
    let mut attach = base_phase("index-attach", 200);
    attach.duration_seconds = Some(60);
    attach.readers = vec![ReaderSpec::WalletFamily {
        count: 2,
        hold_seconds: 15.0,
        family: "transparent_spent_output".to_owned(),
        snapshot: true,
        paced_interval_ms: 250,
    }];
    attach.index_attach = Some(IndexAttachSpec {
        at_seconds: 2.0,
        family: "transparent_spent_output".to_owned(),
        drop_after_seconds: 3.0,
        final_gap_blocks: 2,
        wait_for_old_files_seconds: 30.0,
    });
    let mut load = base_phase("serving-load", 100);
    load.duration_seconds = Some(15);
    load.load = Some(LoadSpec {
        compact_workers: 4,
        wallet_workers: 4,
        lookup_workers: 4,
        compact_range_blocks: 200,
        think_time_ms: 0,
    });
    let mut density_baseline = base_phase("density-x10-no-readers", 100);
    density_baseline.duration_seconds = Some(10);
    density_baseline.density_multiplier = 10.0;
    let mut density = base_phase("density-x10-hold-5s", 100);
    density.duration_seconds = Some(15);
    density.density_multiplier = 10.0;
    density.readers = hold_readers(2, 5.0, 1_000);
    density.baseline_phase = Some("density-x10-no-readers".to_owned());
    let mut realtime = base_phase("realtime-spacing-compressed", 2_000);
    realtime.duration_seconds = Some(10);
    realtime.readers = hold_readers(1, 5.0, 1_000);
    Plan {
        phases: vec![
            baseline,
            hold_1s,
            hold_long,
            post,
            attach,
            load,
            density_baseline,
            density,
            realtime,
        ],
    }
}

/// Full plan for the measured 1x Testnet run: Arms 1, 2, and 4 (about 2 h 50 min
/// and about 16,700 replayed blocks). Every treatment phase names the no-reader
/// phase before it and, where one exists, a no-reader phase after it.
pub(crate) fn measured_plan() -> Plan {
    let accelerated = 500;
    let mut baseline = base_phase("baseline-no-readers", accelerated);
    baseline.duration_seconds = Some(600);
    let mut replacements_baseline = base_phase("replacements-no-readers", accelerated);
    replacements_baseline.duration_seconds = Some(600);
    replacements_baseline.replacements = vec![
        ReplacementSpec {
            at_seconds: 120.0,
            displaced: 1_000,
            kept: 1_000,
        },
        ReplacementSpec {
            at_seconds: 420.0,
            displaced: 1_000,
            kept: 500,
        },
    ];
    let after = Some("post-30m-release".to_owned());
    let mut hold_1s = base_phase("hold-1s", accelerated);
    hold_1s.duration_seconds = Some(300);
    hold_1s.readers = hold_readers(8, 1.0, 10_000);
    hold_1s.baseline_phase = Some("baseline-no-readers".to_owned());
    hold_1s.after_baseline_phase = after.clone();
    let mut hold_30s = base_phase("hold-30s", accelerated);
    hold_30s.duration_seconds = Some(600);
    hold_30s.readers = hold_readers(8, 30.0, 10_000);
    hold_30s.baseline_phase = Some("baseline-no-readers".to_owned());
    hold_30s.after_baseline_phase = after.clone();
    let mut hold_5m = base_phase("hold-5m-with-replacements", accelerated);
    hold_5m.duration_seconds = Some(960);
    hold_5m.readers = hold_readers(8, 300.0, 10_000);
    hold_5m.readers.push(ReaderSpec::CompactRange {
        count: 1,
        hold_seconds: 300.0,
        range_blocks: 2_000,
        chunk_blocks: 100,
        held_iterator: false,
        snapshot: false,
        paced_interval_ms: 250,
    });
    hold_5m.readers.push(ReaderSpec::WalletFamily {
        count: 1,
        hold_seconds: 300.0,
        family: "transparent_unspent_output".to_owned(),
        snapshot: false,
        paced_interval_ms: 250,
    });
    hold_5m.replacements = vec![
        ReplacementSpec {
            at_seconds: 120.0,
            displaced: 1_000,
            kept: 1_000,
        },
        ReplacementSpec {
            at_seconds: 420.0,
            displaced: 1_000,
            kept: 500,
        },
    ];
    hold_5m.baseline_phase = Some("replacements-no-readers".to_owned());
    hold_5m.after_baseline_phase = after.clone();
    let mut hold_30m = base_phase("hold-30m", accelerated);
    hold_30m.duration_seconds = Some(1_860);
    hold_30m.readers = hold_readers(8, 1_800.0, 10_000);
    hold_30m.baseline_phase = Some("baseline-no-readers".to_owned());
    hold_30m.after_baseline_phase = after;
    let mut post = base_phase("post-30m-release", accelerated);
    post.duration_seconds = Some(900);
    post.compact_all_at_end = true;
    post.baseline_phase = Some("baseline-no-readers".to_owned());
    let mut realtime = base_phase("realtime-25s-hold-5m", 25_000);
    realtime.duration_seconds = Some(1_800);
    realtime.readers = hold_readers(8, 300.0, 10_000);
    realtime.baseline_phase = Some("baseline-no-readers".to_owned());
    realtime.after_baseline_phase = Some("post-30m-release".to_owned());
    let mut attach = base_phase("index-attach", accelerated);
    attach.duration_seconds = Some(960);
    attach.readers = vec![
        ReaderSpec::WalletFamily {
            count: 2,
            hold_seconds: 120.0,
            family: "transparent_spent_output".to_owned(),
            snapshot: true,
            paced_interval_ms: 250,
        },
        // Start before the switch and still hold the old family when it is dropped
        // (drop at about 500 s: 30 s start, about 420 s of SST build, verify, 30 s).
        ReaderSpec::WalletFamily {
            count: 2,
            hold_seconds: 900.0,
            family: "transparent_spent_output".to_owned(),
            snapshot: true,
            paced_interval_ms: 250,
        },
    ];
    attach.index_attach = Some(IndexAttachSpec {
        at_seconds: 30.0,
        family: "transparent_spent_output".to_owned(),
        drop_after_seconds: 30.0,
        final_gap_blocks: 2,
        // Longer than the 900 s holders' remaining hold after the drop, plus margin.
        wait_for_old_files_seconds: 1_200.0,
    });
    attach.baseline_phase = Some("baseline-no-readers".to_owned());
    attach.after_baseline_phase = Some("load-baseline-no-readers".to_owned());
    let mut load_baseline = base_phase("load-baseline-no-readers", accelerated);
    load_baseline.duration_seconds = Some(300);
    let mut load = base_phase("serving-load", accelerated);
    load.duration_seconds = Some(900);
    load.baseline_phase = Some("load-baseline-no-readers".to_owned());
    load.after_baseline_phase = Some("load-baseline-after".to_owned());
    load.load = Some(LoadSpec {
        compact_workers: 8,
        wallet_workers: 8,
        lookup_workers: 8,
        compact_range_blocks: 1_000,
        think_time_ms: 0,
    });
    let mut load_after = base_phase("load-baseline-after", accelerated);
    load_after.duration_seconds = Some(300);
    load_after.baseline_phase = Some("load-baseline-no-readers".to_owned());
    Plan {
        phases: vec![
            baseline,
            replacements_baseline,
            hold_1s,
            hold_30s,
            hold_5m,
            hold_30m,
            post,
            realtime,
            attach,
            load_baseline,
            load,
            load_after,
        ],
    }
}

/// Density arm, run as its own process from the same input stores: a 30-minute
/// no-reader segment, a 30-minute hold, and 15 minutes of natural compaction after
/// release before a full manual compaction. The multiplier is set with
/// `--density-multiplier-override` (default 10).
pub(crate) fn density_plan() -> Plan {
    let accelerated = 500;
    let mut before = base_phase("density-no-readers", accelerated);
    before.duration_seconds = Some(1_800);
    before.density_multiplier = 10.0;
    let mut hold = base_phase("density-hold-30m", accelerated);
    hold.duration_seconds = Some(1_860);
    hold.density_multiplier = 10.0;
    hold.readers = hold_readers(8, 1_800.0, 10_000);
    hold.baseline_phase = Some("density-no-readers".to_owned());
    hold.after_baseline_phase = Some("density-post-release".to_owned());
    let mut post = base_phase("density-post-release", accelerated);
    post.duration_seconds = Some(900);
    post.density_multiplier = 10.0;
    post.compact_all_at_end = true;
    post.baseline_phase = Some("density-no-readers".to_owned());
    Plan {
        phases: vec![before, hold, post],
    }
}
