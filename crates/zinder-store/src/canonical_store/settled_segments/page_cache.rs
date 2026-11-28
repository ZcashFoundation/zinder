//! Page-cache policy for the settled corpus admitted by one canonical open.

use std::num::NonZeroU64;

use crate::StoreRole;

use super::{
    EntryKind, SettledSegmentsError, VerifiedDir,
    layout::{
        SEGMENTS_DIR_NAME, SettledSegmentIndexEntry, SettledSegmentKind, format_segment_filename,
    },
};

#[derive(Clone, Copy, Default)]
pub(in crate::canonical_store) struct SettledSegmentPageCache {
    corpus_bytes: u64,
    memory_budget: Option<NonZeroU64>,
}

impl SettledSegmentPageCache {
    pub(super) fn measure(
        root: &VerifiedDir,
        entries: &[SettledSegmentIndexEntry],
        memory_budget: Option<NonZeroU64>,
    ) -> Result<Self, SettledSegmentsError> {
        Ok(Self {
            corpus_bytes: corpus_bytes(root, entries)?,
            memory_budget,
        })
    }

    pub(super) fn with_new_entries(
        self,
        root: &VerifiedDir,
        entries: &[SettledSegmentIndexEntry],
    ) -> Result<Self, SettledSegmentsError> {
        let added = corpus_bytes(root, entries)?;
        Ok(Self {
            corpus_bytes: add_bytes(self.corpus_bytes, added)?,
            ..self
        })
    }

    fn constrained_budget(self) -> Option<NonZeroU64> {
        self.memory_budget
            .filter(|budget| budget.get() < self.corpus_bytes)
    }

    #[allow(
        clippy::cast_precision_loss,
        reason = "Prometheus gauges use f64 samples"
    )]
    pub(super) fn record_admission(self, role: StoreRole, warn_if_constrained: bool) {
        let store_role = role.as_str();
        let corpus_bytes = self.corpus_bytes;
        let budget_bytes = self
            .memory_budget
            .map_or(f64::INFINITY, |budget| budget.get() as f64);
        metrics::gauge!("zinder_store_settled_segment_corpus_bytes", "store_role" => store_role)
            .set(corpus_bytes as f64);
        metrics::gauge!("zinder_store_settled_segment_memory_budget_bytes", "store_role" => store_role)
            .set(budget_bytes);
        metrics::gauge!("zinder_store_settled_segment_corpus_to_budget_ratio", "store_role" => store_role)
            .set(corpus_bytes as f64 / budget_bytes);
        if warn_if_constrained && let Some(budget) = self.constrained_budget() {
            tracing::warn!(
                target: "zinder::store",
                event = "settled_segment_corpus_exceeds_memory_budget",
                store_role,
                corpus_bytes,
                memory_budget_bytes = budget.get(),
                "settled segment corpus exceeds the container memory budget"
            );
        }
    }

    pub(super) fn constrained(self) -> bool {
        self.constrained_budget().is_some()
    }
}

fn corpus_bytes(
    root: &VerifiedDir,
    entries: &[SettledSegmentIndexEntry],
) -> Result<u64, SettledSegmentsError> {
    use SettledSegmentKind::{RawBlock, Shielded, Supplement};

    if entries.is_empty() {
        return Ok(0);
    }
    let segments = root.open_child_dir_existing(SEGMENTS_DIR_NAME)?;
    let mut total = 0_u64;
    for kind in [Shielded, Supplement, RawBlock] {
        let mut admitted = entries
            .iter()
            .filter(|entry| match kind {
                Shielded => true,
                Supplement => entry.modules.supplement,
                RawBlock => entry.modules.raw_bodies,
            })
            .peekable();
        if admitted.peek().is_none() {
            continue;
        }
        let dir = segments.open_child_dir_existing(kind.subdir_name())?;
        for entry in admitted {
            let filename =
                format_segment_filename(kind, entry.start_height, entry.end_block.height)?;
            let Some((EntryKind::RegularFile, len)) = dir.stat_entry_and_len_nofollow(&filename)?
            else {
                return Err(SettledSegmentsError::InvalidLayout(format!(
                    "expected admitted regular segment file at {}",
                    dir.display_path().join(filename).display()
                )));
            };
            total = add_bytes(total, len)?;
        }
    }
    Ok(total)
}

fn add_bytes(total: u64, added: u64) -> Result<u64, SettledSegmentsError> {
    total.checked_add(added).ok_or_else(|| {
        SettledSegmentsError::InvalidLayout("settled segment corpus exceeds u64 bytes".to_owned())
    })
}

#[cfg(test)]
#[test]
fn scan_cache_is_constrained_only_when_the_corpus_exceeds_a_finite_budget() {
    for (budget, expected) in [
        (NonZeroU64::new(1023), true),
        (NonZeroU64::new(1024), false),
        (NonZeroU64::new(1025), false),
        (None, false),
    ] {
        let page_cache = SettledSegmentPageCache {
            corpus_bytes: 1024,
            memory_budget: budget,
        };
        assert_eq!(page_cache.constrained(), expected);
    }
}
