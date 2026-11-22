//! Identity of the admitted canonical data that derived compact-block bytes come from.

use std::{
    any::Any,
    sync::{Arc, Weak},
};

use zinder_core::CanonicalHistoryBounds;
use zinder_store::CanonicalEventFence;

/// The canonical position and data a locally admitted epoch serves compact blocks from.
///
/// Adapters key reusable derived bytes by all three parts. The fence and
/// bounds name the chain position. The source names the admitted data, since
/// two stores can report equal fences while holding different compact bytes.
#[derive(Clone, Debug)]
pub struct CompactBlockFence {
    /// Writer position at which the epoch was committed.
    pub event_fence: CanonicalEventFence,
    /// Retention bounds of the store serving the epoch.
    pub history_bounds: CanonicalHistoryBounds,
    /// The admitted data the epoch reads.
    pub source: CompactBlockSource,
}

/// Names one admitted serving generation without keeping it alive.
///
/// Two values are the same source only while that generation still exists,
/// so a cache keyed by this value never matches a replacement store and
/// never delays releasing a retired one.
#[derive(Clone, Debug)]
pub struct CompactBlockSource(Weak<dyn Any + Send + Sync>);

impl CompactBlockSource {
    /// Names the generation `owner` holds.
    #[must_use]
    pub fn of<Owner: Send + Sync + 'static>(owner: &Arc<Owner>) -> Self {
        let weak: Weak<Owner> = Arc::downgrade(owner);
        let weak: Weak<dyn Any + Send + Sync> = weak;
        Self(weak)
    }

    /// Returns whether both values name the same live generation.
    #[must_use]
    pub fn is(&self, other: &Self) -> bool {
        self.0.strong_count() > 0 && Weak::ptr_eq(&self.0, &other.0)
    }
}
