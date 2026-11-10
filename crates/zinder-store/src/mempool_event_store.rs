//! Public read/write contracts for the persistent mempool event log.
//!
//! Mempool events live in their own `mempool_event` column family of the
//! canonical store, with an independent monotonic sequence and retention
//! floor. The write API ([`RocksDbCanonicalStore::append_mempool_event`]) and
//! read API ([`RocksDbCanonicalStore::mempool_event_history`]) mirror the
//! chain-event shape so consumers compose mempool reads with the same
//! cursor-resume pattern they already use for chain events.
//!
//! [`RocksDbCanonicalStore::append_mempool_event`]: crate::RocksDbCanonicalStore::append_mempool_event
//! [`RocksDbCanonicalStore::mempool_event_history`]: crate::RocksDbCanonicalStore::mempool_event_history

use std::{
    num::{NonZeroU32, NonZeroU64},
    time::Duration,
};

use zinder_core::UnixTimestampMillis;

use crate::StreamCursorTokenV1;

/// Default maximum mempool events returned by one history read.
pub const DEFAULT_MAX_MEMPOOL_EVENT_HISTORY_EVENTS: NonZeroU32 =
    NonZeroU32::MIN.saturating_add(255);

/// Bounded mempool-event history read request.
#[derive(Clone, Copy, Debug)]
pub struct MempoolEventHistoryRequest<'cursor> {
    /// Cursor to resume strictly after, or `None` to read from retained
    /// history start.
    pub from_cursor: Option<&'cursor StreamCursorTokenV1>,
    /// Maximum number of events returned in this page.
    pub max_events: NonZeroU32,
}

impl<'cursor> MempoolEventHistoryRequest<'cursor> {
    /// Creates a bounded mempool-event history read request.
    #[must_use]
    pub const fn new(
        from_cursor: Option<&'cursor StreamCursorTokenV1>,
        max_events: NonZeroU32,
    ) -> Self {
        Self {
            from_cursor,
            max_events,
        }
    }

    /// Creates a mempool-event history read request with the default page
    /// size.
    #[must_use]
    pub const fn with_default_limit(from_cursor: Option<&'cursor StreamCursorTokenV1>) -> Self {
        Self::new(from_cursor, DEFAULT_MAX_MEMPOOL_EVENT_HISTORY_EVENTS)
    }
}

/// Per-variant retention windows applied during a pruning pass.
///
/// `Mined` events are diagnostic once the mining block has crossed the settled tip;
/// `Invalidated` events feed wallet UX longer because they need to displace
/// "still pending" displays for transactions the network rejected.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MempoolEventRetentionConfig {
    /// Retention window for `Added` events. `None` keeps every `Added`
    /// envelope. `Some` prunes envelopes older than the cutoff.
    pub added_retention: Option<Duration>,
    /// Retention window for `Mined` events. `None` keeps every `Mined`
    /// envelope.
    pub mined_retention: Option<Duration>,
    /// Retention window for `Invalidated` events. `None` keeps every
    /// `Invalidated` envelope.
    pub invalidated_retention: Option<Duration>,
}

impl MempoolEventRetentionConfig {
    /// Returns retention with separate Mined and Invalidated windows; `Added`
    /// envelopes follow whichever window is shorter so a transaction's
    /// post-tip lifecycle stays internally consistent.
    #[must_use]
    pub const fn new(
        mined_retention: Option<Duration>,
        invalidated_retention: Option<Duration>,
    ) -> Self {
        let added_retention = match (mined_retention, invalidated_retention) {
            (Some(mined), Some(invalidated)) => Some(if mined.as_secs() <= invalidated.as_secs() {
                mined
            } else {
                invalidated
            }),
            (Some(mined), None) => Some(mined),
            (None, Some(invalidated)) => Some(invalidated),
            (None, None) => None,
        };
        Self {
            added_retention,
            mined_retention,
            invalidated_retention,
        }
    }

    /// Returns `true` when no retention window is configured.
    #[must_use]
    pub const fn is_unbounded(self) -> bool {
        self.added_retention.is_none()
            && self.mined_retention.is_none()
            && self.invalidated_retention.is_none()
    }
}

/// Work budget for one durable mempool-event retention step.
///
/// The encoded-byte budget is a target: the first row is always examined so
/// every step can make progress, even when one admitted row exceeds the
/// configured byte budget.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MempoolEventRetentionStepBudget {
    max_events: NonZeroU32,
    max_encoded_bytes: NonZeroU64,
}

impl MempoolEventRetentionStepBudget {
    /// Creates a non-zero per-step event and encoded-byte budget.
    #[must_use]
    pub const fn new(max_events: NonZeroU32, max_encoded_bytes: NonZeroU64) -> Self {
        Self {
            max_events,
            max_encoded_bytes,
        }
    }

    /// Maximum event rows examined by one step.
    #[must_use]
    pub const fn max_events(self) -> NonZeroU32 {
        self.max_events
    }

    /// Target maximum encoded event bytes examined by one step.
    #[must_use]
    pub const fn max_encoded_bytes(self) -> NonZeroU64 {
        self.max_encoded_bytes
    }
}

/// Mempool-event retention state observed after a pruning or inspection pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MempoolEventRetentionReport {
    /// Latest mempool-event sequence written to the store.
    pub current_event_sequence: u64,
    /// Oldest retained sequence, or `None` when the store has no mempool
    /// events.
    pub oldest_retained_sequence: Option<u64>,
    /// Source-observation timestamp for the oldest retained event, when
    /// retained.
    pub oldest_retained_observed_at: Option<UnixTimestampMillis>,
    /// Number of event rows retained after the pass.
    pub retained_event_count: u64,
    /// Number of `Added` rows deleted by this pass.
    pub pruned_added_count: u64,
    /// Number of `Mined` rows deleted by this pass.
    pub pruned_mined_count: u64,
    /// Number of `Invalidated` rows deleted by this pass.
    pub pruned_invalidated_count: u64,
}

impl MempoolEventRetentionReport {
    /// Returns the total number of rows deleted by this pass.
    #[must_use]
    pub const fn pruned_total(self) -> u64 {
        self.pruned_added_count
            .saturating_add(self.pruned_mined_count)
            .saturating_add(self.pruned_invalidated_count)
    }
}

/// Reason one bounded mempool-event retention step stopped scanning.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MempoolEventRetentionStepStop {
    /// The event-count or encoded-byte budget was exhausted before the
    /// captured head or first unexpired event was reached.
    BudgetExhausted,
    /// The step reached the captured durable event head.
    ReachedHead,
    /// The first event whose retention window has not expired stopped the
    /// contiguous-prefix scan.
    ReachedUnexpiredEvent,
    /// No finite retention window is configured.
    RetentionDisabled,
}

/// Result of one bounded durable mempool-event retention step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MempoolEventRetentionStepOutcome {
    /// Retention floor and per-kind deletions after this step.
    pub report: MempoolEventRetentionReport,
    /// Number of encoded event rows examined by this step.
    pub examined_event_count: u32,
    /// Total encoded bytes in the event rows examined by this step.
    pub examined_encoded_bytes: u64,
    /// Reason the step stopped scanning.
    pub stop: MempoolEventRetentionStepStop,
}

impl MempoolEventRetentionStepOutcome {
    /// Returns whether another bounded step should be scheduled without the
    /// normal retention interval delay.
    #[must_use]
    pub const fn has_immediate_work(self) -> bool {
        matches!(self.stop, MempoolEventRetentionStepStop::BudgetExhausted)
    }
}
