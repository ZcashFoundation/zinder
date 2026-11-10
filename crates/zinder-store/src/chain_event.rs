//! Post-commit chain event payload values.

use std::num::NonZeroU32;

use zinder_core::{BlockHeight, BlockHeightRange, ChainEpoch};

use crate::{
    CanonicalEventKind, CanonicalRetainedEvent, CanonicalStoreError, ChainEventStreamFamily,
    StreamCursorTokenV1,
};

/// Default maximum chain events returned by one history read.
pub const DEFAULT_MAX_CHAIN_EVENT_HISTORY_EVENTS: NonZeroU32 = NonZeroU32::MIN.saturating_add(999);

/// Bounded chain-event history read request.
#[derive(Clone, Copy, Debug)]
pub struct ChainEventHistoryRequest<'cursor> {
    /// Cursor to resume strictly after, or `None` to read from retained history start.
    pub from_cursor: Option<&'cursor StreamCursorTokenV1>,
    /// Chain-event stream family to read when `from_cursor` is absent.
    ///
    /// When `from_cursor` is present, the cursor's encoded family is
    /// authoritative so reconnecting clients do not need to remember a
    /// parallel option.
    pub family: ChainEventStreamFamily,
    /// Maximum number of events returned in this page.
    pub max_events: NonZeroU32,
}

impl<'cursor> ChainEventHistoryRequest<'cursor> {
    /// Creates a bounded chain-event history read request.
    #[must_use]
    pub const fn new(
        from_cursor: Option<&'cursor StreamCursorTokenV1>,
        max_events: NonZeroU32,
    ) -> Self {
        Self::new_for_family(from_cursor, ChainEventStreamFamily::Visible, max_events)
    }

    /// Creates a bounded chain-event history read request for `family`.
    #[must_use]
    pub const fn new_for_family(
        from_cursor: Option<&'cursor StreamCursorTokenV1>,
        family: ChainEventStreamFamily,
        max_events: NonZeroU32,
    ) -> Self {
        Self {
            from_cursor,
            family,
            max_events,
        }
    }

    /// Creates a chain-event history read request with the default page size.
    #[must_use]
    pub const fn with_default_limit(from_cursor: Option<&'cursor StreamCursorTokenV1>) -> Self {
        Self::new(from_cursor, DEFAULT_MAX_CHAIN_EVENT_HISTORY_EVENTS)
    }
}

/// Result returned after a chain epoch commit becomes durable and visible.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainEpochCommitOutcome {
    /// Chain epoch committed by the storage operation.
    pub chain_epoch: ChainEpoch,
    /// Inclusive block range included in the commit.
    pub block_range: BlockHeightRange,
    /// Post-commit event produced by the durable transition.
    pub event: ChainEvent,
    /// Cursor-bound event persisted for resumable consumers.
    pub event_envelope: ChainEventEnvelope,
}

impl ChainEpochCommitOutcome {
    pub(crate) fn new(committed: ChainEpochCommitted, event_envelope: ChainEventEnvelope) -> Self {
        let event = event_envelope.event.clone();
        Self {
            chain_epoch: committed.chain_epoch,
            block_range: committed.block_range,
            event,
            event_envelope,
        }
    }
}

/// Cursor-bound chain event returned to resumable consumers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainEventEnvelope {
    /// Opaque cursor for resuming strictly after this event.
    pub cursor: StreamCursorTokenV1,
    /// Monotonic sequence in the chain-event stream.
    pub event_sequence: u64,
    /// Chain epoch visible after this event.
    pub chain_epoch: ChainEpoch,
    /// Settled tip height that was true for this event.
    pub settled_tip_height: BlockHeight,
    /// Post-commit canonical transition.
    pub event: ChainEvent,
}

impl ChainEventEnvelope {
    pub(crate) const fn new(
        cursor: StreamCursorTokenV1,
        event_sequence: u64,
        chain_epoch: ChainEpoch,
        settled_tip_height: BlockHeight,
        event: ChainEvent,
    ) -> Self {
        Self {
            cursor,
            event_sequence,
            chain_epoch,
            settled_tip_height,
            event,
        }
    }
}

/// Post-commit canonical chain transition.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ChainEvent {
    /// A non-reorg commit advanced the canonical tip or settled-tip prefix.
    ChainCommitted {
        /// Committed epoch payload.
        committed: ChainEpochCommitted,
    },
    /// A range that had not yet reached the settled tip was replaced by a new committed range.
    ChainReorged {
        /// Previously visible range invalidated by this transition.
        reverted: ChainRangeReverted,
        /// Replacement range committed by this transition.
        committed: ChainEpochCommitted,
    },
}

impl ChainEvent {
    /// Returns `true` when this event is the [`Self::ChainCommitted`] variant.
    ///
    /// Convenience for consumers that need a single boolean for tip-change
    /// notifications without `match`-ing on every variant.
    #[must_use]
    pub const fn is_chain_committed(&self) -> bool {
        matches!(self, Self::ChainCommitted { .. })
    }

    /// Reconstructs the dispatchable event for one retained canonical transition.
    ///
    /// `resulting_epoch` is the epoch the transition made visible.
    /// `reverted_epoch` is the epoch that contained the replaced range and is
    /// required only by a reorg transition.
    pub fn from_canonical_retained(
        retained: CanonicalRetainedEvent,
        resulting_epoch: ChainEpoch,
        reverted_epoch: Option<ChainEpoch>,
    ) -> Result<Self, CanonicalStoreError> {
        if resulting_epoch.id != retained.resulting_epoch_id() {
            return Err(retained_event_mismatch(
                retained,
                "resulting epoch does not match the retained canonical event",
            ));
        }
        let committed = ChainEpochCommitted::new(resulting_epoch, retained.committed_range());
        match retained.kind() {
            CanonicalEventKind::Committed => Ok(Self::ChainCommitted { committed }),
            CanonicalEventKind::Reorged => {
                let reverted_range = retained.reverted_range().ok_or_else(|| {
                    retained_event_mismatch(
                        retained,
                        "reorged canonical event carries no reverted range",
                    )
                })?;
                let reverted_epoch = reverted_epoch.ok_or_else(|| {
                    retained_event_mismatch(
                        retained,
                        "reorged canonical event requires its reverted epoch",
                    )
                })?;
                if Some(reverted_epoch.id) != retained.previous_epoch_id() {
                    return Err(retained_event_mismatch(
                        retained,
                        "reverted epoch does not match the retained canonical event",
                    ));
                }
                Ok(Self::ChainReorged {
                    reverted: ChainRangeReverted::new(reverted_epoch, reverted_range),
                    committed,
                })
            }
        }
    }
}

fn retained_event_mismatch(
    retained: CanonicalRetainedEvent,
    reason: &'static str,
) -> CanonicalStoreError {
    CanonicalStoreError::CanonicalEventRecordMalformed {
        event_sequence: retained.cursor().event_sequence(),
        reason,
    }
}

/// Event payload for a previously visible range that had not yet reached the settled tip.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChainRangeReverted {
    /// Chain epoch that contained the reverted range.
    pub chain_epoch: ChainEpoch,
    /// Inclusive block range invalidated by this transition.
    pub block_range: BlockHeightRange,
}

impl ChainRangeReverted {
    pub(crate) const fn new(chain_epoch: ChainEpoch, block_range: BlockHeightRange) -> Self {
        Self {
            chain_epoch,
            block_range,
        }
    }
}

/// Event payload returned after a chain epoch becomes durable and visible.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChainEpochCommitted {
    /// Chain epoch committed by the storage operation.
    pub chain_epoch: ChainEpoch,
    /// Inclusive block range included in the commit.
    pub block_range: BlockHeightRange,
}

impl ChainEpochCommitted {
    pub(crate) const fn new(chain_epoch: ChainEpoch, block_range: BlockHeightRange) -> Self {
        Self {
            chain_epoch,
            block_range,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_history_request_reads_visible_family_in_bounded_pages() {
        let request = ChainEventHistoryRequest::with_default_limit(None);

        assert!(request.from_cursor.is_none());
        assert_eq!(request.family, ChainEventStreamFamily::Visible);
        assert_eq!(request.max_events.get(), 1_000);
        assert_eq!(DEFAULT_MAX_CHAIN_EVENT_HISTORY_EVENTS.get(), 1_000);
    }
}
