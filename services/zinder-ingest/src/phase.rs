//! Phase classifier behind the `zinder-ingest probe` diagnostic.
//!
//! The canonical writer selects construction or follow from the store
//! lifecycle in `writer/mod.rs`. [`classify_phase`] reports which
//! [`IngestPhase`] a store tip and an upstream tip correspond to, using the
//! gap rules from [ADR-0015](../../../docs/adrs/0015-phase-driven-ingest.md).

use zinder_runtime::IngestPhase;

/// Classifies the [`IngestPhase`] that a store tip and an upstream tip
/// correspond to.
///
/// Rules per [ADR-0015 §Decision]:
///
/// - [`IngestPhase::AwaitingUpstream`] when `upstream_tip == 0`. Models
///   regtest near genesis and freshly initialized nodes: there is no
///   committable chain yet.
/// - [`IngestPhase::BulkCatchup`] when the gap from the store tip to
///   the upstream tip exceeds `catchup_threshold_blocks`.
/// - [`IngestPhase::FollowingTip`] otherwise.
///
/// An empty store (`store_tip = None`) is treated as height `0` so the
/// gap is the upstream tip itself. A store ahead of the upstream tip
/// (local reorg in progress, `invalidateblock` race) collapses the gap
/// to zero via saturating subtraction and lands in `FollowingTip`.
///
/// [ADR-0015 §Decision]:
///     ../../../docs/adrs/0015-phase-driven-ingest.md#decision
#[must_use]
pub fn classify_phase(
    store_tip: Option<u32>,
    upstream_tip: u32,
    catchup_threshold_blocks: u32,
) -> IngestPhase {
    if upstream_tip == 0 {
        return IngestPhase::AwaitingUpstream;
    }
    let store = store_tip.unwrap_or(0);
    let gap = upstream_tip.saturating_sub(store);
    if gap > catchup_threshold_blocks {
        IngestPhase::BulkCatchup
    } else {
        IngestPhase::FollowingTip
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn awaits_upstream_when_upstream_tip_is_genesis() {
        assert_eq!(classify_phase(None, 0, 100), IngestPhase::AwaitingUpstream);
        assert_eq!(
            classify_phase(Some(0), 0, 100),
            IngestPhase::AwaitingUpstream
        );
    }

    #[test]
    fn follows_tip_when_upstream_chain_is_shorter_than_threshold() {
        // Empty store, small upstream: FollowingTip can commit serially.
        assert_eq!(classify_phase(None, 50, 100), IngestPhase::FollowingTip);
    }

    #[test]
    fn follows_tip_when_gap_equals_threshold() {
        // Boundary: gap == threshold is FollowingTip; only a strictly
        // larger gap escalates to BulkCatchup.
        assert_eq!(
            classify_phase(Some(900), 1_000, 100),
            IngestPhase::FollowingTip
        );
    }

    #[test]
    fn bulk_catches_up_when_gap_exceeds_threshold() {
        assert_eq!(
            classify_phase(Some(0), 1_000_000, 100),
            IngestPhase::BulkCatchup
        );
        assert_eq!(
            classify_phase(Some(100), 100_001, 100),
            IngestPhase::BulkCatchup
        );
    }

    #[test]
    fn follows_tip_when_store_is_ahead_of_upstream() {
        // Local reorg or invalidateblock race: gap saturates to zero
        // and we land in FollowingTip; the handler parks until the
        // replacement chain re-emerges.
        assert_eq!(
            classify_phase(Some(1_000), 500, 100),
            IngestPhase::FollowingTip
        );
    }

    #[test]
    fn follows_tip_when_already_at_upstream_tip() {
        assert_eq!(
            classify_phase(Some(1_000), 1_000, 100),
            IngestPhase::FollowingTip
        );
    }
}
