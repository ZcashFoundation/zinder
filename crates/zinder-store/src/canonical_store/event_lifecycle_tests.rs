use std::{cell::RefCell, num::NonZeroU32, path::Path};

use tempfile::TempDir;
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, ChainEpochId, Network, UnixTimestampMillis,
};

use super::{
    CanonicalEventRetentionStepBudget, CanonicalEventRetentionStepStop, CanonicalLiveAppend,
    CanonicalReorgPolicy, CanonicalStoreError, ProjectionBuildLease, ProjectionBuildLeaseId,
    RocksDbCanonicalSecondary, RocksDbCanonicalStore,
    live_replacement_tests::{open_store, published_store, replacement_block},
    publication::read_chain_epoch_tips,
};
use crate::{
    ChainEvent, ChainEventEnvelope, ChainEventHistoryRequest, ChainEventStreamFamily,
    EventStreamStartPosition, RawBlobRetention, RocksDbResourceBudget,
};

const REORG_WINDOW: u32 = 2;
const BASELINE_CREATED_AT: u64 = 1_750_000_000_001;

#[test]
fn wallet_event_history_returns_bounded_pages_that_resume_after_the_last_cursor()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let (_store, secondary) = store_with_two_live_appends(temporary.path())?;

    let first_page = wallet_history(&secondary, None, ChainEventStreamFamily::Visible, 2)?;
    assert_eq!(event_sequences(&first_page), vec![1, 2]);
    let second_page = wallet_history(
        &secondary,
        first_page.last().map(|envelope| &envelope.cursor),
        ChainEventStreamFamily::Visible,
        2,
    )?;
    assert_eq!(event_sequences(&second_page), vec![3]);
    let quiet_page = wallet_history(
        &secondary,
        second_page.last().map(|envelope| &envelope.cursor),
        ChainEventStreamFamily::Visible,
        2,
    )?;
    assert_eq!(quiet_page, []);
    Ok(())
}

#[test]
fn settled_family_delivers_each_height_once_as_the_reorg_window_advances()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let build_tip = BlockHeight::new(4);
    let mut store = published_store(&store_path, REORG_WINDOW, build_tip, build_tip)?;
    let first_height = store.ready_evidence().first_retained_block.height.value();
    for height in 5_u8..=8 {
        let settled_height = build_tip.max(BlockHeight::new(u32::from(height) - REORG_WINDOW));
        store = append_block(
            store,
            &activations,
            height,
            settled_height,
            u64::from(height),
        )?;
    }
    let secondary = open_secondary(&store_path, &temporary.path().join("secondary"))?;
    let settled = wallet_history(&secondary, None, ChainEventStreamFamily::Settled, 10)?;
    assert_eq!(event_sequences(&settled), [1, 4, 5]);
    for (envelope, (start, end)) in settled.iter().zip([(first_height, 4), (5, 5), (6, 6)]) {
        let ChainEvent::ChainCommitted { committed } = &envelope.event else {
            return Err("settled history must contain only newly settled commits".into());
        };
        assert_eq!(committed.chain_epoch, envelope.chain_epoch);
        assert_eq!(
            committed.block_range,
            BlockHeightRange::inclusive(BlockHeight::new(start), BlockHeight::new(end))
        );
        let payload = envelope
            .cursor
            .decode_chain_event(Network::ZcashTestnet, secondary.cursor_auth_key)?;
        assert_eq!(payload.family, ChainEventStreamFamily::Settled);
        assert_eq!(payload.event_sequence, envelope.event_sequence);
        assert_eq!(payload.locator.tip().height, BlockHeight::new(end));
        assert_eq!(
            payload.locator.tip().hash,
            envelope.chain_epoch.settled_tip_hash
        );
        assert!(
            payload
                .locator
                .entries()
                .iter()
                .all(|anchor| anchor.height <= BlockHeight::new(end))
        );
    }
    Ok(())
}

#[test]
fn wallet_cursor_from_another_canonical_store_is_rejected() -> Result<(), Box<dyn std::error::Error>>
{
    let first_directory = TempDir::new()?;
    let second_directory = TempDir::new()?;
    let (_first_store, first_secondary) = store_with_two_live_appends(first_directory.path())?;
    let (_second_store, second_secondary) = store_with_two_live_appends(second_directory.path())?;
    let foreign_cursor =
        wallet_history(&first_secondary, None, ChainEventStreamFamily::Visible, 1)?
            .into_iter()
            .next()
            .ok_or("first store projected no event")?
            .cursor;

    let error = wallet_history(
        &second_secondary,
        Some(&foreign_cursor),
        ChainEventStreamFamily::Visible,
        10,
    )
    .err()
    .ok_or("a cursor minted by another store must be rejected")?;
    assert!(
        matches!(
            error,
            CanonicalStoreError::CanonicalEventCursorMalformed { .. }
        ),
        "unexpected foreign-cursor error: {error:?}"
    );
    let start_error = second_secondary
        .resolve_wallet_chain_event_stream_start(
            &EventStreamStartPosition::AfterCursor(foreign_cursor),
            ChainEventStreamFamily::Visible,
        )
        .err()
        .ok_or("a foreign cursor must not resolve as a stream start")?;
    assert!(
        matches!(
            start_error,
            CanonicalStoreError::CanonicalEventCursorMalformed { .. }
        ),
        "unexpected foreign stream-start error: {start_error:?}"
    );
    Ok(())
}

#[test]
fn live_tail_start_skips_prior_events_and_delivers_later_commits()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )?;
    let store = append_block(store, &activations, 5, BlockHeight::new(3), 2)?;
    let mut secondary = open_secondary(&store_path, &temporary.path().join("secondary"))?;

    let resume = secondary.resolve_wallet_chain_event_stream_start(
        &EventStreamStartPosition::LiveTail,
        ChainEventStreamFamily::Visible,
    )?;
    assert_eq!(resume.family, ChainEventStreamFamily::Visible);
    let head_cursor = resume
        .cursor
        .ok_or("live tail over retained events must mint a head cursor")?;
    assert_eq!(
        wallet_history(
            &secondary,
            Some(&head_cursor),
            ChainEventStreamFamily::Visible,
            10
        )?,
        []
    );

    let _store = append_block(store, &activations, 6, BlockHeight::new(4), 3)?;
    secondary.try_catch_up()?;
    let delivered = wallet_history(
        &secondary,
        Some(&head_cursor),
        ChainEventStreamFamily::Visible,
        10,
    )?;
    assert_eq!(event_sequences(&delivered), vec![3]);
    assert_eq!(
        delivered
            .first()
            .map(|envelope| envelope.chain_epoch.visible_tip_height),
        Some(BlockHeight::new(6))
    );
    Ok(())
}

#[test]
fn live_tail_start_mints_a_cursor_in_the_requested_family() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = TempDir::new()?;
    let (_store, secondary) = store_with_two_live_appends(temporary.path())?;

    let resume = secondary.resolve_wallet_chain_event_stream_start(
        &EventStreamStartPosition::LiveTail,
        ChainEventStreamFamily::Settled,
    )?;
    assert_eq!(resume.family, ChainEventStreamFamily::Settled);
    let head_cursor = resume
        .cursor
        .ok_or("live tail over retained events must mint a head cursor")?;
    let payload =
        head_cursor.decode_chain_event(Network::ZcashTestnet, secondary.cursor_auth_key)?;
    assert_eq!(payload.family, ChainEventStreamFamily::Settled);
    assert_eq!(payload.event_sequence, 3);
    assert_eq!(payload.locator.tip().height, BlockHeight::new(4));
    Ok(())
}

#[test]
fn after_cursor_start_takes_the_cursor_family_as_authoritative()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let (_store, secondary) = store_with_two_live_appends(temporary.path())?;
    let settled_cursor = wallet_history(&secondary, None, ChainEventStreamFamily::Settled, 1)?
        .into_iter()
        .next()
        .ok_or("expected a settled-family event")?
        .cursor;

    let resume = secondary.resolve_wallet_chain_event_stream_start(
        &EventStreamStartPosition::AfterCursor(settled_cursor.clone()),
        ChainEventStreamFamily::Visible,
    )?;

    assert_eq!(resume.family, ChainEventStreamFamily::Settled);
    assert_eq!(resume.cursor, Some(settled_cursor));
    Ok(())
}

#[test]
fn created_at_retention_prunes_the_prefix_expires_stale_cursors_and_keeps_the_newest_event()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let (store, mut secondary) = store_with_two_live_appends(temporary.path())?;
    let stale_cursor = wallet_history(&secondary, None, ChainEventStreamFamily::Visible, 1)?
        .into_iter()
        .next()
        .ok_or("expected the baseline event")?
        .cursor;

    let report = store.prune_canonical_events_before_created_at(
        UnixTimestampMillis::new(BASELINE_CREATED_AT + 2),
        UnixTimestampMillis::new(BASELINE_CREATED_AT + 2),
    )?;
    assert_eq!(report.current_event_sequence, 3);
    assert_eq!(report.oldest_retained_sequence, 3);
    assert_eq!(report.pruned_event_count, 1);
    secondary.try_catch_up()?;

    let retained = wallet_history(&secondary, None, ChainEventStreamFamily::Visible, 10)?;
    assert_eq!(event_sequences(&retained), vec![3]);
    let error = wallet_history(
        &secondary,
        Some(&stale_cursor),
        ChainEventStreamFamily::Visible,
        10,
    )
    .err()
    .ok_or("a cursor below the retention floor must expire")?;
    assert!(
        matches!(
            error,
            CanonicalStoreError::CanonicalEventCursorExpired {
                event_sequence: 1,
                oldest_retained_sequence: 3,
            }
        ),
        "unexpected stale-cursor error: {error:?}"
    );

    let far_future = UnixTimestampMillis::new(BASELINE_CREATED_AT + 1_000_000);
    let newest_only = store.prune_canonical_events_before_created_at(far_future, far_future)?;
    assert_eq!(newest_only.oldest_retained_sequence, 3);
    assert_eq!(newest_only.pruned_event_count, 0);
    secondary.try_catch_up()?;
    assert_eq!(
        event_sequences(&wallet_history(
            &secondary,
            None,
            ChainEventStreamFamily::Visible,
            10
        )?),
        vec![3]
    );
    Ok(())
}

/// Publishes a baseline at height 4 and appends heights 5 and 6, producing
/// three retained events whose visible tips all lead their settled tips.
fn store_with_two_live_appends(
    directory: &Path,
) -> Result<(RocksDbCanonicalStore, RocksDbCanonicalSecondary), Box<dyn std::error::Error>> {
    let store_path = directory.join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )?;
    let store = append_block(store, &activations, 5, BlockHeight::new(3), 1)?;
    let store = append_block(store, &activations, 6, BlockHeight::new(4), 2)?;
    let secondary = open_secondary(&store_path, &directory.join("secondary"))?;
    Ok((store, secondary))
}

fn append_block(
    store: RocksDbCanonicalStore,
    activations: &zinder_core::NetworkUpgradeActivations,
    height: u8,
    settled_height: BlockHeight,
    created_at_offset: u64,
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    let fence = store.event_fence();
    let settled_byte = u8::try_from(settled_height.value())?;
    let (store, _) = store.commit_live_append(
        CanonicalLiveAppend::new(
            fence,
            replacement_block(
                BlockHeight::new(u32::from(height)),
                [height; 32],
                [height - 1; 32],
                100 + height,
            )?,
            Vec::new(),
            BlockId::new(settled_height, BlockHash::from_bytes([settled_byte; 32])),
            UnixTimestampMillis::new(BASELINE_CREATED_AT + created_at_offset),
        ),
        activations,
    )?;
    Ok(store)
}

fn open_secondary(
    store_path: &Path,
    secondary_path: &Path,
) -> Result<RocksDbCanonicalSecondary, Box<dyn std::error::Error>> {
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    Ok(RocksDbCanonicalSecondary::open_ready(
        store_path,
        secondary_path,
        &activations,
        crate::CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::None),
        CanonicalReorgPolicy::new(REORG_WINDOW)?,
        RocksDbResourceBudget::for_local_tests(),
    )?)
}

fn wallet_history(
    secondary: &RocksDbCanonicalSecondary,
    from_cursor: Option<&crate::StreamCursorTokenV1>,
    family: ChainEventStreamFamily,
    max_events: u32,
) -> Result<Vec<ChainEventEnvelope>, CanonicalStoreError> {
    let max_events =
        NonZeroU32::new(max_events).ok_or(CanonicalStoreError::CanonicalEventCursorMalformed {
            reason: "test page limit must be non-zero",
        })?;
    secondary.wallet_chain_event_history(ChainEventHistoryRequest::new_for_family(
        from_cursor,
        family,
        max_events,
    ))
}

fn event_sequences(envelopes: &[ChainEventEnvelope]) -> Vec<u64> {
    envelopes
        .iter()
        .map(|envelope| envelope.event_sequence)
        .collect()
}

fn retention_budget(
    max_inspected: u32,
    max_deleted: u32,
) -> Result<CanonicalEventRetentionStepBudget, Box<dyn std::error::Error>> {
    Ok(CanonicalEventRetentionStepBudget::new(
        NonZeroU32::new(max_inspected).ok_or("max_inspected must be nonzero")?,
        NonZeroU32::new(max_deleted).ok_or("max_deleted must be nonzero")?,
    ))
}

fn store_with_ten_events(
    directory: &Path,
) -> Result<(RocksDbCanonicalStore, RocksDbCanonicalSecondary), Box<dyn std::error::Error>> {
    let store_path = directory.join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let mut store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )?;
    for height in 5_u8..=13 {
        let created_at_offset = if height <= 11 {
            u64::from(height - 4)
        } else {
            u64::from(height - 11) * 100
        };
        store = append_block(
            store,
            &activations,
            height,
            BlockHeight::new(u32::from(height - 2)),
            created_at_offset,
        )?;
    }
    let secondary = open_secondary(&store_path, &directory.join("secondary"))?;
    Ok((store, secondary))
}

fn assert_observed_step(
    store: &RocksDbCanonicalStore,
    cutoff: UnixTimestampMillis,
    now: UnixTimestampMillis,
    budget: CanonicalEventRetentionStepBudget,
) -> Result<(super::CanonicalEventRetentionStepOutcome, Vec<u64>), Box<dyn std::error::Error>> {
    let inspected = RefCell::new(Vec::new());
    let outcome =
        store.advance_canonical_event_retention_with_observer(cutoff, now, budget, |sequence| {
            inspected.borrow_mut().push(sequence);
        })?;
    Ok((outcome, inspected.into_inner()))
}

fn assert_cursor_expired(
    secondary: &RocksDbCanonicalSecondary,
    cursor: &crate::StreamCursorTokenV1,
    expected_sequence: u64,
    expected_floor: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let error = wallet_history(secondary, Some(cursor), ChainEventStreamFamily::Visible, 10)
        .err()
        .ok_or("expected expired cursor")?;
    assert!(
        matches!(
            error,
            CanonicalStoreError::CanonicalEventCursorExpired {
                event_sequence,
                oldest_retained_sequence,
            } if event_sequence == expected_sequence
                && oldest_retained_sequence == expected_floor
        ),
        "unexpected cursor error: {error:?}"
    );
    Ok(())
}

#[test]
fn bounded_retention_noop_when_floor_at_head_or_unexpired() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = TempDir::new()?;
    let (store, _secondary) = store_with_ten_events(temporary.path())?;
    let budget = retention_budget(4, 4)?;
    let before_all = UnixTimestampMillis::new(BASELINE_CREATED_AT);
    let (unexpired_step, inspected_unexpired) =
        assert_observed_step(&store, before_all, before_all, budget)?;
    assert_eq!(inspected_unexpired, vec![1]);
    assert_eq!(unexpired_step.inspected_event_count, 1);
    assert_eq!(unexpired_step.report.oldest_retained_sequence, 1);
    assert_eq!(unexpired_step.report.pruned_event_count, 0);
    assert_eq!(
        unexpired_step.stop,
        CanonicalEventRetentionStepStop::ReachedUnexpiredEvent
    );

    store.prune_canonical_events_before(10, before_all)?;
    let far_future = UnixTimestampMillis::new(BASELINE_CREATED_AT + 1_000);
    let (at_head_step, inspected_at_head) =
        assert_observed_step(&store, far_future, far_future, budget)?;
    assert_eq!(inspected_at_head, [] as [u64; 0]);
    assert_eq!(at_head_step.inspected_event_count, 0);
    assert_eq!(at_head_step.report.oldest_retained_sequence, 10);
    assert_eq!(at_head_step.report.pruned_event_count, 0);
    assert_eq!(
        at_head_step.stop,
        CanonicalEventRetentionStepStop::ReachedHead
    );
    Ok(())
}

#[test]
fn bounded_retention_resumes_across_asymmetric_budgets_restarts_and_preserves_epochs()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let (store, mut secondary) = store_with_ten_events(temporary.path())?;
    let events = wallet_history(&secondary, None, ChainEventStreamFamily::Visible, 10)?;
    assert_eq!(event_sequences(&events), (1..=10).collect::<Vec<_>>());
    let cutoff = UnixTimestampMillis::new(BASELINE_CREATED_AT + 50);

    let (step_one, inspected_one) =
        assert_observed_step(&store, cutoff, cutoff, retention_budget(3, 2)?)?;
    assert_eq!(inspected_one, vec![1, 2, 3]);
    assert_eq!(step_one.inspected_event_count, 3);
    assert_eq!(step_one.report.oldest_retained_sequence, 4);
    assert_eq!(step_one.report.pruned_event_count, 2);
    assert_eq!(
        step_one.stop,
        CanonicalEventRetentionStepStop::BudgetExhausted
    );
    assert!(step_one.has_immediate_work());

    drop(store);
    let store = open_store(&store_path, &activations, REORG_WINDOW)?;
    assert_eq!(store.canonical_event_retention_floor()?, 4);
    secondary.try_catch_up()?;
    assert_cursor_expired(&secondary, &events[1].cursor, 2, 4)?;
    assert_eq!(
        event_sequences(&wallet_history(
            &secondary,
            Some(&events[2].cursor),
            ChainEventStreamFamily::Visible,
            2,
        )?),
        vec![4, 5]
    );

    let (step_two, inspected_two) =
        assert_observed_step(&store, cutoff, cutoff, retention_budget(2, 5)?)?;
    assert_eq!(inspected_two, vec![4, 5]);
    assert_eq!(step_two.report.oldest_retained_sequence, 6);
    assert_eq!(step_two.report.pruned_event_count, 2);
    assert_eq!(
        step_two.stop,
        CanonicalEventRetentionStepStop::BudgetExhausted
    );

    let (step_three, inspected_three) =
        assert_observed_step(&store, cutoff, cutoff, retention_budget(10, 10)?)?;
    assert_eq!(inspected_three, vec![6, 7, 8, 9]);
    assert_eq!(step_three.report.oldest_retained_sequence, 9);
    assert_eq!(step_three.report.pruned_event_count, 3);
    assert_eq!(
        step_three.stop,
        CanonicalEventRetentionStepStop::ReachedUnexpiredEvent
    );

    secondary.try_catch_up()?;
    assert_cursor_expired(&secondary, &events[6].cursor, 7, 9)?;
    assert_eq!(
        event_sequences(&wallet_history(
            &secondary,
            Some(&events[7].cursor),
            ChainEventStreamFamily::Visible,
            10,
        )?),
        vec![9, 10]
    );
    for epoch in 1_u64..=10 {
        let _tips = read_chain_epoch_tips(&store.bounded_open.db, ChainEpochId::new(epoch))?;
    }
    Ok(())
}

fn acquire_test_lease(
    store: &RocksDbCanonicalStore,
    tag: u8,
    anchor: super::ProjectionBuildAnchor,
    expires_at: UnixTimestampMillis,
    now: UnixTimestampMillis,
) -> Result<ProjectionBuildLease, Box<dyn std::error::Error>> {
    Ok(store.acquire_projection_build_lease(
        ProjectionBuildLease::new(
            ProjectionBuildLeaseId::from_bytes([tag; 16]),
            anchor,
            expires_at,
        ),
        now,
    )?)
}

fn assert_pinned_lease_blocks_scans(
    store: &RocksDbCanonicalStore,
    cutoff: UnixTimestampMillis,
    now: UnixTimestampMillis,
    expected_floor: u64,
    expected_anchor: super::ProjectionBuildAnchor,
) -> Result<(), Box<dyn std::error::Error>> {
    let (pinned_step, inspected_pinned) =
        assert_observed_step(store, cutoff, now, retention_budget(5, 5)?)?;
    assert_eq!(inspected_pinned, [] as [u64; 0]);
    assert_eq!(pinned_step.report.oldest_retained_sequence, expected_floor);
    assert_eq!(
        pinned_step.report.lease_protected_anchor,
        Some(expected_anchor)
    );
    assert_eq!(
        pinned_step.stop,
        CanonicalEventRetentionStepStop::LeaseProtected
    );
    let (_, inspected_again) = assert_observed_step(store, cutoff, now, retention_budget(5, 5)?)?;
    assert_eq!(inspected_again, [] as [u64; 0]);
    Ok(())
}

fn setup_pinned_test_leases(
    store: &RocksDbCanonicalStore,
    t0: UnixTimestampMillis,
) -> Result<
    (
        ProjectionBuildLease,
        ProjectionBuildLease,
        ProjectionBuildLease,
    ),
    Box<dyn std::error::Error>,
> {
    let events = store.canonical_event_history(super::CanonicalEventHistoryRequest::new(
        None,
        NonZeroU32::new(10).ok_or("limit must be nonzero")?,
    ))?;
    let lease_one = acquire_test_lease(
        store,
        1,
        events[0].projection_build_anchor(),
        UnixTimestampMillis::new(t0.value() + 100),
        t0,
    )?;
    let lease_five = acquire_test_lease(
        store,
        5,
        events[4].projection_build_anchor(),
        UnixTimestampMillis::new(t0.value() + 500),
        t0,
    )?;
    let expired_lease_two = acquire_test_lease(
        store,
        2,
        events[1].projection_build_anchor(),
        UnixTimestampMillis::new(t0.value() + 10),
        t0,
    )?;
    Ok((lease_one, lease_five, expired_lease_two))
}

#[test]
fn bounded_retention_skips_lease_pinned_prefix_and_cleans_expired_leases_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let (store, mut secondary) = store_with_ten_events(temporary.path())?;
    let first_cursor = wallet_history(&secondary, None, ChainEventStreamFamily::Visible, 1)?
        .into_iter()
        .next()
        .ok_or("expected baseline wallet event")?
        .cursor;
    let t0 = UnixTimestampMillis::new(BASELINE_CREATED_AT + 1_000);
    let cutoff = t0;
    let (lease_one, lease_five, expired_lease_two) = setup_pinned_test_leases(&store, t0)?;

    let t_after_two = UnixTimestampMillis::new(t0.value() + 20);
    assert_pinned_lease_blocks_scans(&store, cutoff, t_after_two, 1, lease_one.anchor())?;
    assert!(
        store
            .active_projection_build_lease(expired_lease_two.id(), t0)?
            .is_none(),
        "expired lease must be deleted atomically even when pruning is pinned"
    );

    let t_after_one = UnixTimestampMillis::new(t0.value() + 150);
    let (step_one, inspected_one) =
        assert_observed_step(&store, cutoff, t_after_one, retention_budget(2, 2)?)?;
    assert_eq!(inspected_one, vec![1, 2]);
    assert_eq!(step_one.report.oldest_retained_sequence, 3);
    assert_eq!(
        step_one.stop,
        CanonicalEventRetentionStepStop::BudgetExhausted
    );
    assert!(
        store
            .active_projection_build_lease(lease_one.id(), t0)?
            .is_none()
    );

    let (step_two, inspected_two) =
        assert_observed_step(&store, cutoff, t_after_one, retention_budget(5, 5)?)?;
    assert_eq!(inspected_two, vec![3, 4]);
    assert_eq!(step_two.report.oldest_retained_sequence, 5);
    assert_eq!(
        step_two.stop,
        CanonicalEventRetentionStepStop::LeaseProtected
    );
    assert_pinned_lease_blocks_scans(&store, cutoff, t_after_one, 5, lease_five.anchor())?;

    let t_after_five = UnixTimestampMillis::new(t0.value() + 600);
    let (drain_one, _) =
        assert_observed_step(&store, cutoff, t_after_five, retention_budget(3, 3)?)?;
    assert_eq!(drain_one.report.oldest_retained_sequence, 8);
    assert_eq!(
        drain_one.stop,
        CanonicalEventRetentionStepStop::BudgetExhausted
    );
    assert!(
        store
            .active_projection_build_lease(lease_five.id(), t0)?
            .is_none()
    );
    let (drain_two, _) =
        assert_observed_step(&store, cutoff, t_after_five, retention_budget(3, 3)?)?;
    assert_eq!(drain_two.report.oldest_retained_sequence, 10);
    assert_eq!(drain_two.stop, CanonicalEventRetentionStepStop::ReachedHead);

    secondary.try_catch_up()?;
    assert_eq!(
        event_sequences(&wallet_history(
            &secondary,
            None,
            ChainEventStreamFamily::Visible,
            10,
        )?),
        vec![10]
    );
    assert_cursor_expired(&secondary, &first_cursor, 1, 10)?;
    for epoch in 1_u64..=10 {
        let _tips = read_chain_epoch_tips(&store.bounded_open.db, ChainEpochId::new(epoch))?;
    }
    Ok(())
}
