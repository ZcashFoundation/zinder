use std::{num::NonZeroU32, path::Path};

use tempfile::TempDir;
use zinder_core::{BlockHash, BlockHeight, BlockId, Network, UnixTimestampMillis};

use super::{
    CanonicalLiveAppend, CanonicalReorgPolicy, CanonicalStoreError, CanonicalStoreWorkload,
    RocksDbCanonicalSecondary, RocksDbCanonicalStore,
    live_replacement_tests::{published_store, replacement_block},
};
use crate::{
    ChainEventEnvelope, ChainEventHistoryRequest, ChainEventStreamFamily, EventStreamStartPosition,
    RawBlobRetention, RocksDbResourceBudget,
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
    assert!(quiet_page.is_empty());
    Ok(())
}

#[test]
fn settled_family_history_skips_unsettled_commits_and_mints_settled_cursors()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let secondary = settled_family_store(temporary.path())?;

    let visible = wallet_history(&secondary, None, ChainEventStreamFamily::Visible, 10)?;
    assert_eq!(event_sequences(&visible), vec![1, 2, 3]);
    let settled = wallet_history(&secondary, None, ChainEventStreamFamily::Settled, 10)?;
    assert_eq!(event_sequences(&settled), vec![1, 3]);
    for envelope in &settled {
        let payload = envelope
            .cursor
            .decode_chain_event(Network::ZcashTestnet, secondary.cursor_auth_key)?;
        assert_eq!(payload.family, ChainEventStreamFamily::Settled);
        assert_eq!(
            envelope.chain_epoch.visible_tip_height,
            envelope.chain_epoch.settled_tip_height
        );
    }
    for envelope in &visible {
        let payload = envelope
            .cursor
            .decode_chain_event(Network::ZcashTestnet, secondary.cursor_auth_key)?;
        assert_eq!(payload.family, ChainEventStreamFamily::Visible);
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
    assert!(
        wallet_history(
            &secondary,
            Some(&head_cursor),
            ChainEventStreamFamily::Visible,
            10
        )?
        .is_empty()
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
    Ok(())
}

#[test]
fn after_cursor_start_takes_the_cursor_family_as_authoritative()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let secondary = settled_family_store(temporary.path())?;
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

/// Publishes a settled baseline at height 4, appends height 5 without
/// settling it, then appends height 6 and settles it. Events 1 and 3 belong
/// to the settled family; event 2 does not.
fn settled_family_store(
    directory: &Path,
) -> Result<RocksDbCanonicalSecondary, Box<dyn std::error::Error>> {
    let store_path = directory.join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(4),
    )?;
    let store = append_block(store, &activations, 5, BlockHeight::new(4), 1)?;
    let _store = append_block(store, &activations, 6, BlockHeight::new(6), 2)?;
    open_secondary(&store_path, &directory.join("secondary"))
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
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
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
