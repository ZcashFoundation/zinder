use std::{
    num::{NonZeroU32, NonZeroU64},
    path::Path,
    time::Duration,
};

use tempfile::TempDir;
use zinder_core::{
    BlockHash, BlockHeight, ChainEpoch, CompactTransactionData, CompactTransparentInput,
    CompactTransparentOutput, MempoolEntry, MempoolEvictionReason, MempoolObservation, Network,
    RawTransactionBytes, TransactionId, UnixTimestampMillis,
};

use super::{
    CanonicalStoreError, RocksDbCanonicalStore,
    live_replacement_tests::{open_store, published_store},
};
use crate::{
    EventStreamStartPosition, MempoolEvent, MempoolEventEnvelope, MempoolEventHistoryRequest,
    MempoolEventRetentionConfig, MempoolEventRetentionStepBudget, StreamCursorTokenV1,
};

const REORG_WINDOW: u32 = 2;
const RECENT_OBSERVATION: UnixTimestampMillis = UnixTimestampMillis::new(1_000_000_000);

#[test]
fn mempool_event_history_round_trips_every_variant_in_append_order()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = ready_store(temporary.path())?;
    let added = synthetic_entry(0xA0, store.chain_epoch()?)?;

    let added_envelope = store.append_mempool_event(
        MempoolEvent::Added {
            entry: added.clone(),
        },
        UnixTimestampMillis::new(1_000),
    )?;
    let mined_envelope = store.append_mempool_event(
        MempoolEvent::Mined {
            transaction_id: added.transaction_id(),
            mined_height: BlockHeight::new(7),
            block_hash: BlockHash::from_bytes([0xC7; 32]),
        },
        UnixTimestampMillis::new(2_000),
    )?;
    let invalidated_envelope = store.append_mempool_event(
        MempoolEvent::Invalidated {
            transaction_id: TransactionId::from_bytes([0xB0; 32]),
            reason: MempoolEvictionReason::Conflict,
        },
        UnixTimestampMillis::new(3_000),
    )?;

    let history =
        store.mempool_event_history(MempoolEventHistoryRequest::with_default_limit(None))?;
    assert_eq!(
        history,
        vec![added_envelope, mined_envelope, invalidated_envelope]
    );
    assert_eq!(event_sequences(&history), vec![1, 2, 3]);
    Ok(())
}

#[test]
fn mempool_event_history_resumes_strictly_after_its_cursor()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = ready_store(temporary.path())?;
    let first = append_added(&store, 0xA0, UnixTimestampMillis::new(1_000))?;
    let second = append_added(&store, 0xA1, UnixTimestampMillis::new(2_000))?;

    let resumed = store.mempool_event_history(MempoolEventHistoryRequest::with_default_limit(
        Some(&first.cursor),
    ))?;
    assert_eq!(resumed, vec![second.clone()]);

    let from_latest = store.mempool_event_history(
        MempoolEventHistoryRequest::with_default_limit(Some(&second.cursor)),
    )?;
    assert!(from_latest.is_empty());
    Ok(())
}

#[test]
fn tampered_mempool_event_cursor_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = ready_store(temporary.path())?;
    let appended = append_added(&store, 0xA0, UnixTimestampMillis::new(1_000))?;
    let mut cursor_bytes = appended.cursor.as_bytes().to_vec();
    *cursor_bytes
        .last_mut()
        .ok_or("mempool cursor must carry authentication bytes")? ^= 1;
    let tampered = StreamCursorTokenV1::from_bytes(cursor_bytes);

    let error = store
        .mempool_event_history(MempoolEventHistoryRequest::with_default_limit(Some(
            &tampered,
        )))
        .err()
        .ok_or("a tampered mempool cursor must be rejected")?;
    assert!(
        matches!(error, CanonicalStoreError::MempoolEventCursorInvalid { .. }),
        "unexpected tampered-cursor error: {error:?}"
    );
    let start_error = store
        .resolve_mempool_event_stream_start(&EventStreamStartPosition::AfterCursor(tampered))
        .err()
        .ok_or("a tampered mempool cursor must not resolve as a stream start")?;
    assert!(
        matches!(
            start_error,
            CanonicalStoreError::MempoolEventCursorInvalid { .. }
        ),
        "unexpected tampered stream-start error: {start_error:?}"
    );
    Ok(())
}

#[test]
fn mempool_snapshot_cursor_round_trips_its_anchor_and_paging_position()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = ready_store(temporary.path())?;
    let anchor_envelope = append_added(&store, 0xA5, UnixTimestampMillis::new(1_000))?;
    let after = TransactionId::from_bytes([0xA0; 32]);

    let first_page = store.begin_mempool_snapshot(&[])?;
    assert_eq!(first_page.after_transaction_id(), None);
    assert_eq!(
        first_page.events_resume_anchor(),
        Some(anchor_envelope.position())
    );
    assert_eq!(
        first_page.events_resume_cursor(),
        Some(&anchor_envelope.cursor)
    );

    let anchored =
        store.encode_mempool_snapshot_next_cursor(first_page.events_resume_anchor(), after)?;
    let next_page = store.begin_mempool_snapshot(anchored.as_bytes())?;
    assert_eq!(next_page.after_transaction_id(), Some(after));
    assert_eq!(
        next_page.events_resume_anchor(),
        Some(anchor_envelope.position())
    );
    assert_eq!(
        next_page.events_resume_cursor(),
        Some(&anchor_envelope.cursor)
    );

    let unanchored = store.encode_mempool_snapshot_next_cursor(None, after)?;
    let unanchored_page = store.begin_mempool_snapshot(unanchored.as_bytes())?;
    assert_eq!(unanchored_page.after_transaction_id(), Some(after));
    assert_eq!(unanchored_page.events_resume_anchor(), None);
    assert_eq!(unanchored_page.events_resume_cursor(), None);

    let mut tampered = anchored.as_bytes().to_vec();
    *tampered
        .last_mut()
        .ok_or("snapshot cursor must carry authentication bytes")? ^= 1;
    let error = store
        .begin_mempool_snapshot(&tampered)
        .err()
        .ok_or("a tampered snapshot cursor must be rejected")?;
    assert!(
        matches!(
            error,
            CanonicalStoreError::MempoolSnapshotCursorInvalid { .. }
        ),
        "unexpected tampered snapshot error: {error:?}"
    );
    Ok(())
}

#[test]
fn mempool_live_tail_start_skips_prior_events_and_delivers_later_appends()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = ready_store(temporary.path())?;
    assert_eq!(
        store.resolve_mempool_event_stream_start(&EventStreamStartPosition::LiveTail)?,
        None,
        "live tail over an empty mempool log starts at the earliest retained event"
    );
    let _first = append_added(&store, 0xA0, UnixTimestampMillis::new(1_000))?;
    let second = append_added(&store, 0xA1, UnixTimestampMillis::new(2_000))?;

    let head_cursor = store
        .resolve_mempool_event_stream_start(&EventStreamStartPosition::LiveTail)?
        .ok_or("live tail over retained events must resolve to a cursor")?;
    assert_eq!(head_cursor, second.cursor);
    assert!(
        store
            .mempool_event_history(MempoolEventHistoryRequest::with_default_limit(Some(
                &head_cursor
            )))?
            .is_empty()
    );

    let third = append_added(&store, 0xA2, UnixTimestampMillis::new(3_000))?;
    let delivered = store.mempool_event_history(MempoolEventHistoryRequest::with_default_limit(
        Some(&head_cursor),
    ))?;
    assert_eq!(delivered, vec![third]);
    Ok(())
}

#[test]
fn mempool_retention_advances_the_floor_once_and_is_idempotent_on_repeat()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = ready_store(temporary.path())?;
    let stale_one = append_mined(&store, 0xA0, UnixTimestampMillis::new(0))?;
    let _stale_two = append_mined(&store, 0xA1, UnixTimestampMillis::new(0))?;
    let recent = append_added(&store, 0xA2, RECENT_OBSERVATION)?;

    let first = drain_mempool_retention(&store)?;
    assert_eq!(first.pruned_mined_count, 2);
    assert_eq!(first.oldest_retained_sequence, Some(recent.event_sequence));
    assert!(
        first
            .oldest_retained_sequence
            .is_some_and(|floor| floor > stale_one.event_sequence)
    );

    let second = drain_mempool_retention(&store)?;
    assert_eq!(second.pruned_total(), 0);
    assert_eq!(
        second.oldest_retained_sequence,
        first.oldest_retained_sequence
    );
    assert_eq!(
        store.mempool_event_history(MempoolEventHistoryRequest::with_default_limit(None))?,
        vec![recent]
    );
    Ok(())
}

#[test]
fn mempool_retention_keeps_the_only_current_event() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = ready_store(temporary.path())?;
    let only = append_added(&store, 0xA0, RECENT_OBSERVATION)?;

    let report = drain_mempool_retention(&store)?;
    assert_eq!(report.pruned_total(), 0);
    assert_eq!(report.oldest_retained_sequence, Some(only.event_sequence));
    assert_eq!(report.retained_event_count, 1);
    assert_eq!(
        store.mempool_event_history(MempoolEventHistoryRequest::with_default_limit(None))?,
        vec![only]
    );
    Ok(())
}

#[test]
fn mempool_cursor_below_the_pruned_floor_expires() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = ready_store(temporary.path())?;
    let stale = append_mined(&store, 0xA0, UnixTimestampMillis::new(0))?;
    let _stale_two = append_mined(&store, 0xA1, UnixTimestampMillis::new(0))?;
    let _recent = append_added(&store, 0xA2, RECENT_OBSERVATION)?;

    let report = drain_mempool_retention(&store)?;
    assert_eq!(report.pruned_mined_count, 2);

    let error = store
        .mempool_event_history(MempoolEventHistoryRequest::with_default_limit(Some(
            &stale.cursor,
        )))
        .err()
        .ok_or("a cursor below the retention floor must expire")?;
    assert!(
        matches!(
            error,
            CanonicalStoreError::MempoolEventCursorExpired {
                event_sequence: 1,
                oldest_retained_sequence: 3,
            }
        ),
        "unexpected expired-cursor error: {error:?}"
    );
    Ok(())
}

#[test]
fn mempool_retention_floor_persists_across_primary_reopen() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let pruned_floor = {
        let store = published_store(
            &store_path,
            REORG_WINDOW,
            BlockHeight::new(4),
            BlockHeight::new(2),
        )?;
        let _stale = append_mined(&store, 0xA0, UnixTimestampMillis::new(0))?;
        let _recent = append_added(&store, 0xA1, RECENT_OBSERVATION)?;
        drain_mempool_retention(&store)?
            .oldest_retained_sequence
            .ok_or("retention must keep the recent event")?
    };

    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let reopened = open_store(&store_path, &activations, REORG_WINDOW)?;
    assert_eq!(
        reopened
            .mempool_event_retention_report()?
            .oldest_retained_sequence,
        Some(pruned_floor)
    );
    assert_eq!(
        event_sequences(
            &reopened
                .mempool_event_history(MempoolEventHistoryRequest::with_default_limit(None))?
        ),
        vec![pruned_floor]
    );
    Ok(())
}

fn ready_store(directory: &Path) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    published_store(
        &directory.join("canonical"),
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )
}

/// Runs bounded retention steps until the scan stops on its own, using a
/// one-second window for every event variant.
fn drain_mempool_retention(
    store: &RocksDbCanonicalStore,
) -> Result<crate::MempoolEventRetentionReport, Box<dyn std::error::Error>> {
    let retention = MempoolEventRetentionConfig::new(Some(Duration::from_secs(1)), None);
    let budget = MempoolEventRetentionStepBudget::new(
        NonZeroU32::new(64).ok_or("zero retention event budget")?,
        NonZeroU64::new(1 << 20).ok_or("zero retention byte budget")?,
    );
    let mut pruned_added_count = 0;
    let mut pruned_mined_count = 0;
    let mut pruned_invalidated_count = 0;
    loop {
        let outcome =
            store.advance_mempool_event_retention(RECENT_OBSERVATION, retention, budget)?;
        pruned_added_count += outcome.report.pruned_added_count;
        pruned_mined_count += outcome.report.pruned_mined_count;
        pruned_invalidated_count += outcome.report.pruned_invalidated_count;
        if !outcome.has_immediate_work() {
            let mut report = outcome.report;
            report.pruned_added_count = pruned_added_count;
            report.pruned_mined_count = pruned_mined_count;
            report.pruned_invalidated_count = pruned_invalidated_count;
            return Ok(report);
        }
    }
}

fn append_added(
    store: &RocksDbCanonicalStore,
    transaction_id_byte: u8,
    observed_at: UnixTimestampMillis,
) -> Result<MempoolEventEnvelope, Box<dyn std::error::Error>> {
    Ok(store.append_mempool_event(
        MempoolEvent::Added {
            entry: synthetic_entry(transaction_id_byte, store.chain_epoch()?)?,
        },
        observed_at,
    )?)
}

fn append_mined(
    store: &RocksDbCanonicalStore,
    transaction_id_byte: u8,
    observed_at: UnixTimestampMillis,
) -> Result<MempoolEventEnvelope, CanonicalStoreError> {
    store.append_mempool_event(
        MempoolEvent::Mined {
            transaction_id: TransactionId::from_bytes([transaction_id_byte; 32]),
            mined_height: BlockHeight::new(1),
            block_hash: BlockHash::from_bytes([transaction_id_byte; 32]),
        },
        observed_at,
    )
}

fn synthetic_entry(
    transaction_id_byte: u8,
    chain_epoch: ChainEpoch,
) -> Result<MempoolEntry, Box<dyn std::error::Error>> {
    Ok(MempoolEntry::new(
        TransactionId::from_bytes([transaction_id_byte; 32]),
        None,
        RawTransactionBytes::new(vec![transaction_id_byte; 8]),
        CompactTransactionData {
            fee_zat: None,
            sapling_spends: Vec::new(),
            sapling_outputs: Vec::new(),
            orchard_actions: Vec::new(),
            ironwood_actions: Vec::new(),
            transparent_inputs: vec![CompactTransparentInput {
                previous_transaction_id: TransactionId::from_bytes(
                    [transaction_id_byte.wrapping_add(1); 32],
                ),
                previous_output_index: 0,
            }],
            transparent_outputs: vec![CompactTransparentOutput {
                script_pub_key: vec![transaction_id_byte; 25],
                value_zat: 1_000,
            }],
        },
        MempoolObservation {
            first_seen_unix_millis: UnixTimestampMillis::new(1_700_000_000_000),
            first_seen_chain_epoch: chain_epoch,
        },
    )?)
}

fn event_sequences(envelopes: &[MempoolEventEnvelope]) -> Vec<u64> {
    envelopes
        .iter()
        .map(|envelope| envelope.event_sequence)
        .collect()
}
