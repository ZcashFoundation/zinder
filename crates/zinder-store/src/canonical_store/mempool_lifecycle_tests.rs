use std::{
    collections::{BTreeMap, HashMap},
    num::{NonZeroU32, NonZeroU64},
    path::Path,
    time::Duration,
};

use tempfile::TempDir;
use zinder_core::{
    ArtifactSchemaVersion, BlockHash, BlockHeight, ChainEpoch, CompactTransactionData,
    CompactTransparentInput, CompactTransparentOutput, MempoolEntry, MempoolEvictionReason,
    MempoolObservation, Network, RawTransactionBytes, TransactionId, UnixTimestampMillis,
};
use zinder_proto::v1::wallet;

use super::{
    CanonicalStoreError, RocksDbCanonicalStore,
    live_replacement_tests::{open_store, published_store},
    mempool_lifecycle::ActiveAddSequences,
};
use crate::{
    CANONICAL_STORE_SCHEMA_VERSION, CURRENT_ARTIFACT_SCHEMA_VERSION, EventStreamStartPosition,
    MempoolEvent, MempoolEventEnvelope, MempoolEventHistoryRequest, MempoolEventReadCounts,
    MempoolEventRetentionConfig, MempoolEventRetentionStepBudget, MempoolEventRetentionStepStop,
    StreamCursorTokenV1, mempool_event_envelope_message,
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
    assert_eq!(from_latest, []);
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
    assert_eq!(
        store.mempool_event_history(MempoolEventHistoryRequest::with_default_limit(Some(
            &head_cursor
        )))?,
        []
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

/// A replayed added event reports the artifact schema version of its
/// first-seen epoch that a canonical read of that epoch reports.
///
/// Events saved by earlier releases carry the store layout version in that
/// epoch. After a reopen, the replayed entry and its wire message report the
/// current version instead.
#[test]
fn a_reopened_pre_upgrade_added_event_reports_the_canonical_artifact_schema_version()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let first_seen_epoch_id = {
        let store = published_store(
            &store_path,
            REORG_WINDOW,
            BlockHeight::new(4),
            BlockHeight::new(2),
        )?;
        let pre_upgrade_epoch = ChainEpoch {
            artifact_schema_version: ArtifactSchemaVersion::new(CANONICAL_STORE_SCHEMA_VERSION),
            ..store.chain_epoch()?
        };
        store.append_mempool_event(
            MempoolEvent::Added {
                entry: synthetic_entry(0xA0, pre_upgrade_epoch)?,
            },
            RECENT_OBSERVATION,
        )?;
        pre_upgrade_epoch.id
    };

    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let reopened = open_store(&store_path, &activations, REORG_WINDOW)?;
    let history =
        reopened.mempool_event_history(MempoolEventHistoryRequest::with_default_limit(None))?;
    let [replayed] = history.as_slice() else {
        return Err(format!("expected one replayed event, got {}", history.len()).into());
    };
    let MempoolEvent::Added { entry } = &replayed.event else {
        return Err("the replayed event must be the added event".into());
    };
    let canonical_version = reopened
        .chain_epoch_at(first_seen_epoch_id)?
        .artifact_schema_version;
    assert_eq!(canonical_version, CURRENT_ARTIFACT_SCHEMA_VERSION);
    assert_eq!(
        entry.first_seen_chain_epoch().artifact_schema_version,
        canonical_version
    );
    let message = mempool_event_envelope_message(replayed)?;
    let Some(wallet::mempool_event_envelope::Event::Added(added)) = message.event else {
        return Err("the replayed message must carry the added event".into());
    };
    assert_eq!(
        added
            .entry
            .and_then(|entry| entry.first_seen_chain_epoch)
            .map(|epoch| epoch.artifact_schema_version),
        Some(u32::from(canonical_version.value()))
    );
    Ok(())
}

#[test]
fn active_add_sequences_matches_naive_min_across_duplicate_and_out_of_order_lifecycles() {
    let mut tracked = ActiveAddSequences::default();
    let mut reference = HashMap::new();
    let mut record = |transaction_id: TransactionId, sequence: u64, added: bool| {
        if added {
            tracked.record_added(transaction_id, sequence);
            reference.insert(transaction_id, sequence);
        } else {
            tracked.record_removed(transaction_id);
            reference.remove(&transaction_id);
        }
        assert_eq!(
            tracked.earliest_sequence(),
            reference.values().copied().min()
        );
        assert_eq!(tracked.len(), reference.len());
    };

    for idx in 1_u64..=16 {
        record(indexed_txid(idx), idx, true);
    }
    // Consecutive duplicate Added advances the active sequence for txid 1.
    record(indexed_txid(1), 17, true);
    // Out-of-order removals including earliest active txid 2 and unknown txid 99.
    record(indexed_txid(99), 18, false);
    record(indexed_txid(2), 19, false);
    record(indexed_txid(5), 20, false);
    record(indexed_txid(3), 21, false);
    // Remove-then-re-add lifecycle for txid 1.
    record(indexed_txid(1), 22, false);
    record(indexed_txid(1), 23, true);
    for idx in [1_u64, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] {
        record(indexed_txid(idx), 30 + idx, false);
    }
    assert!(tracked.is_empty());

    // Large unmatched group confirms non-quadratic tracking.
    for idx in 1_u64..=20_000 {
        tracked.record_added(indexed_txid(idx), idx);
    }
    assert_eq!(tracked.earliest_sequence(), Some(1));
    for idx in 1_u64..=20_000 {
        assert_eq!(tracked.earliest_sequence(), Some(idx));
        tracked.record_removed(indexed_txid(idx));
    }
    assert!(tracked.is_empty());
    assert_eq!(tracked.earliest_sequence(), None);
}

#[test]
fn grouped_expired_adds_with_mixed_removals_duplicates_and_restart_match_reference_floors()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_a_path = temporary.path().join("canonical-a");
    let store_b_path = temporary.path().join("canonical-b");
    let store_a = ready_store_at(&store_a_path)?;
    let store_b = ready_store_at(&store_b_path)?;
    let chain_epoch = store_a.chain_epoch()?;
    let expired_at = UnixTimestampMillis::new(1_000);
    let unexpired_at = RECENT_OBSERVATION;
    let events = mixed_lifecycle_retention_fixture(chain_epoch, expired_at, unexpired_at)?;
    for (event, observed_at) in &events {
        store_a.append_mempool_event(event.clone(), *observed_at)?;
        store_b.append_mempool_event(event.clone(), *observed_at)?;
    }

    let retention = MempoolEventRetentionConfig::new(
        Some(Duration::from_secs(10)),
        Some(Duration::from_secs(10)),
    );
    let expected_no_restart =
        simulate_reference_retention(&events, retention, RECENT_OBSERVATION, 3, None)?;
    let expected_with_restart = simulate_reference_retention(
        &events,
        retention,
        RECENT_OBSERVATION,
        3,
        Some(expected_no_restart.len() / 2),
    )?;
    let step_budget = MempoolEventRetentionStepBudget::new(
        NonZeroU32::new(3).ok_or("zero step event budget")?,
        NonZeroU64::new(1_000_000).ok_or("zero step byte budget")?,
    );

    let (floors_a, pruned_a) = run_retention_with_midway_reopen(
        store_a,
        &store_a_path,
        retention,
        step_budget,
        expected_no_restart.len() / 2,
    )?;
    assert_eq!(floors_a, expected_with_restart);

    let tight_byte_budget = MempoolEventRetentionStepBudget::new(
        NonZeroU32::new(64).ok_or("zero byte-budget event cap")?,
        NonZeroU64::new(512).ok_or("zero byte-budget byte cap")?,
    );
    let pruned_b = drain_retention_with_budget(&store_b, retention, tight_byte_budget)?;
    assert_eq!(pruned_b, pruned_a);

    let store_c_path = temporary.path().join("canonical-c");
    let store_c = ready_store_at(&store_c_path)?;
    for (event, observed_at) in &events {
        store_c.append_mempool_event(event.clone(), *observed_at)?;
    }
    let (floors_c, pruned_c) =
        run_retention_with_midway_reopen(store_c, &store_c_path, retention, step_budget, 0)?;
    assert_eq!(floors_c, expected_no_restart);
    assert_eq!(pruned_c, pruned_a);

    let retained_a = store_a_reopened_history(
        &store_a_path,
        MempoolEventHistoryRequest::with_default_limit(None),
    )?;
    let retained_b =
        store_b.mempool_event_history(MempoolEventHistoryRequest::with_default_limit(None))?;
    assert_eq!(event_sequences(&retained_a), event_sequences(&retained_b));
    assert_eq!(
        reconstruct_live_txids(&retained_a),
        reconstruct_live_txids_from_raw(&events)
    );
    Ok(())
}

#[tokio::test]
async fn open_mempool_event_stream_authenticates_cursors_and_notifies_on_head_advance()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let store = ready_store_at(&store_path)?;

    let empty_earliest =
        store.open_mempool_event_stream(&EventStreamStartPosition::EarliestRetained)?;
    assert_eq!(empty_earliest.after_cursor(), None);
    assert_eq!(empty_earliest.after_sequence(), 0);
    assert_eq!(empty_earliest.head_sequence(), 0);
    assert_eq!(
        store.mempool_event_read_counts(),
        MempoolEventReadCounts::default()
    );

    let (after_cursor, after_sequence, head_sequence, mut head_sub) = store
        .open_mempool_event_stream(&EventStreamStartPosition::LiveTail)?
        .into_parts();
    assert_eq!((after_cursor, after_sequence, head_sequence), (None, 0, 0));

    let first = append_added(&store, 0xA1, UnixTimestampMillis::new(1_000))?;
    assert_eq!(head_sub.wait_for_advance_past(0).await, Some(1));

    let chain_epoch = store.chain_epoch()?;
    let batch = store.append_mempool_events(vec![
        (
            MempoolEvent::Added {
                entry: synthetic_entry(0xA2, chain_epoch)?,
            },
            UnixTimestampMillis::new(2_000),
        ),
        (
            MempoolEvent::Invalidated {
                transaction_id: TransactionId::from_bytes([0xA2; 32]),
                reason: MempoolEvictionReason::Expired,
            },
            UnixTimestampMillis::new(3_000),
        ),
    ])?;
    assert_eq!(batch.len(), 2);
    assert_eq!(head_sub.wait_for_advance_past(1).await, Some(3));

    let counts_before_open = store.mempool_event_read_counts();
    let earliest = store.open_mempool_event_stream(&EventStreamStartPosition::EarliestRetained)?;
    assert_eq!(earliest.after_sequence(), 0);
    assert_eq!(earliest.head_sequence(), 3);
    assert_eq!(store.mempool_event_read_counts(), counts_before_open);

    let mut tampered_bytes = first.cursor.as_bytes().to_vec();
    *tampered_bytes.last_mut().ok_or("cursor missing tag")? ^= 1;
    let tampered = StreamCursorTokenV1::from_bytes(tampered_bytes);
    assert!(matches!(
        store.open_mempool_event_stream(&EventStreamStartPosition::AfterCursor(tampered)),
        Err(CanonicalStoreError::MempoolEventCursorInvalid { .. })
    ));

    drop(store);
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let reopened = open_store(&store_path, &activations, REORG_WINDOW)?;
    let mut reopened_sub = reopened
        .open_mempool_event_stream(&EventStreamStartPosition::LiveTail)?
        .into_parts()
        .3;
    assert_eq!(reopened_sub.current_head_sequence(), 3);
    let _fourth = append_mined(&reopened, 0xA1, UnixTimestampMillis::new(4_000))?;
    assert_eq!(reopened_sub.wait_for_advance_past(3).await, Some(4));

    let retention = MempoolEventRetentionConfig::new(
        Some(Duration::from_secs(1)),
        Some(Duration::from_secs(1)),
    );
    let budget = MempoolEventRetentionStepBudget::new(
        NonZeroU32::new(64).ok_or("zero retention event budget")?,
        NonZeroU64::new(1 << 20).ok_or("zero retention byte budget")?,
    );
    let _pruned = drain_retention_with_budget(&reopened, retention, budget)?;
    assert!(matches!(
        reopened.open_mempool_event_stream(&EventStreamStartPosition::AfterCursor(first.cursor)),
        Err(CanonicalStoreError::MempoolEventCursorExpired { .. })
    ));
    Ok(())
}

fn indexed_txid(index: u64) -> TransactionId {
    let mut bytes = [0_u8; 32];
    bytes[..8].copy_from_slice(&index.to_le_bytes());
    TransactionId::from_bytes(bytes)
}

fn ready_store_at(store_path: &Path) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    published_store(
        store_path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )
}

fn mixed_lifecycle_retention_fixture(
    chain_epoch: ChainEpoch,
    expired_at: UnixTimestampMillis,
    unexpired_at: UnixTimestampMillis,
) -> Result<Vec<(MempoolEvent, UnixTimestampMillis)>, Box<dyn std::error::Error>> {
    let mut events = Vec::new();
    for tag in 1_u8..=10 {
        let txid = TransactionId::from_bytes([tag; 32]);
        events.push((
            MempoolEvent::Added {
                entry: synthetic_entry(tag, chain_epoch)?,
            },
            expired_at,
        ));
        if tag == 1 {
            // Re-add tx 1 consecutively so its active sequence advances past 1.
            events.push((
                MempoolEvent::Added {
                    entry: synthetic_entry(tag, chain_epoch)?,
                },
                expired_at,
            ));
        }
        let _ = txid;
    }
    // Remove tx 2..=10 first (mixed Mined and Invalidated), keeping tx 1 active longest.
    for tag in 2_u8..=10 {
        let txid = TransactionId::from_bytes([tag; 32]);
        let event = if tag % 2 == 0 {
            MempoolEvent::Mined {
                transaction_id: txid,
                mined_height: BlockHeight::new(5),
                block_hash: BlockHash::from_bytes([0x55; 32]),
            }
        } else {
            MempoolEvent::Invalidated {
                transaction_id: txid,
                reason: MempoolEvictionReason::Conflict,
            }
        };
        events.push((event, expired_at));
    }
    // Remove tx 1, then remove-and-re-add tx 3 as an unexpired tail entry.
    events.push((
        MempoolEvent::Invalidated {
            transaction_id: TransactionId::from_bytes([1; 32]),
            reason: MempoolEvictionReason::Expired,
        },
        expired_at,
    ));
    events.push((
        MempoolEvent::Added {
            entry: synthetic_entry(3, chain_epoch)?,
        },
        unexpired_at,
    ));
    events.push((
        MempoolEvent::Invalidated {
            transaction_id: TransactionId::from_bytes([0xEE; 32]),
            reason: MempoolEvictionReason::Unknown,
        },
        unexpired_at,
    ));
    Ok(events)
}

type RetentionStepTrace = Vec<(Option<u64>, MempoolEventRetentionStepStop)>;

fn simulate_reference_retention(
    events: &[(MempoolEvent, UnixTimestampMillis)],
    retention: MempoolEventRetentionConfig,
    now: UnixTimestampMillis,
    max_events_per_step: usize,
    restart_after_step: Option<usize>,
) -> Result<RetentionStepTrace, Box<dyn std::error::Error>> {
    let head = u64::try_from(events.len())?;
    let mut floor = 1_u64;
    let mut next_seq = 1_u64;
    let mut prunable = 0_u64;
    let mut active_adds = HashMap::new();
    let mut terminal_stop = None;
    let mut steps = Vec::new();
    let mut step_index = 0_usize;
    loop {
        if restart_after_step == Some(step_index) {
            next_seq = floor;
            prunable = floor.saturating_sub(1);
            active_adds.clear();
            terminal_stop = None;
        }
        let mut examined = 0_usize;
        loop {
            let before = (examined, floor, next_seq);
            while floor <= prunable && examined < max_events_per_step {
                floor += 1;
                examined += 1;
            }
            while floor > prunable
                && terminal_stop.is_none()
                && next_seq <= head
                && examined < max_events_per_step
            {
                let (event, observed_at) = &events[usize::try_from(next_seq - 1)?];
                examined += 1;
                let window = match event {
                    MempoolEvent::Added { .. } => retention.added_retention,
                    MempoolEvent::Mined { .. } => retention.mined_retention,
                    MempoolEvent::Invalidated { .. } => retention.invalidated_retention,
                }
                .ok_or("missing finite retention window")?;
                let window_ms = u64::try_from(window.as_millis()).unwrap_or(u64::MAX);
                if now.value().saturating_sub(observed_at.value()) <= window_ms {
                    terminal_stop = Some(MempoolEventRetentionStepStop::ReachedUnexpiredEvent);
                    break;
                }
                match event {
                    MempoolEvent::Added { entry } => {
                        active_adds.insert(entry.transaction_id(), next_seq);
                    }
                    MempoolEvent::Invalidated { transaction_id, .. }
                    | MempoolEvent::Mined { transaction_id, .. } => {
                        active_adds.remove(transaction_id);
                    }
                }
                if next_seq == head {
                    terminal_stop = Some(MempoolEventRetentionStepStop::ReachedHead);
                } else {
                    next_seq += 1;
                }
                let before_head = (next_seq - usize_u64(usize::from(terminal_stop.is_none())))
                    .min(head.saturating_sub(1));
                prunable = active_adds
                    .values()
                    .copied()
                    .min()
                    .map_or(before_head, |earliest| {
                        before_head.min(earliest.saturating_sub(1))
                    });
            }
            if (examined, floor, next_seq) == before {
                break;
            }
        }
        let stop = if floor <= prunable || terminal_stop.is_none() {
            MempoolEventRetentionStepStop::BudgetExhausted
        } else {
            terminal_stop.unwrap_or(MempoolEventRetentionStepStop::ReachedHead)
        };
        steps.push((Some(floor), stop));
        if stop != MempoolEventRetentionStepStop::BudgetExhausted {
            return Ok(steps);
        }
        step_index = step_index.saturating_add(1);
    }
}

const fn usize_u64(count: usize) -> u64 {
    count as u64
}

fn run_retention_with_midway_reopen(
    mut store: RocksDbCanonicalStore,
    store_path: &Path,
    retention: MempoolEventRetentionConfig,
    budget: MempoolEventRetentionStepBudget,
    reopen_after_steps: usize,
) -> Result<(RetentionStepTrace, u64), Box<dyn std::error::Error>> {
    let mut outcomes = Vec::new();
    let mut pruned_total = 0_u64;
    let mut step_index = 0_usize;
    loop {
        if step_index == reopen_after_steps && reopen_after_steps > 0 {
            drop(store);
            let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
            store = open_store(store_path, &activations, REORG_WINDOW)?;
        }
        let outcome =
            store.advance_mempool_event_retention(RECENT_OBSERVATION, retention, budget)?;
        pruned_total = pruned_total.saturating_add(outcome.report.pruned_total());
        outcomes.push((outcome.report.oldest_retained_sequence, outcome.stop));
        if !outcome.has_immediate_work() {
            return Ok((outcomes, pruned_total));
        }
        step_index = step_index.saturating_add(1);
    }
}

fn drain_retention_with_budget(
    store: &RocksDbCanonicalStore,
    retention: MempoolEventRetentionConfig,
    budget: MempoolEventRetentionStepBudget,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut pruned_total = 0_u64;
    loop {
        let outcome =
            store.advance_mempool_event_retention(RECENT_OBSERVATION, retention, budget)?;
        pruned_total = pruned_total.saturating_add(outcome.report.pruned_total());
        if !outcome.has_immediate_work() {
            return Ok(pruned_total);
        }
    }
}

fn store_a_reopened_history(
    store_path: &Path,
    request: MempoolEventHistoryRequest<'_>,
) -> Result<Vec<MempoolEventEnvelope>, Box<dyn std::error::Error>> {
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let reopened = open_store(store_path, &activations, REORG_WINDOW)?;
    Ok(reopened.mempool_event_history(request)?)
}

fn reconstruct_live_txids(envelopes: &[MempoolEventEnvelope]) -> BTreeMap<TransactionId, u64> {
    let mut active = BTreeMap::new();
    for envelope in envelopes {
        match &envelope.event {
            MempoolEvent::Added { entry } => {
                active.insert(entry.transaction_id(), envelope.event_sequence);
            }
            MempoolEvent::Invalidated { transaction_id, .. }
            | MempoolEvent::Mined { transaction_id, .. } => {
                active.remove(transaction_id);
            }
        }
    }
    active
}

fn reconstruct_live_txids_from_raw(
    events: &[(MempoolEvent, UnixTimestampMillis)],
) -> BTreeMap<TransactionId, u64> {
    let mut active = BTreeMap::new();
    for (index, (event, _)) in events.iter().enumerate() {
        let seq = u64::try_from(index + 1).unwrap_or(u64::MAX);
        match event {
            MempoolEvent::Added { entry } => {
                active.insert(entry.transaction_id(), seq);
            }
            MempoolEvent::Invalidated { transaction_id, .. }
            | MempoolEvent::Mined { transaction_id, .. } => {
                active.remove(transaction_id);
            }
        }
    }
    active
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
