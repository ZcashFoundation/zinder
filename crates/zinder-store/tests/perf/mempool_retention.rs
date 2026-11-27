//! Performance regression gate for bounded mempool-event retention over
//! grouped unmatched `Added` runs (`#642`).

use std::{
    collections::BTreeSet,
    error::Error,
    num::{NonZeroU32, NonZeroU64},
    time::{Duration, Instant},
};

use zinder_core::{
    BlockHash, BlockHeight, ChainEpoch, CompactTransactionData, MempoolEntry,
    MempoolEvictionReason, MempoolObservation, Network, RawTransactionBytes, TransactionId,
    UnixTimestampMillis,
};
use zinder_store::{
    MempoolEvent, MempoolEventHistoryRequest, MempoolEventRetentionConfig,
    MempoolEventRetentionStepBudget, MempoolEventRetentionStepStop, RocksDbCanonicalStore,
};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture,
    network_upgrade_fixtures::sample_regtest_upgrade_activations,
};

type TestOutcome<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const SMALL_GROUP_SIZE: usize = 256;
const LARGE_GROUP_SIZE: usize = 2_048;
const APPEND_BATCH_SIZE: usize = 256;
const STEP_MAX_EVENTS: u32 = 64;
const STEP_MAX_BYTES: u64 = 256 * 1024;
const EXPIRED_OBSERVED_AT: UnixTimestampMillis = UnixTimestampMillis::new(1_000);
const RETENTION_EVALUATED_AT: UnixTimestampMillis = UnixTimestampMillis::new(100_000);

fn synthetic_txid(index: usize) -> TransactionId {
    let mut bytes = [0_u8; 32];
    bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
    bytes[24..].copy_from_slice(&(index as u64).to_be_bytes());
    TransactionId::from_bytes(bytes)
}

fn synthetic_entry(
    index: usize,
    chain_epoch: ChainEpoch,
) -> Result<MempoolEntry, Box<dyn Error + Send + Sync>> {
    Ok(MempoolEntry::new(
        synthetic_txid(index),
        None,
        RawTransactionBytes::new(vec![0xAB; 64]),
        CompactTransactionData::default(),
        MempoolObservation {
            first_seen_unix_millis: EXPIRED_OBSERVED_AT,
            first_seen_chain_epoch: chain_epoch,
        },
    )?)
}

fn build_grouped_retention_store(
    group_size: usize,
) -> Result<(CanonicalWriterFixture, TransactionId), Box<dyn Error + Send + Sync>> {
    let activations = sample_regtest_upgrade_activations();
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let writer = CanonicalWriterFixture::publish(&CanonicalBaseline::new(&chain), &activations)?;
    let store = writer.canonical_primary()?;
    let chain_epoch = store.chain_epoch()?;

    let mut events = Vec::with_capacity(group_size.saturating_mul(2).saturating_add(1));
    for idx in 1..=group_size {
        events.push((
            MempoolEvent::Added {
                entry: synthetic_entry(idx, chain_epoch)?,
            },
            EXPIRED_OBSERVED_AT,
        ));
    }
    // Remove in reverse order so the earliest active addition stays pinned until
    // the end of the removal phase.
    for idx in (1..=group_size).rev() {
        let event = if idx % 2 == 0 {
            MempoolEvent::Mined {
                transaction_id: synthetic_txid(idx),
                mined_height: BlockHeight::new(2),
                block_hash: BlockHash::from_bytes([0x22; 32]),
            }
        } else {
            MempoolEvent::Invalidated {
                transaction_id: synthetic_txid(idx),
                reason: MempoolEvictionReason::Expired,
            }
        };
        events.push((event, EXPIRED_OBSERVED_AT));
    }
    let tail_txid = synthetic_txid(group_size.saturating_add(1));
    events.push((
        MempoolEvent::Added {
            entry: synthetic_entry(group_size.saturating_add(1), chain_epoch)?,
        },
        RETENTION_EVALUATED_AT,
    ));

    for chunk in events.chunks(APPEND_BATCH_SIZE) {
        let _envelopes = store.append_mempool_events(chunk.to_vec())?;
    }
    Ok((writer, tail_txid))
}

fn run_bounded_retention_benchmark(
    store: &RocksDbCanonicalStore,
) -> Result<(Duration, u64, Option<u64>), Box<dyn Error + Send + Sync>> {
    let retention = MempoolEventRetentionConfig::new(
        Some(Duration::from_secs(10)),
        Some(Duration::from_secs(10)),
    );
    let budget = MempoolEventRetentionStepBudget::new(
        NonZeroU32::new(STEP_MAX_EVENTS).ok_or("zero event budget")?,
        NonZeroU64::new(STEP_MAX_BYTES).ok_or("zero byte budget")?,
    );
    let started = Instant::now();
    let mut pruned_total = 0_u64;
    loop {
        let outcome =
            store.advance_mempool_event_retention(RETENTION_EVALUATED_AT, retention, budget)?;
        assert!(outcome.examined_event_count <= STEP_MAX_EVENTS);
        assert!(
            outcome.examined_encoded_bytes <= STEP_MAX_BYTES || outcome.examined_event_count == 1
        );
        pruned_total = pruned_total.saturating_add(outcome.report.pruned_total());
        if !outcome.has_immediate_work() {
            assert_eq!(
                outcome.stop,
                MempoolEventRetentionStepStop::ReachedUnexpiredEvent
            );
            return Ok((
                started.elapsed(),
                pruned_total,
                outcome.report.oldest_retained_sequence,
            ));
        }
    }
}

fn live_txids_from_history(store: &RocksDbCanonicalStore) -> TestOutcome<BTreeSet<TransactionId>> {
    let retained =
        store.mempool_event_history(MempoolEventHistoryRequest::with_default_limit(None))?;
    let mut active = BTreeSet::new();
    for envelope in retained {
        match envelope.event {
            MempoolEvent::Added { entry } => {
                active.insert(entry.transaction_id());
            }
            MempoolEvent::Invalidated { transaction_id, .. }
            | MempoolEvent::Mined { transaction_id, .. } => {
                active.remove(&transaction_id);
            }
            _ => {}
        }
    }
    Ok(active)
}

#[test]
fn grouped_unmatched_additions_retention_scales_subquadratically() -> TestOutcome {
    let (small_writer, small_tail_txid) = build_grouped_retention_store(SMALL_GROUP_SIZE)?;
    let (large_writer, large_tail_txid) = build_grouped_retention_store(LARGE_GROUP_SIZE)?;

    let (small_elapsed, small_pruned, small_floor) =
        run_bounded_retention_benchmark(small_writer.canonical_primary()?)?;
    let (large_elapsed, large_pruned, large_floor) =
        run_bounded_retention_benchmark(large_writer.canonical_primary()?)?;

    assert_eq!(
        small_pruned,
        u64::try_from(SMALL_GROUP_SIZE.saturating_mul(2))?
    );
    assert_eq!(
        small_floor,
        Some(u64::try_from(
            SMALL_GROUP_SIZE.saturating_mul(2).saturating_add(1)
        )?)
    );
    assert_eq!(
        large_pruned,
        u64::try_from(LARGE_GROUP_SIZE.saturating_mul(2))?
    );
    assert_eq!(
        large_floor,
        Some(u64::try_from(
            LARGE_GROUP_SIZE.saturating_mul(2).saturating_add(1)
        )?)
    );
    assert_eq!(
        live_txids_from_history(small_writer.canonical_primary()?)?,
        BTreeSet::from([small_tail_txid])
    );
    assert_eq!(
        live_txids_from_history(large_writer.canonical_primary()?)?,
        BTreeSet::from([large_tail_txid])
    );

    let small_secs = small_elapsed.as_secs_f64().max(0.001);
    let large_secs = large_elapsed.as_secs_f64();
    let large_count = f64::from(u32::try_from(LARGE_GROUP_SIZE)?);
    let small_count = f64::from(u32::try_from(SMALL_GROUP_SIZE)?);
    let per_event_ratio = (large_secs / large_count) / (small_secs / small_count);

    #[allow(
        clippy::print_stderr,
        reason = "perf gate reports measured retention durations across group sizes"
    )]
    {
        eprintln!(
            "mempool_retention_perf small_n={} small_ms={} large_n={} large_ms={} per_event_ratio={:.2}",
            SMALL_GROUP_SIZE,
            small_elapsed.as_millis(),
            LARGE_GROUP_SIZE,
            large_elapsed.as_millis(),
            per_event_ratio,
        );
    }

    assert!(
        per_event_ratio <= 4.0,
        "mempool retention scaled worse than linear/O(log N): small_ms={}, large_ms={}, per_event_ratio={per_event_ratio:.2}",
        small_elapsed.as_millis(),
        large_elapsed.as_millis(),
    );
    Ok(())
}
