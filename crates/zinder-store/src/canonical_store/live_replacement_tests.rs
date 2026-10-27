use std::{
    env,
    num::NonZeroU32,
    path::Path,
    process::{Command, Stdio},
};

use rust_rocksdb::DB;
use tempfile::TempDir;
use zinder_core::{
    BlockHash, BlockHeaderArtifact, BlockHeight, BlockHeightRange, BlockId, CanonicalBlockFacts,
    CanonicalBlockFactsDigestVersion, CanonicalBlockReplayFormatVersion, CanonicalTransactionFacts,
    ChainEpochId, ChainTipMetadata, CompactBlockArtifact, DisplacedBlockArchiveCoverage, LockTime,
    Network, PrivacyShape, SerializedBytesDigest, TransactionBlobArtifact,
    TransactionComponentCounts, TransactionId, TransactionIntrinsicValueBalances,
    TransactionLocation, TransactionPublicFacts, TransactionVersion, UnixTimestampMillis,
    UnsupportedSection, encode_canonical_block_replay,
};

use super::{
    CanonicalBaselinePublication, CanonicalBuildBlock, CanonicalEventFence,
    CanonicalEventHistoryRequest, CanonicalEventKind, CanonicalLiveAppend,
    CanonicalLiveReplacement, CanonicalReorgPolicy, CanonicalReplacementBlock,
    CanonicalStoreBuildPlan, CanonicalStoreError, CanonicalStoreReadyEvidence,
    CanonicalStoreWorkload, ProjectionBuildLease, ProjectionBuildLeaseId, RocksDbCanonicalBuilder,
    RocksDbCanonicalSecondary, RocksDbCanonicalStore,
    displaced_archive::{
        encode_test_archive_state, encode_test_hash_pointer_rows, encode_test_order_key,
    },
    publication::encode_live_event,
    rocksdb::{BLOCK_HASH_INDEX_COLUMN_FAMILY, DISPLACED_BLOCK_FACTS_COLUMN_FAMILY},
};
use crate::{
    BlockHashLookup, ChainEpochCommitted, ChainEvent, ChainEventHistoryRequest,
    ChainEventStreamFamily, ChainRangeReverted, EventStreamStartPosition, RawBlobRetention,
    RocksDbResourceBudget, StreamCursorTokenV1,
    format::{ChainEventCursorAnchor, ChainEventLocator},
};

const REPLACEMENT_FAILPOINT_ENV: &str = "ZINDER_TEST_CANONICAL_LIVE_REPLACEMENT_FAILPOINT";
const REPLACEMENT_STORE_PATH_ENV: &str = "ZINDER_TEST_CANONICAL_LIVE_REPLACEMENT_STORE_PATH";

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one end-to-end proof checks the atomic replacement, archive, stale-row deletion, reopen, and following append boundary"
)]
fn maximum_depth_replacement_is_atomic_archived_and_reopenable_then_appends()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let mut store = published_store(&store_path, 2, BlockHeight::new(4), BlockHeight::new(2))?;
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let old_fence = store.event_fence();
    let old_three_location = transaction_location(BlockHeight::new(3), [3; 32], 103);
    let old_four_location = transaction_location(BlockHeight::new(4), [4; 32], 104);
    assert_eq!(
        store.block_hash_lookup(BlockHash::from_bytes([3; 32]))?,
        BlockHashLookup::Resolved(BlockId::new(
            BlockHeight::new(3),
            BlockHash::from_bytes([3; 32]),
        ))
    );
    assert_eq!(
        store.block_hash_lookup(BlockHash::from_bytes([0xff; 32]))?,
        BlockHashLookup::NotIndexed
    );
    assert_eq!(
        store.transaction_location(old_three_location.transaction_id)?,
        Some(old_three_location)
    );
    assert_eq!(
        store
            .transaction_blob(old_three_location)?
            .map(|blob| blob.raw_transaction_bytes),
        Some(vec![103])
    );
    let replacement_three = replacement_block(BlockHeight::new(3), [33; 32], [2; 32], 203)?;

    let (next, outcome) = store.commit_live_replacement(
        CanonicalLiveReplacement::new(
            old_fence,
            vec![CanonicalReplacementBlock::new(
                replacement_three,
                Vec::new(),
            )],
            UnixTimestampMillis::new(1_750_000_000_002),
        ),
        &activations,
    )?;
    store = next;

    assert_eq!(outcome.chain_epoch_id(), ChainEpochId::new(2));
    assert_eq!(outcome.chain_event_sequence(), 2);
    assert_eq!(
        outcome.visible_tip(),
        BlockId::new(BlockHeight::new(3), BlockHash::from_bytes([33; 32]))
    );
    assert_eq!(outcome.sequence_digest().block_count(), 3);
    assert_eq!(
        store.sequence_checkpoint().through(),
        BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([2; 32]))
    );
    assert_eq!(store.displaced_block_count()?, 2);
    let event_blocks =
        store.displaced_blocks_for_event(2, NonZeroU32::new(2).ok_or("zero limit")?)?;
    assert_eq!(
        event_blocks
            .iter()
            .map(|block| block.block_hash)
            .collect::<Vec<_>>(),
        vec![
            BlockHash::from_bytes([4; 32]),
            BlockHash::from_bytes([3; 32])
        ]
    );
    assert_eq!(
        store
            .displaced_block_by_hash(BlockHash::from_bytes([3; 32]))?
            .map(|block| block.header.height),
        Some(BlockHeight::new(3))
    );
    assert_eq!(
        store
            .block_header_at(BlockHeight::new(3))?
            .map(|header| header.block_hash),
        Some(BlockHash::from_bytes([33; 32]))
    );
    assert_eq!(
        store.block_hash_lookup(BlockHash::from_bytes([3; 32]))?,
        BlockHashLookup::NotIndexed
    );
    assert_eq!(
        store.block_hash_lookup(BlockHash::from_bytes([33; 32]))?,
        BlockHashLookup::Resolved(BlockId::new(
            BlockHeight::new(3),
            BlockHash::from_bytes([33; 32]),
        ))
    );
    assert_eq!(store.block_header_at(BlockHeight::new(4))?, None);
    assert_eq!(
        store.transaction_location(old_three_location.transaction_id)?,
        None
    );
    assert_eq!(
        store.transaction_location(old_four_location.transaction_id)?,
        None
    );
    assert_eq!(store.transaction_blob(old_three_location)?, None);
    assert_eq!(store.transaction_blob(old_four_location)?, None);
    let old_hash_family = store
        .bounded_open
        .db
        .cf_handle(BLOCK_HASH_INDEX_COLUMN_FAMILY)
        .ok_or("block hash family absent")?;
    assert_eq!(
        store.bounded_open.db.get_cf(&old_hash_family, [3; 32])?,
        None
    );
    assert_eq!(
        store.bounded_open.db.get_cf(&old_hash_family, [4; 32])?,
        None
    );
    store
        .bounded_open
        .db
        .put_cf(&old_hash_family, [3; 32], 3_u32.to_be_bytes())?;
    assert_eq!(
        store.block_hash_lookup(BlockHash::from_bytes([3; 32]))?,
        BlockHashLookup::NotInBestChain
    );
    let event_family = store
        .bounded_open
        .db
        .cf_handle(super::rocksdb::CHAIN_EVENT_COLUMN_FAMILY)
        .ok_or("event family absent")?;
    assert_eq!(
        store
            .bounded_open
            .db
            .get_cf(&event_family, 2_u64.to_be_bytes())?
            .as_deref(),
        Some(
            encode_live_event(
                ChainEpochId::new(2),
                ChainEpochId::new(1),
                Some(BlockHeightRange::inclusive(
                    BlockHeight::new(3),
                    BlockHeight::new(4)
                )),
                BlockHeightRange::inclusive(BlockHeight::new(3), BlockHeight::new(3)),
                CanonicalEventFence::from_persisted_event(
                    ChainEpochId::new(2),
                    2,
                    BlockId::new(BlockHeight::new(3), BlockHash::from_bytes([33; 32])),
                    outcome.sequence_digest().block_count(),
                    outcome.sequence_digest().as_bytes(),
                ),
            )
            .as_slice()
        ),
    );
    drop(event_family);
    drop(old_hash_family);
    drop(store);

    let reopened = open_store(&store_path, &activations, 2)?;
    assert_eq!(reopened.event_fence(), outcome);
    assert_eq!(reopened.displaced_block_count()?, 2);
    let append_fence = reopened.event_fence();
    let block_four = replacement_block(BlockHeight::new(4), [44; 32], [33; 32], 204)?;
    let (store, append_outcome) = reopened.commit_live_append(
        CanonicalLiveAppend::new(
            append_fence,
            block_four,
            Vec::new(),
            BlockId::new(BlockHeight::new(3), BlockHash::from_bytes([33; 32])),
            UnixTimestampMillis::new(1_750_000_000_003),
        ),
        &activations,
    )?;
    assert_eq!(append_outcome.chain_epoch_id(), ChainEpochId::new(3));
    assert_eq!(
        append_outcome.visible_tip(),
        BlockId::new(BlockHeight::new(4), BlockHash::from_bytes([44; 32]))
    );
    assert_eq!(store.displaced_block_count()?, 2);
    Ok(())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one end-to-end proof keeps append, replacement, secondary catch-up, and cold reopen together"
)]
fn all_retention_keeps_block_blobs_across_append_replacement_secondary_and_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let mut store = published_store_with_retention(
        &store_path,
        2,
        BlockHeight::new(2),
        BlockHeight::new(1),
        RawBlobRetention::All,
    )?;
    let initial_transaction_location = transaction_location(BlockHeight::new(1), [1; 32], 101);
    assert_eq!(
        store.transaction_location(initial_transaction_location.transaction_id)?,
        Some(initial_transaction_location)
    );
    assert_eq!(
        store
            .transaction_blob(initial_transaction_location)?
            .map(|blob| blob.raw_transaction_bytes),
        Some(vec![101])
    );
    let settled_tip = store.append_anchor()?.settled_tip();
    let append = replacement_block_with_retention(
        BlockHeight::new(3),
        [3; 32],
        [2; 32],
        203,
        RawBlobRetention::All,
    )?;
    let append_fence = store.event_fence();
    let (next, _) = store.commit_live_append(
        CanonicalLiveAppend::new(
            append_fence,
            append,
            Vec::new(),
            settled_tip,
            UnixTimestampMillis::new(1_750_000_000_002),
        ),
        &activations,
    )?;
    store = next;
    assert_eq!(
        store
            .block_blob_at(BlockHeight::new(3))?
            .ok_or("appended block blob must be retained")?
            .raw_block_bytes,
        vec![203]
    );

    let mut secondary = RocksDbCanonicalSecondary::open_ready(
        &store_path,
        temporary.path().join("secondary"),
        &activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::All,
        CanonicalReorgPolicy::new(2)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let replacement = replacement_block_with_retention(
        BlockHeight::new(3),
        [33; 32],
        [2; 32],
        233,
        RawBlobRetention::All,
    )?;
    let replacement_fence = store.event_fence();
    let (next, _) = store.commit_live_replacement(
        CanonicalLiveReplacement::new(
            replacement_fence,
            vec![CanonicalReplacementBlock::new(replacement, Vec::new())],
            UnixTimestampMillis::new(1_750_000_000_003),
        ),
        &activations,
    )?;
    store = next;
    assert_eq!(
        store
            .block_blobs_in_range(BlockHeightRange::inclusive(
                BlockHeight::new(1),
                BlockHeight::new(3),
            ))?
            .into_iter()
            .map(|blob| blob.map(|blob| blob.raw_block_bytes))
            .collect::<Vec<_>>(),
        vec![Some(vec![101]), Some(vec![102]), Some(vec![233])]
    );
    secondary.try_catch_up()?;
    assert_eq!(
        secondary
            .block_blob_at(BlockHeight::new(3))?
            .ok_or("secondary replacement block blob must be retained")?
            .raw_block_bytes,
        vec![233]
    );
    drop(secondary);
    drop(store);

    let reopened = open_store_with_retention(&store_path, &activations, 2, RawBlobRetention::All)?;
    assert_eq!(reopened.raw_blob_retention(), RawBlobRetention::All);
    assert_eq!(
        reopened
            .block_blob_at(BlockHeight::new(3))?
            .ok_or("reopened replacement block blob must be retained")?
            .raw_block_bytes,
        vec![233]
    );
    Ok(())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one end-to-end proof keeps no-blob construction, reopen, append, replacement, stale deletion, and secondary catch-up together"
)]
fn no_blob_retention_preserves_transaction_locations_across_the_complete_store_lifecycle()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let mut store = published_store_with_retention(
        &store_path,
        2,
        BlockHeight::new(2),
        BlockHeight::new(1),
        RawBlobRetention::None,
    )?;
    let initial_location = transaction_location(BlockHeight::new(1), [1; 32], 101);
    assert_eq!(
        store.transaction_location(initial_location.transaction_id)?,
        Some(initial_location)
    );
    assert_eq!(store.transaction_blob(initial_location)?, None);
    assert_eq!(store.block_blob_at(BlockHeight::new(1))?, None);
    drop(store);

    store = open_store_with_retention(&store_path, &activations, 2, RawBlobRetention::None)?;
    assert_eq!(
        store.transaction_location(initial_location.transaction_id)?,
        Some(initial_location)
    );
    assert_eq!(store.transaction_blob(initial_location)?, None);

    let secondary_path = temporary.path().join("secondary");
    let mut secondary = RocksDbCanonicalSecondary::open_ready(
        &store_path,
        &secondary_path,
        &activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::None,
        CanonicalReorgPolicy::new(2)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    assert_eq!(
        secondary.transaction_location(initial_location.transaction_id)?,
        Some(initial_location)
    );
    assert_eq!(secondary.transaction_blob(initial_location)?, None);

    let appended_location = transaction_location(BlockHeight::new(3), [3; 32], 203);
    let append = replacement_block_with_retention(
        BlockHeight::new(3),
        [3; 32],
        [2; 32],
        203,
        RawBlobRetention::None,
    )?;
    let append_fence = store.event_fence();
    let settled_tip = store.append_anchor()?.settled_tip();
    let (next, _) = store.commit_live_append(
        CanonicalLiveAppend::new(
            append_fence,
            append,
            Vec::new(),
            settled_tip,
            UnixTimestampMillis::new(1_750_000_000_002),
        ),
        &activations,
    )?;
    store = next;
    assert_eq!(
        store.transaction_location(appended_location.transaction_id)?,
        Some(appended_location)
    );
    assert_eq!(store.transaction_blob(appended_location)?, None);
    secondary.try_catch_up()?;
    assert_eq!(
        secondary.transaction_location(appended_location.transaction_id)?,
        Some(appended_location)
    );
    assert_eq!(secondary.transaction_blob(appended_location)?, None);

    let replacement_location = transaction_location(BlockHeight::new(3), [33; 32], 233);
    let replacement = replacement_block_with_retention(
        BlockHeight::new(3),
        [33; 32],
        [2; 32],
        233,
        RawBlobRetention::None,
    )?;
    let replacement_fence = store.event_fence();
    let (next, _) = store.commit_live_replacement(
        CanonicalLiveReplacement::new(
            replacement_fence,
            vec![CanonicalReplacementBlock::new(replacement, Vec::new())],
            UnixTimestampMillis::new(1_750_000_000_003),
        ),
        &activations,
    )?;
    store = next;
    assert_eq!(
        store.transaction_location(appended_location.transaction_id)?,
        None
    );
    assert_eq!(
        store.transaction_location(replacement_location.transaction_id)?,
        Some(replacement_location)
    );
    assert_eq!(store.transaction_blob(replacement_location)?, None);
    secondary.try_catch_up()?;
    assert_eq!(
        secondary.transaction_location(appended_location.transaction_id)?,
        None
    );
    assert_eq!(
        secondary.transaction_location(replacement_location.transaction_id)?,
        Some(replacement_location)
    );
    assert_eq!(secondary.transaction_blob(replacement_location)?, None);
    drop(secondary);
    drop(store);

    let reopened = open_store_with_retention(&store_path, &activations, 2, RawBlobRetention::None)?;
    assert_eq!(
        reopened.transaction_location(appended_location.transaction_id)?,
        None
    );
    assert_eq!(
        reopened.transaction_location(replacement_location.transaction_id)?,
        Some(replacement_location)
    );
    assert_eq!(reopened.transaction_blob(replacement_location)?, None);
    Ok(())
}

#[test]
fn repeatedly_displaced_block_hash_reopens_and_resolves_its_newest_occurrence()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let mut store = published_store(&store_path, 2, BlockHeight::new(4), BlockHeight::new(2))?;

    for (block_hash, transaction_tag, displaced_at) in [
        ([33; 32], 203, 1_750_000_000_002),
        ([3; 32], 103, 1_750_000_000_003),
        ([44; 32], 204, 1_750_000_000_004),
    ] {
        let expected_fence = store.event_fence();
        let replacement =
            replacement_block(BlockHeight::new(3), block_hash, [2; 32], transaction_tag)?;
        let (next, _) = store.commit_live_replacement(
            CanonicalLiveReplacement::new(
                expected_fence,
                vec![CanonicalReplacementBlock::new(replacement, Vec::new())],
                UnixTimestampMillis::new(displaced_at),
            ),
            &activations,
        )?;
        store = next;
    }

    assert_eq!(store.displaced_block_count()?, 4);
    assert_eq!(
        store
            .displaced_block_by_hash(BlockHash::from_bytes([3; 32]))?
            .ok_or("repeatedly displaced block is absent")?
            .displacement_event_sequence,
        4
    );
    assert_eq!(
        store
            .displaced_blocks_for_event(2, NonZeroU32::new(2).ok_or("zero limit")?)?
            .last()
            .ok_or("original displacement is absent")?
            .block_hash,
        BlockHash::from_bytes([3; 32])
    );

    drop(store);
    let reopened = open_store(&store_path, &activations, 2)?;
    assert_eq!(
        reopened
            .displaced_block_by_hash(BlockHash::from_bytes([3; 32]))?
            .ok_or("repeatedly displaced block is absent after reopen")?
            .displacement_event_sequence,
        4
    );
    Ok(())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one public-boundary lifecycle proof keeps cursor, reorg, lease, pruning, reopen, and corrupt-record assertions together"
)]
fn retained_events_resume_exactly_and_leases_bound_pruning()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let mut store = published_store(&store_path, 2, BlockHeight::new(4), BlockHeight::new(2))?;
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;

    let replacement_three = replacement_block(BlockHeight::new(3), [33; 32], [2; 32], 203)?;
    let replacement_fence = store.event_fence();
    let (next, _) = store.commit_live_replacement(
        CanonicalLiveReplacement::new(
            replacement_fence,
            vec![CanonicalReplacementBlock::new(
                replacement_three,
                Vec::new(),
            )],
            UnixTimestampMillis::new(1_750_000_000_002),
        ),
        &activations,
    )?;
    store = next;
    let appended_four = replacement_block(BlockHeight::new(4), [44; 32], [33; 32], 204)?;
    let append_fence = store.event_fence();
    let (next, _) = store.commit_live_append(
        CanonicalLiveAppend::new(
            append_fence,
            appended_four,
            Vec::new(),
            BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([2; 32])),
            UnixTimestampMillis::new(1_750_000_000_003),
        ),
        &activations,
    )?;
    store = next;

    let page_limit = NonZeroU32::new(10).ok_or("zero page limit")?;
    let page =
        store.canonical_event_history(CanonicalEventHistoryRequest::new(None, page_limit))?;
    assert_eq!(page.len(), 3);
    assert_eq!(page[0].kind(), CanonicalEventKind::Committed);
    assert_eq!(page[1].kind(), CanonicalEventKind::Reorged);
    assert_eq!(page[2].kind(), CanonicalEventKind::Committed);
    let first_cursor = page[0].cursor().as_bytes();
    let resumed = store.canonical_event_history(CanonicalEventHistoryRequest::new(
        Some(&first_cursor),
        page_limit,
    ))?;
    assert_eq!(
        resumed
            .iter()
            .map(|event| event.cursor().event_sequence())
            .collect::<Vec<_>>(),
        vec![2, 3]
    );

    let mut unknown_cursor = first_cursor;
    unknown_cursor[0] = 2;
    assert!(matches!(
        store.canonical_event_history(CanonicalEventHistoryRequest::new(
            Some(&unknown_cursor),
            page_limit,
        )),
        Err(CanonicalStoreError::CanonicalEventCursorUnknownVersion { version: 2 })
    ));

    let lease = ProjectionBuildLease::new(
        ProjectionBuildLeaseId::from_bytes([7; 16]),
        page[1].projection_build_anchor(),
        UnixTimestampMillis::new(20),
    );
    let acquired_lease =
        store.acquire_projection_build_lease(lease, UnixTimestampMillis::new(10))?;
    assert_eq!(acquired_lease.generation(), 1);
    assert!(matches!(
        store.acquire_projection_build_lease(lease, UnixTimestampMillis::new(10)),
        Err(CanonicalStoreError::ProjectionBuildLeaseInvalid { .. })
    ));
    let beyond_maximum_ttl = ProjectionBuildLease::new(
        ProjectionBuildLeaseId::from_bytes([9; 16]),
        page[1].projection_build_anchor(),
        UnixTimestampMillis::new(
            10_u64
                .saturating_add(super::event_lifecycle::MAX_PROJECTION_BUILD_LEASE_DURATION_MILLIS)
                .saturating_add(1),
        ),
    );
    assert!(matches!(
        store.acquire_projection_build_lease(beyond_maximum_ttl, UnixTimestampMillis::new(10)),
        Err(CanonicalStoreError::ProjectionBuildLeaseInvalid { .. })
    ));
    let protected = store.prune_canonical_events_before(3, UnixTimestampMillis::new(10))?;
    assert_eq!(protected.oldest_retained_sequence, 2);
    assert_eq!(protected.pruned_event_count, 0);
    assert_eq!(
        protected
            .lease_protected_anchor
            .map(|anchor| anchor.event_cursor().event_sequence()),
        Some(2)
    );
    let lease_live_across_reopen_request = ProjectionBuildLease::new(
        ProjectionBuildLeaseId::from_bytes([8; 16]),
        page[1].projection_build_anchor(),
        UnixTimestampMillis::new(UnixTimestampMillis::now().value().saturating_add(60_000)),
    );
    let lease_live_across_reopen = store.acquire_projection_build_lease(
        lease_live_across_reopen_request,
        UnixTimestampMillis::now(),
    )?;

    drop(store);
    let store = open_store(&store_path, &activations, 2)?;
    assert_eq!(
        store.active_projection_build_lease(
            lease_live_across_reopen.id(),
            UnixTimestampMillis::now(),
        )?,
        Some(lease_live_across_reopen)
    );
    assert_eq!(store.canonical_event_retention_floor()?, 2);
    assert_eq!(
        store
            .canonical_event_history(CanonicalEventHistoryRequest::new(None, page_limit))?
            .iter()
            .map(|event| event.cursor().event_sequence())
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    store.release_projection_build_lease(lease_live_across_reopen)?;

    let expired = store.prune_canonical_events_before(3, UnixTimestampMillis::new(20))?;
    assert_eq!(expired.oldest_retained_sequence, 3);
    assert_eq!(expired.pruned_event_count, 1);
    assert!(matches!(
        store.canonical_event_history(CanonicalEventHistoryRequest::new(
            Some(&first_cursor),
            page_limit,
        )),
        Err(CanonicalStoreError::CanonicalEventCursorExpired {
            event_sequence: 1,
            oldest_retained_sequence: 3,
        })
    ));

    drop(store);
    let store = open_store(&store_path, &activations, 2)?;
    assert_eq!(store.canonical_event_retention_floor()?, 3);

    let takeover_request = ProjectionBuildLease::new(
        acquired_lease.id(),
        page[2].projection_build_anchor(),
        UnixTimestampMillis::new(30),
    );
    let takeover =
        store.acquire_projection_build_lease(takeover_request, UnixTimestampMillis::new(20))?;
    assert_eq!(takeover.generation(), 3);
    let stale_renewal = ProjectionBuildLease::new(
        acquired_lease.id(),
        acquired_lease.anchor(),
        UnixTimestampMillis::new(30),
    )
    .with_generation(acquired_lease.generation());
    assert!(matches!(
        store.renew_projection_build_lease(stale_renewal, UnixTimestampMillis::new(20)),
        Err(CanonicalStoreError::ProjectionBuildLeaseInvalid { .. })
    ));
    assert!(matches!(
        store.release_projection_build_lease(acquired_lease),
        Err(CanonicalStoreError::ProjectionBuildLeaseInvalid { .. })
    ));
    assert_eq!(
        store.active_projection_build_lease(takeover.id(), UnixTimestampMillis::new(20))?,
        Some(takeover)
    );
    store.release_projection_build_lease(takeover)?;
    store.release_projection_build_lease(takeover)?;

    let event_family = store
        .bounded_open
        .db
        .cf_handle(super::rocksdb::CHAIN_EVENT_COLUMN_FAMILY)
        .ok_or("event family absent")?;
    let encoded_event = store
        .bounded_open
        .db
        .get_cf(&event_family, 3_u64.to_be_bytes())?
        .ok_or("event three absent")?;
    let original_event = encoded_event.clone();
    let mut unknown_event_version = encoded_event.clone();
    unknown_event_version[0] = 2;
    store
        .bounded_open
        .db
        .put_cf(&event_family, 3_u64.to_be_bytes(), unknown_event_version)?;
    assert!(matches!(
        store.canonical_event_history(CanonicalEventHistoryRequest::new(None, page_limit)),
        Err(CanonicalStoreError::CanonicalEventVersionUnsupported {
            event_sequence: 3,
            version: 2,
        })
    ));
    let mut malformed_range = encoded_event;
    malformed_range[27..31].copy_from_slice(&5_u32.to_le_bytes());
    malformed_range[31..35].copy_from_slice(&3_u32.to_le_bytes());
    store
        .bounded_open
        .db
        .put_cf(&event_family, 3_u64.to_be_bytes(), malformed_range)?;
    assert!(matches!(
        store.canonical_event_history(CanonicalEventHistoryRequest::new(None, page_limit)),
        Err(CanonicalStoreError::CanonicalEventRecordMalformed {
            event_sequence: 3,
            ..
        })
    ));
    store
        .bounded_open
        .db
        .put_cf(&event_family, 3_u64.to_be_bytes(), original_event)?;
    let mut malformed_lease_key = super::event_lifecycle::PROJECTION_BUILD_LEASE_PREFIX.to_vec();
    malformed_lease_key.extend_from_slice(&[9; 16]);
    store.bounded_open.db.put(malformed_lease_key, [2])?;
    drop(event_family);
    drop(store);
    let error = RocksDbCanonicalStore::open_ready(
        &store_path,
        &activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(2)?,
        RocksDbResourceBudget::for_local_tests(),
    )
    .err()
    .ok_or("malformed lifecycle lease must reject cold open")?;
    assert!(
        matches!(
            error,
            CanonicalStoreError::ProjectionBuildLeaseInvalid { .. }
        ),
        "unexpected cold admission error: {error:?}"
    );
    Ok(())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one secondary-boundary lifecycle proves authenticated cursors, permanent historical locators, real and synthetic reorg delivery, and expiry together"
)]
fn secondary_wallet_events_reconstruct_historical_branches_across_multiple_reorgs()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let secondary_path = temporary.path().join("secondary");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let mut store = published_store(&store_path, 32, BlockHeight::new(16), BlockHeight::new(2))?;
    let mut secondary = RocksDbCanonicalSecondary::open_ready(
        &store_path,
        &secondary_path,
        &activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(32)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let page_limit = NonZeroU32::new(16).ok_or("wallet event page limit is zero")?;
    let baseline = secondary
        .wallet_chain_event_history(ChainEventHistoryRequest {
            from_cursor: None,
            max_events: page_limit,
            family: ChainEventStreamFamily::Visible,
        })?
        .into_iter()
        .next()
        .ok_or("secondary did not project the baseline event")?;
    assert_eq!(baseline.event_sequence, 1);

    let mut tampered = baseline.cursor.as_bytes().to_vec();
    let tampered_byte = tampered
        .last_mut()
        .ok_or("baseline cursor must carry authentication bytes")?;
    *tampered_byte ^= 0xff;
    let tampered_cursor = StreamCursorTokenV1::from_bytes(tampered);
    assert!(matches!(
        secondary.wallet_chain_event_history(ChainEventHistoryRequest {
            from_cursor: Some(&tampered_cursor),
            max_events: page_limit,
            family: ChainEventStreamFamily::Visible,
        }),
        Err(CanonicalStoreError::CanonicalEventCursorMalformed { .. })
    ));

    let baseline_payload = baseline
        .cursor
        .decode_chain_event(Network::ZcashTestnet, secondary.cursor_auth_key)?;
    let wrong_network_cursor = StreamCursorTokenV1::chain_event(
        Network::ZcashMainnet,
        baseline_payload.family,
        baseline_payload.event_sequence,
        &baseline_payload.locator,
        secondary.cursor_auth_key,
    )?;
    assert!(matches!(
        secondary.wallet_chain_event_history(ChainEventHistoryRequest {
            from_cursor: Some(&wrong_network_cursor),
            max_events: page_limit,
            family: ChainEventStreamFamily::Visible,
        }),
        Err(CanonicalStoreError::CanonicalEventCursorMalformed { .. })
    ));
    assert!(matches!(
        secondary.resolve_wallet_chain_event_stream_start(
            &EventStreamStartPosition::AfterCursor(baseline.cursor.clone()),
            ChainEventStreamFamily::Settled,
        ),
        Err(CanonicalStoreError::CanonicalEventCursorMalformed { .. })
    ));

    let first_replacement = replacement_suffix(10, 16, [9; 32], 100)?;
    let first_fence = store.event_fence();
    let (next, _) = store.commit_live_replacement(
        CanonicalLiveReplacement::new(
            first_fence,
            first_replacement,
            UnixTimestampMillis::new(1_750_000_000_002),
        ),
        &activations,
    )?;
    store = next;
    secondary.try_catch_up()?;
    let retained_reorg = secondary
        .wallet_chain_event_history(ChainEventHistoryRequest {
            from_cursor: Some(&baseline.cursor),
            max_events: page_limit,
            family: ChainEventStreamFamily::Visible,
        })?
        .into_iter()
        .next()
        .ok_or("secondary did not project the retained real reorg")?;
    let ChainEvent::ChainReorged { reverted, .. } = retained_reorg.event else {
        return Err("secondary projected a non-reorg event for the replacement".into());
    };
    assert_eq!(
        reverted.block_range,
        BlockHeightRange::inclusive(BlockHeight::new(10), BlockHeight::new(16))
    );

    let second_replacement = replacement_suffix(13, 16, [112; 32], 200)?;
    let second_fence = store.event_fence();
    let (next, _) = store.commit_live_replacement(
        CanonicalLiveReplacement::new(
            second_fence,
            second_replacement,
            UnixTimestampMillis::new(1_750_000_000_003),
        ),
        &activations,
    )?;
    store = next;
    let append_fence = store.event_fence();
    let (next, _) = store.commit_live_append(
        CanonicalLiveAppend::new(
            append_fence,
            replacement_block(BlockHeight::new(17), [217; 32], [216; 32], 217)?,
            Vec::new(),
            BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([2; 32])),
            UnixTimestampMillis::new(1_750_000_000_004),
        ),
        &activations,
    )?;
    store = next;
    secondary.try_catch_up()?;

    let all_events = secondary.wallet_chain_event_history(ChainEventHistoryRequest {
        from_cursor: None,
        max_events: page_limit,
        family: ChainEventStreamFamily::Visible,
    })?;
    let historical_baseline = all_events
        .first()
        .ok_or("secondary history lost its retained baseline")?;
    let historical_baseline_payload = historical_baseline
        .cursor
        .decode_chain_event(Network::ZcashTestnet, secondary.cursor_auth_key)?;
    assert_eq!(
        historical_baseline_payload
            .locator
            .entries()
            .iter()
            .find(|entry| entry.height == BlockHeight::new(13))
            .map(|entry| entry.hash),
        Some(BlockHash::from_bytes([13; 32])),
        "the earliest later displacement must reconstruct baseline block 13"
    );
    let historical_first_reorg = all_events
        .get(1)
        .ok_or("secondary history lost the first reorg")?;
    let historical_first_reorg_payload = historical_first_reorg
        .cursor
        .decode_chain_event(Network::ZcashTestnet, secondary.cursor_auth_key)?;
    assert_eq!(
        historical_first_reorg_payload
            .locator
            .entries()
            .iter()
            .find(|entry| entry.height == BlockHeight::new(13))
            .map(|entry| entry.hash),
        Some(BlockHash::from_bytes([113; 32])),
        "the next displacement must reconstruct the first replacement at block 13"
    );

    store.prune_canonical_events_before(4, UnixTimestampMillis::new(1_750_000_000_005))?;
    secondary.try_catch_up()?;
    let resumed = secondary.wallet_chain_event_history(ChainEventHistoryRequest {
        from_cursor: Some(&historical_baseline.cursor),
        max_events: page_limit,
        family: ChainEventStreamFamily::Visible,
    })?;
    let synthetic = resumed
        .first()
        .ok_or("pruned reconnect did not synthesize a reorg")?;
    let ChainEvent::ChainReorged {
        reverted,
        committed,
    } = synthetic.event
    else {
        return Err("pruned reconnect did not lead with a synthetic reorg".into());
    };
    assert_eq!(
        reverted.block_range,
        BlockHeightRange::inclusive(BlockHeight::new(10), BlockHeight::new(16))
    );
    assert_eq!(reverted.chain_epoch, historical_baseline.chain_epoch);
    assert_eq!(reverted.chain_epoch.id, ChainEpochId::new(1));
    assert_eq!(
        BlockId::new(
            reverted.chain_epoch.visible_tip_height,
            reverted.chain_epoch.visible_tip_hash,
        ),
        BlockId::new(BlockHeight::new(16), BlockHash::from_bytes([16; 32]))
    );
    assert_eq!(
        reverted.chain_epoch.settled_tip_height,
        committed.chain_epoch.settled_tip_height
    );
    assert_eq!(
        committed.block_range,
        BlockHeightRange::inclusive(BlockHeight::new(10), BlockHeight::new(17))
    );

    let unresolvable_locator = ChainEventLocator::new(vec![ChainEventCursorAnchor {
        height: BlockHeight::new(16),
        hash: BlockHash::from_bytes([0xee; 32]),
    }])?;
    let deep_cursor = StreamCursorTokenV1::chain_event(
        Network::ZcashTestnet,
        ChainEventStreamFamily::Visible,
        1,
        &unresolvable_locator,
        secondary.cursor_auth_key,
    )?;
    assert!(matches!(
        secondary.wallet_chain_event_history(ChainEventHistoryRequest {
            from_cursor: Some(&deep_cursor),
            max_events: page_limit,
            family: ChainEventStreamFamily::Visible,
        }),
        Err(CanonicalStoreError::CanonicalEventCursorExpired {
            event_sequence: 1,
            oldest_retained_sequence: 4,
        })
    ));
    Ok(())
}

#[test]
fn projection_build_lease_capacity_is_bounded() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = published_store(
        &temporary.path().join("canonical"),
        1,
        BlockHeight::new(1),
        BlockHeight::new(1),
    )?;
    let page =
        store.canonical_event_history(CanonicalEventHistoryRequest::new(None, NonZeroU32::MIN))?;
    let anchor = page
        .first()
        .ok_or("published baseline did not produce a canonical event")?
        .projection_build_anchor();
    let now = UnixTimestampMillis::new(10);
    let expires_at = UnixTimestampMillis::new(20);

    let leases = (0..super::event_lifecycle::MAX_LIVE_PROJECTION_BUILD_LEASES)
        .map(|identifier| {
            Ok(ProjectionBuildLease::new(
                ProjectionBuildLeaseId::from_bytes(u128::try_from(identifier)?.to_be_bytes()),
                anchor,
                expires_at,
            )
            .with_generation(u64::try_from(identifier + 1)?))
        })
        .collect::<Result<Vec<_>, std::num::TryFromIntError>>()?;
    super::event_lifecycle::seed_projection_build_leases_for_capacity_test(&store, &leases)?;
    let overflow = ProjectionBuildLease::new(
        ProjectionBuildLeaseId::from_bytes(
            u128::try_from(super::event_lifecycle::MAX_LIVE_PROJECTION_BUILD_LEASES)?.to_be_bytes(),
        ),
        anchor,
        expires_at,
    );
    let error = store
        .acquire_projection_build_lease(overflow, now)
        .err()
        .ok_or("lease above cardinality bound was accepted")?;
    assert!(
        matches!(
            error,
            CanonicalStoreError::ProjectionBuildLeaseInvalid {
                reason: "live projection build lease capacity is exhausted",
            }
        ),
        "unexpected cardinality rejection: {error:?}"
    );
    Ok(())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one proof binds the in-place raise, restart admission, refused lowering, and a replacement deeper than the construction window"
)]
fn writer_raises_the_reorg_window_in_place_across_restart_and_never_lowers_it()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let construction = CanonicalReorgPolicy::new(100)?;
    let raised = CanonicalReorgPolicy::new(1_000)?;
    let settled_tip = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([2; 32]));
    let store = published_store(&store_path, 100, BlockHeight::new(102), settled_tip.height)?;
    let construction_fence = store.event_fence();
    drop(store);
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;

    let store = open_writer(&store_path, &activations, 1_000)?;
    assert_eq!(store.reorg_policy(), raised);
    assert_eq!(store.build_plan().reorg_policy(), construction);
    assert_eq!(store.event_fence(), construction_fence);
    // The settled tip holds, so the visible tip may now trail it by more
    // blocks than the construction window allowed.
    let (store, appended) = store.commit_live_append(
        CanonicalLiveAppend::new(
            construction_fence,
            replacement_block(BlockHeight::new(103), [103; 32], [102; 32], 203)?,
            Vec::new(),
            settled_tip,
            UnixTimestampMillis::new(1_750_000_000_002),
        ),
        &activations,
    )?;
    assert_eq!(store.chain_epoch()?.settled_tip_height, settled_tip.height);
    drop(store);

    let refusals = [
        (
            open_store(&store_path, &activations, 100).err(),
            "does not equal requested reorg window",
        ),
        (
            open_writer(&store_path, &activations, 100).err(),
            "never lowered in place",
        ),
        (
            open_writer(&store_path, &activations, 999).err(),
            "never lowered in place",
        ),
        (
            RocksDbCanonicalSecondary::open_ready(
                &store_path,
                temporary.path().join("stale-secondary"),
                &activations,
                CanonicalStoreWorkload::Wallet,
                RawBlobRetention::Transactions,
                construction,
                RocksDbResourceBudget::for_local_tests(),
            )
            .err()
            .map(Into::into),
            "does not equal requested reorg window",
        ),
    ];
    for (error, reason) in refusals {
        let error = error.ok_or("an admission below the raised window must fail")?;
        assert!(error.to_string().contains(reason), "{error}");
    }
    let secondary = RocksDbCanonicalSecondary::open_ready(
        &store_path,
        temporary.path().join("secondary"),
        &activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        raised,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    assert_eq!(secondary.reorg_policy(), raised);
    assert_eq!(secondary.event_fence(), appended);
    drop(secondary);

    let store = open_writer(&store_path, &activations, 1_000)?;
    assert_eq!(store.reorg_policy(), raised);
    assert_eq!(store.event_fence(), appended);
    // Replace 101 blocks above the held settled tip: one deeper than the
    // construction window.
    let mut parent_hash = settled_tip.hash.as_bytes();
    let mut replacement = Vec::new();
    for height in 3..=103_u32 {
        let transaction_tag = u8::try_from(height - 3)?;
        let mut block_hash = [transaction_tag; 32];
        block_hash[0] = 0xee;
        replacement.push(CanonicalReplacementBlock::new(
            replacement_block(
                BlockHeight::new(height),
                block_hash,
                parent_hash,
                transaction_tag,
            )?,
            Vec::new(),
        ));
        parent_hash = block_hash;
    }
    let (store, replaced) = store.commit_live_replacement(
        CanonicalLiveReplacement::new(
            appended,
            replacement,
            UnixTimestampMillis::new(1_750_000_000_003),
        ),
        &activations,
    )?;
    assert_eq!(store.displaced_block_count()?, 101);
    drop(store);
    let reopened = open_store(&store_path, &activations, 1_000)?;
    assert_eq!(reopened.event_fence(), replaced);
    assert_eq!(reopened.reorg_policy(), raised);
    Ok(())
}

#[test]
fn secondary_refuses_a_writer_raise_until_it_reopens_with_the_raised_window()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    drop(published_store(
        &store_path,
        2,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )?);
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let open_secondary = |name: &str, window: u32| {
        RocksDbCanonicalSecondary::open_ready(
            &store_path,
            temporary.path().join(name),
            &activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(window)?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .map_err(Box::<dyn std::error::Error>::from)
    };
    let mut stale = open_secondary("stale", 2)?;
    let writer = open_writer(&store_path, &activations, 3)?;
    assert_eq!(writer.reorg_policy(), CanonicalReorgPolicy::new(3)?);
    let error = stale
        .try_catch_up()
        .err()
        .ok_or("catch-up across a writer raise must fail")?;
    assert!(
        error
            .to_string()
            .contains("persisted reorg window 3 does not equal requested reorg window 2"),
        "{error}"
    );
    let current = open_secondary("current", 3)?;
    assert_eq!(current.reorg_policy(), writer.reorg_policy());
    Ok(())
}

#[test]
fn incremental_replay_tails_match_a_complete_derivation_across_appends_and_replacements()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let window = 3;
    let mut store = published_store(
        &store_path,
        window,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )?;
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let mut secondary = RocksDbCanonicalSecondary::open_ready(
        &store_path,
        temporary.path().join("secondary"),
        &activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(window)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let complete = |db: &DB, ready: CanonicalStoreReadyEvidence| {
        super::replay_tail::AuthenticatedReplayTail::authenticate(
            db,
            ready.sequence_checkpoint,
            ready.visible_tip.height,
            window,
        )
        .map(|tail| tail.checkpoints())
    };

    let idle = secondary.try_catch_up()?;
    assert_eq!(idle.before(), idle.after());

    let fence = store.event_fence();
    let (next, _) = store.commit_live_append(
        CanonicalLiveAppend::new(
            fence,
            replacement_block(BlockHeight::new(5), [5; 32], [4; 32], 105)?,
            Vec::new(),
            BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([2; 32])),
            UnixTimestampMillis::new(1_750_000_000_002),
        ),
        &activations,
    )?;
    store = next;
    let fence = store.event_fence();
    let (next, _) = store.commit_live_replacement(
        CanonicalLiveReplacement::new(
            fence,
            replacement_suffix(4, 5, [3; 32], 40)?,
            UnixTimestampMillis::new(1_750_000_000_003),
        ),
        &activations,
    )?;
    store = next;
    let fence = store.event_fence();
    let (next, _) = store.commit_live_append(
        CanonicalLiveAppend::new(
            fence,
            replacement_block(BlockHeight::new(6), [6; 32], [45; 32], 106)?,
            Vec::new(),
            BlockId::new(BlockHeight::new(3), BlockHash::from_bytes([3; 32])),
            UnixTimestampMillis::new(1_750_000_000_004),
        ),
        &activations,
    )?;
    store = next;

    let store_complete = complete(&store.bounded_open.db, store.ready_evidence())?;
    assert_eq!(store.replay_tail.checkpoints(), store_complete);
    secondary.try_catch_up()?;
    assert_eq!(secondary.event_fence(), store.event_fence());
    assert_eq!(secondary.replay_tail.checkpoints(), store_complete);
    assert_eq!(
        store_complete
            .last()
            .map(|checkpoint| checkpoint.through().height),
        Some(BlockHeight::new(6))
    );
    Ok(())
}

#[test]
fn writer_detects_an_unannounced_replay_row_change_before_it_settles()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let store = published_store(&store_path, 2, BlockHeight::new(4), BlockHeight::new(2))?;
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    // Rewrite the unsettled row at height 3 outside any transition, keeping
    // its block identity so that only its facts differ.
    let mut forged = canonical_block(BlockHeight::new(3), [3; 32], [2; 32], 99);
    forged.facts.block_header = BlockHeaderArtifact::new(
        BlockHeight::new(3),
        BlockHash::from_bytes([3; 32]),
        BlockHash::from_bytes([2; 32]),
        [9; 32],
        [4; 32],
        3,
        0x1d00_ffff,
        [5; 32],
        4,
        128,
    );
    let forged_replay = encode_canonical_block_replay(
        &forged.facts,
        CanonicalBlockReplayFormatVersion::V1,
        CanonicalBlockFactsDigestVersion::V1,
    );
    let replay_family = store
        .bounded_open
        .db
        .cf_handle(super::block_replay::BLOCK_REPLAY_COLUMN_FAMILY)
        .ok_or("replay family absent")?;
    store.bounded_open.db.put_cf(
        &replay_family,
        zinder_core::wire::encode_height_key_ascending(BlockHeight::new(3)),
        forged_replay.as_bytes(),
    )?;
    drop(replay_family);

    let fence = store.event_fence();
    let error = store
        .commit_live_append(
            CanonicalLiveAppend::new(
                fence,
                replacement_block(BlockHeight::new(5), [5; 32], [4; 32], 105)?,
                Vec::new(),
                BlockId::new(BlockHeight::new(3), BlockHash::from_bytes([3; 32])),
                UnixTimestampMillis::new(1_750_000_000_002),
            ),
            &activations,
        )
        .err()
        .ok_or("settling a row changed outside a transition must fail")?;
    assert!(
        error.to_string().contains("settled checkpoint") || error.to_string().contains("replay"),
        "{error}"
    );
    Ok(())
}

#[test]
fn replacement_rejects_settled_range_without_any_mutation() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let store = published_store(&store_path, 2, BlockHeight::new(4), BlockHeight::new(2))?;
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let fence = store.event_fence();
    let replacement_two = replacement_block(BlockHeight::new(2), [22; 32], [1; 32], 202)?;
    let error = store
        .commit_live_replacement(
            CanonicalLiveReplacement::new(
                fence,
                vec![CanonicalReplacementBlock::new(replacement_two, Vec::new())],
                UnixTimestampMillis::new(1_750_000_000_002),
            ),
            &activations,
        )
        .err()
        .ok_or("replacement through settlement must fail")?;
    assert!(error.to_string().contains("settled tip"));
    let reopened = open_store(&store_path, &activations, 2)?;
    assert_eq!(reopened.event_fence(), fence);
    assert_eq!(reopened.displaced_block_count()?, 0);
    assert_eq!(
        reopened
            .block_header_at(BlockHeight::new(4))?
            .map(|header| header.block_hash),
        Some(BlockHash::from_bytes([4; 32]))
    );
    Ok(())
}

#[test]
fn replacement_rejects_a_stale_fence_without_archive_writes()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let store = published_store(&store_path, 2, BlockHeight::new(4), BlockHeight::new(2))?;
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let stale_fence = store.event_fence();
    let append = replacement_block(BlockHeight::new(5), [5; 32], [4; 32], 105)?;
    let (store, current_fence) = store.commit_live_append(
        CanonicalLiveAppend::new(
            stale_fence,
            append,
            Vec::new(),
            BlockId::new(BlockHeight::new(3), BlockHash::from_bytes([3; 32])),
            UnixTimestampMillis::new(1_750_000_000_002),
        ),
        &activations,
    )?;
    let replacement = replacement_block(BlockHeight::new(5), [55; 32], [4; 32], 205)?;
    let error = store
        .commit_live_replacement(
            CanonicalLiveReplacement::new(
                stale_fence,
                vec![CanonicalReplacementBlock::new(replacement, Vec::new())],
                UnixTimestampMillis::new(1_750_000_000_003),
            ),
            &activations,
        )
        .err()
        .ok_or("stale replacement fence must fail")?;
    assert!(error.to_string().contains("stale"));
    let reopened = open_store(&store_path, &activations, 2)?;
    assert_eq!(reopened.event_fence(), current_fence);
    assert_eq!(reopened.displaced_block_count()?, 0);
    Ok(())
}

#[test]
fn replacement_batch_is_either_pre_update_or_post_update_across_process_abort()
-> Result<(), Box<dyn std::error::Error>> {
    for failpoint in ["before_atomic_write", "after_atomic_write"] {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let store = published_store(&store_path, 2, BlockHeight::new(4), BlockHeight::new(2))?;
        drop(store);
        run_replacement_crash_child(&store_path, failpoint)?;
        let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let reopened = open_store(&store_path, &activations, 2)?;
        match failpoint {
            "before_atomic_write" => {
                assert_eq!(
                    reopened.event_fence().chain_epoch_id(),
                    ChainEpochId::new(1)
                );
                assert_eq!(reopened.displaced_block_count()?, 0);
                assert_eq!(
                    reopened
                        .block_header_at(BlockHeight::new(4))?
                        .map(|header| header.block_hash),
                    Some(BlockHash::from_bytes([4; 32]))
                );
            }
            "after_atomic_write" => {
                assert_eq!(
                    reopened.event_fence().chain_epoch_id(),
                    ChainEpochId::new(2)
                );
                assert_eq!(reopened.displaced_block_count()?, 2);
                assert_eq!(
                    reopened
                        .block_header_at(BlockHeight::new(3))?
                        .map(|header| header.block_hash),
                    Some(BlockHash::from_bytes([33; 32]))
                );
                assert_eq!(reopened.block_header_at(BlockHeight::new(4))?, None);
            }
            _ => return Err("unexpected replacement failpoint".into()),
        }
    }
    Ok(())
}

#[test]
fn ready_open_rejects_missing_or_inconsistent_reorg_archive_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let event_sequence = 2_u64;
    let context_key = {
        let mut key = vec![0x03];
        key.extend_from_slice(&event_sequence.to_be_bytes());
        key
    };
    let order_key = encode_test_order_key(
        event_sequence,
        BlockHeight::new(3),
        BlockHash::from_bytes([3; 32]),
    );
    let (pointer_key, _) = encode_test_hash_pointer_rows(
        event_sequence,
        BlockHeight::new(3),
        BlockHash::from_bytes([3; 32]),
    );
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let store = replaced_store(&store_path)?;
    drop(store);
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    for key in [vec![0x00], context_key, order_key, pointer_key] {
        let encoded_row = replace_archive_row(&store_path, &key, None)?
            .ok_or("archive corruption target must exist")?;
        let error = open_store(&store_path, &activations, 2)
            .err()
            .ok_or("corrupt reorg archive must fail READY admission")?;
        assert!(error.to_string().contains("archive"));
        replace_archive_row(&store_path, &key, Some(&encoded_row))?;
    }

    let corrupt_state = encode_test_archive_state(
        DisplacedBlockArchiveCoverage {
            activation_event_sequence: 2,
            activation_epoch: ChainEpochId::new(2),
            activated_at: UnixTimestampMillis::new(1_750_000_000_002),
        },
        3,
    );
    let original_state = replace_archive_row(&store_path, &[0x00], Some(&corrupt_state))?
        .ok_or("archive state must exist")?;
    let error = open_store(&store_path, &activations, 2)
        .err()
        .ok_or("inconsistent archive count must fail READY admission")?;
    assert!(error.to_string().contains("archive"));
    replace_archive_row(&store_path, &[0x00], Some(&original_state))?;
    let reopened = open_store(&store_path, &activations, 2)?;
    assert_eq!(reopened.displaced_block_count()?, 2);
    Ok(())
}

#[test]
fn secondary_point_reads_agree_with_the_bounded_replay_range_scan()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let secondary_path = temporary.path().join("secondary");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let _store = published_store(&store_path, 2, BlockHeight::new(4), BlockHeight::new(2))?;
    let secondary = RocksDbCanonicalSecondary::open_ready(
        &store_path,
        &secondary_path,
        &activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(2)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;

    let scanned = secondary
        .scan_canonical_replay_range(BlockHeightRange::inclusive(
            BlockHeight::new(1),
            BlockHeight::new(4),
        ))?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(scanned.len(), 4);
    for replay in &scanned {
        assert_eq!(
            secondary
                .block_replay_facts_at(replay.facts().block_header.height)?
                .as_ref(),
            Some(replay.facts())
        );
    }

    assert_eq!(secondary.block_replay_facts_at(BlockHeight::new(0))?, None);
    assert_eq!(secondary.block_replay_facts_at(BlockHeight::new(5))?, None);
    Ok(())
}

#[test]
fn retained_canonical_events_convert_into_dispatchable_chain_events()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = replaced_store(&temporary.path().join("canonical"))?;
    let page = store.canonical_event_history(CanonicalEventHistoryRequest::new(
        None,
        NonZeroU32::new(10).ok_or("zero page limit")?,
    ))?;
    let [baseline, reorged] = page.as_slice() else {
        return Err("expected one baseline event and one reorg event".into());
    };
    assert_eq!(baseline.kind(), CanonicalEventKind::Committed);
    assert_eq!(reorged.kind(), CanonicalEventKind::Reorged);

    let baseline_epoch = store.chain_epoch_at(baseline.resulting_epoch_id())?;
    assert_eq!(
        ChainEvent::from_canonical_retained(*baseline, baseline_epoch, None)?,
        ChainEvent::ChainCommitted {
            committed: ChainEpochCommitted::new(baseline_epoch, baseline.committed_range()),
        }
    );

    let reorged_epoch = store.chain_epoch_at(reorged.resulting_epoch_id())?;
    let reverted_epoch = store.chain_epoch_at(
        reorged
            .previous_epoch_id()
            .ok_or("reorg event carries no previous epoch")?,
    )?;
    assert_eq!(
        ChainEvent::from_canonical_retained(*reorged, reorged_epoch, Some(reverted_epoch))?,
        ChainEvent::ChainReorged {
            reverted: ChainRangeReverted::new(
                reverted_epoch,
                reorged
                    .reverted_range()
                    .ok_or("reorg event carries no reverted range")?,
            ),
            committed: ChainEpochCommitted::new(reorged_epoch, reorged.committed_range()),
        }
    );

    assert!(matches!(
        ChainEvent::from_canonical_retained(*reorged, reorged_epoch, None),
        Err(CanonicalStoreError::CanonicalEventRecordMalformed { .. })
    ));
    assert!(matches!(
        ChainEvent::from_canonical_retained(*baseline, reorged_epoch, None),
        Err(CanonicalStoreError::CanonicalEventRecordMalformed { .. })
    ));
    Ok(())
}

#[test]
fn replacement_crash_child_process() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(store_path) = env::var(REPLACEMENT_STORE_PATH_ENV) else {
        return Ok(());
    };
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let store = open_store(Path::new(&store_path), &activations, 2)?;
    let fence = store.event_fence();
    let replacement = replacement_block(BlockHeight::new(3), [33; 32], [2; 32], 203)?;
    let _ = store.commit_live_replacement(
        CanonicalLiveReplacement::new(
            fence,
            vec![CanonicalReplacementBlock::new(replacement, Vec::new())],
            UnixTimestampMillis::new(1_750_000_000_002),
        ),
        &activations,
    )?;
    Err("replacement crash failpoint did not abort".into())
}

fn run_replacement_crash_child(
    store_path: &Path,
    failpoint: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new(env::current_exe()?)
        .arg("--exact")
        .arg("canonical_store::live_replacement_tests::replacement_crash_child_process")
        .arg("--nocapture")
        .env(REPLACEMENT_STORE_PATH_ENV, store_path)
        .env(REPLACEMENT_FAILPOINT_ENV, failpoint)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if status.success() {
        return Err(format!("replacement failpoint {failpoint} did not crash").into());
    }
    Ok(())
}

fn replaced_store(path: &Path) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    let store = published_store(path, 2, BlockHeight::new(4), BlockHeight::new(2))?;
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let fence = store.event_fence();
    let replacement = replacement_block(BlockHeight::new(3), [33; 32], [2; 32], 203)?;
    let (store, _) = store.commit_live_replacement(
        CanonicalLiveReplacement::new(
            fence,
            vec![CanonicalReplacementBlock::new(replacement, Vec::new())],
            UnixTimestampMillis::new(1_750_000_000_002),
        ),
        &activations,
    )?;
    Ok(store)
}

fn replace_archive_row(
    path: &Path,
    key: &[u8],
    replacement: Option<&[u8]>,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    let column_families = DB::list_cf(&rust_rocksdb::Options::default(), path)?;
    let db = DB::open_cf(&rust_rocksdb::Options::default(), path, &column_families)?;
    let archive = db
        .cf_handle(DISPLACED_BLOCK_FACTS_COLUMN_FAMILY)
        .ok_or("archive family absent")?;
    let previous = db.get_cf(&archive, key)?;
    if let Some(replacement) = replacement {
        db.put_cf(&archive, key, replacement)?;
    } else {
        db.delete_cf(&archive, key)?;
    }
    db.flush_wal(true)?;
    Ok(previous)
}

fn published_store(
    path: &Path,
    reorg_window: u32,
    build_tip_height: BlockHeight,
    settled_height: BlockHeight,
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    published_store_with_retention(
        path,
        reorg_window,
        build_tip_height,
        settled_height,
        RawBlobRetention::Transactions,
    )
}

fn published_store_with_retention(
    path: &Path,
    reorg_window: u32,
    build_tip_height: BlockHeight,
    settled_height: BlockHeight,
    raw_blob_retention: RawBlobRetention,
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let tip_byte = u8::try_from(build_tip_height.value())?;
    let build_plan = CanonicalStoreBuildPlan::complete(
        &activations,
        0,
        BlockId::new(build_tip_height, BlockHash::from_bytes([tip_byte; 32])),
        raw_blob_retention,
        CanonicalReorgPolicy::new(reorg_window)?,
    )?;
    let mut builder = RocksDbCanonicalBuilder::create_fresh(
        path,
        CanonicalStoreWorkload::Wallet,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let mut blocks = Vec::new();
    for height in 1..=build_tip_height.value() {
        let height = BlockHeight::new(height);
        let hash_byte = u8::try_from(height.value())?;
        let parent_hash = if height.value() == 1 {
            Network::ZcashTestnet.genesis_hash().as_bytes()
        } else {
            [u8::try_from(height.value() - 1)?; 32]
        };
        let mut block = canonical_block(height, [hash_byte; 32], parent_hash, 100 + hash_byte);
        apply_raw_blob_retention(&mut block, raw_blob_retention, 100 + hash_byte);
        if height == build_tip_height
            || height
                .value()
                .is_multiple_of(super::TREE_STATE_CHECKPOINT_STRIDE)
        {
            add_checkpoint(&mut block)?;
        }
        blocks.push(Ok::<_, std::io::Error>(block));
    }
    builder.bulk_load_blocks(blocks)?;
    builder.load_subtree_roots(std::iter::empty())?;
    builder.confirm_source_tip_checkpoint(&zinder_core::CommitmentTreeCheckpoint::new(
        BlockId::new(build_tip_height, BlockHash::from_bytes([tip_byte; 32])),
        build_tip_height.value(),
        super::test_checkpoint_frontiers(&activations, build_tip_height),
    ))?;
    let validated = builder.prepare_cold_certified_publication()?;
    let settled_byte = u8::try_from(settled_height.value())?;
    let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
        BlockId::new(settled_height, BlockHash::from_bytes([settled_byte; 32])),
        UnixTimestampMillis::new(1_750_000_000_001),
    ))?;
    Ok(validated.publish_baseline(publication)?)
}

fn open_store(
    path: &Path,
    activations: &zinder_core::NetworkUpgradeActivations,
    reorg_window: u32,
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    open_store_with_retention(
        path,
        activations,
        reorg_window,
        RawBlobRetention::Transactions,
    )
}

fn open_store_with_retention(
    path: &Path,
    activations: &zinder_core::NetworkUpgradeActivations,
    reorg_window: u32,
    raw_blob_retention: RawBlobRetention,
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    Ok(RocksDbCanonicalStore::open_ready(
        path,
        activations,
        CanonicalStoreWorkload::Wallet,
        raw_blob_retention,
        CanonicalReorgPolicy::new(reorg_window)?,
        RocksDbResourceBudget::for_local_tests(),
    )?)
}

fn open_writer(
    path: &Path,
    activations: &zinder_core::NetworkUpgradeActivations,
    reorg_window: u32,
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    Ok(
        RocksDbCanonicalStore::open_ready_with_network_upgrade_schedule(
            path,
            activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(reorg_window)?,
            RocksDbResourceBudget::for_local_tests(),
        )?,
    )
}

fn replacement_block(
    height: BlockHeight,
    block_hash: [u8; 32],
    parent_hash: [u8; 32],
    transaction_tag: u8,
) -> Result<CanonicalBuildBlock, Box<dyn std::error::Error>> {
    replacement_block_with_retention(
        height,
        block_hash,
        parent_hash,
        transaction_tag,
        RawBlobRetention::Transactions,
    )
}

fn replacement_block_with_retention(
    height: BlockHeight,
    block_hash: [u8; 32],
    parent_hash: [u8; 32],
    transaction_tag: u8,
    raw_blob_retention: RawBlobRetention,
) -> Result<CanonicalBuildBlock, Box<dyn std::error::Error>> {
    let mut block = canonical_block(height, block_hash, parent_hash, transaction_tag);
    apply_raw_blob_retention(&mut block, raw_blob_retention, transaction_tag);
    add_checkpoint(&mut block)?;
    Ok(block)
}

fn replacement_suffix(
    start_height: u32,
    end_height: u32,
    mut parent_hash: [u8; 32],
    hash_offset: u8,
) -> Result<Vec<CanonicalReplacementBlock>, Box<dyn std::error::Error>> {
    let mut blocks = Vec::new();
    for height in start_height..=end_height {
        let hash_byte = hash_offset
            .checked_add(u8::try_from(height)?)
            .ok_or("replacement hash tag overflowed")?;
        blocks.push(CanonicalReplacementBlock::new(
            replacement_block(
                BlockHeight::new(height),
                [hash_byte; 32],
                parent_hash,
                hash_byte,
            )?,
            Vec::new(),
        ));
        parent_hash = [hash_byte; 32];
    }
    Ok(blocks)
}

fn canonical_block(
    height: BlockHeight,
    block_hash: [u8; 32],
    parent_hash: [u8; 32],
    transaction_tag: u8,
) -> CanonicalBuildBlock {
    let raw_transaction_bytes = vec![transaction_tag];
    let transaction_id = TransactionId::from_bytes([transaction_tag; 32]);
    let transaction = CanonicalTransactionFacts {
        public_facts: TransactionPublicFacts {
            transaction_id,
            auth_digest: None,
            wtxid: None,
            version: TransactionVersion::Unsupported {
                effective_version: 0,
                version_group_id: None,
            },
            consensus_branch_id: None,
            lock_time: LockTime::Unlocked,
            expiry_height: None,
            size_bytes: 1,
            counts: TransactionComponentCounts::EMPTY,
            orchard_value_balance_zat: None,
            orchard_anchor: None,
            ironwood_value_balance_zat: None,
            privacy_shape: PrivacyShape::Unclassified,
            is_coinbase: true,
            unsupported_sections: vec![UnsupportedSection::FutureVersionHeader],
        },
        serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
            &raw_transaction_bytes,
        ),
        intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
        transparent_inputs: Vec::new(),
        transparent_outputs: Vec::new(),
    };
    let facts = CanonicalBlockFacts {
        block_header: BlockHeaderArtifact::new(
            height,
            BlockHash::from_bytes(block_hash),
            BlockHash::from_bytes(parent_hash),
            [3; 32],
            [4; 32],
            i64::from(height.value()),
            0x1d00_ffff,
            [5; 32],
            4,
            128,
        ),
        serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[]),
        transactions: vec![transaction],
    };
    let replay_envelope = encode_canonical_block_replay(
        &facts,
        CanonicalBlockReplayFormatVersion::V1,
        CanonicalBlockFactsDigestVersion::V1,
    );
    CanonicalBuildBlock {
        compact_block: CompactBlockArtifact::empty(
            BlockId::new(height, facts.block_header.block_hash),
            facts.block_header.parent_hash,
            height.value(),
            zinder_core::CompactChainMetadata {
                sapling_commitment_tree_size: 0,
                orchard_commitment_tree_size: 0,
                ironwood_commitment_tree_size: 0,
            },
        ),
        replay_envelope,
        tip_metadata: ChainTipMetadata::new(0, 0, 0),
        tree_state_checkpoint: None,
        block_final_note_commitment_roots: None,
        transaction_blobs: vec![TransactionBlobArtifact::new(
            transaction_location(height, block_hash, transaction_tag),
            raw_transaction_bytes,
        )],
        block_blob: None,
        facts,
    }
}

fn apply_raw_blob_retention(
    block: &mut CanonicalBuildBlock,
    raw_blob_retention: RawBlobRetention,
    block_tag: u8,
) {
    match raw_blob_retention {
        RawBlobRetention::None => block.transaction_blobs.clear(),
        RawBlobRetention::Transactions => {}
        RawBlobRetention::All => {
            let raw_block_bytes = vec![block_tag];
            block.facts.serialized_bytes_digest =
                SerializedBytesDigest::from_serialized_bytes(&raw_block_bytes);
            block.block_blob = Some(zinder_core::BlockBlobArtifact::new(
                block.facts.block_header.height,
                block.facts.block_header.block_hash,
                block.facts.block_header.parent_hash,
                raw_block_bytes,
            ));
            block.replay_envelope = encode_canonical_block_replay(
                &block.facts,
                CanonicalBlockReplayFormatVersion::V1,
                CanonicalBlockFactsDigestVersion::V1,
            );
        }
    }
}

fn transaction_location(
    height: BlockHeight,
    block_hash: [u8; 32],
    transaction_tag: u8,
) -> TransactionLocation {
    TransactionLocation::new(
        TransactionId::from_bytes([transaction_tag; 32]),
        height,
        BlockHash::from_bytes(block_hash),
        0,
    )
}

fn add_checkpoint(block: &mut CanonicalBuildBlock) -> Result<(), Box<dyn std::error::Error>> {
    let header = &block.facts.block_header;
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    block.tree_state_checkpoint = Some(zinder_core::CommitmentTreeCheckpoint::new(
        BlockId::new(header.height, header.block_hash),
        u32::try_from(header.block_time)?,
        super::test_checkpoint_frontiers(&activations, header.height),
    ));
    Ok(())
}
