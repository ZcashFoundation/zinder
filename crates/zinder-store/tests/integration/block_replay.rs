#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{num::NonZeroU32, path::Path};

use eyre::eyre;
use rust_rocksdb::{DB, Options};
use tempfile::tempdir;
use zinder_core::{
    BlockBlobArtifact, BlockHash, BlockHeaderArtifact, BlockHeight, CanonicalBlockFacts,
    CanonicalBlockFactsDigestVersion, CanonicalBlockReplayEnvelope,
    CanonicalBlockReplayFormatVersion, ChainEpoch, ChainEpochId, ChainTipMetadata,
    CompactBlockArtifact, Network, SerializedBytesDigest, TransactionId,
    TransactionIntrinsicValueBalances, TransactionIntrinsicValueBalancesArtifact,
    TransactionLocation, UnixTimestampMillis, encode_canonical_block_replay,
};
use zinder_store::{
    ArtifactFamily, BlockReplayBatchRequest, CURRENT_ARTIFACT_SCHEMA_VERSION, ChainEpochArtifacts,
    ChainStoreOptions, MAX_BLOCK_REPLAY_BATCH_BLOCKS, PrimaryChainStore, ReorgWindowChange,
    SecondaryChainStore, StoreError,
};

const BLOCK_REPLAY_COLUMN_FAMILY: &str = "block_replay";
const REORG_WINDOW_COLUMN_FAMILY: &str = "reorg_window";
const TRANSACTION_FACTS_COLUMN_FAMILY: &str = "transaction_facts";
const BLOCK_HEADER_VISIBILITY_KEY_KIND: u8 = 33;

#[test]
fn append_reorg_and_reopen_expose_only_epoch_canonical_replay() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let path = tempdir.path().join("canonical");
    let store = PrimaryChainStore::open(&path, ChainStoreOptions::for_local_tests())?;
    let initial = initial_artifacts();
    let initial_epoch = initial.chain_epoch;
    let initial_height_two_hash = initial.block_headers[1].block_hash;
    store.commit_chain_epoch(initial)?;

    let initial_reader = store.current_chain_epoch_reader()?;
    assert_eq!(
        initial_reader
            .block_replay_at(BlockHeight::new(2))?
            .map(|replay| replay.facts().block_header.block_hash),
        Some(initial_height_two_hash)
    );

    let replacement = replacement_artifacts();
    let replacement_epoch = replacement.chain_epoch;
    let replacement_hash = replacement.block_headers[0].block_hash;
    store.commit_chain_epoch(replacement)?;
    let replacement_reader = store.current_chain_epoch_reader()?;
    assert_eq!(replacement_reader.chain_epoch(), replacement_epoch);
    assert_eq!(
        replacement_reader
            .block_replay_at(BlockHeight::new(2))?
            .map(|replay| replay.facts().block_header.block_hash),
        Some(replacement_hash)
    );
    assert_eq!(
        initial_reader
            .block_replay_at(BlockHeight::new(2))?
            .map(|replay| replay.facts().block_header.block_hash),
        Some(initial_height_two_hash)
    );

    drop(initial_reader);
    drop(replacement_reader);
    drop(store);
    let reopened = PrimaryChainStore::open(&path, ChainStoreOptions::for_local_tests())?;
    let reopened_reader = reopened.current_chain_epoch_reader()?;
    assert_eq!(reopened_reader.chain_epoch(), replacement_epoch);
    assert_eq!(
        reopened_reader
            .block_replay_at(BlockHeight::new(2))?
            .map(|replay| replay.facts().block_header.block_hash),
        Some(replacement_hash)
    );
    assert_eq!(
        reopened
            .chain_epoch_reader_at(initial_epoch.id)?
            .block_replay_at(BlockHeight::new(2))?
            .map(|replay| replay.facts().block_header.block_hash),
        Some(initial_height_two_hash)
    );

    Ok(())
}

#[test]
fn batch_reads_are_ordered_clipped_and_empty_beyond_the_visible_tip() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;
    store.commit_chain_epoch(initial_artifacts())?;
    let reader = store.current_chain_epoch_reader()?;

    let replay_hashes = reader
        .block_replay_batch(BlockReplayBatchRequest::new(
            BlockHeight::new(1),
            NonZeroU32::MIN.saturating_add(1),
        ))?
        .into_iter()
        .map(|replay| replay.facts().block_header.block_hash)
        .collect::<Vec<_>>();
    assert_eq!(replay_hashes, vec![block_hash(1), block_hash(2)]);

    let clipped_hashes = reader
        .block_replay_batch(BlockReplayBatchRequest::new(
            BlockHeight::new(2),
            NonZeroU32::MIN.saturating_add(9),
        ))?
        .into_iter()
        .map(|replay| replay.facts().block_header.block_hash)
        .collect::<Vec<_>>();
    assert_eq!(clipped_hashes, vec![block_hash(2)]);
    assert!(
        reader
            .block_replay_batch(BlockReplayBatchRequest::new(
                BlockHeight::new(3),
                NonZeroU32::MIN,
            ))?
            .is_empty()
    );

    Ok(())
}

#[test]
fn batch_reads_resolve_mixed_source_epochs_after_a_reorg() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;
    store.commit_chain_epoch(initial_artifacts())?;
    store.commit_chain_epoch(replacement_artifacts())?;

    let replay_hashes = store
        .current_chain_epoch_reader()?
        .block_replay_batch(BlockReplayBatchRequest::new(
            BlockHeight::new(1),
            NonZeroU32::MIN.saturating_add(1),
        ))?
        .into_iter()
        .map(|replay| replay.facts().block_header.block_hash)
        .collect::<Vec<_>>();
    assert_eq!(replay_hashes, vec![block_hash(1), block_hash(20)]);

    Ok(())
}

#[test]
fn batch_limit_is_rejected_before_storage_or_visibility_reads() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;
    store.commit_chain_epoch(initial_artifacts())?;
    let requested_blocks = MAX_BLOCK_REPLAY_BATCH_BLOCKS.saturating_add(1);

    let error =
        match store
            .current_chain_epoch_reader()?
            .block_replay_batch(BlockReplayBatchRequest::new(
                BlockHeight::new(3),
                requested_blocks,
            )) {
            Ok(replays) => return Err(eyre!("over-limit replay batch succeeded: {replays:?}")),
            Err(error) => error,
        };
    assert!(matches!(
        error,
        StoreError::ArtifactRangeTooLarge {
            family: ArtifactFamily::BlockReplay,
            requested_block_count: actual_requested_blocks,
            maximum_block_count,
        } if actual_requested_blocks == requested_blocks
            && maximum_block_count == MAX_BLOCK_REPLAY_BATCH_BLOCKS
    ));

    Ok(())
}

#[test]
fn secondary_catchup_switches_epoch_and_replay_together() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let primary_path = tempdir.path().join("primary");
    let secondary_path = tempdir.path().join("secondary");
    let primary = PrimaryChainStore::open(&primary_path, ChainStoreOptions::for_local_tests())?;
    let initial = initial_artifacts();
    let initial_epoch = initial.chain_epoch;
    let initial_hash = initial.block_headers[1].block_hash;
    primary.commit_chain_epoch(initial)?;
    let secondary = SecondaryChainStore::open(
        &primary_path,
        &secondary_path,
        ChainStoreOptions::for_local_tests(),
    )?;

    let replacement = replacement_artifacts();
    let replacement_epoch = replacement.chain_epoch;
    let replacement_hash = replacement.block_headers[0].block_hash;
    primary.commit_chain_epoch(replacement)?;

    let before = secondary.current_chain_epoch_reader()?;
    assert_eq!(before.chain_epoch(), initial_epoch);
    assert_eq!(
        before
            .block_replay_batch(BlockReplayBatchRequest::new(
                BlockHeight::new(1),
                NonZeroU32::MIN.saturating_add(1),
            ))?
            .into_iter()
            .map(|replay| replay.facts().block_header.block_hash)
            .collect::<Vec<_>>(),
        vec![block_hash(1), initial_hash]
    );

    let catchup = secondary.try_catch_up()?;
    assert_eq!(catchup.before, Some(initial_epoch.id));
    assert_eq!(catchup.after, Some(replacement_epoch.id));
    assert!(matches!(
        before.block_replay_batch(BlockReplayBatchRequest::new(
            BlockHeight::new(1),
            NonZeroU32::MIN.saturating_add(1),
        )),
        Err(StoreError::ChainEpochConflict { current, attempted })
            if current == replacement_epoch.id && attempted == initial_epoch.id
    ));
    drop(before);
    let after = secondary.current_chain_epoch_reader()?;
    assert_eq!(after.chain_epoch(), replacement_epoch);
    assert_eq!(
        after
            .block_replay_batch(BlockReplayBatchRequest::new(
                BlockHeight::new(1),
                NonZeroU32::MIN.saturating_add(1),
            ))?
            .into_iter()
            .map(|replay| replay.facts().block_header.block_hash)
            .collect::<Vec<_>>(),
        vec![block_hash(1), replacement_hash]
    );

    Ok(())
}

#[test]
fn missing_replay_envelope_rejects_the_commit_without_advancing_visibility() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;
    let initial = initial_artifacts();
    let initial_epoch = initial.chain_epoch;
    store.commit_chain_epoch(initial)?;

    let replacement = replacement_artifacts();
    let incomplete = ChainEpochArtifacts::new(
        replacement.chain_epoch,
        replacement.block_headers,
        Vec::new(),
        replacement.compact_blocks,
    )
    .with_reorg_window_change(ReorgWindowChange::Replace {
        from_height: BlockHeight::new(2),
    });
    assert!(matches!(
        store.commit_chain_epoch(incomplete),
        Err(StoreError::InvalidChainEpochArtifacts { .. })
    ));
    assert_eq!(store.current_chain_epoch()?, Some(initial_epoch));
    assert_eq!(
        store
            .current_chain_epoch_reader()?
            .block_replay_at(BlockHeight::new(2))?
            .map(|replay| replay.facts().block_header.block_hash),
        Some(block_hash(2))
    );

    Ok(())
}

#[test]
fn replay_envelopes_must_follow_committed_block_header_order() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;
    let mut artifacts = initial_artifacts();
    artifacts.block_replay_envelopes.swap(0, 1);

    assert!(matches!(
        store.commit_chain_epoch(artifacts),
        Err(StoreError::InvalidChainEpochArtifacts {
            reason: "block replay must follow committed block header order",
        })
    ));
    assert_eq!(store.current_chain_epoch()?, None);

    Ok(())
}

#[test]
fn replay_public_facts_and_intrinsic_balances_must_match_canonical_rows() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;
    let mut public_facts_mismatch = single_transaction_artifacts();
    public_facts_mismatch.transaction_facts[0]
        .public_facts
        .size_bytes += 1;
    assert_commit_rejected_with_reason(
        &store,
        public_facts_mismatch,
        "ordered block replay transactions must match index and transaction facts",
    )?;

    let mut intrinsic_balance_mismatch = single_transaction_artifacts();
    intrinsic_balance_mismatch.transaction_intrinsic_value_balances[0].value_balances =
        TransactionIntrinsicValueBalances::new(5, 6, 7, 8);
    assert_commit_rejected_with_reason(
        &store,
        intrinsic_balance_mismatch,
        "transaction intrinsic balances must match block replay",
    )
}

#[test]
fn retained_transaction_blob_size_and_bytes_must_match_block_replay() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let mut size_mismatch = single_transaction_artifacts();
    size_mismatch.transaction_blobs[0]
        .raw_transaction_bytes
        .pop();
    assert_commit_rejected_with_reason(
        &store,
        size_mismatch,
        "transaction blob size must match block replay",
    )?;

    let mut same_size_substitution = single_transaction_artifacts();
    same_size_substitution.transaction_blobs[0]
        .raw_transaction_bytes
        .fill(0xff);
    assert_commit_rejected_with_reason(
        &store,
        same_size_substitution,
        "transaction blob bytes must match block replay digest",
    )
}

#[test]
fn retained_block_blob_size_and_bytes_must_match_block_replay() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let mut size_mismatch = single_transaction_artifacts();
    size_mismatch.block_blobs[0].raw_block_bytes.pop();
    assert_commit_rejected_with_reason(
        &store,
        size_mismatch,
        "block blob size must match block replay",
    )?;

    let mut same_size_substitution = single_transaction_artifacts();
    same_size_substitution.block_blobs[0]
        .raw_block_bytes
        .fill(0xff);
    assert_commit_rejected_with_reason(
        &store,
        same_size_substitution,
        "block blob bytes must match block replay digest",
    )
}

#[test]
fn canonical_transaction_rows_not_represented_by_replay_are_rejected() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;
    let mut artifacts = single_transaction_artifacts();
    let header = &artifacts.block_headers[0];
    let extra_transaction_id = TransactionId::from_bytes([8; 32]);
    let (index, location, facts, blob) = super::synthetic_transaction_rows(
        extra_transaction_id,
        header.height,
        header.block_hash,
        1,
        b"extra-canonical-transaction",
    );
    artifacts.block_transaction_index.push(index);
    artifacts.transaction_locations.push(location);
    artifacts.transaction_facts.push(facts);
    artifacts.transaction_intrinsic_value_balances.push(
        TransactionIntrinsicValueBalancesArtifact::new(
            location,
            TransactionIntrinsicValueBalances::default(),
        ),
    );
    artifacts.transaction_blobs.push(blob);

    assert_commit_rejected_with_reason(
        &store,
        artifacts,
        "block replay transaction count must match block index and transaction facts",
    )
}

#[test]
fn intrinsic_balance_rows_not_represented_by_replay_are_rejected() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;
    let mut artifacts = single_transaction_artifacts();
    let header = &artifacts.block_headers[0];
    artifacts.transaction_intrinsic_value_balances.push(
        TransactionIntrinsicValueBalancesArtifact::new(
            TransactionLocation::new(
                TransactionId::from_bytes([9; 32]),
                header.height,
                header.block_hash,
                1,
            ),
            TransactionIntrinsicValueBalances::default(),
        ),
    );

    assert_commit_rejected_with_reason(
        &store,
        artifacts,
        "committed artifacts must belong to the supplied block replay",
    )
}

#[test]
fn missing_and_corrupt_replay_rows_fail_closed_on_public_reads() -> eyre::Result<()> {
    for mutation in [StoredRowMutation::Delete, StoredRowMutation::Corrupt] {
        let tempdir = tempdir()?;
        let path = tempdir.path().join("canonical");
        {
            let store = PrimaryChainStore::open(&path, ChainStoreOptions::for_local_tests())?;
            store.commit_chain_epoch(single_block_artifacts())?;
        }
        mutate_first_replay_row(&path, mutation)?;

        let store = PrimaryChainStore::open(&path, ChainStoreOptions::for_local_tests())?;
        let error = match store
            .current_chain_epoch_reader()?
            .block_replay_at(BlockHeight::new(1))
        {
            Ok(replay) => return Err(eyre!("mutated replay row was accepted: {replay:?}")),
            Err(error) => error,
        };
        match mutation {
            StoredRowMutation::Delete => assert!(matches!(
                error,
                StoreError::ArtifactMissing {
                    family: ArtifactFamily::BlockReplay,
                    ..
                }
            )),
            StoredRowMutation::Corrupt => assert!(matches!(
                error,
                StoreError::ArtifactCorrupt {
                    family: ArtifactFamily::BlockReplay,
                    ..
                }
            )),
        }
    }

    Ok(())
}

#[test]
fn missing_and_corrupt_visibility_rows_fail_closed_on_batch_reads() -> eyre::Result<()> {
    for mutation in [StoredRowMutation::Delete, StoredRowMutation::Corrupt] {
        let tempdir = tempdir()?;
        let path = tempdir.path().join("canonical");
        {
            let store = PrimaryChainStore::open(&path, ChainStoreOptions::for_local_tests())?;
            store.commit_chain_epoch(single_block_artifacts())?;
        }
        mutate_first_block_header_visibility_row(&path, mutation)?;

        let store = PrimaryChainStore::open(&path, ChainStoreOptions::for_local_tests())?;
        let error = match store.current_chain_epoch_reader()?.block_replay_batch(
            BlockReplayBatchRequest::new(BlockHeight::new(1), NonZeroU32::MIN),
        ) {
            Ok(replays) => {
                return Err(eyre!(
                    "batch accepted mutated block visibility row: {replays:?}"
                ));
            }
            Err(error) => error,
        };
        match mutation {
            StoredRowMutation::Delete => assert!(matches!(
                error,
                StoreError::ArtifactMissing {
                    family: ArtifactFamily::BlockReplay,
                    ..
                }
            )),
            StoredRowMutation::Corrupt => assert!(matches!(
                error,
                StoreError::ArtifactCorrupt {
                    family: ArtifactFamily::BlockReplay,
                    ..
                }
            )),
        }
    }

    Ok(())
}

#[test]
fn valid_length_visibility_pointer_to_displaced_replay_fails_closed() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let path = tempdir.path().join("canonical");
    {
        let store = PrimaryChainStore::open(&path, ChainStoreOptions::for_local_tests())?;
        store.commit_chain_epoch(initial_artifacts())?;
        store.commit_chain_epoch(replacement_artifacts())?;
    }
    rewrite_block_header_visibility_pointer(
        &path,
        BlockHeight::new(2),
        ChainEpochId::new(2),
        ChainEpochId::new(1),
    )?;

    let store = PrimaryChainStore::open(&path, ChainStoreOptions::for_local_tests())?;
    let reader = store.current_chain_epoch_reader()?;
    let point_error = match reader.block_replay_at(BlockHeight::new(2)) {
        Ok(replay) => {
            return Err(eyre!(
                "point read accepted redirected visibility pointer: {replay:?}"
            ));
        }
        Err(error) => error,
    };
    assert!(matches!(
        point_error,
        StoreError::ArtifactCorrupt {
            family: ArtifactFamily::BlockReplay,
            reason: "visible artifact epoch pointer does not match its publication epoch",
            ..
        }
    ));

    let batch_error = match reader.block_replay_batch(BlockReplayBatchRequest::new(
        BlockHeight::new(2),
        NonZeroU32::MIN,
    )) {
        Ok(replays) => {
            return Err(eyre!(
                "batch read accepted redirected visibility pointer: {replays:?}"
            ));
        }
        Err(error) => error,
    };
    assert!(matches!(
        batch_error,
        StoreError::ArtifactCorrupt {
            family: ArtifactFamily::BlockReplay,
            reason: "visible artifact epoch pointer does not match its publication epoch",
            ..
        }
    ));

    Ok(())
}

#[test]
fn missing_transaction_facts_row_is_a_storage_fault_not_an_absence() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let path = tempdir.path().join("canonical");
    let options = ChainStoreOptions {
        raw_blob_retention: zinder_store::RawBlobRetention::All,
        ..ChainStoreOptions::for_local_tests()
    };
    {
        let store = PrimaryChainStore::open(&path, options)?;
        store.commit_chain_epoch(single_transaction_artifacts())?;
    }
    delete_first_row(&path, TRANSACTION_FACTS_COLUMN_FAMILY)?;

    let store = PrimaryChainStore::open(&path, options)?;
    let lookup = store
        .current_chain_epoch_reader()?
        .transaction_facts_or_proven_absence(TransactionId::from_bytes([7; 32]));

    assert!(
        matches!(
            lookup,
            Err(StoreError::ArtifactMissing {
                family: ArtifactFamily::TransactionFacts,
                ..
            })
        ),
        "a visible transaction whose facts row is gone must fail, got {lookup:?}"
    );
    Ok(())
}

fn delete_first_row(path: &Path, column_family_name: &str) -> eyre::Result<()> {
    let column_families = DB::list_cf(&Options::default(), path)?;
    let database = DB::open_cf(&Options::default(), path, column_families)?;
    let column_family = database
        .cf_handle(column_family_name)
        .ok_or_else(|| eyre!("missing {column_family_name} column family"))?;
    let mut iterator = database.raw_iterator_cf(&column_family);
    iterator.seek_to_first();
    let key = iterator
        .key()
        .ok_or_else(|| eyre!("missing {column_family_name} row"))?
        .to_vec();
    drop(iterator);
    database.delete_cf(&column_family, key)?;
    database.flush_cf(&column_family)?;
    Ok(())
}

#[derive(Clone, Copy)]
enum StoredRowMutation {
    Delete,
    Corrupt,
}

fn mutate_first_replay_row(path: &Path, mutation: StoredRowMutation) -> eyre::Result<()> {
    let column_families = DB::list_cf(&Options::default(), path)?;
    let database = DB::open_cf(&Options::default(), path, column_families)?;
    let column_family = database
        .cf_handle(BLOCK_REPLAY_COLUMN_FAMILY)
        .ok_or_else(|| eyre!("missing block replay column family"))?;
    let mut iterator = database.raw_iterator_cf(&column_family);
    iterator.seek_to_first();
    let key = iterator
        .key()
        .ok_or_else(|| eyre!("missing block replay row"))?
        .to_vec();
    drop(iterator);
    match mutation {
        StoredRowMutation::Delete => database.delete_cf(&column_family, key)?,
        StoredRowMutation::Corrupt => database.put_cf(&column_family, key, [0xff])?,
    }
    database.flush_cf(&column_family)?;
    Ok(())
}

fn mutate_first_block_header_visibility_row(
    path: &Path,
    mutation: StoredRowMutation,
) -> eyre::Result<()> {
    let column_families = DB::list_cf(&Options::default(), path)?;
    let database = DB::open_cf(&Options::default(), path, column_families)?;
    let column_family = database
        .cf_handle(REORG_WINDOW_COLUMN_FAMILY)
        .ok_or_else(|| eyre!("missing reorg window column family"))?;
    let mut iterator = database.raw_iterator_cf(&column_family);
    iterator.seek_to_first();
    let mut visibility_key = None;
    while iterator.valid() {
        if let Some(key) = iterator.key()
            && key.get(1) == Some(&BLOCK_HEADER_VISIBILITY_KEY_KIND)
        {
            visibility_key = Some(key.to_vec());
            break;
        }
        iterator.next();
    }
    iterator.status()?;
    let key = visibility_key.ok_or_else(|| eyre!("missing block-header visibility row"))?;
    drop(iterator);
    match mutation {
        StoredRowMutation::Delete => database.delete_cf(&column_family, key)?,
        StoredRowMutation::Corrupt => database.put_cf(&column_family, key, [0xff])?,
    }
    database.flush_cf(&column_family)?;
    Ok(())
}

fn rewrite_block_header_visibility_pointer(
    path: &Path,
    height: BlockHeight,
    publication_epoch: ChainEpochId,
    source_epoch: ChainEpochId,
) -> eyre::Result<()> {
    let column_families = DB::list_cf(&Options::default(), path)?;
    let database = DB::open_cf(&Options::default(), path, column_families)?;
    let column_family = database
        .cf_handle(REORG_WINDOW_COLUMN_FAMILY)
        .ok_or_else(|| eyre!("missing reorg window column family"))?;
    let expected_height_bytes = height.value().to_be_bytes();
    let expected_publication_epoch_bytes = publication_epoch.value().to_be_bytes();
    let mut iterator = database.raw_iterator_cf(&column_family);
    iterator.seek_to_first();
    let mut visibility_key = None;
    while iterator.valid() {
        if let Some(key) = iterator.key()
            && key.get(1) == Some(&BLOCK_HEADER_VISIBILITY_KEY_KIND)
            && key.get(key.len().saturating_sub(12)..key.len().saturating_sub(8))
                == Some(expected_height_bytes.as_slice())
            && key.get(key.len().saturating_sub(8)..)
                == Some(expected_publication_epoch_bytes.as_slice())
        {
            visibility_key = Some(key.to_vec());
            break;
        }
        iterator.next();
    }
    iterator.status()?;
    let key =
        visibility_key.ok_or_else(|| eyre!("missing selected block-header visibility row"))?;
    drop(iterator);
    database.put_cf(&column_family, key, source_epoch.value().to_be_bytes())?;
    database.flush_cf(&column_family)?;
    Ok(())
}

fn initial_artifacts() -> ChainEpochArtifacts {
    let header_1 = block_header(1, block_hash(1), block_hash(0));
    let header_2 = block_header(2, block_hash(2), block_hash(1));
    let headers = vec![header_1, header_2];
    ChainEpochArtifacts::new(
        chain_epoch(1, block_hash(2)),
        headers.clone(),
        replay_envelopes(&headers),
        compact_blocks(&headers),
    )
}

fn replacement_artifacts() -> ChainEpochArtifacts {
    let header = block_header(2, block_hash(20), block_hash(1));
    ChainEpochArtifacts::new(
        chain_epoch(2, header.block_hash),
        vec![header.clone()],
        replay_envelopes(std::slice::from_ref(&header)),
        compact_blocks(std::slice::from_ref(&header)),
    )
    .with_reorg_window_change(ReorgWindowChange::Replace {
        from_height: BlockHeight::new(2),
    })
}

fn single_block_artifacts() -> ChainEpochArtifacts {
    let header = block_header(1, block_hash(1), block_hash(0));
    ChainEpochArtifacts::new(
        ChainEpoch {
            visible_tip_height: BlockHeight::new(1),
            visible_tip_hash: header.block_hash,
            settled_tip_height: BlockHeight::new(1),
            settled_tip_hash: header.block_hash,
            ..chain_epoch(1, header.block_hash)
        },
        vec![header.clone()],
        replay_envelopes(std::slice::from_ref(&header)),
        compact_blocks(std::slice::from_ref(&header)),
    )
}

fn single_transaction_artifacts() -> ChainEpochArtifacts {
    let raw_block_bytes = b"canonical-block";
    let mut header = block_header(1, block_hash(1), block_hash(0));
    header.block_size_bytes = u64::try_from(raw_block_bytes.len()).unwrap_or(u64::MAX);
    let epoch = ChainEpoch {
        visible_tip_height: header.height,
        visible_tip_hash: header.block_hash,
        settled_tip_height: header.height,
        settled_tip_hash: header.block_hash,
        ..chain_epoch(1, header.block_hash)
    };
    let transaction_id = TransactionId::from_bytes([7; 32]);
    let raw_transaction_bytes = b"canonical-transaction";
    let (index, location, facts, blob) = super::synthetic_transaction_rows(
        transaction_id,
        header.height,
        header.block_hash,
        0,
        raw_transaction_bytes,
    );
    super::with_synthetic_block_replay_envelopes(
        super::synthetic_chain_epoch_artifacts(
            epoch,
            vec![header.clone()],
            compact_blocks(std::slice::from_ref(&header)),
        )
        .with_block_blobs(vec![BlockBlobArtifact::new(
            header.height,
            header.block_hash,
            header.parent_hash,
            raw_block_bytes.to_vec(),
        )])
        .with_block_transaction_index(vec![index])
        .with_transaction_locations(vec![location])
        .with_transaction_facts(vec![facts])
        .with_transaction_intrinsic_value_balances(vec![
            TransactionIntrinsicValueBalancesArtifact::new(
                location,
                TransactionIntrinsicValueBalances::new(1, 2, 3, 4),
            ),
        ])
        .with_transaction_blobs(vec![blob]),
    )
}

fn assert_commit_rejected_with_reason(
    store: &PrimaryChainStore,
    artifacts: ChainEpochArtifacts,
    expected_reason: &'static str,
) -> eyre::Result<()> {
    match store.commit_chain_epoch(artifacts) {
        Err(StoreError::InvalidChainEpochArtifacts { reason }) => {
            assert_eq!(reason, expected_reason);
        }
        outcome => return Err(eyre!("unexpected commit outcome: {outcome:?}")),
    }
    assert_eq!(store.current_chain_epoch()?, None);
    Ok(())
}

fn replay_envelopes(headers: &[BlockHeaderArtifact]) -> Vec<CanonicalBlockReplayEnvelope> {
    headers
        .iter()
        .map(|header| {
            encode_canonical_block_replay(
                &CanonicalBlockFacts {
                    block_header: header.clone(),
                    serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[]),
                    transactions: Vec::new(),
                },
                CanonicalBlockReplayFormatVersion::CURRENT,
                CanonicalBlockFactsDigestVersion::CURRENT,
            )
        })
        .collect()
}

fn compact_blocks(headers: &[BlockHeaderArtifact]) -> Vec<CompactBlockArtifact> {
    headers
        .iter()
        .map(|header| super::empty_compact_block_for_header(header, ChainTipMetadata::empty()))
        .collect()
}

fn block_header(height: u32, hash: BlockHash, parent_hash: BlockHash) -> BlockHeaderArtifact {
    BlockHeaderArtifact::new(
        BlockHeight::new(height),
        hash,
        parent_hash,
        [0x01; 32],
        [0x02; 32],
        i64::from(height),
        0x1d00_ffff,
        [0x03; 32],
        4,
        128,
    )
}

fn chain_epoch(id: u64, tip_hash: BlockHash) -> ChainEpoch {
    ChainEpoch {
        id: ChainEpochId::new(id),
        network: Network::ZcashRegtest,
        visible_tip_height: BlockHeight::new(2),
        visible_tip_hash: tip_hash,
        settled_tip_height: BlockHeight::new(1),
        settled_tip_hash: block_hash(1),
        artifact_schema_version: CURRENT_ARTIFACT_SCHEMA_VERSION,
        tip_metadata: ChainTipMetadata::empty(),
        created_at: UnixTimestampMillis::new(1_774_668_000_000 + id),
    }
}

fn block_hash(seed: u8) -> BlockHash {
    BlockHash::from_bytes([seed; 32])
}
