//! A by-ID transaction read answers as the location read followed by the
//! by-location blob read does, and reads the location row once.

use std::path::Path;

use rust_rocksdb::statistics::Ticker;
use tempfile::TempDir;
use zinder_core::{
    BlockHash, BlockHeight, Network, TransactionBlobArtifact, TransactionId, TransactionLocation,
    UnixTimestampMillis,
};

use super::{
    CanonicalLiveReplacement, CanonicalReplacementBlock, CanonicalStoreError,
    RocksDbCanonicalStore,
    block_load::encode_transaction_location,
    live_replacement_tests::{
        published_note_commitment_store, published_store, published_store_with_retention,
        replacement_block,
    },
    rocksdb::TRANSACTION_LOCATION_COLUMN_FAMILY,
};
use crate::RawBlobRetention;

type TestResult = Result<(), Box<dyn std::error::Error>>;
type LocatedBlob = Option<(TransactionLocation, Option<TransactionBlobArtifact>)>;

/// The fixture chain mines transaction `[100 + h; 32]` as the only transaction
/// of block `h`, whose hash is `[h; 32]`, with raw bytes `[100 + h]`.
fn mined_at(height: u8) -> TransactionId {
    TransactionId::from_bytes([100 + height; 32])
}

fn never_mined() -> TransactionId {
    TransactionId::from_bytes([0xee; 32])
}

/// The answer the location read and then the by-location blob read give.
fn two_reads(
    store: &RocksDbCanonicalStore,
    transaction_id: TransactionId,
) -> Result<LocatedBlob, CanonicalStoreError> {
    let Some(location) = store.transaction_location(transaction_id)? else {
        return Ok(None);
    };
    Ok(Some((location, store.transaction_blob(location)?)))
}

/// Checks that both by-ID reads give the two-read answer, and returns it.
fn assert_by_id_matches_two_reads(
    store: &RocksDbCanonicalStore,
    transaction_id: TransactionId,
) -> Result<LocatedBlob, Box<dyn std::error::Error>> {
    let expected = two_reads(store, transaction_id)?;
    assert_eq!(
        store.transaction_location_and_blob(transaction_id)?,
        expected
    );
    assert_eq!(
        store.transaction_location_and_blob_or_proven_absence(transaction_id)?,
        expected
    );
    Ok(expected)
}

fn keys_read(store: &RocksDbCanonicalStore) -> u64 {
    store
        .bounded_open
        .statistics
        .get_ticker_count(Ticker::NumberKeysRead)
}

/// Writes `location` over its transaction's location row, as store damage would.
fn overwrite_location_row(
    store: &RocksDbCanonicalStore,
    location: TransactionLocation,
) -> TestResult {
    let family = store
        .bounded_open
        .db
        .cf_handle(TRANSACTION_LOCATION_COLUMN_FAMILY)
        .ok_or("transaction location family absent")?;
    let record = encode_transaction_location(location);
    let (key, location_value) = record.split_at(32);
    store.bounded_open.db.put_cf(&family, key, location_value)?;
    Ok(())
}

fn store_at(
    path: &Path,
    retention: RawBlobRetention,
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    published_store_with_retention(path, 2, BlockHeight::new(4), BlockHeight::new(2), retention)
}

#[test]
fn a_mined_transaction_comes_back_with_its_location_and_bytes() -> TestResult {
    let temporary = TempDir::new()?;
    let store = store_at(
        &temporary.path().join("canonical"),
        RawBlobRetention::Transactions,
    )?;

    let (location, blob) =
        assert_by_id_matches_two_reads(&store, mined_at(3))?.ok_or("transaction 103 is mined")?;
    assert_eq!(
        location,
        TransactionLocation::new(
            mined_at(3),
            BlockHeight::new(3),
            BlockHash::from_bytes([3; 32]),
            0
        )
    );
    let blob = blob.ok_or("transaction bytes are retained")?;
    assert_eq!(blob.location, location);
    assert_eq!(blob.raw_transaction_bytes, vec![103]);
    Ok(())
}

#[test]
fn a_transaction_the_chain_never_mined_is_a_proven_absence() -> TestResult {
    let temporary = TempDir::new()?;
    let store = store_at(
        &temporary.path().join("canonical"),
        RawBlobRetention::Transactions,
    )?;

    assert_eq!(assert_by_id_matches_two_reads(&store, never_mined())?, None);
    Ok(())
}

#[test]
fn a_checkpointed_store_refuses_to_prove_an_absence_but_finds_retained_transactions() -> TestResult
{
    let temporary = TempDir::new()?;
    let store = published_note_commitment_store(
        &temporary.path().join("canonical"),
        2,
        BlockHeight::new(4),
        BlockHeight::new(4),
        Some(BlockHeight::new(2)),
    )?;

    assert_eq!(store.transaction_location_and_blob(never_mined())?, None);
    match store.transaction_location_and_blob_or_proven_absence(never_mined()) {
        Err(CanonicalStoreError::CanonicalHistoryUnavailable {
            first_available_height,
        }) => assert_eq!(first_available_height, BlockHeight::new(3)),
        other => return Err(format!("expected an unprovable absence, got {other:?}").into()),
    }
    match store.transaction_location_or_proven_absence(never_mined()) {
        Err(CanonicalStoreError::CanonicalHistoryUnavailable {
            first_available_height,
        }) => assert_eq!(first_available_height, BlockHeight::new(3)),
        other => return Err(format!("expected an unprovable absence, got {other:?}").into()),
    }

    let (location, blob) = assert_by_id_matches_two_reads(&store, mined_at(3))?
        .ok_or("transaction 103 is retained")?;
    assert_eq!(location.block_height, BlockHeight::new(3));
    assert_eq!(blob.map(|blob| blob.raw_transaction_bytes), Some(vec![103]));
    Ok(())
}

#[test]
fn a_transaction_whose_bytes_are_not_retained_keeps_its_location() -> TestResult {
    let temporary = TempDir::new()?;
    let store = store_at(&temporary.path().join("canonical"), RawBlobRetention::None)?;

    let (location, blob) =
        assert_by_id_matches_two_reads(&store, mined_at(3))?.ok_or("transaction 103 is mined")?;
    assert_eq!(location.block_height, BlockHeight::new(3));
    assert_eq!(blob, None);
    Ok(())
}

#[test]
fn a_reorg_moves_or_removes_the_transaction_a_by_id_read_returns() -> TestResult {
    let temporary = TempDir::new()?;
    let store = store_at(
        &temporary.path().join("canonical"),
        RawBlobRetention::Transactions,
    )?;
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let stale_location = store
        .transaction_location(mined_at(3))?
        .ok_or("transaction 103 is mined")?;
    let fence = store.event_fence();

    // The replacement block at height 3 mines transaction 103 again under a
    // new hash, and drops block 4 with transaction 104.
    let (store, _) = store.commit_live_replacement(
        CanonicalLiveReplacement::new(
            fence,
            vec![CanonicalReplacementBlock::new(
                replacement_block(BlockHeight::new(3), [33; 32], [2; 32], 103)?,
                Vec::new(),
            )],
            UnixTimestampMillis::new(1_750_000_000_002),
        ),
        &activations,
    )?;

    assert_eq!(store.transaction_blob(stale_location)?, None);
    let (location, blob) = assert_by_id_matches_two_reads(&store, mined_at(3))?
        .ok_or("transaction 103 is mined again")?;
    assert_eq!(location.block_hash, BlockHash::from_bytes([33; 32]));
    assert_ne!(location, stale_location);
    assert_eq!(blob.map(|blob| blob.location), Some(location));
    assert_eq!(assert_by_id_matches_two_reads(&store, mined_at(4))?, None);
    Ok(())
}

#[test]
fn a_location_row_that_disagrees_with_the_blob_rows_answers_as_the_two_reads_do() -> TestResult {
    let temporary = TempDir::new()?;
    let store = store_at(
        &temporary.path().join("canonical"),
        RawBlobRetention::Transactions,
    )?;
    let written = store
        .transaction_location(mined_at(3))?
        .ok_or("transaction 103 is mined")?;

    // A damaged row that names block 2's position. Neither read checks raw
    // bytes against the transaction ID, so both return block 2's bytes.
    let other_position = TransactionLocation::new(
        mined_at(3),
        BlockHeight::new(2),
        BlockHash::from_bytes([2; 32]),
        0,
    );
    overwrite_location_row(&store, other_position)?;
    assert_eq!(store.transaction_blob(written)?, None);
    let (location, blob) =
        assert_by_id_matches_two_reads(&store, mined_at(3))?.ok_or("the damaged row is read")?;
    assert_eq!(location, other_position);
    assert_eq!(blob.map(|blob| blob.raw_transaction_bytes), Some(vec![102]));

    // A damaged row that names a position with no stored bytes.
    let empty_position = TransactionLocation::new(
        mined_at(3),
        BlockHeight::new(3),
        BlockHash::from_bytes([3; 32]),
        7,
    );
    overwrite_location_row(&store, empty_position)?;
    assert_eq!(
        assert_by_id_matches_two_reads(&store, mined_at(3))?,
        Some((empty_position, None))
    );
    Ok(())
}

#[test]
fn a_by_id_read_reads_the_location_row_once() -> TestResult {
    let temporary = TempDir::new()?;
    let store = published_store(
        &temporary.path().join("canonical"),
        2,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )?;

    let before = keys_read(&store);
    two_reads(&store, mined_at(3))?;
    // The location row, the location row again to authenticate it, and the
    // blob row.
    assert_eq!(keys_read(&store) - before, 3);

    let before = keys_read(&store);
    store.transaction_location_and_blob(mined_at(3))?;
    assert_eq!(keys_read(&store) - before, 2);

    let before = keys_read(&store);
    store.transaction_location_and_blob_or_proven_absence(mined_at(3))?;
    assert_eq!(keys_read(&store) - before, 2);

    // A miss stops at the location row, through either by-ID read.
    let before = keys_read(&store);
    store.transaction_location_and_blob(never_mined())?;
    assert_eq!(keys_read(&store) - before, 1);

    let before = keys_read(&store);
    store.transaction_location_and_blob_or_proven_absence(never_mined())?;
    assert_eq!(keys_read(&store) - before, 1);
    Ok(())
}
