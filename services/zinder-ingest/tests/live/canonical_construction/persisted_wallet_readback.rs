use std::{path::Path, time::Instant};

use eyre::{Result, eyre};
use rust_rocksdb::{DB, LiveFile, Options, ReadOptions};
use zinder_core::{
    BlockHeaderArtifact, BlockHeight, SerializedBytesDigest, decode_canonical_block_replay,
    wire::encode_height_key_ascending,
};
use zinder_store::CanonicalBlockLoadEvidence;

const BLOCK_HEADER_COLUMN_FAMILY: &str = "block_header";
const BLOCK_HASH_INDEX_COLUMN_FAMILY: &str = "block_hash_index";
const BLOCK_REPLAY_COLUMN_FAMILY: &str = "block_replay";
const COMPACT_BLOCK_COLUMN_FAMILY: &str = "compact_block";
const TRANSACTION_LOCATION_COLUMN_FAMILY: &str = "transaction_location";
const TRANSACTION_BLOB_COLUMN_FAMILY: &str = "transaction_blob";
const BLOCK_BLOB_COLUMN_FAMILY: &str = "block_blob";

#[derive(Clone, Copy, Debug)]
pub(super) struct PersistedFamilyEvidence {
    pub(super) row_count: u64,
    pub(super) prepared_logical_bytes: u64,
    pub(super) sst_file_bytes: u64,
    pub(super) sst_file_count: u64,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct PersistedCanonicalEvidence {
    pub(super) block_header: PersistedFamilyEvidence,
    pub(super) block_hash_index: PersistedFamilyEvidence,
    pub(super) block_replay: PersistedFamilyEvidence,
    pub(super) compact_block: PersistedFamilyEvidence,
    pub(super) transaction_location: PersistedFamilyEvidence,
    pub(super) transaction_blob: PersistedFamilyEvidence,
    pub(super) block_blob: PersistedFamilyEvidence,
    pub(super) readback_milliseconds: u64,
}

pub(super) fn validate_persisted_wallet_families(
    store_path: &Path,
    expected: &CanonicalBlockLoadEvidence,
) -> Result<PersistedCanonicalEvidence> {
    let readback_started_at = Instant::now();
    let column_families = DB::list_cf(&Options::default(), store_path)?;
    let database =
        DB::open_cf_for_read_only(&Options::default(), store_path, &column_families, false)?;
    let live_files = database.live_files()?;
    let mut persisted = inspect_wallet_families(&database, &live_files, expected)?;

    assert_eq!(
        persisted.block_header.row_count,
        expected.block_header_count
    );
    assert_eq!(
        persisted.block_hash_index.row_count,
        expected.block_hash_index_count
    );
    assert_eq!(
        persisted.block_replay.row_count,
        expected.block_replay_count
    );
    assert_eq!(
        persisted.compact_block.row_count,
        expected.compact_block_count
    );
    assert_eq!(
        persisted.transaction_location.row_count,
        expected.transaction_location_count
    );
    assert_eq!(
        persisted.transaction_blob.row_count,
        expected.transaction_blob_count
    );
    assert_eq!(persisted.block_blob.row_count, expected.block_blob_count);
    for height in sample_heights(expected) {
        validate_sampled_block_families(&database, height, expected)?;
    }
    persisted.readback_milliseconds = u64::try_from(readback_started_at.elapsed().as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    Ok(persisted)
}

fn inspect_wallet_families(
    database: &DB,
    live_files: &[LiveFile],
    expected: &CanonicalBlockLoadEvidence,
) -> Result<PersistedCanonicalEvidence> {
    Ok(PersistedCanonicalEvidence {
        block_header: inspect_family(
            database,
            live_files,
            BLOCK_HEADER_COLUMN_FAMILY,
            expected.block_header_logical_bytes,
            4,
        )?,
        block_hash_index: inspect_family(
            database,
            live_files,
            BLOCK_HASH_INDEX_COLUMN_FAMILY,
            expected.block_hash_index_logical_bytes,
            32,
        )?,
        block_replay: inspect_family(
            database,
            live_files,
            BLOCK_REPLAY_COLUMN_FAMILY,
            expected.block_replay_logical_bytes,
            4,
        )?,
        compact_block: inspect_family(
            database,
            live_files,
            COMPACT_BLOCK_COLUMN_FAMILY,
            expected.compact_block_logical_bytes,
            4,
        )?,
        transaction_location: inspect_family(
            database,
            live_files,
            TRANSACTION_LOCATION_COLUMN_FAMILY,
            expected.transaction_location_logical_bytes,
            32,
        )?,
        transaction_blob: inspect_family(
            database,
            live_files,
            TRANSACTION_BLOB_COLUMN_FAMILY,
            expected.transaction_blob_logical_bytes,
            8,
        )?,
        block_blob: inspect_family(
            database,
            live_files,
            BLOCK_BLOB_COLUMN_FAMILY,
            expected.block_blob_logical_bytes,
            4,
        )?,
        readback_milliseconds: 0,
    })
}

fn inspect_family(
    database: &DB,
    live_files: &[LiveFile],
    family_name: &'static str,
    prepared_logical_bytes: u64,
    expected_key_length: usize,
) -> Result<PersistedFamilyEvidence> {
    database
        .cf_handle(family_name)
        .ok_or_else(|| eyre!("fresh canonical store is missing {family_name}"))?;
    let mut family_files = live_files
        .iter()
        .filter(|file| file.column_family_name == family_name)
        .collect::<Vec<_>>();
    family_files.sort_unstable_by(|left, right| left.start_key.cmp(&right.start_key));
    let mut row_count = 0_u64;
    let mut sst_file_bytes = 0_u64;
    let mut sst_file_count = 0_u64;
    let mut previous_end_key: Option<&[u8]> = None;
    for file in family_files {
        if file.num_entries == 0 {
            return Err(eyre!(
                "fresh {family_name} SST {} contains no entries",
                file.name
            ));
        }
        if file.num_deletions != 0 {
            return Err(eyre!(
                "fresh {family_name} SST {} contains {} deletions",
                file.name,
                file.num_deletions
            ));
        }
        let start_key = file
            .start_key
            .as_deref()
            .ok_or_else(|| eyre!("fresh {family_name} SST {} has no start key", file.name))?;
        let end_key = file
            .end_key
            .as_deref()
            .ok_or_else(|| eyre!("fresh {family_name} SST {} has no end key", file.name))?;
        if start_key.len() != expected_key_length || end_key.len() != expected_key_length {
            return Err(eyre!(
                "fresh {family_name} SST {} has a non-version-1 key width",
                file.name
            ));
        }
        if start_key > end_key
            || previous_end_key.is_some_and(|previous_end| previous_end >= start_key)
        {
            return Err(eyre!(
                "fresh {family_name} SST {} has an invalid or overlapping key range",
                file.name
            ));
        }
        let _ = read_uncached(database, family_name, start_key)?;
        if start_key != end_key {
            let _ = read_uncached(database, family_name, end_key)?;
        }
        previous_end_key = Some(end_key);
        row_count = row_count
            .checked_add(file.num_entries)
            .ok_or_else(|| eyre!("{family_name} row count exceeds u64::MAX"))?;
        sst_file_count = sst_file_count
            .checked_add(1)
            .ok_or_else(|| eyre!("{family_name} SST count exceeds u64::MAX"))?;
        let file_bytes = u64::try_from(file.size)
            .map_err(|_| eyre!("{family_name} SST bytes exceed u64::MAX"))?;
        sst_file_bytes = sst_file_bytes
            .checked_add(file_bytes)
            .ok_or_else(|| eyre!("{family_name} SST bytes exceed u64::MAX"))?;
    }
    Ok(PersistedFamilyEvidence {
        row_count,
        prepared_logical_bytes,
        sst_file_bytes,
        sst_file_count,
    })
}

fn sample_heights(expected: &CanonicalBlockLoadEvidence) -> Vec<BlockHeight> {
    let midpoint = BlockHeight::new(
        expected
            .first_height
            .value()
            .saturating_add(u32::try_from(expected.block_count / 2).unwrap_or(u32::MAX)),
    );
    let mut heights = vec![expected.first_height, midpoint, expected.tip_height];
    heights.sort_unstable();
    heights.dedup();
    heights
}

fn validate_sampled_block_families(
    database: &DB,
    height: BlockHeight,
    expected: &CanonicalBlockLoadEvidence,
) -> Result<()> {
    let height_key = encode_height_key_ascending(height);
    let replay_bytes = read_uncached(database, BLOCK_REPLAY_COLUMN_FAMILY, &height_key)?;
    let replay = decode_canonical_block_replay(&replay_bytes)?;
    let facts = replay.facts();
    let header = &facts.block_header;
    assert_eq!(header.height, height);
    if height == expected.first_height {
        assert_eq!(header.block_hash, expected.first_hash);
        assert_eq!(header.parent_hash, expected.first_parent_hash);
    }
    if height == expected.tip_height {
        assert_eq!(header.block_hash, expected.tip_hash);
    }

    let persisted_header = read_uncached(database, BLOCK_HEADER_COLUMN_FAMILY, &height_key)?;
    assert_eq!(persisted_header, encode_block_header_v1(header));
    let persisted_height = read_uncached(
        database,
        BLOCK_HASH_INDEX_COLUMN_FAMILY,
        &header.block_hash.as_bytes(),
    )?;
    assert_eq!(persisted_height, height_key);

    let compact_bytes = read_uncached(database, COMPACT_BLOCK_COLUMN_FAMILY, &height_key)?;
    assert_ne!(compact_bytes, [] as [u8; 0]);

    let last_transaction_index = facts.transactions.len().saturating_sub(1);
    for transaction_index in [0, last_transaction_index] {
        let transaction = facts
            .transactions
            .get(transaction_index)
            .ok_or_else(|| eyre!("sampled block {} has no transactions", height.value()))?;
        let transaction_index = u32::try_from(transaction_index)?;
        let transaction_id = transaction.public_facts.transaction_id;
        let persisted_location = read_uncached(
            database,
            TRANSACTION_LOCATION_COLUMN_FAMILY,
            &transaction_id.as_bytes(),
        )?;
        assert_eq!(
            persisted_location,
            encode_transaction_location_v1(
                height,
                &header.block_hash.as_bytes(),
                transaction_index,
            )
        );
        let transaction_key = encode_transaction_position_v1(height, transaction_index);
        let transaction_bytes =
            read_uncached(database, TRANSACTION_BLOB_COLUMN_FAMILY, &transaction_key)?;
        assert_eq!(
            SerializedBytesDigest::from_serialized_bytes(&transaction_bytes),
            transaction.serialized_bytes_digest
        );
    }
    Ok(())
}

fn read_uncached(database: &DB, family_name: &'static str, key: &[u8]) -> Result<Vec<u8>> {
    let column_family = database
        .cf_handle(family_name)
        .ok_or_else(|| eyre!("fresh canonical store is missing {family_name}"))?;
    let mut read_options = ReadOptions::default();
    read_options.fill_cache(false);
    read_options.set_verify_checksums(true);
    database
        .get_cf_opt(&column_family, key, &read_options)?
        .ok_or_else(|| eyre!("{family_name} is missing sampled key {}", hex::encode(key)))
}

fn encode_block_header_v1(header: &BlockHeaderArtifact) -> [u8; 184] {
    let mut encoded = [0_u8; 184];
    encoded[..32].copy_from_slice(&header.block_hash.as_bytes());
    encoded[32..64].copy_from_slice(&header.parent_hash.as_bytes());
    encoded[64..96].copy_from_slice(&header.merkle_root_hash);
    encoded[96..128].copy_from_slice(&header.commitment_bytes);
    encoded[128..136].copy_from_slice(&header.block_time.to_le_bytes());
    encoded[136..140].copy_from_slice(&header.bits.to_le_bytes());
    encoded[140..172].copy_from_slice(&header.nonce);
    encoded[172..176].copy_from_slice(&header.version.to_le_bytes());
    encoded[176..184].copy_from_slice(&header.block_size_bytes.to_le_bytes());
    encoded
}

fn encode_transaction_location_v1(
    height: BlockHeight,
    block_hash: &[u8; 32],
    transaction_index: u32,
) -> [u8; 40] {
    let mut encoded = [0_u8; 40];
    encoded[..4].copy_from_slice(&encode_height_key_ascending(height));
    encoded[4..36].copy_from_slice(block_hash);
    encoded[36..].copy_from_slice(&transaction_index.to_be_bytes());
    encoded
}

fn encode_transaction_position_v1(height: BlockHeight, transaction_index: u32) -> [u8; 8] {
    let mut encoded = [0_u8; 8];
    encoded[..4].copy_from_slice(&encode_height_key_ascending(height));
    encoded[4..].copy_from_slice(&transaction_index.to_be_bytes());
    encoded
}
