use std::{fs, path::Path, time::Instant};

use eyre::{Result, eyre};
use rust_rocksdb::{DB, LiveFile, Options, ReadOptions};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use zinder_core::{
    BlockHeight,
    wire::{decode_internal_block_hash, encode_height_key_ascending},
};
use zinder_store::{CanonicalBlockLoadEvidence, SEGMENTS_MANIFEST_FILE_NAME};

const BLOCK_HEADER_COLUMN_FAMILY: &str = "block_header";
const BLOCK_HASH_INDEX_COLUMN_FAMILY: &str = "block_hash_index";
const SETTLED_SEGMENT_INDEX_COLUMN_FAMILY: &str = "settled_segment_index";
const LIVE_TAIL_BLOCK_COLUMN_FAMILY: &str = "live_tail_block";
const TRANSACTION_LOCATION_COLUMN_FAMILY: &str = "transaction_location";

const BLOCK_HEADER_VALUE_LEN: usize = 184;
const LIVE_TAIL_MAGIC: &[u8; 8] = b"ZSLVTL01";
const LIVE_TAIL_FORMAT_VERSION_V1: u16 = 1;
const LIVE_TAIL_HEADER_MIN_LEN: usize = 123;
const LIVE_TAIL_TRAILER_LEN: usize = 32;

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
    pub(super) settled_segment_index: PersistedFamilyEvidence,
    pub(super) live_tail_block: PersistedFamilyEvidence,
    pub(super) transaction_location: PersistedFamilyEvidence,
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
        persisted.settled_segment_index.row_count,
        expected.settled_segment_index_count
    );
    assert_eq!(
        persisted.live_tail_block.row_count,
        expected.live_tail_block_count
    );
    assert_eq!(
        persisted.transaction_location.row_count,
        expected.transaction_location_count
    );
    let manifest_json = load_segments_manifest_if_needed(store_path, expected)?;
    for height in sample_heights(expected) {
        validate_sampled_block_families(
            store_path,
            &database,
            height,
            expected,
            manifest_json.as_ref(),
        )?;
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
        settled_segment_index: inspect_family(
            database,
            live_files,
            SETTLED_SEGMENT_INDEX_COLUMN_FAMILY,
            expected.settled_segment_index_logical_bytes,
            4,
        )?,
        live_tail_block: inspect_family(
            database,
            live_files,
            LIVE_TAIL_BLOCK_COLUMN_FAMILY,
            expected.live_tail_block_logical_bytes,
            4,
        )?,
        transaction_location: inspect_family(
            database,
            live_files,
            TRANSACTION_LOCATION_COLUMN_FAMILY,
            expected.transaction_location_logical_bytes,
            32,
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

fn load_segments_manifest_if_needed(
    store_path: &Path,
    expected: &CanonicalBlockLoadEvidence,
) -> Result<Option<Value>> {
    let manifest_path = store_path.join(SEGMENTS_MANIFEST_FILE_NAME);
    if !manifest_path.exists() {
        if expected.settled_segment_index_count > 0 {
            return Err(eyre!(
                "missing {SEGMENTS_MANIFEST_FILE_NAME} with {} settled segments",
                expected.settled_segment_index_count
            ));
        }
        return Ok(None);
    }
    let manifest_bytes = fs::read(&manifest_path)?;
    let parsed: Value = serde_json::from_slice(&manifest_bytes)?;
    assert_eq!(
        parsed.get("schema_version").and_then(Value::as_u64),
        Some(2)
    );
    Ok(Some(parsed))
}

fn validate_sampled_block_families(
    store_path: &Path,
    database: &DB,
    height: BlockHeight,
    expected: &CanonicalBlockLoadEvidence,
    manifest_json: Option<&Value>,
) -> Result<()> {
    let height_key = encode_height_key_ascending(height);
    let persisted_header = read_uncached(database, BLOCK_HEADER_COLUMN_FAMILY, &height_key)?;
    if persisted_header.len() != BLOCK_HEADER_VALUE_LEN {
        return Err(eyre!(
            "sampled block {} header has unexpected byte length {}",
            height.value(),
            persisted_header.len()
        ));
    }
    let block_hash_wire = persisted_header
        .get(..32)
        .ok_or_else(|| eyre!("sampled block {} header is truncated", height.value()))?;
    let parent_hash_wire = persisted_header
        .get(32..64)
        .ok_or_else(|| eyre!("sampled block {} header is truncated", height.value()))?;
    let block_hash = decode_internal_block_hash(block_hash_wire)?;
    let parent_hash = decode_internal_block_hash(parent_hash_wire)?;
    if height == expected.first_height {
        assert_eq!(block_hash, expected.first_hash);
        assert_eq!(parent_hash, expected.first_parent_hash);
    }
    if height == expected.tip_height {
        assert_eq!(block_hash, expected.tip_hash);
    }

    let persisted_height =
        read_uncached(database, BLOCK_HASH_INDEX_COLUMN_FAMILY, block_hash_wire)?;
    assert_eq!(persisted_height, height_key);

    if let Some(live_tail_bytes) =
        read_optional_uncached(database, LIVE_TAIL_BLOCK_COLUMN_FAMILY, &height_key)?
    {
        validate_sampled_live_tail_bytes(&live_tail_bytes, height, block_hash_wire)?;
    } else {
        validate_sampled_settled_segment(store_path, database, height, manifest_json)?;
    }
    Ok(())
}

fn validate_sampled_live_tail_bytes(
    live_tail_bytes: &[u8],
    height: BlockHeight,
    expected_block_hash_wire: &[u8],
) -> Result<()> {
    let min_len = LIVE_TAIL_HEADER_MIN_LEN + LIVE_TAIL_TRAILER_LEN;
    if live_tail_bytes.len() < min_len {
        return Err(eyre!(
            "sampled live-tail row at {} is truncated ({} bytes)",
            height.value(),
            live_tail_bytes.len()
        ));
    }
    let magic = live_tail_bytes
        .get(..8)
        .ok_or_else(|| eyre!("missing live-tail magic"))?;
    assert_eq!(magic, LIVE_TAIL_MAGIC);
    let version_bytes: [u8; 2] = live_tail_bytes
        .get(8..10)
        .and_then(|slice| slice.try_into().ok())
        .ok_or_else(|| eyre!("missing live-tail version"))?;
    assert_eq!(
        u16::from_be_bytes(version_bytes),
        LIVE_TAIL_FORMAT_VERSION_V1
    );
    let row_height_bytes: [u8; 4] = live_tail_bytes
        .get(12..16)
        .and_then(|slice| slice.try_into().ok())
        .ok_or_else(|| eyre!("missing live-tail height"))?;
    assert_eq!(u32::from_be_bytes(row_height_bytes), height.value());
    let row_hash_wire = live_tail_bytes
        .get(16..48)
        .ok_or_else(|| eyre!("missing live-tail hash"))?;
    assert_eq!(row_hash_wire, expected_block_hash_wire);
    let split = live_tail_bytes
        .len()
        .checked_sub(LIVE_TAIL_TRAILER_LEN)
        .ok_or_else(|| eyre!("live-tail row missing SHA-256 trailer"))?;
    let body = live_tail_bytes
        .get(..split)
        .ok_or_else(|| eyre!("live-tail body slice out of bounds"))?;
    let trailer = live_tail_bytes
        .get(split..)
        .ok_or_else(|| eyre!("live-tail trailer slice out of bounds"))?;
    let computed: [u8; 32] = Sha256::digest(body).into();
    assert_eq!(computed.as_slice(), trailer);
    Ok(())
}

fn validate_sampled_settled_segment(
    store_path: &Path,
    database: &DB,
    height: BlockHeight,
    manifest_json: Option<&Value>,
) -> Result<()> {
    let manifest_root = manifest_json.ok_or_else(|| {
        eyre!(
            "sampled block {} is absent from live_tail_block without {SEGMENTS_MANIFEST_FILE_NAME}",
            height.value()
        )
    })?;
    let entries = manifest_root
        .get("entries")
        .and_then(Value::as_array)
        .ok_or_else(|| eyre!("{SEGMENTS_MANIFEST_FILE_NAME} has no entries array"))?;
    let covering = entries
        .iter()
        .find(|entry_json| {
            let start = entry_json.get("start_height").and_then(Value::as_u64);
            let end = entry_json
                .get("end_block")
                .and_then(|block| block.get("height"))
                .and_then(Value::as_u64);
            matches!(
                (start, end),
                (Some(low), Some(high))
                    if u64::from(height.value()) >= low && u64::from(height.value()) <= high
            )
        })
        .ok_or_else(|| {
            eyre!(
                "sampled block {} is covered by neither live_tail_block nor settled segments",
                height.value()
            )
        })?;
    let start_height = covering
        .get("start_height")
        .and_then(Value::as_u64)
        .and_then(|num| u32::try_from(num).ok())
        .ok_or_else(|| eyre!("segment entry missing start_height"))?;
    let end_height = covering
        .get("end_block")
        .and_then(|block| block.get("height"))
        .and_then(Value::as_u64)
        .and_then(|num| u32::try_from(num).ok())
        .ok_or_else(|| eyre!("segment entry missing end_block.height"))?;
    let start_key = encode_height_key_ascending(BlockHeight::new(start_height));
    let index_row = read_uncached(database, SETTLED_SEGMENT_INDEX_COLUMN_FAMILY, &start_key)?;
    assert_ne!(index_row, [] as [u8; 0]);
    let shielded_file = store_path
        .join("segments")
        .join("shielded")
        .join(format!("{start_height:010}-{end_height:010}.zshrng02"));
    if !shielded_file.is_file() {
        return Err(eyre!(
            "missing settled shielded segment {}",
            shielded_file.display()
        ));
    }
    Ok(())
}

fn read_optional_uncached(
    database: &DB,
    family_name: &'static str,
    key: &[u8],
) -> Result<Option<Vec<u8>>> {
    let column_family = database
        .cf_handle(family_name)
        .ok_or_else(|| eyre!("fresh canonical store is missing {family_name}"))?;
    let mut read_options = ReadOptions::default();
    read_options.fill_cache(false);
    read_options.set_verify_checksums(true);
    Ok(database.get_cf_opt(&column_family, key, &read_options)?)
}

fn read_uncached(database: &DB, family_name: &'static str, key: &[u8]) -> Result<Vec<u8>> {
    read_optional_uncached(database, family_name, key)?
        .ok_or_else(|| eyre!("{family_name} is missing sampled key {}", hex::encode(key)))
}
