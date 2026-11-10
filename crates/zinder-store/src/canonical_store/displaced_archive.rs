//! Version-1 displaced canonical-fact archive reads.

use std::{collections::BTreeMap, num::NonZeroU32};

use rust_rocksdb::{ReadOptions, WriteBatch};
use zinder_core::{
    BlockFinalNoteCommitmentRoots, BlockHash, BlockHeight, BlockHeightRange, BlockId,
    CanonicalBlockFacts, CanonicalBlockFactsDigestVersion, CanonicalBlockReplayFormatVersion,
    ChainEpochId, DisplacedBlock, DisplacedBlockArchiveCoverage, DisplacedBlockCoinbaseOutput,
    FinalNoteCommitmentRoot, SerializedBytesDigest, UnixTimestampMillis,
    decode_canonical_block_replay, encode_canonical_block_replay,
};

use super::{
    CanonicalStoreError,
    publication::column_family,
    rocksdb::{DISPLACED_BLOCK_FACTS_COLUMN_FAMILY, RocksDbCanonicalStore},
};

const VERSION_ONE: u8 = 1;
const STATE_KEY: [u8; 1] = [0x00];
const ORDER_KEY_TAG: u8 = 0x01;
const HASH_POINTER_KEY_TAG: u8 = 0x02;
const EVENT_CONTEXT_KEY_TAG: u8 = 0x03;
const ORDER_KEY_LENGTH: usize = 45;
const HASH_POINTER_KEY_LENGTH: usize = 33;
const HASH_POINTER_VALUE_LENGTH: usize = 13;
const STATE_VALUE_LENGTH: usize = 41;
const EVENT_CONTEXT_KEY_LENGTH: usize = 9;
const EVENT_CONTEXT_VALUE_LENGTH: usize = 37;
const MAX_PUBLIC_PAGE_LIMIT: u32 = 4_096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ArchiveState {
    coverage: DisplacedBlockArchiveCoverage,
    block_count: u64,
    latest_event_sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ArchivePosition {
    event_sequence: u64,
    height: BlockHeight,
    block_hash: BlockHash,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ArchiveEventContext {
    event_sequence: u64,
    reverted_range: BlockHeightRange,
    displacement_epoch: ChainEpochId,
    displaced_at: UnixTimestampMillis,
    row_count: u32,
    cumulative_block_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ArchiveRecord {
    replay_bytes: Vec<u8>,
    displacement_epoch: ChainEpochId,
    displaced_at: UnixTimestampMillis,
    raw_block_bytes: Option<Vec<u8>>,
    final_note_commitment_roots: Option<BlockFinalNoteCommitmentRoots>,
}

pub(super) struct CanonicalDisplacedBlock {
    pub(super) block_id: BlockId,
    pub(super) replay_bytes: Vec<u8>,
    pub(super) raw_block_bytes: Option<Vec<u8>>,
    pub(super) final_note_commitment_roots: Option<BlockFinalNoteCommitmentRoots>,
}

pub(super) struct PreparedDisplacedArchiveWrite {
    rows: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Clone, Copy)]
pub(super) struct DisplacedArchiveEvent {
    pub(super) event_sequence: u64,
    pub(super) displacement_epoch: ChainEpochId,
    pub(super) reverted_range: BlockHeightRange,
    pub(super) displaced_at: UnixTimestampMillis,
}

impl PreparedDisplacedArchiveWrite {
    #[expect(
        clippy::too_many_lines,
        reason = "one bounded preparation pass encodes the event context, displaced rows, newest pointers, and state transition together"
    )]
    pub(super) fn new(
        store: &RocksDbCanonicalStore,
        event: DisplacedArchiveEvent,
        displaced_blocks: Vec<CanonicalDisplacedBlock>,
    ) -> Result<Self, CanonicalStoreError> {
        let DisplacedArchiveEvent {
            event_sequence,
            displacement_epoch,
            reverted_range,
            displaced_at,
        } = event;
        if event_sequence == 0 || displacement_epoch.value() != event_sequence {
            return Err(CanonicalStoreError::displaced_archive(
                "archive write sequence and epoch must be matching nonzero values",
            ));
        }
        let expected_count = checked_reorg_range_count(store, reverted_range)?;
        if displaced_blocks.len() != expected_count {
            return Err(CanonicalStoreError::displaced_archive(
                "archive write does not exactly cover the reverted range",
            ));
        }
        let context_key = encode_event_context_key(event_sequence);
        if read_exact_row(
            store,
            &context_key,
            "displaced archive event collision read",
        )?
        .is_some()
        {
            return Err(CanonicalStoreError::displaced_archive(
                "archive event sequence already exists",
            ));
        }

        let prior_state = read_archive_state(store)?;
        if prior_state.is_some_and(|state| event_sequence <= state.latest_event_sequence) {
            return Err(CanonicalStoreError::displaced_archive(
                "archive event sequence does not advance its latest event",
            ));
        }
        let row_count = u32::try_from(expected_count).map_err(|_| {
            CanonicalStoreError::displaced_archive("archive event row count exceeds u32")
        })?;
        let displaced_count = u64::try_from(expected_count).map_err(|_| {
            CanonicalStoreError::displaced_archive("archive event block count exceeds u64")
        })?;
        let state = match prior_state {
            Some(state) => ArchiveState {
                coverage: state.coverage,
                block_count: state
                    .block_count
                    .checked_add(displaced_count)
                    .ok_or_else(|| {
                        CanonicalStoreError::displaced_archive("archive block count exceeds u64")
                    })?,
                latest_event_sequence: event_sequence,
            },
            None => ArchiveState {
                coverage: DisplacedBlockArchiveCoverage {
                    activation_event_sequence: event_sequence,
                    activation_epoch: displacement_epoch,
                    activated_at: displaced_at,
                },
                block_count: displaced_count,
                latest_event_sequence: event_sequence,
            },
        };
        let context = ArchiveEventContext {
            event_sequence,
            reverted_range,
            displacement_epoch,
            displaced_at,
            row_count,
            cumulative_block_count: state.block_count,
        };
        let mut rows = Vec::with_capacity(expected_count.saturating_mul(2).saturating_add(2));
        rows.push((context_key.to_vec(), encode_event_context(context).to_vec()));
        for (expected_height, displaced) in reverted_range.into_iter().zip(displaced_blocks) {
            if displaced.block_id.height != expected_height {
                return Err(CanonicalStoreError::displaced_archive(
                    "archive write blocks are not in exact reverted-height order",
                ));
            }
            validate_displaced_write_block(&displaced)?;
            let position = ArchivePosition {
                event_sequence,
                height: expected_height,
                block_hash: displaced.block_id.hash,
            };
            let record = ArchiveRecord {
                replay_bytes: displaced.replay_bytes,
                displacement_epoch,
                displaced_at,
                raw_block_bytes: displaced.raw_block_bytes,
                final_note_commitment_roots: displaced.final_note_commitment_roots,
            };
            rows.push((
                encode_order_key(position).to_vec(),
                encode_archive_record(&record)?,
            ));
            rows.push((
                encode_hash_pointer_key(position.block_hash).to_vec(),
                encode_hash_pointer(position).to_vec(),
            ));
        }
        rows.push((STATE_KEY.to_vec(), encode_archive_state(state).to_vec()));
        Ok(Self { rows })
    }

    pub(super) fn put_into(
        &self,
        store: &RocksDbCanonicalStore,
        batch: &mut WriteBatch,
    ) -> Result<(), CanonicalStoreError> {
        let family = archive_family(store)?;
        for (key, encoded) in &self.rows {
            batch.put_cf(&family, key, encoded);
        }
        Ok(())
    }

    pub(super) fn validate_readback(
        &self,
        store: &RocksDbCanonicalStore,
    ) -> Result<(), CanonicalStoreError> {
        for (key, expected) in &self.rows {
            let observed = read_exact_row(store, key, "displaced archive write readback")?;
            if observed.as_deref() != Some(expected.as_slice()) {
                return Err(CanonicalStoreError::displaced_archive(
                    "archive row differs after atomic replacement write",
                ));
            }
        }
        Ok(())
    }
}

fn validate_displaced_write_block(
    displaced: &CanonicalDisplacedBlock,
) -> Result<(), CanonicalStoreError> {
    let replay = decode_canonical_block_replay(&displaced.replay_bytes).map_err(|source| {
        CanonicalStoreError::displaced_archive(format!(
            "displaced canonical replay is invalid: {source}"
        ))
    })?;
    let facts = replay.facts();
    let canonical = encode_canonical_block_replay(
        facts,
        CanonicalBlockReplayFormatVersion::V1,
        CanonicalBlockFactsDigestVersion::V1,
    );
    if canonical.as_bytes() != displaced.replay_bytes
        || facts.block_header.height != displaced.block_id.height
        || facts.block_header.block_hash != displaced.block_id.hash
    {
        return Err(CanonicalStoreError::displaced_archive(
            "displaced replay is not the exact canonical block identity",
        ));
    }
    let Some(coinbase) = facts.transactions.first() else {
        return Err(CanonicalStoreError::displaced_archive(
            "displaced canonical replay has no coinbase transaction",
        ));
    };
    if !coinbase.public_facts.is_coinbase
        || facts
            .transactions
            .iter()
            .skip(1)
            .any(|transaction| transaction.public_facts.is_coinbase)
    {
        return Err(CanonicalStoreError::displaced_archive(
            "displaced canonical replay must have tx[0] as its unique coinbase",
        ));
    }
    if displaced.raw_block_bytes.as_ref().is_some_and(|raw| {
        SerializedBytesDigest::from_serialized_bytes(raw) != facts.serialized_bytes_digest
    }) {
        return Err(CanonicalStoreError::displaced_archive(
            "displaced raw block bytes do not match the canonical replay",
        ));
    }
    if displaced.final_note_commitment_roots.is_some_and(|roots| {
        roots.height != displaced.block_id.height || roots.block_hash != displaced.block_id.hash
    }) {
        return Err(CanonicalStoreError::displaced_archive(
            "displaced final roots do not match the canonical replay identity",
        ));
    }
    Ok(())
}

/// Opaque stable position in newest-first displaced-block history.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DisplacedBlockCursor {
    event_sequence: u64,
    height: zinder_core::BlockHeight,
    block_hash: BlockHash,
}

impl DisplacedBlockCursor {
    /// Reconstructs a cursor from a native RPC position.
    #[must_use]
    pub const fn from_position(
        event_sequence: u64,
        height: zinder_core::BlockHeight,
        block_hash: BlockHash,
    ) -> Self {
        Self {
            event_sequence,
            height,
            block_hash,
        }
    }

    /// Returns the displacement event sequence represented by this cursor.
    #[must_use]
    pub const fn event_sequence(self) -> u64 {
        self.event_sequence
    }

    /// Returns the displaced block height represented by this cursor.
    #[must_use]
    pub const fn height(self) -> zinder_core::BlockHeight {
        self.height
    }

    /// Returns the displaced block hash represented by this cursor.
    #[must_use]
    pub const fn block_hash(self) -> BlockHash {
        self.block_hash
    }
}

/// Bounded newest-first displaced-block history page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DisplacedBlockPage {
    /// Archived blocks in descending `(event sequence, height)` order.
    pub blocks: Vec<DisplacedBlock>,
    /// Whether at least one older archive row remains.
    pub has_more: bool,
    /// Cursor for requesting rows strictly older than this page.
    pub next_cursor: Option<DisplacedBlockCursor>,
}

impl RocksDbCanonicalStore {
    /// Reads a bounded newest-first page strictly older than `after`.
    pub fn displaced_block_page(
        &self,
        after: Option<&DisplacedBlockCursor>,
        limit: NonZeroU32,
    ) -> Result<DisplacedBlockPage, CanonicalStoreError> {
        let max_blocks = checked_public_limit(limit)?;
        let Some(_) = read_archive_state(self)? else {
            if after.is_some() {
                return Err(CanonicalStoreError::displaced_archive(
                    "archive cursor cannot resolve in an empty archive",
                ));
            }
            return Ok(empty_archive_page());
        };

        let family = archive_family(self)?;
        let mut options = ReadOptions::default();
        options.fill_cache(false);
        let mut rows = self.bounded_open.db.raw_iterator_cf_opt(&family, options);
        let mut context_cache = None;
        if let Some(cursor) = after {
            let position = ArchivePosition {
                event_sequence: cursor.event_sequence(),
                height: cursor.height(),
                block_hash: cursor.block_hash(),
            };
            let key = encode_order_key(position);
            let encoded =
                read_exact_row(self, &key, "displaced archive cursor read")?.ok_or_else(|| {
                    CanonicalStoreError::displaced_archive(
                        "archive cursor does not resolve an exact order row",
                    )
                })?;
            let _ = decode_and_validate_record_cached(self, &key, &encoded, &mut context_cache)?;
            rows.seek_for_prev(key);
            if rows.valid() && rows.key() == Some(key.as_slice()) {
                rows.prev();
            }
        } else {
            rows.seek_for_prev([HASH_POINTER_KEY_TAG]);
        }

        let requested_with_lookahead = max_blocks.checked_add(1).ok_or_else(|| {
            CanonicalStoreError::displaced_archive("archive page lookahead overflowed")
        })?;
        let mut blocks = Vec::with_capacity(requested_with_lookahead);
        while rows.valid() && blocks.len() < requested_with_lookahead {
            let Some((key, encoded_record)) = rows.item() else {
                break;
            };
            if key.first() != Some(&ORDER_KEY_TAG) {
                break;
            }
            blocks.push(decode_and_validate_record_cached(
                self,
                key,
                encoded_record,
                &mut context_cache,
            )?);
            rows.prev();
        }
        rows.status()
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "displaced archive page iteration",
                source,
            })?;
        let has_more = blocks.len() > max_blocks;
        blocks.truncate(max_blocks);
        let next_cursor = has_more
            .then(|| {
                blocks.last().map(|block| {
                    DisplacedBlockCursor::from_position(
                        block.displacement_event_sequence,
                        block.header.height,
                        block.block_hash,
                    )
                })
            })
            .flatten();
        Ok(DisplacedBlockPage {
            blocks,
            has_more,
            next_cursor,
        })
    }

    /// Reads up to `limit` archived blocks in newest event/height order.
    pub fn newest_displaced_blocks(
        &self,
        limit: NonZeroU32,
    ) -> Result<Vec<DisplacedBlock>, CanonicalStoreError> {
        Ok(self.displaced_block_page(None, limit)?.blocks)
    }

    /// Reads the newest archived occurrence of one stable block hash.
    pub fn displaced_block_by_hash(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<DisplacedBlock>, CanonicalStoreError> {
        if read_archive_state(self)?.is_none() {
            return Ok(None);
        }
        let pointer_key = encode_hash_pointer_key(block_hash);
        let Some(pointer_bytes) =
            read_exact_row(self, &pointer_key, "displaced archive hash pointer read")?
        else {
            return Ok(None);
        };
        let position = decode_hash_pointer(&pointer_bytes, block_hash)?;
        let order_key = encode_order_key(position);
        let encoded = read_exact_row(self, &order_key, "displaced archive hash target read")?
            .ok_or_else(|| {
                CanonicalStoreError::displaced_archive("archive hash pointer target row is absent")
            })?;
        let mut context_cache = None;
        let block =
            decode_and_validate_record_cached(self, &order_key, &encoded, &mut context_cache)?;
        if block.block_hash != block_hash {
            return Err(CanonicalStoreError::displaced_archive(
                "archive hash pointer resolved a different block hash",
            ));
        }
        Ok(Some(block))
    }

    /// Reads a bounded newest-first prefix after validating the event's complete reorg range.
    pub fn displaced_blocks_for_event(
        &self,
        event_sequence: u64,
        limit: NonZeroU32,
    ) -> Result<Vec<DisplacedBlock>, CanonicalStoreError> {
        let max_blocks = checked_public_limit(limit)?;
        let Some(state) = read_archive_state(self)? else {
            return Ok(Vec::new());
        };
        if event_sequence < state.coverage.activation_event_sequence {
            return Ok(Vec::new());
        }
        let Some(context) = read_event_context_optional(self, event_sequence)? else {
            if event_has_order_row(self, event_sequence)? {
                return Err(CanonicalStoreError::displaced_archive(
                    "archive event order rows exist without event context",
                ));
            }
            return Ok(Vec::new());
        };
        let expected_count = validate_event_context(self, context)?;
        let family = archive_family(self)?;
        let mut options = ReadOptions::default();
        options.fill_cache(false);
        let mut rows = self.bounded_open.db.raw_iterator_cf_opt(&family, options);
        let last_position = ArchivePosition {
            event_sequence,
            height: BlockHeight::new(u32::MAX),
            block_hash: BlockHash::from_bytes([u8::MAX; 32]),
        };
        rows.seek_for_prev(encode_order_key(last_position));
        let mut blocks = Vec::with_capacity(expected_count);
        let mut heights = BTreeMap::new();
        while rows.valid() && blocks.len() <= expected_count {
            let Some((key, encoded_record)) = rows.item() else {
                break;
            };
            if key.first() != Some(&ORDER_KEY_TAG) {
                break;
            }
            let position = decode_order_key(key)?;
            if position.event_sequence != event_sequence {
                break;
            }
            let block = decode_and_validate_record_with_context(key, encoded_record, context)?;
            if heights
                .insert(position.height, position.block_hash)
                .is_some()
            {
                return Err(CanonicalStoreError::displaced_archive(
                    "archive event has duplicate rows at one displaced height",
                ));
            }
            blocks.push(block);
            rows.prev();
        }
        rows.status()
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "displaced archive event iteration",
                source,
            })?;
        let expected_heights = context.reverted_range.into_iter().collect::<Vec<_>>();
        if blocks.len() != expected_count
            || heights.keys().copied().collect::<Vec<_>>() != expected_heights
        {
            return Err(CanonicalStoreError::displaced_archive(
                "archive event rows do not exactly cover the canonical reverted range",
            ));
        }
        blocks.truncate(max_blocks);
        Ok(blocks)
    }

    /// Returns the persisted number of archived displaced blocks.
    pub fn displaced_block_count(&self) -> Result<u64, CanonicalStoreError> {
        Ok(read_archive_state(self)?.map_or(0, |state| state.block_count))
    }

    /// Returns the event from which replacement archive coverage is guaranteed.
    pub fn displaced_block_archive_coverage(
        &self,
    ) -> Result<Option<DisplacedBlockArchiveCoverage>, CanonicalStoreError> {
        Ok(read_archive_state(self)?.map(|state| state.coverage))
    }
}

/// Validates the permanent reorg archive without depending on prunable events.
///
/// Archive contexts are a durable, self-contained witness for each replacement.
/// Retained canonical events can therefore be compacted without weakening cold
/// admission of the historical displaced-block API.
#[expect(
    clippy::too_many_lines,
    reason = "one cold-admission pass must authenticate every archive context, row set, and terminal state before serving historical displaced blocks"
)]
pub(super) fn validate_permanent_reorg_archive(
    db: &rust_rocksdb::DB,
    reorg_window_blocks: u32,
    visible_event_sequence: u64,
) -> Result<(), CanonicalStoreError> {
    let family = db
        .cf_handle(DISPLACED_BLOCK_FACTS_COLUMN_FAMILY)
        .ok_or_else(|| CanonicalStoreError::displaced_archive("archive column family is absent"))?;
    let mut options = ReadOptions::default();
    options.fill_cache(false);
    let mut rows = db.raw_iterator_cf_opt(&family, options);
    rows.seek([EVENT_CONTEXT_KEY_TAG]);

    let mut first_context = None;
    let mut latest_context = None;
    let mut cumulative_block_count = 0_u64;
    while rows.valid() {
        let Some((key, encoded_context)) = rows.item() else {
            break;
        };
        if key.first() != Some(&EVENT_CONTEXT_KEY_TAG) {
            break;
        }
        if key.len() != EVENT_CONTEXT_KEY_LENGTH {
            return Err(CanonicalStoreError::displaced_archive(
                "archive event context key is not exact version-1 bytes",
            ));
        }
        let event_sequence = u64::from_be_bytes(read_array(key, 1)?);
        if event_sequence == 0 || event_sequence > visible_event_sequence {
            return Err(CanonicalStoreError::displaced_archive(
                "archive event context is outside canonical event history",
            ));
        }
        let context = decode_event_context(encoded_context, event_sequence)?;
        if context.displacement_epoch.value() != event_sequence {
            return Err(CanonicalStoreError::displaced_archive(
                "archive event context does not match its displacement epoch",
            ));
        }
        let expected_count =
            checked_reorg_range_count_with_window(context.reverted_range, reorg_window_blocks)?;
        if usize::try_from(context.row_count).ok() != Some(expected_count) {
            return Err(CanonicalStoreError::displaced_archive(
                "archive event context row count does not match its reverted range",
            ));
        }
        cumulative_block_count = cumulative_block_count
            .checked_add(u64::try_from(expected_count).map_err(|_| {
                CanonicalStoreError::displaced_archive("archive event count exceeds u64")
            })?)
            .ok_or_else(|| {
                CanonicalStoreError::displaced_archive("archive cumulative count exceeds u64")
            })?;
        if context.cumulative_block_count != cumulative_block_count {
            return Err(CanonicalStoreError::displaced_archive(
                "archive event context cumulative count is not contiguous",
            ));
        }
        validate_latest_event_rows(db, context, expected_count)?;
        first_context.get_or_insert(context);
        latest_context = Some(context);
        rows.next();
    }
    rows.status()
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "reorg archive context admission iteration",
            source,
        })?;

    let encoded_state = read_exact_db_row(db, &STATE_KEY, "reorg archive admission state read")?;
    let (Some(first_context), Some(latest_context)) = (first_context, latest_context) else {
        if encoded_state.is_some() || archive_has_any_row(db)? {
            return Err(CanonicalStoreError::displaced_archive(
                "archive rows exist without a version-1 archive event context",
            ));
        }
        return Ok(());
    };
    let state = encoded_state
        .as_deref()
        .ok_or_else(|| {
            CanonicalStoreError::displaced_archive(
                "archive event contexts have no version-1 archive state",
            )
        })
        .and_then(decode_archive_state)?;
    if state.coverage.activation_event_sequence != first_context.event_sequence
        || state.coverage.activation_epoch != first_context.displacement_epoch
        || state.coverage.activated_at != first_context.displaced_at
        || state.latest_event_sequence != latest_context.event_sequence
        || state.block_count != cumulative_block_count
    {
        return Err(CanonicalStoreError::displaced_archive(
            "archive state does not match its permanent event contexts",
        ));
    }
    Ok(())
}

fn validate_latest_event_rows(
    db: &rust_rocksdb::DB,
    context: ArchiveEventContext,
    expected_count: usize,
) -> Result<(), CanonicalStoreError> {
    let family = db
        .cf_handle(DISPLACED_BLOCK_FACTS_COLUMN_FAMILY)
        .ok_or_else(|| CanonicalStoreError::displaced_archive("archive column family is absent"))?;
    let mut options = ReadOptions::default();
    options.fill_cache(false);
    let mut rows = db.raw_iterator_cf_opt(&family, options);
    rows.seek_for_prev(encode_order_key(ArchivePosition {
        event_sequence: context.event_sequence,
        height: BlockHeight::new(u32::MAX),
        block_hash: BlockHash::from_bytes([u8::MAX; 32]),
    }));
    let mut observed = Vec::with_capacity(expected_count);
    while rows.valid() && observed.len() <= expected_count {
        let Some((key, encoded_record)) = rows.item() else {
            break;
        };
        if key.first() != Some(&ORDER_KEY_TAG) {
            break;
        }
        let position = decode_order_key(key)?;
        if position.event_sequence != context.event_sequence {
            break;
        }
        let block = decode_and_validate_record_with_context(key, encoded_record, context)?;
        validate_latest_hash_pointer(db, position)?;
        observed.push((position.height, block.block_hash));
        rows.prev();
    }
    rows.status()
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "reorg archive admission row iteration",
            source,
        })?;
    let mut expected_heights = context.reverted_range.into_iter().collect::<Vec<_>>();
    expected_heights.reverse();
    if observed.len() != expected_count
        || observed
            .iter()
            .map(|(height, _)| *height)
            .ne(expected_heights)
    {
        return Err(CanonicalStoreError::displaced_archive(
            "archive rows do not exactly cover their retained reorg event",
        ));
    }
    Ok(())
}

fn validate_latest_hash_pointer(
    db: &rust_rocksdb::DB,
    archived_position: ArchivePosition,
) -> Result<(), CanonicalStoreError> {
    let pointer_key = encode_hash_pointer_key(archived_position.block_hash);
    let encoded_pointer = read_exact_db_row(
        db,
        &pointer_key,
        "reorg archive required hash pointer admission read",
    )?
    .ok_or_else(|| {
        CanonicalStoreError::displaced_archive(
            "retained displaced block has no newest-hash pointer",
        )
    })?;
    let pointed_position = decode_hash_pointer(&encoded_pointer, archived_position.block_hash)?;
    if pointed_position.height != archived_position.height {
        return Err(CanonicalStoreError::displaced_archive(
            "retained reorg newest-hash pointer changes block height",
        ));
    }
    if pointed_position.event_sequence < archived_position.event_sequence {
        return Err(CanonicalStoreError::displaced_archive(
            "latest retained reorg newest-hash pointer is stale",
        ));
    }
    if read_exact_db_row(
        db,
        &encode_order_key(pointed_position),
        "reorg archive newest-hash pointer target admission read",
    )?
    .is_none()
    {
        return Err(CanonicalStoreError::displaced_archive(
            "retained reorg newest-hash pointer target row is absent",
        ));
    }
    Ok(())
}

fn read_exact_db_row(
    db: &rust_rocksdb::DB,
    key: &[u8],
    operation: &'static str,
) -> Result<Option<Vec<u8>>, CanonicalStoreError> {
    let family = db
        .cf_handle(DISPLACED_BLOCK_FACTS_COLUMN_FAMILY)
        .ok_or_else(|| CanonicalStoreError::displaced_archive("archive column family is absent"))?;
    let mut options = ReadOptions::default();
    options.fill_cache(false);
    db.get_cf_opt(&family, key, &options)
        .map_err(|source| CanonicalStoreError::RocksDbOperation { operation, source })
}

fn archive_has_any_row(db: &rust_rocksdb::DB) -> Result<bool, CanonicalStoreError> {
    let family = db
        .cf_handle(DISPLACED_BLOCK_FACTS_COLUMN_FAMILY)
        .ok_or_else(|| CanonicalStoreError::displaced_archive("archive column family is absent"))?;
    let mut options = ReadOptions::default();
    options.fill_cache(false);
    let mut rows = db.raw_iterator_cf_opt(&family, options);
    rows.seek_to_first();
    let has_row = rows.valid();
    rows.status()
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "reorg archive empty admission iteration",
            source,
        })?;
    Ok(has_row)
}

fn empty_archive_page() -> DisplacedBlockPage {
    DisplacedBlockPage {
        blocks: Vec::new(),
        has_more: false,
        next_cursor: None,
    }
}

fn checked_public_limit(limit: NonZeroU32) -> Result<usize, CanonicalStoreError> {
    if limit.get() > MAX_PUBLIC_PAGE_LIMIT {
        return Err(CanonicalStoreError::displaced_archive(format!(
            "archive read limit {} exceeds {MAX_PUBLIC_PAGE_LIMIT}",
            limit.get()
        )));
    }
    usize::try_from(limit.get())
        .map_err(|_| CanonicalStoreError::displaced_archive("archive read limit exceeds usize"))
}

fn checked_reorg_range_count(
    store: &RocksDbCanonicalStore,
    range: zinder_core::BlockHeightRange,
) -> Result<usize, CanonicalStoreError> {
    checked_reorg_range_count_with_window(range, store.reorg_policy().reorg_window_blocks())
}

fn checked_reorg_range_count_with_window(
    range: zinder_core::BlockHeightRange,
    reorg_window_blocks: u32,
) -> Result<usize, CanonicalStoreError> {
    let count = range
        .end
        .value()
        .checked_sub(range.start.value())
        .and_then(|difference| difference.checked_add(1))
        .ok_or_else(|| {
            CanonicalStoreError::displaced_archive("archive event reverted range is empty")
        })?;
    if count > reorg_window_blocks {
        return Err(CanonicalStoreError::displaced_archive(
            "archive event reverted range exceeds the admitted reorg window",
        ));
    }
    usize::try_from(count)
        .map_err(|_| CanonicalStoreError::displaced_archive("archive range exceeds usize"))
}

fn validate_event_context(
    store: &RocksDbCanonicalStore,
    context: ArchiveEventContext,
) -> Result<usize, CanonicalStoreError> {
    if context.event_sequence == 0
        || context.displacement_epoch.value() != context.event_sequence
        || context.row_count == 0
        || context.cumulative_block_count < u64::from(context.row_count)
    {
        return Err(CanonicalStoreError::displaced_archive(
            "archive event context sequence, epoch, and row count must be matching nonzero values",
        ));
    }
    let expected_count = checked_reorg_range_count(store, context.reverted_range)?;
    if usize::try_from(context.row_count).ok() != Some(expected_count) {
        return Err(CanonicalStoreError::displaced_archive(
            "archive event row count does not match its exact reverted range",
        ));
    }
    Ok(expected_count)
}

fn read_event_context(
    store: &RocksDbCanonicalStore,
    event_sequence: u64,
) -> Result<ArchiveEventContext, CanonicalStoreError> {
    read_event_context_optional(store, event_sequence)?.ok_or_else(|| {
        CanonicalStoreError::displaced_archive("archive event context row is absent")
    })
}

fn read_event_context_optional(
    store: &RocksDbCanonicalStore,
    event_sequence: u64,
) -> Result<Option<ArchiveEventContext>, CanonicalStoreError> {
    let key = encode_event_context_key(event_sequence);
    let Some(encoded) = read_exact_row(store, &key, "displaced archive event context read")? else {
        return Ok(None);
    };
    Ok(Some(decode_event_context(&encoded, event_sequence)?))
}

/// Resolves the block hash at `height` on the branch visible immediately after
/// `event_sequence` by consulting permanent displacement history.
///
/// The earliest later replacement covering the height displaced exactly the
/// block that was visible at the requested event. Later replacements may
/// displace that replacement again, so choosing the newest archive row would
/// reconstruct the wrong historical branch.
pub(super) fn first_displaced_block_hash_after_event(
    db: &rust_rocksdb::DB,
    event_sequence: u64,
    height: BlockHeight,
) -> Result<Option<BlockHash>, CanonicalStoreError> {
    let Some(first_later_sequence) = event_sequence.checked_add(1) else {
        return Ok(None);
    };
    let family = db
        .cf_handle(DISPLACED_BLOCK_FACTS_COLUMN_FAMILY)
        .ok_or_else(|| CanonicalStoreError::displaced_archive("archive column family is absent"))?;
    let mut options = ReadOptions::default();
    options.fill_cache(false);
    let mut contexts = db.raw_iterator_cf_opt(&family, options);
    contexts.seek(encode_event_context_key(first_later_sequence));
    while contexts.valid() {
        let Some((key, encoded)) = contexts.item() else {
            break;
        };
        if key.first() != Some(&EVENT_CONTEXT_KEY_TAG) {
            break;
        }
        if key.len() != EVENT_CONTEXT_KEY_LENGTH {
            return Err(CanonicalStoreError::displaced_archive(
                "archive event context key is not exact version-1 bytes",
            ));
        }
        let context_sequence = u64::from_be_bytes(read_array(key, 1)?);
        let context = decode_event_context(encoded, context_sequence)?;
        if context.reverted_range.start <= height && height <= context.reverted_range.end {
            let mut first_position = [0; ORDER_KEY_LENGTH];
            first_position[0] = ORDER_KEY_TAG;
            first_position[1..9].copy_from_slice(&context_sequence.to_be_bytes());
            first_position[9..13].copy_from_slice(&height.value().to_be_bytes());
            let mut rows = db.raw_iterator_cf_opt(&family, ReadOptions::default());
            rows.seek(first_position);
            let Some(key) = rows.key() else {
                return Err(CanonicalStoreError::displaced_archive(
                    "archive context has no row at a covered historical height",
                ));
            };
            let position = decode_order_key(key)?;
            if position.event_sequence != context_sequence || position.height != height {
                return Err(CanonicalStoreError::displaced_archive(
                    "archive context does not resolve its covered historical height",
                ));
            }
            return Ok(Some(position.block_hash));
        }
        contexts.next();
    }
    contexts
        .status()
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "historical displacement context iteration",
            source,
        })?;
    Ok(None)
}

fn event_has_order_row(
    store: &RocksDbCanonicalStore,
    event_sequence: u64,
) -> Result<bool, CanonicalStoreError> {
    let family = archive_family(store)?;
    let mut options = ReadOptions::default();
    options.fill_cache(false);
    let mut rows = store.bounded_open.db.raw_iterator_cf_opt(&family, options);
    rows.seek_for_prev(encode_order_key(ArchivePosition {
        event_sequence,
        height: BlockHeight::new(u32::MAX),
        block_hash: BlockHash::from_bytes([u8::MAX; 32]),
    }));
    let has_row = match rows.key() {
        Some(key) if key.first() == Some(&ORDER_KEY_TAG) => {
            decode_order_key(key)?.event_sequence == event_sequence
        }
        Some(_) | None => false,
    };
    rows.status()
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "displaced archive event existence read",
            source,
        })?;
    Ok(has_row)
}

fn encode_event_context_key(event_sequence: u64) -> [u8; EVENT_CONTEXT_KEY_LENGTH] {
    let mut encoded = [0; EVENT_CONTEXT_KEY_LENGTH];
    encoded[0] = EVENT_CONTEXT_KEY_TAG;
    encoded[1..].copy_from_slice(&event_sequence.to_be_bytes());
    encoded
}

fn encode_event_context(context: ArchiveEventContext) -> [u8; EVENT_CONTEXT_VALUE_LENGTH] {
    let mut encoded = [0; EVENT_CONTEXT_VALUE_LENGTH];
    encoded[0] = VERSION_ONE;
    encoded[1..5].copy_from_slice(&context.reverted_range.start.value().to_be_bytes());
    encoded[5..9].copy_from_slice(&context.reverted_range.end.value().to_be_bytes());
    encoded[9..17].copy_from_slice(&context.displacement_epoch.value().to_be_bytes());
    encoded[17..25].copy_from_slice(&context.displaced_at.value().to_be_bytes());
    encoded[25..29].copy_from_slice(&context.row_count.to_be_bytes());
    encoded[29..37].copy_from_slice(&context.cumulative_block_count.to_be_bytes());
    encoded
}

fn decode_event_context(
    encoded: &[u8],
    event_sequence: u64,
) -> Result<ArchiveEventContext, CanonicalStoreError> {
    if encoded.len() != EVENT_CONTEXT_VALUE_LENGTH || encoded.first() != Some(&VERSION_ONE) {
        return Err(CanonicalStoreError::displaced_archive(
            "archive event context is not exact version-1 bytes",
        ));
    }
    let context = ArchiveEventContext {
        event_sequence,
        reverted_range: BlockHeightRange::inclusive(
            BlockHeight::new(u32::from_be_bytes(read_array(encoded, 1)?)),
            BlockHeight::new(u32::from_be_bytes(read_array(encoded, 5)?)),
        ),
        displacement_epoch: ChainEpochId::new(u64::from_be_bytes(read_array(encoded, 9)?)),
        displaced_at: UnixTimestampMillis::new(u64::from_be_bytes(read_array(encoded, 17)?)),
        row_count: u32::from_be_bytes(read_array(encoded, 25)?),
        cumulative_block_count: u64::from_be_bytes(read_array(encoded, 29)?),
    };
    if encode_event_context(context).as_slice() != encoded {
        return Err(CanonicalStoreError::displaced_archive(
            "archive event context is not canonical",
        ));
    }
    Ok(context)
}

fn read_archive_state(
    store: &RocksDbCanonicalStore,
) -> Result<Option<ArchiveState>, CanonicalStoreError> {
    let Some(encoded) = read_exact_row(store, &STATE_KEY, "displaced archive state read")? else {
        let family = archive_family(store)?;
        let mut options = ReadOptions::default();
        options.fill_cache(false);
        let mut rows = store.bounded_open.db.raw_iterator_cf_opt(&family, options);
        rows.seek_to_first();
        if rows.valid() {
            return Err(CanonicalStoreError::displaced_archive(
                "archive rows exist without version-1 archive state",
            ));
        }
        rows.status()
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "displaced archive empty-state iteration",
                source,
            })?;
        return Ok(None);
    };
    let state = decode_archive_state(&encoded)?;
    if state.coverage.activation_event_sequence == 0
        || state.coverage.activation_epoch.value() != state.coverage.activation_event_sequence
        || state.block_count == 0
        || state.latest_event_sequence < state.coverage.activation_event_sequence
    {
        return Err(CanonicalStoreError::displaced_archive(
            "archive state activation sequence, epoch, and count must be matching nonzero values",
        ));
    }
    let context = read_event_context(store, state.coverage.activation_event_sequence)?;
    let activation_count = validate_event_context(store, context)?;
    if state.block_count < u64::try_from(activation_count).unwrap_or(u64::MAX) {
        return Err(CanonicalStoreError::displaced_archive(
            "archive state count is smaller than its activation event row count",
        ));
    }
    if state.coverage.activation_epoch != context.displacement_epoch
        || state.coverage.activated_at != context.displaced_at
    {
        return Err(CanonicalStoreError::displaced_archive(
            "archive coverage does not match its canonical activation event and epoch",
        ));
    }
    let latest_context = read_event_context(store, state.latest_event_sequence)?;
    let _ = validate_event_context(store, latest_context)?;
    if latest_context.cumulative_block_count != state.block_count {
        return Err(CanonicalStoreError::displaced_archive(
            "archive state count is not bound to its latest event context",
        ));
    }
    Ok(Some(state))
}

fn decode_and_validate_record_cached(
    store: &RocksDbCanonicalStore,
    key: &[u8],
    encoded: &[u8],
    context_cache: &mut Option<ArchiveEventContext>,
) -> Result<DisplacedBlock, CanonicalStoreError> {
    let position = decode_order_key(key)?;
    let context =
        if context_cache.is_some_and(|context| context.event_sequence == position.event_sequence) {
            context_cache.as_ref().copied().ok_or_else(|| {
                CanonicalStoreError::displaced_archive("archive event context cache is absent")
            })?
        } else {
            let context = read_event_context(store, position.event_sequence)?;
            let _ = validate_event_context(store, context)?;
            *context_cache = Some(context);
            context
        };
    decode_and_validate_record_with_context(key, encoded, context)
}

fn decode_and_validate_record_with_context(
    key: &[u8],
    encoded: &[u8],
    context: ArchiveEventContext,
) -> Result<DisplacedBlock, CanonicalStoreError> {
    let position = decode_order_key(key)?;
    if position.event_sequence != context.event_sequence {
        return Err(CanonicalStoreError::displaced_archive(
            "archive row event does not match its cached event context",
        ));
    }
    let record = decode_archive_record(encoded)?;
    let replay = decode_canonical_block_replay(&record.replay_bytes).map_err(|source| {
        CanonicalStoreError::displaced_archive(format!(
            "archive canonical replay is invalid: {source}"
        ))
    })?;
    let facts = replay.facts();
    let canonical = encode_canonical_block_replay(
        facts,
        CanonicalBlockReplayFormatVersion::V1,
        CanonicalBlockFactsDigestVersion::V1,
    );
    if canonical.as_bytes() != record.replay_bytes {
        return Err(CanonicalStoreError::displaced_archive(
            "archive replay is not the exact canonical encoding",
        ));
    }
    if facts.block_header.height != position.height
        || facts.block_header.block_hash != position.block_hash
    {
        return Err(CanonicalStoreError::displaced_archive(
            "archive order key height or hash does not match canonical replay",
        ));
    }
    if record.raw_block_bytes.as_ref().is_some_and(|raw| {
        SerializedBytesDigest::from_serialized_bytes(raw) != facts.serialized_bytes_digest
    }) {
        return Err(CanonicalStoreError::displaced_archive(
            "archive raw block bytes do not match the replay serialized-block digest",
        ));
    }
    if record.final_note_commitment_roots.is_some_and(|roots| {
        roots.height != position.height || roots.block_hash != position.block_hash
    }) {
        return Err(CanonicalStoreError::displaced_archive(
            "archive final roots height or hash does not match canonical replay",
        ));
    }
    if position.height < context.reverted_range.start
        || position.height > context.reverted_range.end
        || record.displacement_epoch != context.displacement_epoch
        || record.displaced_at != context.displaced_at
    {
        return Err(CanonicalStoreError::displaced_archive(
            "archive row does not match its canonical reorg event and epoch",
        ));
    }
    derive_displaced_block(position, record, facts)
}

fn derive_displaced_block(
    position: ArchivePosition,
    record: ArchiveRecord,
    facts: &CanonicalBlockFacts,
) -> Result<DisplacedBlock, CanonicalStoreError> {
    let transaction_ids = facts
        .transactions
        .iter()
        .map(|transaction| transaction.public_facts.transaction_id)
        .collect();
    let coinbase = facts.transactions.first().ok_or_else(|| {
        CanonicalStoreError::displaced_archive(
            "archive canonical replay has no first coinbase transaction",
        )
    })?;
    if !coinbase.public_facts.is_coinbase
        || facts
            .transactions
            .iter()
            .skip(1)
            .any(|transaction| transaction.public_facts.is_coinbase)
    {
        return Err(CanonicalStoreError::displaced_archive(
            "archive canonical replay must have tx[0] as its unique coinbase",
        ));
    }
    let coinbase_outputs = coinbase
        .transparent_outputs
        .iter()
        .map(|output| {
            DisplacedBlockCoinbaseOutput::new(
                output.output_index,
                output.value_zat,
                output.script_pub_key.clone(),
            )
        })
        .collect();
    Ok(DisplacedBlock {
        block_hash: position.block_hash,
        header: facts.block_header.clone(),
        transaction_ids,
        coinbase_outputs,
        raw_block_bytes: record.raw_block_bytes,
        final_note_commitment_roots: record.final_note_commitment_roots,
        displacement_event_sequence: position.event_sequence,
        displacement_epoch: record.displacement_epoch,
        displaced_at: record.displaced_at,
    })
}

fn archive_family(
    store: &RocksDbCanonicalStore,
) -> Result<std::sync::Arc<rust_rocksdb::BoundColumnFamily<'_>>, CanonicalStoreError> {
    column_family(&store.bounded_open.db, DISPLACED_BLOCK_FACTS_COLUMN_FAMILY)
}

fn read_exact_row(
    store: &RocksDbCanonicalStore,
    key: &[u8],
    operation: &'static str,
) -> Result<Option<Vec<u8>>, CanonicalStoreError> {
    let family = archive_family(store)?;
    let mut options = ReadOptions::default();
    options.fill_cache(false);
    store
        .bounded_open
        .db
        .get_cf_opt(&family, key, &options)
        .map_err(|source| CanonicalStoreError::RocksDbOperation { operation, source })
}

fn encode_order_key(position: ArchivePosition) -> [u8; ORDER_KEY_LENGTH] {
    let mut encoded = [0; ORDER_KEY_LENGTH];
    encoded[0] = ORDER_KEY_TAG;
    encoded[1..9].copy_from_slice(&position.event_sequence.to_be_bytes());
    encoded[9..13].copy_from_slice(&position.height.value().to_be_bytes());
    encoded[13..45].copy_from_slice(&position.block_hash.as_bytes());
    encoded
}

fn decode_order_key(encoded: &[u8]) -> Result<ArchivePosition, CanonicalStoreError> {
    if encoded.len() != ORDER_KEY_LENGTH || encoded.first() != Some(&ORDER_KEY_TAG) {
        return Err(CanonicalStoreError::displaced_archive(
            "archive order key is not exact version-1 bytes",
        ));
    }
    Ok(ArchivePosition {
        event_sequence: u64::from_be_bytes(read_array(encoded, 1)?),
        height: BlockHeight::new(u32::from_be_bytes(read_array(encoded, 9)?)),
        block_hash: BlockHash::from_bytes(read_array(encoded, 13)?),
    })
}

fn encode_hash_pointer_key(block_hash: BlockHash) -> [u8; HASH_POINTER_KEY_LENGTH] {
    let mut encoded = [0; HASH_POINTER_KEY_LENGTH];
    encoded[0] = HASH_POINTER_KEY_TAG;
    encoded[1..].copy_from_slice(&block_hash.as_bytes());
    encoded
}

fn encode_hash_pointer(position: ArchivePosition) -> [u8; HASH_POINTER_VALUE_LENGTH] {
    let mut encoded = [0; HASH_POINTER_VALUE_LENGTH];
    encoded[0] = VERSION_ONE;
    encoded[1..9].copy_from_slice(&position.event_sequence.to_be_bytes());
    encoded[9..13].copy_from_slice(&position.height.value().to_be_bytes());
    encoded
}

fn decode_hash_pointer(
    encoded: &[u8],
    block_hash: BlockHash,
) -> Result<ArchivePosition, CanonicalStoreError> {
    if encoded.len() != HASH_POINTER_VALUE_LENGTH || encoded.first() != Some(&VERSION_ONE) {
        return Err(CanonicalStoreError::displaced_archive(
            "archive hash pointer is not exact version-1 bytes",
        ));
    }
    let position = ArchivePosition {
        event_sequence: u64::from_be_bytes(read_array(encoded, 1)?),
        height: BlockHeight::new(u32::from_be_bytes(read_array(encoded, 9)?)),
        block_hash,
    };
    if encode_hash_pointer(position).as_slice() != encoded {
        return Err(CanonicalStoreError::displaced_archive(
            "archive hash pointer is not canonical",
        ));
    }
    Ok(position)
}

fn encode_archive_state(state: ArchiveState) -> [u8; STATE_VALUE_LENGTH] {
    let mut encoded = [0; STATE_VALUE_LENGTH];
    encoded[0] = VERSION_ONE;
    encoded[1..9].copy_from_slice(&state.coverage.activation_event_sequence.to_be_bytes());
    encoded[9..17].copy_from_slice(&state.coverage.activation_epoch.value().to_be_bytes());
    encoded[17..25].copy_from_slice(&state.coverage.activated_at.value().to_be_bytes());
    encoded[25..33].copy_from_slice(&state.block_count.to_be_bytes());
    encoded[33..41].copy_from_slice(&state.latest_event_sequence.to_be_bytes());
    encoded
}

fn decode_archive_state(encoded: &[u8]) -> Result<ArchiveState, CanonicalStoreError> {
    if encoded.len() != STATE_VALUE_LENGTH || encoded.first() != Some(&VERSION_ONE) {
        return Err(CanonicalStoreError::displaced_archive(
            "archive state is not exact version-1 bytes",
        ));
    }
    let state = ArchiveState {
        coverage: DisplacedBlockArchiveCoverage {
            activation_event_sequence: u64::from_be_bytes(read_array(encoded, 1)?),
            activation_epoch: ChainEpochId::new(u64::from_be_bytes(read_array(encoded, 9)?)),
            activated_at: UnixTimestampMillis::new(u64::from_be_bytes(read_array(encoded, 17)?)),
        },
        block_count: u64::from_be_bytes(read_array(encoded, 25)?),
        latest_event_sequence: u64::from_be_bytes(read_array(encoded, 33)?),
    };
    if encode_archive_state(state).as_slice() != encoded {
        return Err(CanonicalStoreError::displaced_archive(
            "archive state is not canonical",
        ));
    }
    Ok(state)
}

fn encode_archive_record(record: &ArchiveRecord) -> Result<Vec<u8>, CanonicalStoreError> {
    let replay_length = u32::try_from(record.replay_bytes.len())
        .map_err(|_| CanonicalStoreError::displaced_archive("archive replay length exceeds u32"))?;
    let mut encoded = Vec::new();
    encoded.push(VERSION_ONE);
    encoded.extend_from_slice(&replay_length.to_be_bytes());
    encoded.extend_from_slice(&record.replay_bytes);
    encoded.extend_from_slice(&record.displacement_epoch.value().to_be_bytes());
    encoded.extend_from_slice(&record.displaced_at.value().to_be_bytes());
    encode_optional_bytes(&mut encoded, record.raw_block_bytes.as_deref())?;
    encode_optional_roots(&mut encoded, record.final_note_commitment_roots);
    Ok(encoded)
}

fn decode_archive_record(encoded: &[u8]) -> Result<ArchiveRecord, CanonicalStoreError> {
    let mut decoder = ArchiveDecoder::new(encoded);
    decoder.require_version()?;
    let replay_length = usize::try_from(decoder.read_u32()?).map_err(|_| {
        CanonicalStoreError::displaced_archive("archive replay length exceeds usize")
    })?;
    let replay_bytes = decoder.read_bytes(replay_length)?.to_vec();
    let record = ArchiveRecord {
        replay_bytes,
        displacement_epoch: ChainEpochId::new(decoder.read_u64()?),
        displaced_at: UnixTimestampMillis::new(decoder.read_u64()?),
        raw_block_bytes: decoder.read_optional_bytes()?,
        final_note_commitment_roots: decoder.read_optional_roots()?,
    };
    decoder.require_end()?;
    if encode_archive_record(&record)? != encoded {
        return Err(CanonicalStoreError::displaced_archive(
            "archive order value is not canonical bytes",
        ));
    }
    Ok(record)
}

fn encode_optional_bytes(
    encoded: &mut Vec<u8>,
    bytes: Option<&[u8]>,
) -> Result<(), CanonicalStoreError> {
    let Some(bytes) = bytes else {
        encoded.push(0);
        return Ok(());
    };
    let length = u32::try_from(bytes.len()).map_err(|_| {
        CanonicalStoreError::displaced_archive("archive raw block length exceeds u32")
    })?;
    encoded.push(1);
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(bytes);
    Ok(())
}

fn encode_optional_roots(encoded: &mut Vec<u8>, roots: Option<BlockFinalNoteCommitmentRoots>) {
    let Some(roots) = roots else {
        encoded.push(0);
        return;
    };
    encoded.push(1);
    encoded.extend_from_slice(&roots.height.value().to_be_bytes());
    encoded.extend_from_slice(&roots.block_hash.as_bytes());
    for root in [roots.sapling, roots.orchard, roots.ironwood] {
        if let Some(root) = root {
            encoded.push(1);
            encoded.extend_from_slice(&root.as_bytes());
        } else {
            encoded.push(0);
        }
    }
}

struct ArchiveDecoder<'a> {
    encoded: &'a [u8],
    offset: usize,
}

impl<'a> ArchiveDecoder<'a> {
    const fn new(encoded: &'a [u8]) -> Self {
        Self { encoded, offset: 0 }
    }

    fn require_version(&mut self) -> Result<(), CanonicalStoreError> {
        if self.read_u8()? != VERSION_ONE {
            return Err(CanonicalStoreError::displaced_archive(
                "archive order value has an unsupported version",
            ));
        }
        Ok(())
    }

    fn read_u8(&mut self) -> Result<u8, CanonicalStoreError> {
        let byte = *self.read_bytes(1)?.first().ok_or_else(|| {
            CanonicalStoreError::displaced_archive("archive value ended before one byte")
        })?;
        Ok(byte)
    }

    fn read_u32(&mut self) -> Result<u32, CanonicalStoreError> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64, CanonicalStoreError> {
        Ok(u64::from_be_bytes(self.read_array()?))
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], CanonicalStoreError> {
        self.read_bytes(N)?.try_into().map_err(|_| {
            CanonicalStoreError::displaced_archive("archive fixed-width value is truncated")
        })
    }

    fn read_bytes(&mut self, length: usize) -> Result<&'a [u8], CanonicalStoreError> {
        let end = self.offset.checked_add(length).ok_or_else(|| {
            CanonicalStoreError::displaced_archive("archive value offset overflowed")
        })?;
        let bytes = self
            .encoded
            .get(self.offset..end)
            .ok_or_else(|| CanonicalStoreError::displaced_archive("archive value is truncated"))?;
        self.offset = end;
        Ok(bytes)
    }

    fn read_presence(&mut self) -> Result<bool, CanonicalStoreError> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(CanonicalStoreError::displaced_archive(
                "archive optional presence is not canonical",
            )),
        }
    }

    fn read_optional_bytes(&mut self) -> Result<Option<Vec<u8>>, CanonicalStoreError> {
        if !self.read_presence()? {
            return Ok(None);
        }
        let length = usize::try_from(self.read_u32()?).map_err(|_| {
            CanonicalStoreError::displaced_archive("archive raw block length exceeds usize")
        })?;
        Ok(Some(self.read_bytes(length)?.to_vec()))
    }

    fn read_optional_root(
        &mut self,
    ) -> Result<Option<FinalNoteCommitmentRoot>, CanonicalStoreError> {
        if self.read_presence()? {
            Ok(Some(FinalNoteCommitmentRoot::from_bytes(
                self.read_array()?,
            )))
        } else {
            Ok(None)
        }
    }

    fn read_optional_roots(
        &mut self,
    ) -> Result<Option<BlockFinalNoteCommitmentRoots>, CanonicalStoreError> {
        if !self.read_presence()? {
            return Ok(None);
        }
        let height = BlockHeight::new(self.read_u32()?);
        let block_hash = BlockHash::from_bytes(self.read_array()?);
        Ok(Some(BlockFinalNoteCommitmentRoots::new(
            height,
            block_hash,
            self.read_optional_root()?,
            self.read_optional_root()?,
            self.read_optional_root()?,
        )))
    }

    fn require_end(&self) -> Result<(), CanonicalStoreError> {
        if self.offset != self.encoded.len() {
            return Err(CanonicalStoreError::displaced_archive(
                "archive order value has trailing bytes",
            ));
        }
        Ok(())
    }
}

fn read_array<const N: usize>(
    encoded: &[u8],
    offset: usize,
) -> Result<[u8; N], CanonicalStoreError> {
    encoded
        .get(offset..offset.saturating_add(N))
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| {
            CanonicalStoreError::displaced_archive("archive fixed-width bytes are truncated")
        })
}

#[cfg(test)]
pub(super) fn encode_test_archive_state(
    coverage: DisplacedBlockArchiveCoverage,
    block_count: u64,
) -> Vec<u8> {
    encode_archive_state(ArchiveState {
        coverage,
        block_count,
        latest_event_sequence: coverage.activation_event_sequence,
    })
    .to_vec()
}

#[cfg(test)]
pub(super) fn encode_test_event_context(
    event_sequence: u64,
    reverted_range: BlockHeightRange,
    displacement_epoch: ChainEpochId,
    displaced_at: UnixTimestampMillis,
    row_count: u32,
) -> (Vec<u8>, Vec<u8>) {
    let context = ArchiveEventContext {
        event_sequence,
        reverted_range,
        displacement_epoch,
        displaced_at,
        row_count,
        cumulative_block_count: u64::from(row_count),
    };
    (
        encode_event_context_key(event_sequence).to_vec(),
        encode_event_context(context).to_vec(),
    )
}

#[cfg(test)]
pub(super) fn encode_test_archive_record(
    replay_bytes: Vec<u8>,
    displacement_epoch: ChainEpochId,
    displaced_at: UnixTimestampMillis,
    raw_block_bytes: Option<Vec<u8>>,
    final_note_commitment_roots: Option<BlockFinalNoteCommitmentRoots>,
) -> Result<Vec<u8>, CanonicalStoreError> {
    encode_archive_record(&ArchiveRecord {
        replay_bytes,
        displacement_epoch,
        displaced_at,
        raw_block_bytes,
        final_note_commitment_roots,
    })
}

#[cfg(test)]
pub(super) fn encode_test_order_key(
    event_sequence: u64,
    height: BlockHeight,
    block_hash: BlockHash,
) -> Vec<u8> {
    encode_order_key(ArchivePosition {
        event_sequence,
        height,
        block_hash,
    })
    .to_vec()
}

#[cfg(test)]
pub(super) fn encode_test_hash_pointer_rows(
    event_sequence: u64,
    height: BlockHeight,
    block_hash: BlockHash,
) -> (Vec<u8>, Vec<u8>) {
    let position = ArchivePosition {
        event_sequence,
        height,
        block_hash,
    };
    (
        encode_hash_pointer_key(block_hash).to_vec(),
        encode_hash_pointer(position).to_vec(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_one_keys_preserve_newest_order_and_exact_pointer() -> Result<(), CanonicalStoreError>
    {
        let older = ArchivePosition {
            event_sequence: 8,
            height: BlockHeight::new(100),
            block_hash: BlockHash::from_bytes([1; 32]),
        };
        let newer = ArchivePosition {
            event_sequence: 9,
            height: BlockHeight::new(99),
            block_hash: BlockHash::from_bytes([2; 32]),
        };
        assert!(encode_order_key(older) < encode_order_key(newer));
        assert_eq!(decode_order_key(&encode_order_key(older))?, older);
        assert_eq!(
            decode_hash_pointer(&encode_hash_pointer(newer), newer.block_hash)?,
            newer
        );
        assert_eq!(encode_hash_pointer_key(newer.block_hash)[0], 0x02);
        Ok(())
    }

    #[test]
    fn state_and_order_value_require_canonical_exact_bytes() -> Result<(), CanonicalStoreError> {
        let state = ArchiveState {
            coverage: DisplacedBlockArchiveCoverage {
                activation_event_sequence: 4,
                activation_epoch: ChainEpochId::new(4),
                activated_at: UnixTimestampMillis::new(17),
            },
            block_count: 3,
            latest_event_sequence: 4,
        };
        assert_eq!(encode_archive_state(state).len(), STATE_VALUE_LENGTH);
        assert_eq!(decode_archive_state(&encode_archive_state(state))?, state);
        let context = ArchiveEventContext {
            event_sequence: 4,
            reverted_range: BlockHeightRange::inclusive(BlockHeight::new(8), BlockHeight::new(10)),
            displacement_epoch: ChainEpochId::new(4),
            displaced_at: UnixTimestampMillis::new(17),
            row_count: 3,
            cumulative_block_count: 3,
        };
        assert_eq!(
            encode_event_context(context).len(),
            EVENT_CONTEXT_VALUE_LENGTH
        );
        assert_eq!(
            decode_event_context(&encode_event_context(context), 4)?,
            context
        );
        assert_eq!(
            encode_event_context_key(4),
            [EVENT_CONTEXT_KEY_TAG, 0, 0, 0, 0, 0, 0, 0, 4]
        );
        let record = ArchiveRecord {
            replay_bytes: vec![1, 2, 3],
            displacement_epoch: ChainEpochId::new(4),
            displaced_at: UnixTimestampMillis::new(17),
            raw_block_bytes: Some(vec![8, 9]),
            final_note_commitment_roots: Some(BlockFinalNoteCommitmentRoots::new(
                BlockHeight::new(10),
                BlockHash::from_bytes([11; 32]),
                Some(FinalNoteCommitmentRoot::from_bytes([12; 32])),
                None,
                Some(FinalNoteCommitmentRoot::from_bytes([13; 32])),
            )),
        };
        let encoded = encode_archive_record(&record)?;
        assert_eq!(decode_archive_record(&encoded)?, record);
        let mut trailing = encoded;
        trailing.push(0);
        assert!(decode_archive_record(&trailing).is_err());
        Ok(())
    }

    #[test]
    fn malformed_keys_and_optional_presence_fail_closed() {
        assert!(decode_order_key(&[ORDER_KEY_TAG]).is_err());
        assert!(decode_hash_pointer(&[VERSION_ONE], BlockHash::from_bytes([0; 32])).is_err());
        let mut encoded = vec![VERSION_ONE];
        encoded.extend_from_slice(&0_u32.to_be_bytes());
        encoded.extend_from_slice(&0_u64.to_be_bytes());
        encoded.extend_from_slice(&0_u64.to_be_bytes());
        encoded.push(2);
        assert!(decode_archive_record(&encoded).is_err());
    }
}
