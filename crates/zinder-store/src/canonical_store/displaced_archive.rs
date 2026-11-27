//! Version-1 displaced canonical-fact archive reads.

use std::{collections::BTreeMap, num::NonZeroU32};

use rust_rocksdb::{DB, ReadOptions, WriteBatch, WriteOptions};
use zinder_core::{
    BlockFinalNoteCommitmentRoots, BlockHash, BlockHeaderArtifact, BlockHeight, BlockHeightRange,
    BlockId, CanonicalBlockFactsDigestVersion, CanonicalBlockReplayFormatVersion, ChainEpochId,
    DisplacedBlock, DisplacedBlockArchiveCoverage, DisplacedBlockCoinbaseOutput,
    DisplacedRootArchiveCoverage, DisplacedRootCandidate, FinalNoteCommitmentRoot,
    SerializedBytesDigest, ShieldedProtocol, TransactionId, UnixTimestampMillis,
    decode_canonical_block_replay, encode_canonical_block_replay,
};

use super::{
    CanonicalStoreError, RocksDbCanonicalSecondary,
    publication::column_family,
    rocksdb::{DISPLACED_BLOCK_FACTS_COLUMN_FAMILY, RocksDbCanonicalStore},
};

const VERSION_ONE: u8 = 1;
const STATE_KEY: [u8; 1] = [0x00];
const ORDER_KEY_TAG: u8 = 0x01;
const HASH_POINTER_KEY_TAG: u8 = 0x02;
const EVENT_CONTEXT_KEY_TAG: u8 = 0x03;
const ROOT_INDEX_KEY_TAG: u8 = 0x04;
const ROOT_UNAVAILABLE_STATE_KEY: [u8; 1] = [0x05];
const ORDER_KEY_LENGTH: usize = 45;
const HASH_POINTER_KEY_LENGTH: usize = 33;
const HASH_POINTER_VALUE_LENGTH: usize = 13;
const STATE_VALUE_LENGTH: usize = 41;
const EVENT_CONTEXT_KEY_LENGTH: usize = 9;
const EVENT_CONTEXT_VALUE_LENGTH: usize = 37;
const ROOT_INDEX_KEY_LENGTH: usize = 78;
const ROOT_INDEX_VALUE_LENGTH: usize = 17;
const ROOT_UNAVAILABLE_STATE_VALUE_LENGTH: usize = 9;
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BorrowedArchiveRecord<'a> {
    replay_bytes: &'a [u8],
    displacement_epoch: ChainEpochId,
    displaced_at: UnixTimestampMillis,
    raw_block_bytes: Option<&'a [u8]>,
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
        let mut root_index_unavailable =
            prepare_root_unavailable_count(store, prior_state.is_none())?;
        let mut rows = Vec::with_capacity(expected_count.saturating_mul(5).saturating_add(3));
        rows.push((context_key.to_vec(), encode_event_context(context).to_vec()));
        for (expected_height, displaced) in reverted_range.into_iter().zip(displaced_blocks) {
            if displaced.block_id.height != expected_height {
                return Err(CanonicalStoreError::displaced_archive(
                    "archive write blocks are not in exact reverted-height order",
                ));
            }
            let block_time = validate_displaced_write_block(&displaced)?;
            let position = ArchivePosition {
                event_sequence,
                height: expected_height,
                block_hash: displaced.block_id.hash,
            };
            if let Some(unavailable_count) = &mut root_index_unavailable {
                append_root_index_rows(
                    RootIndexWriteInput {
                        position,
                        displacement_epoch,
                        block_time,
                        roots: displaced.final_note_commitment_roots,
                    },
                    unavailable_count,
                    &mut rows,
                )?;
            }
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
        if let Some(unavailable_count) = root_index_unavailable {
            rows.push((
                ROOT_UNAVAILABLE_STATE_KEY.to_vec(),
                encode_root_unavailable_state(unavailable_count).to_vec(),
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

#[derive(Clone, Copy)]
struct RootIndexWriteInput {
    position: ArchivePosition,
    displacement_epoch: ChainEpochId,
    block_time: i64,
    roots: Option<BlockFinalNoteCommitmentRoots>,
}

fn prepare_root_unavailable_count(
    store: &RocksDbCanonicalStore,
    activating: bool,
) -> Result<Option<u64>, CanonicalStoreError> {
    if activating {
        return Ok(Some(0));
    }
    read_root_unavailable_count_optional(store)
}

fn append_root_index_rows(
    entry: RootIndexWriteInput,
    unavailable_count: &mut u64,
    rows: &mut Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<(), CanonicalStoreError> {
    let Some(roots) = entry.roots else {
        *unavailable_count = unavailable_count.checked_add(1).ok_or_else(|| {
            CanonicalStoreError::displaced_archive("archive unavailable-root count exceeds u64")
        })?;
        return Ok(());
    };
    let encoded_val = encode_root_index_value(entry.displacement_epoch, entry.block_time);
    for (protocol, root) in [
        (ShieldedProtocol::Sapling, roots.sapling),
        (ShieldedProtocol::Orchard, roots.orchard),
        (ShieldedProtocol::Ironwood, roots.ironwood),
    ] {
        if let Some(root) = root {
            rows.push((
                encode_root_index_key(root, entry.position, protocol).to_vec(),
                encoded_val.to_vec(),
            ));
        }
    }
    Ok(())
}

fn validate_displaced_write_block(
    displaced: &CanonicalDisplacedBlock,
) -> Result<i64, CanonicalStoreError> {
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
    Ok(facts.block_header.block_time)
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

/// Summary facts retained when a canonical block is displaced, omitting raw block bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchivedDisplacedBlockSummary {
    /// Stable identity of the displaced block.
    pub block_hash: BlockHash,
    /// Complete header facts visible immediately before displacement.
    pub header: BlockHeaderArtifact,
    /// Transaction identifiers in block order.
    pub transaction_ids: Vec<TransactionId>,
    /// Transparent payout outputs from the coinbase transaction, in output order.
    pub coinbase_outputs: Vec<DisplacedBlockCoinbaseOutput>,
    /// Chain event that displaced this block.
    pub displacement_event_sequence: u64,
    /// Epoch made visible by the displacement event.
    pub displacement_epoch: ChainEpochId,
    /// Wall-clock time of the displacement epoch.
    pub displaced_at: UnixTimestampMillis,
}

/// Bounded newest-first displaced-block summary page without retained raw payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DisplacedBlockSummaryPage {
    /// Archived block summaries in descending `(event sequence, height)` order.
    pub blocks: Vec<ArchivedDisplacedBlockSummary>,
    /// Whether at least one older archive row remains.
    pub has_more: bool,
    /// Cursor for requesting rows strictly older than this page.
    pub next_cursor: Option<DisplacedBlockCursor>,
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

/// Opaque cursor for resuming newest-first displaced final-root pages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DisplacedRootCursor {
    event_sequence: u64,
    height: BlockHeight,
    block_hash: BlockHash,
    protocol: Option<ShieldedProtocol>,
}

/// Bounded newest-first displaced final-root search outcome and archive coverage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DisplacedFinalRootSearch {
    /// Matching displaced blocks ordered newest replacement first.
    pub matches: Vec<DisplacedRootCandidate>,
    /// Coverage across all displaced blocks archived at or after activation.
    pub coverage: Option<DisplacedRootArchiveCoverage>,
}

/// Bounded newest-first page of displaced final-root candidates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DisplacedFinalRootPage {
    /// Matching displaced blocks ordered newest replacement first.
    pub matches: Vec<DisplacedRootCandidate>,
    /// Whether additional archive entries remain beyond this page.
    pub has_more: bool,
    /// Cursor for continuing strictly older than this page when `has_more` is `true`.
    pub next_cursor: Option<DisplacedRootCursor>,
    /// Coverage across displaced blocks archived at or after activation.
    pub coverage: Option<DisplacedRootArchiveCoverage>,
    /// True when the result reflects complete coverage of the active archive.
    pub scan_complete: bool,
    /// Number of index entries or archive blocks inspected while producing this page.
    pub scanned_work: usize,
}

trait DisplacedArchiveRead {
    fn archive_db(&self) -> &DB;
    fn archive_reorg_window_blocks(&self) -> u32;
    fn cached_legacy_unavailable_count(
        &self,
        latest_event_sequence: u64,
        compute: &mut dyn FnMut() -> Result<u64, CanonicalStoreError>,
    ) -> Result<u64, CanonicalStoreError>;
}

impl DisplacedArchiveRead for RocksDbCanonicalStore {
    fn archive_db(&self) -> &DB {
        &self.bounded_open.db
    }

    fn archive_reorg_window_blocks(&self) -> u32 {
        self.reorg_policy().reorg_window_blocks()
    }

    fn cached_legacy_unavailable_count(
        &self,
        _latest_event_sequence: u64,
        compute: &mut dyn FnMut() -> Result<u64, CanonicalStoreError>,
    ) -> Result<u64, CanonicalStoreError> {
        compute()
    }
}

impl DisplacedArchiveRead for RocksDbCanonicalSecondary {
    fn archive_db(&self) -> &DB {
        &self.bounded_open.db
    }

    fn archive_reorg_window_blocks(&self) -> u32 {
        self.reorg_policy().reorg_window_blocks()
    }

    fn cached_legacy_unavailable_count(
        &self,
        latest_event_sequence: u64,
        compute: &mut dyn FnMut() -> Result<u64, CanonicalStoreError>,
    ) -> Result<u64, CanonicalStoreError> {
        let cached = *self.legacy_root_unavailable_count.lock();
        if let Some((cached_sequence, unavailable_count)) = cached
            && cached_sequence == latest_event_sequence
        {
            return Ok(unavailable_count);
        }
        let unavailable_count = compute()?;
        *self.legacy_root_unavailable_count.lock() =
            Some((latest_event_sequence, unavailable_count));
        Ok(unavailable_count)
    }
}

macro_rules! impl_displaced_archive_reads {
    ($store_type:ty) => {
        impl $store_type {
            /// Reads a bounded newest-first page strictly older than `after`.
            pub fn displaced_block_page(
                &self,
                after: Option<&DisplacedBlockCursor>,
                limit: NonZeroU32,
            ) -> Result<DisplacedBlockPage, CanonicalStoreError> {
                read_displaced_block_page(self, after, limit)
            }

            /// Reads a bounded newest-first summary page without copying retained raw block bytes.
            pub fn displaced_block_summary_page(
                &self,
                after: Option<&DisplacedBlockCursor>,
                limit: NonZeroU32,
            ) -> Result<DisplacedBlockSummaryPage, CanonicalStoreError> {
                read_displaced_block_summary_page(self, after, limit)
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
                read_displaced_block_by_hash(self, block_hash)
            }

            /// Reads a bounded newest-first prefix after validating the event's complete reorg range.
            pub fn displaced_blocks_for_event(
                &self,
                event_sequence: u64,
                limit: NonZeroU32,
            ) -> Result<Vec<DisplacedBlock>, CanonicalStoreError> {
                read_displaced_blocks_for_event(self, event_sequence, limit)
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

            /// Searches archived displaced blocks newest-first for a matching final root.
            pub fn search_displaced_final_note_commitment_roots(
                &self,
                root: FinalNoteCommitmentRoot,
                max_matches: usize,
            ) -> Result<DisplacedFinalRootSearch, CanonicalStoreError> {
                search_displaced_roots(self, root, max_matches)
            }

            /// Reads one bounded newest-first page of displaced blocks matching `root`.
            pub fn search_displaced_final_note_commitment_roots_page(
                &self,
                root: FinalNoteCommitmentRoot,
                after: Option<&DisplacedRootCursor>,
                limit: NonZeroU32,
            ) -> Result<DisplacedFinalRootPage, CanonicalStoreError> {
                search_displaced_roots_page(self, root, after, limit)
            }
        }
    };
}

impl_displaced_archive_reads!(RocksDbCanonicalStore);
impl_displaced_archive_reads!(RocksDbCanonicalSecondary);

struct ValidatedArchiveRecord<'a> {
    position: ArchivePosition,
    header: BlockHeaderArtifact,
    transaction_ids: Vec<TransactionId>,
    coinbase_outputs: Vec<DisplacedBlockCoinbaseOutput>,
    record: BorrowedArchiveRecord<'a>,
}

fn read_displaced_block_page(
    store: &impl DisplacedArchiveRead,
    after: Option<&DisplacedBlockCursor>,
    limit: NonZeroU32,
) -> Result<DisplacedBlockPage, CanonicalStoreError> {
    let (blocks, has_more, next_cursor) =
        read_displaced_page_entries(store, after, limit, derive_displaced_block, |block| {
            DisplacedBlockCursor::from_position(
                block.displacement_event_sequence,
                block.header.height,
                block.block_hash,
            )
        })?;
    Ok(DisplacedBlockPage {
        blocks,
        has_more,
        next_cursor,
    })
}

fn read_displaced_block_summary_page(
    store: &impl DisplacedArchiveRead,
    after: Option<&DisplacedBlockCursor>,
    limit: NonZeroU32,
) -> Result<DisplacedBlockSummaryPage, CanonicalStoreError> {
    let (blocks, has_more, next_cursor) = read_displaced_page_entries(
        store,
        after,
        limit,
        |validated| derive_displaced_summary(&validated),
        |block| {
            DisplacedBlockCursor::from_position(
                block.displacement_event_sequence,
                block.header.height,
                block.block_hash,
            )
        },
    )?;
    Ok(DisplacedBlockSummaryPage {
        blocks,
        has_more,
        next_cursor,
    })
}

fn read_displaced_page_entries<Entry>(
    store: &impl DisplacedArchiveRead,
    after: Option<&DisplacedBlockCursor>,
    limit: NonZeroU32,
    build_entry: impl Fn(ValidatedArchiveRecord<'_>) -> Entry,
    cursor_for: impl Fn(&Entry) -> DisplacedBlockCursor,
) -> Result<(Vec<Entry>, bool, Option<DisplacedBlockCursor>), CanonicalStoreError> {
    let max_blocks = checked_public_limit(limit)?;
    if read_archive_state(store)?.is_none() {
        if after.is_some() {
            return Err(CanonicalStoreError::displaced_archive(
                "archive cursor cannot resolve in an empty archive",
            ));
        }
        return Ok((Vec::new(), false, None));
    }

    let family = archive_family(store)?;
    let mut options = ReadOptions::default();
    options.fill_cache(false);
    let mut rows = store.archive_db().raw_iterator_cf_opt(&family, options);
    let mut context_cache = None;
    seek_displaced_page_start(store, after, &mut rows, &mut context_cache)?;

    let requested_with_lookahead = max_blocks.checked_add(1).ok_or_else(|| {
        CanonicalStoreError::displaced_archive("archive page lookahead overflowed")
    })?;
    let mut entries = Vec::with_capacity(requested_with_lookahead);
    while rows.valid() && entries.len() < requested_with_lookahead {
        let Some((key, encoded_record)) = rows.item() else {
            break;
        };
        if key.first() != Some(&ORDER_KEY_TAG) {
            break;
        }
        let validated =
            decode_and_validate_borrowed_cached(store, key, encoded_record, &mut context_cache)?;
        entries.push(build_entry(validated));
        rows.prev();
    }
    rows.status()
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "displaced archive page iteration",
            source,
        })?;
    let has_more = entries.len() > max_blocks;
    entries.truncate(max_blocks);
    let next_cursor = has_more.then(|| entries.last().map(&cursor_for)).flatten();
    Ok((entries, has_more, next_cursor))
}

fn seek_displaced_page_start(
    store: &impl DisplacedArchiveRead,
    after: Option<&DisplacedBlockCursor>,
    rows: &mut rust_rocksdb::DBRawIteratorWithThreadMode<'_, rust_rocksdb::DB>,
    context_cache: &mut Option<ArchiveEventContext>,
) -> Result<(), CanonicalStoreError> {
    if let Some(cursor) = after {
        let position = ArchivePosition {
            event_sequence: cursor.event_sequence(),
            height: cursor.height(),
            block_hash: cursor.block_hash(),
        };
        let key = encode_order_key(position);
        let encoded =
            read_exact_row(store, &key, "displaced archive cursor read")?.ok_or_else(|| {
                CanonicalStoreError::displaced_archive(
                    "archive cursor does not resolve an exact order row",
                )
            })?;
        let _ = decode_and_validate_record_cached(store, &key, &encoded, context_cache)?;
        rows.seek_for_prev(key);
        if rows.valid() && rows.key() == Some(key.as_slice()) {
            rows.prev();
        }
    } else {
        rows.seek_for_prev([HASH_POINTER_KEY_TAG]);
    }
    Ok(())
}

fn read_displaced_block_by_hash(
    store: &impl DisplacedArchiveRead,
    block_hash: BlockHash,
) -> Result<Option<DisplacedBlock>, CanonicalStoreError> {
    if read_archive_state(store)?.is_none() {
        return Ok(None);
    }
    let pointer_key = encode_hash_pointer_key(block_hash);
    let Some(pointer_bytes) =
        read_exact_row(store, &pointer_key, "displaced archive hash pointer read")?
    else {
        return Ok(None);
    };
    let position = decode_hash_pointer(&pointer_bytes, block_hash)?;
    let order_key = encode_order_key(position);
    let encoded = read_exact_row(store, &order_key, "displaced archive hash target read")?
        .ok_or_else(|| {
            CanonicalStoreError::displaced_archive("archive hash pointer target row is absent")
        })?;
    let mut context_cache = None;
    let block = decode_and_validate_record_cached(store, &order_key, &encoded, &mut context_cache)?;
    if block.block_hash != block_hash {
        return Err(CanonicalStoreError::displaced_archive(
            "archive hash pointer resolved a different block hash",
        ));
    }
    Ok(Some(block))
}

fn read_displaced_blocks_for_event(
    store: &impl DisplacedArchiveRead,
    event_sequence: u64,
    limit: NonZeroU32,
) -> Result<Vec<DisplacedBlock>, CanonicalStoreError> {
    let max_blocks = checked_public_limit(limit)?;
    let Some(state) = read_archive_state(store)? else {
        return Ok(Vec::new());
    };
    if event_sequence < state.coverage.activation_event_sequence {
        return Ok(Vec::new());
    }
    let Some(context) = read_event_context_optional(store, event_sequence)? else {
        if event_has_order_row(store, event_sequence)? {
            return Err(CanonicalStoreError::displaced_archive(
                "archive event order rows exist without event context",
            ));
        }
        return Ok(Vec::new());
    };
    let expected_count = validate_event_context(store, context)?;
    let mut blocks =
        collect_event_displaced_blocks(store, event_sequence, context, expected_count)?;
    blocks.truncate(max_blocks);
    Ok(blocks)
}

fn collect_event_displaced_blocks(
    store: &impl DisplacedArchiveRead,
    event_sequence: u64,
    context: ArchiveEventContext,
    expected_count: usize,
) -> Result<Vec<DisplacedBlock>, CanonicalStoreError> {
    let family = archive_family(store)?;
    let mut options = ReadOptions::default();
    options.fill_cache(false);
    let mut rows = store.archive_db().raw_iterator_cf_opt(&family, options);
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
    Ok(blocks)
}

#[derive(Clone, Copy)]
struct RootSearchQuery<'a> {
    root: FinalNoteCommitmentRoot,
    after: Option<&'a DisplacedRootCursor>,
    limit: usize,
}

const fn empty_root_page() -> DisplacedFinalRootPage {
    DisplacedFinalRootPage {
        matches: Vec::new(),
        has_more: false,
        next_cursor: None,
        coverage: None,
        scan_complete: false,
        scanned_work: 0,
    }
}

const fn cursor_for_candidate(candidate: &DisplacedRootCandidate) -> DisplacedRootCursor {
    DisplacedRootCursor {
        event_sequence: candidate.displacement_event_sequence,
        height: candidate.block_id.height,
        block_hash: candidate.block_id.hash,
        protocol: Some(candidate.protocol),
    }
}

const fn cursor_for_position(position: ArchivePosition) -> DisplacedRootCursor {
    DisplacedRootCursor {
        event_sequence: position.event_sequence,
        height: position.height,
        block_hash: position.block_hash,
        protocol: None,
    }
}

fn search_displaced_roots(
    store: &impl DisplacedArchiveRead,
    root: FinalNoteCommitmentRoot,
    max_matches: usize,
) -> Result<DisplacedFinalRootSearch, CanonicalStoreError> {
    if let Some(unavailable_count) = read_root_unavailable_count_optional(store)? {
        let Some(state) = read_archive_state(store)? else {
            return Err(CanonicalStoreError::displaced_archive(
                "archive root-unavailable state exists without version-1 archive state",
            ));
        };
        let bounded = max_matches.min(MAX_PUBLIC_PAGE_LIMIT as usize);
        let matches = if bounded == 0 {
            Vec::new()
        } else {
            search_displaced_roots_indexed(
                store,
                state,
                unavailable_count,
                RootSearchQuery {
                    root,
                    after: None,
                    limit: bounded,
                },
            )?
            .matches
        };
        return Ok(DisplacedFinalRootSearch {
            matches,
            coverage: Some(DisplacedRootArchiveCoverage {
                activation_event_sequence: state.coverage.activation_event_sequence,
                activation_epoch: state.coverage.activation_epoch,
                activated_at: state.coverage.activated_at,
                captured_block_count: state.block_count,
                root_artifact_unavailable_count: unavailable_count,
            }),
        });
    }
    let Some(state) = read_archive_state(store)? else {
        return Ok(DisplacedFinalRootSearch {
            matches: Vec::new(),
            coverage: None,
        });
    };
    let page = search_displaced_roots_fallback(
        store,
        state,
        RootSearchQuery {
            root,
            after: None,
            limit: max_matches.min(MAX_PUBLIC_PAGE_LIMIT as usize),
        },
    )?;
    Ok(DisplacedFinalRootSearch {
        matches: page.matches,
        coverage: page.coverage,
    })
}

fn search_displaced_roots_page(
    store: &impl DisplacedArchiveRead,
    root: FinalNoteCommitmentRoot,
    after: Option<&DisplacedRootCursor>,
    limit: NonZeroU32,
) -> Result<DisplacedFinalRootPage, CanonicalStoreError> {
    let page_limit = checked_public_limit(limit)?;
    let Some(state) = read_archive_state(store)? else {
        if after.is_some() {
            return Err(CanonicalStoreError::displaced_archive(
                "archive root cursor cannot resolve in an empty archive",
            ));
        }
        return Ok(empty_root_page());
    };
    let query = RootSearchQuery {
        root,
        after,
        limit: page_limit,
    };
    read_root_unavailable_count_optional(store)?.map_or_else(
        || search_displaced_roots_fallback(store, state, query),
        |unavailable_count| search_displaced_roots_indexed(store, state, unavailable_count, query),
    )
}

fn search_displaced_roots_indexed(
    store: &impl DisplacedArchiveRead,
    state: ArchiveState,
    unavailable_count: u64,
    query: RootSearchQuery<'_>,
) -> Result<DisplacedFinalRootPage, CanonicalStoreError> {
    let family = archive_family(store)?;
    let mut options = ReadOptions::default();
    options.fill_cache(false);
    let mut rows = store.archive_db().raw_iterator_cf_opt(&family, options);
    seek_indexed_root_start(&mut rows, query.root, query.after);

    let lookahead = query.limit.saturating_add(1);
    let mut matches = Vec::with_capacity(lookahead.min(64));
    let mut rows_scanned = 0_usize;
    while rows.valid() && matches.len() < lookahead {
        let Some((key, encoded_val)) = rows.item() else {
            break;
        };
        if key.first() != Some(&ROOT_INDEX_KEY_TAG) {
            break;
        }
        let Some(candidate) = decode_matching_root_index_row(key, encoded_val, query.root)? else {
            break;
        };
        rows_scanned = rows_scanned.saturating_add(1);
        matches.push(candidate);
        rows.next();
    }
    rows.status()
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "displaced archive indexed root search",
            source,
        })?;
    let has_more = matches.len() > query.limit;
    matches.truncate(query.limit);
    let next_cursor = has_more
        .then(|| matches.last().map(cursor_for_candidate))
        .flatten();
    Ok(DisplacedFinalRootPage {
        matches,
        has_more,
        next_cursor,
        coverage: Some(DisplacedRootArchiveCoverage {
            activation_event_sequence: state.coverage.activation_event_sequence,
            activation_epoch: state.coverage.activation_epoch,
            activated_at: state.coverage.activated_at,
            captured_block_count: state.block_count,
            root_artifact_unavailable_count: unavailable_count,
        }),
        scan_complete: true,
        scanned_work: rows_scanned,
    })
}

fn seek_indexed_root_start(
    rows: &mut rust_rocksdb::DBRawIteratorWithThreadMode<'_, rust_rocksdb::DB>,
    root: FinalNoteCommitmentRoot,
    after: Option<&DisplacedRootCursor>,
) {
    let Some(cursor) = after else {
        rows.seek(encode_root_index_prefix(root));
        return;
    };
    let cursor_position = ArchivePosition {
        event_sequence: cursor.event_sequence,
        height: cursor.height,
        block_hash: cursor.block_hash,
    };
    let cursor_protocol = cursor.protocol.unwrap_or(ShieldedProtocol::Ironwood);
    let cursor_key = encode_root_index_key(root, cursor_position, cursor_protocol);
    rows.seek(cursor_key);
    while rows.valid() {
        let Some(current_key) = rows.key() else {
            break;
        };
        let same_row = current_key == cursor_key.as_slice()
            || (cursor.protocol.is_none()
                && current_key.get(..ROOT_INDEX_KEY_LENGTH - 1)
                    == Some(&cursor_key[..ROOT_INDEX_KEY_LENGTH - 1]));
        if !same_row {
            break;
        }
        rows.next();
    }
}

fn search_displaced_roots_fallback(
    store: &impl DisplacedArchiveRead,
    state: ArchiveState,
    query: RootSearchQuery<'_>,
) -> Result<DisplacedFinalRootPage, CanonicalStoreError> {
    let archive_unavailable_count =
        legacy_archive_unavailable_count(store, state.latest_event_sequence)?;
    let family = archive_family(store)?;
    let mut options = ReadOptions::default();
    options.fill_cache(false);
    let mut rows = store.archive_db().raw_iterator_cf_opt(&family, options);
    let mut context_cache = None;
    let lookahead = query.limit.saturating_add(1);
    let mut matches = Vec::with_capacity(lookahead.min(64));
    let mut blocks_scanned = 0_usize;
    let mut unused_unavailable = 0_u64;
    let mut last_scanned_position = seek_fallback_initial_position(
        store,
        query,
        (&mut rows, &mut context_cache),
        (&mut unused_unavailable, &mut matches),
    )?;

    while rows.valid()
        && matches.len() < lookahead
        && blocks_scanned < MAX_PUBLIC_PAGE_LIMIT as usize
    {
        let Some((key, encoded_record)) = rows.item() else {
            break;
        };
        if key.first() != Some(&ORDER_KEY_TAG) {
            break;
        }
        let position = decode_order_key(key)?;
        let validated =
            decode_and_validate_borrowed_cached(store, key, encoded_record, &mut context_cache)?;
        blocks_scanned = blocks_scanned.saturating_add(1);
        last_scanned_position = Some(position);
        append_matching_displaced_roots(
            validated.record.final_note_commitment_roots,
            position,
            validated.record.displacement_epoch,
            validated.header.block_time,
            (query.root, lookahead, None),
            (&mut unused_unavailable, &mut matches),
        );
        rows.prev();
    }
    rows.status()
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "displaced archive root scan",
            source,
        })?;
    let more_blocks_remain =
        rows.valid() && rows.key().and_then(|key| key.first()) == Some(&ORDER_KEY_TAG);
    Ok(finalize_fallback_root_page(
        state,
        query,
        (
            matches,
            more_blocks_remain,
            last_scanned_position,
            blocks_scanned,
            archive_unavailable_count,
        ),
    ))
}

fn legacy_archive_unavailable_count(
    store: &impl DisplacedArchiveRead,
    latest_event_sequence: u64,
) -> Result<u64, CanonicalStoreError> {
    store.cached_legacy_unavailable_count(latest_event_sequence, &mut || {
        count_archive_unavailable_roots(store)
    })
}

fn count_archive_unavailable_roots(
    store: &impl DisplacedArchiveRead,
) -> Result<u64, CanonicalStoreError> {
    let family = archive_family(store)?;
    let mut options = ReadOptions::default();
    options.fill_cache(false);
    let mut rows = store.archive_db().raw_iterator_cf_opt(&family, options);
    rows.seek([ORDER_KEY_TAG]);
    let mut unavailable_count = 0_u64;
    while rows.valid() {
        let Some((key, encoded_record)) = rows.item() else {
            break;
        };
        if key.first() != Some(&ORDER_KEY_TAG) {
            break;
        }
        if decode_borrowed_archive_record(encoded_record)?
            .final_note_commitment_roots
            .is_none()
        {
            unavailable_count = unavailable_count.checked_add(1).ok_or_else(|| {
                CanonicalStoreError::displaced_archive("archive unavailable-root count exceeds u64")
            })?;
        }
        rows.next();
    }
    rows.status()
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "displaced archive unavailable-root scan",
            source,
        })?;
    Ok(unavailable_count)
}

fn seek_fallback_initial_position(
    store: &impl DisplacedArchiveRead,
    query: RootSearchQuery<'_>,
    iter: (
        &mut rust_rocksdb::DBRawIteratorWithThreadMode<'_, DB>,
        &mut Option<ArchiveEventContext>,
    ),
    match_state: (&mut u64, &mut Vec<DisplacedRootCandidate>),
) -> Result<Option<ArchivePosition>, CanonicalStoreError> {
    let (rows, context_cache) = iter;
    let Some(cursor) = query.after else {
        rows.seek_for_prev([HASH_POINTER_KEY_TAG]);
        return Ok(None);
    };
    let anchor_block = seek_fallback_root_cursor(store, cursor, rows, context_cache)?;
    let lookahead = query.limit.saturating_add(1);
    let anchor_pos = ArchivePosition {
        event_sequence: cursor.event_sequence,
        height: cursor.height,
        block_hash: cursor.block_hash,
    };
    let last_pos = cursor.protocol.map(|after_proto| {
        append_matching_displaced_roots(
            anchor_block.final_note_commitment_roots,
            anchor_pos,
            anchor_block.displacement_epoch,
            anchor_block.header.block_time,
            (query.root, lookahead, Some(after_proto)),
            match_state,
        );
        anchor_pos
    });
    rows.prev();
    Ok(last_pos)
}

fn finalize_fallback_root_page(
    state: ArchiveState,
    query: RootSearchQuery<'_>,
    page_parts: (
        Vec<DisplacedRootCandidate>,
        bool,
        Option<ArchivePosition>,
        usize,
        u64,
    ),
) -> DisplacedFinalRootPage {
    let (
        mut matches,
        more_blocks_remain,
        last_scanned_position,
        blocks_scanned,
        archive_unavailable_count,
    ) = page_parts;
    let (has_more, next_cursor) = if matches.len() > query.limit {
        matches.truncate(query.limit);
        (true, matches.last().map(cursor_for_candidate))
    } else if more_blocks_remain {
        (true, last_scanned_position.map(cursor_for_position))
    } else {
        (false, None)
    };
    let scan_complete = query.after.is_none() && !has_more;
    DisplacedFinalRootPage {
        matches,
        has_more,
        next_cursor,
        coverage: Some(DisplacedRootArchiveCoverage {
            activation_event_sequence: state.coverage.activation_event_sequence,
            activation_epoch: state.coverage.activation_epoch,
            activated_at: state.coverage.activated_at,
            captured_block_count: state.block_count,
            root_artifact_unavailable_count: archive_unavailable_count,
        }),
        scan_complete,
        scanned_work: blocks_scanned,
    }
}

fn seek_fallback_root_cursor(
    store: &impl DisplacedArchiveRead,
    cursor: &DisplacedRootCursor,
    rows: &mut rust_rocksdb::DBRawIteratorWithThreadMode<'_, DB>,
    context_cache: &mut Option<ArchiveEventContext>,
) -> Result<DisplacedBlock, CanonicalStoreError> {
    let key = encode_order_key(ArchivePosition {
        event_sequence: cursor.event_sequence,
        height: cursor.height,
        block_hash: cursor.block_hash,
    });
    let encoded =
        read_exact_row(store, &key, "displaced archive root cursor read")?.ok_or_else(|| {
            CanonicalStoreError::displaced_archive(
                "archive root cursor does not resolve an exact order row",
            )
        })?;
    let block = decode_and_validate_record_cached(store, &key, &encoded, context_cache)?;
    rows.seek(key);
    Ok(block)
}

#[expect(
    clippy::too_many_arguments,
    reason = "match helper inspects only decoded root metadata without allocating a DisplacedBlock"
)]
fn append_matching_displaced_roots(
    maybe_roots: Option<BlockFinalNoteCommitmentRoots>,
    position: ArchivePosition,
    displacement_epoch: ChainEpochId,
    block_time: i64,
    filter: (FinalNoteCommitmentRoot, usize, Option<ShieldedProtocol>),
    state: (&mut u64, &mut Vec<DisplacedRootCandidate>),
) {
    let (target_root, max_matches, after_protocol) = filter;
    let (root_artifact_unavailable_count, matches) = state;
    let Some(roots) = maybe_roots else {
        if after_protocol.is_none() {
            *root_artifact_unavailable_count = root_artifact_unavailable_count.saturating_add(1);
        }
        return;
    };
    for (protocol, root) in [
        (ShieldedProtocol::Sapling, roots.sapling),
        (ShieldedProtocol::Orchard, roots.orchard),
        (ShieldedProtocol::Ironwood, roots.ironwood),
    ] {
        if after_protocol.is_some_and(|cursor_proto| protocol.id() <= cursor_proto.id()) {
            continue;
        }
        if matches.len() >= max_matches {
            break;
        }
        if root == Some(target_root) {
            matches.push(DisplacedRootCandidate {
                block_id: BlockId::new(position.height, position.block_hash),
                protocol,
                root: target_root,
                displacement_event_sequence: position.event_sequence,
                displacement_epoch,
                block_time_unix_seconds: block_time,
            });
        }
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
    options.fill_cache(true);
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
    validate_root_index_state(db, state.block_count)?;
    Ok(())
}

fn validate_root_index_state(
    db: &rust_rocksdb::DB,
    archive_block_count: u64,
) -> Result<(), CanonicalStoreError> {
    let encoded_unavailable = read_exact_db_row(
        db,
        &ROOT_UNAVAILABLE_STATE_KEY,
        "reorg archive root-unavailable state read",
    )?;
    if let Some(encoded) = encoded_unavailable {
        let unavailable_count = decode_root_unavailable_state(&encoded)?;
        if unavailable_count > archive_block_count {
            return Err(CanonicalStoreError::displaced_archive(
                "archive unavailable-root count exceeds archived block count",
            ));
        }
        return Ok(());
    }
    let family = db
        .cf_handle(DISPLACED_BLOCK_FACTS_COLUMN_FAMILY)
        .ok_or_else(|| CanonicalStoreError::displaced_archive("archive column family is absent"))?;
    let mut options = ReadOptions::default();
    options.fill_cache(false);
    let mut rows = db.raw_iterator_cf_opt(&family, options);
    rows.seek([ROOT_INDEX_KEY_TAG]);
    let has_root_index = rows.valid()
        && rows
            .key()
            .and_then(|key| key.first())
            .is_some_and(|tag| *tag == ROOT_INDEX_KEY_TAG);
    rows.status()
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "reorg archive root index admission check",
            source,
        })?;
    if has_root_index {
        return Err(CanonicalStoreError::displaced_archive(
            "archive root-index rows exist without root-unavailable state",
        ));
    }
    Ok(())
}

/// Populates missing `[0x04]`/`[0x05]` root-index rows once for upgraded ready primaries.
pub(super) fn backfill_displaced_root_index_if_absent(db: &DB) -> Result<(), CanonicalStoreError> {
    let Some(encoded_state) =
        read_exact_db_row(db, &STATE_KEY, "reorg archive backfill state read")?
    else {
        return Ok(());
    };
    if read_exact_db_row(
        db,
        &ROOT_UNAVAILABLE_STATE_KEY,
        "reorg archive root-unavailable backfill check",
    )?
    .is_some()
    {
        return Ok(());
    }
    let state = decode_archive_state(&encoded_state)?;
    let rows = collect_backfill_root_rows(db)?;
    write_backfill_root_rows_and_verify(db, &rows)?;
    validate_root_index_state(db, state.block_count)
}

type StagedArchiveRows = Vec<(Vec<u8>, Vec<u8>)>;

fn collect_backfill_root_rows(db: &DB) -> Result<StagedArchiveRows, CanonicalStoreError> {
    let family = db
        .cf_handle(DISPLACED_BLOCK_FACTS_COLUMN_FAMILY)
        .ok_or_else(|| CanonicalStoreError::displaced_archive("archive column family is absent"))?;
    let mut read_opts = ReadOptions::default();
    read_opts.fill_cache(false);
    let mut rows_iter = db.raw_iterator_cf_opt(&family, read_opts);
    rows_iter.seek([ORDER_KEY_TAG]);

    let mut unavailable_count = 0_u64;
    let mut staged = Vec::new();
    while rows_iter.valid() {
        let Some((key, encoded_record)) = rows_iter.item() else {
            break;
        };
        if key.first() != Some(&ORDER_KEY_TAG) {
            break;
        }
        let position = decode_order_key(key)?;
        let borrowed = decode_borrowed_archive_record(encoded_record)?;
        let replay = decode_canonical_block_replay(borrowed.replay_bytes).map_err(|source| {
            CanonicalStoreError::displaced_archive(format!(
                "archive canonical replay is invalid during backfill: {source}"
            ))
        })?;
        append_root_index_rows(
            RootIndexWriteInput {
                position,
                displacement_epoch: borrowed.displacement_epoch,
                block_time: replay.facts().block_header.block_time,
                roots: borrowed.final_note_commitment_roots,
            },
            &mut unavailable_count,
            &mut staged,
        )?;
        rows_iter.next();
    }
    rows_iter
        .status()
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "displaced archive root-index backfill scan",
            source,
        })?;
    staged.push((
        ROOT_UNAVAILABLE_STATE_KEY.to_vec(),
        encode_root_unavailable_state(unavailable_count).to_vec(),
    ));
    Ok(staged)
}

fn write_backfill_root_rows_and_verify(
    db: &DB,
    rows: &[(Vec<u8>, Vec<u8>)],
) -> Result<(), CanonicalStoreError> {
    let family = db
        .cf_handle(DISPLACED_BLOCK_FACTS_COLUMN_FAMILY)
        .ok_or_else(|| CanonicalStoreError::displaced_archive("archive column family is absent"))?;
    let mut batch = WriteBatch::default();
    for (key, encoded) in rows {
        batch.put_cf(&family, key, encoded);
    }
    let mut write_opts = WriteOptions::default();
    write_opts.disable_wal(false);
    write_opts.set_sync(true);
    db.write_opt(&batch, &write_opts)
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "displaced archive root-index backfill write",
            source,
        })?;
    for (key, expected) in rows {
        let observed = read_exact_db_row(db, key, "displaced archive backfill readback")?;
        if observed.as_deref() != Some(expected.as_slice()) {
            return Err(CanonicalStoreError::displaced_archive(
                "archive root-index backfill row differs after synced write",
            ));
        }
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
    options.fill_cache(true);
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
    options.fill_cache(true);
    db.get_cf_opt(&family, key, &options)
        .map_err(|source| CanonicalStoreError::RocksDbOperation { operation, source })
}

fn archive_has_any_row(db: &rust_rocksdb::DB) -> Result<bool, CanonicalStoreError> {
    let family = db
        .cf_handle(DISPLACED_BLOCK_FACTS_COLUMN_FAMILY)
        .ok_or_else(|| CanonicalStoreError::displaced_archive("archive column family is absent"))?;
    let mut options = ReadOptions::default();
    options.fill_cache(true);
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
    store: &impl DisplacedArchiveRead,
    range: zinder_core::BlockHeightRange,
) -> Result<usize, CanonicalStoreError> {
    checked_reorg_range_count_with_window(range, store.archive_reorg_window_blocks())
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
    store: &impl DisplacedArchiveRead,
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
    store: &impl DisplacedArchiveRead,
    event_sequence: u64,
) -> Result<ArchiveEventContext, CanonicalStoreError> {
    read_event_context_optional(store, event_sequence)?.ok_or_else(|| {
        CanonicalStoreError::displaced_archive("archive event context row is absent")
    })
}

fn read_event_context_optional(
    store: &impl DisplacedArchiveRead,
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
    store: &impl DisplacedArchiveRead,
    event_sequence: u64,
) -> Result<bool, CanonicalStoreError> {
    let family = archive_family(store)?;
    let mut options = ReadOptions::default();
    options.fill_cache(false);
    let mut rows = store.archive_db().raw_iterator_cf_opt(&family, options);
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
    store: &impl DisplacedArchiveRead,
) -> Result<Option<ArchiveState>, CanonicalStoreError> {
    let Some(encoded) = read_exact_row(store, &STATE_KEY, "displaced archive state read")? else {
        let family = archive_family(store)?;
        let mut options = ReadOptions::default();
        options.fill_cache(false);
        let mut rows = store.archive_db().raw_iterator_cf_opt(&family, options);
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
    store: &impl DisplacedArchiveRead,
    key: &[u8],
    encoded: &[u8],
    context_cache: &mut Option<ArchiveEventContext>,
) -> Result<DisplacedBlock, CanonicalStoreError> {
    decode_and_validate_borrowed_cached(store, key, encoded, context_cache)
        .map(derive_displaced_block)
}

fn decode_and_validate_borrowed_cached<'a>(
    store: &impl DisplacedArchiveRead,
    key: &[u8],
    encoded: &'a [u8],
    context_cache: &mut Option<ArchiveEventContext>,
) -> Result<ValidatedArchiveRecord<'a>, CanonicalStoreError> {
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
    validate_borrowed_record_with_context(key, encoded, context)
}

fn decode_and_validate_record_with_context(
    key: &[u8],
    encoded: &[u8],
    context: ArchiveEventContext,
) -> Result<DisplacedBlock, CanonicalStoreError> {
    validate_borrowed_record_with_context(key, encoded, context).map(derive_displaced_block)
}

fn validate_borrowed_record_with_context<'a>(
    key: &[u8],
    encoded: &'a [u8],
    context: ArchiveEventContext,
) -> Result<ValidatedArchiveRecord<'a>, CanonicalStoreError> {
    let position = decode_order_key(key)?;
    if position.event_sequence != context.event_sequence {
        return Err(CanonicalStoreError::displaced_archive(
            "archive row event does not match its cached event context",
        ));
    }
    let record = decode_borrowed_archive_record(encoded)?;
    let replay = decode_canonical_block_replay(record.replay_bytes).map_err(|source| {
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
    if record.raw_block_bytes.is_some_and(|raw| {
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
    extract_validated_archive_record(position, record, &replay.into_facts())
}

fn extract_validated_archive_record<'a>(
    position: ArchivePosition,
    record: BorrowedArchiveRecord<'a>,
    facts: &zinder_core::CanonicalBlockFacts,
) -> Result<ValidatedArchiveRecord<'a>, CanonicalStoreError> {
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
    Ok(ValidatedArchiveRecord {
        position,
        header: facts.block_header.clone(),
        transaction_ids,
        coinbase_outputs,
        record,
    })
}

fn derive_displaced_summary(
    validated: &ValidatedArchiveRecord<'_>,
) -> ArchivedDisplacedBlockSummary {
    ArchivedDisplacedBlockSummary {
        block_hash: validated.position.block_hash,
        header: validated.header.clone(),
        transaction_ids: validated.transaction_ids.clone(),
        coinbase_outputs: validated.coinbase_outputs.clone(),
        displacement_event_sequence: validated.position.event_sequence,
        displacement_epoch: validated.record.displacement_epoch,
        displaced_at: validated.record.displaced_at,
    }
}

fn derive_displaced_block(validated: ValidatedArchiveRecord<'_>) -> DisplacedBlock {
    DisplacedBlock {
        block_hash: validated.position.block_hash,
        header: validated.header,
        transaction_ids: validated.transaction_ids,
        coinbase_outputs: validated.coinbase_outputs,
        raw_block_bytes: validated.record.raw_block_bytes.map(<[u8]>::to_vec),
        final_note_commitment_roots: validated.record.final_note_commitment_roots,
        displacement_event_sequence: validated.position.event_sequence,
        displacement_epoch: validated.record.displacement_epoch,
        displaced_at: validated.record.displaced_at,
    }
}

fn archive_family(
    store: &impl DisplacedArchiveRead,
) -> Result<std::sync::Arc<rust_rocksdb::BoundColumnFamily<'_>>, CanonicalStoreError> {
    column_family(store.archive_db(), DISPLACED_BLOCK_FACTS_COLUMN_FAMILY)
}

fn read_exact_row(
    store: &impl DisplacedArchiveRead,
    key: &[u8],
    operation: &'static str,
) -> Result<Option<Vec<u8>>, CanonicalStoreError> {
    let family = archive_family(store)?;
    let mut options = ReadOptions::default();
    options.fill_cache(false);
    store
        .archive_db()
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

fn read_root_unavailable_count_optional(
    store: &impl DisplacedArchiveRead,
) -> Result<Option<u64>, CanonicalStoreError> {
    let Some(encoded) = read_exact_row(
        store,
        &ROOT_UNAVAILABLE_STATE_KEY,
        "displaced archive root-unavailable state read",
    )?
    else {
        return Ok(None);
    };
    decode_root_unavailable_state(&encoded).map(Some)
}

fn encode_root_unavailable_state(
    unavailable_count: u64,
) -> [u8; ROOT_UNAVAILABLE_STATE_VALUE_LENGTH] {
    let mut encoded = [0; ROOT_UNAVAILABLE_STATE_VALUE_LENGTH];
    encoded[0] = VERSION_ONE;
    encoded[1..9].copy_from_slice(&unavailable_count.to_be_bytes());
    encoded
}

fn decode_root_unavailable_state(encoded: &[u8]) -> Result<u64, CanonicalStoreError> {
    if encoded.len() != ROOT_UNAVAILABLE_STATE_VALUE_LENGTH || encoded.first() != Some(&VERSION_ONE)
    {
        return Err(CanonicalStoreError::displaced_archive(
            "archive root-unavailable state is not exact version-1 bytes",
        ));
    }
    Ok(u64::from_be_bytes(read_array(encoded, 1)?))
}

fn encode_root_index_prefix(root: FinalNoteCommitmentRoot) -> [u8; 33] {
    let mut prefix = [0; 33];
    prefix[0] = ROOT_INDEX_KEY_TAG;
    prefix[1..33].copy_from_slice(&root.as_bytes());
    prefix
}

fn encode_root_index_key(
    root: FinalNoteCommitmentRoot,
    position: ArchivePosition,
    protocol: ShieldedProtocol,
) -> [u8; ROOT_INDEX_KEY_LENGTH] {
    let mut encoded = [0; ROOT_INDEX_KEY_LENGTH];
    encoded[0] = ROOT_INDEX_KEY_TAG;
    encoded[1..33].copy_from_slice(&root.as_bytes());
    encoded[33..41].copy_from_slice(&(!position.event_sequence).to_be_bytes());
    encoded[41..45].copy_from_slice(&(!position.height.value()).to_be_bytes());
    let hash_bytes = position.block_hash.as_bytes();
    for (slot, byte) in encoded[45..77].iter_mut().zip(hash_bytes) {
        *slot = !byte;
    }
    encoded[77] = protocol.id();
    encoded
}

fn encode_root_index_value(
    displacement_epoch: ChainEpochId,
    block_time_unix_seconds: i64,
) -> [u8; ROOT_INDEX_VALUE_LENGTH] {
    let mut encoded = [0; ROOT_INDEX_VALUE_LENGTH];
    encoded[0] = VERSION_ONE;
    encoded[1..9].copy_from_slice(&displacement_epoch.value().to_be_bytes());
    encoded[9..17].copy_from_slice(&block_time_unix_seconds.to_be_bytes());
    encoded
}

fn decode_matching_root_index_row(
    key: &[u8],
    encoded_val: &[u8],
    expected_root: FinalNoteCommitmentRoot,
) -> Result<Option<DisplacedRootCandidate>, CanonicalStoreError> {
    if key.len() != ROOT_INDEX_KEY_LENGTH || key.first() != Some(&ROOT_INDEX_KEY_TAG) {
        return Err(CanonicalStoreError::displaced_archive(
            "archive root-index key is not exact version-1 bytes",
        ));
    }
    if key.get(1..33) != Some(&expected_root.as_bytes()) {
        return Ok(None);
    }
    if encoded_val.len() != ROOT_INDEX_VALUE_LENGTH || encoded_val.first() != Some(&VERSION_ONE) {
        return Err(CanonicalStoreError::displaced_archive(
            "archive root-index value is not exact version-1 bytes",
        ));
    }
    let event_sequence = !u64::from_be_bytes(read_array(key, 33)?);
    let height = BlockHeight::new(!u32::from_be_bytes(read_array(key, 41)?));
    let mut inverted_hash = read_array::<32>(key, 45)?;
    for byte in &mut inverted_hash {
        *byte = !*byte;
    }
    let protocol = ShieldedProtocol::from_id(key[77]).ok_or_else(|| {
        CanonicalStoreError::displaced_archive("archive root-index protocol id is invalid")
    })?;
    let displacement_epoch = ChainEpochId::new(u64::from_be_bytes(read_array(encoded_val, 1)?));
    if event_sequence == 0 || displacement_epoch.value() != event_sequence {
        return Err(CanonicalStoreError::displaced_archive(
            "archive root-index event sequence and epoch are invalid",
        ));
    }
    let block_time_unix_seconds = i64::from_be_bytes(read_array(encoded_val, 9)?);
    Ok(Some(DisplacedRootCandidate {
        block_id: BlockId::new(height, BlockHash::from_bytes(inverted_hash)),
        protocol,
        root: expected_root,
        displacement_event_sequence: event_sequence,
        displacement_epoch,
        block_time_unix_seconds,
    }))
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

fn decode_borrowed_archive_record(
    encoded: &[u8],
) -> Result<BorrowedArchiveRecord<'_>, CanonicalStoreError> {
    let mut decoder = ArchiveDecoder::new(encoded);
    decoder.require_version()?;
    let replay_length = usize::try_from(decoder.read_u32()?).map_err(|_| {
        CanonicalStoreError::displaced_archive("archive replay length exceeds usize")
    })?;
    let replay_bytes = decoder.read_bytes(replay_length)?;
    let record = BorrowedArchiveRecord {
        replay_bytes,
        displacement_epoch: ChainEpochId::new(decoder.read_u64()?),
        displaced_at: UnixTimestampMillis::new(decoder.read_u64()?),
        raw_block_bytes: decoder.read_optional_slice()?,
        final_note_commitment_roots: decoder.read_optional_roots()?,
    };
    decoder.require_end()?;
    Ok(record)
}

#[cfg(test)]
fn decode_archive_record(encoded: &[u8]) -> Result<ArchiveRecord, CanonicalStoreError> {
    let borrowed = decode_borrowed_archive_record(encoded)?;
    let record = ArchiveRecord {
        replay_bytes: borrowed.replay_bytes.to_vec(),
        displacement_epoch: borrowed.displacement_epoch,
        displaced_at: borrowed.displaced_at,
        raw_block_bytes: borrowed.raw_block_bytes.map(<[u8]>::to_vec),
        final_note_commitment_roots: borrowed.final_note_commitment_roots,
    };
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

    fn read_optional_slice(&mut self) -> Result<Option<&'a [u8]>, CanonicalStoreError> {
        if !self.read_presence()? {
            return Ok(None);
        }
        let length = usize::try_from(self.read_u32()?).map_err(|_| {
            CanonicalStoreError::displaced_archive("archive raw block length exceeds usize")
        })?;
        Ok(Some(self.read_bytes(length)?))
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
type ExistingTestArchive = (
    Vec<ArchiveEventContext>,
    Vec<(ArchivePosition, ArchiveRecord)>,
    Vec<Vec<u8>>,
);

#[cfg(test)]
pub(super) fn reindex_test_archive_with_roots(
    store: &RocksDbCanonicalStore,
    roots_by_block: &[(BlockId, BlockFinalNoteCommitmentRoots)],
) -> Result<(), CanonicalStoreError> {
    let (mut contexts, records, all_keys) = read_existing_test_archive(store)?;
    contexts.sort_by_key(|ctx| ctx.event_sequence);
    let family = archive_family(store)?;
    let mut clear_batch = WriteBatch::default();
    for key in all_keys {
        clear_batch.delete_cf(&family, key);
    }
    store
        .bounded_open
        .db
        .write(&clear_batch)
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "clear test displaced archive",
            source,
        })?;
    for ctx in contexts {
        let mut blocks: Vec<CanonicalDisplacedBlock> = records
            .iter()
            .filter(|(pos, _)| pos.event_sequence == ctx.event_sequence)
            .map(|(pos, rec)| {
                let block_id = BlockId::new(pos.height, pos.block_hash);
                let roots = roots_by_block
                    .iter()
                    .find_map(|(id, roots)| (*id == block_id).then_some(*roots));
                CanonicalDisplacedBlock {
                    block_id,
                    replay_bytes: rec.replay_bytes.clone(),
                    raw_block_bytes: rec.raw_block_bytes.clone(),
                    final_note_commitment_roots: roots,
                }
            })
            .collect();
        blocks.sort_by_key(|block| block.block_id.height.value());
        let prepared = PreparedDisplacedArchiveWrite::new(
            store,
            DisplacedArchiveEvent {
                event_sequence: ctx.event_sequence,
                displacement_epoch: ctx.displacement_epoch,
                reverted_range: ctx.reverted_range,
                displaced_at: ctx.displaced_at,
            },
            blocks,
        )?;
        let mut write_batch = WriteBatch::default();
        prepared.put_into(store, &mut write_batch)?;
        store
            .bounded_open
            .db
            .write(&write_batch)
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "write reindexed test displaced archive",
                source,
            })?;
    }
    store
        .bounded_open
        .db
        .flush_wal(true)
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "flush reindexed test displaced archive wal",
            source,
        })
}

#[cfg(test)]
fn read_existing_test_archive(
    store: &RocksDbCanonicalStore,
) -> Result<ExistingTestArchive, CanonicalStoreError> {
    let family = archive_family(store)?;
    let mut contexts = Vec::new();
    let mut records = Vec::new();
    let mut all_keys = Vec::new();
    for entry in store
        .bounded_open
        .db
        .iterator_cf(&family, rust_rocksdb::IteratorMode::Start)
    {
        let (key, encoded) = entry.map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "iterate existing test displaced archive",
            source,
        })?;
        all_keys.push(key.to_vec());
        match key.first().copied() {
            Some(EVENT_CONTEXT_KEY_TAG) => {
                let seq = u64::from_be_bytes(read_array(&key, 1)?);
                contexts.push(decode_event_context(&encoded, seq)?);
            }
            Some(ORDER_KEY_TAG) => {
                records.push((decode_order_key(&key)?, decode_archive_record(&encoded)?));
            }
            _ => {}
        }
    }
    Ok((contexts, records, all_keys))
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

    #[test]
    fn root_index_keys_preserve_newest_order_and_round_trip() -> Result<(), CanonicalStoreError> {
        let root = FinalNoteCommitmentRoot::from_bytes([0x5a; 32]);
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
        let key_newer = encode_root_index_key(root, newer, ShieldedProtocol::Sapling);
        let key_older_orchard = encode_root_index_key(root, older, ShieldedProtocol::Orchard);
        let key_older_sapling = encode_root_index_key(root, older, ShieldedProtocol::Sapling);
        assert!(key_newer < key_older_sapling);
        assert!(key_older_sapling < key_older_orchard);

        let encoded_val = encode_root_index_value(ChainEpochId::new(9), 1_750_000_000);
        let candidate = decode_matching_root_index_row(&key_newer, &encoded_val, root)?
            .ok_or_else(|| CanonicalStoreError::displaced_archive("missing decoded row"))?;
        assert_eq!(candidate.block_id.height, BlockHeight::new(99));
        assert_eq!(candidate.block_id.hash, BlockHash::from_bytes([2; 32]));
        assert_eq!(candidate.protocol, ShieldedProtocol::Sapling);
        assert_eq!(candidate.block_time_unix_seconds, 1_750_000_000);
        assert_eq!(
            decode_root_unavailable_state(&encode_root_unavailable_state(7))?,
            7
        );
        Ok(())
    }
}
