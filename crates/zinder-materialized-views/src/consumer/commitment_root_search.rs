//! Reverse index for final note-commitment-tree roots.
//!
//! One block can contribute Sapling, Orchard, and Ironwood roots, and the same
//! root can repeat across many blocks when a pool receives no commitments. The
//! primary key therefore retains protocol, height, and block hash. A per-height
//! side index makes reorg deletion deterministic without re-reading canonical
//! facts from the superseded branch.

use rust_rocksdb::WriteBatch;
use zinder_core::wire::{
    HEIGHT_KEY_LEN, decode_height_key_descending, decode_internal_block_hash,
    encode_height_key_ascending, encode_height_key_descending, encode_internal_block_hash,
};
use zinder_core::{
    BlockFinalNoteCommitmentRoots, BlockHash, BlockHeight, FinalNoteCommitmentRoot,
    ShieldedProtocol,
};

use crate::consumer::{
    BlockCommitContext, BlockKeyedConsumer, MaterializedViewConsumerCtx,
    MaterializedViewConsumerError, MaterializedViewConsumerName, MaterializedViewConsumerSchema,
};
use crate::{
    MaterializedViewStore, MaterializedViewStoreColumnFamily, MaterializedViewStoreError,
    MaterializedViewStoreReadSnapshot,
};

/// Root-keyed canonical block matches.
pub const COMMITMENT_ROOT_SEARCH_COLUMN_FAMILY: &str = "commitment_root_search";
/// Per-height list of root keys used for deterministic reorg deletion.
pub const COMMITMENT_ROOT_SEARCH_INDEX_COLUMN_FAMILY: &str = "commitment_root_search_index";
/// Contiguous historical backfill range for negative-result interpretation.
pub const COMMITMENT_ROOT_SEARCH_COVERAGE_COLUMN_FAMILY: &str = "commitment_root_search_coverage";

/// Column families the root-search consumer owns.
pub const COMMITMENT_ROOT_SEARCH_COLUMN_FAMILIES: &[&str] = &[
    COMMITMENT_ROOT_SEARCH_COLUMN_FAMILY,
    COMMITMENT_ROOT_SEARCH_INDEX_COLUMN_FAMILY,
    COMMITMENT_ROOT_SEARCH_COVERAGE_COLUMN_FAMILY,
];

/// Stable consumer identity persisted in materialized-view metadata and cursor rows.
pub const COMMITMENT_ROOT_SEARCH_CONSUMER_NAME: MaterializedViewConsumerName =
    MaterializedViewConsumerName::from_static("commitment_root_search");

/// Initial on-disk schema for final-root lookup and coverage rows.
pub const COMMITMENT_ROOT_SEARCH_SCHEMA: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(
        COMMITMENT_ROOT_SEARCH_CONSUMER_NAME,
        1,
        COMMITMENT_ROOT_SEARCH_COLUMN_FAMILIES,
    );

const ROOT_LEN: usize = 32;
const PROTOCOL_LEN: usize = 1;
const BLOCK_HASH_LEN: usize = 32;
const BLOCK_TIME_LEN: usize = 8;
const ROOT_KEY_LEN: usize = ROOT_LEN + PROTOCOL_LEN + HEIGHT_KEY_LEN + BLOCK_HASH_LEN;
const ROOT_PREFIX_LEN: usize = ROOT_LEN;
const HEIGHT_RANGE: std::ops::Range<usize> = ROOT_LEN..ROOT_LEN + HEIGHT_KEY_LEN;
const PROTOCOL_OFFSET: usize = ROOT_LEN + HEIGHT_KEY_LEN;
const BLOCK_HASH_RANGE: std::ops::Range<usize> =
    ROOT_LEN + HEIGHT_KEY_LEN + PROTOCOL_LEN..ROOT_KEY_LEN;
const COVERAGE_KEY: &[u8] = b"canonical_backfill";
const COVERAGE_VALUE_LEN: usize = HEIGHT_KEY_LEN * 2;

/// One reverse-index candidate before canonical-chain validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitmentRootIndexEntry {
    /// Shielded protocol whose final tree root matched.
    pub protocol: ShieldedProtocol,
    /// Block height encoded by the reverse index key.
    pub block_height: BlockHeight,
    /// Block hash observed when the materialized-view row was written.
    pub block_hash: BlockHash,
    /// Block timestamp as Unix seconds.
    pub block_time_unix_seconds: i64,
}

/// Contiguous canonical history covered by an ingest-owned backfill.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitmentRootBackfillCoverage {
    /// First root-bearing height included by the backfill.
    pub complete_from_height: BlockHeight,
    /// Last contiguous height included by the backfill.
    pub complete_through_height: BlockHeight,
}

impl CommitmentRootBackfillCoverage {
    /// Creates one non-empty contiguous coverage range.
    #[must_use]
    pub const fn new(
        complete_from_height: BlockHeight,
        complete_through_height: BlockHeight,
    ) -> Self {
        Self {
            complete_from_height,
            complete_through_height,
        }
    }
}

/// Materializes and searches final note-commitment-tree roots.
#[derive(Default)]
pub struct CommitmentRootSearchConsumer;

impl CommitmentRootSearchConsumer {
    /// Builds the consumer.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Returns newest-first candidates for `root` across all shielded pools.
    ///
    /// Callers must validate each candidate's block identity against a pinned
    /// canonical reader. A historical enrichment can race a later reorg after
    /// its canonical write; retaining the hash makes that race fail closed.
    pub fn search(
        store: &MaterializedViewStore,
        root: FinalNoteCommitmentRoot,
        max_matches: usize,
    ) -> Result<Vec<CommitmentRootIndexEntry>, MaterializedViewStoreError> {
        let snapshot = store.read_snapshot();
        Self::search_snapshot_page(&snapshot, root, None, max_matches)
    }

    /// Reads one bounded newest-first page of candidates for `root` from a
    /// consistent read snapshot, strictly after `after` when provided.
    pub fn search_snapshot_page(
        snapshot: &MaterializedViewStoreReadSnapshot<'_>,
        root: FinalNoteCommitmentRoot,
        after: Option<&CommitmentRootIndexEntry>,
        page_limit: usize,
    ) -> Result<Vec<CommitmentRootIndexEntry>, MaterializedViewStoreError> {
        if page_limit == 0 {
            return Ok(Vec::new());
        }
        let cursor_key = after.map(|cursor| {
            encode_root_key(
                root,
                cursor.protocol,
                cursor.block_height,
                cursor.block_hash,
            )
        });
        let start_key = cursor_key.unwrap_or_else(|| {
            let mut key = [0_u8; ROOT_KEY_LEN];
            key[..ROOT_PREFIX_LEN].copy_from_slice(&root.as_bytes());
            key
        });
        let mut end_key = [0xff_u8; ROOT_KEY_LEN];
        end_key[..ROOT_PREFIX_LEN].copy_from_slice(&root.as_bytes());
        let fetch_cap = if cursor_key.is_some() {
            page_limit.saturating_add(1)
        } else {
            page_limit
        };
        let raw_entries = snapshot.range_iterate_consumer(
            COMMITMENT_ROOT_SEARCH_COLUMN_FAMILY,
            &start_key,
            &end_key,
            fetch_cap,
        )?;
        let mut out = Vec::with_capacity(raw_entries.len().min(page_limit));
        for (key, payload) in raw_entries {
            if cursor_key
                .as_ref()
                .is_some_and(|cursor| key.as_slice() == cursor.as_slice())
            {
                continue;
            }
            out.push(decode_index_entry(&key, &payload)?);
            if out.len() >= page_limit {
                break;
            }
        }
        Ok(out)
    }

    /// Reads the contiguous historical range completed by backfill.
    pub fn backfill_coverage(
        store: &MaterializedViewStore,
    ) -> Result<Option<CommitmentRootBackfillCoverage>, MaterializedViewStoreError> {
        let snapshot = store.read_snapshot();
        Self::backfill_coverage_snapshot(&snapshot)
    }

    /// Reads the contiguous historical backfill range from one read snapshot.
    pub fn backfill_coverage_snapshot(
        snapshot: &MaterializedViewStoreReadSnapshot<'_>,
    ) -> Result<Option<CommitmentRootBackfillCoverage>, MaterializedViewStoreError> {
        let Some(payload) =
            snapshot.get_consumer(COMMITMENT_ROOT_SEARCH_COVERAGE_COLUMN_FAMILY, COVERAGE_KEY)?
        else {
            return Ok(None);
        };
        decode_coverage(&payload).map(Some)
    }

    /// Atomically applies an ordered historical batch and advances coverage.
    ///
    /// This does not advance the chain-event cursor: canonical enrichment and
    /// materialized-view backfill have their own resumable progress contract.
    pub fn write_backfill_batch(
        &mut self,
        store: &MaterializedViewStore,
        blocks: &[BlockCommitContext],
        next_coverage: CommitmentRootBackfillCoverage,
    ) -> Result<(), MaterializedViewConsumerError> {
        validate_backfill_batch(store, blocks, next_coverage)?;
        let mut batch = WriteBatch::default();
        let mut ctx = MaterializedViewConsumerCtx {
            store,
            batch: &mut batch,
        };
        for block in blocks {
            self.apply_block(block, &mut ctx)?;
        }
        let coverage_cf =
            store.consumer_column_family(COMMITMENT_ROOT_SEARCH_COVERAGE_COLUMN_FAMILY)?;
        ctx.batch
            .put_cf(&coverage_cf, COVERAGE_KEY, encode_coverage(next_coverage));
        store.write_consumer_batch(COMMITMENT_ROOT_SEARCH_SCHEMA.name, ctx.batch)?;
        Ok(())
    }
}

impl BlockKeyedConsumer for CommitmentRootSearchConsumer {
    fn name(&self) -> MaterializedViewConsumerName {
        COMMITMENT_ROOT_SEARCH_CONSUMER_NAME
    }

    fn apply_block(
        &mut self,
        block: &BlockCommitContext,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        let root_cf = ctx
            .store
            .consumer_column_family(COMMITMENT_ROOT_SEARCH_COLUMN_FAMILY)?;
        let index_cf = ctx
            .store
            .consumer_column_family(COMMITMENT_ROOT_SEARCH_INDEX_COLUMN_FAMILY)?;
        let mut index_payload = Vec::with_capacity(3 * ROOT_KEY_LEN);
        if let Some(roots) = block.final_note_commitment_roots {
            validate_block_roots(block, roots)?;
            for (protocol, root) in present_roots(roots) {
                let key = encode_root_key(root, protocol, block.height, block.block_hash);
                ctx.batch
                    .put_cf(&root_cf, key, block.block_time_unix_seconds.to_be_bytes());
                index_payload.extend_from_slice(&key);
            }
        }
        ctx.batch.put_cf(
            &index_cf,
            encode_height_key_ascending(block.height),
            index_payload,
        );
        Ok(())
    }

    fn revert_block(
        &mut self,
        height: BlockHeight,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        let index_key = encode_height_key_ascending(height);
        let Some(index_payload) = ctx
            .store
            .get_consumer(COMMITMENT_ROOT_SEARCH_INDEX_COLUMN_FAMILY, &index_key)?
        else {
            return Ok(());
        };
        if index_payload.len() % ROOT_KEY_LEN != 0 {
            return Err(Box::new(CommitmentRootSearchConsumerError::IndexLength {
                height: height.value(),
                bytes: index_payload.len(),
            }));
        }
        let root_cf = ctx
            .store
            .consumer_column_family(COMMITMENT_ROOT_SEARCH_COLUMN_FAMILY)?;
        let index_cf = ctx
            .store
            .consumer_column_family(COMMITMENT_ROOT_SEARCH_INDEX_COLUMN_FAMILY)?;
        for key in index_payload.as_chunks::<ROOT_KEY_LEN>().0 {
            ctx.batch.delete_cf(&root_cf, key);
        }
        ctx.batch.delete_cf(&index_cf, index_key);
        Ok(())
    }
}

fn present_roots(
    roots: BlockFinalNoteCommitmentRoots,
) -> impl Iterator<Item = (ShieldedProtocol, FinalNoteCommitmentRoot)> {
    [
        (ShieldedProtocol::Sapling, roots.sapling),
        (ShieldedProtocol::Orchard, roots.orchard),
        (ShieldedProtocol::Ironwood, roots.ironwood),
    ]
    .into_iter()
    .filter_map(|(protocol, root)| root.map(|root| (protocol, root)))
}

fn validate_block_roots(
    block: &BlockCommitContext,
    roots: BlockFinalNoteCommitmentRoots,
) -> Result<(), MaterializedViewConsumerError> {
    if roots.height == block.height && roots.block_hash == block.block_hash {
        return Ok(());
    }
    Err(Box::new(
        CommitmentRootSearchConsumerError::BlockIdentityMismatch {
            block_height: block.height.value(),
            roots_height: roots.height.value(),
        },
    ))
}

fn encode_root_key(
    root: FinalNoteCommitmentRoot,
    protocol: ShieldedProtocol,
    height: BlockHeight,
    block_hash: BlockHash,
) -> [u8; ROOT_KEY_LEN] {
    let mut key = [0_u8; ROOT_KEY_LEN];
    key[..ROOT_LEN].copy_from_slice(&root.as_bytes());
    key[PROTOCOL_OFFSET] = protocol.id();
    key[HEIGHT_RANGE].copy_from_slice(&encode_height_key_descending(height));
    key[BLOCK_HASH_RANGE].copy_from_slice(&encode_internal_block_hash(block_hash));
    key
}

fn decode_index_entry(
    key: &[u8],
    payload: &[u8],
) -> Result<CommitmentRootIndexEntry, MaterializedViewStoreError> {
    if key.len() != ROOT_KEY_LEN {
        return Err(decode_error("commitment-root index key length is invalid"));
    }
    if payload.len() != BLOCK_TIME_LEN {
        return Err(decode_error("commitment-root block-time length is invalid"));
    }
    let protocol = ShieldedProtocol::from_id(key[PROTOCOL_OFFSET])
        .ok_or_else(|| decode_error("commitment-root protocol id is invalid"))?;
    let block_height = decode_height_key_descending(&key[HEIGHT_RANGE])
        .map_err(|error| decode_error(error.to_string()))?;
    let block_hash = decode_internal_block_hash(&key[BLOCK_HASH_RANGE])
        .map_err(|error| decode_error(error.to_string()))?;
    let block_time_bytes: [u8; BLOCK_TIME_LEN] = payload
        .try_into()
        .map_err(|_| decode_error("commitment-root block-time length is invalid"))?;
    Ok(CommitmentRootIndexEntry {
        protocol,
        block_height,
        block_hash,
        block_time_unix_seconds: i64::from_be_bytes(block_time_bytes),
    })
}

fn encode_coverage(coverage: CommitmentRootBackfillCoverage) -> [u8; COVERAGE_VALUE_LEN] {
    let mut payload = [0_u8; COVERAGE_VALUE_LEN];
    payload[..HEIGHT_KEY_LEN]
        .copy_from_slice(&encode_height_key_ascending(coverage.complete_from_height));
    payload[HEIGHT_KEY_LEN..].copy_from_slice(&encode_height_key_ascending(
        coverage.complete_through_height,
    ));
    payload
}

fn decode_coverage(
    payload: &[u8],
) -> Result<CommitmentRootBackfillCoverage, MaterializedViewStoreError> {
    if payload.len() != COVERAGE_VALUE_LEN {
        return Err(decode_error("commitment-root coverage length is invalid"));
    }
    let complete_from_height =
        zinder_core::wire::decode_height_key_ascending(&payload[..HEIGHT_KEY_LEN])
            .map_err(|error| decode_error(error.to_string()))?;
    let complete_through_height =
        zinder_core::wire::decode_height_key_ascending(&payload[HEIGHT_KEY_LEN..])
            .map_err(|error| decode_error(error.to_string()))?;
    Ok(CommitmentRootBackfillCoverage::new(
        complete_from_height,
        complete_through_height,
    ))
}

fn validate_backfill_batch(
    store: &MaterializedViewStore,
    blocks: &[BlockCommitContext],
    next_coverage: CommitmentRootBackfillCoverage,
) -> Result<(), MaterializedViewConsumerError> {
    let Some(first) = blocks.first() else {
        return Err(Box::new(CommitmentRootSearchConsumerError::EmptyBackfill));
    };
    let last = blocks
        .last()
        .ok_or(CommitmentRootSearchConsumerError::EmptyBackfill)?;
    if last.height != next_coverage.complete_through_height {
        return Err(Box::new(
            CommitmentRootSearchConsumerError::CoverageDiscontinuous,
        ));
    }
    for pair in blocks.windows(2) {
        if pair[0].height.next() != Some(pair[1].height) {
            return Err(Box::new(
                CommitmentRootSearchConsumerError::CoverageDiscontinuous,
            ));
        }
    }
    match CommitmentRootSearchConsumer::backfill_coverage(store)? {
        Some(existing)
            if existing.complete_from_height == next_coverage.complete_from_height
                && existing.complete_through_height.next() == Some(first.height) => {}
        None if first.height == next_coverage.complete_from_height => {}
        Some(_) | None => {
            return Err(Box::new(
                CommitmentRootSearchConsumerError::CoverageDiscontinuous,
            ));
        }
    }
    Ok(())
}

fn decode_error(reason: impl Into<String>) -> MaterializedViewStoreError {
    MaterializedViewStoreError::Decode {
        column_family: MaterializedViewStoreColumnFamily::ConsumerMetadata,
        reason: reason.into(),
    }
}

/// Root-search materialized-view failures.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CommitmentRootSearchConsumerError {
    /// Root identity and enclosing block context disagree.
    #[error(
        "final note-commitment roots at height {roots_height} do not match block context {block_height}"
    )]
    BlockIdentityMismatch {
        /// Height of the block context.
        block_height: u32,
        /// Height carried by the roots artifact.
        roots_height: u32,
    },
    /// Per-height rollback payload is not a whole number of root keys.
    #[error("commitment-root index at height {height} has invalid length {bytes}")]
    IndexLength {
        /// Indexed block height.
        height: u32,
        /// Malformed payload length.
        bytes: usize,
    },
    /// A backfill call contained no blocks.
    #[error("commitment-root backfill batch cannot be empty")]
    EmptyBackfill,
    /// Backfill blocks or requested coverage do not extend one contiguous range.
    #[error("commitment-root backfill coverage must advance contiguously")]
    CoverageDiscontinuous,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use rust_rocksdb::WriteBatch;
    use tempfile::tempdir;
    use zinder_core::{
        BlockFinalNoteCommitmentRoots, BlockHash, BlockHeight, FinalNoteCommitmentRoot,
        ShieldedProtocol,
    };
    use zinder_store::RocksDbResourceBudget;

    use super::{
        COMMITMENT_ROOT_SEARCH_SCHEMA, CommitmentRootBackfillCoverage, CommitmentRootSearchConsumer,
    };
    use crate::consumer::{
        BlockCommitContext, BlockCommitInput, BlockKeyedConsumer, MaterializedViewConsumerCtx,
        TransparentSpendFacts,
    };
    use crate::{MaterializedViewStore, MaterializedViewStoreOptions};

    type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

    fn root(seed: u8) -> FinalNoteCommitmentRoot {
        FinalNoteCommitmentRoot::from_bytes([seed; 32])
    }

    fn block(height: u32, roots: [Option<FinalNoteCommitmentRoot>; 3]) -> BlockCommitContext {
        let block_height = BlockHeight::new(height);
        let block_hash = BlockHash::from_bytes([height.to_le_bytes()[0]; 32]);
        BlockCommitContext::new(
            BlockCommitInput {
                height: block_height,
                block_hash,
                previous_block_hash: BlockHash::from_bytes(
                    [height.saturating_sub(1).to_le_bytes()[0]; 32],
                ),
                block_time_unix_seconds: 1_700_000_000 + i64::from(height),
                block_size_bytes: 0,
                transactions: Vec::new(),
                final_note_commitment_roots: Some(BlockFinalNoteCommitmentRoots::new(
                    block_height,
                    block_hash,
                    roots[0],
                    roots[1],
                    roots[2],
                )),
            },
            TransparentSpendFacts::Offline,
        )
    }

    fn open_store() -> TestResult<(tempfile::TempDir, MaterializedViewStore)> {
        let tempdir = tempdir()?;
        let store = MaterializedViewStore::open(
            tempdir.path(),
            MaterializedViewStoreOptions {
                consumers: &[COMMITMENT_ROOT_SEARCH_SCHEMA],
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
                sync_writes: false,
                reorg_policy: None,
            },
        )?;
        Ok((tempdir, store))
    }

    fn apply_block(
        store: &MaterializedViewStore,
        consumer: &mut CommitmentRootSearchConsumer,
        block: &BlockCommitContext,
    ) -> TestResult {
        let mut batch = WriteBatch::default();
        let mut context = MaterializedViewConsumerCtx {
            store,
            batch: &mut batch,
        };
        consumer.apply_block(block, &mut context)?;
        store.write_batch(&batch)?;
        Ok(())
    }

    fn revert_block(
        store: &MaterializedViewStore,
        consumer: &mut CommitmentRootSearchConsumer,
        height: BlockHeight,
    ) -> TestResult {
        let mut batch = WriteBatch::default();
        let mut context = MaterializedViewConsumerCtx {
            store,
            batch: &mut batch,
        };
        consumer.revert_block(height, &mut context)?;
        store.write_batch(&batch)?;
        Ok(())
    }

    #[test]
    fn repeated_roots_are_returned_newest_first() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = CommitmentRootSearchConsumer::new();
        apply_block(
            &store,
            &mut consumer,
            &block(100, [Some(root(1)), None, None]),
        )?;
        apply_block(
            &store,
            &mut consumer,
            &block(101, [Some(root(1)), None, None]),
        )?;

        let matches = CommitmentRootSearchConsumer::search(&store, root(1), 10)?;
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].block_height, BlockHeight::new(101));
        assert_eq!(matches[1].block_height, BlockHeight::new(100));
        Ok(())
    }

    #[test]
    fn one_root_can_match_all_three_protocols() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = CommitmentRootSearchConsumer::new();
        apply_block(
            &store,
            &mut consumer,
            &block(100, [Some(root(1)), Some(root(1)), Some(root(1))]),
        )?;

        let matches = CommitmentRootSearchConsumer::search(&store, root(1), 10)?;
        assert_eq!(matches.len(), 3);
        assert!(
            matches
                .iter()
                .any(|entry| entry.protocol == ShieldedProtocol::Sapling)
        );
        assert!(
            matches
                .iter()
                .any(|entry| entry.protocol == ShieldedProtocol::Orchard)
        );
        assert!(
            matches
                .iter()
                .any(|entry| entry.protocol == ShieldedProtocol::Ironwood)
        );
        Ok(())
    }

    #[test]
    fn reorg_rewind_deletes_every_root_for_the_height() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = CommitmentRootSearchConsumer::new();
        apply_block(
            &store,
            &mut consumer,
            &block(100, [Some(root(1)), Some(root(2)), Some(root(3))]),
        )?;

        revert_block(&store, &mut consumer, BlockHeight::new(100))?;

        assert_eq!(
            CommitmentRootSearchConsumer::search(&store, root(1), 10)?,
            []
        );
        assert_eq!(
            CommitmentRootSearchConsumer::search(&store, root(2), 10)?,
            []
        );
        assert_eq!(
            CommitmentRootSearchConsumer::search(&store, root(3), 10)?,
            []
        );
        Ok(())
    }

    #[test]
    fn backfill_coverage_advances_only_contiguously() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = CommitmentRootSearchConsumer::new();
        consumer.write_backfill_batch(
            &store,
            &[block(100, [Some(root(1)), None, None])],
            CommitmentRootBackfillCoverage::new(BlockHeight::new(100), BlockHeight::new(100)),
        )?;
        consumer.write_backfill_batch(
            &store,
            &[block(101, [Some(root(2)), None, None])],
            CommitmentRootBackfillCoverage::new(BlockHeight::new(100), BlockHeight::new(101)),
        )?;

        assert_eq!(
            CommitmentRootSearchConsumer::backfill_coverage(&store)?,
            Some(CommitmentRootBackfillCoverage::new(
                BlockHeight::new(100),
                BlockHeight::new(101),
            ))
        );
        let discontinuous = consumer.write_backfill_batch(
            &store,
            &[block(103, [Some(root(3)), None, None])],
            CommitmentRootBackfillCoverage::new(BlockHeight::new(100), BlockHeight::new(103)),
        );
        assert!(discontinuous.is_err());
        Ok(())
    }

    #[test]
    fn snapshot_paging_walks_repeated_roots_without_gaps_or_duplicates() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = CommitmentRootSearchConsumer::new();
        for height in 100..=104 {
            apply_block(
                &store,
                &mut consumer,
                &block(height, [Some(root(7)), Some(root(7)), None]),
            )?;
        }

        let snapshot = store.read_snapshot();
        let page1 =
            CommitmentRootSearchConsumer::search_snapshot_page(&snapshot, root(7), None, 4)?;
        let page2 = CommitmentRootSearchConsumer::search_snapshot_page(
            &snapshot,
            root(7),
            page1.last(),
            4,
        )?;
        let page3 = CommitmentRootSearchConsumer::search_snapshot_page(
            &snapshot,
            root(7),
            page2.last(),
            4,
        )?;
        drop(snapshot);

        assert_eq!(page1.len(), 4);
        assert_eq!(page2.len(), 4);
        assert_eq!(page3.len(), 2);
        let heights: Vec<u32> = page1
            .into_iter()
            .chain(page2)
            .chain(page3)
            .map(|entry| entry.block_height.value())
            .collect();
        assert_eq!(
            heights,
            vec![104, 104, 103, 103, 102, 102, 101, 101, 100, 100]
        );
        Ok(())
    }
}
