//! Verifying stored tree-state checkpoints in full.
//!
//! A stored tree-state read serves a checkpoint row after checking only its
//! framing, so a served row rests on the checks made when it was written and
//! on `RocksDB` block checksums. Verification reads settled rows again and
//! checks each one as a writer does: every frontier parses with the official
//! codec, is its canonical encoding, and derives the root stored beside it,
//! and the row carries a frontier for exactly the pools active at its height.
//! A caller repeats bounded verifications over settled history, so a damaged
//! row is found within one pass over it.
//!
//! Verification keeps the blocks it reads out of the block cache, so a later
//! pass reads them from disk again and its checksum check sees damage done
//! since. A
//! block that fails that check is damage, reported like a row that fails a
//! row check. A table file the primary has since deleted is not damage: it is
//! an ordinary store error that clears once the secondary catches up.

use std::num::NonZeroU32;

use rust_rocksdb::{Direction, ErrorKind, IteratorMode, ReadOptions};
use thiserror::Error;
use zinder_core::{
    BlockHeight, NetworkUpgradeActivations, ShieldedProtocol,
    wire::{ascending_height_key_scan_start, decode_height_key_ascending},
};

use super::{
    CanonicalStoreError, RocksDbCanonicalSecondary, RocksDbCanonicalStore,
    block_load::decode_tree_state_checkpoint, frontier_presence_mismatch,
    publication::column_family, reader::CanonicalServingRead,
    rocksdb::TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
};

/// What one bounded verification of stored tree-state checkpoints found.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeStateCheckpointVerification {
    /// Rows that passed every check.
    pub verified_rows: u32,
    /// The first row that failed a check. Verification stops at it.
    pub mismatch: Option<TreeStateCheckpointMismatch>,
    /// Height the next verification continues from: the row that failed, or
    /// the height above the last row verified.
    pub next_from: BlockHeight,
    /// Whether every row stored from the starting height through the settled
    /// tip passed, so the next verification needs to start over.
    pub reached_settled_tip: bool,
}

/// A stored tree-state checkpoint that failed verification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeStateCheckpointMismatch {
    /// Height the row is stored at. For a key that is not a height, or a
    /// table block that fails its checksum, the first height the damage may
    /// cover: the height above the last row verified.
    pub height: BlockHeight,
    /// Pool whose frontier failed, or `None` when the row failed outside any
    /// one frontier, such as a value that ends inside its block time.
    pub pool: Option<ShieldedProtocol>,
    /// The check that failed.
    pub reason: String,
}

/// A verification that stopped on a store error before it found a mismatch
/// or reached the settled tip.
///
/// The rows it verified before the error passed every check, so the next
/// verification continues from [`Self::next_from`] instead of checking them
/// again.
#[derive(Debug, Error)]
#[error("tree-state verification stopped after {verified_rows} intact rows: {source}")]
pub struct TreeStateVerificationError {
    /// Rows that passed every check before the error.
    pub verified_rows: u32,
    /// Height the next verification continues from.
    pub next_from: BlockHeight,
    /// The store error.
    #[source]
    pub source: CanonicalStoreError,
}

impl RocksDbCanonicalSecondary {
    /// Verifies up to `max_rows` stored tree-state checkpoints, in height
    /// order, from `from` through the settled tip.
    ///
    /// Each row must decode in full: every frontier parses with the official
    /// codec, is its canonical encoding, and derives the root stored beside
    /// it. It must also carry a frontier for exactly the pools that
    /// `network_upgrade_activations` activates at or below its height. The
    /// first row that fails is reported in
    /// [`TreeStateCheckpointVerification::mismatch`], as is a key that is not
    /// a height and a table block that fails its checksum. An error means the
    /// store could not be read or does not admit the activation table.
    pub fn verify_tree_state_checkpoints(
        &self,
        from: BlockHeight,
        max_rows: NonZeroU32,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<TreeStateCheckpointVerification, TreeStateVerificationError> {
        verify_tree_state_checkpoints(self, from, max_rows, network_upgrade_activations)
    }
}

impl RocksDbCanonicalStore {
    /// Verifies up to `max_rows` stored tree-state checkpoints from `from`
    /// through the settled tip; see
    /// [`RocksDbCanonicalSecondary::verify_tree_state_checkpoints`].
    pub fn verify_tree_state_checkpoints(
        &self,
        from: BlockHeight,
        max_rows: NonZeroU32,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<TreeStateCheckpointVerification, TreeStateVerificationError> {
        verify_tree_state_checkpoints(self, from, max_rows, network_upgrade_activations)
    }
}

fn verify_tree_state_checkpoints(
    store: &impl CanonicalServingRead,
    from: BlockHeight,
    max_rows: NonZeroU32,
    network_upgrade_activations: &NetworkUpgradeActivations,
) -> Result<TreeStateCheckpointVerification, TreeStateVerificationError> {
    let mut verification = TreeStateCheckpointVerification {
        verified_rows: 0,
        mismatch: None,
        next_from: from,
        reached_settled_tip: false,
    };
    let interrupted =
        |verification: &TreeStateCheckpointVerification, source| TreeStateVerificationError {
            verified_rows: verification.verified_rows,
            next_from: verification.next_from,
            source,
        };
    let settled_height = store
        .serving_ready_evidence()
        .sequence_checkpoint
        .through()
        .height;
    let db = &store.serving_open().db;
    if !store.serving_admits_activations_through(network_upgrade_activations, settled_height) {
        return Err(interrupted(
            &verification,
            CanonicalStoreError::admission(
                db.path(),
                "tree-state verification activation table differs from the admitted schedule at or below the settled tip",
            ),
        ));
    }
    let family = column_family(db, TREE_STATE_CHECKPOINT_COLUMN_FAMILY)
        .map_err(|source| interrupted(&verification, source))?;
    let mut read_options = ReadOptions::default();
    read_options.fill_cache(false);
    read_options.set_verify_checksums(true);
    let start = ascending_height_key_scan_start(from);
    for entry in db.iterator_cf_opt(
        &family,
        read_options,
        IteratorMode::From(&start, Direction::Forward),
    ) {
        let (height, encoded) =
            match scanned_entry(entry).map_err(|source| interrupted(&verification, source))? {
                ScannedEntry::Row { height, encoded } => (height, encoded),
                ScannedEntry::Unreadable { reason } => {
                    verification.mismatch = Some(TreeStateCheckpointMismatch {
                        height: verification.next_from,
                        pool: None,
                        reason,
                    });
                    return Ok(verification);
                }
            };
        if height > settled_height {
            break;
        }
        if verification.verified_rows >= max_rows.get() {
            return Ok(verification);
        }
        if let Some(mismatch) = verify_row(network_upgrade_activations, height, &encoded) {
            verification.mismatch = Some(mismatch);
            verification.next_from = height;
            return Ok(verification);
        }
        verification.verified_rows += 1;
        verification.next_from = height.next().ok_or_else(|| {
            interrupted(
                &verification,
                CanonicalStoreError::publication("tree-state verification height exceeds u32::MAX"),
            )
        })?;
    }
    verification.reached_settled_tip = true;
    Ok(verification)
}

/// One entry a verification scan read.
enum ScannedEntry {
    /// A row stored at a height.
    Row {
        height: BlockHeight,
        encoded: Box<[u8]>,
    },
    /// Damage that keeps the entry from being checked as a row: a key that
    /// is not a height, or a table block that fails its checksum.
    Unreadable { reason: String },
}

/// A key and value as a `RocksDB` iterator yields them.
type IteratedEntry = (Box<[u8]>, Box<[u8]>);

/// Classifies one entry of the scan, keeping a missing table file and every
/// other store failure apart from damage.
fn scanned_entry(
    entry: Result<IteratedEntry, rust_rocksdb::Error>,
) -> Result<ScannedEntry, CanonicalStoreError> {
    match entry {
        Ok((key, encoded)) => Ok(decode_height_key_ascending(&key).map_or_else(
            |_| ScannedEntry::Unreadable {
                reason: "a tree_state_checkpoint key at or above this height is not a height"
                    .to_owned(),
            },
            |height| ScannedEntry::Row { height, encoded },
        )),
        Err(source) if source.kind() == ErrorKind::Corruption => Ok(ScannedEntry::Unreadable {
            reason: format!(
                "a stored tree_state_checkpoint block at or above this height is corrupt: {source}"
            ),
        }),
        Err(source) => Err(CanonicalStoreError::RocksDbOperation {
            operation: "tree-state verification read",
            source,
        }),
    }
}

/// Checks one stored row as a writer checks the checkpoint it stores.
fn verify_row(
    network_upgrade_activations: &NetworkUpgradeActivations,
    height: BlockHeight,
    encoded: &[u8],
) -> Option<TreeStateCheckpointMismatch> {
    let mismatch = |pool, reason| TreeStateCheckpointMismatch {
        height,
        pool,
        reason,
    };
    match decode_tree_state_checkpoint(encoded) {
        Err(error) => Some(mismatch(error.protocol(), error.to_string())),
        Ok((_, frontiers)) => {
            frontier_presence_mismatch(network_upgrade_activations, height, &frontiers).map(
                |presence| {
                    mismatch(
                        Some(presence.protocol),
                        format!("the checkpoint {}", presence.describe()),
                    )
                },
            )
        }
    }
}
