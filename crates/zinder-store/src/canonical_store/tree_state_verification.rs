//! Verifying stored tree-state checkpoints in full.
//!
//! A stored tree-state read serves a checkpoint row after checking only its
//! framing, so a served row rests on the checks made when it was written and
//! on `RocksDB` block checksums. Verification reads settled rows again and
//! checks each one as a writer does: every frontier parses with the official
//! codec, is its canonical encoding, and derives the root stored beside it,
//! the row carries a frontier for exactly the pools active at its height, and
//! its block time is the canonical block time at that height. A caller
//! repeats bounded verifications over settled history, so a damaged row is
//! found within one pass over it.
//!
//! Verification reads through a [`RocksDbTreeStateVerifier`], a canonical
//! secondary whose checkpoint family has no block cache, so every read takes
//! its blocks from the table file and checks their checksums. A block that an
//! earlier read cached cannot hide damage done to the file since. A block
//! that fails that check is damage, reported like a row that fails a row
//! check. A table file the primary has since deleted is not damage: it is an
//! ordinary store error that clears once the secondary catches up.

use std::{num::NonZeroU32, path::Path};

use rust_rocksdb::{Direction, ErrorKind, IteratorMode, ReadOptions};
use thiserror::Error;
use zinder_core::{
    BlockHeight, NetworkUpgradeActivations, ShieldedProtocol,
    wire::{ascending_height_key_scan_start, decode_height_key_ascending},
};

use super::{
    CanonicalReorgPolicy, CanonicalSecondaryCatchupOutcome, CanonicalStoreError,
    CanonicalStoreReadyEvidence, CanonicalStoreWorkload, RocksDbCanonicalSecondary,
    block_load::{decode_tree_state_checkpoint, encode_block_position},
    frontier_presence_mismatch,
    publication::column_family,
    reader::{CanonicalServingRead, decode_block_header},
    rocksdb::{
        BLOCK_HEADER_COLUMN_FAMILY, CANONICAL_DATA_COLUMN_FAMILIES,
        CanonicalStoreAdmissionExpectation, TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
        canonical_verification_column_family_descriptors,
    },
};
use crate::{RawBlobRetention, RocksDbResourceBudget, StoreRole};

/// A canonical secondary opened to verify stored tree-state checkpoints.
///
/// Its tree-state checkpoint family reads through no block cache, so each
/// verification reads the checkpoint blocks from their table files and
/// checks their checksums, whatever an earlier read loaded. It serves no
/// other read.
pub struct RocksDbTreeStateVerifier {
    pub(super) secondary: RocksDbCanonicalSecondary,
}

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

/// What verifying the checkpoint stored at one height found.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TreeStateCheckpointRowVerification {
    /// The row passed every check.
    Intact,
    /// No row is stored at the height.
    Absent,
    /// The row failed a check.
    Mismatch(TreeStateCheckpointMismatch),
}

/// A stored tree-state checkpoint that failed verification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeStateCheckpointMismatch {
    /// Height the row is stored at. For an
    /// [`TreeStateCheckpointMismatchKind::UnreadableEntry`], the first height
    /// the damage may cover: the height above the last row verified.
    pub height: BlockHeight,
    /// What `height` names.
    pub kind: TreeStateCheckpointMismatchKind,
    /// Pool whose frontier failed, or `None` when the row failed outside any
    /// one frontier, such as a value that ends inside its block time, a
    /// block time that differs from the canonical block's, or a canonical
    /// header that cannot be read to check it against.
    pub pool: Option<ShieldedProtocol>,
    /// The check that failed.
    pub reason: String,
}

/// What the height of a [`TreeStateCheckpointMismatch`] names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TreeStateCheckpointMismatchKind {
    /// The row stored at the height failed a check, or a table block that
    /// holds the row or the canonical header at its height failed its
    /// checksum.
    Row,
    /// A scan could not read an entry as a row: a key that is not a height,
    /// or a table block that fails its checksum. The damage lies at or above
    /// the height.
    UnreadableEntry,
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

impl RocksDbTreeStateVerifier {
    /// Opens one exactly admitted secondary for verification and catches it
    /// up to the primary's current state, as
    /// [`RocksDbCanonicalSecondary::open_ready`] does.
    ///
    /// `resource_budget` bounds every family but the checkpoint family,
    /// which reads through no block cache.
    #[allow(
        clippy::too_many_arguments,
        reason = "secondary admission keeps both filesystem identities and every immutable canonical contract explicit"
    )]
    pub fn open_ready(
        primary_path: impl AsRef<Path>,
        secondary_path: impl AsRef<Path>,
        expected_network_upgrade_activations: &NetworkUpgradeActivations,
        expected_workload: CanonicalStoreWorkload,
        expected_raw_blob_retention: RawBlobRetention,
        expected_reorg_policy: CanonicalReorgPolicy,
        resource_budget: RocksDbResourceBudget,
    ) -> Result<Self, CanonicalStoreError> {
        RocksDbCanonicalSecondary::open_ready_with_expectation(
            primary_path.as_ref(),
            secondary_path.as_ref(),
            &CanonicalStoreAdmissionExpectation::from_activations(
                expected_network_upgrade_activations,
                expected_workload,
                expected_raw_blob_retention,
                expected_reorg_policy,
            ),
            resource_budget.into(),
            canonical_verification_column_family_descriptors,
        )
        .map(|secondary| Self { secondary })
    }

    /// Publishes this verifier's resource gauges independently of replay and serving readers.
    pub fn record_resource_gauges(&self) {
        self.secondary.bounded_open.record_resource_gauges(
            StoreRole::TreeStateVerificationSecondary,
            &CANONICAL_DATA_COLUMN_FAMILIES,
        );
    }

    /// Catches the secondary up, as
    /// [`RocksDbCanonicalSecondary::try_catch_up`] does.
    pub fn try_catch_up(
        &mut self,
    ) -> Result<CanonicalSecondaryCatchupOutcome, CanonicalStoreError> {
        self.secondary.try_catch_up()
    }

    /// Returns the currently admitted READY evidence.
    #[must_use]
    pub const fn ready_evidence(&self) -> CanonicalStoreReadyEvidence {
        self.secondary.ready_evidence()
    }

    /// Verifies up to `max_rows` stored tree-state checkpoints, in height
    /// order, from `from` through the settled tip.
    ///
    /// Each row must decode in full: every frontier parses with the official
    /// codec, is its canonical encoding, and derives the root stored beside
    /// it. It must also carry a frontier for exactly the pools that
    /// `network_upgrade_activations` activates at or below its height, and the
    /// block time of the canonical header at its height, or of the history
    /// predecessor, which has no retained header. The first row that fails is
    /// reported in [`TreeStateCheckpointVerification::mismatch`], as is a row
    /// whose header is missing, does not decode, or sits in a block that
    /// fails its checksum, a key that is not a height, and a table block that
    /// fails its checksum.
    /// Entries above the settled tip are never read, so damage there waits
    /// for the tip to pass it. An error means the store could not be read or
    /// does not admit the activation table.
    pub fn verify_tree_state_checkpoints(
        &self,
        from: BlockHeight,
        max_rows: NonZeroU32,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<TreeStateCheckpointVerification, TreeStateVerificationError> {
        verify_tree_state_checkpoints(&self.secondary, from, max_rows, network_upgrade_activations)
    }

    /// Verifies the checkpoint stored at exactly `height` with the checks
    /// [`Self::verify_tree_state_checkpoints`] applies.
    ///
    /// A block that fails its checksum, whether it holds the row or the
    /// canonical header at its height, is a mismatch of that row. An error
    /// means the store could not be read or does not admit the activation
    /// table.
    pub fn verify_tree_state_checkpoint_at(
        &self,
        height: BlockHeight,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<TreeStateCheckpointRowVerification, CanonicalStoreError> {
        let store = &self.secondary;
        require_admitted_activations(store, network_upgrade_activations)?;
        match read_checksummed(store, TREE_STATE_CHECKPOINT_COLUMN_FAMILY, height)? {
            ChecksummedRead::Absent => Ok(TreeStateCheckpointRowVerification::Absent),
            ChecksummedRead::Stored(encoded) => {
                verify_row(store, network_upgrade_activations, height, &encoded).map(|mismatch| {
                    mismatch.map_or(
                        TreeStateCheckpointRowVerification::Intact,
                        TreeStateCheckpointRowVerification::Mismatch,
                    )
                })
            }
            ChecksummedRead::Corrupt(source) => Ok(TreeStateCheckpointRowVerification::Mismatch(
                TreeStateCheckpointMismatch {
                    height,
                    kind: TreeStateCheckpointMismatchKind::Row,
                    pool: None,
                    reason: format!(
                        "the stored tree_state_checkpoint block holding this height is corrupt: {source}"
                    ),
                },
            )),
        }
    }
}

/// Refuses an activation table the store does not admit at or below its
/// settled tip, which would judge each row's pools by another schedule.
fn require_admitted_activations(
    store: &impl CanonicalServingRead,
    network_upgrade_activations: &NetworkUpgradeActivations,
) -> Result<(), CanonicalStoreError> {
    let settled_height = store
        .serving_ready_evidence()
        .sequence_checkpoint
        .through()
        .height;
    if store.serving_admits_activations_through(network_upgrade_activations, settled_height) {
        return Ok(());
    }
    Err(CanonicalStoreError::admission(
        store.serving_open().db.path(),
        "tree-state verification activation table differs from the admitted schedule at or below the settled tip",
    ))
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
    require_admitted_activations(store, network_upgrade_activations)
        .map_err(|source| interrupted(&verification, source))?;
    let settled_height = store
        .serving_ready_evidence()
        .sequence_checkpoint
        .through()
        .height;
    let db = &store.serving_open().db;
    let family = column_family(db, TREE_STATE_CHECKPOINT_COLUMN_FAMILY)
        .map_err(|source| interrupted(&verification, source))?;
    let mut read_options = ReadOptions::default();
    read_options.set_verify_checksums(true);
    // A scan of heights through the settled tip ends where a scan from the
    // height above it starts, so it never reads an entry above settlement,
    // a key that extends the settled tip's key included.
    if let Some(above_settled) = settled_height.next() {
        read_options.set_iterate_upper_bound(ascending_height_key_scan_start(above_settled));
    }
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
                        kind: TreeStateCheckpointMismatchKind::UnreadableEntry,
                        pool: None,
                        reason,
                    });
                    return Ok(verification);
                }
            };
        if verification.verified_rows >= max_rows.get() {
            return Ok(verification);
        }
        if let Some(mismatch) = verify_row(store, network_upgrade_activations, height, &encoded)
            .map_err(|source| interrupted(&verification, source))?
        {
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
    store: &impl CanonicalServingRead,
    network_upgrade_activations: &NetworkUpgradeActivations,
    height: BlockHeight,
    encoded: &[u8],
) -> Result<Option<TreeStateCheckpointMismatch>, CanonicalStoreError> {
    let mismatch = |pool, reason| TreeStateCheckpointMismatch {
        height,
        kind: TreeStateCheckpointMismatchKind::Row,
        pool,
        reason,
    };
    let (block_time_seconds, frontiers) = match decode_tree_state_checkpoint(encoded) {
        Ok(decoded) => decoded,
        Err(error) => return Ok(Some(mismatch(error.protocol(), error.to_string()))),
    };
    if let Some(presence) =
        frontier_presence_mismatch(network_upgrade_activations, height, &frontiers)
    {
        return Ok(Some(mismatch(
            Some(presence.protocol),
            format!("the checkpoint {}", presence.describe()),
        )));
    }
    Ok(block_time_failure(store, height, block_time_seconds)?.map(|reason| mismatch(None, reason)))
}

/// Returns why `block_time_seconds`, the block time of the row at `height`,
/// fails to be the canonical block time there: the history predecessor's,
/// which has no retained header, or the canonical header's.
///
/// Every other settled height has a header, so a header that is missing,
/// sits in a block that fails its checksum, or does not decode fails the
/// row as well, instead of stopping verification at it.
fn block_time_failure(
    store: &impl CanonicalServingRead,
    height: BlockHeight,
    block_time_seconds: u32,
) -> Result<Option<String>, CanonicalStoreError> {
    let history_predecessor = store.serving_build_plan().history_predecessor();
    let canonical_block_time = if height == history_predecessor.block_id.height {
        i64::from(history_predecessor.block_time_seconds)
    } else {
        let encoded = match read_checksummed(store, BLOCK_HEADER_COLUMN_FAMILY, height)? {
            ChecksummedRead::Stored(encoded) => encoded,
            ChecksummedRead::Absent => {
                return Ok(Some(
                    "no canonical block header is stored at the checkpoint's height".to_owned(),
                ));
            }
            ChecksummedRead::Corrupt(source) => {
                return Ok(Some(format!(
                    "the stored block_header block holding the checkpoint's height is corrupt: {source}"
                )));
            }
        };
        match decode_block_header(height, &encoded) {
            Ok(header) => header.block_time,
            Err(error) => {
                return Ok(Some(format!(
                    "the canonical block header at the checkpoint's height does not decode: {error}"
                )));
            }
        }
    };
    Ok((canonical_block_time != i64::from(block_time_seconds)).then(|| {
        format!(
            "the checkpoint block time {block_time_seconds} differs from the canonical block time {canonical_block_time}"
        )
    }))
}

/// What a read of the value stored at one height found, with the checksum of
/// every block it read verified.
enum ChecksummedRead {
    /// No value is stored at the height.
    Absent,
    /// The stored value.
    Stored(Vec<u8>),
    /// The table block holding the height fails its checksum.
    Corrupt(rust_rocksdb::Error),
}

/// Reads the value `family_name` stores at `height`, keeping a block that
/// fails its checksum apart from every other store failure, such as a table
/// file compacted away under the secondary.
fn read_checksummed(
    store: &impl CanonicalServingRead,
    family_name: &'static str,
    height: BlockHeight,
) -> Result<ChecksummedRead, CanonicalStoreError> {
    let db = &store.serving_open().db;
    let family = column_family(db, family_name)?;
    let mut read_options = ReadOptions::default();
    read_options.set_verify_checksums(true);
    match db.get_cf_opt(&family, encode_block_position(height), &read_options) {
        Ok(None) => Ok(ChecksummedRead::Absent),
        Ok(Some(encoded)) => Ok(ChecksummedRead::Stored(encoded)),
        Err(source) if source.kind() == ErrorKind::Corruption => {
            Ok(ChecksummedRead::Corrupt(source))
        }
        Err(source) => Err(CanonicalStoreError::RocksDbOperation {
            operation: "tree-state verification read",
            source,
        }),
    }
}
