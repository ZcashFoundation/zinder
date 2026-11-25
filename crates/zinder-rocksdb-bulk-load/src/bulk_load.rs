//! Bounded external sorting and ordered SST construction for `RocksDB`.

use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    mem::size_of,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    thread::{self, JoinHandle},
};

use parking_lot::{Condvar, Mutex};
use rust_rocksdb::{Options, SstFileWriter};
use sha2::{Digest, Sha256};
use thiserror::Error;

mod variable_value_sort;

pub use variable_value_sort::{
    ParallelVariableValueSorter, SortedVariableValues, StagedParallelVariableRuns,
    VariableValueRecord, VariableValueSortDurations, VariableValueSortEvidence,
    VariableValueSortShard, VariableValueSorter,
};

pub(super) const MIN_MERGE_FAN_IN: usize = 2;
#[cfg(not(unix))]
const FALLBACK_MERGE_FAN_IN: usize = 64;
pub(super) const MAX_MERGE_FAN_IN: usize = 256;
#[cfg(any(unix, test))]
const MERGE_FD_HEADROOM_RESERVE: u64 = 64;
#[cfg(any(unix, test))]
const MERGE_FD_MIN_RESERVE: u64 = 8;

/// Computes the active k-way merge fan-in from descriptor limits.
///
/// Reserves file-descriptor headroom for the merge output, open `RocksDB`
/// handles, and standard process I/O.
#[cfg(any(unix, test))]
fn effective_merge_fan_in_for_limits(soft_limit: Option<u64>, open_fds: Option<usize>) -> usize {
    let Some(limit) = soft_limit else {
        return MAX_MERGE_FAN_IN;
    };
    let available = open_fds.map_or(limit / 2, |used| {
        limit.saturating_sub(u64::try_from(used).unwrap_or(u64::MAX))
    });
    let reserved = MERGE_FD_HEADROOM_RESERVE
        .min(available / 2)
        .max(MERGE_FD_MIN_RESERVE);
    let usable = available.saturating_sub(reserved).min(available / 2);
    usize::try_from(usable)
        .unwrap_or(MAX_MERGE_FAN_IN)
        .clamp(MIN_MERGE_FAN_IN, MAX_MERGE_FAN_IN)
}

#[cfg(unix)]
fn open_fd_count() -> Option<usize> {
    for fd_dir in ["/proc/self/fd", "/dev/fd"] {
        let Ok(entries) = fs::read_dir(fd_dir) else {
            continue;
        };
        let mut count = 0_usize;
        for entry in entries {
            if entry.is_ok() {
                count = count.saturating_add(1);
            }
        }
        return Some(count);
    }
    None
}

/// Returns the active k-way merge fan-in bounded by `MAX_MERGE_FAN_IN` and the
/// process file-descriptor headroom.
pub(super) fn effective_merge_fan_in() -> usize {
    #[cfg(unix)]
    {
        let nofile = rustix::process::getrlimit(rustix::process::Resource::Nofile);
        effective_merge_fan_in_for_limits(nofile.current, open_fd_count())
    }
    #[cfg(not(unix))]
    {
        FALLBACK_MERGE_FAN_IN
    }
}

pub(super) fn validate_merge_fan_in(merge_fan_in: usize) -> Result<(), BulkLoadError> {
    if !(MIN_MERGE_FAN_IN..=MAX_MERGE_FAN_IN).contains(&merge_fan_in) {
        return Err(BulkLoadError::invalid(
            "sort merge fan-in must remain between 2 and MAX_MERGE_FAN_IN",
        ));
    }
    Ok(())
}

/// Failure while staging opaque ordered records for `RocksDB` ingestion.
#[derive(Debug, Error)]
pub enum BulkLoadError {
    /// A caller supplied an invalid sorter or SST boundary.
    #[error("RocksDB bulk-load input is invalid: {reason}")]
    InvalidInput {
        /// Stable description of the rejected invariant.
        reason: String,
    },
    /// One in-memory record or run would cross its accounted byte ceiling.
    #[error(
        "RocksDB bulk-load records require at least {required_bytes} accounted bytes, limit is {limit_bytes}"
    )]
    AccountedMemoryLimit {
        /// Caller-supplied accounted byte ceiling.
        limit_bytes: u64,
        /// Minimum accounted bytes required by the refused operation.
        required_bytes: u64,
    },
    /// Run creation or merging would cross the temporary-file byte ceiling.
    #[error(
        "RocksDB bulk-load runs require at least {required_bytes} temporary bytes, limit is {limit_bytes}"
    )]
    TemporaryFileLimit {
        /// Caller-supplied temporary-file byte ceiling.
        limit_bytes: u64,
        /// Peak temporary bytes required by the refused operation.
        required_bytes: u64,
    },
    /// Records staged before `finish` already need more temporary-file bytes
    /// than the ceiling allows, so the complete sort cannot fit either.
    #[error(
        "RocksDB bulk-load runs already require at least {required_bytes} temporary bytes before the input ended, limit is {limit_bytes}"
    )]
    TemporaryFileLimitBeforeFinish {
        /// Caller-supplied temporary-file byte ceiling.
        limit_bytes: u64,
        /// Temporary bytes required by the records staged so far.
        ///
        /// This is a lower bound: records the caller has not pushed yet add
        /// to the complete requirement.
        required_bytes: u64,
    },
    /// A bounded staging allocation could not be reserved.
    #[error("RocksDB bulk-load {operation} allocation failed")]
    MemoryAllocation {
        /// Stable allocation label.
        operation: &'static str,
        /// Underlying fallible-reservation error.
        #[source]
        source: std::collections::TryReserveError,
    },
    /// A staging path could not be read, written, or removed.
    #[error("RocksDB bulk-load path is unavailable: {path}: {source}")]
    PathUnavailable {
        /// Path whose operation failed.
        path: PathBuf,
        /// Underlying filesystem failure.
        #[source]
        source: std::io::Error,
    },
    /// `RocksDB` rejected an ordered SST operation.
    #[error("RocksDB bulk-load {operation} failed")]
    RocksDbOperation {
        /// Stable operation label.
        operation: &'static str,
        /// Underlying `RocksDB` failure.
        #[source]
        source: rust_rocksdb::Error,
    },
}

impl BulkLoadError {
    fn invalid(reason: impl Into<String>) -> Self {
        Self::InvalidInput {
            reason: reason.into(),
        }
    }

    /// Marks a temporary-file refusal as covering only the records seen so far.
    fn temporary_file_limit_before_finish(self) -> Self {
        if let Self::TemporaryFileLimit {
            limit_bytes,
            required_bytes,
        } = self
        {
            return Self::TemporaryFileLimitBeforeFinish {
                limit_bytes,
                required_bytes,
            };
        }
        self
    }
}

/// Physical SST files produced from one strictly ordered logical family.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct SstFileSet {
    /// Ordered paths ready for external-file ingestion.
    pub paths: Vec<PathBuf>,
    /// Total physical bytes occupied by `paths`.
    pub file_bytes: u64,
    /// Cryptographic key/value evidence for every staged physical file.
    pub files: Vec<SstFileEvidence>,
    /// Whole-family evidence independent of physical SST rotation.
    pub logical_family_evidence: OrderedKeyValueEvidence,
}

/// Immutable evidence for one complete ordered logical key/value family.
///
/// Logical bytes count exact key and value bytes, excluding the digest's
/// framing. Empty families retain zero counts, absent boundary keys, and the
/// digest of the family domain alone.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedKeyValueEvidence {
    /// Exact number of ordered key/value rows.
    pub row_count: u64,
    /// Sum of the exact key and value byte lengths.
    pub logical_bytes: u64,
    /// First key, or `None` when the family is empty.
    pub first_key: Option<Vec<u8>>,
    /// Last key, or `None` when the family is empty.
    pub last_key: Option<Vec<u8>>,
    /// SHA-256 of the version-1 domain followed by every exact ordered row.
    ///
    /// Each row is framed as the little-endian `u64` key length, key bytes,
    /// little-endian `u64` value length, and value bytes.
    pub ordered_key_value_digest: [u8; 32],
}

impl Default for OrderedKeyValueEvidence {
    fn default() -> Self {
        OrderedKeyValueEvidenceAccumulator::new().finish()
    }
}

/// Accumulates rotation-independent evidence for an ordered logical family.
pub struct OrderedKeyValueEvidenceAccumulator {
    row_count: u64,
    logical_bytes: u64,
    first_key: Option<Vec<u8>>,
    last_key: Option<Vec<u8>>,
    digest: Sha256,
}

impl Default for OrderedKeyValueEvidenceAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderedKeyValueEvidenceAccumulator {
    /// Creates empty evidence in the version-1 logical-family domain.
    #[must_use]
    pub fn new() -> Self {
        let mut digest = Sha256::new();
        digest.update(ORDERED_KEY_VALUE_EVIDENCE_DIGEST_DOMAIN);
        Self {
            row_count: 0,
            logical_bytes: 0,
            first_key: None,
            last_key: None,
            digest,
        }
    }

    /// Records one strictly increasing key and its exact encoded value.
    pub fn record(&mut self, key: &[u8], encoded_value: &[u8]) -> Result<(), BulkLoadError> {
        let update = self.prepare_update(key, encoded_value)?;
        self.apply_update(update, key, encoded_value);
        Ok(())
    }

    /// Finishes the complete ordered-family evidence.
    #[must_use]
    pub fn finish(self) -> OrderedKeyValueEvidence {
        OrderedKeyValueEvidence {
            row_count: self.row_count,
            logical_bytes: self.logical_bytes,
            first_key: self.first_key,
            last_key: self.last_key,
            ordered_key_value_digest: self.digest.finalize().into(),
        }
    }

    fn prepare_update(
        &self,
        key: &[u8],
        encoded_value: &[u8],
    ) -> Result<OrderedKeyValueEvidenceUpdate, BulkLoadError> {
        prepare_ordered_row_update(
            self.last_key.as_deref(),
            self.row_count,
            self.logical_bytes,
            key,
            encoded_value,
        )
    }

    fn apply_update(
        &mut self,
        update: OrderedKeyValueEvidenceUpdate,
        key: &[u8],
        encoded_value: &[u8],
    ) {
        self.row_count = update.row_count;
        self.logical_bytes = update.logical_bytes;
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        let last_slot = self.last_key.get_or_insert_with(Vec::new);
        last_slot.clear();
        last_slot.extend_from_slice(key);
        update_ordered_key_value_digest(
            &mut self.digest,
            update.key_length,
            key,
            update.encoded_value_length,
            encoded_value,
        );
    }
}

fn prepare_ordered_row_update(
    previous_key: Option<&[u8]>,
    row_count: u64,
    logical_bytes: u64,
    key: &[u8],
    encoded_value: &[u8],
) -> Result<OrderedKeyValueEvidenceUpdate, BulkLoadError> {
    if previous_key.is_some_and(|previous| previous >= key) {
        return Err(BulkLoadError::invalid(
            "ordered logical-family keys are duplicated or not strictly increasing",
        ));
    }
    let row_logical_bytes = logical_row_bytes(key.len(), encoded_value.len())?;
    let next_row_count = row_count
        .checked_add(1)
        .ok_or_else(|| BulkLoadError::invalid("logical-family row count exceeds u64::MAX"))?;
    let next_logical_bytes = logical_bytes
        .checked_add(row_logical_bytes)
        .ok_or_else(|| BulkLoadError::invalid("logical-family bytes exceed u64::MAX"))?;
    let key_length = u64::try_from(key.len())
        .map_err(|_| BulkLoadError::invalid("SST key length exceeds u64::MAX"))?;
    let encoded_value_length = u64::try_from(encoded_value.len())
        .map_err(|_| BulkLoadError::invalid("SST value length exceeds u64::MAX"))?;
    Ok(OrderedKeyValueEvidenceUpdate {
        row_count: next_row_count,
        logical_bytes: next_logical_bytes,
        row_logical_bytes,
        key_length,
        encoded_value_length,
    })
}

#[derive(Default)]
struct OrderedRowBounds {
    row_count: u64,
    logical_bytes: u64,
    last_key: Option<Vec<u8>>,
}

impl OrderedRowBounds {
    fn prepare_and_commit(
        &mut self,
        key: &[u8],
        encoded_value: &[u8],
    ) -> Result<(), BulkLoadError> {
        let update = prepare_ordered_row_update(
            self.last_key.as_deref(),
            self.row_count,
            self.logical_bytes,
            key,
            encoded_value,
        )?;
        self.row_count = update.row_count;
        self.logical_bytes = update.logical_bytes;
        let last_slot = self.last_key.get_or_insert_with(Vec::new);
        last_slot.clear();
        last_slot.extend_from_slice(key);
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct OrderedKeyValueEvidenceUpdate {
    row_count: u64,
    logical_bytes: u64,
    row_logical_bytes: u64,
    key_length: u64,
    encoded_value_length: u64,
}

/// Immutable evidence emitted while one staged external SST is written.
///
/// The digest is domain-separated and receives the length-prefixed key and
/// value of each row in the exact writer order. The evidence is therefore
/// available before ingestion without reading an SST back from disk.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SstFileEvidence {
    /// Zero-based ordinal within its logical family.
    pub ordinal: u64,
    /// Physical file size after the writer closes the file.
    pub file_bytes: u64,
    /// Exact number of key/value rows in the file.
    pub entry_count: u64,
    /// First ordered key in the file.
    pub first_key: Vec<u8>,
    /// Last ordered key in the file.
    pub last_key: Vec<u8>,
    /// Version-1 domain-separated digest of every exact key/value row.
    pub ordered_key_value_digest: [u8; 32],
}

const SST_FILE_EVIDENCE_DIGEST_DOMAIN: &[u8] = b"zinder.rocksdb.staged-sst.v1\0";
const ORDERED_KEY_VALUE_EVIDENCE_DIGEST_DOMAIN: &[u8] =
    b"zinder.rocksdb.ordered-logical-family.v1\0";
const PIPELINE_BATCH_MAX_BYTES: usize = 256 * 1024;
const PIPELINE_BATCH_MAX_ENTRIES: usize = 4_096;

/// Writes strictly increasing opaque keys into bounded-size SST files.
pub struct OrderedSstWriter<'options> {
    sink: OrderedSstSink<'options>,
}

enum OrderedSstSink<'options> {
    Sync(Box<SyncOrderedSstState<'options>>),
    Pipelined(Box<PipelinedOrderedSstState>),
}

struct SyncOrderedSstState<'options> {
    physical: SstPhysicalWriter<'options>,
    digest: SstDigestAccumulator,
}

impl<'options> OrderedSstWriter<'options> {
    /// Creates an ordered writer whose files rotate after the target is reached.
    pub fn new(
        staging_path: &'options Path,
        artifact_label: &'static str,
        options: &'options Options,
        target_logical_bytes: u64,
    ) -> Result<Self, BulkLoadError> {
        if target_logical_bytes == 0 {
            return Err(BulkLoadError::invalid(
                "SST target logical bytes must be greater than zero",
            ));
        }
        Ok(Self {
            sink: OrderedSstSink::Sync(Box::new(SyncOrderedSstState {
                physical: SstPhysicalWriter::new(
                    staging_path,
                    artifact_label,
                    options,
                    target_logical_bytes,
                ),
                digest: SstDigestAccumulator::new(target_logical_bytes),
            })),
        })
    }

    /// Creates a pipelined ordered writer with bounded queue memory.
    ///
    /// Physical SST compression/writing (`SstFileWriter`) and dual SHA-256
    /// hashing run on dedicated worker threads behind shared bounded batches.
    /// Synchronous key-order and size validation still runs on the caller
    /// thread before rows are queued.
    pub fn new_pipelined(
        staging_path: &'options Path,
        artifact_label: &'static str,
        options: &'options Options,
        target_logical_bytes: u64,
        queue_budget_bytes: u64,
    ) -> Result<Self, BulkLoadError> {
        if target_logical_bytes == 0 {
            return Err(BulkLoadError::invalid(
                "SST target logical bytes must be greater than zero",
            ));
        }
        if queue_budget_bytes == 0 {
            return Err(BulkLoadError::invalid(
                "SST queue budget bytes must be greater than zero",
            ));
        }
        let pipeline = PipelinedOrderedSstState::spawn(
            staging_path,
            artifact_label,
            options,
            target_logical_bytes,
            queue_budget_bytes,
        )?;
        Ok(Self {
            sink: OrderedSstSink::Pipelined(Box::new(pipeline)),
        })
    }

    /// Appends one key and value after verifying strict key order.
    pub fn put(&mut self, key: &[u8], encoded_value: &[u8]) -> Result<(), BulkLoadError> {
        match &mut self.sink {
            OrderedSstSink::Sync(sync) => sync.put(key, encoded_value),
            OrderedSstSink::Pipelined(pipelined) => pipelined.put(key, encoded_value),
        }
    }

    /// Finishes the last file and returns every produced SST path.
    pub fn finish(self) -> Result<SstFileSet, BulkLoadError> {
        match self.sink {
            OrderedSstSink::Sync(sync) => sync.finish(),
            OrderedSstSink::Pipelined(pipelined) => pipelined.finish(),
        }
    }

    #[cfg(test)]
    fn peak_pipeline_accounted_bytes(&self) -> usize {
        match &self.sink {
            OrderedSstSink::Sync(_) => 0,
            OrderedSstSink::Pipelined(pipelined) => pipelined.control.peak_accounted_bytes(),
        }
    }
}

impl SyncOrderedSstState<'_> {
    fn put(&mut self, key: &[u8], encoded_value: &[u8]) -> Result<(), BulkLoadError> {
        let evidence_update = self.digest.prepare_put(key, encoded_value)?;
        self.physical
            .put(key, encoded_value, evidence_update.row_logical_bytes)?;
        self.digest.apply_put(evidence_update, key, encoded_value)?;
        Ok(())
    }

    fn finish(self) -> Result<SstFileSet, BulkLoadError> {
        let written = self.physical.finish()?;
        let (digested_files, logical_family_evidence) = self.digest.finish()?;
        assemble_sst_file_set(written, digested_files, logical_family_evidence)
    }
}

struct SstWrittenFileMeta {
    ordinal: u64,
    file_bytes: u64,
    entry_count: u64,
}

struct WrittenSstFiles {
    paths: Vec<PathBuf>,
    file_bytes: u64,
    files: Vec<SstWrittenFileMeta>,
}

struct SstPhysicalWriter<'options> {
    staging_path: &'options Path,
    artifact_label: &'static str,
    options: &'options Options,
    target_logical_bytes: u64,
    writer: Option<SstFileWriter<'options>>,
    current_path: Option<PathBuf>,
    current_logical_bytes: u64,
    current_entry_count: u64,
    current_ordinal: Option<u64>,
    next_file_index: u64,
    file_bytes: u64,
    paths: Vec<PathBuf>,
    files: Vec<SstWrittenFileMeta>,
}

impl<'options> SstPhysicalWriter<'options> {
    fn new(
        staging_path: &'options Path,
        artifact_label: &'static str,
        options: &'options Options,
        target_logical_bytes: u64,
    ) -> Self {
        Self {
            staging_path,
            artifact_label,
            options,
            target_logical_bytes,
            writer: None,
            current_path: None,
            current_logical_bytes: 0,
            current_entry_count: 0,
            current_ordinal: None,
            next_file_index: 0,
            file_bytes: 0,
            paths: Vec::new(),
            files: Vec::new(),
        }
    }

    fn put(
        &mut self,
        key: &[u8],
        encoded_value: &[u8],
        row_logical_bytes: u64,
    ) -> Result<(), BulkLoadError> {
        if self.writer.is_some() && self.current_logical_bytes >= self.target_logical_bytes {
            self.finish_current()?;
        }
        if self.writer.is_none() {
            self.open_next()?;
        }
        let writer = self.writer.as_mut().ok_or_else(|| {
            BulkLoadError::invalid("ordered SST writer did not retain its open file")
        })?;
        writer
            .put(key, encoded_value)
            .map_err(|source| BulkLoadError::RocksDbOperation {
                operation: "ordered SST write",
                source,
            })?;
        self.current_logical_bytes = self
            .current_logical_bytes
            .checked_add(row_logical_bytes)
            .ok_or_else(|| BulkLoadError::invalid("current SST logical bytes exceed u64::MAX"))?;
        self.current_entry_count = self
            .current_entry_count
            .checked_add(1)
            .ok_or_else(|| BulkLoadError::invalid("SST entry count exceeds u64::MAX"))?;
        Ok(())
    }

    fn finish(mut self) -> Result<WrittenSstFiles, BulkLoadError> {
        self.finish_current()?;
        Ok(WrittenSstFiles {
            paths: self.paths,
            file_bytes: self.file_bytes,
            files: self.files,
        })
    }

    fn open_next(&mut self) -> Result<(), BulkLoadError> {
        let ordinal = self.next_file_index;
        let path = self
            .staging_path
            .join(format!("{}-{:08}.sst", self.artifact_label, ordinal));
        self.next_file_index = ordinal
            .checked_add(1)
            .ok_or_else(|| BulkLoadError::invalid("SST file count exceeds u64::MAX"))?;
        let mut writer = SstFileWriter::create(self.options);
        writer
            .open(&path)
            .map_err(|source| BulkLoadError::RocksDbOperation {
                operation: "ordered SST open",
                source,
            })?;
        self.writer = Some(writer);
        self.current_path = Some(path);
        self.current_logical_bytes = 0;
        self.current_entry_count = 0;
        self.current_ordinal = Some(ordinal);
        Ok(())
    }

    fn finish_current(&mut self) -> Result<(), BulkLoadError> {
        let Some(mut writer) = self.writer.take() else {
            return Ok(());
        };
        let path = self
            .current_path
            .take()
            .ok_or_else(|| BulkLoadError::invalid("open ordered SST writer has no output path"))?;
        writer
            .finish()
            .map_err(|source| BulkLoadError::RocksDbOperation {
                operation: "ordered SST finish",
                source,
            })?;
        let file_bytes = fs::metadata(&path)
            .map_err(|source| path_unavailable(&path, source))?
            .len();
        self.file_bytes = self
            .file_bytes
            .checked_add(file_bytes)
            .ok_or_else(|| BulkLoadError::invalid("total SST file bytes exceed u64::MAX"))?;
        let ordinal = self
            .current_ordinal
            .take()
            .ok_or_else(|| BulkLoadError::invalid("closed ordered SST has no evidence ordinal"))?;
        self.files.push(SstWrittenFileMeta {
            ordinal,
            file_bytes,
            entry_count: self.current_entry_count,
        });
        self.paths.push(path);
        Ok(())
    }
}

struct SstDigestFileMeta {
    ordinal: u64,
    entry_count: u64,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    ordered_key_value_digest: [u8; 32],
}

struct SstDigestAccumulator {
    target_logical_bytes: u64,
    next_file_index: u64,
    current_logical_bytes: u64,
    current_entry_count: u64,
    current_ordinal: Option<u64>,
    current_first_key: Option<Vec<u8>>,
    current_last_key: Option<Vec<u8>>,
    current_digest: Option<Sha256>,
    files: Vec<SstDigestFileMeta>,
    logical_family_evidence: OrderedKeyValueEvidenceAccumulator,
}

impl SstDigestAccumulator {
    fn new(target_logical_bytes: u64) -> Self {
        Self {
            target_logical_bytes,
            next_file_index: 0,
            current_logical_bytes: 0,
            current_entry_count: 0,
            current_ordinal: None,
            current_first_key: None,
            current_last_key: None,
            current_digest: None,
            files: Vec::new(),
            logical_family_evidence: OrderedKeyValueEvidenceAccumulator::new(),
        }
    }

    fn prepare_put(
        &self,
        key: &[u8],
        encoded_value: &[u8],
    ) -> Result<OrderedKeyValueEvidenceUpdate, BulkLoadError> {
        self.logical_family_evidence
            .prepare_update(key, encoded_value)
    }

    fn apply_put(
        &mut self,
        evidence_update: OrderedKeyValueEvidenceUpdate,
        key: &[u8],
        encoded_value: &[u8],
    ) -> Result<(), BulkLoadError> {
        if self.current_ordinal.is_some() && self.current_logical_bytes >= self.target_logical_bytes
        {
            self.finish_current()?;
        }
        if self.current_ordinal.is_none() {
            self.open_next()?;
        }
        self.current_logical_bytes = self
            .current_logical_bytes
            .checked_add(evidence_update.row_logical_bytes)
            .ok_or_else(|| BulkLoadError::invalid("current SST logical bytes exceed u64::MAX"))?;
        self.current_entry_count = self
            .current_entry_count
            .checked_add(1)
            .ok_or_else(|| BulkLoadError::invalid("SST entry count exceeds u64::MAX"))?;
        if self.current_first_key.is_none() {
            self.current_first_key = Some(key.to_vec());
        }
        let last_slot = self.current_last_key.get_or_insert_with(Vec::new);
        last_slot.clear();
        last_slot.extend_from_slice(key);
        let digest = self.current_digest.as_mut().ok_or_else(|| {
            BulkLoadError::invalid("ordered SST writer has no active evidence digest")
        })?;
        update_ordered_key_value_digest(
            digest,
            evidence_update.key_length,
            key,
            evidence_update.encoded_value_length,
            encoded_value,
        );
        self.logical_family_evidence
            .apply_update(evidence_update, key, encoded_value);
        Ok(())
    }

    fn put(&mut self, key: &[u8], encoded_value: &[u8]) -> Result<(), BulkLoadError> {
        let evidence_update = self.prepare_put(key, encoded_value)?;
        self.apply_put(evidence_update, key, encoded_value)
    }

    fn finish(
        mut self,
    ) -> Result<(Vec<SstDigestFileMeta>, OrderedKeyValueEvidence), BulkLoadError> {
        self.finish_current()?;
        Ok((self.files, self.logical_family_evidence.finish()))
    }

    fn open_next(&mut self) -> Result<(), BulkLoadError> {
        let ordinal = self.next_file_index;
        self.next_file_index = ordinal
            .checked_add(1)
            .ok_or_else(|| BulkLoadError::invalid("SST file count exceeds u64::MAX"))?;
        self.current_logical_bytes = 0;
        self.current_entry_count = 0;
        self.current_first_key = None;
        self.current_last_key = None;
        self.current_ordinal = Some(ordinal);
        let mut digest = Sha256::new();
        digest.update(SST_FILE_EVIDENCE_DIGEST_DOMAIN);
        digest.update(ordinal.to_le_bytes());
        self.current_digest = Some(digest);
        Ok(())
    }

    fn finish_current(&mut self) -> Result<(), BulkLoadError> {
        let Some(digest) = self.current_digest.take() else {
            return Ok(());
        };
        let ordinal = self
            .current_ordinal
            .take()
            .ok_or_else(|| BulkLoadError::invalid("closed ordered SST has no evidence ordinal"))?;
        let first_key = self
            .current_first_key
            .take()
            .ok_or_else(|| BulkLoadError::invalid("closed ordered SST has no first key"))?;
        let last_key = self
            .current_last_key
            .take()
            .ok_or_else(|| BulkLoadError::invalid("closed ordered SST has no last key"))?;
        self.files.push(SstDigestFileMeta {
            ordinal,
            entry_count: self.current_entry_count,
            first_key,
            last_key,
            ordered_key_value_digest: digest.finalize().into(),
        });
        Ok(())
    }
}

fn assemble_sst_file_set(
    written: WrittenSstFiles,
    digested_files: Vec<SstDigestFileMeta>,
    logical_family_evidence: OrderedKeyValueEvidence,
) -> Result<SstFileSet, BulkLoadError> {
    if written.files.len() != digested_files.len() {
        return Err(BulkLoadError::invalid(
            "ordered SST file count disagreed with evidence file count",
        ));
    }
    let mut files = Vec::with_capacity(written.files.len());
    for (written_meta, digest_meta) in written.files.into_iter().zip(digested_files) {
        if written_meta.ordinal != digest_meta.ordinal
            || written_meta.entry_count != digest_meta.entry_count
        {
            return Err(BulkLoadError::invalid(
                "ordered SST file metadata disagreed with evidence digest metadata",
            ));
        }
        files.push(SstFileEvidence {
            ordinal: written_meta.ordinal,
            file_bytes: written_meta.file_bytes,
            entry_count: written_meta.entry_count,
            first_key: digest_meta.first_key,
            last_key: digest_meta.last_key,
            ordered_key_value_digest: digest_meta.ordered_key_value_digest,
        });
    }
    Ok(SstFileSet {
        paths: written.paths,
        file_bytes: written.file_bytes,
        files,
        logical_family_evidence,
    })
}

struct PipelineControlState {
    in_flight_bytes: usize,
    peak_accounted_bytes: usize,
    aborted: bool,
    error: Option<BulkLoadError>,
}

struct PipelineControl {
    max_queued_bytes: usize,
    aborted_flag: AtomicBool,
    state: Mutex<PipelineControlState>,
    released: Condvar,
}

impl PipelineControl {
    fn new(max_queued_bytes: usize) -> Self {
        Self {
            max_queued_bytes,
            aborted_flag: AtomicBool::new(false),
            state: Mutex::new(PipelineControlState {
                in_flight_bytes: 0,
                peak_accounted_bytes: 0,
                aborted: false,
                error: None,
            }),
            released: Condvar::new(),
        }
    }

    fn is_aborted(&self) -> bool {
        self.aborted_flag.load(Ordering::Acquire)
    }

    fn check_error(&self) -> Result<(), BulkLoadError> {
        if !self.is_aborted() {
            return Ok(());
        }
        Err(self.take_or_fallback_error())
    }

    fn acquire_budget(
        self: &Arc<Self>,
        bytes: usize,
        oversized_row: bool,
    ) -> Result<BatchReservation, BulkLoadError> {
        let mut guard = self.state.lock();
        loop {
            if guard.aborted {
                let err = guard.error.take().unwrap_or_else(|| {
                    BulkLoadError::invalid("ordered SST pipeline aborted after worker failure")
                });
                drop(guard);
                return Err(err);
            }
            let can_fit = if oversized_row {
                guard.in_flight_bytes == 0
            } else {
                guard
                    .in_flight_bytes
                    .checked_add(bytes)
                    .is_some_and(|total| total <= self.max_queued_bytes)
            };
            if can_fit {
                guard.in_flight_bytes = guard.in_flight_bytes.saturating_add(bytes);
                guard.peak_accounted_bytes = guard.peak_accounted_bytes.max(guard.in_flight_bytes);
                drop(guard);
                return Ok(BatchReservation {
                    bytes,
                    control: Arc::clone(self),
                });
            }
            self.released.wait(&mut guard);
        }
    }

    fn release_bytes(&self, bytes: usize) {
        let mut guard = self.state.lock();
        guard.in_flight_bytes = guard.in_flight_bytes.saturating_sub(bytes);
        drop(guard);
        self.released.notify_all();
    }

    fn abort(&self) {
        let mut guard = self.state.lock();
        guard.aborted = true;
        self.aborted_flag.store(true, Ordering::Release);
        drop(guard);
        self.released.notify_all();
    }

    fn fail(&self, failure: BulkLoadError) {
        let mut guard = self.state.lock();
        if guard.error.is_none() {
            guard.error = Some(failure);
        }
        guard.aborted = true;
        self.aborted_flag.store(true, Ordering::Release);
        drop(guard);
        self.released.notify_all();
    }

    fn fail_if_unterminated(&self, reason: &'static str) {
        let mut guard = self.state.lock();
        if !guard.aborted {
            if guard.error.is_none() {
                guard.error = Some(BulkLoadError::invalid(reason));
            }
            guard.aborted = true;
        }
        self.aborted_flag.store(true, Ordering::Release);
        drop(guard);
        self.released.notify_all();
    }

    fn take_or_fallback_error(&self) -> BulkLoadError {
        let mut guard = self.state.lock();
        guard.error.take().unwrap_or_else(|| {
            BulkLoadError::invalid("ordered SST pipeline aborted after worker failure")
        })
    }

    #[cfg(test)]
    fn peak_accounted_bytes(&self) -> usize {
        self.state.lock().peak_accounted_bytes
    }
}

struct BatchReservation {
    bytes: usize,
    control: Arc<PipelineControl>,
}

impl Drop for BatchReservation {
    fn drop(&mut self) {
        self.control.release_bytes(self.bytes);
    }
}

struct SstBatchEntry {
    key_len: u32,
    value_len: u32,
}

struct QueuedBatch {
    payload: Vec<u8>,
    entries: Vec<SstBatchEntry>,
    _reservation: BatchReservation,
}

struct StagingBatch {
    payload: Vec<u8>,
    entries: Vec<SstBatchEntry>,
    payload_cap: usize,
    entries_cap: usize,
    reservation: BatchReservation,
}

impl StagingBatch {
    fn allocate(
        control: &Arc<PipelineControl>,
        payload_cap: usize,
        entries_cap: usize,
        oversized_row: bool,
    ) -> Result<Self, BulkLoadError> {
        let entries_bytes = entries_cap
            .checked_mul(size_of::<SstBatchEntry>())
            .ok_or_else(|| BulkLoadError::invalid("SST pipeline batch size overflow"))?;
        let total_bytes = payload_cap
            .checked_add(entries_bytes)
            .ok_or_else(|| BulkLoadError::invalid("SST pipeline batch size overflow"))?;
        let reservation = control.acquire_budget(total_bytes, oversized_row)?;
        let mut payload = Vec::new();
        payload.try_reserve_exact(payload_cap).map_err(|source| {
            BulkLoadError::MemoryAllocation {
                operation: "SST pipeline batch payload",
                source,
            }
        })?;
        let mut entries = Vec::new();
        entries.try_reserve_exact(entries_cap).map_err(|source| {
            BulkLoadError::MemoryAllocation {
                operation: "SST pipeline batch entries",
                source,
            }
        })?;
        Ok(Self {
            payload,
            entries,
            payload_cap,
            entries_cap,
            reservation,
        })
    }

    fn can_fit(&self, row_payload_len: usize) -> bool {
        self.entries.len() < self.entries_cap
            && self
                .payload
                .len()
                .checked_add(row_payload_len)
                .is_some_and(|total| total <= self.payload_cap)
    }

    fn push(&mut self, key: &[u8], encoded_value: &[u8], key_len: u32, value_len: u32) {
        self.payload.extend_from_slice(key);
        self.payload.extend_from_slice(encoded_value);
        self.entries.push(SstBatchEntry { key_len, value_len });
    }

    fn seal(self) -> QueuedBatch {
        QueuedBatch {
            payload: self.payload,
            entries: self.entries,
            _reservation: self.reservation,
        }
    }
}

struct WriteWorkerConfig {
    staging_path: PathBuf,
    artifact_label: &'static str,
    options: Options,
    target_logical_bytes: u64,
}

type WriteWorkerResult = Option<WrittenSstFiles>;
type DigestWorkerResult = Option<(Vec<SstDigestFileMeta>, OrderedKeyValueEvidence)>;

struct PipelinedOrderedSstState {
    bounds: OrderedRowBounds,
    control: Arc<PipelineControl>,
    batch_payload_cap: usize,
    batch_entries_cap: usize,
    staging: Option<StagingBatch>,
    write_tx: Option<Sender<Arc<QueuedBatch>>>,
    digest_tx: Option<Sender<Arc<QueuedBatch>>>,
    write_handle: Option<JoinHandle<WriteWorkerResult>>,
    digest_handle: Option<JoinHandle<DigestWorkerResult>>,
}

impl PipelinedOrderedSstState {
    fn spawn(
        staging_path: &Path,
        artifact_label: &'static str,
        options: &Options,
        target_logical_bytes: u64,
        queue_budget_bytes: u64,
    ) -> Result<Self, BulkLoadError> {
        let max_queued_bytes = usize::try_from(queue_budget_bytes)
            .unwrap_or(usize::MAX)
            .max(1);
        let entry_size = size_of::<SstBatchEntry>();
        let target_batch_bytes = (max_queued_bytes / 4).min(PIPELINE_BATCH_MAX_BYTES);
        let batch_entries_cap =
            (target_batch_bytes / (entry_size * 4)).min(PIPELINE_BATCH_MAX_ENTRIES);
        let batch_payload_cap = if batch_entries_cap > 0 {
            target_batch_bytes.saturating_sub(batch_entries_cap * entry_size)
        } else {
            0
        };
        let control = Arc::new(PipelineControl::new(max_queued_bytes));
        let (write_tx, write_rx) = mpsc::channel();
        let (digest_tx, digest_rx) = mpsc::channel();
        let write_config = WriteWorkerConfig {
            staging_path: staging_path.to_path_buf(),
            artifact_label,
            options: options.clone(),
            target_logical_bytes,
        };
        let write_control = Arc::clone(&control);
        let write_handle = thread::Builder::new()
            .name("zinder-sst-write".to_owned())
            .spawn(move || run_write_worker(&write_config, &write_control, &write_rx))
            .map_err(|source| {
                BulkLoadError::invalid(format!("failed to start zinder-sst-write worker: {source}"))
            })?;
        let digest_control = Arc::clone(&control);
        let digest_handle = match thread::Builder::new()
            .name("zinder-sst-digest".to_owned())
            .spawn(move || run_digest_worker(target_logical_bytes, &digest_control, &digest_rx))
        {
            Ok(handle) => handle,
            Err(source) => {
                control.abort();
                drop(write_tx);
                drop(digest_tx);
                let _joined = write_handle.join();
                return Err(BulkLoadError::invalid(format!(
                    "failed to start zinder-sst-digest worker: {source}"
                )));
            }
        };
        Ok(Self {
            bounds: OrderedRowBounds::default(),
            control,
            batch_payload_cap,
            batch_entries_cap,
            staging: None,
            write_tx: Some(write_tx),
            digest_tx: Some(digest_tx),
            write_handle: Some(write_handle),
            digest_handle: Some(digest_handle),
        })
    }

    fn put(&mut self, key: &[u8], encoded_value: &[u8]) -> Result<(), BulkLoadError> {
        self.control.check_error()?;
        let key_len = u32::try_from(key.len())
            .map_err(|_| BulkLoadError::invalid("SST key length exceeds u32::MAX"))?;
        let value_len = u32::try_from(encoded_value.len())
            .map_err(|_| BulkLoadError::invalid("SST value length exceeds u32::MAX"))?;
        let row_payload_len = key
            .len()
            .checked_add(encoded_value.len())
            .ok_or_else(|| BulkLoadError::invalid("SST row byte length exceeds usize::MAX"))?;
        self.bounds.prepare_and_commit(key, encoded_value)?;
        if let Some(staging) = self.staging.as_ref()
            && !staging.can_fit(row_payload_len)
        {
            self.flush_staging()?;
        }
        if self.batch_entries_cap == 0 || row_payload_len > self.batch_payload_cap {
            let entry_bytes = size_of::<SstBatchEntry>();
            let exact_bytes = row_payload_len
                .checked_add(entry_bytes)
                .ok_or_else(|| BulkLoadError::invalid("SST pipeline batch size overflow"))?;
            let oversized = exact_bytes > self.control.max_queued_bytes;
            let mut single = StagingBatch::allocate(&self.control, row_payload_len, 1, oversized)?;
            single.push(key, encoded_value, key_len, value_len);
            return self.enqueue_batch(single.seal());
        }
        if self.staging.is_none() {
            self.staging = Some(StagingBatch::allocate(
                &self.control,
                self.batch_payload_cap,
                self.batch_entries_cap,
                false,
            )?);
        }
        let staging = self.staging.as_mut().ok_or_else(|| {
            BulkLoadError::invalid("ordered SST pipeline did not retain its staging batch")
        })?;
        staging.push(key, encoded_value, key_len, value_len);
        Ok(())
    }

    fn flush_staging(&mut self) -> Result<(), BulkLoadError> {
        let Some(staging) = self.staging.take() else {
            return Ok(());
        };
        self.enqueue_batch(staging.seal())
    }

    fn enqueue_batch(&self, batch: QueuedBatch) -> Result<(), BulkLoadError> {
        let shared = Arc::new(batch);
        let write_tx = self
            .write_tx
            .as_ref()
            .ok_or_else(|| BulkLoadError::invalid("ordered SST write channel already closed"))?;
        if write_tx.send(Arc::clone(&shared)).is_err() {
            return Err(self.control.take_or_fallback_error());
        }
        let digest_tx = self
            .digest_tx
            .as_ref()
            .ok_or_else(|| BulkLoadError::invalid("ordered SST digest channel already closed"))?;
        if digest_tx.send(shared).is_err() {
            return Err(self.control.take_or_fallback_error());
        }
        Ok(())
    }

    fn finish(mut self) -> Result<SstFileSet, BulkLoadError> {
        let flush_outcome = self.flush_staging();
        drop(self.write_tx.take());
        drop(self.digest_tx.take());
        let write_join = self.write_handle.take().map(JoinHandle::join);
        let digest_join = self.digest_handle.take().map(JoinHandle::join);
        flush_outcome?;
        let written = match write_join {
            Some(Ok(Some(written_files))) => written_files,
            Some(Ok(None) | Err(_)) => return Err(self.control.take_or_fallback_error()),
            None => return Err(BulkLoadError::invalid("ordered SST write worker missing")),
        };
        let (digested_files, logical_family_evidence) = match digest_join {
            Some(Ok(Some(digested))) => digested,
            Some(Ok(None) | Err(_)) => return Err(self.control.take_or_fallback_error()),
            None => return Err(BulkLoadError::invalid("ordered SST digest worker missing")),
        };
        assemble_sst_file_set(written, digested_files, logical_family_evidence)
    }
}

impl Drop for PipelinedOrderedSstState {
    fn drop(&mut self) {
        self.staging = None;
        self.control.abort();
        drop(self.write_tx.take());
        drop(self.digest_tx.take());
        if let Some(handle) = self.write_handle.take() {
            let _joined = handle.join();
        }
        if let Some(handle) = self.digest_handle.take() {
            let _joined = handle.join();
        }
    }
}

struct WorkerPanicGuard {
    control: Arc<PipelineControl>,
    reason: &'static str,
    armed: bool,
}

impl WorkerPanicGuard {
    fn new(control: Arc<PipelineControl>, reason: &'static str) -> Self {
        Self {
            control,
            reason,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for WorkerPanicGuard {
    fn drop(&mut self) {
        if self.armed {
            self.control.fail_if_unterminated(self.reason);
        }
    }
}

fn for_each_batch_entry<F>(batch: &QueuedBatch, mut visitor: F) -> Result<(), BulkLoadError>
where
    F: FnMut(&[u8], &[u8], u64) -> Result<(), BulkLoadError>,
{
    let mut offset = 0_usize;
    for entry in &batch.entries {
        let key_len = usize::try_from(entry.key_len)
            .map_err(|_| BulkLoadError::invalid("SST batch key length exceeds usize::MAX"))?;
        let val_len = usize::try_from(entry.value_len)
            .map_err(|_| BulkLoadError::invalid("SST batch value length exceeds usize::MAX"))?;
        let key_end = offset
            .checked_add(key_len)
            .ok_or_else(|| BulkLoadError::invalid("SST batch key slice overflow"))?;
        let val_end = key_end
            .checked_add(val_len)
            .ok_or_else(|| BulkLoadError::invalid("SST batch value slice overflow"))?;
        let key = batch
            .payload
            .get(offset..key_end)
            .ok_or_else(|| BulkLoadError::invalid("SST batch key slice out of bounds"))?;
        let encoded_value = batch
            .payload
            .get(key_end..val_end)
            .ok_or_else(|| BulkLoadError::invalid("SST batch value slice out of bounds"))?;
        let row_logical_bytes = logical_row_bytes(key_len, val_len)?;
        visitor(key, encoded_value, row_logical_bytes)?;
        offset = val_end;
    }
    Ok(())
}

fn run_write_worker(
    config: &WriteWorkerConfig,
    control: &Arc<PipelineControl>,
    rx: &Receiver<Arc<QueuedBatch>>,
) -> WriteWorkerResult {
    let mut panic_guard = WorkerPanicGuard::new(
        Arc::clone(control),
        "ordered SST write worker terminated unexpectedly",
    );
    let outcome = (|| -> Result<Option<WrittenSstFiles>, BulkLoadError> {
        let mut physical = SstPhysicalWriter::new(
            &config.staging_path,
            config.artifact_label,
            &config.options,
            config.target_logical_bytes,
        );
        while !control.is_aborted() {
            let Ok(batch) = rx.recv() else {
                break;
            };
            if control.is_aborted() {
                break;
            }
            for_each_batch_entry(&batch, |key, encoded_value, row_logical_bytes| {
                physical.put(key, encoded_value, row_logical_bytes)
            })?;
        }
        if control.is_aborted() {
            return Ok(None);
        }
        physical.finish().map(Some)
    })();
    panic_guard.disarm();
    match outcome {
        Ok(written) => written,
        Err(failure) => {
            control.fail(failure);
            None
        }
    }
}

fn run_digest_worker(
    target_logical_bytes: u64,
    control: &Arc<PipelineControl>,
    rx: &Receiver<Arc<QueuedBatch>>,
) -> DigestWorkerResult {
    let mut panic_guard = WorkerPanicGuard::new(
        Arc::clone(control),
        "ordered SST digest worker terminated unexpectedly",
    );
    let outcome =
        (|| -> Result<Option<(Vec<SstDigestFileMeta>, OrderedKeyValueEvidence)>, BulkLoadError> {
            let mut digest = SstDigestAccumulator::new(target_logical_bytes);
            while !control.is_aborted() {
                let Ok(batch) = rx.recv() else {
                    break;
                };
                if control.is_aborted() {
                    break;
                }
                for_each_batch_entry(&batch, |key, encoded_value, _| {
                    digest.put(key, encoded_value)
                })?;
            }
            if control.is_aborted() {
                return Ok(None);
            }
            digest.finish().map(Some)
        })();
    panic_guard.disarm();
    match outcome {
        Ok(digested) => digested,
        Err(failure) => {
            control.fail(failure);
            None
        }
    }
}

fn update_ordered_key_value_digest(
    digest: &mut Sha256,
    key_length: u64,
    key: &[u8],
    encoded_value_length: u64,
    encoded_value: &[u8],
) {
    digest.update(key_length.to_le_bytes());
    digest.update(key);
    digest.update(encoded_value_length.to_le_bytes());
    digest.update(encoded_value);
}

/// Bounded external sorter for opaque fixed-width records.
///
/// Records are ordered lexicographically. `KEY_LEN` is supplied at finish so
/// the sorter can reject duplicate logical keys before emitting SST files.
pub struct FixedRecordSorter<const RECORD_LEN: usize> {
    staging_path: PathBuf,
    artifact_label: &'static str,
    record_capacity: usize,
    records: Vec<[u8; RECORD_LEN]>,
    runs: Vec<PathBuf>,
    next_run_index: u64,
}

impl<const RECORD_LEN: usize> FixedRecordSorter<RECORD_LEN> {
    /// Creates a sorter with a caller-computed in-memory record capacity.
    pub fn new(
        staging_path: &Path,
        artifact_label: &'static str,
        record_capacity: usize,
    ) -> Result<Self, BulkLoadError> {
        if RECORD_LEN == 0 {
            return Err(BulkLoadError::invalid(
                "fixed records must contain at least one byte",
            ));
        }
        if record_capacity == 0 {
            return Err(BulkLoadError::invalid(
                "fixed-record sort memory must hold at least one record",
            ));
        }
        Ok(Self {
            staging_path: staging_path.to_path_buf(),
            artifact_label,
            record_capacity,
            records: Vec::with_capacity(record_capacity),
            runs: Vec::new(),
            next_run_index: 0,
        })
    }

    /// Stages one opaque record and flushes a sorted run at the memory bound.
    pub fn push(&mut self, record: [u8; RECORD_LEN]) -> Result<(), BulkLoadError> {
        self.records.push(record);
        if self.records.len() >= self.record_capacity {
            self.flush_run()?;
        }
        Ok(())
    }

    /// Merges every run and emits ordered SST files split at `KEY_LEN`.
    pub fn finish<const KEY_LEN: usize>(
        mut self,
        options: &Options,
        sst_target_logical_bytes: u64,
    ) -> Result<SstFileSet, BulkLoadError> {
        validate_key_length::<RECORD_LEN, KEY_LEN>()?;
        if sst_target_logical_bytes == 0 {
            return Err(BulkLoadError::invalid(
                "SST target logical bytes must be greater than zero",
            ));
        }
        self.flush_run()?;
        let merge_fan_in = effective_merge_fan_in();
        let Some(sorted_run) = merge_sort_runs::<RECORD_LEN, KEY_LEN>(
            &self.staging_path,
            self.artifact_label,
            self.runs,
            merge_fan_in,
        )?
        else {
            return Ok(SstFileSet::default());
        };
        let file =
            File::open(&sorted_run).map_err(|source| path_unavailable(&sorted_run, source))?;
        let mut reader = BufReader::new(file);
        let mut writer = OrderedSstWriter::new(
            &self.staging_path,
            self.artifact_label,
            options,
            sst_target_logical_bytes,
        )?;
        while let Some(record) = read_fixed_record::<RECORD_LEN>(&mut reader, &sorted_run)? {
            writer.put(&record[..KEY_LEN], &record[KEY_LEN..])?;
        }
        let files = writer.finish()?;
        fs::remove_file(&sorted_run).map_err(|source| path_unavailable(&sorted_run, source))?;
        Ok(files)
    }

    fn flush_run(&mut self) -> Result<(), BulkLoadError> {
        if self.records.is_empty() {
            return Ok(());
        }
        self.records.sort_unstable();
        let path = self.staging_path.join(format!(
            "{}-run-{:08}.bin",
            self.artifact_label, self.next_run_index
        ));
        self.next_run_index = self
            .next_run_index
            .checked_add(1)
            .ok_or_else(|| BulkLoadError::invalid("sort run count exceeds u64::MAX"))?;
        let file = File::create(&path).map_err(|source| path_unavailable(&path, source))?;
        let mut writer = BufWriter::new(file);
        for record in self.records.drain(..) {
            writer
                .write_all(&record)
                .map_err(|source| path_unavailable(&path, source))?;
        }
        writer
            .flush()
            .map_err(|source| path_unavailable(&path, source))?;
        self.runs.push(path);
        Ok(())
    }
}

/// Calculates how many fixed-width records fit inside a byte limit.
pub fn fixed_record_capacity<const RECORD_LEN: usize>(
    memory_bytes: usize,
) -> Result<usize, BulkLoadError> {
    if RECORD_LEN == 0 {
        return Err(BulkLoadError::invalid(
            "fixed records must contain at least one byte",
        ));
    }
    let capacity = memory_bytes / RECORD_LEN;
    if capacity == 0 {
        return Err(BulkLoadError::invalid(
            "fixed-record sort memory must hold at least one record",
        ));
    }
    Ok(capacity)
}

fn validate_key_length<const RECORD_LEN: usize, const KEY_LEN: usize>() -> Result<(), BulkLoadError>
{
    if KEY_LEN == 0 || KEY_LEN > RECORD_LEN {
        return Err(BulkLoadError::invalid(
            "fixed-record key length must be between one and the record length",
        ));
    }
    Ok(())
}

fn merge_sort_runs<const RECORD_LEN: usize, const KEY_LEN: usize>(
    staging_path: &Path,
    artifact_label: &'static str,
    mut runs: Vec<PathBuf>,
    merge_fan_in: usize,
) -> Result<Option<PathBuf>, BulkLoadError> {
    validate_merge_fan_in(merge_fan_in)?;
    let mut pass = 0_u64;
    while runs.len() > 1 {
        let mut merged_runs = Vec::with_capacity(runs.len().div_ceil(merge_fan_in));
        for (group_index, group) in runs.chunks(merge_fan_in).enumerate() {
            let path = staging_path.join(format!(
                "{artifact_label}-merge-{pass:04}-{group_index:08}.bin"
            ));
            merge_run_group::<RECORD_LEN, KEY_LEN>(group, &path, artifact_label, merge_fan_in)?;
            for input in group {
                fs::remove_file(input).map_err(|source| path_unavailable(input, source))?;
            }
            merged_runs.push(path);
        }
        runs = merged_runs;
        pass = pass
            .checked_add(1)
            .ok_or_else(|| BulkLoadError::invalid("sort merge pass count exceeds u64::MAX"))?;
    }
    Ok(runs.pop())
}

fn merge_run_group<const RECORD_LEN: usize, const KEY_LEN: usize>(
    inputs: &[PathBuf],
    output: &Path,
    artifact_label: &'static str,
    merge_fan_in: usize,
) -> Result<(), BulkLoadError> {
    validate_merge_fan_in(merge_fan_in)?;
    if inputs.len() > merge_fan_in {
        return Err(BulkLoadError::invalid(
            "sort merge exceeded its file-descriptor fan-in",
        ));
    }
    let mut readers = Vec::with_capacity(inputs.len());
    let mut heap = BinaryHeap::new();
    for (reader_index, path) in inputs.iter().enumerate() {
        let file = File::open(path).map_err(|source| path_unavailable(path, source))?;
        let mut reader = BufReader::new(file);
        if let Some(record) = read_fixed_record::<RECORD_LEN>(&mut reader, path)? {
            heap.push(Reverse((record, reader_index)));
        }
        readers.push(reader);
    }
    let file = File::create(output).map_err(|source| path_unavailable(output, source))?;
    let mut writer = BufWriter::new(file);
    let mut previous_key: Option<Vec<u8>> = None;
    while let Some(Reverse((record, reader_index))) = heap.pop() {
        if previous_key
            .as_deref()
            .is_some_and(|previous| previous == &record[..KEY_LEN])
        {
            return Err(duplicate_key_error(artifact_label));
        }
        writer
            .write_all(&record)
            .map_err(|source| path_unavailable(output, source))?;
        previous_key = Some(record[..KEY_LEN].to_vec());
        if let Some(next) =
            read_fixed_record::<RECORD_LEN>(&mut readers[reader_index], &inputs[reader_index])?
        {
            heap.push(Reverse((next, reader_index)));
        }
    }
    writer
        .flush()
        .map_err(|source| path_unavailable(output, source))
}

fn duplicate_key_error(artifact_label: &'static str) -> BulkLoadError {
    BulkLoadError::invalid(format!("{artifact_label} contains a duplicate logical key"))
}

fn read_fixed_record<const RECORD_LEN: usize>(
    reader: &mut BufReader<File>,
    path: &Path,
) -> Result<Option<[u8; RECORD_LEN]>, BulkLoadError> {
    let mut record = [0_u8; RECORD_LEN];
    let first_byte_count = reader
        .read(&mut record[..1])
        .map_err(|source| path_unavailable(path, source))?;
    if first_byte_count == 0 {
        return Ok(None);
    }
    reader
        .read_exact(&mut record[1..])
        .map_err(|source| path_unavailable(path, source))?;
    Ok(Some(record))
}

fn logical_row_bytes(key_bytes: usize, value_bytes: usize) -> Result<u64, BulkLoadError> {
    let row_bytes = key_bytes
        .checked_add(value_bytes)
        .ok_or_else(|| BulkLoadError::invalid("logical row bytes exceed usize::MAX"))?;
    u64::try_from(row_bytes)
        .map_err(|_| BulkLoadError::invalid("logical row bytes exceed u64::MAX"))
}

fn path_unavailable(path: &Path, source: std::io::Error) -> BulkLoadError {
    BulkLoadError::PathUnavailable {
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn fixed_record_sorter_rejects_memory_smaller_than_one_record() {
        assert!(matches!(
            fixed_record_capacity::<72>(71),
            Err(BulkLoadError::InvalidInput { .. })
        ));
    }

    #[test]
    fn fixed_record_sorter_rejects_same_key_with_different_values_across_runs()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let mut sorter = FixedRecordSorter::<4>::new(temporary.path(), "duplicate", 1)?;
        sorter.push([1, 2, 3, 4])?;
        sorter.push([1, 2, 5, 6])?;

        let error = sorter
            .finish::<2>(&Options::default(), 1_024)
            .err()
            .ok_or("duplicate logical keys must fail")?;

        assert!(matches!(error, BulkLoadError::InvalidInput { .. }));
        Ok(())
    }

    #[test]
    fn ordered_sst_writer_rejects_non_increasing_keys() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let options = Options::default();
        let mut writer = OrderedSstWriter::new(temporary.path(), "rows", &options, 1_024)?;
        writer.put(b"b", b"value")?;

        assert!(matches!(
            writer.put(b"a", b"value"),
            Err(BulkLoadError::InvalidInput { .. })
        ));
        Ok(())
    }

    #[test]
    fn path_failure_display_includes_the_underlying_io_cause() {
        let failure = BulkLoadError::PathUnavailable {
            path: PathBuf::from("/staging/run.bin"),
            source: std::io::Error::from_raw_os_error(28),
        };

        assert!(
            failure.to_string().contains("No space left on device"),
            "path failures must preserve the actionable operating-system cause: {failure}"
        );
    }

    #[test]
    fn ordered_family_evidence_rejects_non_increasing_keys_without_mutation()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut evidence = OrderedKeyValueEvidenceAccumulator::new();
        evidence.record(b"b", b"two")?;
        assert!(matches!(
            evidence.record(b"a", b"one"),
            Err(BulkLoadError::InvalidInput { .. })
        ));

        let mut expected = OrderedKeyValueEvidenceAccumulator::new();
        expected.record(b"b", b"two")?;
        assert_eq!(evidence.finish(), expected.finish());
        Ok(())
    }

    #[test]
    fn empty_fixed_record_sorter_still_rejects_zero_sst_target()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let sorter = FixedRecordSorter::<4>::new(temporary.path(), "empty", 1)?;

        assert!(matches!(
            sorter.finish::<2>(&Options::default(), 0),
            Err(BulkLoadError::InvalidInput { .. })
        ));
        Ok(())
    }

    #[test]
    fn ordered_sst_writer_records_domain_separated_per_file_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let options = Options::default();
        let mut writer = OrderedSstWriter::new(temporary.path(), "evidence", &options, 2)?;
        writer.put(b"a", b"1")?;
        writer.put(b"b", b"22")?;
        let files = writer.finish()?;

        assert_eq!(files.files.len(), 2);
        assert_eq!(files.files[0].ordinal, 0);
        assert_eq!(files.files[0].entry_count, 1);
        assert_eq!(files.files[0].first_key, b"a");
        assert_eq!(files.files[0].last_key, b"a");
        assert_eq!(
            files.files[0].ordered_key_value_digest,
            [
                75, 165, 213, 127, 94, 102, 70, 148, 94, 23, 94, 209, 179, 67, 79, 62, 24, 149,
                140, 126, 57, 10, 152, 114, 152, 56, 86, 58, 133, 19, 85, 98,
            ]
        );
        assert_eq!(files.files[1].ordinal, 1);
        assert_eq!(files.files[1].entry_count, 1);
        assert_eq!(files.files[1].first_key, b"b");
        assert_eq!(files.files[1].last_key, b"b");
        assert!(files.files.iter().all(|file| file.file_bytes > 0));
        Ok(())
    }

    #[test]
    fn ordered_family_evidence_is_independent_of_sst_rotation()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let options = Options::default();
        let rows = [
            (b"a".as_slice(), b"1".as_slice()),
            (b"b", b"2"),
            (b"c", b"3"),
        ];

        let mut rotating = OrderedSstWriter::new(temporary.path(), "rotating", &options, 2)?;
        let mut single = OrderedSstWriter::new(temporary.path(), "single", &options, 1_024)?;
        for (key, value) in rows {
            rotating.put(key, value)?;
            single.put(key, value)?;
        }
        let rotating = rotating.finish()?;
        let single = single.finish()?;

        assert_eq!(rotating.files.len(), 3);
        assert_eq!(single.files.len(), 1);
        assert_eq!(
            rotating.logical_family_evidence,
            single.logical_family_evidence
        );
        assert_eq!(rotating.logical_family_evidence.row_count, 3);
        assert_eq!(rotating.logical_family_evidence.logical_bytes, 6);
        assert_eq!(
            rotating.logical_family_evidence.first_key.as_deref(),
            Some(b"a".as_slice())
        );
        assert_eq!(
            rotating.logical_family_evidence.last_key.as_deref(),
            Some(b"c".as_slice())
        );
        Ok(())
    }

    #[test]
    fn empty_sst_sets_retain_domain_separated_logical_family_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let options = Options::default();
        let writer = OrderedSstWriter::new(temporary.path(), "empty-writer", &options, 1_024)?;
        let writer_files = writer.finish()?;
        let sorter = FixedRecordSorter::<4>::new(temporary.path(), "empty-sorter", 1)?;
        let sorter_files = sorter.finish::<2>(&options, 1_024)?;
        let mut digest = Sha256::new();
        digest.update(ORDERED_KEY_VALUE_EVIDENCE_DIGEST_DOMAIN);
        let empty_digest: [u8; 32] = digest.finalize().into();

        for files in [writer_files, sorter_files] {
            assert_eq!(files.paths, [] as [std::path::PathBuf; 0]);
            assert_eq!(files.files, []);
            assert_eq!(files.file_bytes, 0);
            assert_eq!(files.logical_family_evidence.row_count, 0);
            assert_eq!(files.logical_family_evidence.logical_bytes, 0);
            assert_eq!(files.logical_family_evidence.first_key, None);
            assert_eq!(files.logical_family_evidence.last_key, None);
            assert_eq!(
                files.logical_family_evidence.ordered_key_value_digest,
                empty_digest
            );
        }
        Ok(())
    }

    #[test]
    fn fixed_record_sorter_preserves_per_file_evidence_after_external_sort()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let mut sorter = FixedRecordSorter::<4>::new(temporary.path(), "reverse", 1)?;
        sorter.push([2, 0, 0, 2])?;
        sorter.push([1, 0, 0, 1])?;
        let files = sorter.finish::<2>(&Options::default(), 1_024)?;

        assert_eq!(files.files.len(), 1);
        assert_eq!(files.files[0].entry_count, 2);
        assert_eq!(files.files[0].first_key, [1, 0]);
        assert_eq!(files.files[0].last_key, [2, 0]);
        assert_ne!(files.files[0].ordered_key_value_digest, [0; 32]);
        assert_eq!(files.logical_family_evidence.row_count, 2);
        assert_eq!(files.logical_family_evidence.logical_bytes, 8);
        assert_eq!(files.logical_family_evidence.first_key, Some(vec![1, 0]));
        assert_eq!(files.logical_family_evidence.last_key, Some(vec![2, 0]));
        Ok(())
    }

    #[test]
    fn effective_merge_fan_in_scales_with_descriptor_headroom() {
        assert_eq!(
            effective_merge_fan_in_for_limits(None, Some(20)),
            MAX_MERGE_FAN_IN
        );
        assert_eq!(
            effective_merge_fan_in_for_limits(Some(1_024), Some(20)),
            MAX_MERGE_FAN_IN
        );
        assert_eq!(effective_merge_fan_in_for_limits(Some(256), Some(20)), 118);
        assert_eq!(effective_merge_fan_in_for_limits(Some(256), None), 64);
        assert_eq!(effective_merge_fan_in_for_limits(Some(256), Some(200)), 28);
        assert_eq!(
            effective_merge_fan_in_for_limits(Some(256), Some(252)),
            MIN_MERGE_FAN_IN
        );
        assert_eq!(
            effective_merge_fan_in_for_limits(Some(16), Some(16)),
            MIN_MERGE_FAN_IN
        );
    }

    #[cfg(unix)]
    #[test]
    fn external_merges_succeed_under_256_file_descriptor_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        const CHILD_ENV: &str = "ZINDER_BULK_LOAD_TEST_NOFILE_256_CHILD";
        if std::env::var_os(CHILD_ENV).is_none() {
            let binary = std::env::current_exe()?;
            let status = std::process::Command::new(binary)
                .args([
                    "--exact",
                    "bulk_load::tests::external_merges_succeed_under_256_file_descriptor_limit",
                    "--nocapture",
                ])
                .env(CHILD_ENV, "1")
                .status()?;
            assert!(status.success(), "child process failed with {status}");
            return Ok(());
        }

        let current_limit = rustix::process::getrlimit(rustix::process::Resource::Nofile);
        let bounded_soft = current_limit.current.map_or(256, |soft| soft.min(256));
        rustix::process::setrlimit(
            rustix::process::Resource::Nofile,
            rustix::process::Rlimit {
                current: Some(bounded_soft),
                maximum: current_limit.maximum,
            },
        )?;
        let active_fan_in = effective_merge_fan_in();
        assert!(active_fan_in >= MIN_MERGE_FAN_IN);
        assert!(active_fan_in < MAX_MERGE_FAN_IN);

        assert_fixed_record_sort_handles_256_runs()?;
        assert_serial_variable_sort_handles_256_runs()?;
        assert_parallel_variable_sort_handles_256_runs()?;
        Ok(())
    }

    #[cfg(unix)]
    fn assert_fixed_record_sort_handles_256_runs() -> Result<(), Box<dyn std::error::Error>> {
        let staging_dir = TempDir::new()?;
        let mut sorter = FixedRecordSorter::<4>::new(staging_dir.path(), "fixed-low-nofile", 1)?;
        for byte in (0..=255_u8).rev() {
            sorter.push([0, byte, 0, byte])?;
        }
        let sst_files = sorter.finish::<2>(&Options::default(), 64 * 1024)?;
        assert_eq!(sst_files.logical_family_evidence.row_count, 256);
        assert_eq!(
            sst_files.logical_family_evidence.first_key,
            Some(vec![0, 0])
        );
        assert_eq!(
            sst_files.logical_family_evidence.last_key,
            Some(vec![0, 255])
        );
        Ok(())
    }

    #[cfg(unix)]
    fn assert_serial_variable_sort_handles_256_runs() -> Result<(), Box<dyn std::error::Error>> {
        let staging_dir = TempDir::new()?;
        let memory_limit = variable_value_sort::one_record_memory_limit_bytes::<2>(1);
        let mut sorter = VariableValueSorter::<2>::new(
            staging_dir.path(),
            "serial-low-nofile",
            memory_limit,
            1024 * 1024,
        )?;
        for index in (0_u16..256).rev() {
            let key_bytes = index.to_be_bytes();
            sorter.push(key_bytes, &[key_bytes[1]])?;
        }
        let mut ordered_stream = sorter.finish()?;
        let evidence = ordered_stream.evidence();
        assert_eq!(evidence.initial_run_count, 256);
        assert!(evidence.merge_pass_count >= 1);
        let mut observed_keys = Vec::new();
        while let Some(record) = ordered_stream.next_record()? {
            observed_keys.push(record.key);
        }
        assert_eq!(
            observed_keys,
            (0_u16..256).map(u16::to_be_bytes).collect::<Vec<_>>()
        );
        Ok(())
    }

    #[cfg(unix)]
    fn assert_parallel_variable_sort_handles_256_runs() -> Result<(), Box<dyn std::error::Error>> {
        let staging_dir = TempDir::new()?;
        let shard_count = std::num::NonZeroU32::new(4).ok_or("zero shards")?;
        let per_record_bytes = variable_value_sort::one_record_memory_limit_bytes::<2>(1);
        let parallel = ParallelVariableValueSorter::<2>::new(
            staging_dir.path(),
            "parallel-low-nofile",
            shard_count,
            per_record_bytes * u64::from(shard_count.get()),
            1024 * 1024,
        )?;
        let mut active_shards = parallel.open_shards()?;
        for index in 0_u16..256 {
            let shard_idx = usize::from(index) % active_shards.len();
            let key_bytes = (255 - index).to_be_bytes();
            active_shards[shard_idx].push(key_bytes, &[key_bytes[1]])?;
        }
        let staged_runs = parallel.flush_shards(active_shards)?;
        let mut sorted = parallel.finish(staged_runs)?;
        let evidence = sorted.evidence();
        assert_eq!(evidence.initial_run_count, 256);
        assert!(evidence.merge_pass_count >= 1);
        let mut observed_keys = Vec::new();
        while let Some(record) = sorted.next_record()? {
            observed_keys.push(record.key);
        }
        assert_eq!(
            observed_keys,
            (0_u16..256).map(u16::to_be_bytes).collect::<Vec<_>>()
        );
        Ok(())
    }

    fn assert_sst_evidence_matches(expected: &SstFileSet, actual: &SstFileSet) {
        assert_eq!(
            expected.logical_family_evidence,
            actual.logical_family_evidence
        );
        assert_eq!(expected.paths.len(), actual.paths.len());
        assert_eq!(expected.files.len(), actual.files.len());
        if !expected.files.is_empty() {
            assert!(expected.file_bytes > 0);
            assert!(actual.file_bytes > 0);
        }
        for (expected_path, actual_path) in expected.paths.iter().zip(actual.paths.iter()) {
            assert_eq!(expected_path.file_name(), actual_path.file_name());
        }
        for (expected_file, actual_file) in expected.files.iter().zip(actual.files.iter()) {
            assert!(actual_file.file_bytes > 0);
            assert_eq!(actual_file.ordinal, expected_file.ordinal);
            assert_eq!(actual_file.entry_count, expected_file.entry_count);
            assert_eq!(actual_file.first_key, expected_file.first_key);
            assert_eq!(actual_file.last_key, expected_file.last_key);
            assert_eq!(
                actual_file.ordered_key_value_digest,
                expected_file.ordered_key_value_digest
            );
        }
    }

    type IngestedRows = Vec<(Box<[u8]>, Box<[u8]>)>;

    fn read_sst_entries(paths: &[PathBuf]) -> Result<IngestedRows, Box<dyn std::error::Error>> {
        let db_dir = TempDir::new()?;
        let mut db_options = Options::default();
        db_options.create_if_missing(true);
        let db = rust_rocksdb::DB::open(&db_options, db_dir.path())?;
        if !paths.is_empty() {
            let ingest_opts = rust_rocksdb::IngestExternalFileOptions::default();
            db.ingest_external_file_opts(&ingest_opts, paths.to_vec())?;
        }
        let rows = db
            .iterator(rust_rocksdb::IteratorMode::Start)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    #[test]
    fn pipelined_ordered_sst_writer_matches_sync_evidence_and_ingested_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let options = Options::default();

        // Empty input produces identical default SstFileSet.
        let empty_sync_dir = TempDir::new()?;
        let empty_pipe_dir = TempDir::new()?;
        let empty_sync =
            OrderedSstWriter::new(empty_sync_dir.path(), "empty", &options, 64)?.finish()?;
        let empty_pipe =
            OrderedSstWriter::new_pipelined(empty_pipe_dir.path(), "empty", &options, 64, 512)?
                .finish()?;
        assert_eq!(empty_pipe, SstFileSet::default());
        assert_eq!(empty_pipe, empty_sync);

        // Multi-file rotation + oversized single row (> queue_budget_bytes).
        let sync_dir = TempDir::new()?;
        let pipe_dir = TempDir::new()?;
        let mut sync_writer = OrderedSstWriter::new(sync_dir.path(), "rows", &options, 180)?;
        let mut pipe_writer =
            OrderedSstWriter::new_pipelined(pipe_dir.path(), "rows", &options, 180, 256)?;
        let mut expected_rows = Vec::with_capacity(40);
        for index in 0_u16..40 {
            let key = index.to_be_bytes();
            let payload = if index == 17 {
                vec![0x5a_u8; 640]
            } else {
                vec![u8::try_from(index % 251).unwrap_or(0); 32]
            };
            sync_writer.put(&key, &payload)?;
            pipe_writer.put(&key, &payload)?;
            expected_rows.push((key.to_vec().into_boxed_slice(), payload.into_boxed_slice()));
        }
        let sync_set = sync_writer.finish()?;
        let pipe_set = pipe_writer.finish()?;
        assert!(sync_set.files.len() > 1);
        assert_sst_evidence_matches(&sync_set, &pipe_set);
        assert_eq!(read_sst_entries(&pipe_set.paths)?, expected_rows);
        assert_eq!(
            read_sst_entries(&pipe_set.paths)?,
            read_sst_entries(&sync_set.paths)?
        );
        Ok(())
    }

    #[test]
    fn pipelined_ordered_sst_writer_enforces_queue_budget_backpressure()
    -> Result<(), Box<dyn std::error::Error>> {
        let sync_dir = TempDir::new()?;
        let pipe_dir = TempDir::new()?;
        let options = Options::default();
        let queue_budget_bytes = 256_u64;
        let mut sync_writer = OrderedSstWriter::new(sync_dir.path(), "bp", &options, 512)?;
        let mut pipe_writer = OrderedSstWriter::new_pipelined(
            pipe_dir.path(),
            "bp",
            &options,
            512,
            queue_budget_bytes,
        )?;
        for index in 0_u16..160 {
            let key = index.to_be_bytes();
            let payload = [u8::try_from(index % 199).unwrap_or(0); 32];
            sync_writer.put(&key, &payload)?;
            pipe_writer.put(&key, &payload)?;
        }
        assert!(
            pipe_writer.peak_pipeline_accounted_bytes() <= usize::try_from(queue_budget_bytes)?
        );
        let sync_set = sync_writer.finish()?;
        let pipe_set = pipe_writer.finish()?;
        assert_sst_evidence_matches(&sync_set, &pipe_set);
        assert_eq!(
            read_sst_entries(&pipe_set.paths)?,
            read_sst_entries(&sync_set.paths)?
        );
        Ok(())
    }

    #[test]
    fn pipelined_ordered_sst_writer_rejects_non_increasing_key_and_cleans_up()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let options = Options::default();
        let mut writer = OrderedSstWriter::new_pipelined(dir.path(), "order", &options, 1024, 512)?;
        writer.put(b"b", b"two")?;
        assert!(matches!(
            writer.put(b"a", b"one"),
            Err(BulkLoadError::InvalidInput { .. })
        ));
        drop(writer);

        assert!(matches!(
            OrderedSstWriter::new_pipelined(dir.path(), "bad-budget", &options, 1024, 0),
            Err(BulkLoadError::InvalidInput { .. })
        ));
        assert!(matches!(
            OrderedSstWriter::new_pipelined(dir.path(), "bad-target", &options, 0, 512),
            Err(BulkLoadError::InvalidInput { .. })
        ));
        Ok(())
    }

    #[test]
    fn pipelined_ordered_sst_writer_surfaces_write_worker_root_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let blocking_dir = dir.path().join("blocked-00000000.sst");
        fs::create_dir(&blocking_dir)?;
        let options = Options::default();
        let mut writer =
            OrderedSstWriter::new_pipelined(dir.path(), "blocked", &options, 1024, 4096)?;
        writer.put(b"a", b"1")?;
        let Err(failure) = writer.finish() else {
            return Err("expected blocked SST path to fail write worker".into());
        };
        assert!(matches!(
            failure,
            BulkLoadError::RocksDbOperation {
                operation: "ordered SST open",
                ..
            }
        ));

        let missing = TempDir::new()?.path().join("nonexistent-dir");
        let mut streaming_writer =
            OrderedSstWriter::new_pipelined(&missing, "fail", &options, 64, 128)?;
        let mut observed_error = None;
        for index in 0_u16..64 {
            let key = index.to_be_bytes();
            let payload = [0x11_u8; 32];
            if let Err(err) = streaming_writer.put(&key, &payload) {
                observed_error = Some(err);
                break;
            }
        }
        let streaming_failure = if let Some(err) = observed_error {
            err
        } else {
            let Err(err) = streaming_writer.finish() else {
                return Err("expected missing directory to fail pipelined SST write".into());
            };
            err
        };
        assert!(matches!(
            streaming_failure,
            BulkLoadError::RocksDbOperation {
                operation: "ordered SST open",
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn pipelined_ordered_sst_writer_surfaces_digest_worker_root_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let options = Options::default();
        let pipeline =
            PipelinedOrderedSstState::spawn(dir.path(), "digest-err", &options, 1024, 4096)?;
        let build_batch = |key: &[u8], val: &[u8]| -> Result<Arc<QueuedBatch>, BulkLoadError> {
            let mut staging =
                StagingBatch::allocate(&pipeline.control, key.len() + val.len(), 1, false)?;
            staging.push(
                key,
                val,
                u32::try_from(key.len()).unwrap_or(0),
                u32::try_from(val.len()).unwrap_or(0),
            );
            Ok(Arc::new(staging.seal()))
        };
        let first = build_batch(b"b", b"1")?;
        if let Some(write_tx) = pipeline.write_tx.as_ref() {
            let _sent = write_tx.send(Arc::clone(&first));
        }
        if let Some(digest_tx) = pipeline.digest_tx.as_ref() {
            let _sent = digest_tx.send(first);
        }
        let write_valid = build_batch(b"c", b"2")?;
        let digest_invalid = build_batch(b"a", b"3")?;
        if let Some(write_tx) = pipeline.write_tx.as_ref() {
            let _sent = write_tx.send(write_valid);
        }
        if let Some(digest_tx) = pipeline.digest_tx.as_ref() {
            let _sent = digest_tx.send(digest_invalid);
        }
        let Err(failure) = pipeline.finish() else {
            return Err("expected digest worker failure to surface".into());
        };
        assert!(matches!(
            failure,
            BulkLoadError::InvalidInput { ref reason }
                if reason.contains("strictly increasing")
        ));
        Ok(())
    }

    #[test]
    fn pipeline_control_publishes_aborted_flag_after_recording_root_error() {
        use std::{
            sync::atomic::{AtomicBool, Ordering},
            time::Duration,
        };

        let fail_control = Arc::new(PipelineControl::new(1024));
        let entered_fail = Arc::new(AtomicBool::new(false));
        let fail_guard = fail_control.state.lock();
        thread::scope(|scope| {
            let control_for_thread = Arc::clone(&fail_control);
            let entered_for_thread = Arc::clone(&entered_fail);
            let handle = scope.spawn(move || {
                entered_for_thread.store(true, Ordering::Release);
                control_for_thread
                    .fail(BulkLoadError::invalid("simulated write worker root error"));
            });
            while !entered_fail.load(Ordering::Acquire) {
                thread::yield_now();
            }
            thread::sleep(Duration::from_millis(10));
            assert!(
                !fail_control.is_aborted(),
                "aborted_flag must not become visible before root error is recorded"
            );
            drop(fail_guard);
            assert!(handle.join().is_ok());
        });
        assert!(fail_control.is_aborted());
        assert!(matches!(
            fail_control.check_error(),
            Err(BulkLoadError::InvalidInput { ref reason })
                if reason == "simulated write worker root error"
        ));

        let unterminated_control = Arc::new(PipelineControl::new(1024));
        let entered_unterminated = Arc::new(AtomicBool::new(false));
        let unterminated_guard = unterminated_control.state.lock();
        thread::scope(|scope| {
            let control_for_thread = Arc::clone(&unterminated_control);
            let entered_for_thread = Arc::clone(&entered_unterminated);
            let handle = scope.spawn(move || {
                entered_for_thread.store(true, Ordering::Release);
                control_for_thread
                    .fail_if_unterminated("worker terminated unexpectedly without closing");
            });
            while !entered_unterminated.load(Ordering::Acquire) {
                thread::yield_now();
            }
            thread::sleep(Duration::from_millis(10));
            assert!(
                !unterminated_control.is_aborted(),
                "aborted_flag must not become visible before unterminated error is recorded"
            );
            drop(unterminated_guard);
            assert!(handle.join().is_ok());
        });
        assert!(unterminated_control.is_aborted());
        assert!(matches!(
            unterminated_control.check_error(),
            Err(BulkLoadError::InvalidInput { ref reason })
                if reason == "worker terminated unexpectedly without closing"
        ));
    }
}
