//! Bounded external sorting and ordered SST construction for `RocksDB`.

use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use rust_rocksdb::{Options, SstFileWriter};
use sha2::{Digest, Sha256};
use thiserror::Error;

mod variable_value_sort;

pub use variable_value_sort::{
    SortedVariableValues, VariableValueRecord, VariableValueSortEvidence, VariableValueSorter,
};

pub(super) const MAX_MERGE_FAN_IN: usize = 64;

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
        ///
        /// A refusal raised while records are still arriving reports the
        /// requirement of the records seen so far, so the complete sort needs
        /// at least this much and possibly more.
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
        if self
            .last_key
            .as_deref()
            .is_some_and(|previous| previous >= key)
        {
            return Err(BulkLoadError::invalid(
                "ordered logical-family keys are duplicated or not strictly increasing",
            ));
        }
        let row_logical_bytes = logical_row_bytes(key.len(), encoded_value.len())?;
        let row_count = self
            .row_count
            .checked_add(1)
            .ok_or_else(|| BulkLoadError::invalid("logical-family row count exceeds u64::MAX"))?;
        let logical_bytes = self
            .logical_bytes
            .checked_add(row_logical_bytes)
            .ok_or_else(|| BulkLoadError::invalid("logical-family bytes exceed u64::MAX"))?;
        let key_length = u64::try_from(key.len())
            .map_err(|_| BulkLoadError::invalid("SST key length exceeds u64::MAX"))?;
        let encoded_value_length = u64::try_from(encoded_value.len())
            .map_err(|_| BulkLoadError::invalid("SST value length exceeds u64::MAX"))?;
        Ok(OrderedKeyValueEvidenceUpdate {
            row_count,
            logical_bytes,
            row_logical_bytes,
            key_length,
            encoded_value_length,
        })
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
        self.last_key = Some(key.to_vec());
        update_ordered_key_value_digest(
            &mut self.digest,
            update.key_length,
            key,
            update.encoded_value_length,
            encoded_value,
        );
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

/// Writes strictly increasing opaque keys into bounded-size SST files.
pub struct OrderedSstWriter<'options> {
    staging_path: &'options Path,
    artifact_label: &'static str,
    options: &'options Options,
    target_logical_bytes: u64,
    writer: Option<SstFileWriter<'options>>,
    current_path: Option<PathBuf>,
    current_logical_bytes: u64,
    next_file_index: u64,
    paths: Vec<PathBuf>,
    file_bytes: u64,
    files: Vec<SstFileEvidence>,
    logical_family_evidence: OrderedKeyValueEvidenceAccumulator,
    current_entry_count: u64,
    current_first_key: Option<Vec<u8>>,
    current_last_key: Option<Vec<u8>>,
    current_ordinal: Option<u64>,
    current_digest: Option<Sha256>,
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
            staging_path,
            artifact_label,
            options,
            target_logical_bytes,
            writer: None,
            current_path: None,
            current_logical_bytes: 0,
            next_file_index: 0,
            paths: Vec::new(),
            file_bytes: 0,
            files: Vec::new(),
            logical_family_evidence: OrderedKeyValueEvidenceAccumulator::new(),
            current_entry_count: 0,
            current_first_key: None,
            current_last_key: None,
            current_ordinal: None,
            current_digest: None,
        })
    }

    /// Appends one key and value after verifying strict key order.
    pub fn put(&mut self, key: &[u8], encoded_value: &[u8]) -> Result<(), BulkLoadError> {
        let evidence_update = self
            .logical_family_evidence
            .prepare_update(key, encoded_value)?;
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
            .checked_add(evidence_update.row_logical_bytes)
            .ok_or_else(|| BulkLoadError::invalid("current SST logical bytes exceed u64::MAX"))?;
        self.current_entry_count = self
            .current_entry_count
            .checked_add(1)
            .ok_or_else(|| BulkLoadError::invalid("SST entry count exceeds u64::MAX"))?;
        if self.current_first_key.is_none() {
            self.current_first_key = Some(key.to_vec());
        }
        self.current_last_key = Some(key.to_vec());
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

    /// Finishes the last file and returns every produced SST path.
    pub fn finish(mut self) -> Result<SstFileSet, BulkLoadError> {
        self.finish_current()?;
        Ok(SstFileSet {
            paths: self.paths,
            file_bytes: self.file_bytes,
            files: self.files,
            logical_family_evidence: self.logical_family_evidence.finish(),
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
        let writer = SstFileWriter::create(self.options);
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
        let entry_count = self.current_entry_count;
        let first_key = self
            .current_first_key
            .take()
            .ok_or_else(|| BulkLoadError::invalid("closed ordered SST has no first key"))?;
        let last_key = self
            .current_last_key
            .take()
            .ok_or_else(|| BulkLoadError::invalid("closed ordered SST has no last key"))?;
        let digest = self
            .current_digest
            .take()
            .ok_or_else(|| BulkLoadError::invalid("closed ordered SST has no evidence digest"))?;
        self.files.push(SstFileEvidence {
            ordinal,
            file_bytes,
            entry_count,
            first_key,
            last_key,
            ordered_key_value_digest: digest.finalize().into(),
        });
        self.paths.push(path);
        Ok(())
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
        let Some(sorted_run) = merge_sort_runs::<RECORD_LEN, KEY_LEN>(
            &self.staging_path,
            self.artifact_label,
            self.runs,
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
) -> Result<Option<PathBuf>, BulkLoadError> {
    let mut pass = 0_u64;
    while runs.len() > 1 {
        let mut merged_runs = Vec::with_capacity(runs.len().div_ceil(MAX_MERGE_FAN_IN));
        for (group_index, group) in runs.chunks(MAX_MERGE_FAN_IN).enumerate() {
            let path = staging_path.join(format!(
                "{artifact_label}-merge-{pass:04}-{group_index:08}.bin"
            ));
            merge_run_group::<RECORD_LEN, KEY_LEN>(group, &path, artifact_label)?;
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
) -> Result<(), BulkLoadError> {
    if inputs.len() > MAX_MERGE_FAN_IN {
        return Err(BulkLoadError::invalid(
            "sort merge exceeded its fixed file-descriptor fan-in",
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
            assert!(files.paths.is_empty());
            assert!(files.files.is_empty());
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
}
