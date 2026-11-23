//! Bounded external sorting for fixed-width keys with variable encoded values.

use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    mem::size_of,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tempfile::{Builder as TemporaryDirectoryBuilder, TempDir};

use super::{BulkLoadError, MAX_MERGE_FAN_IN, path_unavailable};

const VALUE_LENGTH_BYTES: u64 = 8;

/// One sorted fixed-width key and its opaque encoded value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VariableValueRecord<const KEY_LEN: usize> {
    /// Opaque fixed-width key in nondecreasing byte order.
    pub key: [u8; KEY_LEN],
    /// Opaque value bytes associated with `key`.
    pub encoded_value: Vec<u8>,
}

/// Bounded-work evidence from one completed variable-value sort.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VariableValueSortEvidence {
    /// Records accepted by the sorter.
    pub record_count: u64,
    /// Initial sorted runs written before merge passes.
    pub initial_run_count: u64,
    /// Merge passes that wrote merged runs before the reader's streamed merge.
    pub merge_pass_count: u64,
    /// Highest accounted in-memory key, index, and value byte count.
    pub peak_accounted_memory_bytes: u64,
    /// Caller-supplied ceiling for accounted in-memory record bytes.
    pub max_accounted_memory_bytes: u64,
    /// Highest simultaneously retained temporary-run byte count.
    pub peak_temporary_file_bytes: u64,
    /// Caller-supplied ceiling for temporary-run bytes.
    pub max_temporary_file_bytes: u64,
    /// Bytes of the runs left for the reader, or zero for empty input.
    pub final_run_file_bytes: u64,
}

/// Wall time one variable-value sort spent sorting, writing, and merging runs.
///
/// The parts do not overlap. `initial_run_spill` passes inside
/// [`VariableValueSorter::push`] while records are still arriving, so it is
/// part of the caller's time spent pushing them. The other two pass inside
/// [`VariableValueSorter::finish`]. Reading the final run back is the
/// consumer's time and is not included.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VariableValueSortDurations {
    /// Sorting and writing the runs that `push` flushed at the memory ceiling.
    pub initial_run_spill: Duration,
    /// Sorting and writing the records still staged when `finish` began.
    pub final_run_flush: Duration,
    /// Merge passes that wrote merged runs; zero for at most 64 runs, which
    /// the reader merges as it is read.
    pub merge: Duration,
}

#[derive(Debug)]
struct StagedVariableValue<const KEY_LEN: usize> {
    key: [u8; KEY_LEN],
    value_start: usize,
    value_end: usize,
}

#[derive(Debug)]
struct RunFile {
    path: PathBuf,
    file_bytes: u64,
}

impl Drop for RunFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[derive(Debug)]
struct TemporaryFileBudget {
    byte_limit: u64,
    bytes_in_use: u64,
    peak_bytes_in_use: u64,
}

impl TemporaryFileBudget {
    const fn new(limit: u64) -> Self {
        Self {
            byte_limit: limit,
            bytes_in_use: 0,
            peak_bytes_in_use: 0,
        }
    }

    fn reserve(&mut self, bytes: u64) -> Result<(), BulkLoadError> {
        let required_bytes = self
            .bytes_in_use
            .checked_add(bytes)
            .ok_or_else(|| BulkLoadError::invalid("temporary file bytes exceed u64::MAX"))?;
        if required_bytes > self.byte_limit {
            return Err(BulkLoadError::TemporaryFileLimit {
                limit_bytes: self.byte_limit,
                required_bytes,
            });
        }
        self.bytes_in_use = required_bytes;
        self.peak_bytes_in_use = self.peak_bytes_in_use.max(required_bytes);
        Ok(())
    }

    /// Reserves one more initial run of a sort that already holds a run.
    ///
    /// A sort that ends with more than `MAX_MERGE_FAN_IN` runs merges them in
    /// passes that write every record again while the input runs still exist,
    /// so admission keeps room for twice the total run bytes. A sort that ends
    /// with fewer runs streams its final merge and stays under that
    /// reservation. Refusing here, before the run is written, stops a sort that
    /// cannot finish while records are still arriving instead of after the
    /// whole input has been spilled.
    fn reserve_merged_run(&mut self, bytes: u64) -> Result<(), BulkLoadError> {
        let required_bytes = self
            .bytes_in_use
            .checked_add(bytes)
            .and_then(|run_bytes| run_bytes.checked_mul(2))
            .ok_or_else(|| BulkLoadError::invalid("temporary file bytes exceed u64::MAX"))?;
        if required_bytes > self.byte_limit {
            return Err(BulkLoadError::TemporaryFileLimit {
                limit_bytes: self.byte_limit,
                required_bytes,
            });
        }
        self.reserve(bytes)
    }

    fn release(&mut self, bytes: u64) -> Result<(), BulkLoadError> {
        self.bytes_in_use = self
            .bytes_in_use
            .checked_sub(bytes)
            .ok_or_else(|| BulkLoadError::invalid("temporary file accounting underflow"))?;
        Ok(())
    }
}

/// Externally sorts fixed-width keys with variable opaque values.
///
/// The in-memory ceiling accounts for staged keys, indexes, and value bytes.
/// Allocator metadata and the fixed per-run buffered-I/O overhead are not part
/// of that caller-selected ceiling; merge fan-in caps the latter independently.
/// Temporary-file admission accounts for both input and output runs while a
/// merge pass is in progress, and refuses a new run as soon as a merge pass
/// could no longer fit under the ceiling. Duplicate keys remain adjacent for the domain caller
/// to accept or reject.
pub struct VariableValueSorter<const KEY_LEN: usize> {
    workspace: TempDir,
    max_accounted_memory_bytes: u64,
    staged_records: Vec<StagedVariableValue<KEY_LEN>>,
    encoded_values: Vec<u8>,
    runs: Vec<RunFile>,
    next_run_index: u64,
    record_count: u64,
    initial_run_count: u64,
    merge_pass_count: u64,
    peak_accounted_memory_bytes: u64,
    temporary_files: TemporaryFileBudget,
    durations: VariableValueSortDurations,
}

impl<const KEY_LEN: usize> VariableValueSorter<KEY_LEN> {
    /// Creates a sorter with explicit record-memory and temporary-file ceilings.
    pub fn new(
        staging_path: &Path,
        artifact_label: &'static str,
        max_accounted_memory_bytes: u64,
        max_temporary_file_bytes: u64,
    ) -> Result<Self, BulkLoadError> {
        if KEY_LEN == 0 {
            return Err(BulkLoadError::invalid(
                "variable-value keys must contain at least one byte",
            ));
        }
        validate_artifact_label(artifact_label)?;
        let minimum_record_bytes = u64::try_from(size_of::<StagedVariableValue<KEY_LEN>>())
            .map_err(|_| BulkLoadError::invalid("record metadata bytes exceed u64::MAX"))?;
        if max_accounted_memory_bytes < minimum_record_bytes {
            return Err(BulkLoadError::AccountedMemoryLimit {
                limit_bytes: max_accounted_memory_bytes,
                required_bytes: minimum_record_bytes,
            });
        }
        if max_temporary_file_bytes == 0 {
            return Err(BulkLoadError::TemporaryFileLimit {
                limit_bytes: 0,
                required_bytes: 1,
            });
        }
        let workspace_prefix = format!("{artifact_label}-variable-sort-");
        let workspace = TemporaryDirectoryBuilder::new()
            .prefix(&workspace_prefix)
            .tempdir_in(staging_path)
            .map_err(|source| path_unavailable(staging_path, source))?;
        Ok(Self {
            workspace,
            max_accounted_memory_bytes,
            staged_records: Vec::new(),
            encoded_values: Vec::new(),
            runs: Vec::new(),
            next_run_index: 0,
            record_count: 0,
            initial_run_count: 0,
            merge_pass_count: 0,
            peak_accounted_memory_bytes: 0,
            temporary_files: TemporaryFileBudget::new(max_temporary_file_bytes),
            durations: VariableValueSortDurations::default(),
        })
    }

    /// Stages one record, flushing the current run before crossing memory admission.
    pub fn push(&mut self, key: [u8; KEY_LEN], encoded_value: &[u8]) -> Result<(), BulkLoadError> {
        let record_bytes = accounted_record_bytes::<KEY_LEN>(encoded_value.len())?;
        if record_bytes > self.max_accounted_memory_bytes {
            return Err(BulkLoadError::AccountedMemoryLimit {
                limit_bytes: self.max_accounted_memory_bytes,
                required_bytes: record_bytes,
            });
        }
        let current_bytes = self.current_accounted_memory_bytes()?;
        let required_bytes = current_bytes
            .checked_add(record_bytes)
            .ok_or_else(|| BulkLoadError::invalid("accounted record bytes exceed u64::MAX"))?;
        if required_bytes > self.max_accounted_memory_bytes {
            let spill_started = Instant::now();
            self.flush_run()
                .map_err(BulkLoadError::temporary_file_limit_before_finish)?;
            self.durations.initial_run_spill = self
                .durations
                .initial_run_spill
                .saturating_add(spill_started.elapsed());
        }

        self.staged_records.try_reserve_exact(1).map_err(|source| {
            BulkLoadError::MemoryAllocation {
                operation: "variable-value record index",
                source,
            }
        })?;
        self.encoded_values
            .try_reserve_exact(encoded_value.len())
            .map_err(|source| BulkLoadError::MemoryAllocation {
                operation: "variable-value record bytes",
                source,
            })?;
        let value_start = self.encoded_values.len();
        self.encoded_values.extend_from_slice(encoded_value);
        let value_end = self.encoded_values.len();
        self.staged_records.push(StagedVariableValue {
            key,
            value_start,
            value_end,
        });
        self.record_count = self.record_count.checked_add(1).ok_or_else(|| {
            BulkLoadError::invalid("variable-value record count exceeds u64::MAX")
        })?;
        let current_bytes = self.current_accounted_memory_bytes()?;
        self.peak_accounted_memory_bytes = self.peak_accounted_memory_bytes.max(current_bytes);
        Ok(())
    }

    /// Finishes all runs and opens a bounded streaming reader over sorted records.
    ///
    /// Merge passes write merged runs only while more than `MAX_MERGE_FAN_IN`
    /// runs remain. The reader merges the remaining runs as it is read, so the
    /// last merge never writes a run and reads it back.
    pub fn finish(mut self) -> Result<SortedVariableValues<KEY_LEN>, BulkLoadError> {
        let flush_started = Instant::now();
        self.flush_run()?;
        self.durations.final_run_flush = flush_started.elapsed();
        let mut runs = std::mem::take(&mut self.runs);
        if runs.len() > MAX_MERGE_FAN_IN {
            let merge_started = Instant::now();
            while runs.len() > MAX_MERGE_FAN_IN {
                runs = self.merge_pass(runs)?;
                self.merge_pass_count = self
                    .merge_pass_count
                    .checked_add(1)
                    .ok_or_else(|| BulkLoadError::invalid("merge pass count exceeds u64::MAX"))?;
            }
            self.durations.merge = merge_started.elapsed();
        }
        let final_run_file_bytes = runs.iter().try_fold(0_u64, |total, run| {
            total
                .checked_add(run.file_bytes)
                .ok_or_else(|| BulkLoadError::invalid("final run bytes exceed u64::MAX"))
        })?;
        let source = SortedRunSource::open(runs)?;
        let evidence = VariableValueSortEvidence {
            record_count: self.record_count,
            initial_run_count: self.initial_run_count,
            merge_pass_count: self.merge_pass_count,
            peak_accounted_memory_bytes: self.peak_accounted_memory_bytes,
            max_accounted_memory_bytes: self.max_accounted_memory_bytes,
            peak_temporary_file_bytes: self.temporary_files.peak_bytes_in_use,
            max_temporary_file_bytes: self.temporary_files.byte_limit,
            final_run_file_bytes,
        };
        Ok(SortedVariableValues {
            source,
            evidence,
            durations: self.durations,
            max_accounted_memory_bytes: self.max_accounted_memory_bytes,
            previous_key: None,
            emitted_record_count: 0,
            workspace: Some(self.workspace),
            finished: false,
        })
    }

    fn current_accounted_memory_bytes(&self) -> Result<u64, BulkLoadError> {
        let index_bytes = self
            .staged_records
            .len()
            .checked_mul(size_of::<StagedVariableValue<KEY_LEN>>())
            .ok_or_else(|| BulkLoadError::invalid("record index bytes exceed usize::MAX"))?;
        let total_bytes = index_bytes
            .checked_add(self.encoded_values.len())
            .ok_or_else(|| BulkLoadError::invalid("record bytes exceed usize::MAX"))?;
        u64::try_from(total_bytes)
            .map_err(|_| BulkLoadError::invalid("record bytes exceed u64::MAX"))
    }

    fn flush_run(&mut self) -> Result<(), BulkLoadError> {
        if self.staged_records.is_empty() {
            return Ok(());
        }
        self.staged_records
            .sort_unstable_by_key(|record| record.key);
        let run_file_bytes = self
            .staged_records
            .iter()
            .try_fold(0_u64, |total, record| {
                let value_bytes = record
                    .value_end
                    .checked_sub(record.value_start)
                    .ok_or_else(|| BulkLoadError::invalid("staged value range is reversed"))?;
                total
                    .checked_add(encoded_run_record_bytes::<KEY_LEN>(value_bytes)?)
                    .ok_or_else(|| BulkLoadError::invalid("run file bytes exceed u64::MAX"))
            })?;
        if self.runs.is_empty() {
            self.temporary_files.reserve(run_file_bytes)?;
        } else {
            self.temporary_files.reserve_merged_run(run_file_bytes)?;
        }
        let path = self
            .workspace
            .path()
            .join(format!("run-{:08}.bin", self.next_run_index));
        self.next_run_index = self
            .next_run_index
            .checked_add(1)
            .ok_or_else(|| BulkLoadError::invalid("variable run count exceeds u64::MAX"))?;
        let write_result = self.write_staged_run(&path);
        if let Err(source) = write_result {
            discard_reserved_file(&path, &mut self.temporary_files, run_file_bytes)?;
            return Err(source);
        }
        let observed_file_bytes = match fs::metadata(&path) {
            Ok(metadata) => metadata.len(),
            Err(source) => {
                let error = path_unavailable(&path, source);
                discard_reserved_file(&path, &mut self.temporary_files, run_file_bytes)?;
                return Err(error);
            }
        };
        if observed_file_bytes != run_file_bytes {
            discard_reserved_file(&path, &mut self.temporary_files, run_file_bytes)?;
            return Err(BulkLoadError::invalid(
                "variable run file size differs from its admitted bytes",
            ));
        }
        self.runs.push(RunFile {
            path,
            file_bytes: run_file_bytes,
        });
        self.initial_run_count = self
            .initial_run_count
            .checked_add(1)
            .ok_or_else(|| BulkLoadError::invalid("initial run count exceeds u64::MAX"))?;
        self.staged_records = Vec::new();
        self.encoded_values = Vec::new();
        Ok(())
    }

    fn write_staged_run(&self, path: &Path) -> Result<(), BulkLoadError> {
        let file = File::create(path).map_err(|source| path_unavailable(path, source))?;
        let mut writer = BufWriter::new(file);
        for record in &self.staged_records {
            let encoded_value = self
                .encoded_values
                .get(record.value_start..record.value_end)
                .ok_or_else(|| BulkLoadError::invalid("staged value range is unavailable"))?;
            writer
                .write_all(&record.key)
                .and_then(|()| {
                    let value_len = u64::try_from(encoded_value.len()).map_err(|_| {
                        std::io::Error::other("encoded value length exceeds u64::MAX")
                    })?;
                    writer.write_all(&value_len.to_be_bytes())
                })
                .and_then(|()| writer.write_all(encoded_value))
                .map_err(|source| path_unavailable(path, source))?;
        }
        writer
            .flush()
            .map_err(|source| path_unavailable(path, source))
    }

    fn merge_pass(&mut self, runs: Vec<RunFile>) -> Result<Vec<RunFile>, BulkLoadError> {
        let mut pending = runs.into_iter();
        let mut merged = Vec::new();
        let mut group_index = 0_u64;
        loop {
            let group = pending.by_ref().take(MAX_MERGE_FAN_IN).collect::<Vec<_>>();
            if group.is_empty() {
                break;
            }
            if group.len() == 1 {
                merged.extend(group);
            } else {
                let output_path = self.workspace.path().join(format!(
                    "merge-{:04}-{:08}.bin",
                    self.merge_pass_count, group_index
                ));
                merged.push(self.merge_group(group, output_path)?);
            }
            group_index = group_index
                .checked_add(1)
                .ok_or_else(|| BulkLoadError::invalid("merge group count exceeds u64::MAX"))?;
        }
        Ok(merged)
    }

    fn merge_group(
        &mut self,
        input_runs: Vec<RunFile>,
        output_path: PathBuf,
    ) -> Result<RunFile, BulkLoadError> {
        let input_file_bytes = input_runs.iter().try_fold(0_u64, |total, run| {
            total
                .checked_add(run.file_bytes)
                .ok_or_else(|| BulkLoadError::invalid("merged run bytes exceed u64::MAX"))
        })?;
        let output_file_bytes = input_file_bytes;
        self.temporary_files.reserve(output_file_bytes)?;
        let merge_result = merge_run_group::<KEY_LEN>(&input_runs, &output_path);
        if let Err(source) = merge_result {
            discard_reserved_file(&output_path, &mut self.temporary_files, output_file_bytes)?;
            return Err(source);
        }
        let observed_file_bytes = match fs::metadata(&output_path) {
            Ok(metadata) => metadata.len(),
            Err(source) => {
                let error = path_unavailable(&output_path, source);
                discard_reserved_file(&output_path, &mut self.temporary_files, output_file_bytes)?;
                return Err(error);
            }
        };
        if observed_file_bytes != output_file_bytes {
            discard_reserved_file(&output_path, &mut self.temporary_files, output_file_bytes)?;
            return Err(BulkLoadError::invalid(
                "merged run file size differs from its admitted bytes",
            ));
        }
        for input_run in &input_runs {
            remove_temporary_file(&input_run.path)?;
        }
        drop(input_runs);
        self.temporary_files.release(input_file_bytes)?;
        Ok(RunFile {
            path: output_path,
            file_bytes: output_file_bytes,
        })
    }
}

/// Streaming reader for a completed variable-value sort.
///
/// Dropping this reader removes its final temporary runs. Records with equal
/// keys are adjacent, but their relative value order is intentionally not a
/// contract.
pub struct SortedVariableValues<const KEY_LEN: usize> {
    source: SortedRunSource<KEY_LEN>,
    evidence: VariableValueSortEvidence,
    durations: VariableValueSortDurations,
    max_accounted_memory_bytes: u64,
    previous_key: Option<[u8; KEY_LEN]>,
    emitted_record_count: u64,
    workspace: Option<TempDir>,
    finished: bool,
}

impl<const KEY_LEN: usize> SortedVariableValues<KEY_LEN> {
    /// Returns the bounded-work evidence fixed when sorting completed.
    #[must_use]
    pub const fn evidence(&self) -> VariableValueSortEvidence {
        self.evidence
    }

    /// Returns the run spill, final flush, and merge time fixed when sorting completed.
    #[must_use]
    pub const fn durations(&self) -> VariableValueSortDurations {
        self.durations
    }

    /// Reads the next key and value without retaining earlier records.
    pub fn next_record(&mut self) -> Result<Option<VariableValueRecord<KEY_LEN>>, BulkLoadError> {
        if self.finished {
            return Ok(None);
        }
        let Some((head, run_index)) = self.source.next_head()? else {
            if self.emitted_record_count != self.evidence.record_count {
                return Err(BulkLoadError::invalid(
                    "completed variable run ended before its admitted record count",
                ));
            }
            self.finished = true;
            self.source = SortedRunSource::Drained;
            self.workspace = None;
            return Ok(None);
        };
        if self.emitted_record_count >= self.evidence.record_count {
            return Err(BulkLoadError::invalid(
                "completed variable run exceeds its admitted record count",
            ));
        }
        if self
            .previous_key
            .is_some_and(|previous_key| previous_key > head.key)
        {
            return Err(BulkLoadError::invalid(
                "completed variable run keys are not nondecreasing",
            ));
        }
        let value_len = usize::try_from(head.value_len)
            .map_err(|_| BulkLoadError::invalid("encoded value length exceeds usize::MAX"))?;
        let required_bytes = accounted_record_bytes::<KEY_LEN>(value_len)?;
        if required_bytes > self.max_accounted_memory_bytes {
            return Err(BulkLoadError::AccountedMemoryLimit {
                limit_bytes: self.max_accounted_memory_bytes,
                required_bytes,
            });
        }
        let mut encoded_value = Vec::new();
        encoded_value
            .try_reserve_exact(value_len)
            .map_err(|source| BulkLoadError::MemoryAllocation {
                operation: "sorted variable value",
                source,
            })?;
        encoded_value.resize(value_len, 0);
        self.source.read_value(run_index, &mut encoded_value)?;
        self.previous_key = Some(head.key);
        self.emitted_record_count = self
            .emitted_record_count
            .checked_add(1)
            .ok_or_else(|| BulkLoadError::invalid("emitted record count exceeds u64::MAX"))?;
        Ok(Some(VariableValueRecord {
            key: head.key,
            encoded_value,
        }))
    }

    #[cfg(test)]
    fn run_paths(&self) -> Vec<PathBuf> {
        match &self.source {
            SortedRunSource::Drained => Vec::new(),
            SortedRunSource::Single { run, .. } => vec![run.path.clone()],
            SortedRunSource::Merging { runs, .. } => {
                runs.iter().map(|run| run.path.clone()).collect()
            }
        }
    }
}

/// Runs a completed sort hands to its reader.
enum SortedRunSource<const KEY_LEN: usize> {
    /// No run remains, because the input was empty or every record was read.
    Drained,
    /// One run, read front to back.
    Single {
        run: RunFile,
        reader: BufReader<File>,
    },
    /// At most `MAX_MERGE_FAN_IN` runs, merged by smallest key as they are read.
    Merging {
        runs: Vec<RunFile>,
        readers: Vec<BufReader<File>>,
        heads: BinaryHeap<Reverse<([u8; KEY_LEN], usize, u64)>>,
        pending_run: Option<usize>,
    },
}

impl<const KEY_LEN: usize> SortedRunSource<KEY_LEN> {
    fn open(mut runs: Vec<RunFile>) -> Result<Self, BulkLoadError> {
        if runs.len() > MAX_MERGE_FAN_IN {
            return Err(BulkLoadError::invalid(
                "variable merge exceeded its fixed file-descriptor fan-in",
            ));
        }
        if runs.len() <= 1 {
            return Ok(match runs.pop() {
                None => Self::Drained,
                Some(run) => {
                    let reader = File::open(&run.path)
                        .map(BufReader::new)
                        .map_err(|source| path_unavailable(&run.path, source))?;
                    Self::Single { run, reader }
                }
            });
        }
        let mut readers = Vec::with_capacity(runs.len());
        let mut heads = BinaryHeap::new();
        for (run_index, run) in runs.iter().enumerate() {
            let file =
                File::open(&run.path).map_err(|source| path_unavailable(&run.path, source))?;
            let mut reader = BufReader::new(file);
            if let Some(head) = read_run_head::<KEY_LEN>(&mut reader, &run.path)? {
                heads.push(Reverse((head.key, run_index, head.value_len)));
            }
            readers.push(reader);
        }
        Ok(Self::Merging {
            runs,
            readers,
            heads,
            pending_run: None,
        })
    }

    /// Returns the smallest remaining record head and the run its value follows in.
    fn next_head(&mut self) -> Result<Option<(RunHead<KEY_LEN>, usize)>, BulkLoadError> {
        match self {
            Self::Drained => Ok(None),
            Self::Single { run, reader } => {
                Ok(read_run_head::<KEY_LEN>(reader, &run.path)?.map(|head| (head, 0)))
            }
            Self::Merging {
                runs,
                readers,
                heads,
                pending_run,
            } => {
                if let Some(run_index) = pending_run.take() {
                    let (Some(reader), Some(run)) =
                        (readers.get_mut(run_index), runs.get(run_index))
                    else {
                        return Err(BulkLoadError::invalid("merged run index is unavailable"));
                    };
                    if let Some(head) = read_run_head::<KEY_LEN>(reader, &run.path)? {
                        heads.push(Reverse((head.key, run_index, head.value_len)));
                    }
                }
                Ok(heads.pop().map(|Reverse((key, run_index, value_len))| {
                    (RunHead { key, value_len }, run_index)
                }))
            }
        }
    }

    /// Reads the value that follows the head `next_head` just returned.
    fn read_value(
        &mut self,
        run_index: usize,
        encoded_value: &mut [u8],
    ) -> Result<(), BulkLoadError> {
        let (reader, path) = match self {
            Self::Drained => {
                return Err(BulkLoadError::invalid("drained sort has no value to read"));
            }
            Self::Single { run, reader } => (reader, run.path.as_path()),
            Self::Merging {
                runs,
                readers,
                pending_run,
                ..
            } => {
                *pending_run = Some(run_index);
                let (Some(reader), Some(run)) = (readers.get_mut(run_index), runs.get(run_index))
                else {
                    return Err(BulkLoadError::invalid("merged run index is unavailable"));
                };
                (reader, run.path.as_path())
            }
        };
        reader
            .read_exact(encoded_value)
            .map_err(|source| path_unavailable(path, source))
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RunHead<const KEY_LEN: usize> {
    key: [u8; KEY_LEN],
    value_len: u64,
}

fn merge_run_group<const KEY_LEN: usize>(
    input_runs: &[RunFile],
    output_path: &Path,
) -> Result<(), BulkLoadError> {
    if input_runs.len() > MAX_MERGE_FAN_IN {
        return Err(BulkLoadError::invalid(
            "variable merge exceeded its fixed file-descriptor fan-in",
        ));
    }
    let mut readers = Vec::with_capacity(input_runs.len());
    let mut heads = BinaryHeap::new();
    for (reader_index, run) in input_runs.iter().enumerate() {
        let file = File::open(&run.path).map_err(|source| path_unavailable(&run.path, source))?;
        let mut reader = BufReader::new(file);
        if let Some(head) = read_run_head::<KEY_LEN>(&mut reader, &run.path)? {
            heads.push(Reverse((head.key, reader_index, head.value_len)));
        }
        readers.push(reader);
    }
    let output =
        File::create(output_path).map_err(|source| path_unavailable(output_path, source))?;
    let mut writer = BufWriter::new(output);
    while let Some(Reverse((key, reader_index, value_len))) = heads.pop() {
        writer
            .write_all(&key)
            .and_then(|()| writer.write_all(&value_len.to_be_bytes()))
            .map_err(|source| path_unavailable(output_path, source))?;
        copy_exact_value(
            &mut readers[reader_index],
            &mut writer,
            value_len,
            &input_runs[reader_index].path,
        )?;
        if let Some(next) =
            read_run_head::<KEY_LEN>(&mut readers[reader_index], &input_runs[reader_index].path)?
        {
            heads.push(Reverse((next.key, reader_index, next.value_len)));
        }
    }
    writer
        .flush()
        .map_err(|source| path_unavailable(output_path, source))
}

fn read_run_head<const KEY_LEN: usize>(
    reader: &mut BufReader<File>,
    path: &Path,
) -> Result<Option<RunHead<KEY_LEN>>, BulkLoadError> {
    if KEY_LEN == 0 {
        return Err(BulkLoadError::invalid(
            "variable run key length must be greater than zero",
        ));
    }
    let mut key = [0_u8; KEY_LEN];
    let first_byte_count = reader
        .read(&mut key[..1])
        .map_err(|source| path_unavailable(path, source))?;
    if first_byte_count == 0 {
        return Ok(None);
    }
    reader
        .read_exact(&mut key[1..])
        .map_err(|source| path_unavailable(path, source))?;
    let mut encoded_value_len = [0_u8; 8];
    reader
        .read_exact(&mut encoded_value_len)
        .map_err(|source| path_unavailable(path, source))?;
    Ok(Some(RunHead {
        key,
        value_len: u64::from_be_bytes(encoded_value_len),
    }))
}

fn copy_exact_value(
    reader: &mut BufReader<File>,
    writer: &mut BufWriter<File>,
    value_len: u64,
    input_path: &Path,
) -> Result<(), BulkLoadError> {
    let copied = std::io::copy(&mut reader.take(value_len), writer)
        .map_err(|source| path_unavailable(input_path, source))?;
    if copied != value_len {
        return Err(path_unavailable(
            input_path,
            std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "variable run value is truncated",
            ),
        ));
    }
    Ok(())
}

fn accounted_record_bytes<const KEY_LEN: usize>(value_len: usize) -> Result<u64, BulkLoadError> {
    let total = size_of::<StagedVariableValue<KEY_LEN>>()
        .checked_add(value_len)
        .ok_or_else(|| BulkLoadError::invalid("accounted record bytes exceed usize::MAX"))?;
    u64::try_from(total).map_err(|_| BulkLoadError::invalid("record bytes exceed u64::MAX"))
}

fn validate_artifact_label(artifact_label: &str) -> Result<(), BulkLoadError> {
    if artifact_label.is_empty()
        || !artifact_label.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
    {
        return Err(BulkLoadError::invalid(
            "variable-sort artifact label must contain only lowercase ASCII letters, digits, '-' or '_'",
        ));
    }
    Ok(())
}

fn discard_reserved_file(
    path: &Path,
    temporary_files: &mut TemporaryFileBudget,
    reserved_bytes: u64,
) -> Result<(), BulkLoadError> {
    remove_temporary_file(path)?;
    temporary_files.release(reserved_bytes)
}

fn remove_temporary_file(path: &Path) -> Result<(), BulkLoadError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(path_unavailable(path, source)),
    }
}

fn encoded_run_record_bytes<const KEY_LEN: usize>(value_len: usize) -> Result<u64, BulkLoadError> {
    let key_bytes = u64::try_from(KEY_LEN)
        .map_err(|_| BulkLoadError::invalid("variable key bytes exceed u64::MAX"))?;
    let value_bytes = u64::try_from(value_len)
        .map_err(|_| BulkLoadError::invalid("variable value bytes exceed u64::MAX"))?;
    key_bytes
        .checked_add(VALUE_LENGTH_BYTES)
        .and_then(|bytes| bytes.checked_add(value_bytes))
        .ok_or_else(|| BulkLoadError::invalid("encoded run record bytes exceed u64::MAX"))
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom};

    use super::*;
    use tempfile::TempDir;

    fn one_record_memory_limit<const KEY_LEN: usize>(value_len: usize) -> u64 {
        u64::try_from(size_of::<StagedVariableValue<KEY_LEN>>().saturating_add(value_len))
            .unwrap_or(u64::MAX)
    }

    #[test]
    fn many_tiny_runs_stream_in_key_order_and_clean_up() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let memory_limit = one_record_memory_limit::<2>(1);
        let mut staged_sort =
            VariableValueSorter::<2>::new(temporary.path(), "ordered", memory_limit, 1_024)?;
        for key in [[3, 0], [1, 0], [4, 0], [2, 0]] {
            staged_sort.push(key, &[key[0]])?;
        }
        let mut sorted_records = staged_sort.finish()?;
        let final_paths = sorted_records.run_paths();
        assert_eq!(final_paths.len(), 4);
        let mut observed = Vec::new();
        while let Some(record) = sorted_records.next_record()? {
            observed.push((record.key, record.encoded_value));
        }
        assert_eq!(
            observed,
            vec![
                ([1, 0], vec![1]),
                ([2, 0], vec![2]),
                ([3, 0], vec![3]),
                ([4, 0], vec![4]),
            ]
        );
        assert_eq!(sorted_records.evidence().initial_run_count, 4);
        assert_eq!(sorted_records.evidence().record_count, 4);
        assert_eq!(sorted_records.evidence().merge_pass_count, 0);
        assert!(final_paths.iter().all(|path| !path.exists()));
        Ok(())
    }

    #[test]
    fn durations_separate_spills_from_the_final_flush() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let memory_limit = one_record_memory_limit::<2>(1);
        let mut staged_sort =
            VariableValueSorter::<2>::new(temporary.path(), "timed", memory_limit, 1_024)?;
        staged_sort.push([1, 0], &[1])?;
        assert_eq!(staged_sort.durations, VariableValueSortDurations::default());
        staged_sort.push([3, 0], &[3])?;
        staged_sort.push([2, 0], &[2])?;
        let sorted_records = staged_sort.finish()?;
        let durations = sorted_records.durations();

        assert_eq!(sorted_records.evidence().initial_run_count, 3);
        assert!(durations.initial_run_spill > Duration::ZERO);
        assert!(durations.final_run_flush > Duration::ZERO);
        assert_eq!(durations.merge, Duration::ZERO);
        Ok(())
    }

    #[test]
    fn one_run_sort_records_no_spill_or_merge_time() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let mut staged_sort =
            VariableValueSorter::<1>::new(temporary.path(), "one-run", 1_024, 1_024)?;
        staged_sort.push([2], b"b")?;
        staged_sort.push([1], b"a")?;
        let sorted_records = staged_sort.finish()?;
        let durations = sorted_records.durations();

        assert_eq!(sorted_records.evidence().initial_run_count, 1);
        assert_eq!(durations.initial_run_spill, Duration::ZERO);
        assert_eq!(durations.merge, Duration::ZERO);
        assert!(durations.final_run_flush > Duration::ZERO);
        Ok(())
    }

    #[test]
    fn duplicate_keys_remain_adjacent_for_domain_validation()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let mut staged_sort = VariableValueSorter::<1>::new(
            temporary.path(),
            "duplicates",
            one_record_memory_limit::<1>(1),
            1_024,
        )?;
        staged_sort.push([2], b"x")?;
        staged_sort.push([1], b"a")?;
        staged_sort.push([2], b"y")?;
        let mut sorted_records = staged_sort.finish()?;
        let mut keys = Vec::new();
        while let Some(record) = sorted_records.next_record()? {
            keys.push(record.key);
        }
        assert_eq!(keys, vec![[1], [2], [2]]);
        Ok(())
    }

    #[test]
    fn more_than_one_merge_group_finishes_in_global_order() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = TempDir::new()?;
        let memory_limit = one_record_memory_limit::<2>(1);
        let mut staged_sort = VariableValueSorter::<2>::new(
            temporary.path(),
            "multiple-groups",
            memory_limit,
            10_000,
        )?;
        for number in (0_u16..65).rev() {
            staged_sort.push(number.to_be_bytes(), &[number.to_be_bytes()[1]])?;
        }
        let mut sorted_records = staged_sort.finish()?;
        let evidence = sorted_records.evidence();
        let mut observed_keys = Vec::new();
        while let Some(record) = sorted_records.next_record()? {
            observed_keys.push(record.key);
        }
        let expected_keys = (0_u16..65).map(u16::to_be_bytes).collect::<Vec<_>>();

        assert_eq!(observed_keys, expected_keys);
        assert_eq!(evidence.initial_run_count, 65);
        // One pass merges 64 runs into one; the reader streams the last two.
        assert_eq!(evidence.merge_pass_count, 1);
        assert!(sorted_records.durations().merge > Duration::ZERO);
        let one_run_bytes = encoded_run_record_bytes::<2>(1)?;
        assert_eq!(evidence.final_run_file_bytes, one_run_bytes * 65);
        assert_eq!(evidence.peak_temporary_file_bytes, one_run_bytes * 129);
        Ok(())
    }

    #[test]
    fn oversized_record_is_rejected_before_a_run_exists() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = TempDir::new()?;
        let memory_limit = one_record_memory_limit::<1>(1);
        let mut staged_sort =
            VariableValueSorter::<1>::new(temporary.path(), "oversized", memory_limit, 1_024)?;
        assert!(matches!(
            staged_sort.push([1], b"ab"),
            Err(BulkLoadError::AccountedMemoryLimit { .. })
        ));
        assert!(staged_sort.runs.is_empty());
        Ok(())
    }

    #[test]
    fn artifact_labels_cannot_escape_or_collide_in_staging()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        assert!(matches!(
            VariableValueSorter::<1>::new(temporary.path(), "../escape", 32, 1_024),
            Err(BulkLoadError::InvalidInput { .. })
        ));

        let first = VariableValueSorter::<1>::new(temporary.path(), "same", 32, 1_024)?;
        let second = VariableValueSorter::<1>::new(temporary.path(), "same", 32, 1_024)?;
        assert_ne!(first.workspace.path(), second.workspace.path());
        drop(first);
        drop(second);
        assert_eq!(fs::read_dir(temporary.path())?.count(), 0);
        Ok(())
    }

    #[test]
    fn merge_refuses_before_crossing_temporary_file_limit() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = TempDir::new()?;
        let one_run_bytes = encoded_run_record_bytes::<1>(1)?;
        let two_run_bytes = one_run_bytes.checked_mul(2).ok_or("test byte overflow")?;
        let mut staged_sort = VariableValueSorter::<1>::new(
            temporary.path(),
            "disk-limit",
            one_record_memory_limit::<1>(1),
            two_run_bytes,
        )?;
        staged_sort.push([2], b"b")?;
        staged_sort.push([1], b"a")?;
        assert!(matches!(
            staged_sort.finish(),
            Err(BulkLoadError::TemporaryFileLimit { .. })
        ));
        assert_eq!(fs::read_dir(temporary.path())?.count(), 0);
        Ok(())
    }

    #[test]
    fn spill_refuses_once_the_final_merge_cannot_fit() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let one_run_bytes = encoded_run_record_bytes::<1>(1)?;
        let mut staged_sort = VariableValueSorter::<1>::new(
            temporary.path(),
            "early-disk-limit",
            one_record_memory_limit::<1>(1),
            one_run_bytes.checked_mul(3).ok_or("test byte overflow")?,
        )?;
        staged_sort.push([3], b"c")?;
        staged_sort.push([2], b"b")?;
        let refused = staged_sort.push([1], b"a");

        assert!(matches!(
            refused,
            Err(BulkLoadError::TemporaryFileLimitBeforeFinish { limit_bytes, required_bytes })
                if Some(limit_bytes) == one_run_bytes.checked_mul(3)
                    && Some(required_bytes) == one_run_bytes.checked_mul(4)
        ));
        assert_eq!(staged_sort.runs.len(), 1);
        assert_eq!(fs::read_dir(staged_sort.workspace.path())?.count(), 1);
        Ok(())
    }

    #[test]
    fn single_run_needs_no_merge_headroom() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let one_run_bytes = encoded_run_record_bytes::<1>(1)?;
        let mut staged_sort = VariableValueSorter::<1>::new(
            temporary.path(),
            "single-run",
            one_record_memory_limit::<1>(1),
            one_run_bytes,
        )?;
        staged_sort.push([1], b"a")?;
        let sorted_records = staged_sort.finish()?;

        assert_eq!(sorted_records.evidence().merge_pass_count, 0);
        assert_eq!(
            sorted_records.evidence().peak_temporary_file_bytes,
            one_run_bytes
        );
        Ok(())
    }

    #[test]
    fn truncated_final_run_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let mut staged_sort = VariableValueSorter::<1>::new(
            temporary.path(),
            "truncated",
            one_record_memory_limit::<1>(4),
            1_024,
        )?;
        staged_sort.push([1], b"data")?;
        let mut sorted_records = staged_sort.finish()?;
        let run_path = sorted_records
            .run_paths()
            .pop()
            .ok_or("sorted run must exist")?;
        File::options().write(true).open(&run_path)?.set_len(9)?;

        assert!(matches!(
            sorted_records.next_record(),
            Err(BulkLoadError::PathUnavailable { .. })
        ));
        Ok(())
    }

    #[test]
    fn forged_value_length_cannot_cross_the_memory_limit() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = TempDir::new()?;
        let memory_limit = one_record_memory_limit::<1>(4);
        let mut staged_sort =
            VariableValueSorter::<1>::new(temporary.path(), "forged-length", memory_limit, 1_024)?;
        staged_sort.push([1], b"data")?;
        let mut sorted_records = staged_sort.finish()?;
        let run_path = sorted_records
            .run_paths()
            .pop()
            .ok_or("sorted run must exist")?;
        let mut run = File::options().write(true).open(&run_path)?;
        run.write_all(&[1])?;
        run.write_all(&u64::MAX.to_be_bytes())?;
        run.flush()?;

        assert!(matches!(
            sorted_records.next_record(),
            Err(BulkLoadError::InvalidInput { .. } | BulkLoadError::AccountedMemoryLimit { .. })
        ));
        Ok(())
    }

    #[test]
    fn forged_value_length_cannot_swallow_the_next_record() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = TempDir::new()?;
        let memory_limit = accounted_record_bytes::<1>(1)?
            .checked_mul(2)
            .ok_or("test byte overflow")?;
        let mut staged_sort = VariableValueSorter::<1>::new(
            temporary.path(),
            "swallowed-record",
            memory_limit,
            1_024,
        )?;
        staged_sort.push([1], b"a")?;
        staged_sort.push([2], b"b")?;
        let mut sorted_records = staged_sort.finish()?;
        let run_path = sorted_records
            .run_paths()
            .pop()
            .ok_or("sorted run must exist")?;
        let mut run = File::options().write(true).open(&run_path)?;
        run.seek(SeekFrom::Start(1))?;
        run.write_all(&11_u64.to_be_bytes())?;
        run.flush()?;

        assert!(sorted_records.next_record()?.is_some());
        assert!(matches!(
            sorted_records.next_record(),
            Err(BulkLoadError::InvalidInput { .. })
        ));
        Ok(())
    }
}
