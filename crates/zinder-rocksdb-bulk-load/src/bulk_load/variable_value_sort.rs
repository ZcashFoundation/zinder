//! Bounded external sorting for fixed-width keys with variable encoded values.

use parking_lot::{Condvar, Mutex};
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    mem::size_of,
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::{Duration, Instant},
};
use tempfile::{Builder as TemporaryDirectoryBuilder, TempDir};

#[cfg(test)]
use super::MAX_MERGE_FAN_IN;
use super::{
    BulkLoadError, MIN_MERGE_FAN_IN, effective_merge_fan_in, path_unavailable,
    validate_merge_fan_in,
};

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
    /// Merge passes that wrote merged runs; zero for at most 256 runs, which
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
    /// A sort that ends with more runs than the active merge fan-in (up to
    /// `MAX_MERGE_FAN_IN`, reduced when process file-descriptor headroom is
    /// tight) merges them in passes that write every record again while the
    /// input runs still exist, so admission keeps room for twice the total run
    /// bytes. A sort that ends with fewer runs streams its final merge and
    /// stays under that reservation. Refusing here, before the run is written,
    /// stops a sort that cannot finish while records are still arriving instead
    /// of after the whole input has been spilled.
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
    /// Merge passes write merged runs only while more runs remain than the
    /// active merge fan-in (up to `MAX_MERGE_FAN_IN`, reduced when process
    /// file-descriptor headroom is tight). The reader merges the remaining runs
    /// as it is read, so the last merge never writes a run and reads it back.
    pub fn finish(self) -> Result<SortedVariableValues<KEY_LEN>, BulkLoadError> {
        let merge_fan_in = effective_merge_fan_in();
        self.finish_with_fan_in(merge_fan_in)
    }

    fn finish_with_fan_in(
        mut self,
        merge_fan_in: usize,
    ) -> Result<SortedVariableValues<KEY_LEN>, BulkLoadError> {
        validate_merge_fan_in(merge_fan_in)?;
        let flush_started = Instant::now();
        self.flush_run()?;
        self.durations.final_run_flush = flush_started.elapsed();
        let mut runs = std::mem::take(&mut self.runs);
        if runs.len() > merge_fan_in {
            let merge_started = Instant::now();
            while runs.len() > merge_fan_in {
                runs = self.merge_pass(runs, merge_fan_in)?;
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
        let source = SortedRunSource::open(runs, merge_fan_in)?;
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

    fn merge_pass(
        &mut self,
        runs: Vec<RunFile>,
        merge_fan_in: usize,
    ) -> Result<Vec<RunFile>, BulkLoadError> {
        let mut pending = runs.into_iter();
        let mut merged = Vec::new();
        let mut group_index = 0_u64;
        loop {
            let group = pending.by_ref().take(merge_fan_in).collect::<Vec<_>>();
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
                merged.push(self.merge_group(group, output_path, merge_fan_in)?);
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
        merge_fan_in: usize,
    ) -> Result<RunFile, BulkLoadError> {
        merge_group_with_budget::<KEY_LEN>(
            &mut self.temporary_files,
            input_runs,
            output_path,
            merge_fan_in,
        )
    }
}

#[derive(Debug)]
struct SharedRunBudget {
    temporary_files: TemporaryFileBudget,
    next_run_index: u64,
    reserved_run_count: u64,
    shards_opened: bool,
}

impl SharedRunBudget {
    fn reserve_initial_run(&mut self, run_file_bytes: u64) -> Result<u64, BulkLoadError> {
        if self.reserved_run_count == 0 {
            self.temporary_files.reserve(run_file_bytes)?;
        } else {
            self.temporary_files.reserve_merged_run(run_file_bytes)?;
        }
        let run_index = self.next_run_index;
        self.next_run_index = self
            .next_run_index
            .checked_add(1)
            .ok_or_else(|| BulkLoadError::invalid("variable run count exceeds u64::MAX"))?;
        self.reserved_run_count = self
            .reserved_run_count
            .checked_add(1)
            .ok_or_else(|| BulkLoadError::invalid("variable run count exceeds u64::MAX"))?;
        Ok(run_index)
    }

    fn discard_reserved_run(
        &mut self,
        path: &Path,
        run_file_bytes: u64,
    ) -> Result<(), BulkLoadError> {
        discard_reserved_file(path, &mut self.temporary_files, run_file_bytes)?;
        self.reserved_run_count = self
            .reserved_run_count
            .checked_sub(1)
            .ok_or_else(|| BulkLoadError::invalid("variable run count underflow"))?;
        Ok(())
    }
}

static NEXT_PARALLEL_SORTER_ID: AtomicU64 = AtomicU64::new(1);

/// Shared workspace and temporary-file budget for concurrent variable-value
/// sort shards that merge into one [`SortedVariableValues`].
///
/// The accounted memory ceiling is split evenly across `shard_count` shards so
/// the sum of every shard's peak accounted memory never exceeds
/// `max_accounted_memory_bytes`. `open_shards` is single-use per sorter so
/// outstanding shards cannot duplicate that allowance. Every shard shares one
/// temporary-file budget enforcing `max_temporary_file_bytes` across all
/// spilled and merged runs.
pub struct ParallelVariableValueSorter<const KEY_LEN: usize> {
    sorter_id: u64,
    workspace: TempDir,
    shard_count: NonZeroU32,
    max_accounted_memory_bytes: u64,
    shard_memory_limit_bytes: u64,
    shared_budget: Mutex<SharedRunBudget>,
}

/// One independent shard writer owned by a worker thread of a
/// [`ParallelVariableValueSorter`].
pub struct VariableValueSortShard<'a, const KEY_LEN: usize> {
    workspace_path: &'a Path,
    shared_budget: &'a Mutex<SharedRunBudget>,
    max_accounted_memory_bytes: u64,
    global_max_accounted_memory_bytes: u64,
    staged_records: Vec<StagedVariableValue<KEY_LEN>>,
    encoded_values: Vec<u8>,
    runs: Vec<RunFile>,
    record_count: u64,
    initial_run_count: u64,
    peak_accounted_memory_bytes: u64,
    peak_oversized_record_bytes: u64,
    initial_run_spill: Duration,
}

/// Flushed runs collected from all shards of a [`ParallelVariableValueSorter`].
///
/// Bound to both the key width (`KEY_LEN`) and originating sorter instance so
/// staged runs cannot be finished against an unrelated sorter's run budget.
#[derive(Debug)]
pub struct StagedParallelVariableRuns<const KEY_LEN: usize> {
    sorter_id: u64,
    runs: Vec<RunFile>,
    record_count: u64,
    initial_run_count: u64,
    peak_accounted_memory_bytes: u64,
    durations: VariableValueSortDurations,
}

impl<const KEY_LEN: usize> ParallelVariableValueSorter<KEY_LEN> {
    /// Creates a shared workspace and temporary-file budget for `shard_count`
    /// concurrent sort shards.
    pub fn new(
        staging_path: &Path,
        artifact_label: &'static str,
        shard_count: NonZeroU32,
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
        let shard_memory_limit_bytes = max_accounted_memory_bytes / u64::from(shard_count.get());
        if shard_memory_limit_bytes < minimum_record_bytes {
            return Err(BulkLoadError::AccountedMemoryLimit {
                limit_bytes: max_accounted_memory_bytes,
                required_bytes: minimum_record_bytes.saturating_mul(u64::from(shard_count.get())),
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
            sorter_id: NEXT_PARALLEL_SORTER_ID.fetch_add(1, Ordering::Relaxed),
            workspace,
            shard_count,
            max_accounted_memory_bytes,
            shard_memory_limit_bytes,
            shared_budget: Mutex::new(SharedRunBudget {
                temporary_files: TemporaryFileBudget::new(max_temporary_file_bytes),
                next_run_index: 0,
                reserved_run_count: 0,
                shards_opened: false,
            }),
        })
    }

    /// Opens one shard writer per configured worker.
    ///
    /// Shards may be opened only once per sorter so outstanding shard buffers
    /// cannot exceed `max_accounted_memory_bytes`.
    pub fn open_shards(&self) -> Result<Vec<VariableValueSortShard<'_, KEY_LEN>>, BulkLoadError> {
        {
            let mut budget = self.shared_budget.lock();
            if budget.shards_opened {
                return Err(BulkLoadError::invalid(
                    "parallel variable-value sorter shards were already opened",
                ));
            }
            budget.shards_opened = true;
        }
        let count = usize::try_from(self.shard_count.get()).unwrap_or(usize::MAX);
        Ok((0..count)
            .map(|_| VariableValueSortShard {
                workspace_path: self.workspace.path(),
                shared_budget: &self.shared_budget,
                max_accounted_memory_bytes: self.shard_memory_limit_bytes,
                global_max_accounted_memory_bytes: self.max_accounted_memory_bytes,
                staged_records: Vec::new(),
                encoded_values: Vec::new(),
                runs: Vec::new(),
                record_count: 0,
                initial_run_count: 0,
                peak_accounted_memory_bytes: 0,
                peak_oversized_record_bytes: 0,
                initial_run_spill: Duration::ZERO,
            })
            .collect())
    }

    fn verify_owned_shards(
        &self,
        shards: &[VariableValueSortShard<'_, KEY_LEN>],
    ) -> Result<(), BulkLoadError> {
        let expected_count = usize::try_from(self.shard_count.get()).unwrap_or(usize::MAX);
        if shards.len() != expected_count {
            return Err(BulkLoadError::invalid(format!(
                "expected {expected_count} variable-value sort shards, got {}",
                shards.len()
            )));
        }
        if shards
            .iter()
            .any(|shard| !std::ptr::eq(shard.shared_budget, &raw const self.shared_budget))
        {
            return Err(BulkLoadError::invalid(
                "cannot flush variable-value sort shards opened by another sorter",
            ));
        }
        Ok(())
    }

    /// Flushes all remaining staged records across `shards` in parallel.
    pub fn flush_shards<'a>(
        &'a self,
        mut shards: Vec<VariableValueSortShard<'a, KEY_LEN>>,
    ) -> Result<StagedParallelVariableRuns<KEY_LEN>, BulkLoadError> {
        self.verify_owned_shards(&shards)?;
        let flush_started = Instant::now();
        let outcomes = if let Some((first, rest)) = shards.split_first_mut() {
            let slots = rest
                .iter_mut()
                .map(|shard| Mutex::new(Some(shard)))
                .collect::<Vec<_>>();
            std::thread::scope(|scope| {
                let mut workers = Vec::with_capacity(slots.len());
                let mut outcomes = Vec::with_capacity(slots.len().saturating_add(1));
                for (index, slot) in slots.iter().enumerate() {
                    if let Ok(worker) = std::thread::Builder::new()
                        .name(format!("zinder-sort-flush-{index}"))
                        .spawn_scoped(scope, move || {
                            let taken = slot.lock().take();
                            taken.map_or(Ok(()), VariableValueSortShard::flush_run)
                        })
                    {
                        workers.push(worker);
                    } else {
                        let taken = slot.lock().take();
                        if let Some(shard) = taken {
                            outcomes.push(shard.flush_run());
                        }
                    }
                }
                outcomes.push(first.flush_run());
                for worker in workers {
                    outcomes.push(
                        worker
                            .join()
                            .unwrap_or_else(|panic| std::panic::resume_unwind(panic)),
                    );
                }
                outcomes
            })
        } else {
            Vec::new()
        };
        let final_run_flush = flush_started.elapsed();
        for outcome in outcomes {
            outcome?;
        }
        self.collect_staged_runs(shards, final_run_flush)
    }

    fn collect_staged_runs(
        &self,
        shards: Vec<VariableValueSortShard<'_, KEY_LEN>>,
        final_run_flush: Duration,
    ) -> Result<StagedParallelVariableRuns<KEY_LEN>, BulkLoadError> {
        let mut runs = Vec::new();
        let mut record_count = 0_u64;
        let mut initial_run_count = 0_u64;
        let mut buffered_peak_bytes = 0_u64;
        let mut oversized_peak_bytes = 0_u64;
        let mut initial_run_spill = Duration::ZERO;
        for shard in shards {
            runs.extend(shard.runs);
            record_count = record_count
                .checked_add(shard.record_count)
                .ok_or_else(|| {
                    BulkLoadError::invalid("variable-value record count exceeds u64::MAX")
                })?;
            initial_run_count = initial_run_count
                .checked_add(shard.initial_run_count)
                .ok_or_else(|| BulkLoadError::invalid("initial run count exceeds u64::MAX"))?;
            buffered_peak_bytes = buffered_peak_bytes
                .checked_add(shard.peak_accounted_memory_bytes)
                .ok_or_else(|| BulkLoadError::invalid("accounted memory bytes exceed u64::MAX"))?;
            oversized_peak_bytes = oversized_peak_bytes.max(shard.peak_oversized_record_bytes);
            initial_run_spill = initial_run_spill.max(shard.initial_run_spill);
        }
        let peak_accounted_memory_bytes = buffered_peak_bytes.max(oversized_peak_bytes);
        Ok(StagedParallelVariableRuns {
            sorter_id: self.sorter_id,
            runs,
            record_count,
            initial_run_count,
            peak_accounted_memory_bytes,
            durations: VariableValueSortDurations {
                initial_run_spill,
                final_run_flush,
                merge: Duration::ZERO,
            },
        })
    }

    /// Merges excess runs and opens a bounded streaming reader.
    ///
    /// Runs above the active merge fan-in (up to `MAX_MERGE_FAN_IN`, reduced
    /// when process file-descriptor headroom is tight) are merged into passes.
    pub fn finish(
        self,
        staged: StagedParallelVariableRuns<KEY_LEN>,
    ) -> Result<SortedVariableValues<KEY_LEN>, BulkLoadError> {
        let merge_fan_in = effective_merge_fan_in();
        self.finish_with_fan_in(staged, merge_fan_in)
    }

    fn finish_with_fan_in(
        self,
        mut staged: StagedParallelVariableRuns<KEY_LEN>,
        merge_fan_in: usize,
    ) -> Result<SortedVariableValues<KEY_LEN>, BulkLoadError> {
        validate_merge_fan_in(merge_fan_in)?;
        if staged.sorter_id != self.sorter_id {
            return Err(BulkLoadError::invalid(
                "cannot finish variable-value runs staged by another sorter",
            ));
        }
        let mut temporary_files = self.shared_budget.into_inner().temporary_files;
        let mut runs = staged.runs;
        let mut merge_pass_count = 0_u64;
        if runs.len() > merge_fan_in {
            let merge_started = Instant::now();
            while runs.len() > merge_fan_in {
                runs = merge_pass_with_budget::<KEY_LEN>(
                    self.workspace.path(),
                    &mut temporary_files,
                    MergePassBudget {
                        pass_count: merge_pass_count,
                        fan_in: merge_fan_in,
                    },
                    runs,
                )?;
                merge_pass_count = merge_pass_count
                    .checked_add(1)
                    .ok_or_else(|| BulkLoadError::invalid("merge pass count exceeds u64::MAX"))?;
            }
            staged.durations.merge = merge_started.elapsed();
        }
        let final_run_file_bytes = runs.iter().try_fold(0_u64, |total, run| {
            total
                .checked_add(run.file_bytes)
                .ok_or_else(|| BulkLoadError::invalid("final run bytes exceed u64::MAX"))
        })?;
        let source = SortedRunSource::open(runs, merge_fan_in)?;
        let evidence = VariableValueSortEvidence {
            record_count: staged.record_count,
            initial_run_count: staged.initial_run_count,
            merge_pass_count,
            peak_accounted_memory_bytes: staged.peak_accounted_memory_bytes,
            max_accounted_memory_bytes: self.max_accounted_memory_bytes,
            peak_temporary_file_bytes: temporary_files.peak_bytes_in_use,
            max_temporary_file_bytes: temporary_files.byte_limit,
            final_run_file_bytes,
        };
        Ok(SortedVariableValues {
            source,
            evidence,
            durations: staged.durations,
            max_accounted_memory_bytes: self.max_accounted_memory_bytes,
            previous_key: None,
            emitted_record_count: 0,
            workspace: Some(self.workspace),
            finished: false,
        })
    }
}

impl<const KEY_LEN: usize> VariableValueSortShard<'_, KEY_LEN> {
    /// Stages one record on this shard, spilling a sorted run before crossing
    /// its per-shard accounted memory limit.
    ///
    /// A single record larger than one shard's staging limit but within the
    /// sorter's global accounted memory limit flushes the shard's buffer and
    /// streams directly to a single-record run file, preserving full serial
    /// record admission without enlarging per-shard buffers.
    pub fn push(&mut self, key: [u8; KEY_LEN], encoded_value: &[u8]) -> Result<(), BulkLoadError> {
        let record_bytes = accounted_record_bytes::<KEY_LEN>(encoded_value.len())?;
        if record_bytes > self.global_max_accounted_memory_bytes {
            return Err(BulkLoadError::AccountedMemoryLimit {
                limit_bytes: self.global_max_accounted_memory_bytes,
                required_bytes: record_bytes,
            });
        }
        if record_bytes > self.max_accounted_memory_bytes {
            let spill_started = Instant::now();
            self.flush_run()
                .map_err(BulkLoadError::temporary_file_limit_before_finish)?;
            self.flush_single_record_run(key, encoded_value)
                .map_err(BulkLoadError::temporary_file_limit_before_finish)?;
            self.initial_run_spill = self
                .initial_run_spill
                .saturating_add(spill_started.elapsed());
            self.record_count = self.record_count.checked_add(1).ok_or_else(|| {
                BulkLoadError::invalid("variable-value record count exceeds u64::MAX")
            })?;
            self.peak_oversized_record_bytes = self.peak_oversized_record_bytes.max(record_bytes);
            return Ok(());
        }
        let current_bytes =
            staged_accounted_memory_bytes(&self.staged_records, self.encoded_values.len())?;
        let required_bytes = current_bytes
            .checked_add(record_bytes)
            .ok_or_else(|| BulkLoadError::invalid("accounted record bytes exceed u64::MAX"))?;
        if required_bytes > self.max_accounted_memory_bytes {
            let spill_started = Instant::now();
            self.flush_run()
                .map_err(BulkLoadError::temporary_file_limit_before_finish)?;
            self.initial_run_spill = self
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
        let current_bytes =
            staged_accounted_memory_bytes(&self.staged_records, self.encoded_values.len())?;
        self.peak_accounted_memory_bytes = self.peak_accounted_memory_bytes.max(current_bytes);
        Ok(())
    }

    fn flush_single_record_run(
        &mut self,
        key: [u8; KEY_LEN],
        encoded_value: &[u8],
    ) -> Result<(), BulkLoadError> {
        let run_file_bytes = encoded_run_record_bytes::<KEY_LEN>(encoded_value.len())?;
        let run_index = self
            .shared_budget
            .lock()
            .reserve_initial_run(run_file_bytes)?;
        let path = self.workspace_path.join(format!("run-{run_index:08}.bin"));
        let single_record = [StagedVariableValue {
            key,
            value_start: 0,
            value_end: encoded_value.len(),
        }];
        if let Err(source) = write_staged_records_run(&single_record, encoded_value, &path) {
            self.shared_budget
                .lock()
                .discard_reserved_run(&path, run_file_bytes)?;
            return Err(source);
        }
        let observed_file_bytes = match fs::metadata(&path) {
            Ok(metadata) => metadata.len(),
            Err(source) => {
                let error = path_unavailable(&path, source);
                self.shared_budget
                    .lock()
                    .discard_reserved_run(&path, run_file_bytes)?;
                return Err(error);
            }
        };
        if observed_file_bytes != run_file_bytes {
            self.shared_budget
                .lock()
                .discard_reserved_run(&path, run_file_bytes)?;
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
        Ok(())
    }

    fn flush_run(&mut self) -> Result<(), BulkLoadError> {
        if self.staged_records.is_empty() {
            return Ok(());
        }
        self.staged_records
            .sort_unstable_by_key(|record| record.key);
        let run_file_bytes = staged_run_file_bytes(&self.staged_records)?;
        let run_index = self
            .shared_budget
            .lock()
            .reserve_initial_run(run_file_bytes)?;
        let path = self.workspace_path.join(format!("run-{run_index:08}.bin"));
        if let Err(source) =
            write_staged_records_run(&self.staged_records, &self.encoded_values, &path)
        {
            self.shared_budget
                .lock()
                .discard_reserved_run(&path, run_file_bytes)?;
            return Err(source);
        }
        let observed_file_bytes = match fs::metadata(&path) {
            Ok(metadata) => metadata.len(),
            Err(source) => {
                let error = path_unavailable(&path, source);
                self.shared_budget
                    .lock()
                    .discard_reserved_run(&path, run_file_bytes)?;
                return Err(error);
            }
        };
        if observed_file_bytes != run_file_bytes {
            self.shared_budget
                .lock()
                .discard_reserved_run(&path, run_file_bytes)?;
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
}

fn staged_accounted_memory_bytes<const KEY_LEN: usize>(
    staged_records: &[StagedVariableValue<KEY_LEN>],
    encoded_values_len: usize,
) -> Result<u64, BulkLoadError> {
    let index_bytes = staged_records
        .len()
        .checked_mul(size_of::<StagedVariableValue<KEY_LEN>>())
        .ok_or_else(|| BulkLoadError::invalid("record index bytes exceed usize::MAX"))?;
    let total_bytes = index_bytes
        .checked_add(encoded_values_len)
        .ok_or_else(|| BulkLoadError::invalid("record bytes exceed usize::MAX"))?;
    u64::try_from(total_bytes).map_err(|_| BulkLoadError::invalid("record bytes exceed u64::MAX"))
}

fn staged_run_file_bytes<const KEY_LEN: usize>(
    staged_records: &[StagedVariableValue<KEY_LEN>],
) -> Result<u64, BulkLoadError> {
    staged_records.iter().try_fold(0_u64, |total, record| {
        let value_bytes = record
            .value_end
            .checked_sub(record.value_start)
            .ok_or_else(|| BulkLoadError::invalid("staged value range is reversed"))?;
        total
            .checked_add(encoded_run_record_bytes::<KEY_LEN>(value_bytes)?)
            .ok_or_else(|| BulkLoadError::invalid("run file bytes exceed u64::MAX"))
    })
}

fn write_staged_records_run<const KEY_LEN: usize>(
    staged_records: &[StagedVariableValue<KEY_LEN>],
    encoded_values: &[u8],
    path: &Path,
) -> Result<(), BulkLoadError> {
    let file = File::create(path).map_err(|source| path_unavailable(path, source))?;
    let mut writer = BufWriter::new(file);
    for record in staged_records {
        let encoded_value = encoded_values
            .get(record.value_start..record.value_end)
            .ok_or_else(|| BulkLoadError::invalid("staged value range is unavailable"))?;
        writer
            .write_all(&record.key)
            .and_then(|()| {
                let value_len = u64::try_from(encoded_value.len())
                    .map_err(|_| std::io::Error::other("encoded value length exceeds u64::MAX"))?;
                writer.write_all(&value_len.to_be_bytes())
            })
            .and_then(|()| writer.write_all(encoded_value))
            .map_err(|source| path_unavailable(path, source))?;
    }
    writer
        .flush()
        .map_err(|source| path_unavailable(path, source))
}

#[derive(Clone, Copy)]
struct MergePassBudget {
    pass_count: u64,
    fan_in: usize,
}

fn merge_pass_with_budget<const KEY_LEN: usize>(
    workspace_path: &Path,
    temporary_files: &mut TemporaryFileBudget,
    pass: MergePassBudget,
    runs: Vec<RunFile>,
) -> Result<Vec<RunFile>, BulkLoadError> {
    validate_merge_fan_in(pass.fan_in)?;
    let mut pending = runs.into_iter();
    let mut merged = Vec::new();
    let mut group_index = 0_u64;
    loop {
        let group = pending.by_ref().take(pass.fan_in).collect::<Vec<_>>();
        if group.is_empty() {
            break;
        }
        if group.len() == 1 {
            merged.extend(group);
        } else {
            let output_path =
                workspace_path.join(format!("merge-{:04}-{group_index:08}.bin", pass.pass_count));
            merged.push(merge_group_with_budget::<KEY_LEN>(
                temporary_files,
                group,
                output_path,
                pass.fan_in,
            )?);
        }
        group_index = group_index
            .checked_add(1)
            .ok_or_else(|| BulkLoadError::invalid("merge group count exceeds u64::MAX"))?;
    }
    Ok(merged)
}

fn merge_group_with_budget<const KEY_LEN: usize>(
    temporary_files: &mut TemporaryFileBudget,
    input_runs: Vec<RunFile>,
    output_path: PathBuf,
    merge_fan_in: usize,
) -> Result<RunFile, BulkLoadError> {
    let input_file_bytes = input_runs.iter().try_fold(0_u64, |total, run| {
        total
            .checked_add(run.file_bytes)
            .ok_or_else(|| BulkLoadError::invalid("merged run bytes exceed u64::MAX"))
    })?;
    let output_file_bytes = input_file_bytes;
    temporary_files.reserve(output_file_bytes)?;
    let merge_outcome = merge_run_group::<KEY_LEN>(&input_runs, &output_path, merge_fan_in);
    if let Err(source) = merge_outcome {
        discard_reserved_file(&output_path, temporary_files, output_file_bytes)?;
        return Err(source);
    }
    let observed_file_bytes = match fs::metadata(&output_path) {
        Ok(metadata) => metadata.len(),
        Err(source) => {
            let error = path_unavailable(&output_path, source);
            discard_reserved_file(&output_path, temporary_files, output_file_bytes)?;
            return Err(error);
        }
    };
    if observed_file_bytes != output_file_bytes {
        discard_reserved_file(&output_path, temporary_files, output_file_bytes)?;
        return Err(BulkLoadError::invalid(
            "merged run file size differs from its admitted bytes",
        ));
    }
    for input_run in &input_runs {
        remove_temporary_file(&input_run.path)?;
    }
    drop(input_runs);
    temporary_files.release(input_file_bytes)?;
    Ok(RunFile {
        path: output_path,
        file_bytes: output_file_bytes,
    })
}

/// Streaming reader for a completed variable-value sort.
///
/// A fault in a run, such as a truncated record, a length that differs from
/// the run's admitted bytes, or keys out of order, is reported by the read
/// that reaches it. Only a read that returns `None` has checked every run, so
/// a consumer that relies on the sort's integrity reads to the end. That read
/// also removes the runs and reports a failure to remove them. Dropping the
/// reader earlier removes them on a best-effort basis. Records with equal keys
/// are adjacent, but their relative value order is intentionally not a
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
            std::mem::replace(&mut self.source, SortedRunSource::Drained).remove_runs()?;
            if let Some(workspace) = self.workspace.take() {
                let workspace_path = workspace.path().to_path_buf();
                workspace
                    .close()
                    .map_err(|source| path_unavailable(&workspace_path, source))?;
            }
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
            SortedRunSource::Single(run) => vec![run.run.path.clone()],
            SortedRunSource::Merging { runs, .. } => {
                runs.iter().map(|run| run.run.path.clone()).collect()
            }
        }
    }
}

/// One run opened for reading, with the bytes read from it so far.
///
/// A run never holds more bytes than were admitted for it, and at its end it
/// must hold exactly that many, so a record replaced by a well-framed record
/// of another size is refused even when the record count still matches.
struct RunReader {
    run: RunFile,
    reader: BufReader<File>,
    consumed_bytes: u64,
}

impl RunReader {
    fn open(run: RunFile) -> Result<Self, BulkLoadError> {
        let reader = File::open(&run.path)
            .map(BufReader::new)
            .map_err(|source| path_unavailable(&run.path, source))?;
        Ok(Self {
            run,
            reader,
            consumed_bytes: 0,
        })
    }

    /// Reads the next record head, or `None` once the run is exhausted at
    /// exactly its admitted length.
    fn next_head<const KEY_LEN: usize>(
        &mut self,
    ) -> Result<Option<RunHead<KEY_LEN>>, BulkLoadError> {
        let Some(head) = read_run_head::<KEY_LEN>(&mut self.reader, &self.run.path)? else {
            if self.consumed_bytes != self.run.file_bytes {
                return Err(BulkLoadError::invalid(
                    "variable run length differs from its admitted bytes",
                ));
            }
            return Ok(None);
        };
        self.consume(encoded_run_record_bytes::<KEY_LEN>(0)?)?;
        Ok(Some(head))
    }

    /// Reads the value that follows the head `next_head` just returned.
    fn read_value(&mut self, encoded_value: &mut [u8]) -> Result<(), BulkLoadError> {
        self.consume(
            u64::try_from(encoded_value.len())
                .map_err(|_| BulkLoadError::invalid("encoded value length exceeds u64::MAX"))?,
        )?;
        self.reader
            .read_exact(encoded_value)
            .map_err(|source| path_unavailable(&self.run.path, source))
    }

    fn consume(&mut self, bytes: u64) -> Result<(), BulkLoadError> {
        let consumed_bytes = self
            .consumed_bytes
            .checked_add(bytes)
            .filter(|consumed_bytes| *consumed_bytes <= self.run.file_bytes)
            .ok_or_else(|| {
                BulkLoadError::invalid("variable run is longer than its admitted bytes")
            })?;
        self.consumed_bytes = consumed_bytes;
        Ok(())
    }
}

/// Runs a completed sort hands to its reader.
enum SortedRunSource<const KEY_LEN: usize> {
    /// No run remains, because the input was empty or every record was read.
    Drained,
    /// One run, read front to back.
    Single(RunReader),
    /// At most the active merge fan-in runs (up to `MAX_MERGE_FAN_IN`), merged
    /// by smallest key as they are read.
    Merging {
        runs: Vec<RunReader>,
        heads: BinaryHeap<Reverse<([u8; KEY_LEN], usize, u64)>>,
        pending_run: Option<usize>,
    },
}

impl<const KEY_LEN: usize> SortedRunSource<KEY_LEN> {
    fn open(mut runs: Vec<RunFile>, merge_fan_in: usize) -> Result<Self, BulkLoadError> {
        validate_merge_fan_in(merge_fan_in)?;
        if runs.len() > merge_fan_in {
            return Err(BulkLoadError::invalid(
                "variable merge exceeded its file-descriptor fan-in",
            ));
        }
        if runs.len() <= 1 {
            return Ok(match runs.pop() {
                None => Self::Drained,
                Some(run) => Self::Single(RunReader::open(run)?),
            });
        }
        let mut readers = Vec::with_capacity(runs.len());
        let mut heads = BinaryHeap::new();
        for (run_index, run) in runs.into_iter().enumerate() {
            let mut reader = RunReader::open(run)?;
            if let Some(head) = reader.next_head::<KEY_LEN>()? {
                heads.push(Reverse((head.key, run_index, head.value_len)));
            }
            readers.push(reader);
        }
        Ok(Self::Merging {
            runs: readers,
            heads,
            pending_run: None,
        })
    }

    /// Removes every run file, reporting the first removal that fails.
    fn remove_runs(self) -> Result<(), BulkLoadError> {
        let runs = match self {
            Self::Drained => Vec::new(),
            Self::Single(run) => vec![run],
            Self::Merging { runs, .. } => runs,
        };
        for run in runs {
            remove_temporary_file(&run.run.path)?;
        }
        Ok(())
    }

    /// Returns the smallest remaining record head and the run its value follows in.
    fn next_head(&mut self) -> Result<Option<(RunHead<KEY_LEN>, usize)>, BulkLoadError> {
        match self {
            Self::Drained => Ok(None),
            Self::Single(run) => Ok(run.next_head::<KEY_LEN>()?.map(|head| (head, 0))),
            Self::Merging {
                runs,
                heads,
                pending_run,
            } => {
                if let Some(run_index) = pending_run.take() {
                    let run = runs
                        .get_mut(run_index)
                        .ok_or_else(|| BulkLoadError::invalid("merged run index is unavailable"))?;
                    if let Some(head) = run.next_head::<KEY_LEN>()? {
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
        let run = match self {
            Self::Drained => {
                return Err(BulkLoadError::invalid("drained sort has no value to read"));
            }
            Self::Single(run) => run,
            Self::Merging {
                runs, pending_run, ..
            } => {
                *pending_run = Some(run_index);
                runs.get_mut(run_index)
                    .ok_or_else(|| BulkLoadError::invalid("merged run index is unavailable"))?
            }
        };
        run.read_value(encoded_value)
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
    merge_fan_in: usize,
) -> Result<(), BulkLoadError> {
    validate_merge_fan_in(merge_fan_in)?;
    if input_runs.len() > merge_fan_in {
        return Err(BulkLoadError::invalid(
            "variable merge exceeded its file-descriptor fan-in",
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

const WRITER_CREDIT_MIN_MEMORY_BYTES: u64 = 16 * 1024 * 1024;
const WRITER_CREDIT_CHUNK_BYTES: u64 = 32 * 1024;

/// Maximum number of disjoint key-prefix shards supported by two-byte prefix
/// partitioning.
pub const MAX_KEY_PREFIX_SHARD_COUNT: u32 = 1 << 16;

/// Maps a fixed-width key into `[0, shard_count)` by its leading big-endian
/// two-byte prefix, preserving strict ascending range separation across shards.
#[must_use]
pub fn key_prefix_shard_index<const KEY_LEN: usize>(
    key: &[u8; KEY_LEN],
    shard_count: NonZeroU32,
) -> usize {
    const {
        assert!(
            KEY_LEN >= 2,
            "key-prefix sharding requires keys of at least two bytes"
        );
    }
    let effective_shards = shard_count.get().min(MAX_KEY_PREFIX_SHARD_COUNT);
    let prefix = u16::from_be_bytes([key[0], key[1]]);
    let shard = u16::try_from((u32::from(prefix).saturating_mul(effective_shards)) >> 16)
        .unwrap_or(u16::MAX);
    usize::from(shard)
}

/// Completed sharded variable-value sort exposing disjoint ascending shard streams.
pub struct ShardedSortedVariableValues<const KEY_LEN: usize> {
    shards: Vec<SortedVariableValues<KEY_LEN>>,
    evidence: VariableValueSortEvidence,
    durations: VariableValueSortDurations,
}

impl<const KEY_LEN: usize> ShardedSortedVariableValues<KEY_LEN> {
    /// Consumes the container and returns each shard stream in ascending prefix order.
    #[must_use]
    pub fn into_shards(self) -> Vec<SortedVariableValues<KEY_LEN>> {
        self.shards
    }

    /// Number of key-prefix shards in this sort.
    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Aggregate bounded-work evidence across all shards.
    #[must_use]
    pub fn evidence(&self) -> VariableValueSortEvidence {
        self.evidence
    }

    /// Wall durations observed across flush and merge phases.
    #[must_use]
    pub fn durations(&self) -> VariableValueSortDurations {
        self.durations
    }
}

struct SharedTempFileState {
    active: u64,
    live_data_bytes: u64,
    peak: u64,
    max: u64,
    reserved_run_count: u64,
}

struct SharedTempFileBudget {
    state: Mutex<SharedTempFileState>,
}

impl SharedTempFileBudget {
    fn new(max_bytes: u64) -> Self {
        Self {
            state: Mutex::new(SharedTempFileState {
                active: 0,
                live_data_bytes: 0,
                peak: 0,
                max: max_bytes,
                reserved_run_count: 0,
            }),
        }
    }

    fn reserve_initial_run(&self, bytes: u64) -> Result<(), BulkLoadError> {
        let mut guard = self.state.lock();
        let next_active = guard
            .active
            .checked_add(bytes)
            .ok_or_else(|| BulkLoadError::invalid("temporary file byte sum exceeds u64::MAX"))?;
        let next_live_data = guard
            .live_data_bytes
            .checked_add(bytes)
            .ok_or_else(|| BulkLoadError::invalid("temporary file byte sum exceeds u64::MAX"))?;
        let headroom_required = if guard.reserved_run_count == 0 {
            next_live_data
        } else {
            next_live_data
                .checked_mul(2)
                .ok_or_else(|| BulkLoadError::invalid("temporary file byte sum exceeds u64::MAX"))?
        };
        let required_bytes = headroom_required.max(next_active);
        if required_bytes > guard.max {
            return Err(BulkLoadError::TemporaryFileLimit {
                limit_bytes: guard.max,
                required_bytes,
            });
        }
        guard.reserved_run_count = guard
            .reserved_run_count
            .checked_add(1)
            .ok_or_else(|| BulkLoadError::invalid("sharded run count exceeds u64::MAX"))?;
        guard.active = next_active;
        guard.live_data_bytes = next_live_data;
        guard.peak = guard.peak.max(next_active);
        drop(guard);
        Ok(())
    }

    fn release_initial_run(&self, bytes: u64) -> Result<(), BulkLoadError> {
        let mut guard = self.state.lock();
        let remaining = guard.active.checked_sub(bytes).ok_or_else(|| {
            BulkLoadError::invalid("released more temporary file bytes than reserved")
        })?;
        let remaining_live = guard.live_data_bytes.checked_sub(bytes).ok_or_else(|| {
            BulkLoadError::invalid("released more live initial run bytes than reserved")
        })?;
        guard.reserved_run_count = guard
            .reserved_run_count
            .checked_sub(1)
            .ok_or_else(|| BulkLoadError::invalid("sharded run count underflow"))?;
        guard.active = remaining;
        guard.live_data_bytes = remaining_live;
        drop(guard);
        Ok(())
    }

    fn reserve(&self, bytes: u64) -> Result<(), BulkLoadError> {
        let mut guard = self.state.lock();
        let next = guard
            .active
            .checked_add(bytes)
            .ok_or_else(|| BulkLoadError::invalid("temporary file byte sum exceeds u64::MAX"))?;
        if next > guard.max {
            return Err(BulkLoadError::TemporaryFileLimit {
                limit_bytes: guard.max,
                required_bytes: next,
            });
        }
        guard.active = next;
        guard.peak = guard.peak.max(next);
        drop(guard);
        Ok(())
    }

    fn release(&self, bytes: u64) -> Result<(), BulkLoadError> {
        let mut guard = self.state.lock();
        let remaining = guard.active.checked_sub(bytes).ok_or_else(|| {
            BulkLoadError::invalid("released more temporary file bytes than reserved")
        })?;
        guard.active = remaining;
        drop(guard);
        Ok(())
    }

    fn peak_bytes(&self) -> u64 {
        self.state.lock().peak
    }
}

struct SharedMemoryTracker {
    max_accounted_bytes: u64,
    per_partition_bytes: u64,
    active_bytes: AtomicU64,
    peak_bytes: AtomicU64,
    in_flight_work: AtomicU64,
    transition_epoch: AtomicU64,
    waiters: AtomicU64,
    wait_lock: Mutex<()>,
    reclaimed: Condvar,
    #[cfg(test)]
    hook: Option<std::sync::Arc<dyn ReclaimRaceHook>>,
}

impl SharedMemoryTracker {
    fn new(max_accounted_bytes: u64, shard_count: NonZeroU32) -> Self {
        let per_partition_bytes = (max_accounted_bytes / u64::from(shard_count.get())).max(1);
        Self {
            max_accounted_bytes,
            per_partition_bytes,
            active_bytes: AtomicU64::new(0),
            peak_bytes: AtomicU64::new(0),
            in_flight_work: AtomicU64::new(0),
            transition_epoch: AtomicU64::new(0),
            waiters: AtomicU64::new(0),
            wait_lock: Mutex::new(()),
            reclaimed: Condvar::new(),
            #[cfg(test)]
            hook: None,
        }
    }

    fn try_reserve_bytes(&self, bytes: u64) -> bool {
        let updated = self
            .active_bytes
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                let next = current.checked_add(bytes)?;
                (next <= self.max_accounted_bytes).then_some(next)
            });
        updated.is_ok_and(|previous| {
            let next = previous.saturating_add(bytes);
            self.peak_bytes.fetch_max(next, Ordering::AcqRel);
            true
        })
    }

    fn try_reserve_up_to(&self, max_bytes: u64) -> u64 {
        if max_bytes == 0 {
            return 0;
        }
        let mut current = self.active_bytes.load(Ordering::SeqCst);
        loop {
            let headroom = self.max_accounted_bytes.saturating_sub(current);
            let grant = headroom.min(max_bytes);
            if grant == 0 {
                return 0;
            }
            let next = current.saturating_add(grant);
            match self.active_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    self.peak_bytes.fetch_max(next, Ordering::AcqRel);
                    return grant;
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn release_bytes(&self, bytes: u64) {
        if bytes > 0 {
            self.active_bytes.fetch_sub(bytes, Ordering::SeqCst);
            self.transition_epoch.fetch_add(1, Ordering::SeqCst);
        }
        self.notify_waiters();
    }

    fn record_transition(&self) {
        self.transition_epoch.fetch_add(1, Ordering::SeqCst);
        self.notify_waiters();
    }

    fn notify_waiters(&self) {
        if self.waiters.load(Ordering::SeqCst) > 0 {
            let _guard = self.wait_lock.lock();
            self.reclaimed.notify_all();
        }
    }

    fn peak_bytes(&self) -> u64 {
        self.peak_bytes.load(Ordering::Acquire)
    }

    fn try_reserve_in_flight(&self, bytes: u64) -> Option<InFlightMemoryGuard<'_>> {
        self.in_flight_work.fetch_add(1, Ordering::SeqCst);
        if self.try_reserve_bytes(bytes) {
            #[cfg(test)]
            if let Some(hook) = &self.hook {
                hook.on_after_reserve_before_guard_return(
                    self.in_flight_work.load(Ordering::SeqCst),
                );
            }
            return Some(InFlightMemoryGuard::from_registered(self, bytes));
        }
        self.in_flight_work.fetch_sub(1, Ordering::SeqCst);
        self.notify_waiters();
        None
    }

    fn try_reserve_in_flight_locked(
        &self,
        bytes: u64,
        _wait_guard: &parking_lot::MutexGuard<'_, ()>,
    ) -> Option<InFlightMemoryGuard<'_>> {
        self.in_flight_work.fetch_add(1, Ordering::SeqCst);
        if self.try_reserve_bytes(bytes) {
            #[cfg(test)]
            if let Some(hook) = &self.hook {
                hook.on_after_reserve_before_guard_return(
                    self.in_flight_work.load(Ordering::SeqCst),
                );
            }
            return Some(InFlightMemoryGuard::from_registered(self, bytes));
        }
        self.in_flight_work.fetch_sub(1, Ordering::SeqCst);
        // `wait_lock` is already held by `_wait_guard`; notify directly to avoid re-locking.
        self.reclaimed.notify_all();
        None
    }
}

struct WaiterGuard<'tracker> {
    waiters: &'tracker AtomicU64,
}

impl<'tracker> WaiterGuard<'tracker> {
    fn new(waiters: &'tracker AtomicU64) -> Self {
        waiters.fetch_add(1, Ordering::SeqCst);
        Self { waiters }
    }
}

impl Drop for WaiterGuard<'_> {
    fn drop(&mut self) {
        self.waiters.fetch_sub(1, Ordering::SeqCst);
    }
}

struct InFlightMemoryGuard<'tracker> {
    tracker: &'tracker SharedMemoryTracker,
    bytes: u64,
    count_in_flight: bool,
    active: bool,
}

impl<'tracker> InFlightMemoryGuard<'tracker> {
    fn new(tracker: &'tracker SharedMemoryTracker, bytes: u64, count_in_flight: bool) -> Self {
        if count_in_flight {
            tracker.in_flight_work.fetch_add(1, Ordering::SeqCst);
        }
        Self {
            tracker,
            bytes,
            count_in_flight,
            active: true,
        }
    }

    fn from_registered(tracker: &'tracker SharedMemoryTracker, bytes: u64) -> Self {
        Self {
            tracker,
            bytes,
            count_in_flight: true,
            active: true,
        }
    }

    fn add_bytes(&mut self, extra_bytes: u64) {
        self.bytes = self.bytes.saturating_add(extra_bytes);
    }

    fn consume_bytes(&mut self, consumed_bytes: u64) {
        self.bytes = self.bytes.saturating_sub(consumed_bytes);
    }

    fn commit_and_release_excess(mut self, consumed_bytes: u64) {
        let excess = self.bytes.saturating_sub(consumed_bytes);
        self.bytes = 0;
        self.active = false;
        if excess > 0 {
            self.tracker
                .active_bytes
                .fetch_sub(excess, Ordering::SeqCst);
            self.tracker.transition_epoch.fetch_add(1, Ordering::SeqCst);
            #[cfg(test)]
            if self.count_in_flight
                && let Some(hook) = &self.tracker.hook
            {
                hook.on_after_guard_bytes_released(
                    self.tracker.active_bytes.load(Ordering::SeqCst),
                    self.tracker.in_flight_work.load(Ordering::SeqCst),
                );
            }
        }
        if self.count_in_flight {
            self.tracker.in_flight_work.fetch_sub(1, Ordering::SeqCst);
        }
        if excess > 0 || self.count_in_flight {
            self.tracker.notify_waiters();
        }
    }
}

impl Drop for InFlightMemoryGuard<'_> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        let released = self.bytes;
        self.bytes = 0;
        if released > 0 {
            self.tracker
                .active_bytes
                .fetch_sub(released, Ordering::SeqCst);
            self.tracker.transition_epoch.fetch_add(1, Ordering::SeqCst);
            #[cfg(test)]
            if self.count_in_flight
                && let Some(hook) = &self.tracker.hook
            {
                hook.on_after_guard_bytes_released(
                    self.tracker.active_bytes.load(Ordering::SeqCst),
                    self.tracker.in_flight_work.load(Ordering::SeqCst),
                );
            }
        }
        if self.count_in_flight {
            self.tracker.in_flight_work.fetch_sub(1, Ordering::SeqCst);
        }
        if released > 0 || self.count_in_flight {
            self.tracker.notify_waiters();
        }
    }
}

#[derive(Clone, Copy)]
struct VecReserveSpec {
    additional: usize,
    max_growth_bytes: u64,
    min_initial_elems: usize,
    operation: &'static str,
}

fn reserve_vec_within_budget<T>(
    buffer: &mut Vec<T>,
    spec: VecReserveSpec,
) -> Result<u64, BulkLoadError> {
    let needed_cap = buffer
        .len()
        .checked_add(spec.additional)
        .ok_or_else(|| BulkLoadError::invalid("staged partition capacity exceeds usize::MAX"))?;
    let old_cap = buffer.capacity();
    if needed_cap <= old_cap {
        return Ok(0);
    }
    let elem_size = size_of::<T>();
    if elem_size == 0 {
        return Err(BulkLoadError::invalid(
            "zero-sized partition staging elements are unsupported",
        ));
    }
    let min_delta_elems = needed_cap.saturating_sub(old_cap);
    let elem_size_u64 = u64::try_from(elem_size)
        .map_err(|_| BulkLoadError::invalid("element size exceeds u64::MAX"))?;
    let max_delta_elems =
        usize::try_from(spec.max_growth_bytes / elem_size_u64).unwrap_or(usize::MAX);
    if max_delta_elems < min_delta_elems {
        return Err(BulkLoadError::invalid(
            "partition vector growth exceeds accounted memory budget",
        ));
    }
    let desired_cap = old_cap
        .saturating_mul(2)
        .max(spec.min_initial_elems)
        .max(needed_cap);
    let target_delta_elems = desired_cap
        .saturating_sub(old_cap)
        .clamp(min_delta_elems, max_delta_elems);
    let reserve_from_len = old_cap
        .saturating_add(target_delta_elems)
        .saturating_sub(buffer.len());
    buffer
        .try_reserve_exact(reserve_from_len)
        .map_err(|source| BulkLoadError::MemoryAllocation {
            operation: spec.operation,
            source,
        })?;
    let actual_delta_bytes = buffer
        .capacity()
        .saturating_sub(old_cap)
        .checked_mul(elem_size)
        .ok_or_else(|| BulkLoadError::invalid("allocated partition bytes exceed usize::MAX"))?;
    u64::try_from(actual_delta_bytes)
        .map_err(|_| BulkLoadError::invalid("allocated partition bytes exceed u64::MAX"))
}

struct PartitionBatch<const KEY_LEN: usize> {
    staged_records: Vec<StagedVariableValue<KEY_LEN>>,
    encoded_values: Vec<u8>,
    accounted_bytes: u64,
}

impl<const KEY_LEN: usize> Default for PartitionBatch<KEY_LEN> {
    fn default() -> Self {
        Self {
            staged_records: Vec::new(),
            encoded_values: Vec::new(),
            accounted_bytes: 0,
        }
    }
}

impl<const KEY_LEN: usize> PartitionBatch<KEY_LEN> {
    fn is_empty(&self) -> bool {
        self.staged_records.is_empty()
    }

    fn allocated_bytes(&self) -> Result<u64, BulkLoadError> {
        let rec_bytes = self
            .staged_records
            .capacity()
            .checked_mul(size_of::<StagedVariableValue<KEY_LEN>>())
            .ok_or_else(|| {
                BulkLoadError::invalid("partition record capacity exceeds usize::MAX")
            })?;
        let total = rec_bytes
            .checked_add(self.encoded_values.capacity())
            .ok_or_else(|| BulkLoadError::invalid("partition allocated bytes exceed usize::MAX"))?;
        u64::try_from(total)
            .map_err(|_| BulkLoadError::invalid("partition allocated bytes exceed u64::MAX"))
    }

    fn min_growth_bytes_split(
        &self,
        additional_records: usize,
        additional_value_bytes: usize,
    ) -> Result<(u64, u64), BulkLoadError> {
        let needed_rec = self
            .staged_records
            .len()
            .checked_add(additional_records)
            .ok_or_else(|| BulkLoadError::invalid("staged record count exceeds usize::MAX"))?;
        let extra_rec = needed_rec.saturating_sub(self.staged_records.capacity());
        let rec_bytes_usize = extra_rec
            .checked_mul(size_of::<StagedVariableValue<KEY_LEN>>())
            .ok_or_else(|| BulkLoadError::invalid("partition growth bytes exceed usize::MAX"))?;
        let rec_bytes = u64::try_from(rec_bytes_usize)
            .map_err(|_| BulkLoadError::invalid("partition growth bytes exceed u64::MAX"))?;

        let needed_val = self
            .encoded_values
            .len()
            .checked_add(additional_value_bytes)
            .ok_or_else(|| BulkLoadError::invalid("staged encoded buffer exceeds usize::MAX"))?;
        let extra_val = needed_val.saturating_sub(self.encoded_values.capacity());
        let val_bytes = u64::try_from(extra_val)
            .map_err(|_| BulkLoadError::invalid("partition growth bytes exceed u64::MAX"))?;
        Ok((rec_bytes, val_bytes))
    }

    fn min_growth_bytes(
        &self,
        additional_records: usize,
        additional_value_bytes: usize,
    ) -> Result<u64, BulkLoadError> {
        let (rec_bytes, val_bytes) =
            self.min_growth_bytes_split(additional_records, additional_value_bytes)?;
        rec_bytes
            .checked_add(val_bytes)
            .ok_or_else(|| BulkLoadError::invalid("partition growth bytes exceed u64::MAX"))
    }

    fn desired_growth_slack(
        &self,
        additional_records: usize,
        additional_value_bytes: usize,
    ) -> u64 {
        let rec_size = size_of::<StagedVariableValue<KEY_LEN>>();
        let needed_rec = self.staged_records.len().saturating_add(additional_records);
        let rec_slack = if needed_rec <= self.staged_records.capacity() {
            0
        } else {
            let cap_bytes = u64::try_from(self.staged_records.capacity().saturating_mul(rec_size))
                .unwrap_or(u64::MAX);
            let min_slack = u64::try_from(rec_size.saturating_mul(16)).unwrap_or(u64::MAX);
            cap_bytes.max(min_slack)
        };
        let needed_val = self
            .encoded_values
            .len()
            .saturating_add(additional_value_bytes);
        let val_slack = if needed_val <= self.encoded_values.capacity() {
            0
        } else {
            u64::try_from(self.encoded_values.capacity())
                .unwrap_or(u64::MAX)
                .max(256)
        };
        rec_slack.saturating_add(val_slack)
    }

    fn reserve_within(
        &mut self,
        additional_records: usize,
        additional_value_bytes: usize,
        max_growth_bytes: u64,
        charged: &mut u64,
    ) -> Result<(), BulkLoadError> {
        let (min_rec_bytes, min_val_bytes) =
            self.min_growth_bytes_split(additional_records, additional_value_bytes)?;
        let total_min = min_rec_bytes
            .checked_add(min_val_bytes)
            .ok_or_else(|| BulkLoadError::invalid("partition growth bytes exceed u64::MAX"))?;
        let mut remaining_slack = max_growth_bytes.saturating_sub(total_min);
        let rec_slack_budget = if min_val_bytes == 0 {
            remaining_slack
        } else {
            remaining_slack / 2
        };
        let rec_delta = reserve_vec_within_budget(
            &mut self.staged_records,
            VecReserveSpec {
                additional: additional_records,
                max_growth_bytes: min_rec_bytes.saturating_add(rec_slack_budget),
                min_initial_elems: 16,
                operation: "sharded partition record staging",
            },
        )?;
        self.accounted_bytes = self
            .accounted_bytes
            .checked_add(rec_delta)
            .ok_or_else(|| BulkLoadError::invalid("partition accounted bytes exceed u64::MAX"))?;
        *charged = charged.saturating_add(rec_delta);
        let rec_slack_used = rec_delta.saturating_sub(min_rec_bytes);
        remaining_slack = remaining_slack.saturating_sub(rec_slack_used);

        let val_delta = reserve_vec_within_budget(
            &mut self.encoded_values,
            VecReserveSpec {
                additional: additional_value_bytes,
                max_growth_bytes: min_val_bytes.saturating_add(remaining_slack),
                min_initial_elems: 256,
                operation: "sharded partition value staging",
            },
        )?;
        self.accounted_bytes = self
            .accounted_bytes
            .checked_add(val_delta)
            .ok_or_else(|| BulkLoadError::invalid("partition accounted bytes exceed u64::MAX"))?;
        *charged = charged.saturating_add(val_delta);
        debug_assert_eq!(self.allocated_bytes().ok(), Some(self.accounted_bytes));
        Ok(())
    }

    fn push_record_within(
        &mut self,
        key: [u8; KEY_LEN],
        encoded_value: &[u8],
        max_growth_bytes: u64,
        charged: &mut u64,
    ) -> Result<(), BulkLoadError> {
        let value_start = self.encoded_values.len();
        let value_end = value_start
            .checked_add(encoded_value.len())
            .ok_or_else(|| BulkLoadError::invalid("staged encoded buffer exceeds usize::MAX"))?;
        self.reserve_within(1, encoded_value.len(), max_growth_bytes, charged)?;
        self.encoded_values.extend_from_slice(encoded_value);
        self.staged_records.push(StagedVariableValue {
            key,
            value_start,
            value_end,
        });
        debug_assert_eq!(self.allocated_bytes().ok(), Some(self.accounted_bytes));
        Ok(())
    }

    fn append_batch_within(
        &mut self,
        other: &mut Self,
        max_growth_bytes: u64,
        charged: &mut u64,
    ) -> Result<(), BulkLoadError> {
        if other.staged_records.is_empty() {
            return Ok(());
        }
        let base_offset = self.encoded_values.len();
        let next_encoded_len = base_offset
            .checked_add(other.encoded_values.len())
            .ok_or_else(|| BulkLoadError::invalid("staged encoded buffer exceeds usize::MAX"))?;
        self.reserve_within(
            other.staged_records.len(),
            other.encoded_values.len(),
            max_growth_bytes,
            charged,
        )?;
        debug_assert!(next_encoded_len >= base_offset);
        self.encoded_values.extend_from_slice(&other.encoded_values);
        for mut entry in other.staged_records.drain(..) {
            entry.value_start = base_offset.checked_add(entry.value_start).ok_or_else(|| {
                BulkLoadError::invalid("partition batch offset exceeds usize::MAX")
            })?;
            entry.value_end = base_offset.checked_add(entry.value_end).ok_or_else(|| {
                BulkLoadError::invalid("partition batch offset exceeds usize::MAX")
            })?;
            self.staged_records.push(entry);
        }
        other.encoded_values.clear();
        other.accounted_bytes = other.allocated_bytes()?;
        debug_assert_eq!(self.allocated_bytes().ok(), Some(self.accounted_bytes));
        Ok(())
    }
}

struct PartitionState<const KEY_LEN: usize> {
    staging: PartitionBatch<KEY_LEN>,
    next_run_index: u64,
    runs: Vec<RunFile>,
    record_count: u64,
    initial_run_count: u64,
    initial_run_spill_duration: Duration,
}

impl<const KEY_LEN: usize> Default for PartitionState<KEY_LEN> {
    fn default() -> Self {
        Self {
            staging: PartitionBatch::default(),
            next_run_index: 0,
            runs: Vec::new(),
            record_count: 0,
            initial_run_count: 0,
            initial_run_spill_duration: Duration::ZERO,
        }
    }
}

impl<const KEY_LEN: usize> PartitionState<KEY_LEN> {
    fn take_pending_flush_with_guard<'tracker>(
        &mut self,
        memory: &'tracker SharedMemoryTracker,
    ) -> Result<Option<(PartitionBatch<KEY_LEN>, u64, InFlightMemoryGuard<'tracker>)>, BulkLoadError>
    {
        if self.staging.is_empty() {
            return Ok(None);
        }
        let run_index = self.allocate_run_index()?;
        let guard = InFlightMemoryGuard::new(memory, self.staging.accounted_bytes, true);
        let taken = std::mem::take(&mut self.staging);
        Ok(Some((taken, run_index, guard)))
    }

    fn allocate_run_index(&mut self) -> Result<u64, BulkLoadError> {
        let run_index = self.next_run_index;
        self.next_run_index = run_index
            .checked_add(1)
            .ok_or_else(|| BulkLoadError::invalid("sharded run count exceeds u64::MAX"))?;
        Ok(run_index)
    }

    fn record_flushed_run(&mut self, run: RunFile, elapsed: Duration) -> Result<(), BulkLoadError> {
        self.runs.push(run);
        self.initial_run_count = self
            .initial_run_count
            .checked_add(1)
            .ok_or_else(|| BulkLoadError::invalid("sharded initial run count exceeds u64::MAX"))?;
        self.initial_run_spill_duration = self.initial_run_spill_duration.saturating_add(elapsed);
        Ok(())
    }
}

struct FlushedPartitionOutput<const KEY_LEN: usize> {
    stream: SortedVariableValues<KEY_LEN>,
    record_count: u64,
    initial_run_count: u64,
    merge_pass_count: u64,
    initial_run_spill_duration: Duration,
    final_run_flush_duration: Duration,
    merge_duration: Duration,
    final_run_file_bytes: u64,
}

enum BucketAdmission<'tracker, const KEY_LEN: usize> {
    Moved(u64),
    Appended,
    FlushPartition(PartitionBatch<KEY_LEN>, u64, InFlightMemoryGuard<'tracker>),
    SpillBucket(u64),
}

#[cfg(test)]
trait ReclaimRaceHook: Send + Sync {
    fn on_before_publish(&self) {}
    fn on_after_empty_scan(&self, _in_flight_work: u64) {}
    fn on_after_flush_detach(&self) {}
    fn on_after_guard_bytes_released(&self, _active_bytes: u64, _in_flight_work: u64) {}
    fn on_after_reserve_before_guard_return(&self, _in_flight_work: u64) {}
    fn on_after_bucket_append(
        &self,
        _staging_allocated_bytes: u64,
        _bucket_allocated_bytes: u64,
        _active_bytes: u64,
    ) {
    }
}

fn default_per_shard_merge_fan_in(shards_usize: usize, total_fan_in: usize) -> usize {
    if shards_usize <= 1 {
        total_fan_in
    } else {
        total_fan_in / shards_usize
    }
}

/// Configuration bounds for creating a [`ShardedKeyPrefixSorter`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardedSortBudget {
    /// Number of disjoint two-byte prefix partitions.
    pub shard_count: NonZeroU32,
    /// Shared accounted in-memory record ceiling across all partitions.
    pub max_accounted_memory_bytes: u64,
    /// Shared temporary spill-file ceiling across all partitions.
    pub max_temporary_file_bytes: u64,
    /// Optional explicit per-shard merge fan-in ceiling (`None` divides active
    /// descriptor headroom evenly across all `shard_count` shards when merging).
    pub per_shard_merge_fan_in_cap: Option<usize>,
}

/// Concurrent key-prefix partitioned external sorter yielding disjoint ordered
/// shard streams across `[0, shard_count)`.
pub struct ShardedKeyPrefixSorter<const KEY_LEN: usize> {
    shard_count: NonZeroU32,
    per_shard_merge_fan_in_cap: Option<usize>,
    max_temporary_file_bytes: u64,
    workspaces: Vec<TempDir>,
    memory: SharedMemoryTracker,
    temporary_files: SharedTempFileBudget,
    partitions: Vec<Mutex<PartitionState<KEY_LEN>>>,
}

impl<const KEY_LEN: usize> ShardedKeyPrefixSorter<KEY_LEN> {
    /// Creates a sharded sorter over `shard_count` disjoint key-prefix ranges.
    pub fn new(
        staging_dir: &Path,
        artifact_label: &str,
        shard_count: NonZeroU32,
        max_accounted_memory_bytes: u64,
        max_temporary_file_bytes: u64,
    ) -> Result<Self, BulkLoadError> {
        Self::new_with_budget(
            staging_dir,
            artifact_label,
            ShardedSortBudget {
                shard_count,
                max_accounted_memory_bytes,
                max_temporary_file_bytes,
                per_shard_merge_fan_in_cap: None,
            },
        )
    }

    /// Creates a sharded sorter with an explicit [`ShardedSortBudget`].
    pub fn new_with_budget(
        staging_dir: &Path,
        artifact_label: &str,
        budget: ShardedSortBudget,
    ) -> Result<Self, BulkLoadError> {
        const {
            assert!(
                KEY_LEN >= 2,
                "key-prefix sharding requires keys of at least two bytes"
            );
        }
        validate_artifact_label(artifact_label)?;
        if budget.max_accounted_memory_bytes == 0 {
            return Err(BulkLoadError::invalid(
                "sharded sort accounted memory ceiling must be greater than zero",
            ));
        }
        if budget.shard_count.get() > MAX_KEY_PREFIX_SHARD_COUNT {
            return Err(BulkLoadError::invalid(
                "key-prefix shard count exceeds two-byte prefix capacity",
            ));
        }
        let shards_usize = usize::try_from(budget.shard_count.get())
            .map_err(|_| BulkLoadError::invalid("shard count exceeds platform usize::MAX"))?;
        if let Some(explicit) = budget.per_shard_merge_fan_in_cap {
            validate_merge_fan_in(explicit)?;
            let total_fan_in = effective_merge_fan_in();
            if explicit
                .checked_mul(shards_usize)
                .is_none_or(|total| total > total_fan_in)
            {
                return Err(BulkLoadError::invalid(
                    "explicit per-shard fan-in exceeds active descriptor headroom",
                ));
            }
        }
        let mut workspaces = Vec::with_capacity(shards_usize);
        let mut partitions = Vec::with_capacity(shards_usize);
        for index in 0..shards_usize {
            let prefix = format!("{artifact_label}-s{index:03}-");
            let workspace = TemporaryDirectoryBuilder::new()
                .prefix(&prefix)
                .tempdir_in(staging_dir)
                .map_err(|source| path_unavailable(staging_dir, source))?;
            workspaces.push(workspace);
            partitions.push(Mutex::new(PartitionState::default()));
        }
        Ok(Self {
            shard_count: budget.shard_count,
            per_shard_merge_fan_in_cap: budget.per_shard_merge_fan_in_cap,
            max_temporary_file_bytes: budget.max_temporary_file_bytes,
            workspaces,
            memory: SharedMemoryTracker::new(budget.max_accounted_memory_bytes, budget.shard_count),
            temporary_files: SharedTempFileBudget::new(budget.max_temporary_file_bytes),
            partitions,
        })
    }

    #[cfg(test)]
    fn set_reclaim_race_hook(&mut self, hook: std::sync::Arc<dyn ReclaimRaceHook>) {
        self.memory.hook = Some(hook);
    }

    /// Returns the number of disjoint key-prefix shards.
    pub fn shard_count(&self) -> NonZeroU32 {
        self.shard_count
    }

    /// Returns the per-shard merge fan-in reserved from active file descriptor limits.
    pub fn per_shard_merge_fan_in(&self) -> usize {
        self.per_shard_merge_fan_in_cap.unwrap_or_else(|| {
            let shards_usize = usize::try_from(self.shard_count.get()).unwrap_or(1);
            default_per_shard_merge_fan_in(shards_usize, effective_merge_fan_in())
        })
    }

    /// Maps `key` to its disjoint shard index in `[0, shard_count)`.
    pub fn partition_index(&self, key: &[u8; KEY_LEN]) -> usize {
        key_prefix_shard_index(key, self.shard_count)
    }

    /// Creates a thread-local batching writer targeting up to `expected_writers`
    /// concurrent callers with bounded global memory accounting.
    pub fn writer(&self, expected_writers: NonZeroU32) -> ShardedKeyPrefixWriter<'_, KEY_LEN> {
        let credit_chunk = compute_writer_credit_chunk(
            self.memory.max_accounted_bytes,
            self.memory.per_partition_bytes,
            expected_writers,
        );
        let shard_count = self.partitions.len();
        let mut buckets = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            buckets.push(PartitionBatch::default());
        }
        ShardedKeyPrefixWriter {
            sorter: self,
            buckets,
            credit_chunk_bytes: credit_chunk,
            unused_credit_bytes: 0,
        }
    }

    /// Inserts one key and value directly into its owning key-prefix shard.
    pub fn push(&self, key: [u8; KEY_LEN], encoded_value: &[u8]) -> Result<(), BulkLoadError> {
        let record_bytes = accounted_record_bytes::<KEY_LEN>(encoded_value.len())?;
        let reservation = self.reserve_global_memory(record_bytes)?;
        let shard_idx = self.partition_index(&key);
        if record_bytes > self.memory.per_partition_bytes {
            return self.push_oversized_single_record(shard_idx, key, encoded_value, reservation);
        }
        self.push_into_partition(shard_idx, (key, encoded_value, record_bytes), reservation)
    }

    /// Flushes remaining staged records in all shards and merges runs down to
    /// each shard's safe concurrent merge fan-in.
    pub fn finish(self) -> Result<ShardedSortedVariableValues<KEY_LEN>, BulkLoadError> {
        let shards_usize = usize::try_from(self.shard_count.get())
            .map_err(|_| BulkLoadError::invalid("shard count exceeds platform usize::MAX"))?;
        let total_fan_in = effective_merge_fan_in();
        let per_shard_fan_in = match self.per_shard_merge_fan_in_cap {
            Some(explicit) => {
                validate_merge_fan_in(explicit)?;
                if explicit
                    .checked_mul(shards_usize)
                    .is_none_or(|total| total > total_fan_in)
                {
                    return Err(BulkLoadError::invalid(
                        "explicit per-shard fan-in exceeds active descriptor headroom",
                    ));
                }
                explicit
            }
            None => default_per_shard_merge_fan_in(shards_usize, total_fan_in),
        };
        if per_shard_fan_in < MIN_MERGE_FAN_IN {
            return Err(BulkLoadError::invalid(
                "sharded sort exceeds active merge file-descriptor fan-in",
            ));
        }
        self.finish_with_per_shard_merge_fan_in(per_shard_fan_in)
    }

    /// Flushes remaining staged records in all shards and merges runs using an
    /// explicit per-shard fan-in budgeted by the caller for the merge phase.
    ///
    /// Callers coordinating multiple concurrent sorters compute `per_shard_fan_in`
    /// from [`active_merge_fan_in`](crate::active_merge_fan_in) once earlier
    /// phase run readers have closed, preserving global descriptor limits across
    /// all shards (`per_shard_fan_in * shard_count <= MAX_MERGE_FAN_IN`).
    pub fn finish_with_per_shard_merge_fan_in(
        self,
        per_shard_fan_in: usize,
    ) -> Result<ShardedSortedVariableValues<KEY_LEN>, BulkLoadError> {
        validate_merge_fan_in(per_shard_fan_in)?;
        let shards_usize = usize::try_from(self.shard_count.get())
            .map_err(|_| BulkLoadError::invalid("shard count exceeds platform usize::MAX"))?;
        if per_shard_fan_in
            .checked_mul(shards_usize)
            .is_none_or(|total| total > super::MAX_MERGE_FAN_IN)
        {
            return Err(BulkLoadError::invalid(
                "sharded sort per-shard fan-in exceeds MAX_MERGE_FAN_IN",
            ));
        }
        let max_accounted = self.memory.max_accounted_bytes;
        let max_temporary = self.max_temporary_file_bytes;
        let temporary_files = &self.temporary_files;
        let shards_input: Vec<_> = self
            .workspaces
            .into_iter()
            .zip(self.partitions.into_iter().map(Mutex::into_inner))
            .collect();
        let outcomes = finish_partition_phases(
            shards_input,
            per_shard_fan_in,
            max_accounted,
            temporary_files,
        )?;
        let peak_accounted_memory_bytes = self.memory.peak_bytes();
        let peak_temporary_file_bytes = self.temporary_files.peak_bytes();
        assemble_sharded_sorted_values(
            outcomes,
            peak_accounted_memory_bytes,
            max_accounted,
            peak_temporary_file_bytes,
            max_temporary,
        )
    }

    fn reserve_global_memory(
        &self,
        record_bytes: u64,
    ) -> Result<InFlightMemoryGuard<'_>, BulkLoadError> {
        if record_bytes > self.memory.max_accounted_bytes {
            return Err(BulkLoadError::AccountedMemoryLimit {
                limit_bytes: self.memory.max_accounted_bytes,
                required_bytes: record_bytes,
            });
        }
        if let Some(guard) = self.memory.try_reserve_in_flight(record_bytes) {
            return Ok(guard);
        }
        self.reclaim_and_reserve(record_bytes)
    }

    fn reclaim_and_reserve(
        &self,
        record_bytes: u64,
    ) -> Result<InFlightMemoryGuard<'_>, BulkLoadError> {
        loop {
            let epoch_before_scan = self.memory.transition_epoch.load(Ordering::SeqCst);
            let mut drained_any = false;
            for shard_idx in 0..self.partitions.len() {
                if self.flush_partition_if_nonempty(shard_idx)? {
                    drained_any = true;
                }
                if let Some(guard) = self.memory.try_reserve_in_flight(record_bytes) {
                    return Ok(guard);
                }
            }
            if drained_any {
                continue;
            }
            #[cfg(test)]
            if let Some(hook) = &self.memory.hook {
                hook.on_after_empty_scan(self.memory.in_flight_work.load(Ordering::SeqCst));
            }
            if self.memory.transition_epoch.load(Ordering::SeqCst) != epoch_before_scan {
                continue;
            }
            let _waiter = WaiterGuard::new(&self.memory.waiters);
            let mut wait_guard = self.memory.wait_lock.lock();
            if let Some(guard) = self
                .memory
                .try_reserve_in_flight_locked(record_bytes, &wait_guard)
            {
                return Ok(guard);
            }
            if self.memory.transition_epoch.load(Ordering::SeqCst) != epoch_before_scan {
                continue;
            }
            if self.memory.in_flight_work.load(Ordering::SeqCst) == 0 {
                if self.memory.transition_epoch.load(Ordering::SeqCst) != epoch_before_scan {
                    continue;
                }
                return Err(BulkLoadError::AccountedMemoryLimit {
                    limit_bytes: self.memory.max_accounted_bytes,
                    required_bytes: record_bytes,
                });
            }
            self.memory.reclaimed.wait(&mut wait_guard);
        }
    }

    fn push_oversized_single_record(
        &self,
        shard_idx: usize,
        key: [u8; KEY_LEN],
        encoded_value: &[u8],
        _reservation: InFlightMemoryGuard<'_>,
    ) -> Result<(), BulkLoadError> {
        self.flush_partition_if_nonempty(shard_idx)?;
        let run_index = {
            let mut partition = self.partitions[shard_idx].lock();
            partition.allocate_run_index()?
        };
        let spill_started = Instant::now();
        let workspace_path = self.workspaces[shard_idx].path();
        let run = write_single_record_run::<KEY_LEN>(
            workspace_path,
            run_index,
            key,
            encoded_value,
            &self.temporary_files,
        )
        .map_err(BulkLoadError::temporary_file_limit_before_finish)?;
        let elapsed = spill_started.elapsed();
        let mut partition = self.partitions[shard_idx].lock();
        partition.record_count = partition
            .record_count
            .checked_add(1)
            .ok_or_else(|| BulkLoadError::invalid("sharded record count exceeds u64::MAX"))?;
        partition.record_flushed_run(run, elapsed)?;
        drop(partition);
        Ok(())
    }

    fn push_into_partition(
        &self,
        shard_idx: usize,
        record: ([u8; KEY_LEN], &[u8], u64),
        mut reservation: InFlightMemoryGuard<'_>,
    ) -> Result<(), BulkLoadError> {
        let (key, encoded_value, record_bytes) = record;
        #[cfg(test)]
        if let Some(hook) = &self.memory.hook {
            hook.on_before_publish();
        }
        loop {
            let pending = {
                let mut partition = self.partitions[shard_idx].lock();
                let min_needed = partition.staging.min_growth_bytes(1, encoded_value.len())?;
                if partition.staging.accounted_bytes.saturating_add(min_needed)
                    <= self.memory.per_partition_bytes
                {
                    let partition_headroom = self.memory.per_partition_bytes.saturating_sub(
                        partition.staging.accounted_bytes.saturating_add(min_needed),
                    );
                    let desired_slack = partition
                        .staging
                        .desired_growth_slack(1, encoded_value.len())
                        .min(partition_headroom);
                    let excess_reserved = record_bytes.saturating_sub(min_needed);
                    if desired_slack > excess_reserved {
                        let extra = self
                            .memory
                            .try_reserve_up_to(desired_slack.saturating_sub(excess_reserved));
                        reservation.add_bytes(extra);
                    }
                    let max_growth = min_needed
                        .saturating_add(desired_slack)
                        .min(reservation.bytes);
                    let mut charged = 0_u64;
                    let push_outcome = partition.staging.push_record_within(
                        key,
                        encoded_value,
                        max_growth,
                        &mut charged,
                    );
                    if let Err(error) = push_outcome {
                        drop(partition);
                        reservation.consume_bytes(charged);
                        return Err(error);
                    }
                    partition.record_count =
                        partition.record_count.checked_add(1).ok_or_else(|| {
                            BulkLoadError::invalid("sharded record count exceeds u64::MAX")
                        })?;
                    drop(partition);
                    self.memory.record_transition();
                    reservation.commit_and_release_excess(charged);
                    return Ok(());
                }
                partition.take_pending_flush_with_guard(&self.memory)?
            };
            if let Some((batch, run_index, flush_guard)) = pending {
                self.flush_taken_batch_with_guard(shard_idx, batch, run_index, flush_guard)?;
            }
        }
    }

    #[cfg(test)]
    fn record_bucket_append_hook(
        &self,
        staging: &PartitionBatch<KEY_LEN>,
        bucket: &PartitionBatch<KEY_LEN>,
    ) -> Result<(), BulkLoadError> {
        if let Some(hook) = &self.memory.hook {
            let staging_alloc = staging.allocated_bytes()?;
            let bucket_alloc = bucket.allocated_bytes()?;
            hook.on_after_bucket_append(
                staging_alloc,
                bucket_alloc,
                self.memory.active_bytes.load(Ordering::SeqCst),
            );
        }
        Ok(())
    }

    fn try_reserve_and_append_bucket(
        &self,
        partition: &mut PartitionState<KEY_LEN>,
        bucket: &mut PartitionBatch<KEY_LEN>,
        min_needed: u64,
    ) -> Result<bool, BulkLoadError> {
        if min_needed == 0 {
            let mut charged = 0_u64;
            partition
                .staging
                .append_batch_within(bucket, 0, &mut charged)?;
            #[cfg(test)]
            self.record_bucket_append_hook(&partition.staging, bucket)?;
            return Ok(true);
        }
        // Reserve destination growth separately while the source bucket's
        // reservation keeps covering `bucket` until `drop(bucket)`.
        if !self.memory.try_reserve_bytes(min_needed) {
            return Ok(false);
        }
        let mut dest_guard = InFlightMemoryGuard::new(&self.memory, min_needed, false);
        let partition_headroom = self
            .memory
            .per_partition_bytes
            .saturating_sub(partition.staging.accounted_bytes.saturating_add(min_needed));
        let desired_slack = partition
            .staging
            .desired_growth_slack(bucket.staged_records.len(), bucket.encoded_values.len())
            .min(partition_headroom);
        if desired_slack > 0 {
            let extra = self.memory.try_reserve_up_to(desired_slack);
            dest_guard.add_bytes(extra);
        }
        let mut charged = 0_u64;
        if let Err(error) =
            partition
                .staging
                .append_batch_within(bucket, dest_guard.bytes, &mut charged)
        {
            dest_guard.consume_bytes(charged);
            return Err(error);
        }
        dest_guard.commit_and_release_excess(charged);
        #[cfg(test)]
        self.record_bucket_append_hook(&partition.staging, bucket)?;
        Ok(true)
    }

    fn try_admit_bucket_to_partition(
        &self,
        shard_idx: usize,
        bucket: &mut PartitionBatch<KEY_LEN>,
    ) -> Result<BucketAdmission<'_, KEY_LEN>, BulkLoadError> {
        let batch_records = u64::try_from(bucket.staged_records.len())
            .map_err(|_| BulkLoadError::invalid("bucket record count exceeds u64::MAX"))?;
        let mut partition = self.partitions[shard_idx].lock();
        if partition.staging.is_empty() && bucket.accounted_bytes <= self.memory.per_partition_bytes
        {
            let moved = bucket.accounted_bytes;
            partition.staging = std::mem::take(bucket);
            partition.record_count = partition
                .record_count
                .checked_add(batch_records)
                .ok_or_else(|| BulkLoadError::invalid("sharded record count exceeds u64::MAX"))?;
            drop(partition);
            return Ok(BucketAdmission::Moved(moved));
        }
        let min_needed = partition
            .staging
            .min_growth_bytes(bucket.staged_records.len(), bucket.encoded_values.len())?;
        if partition.staging.accounted_bytes.saturating_add(min_needed)
            <= self.memory.per_partition_bytes
            && self.try_reserve_and_append_bucket(&mut partition, bucket, min_needed)?
        {
            partition.record_count = partition
                .record_count
                .checked_add(batch_records)
                .ok_or_else(|| BulkLoadError::invalid("sharded record count exceeds u64::MAX"))?;
            drop(partition);
            return Ok(BucketAdmission::Appended);
        }
        if let Some((batch, run_index, flush_guard)) =
            partition.take_pending_flush_with_guard(&self.memory)?
        {
            drop(partition);
            return Ok(BucketAdmission::FlushPartition(
                batch,
                run_index,
                flush_guard,
            ));
        }
        let run_index = partition.allocate_run_index()?;
        partition.record_count = partition
            .record_count
            .checked_add(batch_records)
            .ok_or_else(|| BulkLoadError::invalid("sharded record count exceeds u64::MAX"))?;
        drop(partition);
        Ok(BucketAdmission::SpillBucket(run_index))
    }

    fn push_preaccounted_bucket(
        &self,
        shard_idx: usize,
        mut bucket: PartitionBatch<KEY_LEN>,
    ) -> Result<(), BulkLoadError> {
        if bucket.is_empty() {
            return Ok(());
        }
        let reservation = InFlightMemoryGuard::new(&self.memory, bucket.accounted_bytes, true);
        loop {
            match self.try_admit_bucket_to_partition(shard_idx, &mut bucket)? {
                BucketAdmission::Moved(moved) => {
                    drop(bucket);
                    self.memory.record_transition();
                    reservation.commit_and_release_excess(moved);
                    return Ok(());
                }
                BucketAdmission::Appended => {
                    drop(bucket);
                    self.memory.record_transition();
                    drop(reservation);
                    return Ok(());
                }
                BucketAdmission::FlushPartition(batch, run_index, flush_guard) => {
                    self.flush_taken_batch_with_guard(shard_idx, batch, run_index, flush_guard)?;
                }
                BucketAdmission::SpillBucket(run_index) => {
                    return self.flush_taken_batch_with_guard(
                        shard_idx,
                        bucket,
                        run_index,
                        reservation,
                    );
                }
            }
        }
    }

    fn flush_partition_if_nonempty(&self, shard_idx: usize) -> Result<bool, BulkLoadError> {
        let pending = {
            let mut partition = self.partitions[shard_idx].lock();
            partition.take_pending_flush_with_guard(&self.memory)?
        };
        let Some((batch, run_index, flush_guard)) = pending else {
            return Ok(false);
        };
        #[cfg(test)]
        if let Some(hook) = &self.memory.hook {
            hook.on_after_flush_detach();
        }
        self.flush_taken_batch_with_guard(shard_idx, batch, run_index, flush_guard)?;
        Ok(true)
    }

    fn flush_taken_batch_with_guard(
        &self,
        shard_idx: usize,
        batch: PartitionBatch<KEY_LEN>,
        run_index: u64,
        _flush_guard: InFlightMemoryGuard<'_>,
    ) -> Result<(), BulkLoadError> {
        let spill_started = Instant::now();
        let workspace_path = self.workspaces[shard_idx].path();
        let run = write_batch_to_run(workspace_path, run_index, batch, &self.temporary_files)
            .map_err(BulkLoadError::temporary_file_limit_before_finish)?;
        let elapsed = spill_started.elapsed();
        let mut partition = self.partitions[shard_idx].lock();
        partition.record_flushed_run(run, elapsed)
    }
}

/// Thread-local batching writer attached to a [`ShardedKeyPrefixSorter`].
pub struct ShardedKeyPrefixWriter<'sorter, const KEY_LEN: usize> {
    sorter: &'sorter ShardedKeyPrefixSorter<KEY_LEN>,
    buckets: Vec<PartitionBatch<KEY_LEN>>,
    credit_chunk_bytes: u64,
    unused_credit_bytes: u64,
}

impl<const KEY_LEN: usize> ShardedKeyPrefixWriter<'_, KEY_LEN> {
    /// Pushes one record into its key-prefix partition, buffering small records
    /// under pre-reserved global memory credit when enabled.
    pub fn push(&mut self, key: [u8; KEY_LEN], encoded_value: &[u8]) -> Result<(), BulkLoadError> {
        let record_bytes = accounted_record_bytes::<KEY_LEN>(encoded_value.len())?;
        if self.credit_chunk_bytes == 0 || record_bytes > self.credit_chunk_bytes / 2 {
            // Release all retained local buckets and unused credit before a direct
            // sorter push so large records never hit a spurious AccountedMemoryLimit.
            self.flush_all_buckets()?;
            return self.sorter.push(key, encoded_value);
        }
        let shard_idx = self.sorter.partition_index(&key);
        let min_needed = self.buckets[shard_idx].min_growth_bytes(1, encoded_value.len())?;
        if min_needed > self.unused_credit_bytes {
            self.flush_all_buckets()?;
            self.refill_credit(min_needed.max(record_bytes));
            let refreshed_needed =
                self.buckets[shard_idx].min_growth_bytes(1, encoded_value.len())?;
            if refreshed_needed > self.unused_credit_bytes {
                return self.sorter.push(key, encoded_value);
            }
        }
        let mut charged = 0_u64;
        let stage_outcome = self.buckets[shard_idx].push_record_within(
            key,
            encoded_value,
            self.unused_credit_bytes,
            &mut charged,
        );
        self.unused_credit_bytes = self.unused_credit_bytes.saturating_sub(charged);
        stage_outcome
    }

    /// Flushes every locally buffered record to its owning sorter partition and
    /// releases remaining memory credit.
    pub fn flush(mut self) -> Result<(), BulkLoadError> {
        self.flush_all_buckets()
    }

    fn refill_credit(&mut self, needed_bytes: u64) {
        let request = self.credit_chunk_bytes.max(needed_bytes);
        if self.sorter.memory.try_reserve_bytes(request) {
            self.unused_credit_bytes = self.unused_credit_bytes.saturating_add(request);
            return;
        }
        if self.unused_credit_bytes > 0 {
            self.sorter.memory.release_bytes(self.unused_credit_bytes);
            self.unused_credit_bytes = 0;
        }
    }

    fn flush_all_buckets(&mut self) -> Result<(), BulkLoadError> {
        for idx in 0..self.buckets.len() {
            let taken = std::mem::take(&mut self.buckets[idx]);
            if !taken.is_empty() {
                self.sorter.push_preaccounted_bucket(idx, taken)?;
            }
        }
        if self.unused_credit_bytes > 0 {
            let released = self.unused_credit_bytes;
            self.unused_credit_bytes = 0;
            self.sorter.memory.release_bytes(released);
        }
        Ok(())
    }
}

impl<const KEY_LEN: usize> Drop for ShardedKeyPrefixWriter<'_, KEY_LEN> {
    fn drop(&mut self) {
        let buffered_bytes = self.buckets.iter().fold(0_u64, |acc, bucket| {
            acc.saturating_add(bucket.accounted_bytes)
        });
        let remaining = buffered_bytes.saturating_add(self.unused_credit_bytes);
        self.buckets.clear();
        self.unused_credit_bytes = 0;
        if remaining > 0 {
            self.sorter.memory.release_bytes(remaining);
        }
    }
}

fn compute_writer_credit_chunk(
    max_accounted_bytes: u64,
    per_partition_bytes: u64,
    expected_writers: NonZeroU32,
) -> u64 {
    if max_accounted_bytes < WRITER_CREDIT_MIN_MEMORY_BYTES {
        return 0;
    }
    let allowance = max_accounted_bytes / 16 / u64::from(expected_writers.get());
    let chunk = allowance
        .min(WRITER_CREDIT_CHUNK_BYTES)
        .min(per_partition_bytes / 2);
    if chunk >= 1024 { chunk } else { 0 }
}

fn write_batch_to_run<const KEY_LEN: usize>(
    workspace_path: &Path,
    run_index: u64,
    mut batch: PartitionBatch<KEY_LEN>,
    temporary_files: &SharedTempFileBudget,
) -> Result<RunFile, BulkLoadError> {
    batch.staged_records.sort_unstable_by_key(|left| left.key);
    let mut run_file_bytes = 0_u64;
    for record in &batch.staged_records {
        let value_len = record.value_end.saturating_sub(record.value_start);
        run_file_bytes = run_file_bytes
            .checked_add(encoded_run_record_bytes::<KEY_LEN>(value_len)?)
            .ok_or_else(|| BulkLoadError::invalid("sharded run bytes exceed u64::MAX"))?;
    }
    temporary_files.reserve_initial_run(run_file_bytes)?;
    let path = workspace_path.join(format!("run-{run_index:08}.bin"));
    if let Err(source) = write_sorted_records_file(&path, &batch) {
        let _ignored = remove_temporary_file(&path);
        let _ignored_release = temporary_files.release_initial_run(run_file_bytes);
        return Err(source);
    }
    verify_run_file_size(&path, run_file_bytes, temporary_files, true)?;
    Ok(RunFile {
        path,
        file_bytes: run_file_bytes,
    })
}

fn write_sorted_records_file<const KEY_LEN: usize>(
    path: &Path,
    batch: &PartitionBatch<KEY_LEN>,
) -> Result<(), BulkLoadError> {
    let file = File::create(path).map_err(|source| path_unavailable(path, source))?;
    let mut writer = BufWriter::new(file);
    for record in &batch.staged_records {
        let encoded_value = batch
            .encoded_values
            .get(record.value_start..record.value_end)
            .ok_or_else(|| BulkLoadError::invalid("staged sharded value range unavailable"))?;
        let value_len = u64::try_from(encoded_value.len())
            .map_err(|_| BulkLoadError::invalid("encoded value length exceeds u64::MAX"))?;
        writer
            .write_all(&record.key)
            .and_then(|()| writer.write_all(&value_len.to_be_bytes()))
            .and_then(|()| writer.write_all(encoded_value))
            .map_err(|source| path_unavailable(path, source))?;
    }
    writer
        .flush()
        .map_err(|source| path_unavailable(path, source))
}

fn write_single_record_run<const KEY_LEN: usize>(
    workspace_path: &Path,
    run_index: u64,
    key: [u8; KEY_LEN],
    encoded_value: &[u8],
    temporary_files: &SharedTempFileBudget,
) -> Result<RunFile, BulkLoadError> {
    let run_file_bytes = encoded_run_record_bytes::<KEY_LEN>(encoded_value.len())?;
    temporary_files.reserve_initial_run(run_file_bytes)?;
    let path = workspace_path.join(format!("run-{run_index:08}.bin"));
    let write_outcome = (|| {
        let value_len = u64::try_from(encoded_value.len())
            .map_err(|_| BulkLoadError::invalid("encoded value length exceeds u64::MAX"))?;
        let file = File::create(&path).map_err(|source| path_unavailable(&path, source))?;
        let mut writer = BufWriter::new(file);
        writer
            .write_all(&key)
            .and_then(|()| writer.write_all(&value_len.to_be_bytes()))
            .and_then(|()| writer.write_all(encoded_value))
            .and_then(|()| writer.flush())
            .map_err(|source| path_unavailable(&path, source))
    })();
    if let Err(source) = write_outcome {
        let _ignored = remove_temporary_file(&path);
        let _ignored_release = temporary_files.release_initial_run(run_file_bytes);
        return Err(source);
    }
    verify_run_file_size(&path, run_file_bytes, temporary_files, true)?;
    Ok(RunFile {
        path,
        file_bytes: run_file_bytes,
    })
}

fn verify_run_file_size(
    path: &Path,
    expected_bytes: u64,
    temporary_files: &SharedTempFileBudget,
    initial_run: bool,
) -> Result<(), BulkLoadError> {
    let release = |bytes| {
        if initial_run {
            let _ignored = temporary_files.release_initial_run(bytes);
        } else {
            let _ignored = temporary_files.release(bytes);
        }
    };
    let observed = match fs::metadata(path) {
        Ok(metadata) => metadata.len(),
        Err(source) => {
            let error = path_unavailable(path, source);
            let _ignored = remove_temporary_file(path);
            release(expected_bytes);
            return Err(error);
        }
    };
    if observed != expected_bytes {
        let _ignored = remove_temporary_file(path);
        release(expected_bytes);
        return Err(BulkLoadError::invalid(
            "sharded run file size differs from its admitted bytes",
        ));
    }
    Ok(())
}

#[cfg(test)]
thread_local! {
    static REFUSED_SORT_PHASE_THREAD: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
struct RefusedSortThreadGuard;

#[cfg(test)]
impl Drop for RefusedSortThreadGuard {
    fn drop(&mut self) {
        REFUSED_SORT_PHASE_THREAD.set(None);
    }
}

#[cfg(test)]
fn arm_refused_sort_phase_thread(skip_spawns: usize) -> RefusedSortThreadGuard {
    REFUSED_SORT_PHASE_THREAD.set(Some(skip_spawns));
    RefusedSortThreadGuard
}

fn spawn_partition_worker<'scope, 'env, F, T>(
    scope: &'scope thread::Scope<'scope, 'env>,
    worker_index: usize,
    task: F,
) -> std::io::Result<thread::ScopedJoinHandle<'scope, T>>
where
    F: FnOnce() -> T + Send + 'scope,
    T: Send + 'scope,
{
    #[cfg(test)]
    if let Some(remaining) = REFUSED_SORT_PHASE_THREAD.get() {
        if remaining == 0 {
            REFUSED_SORT_PHASE_THREAD.set(None);
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "injected sort worker thread start refusal",
            ));
        }
        REFUSED_SORT_PHASE_THREAD.set(Some(remaining - 1));
    }
    thread::Builder::new()
        .name(format!("zinder-sharded-sort-{worker_index}"))
        .spawn_scoped(scope, task)
}

fn run_scoped_partition_phase<Input, Output, Task>(
    inputs: Vec<Input>,
    task: Task,
) -> Result<Vec<Output>, BulkLoadError>
where
    Input: Send,
    Output: Send,
    Task: Fn(Input) -> Result<Output, BulkLoadError> + Sync,
{
    let count = inputs.len();
    let slots = inputs
        .into_iter()
        .map(|entry| Mutex::new(Some(entry)))
        .collect::<Vec<_>>();
    let slot_outcomes = thread::scope(|scope| {
        let task_ref = &task;
        let mut handles = Vec::with_capacity(count);
        let mut outcomes: Vec<Option<Result<Output, BulkLoadError>>> =
            (0..count).map(|_| None).collect();
        for (index, slot) in slots.iter().enumerate() {
            if let Ok(handle) = spawn_partition_worker(scope, index, move || {
                let taken = slot.lock().take();
                taken.map_or_else(
                    || Err(BulkLoadError::invalid("sharded sort slot missing input")),
                    task_ref,
                )
            }) {
                handles.push((index, handle));
            } else {
                let taken = slot.lock().take();
                if let Some(entry) = taken {
                    outcomes[index] = Some(task_ref(entry));
                }
            }
        }
        for (index, handle) in handles {
            let outcome = handle
                .join()
                .unwrap_or_else(|_| Err(BulkLoadError::invalid("sharded sort worker panicked")));
            outcomes[index] = Some(outcome);
        }
        outcomes
    });
    let mut collected = Vec::with_capacity(count);
    for slot_outcome in slot_outcomes {
        let outcome = slot_outcome
            .ok_or_else(|| BulkLoadError::invalid("sharded sort worker outcome missing"))?;
        collected.push(outcome?);
    }
    Ok(collected)
}

fn finish_partition_phases<const KEY_LEN: usize>(
    shards_input: Vec<(TempDir, PartitionState<KEY_LEN>)>,
    per_shard_fan_in: usize,
    max_accounted: u64,
    temporary_files: &SharedTempFileBudget,
) -> Result<Vec<FlushedPartitionOutput<KEY_LEN>>, BulkLoadError> {
    if shards_input.len() == 1 {
        return shards_input
            .into_iter()
            .map(|(workspace, mut partition)| {
                let tail_flush = flush_tail_batch(&workspace, &mut partition, temporary_files)?;
                merge_and_open_partition(
                    workspace,
                    partition,
                    tail_flush,
                    (per_shard_fan_in, max_accounted, temporary_files),
                )
            })
            .collect();
    }
    let flushed_shards = run_scoped_partition_phase(shards_input, |(workspace, mut partition)| {
        let tail_flush = flush_tail_batch(&workspace, &mut partition, temporary_files)?;
        Ok((workspace, partition, tail_flush))
    })?;
    run_scoped_partition_phase(flushed_shards, |(workspace, partition, tail_flush)| {
        merge_and_open_partition(
            workspace,
            partition,
            tail_flush,
            (per_shard_fan_in, max_accounted, temporary_files),
        )
    })
}

fn merge_and_open_partition<const KEY_LEN: usize>(
    workspace: TempDir,
    partition: PartitionState<KEY_LEN>,
    final_run_flush_duration: Duration,
    budget: (usize, u64, &SharedTempFileBudget),
) -> Result<FlushedPartitionOutput<KEY_LEN>, BulkLoadError> {
    let (per_shard_fan_in, max_accounted, temporary_files) = budget;
    let (runs, merge_pass_count, merge_duration) = merge_partition_runs::<KEY_LEN>(
        workspace.path(),
        temporary_files,
        partition.runs,
        per_shard_fan_in,
    )?;
    let final_run_file_bytes = runs.iter().try_fold(0_u64, |sum, run| {
        sum.checked_add(run.file_bytes)
            .ok_or_else(|| BulkLoadError::invalid("sharded final run bytes exceed u64::MAX"))
    })?;
    let source = SortedRunSource::<KEY_LEN>::open(runs, per_shard_fan_in)?;
    let max_temporary = temporary_files.state.lock().max;
    let shard_evidence = VariableValueSortEvidence {
        record_count: partition.record_count,
        initial_run_count: partition.initial_run_count,
        merge_pass_count,
        peak_accounted_memory_bytes: 0,
        max_accounted_memory_bytes: max_accounted,
        peak_temporary_file_bytes: 0,
        max_temporary_file_bytes: max_temporary,
        final_run_file_bytes,
    };
    let shard_durations = VariableValueSortDurations {
        initial_run_spill: partition.initial_run_spill_duration,
        final_run_flush: final_run_flush_duration,
        merge: merge_duration,
    };
    let stream = SortedVariableValues {
        source,
        evidence: shard_evidence,
        durations: shard_durations,
        max_accounted_memory_bytes: max_accounted,
        previous_key: None,
        emitted_record_count: 0,
        workspace: Some(workspace),
        finished: false,
    };
    Ok(FlushedPartitionOutput {
        stream,
        record_count: partition.record_count,
        initial_run_count: partition.initial_run_count,
        merge_pass_count,
        initial_run_spill_duration: partition.initial_run_spill_duration,
        final_run_flush_duration,
        merge_duration,
        final_run_file_bytes,
    })
}

fn flush_tail_batch<const KEY_LEN: usize>(
    workspace: &TempDir,
    partition: &mut PartitionState<KEY_LEN>,
    temporary_files: &SharedTempFileBudget,
) -> Result<Duration, BulkLoadError> {
    let flush_started = Instant::now();
    if partition.staging.is_empty() {
        return Ok(Duration::ZERO);
    }
    let run_index = partition.allocate_run_index()?;
    let batch = std::mem::take(&mut partition.staging);
    let run = write_batch_to_run(workspace.path(), run_index, batch, temporary_files)?;
    partition.runs.push(run);
    partition.initial_run_count = partition
        .initial_run_count
        .checked_add(1)
        .ok_or_else(|| BulkLoadError::invalid("sharded initial run count exceeds u64::MAX"))?;
    Ok(flush_started.elapsed())
}

fn merge_partition_runs<const KEY_LEN: usize>(
    workspace_path: &Path,
    temporary_files: &SharedTempFileBudget,
    mut runs: Vec<RunFile>,
    per_shard_fan_in: usize,
) -> Result<(Vec<RunFile>, u64, Duration), BulkLoadError> {
    let mut merge_pass_count = 0_u64;
    let merge_started = Instant::now();
    while runs.len() > per_shard_fan_in {
        runs = sharded_merge_pass::<KEY_LEN>(
            workspace_path,
            temporary_files,
            runs,
            merge_pass_count,
            per_shard_fan_in,
        )?;
        merge_pass_count = merge_pass_count
            .checked_add(1)
            .ok_or_else(|| BulkLoadError::invalid("sharded merge pass count exceeds u64::MAX"))?;
    }
    let merge_duration = if merge_pass_count > 0 {
        merge_started.elapsed()
    } else {
        Duration::ZERO
    };
    Ok((runs, merge_pass_count, merge_duration))
}

fn sharded_merge_pass<const KEY_LEN: usize>(
    workspace_path: &Path,
    temporary_files: &SharedTempFileBudget,
    runs: Vec<RunFile>,
    pass_count: u64,
    fan_in: usize,
) -> Result<Vec<RunFile>, BulkLoadError> {
    validate_merge_fan_in(fan_in)?;
    let mut pending = runs.into_iter();
    let mut merged = Vec::new();
    let mut group_index = 0_u64;
    loop {
        let group: Vec<_> = pending.by_ref().take(fan_in).collect();
        if group.is_empty() {
            break;
        }
        if group.len() == 1 {
            merged.extend(group);
        } else {
            let output_path =
                workspace_path.join(format!("merge-{pass_count:04}-{group_index:08}.bin"));
            merged.push(sharded_merge_group::<KEY_LEN>(
                temporary_files,
                group,
                output_path,
                fan_in,
            )?);
        }
        group_index = group_index
            .checked_add(1)
            .ok_or_else(|| BulkLoadError::invalid("sharded merge group count exceeds u64::MAX"))?;
    }
    Ok(merged)
}

fn sharded_merge_group<const KEY_LEN: usize>(
    temporary_files: &SharedTempFileBudget,
    input_runs: Vec<RunFile>,
    output_path: PathBuf,
    merge_fan_in: usize,
) -> Result<RunFile, BulkLoadError> {
    let input_file_bytes = input_runs.iter().try_fold(0_u64, |sum, run| {
        sum.checked_add(run.file_bytes)
            .ok_or_else(|| BulkLoadError::invalid("merged sharded run bytes exceed u64::MAX"))
    })?;
    temporary_files.reserve(input_file_bytes)?;
    if let Err(source) = merge_run_group::<KEY_LEN>(&input_runs, &output_path, merge_fan_in) {
        let _ignored = remove_temporary_file(&output_path);
        let _ignored_release = temporary_files.release(input_file_bytes);
        return Err(source);
    }
    verify_run_file_size(&output_path, input_file_bytes, temporary_files, false)?;
    for input_run in &input_runs {
        remove_temporary_file(&input_run.path)?;
    }
    drop(input_runs);
    temporary_files.release(input_file_bytes)?;
    Ok(RunFile {
        path: output_path,
        file_bytes: input_file_bytes,
    })
}

fn assemble_sharded_sorted_values<const KEY_LEN: usize>(
    outcomes: Vec<FlushedPartitionOutput<KEY_LEN>>,
    peak_accounted_memory_bytes: u64,
    max_accounted_memory_bytes: u64,
    peak_temporary_file_bytes: u64,
    max_temporary_file_bytes: u64,
) -> Result<ShardedSortedVariableValues<KEY_LEN>, BulkLoadError> {
    let mut shards = Vec::with_capacity(outcomes.len());
    let mut record_count = 0_u64;
    let mut initial_run_count = 0_u64;
    let mut merge_pass_count = 0_u64;
    let mut final_run_file_bytes = 0_u64;
    let mut initial_run_spill = Duration::ZERO;
    let mut final_run_flush = Duration::ZERO;
    let mut merge = Duration::ZERO;
    for outcome in outcomes {
        shards.push(outcome.stream);
        record_count = record_count
            .checked_add(outcome.record_count)
            .ok_or_else(|| BulkLoadError::invalid("sharded total record count exceeds u64::MAX"))?;
        initial_run_count = initial_run_count
            .checked_add(outcome.initial_run_count)
            .ok_or_else(|| BulkLoadError::invalid("sharded total initial runs exceed u64::MAX"))?;
        merge_pass_count = merge_pass_count
            .checked_add(outcome.merge_pass_count)
            .ok_or_else(|| BulkLoadError::invalid("sharded total merge passes exceed u64::MAX"))?;
        final_run_file_bytes = final_run_file_bytes
            .checked_add(outcome.final_run_file_bytes)
            .ok_or_else(|| {
                BulkLoadError::invalid("sharded total final run bytes exceed u64::MAX")
            })?;
        initial_run_spill = initial_run_spill.max(outcome.initial_run_spill_duration);
        let flush_and_merge = outcome
            .final_run_flush_duration
            .saturating_add(outcome.merge_duration);
        if flush_and_merge > final_run_flush.saturating_add(merge) {
            final_run_flush = outcome.final_run_flush_duration;
            merge = outcome.merge_duration;
        }
    }
    Ok(ShardedSortedVariableValues {
        shards,
        evidence: VariableValueSortEvidence {
            record_count,
            initial_run_count,
            merge_pass_count,
            peak_accounted_memory_bytes,
            max_accounted_memory_bytes,
            peak_temporary_file_bytes,
            max_temporary_file_bytes,
            final_run_file_bytes,
        },
        durations: VariableValueSortDurations {
            initial_run_spill,
            final_run_flush,
            merge,
        },
    })
}

#[cfg(test)]
pub(super) fn one_record_memory_limit_bytes<const KEY_LEN: usize>(value_len: usize) -> u64 {
    u64::try_from(size_of::<StagedVariableValue<KEY_LEN>>().saturating_add(value_len))
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom};

    use super::*;
    use tempfile::TempDir;

    fn one_record_memory_limit<const KEY_LEN: usize>(value_len: usize) -> u64 {
        one_record_memory_limit_bytes::<KEY_LEN>(value_len)
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
        let active_fan_in = effective_merge_fan_in();
        let run_count = active_fan_in
            .checked_add(1)
            .ok_or("active fan-in plus one overflows usize")?;
        let run_limit = u16::try_from(run_count)?;
        let run_count_u64 = u64::try_from(run_count)?;
        let active_fan_in_u64 = u64::try_from(active_fan_in)?;
        let one_run_bytes = encoded_run_record_bytes::<2>(1)?;
        let max_temporary_bytes = one_run_bytes.saturating_mul(run_count_u64.saturating_mul(2));
        let memory_limit = one_record_memory_limit::<2>(1);
        let mut staged_sort = VariableValueSorter::<2>::new(
            temporary.path(),
            "multiple-groups",
            memory_limit,
            max_temporary_bytes.max(50_000),
        )?;
        for number in (0_u16..run_limit).rev() {
            staged_sort.push(number.to_be_bytes(), &[number.to_be_bytes()[1]])?;
        }
        let mut sorted_records = staged_sort.finish_with_fan_in(active_fan_in)?;
        let evidence = sorted_records.evidence();
        let mut observed_keys = Vec::new();
        while let Some(record) = sorted_records.next_record()? {
            observed_keys.push(record.key);
        }
        let expected_keys = (0_u16..run_limit).map(u16::to_be_bytes).collect::<Vec<_>>();

        assert_eq!(observed_keys, expected_keys);
        assert_eq!(evidence.initial_run_count, run_count_u64);
        // One pass merges `active_fan_in` runs into one; the reader streams the last two.
        assert_eq!(evidence.merge_pass_count, 1);
        assert!(sorted_records.durations().merge > Duration::ZERO);
        assert_eq!(evidence.final_run_file_bytes, one_run_bytes * run_count_u64);
        let expected_peak_runs = active_fan_in_u64.saturating_mul(2).saturating_add(1);
        assert_eq!(
            evidence.peak_temporary_file_bytes,
            one_run_bytes * expected_peak_runs
        );
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

    /// Drains `sorted_records` and returns the first error, if any.
    fn drain_to_end<const KEY_LEN: usize>(
        sorted_records: &mut SortedVariableValues<KEY_LEN>,
    ) -> Result<usize, BulkLoadError> {
        let mut records = 0;
        while sorted_records.next_record()?.is_some() {
            records += 1;
        }
        Ok(records)
    }

    /// One well-framed record: key, big-endian value length, value.
    fn framed_record(key: u8, encoded_value: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut record = vec![key];
        record.extend_from_slice(&u64::try_from(encoded_value.len())?.to_be_bytes());
        record.extend_from_slice(encoded_value);
        Ok(record)
    }

    #[test]
    fn a_resized_record_in_a_merged_run_fails_the_drain() -> Result<(), Box<dyn std::error::Error>>
    {
        // The spilled run holds key 1 with a two-byte value. Each forgery keeps
        // the key and the record count but changes the run's length.
        for forged_value in [b"a".as_slice(), b"abc".as_slice()] {
            let temporary = TempDir::new()?;
            let mut staged_sort = VariableValueSorter::<1>::new(
                temporary.path(),
                "resized",
                one_record_memory_limit::<1>(3),
                1_024,
            )?;
            staged_sort.push([1], b"ab")?;
            staged_sort.push([2], b"cde")?;
            let first_run = staged_sort
                .runs
                .first()
                .map(|run| run.path.clone())
                .ok_or("the first record must have spilled")?;
            fs::write(&first_run, framed_record(1, forged_value)?)?;
            let mut sorted_records = staged_sort.finish()?;
            assert_eq!(sorted_records.run_paths().len(), 2);

            assert!(
                matches!(
                    drain_to_end(&mut sorted_records),
                    Err(BulkLoadError::InvalidInput { .. })
                ),
                "a {}-byte forged value was accepted",
                forged_value.len()
            );
        }
        Ok(())
    }

    /// Pushes one-byte-value records with keys `keys`, one record per run.
    fn one_record_runs(
        directory: &Path,
        label: &'static str,
        keys: impl IntoIterator<Item = u8>,
    ) -> Result<SortedVariableValues<1>, BulkLoadError> {
        let mut staged_sort = VariableValueSorter::<1>::new(
            directory,
            label,
            one_record_memory_limit::<1>(1),
            4_096,
        )?;
        for key in keys {
            staged_sort.push([key], &[key])?;
        }
        staged_sort.finish()
    }

    fn workspace_path<const KEY_LEN: usize>(
        sorted_records: &SortedVariableValues<KEY_LEN>,
    ) -> Result<PathBuf, Box<dyn std::error::Error>> {
        Ok(sorted_records
            .workspace
            .as_ref()
            .ok_or("an unread sort owns its workspace")?
            .path()
            .to_path_buf())
    }

    #[test]
    fn an_empty_sort_reads_nothing_and_removes_its_workspace()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let mut sorted_records = one_record_runs(temporary.path(), "empty", [])?;
        let workspace = workspace_path(&sorted_records)?;
        assert_eq!(sorted_records.run_paths(), Vec::<PathBuf>::new());
        assert_eq!(sorted_records.next_record()?, None);
        assert_eq!(sorted_records.next_record()?, None);
        assert_eq!(sorted_records.evidence().record_count, 0);
        assert_eq!(sorted_records.evidence().final_run_file_bytes, 0);
        assert!(!workspace.exists());
        Ok(())
    }

    #[test]
    fn exactly_the_fan_in_streams_without_a_written_merge() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = TempDir::new()?;
        let active_fan_in = effective_merge_fan_in();
        let fan_in = u16::try_from(active_fan_in)?;
        let max_temporary_bytes = u64::from(fan_in)
            .saturating_mul(encoded_run_record_bytes::<2>(1)?)
            .saturating_mul(2);
        let mut staged_sort = VariableValueSorter::<2>::new(
            temporary.path(),
            "fan-in",
            one_record_memory_limit::<2>(1),
            max_temporary_bytes,
        )?;
        for number in (0..fan_in).rev() {
            staged_sort.push(number.to_be_bytes(), &[number.to_be_bytes()[1]])?;
        }
        let mut sorted_records = staged_sort.finish_with_fan_in(active_fan_in)?;
        assert_eq!(sorted_records.run_paths().len(), active_fan_in);
        assert_eq!(sorted_records.evidence().merge_pass_count, 0);
        let mut keys = Vec::new();
        while let Some(record) = sorted_records.next_record()? {
            keys.push(record.key);
        }
        assert_eq!(keys, (0..fan_in).map(u16::to_be_bytes).collect::<Vec<_>>());
        Ok(())
    }

    #[test]
    fn a_fault_after_the_first_record_of_a_merged_run_fails_the_drain()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let mut staged_sort = VariableValueSorter::<1>::new(
            temporary.path(),
            "late-fault",
            accounted_record_bytes::<1>(1)?
                .checked_mul(2)
                .ok_or("test byte overflow")?,
            1_024,
        )?;
        staged_sort.push([1], b"a")?;
        staged_sort.push([3], b"b")?;
        staged_sort.push([2], b"c")?;
        let first_run = staged_sort
            .runs
            .first()
            .map(|run| run.path.clone())
            .ok_or("the first two records must have spilled")?;
        // Cut the last value byte, so the run's second record is truncated.
        let mut truncated = framed_record(1, b"a")?;
        truncated.extend(framed_record(3, b"b")?);
        truncated.pop();
        fs::write(&first_run, truncated)?;
        let mut sorted_records = staged_sort.finish()?;

        assert_eq!(
            sorted_records.next_record()?.map(|record| record.key),
            Some([1])
        );
        assert_eq!(
            sorted_records.next_record()?.map(|record| record.key),
            Some([2])
        );
        assert!(sorted_records.next_record().is_err());
        Ok(())
    }

    #[test]
    fn dropping_a_partly_read_sort_removes_its_runs() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let mut sorted_records = one_record_runs(temporary.path(), "early-drop", [3, 1, 2])?;
        let run_paths = sorted_records.run_paths();
        let workspace = workspace_path(&sorted_records)?;
        assert_eq!(run_paths.len(), 3);
        assert!(sorted_records.next_record()?.is_some());
        drop(sorted_records);
        assert!(run_paths.iter().all(|path| !path.exists()));
        assert!(!workspace.exists());
        Ok(())
    }

    #[test]
    fn unwinding_past_a_partly_read_sort_removes_its_runs() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = TempDir::new()?;
        let mut sorted_records = one_record_runs(temporary.path(), "unwind", [3, 1, 2])?;
        let run_paths = sorted_records.run_paths();
        let workspace = workspace_path(&sorted_records)?;
        assert!(sorted_records.next_record()?.is_some());
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _owned = sorted_records;
            std::panic::resume_unwind(Box::new("injected unwind"));
        }));
        assert!(unwound.is_err());
        assert!(run_paths.iter().all(|path| !path.exists()));
        assert!(!workspace.exists());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn a_run_that_cannot_be_removed_at_the_end_is_reported()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::PermissionsExt;

        let temporary = TempDir::new()?;
        let mut sorted_records = one_record_runs(temporary.path(), "locked", [2, 1])?;
        let workspace = workspace_path(&sorted_records)?;
        fs::set_permissions(&workspace, fs::Permissions::from_mode(0o500))?;
        // A privileged user can still unlink in a read-only directory, so the
        // refusal this test needs cannot be produced.
        if File::create(workspace.join("probe")).is_ok() {
            fs::set_permissions(&workspace, fs::Permissions::from_mode(0o700))?;
            return Ok(());
        }
        let outcome = drain_to_end(&mut sorted_records);
        fs::set_permissions(&workspace, fs::Permissions::from_mode(0o700))?;

        assert!(matches!(
            outcome,
            Err(BulkLoadError::PathUnavailable { .. })
        ));
        Ok(())
    }

    #[test]
    fn a_resized_record_in_a_single_run_fails_the_drain() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = TempDir::new()?;
        let mut staged_sort = VariableValueSorter::<1>::new(
            temporary.path(),
            "resized-single",
            one_record_memory_limit::<1>(2),
            1_024,
        )?;
        staged_sort.push([1], b"ab")?;
        let mut sorted_records = staged_sort.finish()?;
        let run_path = sorted_records
            .run_paths()
            .pop()
            .ok_or("sorted run must exist")?;
        fs::write(&run_path, framed_record(1, b"a")?)?;

        assert!(matches!(
            drain_to_end(&mut sorted_records),
            Err(BulkLoadError::InvalidInput { .. })
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

    fn collect_sorted<const KEY_LEN: usize>(
        sorted: &mut SortedVariableValues<KEY_LEN>,
    ) -> Result<Vec<VariableValueRecord<KEY_LEN>>, BulkLoadError> {
        let mut collected = Vec::new();
        while let Some(record) = sorted.next_record()? {
            collected.push(record);
        }
        Ok(collected)
    }

    #[test]
    fn parallel_sorter_matches_serial_output_across_spills_and_merges()
    -> Result<(), Box<dyn std::error::Error>> {
        let records = (0_u16..320)
            .map(|index| {
                let key = ((index.wrapping_mul(173)) ^ 0x5A3C).to_be_bytes();
                let payload =
                    vec![u8::try_from(index % 251).unwrap_or(0); usize::from(index % 9) + 1];
                (key, payload)
            })
            .collect::<Vec<_>>();
        let serial_dir = TempDir::new()?;
        let mut serial =
            VariableValueSorter::<2>::new(serial_dir.path(), "serial-ref", 2_048, 1024 * 1024)?;
        for (key, payload) in &records {
            serial.push(*key, payload)?;
        }
        let mut serial_sorted = serial.finish()?;
        let expected = collect_sorted(&mut serial_sorted)?;

        for shards in [1_u32, 2, 4, 8] {
            let shard_count = NonZeroU32::new(shards).ok_or("zero shards")?;
            let parallel_dir = TempDir::new()?;
            let max_memory = u64::from(shards) * 512;
            let max_temp = 1024 * 1024;
            let parallel = ParallelVariableValueSorter::<2>::new(
                parallel_dir.path(),
                "parallel-check",
                shard_count,
                max_memory,
                max_temp,
            )?;
            let open_shards = parallel.open_shards()?;
            let chunk_size = records.len().div_ceil(open_shards.len());
            let returned_shards = std::thread::scope(|scope| {
                let mut handles = Vec::with_capacity(open_shards.len());
                for (shard_index, mut shard) in open_shards.into_iter().enumerate() {
                    let start = shard_index * chunk_size;
                    let end = (start + chunk_size).min(records.len());
                    let slice = &records[start..end];
                    handles.push(scope.spawn(move || -> Result<_, BulkLoadError> {
                        for (key, payload) in slice {
                            shard.push(*key, payload)?;
                        }
                        Ok(shard)
                    }));
                }
                handles
                    .into_iter()
                    .map(|handle| {
                        handle
                            .join()
                            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
                    })
                    .collect::<Result<Vec<_>, _>>()
            })?;
            let staged = parallel.flush_shards(returned_shards)?;
            let mut sorted = parallel.finish(staged)?;
            let evidence = sorted.evidence();
            assert_eq!(evidence.record_count, u64::try_from(records.len())?);
            assert!(evidence.peak_accounted_memory_bytes <= evidence.max_accounted_memory_bytes);
            assert!(evidence.peak_temporary_file_bytes <= evidence.max_temporary_file_bytes);
            let observed = collect_sorted(&mut sorted)?;
            assert_eq!(observed, expected, "{shards} shards");
        }
        Ok(())
    }

    #[test]
    fn parallel_sorter_triggers_merge_pass_when_combined_runs_exceed_fan_in()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let shard_count = NonZeroU32::new(4).ok_or("zero shards")?;
        let one_record = accounted_record_bytes::<2>(4)?;
        let parallel = ParallelVariableValueSorter::<2>::new(
            dir.path(),
            "parallel-merge-pass",
            shard_count,
            one_record * 4,
            1024 * 1024,
        )?;
        let mut shards = parallel.open_shards()?;
        for index in 0_u16..1_024 {
            let shard_idx = usize::from(index) % shards.len();
            shards[shard_idx].push((1_024 - index).to_be_bytes(), &[0xAB; 4])?;
        }
        let staged = parallel.flush_shards(shards)?;
        let mut sorted = parallel.finish(staged)?;
        let evidence = sorted.evidence();
        assert_eq!(evidence.record_count, 1_024);
        assert!(evidence.initial_run_count > u64::try_from(MAX_MERGE_FAN_IN)?);
        assert!(evidence.merge_pass_count >= 1);
        let drained = collect_sorted(&mut sorted)?;
        assert_eq!(drained.len(), 1_024);
        assert!(drained.windows(2).all(|pair| pair[0].key <= pair[1].key));
        Ok(())
    }

    #[test]
    fn parallel_sorter_enforces_shared_temporary_file_ceiling_across_shards()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let shard_count = NonZeroU32::new(2).ok_or("zero shards")?;
        let one_record = accounted_record_bytes::<2>(16)?;
        let encoded_bytes = encoded_run_record_bytes::<2>(16)?;
        // Enough for one single run (encoded_bytes), but NOT enough for two
        // runs (which require 2 * (encoded_bytes + encoded_bytes) = 4 * encoded_bytes).
        let tight_temp_limit = encoded_bytes * 3;
        let parallel = ParallelVariableValueSorter::<2>::new(
            dir.path(),
            "shared-temp-cap",
            shard_count,
            one_record * 2,
            tight_temp_limit,
        )?;
        let mut shards = parallel.open_shards()?;
        shards[0].push([0, 1], &[1_u8; 16])?;
        shards[1].push([0, 2], &[2_u8; 16])?;
        let flushed = parallel.flush_shards(shards);
        assert!(
            matches!(flushed, Err(BulkLoadError::TemporaryFileLimit { .. })),
            "expected shared temporary-file limit error, got {flushed:?}"
        );
        Ok(())
    }

    #[test]
    fn parallel_sorter_rejects_under_minimum_shard_memory_and_zero_temp_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let four = NonZeroU32::new(4).ok_or("zero shards")?;
        let min_record = u64::try_from(size_of::<StagedVariableValue<2>>())?;
        assert!(matches!(
            ParallelVariableValueSorter::<2>::new(
                dir.path(),
                "too-small",
                four,
                min_record * 4 - 1,
                1024
            ),
            Err(BulkLoadError::AccountedMemoryLimit { .. })
        ));
        assert!(matches!(
            ParallelVariableValueSorter::<2>::new(dir.path(), "zero-temp", four, min_record * 8, 0),
            Err(BulkLoadError::TemporaryFileLimit { .. })
        ));
        Ok(())
    }

    #[test]
    fn parallel_sorter_admits_record_larger_than_shard_within_global_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let four = NonZeroU32::new(4).ok_or("zero shards")?;
        let global_budget = 256 * 1024_u64;
        let oversized_payload = vec![0x5A_u8; 100 * 1024];
        let parallel = ParallelVariableValueSorter::<2>::new(
            dir.path(),
            "oversized-record",
            four,
            global_budget,
            4 * 1024 * 1024,
        )?;
        let mut shards = parallel.open_shards()?;
        shards[0].push([0, 3], &[0x11; 16])?;
        shards[0].push([0, 1], &oversized_payload)?;
        shards[1].push([0, 2], &[0x22; 16])?;
        let too_large = vec![0xFF_u8; usize::try_from(global_budget)? + 1];
        assert!(matches!(
            shards[2].push([0, 4], &too_large),
            Err(BulkLoadError::AccountedMemoryLimit { limit_bytes, .. })
                if limit_bytes == global_budget
        ));
        let staged = parallel.flush_shards(shards)?;
        let mut sorted = parallel.finish(staged)?;
        let evidence = sorted.evidence();
        assert_eq!(evidence.record_count, 3);
        assert!(evidence.peak_accounted_memory_bytes <= global_budget);
        let drained = collect_sorted(&mut sorted)?;
        assert_eq!(drained.len(), 3);
        assert_eq!(drained[0].key, [0, 1]);
        assert_eq!(drained[0].encoded_value, oversized_payload);
        assert_eq!(drained[1].key, [0, 2]);
        assert_eq!(drained[2].key, [0, 3]);
        Ok(())
    }

    #[test]
    fn parallel_sorter_rejects_second_open_shards_and_foreign_shards()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let two = NonZeroU32::new(2).ok_or("zero shards")?;
        let min_record = u64::try_from(size_of::<StagedVariableValue<2>>())?;
        let parallel_a = ParallelVariableValueSorter::<2>::new(
            dir.path(),
            "sorter-a",
            two,
            min_record * 4,
            1024,
        )?;
        let parallel_b = ParallelVariableValueSorter::<2>::new(
            dir.path(),
            "sorter-b",
            two,
            min_record * 4,
            1024,
        )?;

        let shards_a = parallel_a.open_shards()?;
        assert!(matches!(
            parallel_a.open_shards(),
            Err(BulkLoadError::InvalidInput { .. })
        ));

        let shards_b = parallel_b.open_shards()?;
        assert!(matches!(
            parallel_a.flush_shards(shards_b),
            Err(BulkLoadError::InvalidInput { .. })
        ));
        assert!(matches!(
            parallel_a.flush_shards(Vec::new()),
            Err(BulkLoadError::InvalidInput { .. })
        ));

        let staged_a = parallel_a.flush_shards(shards_a)?;
        assert!(matches!(
            parallel_a.open_shards(),
            Err(BulkLoadError::InvalidInput { .. })
        ));
        let mut sorted = parallel_a.finish(staged_a)?;
        assert_eq!(collect_sorted(&mut sorted)?, Vec::new());
        Ok(())
    }

    #[test]
    fn parallel_sorter_rejects_foreign_staged_runs_and_incomplete_shard_flush()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let two = NonZeroU32::new(2).ok_or("zero shards")?;
        let parallel_a =
            ParallelVariableValueSorter::<2>::new(dir.path(), "staged-a", two, 1_024, 4_096)?;
        let parallel_b =
            ParallelVariableValueSorter::<2>::new(dir.path(), "staged-b", two, 1_024, 4_096)?;

        let mut shards_a = parallel_a.open_shards()?;
        shards_a[0].push([0, 1], &[0x11; 8])?;
        let staged_a = parallel_a.flush_shards(shards_a)?;
        assert!(matches!(
            parallel_b.finish(staged_a),
            Err(BulkLoadError::InvalidInput { .. })
        ));

        let parallel_c =
            ParallelVariableValueSorter::<2>::new(dir.path(), "incomplete-c", two, 1_024, 4_096)?;
        let mut shards_c = parallel_c.open_shards()?;
        shards_c.pop();
        assert!(matches!(
            parallel_c.flush_shards(shards_c),
            Err(BulkLoadError::InvalidInput { .. })
        ));
        Ok(())
    }

    #[test]
    fn sharded_key_prefix_sorter_partitions_disjoint_ascending_streams_with_empty_shards()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let limit = one_record_memory_limit::<4>(8) * 6;
        let records = [
            ([0xfe, 0x00, 0x00, 0x01], vec![1_u8, 2]),
            ([0x10, 0x20, 0x00, 0x02], vec![3, 4, 5]),
            ([0x10, 0x20, 0x00, 0x02], vec![6, 7]),
            ([0x00, 0x01, 0xff, 0x00], vec![8]),
            ([0x7f, 0xff, 0x00, 0x01], vec![9, 10]),
        ];

        let mut serial = VariableValueSorter::<4>::new(dir.path(), "serial", limit, 16_384)?;
        for (key, payload) in &records {
            serial.push(*key, payload)?;
        }
        let mut expected_reader = serial.finish()?;
        let expected = collect_sorted(&mut expected_reader)?;

        for shards in [1_u32, 2, 3, 5, 8] {
            let shard_count = NonZeroU32::new(shards).ok_or("nonzero shard count")?;
            let sharded = ShardedKeyPrefixSorter::<4>::new(
                dir.path(),
                "sharded",
                shard_count,
                limit,
                16_384,
            )?;
            assert!(sharded.per_shard_merge_fan_in() >= MIN_MERGE_FAN_IN);
            assert!(
                sharded.per_shard_merge_fan_in() * usize::try_from(shards)? <= MAX_MERGE_FAN_IN
            );
            for (key, payload) in &records {
                sharded.push(*key, payload)?;
            }
            let finished = sharded.finish()?;
            assert_eq!(finished.shard_count(), usize::try_from(shards)?);
            assert_eq!(
                finished.evidence().record_count,
                u64::try_from(records.len())?
            );
            assert!(finished.evidence().peak_accounted_memory_bytes <= limit);

            let mut combined = Vec::new();
            let mut previous_shard_max: Option<[u8; 4]> = None;
            for mut stream in finished.into_shards() {
                let shard_records = collect_sorted(&mut stream)?;
                if let (Some(prev_max), Some(first_in_shard)) =
                    (previous_shard_max, shard_records.first())
                {
                    assert!(
                        prev_max < first_in_shard.key,
                        "shard boundaries must be strictly ascending"
                    );
                }
                if let Some(last_in_shard) = shard_records.last() {
                    previous_shard_max = Some(last_in_shard.key);
                }
                combined.extend(shard_records);
            }
            assert_eq!(combined, expected);
        }
        Ok(())
    }

    #[test]
    fn sharded_key_prefix_sorter_allows_concurrent_writers_and_oversized_records()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let shard_count = NonZeroU32::new(4).ok_or("nonzero shards")?;
        let small_record_limit = one_record_memory_limit::<4>(8);
        let oversized_payload = vec![0xaa_u8; 256];
        let global_limit = one_record_memory_limit::<4>(oversized_payload.len());
        let sorter = ShardedKeyPrefixSorter::<4>::new(
            dir.path(),
            "concurrent-prefix",
            shard_count,
            global_limit,
            64_000,
        )?;
        assert!(global_limit > small_record_limit);

        thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
            let worker_count = NonZeroU32::new(4).ok_or("nonzero workers")?;
            let mut handles = Vec::new();
            for worker_id in 0_u8..4 {
                let sorter_ref = &sorter;
                handles.push(scope.spawn(move || -> Result<(), BulkLoadError> {
                    let mut writer = sorter_ref.writer(worker_count);
                    for prefix in [0x00_u8, 0x40, 0x80, 0xc0] {
                        let key = [prefix.wrapping_add(worker_id), worker_id, 0, prefix];
                        writer.push(key, &[worker_id; 8])?;
                    }
                    writer.flush()
                }));
            }
            for handle in handles {
                handle.join().map_err(|_| "worker panicked")??;
            }
            Ok(())
        })?;

        sorter.push([0x01, 0x00, 0x00, 0x00], &oversized_payload)?;
        let finished = sorter.finish()?;
        let evidence = finished.evidence();
        assert_eq!(evidence.record_count, 17);
        assert!(evidence.peak_accounted_memory_bytes <= global_limit);

        let mut total = 0_usize;
        let mut saw_oversized = false;
        for mut stream in finished.into_shards() {
            while let Some(record) = stream.next_record()? {
                total = total.saturating_add(1);
                if record.key == [0x01, 0x00, 0x00, 0x00] {
                    assert_eq!(record.encoded_value, oversized_payload);
                    saw_oversized = true;
                }
            }
        }
        assert_eq!(total, 17);
        assert!(saw_oversized);
        Ok(())
    }

    #[test]
    fn sharded_key_prefix_sorter_spills_and_merges_within_per_shard_fan_in()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let shard_count = NonZeroU32::new(4).ok_or("nonzero shards")?;
        let single_record_bytes = one_record_memory_limit::<4>(12);
        let global_limit = single_record_bytes * 4;
        let sorter = ShardedKeyPrefixSorter::<4>::new(
            dir.path(),
            "merge-fan-in",
            shard_count,
            global_limit,
            256_000,
        )?;
        let expected_fan_in = sorter.per_shard_merge_fan_in();
        let entry_count = expected_fan_in.saturating_add(12);
        for index in 0..entry_count {
            let tail = u16::try_from(index)?;
            let tail_bytes = tail.to_be_bytes();
            sorter.push([0x00, 0x00, tail_bytes[0], tail_bytes[1]], &[0x33; 12])?;
        }
        let finished = sorter.finish()?;
        let evidence = finished.evidence();
        assert_eq!(evidence.record_count, u64::try_from(entry_count)?);
        assert!(evidence.merge_pass_count >= 1);
        assert!(evidence.peak_accounted_memory_bytes <= global_limit);

        let mut drained = 0_usize;
        let mut prev_key: Option<[u8; 4]> = None;
        for mut stream in finished.into_shards() {
            while let Some(record) = stream.next_record()? {
                if let Some(prev) = prev_key {
                    assert!(prev < record.key);
                }
                prev_key = Some(record.key);
                drained = drained.saturating_add(1);
            }
        }
        assert_eq!(drained, entry_count);
        Ok(())
    }

    #[test]
    fn sharded_key_prefix_sorter_enforces_memory_and_temporary_file_limits()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let shard_count = NonZeroU32::new(2).ok_or("nonzero shards")?;
        let mem_sorter =
            ShardedKeyPrefixSorter::<2>::new(dir.path(), "mem-limit", shard_count, 8, 4_096)?;
        assert!(matches!(
            mem_sorter.push([0x10, 0x20], &[0x44; 32]),
            Err(BulkLoadError::AccountedMemoryLimit {
                limit_bytes: 8,
                required_bytes: _
            })
        ));

        let temp_sorter =
            ShardedKeyPrefixSorter::<2>::new(dir.path(), "temp-limit", shard_count, 1024, 1)?;
        temp_sorter.push([0x10, 0x20], &[0x55; 16])?;
        assert!(matches!(
            temp_sorter.finish(),
            Err(BulkLoadError::TemporaryFileLimit {
                limit_bytes: 1,
                required_bytes: _
            })
        ));
        Ok(())
    }

    #[test]
    fn sharded_key_prefix_sorter_rejects_explicit_fan_in_oversubscribing_total()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let shard_count = NonZeroU32::new(8).ok_or("nonzero shards")?;
        let total_fan_in = effective_merge_fan_in();
        let rejected = ShardedKeyPrefixSorter::<2>::new_with_budget(
            dir.path(),
            "oversubscribed-fan-in",
            ShardedSortBudget {
                shard_count,
                max_accounted_memory_bytes: 16_384,
                max_temporary_file_bytes: 65_536,
                per_shard_merge_fan_in_cap: Some(total_fan_in),
            },
        );
        assert!(matches!(rejected, Err(BulkLoadError::InvalidInput { .. })));
        Ok(())
    }

    #[test]
    fn sharded_key_prefix_writer_flushes_retained_buckets_before_direct_large_record()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let shard_count = NonZeroU32::new(4).ok_or("nonzero shards")?;
        let global_limit = WRITER_CREDIT_MIN_MEMORY_BYTES;
        let sorter = ShardedKeyPrefixSorter::<2>::new(
            dir.path(),
            "writer-flush",
            shard_count,
            global_limit,
            global_limit * 4,
        )?;
        let mut writer = sorter.writer(NonZeroU32::MIN);
        for prefix in [0x00_u8, 0x40, 0x80, 0xc0] {
            writer.push([prefix, 0x01], &[0x11; 64])?;
        }
        let large_len =
            usize::try_from(global_limit.saturating_sub(one_record_memory_limit::<2>(0) + 512))?;
        let large_payload = vec![0x22_u8; large_len];
        writer.push([0x20, 0x01], &large_payload)?;
        writer.flush()?;

        let finished = sorter.finish()?;
        assert_eq!(finished.evidence().record_count, 5);
        assert!(finished.evidence().peak_accounted_memory_bytes <= global_limit);
        Ok(())
    }

    #[test]
    fn sharded_key_prefix_sorter_enforces_two_x_merge_headroom_and_before_finish_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let shard_count = NonZeroU32::new(2).ok_or("nonzero shards")?;
        let single_record_bytes = one_record_memory_limit::<2>(32);
        let run_bytes = encoded_run_record_bytes::<2>(32)?;
        let max_accounted = single_record_bytes * 2;
        let tight_temp_limit = run_bytes * 4 - 1;
        let limited = ShardedKeyPrefixSorter::<2>::new(
            dir.path(),
            "before-finish-temp",
            shard_count,
            max_accounted,
            tight_temp_limit,
        )?;
        limited.push([0x00, 0x01], &[0x44; 32])?;
        limited.push([0x00, 0x02], &[0x44; 32])?;
        let spill_err = limited.push([0x00, 0x03], &[0x44; 32]);
        assert!(matches!(
            spill_err,
            Err(BulkLoadError::TemporaryFileLimitBeforeFinish {
                limit_bytes,
                required_bytes,
            }) if limit_bytes == tight_temp_limit && required_bytes == run_bytes * 4
        ));

        let multi_pass = ShardedKeyPrefixSorter::<2>::new_with_budget(
            dir.path(),
            "two-x-multipass",
            ShardedSortBudget {
                shard_count,
                max_accounted_memory_bytes: max_accounted,
                max_temporary_file_bytes: run_bytes * 32,
                per_shard_merge_fan_in_cap: Some(2),
            },
        )?;
        for seq in 0_u8..8 {
            multi_pass.push([0x00, seq], &[0x55; 32])?;
            multi_pass.push([0x80, seq], &[0x66; 32])?;
        }
        let finished = multi_pass.finish()?;
        assert_eq!(finished.evidence().record_count, 16);
        assert!(finished.evidence().merge_pass_count >= 1);
        Ok(())
    }

    fn assert_sorter_capacity_invariants<const KEY_LEN: usize>(
        sorter: &ShardedKeyPrefixSorter<KEY_LEN>,
        global_limit: u64,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let mut total_capacity_bytes = 0_u64;
        for partition_lock in &sorter.partitions {
            let partition = partition_lock.lock();
            let rec_bytes = partition
                .staging
                .staged_records
                .capacity()
                .checked_mul(size_of::<StagedVariableValue<KEY_LEN>>())
                .ok_or("record capacity overflow")?;
            let actual_bytes = u64::try_from(
                rec_bytes
                    .checked_add(partition.staging.encoded_values.capacity())
                    .ok_or("capacity sum overflow")?,
            )?;
            assert_eq!(actual_bytes, partition.staging.accounted_bytes);
            assert_eq!(actual_bytes, partition.staging.allocated_bytes()?);
            total_capacity_bytes = total_capacity_bytes.saturating_add(actual_bytes);
        }
        let active_bytes = sorter.memory.active_bytes.load(Ordering::SeqCst);
        assert!(total_capacity_bytes <= global_limit);
        assert!(active_bytes <= global_limit);
        assert!(total_capacity_bytes <= active_bytes);
        Ok(total_capacity_bytes)
    }

    #[test]
    fn sharded_key_prefix_sorter_bounds_actual_vector_capacity_within_memory_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let shard_count = NonZeroU32::new(4).ok_or("nonzero shards")?;
        let tight_limit = one_record_memory_limit::<2>(64) * 4;
        let tight = ShardedKeyPrefixSorter::<2>::new(
            dir.path(),
            "cap-tight",
            shard_count,
            tight_limit,
            tight_limit * 4,
        )?;
        for (idx, prefix) in [0x00_u8, 0x40, 0x80, 0xc0].into_iter().enumerate() {
            tight.push([prefix, 0x01], &[u8::try_from(idx)?; 64])?;
            assert_sorter_capacity_invariants(&tight, tight_limit)?;
        }
        assert_eq!(tight.finish()?.evidence().record_count, 4);

        let global_limit = WRITER_CREDIT_MIN_MEMORY_BYTES;
        let sorter = ShardedKeyPrefixSorter::<2>::new(
            dir.path(),
            "cap-growth",
            shard_count,
            global_limit,
            global_limit * 4,
        )?;
        let mut writer = sorter.writer(NonZeroU32::MIN);
        for index in 0_usize..120 {
            let prefix = u8::try_from((index % 4) * 64)?;
            let payload_len = 17 + (index % 253);
            let payload = vec![u8::try_from(index & 0xff)?; payload_len];
            writer.push([prefix, u8::try_from(index)?], &payload)?;
            for bucket in &writer.buckets {
                assert_eq!(bucket.allocated_bytes()?, bucket.accounted_bytes);
            }
        }
        writer.flush()?;
        assert_sorter_capacity_invariants(&sorter, global_limit)?;
        for index in 120_usize..160 {
            let prefix = u8::try_from((index % 4) * 64)?;
            let payload = vec![0x77_u8; 40 + (index % 190)];
            sorter.push([prefix, u8::try_from(index)?], &payload)?;
            if index % 10 == 0 {
                assert_sorter_capacity_invariants(&sorter, global_limit)?;
            }
        }
        let live = assert_sorter_capacity_invariants(&sorter, global_limit)?;
        assert_eq!(live, sorter.memory.active_bytes.load(Ordering::SeqCst));
        assert_eq!(sorter.finish()?.evidence().record_count, 160);
        Ok(())
    }

    struct BarrierPublishRaceHook {
        publish_gated: std::sync::atomic::AtomicBool,
        scan_gated: std::sync::atomic::AtomicBool,
        reserved_barrier: std::sync::Barrier,
        scanned_barrier: std::sync::Barrier,
        after_publish_barrier: std::sync::Barrier,
    }

    impl ReclaimRaceHook for BarrierPublishRaceHook {
        fn on_before_publish(&self) {
            if !self.publish_gated.swap(true, Ordering::SeqCst) {
                self.reserved_barrier.wait();
                self.scanned_barrier.wait();
            }
        }

        fn on_after_empty_scan(&self, _in_flight_work: u64) {
            if !self.scan_gated.swap(true, Ordering::SeqCst) {
                self.scanned_barrier.wait();
                self.after_publish_barrier.wait();
            }
        }
    }

    #[test]
    fn sharded_key_prefix_sorter_retries_reclaim_when_concurrent_writer_publishes_after_scan()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let shard_count = NonZeroU32::new(2).ok_or("nonzero shards")?;
        let r_small = one_record_memory_limit::<2>(16);
        let global_limit = r_small * 2;
        let hook = std::sync::Arc::new(BarrierPublishRaceHook {
            publish_gated: std::sync::atomic::AtomicBool::new(false),
            scan_gated: std::sync::atomic::AtomicBool::new(false),
            reserved_barrier: std::sync::Barrier::new(2),
            scanned_barrier: std::sync::Barrier::new(2),
            after_publish_barrier: std::sync::Barrier::new(2),
        });
        let mut sorter = ShardedKeyPrefixSorter::<2>::new(
            dir.path(),
            "reclaim-race",
            shard_count,
            global_limit,
            64_000,
        )?;
        sorter.set_reclaim_race_hook(std::sync::Arc::clone(&hook) as _);

        thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
            let sorter_ref = &sorter;
            let hook_a = std::sync::Arc::clone(&hook);
            let handle_a = scope.spawn(move || -> Result<(), BulkLoadError> {
                sorter_ref.push([0x00, 0x01], &[0xaa; 16])?;
                hook_a.after_publish_barrier.wait();
                Ok(())
            });
            let hook_b = std::sync::Arc::clone(&hook);
            let handle_b = scope.spawn(move || -> Result<(), BulkLoadError> {
                hook_b.reserved_barrier.wait();
                sorter_ref.push([0x80, 0x02], &[0xbb; 32])
            });
            handle_a.join().map_err(|_| "writer A panicked")??;
            handle_b.join().map_err(|_| "writer B panicked")??;
            Ok(())
        })?;

        let finished = sorter.finish()?;
        assert_eq!(finished.evidence().record_count, 2);
        let mut keys = Vec::new();
        for mut stream in finished.into_shards() {
            while let Some(record) = stream.next_record()? {
                keys.push(record.key);
            }
        }
        assert_eq!(keys, vec![[0x00, 0x01], [0x80, 0x02]]);
        Ok(())
    }

    #[test]
    fn sharded_key_prefix_sorter_flushes_all_tails_before_merging_within_two_x_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_budget = SharedTempFileBudget::new(1_000);
        for _ in 0..4 {
            temp_budget.reserve_initial_run(100)?;
        }
        temp_budget.reserve(200)?;
        temp_budget.reserve_initial_run(100)?;
        assert!(matches!(
            temp_budget.reserve_initial_run(1),
            Err(BulkLoadError::TemporaryFileLimit {
                limit_bytes: 1_000,
                required_bytes: 1_002,
            })
        ));
        temp_budget.release(200)?;

        let dir = TempDir::new()?;
        let shard_count = NonZeroU32::new(2).ok_or("nonzero shards")?;
        let one_rec = one_record_memory_limit::<2>(32);
        let run_bytes = encoded_run_record_bytes::<2>(32)?;
        let exact_temp_limit = run_bytes * 10;
        let sorter = ShardedKeyPrefixSorter::<2>::new_with_budget(
            dir.path(),
            "phased-tail-flush",
            ShardedSortBudget {
                shard_count,
                max_accounted_memory_bytes: one_rec * 2,
                max_temporary_file_bytes: exact_temp_limit,
                per_shard_merge_fan_in_cap: Some(2),
            },
        )?;
        for seq in 0_u8..4 {
            sorter.push([0x00, seq], &[0x44; 32])?;
        }
        sorter.push([0x80, 0x01], &[0x55; 32])?;
        let finished = sorter.finish()?;
        let evidence = finished.evidence();
        assert_eq!(evidence.initial_run_count, 5);
        assert!(evidence.merge_pass_count >= 1);
        assert!(evidence.peak_temporary_file_bytes <= exact_temp_limit);
        let mut count = 0_usize;
        for mut stream in finished.into_shards() {
            while stream.next_record()?.is_some() {
                count = count.saturating_add(1);
            }
        }
        assert_eq!(count, 5);
        Ok(())
    }

    struct BucketAppendPeakHook {
        calls: AtomicU64,
        observed_combined: AtomicU64,
        observed_active: AtomicU64,
    }

    impl ReclaimRaceHook for BucketAppendPeakHook {
        fn on_after_bucket_append(
            &self,
            staging_allocated_bytes: u64,
            bucket_allocated_bytes: u64,
            active_bytes: u64,
        ) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let combined = staging_allocated_bytes.saturating_add(bucket_allocated_bytes);
            self.observed_combined.fetch_max(combined, Ordering::SeqCst);
            self.observed_active
                .fetch_max(active_bytes, Ordering::SeqCst);
        }
    }

    fn make_exact_preaccounted_bucket<const KEY_LEN: usize>(
        sorter: &ShardedKeyPrefixSorter<KEY_LEN>,
        key: [u8; KEY_LEN],
        payload: &[u8],
    ) -> Result<PartitionBatch<KEY_LEN>, Box<dyn std::error::Error>> {
        let mut bucket = PartitionBatch::default();
        let min_bytes = bucket.min_growth_bytes(1, payload.len())?;
        assert!(sorter.memory.try_reserve_bytes(min_bytes));
        let mut charged = 0_u64;
        bucket.push_record_within(key, payload, min_bytes, &mut charged)?;
        assert_eq!(charged, min_bytes);
        assert_eq!(bucket.allocated_bytes()?, min_bytes);
        Ok(bucket)
    }

    fn assert_tight_bucket_append_flush_fallback(
        staging_root: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let one_shard = NonZeroU32::MIN;
        let tight_base = one_record_memory_limit::<2>(64) * 16;
        let incoming_bytes = one_record_memory_limit::<2>(512);
        let probe = ShardedKeyPrefixSorter::<2>::new(
            staging_root,
            "append-tight-probe",
            one_shard,
            tight_base + incoming_bytes,
            256_000,
        )?;
        probe.push([0x00, 0x01], &[0x33; 64])?;
        let staged_cap = probe.partitions[0].lock().staging.accounted_bytes;
        drop(probe);

        let exact_limit = staged_cap + incoming_bytes;
        let tight = ShardedKeyPrefixSorter::<2>::new(
            staging_root,
            "append-tight-fallback",
            one_shard,
            exact_limit,
            256_000,
        )?;
        tight.push([0x00, 0x01], &[0x33; 64])?;
        let min_growth = tight.partitions[0]
            .lock()
            .staging
            .min_growth_bytes(1, 512)?;
        assert!(min_growth > 0);
        assert!(staged_cap + min_growth <= tight.memory.per_partition_bytes);
        let tight_bucket = make_exact_preaccounted_bucket(&tight, [0x00, 0x02], &[0x44; 512])?;
        assert_eq!(
            tight.memory.active_bytes.load(Ordering::SeqCst),
            exact_limit
        );
        tight.push_preaccounted_bucket(0, tight_bucket)?;
        assert_eq!(
            tight.memory.active_bytes.load(Ordering::SeqCst),
            incoming_bytes
        );
        let finished_tight = tight.finish()?;
        let evidence = finished_tight.evidence();
        assert_eq!(evidence.record_count, 2);
        assert!(evidence.initial_run_count >= 2);
        assert!(evidence.peak_accounted_memory_bytes <= exact_limit);
        let mut keys = Vec::new();
        for mut stream in finished_tight.into_shards() {
            while let Some(record) = stream.next_record()? {
                keys.push(record.key);
            }
        }
        assert_eq!(keys, vec![[0x00, 0x01], [0x00, 0x02]]);
        Ok(())
    }

    #[test]
    fn sharded_key_prefix_sorter_accounts_for_intermediate_bucket_append_peak()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let shard_count = NonZeroU32::new(2).ok_or("nonzero shards")?;
        let r_large = one_record_memory_limit::<2>(512);
        let global_limit = r_large * 16;
        let hook = std::sync::Arc::new(BucketAppendPeakHook {
            calls: AtomicU64::new(0),
            observed_combined: AtomicU64::new(0),
            observed_active: AtomicU64::new(0),
        });
        let mut sorter = ShardedKeyPrefixSorter::<2>::new(
            dir.path(),
            "append-peak",
            shard_count,
            global_limit,
            256_000,
        )?;
        sorter.set_reclaim_race_hook(std::sync::Arc::clone(&hook) as _);
        sorter.push([0x00, 0x01], &[0x11; 64])?;
        let bucket = make_exact_preaccounted_bucket(&sorter, [0x00, 0x02], &[0x22; 512])?;
        let shard_idx = sorter.partition_index(&[0x00, 0x02]);
        assert!(
            sorter.partitions[shard_idx]
                .lock()
                .staging
                .min_growth_bytes(1, 512)?
                > 0
        );
        sorter.push_preaccounted_bucket(shard_idx, bucket)?;
        assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
        let combined = hook.observed_combined.load(Ordering::SeqCst);
        let active = hook.observed_active.load(Ordering::SeqCst);
        assert!(combined > 0);
        assert!(combined <= active);
        assert!(active <= global_limit);
        assert!(sorter.memory.peak_bytes() >= combined);
        let finished = sorter.finish()?;
        assert_eq!(finished.evidence().record_count, 2);
        assert!(finished.evidence().peak_accounted_memory_bytes <= global_limit);

        assert_tight_bucket_append_flush_fallback(dir.path())
    }

    struct FlushDetachBarrierHook {
        flush_gated: std::sync::atomic::AtomicBool,
        scan_gated: std::sync::atomic::AtomicBool,
        detached_barrier: std::sync::Barrier,
        scanned_barrier: std::sync::Barrier,
        observed_in_flight: AtomicU64,
    }

    impl ReclaimRaceHook for FlushDetachBarrierHook {
        fn on_after_empty_scan(&self, in_flight_work: u64) {
            if !self.scan_gated.swap(true, Ordering::SeqCst) {
                self.observed_in_flight
                    .store(in_flight_work, Ordering::SeqCst);
                self.scanned_barrier.wait();
            }
        }

        fn on_after_flush_detach(&self) {
            if !self.flush_gated.swap(true, Ordering::SeqCst) {
                self.detached_barrier.wait();
                self.scanned_barrier.wait();
            }
        }
    }

    #[test]
    fn sharded_key_prefix_sorter_tracks_flush_in_flight_across_detach_window()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let shard_count = NonZeroU32::new(2).ok_or("nonzero shards")?;
        let r_small = one_record_memory_limit::<2>(16);
        let global_limit = r_small * 2;
        let hook = std::sync::Arc::new(FlushDetachBarrierHook {
            flush_gated: std::sync::atomic::AtomicBool::new(false),
            scan_gated: std::sync::atomic::AtomicBool::new(false),
            detached_barrier: std::sync::Barrier::new(2),
            scanned_barrier: std::sync::Barrier::new(2),
            observed_in_flight: AtomicU64::new(0),
        });
        let mut sorter = ShardedKeyPrefixSorter::<2>::new(
            dir.path(),
            "flush-detach-window",
            shard_count,
            global_limit,
            64_000,
        )?;
        sorter.push([0x00, 0x01], &[0xaa; 16])?;
        sorter.set_reclaim_race_hook(std::sync::Arc::clone(&hook) as _);

        thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
            let sorter_ref = &sorter;
            let flusher = scope.spawn(move || sorter_ref.flush_partition_if_nonempty(0));
            let hook_pusher = std::sync::Arc::clone(&hook);
            let pusher = scope.spawn(move || -> Result<(), BulkLoadError> {
                hook_pusher.detached_barrier.wait();
                sorter_ref.push([0x80, 0x02], &[0xbb; 32])
            });
            assert!(flusher.join().map_err(|_| "flusher panicked")??);
            pusher.join().map_err(|_| "pusher panicked")??;
            Ok(())
        })?;

        assert!(hook.observed_in_flight.load(Ordering::SeqCst) >= 1);
        let finished = sorter.finish()?;
        assert_eq!(finished.evidence().record_count, 2);
        assert!(finished.evidence().peak_accounted_memory_bytes <= global_limit);
        Ok(())
    }

    struct GuardDropOrderHook {
        scan_gated: std::sync::atomic::AtomicBool,
        release_gated: std::sync::atomic::AtomicBool,
        scanned_barrier: std::sync::Barrier,
        after_drop_barrier: std::sync::Barrier,
        active_at_release: AtomicU64,
        in_flight_at_release: AtomicU64,
    }

    impl ReclaimRaceHook for GuardDropOrderHook {
        fn on_after_empty_scan(&self, _in_flight_work: u64) {
            if !self.scan_gated.swap(true, Ordering::SeqCst) {
                self.scanned_barrier.wait();
                // Wait until `drop(guard)` has completely finished and decremented
                // `in_flight_work` to 0 so `reclaim_and_reserve` must observe `transition_epoch`.
                self.after_drop_barrier.wait();
            }
        }

        fn on_after_guard_bytes_released(&self, active_bytes: u64, in_flight_work: u64) {
            if !self.release_gated.swap(true, Ordering::SeqCst) {
                self.active_at_release.store(active_bytes, Ordering::SeqCst);
                self.in_flight_at_release
                    .store(in_flight_work, Ordering::SeqCst);
            }
        }
    }

    #[test]
    fn sharded_key_prefix_sorter_keeps_in_flight_registered_until_guard_releases_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let shard_count = NonZeroU32::new(2).ok_or("nonzero shards")?;
        let global_limit = one_record_memory_limit::<2>(64);
        let hook = std::sync::Arc::new(GuardDropOrderHook {
            scan_gated: std::sync::atomic::AtomicBool::new(false),
            release_gated: std::sync::atomic::AtomicBool::new(false),
            scanned_barrier: std::sync::Barrier::new(2),
            after_drop_barrier: std::sync::Barrier::new(2),
            active_at_release: AtomicU64::new(u64::MAX),
            in_flight_at_release: AtomicU64::new(0),
        });
        let mut sorter = ShardedKeyPrefixSorter::<2>::new(
            dir.path(),
            "guard-drop-order",
            shard_count,
            global_limit,
            64_000,
        )?;
        sorter.set_reclaim_race_hook(std::sync::Arc::clone(&hook) as _);

        let guard = sorter.reserve_global_memory(global_limit)?;
        thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
            let hook_dropper = std::sync::Arc::clone(&hook);
            let dropper = scope.spawn(move || {
                hook_dropper.scanned_barrier.wait();
                drop(guard);
                hook_dropper.after_drop_barrier.wait();
            });
            let sorter_ref = &sorter;
            let pusher = scope.spawn(|| -> Result<(), BulkLoadError> {
                sorter_ref.push([0x80, 0x02], &[0xbb; 64])
            });
            dropper.join().map_err(|_| "dropper panicked")?;
            pusher.join().map_err(|_| "pusher panicked")??;
            Ok(())
        })?;

        assert_eq!(hook.active_at_release.load(Ordering::SeqCst), 0);
        assert_eq!(hook.in_flight_at_release.load(Ordering::SeqCst), 1);
        let finished = sorter.finish()?;
        assert_eq!(finished.evidence().record_count, 1);
        assert!(finished.evidence().peak_accounted_memory_bytes <= global_limit);
        Ok(())
    }

    struct ReserveRegistrationHook {
        reserve_gated: std::sync::atomic::AtomicBool,
        scan_gated: std::sync::atomic::AtomicBool,
        reserved_barrier: std::sync::Barrier,
        scanned_barrier: std::sync::Barrier,
        in_flight_at_reserve: AtomicU64,
        in_flight_at_scan: AtomicU64,
    }

    impl ReclaimRaceHook for ReserveRegistrationHook {
        fn on_after_reserve_before_guard_return(&self, in_flight_work: u64) {
            if !self.reserve_gated.swap(true, Ordering::SeqCst) {
                self.in_flight_at_reserve
                    .store(in_flight_work, Ordering::SeqCst);
                self.reserved_barrier.wait();
                self.scanned_barrier.wait();
            }
        }

        fn on_after_empty_scan(&self, in_flight_work: u64) {
            if !self.scan_gated.swap(true, Ordering::SeqCst) {
                self.in_flight_at_scan
                    .store(in_flight_work, Ordering::SeqCst);
                self.scanned_barrier.wait();
            }
        }
    }

    #[test]
    fn sharded_key_prefix_sorter_registers_in_flight_before_charging_reservation()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let shard_count = NonZeroU32::new(2).ok_or("nonzero shards")?;
        let global_limit = one_record_memory_limit::<2>(64);
        let hook = std::sync::Arc::new(ReserveRegistrationHook {
            reserve_gated: std::sync::atomic::AtomicBool::new(false),
            scan_gated: std::sync::atomic::AtomicBool::new(false),
            reserved_barrier: std::sync::Barrier::new(2),
            scanned_barrier: std::sync::Barrier::new(2),
            in_flight_at_reserve: AtomicU64::new(0),
            in_flight_at_scan: AtomicU64::new(0),
        });
        let mut sorter = ShardedKeyPrefixSorter::<2>::new(
            dir.path(),
            "reserve-before-guard",
            shard_count,
            global_limit,
            64_000,
        )?;
        let held = sorter.reserve_global_memory(global_limit)?;
        assert!(sorter.memory.try_reserve_in_flight(1).is_none());
        assert_eq!(sorter.memory.in_flight_work.load(Ordering::SeqCst), 1);
        drop(held);
        assert_eq!(sorter.memory.in_flight_work.load(Ordering::SeqCst), 0);
        assert_eq!(sorter.memory.active_bytes.load(Ordering::SeqCst), 0);

        sorter.set_reclaim_race_hook(std::sync::Arc::clone(&hook) as _);
        thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
            let sorter_ref = &sorter;
            let writer_a = scope.spawn(move || sorter_ref.push([0x00, 0x01], &[0xaa; 64]));
            let hook_b = std::sync::Arc::clone(&hook);
            let writer_b = scope.spawn(move || -> Result<(), BulkLoadError> {
                hook_b.reserved_barrier.wait();
                sorter_ref.push([0x80, 0x02], &[0xbb; 64])
            });
            writer_a.join().map_err(|_| "writer A panicked")??;
            writer_b.join().map_err(|_| "writer B panicked")??;
            Ok(())
        })?;

        assert_eq!(hook.in_flight_at_reserve.load(Ordering::SeqCst), 1);
        assert_eq!(hook.in_flight_at_scan.load(Ordering::SeqCst), 1);
        let finished = sorter.finish()?;
        let mut keys = Vec::new();
        for mut stream in finished.into_shards() {
            while let Some(record) = stream.next_record()? {
                keys.push(record.key);
            }
        }
        assert_eq!(keys, vec![[0x00, 0x01], [0x80, 0x02]]);
        Ok(())
    }

    #[test]
    fn sharded_key_prefix_sorter_falls_back_synchronously_when_worker_spawn_fails()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let shard_count = NonZeroU32::new(4).ok_or("nonzero shards")?;
        let single_record_bytes = one_record_memory_limit::<4>(8);
        let sorter = ShardedKeyPrefixSorter::<4>::new(
            dir.path(),
            "spawn-fallback",
            shard_count,
            single_record_bytes * 6,
            64_000,
        )?;
        let entries = [
            ([0xc0, 0x02, 0x00, 0x01], [0x44; 8]),
            ([0x40, 0x01, 0x00, 0x02], [0x22; 8]),
            ([0x80, 0x03, 0x00, 0x03], [0x33; 8]),
            ([0x00, 0x04, 0x00, 0x04], [0x11; 8]),
            ([0xc0, 0x01, 0x00, 0x05], [0x55; 8]),
            ([0x00, 0x02, 0x00, 0x06], [0x66; 8]),
        ];
        for (key, payload) in entries {
            sorter.push(key, &payload)?;
        }

        let _guard = arm_refused_sort_phase_thread(1);
        let finished = sorter.finish()?;
        assert_eq!(finished.shard_count(), 4);
        assert_eq!(finished.evidence().record_count, 6);
        let mut observed_keys = Vec::new();
        for mut stream in finished.into_shards() {
            while let Some(record) = stream.next_record()? {
                observed_keys.push(record.key);
            }
        }
        assert_eq!(
            observed_keys,
            vec![
                [0x00, 0x02, 0x00, 0x06],
                [0x00, 0x04, 0x00, 0x04],
                [0x40, 0x01, 0x00, 0x02],
                [0x80, 0x03, 0x00, 0x03],
                [0xc0, 0x01, 0x00, 0x05],
                [0xc0, 0x02, 0x00, 0x01],
            ]
        );
        Ok(())
    }
}
