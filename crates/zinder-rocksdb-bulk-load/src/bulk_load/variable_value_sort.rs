//! Bounded external sorting for fixed-width keys with variable encoded values.

use parking_lot::Mutex;
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    mem::size_of,
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
use tempfile::{Builder as TemporaryDirectoryBuilder, TempDir};

#[cfg(test)]
use super::MAX_MERGE_FAN_IN;
use super::{BulkLoadError, effective_merge_fan_in, path_unavailable, validate_merge_fan_in};

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
}
