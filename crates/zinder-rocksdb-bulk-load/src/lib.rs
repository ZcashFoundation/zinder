//! Bulk-loading and SST mechanics shared by Zinder storage engines.
//!
//! This crate owns physical bulk-loading mechanics, not domain schemas,
//! publication state, source fences, or cross-engine storage abstractions.

mod bulk_load;

pub use bulk_load::{
    BulkLoadError, FixedRecordSorter, MAX_KEY_PREFIX_SHARD_COUNT, OrderedKeyValueEvidence,
    OrderedKeyValueEvidenceAccumulator, OrderedSstWriter, ParallelVariableValueSorter,
    ShardedKeyPrefixSorter, ShardedKeyPrefixWriter, ShardedSortBudget, ShardedSortedVariableValues,
    SortedVariableValues, SstFileEvidence, SstFileSet, StagedParallelVariableRuns,
    VariableValueRecord, VariableValueSortDurations, VariableValueSortEvidence,
    VariableValueSortShard, VariableValueSorter, active_merge_fan_in, fixed_record_capacity,
    key_prefix_shard_index,
};
