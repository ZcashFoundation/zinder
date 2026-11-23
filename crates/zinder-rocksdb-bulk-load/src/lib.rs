//! Bulk-loading and SST mechanics shared by Zinder storage engines.
//!
//! This crate owns physical bulk-loading mechanics, not domain schemas,
//! publication state, source fences, or cross-engine storage abstractions.

mod bulk_load;

pub use bulk_load::{
    BulkLoadError, FixedRecordSorter, OrderedKeyValueEvidence, OrderedKeyValueEvidenceAccumulator,
    OrderedSstWriter, SortedVariableValues, SstFileEvidence, SstFileSet, VariableValueRecord,
    VariableValueSortDurations, VariableValueSortEvidence, VariableValueSorter,
    fixed_record_capacity,
};
