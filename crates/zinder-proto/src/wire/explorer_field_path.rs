//! `UnavailableField.field_path` values that Explorer responses publish.
//!
//! Producers and consumers of `ExplorerFreshness.unavailable` match these
//! paths exactly, so each path lives here once.

/// `UnavailableField.field_path` naming every
/// `TransactionDetailResponse.transparent_outputs[].spent_by` relation.
///
/// Its presence means the response does not say which outputs are spent; an
/// absent `spent_by` then means unknown rather than unspent.
pub const TRANSPARENT_OUTPUT_SPENT_BY_FIELD_PATH: &str = "transparent_outputs.spent_by";
