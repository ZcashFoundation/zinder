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

/// `UnavailableField.field_path` naming `OverviewSnapshotResponse.mempool`.
pub const OVERVIEW_MEMPOOL_FIELD_PATH: &str = "mempool";

/// `UnavailableField.field_path` naming
/// `OverviewSnapshotResponse.mempool_events`.
pub const OVERVIEW_MEMPOOL_EVENTS_FIELD_PATH: &str = "mempool_events";

/// `UnavailableField.field_path` naming `OverviewSnapshotResponse.value_pools`.
pub const OVERVIEW_VALUE_POOLS_FIELD_PATH: &str = "value_pools";
