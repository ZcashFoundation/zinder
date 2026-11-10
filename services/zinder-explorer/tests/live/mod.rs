//! Live federation tests for the materialized-view balance surface.
//!
//! Each test is double-gated by `#[ignore = LIVE_TEST_IGNORE_REASON]` and a
//! runtime `zinder_testkit::live::require_live*` call, and reads the unified
//! `ZINDER_NETWORK` + `ZINDER_NODE__*` env-var schema rather than inventing a
//! parallel test-only namespace.

mod mempool_federation;
mod search_federation;
mod value_pool_summary_federation;
