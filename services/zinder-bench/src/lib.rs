//! Fixed-range storage benchmark harness for Zinder.
//!
//! This crate captures immutable source fixtures, replays them into the
//! production canonical store, and compares concrete `RocksDB` and `PostgreSQL`
//! round trips over the same backend-neutral canonical block facts. Each
//! measurement owns an explicit report shape so a block-local replay diagnostic cannot
//! claim a production canonical or projection lifecycle.
//!
//! The harness is a standalone binary and is not linked into production
//! services.

pub mod canonical_fixture_replay;
pub mod canonical_fixture_transport_server;
pub mod canonical_replay_storage;
pub mod capture;
pub mod error;
pub mod fixture;
pub mod metrics_scrape;
pub mod recorder;
pub mod report;
pub mod rss;

pub use error::BenchError;
