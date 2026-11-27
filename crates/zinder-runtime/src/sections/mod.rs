//! Shared config sections used by every Zinder service binary.
//!
//! Each module owns one section of the canonical TOML layout
//! (`docs/reference/configuration.md`). Section structs are
//! consumed by the per-service `RawConfig` and the matching
//! [`crate::ConfigLoader`] helpers (`with_*_section`) wire the per-service
//! defaults so a new service cannot drift from the established schema.

pub mod defaults;
mod deployment;
mod ingest_control;
mod ops;
mod projector_control;
mod retention;
mod runtime_service;
mod security;
mod storage;

pub use deployment::{
    DeploymentProfile, DeploymentSection, DeploymentToml, resolve_manual_raw_blob_retention,
};
pub use ingest_control::{
    IngestControlReaderToml, IngestControlSection, IngestControlWriterToml,
    ResolvedIngestControlReader, ResolvedIngestControlWriter, resolve_ingest_control_reader,
    resolve_ingest_control_writer, resolve_ingest_control_writer_with_data_dir,
};
pub use ops::{OpsSection, OpsToml, resolve_ops_listen_addr};
pub use projector_control::{
    ProjectorControlSection, ProjectorControlToml, ResolvedProjectorControl,
    resolve_projector_control, resolve_projector_control_with_data_dir,
};
pub use retention::{ResolvedRetention, RetentionSection, RetentionToml, resolve_retention};
pub use runtime_service::RuntimeService;
pub use security::{
    DEFAULT_ALLOW_PUBLIC_BIND, SecuritySection, SecurityToml, resolve_allow_public_bind,
};
pub use storage::{
    CanonicalSecondaryStorageSection, CanonicalSecondaryStorageToml,
    ResolvedCanonicalSecondaryStorage, ResolvedSecondaryStorage, RocksDbResourceBudgetSection,
    RocksDbResourceBudgetToml, SecondaryStorageSection, SecondaryStorageToml, StorageRoleSection,
    StorageRoleToml, derive_canonical_path, derive_canonical_secondary_path,
    derive_checkpoints_path, derive_explorer_secondary_path, derive_wallet_path,
    derive_wallet_secondary_path, resolve_canonical_reader_rocksdb_budget,
    resolve_canonical_secondary_storage, resolve_canonical_writer_rocksdb_budget,
    resolve_materialized_view_reader_rocksdb_budget,
    resolve_materialized_view_writer_rocksdb_budget, resolve_secondary_storage,
    resolve_wallet_projection_reader_rocksdb_budget,
    resolve_wallet_projection_writer_rocksdb_budget, validate_optional_data_dir,
};
