//! Shared and role-scoped config sections forming [`crate::ZinderConfig`].
//!
//! Each module owns one section (or the role-specific sections in [`roles`]) of
//! the canonical TOML layout (`docs/reference/configuration.md`). Section
//! structs are assembled directly in [`crate::ZinderConfig`] and wired with
//! runtime defaults via [`crate::ConfigLoader`].

pub mod defaults;
mod deployment;
mod ingest_control;
mod observability;
mod ops;
mod projector_control;
mod retention;
mod roles;
mod runtime_service;
mod security;
mod storage;

pub use deployment::{DeploymentProfile, DeploymentSection, resolve_manual_raw_blob_retention};
pub use ingest_control::{
    IngestControlSection, ResolvedIngestControlReader, ResolvedIngestControlWriter,
    resolve_ingest_control_reader, resolve_ingest_control_writer,
    resolve_ingest_control_writer_with_data_dir,
};
pub use observability::ObservabilityConfig;
pub use ops::{OpsSection, resolve_ops_listen_addr};
pub use projector_control::{
    ProjectorControlSection, ResolvedProjectorControl, resolve_projector_control,
    resolve_projector_control_with_data_dir,
};
pub use retention::{ResolvedRetention, RetentionSection, resolve_retention};
pub use roles::{
    CipherscanSection, CompatSection, CompatServing, ExplorerSection, IngestConstructionSection,
    IngestCoverage, IngestFollowSection, IngestMempoolSection, IngestPhaseClassificationSection,
    IngestRunOverridesSection, IngestSection, IngestTreeStateDensificationSection,
    IngestTreeStateVerificationSection, ProjectorBuildSection, ProjectorFollowSection,
    ProjectorSection, QuerySection, TreeStateSelectionName,
};
pub use runtime_service::RuntimeService;
pub use security::{DEFAULT_ALLOW_PUBLIC_BIND, SecuritySection, resolve_allow_public_bind};
pub use storage::{
    ResolvedCanonicalSecondaryStorage, ResolvedSecondaryStorage, RocksDbResourceBudgetSection,
    StorageRoleSection, StorageSection, WalletSection, derive_canonical_path,
    derive_canonical_secondary_path, derive_checkpoints_path, derive_explorer_secondary_path,
    derive_wallet_path, derive_wallet_secondary_path, resolve_canonical_reader_rocksdb_budget,
    resolve_canonical_secondary_storage, resolve_canonical_writer_rocksdb_budget,
    resolve_materialized_view_reader_rocksdb_budget,
    resolve_materialized_view_writer_rocksdb_budget, resolve_secondary_storage,
    resolve_wallet_projection_reader_rocksdb_budget,
    resolve_wallet_projection_writer_rocksdb_budget, validate_optional_data_dir,
};
