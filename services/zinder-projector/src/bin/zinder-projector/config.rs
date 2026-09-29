//! Configuration loading for the `zinder-projector` binary.

use std::{
    net::SocketAddr,
    num::NonZeroU64,
    path::{Component, Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use zinder_core::Network;
use zinder_runtime::{
    BearerToken, ConfigError, ConfigLoader, IngestControlReaderToml, IngestControlSection,
    NetworkSection, NetworkToml, NodeToml, OpsSection, OpsToml, ProjectorControlSection,
    ProjectorControlToml, RocksDbResourceBudgetSection, RocksDbResourceBudgetToml, SecuritySection,
    SecurityToml, StorageRoleSection, guard_optional_serving_bind, require_field,
    resolve_allow_public_bind, resolve_canonical_reader_rocksdb_budget,
    resolve_ingest_control_reader, resolve_ops_listen_addr, resolve_projector_control,
    resolve_wallet_projection_writer_rocksdb_budget,
};
use zinder_source::{NodeSection, NodeTarget};
use zinder_store::RocksDbResourceBudget;

const DEFAULT_CANONICAL_PATH: &str = "/var/lib/zinder/canonical";
const DEFAULT_CANONICAL_SECONDARY_PATH: &str = "/var/lib/zinder/projector/canonical-secondary";
const DEFAULT_WALLET_PATH: &str = "/var/lib/zinder/wallet";
const DEFAULT_OPS_LISTEN_ADDR: &str = "127.0.0.1:9110";
const DEFAULT_REORG_WINDOW_BLOCKS: u32 = 100;
const DEFAULT_FENCE_CONVERGENCE_TIMEOUT_SECONDS: u64 = 30;
/// ADR-0035 permits at most two hours for wallet construction after canonical.
///
/// The builder can heartbeat only at durable phase boundaries, so its lease
/// must cover two complete hard-gate windows rather than relying on a periodic
/// renewal that does not exist.
const MINIMUM_LEASE_DURATION_SECONDS: u64 = 4 * 60 * 60;
const DEFAULT_OUTPOINT_SORT_MEMORY_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const DEFAULT_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER: u64 = 1024 * 1024 * 1024;
const DEFAULT_TEMPORARY_FILE_BYTES_PER_SORTER: u64 = 64 * 1024 * 1024 * 1024;
const DEFAULT_SST_TARGET_LOGICAL_BYTES: u64 = 128 * 1024 * 1024;
const DEFAULT_ACCOUNTED_REORG_UNDO_BYTES: u64 = 512 * 1024 * 1024;
/// One following transition may account for at most this many logical bytes
/// across its planner overlay and durable write batch.
const DEFAULT_FOLLOW_MAX_TRANSITION_LOGICAL_BYTES: u64 = 512 * 1024 * 1024;
/// The single-host production envelope reserves the rest of its 10 GiB budget
/// for canonical/wallet `RocksDB`, replay rows, and builder spill files.
const MAX_FOLLOW_MAX_TRANSITION_LOGICAL_BYTES: u64 =
    zinder_wallet_rocksdb::MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES;

/// Resolved release configuration.
#[derive(Clone, Debug)]
pub(crate) struct ProjectorConfig {
    pub(crate) network: Network,
    pub(crate) canonical_path: PathBuf,
    pub(crate) canonical_secondary_path: PathBuf,
    pub(crate) wallet_path: PathBuf,
    pub(crate) canonical_rocksdb_budget: RocksDbResourceBudget,
    pub(crate) wallet_rocksdb_budget: RocksDbResourceBudget,
    pub(crate) reorg_window_blocks: u32,
    pub(crate) build_owner: [u8; 16],
    pub(crate) lease_duration: Duration,
    pub(crate) fence_convergence_timeout: Duration,
    pub(crate) build: ProjectorBuildConfig,
    pub(crate) follow: ProjectorFollowConfig,
    pub(crate) ingest_control_addr: String,
    pub(crate) ingest_control_bearer_token_path: Option<PathBuf>,
    pub(crate) ingest_control_bearer_token: Option<BearerToken>,
    pub(crate) projector_control: zinder_runtime::ResolvedProjectorControl,
    pub(crate) node: NodeTarget,
    pub(crate) ops_listen_addr: Option<SocketAddr>,
    pub(crate) allow_public_bind: bool,
}

/// Bounded resources for one fixed-tip wallet construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProjectorBuildConfig {
    pub(crate) max_outpoint_sort_memory_bytes: u64,
    pub(crate) max_secondary_sort_memory_bytes_per_sorter: u64,
    pub(crate) max_temporary_file_bytes_per_sorter: u64,
    pub(crate) sst_target_logical_bytes: u64,
    pub(crate) max_accounted_reorg_undo_bytes: u64,
}

/// Bounded resources for one atomic continuous-following transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProjectorFollowConfig {
    /// Logical planner and write-batch bytes permitted per transition.
    pub(crate) max_transition_logical_bytes: NonZeroU64,
}

/// CLI overrides layered above file and environment configuration.
#[derive(Debug, Default)]
pub(crate) struct ProjectorConfigOverrides {
    pub(crate) network: Option<String>,
    pub(crate) canonical_path: Option<PathBuf>,
    pub(crate) canonical_secondary_path: Option<PathBuf>,
    pub(crate) wallet_path: Option<PathBuf>,
    pub(crate) reorg_window_blocks: Option<u32>,
    pub(crate) build_owner_hex: Option<String>,
    pub(crate) lease_duration_seconds: Option<u64>,
    pub(crate) node_json_rpc_addr: Option<String>,
    pub(crate) ingest_control_addr: Option<String>,
    pub(crate) ingest_control_bearer_token_path: Option<PathBuf>,
    pub(crate) projector_control_listen_addr: Option<SocketAddr>,
    pub(crate) projector_control_bearer_token_path: Option<PathBuf>,
    pub(crate) projector_control_checkpoint_staging_root: Option<PathBuf>,
    pub(crate) ops_listen_addr: Option<SocketAddr>,
}

/// Startup or runtime error surfaced by the release binary.
#[derive(Debug, Error)]
pub(crate) enum ProjectorError {
    #[error(transparent)]
    Config(#[from] ConfigError),

    #[error("node source initialization failed: {0}")]
    NodeConfig(#[from] zinder_source::NodeConfigError),

    #[error("node source request failed: {0}")]
    Source(#[from] zinder_source::SourceError),

    #[error(transparent)]
    CanonicalStore(#[from] zinder_store::CanonicalStoreError),

    #[error(transparent)]
    WalletStore(#[from] zinder_wallet_rocksdb::RocksDbWalletError),

    #[error(transparent)]
    CanonicalControl(#[from] crate::canonical_writer_control::CanonicalWriterControlError),

    #[error("projector build task failed: {0}")]
    BuildTask(#[from] tokio::task::JoinError),

    #[error("projector owner control could not bind {address}: {source}")]
    ProjectorControlBind {
        /// Configured private-control address that could not be reserved.
        address: SocketAddr,
        /// Concrete listener failure.
        #[source]
        source: std::io::Error,
    },

    #[error("projector owner control server failed: {0}")]
    ProjectorControlServer(tonic::transport::Error),

    #[error("projector owner control server stopped unexpectedly")]
    ProjectorControlStopped,

    #[error("enabled projector owner control omitted its resolved bearer token")]
    ProjectorControlTokenMissing,

    #[error("canonical secondary did not reach the writer fence before the convergence deadline")]
    CanonicalFenceDidNotConverge,

    #[error(
        "canonical secondary at event {secondary_event_sequence} diverged from writer fence at event {writer_event_sequence}"
    )]
    CanonicalFenceDiverged {
        /// Event sequence of the projector's canonical secondary.
        secondary_event_sequence: u64,
        /// Event sequence reported by the canonical writer.
        writer_event_sequence: u64,
    },

    #[error("constructed wallet source differs from its fixed canonical construction fence")]
    WalletConstructionFenceMismatch,

    #[error("canonical retained-event page is invalid: {reason}")]
    CanonicalEventPageInvalid {
        /// Stable validation reason without untrusted event contents.
        reason: &'static str,
    },

    #[error(
        "wallet event cursor {wallet_event_sequence} expired before retained-event floor {oldest_retained_event_sequence}"
    )]
    CanonicalEventCursorExpired {
        /// Persisted READY wallet cursor that could not be resumed.
        wallet_event_sequence: u64,
        /// First retained event still available from the canonical writer.
        oldest_retained_event_sequence: u64,
    },

    #[error(
        "wallet cursor {wallet_event_sequence} is no longer retained (floor {oldest_retained_event_sequence}); rebuild the wallet in a separately provisioned side-by-side lane before replacing this projection"
    )]
    WalletRebuildRequired {
        /// Persisted READY wallet cursor that cannot be replayed from the writer.
        wallet_event_sequence: u64,
        /// First retained event still available from the canonical writer.
        oldest_retained_event_sequence: u64,
    },

    #[error(
        "wallet reconciliation requires more than {maximum_events} retained events in one bounded page"
    )]
    CanonicalReconciliationEventPageTooLarge {
        /// Hard per-reconciliation retained-event page ceiling.
        maximum_events: u32,
    },

    #[error(
        "wallet reconciliation replay suffix has {requested_blocks} blocks, exceeding the bounded maximum {maximum_blocks}"
    )]
    CanonicalReconciliationReplayTooLarge {
        /// Number of current canonical blocks needed after the common ancestor.
        requested_blocks: u32,
        /// Hard per-reconciliation replay ceiling.
        maximum_blocks: u32,
    },

    #[error(
        "wallet and current canonical history have no verified common ancestor in the retained undo suffix"
    )]
    CanonicalCommonAncestorUnavailable,

    #[error("could not generate a unique canonical retention lease identity: {0}")]
    RetentionLeaseEntropy(#[from] getrandom::Error),
}

fn projector_config_defaults() -> Result<ConfigLoader, ConfigError> {
    ConfigLoader::new()
        .with_default("storage.canonical_path", DEFAULT_CANONICAL_PATH)?
        .with_default(
            "storage.canonical_secondary_path",
            DEFAULT_CANONICAL_SECONDARY_PATH,
        )?
        .with_default("wallet.path", DEFAULT_WALLET_PATH)?
        .with_default("projector.reorg_window_blocks", DEFAULT_REORG_WINDOW_BLOCKS)?
        .with_default(
            "projector.fence_convergence_timeout_seconds",
            DEFAULT_FENCE_CONVERGENCE_TIMEOUT_SECONDS,
        )?
        .with_default(
            "projector.build.max_outpoint_sort_memory_bytes",
            DEFAULT_OUTPOINT_SORT_MEMORY_BYTES,
        )?
        .with_default(
            "projector.build.max_secondary_sort_memory_bytes_per_sorter",
            DEFAULT_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER,
        )?
        .with_default(
            "projector.build.max_temporary_file_bytes_per_sorter",
            DEFAULT_TEMPORARY_FILE_BYTES_PER_SORTER,
        )?
        .with_default(
            "projector.build.sst_target_logical_bytes",
            DEFAULT_SST_TARGET_LOGICAL_BYTES,
        )?
        .with_default(
            "projector.build.max_accounted_reorg_undo_bytes",
            DEFAULT_ACCOUNTED_REORG_UNDO_BYTES,
        )?
        .with_default(
            "projector.follow.max_transition_logical_bytes",
            DEFAULT_FOLLOW_MAX_TRANSITION_LOGICAL_BYTES,
        )?
        .with_default("ops.listen_addr", DEFAULT_OPS_LISTEN_ADDR)?
        .with_default("ingest_control.addr", "http://127.0.0.1:9100")?
        .with_security_section()
}

/// Loads and validates projector configuration.
pub(crate) fn load_projector_config(
    config_path: Option<PathBuf>,
    overrides: ProjectorConfigOverrides,
) -> Result<ProjectorConfig, ProjectorError> {
    let raw: ProjectorRawConfig = projector_config_defaults()?
        .with_file(config_path)
        .with_zinder_env()?
        .with_override_if("network.name", overrides.network)?
        .with_override_path_if("storage.canonical_path", overrides.canonical_path)?
        .with_override_path_if(
            "storage.canonical_secondary_path",
            overrides.canonical_secondary_path,
        )?
        .with_override_path_if("wallet.path", overrides.wallet_path)?
        .with_override_if(
            "projector.reorg_window_blocks",
            overrides.reorg_window_blocks,
        )?
        .with_override_if("projector.build_owner_hex", overrides.build_owner_hex)?
        .with_override_if(
            "projector.lease_duration_seconds",
            overrides.lease_duration_seconds,
        )?
        .with_override_if("node.json_rpc_addr", overrides.node_json_rpc_addr)?
        .with_override_if("ingest_control.addr", overrides.ingest_control_addr)?
        .with_override_path_if(
            "ingest_control.bearer_token_path",
            overrides.ingest_control_bearer_token_path,
        )?
        .with_override_if(
            "projector_control.listen_addr",
            overrides
                .projector_control_listen_addr
                .map(|address| address.to_string()),
        )?
        .with_override_path_if(
            "projector_control.bearer_token_path",
            overrides.projector_control_bearer_token_path,
        )?
        .with_override_path_if(
            "projector_control.checkpoint_staging_root",
            overrides.projector_control_checkpoint_staging_root,
        )?
        .with_override_if(
            "ops.listen_addr",
            overrides.ops_listen_addr.map(|address| address.to_string()),
        )?
        .load()?;

    resolve_projector_config(raw)
}

/// Renders the complete accepted config shape without exposing auth material.
pub(crate) fn projector_config_toml(config: &ProjectorConfig) -> Result<String, ProjectorError> {
    toml::to_string(&ProjectorConfigToml::from_config(config))
        .map_err(|source| ConfigError::Render { source })
        .map_err(ProjectorError::from)
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ProjectorRawConfig {
    network: NetworkSection,
    storage: ProjectorStorageSection,
    wallet: ProjectorWalletSection,
    projector: ProjectorSection,
    node: NodeSection,
    ingest_control: IngestControlSection,
    projector_control: ProjectorControlSection,
    ops: OpsSection,
    security: SecuritySection,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ProjectorStorageSection {
    canonical_path: Option<PathBuf>,
    canonical_secondary_path: Option<PathBuf>,
    canonical: StorageRoleSection,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ProjectorWalletSection {
    path: Option<PathBuf>,
    rocksdb: RocksDbResourceBudgetSection,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ProjectorSection {
    reorg_window_blocks: Option<u32>,
    build_owner_hex: Option<String>,
    lease_duration_seconds: Option<u64>,
    fence_convergence_timeout_seconds: Option<u64>,
    build: ProjectorBuildSection,
    follow: ProjectorFollowSection,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ProjectorBuildSection {
    max_outpoint_sort_memory_bytes: Option<u64>,
    max_secondary_sort_memory_bytes_per_sorter: Option<u64>,
    max_temporary_file_bytes_per_sorter: Option<u64>,
    sst_target_logical_bytes: Option<u64>,
    max_accounted_reorg_undo_bytes: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ProjectorFollowSection {
    max_transition_logical_bytes: Option<u64>,
}

fn resolve_projector_config(raw: ProjectorRawConfig) -> Result<ProjectorConfig, ProjectorError> {
    let network = raw.network.resolve()?;
    let canonical_path = require_field(raw.storage.canonical_path, "storage.canonical_path")?;
    let canonical_secondary_path = require_field(
        raw.storage.canonical_secondary_path,
        "storage.canonical_secondary_path",
    )?;
    let wallet_path = require_field(raw.wallet.path, "wallet.path")?;
    require_distinct_paths(&canonical_path, &canonical_secondary_path, &wallet_path)?;
    let canonical_rocksdb_budget =
        resolve_canonical_reader_rocksdb_budget(raw.storage.canonical.rocksdb)?;
    let wallet_rocksdb_budget =
        resolve_wallet_projection_writer_rocksdb_budget(raw.wallet.rocksdb)?;
    let reorg_window_blocks = require_nonzero_u32(
        raw.projector.reorg_window_blocks,
        "projector.reorg_window_blocks",
    )?;
    let build_owner_encoded =
        require_field(raw.projector.build_owner_hex, "projector.build_owner_hex")?;
    let build_owner = parse_build_owner(&build_owner_encoded)?;
    let lease_duration_seconds = require_minimum_u64(
        raw.projector.lease_duration_seconds,
        "projector.lease_duration_seconds",
        MINIMUM_LEASE_DURATION_SECONDS,
    )?;
    let fence_convergence_timeout_seconds = require_nonzero_u64(
        raw.projector.fence_convergence_timeout_seconds,
        "projector.fence_convergence_timeout_seconds",
    )?;
    let build = resolve_build_config(&raw.projector.build)?;
    let follow = resolve_follow_config(&raw.projector.follow)?;
    let node = NodeTarget::resolve(network, raw.node)?;
    let ingest_control = resolve_ingest_control_reader(raw.ingest_control)?;
    let projector_control = resolve_projector_control(raw.projector_control)?;
    let ops_listen_addr = resolve_ops_listen_addr(raw.ops)?;
    let allow_public_bind = resolve_allow_public_bind(raw.security)?;
    guard_optional_serving_bind("ops.listen_addr", ops_listen_addr, allow_public_bind)?;

    Ok(ProjectorConfig {
        network,
        canonical_path,
        canonical_secondary_path,
        wallet_path,
        canonical_rocksdb_budget,
        wallet_rocksdb_budget,
        reorg_window_blocks,
        build_owner,
        lease_duration: Duration::from_secs(lease_duration_seconds),
        fence_convergence_timeout: Duration::from_secs(fence_convergence_timeout_seconds),
        build,
        follow,
        ingest_control_addr: ingest_control.addr,
        ingest_control_bearer_token_path: ingest_control.bearer_token_path,
        ingest_control_bearer_token: ingest_control.bearer_token,
        projector_control,
        node,
        ops_listen_addr,
        allow_public_bind,
    })
}

fn resolve_build_config(raw: &ProjectorBuildSection) -> Result<ProjectorBuildConfig, ConfigError> {
    Ok(ProjectorBuildConfig {
        max_outpoint_sort_memory_bytes: require_nonzero_u64(
            raw.max_outpoint_sort_memory_bytes,
            "projector.build.max_outpoint_sort_memory_bytes",
        )?,
        max_secondary_sort_memory_bytes_per_sorter: require_nonzero_u64(
            raw.max_secondary_sort_memory_bytes_per_sorter,
            "projector.build.max_secondary_sort_memory_bytes_per_sorter",
        )?,
        max_temporary_file_bytes_per_sorter: require_nonzero_u64(
            raw.max_temporary_file_bytes_per_sorter,
            "projector.build.max_temporary_file_bytes_per_sorter",
        )?,
        sst_target_logical_bytes: require_nonzero_u64(
            raw.sst_target_logical_bytes,
            "projector.build.sst_target_logical_bytes",
        )?,
        max_accounted_reorg_undo_bytes: require_nonzero_u64(
            raw.max_accounted_reorg_undo_bytes,
            "projector.build.max_accounted_reorg_undo_bytes",
        )?,
    })
}

fn resolve_follow_config(
    raw: &ProjectorFollowSection,
) -> Result<ProjectorFollowConfig, ConfigError> {
    let max_transition_logical_bytes = require_nonzero_u64(
        raw.max_transition_logical_bytes,
        "projector.follow.max_transition_logical_bytes",
    )?;
    if max_transition_logical_bytes > MAX_FOLLOW_MAX_TRANSITION_LOGICAL_BYTES {
        return Err(ConfigError::invalid(format!(
            "projector.follow.max_transition_logical_bytes must not exceed {MAX_FOLLOW_MAX_TRANSITION_LOGICAL_BYTES} bytes"
        )));
    }
    let Some(max_transition_logical_bytes) = NonZeroU64::new(max_transition_logical_bytes) else {
        return Err(ConfigError::invalid(
            "projector.follow.max_transition_logical_bytes must be greater than zero",
        ));
    };
    Ok(ProjectorFollowConfig {
        max_transition_logical_bytes,
    })
}

fn require_distinct_paths(
    canonical: &Path,
    canonical_secondary: &Path,
    wallet: &Path,
) -> Result<(), ConfigError> {
    let paths = [canonical, canonical_secondary, wallet];
    let identities = paths
        .iter()
        .map(|path| normalized_storage_path_identity(path))
        .collect::<Result<Vec<_>, _>>()?;
    for (index, path) in identities.iter().enumerate() {
        if identities[index + 1..]
            .iter()
            .any(|other| other == path || other.starts_with(path) || path.starts_with(other))
        {
            return Err(ConfigError::invalid(
                "storage.canonical_path, storage.canonical_secondary_path, and wallet.path must be disjoint roots",
            ));
        }
    }
    Ok(())
}

/// Resolves a configured storage path to the lexical absolute identity the
/// projector will use, without requiring its secondary directory to exist.
///
/// The projector must reject `./`, `..`, and nested aliases before `RocksDB`
/// creates secondary metadata. Filesystem canonicalization alone is
/// insufficient because the secondary directory is intentionally absent on
/// first startup.
fn normalized_storage_path_identity(path: &Path) -> Result<PathBuf, ConfigError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|source| {
                ConfigError::invalid(format!(
                    "could not resolve relative storage path {}: {source}",
                    path.display()
                ))
            })?
            .join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                let _was_popped = normalized.pop();
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    Ok(normalized)
}

fn parse_build_owner(encoded: &str) -> Result<[u8; 16], ConfigError> {
    if encoded.len() != 32 || !encoded.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ConfigError::invalid(
            "projector.build_owner_hex must be exactly 32 hexadecimal characters",
        ));
    }
    let mut owner = [0_u8; 16];
    for (index, chunk) in encoded.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(chunk).map_err(|_| {
            ConfigError::invalid(
                "projector.build_owner_hex must be exactly 32 hexadecimal characters",
            )
        })?;
        owner[index] = u8::from_str_radix(text, 16).map_err(|_| {
            ConfigError::invalid(
                "projector.build_owner_hex must be exactly 32 hexadecimal characters",
            )
        })?;
    }
    Ok(owner)
}

fn require_nonzero_u32(configured: Option<u32>, field: &'static str) -> Result<u32, ConfigError> {
    let configured = require_field(configured, field)?;
    if configured == 0 {
        return Err(ConfigError::invalid(format!(
            "{field} must be greater than zero"
        )));
    }
    Ok(configured)
}

fn require_nonzero_u64(configured: Option<u64>, field: &'static str) -> Result<u64, ConfigError> {
    let configured = require_field(configured, field)?;
    if configured == 0 {
        return Err(ConfigError::invalid(format!(
            "{field} must be greater than zero"
        )));
    }
    Ok(configured)
}

fn require_minimum_u64(
    configured: Option<u64>,
    field: &'static str,
    minimum: u64,
) -> Result<u64, ConfigError> {
    let configured = require_field(configured, field)?;
    if configured < minimum {
        return Err(ConfigError::invalid(format!(
            "{field} must be at least {minimum} seconds to cover the construction hard gate"
        )));
    }
    Ok(configured)
}

#[derive(Serialize)]
struct ProjectorConfigToml {
    network: NetworkToml,
    storage: ProjectorStorageToml,
    wallet: ProjectorWalletToml,
    projector: ProjectorToml,
    node: NodeToml,
    ingest_control: IngestControlReaderToml,
    projector_control: ProjectorControlToml,
    ops: OpsToml,
    security: SecurityToml,
}

impl ProjectorConfigToml {
    fn from_config(config: &ProjectorConfig) -> Self {
        Self {
            network: NetworkToml::from_network(config.network),
            storage: ProjectorStorageToml {
                canonical_path: config.canonical_path.clone(),
                canonical_secondary_path: config.canonical_secondary_path.clone(),
                canonical: ProjectorStorageRoleToml::from_budget(config.canonical_rocksdb_budget),
            },
            wallet: ProjectorWalletToml {
                path: config.wallet_path.clone(),
                rocksdb: RocksDbResourceBudgetToml::from_resolved(config.wallet_rocksdb_budget),
            },
            projector: ProjectorToml {
                reorg_window_blocks: config.reorg_window_blocks,
                build_owner_hex: encode_hex(config.build_owner),
                lease_duration_seconds: config.lease_duration.as_secs(),
                fence_convergence_timeout_seconds: config.fence_convergence_timeout.as_secs(),
                build: ProjectorBuildToml::from(config.build),
                follow: ProjectorFollowToml::from(config.follow),
            },
            node: NodeToml::from_node_target(&config.node),
            ingest_control: IngestControlReaderToml::from_resolved(
                config.ingest_control_addr.clone(),
                config.ingest_control_bearer_token_path.as_deref(),
            ),
            projector_control: ProjectorControlToml::from_resolved(
                config.projector_control.listen_addr,
                config.projector_control.bearer_token_path.as_deref(),
                &config.projector_control.checkpoint_staging_root,
            ),
            ops: OpsToml::from_resolved(config.ops_listen_addr),
            security: SecurityToml::from_resolved(config.allow_public_bind),
        }
    }
}

#[derive(Serialize)]
struct ProjectorStorageToml {
    canonical_path: PathBuf,
    canonical_secondary_path: PathBuf,
    canonical: ProjectorStorageRoleToml,
}

#[derive(Serialize)]
struct ProjectorWalletToml {
    path: PathBuf,
    rocksdb: RocksDbResourceBudgetToml,
}

#[derive(Serialize)]
struct ProjectorStorageRoleToml {
    rocksdb: RocksDbResourceBudgetToml,
}

impl ProjectorStorageRoleToml {
    const fn from_budget(budget: RocksDbResourceBudget) -> Self {
        Self {
            rocksdb: RocksDbResourceBudgetToml::from_resolved(budget),
        }
    }
}

#[derive(Serialize)]
struct ProjectorToml {
    reorg_window_blocks: u32,
    build_owner_hex: String,
    lease_duration_seconds: u64,
    fence_convergence_timeout_seconds: u64,
    build: ProjectorBuildToml,
    follow: ProjectorFollowToml,
}

#[derive(Serialize)]
struct ProjectorBuildToml {
    max_outpoint_sort_memory_bytes: u64,
    max_secondary_sort_memory_bytes_per_sorter: u64,
    max_temporary_file_bytes_per_sorter: u64,
    sst_target_logical_bytes: u64,
    max_accounted_reorg_undo_bytes: u64,
}

impl From<ProjectorBuildConfig> for ProjectorBuildToml {
    fn from(config: ProjectorBuildConfig) -> Self {
        Self {
            max_outpoint_sort_memory_bytes: config.max_outpoint_sort_memory_bytes,
            max_secondary_sort_memory_bytes_per_sorter: config
                .max_secondary_sort_memory_bytes_per_sorter,
            max_temporary_file_bytes_per_sorter: config.max_temporary_file_bytes_per_sorter,
            sst_target_logical_bytes: config.sst_target_logical_bytes,
            max_accounted_reorg_undo_bytes: config.max_accounted_reorg_undo_bytes,
        }
    }
}

#[derive(Serialize)]
struct ProjectorFollowToml {
    max_transition_logical_bytes: u64,
}

impl From<ProjectorFollowConfig> for ProjectorFollowToml {
    fn from(config: ProjectorFollowConfig) -> Self {
        Self {
            max_transition_logical_bytes: config.max_transition_logical_bytes.get(),
        }
    }
}

fn encode_hex(bytes: [u8; 16]) -> String {
    use std::fmt::Write as _;
    let mut encoded = String::with_capacity(32);
    for byte in bytes {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_FOLLOW_MAX_TRANSITION_LOGICAL_BYTES, MAX_FOLLOW_MAX_TRANSITION_LOGICAL_BYTES,
        ProjectorFollowSection, resolve_follow_config,
    };

    #[test]
    fn follow_transition_budget_defaults_within_the_single_host_envelope()
    -> Result<(), zinder_runtime::ConfigError> {
        let resolved = resolve_follow_config(&ProjectorFollowSection {
            max_transition_logical_bytes: Some(DEFAULT_FOLLOW_MAX_TRANSITION_LOGICAL_BYTES),
        })?;

        assert_eq!(
            resolved.max_transition_logical_bytes.get(),
            DEFAULT_FOLLOW_MAX_TRANSITION_LOGICAL_BYTES
        );
        Ok(())
    }

    #[test]
    fn follow_transition_budget_refuses_the_unbounded_single_host_value() {
        assert!(
            resolve_follow_config(&ProjectorFollowSection {
                max_transition_logical_bytes: Some(
                    MAX_FOLLOW_MAX_TRANSITION_LOGICAL_BYTES.saturating_add(1),
                ),
            })
            .is_err()
        );
    }
}
