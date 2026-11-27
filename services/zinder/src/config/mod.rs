//! Unified configuration loading, role filtering, validation, and redacted rendering.

mod filter;
mod validate;

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
    path::PathBuf,
};

use thiserror::Error;
use zinder_core::Network;
use zinder_runtime::DeploymentProfile;

use crate::cli::{ConfigScopeArgs, Preset, ServiceRole};

pub use validate::render_combined_redacted_toml;

/// Selects whether `zinder` supervises a full serving preset or writer-only `index`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SupervisorMode {
    /// `zinder serve` preset mode.
    Serve,
    /// `zinder index` writer-only mode.
    Index,
}

/// Resolved scope options extracted from CLI subcommands (`serve`, `index`, `config`).
#[derive(Clone, Debug)]
pub struct ConfigScope {
    /// Execution mode (`serve` or `index`).
    pub mode: SupervisorMode,
    /// Explicit CLI `--preset` override, if passed.
    pub preset: Option<Preset>,
    /// Whether writer and projector roles are omitted.
    pub readers_only: bool,
    /// Whether `--full-blocks` forces `storage.full_blocks = true`.
    pub full_blocks: bool,
    /// Explicit CLI `--data-dir` override, if passed.
    pub data_dir: Option<PathBuf>,
    /// Optional combined `--config` TOML file path.
    pub config_path: Option<PathBuf>,
}

impl ConfigScope {
    /// Builds a serving scope from `zinder serve` or `zinder config` flags.
    #[must_use]
    pub fn serve(args: ConfigScopeArgs) -> Self {
        Self {
            mode: SupervisorMode::Serve,
            preset: args.preset,
            readers_only: args.readers_only,
            full_blocks: args.full_blocks,
            data_dir: args.data_dir,
            config_path: args.config_path,
        }
    }

    /// Builds a writer-only scope from `zinder index` flags.
    #[must_use]
    pub fn index(args: ConfigScopeArgs) -> Self {
        Self {
            mode: SupervisorMode::Index,
            preset: args.preset,
            readers_only: false,
            full_blocks: args.full_blocks,
            data_dir: args.data_dir,
            config_path: args.config_path,
        }
    }

    /// Returns `"serve"` or `"index"`.
    #[must_use]
    pub const fn mode_label(&self) -> &'static str {
        match self.mode {
            SupervisorMode::Serve => "serve",
            SupervisorMode::Index => "index",
        }
    }
}

/// Periodic catalog publication arguments passed to `zinder run history-publisher`.
#[derive(Clone, Debug)]
pub struct HistoryPublisherPlan {
    /// Target directory containing `current.json` and segments.
    pub directory: PathBuf,
    /// File containing the DER-encoded Ed25519 PKCS#8 private key.
    pub signing_key: PathBuf,
    /// Optional starting height.
    pub first_height: Option<u32>,
    /// Optional segment size in blocks.
    pub segment_blocks: Option<NonZeroU32>,
    /// Optional concurrent builder worker count.
    pub workers: Option<NonZeroUsize>,
    /// Poll interval in seconds when running continuously.
    pub follow_interval_secs: NonZeroU32,
}

/// Filtered per-role configuration material ready for validation or child launch.
#[derive(Clone, Debug)]
pub struct RoleLaunchSpec {
    /// Service role identifier.
    pub role: ServiceRole,
    /// Filtered TOML document rendered cleanly for this role.
    pub filtered_toml: String,
    /// Filtered `ZINDER_*` environment pairs scoped to this role.
    pub filtered_env: BTreeMap<String, String>,
    /// Whether supervisor `/readyz` requires this role to be ready.
    pub required_for_readyz: bool,
}

/// Complete validated supervision plan across all active preset roles.
#[derive(Clone, Debug)]
pub struct SupervisionPlan {
    /// Execution mode (`serve` or `index`).
    pub mode: SupervisorMode,
    /// Resolved deployment profile (`light-wallet`, `wallet`, or `explorer`).
    pub profile: DeploymentProfile,
    /// Resolved Zcash network.
    pub network: Network,
    /// Supervisor operational HTTP listen address, or `None` when disabled.
    pub ops_listen_addr: Option<SocketAddr>,
    /// Whether `storage.full_blocks` was enabled globally.
    pub full_blocks: bool,
    /// Canonical primary store path observed for startup gating.
    pub canonical_path: PathBuf,
    /// Wallet projection primary path observed for startup gating, if applicable.
    pub wallet_path: Option<PathBuf>,
    /// Internal `IngestControl` socket address when co-supervising writer and readers.
    pub ingest_control_addr: Option<SocketAddr>,
    /// Native `WalletQuery` socket address when explorer or cipherscan depends on it.
    pub query_listen_addr: Option<SocketAddr>,
    /// `ExplorerQuery` socket address when cipherscan depends on it.
    pub explorer_listen_addr: Option<SocketAddr>,
    /// Optional periodic shielded-history publication flags.
    pub history_publisher: Option<HistoryPublisherPlan>,
    /// Ordered per-role filtered configurations matching active startup sequence.
    pub roles: Vec<RoleLaunchSpec>,
    pub(crate) redacted_sections: validate::ValidatedRoleRenderings,
}

/// Error returned while loading, filtering, or validating unified `zinder` configuration.
#[derive(Debug, Error)]
pub enum UnifiedConfigError {
    /// Failed to read or parse the `--config` TOML file.
    #[error("failed to load configuration file {path}: {detail}")]
    ReadConfigFile {
        /// File path that could not be read or parsed.
        path: String,
        /// Underlying failure reason.
        detail: String,
    },
    /// Layered `zinder-runtime` config loader failure.
    #[error(transparent)]
    Config(#[from] zinder_runtime::ConfigError),
    /// Invalid unified CLI or coordination rule.
    #[error("{0}")]
    Invalid(String),
    /// Active role failed its authoritative configuration validation.
    #[error("invalid configuration for role `{role}`: {detail}")]
    RoleValidation {
        /// Role label whose configuration loader rejected the input.
        role: &'static str,
        /// Authoritative error message emitted by the role crate.
        detail: String,
    },
    /// Temporary directory creation failed during validation.
    #[error("failed to create temporary validation config directory: {0}")]
    TempIo(#[from] std::io::Error),
}

/// Loads, filters, cross-validates, and renders configuration for every active role.
pub fn prepare_supervision_plan(
    scope: &ConfigScope,
) -> Result<SupervisionPlan, UnifiedConfigError> {
    prepare_supervision_plan_with_env(scope, std::env::vars())
}

/// Variant of [`prepare_supervision_plan`] reading `ZINDER_*` vars from an explicit iterator.
pub fn prepare_supervision_plan_with_env<I>(
    scope: &ConfigScope,
    environment: I,
) -> Result<SupervisionPlan, UnifiedConfigError>
where
    I: IntoIterator<Item = (String, String)>,
{
    let env_entries: Vec<(String, String)> = environment.into_iter().collect();
    let mut plan = filter::build_supervision_plan(scope, &env_entries)?;
    plan.redacted_sections = validate::validate_all_roles(&plan)?;
    Ok(plan)
}
