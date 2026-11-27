//! Command-line parser definitions for the unified `zinder` binary.

use std::{ffi::OsString, net::SocketAddr, num::NonZeroU32, path::PathBuf};

use clap::{Args, Parser, Subcommand, ValueEnum};
use zinder_ingest::runtime::{
    IngestConfigOverrides,
    edge_snapshot::{BootstrapArgs, ExportArgs, SnapshotCommand},
};
use zinder_projector::runtime::{
    config::ProjectorConfigOverrides,
    recovery::{ArchiveArgs, RecoveryCommand, ValidationArgs},
};
use zinder_runtime::DeploymentProfile;

/// Top-level command-line interface for the unified `zinder` runtime.
#[derive(Debug, Parser)]
#[command(name = "zinder")]
#[command(about = "Unified Zinder Zcash indexing, projection, and wallet serving runtime")]
#[command(version)]
pub struct Cli {
    /// Subcommand to run.
    #[command(subcommand)]
    pub command: Command,
}

/// Top-level `zinder` subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Supervise the full set of roles for the selected deployment preset.
    Serve(ServeArgs),
    /// Run the canonical writer and operational health endpoint.
    Index(IndexArgs),
    /// Run a single split-process service role directly.
    Run(RunArgs),
    /// Export or bootstrap signed canonical edge snapshots.
    Snapshot(SnapshotArgs),
    /// Create, verify, or restore coherent private wallet backups.
    Backup(BackupArgs),
    /// Print or validate effective configuration across active roles.
    Config(ConfigArgs),
    /// Probe the operational `/healthz` or `/readyz` HTTP endpoint.
    Healthcheck(HealthcheckArgs),
}

/// Coordinated deployment preset for `zinder serve`, `zinder index`, and `zinder config`.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq, ValueEnum)]
pub enum Preset {
    /// Canonical writer plus lightwalletd-compatible reader without wallet projection.
    #[value(name = "light-wallet", alias = "lightwalletd")]
    LightWallet,
    /// Full wallet serving over canonical and wallet projection stores.
    #[default]
    Wallet,
    /// Full wallet serving plus explorer views, `ExplorerQuery`, and Cipherscan compat.
    Explorer,
}

impl Preset {
    /// Maps this CLI preset onto the shared [`DeploymentProfile`].
    #[must_use]
    pub const fn deployment_profile(self) -> DeploymentProfile {
        match self {
            Self::LightWallet => DeploymentProfile::LightWallet,
            Self::Wallet => DeploymentProfile::Wallet,
            Self::Explorer => DeploymentProfile::Explorer,
        }
    }

    /// Returns the kebab-case preset name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.deployment_profile().as_str()
    }
}

/// Individual runtime role executable via `zinder run <role>`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, ValueEnum)]
pub enum ServiceRole {
    /// Canonical store writer and indexer (`zinder-ingest`).
    Ingest,
    /// Wallet projection writer (`zinder-projector`).
    #[value(alias = "wallet-index")]
    Projector,
    /// Native `WalletQuery` `gRPC` reader (`zinder-query`).
    #[value(alias = "wallet-grpc")]
    Query,
    /// Lightwalletd-compatible `gRPC` reader (`zinder-compat-lightwalletd`).
    #[value(name = "compat-lightwalletd", alias = "lightwalletd")]
    CompatLightwalletd,
    /// Explorer materialized-view `gRPC` reader (`zinder-explorer`).
    Explorer,
    /// Cipherscan-compatible REST adapter (`zinder-compat-cipherscan`).
    #[value(name = "compat-cipherscan", alias = "cipherscan")]
    CompatCipherscan,
    /// Signed shielded-history catalog publisher (`publish-shielded-history`).
    #[value(name = "history-publisher", alias = "publish-history")]
    HistoryPublisher,
}

impl ServiceRole {
    /// Returns the underlying service binary name passed to role entrypoints.
    #[must_use]
    pub const fn binary_name(self) -> &'static str {
        match self {
            Self::Ingest => "zinder-ingest",
            Self::Projector => "zinder-projector",
            Self::Query => "zinder-query",
            Self::CompatLightwalletd => "zinder-compat-lightwalletd",
            Self::Explorer => "zinder-explorer",
            Self::CompatCipherscan => "zinder-compat-cipherscan",
            Self::HistoryPublisher => "zinder-history-publisher",
        }
    }

    /// Returns a concise role label for status payloads, logs, and diagnostics.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Ingest => "ingest",
            Self::Projector => "projector",
            Self::Query => "query",
            Self::CompatLightwalletd => "compat-lightwalletd",
            Self::Explorer => "explorer",
            Self::CompatCipherscan => "compat-cipherscan",
            Self::HistoryPublisher => "history-publisher",
        }
    }
}

/// CLI options for `zinder serve`.
#[derive(Clone, Debug, Args)]
pub struct ServeArgs {
    /// Deployment preset (`light-wallet`, `wallet`, `explorer`).
    #[arg(long)]
    pub preset: Option<Preset>,
    /// Run stateless reader roles only without `ingest` or `projector`.
    #[arg(long = "readers-only")]
    pub readers_only: bool,
    /// Retain full raw block blobs (`storage.full_blocks = true`).
    #[arg(long = "full-blocks")]
    pub full_blocks: bool,
    /// Base data directory from which canonical, wallet, and secondary paths derive.
    #[arg(long = "data-dir")]
    pub data_dir: Option<PathBuf>,
    /// Combined TOML configuration file loaded before `ZINDER_*` environment variables.
    #[arg(long = "config")]
    pub config_path: Option<PathBuf>,
    /// Print the effective redacted configuration without opening stores or binding sockets.
    #[arg(long = "print-config")]
    pub print_config: bool,
}

/// CLI options for `zinder index`.
#[derive(Clone, Debug, Args)]
pub struct IndexArgs {
    /// Deployment preset (`light-wallet`, `wallet`, `explorer`).
    #[arg(long)]
    pub preset: Option<Preset>,
    /// Retain full raw block blobs (`storage.full_blocks = true`).
    #[arg(long = "full-blocks")]
    pub full_blocks: bool,
    /// Base data directory from which canonical storage paths derive.
    #[arg(long = "data-dir")]
    pub data_dir: Option<PathBuf>,
    /// TOML configuration file loaded before `ZINDER_*` environment variables.
    #[arg(long = "config")]
    pub config_path: Option<PathBuf>,
    /// Print the effective redacted configuration without opening stores or binding sockets.
    #[arg(long = "print-config")]
    pub print_config: bool,
}

/// CLI options for `zinder run <role> [args...]`.
#[derive(Debug, Args)]
pub struct RunArgs {
    /// Individual service role to run.
    pub role: ServiceRole,
    /// Arguments forwarded verbatim to the selected role entrypoint.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub args: Vec<OsString>,
}

/// CLI options for `zinder snapshot`.
#[derive(Debug, Args)]
pub struct SnapshotArgs {
    /// TOML configuration file loaded before environment variables and overrides.
    #[arg(long = "config", global = true)]
    pub config_path: Option<PathBuf>,
    /// Network name, such as `zcash-regtest`.
    #[arg(long, global = true)]
    pub network: Option<String>,
    /// Upstream node source (`zebra-json-rpc` or `zebra-indexer-grpc`).
    #[arg(long = "node-source", global = true)]
    pub node_source: Option<String>,
    /// Upstream Zebra JSON-RPC URL.
    #[arg(long = "json-rpc-addr", global = true)]
    pub json_rpc_addr: Option<String>,
    /// Node auth method (`none`, `basic`, or `cookie`).
    #[arg(long = "node-auth-method", global = true)]
    pub node_auth_method: Option<String>,
    /// Node basic-auth username.
    #[arg(long = "node-auth-username", global = true)]
    pub node_auth_username: Option<String>,
    /// Path to the Zebra cookie file.
    #[arg(long = "node-auth-path", global = true)]
    pub node_auth_path: Option<PathBuf>,
    /// Canonical `RocksDB` primary store path.
    #[arg(long = "storage-path", global = true)]
    pub storage_path: Option<PathBuf>,
    /// Upstream node RPC request timeout in seconds.
    #[arg(long = "request-timeout-secs", global = true)]
    pub request_timeout_secs: Option<u64>,
    /// Maximum JSON-RPC response body size in bytes.
    #[arg(long = "max-response-bytes", global = true)]
    pub max_response_bytes: Option<u64>,
    /// Canonical replacement-depth policy window.
    #[arg(long = "reorg-window-blocks", global = true)]
    pub reorg_window_blocks: Option<u32>,
    /// Initial checkpoint height for checkpointed bootstrap construction.
    #[arg(long = "checkpoint-height", global = true)]
    pub checkpoint_height: Option<u32>,
    /// Snapshot action to run.
    #[command(subcommand)]
    pub action: SnapshotAction,
}

impl SnapshotArgs {
    /// Converts these snapshot CLI flags into [`IngestConfigOverrides`].
    #[must_use]
    pub fn into_parts(self) -> (SnapshotCommand, Option<PathBuf>, IngestConfigOverrides) {
        let command = match self.action {
            SnapshotAction::ExportEdgeSnapshot(args) => SnapshotCommand::ExportEdgeSnapshot(args),
            SnapshotAction::BootstrapFromSnapshot(args) => {
                SnapshotCommand::BootstrapFromSnapshot(args)
            }
        };
        let overrides = IngestConfigOverrides {
            network: self.network,
            node_source: self.node_source,
            json_rpc_addr: self.json_rpc_addr,
            node_auth_method: self.node_auth_method,
            node_auth_username: self.node_auth_username,
            node_auth_path: self.node_auth_path,
            storage_path: self.storage_path,
            request_timeout_secs: self.request_timeout_secs,
            max_response_bytes: self.max_response_bytes,
            reorg_window_blocks: self.reorg_window_blocks,
            checkpoint_height: self.checkpoint_height,
            ..Default::default()
        };
        (command, self.config_path, overrides)
    }
}

/// Edge-snapshot management operations under `zinder snapshot`.
#[derive(Debug, Subcommand)]
pub enum SnapshotAction {
    /// Export a signed canonical edge-snapshot bundle.
    #[command(name = "export-edge-snapshot", alias = "export")]
    ExportEdgeSnapshot(ExportArgs),
    /// Bootstrap and verify a canonical store from a signed edge snapshot.
    #[command(name = "bootstrap-from-snapshot", alias = "import")]
    BootstrapFromSnapshot(BootstrapArgs),
}

/// CLI options for `zinder backup`.
#[derive(Debug, Args)]
pub struct BackupArgs {
    /// TOML configuration file loaded before environment and CLI overrides.
    #[arg(long = "config", global = true)]
    pub config_path: Option<PathBuf>,
    /// Zinder-native network name.
    #[arg(long, global = true)]
    pub network: Option<String>,
    /// Loopback projector control `gRPC` listen address used for checkpoint capture.
    #[arg(long = "projector-control-listen-addr", global = true)]
    pub projector_control_listen_addr: Option<SocketAddr>,
    /// Bearer token file authenticating requests to projector owner control.
    #[arg(long = "projector-control-token-path", global = true)]
    pub projector_control_bearer_token_path: Option<PathBuf>,
    /// Shared state-bundle candidate root matching projector configuration.
    #[arg(long = "projector-control-checkpoint-staging-root", global = true)]
    pub projector_control_checkpoint_staging_root: Option<PathBuf>,
    /// Backup operation to execute.
    #[command(subcommand)]
    pub action: BackupAction,
}

impl BackupArgs {
    /// Converts these CLI arguments into a projector [`RecoveryCommand`] and overrides.
    #[must_use]
    pub fn into_parts(self) -> (RecoveryCommand, Option<PathBuf>, ProjectorConfigOverrides) {
        let command = match self.action {
            BackupAction::Backup {
                archive,
                capture_timeout_seconds,
                validation,
            } => RecoveryCommand::Backup {
                archive,
                capture_timeout_seconds,
                validation,
            },
            BackupAction::Verify(args) => RecoveryCommand::Verify(args),
            BackupAction::Restore { backup, into } => RecoveryCommand::Restore { backup, into },
        };
        let overrides = ProjectorConfigOverrides {
            network: self.network,
            projector_control_listen_addr: self.projector_control_listen_addr,
            projector_control_bearer_token_path: self.projector_control_bearer_token_path,
            projector_control_checkpoint_staging_root: self
                .projector_control_checkpoint_staging_root,
            ..Default::default()
        };
        (command, self.config_path, overrides)
    }
}

/// Backup lifecycle operations under `zinder backup`.
#[derive(Debug, Subcommand)]
pub enum BackupAction {
    /// Capture canonical and wallet stores via the running projector into a sealed backup.
    #[command(alias = "create")]
    Backup {
        /// Destination archive directory.
        #[arg(long)]
        archive: PathBuf,
        /// Maximum seconds to wait for the capture receipt from projector owner control.
        #[arg(long, default_value = "3600", value_name = "SECONDS")]
        capture_timeout_seconds: NonZeroU32,
        /// Bounded verification memory settings.
        #[command(flatten)]
        validation: ValidationArgs,
    },
    /// Verify a sealed backup archive against its expected manifest SHA-256 digest.
    Verify(ArchiveArgs),
    /// Restore a verified backup archive into a fresh directory.
    Restore {
        /// Archive identity and verification parameters.
        #[command(flatten)]
        backup: ArchiveArgs,
        /// Fresh destination directory for restored canonical and wallet stores.
        #[arg(long)]
        into: PathBuf,
    },
}

/// CLI options for `zinder config`.
#[derive(Debug, Args)]
pub struct ConfigArgs {
    /// Configuration inspection action.
    #[command(subcommand)]
    pub action: ConfigAction,
}

/// Configuration inspection subcommands under `zinder config`.
#[derive(Debug, Subcommand)]
pub enum ConfigAction {
    /// Print effective redacted configuration across all active preset roles.
    Print(ConfigScopeArgs),
    /// Validate configuration across all active preset roles without opening stores.
    Validate(ConfigScopeArgs),
}

/// Scope options shared by `zinder config print` and `zinder config validate`.
#[derive(Clone, Debug, Args)]
pub struct ConfigScopeArgs {
    /// Deployment preset (`light-wallet`, `wallet`, `explorer`).
    #[arg(long)]
    pub preset: Option<Preset>,
    /// Inspect reader-only mode (`--readers-only`).
    #[arg(long = "readers-only")]
    pub readers_only: bool,
    /// Apply `storage.full_blocks = true`.
    #[arg(long = "full-blocks")]
    pub full_blocks: bool,
    /// Override base `storage.data_dir`.
    #[arg(long = "data-dir")]
    pub data_dir: Option<PathBuf>,
    /// Combined TOML configuration file path.
    #[arg(long = "config")]
    pub config_path: Option<PathBuf>,
}

/// CLI options for `zinder healthcheck`.
#[derive(Clone, Debug, Args)]
pub struct HealthcheckArgs {
    /// Probe `/readyz` instead of `/healthz` unless `--path` overrides it.
    #[arg(long)]
    pub ready: bool,
    /// Explicit operational HTTP endpoint socket address.
    #[arg(long = "addr")]
    pub addr: Option<SocketAddr>,
    /// Explicit HTTP path to request instead of `/healthz` or `/readyz`.
    #[arg(long = "path")]
    pub path: Option<String>,
    /// Optional TOML configuration file consulted for `[ops].listen_addr`.
    #[arg(long = "config")]
    pub config_path: Option<PathBuf>,
}

impl From<ServeArgs> for ConfigScopeArgs {
    fn from(args: ServeArgs) -> Self {
        Self {
            preset: args.preset,
            readers_only: args.readers_only,
            full_blocks: args.full_blocks,
            data_dir: args.data_dir,
            config_path: args.config_path,
        }
    }
}

impl From<IndexArgs> for ConfigScopeArgs {
    fn from(args: IndexArgs) -> Self {
        Self {
            preset: args.preset,
            readers_only: false,
            full_blocks: args.full_blocks,
            data_dir: args.data_dir,
            config_path: args.config_path,
        }
    }
}
