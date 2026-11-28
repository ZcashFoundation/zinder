//! Private backup operations for the `zinder-projector` runtime.

use std::{
    num::{NonZeroU32, NonZeroU64},
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration,
};

use clap::{Args, Subcommand};
use hex::FromHex;
use thiserror::Error;
use zinder_core::{Network, wire::decode_zinder_native_chain_name};
use zinder_proto::v1::ingest::{
    CreateStateBundleCaptureRequest, projector_control_client::ProjectorControlClient,
};
use zinder_runtime::{
    BearerTokenConnectError, ConfigError, ResolvedProjectorControl, connect_zinder_grpc,
};

use crate::recovery_archive::{
    RecoveryArchiveError, RecoveryValidationBudget, export_private_backup, restore_private_backup,
    validate_private_backup_destination, verify_private_backup,
};

use super::{
    Cli,
    config::{ProjectorConfigOverrides, load_backup_config},
    projector_control::{CaptureReceiptError, read_capture_receipt},
};

/// Recovery subcommands (`backup`, `verify`, `restore`) for coherent wallet backups.
#[derive(Debug, Subcommand)]
pub enum RecoveryCommand {
    /// Capture both stores through the running projector and seal a private backup.
    Backup {
        /// Absolute path to the archive directory.
        ///
        /// The basename identifies this capture on retry. Use 1-64 lowercase ASCII
        /// letters, digits, or interior hyphens. Choose a new name for a new capture.
        #[arg(long)]
        archive: PathBuf,
        /// Maximum seconds to wait for the owner to return a capture receipt.
        #[arg(long, default_value = "3600", value_name = "SECONDS")]
        capture_timeout_seconds: NonZeroU32,
        /// Bounded validation memory settings.
        #[command(flatten)]
        validation: ValidationArgs,
    },
    /// Verify a private backup against the SHA-256 recorded at capture.
    Verify(ArchiveArgs),
    /// Restore a verified private backup into a fresh directory.
    Restore {
        /// Archive identity and verification parameters.
        #[command(flatten)]
        backup: ArchiveArgs,
        /// Fresh destination directory for the restored canonical and wallet stores.
        #[arg(long)]
        into: PathBuf,
    },
}

/// Shared identifying arguments for verifying or restoring a private backup archive.
#[derive(Debug, Args)]
pub struct ArchiveArgs {
    /// Path to the sealed archive directory.
    #[arg(long)]
    pub archive: PathBuf,
    /// Expected Zinder-native network name.
    #[arg(long, value_parser = decode_zinder_native_chain_name)]
    pub network: Network,
    /// Expected outer manifest SHA-256 (64 hex characters) recorded when the archive was created.
    #[arg(long, value_parser = parse_sha256, value_name = "HEX")]
    pub sha256: [u8; 32],
    /// Bounded validation memory settings.
    #[command(flatten)]
    pub validation: ValidationArgs,
}

/// Bounded memory settings used while verifying or restoring a sealed backup archive.
#[derive(Debug, Args)]
pub struct ValidationArgs {
    /// Maximum memory in bytes per sorter while validating the wallet checkpoint.
    #[arg(long = "max-sort-memory-bytes-per-sorter", value_name = "BYTES", default_value_t = RecoveryValidationBudget::default().max_sort_memory_bytes_per_sorter)]
    pub sort_memory_bytes_per_sorter: NonZeroU64,
    /// Maximum temporary file bytes per sorter while validating the wallet checkpoint.
    #[arg(long = "max-temporary-file-bytes-per-sorter", value_name = "BYTES", default_value_t = RecoveryValidationBudget::default().max_temporary_file_bytes_per_sorter)]
    pub temporary_file_bytes_per_sorter: NonZeroU64,
    /// Maximum accounted reorg undo bytes while validating the wallet checkpoint.
    #[arg(long = "max-accounted-reorg-undo-bytes", value_name = "BYTES", default_value_t = RecoveryValidationBudget::default().max_accounted_reorg_undo_bytes)]
    pub accounted_reorg_undo_bytes: NonZeroU64,
}

impl From<ValidationArgs> for RecoveryValidationBudget {
    fn from(args: ValidationArgs) -> Self {
        Self {
            max_sort_memory_bytes_per_sorter: args.sort_memory_bytes_per_sorter,
            max_temporary_file_bytes_per_sorter: args.temporary_file_bytes_per_sorter,
            max_accounted_reorg_undo_bytes: args.accounted_reorg_undo_bytes,
        }
    }
}

fn parse_sha256(input: &str) -> Result<[u8; 32], hex::FromHexError> {
    <[u8; 32]>::from_hex(input)
}

#[derive(Debug, Error)]
enum RecoveryCommandError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("backup requires an enabled projector_control.listen_addr and bearer_token_path")]
    ControlDisabled,
    #[error("could not connect to projector control: {0}")]
    Connect(#[from] BearerTokenConnectError),
    #[error("projector capture failed ({code}): {message}")]
    Capture { code: tonic::Code, message: String },
    #[error(
        "timed out after {seconds}s waiting for the capture receipt for '{candidate}'. The owner may still complete the capture. Retry with the same --archive path"
    )]
    CaptureTimeout {
        candidate: String,
        seconds: NonZeroU32,
    },
    #[error(transparent)]
    StagedCapture(#[from] CaptureReceiptError),
    #[error("the projector capture receipt does not match the staged manifest")]
    ReceiptMismatch,
    #[error(transparent)]
    Archive(#[from] RecoveryArchiveError),
}

impl RecoveryCommand {
    /// Executes a parsed [`RecoveryCommand`] with the provided config path and projector overrides.
    #[allow(
        clippy::print_stdout,
        clippy::print_stderr,
        reason = "the recovery CLI emits one JSON success record or a diagnostic on stderr"
    )]
    pub async fn run_with_overrides(
        self,
        binary_name: &str,
        config_path: Option<PathBuf>,
        overrides: ProjectorConfigOverrides,
    ) -> ExitCode {
        let outcome = match self {
            Self::Backup {
                archive,
                capture_timeout_seconds,
                validation,
            } => match load_backup_config(config_path, overrides) {
                Ok((network, control)) => {
                    backup(
                        &archive,
                        network,
                        &control,
                        capture_timeout_seconds,
                        validation.into(),
                    )
                    .await
                }
                Err(error) => Err(error.into()),
            },
            Self::Verify(args) => verify_private_backup(
                &args.archive,
                args.network,
                &args.sha256,
                args.validation.into(),
            )
            .map(|()| args.sha256)
            .map_err(RecoveryCommandError::from),
            Self::Restore { backup, into } => restore_private_backup(
                &backup.archive,
                &into,
                backup.network,
                &backup.sha256,
                backup.validation.into(),
            )
            .map(|()| backup.sha256)
            .map_err(RecoveryCommandError::from),
        };
        match outcome {
            Ok(digest) => {
                println!(
                    "{}",
                    serde_json::json!({ "manifest_sha256": hex::encode(digest) })
                );
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("{binary_name}: {error}");
                ExitCode::FAILURE
            }
        }
    }

    pub(super) async fn run(self, binary_name: &str, mut cli: Cli) -> ExitCode {
        let path = cli.config_path.take();
        self.run_with_overrides(binary_name, path, cli.into()).await
    }
}

async fn backup(
    archive: &Path,
    network: Network,
    control: &ResolvedProjectorControl,
    capture_timeout_seconds: NonZeroU32,
    validation: RecoveryValidationBudget,
) -> Result<[u8; 32], RecoveryCommandError> {
    let (Some(address), Some(token)) = (control.listen_addr, control.bearer_token.as_ref()) else {
        return Err(RecoveryCommandError::ControlDisabled);
    };
    let staging_root = &control.checkpoint_staging_root;
    let candidate = validate_private_backup_destination(staging_root, archive)?;
    tracing::info!(
        candidate,
        "requesting a coherent checkpoint from the projector"
    );
    let channel = connect_zinder_grpc(&format!("http://{address}"), Some(token)).await?;
    let mut client = ProjectorControlClient::new(channel);
    let receipt = tokio::time::timeout(
        Duration::from_secs(capture_timeout_seconds.get().into()),
        client.create_state_bundle_capture(CreateStateBundleCaptureRequest {
            candidate_id: candidate.to_owned(),
            reuse_completed: true,
        }),
    )
    .await
    .map_err(|_| RecoveryCommandError::CaptureTimeout {
        candidate: candidate.to_owned(),
        seconds: capture_timeout_seconds,
    })?
    .map_err(|status| RecoveryCommandError::Capture {
        code: status.code(),
        message: status.message().to_owned(),
    })?
    .into_inner();
    let (staged_receipt, digest) =
        read_capture_receipt(&staging_root.join(candidate), candidate, network, None)?;
    if staged_receipt != receipt {
        return Err(RecoveryCommandError::ReceiptMismatch);
    }
    Ok(export_private_backup(
        staging_root,
        archive,
        network,
        &digest,
        validation,
    )?)
}
