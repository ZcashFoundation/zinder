//! Private backup operations on the projector release binary.

use std::{
    path::{Path, PathBuf},
    process::ExitCode,
};

use clap::{Args, Subcommand};
use hex::FromHex;
use thiserror::Error;
use zinder_core::{Network, wire::decode_zinder_native_chain_name};
use zinder_projector::recovery_archive::{
    RecoveryArchiveError, export_private_backup, restore_private_backup,
    validate_private_backup_destination, verify_private_backup,
};
use zinder_proto::v1::ingest::{
    CreateStateBundleCaptureRequest, projector_control_client::ProjectorControlClient,
};
use zinder_runtime::{
    BearerTokenConnectError, ConfigError, ResolvedProjectorControl, connect_zinder_grpc,
};

use crate::{
    Cli,
    config::load_backup_config,
    projector_control::{CaptureReceiptError, read_capture_receipt},
};

#[derive(Subcommand)]
pub(super) enum RecoveryCommand {
    /// Capture both stores through the running projector and seal a private backup.
    Backup {
        /// Absolute path to the archive directory.
        ///
        /// The basename identifies this capture on retry. Use 1-64 lowercase ASCII
        /// letters, digits, or interior hyphens. Choose a new name for a new capture.
        #[arg(long)]
        archive: PathBuf,
    },
    /// Verify a private backup against the SHA-256 recorded at capture.
    Verify(ArchiveArgs),
    /// Restore a verified private backup into a fresh directory.
    Restore {
        #[command(flatten)]
        backup: ArchiveArgs,
        /// Fresh destination directory for the restored canonical and wallet stores.
        #[arg(long)]
        into: PathBuf,
    },
}

#[derive(Args)]
pub(super) struct ArchiveArgs {
    /// Path to the sealed archive directory.
    #[arg(long)]
    archive: PathBuf,
    /// Expected Zinder-native network name.
    #[arg(long, value_parser = decode_zinder_native_chain_name)]
    network: Network,
    /// Expected outer manifest SHA-256 (64 hex characters) recorded when the archive was created.
    #[arg(long, value_parser = parse_sha256, value_name = "HEX")]
    sha256: [u8; 32],
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
    #[error(transparent)]
    StagedCapture(#[from] CaptureReceiptError),
    #[error("the projector capture receipt does not match the staged manifest")]
    ReceiptMismatch,
    #[error(transparent)]
    Archive(#[from] RecoveryArchiveError),
}

impl RecoveryCommand {
    #[allow(
        clippy::print_stdout,
        clippy::print_stderr,
        reason = "the recovery CLI emits one JSON success record or a diagnostic on stderr"
    )]
    pub(super) async fn run(self, mut cli: Cli) -> ExitCode {
        let outcome = match self {
            Self::Backup { archive } => {
                let path = cli.config_path.take();
                match load_backup_config(path, cli.into()) {
                    Ok((network, control)) => backup(&archive, network, &control).await,
                    Err(error) => Err(error.into()),
                }
            }
            Self::Verify(args) => verify_private_backup(&args.archive, args.network, &args.sha256)
                .map(|()| args.sha256)
                .map_err(RecoveryCommandError::from),
            Self::Restore { backup, into } => {
                restore_private_backup(&backup.archive, &into, backup.network, &backup.sha256)
                    .map(|()| backup.sha256)
                    .map_err(RecoveryCommandError::from)
            }
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
                eprintln!("{}: {error}", env!("CARGO_BIN_NAME"));
                ExitCode::FAILURE
            }
        }
    }
}

async fn backup(
    archive: &Path,
    network: Network,
    control: &ResolvedProjectorControl,
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
    let receipt = ProjectorControlClient::new(channel)
        .create_state_bundle_capture(CreateStateBundleCaptureRequest {
            candidate_id: candidate.to_owned(),
            reuse_completed: true,
        })
        .await
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
    )?)
}
