//! Unified `zinder` library, CLI dispatcher, and multi-role runtime supervisor.

pub mod cli;
pub mod config;
pub mod healthcheck;
pub mod ops;
pub mod supervisor;

use std::{ffi::OsString, process::ExitCode};

use clap::Parser;
use cli::{Cli, Command, ConfigAction, ServiceRole};
use config::{ConfigScope, SupervisionPlan, render_combined_redacted_toml};
use zinder_runtime::{TracingSubscriberError, install_tracing_subscriber};

/// Entrypoint called by the `zinder` binary.
pub async fn main_from_env() -> ExitCode {
    let cli = Cli::parse();
    dispatch_cli(cli).await
}

async fn dispatch_cli(cli: Cli) -> ExitCode {
    match cli.command {
        Command::Healthcheck(args) => healthcheck::run_healthcheck(&args),
        Command::Config(args) => run_config_command(args),
        Command::Run(args) => run_single_role(args).await,
        Command::Serve(args) => run_serve(args).await,
        Command::Index(args) => run_index(args).await,
        Command::Snapshot(args) => run_snapshot_command(args).await,
        Command::Backup(args) => run_backup_command(args).await,
    }
}

fn run_config_command(args: cli::ConfigArgs) -> ExitCode {
    let (print_toml, scope_args) = match args.action {
        ConfigAction::Print(scope) => (true, scope),
        ConfigAction::Validate(scope) => (false, scope),
    };
    let scope = ConfigScope::serve(scope_args);
    match config::prepare_supervision_plan(&scope) {
        Ok(plan) => {
            if print_toml {
                emit_toml(&render_combined_redacted_toml(&plan));
            }
            ExitCode::SUCCESS
        }
        Err(error) => emit_cli_error("config", &error.to_string()),
    }
}

async fn run_serve(args: cli::ServeArgs) -> ExitCode {
    let print_config = args.print_config;
    let scope = ConfigScope::serve(args.into());
    run_supervised_scope(&scope, print_config).await
}

async fn run_index(args: cli::IndexArgs) -> ExitCode {
    let print_config = args.print_config;
    let scope = ConfigScope::index(args.into());
    run_supervised_scope(&scope, print_config).await
}

async fn run_supervised_scope(scope: &ConfigScope, print_config: bool) -> ExitCode {
    let plan: SupervisionPlan = match config::prepare_supervision_plan(scope) {
        Ok(plan) => plan,
        Err(error) => return emit_cli_error(scope.mode_label(), &error.to_string()),
    };
    if print_config {
        emit_toml(&render_combined_redacted_toml(&plan));
        return ExitCode::SUCCESS;
    }
    if let Err(error) = install_tracing_subscriber() {
        return emit_tracing_error(scope.mode_label(), &error);
    }
    match supervisor::run_supervision(plan).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => emit_cli_error(scope.mode_label(), &error.to_string()),
    }
}

async fn run_single_role(args: cli::RunArgs) -> ExitCode {
    let has_healthcheck = args.args.iter().any(|arg| arg == "healthcheck");
    if !has_healthcheck && let Err(error) = install_tracing_subscriber() {
        return emit_tracing_error(args.role.binary_name(), &error);
    }
    let forwarded: Vec<OsString> = std::iter::once(OsString::from(args.role.binary_name()))
        .chain(args.args)
        .collect();
    match args.role {
        ServiceRole::Ingest => {
            zinder_ingest::runtime::run_from_args(args.role.binary_name(), forwarded).await
        }
        ServiceRole::Projector => {
            zinder_projector::runtime::run_from_args(args.role.binary_name(), forwarded).await
        }
        ServiceRole::Query => {
            zinder_query::runtime::run_from_args(args.role.binary_name(), forwarded).await
        }
        ServiceRole::CompatLightwalletd => {
            zinder_compat_lightwalletd::runtime::run_from_args(args.role.binary_name(), forwarded)
                .await
        }
        ServiceRole::Explorer => {
            zinder_explorer::runtime::run_from_args(args.role.binary_name(), forwarded).await
        }
        ServiceRole::CompatCipherscan => {
            zinder_compat_cipherscan::runtime::run_from_args(forwarded).await
        }
        ServiceRole::HistoryPublisher => {
            zinder_compat_lightwalletd::runtime::run_history_publisher_from_args(forwarded).await
        }
    }
}

async fn run_snapshot_command(args: cli::SnapshotArgs) -> ExitCode {
    if let Err(error) = install_tracing_subscriber() {
        return emit_tracing_error("snapshot", &error);
    }
    let (command, config_path, overrides) = args.into_parts();
    match zinder_ingest::runtime::edge_snapshot::run(command, config_path, overrides).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => emit_cli_error("snapshot", &error.to_string()),
    }
}

async fn run_backup_command(args: cli::BackupArgs) -> ExitCode {
    if let Err(error) = install_tracing_subscriber() {
        return emit_tracing_error("backup", &error);
    }
    let (command, config_path, overrides) = args.into_parts();
    command
        .run_with_overrides("zinder backup", config_path, overrides)
        .await
}

#[allow(
    clippy::print_stdout,
    reason = "--print-config renders redacted TOML cleanly on stdout"
)]
fn emit_toml(rendered: &str) {
    println!("{rendered}");
}

#[allow(
    clippy::print_stderr,
    reason = "CLI startup and configuration errors print on stderr before exiting non-zero"
)]
fn emit_cli_error(command: &str, message: &str) -> ExitCode {
    eprintln!("zinder {command}: {message}");
    ExitCode::FAILURE
}

#[allow(
    clippy::print_stderr,
    reason = "tracing initialization runs before a subscriber exists to log failures"
)]
fn emit_tracing_error(command: &str, error: &TracingSubscriberError) -> ExitCode {
    eprintln!("zinder {command}: {error}");
    ExitCode::FAILURE
}
