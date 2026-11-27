//! Pure-Rust operational endpoint probe for `zinder healthcheck`.

use std::process::ExitCode;

use zinder_runtime::{
    OPS_HEALTHCHECK_TIMEOUT, OPS_LISTEN_ADDR_ENV_VAR, config_file_ops_listen_addr,
    probe_ops_endpoint, resolve_ops_healthcheck_addr,
};

use crate::cli::HealthcheckArgs;

/// Default operational endpoint address used by `zinder serve` and `zinder healthcheck`.
pub const DEFAULT_SUPERVISOR_OPS_LISTEN_ADDR: &str = "127.0.0.1:9108";

/// Probes `/healthz` (or `/readyz` when `--ready` is passed) and exits `0` only on HTTP 200.
pub fn run_healthcheck(args: &HealthcheckArgs) -> ExitCode {
    let target_path =
        args.path
            .as_deref()
            .unwrap_or(if args.ready { "/readyz" } else { "/healthz" });
    let environment_setting = std::env::var_os(OPS_LISTEN_ADDR_ENV_VAR)
        .map(|env_val| env_val.to_string_lossy().into_owned());
    let file_setting = args
        .config_path
        .as_deref()
        .and_then(config_file_ops_listen_addr);
    let endpoint = match resolve_ops_healthcheck_addr(
        args.addr,
        environment_setting.as_deref(),
        file_setting.as_deref(),
        DEFAULT_SUPERVISOR_OPS_LISTEN_ADDR,
    ) {
        Ok(addr) => addr,
        Err(error) => {
            emit_healthcheck_error(&error.to_string());
            return ExitCode::FAILURE;
        }
    };
    match probe_ops_endpoint(endpoint, target_path, OPS_HEALTHCHECK_TIMEOUT) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            emit_healthcheck_error(&error.to_string());
            ExitCode::FAILURE
        }
    }
}

#[allow(
    clippy::print_stderr,
    reason = "container healthchecks report failure reasons on stderr before exiting non-zero"
)]
fn emit_healthcheck_error(message: &str) {
    eprintln!("zinder healthcheck: {message}");
}
