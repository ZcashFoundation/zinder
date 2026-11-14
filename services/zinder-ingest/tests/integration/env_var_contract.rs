//! Keeps `zinder_runtime::ENVIRONMENT_VARIABLES` aligned with the variables
//! the `zinder-ingest` loader accepts.

use zinder_runtime::ENVIRONMENT_VARIABLES;
use zinder_testkit::EnvVarContractProbe;

const BINARY: &str = "zinder-ingest";

// `[ingest_control]` is shared with the reader processes; the writer reads
// only the listen, token, and staging fields.
const ACCEPTED_BUT_UNCONSUMED: &[&str] = &["ZINDER_INGEST_CONTROL__ADDR"];

fn documented_variables() -> impl Iterator<Item = &'static str> {
    ENVIRONMENT_VARIABLES
        .iter()
        .filter(|env_var| env_var.is_used_by(BINARY))
        .map(|env_var| env_var.name)
}

#[test]
fn every_documented_variable_is_accepted_by_the_loader() -> eyre::Result<()> {
    EnvVarContractProbe::new(env!("CARGO_BIN_EXE_zinder-ingest"))
        .check_documented_variables_are_accepted(documented_variables())
}

#[test]
fn every_variable_the_loader_accepts_is_documented() -> eyre::Result<()> {
    EnvVarContractProbe::new(env!("CARGO_BIN_EXE_zinder-ingest"))
        .check_accepted_variables_are_documented(documented_variables(), ACCEPTED_BUT_UNCONSUMED)
}

#[test]
fn wallet_serving_entrypoint_routes_exactly_the_accepted_variables() -> eyre::Result<()> {
    EnvVarContractProbe::new(env!("CARGO_BIN_EXE_zinder-ingest")).check_wallet_serving_routing(
        "ingest",
        ENVIRONMENT_VARIABLES.iter().map(|env_var| env_var.name),
    )
}
