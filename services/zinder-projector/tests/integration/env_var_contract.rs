//! Keeps `zinder_runtime::ENVIRONMENT_VARIABLES` aligned with the variables
//! the `zinder-projector` loader accepts.

use zinder_runtime::ENVIRONMENT_VARIABLES;
use zinder_testkit::EnvVarContractProbe;

const BINARY: &str = "zinder-projector";

// `[ingest_control]` is a shared section type; the projector only dials the
// control endpoint.
const ACCEPTED_BUT_UNCONSUMED: &[&str] = &[
    "ZINDER_INGEST_CONTROL__CHECKPOINT_BEARER_TOKEN_PATH",
    "ZINDER_INGEST_CONTROL__CHECKPOINT_BEARER_TOKEN",
    "ZINDER_INGEST_CONTROL__CHECKPOINT_STAGING_ROOT",
    "ZINDER_INGEST_CONTROL__LISTEN_ADDR",
];

fn documented_variables() -> impl Iterator<Item = &'static str> {
    ENVIRONMENT_VARIABLES
        .iter()
        .filter(|env_var| env_var.is_used_by(BINARY))
        .map(|env_var| env_var.name)
}

#[test]
fn every_documented_variable_is_accepted_by_the_loader() -> eyre::Result<()> {
    EnvVarContractProbe::new(zinder_testkit::bin_exe!("zinder-projector"))
        .check_documented_variables_are_accepted(documented_variables())
}

#[test]
fn every_variable_the_loader_accepts_is_documented() -> eyre::Result<()> {
    EnvVarContractProbe::new(zinder_testkit::bin_exe!("zinder-projector"))
        .check_accepted_variables_are_documented(documented_variables(), ACCEPTED_BUT_UNCONSUMED)
}

#[test]
fn wallet_serving_entrypoint_routes_exactly_the_accepted_variables() -> eyre::Result<()> {
    EnvVarContractProbe::new(zinder_testkit::bin_exe!("zinder-projector"))
        .check_wallet_serving_routing(
            "projector",
            ENVIRONMENT_VARIABLES.iter().map(|env_var| env_var.name),
        )
}
