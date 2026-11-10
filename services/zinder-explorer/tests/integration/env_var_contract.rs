//! Keeps `zinder_runtime::ENVIRONMENT_VARIABLES` aligned with the variables
//! the `zinder-explorer` loader accepts.

use zinder_runtime::ENVIRONMENT_VARIABLES;
use zinder_testkit::EnvVarContractProbe;

const BINARY: &str = "zinder-explorer";

// `[storage]` and `[node]` are shared section types; the explorer fixes its
// catchup cadence, never gates startup on catchup, and does not read the
// indexer stream.
const ACCEPTED_BUT_UNCONSUMED: &[&str] = &[
    "ZINDER_NODE__INDEXER_GRPC_ADDR",
    "ZINDER_STORAGE__INITIAL_CATCHUP_TIMEOUT_MS",
    "ZINDER_STORAGE__SECONDARY_CATCHUP_INTERVAL_MS",
    "ZINDER_STORAGE__SECONDARY_REPLICA_LAG_THRESHOLD_CHAIN_EPOCHS",
];

fn documented_variables() -> impl Iterator<Item = &'static str> {
    ENVIRONMENT_VARIABLES
        .iter()
        .filter(|env_var| env_var.is_used_by(BINARY))
        .map(|env_var| env_var.name)
}

#[test]
fn every_documented_variable_is_accepted_by_the_loader() -> eyre::Result<()> {
    EnvVarContractProbe::new(env!("CARGO_BIN_EXE_zinder-explorer"))
        .check_documented_variables_are_accepted(documented_variables())
}

#[test]
fn every_variable_the_loader_accepts_is_documented() -> eyre::Result<()> {
    EnvVarContractProbe::new(env!("CARGO_BIN_EXE_zinder-explorer"))
        .check_accepted_variables_are_documented(documented_variables(), ACCEPTED_BUT_UNCONSUMED)
}
