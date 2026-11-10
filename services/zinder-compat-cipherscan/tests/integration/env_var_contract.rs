//! Keeps `zinder_runtime::ENVIRONMENT_VARIABLES` aligned with the variables
//! the `zinder-compat-cipherscan` loader accepts.

use zinder_runtime::ENVIRONMENT_VARIABLES;
use zinder_testkit::EnvVarContractProbe;

const BINARY: &str = "zinder-compat-cipherscan";

fn probe() -> EnvVarContractProbe {
    EnvVarContractProbe::new(env!("CARGO_BIN_EXE_zinder-compat-cipherscan"))
}

fn documented_variables() -> impl Iterator<Item = &'static str> {
    ENVIRONMENT_VARIABLES
        .iter()
        .filter(|env_var| env_var.is_used_by(BINARY))
        .map(|env_var| env_var.name)
}

#[test]
fn every_documented_variable_is_accepted_by_the_loader() -> eyre::Result<()> {
    probe().check_documented_variables_are_accepted(documented_variables())
}

#[test]
fn every_variable_the_loader_accepts_is_documented() -> eyre::Result<()> {
    probe().check_accepted_variables_are_documented(documented_variables(), &[])
}

#[test]
fn a_variable_outside_the_loader_schema_is_rejected() {
    let outcome = probe().check_documented_variables_are_accepted(["ZINDER_STORAGE__PATH"]);
    assert!(
        outcome.is_err(),
        "the loader must reject a section it does not define"
    );
}
