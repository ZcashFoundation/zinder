//! Keeps `zinder_runtime::ENVIRONMENT_VARIABLES` aligned with the variables
//! the `zinder-compat-cipherscan` loader accepts.

use zinder_runtime::ENVIRONMENT_VARIABLES;
use zinder_testkit::EnvVarContractProbe;

const BINARY: &str = "zinder-compat-cipherscan";

fn probe() -> EnvVarContractProbe {
    EnvVarContractProbe::new(zinder_testkit::bin_exe!("zinder-compat-cipherscan"))
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
    let outcome = probe().check_documented_variables_are_accepted(["ZINDER_UNKNOWN_SECTION__PATH"]);
    assert!(
        outcome.is_err(),
        "the loader must reject a section it does not define"
    );
}

#[test]
fn sibling_service_variables_are_tolerated_in_shared_environment() -> eyre::Result<()> {
    let scratch = tempfile::tempdir()?;
    let output = std::process::Command::new(zinder_testkit::bin_exe!("zinder-compat-cipherscan"))
        .args(["--print-config"])
        .env("ZINDER_NETWORK__NAME", "zcash-regtest")
        .env(
            "ZINDER_STORAGE__DATA_DIR",
            scratch.path().join("shared-data"),
        )
        .env(
            "ZINDER_STORAGE__PATH",
            scratch.path().join("shared-canonical"),
        )
        .env("ZINDER_INGEST__SOURCE", "zebra-json-rpc")
        .env("ZINDER_DEPLOYMENT__PROFILE", "explorer")
        .output()?;
    assert!(output.status.success(), "{output:?}");
    Ok(())
}

#[test]
fn a_variable_nested_below_a_leaf_field_is_rejected() {
    let outcome =
        probe().check_documented_variables_are_accepted(["ZINDER_OPS__LISTEN_ADDR__PORT"]);
    assert!(
        outcome.is_err(),
        "the loader must reject a key nested below a leaf field"
    );
}
