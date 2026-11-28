//! Binary smoke checks for unified `ZinderConfig` environment parsing on `zinder-compat-cipherscan`.

use std::process::Command;

fn print_config_with_env(pairs: &[(&str, &str)]) -> eyre::Result<std::process::Output> {
    let mut command = Command::new(zinder_testkit::bin_exe!("zinder-compat-cipherscan"));
    command.env_clear().arg("--print-config");
    command.env("ZINDER_NETWORK__NAME", "zcash-regtest");
    for (name, env_val) in pairs {
        command.env(name, env_val);
    }
    Ok(command.output()?)
}

#[test]
fn shared_schema_env_vars_are_accepted_and_empty_or_unknown_keys_are_rejected() -> eyre::Result<()>
{
    let scratch = tempfile::tempdir()?;
    let shared_data = scratch.path().join("shared-data").display().to_string();
    let shared_canonical = scratch
        .path()
        .join("shared-canonical")
        .display()
        .to_string();
    let ok = print_config_with_env(&[
        ("ZINDER_STORAGE__DATA_DIR", &shared_data),
        ("ZINDER_STORAGE__PATH", &shared_canonical),
        ("ZINDER_INGEST__SOURCE", "zebra-json-rpc"),
        ("ZINDER_DEPLOYMENT__PROFILE", "explorer"),
    ])?;
    assert!(ok.status.success(), "{ok:?}");

    let unknown = print_config_with_env(&[("ZINDER_UNKNOWN_SECTION__PATH", "x")])?;
    assert!(!unknown.status.success(), "{unknown:?}");

    let nested = print_config_with_env(&[("ZINDER_OPS__LISTEN_ADDR__PORT", "9108")])?;
    assert!(!nested.status.success(), "{nested:?}");

    let empty = print_config_with_env(&[("ZINDER_OPS__LISTEN_ADDR", "")])?;
    assert!(!empty.status.success(), "{empty:?}");
    Ok(())
}
