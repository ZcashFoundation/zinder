use std::{fs, path::Path, process::Command};

use tempfile::tempdir;

#[test]
fn version_reports_the_product_version() -> eyre::Result<()> {
    let output = projector_command().arg("--version").output()?;

    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        String::from_utf8(output.stdout)?,
        format!("zinder-projector {}\n", env!("CARGO_PKG_VERSION"))
    );

    Ok(())
}

#[test]
fn print_config_renders_the_complete_fail_closed_contract() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let canonical_path = tempdir.path().join("canonical");
    let canonical_secondary_path = tempdir.path().join("projector-canonical-secondary");
    let wallet_path = tempdir.path().join("wallet");

    let output = projector_command()
        .args([
            "--print-config",
            "--network",
            "zcash-regtest",
            "--canonical-path",
            path_str(&canonical_path)?,
            "--canonical-secondary-path",
            path_str(&canonical_secondary_path)?,
            "--wallet-path",
            path_str(&wallet_path)?,
            "--reorg-window-blocks",
            "100",
            "--node-json-rpc-addr",
            "http://127.0.0.1:18232",
            "--build-owner-hex",
            "00112233445566778899aabbccddeeff",
            "--lease-duration-seconds",
            "14400",
        ])
        .output()?;

    assert!(
        output.status.success(),
        "print-config failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("name = \"zcash-regtest\""), "{stdout}");
    assert!(
        stdout.contains(&format!(
            "canonical_path = {:?}",
            canonical_path.display().to_string()
        )),
        "{stdout}"
    );
    assert!(
        stdout.contains(&format!(
            "canonical_secondary_path = {:?}",
            canonical_secondary_path.display().to_string()
        )),
        "{stdout}"
    );
    assert!(
        stdout.contains("raw_blob_policy = \"transactions\""),
        "{stdout}"
    );
    assert!(stdout.contains("[wallet]"), "{stdout}");
    assert!(
        stdout.contains(&format!("path = {:?}", wallet_path.display().to_string())),
        "{stdout}"
    );
    assert!(stdout.contains("[wallet.rocksdb]"), "{stdout}");
    assert!(stdout.contains("reorg_window_blocks = 100"), "{stdout}");
    assert!(
        stdout.contains("lease_duration_seconds = 14400"),
        "{stdout}"
    );
    assert!(
        stdout.contains("fence_convergence_timeout_seconds = 30"),
        "{stdout}"
    );
    assert!(
        stdout.contains("max_transition_logical_bytes = 536870912"),
        "{stdout}"
    );
    assert!(
        stdout.contains("listen_addr = \"127.0.0.1:9110\""),
        "{stdout}"
    );
    assert!(
        stdout.contains("addr = \"http://127.0.0.1:9100\""),
        "{stdout}"
    );
    Ok(())
}

#[test]
fn print_config_rejects_paths_that_cannot_have_independent_owners() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let same_path = tempdir.path().join("same");
    let output = projector_command()
        .args([
            "--print-config",
            "--network",
            "zcash-regtest",
            "--canonical-path",
            path_str(&same_path)?,
            "--canonical-secondary-path",
            path_str(&same_path)?,
            "--wallet-path",
            path_str(&same_path)?,
            "--node-json-rpc-addr",
            "http://127.0.0.1:18232",
            "--build-owner-hex",
            "00112233445566778899aabbccddeeff",
        ])
        .output()?;

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("must be disjoint roots"), "{stderr}");
    Ok(())
}

#[test]
fn print_config_rejects_nested_storage_roots() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let canonical_path = tempdir.path().join("canonical");
    let output = projector_command()
        .args([
            "--print-config",
            "--network",
            "zcash-regtest",
            "--canonical-path",
            path_str(&canonical_path)?,
            "--canonical-secondary-path",
            path_str(&canonical_path.join("secondary"))?,
            "--wallet-path",
            path_str(&tempdir.path().join("wallet"))?,
            "--node-json-rpc-addr",
            "http://127.0.0.1:18232",
            "--build-owner-hex",
            "00112233445566778899aabbccddeeff",
            "--lease-duration-seconds",
            "14400",
        ])
        .output()?;

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("must be disjoint roots"), "{stderr}");
    Ok(())
}

#[test]
fn print_config_rejects_lexically_aliased_storage_roots() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let canonical_path = tempdir.path().join("canonical");
    let wallet_alias = tempdir.path().join("wallet/../canonical");
    let output = projector_command()
        .args([
            "--print-config",
            "--network",
            "zcash-regtest",
            "--canonical-path",
            path_str(&canonical_path)?,
            "--canonical-secondary-path",
            path_str(&tempdir.path().join("canonical-secondary"))?,
            "--wallet-path",
            path_str(&wallet_alias)?,
            "--node-json-rpc-addr",
            "http://127.0.0.1:18232",
            "--build-owner-hex",
            "00112233445566778899aabbccddeeff",
            "--lease-duration-seconds",
            "14400",
        ])
        .output()?;

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("must be disjoint roots"), "{stderr}");
    Ok(())
}

#[test]
fn print_config_requires_an_explicit_lease_that_exceeds_the_build_hard_gate() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let output = projector_command()
        .args([
            "--print-config",
            "--network",
            "zcash-regtest",
            "--canonical-path",
            path_str(&tempdir.path().join("canonical"))?,
            "--canonical-secondary-path",
            path_str(&tempdir.path().join("secondary"))?,
            "--wallet-path",
            path_str(&tempdir.path().join("wallet"))?,
            "--node-json-rpc-addr",
            "http://127.0.0.1:18232",
            "--build-owner-hex",
            "00112233445566778899aabbccddeeff",
            "--lease-duration-seconds",
            "7200",
        ])
        .output()?;

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("at least 14400 seconds"), "{stderr}");
    Ok(())
}

#[test]
fn print_config_rejects_an_implicit_construction_lease() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let output = projector_command()
        .args([
            "--print-config",
            "--network",
            "zcash-regtest",
            "--canonical-path",
            path_str(&tempdir.path().join("canonical"))?,
            "--canonical-secondary-path",
            path_str(&tempdir.path().join("secondary"))?,
            "--wallet-path",
            path_str(&tempdir.path().join("wallet"))?,
            "--node-json-rpc-addr",
            "http://127.0.0.1:18232",
            "--build-owner-hex",
            "00112233445566778899aabbccddeeff",
        ])
        .output()?;

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("projector.lease_duration_seconds"),
        "{stderr}"
    );
    Ok(())
}

#[test]
fn print_config_rejects_a_zero_fence_convergence_timeout() -> eyre::Result<()> {
    let tempdir = tempdir()?;

    let output = projector_command()
        .env("ZINDER_PROJECTOR__FENCE_CONVERGENCE_TIMEOUT_SECONDS", "0")
        .args([
            "--print-config",
            "--network",
            "zcash-regtest",
            "--canonical-path",
            path_str(&tempdir.path().join("canonical"))?,
            "--canonical-secondary-path",
            path_str(&tempdir.path().join("secondary"))?,
            "--wallet-path",
            path_str(&tempdir.path().join("wallet"))?,
            "--node-json-rpc-addr",
            "http://127.0.0.1:18232",
            "--build-owner-hex",
            "00112233445566778899aabbccddeeff",
            "--lease-duration-seconds",
            "14400",
        ])
        .output()?;

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("projector.fence_convergence_timeout_seconds must be greater than zero"),
        "{stderr}"
    );
    Ok(())
}

#[test]
fn unknown_storage_field_is_rejected() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let config_path = tempdir.path().join("zinder-projector.toml");
    fs::write(
        &config_path,
        format!(
            r#"[network]
name = "zcash-regtest"

[storage]
canonical_path = "{}"
canonical_secondary_path = "{}"
unsupported_path = "{}"

[projector]
reorg_window_blocks = 100
build_owner_hex = "00112233445566778899aabbccddeeff"
lease_duration_seconds = 14400

[node]
json_rpc_addr = "http://127.0.0.1:18232"
"#,
            path_str(&tempdir.path().join("canonical"))?,
            path_str(&tempdir.path().join("canonical-secondary"))?,
            path_str(&tempdir.path().join("wallet"))?,
        ),
    )?;

    let output = projector_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .output()?;

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("unknown field `unsupported_path`"),
        "{stderr}"
    );
    Ok(())
}

fn projector_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_zinder-projector"))
}

fn path_str(path: &Path) -> eyre::Result<&str> {
    path.to_str()
        .ok_or_else(|| eyre::eyre!("test path is not UTF-8: {path:?}"))
}
