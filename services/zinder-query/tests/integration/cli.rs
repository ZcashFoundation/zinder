//! Public CLI contract for the native wallet-query runtime.

use std::process::Command;

use tempfile::TempDir;

#[test]
fn version_reports_the_product_version() -> eyre::Result<()> {
    let output = Command::new(zinder_testkit::bin_exe!("zinder-query"))
        .arg("--version")
        .output()?;

    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        String::from_utf8(output.stdout)?,
        format!("zinder-query {}\n", env!("CARGO_PKG_VERSION"))
    );

    Ok(())
}

fn query_command(temporary: &TempDir, canonical_secondary_root: &std::path::Path) -> Command {
    let mut command = Command::new(zinder_testkit::bin_exe!("zinder-query"));
    command.args([
        "--print-config",
        "--network",
        "zcash-regtest",
        "--canonical-primary-path",
    ]);
    command.arg(temporary.path().join("canonical-primary"));
    command.arg("--canonical-secondary-root");
    command.arg(canonical_secondary_root);
    command.arg("--wallet-primary-path");
    command.arg(temporary.path().join("wallet-primary"));
    command.arg("--wallet-secondary-root");
    command.arg(temporary.path().join("wallet-secondary"));
    command.args([
        "--ingest-control-addr",
        "http://127.0.0.1:9100",
        "--node-json-rpc-addr",
        "http://127.0.0.1:29232",
    ]);
    command
}

#[test]
fn print_config_uses_native_ports_and_dedicated_secondary_roots() -> eyre::Result<()> {
    let temporary = TempDir::new()?;
    let output =
        query_command(&temporary, &temporary.path().join("canonical-secondary")).output()?;
    assert!(
        output.status.success(),
        "print-config failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("listen_addr = \"127.0.0.1:9102\""));
    assert!(stdout.contains("listen_addr = \"127.0.0.1:9106\""));
    assert!(stdout.contains(&format!(
        "secondary_path = \"{}\"",
        temporary.path().join("canonical-secondary").display()
    )));
    assert!(stdout.contains(&format!(
        "secondary_path = \"{}\"",
        temporary.path().join("wallet-secondary").display()
    )));
    assert!(stdout.contains("raw_blob_policy = \"transactions\""));
    assert!(!stdout.contains("9101"));
    Ok(())
}

/// A query without a node serves storage only, so `[node]` is optional and
/// the rendered configuration omits it.
#[test]
fn print_config_accepts_a_query_without_a_node() -> eyre::Result<()> {
    let temporary = TempDir::new()?;
    let output = Command::new(zinder_testkit::bin_exe!("zinder-query"))
        .args(["--print-config", "--network", "zcash-regtest"])
        .arg("--canonical-primary-path")
        .arg(temporary.path().join("canonical-primary"))
        .arg("--canonical-secondary-root")
        .arg(temporary.path().join("canonical-secondary"))
        .arg("--wallet-primary-path")
        .arg(temporary.path().join("wallet-primary"))
        .arg("--wallet-secondary-root")
        .arg(temporary.path().join("wallet-secondary"))
        .args(["--ingest-control-addr", "http://127.0.0.1:9100"])
        .output()?;
    assert!(
        output.status.success(),
        "print-config failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(!stdout.contains("[node]"), "{stdout}");
    assert!(!stdout.contains("json_rpc_addr"), "{stdout}");
    Ok(())
}

/// A node setting asks for broadcast, so a `[node]` section without its
/// address fails configuration instead of starting a storage-only query.
#[test]
fn node_settings_without_an_address_are_rejected() -> eyre::Result<()> {
    let temporary = TempDir::new()?;
    let config_path = temporary.path().join("query-node.toml");
    std::fs::write(
        &config_path,
        "[node]\nrequest_timeout_secs = 30\nbroadcast_timeout_secs = 7\n",
    )?;
    let base = || {
        let mut command = Command::new(zinder_testkit::bin_exe!("zinder-query"));
        command
            .env_clear()
            .args(["--print-config", "--network", "zcash-regtest"])
            .arg("--canonical-primary-path")
            .arg(temporary.path().join("canonical-primary"))
            .arg("--canonical-secondary-root")
            .arg(temporary.path().join("canonical-secondary"))
            .arg("--wallet-primary-path")
            .arg(temporary.path().join("wallet-primary"))
            .arg("--wallet-secondary-root")
            .arg(temporary.path().join("wallet-secondary"))
            .args(["--ingest-control-addr", "http://127.0.0.1:9100"]);
        command
    };

    let from_file = base().arg("--config").arg(&config_path).output()?;
    let from_environment = base()
        .env("ZINDER_NODE__AUTH__METHOD", "cookie")
        .env("ZINDER_NODE__AUTH__PATH", "/var/run/auth/.cookie")
        .output()?;

    for output in [from_file, from_environment] {
        assert!(!output.status.success(), "{output:?}");
        let stderr = String::from_utf8(output.stderr)?;
        assert!(stderr.contains("node.json_rpc_addr"), "{stderr}");
    }
    Ok(())
}

/// Query reads the shared node list, so the environment one ingest reads
/// does not stop it, and its fallback tables print redacted.
#[test]
fn print_config_accepts_the_shared_node_fallback_list() -> eyre::Result<()> {
    let temporary = TempDir::new()?;
    let output = query_command(&temporary, &temporary.path().join("canonical-secondary"))
        .env(
            "ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR",
            "http://127.0.0.1:39232",
        )
        .env("ZINDER_NODE_FALLBACKS__1__AUTH__METHOD", "basic")
        .env("ZINDER_NODE_FALLBACKS__1__AUTH__USERNAME", "zakura")
        .env(
            "ZINDER_NODE_FALLBACKS__1__AUTH__PASSWORD",
            "fallback-secret",
        )
        .output()?;

    assert!(
        output.status.success(),
        "print-config failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("[node_fallbacks.1]"), "{stdout}");
    assert!(stdout.contains("json_rpc_addr = \"http://127.0.0.1:39232\""));
    assert!(!stdout.contains("fallback-secret"), "{stdout}");
    Ok(())
}

#[test]
fn print_config_exposes_the_full_block_retention_contract() -> eyre::Result<()> {
    let temporary = TempDir::new()?;
    let mut command = query_command(&temporary, &temporary.path().join("canonical-secondary"));
    command.args(["--raw-blob-policy", "all"]);
    let output = command.output()?;
    assert!(
        output.status.success(),
        "print-config failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8(output.stdout)?.contains("raw_blob_policy = \"all\""));
    Ok(())
}

#[test]
fn nested_secondary_roots_are_rejected_before_storage_open() -> eyre::Result<()> {
    let temporary = TempDir::new()?;
    let canonical_primary = temporary.path().join("canonical-primary");
    let nested_secondary = canonical_primary.join("query-secondary");
    let output = query_command(&temporary, &nested_secondary).output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("must be disjoint roots"));
    Ok(())
}

#[test]
fn public_bind_requires_an_explicit_security_opt_in() -> eyre::Result<()> {
    let temporary = TempDir::new()?;
    let mut command = query_command(&temporary, &temporary.path().join("canonical-secondary"));
    command.args(["--listen-addr", "0.0.0.0:9102"]);
    let output = command.output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("public"));
    Ok(())
}
