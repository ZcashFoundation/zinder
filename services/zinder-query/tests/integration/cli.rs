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
    let mut command = Command::new(zinder_testkit::bin_exe!("zinder-query"));
    command
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
    let output = command.output()?;
    assert!(
        output.status.success(),
        "print-config failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(!stdout.contains("[node]"), "{stdout}");
    assert!(!stdout.contains("json_rpc_addr"), "{stdout}");
    let lean = command
        .env("ZINDER_DEPLOYMENT__PROFILE", "wallet")
        .output()?;
    assert!(!lean.status.success(), "{lean:?}");
    assert!(String::from_utf8(lean.stderr)?.contains("requires [node]"));
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

/// The tree-state rebuild wait defaults to two seconds, follows its
/// environment variable, and refuses a wait the client deadline cannot hold.
#[test]
fn tree_state_replay_wait_defaults_to_two_seconds_and_is_bounded() -> eyre::Result<()> {
    let temporary = TempDir::new()?;
    let secondary = temporary.path().join("canonical-secondary");
    let rendered = |wait_ms: Option<&str>| -> eyre::Result<std::process::Output> {
        let mut command = query_command(&temporary, &secondary);
        command.env_remove("ZINDER_QUERY__TREE_STATE_REPLAY_WAIT_MS");
        if let Some(wait_ms) = wait_ms {
            command.env("ZINDER_QUERY__TREE_STATE_REPLAY_WAIT_MS", wait_ms);
        }
        Ok(command.output()?)
    };

    for (configured, expected) in [(None, "2000"), (Some("0"), "0"), (Some("5000"), "5000")] {
        let output = rendered(configured)?;
        assert!(
            output.status.success(),
            "print-config failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout)?;
        assert!(
            stdout.contains(&format!("tree_state_replay_wait_ms = {expected}")),
            "{stdout}"
        );
    }
    let too_long = rendered(Some("5001"))?;
    assert!(!too_long.status.success(), "{too_long:?}");
    let stderr = String::from_utf8(too_long.stderr)?;
    assert!(
        stderr.contains("query.tree_state_replay_wait_ms must not exceed 5000"),
        "{stderr}"
    );
    Ok(())
}

/// Tree-state rebuild waiters per slot default to 64, follow their
/// environment variable, and are refused outside 1..=256.
#[test]
fn tree_state_replay_waiters_per_slot_default_to_64_and_are_bounded() -> eyre::Result<()> {
    let temporary = TempDir::new()?;
    let secondary = temporary.path().join("canonical-secondary");
    let rendered = |waiters: Option<&str>| -> eyre::Result<std::process::Output> {
        let mut command = query_command(&temporary, &secondary);
        command.env_remove("ZINDER_QUERY__TREE_STATE_REPLAY_WAITERS_PER_SLOT");
        if let Some(waiters) = waiters {
            command.env("ZINDER_QUERY__TREE_STATE_REPLAY_WAITERS_PER_SLOT", waiters);
        }
        Ok(command.output()?)
    };

    for (configured, expected) in [(None, "64"), (Some("1"), "1"), (Some("256"), "256")] {
        let output = rendered(configured)?;
        assert!(
            output.status.success(),
            "print-config failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout)?;
        assert!(
            stdout.contains(&format!("tree_state_replay_waiters_per_slot = {expected}")),
            "{stdout}"
        );
    }
    for refused in ["0", "257"] {
        let output = rendered(Some(refused))?;
        assert!(!output.status.success(), "{output:?}");
        let stderr = String::from_utf8(output.stderr)?;
        assert!(
            stderr.contains("query.tree_state_replay_waiters_per_slot must be in 1..=256"),
            "{stderr}"
        );
    }
    Ok(())
}

/// Forwarded-read admission and cache settings render their defaults,
/// follow their environment variables, and are refused outside their ranges.
#[test]
fn forwarded_read_settings_default_and_are_bounded() -> eyre::Result<()> {
    const SETTINGS: [(&str, &str, &str, &str, &str); 4] = [
        (
            "FORWARDED_READ_MAX_CONCURRENT",
            "forwarded_read_max_concurrent",
            "32",
            "256",
            "257",
        ),
        (
            "FORWARDED_READ_WAIT_MS",
            "forwarded_read_wait_ms",
            "10000",
            "0",
            "30001",
        ),
        (
            "FORWARDED_READ_WAITERS_PER_SLOT",
            "forwarded_read_waiters_per_slot",
            "64",
            "256",
            "0",
        ),
        (
            "FORWARDED_TRANSACTION_CACHE_BYTES",
            "forwarded_transaction_cache_bytes",
            "67108864",
            "0",
            "1073741825",
        ),
    ];
    let temporary = TempDir::new()?;
    let secondary = temporary.path().join("canonical-secondary");
    let rendered = |variable: &str, value: Option<&str>| -> eyre::Result<std::process::Output> {
        let mut command = query_command(&temporary, &secondary);
        for (suffix, ..) in SETTINGS {
            command.env_remove(format!("ZINDER_QUERY__{suffix}"));
        }
        if let Some(value) = value {
            command.env(format!("ZINDER_QUERY__{variable}"), value);
        }
        Ok(command.output()?)
    };
    for (variable, key, default, accepted, refused) in SETTINGS {
        for (value, expected) in [(None, default), (Some(accepted), accepted)] {
            let output = rendered(variable, value)?;
            assert!(output.status.success(), "{output:?}");
            let stdout = String::from_utf8(output.stdout)?;
            assert!(stdout.contains(&format!("{key} = {expected}")), "{stdout}");
        }
        let output = rendered(variable, Some(refused))?;
        assert!(!output.status.success(), "{output:?}");
        let stderr = String::from_utf8(output.stderr)?;
        assert!(
            stderr.contains(&format!("query.{key} must be in")),
            "{stderr}"
        );
    }
    Ok(())
}

#[test]
fn wallet_profiles_render_without_injected_retention() -> eyre::Result<()> {
    let temporary = TempDir::new()?;
    let secondary = temporary.path().join("canonical-secondary");
    for profile in ["wallet", "wallet-archive"] {
        let output = query_command(&temporary, &secondary)
            .env("ZINDER_DEPLOYMENT__PROFILE", profile)
            .output()?;
        assert!(output.status.success(), "{output:?}");
        let stdout = String::from_utf8(output.stdout)?;
        assert!(
            stdout.contains(&format!("profile = \"{profile}\"")),
            "{stdout}"
        );
        assert!(!stdout.contains("raw_blob_policy ="), "{stdout}");
        let conflict = query_command(&temporary, &secondary)
            .env("ZINDER_DEPLOYMENT__PROFILE", profile)
            .env("ZINDER_STORAGE__RAW_BLOB_POLICY", "transactions")
            .output()?;
        assert!(!conflict.status.success(), "{conflict:?}");
        assert!(String::from_utf8(conflict.stderr)?.contains("storage.raw_blob_policy"));
    }
    Ok(())
}

#[tokio::test]
async fn wallet_profiles_refuse_checkpointed_canonical_history() -> eyre::Result<()> {
    use std::time::Duration;

    use zinder_core::{BlockHeight, Network};
    use zinder_store::RawBlobRetention;
    use zinder_testkit::{
        CanonicalBaseline, CanonicalWriterFixture, ChainFixture, IngestControlFixture,
        sample_regtest_upgrade_activations,
    };

    let network = Network::ZcashRegtest;
    let control = IngestControlFixture::spawn(network).await?;
    let activations = sample_regtest_upgrade_activations();
    for (profile, retention) in [
        ("wallet", RawBlobRetention::None),
        ("wallet-archive", RawBlobRetention::All),
    ] {
        let chain = ChainFixture::new(network)
            .with_raw_blob_retention(retention)
            .extend_blocks(2);
        let baseline = CanonicalBaseline::new(&chain).with_history_floor(BlockHeight::new(1));
        let writer = CanonicalWriterFixture::publish(&baseline, &activations)?;
        let reorg_window = writer
            .canonical_primary()?
            .reorg_policy()
            .reorg_window_blocks();
        let directory = tempfile::tempdir()?;
        let output = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::process::Command::new(zinder_testkit::bin_exe!("zinder-query"))
                .env_clear()
                .env("ZINDER_DEPLOYMENT__PROFILE", profile)
                .env("ZINDER_NETWORK__NAME", "zcash-regtest")
                .env("ZINDER_STORAGE__PATH", writer.canonical_primary_path())
                .env(
                    "ZINDER_STORAGE__SECONDARY_PATH",
                    directory.path().join("canonical-secondary"),
                )
                .env("ZINDER_WALLET__PATH", directory.path().join("wallet"))
                .env(
                    "ZINDER_WALLET__SECONDARY_PATH",
                    directory.path().join("wallet-secondary"),
                )
                .env("ZINDER_INGEST_CONTROL__ADDR", control.endpoint())
                .env("ZINDER_NODE__JSON_RPC_ADDR", "http://127.0.0.1:1")
                .env(
                    "ZINDER_QUERY__REORG_WINDOW_BLOCKS",
                    reorg_window.to_string(),
                )
                .kill_on_drop(true)
                .output(),
        )
        .await??;
        assert!(!output.status.success(), "{profile} unexpectedly started");
        let stderr = String::from_utf8(output.stderr)?;
        assert!(
            stderr.contains("lacks required BlockHistory coverage"),
            "{profile} failed for a different reason: {stderr}",
        );
    }
    control.shutdown().await?;
    Ok(())
}
