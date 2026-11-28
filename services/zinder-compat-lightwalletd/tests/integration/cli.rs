#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{fs, path::Path, process::Command};

use tempfile::tempdir;

#[tokio::test]
async fn explicit_args_use_the_callers_tracing_subscriber() -> eyre::Result<()> {
    const BINARY: &str = "zinder-compat-lightwalletd";
    zinder_runtime::install_tracing_subscriber()?;
    let exit = zinder_compat_lightwalletd::runtime::run_from_args(
        BINARY,
        [BINARY, "--print-config", "--network", "zcash-regtest"],
    )
    .await;
    assert_eq!(exit, std::process::ExitCode::SUCCESS);
    Ok(())
}

#[test]
fn version_reports_the_product_version() -> eyre::Result<()> {
    let output = zinder_compat_command().arg("--version").output()?;

    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        String::from_utf8(output.stdout)?,
        format!("zinder-compat-lightwalletd {}\n", env!("CARGO_PKG_VERSION"))
    );

    Ok(())
}

#[test]
fn print_config_renders_resolved_toml_to_stdout() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("compat-print-config-store");
    let secondary_path = tempdir.path().join("compat-print-config-secondary");
    let wallet_path = tempdir.path().join("compat-print-config-wallet");
    let wallet_secondary_path = wallet_path.with_extension("secondary");
    let config_path = tempdir.path().join("zinder-compat.toml");
    fs::write(
        &config_path,
        compat_config_toml(&storage_path, &secondary_path, &wallet_path)?,
    )?;

    let output = zinder_compat_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stdout.contains("[network]"), "{stdout}");
    assert!(stdout.contains("name = \"zcash-regtest\""), "{stdout}");
    assert!(stdout.contains("[compat]"), "{stdout}");
    assert!(stdout.contains("serving = \"wallet\""), "{stdout}");
    assert!(stdout.contains("browser_origins = []"), "{stdout}");
    assert!(stdout.contains("reorg_window_blocks = 100"), "{stdout}");
    assert!(
        stdout.contains("pair_convergence_attempts = 12"),
        "{stdout}"
    );
    assert!(
        stdout.contains("max_concurrent_block_range_streams = 100"),
        "{stdout}"
    );
    assert!(stdout.contains("ping_very_insecure = false"), "{stdout}");
    assert!(
        stdout.contains("listen_addr = \"127.0.0.1:9067\""),
        "{stdout}"
    );
    assert!(
        stdout.contains(&format!(
            "secondary_path = \"{}\"",
            path_str(&secondary_path)?
        )),
        "{stdout}"
    );
    assert!(stdout.contains("[storage.canonical.rocksdb]"), "{stdout}");
    assert!(stdout.contains("[wallet]"), "{stdout}");
    assert!(
        stdout.contains(&format!("path = \"{}\"", path_str(&wallet_path)?)),
        "{stdout}"
    );
    assert!(
        stdout.contains(&format!(
            "secondary_path = \"{}\"",
            path_str(&wallet_secondary_path)?
        )),
        "{stdout}"
    );
    assert!(stdout.contains("[wallet.rocksdb]"), "{stdout}");
    assert!(stdout.contains("[ingest_control]"), "{stdout}");
    assert!(
        stdout.contains("addr = \"http://127.0.0.1:9100\""),
        "{stdout}"
    );
    assert!(!stderr.contains("ERROR"), "{stderr}");

    Ok(())
}

#[test]
fn compact_blocks_print_config_omits_wallet_configuration() -> eyre::Result<()> {
    let temporary = tempdir()?;
    let storage_path = temporary.path().join("compact-blocks-store");
    let secondary_path = temporary.path().join("compact-blocks-secondary");
    let config_path = temporary.path().join("compact-blocks.toml");
    fs::write(
        &config_path,
        compact_blocks_config_toml(&storage_path, &secondary_path)?,
    )?;

    let output = zinder_compat_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("serving = \"compact-blocks\""), "{stdout}");
    assert!(!stdout.contains("[wallet]"), "{stdout}");
    assert!(!stdout.contains("wallet.path"), "{stdout}");
    Ok(())
}

#[test]
fn browser_origins_read_the_file_and_honor_environment_overrides() -> eyre::Result<()> {
    let temporary = tempdir()?;
    let config_path = temporary.path().join("compat.toml");
    let config = compact_blocks_config_toml(
        &temporary.path().join("store"),
        &temporary.path().join("secondary"),
    )?;
    fs::write(
        &config_path,
        format!(
            "{config}browser_origins = [\"https://wallet.example\", \"http://localhost:5173\"]\n"
        ),
    )?;
    let expected_file: &[&str] = &["https://wallet.example", "http://localhost:5173"];
    for (environment, expected) in [
        (None, expected_file),
        (
            Some("HTTPS://Wallet.Example:443/"),
            &["https://wallet.example"],
        ),
        (
            Some("http://[::1]:5173, https://wallet.example"),
            &["http://[::1]:5173", "https://wallet.example"],
        ),
        (Some("*"), &["*"][..]),
        (Some("none"), &[][..]),
    ] {
        let mut command = zinder_compat_command();
        command
            .args(["--print-config", "--config"])
            .arg(&config_path);
        if let Some(environment) = environment {
            command.env("ZINDER_COMPAT__BROWSER_ORIGINS", environment);
        }
        let output = command.output()?;
        assert!(output.status.success(), "{output:?}");
        let printed: toml::Table = toml::from_str(&String::from_utf8(output.stdout)?)?;
        assert_eq!(
            printed["compat"]["browser_origins"],
            toml::Value::try_from(expected)?
        );
    }
    Ok(())
}

#[test]
fn browser_origins_reject_invalid_or_ambiguous_configuration_without_echoing_credentials()
-> eyre::Result<()> {
    let temporary = tempdir()?;
    let config_path = temporary.path().join("compat.toml");
    fs::write(
        &config_path,
        compact_blocks_config_toml(
            &temporary.path().join("store"),
            &temporary.path().join("secondary"),
        )?,
    )?;
    for invalid in [
        "",
        "null",
        "file:///wallet",
        "https://wallet.example/path",
        "https://wallet.example/path/..",
        "https://wallet.example?query",
        "https://wallet.example#fragment",
        "https://*.example",
        "https://%2a.example",
        "https://wallet.example,*",
        "https://user:private-password@wallet.example",
        "https://wallet.example,",
    ] {
        let output = zinder_compat_command()
            .args(["--print-config", "--config"])
            .arg(&config_path)
            .env("ZINDER_COMPAT__BROWSER_ORIGINS", invalid)
            .output()?;
        assert!(!output.status.success(), "{output:?}");
        let stderr = String::from_utf8(output.stderr)?;
        assert!(stderr.contains("compat.browser_origins"), "{stderr}");
        assert!(!stderr.contains("private-password"), "{stderr}");
    }
    Ok(())
}

/// Wallet-mode compatibility reads the shared node list, so its fallback
/// tables print redacted, and a list without `[node]` is refused.
#[test]
fn wallet_serving_accepts_node_fallbacks_only_with_a_primary_node() -> eyre::Result<()> {
    let temporary = tempdir()?;
    let config_path = temporary.path().join("compat-wallet.toml");
    fs::write(
        &config_path,
        compat_config_toml(
            &temporary.path().join("compat-store"),
            &temporary.path().join("compat-secondary"),
            &temporary.path().join("compat-wallet"),
        )?,
    )?;
    let with_fallback = |command: &mut Command| {
        command
            .args(["--print-config", "--config"])
            .arg(&config_path)
            .env(
                "ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR",
                "http://127.0.0.1:28232",
            )
            .env("ZINDER_NODE_FALLBACKS__1__AUTH__METHOD", "cookie")
            .env("ZINDER_NODE_FALLBACKS__1__AUTH__COOKIE", "zakura:secret")
            .output()
    };

    let output = with_fallback(
        zinder_compat_command().env("ZINDER_NODE__JSON_RPC_ADDR", "http://127.0.0.1:18232"),
    )?;
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("[node_fallbacks.1]"), "{stdout}");
    assert!(!stdout.contains("zakura:secret"), "{stdout}");

    let without_primary = with_fallback(&mut zinder_compat_command())?;
    assert!(!without_primary.status.success(), "{without_primary:?}");
    let stderr = String::from_utf8(without_primary.stderr)?;
    assert!(stderr.contains("requires [node]"), "{stderr}");
    Ok(())
}

/// The wallet-serving image runs compatibility beside native query, on its own
/// secondary roots and with the cache budget pinned as query's is.
#[test]
fn wallet_serving_image_config_resolves() -> eyre::Result<()> {
    let temporary = tempdir()?;
    let token_path = temporary.path().join("ingest.token");
    fs::write(&token_path, "fixture-token\n")?;

    let output = zinder_compat_command()
        .args(["--print-config"])
        .env("ZINDER_DEPLOYMENT__PROFILE", "wallet")
        .env("ZINDER_NETWORK__NAME", "zcash-testnet")
        .env("ZINDER_NODE__JSON_RPC_ADDR", "http://127.0.0.1:18232")
        .env("ZINDER_STORAGE__PATH", "/var/lib/zinder/canonical")
        .env(
            "ZINDER_STORAGE__SECONDARY_PATH",
            "/var/lib/zinder/compat/canonical-secondary",
        )
        .env(
            "ZINDER_STORAGE__CANONICAL__ROCKSDB__BLOCK_CACHE_BYTES",
            "268435456",
        )
        .env("ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_OPEN_FILES", "1024")
        .env("ZINDER_WALLET__PATH", "/var/lib/zinder/wallet")
        .env(
            "ZINDER_WALLET__SECONDARY_PATH",
            "/var/lib/zinder/compat/wallet-secondary",
        )
        .env("ZINDER_INGEST_CONTROL__ADDR", "http://127.0.0.1:9100")
        .env(
            "ZINDER_INGEST_CONTROL__BEARER_TOKEN_PATH",
            path_str(&token_path)?,
        )
        .env("ZINDER_COMPAT__LISTEN_ADDR", "[::]:9067")
        .env("ZINDER_COMPAT__REORG_WINDOW_BLOCKS", "1000")
        .env("ZINDER_OPS__LISTEN_ADDR", "[::]:9107")
        .env("ZINDER_SECURITY__ALLOW_PUBLIC_BIND", "true")
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    for expected in [
        "path = \"/var/lib/zinder/canonical\"",
        "secondary_path = \"/var/lib/zinder/compat/canonical-secondary\"",
        "block_cache_bytes = 268435456",
        "max_open_files = 1024",
        "path = \"/var/lib/zinder/wallet\"",
        "secondary_path = \"/var/lib/zinder/compat/wallet-secondary\"",
        "addr = \"http://127.0.0.1:9100\"",
        "listen_addr = \"[::]:9067\"",
        "profile = \"wallet\"",
        "listen_addr = \"[::]:9107\"",
        "allow_public_bind = true",
    ] {
        assert!(stdout.contains(expected), "missing {expected}: {stdout}");
    }

    Ok(())
}

#[test]
fn compact_blocks_serving_can_be_selected_by_cli_and_environment() -> eyre::Result<()> {
    let temporary = tempdir()?;
    let storage_path = temporary.path().join("compact-blocks-store");
    let secondary_path = temporary.path().join("compact-blocks-secondary");
    let config_path = temporary.path().join("compact-blocks.toml");
    fs::write(
        &config_path,
        compact_blocks_config_toml(&storage_path, &secondary_path)?,
    )?;

    let cli_output = zinder_compat_command()
        .args([
            "--print-config",
            "--config",
            path_str(&config_path)?,
            "--serving",
            "compact-blocks",
        ])
        .output()?;
    assert!(cli_output.status.success(), "{cli_output:?}");
    assert!(String::from_utf8(cli_output.stdout)?.contains("serving = \"compact-blocks\""));

    let env_output = zinder_compat_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env("ZINDER_COMPAT__SERVING", "compact-blocks")
        .output()?;
    assert!(env_output.status.success(), "{env_output:?}");
    assert!(String::from_utf8(env_output.stdout)?.contains("serving = \"compact-blocks\""));
    Ok(())
}

#[test]
fn compact_blocks_rejects_explicit_wallet_configuration() -> eyre::Result<()> {
    let temporary = tempdir()?;
    let storage_path = temporary.path().join("compact-blocks-store");
    let secondary_path = temporary.path().join("compact-blocks-secondary");
    let wallet_path = temporary.path().join("wallet");
    let config_path = temporary.path().join("compact-blocks.toml");
    let config = format!(
        "{}\n[wallet]\npath = \"{}\"\nsecondary_path = \"{}\"\n",
        compact_blocks_config_toml(&storage_path, &secondary_path)?,
        path_str(&wallet_path)?,
        path_str(&wallet_path.with_extension("secondary"))?,
    );
    fs::write(&config_path, config)?;

    let output = zinder_compat_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .output()?;
    assert!(!output.status.success(), "{output:?}");
    assert!(String::from_utf8(output.stderr)?.contains("does not accept wallet.path"));
    Ok(())
}

/// Compact-block serving never contacts a node, so a node setting from the
/// file, the command line, or the environment, fallback endpoints included,
/// is a configuration error.
#[test]
fn compact_blocks_rejects_node_configuration() -> eyre::Result<()> {
    let temporary = tempdir()?;
    let storage_path = temporary.path().join("compact-blocks-store");
    let secondary_path = temporary.path().join("compact-blocks-secondary");
    let config_path = temporary.path().join("compact-blocks.toml");
    fs::write(
        &config_path,
        compact_blocks_config_toml(&storage_path, &secondary_path)?,
    )?;
    let file_with_node = temporary.path().join("compact-blocks-node.toml");
    fs::write(
        &file_with_node,
        format!(
            "{}\n[node]\nrequest_timeout_secs = 30\n",
            compact_blocks_config_toml(&storage_path, &secondary_path)?
        ),
    )?;

    let from_file = zinder_compat_command()
        .args(["--print-config", "--config", path_str(&file_with_node)?])
        .output()?;
    let from_cli = zinder_compat_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .args(["--node-json-rpc-addr", "http://127.0.0.1:29232"])
        .output()?;
    let from_environment = zinder_compat_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env("ZINDER_NODE__AUTH__METHOD", "cookie")
        .output()?;
    let from_fallbacks = zinder_compat_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env(
            "ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR",
            "http://127.0.0.1:28232",
        )
        .output()?;

    for output in [from_file, from_cli, from_environment, from_fallbacks] {
        assert!(!output.status.success(), "{output:?}");
        let stderr = String::from_utf8(output.stderr)?;
        assert!(stderr.contains("does not accept node settings"), "{stderr}");
    }
    Ok(())
}

/// In wallet mode a node setting asks for broadcast, so a `[node]` section
/// without its address fails configuration instead of starting without
/// `SendTransaction`.
#[test]
fn wallet_node_settings_without_an_address_are_rejected() -> eyre::Result<()> {
    let temporary = tempdir()?;
    let config_path = temporary.path().join("compat-wallet.toml");
    let config = compat_config_toml(
        &temporary.path().join("compat-store"),
        &temporary.path().join("compat-secondary"),
        &temporary.path().join("compat-wallet"),
    )?;
    fs::write(&config_path, &config)?;
    let file_with_node = temporary.path().join("compat-wallet-node.toml");
    fs::write(
        &file_with_node,
        format!("{config}\n[node]\nrequest_timeout_secs = 30\n"),
    )?;

    let from_file = zinder_compat_command()
        .args(["--print-config", "--config", path_str(&file_with_node)?])
        .output()?;
    let from_environment = zinder_compat_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
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

#[test]
fn light_wallet_profile_resolves_with_standard_binary() -> eyre::Result<()> {
    let temporary = tempdir()?;
    let path = temporary.path().join("compat.toml");
    let config = compact_blocks_config_toml(
        &temporary.path().join("canonical"),
        &temporary.path().join("secondary"),
    )?
    .replace("serving = \"compact-blocks\"\n", "");
    fs::write(
        &path,
        format!("{config}\n[node]\njson_rpc_addr = \"http://127.0.0.1:1\"\n"),
    )?;
    let output = zinder_compat_command()
        .args(["--print-config", "--config", path_str(&path)?])
        .env("ZINDER_DEPLOYMENT__PROFILE", "light-wallet")
        .output()?;
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("[deployment]"), "{stdout}");
    assert!(stdout.contains("profile = \"light-wallet\""), "{stdout}");
    assert!(!stdout.contains("serving ="), "{stdout}");
    assert!(!stdout.contains("raw_blob_policy ="), "{stdout}");
    assert!(!stdout.contains("[wallet]"), "{stdout}");

    let conflict = zinder_compat_command()
        .args(["--print-config", "--config", path_str(&path)?])
        .env("ZINDER_DEPLOYMENT__PROFILE", "light-wallet")
        .env("ZINDER_COMPAT__SERVING", "compact-blocks")
        .output()?;
    assert!(!conflict.status.success(), "{conflict:?}");
    assert!(String::from_utf8(conflict.stderr)?.contains("remove conflicting overrides"));
    let legacy_key = zinder_compat_command()
        .args(["--print-config", "--config", path_str(&path)?])
        .env("ZINDER_PROFILE", "light-wallet")
        .output()?;
    assert!(!legacy_key.status.success(), "{legacy_key:?}");

    Ok(())
}

#[test]
fn compatibility_serving_rejects_unapproved_aliases() -> eyre::Result<()> {
    let temporary = tempdir()?;
    let storage_path = temporary.path().join("compat-alias-store");
    let secondary_path = temporary.path().join("compat-alias-secondary");
    let config_path = temporary.path().join("compat-alias.toml");
    let config = compact_blocks_config_toml(&storage_path, &secondary_path)?;
    for alias in ["canonical", "light-wallet-qualification"] {
        fs::write(
            &config_path,
            config.replace(
                "serving = \"compact-blocks\"",
                &format!("serving = \"{alias}\""),
            ),
        )?;
        let output = zinder_compat_command()
            .args(["--print-config", "--config", path_str(&config_path)?])
            .output()?;
        assert!(!output.status.success(), "{output:?}");
        assert!(String::from_utf8(output.stderr)?.contains("serving"));
    }
    let cli_alias = zinder_compat_command()
        .args(["--print-config", "--serving", "light-wallet-qualification"])
        .output()?;
    assert!(!cli_alias.status.success(), "{cli_alias:?}");
    assert!(String::from_utf8(cli_alias.stderr)?.contains("invalid value"));
    Ok(())
}

#[test]
fn omitted_paths_default_from_data_dir_and_partial_explicit_paths_require_storage_path()
-> eyre::Result<()> {
    let default_output = zinder_compat_command()
        .args(["--print-config", "--network", "zcash-regtest"])
        .output()?;
    assert!(default_output.status.success(), "{default_output:?}");
    let default_stdout = String::from_utf8(default_output.stdout)?;
    assert!(
        default_stdout.contains("path = \"/var/lib/zinder/canonical\""),
        "{default_stdout}"
    );
    assert!(
        default_stdout.contains(
            "secondary_path = \"/var/lib/zinder/canonical-secondary/compat-lightwalletd\""
        ),
        "{default_stdout}"
    );
    assert!(
        default_stdout.contains("path = \"/var/lib/zinder/wallet\""),
        "{default_stdout}"
    );
    assert!(
        default_stdout
            .contains("secondary_path = \"/var/lib/zinder/wallet-secondary/compat-lightwalletd\""),
        "{default_stdout}"
    );

    let temporary = tempdir()?;
    let derived_root = temporary.path().join("custom-root");
    let derived_output = zinder_compat_command()
        .args(["--print-config", "--network", "zcash-regtest"])
        .env("ZINDER_STORAGE__DATA_DIR", &derived_root)
        .output()?;
    assert!(derived_output.status.success(), "{derived_output:?}");
    let derived_stdout = String::from_utf8(derived_output.stdout)?;
    assert!(
        derived_stdout.contains(&format!(
            "path = \"{}\"",
            derived_root.join("canonical").display()
        )),
        "{derived_stdout}"
    );
    assert!(
        derived_stdout.contains(&format!(
            "secondary_path = \"{}\"",
            derived_root
                .join("canonical-secondary/compat-lightwalletd")
                .display()
        )),
        "{derived_stdout}"
    );

    let partial_output = zinder_compat_command()
        .args([
            "--print-config",
            "--network",
            "zcash-regtest",
            "--canonical-secondary-root",
            path_str(&temporary.path().join("explicit-secondary"))?,
            "--wallet-primary-path",
            path_str(&temporary.path().join("explicit-wallet"))?,
            "--wallet-secondary-root",
            path_str(&temporary.path().join("explicit-wallet-secondary"))?,
        ])
        .output()?;
    assert!(!partial_output.status.success());
    let stderr = String::from_utf8(partial_output.stderr)?;
    assert!(
        stderr.contains("missing configuration field: storage.path"),
        "{stderr}"
    );

    Ok(())
}

#[test]
fn zero_reorg_window_is_rejected_before_storage_open() -> eyre::Result<()> {
    assert_compat_cli_rejects(
        |root| {
            compat_config_toml(
                &root.join("compat-zero-window-store"),
                &root.join("compat-zero-window-secondary"),
                &root.join("compat-zero-window-wallet"),
            )
        },
        &["--reorg-window-blocks", "0"],
        "canonical build reorg window must be greater than zero",
    )
}

/// `--ping-very-insecure` and `ZINDER_COMPAT__PING_VERY_INSECURE` turn on
/// `Ping`, matching the lightwalletd flag name.
#[test]
fn ping_very_insecure_is_enabled_by_flag_or_environment() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let config_path = tempdir.path().join("zinder-compat.toml");
    fs::write(
        &config_path,
        compat_config_toml(
            &tempdir.path().join("compat-ping-store"),
            &tempdir.path().join("compat-ping-secondary"),
            &tempdir.path().join("compat-ping-wallet"),
        )?,
    )?;

    let by_flag = zinder_compat_command()
        .args([
            "--print-config",
            "--config",
            path_str(&config_path)?,
            "--ping-very-insecure",
        ])
        .output()?;
    let by_environment = zinder_compat_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env("ZINDER_COMPAT__PING_VERY_INSECURE", "true")
        .output()?;

    for output in [by_flag, by_environment] {
        assert!(output.status.success(), "{output:?}");
        let stdout = String::from_utf8(output.stdout)?;
        assert!(stdout.contains("ping_very_insecure = true"), "{stdout}");
    }
    Ok(())
}

/// The tree-state rebuild wait defaults to two seconds, follows its
/// environment variable, and refuses a wait above five seconds, as in
/// `zinder-query`.
#[test]
fn tree_state_replay_wait_defaults_to_two_seconds_and_is_bounded() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let config_path = tempdir.path().join("zinder-compat.toml");
    fs::write(
        &config_path,
        compat_config_toml(
            &tempdir.path().join("compat-replay-wait-store"),
            &tempdir.path().join("compat-replay-wait-secondary"),
            &tempdir.path().join("compat-replay-wait-wallet"),
        )?,
    )?;
    let rendered = |wait_ms: Option<&str>| -> eyre::Result<std::process::Output> {
        let mut command = zinder_compat_command();
        command.args(["--print-config", "--config", path_str(&config_path)?]);
        command.env_remove("ZINDER_COMPAT__TREE_STATE_REPLAY_WAIT_MS");
        if let Some(wait_ms) = wait_ms {
            command.env("ZINDER_COMPAT__TREE_STATE_REPLAY_WAIT_MS", wait_ms);
        }
        Ok(command.output()?)
    };

    for (configured, expected) in [(None, "2000"), (Some("0"), "0"), (Some("5000"), "5000")] {
        let output = rendered(configured)?;
        assert!(output.status.success(), "{output:?}");
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
        stderr.contains("compat.tree_state_replay_wait_ms must not exceed 5000"),
        "{stderr}"
    );
    Ok(())
}

/// Wallet serving's tree-state rebuild waiters per slot default to 64,
/// follow their environment variable, and are refused outside 1..=256.
#[test]
fn tree_state_replay_waiters_per_slot_default_to_64_and_are_bounded() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let config_path = tempdir.path().join("zinder-compat.toml");
    fs::write(
        &config_path,
        compat_config_toml(
            &tempdir.path().join("compat-replay-waiters-store"),
            &tempdir.path().join("compat-replay-waiters-secondary"),
            &tempdir.path().join("compat-replay-waiters-wallet"),
        )?,
    )?;
    let rendered = |waiters: Option<&str>| -> eyre::Result<std::process::Output> {
        let mut command = zinder_compat_command();
        command.args(["--print-config", "--config", path_str(&config_path)?]);
        command.env_remove("ZINDER_COMPAT__TREE_STATE_REPLAY_WAITERS_PER_SLOT");
        if let Some(waiters) = waiters {
            command.env("ZINDER_COMPAT__TREE_STATE_REPLAY_WAITERS_PER_SLOT", waiters);
        }
        Ok(command.output()?)
    };

    for (configured, expected) in [(None, "64"), (Some("1"), "1"), (Some("256"), "256")] {
        let output = rendered(configured)?;
        assert!(output.status.success(), "{output:?}");
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
            stderr.contains("compat.tree_state_replay_waiters_per_slot must be in 1..=256"),
            "{stderr}"
        );
    }
    Ok(())
}

const FORWARDED_READ_SETTINGS: [(&str, &str, &str, &str, &str); 4] = [
    (
        "FORWARDED_READ_MAX_CONCURRENT",
        "forwarded_read_max_concurrent",
        "32",
        "256",
        "0",
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
        "257",
    ),
    (
        "FORWARDED_TRANSACTION_CACHE_BYTES",
        "forwarded_transaction_cache_bytes",
        "67108864",
        "0",
        "1073741825",
    ),
];

/// Wallet serving's forwarded-read admission and cache settings render their
/// defaults, follow their environment variables, and are refused outside
/// their ranges.
#[test]
fn forwarded_read_settings_default_and_are_bounded() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let config_path = tempdir.path().join("zinder-compat.toml");
    fs::write(
        &config_path,
        compat_config_toml(
            &tempdir.path().join("compat-forwarded-store"),
            &tempdir.path().join("compat-forwarded-secondary"),
            &tempdir.path().join("compat-forwarded-wallet"),
        )?,
    )?;
    let rendered = |variable: &str, value: Option<&str>| -> eyre::Result<std::process::Output> {
        let mut command = zinder_compat_command();
        command.args(["--print-config", "--config", path_str(&config_path)?]);
        for (suffix, ..) in FORWARDED_READ_SETTINGS {
            command.env_remove(format!("ZINDER_COMPAT__{suffix}"));
        }
        if let Some(value) = value {
            command.env(format!("ZINDER_COMPAT__{variable}"), value);
        }
        Ok(command.output()?)
    };
    for (variable, key, default, accepted, refused) in FORWARDED_READ_SETTINGS {
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
            stderr.contains(&format!("compat.{key} must be in")),
            "{stderr}"
        );
    }
    Ok(())
}

/// Compact-block serving forwards no wallet read, so it refuses every
/// forwarded-read setting and renders none.
#[test]
fn compact_blocks_rejects_the_forwarded_read_settings() -> eyre::Result<()> {
    let temporary = tempdir()?;
    let config_path = temporary.path().join("compact-blocks.toml");
    fs::write(
        &config_path,
        compact_blocks_config_toml(
            &temporary.path().join("compact-blocks-store"),
            &temporary.path().join("compact-blocks-secondary"),
        )?,
    )?;
    let command = || -> eyre::Result<std::process::Command> {
        let mut command = zinder_compat_command();
        command.args(["--print-config", "--config", path_str(&config_path)?]);
        for (suffix, ..) in FORWARDED_READ_SETTINGS {
            command.env_remove(format!("ZINDER_COMPAT__{suffix}"));
        }
        Ok(command)
    };
    for (variable, key, default, ..) in FORWARDED_READ_SETTINGS {
        let output = command()?
            .env(format!("ZINDER_COMPAT__{variable}"), default)
            .output()?;
        assert!(!output.status.success(), "{output:?}");
        let stderr = String::from_utf8(output.stderr)?;
        assert!(
            stderr.contains(&format!("does not accept compat.{key}")),
            "{stderr}"
        );
    }
    let output = command()?.output()?;
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(!stdout.contains("forwarded_"), "{stdout}");
    Ok(())
}

/// Compact-block serving answers no tree-state request, so it refuses the
/// tree-state rebuild wait and waiters like any other wallet setting.
#[test]
fn compact_blocks_rejects_the_tree_state_replay_wait() -> eyre::Result<()> {
    let temporary = tempdir()?;
    let config_path = temporary.path().join("compact-blocks.toml");
    fs::write(
        &config_path,
        compact_blocks_config_toml(
            &temporary.path().join("compact-blocks-store"),
            &temporary.path().join("compact-blocks-secondary"),
        )?,
    )?;

    let output = zinder_compat_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env("ZINDER_COMPAT__TREE_STATE_REPLAY_WAIT_MS", "1000")
        .output()?;
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("does not accept compat.tree_state_replay_wait_ms"),
        "{stderr}"
    );

    let waiters = zinder_compat_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env_remove("ZINDER_COMPAT__TREE_STATE_REPLAY_WAIT_MS")
        .env("ZINDER_COMPAT__TREE_STATE_REPLAY_WAITERS_PER_SLOT", "8")
        .output()?;
    assert!(!waiters.status.success(), "{waiters:?}");
    let stderr = String::from_utf8(waiters.stderr)?;
    assert!(
        stderr.contains("does not accept compat.tree_state_replay_waiters_per_slot"),
        "{stderr}"
    );

    let default = zinder_compat_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env_remove("ZINDER_COMPAT__TREE_STATE_REPLAY_WAIT_MS")
        .env_remove("ZINDER_COMPAT__TREE_STATE_REPLAY_WAITERS_PER_SLOT")
        .output()?;
    assert!(default.status.success(), "{default:?}");
    let stdout = String::from_utf8(default.stdout)?;
    assert!(
        !stdout.contains("tree_state_replay_wait_ms"),
        "compact-block serving must not render a tree-state wait"
    );
    assert!(
        !stdout.contains("tree_state_replay_waiters_per_slot"),
        "compact-block serving must not render tree-state waiters"
    );
    Ok(())
}

/// The stale-tip limit defaults to 25 minutes on every network and is
/// bounded.
///
/// An operator may set any limit of at least five minutes, or 0 to serve
/// through any outage.
#[test]
fn stale_tip_limit_defaults_to_25_minutes_and_is_bounded() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let config_path = tempdir.path().join("zinder-compat.toml");
    fs::write(
        &config_path,
        compat_config_toml(
            &tempdir.path().join("compat-stale-tip-store"),
            &tempdir.path().join("compat-stale-tip-secondary"),
            &tempdir.path().join("compat-stale-tip-wallet"),
        )?,
    )?;
    let rendered = |network: &str, limit_ms: Option<&str>| -> eyre::Result<std::process::Output> {
        let mut command = zinder_compat_command();
        command.args(["--print-config", "--config", path_str(&config_path)?]);
        command.args(["--network", network]);
        command.env_remove("ZINDER_COMPAT__STALE_TIP_LIMIT_MS");
        if let Some(limit_ms) = limit_ms {
            command.env("ZINDER_COMPAT__STALE_TIP_LIMIT_MS", limit_ms);
        }
        Ok(command.output()?)
    };

    for (network, configured, expected) in [
        ("zcash-regtest", None, "1500000"),
        ("zcash-testnet", None, "1500000"),
        ("zcash-mainnet", None, "1500000"),
        ("zcash-mainnet", Some("0"), "0"),
        ("zcash-regtest", Some("300000"), "300000"),
    ] {
        let output = rendered(network, configured)?;
        assert!(output.status.success(), "{output:?}");
        let stdout = String::from_utf8(output.stdout)?;
        assert!(
            stdout.contains(&format!("stale_tip_limit_ms = {expected}")),
            "{network} {configured:?}: {stdout}"
        );
    }
    let too_short = rendered("zcash-mainnet", Some("299999"))?;
    assert!(!too_short.status.success(), "{too_short:?}");
    let stderr = String::from_utf8(too_short.stderr)?;
    assert!(
        stderr.contains("compat.stale_tip_limit_ms must be 0 or at least 300000"),
        "{stderr}"
    );
    Ok(())
}

/// Prints the effective configuration with the given environment settings.
fn print_config_with_env(
    config_path: &Path,
    settings: &[(&str, &str)],
) -> eyre::Result<std::process::Output> {
    let mut command = zinder_compat_command();
    command.args(["--print-config", "--config", path_str(config_path)?]);
    command.envs(settings.iter().copied());
    Ok(command.output()?)
}

/// The tip wait defaults to 3000 ms, 2 blocks, and 256 waiters, and 0 turns
/// the wait off.
#[test]
fn tip_wait_defaults_to_three_seconds_two_blocks_and_256_waiters() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let config_path = tempdir.path().join("zinder-compat.toml");
    fs::write(
        &config_path,
        compat_config_toml(
            &tempdir.path().join("compat-tip-wait-store"),
            &tempdir.path().join("compat-tip-wait-secondary"),
            &tempdir.path().join("compat-tip-wait-wallet"),
        )?,
    )?;

    let defaults = print_config_with_env(&config_path, &[])?;
    assert!(defaults.status.success(), "{defaults:?}");
    let stdout = String::from_utf8(defaults.stdout)?;
    assert!(stdout.contains("tip_wait_ms = 3000"), "{stdout}");
    assert!(stdout.contains("tip_wait_max_blocks = 2"), "{stdout}");
    assert!(stdout.contains("tip_wait_max_waiters = 256"), "{stdout}");

    let disabled = print_config_with_env(&config_path, &[("ZINDER_COMPAT__TIP_WAIT_MS", "0")])?;
    assert!(disabled.status.success(), "{disabled:?}");
    let stdout = String::from_utf8(disabled.stdout)?;
    assert!(stdout.contains("tip_wait_ms = 0"), "{stdout}");

    let raised = print_config_with_env(
        &config_path,
        &[
            ("ZINDER_COMPAT__TIP_WAIT_MS", "60000"),
            ("ZINDER_COMPAT__TIP_WAIT_MAX_BLOCKS", "1000"),
            ("ZINDER_COMPAT__TIP_WAIT_MAX_WAITERS", "4096"),
        ],
    )?;
    assert!(raised.status.success(), "{raised:?}");
    let stdout = String::from_utf8(raised.stdout)?;
    assert!(stdout.contains("tip_wait_ms = 60000"), "{stdout}");
    assert!(stdout.contains("tip_wait_max_blocks = 1000"), "{stdout}");
    assert!(stdout.contains("tip_wait_max_waiters = 4096"), "{stdout}");
    Ok(())
}

/// A nonzero tip wait must span two catch-up intervals, because a shorter
/// wait cannot reliably see the next publication.
#[test]
fn tip_wait_shorter_than_two_catchup_intervals_is_rejected() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let config_path = tempdir.path().join("zinder-compat.toml");
    fs::write(
        &config_path,
        compat_config_toml(
            &tempdir.path().join("compat-tip-wait-short-store"),
            &tempdir.path().join("compat-tip-wait-short-secondary"),
            &tempdir.path().join("compat-tip-wait-short-wallet"),
        )?,
    )?;

    let too_short = print_config_with_env(
        &config_path,
        &[
            ("ZINDER_STORAGE__SECONDARY_CATCHUP_INTERVAL_MS", "1000"),
            ("ZINDER_COMPAT__TIP_WAIT_MS", "1999"),
        ],
    )?;
    assert!(!too_short.status.success(), "{too_short:?}");
    let stderr = String::from_utf8(too_short.stderr)?;
    assert!(
        stderr.contains("compat.tip_wait_ms must be 0 or at least twice")
            && stderr.contains("storage.secondary_catchup_interval_ms (2000)"),
        "{stderr}"
    );

    let boundary = print_config_with_env(
        &config_path,
        &[
            ("ZINDER_STORAGE__SECONDARY_CATCHUP_INTERVAL_MS", "1000"),
            ("ZINDER_COMPAT__TIP_WAIT_MS", "2000"),
        ],
    )?;
    assert!(boundary.status.success(), "{boundary:?}");

    let slow_catchup = print_config_with_env(
        &config_path,
        &[("ZINDER_STORAGE__SECONDARY_CATCHUP_INTERVAL_MS", "2000")],
    )?;
    assert!(!slow_catchup.status.success(), "{slow_catchup:?}");
    let stderr = String::from_utf8(slow_catchup.stderr)?;
    assert!(
        stderr.contains("storage.secondary_catchup_interval_ms (4000)"),
        "{stderr}"
    );

    let disabled_with_slow_catchup = print_config_with_env(
        &config_path,
        &[
            ("ZINDER_STORAGE__SECONDARY_CATCHUP_INTERVAL_MS", "2000"),
            ("ZINDER_COMPAT__TIP_WAIT_MS", "0"),
        ],
    )?;
    assert!(
        disabled_with_slow_catchup.status.success(),
        "{disabled_with_slow_catchup:?}"
    );
    Ok(())
}

#[test]
fn tip_wait_blocks_and_waiters_of_zero_are_rejected() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let config_path = tempdir.path().join("zinder-compat.toml");
    fs::write(
        &config_path,
        compat_config_toml(
            &tempdir.path().join("compat-tip-wait-zero-store"),
            &tempdir.path().join("compat-tip-wait-zero-secondary"),
            &tempdir.path().join("compat-tip-wait-zero-wallet"),
        )?,
    )?;

    for (variable, message) in [
        (
            "ZINDER_COMPAT__TIP_WAIT_MAX_BLOCKS",
            "compat.tip_wait_max_blocks must be at least 1",
        ),
        (
            "ZINDER_COMPAT__TIP_WAIT_MAX_WAITERS",
            "compat.tip_wait_max_waiters must be at least 1",
        ),
    ] {
        let output = print_config_with_env(&config_path, &[(variable, "0")])?;
        assert!(!output.status.success(), "{variable}: {output:?}");
        let stderr = String::from_utf8(output.stderr)?;
        assert!(stderr.contains(message), "{variable}: {stderr}");
    }
    Ok(())
}

/// Every serving mode answers height-selected block reads, so compact-block
/// serving accepts and renders the tip wait settings.
#[test]
fn compact_blocks_accepts_the_tip_wait() -> eyre::Result<()> {
    let temporary = tempdir()?;
    let config_path = temporary.path().join("compact-blocks.toml");
    fs::write(
        &config_path,
        compact_blocks_config_toml(
            &temporary.path().join("compact-blocks-tip-wait-store"),
            &temporary.path().join("compact-blocks-tip-wait-secondary"),
        )?,
    )?;

    let defaults = print_config_with_env(&config_path, &[])?;
    assert!(defaults.status.success(), "{defaults:?}");
    let stdout = String::from_utf8(defaults.stdout)?;
    assert!(stdout.contains("tip_wait_ms = 3000"), "{stdout}");

    let configured = print_config_with_env(
        &config_path,
        &[
            ("ZINDER_COMPAT__TIP_WAIT_MS", "5000"),
            ("ZINDER_COMPAT__TIP_WAIT_MAX_BLOCKS", "3"),
            ("ZINDER_COMPAT__TIP_WAIT_MAX_WAITERS", "8"),
        ],
    )?;
    assert!(configured.status.success(), "{configured:?}");
    let stdout = String::from_utf8(configured.stdout)?;
    assert!(stdout.contains("tip_wait_ms = 5000"), "{stdout}");
    assert!(stdout.contains("tip_wait_max_blocks = 3"), "{stdout}");
    assert!(stdout.contains("tip_wait_max_waiters = 8"), "{stdout}");
    Ok(())
}

#[test]
fn block_range_stream_limit_outside_its_range_is_rejected() -> eyre::Result<()> {
    for configured_streams in ["0", "65537"] {
        assert_compat_cli_rejects(
            |root| {
                let config = compat_config_toml(
                    &root.join("compat-stream-limit-store"),
                    &root.join("compat-stream-limit-secondary"),
                    &root.join("compat-stream-limit-wallet"),
                )?;
                Ok(format!(
                    "{config}max_concurrent_block_range_streams = {configured_streams}\n"
                ))
            },
            &[],
            "compat.max_concurrent_block_range_streams must be between 1 and 65536",
        )?;
    }
    Ok(())
}

#[test]
fn missing_secondary_path_is_rejected_before_binding() -> eyre::Result<()> {
    assert_compat_cli_rejects(
        |root| compat_config_without_secondary_toml(&root.join("compat-missing-secondary-store")),
        &[],
        "missing configuration field: storage.secondary_path",
    )
}

#[test]
fn missing_wallet_secondary_path_is_rejected_before_binding() -> eyre::Result<()> {
    assert_compat_cli_rejects(
        |root| {
            Ok(format!(
                r#"[network]
name = "zcash-regtest"

[storage]
path = "{}"
secondary_path = "{}"

[wallet]
path = "{}"

[compat]
listen_addr = "127.0.0.1:9067"
"#,
                path_str(&root.join("compat-missing-wallet-secondary-store"))?,
                path_str(&root.join("compat-missing-wallet-secondary-canonical"))?,
                path_str(&root.join("compat-missing-wallet-secondary-wallet"))?,
            ))
        },
        &[],
        "missing configuration field: wallet.secondary_path",
    )
}

#[test]
fn overlapping_primary_and_secondary_roots_are_rejected_before_binding() -> eyre::Result<()> {
    assert_compat_cli_rejects(
        |root| {
            let storage_path = root.join("compat-overlapping-store");
            compat_config_toml(
                &storage_path,
                &root.join("compat-overlapping-secondary"),
                &storage_path,
            )
        },
        &[],
        "storage.path, storage.secondary_path, wallet.path, and wallet.secondary_path must be disjoint roots",
    )
}

#[test]
fn nested_or_lexically_aliased_storage_roots_are_rejected_before_binding() -> eyre::Result<()> {
    assert_compat_cli_rejects(
        |root| {
            let storage_path = root.join("compat-root-alias-store");
            let canonical_secondary_path = storage_path.join("secondary");
            let wallet_path = storage_path.join("..").join(
                storage_path
                    .file_name()
                    .ok_or_else(|| eyre::eyre!("missing file name"))?,
            );
            compat_config_toml(&storage_path, &canonical_secondary_path, &wallet_path)
        },
        &[],
        "storage.path, storage.secondary_path, wallet.path, and wallet.secondary_path must be disjoint roots",
    )
}

#[test]
fn sibling_ingest_section_is_tolerated_and_unknown_section_is_rejected() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let config_path = tempdir.path().join("compat-sibling.toml");
    let config = compat_config_with_ingest_section_toml(
        &tempdir.path().join("compat-sibling-store"),
        &tempdir.path().join("compat-sibling-secondary"),
        &tempdir.path().join("compat-sibling-wallet"),
    )?;
    fs::write(&config_path, &config)?;
    let tolerated = zinder_compat_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .output()?;
    assert!(tolerated.status.success(), "{tolerated:?}");

    assert_compat_cli_rejects(
        |root| {
            Ok(format!(
                "{}\n[injest]\nsource = \"zebra-json-rpc\"\n",
                compat_config_toml(
                    &root.join("compat-typo-store"),
                    &root.join("compat-typo-secondary"),
                    &root.join("compat-typo-wallet"),
                )?
            ))
        },
        &[],
        "unknown field `injest`",
    )
}

#[test]
fn wallet_rocksdb_section_is_accepted() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("compat-canonical");
    let secondary_path = tempdir.path().join("compat-canonical-secondary");
    let wallet_path = tempdir.path().join("compat-wallet-rocksdb");
    let config_path = tempdir.path().join("zinder-compat.toml");
    fs::write(
        &config_path,
        compat_config_with_wallet_rocksdb_toml(&storage_path, &secondary_path, &wallet_path)?,
    )?;

    let output = zinder_compat_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stdout.contains("[wallet.rocksdb]"), "{stdout}");
    assert!(stdout.contains("block_cache_bytes = 134217728"), "{stdout}");
    assert!(!stderr.contains("ERROR"), "{stderr}");

    Ok(())
}

#[test]
fn unknown_storage_subsection_is_rejected() -> eyre::Result<()> {
    assert_compat_cli_rejects(
        |root| {
            Ok(format!(
                "{}\n[storage.unsupported.rocksdb]\nblock_cache_bytes = 134217728\n",
                compat_config_toml(
                    &root.join("compat-canonical"),
                    &root.join("compat-canonical-secondary"),
                    &root.join("compat-wallet"),
                )?
            ))
        },
        &[],
        "unknown field `unsupported`",
    )
}

fn assert_compat_cli_rejects(
    build_config_toml: impl FnOnce(&Path) -> eyre::Result<String>,
    args: &[&str],
    expected_error: &str,
) -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let config_path = tempdir.path().join("zinder-compat.toml");
    fs::write(&config_path, build_config_toml(tempdir.path())?)?;

    let mut command = zinder_compat_command();
    command.args(["--print-config", "--config", path_str(&config_path)?]);
    command.args(args);
    let output = command.output()?;

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains(expected_error), "{stderr}");

    Ok(())
}

fn compat_config_toml(
    storage_path: &Path,
    secondary_path: &Path,
    wallet_path: &Path,
) -> eyre::Result<String> {
    let wallet_secondary_path = wallet_path.with_extension("secondary");
    Ok(format!(
        r#"[network]
name = "zcash-regtest"

[storage]
path = "{}"
secondary_path = "{}"

[wallet]
path = "{}"
secondary_path = "{}"

[compat]
listen_addr = "127.0.0.1:9067"
"#,
        path_str(storage_path)?,
        path_str(secondary_path)?,
        path_str(wallet_path)?,
        path_str(&wallet_secondary_path)?,
    ))
}

fn compact_blocks_config_toml(storage_path: &Path, secondary_path: &Path) -> eyre::Result<String> {
    Ok(format!(
        r#"[network]
name = "zcash-regtest"

[storage]
path = "{}"
secondary_path = "{}"

[compat]
serving = "compact-blocks"
listen_addr = "127.0.0.1:9067"
"#,
        path_str(storage_path)?,
        path_str(secondary_path)?,
    ))
}

fn compat_config_with_wallet_rocksdb_toml(
    storage_path: &Path,
    secondary_path: &Path,
    wallet_path: &Path,
) -> eyre::Result<String> {
    let wallet_secondary_path = wallet_path.with_extension("secondary");
    Ok(format!(
        r#"[network]
name = "zcash-regtest"

[storage]
path = "{}"
secondary_path = "{}"

[wallet]
path = "{}"
secondary_path = "{}"

[wallet.rocksdb]
block_cache_bytes = 134217728

[compat]
listen_addr = "127.0.0.1:9067"
"#,
        path_str(storage_path)?,
        path_str(secondary_path)?,
        path_str(wallet_path)?,
        path_str(&wallet_secondary_path)?,
    ))
}

fn compat_config_with_ingest_section_toml(
    storage_path: &Path,
    secondary_path: &Path,
    wallet_path: &Path,
) -> eyre::Result<String> {
    let wallet_secondary_path = wallet_path.with_extension("secondary");
    Ok(format!(
        r#"[network]
name = "zcash-regtest"

[storage]
path = "{}"
secondary_path = "{}"

[wallet]
path = "{}"
secondary_path = "{}"

[compat]
listen_addr = "127.0.0.1:9067"

[node]
json_rpc_addr = "http://127.0.0.1:18232"

[ingest]
source = "zebra-json-rpc"
"#,
        path_str(storage_path)?,
        path_str(secondary_path)?,
        path_str(wallet_path)?,
        path_str(&wallet_secondary_path)?,
    ))
}

fn compat_config_without_secondary_toml(storage_path: &Path) -> eyre::Result<String> {
    let wallet_path = storage_path.with_extension("wallet");
    let wallet_secondary_path = storage_path.with_extension("wallet-secondary");
    Ok(format!(
        r#"[network]
name = "zcash-regtest"

[storage]
path = "{}"

[wallet]
path = "{}"
secondary_path = "{}"

[compat]
listen_addr = "127.0.0.1:9067"
"#,
        path_str(storage_path)?,
        path_str(&wallet_path)?,
        path_str(&wallet_secondary_path)?,
    ))
}

fn zinder_compat_command() -> Command {
    let mut command = Command::new(zinder_testkit::bin_exe!("zinder-compat-lightwalletd"));
    command.env_clear();
    command
}

fn path_str(path: &Path) -> eyre::Result<&str> {
    path.to_str()
        .ok_or_else(|| eyre::eyre!("path is not valid UTF-8: {}", path.display()))
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
    for (profile, full_blocks, retention) in [
        ("wallet", None, RawBlobRetention::None),
        ("wallet", Some("true"), RawBlobRetention::All),
        ("explorer", Some("true"), RawBlobRetention::All),
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
        let mut command =
            tokio::process::Command::new(zinder_testkit::bin_exe!("zinder-compat-lightwalletd"));
        command
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
                "ZINDER_COMPAT__REORG_WINDOW_BLOCKS",
                reorg_window.to_string(),
            )
            .kill_on_drop(true);
        if let Some(full_blocks) = full_blocks {
            command.env("ZINDER_STORAGE__FULL_BLOCKS", full_blocks);
        }
        let output = tokio::time::timeout(Duration::from_secs(10), command.output()).await??;
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

#[test]
fn inline_ingest_control_bearer_token_is_accepted_and_redacted() -> eyre::Result<()> {
    let default_output = zinder_compat_command()
        .args(["--print-config", "--network", "zcash-regtest"])
        .env(
            "ZINDER_INGEST_CONTROL__BEARER_TOKEN",
            "compat-inline-secret",
        )
        .env("ZINDER_PROJECTOR__REORG_WINDOW_BLOCKS", "100")
        .output()?;
    assert!(default_output.status.success(), "{default_output:?}");
    let stdout = String::from_utf8(default_output.stdout)?;
    assert!(stdout.contains("bearer_token = \"[REDACTED]\""), "{stdout}");
    assert!(!stdout.contains("compat-inline-secret"), "{stdout}");
    Ok(())
}
