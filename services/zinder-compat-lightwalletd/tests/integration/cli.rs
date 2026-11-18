#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{fs, path::Path, process::Command};

use tempfile::tempdir;

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
    assert!(stdout.contains("reorg_window_blocks = 100"), "{stdout}");
    assert!(
        stdout.contains("pair_convergence_attempts = 12"),
        "{stdout}"
    );
    assert!(
        stdout.contains("max_concurrent_block_range_streams = 100"),
        "{stdout}"
    );
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
fn compatibility_serving_rejects_unapproved_aliases() -> eyre::Result<()> {
    let temporary = tempdir()?;
    let storage_path = temporary.path().join("compat-alias-store");
    let secondary_path = temporary.path().join("compat-alias-secondary");
    let config_path = temporary.path().join("compat-alias.toml");
    let config = compact_blocks_config_toml(&storage_path, &secondary_path)?
        .replace("serving = \"compact-blocks\"", "serving = \"canonical\"");
    fs::write(&config_path, config)?;

    let output = zinder_compat_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .output()?;
    assert!(!output.status.success(), "{output:?}");
    assert!(String::from_utf8(output.stderr)?.contains("serving"));
    Ok(())
}

#[test]
fn missing_storage_path_is_rejected_before_binding() -> eyre::Result<()> {
    let output = zinder_compat_command()
        .args(["--print-config", "--network", "zcash-regtest"])
        .output()?;

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr)?;
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
fn ingest_only_section_is_rejected() -> eyre::Result<()> {
    assert_compat_cli_rejects(
        |root| {
            compat_config_with_ingest_section_toml(
                &root.join("compat-node-source-store"),
                &root.join("compat-node-source-secondary"),
                &root.join("compat-node-source-wallet"),
            )
        },
        &[],
        "unknown field `ingest`",
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
