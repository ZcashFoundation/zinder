//! Integration tests for unified `zinder` CLI subcommands, config validation, and redaction.

use std::{path::Path, process::Command};

use tempfile::TempDir;

fn zinder_command(args: &[&str]) -> Command {
    let mut cmd = Command::new(zinder_testkit::bin_exe!("zinder"));
    cmd.args(args);
    for (env_name, _) in std::env::vars_os() {
        if let Some(key) = env_name.to_str()
            && key.starts_with("ZINDER_")
        {
            cmd.env_remove(&env_name);
        }
    }
    cmd
}

fn write_test_config(dir: &Path, body: &str) -> eyre::Result<std::path::PathBuf> {
    let path = dir.join("zinder.toml");
    std::fs::write(&path, body)?;
    Ok(path)
}

#[test]
fn subcommand_help_and_version_succeed() -> eyre::Result<()> {
    for args in [
        &["--help"][..],
        &["--version"],
        &["serve", "--help"],
        &["index", "--help"],
        &["run", "--help"],
        &["snapshot", "--help"],
        &["backup", "--help"],
        &["config", "--help"],
        &["healthcheck", "--help"],
        &["snapshot", "export", "--help"],
        &["snapshot", "import", "--help"],
        &["backup", "create", "--help"],
    ] {
        let output = zinder_command(args).output()?;
        assert!(
            output.status.success(),
            "command `zinder {}` failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

#[test]
fn run_subcommand_accepts_every_role_and_alias() -> eyre::Result<()> {
    for role in [
        "ingest",
        "projector",
        "wallet-index",
        "query",
        "wallet-grpc",
        "compat-lightwalletd",
        "lightwalletd",
        "explorer",
        "compat-cipherscan",
        "cipherscan",
    ] {
        let output = zinder_command(&["run", role, "--help"]).output()?;
        assert!(
            output.status.success(),
            "`zinder run {role} --help` failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

#[test]
fn config_validate_and_print_cover_all_presets_with_redaction() -> eyre::Result<()> {
    let sandbox = TempDir::new()?;
    let storage_root = sandbox.path().join("store-root");
    let config_body = format!(
        concat!(
            "[network]\nname = \"zcash-regtest\"\n\n",
            "[storage]\ndata_dir = \"{}\"\n\n",
            "[node]\njson_rpc_addr = \"http://rpcuser:super-secret-url@127.0.0.1:18232/?token=url-secret\"\n",
            "[node.auth]\nmethod = \"basic\"\nusername = \"rpcuser\"\npassword = \"secret-password\"\n\n",
            "[ingest_control]\nbearer_token = \"secret-control-token\"\n",
            "checkpoint_bearer_token = \"secret-checkpoint-token\"\n"
        ),
        storage_root.display()
    );
    let cfg_path = write_test_config(sandbox.path(), &config_body)?;
    let cfg_str = cfg_path
        .to_str()
        .ok_or_else(|| eyre::eyre!("non-utf8 path"))?;

    for preset in ["light-wallet", "lightwalletd", "wallet", "explorer"] {
        let validate = zinder_command(&[
            "config", "validate", "--preset", preset, "--config", cfg_str,
        ])
        .output()?;
        assert!(
            validate.status.success(),
            "preset `{preset}` failed validate: {}",
            String::from_utf8_lossy(&validate.stderr)
        );

        let printed = zinder_command(&["config", "print", "--preset", preset, "--config", cfg_str])
            .output()?;
        assert!(
            printed.status.success(),
            "preset `{preset}` failed print: {}",
            String::from_utf8_lossy(&printed.stderr)
        );
        let stdout = String::from_utf8_lossy(&printed.stdout);
        for leaked in [
            "secret-password",
            "super-secret-url",
            "url-secret",
            "secret-control-token",
            "secret-checkpoint-token",
        ] {
            assert!(
                !stdout.contains(leaked),
                "preset `{preset}` leaked `{leaked}`:\n{stdout}"
            );
        }
        assert!(
            stdout.contains("[REDACTED]"),
            "expected redaction marker:\n{stdout}"
        );
    }
    Ok(())
}

#[test]
fn serve_and_index_print_config_follow_preset_precedence() -> eyre::Result<()> {
    let sandbox = TempDir::new()?;
    let store_dir = sandbox.path().join("data-root");
    let cfg_path = write_test_config(
        sandbox.path(),
        "[network]\nname = \"zcash-regtest\"\n\n[node]\njson_rpc_addr = \"http://127.0.0.1:18232\"\n",
    )?;
    let cfg_str = cfg_path
        .to_str()
        .ok_or_else(|| eyre::eyre!("non-utf8 path"))?;
    let dir_str = store_dir
        .to_str()
        .ok_or_else(|| eyre::eyre!("non-utf8 path"))?;

    let serve_out = zinder_command(&[
        "serve",
        "--preset",
        "wallet",
        "--full-blocks",
        "--data-dir",
        dir_str,
        "--config",
        cfg_str,
        "--print-config",
    ])
    .env("ZINDER_DEPLOYMENT__PROFILE", "light-wallet")
    .output()?;
    assert!(
        serve_out.status.success(),
        "serve --print-config failed: {}",
        String::from_utf8_lossy(&serve_out.stderr)
    );
    let serve_toml = String::from_utf8_lossy(&serve_out.stdout);
    assert!(serve_toml.contains("profile = \"wallet\""), "{serve_toml}");
    assert!(serve_toml.contains("full_blocks = true"), "{serve_toml}");
    assert!(serve_toml.contains(dir_str), "{serve_toml}");

    let index_out = zinder_command(&[
        "index",
        "--preset",
        "explorer",
        "--data-dir",
        dir_str,
        "--config",
        cfg_str,
        "--print-config",
    ])
    .output()?;
    assert!(
        index_out.status.success(),
        "index --print-config failed: {}",
        String::from_utf8_lossy(&index_out.stderr)
    );
    let index_toml = String::from_utf8_lossy(&index_out.stdout);
    assert!(
        index_toml.contains("profile = \"explorer\""),
        "{index_toml}"
    );
    assert!(
        index_toml.contains("[storage.materialized_views.rocksdb]"),
        "{index_toml}"
    );
    assert!(index_toml.contains("[projector]"), "{index_toml}");
    assert!(index_toml.contains("127.0.0.1:9105"), "{index_toml}");

    let index_lw_out = zinder_command(&[
        "index",
        "--preset",
        "light-wallet",
        "--data-dir",
        dir_str,
        "--config",
        cfg_str,
        "--print-config",
    ])
    .output()?;
    assert!(index_lw_out.status.success());
    let index_lw_toml = String::from_utf8_lossy(&index_lw_out.stdout);
    assert!(!index_lw_toml.contains("[projector]"), "{index_lw_toml}");
    Ok(())
}

#[test]
fn invalid_sections_and_conflicting_explorer_views_are_rejected() -> eyre::Result<()> {
    let sandbox = TempDir::new()?;
    let bad_section = write_test_config(
        sandbox.path(),
        "[network]\nname = \"zcash-regtest\"\n[typo_section]\nfoo = 1\n",
    )?;
    let bad_out = zinder_command(&[
        "config",
        "validate",
        "--config",
        bad_section.to_str().ok_or_else(|| eyre::eyre!("path"))?,
    ])
    .output()?;
    assert!(!bad_out.status.success());

    let bad_env_out = zinder_command(&[
        "config",
        "validate",
        "--config",
        bad_section.to_str().ok_or_else(|| eyre::eyre!("path"))?,
    ])
    .env("ZINDER_UNKNOWN__KEY", "1")
    .output()?;
    assert!(!bad_env_out.status.success());

    let conflict_cfg = sandbox.path().join("conflict.toml");
    std::fs::write(
        &conflict_cfg,
        "[network]\nname = \"zcash-regtest\"\n[node]\njson_rpc_addr = \"http://127.0.0.1:18232\"\n[ingest]\nexplorer_views = false\n",
    )?;
    let conflict_out = zinder_command(&[
        "config",
        "validate",
        "--preset",
        "explorer",
        "--config",
        conflict_cfg.to_str().ok_or_else(|| eyre::eyre!("path"))?,
    ])
    .output()?;
    assert!(!conflict_out.status.success());
    Ok(())
}

#[test]
fn explorer_print_config_redacts_cipherscan_and_wallet_query_urls() -> eyre::Result<()> {
    let sandbox = TempDir::new()?;
    let storage_root = sandbox.path().join("explorer-root");
    let config_body = format!(
        concat!(
            "[network]\nname = \"zcash-regtest\"\n\n",
            "[storage]\ndata_dir = \"{}\"\n\n",
            "[node]\njson_rpc_addr = \"http://127.0.0.1:18232\"\n\n",
            "[explorer]\nwallet_query_endpoint = \"http://exp-user:exp-secret@127.0.0.1:9102/?tok=wq-secret#frag\"\n\n",
            "[cipherscan]\n",
            "explorer_query_endpoint = \"http://cs-exp:cs-exp-secret@127.0.0.1:9068/?k=1\"\n",
            "wallet_query_endpoint = \"http://cs-wal:cs-wal-secret@127.0.0.1:9102/?k=2\"\n",
            "current_price_endpoint = \"https://price-user:price-pass@prices.example/v1/spot?api_key=spot-secret\"\n",
            "historical_price_endpoint_template = \"https://hist-user:hist-pass@prices.example/v1/hist/{{date}}?api_key=hist-secret\"\n",
        ),
        storage_root.display()
    );
    let cfg_path = write_test_config(sandbox.path(), &config_body)?;
    let cfg_str = cfg_path.to_str().ok_or_else(|| eyre::eyre!("path"))?;

    for args in [
        vec![
            "config", "print", "--preset", "explorer", "--config", cfg_str,
        ],
        vec![
            "serve",
            "--preset",
            "explorer",
            "--config",
            cfg_str,
            "--print-config",
        ],
    ] {
        let output = zinder_command(&args).output()?;
        assert!(
            output.status.success(),
            "`{}` failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        for leaked in [
            "exp-secret",
            "wq-secret",
            "cs-exp-secret",
            "cs-wal-secret",
            "price-pass",
            "spot-secret",
            "hist-pass",
            "hist-secret",
        ] {
            assert!(
                !stdout.contains(leaked),
                "command `{}` leaked `{leaked}`:\n{stdout}",
                args.join(" ")
            );
        }
        assert!(stdout.contains("[REDACTED]"), "{stdout}");
    }
    Ok(())
}

#[test]
fn legacy_transactions_raw_blob_policy_fails_closed() -> eyre::Result<()> {
    let sandbox = TempDir::new()?;
    let storage_root = sandbox.path().join("legacy-root");
    let config_body = format!(
        concat!(
            "[network]\nname = \"zcash-regtest\"\n\n",
            "[storage]\ndata_dir = \"{}\"\nraw_blob_policy = \"transactions\"\n\n",
            "[node]\njson_rpc_addr = \"http://127.0.0.1:18232\"\n",
        ),
        storage_root.display()
    );
    let cfg_path = write_test_config(sandbox.path(), &config_body)?;
    let cfg_str = cfg_path.to_str().ok_or_else(|| eyre::eyre!("path"))?;

    let validate = zinder_command(&["config", "validate", "--config", cfg_str]).output()?;
    assert!(
        !validate.status.success(),
        "legacy raw_blob_policy = transactions must fail closed"
    );
    let stderr = String::from_utf8_lossy(&validate.stderr);
    assert!(stderr.contains("transactions"), "{stderr}");
    Ok(())
}
