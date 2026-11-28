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
        &["dev", "darkside", "--help"],
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
fn role_startup_distinguishes_config_sources_from_observability_settings() -> eyre::Result<()> {
    let sandbox = TempDir::new()?;
    let missing = sandbox.path().join("private-missing-config.toml");
    let missing_path = missing
        .to_str()
        .ok_or_else(|| eyre::eyre!("non-UTF-8 config path"))?;
    for role in [
        "ingest",
        "projector",
        "query",
        "compat-lightwalletd",
        "explorer",
        "compat-cipherscan",
    ] {
        let output = zinder_command(&["run", role, "--config", missing_path]).output()?;
        assert!(!output.status.success(), "{role}");
        let stderr = String::from_utf8(output.stderr)?;
        assert!(
            stderr.contains("failed to load configuration sources"),
            "{role}: {stderr}"
        );
        assert!(!stderr.contains("observability"), "{role}: {stderr}");
        assert!(
            !stderr.contains("private-missing-config"),
            "{role}: {stderr}"
        );
    }

    for (contents, diagnostic) in [
        (
            "[node]\npassword = \"private-config-token",
            "failed to load configuration sources",
        ),
        (
            "[observability]\notlp_endpoint = \"http://collector.internal?token=private-config-token\"\n",
            "invalid observability configuration",
        ),
    ] {
        let config = write_test_config(sandbox.path(), contents)?;
        let path = config
            .to_str()
            .ok_or_else(|| eyre::eyre!("non-UTF-8 config path"))?;
        let output = zinder_command(&["run", "query", "--config", path]).output()?;
        assert!(!output.status.success());
        let stderr = String::from_utf8(output.stderr)?;
        assert!(stderr.contains(diagnostic), "{stderr}");
        assert!(!stderr.contains("private-config-token"), "{stderr}");
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
            "checkpoint_bearer_token = \"secret-checkpoint-token\"\n\n",
            "[observability]\notlp_endpoint = \"http://private-collector.internal:4317\"\n",
            "sampling_rate = 0.25\n"
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
            .env("ZINDER_OBSERVABILITY__SAMPLING_RATE", "0.5")
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
            "private-collector.internal",
        ] {
            assert!(
                !stdout.contains(leaked),
                "preset `{preset}` leaked `{leaked}`:\n{stdout}"
            );
        }
        let config: toml::Table = toml::from_str(&stdout)?;
        assert_eq!(
            config["observability"]["otlp_endpoint"].as_str(),
            Some("[REDACTED]")
        );
        assert_eq!(
            config["observability"]["sampling_rate"]
                .as_float()
                .map(f64::to_bits),
            Some(0.5_f64.to_bits())
        );
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
fn explorer_print_config_preserves_browser_origins_and_redacts_urls() -> eyre::Result<()> {
    let sandbox = TempDir::new()?;
    let storage_root = sandbox.path().join("explorer-root");
    let config_body = format!(
        concat!(
            "[network]\nname = \"zcash-regtest\"\n\n",
            "[storage]\ndata_dir = \"{}\"\n\n",
            "[node]\njson_rpc_addr = \"http://127.0.0.1:18232\"\n\n",
            "[explorer]\nwallet_query_endpoint = \"http://exp-user:exp-secret@127.0.0.1:9102/?tok=wq-secret#frag\"\n",
            "browser_origins = [\"https://explorer.example\"]\n\n",
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
        let output = zinder_command(&args)
            .env("ZINDER_QUERY__BROWSER_ORIGINS", "https://wallet.example")
            .output()?;
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
        let printed: toml::Table = toml::from_str(&stdout)?;
        for (role, origin) in [
            ("query", "https://wallet.example"),
            ("explorer", "https://explorer.example"),
        ] {
            assert_eq!(
                printed[role]["browser_origins"],
                toml::Value::try_from([origin])?
            );
        }
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

#[test]
fn combined_explorer_rejects_a_query_listener_requiring_proxy_protocol() -> eyre::Result<()> {
    let sandbox = TempDir::new()?;
    let base = format!(
        "[network]\nname = \"zcash-regtest\"\n[storage]\ndata_dir = \"{}\"\n[node]\njson_rpc_addr = \"http://127.0.0.1:18232\"\n",
        sandbox.path().display()
    );
    let config = write_test_config(sandbox.path(), &base)?;
    let config = config
        .to_str()
        .ok_or_else(|| eyre::eyre!("non-UTF-8 path"))?;
    let allowlist = "192.0.2.10/32";

    let env_rejected = zinder_command(&[
        "serve",
        "--print-config",
        "--preset",
        "explorer",
        "--config",
        config,
    ])
    .env("ZINDER_QUERY__PROXY_PROTOCOL_TRUSTED_CIDRS", allowlist)
    .output()?;
    std::fs::write(
        config,
        format!("{base}\n[query]\nproxy_protocol_trusted_cidrs = \"{allowlist}\"\n"),
    )?;
    let file_rejected = zinder_command(&[
        "config", "validate", "--preset", "explorer", "--config", config,
    ])
    .output()?;
    for rejected in [env_rejected, file_rejected] {
        assert!(!rejected.status.success());
        let stderr = String::from_utf8_lossy(&rejected.stderr);
        assert!(
            stderr.contains("incompatible with the combined explorer preset"),
            "{stderr}"
        );
        assert!(
            stderr.contains("ZINDER_QUERY__PROXY_PROTOCOL_TRUSTED_CIDRS"),
            "{stderr}"
        );
        assert!(!stderr.contains(allowlist));
    }

    let wallet = zinder_command(&[
        "config", "validate", "--preset", "wallet", "--config", config,
    ])
    .output()?;
    assert!(
        wallet.status.success(),
        "{}",
        String::from_utf8_lossy(&wallet.stderr)
    );
    Ok(())
}

#[test]
fn inactive_role_typos_in_toml_and_env_fail_closed() -> eyre::Result<()> {
    let sandbox = TempDir::new()?;
    let base = format!(
        "[network]\nname = \"zcash-regtest\"\n[storage]\ndata_dir = \"{}\"\n[node]\njson_rpc_addr = \"http://127.0.0.1:18232\"\n",
        sandbox.path().display()
    );
    let clean_cfg = write_test_config(sandbox.path(), &base)?;
    let clean_str = clean_cfg.to_str().ok_or_else(|| eyre::eyre!("path"))?;

    let typo_env = zinder_command(&[
        "config",
        "validate",
        "--preset",
        "light-wallet",
        "--config",
        clean_str,
    ])
    .env("ZINDER_PROJECTOR__TYPO_KEY", "1")
    .output()?;
    assert!(!typo_env.status.success());

    let readers_typo_env = zinder_command(&[
        "config",
        "validate",
        "--readers-only",
        "--preset",
        "wallet",
        "--config",
        clean_str,
    ])
    .env("ZINDER_INGEST__TYPO_KEY", "1")
    .output()?;
    assert!(!readers_typo_env.status.success());

    let bad_toml = format!("{base}\n[projector]\ntypo_key = 1\n");
    let bad_cfg = write_test_config(sandbox.path(), &bad_toml)?;
    let bad_str = bad_cfg.to_str().ok_or_else(|| eyre::eyre!("path"))?;
    for extra in [&["--preset", "light-wallet"][..], &["--readers-only"]] {
        let mut cmd = zinder_command(&["config", "validate", "--config", bad_str]);
        cmd.args(extra);
        let rejected = cmd.output()?;
        assert!(
            !rejected.status.success(),
            "expected rejection for {extra:?}: {}",
            String::from_utf8_lossy(&rejected.stdout)
        );
    }
    Ok(())
}

#[test]
fn readers_only_rejects_every_writer_only_secret_field() -> eyre::Result<()> {
    let sandbox = TempDir::new()?;
    let base = format!(
        "[network]\nname = \"zcash-regtest\"\n[storage]\ndata_dir = \"{}\"\n[node]\njson_rpc_addr = \"http://127.0.0.1:18232\"\n",
        sandbox.path().display()
    );
    let cases = [
        (
            "ingest_control.checkpoint_bearer_token",
            "[ingest_control]\ncheckpoint_bearer_token = \"writer-cp-secret\"\n",
        ),
        (
            "ingest_control.checkpoint_bearer_token_path",
            "[ingest_control]\ncheckpoint_bearer_token_path = \"/etc/zinder/cp.token\"\n",
        ),
        (
            "projector_control.bearer_token",
            "[projector_control]\nbearer_token = \"writer-proj-secret\"\n",
        ),
        (
            "projector_control.bearer_token_path",
            "[projector_control]\nbearer_token_path = \"/etc/zinder/proj.token\"\n",
        ),
        (
            "node.indexer_tls.client_key_pem",
            "[node.indexer_tls]\nclient_key_pem = \"writer-key-pem\"\n",
        ),
        (
            "node.indexer_tls.client_key_path",
            "[node.indexer_tls]\nclient_key_path = \"/etc/zinder/client.key\"\n",
        ),
        (
            "node_fallbacks.1.indexer_tls.client_key_pem",
            "[node_fallbacks.1]\njson_rpc_addr = \"http://127.0.0.1:18233\"\n[node_fallbacks.1.indexer_tls]\nclient_key_pem = \"fb-key-pem\"\n",
        ),
        (
            "node_fallbacks.1.indexer_tls.client_key_path",
            "[node_fallbacks.1]\njson_rpc_addr = \"http://127.0.0.1:18233\"\n[node_fallbacks.1.indexer_tls]\nclient_key_path = \"/etc/zinder/fb-client.key\"\n",
        ),
    ];

    for (expected_field, snippet) in cases {
        let cfg_path = write_test_config(sandbox.path(), &format!("{base}\n{snippet}"))?;
        let cfg_str = cfg_path.to_str().ok_or_else(|| eyre::eyre!("path"))?;
        let rejected = zinder_command(&[
            "serve",
            "--readers-only",
            "--print-config",
            "--config",
            cfg_str,
        ])
        .output()?;
        assert!(
            !rejected.status.success(),
            "expected --readers-only rejection for {expected_field}"
        );
        let stderr = String::from_utf8_lossy(&rejected.stderr);
        assert!(
            stderr.contains(expected_field),
            "expected `{expected_field}` in stderr: {stderr}"
        );
    }
    Ok(())
}

#[test]
fn none_sentinel_versus_empty_string_and_co_supervised_ingest_control() -> eyre::Result<()> {
    let sandbox = TempDir::new()?;
    let cfg_path = write_test_config(
        sandbox.path(),
        &format!(
            "[network]\nname = \"zcash-regtest\"\n[storage]\ndata_dir = \"{}\"\n[node]\njson_rpc_addr = \"http://127.0.0.1:18232\"\n",
            sandbox.path().display()
        ),
    )?;
    let cfg_str = cfg_path.to_str().ok_or_else(|| eyre::eyre!("path"))?;

    let disabled_ops = zinder_command(&["config", "print", "--config", cfg_str])
        .env("ZINDER_OPS__LISTEN_ADDR", "none")
        .output()?;
    assert!(
        disabled_ops.status.success(),
        "{}",
        String::from_utf8_lossy(&disabled_ops.stderr)
    );
    let printed = String::from_utf8_lossy(&disabled_ops.stdout);
    let parsed: toml::Table = toml::from_str(&printed)?;
    assert_eq!(parsed["ops"]["listen_addr"].as_str(), Some("none"));

    let empty_ops = zinder_command(&["config", "validate", "--config", cfg_str])
        .env("ZINDER_OPS__LISTEN_ADDR", "")
        .output()?;
    assert!(!empty_ops.status.success());

    let disabled_ctrl = zinder_command(&["config", "validate", "--config", cfg_str])
        .env("ZINDER_INGEST_CONTROL__LISTEN_ADDR", "none")
        .output()?;
    assert!(!disabled_ctrl.status.success());
    let stderr = String::from_utf8_lossy(&disabled_ctrl.stderr);
    assert!(stderr.contains("ingest_control.listen_addr"), "{stderr}");
    Ok(())
}

#[test]
fn supervision_plan_splits_secrets_redacts_debug_and_rejects_printed_redacted() -> eyre::Result<()>
{
    use secrecy::ExposeSecret;
    use zinder::{
        cli::{ConfigScopeArgs, Preset},
        config::{ConfigScope, prepare_supervision_plan_with_env},
    };

    let sandbox = TempDir::new()?;
    let cfg_path = write_test_config(
        sandbox.path(),
        &format!(
            concat!(
                "[network]\nname = \"zcash-regtest\"\n",
                "[storage]\ndata_dir = \"{}\"\n",
                "[node]\njson_rpc_addr = \"http://rpcu:rpcpass-secret@127.0.0.1:18232/?tok=nodeq\"\n",
                "[node.auth]\nmethod = \"basic\"\nusername = \"rpcu\"\npassword = \"pw-secret\"\n",
                "[ingest_control]\nbearer_token = \"ic-secret\"\ncheckpoint_bearer_token = \"cp-secret\"\n",
                "[query]\nproxy_protocol_trusted_cidrs = \"10.0.0.0/8\"\n",
                "[observability]\notlp_endpoint = \"https://private-collector.internal:4317\"\n",
            ),
            sandbox.path().display()
        ),
    )?;
    let scope = ConfigScope::serve(ConfigScopeArgs {
        preset: Some(Preset::Wallet),
        readers_only: false,
        full_blocks: false,
        data_dir: None,
        config_path: Some(cfg_path),
    });
    let plan = prepare_supervision_plan_with_env(&scope, std::iter::empty::<(String, String)>())?;
    let ingest_spec = plan
        .roles
        .iter()
        .find(|spec| spec.role == zinder::cli::ServiceRole::Ingest)
        .ok_or_else(|| eyre::eyre!("missing ingest role"))?;
    let debug_text = format!("{ingest_spec:?}");
    for secret_needle in [
        "rpcpass-secret",
        "pw-secret",
        "ic-secret",
        "cp-secret",
        "private-collector.internal",
    ] {
        assert!(!ingest_spec.non_secret_toml.contains(secret_needle));
        assert!(!debug_text.contains(secret_needle));
    }
    assert_eq!(
        ingest_spec
            .secret_env
            .get("ZINDER_NODE__AUTH__PASSWORD")
            .map(ExposeSecret::expose_secret),
        Some("pw-secret")
    );
    let query_spec = plan
        .roles
        .iter()
        .find(|spec| spec.role == zinder::cli::ServiceRole::Query)
        .ok_or_else(|| eyre::eyre!("missing query role"))?;
    assert!(!query_spec.non_secret_toml.contains("10.0.0.0/8"));
    assert_eq!(
        query_spec
            .secret_env
            .get("ZINDER_QUERY__PROXY_PROTOCOL_TRUSTED_CIDRS")
            .map(ExposeSecret::expose_secret),
        Some("10.0.0.0/8")
    );
    Ok(())
}

#[test]
fn config_print_redacted_output_is_rejected_on_revalidate() -> eyre::Result<()> {
    let sandbox = TempDir::new()?;
    let cfg_path = write_test_config(
        sandbox.path(),
        &format!(
            concat!(
                "[network]\nname = \"zcash-regtest\"\n",
                "[storage]\ndata_dir = \"{}\"\n",
                "[node]\njson_rpc_addr = \"http://rpcu:rpcpass-secret@127.0.0.1:18232/?tok=nodeq\"\n",
                "[node.auth]\nmethod = \"basic\"\nusername = \"rpcu\"\npassword = \"pw-secret\"\n",
                "[ingest_control]\nbearer_token = \"ic-secret\"\n",
            ),
            sandbox.path().display()
        ),
    )?;
    let printed = zinder_command(&[
        "config",
        "print",
        "--preset",
        "wallet",
        "--config",
        cfg_path.to_str().ok_or_else(|| eyre::eyre!("path"))?,
    ])
    .output()?;
    assert!(printed.status.success());
    let printed_path = sandbox.path().join("printed.toml");
    std::fs::write(&printed_path, &printed.stdout)?;
    let revalidated = zinder_command(&[
        "config",
        "validate",
        "--preset",
        "wallet",
        "--config",
        printed_path.to_str().ok_or_else(|| eyre::eyre!("path"))?,
    ])
    .output()?;
    assert!(!revalidated.status.success());
    let stderr = String::from_utf8_lossy(&revalidated.stderr);
    assert!(stderr.contains("[REDACTED]"), "{stderr}");
    Ok(())
}
