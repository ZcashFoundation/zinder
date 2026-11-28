#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{error::Error, fs, path::Path, process::Output};

use serde_json::{Value, json};
use tempfile::tempdir;
use zinder_testkit::json_rpc_test_server::{JsonRpcTestServer, RpcReply, method};

use crate::common::zinder_ingest_command;

#[test]
fn version_reports_the_product_version() -> Result<(), Box<dyn Error>> {
    let output = zinder_ingest_command().arg("--version").output()?;

    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        String::from_utf8(output.stdout)?,
        format!("zinder-ingest {}\n", env!("CARGO_PKG_VERSION"))
    );

    Ok(())
}

#[test]
fn print_config_validates_and_redacts_basic_auth() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("print-config-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(&config_path, ingest_config_toml(&storage_path)?)?;
    let output = zinder_ingest_command()
        .args([
            "--print-config",
            "--config",
            path_str(&config_path)?,
            "--json-rpc-addr",
            "http://127.0.0.1:40000",
        ])
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("[node.auth]"));
    assert!(stdout.contains("method = \"basic\""));
    assert!(stdout.contains("password = \"[REDACTED]\""));
    assert!(stdout.contains("json_rpc_addr = \"http://127.0.0.1:40000\""));
    assert!(!stdout.contains("file-secret"));

    Ok(())
}

#[test]
fn print_config_applies_ops_listener_override() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("ops-listener-override-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(&config_path, ingest_config_toml(&storage_path)?)?;

    let output = zinder_ingest_command()
        .args([
            "--print-config",
            "--config",
            path_str(&config_path)?,
            "--ops-listen-addr",
            "127.0.0.1:29105",
        ])
        .output()?;

    assert!(output.status.success(), "{output:?}");
    assert!(String::from_utf8(output.stdout)?.contains("listen_addr = \"127.0.0.1:29105\""));

    Ok(())
}

#[test]
fn print_config_resolves_the_materialized_view_writer_budget() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("materialized-view-budget-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(&config_path, ingest_config_toml(&storage_path)?)?;

    let output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        stdout.contains("[storage.materialized_views.rocksdb]"),
        "{stdout}"
    );
    assert!(stdout.contains("block_cache_bytes = 268435456"), "{stdout}");

    Ok(())
}

#[test]
fn print_config_applies_materialized_view_budget_overrides() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("materialized-view-override-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    let config = format!(
        "{}\n[storage.materialized_views.rocksdb]\nmax_open_files = 96\n",
        ingest_config_toml(&storage_path)?
    );
    fs::write(&config_path, config)?;

    let output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("max_open_files = 96"), "{stdout}");

    Ok(())
}

#[test]
fn print_config_accepts_zebra_cookie_auth() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("cookie-auth-store");
    let cookie_path = tempdir.path().join("zebra-cookie");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(
        &config_path,
        cookie_ingest_config_toml(&storage_path, &cookie_path)?,
    )?;
    let output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("[node.auth]"));
    assert!(stdout.contains("method = \"cookie\""));
    assert!(stdout.contains("path = \"[REDACTED]\""));
    assert!(!stdout.contains("zebra-cookie"));

    Ok(())
}

#[test]
fn print_config_shows_indexer_tls_paths_from_the_environment() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("indexer-tls-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(&config_path, ingest_config_toml(&storage_path)?)?;
    let output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env("ZINDER_NODE__INDEXER_GRPC_ADDR", "https://zakura:8230")
        .env(
            "ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH",
            "/run/secrets/indexer-ca.pem",
        )
        .env(
            "ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH",
            "/run/secrets/indexer-client.pem",
        )
        .env(
            "ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH",
            "/run/secrets/indexer-client.key",
        )
        .env("ZINDER_NODE__INDEXER_TLS__SERVER_NAME", "zakura.internal")
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("indexer_grpc_addr = \"https://zakura:8230\""));
    assert!(stdout.contains("[node.indexer_tls]"));
    assert!(stdout.contains("ca_cert_path = \"/run/secrets/indexer-ca.pem\""));
    assert!(stdout.contains("client_cert_path = \"/run/secrets/indexer-client.pem\""));
    assert!(stdout.contains("client_key_path = \"/run/secrets/indexer-client.key\""));
    assert!(stdout.contains("server_name = \"zakura.internal\""));

    Ok(())
}

#[test]
fn print_config_rejects_indexer_tls_for_a_plaintext_indexer() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("plaintext-indexer-tls-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(&config_path, ingest_config_toml(&storage_path)?)?;
    let output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env("ZINDER_NODE__INDEXER_GRPC_ADDR", "http://zakura:8230")
        .env(
            "ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH",
            "/run/secrets/indexer-ca.pem",
        )
        .output()?;

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("node.indexer_tls requires an https:// node.indexer_grpc_addr"),
        "{stderr}"
    );

    Ok(())
}

#[test]
fn print_config_accepts_and_redacts_inline_indexer_tls_pems() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("indexer-tls-inline-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(&config_path, ingest_config_toml(&storage_path)?)?;
    let output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env("ZINDER_NODE__INDEXER_GRPC_ADDR", "https://zakura:8230")
        .env(
            "ZINDER_NODE__INDEXER_TLS__CA_CERT_PEM",
            "-----BEGIN CERTIFICATE-----\nca-sentinel\n-----END CERTIFICATE-----\n",
        )
        .env(
            "ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PEM",
            "-----BEGIN CERTIFICATE-----\nclient-cert-sentinel\n-----END CERTIFICATE-----\n",
        )
        .env(
            "ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM",
            "-----BEGIN PRIVATE KEY-----\nkey-material-sentinel\n-----END PRIVATE KEY-----\n",
        )
        .env("ZINDER_NODE__INDEXER_TLS__SERVER_NAME", "zakura.internal")
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("[node.indexer_tls]"), "{stdout}");
    assert!(stdout.contains("ca_cert_pem = \"[REDACTED]\""), "{stdout}");
    assert!(
        stdout.contains("client_cert_pem = \"[REDACTED]\""),
        "{stdout}"
    );
    assert!(
        stdout.contains("client_key_pem = \"[REDACTED]\""),
        "{stdout}"
    );
    assert!(!stdout.contains("ca-sentinel"), "{stdout}");
    assert!(!stdout.contains("client-cert-sentinel"), "{stdout}");
    assert!(!stdout.contains("key-material-sentinel"), "{stdout}");

    Ok(())
}

#[test]
fn print_config_rejects_simultaneous_indexer_tls_path_and_pem_without_leaking_secret()
-> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("indexer-tls-conflict-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(&config_path, ingest_config_toml(&storage_path)?)?;
    let output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env("ZINDER_NODE__INDEXER_GRPC_ADDR", "https://zakura:8230")
        .env(
            "ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH",
            "/run/secrets/client.key",
        )
        .env(
            "ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM",
            "-----BEGIN PRIVATE KEY-----\nkey-material-sentinel\n-----END PRIVATE KEY-----\n",
        )
        .output()?;

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("client_key_path"), "{stderr}");
    assert!(stderr.contains("client_key_pem"), "{stderr}");
    assert!(!stderr.contains("key-material-sentinel"), "{stderr}");

    Ok(())
}

#[test]
fn startup_fails_before_connecting_when_the_indexer_ca_file_is_missing()
-> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("missing-indexer-ca-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    let missing_ca_path = tempdir.path().join("missing-indexer-ca.pem");
    fs::write(&config_path, ingest_config_toml(&storage_path)?)?;
    let output = zinder_ingest_command()
        .args(["--config", path_str(&config_path)?])
        .env("ZINDER_OPS__LISTEN_ADDR", "")
        .env("ZINDER_NODE__INDEXER_GRPC_ADDR", "https://127.0.0.1:1")
        .env(
            "ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH",
            path_str(&missing_ca_path)?,
        )
        .output()?;

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("indexer TLS CA certificate file"),
        "{stderr}"
    );
    assert!(stderr.contains(path_str(&missing_ca_path)?), "{stderr}");
    assert!(
        !storage_path.exists(),
        "startup opened storage before failing"
    );

    Ok(())
}

/// A single `[node]` whose block 0 is not the configured network's genesis
/// block refuses startup before storage is created.
#[test]
fn startup_refuses_a_single_node_on_another_chain() -> Result<(), Box<dyn Error>> {
    let block_one: Value =
        serde_json::from_str(include_str!("../fixtures/z3-regtest-block-1.json"))?;
    let raw_block_hex = block_one
        .get("raw_block_hex")
        .cloned()
        .ok_or("the fixture carries raw_block_hex")?;
    let node = archive_node_stub(RpcReply::result(raw_block_hex))?;

    let (output, storage_created) = run_ingest_against(&node)?;

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("node_endpoints_unavailable"), "{stderr}");
    assert!(
        stderr.contains(
            "position 0: excluded (the node's block 0 is not the configured network's genesis block)"
        ),
        "{stderr}"
    );
    assert_eq!(node.requests_for("getblock")?.len(), 1);
    assert!(!storage_created, "startup opened storage before failing");
    Ok(())
}

/// A single `[node]` that cannot serve block 0 while it warms up fails
/// startup as unreachable, never as another chain.
#[test]
fn startup_reports_a_warming_up_single_node_as_unreachable() -> Result<(), Box<dyn Error>> {
    let node = archive_node_stub(RpcReply::error_with_code(-28, "Loading block index..."))?;

    let (output, storage_created) = run_ingest_against(&node)?;

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("position 0: unreachable"), "{stderr}");
    assert!(!stderr.contains("genesis block"), "{stderr}");
    assert!(!storage_created, "startup opened storage before failing");
    Ok(())
}

/// A Zebra archive node that passes the capability and activation reads and
/// answers `getblock` with `block_reply`.
fn archive_node_stub(block_reply: RpcReply) -> Result<JsonRpcTestServer, Box<dyn Error>> {
    let blockchain_info = || RpcReply::result(json!({"pruned": false, "upgrades": {}}));
    Ok(JsonRpcTestServer::start([
        method("rpc.discover").reply(RpcReply::result(json!({
            "openrpc": "1.3.2",
            "methods": [
                {"name": "getblock"},
                {"name": "getbestblockheightandhash"},
                {"name": "z_gettreestate"},
                {"name": "z_getsubtreesbyindex"},
                {"name": "getblockchaininfo"},
                {"name": "rpc.discover"},
            ],
        }))),
        method("getblockchaininfo").reply(blockchain_info()),
        method("getblockchaininfo").reply(blockchain_info()),
        method("getblock").reply(block_reply),
    ])?)
}

/// Runs `zinder-ingest` on regtest against `node` and returns its output and
/// whether it created its storage path.
fn run_ingest_against(node: &JsonRpcTestServer) -> Result<(Output, bool), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(
        &config_path,
        ingest_config_toml(&storage_path)?.replace("http://127.0.0.1:39232", &node.url()),
    )?;
    let output = zinder_ingest_command()
        .args(["--config", path_str(&config_path)?])
        .env("ZINDER_OPS__LISTEN_ADDR", "")
        .output()?;
    Ok((output, storage_path.exists()))
}

#[test]
fn startup_fails_before_opening_storage_when_rust_log_is_invalid() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("invalid-rust-log-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(&config_path, ingest_config_toml(&storage_path)?)?;
    let output = zinder_ingest_command()
        .args(["--config", path_str(&config_path)?])
        .env("ZINDER_OPS__LISTEN_ADDR", "")
        .env("RUST_LOG", "zinder=bogus")
        .output()?;

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("zinder-ingest: RUST_LOG=\"zinder=bogus\" is not a valid log filter"),
        "{stderr}"
    );
    assert!(
        !storage_path.exists(),
        "startup opened storage before failing"
    );

    Ok(())
}

#[test]
fn print_config_renders_ingest_sub_sections() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("ingest-print-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(&config_path, ingest_config_toml(&storage_path)?)?;

    let output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("[ingest]"));
    assert!(stdout.contains("explorer_views = false"), "{stdout}");
    assert!(stdout.contains("reorg_window_blocks = 100"));
    assert!(stdout.contains("[ingest.phase_classification]"));
    assert!(stdout.contains("catchup_threshold_blocks ="));
    assert!(stdout.contains("[ingest.construction]"));
    assert!(stdout.contains("source_segment_max_blocks = 64"));
    assert!(stdout.contains("source_segment_target_response_bytes = 33554432"));
    assert!(stdout.contains("source_fetch_max_in_flight_requests = 12"));
    assert!(stdout.contains("source_fetch_max_in_flight_bytes = 402653184"));
    assert!(stdout.contains("block_prepare_concurrency ="));
    assert!(stdout.contains("block_prepare_memory_watermark_bytes = 536870912"));
    assert!(stdout.contains("[ingest.mempool]"));
    assert!(stdout.contains("max_transaction_count = 8000"));
    assert!(stdout.contains("max_total_raw_transaction_bytes = 80000000"));
    assert!(stdout.contains("reconciliation_batch_target_raw_transaction_bytes = 16000000"));
    assert!(stdout.contains("[ingest.follow]"));
    assert!(stdout.contains("poll_interval_ms = 1000"));
    assert!(stdout.contains("lag_threshold_blocks ="));
    assert!(stdout.contains("[ingest.run_overrides]"));
    assert!(stdout.contains("coverage = \"explicit\""));
    assert!(stdout.contains("during_construction = false"));

    Ok(())
}

#[test]
fn mempool_limits_use_environment_overrides() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("mempool-limit-overrides-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(&config_path, ingest_config_toml(&storage_path)?)?;

    let output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env("ZINDER_INGEST__MEMPOOL__MAX_TRANSACTION_COUNT", "7000")
        .env(
            "ZINDER_INGEST__MEMPOOL__MAX_TOTAL_RAW_TRANSACTION_BYTES",
            "70000000",
        )
        .env(
            "ZINDER_INGEST__MEMPOOL__RECONCILIATION_BATCH_TARGET_RAW_TRANSACTION_BYTES",
            "7000000",
        )
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("max_transaction_count = 7000"), "{stdout}");
    assert!(
        stdout.contains("max_total_raw_transaction_bytes = 70000000"),
        "{stdout}"
    );
    assert!(
        stdout.contains("reconciliation_batch_target_raw_transaction_bytes = 7000000"),
        "{stdout}"
    );

    Ok(())
}

#[test]
fn zero_mempool_limits_fail_before_storage_creation() -> Result<(), Box<dyn Error>> {
    let cases = [
        (
            "ZINDER_INGEST__MEMPOOL__MAX_TRANSACTION_COUNT",
            "ingest.mempool.max_transaction_count must be greater than zero",
        ),
        (
            "ZINDER_INGEST__MEMPOOL__MAX_TOTAL_RAW_TRANSACTION_BYTES",
            "ingest.mempool.max_total_raw_transaction_bytes must be greater than zero",
        ),
        (
            "ZINDER_INGEST__MEMPOOL__RECONCILIATION_BATCH_TARGET_RAW_TRANSACTION_BYTES",
            "ingest.mempool.reconciliation_batch_target_raw_transaction_bytes must be greater than zero",
        ),
    ];

    for (environment_variable, expected_error) in cases {
        let tempdir = tempdir()?;
        let storage_path = tempdir.path().join("zero-mempool-limit-store");
        let config_path = tempdir.path().join("zinder-ingest.toml");
        fs::write(&config_path, ingest_config_toml(&storage_path)?)?;

        let output = zinder_ingest_command()
            .args(["--print-config", "--config", path_str(&config_path)?])
            .env(environment_variable, "0")
            .output()?;

        assert!(!output.status.success(), "{output:?}");
        let stderr = String::from_utf8(output.stderr)?;
        assert!(stderr.contains(expected_error), "{stderr}");
        assert!(!storage_path.exists());
    }

    Ok(())
}

#[test]
fn print_config_output_round_trips() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("round-trip-store");
    let source_config_path = tempdir.path().join("source.toml");
    let rendered_config_path = tempdir.path().join("rendered.toml");
    let config = format!(
        "{}\n[ingest.tree_state_densification]\nduring_construction = true\nselection = \"commitment-budget\"\ncommitment_budget = 256\n",
        ingest_config_toml(&storage_path)?
    );
    fs::write(&source_config_path, config)?;

    let first_output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&source_config_path)?])
        .output()?;
    assert!(first_output.status.success(), "{first_output:?}");
    fs::write(&rendered_config_path, &first_output.stdout)?;

    let second_output = zinder_ingest_command()
        .args([
            "--print-config",
            "--config",
            path_str(&rendered_config_path)?,
        ])
        .output()?;
    assert!(second_output.status.success(), "{second_output:?}");

    for rendered in [first_output.stdout, second_output.stdout] {
        let config: toml::Table = toml::from_str(std::str::from_utf8(&rendered)?)?;
        let densification = &config["ingest"]["tree_state_densification"];
        assert_eq!(
            densification
                .get("during_construction")
                .and_then(toml::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            densification["selection"].as_str(),
            Some("commitment-budget")
        );
        assert_eq!(densification["commitment_budget"].as_integer(), Some(256));
    }

    Ok(())
}

#[test]
fn print_config_allows_disabled_ingest_control_for_one_shot_runs() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("disabled-ingest-control-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    let config = ingest_config_toml(&storage_path)?
        .replace("listen_addr = \"127.0.0.1:9100\"", "listen_addr = \"\"");
    fs::write(&config_path, config)?;

    let output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("[ingest_control]"));
    assert!(stdout.contains("listen_addr = \"\""));

    Ok(())
}

/// Settings that tuned the removed batch-commit catch-up path, as
/// `(TOML section, field, environment variable)`.
const REMOVED_INGEST_SETTINGS: &[(&str, &str, &str)] = &[
    (
        "ingest.construction",
        "canonical_batch_max_blocks",
        "ZINDER_INGEST__CONSTRUCTION__CANONICAL_BATCH_MAX_BLOCKS",
    ),
    (
        "ingest.construction",
        "canonical_batch_max_artifact_bytes",
        "ZINDER_INGEST__CONSTRUCTION__CANONICAL_BATCH_MAX_ARTIFACT_BYTES",
    ),
    (
        "ingest.construction",
        "canonical_batch_max_estimated_write_bytes",
        "ZINDER_INGEST__CONSTRUCTION__CANONICAL_BATCH_MAX_ESTIMATED_WRITE_BYTES",
    ),
    (
        "ingest.construction",
        "canonical_batch_min_blocks_before_estimated_write_close",
        "ZINDER_INGEST__CONSTRUCTION__CANONICAL_BATCH_MIN_BLOCKS_BEFORE_ESTIMATED_WRITE_CLOSE",
    ),
    (
        "ingest.construction",
        "commit_reassembly_max_queued_artifact_bytes",
        "ZINDER_INGEST__CONSTRUCTION__COMMIT_REASSEMBLY_MAX_QUEUED_ARTIFACT_BYTES",
    ),
    (
        "ingest.construction",
        "flush_interval_epochs",
        "ZINDER_INGEST__CONSTRUCTION__FLUSH_INTERVAL_EPOCHS",
    ),
    (
        "ingest.run_overrides",
        "allow_reorg_window_settlement",
        "ZINDER_INGEST__RUN_OVERRIDES__ALLOW_REORG_WINDOW_SETTLEMENT",
    ),
];

fn removed_setting_toml_value(field: &str) -> &'static str {
    match field {
        "allow_reorg_window_settlement" => "true",
        _ => "1",
    }
}

#[test]
fn removed_ingest_settings_are_rejected_from_config_files() -> Result<(), Box<dyn Error>> {
    for (section, field, _) in REMOVED_INGEST_SETTINGS {
        let value = removed_setting_toml_value(field);
        assert_ingest_cli_rejects(
            |root| {
                let base = ingest_config_toml(&root.join("removed-setting-store"))?;
                Ok(format!("{base}\n[{section}]\n{field} = {value}\n"))
            },
            &[],
            &[],
            &format!("unknown field `{field}`"),
        )?;
    }
    Ok(())
}

#[test]
fn removed_ingest_settings_are_rejected_from_the_environment() -> Result<(), Box<dyn Error>> {
    for (_, field, environment_variable) in REMOVED_INGEST_SETTINGS {
        let value = removed_setting_toml_value(field);
        assert_ingest_cli_rejects(
            |root| ingest_config_toml(&root.join("removed-setting-environment-store")),
            &[],
            &[(environment_variable, value)],
            &format!("unknown field `{field}`"),
        )?;
    }
    Ok(())
}

#[test]
fn removed_ingest_flags_are_rejected() -> Result<(), Box<dyn Error>> {
    for (flag, flag_value) in [
        ("--canonical-batch-max-blocks", Some("1")),
        ("--canonical-batch-max-artifact-bytes", Some("1")),
        ("--canonical-batch-max-estimated-write-bytes", Some("1")),
        (
            "--canonical-batch-min-blocks-before-estimated-write-close",
            Some("1"),
        ),
        ("--allow-reorg-window-settlement", None),
    ] {
        let arguments: Vec<&str> = std::iter::once(flag).chain(flag_value).collect();
        assert_ingest_cli_rejects(
            |root| ingest_config_toml(&root.join("removed-flag-store")),
            &arguments,
            &[],
            &format!("unexpected argument '{flag}'"),
        )?;
    }
    Ok(())
}

#[test]
fn verify_canonical_replay_subcommand_is_rejected() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let output = zinder_ingest_command()
        .args([
            "verify-canonical-replay",
            "--network",
            "zcash-regtest",
            "--storage-path",
            path_str(&tempdir.path().join("store"))?,
            "--secondary-path",
            path_str(&tempdir.path().join("secondary"))?,
        ])
        .output()?;

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("unrecognized subcommand 'verify-canonical-replay'"),
        "{stderr}"
    );
    assert!(!tempdir.path().join("secondary").exists());

    Ok(())
}

#[test]
fn cli_overrides_environment_and_environment_overrides_config_file() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("precedence-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(&config_path, ingest_config_toml(&storage_path)?)?;

    let output = zinder_ingest_command()
        .args([
            "--print-config",
            "--config",
            path_str(&config_path)?,
            "--target-height",
            "300",
        ])
        .env("ZINDER_INGEST__RUN_OVERRIDES__TARGET_HEIGHT", "200")
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    // CLI override (300) wins over env (200) which would win over file.
    assert!(stdout.contains("target_height = 300"), "{stdout}");

    Ok(())
}

#[test]
fn target_height_uses_standard_config_precedence() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("target-height-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(&config_path, ingest_config_toml(&storage_path)?)?;

    let output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env("ZINDER_INGEST__RUN_OVERRIDES__TARGET_HEIGHT", "777")
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("target_height = 777"), "{stdout}");

    Ok(())
}

#[test]
fn wallet_serving_print_config_marks_coverage() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("wallet-serving-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(
        &config_path,
        wallet_serving_ingest_config_toml(&storage_path)?,
    )?;

    let output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("coverage = \"wallet-serving\""), "{stdout}");
    assert!(stdout.contains("explorer_views = false"), "{stdout}");
    assert!(stdout.contains("raw_blob_policy = \"none\""), "{stdout}");

    Ok(())
}

#[test]
fn packaged_wallet_configs_select_dense_construction_unless_overridden_or_serving_explorer()
-> Result<(), Box<dyn Error>> {
    let temporary = tempdir()?;
    let token = temporary.path().join("fixture.token");
    fs::write(&token, "fixture-token\n")?;
    let json_rpc_toml = temporary.path().join("ingest-json-rpc.toml");
    let grpc_toml = temporary.path().join("ingest-grpc.toml");
    for (path, source) in [
        (&json_rpc_toml, "zebra-json-rpc"),
        (&grpc_toml, "zebra-indexer-grpc"),
    ] {
        fs::write(
            path,
            format!(
                concat!(
                    "[storage]\npath = \"/var/lib/zinder/canonical\"\n\n",
                    "[ingest]\nsource = \"{source}\"\nexplorer_views = false\nreorg_window_blocks = 1000\n\n",
                    "[ingest.run_overrides]\ncoverage = \"wallet-serving\"\n\n",
                    "[ingest_control]\nlisten_addr = \"127.0.0.1:9100\"\n",
                    "checkpoint_staging_root = \"/var/lib/zinder/checkpoints\"\n",
                    "bearer_token_path = \"/var/run/zinder-control/ingest.token\"\n",
                    "checkpoint_bearer_token_path = \"/var/run/zinder-checkpoint/checkpoint.token\"\n",
                ),
                source = source,
            ),
        )?;
    }
    let core_bounded_workers = std::thread::available_parallelism()
        .map_or(1, std::num::NonZeroUsize::get)
        .min(8);

    for (label, config_path) in [("json-rpc", &json_rpc_toml), ("indexer-grpc", &grpc_toml)] {
        for (setting, dense) in [
            (None, true),
            (Some(("ZINDER_INGEST__EXPLORER_VIEWS", "true")), false),
            (
                Some((
                    "ZINDER_INGEST__TREE_STATE_DENSIFICATION__DURING_CONSTRUCTION",
                    "false",
                )),
                false,
            ),
        ] {
            let output = zinder_ingest_command()
                .args(["--print-config", "--config"])
                .arg(config_path)
                .env("ZINDER_NETWORK__NAME", "zcash-regtest")
                .env("ZINDER_NODE__JSON_RPC_ADDR", "http://127.0.0.1:18232")
                .env("ZINDER_NODE__INDEXER_GRPC_ADDR", "http://127.0.0.1:18155")
                .env("ZINDER_INGEST_CONTROL__BEARER_TOKEN_PATH", &token)
                .env(
                    "ZINDER_INGEST_CONTROL__CHECKPOINT_BEARER_TOKEN_PATH",
                    &token,
                )
                .envs(setting)
                .output()?;
            assert!(output.status.success(), "{label}: {output:?}");
            let config: toml::Table = toml::from_str(std::str::from_utf8(&output.stdout)?)?;
            let policy = &config["ingest"]["tree_state_densification"];
            assert_eq!(policy["enabled"].as_bool(), Some(true), "{label}");
            assert_eq!(policy["selection"].as_str(), Some("every-block"), "{label}");
            assert_eq!(
                policy["during_construction"].as_bool(),
                Some(dense),
                "{label}, override={setting:?}"
            );
            let expected_workers = if dense { core_bounded_workers } else { 1 };
            assert_eq!(
                config["ingest"]["construction"]["tree_hashing_workers"].as_integer(),
                Some(i64::try_from(expected_workers)?),
                "{label}, override={setting:?}"
            );
        }
    }
    Ok(())
}

#[test]
fn explorer_views_can_be_enabled_through_environment_without_changing_coverage()
-> Result<(), Box<dyn Error>> {
    let temporary = tempdir()?;
    let storage_path = temporary.path().join("explorer-views-store");
    let config_path = temporary.path().join("zinder-ingest.toml");
    fs::write(&config_path, ingest_config_toml(&storage_path)?)?;

    let output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env("ZINDER_INGEST__EXPLORER_VIEWS", "true")
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("explorer_views = true"), "{stdout}");
    assert!(stdout.contains("coverage = \"explicit\""), "{stdout}");
    Ok(())
}

#[test]
fn explorer_views_free_space_floor_defaults_to_ten_gib_and_accepts_zero()
-> Result<(), Box<dyn Error>> {
    let temporary = tempdir()?;
    let storage_path = temporary.path().join("explorer-views-store");
    let config_path = temporary.path().join("zinder-ingest.toml");
    fs::write(&config_path, ingest_config_toml(&storage_path)?)?;

    let default_output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .output()?;
    let disabled_output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env("ZINDER_INGEST__EXPLORER_VIEWS_MIN_FREE_BYTES", "0")
        .output()?;

    assert!(default_output.status.success(), "{default_output:?}");
    let default_stdout = String::from_utf8(default_output.stdout)?;
    assert!(
        default_stdout.contains("explorer_views_min_free_bytes = 10737418240"),
        "{default_stdout}"
    );
    assert!(disabled_output.status.success(), "{disabled_output:?}");
    let disabled_stdout = String::from_utf8(disabled_output.stdout)?;
    assert!(
        disabled_stdout.contains("explorer_views_min_free_bytes = 0"),
        "{disabled_stdout}"
    );
    Ok(())
}

#[test]
fn explorer_views_reject_positive_checkpoint_before_storage_mutation() -> Result<(), Box<dyn Error>>
{
    let temporary = tempdir()?;
    let storage_path = temporary.path().join("explorer-views-checkpoint-store");
    let config_path = temporary.path().join("zinder-ingest.toml");
    let config = ingest_config_toml(&storage_path)?.replacen(
        "[ingest]\nsource = \"zebra-json-rpc\"\n",
        "[ingest]\nsource = \"zebra-json-rpc\"\nexplorer_views = true\n",
        1,
    );
    fs::write(
        &config_path,
        format!("{config}\n[ingest.run_overrides]\ncheckpoint_height = 1\n"),
    )?;

    let output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .output()?;

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("explorer_views = true"), "{stderr}");
    assert!(!storage_path.exists());
    Ok(())
}

#[test]
fn wallet_serving_allows_default_none_retention_and_rejects_legacy_transactions()
-> Result<(), Box<dyn Error>> {
    assert_ingest_cli_rejects(
        |root| {
            Ok(
                wallet_serving_ingest_config_toml(&root.join("wallet-serving-store"))?.replacen(
                    "[storage]\n",
                    "[storage]\nraw_blob_policy = \"transactions\"\n",
                    1,
                ),
            )
        },
        &[],
        &[],
        "unknown variant `transactions`, expected `none` or `all`",
    )
}

fn assert_profile_print_and_conflict(
    config_path: &Path,
    profile: &str,
    full_blocks: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    let mut command = zinder_ingest_command();
    command
        .args(["--print-config", "--config", path_str(config_path)?])
        .env("ZINDER_DEPLOYMENT__PROFILE", profile);
    if let Some(full_blocks) = full_blocks {
        command.env("ZINDER_STORAGE__FULL_BLOCKS", full_blocks);
    }
    let output = command.output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("[deployment]"), "{stdout}");
    assert!(
        stdout.contains(&format!("profile = \"{profile}\"")),
        "{stdout}"
    );
    if full_blocks == Some("true") {
        assert!(stdout.contains("full_blocks = true"), "{stdout}");
    }
    assert!(!stdout.contains("coverage ="), "{stdout}");
    assert!(!stdout.contains("raw_blob_policy ="), "{stdout}");
    assert!(!stdout.contains("explorer_views ="), "{stdout}");

    let conflicting_policy = if full_blocks == Some("true") {
        "none"
    } else {
        "all"
    };
    let mut conflict_command = zinder_ingest_command();
    conflict_command
        .args(["--print-config", "--config", path_str(config_path)?])
        .env("ZINDER_DEPLOYMENT__PROFILE", profile)
        .env("ZINDER_STORAGE__RAW_BLOB_POLICY", conflicting_policy);
    if let Some(full_blocks) = full_blocks {
        conflict_command.env("ZINDER_STORAGE__FULL_BLOCKS", full_blocks);
    }
    let conflict = conflict_command.output()?;
    assert!(!conflict.status.success(), "{conflict:?}");
    assert!(String::from_utf8(conflict.stderr)?.contains("remove conflicting overrides"));

    let legacy_transactions = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(config_path)?])
        .env("ZINDER_DEPLOYMENT__PROFILE", profile)
        .env("ZINDER_STORAGE__RAW_BLOB_POLICY", "transactions")
        .output()?;
    assert!(
        !legacy_transactions.status.success(),
        "{legacy_transactions:?}"
    );
    let legacy_stderr = String::from_utf8(legacy_transactions.stderr)?;
    assert!(
        legacy_stderr.contains("ZINDER_STORAGE__RAW_BLOB_POLICY")
            && legacy_stderr.contains("expected `none` or `all`"),
        "{legacy_stderr}"
    );
    Ok(())
}

#[test]
fn deployment_profiles_render_and_reject_conflicting_retention() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("wallet-serving-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    let config = wallet_serving_ingest_config_toml(&storage_path)?
        .replace("coverage = \"wallet-serving\"\n", "");
    fs::write(&config_path, config)?;

    for (profile, full_blocks) in [
        ("light-wallet", None),
        ("wallet", None),
        ("wallet", Some("true")),
        ("explorer", None),
    ] {
        assert_profile_print_and_conflict(&config_path, profile, full_blocks)?;
    }

    // Wallet/light-wallet presets allow opting in to explorer_views = true, while
    // explorer enables explorer_views = true automatically and rejects false.
    let wallet_with_explorer = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env("ZINDER_DEPLOYMENT__PROFILE", "wallet")
        .env("ZINDER_INGEST__EXPLORER_VIEWS", "true")
        .output()?;
    assert!(
        wallet_with_explorer.status.success(),
        "{wallet_with_explorer:?}"
    );
    let wallet_stdout = String::from_utf8(wallet_with_explorer.stdout)?;
    assert!(
        wallet_stdout.contains("explorer_views = true"),
        "{wallet_stdout}"
    );

    let explorer_false = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env("ZINDER_DEPLOYMENT__PROFILE", "explorer")
        .env("ZINDER_INGEST__EXPLORER_VIEWS", "false")
        .output()?;
    assert!(!explorer_false.status.success(), "{explorer_false:?}");

    let legacy_key = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env("ZINDER_PROFILE", "light-wallet")
        .output()?;
    assert!(!legacy_key.status.success(), "{legacy_key:?}");

    Ok(())
}

#[test]
fn shared_env_tolerates_sibling_sections_derives_data_dir_and_redacts_control_tokens()
-> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let data_dir = tempdir.path().join("ingest-data-root");
    let stripped_config_path = tempdir.path().join("ingest-data-dir.toml");
    fs::write(
        &stripped_config_path,
        "[network]\nname = \"zcash-regtest\"\n\n[node]\njson_rpc_addr = \"http://127.0.0.1:39232\"\n\n[ingest]\nsource = \"zebra-json-rpc\"\n\n[explorer]\nlisten_addr = \"127.0.0.1:9068\"\n\n[projector]\nreorg_window_blocks = 100\n",
    )?;
    let shared_env_output = zinder_ingest_command()
        .args([
            "--print-config",
            "--config",
            path_str(&stripped_config_path)?,
        ])
        .env("ZINDER_STORAGE__DATA_DIR", &data_dir)
        .env(
            "ZINDER_INGEST_CONTROL__BEARER_TOKEN",
            "ingest-inline-secret",
        )
        .env(
            "ZINDER_INGEST_CONTROL__CHECKPOINT_BEARER_TOKEN",
            "ckpt-inline-secret",
        )
        .env("ZINDER_CIPHERSCAN__LISTEN_ADDR", "127.0.0.1:9070")
        .output()?;
    assert!(shared_env_output.status.success(), "{shared_env_output:?}");
    let shared_stdout = String::from_utf8(shared_env_output.stdout)?;
    assert!(
        shared_stdout.contains(&format!(
            "path = \"{}\"",
            data_dir.join("canonical").display()
        )),
        "{shared_stdout}"
    );
    assert!(
        shared_stdout.contains("bearer_token = \"[REDACTED]\""),
        "{shared_stdout}"
    );
    assert!(
        shared_stdout.contains("checkpoint_bearer_token = \"[REDACTED]\""),
        "{shared_stdout}"
    );
    assert!(
        !shared_stdout.contains("ingest-inline-secret"),
        "{shared_stdout}"
    );
    assert!(
        !shared_stdout.contains("ckpt-inline-secret"),
        "{shared_stdout}"
    );

    Ok(())
}

#[test]
fn wallet_serving_preserves_full_block_blob_retention() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("wallet-serving-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    let config = wallet_serving_ingest_config_toml(&storage_path)?.replacen(
        "[storage]\n",
        "[storage]\nraw_blob_policy = \"all\"\n",
        1,
    );
    fs::write(&config_path, config)?;

    let output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("raw_blob_policy = \"all\""), "{stdout}");

    Ok(())
}

#[test]
fn wallet_serving_rejects_explicit_checkpoint_height() -> Result<(), Box<dyn Error>> {
    assert_ingest_cli_rejects(
        |root| {
            wallet_serving_ingest_config_toml_with_checkpoint(
                &root.join("wallet-serving-checkpoint-store"),
            )
        },
        &[],
        &[],
        "ingest.run_overrides.coverage = \"wallet-serving\" requires complete transparent history and sets checkpoint_height to zero",
    )
}

#[test]
fn max_response_bytes_can_be_overridden_from_cli() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("max-response-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(&config_path, ingest_config_toml(&storage_path)?)?;

    let output = zinder_ingest_command()
        .args([
            "--print-config",
            "--config",
            path_str(&config_path)?,
            "--max-response-bytes",
            "8388608",
            "--source-segment-target-response-bytes",
            "8388608",
        ])
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("max_response_bytes = 8388608"), "{stdout}");
    assert!(
        stdout.contains("source_segment_target_response_bytes = 8388608"),
        "{stdout}"
    );

    Ok(())
}

#[test]
fn print_config_redacts_password_supplied_through_environment() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("env-password-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(
        &config_path,
        ingest_config_toml_without_auth(&storage_path)?,
    )?;

    let output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env("ZINDER_NODE__AUTH__METHOD", "basic")
        .env("ZINDER_NODE__AUTH__USERNAME", "zebra")
        .env("ZINDER_NODE__AUTH__PASSWORD", "env-secret")
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("password = \"[REDACTED]\""));
    assert!(!stdout.contains("env-secret"));

    Ok(())
}

#[test]
fn print_config_redacts_inline_cookie_supplied_through_environment() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("inline-cookie-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(
        &config_path,
        ingest_config_toml_without_auth(&storage_path)?,
    )?;

    let output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env("ZINDER_NODE__AUTH__METHOD", "cookie")
        .env("ZINDER_NODE__AUTH__COOKIE", "zebra:inline-cookie-secret")
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("method = \"cookie\""));
    assert!(!stdout.contains("inline-cookie-secret"));

    Ok(())
}

/// A fallback endpoint's file fields and environment fields merge.
///
/// The file declares the endpoint and the environment injects its password,
/// the usual secret pattern. The rendered tables keep position order with the
/// password redacted.
#[test]
fn fallback_endpoints_merge_file_and_environment_fields() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("fallback-merge-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(
        &config_path,
        format!(
            "{}\n[node_fallbacks.1]\njson_rpc_addr = \"http://zakura:39232\"\n\n[node_fallbacks.1.auth]\nmethod = \"basic\"\nusername = \"zakura\"\n",
            ingest_config_toml(&storage_path)?
        ),
    )?;

    let output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env(
            "ZINDER_NODE_FALLBACKS__1__AUTH__PASSWORD",
            "fallback-secret",
        )
        .env(
            "ZINDER_NODE_FALLBACKS__2__JSON_RPC_ADDR",
            "http://zebra-b:18232",
        )
        .output()?;

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(!stdout.contains("fallback-secret"), "{stdout}");
    let first = stdout
        .find("[node_fallbacks.1]")
        .ok_or("fallback 1 is rendered")?;
    let first_auth = stdout
        .find("[node_fallbacks.1.auth]")
        .ok_or("fallback 1 auth is rendered")?;
    let second = stdout
        .find("[node_fallbacks.2]")
        .ok_or("fallback 2 is rendered")?;
    assert!(first < first_auth && first_auth < second, "{stdout}");
    assert!(stdout.contains("json_rpc_addr = \"http://zakura:39232\""));
    assert!(stdout.contains("json_rpc_addr = \"http://zebra-b:18232\""));
    assert!(stdout[first_auth..second].contains("password = \"[REDACTED]\""));

    let rendered_path = tempdir.path().join("rendered.toml");
    fs::write(&rendered_path, &stdout)?;
    let round_trip = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&rendered_path)?])
        .env(
            "ZINDER_NODE_FALLBACKS__1__AUTH__PASSWORD",
            "fallback-secret",
        )
        .env("ZINDER_NODE__AUTH__PASSWORD", "file-secret")
        .output()?;
    assert!(round_trip.status.success(), "{round_trip:?}");
    assert_eq!(String::from_utf8(round_trip.stdout)?, stdout);
    Ok(())
}

#[test]
fn single_endpoint_print_config_has_no_fallback_tables() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("single-endpoint-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(&config_path, ingest_config_toml(&storage_path)?)?;

    let output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .output()?;

    assert!(output.status.success(), "{output:?}");
    assert!(!String::from_utf8(output.stdout)?.contains("node_fallbacks"));
    Ok(())
}

#[test]
fn fallback_endpoints_refuse_arrays_gaps_and_non_positions() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("fallback-refusal-store");
    let base = ingest_config_toml(&storage_path)?;
    for (tables, expected) in [
        (
            "[[node_fallbacks]]\njson_rpc_addr = \"http://zakura:39232\"\n",
            "array of tables is not accepted",
        ),
        (
            "[node_fallbacks.2]\njson_rpc_addr = \"http://zakura:39232\"\n",
            "[node_fallbacks.1] is missing",
        ),
        (
            "[node_fallbacks.zakura]\njson_rpc_addr = \"http://zakura:39232\"\n",
            "expected a position key",
        ),
        (
            "[node_fallbacks.1]\nrequest_timeout_secs = 5\n",
            "[node_fallbacks.1]",
        ),
    ] {
        let config_path = tempdir.path().join("zinder-ingest.toml");
        fs::write(&config_path, format!("{base}\n{tables}"))?;

        let output = zinder_ingest_command()
            .args(["--print-config", "--config", path_str(&config_path)?])
            .output()?;

        assert!(!output.status.success(), "{tables}: {output:?}");
        let stderr = String::from_utf8(output.stderr)?;
        assert!(stderr.contains(expected), "{tables}: {stderr}");
    }
    Ok(())
}

#[test]
fn zero_source_segment_max_blocks_fails_before_storage_creation() -> Result<(), Box<dyn Error>> {
    assert_ingest_cli_rejects(
        |root| ingest_config_toml(&root.join("zero-source-segment-store")),
        &[],
        &[(
            "ZINDER_INGEST__CONSTRUCTION__SOURCE_SEGMENT_MAX_BLOCKS",
            "0",
        )],
        "ingest.construction.source_segment_max_blocks must be greater than zero",
    )
}

#[test]
fn source_segment_max_blocks_above_node_batch_limit_fails_before_storage_creation()
-> Result<(), Box<dyn Error>> {
    assert_ingest_cli_rejects(
        |root| ingest_config_toml(&root.join("oversized-source-segment-store")),
        &[],
        &[(
            "ZINDER_INGEST__CONSTRUCTION__SOURCE_SEGMENT_MAX_BLOCKS",
            "1001",
        )],
        "invalid ingest.construction pipeline limits: source segment max blocks 1001 exceeds the node batch limit of 1000 requests",
    )
}

#[test]
fn zero_block_prepare_concurrency_fails_before_storage_creation() -> Result<(), Box<dyn Error>> {
    assert_ingest_cli_rejects(
        |root| ingest_config_toml(&root.join("zero-block-prepare")),
        &["--block-prepare-concurrency", "0"],
        &[],
        "ingest.construction.block_prepare_concurrency must be greater than zero",
    )
}

#[test]
fn zero_max_response_bytes_fails_before_storage_creation() -> Result<(), Box<dyn Error>> {
    assert_ingest_cli_rejects(
        |root| ingest_config_toml(&root.join("zero-response-store")),
        &["--max-response-bytes", "0"],
        &[],
        "node.max_response_bytes must be greater than zero",
    )
}

#[test]
fn source_fetch_byte_budget_below_max_response_fails_before_storage_creation()
-> Result<(), Box<dyn Error>> {
    assert_ingest_cli_rejects(
        |root| ingest_config_toml(&root.join("source-fetch-budget-store")),
        &[
            "--max-response-bytes",
            "67108864",
            "--source-fetch-max-in-flight-bytes",
            "33554432",
        ],
        &[],
        "invalid ingest.construction pipeline limits: source fetch watermark 33554432 is below maximum response 67108864",
    )
}

#[test]
fn zero_poll_interval_fails_before_storage_creation() -> Result<(), Box<dyn Error>> {
    assert_ingest_cli_rejects(
        |root| ingest_config_toml(&root.join("zero-poll-store")),
        &["--poll-interval-ms", "0"],
        &[],
        "ingest.follow.poll_interval_ms must be greater than zero",
    )
}

#[test]
fn storage_path_env_override_keeps_the_legacy_store() -> Result<(), Box<dyn Error>> {
    let output = zinder_ingest_command()
        .env("ZINDER_STORAGE__PATH", "/var/lib/zinder/store")
        .args([
            "--print-config",
            "--network",
            "zcash-regtest",
            "--node-source",
            "zebra-json-rpc",
            "--json-rpc-addr",
            "http://127.0.0.1:18232",
        ])
        .output()?;

    assert!(
        output.status.success(),
        "print-config failed: stderr=\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let config: toml::Table = toml::from_str(std::str::from_utf8(&output.stdout)?)?;
    assert_eq!(
        config["storage"]["path"].as_str(),
        Some("/var/lib/zinder/store")
    );

    Ok(())
}

#[test]
fn unknown_network_fails_before_storage_creation() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("unknown-network-store");
    let output = zinder_ingest_command()
        .args([
            "--print-config",
            "--network",
            "zcash-mars",
            "--node-source",
            "zebra-json-rpc",
            "--json-rpc-addr",
            "http://127.0.0.1:18232",
            "--storage-path",
            path_str(&storage_path)?,
        ])
        .output()?;

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("network.name") || stderr.contains("zcash-mars"),
        "{stderr}"
    );

    Ok(())
}

#[test]
fn partial_basic_auth_fails_before_storage_creation() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("partial-auth-store");
    let output = zinder_ingest_command()
        .args([
            "--print-config",
            "--network",
            "zcash-regtest",
            "--node-source",
            "zebra-json-rpc",
            "--json-rpc-addr",
            "http://127.0.0.1:18232",
            "--storage-path",
            path_str(&storage_path)?,
            "--node-auth-method",
            "basic",
            "--node-auth-username",
            "zebra",
        ])
        .output()?;

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("missing configuration field: node.auth.password"),
        "{stderr}"
    );

    Ok(())
}

fn run_wallet_serving_ingest_print_config(
    temporary_root: &Path,
    config_path: &Path,
    store_path: &Path,
    raw_blob_policy: Option<&str>,
    profile: Option<&str>,
) -> Result<Output, Box<dyn Error>> {
    let token_path = temporary_root.join("fixture.token");
    fs::write(&token_path, "fixture-token\n")?;

    let mut command = zinder_ingest_command();
    command
        .args(["--print-config", "--config"])
        .arg(config_path)
        .env("ZINDER_NETWORK__NAME", "zcash-regtest")
        .env("ZINDER_NODE__JSON_RPC_ADDR", "http://127.0.0.1:18232")
        .env("ZINDER_NODE__INDEXER_GRPC_ADDR", "http://127.0.0.1:18155")
        .env("ZINDER_STORAGE__PATH", store_path)
        .env("ZINDER_INGEST_CONTROL__BEARER_TOKEN_PATH", &token_path)
        .env(
            "ZINDER_INGEST_CONTROL__CHECKPOINT_BEARER_TOKEN_PATH",
            &token_path,
        );
    match raw_blob_policy {
        Some(policy_name) => {
            command.env("ZINDER_STORAGE__RAW_BLOB_POLICY", policy_name);
        }
        None => {
            command.env_remove("ZINDER_STORAGE__RAW_BLOB_POLICY");
        }
    }
    match profile {
        Some(profile_name) => {
            command.env("ZINDER_DEPLOYMENT__PROFILE", profile_name);
        }
        None => {
            command.env_remove("ZINDER_DEPLOYMENT__PROFILE");
        }
    }
    Ok(command.output()?)
}

fn assert_single_volume_legacy_transactions_fails_closed(
    temporary_root: &Path,
    config_path: &Path,
    store_path: &Path,
) -> Result<(), Box<dyn Error>> {
    let rejected = run_wallet_serving_ingest_print_config(
        temporary_root,
        config_path,
        store_path,
        Some("transactions"),
        None,
    )?;
    assert!(!rejected.status.success(), "{rejected:?}");
    let stderr = String::from_utf8(rejected.stderr)?;
    assert!(
        stderr.contains("ZINDER_STORAGE__RAW_BLOB_POLICY")
            && stderr.contains("expected `none` or `all`"),
        "{stderr}"
    );
    Ok(())
}

async fn assert_binary_retention_mismatch_rejected(
    source: &crate::common::construction_fixture::MultiBlockConstructionSource,
    activations: &std::sync::Arc<zinder_core::NetworkUpgradeActivations>,
    none_config: &zinder_ingest::CanonicalWriterConfig,
    readiness: &zinder_runtime::ComposedReadiness,
) -> Result<(), Box<dyn Error>> {
    use std::sync::{Arc, atomic::Ordering};
    use tokio_util::sync::CancellationToken;
    use zinder_ingest::{CanonicalWriterConfig, CanonicalWriterError, admit_canonical_store};
    use zinder_store::{CanonicalStoreError, RawBlobRetention};

    let initial_store = admit_canonical_store(
        source,
        Arc::clone(activations),
        none_config,
        readiness,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(initial_store.raw_blob_retention(), RawBlobRetention::None);
    let construction_requests = source.chain_segment_requests.load(Ordering::SeqCst);
    drop(initial_store);

    let full_blocks_config = CanonicalWriterConfig {
        raw_blob_retention: RawBlobRetention::All,
        ..none_config.clone()
    };
    let rejected = admit_canonical_store(
        source,
        Arc::clone(activations),
        &full_blocks_config,
        readiness,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("full_blocks = true must reject reopening a none-retention store")?;
    assert!(
        matches!(
            &rejected,
            CanonicalWriterError::Store(CanonicalStoreError::AdmissionRefused { reason, .. })
                if reason.contains("raw blob retention")
                    && reason.contains("none")
                    && reason.contains("all")
        ),
        "{rejected}"
    );

    let reopened = admit_canonical_store(
        source,
        Arc::clone(activations),
        none_config,
        readiness,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(reopened.raw_blob_retention(), RawBlobRetention::None);
    assert_eq!(
        source.chain_segment_requests.load(Ordering::SeqCst),
        construction_requests
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_transactions_policy_fails_closed_and_binary_retention_mismatch_is_refused()
-> Result<(), Box<dyn Error>> {
    use std::sync::Arc;
    use zinder_runtime::{ComposedReadiness, Readiness};

    use crate::common::construction_fixture::{
        MultiBlockConstructionSource, multi_block_writer_config, regtest_activations,
    };

    let activations = regtest_activations();
    let source =
        MultiBlockConstructionSource::build_with_transactions(2, &activations, |_| Ok(Vec::new()))?;
    let directory = tempdir()?;
    let wallet_serving_ingest_config = directory.path().join("wallet-serving-ingest.toml");
    fs::write(
        &wallet_serving_ingest_config,
        concat!(
            "[storage]\npath = \"/var/lib/zinder/canonical\"\n\n",
            "[ingest]\nsource = \"zebra-indexer-grpc\"\nexplorer_views = false\nreorg_window_blocks = 1000\n\n",
            "[ingest.run_overrides]\ncoverage = \"wallet-serving\"\n\n",
            "[ingest_control]\nlisten_addr = \"127.0.0.1:9100\"\n",
            "checkpoint_staging_root = \"/var/lib/zinder/checkpoints\"\n",
            "bearer_token_path = \"/var/run/zinder-control/ingest.token\"\n",
            "checkpoint_bearer_token_path = \"/var/run/zinder-checkpoint/checkpoint.token\"\n",
        ),
    )?;
    let store_path = directory.path().join("canonical");
    assert_single_volume_legacy_transactions_fails_closed(
        directory.path(),
        &wallet_serving_ingest_config,
        &store_path,
    )?;

    let none_config =
        multi_block_writer_config(store_path.clone(), Arc::clone(&activations), false);
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    Box::pin(assert_binary_retention_mismatch_rejected(
        &source,
        &activations,
        &none_config,
        &readiness,
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mounted_toml_wallet_profile_with_full_blocks_preserves_all_retention()
-> Result<(), Box<dyn Error>> {
    use std::sync::Arc;

    use tokio_util::sync::CancellationToken;
    use zinder_ingest::{CanonicalWriterConfig, CanonicalWriterError, admit_canonical_store};
    use zinder_runtime::{ComposedReadiness, Readiness};
    use zinder_store::{CanonicalStoreError, RawBlobRetention};

    use crate::common::construction_fixture::{
        MultiBlockConstructionSource, multi_block_writer_config, regtest_activations,
    };

    let directory = tempdir()?;
    let custom_ingest_toml = directory.path().join("ingest.toml");
    fs::write(
        &custom_ingest_toml,
        concat!(
            "[deployment]\nprofile = \"wallet\"\n\n",
            "[storage]\nfull_blocks = true\npath = \"/var/lib/zinder/canonical\"\n\n",
            "[ingest]\nsource = \"zebra-indexer-grpc\"\nexplorer_views = false\nreorg_window_blocks = 1000\n\n",
            "[ingest_control]\nlisten_addr = \"127.0.0.1:9100\"\n",
            "checkpoint_staging_root = \"/var/lib/zinder/checkpoints\"\n",
            "bearer_token_path = \"/var/run/zinder-control/ingest.token\"\n",
            "checkpoint_bearer_token_path = \"/var/run/zinder-checkpoint/checkpoint.token\"\n",
        ),
    )?;

    let store_path = directory.path().join("canonical");
    // When deployment.profile = "wallet" is paired with storage.full_blocks = true,
    // ingest keeps full blocks (`all` retention) rather than transaction-only blobs.
    let printed = run_wallet_serving_ingest_print_config(
        directory.path(),
        &custom_ingest_toml,
        &store_path,
        None,
        Some("wallet"),
    )?;
    assert!(printed.status.success(), "{printed:?}");
    let stdout = String::from_utf8(printed.stdout)?;
    assert!(stdout.contains("profile = \"wallet\""), "{stdout}");
    assert!(stdout.contains("full_blocks = true"), "{stdout}");
    assert!(!stdout.contains("raw_blob_policy ="), "{stdout}");

    let activations = regtest_activations();
    let source =
        MultiBlockConstructionSource::build_with_transactions(2, &activations, |_| Ok(Vec::new()))?;
    let archive_config = CanonicalWriterConfig {
        raw_blob_retention: RawBlobRetention::All,
        ..multi_block_writer_config(store_path, Arc::clone(&activations), false)
    };
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let archive_store = admit_canonical_store(
        &source,
        Arc::clone(&activations),
        &archive_config,
        &readiness,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(archive_store.raw_blob_retention(), RawBlobRetention::All);
    drop(archive_store);

    // Clobbering with `wallet` (`none`) would fail admission on an existing archive store.
    let lean_config = CanonicalWriterConfig {
        raw_blob_retention: RawBlobRetention::None,
        ..archive_config
    };
    let rejected = admit_canonical_store(
        &source,
        Arc::clone(&activations),
        &lean_config,
        &readiness,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("wallet profile must reject reopening a wallet-archive store")?;
    assert!(
        matches!(
            &rejected,
            CanonicalWriterError::Store(CanonicalStoreError::AdmissionRefused { reason, .. })
                if reason.contains("raw blob retention")
                    && reason.contains("all")
                    && reason.contains("none")
        ),
        "{rejected}"
    );
    Ok(())
}

#[test]
fn data_dir_derives_checkpoint_staging_root_and_rejects_sibling_typo() -> Result<(), Box<dyn Error>>
{
    let directory = tempdir()?;
    let custom_dir = directory.path().join("ingest-root");
    let shared_toml = directory.path().join("shared.toml");
    fs::write(
        &shared_toml,
        concat!(
            "[deployment]\nprofile = \"wallet-archive\"\n",
            "[storage]\n",
            "canonical_path = \"/unused/by/ingest\"\n",
            "secondary_path = \"/unused/by/ingest/secondary\"\n",
            "[wallet]\n",
            "path = \"/unused/wallet\"\n",
            "secondary_path = \"/unused/wsec\"\n",
            "[query]\nlisten_addr = \"127.0.0.1:9102\"\n",
        ),
    )?;

    let output = zinder_ingest_command()
        .arg("--print-config")
        .arg("--config")
        .arg(&shared_toml)
        .env("ZINDER_NETWORK__NAME", "zcash-regtest")
        .env("ZINDER_NODE__JSON_RPC_ADDR", "http://127.0.0.1:39232")
        .env("ZINDER_STORAGE__DATA_DIR", &custom_dir)
        .env("ZINDER_STORAGE__CANONICAL_PATH", "/unused/by/ingest/env")
        .env(
            "ZINDER_STORAGE__SECONDARY_PATH",
            "/unused/by/ingest/sec-env",
        )
        .output()?;
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("profile = \"wallet-archive\""), "{stdout}");
    assert!(
        stdout.contains(&format!(
            "checkpoint_staging_root = \"{}\"",
            custom_dir.join("checkpoints").display()
        )),
        "{stdout}"
    );

    let sibling_env_typo = zinder_ingest_command()
        .arg("--print-config")
        .env("ZINDER_NETWORK__NAME", "zcash-regtest")
        .env("ZINDER_NODE__JSON_RPC_ADDR", "http://127.0.0.1:39232")
        .env("ZINDER_QUERY__LISTEN_ADRR", "127.0.0.1:9102")
        .output()?;
    assert!(!sibling_env_typo.status.success());
    let stderr = String::from_utf8(sibling_env_typo.stderr)?;
    assert!(stderr.contains("ZINDER_QUERY__LISTEN_ADRR"), "{stderr}");
    assert!(stderr.contains("ZINDER_QUERY__LISTEN_ADDR"), "{stderr}");

    let storage_env_typo = zinder_ingest_command()
        .arg("--print-config")
        .env("ZINDER_NETWORK__NAME", "zcash-regtest")
        .env("ZINDER_NODE__JSON_RPC_ADDR", "http://127.0.0.1:39232")
        .env("ZINDER_STORAGE__SECONDRY_PATH", "/typo")
        .output()?;
    assert!(!storage_env_typo.status.success());
    let storage_stderr = String::from_utf8(storage_env_typo.stderr)?;
    assert!(
        storage_stderr.contains("unknown field `secondry_path`"),
        "{storage_stderr}"
    );
    Ok(())
}

fn assert_ingest_cli_rejects(
    build_config_toml: impl FnOnce(&Path) -> Result<String, Box<dyn Error>>,
    args: &[&str],
    environment: &[(&str, &str)],
    expected_error: &str,
) -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(&config_path, build_config_toml(tempdir.path())?)?;

    let mut command = zinder_ingest_command();
    command.args(["--print-config", "--config", path_str(&config_path)?]);
    command.args(args);
    command.envs(environment.iter().copied());
    let output = command.output()?;

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains(expected_error), "{stderr}");

    Ok(())
}

fn ingest_config_toml(storage_path: &Path) -> Result<String, Box<dyn Error>> {
    Ok(format!(
        r#"[network]
name = "zcash-regtest"

[node]
json_rpc_addr = "http://127.0.0.1:39232"
request_timeout_secs = 30

[node.auth]
method = "basic"
username = "zebra"
password = "file-secret"

[storage]
path = "{}"

[ingest]
source = "zebra-json-rpc"
reorg_window_blocks = 100

[ingest_control]
listen_addr = "127.0.0.1:9100"
"#,
        path_str(storage_path)?,
    ))
}

fn ingest_config_toml_without_auth(storage_path: &Path) -> Result<String, Box<dyn Error>> {
    Ok(format!(
        r#"[network]
name = "zcash-regtest"

[node]
json_rpc_addr = "http://127.0.0.1:39232"
request_timeout_secs = 30

[storage]
path = "{}"

[ingest]
source = "zebra-json-rpc"
reorg_window_blocks = 100

[ingest_control]
listen_addr = "127.0.0.1:9100"
"#,
        path_str(storage_path)?,
    ))
}

fn wallet_serving_ingest_config_toml(storage_path: &Path) -> Result<String, Box<dyn Error>> {
    Ok(format!(
        r#"[network]
name = "zcash-regtest"

[node]
json_rpc_addr = "http://127.0.0.1:39232"
request_timeout_secs = 30

[node.auth]
method = "basic"
username = "zebra"
password = "file-secret"

[storage]
path = "{}"

[ingest]
source = "zebra-json-rpc"
reorg_window_blocks = 100

[ingest.run_overrides]
coverage = "wallet-serving"

[ingest_control]
listen_addr = "127.0.0.1:9100"
"#,
        path_str(storage_path)?,
    ))
}

fn wallet_serving_ingest_config_toml_with_checkpoint(
    storage_path: &Path,
) -> Result<String, Box<dyn Error>> {
    Ok(format!(
        r#"[network]
name = "zcash-regtest"

[node]
json_rpc_addr = "http://127.0.0.1:39232"
request_timeout_secs = 30

[node.auth]
method = "basic"
username = "zebra"
password = "file-secret"

[storage]
path = "{}"

[ingest]
source = "zebra-json-rpc"
reorg_window_blocks = 100

[ingest.run_overrides]
coverage = "wallet-serving"
checkpoint_height = 1

[ingest_control]
listen_addr = "127.0.0.1:9100"
"#,
        path_str(storage_path)?,
    ))
}

fn cookie_ingest_config_toml(
    storage_path: &Path,
    cookie_path: &Path,
) -> Result<String, Box<dyn Error>> {
    Ok(format!(
        r#"[network]
name = "zcash-regtest"

[node]
json_rpc_addr = "http://127.0.0.1:39232"
request_timeout_secs = 30

[node.auth]
method = "cookie"
path = "{}"

[storage]
path = "{}"

[ingest]
source = "zebra-json-rpc"
reorg_window_blocks = 100

[ingest_control]
listen_addr = "127.0.0.1:9100"
"#,
        path_str(cookie_path)?,
        path_str(storage_path)?,
    ))
}

fn path_str(path: &Path) -> Result<&str, Box<dyn Error>> {
    path.to_str()
        .ok_or_else(|| format!("path is not valid UTF-8: {}", path.display()).into())
}
