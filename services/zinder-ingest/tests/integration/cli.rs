#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{error::Error, fs, path::Path};

use tempfile::tempdir;

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
fn indexer_tls_pem_variable_names_the_images_that_accept_it() -> Result<(), Box<dyn Error>> {
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("indexer-tls-pem-store");
    let config_path = tempdir.path().join("zinder-ingest.toml");
    fs::write(&config_path, ingest_config_toml(&storage_path)?)?;
    let output = zinder_ingest_command()
        .args(["--print-config", "--config", path_str(&config_path)?])
        .env(
            "ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM",
            "-----BEGIN PRIVATE KEY-----\nkey-material-sentinel\n-----END PRIVATE KEY-----\n",
        )
        .output()?;

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM"),
        "{stderr}"
    );
    assert!(stderr.contains("zinder-canonical-runtime"), "{stderr}");
    assert!(stderr.contains("zinder-wallet-serving"), "{stderr}");
    assert!(
        stderr.contains("ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH"),
        "{stderr}"
    );
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
    fs::write(&source_config_path, ingest_config_toml(&storage_path)?)?;

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
    (
        "storage",
        "secondary_path",
        "ZINDER_STORAGE__SECONDARY_PATH",
    ),
];

fn removed_setting_toml_value(field: &str) -> &'static str {
    match field {
        "allow_reorg_window_settlement" => "true",
        "secondary_path" => "\"/var/lib/zinder/ingest-secondary\"",
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
                Ok(if *section == "storage" {
                    base.replacen("[storage]\n", &format!("[storage]\n{field} = {value}\n"), 1)
                } else {
                    format!("{base}\n[{section}]\n{field} = {value}\n")
                })
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
        let value = if *field == "secondary_path" {
            "/var/lib/zinder/ingest-secondary"
        } else {
            removed_setting_toml_value(field)
        };
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
    assert!(
        stdout.contains("raw_blob_policy = \"transactions\""),
        "{stdout}"
    );

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
fn wallet_serving_rejects_no_transaction_blob_retention() -> Result<(), Box<dyn Error>> {
    assert_ingest_cli_rejects(
        |root| {
            Ok(
                wallet_serving_ingest_config_toml(&root.join("wallet-serving-store"))?.replacen(
                    "[storage]\n",
                    "[storage]\nraw_blob_policy = \"none\"\n",
                    1,
                ),
            )
        },
        &[],
        &[],
        "ingest.run_overrides.coverage = \"wallet-serving\" requires storage.raw_blob_policy = \"transactions\" or \"all\"",
    )
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
fn storage_path_default_resolves_to_canonical_zinder_layout() -> Result<(), Box<dyn Error>> {
    // The binary's default for `storage.path` matches the canonical Zinder
    // layout under `/var/lib/zinder/store`. Operators on non-PaaS hosts
    // override via `ZINDER_STORAGE__PATH` or the `--storage-path` flag.
    // This test guards the default's stability so the env-only deployment
    // shape (single-container Docker image) keeps working without a TOML
    // file or a `--config` argument.
    let output = zinder_ingest_command()
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
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        stdout.contains("path = \"/var/lib/zinder/store\"")
            || stdout.contains("path = '/var/lib/zinder/store'"),
        "stdout does not carry the canonical storage.path default:\n{stdout}"
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
