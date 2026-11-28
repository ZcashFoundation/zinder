use std::{
    fs,
    path::Path,
    process::Command,
    sync::atomic::{AtomicBool, Ordering},
};

use tempfile::tempdir;
use tonic::{Request, Response, Status};
use zinder_proto::v1::ingest::{
    CreateStateBundleCaptureRequest, CreateStateBundleCaptureResponse,
    projector_control_server::ProjectorControl,
};

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
fn private_backup_requires_authenticated_owner_control() -> eyre::Result<()> {
    let directory = tempdir()?;
    let config = directory.path().join("projector.toml");
    fs::write(&config, "[network]\nname = \"zcash-regtest\"\n")?;
    let archive = directory.path().join("wallet-backup");
    let output = projector_command()
        .arg("--config")
        .arg(config)
        .args(["backup", "--archive"])
        .arg(&archive)
        .output()?;
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("projector_control.listen_addr"), "{stderr}");
    assert!(!archive.try_exists()?);
    Ok(())
}

#[derive(Default)]
struct StalledCaptureOwner {
    received_capture: AtomicBool,
}
#[tonic::async_trait]
impl ProjectorControl for StalledCaptureOwner {
    async fn create_state_bundle_capture(
        &self,
        request: Request<CreateStateBundleCaptureRequest>,
    ) -> Result<Response<CreateStateBundleCaptureResponse>, Status> {
        let request = request.into_inner();
        if request.candidate_id != "wallet-backup" || !request.reuse_completed {
            return Err(Status::invalid_argument("unexpected capture request"));
        }
        if !self.received_capture.swap(true, Ordering::Relaxed) {
            std::future::pending::<()>().await;
        }
        Err(Status::failed_precondition("incomplete capture"))
    }
}

#[tokio::test]
async fn backup_times_out_waiting_for_the_owner_and_retries_the_named_candidate() -> eyre::Result<()>
{
    use std::{str::FromStr as _, time::Duration};
    use tokio_stream::wrappers::TcpListenerStream;
    use zinder_proto::v1::ingest::projector_control_server::ProjectorControlServer;
    use zinder_runtime::{BearerToken, BearerTokenServerInterceptor};

    const TOKEN: &str = "fixture-projector-token";
    let directory = tempdir()?;
    let token_path = directory.path().join("owner-token");
    fs::write(&token_path, TOKEN)?;
    let staging = directory.path().join("staging");
    fs::create_dir(&staging)?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let config = directory.path().join("projector.toml");
    fs::write(
        &config,
        format!(
            "[network]\nname = \"zcash-regtest\"\n[projector_control]\nlisten_addr = \"{address}\"\nbearer_token_path = {:?}\ncheckpoint_staging_root = {:?}\n",
            path_str(&token_path)?,
            path_str(&staging)?
        ),
    )?;
    let owner = ProjectorControlServer::with_interceptor(
        StalledCaptureOwner::default(),
        BearerTokenServerInterceptor::new(Some(BearerToken::from_str(TOKEN)?)),
    );
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(owner)
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let archive = directory.path().join("wallet-backup");
    let mut invocation = tokio::process::Command::from(projector_command());
    invocation
        .kill_on_drop(true)
        .arg("--config")
        .arg(config)
        .args(["backup", "--capture-timeout-seconds", "1", "--archive"])
        .arg(&archive);
    let timeout = tokio::time::timeout(Duration::from_secs(15), invocation.output()).await??;
    assert!(!timeout.status.success());
    let stderr = String::from_utf8(timeout.stderr)?;
    assert!(stderr.contains("timed out after 1s"), "{stderr}");
    assert!(stderr.contains("wallet-backup"), "{stderr}");
    assert!(stderr.contains("Retry with the same --archive"), "{stderr}");
    assert!(!stderr.contains(TOKEN));
    assert_eq!(timeout.stdout, b"");
    assert!(!archive.try_exists()?);

    let output = tokio::time::timeout(Duration::from_secs(15), invocation.output()).await??;
    server.abort();
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("incomplete capture"), "{stderr}");
    assert!(!stderr.contains(TOKEN));
    assert_eq!(output.stdout, b"");
    assert!(!archive.try_exists()?);
    Ok(())
}

#[test]
fn backup_rejects_a_zero_capture_timeout() -> eyre::Result<()> {
    let output = projector_command()
        .args([
            "backup",
            "--capture-timeout-seconds",
            "0",
            "--archive",
            "test",
        ])
        .output()?;
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("invalid value '0'"), "{stderr}");
    assert!(stderr.contains("--capture-timeout-seconds"), "{stderr}");
    Ok(())
}

#[test]
fn backup_reports_unknown_projector_control_environment_settings() -> eyre::Result<()> {
    let directory = tempdir()?;
    let archive = directory.path().join("backup");
    let output = projector_command()
        .env("ZINDER_PROJECTOR_CONTROL__UNKNOWN_OPTION", "true")
        .args(["backup", "--archive"])
        .arg(&archive)
        .output()?;
    assert!(!output.status.success());
    let error = String::from_utf8(output.stderr)?;
    assert!(
        error.contains("unknown_option") && error.contains("projector_control"),
        "{error}"
    );
    assert!(!archive.exists());
    Ok(())
}

#[test]
fn offline_backup_commands_require_an_explicit_network_and_digest() -> eyre::Result<()> {
    for command in ["verify", "restore"] {
        let mut invocation = projector_command();
        invocation.args([command, "--archive", "/unused/backup"]);
        if command == "restore" {
            invocation.args(["--into", "/unused/replacement"]);
        }
        let missing = invocation.output()?;
        assert!(!missing.status.success());
        let stderr = String::from_utf8(missing.stderr)?;
        assert!(
            stderr.contains("--network") && stderr.contains("--sha256"),
            "{stderr}"
        );

        let malformed = invocation
            .args(["--network", "zcash-regtest", "--sha256", "f0"])
            .output()?;
        assert!(!malformed.status.success());
        let stderr = String::from_utf8(malformed.stderr)?;
        assert!(
            stderr.contains("invalid value") && stderr.contains("--sha256"),
            "{stderr}"
        );
    }
    Ok(())
}

#[test]
fn offline_backup_failure_leaves_the_destination_absent_without_writer_config() -> eyre::Result<()>
{
    let directory = tempdir()?;
    let archive = directory.path().join("missing-archive");
    let destination = directory.path().join("replacement");
    for command in ["verify", "restore"] {
        let mut invocation = projector_command();
        invocation
            .arg("--config")
            .arg(directory.path().join("missing-projector.toml"))
            .env("ZINDER_NODE__JSON_RPC_ADDR", "invalid")
            .args([command, "--archive"])
            .arg(&archive)
            .args(["--network", "zcash-regtest", "--sha256", &"00".repeat(32)]);
        if command == "restore" {
            invocation.arg("--into").arg(&destination);
        }
        let output = invocation.output()?;
        assert!(!output.status.success());
        let stderr = String::from_utf8(output.stderr)?;
        assert!(stderr.contains("missing-archive"), "{stderr}");
        assert_eq!(output.stdout, b"");
        assert!(!destination.try_exists()?);
    }
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
            "path = {:?}",
            canonical_path.display().to_string()
        )),
        "{stdout}"
    );
    assert!(
        stdout.contains(&format!(
            "secondary_path = {:?}",
            canonical_secondary_path.display().to_string()
        )),
        "{stdout}"
    );
    assert!(stdout.contains("raw_blob_policy = \"none\""), "{stdout}");
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
path = "{}"
secondary_path = "{}"
unsupported_path = "{}"

[projector]
reorg_window_blocks = 100
build_owner_hex = "00112233445566778899aabbccddeeff"
lease_duration_seconds = 14400
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

/// The projector ignores sibling `[node]` and `[node_fallbacks]` settings in
/// shared environments, while still refusing the CLI `--node-json-rpc-addr`
/// flag and unknown typo sections.
#[test]
fn node_cli_flag_and_unknown_sections_are_rejected_while_sibling_node_is_tolerated()
-> eyre::Result<()> {
    let flag = projector_command()
        .args([
            "--print-config",
            "--node-json-rpc-addr",
            "http://127.0.0.1:18232",
        ])
        .output()?;
    assert!(!flag.status.success(), "{flag:?}");
    assert!(
        String::from_utf8(flag.stderr)?.contains("--node-json-rpc-addr"),
        "the removed flag must be named in the refusal"
    );

    let sibling = projector_command()
        .env_clear()
        .args([
            "--print-config",
            "--network",
            "zcash-regtest",
            "--lease-duration-seconds",
            "14400",
        ])
        .env("ZINDER_NODE__JSON_RPC_ADDR", "http://127.0.0.1:18232")
        .env(
            "ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR",
            "http://127.0.0.1:28232",
        )
        .output()?;
    assert!(sibling.status.success(), "{sibling:?}");

    let typo = projector_command()
        .env_clear()
        .args([
            "--print-config",
            "--network",
            "zcash-regtest",
            "--lease-duration-seconds",
            "14400",
        ])
        .env("ZINDER_UNKNOWN_SECTION__VALUE", "x")
        .output()?;
    assert!(!typo.status.success(), "{typo:?}");
    let stderr = String::from_utf8(typo.stderr)?;
    assert!(
        stderr.contains("unknown field `unknown_section`"),
        "{stderr}"
    );
    Ok(())
}

fn projector_command() -> Command {
    Command::new(zinder_testkit::bin_exe!("zinder-projector"))
}

fn path_str(path: &Path) -> eyre::Result<&str> {
    path.to_str()
        .ok_or_else(|| eyre::eyre!("test path is not UTF-8: {path:?}"))
}

#[test]
fn wallet_profiles_supply_omitted_retention_and_round_trip_printed_config() -> eyre::Result<()> {
    let directory = tempdir()?;
    let config_path = directory.path().join("projector.toml");
    for (profile, full_blocks) in [
        ("wallet", None),
        ("wallet", Some("true")),
        ("explorer", None),
    ] {
        let initial = || {
            let mut command = projector_command();
            command
                .env_clear()
                .args([
                    "--print-config",
                    "--network",
                    "zcash-regtest",
                    "--build-owner-hex",
                    "00112233445566778899aabbccddeeff",
                    "--lease-duration-seconds",
                    "14400",
                ])
                .env("ZINDER_DEPLOYMENT__PROFILE", profile);
            if let Some(full_blocks) = full_blocks {
                command.env("ZINDER_STORAGE__FULL_BLOCKS", full_blocks);
            }
            command
        };
        let output = initial().output()?;
        assert!(output.status.success(), "{output:?}");
        let rendered = String::from_utf8(output.stdout)?;
        assert!(rendered.contains(&format!("[deployment]\nprofile = \"{profile}\"")));
        if full_blocks == Some("true") {
            assert!(rendered.contains("full_blocks = true"), "{rendered}");
        }
        assert!(!rendered.contains("raw_blob_policy ="));
        fs::write(&config_path, &rendered)?;

        let round_trip = projector_command()
            .env_clear()
            .args(["--print-config", "--config", path_str(&config_path)?])
            .output()?;
        assert!(round_trip.status.success(), "{round_trip:?}");
        assert_eq!(String::from_utf8(round_trip.stdout)?, rendered);

        let conflicting_policy = if full_blocks == Some("true") {
            "none"
        } else {
            "all"
        };
        let conflict = initial()
            .env("ZINDER_STORAGE__RAW_BLOB_POLICY", conflicting_policy)
            .output()?;
        assert!(!conflict.status.success(), "{conflict:?}");
        assert!(String::from_utf8(conflict.stderr)?.contains("storage.raw_blob_policy"));

        let legacy_transactions = initial()
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
    }
    Ok(())
}

#[test]
fn startup_creates_nested_secondary_and_reports_missing_paths() -> eyre::Result<()> {
    let directory = tempdir()?;
    let missing_canonical = directory.path().join("missing/canonical");
    let nested_secondary = directory.path().join("nested/parent/canonical-secondary");
    let wallet_path = directory.path().join("wallet");
    let created = projector_command()
        .env("ZINDER_OPS__LISTEN_ADDR", "none")
        .args([
            "--network",
            "zcash-regtest",
            "--canonical-path",
            path_str(&missing_canonical)?,
            "--canonical-secondary-path",
            path_str(&nested_secondary)?,
            "--wallet-path",
            path_str(&wallet_path)?,
            "--build-owner-hex",
            "00112233445566778899aabbccddeeff",
            "--lease-duration-seconds",
            "14400",
        ])
        .output()?;
    assert!(!created.status.success(), "{created:?}");
    assert!(
        nested_secondary.is_dir(),
        "missing nested canonical_secondary_path parents should be created before opening"
    );
    let stderr = String::from_utf8(created.stderr)?;
    assert!(stderr.contains(path_str(&nested_secondary)?), "{stderr}");
    assert!(stderr.contains(path_str(&missing_canonical)?), "{stderr}");

    let file_obstacle = directory.path().join("blocker");
    fs::write(&file_obstacle, b"not-a-directory")?;
    let uncreatable_secondary = file_obstacle.join("canonical-secondary");
    let failure_output = projector_command()
        .env("ZINDER_OPS__LISTEN_ADDR", "none")
        .args([
            "--network",
            "zcash-regtest",
            "--canonical-path",
            path_str(&missing_canonical)?,
            "--canonical-secondary-path",
            path_str(&uncreatable_secondary)?,
            "--wallet-path",
            path_str(&wallet_path)?,
            "--build-owner-hex",
            "00112233445566778899aabbccddeeff",
            "--lease-duration-seconds",
            "14400",
        ])
        .output()?;
    assert!(!failure_output.status.success(), "{failure_output:?}");
    let failure_stderr = String::from_utf8(failure_output.stderr)?;
    assert!(
        failure_stderr.contains(path_str(&uncreatable_secondary)?),
        "{failure_stderr}"
    );
    Ok(())
}

#[test]
fn data_dir_optional_build_owner_and_inline_bearer_tokens_work() -> eyre::Result<()> {
    let scratch = tempdir()?;
    let custom_dir = scratch.path().join("projector-root");
    let expected_owner_file = custom_dir.join("wallet.build-owner");
    let staging = scratch.path().join("staging");

    let output = projector_command()
        .env_clear()
        .args([
            "--print-config",
            "--network",
            "zcash-regtest",
            "--lease-duration-seconds",
            "14400",
        ])
        .env("ZINDER_STORAGE__DATA_DIR", &custom_dir)
        .env("ZINDER_INGEST_CONTROL__BEARER_TOKEN", "proj-ingest-secret")
        .env("ZINDER_PROJECTOR_CONTROL__LISTEN_ADDR", "127.0.0.1:9109")
        .env(
            "ZINDER_PROJECTOR_CONTROL__BEARER_TOKEN",
            "proj-control-secret",
        )
        .env(
            "ZINDER_PROJECTOR_CONTROL__CHECKPOINT_STAGING_ROOT",
            &staging,
        )
        .output()?;
    assert!(output.status.success(), "{output:?}");
    assert!(
        !expected_owner_file.exists(),
        "--print-config must not create build-owner state on disk"
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        stdout.contains(&format!(
            "path = \"{}\"",
            custom_dir.join("canonical").display()
        )),
        "{stdout}"
    );
    assert!(
        stdout.contains(&format!(
            "secondary_path = \"{}\"",
            custom_dir.join("canonical-secondary/projector").display()
        )),
        "{stdout}"
    );
    assert!(
        stdout.contains(&format!(
            "path = \"{}\"",
            custom_dir.join("wallet").display()
        )),
        "{stdout}"
    );
    assert!(!stdout.contains("build_owner_hex"), "{stdout}");
    assert!(stdout.contains("bearer_token = \"[REDACTED]\""), "{stdout}");
    assert!(!stdout.contains("proj-ingest-secret"), "{stdout}");
    assert!(!stdout.contains("proj-control-secret"), "{stdout}");

    fs::create_dir_all(&custom_dir)?;
    fs::write(&expected_owner_file, "00112233445566778899aabbccddeeff")?;
    let with_persisted = projector_command()
        .env_clear()
        .args([
            "--print-config",
            "--network",
            "zcash-regtest",
            "--lease-duration-seconds",
            "14400",
        ])
        .env("ZINDER_STORAGE__DATA_DIR", &custom_dir)
        .output()?;
    assert!(with_persisted.status.success(), "{with_persisted:?}");
    let persisted_stdout = String::from_utf8(with_persisted.stdout)?;
    assert!(
        persisted_stdout.contains("build_owner_hex = \"00112233445566778899aabbccddeeff\""),
        "{persisted_stdout}"
    );
    Ok(())
}

#[test]
fn projector_tolerates_shared_storage_and_wallet_archive_profile() -> eyre::Result<()> {
    let scratch = tempdir()?;
    let custom_dir = scratch.path().join("projector-root");
    let shared_toleration = projector_command()
        .env_clear()
        .args([
            "--print-config",
            "--network",
            "zcash-regtest",
            "--lease-duration-seconds",
            "14400",
        ])
        .env("ZINDER_STORAGE__DATA_DIR", &custom_dir)
        .env("ZINDER_STORAGE__PATH", custom_dir.join("canonical"))
        .env("ZINDER_WALLET__SECONDARY_PATH", custom_dir.join("wsec"))
        .env("ZINDER_DEPLOYMENT__PROFILE", "wallet-archive")
        .output()?;
    assert!(shared_toleration.status.success(), "{shared_toleration:?}");
    let shared_stdout = String::from_utf8(shared_toleration.stdout)?;
    assert!(
        shared_stdout.contains("profile = \"wallet-archive\""),
        "{shared_stdout}"
    );
    assert!(
        shared_stdout.contains(&format!(
            "checkpoint_staging_root = \"{}\"",
            custom_dir.join("checkpoints").display()
        )),
        "{shared_stdout}"
    );
    Ok(())
}

fn run_print_config_with_env(extra_env: &[(&str, &str)]) -> eyre::Result<String> {
    let mut command = projector_command();
    command.env_clear();
    for (env_name, env_value) in extra_env {
        command.env(env_name, env_value);
    }
    let output = command
        .args([
            "--print-config",
            "--network",
            "zcash-regtest",
            "--build-owner-hex",
            "00112233445566778899aabbccddeeff",
            "--lease-duration-seconds",
            "14400",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}

#[test]
fn print_config_scales_build_defaults_from_container_budget_and_honors_overrides()
-> eyre::Result<()> {
    let rendered_default = run_print_config_with_env(&[])?;
    for (key, expected) in [
        (
            "max_outpoint_sort_memory_bytes",
            zinder_runtime::wallet_build_outpoint_sort_memory_bytes(),
        ),
        (
            "max_secondary_sort_memory_bytes_per_sorter",
            zinder_runtime::wallet_build_secondary_sort_memory_bytes_per_sorter(),
        ),
        (
            "max_accounted_reorg_undo_bytes",
            zinder_runtime::wallet_build_reorg_undo_memory_bytes(),
        ),
    ] {
        assert!(
            rendered_default.contains(&format!("{key} = {expected}")),
            "{rendered_default}"
        );
    }

    let overrides = [
        (
            "ZINDER_PROJECTOR__BUILD__MAX_OUTPOINT_SORT_MEMORY_BYTES",
            "123456789",
        ),
        (
            "ZINDER_PROJECTOR__BUILD__MAX_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER",
            "23456789",
        ),
        (
            "ZINDER_PROJECTOR__BUILD__MAX_ACCOUNTED_REORG_UNDO_BYTES",
            "3456789",
        ),
    ];
    let rendered_override = run_print_config_with_env(&overrides)?;
    for expected in [
        "max_outpoint_sort_memory_bytes = 123456789",
        "max_secondary_sort_memory_bytes_per_sorter = 23456789",
        "max_accounted_reorg_undo_bytes = 3456789",
    ] {
        assert!(rendered_override.contains(expected), "{rendered_override}");
    }
    Ok(())
}
