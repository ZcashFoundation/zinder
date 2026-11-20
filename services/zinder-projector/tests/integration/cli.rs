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

#[tokio::test]
async fn backup_presents_the_control_token_and_reuses_the_named_candidate() -> eyre::Result<()> {
    use std::str::FromStr as _;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status};
    use zinder_proto::v1::ingest::{
        CreateStateBundleCaptureRequest, CreateStateBundleCaptureResponse,
        projector_control_server::{ProjectorControl, ProjectorControlServer},
    };
    use zinder_runtime::{BearerToken, BearerTokenServerInterceptor};

    struct Owner;
    #[tonic::async_trait]
    impl ProjectorControl for Owner {
        async fn create_state_bundle_capture(
            &self,
            request: Request<CreateStateBundleCaptureRequest>,
        ) -> Result<Response<CreateStateBundleCaptureResponse>, Status> {
            let request = request.into_inner();
            if request.candidate_id != "wallet-backup" || !request.reuse_completed {
                return Err(Status::invalid_argument("unexpected capture request"));
            }
            Err(Status::failed_precondition("incomplete capture"))
        }
    }

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
        Owner,
        BearerTokenServerInterceptor::new(Some(BearerToken::from_str(TOKEN)?)),
    );
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(owner)
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let archive = directory.path().join("wallet-backup");
    let mut invocation = projector_command();
    invocation
        .arg("--config")
        .arg(config)
        .args(["backup", "--archive"])
        .arg(&archive);
    let output = tokio::task::spawn_blocking(move || invocation.output()).await??;
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
canonical_path = "{}"
canonical_secondary_path = "{}"
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

/// The projector reads the canonical writer's admitted schedule, so it
/// accepts no node setting, fallback endpoints included, from any source.
#[test]
fn node_settings_are_rejected() -> eyre::Result<()> {
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

    let environment = projector_command()
        .args(["--print-config", "--network", "zcash-regtest"])
        .env("ZINDER_NODE__JSON_RPC_ADDR", "http://127.0.0.1:18232")
        .output()?;
    assert!(!environment.status.success(), "{environment:?}");
    let stderr = String::from_utf8(environment.stderr)?;
    assert!(stderr.contains("unknown field `node`"), "{stderr}");

    let fallbacks = projector_command()
        .args(["--print-config", "--network", "zcash-regtest"])
        .env(
            "ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR",
            "http://127.0.0.1:28232",
        )
        .output()?;
    assert!(!fallbacks.status.success(), "{fallbacks:?}");
    let stderr = String::from_utf8(fallbacks.stderr)?;
    assert!(
        stderr.contains("unknown field `node_fallbacks`"),
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
