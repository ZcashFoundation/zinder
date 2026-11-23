//! Following and bootstrap that park on a refused transition and resume once
//! a reorg changes it, against a real canonical writer, secondary, and wallet
//! store.

use std::{
    net::SocketAddr,
    num::{NonZeroU32, NonZeroU64},
    path::{Path, PathBuf},
    str::FromStr as _,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use parking_lot::Mutex;
use tempfile::TempDir;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request, Response, Status};
use zinder_core::{
    BlockHash, BlockId, CanonicalBlockFactsDigestVersion, CanonicalBlockReplayFormatVersion,
    Network, TransactionComponentCounts, TransactionVersion, TransparentAddressScriptHash,
    TransparentInputFact, TransparentOutPoint, TransparentOutputFact, UnixTimestampMillis,
    encode_canonical_block_replay,
};
use zinder_proto::v1::ingest::{
    AcquireCanonicalProjectionBuildLeaseRequest, CanonicalEventPageRequest,
    CanonicalEventPageResponse, CanonicalProjectionBuildLeaseResponse,
    CanonicalWriterStatusRequest, CanonicalWriterStatusResponse,
    CreateCanonicalOwnerCheckpointRequest, CreateCanonicalOwnerCheckpointResponse,
    CreateStateBundleCaptureRequest, ReadmitCanonicalOwnerCheckpointRequest,
    ReleaseCanonicalProjectionBuildLeaseRequest, ReleaseCanonicalProjectionBuildLeaseResponse,
    RenewCanonicalProjectionBuildLeaseRequest,
    canonical_control_server::{CanonicalControl, CanonicalControlServer},
    projector_control_client::ProjectorControlClient,
};
use zinder_runtime::{
    AuthenticatedChannel, BearerToken, Readiness, ReadinessCause, ReadinessReport,
    ResolvedProjectorControl, connect_zinder_grpc, expected_writer_fence,
};
use zinder_store::{
    CanonicalBuildBlock, CanonicalEventFence, CanonicalEventHistoryRequest, CanonicalLiveAppend,
    CanonicalLiveReplacement, CanonicalReorgPolicy, CanonicalReplacementBlock,
    CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalSecondary, RocksDbCanonicalStore,
    RocksDbResourceBudget,
};
use zinder_wallet_projection::{WalletCanonicalSourceIdentity, WalletProjectionBuildOwner};
use zinder_wallet_rocksdb::{
    RocksDbWalletBuildOptions, RocksDbWalletBuildStore, RocksDbWalletError, RocksDbWalletStore,
    build_wallet_from_canonical,
};

use crate::{
    ProjectorControlTasks, acquire_following_retention_lease, bootstrap_resumed_wallet_following,
    canonical_writer_control::{CanonicalWriterControlClient, CanonicalWriterControlError},
    config::{ProjectorBuildConfig, ProjectorConfig, ProjectorError, ProjectorFollowConfig},
    run_continuous_wallet_following,
    tests::{baseline_writer, empty_canonical_block, writer_retained_event},
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const REORG_WINDOW_BLOCKS: u32 = 16;
/// Admits a transition over empty blocks and refuses one that creates
/// [`DENSE_BLOCK_OUTPUTS`] transparent outputs.
const TRANSITION_LIMIT_BYTES: u64 = 64 * 1024;
const DENSE_BLOCK_OUTPUTS: u32 = 1_024;
/// Short enough that every parked poll is due to renew the lease.
const LEASE_DURATION: Duration = Duration::from_mins(1);
const WAIT_TIMEOUT: Duration = Duration::from_mins(1);
const PROJECTOR_CONTROL_TOKEN: &str = "parked-following-test-token";
const CAPTURE_RETRIES: u32 = 8;
/// Long enough that each renewal extends the lease by a whole millisecond.
const CAPTURE_RETRY_INTERVAL: Duration = Duration::from_millis(10);

#[tokio::test]
async fn fresh_and_resumed_wallets_complete_startup_at_the_writer_fence() -> TestResult {
    let Some(listener) = bind_loopback().await? else {
        return Ok(());
    };
    let root = TempDir::new()?;
    let policy = CanonicalReorgPolicy::new(REORG_WINDOW_BLOCKS)?;
    let (mut writer, _) = baseline_writer(&root.path().join("canonical"), policy)?;
    writer.admit_network_upgrade_schedule(&zinder_testkit::sample_regtest_upgrade_activations())?;
    let server_cancel = CancellationToken::new();
    let endpoint = spawn_canonical_control(
        listener,
        RecordingCanonicalControl::new(writer),
        server_cancel.clone(),
    )?;
    let mut config = park_config(root.path(), &endpoint, "127.0.0.1:0".parse()?)?;
    config.lease_duration = Duration::from_hours(4);

    for resume in [false, true] {
        assert_eq!(config.wallet_path.exists(), resume);
        let capture = zinder_testkit::LogCapture::install_for_target("zinder::startup");
        let readiness = Readiness::default();
        let cancel = CancellationToken::new();
        let mut owner = Box::pin(crate::run_owned_projector(
            &config,
            &readiness,
            cancel.clone(),
        ));
        tokio::select! {
            outcome = &mut owner => return Err(format!("projector exited during startup: {outcome:?}").into()),
            report = wait_for_report(&readiness, "initial wallet catch-up", |state| state.is_ready) => {
                assert_eq!(report?.current_height, Some(1));
            }
        }
        cancel.cancel();
        owner.await?;

        let events = capture.events();
        let completed: Vec<_> = events
            .iter()
            .filter(|event| event.field("outcome") == Some("ok"))
            .filter_map(|event| event.field("phase"))
            .collect();
        assert_eq!(
            completed,
            [
                "open_storage",
                "admit_ingest_control",
                "recover_state",
                "start_api",
                "ready"
            ]
        );
    }
    server_cancel.cancel();
    Ok(())
}

#[tokio::test]
async fn startup_attributes_wallet_open_and_writer_fence_errors_to_their_own_phases() -> TestResult
{
    let Some(listener) = bind_loopback().await? else {
        return Ok(());
    };
    let root = TempDir::new()?;
    let policy = CanonicalReorgPolicy::new(REORG_WINDOW_BLOCKS)?;
    let (mut writer, _) = baseline_writer(&root.path().join("canonical"), policy)?;
    writer.admit_network_upgrade_schedule(&zinder_testkit::sample_regtest_upgrade_activations())?;
    let control = RecordingCanonicalControl::new(writer);
    let server_cancel = CancellationToken::new();
    let endpoint = spawn_canonical_control(listener, control.clone(), server_cancel.clone())?;
    let config = park_config(root.path(), &endpoint, "127.0.0.1:0".parse()?)?;
    std::fs::write(&config.wallet_path, b"not a wallet directory")?;

    let error = startup_failure(&config, "recover_state", "admit_ingest_control").await?;
    assert!(matches!(error, ProjectorError::WalletStore(_)));
    assert_eq!(control.calls.snapshot().writer_statuses, 0);

    std::fs::remove_file(&config.wallet_path)?;
    let _committing_writer = control
        .writer
        .lock()
        .take()
        .ok_or("missing fixture writer")?;
    let error = startup_failure(&config, "admit_ingest_control", "recover_state").await?;
    assert!(matches!(error, ProjectorError::CanonicalControl(
        CanonicalWriterControlError::Status(status)
    ) if status.code() == Code::Unavailable));
    assert_eq!(control.calls.snapshot().writer_statuses, 1);
    server_cancel.cancel();
    Ok(())
}

#[tokio::test]
async fn owner_control_bind_failure_aborts_resumed_wallet_recovery() -> TestResult {
    let Some(listener) = bind_loopback().await? else {
        return Ok(());
    };
    let control_addr = listener.local_addr()?;
    let root = TempDir::new()?;
    let ParkFixture {
        mut writer,
        secondary,
        wallet,
    } = ParkFixture::build(root.path())?;
    writer.admit_network_upgrade_schedule(&zinder_testkit::sample_regtest_upgrade_activations())?;
    let successor = writer.event_fence().chain_event_sequence();
    writer.prune_canonical_events_before(successor, UnixTimestampMillis::now())?;
    drop((wallet, secondary));

    let server_cancel = CancellationToken::new();
    let endpoint = spawn_canonical_control(
        listener,
        RecordingCanonicalControl::new(writer),
        server_cancel.clone(),
    )?;
    let config = park_config(root.path(), &endpoint, control_addr)?;
    let error = startup_failure(&config, "start_api", "recover_state").await?;
    assert!(
        matches!(error, ProjectorError::ProjectorControlBind { source, .. }
        if source.kind() == std::io::ErrorKind::AddrInUse)
    );
    server_cancel.cancel();
    Ok(())
}

async fn startup_failure(
    config: &ProjectorConfig,
    failed: &str,
    aborted: &str,
) -> TestResult<ProjectorError> {
    let capture = zinder_testkit::LogCapture::install_for_target("zinder::startup");
    let readiness = Readiness::default();
    let error = tokio::time::timeout(
        WAIT_TIMEOUT,
        Box::pin(crate::run_owned_projector(
            config,
            &readiness,
            CancellationToken::new(),
        )),
    )
    .await?
    .err()
    .ok_or("expected startup to fail")?;
    let reason = error.to_string();
    let events = capture.events();
    let failures: Vec<_> = events
        .iter()
        .filter(|event| event.field("outcome") == Some("failed"))
        .map(|event| (event.field("phase"), event.field("reason")))
        .collect();
    assert_eq!(failures, [(Some(failed), Some(reason.as_str()))]);
    assert!(capture.any(|event| event.field("phase") == Some(aborted)
        && event.field("outcome") == Some("aborted")
        && event.field("reason").is_none()));
    assert!(!readiness.report().is_ready);
    Ok(error)
}

#[tokio::test]
async fn wallet_build_shutdown_distinguishes_cancellation_from_a_refused_renewal() -> TestResult {
    for (refuse_renewal, release_error) in [
        (false, None),
        (false, Some(Code::Unavailable)),
        (false, Some(Code::InvalidArgument)),
        (true, Some(Code::Unavailable)),
    ] {
        let Some(listener) = bind_loopback().await? else {
            return Ok(());
        };
        let root = TempDir::new()?;
        let policy = CanonicalReorgPolicy::new(REORG_WINDOW_BLOCKS)?;
        let (mut writer, _) = baseline_writer(&root.path().join("canonical"), policy)?;
        writer
            .admit_network_upgrade_schedule(&zinder_testkit::sample_regtest_upgrade_activations())?;
        let cancel = CancellationToken::new();
        let mut control = RecordingCanonicalControl::new(writer);
        control.release_error = release_error;
        if refuse_renewal {
            control.refuse_renewal_during_shutdown = Some(cancel.clone());
        } else {
            control.shutdown_on_acquire = Some(cancel.clone());
        }
        let calls = control.calls.clone();
        let server_cancel = CancellationToken::new();
        let endpoint = spawn_canonical_control(listener, control, server_cancel.clone())?;
        let config = park_config(root.path(), &endpoint, "127.0.0.1:0".parse()?)?;
        let capture = zinder_testkit::LogCapture::install();
        let readiness = Readiness::default();
        let outcome = tokio::time::timeout(
            WAIT_TIMEOUT,
            Box::pin(crate::run_owned_projector(&config, &readiness, cancel)),
        )
        .await?;
        server_cancel.cancel();

        let counts = calls.snapshot();
        assert_eq!(counts.acquires, 1);
        assert_eq!(counts.releases, 1);
        RocksDbWalletBuildStore::open(
            &config.wallet_path,
            config.network,
            config.wallet_rocksdb_budget,
        )?
        .discard_unpublished(
            WalletProjectionBuildOwner::from_bytes([0x99; 16]),
            UnixTimestampMillis::now(),
        )?;
        assert!(!readiness.report().is_ready);
        assert_eq!(counts.renews, u32::from(refuse_renewal));
        assert_build_shutdown(&capture, outcome, refuse_renewal, release_error)?;
    }
    Ok(())
}

fn assert_build_shutdown(
    capture: &zinder_testkit::LogCapture,
    outcome: Result<(), ProjectorError>,
    refuse_renewal: bool,
    release_error: Option<Code>,
) -> TestResult {
    let events = capture.events();
    let recovery_exits: Vec<_> = events
        .iter()
        .filter(|event| {
            event.field("phase") == Some("recover_state")
                && event.field("phase_state") == Some("exit")
        })
        .collect();
    let [exit] = recovery_exits.as_slice() else {
        return Err(format!("expected one recovery exit, got {recovery_exits:?}").into());
    };
    if refuse_renewal || release_error == Some(Code::InvalidArgument) {
        let error = outcome.err().ok_or("a refused lease operation must fail")?;
        assert_eq!(exit.field("outcome"), Some("failed"));
        assert_eq!(exit.field("reason"), Some(error.to_string().as_str()));
        if refuse_renewal {
            assert!(matches!(
                error,
                ProjectorError::WalletStore(RocksDbWalletError::ProjectionBuildCancelled)
            ));
        } else {
            assert!(
                matches!(error, ProjectorError::CanonicalControl(CanonicalWriterControlError::Status(status))
                    if status.code() == Code::InvalidArgument)
            );
        }
    } else {
        outcome?;
        assert_eq!(exit.field("outcome"), Some("aborted"));
        assert_eq!(exit.field("reason"), None);
    }
    assert_eq!(
        capture.any(|event| event.target == "zinder::projector"
            && event.level == tracing::Level::WARN
            && event.field("expires_at_unix_millis").is_some()),
        !refuse_renewal && release_error == Some(Code::Unavailable)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_transition_parks_following_until_a_reorg_changes_it() -> TestResult {
    let Some(canonical_control_listener) = bind_loopback().await? else {
        return Ok(());
    };
    let Some(projector_control_addr) = free_loopback_addr().await? else {
        return Ok(());
    };
    let temporary = TempDir::new()?;
    let chain = ParkFixture::build(temporary.path())?;
    let wallet_source =
        WalletCanonicalSourceIdentity::from_ready_evidence(chain.wallet.ready_evidence());
    let control = RecordingCanonicalControl::new(chain.writer);
    let server_cancel = CancellationToken::new();
    let endpoint = spawn_canonical_control(
        canonical_control_listener,
        control.clone(),
        server_cancel.clone(),
    )?;

    let mut canonical_control =
        CanonicalWriterControlClient::connect(&endpoint, None, None).await?;
    let lease =
        acquire_following_retention_lease(&mut canonical_control, wallet_source, LEASE_DURATION)
            .await?;
    let config = park_config(temporary.path(), &endpoint, projector_control_addr)?;
    let readiness = Readiness::default();
    let cancel = CancellationToken::new();
    let owner_control = ProjectorControlTasks::start(&config.projector_control, &cancel).await?;

    let following = run_continuous_wallet_following(
        &config,
        &readiness,
        cancel.clone(),
        chain.secondary,
        canonical_control,
        chain.wallet,
        lease,
        owner_control,
    );
    tokio::pin!(following);
    let script = drive_park_and_resume(&control, &readiness, projector_control_addr, &cancel);
    tokio::pin!(script);
    tokio::select! {
        outcome = &mut following => {
            return Err(format!("wallet following ended while it should have stayed parked: {outcome:?}").into());
        }
        outcome = &mut script => outcome?,
    }
    following.await?;
    server_cancel.cancel();
    Ok(())
}

/// Drives the parked follower through the scenario, then cancels it.
async fn drive_park_and_resume(
    control: &RecordingCanonicalControl,
    readiness: &Readiness,
    projector_control_addr: SocketAddr,
    cancel: &CancellationToken,
) -> TestResult {
    let parked = wait_for_report(readiness, "the transition-limit park", |report| {
        matches!(
            report.cause,
            ReadinessCause::WalletTransitionLimitExceeded { .. }
        )
    })
    .await?;
    let ReadinessCause::WalletTransitionLimitExceeded {
        required_bytes,
        configured_bytes,
    } = parked.cause
    else {
        return Err("the park reported another cause".into());
    };
    assert!(!parked.is_ready);
    assert_eq!(configured_bytes, TRANSITION_LIMIT_BYTES);
    assert!(required_bytes > TRANSITION_LIMIT_BYTES);
    assert_eq!(parked.current_height, Some(1));
    assert_eq!(parked.target_height, Some(2));

    let mut owner_control = connect_projector_control(projector_control_addr).await?;
    let refused_capture = capture_state_bundle(&mut owner_control).await?;
    assert_eq!(refused_capture.code(), tonic::Code::FailedPrecondition);
    assert!(refused_capture.message().contains("parked"));

    // Captures retried far more often than the poll interval still let the
    // follower renew its lease and poll the writer after each refusal.
    let before_retries = control.calls.snapshot();
    for _ in 0..CAPTURE_RETRIES {
        tokio::time::sleep(CAPTURE_RETRY_INTERVAL).await;
        let refused_retry = capture_state_bundle(&mut owner_control).await?;
        assert_eq!(refused_retry.code(), tonic::Code::FailedPrecondition);
    }
    let after_retries = control.calls.snapshot();
    assert!(after_retries.renews >= before_retries.renews + CAPTURE_RETRIES - 1);
    assert!(after_retries.writer_statuses >= before_retries.writer_statuses + CAPTURE_RETRIES - 1);
    assert_eq!(after_retries.event_pages, before_retries.event_pages);

    // Nothing changes on the writer, so the parked follower keeps polling and
    // renewing its one lease without planning again.
    let before_idle = control.calls.snapshot();
    for _ in 0..8 {
        tokio::time::sleep(Duration::from_millis(125)).await;
        assert_eq!(readiness.report().cause, parked.cause);
    }
    let after_idle = control.calls.snapshot();
    assert_eq!(after_idle.acquires, 1);
    assert_eq!(after_idle.releases, 0);
    assert!(after_idle.renews > before_idle.renews);
    assert!(after_idle.writer_statuses > before_idle.writer_statuses);
    assert_eq!(after_idle.event_pages, before_idle.event_pages);

    // A block on top of the refused one moves the writer but leaves the same
    // transition first: the follower plans once, stays parked, and reports
    // the new target.
    control.append(empty_canonical_block(3, [0x22; 32], [0x23; 32])?)?;
    let still_parked = wait_for_report(readiness, "the park at the new target", |report| {
        report.target_height == Some(3)
    })
    .await?;
    assert_eq!(still_parked.cause, parked.cause);
    assert_eq!(still_parked.current_height, Some(1));
    let after_append = control.calls.snapshot();
    assert_eq!(after_append.event_pages, after_idle.event_pages + 1);
    assert_eq!(after_append.acquires, 1);
    assert_eq!(after_append.releases, 0);

    // A reorg that replaces the dense block changes the next transition, so
    // following resumes, applies it, and moves its lease as usual.
    control.replace_from_height_two(vec![
        empty_canonical_block(2, [0x11; 32], [0x32; 32])?,
        empty_canonical_block(3, [0x32; 32], [0x33; 32])?,
    ])?;
    let resumed = wait_for_report(
        readiness,
        "following to catch up after the reorg",
        |report| report.is_ready && report.current_height == Some(3),
    )
    .await?;
    assert_eq!(resumed.cause, ReadinessCause::Ready);
    let after_resume = control.calls.snapshot();
    assert_eq!(after_resume.acquires, 2);
    assert_eq!(after_resume.releases, 1);

    cancel.cancel();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_bootstrap_transition_parks_behind_the_owner_control_endpoint() -> TestResult {
    let Some(canonical_control_listener) = bind_loopback().await? else {
        return Ok(());
    };
    let Some(projector_control_addr) = free_loopback_addr().await? else {
        return Ok(());
    };
    let temporary = TempDir::new()?;
    let chain = ParkFixture::build(temporary.path())?;
    // Admission bootstraps when the writer retains only the wallet cursor's
    // successor, which here is the dense block the limit refuses.
    let first_retained_event_sequence =
        WalletCanonicalSourceIdentity::from_ready_evidence(chain.wallet.ready_evidence())
            .source_position()
            .event_sequence
            .checked_add(1)
            .ok_or("the wallet event sequence must advance")?;
    let control = RecordingCanonicalControl::new(chain.writer);
    let server_cancel = CancellationToken::new();
    let endpoint = spawn_canonical_control(
        canonical_control_listener,
        control.clone(),
        server_cancel.clone(),
    )?;

    let canonical_control = CanonicalWriterControlClient::connect(&endpoint, None, None).await?;
    let config = park_config(temporary.path(), &endpoint, projector_control_addr)?;
    let readiness = Readiness::default();
    let cancel = CancellationToken::new();

    let mut recovery_phase = Some(zinder_runtime::StartupPhase::RecoverState.start());
    let mut following = Box::pin(bootstrap_resumed_wallet_following(
        &config,
        &readiness,
        cancel.clone(),
        chain.secondary,
        canonical_control,
        chain.wallet,
        first_retained_event_sequence,
        &mut recovery_phase,
    ));
    let script =
        drive_bootstrap_park_and_resume(&control, &readiness, projector_control_addr, &cancel);
    tokio::pin!(script);
    tokio::select! {
        outcome = &mut following => {
            return Err(format!("the bootstrap ended before the scenario cancelled it: {outcome:?}").into());
        }
        outcome = &mut script => outcome?,
    }
    following.await?;
    server_cancel.cancel();
    Ok(())
}

/// Drives a parked bootstrap through a refused capture and a resuming reorg.
///
/// Continuous following must then answer captures on the same owner-control
/// connection.
async fn drive_bootstrap_park_and_resume(
    control: &RecordingCanonicalControl,
    readiness: &Readiness,
    projector_control_addr: SocketAddr,
    cancel: &CancellationToken,
) -> TestResult {
    let parked = wait_for_report(readiness, "the bootstrap park", |report| {
        matches!(
            report.cause,
            ReadinessCause::WalletTransitionLimitExceeded { .. }
        )
    })
    .await?;
    assert!(!parked.is_ready);
    assert_eq!(parked.current_height, Some(1));
    assert_eq!(parked.target_height, Some(2));

    let mut owner_control = connect_projector_control(projector_control_addr).await?;
    let refused_capture = capture_state_bundle(&mut owner_control).await?;
    assert_eq!(refused_capture.code(), tonic::Code::FailedPrecondition);
    assert!(refused_capture.message().contains("parked"));

    control.append(empty_canonical_block(3, [0x22; 32], [0x23; 32])?)?;
    let still_parked = wait_for_report(
        readiness,
        "the bootstrap park at the new target",
        |report| report.target_height == Some(3),
    )
    .await?;
    assert_eq!(still_parked.cause, parked.cause);
    control.replace_from_height_two(vec![
        empty_canonical_block(2, [0x11; 32], [0x32; 32])?,
        empty_canonical_block(3, [0x32; 32], [0x33; 32])?,
    ])?;
    let resumed = wait_for_report(
        readiness,
        "following to catch up after the reorg",
        |report| report.is_ready && report.current_height == Some(3),
    )
    .await?;
    assert_eq!(resumed.cause, ReadinessCause::Ready);
    let after_resume = control.calls.snapshot();
    assert_eq!(after_resume.acquires, 2);
    assert_eq!(after_resume.releases, 1);

    // Continuous following now owns the capture; the fixture has no staging
    // root, so the capture itself fails, but not as a parked refusal.
    let following_capture = capture_state_bundle(&mut owner_control).await?;
    assert_eq!(following_capture.code(), tonic::Code::FailedPrecondition);
    assert!(!following_capture.message().contains("parked"));

    cancel.cancel();
    Ok(())
}

async fn connect_projector_control(
    projector_control_addr: SocketAddr,
) -> TestResult<ProjectorControlClient<AuthenticatedChannel>> {
    let token = BearerToken::from_str(PROJECTOR_CONTROL_TOKEN)?;
    let channel =
        connect_zinder_grpc(&format!("http://{projector_control_addr}"), Some(&token)).await?;
    Ok(ProjectorControlClient::new(channel))
}

async fn capture_state_bundle(
    owner_control: &mut ProjectorControlClient<AuthenticatedChannel>,
) -> TestResult<Status> {
    match owner_control
        .create_state_bundle_capture(CreateStateBundleCaptureRequest {
            candidate_id: "parked-capture".to_owned(),
            reuse_completed: false,
        })
        .await
    {
        Ok(_) => Err("a parked projector captured a state bundle".into()),
        Err(status) => Ok(status),
    }
}

async fn wait_for_report(
    readiness: &Readiness,
    description: &str,
    accept: impl Fn(&ReadinessReport) -> bool,
) -> TestResult<ReadinessReport> {
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        let report = readiness.report();
        if accept(&report) {
            return Ok(report);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for {description}; readiness is {:?}",
                report.cause
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A wallet built at block one behind a writer that appended one dense block.
struct ParkFixture {
    writer: RocksDbCanonicalStore,
    secondary: RocksDbCanonicalSecondary,
    wallet: zinder_wallet_rocksdb::RocksDbWalletFollowingStore,
}

impl ParkFixture {
    fn build(root: &Path) -> TestResult<Self> {
        let canonical_path = root.join("canonical");
        let reorg_policy = CanonicalReorgPolicy::new(REORG_WINDOW_BLOCKS)?;
        let (writer, block_one_id) = baseline_writer(&canonical_path, reorg_policy)?;
        let wallet_path = root.join("wallet");
        drop(build_wallet_from_canonical(
            &writer,
            &wallet_path,
            RocksDbWalletBuildOptions {
                supported_reorg_depth: REORG_WINDOW_BLOCKS,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?);
        let wallet = RocksDbWalletStore::open_ready_for_following(
            &wallet_path,
            Network::ZcashRegtest,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let fence = writer.event_fence();
        let (writer, _) = writer.commit_live_append(
            CanonicalLiveAppend::new(
                fence,
                dense_canonical_block(2, [0x11; 32], [0x22; 32])?,
                Vec::new(),
                block_one_id,
                UnixTimestampMillis::new(2),
            ),
            &zinder_testkit::sample_regtest_upgrade_activations(),
        )?;
        let secondary = RocksDbCanonicalSecondary::open_ready(
            &canonical_path,
            root.join("canonical-secondary"),
            &zinder_testkit::sample_regtest_upgrade_activations(),
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            reorg_policy,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        Ok(Self {
            writer,
            secondary,
            wallet,
        })
    }
}

/// An otherwise empty block whose coinbase pays [`DENSE_BLOCK_OUTPUTS`]
/// distinct transparent addresses.
fn dense_canonical_block(
    height: u32,
    parent_hash: [u8; 32],
    block_hash: [u8; 32],
) -> Result<CanonicalBuildBlock, zinder_core::CommitmentTreeFrontiersError> {
    let mut block = empty_canonical_block(height, parent_hash, block_hash)?;
    let outputs: Vec<TransparentOutputFact> = (0..DENSE_BLOCK_OUTPUTS)
        .map(|output_index| {
            let mut address = [0xa0; 32];
            address[..4].copy_from_slice(&output_index.to_le_bytes());
            TransparentOutputFact::new(
                output_index,
                1_000,
                vec![0x76, 0xa9],
                TransparentAddressScriptHash::from_bytes(address),
            )
        })
        .collect();
    if let Some(coinbase) = block.facts.transactions.first_mut() {
        coinbase.public_facts.version = TransactionVersion::V4;
        coinbase.public_facts.unsupported_sections = Vec::new();
        coinbase.public_facts.counts = TransactionComponentCounts {
            transparent_input_count: 1,
            transparent_output_count: DENSE_BLOCK_OUTPUTS,
            ..TransactionComponentCounts::EMPTY
        };
        coinbase.transparent_inputs = vec![TransparentInputFact::new(
            0,
            TransparentOutPoint::COINBASE_SENTINEL,
        )];
        coinbase.transparent_outputs = outputs;
    }
    block.replay_envelope = encode_canonical_block_replay(
        &block.facts,
        CanonicalBlockReplayFormatVersion::V1,
        CanonicalBlockFactsDigestVersion::V1,
    );
    Ok(block)
}

fn park_config(
    root: &Path,
    canonical_control_endpoint: &str,
    projector_control_addr: SocketAddr,
) -> TestResult<ProjectorConfig> {
    Ok(ProjectorConfig {
        network: Network::ZcashRegtest,
        canonical_path: root.join("canonical"),
        canonical_secondary_path: root.join("canonical-secondary"),
        expected_canonical_raw_blob_retention: RawBlobRetention::Transactions,
        wallet_path: root.join("wallet"),
        canonical_rocksdb_budget: RocksDbResourceBudget::for_local_tests(),
        wallet_rocksdb_budget: RocksDbResourceBudget::for_local_tests(),
        reorg_window_blocks: REORG_WINDOW_BLOCKS,
        build_owner: [0x42; 16],
        lease_duration: LEASE_DURATION,
        fence_convergence_timeout: Duration::from_secs(5),
        build: ProjectorBuildConfig {
            max_outpoint_sort_memory_bytes: 16 * 1024 * 1024,
            max_secondary_sort_memory_bytes_per_sorter: 16 * 1024 * 1024,
            max_temporary_file_bytes_per_sorter: 256 * 1024 * 1024,
            sst_target_logical_bytes: 1024 * 1024,
            max_accounted_reorg_undo_bytes: 16 * 1024 * 1024,
            accumulator_hashing_workers: std::num::NonZeroU32::new(2)
                .ok_or("the test hashing worker count must be nonzero")?,
        },
        follow: ProjectorFollowConfig {
            max_transition_logical_bytes: NonZeroU64::new(TRANSITION_LIMIT_BYTES)
                .ok_or("the test transition limit must be nonzero")?,
        },
        ingest_control_addr: canonical_control_endpoint.to_owned(),
        ingest_control_bearer_token_path: None,
        ingest_control_bearer_token: None,
        projector_control: ResolvedProjectorControl {
            listen_addr: Some(projector_control_addr),
            bearer_token_path: Some(PathBuf::from("projector-control.token")),
            bearer_token: Some(BearerToken::from_str(PROJECTOR_CONTROL_TOKEN)?),
            checkpoint_staging_root: root.join("checkpoint-staging"),
        },
        ops_listen_addr: None,
        allow_public_bind: false,
    })
}

/// The projector brings no activation table of its own: it opens its
/// canonical secondary once the writer admits the store's first schedule,
/// and fails typed when the writer never does within the deadline.
#[tokio::test]
async fn the_canonical_secondary_waits_for_the_writers_first_schedule_admission() -> TestResult {
    let root = TempDir::new()?;
    let reorg_policy = CanonicalReorgPolicy::new(REORG_WINDOW_BLOCKS)?;
    let (mut writer, _) = baseline_writer(&root.path().join("canonical"), reorg_policy)?;
    let mut config = park_config(root.path(), "http://127.0.0.1:1", "127.0.0.1:1".parse()?)?;
    config.fence_convergence_timeout = Duration::from_millis(200);
    let cancel = CancellationToken::new();

    let capture = zinder_testkit::LogCapture::install_for_target("zinder::startup");
    let refused = Box::pin(crate::run_owned_projector(
        &config,
        &Readiness::default(),
        cancel.clone(),
    ))
    .await;
    assert!(
        capture.any(|event| event.field("phase") == Some("open_storage")
            && event.field("outcome") == Some("failed")
            && event.field("reason").is_some())
    );

    assert!(
        matches!(
            refused,
            Err(crate::config::ProjectorError::CanonicalStore(
                zinder_store::CanonicalStoreError::NetworkUpgradeScheduleNotAdmitted { .. }
            ))
        ),
        "a store without an admitted schedule must stay refused"
    );

    config.fence_convergence_timeout = Duration::from_secs(10);
    let admit = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        writer.admit_network_upgrade_schedule(&zinder_testkit::sample_regtest_upgrade_activations())
    };
    let (opened, admitted) = tokio::join!(
        crate::open_canonical_secondary(&config, reorg_policy, &cancel),
        admit
    );
    admitted?;
    let secondary = opened?.ok_or("the open must not report cancellation")?;
    assert_eq!(
        secondary.network_upgrade_activations(),
        Some(&zinder_testkit::sample_regtest_upgrade_activations())
    );
    Ok(())
}

async fn bind_loopback() -> Result<Option<tokio::net::TcpListener>, std::io::Error> {
    match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => Ok(Some(listener)),
        // Some sandboxed host runners deny every socket operation. The
        // unrestricted Linux CI profile exercises this boundary.
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => Ok(None),
        Err(error) => Err(error),
    }
}

/// Reserves and releases a loopback port for the projector's own listener.
async fn free_loopback_addr() -> Result<Option<SocketAddr>, std::io::Error> {
    let Some(listener) = bind_loopback().await? else {
        return Ok(None);
    };
    listener.local_addr().map(Some)
}

fn spawn_canonical_control(
    listener: tokio::net::TcpListener,
    control: RecordingCanonicalControl,
    cancel: CancellationToken,
) -> Result<String, std::io::Error> {
    let endpoint = format!("http://{}", listener.local_addr()?);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(CanonicalControlServer::new(control))
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                cancel.cancelled_owned(),
            )
            .await
    });
    Ok(endpoint)
}

/// Counts of the canonical-control calls the projector made.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CallCounts {
    acquires: u32,
    renews: u32,
    releases: u32,
    event_pages: u32,
    writer_statuses: u32,
}

#[derive(Default)]
struct RecordedCalls {
    acquires: AtomicU32,
    renews: AtomicU32,
    releases: AtomicU32,
    event_pages: AtomicU32,
    writer_statuses: AtomicU32,
    last_generation: AtomicU64,
}

impl RecordedCalls {
    fn snapshot(&self) -> CallCounts {
        CallCounts {
            acquires: self.acquires.load(Ordering::SeqCst),
            renews: self.renews.load(Ordering::SeqCst),
            releases: self.releases.load(Ordering::SeqCst),
            event_pages: self.event_pages.load(Ordering::SeqCst),
            writer_statuses: self.writer_statuses.load(Ordering::SeqCst),
        }
    }
}

/// Serves canonical control from one writer store, as ingest does, and
/// records the calls the projector makes.
#[derive(Clone)]
struct RecordingCanonicalControl {
    writer: Arc<Mutex<Option<RocksDbCanonicalStore>>>,
    calls: Arc<RecordedCalls>,
    shutdown_on_acquire: Option<CancellationToken>,
    refuse_renewal_during_shutdown: Option<CancellationToken>,
    release_error: Option<Code>,
}

impl RecordingCanonicalControl {
    fn new(writer: RocksDbCanonicalStore) -> Self {
        Self {
            writer: Arc::new(Mutex::new(Some(writer))),
            calls: Arc::new(RecordedCalls::default()),
            shutdown_on_acquire: None,
            refuse_renewal_during_shutdown: None,
            release_error: None,
        }
    }

    fn append(&self, block: CanonicalBuildBlock) -> TestResult {
        self.commit(|writer, fence| {
            let settled_tip = settled_block_one();
            Ok(writer
                .commit_live_append(
                    CanonicalLiveAppend::new(
                        fence,
                        block,
                        Vec::new(),
                        settled_tip,
                        UnixTimestampMillis::new(10),
                    ),
                    &zinder_testkit::sample_regtest_upgrade_activations(),
                )?
                .0)
        })
    }

    fn replace_from_height_two(&self, blocks: Vec<CanonicalBuildBlock>) -> TestResult {
        self.commit(|writer, fence| {
            let replacement = blocks
                .into_iter()
                .map(|block| CanonicalReplacementBlock::new(block, Vec::new()))
                .collect();
            Ok(writer
                .commit_live_replacement(
                    CanonicalLiveReplacement::new(fence, replacement, UnixTimestampMillis::new(20)),
                    &zinder_testkit::sample_regtest_upgrade_activations(),
                )?
                .0)
        })
    }

    fn commit(
        &self,
        commit: impl FnOnce(
            RocksDbCanonicalStore,
            CanonicalEventFence,
        ) -> TestResult<RocksDbCanonicalStore>,
    ) -> TestResult {
        let mut writer = self.writer.lock();
        let current = writer.take().ok_or("the fixture writer is missing")?;
        let fence = current.event_fence();
        *writer = Some(commit(current, fence)?);
        drop(writer);
        Ok(())
    }

    fn with_writer<T>(
        &self,
        read: impl FnOnce(&RocksDbCanonicalStore) -> Result<T, zinder_store::CanonicalStoreError>,
    ) -> Result<T, Status> {
        let writer = self.writer.lock();
        let outcome = writer
            .as_ref()
            .ok_or_else(|| Status::unavailable("the fixture writer is committing"))
            .and_then(|writer| read(writer).map_err(|error| Status::internal(error.to_string())));
        drop(writer);
        outcome
    }
}

fn settled_block_one() -> BlockId {
    BlockId::new(
        zinder_core::BlockHeight::new(1),
        BlockHash::from_bytes([0x11; 32]),
    )
}

#[tonic::async_trait]
impl CanonicalControl for RecordingCanonicalControl {
    async fn writer_status(
        &self,
        _request: Request<CanonicalWriterStatusRequest>,
    ) -> Result<Response<CanonicalWriterStatusResponse>, Status> {
        self.calls.writer_statuses.fetch_add(1, Ordering::SeqCst);
        self.with_writer(|writer| {
            Ok(CanonicalWriterStatusResponse {
                network_name: zinder_core::wire::encode_zinder_native_chain_name(
                    Network::ZcashRegtest,
                )
                .to_owned(),
                fence: Some(expected_writer_fence(writer.event_fence())),
                oldest_retained_event_sequence: writer.canonical_event_retention_floor()?,
                network_upgrade_schedule: writer.network_upgrade_schedule().map(|schedule| {
                    zinder_proto::wire::encode_canonical_network_upgrade_schedule(
                        schedule.activations(),
                        schedule.revision(),
                    )
                }),
                reorg_window_exceeded: None,
                tip_unconfirmed: false,
                canonical_store_damaged: None,
            })
        })
        .map(Response::new)
    }

    async fn event_page(
        &self,
        request: Request<CanonicalEventPageRequest>,
    ) -> Result<Response<CanonicalEventPageResponse>, Status> {
        self.calls.event_pages.fetch_add(1, Ordering::SeqCst);
        let request = request.into_inner();
        let max_events = NonZeroU32::new(request.max_events)
            .ok_or_else(|| Status::invalid_argument("the page limit must be nonzero"))?;
        self.with_writer(|writer| {
            Ok(CanonicalEventPageResponse {
                events: writer
                    .canonical_event_history(CanonicalEventHistoryRequest::new(
                        Some(request.from_cursor.as_slice()),
                        max_events,
                    ))?
                    .into_iter()
                    .map(writer_retained_event)
                    .collect(),
                writer_fence: Some(expected_writer_fence(writer.event_fence())),
                oldest_retained_event_sequence: writer.canonical_event_retention_floor()?,
            })
        })
        .map(Response::new)
    }

    async fn create_owner_checkpoint(
        &self,
        _request: Request<CreateCanonicalOwnerCheckpointRequest>,
    ) -> Result<Response<CreateCanonicalOwnerCheckpointResponse>, Status> {
        Err(Status::unimplemented("the fixture does not checkpoint"))
    }

    async fn readmit_owner_checkpoint(
        &self,
        _request: Request<ReadmitCanonicalOwnerCheckpointRequest>,
    ) -> Result<Response<CreateCanonicalOwnerCheckpointResponse>, Status> {
        Err(Status::unimplemented("the fixture does not checkpoint"))
    }

    async fn acquire_projection_build_lease(
        &self,
        request: Request<AcquireCanonicalProjectionBuildLeaseRequest>,
    ) -> Result<Response<CanonicalProjectionBuildLeaseResponse>, Status> {
        self.calls.acquires.fetch_add(1, Ordering::SeqCst);
        if let Some(cancel) = &self.shutdown_on_acquire {
            cancel.cancel();
        }
        let mut lease = request
            .into_inner()
            .lease
            .ok_or_else(|| Status::invalid_argument("the acquire request omitted its lease"))?;
        lease.generation = self.calls.last_generation.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(Response::new(CanonicalProjectionBuildLeaseResponse {
            lease: Some(lease),
        }))
    }

    async fn renew_projection_build_lease(
        &self,
        request: Request<RenewCanonicalProjectionBuildLeaseRequest>,
    ) -> Result<Response<CanonicalProjectionBuildLeaseResponse>, Status> {
        self.calls.renews.fetch_add(1, Ordering::SeqCst);
        if let Some(cancel) = &self.refuse_renewal_during_shutdown {
            cancel.cancel();
            return Err(Status::failed_precondition("build lease renewal refused"));
        }
        Ok(Response::new(CanonicalProjectionBuildLeaseResponse {
            lease: request.into_inner().lease,
        }))
    }

    async fn release_projection_build_lease(
        &self,
        _request: Request<ReleaseCanonicalProjectionBuildLeaseRequest>,
    ) -> Result<Response<ReleaseCanonicalProjectionBuildLeaseResponse>, Status> {
        self.calls.releases.fetch_add(1, Ordering::SeqCst);
        if let Some(code) = self.release_error {
            return Err(Status::new(code, "canonical lease release failed"));
        }
        Ok(Response::new(
            ReleaseCanonicalProjectionBuildLeaseResponse::default(),
        ))
    }
}
