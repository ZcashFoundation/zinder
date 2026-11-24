//! A supervised runtime's shutdown against a real gRPC server on loopback.

use std::{process::Stdio, time::Duration};

use eyre::{Result, ensure, eyre};
use tokio::{
    io::{AsyncBufReadExt as _, BufReader},
    net::TcpListener,
    sync::mpsc,
};
use tokio_stream::{StreamExt as _, wrappers::ReceiverStream};
use tonic::{Code, Request, Status, transport::Endpoint};
use tonic_health::pb::{
    HealthCheckRequest, health_check_response::ServingStatus, health_client::HealthClient,
};
use tonic_reflection::pb::v1::{
    ServerReflectionRequest, ServerReflectionResponse,
    server_reflection_client::ServerReflectionClient, server_reflection_request::MessageRequest,
};
use tonic_types::StatusExt as _;
use zinder_runtime::{
    ClientAddressSource, Readiness, ReadinessCause, ReadinessState, RuntimeService,
    SupervisedRuntime, SupervisedTasks,
};
use zinder_testkit::{next_watch_status, watch_ends};

/// Bound on each wait in these tests; every expected outcome arrives well within it.
const WAIT_LIMIT: Duration = Duration::from_secs(5);
const SHORT_DRAIN_TIMEOUT: Duration = Duration::from_millis(300);

/// Selects the child process of
/// [`an_open_call_receives_its_status_before_the_process_exits`].
const SUPERVISED_CHILD_ENV: &str = "ZINDER_TEST_SUPERVISED_REFLECTION_CHILD";
const SUPERVISED_CHILD_TEST: &str =
    "integration::runtime_supervision::supervised_reflection_server_child";
/// The line the child prints, followed by its listen address, once it serves.
const CHILD_LISTENING: &str = "supervised reflection server listening on ";

fn list_services() -> ServerReflectionRequest {
    ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    }
}

async fn within_wait_limit<T>(future: impl Future<Output = T>) -> Result<T> {
    tokio::time::timeout(WAIT_LIMIT, future)
        .await
        .map_err(|_| eyre!("no outcome within {WAIT_LIMIT:?}"))
}

/// Serves `runtime`'s wallet listener on `listener`, with a stand-in for the
/// wallet service, until the runtime starts stopping.
fn serve_wallet_listener(
    runtime: &SupervisedRuntime,
    readiness: &Readiness,
    listener: TcpListener,
) -> Result<impl Future<Output = Result<(), std::io::Error>> + use<>> {
    let stand_in_wallet_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(zinder_proto::ZINDER_V1_FILE_DESCRIPTOR_SET)
        .build_v1alpha()?;
    let router = runtime.public_grpc_router(
        stand_in_wallet_service,
        zinder_proto::ZINDER_V1_FILE_DESCRIPTOR_SET,
        ClientAddressSource::Connection,
        readiness,
    )?;
    Ok(router.serve_with_incoming_shutdown(listener, runtime.grpc_server_shutdown()))
}

fn ready() -> Readiness {
    Readiness::new(ReadinessState::ready(Some(1)))
}

/// Checks that a call ended with the status a stopping runtime ends its open
/// calls with.
fn ensure_ended_by_shutdown(
    ended_with: Option<Result<ServerReflectionResponse, Status>>,
) -> Result<()> {
    let status = match ended_with {
        Some(Err(status)) => status,
        other => return Err(eyre!("expected the call to fail, got {other:?}")),
    };
    ensure!(status.code() == Code::Unavailable, "{status:?}");
    let details = status.get_error_details();
    let error_info = details
        .error_info()
        .ok_or_else(|| eyre!("the status carries no ErrorInfo: {status:?}"))?;
    ensure!(error_info.reason == "SERVICE_NOT_READY", "{status:?}");
    ensure!(
        error_info
            .metadata
            .get("readiness_cause")
            .map(String::as_str)
            == Some("shutting_down"),
        "{status:?}"
    );
    Ok(())
}

/// Opens a reflection stream on `address` and waits for its first answer.
async fn open_reflection_stream(
    address: &str,
) -> Result<(
    mpsc::Sender<ServerReflectionRequest>,
    tonic::Streaming<ServerReflectionResponse>,
)> {
    let channel = Endpoint::new(format!("http://{address}"))?
        .connect()
        .await?;
    let (sender, receiver) = mpsc::channel(4);
    let mut reflection = ServerReflectionClient::new(channel)
        .server_reflection_info(ReceiverStream::new(receiver))
        .await?
        .into_inner();
    sender.send(list_services()).await?;
    within_wait_limit(reflection.next())
        .await?
        .ok_or_else(|| eyre!("reflection ended before answering"))??;
    Ok((sender, reflection))
}

/// A call still open at the drain deadline ends before the serving
/// generations are released, and answers nothing afterwards.
///
/// A reflection stream stays open through a graceful drain, so the drain
/// deadline passes with it open.
#[tokio::test]
async fn a_call_open_at_the_drain_deadline_ends_before_serving_generations_are_released()
-> Result<()> {
    let runtime = SupervisedRuntime::new(RuntimeService::CompatLightwalletd)
        .with_grpc_drain_timeout(SHORT_DRAIN_TIMEOUT);
    let serving_drained = runtime.serving_drained();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = serve_wallet_listener(&runtime, &ready(), listener)?;
    let shutdown = runtime.shutdown_request();
    let supervised = tokio::spawn(runtime.supervise(server, SupervisedTasks::new(), || {}));
    let (sender, mut reflection) = open_reflection_stream(&address.to_string()).await?;

    shutdown.request();
    within_wait_limit(serving_drained.cancelled()).await?;

    ensure_ended_by_shutdown(within_wait_limit(reflection.next()).await?)?;
    // The stream has ended, so a request sent now gets no answer.
    let _ = sender.send(list_services()).await;
    assert!(within_wait_limit(reflection.next()).await?.is_none());

    within_wait_limit(supervised).await???;
    Ok(())
}

/// Health answers on a supervised listener, and a `Watch` open at shutdown
/// ends cleanly as the drain starts.
///
/// The stream reports `NOT_SERVING` and then ends with an `OK` status. It
/// does not hold the drain open until the deadline ends it as `UNAVAILABLE`.
#[tokio::test]
async fn health_answers_on_a_supervised_listener_and_its_watch_ends_at_shutdown() -> Result<()> {
    let readiness = ready();
    let runtime = SupervisedRuntime::new(RuntimeService::Query);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = serve_wallet_listener(&runtime, &readiness, listener)?;
    let shutdown = runtime.shutdown_request();
    let supervised = tokio::spawn({
        let readiness = readiness.clone();
        runtime.supervise(server, SupervisedTasks::new(), move || {
            readiness.set(ReadinessState::not_ready(ReadinessCause::ShuttingDown));
        })
    });
    let channel = Endpoint::new(format!("http://{address}"))?
        .connect()
        .await?;
    let mut health = HealthClient::new(channel);
    let listener_health = || {
        Request::new(HealthCheckRequest {
            service: String::new(),
        })
    };

    let checked = health.check(listener_health()).await?.into_inner();
    ensure!(
        checked.status == i32::from(ServingStatus::Serving),
        "{checked:?}"
    );
    let mut watch = health.watch(listener_health()).await?.into_inner();
    ensure!(next_watch_status(&mut watch).await? == ServingStatus::Serving);

    shutdown.request();

    ensure!(next_watch_status(&mut watch).await? == ServingStatus::NotServing);
    watch_ends(&mut watch).await?;
    within_wait_limit(supervised).await???;
    Ok(())
}

/// A call open at the drain deadline receives its status before the process
/// exits.
///
/// A service binary's process exits as soon as its runtime returns, which
/// ends every task the runtime still runs, so the status must be sent first.
/// A client would otherwise see a broken connection.
#[cfg(unix)]
#[tokio::test]
async fn an_open_call_receives_its_status_before_the_process_exits() -> Result<()> {
    let mut child = tokio::process::Command::new(std::env::current_exe()?)
        .args(["--exact", SUPERVISED_CHILD_TEST, "--nocapture"])
        .env(SUPERVISED_CHILD_ENV, "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| eyre!("the child's stdout is not piped"))?;
    let mut lines = BufReader::new(stdout).lines();
    let address = within_wait_limit(async {
        while let Some(line) = lines.next_line().await? {
            if let Some(address) = line.strip_prefix(CHILD_LISTENING) {
                return Ok(address.to_owned());
            }
        }
        Err(eyre!("the child exited before it listened"))
    })
    .await??;
    let (_sender, mut reflection) = open_reflection_stream(&address).await?;

    let child_id = child
        .id()
        .ok_or_else(|| eyre!("the child exited before it was signalled"))?;
    let signalled = tokio::process::Command::new("kill")
        .args(["-TERM", &child_id.to_string()])
        .status()
        .await?;
    ensure!(signalled.success(), "kill -TERM failed: {signalled}");

    ensure_ended_by_shutdown(within_wait_limit(reflection.next()).await?)?;
    let exit = within_wait_limit(child.wait()).await??;
    ensure!(exit.success(), "the child exited with {exit}");
    Ok(())
}

/// The child of [`an_open_call_receives_its_status_before_the_process_exits`].
///
/// It serves gRPC reflection under a supervised runtime until `SIGTERM`, then
/// drops its Tokio runtime and exits the process, as a service binary's
/// `main` does. Run by the test runner directly, it does nothing.
#[test]
fn supervised_reflection_server_child() -> Result<()> {
    if std::env::var_os(SUPERVISED_CHILD_ENV).is_none() {
        return Ok(());
    }
    let exit_code = {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        match runtime.block_on(serve_reflection_until_terminated()) {
            Ok(()) => 0,
            Err(_) => 1,
        }
    };
    std::process::exit(exit_code)
}

async fn serve_reflection_until_terminated() -> Result<()> {
    let runtime = SupervisedRuntime::new(RuntimeService::CompatLightwalletd)
        .with_grpc_drain_timeout(SHORT_DRAIN_TIMEOUT);
    let _signal = runtime.request_shutdown_on_terminating_signal();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = serve_wallet_listener(&runtime, &ready(), listener)?;
    #[allow(
        clippy::print_stdout,
        reason = "the parent test reads the child's listen address from its stdout"
    )]
    {
        println!("{CHILD_LISTENING}{address}");
    }
    runtime
        .supervise(server, SupervisedTasks::new(), || {})
        .await?;
    Ok(())
}
