use std::{net::TcpListener as PortReservation, time::Duration};

use eyre::{Result, eyre};
use tokio::{net::TcpListener, time::timeout};
use tonic::Code;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_types::StatusExt as _;
use zinder_proto::v1::explorer::{
    MempoolSummaryRequest, ServerInfoRequest, explorer_query_client::ExplorerQueryClient,
    explorer_query_server::SERVICE_NAME,
};
use zinder_runtime::{
    AuthenticatedChannel, BearerTokenClientInterceptor, RuntimeSupervisionError, RuntimeTaskFailure,
};
use zinder_testkit::{PROBE_WAIT_LIMIT, PublicListenerProbe, next_watch_status, watch_ends};

use super::*;

#[tokio::test]
async fn an_early_exit_or_panic_names_the_task_and_drains_before_exit_one() -> Result<()> {
    for panics in [false, true] {
        let runtime = SupervisedRuntime::new(RuntimeService::Explorer);
        let composed = ComposedReadiness::new(Readiness::default(), &[]);
        composed.publish_admission(ReadinessState::ready(None));
        let readiness = composed.runtime_readiness();
        let shutdown = runtime.grpc_server_shutdown();
        let mut cause_at_drain = None;
        let server = async {
            shutdown.await;
            cause_at_drain = Some(readiness.report().cause);
            Ok(())
        };
        let until_stop = tokio::spawn(runtime.stopping().cancelled_owned());
        let tasks = if panics {
            #[allow(
                clippy::panic,
                reason = "inject a probe panic to exercise its JoinError"
            )]
            let probe = tokio::spawn(async { panic!("injected probe failure") });
            supervised_tasks(None, Some(probe), Some(until_stop))
        } else {
            supervised_tasks(None, Some(until_stop), Some(tokio::spawn(async {})))
        };
        let error = timeout(
            PROBE_WAIT_LIMIT,
            runtime.supervise(server, tasks, || composed.publish_shutting_down()),
        )
        .await?
        .err()
        .ok_or_else(|| eyre!("runtime exited successfully after losing a task"))?;
        assert_eq!(cause_at_drain, Some(ReadinessCause::ShuttingDown));
        if panics {
            assert!(
                matches!(&error, RuntimeSupervisionError::TaskFailed {
                task: "upstream-observation probe",
                source: RuntimeTaskFailure::Join(join)
            } if join.is_panic()),
                "{error}"
            );
        } else {
            assert!(
                matches!(
                    &error,
                    RuntimeSupervisionError::TaskStopped {
                        task: "materialized-view catch-up"
                    }
                ),
                "{error}"
            );
        }
        assert_eq!(emit_runtime_error(&error.into()), ExitCode::from(1));
    }
    Ok(())
}

async fn check_health_and_traffic(
    probe: &PublicListenerProbe,
    readiness: &Readiness,
    explorer: &mut ExplorerQueryClient<AuthenticatedChannel>,
) -> Result<()> {
    let mut unauthenticated = ExplorerQueryClient::new(probe.channel());
    assert_eq!(
        unauthenticated
            .server_info(ServerInfoRequest {})
            .await
            .err()
            .map(|status| status.code()),
        Some(Code::Unauthenticated)
    );
    let mut watch = probe.watch(SERVICE_NAME).await?;
    assert_eq!(probe.agreed_status().await?, ServingStatus::Serving);
    assert_eq!(next_watch_status(&mut watch).await?, ServingStatus::Serving);
    explorer.server_info(ServerInfoRequest {}).await?;

    readiness.set(ReadinessState::not_ready(
        ReadinessCause::StorageUnavailable,
    ));
    assert_eq!(probe.agreed_status().await?, ServingStatus::NotServing);
    assert_eq!(probe.check(SERVICE_NAME).await?, ServingStatus::NotServing);
    assert_eq!(
        next_watch_status(&mut watch).await?,
        ServingStatus::NotServing
    );
    assert_eq!(
        explorer
            .server_info(ServerInfoRequest {})
            .await
            .err()
            .map(|status| status.code()),
        Some(Code::Unavailable)
    );
    let reflected = probe.reflected_services().await?;
    assert!(reflected.iter().any(|name| name == SERVICE_NAME));
    assert!(reflected.iter().any(|name| name == "grpc.health.v1.Health"));

    readiness.set(ReadinessState::ready(None));
    assert_eq!(probe.agreed_status().await?, ServingStatus::Serving);
    assert_eq!(next_watch_status(&mut watch).await?, ServingStatus::Serving);
    Ok(())
}

#[tokio::test]
async fn explorer_health_follows_readyz_and_a_pending_federated_call_ends_at_the_drain_deadline()
-> Result<()> {
    const DRAIN_TIMEOUT: Duration = Duration::from_millis(100);
    let wallet = TcpListener::bind("127.0.0.1:0").await?;
    let grpc_port = PortReservation::bind("127.0.0.1:0")?;
    let ops_port = PortReservation::bind("127.0.0.1:0")?;
    let grpc = grpc_port.local_addr()?;
    let ops = ops_port.local_addr()?;
    let dir = tempfile::tempdir()?;
    let token_path = dir.path().join("explorer.token");
    std::fs::write(&token_path, "test-token")?;
    let config = config::load_explorer_config(
        None,
        ExplorerConfigOverrides {
            network: Some("zcash-regtest".to_owned()),
            storage_path: Some(dir.path().join("store")),
            secondary_path: Some(dir.path().join("secondary")),
            bearer_token_path: Some(token_path),
            listen_addr: Some(grpc),
            ops_listen_addr: Some(ops),
            wallet_query_endpoint: Some(format!("http://{}", wallet.local_addr()?)),
        },
    )?;
    let runtime =
        SupervisedRuntime::new(RuntimeService::Explorer).with_grpc_drain_timeout(DRAIN_TIMEOUT);
    let shutdown = runtime.shutdown_request();
    let readiness = Readiness::default();
    drop((grpc_port, ops_port));
    let server = tokio::spawn(serve_explorer(config, runtime, readiness.clone()));
    let probe = timeout(PROBE_WAIT_LIMIT, async {
        loop {
            if let Ok(probe) = PublicListenerProbe::connect(grpc, ops).await {
                break probe;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let token = "test-token".parse()?;
    let mut explorer = ExplorerQueryClient::with_interceptor(
        probe.channel(),
        BearerTokenClientInterceptor::new(Some(&token))?,
    );
    check_health_and_traffic(&probe, &readiness, &mut explorer).await?;
    let mut watch = probe.watch("").await?;
    assert_eq!(next_watch_status(&mut watch).await?, ServingStatus::Serving);
    let pending = tokio::spawn(async move {
        explorer
            .mempool_summary(MempoolSummaryRequest::default())
            .await
    });
    // Accept the federated dial but withhold a reply so this Explorer call spans the drain.
    let (_connection, _) = timeout(PROBE_WAIT_LIMIT, wallet.accept()).await??;
    let started = tokio::time::Instant::now();
    shutdown.request();
    assert_eq!(
        next_watch_status(&mut watch).await?,
        ServingStatus::NotServing
    );
    watch_ends(&mut watch).await?;
    let status = timeout(PROBE_WAIT_LIMIT, pending)
        .await??
        .err()
        .ok_or_else(|| eyre!("pending Explorer call succeeded during shutdown"))?;
    assert!(started.elapsed() >= DRAIN_TIMEOUT);
    assert_eq!(status.code(), Code::Unavailable);
    let details = status.get_error_details();
    let error = details
        .error_info()
        .ok_or_else(|| eyre!("missing ErrorInfo: {status}"))?;
    assert_eq!(error.reason, "SERVICE_NOT_READY");
    assert_eq!(
        error.metadata.get("readiness_cause").map(String::as_str),
        Some("shutting_down")
    );
    timeout(PROBE_WAIT_LIMIT, server).await???;
    assert_eq!(readiness.report().cause, ReadinessCause::ShuttingDown);
    Ok(())
}

#[tokio::test]
async fn shutdown_joins_an_upstream_observation_waiting_on_the_node() -> Result<()> {
    let node = TcpListener::bind("127.0.0.1:0").await?;
    let settings = ExplorerServerInfoSettings::default();
    let source = ZebraJsonRpcSource::new(
        settings.network,
        format!("http://{}", node.local_addr()?),
        zinder_source::NodeAuth::None,
        Duration::from_secs(60),
    )?;
    let cancel = CancellationToken::new();
    let observation = ExplorerQueryGrpcAdapter::new(settings).spawn_upstream_observation_probe(
        Arc::new(source),
        Duration::from_millis(1),
        cancel.clone(),
    );
    let (_unanswered, _) = timeout(PROBE_WAIT_LIMIT, node.accept()).await??;
    cancel.cancel();
    timeout(PROBE_WAIT_LIMIT, observation).await??;
    Ok(())
}

#[tokio::test]
async fn losing_the_operations_endpoint_stops_explorer() -> Result<()> {
    // Dropping the endpoint's executor fails its server task while Explorer is still running.
    let operations = tokio::task::spawn_blocking(|| -> Result<_> {
        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        Ok(executor.block_on(spawn_ops_endpoint_for(
            RuntimeService::Explorer,
            Some("127.0.0.1:0".parse()?),
            "test",
            "zcash-regtest",
            Readiness::default(),
            Arc::from([]),
        ))?)
    })
    .await??;
    let runtime = SupervisedRuntime::new(RuntimeService::Explorer);
    let stop = runtime.grpc_server_shutdown();
    let outcome = timeout(
        PROBE_WAIT_LIMIT,
        runtime.supervise(
            async {
                stop.await;
                Ok(())
            },
            supervised_tasks(operations, None, None),
            || {},
        ),
    )
    .await?;
    assert!(
        matches!(
            outcome,
            Err(RuntimeSupervisionError::TaskFailed {
                task: "operations endpoint",
                ..
            })
        ),
        "{outcome:?}"
    );
    Ok(())
}
