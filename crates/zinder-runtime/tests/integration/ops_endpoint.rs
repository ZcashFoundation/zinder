use std::{net::SocketAddr, sync::Arc};

use eyre::Result;
use http_body_util::BodyExt;
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use tokio::net::TcpListener;
use zinder_runtime::{
    ComposedReadiness, OpsServer, OpsServerError, Readiness, ReadinessCause, ReadinessInput,
    ReadinessState, RuntimeService, install_metrics_recorder_for_service, spawn_ops_endpoint,
};

async fn get_json(listen_addr: SocketAddr, path: &str) -> Result<(u16, serde_json::Value)> {
    let client = Client::builder(TokioExecutor::new()).build_http::<String>();
    let response = client
        .get(format!("http://{listen_addr}{path}").parse()?)
        .await?;
    let status = response.status().as_u16();
    let body = response.into_body().collect().await?.to_bytes();
    Ok((status, serde_json::from_slice(&body)?))
}

async fn get_text(listen_addr: SocketAddr, path: &str) -> Result<(u16, String)> {
    let client = Client::builder(TokioExecutor::new()).build_http::<String>();
    let response = client
        .get(format!("http://{listen_addr}{path}").parse()?)
        .await?;
    let status = response.status().as_u16();
    let body = response.into_body().collect().await?.to_bytes();
    Ok((status, String::from_utf8(body.to_vec())?))
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "end-to-end test covers healthz, readyz, and metrics in a single fixture"
)]
async fn ops_endpoint_serves_health_readiness_and_metrics() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let listen_addr: SocketAddr = listener.local_addr()?;
    drop(listener);

    install_metrics_recorder_for_service(RuntimeService::Query, "0.0.0", "zcash-regtest")?;
    metrics::counter!("zinder_runtime_metrics_emitted_before_ops_spawn_total").increment(7);

    let readiness = Readiness::new(ReadinessState::ready(Some(7)));
    let server_handle = spawn_ops_endpoint(
        listen_addr,
        OpsServer {
            service_name: "zinder-test",
            service_version: "0.0.0",
            network_name: "zcash-regtest",
            advertised_capabilities: Arc::from([
                zinder_proto::capabilities::WALLET_READ_VISIBLE_TIP_BLOCK_V1,
            ]),
        },
        readiness.clone(),
    )
    .await?;

    let (status, healthz_value) = get_json(listen_addr, "/healthz").await?;
    assert_eq!(status, 200);
    assert_eq!(healthz_value["version"], "0.0.0");
    assert_eq!(
        healthz_value["git_commit"],
        zinder_runtime::BUILD_GIT_COMMIT
    );

    let (status, readyz_value) = get_json(listen_addr, "/readyz").await?;
    assert_eq!(status, 200);
    assert_eq!(readyz_value["status"], "ready");
    assert_eq!(readyz_value["cause"], "ready");
    assert_eq!(readyz_value["current_height"], 7);

    readiness.set(ReadinessState::node_unavailable_with_detail(
        zinder_runtime::NodeUnavailableDetail::first_iteration(
            "node_unreachable",
            "synthetic test outage",
        ),
        Some(7),
    ));
    let (status, readyz_value) = get_json(listen_addr, "/readyz").await?;
    assert_eq!(status, 503);
    assert_eq!(readyz_value["status"], "not_ready");
    assert_eq!(
        readyz_value["cause"]["node_unavailable"]["failure_class"],
        "node_unreachable"
    );

    let (status, metrics_text) = get_text(listen_addr, "/metrics").await?;
    assert_eq!(status, 200);
    assert!(
        metrics_text.contains("zinder_build_info{"),
        "{metrics_text}"
    );
    assert!(
        metrics_text.contains("service=\"zinder-test\""),
        "{metrics_text}"
    );
    assert!(metrics_text.contains("version=\"0.0.0\""), "{metrics_text}");
    assert!(
        metrics_text.contains(&format!(
            "git_commit=\"{}\"",
            zinder_runtime::BUILD_GIT_COMMIT
        )),
        "{metrics_text}"
    );
    assert!(
        metrics_text.contains("network=\"zcash-regtest\""),
        "{metrics_text}"
    );
    assert!(
        metrics_text.contains("zinder_readiness_state{"),
        "{metrics_text}"
    );
    assert!(
        metrics_text.contains("cause=\"node_unavailable\""),
        "{metrics_text}"
    );
    assert!(
        metrics_text.contains("zinder_readiness_sync_lag_blocks"),
        "{metrics_text}"
    );
    assert!(
        metrics_text.contains("zinder_readiness_replica_lag_chain_epochs"),
        "{metrics_text}"
    );
    assert!(
        metrics_text
            .lines()
            .any(|line| line == "zinder_runtime_metrics_emitted_before_ops_spawn_total 7"),
        "{metrics_text}"
    );

    server_handle.shutdown().await?;

    Ok(())
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one fixture covers the degraded /readyz body, its dependency metrics, and recovery"
)]
async fn readyz_admits_traffic_and_lists_degraded_dependencies() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let listen_addr: SocketAddr = listener.local_addr()?;
    drop(listener);

    install_metrics_recorder_for_service(RuntimeService::Query, "0.0.0", "zcash-regtest")?;
    let readiness = Readiness::default();
    let composed = ComposedReadiness::with_dependency_inputs(
        readiness.clone(),
        &[ReadinessInput::NodeSource, ReadinessInput::IngestControl],
    );
    composed.publish_admission(ReadinessState::ready(Some(7)));
    composed.publish_input(ReadinessInput::IngestControl, ReadinessCause::Ready);
    composed.publish_input(
        ReadinessInput::NodeSource,
        ReadinessCause::NodeUnavailable(zinder_runtime::NodeUnavailableDetail::first_iteration(
            "node_unreachable",
            "synthetic test outage",
        )),
    );
    readiness.set_served_epoch_committed_at(zinder_core::UnixTimestampMillis::new(
        zinder_core::UnixTimestampMillis::now()
            .value()
            .saturating_sub(120_000),
    ));
    let server_handle = spawn_ops_endpoint(
        listen_addr,
        OpsServer {
            service_name: "zinder-test",
            service_version: "0.0.0",
            network_name: "zcash-regtest",
            advertised_capabilities: Arc::from([]),
        },
        readiness.clone(),
    )
    .await?;

    let (status, readyz_value) = get_text(listen_addr, "/readyz").await?;
    assert_eq!(status, 200);
    let cause_offset = readyz_value
        .find("\"cause\":")
        .ok_or_else(|| eyre::eyre!("/readyz omitted its cause: {readyz_value}"))?;
    let degraded_offset = readyz_value
        .find("\"degraded\":")
        .ok_or_else(|| eyre::eyre!("/readyz omitted degraded: {readyz_value}"))?;
    assert!(
        cause_offset < degraded_offset,
        "the runtime's cause must precede its degraded dependencies: {readyz_value}"
    );
    assert_eq!(
        readyz_value.matches("\"cause\":").count(),
        1,
        "{readyz_value}"
    );
    let readyz_value: serde_json::Value = serde_json::from_str(&readyz_value)?;
    assert_eq!(readyz_value["status"], "ready");
    assert_eq!(readyz_value["cause"], "ready");
    assert!(
        readyz_value["served_epoch_age_seconds"]
            .as_u64()
            .is_some_and(|age| (120..=122).contains(&age)),
        "{readyz_value}"
    );
    assert_eq!(
        readyz_value["degraded"],
        serde_json::json!([{
            "input": "node_source",
            "dependency_cause": {"node_unavailable": {
                "failure_class": "node_unreachable",
                "last_reason": "synthetic test outage",
                "consecutive_failures": 1,
                "outage_seconds": 0,
            }},
        }])
    );

    let (_, metrics_text) = get_text(listen_addr, "/metrics").await?;
    let dependency_sample = |input: &str, cause: &str| {
        metrics_text
            .lines()
            .find(|line| {
                line.starts_with("zinder_readiness_dependency_state{")
                    && line.contains(&format!("input=\"{input}\""))
                    && line.contains(&format!("cause=\"{cause}\""))
            })
            .and_then(|line| line.rsplit(' ').next())
            .map(str::to_owned)
    };
    assert_eq!(
        dependency_sample("node_source", "node_unavailable").as_deref(),
        Some("1"),
        "{metrics_text}"
    );
    assert_eq!(
        dependency_sample("node_source", "ready").as_deref(),
        Some("0"),
        "{metrics_text}"
    );
    assert_eq!(
        dependency_sample("ingest_control", "ready").as_deref(),
        Some("1"),
        "{metrics_text}"
    );
    assert!(
        metrics_text
            .lines()
            .any(|line| line.starts_with("zinder_readiness_served_epoch_age_seconds{")),
        "{metrics_text}"
    );

    composed.publish_input(ReadinessInput::NodeSource, ReadinessCause::Ready);
    let (status, readyz_value) = get_json(listen_addr, "/readyz").await?;
    assert_eq!(status, 200);
    assert!(readyz_value.get("degraded").is_none(), "{readyz_value}");
    let (_, metrics_text) = get_text(listen_addr, "/metrics").await?;
    assert!(
        metrics_text.lines().any(|line| {
            line.starts_with("zinder_readiness_dependency_state{")
                && line.contains("input=\"node_source\"")
                && line.contains("cause=\"ready\"")
                && line.ends_with(" 1")
        }),
        "{metrics_text}"
    );

    server_handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn writer_halt_through_a_dependency_refuses_traffic_without_a_degraded_entry() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let listen_addr: SocketAddr = listener.local_addr()?;
    drop(listener);

    install_metrics_recorder_for_service(RuntimeService::Query, "0.0.0", "zcash-regtest")?;
    let readiness = Readiness::default();
    let composed = ComposedReadiness::with_dependency_inputs(
        readiness.clone(),
        &[ReadinessInput::NodeSource, ReadinessInput::IngestControl],
    );
    composed.publish_admission(ReadinessState::ready(Some(7)));
    composed.publish_input(ReadinessInput::NodeSource, ReadinessCause::Ready);
    composed.publish_input(
        ReadinessInput::IngestControl,
        ReadinessState::reorg_window_exceeded(1_001, 1_000, Some(1_000), Some(7)).cause,
    );
    let server_handle = spawn_ops_endpoint(
        listen_addr,
        OpsServer {
            service_name: "zinder-test",
            service_version: "0.0.0",
            network_name: "zcash-regtest",
            advertised_capabilities: Arc::from([]),
        },
        readiness.clone(),
    )
    .await?;

    let (status, readyz_value) = get_json(listen_addr, "/readyz").await?;
    assert_eq!(status, 503);
    assert!(
        readyz_value["cause"].get("reorg_window_exceeded").is_some(),
        "{readyz_value}"
    );
    assert!(readyz_value.get("degraded").is_none(), "{readyz_value}");

    // The dependency gauge reports the cause ingest control published, so the
    // degraded-dependency alert must exclude the halt the runtime's own cause
    // already reports.
    let (_, metrics_text) = get_text(listen_addr, "/metrics").await?;
    let sample = |metric: &str, labels: &[&str]| {
        metrics_text
            .lines()
            .find(|line| {
                line.starts_with(metric)
                    && labels
                        .iter()
                        .all(|label| line.contains(&format!("{label}\"")))
            })
            .and_then(|line| line.rsplit(' ').next())
            .map(str::to_owned)
    };
    assert_eq!(
        sample(
            "zinder_readiness_dependency_state{",
            &["input=\"ingest_control", "cause=\"reorg_window_exceeded"]
        )
        .as_deref(),
        Some("1"),
        "{metrics_text}"
    );
    assert_eq!(
        sample(
            "zinder_readiness_state{",
            &["cause=\"reorg_window_exceeded"]
        )
        .as_deref(),
        Some("1"),
        "{metrics_text}"
    );

    server_handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn spawn_ops_endpoint_fails_before_returning_when_port_is_occupied() -> Result<()> {
    let occupied_listener = TcpListener::bind("127.0.0.1:0").await?;
    let listen_addr = occupied_listener.local_addr()?;

    let spawn_result = spawn_ops_endpoint(
        listen_addr,
        OpsServer {
            service_name: "zinder-test",
            service_version: "0.0.0",
            network_name: "zcash-regtest",
            advertised_capabilities: Arc::from([]),
        },
        Readiness::default(),
    )
    .await;

    let Err(OpsServerError::Bind {
        listen_addr: failed_addr,
        ..
    }) = spawn_result
    else {
        return Err(eyre::eyre!(
            "occupied operational listen address must fail before a handle is returned"
        ));
    };
    assert_eq!(failed_addr, listen_addr);
    Ok(())
}
