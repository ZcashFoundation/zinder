use std::{net::SocketAddr, sync::Arc};

use eyre::{OptionExt as _, Result};
use http_body_util::BodyExt;
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use tokio::net::TcpListener;
use zinder_runtime::{
    ComposedReadiness, FollowWaitReason, OpsServer, OpsServerError, Readiness, ReadinessCause,
    ReadinessInput, ReadinessState, RuntimeService, install_metrics_recorder_for_service,
    spawn_ops_endpoint,
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

async fn head_readyz(listen_addr: SocketAddr) -> Result<u16> {
    let client = Client::builder(TokioExecutor::new()).build_http::<String>();
    let request =
        hyper::Request::head(format!("http://{listen_addr}/readyz")).body(String::new())?;
    let response = client.request(request).await?;
    let status = response.status().as_u16();
    assert!(response.into_body().collect().await?.to_bytes().is_empty());
    Ok(status)
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

#[cfg(target_os = "linux")]
fn process_gauge(metrics: &str, name: &str) -> Result<f64> {
    let kind = format!("# TYPE {name} gauge");
    assert!(metrics.lines().any(|line| line == kind), "{kind}");
    let prefix = format!("{name} ");
    let sample = metrics
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .ok_or_else(|| eyre::eyre!("missing {name} sample"))?;
    Ok(sample.parse()?)
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
    assert_eq!(head_readyz(listen_addr).await?, status);
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
    assert_eq!(head_readyz(listen_addr).await?, status);
    assert_eq!(readyz_value["status"], "not_ready");
    assert_eq!(
        readyz_value["cause"]["node_unavailable"]["failure_class"],
        "node_unreachable"
    );

    readiness.set(ReadinessState::syncing(None, Some(7), Some(7)));
    let (status, readyz_value) = get_json(listen_addr, "/readyz").await?;
    assert_eq!(status, 503);
    assert_eq!(readyz_value["current_height"], 7);
    assert_eq!(readyz_value["target_height"], 7);
    assert_eq!(
        readyz_value["cause"]["syncing"],
        serde_json::json!({ "lag_blocks": null })
    );

    for (reason, expected) in [
        (
            FollowWaitReason::MempoolHydrationAtTip,
            "mempool_hydration_at_tip",
        ),
        (FollowWaitReason::CanonicalProgress, "canonical_progress"),
    ] {
        readiness.set(ReadinessState::follow_syncing(
            reason,
            None,
            Some(7),
            Some(7),
        ));
        let (status, readyz_value) = get_json(listen_addr, "/readyz").await?;
        assert_eq!(status, 503);
        assert_eq!(readyz_value["phase"], "following_tip");
        assert_eq!(readyz_value["cause"]["syncing"]["wait_reason"], expected);
    }

    let (status, metrics_text) = get_text(listen_addr, "/metrics").await?;
    assert_eq!(status, 200);
    #[cfg(target_os = "linux")]
    {
        let start = process_gauge(&metrics_text, "zinder_process_start_time_seconds")?;
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
        assert!(start > 0.0 && start <= now.as_secs_f64());
        let rss = process_gauge(&metrics_text, "zinder_process_rss_bytes")?;
        let anon = process_gauge(&metrics_text, "zinder_process_rss_anon_bytes")?;
        assert!(rss > 0.0 && (0.0..=rss).contains(&anon));
        let (_, next_scrape) = get_text(listen_addr, "/metrics").await?;
        assert_eq!(
            start.to_bits(),
            process_gauge(&next_scrape, "zinder_process_start_time_seconds")?.to_bits()
        );
    }
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

/// Without upkeep, the recorder keeps every histogram sample until the next
/// scrape, so an unscraped runtime's memory grows with its traffic.
#[cfg(target_os = "linux")]
#[test]
fn installing_the_recorder_starts_its_upkeep_thread() -> Result<()> {
    install_metrics_recorder_for_service(RuntimeService::Query, "0.0.0", "zcash-regtest")?;

    // A new thread names itself once it starts running, so its name can
    // appear a little after the install returns.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let mut thread_names = Vec::new();
        for task in std::fs::read_dir("/proc/self/task")? {
            // A thread that exits while the loop runs has no name left to read.
            if let Ok(name) = std::fs::read_to_string(task?.path().join("comm")) {
                thread_names.push(name.trim_end().to_owned());
            }
        }
        if thread_names.iter().any(|name| name == "metrics-upkeep") {
            return Ok(());
        }
        eyre::ensure!(
            std::time::Instant::now() < deadline,
            "no metrics-upkeep thread among {thread_names:?}"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[tokio::test]
async fn status_reports_only_public_fields_and_served_epoch() -> Result<()> {
    use zinder_core::{BlockHeight, ChainEpochId, Network};
    use zinder_runtime::{ActiveNodeEndpoint, NodeUnavailableDetail};
    use zinder_testkit::ChainFixture;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let listen_addr = listener.local_addr()?;
    drop(listener);
    let readiness = Readiness::default();
    let server = spawn_ops_endpoint(
        listen_addr,
        OpsServer {
            service_name: "zinder-query",
            service_version: "0.0.0",
            network_name: "zcash-regtest",
            advertised_capabilities: Arc::from(["private-capability"]),
        },
        readiness.clone(),
    )
    .await?;
    let (code, starting) = get_json(listen_addr, "/status").await?;
    let mut expected = serde_json::json!({
        "status": "not_ready",
        "service": "zinder-query",
        "version": "0.0.0",
        "git_commit": zinder_runtime::BUILD_GIT_COMMIT,
        "network": "zcash-regtest",
        "cause": "starting",
    });
    assert_eq!(code, 200);
    assert_eq!(starting, expected);

    let mut epoch = ChainFixture::new(Network::ZcashRegtest)
        .extend_blocks(7)
        .chain_epoch(ChainEpochId::new(3))
        .ok_or_eyre("fixture must include a chain epoch")?;
    epoch.settled_tip_height = BlockHeight::new(2);
    readiness.set_served_chain_epoch(&epoch);
    readiness.set_active_node(Some(ActiveNodeEndpoint { position: 1 }));
    readiness.set_materialized_view_identities(vec!["/var/lib/private-zinder/view".to_owned()]);
    readiness.set(ReadinessState::node_unavailable_with_detail(
        NodeUnavailableDetail::first_iteration(
            "node_unreachable",
            "http://test:private-token@192.0.2.5:8232 /var/lib/private-zinder/node.cookie",
        ),
        Some(99),
    ));

    let (code, body) = get_text(listen_addr, "/status").await?;
    assert_eq!(code, 200);
    for private in [
        "private-token",
        "://",
        "/var/lib",
        "192.0.2.5",
        "private-capability",
    ] {
        assert!(!body.contains(private));
    }
    let mut status: serde_json::Value = serde_json::from_str(&body)?;
    let age = status
        .as_object_mut()
        .and_then(|fields| fields.remove("served_epoch_age_seconds"))
        .and_then(|age| age.as_u64())
        .ok_or_eyre("status must include a numeric age")?;
    assert!(age > 0);
    expected["cause"] = serde_json::json!("node_unavailable");
    expected["served_visible_tip_height"] = serde_json::json!(epoch.visible_tip_height.value());
    expected["served_settled_tip_height"] = serde_json::json!(2);
    assert_eq!(status, expected);

    readiness.set(ReadinessState::ready(Some(
        epoch.visible_tip_height.value(),
    )));
    let (_, recovered) = get_json(listen_addr, "/status").await?;
    assert_eq!(recovered["status"], "ready");
    assert_eq!(recovered["cause"], "ready");
    server.shutdown().await?;
    Ok(())
}
