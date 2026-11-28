//! Isolated integration test binary for construction progress HTTP ops endpoints.
//!
//! `spawn_ops_endpoint` installs a process-wide Prometheus recorder, so keeping
//! this test in its own integration binary prevents global-recorder collisions
//! with `canonical_construction` tests under `cargo test`.

#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

mod common;

mod integration {
    use std::{net::SocketAddr, sync::Arc, time::Duration};

    use tokio::net::TcpListener;
    use tokio_util::sync::CancellationToken;
    use zinder_ingest::{admit_canonical_store, follow_admitted_canonical_store};
    use zinder_runtime::{
        ComposedReadiness, IngestPhase, OpsServer, Readiness, spawn_ops_endpoint,
    };

    use crate::common::construction_fixture::{
        GatedConstructionSource, MultiBlockConstructionSource, gated_construction_writer_config,
        regtest_activations,
    };

    async fn spawn_ingest_ops_endpoint(
        readiness: Readiness,
    ) -> Result<(SocketAddr, zinder_runtime::OpsEndpointHandle), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let ops_addr = listener.local_addr()?;
        drop(listener);
        let handle = spawn_ops_endpoint(
            ops_addr,
            OpsServer {
                service_name: "zinder-ingest",
                service_version: "0.0.0",
                network_name: "zcash-regtest",
                advertised_capabilities: Arc::from([]),
            },
            readiness,
        )
        .await?;
        Ok((ops_addr, handle))
    }

    async fn fetch_readyz_json(
        client: &reqwest::Client,
        ops_addr: SocketAddr,
    ) -> Result<(u16, serde_json::Value), Box<dyn std::error::Error>> {
        let response = client
            .get(format!("http://{ops_addr}/readyz"))
            .send()
            .await?;
        let status = response.status().as_u16();
        let parsed_json: serde_json::Value = serde_json::from_str(&response.text().await?)?;
        Ok((status, parsed_json))
    }

    async fn fetch_metrics_text(
        client: &reqwest::Client,
        ops_addr: SocketAddr,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let response = client
            .get(format!("http://{ops_addr}/metrics"))
            .send()
            .await?;
        assert_eq!(response.status().as_u16(), 200);
        Ok(response.text().await?)
    }

    fn prometheus_gauge_sample(
        metrics_text: &str,
        name: &str,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let expected_type = format!("# TYPE {name} gauge");
        if !metrics_text.lines().any(|line| line == expected_type) {
            return Err(format!("missing gauge TYPE line for {name} in:\n{metrics_text}").into());
        }
        let unlabeled_prefix = format!("{name} ");
        let labeled_prefix = format!("{name}{{");
        let sample_text = metrics_text
            .lines()
            .find_map(|line| {
                line.strip_prefix(&unlabeled_prefix).or_else(|| {
                    line.starts_with(&labeled_prefix)
                        .then(|| line.rsplit(' ').next())
                        .flatten()
                })
            })
            .ok_or_else(|| format!("missing sample for gauge {name} in:\n{metrics_text}"))?;
        Ok(sample_text.parse()?)
    }

    async fn assert_construction_stage(
        client: &reqwest::Client,
        ops_addr: SocketAddr,
        expected_current: u32,
        expected_target: u32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let readyz_json = loop {
            let (status, body) = fetch_readyz_json(client, ops_addr).await?;
            if body["current_height"] == expected_current
                && body["target_height"] == expected_target
            {
                assert_eq!(status, 503);
                break body;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "timed out waiting for /readyz current_height={expected_current}, target_height={expected_target}; last={body}"
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let expected_lag = u64::from(expected_target.saturating_sub(expected_current));
        assert_eq!(readyz_json["status"], "not_ready");
        assert_eq!(readyz_json["phase"], "bulk_catchup");
        assert_eq!(
            readyz_json["cause"],
            serde_json::json!({ "syncing": { "lag_blocks": expected_lag } })
        );

        let metrics_text = fetch_metrics_text(client, ops_addr).await?;
        for metric_name in [
            "zinder_ingest_construction_loaded_height",
            "zinder_ingest_construction_target_height",
        ] {
            assert!(
                metrics_text
                    .lines()
                    .any(|line| line.starts_with(&format!("# HELP {metric_name} "))),
                "missing HELP for {metric_name} in:\n{metrics_text}"
            );
        }
        assert_eq!(
            prometheus_gauge_sample(&metrics_text, "zinder_ingest_construction_loaded_height")?,
            f64::from(expected_current)
        );
        assert_eq!(
            prometheus_gauge_sample(&metrics_text, "zinder_ingest_construction_target_height")?,
            f64::from(expected_target)
        );
        assert_eq!(
            prometheus_gauge_sample(&metrics_text, "zinder_readiness_sync_lag_blocks")?,
            f64::from(expected_current.abs_diff(expected_target))
        );
        Ok(())
    }

    async fn wait_for_ready_follow(
        client: &reqwest::Client,
        ops_addr: SocketAddr,
        expected_tip: u32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let (status, body) = fetch_readyz_json(client, ops_addr).await?;
            if status == 200
                && body["status"] == "ready"
                && body["phase"] == "following_tip"
                && body["current_height"] == expected_tip
                && body["target_height"] == expected_tip
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!("timed out waiting for following_tip readyz: {body}").into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fresh_construction_reports_live_progress_on_readyz_and_metrics()
    -> Result<(), Box<dyn std::error::Error>> {
        let activations = regtest_activations();
        let source =
            GatedConstructionSource::new(MultiBlockConstructionSource::build(10, &activations)?);
        let temporary = tempfile::TempDir::new()?;
        let config = gated_construction_writer_config(
            temporary.path().join("canonical"),
            Arc::clone(&activations),
            1,
        )?;
        let readiness = Readiness::default();
        let composed = ComposedReadiness::new(readiness.clone(), &[]);
        let (ops_addr, ops_handle) = spawn_ingest_ops_endpoint(readiness).await?;
        let client = reqwest::Client::new();

        let admission_task = tokio::spawn({
            let source = source.clone();
            let activations = Arc::clone(&activations);
            let config = config.clone();
            let composed = composed.clone();
            async move {
                admit_canonical_store(
                    &source,
                    activations,
                    &config,
                    &composed,
                    &CancellationToken::new(),
                )
                .await
            }
        });

        assert_construction_stage(&client, ops_addr, 0, 9).await?;
        source.allow_through(3);
        assert_construction_stage(&client, ops_addr, 3, 9).await?;
        source.allow_through(6);
        assert_construction_stage(&client, ops_addr, 6, 9).await?;
        source.allow_through(9);
        let store = admission_task.await??;
        assert_construction_stage(&client, ops_addr, 9, 9).await?;

        let follow_cancel = CancellationToken::new();
        let follow_task = tokio::spawn({
            let source = source.clone();
            let follow = config.follow.clone();
            let composed = composed.clone();
            let follow_cancel = follow_cancel.clone();
            async move {
                follow_admitted_canonical_store(
                    store,
                    &source,
                    follow,
                    &composed,
                    &follow_cancel,
                    None,
                )
                .await
            }
        });
        wait_for_ready_follow(&client, ops_addr, 10).await?;

        let metrics_text = fetch_metrics_text(&client, ops_addr).await?;
        assert_eq!(
            prometheus_gauge_sample(&metrics_text, "zinder_ingest_construction_loaded_height")?,
            9.0
        );
        assert_eq!(
            prometheus_gauge_sample(&metrics_text, "zinder_ingest_construction_target_height")?,
            9.0
        );
        assert_eq!(
            prometheus_gauge_sample(&metrics_text, "zinder_readiness_sync_lag_blocks")?,
            0.0
        );
        assert_eq!(
            composed.runtime_readiness().report().phase,
            Some(IngestPhase::FollowingTip)
        );

        follow_cancel.cancel();
        follow_task.await??;
        ops_handle.shutdown().await?;
        Ok(())
    }
}
