#![allow(
    missing_docs,
    reason = "Live test names describe the behavior under test."
)]

use std::process::Command;
use std::time::{Duration, Instant};

use eyre::{Result, eyre};
use zinder_core::Network;
use zinder_source::{
    MempoolSource, MempoolSourceBackend, MempoolSourceEventStream, SourceError,
    ZebraIndexerMempoolSource, ZebraIndexerMempoolSourceOptions, ZebraIndexerSourceTarget,
    ZebraJsonRpcSource, ZebraJsonRpcSourceOptions,
};
use zinder_testkit::live::{LiveTestEnv, init, require_live, require_live_for};

#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn zebra_indexer_mempool_source_opens_stream_against_running_indexer() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live()? else {
        return Ok(());
    };
    let Some(indexer_target) = ZebraIndexerSourceTarget::from_node_target(&env.target) else {
        return Ok(());
    };
    let mempool_source = build_indexer_mempool_source(&env, indexer_target)?;
    assert_eq!(
        mempool_source.capabilities().backend,
        MempoolSourceBackend::Streaming
    );

    let mut event_stream = mempool_source.events().await?;
    // The streaming task runs in the background. On an empty mempool there
    // will be no `Added` events to observe; what we are validating is that
    // the gRPC connection succeeded and the stream is alive (no error item)
    // for a brief window. A populated mempool would surface ADDED events
    // here.
    let outcome = tokio::time::timeout(
        Duration::from_secs(3),
        tokio_stream::StreamExt::next(&mut event_stream),
    )
    .await;
    match outcome {
        Ok(Some(Ok(_event))) => {} // Mempool had a transaction; that is fine.
        Ok(Some(Err(error))) => {
            return Err(eyre!(
                "indexer streaming source emitted error item: {error}"
            ));
        }
        Ok(None) => {
            return Err(eyre!(
                "indexer streaming source closed unexpectedly while the upstream node is idle"
            ));
        }
        Err(_elapsed) => {} // Empty mempool, no events; stream is alive.
    }
    Ok(())
}

fn build_indexer_mempool_source(
    env: &LiveTestEnv,
    indexer_target: ZebraIndexerSourceTarget,
) -> Result<ZebraIndexerMempoolSource> {
    let hydration_json_rpc = ZebraJsonRpcSource::with_options(
        env.target.network,
        &env.target.json_rpc_addr,
        env.target.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: env.target.request_timeout,
            max_response_bytes: env.target.max_response_bytes,
            broadcast_timeout: None,
        },
    )?;
    Ok(ZebraIndexerMempoolSource::with_options(
        indexer_target,
        hydration_json_rpc,
        ZebraIndexerMempoolSourceOptions::default(),
    ))
}

/// Validates the streaming source's reconnect contract: when Zebra restarts
/// mid-stream, the source must surface a retryable [`SourceError`] (or a
/// clean stream end) rather than panicking.
///
/// A fresh `events()` call after Zebra recovers must succeed.
///
/// **Destructive**: this test restarts the Zebra container named by
/// `ZINDER_TEST_INDEXER_CONTAINER_NAME` (default `z3_regtest_sidecar_zebra`).
/// The `node-mutating` nextest group only serializes it against other group
/// members, so CI runs it in its own invocation after the rest of the live suite.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn streaming_source_recovers_after_zebra_indexer_restart() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[Network::ZcashRegtest])? else {
        return Ok(());
    };
    let Some(indexer_target) = ZebraIndexerSourceTarget::from_node_target(&env.target) else {
        return Ok(());
    };
    let container_name = std::env::var("ZINDER_TEST_INDEXER_CONTAINER_NAME")
        .unwrap_or_else(|_| "z3_regtest_sidecar_zebra".to_owned());

    let mempool_source = build_indexer_mempool_source(&env, indexer_target)?;
    let mut event_stream = mempool_source.events().await?;
    assert_stream_alive_for(&mut event_stream, Duration::from_secs(2)).await?;

    restart_container(&container_name)?;
    observe_terminal_disconnect(&mut event_stream, Duration::from_secs(30)).await?;

    wait_for_container_healthy(&container_name, Duration::from_mins(1))?;
    let mut reopened_stream = open_with_retry(&mempool_source, Duration::from_secs(30)).await?;
    assert_stream_alive_for(&mut reopened_stream, Duration::from_secs(2)).await?;
    Ok(())
}

async fn assert_stream_alive_for(
    event_stream: &mut MempoolSourceEventStream,
    duration: Duration,
) -> Result<()> {
    let outcome = tokio::time::timeout(duration, tokio_stream::StreamExt::next(event_stream)).await;
    match outcome {
        Ok(Some(Ok(_event))) => Ok(()), // Mempool produced an event; stream alive.
        Ok(Some(Err(stream_error))) => Err(eyre!(
            "indexer stream emitted error item early: {stream_error}"
        )),
        Ok(None) => Err(eyre!(
            "indexer stream closed before the test could observe it"
        )),
        Err(_elapsed) => Ok(()), // No events; stream is alive on an idle mempool.
    }
}

async fn observe_terminal_disconnect(
    event_stream: &mut MempoolSourceEventStream,
    overall_deadline: Duration,
) -> Result<()> {
    let started = Instant::now();
    while started.elapsed() < overall_deadline {
        let remaining = overall_deadline.saturating_sub(started.elapsed());
        let outcome =
            tokio::time::timeout(remaining, tokio_stream::StreamExt::next(event_stream)).await;
        match outcome {
            Ok(Some(Ok(_residual_event))) => {
                // Best-effort residual events from before the disconnect are
                // benign; keep waiting for the disconnect signal itself.
            }
            Ok(Some(Err(error @ SourceError::MempoolStreamUnavailable { .. }))) => {
                // The indexer mempool stream is inherently
                // StreamDisconnected — every variant of this error
                // routes the writer loop through reconnect. We assert
                // the classification rather than a now-deleted boolean
                // so the test pins the loop-recovery contract instead
                // of the wire shape.
                assert_eq!(
                    error.upstream_classification(),
                    zinder_source::SourceFailureClass::StreamDisconnected,
                );
                return Ok(());
            }
            #[allow(
                clippy::wildcard_enum_match_arm,
                reason = "SourceError is non-exhaustive; only the unavailable variant is expected here"
            )]
            Ok(Some(Err(other_error))) => {
                return Err(eyre!(
                    "indexer stream emitted unexpected error variant: {other_error}"
                ));
            }
            Ok(None) => return Ok(()), // Stream ended cleanly; equivalent for reconnect.
            Err(_elapsed) => {
                return Err(eyre!(
                    "indexer stream did not emit a disconnect signal within {overall_deadline:?}"
                ));
            }
        }
    }
    Err(eyre!(
        "indexer stream did not emit a disconnect signal within {overall_deadline:?}"
    ))
}

fn restart_container(container_name: &str) -> Result<()> {
    let outcome = Command::new("docker")
        .args(["restart", container_name])
        .output()
        .map_err(|error| eyre!("invoking docker restart {container_name} failed: {error}"))?;
    if !outcome.status.success() {
        return Err(eyre!(
            "docker restart {container_name} exited {:?}; stderr: {}",
            outcome.status.code(),
            String::from_utf8_lossy(&outcome.stderr)
        ));
    }
    Ok(())
}

fn wait_for_container_healthy(container_name: &str, deadline: Duration) -> Result<()> {
    let started = Instant::now();
    loop {
        let outcome = Command::new("docker")
            .args([
                "inspect",
                "--format",
                "{{.State.Health.Status}}",
                container_name,
            ])
            .output()
            .map_err(|error| eyre!("docker inspect {container_name} failed: {error}"))?;
        if outcome.status.success() {
            let health = String::from_utf8_lossy(&outcome.stdout).trim().to_owned();
            if health == "healthy" {
                return Ok(());
            }
        }
        if started.elapsed() >= deadline {
            return Err(eyre!(
                "container {container_name} did not become healthy within {deadline:?}"
            ));
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

async fn open_with_retry(
    mempool_source: &ZebraIndexerMempoolSource,
    deadline: Duration,
) -> Result<MempoolSourceEventStream> {
    let started = Instant::now();
    loop {
        match mempool_source.events().await {
            Ok(stream) => return Ok(stream),
            Err(error) => {
                if started.elapsed() >= deadline {
                    return Err(eyre!(
                        "reopening indexer events stream failed within {deadline:?}: {error}"
                    ));
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}
