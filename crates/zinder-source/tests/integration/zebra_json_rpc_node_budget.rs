#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

//! A node's HTTP 429 refusals and the `node.max_concurrent_requests` call
//! budget, against a mock JSON-RPC server.

use std::{num::NonZeroU32, time::Duration};

use eyre::eyre;
use futures_util::future::join_all;
use metrics_exporter_prometheus::PrometheusBuilder;
use serde_json::json;
use zinder_core::{Network, RawTransactionBytes, TransactionId};
use zinder_source::{
    NodeAuth, NodeSource, SourceError, SourceFailureClass, TransactionBroadcaster,
    ZEBRA_REBUILD_THRESHOLD, ZebraJsonRpcSource, ZebraJsonRpcSourceOptions,
};
use zinder_testkit::{JsonRpcTestServer, RpcReply, method};

fn source_with_budget(
    server: &JsonRpcTestServer,
    max_concurrent_requests: u32,
) -> eyre::Result<ZebraJsonRpcSource> {
    let max_concurrent_requests = NonZeroU32::new(max_concurrent_requests)
        .ok_or_else(|| eyre!("max_concurrent_requests must be nonzero"))?;
    Ok(ZebraJsonRpcSource::with_options(
        Network::ZcashRegtest,
        server.url(),
        NodeAuth::None,
        ZebraJsonRpcSourceOptions {
            request_timeout: Duration::from_secs(5),
            max_concurrent_requests,
            ..ZebraJsonRpcSourceOptions::default()
        },
    )?)
}

fn tip_reply() -> RpcReply {
    let hash = [0x11_u8; 32];
    RpcReply::result(json!({ "height": 7, "hash": hash }))
}

fn assert_rate_limited(error: &SourceError, operation: &str) {
    assert!(
        matches!(error, SourceError::NodeRateLimited { operation: refused } if *refused == operation),
        "expected an HTTP 429 refusal of {operation}, got {error:?}"
    );
    assert_eq!(
        error.upstream_classification(),
        SourceFailureClass::NodeRateLimited
    );
    assert!(!zinder_source::is_transport_failure(error));
}

/// Counter lines in a Prometheus rendering whose name and labels match.
fn counter_value(rendered: &str, series_prefix: &str, labels: &[&str]) -> Option<u64> {
    rendered
        .lines()
        .filter(|line| line.starts_with(series_prefix))
        .find(|line| labels.iter().all(|label| line.contains(label)))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|count| count.parse().ok())
}

#[test]
fn node_rate_limited_counter_is_preregistered_at_zero_before_any_call() -> eyre::Result<()> {
    const EXPECTED_METHODS: [&str; 15] = [
        "batch_getblock",
        "getaddressbalance",
        "getaddresstxids",
        "getaddressutxos",
        "getbestblockheightandhash",
        "getblock",
        "getblockchaininfo",
        "getblockhash",
        "getrawmempool",
        "getrawtransaction",
        "getstandardfee",
        "rpc.discover",
        "sendrawtransaction",
        "z_getsubtreesbyindex",
        "z_gettreestate",
    ];
    let recorder = PrometheusBuilder::new().build_recorder();
    let recorded_metrics = recorder.handle();
    let _local_recorder = metrics::set_default_local_recorder(&recorder);
    let _source = ZebraJsonRpcSource::new(
        Network::ZcashRegtest,
        "http://127.0.0.1:18232",
        NodeAuth::None,
        Duration::from_secs(5),
    )?;

    let rendered = recorded_metrics.render();
    assert!(
        rendered.contains("# HELP zinder_node_rate_limited_total "),
        "missing # HELP for zinder_node_rate_limited_total in:\n{rendered}"
    );
    assert!(
        rendered.contains("# TYPE zinder_node_rate_limited_total counter"),
        "missing # TYPE for zinder_node_rate_limited_total in:\n{rendered}"
    );
    let series_count = rendered
        .lines()
        .filter(|line| line.starts_with("zinder_node_rate_limited_total{"))
        .count();
    assert_eq!(
        series_count,
        EXPECTED_METHODS.len(),
        "unexpected zinder_node_rate_limited_total series count in:\n{rendered}"
    );
    for method_name in EXPECTED_METHODS {
        let method_label = format!("method=\"{method_name}\"");
        assert_eq!(
            counter_value(
                &rendered,
                "zinder_node_rate_limited_total{",
                &["source=\"zebra_json_rpc\"", &method_label],
            ),
            Some(0),
            "expected zero counter for {method_name} in:\n{rendered}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn http_429_is_node_rate_limited_on_every_call_path() -> eyre::Result<()> {
    let server = JsonRpcTestServer::start([
        method("getbestblockheightandhash").reply(RpcReply::rate_limited()),
        method("getrawtransaction").reply(RpcReply::rate_limited()),
        method("getrawtransaction").reply(RpcReply::rate_limited()),
        method("sendrawtransaction").reply(RpcReply::rate_limited()),
    ])?;
    let source = source_with_budget(&server, 4)?;
    let transaction_id = TransactionId::from_bytes([0x21; 32]);

    let Err(tip_error) = source.tip_id().await else {
        return Err(eyre!("tip_id must fail on HTTP 429"));
    };
    assert_rate_limited(&tip_error, "getbestblockheightandhash");

    let Err(verbose_error) = source.fetch_mined_transaction_bytes(transaction_id).await else {
        return Err(eyre!("verbose getrawtransaction must fail on HTTP 429"));
    };
    assert_rate_limited(&verbose_error, "getrawtransaction");

    let Err(raw_error) = source.fetch_raw_transaction_bytes(transaction_id).await else {
        return Err(eyre!("raw getrawtransaction must fail on HTTP 429"));
    };
    assert_rate_limited(&raw_error, "getrawtransaction");

    let Err(broadcast_error) = source
        .broadcast_transaction(RawTransactionBytes::new([0x01, 0x02]))
        .await
    else {
        return Err(eyre!("sendrawtransaction must fail on HTTP 429"));
    };
    assert_rate_limited(&broadcast_error, "sendrawtransaction");
    Ok(())
}

#[tokio::test]
async fn http_429_is_counted_once_per_call_by_method() -> eyre::Result<()> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let recorded_metrics = recorder.handle();
    let _local_recorder = metrics::set_default_local_recorder(&recorder);
    let server = JsonRpcTestServer::start([
        method("getbestblockheightandhash").reply(RpcReply::rate_limited()),
        method("getbestblockheightandhash").reply(RpcReply::rate_limited()),
        method("getrawtransaction").reply(RpcReply::rate_limited()),
        method("sendrawtransaction").reply(RpcReply::rate_limited()),
    ])?;
    let source = source_with_budget(&server, 4)?;

    for _ in 0..2 {
        let _refused = source.tip_id().await;
    }
    let _refused = source
        .fetch_raw_transaction_bytes(TransactionId::from_bytes([0x22; 32]))
        .await;
    let _refused = source
        .broadcast_transaction(RawTransactionBytes::new([0x03]))
        .await;

    let rendered = recorded_metrics.render();
    for (method_name, expected) in [
        ("getbestblockheightandhash", 2),
        ("getrawtransaction", 1),
        ("sendrawtransaction", 1),
    ] {
        let method_label = format!("method=\"{method_name}\"");
        assert_eq!(
            counter_value(
                &rendered,
                "zinder_node_rate_limited_total{",
                &["source=\"zebra_json_rpc\"", &method_label],
            ),
            Some(expected),
            "zinder_node_rate_limited_total for {method_name} in:\n{rendered}"
        );
        assert_eq!(
            counter_value(
                &rendered,
                "zinder_node_request_total{",
                &[&method_label, "error_class=\"node_rate_limited\""],
            ),
            Some(expected),
            "zinder_node_request_total for {method_name} in:\n{rendered}"
        );
    }
    Ok(())
}

/// Repeated HTTP 429 refusals never rebuild the client.
///
/// A node refusing connections with HTTP 429 is up and its connection works,
/// and a rebuilt client opens new connections to a node that refuses them for
/// having too many. HTTP 503 on the same path does rebuild, which shows the
/// check would see a rebuild.
#[tokio::test]
async fn repeated_http_429_never_rebuilds_the_client() -> eyre::Result<()> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let recorded_metrics = recorder.handle();
    let _local_recorder = metrics::set_default_local_recorder(&recorder);
    let refusals = usize::try_from(ZEBRA_REBUILD_THRESHOLD.get())?.saturating_mul(3);
    let rate_limited_server = JsonRpcTestServer::start(
        (0..refusals).map(|_| method("getbestblockheightandhash").reply(RpcReply::rate_limited())),
    )?;
    let rate_limited_source = source_with_budget(&rate_limited_server, 4)?;

    for _ in 0..refusals {
        let Err(error) = rate_limited_source.tip_id().await else {
            return Err(eyre!("tip_id must fail on HTTP 429"));
        };
        assert_rate_limited(&error, "getbestblockheightandhash");
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    let rendered = recorded_metrics.render();
    assert!(
        !rendered.contains("zinder_transport_reconnect_total"),
        "HTTP 429 refusals must not rebuild the client:\n{rendered}"
    );

    let unavailable_server = JsonRpcTestServer::start(
        (0..refusals)
            .map(|_| method("getbestblockheightandhash").reply(RpcReply::http_status(503))),
    )?;
    let unavailable_source = source_with_budget(&unavailable_server, 4)?;
    for _ in 0..ZEBRA_REBUILD_THRESHOLD.get() {
        let _refused = unavailable_source.tip_id().await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        recorded_metrics
            .render()
            .contains("zinder_transport_reconnect_total"),
        "HTTP 503 refusals at the threshold must rebuild the client"
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_calls_never_exceed_max_concurrent_requests() -> eyre::Result<()> {
    const MAX_CONCURRENT_REQUESTS: u32 = 3;
    const CALLS: usize = 12;
    let server = JsonRpcTestServer::start([method("getbestblockheightandhash")
        .reply_with(|_params| RpcReply::delayed(Duration::from_millis(100), tip_reply()))])?;
    let source = source_with_budget(&server, MAX_CONCURRENT_REQUESTS)?;

    let outcomes = join_all((0..CALLS).map(|_| source.tip_id())).await;

    for outcome in outcomes {
        outcome?;
    }
    assert_eq!(
        server.requests_for("getbestblockheightandhash")?.len(),
        CALLS
    );
    let peak = server.peak_requests_in_flight();
    assert!(
        peak <= usize::try_from(MAX_CONCURRENT_REQUESTS)?,
        "the node saw {peak} requests at once, above node.max_concurrent_requests"
    );
    assert!(
        peak >= 2,
        "calls must still run concurrently, peak was {peak}"
    );
    Ok(())
}

/// Readiness probes use their own slot, so slow calls holding every shared
/// slot do not delay them.
#[tokio::test]
async fn health_probes_answer_while_every_shared_slot_is_held() -> eyre::Result<()> {
    const SLOW_CALL: Duration = Duration::from_secs(3);
    let server = JsonRpcTestServer::start([
        method("getrawtransaction")
            .reply_with(|_params| RpcReply::delayed(SLOW_CALL, RpcReply::result(json!("00")))),
        method("getbestblockheightandhash").reply_with(|_params| tip_reply()),
        method("getblockchaininfo").reply_with(|_params| {
            RpcReply::result(json!({
                "blocks": 7,
                "estimatedheight": 7,
                "verificationprogress": 1.0,
            }))
        }),
    ])?;
    let source = source_with_budget(&server, 1)?;
    let slow_source = source.clone();
    let slow_call = tokio::spawn(async move {
        slow_source
            .fetch_raw_transaction_bytes(TransactionId::from_bytes([0x23; 32]))
            .await
    });
    while server.requests_for("getrawtransaction")?.is_empty() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let probe_deadline = Duration::from_secs(1);
    let probed_tip = tokio::time::timeout(probe_deadline, source.probe_tip_id()).await??;
    let health = tokio::time::timeout(probe_deadline, source.poll_upstream_health()).await??;
    let queued_tip = tokio::time::timeout(Duration::from_millis(200), source.tip_id()).await;

    assert_eq!(probed_tip.height.value(), 7);
    assert!(health.ready_for_queries);
    assert!(
        queued_tip.is_err(),
        "an ordinary call must wait for the held shared slot"
    );
    slow_call.await??;
    Ok(())
}

/// Without `broadcast_timeout`, a broadcast that queued for a call slot still
/// ends within one `request_timeout`, counted from when it started waiting.
#[tokio::test]
async fn queued_broadcast_without_its_own_timeout_ends_within_the_request_timeout()
-> eyre::Result<()> {
    const REQUEST_TIMEOUT: Duration = Duration::from_millis(1_000);
    let server = JsonRpcTestServer::start([
        method("getrawtransaction").reply_with(|_params| {
            RpcReply::delayed(Duration::from_millis(600), RpcReply::result(json!("00")))
        }),
        method("sendrawtransaction").reply_with(|_params| {
            RpcReply::delayed(
                Duration::from_millis(900),
                RpcReply::result(json!("ab".repeat(32))),
            )
        }),
    ])?;
    let source = ZebraJsonRpcSource::with_options(
        Network::ZcashRegtest,
        server.url(),
        NodeAuth::None,
        ZebraJsonRpcSourceOptions {
            request_timeout: REQUEST_TIMEOUT,
            broadcast_timeout: None,
            max_concurrent_requests: NonZeroU32::MIN,
            ..ZebraJsonRpcSourceOptions::default()
        },
    )?;
    let holder = source.clone();
    let held_slot = tokio::spawn(async move {
        holder
            .fetch_raw_transaction_bytes(TransactionId::from_bytes([0x24; 32]))
            .await
    });
    while server.requests_for("getrawtransaction")?.is_empty() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let started_at = std::time::Instant::now();
    let outcome = source
        .broadcast_transaction(RawTransactionBytes::new([0x04]))
        .await;
    let elapsed = started_at.elapsed();

    assert!(
        matches!(&outcome, Err(SourceError::NodeUnavailable { reason }) if reason.contains("timed out after 1000ms")),
        "a queued broadcast must time out at the request timeout, got {outcome:?}"
    );
    assert!(
        elapsed < REQUEST_TIMEOUT + Duration::from_millis(300),
        "the queued broadcast took {elapsed:?}, longer than one request timeout"
    );
    held_slot.await??;
    Ok(())
}
