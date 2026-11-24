#![allow(
    missing_docs,
    reason = "Live test names describe the behavior under test."
)]

use std::{net::SocketAddr, time::Duration};

use eyre::{Result, eyre};
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::{
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
};
use zinder_core::{Network, RawTransactionBytes, TransactionBroadcastOutcome};
use zinder_query::{OrderedBroadcaster, WalletQueryApi, WalletServingQuery};
use zinder_source::{ZebraJsonRpcSource, ZebraJsonRpcSourceOptions};
use zinder_testkit::live::{init, require_live_for};

use crate::common::{
    fetch_live_network_upgrade_activations, fetch_live_tip_height, live_serving_pair,
};

/// A TCP proxy to the live node that can be stopped, standing in for a
/// `[node]` that passes verification and then goes down.
struct StoppableProxy {
    address: SocketAddr,
    forwarding: JoinHandle<()>,
}

impl StoppableProxy {
    async fn start(upstream: String) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let forwarding = tokio::spawn(async move {
            // Dropped with the task, which closes every open connection.
            let mut connections = JoinSet::new();
            while let Ok((mut inbound, _)) = listener.accept().await {
                let upstream = upstream.clone();
                connections.spawn(async move {
                    if let Ok(mut outbound) = TcpStream::connect(upstream).await {
                        _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
            }
        });
        Ok(Self {
            address,
            forwarding,
        })
    }

    /// Closes the listener and every connection through it, so the next
    /// request is refused.
    async fn stop(self) {
        self.forwarding.abort();
        _ = self.forwarding.await;
    }
}

/// With `[node]` verified and then stopped, a broadcast fails over to the
/// node configured as its fallback, and that node becomes the preferred
/// endpoint for the cause `node_unreachable`.
#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn broadcast_fails_over_to_the_fallback_node_after_the_first_node_stops() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[Network::ZcashRegtest])? else {
        return Ok(());
    };
    let recorder = PrometheusBuilder::new().build_recorder();
    let recorded_metrics = recorder.handle();
    let _local_recorder = metrics::set_default_local_recorder(&recorder);
    let tip_height = fetch_live_tip_height(&env).await?;
    let activations = fetch_live_network_upgrade_activations(&env).await?;
    let live = live_serving_pair(&env, &activations, tip_height).await?;
    let upstream = env
        .target
        .json_rpc_addr
        .strip_prefix("http://")
        .ok_or_else(|| eyre!("the proxy forwards only an http:// node JSON-RPC address"))?
        .trim_end_matches('/')
        .to_owned();
    let proxy = StoppableProxy::start(upstream).await?;
    let proxied_node = ZebraJsonRpcSource::with_options(
        env.target.network,
        format!("http://{}", proxy.address),
        env.target.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: env.target.request_timeout,
            max_response_bytes: env.target.max_response_bytes,
            broadcast_timeout: Some(Duration::from_secs(2)),
            max_concurrent_requests: env.target.max_concurrent_requests,
        },
    )?;
    let nodes = OrderedBroadcaster::new(proxied_node, [live.source]);
    nodes.verify(&live.slot).await?;
    let verified = recorded_metrics.render();
    for position in ["0", "1"] {
        let state = format!(
            "zinder_wallet_broadcast_endpoint_state{{position=\"{position}\",state=\"verified\"}} 1"
        );
        assert!(
            verified.lines().any(|line| line == state),
            "endpoint {position} must pass verification before the first one stops:\n{verified}"
        );
    }
    proxy.stop().await;
    let wallet_query = WalletServingQuery::from_serving_pair_slot(live.slot)
        .with_node_broadcast(Some(nodes.clone()));

    // A byte that is no transaction makes the reachable node answer with a
    // refusal, which proves the broadcast reached it without spending funds.
    let outcome = wallet_query
        .broadcast_transaction(RawTransactionBytes::new(vec![0x00]))
        .await?;

    if matches!(outcome, TransactionBroadcastOutcome::Accepted(_)) {
        return Err(eyre!(
            "the node accepted a one-byte transaction: {outcome:?}"
        ));
    }
    assert_eq!(nodes.preferred_position(), 1);
    let rendered = recorded_metrics.render();
    let switched = "zinder_wallet_broadcast_endpoint_switches_total{from_position=\"0\",to_position=\"1\",cause=\"node_unreachable\"} 1";
    assert!(
        rendered.lines().any(|line| line == switched),
        "missing {switched}:\n{rendered}"
    );
    Ok(())
}
