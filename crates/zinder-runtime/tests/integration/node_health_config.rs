//! Operator settings for the JSON-RPC fallback and the optional HTTP readiness probe.

use serde::Deserialize;
use zinder_core::Network;
use zinder_runtime::ConfigLoader;
use zinder_source::{
    NodeCapability, NodeSection, NodeSource, NodeTarget,
    UPSTREAM_HEALTH_SOURCE_VERIFICATION_PROGRESS_FALLBACK, ZebraJsonRpcSource,
};
use zinder_testkit::{JsonRpcTestServer, RpcReply, method};

#[derive(Deserialize)]
struct RawConfig {
    node: NodeSection,
}

#[tokio::test]
async fn fallback_health_config_applies_with_an_absent_or_failed_ready_endpoint() -> eyre::Result<()>
{
    let rpc = JsonRpcTestServer::start([method("getblockchaininfo").reply_with(|_| {
        RpcReply::result(serde_json::json!({
            "blocks": 100, "estimatedheight": 114, "verificationprogress": 0.995
        }))
    })])?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("node.toml");
    std::fs::write(
        &path,
        format!(
            "[node]\njson_rpc_addr = \"{}\"\n[node.health]\nverification_progress_floor = 0.995\nestimated_gap_floor_blocks = 14\n",
            rpc.url(),
        ),
    )?;

    let offline = tokio::net::TcpSocket::new_v4()?;
    offline.bind("127.0.0.1:0".parse()?)?;
    let unavailable_endpoint = format!("http://{}/ready", offline.local_addr()?);
    for addr in [None, Some(unavailable_endpoint.as_str())] {
        let settings = addr.map(|addr| ("ZINDER_NODE__HEALTH__ADDR".to_owned(), addr.to_owned()));
        let raw = ConfigLoader::new()
            .with_file(Some(path.clone()))
            .with_zinder_env_from_map(settings)?
            .load::<RawConfig>()?;
        let target = NodeTarget::resolve(Network::ZcashTestnet, raw.node)?;
        let source = ZebraJsonRpcSource::from_node_target(&target)?;
        let snapshot = source.poll_upstream_health().await?;
        assert!(
            snapshot.ready_for_queries,
            "addr={addr:?}: {}",
            snapshot.reason
        );
        assert_eq!(
            snapshot.source,
            UPSTREAM_HEALTH_SOURCE_VERIFICATION_PROGRESS_FALLBACK
        );
        assert_eq!(
            source
                .capabilities()
                .supports(NodeCapability::ReadinessProbe),
            addr.is_some()
        );

        let printed = toml::Value::try_from(NodeSection::from_resolved(&target))?;
        let health = &printed["health"];
        assert_eq!(health["estimated_gap_floor_blocks"].as_integer(), Some(14));
        assert_eq!(
            health["verification_progress_floor"].as_float(),
            Some(0.995)
        );
        assert_eq!(health.get("addr").and_then(toml::Value::as_str), addr);
    }
    Ok(())
}
