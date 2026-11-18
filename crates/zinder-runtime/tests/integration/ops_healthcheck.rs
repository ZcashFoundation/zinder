use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use eyre::Result;
use tokio::net::TcpListener;
use zinder_runtime::{
    OPS_HEALTHCHECK_TIMEOUT, OpsHealthcheckError, OpsServer, Readiness, ReadinessState,
    probe_ops_endpoint, spawn_ops_endpoint,
};

async fn probe(addr: SocketAddr, path: &'static str) -> Result<Result<(), OpsHealthcheckError>> {
    Ok(
        tokio::task::spawn_blocking(move || {
            probe_ops_endpoint(addr, path, OPS_HEALTHCHECK_TIMEOUT)
        })
        .await?,
    )
}

/// The container image binds `[::]` or `0.0.0.0` and probes its own
/// operational endpoint through loopback, so the probe must accept exactly
/// the status codes the real endpoint serves.
#[tokio::test]
async fn healthcheck_follows_the_real_operational_endpoint() -> Result<()> {
    let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
    let listen_addr = listener.local_addr()?;
    drop(listener);
    let readiness = Readiness::default();
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
    let unspecified = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), listen_addr.port());

    probe(unspecified, "/healthz").await??;
    let not_ready = probe(unspecified, "/readyz").await?;
    assert!(
        matches!(
            &not_ready,
            Err(OpsHealthcheckError::UnhealthyStatus { status: 503, body_excerpt, .. })
                if body_excerpt.contains("\"not_ready\"")
        ),
        "{not_ready:?}"
    );

    readiness.set(ReadinessState::ready(Some(7)));
    probe(unspecified, "/readyz").await??;
    let missing = probe(unspecified, "/no-such-route").await?;
    assert!(
        matches!(
            missing,
            Err(OpsHealthcheckError::UnhealthyStatus { status: 404, .. })
        ),
        "{missing:?}"
    );

    server_handle.shutdown().await?;
    Ok(())
}
