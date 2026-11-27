//! gRPC `grpc.health.v1.Health` probe helper for `zinder-query`.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    process::ExitCode,
};

use tonic_health::pb::{
    HealthCheckRequest, health_check_response::ServingStatus, health_client::HealthClient,
};
use zinder_runtime::{
    BearerTokenConnectError, OPS_HEALTHCHECK_TIMEOUT, connect_zinder_grpc_with_timeout,
};

#[allow(
    clippy::print_stderr,
    reason = "the supervisor captures a bounded probe diagnostic"
)]
pub(super) async fn run(mut addr: SocketAddr) -> ExitCode {
    if addr.ip().is_unspecified() {
        addr.set_ip(if addr.is_ipv4() {
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        } else {
            IpAddr::V6(Ipv6Addr::LOCALHOST)
        });
    }
    let outcome = tokio::time::timeout(OPS_HEALTHCHECK_TIMEOUT, check(addr))
        .await
        .unwrap_or(Err("deadline expired"));
    match outcome {
        Ok(ServingStatus::Serving) => ExitCode::SUCCESS,
        Ok(status @ (ServingStatus::NotServing | ServingStatus::Unknown)) => {
            eprintln!("gRPC health check for {addr}: {}", status.as_str_name());
            ExitCode::from(3)
        }
        Ok(ServingStatus::ServiceUnknown) => {
            eprintln!("gRPC health check for {addr}: SERVICE_UNKNOWN is only valid for Watch");
            ExitCode::FAILURE
        }
        Err(reason) => {
            eprintln!("gRPC health check for {addr}: {reason}");
            ExitCode::FAILURE
        }
    }
}

async fn check(addr: SocketAddr) -> Result<ServingStatus, &'static str> {
    let channel =
        connect_zinder_grpc_with_timeout(&format!("http://{addr}"), None, OPS_HEALTHCHECK_TIMEOUT)
            .await
            .map_err(|error| {
                if matches!(error, BearerTokenConnectError::ConnectTimeout { .. }) {
                    "deadline expired"
                } else {
                    "could not connect to the HTTP/2 listener"
                }
            })?;
    let reply = HealthClient::new(channel)
        .check(HealthCheckRequest::default())
        .await
        .map_err(|status| status.code().description())?
        .into_inner();
    ServingStatus::try_from(reply.status).map_err(|_| "invalid serving status")
}
