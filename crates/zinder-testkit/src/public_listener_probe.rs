//! Client for the services every public wallet listener serves outside its
//! readiness gate, `grpc.health.v1.Health` and gRPC reflection, beside the
//! `/readyz` endpoint of the same runtime.
//!
//! [`PublicListenerProbe::agreed_status`] asks both the listener and
//! `/readyz` and fails when they disagree, so a test can drive readiness and
//! check at each step that gRPC health tells clients what `/readyz` tells
//! operators.

use std::{net::SocketAddr, time::Duration};

use eyre::{Result, bail, ensure, eyre};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
};
use tokio_stream::StreamExt as _;
use tonic::{
    Request, Status, Streaming,
    transport::{Channel, Endpoint},
};
use tonic_health::pb::{
    HealthCheckRequest, HealthCheckResponse, health_check_response::ServingStatus,
    health_client::HealthClient,
};
use tonic_reflection::pb::v1::{
    ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
    server_reflection_request::MessageRequest, server_reflection_response::MessageResponse,
};

/// Bound on each wait for a listener answer.
pub const PROBE_WAIT_LIMIT: Duration = Duration::from_secs(5);

/// Client of one public listener and of its runtime's operations endpoint.
#[derive(Clone, Debug)]
pub struct PublicListenerProbe {
    channel: Channel,
    ops_address: SocketAddr,
}

impl PublicListenerProbe {
    /// Connects to the gRPC listener at `listener_address`, whose runtime
    /// serves `/readyz` at `ops_address`.
    pub async fn connect(listener_address: SocketAddr, ops_address: SocketAddr) -> Result<Self> {
        let channel = Endpoint::new(format!("http://{listener_address}"))?
            .connect()
            .await?;
        Ok(Self {
            channel,
            ops_address,
        })
    }

    /// The connection to the listener, for calls to its wallet service.
    #[must_use]
    pub fn channel(&self) -> Channel {
        self.channel.clone()
    }

    /// Calls `grpc.health.v1.Health/Check` for `service`.
    pub async fn check(&self, service: &str) -> Result<ServingStatus, Status> {
        let response = HealthClient::new(self.channel.clone())
            .check(Request::new(health_request(service)))
            .await?
            .into_inner();
        Ok(serving_status(response))
    }

    /// Opens `grpc.health.v1.Health/Watch` for `service`.
    pub async fn watch(&self, service: &str) -> Result<Streaming<HealthCheckResponse>, Status> {
        Ok(HealthClient::new(self.channel.clone())
            .watch(Request::new(health_request(service)))
            .await?
            .into_inner())
    }

    /// Returns the HTTP status of the runtime's `/readyz`.
    pub async fn readyz_status(&self) -> Result<u16> {
        let mut stream = TcpStream::connect(self.ops_address).await?;
        stream
            .write_all(
                format!(
                    "GET /readyz HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
                    self.ops_address
                )
                .as_bytes(),
            )
            .await?;
        let mut response = Vec::new();
        tokio::time::timeout(PROBE_WAIT_LIMIT, stream.read_to_end(&mut response)).await??;
        let response = String::from_utf8(response)?;
        Ok(response
            .lines()
            .next()
            .and_then(|status_line| status_line.split_whitespace().nth(1))
            .ok_or_else(|| eyre!("/readyz answered without an HTTP status line"))?
            .parse()?)
    }

    /// Returns the listener's health status for the whole listener, after
    /// checking that `/readyz` reports the same admission: `SERVING` with
    /// 200 and `NOT_SERVING` with 503.
    pub async fn agreed_status(&self) -> Result<ServingStatus> {
        let status = self.check("").await?;
        let readyz = self.readyz_status().await?;
        let expected_readyz = match status {
            ServingStatus::Serving => 200,
            ServingStatus::NotServing => 503,
            ServingStatus::Unknown | ServingStatus::ServiceUnknown => {
                bail!("listener health reported {status:?}")
            }
        };
        ensure!(
            readyz == expected_readyz,
            "listener health reported {status:?} while /readyz answered {readyz}"
        );
        Ok(status)
    }

    /// Returns the service names gRPC reflection v1 lists.
    pub async fn reflected_services(&self) -> Result<Vec<String>> {
        let request = tokio_stream::once(ServerReflectionRequest {
            host: String::new(),
            message_request: Some(MessageRequest::ListServices(String::new())),
        });
        let mut responses = ServerReflectionClient::new(self.channel.clone())
            .server_reflection_info(Request::new(request))
            .await?
            .into_inner();
        let message_response = responses
            .next()
            .await
            .ok_or_else(|| eyre!("reflection stream closed without a response"))??
            .message_response;
        #[allow(
            clippy::wildcard_enum_match_arm,
            reason = "MessageResponse is non_exhaustive; any other variant is a failure here."
        )]
        match message_response {
            Some(MessageResponse::ListServicesResponse(list)) => Ok(list
                .service
                .into_iter()
                .map(|service| service.name)
                .collect()),
            other => Err(eyre!("unexpected reflection response: {other:?}")),
        }
    }
}

/// Returns the next status a `Watch` stream sends.
pub async fn next_watch_status(
    watch: &mut Streaming<HealthCheckResponse>,
) -> Result<ServingStatus> {
    let message = tokio::time::timeout(PROBE_WAIT_LIMIT, watch.next())
        .await
        .map_err(|_| eyre!("no health message within {PROBE_WAIT_LIMIT:?}"))?
        .ok_or_else(|| eyre!("health stream ended"))??;
    Ok(serving_status(message))
}

/// Waits for a `Watch` stream to end without sending another message.
pub async fn watch_ends(watch: &mut Streaming<HealthCheckResponse>) -> Result<()> {
    let next = tokio::time::timeout(PROBE_WAIT_LIMIT, watch.next())
        .await
        .map_err(|_| eyre!("health stream stayed open"))?;
    next.map_or(Ok(()), |message| {
        Err(eyre!("health stream sent {message:?} instead of ending"))
    })
}

fn health_request(service: &str) -> HealthCheckRequest {
    HealthCheckRequest {
        service: service.to_owned(),
    }
}

fn serving_status(response: HealthCheckResponse) -> ServingStatus {
    ServingStatus::try_from(response.status).unwrap_or(ServingStatus::Unknown)
}
