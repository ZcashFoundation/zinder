use std::{net::SocketAddr, process::Output, sync::Arc, time::Duration};

use tokio::{
    net::{TcpListener, TcpSocket},
    sync::{Notify, watch},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, transport::Server};
use tonic_health::pb::{
    HealthCheckRequest, HealthCheckResponse,
    health_check_response::ServingStatus,
    health_server::{Health, HealthServer},
};
use zinder_runtime::OPS_HEALTHCHECK_TIMEOUT;

pub(super) async fn probe(addr: SocketAddr) -> eyre::Result<Output> {
    Ok(tokio::time::timeout(
        OPS_HEALTHCHECK_TIMEOUT + Duration::from_secs(2),
        tokio::process::Command::new(zinder_testkit::bin_exe!("zinder-query"))
            .arg("grpc-healthcheck")
            .arg(addr.to_string())
            .kill_on_drop(true)
            .output(),
    )
    .await??)
}

struct HealthReplies {
    status: watch::Receiver<Option<i32>>,
    stalled: Arc<Notify>,
}

#[tonic::async_trait]
impl Health for HealthReplies {
    async fn check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        if !request.get_ref().service.is_empty() {
            return Err(Status::not_found("only whole-listener health is available"));
        }
        let status = *self.status.borrow();
        if let Some(status) = status {
            return Ok(Response::new(HealthCheckResponse { status }));
        }
        self.stalled.notify_one();
        std::future::pending().await
    }

    type WatchStream = tokio_stream::Empty<Result<HealthCheckResponse, Status>>;

    async fn watch(
        &self,
        _: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        Err(Status::unimplemented("unary checks only"))
    }
}

#[tokio::test]
async fn grpc_healthcheck_distinguishes_serving_non_serving_and_failed_calls() -> eyre::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = SocketAddr::from(([0, 0, 0, 0], listener.local_addr()?.port()));
    let (status, replies) = watch::channel(None);
    let stalled = Arc::new(Notify::new());
    let health = HealthServer::new(HealthReplies {
        status: replies,
        stalled: stalled.clone(),
    });
    let server = tokio::spawn(
        Server::builder()
            .add_service(health)
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    for (reply, exit) in [
        (ServingStatus::Serving as i32, 0),
        (ServingStatus::NotServing as i32, 3),
        (ServingStatus::Unknown as i32, 3),
        (99, 1),
    ] {
        status.send(Some(reply))?;
        assert_eq!(
            probe(addr).await?.status.code(),
            Some(exit),
            "status {reply}"
        );
    }

    status.send(None)?;
    let (output, checked) = tokio::join!(
        probe(addr),
        tokio::time::timeout(OPS_HEALTHCHECK_TIMEOUT, stalled.notified())
    );
    checked?;
    let output = output?;
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("deadline expired"));
    server.abort();
    Ok(())
}

#[tokio::test]
async fn grpc_healthcheck_times_out_when_an_accepted_socket_withholds_http2_settings()
-> eyre::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let (output, accepted) = tokio::join!(
        probe(listener.local_addr()?),
        tokio::time::timeout(OPS_HEALTHCHECK_TIMEOUT, listener.accept())
    );
    drop(accepted??);
    let output = output?;
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("deadline expired"));
    Ok(())
}

#[tokio::test]
async fn grpc_healthcheck_fails_when_the_socket_refuses_the_connection() -> eyre::Result<()> {
    let socket = TcpSocket::new_v4()?;
    socket.bind(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    let output = probe(socket.local_addr()?).await?;
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("could not connect"));
    Ok(())
}
