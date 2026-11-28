use std::{io, net::SocketAddr, time::Duration};

use eyre::Result;
use hyper_util::rt::TokioIo;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
};
use tonic::transport::{Channel, Endpoint};
use tonic_health::pb::{HealthCheckRequest, health_client::HealthClient};
use tower::service_fn;
use zinder_runtime::{ClientAddressSource, GrpcProxyProtocol, public_grpc_server};

#[tokio::test]
async fn listener_names_the_forwarded_client_and_closes_untrusted_peers() -> Result<()> {
    for trusted in ["127.0.0.1/32", "192.0.2.10/32"] {
        let protocol = GrpcProxyProtocol::resolve(Some(trusted), ClientAddressSource::Connection)?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (_reporter, health) = tonic_health::server::health_reporter();
        let forwarded: SocketAddr = "192.0.2.9:5000".parse()?;
        let health = tonic::service::interceptor::InterceptedService::new(
            health,
            move |request: tonic::Request<()>| {
                if request.remote_addr() == Some(forwarded) {
                    Ok(request)
                } else {
                    Err(tonic::Status::internal("wrong client endpoint"))
                }
            },
        );
        let stop = tokio_util::sync::CancellationToken::new();
        let server = tokio::spawn(
            public_grpc_server(ClientAddressSource::Connection)
                .with_proxy_protocol(protocol)
                .add_service(health)
                .serve_with_incoming_shutdown(listener, stop.clone().cancelled_owned()),
        );
        let preface = b"PROXY TCP4 192.0.2.9 192.0.2.10 5000 443\r\n";
        if trusted.starts_with("127.") {
            let channel = Endpoint::from_shared(format!("http://{address}"))?
                .connect_timeout(Duration::from_secs(5))
                .connect_with_connector(service_fn(move |_| async move {
                    let mut stream = TcpStream::connect(address).await?;
                    stream.write_all(preface).await?;
                    Ok::<_, io::Error>(TokioIo::new(stream))
                }))
                .await?;
            HealthClient::new(channel)
                .check(HealthCheckRequest::default())
                .await?;
        } else {
            let mut stream = TcpStream::connect(address).await?;
            let _ = stream.write_all(preface).await;
            tokio::time::timeout(
                Duration::from_secs(5),
                expect_connection_closed(&mut stream),
            )
            .await??;
        }
        stop.cancel();
        server.await??;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn listener_reuses_preface_slots_and_shuts_down_with_pending_connections() -> Result<()> {
    // Freeze preface deadlines while loopback I/O is pending. The blocking
    // task also bounds the test in wall time if an expected socket event stalls.
    let (finished, wait_for_test) = std::sync::mpsc::channel::<()>();
    let wall_deadline = tokio::task::spawn_blocking(move || {
        let _ = wait_for_test.recv_timeout(Duration::from_secs(10));
    });
    let outcome = tokio::select! {
        outcome = verify_preface_slots() => outcome,
        _ = wall_deadline => Err(eyre::eyre!("preface lifecycle did not finish within 10 seconds")),
    };
    drop(finished);
    outcome
}

enum PrefaceCompletion {
    Valid,
    Invalid,
    TimedOut,
}

async fn verify_preface_slots() -> Result<()> {
    const MAX_PENDING: usize = 128;
    for completion in [
        PrefaceCompletion::Valid,
        PrefaceCompletion::Invalid,
        PrefaceCompletion::TimedOut,
    ] {
        let protocol =
            GrpcProxyProtocol::resolve(Some("127.0.0.1/32"), ClientAddressSource::Connection)?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (_reporter, health) = tonic_health::server::health_reporter();
        let stop = tokio_util::sync::CancellationToken::new();
        let server = tokio::spawn(
            public_grpc_server(ClientAddressSource::Connection)
                .with_proxy_protocol(protocol)
                .add_service(health)
                .serve_with_incoming_shutdown(listener, stop.clone().cancelled_owned()),
        );

        let mut pending = Vec::with_capacity(MAX_PENDING);
        for _ in 0..MAX_PENDING {
            pending.push(connect_with_incomplete_preface(address).await?);
        }
        let mut overflow = TcpStream::connect(address).await?;
        expect_connection_closed(&mut overflow).await?;
        for stream in &pending {
            assert_pending(stream);
        }

        let mut first = pending.remove(0);
        let established = match completion {
            PrefaceCompletion::Valid => {
                Some(complete_preface_and_check_health(address, first).await?)
            }
            PrefaceCompletion::Invalid => {
                first.write_all(b"INVALID\r\n").await?;
                expect_connection_closed(&mut first).await?;
                None
            }
            PrefaceCompletion::TimedOut => {
                tokio::time::advance(Duration::from_secs(2)).await;
                expect_connection_closed(&mut first).await?;
                None
            }
        };
        let replacement = connect_with_incomplete_preface(address).await?;
        drop(complete_preface_and_check_health(address, replacement).await?);
        drop(established);

        if matches!(
            completion,
            PrefaceCompletion::Valid | PrefaceCompletion::Invalid
        ) {
            assert_pending(&pending[0]);
        }
        stop.cancel();
        server.await??;
        expect_connection_closed(&mut pending[0]).await?;
    }
    Ok(())
}

async fn connect_with_incomplete_preface(address: SocketAddr) -> io::Result<TcpStream> {
    let mut stream = TcpStream::connect(address).await?;
    stream.write_all(b"PROXY ").await?;
    Ok(stream)
}

async fn complete_preface_and_check_health(
    address: SocketAddr,
    mut stream: TcpStream,
) -> Result<Channel> {
    stream.write_all(b"UNKNOWN\r\n").await?;
    let mut stream = Some(stream);
    let channel = Endpoint::from_shared(format!("http://{address}"))?
        .connect_with_connector(service_fn(move |_| {
            std::future::ready(
                stream
                    .take()
                    .map(TokioIo::new)
                    .ok_or_else(|| io::Error::other("test socket already used")),
            )
        }))
        .await?;
    HealthClient::new(channel.clone())
        .check(HealthCheckRequest::default())
        .await?;
    Ok(channel)
}

fn assert_pending(stream: &TcpStream) {
    assert!(matches!(
        stream.try_read(&mut [0]),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock
    ));
}

async fn expect_connection_closed(stream: &mut TcpStream) -> Result<()> {
    let mut byte = [0];
    let closed = match stream.read(&mut byte).await {
        Ok(count) => count == 0,
        Err(error) => error.kind() == io::ErrorKind::ConnectionReset,
    };
    eyre::ensure!(closed, "expected the listener to close the connection");
    Ok(())
}
