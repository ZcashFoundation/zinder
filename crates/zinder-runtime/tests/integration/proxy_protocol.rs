use eyre::Result;
use hyper_util::rt::TokioIo;
use tokio::{io::AsyncWriteExt as _, net::TcpListener};
use tonic::transport::Endpoint;
use tonic_health::pb::{HealthCheckRequest, health_client::HealthClient};
use tower::service_fn;

use std::{io, net::SocketAddr, time::Duration};
use tokio::{io::AsyncReadExt as _, net::TcpStream};
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
            let mut byte = [0];
            let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut byte)).await?;
            assert!(match read {
                Ok(count) => count == 0,
                Err(error) => error.kind() == io::ErrorKind::ConnectionReset,
            });
        }
        stop.cancel();
        server.await??;
    }
    Ok(())
}
