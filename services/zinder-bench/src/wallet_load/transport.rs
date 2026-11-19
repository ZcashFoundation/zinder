//! Connections a simulated wallet opens to the server under test.
//!
//! The server can be outside Zinder, such as Go lightwalletd. Its grpc-go
//! server closes a connection whose client sends HTTP/2 keep-alive pings more
//! often than its enforcement policy allows, five minutes by default, so the
//! 30-second pings of Zinder's internal channels would end streams that a
//! wallet without them keeps. The load client therefore builds its own
//! endpoint with no keep-alive pings, which ADR-0007 lets a component do for
//! an external product it calls.

use std::time::Duration;

use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

/// Checks `url` and returns an endpoint that opens one connection per
/// `connect` call. An `https` URL uses TLS with the platform's trusted roots;
/// an `http` URL stays plaintext HTTP/2.
pub(super) fn wallet_endpoint(url: &str, connect_timeout: Duration) -> Result<Endpoint, String> {
    let endpoint = Endpoint::from_shared(url.to_owned())
        .map_err(|error| error.to_string())?
        .connect_timeout(connect_timeout);
    if endpoint.uri().scheme_str() == Some("https") {
        return endpoint
            .tls_config(ClientTlsConfig::new().with_enabled_roots())
            .map_err(|error| error.to_string());
    }
    Ok(endpoint)
}

/// Opens one new HTTP/2 connection through `endpoint`.
pub(super) async fn connect(endpoint: &Endpoint) -> Result<Channel, String> {
    endpoint.connect().await.map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{io::AsyncReadExt as _, net::TcpListener};

    use super::{connect, wallet_endpoint};

    /// Connects to a listener that records the first byte the client sends.
    async fn first_byte_sent(scheme: &str) -> Result<u8, Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = wallet_endpoint(
            &format!("{scheme}://{}", listener.local_addr()?),
            Duration::from_secs(5),
        )?;
        let client = tokio::spawn(async move { connect(&endpoint).await });
        let (mut socket, _) = listener.accept().await?;
        let mut first = [0_u8; 1];
        socket.read_exact(&mut first).await?;
        drop(socket);
        client.abort();
        Ok(first[0])
    }

    #[tokio::test]
    async fn https_urls_open_with_a_tls_handshake() -> Result<(), Box<dyn std::error::Error>> {
        // 0x16 starts a TLS handshake record.
        assert_eq!(first_byte_sent("https").await?, 0x16);
        Ok(())
    }

    #[tokio::test]
    async fn http_urls_stay_plaintext_http2() -> Result<(), Box<dyn std::error::Error>> {
        // The HTTP/2 connection preface starts with "PRI".
        assert_eq!(first_byte_sent("http").await?, b'P');
        Ok(())
    }
}
