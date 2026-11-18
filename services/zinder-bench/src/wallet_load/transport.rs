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

use tonic::transport::{Channel, Endpoint};

/// Checks `url` and returns an endpoint that opens one connection per
/// `connect` call. An `https` URL uses TLS with the platform's trusted roots.
pub(super) fn wallet_endpoint(url: &str, connect_timeout: Duration) -> Result<Endpoint, String> {
    Endpoint::from_shared(url.to_owned())
        .map(|endpoint| endpoint.connect_timeout(connect_timeout))
        .map_err(|error| error.to_string())
}

/// Opens one new HTTP/2 connection through `endpoint`.
pub(super) async fn connect(endpoint: &Endpoint) -> Result<Channel, String> {
    endpoint.connect().await.map_err(|error| error.to_string())
}
