//! Proxy-side destination resolution for remote wallet connections.

use hyper_util::client::legacy::connect::{HttpConnector, proxy::SocksV5};
use tonic::transport::{Channel, Endpoint, Uri};
use tower::util::MapRequest;

use crate::IndexerError;

#[derive(Clone)]
pub(super) struct Socks5Proxy {
    connector: SocksV5<HttpConnector>,
    target: Uri,
}

impl Socks5Proxy {
    pub(super) fn new(proxy: &str, endpoint: &Endpoint) -> Result<Self, IndexerError> {
        let invalid_proxy = || {
            IndexerError::invalid_request(
                "SOCKS5 proxy must be a socks5:// or socks5h:// host and port without credentials",
            )
        };
        let proxy: Uri = proxy.parse().map_err(|_| invalid_proxy())?;
        let authority = proxy.authority().ok_or_else(invalid_proxy)?;
        if !matches!(proxy.scheme_str(), Some("socks5" | "socks5h"))
            || authority.as_str().contains('@')
            || authority.host().is_empty()
            || authority.port_u16().is_none_or(|port| port == 0)
            || proxy.path() != "/"
            || proxy.query().is_some()
        {
            return Err(invalid_proxy());
        }

        let proxy_address = Uri::builder()
            .scheme("http")
            .authority(authority.clone())
            .path_and_query("/")
            .build()
            .map_err(|_| invalid_proxy())?;
        let mut tcp = HttpConnector::new();
        tcp.set_nodelay(endpoint.get_tcp_nodelay());
        tcp.set_keepalive(endpoint.get_tcp_keepalive());
        let connector = SocksV5::new(proxy_address, tcp).local_dns(false);
        let target = socks_target(endpoint.uri())?;
        Ok(Self { connector, target })
    }

    pub(super) fn connect_lazy(&self, endpoint: &Endpoint) -> Channel {
        let target = self.target.clone();
        endpoint
            .connect_with_connector_lazy(MapRequest::new(self.connector.clone(), move |_: Uri| {
                target.clone()
            }))
    }
}

fn socks_target(endpoint: &Uri) -> Result<Uri, IndexerError> {
    let invalid_target =
        || IndexerError::invalid_request("SOCKS5 requires an http:// or https:// endpoint");
    let default_port = match endpoint.scheme_str() {
        Some("http") => 80,
        Some("https") => 443,
        Some(_) | None => return Err(invalid_target()),
    };
    let host = endpoint.host().ok_or_else(invalid_target)?;
    // Hyper's SOCKS5 connector assumes port 443 when the URI omits the port.
    let port = endpoint.port_u16().unwrap_or(default_port);
    Uri::builder()
        .authority(format!("{host}:{port}"))
        .build()
        .map_err(|_| invalid_target())
}
