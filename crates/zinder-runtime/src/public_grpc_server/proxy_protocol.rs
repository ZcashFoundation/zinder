//! A required PROXY preface on a listener reserved for trusted TCP proxies.

use std::{
    fmt, io,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use ipnet::IpNet;
use tokio::{io::AsyncReadExt as _, net::TcpStream};

use super::ClientAddressSource;
use crate::ConfigError;

const MAX_TRUSTED_CIDRS: usize = 64;
const MAX_V1_HEADER_BYTES: usize = 107;
const MAX_V2_PAYLOAD_BYTES: usize = 4096 - 16;
const V2_SIGNATURE: &[u8; 12] = b"\r\n\r\n\0\r\nQUIT\n";
const HEADER_TIMEOUT: Duration = Duration::from_secs(1);

/// Trusted TCP peers of a public gRPC listener using PROXY protocol v1 or v2.
///
/// The default disables PROXY protocol. An enabled listener requires a preface
/// from every connection before accepting HTTP.
#[derive(Clone, Default)]
pub struct GrpcProxyProtocol(Arc<[IpNet]>);

impl fmt::Debug for GrpcProxyProtocol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("GrpcProxyProtocol")
            .field(&self.redacted_cidrs())
            .finish()
    }
}

impl GrpcProxyProtocol {
    /// Enables PROXY protocol when a comma-separated CIDR allowlist is supplied.
    /// Use a private bind or firewall that admits only those proxies.
    ///
    /// # Errors
    ///
    /// Rejects malformed, empty, or overly broad lists, and simultaneous HTTP
    /// client address forwarding.
    pub fn resolve(
        trusted_cidrs: Option<&str>,
        client_address: ClientAddressSource,
    ) -> Result<Self, ConfigError> {
        let Some(trusted_cidrs) = trusted_cidrs else {
            return Ok(Self::default());
        };
        if client_address.trust_proxy_client_address() {
            return Err(ConfigError::invalid(
                "proxy_protocol_trusted_cidrs cannot be combined with trust_proxy_client_address",
            ));
        }
        let invalid = || {
            ConfigError::invalid(
                "proxy_protocol_trusted_cidrs requires 1 to 64 comma-separated CIDRs with nonzero prefixes",
            )
        };
        let mut networks = Vec::new();
        for cidr in trusted_cidrs.split(',') {
            let network = cidr.trim().parse::<IpNet>().map_err(|_| invalid())?;
            if networks.len() == MAX_TRUSTED_CIDRS || network.prefix_len() == 0 {
                return Err(invalid());
            }
            networks.push(network);
        }
        Ok(Self(networks.into()))
    }

    /// Shows whether an allowlist is configured without exposing proxy networks.
    #[must_use]
    pub fn redacted_cidrs(&self) -> Option<&'static str> {
        self.enabled().then_some("[REDACTED]")
    }

    pub(super) fn enabled(&self) -> bool {
        !self.0.is_empty()
    }

    pub(super) fn trusts(&self, peer: IpAddr) -> bool {
        let canonical = peer.to_canonical();
        self.0
            .iter()
            .any(|network| network.contains(&peer) || network.contains(&canonical))
    }
}

pub(super) async fn read_client(
    stream: &mut TcpStream,
    peer: SocketAddr,
) -> io::Result<SocketAddr> {
    tokio::time::timeout(HEADER_TIMEOUT, read_preface(stream, peer)).await?
}

async fn read_preface(stream: &mut TcpStream, peer: SocketAddr) -> io::Result<SocketAddr> {
    match stream.read_u8().await? {
        b'P' => {
            let mut line = [0; MAX_V1_HEADER_BYTES];
            line[0] = b'P';
            for end in 2..=line.len() {
                line[end - 1] = stream.read_u8().await?;
                if line[end - 1] == b'\n' {
                    return parse_v1(&line[..end], peer).ok_or_else(invalid_header);
                }
            }
            Err(invalid_header())
        }
        b'\r' => {
            let mut fixed = [0; 16];
            fixed[0] = b'\r';
            stream.read_exact(&mut fixed[1..]).await?;
            if fixed[..12] != *V2_SIGNATURE || !matches!(fixed[12], 0x20 | 0x21) {
                return Err(invalid_header());
            }
            let length = usize::from(u16::from_be_bytes([fixed[14], fixed[15]]));
            let mut payload = [0; MAX_V2_PAYLOAD_BYTES];
            let payload = payload.get_mut(..length).ok_or_else(invalid_header)?;
            stream.read_exact(payload).await?;
            if fixed[12] == 0x20 {
                return Ok(peer);
            }
            parse_v2(fixed[13], payload, peer).ok_or_else(invalid_header)
        }
        _ => Err(invalid_header()),
    }
}

fn parse_v1(line: &[u8], peer: SocketAddr) -> Option<SocketAddr> {
    let line = str::from_utf8(line.strip_suffix(b"\r\n")?).ok()?;
    if !line
        .bytes()
        .all(|byte| byte.is_ascii_graphic() || byte == b' ')
    {
        return None;
    }
    let mut fields = line.strip_prefix("PROXY ")?.split(' ');
    let family = fields.next()?;
    if family == "UNKNOWN" {
        return Some(peer);
    }
    let source: IpAddr = fields.next()?.parse().ok()?;
    let destination: IpAddr = fields.next()?.parse().ok()?;
    match (family, source, destination) {
        ("TCP4", IpAddr::V4(_), IpAddr::V4(_)) | ("TCP6", IpAddr::V6(_), IpAddr::V6(_)) => {}
        _ => return None,
    }
    let source_port = parse_port(fields.next()?)?;
    parse_port(fields.next()?)?;
    fields
        .next()
        .is_none()
        .then_some(SocketAddr::new(source, source_port))
}

fn parse_port(port: &str) -> Option<u16> {
    if !port.bytes().all(|byte| byte.is_ascii_digit()) || (port.len() > 1 && port.starts_with('0'))
    {
        return None;
    }
    port.parse().ok()
}

fn parse_v2(family: u8, payload: &[u8], peer: SocketAddr) -> Option<SocketAddr> {
    let (client, mut tlvs) = match family {
        0x00 => (peer, payload),
        0x11 => {
            let (addresses, tlvs) = payload.split_first_chunk::<12>()?;
            let ip = IpAddr::from(*addresses.first_chunk::<4>()?);
            let port = u16::from_be_bytes([addresses[8], addresses[9]]);
            (SocketAddr::new(ip, port), tlvs)
        }
        0x21 => {
            let (addresses, tlvs) = payload.split_first_chunk::<36>()?;
            let ip = IpAddr::from(*addresses.first_chunk::<16>()?);
            let port = u16::from_be_bytes([addresses[32], addresses[33]]);
            (SocketAddr::new(ip, port), tlvs)
        }
        _ => return None,
    };
    while !tlvs.is_empty() {
        let ([_, high, low], body) = tlvs.split_first_chunk::<3>()?;
        tlvs = body.get(usize::from(u16::from_be_bytes([*high, *low]))..)?;
    }
    Some(client)
}

fn invalid_header() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid PROXY protocol preface")
}

#[cfg(test)]
mod tests {
    use eyre::Result;
    use tokio::{io::AsyncWriteExt as _, net::TcpListener};

    use super::*;

    #[test]
    fn trust_is_explicit_and_prefix_scoped() -> Result<()> {
        let source = ClientAddressSource::Connection;
        let protocol = GrpcProxyProtocol::resolve(Some("192.0.2.0/24, 2001:db8:1::/64"), source)?;
        for address in ["192.0.2.15", "::ffff:192.0.2.15", "2001:db8:1::5"] {
            assert!(protocol.trusts(address.parse()?));
        }
        for address in ["192.0.3.15", "2001:db8:2::5"] {
            assert!(!protocol.trusts(address.parse()?));
        }
        for list in ["", "192.0.2.15", "192.0.2.0/24,", "0.0.0.0/0", "::/0"] {
            assert!(GrpcProxyProtocol::resolve(Some(list), source).is_err());
        }
        assert!(
            GrpcProxyProtocol::resolve(Some("127.0.0.1/32"), ClientAddressSource::ProxyHeader)
                .is_err()
        );
        assert_eq!(protocol.redacted_cidrs(), Some("[REDACTED]"));
        Ok(())
    }

    #[test]
    fn rejects_invalid_v1_addresses_ports_and_terminators() {
        for line in [
            &b"PROXY TCP4 192.0.2.9 ::1 5000 443\r\n"[..],
            b"PROXY TCP4 192.0.2.9 192.0.2.10 +5 443\r\n",
            b"PROXY TCP4 192.0.2.9 192.0 asdf 443\r\n",
            b"PROXY TCP4 192.0.2.9 192.0.2.10 5 443\n",
            b"PROXY TCP4 192.0.2.9 192.0.2.10 5 443 extra\r\n",
        ] {
            assert_eq!(parse_v1(line, ([127, 0, 0, 1], 50).into()), None);
        }
    }

    #[tokio::test]
    async fn fragmented_prefaces_leave_http_bytes_in_the_socket() -> Result<()> {
        let mut binary = V2_SIGNATURE.to_vec();
        binary.extend_from_slice(&[0x21, 0x21, 0, 40]);
        binary.extend_from_slice(&"2001:db8::1".parse::<std::net::Ipv6Addr>()?.octets());
        binary.extend_from_slice(&"2001:db8::2".parse::<std::net::Ipv6Addr>()?.octets());
        binary.extend_from_slice(&[0x13, 0x88, 1, 187, 4, 0, 1, 0]);
        for (preface, source) in [
            (
                &b"PROXY TCP4 192.0.2.9 192.0.2.10 5000 443\r\n"[..],
                "192.0.2.9:5000",
            ),
            (
                &b"PROXY TCP6 2001:db8::1 2001:db8::2 5000 443\r\n"[..],
                "[2001:db8::1]:5000",
            ),
            (&binary, "[2001:db8::1]:5000"),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let mut client = TcpStream::connect(listener.local_addr()?).await?;
            let (mut peer_stream, peer) = listener.accept().await?;
            let write_preface = async {
                for fragment in preface.chunks(3) {
                    client.write_all(fragment).await?;
                    tokio::task::yield_now().await;
                }
                client.write_all(b"PRI").await
            };
            let (written, address) =
                tokio::join!(write_preface, read_client(&mut peer_stream, peer));
            written?;
            assert_eq!(address?, source.parse()?);
            let mut http_prefix = [0; 3];
            peer_stream.read_exact(&mut http_prefix).await?;
            assert_eq!(&http_prefix, b"PRI");
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn incomplete_preface_times_out_and_oversized_v2_is_refused() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut client = TcpStream::connect(listener.local_addr()?).await?;
        let (mut peer_stream, peer) = listener.accept().await?;
        client.write_all(b"PROXY TCP4").await?;
        let ((), outcome) = tokio::join!(
            tokio::time::sleep(HEADER_TIMEOUT),
            read_client(&mut peer_stream, peer)
        );
        assert_eq!(
            outcome.map_err(|error| error.kind()),
            Err(io::ErrorKind::TimedOut)
        );

        let mut client = TcpStream::connect(listener.local_addr()?).await?;
        let (mut peer_stream, peer) = listener.accept().await?;
        client.write_all(V2_SIGNATURE).await?;
        client.write_all(&[0x21, 0x11, 0xff, 0xff]).await?;
        assert_eq!(
            read_client(&mut peer_stream, peer)
                .await
                .map_err(|error| error.kind()),
            Err(io::ErrorKind::InvalidData)
        );
        Ok(())
    }
}
