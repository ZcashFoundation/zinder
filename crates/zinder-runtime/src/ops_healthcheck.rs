//! The `healthcheck` subcommand that every service binary runs as its
//! container health probe.
//!
//! Release service images carry no shell and no HTTP client, so the image
//! `HEALTHCHECK` runs the service binary itself. The probe resolves the
//! operational endpoint without loading the service configuration, sends one
//! HTTP/1.1 `GET` with `Connection: close`, and succeeds only when the endpoint
//! answers status 200.

use std::{
    io::{self, Read, Write},
    net::{AddrParseError, IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream},
    time::{Duration, Instant},
};

use thiserror::Error;

/// Environment variable that sets `ops.listen_addr` for every service binary.
pub const OPS_LISTEN_ADDR_ENV_VAR: &str = "ZINDER_OPS__LISTEN_ADDR";

/// Path the `healthcheck` subcommand requests unless `--path` names another.
pub const DEFAULT_OPS_HEALTHCHECK_PATH: &str = "/readyz";

/// Bound on connecting, sending the request, and reading the response status.
///
/// It stays below the 5-second timeout that the release image `HEALTHCHECK`
/// and the Compose healthchecks allow, so the probe reports its own reason
/// before the container runtime kills it.
pub const OPS_HEALTHCHECK_TIMEOUT: Duration = Duration::from_secs(3);

/// Largest response head the probe reads before it stops looking for the
/// status line.
const MAX_RESPONSE_HEAD_BYTES: usize = 8 * 1024;

/// Longest body excerpt an unhealthy-status error carries.
const MAX_BODY_EXCERPT_BYTES: usize = 256;

const RESPONSE_HEAD_TERMINATOR: &[u8] = b"\r\n\r\n";

/// Arguments of the `healthcheck` subcommand.
#[derive(Clone, Debug, clap::Args)]
pub struct OpsHealthcheckArgs {
    /// Operational endpoint to probe, such as 127.0.0.1:9106. Defaults to
    /// `ZINDER_OPS__LISTEN_ADDR`, then to the service's default ops address.
    /// An unspecified host (`[::]` or `0.0.0.0`) probes loopback.
    #[arg(long = "addr")]
    pub addr: Option<SocketAddr>,
    /// HTTP path to request; only status 200 counts as healthy.
    #[arg(long = "path", default_value = DEFAULT_OPS_HEALTHCHECK_PATH)]
    pub path: String,
}

impl OpsHealthcheckArgs {
    /// Resolves the endpoint address and probes it once.
    ///
    /// `default_ops_listen_addr` is the service's default `ops.listen_addr`,
    /// used when neither `--addr` nor `ZINDER_OPS__LISTEN_ADDR` names one.
    /// The service configuration file is not read; pass `--addr` when the ops
    /// address is set only there.
    pub fn run(&self, default_ops_listen_addr: &str) -> Result<(), OpsHealthcheckError> {
        let environment_value = std::env::var_os(OPS_LISTEN_ADDR_ENV_VAR)
            .map(|environment_value| environment_value.to_string_lossy().into_owned());
        let endpoint = resolve_ops_healthcheck_addr(
            self.addr,
            environment_value.as_deref(),
            default_ops_listen_addr,
        )?;
        probe_ops_endpoint(endpoint, &self.path, OPS_HEALTHCHECK_TIMEOUT)
    }
}

/// Error returned when the operational endpoint is not healthy or cannot be
/// probed.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OpsHealthcheckError {
    /// `ZINDER_OPS__LISTEN_ADDR` is empty, which disables the endpoint.
    #[error("the operational endpoint is disabled because ZINDER_OPS__LISTEN_ADDR is empty")]
    EndpointDisabled,

    /// The address to probe is not a socket address.
    #[error("{origin} {text:?} is not a socket address: {source}")]
    InvalidAddress {
        /// Where the address came from.
        origin: &'static str,
        /// The rejected text.
        text: String,
        /// Underlying parse error.
        #[source]
        source: AddrParseError,
    },

    /// The requested path is not an origin-form HTTP path.
    #[error("path {path:?} must start with '/' and contain no whitespace or control characters")]
    InvalidPath {
        /// The rejected path.
        path: String,
    },

    /// No connection to the endpoint could be opened.
    #[error("could not connect to {addr}: {source}")]
    Connect {
        /// Address the probe connected to.
        addr: SocketAddr,
        /// Underlying I/O error.
        #[source]
        source: io::Error,
    },

    /// The endpoint did not answer within the probe timeout.
    #[error("{addr} did not answer within {timeout:?}")]
    TimedOut {
        /// Address the probe connected to.
        addr: SocketAddr,
        /// Probe timeout that elapsed.
        timeout: Duration,
    },

    /// Sending the request or reading the response failed.
    #[error("request to {addr} failed: {source}")]
    Transport {
        /// Address the probe connected to.
        addr: SocketAddr,
        /// Underlying I/O error.
        #[source]
        source: io::Error,
    },

    /// The endpoint answered with something other than an HTTP/1.x status
    /// line.
    #[error("{addr} answered with a response that is not HTTP/1.x")]
    MalformedResponse {
        /// Address the probe connected to.
        addr: SocketAddr,
    },

    /// The endpoint answered with a status other than 200.
    #[error("{addr}{path} answered HTTP {status}{}", body_suffix(.body_excerpt))]
    UnhealthyStatus {
        /// Address the probe connected to.
        addr: SocketAddr,
        /// Requested path.
        path: String,
        /// HTTP status code the endpoint returned.
        status: u16,
        /// Start of the response body on one line, possibly empty.
        body_excerpt: String,
    },
}

fn body_suffix(body_excerpt: &str) -> String {
    if body_excerpt.is_empty() {
        String::new()
    } else {
        format!(": {body_excerpt}")
    }
}

/// Selects the operational endpoint address the healthcheck probes.
///
/// `addr_flag` wins over `environment_value`, the value of
/// `ZINDER_OPS__LISTEN_ADDR`, which wins over `default_ops_listen_addr`. An
/// empty environment value disables the endpoint, as it does for the service.
pub fn resolve_ops_healthcheck_addr(
    addr_flag: Option<SocketAddr>,
    environment_value: Option<&str>,
    default_ops_listen_addr: &str,
) -> Result<SocketAddr, OpsHealthcheckError> {
    if let Some(addr) = addr_flag {
        return Ok(addr);
    }
    let (origin, text) = match environment_value {
        Some(environment_value) if environment_value.trim().is_empty() => {
            return Err(OpsHealthcheckError::EndpointDisabled);
        }
        Some(environment_value) => (OPS_LISTEN_ADDR_ENV_VAR, environment_value.trim()),
        None => ("default ops address", default_ops_listen_addr),
    };
    text.parse()
        .map_err(|source| OpsHealthcheckError::InvalidAddress {
            origin,
            text: text.to_owned(),
            source,
        })
}

/// Sends one `GET path` to the operational endpoint at `addr` and succeeds
/// only on status 200.
///
/// An unspecified address probes loopback: `0.0.0.0` probes `127.0.0.1`, and
/// `[::]` probes `[::1]`, then `127.0.0.1` when `[::1]` refuses, because a
/// dual-stack listener accepts either and a host without IPv6 loopback still
/// reaches it over IPv4. `timeout` bounds the whole probe.
pub fn probe_ops_endpoint(
    addr: SocketAddr,
    path: &str,
    timeout: Duration,
) -> Result<(), OpsHealthcheckError> {
    if !is_origin_form_path(path) {
        return Err(OpsHealthcheckError::InvalidPath {
            path: path.to_owned(),
        });
    }
    let deadline = Instant::now() + timeout;
    let (primary, fallback) = probe_targets(addr);
    let (stream, connected_addr) = match (connect(primary, deadline, timeout), fallback) {
        (Ok(stream), _) => (stream, primary),
        (Err(_), Some(fallback)) => (connect(fallback, deadline, timeout)?, fallback),
        (Err(error), None) => return Err(error),
    };
    exchange(stream, connected_addr, path, deadline, timeout)
}

fn is_origin_form_path(path: &str) -> bool {
    path.starts_with('/')
        && !path
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
}

const fn probe_targets(addr: SocketAddr) -> (SocketAddr, Option<SocketAddr>) {
    let port = addr.port();
    match addr.ip() {
        IpAddr::V6(ip) if ip.is_unspecified() => (
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port),
            Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)),
        ),
        IpAddr::V4(ip) if ip.is_unspecified() => {
            (SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port), None)
        }
        IpAddr::V4(_) | IpAddr::V6(_) => (addr, None),
    }
}

fn connect(
    addr: SocketAddr,
    deadline: Instant,
    timeout: Duration,
) -> Result<TcpStream, OpsHealthcheckError> {
    let remaining = remaining_until(deadline, addr, timeout)?;
    TcpStream::connect_timeout(&addr, remaining).map_err(|source| {
        if is_timeout(&source) {
            OpsHealthcheckError::TimedOut { addr, timeout }
        } else {
            OpsHealthcheckError::Connect { addr, source }
        }
    })
}

fn exchange(
    mut stream: TcpStream,
    addr: SocketAddr,
    path: &str,
    deadline: Instant,
    timeout: Duration,
) -> Result<(), OpsHealthcheckError> {
    let transport_error = |source: io::Error| {
        if is_timeout(&source) {
            OpsHealthcheckError::TimedOut { addr, timeout }
        } else {
            OpsHealthcheckError::Transport { addr, source }
        }
    };
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nUser-Agent: zinder-healthcheck\r\nAccept: */*\r\nConnection: close\r\n\r\n"
    );
    stream
        .set_write_timeout(Some(remaining_until(deadline, addr, timeout)?))
        .map_err(transport_error)?;
    stream
        .write_all(request.as_bytes())
        .map_err(transport_error)?;

    let mut response = Vec::with_capacity(1024);
    let mut chunk = [0_u8; 1024];
    let mut status = None;
    loop {
        if status.is_none() {
            status = parse_status(&response, addr)?;
            if status == Some(200) {
                return Ok(());
            }
        }
        if let Some(status) = status
            && body_excerpt_is_complete(&response)
        {
            return Err(unhealthy_status(addr, path, status, &response));
        }
        if status.is_none() && response.len() >= MAX_RESPONSE_HEAD_BYTES {
            return Err(OpsHealthcheckError::MalformedResponse { addr });
        }
        let read_outcome = remaining_until(deadline, addr, timeout).and_then(|remaining| {
            stream
                .set_read_timeout(Some(remaining))
                .and_then(|()| stream.read(&mut chunk))
                .map_err(transport_error)
        });
        match (read_outcome, status) {
            (Ok(0), None) => return Err(OpsHealthcheckError::MalformedResponse { addr }),
            (Ok(0) | Err(_), Some(status)) => {
                return Err(unhealthy_status(addr, path, status, &response));
            }
            (Ok(read_bytes), _) => {
                response.extend_from_slice(chunk.get(..read_bytes).unwrap_or_default());
            }
            (Err(error), None) => return Err(error),
        }
    }
}

/// Parses the status code once the response head is complete.
fn parse_status(response: &[u8], addr: SocketAddr) -> Result<Option<u16>, OpsHealthcheckError> {
    let Some(head_end) = find_subslice(response, RESPONSE_HEAD_TERMINATOR) else {
        return Ok(None);
    };
    let head = response.get(..head_end).unwrap_or_default();
    let status_line = head.split(|byte| *byte == b'\n').next().unwrap_or_default();
    let status_line = String::from_utf8_lossy(status_line);
    let mut fields = status_line.trim_end().split(' ');
    let version = fields.next().unwrap_or_default();
    let status = fields.next().unwrap_or_default();
    if !version.starts_with("HTTP/1.") || status.len() != 3 {
        return Err(OpsHealthcheckError::MalformedResponse { addr });
    }
    status
        .parse()
        .map(Some)
        .map_err(|_| OpsHealthcheckError::MalformedResponse { addr })
}

fn body_excerpt_is_complete(response: &[u8]) -> bool {
    response_body(response).len() >= MAX_BODY_EXCERPT_BYTES
}

fn response_body(response: &[u8]) -> &[u8] {
    find_subslice(response, RESPONSE_HEAD_TERMINATOR)
        .and_then(|head_end| response.get(head_end + RESPONSE_HEAD_TERMINATOR.len()..))
        .unwrap_or_default()
}

fn unhealthy_status(
    addr: SocketAddr,
    path: &str,
    status: u16,
    response: &[u8],
) -> OpsHealthcheckError {
    let body = response_body(response);
    let body = body.get(..MAX_BODY_EXCERPT_BYTES).unwrap_or(body);
    let body_excerpt = String::from_utf8_lossy(body)
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>()
        .trim()
        .to_owned();
    OpsHealthcheckError::UnhealthyStatus {
        addr,
        path: path.to_owned(),
        status,
        body_excerpt,
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn remaining_until(
    deadline: Instant,
    addr: SocketAddr,
    timeout: Duration,
) -> Result<Duration, OpsHealthcheckError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(OpsHealthcheckError::TimedOut { addr, timeout })
    } else {
        Ok(remaining)
    }
}

fn is_timeout(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    )
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener},
        thread::{self, JoinHandle},
        time::Duration,
    };

    use super::{
        OpsHealthcheckError, probe_ops_endpoint, probe_targets, resolve_ops_healthcheck_addr,
    };

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    const TEST_TIMEOUT: Duration = Duration::from_secs(2);

    /// Accepts one connection, records the request head, and answers with
    /// `response` before closing.
    fn serve_once(
        listener: TcpListener,
        response: &'static [u8],
    ) -> JoinHandle<std::io::Result<String>> {
        thread::spawn(move || {
            let (mut stream, _) = listener.accept()?;
            let mut request = Vec::new();
            let mut chunk = [0_u8; 512];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read_bytes = stream.read(&mut chunk)?;
                if read_bytes == 0 {
                    break;
                }
                request.extend_from_slice(chunk.get(..read_bytes).unwrap_or_default());
            }
            stream.write_all(response)?;
            Ok(String::from_utf8_lossy(&request).into_owned())
        })
    }

    fn join_request(handle: JoinHandle<std::io::Result<String>>) -> Result<String, String> {
        handle
            .join()
            .map_err(|_| "stub server thread panicked".to_owned())?
            .map_err(|error| error.to_string())
    }

    #[test]
    fn status_200_is_healthy_and_the_request_closes_the_connection() -> TestResult {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = serve_once(
            listener,
            b"HTTP/1.1 200 OK\r\ncontent-length: 18\r\n\r\n{\"status\":\"ready\"}",
        );

        probe_ops_endpoint(addr, "/readyz", TEST_TIMEOUT)?;

        let request = join_request(server)?;
        assert!(request.starts_with("GET /readyz HTTP/1.1\r\n"), "{request}");
        assert!(request.contains("\r\nConnection: close\r\n"), "{request}");
        assert!(
            request.contains(&format!("\r\nHost: {addr}\r\n")),
            "{request}"
        );
        Ok(())
    }

    #[test]
    fn a_non_200_status_is_unhealthy_and_reports_the_body() -> TestResult {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = serve_once(
            listener,
            b"HTTP/1.1 503 Service Unavailable\r\ncontent-length: 40\r\n\r\n{\"status\":\"not_ready\",\"cause\":\"syncing\"}",
        );

        let outcome = probe_ops_endpoint(addr, "/readyz", TEST_TIMEOUT);

        join_request(server)?;
        match &outcome {
            Err(
                error @ OpsHealthcheckError::UnhealthyStatus {
                    status,
                    body_excerpt,
                    ..
                },
            ) => {
                assert_eq!(*status, 503);
                assert!(body_excerpt.contains("not_ready"), "{body_excerpt}");
                let message = error.to_string();
                assert!(
                    message.contains("/readyz answered HTTP 503: {"),
                    "{message}"
                );
                Ok(())
            }
            Ok(()) | Err(_) => Err(format!("expected an unhealthy status, got {outcome:?}").into()),
        }
    }

    #[test]
    fn a_redirect_is_not_healthy() -> TestResult {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = serve_once(
            listener,
            b"HTTP/1.1 301 Moved Permanently\r\nlocation: /readyz\r\ncontent-length: 0\r\n\r\n",
        );

        let outcome = probe_ops_endpoint(addr, "/", TEST_TIMEOUT);

        join_request(server)?;
        assert!(
            matches!(
                outcome,
                Err(OpsHealthcheckError::UnhealthyStatus { status: 301, ref body_excerpt, .. })
                    if body_excerpt.is_empty()
            ),
            "{outcome:?}"
        );
        Ok(())
    }

    #[test]
    fn a_refused_connection_is_unhealthy() -> TestResult {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        drop(listener);

        let outcome = probe_ops_endpoint(addr, "/readyz", TEST_TIMEOUT);

        assert!(
            matches!(outcome, Err(OpsHealthcheckError::Connect { addr: refused, .. }) if refused == addr),
            "{outcome:?}"
        );
        Ok(())
    }

    #[test]
    fn an_endpoint_that_never_answers_times_out() -> TestResult {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let timeout = Duration::from_millis(200);

        let outcome = probe_ops_endpoint(addr, "/readyz", timeout);

        assert!(
            matches!(outcome, Err(OpsHealthcheckError::TimedOut { timeout: elapsed, .. }) if elapsed == timeout),
            "{outcome:?}"
        );
        drop(listener);
        Ok(())
    }

    #[test]
    fn a_response_that_is_not_http_is_malformed() -> TestResult {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = serve_once(listener, b"SSH-2.0-OpenSSH_9.6\r\n\r\n");

        let outcome = probe_ops_endpoint(addr, "/readyz", TEST_TIMEOUT);

        join_request(server)?;
        assert!(
            matches!(outcome, Err(OpsHealthcheckError::MalformedResponse { .. })),
            "{outcome:?}"
        );
        Ok(())
    }

    #[test]
    fn an_unspecified_ipv4_address_probes_ipv4_loopback() -> TestResult {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        let server = serve_once(listener, b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n");

        probe_ops_endpoint(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port),
            "/healthz",
            TEST_TIMEOUT,
        )?;

        join_request(server)?;
        Ok(())
    }

    #[test]
    fn an_unspecified_ipv6_address_reaches_an_ipv4_only_listener() -> TestResult {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        let server = serve_once(listener, b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n");

        probe_ops_endpoint(
            SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port),
            "/readyz",
            TEST_TIMEOUT,
        )?;

        join_request(server)?;
        Ok(())
    }

    #[test]
    fn unspecified_addresses_map_to_loopback_targets() {
        let v6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 9106);
        let v4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 9105);
        let explicit = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 9107);

        assert_eq!(
            probe_targets(v6),
            (
                SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 9106),
                Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9106)),
            )
        );
        assert_eq!(
            probe_targets(v4),
            (SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9105), None)
        );
        assert_eq!(probe_targets(explicit), (explicit, None));
    }

    #[test]
    fn paths_that_could_inject_request_bytes_are_rejected() {
        for path in ["readyz", "/readyz HTTP/1.0", "/readyz\r\nX-Injected: 1", ""] {
            let outcome = probe_ops_endpoint(
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9),
                path,
                TEST_TIMEOUT,
            );
            assert!(
                matches!(outcome, Err(OpsHealthcheckError::InvalidPath { .. })),
                "{path:?}: {outcome:?}"
            );
        }
    }

    #[test]
    fn the_flag_wins_over_the_environment_and_the_default() -> TestResult {
        let flag = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19106);

        let resolved =
            resolve_ops_healthcheck_addr(Some(flag), Some("[::]:9106"), "127.0.0.1:9106")?;

        assert_eq!(resolved, flag);
        Ok(())
    }

    #[test]
    fn the_environment_wins_over_the_default() -> TestResult {
        let resolved = resolve_ops_healthcheck_addr(None, Some(" [::]:9107 "), "127.0.0.1:9106")?;

        assert_eq!(
            resolved,
            SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 9107)
        );
        Ok(())
    }

    #[test]
    fn the_default_applies_without_a_flag_or_environment() -> TestResult {
        let resolved = resolve_ops_healthcheck_addr(None, None, "127.0.0.1:9110")?;

        assert_eq!(
            resolved,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9110)
        );
        Ok(())
    }

    #[test]
    fn an_empty_environment_value_means_the_endpoint_is_disabled() {
        let outcome = resolve_ops_healthcheck_addr(None, Some("  "), "127.0.0.1:9106");

        assert!(
            matches!(outcome, Err(OpsHealthcheckError::EndpointDisabled)),
            "{outcome:?}"
        );
    }

    #[test]
    fn an_invalid_environment_value_names_its_origin() {
        let outcome = resolve_ops_healthcheck_addr(None, Some("localhost:9106"), "127.0.0.1:9106");

        assert!(
            matches!(
                outcome,
                Err(OpsHealthcheckError::InvalidAddress {
                    origin: "ZINDER_OPS__LISTEN_ADDR",
                    ..
                })
            ),
            "{outcome:?}"
        );
    }
}
