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

use rustls::{
    RootCertStore,
    pki_types::{CertificateDer, pem::PemObject as _},
};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Uri};

/// Optional TLS overrides for an `https` endpoint behind a private CA or
/// reached through a name other than its certificate's.
#[derive(Clone, Debug, Default)]
pub(super) struct TlsOptions {
    pub(super) ca_cert_pem: Option<Vec<u8>>,
    pub(super) domain_name: Option<String>,
}

/// Checks `url` and returns an endpoint that opens one connection per
/// `connect` call.
///
/// An `https` URL uses TLS with the platform's trusted roots, or with only
/// the `tls_options` CA when one is given, so a host whose platform trust
/// store is empty or unreadable still reaches a server behind a private CA.
/// A `tls_options` domain name is both the TLS server name
/// and the HTTP/2 `:authority`, so a proxy that routes by virtual host, such
/// as Caddy, matches the request while the connection dials the URL's
/// address. An `http` URL stays plaintext HTTP/2 and refuses TLS overrides.
pub(super) fn wallet_endpoint(
    url: &str,
    connect_timeout: Duration,
    tls_options: &TlsOptions,
) -> Result<Endpoint, String> {
    let mut endpoint = Endpoint::from_shared(url.to_owned())
        .map_err(|error| error.to_string())?
        .connect_timeout(connect_timeout);
    let is_https = endpoint.uri().scheme_str() == Some("https");
    if !is_https && (tls_options.ca_cert_pem.is_some() || tls_options.domain_name.is_some()) {
        return Err("--tls-ca-cert and --tls-domain-name require an https:// endpoint".to_owned());
    }
    if !is_https {
        return Ok(endpoint);
    }
    // A workspace build enables more than one rustls crypto backend, and rustls
    // then refuses to pick a process default, so install ring explicitly. An
    // error only means a provider is already installed.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut tls = match &tls_options.ca_cert_pem {
        Some(ca_cert_pem) => {
            require_usable_ca_certificate(ca_cert_pem)?;
            ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca_cert_pem))
        }
        None => ClientTlsConfig::new().with_enabled_roots(),
    };
    if let Some(domain_name) = &tls_options.domain_name {
        let origin = virtual_host_origin(endpoint.uri(), domain_name)?;
        tls = tls.domain_name(domain_name);
        endpoint = endpoint.origin(origin);
    }
    endpoint.tls_config(tls).map_err(|error| error.to_string())
}

/// The `https` origin naming `domain_name`, keeping the dialed URL's port
/// when it is not the default 443, as an HTTP client's `Host` would.
fn virtual_host_origin(dialed: &Uri, domain_name: &str) -> Result<Uri, String> {
    let origin = match dialed.port_u16() {
        Some(port) if port != 443 => format!("https://{domain_name}:{port}"),
        Some(_) | None => format!("https://{domain_name}"),
    };
    origin
        .parse()
        .map_err(|error| format!("--tls-domain-name `{domain_name}`: {error}"))
}

/// Fails unless `pem` holds at least one certificate usable as a trust
/// anchor.
///
/// tonic skips a certificate it cannot parse, so an unusable file
/// would otherwise leave the endpoint with no trust anchor and fail only at
/// the first handshake.
fn require_usable_ca_certificate(pem: &[u8]) -> Result<(), String> {
    let certificates = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("--tls-ca-cert is not a readable PEM file: {error}"))?;
    let (usable, _unusable) = RootCertStore::empty().add_parsable_certificates(certificates);
    if usable == 0 {
        return Err("--tls-ca-cert holds no usable CA certificate".to_owned());
    }
    Ok(())
}

/// Opens one new HTTP/2 connection through `endpoint`, failing when the TCP
/// connection and any TLS handshake together take longer than
/// `connect_timeout`.
///
/// The endpoint's own connect timeout bounds only the TCP connection, so a
/// server that accepts it and never answers the TLS handshake would
/// otherwise hold the wallet forever.
pub(super) async fn connect(
    endpoint: &Endpoint,
    connect_timeout: Duration,
) -> Result<Channel, String> {
    tokio::time::timeout(connect_timeout, endpoint.connect())
        .await
        .map_err(|_| {
            format!(
                "connection to {} did not open within {}s",
                endpoint.uri(),
                connect_timeout.as_secs_f64()
            )
        })?
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc, time::Duration};

    use parking_lot::Mutex;
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, DnType, IsCa, KeyPair,
        KeyUsagePurpose,
    };
    use tokio::{io::AsyncReadExt as _, net::TcpListener, sync::oneshot};
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::{Identity, Server, ServerTlsConfig};
    use tower::util::MapRequestLayer;
    use zinder_proto::compat::lightwalletd::{
        Empty, compact_tx_streamer_client::CompactTxStreamerClient,
        compact_tx_streamer_server::CompactTxStreamerServer,
    };

    use super::{TlsOptions, connect, wallet_endpoint};
    use crate::wallet_load::mock_streamer::{MockStreamer, RangeEnding};

    type TestResult = Result<(), Box<dyn Error>>;

    /// The name a TLS proxy's virtual host serves, unlike the dialed address.
    const VIRTUAL_HOST: &str = "wallet.zinder.test";

    /// Connects to a listener that records the first byte the client sends.
    async fn first_byte_sent(scheme: &str) -> Result<u8, Box<dyn Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = wallet_endpoint(
            &format!("{scheme}://{}", listener.local_addr()?),
            Duration::from_secs(5),
            &TlsOptions::default(),
        )?;
        let client = tokio::spawn(async move { connect(&endpoint, Duration::from_secs(5)).await });
        let (mut socket, _) = listener.accept().await?;
        let mut first = [0_u8; 1];
        socket.read_exact(&mut first).await?;
        drop(socket);
        client.abort();
        Ok(first[0])
    }

    /// A self-signed CA and a server certificate it issued for
    /// [`VIRTUAL_HOST`] only, as PEM.
    struct TestPki {
        ca_cert: String,
        server_cert: String,
        server_key: String,
    }

    impl TestPki {
        fn generate() -> Result<Self, Box<dyn Error>> {
            let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
            ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            ca_params
                .distinguished_name
                .push(DnType::CommonName, "wallet load test CA");
            ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
            let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate()?)?;
            let server_key = KeyPair::generate()?;
            let server_cert = CertificateParams::new(vec![VIRTUAL_HOST.to_owned()])?
                .signed_by(&server_key, &ca)?;
            Ok(Self {
                ca_cert: ca.pem(),
                server_cert: server_cert.pem(),
                server_key: server_key.serialize_pem(),
            })
        }
    }

    #[tokio::test]
    async fn https_urls_open_with_a_tls_handshake() -> TestResult {
        // 0x16 starts a TLS handshake record.
        assert_eq!(first_byte_sent("https").await?, 0x16);
        Ok(())
    }

    #[tokio::test]
    async fn http_urls_stay_plaintext_http2() -> TestResult {
        // The HTTP/2 connection preface starts with "PRI".
        assert_eq!(first_byte_sent("http").await?, b'P');
        Ok(())
    }

    #[tokio::test]
    async fn a_server_that_never_answers_the_tls_handshake_times_out() -> TestResult {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let connect_timeout = Duration::from_millis(200);
        let endpoint = wallet_endpoint(
            &format!("https://{}", listener.local_addr()?),
            connect_timeout,
            &TlsOptions::default(),
        )?;
        // Accept the TCP connection and hold it open without a reply.
        let silent = tokio::spawn(async move {
            let accepted = listener.accept().await;
            tokio::time::sleep(Duration::from_secs(30)).await;
            drop(accepted);
        });

        let outcome =
            tokio::time::timeout(Duration::from_secs(10), connect(&endpoint, connect_timeout))
                .await;
        silent.abort();

        let refused = outcome.map_err(|_| "connect hung past the test's own deadline")?;
        assert!(
            refused.is_err_and(|reason| reason.contains("did not open within")),
            "the stalled handshake fails with the connect timeout"
        );
        Ok(())
    }

    #[test]
    fn tls_overrides_apply_to_https_endpoints_only() -> TestResult {
        let tls_options = TlsOptions {
            ca_cert_pem: Some(TestPki::generate()?.ca_cert.into_bytes()),
            domain_name: Some("wallet.local".to_owned()),
        };
        let timeout = Duration::from_secs(5);

        assert!(wallet_endpoint("http://127.0.0.1:9067", timeout, &tls_options).is_err());
        assert!(wallet_endpoint("https://127.0.0.1:443", timeout, &tls_options).is_ok());
        Ok(())
    }

    #[test]
    fn a_ca_file_without_a_usable_certificate_is_refused() {
        let timeout = Duration::from_secs(5);
        for unusable in [
            &b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"[..],
            b"not a certificate\n",
            b"",
        ] {
            let tls_options = TlsOptions {
                ca_cert_pem: Some(unusable.to_vec()),
                domain_name: None,
            };

            let refused = wallet_endpoint("https://127.0.0.1:443", timeout, &tls_options).err();

            assert!(
                refused.is_some_and(|reason| reason.contains("--tls-ca-cert")),
                "{}",
                String::from_utf8_lossy(unusable)
            );
        }
    }

    /// Set in the child process that
    /// [`a_supplied_ca_works_without_platform_roots`] starts with an empty
    /// platform trust store.
    const EMPTY_TRUST_STORE_CHILD: &str = "ZINDER_TEST_EMPTY_TRUST_STORE_CHILD";

    /// Reruns one test of this binary in a child process whose platform
    /// trust store, as `rustls-native-certs` reads it, is empty, so the
    /// environment change stays out of every other test.
    #[test]
    fn a_supplied_ca_works_without_platform_roots() -> TestResult {
        let timeout = Duration::from_secs(5);
        if std::env::var_os(EMPTY_TRUST_STORE_CHILD).is_some() {
            let tls_options = TlsOptions {
                ca_cert_pem: Some(TestPki::generate()?.ca_cert.into_bytes()),
                domain_name: Some(VIRTUAL_HOST.to_owned()),
            };
            let platform_roots =
                wallet_endpoint("https://127.0.0.1:443", timeout, &TlsOptions::default());
            assert!(
                platform_roots.is_err(),
                "the child's platform trust store is empty"
            );
            wallet_endpoint("https://127.0.0.1:443", timeout, &tls_options)?;
            return Ok(());
        }
        let test_name = format!(
            "{}::a_supplied_ca_works_without_platform_roots",
            module_path!()
                .split_once("::")
                .map_or(module_path!(), |(_, path)| path)
        );
        let child = std::process::Command::new(std::env::current_exe()?)
            .args([test_name.as_str(), "--exact", "--nocapture"])
            .env(EMPTY_TRUST_STORE_CHILD, "1")
            .env("SSL_CERT_FILE", "/dev/null")
            .env("SSL_CERT_DIR", "/dev/null")
            .output()?;
        let stdout = String::from_utf8_lossy(&child.stdout);

        assert!(
            child.status.success() && stdout.contains("1 passed"),
            "{stdout}{}",
            String::from_utf8_lossy(&child.stderr)
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_domain_name_sets_the_authority_a_virtual_host_proxy_matches() -> TestResult {
        // The test server builds its TLS config before any client endpoint does.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let pki = TestPki::generate()?;
        let authorities = Arc::new(Mutex::new(Vec::<String>::new()));
        let recorded = Arc::clone(&authorities);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let (shutdown, shutdown_rx) = oneshot::channel::<()>();
        let server = Server::builder()
            .tls_config(
                ServerTlsConfig::new()
                    .identity(Identity::from_pem(&pki.server_cert, &pki.server_key)),
            )?
            .layer(MapRequestLayer::new(
                move |request: http::Request<tonic::body::Body>| {
                    let authority = request
                        .uri()
                        .authority()
                        .map_or_else(String::new, ToString::to_string);
                    recorded.lock().push(authority);
                    request
                },
            ))
            .add_service(CompactTxStreamerServer::new(MockStreamer::new(
                RangeEnding::Clean,
            )))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = shutdown_rx.await;
            });
        let server = tokio::spawn(server);
        let endpoint = wallet_endpoint(
            &format!("https://127.0.0.1:{port}"),
            Duration::from_secs(5),
            &TlsOptions {
                ca_cert_pem: Some(pki.ca_cert.into_bytes()),
                domain_name: Some(VIRTUAL_HOST.to_owned()),
            },
        )?;

        let answered =
            CompactTxStreamerClient::new(connect(&endpoint, Duration::from_secs(5)).await?)
                .get_lightd_info(Empty {})
                .await;
        let _ = shutdown.send(());
        server.await??;

        assert!(answered.is_ok(), "{answered:?}");
        assert_eq!(
            authorities.lock().as_slice(),
            [format!("{VIRTUAL_HOST}:{port}")]
        );
        Ok(())
    }
}
