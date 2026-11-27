//! Loopback tests of the public remote transport settings.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use std::{
    convert::Infallible,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair, PublicKeyData as _,
};
use sha2::{Digest as _, Sha256};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_stream::{Stream, StreamExt as _, wrappers::TcpListenerStream};
use tonic::{
    Request, Response, Status,
    body::Body as TonicBody,
    codegen::{BoxFuture, Service, http},
    server::{NamedService, ServerStreamingService},
    transport::{Identity, Server, ServerTlsConfig},
};
use tonic_types::ErrorDetails;
use zinder_client::{
    BlockHeightRange, ChainEpochId, ChainIndex as _, EndpointBackedIndex as _, IndexerError,
    Network, RawTransactionBytes, RemoteChainIndex, RemoteClientOptions, RemoteOpenOptions,
    RetryPolicy,
};
use zinder_proto::{
    status_with_reason,
    v1::{
        ops::ErrorReason,
        wallet::{self, compact_blocks_in_range_chunk::Body as RangeChunk},
    },
    wire::{chain_epoch_message, compact_block_message},
};
use zinder_testkit::ChainFixture;

use super::remote_chain_index::spawn_transport_wallet_query;

fn connect(
    endpoint: String,
    options: RemoteClientOptions,
) -> Result<RemoteChainIndex, IndexerError> {
    RemoteChainIndex::connect_with_options(
        RemoteOpenOptions {
            endpoint,
            network: Network::ZcashRegtest,
        },
        options,
    )
}

#[tokio::test]
async fn private_ca_and_spki_pin_are_verified_together() -> eyre::Result<()> {
    // Workspace test dependencies enable multiple rustls providers.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let other_ca = CertifiedIssuer::self_signed(ca_params.clone(), KeyPair::generate()?)?;
    let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate()?)?;
    let key = KeyPair::generate()?;
    let leaf =
        CertificateParams::new(vec!["wallet.zinder.test".to_owned()])?.signed_by(&key, &ca)?;
    let pin = Sha256::digest(key.subject_public_key_info()).into();
    let tls = ServerTlsConfig::new().identity(Identity::from_pem(leaf.pem(), key.serialize_pem()));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("https://{}", listener.local_addr()?);
    let server = spawn_transport_wallet_query(listener, Some(tls), Ok)?;

    let named = RemoteClientOptions::default().with_tls_domain_name("wallet.zinder.test");
    let pem = named.clone().with_tls_ca_pem(ca.pem().into_bytes());
    let der = named
        .clone()
        .with_tls_ca_der([ca.der().to_vec()])
        .with_tls_spki_pin(pin);
    for options in [pem.clone(), der.clone()] {
        let index = connect(endpoint.clone(), options)?;
        assert_eq!(index.server_info().await?.network, Network::ZcashRegtest);
    }

    for options in [
        pem.with_tls_spki_pin([0; 32]),
        der.with_tls_domain_name("wrong.zinder.test"),
        named
            .with_tls_ca_der([other_ca.der().to_vec()])
            .with_tls_spki_pin(pin),
    ] {
        let index = connect(endpoint.clone(), options)?;
        assert!(index.server_info().await.is_err());
    }
    server.abort();
    Ok(())
}

#[tokio::test]
async fn socks5h_resolves_at_the_proxy_even_after_reconnecting() -> eyre::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let wallet_addr = listener.local_addr()?;
    let wallet_attempts = Arc::new(AtomicUsize::new(0));
    let server = spawn_transport_wallet_query(listener, None, {
        let wallet_attempts = Arc::clone(&wallet_attempts);
        move |request| {
            if wallet_attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                return Err(Status::unavailable("connection needs rebuilding"));
            }
            Ok(request)
        }
    })?;
    let proxy = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_url = format!("socks5h://{}", proxy.local_addr()?);
    let index = connect(
        "http://wallet.zinder.test.onion".to_owned(),
        RemoteClientOptions::default()
            .with_socks5_proxy(proxy_url)
            .with_read_retries(),
    )?;

    tokio::time::timeout(Duration::from_secs(10), async {
        let mut read = Box::pin(index.server_info());
        let tunnels = tokio::select! {
            reply = &mut read => return Err(eyre::eyre!("expected two proxy connections: {reply:?}")),
            tunnels = async {
                Ok::<_, eyre::Report>([
                    spawn_tunnel(&proxy, wallet_addr).await?,
                    spawn_tunnel(&proxy, wallet_addr).await?,
                ])
            } => tunnels?,
        };
        assert_eq!(read.await?.network, Network::ZcashRegtest);
        assert_eq!(wallet_attempts.load(Ordering::Relaxed), 2);
        for tunnel in tunnels {
            tunnel.abort();
        }
        Ok::<_, eyre::Report>(())
    })
    .await??;
    server.abort();
    Ok(())
}

async fn spawn_tunnel(
    proxy: &TcpListener,
    wallet_addr: SocketAddr,
) -> eyre::Result<tokio::task::JoinHandle<eyre::Result<()>>> {
    let (mut peer, _) = proxy.accept().await?;
    let mut greeting = [0; 3];
    peer.read_exact(&mut greeting).await?;
    assert_eq!(greeting, [5, 1, 0]);
    peer.write_all(&[5, 0]).await?;

    let mut header = [0; 5];
    peer.read_exact(&mut header).await?;
    assert_eq!(&header[..4], &[5, 1, 0, 3]);
    let mut hostname = vec![0; usize::from(header[4])];
    peer.read_exact(&mut hostname).await?;
    assert_eq!(hostname, b"wallet.zinder.test.onion");
    assert_eq!(peer.read_u16().await?, 80);
    let mut wallet = TcpStream::connect(wallet_addr).await?;
    peer.write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0]).await?;
    Ok(tokio::spawn(async move {
        tokio::io::copy_bidirectional(&mut peer, &mut wallet).await?;
        Ok(())
    }))
}

#[tokio::test]
async fn read_retries_wait_for_positive_pushback_but_broadcast_has_one_attempt() -> eyre::Result<()>
{
    let attempts = Arc::new(AtomicUsize::new(0));
    let (attempt_times, mut observed) = tokio::sync::mpsc::channel(4);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let index = connect(
        format!("http://{}", listener.local_addr()?),
        RemoteClientOptions::default().with_read_retries(),
    )?;
    let failure = transient_failure("350")?;
    let server = spawn_transport_wallet_query(listener, None, {
        let attempts = Arc::clone(&attempts);
        move |request| {
            let _ = attempt_times.try_send(Instant::now());
            if attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                Err(failure.clone())
            } else {
                Ok(request)
            }
        }
    })?;
    assert_eq!(index.server_info().await?.network, Network::ZcashRegtest);
    let first_attempt = observed.try_recv()?;
    let second_attempt = observed.try_recv()?;
    assert!(second_attempt.duration_since(first_attempt) >= Duration::from_millis(350));
    assert_eq!(attempts.load(Ordering::Relaxed), 2);
    server.abort();

    let broadcasts = Arc::new(AtomicUsize::new(0));
    let (index, server) = scripted_retry_endpoint(
        RemoteClientOptions::default().with_read_retries(),
        transient_failure("0")?,
        Arc::clone(&broadcasts),
        usize::MAX,
    )
    .await?;
    let reply = index
        .broadcast_transaction(RawTransactionBytes::new([1]))
        .await;
    assert!(matches!(reply, Err(error) if error.retry_policy() == RetryPolicy::RetryWithBackoff));
    assert_eq!(broadcasts.load(Ordering::Relaxed), 1);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn read_retries_are_opt_in_and_stop_at_the_limit_or_pushback_refusal() -> eyre::Result<()> {
    for (options, pushback, expected_attempts) in [
        (RemoteClientOptions::default(), "0", 1),
        (RemoteClientOptions::default().with_read_retries(), "0", 4),
        (RemoteClientOptions::default().with_read_retries(), "-1", 1),
        (RemoteClientOptions::default().with_read_retries(), "bad", 1),
        (
            RemoteClientOptions::default().with_read_retries(),
            "2001",
            1,
        ),
    ] {
        let attempts = Arc::new(AtomicUsize::new(0));
        let (index, server) = scripted_retry_endpoint(
            options,
            transient_failure(pushback)?,
            Arc::clone(&attempts),
            usize::MAX,
        )
        .await?;
        let reply = index.server_info().await;
        assert!(
            matches!(reply, Err(error) if error.retry_policy() == RetryPolicy::RetryWithBackoff)
        );
        assert_eq!(attempts.load(Ordering::Relaxed), expected_attempts);
        server.abort();
    }
    Ok(())
}

#[tokio::test]
async fn retry_enabled_reads_return_terminal_policies_after_one_attempt() -> eyre::Result<()> {
    for (reason, policy) in [
        (
            ErrorReason::ChainEpochPinUnavailable,
            RetryPolicy::RefreshChainEpoch,
        ),
        (ErrorReason::InvalidBlockRange, RetryPolicy::ClientError),
        (
            ErrorReason::DependencyNotConfigured,
            RetryPolicy::OperatorActionRequired,
        ),
    ] {
        let attempts = Arc::new(AtomicUsize::new(0));
        let (index, server) = scripted_retry_endpoint(
            RemoteClientOptions::default().with_read_retries(),
            status_with_reason(
                reason,
                "scripted terminal read failure",
                ErrorDetails::new(),
            ),
            Arc::clone(&attempts),
            usize::MAX,
        )
        .await?;
        let reply = index.server_info().await;
        assert!(matches!(reply, Err(error) if error.retry_policy() == policy));
        assert_eq!(attempts.load(Ordering::Relaxed), 1, "{reason:?}");
        server.abort();
    }
    Ok(())
}

#[tokio::test]
async fn compact_range_retries_opening_but_surfaces_errors_after_a_block() -> eyre::Result<()> {
    let network = Network::ZcashRegtest;
    let epoch_id = ChainEpochId::new(7);
    let chain = ChainFixture::new(network).extend_blocks(2);
    let epoch = chain
        .chain_epoch(epoch_id)
        .ok_or_else(|| eyre::eyre!("missing fixture epoch"))?;
    let first_block = chain
        .compact_block_artifacts()
        .into_iter()
        .next()
        .ok_or_else(|| eyre::eyre!("missing fixture block"))?;
    let (observed, mut attempts) = mpsc::channel(4);
    let rpc_count = Arc::new(AtomicUsize::new(0));
    let range_rpc = ScriptedCompactRange {
        rpc_count: Arc::clone(&rpc_count),
        observed,
        opening_failure: transient_failure("350")?,
        header: wallet::ChainView {
            chain_epoch: Some(chain_epoch_message(epoch)),
            ..Default::default()
        },
        block: compact_block_message(&first_block),
    };

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let index = connect(
        format!("http://{}", listener.local_addr()?),
        RemoteClientOptions::default().with_read_retries(),
    )?;
    let server = tokio::spawn(
        Server::builder()
            .add_service(range_rpc)
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );

    let range = BlockHeightRange::inclusive(first_block.height(), epoch.visible_tip_height);
    let mut stream = index.compact_blocks_in_range(range, Some(epoch_id)).await?;
    let (first_request, first_attempt) = attempts.try_recv()?;
    let (second_request, second_attempt) = attempts.try_recv()?;
    assert!(second_attempt.duration_since(first_attempt) >= Duration::from_millis(350));
    assert_eq!(
        first_request,
        wallet::CompactBlocksInRangeRequest {
            start_height: range.start.value(),
            end_height: range.end.value(),
            at_epoch_id: Some(epoch_id.value()),
        }
    );
    assert_eq!(second_request, first_request);

    assert!(
        matches!(stream.next().await, Some(Ok(block)) if block.height() == first_block.height())
    );
    assert!(matches!(
        stream.next().await,
        Some(Err(IndexerError::ServiceUnavailable { reason }))
            if reason.contains("range interrupted after the first block")
    ));
    assert!(stream.next().await.is_none());
    assert_eq!(rpc_count.load(Ordering::Relaxed), 2);
    server.abort();
    Ok(())
}

#[derive(Clone)]
struct ScriptedCompactRange {
    rpc_count: Arc<AtomicUsize>,
    observed: mpsc::Sender<(wallet::CompactBlocksInRangeRequest, Instant)>,
    opening_failure: Status,
    header: wallet::ChainView,
    block: wallet::CompactBlock,
}

impl NamedService for ScriptedCompactRange {
    const NAME: &'static str = "zinder.v1.wallet.WalletQuery";
}

impl Service<http::Request<TonicBody>> for ScriptedCompactRange {
    type Response = http::Response<TonicBody>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<TonicBody>) -> Self::Future {
        let range_rpc = self.clone();
        Box::pin(async move {
            Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                .server_streaming(range_rpc, request)
                .await)
        })
    }
}

impl ServerStreamingService<wallet::CompactBlocksInRangeRequest> for ScriptedCompactRange {
    type Response = wallet::CompactBlocksInRangeChunk;
    type ResponseStream = Pin<Box<dyn Stream<Item = Result<Self::Response, Status>> + Send>>;
    type Future = BoxFuture<Response<Self::ResponseStream>, Status>;

    fn call(&mut self, request: Request<wallet::CompactBlocksInRangeRequest>) -> Self::Future {
        let attempt = self.rpc_count.fetch_add(1, Ordering::Relaxed);
        let _ = self
            .observed
            .try_send((request.into_inner(), Instant::now()));
        let Self {
            opening_failure,
            header,
            block,
            ..
        } = self.clone();
        Box::pin(async move {
            if attempt == 0 {
                return Err(opening_failure);
            }
            let chunks = tokio_stream::iter([
                Ok(wallet::CompactBlocksInRangeChunk {
                    body: Some(RangeChunk::Header(header)),
                }),
                Ok(wallet::CompactBlocksInRangeChunk {
                    body: Some(RangeChunk::Item(block)),
                }),
                Err(Status::unavailable(
                    "range interrupted after the first block",
                )),
            ]);
            Ok(Response::new(Box::pin(chunks) as Self::ResponseStream))
        })
    }
}

#[tokio::test]
async fn dropping_a_read_cancels_backoff() -> eyre::Result<()> {
    let attempts = Arc::new(AtomicUsize::new(0));
    let first_attempt = Arc::new(tokio::sync::Notify::new());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let index = connect(
        format!("http://{}", listener.local_addr()?),
        RemoteClientOptions::default().with_read_retries(),
    )?;
    let failure = transient_failure("200")?;
    let server = spawn_transport_wallet_query(listener, None, {
        let first_attempt = Arc::clone(&first_attempt);
        let attempts = Arc::clone(&attempts);
        move |_: Request<()>| {
            attempts.fetch_add(1, Ordering::Relaxed);
            first_attempt.notify_one();
            Err(failure.clone())
        }
    })?;

    {
        let read = index.server_info();
        tokio::pin!(read);
        tokio::select! {
            reply = &mut read => return Err(eyre::eyre!("read unexpectedly completed: {reply:?}")),
            () = first_attempt.notified() => {}
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut read)
                .await
                .is_err()
        );
    }
    tokio::time::sleep(Duration::from_millis(225)).await;
    assert_eq!(attempts.load(Ordering::Relaxed), 1);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn malformed_ca_bundles_fail_before_dialing() -> eyre::Result<()> {
    let mut ca = CertificateParams::default();
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let certificate = ca.self_signed(&KeyPair::generate()?)?.pem();
    let endpoint = "https://127.0.0.1:443";

    let padded_bundle = format!(" \t\r\n{certificate}\r\n \t");
    connect(
        endpoint.to_owned(),
        RemoteClientOptions::default().with_tls_ca_pem(padded_bundle.into_bytes()),
    )?;

    for (case, bundle) in [
        (
            "nested certificate",
            format!("-----BEGIN CERTIFICATE-----\n{certificate}"),
        ),
        ("trailing text", format!("{certificate}\ntrailing junk\n")),
        (
            "truncated certificate",
            format!("{certificate}\n-----BEGIN CERTIFICATE-----\nAAAA\n"),
        ),
    ] {
        let outcome = connect(
            endpoint.to_owned(),
            RemoteClientOptions::default().with_tls_ca_pem(bundle.into_bytes()),
        );
        assert!(
            matches!(outcome, Err(IndexerError::InvalidRequest { reason }) if reason == "invalid TLS CA PEM framing"),
            "{case}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn invalid_transport_settings_fail_before_dialing() -> eyre::Result<()> {
    for options in [
        RemoteClientOptions::default().with_tls_ca_pem(b"invalid".to_vec()),
        RemoteClientOptions::default().with_tls_ca_der([]),
        RemoteClientOptions::default().with_tls_ca_der([vec![1]]),
        RemoteClientOptions::default().with_tls_domain_name("wallet.internal/path"),
    ] {
        assert!(matches!(
            connect("https://127.0.0.1:443".to_owned(), options),
            Err(IndexerError::InvalidRequest { .. })
        ));
    }
    for options in [
        RemoteClientOptions::default().with_tls_spki_pin([0; 32]),
        RemoteClientOptions::default().with_socks5_proxy("http://127.0.0.1:9050"),
        RemoteClientOptions::default().with_socks5_proxy("socks5h://user:secret@127.0.0.1:9050"),
    ] {
        let Err(IndexerError::InvalidRequest { reason }) =
            connect("http://127.0.0.1:80".to_owned(), options)
        else {
            return Err(eyre::eyre!("expected invalid transport settings"));
        };
        assert!(!reason.contains("secret"));
    }
    Ok(())
}

fn transient_failure(pushback: &str) -> eyre::Result<Status> {
    let mut failure = status_with_reason(
        ErrorReason::ServiceNotReady,
        "waiting for the chain tip",
        ErrorDetails::new(),
    );
    failure
        .metadata_mut()
        .insert("grpc-retry-pushback-ms", pushback.parse()?);
    Ok(failure)
}

async fn scripted_retry_endpoint(
    options: RemoteClientOptions,
    failure: Status,
    attempts: Arc<AtomicUsize>,
    failures: usize,
) -> eyre::Result<(
    RemoteChainIndex,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let index = connect(format!("http://{}", listener.local_addr()?), options)?;
    let server = spawn_transport_wallet_query(listener, None, move |request: Request<()>| {
        if attempts.fetch_add(1, Ordering::Relaxed) < failures {
            Err(failure.clone())
        } else {
            Ok(request)
        }
    })?;
    Ok((index, server))
}
