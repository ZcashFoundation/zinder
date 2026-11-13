#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{
    fs,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use eyre::{Result, eyre};
use futures_util::{Stream, StreamExt as _, stream};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use tempfile::TempDir;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    Request, Response, Status,
    transport::{Certificate, Identity, Server, ServerTlsConfig},
};
use zinder_proto::external::zebra_indexer_rpc::{
    BlockAndHash, BlockHashAndHeight, BlockRequest, Empty, MempoolChangeMessage,
    NonFinalizedStateChangeRequest,
    indexer_server::{Indexer, IndexerServer},
};
use zinder_source::{
    NodeIndexerTls, ZebraIndexerChainTipSource, ZebraIndexerSourceTarget, ZebraTransportError,
    validate_zebra_indexer_endpoint,
};

const TIP_HEIGHT: u32 = 42;
/// DNS name the server certificate carries besides `127.0.0.1`.
const INDEXER_DNS_NAME: &str = "indexer.zinder.test";
const SUBSCRIBE_DEADLINE: Duration = Duration::from_secs(10);

type ChainTipStream =
    Pin<Box<dyn Stream<Item = Result<BlockHashAndHeight, Status>> + Send + 'static>>;
type BlockStream = Pin<Box<dyn Stream<Item = Result<BlockAndHash, Status>> + Send + 'static>>;
type MempoolStream =
    Pin<Box<dyn Stream<Item = Result<MempoolChangeMessage, Status>> + Send + 'static>>;

/// Indexer that answers `ChainTipChange` with one tip and keeps the stream open.
#[derive(Clone, Default)]
struct ChainTipService {
    served_calls: Arc<AtomicU64>,
}

#[tonic::async_trait]
impl Indexer for ChainTipService {
    type ChainTipChangeStream = ChainTipStream;
    type NonFinalizedStateChangeStream = BlockStream;
    type MempoolChangeStream = MempoolStream;

    async fn chain_tip_change(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ChainTipChangeStream>, Status> {
        self.served_calls.fetch_add(1, Ordering::Relaxed);
        let tip = BlockHashAndHeight {
            hash: vec![7; 32],
            height: TIP_HEIGHT,
        };
        Ok(Response::new(Box::pin(
            stream::iter([Ok(tip)]).chain(stream::pending()),
        )))
    }

    async fn non_finalized_state_change(
        &self,
        _request: Request<NonFinalizedStateChangeRequest>,
    ) -> Result<Response<Self::NonFinalizedStateChangeStream>, Status> {
        Err(Status::unimplemented(
            "test server exposes only ChainTipChange",
        ))
    }

    async fn mempool_change(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::MempoolChangeStream>, Status> {
        Err(Status::unimplemented(
            "test server exposes only ChainTipChange",
        ))
    }

    async fn get_block(
        &self,
        _request: Request<BlockRequest>,
    ) -> Result<Response<BlockAndHash>, Status> {
        Err(Status::unimplemented(
            "test server exposes only ChainTipChange",
        ))
    }
}

/// Throwaway PKI written to PEM files: the node's CA, a server leaf for
/// `127.0.0.1` and [`INDEXER_DNS_NAME`], a client leaf from the same CA, and a client leaf from an
/// unrelated CA.
struct TestPki {
    directory: TempDir,
    server_cert_pem: String,
    server_key_pem: String,
}

impl TestPki {
    fn generate() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let node_ca = certificate_authority("indexer test CA")?;
        let other_ca = certificate_authority("unrelated test CA")?;
        let (server_cert_pem, server_key_pem) = leaf_certificate(
            &node_ca,
            &["127.0.0.1", INDEXER_DNS_NAME],
            ExtendedKeyUsagePurpose::ServerAuth,
        )?;
        let (client_cert_pem, client_key_pem) = leaf_certificate(
            &node_ca,
            &["zinder-ingest"],
            ExtendedKeyUsagePurpose::ClientAuth,
        )?;
        let (untrusted_cert_pem, untrusted_key_pem) = leaf_certificate(
            &other_ca,
            &["zinder-ingest"],
            ExtendedKeyUsagePurpose::ClientAuth,
        )?;

        let pki = Self {
            directory,
            server_cert_pem,
            server_key_pem,
        };
        fs::write(pki.path("ca.pem"), node_ca.pem())?;
        fs::write(pki.path("client.pem"), client_cert_pem)?;
        fs::write(pki.path("client.key"), client_key_pem)?;
        fs::write(pki.path("untrusted-client.pem"), untrusted_cert_pem)?;
        fs::write(pki.path("untrusted-client.key"), untrusted_key_pem)?;
        Ok(pki)
    }

    fn path(&self, file_name: &str) -> PathBuf {
        self.directory.path().join(file_name)
    }

    fn ca_cert(&self) -> PathBuf {
        self.path("ca.pem")
    }

    fn trusted_identity(&self) -> NodeIndexerTls {
        NodeIndexerTls::new()
            .with_ca_cert_path(self.ca_cert())
            .with_client_identity(self.path("client.pem"), self.path("client.key"))
    }
}

fn certificate_authority(common_name: &str) -> Result<CertifiedIssuer<'static, KeyPair>> {
    let mut params = CertificateParams::new(Vec::<String>::new())?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    Ok(CertifiedIssuer::self_signed(params, KeyPair::generate()?)?)
}

fn leaf_certificate(
    issuer: &CertifiedIssuer<'static, KeyPair>,
    subject_names: &[&str],
    usage: ExtendedKeyUsagePurpose,
) -> Result<(String, String)> {
    let mut params = CertificateParams::new(
        subject_names
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<Vec<_>>(),
    )?;
    if let Some(common_name) = subject_names.first() {
        params
            .distinguished_name
            .push(DnType::CommonName, *common_name);
    }
    params.extended_key_usages = vec![usage];
    let key = KeyPair::generate()?;
    let certificate = params.signed_by(&key, issuer)?;
    Ok((certificate.pem(), key.serialize_pem()))
}

struct RunningIndexer {
    endpoint_url: String,
    served_calls: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Drop for RunningIndexer {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// Starts an indexer that requires a client certificate issued by the test CA.
async fn start_mutual_tls_indexer(pki: &TestPki) -> Result<RunningIndexer> {
    // The workspace builds rustls with two crypto providers, so the server
    // side needs a process default; the client falls back to ring on its own.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let service = ChainTipService::default();
    let served_calls = Arc::clone(&service.served_calls);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let tls = ServerTlsConfig::new()
        .identity(Identity::from_pem(
            &pki.server_cert_pem,
            &pki.server_key_pem,
        ))
        .client_ca_root(Certificate::from_pem(fs::read(pki.ca_cert())?));
    let server = tokio::spawn(
        Server::builder()
            .tls_config(tls)?
            .add_service(IndexerServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    Ok(RunningIndexer {
        endpoint_url: format!("https://{address}"),
        served_calls,
        server,
    })
}

async fn first_tip(target: ZebraIndexerSourceTarget) -> Result<u32> {
    let mut notifications = tokio::time::timeout(
        SUBSCRIBE_DEADLINE,
        ZebraIndexerChainTipSource::new(target).subscribe(),
    )
    .await??;
    let notification = tokio::time::timeout(SUBSCRIBE_DEADLINE, notifications.next())
        .await?
        .ok_or_else(|| eyre!("chain-tip stream ended before its first notification"))??;
    Ok(notification.tip_id.height.value())
}

async fn assert_refused(indexer: &RunningIndexer, target: ZebraIndexerSourceTarget) -> Result<()> {
    let outcome = first_tip(target).await;
    assert!(
        outcome.is_err(),
        "the indexer served a client it must refuse: {outcome:?}"
    );
    assert_eq!(indexer.served_calls.load(Ordering::Relaxed), 0);
    Ok(())
}

fn write_file(path: &Path, contents: &str) -> Result<PathBuf> {
    fs::write(path, contents)?;
    Ok(path.to_path_buf())
}

#[tokio::test]
async fn mutual_tls_identity_streams_chain_tip_notifications() -> Result<()> {
    let pki = TestPki::generate()?;
    let indexer = start_mutual_tls_indexer(&pki).await?;
    let target = ZebraIndexerSourceTarget::new(indexer.endpoint_url.clone())
        .with_tls(pki.trusted_identity());

    validate_zebra_indexer_endpoint(&target)?;
    assert_eq!(first_tip(target).await?, TIP_HEIGHT);
    assert_eq!(indexer.served_calls.load(Ordering::Relaxed), 1);
    Ok(())
}

async fn first_tip_from(source: &ZebraIndexerChainTipSource) -> Result<u32> {
    let mut notifications = tokio::time::timeout(SUBSCRIBE_DEADLINE, source.subscribe()).await??;
    let notification = tokio::time::timeout(SUBSCRIBE_DEADLINE, notifications.next())
        .await?
        .ok_or_else(|| eyre!("chain-tip stream ended before its first notification"))??;
    Ok(notification.tip_id.height.value())
}

#[tokio::test]
async fn each_subscription_reads_the_client_identity_files_again() -> Result<()> {
    let pki = TestPki::generate()?;
    let indexer = start_mutual_tls_indexer(&pki).await?;
    let source = ZebraIndexerChainTipSource::new(
        ZebraIndexerSourceTarget::new(indexer.endpoint_url.clone())
            .with_tls(pki.trusted_identity()),
    );
    let trusted_cert = fs::read_to_string(pki.path("client.pem"))?;
    let trusted_key = fs::read_to_string(pki.path("client.key"))?;

    assert_eq!(first_tip_from(&source).await?, TIP_HEIGHT);

    // Rotating an untrusted identity into the same paths must reach the next
    // subscription; a source that cached the first identity would still pass.
    fs::copy(pki.path("untrusted-client.pem"), pki.path("client.pem"))?;
    fs::copy(pki.path("untrusted-client.key"), pki.path("client.key"))?;
    let refused = first_tip_from(&source).await;
    assert!(
        refused.is_err(),
        "the rotated untrusted identity was not used: {refused:?}"
    );
    assert_eq!(indexer.served_calls.load(Ordering::Relaxed), 1);

    write_file(&pki.path("client.pem"), &trusted_cert)?;
    write_file(&pki.path("client.key"), &trusted_key)?;
    assert_eq!(first_tip_from(&source).await?, TIP_HEIGHT);
    assert_eq!(indexer.served_calls.load(Ordering::Relaxed), 2);
    Ok(())
}

#[tokio::test]
async fn indexer_refuses_client_without_identity() -> Result<()> {
    let pki = TestPki::generate()?;
    let indexer = start_mutual_tls_indexer(&pki).await?;
    let target = ZebraIndexerSourceTarget::new(indexer.endpoint_url.clone())
        .with_tls(NodeIndexerTls::new().with_ca_cert_path(pki.ca_cert()));

    assert_refused(&indexer, target).await
}

#[tokio::test]
async fn indexer_refuses_identity_from_untrusted_authority() -> Result<()> {
    let pki = TestPki::generate()?;
    let indexer = start_mutual_tls_indexer(&pki).await?;
    let target = ZebraIndexerSourceTarget::new(indexer.endpoint_url.clone()).with_tls(
        NodeIndexerTls::new()
            .with_ca_cert_path(pki.ca_cert())
            .with_client_identity(
                pki.path("untrusted-client.pem"),
                pki.path("untrusted-client.key"),
            ),
    );

    assert_refused(&indexer, target).await
}

#[tokio::test]
async fn client_refuses_indexer_certificate_for_another_name() -> Result<()> {
    let pki = TestPki::generate()?;
    let indexer = start_mutual_tls_indexer(&pki).await?;
    let target = ZebraIndexerSourceTarget::new(indexer.endpoint_url.clone()).with_tls(
        pki.trusted_identity()
            .with_server_name("indexer.other.example"),
    );

    assert_refused(&indexer, target).await
}

#[tokio::test]
async fn server_name_override_verifies_the_named_certificate() -> Result<()> {
    let pki = TestPki::generate()?;
    let indexer = start_mutual_tls_indexer(&pki).await?;
    let target = ZebraIndexerSourceTarget::new(indexer.endpoint_url.clone())
        .with_tls(pki.trusted_identity().with_server_name(INDEXER_DNS_NAME));

    assert_eq!(first_tip(target).await?, TIP_HEIGHT);
    Ok(())
}

#[test]
fn unreadable_client_key_is_a_typed_error() -> Result<()> {
    let pki = TestPki::generate()?;
    let missing_key = pki.path("missing.key");
    let target = ZebraIndexerSourceTarget::new("https://127.0.0.1:8230").with_tls(
        NodeIndexerTls::new()
            .with_ca_cert_path(pki.ca_cert())
            .with_client_identity(pki.path("client.pem"), &missing_key),
    );

    let error = validate_zebra_indexer_endpoint(&target).err();

    assert!(
        matches!(
            &error,
            Some(ZebraTransportError::TlsMaterialUnreadable { role: "client key", path, .. })
                if *path == missing_key
        ),
        "{error:?}"
    );
    Ok(())
}

#[test]
fn certificate_free_ca_file_is_a_typed_error() -> Result<()> {
    let pki = TestPki::generate()?;
    let key_only = write_file(
        &pki.path("key-only.pem"),
        &fs::read_to_string(pki.path("client.key"))?,
    )?;
    let target = ZebraIndexerSourceTarget::new("https://127.0.0.1:8230")
        .with_tls(NodeIndexerTls::new().with_ca_cert_path(&key_only));

    let error = validate_zebra_indexer_endpoint(&target).err();

    assert!(
        matches!(
            &error,
            Some(ZebraTransportError::TlsMaterialUnreadable {
                role: "CA certificate",
                reason,
                ..
            }) if reason == "contains no PEM certificate"
        ),
        "{error:?}"
    );
    let rendered = error.map(|error| error.to_string()).unwrap_or_default();
    assert!(!rendered.contains("PRIVATE KEY"), "{rendered}");
    Ok(())
}

#[test]
fn key_free_client_key_file_is_a_typed_error() -> Result<()> {
    let pki = TestPki::generate()?;
    let target = ZebraIndexerSourceTarget::new("https://127.0.0.1:8230").with_tls(
        NodeIndexerTls::new()
            .with_ca_cert_path(pki.ca_cert())
            .with_client_identity(pki.path("client.pem"), pki.path("client.pem")),
    );

    let error = validate_zebra_indexer_endpoint(&target).err();

    assert!(
        matches!(
            &error,
            Some(ZebraTransportError::TlsMaterialUnreadable {
                role: "client key",
                reason,
                ..
            }) if reason == "contains no PEM private key"
        ),
        "{error:?}"
    );
    Ok(())
}

#[test]
fn tls_settings_on_plaintext_url_are_rejected() -> Result<()> {
    let pki = TestPki::generate()?;
    let target =
        ZebraIndexerSourceTarget::new("http://127.0.0.1:8230").with_tls(pki.trusted_identity());

    assert!(matches!(
        validate_zebra_indexer_endpoint(&target),
        Err(ZebraTransportError::TlsConfigRejected(_))
    ));
    Ok(())
}
