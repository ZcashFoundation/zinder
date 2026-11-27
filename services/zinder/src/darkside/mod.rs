use std::{
    collections::BTreeMap,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures_util::{Stream, stream};
use http::{HeaderValue, Method, header};
use parking_lot::Mutex;
use reqwest::{Client, Url};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore, watch},
};
use tokio_util::sync::CancellationToken;
use tonic::{
    Response, Status,
    transport::{Server, server::Connected},
};
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use zinder_core::{BlockHash, TransactionId};
use zinder_proto::{
    DARKSIDE_FILE_DESCRIPTOR_SET,
    compat::lightwalletd::{
        DarksideAddressTransaction, GetAddressUtxosReply, SubtreeRoot, TreeState,
        compact_tx_streamer_server::CompactTxStreamerServer,
        darkside_streamer_server::DarksideStreamerServer,
    },
};

use crate::cli::DarksideArgs;
use chain::{Chain, FixtureTx};

mod chain;
mod control;
mod wallet;

const MAX_MESSAGE_BYTES: usize = 5 * 1024 * 1024;
const MAX_CACHE_ENTRIES: usize = 1_024;
const MAX_INJECTED_BYTES: usize = 4_096;
const MAX_POOL_BYTES: usize = 8 * 1024 * 1024;

type Rpc<T> = Result<Response<T>, Status>;
type StreamReply<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

fn stream_reply<T: Send + 'static>(
    permit: OwnedSemaphorePermit,
    messages: impl Iterator<Item = T> + Send + 'static,
) -> Response<StreamReply<T>> {
    Response::new(Box::pin(stream::iter(
        messages.scan(permit, |_, message| Some(Ok(message))),
    )))
}

#[derive(Default)]
struct Scenario {
    generation: Arc<()>,
    tip_generation: Arc<()>,
    chain: Chain,
    tree_states: BTreeMap<u64, TreeState>,
    subtree_roots: BTreeMap<i32, (u32, Vec<SubtreeRoot>)>,
    utxos: Vec<GetAddressUtxosReply>,
    address_transactions: Vec<DarksideAddressTransaction>,
    incoming: Vec<Arc<FixtureTx>>,
    mempool: Vec<Arc<FixtureTx>>,
}

impl Scenario {
    fn visible_tip(&self) -> Option<(u64, BlockHash)> {
        self.chain
            .visible()
            .last()
            .map(|block| (block.height, block.hash))
    }

    fn mined(&self, id: TransactionId) -> bool {
        self.chain
            .visible()
            .iter()
            .any(|block| block.transactions.iter().any(|tx| tx.id == id))
    }
}

#[derive(Clone)]
struct Darkside {
    scenario: Arc<Mutex<Scenario>>,
    stage_slot: Arc<Semaphore>,
    stream_slots: Arc<Semaphore>,
    changed: watch::Sender<()>,
    stop: CancellationToken,
    fixtures: Option<Arc<FixtureOrigin>>,
    reject_broadcasts: bool,
}

impl Darkside {
    fn stream_slot(&self) -> Result<OwnedSemaphorePermit, Status> {
        Arc::clone(&self.stream_slots)
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("too many active test streams"))
    }
}

#[derive(Debug, Error)]
pub(crate) enum StartError {
    #[error("non-loopback listener {0} requires --allow-non-loopback")]
    NonLoopback(SocketAddr),
    #[error("{0} requires an HTTP(S) origin with no credentials, path, query, or fragment")]
    InvalidOrigin(&'static str),
    #[error("could not bind Darkside listener: {0}")]
    Bind(#[source] io::Error),
    #[error("could not configure fixture downloads: {0}")]
    Fixtures(#[source] reqwest::Error),
    #[error("could not configure gRPC reflection: {0}")]
    Reflection(#[source] tonic_reflection::server::Error),
    #[error("Darkside listener failed: {0}")]
    Serve(#[source] tonic::transport::Error),
}

struct FixtureOrigin {
    client: Client,
    origin: reqwest::Url,
}

impl FixtureOrigin {
    fn new(input: &str) -> Result<Arc<Self>, StartError> {
        let origin = parse_origin(input, "--fixture-origin")?;
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(StartError::Fixtures)?;
        Ok(Arc::new(Self { client, origin }))
    }
}

fn parse_origin(input: &str, flag: &'static str) -> Result<Url, StartError> {
    let origin = Url::parse(input).map_err(|_| StartError::InvalidOrigin(flag))?;
    if !matches!(origin.scheme(), "http" | "https")
        || origin.host().is_none()
        || !origin.username().is_empty()
        || origin.password().is_some()
        || origin.path() != "/"
        || origin.query().is_some()
        || origin.fragment().is_some()
    {
        return Err(StartError::InvalidOrigin(flag));
    }
    Ok(origin)
}

pub(crate) async fn run(args: DarksideArgs) -> Result<(), StartError> {
    if !args.listen_addr.ip().is_loopback() && !args.allow_non_loopback {
        return Err(StartError::NonLoopback(args.listen_addr));
    }
    let browser_origin = args
        .grpc_web_origin
        .as_deref()
        .map(|input| {
            let flag = "--grpc-web-origin";
            let origin = parse_origin(input, flag)?;
            HeaderValue::from_str(&origin.origin().ascii_serialization())
                .map_err(|_| StartError::InvalidOrigin(flag))
        })
        .transpose()?;
    let fixtures = args
        .fixture_origin
        .as_deref()
        .map(FixtureOrigin::new)
        .transpose()?;
    let listener = TcpListener::bind(args.listen_addr)
        .await
        .map_err(StartError::Bind)?;
    let address = listener.local_addr().map_err(StartError::Bind)?;
    let (changed, _) = watch::channel(());
    let stop = CancellationToken::new();
    let darkside = Darkside {
        scenario: Arc::new(Mutex::new(Scenario::default())),
        stage_slot: Arc::new(Semaphore::new(1)),
        stream_slots: Arc::new(Semaphore::new(4)),
        changed,
        stop: stop.clone(),
        fixtures,
        reject_broadcasts: args.reject_broadcasts,
    };
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(DARKSIDE_FILE_DESCRIPTOR_SET)
        .build_v1()
        .map_err(StartError::Reflection)?;
    let browser_enabled = browser_origin.is_some();
    let layers = ServiceBuilder::new()
        .option_layer(browser_origin.map(cors))
        .option_layer(browser_enabled.then(tonic_web::GrpcWebLayer::new))
        .into_inner();
    let mut server = Server::builder()
        .accept_http1(browser_enabled)
        .concurrency_limit_per_connection(4)
        .max_concurrent_streams(4)
        .timeout(Duration::from_secs(20))
        .layer(layers);
    let serving = server
        .add_service(reflection)
        .add_service(
            DarksideStreamerServer::new(darkside.clone())
                .max_decoding_message_size(MAX_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_MESSAGE_BYTES),
        )
        .add_service(
            CompactTxStreamerServer::new(darkside)
                .max_decoding_message_size(MAX_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_MESSAGE_BYTES),
        )
        .serve_with_incoming_shutdown(incoming(listener), stop.clone().cancelled_owned());
    tokio::pin!(serving);
    announce(address);

    let deadline = tokio::time::sleep(Duration::from_secs(u64::from(args.timeout_secs.get())));
    tokio::pin!(deadline);
    tokio::select! {
        outcome = &mut serving => outcome.map_err(StartError::Serve),
        () = &mut deadline => Ok(()),
        () = stop.cancelled() => {
            tokio::select! {
                outcome = &mut serving => outcome.map_err(StartError::Serve),
                () = &mut deadline => Ok(()),
                () = tokio::time::sleep(Duration::from_secs(1)) => Ok(()),
            }
        }
    }
}

#[allow(
    clippy::print_stdout,
    reason = "test drivers discover the bound address from CLI stdout"
)]
fn announce(address: SocketAddr) {
    println!("Darkside listening on {address}");
}

fn cors(origin: HeaderValue) -> CorsLayer {
    const GRPC_STATUS: header::HeaderName = header::HeaderName::from_static("grpc-status");
    const GRPC_MESSAGE: header::HeaderName = header::HeaderName::from_static("grpc-message");
    const GRPC_STATUS_DETAILS: header::HeaderName =
        header::HeaderName::from_static("grpc-status-details-bin");
    CorsLayer::new()
        .allow_origin(origin)
        .allow_methods([Method::POST])
        .allow_headers([
            header::ACCEPT,
            header::CONTENT_TYPE,
            header::HeaderName::from_static("x-grpc-web"),
            header::HeaderName::from_static("x-user-agent"),
            header::HeaderName::from_static("grpc-timeout"),
        ])
        .expose_headers([GRPC_STATUS, GRPC_MESSAGE, GRPC_STATUS_DETAILS])
}

fn incoming(listener: TcpListener) -> impl Stream<Item = io::Result<Connection>> {
    let slots = Arc::new(Semaphore::new(16));
    stream::unfold((listener, slots), |(listener, slots)| async move {
        let permit = Arc::clone(&slots).acquire_owned().await.ok()?;
        let connection = listener.accept().await.map(|(socket, _)| Connection {
            socket,
            _permit: permit,
        });
        Some((connection, (listener, slots)))
    })
}

struct Connection {
    socket: TcpStream,
    _permit: OwnedSemaphorePermit,
}

impl Connected for Connection {
    type ConnectInfo = ();

    fn connect_info(&self) -> Self::ConnectInfo {}
}

impl AsyncRead for Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_read(cx, buf)
    }
}

impl AsyncWrite for Connection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.socket).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}
