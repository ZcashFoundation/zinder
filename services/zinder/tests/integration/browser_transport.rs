use std::{future::Future, net::SocketAddr, path::Path, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use eyre::{OptionExt as _, Result, bail, ensure};
use prost::Message;
use reqwest::{Client, Method, Response, StatusCode, header};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use zinder_core::{Network, wire::encode_zinder_native_chain_name};
use zinder_explorer::runtime::{ExplorerConfigOverrides, load_explorer_config_with_env};
use zinder_ingest::{
    CanonicalCheckpointStagingRoot, CanonicalControlCommand, CanonicalControlGrpcAdapter,
    CanonicalStoreDamageSignal, canonical_control_channel,
};
use zinder_proto::v1::{
    explorer::{self, explorer_query_client::ExplorerQueryClient, explorer_query_server},
    ingest::{CanonicalWriterStatusResponse, ingest_control_server::IngestControlServer},
    wallet::{
        self, compact_blocks_in_range_chunk::Body, wallet_query_client::WalletQueryClient,
        wallet_query_server,
    },
};
use zinder_proto::wire::CHAIN_EVENT_FAMILY_HEADER;
use zinder_query::runtime::{QueryConfigError, QueryConfigOverrides, load_query_config_with_env};
use zinder_runtime::{Readiness, expected_writer_fence};
use zinder_store::{RocksDbCanonicalStore, RocksDbResourceBudget};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, IngestControlFixtureService,
    sample_regtest_upgrade_activations,
};
use zinder_wallet_rocksdb::{RocksDbWalletBuildOptions, build_wallet_from_canonical};

use super::stateless_topology_conformance::{
    build_fixture_chain, encode_grpc_web_frame, prepare_materialized_views,
};

const ALLOWED_ORIGIN: &str = "https://wallet.example";
const DENIED_ORIGIN: &str = "https://elsewhere.example";
const BINARY: &str = "application/grpc-web+proto";
const TEXT: &str = "application/grpc-web-text";
const EXPLORER_TOKEN: &str = "fixture-browser-token";
const WAIT: Duration = Duration::from_secs(10);
const NETWORK: Network = Network::ZcashRegtest;

struct NativeListener {
    addr: SocketAddr,
    service: &'static str,
    bearer_token: Option<&'static str>,
}

impl NativeListener {
    fn url(&self, method: &str) -> String {
        format!("http://{}/{}/{method}", self.addr, self.service)
    }

    async fn post(
        &self,
        http: &Client,
        encoding: &str,
        origin: &str,
        (method, message): (&str, impl Message),
    ) -> Result<Response> {
        let wire = encode_grpc_web_frame(&message.encode_to_vec())?;
        let wire = if encoding == TEXT {
            STANDARD.encode(wire).into_bytes()
        } else {
            wire
        };
        let request = http
            .post(self.url(method))
            .header(header::ORIGIN, origin)
            .header(header::CONTENT_TYPE, encoding)
            .header(header::ACCEPT, encoding)
            .header("x-grpc-web", "1")
            .body(wire);
        let request = if let Some(token) = self.bearer_token {
            request.bearer_auth(token)
        } else {
            request
        };
        Ok(request.send().await?)
    }
}

async fn start_native_listener<F, E>(
    listener: &NativeListener,
    readiness: &Readiness,
    serve: F,
) -> Result<JoinHandle<Result<(), E>>>
where
    F: Future<Output = Result<(), E>> + Send + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    let mut task = tokio::spawn(serve);
    tokio::time::timeout(WAIT, async {
        loop {
            if readiness.report().is_ready && TcpStream::connect(listener.addr).await.is_ok() {
                return Ok(task);
            }
            tokio::select! {
                outcome = &mut task => {
                    outcome??;
                    bail!("{} exited before it became ready", listener.service);
                }
                () = tokio::time::sleep(Duration::from_millis(20)) => {}
            }
        }
    })
    .await?
}

async fn assert_preflight_and_denied_post(listener: &NativeListener, http: &Client) -> Result<()> {
    for (origin, expected) in [
        (ALLOWED_ORIGIN, StatusCode::OK),
        (DENIED_ORIGIN, StatusCode::FORBIDDEN),
    ] {
        let preflight = http
            .request(Method::OPTIONS, listener.url("ServerInfo"))
            .header(header::ORIGIN, origin)
            .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
            .header(
                header::ACCESS_CONTROL_REQUEST_HEADERS,
                "authorization,content-type,x-grpc-web",
            )
            .send()
            .await?;
        assert_eq!(preflight.status(), expected);
        let headers = preflight.headers();
        assert!(!headers.contains_key(header::ACCESS_CONTROL_ALLOW_CREDENTIALS));
        if expected == StatusCode::OK {
            assert_eq!(headers[header::ACCESS_CONTROL_ALLOW_ORIGIN], origin);
            assert_eq!(headers[header::ACCESS_CONTROL_ALLOW_METHODS], "POST");
            for name in ["authorization", "content-type", "x-grpc-web"] {
                assert!(
                    headers[header::ACCESS_CONTROL_ALLOW_HEADERS]
                        .to_str()?
                        .split(',')
                        .any(|header| header.trim() == name)
                );
            }
        } else {
            assert!(!headers.contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN));
        }
    }
    let denied = listener
        .post(
            http,
            BINARY,
            DENIED_ORIGIN,
            ("ServerInfo", wallet::ServerInfoRequest {}),
        )
        .await?;
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    assert!(
        !denied
            .headers()
            .contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN)
    );
    Ok(())
}

async fn assert_native_only(listener: &NativeListener, http: &Client) -> Result<()> {
    let request = wallet::ServerInfoRequest {};
    let http1 = listener
        .post(http, BINARY, ALLOWED_ORIGIN, ("ServerInfo", request))
        .await;
    assert!(
        http1.is_err(),
        "HTTP/1.1 connected to a native-only listener: {http1:?}"
    );
    let http2 = Client::builder()
        .http2_prior_knowledge()
        .timeout(WAIT)
        .build()?;
    let denied = listener
        .post(&http2, BINARY, ALLOWED_ORIGIN, ("ServerInfo", request))
        .await?;
    assert_eq!(denied.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert!(
        !denied
            .headers()
            .contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN)
    );
    Ok(())
}

async fn read_web_messages<M: Message + Default>(
    response: Response,
    encoding: &str,
) -> Result<Vec<M>> {
    assert_eq!(response.status(), StatusCode::OK);
    let response_encoding = if encoding == TEXT {
        "application/grpc-web-text+proto"
    } else {
        BINARY
    };
    assert_eq!(response.headers()[header::CONTENT_TYPE], response_encoding);
    assert_eq!(
        response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
        ALLOWED_ORIGIN
    );
    let body = response.bytes().await?;
    let mut decoded = Vec::new();
    let mut wire = if encoding == TEXT {
        for quartet in body.chunks(4) {
            STANDARD.decode_vec(quartet, &mut decoded)?;
        }
        decoded.as_slice()
    } else {
        &body
    };
    let mut messages = Vec::new();
    while let Some((head, remaining)) = wire.split_at_checked(5) {
        let length = usize::try_from(u32::from_be_bytes(head[1..].try_into()?))?;
        let (payload, tail) = remaining
            .split_at_checked(length)
            .ok_or_eyre("truncated gRPC-Web frame")?;
        match head[0] {
            0 => messages.push(M::decode(payload)?),
            0x80 => {
                assert_eq!(tail, b"");
                assert!(
                    std::str::from_utf8(payload)?
                        .lines()
                        .any(|line| line == "grpc-status:0")
                );
                return Ok(messages);
            }
            tag => bail!("unexpected gRPC-Web frame type {tag}"),
        }
        wire = tail;
    }
    bail!("response has no terminal gRPC status frame")
}

async fn wallet_browser_calls(listener: &NativeListener, http: &Client) -> Result<()> {
    assert_preflight_and_denied_post(listener, http).await?;
    for encoding in [BINARY, TEXT] {
        let unary = listener
            .post(
                http,
                encoding,
                ALLOWED_ORIGIN,
                ("ServerInfo", wallet::ServerInfoRequest {}),
            )
            .await?;
        let messages = read_web_messages::<wallet::ServerInfoResponse>(unary, encoding).await?;
        assert_eq!(messages.len(), 1);
        assert!(messages[0].info.is_some());

        let stream = listener
            .post(
                http,
                encoding,
                ALLOWED_ORIGIN,
                (
                    "CompactBlocksInRange",
                    wallet::CompactBlocksInRangeRequest {
                        start_height: 1,
                        end_height: 2,
                        at_epoch_id: None,
                        pool_types: Vec::new(),
                    },
                ),
            )
            .await?;
        let chunks =
            read_web_messages::<wallet::CompactBlocksInRangeChunk>(stream, encoding).await?;
        ensure!(
            chunks.len() == 3,
            "expected one epoch header and two blocks, received {chunks:?}"
        );
        assert!(matches!(chunks[0].body, Some(Body::Header(_))));
        for (height, chunk) in (1..=2).zip(chunks[1..].iter()) {
            assert!(matches!(&chunk.body, Some(Body::Item(block)) if block.height == height));
        }
        assert_settled_chain_event_headers(listener, http, encoding).await?;
    }
    Ok(())
}

async fn spawn_control(
    primary: &RocksDbCanonicalStore,
    staging: &Path,
) -> Result<(SocketAddr, JoinHandle<Result<(), tonic::transport::Error>>)> {
    let status = CanonicalWriterStatusResponse {
        network_name: encode_zinder_native_chain_name(NETWORK).to_owned(),
        fence: Some(expected_writer_fence(primary.event_fence())),
        oldest_retained_event_sequence: primary.canonical_event_retention_floor()?,
        network_upgrade_schedule: primary.network_upgrade_schedule().map(|schedule| {
            zinder_proto::wire::encode_canonical_network_upgrade_schedule(
                schedule.activations(),
                schedule.revision(),
            )
        }),
        ..CanonicalWriterStatusResponse::default()
    };
    let (control, mut commands) = canonical_control_channel();
    let canonical = CanonicalControlGrpcAdapter::new(
        control,
        CanonicalCheckpointStagingRoot::new(staging.to_owned()),
        RocksDbResourceBudget::for_local_tests(),
        CanonicalStoreDamageSignal::default(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let task = tokio::spawn(async move {
        let requests = async {
            while let Some(command) = commands.recv().await {
                if let CanonicalControlCommand::WriterStatus { reply } = command {
                    let _ = reply.send(Ok(status.clone()));
                }
            }
        };
        let server = Server::builder()
            .add_service(canonical.into_server())
            .add_service(IngestControlServer::new(IngestControlFixtureService::new(
                NETWORK,
            )))
            .serve_with_incoming(TcpListenerStream::new(listener));
        tokio::select! {
            outcome = server => outcome,
            () = requests => Ok(()),
        }
    });
    Ok((addr, task))
}

async fn assert_settled_chain_event_headers(
    listener: &NativeListener,
    http: &Client,
    encoding: &str,
) -> Result<()> {
    let response = listener
        .post(
            http,
            encoding,
            ALLOWED_ORIGIN,
            (
                "ChainEvents",
                wallet::ChainEventsRequest {
                    start: Some(wallet::EventStreamStart {
                        position: Some(wallet::event_stream_start::Position::LiveTail(
                            wallet::LiveTail {},
                        )),
                    }),
                    family: wallet::ChainEventStreamFamily::Settled.into(),
                    address_filter: Vec::new(),
                },
            ),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let headers = response.headers();
    assert_eq!(headers[CHAIN_EVENT_FAMILY_HEADER], "settled");
    assert_eq!(headers[header::ACCESS_CONTROL_ALLOW_ORIGIN], ALLOWED_ORIGIN);
    assert!(
        headers[header::ACCESS_CONTROL_EXPOSE_HEADERS]
            .to_str()?
            .split(',')
            .any(|name| name.trim().eq_ignore_ascii_case(CHAIN_EVENT_FAMILY_HEADER))
    );
    Ok(())
}

struct WalletFixture {
    storage: tempfile::TempDir,
    writer: CanonicalWriterFixture,
    control_addr: SocketAddr,
    control: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl WalletFixture {
    async fn new(chain: &ChainFixture) -> Result<Self> {
        let storage = tempfile::tempdir()?;
        let writer = CanonicalWriterFixture::publish(
            &CanonicalBaseline::new(chain),
            &sample_regtest_upgrade_activations(),
        )?;
        let primary = writer.canonical_primary()?;
        let wallet_path = storage.path().join("wallet");
        let reorg_window_blocks = primary.reorg_policy().reorg_window_blocks();
        let wallet_build = build_wallet_from_canonical(
            primary,
            &wallet_path,
            RocksDbWalletBuildOptions {
                supported_reorg_depth: reorg_window_blocks,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        drop(wallet_build.store);
        let (control_addr, control) = spawn_control(primary, storage.path()).await?;
        Ok(Self {
            storage,
            writer,
            control_addr,
            control,
        })
    }

    async fn start(
        &self,
        browser_enabled: bool,
        cancel: &CancellationToken,
    ) -> Result<(NativeListener, JoinHandle<Result<(), QueryConfigError>>)> {
        let incoming = TcpListener::bind("127.0.0.1:0").await?;
        let listener = NativeListener {
            addr: incoming.local_addr()?,
            service: wallet_query_server::SERVICE_NAME,
            bearer_token: None,
        };
        let config = load_query_config_with_env(
            None,
            QueryConfigOverrides {
                network: Some(encode_zinder_native_chain_name(NETWORK).to_owned()),
                canonical_primary_path: Some(self.writer.canonical_primary_path().to_owned()),
                canonical_secondary_root: Some(self.storage.path().join("canonical-secondary")),
                wallet_primary_path: Some(self.storage.path().join("wallet")),
                wallet_secondary_root: Some(self.storage.path().join("wallet-secondary")),
                reorg_window_blocks: Some(
                    self.writer
                        .canonical_primary()?
                        .reorg_policy()
                        .reorg_window_blocks(),
                ),
                ingest_control_addr: Some(format!("http://{}", self.control_addr)),
                ..QueryConfigOverrides::default()
            },
            browser_enabled.then(|| {
                (
                    "ZINDER_QUERY__BROWSER_ORIGINS".to_owned(),
                    ALLOWED_ORIGIN.to_owned(),
                )
            }),
        )?;
        let readiness = Readiness::default();
        let task = start_native_listener(
            &listener,
            &readiness,
            zinder_query::runtime::serve_supervised(
                config,
                readiness.clone(),
                cancel.clone(),
                incoming,
            ),
        )
        .await?;
        Ok((listener, task))
    }
}

impl Drop for WalletFixture {
    fn drop(&mut self) {
        self.control.abort();
    }
}

#[tokio::test]
async fn wallet_query_serves_browser_unary_and_streaming_when_configured() -> Result<()> {
    let wallet = WalletFixture::new(&ChainFixture::new(NETWORK).extend_blocks(2)).await?;
    let http = Client::builder().http1_only().timeout(WAIT).build()?;
    for browser_enabled in [false, true] {
        let cancel = CancellationToken::new();
        let _cancel_on_drop = cancel.clone().drop_guard();
        let (listener, task) = wallet.start(browser_enabled, &cancel).await?;
        let mut native = WalletQueryClient::connect(format!("http://{}", listener.addr)).await?;
        native.server_info(wallet::ServerInfoRequest {}).await?;
        if browser_enabled {
            wallet_browser_calls(&listener, &http).await?;
        } else {
            assert_native_only(&listener, &http).await?;
        }
        cancel.cancel();
        tokio::time::timeout(WAIT, task).await???;
    }
    Ok(())
}

async fn explorer_browser_calls(listener: &NativeListener, http: &Client) -> Result<()> {
    assert_preflight_and_denied_post(listener, http).await?;
    let unauthenticated = NativeListener {
        bearer_token: None,
        ..*listener
    };
    for encoding in [BINARY, TEXT] {
        let unary = listener
            .post(
                http,
                encoding,
                ALLOWED_ORIGIN,
                ("ServerInfo", explorer::ServerInfoRequest {}),
            )
            .await?;
        let messages = read_web_messages::<explorer::ServerInfoResponse>(unary, encoding).await?;
        assert_eq!(messages.len(), 1);
        assert!(messages[0].info.is_some());
        let denied = unauthenticated
            .post(
                http,
                encoding,
                ALLOWED_ORIGIN,
                ("ServerInfo", explorer::ServerInfoRequest {}),
            )
            .await?;
        assert_eq!(
            denied.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            ALLOWED_ORIGIN
        );
        assert_eq!(denied.headers()["grpc-status"], "16");
    }
    Ok(())
}

async fn explorer_browser_streams(listener: &NativeListener, http: &Client) -> Result<()> {
    for encoding in ["application/grpc-web", TEXT] {
        let response = listener
            .post(
                http,
                encoding,
                ALLOWED_ORIGIN,
                (
                    "RecentTransactions",
                    explorer::RecentTransactionsRequest {
                        max_entries: 2,
                        from_cursor: Vec::new(),
                    },
                ),
            )
            .await?;
        let chunks =
            read_web_messages::<explorer::RecentTransactionsChunk>(response, encoding).await?;
        ensure!(
            chunks.len() == 1,
            "expected one recent-transactions chunk, received {chunks:?}"
        );
        let chunk = &chunks[0];
        assert!(chunk.freshness.is_some());
        assert_eq!(chunk.cursor.len(), 8);
        assert_eq!(
            chunk
                .entries
                .iter()
                .map(|entry| entry.block_height)
                .collect::<Vec<_>>(),
            [3, 2]
        );
        assert!(chunk.entries.iter().all(|entry| entry.is_coinbase));
    }
    Ok(())
}

#[tokio::test]
async fn explorer_query_serves_authenticated_browser_calls_when_configured() -> Result<()> {
    let storage = tempfile::tempdir()?;
    let config_file = storage.path().join("explorer.toml");
    let secondary_path = storage.path().join("explorer-secondary");
    std::fs::create_dir(&secondary_path)?;
    let wallet = WalletFixture::new(&build_fixture_chain()?).await?;
    let canonical_path = wallet.writer.canonical_primary_path();
    let (_tail_dir, _views) = prepare_materialized_views(canonical_path)?;
    let wallet_cancel = CancellationToken::new();
    let _cancel_wallet_on_drop = wallet_cancel.clone().drop_guard();
    let (wallet_listener, wallet_task) = wallet.start(false, &wallet_cancel).await?;
    let http = Client::builder().http1_only().timeout(WAIT).build()?;
    for browser_enabled in [false, true] {
        std::fs::write(
            &config_file,
            if browser_enabled {
                format!("[explorer]\nbrowser_origins = [\"{ALLOWED_ORIGIN}\"]\n")
            } else {
                String::new()
            },
        )?;
        let incoming = TcpListener::bind("127.0.0.1:0").await?;
        let listener = NativeListener {
            addr: incoming.local_addr()?,
            service: explorer_query_server::SERVICE_NAME,
            bearer_token: Some(EXPLORER_TOKEN),
        };
        let config = load_explorer_config_with_env(
            Some(config_file.clone()),
            ExplorerConfigOverrides {
                network: Some(encode_zinder_native_chain_name(NETWORK).to_owned()),
                storage_path: Some(canonical_path.to_owned()),
                wallet_query_endpoint: Some(format!("http://{}", wallet_listener.addr)),
                secondary_path: Some(secondary_path.clone()),
                ..ExplorerConfigOverrides::default()
            },
            [(
                "ZINDER_EXPLORER__BEARER_TOKEN".to_owned(),
                EXPLORER_TOKEN.to_owned(),
            )],
        )?;
        let cancel = CancellationToken::new();
        let _cancel_on_drop = cancel.clone().drop_guard();
        let readiness = Readiness::default();
        let task = start_native_listener(
            &listener,
            &readiness,
            zinder_explorer::runtime::serve_supervised(
                config,
                readiness.clone(),
                cancel.clone(),
                incoming,
            ),
        )
        .await?;
        let mut native = ExplorerQueryClient::connect(format!("http://{}", listener.addr)).await?;
        let mut request = tonic::Request::new(explorer::ServerInfoRequest {});
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {EXPLORER_TOKEN}").parse()?);
        native.server_info(request).await?;
        if browser_enabled {
            explorer_browser_calls(&listener, &http).await?;
            explorer_browser_streams(&listener, &http).await?;
        } else {
            assert_native_only(&listener, &http).await?;
        }
        cancel.cancel();
        tokio::time::timeout(WAIT, task).await???;
    }
    wallet_cancel.cancel();
    tokio::time::timeout(WAIT, wallet_task).await???;
    Ok(())
}
