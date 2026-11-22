mod published_range;

use std::{fs, net::SocketAddr, path::Path, sync::Arc, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use eyre::{Result, WrapErr as _, ensure, eyre};
use parking_lot::Mutex;
use prost::Message;
use reqwest::{Client, Method, Response, header};
use tokio::{net::TcpListener, process::Command};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};
use tonic_health::pb::{
    HealthCheckRequest, health_check_response::ServingStatus, health_client::HealthClient,
};
use zinder_core::Network;
use zinder_proto::{
    compat::lightwalletd::{self, compact_tx_streamer_client::CompactTxStreamerClient},
    v1::ingest::{
        canonical_control_server::CanonicalControlServer,
        ingest_control_server::IngestControlServer,
    },
};
use zinder_store::{RawBlobRetention, RocksDbCanonicalStore};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, IngestControlFixtureService,
    sample_regtest_upgrade_activations,
};
use zinder_wallet_rocksdb::{RocksDbWalletBuildOptions, build_wallet_from_canonical};

use crate::common::{MutableCanonicalControl, writer_status_for_fence};

const ORIGIN: &str = "http://wallet.example";
const CONTENT_TYPE: &str = "application/grpc-web+proto";
const TEXT_CONTENT_TYPE: &str = "application/grpc-web-text+proto";
const SERVICE: &str = "cash.z.wallet.sdk.rpc.CompactTxStreamer";

#[tokio::test]
async fn browser_wallet_reads_unary_and_streaming_rpcs_on_the_native_listener() -> Result<()> {
    let runtime = CompatProcess::start("wallet", Some(ORIGIN)).await?;
    let http = Client::builder().http1_only().build()?;
    let tip = runtime
        .post(&http, "GetLatestBlock", lightwalletd::ChainSpec {})
        .await?;
    assert_eq!(tip.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], ORIGIN);
    let messages = read_web_response(tip, CONTENT_TYPE, 0).await?;
    assert_eq!(messages.len(), 1);
    assert_eq!(
        lightwalletd::BlockId::decode(messages[0].as_slice())?.height,
        3
    );
    let range = lightwalletd::BlockRange {
        start: Some(lightwalletd::BlockId {
            height: 1,
            hash: vec![],
        }),
        end: Some(lightwalletd::BlockId {
            height: 3,
            hash: vec![],
        }),
        pool_types: vec![lightwalletd::PoolType::Sapling.into()],
    };
    for encoding in [CONTENT_TYPE, TEXT_CONTENT_TYPE] {
        let blocks = read_web_response(
            runtime
                .post_as(&http, "GetBlockRange", encoding, &range)
                .await?,
            encoding,
            0,
        )
        .await?;
        let heights = blocks
            .iter()
            .map(|block| {
                lightwalletd::CompactBlock::decode(block.as_slice()).map(|block| block.height)
            })
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(heights, [1, 2, 3]);
    }
    let missing = runtime
        .post(
            &http,
            "GetBlock",
            lightwalletd::BlockId {
                height: 4,
                hash: vec![],
            },
        )
        .await?;
    assert_eq!(
        read_web_response(missing, CONTENT_TYPE, tonic::Code::OutOfRange as i32).await?,
        Vec::<Vec<u8>>::new()
    );

    let unsupported = runtime
        .post(
            &http,
            "GetTaddressBalanceStream",
            lightwalletd::Address::default(),
        )
        .await?;
    assert_eq!(
        read_web_response(unsupported, CONTENT_TYPE, tonic::Code::Unimplemented as i32).await?,
        Vec::<Vec<u8>>::new()
    );

    let mut native = runtime.native_client().await?;
    assert_eq!(
        native
            .get_latest_block(lightwalletd::ChainSpec {})
            .await?
            .into_inner()
            .height,
        3
    );
    Ok(())
}

#[tokio::test]
async fn browser_enabled_listener_serves_health_over_native_grpc() -> Result<()> {
    let runtime = CompatProcess::start("wallet", Some(ORIGIN)).await?;
    let http = Client::builder().http1_only().build()?;
    let channel = Channel::from_shared(format!("http://{}", runtime.addr))?
        .connect()
        .await?;
    let health = HealthClient::new(channel)
        .check(HealthCheckRequest {
            service: SERVICE.into(),
        })
        .await?
        .into_inner();
    assert_eq!(health.status(), ServingStatus::Serving);
    let browser_health = http
        .post(format!(
            "http://{}/grpc.health.v1.Health/Check",
            runtime.addr
        ))
        .header(header::CONTENT_TYPE, CONTENT_TYPE)
        .header(header::ORIGIN, ORIGIN)
        .body(vec![0; 5])
        .send()
        .await?;
    assert_eq!(browser_health.status(), reqwest::StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test]
async fn compact_blocks_listener_limits_browser_origins_and_available_rpcs() -> Result<()> {
    let runtime = CompatProcess::start("compact-blocks", Some(ORIGIN)).await?;
    let http = Client::builder().http1_only().build()?;
    let preflight = |origin| {
        http.request(Method::OPTIONS, runtime.method_url("GetLatestBlock"))
            .header(header::ORIGIN, origin)
            .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
            .header(
                header::ACCESS_CONTROL_REQUEST_HEADERS,
                "content-type,x-grpc-web",
            )
            .send()
    };
    let allowed = preflight(ORIGIN).await?;
    assert!(allowed.status().is_success());
    assert_eq!(allowed.headers()[header::ACCESS_CONTROL_MAX_AGE], "600");
    assert_eq!(
        allowed.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
        ORIGIN
    );
    for name in ["content-type", "x-grpc-web"] {
        assert!(
            allowed.headers()[header::ACCESS_CONTROL_ALLOW_HEADERS]
                .to_str()?
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case(name))
        );
    }

    let tip = runtime
        .post(&http, "GetLatestBlock", lightwalletd::ChainSpec {})
        .await?;
    let replies = read_web_response(tip, CONTENT_TYPE, 0).await?;
    assert_eq!(replies.len(), 1);
    assert_eq!(
        lightwalletd::BlockId::decode(replies[0].as_slice())?.height,
        3
    );
    let roots = runtime
        .post(
            &http,
            "GetSubtreeRoots",
            lightwalletd::GetSubtreeRootsArg::default(),
        )
        .await?;
    assert_eq!(
        read_web_response(roots, CONTENT_TYPE, tonic::Code::Unimplemented as i32).await?,
        Vec::<Vec<u8>>::new()
    );

    let denied = "http://unlisted.example";
    let response = preflight(denied).await?;
    let granted = response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN);
    assert!(granted.is_none_or(|origin| origin != denied && origin != "*"));
    Ok(())
}

#[tokio::test]
async fn native_only_listener_refuses_browser_requests_by_default() -> Result<()> {
    let runtime = CompatProcess::start("wallet", None).await?;
    let mut native = runtime.native_client().await?;
    assert_eq!(
        native
            .get_latest_block(lightwalletd::ChainSpec {})
            .await?
            .into_inner()
            .height,
        3
    );
    let http = Client::builder().http1_only().build()?;
    if let Ok(response) = runtime
        .post(&http, "GetLatestBlock", lightwalletd::ChainSpec {})
        .await
    {
        assert!(
            !response.status().is_success()
                || response
                    .headers()
                    .get(header::CONTENT_TYPE)
                    .is_none_or(|kind| kind != CONTENT_TYPE)
        );
    }
    Ok(())
}

async fn read_web_response(
    response: Response,
    encoding: &str,
    expected_status: i32,
) -> Result<Vec<Vec<u8>>> {
    ensure!(
        response.status().is_success(),
        "HTTP status {}",
        response.status()
    );
    ensure!(
        response.headers()[header::CONTENT_TYPE] == encoding,
        "unexpected content type"
    );
    let header_status = response
        .headers()
        .get("grpc-status")
        .map(|code| code.to_str()?.parse::<i32>().map_err(eyre::Report::from))
        .transpose()?;
    let mut body = response.bytes().await?;
    if encoding == TEXT_CONTENT_TYPE {
        // A flush can introduce base64 padding before the end of the stream.
        let mut decoded = Vec::new();
        for quartet in body.chunks(4) {
            STANDARD.decode_vec(quartet, &mut decoded)?;
        }
        body = decoded.into();
    }
    let mut remaining = body.as_ref();
    let mut trailer_status = None;
    let mut messages = Vec::new();
    while !remaining.is_empty() {
        let (prefix, rest) = remaining
            .split_at_checked(5)
            .ok_or_else(|| eyre!("incomplete frame header"))?;
        let length = usize::try_from(u32::from_be_bytes(prefix[1..].try_into()?))?;
        let (payload, rest) = rest
            .split_at_checked(length)
            .ok_or_else(|| eyre!("incomplete frame body"))?;
        match prefix[0] {
            0 => messages.push(payload.to_vec()),
            0x80 => {
                ensure!(rest.is_empty(), "trailers must be last");
                trailer_status = std::str::from_utf8(payload)?
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("grpc-status"))
                    .map(|(_, code)| code.trim().parse())
                    .transpose()?;
            }
            flag => return Err(eyre!("unexpected frame flag {flag}")),
        }
        remaining = rest;
    }
    let status = if messages.is_empty() {
        trailer_status.or(header_status)
    } else {
        trailer_status
    };
    assert_eq!(status, Some(expected_status));
    Ok(messages)
}

struct CompatProcess {
    addr: SocketAddr,
    _child: tokio::process::Child,
    control: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    storage: tempfile::TempDir,
    writer: CanonicalWriterFixture,
    writer_control: MutableCanonicalControl,
}

impl Drop for CompatProcess {
    fn drop(&mut self) {
        self.control.abort();
    }
}

impl CompatProcess {
    async fn start(serving: &str, origin: Option<&str>) -> Result<Self> {
        Self::start_with_history(serving, origin, "").await
    }

    async fn start_with_history(
        serving: &str,
        origin: Option<&str>,
        history: &str,
    ) -> Result<Self> {
        Self::start_with_history_in(serving, origin, history, tempfile::tempdir()?).await
    }

    async fn start_with_history_in(
        serving: &str,
        origin: Option<&str>,
        history: &str,
        storage: tempfile::TempDir,
    ) -> Result<Self> {
        let network = Network::ZcashRegtest;
        let chain = ChainFixture::new(network)
            .extend_blocks(3)
            .with_raw_blob_retention(RawBlobRetention::Transactions);
        let writer = CanonicalWriterFixture::publish(
            &CanonicalBaseline::new(&chain),
            &sample_regtest_upgrade_activations(),
        )?;
        let primary = writer.canonical_primary()?;
        let wallet_config = if serving == "wallet" {
            build_wallet_config(primary, storage.path())?
        } else {
            String::new()
        };
        let status = writer_status_for_fence(
            primary.event_fence(),
            network,
            primary.network_upgrade_schedule(),
        );
        let control_listener = TcpListener::bind("127.0.0.1:0").await?;
        let control_addr = control_listener.local_addr()?;
        let control_service = MutableCanonicalControl {
            status: Arc::new(Mutex::new(status)),
        };
        let writer_control = control_service.clone();
        let control = tokio::spawn(async move {
            Server::builder()
                .add_service(IngestControlServer::new(IngestControlFixtureService::new(
                    network,
                )))
                .add_service(CanonicalControlServer::new(control_service))
                .serve_with_incoming(TcpListenerStream::new(control_listener))
                .await
        });
        let config = storage.path().join("compat.toml");
        let origin_setting = origin.map_or_else(String::new, |origin| {
            format!("browser_origins = [\"{origin}\"]")
        });
        fs::write(
            &config,
            format!(
                r#"[network]
name = "zcash-regtest"
[storage]
path = "{}"
secondary_path = "{}"
raw_blob_policy = "transactions"
{wallet_config}
[ops]
listen_addr = "127.0.0.1:0"
[ingest_control]
addr = "http://{control_addr}"
[compat]
serving = "{serving}"
listen_addr = "127.0.0.1:0"
reorg_window_blocks = 100
stale_tip_limit_ms = 0
{origin_setting}
{history}
"#,
                writer.canonical_primary_path().display(),
                storage.path().join("canonical-reader").display(),
            ),
        )?;
        let (addr, child) = start_listener(&config, &storage.path().join("compat.log"))
            .await
            .inspect_err(|_| control.abort())?;
        Ok(Self {
            addr,
            _child: child,
            control,
            storage,
            writer,
            writer_control,
        })
    }

    fn method_url(&self, method: &str) -> String {
        format!("http://{}/{SERVICE}/{method}", self.addr)
    }

    async fn native_client(&self) -> Result<CompactTxStreamerClient<Channel>> {
        CompactTxStreamerClient::connect(format!("http://{}", self.addr))
            .await
            .wrap_err_with(|| {
                format!(
                    "compat listener for {} failed to connect: {}",
                    self.writer.canonical_primary_path().display(),
                    fs::read_to_string(self.storage.path().join("compat.log")).unwrap_or_default()
                )
            })
    }

    async fn post(&self, http: &Client, method: &str, message: impl Message) -> Result<Response> {
        self.post_as(http, method, CONTENT_TYPE, &message).await
    }

    async fn post_as(
        &self,
        http: &Client,
        method: &str,
        encoding: &str,
        message: &impl Message,
    ) -> Result<Response> {
        let mut body = vec![0];
        body.extend_from_slice(&u32::try_from(message.encoded_len())?.to_be_bytes());
        message.encode(&mut body)?;
        if encoding == TEXT_CONTENT_TYPE {
            body = STANDARD.encode(&body).into_bytes();
        }
        Ok(http
            .post(self.method_url(method))
            .header(header::ORIGIN, ORIGIN)
            .header(header::CONTENT_TYPE, encoding)
            .header(header::ACCEPT, encoding)
            .header("x-grpc-web", "1")
            .body(body)
            .send()
            .await?)
    }
}

async fn start_listener(config: &Path, log: &Path) -> Result<(SocketAddr, tokio::process::Child)> {
    let mut child = Command::new(zinder_testkit::bin_exe!("zinder-compat-lightwalletd"))
        .env_clear()
        .arg("--config")
        .arg(config)
        .stderr(fs::File::create(log)?)
        .kill_on_drop(true)
        .spawn()?;
    let addr = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Some(exit) = child.try_wait()? {
                return Err(eyre!("compat process exited with {exit}"));
            }
            let output = fs::read_to_string(log)?;
            if let Some((complete, _)) = output.rsplit_once('\n')
                && let Some(field) = complete
                    .lines()
                    .find(|line| {
                        line.split_whitespace()
                            .any(|field| field == r#"event="compat_started""#)
                    })
                    .and_then(|line| {
                        line.split_whitespace()
                            .find_map(|field| field.strip_prefix("listen_addr="))
                    })
            {
                let addr: SocketAddr = field.parse()?;
                if CompactTxStreamerClient::connect(format!("http://{addr}"))
                    .await
                    .is_ok()
                {
                    return Ok(addr);
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    })
    .await
    .wrap_err("compat listener startup timed out")
    .and_then(std::convert::identity)
    .wrap_err_with(|| {
        format!(
            "compat process log:\n{}",
            fs::read_to_string(log).unwrap_or_else(|error| error.to_string())
        )
    })?;
    Ok((addr, child))
}

fn build_wallet_config(primary: &RocksDbCanonicalStore, root: &Path) -> Result<String> {
    let wallet_path = root.join("wallet");
    let wallet = build_wallet_from_canonical(
        primary,
        &wallet_path,
        RocksDbWalletBuildOptions {
            supported_reorg_depth: primary.reorg_policy().reorg_window_blocks(),
            ..RocksDbWalletBuildOptions::for_local_tests()
        },
    )?;
    drop(wallet.store);
    Ok(format!(
        "[wallet]\npath = \"{}\"\nsecondary_path = \"{}\"",
        wallet_path.display(),
        root.join("wallet-reader").display()
    ))
}
