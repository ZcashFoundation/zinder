//! The DATA frames a range of small rows travels in, with and without the
//! release gate.
//!
//! tonic's encoder sends what its source has ready each time the source
//! returns pending, and `tokio_stream::iter` yields every 32 items. A short
//! range of small rows therefore starts with a frame of 32 messages, which
//! lets a wallet decode its first rows a round trip earlier behind a gateway
//! that keeps frames. The gate must keep that frame.

use futures_util::stream::{self as futures_stream, StreamExt as _};
use tokio::sync::Notify;
use tokio_stream::wrappers::TcpListenerStream;

use super::*;
use crate::{
    ClientAddressSource, GrpcReceiveLimitLayer, GrpcReceiveLimits, GrpcSendStallLayer,
    public_grpc_server_with,
};

const ROWS: usize = 1000;
const ROW_BYTES: usize = 400;
const FIRST_BATCH: usize = 32;
const ROWS_PATH: &str = "/zinder.test.v1.Rows/Range";

type RowStream = Pin<Box<dyn futures_util::Stream<Item = Result<Vec<u8>, Status>> + Send>>;

/// The rows of one range, produced the way the compatibility reader produces
/// them. With `release`, the producer waits for it after the first batch.
#[derive(Clone)]
struct Rows {
    release: Option<Arc<Notify>>,
}

impl tonic::server::ServerStreamingService<()> for Rows {
    type Response = Vec<u8>;
    type ResponseStream = RowStream;
    type Future = std::future::Ready<Result<tonic::Response<RowStream>, Status>>;

    fn call(&mut self, _request: tonic::Request<()>) -> Self::Future {
        let row =
            |index: usize| Ok::<_, Status>(vec![u8::try_from(index % 251).unwrap_or(0); ROW_BYTES]);
        let rows: RowStream = match self.release.clone() {
            // Batches of 32 rows with a yield between them, which is ready
            // again as soon as the task runs, as a reader handing chunks
            // through a channel can be.
            None => Box::pin(futures_util::StreamExt::flatten(
                futures_util::StreamExt::then(
                    futures_stream::iter((0..ROWS).step_by(FIRST_BATCH)),
                    move |start| async move {
                        tokio::task::yield_now().await;
                        tokio_stream::iter((start..ROWS.min(start + FIRST_BATCH)).map(row))
                    },
                ),
            )),
            Some(release) => Box::pin(futures_util::StreamExt::chain(
                tokio_stream::iter((0..FIRST_BATCH).map(row)),
                futures_stream::once(async move {
                    release.notified().await;
                    tokio_stream::iter((FIRST_BATCH..ROWS).map(row))
                })
                .flatten(),
            )),
        };
        std::future::ready(Ok(tonic::Response::new(rows)))
    }
}

impl NamedService for Rows {
    const NAME: &'static str = "zinder.test.v1.Rows";
}

impl Service<http::Request<Body>> for Rows {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let rows = self.clone();
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(ProstCodec::<Vec<u8>, ()>::default());
            Ok(grpc.server_streaming(rows, request).await)
        })
    }
}

/// Serves `rows` through the public layers, on Zinder's loop with the gate
/// or on tonic's own server without it.
async fn serve_rows(rows: Rows, gated: bool) -> Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let limits = GrpcReceiveLimits::default();
    if gated {
        tokio::spawn(
            public_grpc_server_with(limits, STALL, ClientAddressSource::Connection)
                .add_service(rows)
                .serve_with_incoming(listener),
        );
    } else {
        tokio::spawn(
            tonic::transport::Server::builder()
                .layer(GrpcReceiveLimitLayer::new(
                    limits,
                    ClientAddressSource::Connection,
                ))
                .layer(GrpcSendStallLayer::new(STALL))
                .add_service(rows)
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
    }
    Ok(address)
}

/// Opens the range with a window that never limits it, and returns its body.
async fn open_range(address: SocketAddr) -> Result<hyper::body::Incoming> {
    let io = TokioIo::new(tokio::net::TcpStream::connect(address).await?);
    let (mut sender, connection) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .initial_stream_window_size(16 * 1024 * 1024)
        .initial_connection_window_size(16 * 1024 * 1024)
        .handshake(io)
        .await?;
    tokio::spawn(connection);
    let request = http::Request::post(format!("http://{address}{ROWS_PATH}"))
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header(http::header::TE, "trailers")
        .body(Full::new(Bytes::from_static(&[0, 0, 0, 0, 0])))?;
    Ok(sender.send_request(request).await?.into_body())
}

/// The size of each DATA frame of `body`, in the order they arrived.
async fn data_frame_sizes(mut body: hyper::body::Incoming) -> Result<Vec<usize>> {
    let mut sizes = Vec::new();
    while let Some(frame) = within_wait_limit(body.frame()).await? {
        if let Ok(chunk) = frame?.into_data() {
            sizes.push(chunk.len());
        }
    }
    Ok(sizes)
}

#[tokio::test]
async fn a_short_small_row_range_keeps_its_data_frames() -> Result<()> {
    let ungated =
        data_frame_sizes(open_range(serve_rows(Rows { release: None }, false).await?).await?)
            .await?;
    let gated =
        data_frame_sizes(open_range(serve_rows(Rows { release: None }, true).await?).await?)
            .await?;

    let total = |sizes: &[usize]| sizes.iter().sum::<usize>();
    assert_eq!(total(&gated), total(&ungated), "the body's length changed");
    // Every frame stays as it was: the last frame (3,264 bytes) fits within
    // the whole-frame probe threshold, so the gate probes it intact without
    // splitting off its trailing byte.
    assert_eq!(
        gated, ungated,
        "frames with the gate {gated:?}, without it {ungated:?}"
    );
    Ok(())
}

#[tokio::test]
async fn the_first_batch_arrives_before_the_producer_emits_the_next() -> Result<()> {
    let ungated_release = Arc::new(Notify::new());
    let ungated = open_range(
        serve_rows(
            Rows {
                release: Some(Arc::clone(&ungated_release)),
            },
            false,
        )
        .await?,
    )
    .await?;
    let release = Arc::new(Notify::new());
    let mut gated = open_range(
        serve_rows(
            Rows {
                release: Some(Arc::clone(&release)),
            },
            true,
        )
        .await?,
    )
    .await?;

    // Without the gate, the first frame carries the first batch.
    let mut ungated = ungated;
    let ungated_first = within_wait_limit(ungated.frame())
        .await?
        .ok_or_else(|| eyre!("the ungated range ended"))??
        .into_data()
        .map_err(|_| eyre!("the ungated range sent no data first"))?;
    ungated_release.notify_one();

    // With it, the same batch arrives, minus the probe byte and then that
    // byte, while the producer still waits.
    let mut first_batch = Vec::new();
    while first_batch.iter().sum::<usize>() < ungated_first.len() {
        let frame = within_wait_limit(gated.frame())
            .await?
            .ok_or_else(|| eyre!("the gated range ended"))??;
        if let Ok(chunk) = frame.into_data() {
            first_batch.push(chunk.len());
        }
    }
    assert_eq!(
        first_batch,
        [ungated_first.len() - 1, 1],
        "the first batch was {first_batch:?}"
    );
    release.notify_one();
    let rest = data_frame_sizes(gated).await?;
    assert!(
        !rest.is_empty(),
        "the rows after the first batch never came"
    );
    Ok(())
}
