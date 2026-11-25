//! Counts wallet calls at the native gRPC boundary, including early refusals
//! and the terminal status of a streaming response.

use std::{
    cell::RefCell,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use bytes::Bytes;
use futures_util::future::{BoxFuture, Either};
use http::{HeaderMap, Request, Response};
use http_body::{Body as _, Frame, SizeHint};
use tonic::{Code, Status, body::Body};
use tonic_types::StatusExt as _;
use tower::{Layer, Service};
use zinder_proto::{
    ZINDER_ERROR_DOMAIN,
    compat::lightwalletd,
    v1::{ops::ErrorReason, wallet},
};

use super::MessageBoundaries;
use super::grpc_timeout::DeadlineExpiry;

const REQUESTS: &str = "zinder_wallet_rpc_requests_total";
const READINESS_REFUSALS: &str = "zinder_wallet_rpc_readiness_refusals_total";
const DURATION: &str = "zinder_wallet_rpc_duration_seconds";
const IN_FLIGHT: &str = "zinder_wallet_rpc_in_flight";
const OPEN_STREAMS: &str = "zinder_wallet_rpc_open_streams";
pub(crate) const RANGE_BLOCKS: &str = "zinder_wallet_rpc_block_range_blocks";
pub(crate) const RANGE_DURATION: &str = "zinder_wallet_rpc_block_range_duration_seconds";

/// Records a wallet listener's method counts, concurrent calls, and stream
/// outcomes. Place it inside transport translation, before admission.
#[derive(Clone, Copy, Debug)]
pub enum WalletRpcMetricsLayer {
    /// The native `WalletQuery` service.
    WalletQuery,
    /// Either serving mode of the lightwalletd `CompactTxStreamer` service.
    Lightwalletd,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum RpcShape {
    Unary,
    Stream,
    Upload,
    BlockRange,
    CompactRangeWithHeader,
}

impl WalletRpcMetricsLayer {
    fn label(self) -> &'static str {
        match self {
            Self::WalletQuery => "wallet_query",
            Self::Lightwalletd => "lightwalletd",
        }
    }

    fn service_name(self) -> &'static str {
        match self {
            Self::WalletQuery => wallet::wallet_query_server::SERVICE_NAME,
            Self::Lightwalletd => lightwalletd::compact_tx_streamer_server::SERVICE_NAME,
        }
    }

    fn methods(self) -> &'static [(&'static str, RpcShape)] {
        use RpcShape::{BlockRange, CompactRangeWithHeader, Stream, Unary, Upload};
        match self {
            Self::WalletQuery => &[
                ("VisibleTipBlock", Unary),
                ("SettledTipBlock", Unary),
                ("BlockIdBySelector", Unary),
                ("BlockHeaderBySelector", Unary),
                ("CompactBlock", Unary),
                ("CompactBlocksInRange", CompactRangeWithHeader),
                ("FullBlock", Unary),
                ("FullBlocksInRange", BlockRange),
                ("Transaction", Unary),
                ("TreeStateAtHeight", Unary),
                ("LatestTreeStateCheckpoint", Unary),
                ("SubtreeRoots", Unary),
                ("BroadcastTransaction", Unary),
                ("ChainEvents", Stream),
                ("MempoolSnapshot", Unary),
                ("MempoolEvents", Stream),
                ("TransparentAddressUnspentOutputs", Stream),
                ("TransparentAddressTxIdsInRange", Stream),
                ("TransparentOutpointSpendStatus", Unary),
                ("ChainValuePoolsAtTip", Unary),
                ("ServerInfo", Unary),
                ("NetworkUpgradeActivations", Unary),
            ],
            Self::Lightwalletd => &[
                ("GetLatestBlock", Unary),
                ("GetBlock", Unary),
                ("GetBlockNullifiers", Unary),
                ("GetBlockRange", BlockRange),
                ("GetBlockRangeNullifiers", BlockRange),
                ("GetTransaction", Unary),
                ("SendTransaction", Unary),
                ("GetTaddressTxids", Stream),
                ("GetTaddressTransactions", Stream),
                ("GetTaddressBalance", Unary),
                ("GetTaddressBalanceStream", Upload),
                ("GetMempoolTx", Stream),
                ("GetMempoolStream", Stream),
                ("GetTreeState", Unary),
                ("GetLatestTreeState", Unary),
                ("GetSubtreeRoots", Stream),
                ("GetAddressUtxos", Unary),
                ("GetAddressUtxosStream", Stream),
                ("GetLightdInfo", Unary),
                ("GetStandardFee", Unary),
                ("Ping", Unary),
            ],
        }
    }

    fn method(self, path: &str) -> Option<(&'static str, RpcShape)> {
        use RpcShape::{BlockRange, CompactRangeWithHeader, Stream, Unary, Upload};
        let (service, method) = path.strip_prefix('/')?.split_once('/')?;
        if service != self.service_name() {
            return None;
        }
        match self {
            Self::WalletQuery => match method {
                "VisibleTipBlock" => Some(("VisibleTipBlock", Unary)),
                "SettledTipBlock" => Some(("SettledTipBlock", Unary)),
                "BlockIdBySelector" => Some(("BlockIdBySelector", Unary)),
                "BlockHeaderBySelector" => Some(("BlockHeaderBySelector", Unary)),
                "CompactBlock" => Some(("CompactBlock", Unary)),
                "CompactBlocksInRange" => Some(("CompactBlocksInRange", CompactRangeWithHeader)),
                "FullBlock" => Some(("FullBlock", Unary)),
                "FullBlocksInRange" => Some(("FullBlocksInRange", BlockRange)),
                "Transaction" => Some(("Transaction", Unary)),
                "TreeStateAtHeight" => Some(("TreeStateAtHeight", Unary)),
                "LatestTreeStateCheckpoint" => Some(("LatestTreeStateCheckpoint", Unary)),
                "SubtreeRoots" => Some(("SubtreeRoots", Unary)),
                "BroadcastTransaction" => Some(("BroadcastTransaction", Unary)),
                "ChainEvents" => Some(("ChainEvents", Stream)),
                "MempoolSnapshot" => Some(("MempoolSnapshot", Unary)),
                "MempoolEvents" => Some(("MempoolEvents", Stream)),
                "TransparentAddressUnspentOutputs" => {
                    Some(("TransparentAddressUnspentOutputs", Stream))
                }
                "TransparentAddressTxIdsInRange" => {
                    Some(("TransparentAddressTxIdsInRange", Stream))
                }
                "TransparentOutpointSpendStatus" => Some(("TransparentOutpointSpendStatus", Unary)),
                "ChainValuePoolsAtTip" => Some(("ChainValuePoolsAtTip", Unary)),
                "ServerInfo" => Some(("ServerInfo", Unary)),
                "NetworkUpgradeActivations" => Some(("NetworkUpgradeActivations", Unary)),
                _ => None,
            },
            Self::Lightwalletd => match method {
                "GetLatestBlock" => Some(("GetLatestBlock", Unary)),
                "GetBlock" => Some(("GetBlock", Unary)),
                "GetBlockNullifiers" => Some(("GetBlockNullifiers", Unary)),
                "GetBlockRange" => Some(("GetBlockRange", BlockRange)),
                "GetBlockRangeNullifiers" => Some(("GetBlockRangeNullifiers", BlockRange)),
                "GetTransaction" => Some(("GetTransaction", Unary)),
                "SendTransaction" => Some(("SendTransaction", Unary)),
                "GetTaddressTxids" => Some(("GetTaddressTxids", Stream)),
                "GetTaddressTransactions" => Some(("GetTaddressTransactions", Stream)),
                "GetTaddressBalance" => Some(("GetTaddressBalance", Unary)),
                "GetTaddressBalanceStream" => Some(("GetTaddressBalanceStream", Upload)),
                "GetMempoolTx" => Some(("GetMempoolTx", Stream)),
                "GetMempoolStream" => Some(("GetMempoolStream", Stream)),
                "GetTreeState" => Some(("GetTreeState", Unary)),
                "GetLatestTreeState" => Some(("GetLatestTreeState", Unary)),
                "GetSubtreeRoots" => Some(("GetSubtreeRoots", Stream)),
                "GetAddressUtxos" => Some(("GetAddressUtxos", Unary)),
                "GetAddressUtxosStream" => Some(("GetAddressUtxosStream", Stream)),
                "GetLightdInfo" => Some(("GetLightdInfo", Unary)),
                "GetStandardFee" => Some(("GetStandardFee", Unary)),
                "Ping" => Some(("Ping", Unary)),
                _ => None,
            },
        }
    }
}

impl<S> Layer<S> for WalletRpcMetricsLayer {
    type Service = WalletRpcMetrics<S>;

    fn layer(&self, inner: S) -> Self::Service {
        metrics::describe_counter!(
            REQUESTS,
            metrics::Unit::Count,
            "Completed wallet RPCs by surface, method, gRPC code, and typed error class."
        );
        metrics::describe_counter!(
            READINESS_REFUSALS,
            metrics::Unit::Count,
            "Wallet RPCs rejected by the traffic readiness gate, by surface and method."
        );
        metrics::describe_histogram!(
            DURATION,
            metrics::Unit::Seconds,
            "Time from wallet RPC arrival through its final response or cancellation."
        );
        metrics::describe_gauge!(
            IN_FLIGHT,
            metrics::Unit::Count,
            "Wallet RPCs from arrival through completion, by surface and method."
        );
        metrics::describe_gauge!(
            OPEN_STREAMS,
            metrics::Unit::Count,
            "Open wallet response streams or client uploads being read, by surface and method."
        );
        metrics::describe_histogram!(
            RANGE_BLOCKS,
            metrics::Unit::Count,
            "Complete block messages handed to the transport by an opened wallet range stream."
        );
        metrics::describe_histogram!(
            RANGE_DURATION,
            metrics::Unit::Seconds,
            "Time from opening a wallet block-range response body through its final status or cancellation."
        );
        for &(method, shape) in self.methods() {
            let labels = [("surface", self.label()), ("method", method)];
            metrics::counter!(READINESS_REFUSALS, &labels).increment(0);
            metrics::gauge!(IN_FLIGHT, &labels).increment(0);
            if shape != RpcShape::Unary {
                metrics::gauge!(OPEN_STREAMS, &labels).increment(0);
            }
        }
        WalletRpcMetrics {
            inner,
            surface: *self,
        }
    }
}

/// The Tower service built by [`WalletRpcMetricsLayer`].
#[derive(Clone, Debug)]
pub struct WalletRpcMetrics<S> {
    inner: S,
    surface: WalletRpcMetricsLayer,
}

impl<S> Service<Request<Body>> for WalletRpcMetrics<S>
where
    S: Service<Request<Body>, Response = Response<Body>>,
    S::Future: Send + 'static,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = Either<S::Future, BoxFuture<'static, Result<Self::Response, Self::Error>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<Body>) -> Self::Future {
        let Some((method, shape)) = self.surface.method(request.uri().path()) else {
            return Either::Left(self.inner.call(request));
        };
        if request.method() != http::Method::POST {
            return Either::Left(self.inner.call(request));
        }
        let method = WalletRpcMethod {
            surface: self.surface.label(),
            name: method,
        };
        request.extensions_mut().insert(method);
        let mut rpc = ObservedRpc::start(method, shape);
        rpc.deadline = request.extensions().get::<DeadlineExpiry>().cloned();
        let request = if shape == RpcShape::Upload {
            request.map(|body| {
                Body::new(ObservedUpload {
                    body,
                    method,
                    active: None,
                })
            })
        } else {
            request
        };
        let response = self.inner.call(request);
        Either::Right(Box::pin(async move {
            match response.await {
                Ok(response) => Ok(rpc.observe_response(response)),
                Err(error) => {
                    rpc.outcome = Outcome::plain(Code::Unknown);
                    drop(rpc);
                    Err(error)
                }
            }
        }))
    }
}

struct Outcome {
    code: Code,
    class: &'static str,
}

impl Outcome {
    const fn plain(code: Code) -> Self {
        Self {
            code,
            class: "none",
        }
    }

    fn from_status(status: &Status) -> Self {
        let class = status
            .get_details_error_info()
            .filter(|error_info| error_info.domain == ZINDER_ERROR_DOMAIN)
            .and_then(|error_info| ErrorReason::from_str_name(&error_info.reason))
            .map_or("none", |reason| reason.as_str_name());
        Self {
            code: status.code(),
            class,
        }
    }

    fn from_headers(headers: &HeaderMap) -> Option<Self> {
        let code = Code::from_bytes(headers.get(Status::GRPC_STATUS)?.as_bytes());
        if code != Code::Ok && headers.contains_key(Status::GRPC_STATUS_DETAILS) {
            let metadata = tonic::metadata::MetadataMap::from_headers(headers.clone());
            if let Some(details) = metadata
                .get_bin("grpc-status-details-bin")
                .and_then(|encoded| encoded.to_bytes().ok())
            {
                return Some(Self::from_status(&Status::with_details(code, "", details)));
            }
        }
        Some(Self::plain(code))
    }
}

fn code_label(code: Code) -> &'static str {
    match code {
        Code::Ok => "ok",
        Code::Cancelled => "cancelled",
        Code::Unknown => "unknown",
        Code::InvalidArgument => "invalid_argument",
        Code::DeadlineExceeded => "deadline_exceeded",
        Code::NotFound => "not_found",
        Code::AlreadyExists => "already_exists",
        Code::PermissionDenied => "permission_denied",
        Code::ResourceExhausted => "resource_exhausted",
        Code::FailedPrecondition => "failed_precondition",
        Code::Aborted => "aborted",
        Code::OutOfRange => "out_of_range",
        Code::Unimplemented => "unimplemented",
        Code::Internal => "internal",
        Code::Unavailable => "unavailable",
        Code::DataLoss => "data_loss",
        Code::Unauthenticated => "unauthenticated",
    }
}

#[derive(Clone, Copy)]
pub(crate) struct WalletRpcMethod {
    surface: &'static str,
    name: &'static str,
}

impl WalletRpcMethod {
    pub(crate) fn record_readiness_refusal(self) {
        metrics::counter!(
            READINESS_REFUSALS,
            "surface" => self.surface,
            "method" => self.name
        )
        .increment(1);
    }
}

struct ObservedRpc {
    method: WalletRpcMethod,
    shape: RpcShape,
    in_flight: metrics::Gauge,
    started: Instant,
    stream_opened: Option<Instant>,
    range_messages: f64,
    outcome: Outcome,
    /// Set when the client's `grpc-timeout` bounds the call, so a call the
    /// deadline abandoned records `DEADLINE_EXCEEDED`, the status the client
    /// receives, instead of the default cancellation.
    deadline: Option<DeadlineExpiry>,
}

impl ObservedRpc {
    fn start(method: WalletRpcMethod, shape: RpcShape) -> Self {
        let in_flight =
            metrics::gauge!(IN_FLIGHT, "surface" => method.surface, "method" => method.name);
        in_flight.increment(1);
        Self {
            method,
            shape,
            in_flight,
            started: Instant::now(),
            stream_opened: None,
            range_messages: 0.0,
            outcome: Outcome::plain(Code::Cancelled),
            deadline: None,
        }
    }

    fn observe_response(mut self, response: Response<Body>) -> Response<Body> {
        if let Some(outcome) = Outcome::from_headers(response.headers()) {
            self.outcome = outcome;
            return response;
        }
        if response.body().is_end_stream() {
            self.outcome = Outcome::plain(Code::Unknown);
            return response;
        }
        if !matches!(self.shape, RpcShape::Unary | RpcShape::Upload) {
            self.stream_opened = Some(Instant::now());
            metrics::gauge!(OPEN_STREAMS, "surface" => self.method.surface, "method" => self.method.name)
                .increment(1);
        }
        response.map(|body| {
            Body::new(ObservedBody {
                body,
                rpc: Some(self),
                boundaries: MessageBoundaries::default(),
            })
        })
    }

    fn is_range(&self) -> bool {
        matches!(
            self.shape,
            RpcShape::BlockRange | RpcShape::CompactRangeWithHeader
        )
    }
}

impl Drop for ObservedRpc {
    fn drop(&mut self) {
        if self.outcome.code == Code::Cancelled
            && self.deadline.as_ref().is_some_and(DeadlineExpiry::expired)
        {
            self.outcome = Outcome::plain(Code::DeadlineExceeded);
        }
        let labels = [
            ("surface", self.method.surface),
            ("method", self.method.name),
            ("grpc_code", code_label(self.outcome.code)),
            ("error_class", self.outcome.class),
        ];
        self.in_flight.decrement(1);
        metrics::counter!(REQUESTS, &labels).increment(1);
        metrics::histogram!(DURATION, &labels).record(self.started.elapsed());
        if let Some(opened) = self.stream_opened {
            metrics::gauge!(OPEN_STREAMS, "surface" => self.method.surface, "method" => self.method.name)
                .decrement(1);
            if self.is_range() {
                let header = f64::from(u8::from(self.shape == RpcShape::CompactRangeWithHeader));
                metrics::histogram!(RANGE_BLOCKS, &labels)
                    .record((self.range_messages - header).max(0.0));
                metrics::histogram!(RANGE_DURATION, &labels).record(opened.elapsed());
            }
        }
    }
}

struct ObservedUpload {
    body: Body,
    method: WalletRpcMethod,
    active: Option<metrics::Gauge>,
}

impl ObservedUpload {
    fn close(&mut self) {
        if let Some(gauge) = self.active.take() {
            gauge.decrement(1);
        }
    }
}

impl Drop for ObservedUpload {
    fn drop(&mut self) {
        self.close();
    }
}

impl http_body::Body for ObservedUpload {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let this = &mut *self;
        let polled = Pin::new(&mut this.body).poll_frame(cx);
        let receiving = match &polled {
            Poll::Pending => true,
            Poll::Ready(Some(Ok(frame))) => frame.is_data() && !this.body.is_end_stream(),
            Poll::Ready(None | Some(Err(_))) => false,
        };
        if receiving {
            this.active.get_or_insert_with(|| {
                let gauge = metrics::gauge!(OPEN_STREAMS, "surface" => this.method.surface, "method" => this.method.name);
                gauge.increment(1);
                gauge
            });
        } else {
            this.close();
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

struct ObservedBody {
    body: Body,
    rpc: Option<ObservedRpc>,
    boundaries: MessageBoundaries,
}

tokio::task_local! {
    static GATED_COMPLETION: RefCell<Option<RpcCompletion>>;
}

/// A terminal observation that the release gate may still be holding behind
/// DATA. Dropping it before release records the observer's default cancellation.
pub(super) struct RpcCompletion {
    rpc: ObservedRpc,
    outcome: Outcome,
}

impl RpcCompletion {
    /// Captures a completion even when a transport adapter turns the native
    /// trailers into DATA before the gate sees them.
    pub(super) fn capture<T>(held: Option<Self>, poll: impl FnOnce() -> T) -> (T, Option<Self>) {
        GATED_COMPLETION.sync_scope(RefCell::new(held), || {
            let frame = poll();
            (frame, GATED_COMPLETION.with(RefCell::take))
        })
    }

    pub(super) fn replace_status(status: &Status) {
        let _ = GATED_COMPLETION.try_with(|slot| {
            if let Some(completion) = slot.borrow_mut().as_mut() {
                completion.outcome = Outcome::from_status(status);
            }
        });
    }

    fn finish(self) {
        let mut completion = Some(self);
        let _ = GATED_COMPLETION.try_with(|slot| *slot.borrow_mut() = completion.take());
        if let Some(completion) = completion {
            completion.release();
        }
    }

    pub(super) fn release(mut self) {
        self.rpc.outcome = self.outcome;
    }
}

impl http_body::Body for ObservedBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let this = &mut *self;
        let frame = Pin::new(&mut this.body).poll_frame(cx);
        let Some(rpc) = &mut this.rpc else {
            return frame;
        };
        let outcome = match &frame {
            Poll::Pending => return frame,
            Poll::Ready(Some(Ok(next))) => {
                if let Some(chunk) = next.data_ref() {
                    if rpc.is_range() {
                        rpc.range_messages += f64::from(this.boundaries.completed_messages(chunk));
                    }
                    return frame;
                }
                next.trailers_ref()
                    .and_then(Outcome::from_headers)
                    .unwrap_or_else(|| Outcome::plain(Code::Unknown))
            }
            Poll::Ready(Some(Err(status))) => Outcome::from_status(status),
            Poll::Ready(None) => Outcome::plain(Code::Unknown),
        };
        if let Some(rpc) = this.rpc.take() {
            RpcCompletion { rpc, outcome }.finish();
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use eyre::{OptionExt as _, Result};
    use futures_util::future;
    use http_body_util::{BodyExt as _, StreamBody};
    use metrics_exporter_prometheus::PrometheusBuilder;
    use prost::Message as _;
    use prost_types::FileDescriptorSet;
    use std::{collections::BTreeMap, convert::Infallible};
    use tower::{ServiceExt as _, service_fn};

    use super::*;

    #[test]
    fn wallet_rpc_methods_and_stream_shapes_match_the_served_descriptors() -> Result<()> {
        for (surface, descriptors) in [
            (
                WalletRpcMetricsLayer::WalletQuery,
                zinder_proto::ZINDER_V1_FILE_DESCRIPTOR_SET,
            ),
            (
                WalletRpcMetricsLayer::Lightwalletd,
                zinder_proto::LIGHTWALLETD_COMPAT_FILE_DESCRIPTOR_SET,
            ),
        ] {
            let mut described = BTreeMap::new();
            for file in FileDescriptorSet::decode(descriptors)?.file {
                for service in &file.service {
                    if format!("{}.{}", file.package(), service.name()) == surface.service_name() {
                        for method in &service.method {
                            described.insert(
                                method.name().to_owned(),
                                (method.client_streaming(), method.server_streaming()),
                            );
                        }
                    }
                }
            }
            let measured = surface
                .methods()
                .iter()
                .map(|(name, shape)| {
                    (
                        (*name).to_owned(),
                        (
                            *shape == RpcShape::Upload,
                            !matches!(shape, RpcShape::Unary | RpcShape::Upload),
                        ),
                    )
                })
                .collect();
            assert_eq!(described, measured);
            assert_eq!(described.len(), surface.methods().len());
        }
        Ok(())
    }

    fn range_request() -> Result<Request<Body>> {
        Ok(
            Request::post("/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlockRange")
                .body(Body::empty())?,
        )
    }

    #[tokio::test]
    async fn wallet_rpc_stream_counts_only_complete_messages_through_trailers_or_drop() -> Result<()>
    {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _metrics = metrics::set_default_local_recorder(&recorder);
        let service = WalletRpcMetricsLayer::Lightwalletd.layer(service_fn(|_| async {
            let mut trailers = HeaderMap::new();
            Status::internal("opaque diagnostic").add_header(&mut trailers)?;
            let frames = [
                Frame::data(Bytes::from_static(b"\0\0\0\0\x01x\0\0\0\0\x01")),
                Frame::data(Bytes::from_static(b"y")),
                Frame::trailers(trailers),
            ];
            Ok::<_, Status>(Response::new(Body::new(StreamBody::new(
                tokio_stream::iter(frames.map(Ok::<_, Status>)),
            ))))
        }));
        let mut stream = service.clone().oneshot(range_request()?).await?.into_body();
        let mut cancelled = service.clone().oneshot(range_request()?).await?.into_body();
        let active = handle.render();
        for family in [IN_FLIGHT, OPEN_STREAMS] {
            assert!(
                active.contains(&format!(
                    r#"{family}{{surface="lightwalletd",method="GetBlockRange"}} 2"#
                )),
                "{active}"
            );
        }
        let first = cancelled
            .frame()
            .await
            .ok_or_eyre("missing first data frame")??;
        assert!(first.is_data());
        drop(cancelled);
        while let Some(frame) = stream.frame().await {
            frame?;
        }

        service
            .oneshot(
                Request::post("/cash.z.wallet.sdk.rpc.CompactTxStreamer/unrecognized-name")
                    .body(Body::empty())?,
            )
            .await?;
        let finished = handle.render();
        for (code, blocks) in [("internal", 2), ("cancelled", 1)] {
            let labels = format!(
                r#"{{surface="lightwalletd",method="GetBlockRange",grpc_code="{code}",error_class="none"}}"#
            );
            for sample in [
                format!("{REQUESTS}{labels} 1"),
                format!("{RANGE_BLOCKS}_sum{labels} {blocks}"),
                format!("{RANGE_DURATION}_count{labels} 1"),
            ] {
                assert!(
                    finished.lines().any(|line| line == sample),
                    "{sample}\n{finished}"
                );
            }
        }
        for family in [IN_FLIGHT, OPEN_STREAMS] {
            assert!(
                finished.contains(&format!(
                    r#"{family}{{surface="lightwalletd",method="GetBlockRange"}} 0"#
                )),
                "{finished}"
            );
        }
        assert!(!finished.contains("unrecognized-name"));
        assert!(!finished.contains("opaque diagnostic"));
        Ok(())
    }

    #[tokio::test]
    async fn wallet_rpc_abandoned_before_its_response_head_closes_only_the_request() -> Result<()> {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _metrics = metrics::set_default_local_recorder(&recorder);
        let mut service = WalletRpcMetricsLayer::Lightwalletd.layer(service_fn(|_| {
            future::pending::<Result<Response<Body>, Infallible>>()
        }));
        let response = service.ready().await?.call(range_request()?);
        let active = handle.render();
        assert!(
            active.contains(
                r#"zinder_wallet_rpc_in_flight{surface="lightwalletd",method="GetBlockRange"} 1"#
            ),
            "{active}"
        );
        drop(response);
        let finished = handle.render();
        assert!(finished.contains(r#"zinder_wallet_rpc_requests_total{surface="lightwalletd",method="GetBlockRange",grpc_code="cancelled",error_class="none"} 1"#), "{finished}");
        assert!(!finished.contains("zinder_wallet_rpc_block_range_blocks_sum{"));
        Ok(())
    }
}
