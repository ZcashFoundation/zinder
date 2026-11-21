//! Timed `CompactTxStreamer` calls and the samples they leave behind.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    ops::ControlFlow,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use prost::Message as _;
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use tonic::{
    Code, Status,
    transport::{Channel, Endpoint},
};
use zinder_proto::compat::lightwalletd::{
    BlockId, BlockRange, CompactBlock, TreeState, TxFilter,
    compact_tx_streamer_client::CompactTxStreamerClient,
};
use zinder_store::TREE_STATE_CHECKPOINT_STRIDE;

use super::transport::TlsOptions;

/// Client for one HTTP/2 connection to the endpoint under test.
pub(super) type LwdClient = CompactTxStreamerClient<Channel>;

/// How long a wallet waits for a connection to open, as Vizor does.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Longest status message kept as an example in the report.
const STATUS_EXAMPLE_CHARS: usize = 200;

/// Largest response message a simulated wallet accepts.
const MAX_RESPONSE_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// Deadlines one wallet engine applies to its calls.
#[derive(Clone, Copy, Debug, Serialize)]
pub(super) struct CallLimits {
    /// Deadline for a unary call.
    #[serde(serialize_with = "serialize_seconds")]
    pub(super) unary: Duration,
    /// Deadline for the first response of a block-range stream.
    #[serde(serialize_with = "serialize_seconds")]
    pub(super) stream_open: Duration,
    /// Longest silence between two blocks of one stream.
    #[serde(serialize_with = "serialize_optional_seconds")]
    pub(super) stream_idle: Option<Duration>,
    /// Deadline for a whole block-range stream.
    #[serde(serialize_with = "serialize_optional_seconds")]
    pub(super) stream_total: Option<Duration>,
}

/// Opens fresh connections to the endpoint under test and records every call.
#[derive(Clone)]
pub(super) struct Connector {
    endpoint: Endpoint,
    recorder: Arc<CallRecorder>,
}

impl Connector {
    /// Checks `url` and returns a connector for it, applying the TLS
    /// overrides to an `https` endpoint.
    pub(super) fn new(
        url: &str,
        recorder: Arc<CallRecorder>,
        tls_options: &TlsOptions,
    ) -> Result<Self, String> {
        Ok(Self {
            endpoint: super::transport::wallet_endpoint(url, CONNECT_TIMEOUT, tls_options)?,
            recorder,
        })
    }

    pub(super) fn recorder(&self) -> &CallRecorder {
        &self.recorder
    }

    /// The same endpoint recording into `recorder`, so setup calls stay out
    /// of the run's samples.
    pub(super) fn with_recorder(&self, recorder: Arc<CallRecorder>) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            recorder,
        }
    }

    /// Returns the connection in `slot`, opening one first when the slot is
    /// empty, as it is before a wallet's first call and after an error.
    pub(super) async fn reuse_or_connect<'slot>(
        &self,
        slot: &'slot mut Option<LwdClient>,
    ) -> Result<&'slot mut LwdClient, CallFailure> {
        let client = match slot.take() {
            Some(client) => client,
            None => self.connect().await?,
        };
        Ok(slot.insert(client))
    }

    /// Opens one new HTTP/2 connection, as a wallet does after an error.
    pub(super) async fn connect(&self) -> Result<LwdClient, CallFailure> {
        let mut pending = PendingCall::new(&self.recorder, "connect");
        let outcome = super::transport::connect(&self.endpoint, CONNECT_TIMEOUT)
            .await
            .map_err(|detail| CallFailure {
                label: "connect_failed",
                detail,
            });
        pending.finish(outcome.as_ref().map(drop));
        outcome.map(|channel| {
            CompactTxStreamerClient::new(channel)
                .max_decoding_message_size(MAX_RESPONSE_MESSAGE_BYTES)
        })
    }
}

/// A call that did not succeed, as the client observed it.
#[derive(Clone, Debug)]
pub(super) struct CallFailure {
    /// `snake_case` gRPC status name, or a client-side outcome such as
    /// `client_timeout`.
    pub(super) label: &'static str,
    /// Status message or local cause, kept out of aggregate counts.
    pub(super) detail: String,
}

impl CallFailure {
    fn from_status(status: &Status) -> Self {
        Self {
            label: status_label(status.code()),
            detail: status.message().to_owned(),
        }
    }

    fn client_timeout(what: &str, after: Duration) -> Self {
        Self {
            label: "client_timeout",
            detail: format!("{what} exceeded {}s", after.as_secs_f64()),
        }
    }
}

/// One call's outcome as recorded.
#[derive(Clone, Copy)]
pub(super) struct CallSample {
    status: &'static str,
    elapsed: Duration,
    first_message: Option<Duration>,
    last_message: Option<Duration>,
    messages: u64,
    bytes: u64,
    incomplete_stream: bool,
    /// Whether the call ended with the status its method expects to miss
    /// with, such as `not_found` for a transaction no chain contains.
    expected_miss: bool,
    /// Whether the client dropped the call after it delivered a message.
    cancelled_after_messages: bool,
}

/// Every call sample of one run, shared by all simulated wallets.
#[derive(Default)]
pub(super) struct CallRecorder {
    state: Mutex<RecorderState>,
}

#[derive(Default)]
struct RecorderState {
    calls: BTreeMap<&'static str, CallSamples>,
    request_counts: BTreeMap<[u8; 32], u64>,
    tree_state_heights: Vec<u64>,
    tree_state_frontiers: BTreeSet<[u8; 32]>,
}

#[derive(Default)]
struct CallSamples {
    statuses: BTreeMap<&'static str, u64>,
    status_examples: BTreeMap<&'static str, String>,
    ok_seconds: Vec<f64>,
    expected_miss_seconds: Vec<f64>,
    first_message_seconds: Vec<f64>,
    last_message_seconds: Vec<f64>,
    messages: u64,
    bytes: u64,
    incomplete_streams: u64,
    cancelled_after_messages: u64,
}

impl CallRecorder {
    fn note_request(&self, method: &'static str, request: &impl prost::Message) {
        let mut hash = Sha256::new();
        hash.update(method.as_bytes());
        hash.update([0]);
        hash.update(request.encode_to_vec());
        *self
            .state
            .lock()
            .request_counts
            .entry(hash.finalize().into())
            .or_default() += 1;
    }

    /// Hashes sorted request identities and their counts so concurrent wallets
    /// can issue the same work in different orders, including repeated reads.
    pub(super) fn request_identity_sha256(&self) -> String {
        let mut hash = Sha256::new();
        hash.update(b"zinder-wallet-load-requests-v1\0");
        for (identity, count) in &self.state.lock().request_counts {
            hash.update(identity);
            hash.update(count.to_be_bytes());
        }
        hex::encode(hash.finalize())
    }

    pub(super) fn record_call(&self, method: &'static str, sample: CallSample) {
        let mut state = self.state.lock();
        let samples = state.calls.entry(method).or_default();
        *samples.statuses.entry(sample.status).or_default() += 1;
        if sample.status == "ok" {
            samples.ok_seconds.push(sample.elapsed.as_secs_f64());
            if let Some(last_message) = sample.last_message {
                samples
                    .last_message_seconds
                    .push(last_message.as_secs_f64());
            }
        } else if sample.expected_miss {
            samples
                .expected_miss_seconds
                .push(sample.elapsed.as_secs_f64());
        }
        if let Some(first_message) = sample.first_message {
            samples
                .first_message_seconds
                .push(first_message.as_secs_f64());
        }
        samples.messages = samples.messages.saturating_add(sample.messages);
        samples.bytes = samples.bytes.saturating_add(sample.bytes);
        if sample.incomplete_stream {
            samples.incomplete_streams = samples.incomplete_streams.saturating_add(1);
        }
        if sample.cancelled_after_messages {
            samples.cancelled_after_messages = samples.cancelled_after_messages.saturating_add(1);
        }
        drop(state);
    }

    /// Keeps the first message seen for each failed status of `method`.
    fn note_status_example(&self, method: &'static str, status: &'static str, detail: &str) {
        let mut state = self.state.lock();
        state
            .calls
            .entry(method)
            .or_default()
            .status_examples
            .entry(status)
            .or_insert_with(|| detail.chars().take(STATUS_EXAMPLE_CHARS).collect());
        drop(state);
    }

    fn note_tree_state_height(&self, height: u64) {
        self.state.lock().tree_state_heights.push(height);
    }

    fn note_tree_state_frontier(&self, tree_state: &TreeState) {
        let digest = tree_state_frontier_digest(tree_state);
        self.state.lock().tree_state_frontiers.insert(digest);
    }

    /// Summarizes every recorded call by method.
    pub(super) fn rpc_summaries(&self) -> BTreeMap<&'static str, RpcSummary> {
        let state = self.state.lock();
        state
            .calls
            .iter()
            .map(|(method, samples)| {
                (
                    *method,
                    RpcSummary {
                        requests: samples.statuses.values().sum(),
                        statuses: samples.statuses.clone(),
                        status_examples: samples.status_examples.clone(),
                        ok_seconds: Percentiles::of(samples.ok_seconds.iter().copied()),
                        expected_miss_seconds: (!samples.expected_miss_seconds.is_empty()).then(
                            || Percentiles::of(samples.expected_miss_seconds.iter().copied()),
                        ),
                        first_message_seconds: (!samples.first_message_seconds.is_empty()).then(
                            || Percentiles::of(samples.first_message_seconds.iter().copied()),
                        ),
                        last_message_seconds: (!samples.last_message_seconds.is_empty())
                            .then(|| Percentiles::of(samples.last_message_seconds.iter().copied())),
                        messages: samples.messages,
                        bytes: samples.bytes,
                        incomplete_streams: samples.incomplete_streams,
                        cancelled_after_messages: samples.cancelled_after_messages,
                    },
                )
            })
            .collect()
    }

    /// Total protobuf-encoded bytes delivered across all recorded RPC responses.
    pub(super) fn total_rpc_bytes(&self) -> u64 {
        self.state
            .lock()
            .calls
            .values()
            .map(|samples| samples.bytes)
            .sum()
    }

    /// Blocks every `GetBlockRange` stream delivered and their encoded bytes,
    /// counting blocks a wallet rejected as out of sequence or past its
    /// range.
    pub(super) fn block_range_totals(&self) -> (u64, u64) {
        self.state
            .lock()
            .calls
            .get("GetBlockRange")
            .map_or((0, 0), |samples| (samples.messages, samples.bytes))
    }

    /// Total streams that ended early, failed after delivering at least one
    /// message, or that the client dropped after one.
    pub(super) fn incomplete_streams(&self) -> u64 {
        self.state
            .lock()
            .calls
            .values()
            .map(|samples| samples.incomplete_streams)
            .sum()
    }

    /// Summarizes the heights wallets asked tree states for.
    pub(super) fn tree_state_heights(&self) -> TreeStateHeights {
        let state = self.state.lock();
        TreeStateHeights::of(
            &state.tree_state_heights,
            u64::try_from(state.tree_state_frontiers.len()).unwrap_or(u64::MAX),
        )
    }
}

/// Digests a `TreeState`'s Sapling, Orchard, and Ironwood frontiers, so
/// heights that share one frontier count once.
pub(super) fn tree_state_frontier_digest(tree_state: &TreeState) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for tree in [
        &tree_state.sapling_tree,
        &tree_state.orchard_tree,
        &tree_state.ironwood_tree,
    ] {
        hasher.update(u64::try_from(tree.len()).unwrap_or(u64::MAX).to_le_bytes());
        hasher.update(tree.as_bytes());
    }
    hasher.finalize().into()
}

/// Client-observed outcome of one RPC method over the run.
#[derive(Debug, Serialize)]
pub(crate) struct RpcSummary {
    requests: u64,
    /// Requests by client-observed status: a `snake_case` gRPC status name,
    /// `client_timeout`, `client_cancelled` for a call the wallet dropped,
    /// `connect_failed` for a connection that did not open,
    /// `out_of_sequence` for a stream that sent a block other than the next
    /// height, or `incomplete_stream` for one that ended early.
    statuses: BTreeMap<&'static str, u64>,
    /// The first message the client received with each failed status.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    status_examples: BTreeMap<&'static str, String>,
    /// Latency of successful calls. A stream counts until its last block.
    ok_seconds: Percentiles,
    /// Latency of calls that ended with the status their method expects,
    /// such as `not_found` for `GetTransaction(missing)`. The calls keep
    /// that status in `statuses`.
    #[serde(skip_serializing_if = "Option::is_none")]
    expected_miss_seconds: Option<Percentiles>,
    /// Time from opening a stream to its first message.
    #[serde(skip_serializing_if = "Option::is_none")]
    first_message_seconds: Option<Percentiles>,
    /// Time from opening a stream to its last message before trailing EOF.
    #[serde(skip_serializing_if = "Option::is_none")]
    last_message_seconds: Option<Percentiles>,
    /// Messages received: each block of a stream, or a unary response.
    messages: u64,
    /// Protobuf-encoded size of the messages received, not bytes on the wire.
    bytes: u64,
    /// Streams that ended before the requested range, failed mid-stream, or
    /// that the client dropped after a message.
    incomplete_streams: u64,
    /// Calls the client dropped after receiving a message, which keep the
    /// `client_cancelled` status and also count in `incomplete_streams`.
    cancelled_after_messages: u64,
}

/// Requested tree-state heights against Zinder's construction checkpoints.
///
/// First-sync construction stores a checkpoint every 100 blocks, so a height
/// on that stride is a stored checkpoint. Below the first-sync tip, any other
/// height replays its offset from the stride, `height % 100` blocks. Above
/// that tip every block has a checkpoint, and a store whose checkpoint pass
/// has run has one at every settled block, so the offsets are an upper bound
/// on the replay work. `server_observed`, present when the run read the
/// server's metrics, is what the server actually did.
#[derive(Debug, Serialize)]
pub(crate) struct TreeStateHeights {
    requested: u64,
    distinct_heights: u64,
    distinct_frontiers: u64,
    on_cadence: u64,
    off_cadence: u64,
    off_cadence_offset_blocks: Percentiles,
    #[serde(skip_serializing_if = "Option::is_none")]
    server_observed: Option<super::server_metrics::ServerObserved>,
}

impl TreeStateHeights {
    /// Adds what the server counted over the run.
    pub(super) fn with_server_observed(
        mut self,
        observed: super::server_metrics::ServerObserved,
    ) -> Self {
        self.server_observed = Some(observed);
        self
    }
}

impl TreeStateHeights {
    fn of(heights: &[u64], distinct_frontiers: u64) -> Self {
        let distinct_heights =
            u64::try_from(heights.iter().collect::<BTreeSet<_>>().len()).unwrap_or(u64::MAX);
        let offsets = heights
            .iter()
            .map(|height| height % u64::from(TREE_STATE_CHECKPOINT_STRIDE))
            .filter(|offset| *offset != 0)
            .collect::<Vec<_>>();
        let requested = u64::try_from(heights.len()).unwrap_or(u64::MAX);
        let off_cadence = u64::try_from(offsets.len()).unwrap_or(u64::MAX);
        Self {
            requested,
            distinct_heights,
            distinct_frontiers,
            on_cadence: requested.saturating_sub(off_cadence),
            off_cadence,
            off_cadence_offset_blocks: Percentiles::of(offsets.into_iter().map(count_as_f64)),
            server_observed: None,
        }
    }
}

/// Nearest-rank percentiles over a set of samples.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub(crate) struct Percentiles {
    count: u64,
    p50: f64,
    p90: f64,
    p99: f64,
    max: f64,
}

impl Percentiles {
    pub(super) fn of(samples: impl Iterator<Item = f64>) -> Self {
        let mut sorted = samples.collect::<Vec<_>>();
        sorted.sort_by(f64::total_cmp);
        let nearest_rank = |percentile: usize| {
            let rank = sorted.len().saturating_mul(percentile).div_ceil(100);
            sorted
                .get(rank.saturating_sub(1))
                .copied()
                .unwrap_or_default()
        };
        Self {
            count: u64::try_from(sorted.len()).unwrap_or(u64::MAX),
            p50: nearest_rank(50),
            p90: nearest_rank(90),
            p99: nearest_rank(99),
            max: sorted.last().copied().unwrap_or_default(),
        }
    }
}

/// Converts a count to a sample value.
#[allow(
    clippy::cast_precision_loss,
    reason = "counts in a report stay far below 2^52, where f64 is exact"
)]
pub(super) const fn count_as_f64(count: u64) -> f64 {
    count as f64
}

/// Sends one unary call and records its outcome under `method`.
pub(super) async fn unary<Response, Call>(
    recorder: &CallRecorder,
    method: &'static str,
    deadline: Duration,
    call: Call,
) -> Result<Response, CallFailure>
where
    Response: prost::Message,
    Call: Future<Output = Result<tonic::Response<Response>, Status>>,
{
    let mut pending = PendingCall::new(recorder, method);
    let outcome = await_unary(&mut pending, deadline, call).await;
    pending.finish(outcome.as_ref().map(drop));
    outcome
}

/// Awaits one unary response within `deadline`, counting it on `pending`.
async fn await_unary<Response, Call>(
    pending: &mut PendingCall<'_>,
    deadline: Duration,
    call: Call,
) -> Result<Response, CallFailure>
where
    Response: prost::Message,
    Call: Future<Output = Result<tonic::Response<Response>, Status>>,
{
    match tokio::time::timeout(deadline, call).await {
        Ok(Ok(response)) => {
            let response = response.into_inner();
            pending.messages = 1;
            pending.bytes = u64::try_from(response.encoded_len()).unwrap_or(u64::MAX);
            Ok(response)
        }
        Ok(Err(status)) => Err(CallFailure::from_status(&status)),
        Err(_) => Err(CallFailure::client_timeout(pending.method, deadline)),
    }
}

/// Requests the tree state at `height` and records the height.
pub(super) async fn get_tree_state(
    recorder: &CallRecorder,
    client: &mut LwdClient,
    limits: &CallLimits,
    height: u64,
) -> Result<(), CallFailure> {
    get_tree_state_as(recorder, "GetTreeState", client, limits, height).await
}

/// Requests the tree state at `height`, recording it under `method`.
pub(super) async fn get_tree_state_as(
    recorder: &CallRecorder,
    method: &'static str,
    client: &mut LwdClient,
    limits: &CallLimits,
    height: u64,
) -> Result<(), CallFailure> {
    let request = BlockId {
        height,
        hash: Vec::new(),
    };
    recorder.note_tree_state_height(height);
    recorder.note_request("GetTreeState", &request);
    let tree_state = unary(
        recorder,
        method,
        limits.unary,
        client.get_tree_state(request),
    )
    .await?;
    recorder.note_tree_state_frontier(&tree_state);
    Ok(())
}

/// Requests one full transaction by its compact-block transaction ID.
pub(super) async fn get_transaction(
    recorder: &CallRecorder,
    mut client: LwdClient,
    limits: CallLimits,
    txid: Vec<u8>,
) -> Result<(), CallFailure> {
    let request = transaction_filter(txid);
    recorder.note_request("GetTransaction", &request);
    unary(
        recorder,
        "GetTransaction",
        limits.unary,
        client.get_transaction(request),
    )
    .await
    .map(drop)
}

fn transaction_filter(txid: Vec<u8>) -> TxFilter {
    TxFilter {
        block: None,
        index: 0,
        hash: txid,
    }
}

/// Which transaction a `GetTransaction` point read asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TransactionRead {
    /// A transaction mined in the range.
    Mined,
    /// A transaction that was in the mempool when the run started.
    Pending,
    /// A transaction no chain contains, which the server should not find.
    Missing,
}

/// Where a `GetTransaction` answer placed the transaction, from its
/// `RawTransaction.height`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransactionAnswer {
    /// Height 0: in the mempool.
    Pending,
    /// Height `u64::MAX`: mined on a fork that is not the main chain.
    Fork,
    /// Any other height: mined in the main chain.
    Mined,
}

impl TransactionAnswer {
    const fn of_height(height: u64) -> Self {
        match height {
            0 => Self::Pending,
            u64::MAX => Self::Fork,
            _ => Self::Mined,
        }
    }
}

impl TransactionRead {
    const fn requested_method(self) -> &'static str {
        match self {
            Self::Mined => "GetTransaction(mined)",
            Self::Pending => "GetTransaction(pending)",
            Self::Missing => "GetTransaction(missing)",
        }
    }

    /// The method an answered read is recorded under: the requested kind,
    /// plus where the answer placed the transaction when that differs, so a
    /// pending transaction that was mined during the run is reported apart.
    const fn answered_method(self, answer: TransactionAnswer) -> &'static str {
        match (self, answer) {
            (Self::Mined, TransactionAnswer::Mined) => "GetTransaction(mined)",
            (Self::Mined, TransactionAnswer::Pending) => "GetTransaction(mined, answered pending)",
            (Self::Mined, TransactionAnswer::Fork) => "GetTransaction(mined, answered fork)",
            (Self::Pending, TransactionAnswer::Pending) => "GetTransaction(pending)",
            (Self::Pending, TransactionAnswer::Mined) => "GetTransaction(pending, answered mined)",
            (Self::Pending, TransactionAnswer::Fork) => "GetTransaction(pending, answered fork)",
            (
                Self::Missing,
                TransactionAnswer::Pending | TransactionAnswer::Fork | TransactionAnswer::Mined,
            ) => "GetTransaction(missing, answered)",
        }
    }
}

/// Requests one full transaction for a point read. A `Missing` read that
/// ends with `not_found` keeps that status and records its latency as the
/// expected miss.
pub(super) async fn read_transaction(
    recorder: &CallRecorder,
    mut client: LwdClient,
    limits: CallLimits,
    read: TransactionRead,
    txid: Vec<u8>,
) -> Result<(), CallFailure> {
    let request = transaction_filter(txid);
    recorder.note_request("GetTransaction", &request);
    let mut pending = PendingCall::new(recorder, read.requested_method());
    if read == TransactionRead::Missing {
        pending.expected_miss = Some(status_label(Code::NotFound));
    }
    let outcome = await_unary(&mut pending, limits.unary, client.get_transaction(request))
        .await
        .map(|transaction| {
            pending.method = read.answered_method(TransactionAnswer::of_height(transaction.height));
        });
    pending.finish(outcome.as_ref().map(drop));
    outcome
}

/// Opens a server stream with `open` and reads it to its end, recording it
/// under `method`.
pub(super) async fn drain_stream<Reply, Open>(
    recorder: &CallRecorder,
    method: &'static str,
    limits: &CallLimits,
    open: Open,
) -> Result<u64, CallFailure>
where
    Reply: prost::Message + Default,
    Open: Future<Output = Result<tonic::Response<tonic::Streaming<Reply>>, Status>>,
{
    stream_each(recorder, method, limits, open, |reply| {
        drop(reply);
        ControlFlow::Continue(())
    })
    .await
}

/// Opens a server stream with `open` and hands each reply to `on_reply`,
/// recording the stream under `method`.
///
/// The stream ends successfully, without reading the rest, as soon as
/// `on_reply` returns `ControlFlow::Break`. It fails as a client timeout when
/// it outlasts `limits.stream_total`, after `on_reply` saw every reply that
/// arrived before.
pub(super) async fn stream_each<Reply, Open>(
    recorder: &CallRecorder,
    method: &'static str,
    limits: &CallLimits,
    open: Open,
    mut on_reply: impl FnMut(Reply) -> ControlFlow<()>,
) -> Result<u64, CallFailure>
where
    Reply: prost::Message + Default,
    Open: Future<Output = Result<tonic::Response<tonic::Streaming<Reply>>, Status>>,
{
    let mut pending = PendingCall::new(recorder, method);
    let stream = async {
        let mut replies = open_stream(limits, open).await?;
        while let Some(reply) = next_reply(limits, &mut replies).await? {
            pending.note_message(u64::try_from(reply.encoded_len()).unwrap_or(u64::MAX));
            if on_reply(reply).is_break() {
                break;
            }
        }
        Ok(pending.messages)
    };
    let outcome = match limits.stream_total {
        Some(total) => tokio::time::timeout(total, stream)
            .await
            .unwrap_or_else(|_| Err(CallFailure::client_timeout("stream", total))),
        None => stream.await,
    };
    pending.finish(outcome.as_ref().map(drop));
    outcome
}

async fn open_stream<Reply>(
    limits: &CallLimits,
    open: impl Future<Output = Result<tonic::Response<tonic::Streaming<Reply>>, Status>>,
) -> Result<tonic::Streaming<Reply>, CallFailure> {
    Ok(tokio::time::timeout(limits.stream_open, open)
        .await
        .map_err(|_| CallFailure::client_timeout("stream open", limits.stream_open))?
        .map_err(|status| CallFailure::from_status(&status))?
        .into_inner())
}

async fn next_reply<Reply>(
    limits: &CallLimits,
    replies: &mut tonic::Streaming<Reply>,
) -> Result<Option<Reply>, CallFailure> {
    match limits.stream_idle {
        Some(idle) => tokio::time::timeout(idle, replies.message())
            .await
            .map_err(|_| CallFailure::client_timeout("stream idle", idle))?,
        None => replies.message().await,
    }
    .map_err(|status| CallFailure::from_status(&status))
}

/// One `GetBlockRange` request.
pub(super) struct BlockRangeCall {
    pub(super) start: u64,
    pub(super) end: u64,
    pub(super) pool_types: Vec<i32>,
}

/// Streams `range`, handing each block and its encoded size to `on_block`
/// and awaiting the future it returns before reading the next block.
///
/// The stream fails as `out_of_sequence` when a block is not the next height
/// or lies past `range.end`, and as `incomplete_stream` when it ends before
/// `range.end`, because wallet engines reject both. A rejected block still
/// counts as received in the call's messages and bytes, but never reaches
/// `on_block`.
pub(super) async fn get_block_range<Handled: Future<Output = ()>>(
    recorder: &CallRecorder,
    client: &mut LwdClient,
    limits: &CallLimits,
    range: BlockRangeCall,
    mut on_block: impl FnMut(&CompactBlock, u64) -> Handled,
) -> Result<(), CallFailure> {
    let request = BlockRange {
        start: Some(BlockId {
            height: range.start,
            hash: Vec::new(),
        }),
        end: Some(BlockId {
            height: range.end,
            hash: Vec::new(),
        }),
        pool_types: range.pool_types,
    };
    recorder.note_request("GetBlockRange", &request);
    let mut pending = PendingCall::new(recorder, "GetBlockRange");
    let stream = async {
        let mut blocks = open_stream(limits, client.get_block_range(request)).await?;
        let mut expected = range.start;
        while let Some(block) = next_reply(limits, &mut blocks).await? {
            let encoded_bytes = u64::try_from(block.encoded_len()).unwrap_or(u64::MAX);
            pending.note_message(encoded_bytes);
            if block.height != expected || block.height > range.end {
                return Err(CallFailure {
                    label: "out_of_sequence",
                    detail: format!("expected block {expected}, received {}", block.height),
                });
            }
            expected = expected.saturating_add(1);
            on_block(&block, encoded_bytes).await;
        }
        if expected <= range.end {
            return Err(CallFailure {
                label: "incomplete_stream",
                detail: format!("stream ended before block {expected}"),
            });
        }
        Ok(())
    };
    let outcome = match limits.stream_total {
        Some(total) => tokio::time::timeout(total, stream)
            .await
            .unwrap_or_else(|_| Err(CallFailure::client_timeout("stream", total))),
        None => stream.await,
    };
    pending.finish(outcome.as_ref().map(drop));
    outcome
}

/// One call's counters, recorded when the call ends or the client drops it.
///
/// Vizor drops a batch's block stream when its tree-state request fails, and
/// aborts queued batches when a sync attempt fails. A call dropped after it
/// delivered a message keeps the `client_cancelled` status and counts as an
/// incomplete stream.
struct PendingCall<'run> {
    recorder: &'run CallRecorder,
    method: &'static str,
    expected_miss: Option<&'static str>,
    started: Instant,
    /// When the outcome was known, taken before the shared recorder lock so
    /// contention between wallets never counts toward a call's latency.
    ended: Option<Duration>,
    status: Option<&'static str>,
    detail: Option<String>,
    first_message: Option<Duration>,
    last_message: Option<Duration>,
    messages: u64,
    bytes: u64,
    incomplete_stream: bool,
}

impl<'run> PendingCall<'run> {
    fn new(recorder: &'run CallRecorder, method: &'static str) -> Self {
        Self {
            recorder,
            method,
            expected_miss: None,
            started: Instant::now(),
            ended: None,
            status: None,
            detail: None,
            first_message: None,
            last_message: None,
            messages: 0,
            bytes: 0,
            incomplete_stream: false,
        }
    }

    fn note_message(&mut self, encoded_bytes: u64) {
        let elapsed = self.started.elapsed();
        self.first_message.get_or_insert(elapsed);
        self.last_message = Some(elapsed);
        self.messages = self.messages.saturating_add(1);
        self.bytes = self.bytes.saturating_add(encoded_bytes);
    }

    /// Records the call's status. A stream that fails after delivering
    /// messages, or that a wallet engine would reject as short or out of
    /// order, counts as incomplete.
    fn finish(&mut self, outcome: Result<(), &CallFailure>) {
        self.ended = Some(self.started.elapsed());
        self.incomplete_stream = outcome.is_err_and(|failure| {
            self.messages > 0 || matches!(failure.label, "incomplete_stream" | "out_of_sequence")
        });
        self.status = Some(outcome.map_or_else(|failure| failure.label, |()| "ok"));
        self.detail = outcome.err().map(|failure| failure.detail.clone());
    }
}

impl Drop for PendingCall<'_> {
    fn drop(&mut self) {
        let elapsed = self.ended.unwrap_or_else(|| self.started.elapsed());
        let status = self.status.unwrap_or("client_cancelled");
        let cancelled_after_messages = self.status.is_none() && self.messages > 0;
        if let Some(detail) = &self.detail {
            self.recorder
                .note_status_example(self.method, status, detail);
        }
        self.recorder.record_call(
            self.method,
            CallSample {
                status,
                elapsed,
                first_message: self.first_message,
                last_message: self.last_message,
                messages: self.messages,
                bytes: self.bytes,
                incomplete_stream: self.incomplete_stream || cancelled_after_messages,
                expected_miss: self.expected_miss == Some(status),
                cancelled_after_messages,
            },
        );
    }
}

/// The `snake_case` name of a gRPC status code.
const fn status_label(code: Code) -> &'static str {
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

#[allow(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde's serialize_with passes the field by reference"
)]
fn serialize_seconds<S: serde::Serializer>(
    duration: &Duration,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_f64(duration.as_secs_f64())
}

#[allow(
    clippy::ref_option,
    reason = "serde's serialize_with passes the field by reference"
)]
fn serialize_optional_seconds<S: serde::Serializer>(
    duration: &Option<Duration>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match duration {
        Some(duration) => serializer.serialize_some(&duration.as_secs_f64()),
        None => serializer.serialize_none(),
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc, time::Duration};

    use prost::Message as _;
    use tonic::Status;
    use zinder_proto::compat::lightwalletd::TreeState;

    use super::{
        BlockRangeCall, CallLimits, CallRecorder, Connector, PendingCall, Percentiles,
        TreeStateHeights, get_block_range, tree_state_frontier_digest, unary,
    };
    use crate::wallet_load::{
        mock_streamer::{self, MockStreamer, RangeEnding},
        transport::TlsOptions,
    };

    const LIMITS: CallLimits = CallLimits {
        unary: Duration::from_secs(5),
        stream_open: Duration::from_secs(5),
        stream_idle: Some(Duration::from_secs(5)),
        stream_total: None,
    };

    #[tokio::test]
    async fn request_identity_ignores_order_but_detects_changed_txids_and_repeated_attempts()
    -> Result<(), Box<dyn Error>> {
        let server = mock_streamer::serve(MockStreamer::new(RangeEnding::Clean)).await?;
        let original = "ca892fceeb47ae135d5fcbbdb5c0e1de09f585ab90899c1be96f1621eeacc22d";
        let changed = "c767c571a53eed4bc64b4cb2c0bc5b821fe0d23f9e325d708aba0a246d21d76f";
        for (tree_first, txid_byte, expected) in [
            (true, 0x98, original),
            (false, 0x98, original),
            (false, 0x99, changed),
        ] {
            let recorder = Arc::new(CallRecorder::default());
            let connector =
                Connector::new(&server.url, Arc::clone(&recorder), &TlsOptions::default())?;
            let mut client = connector
                .connect()
                .await
                .map_err(|failure| failure.detail)?;
            if tree_first {
                super::get_tree_state(&recorder, &mut client, &LIMITS, 17)
                    .await
                    .map_err(|failure| failure.detail)?;
            }
            let range = BlockRangeCall {
                start: 1,
                end: 2,
                pool_types: vec![1, 2],
            };
            get_block_range(&recorder, &mut client, &LIMITS, range, |_, _| {
                std::future::ready(())
            })
            .await
            .map_err(|failure| failure.detail)?;
            let missing =
                super::get_transaction(&recorder, client.clone(), LIMITS, vec![txid_byte; 32])
                    .await;
            assert_eq!(
                missing.err().map(|failure| failure.label),
                Some("not_found")
            );
            if !tree_first {
                super::get_tree_state(&recorder, &mut client, &LIMITS, 17)
                    .await
                    .map_err(|failure| failure.detail)?;
            }
            assert_eq!(recorder.request_identity_sha256(), expected);
            super::get_tree_state(&recorder, &mut client, &LIMITS, 17)
                .await
                .map_err(|failure| failure.detail)?;
            assert_ne!(recorder.request_identity_sha256(), expected);
        }
        Ok(())
    }

    #[tokio::test]
    async fn a_block_past_the_range_counts_as_received_but_not_handled()
    -> Result<(), Box<dyn Error>> {
        let server = mock_streamer::serve(MockStreamer::new(RangeEnding::ExtraBlock)).await?;
        let recorder = Arc::new(CallRecorder::default());
        let connector = Connector::new(&server.url, Arc::clone(&recorder), &TlsOptions::default())?;
        let mut client = connector
            .connect()
            .await
            .map_err(|failure| failure.detail)?;
        let mut handled = Vec::new();

        let streamed = get_block_range(
            &recorder,
            &mut client,
            &LIMITS,
            BlockRangeCall {
                start: 1,
                end: 5,
                pool_types: Vec::new(),
            },
            |block, _| {
                handled.push(block.height);
                std::future::ready(())
            },
        )
        .await;

        assert_eq!(
            streamed.err().map(|failure| failure.label),
            Some("out_of_sequence")
        );
        assert_eq!(handled, [1, 2, 3, 4, 5]);
        let summary = &recorder.rpc_summaries()["GetBlockRange"];
        let block_bytes = u64::try_from(mock_streamer::block(1).encoded_len())?;
        assert_eq!(summary.messages, 6);
        assert_eq!(summary.bytes, 6 * block_bytes);
        assert_eq!(summary.incomplete_streams, 1);
        Ok(())
    }

    #[test]
    fn a_stream_dropped_after_a_message_counts_as_an_incomplete_cancellation() {
        let recorder = CallRecorder::default();
        let mut partial = PendingCall::new(&recorder, "GetBlockRange");
        partial.note_message(10);
        drop(partial);
        drop(PendingCall::new(&recorder, "GetBlockRange"));

        let summary = &recorder.rpc_summaries()["GetBlockRange"];
        assert_eq!(summary.statuses["client_cancelled"], 2);
        assert_eq!(summary.cancelled_after_messages, 1);
        assert_eq!(summary.incomplete_streams, 1);
        assert_eq!(recorder.incomplete_streams(), 1);
        assert_eq!(summary.bytes, 10);
    }

    /// Records `pending` while another thread holds the recorder lock for
    /// `held`, and returns the latencies recorded under `method`.
    fn record_under_contention(
        recorder: &Arc<CallRecorder>,
        pending: PendingCall<'_>,
        held: Duration,
    ) -> Result<(Vec<f64>, Vec<f64>), Box<dyn Error>> {
        let method = pending.method;
        let (locked, lock_taken) = std::sync::mpsc::channel();
        let holder = {
            let recorder = Arc::clone(recorder);
            std::thread::spawn(move || {
                let state = recorder.state.lock();
                let _ = locked.send(());
                std::thread::sleep(held);
                drop(state);
            })
        };
        lock_taken.recv()?;
        drop(pending);
        holder.join().map_err(|_| "lock holder panicked")?;
        let state = recorder.state.lock();
        let samples = state
            .calls
            .get(method)
            .map(|samples| {
                (
                    samples.expected_miss_seconds.clone(),
                    samples.ok_seconds.clone(),
                )
            })
            .ok_or("call not recorded");
        drop(state);
        Ok(samples?)
    }

    #[test]
    fn recorder_lock_contention_stays_out_of_call_latency() -> Result<(), Box<dyn Error>> {
        let held = Duration::from_millis(300);
        let recorder = Arc::new(CallRecorder::default());

        let mut miss = PendingCall::new(&recorder, "GetTransaction(missing)");
        miss.expected_miss = Some("not_found");
        miss.finish(Err(&super::CallFailure {
            label: "not_found",
            detail: "no such transaction".to_owned(),
        }));
        let (miss_seconds, _) = record_under_contention(&recorder, miss, held)?;

        let mut answered = PendingCall::new(&recorder, "GetTreeState");
        answered.finish(Ok(()));
        let (_, ok_seconds) = record_under_contention(&recorder, answered, held)?;

        let held_seconds = held.as_secs_f64();
        assert!(
            miss_seconds.len() == 1 && miss_seconds.iter().all(|&seconds| seconds < held_seconds),
            "{miss_seconds:?}"
        );
        assert!(
            ok_seconds.len() == 1 && ok_seconds.iter().all(|&seconds| seconds < held_seconds),
            "{ok_seconds:?}"
        );
        Ok(())
    }

    #[test]
    fn the_frontier_digest_covers_every_shielded_pool() {
        let base = TreeState {
            sapling_tree: "aa".to_owned(),
            orchard_tree: "bb".to_owned(),
            ..TreeState::default()
        };
        let with_ironwood = TreeState {
            ironwood_tree: "cc".to_owned(),
            ..base.clone()
        };

        assert_ne!(
            tree_state_frontier_digest(&base),
            tree_state_frontier_digest(&with_ironwood)
        );
    }

    #[tokio::test]
    async fn a_unary_response_counts_as_one_message_of_its_encoded_size() {
        let recorder = CallRecorder::default();
        let tree_state = TreeState {
            height: 3_424_810,
            sapling_tree: "00".repeat(40),
            ..TreeState::default()
        };
        let encoded_bytes = u64::try_from(tree_state.encoded_len()).unwrap_or(u64::MAX);

        let answered = unary(&recorder, "GetTreeState", Duration::from_secs(1), async {
            Ok(tonic::Response::new(tree_state))
        })
        .await;
        let refused = unary::<TreeState, _>(&recorder, "GetTreeState", Duration::from_secs(1), {
            async { Err(Status::unavailable("busy")) }
        })
        .await;

        assert!(answered.is_ok());
        assert!(refused.is_err());
        let summary = &recorder.rpc_summaries()["GetTreeState"];
        assert_eq!(summary.requests, 2);
        assert_eq!(summary.messages, 1);
        assert_eq!(summary.bytes, encoded_bytes);
        assert!(summary.first_message_seconds.is_none());
        assert!(summary.last_message_seconds.is_none());
    }

    #[test]
    fn tree_state_heights_count_replay_depth_and_distinct_frontiers() {
        let heights = TreeStateHeights::of(&[1_000, 1_099, 3_424_810, 3_424_809], 0);

        assert_eq!(heights.requested, 4);
        assert_eq!(heights.distinct_heights, 4);
        assert_eq!(heights.on_cadence, 1);
        assert_eq!(heights.off_cadence, 3);
        assert!((heights.off_cadence_offset_blocks.max - 99.0).abs() < f64::EPSILON);
        assert!((heights.off_cadence_offset_blocks.p50 - 10.0).abs() < f64::EPSILON);

        let recorder = CallRecorder::default();
        recorder.note_tree_state_height(100);
        recorder.note_tree_state_frontier(&TreeState {
            height: 100,
            sapling_tree: "aa".to_owned(),
            ..TreeState::default()
        });
        recorder.note_tree_state_height(101);
        recorder.note_tree_state_frontier(&TreeState {
            height: 101,
            sapling_tree: "aa".to_owned(),
            ..TreeState::default()
        });
        recorder.note_tree_state_height(102);
        recorder.note_tree_state_frontier(&TreeState {
            height: 102,
            sapling_tree: "bb".to_owned(),
            ..TreeState::default()
        });
        let summary = recorder.tree_state_heights();
        assert_eq!(summary.requested, 3);
        assert_eq!(summary.distinct_heights, 3);
        assert_eq!(summary.distinct_frontiers, 2);
    }

    #[test]
    fn percentiles_use_the_nearest_rank() {
        let summary = Percentiles::of((1..=10).map(f64::from));

        assert_eq!(summary.count, 10);
        assert!((summary.p50 - 5.0).abs() < f64::EPSILON);
        assert!((summary.p90 - 9.0).abs() < f64::EPSILON);
        assert!((summary.max - 10.0).abs() < f64::EPSILON);
        assert!(Percentiles::of(std::iter::empty()).max.abs() < f64::EPSILON);
    }
}
