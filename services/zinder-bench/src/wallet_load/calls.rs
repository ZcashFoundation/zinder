//! Timed `CompactTxStreamer` calls and the samples they leave behind.

use std::{
    collections::BTreeMap,
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use prost::Message as _;
use serde::Serialize;
use tonic::{Code, Status};
use zinder_proto::compat::lightwalletd::{
    BlockId, BlockRange, CompactBlock, TxFilter,
    compact_tx_streamer_client::CompactTxStreamerClient,
};
use zinder_runtime::{AuthenticatedChannel, connect_zinder_grpc, validate_zinder_grpc_endpoint};
use zinder_store::TREE_STATE_CHECKPOINT_STRIDE;

/// Client for one HTTP/2 connection to the endpoint under test. The channel
/// carries no bearer token.
pub(super) type LwdClient = CompactTxStreamerClient<AuthenticatedChannel>;

/// How long a wallet waits for a connection to open, as Vizor does.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

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
    /// Endpoint URL, already validated. An `https` URL connects over TLS
    /// with the platform's trusted roots.
    url: Arc<str>,
    recorder: Arc<CallRecorder>,
}

impl Connector {
    /// Checks `url` and returns a connector for it.
    pub(super) fn new(url: &str, recorder: Arc<CallRecorder>) -> Result<Self, String> {
        validate_zinder_grpc_endpoint(url).map_err(|error| error.to_string())?;
        Ok(Self {
            url: Arc::from(url),
            recorder,
        })
    }

    pub(super) fn recorder(&self) -> &CallRecorder {
        &self.recorder
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
        let outcome =
            match tokio::time::timeout(CONNECT_TIMEOUT, connect_zinder_grpc(&self.url, None)).await
            {
                Ok(Ok(channel)) => Ok(channel),
                Ok(Err(error)) => Err(CallFailure {
                    label: "connect_failed",
                    detail: error.to_string(),
                }),
                Err(_) => Err(CallFailure::client_timeout("connect", CONNECT_TIMEOUT)),
            };
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
    messages: u64,
    bytes: u64,
}

/// Every call sample of one run, shared by all simulated wallets.
#[derive(Default)]
pub(super) struct CallRecorder {
    state: Mutex<RecorderState>,
}

#[derive(Default)]
struct RecorderState {
    calls: BTreeMap<&'static str, CallSamples>,
    tree_state_heights: Vec<u64>,
}

#[derive(Default)]
struct CallSamples {
    statuses: BTreeMap<&'static str, u64>,
    ok_seconds: Vec<f64>,
    first_message_seconds: Vec<f64>,
    messages: u64,
    bytes: u64,
}

impl CallRecorder {
    pub(super) fn record_call(&self, method: &'static str, sample: CallSample) {
        let mut state = self.state.lock();
        let samples = state.calls.entry(method).or_default();
        *samples.statuses.entry(sample.status).or_default() += 1;
        if sample.status == "ok" {
            samples.ok_seconds.push(sample.elapsed.as_secs_f64());
        }
        if let Some(first_message) = sample.first_message {
            samples
                .first_message_seconds
                .push(first_message.as_secs_f64());
        }
        samples.messages = samples.messages.saturating_add(sample.messages);
        samples.bytes = samples.bytes.saturating_add(sample.bytes);
        drop(state);
    }

    fn note_tree_state_height(&self, height: u64) {
        self.state.lock().tree_state_heights.push(height);
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
                        ok_seconds: Percentiles::of(samples.ok_seconds.iter().copied()),
                        first_message_seconds: (!samples.first_message_seconds.is_empty()).then(
                            || Percentiles::of(samples.first_message_seconds.iter().copied()),
                        ),
                        messages: samples.messages,
                        bytes: samples.bytes,
                    },
                )
            })
            .collect()
    }

    /// Summarizes the heights wallets asked tree states for.
    pub(super) fn tree_state_heights(&self) -> TreeStateHeights {
        TreeStateHeights::of(&self.state.lock().tree_state_heights)
    }
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
    /// Latency of successful calls. A stream counts until its last block.
    ok_seconds: Percentiles,
    /// Time from opening a stream to its first block.
    #[serde(skip_serializing_if = "Option::is_none")]
    first_message_seconds: Option<Percentiles>,
    messages: u64,
    /// Protobuf-encoded size of the messages received, not bytes on the wire.
    bytes: u64,
}

/// Requested tree-state heights against Zinder's construction checkpoints.
///
/// First-sync construction stores a checkpoint every 100 blocks, so a height
/// on that stride is a stored checkpoint. Below the first-sync tip, any other
/// height replays its offset from the stride, `height % 100` blocks. Above
/// that tip every block has a checkpoint and nothing is replayed, so the
/// offsets are an upper bound on the replay work.
#[derive(Debug, Serialize)]
pub(crate) struct TreeStateHeights {
    requested: u64,
    on_cadence: u64,
    off_cadence: u64,
    off_cadence_offset_blocks: Percentiles,
}

impl TreeStateHeights {
    fn of(heights: &[u64]) -> Self {
        let offsets = heights
            .iter()
            .map(|height| height % u64::from(TREE_STATE_CHECKPOINT_STRIDE))
            .filter(|offset| *offset != 0)
            .collect::<Vec<_>>();
        let requested = u64::try_from(heights.len()).unwrap_or(u64::MAX);
        let off_cadence = u64::try_from(offsets.len()).unwrap_or(u64::MAX);
        Self {
            requested,
            on_cadence: requested.saturating_sub(off_cadence),
            off_cadence,
            off_cadence_offset_blocks: Percentiles::of(offsets.into_iter().map(count_as_f64)),
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
    Call: Future<Output = Result<tonic::Response<Response>, Status>>,
{
    let mut pending = PendingCall::new(recorder, method);
    let outcome = match tokio::time::timeout(deadline, call).await {
        Ok(Ok(response)) => Ok(response.into_inner()),
        Ok(Err(status)) => Err(CallFailure::from_status(&status)),
        Err(_) => Err(CallFailure::client_timeout(method, deadline)),
    };
    pending.finish(outcome.as_ref().map(drop));
    outcome
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
    recorder.note_tree_state_height(height);
    unary(
        recorder,
        method,
        limits.unary,
        client.get_tree_state(BlockId {
            height,
            hash: Vec::new(),
        }),
    )
    .await
    .map(drop)
}

/// Requests one full transaction by its compact-block transaction ID.
pub(super) async fn get_transaction(
    recorder: &CallRecorder,
    mut client: LwdClient,
    limits: CallLimits,
    txid: Vec<u8>,
) -> Result<(), CallFailure> {
    unary(
        recorder,
        "GetTransaction",
        limits.unary,
        client.get_transaction(TxFilter {
            block: None,
            index: 0,
            hash: txid,
        }),
    )
    .await
    .map(drop)
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
    let mut pending = PendingCall::new(recorder, method);
    let started = pending.started;
    let outcome = async {
        let mut replies = tokio::time::timeout(limits.stream_open, open)
            .await
            .map_err(|_| CallFailure::client_timeout("stream open", limits.stream_open))?
            .map_err(|status| CallFailure::from_status(&status))?
            .into_inner();
        loop {
            let next = match limits.stream_idle {
                Some(idle) => tokio::time::timeout(idle, replies.message())
                    .await
                    .map_err(|_| CallFailure::client_timeout("stream idle", idle))?,
                None => replies.message().await,
            }
            .map_err(|status| CallFailure::from_status(&status))?;
            let Some(reply) = next else { break };
            pending
                .first_message
                .get_or_insert_with(|| started.elapsed());
            pending.messages = pending.messages.saturating_add(1);
            pending.bytes = pending
                .bytes
                .saturating_add(u64::try_from(reply.encoded_len()).unwrap_or(u64::MAX));
        }
        Ok(pending.messages)
    }
    .await;
    pending.finish(outcome.as_ref().map(drop));
    outcome
}

/// One `GetBlockRange` request.
pub(super) struct BlockRangeCall {
    pub(super) start: u64,
    pub(super) end: u64,
    pub(super) pool_types: Vec<i32>,
}

/// Streams `range`, handing each block and its encoded size to `on_block`.
///
/// The stream fails as `out_of_sequence` when a block is not the next height
/// or lies past `range.end`,
/// and as `incomplete_stream` when it ends before `range.end`, because wallet
/// engines reject both.
pub(super) async fn get_block_range(
    recorder: &CallRecorder,
    client: &mut LwdClient,
    limits: &CallLimits,
    range: BlockRangeCall,
    mut on_block: impl FnMut(&CompactBlock, u64),
) -> Result<(), CallFailure> {
    let mut pending = PendingCall::new(recorder, "GetBlockRange");
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
    let stream = async {
        let response = tokio::time::timeout(limits.stream_open, client.get_block_range(request))
            .await
            .map_err(|_| CallFailure::client_timeout("stream open", limits.stream_open))?
            .map_err(|status| CallFailure::from_status(&status))?;
        let mut blocks = response.into_inner();
        let mut expected = range.start;
        loop {
            let next = match limits.stream_idle {
                Some(idle) => tokio::time::timeout(idle, blocks.message())
                    .await
                    .map_err(|_| CallFailure::client_timeout("stream idle", idle))?,
                None => blocks.message().await,
            }
            .map_err(|status| CallFailure::from_status(&status))?;
            let Some(block) = next else { break };
            if block.height != expected || block.height > range.end {
                return Err(CallFailure {
                    label: "out_of_sequence",
                    detail: format!("expected block {expected}, received {}", block.height),
                });
            }
            let encoded_bytes = u64::try_from(block.encoded_len()).unwrap_or(u64::MAX);
            let started = pending.started;
            pending
                .first_message
                .get_or_insert_with(|| started.elapsed());
            pending.messages = pending.messages.saturating_add(1);
            pending.bytes = pending.bytes.saturating_add(encoded_bytes);
            expected = expected.saturating_add(1);
            on_block(&block, encoded_bytes);
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
/// aborts queued batches when a sync attempt fails.
struct PendingCall<'run> {
    recorder: &'run CallRecorder,
    method: &'static str,
    started: Instant,
    status: Option<&'static str>,
    first_message: Option<Duration>,
    messages: u64,
    bytes: u64,
}

impl<'run> PendingCall<'run> {
    fn new(recorder: &'run CallRecorder, method: &'static str) -> Self {
        Self {
            recorder,
            method,
            started: Instant::now(),
            status: None,
            first_message: None,
            messages: 0,
            bytes: 0,
        }
    }

    fn finish(&mut self, outcome: Result<(), &CallFailure>) {
        self.status = Some(outcome.map_or_else(|failure| failure.label, |()| "ok"));
    }
}

impl Drop for PendingCall<'_> {
    fn drop(&mut self) {
        self.recorder.record_call(
            self.method,
            CallSample {
                status: self.status.unwrap_or("client_cancelled"),
                elapsed: self.started.elapsed(),
                first_message: self.first_message,
                messages: self.messages,
                bytes: self.bytes,
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
    use super::{Percentiles, TreeStateHeights};

    #[test]
    fn tree_state_heights_count_replay_depth_off_the_checkpoint_cadence() {
        let heights = TreeStateHeights::of(&[1_000, 1_099, 3_424_810, 3_424_809]);

        assert_eq!(heights.requested, 4);
        assert_eq!(heights.on_cadence, 1);
        assert_eq!(heights.off_cadence, 3);
        assert!((heights.off_cadence_offset_blocks.max - 99.0).abs() < f64::EPSILON);
        assert!((heights.off_cadence_offset_blocks.p50 - 10.0).abs() < f64::EPSILON);
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
