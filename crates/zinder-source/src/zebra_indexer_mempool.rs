//! Streaming Zebra indexer mempool source.
//!
//! Consumes Zebra's gRPC `Indexer.MempoolChange` stream (available when
//! Zebra is built with `--features indexer`) and hydrates `ADDED`
//! notifications through the `ZebraJsonRpcSource::fetch_raw_transaction_bytes`
//! call. Streaming is the preferred backend because it surfaces typed
//! `Invalidated` and `Mined` change types that the polling backend can
//! only synthesize from secondary lookups.
//!
//! # Reconnect contract
//!
//! Zebra's broadcast channel terminates the stream with `UNAVAILABLE` on
//! `RecvError::Lagged`; the source treats this as a transient error and
//! the consumer must reconnect. `mempool_change` only reports transitions
//! from the moment it opens, so every `events()` call also resnapshots the
//! upstream mempool via `getrawmempool` and emits a synthetic `Added` for
//! each already-present transaction; otherwise a transaction that entered
//! the mempool before the (re)connect would never be observed. The
//! resnapshot runs concurrently with, not before, draining the wire
//! stream. Wire observations are buffered until the resnapshot has been
//! forwarded, then the source emits
//! [`MempoolSourceEvent::InitialSnapshotComplete`]. The marker is a control
//! event, not a mempool lifecycle transition: it is the consumer's proof that
//! an initially empty index is complete enough to expose. Duplicate `Added`
//! events the two passes both produce for the same txid are a safe no-op
//! (`MempoolIndex::apply_added`).

use std::time::Duration;

use async_trait::async_trait;
use futures_util::{FutureExt as _, StreamExt as _, stream::TryStreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::transport::Channel;
use zinder_core::{AuthDigest, BlockId, MempoolEvictionReason, TransactionId, UnixTimestampMillis};
use zinder_proto::external::zebra_indexer_rpc::{
    Empty, MempoolChangeMessage, indexer_client::IndexerClient, mempool_change_message::ChangeType,
};

use crate::{
    ChainTipNotification, MempoolHydrationFailureReason, MempoolSource,
    MempoolSourceAdmissionLimits, MempoolSourceCapabilities, MempoolSourceEntry,
    MempoolSourceEvent, MempoolSourceEventStream, NodeIndexerTls, NodeSource, NodeTarget,
    SourceError, UpstreamTransactionLookup, ZebraIndexerChainTipSource,
    ZebraIndexerChainTipSourceOptions, ZebraJsonRpcSource, mempool_source::MempoolSourceAdmission,
};

const ZEBRA_INDEXER_STREAMING_BACKEND_LABEL: &str = "zebra_indexer_streaming";

/// Bounds concurrent `getrawtransaction` round trips during a resnapshot.
///
/// Applies while resnapshotting the mempool on `events()` (re)connect.
/// Mirrors the polling backend's concurrency budget
/// (`json_rpc_mempool::MEMPOOL_POLL_HYDRATION_CONCURRENCY`).
const MEMPOOL_RESNAPSHOT_HYDRATION_CONCURRENCY: usize = 16;

fn increment_streaming_hydration_failure(reason: MempoolHydrationFailureReason) {
    metrics::counter!(
        "zinder_mempool_hydration_failures_total",
        "backend" => ZEBRA_INDEXER_STREAMING_BACKEND_LABEL,
        "reason" => reason.as_label()
    )
    .increment(1);
}

/// Endpoint for a Zebra indexer-feature gRPC port.
///
/// The URL scheme selects the transport: `http://` is plaintext HTTP/2 and
/// `https://` negotiates TLS, configured by [`Self::tls`].
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ZebraIndexerSourceTarget {
    /// HTTP/2 endpoint URL of the Zebra indexer port (e.g.
    /// `http://127.0.0.1:8154`).
    pub endpoint_url: String,
    /// TLS settings for an `https://` endpoint. `None` verifies the node
    /// against the system roots and presents no client certificate.
    pub tls: Option<NodeIndexerTls>,
}

impl ZebraIndexerSourceTarget {
    /// Creates an indexer source target from an endpoint URL.
    pub fn new(endpoint_url: impl Into<String>) -> Self {
        Self {
            endpoint_url: endpoint_url.into(),
            tls: None,
        }
    }

    /// Returns this target with `tls` applied to its `https://` channel.
    #[must_use]
    pub fn with_tls(mut self, tls: NodeIndexerTls) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Returns the indexer target `node` configures, or `None` when it sets no
    /// indexer endpoint.
    #[must_use]
    pub fn from_node_target(node: &NodeTarget) -> Option<Self> {
        node.indexer_grpc_addr.as_ref().map(|endpoint_url| Self {
            endpoint_url: endpoint_url.clone(),
            tls: node.indexer_tls.clone(),
        })
    }
}

/// Runtime options for [`ZebraIndexerMempoolSource`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ZebraIndexerMempoolSourceOptions {
    /// Connect timeout for the indexer endpoint.
    pub connect_timeout: Duration,
    /// Request timeout for the streaming RPC call.
    pub request_timeout: Duration,
    /// Channel buffer size for the emitted source event stream.
    pub event_channel_capacity: usize,
    /// Complete-generation transaction-count and raw-byte limits.
    pub admission_limits: MempoolSourceAdmissionLimits,
}

impl Default for ZebraIndexerMempoolSourceOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_mins(1),
            event_channel_capacity: 256,
            admission_limits: MempoolSourceAdmissionLimits::default(),
        }
    }
}

/// Streaming mempool source backed by Zebra's gRPC indexer port.
#[derive(Clone)]
pub struct ZebraIndexerMempoolSource {
    target: ZebraIndexerSourceTarget,
    hydration_json_rpc: ZebraJsonRpcSource,
    options: ZebraIndexerMempoolSourceOptions,
}

impl ZebraIndexerMempoolSource {
    /// Creates a streaming mempool source.
    ///
    /// `target` points at the Zebra indexer gRPC endpoint;
    /// `hydration_json_rpc` is the JSON-RPC source used to fetch raw
    /// transaction bytes for `ADDED` change notifications. Both must
    /// point at the same Zebra deployment to avoid mempool-state
    /// inconsistency.
    #[must_use]
    pub fn new(target: ZebraIndexerSourceTarget, hydration_json_rpc: ZebraJsonRpcSource) -> Self {
        Self::with_options(
            target,
            hydration_json_rpc,
            ZebraIndexerMempoolSourceOptions::default(),
        )
    }

    /// Creates a streaming mempool source with explicit options.
    #[must_use]
    pub const fn with_options(
        target: ZebraIndexerSourceTarget,
        hydration_json_rpc: ZebraJsonRpcSource,
        options: ZebraIndexerMempoolSourceOptions,
    ) -> Self {
        Self {
            target,
            hydration_json_rpc,
            options,
        }
    }

    async fn connect(&self) -> Result<IndexerClient<Channel>, SourceError> {
        let channel = crate::transport::connect_zebra_indexer_channel(
            &self.target,
            crate::transport::ZebraIndexerChannelOptions {
                connect_timeout: self.options.connect_timeout,
                request_timeout: self.options.request_timeout,
            },
        )
        .await
        .map_err(|error| SourceError::MempoolStreamUnavailable {
            reason: error.to_string(),
        })?;
        Ok(IndexerClient::new(channel))
    }
}

#[async_trait]
impl MempoolSource for ZebraIndexerMempoolSource {
    fn capabilities(&self) -> MempoolSourceCapabilities {
        MempoolSourceCapabilities::streaming()
    }

    async fn events(&self) -> Result<MempoolSourceEventStream, SourceError> {
        let chain_tip_stream = ZebraIndexerChainTipSource::with_options(
            self.target.clone(),
            ZebraIndexerChainTipSourceOptions {
                connect_timeout: self.options.connect_timeout,
                request_timeout: self.options.request_timeout,
                notification_channel_capacity: self.options.event_channel_capacity,
            },
        )
        .subscribe()
        .await?;
        let mut indexer_client = self.connect().await?;
        let response = indexer_client
            .mempool_change(Request::new(Empty {}))
            .await
            .map_err(|status| SourceError::MempoolStreamUnavailable {
                reason: format!("indexer mempool_change call failed: {status}"),
            })?;
        let wire_stream = response.into_inner();

        let (event_sender, event_receiver) = mpsc::channel(self.options.event_channel_capacity);
        let (wire_sender, wire_receiver) = mpsc::channel(self.options.event_channel_capacity);
        spawn_wire_event_pump(wire_stream, self.hydration_json_rpc.clone(), wire_sender);
        spawn_certified_event_aggregator(
            self.hydration_json_rpc.clone(),
            self.options.admission_limits,
            chain_tip_stream,
            wire_receiver,
            event_sender,
        );

        Ok(Box::pin(ReceiverStream::new(event_receiver)))
    }
}

fn spawn_wire_event_pump(
    mut wire_stream: tonic::Streaming<MempoolChangeMessage>,
    hydration_json_rpc: ZebraJsonRpcSource,
    wire_sender: mpsc::Sender<Result<MempoolSourceEvent, SourceError>>,
) {
    tokio::spawn(async move {
        loop {
            match wire_stream.message().await {
                Ok(Some(wire_message)) => {
                    if matches!(
                        forward_wire_message(&hydration_json_rpc, wire_message, &wire_sender).await,
                        ForwardOutcome::ChannelClosed
                    ) {
                        return;
                    }
                }
                Ok(None) => return,
                Err(stream_status) => {
                    let _ = wire_sender
                        .send(Err(SourceError::MempoolStreamUnavailable {
                            reason: format!("indexer mempool_change stream ended: {stream_status}"),
                        }))
                        .await;
                    return;
                }
            }
        }
    });
}

fn spawn_certified_event_aggregator(
    snapshot_json_rpc: ZebraJsonRpcSource,
    admission_limits: MempoolSourceAdmissionLimits,
    mut chain_tip_stream: crate::ChainTipNotificationStream,
    mut wire_receiver: mpsc::Receiver<Result<MempoolSourceEvent, SourceError>>,
    event_sender: mpsc::Sender<Result<MempoolSourceEvent, SourceError>>,
) {
    tokio::spawn(async move {
        let (source_tip, mut admission) =
            match resnapshot_current_mempool(snapshot_json_rpc, admission_limits, &event_sender)
                .await
            {
                Ok(ResnapshotOutcome::Complete {
                    source_tip,
                    admission,
                }) => (source_tip, admission),
                Ok(ResnapshotOutcome::SourceTipChanged {
                    generation_source_tip,
                    observed_source_tip,
                }) => {
                    let _ = forward_source_tip_changed(
                        generation_source_tip,
                        observed_source_tip,
                        &event_sender,
                    )
                    .await;
                    return;
                }
                Err(source_error) => {
                    let _ = forward_error(source_error, &event_sender).await;
                    return;
                }
            };
        if matches!(
            flush_certification_prefix(
                source_tip,
                &mut admission,
                &mut chain_tip_stream,
                &mut wire_receiver,
                &event_sender,
            )
            .await,
            GenerationOutcome::Ended
        ) {
            return;
        }
        if matches!(
            publish_certified_snapshot_marker(source_tip, &mut chain_tip_stream, &event_sender,)
                .await,
            GenerationOutcome::Ended
        ) {
            return;
        }
        forward_certified_generation(
            source_tip,
            &mut admission,
            &mut chain_tip_stream,
            &mut wire_receiver,
            &event_sender,
        )
        .await;
    });
}

async fn flush_certification_prefix(
    source_tip: BlockId,
    admission: &mut MempoolSourceAdmission,
    chain_tip_stream: &mut crate::ChainTipNotificationStream,
    wire_receiver: &mut mpsc::Receiver<Result<MempoolSourceEvent, SourceError>>,
    event_sender: &mpsc::Sender<Result<MempoolSourceEvent, SourceError>>,
) -> GenerationOutcome {
    loop {
        match validate_pending_tip_notifications(source_tip, chain_tip_stream) {
            Ok(TipCoherence::Stable) => {}
            Ok(TipCoherence::Changed {
                observed_source_tip,
            }) => {
                let _ =
                    forward_source_tip_changed(source_tip, observed_source_tip, event_sender).await;
                return GenerationOutcome::Ended;
            }
            Err(source_error) => {
                let _ = forward_error(source_error, event_sender).await;
                return GenerationOutcome::Ended;
            }
        }
        match wire_receiver.try_recv() {
            Ok(wire_event) => {
                if matches!(
                    forward_admitted_result(wire_event, admission, event_sender).await,
                    ForwardOutcome::ChannelClosed
                ) {
                    return GenerationOutcome::Ended;
                }
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                return GenerationOutcome::Continue;
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                let _ = forward_error(
                    SourceError::MempoolStreamUnavailable {
                        reason: "indexer mempool-change stream ended before the generation was certified".to_owned(),
                    },
                    event_sender,
                )
                .await;
                return GenerationOutcome::Ended;
            }
        }
    }
}

async fn publish_certified_snapshot_marker(
    source_tip: BlockId,
    chain_tip_stream: &mut crate::ChainTipNotificationStream,
    event_sender: &mpsc::Sender<Result<MempoolSourceEvent, SourceError>>,
) -> GenerationOutcome {
    let Ok(marker_permit) = event_sender.reserve().await else {
        return GenerationOutcome::Ended;
    };
    match validate_pending_tip_notifications(source_tip, chain_tip_stream) {
        Ok(TipCoherence::Stable) => {}
        Ok(TipCoherence::Changed {
            observed_source_tip,
        }) => {
            marker_permit.send(Ok(MempoolSourceEvent::SourceTipChanged {
                generation_source_tip: source_tip,
                observed_source_tip,
            }));
            return GenerationOutcome::Ended;
        }
        Err(source_error) => {
            drop(marker_permit);
            let _ = forward_error(source_error, event_sender).await;
            return GenerationOutcome::Ended;
        }
    }
    marker_permit.send(Ok(MempoolSourceEvent::InitialSnapshotComplete {
        source_tip,
    }));
    GenerationOutcome::Continue
}

fn validate_pending_tip_notifications(
    source_tip: BlockId,
    chain_tip_stream: &mut crate::ChainTipNotificationStream,
) -> Result<TipCoherence, SourceError> {
    loop {
        match chain_tip_stream.next().now_or_never() {
            None => return Ok(TipCoherence::Stable),
            Some(Some(chain_tip_result)) => {
                match coherent_tip_notification(source_tip, chain_tip_result)? {
                    TipCoherence::Stable => {}
                    changed @ TipCoherence::Changed { .. } => return Ok(changed),
                }
            }
            Some(None) => {
                return Err(SourceError::MempoolStreamUnavailable {
                    reason: "indexer chain-tip monitor ended before the mempool generation was certified".to_owned(),
                });
            }
        }
    }
}

async fn forward_certified_generation(
    source_tip: BlockId,
    admission: &mut MempoolSourceAdmission,
    chain_tip_stream: &mut crate::ChainTipNotificationStream,
    wire_receiver: &mut mpsc::Receiver<Result<MempoolSourceEvent, SourceError>>,
    event_sender: &mpsc::Sender<Result<MempoolSourceEvent, SourceError>>,
) {
    loop {
        tokio::select! {
            // The two Zebra broadcasts have no shared sequence. If both are
            // queued, withdraw before forwarding a delta. A delta that arrives
            // before its tip notification remains an upstream ordering limit;
            // owner reads still fail closed when the canonical tip advances.
            biased;
            chain_tip_result = chain_tip_stream.next() => {
                match chain_tip_result {
                    Some(chain_tip_result) => match coherent_tip_notification(
                        source_tip,
                        chain_tip_result,
                    ) {
                        Ok(TipCoherence::Stable) => continue,
                        Ok(TipCoherence::Changed { observed_source_tip }) => {
                            let _ = forward_source_tip_changed(
                                source_tip,
                                observed_source_tip,
                                event_sender,
                            ).await;
                        }
                        Err(source_error) => {
                            let _ = forward_error(source_error, event_sender).await;
                        }
                    },
                    None => {
                        let _ = forward_error(
                            SourceError::MempoolStreamUnavailable {
                                reason: "indexer chain-tip monitor ended while serving a certified mempool generation".to_owned(),
                            },
                            event_sender,
                        ).await;
                    }
                }
                return;
            }
            wire_event = wire_receiver.recv() => {
                let Some(wire_event) = wire_event else {
                    let _ = forward_error(
                        SourceError::MempoolStreamUnavailable {
                            reason: "indexer mempool-change stream ended while serving a certified mempool generation".to_owned(),
                        },
                        event_sender,
                    ).await;
                    return;
                };
                if matches!(
                    forward_admitted_result(wire_event, admission, event_sender).await,
                    ForwardOutcome::ChannelClosed
                ) {
                    return;
                }
            }
        }
    }
}

fn coherent_tip_notification(
    generation_source_tip: BlockId,
    chain_tip_result: Result<ChainTipNotification, SourceError>,
) -> Result<TipCoherence, SourceError> {
    let notification =
        chain_tip_result.map_err(|source_error| SourceError::MempoolStreamUnavailable {
            reason: format!("indexer chain-tip monitor failed: {source_error}"),
        })?;
    if notification.tip_id == generation_source_tip {
        return Ok(TipCoherence::Stable);
    }
    Ok(TipCoherence::Changed {
        observed_source_tip: notification.tip_id,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TipCoherence {
    Stable,
    Changed { observed_source_tip: BlockId },
}

/// Emits a synthetic `Added` event for every transaction already sitting in
/// the upstream mempool at (re)connect time.
///
/// `mempool_change` only reports transitions from the moment it opens, so
/// without this pass a subscriber that (re)connects mid-stream would never
/// observe transactions that arrived before that point. Runs concurrently
/// with the wire-stream drain rather than before it, so its
/// `getrawtransaction` round trips never delay live delta delivery.
async fn resnapshot_current_mempool(
    hydration_json_rpc: ZebraJsonRpcSource,
    admission_limits: MempoolSourceAdmissionLimits,
    event_sender: &mpsc::Sender<Result<MempoolSourceEvent, SourceError>>,
) -> Result<ResnapshotOutcome, SourceError> {
    let source_tip_before = hydration_json_rpc.tip_id().await?;
    let observed_at = UnixTimestampMillis::now();
    let mut transaction_ids = hydration_json_rpc
        .fetch_raw_mempool_transaction_ids()
        .await?;
    transaction_ids.sort_unstable();
    transaction_ids.dedup();
    let mut admission = MempoolSourceAdmission::new(admission_limits);
    admission.validate_snapshot_transaction_count(transaction_ids.len())?;

    let mut source_events =
        futures_util::stream::iter(transaction_ids.into_iter().map(Ok::<_, SourceError>))
            .map_ok(|transaction_id| {
                let hydration_json_rpc = &hydration_json_rpc;
                async move {
                    build_added_event(hydration_json_rpc, transaction_id, None, observed_at).await
                }
            })
            .try_buffer_unordered(MEMPOOL_RESNAPSHOT_HYDRATION_CONCURRENCY);
    loop {
        let source_event = match source_events.try_next().await {
            Ok(Some(source_event)) => source_event,
            Ok(None) => break,
            Err(source_error) => {
                if matches!(&source_error, SourceError::MempoolHydrationFailed { .. }) {
                    let observed_source_tip = hydration_json_rpc.tip_id().await?;
                    if source_tip_before != observed_source_tip {
                        return Ok(ResnapshotOutcome::SourceTipChanged {
                            generation_source_tip: source_tip_before,
                            observed_source_tip,
                        });
                    }
                }
                return Err(source_error);
            }
        };
        admit_source_event(&mut admission, &source_event)?;
        if matches!(
            forward_event(source_event, event_sender).await,
            ForwardOutcome::ChannelClosed
        ) {
            return Err(SourceError::MempoolStreamUnavailable {
                reason: "mempool snapshot receiver closed".to_owned(),
            });
        }
    }
    let source_tip_after = hydration_json_rpc.tip_id().await?;
    if source_tip_before != source_tip_after {
        return Ok(ResnapshotOutcome::SourceTipChanged {
            generation_source_tip: source_tip_before,
            observed_source_tip: source_tip_after,
        });
    }
    Ok(ResnapshotOutcome::Complete {
        source_tip: source_tip_after,
        admission,
    })
}

enum ResnapshotOutcome {
    Complete {
        source_tip: BlockId,
        admission: MempoolSourceAdmission,
    },
    SourceTipChanged {
        generation_source_tip: BlockId,
        observed_source_tip: BlockId,
    },
}

enum GenerationOutcome {
    Continue,
    Ended,
}

/// Whether the consumer is still listening to the source-event channel.
enum ForwardOutcome {
    Continue,
    ChannelClosed,
}

async fn forward_wire_message(
    hydration_json_rpc: &ZebraJsonRpcSource,
    wire_message: MempoolChangeMessage,
    event_sender: &mpsc::Sender<Result<MempoolSourceEvent, SourceError>>,
) -> ForwardOutcome {
    let observed_at = UnixTimestampMillis::now();
    let transaction_id = match decode_transaction_id(&wire_message.tx_hash) {
        Ok(transaction_id) => transaction_id,
        Err(decode_error) => return forward_error(decode_error, event_sender).await,
    };
    let auth_digest = match decode_auth_digest(&wire_message.auth_digest) {
        Ok(auth_digest) => auth_digest,
        Err(decode_error) => return forward_error(decode_error, event_sender).await,
    };

    let outcome = match ChangeType::try_from(wire_message.change_type) {
        Ok(ChangeType::Added) => {
            build_added_event(hydration_json_rpc, transaction_id, auth_digest, observed_at).await
        }
        Ok(ChangeType::Invalidated) => Ok(MempoolSourceEvent::Invalidated {
            transaction_id,
            reason: MempoolEvictionReason::Unknown,
        }),
        Ok(ChangeType::Mined) => build_mined_event(hydration_json_rpc, transaction_id).await,
        Err(_) => Err(SourceError::SourceProtocolMismatch {
            reason: "Zebra indexer mempool_change wire reported unrecognized ChangeType",
        }),
    };

    match outcome {
        Ok(source_event) => forward_event(source_event, event_sender).await,
        Err(source_error) => forward_error(source_error, event_sender).await,
    }
}

async fn forward_event(
    source_event: MempoolSourceEvent,
    event_sender: &mpsc::Sender<Result<MempoolSourceEvent, SourceError>>,
) -> ForwardOutcome {
    if event_sender.send(Ok(source_event)).await.is_err() {
        ForwardOutcome::ChannelClosed
    } else {
        ForwardOutcome::Continue
    }
}

async fn forward_error(
    source_error: SourceError,
    event_sender: &mpsc::Sender<Result<MempoolSourceEvent, SourceError>>,
) -> ForwardOutcome {
    if event_sender.send(Err(source_error)).await.is_err() {
        ForwardOutcome::ChannelClosed
    } else {
        ForwardOutcome::Continue
    }
}

async fn forward_source_tip_changed(
    generation_source_tip: BlockId,
    observed_source_tip: BlockId,
    event_sender: &mpsc::Sender<Result<MempoolSourceEvent, SourceError>>,
) -> ForwardOutcome {
    forward_event(
        MempoolSourceEvent::SourceTipChanged {
            generation_source_tip,
            observed_source_tip,
        },
        event_sender,
    )
    .await
}

async fn forward_admitted_result(
    source_result: Result<MempoolSourceEvent, SourceError>,
    admission: &mut MempoolSourceAdmission,
    event_sender: &mpsc::Sender<Result<MempoolSourceEvent, SourceError>>,
) -> ForwardOutcome {
    match source_result {
        Ok(source_event) => {
            if let Err(source_error) = admit_source_event(admission, &source_event) {
                let _ = forward_error(source_error, event_sender).await;
                return ForwardOutcome::ChannelClosed;
            }
            forward_event(source_event, event_sender).await
        }
        Err(source_error) => forward_error(source_error, event_sender).await,
    }
}

fn admit_source_event(
    admission: &mut MempoolSourceAdmission,
    source_event: &MempoolSourceEvent,
) -> Result<(), SourceError> {
    match source_event {
        MempoolSourceEvent::Added(entry) => admission.admit_added_entry(entry),
        MempoolSourceEvent::Invalidated { transaction_id, .. }
        | MempoolSourceEvent::Mined { transaction_id, .. } => {
            admission.remove_transaction(*transaction_id);
            Ok(())
        }
        MempoolSourceEvent::InitialSnapshotComplete { .. }
        | MempoolSourceEvent::SourceTipChanged { .. } => Ok(()),
    }
}

async fn build_mined_event(
    hydration_json_rpc: &ZebraJsonRpcSource,
    transaction_id: TransactionId,
) -> Result<MempoolSourceEvent, SourceError> {
    match hydration_json_rpc
        .fetch_upstream_transaction_lookup(transaction_id)
        .await?
    {
        UpstreamTransactionLookup::Mined {
            mined_height,
            block_hash,
        } => Ok(MempoolSourceEvent::Mined {
            transaction_id,
            mined_height,
            block_hash,
        }),
        UpstreamTransactionLookup::InMempool | UpstreamTransactionLookup::NotFound => {
            // Race window: streaming reported MINED but the follow-up
            // lookup observed a different state (e.g. fork or restoration
            // to mempool). Surface as Invalidated{Unknown} so downstream
            // does not over-report Mined heights.
            Ok(MempoolSourceEvent::Invalidated {
                transaction_id,
                reason: MempoolEvictionReason::Unknown,
            })
        }
    }
}

async fn build_added_event(
    hydration_json_rpc: &ZebraJsonRpcSource,
    transaction_id: TransactionId,
    auth_digest: Option<AuthDigest>,
    observed_at: UnixTimestampMillis,
) -> Result<MempoolSourceEvent, SourceError> {
    match hydration_json_rpc
        .fetch_raw_transaction_bytes(transaction_id)
        .await
    {
        Ok(Some(raw_transaction_bytes)) => Ok(MempoolSourceEvent::Added(MempoolSourceEntry {
            transaction_id,
            auth_digest,
            raw_transaction_bytes,
            observed_at_unix_millis: observed_at,
        })),
        Ok(None) => {
            increment_streaming_hydration_failure(MempoolHydrationFailureReason::NotFound);
            Err(SourceError::MempoolHydrationFailed {
                transaction_id,
                reason: "transaction disappeared from upstream node before hydration completed"
                    .to_owned(),
            })
        }
        Err(hydration_error) => {
            increment_streaming_hydration_failure(MempoolHydrationFailureReason::RpcError);
            Err(hydration_error)
        }
    }
}

/// Decodes a `MempoolChangeMessage.tx_hash` into a [`TransactionId`].
///
/// Zebra's indexer fills the field with `bytes_in_display_order`, so the
/// wire bytes carry RPC byte order and must be reversed into internal
/// order; a verbatim read yields a byte-reversed txid that fails every
/// follow-up `getrawtransaction` hydration lookup.
fn decode_transaction_id(wire_bytes: &[u8]) -> Result<TransactionId, SourceError> {
    zinder_core::wire::decode_rpc_transaction_id_bytes(wire_bytes).map_err(|_| {
        SourceError::InvalidTransactionIdLength {
            byte_count: wire_bytes.len(),
        }
    })
}

/// Decodes a `MempoolChangeMessage.auth_digest` into an [`AuthDigest`].
///
/// Same `bytes_in_display_order` wire contract as the txid field. Empty
/// bytes represent an omitted digest for pre-v5 transactions. Any
/// non-empty malformed value is a protocol error rather than absent metadata.
fn decode_auth_digest(wire_bytes: &[u8]) -> Result<Option<AuthDigest>, SourceError> {
    if wire_bytes.is_empty() {
        return Ok(None);
    }
    zinder_core::wire::decode_rpc_auth_digest_bytes(wire_bytes)
        .map(Some)
        .map_err(|_| SourceError::SourceProtocolMismatch {
            reason: "Zebra indexer mempool auth_digest was not exactly 32 bytes",
        })
}

#[cfg(test)]
mod tests {
    #![allow(
        missing_docs,
        reason = "Unit test names describe the behavior under test."
    )]

    use super::*;

    #[test]
    fn certified_generation_accepts_only_its_source_tip() {
        let source_tip = BlockId::new(
            zinder_core::BlockHeight::new(7),
            zinder_core::BlockHash::from_bytes([7; 32]),
        );
        assert!(matches!(
            coherent_tip_notification(source_tip, Ok(ChainTipNotification { tip_id: source_tip })),
            Ok(TipCoherence::Stable)
        ));

        let next_tip = BlockId::new(
            zinder_core::BlockHeight::new(8),
            zinder_core::BlockHash::from_bytes([8; 32]),
        );
        assert!(matches!(
            coherent_tip_notification(source_tip, Ok(ChainTipNotification { tip_id: next_tip })),
            Ok(TipCoherence::Changed { observed_source_tip }) if observed_source_tip == next_tip
        ));
    }

    #[test]
    fn certified_generation_normalizes_chain_tip_monitor_failures() {
        let source_tip = BlockId::new(
            zinder_core::BlockHeight::new(7),
            zinder_core::BlockHash::from_bytes([7; 32]),
        );
        let outcome = coherent_tip_notification(
            source_tip,
            Err(SourceError::ChainTipStreamUnavailable {
                reason: "forced restart".to_owned(),
            }),
        );

        assert!(matches!(
            outcome,
            Err(SourceError::MempoolStreamUnavailable { .. })
        ));
    }

    #[tokio::test]
    async fn snapshot_marker_rechecks_tip_after_waiting_for_channel_capacity() -> eyre::Result<()> {
        let source_tip = BlockId::new(
            zinder_core::BlockHeight::new(7),
            zinder_core::BlockHash::from_bytes([7; 32]),
        );
        let next_tip = BlockId::new(
            zinder_core::BlockHeight::new(8),
            zinder_core::BlockHash::from_bytes([8; 32]),
        );
        let (tip_sender, tip_receiver) = mpsc::channel(1);
        let chain_tip_stream: crate::ChainTipNotificationStream =
            Box::pin(ReceiverStream::new(tip_receiver));
        let (event_sender, mut event_receiver) = mpsc::channel(1);
        event_sender
            .send(Err(SourceError::MempoolStreamUnavailable {
                reason: "test channel filler".to_owned(),
            }))
            .await?;
        let publish_task = tokio::spawn(async move {
            let mut chain_tip_stream = chain_tip_stream;
            publish_certified_snapshot_marker(source_tip, &mut chain_tip_stream, &event_sender)
                .await
        });

        tip_sender
            .send(Ok(ChainTipNotification { tip_id: next_tip }))
            .await?;
        let _channel_filler = event_receiver.recv().await;
        let outcome = publish_task.await?;

        assert!(matches!(outcome, GenerationOutcome::Ended));
        assert!(matches!(
            event_receiver.recv().await,
            Some(Ok(MempoolSourceEvent::SourceTipChanged {
                generation_source_tip,
                observed_source_tip,
            })) if generation_source_tip == source_tip && observed_source_tip == next_tip
        ));
        assert!(event_receiver.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn certification_prefix_prioritizes_a_queued_tip_change() -> eyre::Result<()> {
        let source_tip = BlockId::new(
            zinder_core::BlockHeight::new(7),
            zinder_core::BlockHash::from_bytes([7; 32]),
        );
        let next_tip = BlockId::new(
            zinder_core::BlockHeight::new(8),
            zinder_core::BlockHash::from_bytes([8; 32]),
        );
        let (tip_sender, tip_receiver) = mpsc::channel(1);
        let mut chain_tip_stream: crate::ChainTipNotificationStream =
            Box::pin(ReceiverStream::new(tip_receiver));
        let (wire_sender, mut wire_receiver) = mpsc::channel(1);
        let (event_sender, mut event_receiver) = mpsc::channel(1);
        let mut admission = MempoolSourceAdmission::new(MempoolSourceAdmissionLimits::default());
        tip_sender
            .send(Ok(ChainTipNotification { tip_id: next_tip }))
            .await?;
        wire_sender
            .send(Err(SourceError::MempoolStreamUnavailable {
                reason: "queued wire event must not outrun a queued tip change".to_owned(),
            }))
            .await?;

        let outcome = flush_certification_prefix(
            source_tip,
            &mut admission,
            &mut chain_tip_stream,
            &mut wire_receiver,
            &event_sender,
        )
        .await;

        assert!(matches!(outcome, GenerationOutcome::Ended));
        assert!(matches!(
            event_receiver.recv().await,
            Some(Ok(MempoolSourceEvent::SourceTipChanged {
                generation_source_tip,
                observed_source_tip,
            })) if generation_source_tip == source_tip && observed_source_tip == next_tip
        ));
        assert!(event_receiver.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn certified_generation_emits_tip_change_marker_and_ends() -> eyre::Result<()> {
        let source_tip = BlockId::new(
            zinder_core::BlockHeight::new(7),
            zinder_core::BlockHash::from_bytes([7; 32]),
        );
        let next_tip = BlockId::new(
            zinder_core::BlockHeight::new(8),
            zinder_core::BlockHash::from_bytes([8; 32]),
        );
        let (tip_sender, tip_receiver) = mpsc::channel(1);
        let chain_tip_stream: crate::ChainTipNotificationStream =
            Box::pin(ReceiverStream::new(tip_receiver));
        let (_wire_sender, wire_receiver) = mpsc::channel(1);
        let (event_sender, mut event_receiver) = mpsc::channel(1);
        let generation_task = tokio::spawn(async move {
            let mut admission =
                MempoolSourceAdmission::new(MempoolSourceAdmissionLimits::default());
            let mut chain_tip_stream = chain_tip_stream;
            let mut wire_receiver = wire_receiver;
            forward_certified_generation(
                source_tip,
                &mut admission,
                &mut chain_tip_stream,
                &mut wire_receiver,
                &event_sender,
            )
            .await;
        });

        tip_sender
            .send(Ok(ChainTipNotification { tip_id: next_tip }))
            .await?;

        assert!(matches!(
            event_receiver.recv().await,
            Some(Ok(MempoolSourceEvent::SourceTipChanged {
                generation_source_tip,
                observed_source_tip,
            })) if generation_source_tip == source_tip && observed_source_tip == next_tip
        ));
        generation_task.await?;
        assert!(event_receiver.recv().await.is_none());
        Ok(())
    }

    // Zebra's indexer fills `MempoolChangeMessage.tx_hash` and
    // `auth_digest` with `bytes_in_display_order` (RPC byte order); the
    // decoders must reverse into internal order or every hydration lookup
    // built from the txid targets a byte-reversed transaction.
    #[test]
    fn decode_transaction_id_reverses_display_order_wire_bytes() -> Result<(), SourceError> {
        let mut wire_bytes = [0u8; 32];
        for (index, slot) in wire_bytes.iter_mut().enumerate() {
            *slot = u8::try_from(index).unwrap_or_default();
        }
        let transaction_id = decode_transaction_id(&wire_bytes)?;
        let mut internal_bytes = wire_bytes;
        internal_bytes.reverse();
        assert_eq!(transaction_id.as_bytes(), internal_bytes);
        Ok(())
    }

    #[test]
    fn decode_transaction_id_matches_getrawmempool_display_hex_for_same_transaction()
    -> eyre::Result<()> {
        // The same txid arrives display-order-hex from `getrawmempool` and
        // display-order-bytes from the indexer stream; both must decode to
        // one internal-order id or stream hydration diverges from the
        // resnapshot path.
        let display_hex = "c3ca0ce69e0661792cbc65812eb351d0f5ba7238fdec2bb5dca3fc8ab7559436";
        let wire_bytes = hex::decode(display_hex)?;
        let from_stream = decode_transaction_id(&wire_bytes)?;
        let from_rpc = zinder_core::wire::decode_rpc_transaction_id_hex(display_hex)?;
        assert_eq!(from_stream, from_rpc);
        Ok(())
    }

    #[test]
    fn decode_transaction_id_rejects_short_payload() {
        let wire_bytes = [0x42u8; 16];
        let outcome = decode_transaction_id(&wire_bytes);
        assert!(matches!(
            outcome,
            Err(SourceError::InvalidTransactionIdLength { byte_count: 16 })
        ));
    }

    #[test]
    fn decode_auth_digest_returns_none_for_empty_bytes() {
        let auth_digest = decode_auth_digest(&[]);
        assert!(matches!(auth_digest, Ok(None)));
    }

    #[test]
    fn decode_auth_digest_reverses_display_order_wire_bytes() -> Result<(), &'static str> {
        let mut wire_bytes = [0u8; 32];
        for (index, slot) in wire_bytes.iter_mut().enumerate() {
            *slot = u8::try_from(index).unwrap_or_default();
        }
        let auth_digest = decode_auth_digest(&wire_bytes)
            .map_err(|_| "32 byte payload decodes")?
            .ok_or("32 byte payload is present")?;
        let mut internal_bytes = wire_bytes;
        internal_bytes.reverse();
        assert_eq!(auth_digest.as_bytes(), internal_bytes);
        Ok(())
    }

    #[test]
    fn decode_auth_digest_rejects_non_empty_invalid_length() {
        let wire_bytes = [0x55u8; 16];
        let auth_digest = decode_auth_digest(&wire_bytes);
        assert!(matches!(
            auth_digest,
            Err(SourceError::SourceProtocolMismatch { .. })
        ));
    }

    // Regression coverage for the resnapshot-on-connect gap: `mempool_change`
    // only reports transitions from the moment it opens, so without a
    // resnapshot pass a transaction already in the mempool at (re)connect
    // time was never observed (see the module doc's `# Reconnect contract`).
    #[tokio::test]
    async fn resnapshot_current_mempool_emits_added_for_preexisting_transaction() -> eyre::Result<()>
    {
        // All-same-byte txid sidesteps needing to know `getrawmempool`'s
        // display-order byte reversal: reversing `[0x11; 32]` is a no-op.
        let transaction_id = TransactionId::from_bytes([0x11; 32]);
        let txid_hex = "11".repeat(32);
        let source_tip_response = serde_json::json!({
            "height": 7,
            "hash": vec![0x07; 32],
        });
        let server = zinder_testkit::JsonRpcTestServer::start([
            zinder_testkit::method("getbestblockheightandhash").reply(
                zinder_testkit::RpcReply::result(source_tip_response.clone()),
            ),
            zinder_testkit::method("getrawmempool").reply(zinder_testkit::RpcReply::result(
                serde_json::json!([txid_hex]),
            )),
            zinder_testkit::method("getrawtransaction").reply(zinder_testkit::RpcReply::result(
                serde_json::json!("deadbeef"),
            )),
            zinder_testkit::method("getbestblockheightandhash")
                .reply(zinder_testkit::RpcReply::result(source_tip_response)),
        ])?;
        let hydration_json_rpc = ZebraJsonRpcSource::new(
            zinder_core::Network::ZcashRegtest,
            server.url(),
            crate::NodeAuth::None,
            Duration::from_secs(5),
        )?;

        let (event_sender, mut event_receiver) = mpsc::channel(8);
        let snapshot = resnapshot_current_mempool(
            hydration_json_rpc,
            MempoolSourceAdmissionLimits::default(),
            &event_sender,
        )
        .await?;
        drop(event_sender);

        let ResnapshotOutcome::Complete {
            source_tip,
            admission: _admission,
        } = snapshot
        else {
            return Err(eyre::eyre!(
                "stable source tip must complete the resnapshot"
            ));
        };

        assert_eq!(
            source_tip,
            zinder_core::BlockId::new(
                zinder_core::BlockHeight::new(7),
                zinder_core::BlockHash::from_bytes([0x07; 32]),
            )
        );

        let event = event_receiver
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("expected a resnapshot event"))??;
        let MempoolSourceEvent::Added(entry) = event else {
            return Err(eyre::eyre!("expected an Added event, got {event:?}"));
        };
        assert_eq!(entry.transaction_id, transaction_id);
        assert_eq!(
            entry.raw_transaction_bytes.as_slice(),
            [0xde, 0xad, 0xbe, 0xef]
        );
        assert!(
            event_receiver.recv().await.is_none(),
            "resnapshot must not emit more than one event per mempool entry"
        );
        Ok(())
    }

    #[tokio::test]
    async fn resnapshot_rejects_transaction_count_before_hydration() -> eyre::Result<()> {
        let server = zinder_testkit::JsonRpcTestServer::start([
            zinder_testkit::method("getbestblockheightandhash").reply(
                zinder_testkit::RpcReply::result(serde_json::json!({
                    "height": 7,
                    "hash": vec![0x07; 32],
                })),
            ),
            zinder_testkit::method("getrawmempool").reply(zinder_testkit::RpcReply::result(
                serde_json::json!(["11".repeat(32), "22".repeat(32)]),
            )),
        ])?;
        let hydration_json_rpc = ZebraJsonRpcSource::new(
            zinder_core::Network::ZcashRegtest,
            server.url(),
            crate::NodeAuth::None,
            Duration::from_secs(5),
        )?;
        let limits = MempoolSourceAdmissionLimits {
            max_transaction_count: std::num::NonZeroU32::MIN,
            max_total_raw_transaction_bytes: std::num::NonZeroU64::MIN.saturating_add(99),
        };
        let (event_sender, _event_receiver) = mpsc::channel(4);

        let outcome = resnapshot_current_mempool(hydration_json_rpc, limits, &event_sender).await;

        assert!(matches!(
            outcome,
            Err(SourceError::MempoolTransactionCountLimitExceeded {
                transaction_count: 2,
                max_transaction_count: 1,
            })
        ));
        assert!(server.requests_for("getrawtransaction")?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn resnapshot_rejects_cumulative_raw_transaction_bytes() -> eyre::Result<()> {
        let source_tip_response = serde_json::json!({
            "height": 7,
            "hash": vec![0x07; 32],
        });
        let server = zinder_testkit::JsonRpcTestServer::start([
            zinder_testkit::method("getbestblockheightandhash")
                .reply(zinder_testkit::RpcReply::result(source_tip_response)),
            zinder_testkit::method("getrawmempool").reply(zinder_testkit::RpcReply::result(
                serde_json::json!(["11".repeat(32), "22".repeat(32)]),
            )),
            zinder_testkit::method("getrawtransaction")
                .reply(zinder_testkit::RpcReply::result(serde_json::json!("dead"))),
            zinder_testkit::method("getrawtransaction")
                .reply(zinder_testkit::RpcReply::result(serde_json::json!("beef"))),
        ])?;
        let hydration_json_rpc = ZebraJsonRpcSource::new(
            zinder_core::Network::ZcashRegtest,
            server.url(),
            crate::NodeAuth::None,
            Duration::from_secs(5),
        )?;
        let limits = MempoolSourceAdmissionLimits {
            max_transaction_count: std::num::NonZeroU32::MIN.saturating_add(1),
            max_total_raw_transaction_bytes: std::num::NonZeroU64::MIN.saturating_add(2),
        };
        let (event_sender, mut event_receiver) = mpsc::channel(4);

        let outcome = resnapshot_current_mempool(hydration_json_rpc, limits, &event_sender).await;

        assert!(matches!(
            outcome,
            Err(SourceError::MempoolRawTransactionBytesLimitExceeded {
                total_raw_transaction_bytes: 4,
                max_total_raw_transaction_bytes: 3,
            })
        ));
        assert!(matches!(
            event_receiver.try_recv(),
            Ok(Ok(MempoolSourceEvent::Added(_)))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn resnapshot_ends_provisional_generation_when_the_source_tip_changes() -> eyre::Result<()>
    {
        let txid_hex = "11".repeat(32);
        let server = zinder_testkit::JsonRpcTestServer::start([
            zinder_testkit::method("getbestblockheightandhash").reply(
                zinder_testkit::RpcReply::result(serde_json::json!({
                    "height": 7,
                    "hash": vec![0x07; 32],
                })),
            ),
            zinder_testkit::method("getrawmempool").reply(zinder_testkit::RpcReply::result(
                serde_json::json!([txid_hex]),
            )),
            zinder_testkit::method("getrawtransaction").reply(zinder_testkit::RpcReply::result(
                serde_json::json!("deadbeef"),
            )),
            zinder_testkit::method("getbestblockheightandhash").reply(
                zinder_testkit::RpcReply::result(serde_json::json!({
                    "height": 8,
                    "hash": vec![0x08; 32],
                })),
            ),
        ])?;
        let hydration_json_rpc = ZebraJsonRpcSource::new(
            zinder_core::Network::ZcashRegtest,
            server.url(),
            crate::NodeAuth::None,
            Duration::from_secs(5),
        )?;
        let (event_sender, mut event_receiver) = mpsc::channel(8);

        let outcome = resnapshot_current_mempool(
            hydration_json_rpc,
            MempoolSourceAdmissionLimits::default(),
            &event_sender,
        )
        .await;

        assert!(matches!(
            outcome,
            Ok(ResnapshotOutcome::SourceTipChanged {
                generation_source_tip,
                observed_source_tip,
            }) if generation_source_tip == BlockId::new(
                zinder_core::BlockHeight::new(7),
                zinder_core::BlockHash::from_bytes([0x07; 32]),
            ) && observed_source_tip == BlockId::new(
                zinder_core::BlockHeight::new(8),
                zinder_core::BlockHash::from_bytes([0x08; 32]),
            )
        ));
        assert!(matches!(
            event_receiver.try_recv(),
            Ok(Ok(MempoolSourceEvent::Added(_)))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn streaming_backend_emits_tip_change_after_provisional_hydration() -> eyre::Result<()> {
        let txid_hex = "11".repeat(32);
        let generation_source_tip = BlockId::new(
            zinder_core::BlockHeight::new(7),
            zinder_core::BlockHash::from_bytes([0x07; 32]),
        );
        let observed_source_tip = BlockId::new(
            zinder_core::BlockHeight::new(8),
            zinder_core::BlockHash::from_bytes([0x08; 32]),
        );
        let server = zinder_testkit::JsonRpcTestServer::start([
            zinder_testkit::method("getbestblockheightandhash").reply(
                zinder_testkit::RpcReply::result(serde_json::json!({
                    "height": 7,
                    "hash": vec![0x07; 32],
                })),
            ),
            zinder_testkit::method("getrawmempool").reply(zinder_testkit::RpcReply::result(
                serde_json::json!([txid_hex]),
            )),
            zinder_testkit::method("getrawtransaction").reply(zinder_testkit::RpcReply::result(
                serde_json::json!("deadbeef"),
            )),
            zinder_testkit::method("getbestblockheightandhash").reply(
                zinder_testkit::RpcReply::result(serde_json::json!({
                    "height": 8,
                    "hash": vec![0x08; 32],
                })),
            ),
        ])?;
        let hydration_json_rpc = ZebraJsonRpcSource::new(
            zinder_core::Network::ZcashRegtest,
            server.url(),
            crate::NodeAuth::None,
            Duration::from_secs(5),
        )?;
        let (_tip_sender, tip_receiver) = mpsc::channel(1);
        let chain_tip_stream: crate::ChainTipNotificationStream =
            Box::pin(ReceiverStream::new(tip_receiver));
        let (_wire_sender, wire_receiver) = mpsc::channel(1);
        let (event_sender, mut event_receiver) = mpsc::channel(4);

        spawn_certified_event_aggregator(
            hydration_json_rpc,
            MempoolSourceAdmissionLimits::default(),
            chain_tip_stream,
            wire_receiver,
            event_sender,
        );

        assert!(matches!(
            event_receiver.recv().await,
            Some(Ok(MempoolSourceEvent::Added(_)))
        ));
        assert!(matches!(
            event_receiver.recv().await,
            Some(Ok(MempoolSourceEvent::SourceTipChanged {
                generation_source_tip: event_generation_tip,
                observed_source_tip: event_observed_tip,
            })) if event_generation_tip == generation_source_tip
                && event_observed_tip == observed_source_tip
        ));
        assert!(event_receiver.recv().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn provisional_missing_hydration_is_tip_change_when_the_tip_moved() -> eyre::Result<()> {
        let server = zinder_testkit::JsonRpcTestServer::start([
            zinder_testkit::method("getbestblockheightandhash").reply(
                zinder_testkit::RpcReply::result(serde_json::json!({
                    "height": 7,
                    "hash": vec![0x07; 32],
                })),
            ),
            zinder_testkit::method("getrawmempool").reply(zinder_testkit::RpcReply::result(
                serde_json::json!(["11".repeat(32)]),
            )),
            zinder_testkit::method("getrawtransaction").reply(
                zinder_testkit::RpcReply::error_with_code(
                    -5,
                    "No such mempool or blockchain transaction",
                ),
            ),
            zinder_testkit::method("getbestblockheightandhash").reply(
                zinder_testkit::RpcReply::result(serde_json::json!({
                    "height": 8,
                    "hash": vec![0x08; 32],
                })),
            ),
        ])?;
        let hydration_json_rpc = ZebraJsonRpcSource::new(
            zinder_core::Network::ZcashRegtest,
            server.url(),
            crate::NodeAuth::None,
            Duration::from_secs(5),
        )?;
        let (event_sender, mut event_receiver) = mpsc::channel(1);

        let outcome = resnapshot_current_mempool(
            hydration_json_rpc,
            MempoolSourceAdmissionLimits::default(),
            &event_sender,
        )
        .await;

        assert!(matches!(
            outcome,
            Ok(ResnapshotOutcome::SourceTipChanged {
                generation_source_tip,
                observed_source_tip,
            }) if generation_source_tip == BlockId::new(
                zinder_core::BlockHeight::new(7),
                zinder_core::BlockHash::from_bytes([0x07; 32]),
            ) && observed_source_tip == BlockId::new(
                zinder_core::BlockHeight::new(8),
                zinder_core::BlockHash::from_bytes([0x08; 32]),
            )
        ));
        assert!(event_receiver.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn provisional_missing_hydration_remains_an_error_at_the_same_tip() -> eyre::Result<()> {
        let source_tip_response = serde_json::json!({
            "height": 7,
            "hash": vec![0x07; 32],
        });
        let server = zinder_testkit::JsonRpcTestServer::start([
            zinder_testkit::method("getbestblockheightandhash").reply(
                zinder_testkit::RpcReply::result(source_tip_response.clone()),
            ),
            zinder_testkit::method("getrawmempool").reply(zinder_testkit::RpcReply::result(
                serde_json::json!(["11".repeat(32)]),
            )),
            zinder_testkit::method("getrawtransaction").reply(
                zinder_testkit::RpcReply::error_with_code(
                    -5,
                    "No such mempool or blockchain transaction",
                ),
            ),
            zinder_testkit::method("getbestblockheightandhash")
                .reply(zinder_testkit::RpcReply::result(source_tip_response)),
        ])?;
        let hydration_json_rpc = ZebraJsonRpcSource::new(
            zinder_core::Network::ZcashRegtest,
            server.url(),
            crate::NodeAuth::None,
            Duration::from_secs(5),
        )?;
        let (event_sender, mut event_receiver) = mpsc::channel(1);

        let outcome = resnapshot_current_mempool(
            hydration_json_rpc,
            MempoolSourceAdmissionLimits::default(),
            &event_sender,
        )
        .await;

        assert!(matches!(
            outcome,
            Err(SourceError::MempoolHydrationFailed { .. })
        ));
        assert!(event_receiver.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn resnapshot_hydration_obeys_event_channel_backpressure() -> eyre::Result<()> {
        const TRANSACTION_COUNT: usize = MEMPOOL_RESNAPSHOT_HYDRATION_CONCURRENCY * 4;
        let transaction_ids = (0..TRANSACTION_COUNT)
            .map(|index| format!("{:02x}", index.saturating_add(1)).repeat(32))
            .collect::<Vec<_>>();
        let source_tip_response = serde_json::json!({
            "height": 7,
            "hash": vec![0x07; 32],
        });
        let mut stubs = vec![
            zinder_testkit::method("getbestblockheightandhash").reply(
                zinder_testkit::RpcReply::result(source_tip_response.clone()),
            ),
            zinder_testkit::method("getrawmempool").reply(zinder_testkit::RpcReply::result(
                serde_json::json!(transaction_ids),
            )),
        ];
        stubs.extend((0..TRANSACTION_COUNT).map(|_| {
            zinder_testkit::method("getrawtransaction").reply(zinder_testkit::RpcReply::result(
                serde_json::json!("deadbeef"),
            ))
        }));
        stubs.push(
            zinder_testkit::method("getbestblockheightandhash")
                .reply(zinder_testkit::RpcReply::result(source_tip_response)),
        );
        let server = zinder_testkit::JsonRpcTestServer::start(stubs)?;
        let hydration_json_rpc = ZebraJsonRpcSource::new(
            zinder_core::Network::ZcashRegtest,
            server.url(),
            crate::NodeAuth::None,
            Duration::from_secs(5),
        )?;
        let (event_sender, _event_receiver) = mpsc::channel(1);
        let resnapshot_task = tokio::spawn(async move {
            resnapshot_current_mempool(
                hydration_json_rpc,
                MempoolSourceAdmissionLimits::default(),
                &event_sender,
            )
            .await
        });

        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if server
                    .requests_for("getrawtransaction")
                    .unwrap_or_default()
                    .len()
                    >= MEMPOOL_RESNAPSHOT_HYDRATION_CONCURRENCY
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        tokio::time::sleep(Duration::from_millis(25)).await;
        let hydrated_request_count = server.requests_for("getrawtransaction")?.len();
        resnapshot_task.abort();
        let _ = resnapshot_task.await;

        assert!(
            hydrated_request_count < TRANSACTION_COUNT,
            "a blocked consumer must stop resnapshot hydration before all raw transactions are retained"
        );
        Ok(())
    }
}
