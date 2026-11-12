//! Production wirings of [`MempoolSurface`] and [`TipChangeWatcher`] over the
//! private `IngestControl` gRPC.
//!
//! The compatibility binary is colocated with `zinder-projector` and the writer's
//! `IngestControl` endpoint. The wirings here connect on demand so a transient
//! writer outage does not require restarting the compat process.
//!
//! ## Connection lifecycle and fan-out
//!
//! Each [`IngestControlMempoolSurface`] instance dials a single HTTP/2 channel
//! to the writer the first time a method is invoked, caches it in an
//! [`Arc<OnceCell<_>>`], and reuses it for every subsequent
//! [`IngestControlMempoolSurface::mempool_snapshot_page`] /
//! [`IngestControlMempoolSurface::mempool_events`] call. Tonic's `Channel`
//! handles transparent HTTP/2 reconnect, so a transient writer outage does
//! not require restarting the compat process.
//!
//! With N concurrent lightwalletd `GetMempoolStream` clients there are N
//! concurrent `IngestControl` `MempoolEvents` subscriptions multiplexed over
//! the cached channel. Operators capping the public lightwalletd surface
//! (Caddy, nginx) bound N at the proxy edge; Zinder does not enforce a
//! process-wide cap.
//! [`spawn_ingest_control_tip_change_publisher`] runs a separate session-per-
//! reconnect loop for `VisibleChainEvents`, replaying the retained window so a
//! writer or network interruption cannot hide a tip change from an already-
//! fenced mempool stream. A 500 ms backoff between attempts prevents a writer
//! restart from driving a tight reconnect loop.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{OnceCell, mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request};
use zinder_core::{
    BlockHeight, BlockId, ChainEpochId, MempoolEntry, TransactionId,
    wire::encode_rpc_transaction_id_hex,
};
use zinder_proto::v1::{
    ingest::{MempoolTransactionRequest, ingest_control_client::IngestControlClient},
    wallet,
};
use zinder_runtime::{AuthenticatedChannel, BearerToken, connect_zinder_grpc};
use zinder_store::{
    EventStreamStartPosition, StreamCursorTokenV1, event_stream_start_message,
    mempool_entry_from_message, mempool_event_envelope_from_message,
    stream_cursor_from_message_bytes,
};

use super::surface::{
    MempoolEventEnvelopeStream, MempoolSnapshotPage, MempoolSurface, MempoolSurfaceError,
    TipChangeWatcher, TipChangeWatcherError,
};

type AuthenticatedIngestControlClient = IngestControlClient<AuthenticatedChannel>;

/// Reconnect delay after a visible-chain-events session ends without an explicit shutdown.
///
/// Short enough to recover quickly from a writer restart, long enough that a
/// tight reconnect loop does not hammer the writer when it actively rejects
/// connections.
const TIP_PUBLISHER_RECONNECT_BACKOFF: Duration = Duration::from_millis(500);

/// Mempool read surface backed by an `IngestControl` gRPC endpoint.
#[derive(Clone, Debug)]
pub struct IngestControlMempoolSurface {
    endpoint: String,
    bearer_token: Option<BearerToken>,
    channel: Arc<OnceCell<AuthenticatedChannel>>,
}

impl IngestControlMempoolSurface {
    /// Creates a mempool surface that dials the writer's `IngestControl`
    /// endpoint on first use and caches the channel for the surface's
    /// lifetime.
    #[must_use]
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            bearer_token: None,
            channel: Arc::new(OnceCell::new()),
        }
    }

    /// Wires a shared-secret bearer token onto every outbound request. The
    /// token is attached as an `authorization: Bearer <token>` metadata
    /// header by the underlying gRPC interceptor.
    #[must_use]
    pub fn with_bearer_token(mut self, bearer_token: BearerToken) -> Self {
        self.bearer_token = Some(bearer_token);
        self
    }

    /// Returns an `IngestControl` client backed by the surface's cached
    /// HTTP/2 channel.
    ///
    /// The first call dials the writer; subsequent calls reuse the cached
    /// [`AuthenticatedChannel`] (cheap clone, transparent HTTP/2 reconnect).
    async fn ingest_control_client(
        &self,
    ) -> Result<AuthenticatedIngestControlClient, MempoolSurfaceError> {
        let endpoint = self.endpoint.clone();
        let bearer_token = self.bearer_token.clone();
        let channel = self
            .channel
            .get_or_try_init(|| async move {
                connect_zinder_grpc(&endpoint, bearer_token.as_ref())
                    .await
                    .map_err(|error| MempoolSurfaceError::Unavailable {
                        reason: error.to_string(),
                    })
            })
            .await?;
        Ok(IngestControlClient::new(channel.clone()))
    }
}

#[async_trait]
impl MempoolSurface for IngestControlMempoolSurface {
    async fn mempool_snapshot_page(
        &self,
        max_entries: u32,
        from_cursor: Option<Vec<u8>>,
    ) -> Result<MempoolSnapshotPage, MempoolSurfaceError> {
        let mut client = self.ingest_control_client().await?;
        let response = client
            .mempool_snapshot(wallet::MempoolSnapshotRequest {
                max_entries,
                from_cursor: from_cursor.unwrap_or_default(),
            })
            .await
            .map_err(|status| MempoolSurfaceError::Unavailable {
                reason: format!("ingest-control mempool_snapshot failed: {status}"),
            })?
            .into_inner();
        let entries = response
            .entries
            .into_iter()
            .map(|entry| {
                mempool_entry_from_message(entry).map_err(|error| {
                    MempoolSurfaceError::Unavailable {
                        reason: error.to_string(),
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = if response.next_cursor.is_empty() {
            None
        } else {
            Some(response.next_cursor)
        };
        let chain_epoch = response
            .chain_view
            .and_then(|chain_view| chain_view.chain_epoch)
            .ok_or_else(|| MempoolSurfaceError::Unavailable {
                reason: "ingest-control mempool_snapshot omitted chain_view.chain_epoch".to_owned(),
            })?;
        let visible_tip =
            chain_epoch
                .visible_tip
                .ok_or_else(|| MempoolSurfaceError::Unavailable {
                    reason: "ingest-control mempool_snapshot omitted chain_view visible tip"
                        .to_owned(),
                })?;
        let source_tip = response
            .source_tip
            .ok_or_else(|| MempoolSurfaceError::Unavailable {
                reason: "ingest-control mempool_snapshot omitted source_tip".to_owned(),
            })?;
        let visible_tip_hash = zinder_core::wire::decode_rpc_block_hash_hex(&visible_tip.hash)
            .map_err(|error| MempoolSurfaceError::Unavailable {
                reason: format!("ingest-control mempool_snapshot visible tip is invalid: {error}"),
            })?;
        let source_tip_hash = zinder_core::wire::decode_rpc_block_hash_hex(&source_tip.hash)
            .map_err(|error| MempoolSurfaceError::Unavailable {
                reason: format!("ingest-control mempool_snapshot source tip is invalid: {error}"),
            })?;
        if BlockId::new(BlockHeight::new(visible_tip.height), visible_tip_hash)
            != BlockId::new(BlockHeight::new(source_tip.height), source_tip_hash)
        {
            return Err(MempoolSurfaceError::Unavailable {
                reason: "ingest-control mempool_snapshot source tip differs from its chain view"
                    .to_owned(),
            });
        }
        let chain_epoch_id = ChainEpochId::new(chain_epoch.chain_epoch_id);
        let events_resume_cursor = stream_cursor_from_message_bytes(response.events_resume_cursor);
        Ok(MempoolSnapshotPage {
            chain_epoch_id,
            events_resume_cursor,
            entries,
            next_cursor,
        })
    }

    async fn mempool_events(
        &self,
        from_cursor: Option<StreamCursorTokenV1>,
    ) -> Result<MempoolEventEnvelopeStream, MempoolSurfaceError> {
        let mut client = self.ingest_control_client().await?;
        let start = from_cursor.map_or(
            EventStreamStartPosition::EarliestRetained,
            EventStreamStartPosition::AfterCursor,
        );
        let response = client
            .mempool_events(wallet::MempoolEventsRequest {
                start: Some(event_stream_start_message(&start)),
            })
            .await
            .map_err(|status| MempoolSurfaceError::Unavailable {
                reason: format!("ingest-control mempool_events failed: {status}"),
            })?;

        let (output_sender, output_receiver) = mpsc::channel(16);
        let mut response_stream = response.into_inner();
        tokio::spawn(async move {
            loop {
                match tokio_stream::StreamExt::next(&mut response_stream).await {
                    Some(Ok(message)) => {
                        match mempool_event_envelope_from_message(message).map_err(|error| {
                            MempoolSurfaceError::Unavailable {
                                reason: error.to_string(),
                            }
                        }) {
                            Ok(envelope) => {
                                if output_sender.send(Ok(envelope)).await.is_err() {
                                    return;
                                }
                            }
                            Err(error) => {
                                let _ = output_sender.send(Err(error)).await;
                                return;
                            }
                        }
                    }
                    Some(Err(status)) => {
                        let _ = output_sender
                            .send(Err(MempoolSurfaceError::Unavailable {
                                reason: format!("ingest-control mempool_events errored: {status}"),
                            }))
                            .await;
                        return;
                    }
                    None => return,
                }
            }
        });
        Ok(Box::pin(ReceiverStream::new(output_receiver)))
    }

    async fn mempool_transaction(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<MempoolEntry>, MempoolSurfaceError> {
        let mut client = self.ingest_control_client().await?;
        let response = match client
            .mempool_transaction(MempoolTransactionRequest {
                transaction_id: encode_rpc_transaction_id_hex(transaction_id),
            })
            .await
        {
            Ok(response) => response.into_inner(),
            Err(status) if status.code() == Code::NotFound => return Ok(None),
            Err(status) => {
                return Err(MempoolSurfaceError::Unavailable {
                    reason: format!("ingest-control mempool_transaction failed: {status}"),
                });
            }
        };
        let Some(wallet::transaction_location::Location::InMempool(entry)) =
            response.location.and_then(|location| location.location)
        else {
            return Err(MempoolSurfaceError::Unavailable {
                reason: "ingest-control mempool_transaction did not return a mempool entry"
                    .to_owned(),
            });
        };
        let entry = mempool_entry_from_message(entry).map_err(|error| {
            MempoolSurfaceError::Unavailable {
                reason: error.to_string(),
            }
        })?;
        if entry.transaction_id() != transaction_id {
            return Err(MempoolSurfaceError::Unavailable {
                reason: "ingest-control mempool_transaction returned another transaction"
                    .to_owned(),
            });
        }
        Ok(Some(entry))
    }
}

/// Resolves [`TipChangeWatcher::await_tip_change_after`] from a
/// [`tokio::sync::watch::Receiver`] published by a chain-events consumer.
#[derive(Clone, Debug)]
pub struct WatchTipChangeWatcher {
    receiver: watch::Receiver<u64>,
}

impl WatchTipChangeWatcher {
    /// Creates a watcher over the given watch receiver.
    #[must_use]
    pub const fn new(receiver: watch::Receiver<u64>) -> Self {
        Self { receiver }
    }
}

#[async_trait]
impl TipChangeWatcher for WatchTipChangeWatcher {
    async fn await_tip_change_after(
        &self,
        chain_epoch_id: ChainEpochId,
    ) -> Result<(), TipChangeWatcherError> {
        let mut receiver = self.receiver.clone();
        loop {
            if *receiver.borrow_and_update() > chain_epoch_id.value() {
                return Ok(());
            }
            receiver
                .changed()
                .await
                .map_err(|_| TipChangeWatcherError::SignalClosed)?;
        }
    }
}

/// Spawns a task that consumes retained and live
/// `IngestControl.VisibleChainEvents` and publishes committed event sequences
/// to a `watch::Sender<u64>`.
///
/// Returns a watcher view over the same channel. Drop the
/// [`tokio::task::JoinHandle`] to detach, or await it for symmetric
/// shutdown.
#[must_use = "drop the handle to detach the publisher or await it for symmetric shutdown"]
pub fn spawn_ingest_control_tip_change_publisher(
    endpoint: String,
    bearer_token: Option<BearerToken>,
    cancel: CancellationToken,
) -> (Arc<dyn TipChangeWatcher>, tokio::task::JoinHandle<()>) {
    let (tip_sender, tip_receiver) = watch::channel(0_u64);
    let watcher = Arc::new(WatchTipChangeWatcher::new(tip_receiver));
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                () = run_ingest_control_tip_change_session(endpoint.clone(), bearer_token.as_ref(), &tip_sender) => {
                    tokio::time::sleep(TIP_PUBLISHER_RECONNECT_BACKOFF).await;
                }
            }
        }
    });
    (watcher, handle)
}

async fn run_ingest_control_tip_change_session(
    endpoint: String,
    bearer_token: Option<&BearerToken>,
    tip_sender: &watch::Sender<u64>,
) {
    let mut client = match connect_authenticated_ingest_control(&endpoint, bearer_token).await {
        Ok(client) => client,
        Err(error) => {
            tracing::debug!(
                target: "zinder::compat_lightwalletd",
                event = "tip_change_publisher_connect_failed",
                error = %error,
                "tip-change publisher could not connect to ingest-control; will retry"
            );
            return;
        }
    };
    let response_outcome = client
        .visible_chain_events(Request::new(event_stream_start_message(
            &EventStreamStartPosition::EarliestRetained,
        )))
        .await;
    let response = match response_outcome {
        Ok(response) => response,
        Err(error) => {
            tracing::debug!(
                target: "zinder::compat_lightwalletd",
                event = "tip_change_publisher_subscribe_failed",
                error = %error,
                "tip-change publisher visible-chain-events subscription failed; will retry"
            );
            return;
        }
    };
    let mut response_stream = response.into_inner();
    while let Some(message_outcome) = tokio_stream::StreamExt::next(&mut response_stream).await {
        match message_outcome {
            Ok(envelope) => {
                tip_sender.send_if_modified(|latest_event_sequence| {
                    if envelope.event_sequence > *latest_event_sequence {
                        *latest_event_sequence = envelope.event_sequence;
                        true
                    } else {
                        false
                    }
                });
            }
            Err(error) => {
                tracing::debug!(
                    target: "zinder::compat_lightwalletd",
                    event = "tip_change_publisher_stream_error",
                    error = %error,
                    "tip-change publisher visible-chain-events stream errored; will reconnect"
                );
                return;
            }
        }
    }
}

async fn connect_authenticated_ingest_control(
    endpoint: &str,
    bearer_token: Option<&BearerToken>,
) -> Result<AuthenticatedIngestControlClient, MempoolSurfaceError> {
    let channel = connect_zinder_grpc(endpoint, bearer_token)
        .await
        .map_err(|error| MempoolSurfaceError::Unavailable {
            reason: error.to_string(),
        })?;
    Ok(IngestControlClient::new(channel))
}
