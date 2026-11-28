//! In-process mempool surface and tip-change signal for compatibility readers.

use std::{collections::BTreeMap, pin::Pin, sync::Arc};

use async_trait::async_trait;
use parking_lot::RwLock;
use tokio::sync::{broadcast, watch};
use tokio_stream::{
    StreamExt as _,
    wrappers::{BroadcastStream, errors::BroadcastStreamRecvError},
};
use zinder_compat_lightwalletd::{
    MempoolEventEnvelopeStream, MempoolSnapshotPage, MempoolSurface, MempoolSurfaceError,
    MempoolTransactionLookup, SharedMempoolSurface, SharedTipChangeWatcher, WatchTipChangeWatcher,
};
use zinder_core::{
    ChainEpoch, ChainEpochId, CompactShieldedAction, CompactTransactionData, MempoolEntry,
    MempoolEvictionReason, MempoolObservation, RawTransactionBytes, TransactionId,
    UnixTimestampMillis,
};
use zinder_store::{MempoolEvent, MempoolEventEnvelope, StreamCursorTokenV1};

use crate::BenchError;

const EVENT_CHANNEL_CAPACITY: usize = 2_048;

struct MempoolState {
    chain_epoch: ChainEpoch,
    next_event_sequence: u64,
    entries: BTreeMap<[u8; 32], MempoolEntry>,
    events: Vec<MempoolEventEnvelope>,
}

/// Deterministic in-memory mempool surface driving `GetMempoolTx` and
/// `GetMempoolStream` during concurrent tip commits.
pub(crate) struct BenchmarkMempoolSurface {
    state: RwLock<MempoolState>,
    events_tx: broadcast::Sender<MempoolEventEnvelope>,
}

impl BenchmarkMempoolSurface {
    /// Creates an empty mempool surface anchored at `initial_epoch`.
    pub(crate) fn new(initial_epoch: ChainEpoch) -> Arc<Self> {
        let (events_tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Arc::new(Self {
            state: RwLock::new(MempoolState {
                chain_epoch: initial_epoch,
                next_event_sequence: 1,
                entries: BTreeMap::new(),
                events: Vec::new(),
            }),
            events_tx,
        })
    }

    /// Replaces active mempool contents with `batch_size` synthetic V5
    /// transactions and broadcasts `Added` / `Invalidated` transitions.
    pub(crate) fn apply_churn_batch(
        &self,
        batch_size: u32,
        round: u32,
        sample_raw_tx: &[u8],
    ) -> Result<usize, BenchError> {
        // Construct entries outside the write lock so concurrent snapshot and
        // transaction readers are never blocked on serialization work.
        let epoch = self.state.read().chain_epoch;
        let prepared_entries = prepare_batch_entries(epoch, batch_size, round, sample_raw_tx)?;

        let mut guard = self.state.write();
        let existing_keys: Vec<[u8; 32]> = guard.entries.keys().copied().collect();
        for key in existing_keys {
            if let Some(removed) = guard.entries.remove(&key) {
                let envelope = next_envelope(
                    &mut guard.next_event_sequence,
                    MempoolEvent::Invalidated {
                        transaction_id: removed.transaction_id(),
                        reason: MempoolEvictionReason::Expired,
                    },
                );
                guard.events.push(envelope.clone());
                let _ = self.events_tx.send(envelope);
            }
        }
        for (txid_bytes, entry) in prepared_entries {
            let envelope = next_envelope(
                &mut guard.next_event_sequence,
                MempoolEvent::Added {
                    entry: entry.clone(),
                },
            );
            guard.entries.insert(txid_bytes, entry);
            guard.events.push(envelope.clone());
            let _ = self.events_tx.send(envelope);
        }
        if guard.events.len() > EVENT_CHANNEL_CAPACITY {
            let trim = guard.events.len() - EVENT_CHANNEL_CAPACITY;
            guard.events.drain(..trim);
        }
        Ok(guard.entries.len())
    }

    /// Advances the coherent chain epoch observed by mempool lookups.
    pub(crate) fn advance_epoch(&self, chain_epoch: ChainEpoch) {
        self.state.write().chain_epoch = chain_epoch;
    }
}

/// Converts a shared [`BenchmarkMempoolSurface`] to the trait object expected by
/// [`zinder_compat_lightwalletd::LightwalletdGrpcAdapter`].
pub(crate) fn as_mempool_surface(surface: Arc<BenchmarkMempoolSurface>) -> SharedMempoolSurface {
    surface
}

/// Builds a [`WatchTipChangeWatcher`] and its driving sender.
pub(crate) fn create_tip_watcher(
    initial_epoch_id: ChainEpochId,
) -> (watch::Sender<u64>, SharedTipChangeWatcher) {
    let (sender, receiver) = watch::channel(initial_epoch_id.value());
    (sender, Arc::new(WatchTipChangeWatcher::new(receiver)))
}

#[async_trait]
impl MempoolSurface for BenchmarkMempoolSurface {
    async fn mempool_snapshot_page(
        &self,
        max_entries: u32,
        from_cursor: Option<Vec<u8>>,
    ) -> Result<MempoolSnapshotPage, MempoolSurfaceError> {
        let guard = self.state.read();
        let limit = usize::try_from(max_entries).unwrap_or(usize::MAX).max(1);
        let after_key: Option<[u8; 32]> = from_cursor
            .as_deref()
            .and_then(|cursor| <[u8; 32]>::try_from(cursor).ok());
        let mut selected = Vec::with_capacity(limit.min(guard.entries.len()));
        let mut next_cursor = None;
        for (key, entry) in &guard.entries {
            if let Some(boundary) = after_key
                && *key <= boundary
            {
                continue;
            }
            if selected.len() < limit {
                selected.push(entry.clone());
            } else {
                next_cursor = selected.last().map(|last| {
                    zinder_core::wire::encode_internal_transaction_id(last.transaction_id())
                        .to_vec()
                });
                break;
            }
        }
        let events_resume_cursor = guard.events.last().map(|event| event.cursor.clone());
        Ok(MempoolSnapshotPage {
            chain_epoch_id: guard.chain_epoch.id,
            events_resume_cursor,
            entries: selected,
            next_cursor,
        })
    }

    async fn mempool_events(
        &self,
        from_cursor: Option<StreamCursorTokenV1>,
    ) -> Result<MempoolEventEnvelopeStream, MempoolSurfaceError> {
        let after_sequence = from_cursor
            .as_ref()
            .and_then(|token| <[u8; 8]>::try_from(token.as_bytes()).ok())
            .map_or(0, u64::from_be_bytes);
        let (replay, receiver) = {
            let guard = self.state.read();
            let replay: Vec<Result<MempoolEventEnvelope, MempoolSurfaceError>> = guard
                .events
                .iter()
                .filter(|event| event.event_sequence > after_sequence)
                .cloned()
                .map(Ok)
                .collect();
            let receiver = self.events_tx.subscribe();
            drop(guard);
            (replay, receiver)
        };
        let live = BroadcastStream::new(receiver).map(|outcome| match outcome {
            Ok(envelope) => Ok(envelope),
            Err(BroadcastStreamRecvError::Lagged(_)) => Err(MempoolSurfaceError::CursorExpired),
        });
        let combined: Pin<
            Box<
                dyn tokio_stream::Stream<Item = Result<MempoolEventEnvelope, MempoolSurfaceError>>
                    + Send,
            >,
        > = Box::pin(tokio_stream::iter(replay).chain(live));
        Ok(combined)
    }

    async fn mempool_transaction(
        &self,
        transaction_id: TransactionId,
    ) -> Result<MempoolTransactionLookup, MempoolSurfaceError> {
        let guard = self.state.read();
        if let Some(entry) = guard.entries.get(&transaction_id.as_bytes()) {
            return Ok(MempoolTransactionLookup::Pending(Box::new(entry.clone())));
        }
        Ok(MempoolTransactionLookup::Absent {
            chain_epoch_id: guard.chain_epoch.id,
        })
    }
}

fn prepare_batch_entries(
    epoch: ChainEpoch,
    batch_size: u32,
    round: u32,
    sample_raw_tx: &[u8],
) -> Result<Vec<([u8; 32], MempoolEntry)>, BenchError> {
    let cap = usize::try_from(batch_size).unwrap_or(0);
    let mut prepared = Vec::with_capacity(cap);
    for tx_ordinal in 0..batch_size {
        let entry = synthetic_mempool_entry(epoch, round, tx_ordinal, sample_raw_tx)?;
        prepared.push((entry.transaction_id().as_bytes(), entry));
    }
    Ok(prepared)
}

fn next_envelope(sequence_slot: &mut u64, event: MempoolEvent) -> MempoolEventEnvelope {
    let seq = *sequence_slot;
    *sequence_slot = sequence_slot.saturating_add(1);
    MempoolEventEnvelope {
        cursor: StreamCursorTokenV1::from_bytes(seq.to_be_bytes().to_vec()),
        event_sequence: seq,
        source_observed_unix_millis: 1_783_933_200_000,
        event,
    }
}

fn synthetic_mempool_entry(
    chain_epoch: ChainEpoch,
    round: u32,
    ordinal: u32,
    sample_raw_tx: &[u8],
) -> Result<MempoolEntry, BenchError> {
    let mut raw = sample_raw_tx.to_vec();
    if raw.len() >= 16 {
        raw[12..16].copy_from_slice(&(round ^ ordinal.rotate_left(13)).to_le_bytes());
    }
    let mut txid_bytes = [0_u8; 32];
    txid_bytes[..4].copy_from_slice(&round.to_be_bytes());
    txid_bytes[4..8].copy_from_slice(&ordinal.to_be_bytes());
    let transaction_id = TransactionId::from_bytes(txid_bytes);
    let mut nullifier = [0_u8; 32];
    nullifier[..4].copy_from_slice(&round.to_be_bytes());
    nullifier[4..8].copy_from_slice(&ordinal.to_be_bytes());
    nullifier[31] = 0x11;
    let mut commitment = nullifier;
    commitment[31] = 0x22;
    let mut ephemeral_key = nullifier;
    ephemeral_key[31] = 0x33;
    let mut ciphertext = [0_u8; 52];
    ciphertext[..8].copy_from_slice(&nullifier[..8]);
    let compact_data = CompactTransactionData {
        orchard_actions: vec![CompactShieldedAction {
            nullifier,
            commitment,
            ephemeral_key,
            ciphertext,
        }],
        ..CompactTransactionData::default()
    };
    MempoolEntry::new(
        transaction_id,
        None,
        RawTransactionBytes::new(raw),
        compact_data,
        MempoolObservation {
            first_seen_unix_millis: UnixTimestampMillis::new(1_783_933_200_000),
            first_seen_chain_epoch: chain_epoch,
        },
    )
    .map_err(|err| BenchError::tip_replay(err.to_string()))
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc, time::Duration};

    use tokio_stream::StreamExt as _;
    use zinder_compat_lightwalletd::{MempoolSurface as _, MempoolSurfaceError};
    use zinder_core::{
        ArtifactSchemaVersion, BlockHash, BlockHeight, ChainEpoch, ChainEpochId, ChainTipMetadata,
        Network, UnixTimestampMillis,
    };

    use super::{BenchmarkMempoolSurface, EVENT_CHANNEL_CAPACITY};

    fn sample_epoch() -> ChainEpoch {
        ChainEpoch {
            id: ChainEpochId::new(1),
            network: Network::ZcashRegtest,
            visible_tip_height: BlockHeight::new(10),
            visible_tip_hash: BlockHash::from_bytes([1_u8; 32]),
            settled_tip_height: BlockHeight::new(6),
            settled_tip_hash: BlockHash::from_bytes([2_u8; 32]),
            artifact_schema_version: ArtifactSchemaVersion::new(1),
            tip_metadata: ChainTipMetadata::empty(),
            created_at: UnixTimestampMillis::new(1),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mempool_events_handoff_preserves_every_event_under_concurrent_churn()
    -> Result<(), Box<dyn Error>> {
        let surface = BenchmarkMempoolSurface::new(sample_epoch());
        let batch_size = 8_u32;
        let rounds = 24_u32;
        let expected_total =
            u64::from(batch_size) + u64::from(rounds - 1) * u64::from(batch_size) * 2;
        let expected_sequences: Vec<u64> = (1..=expected_total).collect();

        let writer_surface = Arc::clone(&surface);
        let writer = tokio::task::spawn_blocking(move || -> Result<(), super::BenchError> {
            let sample_tx = [0_u8; 16];
            for round in 1..=rounds {
                writer_surface.apply_churn_batch(batch_size, round, &sample_tx)?;
                std::thread::yield_now();
            }
            Ok(())
        });

        let mut streams = Vec::with_capacity(usize::try_from(rounds)?);
        for _ in 0..rounds {
            streams.push(surface.mempool_events(None).await?);
            tokio::task::yield_now().await;
        }
        writer.await??;

        for mut stream in streams {
            let mut observed = Vec::with_capacity(usize::try_from(expected_total)?);
            tokio::time::timeout(Duration::from_secs(5), async {
                while let Some(entry) = stream.next().await {
                    let env = entry?;
                    let seq = env.event_sequence;
                    observed.push(seq);
                    if seq >= expected_total {
                        break;
                    }
                }
                Ok::<(), MempoolSurfaceError>(())
            })
            .await??;
            assert_eq!(observed, expected_sequences);
        }
        Ok(())
    }

    #[tokio::test]
    async fn mempool_events_surfaces_cursor_expired_when_broadcast_lags()
    -> Result<(), Box<dyn Error>> {
        let surface = BenchmarkMempoolSurface::new(sample_epoch());
        let mut stream = surface.mempool_events(None).await?;
        let overflow_batch = u32::try_from(EVENT_CHANNEL_CAPACITY + 32)?;
        surface.apply_churn_batch(overflow_batch, 1, &[0_u8; 16])?;
        let item = stream.next().await.ok_or("expected stream error")?;
        assert!(matches!(item, Err(MempoolSurfaceError::CursorExpired)));
        Ok(())
    }
}
