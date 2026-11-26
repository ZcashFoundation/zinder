//! Ordered, bounded-batch reads of mined transactions at one chain epoch.
//!
//! [`TransactionsInOrder`] amortizes canonical index, block-header, and
//! retained-blob lookups into bounded multi-get batches while returning each
//! uncached cold transaction body as an independent [`PendingTransaction`]
//! future. A streaming consumer can therefore drive up to its channel bound of
//! cold node reads concurrently in [`futures_util::stream::FuturesOrdered`]
//! without batch barriers, without reading retained blobs past its channel
//! capacity, and without pinning a serving pair while idle.

use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_util::future::BoxFuture;
use tracing::Instrument as _;
use zinder_core::{
    BlockHeight, ChainEpoch, MinedTransaction, MinedTransactionChainContext, TransactionId,
    TransactionLocation, TxStatus,
};
use zinder_store::ArtifactFamily;

use crate::{
    ForwardedReadKind, IndexedTransaction, MinedTransactionDigest, QueryError, TransactionStatus,
    WalletServingPairSlot, WalletServingQuery, cold_transaction::ColdTransactionSource,
    serving_read,
};

/// Canonical location rows prefetched in one blocking multi-get when the store
/// retains no transaction bodies.
const CANONICAL_LOCATION_BATCH_SIZE: usize = 16;

/// One transaction lookup started by [`TransactionsInOrder`].
///
/// Local hits, proven absences, and cached cold transaction bodies resolve on
/// their first poll without spawning a task. Uncached cold bodies fetch from
/// the verified node under [`ForwardedReadKind::History`] and stop as soon as
/// dropped or when the serving pair revokes reads.
pub struct PendingTransaction {
    state: PendingTransactionState,
}

enum PendingTransactionState {
    Ready(Option<Box<Result<TransactionStatus, QueryError>>>),
    Cold(BoxFuture<'static, Result<TransactionStatus, QueryError>>),
}

impl fmt::Debug for PendingTransaction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state_label = match &self.state {
            PendingTransactionState::Ready(_) => "Ready",
            PendingTransactionState::Cold(_) => "Cold",
        };
        formatter
            .debug_struct("PendingTransaction")
            .field("state", &state_label)
            .finish()
    }
}

impl PendingTransaction {
    pub(crate) fn ready(outcome: Result<TransactionStatus, QueryError>) -> Self {
        Self::from_boxed_ready(Box::new(outcome))
    }

    const fn from_boxed_ready(outcome: Box<Result<TransactionStatus, QueryError>>) -> Self {
        Self {
            state: PendingTransactionState::Ready(Some(outcome)),
        }
    }

    fn cold(
        future: impl Future<Output = Result<TransactionStatus, QueryError>> + Send + 'static,
    ) -> Self {
        Self {
            state: PendingTransactionState::Cold(Box::pin(future.in_current_span())),
        }
    }
}

impl Future for PendingTransaction {
    type Output = Result<TransactionStatus, QueryError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.state {
            PendingTransactionState::Ready(ready_outcome) => {
                Poll::Ready(ready_outcome.take().map_or_else(
                    || {
                        Err(QueryError::BlockingTaskFailed {
                            reason: "pending transaction polled after completion".to_owned(),
                        })
                    },
                    |boxed| *boxed,
                ))
            }
            PendingTransactionState::Cold(future) => future.as_mut().poll(context),
        }
    }
}

/// Reads a sequence of transactions in request order at one chain epoch.
///
/// Canonical locations, block headers, and retained transaction bodies are read
/// in bounded batches, while uncached cold bodies run concurrently through
/// [`PendingTransaction`] futures as the caller reserves output capacity.
pub struct TransactionsInOrder {
    source: OrderedSource,
    ended_on_error: bool,
}

enum OrderedSource {
    Empty,
    Preloaded(VecDeque<Result<TransactionStatus, QueryError>>),
    Serving(Box<ServingOrderedTransactions>),
}

struct ServingOrderedTransactions {
    slot: WalletServingPairSlot,
    chain_epoch: ChainEpoch,
    retains_transaction_blobs: bool,
    cold_transactions: Option<Arc<ColdTransactionSource>>,
    transaction_ids: Vec<TransactionId>,
    next_id_index: usize,
    prepared: VecDeque<PreparedTransaction>,
}

enum PreparedTransaction {
    Ready(Box<Result<TransactionStatus, QueryError>>),
    Cold {
        location: TransactionLocation,
        chain_context: MinedTransactionChainContext,
    },
}

impl PreparedTransaction {
    fn ready(outcome: Result<TransactionStatus, QueryError>) -> Self {
        Self::Ready(Box::new(outcome))
    }
}

impl fmt::Debug for TransactionsInOrder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionsInOrder")
            .field("has_remaining", &self.has_remaining())
            .finish()
    }
}

impl TransactionsInOrder {
    /// An empty ordered reader that yields no transactions.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            source: OrderedSource::Empty,
            ended_on_error: false,
        }
    }

    pub(crate) fn from_preloaded(
        outcomes: VecDeque<Result<TransactionStatus, QueryError>>,
    ) -> Self {
        Self {
            source: OrderedSource::Preloaded(outcomes),
            ended_on_error: false,
        }
    }

    pub(crate) fn from_serving(
        slot: WalletServingPairSlot,
        chain_epoch: ChainEpoch,
        retains_transaction_blobs: bool,
        cold_transactions: Option<Arc<ColdTransactionSource>>,
        transaction_ids: Vec<TransactionId>,
    ) -> Self {
        if transaction_ids.is_empty() {
            return Self::empty();
        }
        Self {
            source: OrderedSource::Serving(Box::new(ServingOrderedTransactions {
                slot,
                chain_epoch,
                retains_transaction_blobs,
                cold_transactions,
                transaction_ids,
                next_id_index: 0,
                prepared: VecDeque::new(),
            })),
            ended_on_error: false,
        }
    }

    /// Reports whether more transactions remain to be started.
    #[must_use]
    pub fn has_remaining(&self) -> bool {
        if self.ended_on_error {
            return false;
        }
        match &self.source {
            OrderedSource::Empty => false,
            OrderedSource::Preloaded(outcomes) => !outcomes.is_empty(),
            OrderedSource::Serving(serving) => {
                serving.next_id_index < serving.transaction_ids.len()
                    || !serving.prepared.is_empty()
            }
        }
    }

    /// Starts up to `max_count` next transactions in request order.
    ///
    /// Stops early after the first immediate error so no transaction past a
    /// terminal canonical failure is started.
    pub async fn next_batch(&mut self, max_count: usize) -> Vec<PendingTransaction> {
        if max_count == 0 || !self.has_remaining() {
            return Vec::new();
        }
        match &mut self.source {
            OrderedSource::Empty => Vec::new(),
            OrderedSource::Preloaded(outcomes) => {
                take_preloaded_batch(outcomes, max_count, &mut self.ended_on_error)
            }
            OrderedSource::Serving(serving) => {
                serving
                    .next_batch(max_count, &mut self.ended_on_error)
                    .await
            }
        }
    }
}

fn take_preloaded_batch(
    outcomes: &mut VecDeque<Result<TransactionStatus, QueryError>>,
    max_count: usize,
    ended_on_error: &mut bool,
) -> Vec<PendingTransaction> {
    let mut batch = Vec::with_capacity(max_count.min(outcomes.len()));
    while batch.len() < max_count {
        let Some(outcome) = outcomes.pop_front() else {
            break;
        };
        let failed = outcome.is_err();
        batch.push(PendingTransaction::ready(outcome));
        if failed {
            *ended_on_error = true;
            break;
        }
    }
    batch
}

impl ServingOrderedTransactions {
    async fn next_batch(
        &mut self,
        max_count: usize,
        ended_on_error: &mut bool,
    ) -> Vec<PendingTransaction> {
        let mut batch = Vec::with_capacity(max_count);
        while batch.len() < max_count && !*ended_on_error {
            if self.prepared.is_empty() {
                if self.next_id_index >= self.transaction_ids.len() {
                    break;
                }
                self.prepare_next_canonical_slice(max_count - batch.len())
                    .await;
            }
            let Some(prepared) = self.prepared.pop_front() else {
                break;
            };
            let (pending, failed_immediately) = self.start_prepared(prepared);
            batch.push(pending);
            if failed_immediately {
                *ended_on_error = true;
                break;
            }
        }
        batch
    }

    async fn prepare_next_canonical_slice(&mut self, needed_now: usize) {
        let remaining = self.transaction_ids.len() - self.next_id_index;
        let target_len = if self.retains_transaction_blobs {
            needed_now.min(remaining)
        } else {
            needed_now.max(CANONICAL_LOCATION_BATCH_SIZE).min(remaining)
        };
        if target_len == 0 {
            return;
        }
        let batch_ids =
            self.transaction_ids[self.next_id_index..self.next_id_index + target_len].to_vec();
        self.next_id_index += target_len;
        let epoch_id = self.chain_epoch.id;
        let has_cold_source = self.cold_transactions.is_some();
        let pin = serving_read::SlotPin::new(&self.slot, Some(epoch_id));
        let batch_read = serving_read::read_at(pin, move |pair| {
            let ids = batch_ids.clone();
            async move {
                let chain_epoch = WalletServingQuery::<()>::chain_epoch(&pair, Some(epoch_id))?;
                if pair.are_reads_revoked() {
                    return Err(pair.revoked_read_error());
                }
                let indexed = pair
                    .canonical()
                    .mined_transactions(&ids)
                    .await
                    .map_err(QueryError::canonical_read)?;
                if indexed.len() != ids.len() {
                    return Err(QueryError::ArtifactCorrupt {
                        family: ArtifactFamily::TransactionLocation,
                        reason: format!(
                            "canonical batch transaction lookup returned {} entries for {} requested ids",
                            indexed.len(),
                            ids.len(),
                        ),
                    });
                }
                Ok(classify_indexed_batch(
                    &pair,
                    chain_epoch,
                    indexed,
                    has_cold_source,
                ))
            }
        })
        .await;
        match batch_read {
            Ok(classified) => self.prepared.extend(classified),
            Err(error) => {
                self.next_id_index = self.transaction_ids.len();
                self.prepared
                    .push_back(PreparedTransaction::ready(Err(error)));
            }
        }
    }

    fn start_prepared(&self, prepared: PreparedTransaction) -> (PendingTransaction, bool) {
        match prepared {
            PreparedTransaction::Ready(outcome) => {
                let failed = outcome.is_err();
                (PendingTransaction::from_boxed_ready(outcome), failed)
            }
            PreparedTransaction::Cold {
                location,
                chain_context,
            } => self.start_cold(location, chain_context),
        }
    }

    fn start_cold(
        &self,
        location: TransactionLocation,
        chain_context: MinedTransactionChainContext,
    ) -> (PendingTransaction, bool) {
        let Some(cold_source) = &self.cold_transactions else {
            return (
                PendingTransaction::ready(Ok(mined_status(
                    self.chain_epoch,
                    location,
                    chain_context,
                    None,
                ))),
                false,
            );
        };
        let pair = self.slot.capture_at(Some(self.chain_epoch.id));
        if pair.chain_epoch().id != self.chain_epoch.id || pair.are_reads_revoked() {
            return (
                PendingTransaction::ready(Err(QueryError::ChainEpochPinUnavailable {
                    chain_epoch_id: self.chain_epoch.id,
                })),
                true,
            );
        }
        let mined = match MinedTransactionDigest::from_location(location) {
            Ok(mined) => mined,
            Err(error) => return (PendingTransaction::ready(Err(error)), true),
        };
        if let Some(cached_bytes) = cold_source.cached(&mined) {
            drop(pair);
            return (
                PendingTransaction::ready(Ok(mined_status(
                    self.chain_epoch,
                    location,
                    chain_context,
                    Some(cached_bytes.to_vec()),
                ))),
                false,
            );
        }
        let chain_epoch = self.chain_epoch;
        let source = Arc::clone(cold_source);
        let pending = PendingTransaction::cold(async move {
            let raw_bytes = tokio::select! {
                biased;
                () = pair.reads_revoked() => return Err(pair.revoked_read_error()),
                fetched = source.fetch_with_kind(
                    pair.canonical(),
                    location,
                    ForwardedReadKind::History,
                ) => fetched?,
            };
            drop(pair);
            Ok(mined_status(
                chain_epoch,
                location,
                chain_context,
                Some(raw_bytes),
            ))
        });
        (pending, false)
    }
}

fn classify_indexed_batch(
    pair: &crate::WalletServingReadPair,
    chain_epoch: ChainEpoch,
    indexed: Vec<Option<IndexedTransaction>>,
    has_cold_source: bool,
) -> Vec<PreparedTransaction> {
    let history_bounds = pair.canonical().history_bounds();
    let activations = pair.network_upgrade_activations();
    indexed
        .into_iter()
        .map(|entry| match entry {
            None if history_bounds.intentionally_excludes(BlockHeight::new(1)) => {
                PreparedTransaction::ready(Err(QueryError::CanonicalHistoryUnavailable {
                    first_available_height: history_bounds.first_available_height(),
                }))
            }
            None => PreparedTransaction::ready(Ok(TransactionStatus {
                chain_epoch,
                status: TxStatus::NotFound,
            })),
            Some(transaction) => {
                let Some(header) = transaction.block_header else {
                    return PreparedTransaction::ready(Err(QueryError::BlockNotInBestChain));
                };
                let location = transaction.location;
                let chain_context = MinedTransactionChainContext::from_response_epoch(
                    &chain_epoch,
                    location.block_height,
                    activations.consensus_branch_id_at(location.block_height),
                    header.block_time,
                );
                match (transaction.transaction_blob, has_cold_source) {
                    (Some(blob), _) => PreparedTransaction::ready(Ok(mined_status(
                        chain_epoch,
                        location,
                        chain_context,
                        Some(blob.raw_transaction_bytes),
                    ))),
                    (None, true) => PreparedTransaction::Cold {
                        location,
                        chain_context,
                    },
                    (None, false) => PreparedTransaction::ready(Ok(mined_status(
                        chain_epoch,
                        location,
                        chain_context,
                        None,
                    ))),
                }
            }
        })
        .collect()
}

fn mined_status(
    chain_epoch: ChainEpoch,
    location: TransactionLocation,
    chain_context: MinedTransactionChainContext,
    raw_transaction_bytes: Option<Vec<u8>>,
) -> TransactionStatus {
    TransactionStatus {
        chain_epoch,
        status: TxStatus::Mined(MinedTransaction::new(
            location,
            chain_context,
            raw_transaction_bytes,
        )),
    }
}
