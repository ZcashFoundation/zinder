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
    BlockHeight, ChainEpoch, ChainEpochId, MinedTransaction, MinedTransactionChainContext,
    TransactionId, TransactionLocation, TxStatus,
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
    Fallback(Box<dyn LazyQuerySource>),
    Serving(Box<ServingOrderedTransactions>),
}

trait LazyQuerySource: Send + Sync {
    fn has_remaining(&self) -> bool;
    fn start_next(&mut self, max_count: usize) -> Vec<PendingTransaction>;
}

struct LazyQueryReader<QueryApi> {
    query_api: QueryApi,
    transaction_ids: Vec<TransactionId>,
    next_index: usize,
    at_epoch_id: ChainEpochId,
}

impl<QueryApi> LazyQuerySource for LazyQueryReader<QueryApi>
where
    QueryApi: crate::WalletQueryApi + Clone + Send + Sync + 'static,
{
    fn has_remaining(&self) -> bool {
        self.next_index < self.transaction_ids.len()
    }

    fn start_next(&mut self, max_count: usize) -> Vec<PendingTransaction> {
        let end = self
            .next_index
            .saturating_add(max_count)
            .min(self.transaction_ids.len());
        let at_epoch_id = self.at_epoch_id;
        let batch = self.transaction_ids[self.next_index..end]
            .iter()
            .copied()
            .map(|transaction_id| {
                let query_api = self.query_api.clone();
                PendingTransaction::cold(async move {
                    query_api
                        .transaction(transaction_id, Some(at_epoch_id))
                        .await
                })
            })
            .collect();
        self.next_index = end;
        batch
    }
}

struct ServingOrderedTransactions {
    slot: WalletServingPairSlot,
    chain_epoch: ChainEpoch,
    retains_transaction_blobs: bool,
    cold_transactions: Option<Arc<ColdTransactionSource>>,
    transaction_ids: Vec<TransactionId>,
    next_id_index: usize,
    refill_in_flight: bool,
    prepared: VecDeque<PreparedTransaction>,
}

/// Completed canonical batch lookup ready to be applied to [`TransactionsInOrder`].
pub struct CompletedCanonicalRefill {
    outcome: Result<Vec<PreparedTransaction>, QueryError>,
}

impl fmt::Debug for CompletedCanonicalRefill {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompletedCanonicalRefill")
            .field("is_ok", &self.outcome.is_ok())
            .finish()
    }
}

/// In-flight canonical batch lookup started by [`TransactionsInOrder::start_refill`].
///
/// Polling this future alongside [`PendingTransaction`] futures lets a stream
/// keep draining completed rows and observing cancellation while a canonical
/// refill runs.
pub struct CanonicalRefill {
    future: BoxFuture<'static, Result<Vec<PreparedTransaction>, QueryError>>,
}

impl fmt::Debug for CanonicalRefill {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("CanonicalRefill").finish()
    }
}

impl Future for CanonicalRefill {
    type Output = CompletedCanonicalRefill;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.future
            .as_mut()
            .poll(context)
            .map(|outcome| CompletedCanonicalRefill { outcome })
    }
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

    pub(crate) fn from_lazy_query<QueryApi>(
        query_api: QueryApi,
        transaction_ids: Vec<TransactionId>,
        at_epoch_id: ChainEpochId,
    ) -> Self
    where
        QueryApi: crate::WalletQueryApi + Clone + Send + Sync + 'static,
    {
        if transaction_ids.is_empty() {
            return Self::empty();
        }
        Self {
            source: OrderedSource::Fallback(Box::new(LazyQueryReader {
                query_api,
                transaction_ids,
                next_index: 0,
                at_epoch_id,
            })),
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
                refill_in_flight: false,
                prepared: VecDeque::new(),
            })),
            ended_on_error: false,
        }
    }

    /// Maximum [`PendingTransaction`] futures a caller should keep in flight at
    /// once so a cold history stream never exceeds its configured
    /// [`ForwardedReadKind::History`] allowance.
    #[must_use]
    pub fn max_in_flight(&self) -> usize {
        match &self.source {
            OrderedSource::Empty => 0,
            OrderedSource::Fallback(_) => usize::MAX,
            OrderedSource::Serving(serving) => {
                if serving.retains_transaction_blobs {
                    usize::MAX
                } else {
                    serving
                        .cold_transactions
                        .as_ref()
                        .map_or(usize::MAX, |source| source.history_max_concurrent().max(1))
                }
            }
        }
    }

    /// Reports whether more transactions remain to be started or refilled.
    #[must_use]
    pub fn has_remaining(&self) -> bool {
        if self.ended_on_error {
            return false;
        }
        match &self.source {
            OrderedSource::Empty => false,
            OrderedSource::Fallback(reader) => reader.has_remaining(),
            OrderedSource::Serving(serving) => {
                serving.refill_in_flight
                    || serving.next_id_index < serving.transaction_ids.len()
                    || !serving.prepared.is_empty()
            }
        }
    }

    /// Reports whether [`Self::take_ready`] can immediately start at least one
    /// transaction without awaiting a canonical refill.
    #[must_use]
    pub fn can_take_ready(&self) -> bool {
        if self.ended_on_error {
            return false;
        }
        match &self.source {
            OrderedSource::Empty => false,
            OrderedSource::Fallback(reader) => reader.has_remaining(),
            OrderedSource::Serving(serving) => !serving.prepared.is_empty(),
        }
    }

    /// Reports whether the next transaction requires starting a canonical batch
    /// refill via [`Self::start_refill`].
    #[must_use]
    pub fn needs_refill(&self) -> bool {
        if self.ended_on_error {
            return false;
        }
        match &self.source {
            OrderedSource::Empty | OrderedSource::Fallback(_) => false,
            OrderedSource::Serving(serving) => {
                !serving.refill_in_flight
                    && serving.prepared.is_empty()
                    && serving.next_id_index < serving.transaction_ids.len()
            }
        }
    }

    /// Starts up to `max_count` already-prepared transactions in request order
    /// without awaiting canonical storage.
    pub fn take_ready(&mut self, max_count: usize) -> Vec<PendingTransaction> {
        if max_count == 0 || !self.can_take_ready() {
            return Vec::new();
        }
        match &mut self.source {
            OrderedSource::Empty => Vec::new(),
            OrderedSource::Fallback(reader) => reader.start_next(max_count),
            OrderedSource::Serving(serving) => {
                serving.take_prepared(max_count, &mut self.ended_on_error)
            }
        }
    }

    /// Starts the next bounded canonical multi-get refill when [`Self::needs_refill`]
    /// is true.
    pub fn start_refill(&mut self, needed_now: usize) -> Option<CanonicalRefill> {
        if needed_now == 0 || !self.needs_refill() {
            return None;
        }
        match &mut self.source {
            OrderedSource::Empty | OrderedSource::Fallback(_) => None,
            OrderedSource::Serving(serving) => serving.start_canonical_slice(needed_now),
        }
    }

    /// Applies a completed canonical refill started by [`Self::start_refill`].
    pub fn apply_refill(&mut self, completed: CompletedCanonicalRefill) {
        if let OrderedSource::Serving(serving) = &mut self.source {
            serving.finish_canonical_slice(completed.outcome);
        }
    }

    /// Starts up to `max_count` next transactions in request order.
    ///
    /// Stops early after the first immediate error so no transaction past a
    /// terminal canonical failure is started.
    pub async fn next_batch(&mut self, max_count: usize) -> Vec<PendingTransaction> {
        let mut batch = Vec::with_capacity(max_count);
        while batch.len() < max_count && !self.ended_on_error {
            let remaining_budget = max_count - batch.len();
            let ready = self.take_ready(remaining_budget);
            if !ready.is_empty() {
                batch.extend(ready);
                continue;
            }
            let Some(refill) = self.start_refill(remaining_budget) else {
                break;
            };
            let completed = refill.await;
            self.apply_refill(completed);
        }
        batch
    }
}

impl ServingOrderedTransactions {
    fn take_prepared(
        &mut self,
        max_count: usize,
        ended_on_error: &mut bool,
    ) -> Vec<PendingTransaction> {
        let mut batch = Vec::with_capacity(max_count.min(self.prepared.len()));
        while batch.len() < max_count && !*ended_on_error {
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

    fn start_canonical_slice(&mut self, needed_now: usize) -> Option<CanonicalRefill> {
        let remaining = self.transaction_ids.len() - self.next_id_index;
        let target_len = if self.retains_transaction_blobs {
            needed_now.min(remaining)
        } else {
            needed_now.max(CANONICAL_LOCATION_BATCH_SIZE).min(remaining)
        };
        if target_len == 0 {
            return None;
        }
        let batch_ids =
            self.transaction_ids[self.next_id_index..self.next_id_index + target_len].to_vec();
        self.next_id_index += target_len;
        self.refill_in_flight = true;
        let slot = self.slot.clone();
        let epoch_id = self.chain_epoch.id;
        let has_cold_source = self.cold_transactions.is_some();
        let future = async move {
            let pin = serving_read::SlotPin::new(&slot, Some(epoch_id));
            serving_read::read_at(pin, move |pair| {
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
            .await
        }
        .in_current_span();
        Some(CanonicalRefill {
            future: Box::pin(future),
        })
    }

    fn finish_canonical_slice(&mut self, batch_read: Result<Vec<PreparedTransaction>, QueryError>) {
        self.refill_in_flight = false;
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
