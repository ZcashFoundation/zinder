//! The wallet query's network-upgrade activation table, cached for explorer
//! handlers.
//!
//! The query serves the table the canonical writer admitted, so the explorer
//! needs no node of its own for it. The table is fetched on first use and
//! refreshed at most once per [`REFRESH_INTERVAL`], so a query that was
//! unreachable when the explorer started, or a schedule revision the writer
//! admitted later, reaches explorer responses without a restart.

use std::{future::Future, sync::Arc, time::Duration};

use tokio::{sync::Mutex, time::Instant};
use tonic::{Request, Status};
use zinder_core::{
    BlockHeight, ConsensusBranchId, Network, NetworkUpgradeActivation, NetworkUpgradeActivations,
};
use zinder_proto::v1::wallet::{
    NetworkUpgradeActivationsRequest, NetworkUpgradeActivationsResponse,
    wallet_query_client::WalletQueryClient,
};
use zinder_runtime::AuthenticatedChannel;

use super::error::ExplorerError;

/// Interval between refresh attempts while a table is cached, whether or not
/// the last attempt succeeded.
const REFRESH_INTERVAL: Duration = Duration::from_mins(1);

/// Interval between fetch attempts while no table has been fetched.
///
/// Handlers that need the table fail until one arrives, so this is much
/// shorter than [`REFRESH_INTERVAL`]. Requests within it share the last
/// failure instead of each calling an unreachable query.
const FIRST_FETCH_RETRY_INTERVAL: Duration = Duration::from_secs(5);

/// Longest wait for one fetch from the wallet query.
///
/// The wallet channel sets keepalives but no request deadline, so a stalled
/// query would otherwise hold every request that needs a fetch.
const FETCH_TIMEOUT: Duration = Duration::from_secs(5);

/// The last activation table fetched from the wallet query.
#[derive(Clone)]
pub(crate) struct NetworkUpgradeActivationsCache {
    network: Network,
    state: Arc<Mutex<CacheState>>,
}

/// What the cache holds, and when it next calls the wallet query.
enum CacheState {
    /// No fetch has been attempted.
    Empty,
    /// No fetch has succeeded. Requests fail with `status` until `retry_at`.
    Failed { status: Status, retry_at: Instant },
    /// A table was fetched. It is served without a fetch until `refresh_at`.
    Fetched {
        activations: Arc<NetworkUpgradeActivations>,
        refresh_at: Instant,
    },
}

impl NetworkUpgradeActivationsCache {
    /// Starts with no table for `network`.
    pub(crate) fn new(network: Network) -> Self {
        Self {
            network,
            state: Arc::new(Mutex::new(CacheState::Empty)),
        }
    }

    /// Returns the table, fetching it through `wallet_client` when none is
    /// cached or the next refresh attempt is due.
    ///
    /// A failed refresh keeps serving the cached table until the next
    /// attempt. Without one, the failure is returned.
    pub(crate) async fn current(
        &self,
        wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    ) -> Result<Arc<NetworkUpgradeActivations>, Status> {
        let network = self.network;
        self.current_or_fetch(|| fetch_network_upgrade_activations(wallet_client, network))
            .await
    }

    /// Returns the cached table until its refresh is due, and otherwise the
    /// table `fetch` returns, falling back to the cached one when `fetch`
    /// fails or does not finish within [`FETCH_TIMEOUT`].
    async fn current_or_fetch<Fetch, Fetched>(
        &self,
        fetch: Fetch,
    ) -> Result<Arc<NetworkUpgradeActivations>, Status>
    where
        Fetch: FnOnce() -> Fetched,
        Fetched: Future<Output = Result<NetworkUpgradeActivations, Status>>,
    {
        let mut state = self.state.lock().await;
        let now = Instant::now();
        match &mut *state {
            CacheState::Fetched {
                activations,
                refresh_at,
            } => {
                let cached = Arc::clone(activations);
                if now < *refresh_at {
                    return Ok(cached);
                }
                // Reserving the attempt before releasing the lock keeps
                // concurrent requests on the cached table until the next
                // interval, whatever this refresh returns.
                *refresh_at = now + REFRESH_INTERVAL;
                drop(state);
                self.refresh(cached, fetch).await
            }
            CacheState::Failed { status, retry_at } if now < *retry_at => Err(status.clone()),
            CacheState::Empty | CacheState::Failed { .. } => {
                // The lock stays held, so requests that arrive meanwhile wait
                // for this fetch instead of starting their own.
                let fetched = within_fetch_timeout(fetch()).await;
                let (next_state, outcome) = match fetched {
                    Ok(activations) => {
                        let activations = Arc::new(activations);
                        (
                            CacheState::fetched(Arc::clone(&activations)),
                            Ok(activations),
                        )
                    }
                    Err(status) => (
                        CacheState::Failed {
                            status: status.clone(),
                            retry_at: Instant::now() + FIRST_FETCH_RETRY_INTERVAL,
                        },
                        Err(status),
                    ),
                };
                *state = next_state;
                outcome
            }
        }
    }

    /// Replaces `cached` with the table `fetch` returns, or keeps serving it
    /// when `fetch` fails or does not finish within [`FETCH_TIMEOUT`].
    async fn refresh<Fetch, Fetched>(
        &self,
        cached: Arc<NetworkUpgradeActivations>,
        fetch: Fetch,
    ) -> Result<Arc<NetworkUpgradeActivations>, Status>
    where
        Fetch: FnOnce() -> Fetched,
        Fetched: Future<Output = Result<NetworkUpgradeActivations, Status>>,
    {
        match within_fetch_timeout(fetch()).await {
            Ok(activations) => {
                let activations = Arc::new(activations);
                *self.state.lock().await = CacheState::fetched(Arc::clone(&activations));
                Ok(activations)
            }
            Err(status) => {
                tracing::warn!(
                    target: "zinder::explorer",
                    event = "network_upgrade_activations_refresh_failed",
                    code = ?status.code(),
                    error = %status.message(),
                    "could not refresh network-upgrade activations from the wallet query; \
                     serving the cached table until the next attempt"
                );
                Ok(cached)
            }
        }
    }
}

impl CacheState {
    fn fetched(activations: Arc<NetworkUpgradeActivations>) -> Self {
        Self::Fetched {
            activations,
            refresh_at: Instant::now() + REFRESH_INTERVAL,
        }
    }
}

/// Fails `fetch` as an unreachable wallet query when it does not finish
/// within [`FETCH_TIMEOUT`].
async fn within_fetch_timeout<Fetched>(fetch: Fetched) -> Result<NetworkUpgradeActivations, Status>
where
    Fetched: Future<Output = Result<NetworkUpgradeActivations, Status>>,
{
    tokio::time::timeout(FETCH_TIMEOUT, fetch)
        .await
        .unwrap_or_else(|_elapsed| {
            Err(ExplorerError::upstream_unreachable(format!(
                "WalletQuery.NetworkUpgradeActivations did not answer within {} seconds",
                FETCH_TIMEOUT.as_secs()
            ))
            .into())
        })
}

async fn fetch_network_upgrade_activations(
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    network: Network,
) -> Result<NetworkUpgradeActivations, Status> {
    let response = wallet_client
        .network_upgrade_activations(Request::new(NetworkUpgradeActivationsRequest {}))
        .await?
        .into_inner();
    decode_network_upgrade_activations(network, response)
}

fn decode_network_upgrade_activations(
    network: Network,
    response: NetworkUpgradeActivationsResponse,
) -> Result<NetworkUpgradeActivations, Status> {
    let activations = response
        .activations
        .into_iter()
        .map(|activation| NetworkUpgradeActivation {
            branch_id: ConsensusBranchId::new(activation.consensus_branch_id),
            activation_height: BlockHeight::new(activation.activation_height),
            name: activation.name,
        })
        .collect();
    NetworkUpgradeActivations::new(network, activations).map_err(|error| {
        ExplorerError::internal(format!(
            "WalletQuery.NetworkUpgradeActivations returned an invalid table: {error}"
        ))
        .into()
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::oneshot;
    use zinder_proto::v1::wallet;

    use super::*;

    fn table(sapling_height: u32) -> Result<NetworkUpgradeActivations, Status> {
        decode_network_upgrade_activations(
            Network::ZcashRegtest,
            NetworkUpgradeActivationsResponse {
                activations: vec![wallet::NetworkUpgradeActivation {
                    consensus_branch_id: 0x76b8_09bb,
                    name: "Sapling".to_owned(),
                    activation_height: sapling_height,
                }],
            },
        )
    }

    fn sapling_height(activations: &NetworkUpgradeActivations) -> Option<BlockHeight> {
        activations.activation_height_by_name("Sapling")
    }

    /// A fetch that counts its calls and fails, for requests that must be
    /// answered without one.
    fn counted_failure(
        calls: &AtomicUsize,
    ) -> impl FnOnce() -> std::future::Ready<Result<NetworkUpgradeActivations, Status>> + '_ {
        move || {
            calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Err(Status::unavailable("wallet query is down")))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn fetches_once_and_serves_the_cached_table_while_fresh() -> Result<(), Status> {
        let cache = NetworkUpgradeActivationsCache::new(Network::ZcashRegtest);
        let calls = AtomicUsize::new(0);
        let first = cache.current_or_fetch(|| async { table(1) }).await?;
        let second = cache.current_or_fetch(counted_failure(&calls)).await?;

        assert_eq!(sapling_height(&first), Some(BlockHeight::new(1)));
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn refreshes_an_expired_table() -> Result<(), Status> {
        let cache = NetworkUpgradeActivationsCache::new(Network::ZcashRegtest);
        cache.current_or_fetch(|| async { table(1) }).await?;
        tokio::time::advance(REFRESH_INTERVAL).await;

        let refreshed = cache.current_or_fetch(|| async { table(2) }).await?;

        assert_eq!(sapling_height(&refreshed), Some(BlockHeight::new(2)));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn serves_the_cached_table_when_a_refresh_fails() -> Result<(), Status> {
        let cache = NetworkUpgradeActivationsCache::new(Network::ZcashRegtest);
        cache.current_or_fetch(|| async { table(1) }).await?;
        tokio::time::advance(REFRESH_INTERVAL).await;

        let stale = cache
            .current_or_fetch(|| async { Err(Status::unavailable("wallet query is down")) })
            .await?;

        assert_eq!(sapling_height(&stale), Some(BlockHeight::new(1)));
        Ok(())
    }

    /// A failed refresh still counts as the interval's attempt, so requests
    /// until the next interval serve the cached table without a fetch.
    #[tokio::test(start_paused = true)]
    async fn a_failed_refresh_is_not_retried_before_the_next_interval() -> Result<(), Status> {
        let cache = NetworkUpgradeActivationsCache::new(Network::ZcashRegtest);
        let calls = AtomicUsize::new(0);
        cache.current_or_fetch(|| async { table(1) }).await?;
        tokio::time::advance(REFRESH_INTERVAL).await;
        cache.current_or_fetch(counted_failure(&calls)).await?;

        tokio::time::advance(REFRESH_INTERVAL / 2).await;
        let within_interval = cache.current_or_fetch(counted_failure(&calls)).await?;
        tokio::time::advance(REFRESH_INTERVAL / 2).await;
        let refreshed = cache.current_or_fetch(|| async { table(2) }).await?;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(sapling_height(&within_interval), Some(BlockHeight::new(1)));
        assert_eq!(sapling_height(&refreshed), Some(BlockHeight::new(2)));
        Ok(())
    }

    /// Requests that arrive while a refresh is in flight serve the cached
    /// table instead of waiting for it or starting their own.
    #[tokio::test(start_paused = true)]
    async fn requests_during_a_refresh_serve_the_cached_table() -> Result<(), Status> {
        let cache = NetworkUpgradeActivationsCache::new(Network::ZcashRegtest);
        let calls = AtomicUsize::new(0);
        cache.current_or_fetch(|| async { table(1) }).await?;
        tokio::time::advance(REFRESH_INTERVAL).await;
        let (started, refresh_started) = oneshot::channel::<()>();
        let (release, refresh_released) = oneshot::channel::<()>();

        let refreshing = cache.current_or_fetch(move || async move {
            let _ = started.send(());
            let _ = refresh_released.await;
            table(2)
        });
        let concurrent = async {
            let _ = refresh_started.await;
            let concurrent = cache.current_or_fetch(counted_failure(&calls)).await;
            let _ = release.send(());
            concurrent
        };
        let (refreshed, concurrent) = tokio::join!(refreshing, concurrent);
        let (refreshed, concurrent) = (refreshed?, concurrent?);

        assert_eq!(sapling_height(&concurrent), Some(BlockHeight::new(1)));
        assert_eq!(sapling_height(&refreshed), Some(BlockHeight::new(2)));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        Ok(())
    }

    /// Requests that arrive before any table is cached wait for the first
    /// fetch and share its table.
    #[tokio::test(start_paused = true)]
    async fn concurrent_first_requests_share_one_fetch() -> Result<(), Status> {
        let cache = NetworkUpgradeActivationsCache::new(Network::ZcashRegtest);
        let calls = AtomicUsize::new(0);
        let (started, fetch_started) = oneshot::channel::<()>();
        let (release, fetch_released) = oneshot::channel::<()>();

        let fetching = cache.current_or_fetch(move || async move {
            let _ = started.send(());
            let _ = fetch_released.await;
            table(1)
        });
        let waiting = async {
            let _ = fetch_started.await;
            let waiting = cache.current_or_fetch(counted_failure(&calls));
            tokio::pin!(waiting);
            let finished_before_the_fetch = tokio::select! {
                biased;
                _ = &mut waiting => true,
                () = std::future::ready(()) => false,
            };
            assert!(!finished_before_the_fetch);
            let _ = release.send(());
            waiting.await
        };
        let (fetched, waited) = tokio::join!(fetching, waiting);

        assert!(Arc::ptr_eq(&fetched?, &waited?));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        Ok(())
    }

    /// While no table has been fetched, a failure is shared by requests for
    /// [`FIRST_FETCH_RETRY_INTERVAL`], and the next request after it fetches
    /// again.
    #[tokio::test(start_paused = true)]
    async fn a_failed_first_fetch_is_retried_after_the_short_interval() -> Result<(), Status> {
        let cache = NetworkUpgradeActivationsCache::new(Network::ZcashRegtest);
        let calls = AtomicUsize::new(0);

        let first = cache.current_or_fetch(counted_failure(&calls)).await;
        let within_interval = cache.current_or_fetch(counted_failure(&calls)).await;
        tokio::time::advance(FIRST_FETCH_RETRY_INTERVAL).await;
        let retried = cache.current_or_fetch(|| async { table(1) }).await?;

        assert!(matches!(first, Err(status) if status.code() == tonic::Code::Unavailable));
        assert!(
            matches!(within_interval, Err(status) if status.code() == tonic::Code::Unavailable)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(sapling_height(&retried), Some(BlockHeight::new(1)));
        Ok(())
    }

    /// A refresh that never answers falls back to the cached table once
    /// [`FETCH_TIMEOUT`] passes.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_refresh_serves_the_cached_table() -> Result<(), Status> {
        let cache = NetworkUpgradeActivationsCache::new(Network::ZcashRegtest);
        cache.current_or_fetch(|| async { table(1) }).await?;
        tokio::time::advance(REFRESH_INTERVAL).await;
        let started = Instant::now();

        let stale = cache.current_or_fetch(std::future::pending).await?;

        assert_eq!(started.elapsed(), FETCH_TIMEOUT);
        assert_eq!(sapling_height(&stale), Some(BlockHeight::new(1)));
        Ok(())
    }

    /// A first fetch that never answers fails as an unreachable wallet query
    /// once [`FETCH_TIMEOUT`] passes, instead of holding the request.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_first_fetch_fails_as_unavailable() {
        let cache = NetworkUpgradeActivationsCache::new(Network::ZcashRegtest);
        let started = Instant::now();

        let outcome = cache.current_or_fetch(std::future::pending).await;

        assert_eq!(started.elapsed(), FETCH_TIMEOUT);
        assert!(matches!(outcome, Err(status) if status.code() == tonic::Code::Unavailable));
    }

    #[test]
    fn decodes_the_wallet_query_table_for_the_explorer_network() -> Result<(), Status> {
        let response = NetworkUpgradeActivationsResponse {
            activations: vec![
                wallet::NetworkUpgradeActivation {
                    consensus_branch_id: 0x76b8_09bb,
                    name: "Sapling".to_owned(),
                    activation_height: 1,
                },
                wallet::NetworkUpgradeActivation {
                    consensus_branch_id: 0xc2d6_d0b4,
                    name: "NU5".to_owned(),
                    activation_height: 2,
                },
            ],
        };

        let activations = decode_network_upgrade_activations(Network::ZcashRegtest, response)?;

        assert_eq!(activations.network(), Network::ZcashRegtest);
        assert_eq!(
            activations.consensus_branch_id_at(BlockHeight::new(2)),
            ConsensusBranchId::new(0xc2d6_d0b4)
        );
        Ok(())
    }

    #[test]
    fn refuses_a_table_with_a_duplicate_branch_id() {
        let duplicate = wallet::NetworkUpgradeActivation {
            consensus_branch_id: 0x76b8_09bb,
            name: "Sapling".to_owned(),
            activation_height: 1,
        };
        let response = NetworkUpgradeActivationsResponse {
            activations: vec![duplicate.clone(), duplicate],
        };

        let outcome = decode_network_upgrade_activations(Network::ZcashRegtest, response);

        assert!(matches!(outcome, Err(status) if status.code() == tonic::Code::Internal));
    }
}
