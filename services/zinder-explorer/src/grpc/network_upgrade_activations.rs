//! The wallet query's network-upgrade activation table, cached for explorer
//! handlers.
//!
//! The query serves the table the canonical writer admitted, so the explorer
//! needs no node of its own for it. The table is fetched on first use and
//! refreshed after [`REFRESH_INTERVAL`], so a query that was unreachable when
//! the explorer started, or a schedule revision the writer admitted later,
//! reaches explorer responses without a restart.

use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
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

/// Age after which a cached table is fetched again.
const REFRESH_INTERVAL: Duration = Duration::from_mins(1);

/// The last activation table fetched from the wallet query.
#[derive(Clone)]
pub(crate) struct NetworkUpgradeActivationsCache {
    network: Network,
    cached: Arc<Mutex<Option<CachedActivations>>>,
}

#[derive(Clone)]
struct CachedActivations {
    fetched_at: Instant,
    activations: Arc<NetworkUpgradeActivations>,
}

impl NetworkUpgradeActivationsCache {
    /// Starts with no table for `network`.
    pub(crate) fn new(network: Network) -> Self {
        Self {
            network,
            cached: Arc::new(Mutex::new(None)),
        }
    }

    /// Returns the table, fetching it through `wallet_client` when none is
    /// cached or the cached one is older than [`REFRESH_INTERVAL`].
    ///
    /// A failed refresh keeps serving the cached table. Without one, the
    /// failure is returned.
    pub(crate) async fn current(
        &self,
        wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    ) -> Result<Arc<NetworkUpgradeActivations>, Status> {
        let network = self.network;
        self.current_or_fetch(|| fetch_network_upgrade_activations(wallet_client, network))
            .await
    }

    /// Returns the cached table while it is fresh, and otherwise the table
    /// `fetch` returns, falling back to the cached one when `fetch` fails.
    async fn current_or_fetch<Fetch, Fetched>(
        &self,
        fetch: Fetch,
    ) -> Result<Arc<NetworkUpgradeActivations>, Status>
    where
        Fetch: FnOnce() -> Fetched,
        Fetched: Future<Output = Result<NetworkUpgradeActivations, Status>>,
    {
        let cached = self.cached.lock().clone();
        if let Some(cached) = &cached
            && cached.fetched_at.elapsed() < REFRESH_INTERVAL
        {
            return Ok(Arc::clone(&cached.activations));
        }
        match fetch().await {
            Ok(activations) => {
                let activations = Arc::new(activations);
                *self.cached.lock() = Some(CachedActivations {
                    fetched_at: Instant::now(),
                    activations: Arc::clone(&activations),
                });
                Ok(activations)
            }
            Err(status) => match cached {
                Some(cached) => {
                    tracing::warn!(
                        target: "zinder::explorer",
                        event = "network_upgrade_activations_refresh_failed",
                        code = ?status.code(),
                        error = %status.message(),
                        "could not refresh network-upgrade activations from the wallet query; \
                         serving the cached table"
                    );
                    Ok(cached.activations)
                }
                None => Err(status),
            },
        }
    }
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

    /// Ages the cached table past [`REFRESH_INTERVAL`].
    fn expire(cache: &NetworkUpgradeActivationsCache) -> Result<(), Status> {
        let expired_at = Instant::now()
            .checked_sub(REFRESH_INTERVAL + Duration::from_secs(1))
            .ok_or_else(|| Status::internal("the clock cannot be set back"))?;
        cache
            .cached
            .lock()
            .as_mut()
            .ok_or_else(|| Status::internal("no table is cached"))?
            .fetched_at = expired_at;
        Ok(())
    }

    fn sapling_height(activations: &NetworkUpgradeActivations) -> Option<BlockHeight> {
        activations.activation_height_by_name("Sapling")
    }

    #[tokio::test]
    async fn fetches_once_and_serves_the_cached_table_while_fresh() -> Result<(), Status> {
        let cache = NetworkUpgradeActivationsCache::new(Network::ZcashRegtest);
        let first = cache.current_or_fetch(|| async { table(1) }).await?;
        let second = cache
            .current_or_fetch(|| async { Err(Status::internal("a fresh table is not refetched")) })
            .await?;

        assert_eq!(sapling_height(&first), Some(BlockHeight::new(1)));
        assert!(Arc::ptr_eq(&first, &second));
        Ok(())
    }

    #[tokio::test]
    async fn refreshes_an_expired_table() -> Result<(), Status> {
        let cache = NetworkUpgradeActivationsCache::new(Network::ZcashRegtest);
        cache.current_or_fetch(|| async { table(1) }).await?;
        expire(&cache)?;

        let refreshed = cache.current_or_fetch(|| async { table(2) }).await?;

        assert_eq!(sapling_height(&refreshed), Some(BlockHeight::new(2)));
        Ok(())
    }

    #[tokio::test]
    async fn serves_the_cached_table_when_a_refresh_fails() -> Result<(), Status> {
        let cache = NetworkUpgradeActivationsCache::new(Network::ZcashRegtest);
        cache.current_or_fetch(|| async { table(1) }).await?;
        expire(&cache)?;

        let stale = cache
            .current_or_fetch(|| async { Err(Status::unavailable("wallet query is down")) })
            .await?;

        assert_eq!(sapling_height(&stale), Some(BlockHeight::new(1)));
        Ok(())
    }

    #[tokio::test]
    async fn returns_the_fetch_failure_when_nothing_is_cached() {
        let cache = NetworkUpgradeActivationsCache::new(Network::ZcashRegtest);

        let outcome = cache
            .current_or_fetch(|| async { Err(Status::unavailable("wallet query is down")) })
            .await;

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
