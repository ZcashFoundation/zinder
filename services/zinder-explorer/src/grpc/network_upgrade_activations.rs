//! The wallet query's network-upgrade activation table, cached for explorer
//! handlers.
//!
//! The query serves the table the canonical writer admitted, so the explorer
//! needs no node of its own for it. The table is fetched on first use and
//! refreshed after [`REFRESH_INTERVAL`], so a query that was unreachable when
//! the explorer started, or a schedule revision the writer admitted later,
//! reaches explorer responses without a restart.

use std::{
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
        let cached = self.cached.lock().clone();
        if let Some(cached) = &cached
            && cached.fetched_at.elapsed() < REFRESH_INTERVAL
        {
            return Ok(Arc::clone(&cached.activations));
        }
        match fetch_network_upgrade_activations(wallet_client, self.network).await {
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
