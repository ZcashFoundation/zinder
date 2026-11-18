//! `ExplorerQuery.NetworkUpgradeStatus` handler.
//!
//! Projects the writer-admitted [`NetworkUpgradeActivations`] table onto the
//! wire, anchoring the `active` flags to the canonical tip height read from
//! `WalletQuery.VisibleTipBlock`.

use tonic::{Request, Response, Status};
use zinder_core::wire::encode_branch_id_hex;
use zinder_core::{BlockHeight, NetworkUpgradeActivations};
use zinder_materialized_views::MaterializedViewStore;
use zinder_proto::capabilities::EXPLORER_NETWORK_UPGRADE_STATUS_V1;
use zinder_proto::v1::explorer::{
    NetworkUpgradeEntry, NetworkUpgradeStatusRequest, NetworkUpgradeStatusResponse,
};
use zinder_proto::v1::wallet::{VisibleTipBlockRequest, wallet_query_client::WalletQueryClient};
use zinder_runtime::AuthenticatedChannel;

use super::error::ExplorerError;
use super::freshness::{
    UpstreamObservationCache, attach_upstream_observation, build_explorer_freshness,
};

/// Executes one `ExplorerQuery.NetworkUpgradeStatus` request.
pub(crate) async fn query_network_upgrade_status(
    materialized_view_store: Option<&MaterializedViewStore>,
    activations: &NetworkUpgradeActivations,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    upstream_observation_cache: &UpstreamObservationCache,
    _request: Request<NetworkUpgradeStatusRequest>,
) -> Result<Response<NetworkUpgradeStatusResponse>, Status> {
    let response = wallet_client
        .visible_tip_block(Request::new(VisibleTipBlockRequest { at_epoch_id: None }))
        .await?
        .into_inner();
    let chain_epoch = response
        .chain_view
        .and_then(|chain_view| chain_view.chain_epoch)
        .ok_or_else(|| {
            ExplorerError::internal("VisibleTipBlockResponse.chain_view.chain_epoch missing")
        })?;
    let tip_height = response
        .visible_tip_block
        .ok_or_else(|| {
            ExplorerError::internal("VisibleTipBlockResponse.visible_tip_block missing")
        })?
        .height;

    let upgrades = activations
        .activations()
        .iter()
        .map(|activation| {
            let activation_height = activation.activation_height.value();
            NetworkUpgradeEntry {
                name: activation.name.clone(),
                branch_id_hex: encode_branch_id_hex(activation.branch_id),
                activation_height,
                active: activation_height <= tip_height,
            }
        })
        .collect();

    let (active_upgrade_name, active_upgrade_branch_id_hex) = activations
        .active_at(BlockHeight::new(tip_height))
        .map_or_else(
            || (String::new(), String::new()),
            |activation| {
                (
                    activation.name.clone(),
                    encode_branch_id_hex(activation.branch_id),
                )
            },
        );

    let freshness = attach_upstream_observation(
        upstream_observation_cache,
        build_explorer_freshness(
            materialized_view_store,
            EXPLORER_NETWORK_UPGRADE_STATUS_V1,
            Some(chain_epoch),
            0,
        )?,
    )
    .await;
    Ok(Response::new(NetworkUpgradeStatusResponse {
        freshness: Some(freshness),
        tip_height,
        upgrades,
        active_upgrade_name,
        active_upgrade_branch_id_hex,
    }))
}
