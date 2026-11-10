//! `ExplorerQuery.UtxoSetSummary` handler.
//!
//! Composes directly from `WalletQuery.TransparentUtxoSetSummary` and forwards
//! the wallet endpoint's refusal unchanged; the explorer handler only adds the
//! standard freshness envelope.

use tonic::{Request, Response, Status};
use zinder_proto::capabilities::EXPLORER_UTXO_SET_SUMMARY_V1;
use zinder_proto::v1::explorer::{UtxoSetSummaryRequest, UtxoSetSummaryResponse};
use zinder_proto::v1::wallet::{
    TransparentUtxoSetSummaryRequest, wallet_query_client::WalletQueryClient,
};
use zinder_runtime::AuthenticatedChannel;

use super::error::ExplorerError;
use super::freshness::{
    UpstreamObservationCache, attach_upstream_observation, build_explorer_freshness,
};
use zinder_materialized_views::MaterializedViewStore;

/// Executes one `ExplorerQuery.UtxoSetSummary` request.
pub(crate) async fn query_utxo_set_summary(
    materialized_view_store: Option<&MaterializedViewStore>,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    upstream_observation_cache: &UpstreamObservationCache,
    request: Request<UtxoSetSummaryRequest>,
) -> Result<Response<UtxoSetSummaryResponse>, Status> {
    let at_epoch_id = request.into_inner().at_epoch_id;
    let response = wallet_client
        .transparent_utxo_set_summary(Request::new(TransparentUtxoSetSummaryRequest {
            at_epoch_id,
        }))
        .await?
        .into_inner();
    let chain_epoch = response
        .chain_view
        .and_then(|chain_view| chain_view.chain_epoch)
        .ok_or_else(|| {
            ExplorerError::internal(
                "TransparentUtxoSetSummaryResponse.chain_view.chain_epoch missing",
            )
        })?;

    let freshness = attach_upstream_observation(
        upstream_observation_cache,
        build_explorer_freshness(
            materialized_view_store,
            EXPLORER_UTXO_SET_SUMMARY_V1,
            Some(chain_epoch),
            0,
        )?,
    )
    .await;
    Ok(Response::new(UtxoSetSummaryResponse {
        freshness: Some(freshness),
        utxo_count: response.utxo_count,
        total_value_zat: response.total_value_zat,
        summarized_height: response.summarized_height,
        commitment: response.commitment,
    }))
}
