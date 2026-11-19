//! `ExplorerQuery.TransactionComponentSummary` handler.
//!
//! Reads exact half-open block-time aggregates from the materialized view and
//! reports whether its joined historical and live-tail coverage spans the
//! current canonical visible tip.

use tonic::{Request, Response, Status};
use zinder_core::BlockHeight;
use zinder_materialized_views::{
    MaterializedViewStore, TransactionComponentBackfillCoverage,
    TransactionComponentDay as ProjectedTransactionComponentDay,
    TransactionComponentSummary as ProjectedTransactionComponentSummary,
    TransactionComponentSummaryConsumer,
    TransactionComponentTotals as ProjectedTransactionComponentTotals,
};
use zinder_proto::capabilities::EXPLORER_TRANSACTION_COMPONENT_SUMMARY_V2;
use zinder_proto::v1::explorer::{
    TransactionComponentCoverage, TransactionComponentDay, TransactionComponentSummaryRequest,
    TransactionComponentSummaryResponse, TransactionComponentTotals,
};
use zinder_proto::v1::wallet::{
    self, VisibleTipBlockRequest, wallet_query_client::WalletQueryClient,
};
use zinder_runtime::AuthenticatedChannel;

use super::error::ExplorerError;
use super::freshness::{
    UpstreamObservationCache, attach_upstream_observation, build_explorer_freshness,
};

/// Executes one `ExplorerQuery.TransactionComponentSummary` request.
pub(crate) async fn query_transaction_component_summary(
    materialized_view_store: &MaterializedViewStore,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    upstream_observation_cache: &UpstreamObservationCache,
    request: Request<TransactionComponentSummaryRequest>,
) -> Result<Response<TransactionComponentSummaryResponse>, Status> {
    let request = request.into_inner();
    validate_time_range(
        request.start_time_unix_seconds,
        request.end_time_unix_seconds,
    )?;

    let summary = TransactionComponentSummaryConsumer::summary_in_time_range(
        materialized_view_store,
        request.start_time_unix_seconds,
        request.end_time_unix_seconds,
    )
    .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let coverage = TransactionComponentSummaryConsumer::coverage(materialized_view_store)
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let (chain_epoch, visible_tip_height) = fetch_current_chain_epoch(wallet_client).await?;
    let freshness = attach_upstream_observation(
        upstream_observation_cache,
        build_explorer_freshness(
            Some(materialized_view_store),
            EXPLORER_TRANSACTION_COMPONENT_SUMMARY_V2,
            Some(chain_epoch),
            0,
        )?,
    )
    .await;

    let (totals, days) = map_summary(summary, request.totals_only);
    Ok(Response::new(TransactionComponentSummaryResponse {
        freshness: Some(freshness),
        totals: Some(totals),
        days,
        coverage: coverage.map(|coverage| map_coverage(coverage, visible_tip_height)),
    }))
}

fn validate_time_range(start: i64, end: i64) -> Result<(), Status> {
    if start >= end {
        return Err(ExplorerError::invalid_request(
            "start_time_unix_seconds must be less than end_time_unix_seconds",
        )
        .into());
    }
    Ok(())
}

async fn fetch_current_chain_epoch(
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
) -> Result<(wallet::ChainEpoch, u32), Status> {
    let chain_epoch = wallet_client
        .visible_tip_block(Request::new(VisibleTipBlockRequest { at_epoch_id: None }))
        .await?
        .into_inner()
        .chain_view
        .and_then(|chain_view| chain_view.chain_epoch)
        .ok_or_else(|| {
            ExplorerError::internal("VisibleTipBlockResponse.chain_view.chain_epoch missing")
        })?;
    let visible_tip_height = chain_epoch
        .visible_tip
        .as_ref()
        .map(|tip| tip.height)
        .ok_or_else(|| ExplorerError::internal("ChainEpoch.visible_tip missing"))?;
    Ok((chain_epoch, visible_tip_height))
}

fn map_summary(
    summary: ProjectedTransactionComponentSummary,
    totals_only: bool,
) -> (TransactionComponentTotals, Vec<TransactionComponentDay>) {
    let days = if totals_only {
        Vec::new()
    } else {
        summary.days.into_iter().map(map_day).collect()
    };
    (map_totals(summary.totals), days)
}

fn map_totals(totals: ProjectedTransactionComponentTotals) -> TransactionComponentTotals {
    TransactionComponentTotals {
        transaction_count: totals.transaction_count,
        transparent_input_count: totals.transparent_input_count,
        transparent_output_count: totals.transparent_output_count,
        sapling_spend_count: totals.sapling_spend_count,
        sapling_output_count: totals.sapling_output_count,
        orchard_action_count: totals.orchard_action_count,
        ironwood_action_count: totals.ironwood_action_count,
        sprout_joinsplit_count: totals.sprout_joinsplit_count,
        sapling_transaction_count: totals.sapling_transaction_count,
        orchard_transaction_count: totals.orchard_transaction_count,
        ironwood_transaction_count: totals.ironwood_transaction_count,
        sprout_transaction_count: totals.sprout_transaction_count,
        sapling_or_orchard_transaction_count: totals.sapling_or_orchard_transaction_count,
        sapling_without_orchard_transaction_count: totals.sapling_without_orchard_transaction_count,
        orchard_without_sapling_transaction_count: totals.orchard_without_sapling_transaction_count,
        sapling_and_orchard_transaction_count: totals
            .sapling_and_orchard_transaction_count,
        sapling_or_orchard_fully_shielded_transaction_count: totals.sapling_or_orchard_fully_shielded_transaction_count,
        sapling_orchard_or_ironwood_transaction_count: totals
            .sapling_orchard_or_ironwood_transaction_count,
        non_coinbase_without_sapling_orchard_or_ironwood_transaction_count: totals
            .non_coinbase_without_sapling_orchard_or_ironwood_transaction_count,
        non_coinbase_sapling_orchard_or_ironwood_with_transparent_inputs_and_outputs_transaction_count: totals
            .non_coinbase_sapling_orchard_or_ironwood_with_transparent_inputs_and_outputs_transaction_count,
        non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count: totals
            .non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count,
        coinbase_transaction_count: totals.coinbase_transaction_count,
        transaction_predicate_unavailable_count: totals.transaction_predicate_unavailable_count,
    }
}

fn map_day(day: ProjectedTransactionComponentDay) -> TransactionComponentDay {
    TransactionComponentDay {
        day_start_unix_seconds: day.day_start_unix_seconds,
        totals: Some(map_totals(day.totals)),
        first_sapling_or_orchard_transaction_time_unix_seconds: day
            .first_sapling_or_orchard_transaction_time_unix_seconds,
        last_sapling_or_orchard_transaction_time_unix_seconds: day
            .last_sapling_or_orchard_transaction_time_unix_seconds,
    }
}

fn map_coverage(
    coverage: TransactionComponentBackfillCoverage,
    visible_tip_height: u32,
) -> TransactionComponentCoverage {
    TransactionComponentCoverage {
        complete_from_height: coverage.complete_from_height.value(),
        complete_through_height: coverage.complete_through_height.value(),
        complete_from_time_unix_seconds: coverage.complete_from_time_unix_seconds,
        complete_through_time_unix_seconds: coverage.complete_through_time_unix_seconds,
        requested_range_complete: canonical_coverage_reaches_visible_tip(
            coverage,
            visible_tip_height,
        ),
    }
}

fn canonical_coverage_reaches_visible_tip(
    coverage: TransactionComponentBackfillCoverage,
    visible_tip_height: u32,
) -> bool {
    coverage.complete_from_height == BlockHeight::new(1)
        && coverage.complete_through_height.value() >= visible_tip_height
}

#[cfg(test)]
mod tests {
    #![allow(
        missing_docs,
        reason = "Unit test names describe the behavior under test."
    )]

    use super::*;
    use tonic::Code;

    fn derived_totals() -> ProjectedTransactionComponentTotals {
        ProjectedTransactionComponentTotals {
            transaction_count: 1,
            transparent_input_count: 2,
            transparent_output_count: 3,
            sapling_spend_count: 4,
            sapling_output_count: 5,
            orchard_action_count: 6,
            ironwood_action_count: 7,
            sprout_joinsplit_count: 8,
            sapling_transaction_count: 9,
            orchard_transaction_count: 10,
            ironwood_transaction_count: 11,
            sprout_transaction_count: 12,
            sapling_or_orchard_transaction_count: 13,
            sapling_without_orchard_transaction_count: 14,
            orchard_without_sapling_transaction_count: 15,
            sapling_and_orchard_transaction_count: 16,
            sapling_or_orchard_fully_shielded_transaction_count: 17,
            sapling_orchard_or_ironwood_transaction_count: 18,
            non_coinbase_without_sapling_orchard_or_ironwood_transaction_count: 19,
            non_coinbase_sapling_orchard_or_ironwood_with_transparent_inputs_and_outputs_transaction_count: 20,
            non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count: 21,
            coinbase_transaction_count: 22,
            transaction_predicate_unavailable_count: 23,
        }
    }

    fn coverage(from: u32, through: u32) -> TransactionComponentBackfillCoverage {
        TransactionComponentBackfillCoverage::new(
            BlockHeight::new(from),
            BlockHeight::new(through),
            1_700_000_000,
            1_800_000_000,
        )
    }

    #[test]
    fn summary_mapping_preserves_totals_days_and_extrema() {
        let (totals, days) = map_summary(
            ProjectedTransactionComponentSummary {
                totals: derived_totals(),
                days: vec![ProjectedTransactionComponentDay {
                    day_start_unix_seconds: 1_700_006_400,
                    totals: derived_totals(),
                    first_sapling_or_orchard_transaction_time_unix_seconds: Some(1_700_006_401),
                    last_sapling_or_orchard_transaction_time_unix_seconds: Some(1_700_092_799),
                }],
            },
            false,
        );

        assert_eq!(totals.transaction_count, 1);
        assert_eq!(totals.ironwood_action_count, 7);
        assert_eq!(
            totals.sapling_or_orchard_fully_shielded_transaction_count,
            17
        );
        assert_eq!(totals.sapling_orchard_or_ironwood_transaction_count, 18);
        assert_eq!(
            totals.non_coinbase_without_sapling_orchard_or_ironwood_transaction_count,
            19
        );
        assert_eq!(
            totals
                .non_coinbase_sapling_orchard_or_ironwood_with_transparent_inputs_and_outputs_transaction_count,
            20
        );
        assert_eq!(
            totals
                .non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count,
            21
        );
        assert_eq!(totals.coinbase_transaction_count, 22);
        assert_eq!(totals.transaction_predicate_unavailable_count, 23);
        assert_eq!(days.len(), 1);
        assert_eq!(days[0].totals, Some(totals));
        assert_eq!(
            days[0].first_sapling_or_orchard_transaction_time_unix_seconds,
            Some(1_700_006_401)
        );
        assert_eq!(
            days[0].last_sapling_or_orchard_transaction_time_unix_seconds,
            Some(1_700_092_799)
        );
    }

    #[test]
    fn summary_mapping_omits_day_buckets_for_totals_only_request() {
        let (totals, days) = map_summary(
            ProjectedTransactionComponentSummary {
                totals: derived_totals(),
                days: vec![ProjectedTransactionComponentDay {
                    day_start_unix_seconds: 1_700_006_400,
                    totals: derived_totals(),
                    first_sapling_or_orchard_transaction_time_unix_seconds: None,
                    last_sapling_or_orchard_transaction_time_unix_seconds: None,
                }],
            },
            true,
        );

        assert_eq!(totals.transaction_count, 1);
        assert_eq!(
            days,
            [] as [zinder_proto::v1::explorer::TransactionComponentDay; 0]
        );
    }

    #[test]
    fn invalid_or_empty_time_range_is_rejected() {
        for (start, end) in [(10, 10), (11, 10)] {
            let result = validate_time_range(start, end);
            assert!(result.is_err());
            if let Err(error) = result {
                assert_eq!(error.code(), Code::InvalidArgument);
                assert!(
                    error
                        .message()
                        .contains("start_time_unix_seconds must be less than")
                );
            }
        }
    }

    #[test]
    fn completeness_requires_genesis_through_visible_tip_coverage() {
        assert!(canonical_coverage_reaches_visible_tip(
            coverage(1, 100),
            100
        ));
        assert!(canonical_coverage_reaches_visible_tip(
            coverage(1, 101),
            100
        ));
        assert!(!canonical_coverage_reaches_visible_tip(
            coverage(2, 100),
            100
        ));
        assert!(!canonical_coverage_reaches_visible_tip(
            coverage(1, 99),
            100
        ));
    }
}
