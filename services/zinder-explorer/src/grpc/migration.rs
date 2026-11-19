//! `ExplorerQuery.MigrationOverview`, `MigrationCohorts`, and
//! `MigrationDenominations` handlers.
//!
//! All three read the Orchard-to-Ironwood migration facts materialized by
//! `zinder_materialized_views::IronwoodMigrationConsumer` and wrap the result in the
//! cross-cutting `ExplorerFreshness` envelope. `MigrationOverview` pairs the
//! per-migration rows with the cumulative pool-totals records to report the
//! migrated-value total alongside the chain-wide two-sided pool audit;
//! `MigrationCohorts` groups the rows by shared Orchard anchor; and
//! `MigrationDenominations` bins conformant rows by the power-of-ten magnitude
//! of their Ironwood output amount. Grouping and binning run in memory over a
//! bounded range at request time; nothing beyond the consumer's two column
//! families is materialized.

use std::collections::BTreeMap;

use tonic::{Request, Response, Status};
use zinder_core::BlockHeight;
use zinder_materialized_views::{
    IronwoodMigrationConsumer, MaterializedViewStore, Migration, MigrationPoolTotals,
};
use zinder_proto::capabilities::{
    EXPLORER_MIGRATION_COHORTS_V1, EXPLORER_MIGRATION_DENOMINATIONS_V1,
    EXPLORER_MIGRATION_OVERVIEW_V1,
};
use zinder_proto::v1::explorer::{
    MigrationCohort, MigrationCohortsRequest, MigrationCohortsResponse, MigrationDenominationBin,
    MigrationDenominationsRequest, MigrationDenominationsResponse, MigrationOverviewRequest,
    MigrationOverviewResponse,
};
use zinder_proto::v1::wallet::{
    self, VisibleTipBlockRequest, wallet_query_client::WalletQueryClient,
};
use zinder_runtime::AuthenticatedChannel;

use super::error::ExplorerError;
use super::freshness::{
    UpstreamObservationCache, attach_upstream_observation, build_explorer_freshness,
};

/// Hard cap on the block span one cohort or denomination request may cover.
///
/// Bounds a single request's materialized-view scan, mirroring the bounded-page
/// rule the other explorer range reads apply.
const MAX_MIGRATION_BLOCK_SPAN: u32 = 4096;

/// Hard cap on the migration rows one request materializes in memory.
///
/// Migrations are rare coordinated events, so this ceiling sits far above any
/// realistic count; it exists only to bound the response allocation for an
/// unbounded overview range.
const MAX_MIGRATION_ROWS_PER_REQUEST: usize = 65_536;

/// Executes one `ExplorerQuery.MigrationOverview` request.
pub(crate) async fn query_migration_overview(
    materialized_view_store: &MaterializedViewStore,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    upstream_observation_cache: &UpstreamObservationCache,
    request: Request<MigrationOverviewRequest>,
) -> Result<Response<MigrationOverviewResponse>, Status> {
    let inner = request.into_inner();
    if let (Some(start), Some(end)) = (inner.start_height, inner.end_height)
        && end < start
    {
        return Err(ExplorerError::invalid_request("end_height must be >= start_height").into());
    }
    let end_height = match inner.end_height {
        Some(end) => Some(end),
        None => read_latest_pool_totals(materialized_view_store)?.map(|totals| totals.block_height),
    };
    let (aggregate, delta) = match end_height {
        Some(end_height) => {
            let start_height = inner.start_height.unwrap_or(0);
            let migrations = read_migrations(materialized_view_store, start_height, end_height)?;
            let delta = read_range_pool_delta(materialized_view_store, start_height, end_height)?;
            (aggregate_overview(&migrations), delta)
        }
        None => (OverviewAggregate::default(), PoolDelta::default()),
    };

    let chain_epoch = fetch_latest_chain_epoch(wallet_client, None).await?;
    let freshness = attach_upstream_observation(
        upstream_observation_cache,
        build_explorer_freshness(
            Some(materialized_view_store),
            EXPLORER_MIGRATION_OVERVIEW_V1,
            Some(chain_epoch),
            0,
        )?,
    )
    .await;
    Ok(Response::new(MigrationOverviewResponse {
        freshness: Some(freshness),
        total_migrated_ironwood_zat: aggregate.total_migrated_ironwood_zat,
        migration_count: aggregate.migration_count,
        first_height: aggregate.first_height,
        last_height: aggregate.last_height,
        orchard_outflow_zat: delta.orchard_outflow_zat,
        ironwood_inflow_zat: delta.ironwood_inflow_zat,
    }))
}

/// Executes one `ExplorerQuery.MigrationCohorts` request.
pub(crate) async fn query_migration_cohorts(
    materialized_view_store: &MaterializedViewStore,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    upstream_observation_cache: &UpstreamObservationCache,
    request: Request<MigrationCohortsRequest>,
) -> Result<Response<MigrationCohortsResponse>, Status> {
    let inner = request.into_inner();
    validate_span(inner.start_height, inner.end_height)?;
    let migrations = read_migrations(
        materialized_view_store,
        inner.start_height,
        inner.end_height,
    )?;
    let (cohorts, stats) = group_cohorts(&migrations);

    let chain_epoch = fetch_latest_chain_epoch(wallet_client, inner.at_epoch_id).await?;
    let freshness = attach_upstream_observation(
        upstream_observation_cache,
        build_explorer_freshness(
            Some(materialized_view_store),
            EXPLORER_MIGRATION_COHORTS_V1,
            Some(chain_epoch),
            0,
        )?,
    )
    .await;
    Ok(Response::new(MigrationCohortsResponse {
        freshness: Some(freshness),
        cohorts,
        cohort_count: stats.cohorts,
        avg_member_count: stats.avg_members,
        min_member_count: stats.min_members,
        max_member_count: stats.max_members,
    }))
}

/// Executes one `ExplorerQuery.MigrationDenominations` request.
pub(crate) async fn query_migration_denominations(
    materialized_view_store: &MaterializedViewStore,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    upstream_observation_cache: &UpstreamObservationCache,
    request: Request<MigrationDenominationsRequest>,
) -> Result<Response<MigrationDenominationsResponse>, Status> {
    let inner = request.into_inner();
    validate_span(inner.start_height, inner.end_height)?;
    let migrations = read_migrations(
        materialized_view_store,
        inner.start_height,
        inner.end_height,
    )?;
    let (bins, total_tx) = bin_denominations(&migrations);

    let chain_epoch = fetch_latest_chain_epoch(wallet_client, inner.at_epoch_id).await?;
    let freshness = attach_upstream_observation(
        upstream_observation_cache,
        build_explorer_freshness(
            Some(materialized_view_store),
            EXPLORER_MIGRATION_DENOMINATIONS_V1,
            Some(chain_epoch),
            0,
        )?,
    )
    .await;
    Ok(Response::new(MigrationDenominationsResponse {
        freshness: Some(freshness),
        bins,
        total_tx,
    }))
}

/// Migrated-value total and boundary heights over the counted migration rows.
#[derive(Default)]
struct OverviewAggregate {
    total_migrated_ironwood_zat: u64,
    migration_count: u32,
    first_height: Option<u32>,
    last_height: Option<u32>,
}

/// Chain-wide net pool movement over a block range.
#[derive(Default)]
struct PoolDelta {
    orchard_outflow_zat: u64,
    ironwood_inflow_zat: u64,
}

/// Cross-range summary of the grouped cohorts.
#[derive(Default)]
struct CohortStats {
    cohorts: u32,
    avg_members: u32,
    min_members: u32,
    max_members: u32,
}

/// Running tally for one anchor's cohort while grouping.
#[derive(Default)]
struct CohortAccumulator {
    member_count: u32,
    total_migrated_zat: u64,
    conformant_member_count: u32,
}

fn aggregate_overview(migrations: &[Migration]) -> OverviewAggregate {
    let mut total_migrated_ironwood_zat = 0u64;
    let mut migration_count = 0u32;
    for migration in migrations {
        total_migrated_ironwood_zat =
            total_migrated_ironwood_zat.saturating_add(migration.migrated_amount_zat());
        migration_count = migration_count.saturating_add(1);
    }
    OverviewAggregate {
        total_migrated_ironwood_zat,
        migration_count,
        first_height: migrations.first().map(|migration| migration.block_height),
        last_height: migrations.last().map(|migration| migration.block_height),
    }
}

fn group_cohorts(migrations: &[Migration]) -> (Vec<MigrationCohort>, CohortStats) {
    let mut grouped: BTreeMap<[u8; 32], CohortAccumulator> = BTreeMap::new();
    for migration in migrations {
        let accumulator = grouped.entry(migration.orchard_anchor).or_default();
        accumulator.member_count = accumulator.member_count.saturating_add(1);
        accumulator.total_migrated_zat = accumulator
            .total_migrated_zat
            .saturating_add(migration.migrated_amount_zat());
        if migration.conformant {
            accumulator.conformant_member_count =
                accumulator.conformant_member_count.saturating_add(1);
        }
    }

    let mut cohorts = Vec::with_capacity(grouped.len());
    let mut min_member_count: Option<u32> = None;
    let mut max_member_count = 0u32;
    let mut total_members = 0u64;
    for (orchard_anchor, accumulator) in grouped {
        min_member_count = Some(
            min_member_count.map_or(accumulator.member_count, |smallest| {
                smallest.min(accumulator.member_count)
            }),
        );
        max_member_count = max_member_count.max(accumulator.member_count);
        total_members = total_members.saturating_add(u64::from(accumulator.member_count));
        cohorts.push(MigrationCohort {
            orchard_anchor: orchard_anchor.to_vec(),
            member_count: accumulator.member_count,
            total_migrated_zat: accumulator.total_migrated_zat,
            conformant_member_count: accumulator.conformant_member_count,
        });
    }

    let cohort_count = u32::try_from(cohorts.len()).unwrap_or(u32::MAX);
    let avg_member_count = if cohort_count == 0 {
        0
    } else {
        u32::try_from(total_members / u64::from(cohort_count)).unwrap_or(u32::MAX)
    };
    let stats = CohortStats {
        cohorts: cohort_count,
        avg_members: avg_member_count,
        min_members: min_member_count.unwrap_or(0),
        max_members: max_member_count,
    };
    (cohorts, stats)
}

fn bin_denominations(migrations: &[Migration]) -> (Vec<MigrationDenominationBin>, u32) {
    let mut binned: BTreeMap<u64, u32> = BTreeMap::new();
    let mut total_tx = 0u32;
    for migration in migrations {
        if !migration.conformant {
            continue;
        }
        let denomination_zat = denomination_floor(migration.migrated_amount_zat());
        let count = binned.entry(denomination_zat).or_insert(0);
        *count = count.saturating_add(1);
        total_tx = total_tx.saturating_add(1);
    }
    let bins = binned
        .into_iter()
        .map(|(denomination_zat, count)| MigrationDenominationBin {
            denomination_zat,
            count,
        })
        .collect();
    (bins, total_tx)
}

/// Returns the largest power of ten not exceeding `amount_zat`, or zero when
/// `amount_zat` is zero.
fn denomination_floor(amount_zat: u64) -> u64 {
    amount_zat
        .checked_ilog10()
        .and_then(|exponent| 10u64.checked_pow(exponent))
        .unwrap_or(0)
}

fn read_range_pool_delta(
    materialized_view_store: &MaterializedViewStore,
    start_height: u32,
    end_height: u32,
) -> Result<PoolDelta, Status> {
    let end_totals = read_pool_totals_at_or_before(materialized_view_store, end_height)?;
    let baseline = match start_height.checked_sub(1) {
        Some(previous) => read_pool_totals_at_or_before(materialized_view_store, previous)?,
        None => None,
    };
    let end_orchard = end_totals.map_or(0, |totals| totals.cumulative_orchard_value_balance_zat);
    let end_ironwood = end_totals.map_or(0, |totals| totals.cumulative_ironwood_value_balance_zat);
    let base_orchard = baseline.map_or(0, |totals| totals.cumulative_orchard_value_balance_zat);
    let base_ironwood = baseline.map_or(0, |totals| totals.cumulative_ironwood_value_balance_zat);
    Ok(PoolDelta {
        orchard_outflow_zat: saturating_positive_delta(end_orchard, base_orchard),
        ironwood_inflow_zat: saturating_negative_magnitude(end_ironwood, base_ironwood),
    })
}

/// Magnitude of `end - base` when the range's net movement is positive
/// (value left the pool), zero otherwise.
fn saturating_positive_delta(end: i64, base: i64) -> u64 {
    u64::try_from(end.saturating_sub(base)).unwrap_or(0)
}

/// Magnitude of `end - base` when the range's net movement is negative
/// (value entered the pool), zero otherwise.
fn saturating_negative_magnitude(end: i64, base: i64) -> u64 {
    end.saturating_sub(base)
        .checked_neg()
        .and_then(|negated_delta| u64::try_from(negated_delta).ok())
        .unwrap_or(0)
}

fn read_migrations(
    materialized_view_store: &MaterializedViewStore,
    start_height: u32,
    end_height: u32,
) -> Result<Vec<Migration>, Status> {
    IronwoodMigrationConsumer::read_migrations_in_range(
        materialized_view_store,
        BlockHeight::new(start_height),
        BlockHeight::new(end_height),
        MAX_MIGRATION_ROWS_PER_REQUEST,
    )
    .map_err(|error| ExplorerError::internal(error.to_string()).into())
}

fn read_latest_pool_totals(
    materialized_view_store: &MaterializedViewStore,
) -> Result<Option<MigrationPoolTotals>, Status> {
    IronwoodMigrationConsumer::read_latest_pool_totals(materialized_view_store)
        .map_err(|error| ExplorerError::internal(error.to_string()).into())
}

fn read_pool_totals_at_or_before(
    materialized_view_store: &MaterializedViewStore,
    height: u32,
) -> Result<Option<MigrationPoolTotals>, Status> {
    IronwoodMigrationConsumer::read_pool_totals_at_or_before(
        materialized_view_store,
        BlockHeight::new(height),
    )
    .map_err(|error| ExplorerError::internal(error.to_string()).into())
}

fn validate_span(start_height: u32, end_height: u32) -> Result<(), Status> {
    if end_height < start_height {
        return Err(ExplorerError::invalid_request("end_height must be >= start_height").into());
    }
    let span = u64::from(end_height) - u64::from(start_height) + 1;
    if span > u64::from(MAX_MIGRATION_BLOCK_SPAN) {
        return Err(ExplorerError::invalid_request(format!(
            "requested span {span} blocks exceeds the per-request cap of {MAX_MIGRATION_BLOCK_SPAN}",
        ))
        .into());
    }
    Ok(())
}

async fn fetch_latest_chain_epoch(
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    at_epoch_id: Option<u64>,
) -> Result<wallet::ChainEpoch, Status> {
    wallet_client
        .visible_tip_block(Request::new(VisibleTipBlockRequest { at_epoch_id }))
        .await?
        .into_inner()
        .chain_view
        .and_then(|chain_view| chain_view.chain_epoch)
        .ok_or_else(|| {
            ExplorerError::internal("VisibleTipBlockResponse.chain_view.chain_epoch missing").into()
        })
}

#[cfg(test)]
mod tests {
    #![allow(
        missing_docs,
        reason = "Unit test names describe the behavior under test."
    )]

    use super::*;
    use zinder_core::TransactionId;

    fn migration(
        block_height: u32,
        tx_index_in_block: u32,
        anchor_byte: u8,
        migrated_amount_zat: u64,
        conformant: bool,
    ) -> Migration {
        let ironwood_value_balance_zat = -(i64::try_from(migrated_amount_zat).unwrap_or(i64::MAX));
        Migration {
            block_height,
            tx_index_in_block,
            transaction_id: TransactionId::from_bytes([anchor_byte; 32]),
            orchard_value_balance_zat: i64::try_from(migrated_amount_zat).unwrap_or(i64::MAX),
            ironwood_value_balance_zat,
            orchard_anchor: [anchor_byte; 32],
            conformant,
        }
    }

    #[test]
    fn denomination_floor_bins_by_power_of_ten() {
        assert_eq!(denomination_floor(0), 0);
        assert_eq!(denomination_floor(1), 1);
        assert_eq!(denomination_floor(9), 1);
        assert_eq!(denomination_floor(10), 10);
        assert_eq!(denomination_floor(999), 100);
        assert_eq!(denomination_floor(1_000), 1_000);
        assert_eq!(
            denomination_floor(2_100_000_000_000_000),
            1_000_000_000_000_000
        );
    }

    #[test]
    fn aggregate_overview_sums_amounts_and_tracks_boundary_heights() {
        let migrations = vec![
            migration(100, 0, 1, 500, true),
            migration(100, 3, 1, 1_500, false),
            migration(140, 1, 2, 4_000, true),
        ];
        let aggregate = aggregate_overview(&migrations);
        assert_eq!(aggregate.total_migrated_ironwood_zat, 6_000);
        assert_eq!(aggregate.migration_count, 3);
        assert_eq!(aggregate.first_height, Some(100));
        assert_eq!(aggregate.last_height, Some(140));
    }

    #[test]
    fn aggregate_overview_reports_none_boundaries_when_empty() {
        let aggregate = aggregate_overview(&[]);
        assert_eq!(aggregate.total_migrated_ironwood_zat, 0);
        assert_eq!(aggregate.migration_count, 0);
        assert_eq!(aggregate.first_height, None);
        assert_eq!(aggregate.last_height, None);
    }

    #[test]
    fn group_cohorts_groups_by_anchor_and_counts_conformant_members() {
        let migrations = vec![
            migration(100, 0, 1, 1_000, true),
            migration(101, 0, 1, 2_000, false),
            migration(102, 0, 2, 3_000, true),
        ];
        let (cohorts, stats) = group_cohorts(&migrations);
        assert_eq!(cohorts.len(), 2);

        let first = &cohorts[0];
        assert_eq!(first.orchard_anchor, vec![1u8; 32]);
        assert_eq!(first.member_count, 2);
        assert_eq!(first.conformant_member_count, 1);
        assert_eq!(first.total_migrated_zat, 3_000);

        let second = &cohorts[1];
        assert_eq!(second.orchard_anchor, vec![2u8; 32]);
        assert_eq!(second.member_count, 1);
        assert_eq!(second.conformant_member_count, 1);

        assert_eq!(stats.cohorts, 2);
        assert_eq!(stats.min_members, 1);
        assert_eq!(stats.max_members, 2);
        assert_eq!(stats.avg_members, 1);
    }

    #[test]
    fn group_cohorts_reports_zeroed_stats_when_empty() {
        let (cohorts, stats) = group_cohorts(&[]);
        assert_eq!(
            cohorts,
            [] as [zinder_proto::v1::explorer::MigrationCohort; 0]
        );
        assert_eq!(stats.cohorts, 0);
        assert_eq!(stats.avg_members, 0);
        assert_eq!(stats.min_members, 0);
        assert_eq!(stats.max_members, 0);
    }

    #[test]
    fn pool_delta_reports_outflow_only_when_orchard_net_leaves() {
        assert_eq!(saturating_positive_delta(100, 40), 60);
        assert_eq!(saturating_positive_delta(40, 100), 0);
        assert_eq!(saturating_positive_delta(50, 50), 0);
    }

    #[test]
    fn pool_delta_reports_inflow_only_when_ironwood_net_enters() {
        // A migration-heavy range: cumulative Ironwood balance falls (more
        // entered than left), so this must read as inflow, not zero.
        assert_eq!(saturating_negative_magnitude(-500, -100), 400);
        // A range where ordinary Ironwood spends dominate migrations: the
        // cumulative balance rises net, so inflow must clamp to zero rather
        // than reporting that rise as if it were inflow.
        assert_eq!(saturating_negative_magnitude(100, 40), 0);
        assert_eq!(saturating_negative_magnitude(50, 50), 0);
    }

    #[test]
    fn saturating_negative_magnitude_does_not_panic_at_i64_min() {
        assert_eq!(saturating_negative_magnitude(i64::MIN, 0), 0);
    }

    #[test]
    fn bin_denominations_counts_only_conformant_rows() {
        let migrations = vec![
            migration(100, 0, 1, 500, true),
            migration(100, 1, 1, 700, true),
            migration(101, 0, 1, 4_000, true),
            migration(101, 1, 1, 9_000, false),
        ];
        let (bins, total_tx) = bin_denominations(&migrations);
        assert_eq!(total_tx, 3);
        assert_eq!(bins.len(), 2);
        assert_eq!(bins[0].denomination_zat, 100);
        assert_eq!(bins[0].count, 2);
        assert_eq!(bins[1].denomination_zat, 1_000);
        assert_eq!(bins[1].count, 1);
    }
}
