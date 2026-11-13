#![allow(
    missing_docs,
    reason = "Integration test names describe the native protocol contract under test."
)]

use eyre::eyre;
use prost::Message;
use zinder_proto::capabilities::{
    EXPLORER_BLOCK_ACTIVITY_DISTRIBUTION_V1, EXPLORER_BLOCK_PRODUCTION_SERIES_V2,
    EXPLORER_BLOCK_PRODUCTION_TIME_RANGE_V1, EXPLORER_BLOCK_TRANSACTIONS_V2,
    EXPLORER_MEMPOOL_SNAPSHOT_V1, EXPLORER_MIGRATION_COHORTS_V1,
    EXPLORER_MIGRATION_DENOMINATIONS_V1, EXPLORER_MIGRATION_OVERVIEW_V1,
    EXPLORER_NETWORK_UPGRADE_STATUS_V1, EXPLORER_OVERVIEW_SNAPSHOT_V1,
    EXPLORER_TRANSPARENT_ADDRESS_DELTAS_V1,
};
use zinder_proto::v1::{explorer, wallet};

#[test]
fn upstream_tip_round_trips_through_prost() -> eyre::Result<()> {
    let upstream_tip = wallet::UpstreamTip {
        committed_height: Some(2_530_000),
        estimated_height: Some(2_544_375),
    };
    let decoded = round_trip(&upstream_tip)?;

    assert_eq!(decoded.committed_height, Some(2_530_000));
    assert_eq!(decoded.estimated_height, Some(2_544_375));
    Ok(())
}

#[test]
fn upstream_tip_optional_fields_default_to_none() -> eyre::Result<()> {
    let upstream_tip = wallet::UpstreamTip::default();
    let decoded = round_trip(&upstream_tip)?;

    assert!(decoded.committed_height.is_none());
    assert!(decoded.estimated_height.is_none());
    Ok(())
}

#[test]
fn explorer_freshness_carries_chain_view_with_every_axis() -> eyre::Result<()> {
    let freshness = explorer::ExplorerFreshness {
        chain_view: Some(wallet::ChainView {
            chain_epoch: Some(wallet::ChainEpoch::default()),
            indexed_tip: Some(wallet::IndexedTip {
                tip: Some(wallet::BlockTip {
                    height: 2_529_999,
                    hash: "11".repeat(32),
                }),
                block_time_unix_seconds: 1_774_670_000,
            }),
            upstream_tip: Some(wallet::UpstreamTip {
                committed_height: Some(2_530_000),
                estimated_height: Some(2_544_375),
            }),
            materialized_views: Some(wallet::MaterializedViewStatus {
                health: wallet::MaterializedViewHealth::CatchingUp as i32,
                indexed_height: 2_529_999,
                lag_blocks: 1,
                observed_at_millis: 1_774_670_400_000,
            }),
        }),
        snapshot_age_millis: 0,
        capability_version: EXPLORER_OVERVIEW_SNAPSHOT_V1.to_owned(),
        unavailable: Vec::new(),
    };
    let decoded = round_trip(&freshness)?;

    let chain_view = decoded
        .chain_view
        .ok_or_else(|| eyre!("chain_view not set"))?;
    let upstream = chain_view
        .upstream_tip
        .ok_or_else(|| eyre!("upstream_tip not set"))?;
    assert_eq!(upstream.committed_height, Some(2_530_000));
    assert_eq!(upstream.estimated_height, Some(2_544_375));
    let indexed_tip = chain_view
        .indexed_tip
        .ok_or_else(|| eyre!("indexed_tip not set"))?;
    assert_eq!(
        indexed_tip
            .tip
            .ok_or_else(|| eyre!("indexed tip missing"))?
            .height,
        2_529_999
    );
    Ok(())
}

/// An absent `indexed_tip` means "materialized-view head unknown", never "at tip"; the
/// proto3 optional message survives the round trip as `None`.
#[test]
fn explorer_freshness_absent_indexed_tip_means_unknown() -> eyre::Result<()> {
    let freshness = explorer::ExplorerFreshness {
        chain_view: Some(wallet::ChainView {
            chain_epoch: Some(wallet::ChainEpoch::default()),
            indexed_tip: None,
            upstream_tip: None,
            materialized_views: None,
        }),
        snapshot_age_millis: 0,
        capability_version: EXPLORER_OVERVIEW_SNAPSHOT_V1.to_owned(),
        unavailable: Vec::new(),
    };
    let decoded = round_trip(&freshness)?;

    let chain_view = decoded
        .chain_view
        .ok_or_else(|| eyre!("chain_view not set"))?;
    assert!(chain_view.indexed_tip.is_none());
    assert!(chain_view.upstream_tip.is_none());
    Ok(())
}

#[test]
fn block_production_series_round_trips_coverage_header_and_coinbase() -> eyre::Result<()> {
    let response = explorer::BlockProductionSeriesResponse {
        freshness: Some(explorer::ExplorerFreshness {
            capability_version: EXPLORER_BLOCK_PRODUCTION_SERIES_V2.to_owned(),
            ..Default::default()
        }),
        start_height: 99,
        end_height: 100,
        covered_block_count: 1,
        missing_block_count: 1,
        points: vec![explorer::BlockProductionPoint {
            summary: Some(explorer::BlockSummary {
                block_height: 100,
                block_hash: "ab".repeat(32),
                block_time_unix_seconds: 1_774_670_000,
                transaction_count: 3,
                fees_collected_zat: 65_000,
                ..Default::default()
            }),
            bits: 0x1c07_ffff,
            coinbase: Some(explorer::CoinbaseTransactionSummary {
                transaction_id: "cd".repeat(32),
                transparent_outputs: vec![wallet::TransparentOutput {
                    value_zat: 137_500_000,
                    script_pub_key: vec![0x51],
                }],
                has_shielded_outputs: Some(true),
            }),
        }],
    };
    let decoded = round_trip(&response)?;

    assert_eq!(decoded.start_height, 99);
    assert_eq!(decoded.end_height, 100);
    assert_eq!(decoded.covered_block_count, 1);
    assert_eq!(decoded.missing_block_count, 1);
    assert_eq!(decoded.points[0].bits, 0x1c07_ffff);
    let coinbase = decoded.points[0]
        .coinbase
        .as_ref()
        .ok_or_else(|| eyre!("block production point missing coinbase"))?;
    assert_eq!(coinbase.transaction_id, "cd".repeat(32));
    assert_eq!(coinbase.transparent_outputs[0].value_zat, 137_500_000);
    assert_eq!(coinbase.has_shielded_outputs, Some(true));
    assert_eq!(
        decoded.points[0]
            .summary
            .as_ref()
            .ok_or_else(|| eyre!("block production point missing summary"))?
            .fees_collected_zat,
        65_000
    );
    Ok(())
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "One wire-contract test keeps the request and response round-trip assertions together."
)]
fn block_production_time_range_round_trips_request_coverage_and_read_fence() -> eyre::Result<()> {
    let request = explorer::BlockProductionInTimeRangeRequest {
        start_time_unix_seconds: 1_774_670_000,
        end_time_unix_seconds: 1_774_671_800,
        max_entries: 2,
        from_cursor: vec![0x01, 0x02, 0x03],
        at_epoch_id: Some(47),
    };
    let decoded_request = round_trip(&request)?;

    assert_eq!(decoded_request.start_time_unix_seconds, 1_774_670_000);
    assert_eq!(decoded_request.end_time_unix_seconds, 1_774_671_800);
    assert_eq!(decoded_request.max_entries, 2);
    assert_eq!(decoded_request.from_cursor, [0x01, 0x02, 0x03]);
    assert_eq!(decoded_request.at_epoch_id, Some(47));

    let response = explorer::BlockProductionInTimeRangeResponse {
        freshness: Some(explorer::ExplorerFreshness {
            capability_version: EXPLORER_BLOCK_PRODUCTION_TIME_RANGE_V1.to_owned(),
            ..Default::default()
        }),
        points: vec![explorer::BlockProductionPoint {
            summary: Some(explorer::BlockSummary {
                block_height: 2_530_000,
                block_hash: "ab".repeat(32),
                block_time_unix_seconds: 1_774_670_000,
                ..Default::default()
            }),
            bits: 0x1c07_ffff,
            coinbase: None,
        }],
        next_cursor: vec![0x04, 0x05],
        covered_block_count: 1,
        missing_block_count: 2,
        missing_coinbase_count: 1,
        missing_paid_fee_count: 1,
        coverage: Some(explorer::BlockProductionTimeRangeCoverage {
            complete_from_height: Some(1),
            complete_through_height: Some(2_530_000),
            complete_from_time_unix_seconds: Some(1_234),
            complete_through_time_unix_seconds: Some(1_774_670_000),
            requested_range_complete: true,
        }),
        read_fence: Some(explorer::BlockProductionTimeRangeReadFence {
            chain_epoch_id: 47,
            materialized_view_revision: 9,
            materialized_view_tip: Some(wallet::BlockTip {
                height: 2_530_000,
                hash: "cd".repeat(32),
            }),
        }),
    };
    let decoded_response = round_trip(&response)?;

    assert_eq!(decoded_response.points.len(), 1);
    assert_eq!(decoded_response.next_cursor, [0x04, 0x05]);
    assert_eq!(decoded_response.covered_block_count, 1);
    assert_eq!(decoded_response.missing_block_count, 2);
    assert_eq!(decoded_response.missing_coinbase_count, 1);
    assert_eq!(decoded_response.missing_paid_fee_count, 1);
    let coverage = decoded_response
        .coverage
        .ok_or_else(|| eyre!("block-production coverage missing after round-trip"))?;
    assert_eq!(coverage.complete_from_height, Some(1));
    assert_eq!(coverage.complete_through_height, Some(2_530_000));
    assert_eq!(coverage.complete_from_time_unix_seconds, Some(1_234));
    assert_eq!(
        coverage.complete_through_time_unix_seconds,
        Some(1_774_670_000)
    );
    assert!(coverage.requested_range_complete);
    let read_fence = decoded_response
        .read_fence
        .ok_or_else(|| eyre!("block-production read fence missing after round-trip"))?;
    assert_eq!(read_fence.chain_epoch_id, 47);
    assert_eq!(read_fence.materialized_view_revision, 9);
    assert_eq!(
        read_fence
            .materialized_view_tip
            .ok_or_else(|| eyre!(
                "block-production materialized-view tip missing after round-trip"
            ))?
            .height,
        2_530_000
    );
    assert_eq!(
        decoded_response
            .freshness
            .ok_or_else(|| eyre!("freshness envelope missing after round-trip"))?
            .capability_version,
        EXPLORER_BLOCK_PRODUCTION_TIME_RANGE_V1
    );
    Ok(())
}

#[test]
fn transaction_detail_response_embeds_shared_wallet_location() -> eyre::Result<()> {
    let decoded = round_trip(&transaction_detail_response_fixture())?;

    let location = decoded
        .location
        .and_then(|location| location.location)
        .ok_or_else(|| eyre!("location oneof missing"))?;
    let wallet::transaction_location::Location::Mined(mined) = location else {
        return Err(eyre!("expected mined arm"));
    };
    assert_eq!(
        mined
            .location
            .ok_or_else(|| eyre!("mined block location missing"))?
            .block_height,
        99
    );
    let transparent_input = &decoded.transparent_inputs[0];
    assert_eq!(transparent_input.input_index, 0);
    assert_eq!(
        transparent_input
            .spent_outpoint
            .as_ref()
            .ok_or_else(|| eyre!("transparent input missing spent outpoint"))?
            .output_index,
        2
    );
    assert_eq!(transparent_input.value_zat, Some(55_000));
    assert_eq!(
        transparent_input.script_pub_key.as_deref(),
        Some([0x52].as_slice())
    );
    let transparent_output = &decoded.transparent_outputs[0];
    assert_eq!(transparent_output.output_index, 4);
    let output = transparent_output
        .output
        .as_ref()
        .ok_or_else(|| eyre!("transparent output missing intrinsic output"))?;
    assert_eq!(output.value_zat, 34_000);
    assert_eq!(output.script_pub_key, [0x51]);
    assert_eq!(
        transparent_output
            .spent_by
            .as_ref()
            .ok_or_else(|| eyre!("transparent output missing canonical spend"))?
            .spending_transaction_id,
        "11".repeat(32)
    );

    Ok(())
}

fn transaction_detail_response_fixture() -> explorer::TransactionDetailResponse {
    explorer::TransactionDetailResponse {
        freshness: None,
        facts: None,
        location: Some(wallet::TransactionLocation {
            location: Some(wallet::transaction_location::Location::Mined(
                wallet::MinedTransaction {
                    location: Some(wallet::MinedBlockLocation {
                        transaction_id: "ab".repeat(32),
                        block_height: 99,
                        block_hash: "cd".repeat(32),
                        tx_index_in_block: 1,
                    }),
                    chain_context: Some(wallet::MinedTransactionChainContext {
                        consensus_branch_id: 0xc2d6_d0b4,
                        block_time: 1_774_670_000,
                        confirmations: 12,
                    }),
                    raw_transaction_bytes: None,
                },
            )),
        }),
        paid_fee_zat: None,
        prevout_resolution_status: 0,
        transparent_inputs: vec![explorer::TransparentInput {
            input_index: 0,
            spent_outpoint: Some(wallet::OutPoint {
                transaction_id: "ef".repeat(32),
                output_index: 2,
            }),
            value_zat: Some(55_000),
            script_pub_key: Some(vec![0x52]),
        }],
        transparent_outputs: vec![explorer::TransparentOutput {
            output_index: 4,
            output: Some(wallet::TransparentOutput {
                value_zat: 34_000,
                script_pub_key: vec![0x51],
            }),
            spent_by: Some(wallet::TransparentSpend {
                spent_outpoint: Some(wallet::OutPoint {
                    transaction_id: "ab".repeat(32),
                    output_index: 4,
                }),
                spending_transaction_id: "11".repeat(32),
                input_index: 2,
                spending_block: Some(wallet::BlockTip {
                    height: 101,
                    hash: "22".repeat(32),
                }),
            }),
        }],
        intrinsic_value_balances: None,
    }
}

/// A missing canonical fact artifact remains absent from a block row.
///
/// Consumers retain its canonical id and block-local position without treating
/// unavailable facts as a zero-valued transaction.
#[test]
fn block_transactions_response_preserves_optional_facts_and_transparent_rows() -> eyre::Result<()> {
    let decoded = round_trip(&block_transactions_response_fixture())?;

    assert_eq!(decoded.transactions.len(), 2);
    assert_eq!(decoded.transactions[0].transaction_index, 0);
    assert_eq!(
        decoded.transactions[0].transparent_outputs[0].value_zat,
        21_000
    );
    assert_eq!(
        decoded.transactions[0].transparent_outputs[0].script_pub_key,
        [0x51]
    );
    assert!(decoded.transactions[0].public_facts.is_some());
    assert_eq!(decoded.transactions[1].transaction_index, 1);
    assert!(decoded.transactions[1].public_facts.is_none());
    assert!(decoded.transactions[1].transparent_outputs.is_empty());
    assert_eq!(
        decoded.transactions[0].transparent_inputs[0]
            .spent_outpoint
            .as_ref()
            .ok_or_else(|| eyre!("block transaction input missing spent outpoint"))?
            .transaction_id,
        "ef".repeat(32)
    );
    assert_eq!(
        decoded.transactions[0].transparent_inputs[0]
            .spent_outpoint
            .as_ref()
            .ok_or_else(|| eyre!("block transaction input missing spent outpoint"))?
            .output_index,
        7
    );
    assert_eq!(
        decoded.transactions[0].transparent_inputs[0].value_zat,
        Some(55_000)
    );
    assert_eq!(
        decoded.transactions[0].transparent_inputs[0]
            .script_pub_key
            .as_deref(),
        Some([0x52].as_slice())
    );
    assert!(decoded.transactions[1].transparent_inputs.is_empty());
    assert_eq!(
        decoded
            .freshness
            .ok_or_else(|| eyre!("freshness envelope missing"))?
            .capability_version,
        EXPLORER_BLOCK_TRANSACTIONS_V2
    );
    Ok(())
}

fn block_transactions_response_fixture() -> explorer::BlockTransactionsResponse {
    explorer::BlockTransactionsResponse {
        freshness: Some(explorer::ExplorerFreshness {
            capability_version: EXPLORER_BLOCK_TRANSACTIONS_V2.to_owned(),
            ..Default::default()
        }),
        summary: Some(explorer::BlockSummary {
            block_height: 2_530_000,
            transaction_count: 2,
            ..Default::default()
        }),
        transactions: vec![
            explorer::BlockTransaction {
                transaction_index: 0,
                transaction_id: "ab".repeat(32),
                public_facts: Some(explorer::TransactionPublicFacts {
                    transaction_id: "ab".repeat(32),
                    size_bytes: 120,
                    ..Default::default()
                }),
                transparent_outputs: vec![wallet::TransparentOutput {
                    value_zat: 21_000,
                    script_pub_key: vec![0x51],
                }],
                transparent_inputs: vec![explorer::TransparentInput {
                    input_index: 0,
                    spent_outpoint: Some(wallet::OutPoint {
                        transaction_id: "ef".repeat(32),
                        output_index: 7,
                    }),
                    value_zat: Some(55_000),
                    script_pub_key: Some(vec![0x52]),
                }],
            },
            explorer::BlockTransaction {
                transaction_index: 1,
                transaction_id: "cd".repeat(32),
                public_facts: None,
                transparent_outputs: Vec::new(),
                transparent_inputs: Vec::new(),
            },
        ],
        final_note_commitment_roots: None,
    }
}

#[test]
fn ironwood_component_counts_round_trip_through_explorer_messages() -> eyre::Result<()> {
    let counts = explorer::TransactionComponentCounts {
        transparent_input_count: 1,
        transparent_output_count: 2,
        sapling_spend_count: 3,
        sapling_output_count: 4,
        orchard_action_count: 5,
        sprout_joinsplit_count: 6,
        ironwood_action_count: 7,
    };
    let decoded_counts = round_trip(&counts)?;
    assert_eq!(decoded_counts.ironwood_action_count, 7);

    let summary = explorer::BlockSummary {
        ironwood_action_count: 11,
        ..Default::default()
    };
    let decoded_summary = round_trip(&summary)?;
    assert_eq!(decoded_summary.ironwood_action_count, 11);
    Ok(())
}

#[test]
fn mempool_activity_entry_round_trips_component_counts_and_transparent_total() -> eyre::Result<()> {
    let entry = explorer::MempoolActivityEntry {
        transaction_id: "ab".repeat(32),
        component_counts: Some(explorer::TransactionComponentCounts {
            transparent_input_count: 2,
            transparent_output_count: 3,
            sapling_spend_count: 5,
            sapling_output_count: 7,
            orchard_action_count: 11,
            sprout_joinsplit_count: 13,
            ironwood_action_count: 17,
        }),
        transparent_output_total_zat: 1_234_567,
        ..Default::default()
    };
    let decoded = round_trip(&entry)?;
    let counts = decoded
        .component_counts
        .ok_or_else(|| eyre!("component counts missing after round-trip"))?;

    assert_eq!(counts.transparent_input_count, 2);
    assert_eq!(counts.transparent_output_count, 3);
    assert_eq!(counts.sapling_spend_count, 5);
    assert_eq!(counts.sapling_output_count, 7);
    assert_eq!(counts.orchard_action_count, 11);
    assert_eq!(counts.ironwood_action_count, 17);
    assert_eq!(decoded.transparent_output_total_zat, 1_234_567);
    Ok(())
}

#[test]
fn mempool_snapshot_round_trips_one_summary_and_page() -> eyre::Result<()> {
    let response = explorer::MempoolSnapshotResponse {
        freshness: Some(explorer::ExplorerFreshness {
            capability_version: EXPLORER_MEMPOOL_SNAPSHOT_V1.to_owned(),
            ..Default::default()
        }),
        summary: Some(explorer::MempoolSnapshotSummary {
            transaction_count: 2,
            total_size_bytes: 4_096,
            privacy_shape_distribution: vec![explorer::PrivacyShapeCount {
                shape: explorer::PrivacyShape::ShieldedOnly as i32,
                count: 2,
            }],
            version_distribution: vec![explorer::TransactionVersionCount {
                kind: explorer::TransactionVersionKind::V6 as i32,
                count: 2,
            }],
            oldest_entry_age_millis: 200,
            newest_entry_age_millis: 10,
        }),
        entries: vec![explorer::MempoolActivityEntry {
            transaction_id: "ab".repeat(32),
            ..Default::default()
        }],
        next_cursor: vec![0xAA; 12],
    };
    let decoded = round_trip(&response)?;
    let summary = decoded
        .summary
        .ok_or_else(|| eyre!("snapshot summary missing after round-trip"))?;

    assert_eq!(summary.transaction_count, 2);
    assert_eq!(summary.total_size_bytes, 4_096);
    assert_eq!(summary.privacy_shape_distribution[0].count, 2);
    assert_eq!(decoded.entries.len(), 1);
    assert_eq!(decoded.next_cursor, vec![0xAA; 12]);
    Ok(())
}

#[test]
fn block_activity_distribution_round_trips_coverage_and_fixed_buckets() -> eyre::Result<()> {
    let response = explorer::BlockActivityDistributionResponse {
        freshness: Some(explorer::ExplorerFreshness {
            capability_version: EXPLORER_BLOCK_ACTIVITY_DISTRIBUTION_V1.to_owned(),
            ..Default::default()
        }),
        start_height: 100,
        end_height: 200,
        materialized_block_count: 99,
        missing_block_count: 2,
        first_block_time_unix_seconds: Some(1_700_000_000),
        last_block_time_unix_seconds: Some(1_700_001_200),
        transaction_count: 345,
        buckets: vec![explorer::BlockActivityBucket {
            weekday: 0,
            hour: 3,
            transaction_count: 12,
            block_count: 4,
        }],
    };
    let decoded = round_trip(&response)?;

    assert_eq!(decoded.start_height, 100);
    assert_eq!(decoded.end_height, 200);
    assert_eq!(decoded.materialized_block_count, 99);
    assert_eq!(decoded.missing_block_count, 2);
    assert_eq!(decoded.first_block_time_unix_seconds, Some(1_700_000_000));
    assert_eq!(decoded.last_block_time_unix_seconds, Some(1_700_001_200));
    assert_eq!(decoded.transaction_count, 345);
    assert_eq!(decoded.buckets[0].weekday, 0);
    assert_eq!(decoded.buckets[0].hour, 3);
    assert_eq!(decoded.buckets[0].transaction_count, 12);
    assert_eq!(decoded.buckets[0].block_count, 4);
    assert_eq!(
        decoded
            .freshness
            .ok_or_else(|| eyre!("freshness envelope missing"))?
            .capability_version,
        EXPLORER_BLOCK_ACTIVITY_DISTRIBUTION_V1
    );
    Ok(())
}

#[test]
fn transparent_address_deltas_entry_round_trips_signed_values() -> eyre::Result<()> {
    let received = explorer::TransparentAddressDeltasEntry {
        transaction_id: "a".repeat(64),
        block_height: 2_530_000,
        block_time_unix_seconds: 1_700_000_000,
        index: 1,
        value_zat: 5_000_000,
        kind: explorer::TransparentDeltaKind::Received as i32,
    };
    let spent = explorer::TransparentAddressDeltasEntry {
        transaction_id: "b".repeat(64),
        block_height: 2_530_005,
        block_time_unix_seconds: 1_700_000_900,
        index: 0,
        value_zat: -3_000_000,
        kind: explorer::TransparentDeltaKind::Spent as i32,
    };

    let decoded_received = round_trip(&received)?;
    let decoded_spent = round_trip(&spent)?;

    assert_eq!(decoded_received.value_zat, 5_000_000);
    assert_eq!(
        decoded_received.kind,
        explorer::TransparentDeltaKind::Received as i32
    );
    assert_eq!(decoded_spent.value_zat, -3_000_000);
    assert_eq!(
        decoded_spent.kind,
        explorer::TransparentDeltaKind::Spent as i32
    );
    Ok(())
}

#[test]
fn transparent_address_deltas_response_carries_freshness_and_cursor() -> eyre::Result<()> {
    let response = explorer::TransparentAddressDeltasResponse {
        freshness: Some(explorer::ExplorerFreshness {
            capability_version: EXPLORER_TRANSPARENT_ADDRESS_DELTAS_V1.to_owned(),
            ..Default::default()
        }),
        entries: vec![explorer::TransparentAddressDeltasEntry::default()],
        next_cursor: vec![1, 2, 3, 4],
    };
    let decoded = round_trip(&response)?;

    assert_eq!(decoded.entries.len(), 1);
    assert_eq!(decoded.next_cursor, vec![1, 2, 3, 4]);
    assert_eq!(
        decoded
            .freshness
            .ok_or_else(|| eyre!("freshness envelope missing"))?
            .capability_version,
        EXPLORER_TRANSPARENT_ADDRESS_DELTAS_V1
    );
    Ok(())
}

#[test]
fn explorer_server_info_round_trips_materialized_view_identities() -> eyre::Result<()> {
    let server_info = explorer::ExplorerServerInfo {
        common: None,
        vendor: "Zinder".to_owned(),
        materialized_view_status: None,
        materialized_view_identities: vec![
            "block_summary".to_owned(),
            "transparent_outpoint_spend".to_owned(),
        ],
    };
    let decoded = round_trip(&server_info)?;

    assert_eq!(decoded, server_info);
    assert!(
        round_trip(&explorer::ExplorerServerInfo::default())?
            .materialized_view_identities
            .is_empty()
    );
    Ok(())
}

#[test]
fn migration_overview_request_round_trips_absent_bounds() -> eyre::Result<()> {
    let full_history = explorer::MigrationOverviewRequest::default();
    let decoded_full = round_trip(&full_history)?;
    assert!(decoded_full.start_height.is_none());
    assert!(decoded_full.end_height.is_none());

    let bounded = explorer::MigrationOverviewRequest {
        start_height: Some(2_500_000),
        end_height: Some(2_530_000),
    };
    let decoded_bounded = round_trip(&bounded)?;
    assert_eq!(decoded_bounded.start_height, Some(2_500_000));
    assert_eq!(decoded_bounded.end_height, Some(2_530_000));
    Ok(())
}

#[test]
fn migration_overview_response_round_trips_the_two_sided_audit() -> eyre::Result<()> {
    let response = explorer::MigrationOverviewResponse {
        freshness: Some(explorer::ExplorerFreshness {
            capability_version: EXPLORER_MIGRATION_OVERVIEW_V1.to_owned(),
            ..Default::default()
        }),
        total_migrated_ironwood_zat: 4_200_000_000,
        migration_count: 17,
        first_height: Some(2_500_010),
        last_height: Some(2_529_990),
        orchard_outflow_zat: 4_200_010_000,
        ironwood_inflow_zat: 4_200_000_000,
    };
    let decoded = round_trip(&response)?;

    assert_eq!(decoded.total_migrated_ironwood_zat, 4_200_000_000);
    assert_eq!(decoded.migration_count, 17);
    assert_eq!(decoded.first_height, Some(2_500_010));
    assert_eq!(decoded.last_height, Some(2_529_990));
    assert_eq!(decoded.orchard_outflow_zat, 4_200_010_000);
    assert_eq!(decoded.ironwood_inflow_zat, 4_200_000_000);
    assert_eq!(
        decoded
            .freshness
            .ok_or_else(|| eyre!("freshness envelope missing"))?
            .capability_version,
        EXPLORER_MIGRATION_OVERVIEW_V1
    );
    Ok(())
}

#[test]
fn migration_cohorts_response_round_trips_anchor_and_summary() -> eyre::Result<()> {
    let anchor = vec![0xab; 32];
    let response = explorer::MigrationCohortsResponse {
        freshness: Some(explorer::ExplorerFreshness {
            capability_version: EXPLORER_MIGRATION_COHORTS_V1.to_owned(),
            ..Default::default()
        }),
        cohorts: vec![explorer::MigrationCohort {
            orchard_anchor: anchor.clone(),
            member_count: 9,
            total_migrated_zat: 1_800_000_000,
            conformant_member_count: 7,
        }],
        cohort_count: 1,
        avg_member_count: 9,
        min_member_count: 9,
        max_member_count: 9,
    };
    let decoded = round_trip(&response)?;

    let cohort = decoded
        .cohorts
        .first()
        .ok_or_else(|| eyre!("cohort missing after round-trip"))?;
    assert_eq!(cohort.orchard_anchor, anchor);
    assert_eq!(cohort.member_count, 9);
    assert_eq!(cohort.conformant_member_count, 7);
    assert_eq!(cohort.total_migrated_zat, 1_800_000_000);
    assert_eq!(decoded.cohort_count, 1);
    assert_eq!(decoded.max_member_count, 9);
    assert_eq!(
        decoded
            .freshness
            .ok_or_else(|| eyre!("freshness envelope missing"))?
            .capability_version,
        EXPLORER_MIGRATION_COHORTS_V1
    );
    Ok(())
}

#[test]
fn migration_denominations_response_round_trips_bins() -> eyre::Result<()> {
    let response = explorer::MigrationDenominationsResponse {
        freshness: Some(explorer::ExplorerFreshness {
            capability_version: EXPLORER_MIGRATION_DENOMINATIONS_V1.to_owned(),
            ..Default::default()
        }),
        bins: vec![
            explorer::MigrationDenominationBin {
                denomination_zat: 100_000_000,
                count: 5,
            },
            explorer::MigrationDenominationBin {
                denomination_zat: 1_000_000_000,
                count: 2,
            },
        ],
        total_tx: 7,
    };
    let decoded = round_trip(&response)?;

    assert_eq!(decoded.bins.len(), 2);
    assert_eq!(decoded.total_tx, 7);
    assert_eq!(
        decoded
            .bins
            .first()
            .ok_or_else(|| eyre!("first bin missing"))?
            .denomination_zat,
        100_000_000
    );
    assert_eq!(
        decoded
            .freshness
            .ok_or_else(|| eyre!("freshness envelope missing"))?
            .capability_version,
        EXPLORER_MIGRATION_DENOMINATIONS_V1
    );
    Ok(())
}

#[test]
fn network_upgrade_status_response_round_trips_entries_and_active_pointer() -> eyre::Result<()> {
    let response = explorer::NetworkUpgradeStatusResponse {
        freshness: Some(explorer::ExplorerFreshness {
            capability_version: EXPLORER_NETWORK_UPGRADE_STATUS_V1.to_owned(),
            ..Default::default()
        }),
        tip_height: 2_530_000,
        upgrades: vec![
            explorer::NetworkUpgradeEntry {
                name: "NU5".to_owned(),
                branch_id_hex: "c2d6d0b4".to_owned(),
                activation_height: 1_842_420,
                active: true,
            },
            explorer::NetworkUpgradeEntry {
                name: "NU6.3".to_owned(),
                branch_id_hex: "37a5165b".to_owned(),
                activation_height: 3_000_000,
                active: false,
            },
        ],
        active_upgrade_name: "NU5".to_owned(),
        active_upgrade_branch_id_hex: "c2d6d0b4".to_owned(),
    };
    let decoded = round_trip(&response)?;

    assert_eq!(decoded.tip_height, 2_530_000);
    assert_eq!(decoded.upgrades.len(), 2);
    let pending = decoded
        .upgrades
        .get(1)
        .ok_or_else(|| eyre!("pending upgrade entry missing"))?;
    assert_eq!(pending.name, "NU6.3");
    assert_eq!(pending.branch_id_hex, "37a5165b");
    assert_eq!(pending.activation_height, 3_000_000);
    assert!(!pending.active);
    assert_eq!(decoded.active_upgrade_name, "NU5");
    assert_eq!(decoded.active_upgrade_branch_id_hex, "c2d6d0b4");
    assert_eq!(
        decoded
            .freshness
            .ok_or_else(|| eyre!("freshness envelope missing"))?
            .capability_version,
        EXPLORER_NETWORK_UPGRADE_STATUS_V1
    );
    Ok(())
}

fn round_trip<MessageType>(message: &MessageType) -> Result<MessageType, prost::DecodeError>
where
    MessageType: Message + Default,
{
    let encoded = message.encode_to_vec();
    MessageType::decode(encoded.as_slice())
}
