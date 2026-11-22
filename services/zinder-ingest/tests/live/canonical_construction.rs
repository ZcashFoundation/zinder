#![allow(
    missing_docs,
    reason = "Live test names describe the behavior under test."
)]

use std::{
    env::VarError,
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
    time::Instant,
};

use eyre::{Result, eyre};
use tempfile::tempdir;
use zinder_core::{
    BlockHeight, CanonicalBlockFactsDigestVersion, CanonicalBlockFactsSequenceDigestVersion,
    CanonicalBlockReplayFormatVersion, Network, NetworkUpgradeActivations,
    wire::{encode_rpc_block_hash_hex, encode_zinder_native_chain_name},
};
use zinder_ingest::{
    CanonicalConstructionConfig, CanonicalPipelineLimits, load_fresh_canonical_blocks,
};
use zinder_source::NodeSource;
use zinder_store::{
    CanonicalBlockLoadEvidence, CanonicalReorgPolicy, CanonicalStoreBuildPlan, CanonicalStoreError,
    CanonicalStoreWorkload, RocksDbCanonicalBuilder, RocksDbCanonicalStore, RocksDbIoMode,
    RocksDbResourceBudget,
};
use zinder_testkit::live::{LiveTestEnv, init, require_live_for};

use crate::common::{fetch_live_network_upgrade_activations, zebra_source_for_live_env};

mod persisted_wallet_readback;

use persisted_wallet_readback::{PersistedCanonicalEvidence, validate_persisted_wallet_families};

const CANONICAL_BLOCK_COUNT_ENV: &str = "ZINDER_TEST_CANONICAL_BLOCK_COUNT";
const DEFAULT_CANONICAL_BLOCK_COUNT: u32 = 1_000;
const MIB: u64 = 1_024 * 1_024;

#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn canonical_blocks_load_requested_range_from_fixed_checkpoint() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[Network::ZcashTestnet, Network::ZcashMainnet])? else {
        return Ok(());
    };
    let source = zebra_source_for_live_env(&env)?;
    let retained_block_count = requested_canonical_block_count()?;
    let fixed_tip = source.tip_id().await?;
    let checkpoint_height = fixed_tip
        .height
        .value()
        .checked_sub(retained_block_count.get())
        .map(BlockHeight::new)
        .ok_or_else(|| {
            eyre!(
                "canonical block tracer needs a tip at or above {}; got {}",
                retained_block_count.get(),
                fixed_tip.height.value()
            )
        })?;
    let activations = fetch_live_network_upgrade_activations(&env).await?;
    let checkpoint = source
        .fetch_chain_checkpoint(checkpoint_height, &activations)
        .await?;
    let build_plan = CanonicalStoreBuildPlan::checkpointed(
        &activations,
        checkpoint,
        fixed_tip,
        zinder_store::RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(100)?,
    )?;
    let temporary = tempdir()?;
    let store_path = temporary.path().join("canonical");
    let resource_budget = RocksDbResourceBudget::canonical_writer_defaults();
    let config = live_construction_config(&env, activations.clone())?;

    let construction_started_at = Instant::now();
    let builder = RocksDbCanonicalBuilder::create_fresh(
        &store_path,
        CanonicalStoreWorkload::Wallet,
        build_plan.clone(),
        resource_budget,
    )?;
    let outcome = load_fresh_canonical_blocks(builder, &source, &config).await?;
    let elapsed = construction_started_at.elapsed();
    let evidence = outcome.evidence;
    assert_eq!(evidence.block_count, u64::from(retained_block_count.get()));
    let io_mode = outcome.builder.io_mode();
    drop(outcome.builder);
    let persisted = validate_persisted_wallet_families(&store_path, &evidence)?;

    let error = RocksDbCanonicalStore::open_ready(
        &store_path,
        &activations,
        CanonicalStoreWorkload::Wallet,
        zinder_store::RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(100)?,
        resource_budget,
    )
    .err()
    .ok_or_else(|| eyre!("block-local canonical construction must remain BUILDING"))?;
    assert!(matches!(error, CanonicalStoreError::StoreNotReady { .. }));
    assert!(!temporary.path().join("materialized-views").exists());
    assert_and_record_live_evidence(&build_plan, &evidence, &persisted, elapsed, io_mode);
    Ok(())
}

fn requested_canonical_block_count() -> Result<NonZeroU32> {
    match std::env::var(CANONICAL_BLOCK_COUNT_ENV) {
        Ok(encoded_block_count) => encoded_block_count.parse::<NonZeroU32>().map_err(|source| {
            eyre!("invalid {CANONICAL_BLOCK_COUNT_ENV}={encoded_block_count:?}: {source}")
        }),
        Err(VarError::NotPresent) => NonZeroU32::new(DEFAULT_CANONICAL_BLOCK_COUNT)
            .ok_or_else(|| eyre!("default canonical block count must be nonzero")),
        Err(source) => Err(eyre!(
            "could not read {CANONICAL_BLOCK_COUNT_ENV}: {source}"
        )),
    }
}

fn live_construction_config(
    env: &LiveTestEnv,
    activations: Arc<NetworkUpgradeActivations>,
) -> Result<CanonicalConstructionConfig> {
    Ok(CanonicalConstructionConfig {
        request_timeout: env.target.request_timeout,
        pipeline_limits: CanonicalPipelineLimits {
            max_response_bytes: env.target.max_response_bytes,
            source_segment_target_response_bytes: NonZeroU64::new(
                env.target.max_response_bytes.get().min(12 * MIB),
            )
            .ok_or_else(|| eyre!("invalid source response target"))?,
            source_segment_max_blocks: NonZeroU32::new(8)
                .ok_or_else(|| eyre!("invalid source segment bound"))?,
            source_fetch_max_in_flight_requests: NonZeroU32::new(8)
                .ok_or_else(|| eyre!("invalid source request bound"))?,
            source_fetch_max_in_flight_bytes: NonZeroU64::new(
                env.target.max_response_bytes.get().max(64 * MIB),
            )
            .ok_or_else(|| eyre!("invalid source-fetch byte watermark"))?,
            block_prepare_concurrency: NonZeroU32::new(8)
                .ok_or_else(|| eyre!("invalid prepare concurrency"))?,
            block_prepare_memory_watermark_bytes: NonZeroU64::new(128 * MIB)
                .ok_or_else(|| eyre!("invalid prepare byte watermark"))?,
            tree_hashing_workers: NonZeroU32::MIN,
        },
        network_upgrade_activations: activations,
    })
}

#[allow(
    clippy::too_many_lines,
    reason = "the calibration record lists every canonical family measurement explicitly"
)]
fn assert_and_record_live_evidence(
    build_plan: &CanonicalStoreBuildPlan,
    evidence: &CanonicalBlockLoadEvidence,
    persisted: &PersistedCanonicalEvidence,
    elapsed: std::time::Duration,
    io_mode: RocksDbIoMode,
) {
    let elapsed_millis = u64::try_from(elapsed.as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    let blocks_per_second = evidence
        .block_count
        .saturating_mul(1_000)
        .saturating_div(elapsed_millis);
    let logical_mib_per_second = evidence
        .logical_bytes
        .saturating_mul(1_000)
        .saturating_div(elapsed_millis)
        .saturating_div(MIB);
    let fixed_tip = build_plan.build_tip();
    let checkpoint = build_plan.history_predecessor().block_id;

    assert_eq!(
        evidence.first_height,
        build_plan.history_bounds().first_available_height()
    );
    assert_eq!(
        evidence.first_parent_hash,
        build_plan.history_predecessor().block_id.hash
    );
    assert_eq!(evidence.block_header_count, evidence.block_count);
    assert_eq!(evidence.block_hash_index_count, evidence.block_count);
    assert_eq!(evidence.block_replay_count, evidence.block_count);
    assert_eq!(evidence.compact_block_count, evidence.block_count);
    assert_eq!(
        evidence.transaction_location_count,
        evidence.transaction_count
    );
    assert_eq!(evidence.transaction_blob_count, evidence.transaction_count);
    assert_eq!(evidence.block_blob_count, 0);
    assert_eq!(evidence.tip_height, fixed_tip.height);
    assert_eq!(evidence.tip_hash, fixed_tip.hash);
    assert_eq!(
        evidence.replay_format_version,
        CanonicalBlockReplayFormatVersion::V1
    );
    assert_eq!(
        evidence.block_digest_version,
        CanonicalBlockFactsDigestVersion::V1
    );
    assert_eq!(
        evidence.sequence_digest_version,
        CanonicalBlockFactsSequenceDigestVersion::V1
    );
    assert!(evidence.logical_bytes > 0);
    assert!(evidence.sst_file_bytes > 0);
    assert!(evidence.sst_file_count > 0);

    #[allow(
        clippy::print_stderr,
        reason = "calibration test reports range identity and measurements for operator review"
    )]
    {
        eprintln!(
            "canonical_blocks_live_evidence network={} checkpoint_height={} checkpoint_hash={} \
             tip_height={} tip_hash={} block_count={} transaction_count={} \
             block_header_rows={} block_header_logical_bytes={} block_header_sst_bytes={} \
             block_header_sst_files={} block_hash_index_rows={} \
             block_hash_index_logical_bytes={} block_hash_index_sst_bytes={} \
             block_hash_index_sst_files={} block_replay_rows={} block_replay_logical_bytes={} \
             block_replay_sst_bytes={} block_replay_sst_files={} compact_block_rows={} \
             compact_block_logical_bytes={} compact_block_sst_bytes={} compact_block_sst_files={} \
             transaction_location_rows={} transaction_location_logical_bytes={} \
             transaction_location_sst_bytes={} transaction_location_sst_files={} \
             transaction_blob_rows={} transaction_blob_logical_bytes={} transaction_blob_sst_bytes={} \
             transaction_blob_sst_files={} block_blob_rows={} block_blob_logical_bytes={} \
             block_blob_sst_bytes={} block_blob_sst_files={} logical_bytes={} sst_file_bytes={} \
             sst_file_count={} sequence_digest={} tip_sapling_tree_size={} \
             tip_orchard_tree_size={} tip_ironwood_tree_size={} elapsed_milliseconds={} \
             blocks_per_second={} logical_mib_per_second={} io_mode={io_mode:?} \
             persisted_readback_milliseconds={} persisted_readback=metadata_and_boundary_samples \
             build_state=BUILDING ready_admission=REFUSED",
            encode_zinder_native_chain_name(build_plan.network()),
            checkpoint.height.value(),
            encode_rpc_block_hash_hex(checkpoint.hash),
            fixed_tip.height.value(),
            encode_rpc_block_hash_hex(fixed_tip.hash),
            evidence.block_count,
            evidence.transaction_count,
            persisted.block_header.row_count,
            persisted.block_header.prepared_logical_bytes,
            persisted.block_header.sst_file_bytes,
            persisted.block_header.sst_file_count,
            persisted.block_hash_index.row_count,
            persisted.block_hash_index.prepared_logical_bytes,
            persisted.block_hash_index.sst_file_bytes,
            persisted.block_hash_index.sst_file_count,
            persisted.block_replay.row_count,
            persisted.block_replay.prepared_logical_bytes,
            persisted.block_replay.sst_file_bytes,
            persisted.block_replay.sst_file_count,
            persisted.compact_block.row_count,
            persisted.compact_block.prepared_logical_bytes,
            persisted.compact_block.sst_file_bytes,
            persisted.compact_block.sst_file_count,
            persisted.transaction_location.row_count,
            persisted.transaction_location.prepared_logical_bytes,
            persisted.transaction_location.sst_file_bytes,
            persisted.transaction_location.sst_file_count,
            persisted.transaction_blob.row_count,
            persisted.transaction_blob.prepared_logical_bytes,
            persisted.transaction_blob.sst_file_bytes,
            persisted.transaction_blob.sst_file_count,
            persisted.block_blob.row_count,
            persisted.block_blob.prepared_logical_bytes,
            persisted.block_blob.sst_file_bytes,
            persisted.block_blob.sst_file_count,
            evidence.logical_bytes,
            evidence.sst_file_bytes,
            evidence.sst_file_count,
            hex::encode(evidence.sequence_digest.as_bytes()),
            evidence.tip_metadata.sapling_commitment_tree_size,
            evidence.tip_metadata.orchard_commitment_tree_size,
            evidence.tip_metadata.ironwood_commitment_tree_size,
            elapsed_millis,
            blocks_per_second,
            logical_mib_per_second,
            persisted.readback_milliseconds,
        );
    }
}
