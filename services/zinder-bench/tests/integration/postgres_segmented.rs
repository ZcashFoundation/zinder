#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{collections::BTreeMap, num::NonZeroU32, path::Path, sync::Arc};

use eyre::{Result, eyre};
use serde_json::Value;
use tempfile::{TempDir, tempdir};
use zebra_chain::{
    block::Block,
    serialization::{ZcashDeserialize, ZcashSerialize},
    transaction::{self, Transaction},
};
use zinder_bench::{
    canonical_replay_storage::rocksdb::{
        RocksDbCanonicalReplayConfig, run_rocksdb_canonical_replay_storage,
    },
    capture::measure_fixture_blocks,
    fixture::{
        FIXTURE_CONTRACT_IDENTITY, FIXTURE_FORMAT_VERSION, FixtureManifest, FixtureNodeSource,
        SubtreeRootSet, write_segment,
    },
    postgres_segmented::{
        PostgresSegmentedConfig,
        database::{close_postgres, connect_postgres},
        loader::{
            BulkStreamConfig, PendingSealTransition, TipCommitPayload, WriterFenceHandle,
            acquire_post_finalize_writer_term, execute_streaming_bulk_load,
            persist_sealed_tail_transition, persist_tip_block_in_txn,
            prepare_block_range_from_source,
        },
        reader::{
            ReaderConnectionMode, ValidationExpectations, validate_persisted_state_read_only,
        },
        run_postgres_segmented_benchmark,
        schema::{BENCHMARK_LEASE_DURATION_MS, MAX_SETTLED_SEGMENT_BYTES},
        segment_builder::seal_settled_segments,
        tip_follow::{TipAdvanceTarget, TipFollowParameters, TipFollowState},
        wallet_projection::ProvisionalWalletProjection,
    },
    report::{
        FixtureSummary, PostgresSegmentedBaselineInput, PostgresSegmentedReportInputs,
        PostgresSegmentedRuntimeEvidence, build_postgres_segmented_storage_report,
    },
    rss::peak_rss,
};
use zinder_core::{
    BlockHeight, Network, NetworkUpgradeActivations, UnixTimestampMillis,
    wire::encode_zinder_native_chain_name,
};
use zinder_source::SourceBlock;
use zinder_store::RocksDbResourceBudget;
use zinder_testkit::{
    P2pkhSpendArgs, TRANSPARENT_BROADCAST_TEST_SEED, TransparentTestKey,
    ZIP317_FEE_ONE_IN_ONE_OUT_ZATS, sample_regtest_upgrade_activations,
};

use crate::common::regtest_activation_records;

const DATABASE_URL_ENV: &str = "ZINDER_TEST_POSTGRES_DATABASE_URL";
const READER_DATABASE_URL_ENV: &str = "ZINDER_TEST_POSTGRES_READER_DATABASE_URL";

const REGTEST_BLOCK_1: &str =
    include_str!("../../../zinder-ingest/tests/fixtures/z3-regtest-block-1.json");
const SERIALIZED_HEADER_PARENT_OFFSET: usize = 4;
const SERIALIZED_HEADER_PARENT_END: usize = SERIALIZED_HEADER_PARENT_OFFSET + 32;
const COINBASE_HEIGHT_OPCODE_OFFSET: usize = 1_534;
const SPEND_OUTPUT_VALUE_ZATS: u64 = 12_345;
const SPEND_TARGET_HEIGHT: u32 = 120;

fn write_segmented_regtest_fixture(block_count: u32) -> Result<TempDir> {
    let fixture: Value = serde_json::from_str(REGTEST_BLOCK_1)?;
    let raw_block_bytes = hex::decode(
        fixture
            .get("raw_block_hex")
            .and_then(Value::as_str)
            .ok_or_else(|| eyre!("fixture raw_block_hex must be a string"))?,
    )?;
    let mut parent_hash = Network::ZcashRegtest.genesis_hash();
    let mut blocks_by_height = BTreeMap::new();
    let mut ordered_blocks = Vec::with_capacity(usize::try_from(block_count)?);

    for height_num in 1..=block_count {
        let mut block_bytes = raw_block_bytes.clone();
        block_bytes[SERIALIZED_HEADER_PARENT_OFFSET..SERIALIZED_HEADER_PARENT_END]
            .copy_from_slice(&parent_hash.as_bytes());
        block_bytes[COINBASE_HEIGHT_OPCODE_OFFSET] =
            0x50_u8.saturating_add(u8::try_from(height_num)?);
        if height_num == 4 || height_num == 9 {
            let below = blocks_by_height
                .get(&BlockHeight::new(height_num - 1))
                .ok_or_else(|| eyre!("missing prior block for coinbase spend"))?;
            block_bytes = with_coinbase_spends(&block_bytes, below)?;
        }
        let height = BlockHeight::new(height_num);
        let block = SourceBlock::from_raw_block_bytes(Network::ZcashRegtest, height, block_bytes)?;
        parent_hash = block.hash;
        blocks_by_height.insert(height, block.clone());
        ordered_blocks.push(block);
    }

    let fixture_dir = tempdir()?;
    let activations = sample_regtest_upgrade_activations();
    write_fixture_dir(fixture_dir.path(), &ordered_blocks, &activations)?;
    Ok(fixture_dir)
}

fn with_coinbase_spends(block_bytes: &[u8], below: &SourceBlock) -> Result<Vec<u8>> {
    let coinbase = Block::zcash_deserialize(below.raw_block_bytes.as_slice())?
        .transactions
        .first()
        .ok_or_else(|| eyre!("a block must hold a coinbase"))?
        .hash();
    let spend = p2pkh_spend(
        coinbase,
        SPEND_OUTPUT_VALUE_ZATS + ZIP317_FEE_ONE_IN_ONE_OUT_ZATS,
    )?;
    let descendant = p2pkh_spend(spend.hash(), SPEND_OUTPUT_VALUE_ZATS)?;
    let mut block = Block::zcash_deserialize(block_bytes)?;
    block
        .transactions
        .extend([Arc::new(spend), Arc::new(descendant)]);
    Ok(block.zcash_serialize_to_vec()?)
}

fn p2pkh_spend(spent: transaction::Hash, spent_value_zats: u64) -> Result<Transaction> {
    let key = TransparentTestKey::from_seed(&TRANSPARENT_BROADCAST_TEST_SEED)?;
    let recipient = key.scratch_recipient_address(1);
    let bytes = key.build_p2pkh_spend(&P2pkhSpendArgs {
        coinbase_txid_be: spent.0,
        coinbase_vout: 0,
        coinbase_value_zats: spent_value_zats,
        recipient: &recipient,
        target_height: SPEND_TARGET_HEIGHT,
    })?;
    Ok(Transaction::zcash_deserialize(bytes.as_slice())?)
}

fn write_fixture_dir(
    directory: &Path,
    blocks: &[SourceBlock],
    activations: &NetworkUpgradeActivations,
) -> Result<()> {
    let first = blocks.first().ok_or_else(|| eyre!("empty blocks"))?;
    let last = blocks.last().ok_or_else(|| eyre!("empty blocks"))?;
    let descriptor = write_segment(directory, 0, blocks)?;
    let measurements = measure_fixture_blocks(blocks, activations)?;
    FixtureManifest {
        contract_identity: FIXTURE_CONTRACT_IDENTITY.to_owned(),
        fixture_format_version: FIXTURE_FORMAT_VERSION,
        network: encode_zinder_native_chain_name(Network::ZcashRegtest).to_owned(),
        from_height: first.height.value(),
        to_height: last.height.value(),
        block_count: u32::try_from(blocks.len())?,
        workload_density: measurements.workload_density,
        canonical_artifact_schema_version: zinder_store::CURRENT_ARTIFACT_SCHEMA_VERSION.value(),
        canonical_block_facts_digest_evidence: measurements
            .canonical_block_facts_digest_evidence()?,
        tip_hash_hex: hex::encode(last.hash.as_bytes()),
        network_upgrade_activations: regtest_activation_records(),
        segments: vec![descriptor],
        subtree_roots: SubtreeRootSet::default(),
    }
    .write(directory)?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a fresh disposable PostgreSQL database"]
async fn postgres_segmented_matches_rocksdb_digest_and_rejects_schema_reuse() -> Result<()> {
    let database_url = std::env::var(DATABASE_URL_ENV)
        .map_err(|_| eyre!("set {DATABASE_URL_ENV} to a fresh disposable database"))?;
    let reader_url = std::env::var(READER_DATABASE_URL_ENV).ok();
    let reader_mode = if reader_url.is_some() {
        ReaderConnectionMode::PgbouncerTransaction
    } else {
        ReaderConnectionMode::Direct
    };

    let fixture_dir = write_segmented_regtest_fixture(12)?;
    let rocksdb_store_parent = tempdir()?;
    let rocksdb_candidate = rocksdb_store_parent.path().join("rocksdb-oracle");
    let concurrency = NonZeroU32::new(2).ok_or_else(|| eyre!("2 must be non-zero"))?;

    let rocksdb_run = run_rocksdb_canonical_replay_storage(RocksDbCanonicalReplayConfig {
        fixture_directory: fixture_dir.path().to_path_buf(),
        candidate_path: rocksdb_candidate,
        block_prepare_concurrency: concurrency,
        rocksdb_resource_budget: RocksDbResourceBudget::canonical_writer_defaults(),
    })
    .await?;
    let rocksdb_digest_hex = hex::encode(rocksdb_run.validation.sequence_digest.as_bytes());

    let schema_name = "zinder_bench_seg_compact".to_owned();
    let config = PostgresSegmentedConfig {
        fixture_directory: fixture_dir.path().to_path_buf(),
        database_url: database_url.clone(),
        reader_database_url: reader_url.clone(),
        schema_name: schema_name.clone(),
        reader_mode,
        full_blocks: false,
        max_segment_bytes: MAX_SETTLED_SEGMENT_BYTES,
        settled_segment_blocks: 3,
        live_tail_max_blocks: 3,
        tip_follow_blocks: Some(6),
        concurrent_readers: 4,
        mempool_churn_txs_per_block: 4,
    };

    let run_started = UnixTimestampMillis::now().value();
    let seg_run = run_postgres_segmented_benchmark(config.clone()).await?;
    let run_completed = UnixTimestampMillis::now().value();

    assert_compact_segmented_run(&seg_run, &rocksdb_digest_hex);
    verify_compact_report_and_schema_reuse(CompactVerificationContext {
        fixture_path: fixture_dir.path(),
        config,
        seg_run,
        rocksdb_seconds: rocksdb_run.timings.wall_clock_seconds,
        rocksdb_bytes: rocksdb_run.physical_storage_bytes,
        run_started,
        run_completed,
    })
    .await
}

struct CompactVerificationContext<'a> {
    fixture_path: &'a Path,
    config: PostgresSegmentedConfig,
    seg_run: zinder_bench::postgres_segmented::PostgresSegmentedRunReport,
    rocksdb_seconds: f64,
    rocksdb_bytes: u64,
    run_started: u64,
    run_completed: u64,
}

fn assert_compact_segmented_run(
    seg_run: &zinder_bench::postgres_segmented::PostgresSegmentedRunReport,
    rocksdb_digest_hex: &str,
) {
    assert_eq!(seg_run.persisted_sequence_digest_hex, rocksdb_digest_hex);
    assert_eq!(seg_run.total_block_count, 12);
    assert_eq!(seg_run.bulk_block_count, 6);
    assert_eq!(seg_run.tip_follow_block_count, 6);
    assert_eq!(
        seg_run.settled_blocks_verified + seg_run.live_tail_blocks_verified,
        12
    );
    assert!(seg_run.tip_follow.tail_seal_transitions >= 1);
    assert!(seg_run.tip_follow.mempool_admits > 0);
    assert!(seg_run.tip_follow.mempool_evictions > 0);
    assert!(seg_run.reader_workload.completed_transactions >= 4);
    assert!(seg_run.reader_workload.per_reader_blocks_per_second_mean > 0.0);
    assert!(seg_run.reader_workload.per_reader_bytes_per_second_mean > 0.0);
    assert!(seg_run.footprint.live_tail_rows <= 3);
    assert!(seg_run.footprint.settled_shielded_segments >= 2);
    assert!(seg_run.footprint.settled_supplement_segments >= 2);
    assert_eq!(seg_run.footprint.settled_raw_block_segments, 0);
    assert!(seg_run.footprint.wallet_storage_bytes > 0);
    assert!(seg_run.footprint.bulk_wal_bytes > 0);
    assert!(seg_run.footprint.tip_follow_wal_bytes > 0);
    assert!(seg_run.timings.schema_create_seconds > 0.0);
    assert!(seg_run.timings.wallet_projection_seconds >= 0.0);
    assert_eq!(
        seg_run.footprint.total_storage_bytes,
        seg_run.footprint.canonical_storage_bytes + seg_run.footprint.wallet_storage_bytes
    );
}

async fn verify_compact_report_and_schema_reuse(ctx: CompactVerificationContext<'_>) -> Result<()> {
    let manifest = FixtureManifest::read(ctx.fixture_path)?;
    let fixture_summary = FixtureSummary::try_from(&manifest)?;
    let report = build_postgres_segmented_storage_report(
        fixture_summary,
        PostgresSegmentedReportInputs {
            run: ctx.seg_run,
            baseline: Some(PostgresSegmentedBaselineInput {
                build_seconds: Some(ctx.rocksdb_seconds),
                storage_bytes: Some(ctx.rocksdb_bytes),
                streaming_blocks_per_second: None,
            }),
            runtime_evidence: PostgresSegmentedRuntimeEvidence {
                database_image_reference: None,
                pooler_image_reference: None,
                client_cpu_limit_cores: None,
                client_memory_limit_bytes: None,
                database_cpu_limit_cores: None,
                database_memory_limit_bytes: None,
                pooler_cpu_limit_cores: None,
                pooler_memory_limit_bytes: None,
            },
            benchmark_client_peak_rss: peak_rss(),
            software_revision: None,
            trial_id: None,
            fixture_cache_policy: None,
            run_started_at_unix_millis: ctx.run_started,
            run_completed_at_unix_millis: ctx.run_completed,
            runner_id: None,
            cpu_limit_cores: None,
            memory_limit_bytes: None,
            storage_class: None,
            image_reference: None,
        },
    );
    report.validate()?;
    let seg_report = report
        .postgres_segmented_storage()
        .ok_or_else(|| eyre!("expected segmented report variant"))?;
    assert_eq!(
        seg_report
            .adr0004_gate_evaluation
            .tip_commit_latency_under_2s,
        Some(true)
    );

    let database_url = ctx.config.database_url.clone();
    let reader_url = ctx.config.reader_database_url.clone();
    verify_tampered_live_segment_kind_rejected(&database_url, &ctx.config.schema_name, seg_report)
        .await?;

    let Some(reuse_err) = run_postgres_segmented_benchmark(ctx.config).await.err() else {
        return Err(eyre!("existing schema must reject rerun"));
    };
    let reuse_msg = reuse_err.to_string();
    assert!(reuse_msg.contains("already exists"));
    assert!(!reuse_msg.contains(database_url.as_str()));
    if let Some(ref r_url) = reader_url {
        assert!(!reuse_msg.contains(r_url.as_str()));
    }
    Ok(())
}

async fn verify_tampered_live_segment_kind_rejected(
    database_url: &str,
    schema_name: &str,
    seg_report: &zinder_bench::report::PostgresSegmentedStorageReport,
) -> Result<()> {
    let conn = connect_postgres(
        database_url,
        "tampering live transaction_location segment_kind",
    )
    .await?;
    let counts = conn
        .client
        .query_one(
            &format!(
                "SELECT
                    (SELECT COUNT(*)::BIGINT FROM {schema_name}.transaction_location),
                    (SELECT COUNT(*)::BIGINT FROM {schema_name}.block_hash_index),
                    (SELECT COUNT(*)::BIGINT FROM {schema_name}.tree_state_checkpoint),
                    (SELECT COUNT(*)::BIGINT FROM {schema_name}.subtree_root),
                    (SELECT COUNT(*)::BIGINT FROM {schema_name}.wallet_utxo),
                    (SELECT COUNT(*)::BIGINT FROM {schema_name}.wallet_address_tx)"
            ),
            &[],
        )
        .await?;
    let updated = conn
        .client
        .execute(
            &format!(
                "UPDATE {schema_name}.transaction_location SET segment_kind = 1 WHERE segment_kind = 0"
            ),
            &[],
        )
        .await?;
    assert!(updated > 0, "expected live tail transaction_location rows");
    let tip_hash = zinder_core::wire::decode_internal_block_hash(
        hex::decode(&seg_report.validation.tip_hash_hex)?.as_slice(),
    )?;
    let expectations = ValidationExpectations {
        schema_name: schema_name.to_owned(),
        network: zinder_core::Network::ZcashRegtest,
        full_blocks: seg_report.configuration.full_blocks,
        max_segment_bytes: seg_report.configuration.max_segment_bytes,
        live_tail_max_blocks: seg_report.configuration.live_tail_max_blocks,
        expected_first_height: BlockHeight::new(seg_report.validation.first_height),
        expected_tip_height: BlockHeight::new(seg_report.validation.tip_height),
        expected_tip_hash: tip_hash,
        expected_sequence_digest_hex: seg_report.validation.persisted_sequence_digest_hex.clone(),
        expected_transaction_location_rows: u64::try_from(counts.get::<_, i64>(0))?,
        expected_block_hash_index_rows: u64::try_from(counts.get::<_, i64>(1))?,
        expected_tree_state_checkpoint_rows: u64::try_from(counts.get::<_, i64>(2))?,
        expected_subtree_root_rows: u64::try_from(counts.get::<_, i64>(3))?,
        expected_wallet_utxo_rows: u64::try_from(counts.get::<_, i64>(4))?,
        expected_wallet_address_tx_rows: u64::try_from(counts.get::<_, i64>(5))?,
    };
    let err = validate_persisted_state_read_only(&conn.client, &expectations)
        .await
        .err();
    close_postgres(conn).await?;
    let err = err.ok_or_else(|| eyre!("tampered live segment_kind must be rejected"))?;
    assert!(
        err.to_string()
            .contains("transaction_location segment_kind mismatch"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a fresh disposable PostgreSQL database"]
async fn postgres_segmented_full_blocks_verifies_raw_block_segments() -> Result<()> {
    let database_url = std::env::var(DATABASE_URL_ENV)
        .map_err(|_| eyre!("set {DATABASE_URL_ENV} to a fresh disposable database"))?;
    let fixture_dir = write_segmented_regtest_fixture(10)?;

    let run_report = run_postgres_segmented_benchmark(PostgresSegmentedConfig {
        fixture_directory: fixture_dir.path().to_path_buf(),
        database_url,
        reader_database_url: None,
        schema_name: "zinder_bench_seg_full".to_owned(),
        reader_mode: ReaderConnectionMode::Direct,
        full_blocks: true,
        max_segment_bytes: MAX_SETTLED_SEGMENT_BYTES,
        settled_segment_blocks: 3,
        live_tail_max_blocks: 2,
        tip_follow_blocks: Some(4),
        concurrent_readers: 2,
        mempool_churn_txs_per_block: 2,
    })
    .await?;

    assert!(run_report.raw_blocks_verified > 0);
    assert!(run_report.footprint.settled_raw_block_segments >= 1);
    assert!(run_report.footprint.live_tail_rows <= 2);
    assert!(run_report.footprint.max_segment_byte_len <= MAX_SETTLED_SEGMENT_BYTES);

    let manifest = FixtureManifest::read(fixture_dir.path())?;
    let report = build_postgres_segmented_storage_report(
        FixtureSummary::try_from(&manifest)?,
        PostgresSegmentedReportInputs {
            run: run_report,
            baseline: None,
            runtime_evidence: PostgresSegmentedRuntimeEvidence {
                database_image_reference: None,
                pooler_image_reference: None,
                client_cpu_limit_cores: None,
                client_memory_limit_bytes: None,
                database_cpu_limit_cores: None,
                database_memory_limit_bytes: None,
                pooler_cpu_limit_cores: None,
                pooler_memory_limit_bytes: None,
            },
            benchmark_client_peak_rss: peak_rss(),
            software_revision: None,
            trial_id: None,
            fixture_cache_policy: None,
            run_started_at_unix_millis: UnixTimestampMillis::now().value(),
            run_completed_at_unix_millis: UnixTimestampMillis::now().value(),
            runner_id: None,
            cpu_limit_cores: None,
            memory_limit_bytes: None,
            storage_class: None,
            image_reference: None,
        },
    );
    report.validate()?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a fresh disposable PostgreSQL database"]
async fn postgres_segmented_renews_lease_after_long_build_and_fences_settlement() -> Result<()> {
    let database_url = std::env::var(DATABASE_URL_ENV)
        .map_err(|_| eyre!("set {DATABASE_URL_ENV} to a fresh disposable database"))?;
    let fixture_dir = write_segmented_regtest_fixture(10)?;
    let manifest = FixtureManifest::read(fixture_dir.path())?;
    let manifest_sha256 = manifest.digest_sha256()?;
    let source = FixtureNodeSource::open(fixture_dir.path(), &manifest)?;
    let schema = "zinder_bench_seg_fence";

    let mut conn = connect_postgres(&database_url, "connect fence regression test").await?;
    let mut wallet_projection = ProvisionalWalletProjection::new();
    let mut bulk = execute_streaming_bulk_load(
        &mut conn.client,
        &source,
        &BulkStreamConfig {
            manifest: &manifest,
            manifest_sha256: &manifest_sha256,
            schema_name: schema,
            full_blocks: false,
            max_segment_bytes: MAX_SETTLED_SEGMENT_BYTES,
            settled_segment_blocks: 3,
            live_tail_max_blocks: 3,
            bulk_block_count: 6,
            seed_lease_duration_ms: 1,
            post_finalize_lease_duration_ms: BENCHMARK_LEASE_DURATION_MS,
        },
        &mut wallet_projection,
    )
    .await?;
    assert_eq!(bulk.writer_epoch, 2);

    assert_initial_segment_kind_classification(&conn.client, schema).await?;

    let fence = WriterFenceHandle {
        writer_epoch: bulk.writer_epoch,
        lease_duration_ms: BENCHMARK_LEASE_DURATION_MS,
    };
    let tail_for_seal_test = bulk.initial_tail[..2].to_vec();
    let mut tip_state = TipFollowState::new(
        TipFollowParameters {
            network: Network::ZcashRegtest,
            schema_name: schema.to_owned(),
            fence,
            first_height: BlockHeight::new(1),
            full_blocks: false,
            max_segment_bytes: MAX_SETTLED_SEGMENT_BYTES,
            settled_segment_blocks: 3,
            live_tail_max_blocks: 3,
            mempool_churn_txs_per_block: 1,
        },
        std::mem::take(&mut bulk.initial_tail),
        bulk.initial_max_segment_bytes,
    );
    let mut target = TipAdvanceTarget {
        source: &source,
        prepare_state: &mut bulk.prepare_state,
        wallet_projection: &mut wallet_projection,
    };
    let latencies = tip_state
        .advance_height_range(&mut conn.client, &mut target, Some((7, 8)))
        .await?;
    assert_eq!(latencies.sample_count, 2);

    verify_expired_tip_rejection(&mut conn.client, schema, fence, &mut target).await?;
    verify_settlement_fence_rejections(&mut conn.client, schema, fence, &tail_for_seal_test)
        .await?;
    close_postgres(conn).await?;
    Ok(())
}

async fn assert_initial_segment_kind_classification(
    client: &tokio_postgres::Client,
    schema: &str,
) -> Result<()> {
    let settled_cnt: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*)::BIGINT FROM {schema}.transaction_location WHERE block_height <= 3 AND segment_kind = 1"
            ),
            &[],
        )
        .await?
        .get(0);
    let live_cnt: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*)::BIGINT FROM {schema}.transaction_location WHERE block_height > 3 AND segment_kind = 0"
            ),
            &[],
        )
        .await?
        .get(0);
    let tail_cnt: i64 = client
        .query_one(
            &format!("SELECT COUNT(*)::BIGINT FROM {schema}.canonical_live_tail"),
            &[],
        )
        .await?
        .get(0);
    assert!(settled_cnt >= 3);
    assert!(live_cnt >= 3);
    assert_eq!(tail_cnt, 3);
    Ok(())
}

async fn verify_expired_tip_rejection(
    client: &mut tokio_postgres::Client,
    schema: &str,
    fence: WriterFenceHandle,
    target: &mut TipAdvanceTarget<'_>,
) -> Result<()> {
    client
        .execute(
            &format!(
                "UPDATE {schema}.canonical_writer_fence SET lease_expires_at = clock_timestamp() - INTERVAL '5 seconds'"
            ),
            &[],
        )
        .await?;

    let prepared_block_9 =
        prepare_block_range_from_source(target.source, target.prepare_state, 9, 9).await?;
    let block_9 = prepared_block_9
        .first()
        .ok_or_else(|| eyre!("expected prepared block 9"))?;
    let staged_wallet = target.wallet_projection.stage_block(&block_9.facts)?;
    let tip_txn = client.transaction().await?;
    let expired_tip = persist_tip_block_in_txn(
        &tip_txn,
        schema,
        &TipCommitPayload {
            fence,
            full_blocks: false,
            first_height: BlockHeight::new(1),
            block: block_9,
            wallet_delta: &staged_wallet.delta,
            evicted_mempool_txids: &[],
            pending_seal: None,
        },
    )
    .await
    .err()
    .ok_or_else(|| eyre!("expired writer lease must fail tip commit"))?;
    assert!(expired_tip.to_string().contains("lease expired"));
    tip_txn.rollback().await?;
    Ok(())
}

async fn verify_settlement_fence_rejections(
    client: &mut tokio_postgres::Client,
    schema: &str,
    fence: WriterFenceHandle,
    tail_slice: &[zinder_bench::postgres_segmented::segment_builder::PreparedSegmentedBlock],
) -> Result<()> {
    let sealed_out = seal_settled_segments(
        Network::ZcashRegtest,
        tail_slice.to_vec(),
        3,
        false,
        MAX_SETTLED_SEGMENT_BYTES,
    )
    .await?;
    let last_sealed = tail_slice
        .last()
        .ok_or_else(|| eyre!("missing tail slice"))?;
    let pending_seal = PendingSealTransition {
        bundles: sealed_out.bundles,
        settled_height: last_sealed.height,
        settled_hash: last_sealed.block_hash,
        settled_prev: last_sealed.previous_block_hash,
        seal_count: tail_slice.len(),
        max_segment_bytes_observed: sealed_out.max_segment_bytes_observed,
    };

    let expired_seal = persist_sealed_tail_transition(client, schema, fence, false, &pending_seal)
        .await
        .err()
        .ok_or_else(|| eyre!("expired writer lease must fail settlement"))?;
    assert!(expired_seal.to_string().contains("lease expired"));

    client
        .execute(
            &format!(
                "UPDATE {schema}.canonical_writer_fence SET writer_epoch = $1, writer_id = 'competing-writer', lease_expires_at = clock_timestamp() + INTERVAL '30 seconds'",
            ),
            &[&(fence.writer_epoch + 1)],
        )
        .await?;

    let superseded_seal =
        persist_sealed_tail_transition(client, schema, fence, false, &pending_seal)
            .await
            .err()
            .ok_or_else(|| eyre!("superseded writer_epoch must fail settlement"))?;
    assert!(
        superseded_seal
            .to_string()
            .contains("fenced by another epoch")
    );

    let superseded_term = acquire_post_finalize_writer_term(
        client,
        schema,
        fence.writer_epoch,
        BENCHMARK_LEASE_DURATION_MS,
    )
    .await
    .err()
    .ok_or_else(|| eyre!("superseded writer_epoch must fail term acquisition"))?;
    assert!(superseded_term.to_string().contains("superseded"));
    Ok(())
}
