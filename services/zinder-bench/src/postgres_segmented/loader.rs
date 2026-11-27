//! Bulk fixture preparation, binary `COPY` ingestion, finalized index creation,
//! and incremental single-block persistence primitives.

use std::path::Path;
use std::time::{Duration, Instant};

use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, Transaction};
use zinder_core::wire::{
    encode_address_script_hash, encode_internal_block_hash, encode_internal_transaction_id,
    encode_zinder_native_chain_name,
};
use zinder_core::{
    BlockHash, BlockHeight, CanonicalBlockFactsDigestVersion,
    CanonicalBlockFactsSequenceDigestBuilder, CanonicalBlockFactsSequenceDigestVersion, Network,
    NetworkUpgradeActivations, ShieldedProtocol, SubtreeRootArtifact,
    encode_canonical_block_replay,
};
use zinder_ingest::{
    CommitmentTreeSizes, RawBlobPolicy, position_canonical_block, prepare_canonical_block,
};
use zinder_source::SourceBlock;
use zinder_store::TREE_STATE_CHECKPOINT_STRIDE;

use crate::error::BenchError;
use crate::fixture::{FixtureManifest, read_segment_blocks};

use super::database::{candidate_error, postgres_error, u32_to_i64, u64_to_i64, usize_to_i64};
use super::schema::{
    BENCHMARK_LEASE_DURATION_MS, BENCHMARK_WRITER_ID, POSTGRES_SEGMENTED_CONTRACT_IDENTITY,
    POSTGRES_SEGMENTED_SCHEMA_VERSION, build_analyze_sql, build_create_schema_sql,
    build_finalize_schema_sql,
};
use super::segment_builder::{
    PreparedSegmentedBlock, SealedSegmentBundle, SealedSegmentBytes, encode_compact_block_for_tail,
    extract_consensus_tx_slices, seal_settled_segments,
};
use super::wallet_projection::{
    ProvisionalWalletProjection, WalletAddressTxRow, WalletBlockDelta, WalletUtxoRow,
};

/// Prepared benchmark fixture blocks partitioned for bulk ingest + incremental tip-follow.
pub struct PreparedBenchmarkChain {
    /// Chain blocks ordered ascending from `manifest.from_height`.
    pub blocks: Vec<PreparedSegmentedBlock>,
    /// Subtree roots emitted by the fixture manifest.
    pub subtree_roots: Vec<SubtreeRootArtifact>,
    /// Canonical sequence digest hex computed across ALL `blocks`.
    pub full_sequence_digest_hex: String,
    /// Total canonical transaction count across ALL `blocks`.
    pub total_tx_count: u64,
    /// Deterministic count of strided `tree_state_checkpoint` rows over ALL `blocks`.
    pub total_checkpoint_count: u64,
}

/// Breakdown of bulk-load build durations before steady-state tip-following.
#[derive(Clone, Debug, Default)]
pub struct BulkLoadTimings {
    /// Block parse, fact synthesis, compact-block positioning, and replay encoding.
    pub prepare_duration: Duration,
    /// Blocking `ZSHRNG02`, `ZSTSUP01`, and optional `ZSBLKB01` segment sealing duration.
    pub seal_duration: Duration,
    /// Binary `COPY` stream time into unindexed tables.
    pub copy_duration: Duration,
    /// Primary key + secondary index creation + `ANALYZE` time.
    pub finalize_duration: Duration,
}

/// Seeded schema metadata parameters inserted into `{schema}.schema_meta`.
pub struct SchemaSeedMetadata<'a> {
    /// Validated schema name.
    pub schema_name: &'a str,
    /// Network identifier.
    pub network: Network,
    /// Whether `ZSBLKB01` + `raw_block_bytes` retention is enabled.
    pub full_blocks: bool,
    /// Configured settled segment byte cap.
    pub max_segment_bytes: u64,
    /// Configured settled segment block span.
    pub settled_segment_blocks: u32,
    /// Configured live-tail upper bound in blocks.
    pub live_tail_max_blocks: u32,
    /// Fixture manifest schema revision.
    pub fixture_format_version: u32,
    /// Fixture manifest SHA-256 hex digest.
    pub fixture_manifest_sha256: &'a str,
    /// Expected full-run canonical sequence digest hex.
    pub expected_sequence_digest_hex: &'a str,
}

/// All atomic updates applied during a single incremental tip-follow block commit.
pub struct TipCommitPayload<'a> {
    /// Whether raw block retention is active.
    pub full_blocks: bool,
    /// First chain height (governs checkpoint cadence).
    pub first_height: BlockHeight,
    /// Prepared block being appended to the tip.
    pub block: &'a PreparedSegmentedBlock,
    /// Pre-validated wallet UTXO/address changes.
    pub wallet_delta: &'a WalletBlockDelta,
    /// Mempool transaction IDs evicted atomically as mined in this block.
    pub evicted_mempool_txids: &'a [[u8; 32]],
}

/// Deterministic checkpoint predicate: persists at `first_height` and every
/// `TREE_STATE_CHECKPOINT_STRIDE` multiple.
#[must_use]
pub const fn should_checkpoint_height(first_height: BlockHeight, height: BlockHeight) -> bool {
    height.value() == first_height.value()
        || height.value().is_multiple_of(TREE_STATE_CHECKPOINT_STRIDE)
}

/// Reads all segments from `fixture_dir` and prepares all canonical/compact/raw
/// blocks in a blocking worker thread, computing the full reference digest oracle.
pub async fn prepare_all_fixture_blocks(
    fixture_dir: &Path,
    manifest: &FixtureManifest,
    full_blocks: bool,
) -> Result<(PreparedBenchmarkChain, Duration), BenchError> {
    let dir = fixture_dir.to_path_buf();
    let cloned_manifest = manifest.clone();
    let started = Instant::now();
    let chain = tokio::task::spawn_blocking(move || {
        prepare_fixture_blocks_sync(&dir, &cloned_manifest, full_blocks)
    })
    .await
    .map_err(|err| candidate_error(format!("fixture block preparation panicked: {err}")))??;
    Ok((chain, started.elapsed()))
}

fn prepare_fixture_blocks_sync(
    fixture_dir: &Path,
    manifest: &FixtureManifest,
    full_blocks: bool,
) -> Result<PreparedBenchmarkChain, BenchError> {
    let network = manifest.network_typed()?;
    let activations = manifest.activations_typed()?;
    let subtree_roots = manifest.subtree_root_artifacts()?;
    let total_expected = usize::try_from(manifest.block_count)
        .map_err(|_| BenchError::fixture_format("block_count exceeds usize"))?;

    let mut tree_sizes = CommitmentTreeSizes {
        sapling: 0,
        orchard: 0,
        ironwood: 0,
    };
    let mut seq_builder = CanonicalBlockFactsSequenceDigestBuilder::new(
        CanonicalBlockFactsSequenceDigestVersion::CURRENT,
    );
    let mut blocks = Vec::with_capacity(total_expected);
    let mut total_tx_count = 0_u64;
    let mut total_checkpoint_count = 0_u64;
    let first_height = BlockHeight::new(manifest.from_height);

    for descriptor in &manifest.segments {
        let segment_blocks = read_segment_blocks(fixture_dir, descriptor, network)?;
        for source_block in &segment_blocks {
            let prepared =
                prepare_single_block(source_block, &activations, &mut tree_sizes, full_blocks)?;
            seq_builder
                .try_append(prepared.facts.digest(CanonicalBlockFactsDigestVersion::V1))
                .map_err(|err| candidate_error(format!("sequence builder append: {err}")))?;
            total_tx_count = total_tx_count
                .saturating_add(u64::try_from(prepared.facts.transactions.len()).unwrap_or(0));
            if should_checkpoint_height(first_height, prepared.height) {
                total_checkpoint_count = total_checkpoint_count.saturating_add(1);
            }
            blocks.push(prepared);
        }
    }

    if blocks.is_empty() {
        return Err(BenchError::fixture_format("fixture contains zero blocks"));
    }

    Ok(PreparedBenchmarkChain {
        blocks,
        subtree_roots,
        full_sequence_digest_hex: hex::encode(seq_builder.finish().as_bytes()),
        total_tx_count,
        total_checkpoint_count,
    })
}

fn prepare_single_block(
    source_block: &SourceBlock,
    activations: &NetworkUpgradeActivations,
    tree_sizes: &mut CommitmentTreeSizes,
    full_blocks: bool,
) -> Result<PreparedSegmentedBlock, BenchError> {
    let raw_policy = if full_blocks {
        RawBlobPolicy::All
    } else {
        RawBlobPolicy::None
    };
    let prepared = prepare_canonical_block(source_block, activations, raw_policy)
        .map_err(|err| candidate_error(format!("prepare canonical block: {err}")))?;
    let positioned = position_canonical_block(prepared, tree_sizes)
        .map_err(|err| candidate_error(format!("position canonical block: {err}")))?;

    let raw_tx_slices = if full_blocks {
        let block_blob = positioned
            .retained_raw_blobs
            .block_blob
            .as_ref()
            .ok_or_else(|| {
                candidate_error("expected retained block_blob when RawBlobPolicy::All")
            })?;
        extract_consensus_tx_slices(
            &block_blob.raw_block_bytes,
            &positioned.retained_raw_blobs.transaction_blobs,
        )?
    } else {
        Vec::new()
    };
    let raw_block_bytes = if full_blocks {
        positioned
            .retained_raw_blobs
            .block_blob
            .map(|blob| blob.raw_block_bytes)
    } else {
        None
    };

    let compact_block_bytes = encode_compact_block_for_tail(&positioned.compact_block);
    let compact_block_proto_len = compact_block_bytes.len();
    let canonical_facts_bytes = encode_canonical_block_replay(
        &positioned.facts,
        positioned.replay_envelope.format_version(),
        CanonicalBlockFactsDigestVersion::V1,
    )
    .into_bytes();

    Ok(PreparedSegmentedBlock {
        height: positioned.facts.block_header.height,
        block_hash: positioned.facts.block_header.block_hash,
        previous_block_hash: positioned.facts.block_header.parent_hash,
        compact_block: positioned.compact_block,
        compact_block_proto_len,
        compact_block_bytes,
        facts: positioned.facts,
        canonical_facts_bytes,
        raw_block_bytes,
        raw_tx_slices,
    })
}

/// Splits `bulk_blocks` into settled prefix and initial bounded live tail, seals
/// settled segments, bulk-loads all tables via binary `COPY`, and finalizes indexes.
pub async fn load_bulk_prefix(
    client: &mut Client,
    meta: &SchemaSeedMetadata<'_>,
    bulk_blocks: &[PreparedSegmentedBlock],
    subtree_roots: &[SubtreeRootArtifact],
    wallet_projection: &mut ProvisionalWalletProjection,
) -> Result<(BulkLoadTimings, u64), BenchError> {
    let first_block = bulk_blocks
        .first()
        .ok_or_else(|| BenchError::invalid_argument("bulk prefix must contain at least 1 block"))?;
    let last_block = bulk_blocks
        .last()
        .ok_or_else(|| candidate_error("bulk_blocks unexpectedly empty"))?;
    let schema = meta.schema_name;
    let max_tail = usize::try_from(meta.live_tail_max_blocks).unwrap_or(128);
    let settled_count = bulk_blocks.len().saturating_sub(max_tail);
    let (settled_slice, tail_slice) = bulk_blocks.split_at(settled_count);

    let seal_started = Instant::now();
    let sealed_output = seal_settled_segments(
        meta.network,
        settled_slice.to_vec(),
        meta.settled_segment_blocks,
        meta.full_blocks,
        meta.max_segment_bytes,
    )
    .await?;
    let seal_duration = seal_started.elapsed();

    for block in bulk_blocks {
        let _ = wallet_projection.apply_block(&block.facts)?;
    }

    client
        .batch_execute(&build_create_schema_sql(schema)?)
        .await
        .map_err(|err| postgres_error("creating postgres-segmented schema", &err))?;

    let copy_started = Instant::now();
    let settled_tip = settled_slice.last().map(|block| block.height);
    let txn = client
        .transaction()
        .await
        .map_err(|err| postgres_error("starting bulk load transaction", &err))?;
    seed_control_tables(&txn, meta, settled_tip, (first_block, last_block)).await?;
    copy_settled_bundles(&txn, schema, meta.full_blocks, &sealed_output.bundles).await?;
    copy_canonical_live_tail(&txn, schema, meta.full_blocks, tail_slice).await?;
    copy_canonical_indexes(&txn, schema, first_block.height, bulk_blocks).await?;
    copy_subtree_roots(&txn, schema, subtree_roots).await?;
    copy_wallet_projection(&txn, schema, wallet_projection).await?;
    txn.commit()
        .await
        .map_err(|err| postgres_error("committing bulk COPY payload", &err))?;
    let copy_duration = copy_started.elapsed();

    let finalize_started = Instant::now();
    client
        .batch_execute(&build_finalize_schema_sql(schema)?)
        .await
        .map_err(|err| postgres_error("finalizing segmented schema indexes", &err))?;
    client
        .batch_execute(&build_analyze_sql(schema)?)
        .await
        .map_err(|err| postgres_error("analyzing segmented schema tables", &err))?;
    let finalize_duration = finalize_started.elapsed();

    Ok((
        BulkLoadTimings {
            prepare_duration: Duration::ZERO,
            seal_duration,
            copy_duration,
            finalize_duration,
        },
        sealed_output.max_segment_bytes_observed,
    ))
}

async fn seed_control_tables(
    txn: &Transaction<'_>,
    meta: &SchemaSeedMetadata<'_>,
    settled_tip: Option<BlockHeight>,
    bounds: (&PreparedSegmentedBlock, &PreparedSegmentedBlock),
) -> Result<(), BenchError> {
    let (first_block, canonical_tip) = bounds;
    let schema = meta.schema_name;
    let network_name = encode_zinder_native_chain_name(meta.network);
    insert_schema_meta_row(txn, meta, network_name, settled_tip, bounds).await?;
    insert_fence_and_epoch(txn, schema, network_name, first_block, canonical_tip).await
}

async fn insert_schema_meta_row(
    txn: &Transaction<'_>,
    meta: &SchemaSeedMetadata<'_>,
    network_name: &str,
    settled_tip: Option<BlockHeight>,
    bounds: (&PreparedSegmentedBlock, &PreparedSegmentedBlock),
) -> Result<(), BenchError> {
    let (first_block, canonical_tip) = bounds;
    let schema = meta.schema_name;
    let settled_tip_pg = settled_tip.map(|h| u32_to_i64(h.value()));
    let tip_hash_wire = encode_internal_block_hash(canonical_tip.block_hash);
    txn.execute(
        &format!(
            r"
INSERT INTO {schema}.schema_meta (
    singleton, contract_identity, schema_version, network, artifact_schema_version,
    full_blocks, max_segment_bytes, settled_segment_blocks, live_tail_max_blocks,
    completed, first_height, settled_tip_height, canonical_tip_height,
    canonical_tip_hash, expected_sequence_digest, fixture_format_version, fixture_manifest_sha256
) VALUES (TRUE, $1, $2, $3, $4, $5, $6, $7, $8, FALSE, $9, $10, $11, $12, $13, $14, $15)
"
        ),
        &[
            &POSTGRES_SEGMENTED_CONTRACT_IDENTITY,
            &i32::from(POSTGRES_SEGMENTED_SCHEMA_VERSION),
            &network_name,
            &i32::from(zinder_store::CURRENT_ARTIFACT_SCHEMA_VERSION.value()),
            &meta.full_blocks,
            &u64_to_i64(meta.max_segment_bytes, "max_segment_bytes")?,
            &u32_to_i64(meta.settled_segment_blocks),
            &u32_to_i64(meta.live_tail_max_blocks),
            &u32_to_i64(first_block.height.value()),
            &settled_tip_pg,
            &u32_to_i64(canonical_tip.height.value()),
            &tip_hash_wire.as_slice(),
            &meta.expected_sequence_digest_hex,
            &i32::try_from(meta.fixture_format_version)
                .map_err(|_| candidate_error("fixture_format_version exceeds i32"))?,
            &meta.fixture_manifest_sha256,
        ],
    )
    .await
    .map_err(|err| postgres_error("inserting schema_meta", &err))?;
    Ok(())
}

async fn insert_fence_and_epoch(
    txn: &Transaction<'_>,
    schema: &str,
    network_name: &str,
    first_block: &PreparedSegmentedBlock,
    canonical_tip: &PreparedSegmentedBlock,
) -> Result<(), BenchError> {
    txn.execute(
        &format!(
            r"
INSERT INTO {schema}.canonical_writer_fence (
    singleton, writer_epoch, writer_id, lease_expires_at, updated_at
) VALUES (
    TRUE, 1, $1,
    clock_timestamp() + ($2::BIGINT * INTERVAL '1 millisecond'),
    clock_timestamp()
)
"
        ),
        &[&BENCHMARK_WRITER_ID, &BENCHMARK_LEASE_DURATION_MS],
    )
    .await
    .map_err(|err| postgres_error("seeding canonical_writer_fence", &err))?;

    txn.execute(
        &format!(
            "INSERT INTO {schema}.chain_epoch (epoch, network, genesis_hash) VALUES (1, $1, $2)"
        ),
        &[
            &network_name,
            &encode_internal_block_hash(first_block.previous_block_hash).as_slice(),
        ],
    )
    .await
    .map_err(|err| postgres_error("inserting initial chain_epoch", &err))?;

    txn.execute(
        &format!(
            "INSERT INTO {schema}.chain_event (epoch, event_kind, height, block_hash, previous_hash) VALUES (1, 'bulk_seed', $1, $2, $3)"
        ),
        &[
            &u32_to_i64(canonical_tip.height.value()),
            &encode_internal_block_hash(canonical_tip.block_hash).as_slice(),
            &encode_internal_block_hash(canonical_tip.previous_block_hash).as_slice(),
        ],
    )
    .await
    .map_err(|err| postgres_error("inserting bulk_seed chain_event", &err))?;
    Ok(())
}

/// Inserts settled bundles (`ZSHRNG02`, `ZSTSUP01`, optional `ZSBLKB01`) into
/// `{schema}.canonical_settled_segment` via binary `COPY`.
pub async fn copy_settled_bundles(
    txn: &Transaction<'_>,
    schema: &str,
    full_blocks: bool,
    bundles: &[SealedSegmentBundle],
) -> Result<(), BenchError> {
    if bundles.is_empty() {
        return Ok(());
    }
    let sink = txn
        .copy_in(&format!(
            "COPY {schema}.canonical_settled_segment (segment_kind, start_height, end_height, artifact_schema_version, compression_codec, chunk_blocks, per_block_framing, segment_sha256, index_sha256, segment_byte_len, segment_bytes) FROM STDIN BINARY"
        ))
        .await
        .map_err(|err| postgres_error("starting canonical_settled_segment COPY", &err))?;
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            Type::TEXT,
            Type::INT8,
            Type::INT8,
            Type::INT4,
            Type::TEXT,
            Type::INT4,
            Type::BOOL,
            Type::BYTEA,
            Type::BYTEA,
            Type::INT8,
            Type::BYTEA,
        ],
    );
    futures_util::pin_mut!(writer);

    let artifact_ver = i32::from(zinder_store::CURRENT_ARTIFACT_SCHEMA_VERSION.value());
    let sup_chunks = i32::from(zinder_store::SETTLED_SUPPLEMENT_DEFAULT_CHUNK_BLOCKS);
    for bundle in bundles {
        write_settled_row(
            writer.as_mut(),
            SettledSegmentSpec {
                kind: "shielded",
                artifact_ver,
                codec: "none",
                chunk_blocks: None,
                per_block_framing: false,
            },
            &bundle.shielded,
        )
        .await?;
        write_settled_row(
            writer.as_mut(),
            SettledSegmentSpec {
                kind: "supplement",
                artifact_ver,
                codec: "zstd",
                chunk_blocks: Some(sup_chunks),
                per_block_framing: false,
            },
            &bundle.supplement,
        )
        .await?;
        if full_blocks && let Some(raw_seg) = bundle.raw_block.as_ref() {
            write_settled_row(
                writer.as_mut(),
                SettledSegmentSpec {
                    kind: "raw_block",
                    artifact_ver,
                    codec: "zstd",
                    chunk_blocks: None,
                    per_block_framing: true,
                },
                raw_seg,
            )
            .await?;
        }
    }
    writer
        .finish()
        .await
        .map_err(|err| postgres_error("finishing canonical_settled_segment COPY", &err))?;
    Ok(())
}

struct SettledSegmentSpec {
    kind: &'static str,
    artifact_ver: i32,
    codec: &'static str,
    chunk_blocks: Option<i32>,
    per_block_framing: bool,
}

async fn write_settled_row(
    writer: std::pin::Pin<&mut BinaryCopyInWriter>,
    spec: SettledSegmentSpec,
    seg: &SealedSegmentBytes,
) -> Result<(), BenchError> {
    let len_pg = usize_to_i64(seg.bytes.len(), "segment_bytes")?;
    writer
        .write(&[
            &spec.kind,
            &u32_to_i64(seg.start_height.value()),
            &u32_to_i64(seg.end_height.value()),
            &spec.artifact_ver,
            &spec.codec,
            &spec.chunk_blocks,
            &spec.per_block_framing,
            &seg.segment_sha256.as_slice(),
            &seg.index_sha256.as_slice(),
            &len_pg,
            &seg.bytes.as_slice(),
        ])
        .await
        .map_err(|err| postgres_error("writing canonical_settled_segment COPY row", &err))
}

async fn copy_canonical_live_tail(
    txn: &Transaction<'_>,
    schema: &str,
    full_blocks: bool,
    tail_blocks: &[PreparedSegmentedBlock],
) -> Result<(), BenchError> {
    if tail_blocks.is_empty() {
        return Ok(());
    }
    let sink = txn
        .copy_in(&format!(
            "COPY {schema}.canonical_live_tail (height, block_hash, previous_hash, epoch, compact_block_bytes, canonical_facts_bytes, raw_block_bytes) FROM STDIN BINARY"
        ))
        .await
        .map_err(|err| postgres_error("starting canonical_live_tail COPY", &err))?;
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            Type::INT8,
            Type::BYTEA,
            Type::BYTEA,
            Type::INT8,
            Type::BYTEA,
            Type::BYTEA,
            Type::BYTEA,
        ],
    );
    futures_util::pin_mut!(writer);
    for block in tail_blocks {
        let raw_opt = if full_blocks {
            block.raw_block_bytes.as_deref()
        } else {
            None
        };
        writer
            .as_mut()
            .write(&[
                &u32_to_i64(block.height.value()),
                &encode_internal_block_hash(block.block_hash).as_slice(),
                &encode_internal_block_hash(block.previous_block_hash).as_slice(),
                &1_i64,
                &block.compact_block_bytes.as_slice(),
                &block.canonical_facts_bytes.as_slice(),
                &raw_opt,
            ])
            .await
            .map_err(|err| postgres_error("writing canonical_live_tail COPY row", &err))?;
    }
    writer
        .finish()
        .await
        .map_err(|err| postgres_error("finishing canonical_live_tail COPY", &err))?;
    Ok(())
}

async fn copy_canonical_indexes(
    txn: &Transaction<'_>,
    schema: &str,
    first_height: BlockHeight,
    blocks: &[PreparedSegmentedBlock],
) -> Result<(), BenchError> {
    copy_block_headers(txn, schema, blocks).await?;
    copy_block_hash_index(txn, schema, blocks).await?;
    copy_tree_state_checkpoints(txn, schema, first_height, blocks).await?;
    copy_transaction_locations(txn, schema, blocks).await
}

async fn copy_block_headers(
    txn: &Transaction<'_>,
    schema: &str,
    blocks: &[PreparedSegmentedBlock],
) -> Result<(), BenchError> {
    let sink = txn
        .copy_in(&format!(
            "COPY {schema}.block_header (height, block_hash, previous_hash, epoch, version, time_unix, bits, nonce, merkle_root, final_sapling_root, sapling_commitment_tree_size, orchard_commitment_tree_size, ironwood_commitment_tree_size, block_size_bytes, transaction_count) FROM STDIN BINARY"
        ))
        .await
        .map_err(|err| postgres_error("starting block_header COPY", &err))?;
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            Type::INT8,
            Type::BYTEA,
            Type::BYTEA,
            Type::INT8,
            Type::INT8,
            Type::INT8,
            Type::INT8,
            Type::BYTEA,
            Type::BYTEA,
            Type::BYTEA,
            Type::INT8,
            Type::INT8,
            Type::INT8,
            Type::INT8,
            Type::INT8,
        ],
    );
    futures_util::pin_mut!(writer);
    for block in blocks {
        let hdr = &block.facts.block_header;
        let meta = block.compact_block.chain_metadata();
        let tx_count = usize_to_i64(block.facts.transactions.len(), "transaction_count")?;
        writer
            .as_mut()
            .write(&[
                &u32_to_i64(hdr.height.value()),
                &encode_internal_block_hash(hdr.block_hash).as_slice(),
                &encode_internal_block_hash(hdr.parent_hash).as_slice(),
                &1_i64,
                &u32_to_i64(hdr.version),
                &hdr.block_time,
                &u32_to_i64(hdr.bits),
                &hdr.nonce.as_slice(),
                &hdr.merkle_root_hash.as_slice(),
                &hdr.commitment_bytes.as_slice(),
                &u32_to_i64(meta.sapling_commitment_tree_size),
                &u32_to_i64(meta.orchard_commitment_tree_size),
                &u32_to_i64(meta.ironwood_commitment_tree_size),
                &u64_to_i64(hdr.block_size_bytes, "block_size_bytes")?,
                &tx_count,
            ])
            .await
            .map_err(|err| postgres_error("writing block_header COPY row", &err))?;
    }
    writer
        .finish()
        .await
        .map_err(|err| postgres_error("finishing block_header COPY", &err))?;
    Ok(())
}

async fn copy_block_hash_index(
    txn: &Transaction<'_>,
    schema: &str,
    blocks: &[PreparedSegmentedBlock],
) -> Result<(), BenchError> {
    let sink = txn
        .copy_in(&format!(
            "COPY {schema}.block_hash_index (block_hash, height) FROM STDIN BINARY"
        ))
        .await
        .map_err(|err| postgres_error("starting block_hash_index COPY", &err))?;
    let writer = BinaryCopyInWriter::new(sink, &[Type::BYTEA, Type::INT8]);
    futures_util::pin_mut!(writer);
    for block in blocks {
        writer
            .as_mut()
            .write(&[
                &encode_internal_block_hash(block.block_hash).as_slice(),
                &u32_to_i64(block.height.value()),
            ])
            .await
            .map_err(|err| postgres_error("writing block_hash_index COPY row", &err))?;
    }
    writer
        .finish()
        .await
        .map_err(|err| postgres_error("finishing block_hash_index COPY", &err))?;
    Ok(())
}

async fn copy_tree_state_checkpoints(
    txn: &Transaction<'_>,
    schema: &str,
    first_height: BlockHeight,
    blocks: &[PreparedSegmentedBlock],
) -> Result<(), BenchError> {
    let sink = txn
        .copy_in(&format!(
            "COPY {schema}.tree_state_checkpoint (height, block_hash, sapling_tree_size, orchard_tree_size, ironwood_tree_size, commitment_bytes) FROM STDIN BINARY"
        ))
        .await
        .map_err(|err| postgres_error("starting tree_state_checkpoint COPY", &err))?;
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            Type::INT8,
            Type::BYTEA,
            Type::INT8,
            Type::INT8,
            Type::INT8,
            Type::BYTEA,
        ],
    );
    futures_util::pin_mut!(writer);
    for block in blocks {
        if !should_checkpoint_height(first_height, block.height) {
            continue;
        }
        let meta = block.compact_block.chain_metadata();
        let commitment = block.facts.block_header.commitment_bytes;
        writer
            .as_mut()
            .write(&[
                &u32_to_i64(block.height.value()),
                &encode_internal_block_hash(block.block_hash).as_slice(),
                &u32_to_i64(meta.sapling_commitment_tree_size),
                &u32_to_i64(meta.orchard_commitment_tree_size),
                &u32_to_i64(meta.ironwood_commitment_tree_size),
                &commitment.as_slice(),
            ])
            .await
            .map_err(|err| postgres_error("writing tree_state_checkpoint COPY row", &err))?;
    }
    writer
        .finish()
        .await
        .map_err(|err| postgres_error("finishing tree_state_checkpoint COPY", &err))?;
    Ok(())
}

async fn copy_transaction_locations(
    txn: &Transaction<'_>,
    schema: &str,
    blocks: &[PreparedSegmentedBlock],
) -> Result<(), BenchError> {
    let sink = txn
        .copy_in(&format!(
            "COPY {schema}.transaction_location (txid, block_height, block_hash, tx_index, segment_kind) FROM STDIN BINARY"
        ))
        .await
        .map_err(|err| postgres_error("starting transaction_location COPY", &err))?;
    let writer = BinaryCopyInWriter::new(
        sink,
        &[Type::BYTEA, Type::INT8, Type::BYTEA, Type::INT8, Type::INT2],
    );
    futures_util::pin_mut!(writer);
    for block in blocks {
        let height_pg = u32_to_i64(block.height.value());
        let block_hash_wire = encode_internal_block_hash(block.block_hash);
        for (tx_pos, tx) in block.facts.transactions.iter().enumerate() {
            let tx_idx_pg = usize_to_i64(tx_pos, "tx_index")?;
            let txid_wire = encode_internal_transaction_id(tx.public_facts.transaction_id);
            writer
                .as_mut()
                .write(&[
                    &txid_wire.as_slice(),
                    &height_pg,
                    &block_hash_wire.as_slice(),
                    &tx_idx_pg,
                    &1_i16,
                ])
                .await
                .map_err(|err| postgres_error("writing transaction_location COPY row", &err))?;
        }
    }
    writer
        .finish()
        .await
        .map_err(|err| postgres_error("finishing transaction_location COPY", &err))?;
    Ok(())
}

async fn copy_subtree_roots(
    txn: &Transaction<'_>,
    schema: &str,
    subtree_roots: &[SubtreeRootArtifact],
) -> Result<(), BenchError> {
    if subtree_roots.is_empty() {
        return Ok(());
    }
    let sink = txn
        .copy_in(&format!(
            "COPY {schema}.subtree_root (pool, subtree_index, root_hash, completing_block_height, completing_block_hash) FROM STDIN BINARY"
        ))
        .await
        .map_err(|err| postgres_error("starting subtree_root COPY", &err))?;
    let writer = BinaryCopyInWriter::new(
        sink,
        &[Type::INT2, Type::INT8, Type::BYTEA, Type::INT8, Type::BYTEA],
    );
    futures_util::pin_mut!(writer);
    for root in subtree_roots {
        let pool_pg: i16 = match root.protocol {
            ShieldedProtocol::Sapling => 1,
            ShieldedProtocol::Orchard => 2,
            ShieldedProtocol::Ironwood => 3,
            _ => {
                return Err(candidate_error(format!(
                    "unsupported shielded protocol variant for subtree root {}",
                    root.subtree_index.value()
                )));
            }
        };
        let completing_hash_wire = encode_internal_block_hash(root.completing_block_hash);
        let root_hash_bytes = root.root_hash.as_bytes();
        writer
            .as_mut()
            .write(&[
                &pool_pg,
                &u32_to_i64(root.subtree_index.value()),
                &root_hash_bytes.as_slice(),
                &u32_to_i64(root.completing_block_height.value()),
                &completing_hash_wire.as_slice(),
            ])
            .await
            .map_err(|err| postgres_error("writing subtree_root COPY row", &err))?;
    }
    writer
        .finish()
        .await
        .map_err(|err| postgres_error("finishing subtree_root COPY", &err))?;
    Ok(())
}

async fn copy_wallet_projection(
    txn: &Transaction<'_>,
    schema: &str,
    wallet_projection: &ProvisionalWalletProjection,
) -> Result<(), BenchError> {
    copy_wallet_utxos(txn, schema, &wallet_projection.sorted_utxo_rows()).await?;
    copy_wallet_address_txs(txn, schema, wallet_projection.address_tx_rows()).await
}

async fn copy_wallet_utxos(
    txn: &Transaction<'_>,
    schema: &str,
    utxos: &[WalletUtxoRow],
) -> Result<(), BenchError> {
    if utxos.is_empty() {
        return Ok(());
    }
    let sink = txn
        .copy_in(&format!(
            "COPY {schema}.wallet_utxo (script_hash, txid, output_index, block_height, value_zatoshis, spent_by_txid, spent_at_height) FROM STDIN BINARY"
        ))
        .await
        .map_err(|err| postgres_error("starting wallet_utxo COPY", &err))?;
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            Type::BYTEA,
            Type::BYTEA,
            Type::INT8,
            Type::INT8,
            Type::INT8,
            Type::BYTEA,
            Type::INT8,
        ],
    );
    futures_util::pin_mut!(writer);
    for utxo in utxos {
        let script_wire = encode_address_script_hash(utxo.script_hash);
        let txid_wire = encode_internal_transaction_id(utxo.txid);
        let spent_txid_wire = utxo.spent_by_txid.map(encode_internal_transaction_id);
        let spent_height_pg = utxo.spent_at_height.map(|h| u32_to_i64(h.value()));
        writer
            .as_mut()
            .write(&[
                &script_wire.as_slice(),
                &txid_wire.as_slice(),
                &u32_to_i64(utxo.output_index),
                &u32_to_i64(utxo.block_height.value()),
                &utxo.zatoshis,
                &spent_txid_wire.as_ref().map(<[u8; 32]>::as_slice),
                &spent_height_pg,
            ])
            .await
            .map_err(|err| postgres_error("writing wallet_utxo COPY row", &err))?;
    }
    writer
        .finish()
        .await
        .map_err(|err| postgres_error("finishing wallet_utxo COPY", &err))?;
    Ok(())
}

async fn copy_wallet_address_txs(
    txn: &Transaction<'_>,
    schema: &str,
    address_txs: &[WalletAddressTxRow],
) -> Result<(), BenchError> {
    if address_txs.is_empty() {
        return Ok(());
    }
    let sink = txn
        .copy_in(&format!(
            "COPY {schema}.wallet_address_tx (script_hash, block_height, tx_index, txid, net_delta_zatoshis) FROM STDIN BINARY"
        ))
        .await
        .map_err(|err| postgres_error("starting wallet_address_tx COPY", &err))?;
    let writer = BinaryCopyInWriter::new(
        sink,
        &[Type::BYTEA, Type::INT8, Type::INT8, Type::BYTEA, Type::INT8],
    );
    futures_util::pin_mut!(writer);
    for row in address_txs {
        let script_wire = encode_address_script_hash(row.script_hash);
        let txid_wire = encode_internal_transaction_id(row.txid);
        writer
            .as_mut()
            .write(&[
                &script_wire.as_slice(),
                &u32_to_i64(row.block_height.value()),
                &u32_to_i64(row.tx_index),
                &txid_wire.as_slice(),
                &row.net_delta_zatoshis,
            ])
            .await
            .map_err(|err| postgres_error("writing wallet_address_tx COPY row", &err))?;
    }
    writer
        .finish()
        .await
        .map_err(|err| postgres_error("finishing wallet_address_tx COPY", &err))?;
    Ok(())
}

/// Persists a newly mined tip block, writer-lease renewal, canonical index rows,
/// wallet delta, live-tail insert, AND atomic mined-mempool eviction in one ACID txn.
pub async fn persist_tip_block_in_txn(
    txn: &Transaction<'_>,
    schema: &str,
    payload: &TipCommitPayload<'_>,
) -> Result<(), BenchError> {
    renew_writer_fence(txn, schema).await?;
    let block = payload.block;
    insert_single_header_and_indexes(txn, schema, payload.first_height, block).await?;
    insert_single_wallet_delta(txn, schema, payload.wallet_delta).await?;
    evict_mined_mempool_in_txn(txn, schema, block.height, payload.evicted_mempool_txids).await?;
    insert_live_tail_and_advance_tip(txn, schema, payload.full_blocks, block).await
}

async fn renew_writer_fence(txn: &Transaction<'_>, schema: &str) -> Result<(), BenchError> {
    let renewed = txn
        .query_opt(
            &format!(
                r"
UPDATE {schema}.canonical_writer_fence
SET
    lease_expires_at = clock_timestamp() + ($1::BIGINT * INTERVAL '1 millisecond'),
    updated_at = clock_timestamp()
WHERE singleton
  AND writer_epoch = 1
  AND writer_id = $2
  AND lease_expires_at > clock_timestamp()
RETURNING writer_epoch
"
            ),
            &[&BENCHMARK_LEASE_DURATION_MS, &BENCHMARK_WRITER_ID],
        )
        .await
        .map_err(|err| postgres_error("renewing canonical_writer_fence", &err))?;
    if renewed.is_none() {
        return Err(candidate_error(
            "canonical_writer_fence lease expired or fenced by another epoch",
        ));
    }
    Ok(())
}

async fn insert_single_header_and_indexes(
    txn: &Transaction<'_>,
    schema: &str,
    first_height: BlockHeight,
    block: &PreparedSegmentedBlock,
) -> Result<(), BenchError> {
    let hdr = &block.facts.block_header;
    let meta = block.compact_block.chain_metadata();
    let tx_count = usize_to_i64(block.facts.transactions.len(), "transaction_count")?;
    let height_pg = u32_to_i64(hdr.height.value());
    let block_hash_wire = encode_internal_block_hash(hdr.block_hash);

    txn.execute(
        &format!(
            r"
INSERT INTO {schema}.block_header (
    height, block_hash, previous_hash, epoch, version, time_unix, bits,
    nonce, merkle_root, final_sapling_root, sapling_commitment_tree_size,
    orchard_commitment_tree_size, ironwood_commitment_tree_size,
    block_size_bytes, transaction_count
) VALUES ($1, $2, $3, 1, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
"
        ),
        &[
            &height_pg,
            &block_hash_wire.as_slice(),
            &encode_internal_block_hash(hdr.parent_hash).as_slice(),
            &u32_to_i64(hdr.version),
            &hdr.block_time,
            &u32_to_i64(hdr.bits),
            &hdr.nonce.as_slice(),
            &hdr.merkle_root_hash.as_slice(),
            &hdr.commitment_bytes.as_slice(),
            &u32_to_i64(meta.sapling_commitment_tree_size),
            &u32_to_i64(meta.orchard_commitment_tree_size),
            &u32_to_i64(meta.ironwood_commitment_tree_size),
            &u64_to_i64(hdr.block_size_bytes, "block_size_bytes")?,
            &tx_count,
        ],
    )
    .await
    .map_err(|err| postgres_error("inserting tip block_header", &err))?;

    txn.execute(
        &format!("INSERT INTO {schema}.block_hash_index (block_hash, height) VALUES ($1, $2)"),
        &[&block_hash_wire.as_slice(), &height_pg],
    )
    .await
    .map_err(|err| postgres_error("inserting tip block_hash_index", &err))?;

    if should_checkpoint_height(first_height, block.height) {
        txn.execute(
            &format!(
                "INSERT INTO {schema}.tree_state_checkpoint (height, block_hash, sapling_tree_size, orchard_tree_size, ironwood_tree_size, commitment_bytes) VALUES ($1, $2, $3, $4, $5, $6)"
            ),
            &[
                &height_pg,
                &block_hash_wire.as_slice(),
                &u32_to_i64(meta.sapling_commitment_tree_size),
                &u32_to_i64(meta.orchard_commitment_tree_size),
                &u32_to_i64(meta.ironwood_commitment_tree_size),
                &hdr.commitment_bytes.as_slice(),
            ],
        )
        .await
        .map_err(|err| postgres_error("inserting tip tree_state_checkpoint", &err))?;
    }

    for (tx_pos, tx) in block.facts.transactions.iter().enumerate() {
        let txid_wire = encode_internal_transaction_id(tx.public_facts.transaction_id);
        txn.execute(
            &format!(
                "INSERT INTO {schema}.transaction_location (txid, block_height, block_hash, tx_index, segment_kind) VALUES ($1, $2, $3, $4, 0)"
            ),
            &[
                &txid_wire.as_slice(),
                &height_pg,
                &block_hash_wire.as_slice(),
                &usize_to_i64(tx_pos, "tx_index")?,
            ],
        )
        .await
        .map_err(|err| postgres_error("inserting tip transaction_location", &err))?;
    }
    Ok(())
}

async fn evict_mined_mempool_in_txn(
    txn: &Transaction<'_>,
    schema: &str,
    height: BlockHeight,
    txids: &[[u8; 32]],
) -> Result<(), BenchError> {
    if txids.is_empty() {
        return Ok(());
    }
    let height_pg = u32_to_i64(height.value());
    for txid in txids {
        txn.execute(
            &format!("DELETE FROM {schema}.mempool_active_entry WHERE txid = $1"),
            &[&txid.as_slice()],
        )
        .await
        .map_err(|err| postgres_error("evicting mined mempool_active_entry", &err))?;
        txn.execute(
            &format!(
                "INSERT INTO {schema}.mempool_event (event_kind, txid, block_height) VALUES ('evict_mined', $1, $2)"
            ),
            &[&txid.as_slice(), &height_pg],
        )
        .await
        .map_err(|err| postgres_error("inserting evict_mined mempool_event", &err))?;
    }
    Ok(())
}

async fn insert_live_tail_and_advance_tip(
    txn: &Transaction<'_>,
    schema: &str,
    full_blocks: bool,
    block: &PreparedSegmentedBlock,
) -> Result<(), BenchError> {
    let raw_opt = if full_blocks {
        block.raw_block_bytes.as_deref()
    } else {
        None
    };
    let height_pg = u32_to_i64(block.height.value());
    let block_hash_wire = encode_internal_block_hash(block.block_hash);
    let prev_hash_wire = encode_internal_block_hash(block.previous_block_hash);

    txn.execute(
        &format!(
            "INSERT INTO {schema}.canonical_live_tail (height, block_hash, previous_hash, epoch, compact_block_bytes, canonical_facts_bytes, raw_block_bytes) VALUES ($1, $2, $3, 1, $4, $5, $6)"
        ),
        &[
            &height_pg,
            &block_hash_wire.as_slice(),
            &prev_hash_wire.as_slice(),
            &block.compact_block_bytes.as_slice(),
            &block.canonical_facts_bytes.as_slice(),
            &raw_opt,
        ],
    )
    .await
    .map_err(|err| postgres_error("inserting tip block into canonical_live_tail", &err))?;

    txn.execute(
        &format!(
            "INSERT INTO {schema}.chain_event (epoch, event_kind, height, block_hash, previous_hash) VALUES (1, 'tip_append', $1, $2, $3)"
        ),
        &[
            &height_pg,
            &block_hash_wire.as_slice(),
            &prev_hash_wire.as_slice(),
        ],
    )
    .await
    .map_err(|err| postgres_error("inserting tip_append chain_event", &err))?;

    txn.execute(
        &format!(
            "UPDATE {schema}.schema_meta SET canonical_tip_height = $1, canonical_tip_hash = $2 WHERE singleton"
        ),
        &[&height_pg, &block_hash_wire.as_slice()],
    )
    .await
    .map_err(|err| postgres_error("updating canonical tip metadata", &err))?;
    Ok(())
}

async fn insert_single_wallet_delta(
    txn: &Transaction<'_>,
    schema: &str,
    delta: &WalletBlockDelta,
) -> Result<(), BenchError> {
    for utxo in &delta.created_utxos {
        txn.execute(
            &format!(
                "INSERT INTO {schema}.wallet_utxo (script_hash, txid, output_index, block_height, value_zatoshis, spent_by_txid, spent_at_height) VALUES ($1, $2, $3, $4, $5, NULL, NULL)"
            ),
            &[
                &encode_address_script_hash(utxo.script_hash).as_slice(),
                &encode_internal_transaction_id(utxo.txid).as_slice(),
                &u32_to_i64(utxo.output_index),
                &u32_to_i64(utxo.block_height.value()),
                &utxo.zatoshis,
            ],
        )
        .await
        .map_err(|err| postgres_error("inserting tip wallet_utxo", &err))?;
    }
    for spend in &delta.spent_utxos {
        txn.execute(
            &format!(
                "UPDATE {schema}.wallet_utxo SET spent_by_txid = $1, spent_at_height = $2 WHERE txid = $3 AND output_index = $4 AND spent_at_height IS NULL"
            ),
            &[
                &encode_internal_transaction_id(spend.spent_by_txid).as_slice(),
                &u32_to_i64(spend.spent_at_height.value()),
                &encode_internal_transaction_id(spend.outpoint.transaction_id).as_slice(),
                &u32_to_i64(spend.outpoint.output_index),
            ],
        )
        .await
        .map_err(|err| postgres_error("marking spent wallet_utxo", &err))?;
    }
    for addr_row in &delta.address_tx_rows {
        txn.execute(
            &format!(
                "INSERT INTO {schema}.wallet_address_tx (script_hash, block_height, tx_index, txid, net_delta_zatoshis) VALUES ($1, $2, $3, $4, $5)"
            ),
            &[
                &encode_address_script_hash(addr_row.script_hash).as_slice(),
                &u32_to_i64(addr_row.block_height.value()),
                &u32_to_i64(addr_row.tx_index),
                &encode_internal_transaction_id(addr_row.txid).as_slice(),
                &addr_row.net_delta_zatoshis,
            ],
        )
        .await
        .map_err(|err| postgres_error("inserting tip wallet_address_tx", &err))?;
    }
    Ok(())
}

/// Marks `schema_meta.completed = TRUE` once all bulk, tip-follow, and sealing phases finish.
pub async fn mark_schema_completed(client: &Client, schema_name: &str) -> Result<(), BenchError> {
    let rows = client
        .execute(
            &format!("UPDATE {schema_name}.schema_meta SET completed = TRUE WHERE singleton"),
            &[],
        )
        .await
        .map_err(|err| postgres_error("marking schema_meta.completed", &err))?;
    if rows != 1 {
        return Err(candidate_error(
            "schema_meta singleton missing during completion",
        ));
    }
    Ok(())
}

/// Seals and atomically persists `bundles` while evicting covered rows from `canonical_live_tail`
/// and advancing `schema_meta.settled_tip_height` + `transaction_location.segment_kind`.
pub async fn persist_sealed_tail_transition(
    client: &mut Client,
    schema: &str,
    full_blocks: bool,
    bundles: &[SealedSegmentBundle],
    last_block: (BlockHeight, BlockHash, BlockHash),
) -> Result<(), BenchError> {
    let txn = client
        .transaction()
        .await
        .map_err(|err| postgres_error("starting settled transition txn", &err))?;
    copy_settled_bundles(&txn, schema, full_blocks, bundles).await?;

    let (settled_height, settled_hash, settled_prev) = last_block;
    let settled_h_pg = u32_to_i64(settled_height.value());
    txn.execute(
        &format!("DELETE FROM {schema}.canonical_live_tail WHERE height <= $1"),
        &[&settled_h_pg],
    )
    .await
    .map_err(|err| postgres_error("evicting sealed canonical_live_tail rows", &err))?;

    txn.execute(
        &format!(
            "UPDATE {schema}.transaction_location SET segment_kind = 1 WHERE block_height <= $1 AND segment_kind = 0"
        ),
        &[&settled_h_pg],
    )
    .await
    .map_err(|err| postgres_error("promoting transaction_location segment_kind", &err))?;

    txn.execute(
        &format!(
            "INSERT INTO {schema}.chain_event (epoch, event_kind, height, block_hash, previous_hash) VALUES (1, 'segment_seal', $1, $2, $3)"
        ),
        &[
            &settled_h_pg,
            &encode_internal_block_hash(settled_hash).as_slice(),
            &encode_internal_block_hash(settled_prev).as_slice(),
        ],
    )
    .await
    .map_err(|err| postgres_error("inserting segment_seal chain_event", &err))?;

    txn.execute(
        &format!("UPDATE {schema}.schema_meta SET settled_tip_height = $1 WHERE singleton"),
        &[&settled_h_pg],
    )
    .await
    .map_err(|err| postgres_error("updating settled_tip_height", &err))?;

    txn.commit()
        .await
        .map_err(|err| postgres_error("committing settled segment transition", &err))?;
    Ok(())
}
