//! Concrete canonical-replay round-trip benchmark for `RocksDB`.

use std::{
    fs,
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use rust_rocksdb::{
    Cache, ColumnFamilyDescriptor, DB, DBCompressionType, DEFAULT_COLUMN_FAMILY_NAME,
    IngestExternalFileOptions, IteratorMode, Options, SstFileWriter, WriteOptions,
};
use serde::{Deserialize, Serialize};
use zinder_core::{
    BlockHash, BlockHeight, CanonicalBlockFactsDigestVersion, CanonicalBlockFactsSequenceDigest,
    CanonicalBlockFactsSequenceDigestVersion, Network, NetworkUpgradeActivations,
};
use zinder_store::{
    RocksDbIoMode, RocksDbOpenRole, RocksDbResourceBudget, build_block_based_table_factory,
    open_bounded_rocksdb,
};

use crate::{
    BenchError,
    canonical_replay_storage::{
        CanonicalReplayRecord, CanonicalReplaySequenceAccumulator, CanonicalReplayStorageTimings,
        PersistedCanonicalReplayRow, prepare_fixture_segment,
    },
    fixture::FixtureManifest,
};

const CANDIDATE_NAME: &str = "rocksdb-canonical-replay-storage";
const CANONICAL_REPLAY_CONTRACT_IDENTITY: &str = "zinder-canonical-replay";
const CANONICAL_REPLAY_COLUMN_FAMILY: &str = "canonical_replay_storage";
const STORAGE_CONTROL_COLUMN_FAMILY: &str = "storage_control";
const COMPLETION_MARKER_KEY: &[u8] = b"canonical_replay_storage_complete";
const ROW_MAGIC: [u8; 4] = *b"ZCR1";
const COMPLETION_MARKER_FORMAT_VERSION: u16 = 1;

/// Candidate-owned physical schema version for persisted canonical replay rows.
pub const ROCKSDB_CANONICAL_REPLAY_SCHEMA_VERSION: u16 = 1;
/// Explicit compression used for candidate SSTs and the canonical-replays column family.
pub const ROCKSDB_CANONICAL_REPLAY_COMPRESSION: &str = "snappy";

/// Inputs for one fresh `RocksDB` canonical-replay round trip.
#[derive(Clone, Debug)]
pub struct RocksDbCanonicalReplayConfig {
    /// Immutable fixed-range fixture directory.
    pub fixture_directory: PathBuf,
    /// Fresh `RocksDB` directory to create and exclusively own.
    pub candidate_path: PathBuf,
    /// Maximum number of fixture blocks prepared concurrently.
    pub block_prepare_concurrency: NonZeroU32,
    /// Effective bounded `RocksDB` memory, WAL, file, and background-job budget.
    pub rocksdb_resource_budget: RocksDbResourceBudget,
}

/// Persisted canonical-replay evidence recomputed from one `RocksDB` candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RocksDbCanonicalReplayValidation {
    /// First persisted block height.
    pub first_height: BlockHeight,
    /// First persisted block hash in Zinder's internal byte order.
    pub first_hash: BlockHash,
    /// Last persisted block height.
    pub tip_height: BlockHeight,
    /// Last persisted block hash in Zinder's internal byte order.
    pub tip_hash: BlockHash,
    /// Number of persisted canonical replay rows.
    pub block_count: u64,
    /// Complete semantic replay encoding bytes stored across all rows.
    pub logical_replay_bytes: u64,
    /// Semantic replay format version decoded from every persisted row.
    pub replay_format_version: u32,
    /// Ordered digest recomputed from every decoded semantic replay record.
    pub sequence_digest: CanonicalBlockFactsSequenceDigest,
}

/// Measurements and validated evidence from one `RocksDB` canonical-replay round trip.
#[derive(Clone, Debug, PartialEq)]
pub struct RocksDbCanonicalReplayReport {
    /// Prepare concurrency applied to this run.
    pub block_prepare_concurrency: NonZeroU32,
    /// Per-phase elapsed time.
    pub timings: CanonicalReplayStorageTimings,
    /// Evidence recomputed from the persisted rows before publication.
    pub validation: RocksDbCanonicalReplayValidation,
    /// Semantic replay encoding bytes submitted as logical canonical replay records.
    pub logical_replay_bytes: u64,
    /// External-SST file size before ingestion.
    pub external_sst_bytes: u64,
    /// Final bytes under the closed `RocksDB` candidate directory.
    pub physical_storage_bytes: u64,
    /// Effective bounded resource budget used to open `RocksDB`.
    pub rocksdb_resource_budget: RocksDbResourceBudget,
    /// Direct or buffered filesystem I/O mode resolved for database access.
    pub database_io_mode: RocksDbIoMode,
    /// Filesystem I/O mode used while constructing the external SST.
    pub external_sst_io_mode: &'static str,
    /// Explicit compression used for physical canonical-replay blocks.
    pub compression: &'static str,
}

/// Builds, validates, and publishes a fresh `RocksDB` canonical-replay candidate.
///
/// The completion marker is written with WAL enabled and `sync=true` only after
/// every ingested row has been read back and matched to the fixture oracle.
#[allow(
    clippy::too_many_lines,
    reason = "the concrete lifecycle keeps its shared measured stage boundaries visible"
)]
pub async fn run_rocksdb_canonical_replay_storage(
    config: RocksDbCanonicalReplayConfig,
) -> Result<RocksDbCanonicalReplayReport, BenchError> {
    let wall_clock_started = Instant::now();
    let initialization_started = Instant::now();
    validate_resource_budget(config.rocksdb_resource_budget)?;

    let manifest = FixtureManifest::read(&config.fixture_directory)?;
    let preparation_context = CanonicalReplayPreparationContext {
        network: manifest.network_typed()?,
        activations: Arc::new(manifest.activations_typed()?),
        expected_block_digest_version: expected_block_digest_version(&manifest)?,
    };
    create_candidate_parent(&config.candidate_path)?;
    create_fresh_candidate_directory(&config.candidate_path)?;
    let external_sst_staging = ExternalSstStaging::create(&config.candidate_path)?;

    let bounded_open = open_bounded_rocksdb(
        RocksDbOpenRole::Primary {
            path: &config.candidate_path,
        },
        config.rocksdb_resource_budget,
        candidate_column_family_descriptors,
    )
    .map_err(|source| candidate_error(format!("open failed: {source}")))?;
    let database_io_mode = bounded_open.io_mode;
    let db = &bounded_open.db;
    reject_existing_completion_marker(db)?;
    let mut timings = CanonicalReplayStorageTimings {
        storage_initialization_seconds: initialization_started.elapsed().as_secs_f64(),
        ..CanonicalReplayStorageTimings::default()
    };

    let prepared_sst = build_canonical_replay_storage_sst(
        &config,
        &manifest,
        &external_sst_staging.file_path,
        &bounded_open.block_cache,
        preparation_context,
    )
    .await?;
    timings.replay_preparation_seconds = prepared_sst.replay_preparation_seconds;
    timings.replay_persistence_seconds = prepared_sst.replay_persistence_seconds;

    let canonical_replay_storage_cf = db
        .cf_handle(CANONICAL_REPLAY_COLUMN_FAMILY)
        .ok_or_else(|| missing_column_family(CANONICAL_REPLAY_COLUMN_FAMILY))?;
    let ingestion_started = Instant::now();
    let mut ingestion_options = IngestExternalFileOptions::default();
    ingestion_options.set_move_files(true);
    ingestion_options.set_snapshot_consistency(true);
    ingestion_options.set_allow_global_seqno(false);
    ingestion_options.set_allow_blocking_flush(false);
    db.ingest_external_file_cf_opts(
        &canonical_replay_storage_cf,
        &ingestion_options,
        vec![&external_sst_staging.file_path],
    )
    .map_err(|source| candidate_error(format!("external SST ingestion failed: {source}")))?;
    timings.replay_persistence_seconds += ingestion_started.elapsed().as_secs_f64();

    let validation_started = Instant::now();
    let validation = validate_persisted_rows(db, &manifest)?;
    if validation != prepared_sst.source_validation {
        return Err(candidate_error(
            "persisted canonical replay records differ from the prepared source sequence",
        ));
    }
    timings.validation_seconds = validation_started.elapsed().as_secs_f64();

    let publication_started = Instant::now();
    publish_completion_marker(db, &manifest, validation)?;
    timings.publication_seconds = publication_started.elapsed().as_secs_f64();

    let fresh_reader_validation_started = Instant::now();
    drop(canonical_replay_storage_cf);
    drop(bounded_open);
    let fresh_reader_validation = validate_rocksdb_canonical_replay_storage_with_fresh_open(
        &config.candidate_path,
        &config.fixture_directory,
        config.rocksdb_resource_budget,
    )?;
    timings.fresh_reader_validation_seconds =
        fresh_reader_validation_started.elapsed().as_secs_f64();
    if fresh_reader_validation != validation {
        return Err(candidate_error(
            "fresh-open validation does not equal pre-publication validation",
        ));
    }
    let storage_measurement_started = Instant::now();
    let physical_storage_bytes = candidate_directory_bytes(&config.candidate_path)?;
    timings.storage_measurement_seconds = storage_measurement_started.elapsed().as_secs_f64();
    timings.wall_clock_seconds = wall_clock_started.elapsed().as_secs_f64();

    Ok(RocksDbCanonicalReplayReport {
        block_prepare_concurrency: config.block_prepare_concurrency,
        timings,
        validation,
        logical_replay_bytes: validation.logical_replay_bytes,
        external_sst_bytes: prepared_sst.external_sst_bytes,
        physical_storage_bytes,
        rocksdb_resource_budget: config.rocksdb_resource_budget,
        database_io_mode,
        external_sst_io_mode: "buffered",
        compression: ROCKSDB_CANONICAL_REPLAY_COMPRESSION,
    })
}

struct PreparedCanonicalReplaySst {
    source_validation: RocksDbCanonicalReplayValidation,
    replay_preparation_seconds: f64,
    replay_persistence_seconds: f64,
    external_sst_bytes: u64,
}

struct CanonicalReplayPreparationContext {
    network: Network,
    activations: Arc<NetworkUpgradeActivations>,
    expected_block_digest_version: CanonicalBlockFactsDigestVersion,
}

async fn build_canonical_replay_storage_sst(
    config: &RocksDbCanonicalReplayConfig,
    manifest: &FixtureManifest,
    external_sst_path: &Path,
    block_cache: &Cache,
    preparation_context: CanonicalReplayPreparationContext,
) -> Result<PreparedCanonicalReplaySst, BenchError> {
    let CanonicalReplayPreparationContext {
        network,
        activations,
        expected_block_digest_version,
    } = preparation_context;
    let persistence_started = Instant::now();
    let mut sst_options = Options::default();
    sst_options.set_compression_type(DBCompressionType::Snappy);
    sst_options.set_block_based_table_factory(&build_block_based_table_factory(block_cache));
    let mut sst_writer = SstFileWriter::create(&sst_options);
    sst_writer
        .open(external_sst_path)
        .map_err(|source| candidate_error(format!("external SST open failed: {source}")))?;

    let mut replay_preparation_seconds = 0.0;
    let mut replay_persistence_seconds = persistence_started.elapsed().as_secs_f64();
    let mut source_sequence = CanonicalReplaySequenceAccumulator::new();
    for descriptor in &manifest.segments {
        let preparation_started = Instant::now();
        let records = prepare_fixture_segment(
            &config.fixture_directory,
            descriptor,
            network,
            Arc::clone(&activations),
            config.block_prepare_concurrency,
        )
        .await?;
        for record in &records {
            validate_block_digest_version(record, expected_block_digest_version)?;
            source_sequence.append(record)?;
        }
        replay_preparation_seconds += preparation_started.elapsed().as_secs_f64();

        let persistence_started = Instant::now();
        for record in records {
            let replay_key = canonical_replay_key(record.height);
            let row_encoding = encode_canonical_replay_storage_row(&record)?;
            sst_writer.put(replay_key, row_encoding).map_err(|source| {
                candidate_error(format!("external SST write failed: {source}"))
            })?;
        }
        replay_persistence_seconds += persistence_started.elapsed().as_secs_f64();
    }

    let finish_started = Instant::now();
    sst_writer
        .finish()
        .map_err(|source| candidate_error(format!("external SST finish failed: {source}")))?;
    replay_persistence_seconds += finish_started.elapsed().as_secs_f64();
    let external_sst_metadata_started = Instant::now();
    let external_sst_bytes = fs::metadata(external_sst_path)
        .map_err(|source| BenchError::io(external_sst_path, source))?
        .len();
    replay_persistence_seconds += external_sst_metadata_started.elapsed().as_secs_f64();
    let source_validation_started = Instant::now();
    let source_validation = validate_sequence_against_manifest(source_sequence, manifest)?;
    replay_preparation_seconds += source_validation_started.elapsed().as_secs_f64();
    Ok(PreparedCanonicalReplaySst {
        source_validation,
        replay_preparation_seconds,
        replay_persistence_seconds,
        external_sst_bytes,
    })
}

/// Reopens a completed `RocksDB` candidate and independently validates its rows
/// and synchronous completion marker against the fixture manifest.
pub fn validate_rocksdb_canonical_replay_storage_with_fresh_open(
    candidate_path: &Path,
    fixture_directory: &Path,
    rocksdb_resource_budget: RocksDbResourceBudget,
) -> Result<RocksDbCanonicalReplayValidation, BenchError> {
    validate_resource_budget(rocksdb_resource_budget)?;
    let manifest = FixtureManifest::read(fixture_directory)?;
    validate_candidate_column_families(candidate_path)?;
    let bounded_open = open_bounded_rocksdb(
        RocksDbOpenRole::Primary {
            path: candidate_path,
        },
        rocksdb_resource_budget,
        candidate_column_family_descriptors,
    )
    .map_err(|source| candidate_error(format!("reopen failed: {source}")))?;
    let completion_marker = read_completion_marker(&bounded_open.db)?;
    let validation = validate_persisted_rows(&bounded_open.db, &manifest)?;
    validate_completion_marker(&completion_marker, &manifest, validation)?;
    Ok(validation)
}

fn candidate_column_family_descriptors(
    block_cache: &Cache,
    resource_budget: RocksDbResourceBudget,
) -> Vec<ColumnFamilyDescriptor> {
    [
        CANONICAL_REPLAY_COLUMN_FAMILY,
        STORAGE_CONTROL_COLUMN_FAMILY,
    ]
    .into_iter()
    .map(|name| {
        let mut options = Options::default();
        options.set_compression_type(DBCompressionType::Snappy);
        options.set_block_based_table_factory(&build_block_based_table_factory(block_cache));
        options.set_write_buffer_size(
            usize::try_from(resource_budget.write_buffer_bytes).unwrap_or(usize::MAX),
        );
        options.set_max_write_buffer_number(resource_budget.max_write_buffer_count);
        ColumnFamilyDescriptor::new(name, options)
    })
    .collect()
}

fn validate_persisted_rows(
    db: &DB,
    manifest: &FixtureManifest,
) -> Result<RocksDbCanonicalReplayValidation, BenchError> {
    let expected_block_digest_version = expected_block_digest_version(manifest)?;
    let canonical_replay_storage_cf = db
        .cf_handle(CANONICAL_REPLAY_COLUMN_FAMILY)
        .ok_or_else(|| missing_column_family(CANONICAL_REPLAY_COLUMN_FAMILY))?;
    let mut sequence = CanonicalReplaySequenceAccumulator::new();
    for row in db.iterator_cf(&canonical_replay_storage_cf, IteratorMode::Start) {
        let (key, row_encoding) = row.map_err(|source| {
            candidate_error(format!("canonical replay iteration failed: {source}"))
        })?;
        let key_height = decode_canonical_replay_key(&key)?;
        let record = decode_canonical_replay_storage_row(&row_encoding)?;
        if record.height != key_height {
            return Err(candidate_error(format!(
                "canonical replay key height {} does not match row height {}",
                key_height.value(),
                record.height.value()
            )));
        }
        validate_block_digest_version(&record, expected_block_digest_version)?;
        sequence.append(&record)?;
    }
    validate_sequence_against_manifest(sequence, manifest)
}

fn validate_sequence_against_manifest(
    sequence: CanonicalReplaySequenceAccumulator,
    manifest: &FixtureManifest,
) -> Result<RocksDbCanonicalReplayValidation, BenchError> {
    let position = sequence.position();
    let replay_format_version = position
        .replay_format_version
        .ok_or_else(|| candidate_error("canonical replay sequence has no replay format version"))?;
    let sequence_digest = sequence.finish();
    let expected_sequence_version = CanonicalBlockFactsSequenceDigestVersion::try_from(
        manifest
            .canonical_block_facts_digest_evidence
            .sequence_digest_version,
    )
    .map_err(|source| candidate_error(source.to_string()))?;
    let expected_sequence_digest = decode_sha256_hex(
        &manifest
            .canonical_block_facts_digest_evidence
            .sequence_digest_sha256,
        "fixture sequence digest",
    )?;
    if sequence_digest.version() != expected_sequence_version {
        return Err(candidate_error(format!(
            "sequence digest version {} does not match fixture version {}",
            sequence_digest.version().value(),
            expected_sequence_version.value()
        )));
    }
    if sequence_digest.block_count() != manifest.canonical_block_facts_digest_evidence.block_count {
        return Err(candidate_error(format!(
            "persisted block count {} does not match fixture digest count {}",
            sequence_digest.block_count(),
            manifest.canonical_block_facts_digest_evidence.block_count
        )));
    }
    if sequence_digest.as_bytes() != expected_sequence_digest {
        return Err(candidate_error(
            "persisted canonical replay sequence digest does not match fixture oracle",
        ));
    }

    let first_height = position
        .first_height
        .ok_or_else(|| candidate_error("canonical replay sequence is empty"))?;
    let first_hash = position
        .first_hash
        .ok_or_else(|| candidate_error("canonical replay sequence has no first hash"))?;
    let tip_height = position
        .tip_height
        .ok_or_else(|| candidate_error("canonical replay sequence has no tip height"))?;
    let tip_hash = position
        .tip_hash
        .ok_or_else(|| candidate_error("canonical replay sequence has no tip hash"))?;
    let manifest_tip = manifest.tip_id()?;
    if first_height.value() != manifest.from_height || tip_height.value() != manifest.to_height {
        return Err(candidate_error(format!(
            "persisted range {}..={} does not match fixture range {}..={}",
            first_height.value(),
            tip_height.value(),
            manifest.from_height,
            manifest.to_height
        )));
    }
    if position.block_count != u64::from(manifest.block_count) {
        return Err(candidate_error(format!(
            "persisted row count {} does not match fixture block count {}",
            position.block_count, manifest.block_count
        )));
    }
    if tip_hash != manifest_tip.hash {
        return Err(candidate_error(format!(
            "persisted tip hash at height {} does not match fixture tip",
            tip_height.value()
        )));
    }

    Ok(RocksDbCanonicalReplayValidation {
        first_height,
        first_hash,
        tip_height,
        tip_hash,
        block_count: position.block_count,
        logical_replay_bytes: position.logical_replay_bytes,
        replay_format_version,
        sequence_digest,
    })
}

fn validate_block_digest_version(
    record: &CanonicalReplayRecord,
    expected_version: CanonicalBlockFactsDigestVersion,
) -> Result<(), BenchError> {
    if record.digest.version() != expected_version {
        return Err(candidate_error(format!(
            "block {} digest version {} does not match fixture version {}",
            record.height.value(),
            record.digest.version().value(),
            expected_version.value()
        )));
    }
    Ok(())
}

fn expected_block_digest_version(
    manifest: &FixtureManifest,
) -> Result<CanonicalBlockFactsDigestVersion, BenchError> {
    CanonicalBlockFactsDigestVersion::try_from(
        manifest
            .canonical_block_facts_digest_evidence
            .block_digest_version,
    )
    .map_err(|source| candidate_error(source.to_string()))
}

fn canonical_replay_key(height: BlockHeight) -> [u8; 4] {
    height.value().to_be_bytes()
}

fn decode_canonical_replay_key(bytes: &[u8]) -> Result<BlockHeight, BenchError> {
    let encoded: [u8; 4] = bytes.try_into().map_err(|_| {
        candidate_error(format!(
            "canonical replay key must be 4 bytes, observed {}",
            bytes.len()
        ))
    })?;
    Ok(BlockHeight::new(u32::from_be_bytes(encoded)))
}

fn encode_canonical_replay_storage_row(
    record: &CanonicalReplayRecord,
) -> Result<Vec<u8>, BenchError> {
    let replay_envelope_len = u64::try_from(record.replay_envelope_bytes.len())
        .map_err(|_| candidate_error("canonical replay replay encoding exceeds u64::MAX bytes"))?;
    let mut encoded =
        Vec::with_capacity(120_usize.saturating_add(record.replay_envelope_bytes.len()));
    encoded.extend_from_slice(&ROW_MAGIC);
    encoded.extend_from_slice(&ROCKSDB_CANONICAL_REPLAY_SCHEMA_VERSION.to_le_bytes());
    encoded.extend_from_slice(&record.digest.version().value().to_le_bytes());
    encoded.extend_from_slice(&record.height.value().to_le_bytes());
    encoded.extend_from_slice(&record.block_hash.as_bytes());
    encoded.extend_from_slice(&record.parent_hash.as_bytes());
    encoded.extend_from_slice(&record.transaction_count.to_le_bytes());
    encoded.extend_from_slice(&record.digest.as_bytes());
    encoded.extend_from_slice(&replay_envelope_len.to_le_bytes());
    encoded.extend_from_slice(&record.replay_envelope_bytes);
    Ok(encoded)
}

fn decode_canonical_replay_storage_row(bytes: &[u8]) -> Result<CanonicalReplayRecord, BenchError> {
    let mut decoder = CanonicalReplayRowDecoder::new(bytes);
    if decoder.read_array::<4>()? != ROW_MAGIC {
        return Err(candidate_error("canonical replay row has invalid magic"));
    }
    let storage_schema_version = decoder.read_u16()?;
    if storage_schema_version != ROCKSDB_CANONICAL_REPLAY_SCHEMA_VERSION {
        return Err(candidate_error(format!(
            "unsupported canonical replay row storage schema version {storage_schema_version}"
        )));
    }
    let digest_version = CanonicalBlockFactsDigestVersion::try_from(decoder.read_u16()?)
        .map_err(|source| candidate_error(source.to_string()))?;
    let height = BlockHeight::new(decoder.read_u32()?);
    let block_hash = BlockHash::from_bytes(decoder.read_array::<32>()?);
    let parent_hash = BlockHash::from_bytes(decoder.read_array::<32>()?);
    let transaction_count = decoder.read_u32()?;
    let stored_digest = decoder.read_array::<32>()?;
    let replay_envelope_len = usize::try_from(decoder.read_u64()?)
        .map_err(|_| candidate_error("canonical replay replay length exceeds usize::MAX"))?;
    let replay_envelope_bytes = decoder.read_bytes(replay_envelope_len)?.to_vec();
    decoder.reject_trailing_bytes()?;
    CanonicalReplayRecord::from_persisted(PersistedCanonicalReplayRow {
        height,
        block_hash,
        parent_hash,
        transaction_count,
        digest_version,
        stored_digest,
        replay_envelope_bytes,
    })
}

fn publish_completion_marker(
    db: &DB,
    manifest: &FixtureManifest,
    validation: RocksDbCanonicalReplayValidation,
) -> Result<(), BenchError> {
    let storage_control_cf = db
        .cf_handle(STORAGE_CONTROL_COLUMN_FAMILY)
        .ok_or_else(|| missing_column_family(STORAGE_CONTROL_COLUMN_FAMILY))?;
    let marker = CompletionMarker::from_validated_sequence(manifest, validation)?;
    let encoded = serde_json::to_vec(&marker)?;
    let mut write_options = WriteOptions::default();
    write_options.disable_wal(false);
    write_options.set_sync(true);
    db.put_cf_opt(
        &storage_control_cf,
        COMPLETION_MARKER_KEY,
        encoded,
        &write_options,
    )
    .map_err(|source| candidate_error(format!("completion marker publication failed: {source}")))
}

fn reject_existing_completion_marker(db: &DB) -> Result<(), BenchError> {
    let storage_control_cf = db
        .cf_handle(STORAGE_CONTROL_COLUMN_FAMILY)
        .ok_or_else(|| missing_column_family(STORAGE_CONTROL_COLUMN_FAMILY))?;
    let marker = db
        .get_cf(&storage_control_cf, COMPLETION_MARKER_KEY)
        .map_err(|source| candidate_error(format!("completion marker read failed: {source}")))?;
    if marker.is_some() {
        return Err(candidate_error(
            "fresh RocksDB candidate already contains a completion marker",
        ));
    }
    Ok(())
}

fn validate_completion_marker(
    observed: &CompletionMarker,
    manifest: &FixtureManifest,
    validation: RocksDbCanonicalReplayValidation,
) -> Result<(), BenchError> {
    let expected = CompletionMarker::from_validated_sequence(manifest, validation)?;
    if *observed != expected {
        return Err(candidate_error(
            "completion marker does not match the fixture and persisted canonical replay records",
        ));
    }
    Ok(())
}

fn read_completion_marker(db: &DB) -> Result<CompletionMarker, BenchError> {
    let storage_control_cf = db
        .cf_handle(STORAGE_CONTROL_COLUMN_FAMILY)
        .ok_or_else(|| missing_column_family(STORAGE_CONTROL_COLUMN_FAMILY))?;
    let encoded = db
        .get_cf(&storage_control_cf, COMPLETION_MARKER_KEY)
        .map_err(|source| candidate_error(format!("completion marker read failed: {source}")))?
        .ok_or_else(|| candidate_error("completion marker is absent"))?;
    serde_json::from_slice(&encoded)
        .map_err(|source| candidate_error(format!("completion marker decode failed: {source}")))
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CompletionMarker {
    contract_identity: String,
    marker_format_version: u16,
    storage_schema_version: u16,
    fixture_manifest_sha256: String,
    network: String,
    first_height: u32,
    first_hash_hex: String,
    tip_height: u32,
    tip_hash_hex: String,
    block_count: u64,
    block_digest_version: u16,
    replay_format_version: u32,
    sequence_digest_version: u16,
    sequence_digest_sha256: String,
    logical_replay_bytes: u64,
}

impl CompletionMarker {
    fn from_validated_sequence(
        manifest: &FixtureManifest,
        validation: RocksDbCanonicalReplayValidation,
    ) -> Result<Self, BenchError> {
        Ok(Self {
            contract_identity: CANONICAL_REPLAY_CONTRACT_IDENTITY.to_owned(),
            marker_format_version: COMPLETION_MARKER_FORMAT_VERSION,
            storage_schema_version: ROCKSDB_CANONICAL_REPLAY_SCHEMA_VERSION,
            fixture_manifest_sha256: manifest.digest_sha256()?,
            network: manifest.network.clone(),
            first_height: validation.first_height.value(),
            first_hash_hex: hex::encode(validation.first_hash.as_bytes()),
            tip_height: validation.tip_height.value(),
            tip_hash_hex: hex::encode(validation.tip_hash.as_bytes()),
            block_count: validation.block_count,
            block_digest_version: manifest
                .canonical_block_facts_digest_evidence
                .block_digest_version,
            replay_format_version: validation.replay_format_version,
            sequence_digest_version: validation.sequence_digest.version().value(),
            sequence_digest_sha256: hex::encode(validation.sequence_digest.as_bytes()),
            logical_replay_bytes: validation.logical_replay_bytes,
        })
    }
}

struct CanonicalReplayRowDecoder<'row> {
    bytes: &'row [u8],
    position: usize,
}

impl<'row> CanonicalReplayRowDecoder<'row> {
    const fn new(bytes: &'row [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn read_u16(&mut self) -> Result<u16, BenchError> {
        Ok(u16::from_le_bytes(self.read_array::<2>()?))
    }

    fn read_u32(&mut self) -> Result<u32, BenchError> {
        Ok(u32::from_le_bytes(self.read_array::<4>()?))
    }

    fn read_u64(&mut self) -> Result<u64, BenchError> {
        Ok(u64::from_le_bytes(self.read_array::<8>()?))
    }

    fn read_array<const LENGTH: usize>(&mut self) -> Result<[u8; LENGTH], BenchError> {
        let bytes = self.read_bytes(LENGTH)?;
        let mut array = [0_u8; LENGTH];
        array.copy_from_slice(bytes);
        Ok(array)
    }

    fn read_bytes(&mut self, length: usize) -> Result<&'row [u8], BenchError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| candidate_error("canonical replay row offset overflow"))?;
        let bytes = self.bytes.get(self.position..end).ok_or_else(|| {
            candidate_error(format!(
                "canonical replay row ended at byte {}, expected at least {end}",
                self.bytes.len()
            ))
        })?;
        self.position = end;
        Ok(bytes)
    }

    fn reject_trailing_bytes(&self) -> Result<(), BenchError> {
        if self.position != self.bytes.len() {
            return Err(candidate_error(format!(
                "canonical replay row contains {} trailing bytes",
                self.bytes.len().saturating_sub(self.position)
            )));
        }
        Ok(())
    }
}

struct ExternalSstStaging {
    directory_path: PathBuf,
    file_path: PathBuf,
}

impl ExternalSstStaging {
    fn create(candidate_path: &Path) -> Result<Self, BenchError> {
        let candidate_name = candidate_path
            .file_name()
            .ok_or_else(|| BenchError::invalid_argument("candidate_path must name a directory"))?;
        let mut staging_name = candidate_name.to_os_string();
        staging_name.push(".external-sst");
        let directory_path = candidate_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(staging_name);
        fs::create_dir(&directory_path)
            .map_err(|source| BenchError::io(&directory_path, source))?;
        let file_path = directory_path.join("canonical-replay-storage.sst");
        Ok(Self {
            directory_path,
            file_path,
        })
    }
}

impl Drop for ExternalSstStaging {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.file_path);
        let _ = fs::remove_dir(&self.directory_path);
    }
}

fn create_candidate_parent(candidate_path: &Path) -> Result<(), BenchError> {
    let Some(parent) = candidate_path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    fs::create_dir_all(parent).map_err(|source| BenchError::io(parent, source))
}

fn create_fresh_candidate_directory(candidate_path: &Path) -> Result<(), BenchError> {
    match fs::create_dir(candidate_path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
            Err(BenchError::invalid_argument(format!(
                "candidate_path {} already exists; canonical-replay round trips require a fresh path",
                candidate_path.display()
            )))
        }
        Err(source) => Err(BenchError::io(candidate_path, source)),
    }
}

fn validate_resource_budget(resource_budget: RocksDbResourceBudget) -> Result<(), BenchError> {
    resource_budget
        .validate()
        .map_err(BenchError::invalid_argument)
}

fn validate_candidate_column_families(candidate_path: &Path) -> Result<(), BenchError> {
    let mut observed = DB::list_cf(&Options::default(), candidate_path)
        .map_err(|source| candidate_error(format!("column-family discovery failed: {source}")))?;
    observed.sort_unstable();
    let mut expected = vec![
        DEFAULT_COLUMN_FAMILY_NAME.to_owned(),
        CANONICAL_REPLAY_COLUMN_FAMILY.to_owned(),
        STORAGE_CONTROL_COLUMN_FAMILY.to_owned(),
    ];
    expected.sort_unstable();
    if observed != expected {
        return Err(candidate_error(format!(
            "candidate column families {observed:?} do not match required schema {expected:?}"
        )));
    }
    Ok(())
}

fn candidate_directory_bytes(path: &Path) -> Result<u64, BenchError> {
    let entries = fs::read_dir(path).map_err(|source| BenchError::io(path, source))?;
    let mut bytes = 0_u64;
    for entry in entries {
        let entry = entry.map_err(|source| BenchError::io(path, source))?;
        let entry_path = entry.path();
        let metadata = entry
            .metadata()
            .map_err(|source| BenchError::io(&entry_path, source))?;
        let entry_bytes = if metadata.is_dir() {
            candidate_directory_bytes(&entry_path)?
        } else {
            metadata.len()
        };
        bytes = bytes.saturating_add(entry_bytes);
    }
    Ok(bytes)
}

fn decode_sha256_hex(encoded: &str, field: &str) -> Result<[u8; 32], BenchError> {
    let bytes = hex::decode(encoded)
        .map_err(|source| candidate_error(format!("{field} is not valid hex: {source}")))?;
    bytes
        .try_into()
        .map_err(|_| candidate_error(format!("{field} must contain 32 bytes")))
}

fn missing_column_family(name: &'static str) -> BenchError {
    candidate_error(format!("required column family {name} is absent"))
}

fn candidate_error(reason: impl Into<String>) -> BenchError {
    BenchError::canonical_replay_storage_candidate(CANDIDATE_NAME, reason)
}
