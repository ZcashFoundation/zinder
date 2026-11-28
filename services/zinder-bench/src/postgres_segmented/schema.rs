//! Schema constants, DDL builders, and schema-name validation for the
//! segmented `PostgreSQL` benchmark arm.

use crate::error::BenchError;

/// Persisted schema contract revision for `postgres-segmented-storage`.
pub const POSTGRES_SEGMENTED_SCHEMA_VERSION: u16 = 2;

/// Default schema name isolated from application and `v1` diagnostic tables.
pub const DEFAULT_POSTGRES_SEGMENTED_SCHEMA: &str = "zinder_bench_segmented";

/// Stable contract identity stamped into `{schema}.schema_meta`.
pub const POSTGRES_SEGMENTED_CONTRACT_IDENTITY: &str =
    "postgres-segmented-canonical-wallet-benchmark";

/// Candidate identifier stamped into benchmark reports and errors.
pub const POSTGRES_SEGMENTED_CANDIDATE_ID: &str = "postgres-segmented-storage";

/// Hard upper bound for any settled segment payload (`256 MiB`).
pub const MAX_SETTLED_SEGMENT_BYTES: u64 = 256 * 1024 * 1024;

/// Default block span per settled segment sealed by the benchmark harness.
pub const DEFAULT_SETTLED_SEGMENT_BLOCKS: u32 = 1_000;

/// Default maximum block count retained in `{schema}.canonical_live_tail`.
pub const DEFAULT_LIVE_TAIL_MAX_BLOCKS: u32 = 128;

/// Writer identity stamped into `{schema}.canonical_writer_fence`.
pub const BENCHMARK_WRITER_ID: &str = "zinder-bench-segmented-writer";

/// Single-writer fence lease lifetime in milliseconds during active writes.
pub const BENCHMARK_LEASE_DURATION_MS: i64 = 30_000;

const MAX_POSTGRES_IDENTIFIER_LEN: usize = 63;

/// Validates an operator-supplied PostgreSQL schema identifier before DDL interpolation.
///
/// Only lowercase ASCII letters, ASCII digits, and underscores are accepted so
/// unquoted schema qualification behaves identically in SQL and catalog views.
pub fn validate_schema_name(schema_name: &str) -> Result<&str, BenchError> {
    if schema_name.is_empty() || schema_name.len() > MAX_POSTGRES_IDENTIFIER_LEN {
        return Err(BenchError::invalid_argument(format!(
            "postgres-segmented schema name `{schema_name}` must be 1..={MAX_POSTGRES_IDENTIFIER_LEN} bytes"
        )));
    }
    let mut chars = schema_name.chars();
    let first = chars.next().ok_or_else(|| {
        BenchError::invalid_argument("postgres-segmented schema name must not be empty")
    })?;
    if !(first.is_ascii_lowercase() || first == '_') {
        return Err(BenchError::invalid_argument(format!(
            "postgres-segmented schema name `{schema_name}` must start with lowercase ASCII letter or `_`"
        )));
    }
    if !chars.all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_') {
        return Err(BenchError::invalid_argument(format!(
            "postgres-segmented schema name `{schema_name}` may only contain lowercase ASCII letters, digits, and `_`"
        )));
    }
    if schema_name.starts_with("pg_") || schema_name == "information_schema" {
        return Err(BenchError::invalid_argument(format!(
            "postgres-segmented schema name `{schema_name}` targets a reserved PostgreSQL namespace"
        )));
    }
    Ok(schema_name)
}

/// Builds DDL statements that create an isolated schema and unindexed bulk-load
/// tables with `BYTEA STORAGE EXTERNAL` enabled on settled segments and live tail blobs.
pub fn build_create_schema_sql(schema_name: &str) -> Result<String, BenchError> {
    let schema = validate_schema_name(schema_name)?;
    Ok([
        create_core_control_tables_sql(schema),
        create_settled_segment_table_sql(schema),
        create_canonical_tail_and_index_tables_sql(schema),
        create_mempool_and_wallet_tables_sql(schema),
    ]
    .join("\n"))
}

fn create_core_control_tables_sql(schema: &str) -> String {
    format!(
        r"
CREATE SCHEMA {schema};

CREATE TABLE {schema}.schema_meta (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    contract_identity TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version = {POSTGRES_SEGMENTED_SCHEMA_VERSION}),
    network TEXT NOT NULL,
    artifact_schema_version INTEGER NOT NULL,
    full_blocks BOOLEAN NOT NULL,
    max_segment_bytes BIGINT NOT NULL CHECK (max_segment_bytes > 0 AND max_segment_bytes <= {MAX_SETTLED_SEGMENT_BYTES}),
    settled_segment_blocks BIGINT NOT NULL CHECK (settled_segment_blocks > 0),
    live_tail_max_blocks BIGINT NOT NULL CHECK (live_tail_max_blocks > 0),
    completed BOOLEAN NOT NULL DEFAULT FALSE,
    first_height BIGINT NOT NULL,
    settled_tip_height BIGINT,
    canonical_tip_height BIGINT NOT NULL,
    canonical_tip_hash BYTEA NOT NULL,
    expected_sequence_digest TEXT NOT NULL,
    fixture_format_version INTEGER NOT NULL,
    fixture_manifest_sha256 TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE {schema}.canonical_writer_fence (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    writer_epoch BIGINT NOT NULL CHECK (writer_epoch >= 1),
    writer_id TEXT NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE {schema}.chain_epoch (
    epoch BIGINT PRIMARY KEY,
    network TEXT NOT NULL,
    genesis_hash BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE {schema}.chain_event (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    epoch BIGINT NOT NULL REFERENCES {schema}.chain_epoch(epoch),
    event_kind TEXT NOT NULL CHECK (event_kind IN ('bulk_seed', 'tip_append', 'segment_seal', 'reorg_rollback')),
    height BIGINT NOT NULL,
    block_hash BYTEA NOT NULL,
    previous_hash BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
"
    )
}

fn create_settled_segment_table_sql(schema: &str) -> String {
    format!(
        r"
CREATE TABLE {schema}.canonical_settled_segment (
    segment_kind TEXT NOT NULL CHECK (segment_kind IN ('shielded', 'supplement', 'raw_block')),
    start_height BIGINT NOT NULL,
    end_height BIGINT NOT NULL,
    artifact_schema_version INTEGER NOT NULL,
    compression_codec TEXT NOT NULL CHECK (compression_codec IN ('none', 'zstd')),
    chunk_blocks INTEGER CHECK (
        (segment_kind = 'supplement' AND chunk_blocks = 16)
        OR (segment_kind <> 'supplement' AND chunk_blocks IS NULL)
    ),
    per_block_framing BOOLEAN NOT NULL CHECK (
        (segment_kind = 'raw_block' AND per_block_framing)
        OR (segment_kind <> 'raw_block' AND NOT per_block_framing)
    ),
    segment_sha256 BYTEA NOT NULL,
    index_sha256 BYTEA NOT NULL,
    segment_byte_len BIGINT NOT NULL CHECK (segment_byte_len > 0 AND segment_byte_len <= {MAX_SETTLED_SEGMENT_BYTES}),
    segment_bytes BYTEA NOT NULL,
    PRIMARY KEY (segment_kind, start_height, end_height),
    CHECK (start_height >= 0 AND end_height >= start_height),
    CHECK (octet_length(segment_bytes) = segment_byte_len)
);
ALTER TABLE {schema}.canonical_settled_segment ALTER COLUMN segment_bytes SET STORAGE EXTERNAL;
"
    )
}

fn create_canonical_tail_and_index_tables_sql(schema: &str) -> String {
    format!(
        r"
CREATE TABLE {schema}.canonical_live_tail (
    height BIGINT PRIMARY KEY,
    block_hash BYTEA NOT NULL UNIQUE,
    previous_hash BYTEA NOT NULL,
    epoch BIGINT NOT NULL,
    compact_block_bytes BYTEA NOT NULL,
    canonical_facts_bytes BYTEA NOT NULL,
    raw_block_bytes BYTEA
);
ALTER TABLE {schema}.canonical_live_tail ALTER COLUMN raw_block_bytes SET STORAGE EXTERNAL;

CREATE TABLE {schema}.block_header (
    height BIGINT NOT NULL,
    block_hash BYTEA NOT NULL,
    previous_hash BYTEA NOT NULL,
    epoch BIGINT NOT NULL,
    version BIGINT NOT NULL,
    time_unix BIGINT NOT NULL,
    bits BIGINT NOT NULL,
    nonce BYTEA NOT NULL,
    merkle_root BYTEA NOT NULL,
    final_sapling_root BYTEA NOT NULL,
    sapling_commitment_tree_size BIGINT NOT NULL,
    orchard_commitment_tree_size BIGINT NOT NULL,
    ironwood_commitment_tree_size BIGINT NOT NULL,
    block_size_bytes BIGINT NOT NULL,
    transaction_count BIGINT NOT NULL
);

CREATE TABLE {schema}.block_hash_index (
    block_hash BYTEA NOT NULL,
    height BIGINT NOT NULL
);

CREATE TABLE {schema}.tree_state_checkpoint (
    height BIGINT PRIMARY KEY,
    block_hash BYTEA NOT NULL UNIQUE,
    sapling_tree_size BIGINT NOT NULL,
    orchard_tree_size BIGINT NOT NULL,
    ironwood_tree_size BIGINT NOT NULL,
    commitment_bytes BYTEA NOT NULL
);

CREATE TABLE {schema}.transaction_location (
    txid BYTEA NOT NULL,
    block_height BIGINT NOT NULL,
    block_hash BYTEA NOT NULL,
    tx_index BIGINT NOT NULL,
    segment_kind SMALLINT NOT NULL DEFAULT 1
);

CREATE TABLE {schema}.subtree_root (
    pool SMALLINT NOT NULL CHECK (pool IN (1, 2, 3)),
    subtree_index BIGINT NOT NULL,
    root_hash BYTEA NOT NULL,
    completing_block_height BIGINT NOT NULL,
    completing_block_hash BYTEA NOT NULL,
    PRIMARY KEY (pool, subtree_index)
);
"
    )
}

fn create_mempool_and_wallet_tables_sql(schema: &str) -> String {
    format!(
        r"
CREATE TABLE {schema}.mempool_active_entry (
    txid BYTEA PRIMARY KEY,
    raw_tx_bytes BYTEA NOT NULL,
    tx_byte_len BIGINT NOT NULL CHECK (tx_byte_len > 0),
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE {schema}.mempool_event (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_kind TEXT NOT NULL CHECK (event_kind IN ('admit', 'evict_mined', 'evict_expired')),
    txid BYTEA NOT NULL,
    block_height BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE {schema}.wallet_utxo (
    script_hash BYTEA NOT NULL,
    txid BYTEA NOT NULL,
    output_index BIGINT NOT NULL,
    block_height BIGINT NOT NULL,
    value_zatoshis BIGINT NOT NULL CHECK (value_zatoshis >= 0),
    spent_by_txid BYTEA,
    spent_at_height BIGINT
);

CREATE TABLE {schema}.wallet_address_tx (
    script_hash BYTEA NOT NULL,
    block_height BIGINT NOT NULL,
    tx_index BIGINT NOT NULL,
    txid BYTEA NOT NULL,
    net_delta_zatoshis BIGINT NOT NULL
);
"
    )
}

/// Builds finalization DDL that adds primary keys and secondary lookup indexes
/// after bulk `COPY` ingest completes.
pub fn build_finalize_schema_sql(schema_name: &str) -> Result<String, BenchError> {
    let schema = validate_schema_name(schema_name)?;
    Ok(format!(
        r"
ALTER TABLE {schema}.block_header
    ADD CONSTRAINT block_header_pkey PRIMARY KEY (height),
    ADD CONSTRAINT block_header_hash_key UNIQUE (block_hash);

ALTER TABLE {schema}.block_hash_index
    ADD CONSTRAINT block_hash_index_pkey PRIMARY KEY (block_hash),
    ADD CONSTRAINT block_hash_index_height_key UNIQUE (height);

CREATE UNIQUE INDEX canonical_settled_segment_end_height_idx
    ON {schema}.canonical_settled_segment (segment_kind, end_height DESC, start_height ASC);

ALTER TABLE {schema}.transaction_location
    ADD CONSTRAINT transaction_location_pkey PRIMARY KEY (txid, block_height, tx_index);

CREATE INDEX transaction_location_height_idx
    ON {schema}.transaction_location (block_height, tx_index);

ALTER TABLE {schema}.wallet_utxo
    ADD CONSTRAINT wallet_utxo_pkey PRIMARY KEY (txid, output_index);

CREATE INDEX wallet_utxo_script_unspent_idx
    ON {schema}.wallet_utxo (script_hash, block_height, txid, output_index)
    WHERE spent_at_height IS NULL;

ALTER TABLE {schema}.wallet_address_tx
    ADD CONSTRAINT wallet_address_tx_pkey PRIMARY KEY (script_hash, block_height, tx_index, txid);
"
    ))
}

/// Builds `ANALYZE` statements covering every schema table before steady-state
/// reads and tip-following begin.
pub fn build_analyze_sql(schema_name: &str) -> Result<String, BenchError> {
    let schema = validate_schema_name(schema_name)?;
    Ok(format!(
        r"
ANALYZE {schema}.schema_meta;
ANALYZE {schema}.canonical_writer_fence;
ANALYZE {schema}.chain_epoch;
ANALYZE {schema}.chain_event;
ANALYZE {schema}.canonical_settled_segment;
ANALYZE {schema}.canonical_live_tail;
ANALYZE {schema}.block_header;
ANALYZE {schema}.block_hash_index;
ANALYZE {schema}.tree_state_checkpoint;
ANALYZE {schema}.transaction_location;
ANALYZE {schema}.subtree_root;
ANALYZE {schema}.mempool_active_entry;
ANALYZE {schema}.mempool_event;
ANALYZE {schema}.wallet_utxo;
ANALYZE {schema}.wallet_address_tx;
"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_name_validation_accepts_and_rejects_boundaries() -> Result<(), BenchError> {
        assert_eq!(
            validate_schema_name(DEFAULT_POSTGRES_SEGMENTED_SCHEMA)?,
            DEFAULT_POSTGRES_SEGMENTED_SCHEMA
        );
        assert!(validate_schema_name("").is_err());
        assert!(validate_schema_name("UpperCase").is_err());
        assert!(validate_schema_name("123start").is_err());
        assert!(validate_schema_name("bad-dash").is_err());
        assert!(validate_schema_name("bad;sql").is_err());
        assert!(validate_schema_name("pg_catalog_copy").is_err());
        assert!(validate_schema_name("information_schema").is_err());
        assert!(validate_schema_name(&"a".repeat(64)).is_err());
        assert_eq!(validate_schema_name(&"a".repeat(63))?.len(), 63);
        Ok(())
    }

    #[test]
    fn generated_ddl_enforces_external_toast_constraints_and_pooler_safety()
    -> Result<(), BenchError> {
        let create_sql = build_create_schema_sql(DEFAULT_POSTGRES_SEGMENTED_SCHEMA)?;
        let finalize_sql = build_finalize_schema_sql(DEFAULT_POSTGRES_SEGMENTED_SCHEMA)?;
        let analyze_sql = build_analyze_sql(DEFAULT_POSTGRES_SEGMENTED_SCHEMA)?;

        for expected_fragment in [
            "SET STORAGE EXTERNAL",
            "chunk_blocks = 16",
            "per_block_framing",
            "clock_timestamp()",
            "268435456",
            "zinder_bench_segmented.canonical_settled_segment",
            "zinder_bench_segmented.canonical_live_tail",
            "zinder_bench_segmented.block_hash_index",
            "zinder_bench_segmented.tree_state_checkpoint",
            "zinder_bench_segmented.transaction_location",
        ] {
            assert!(
                create_sql.contains(expected_fragment),
                "missing `{expected_fragment}` in create SQL"
            );
        }

        for forbidden_session_feature in ["LISTEN ", "NOTIFY ", "CREATE TEMP TABLE", "pg_advisory"]
        {
            assert!(!create_sql.contains(forbidden_session_feature));
            assert!(!finalize_sql.contains(forbidden_session_feature));
            assert!(!analyze_sql.contains(forbidden_session_feature));
        }

        assert!(finalize_sql.contains("canonical_settled_segment_end_height_idx"));
        assert!(finalize_sql.contains("transaction_location_height_idx"));
        assert!(finalize_sql.contains("wallet_utxo_script_unspent_idx"));
        assert!(analyze_sql.contains("ANALYZE zinder_bench_segmented.wallet_address_tx"));
        Ok(())
    }
}
