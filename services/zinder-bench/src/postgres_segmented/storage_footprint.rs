//! Non-double-counted PostgreSQL table, index, TOAST, segment, and WAL footprint
//! measurement for the segmented schema.

use tokio_postgres::Client;

use crate::error::BenchError;

use super::database::{i64_to_u64, postgres_error};
use super::schema::validate_schema_name;

/// Per-table PostgreSQL relation footprint broken down without TOAST double-counting.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationFootprint {
    /// Table name inside the segmented schema.
    pub table_name: String,
    /// Exact row count queried from the table.
    pub row_count: u64,
    /// Main fork bytes (`pg_relation_size(c.oid, 'main')`).
    pub heap_bytes: u64,
    /// Dedicated TOAST table bytes (`pg_total_relation_size(c.reltoastrelid)`).
    pub toast_bytes: u64,
    /// Secondary + PK index bytes (`pg_indexes_size(c.oid)`).
    pub index_bytes: u64,
    /// Total disk bytes (`pg_total_relation_size(c.oid)` — includes heap + toast + indexes + fsm/vm).
    pub total_relation_bytes: u64,
    /// True if classified as wallet projection state rather than canonical/operational state.
    pub is_wallet_table: bool,
}

/// Complete schema storage and WAL accounting summary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageFootprintSummary {
    /// Non-double-counted canonical relation total bytes (`pg_total_relation_size` sum).
    pub canonical_storage_bytes: u64,
    /// Non-double-counted provisional wallet projection total bytes (`pg_total_relation_size` sum).
    pub wallet_storage_bytes: u64,
    /// Sum of `canonical_storage_bytes + wallet_storage_bytes`.
    pub total_storage_bytes: u64,
    /// Main heap fork bytes across all tables in the schema.
    pub total_heap_bytes: u64,
    /// TOAST relation bytes across all tables in the schema.
    pub total_toast_bytes: u64,
    /// Index bytes across all tables in the schema.
    pub total_index_bytes: u64,
    /// WAL bytes emitted during bulk load (`pg_wal_lsn_diff`).
    pub bulk_wal_bytes: u64,
    /// WAL bytes emitted during tip-following phases (`pg_wal_lsn_diff`).
    pub tip_follow_wal_bytes: u64,
    /// Exact number of `shielded` rows in `canonical_settled_segment`.
    pub settled_shielded_segments: u64,
    /// Exact number of `supplement` rows in `canonical_settled_segment`.
    pub settled_supplement_segments: u64,
    /// Exact number of `raw_block` rows in `canonical_settled_segment`.
    pub settled_raw_block_segments: u64,
    /// Largest single `segment_byte_len` across all settled segments.
    pub max_segment_byte_len: u64,
    /// Exact remaining rows in `canonical_live_tail`.
    pub live_tail_rows: u64,
    /// Individual table footprints in deterministic table-name order.
    pub relations: Vec<RelationFootprint>,
}

struct SegmentTableCounts {
    shielded: u64,
    supplement: u64,
    raw_block: u64,
    max_segment_byte_len: u64,
    live_tail_rows: u64,
}

const RELATION_FOOTPRINT_SQL: &str = r"
SELECT
    c.relname::TEXT,
    pg_relation_size(c.oid, 'main')::BIGINT AS heap_bytes,
    CASE
        WHEN c.reltoastrelid <> 0 THEN pg_total_relation_size(c.reltoastrelid)::BIGINT
        ELSE 0::BIGINT
    END AS toast_bytes,
    pg_indexes_size(c.oid)::BIGINT AS index_bytes,
    pg_total_relation_size(c.oid)::BIGINT AS total_relation_bytes
FROM pg_class AS c
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = $1
  AND c.relkind IN ('r', 'p')
ORDER BY c.relname ASC
";

/// Queries exact PostgreSQL relation sizes (main/TOAST/index/total) and row counts
/// across every table in `schema_name`, preserving non-double-counted `pg_total_relation_size`.
pub async fn measure_storage_footprint(
    client: &Client,
    schema_name: &str,
    bulk_wal_bytes: u64,
    tip_follow_wal_bytes: u64,
) -> Result<StorageFootprintSummary, BenchError> {
    let schema = validate_schema_name(schema_name)?;
    let relations = measure_relation_footprints(client, schema).await?;
    let counts = measure_segment_counts(client, schema).await?;

    let mut canonical_storage_bytes = 0_u64;
    let mut wallet_storage_bytes = 0_u64;
    let mut total_heap_bytes = 0_u64;
    let mut total_toast_bytes = 0_u64;
    let mut total_index_bytes = 0_u64;

    for rel in &relations {
        if rel.is_wallet_table {
            wallet_storage_bytes = wallet_storage_bytes.saturating_add(rel.total_relation_bytes);
        } else {
            canonical_storage_bytes =
                canonical_storage_bytes.saturating_add(rel.total_relation_bytes);
        }
        total_heap_bytes = total_heap_bytes.saturating_add(rel.heap_bytes);
        total_toast_bytes = total_toast_bytes.saturating_add(rel.toast_bytes);
        total_index_bytes = total_index_bytes.saturating_add(rel.index_bytes);
    }

    Ok(StorageFootprintSummary {
        canonical_storage_bytes,
        wallet_storage_bytes,
        total_storage_bytes: canonical_storage_bytes.saturating_add(wallet_storage_bytes),
        total_heap_bytes,
        total_toast_bytes,
        total_index_bytes,
        bulk_wal_bytes,
        tip_follow_wal_bytes,
        settled_shielded_segments: counts.shielded,
        settled_supplement_segments: counts.supplement,
        settled_raw_block_segments: counts.raw_block,
        max_segment_byte_len: counts.max_segment_byte_len,
        live_tail_rows: counts.live_tail_rows,
        relations,
    })
}

async fn measure_relation_footprints(
    client: &Client,
    schema: &str,
) -> Result<Vec<RelationFootprint>, BenchError> {
    let rows = client
        .query(RELATION_FOOTPRINT_SQL, &[&schema])
        .await
        .map_err(|err| postgres_error("measuring postgres-segmented relation footprint", &err))?;

    let mut relations = Vec::with_capacity(rows.len());
    for row in rows {
        let table_name: String = row.get(0);
        let count_sql = format!("SELECT COUNT(*)::BIGINT FROM {schema}.{table_name}");
        let row_count_raw: i64 = client
            .query_one(&count_sql, &[])
            .await
            .map_err(|err| postgres_error("counting table rows", &err))?
            .get(0);
        relations.push(RelationFootprint {
            is_wallet_table: is_wallet_projection_table(&table_name),
            table_name,
            row_count: i64_to_u64(row_count_raw, "table_row_count")?,
            heap_bytes: i64_to_u64(row.get(1), "heap_bytes")?,
            toast_bytes: i64_to_u64(row.get(2), "toast_bytes")?,
            index_bytes: i64_to_u64(row.get(3), "index_bytes")?,
            total_relation_bytes: i64_to_u64(row.get(4), "total_relation_bytes")?,
        });
    }
    Ok(relations)
}

async fn measure_segment_counts(
    client: &Client,
    schema: &str,
) -> Result<SegmentTableCounts, BenchError> {
    let summary_row = client
        .query_one(
            &format!(
                r"
SELECT
    COUNT(*) FILTER (WHERE segment_kind = 'shielded')::BIGINT,
    COUNT(*) FILTER (WHERE segment_kind = 'supplement')::BIGINT,
    COUNT(*) FILTER (WHERE segment_kind = 'raw_block')::BIGINT,
    COALESCE(MAX(segment_byte_len), 0)::BIGINT,
    (SELECT COUNT(*)::BIGINT FROM {schema}.canonical_live_tail)
FROM {schema}.canonical_settled_segment
"
            ),
            &[],
        )
        .await
        .map_err(|err| postgres_error("measuring segment and live-tail counts", &err))?;

    Ok(SegmentTableCounts {
        shielded: i64_to_u64(summary_row.get(0), "settled_shielded_segments")?,
        supplement: i64_to_u64(summary_row.get(1), "settled_supplement_segments")?,
        raw_block: i64_to_u64(summary_row.get(2), "settled_raw_block_segments")?,
        max_segment_byte_len: i64_to_u64(summary_row.get(3), "max_segment_byte_len")?,
        live_tail_rows: i64_to_u64(summary_row.get(4), "live_tail_rows")?,
    })
}

fn is_wallet_projection_table(table_name: &str) -> bool {
    matches!(table_name, "wallet_utxo" | "wallet_address_tx")
}

#[cfg(test)]
mod tests {
    use super::is_wallet_projection_table;

    #[test]
    fn wallet_table_classification_matches_schema_contract() {
        assert!(is_wallet_projection_table("wallet_utxo"));
        assert!(is_wallet_projection_table("wallet_address_tx"));
        assert!(!is_wallet_projection_table("canonical_settled_segment"));
        assert!(!is_wallet_projection_table("canonical_live_tail"));
    }
}
