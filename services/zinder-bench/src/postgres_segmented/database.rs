//! Credential-safe PostgreSQL connection management, posture checks, WAL LSN
//! diffing, and checked numeric/substring conversions.

use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls};

use crate::canonical_replay_storage::postgres::PostgresServerSettings;
use crate::error::BenchError;

use super::schema::POSTGRES_SEGMENTED_CANDIDATE_ID;

const SERVER_SETTINGS_SQL: &str = r"
SELECT
    current_setting('server_version'),
    current_setting('server_version_num')::INTEGER,
    current_setting('max_connections')::INTEGER,
    pg_size_bytes(current_setting('shared_buffers'))::BIGINT,
    pg_size_bytes(current_setting('effective_cache_size'))::BIGINT,
    pg_size_bytes(current_setting('maintenance_work_mem'))::BIGINT,
    pg_size_bytes(current_setting('work_mem'))::BIGINT,
    pg_size_bytes(current_setting('max_wal_size'))::BIGINT,
    pg_size_bytes(current_setting('min_wal_size'))::BIGINT,
    EXTRACT(EPOCH FROM current_setting('checkpoint_timeout')::INTERVAL)::BIGINT,
    current_setting('checkpoint_completion_target')::DOUBLE PRECISION,
    current_setting('wal_compression'),
    current_setting('password_encryption'),
    current_setting('max_worker_processes')::INTEGER,
    current_setting('max_parallel_workers')::INTEGER,
    current_setting('max_parallel_maintenance_workers')::INTEGER,
    current_setting('track_io_timing')::BOOLEAN,
    current_setting('huge_pages'),
    current_setting('fsync')::BOOLEAN,
    current_setting('full_page_writes')::BOOLEAN,
    current_setting('synchronous_commit'),
    current_setting('wal_level'),
    current_setting('data_checksums')::BOOLEAN
";

/// Live `tokio-postgres` client paired with its driven background connection task.
pub struct PostgresConnection {
    /// Connected PostgreSQL query client.
    pub client: Client,
    connection_task: JoinHandle<Result<(), tokio_postgres::Error>>,
}

/// Opaque WAL LSN text snapshot returned by `pg_current_wal_lsn()::TEXT`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalLsn(String);

impl WalLsn {
    /// Returns the textual PostgreSQL WAL LSN representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Opens one `PostgreSQL` connection without leaking connection strings on failure.
pub async fn connect_postgres(
    database_url: &str,
    operation: &'static str,
) -> Result<PostgresConnection, BenchError> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls)
        .await
        .map_err(|err| postgres_error(operation, &err))?;
    let connection_task = tokio::spawn(connection);
    Ok(PostgresConnection {
        client,
        connection_task,
    })
}

/// Gracefully closes a [`PostgresConnection`] and surfaces any connection I/O error.
pub async fn close_postgres(connection: PostgresConnection) -> Result<(), BenchError> {
    let PostgresConnection {
        client,
        connection_task,
    } = connection;
    drop(client);
    connection_task
        .await
        .map_err(|join_err| {
            candidate_error(format!("postgresql connection task aborted: {join_err}"))
        })?
        .map_err(|err| postgres_error("closing connection", &err))
}

/// Fails closed when the configured schema already exists on the server.
pub async fn reject_existing_schema(client: &Client, schema_name: &str) -> Result<(), BenchError> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)",
            &[&schema_name],
        )
        .await
        .map_err(|err| postgres_error("checking existing schema", &err))?
        .get(0);
    if exists {
        return Err(candidate_error(format!(
            "schema `{schema_name}` already exists; point the benchmark at a fresh empty database or schema"
        )));
    }
    Ok(())
}

/// Reads PostgreSQL configuration parameters that govern durability and I/O behavior.
pub async fn read_server_settings(client: &Client) -> Result<PostgresServerSettings, BenchError> {
    let row = client
        .query_one(SERVER_SETTINGS_SQL, &[])
        .await
        .map_err(|err| postgres_error("reading server settings", &err))?;
    Ok(PostgresServerSettings {
        server_version: row.get(0),
        server_version_number: i32_to_u32(row.get(1), "server_version_num")?,
        max_connections: i32_to_u32(row.get(2), "max_connections")?,
        shared_buffers_bytes: i64_to_u64(row.get(3), "shared_buffers")?,
        effective_cache_size_bytes: i64_to_u64(row.get(4), "effective_cache_size")?,
        maintenance_work_mem_bytes: i64_to_u64(row.get(5), "maintenance_work_mem")?,
        work_mem_bytes: i64_to_u64(row.get(6), "work_mem")?,
        max_wal_size_bytes: i64_to_u64(row.get(7), "max_wal_size")?,
        min_wal_size_bytes: i64_to_u64(row.get(8), "min_wal_size")?,
        checkpoint_timeout_seconds: i64_to_u64(row.get(9), "checkpoint_timeout")?,
        checkpoint_completion_target: row.get(10),
        wal_compression: row.get(11),
        password_encryption_default: row.get(12),
        max_worker_processes: i32_to_u32(row.get(13), "max_worker_processes")?,
        max_parallel_workers: i32_to_u32(row.get(14), "max_parallel_workers")?,
        max_parallel_maintenance_workers: i32_to_u32(
            row.get(15),
            "max_parallel_maintenance_workers",
        )?,
        track_io_timing: row.get(16),
        huge_pages: row.get(17),
        fsync: row.get(18),
        full_page_writes: row.get(19),
        synchronous_commit: row.get(20),
        wal_level: row.get(21),
        data_checksums: row.get(22),
    })
}

/// Validates that the server runs in a crash-safe durable posture (`fsync`,
/// `full_page_writes`, `synchronous_commit = on`, `wal_level` in `replica`/`logical`).
pub fn validate_required_posture(settings: &PostgresServerSettings) -> Result<(), BenchError> {
    if !settings.fsync {
        return Err(candidate_error("server setting fsync must be on"));
    }
    if !settings.full_page_writes {
        return Err(candidate_error(
            "server setting full_page_writes must be on",
        ));
    }
    if settings.synchronous_commit != "on" {
        return Err(candidate_error(format!(
            "server setting synchronous_commit must be `on`, found `{}`",
            settings.synchronous_commit
        )));
    }
    if settings.wal_level != "replica" && settings.wal_level != "logical" {
        return Err(candidate_error(format!(
            "server setting wal_level must be `replica` or `logical`, found `{}`",
            settings.wal_level
        )));
    }
    Ok(())
}

/// Captures the current primary WAL insert LSN as text without touching session state.
pub async fn measure_wal_lsn(client: &Client) -> Result<WalLsn, BenchError> {
    let text: String = client
        .query_one("SELECT pg_current_wal_lsn()::TEXT", &[])
        .await
        .map_err(|err| postgres_error("measuring pg_current_wal_lsn", &err))?
        .get(0);
    Ok(WalLsn(text))
}

/// Computes positive byte delta between two [`WalLsn`] markers via `pg_wal_lsn_diff`.
pub async fn wal_bytes_delta(
    client: &Client,
    start_lsn: &WalLsn,
    end_lsn: &WalLsn,
) -> Result<u64, BenchError> {
    let diff: i64 = client
        .query_one(
            "SELECT pg_wal_lsn_diff($2::TEXT::pg_lsn, $1::TEXT::pg_lsn)::BIGINT",
            &[&start_lsn.0, &end_lsn.0],
        )
        .await
        .map_err(|err| postgres_error("computing pg_wal_lsn_diff", &err))?
        .get(0);
    i64_to_u64(diff, "pg_wal_lsn_diff")
}

/// Builds a sanitized storage error without ever formatting connection strings or passwords.
#[must_use]
pub fn postgres_error(operation: &'static str, error: &tokio_postgres::Error) -> BenchError {
    let sqlstate = error
        .code()
        .map_or("none", tokio_postgres::error::SqlState::code);
    let detail = error.as_db_error().map_or_else(
        || sanitize_secret_free_text(&error.to_string()),
        |db_err| {
            format!(
                "{} ({})",
                sanitize_secret_free_text(db_err.message()),
                db_err.code().code()
            )
        },
    );
    candidate_error(format!(
        "{operation} failed [sqlstate={sqlstate}]: {detail}"
    ))
}

/// Wraps an error string under [`POSTGRES_SEGMENTED_CANDIDATE_ID`].
#[must_use]
pub fn candidate_error(reason: impl Into<String>) -> BenchError {
    BenchError::canonical_replay_storage_candidate(POSTGRES_SEGMENTED_CANDIDATE_ID, reason)
}

/// Converts a 0-based byte range `(offset, len)` into PostgreSQL 1-based `INT4`
/// `substring(bytea FROM integer FOR integer)` arguments.
pub fn substring_bounds(offset: u64, len: usize) -> Result<(i32, i32), BenchError> {
    if len == 0 {
        return Err(candidate_error(
            "byte-range substring length must be positive",
        ));
    }
    let one_based = offset
        .checked_add(1)
        .ok_or_else(|| candidate_error("byte-range substring offset overflow"))?;
    let pg_offset = i32::try_from(one_based)
        .map_err(|_| candidate_error("byte-range offset exceeds PostgreSQL INT4 limit"))?;
    let pg_len = i32::try_from(len)
        .map_err(|_| candidate_error("byte-range length exceeds PostgreSQL INT4 limit"))?;
    Ok((pg_offset, pg_len))
}

/// Converts `u32` into SQL `BIGINT` (`i64`).
#[must_use]
pub fn u32_to_i64(input: u32) -> i64 {
    i64::from(input)
}

/// Converts `u64` into non-negative SQL `BIGINT` (`i64`).
pub fn u64_to_i64(input: u64, field: &'static str) -> Result<i64, BenchError> {
    i64::try_from(input)
        .map_err(|_| candidate_error(format!("{field} ({input}) exceeds SQL BIGINT range")))
}

/// Converts `usize` into non-negative SQL `BIGINT` (`i64`).
pub fn usize_to_i64(input: usize, field: &'static str) -> Result<i64, BenchError> {
    i64::try_from(input)
        .map_err(|_| candidate_error(format!("{field} ({input}) exceeds SQL BIGINT range")))
}

/// Converts non-negative SQL `INTEGER` (`i32`) into `u32`.
pub fn i32_to_u32(input: i32, field: &'static str) -> Result<u32, BenchError> {
    u32::try_from(input)
        .map_err(|_| candidate_error(format!("{field} returned negative value ({input})")))
}

/// Converts non-negative SQL `BIGINT` (`i64`) into `u32`.
pub fn i64_to_u32(input: i64, field: &'static str) -> Result<u32, BenchError> {
    u32::try_from(input)
        .map_err(|_| candidate_error(format!("{field} ({input}) is outside u32 range")))
}

/// Converts non-negative SQL `BIGINT` (`i64`) into `u64`.
pub fn i64_to_u64(input: i64, field: &'static str) -> Result<u64, BenchError> {
    u64::try_from(input)
        .map_err(|_| candidate_error(format!("{field} returned negative value ({input})")))
}

/// Converts a SQL `BYTEA` slice into a fixed `[u8; 32]` array.
pub fn slice_to_32_bytes(bytes: &[u8], field: &'static str) -> Result<[u8; 32], BenchError> {
    bytes.try_into().map_err(|_| {
        candidate_error(format!(
            "{field} must be exactly 32 bytes, found {}",
            bytes.len()
        ))
    })
}

fn sanitize_secret_free_text(raw: &str) -> String {
    if raw.contains("://") || raw.contains("password=") {
        "sanitized postgres client diagnostic".to_owned()
    } else {
        raw.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substring_bounds_converts_zero_based_ranges_and_rejects_empty_or_overflow()
    -> Result<(), BenchError> {
        assert_eq!(substring_bounds(0, 128)?, (1, 128));
        assert_eq!(substring_bounds(4096, 64)?, (4097, 64));
        assert!(substring_bounds(0, 0).is_err());
        assert!(substring_bounds(u64::try_from(i32::MAX).unwrap_or(u64::MAX), 1).is_err());
        Ok(())
    }

    #[test]
    fn secret_sanitizer_strips_connection_strings() {
        let leaked = "could not connect to postgres://alice:supersecret@db:5432/app";
        assert!(!sanitize_secret_free_text(leaked).contains("supersecret"));
        assert!(!sanitize_secret_free_text("password=abc").contains("abc"));
        assert_eq!(
            sanitize_secret_free_text("connection refused"),
            "connection refused"
        );
    }
}
