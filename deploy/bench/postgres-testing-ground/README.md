# Stateless-Topology Testing Ground (`PostgreSQL 17/18 + PgBouncer + Mock R2 + Caddy`)

Disposable local and CI topology that reproduces Cloud Run and Cloudflare Containers + Hyperdrive + R2 caching conditions before `zinder-postgres` Slice 1 and Slice 2 land:

- **PostgreSQL 17 & 18 (digest-pinned):** `--data-checksums`, `fsync=on`, `full_page_writes=on`, `synchronous_commit=on`, `password_encryption=scram-sha-256`, and least-privilege `zinder_schema_owner`, `zinder_writer`, `zinder_reader`, and `zinder_admin` roles (`init-roles.sql`).
- **`PgBouncer` in strict `transaction` pool mode:** `default_pool_size = 4`, `max_client_conn = 100`, `max_prepared_statements = 0`, `auth_type = scram-sha-256`. Rapid multi-replica traffic forces server-connection rotation on every transaction and fails immediately on session-pinned state.
- **Mock S3 / R2 HTTP segment cache (`mock-r2/server.py`):** Supports `PUT /segments/<key>`, `GET`/`HEAD` with byte `Range` (`206 Partial Content`), `/metrics`, and runtime `/fault` injection (`normal`, `miss_404`, `outage_503`, `timeout_ms`, `corrupt_payload`) with fail-closed validation.
- **Non-sticky round-robin L7 proxy (`proxy/Caddyfile`):** `caddy:2.11-alpine` distributing REST, HTTP/1.1 `gRPC-Web`, and HTTP/2 `h2c` traffic round-robin with `lb_retries 0` across two `read_only: true` stateless probe replicas (`probe-reader-a`, `probe-reader-b`) with zero local persistent DB volumes.
- **Stateless `zinder` reader profile (`--profile readers`):** Two `read_only: true` containers running `zinder serve --readers-only --preset <preset>` backed by `/tmp` tmpfs only, plus `proxy/Caddyfile.readers` for live upstream `zinder-ingest` + `zinder-postgres` runs.

> **Diagnostic scope:** This testing ground is a maintainer verification harness. `rocksdb` remains Zinder's sole supported production topology until release-specific `postgres` benchmark, recovery, and operator gates pass (ADR-0004).

## Quick start

```bash
scripts/run-postgres-testing-ground.sh --postgres-versions 18,17 --zinder-image zinder:local
```

Reports are written to `.tmp/postgres-testing-ground-<timestamp>/report.json` and `report.md` (or `--report-dir`). Every run validates `docker compose config`, provisions fresh ephemeral PostgreSQL 18 and 17 instances, verifies PgBouncer transaction pooling, drives all Mock R2 fault modes and non-sticky round-robin L7 requests, validates `--readers-only` config under `read_only: true`, and removes all containers and volumes on exit.
