# Stateless-Topology Testing Ground (`PostgreSQL 17/18 + PgBouncer + Mock R2 + Caddy`)

Disposable local and CI topology that reproduces Cloud Run and Cloudflare Containers + Hyperdrive + R2 caching conditions ahead of `zinder-postgres` Slice 1 and Slice 2:

- **PostgreSQL 17 & 18 (digest-pinned):** `--data-checksums`, `fsync=on`, `full_page_writes=on`, `synchronous_commit=on`, `password_encryption=scram-sha-256`, and least-privilege `zinder_schema_owner`, `zinder_writer`, `zinder_reader`, and `zinder_admin` roles (`init-roles.sql`).
- **`PgBouncer` in strict `transaction` pool mode:** `default_pool_size = 4`, `max_client_conn = 100`, `max_prepared_statements = 0`, `auth_type = scram-sha-256`. Rapid multi-replica traffic forces server-connection rotation on every transaction and fails immediately on session-pinned state.
- **Mock S3 / R2 HTTP segment cache (`mock-r2/server.py`):** Immutable `PUT /segments/<key>` (`201` on create, `200` on identical retry, `409` on conflicting overwrite), `GET`/`HEAD` (bodyless on `HEAD`) with byte `Range` (`206 Partial Content`), `/metrics`, and runtime `/fault` injection (`normal`, `miss_404`, `outage_503`, `timeout_ms`, `corrupt_payload`) with fail-closed validation.
- **Non-sticky round-robin L7 proxy (`proxy/Caddyfile`):** `caddy:2.11-alpine` distributing REST, HTTP/1.1 `gRPC-Web` (`:8081`), and native HTTP/2 `h2c` gRPC (`:8082` when `Content-Type` matches `^application/grpc(\+proto)?($|;)`) round-robin with `lb_retries 0` across two `read_only: true` stateless probe replicas (`probe-reader-a`, `probe-reader-b`) with zero local persistent DB volumes.
- **`zinder` reader image validation (`--zinder-image`):** Verifies `zinder config validate --preset <preset> --readers-only` under `--read-only` + `/tmp` tmpfs across `light-wallet`, `wallet`, and `explorer`. Live multi-replica `zinder serve --readers-only` Compose services are deferred until `#676` (PostgreSQL Slice 1) provides remote canonical storage (current `rocksdb` readers require local canonical files and `IngestControl`, exercised in-process by `stateless_topology_conformance`).

> **Diagnostic scope:** This testing ground is a maintainer verification harness. `rocksdb` remains Zinder's sole supported production topology until release-specific `postgres` benchmark, recovery, and operator gates pass (ADR-0004).

## Quick start

```bash
scripts/run-postgres-testing-ground.sh --postgres-versions 18,17 --zinder-image zinder:local
```

Reports are written on both pass and fail to `.tmp/postgres-testing-ground-<timestamp>/report.json` and `report.md` (or `--report-dir`). Every run validates `docker compose config`, provisions fresh ephemeral PostgreSQL 18 and 17 instances, verifies PgBouncer transaction pooling, exercises Mock R2 immutability, persistent-connection `HEAD` behavior, fault injection, and non-sticky round-robin L7 REST + `gRPC-Web` + native HTTP/2 `h2c` gRPC requests, validates `--readers-only` configuration under `--read-only` if requested, and tears down all containers and volumes on exit.
