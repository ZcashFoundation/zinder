# Storage benchmark environment

> **Maintainer benchmark.** This harness compares RocksDB and PostgreSQL for canonical replay-record round trips. PostgreSQL is not a production Zinder storage backend. Operators deploying Zinder should follow [Deploying Zinder](../docs/runbooks/deploying.md).

`docker-compose.storage-benchmark.yml` compares RocksDB and PostgreSQL from one immutable `zinder-bench` image and one captured fixture. Each arm writes every `CanonicalBlockFacts` replay envelope, reads every row back, recomputes the ordered sequence digest, and writes a completion marker only after that check passes. Canonical construction, compact blocks, tree positions, chain events, reorgs, the wallet store, and query serving are outside its scope.

## Profiles

Measure throughput with one isolated profile at a time (`comparison` runs all three services together and is for integration checks only):

| Profile | Services | Budget |
| --- | --- | --- |
| `rocksdb` | `rocksdb-canonical-replay-storage` | 8 CPUs, 16 GiB |
| `postgres` | `postgres-canonical-replay-storage`, `postgres-database` | 8 CPUs, 16 GiB total (client 2 CPUs / 8 GiB; database 6 CPUs / 8 GiB) |
| `comparison` | All 3 services | Shared host resources |

Component limits in PostgreSQL reports must sum to the reported arm total. The fixture (read-only) and reports are host bind mounts; each store uses its own named volume. PostgreSQL keeps logged tables, `fsync`, full-page writes, and synchronous commit enabled (OS temporary files only on `tmpfs`). If you change the database memory limit, update its settings and keep the rendered Compose model with the evidence.

## Prepare the environment

Run from the repository root. Copy the example env file and create a results directory writable by UID 10001:

```bash
cp deploy/.env.storage-benchmark.example /tmp/zinder-storage-benchmark.env
mkdir -p benchmark-results
```

From a clean, committed worktree, build an immutable image and append its provenance (`@sha256:<64-hex>` registry references are also accepted; mutable tags such as `latest` are rejected):

```bash
test -z "$(git status --porcelain)"
: "${ZINDER_BENCH_RUNNER_ID:?export the immutable runner profile identity}"
: "${ZINDER_BENCH_STORAGE_CLASS:?export the measured storage class identity}"
: "${ZINDER_BENCH_TRIAL_ID:?export a unique trial label such as trial-01}"
: "${ZINDER_BENCH_FIXTURE_CACHE_POLICY:?export warm or cold after applying the runbook policy}"
software_revision="$(git rev-parse HEAD)"
image_tag="zinder-bench:${software_revision}"
image_id_file="/tmp/zinder-bench-${software_revision}.iid"

rm -f "$image_id_file"
docker build \
  --file deploy/Dockerfile \
  --target zinder-bench \
  --tag "$image_tag" \
  --iidfile "$image_id_file" \
  .
image_reference="$(sed -n '1p' "$image_id_file")"
test "${#image_reference}" -eq 71

printf 'ZINDER_BENCH_IMAGE=%s\n' "$image_reference" \
  >> /tmp/zinder-storage-benchmark.env
printf 'ZINDER_BENCH_SOFTWARE_REVISION=%s\n' "$software_revision" \
  >> /tmp/zinder-storage-benchmark.env
printf 'ZINDER_BENCH_RUNNER_ID=%s\n' "$ZINDER_BENCH_RUNNER_ID" \
  >> /tmp/zinder-storage-benchmark.env
printf 'ZINDER_BENCH_STORAGE_CLASS=%s\n' "$ZINDER_BENCH_STORAGE_CLASS" \
  >> /tmp/zinder-storage-benchmark.env
printf 'ZINDER_BENCH_TRIAL_ID=%s\n' "$ZINDER_BENCH_TRIAL_ID" \
  >> /tmp/zinder-storage-benchmark.env
printf 'ZINDER_BENCH_FIXTURE_CACHE_POLICY=%s\n' "$ZINDER_BENCH_FIXTURE_CACHE_POLICY" \
  >> /tmp/zinder-storage-benchmark.env
```

## Validate the Compose model

Check the rendered Compose config and profiles (`comparison`, `postgres`, `rocksdb`):

```bash
ZINDER_BENCH_IMAGE=sha256:0000000000000000000000000000000000000000000000000000000000000000 \
ZINDER_BENCH_SOFTWARE_REVISION=config-validation \
ZINDER_BENCH_RUNNER_ID=config-validation \
ZINDER_BENCH_STORAGE_CLASS=config-validation \
ZINDER_BENCH_TRIAL_ID=config-validation \
ZINDER_BENCH_FIXTURE_CACHE_POLICY=warm \
docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/bench/docker-compose.storage-benchmark.yml \
  --profile comparison \
  config --quiet

ZINDER_BENCH_IMAGE=sha256:0000000000000000000000000000000000000000000000000000000000000000 \
ZINDER_BENCH_SOFTWARE_REVISION=config-validation \
ZINDER_BENCH_RUNNER_ID=config-validation \
ZINDER_BENCH_STORAGE_CLASS=config-validation \
ZINDER_BENCH_TRIAL_ID=config-validation \
ZINDER_BENCH_FIXTURE_CACHE_POLICY=warm \
docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/bench/docker-compose.storage-benchmark.yml \
  config --profiles
```

## Run the RocksDB arm

Start from an empty volume (both existing stores and existing output paths are refused):

```bash
docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/bench/docker-compose.storage-benchmark.yml \
  --profile comparison down --volumes --remove-orphans

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/bench/docker-compose.storage-benchmark.yml \
  --profile rocksdb run --rm --no-deps \
  --user 0:0 --entrypoint /bin/sh rocksdb-canonical-replay-storage -ceu '
    test -z "$(ls -A /var/lib/zinder)"
    chown 10001:10001 /var/lib/zinder
  '

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/bench/docker-compose.storage-benchmark.yml \
  --profile rocksdb run --rm --no-deps rocksdb-canonical-replay-storage
```

The arm builds one sorted external SST file, ingests it, validates the replay sequence, writes its completion marker, and reopens the store for a second validation pass, writing:

- `rocksdb-canonical-replay-storage-${ZINDER_BENCH_SOFTWARE_REVISION}-${ZINDER_BENCH_TRIAL_ID}.json` (report)
- `rocksdb-canonical-replay-storage-${ZINDER_BENCH_SOFTWARE_REVISION}-${ZINDER_BENCH_TRIAL_ID}.resources.json` (cgroup `memory.current`, exact `memory.peak`, and sampled disk under `/var/lib/zinder`)

Keep `ZINDER_BENCH_RESOURCE_SAMPLE_INTERVAL_SECONDS` fixed across a campaign; disk sampling uses recursive `du`, so compare observer-on and observer-off smoke runs before relying on peak disk evidence.

## Run the PostgreSQL arm

The client reads `ZINDER_BENCH_POSTGRES_DATABASE_URL` inside its container using SCRAM-SHA-256 without TLS on the isolated benchmark network (`127.0.0.1:55432` with public local-only credentials `zinder_bench` / `zinder_bench_local_only`; point only at a database you control):

```bash
docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/bench/docker-compose.storage-benchmark.yml \
  --profile comparison down --volumes --remove-orphans

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/bench/docker-compose.storage-benchmark.yml \
  --profile postgres up -d --wait postgres-database

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/bench/docker-compose.storage-benchmark.yml \
  exec -T postgres-database sh -ceu '
    pg_isready --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"
    psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
      --set ON_ERROR_STOP=1 \
      --command "SHOW server_version;" \
      --command "SELECT current_database();"
  '

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/bench/docker-compose.storage-benchmark.yml \
  --profile postgres run --rm --no-deps postgres-canonical-replay-storage

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/bench/docker-compose.storage-benchmark.yml \
  --profile postgres stop postgres-database
```

The arm streams the fixture via binary `COPY` in one transaction (`lz4` for large encodings), builds its primary-key index, validates the sequence, and writes three files (wait for `postgres-database` to stop so both resource files exist):

- `postgres-canonical-replay-storage-${ZINDER_BENCH_SOFTWARE_REVISION}-${ZINDER_BENCH_TRIAL_ID}.json`
- `postgres-canonical-replay-storage-client-${ZINDER_BENCH_SOFTWARE_REVISION}-${ZINDER_BENCH_TRIAL_ID}.resources.json`
- `postgres-canonical-replay-storage-database-${ZINDER_BENCH_SOFTWARE_REVISION}-${ZINDER_BENCH_TRIAL_ID}.resources.json`

Formal services require a private cgroup v2 namespace, readable `memory.current` and `memory.peak`, and a sampleable volume root.

## Run the PostgreSQL driver integration gate

Tests the Rust driver path against a fresh database (matching CI `ci-postgres`):

```bash
docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/bench/docker-compose.storage-benchmark.yml \
  --profile postgres down --volumes --remove-orphans

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/bench/docker-compose.storage-benchmark.yml \
  --profile postgres up -d --wait postgres-database

ZINDER_TEST_POSTGRES_DATABASE_URL='postgresql://zinder_bench:zinder_bench_local_only@127.0.0.1:55432/zinder_bench' \
  cargo nextest run -p zinder-bench --profile=ci-postgres --run-ignored=all

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/bench/docker-compose.storage-benchmark.yml \
  --profile postgres down --volumes --remove-orphans
```

## Run a comparison campaign

For evidence beyond a single smoke pair:

1. Run on an idle host with fixed fixture, images, resource partition, storage class, revision, and preparation concurrency.
2. Fix `ZINDER_BENCH_FIXTURE_CACHE_POLICY` (`warm` by reading every fixture file untimed before each arm, or `cold` only if you reset host/VM page caches before every arm and record how).
3. Run at least five fresh-volume pairs with unique `ZINDER_BENCH_TRIAL_ID` values, alternating which arm runs first.
4. Record each completed pair in a tab-separated ledger beside the reports:

   <!-- markdownlint-disable MD010 -->

   ```text
   rocksdb_report	rocksdb_resources	postgres_report	postgres_client_resources	postgres_database_resources
   rocksdb-canonical-replay-storage-REV-trial-01.json	rocksdb-canonical-replay-storage-REV-trial-01.resources.json	postgres-canonical-replay-storage-REV-trial-01.json	postgres-canonical-replay-storage-client-REV-trial-01.resources.json	postgres-canonical-replay-storage-database-REV-trial-01.resources.json
   ```

   <!-- markdownlint-enable MD010 -->

5. Validate the campaign (checks >=5 pairs, alternation, provenance, digests, and observer alignment) and write the summary:

   ```bash
   scripts/validate-storage-benchmark-campaign.sh \
     benchmark-results/campaign.tsv \
     > benchmark-results/campaign-summary.json
   ```

## Compare the reports

Both reports must show `fixture_sequence_digest_match: true` and matching provenance. Evaluate whole-arm wall time, physical bytes, digest equality, and resource partitions together (`sampled_whole_arm_memory_peak_bytes` aligns client + database samples within one interval rather than summing asynchronous `memory.peak` values; `sampled_whole_arm_storage_peak_bytes` tracks volume high water alongside `round_trip.storage`).

```bash
jq '{
  measurement_kind,
  candidate: .storage_candidate.id,
  topology: .storage_candidate.topology,
  fixture_digest: .fixture.canonical_block_facts_digest_evidence.sequence_digest_sha256,
  persisted_digest: .round_trip.persisted_sequence_digest.sha256,
  digest_match: .round_trip.fixture_sequence_digest_match,
  replay_format_version: .round_trip.replay_format_version,
  semantic_replay_validated: .round_trip.semantic_replay_validated,
  blocks: .round_trip.block_count,
  wall_clock_seconds: .round_trip.wall_clock_seconds,
  blocks_per_second: .round_trip.blocks_per_second,
  benchmark_client_peak_rss: .round_trip.benchmark_client_peak_rss,
  phases: {
    storage_initialization: .round_trip.storage_initialization_wall_clock_seconds,
    replay_preparation: .round_trip.replay_preparation_wall_clock_seconds,
    replay_persistence: .round_trip.replay_persistence_wall_clock_seconds,
    index_construction: .round_trip.index_construction_wall_clock_seconds,
    storage_optimization: .round_trip.storage_optimization_wall_clock_seconds,
    validation: .round_trip.validation_wall_clock_seconds,
    publication: .round_trip.publication_wall_clock_seconds,
    fresh_reader_validation: .round_trip.fresh_reader_validation_wall_clock_seconds,
    storage_measurement: .round_trip.storage_measurement_wall_clock_seconds,
    unattributed: .round_trip.unattributed_wall_clock_seconds
  },
  physical_storage_bytes: .round_trip.physical_storage_bytes,
  runner: .provenance.runner
}' \
  benchmark-results/rocksdb-canonical-replay-storage-*.json \
  benchmark-results/postgres-canonical-replay-storage-*.json
```

## Check integration with the comparison profile

Do not publish throughput from `comparison` (shared budgets):

```bash
docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/bench/docker-compose.storage-benchmark.yml \
  --profile comparison up -d --wait postgres-database

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/bench/docker-compose.storage-benchmark.yml \
  --profile comparison run --rm --no-deps \
  --user 0:0 --entrypoint /bin/sh rocksdb-canonical-replay-storage -ceu '
    test -z "$(ls -A /var/lib/zinder)"
    chown 10001:10001 /var/lib/zinder
  '

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/bench/docker-compose.storage-benchmark.yml \
  --profile comparison run --rm --no-deps rocksdb-canonical-replay-storage

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/bench/docker-compose.storage-benchmark.yml \
  --profile comparison run --rm --no-deps postgres-canonical-replay-storage
```

## Clean up

```bash
docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/bench/docker-compose.storage-benchmark.yml \
  --profile comparison down --volumes --remove-orphans
```
