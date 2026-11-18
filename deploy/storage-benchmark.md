# Storage benchmark environment

`docker-compose.storage-benchmark.yml` compares RocksDB and PostgreSQL as
stores for Zinder's canonical replay records. Both arms run from one immutable
`zinder-bench` image and one captured fixture. Each arm writes every
`CanonicalBlockFacts` replay envelope, reads every row back, recomputes the
ordered sequence digest, and writes a completion marker only after that check
passes. This is a maintainer procedure.

The reports cover only this replay-record round trip. They do not measure
canonical construction, compact blocks, tree positions, chain events, reorgs,
the wallet store, or query serving. PostgreSQL is not a Zinder storage backend,
and neither deployment depends on this environment.

## Profiles

Measure throughput with one isolated profile at a time. The `comparison`
profile starts every service, which then compete for resources, so use it only
for integration checks.

| Profile | Services | Budget |
| --- | --- | --- |
| `rocksdb` | `rocksdb-canonical-replay-storage` | 8 CPUs, 16 GiB |
| `postgres` | `postgres-canonical-replay-storage`, `postgres-database` | 8 CPUs, 16 GiB in total: client 2 CPUs and 8 GiB, database 6 CPUs and 8 GiB |
| `comparison` | All 3 services | Shared host resources |

The PostgreSQL report records the arm total and both component limits. The CLI
rejects component limits that do not sum to the reported total.

The fixture and reports are host bind mounts, and the fixture is the only
read-only input. The RocksDB store and PostgreSQL `PGDATA` each use their own
named volume. PostgreSQL keeps logged tables, `fsync`, full-page writes, and
synchronous commit on; only operating-system temporary files use `tmpfs`. If
you change the database memory limit, adjust its memory settings and keep the
rendered Compose model with the evidence.

## Prepare the environment

Run every command from the repository root. Copy the example environment file
and create a results directory writable by UID 1000:

```bash
cp deploy/.env.storage-benchmark.example /tmp/zinder-storage-benchmark.env
mkdir -p benchmark-results
```

A formal run needs an immutable image and recorded provenance. From a clean,
committed worktree, build the image and append its identity to the copied file:

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

Regenerate the file whenever the revision or image changes. A shared image may
use a registry reference pinned with `@sha256:<64-hex>`. The CLI rejects
mutable tags such as `latest`.

## Validate the Compose model

Render the model before starting containers. The all-zero image ID and
`config-validation` labels exist only for this static check:

```bash
ZINDER_BENCH_IMAGE=sha256:0000000000000000000000000000000000000000000000000000000000000000 \
ZINDER_BENCH_SOFTWARE_REVISION=config-validation \
ZINDER_BENCH_RUNNER_ID=config-validation \
ZINDER_BENCH_STORAGE_CLASS=config-validation \
ZINDER_BENCH_TRIAL_ID=config-validation \
ZINDER_BENCH_FIXTURE_CACHE_POLICY=warm \
docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/docker-compose.storage-benchmark.yml \
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
  -f deploy/docker-compose.storage-benchmark.yml \
  config --profiles
```

The second command prints `comparison`, `postgres`, and `rocksdb`.

## Run the RocksDB arm

Start from an empty volume. The candidate refuses an existing store, and the
report refuses an existing output path:

```bash
docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/docker-compose.storage-benchmark.yml \
  --profile comparison down --volumes --remove-orphans

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/docker-compose.storage-benchmark.yml \
  --profile rocksdb run --rm --no-deps \
  --user 0:0 --entrypoint /bin/sh rocksdb-canonical-replay-storage -ceu '
    test -z "$(ls -A /var/lib/zinder)"
    chown 1000:1000 /var/lib/zinder
  '

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/docker-compose.storage-benchmark.yml \
  --profile rocksdb run --rm --no-deps rocksdb-canonical-replay-storage
```

The root-only step changes ownership of the fresh volume before timing starts.
The arm builds one sorted external SST file, ingests it, validates the replay
sequence, writes its completion marker, and reopens the store for a second
validation pass. It writes two files:

- `rocksdb-canonical-replay-storage-${ZINDER_BENCH_SOFTWARE_REVISION}-${ZINDER_BENCH_TRIAL_ID}.json`,
  the report;
- `rocksdb-canonical-replay-storage-${ZINDER_BENCH_SOFTWARE_REVISION}-${ZINDER_BENCH_TRIAL_ID}.resources.json`,
  the resource evidence: sampled cgroup `memory.current`, the exact
  `memory.peak`, and sampled disk use under `/var/lib/zinder`.

Keep `ZINDER_BENCH_RESOURCE_SAMPLE_INTERVAL_SECONDS` fixed for a whole
campaign. The disk sampler runs a recursive `du`, which adds its own I/O, so
the interval is a delay between samples rather than an exact cadence. Compare
observer-on and observer-off smoke runs before you rely on peak disk evidence.

## Run the PostgreSQL arm

The client receives the database URL through the
`ZINDER_BENCH_POSTGRES_DATABASE_URL` variable inside its container, so
credentials stay out of command arguments and the report. The database uses
SCRAM-SHA-256 authentication without TLS on an isolated benchmark network.
Point the client only at a database you control.

```bash
docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/docker-compose.storage-benchmark.yml \
  --profile comparison down --volumes --remove-orphans

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/docker-compose.storage-benchmark.yml \
  --profile postgres up -d --wait postgres-database

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/docker-compose.storage-benchmark.yml \
  exec -T postgres-database sh -ceu '
    pg_isready --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"
    psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
      --set ON_ERROR_STOP=1 \
      --command "SHOW server_version;" \
      --command "SELECT current_database();"
  '

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/docker-compose.storage-benchmark.yml \
  --profile postgres run --rm --no-deps postgres-canonical-replay-storage

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/docker-compose.storage-benchmark.yml \
  --profile postgres stop postgres-database
```

The arm creates a fresh schema, streams the fixture through binary `COPY` in
one transaction, builds its primary-key index, validates the sequence, and
commits its completion row. Large replay encodings use `lz4` compression. It
writes three files:

- `postgres-canonical-replay-storage-${ZINDER_BENCH_SOFTWARE_REVISION}-${ZINDER_BENCH_TRIAL_ID}.json`,
  the report, including the effective database settings;
- `postgres-canonical-replay-storage-client-${ZINDER_BENCH_SOFTWARE_REVISION}-${ZINDER_BENCH_TRIAL_ID}.resources.json`,
  written when the client exits;
- `postgres-canonical-replay-storage-database-${ZINDER_BENCH_SOFTWARE_REVISION}-${ZINDER_BENCH_TRIAL_ID}.resources.json`,
  written only when `postgres-database` stops.

Do not record a campaign row until both resource files exist.

The local host endpoint is
`postgresql://zinder_bench:zinder_bench_local_only@127.0.0.1:55432/zinder_bench`.
These credentials are public and valid only for this loopback-bound database.
The database joins a second bridge network so Docker can publish that loopback
port. That bridge does not block the database's outbound traffic.

The formal services require a private cgroup v2 namespace, readable
`memory.current` and `memory.peak`, and a sampleable volume root. If any is
missing, the container fails with a configuration error before the trial
starts.

## Run the PostgreSQL driver integration gate

This smaller test creates its own one-block fixture and checks the Rust driver
path against the same database service. Pull-request CI runs the same nextest
profile.

```bash
docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/docker-compose.storage-benchmark.yml \
  --profile postgres down --volumes --remove-orphans

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/docker-compose.storage-benchmark.yml \
  --profile postgres up -d --wait postgres-database

ZINDER_TEST_POSTGRES_DATABASE_URL='postgresql://zinder_bench:zinder_bench_local_only@127.0.0.1:55432/zinder_bench' \
  cargo nextest run -p zinder-bench --profile=ci-postgres --run-ignored=all

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/docker-compose.storage-benchmark.yml \
  --profile postgres down --volumes --remove-orphans
```

The test requires a fresh database. It covers SCRAM authentication, binary
`COPY`, commit, reconnect, complete read-back, storage measurement, and
rejection of an existing schema.

## Run a comparison campaign

One RocksDB and PostgreSQL pair is a smoke test. For evidence:

1. Use one otherwise idle host. Keep the fixture, images, resource partition,
   storage class, revision, and preparation concurrency fixed.
2. Choose one fixture page-cache policy for the whole campaign and set
   `ZINDER_BENCH_FIXTURE_CACHE_POLICY`. Use `warm` unless you can reset the
   host or Docker VM page cache before every arm: before each measured arm,
   read every fixture file once, untimed. Claim `cold` only when you reset the
   cache before every arm, and record how. Fresh volumes do not clear the
   cached fixture pages.
3. Run at least five fresh-volume pairs. Set a unique `ZINDER_BENCH_TRIAL_ID`,
   such as `trial-01`, before each pair, and alternate which arm runs first.
4. After each pair completes and the database has stopped, add a row to a
   tab-separated ledger beside the reports. Paths are relative to the ledger:

   <!-- markdownlint-disable MD010 -->

   ```text
   rocksdb_report	rocksdb_resources	postgres_report	postgres_client_resources	postgres_database_resources
   rocksdb-canonical-replay-storage-REV-trial-01.json	rocksdb-canonical-replay-storage-REV-trial-01.resources.json	postgres-canonical-replay-storage-REV-trial-01.json	postgres-canonical-replay-storage-client-REV-trial-01.resources.json	postgres-canonical-replay-storage-database-REV-trial-01.resources.json
   ```

   <!-- markdownlint-enable MD010 -->

5. Validate the campaign and produce the summary:

   ```bash
   scripts/validate-storage-benchmark-campaign.sh \
     benchmark-results/campaign.tsv \
     > benchmark-results/campaign-summary.json
   ```

The validator orders trials from the reports' own timestamps. It rejects fewer
than five pairs, overlapping or non-alternating runs, inconsistent cache policy
or runner identity, duplicate trials or paths, mutable images, a failed digest
check, mismatched configuration, failed or incomplete resource observers, and
PostgreSQL samples it cannot align in time. The summary keeps every artifact
path and SHA-256 with the minimum, median, and maximum of each metric.

## Compare the reports

A valid pair uses the same fixture digest, revision, image, runner, resource
budget, storage class, and preparation concurrency. Both reports must show
`fixture_sequence_digest_match: true` before throughput or size means anything.

Read the results as whole-arm measurements, not as an engine ranking.
PostgreSQL splits its budget between client and database, while RocksDB runs in
one process. External SST ingestion and binary `COPY` are different operations,
and a RocksDB reopen is a stronger check than a PostgreSQL reconnect. Draw
conclusions from end-to-end wall time, final storage bytes, digest equality,
and the recorded resource partition together.

Use these campaign metrics:

- `sampled_whole_arm_memory_peak_bytes`: for RocksDB, the largest sampled
  `memory.current`; for PostgreSQL, the largest sum of client and database
  samples taken within one sample interval of each other. Never add the two
  components' `memory.peak` values, because the peaks can occur at different
  times. `round_trip.benchmark_client_peak_rss` covers only the client process.
- `sampled_whole_arm_storage_peak_bytes`: sampled disk high-water mark of the
  RocksDB volume or the PostgreSQL data volume. Read it together with the
  observed sample gaps and the report's final physical byte count.

Physical byte totals follow each engine's own layout. Read
`round_trip.storage` before you compare storage efficiency.

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

The `comparison` profile makes every service reachable but does not isolate
their budgets. Do not publish throughput from it. Start from clean volumes and
fresh report paths:

```bash
docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/docker-compose.storage-benchmark.yml \
  --profile comparison up -d --wait postgres-database

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/docker-compose.storage-benchmark.yml \
  --profile comparison run --rm --no-deps \
  --user 0:0 --entrypoint /bin/sh rocksdb-canonical-replay-storage -ceu '
    test -z "$(ls -A /var/lib/zinder)"
    chown 1000:1000 /var/lib/zinder
  '

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/docker-compose.storage-benchmark.yml \
  --profile comparison run --rm --no-deps rocksdb-canonical-replay-storage

docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/docker-compose.storage-benchmark.yml \
  --profile comparison run --rm --no-deps postgres-canonical-replay-storage
```

## Clean up

Remove the containers and candidate volumes after you capture the reports. The
fixture and results bind mounts stay untouched:

```bash
docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/docker-compose.storage-benchmark.yml \
  --profile comparison down --volumes --remove-orphans
```
