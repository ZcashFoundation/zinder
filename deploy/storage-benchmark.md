# Storage benchmark environment

`docker-compose.storage-benchmark.yml` runs 2 canonical-replay-storage storage
candidates from one immutable `zinder-bench` image and one captured fixture.
The RocksDB and PostgreSQL canonical-replay-storage arms persist the
same versioned `CanonicalBlockFacts` semantic replay envelope, read every row
back, reconstruct the full aggregate, recompute the independent ordered
sequence digest, and publish a completion marker only after that validation
succeeds.

The canonical-replay-storage reports prove only this persisted canonical replay storage run. They do not
certify fresh canonical construction, compact-block or tree-position material,
`ChainEpoch` or `ChainEvent` publication, reorg and settlement behavior, restore
and following, wallet projection construction, query readiness, or the complete
production lifecycle.

This Compose file is disposable benchmark infrastructure. The application
deployment remains in `docker-compose.yml`, and neither deployment topology
depends on this environment.

## Choose a profile

Run throughput measurements with one isolated profile at a time. The
`comparison` profile enables every service for integration checks, but its
services compete for host resources and cannot produce fair throughput data.

| Profile | Services | Measured boundary | Aggregate budget |
| --- | --- | --- | --- |
| `rocksdb` | `rocksdb-canonical-replay-storage` | RocksDB replay-record write, read-back digest, and completion marker | 8 CPUs, 16 GiB |
| `postgres` | `postgres-canonical-replay-storage`, `postgres-database` | PostgreSQL replay-record write, read-back digest, and completion row | 8 CPUs, 16 GiB |
| `comparison` | All 3 services | Cross-service integration only | Shared host resources |

The PostgreSQL arm divides its default budget between the benchmark client (2
CPUs and 8 GiB) and database (6 CPUs and 8 GiB). Its report records the full
8-CPU, 16-GiB arm budget, both component limits, and both immutable image
identities. The CLI rejects partial evidence or component limits whose exact
sum differs from the reported aggregate.

The fixture and reports use host bind mounts. The RocksDB candidate and
PostgreSQL `PGDATA` use separate named volumes so each candidate owns isolated
Linux filesystem state. The fixture is the sole read-only input for the
canonical-replay-storage services.

PostgreSQL keeps logged tables, `fsync`, full-page writes, and synchronous
commit enabled. Only disposable operating-system temporary files use `tmpfs`.
When you change the database memory limit, adjust its memory-related settings
and record the complete configuration with the evidence. The report queries the
performance and durability server settings used in comparisons; retain the
rendered Compose model beside it for initdb authentication, encoding/locale,
shared-memory, and temporary-filesystem evidence that is outside the SQL report.

## Prepare the environment

Run commands from the repository root. Copy the example environment file,
point it at an immutable fixture, and choose a fresh result directory for each
evidence set:

```bash
cp deploy/.env.storage-benchmark.example /tmp/zinder-storage-benchmark.env
mkdir -p benchmark-results
```

The result directory must be writable by UID 1000, which runs the benchmark
image.

Formal runs require an immutable image identity and structured provenance.
Start from a committed, clean worktree, build the benchmark target, and append
the exact source and image identities to the copied environment file:

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

Regenerate the copied file when the revision or image content changes. A
shared image may use a nonempty registry reference pinned with
`@sha256:<64-hex>`. The CLI rejects mutable tags such as branch names and
`latest` because they cannot identify reproducible evidence.

## Validate the Compose model

Render the complete model before starting containers. The all-zero image ID
and `config-validation` labels below exist only for static validation and must
never identify a benchmark report:

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

The expected profiles are `comparison`, `postgres`, and `rocksdb`.

## Run the RocksDB canonical-replay-storage arm

Start from empty candidate state and run only the `rocksdb` profile. The
candidate refuses an existing store, and the report refuses an existing output
path, which prevents an accidental incremental or destructive rerun:

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

The report is written as
`rocksdb-canonical-replay-storage-${ZINDER_BENCH_SOFTWARE_REVISION}-${ZINDER_BENCH_TRIAL_ID}.json`. This arm builds one
sorted external SST file, ingests it into its own named volume, validates the
persisted replay sequence, publishes its completion marker, and reopens the store
for a second complete validation pass. The root-only initialization above
changes ownership on the fresh disposable volume before timing begins; it does
not create the candidate path or mutate an existing candidate. The report keeps
the database I/O mode separate from external-SST construction, which is buffered
because the current Rust binding does not expose an external-writer I/O-mode
override.

The same container writes
`rocksdb-canonical-replay-storage-${ZINDER_BENCH_SOFTWARE_REVISION}-${ZINDER_BENCH_TRIAL_ID}.resources.json`
after the child process exits. The observer samples cgroup-v2
`memory.current`, preserves the exact component `memory.peak`, and samples
allocated bytes across `/var/lib/zinder`, including the sibling external-SST
staging directory. Keep `ZINDER_BENCH_RESOURCE_SAMPLE_INTERVAL_SECONDS` fixed
for the entire campaign. Storage observation runs a recursive `du`, so it adds
nonzero, layout-dependent metadata and I/O work. Treat the configured interval
as the delay between observations, not their actual cadence. Inspect the
artifact timestamps and observed report-window gaps, and calibrate observer-on
against observer-off smoke runs before using storage high-water evidence to
select a topology. If that overhead is material, replace the sampler with a
runner-supported quota or filesystem counter before a formal campaign.

## Run the PostgreSQL canonical-replay-storage arm

Start from an empty PostgreSQL volume, wait for database readiness, and inspect
the selected server before running the client. The benchmark command receives
only the name `ZINDER_BENCH_POSTGRES_DATABASE_URL`; Compose supplies the value
inside the client container, so credentials do not enter command arguments or
the report. The candidate schema explicitly uses `lz4` compression for large
replay encodings so heap and TOAST bytes do not depend on a server-wide
default. The benchmark contract requires this endpoint to be operator
controlled; do not point the diagnostic client at an untrusted PostgreSQL
server.

The Compose database requires host-password authentication with SCRAM-SHA-256,
so this path exercises `tokio-postgres` authentication as well as binary COPY,
transactions, reconnect, and read-back. Transport is intentionally `NoTls`
inside the isolated benchmark network. This diagnostic does not define the
production transport contract; a remote or shared-network PostgreSQL deployment
must add certificate-validated TLS before its topology can pass production
readiness.

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

The local host endpoint defaults to
`postgresql://zinder_bench:zinder_bench_local_only@127.0.0.1:55432/zinder_bench`.
Its credentials are intentionally public and valid only for this loopback-bound
environment. `postgres-database` joins both the private `storage-benchmark`
network used by the benchmark client and a dedicated `postgres-host-access`
bridge that makes the loopback endpoint publishable. Docker does not publish a
host port for a container attached only to an internal network; no other
benchmark service joins the host-access bridge. The second bridge has ordinary
Docker bridge semantics, so `postgres-database` is not egress-denied. The
isolation guarantee is therefore project-private client/database traffic and
loopback-only host ingress, not database egress prevention. The digest-pinned
official database image runs only PostgreSQL and the local resource observer,
and no benchmark input depends on external network access.

The report is written as
`postgres-canonical-replay-storage-${ZINDER_BENCH_SOFTWARE_REVISION}-${ZINDER_BENCH_TRIAL_ID}.json`. This arm creates a
fresh candidate schema, streams bounded fixture segments through binary `COPY`
inside one load transaction, builds its deferred primary-key index, validates
the persisted sequence, and then commits its completion row. The report includes
the effective database settings, database image identity, and exact client and
database resource partition.

The client resource artifact is written as
`postgres-canonical-replay-storage-client-${ZINDER_BENCH_SOFTWARE_REVISION}-${ZINDER_BENCH_TRIAL_ID}.resources.json`
when the benchmark client exits. The database artifact is written as
`postgres-canonical-replay-storage-database-${ZINDER_BENCH_SOFTWARE_REVISION}-${ZINDER_BENCH_TRIAL_ID}.resources.json`
only when `postgres-database` stops, because its observer owns the full server
lifetime. Do not add a campaign row until both files exist. The database
observer samples allocated bytes across `/var/lib/postgresql`; client storage
is intentionally unsupported because durable candidate state belongs to the
database component.

The formal Compose services force a private cgroup namespace and require
`/proc/self/cgroup` to identify its v2 root before accepting readable
`memory.current` and `memory.peak` as component evidence. RocksDB and the
PostgreSQL database also require the complete candidate volume roots to be
sampleable. A host namespace, missing memory counters, or missing storage root
therefore fails the container with a configuration error before a multi-hour
trial; the standalone observer retains unsupported-source output for explicitly
exploratory uses.

## Run the PostgreSQL driver integration gate

The benchmark CLI exercises a captured corpus. The smaller driver gate creates
its own one-block fixture and directly proves the Rust driver path against the
same disposable database service:

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

The test requires a fresh database, authenticates over the loopback TCP port,
and covers SCRAM setup, binary COPY, commit, reconnect, complete persisted
read-back, WAL/storage measurement, and existing-schema rejection. Pull-request
CI runs the same profile against the same digest-pinned PostgreSQL image.

## Run a comparison campaign

A single RocksDB/PostgreSQL pair is a smoke test, not selection evidence. Run at
least five fresh-volume pairs on the same otherwise-idle host, alternate the arm
order (`RocksDB` first, then PostgreSQL first), and report the median together
with the minimum and maximum. Keep the fixture, images, resource partition,
storage class, software revision, and block-preparation concurrency fixed.
Set `ZINDER_BENCH_TRIAL_ID` to a unique value such as `trial-01` before each
pair and set `ZINDER_BENCH_FIXTURE_CACHE_POLICY` to the campaign's fixed policy.
Both values are written into each report's `provenance.run` object; the trial ID
also makes the two report paths unique.

Choose and record one fixture page-cache policy for the whole campaign. The
recommended portable policy is `warm`: immediately before each measured arm,
perform the same untimed sequential read of every fixture file. Claim `cold`
only when the runner can reset the host or Docker-VM page cache before every
arm, and record the reset mechanism. Fresh named volumes reset candidate state;
they do not clear cached pages for the bind-mounted fixture. If neither policy
can be controlled, label the run exploratory and do not use it to choose a
topology.

Keep a tab-separated campaign ledger beside the JSON reports and resource
artifacts. Its 5 artifact paths are resolved relative to the ledger. Trial
identity, cache policy, runner identity, and start/completion times come from
the artifacts rather than duplicated operator claims. Record a row only after
both arms complete successfully and the PostgreSQL database has stopped.

<!-- markdownlint-disable MD010 -->

```text
rocksdb_report	rocksdb_resources	postgres_report	postgres_client_resources	postgres_database_resources
rocksdb-canonical-replay-storage-REV-trial-01.json	rocksdb-canonical-replay-storage-REV-trial-01.resources.json	postgres-canonical-replay-storage-REV-trial-01.json	postgres-canonical-replay-storage-client-REV-trial-01.resources.json	postgres-canonical-replay-storage-database-REV-trial-01.resources.json
rocksdb-canonical-replay-storage-REV-trial-02.json	rocksdb-canonical-replay-storage-REV-trial-02.resources.json	postgres-canonical-replay-storage-REV-trial-02.json	postgres-canonical-replay-storage-client-REV-trial-02.resources.json	postgres-canonical-replay-storage-database-REV-trial-02.resources.json
```

<!-- markdownlint-enable MD010 -->

After at least five alternating pairs, validate the campaign and create the
required median/minimum/maximum summary:

```bash
scripts/validate-storage-benchmark-campaign.sh \
  benchmark-results/campaign.tsv \
  > benchmark-results/campaign-summary.json
```

The validator derives chronological trial and arm order from each report's
binary-generated start and completion timestamps. It rejects too few pairs,
overlapping trials or arms, nonalternating arm order, inconsistent cache or
runner identity, duplicate paths, hashes, trial IDs, or timestamps, incomplete
candidate evidence, mutable image identities, missing digest acceptance, and
mismatched fixture, revision, image, resource, concurrency, or engine
configuration. It also rejects failed observers, unsupported required sources,
wrong component or trial identities, uncovered report windows, excessive sample
gaps, inconsistent sample intervals, and PostgreSQL samples that cannot be
time-aligned. Compare paired trials first so background host variance is visible
before relying on the aggregate. The summary preserves every artifact path and
SHA-256 alongside chronological trial evidence and candidate
minimum/median/maximum statistics.

## Compare canonical-replay-storage reports

Compare only the 2 `canonical-replay-storage` reports. A valid pair
uses the same fixture digest, source revision, image reference, runner identity,
aggregate resource budget, storage class, and block preparation concurrency.
Both reports must show `fixture_sequence_digest_match: true` before throughput
or storage size has meaning.

The end-to-end block rate measures each complete deployment arm, including its
resource topology. It must not be presented as an isolated engine ranking:
PostgreSQL divides the aggregate budget between its client and database, while
RocksDB uses one process. Use the phase timings to explain work inside each arm,
not as interchangeable engine microbenchmarks: external-SST construction and
ingestion are not the same operation as binary `COPY`, and a RocksDB fresh open
is stronger than a PostgreSQL client reconnection. Cross-arm conclusions should
use end-to-end wall time, final storage bytes, digest equality, and the exact
recorded resource partition together.

`round_trip.benchmark_client_peak_rss` remains a client-process diagnostic, not
whole-arm memory evidence. The formal campaign metric is
`sampled_whole_arm_memory_peak_bytes`: RocksDB uses the maximum sampled
`memory.current` inside its report window; PostgreSQL adds client and database
`memory.current` only for nearest time-aligned samples within one configured
sample interval, then takes the maximum aligned sum. Never sum the independent
component `memory.peak` values: they can occur at different times. Those exact
cgroup peaks remain useful component diagnostics in each resource artifact.

The comparable storage high-water metric is
`sampled_whole_arm_storage_peak_bytes`. RocksDB samples its complete candidate
volume root, including external-SST staging, while PostgreSQL samples the
database volume root. Both metrics are sampled estimates whose resolution is
determined by their observed timestamps, not only the configured delay. The
campaign summary records that delay and the PostgreSQL alignment tolerance,
while each resource summary records the observed report-window sample gap. Use
those cadence measurements, the report's final physical byte count, and the sampled
high-water metric together.

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

The physical byte totals are useful within each engine's explicit measurement
definition, but they are not byte-for-byte equivalent layouts. Read the
engine-specific `round_trip.storage` evidence before drawing storage-efficiency
conclusions.

This first campaign establishes correctness and a reproducible baseline; it is
not the final maximum-throughput search. Once the baseline is stable, run an
explicit optimization sweep over segmented or parallel external-SST generation,
PostgreSQL COPY pipelining, and client/database CPU and memory partitions. Keep
each optimization as a named arm with the same fixture and acceptance gates.

## Use the comparison profile for integration

The `comparison` profile makes all services addressable for integration checks.
It does not preserve the isolated 8-CPU, 16-GiB budget per active candidate, so
do not publish throughput from this profile. For example, an integration check
can keep PostgreSQL available while invoking both replay clients sequentially:

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

Use clean volumes and fresh report paths before this check. The commands above
demonstrate wiring and cross-service availability, not comparative performance.

## Clean up

Remove containers and all mutable candidate state after capturing reports. The
fixture and result bind mounts remain untouched:

```bash
docker compose \
  --env-file /tmp/zinder-storage-benchmark.env \
  -f deploy/docker-compose.storage-benchmark.yml \
  --profile comparison down --volumes --remove-orphans
```
