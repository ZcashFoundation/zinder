# zinder-bench

Fixed-range capture and storage benchmark harness for measuring Zinder against
identical source bytes. It replays captured fixtures into a fresh production
canonical `RocksDB` store and drives two backend-neutral canonical-replay round
trips: one sorted external SST ingestion for `RocksDB` and binary `COPY` with
deferred index construction for PostgreSQL.

The captured fixture is the common input for every fixture-driven command.
Canonical storage arms create fresh candidate storage and compare every
persisted semantic replay envelope against the fixture's ordered digest oracle.

`zinder-bench` is a standalone binary. It links `zinder-ingest` and drives its
real canonical construction pipeline; it never ships inside a production image.

Use the [storage benchmark environment](../../deploy/storage-benchmark.md) for
the resource-bounded Docker Compose topology and repeatable storage-candidate
runs.

## 1. Capture a fixture

Point the harness at a synced Zebra JSON-RPC endpoint and capture a dense
range. The default range is an inclusive 50,001-block window.

```bash
zinder-bench capture \
  --network zcash-mainnet \
  --json-rpc-addr http://127.0.0.1:8232 \
  --from-height 150000 \
  --to-height 200000 \
  --out ./fixtures/mainnet-150k-200k
```

Optional flags: `--node-auth-cookie <path>` for cookie auth, `--segment-blocks`
(blocks per segment file, default 1000), `--fetch-concurrency` (default 16),
`--prepare-concurrency` (default 10), `--request-timeout-secs`,
`--max-response-bytes`.

The fixture directory holds one `segment-NNNNNN.bin` file per segment plus a
`manifest.json` recording the network, range, consensus activations,
canonical artifact schema version, replay tip hash, per-segment SHA-256,
and each shielded protocol's complete subtree-root prefix through the fixed
tip. Fixture format version 3 records the exact completing block identity for
every root, including roots completed before the captured block range, plus
the versioned `CanonicalBlockFacts` block and ordered-sequence digest evidence
used by both canonical-replay-storage arms. Workload totals and per-block
maxima let reviewers detect burst-dominated ranges.

### Capture canonical fixture checkpoints

Canonical replay also needs authenticated tree state immediately
before the fixture and at its fixed tip. Augment an existing fixture while the
same range remains on Zebra's best chain:

```bash
zinder-bench capture-canonical-fixture-checkpoints \
  --fixture ./fixtures/mainnet-150k-200k \
  --network zcash-mainnet \
  --json-rpc-addr http://127.0.0.1:8232
```

The command admits the manifest, activation fingerprint, every segment digest,
and every parent link before requesting 2 `z_gettreestate` checkpoints. It
writes `canonical-replay-plan.json`, which binds the predecessor and fixed-tip
frontiers to the exact manifest SHA-256. Each frontier stores Zebra's
`finalRoot` byte order and canonical `finalState` bytes; replay derives the tree
size when it decodes those bytes. The command publishes the sidecar atomically
and refuses to replace existing evidence. Optional source flags match
`capture`: `--node-auth-cookie`, `--request-timeout-secs`, and
`--max-response-bytes`.

### Replay a fixture into canonical RocksDB

Drive the production canonical construction, READY publication, independent
cold reopen, and full replay scan against an authenticated fixture:

```bash
zinder-bench rocksdb-canonical-fixture-replay \
  --fixture ./fixtures/mainnet-1730000-1734999 \
  --canonical-store ./stores/mainnet-1730000-1734999 \
  --report ./reports/mainnet-1730000-1734999.json
```

The canonical store path must be fresh and disjoint from the fixture. Defaults
are derived from the established 10 CPU, 10 GiB, 64 MiB-response profile and
use the canonical writer resource budget. Every `CanonicalPipelineLimits`
field has an explicit positive override for controlled source-admission
experiments, and `--source-segment-delay-millis` injects a fixed delay into each
outer fixture segment response.

This command certifies the exact captured bytes, the complete index-zero
subtree-root prefixes through the fixed tip, replay-plan checkpoints, canonical
READY fence, cold reopen, full scan, explicit zero prohibited reads, and
per-family cold publication scan attribution. It does not contact live Zebra
and is not live-source, advancing-tip, restart, reorg, canary, or production
certification. Physical read I/O and Linux peak RSS still require a runner that
can expose them.

### Compare raw-blob retention costs

Replay the same authenticated fixture first with transaction blobs and then
with both transaction and block blobs:

```bash
zinder-bench rocksdb-raw-blob-retention-comparison \
  --fixture ./fixtures/mainnet-1730000-1734999 \
  --transactions-canonical-store ./stores/transactions-canonical \
  --transactions-secondary-root ./stores/transactions-secondary \
  --all-canonical-store ./stores/all-canonical \
  --all-secondary-root ./stores/all-secondary \
  --report ./reports/raw-blob-retention.json
```

All four store paths must be absent, have existing parents, and be pairwise
disjoint from each other and the fixture. The command uses the same fixture,
pipeline limits, reorg policy, wallet workload, writer budget, and reader
budget for both arms. It fails instead of reporting if their authenticated
fixture, replay plan, logical replay, subtree-root load coverage and digest,
fence, or effective-limit identities differ. The report records each retention
contract, raw-blob counts, physical canonical bytes, replay throughput, and a
fresh secondary READY-admission timing. `authenticated_replay_lifecycle_seconds`
and its throughput field cover the complete authenticated replay lifecycle:
load, publication, cold reopen, and full-scan certification. They are not
isolated ingest timings, and the report records the fixed arm execution order
so cache effects remain visible.

When the harness runs inside Docker Desktop, place RocksDB stores on named
Linux volumes rather than host bind mounts. The macOS virtiofs path can report
direct-I/O support while producing padded SST file sizes that fail RocksDB
manifest validation on reopen. Bind mounts remain appropriate for immutable
fixture inputs and JSON reports; the writable canonical and materialized-view stores
must use the container VM's ext4-backed named volumes.

## 2. Measure a complete RocksDB storage lifecycle

`rocksdb-storage-lifecycle` builds fresh canonical and wallet stores from an existing
Zebra. It freezes one source tip, retains every non-genesis canonical block,
publishes and cold-admits canonical READY, derives the wallet store from the
authenticated canonical replay, then cold-admits both stores and compares
their source fences.

Both store paths and their deterministic sibling staging paths must be absent.
The optional `--tip-height` must not exceed the node tip observed at startup;
when omitted, the first observed tip is used.

```bash
zinder-bench rocksdb-storage-lifecycle \
  --network zcash-testnet \
  --json-rpc-addr http://127.0.0.1:18232 \
  --node-auth-cookie /var/run/zebra-auth/.cookie \
  --canonical-store ./state/canonical \
  --wallet-store ./state/wallet \
  --tip-height 4174755 \
  --cpu-limit-cores 10 \
  --memory-limit-bytes 10737418240 \
  --max-response-bytes 67108864 \
  --supported-reorg-depth 100 \
  --report ./rocksdb-storage-lifecycle.json
```

The closed lifecycle derives its source and block-preparation limits from the
declared CPU and memory envelope plus `--max-response-bytes`; it does not accept
independent pipeline tuning flags. The report validator recomputes those limits
before accepting evidence. Diagnostic parameter sweeps belong to
`rocksdb-canonical-fixture-replay`.

The report exposes two independent acceptance measurements:

- `canonical_storage_ready` covers source discovery, complete-history
  canonical construction, READY publication, and an independent cold reopen;
- `wallet_storage_ready` covers wallet materialized view construction after canonical readiness
  and the final cold admission of both stores.

Optional target and hard-limit pairs are
`--canonical-storage-ready-target-secs` with
`--canonical-storage-ready-hard-limit-secs`, and
`--wallet-storage-ready-target-secs` with
`--wallet-storage-ready-hard-limit-secs`. The report contains the fixed source
fence, all storage contract identities, effective source and RocksDB limits,
phase durations, row and digest evidence, external-sort evidence, physical
bytes, and peak process RSS. It certifies storage readiness only; it does not
claim query compatibility, service startup, live following, or reorg execution.

## 3. Compare canonical-replay storage engines

The canonical-replay-storage commands parse the same fixture into the same complete
`CanonicalBlockFacts` values. Each engine writes the same independently
versioned semantic replay format, reads every row back, reconstructs the full
aggregate, rejects non-canonical encodings, recomputes the independent
per-block and ordered sequence digests, and publishes a completion fence only
after validation.

The `RocksDB` arm requires a path that does not exist:

```bash
zinder-bench canonical-replay-storage rocksdb \
  --fixture ./fixtures/mainnet-150k-200k \
  --store ./candidates/rocksdb-canonical-replay-storage \
  --block-prepare-concurrency 16 \
  --software-revision "$(git rev-parse HEAD)" \
  --trial-id trial-01 \
  --fixture-cache-policy warm \
  --runner-id linux-amd64-c8-m16-nvme-01 \
  --cpu-limit-cores 8 \
  --memory-limit-bytes 17179869184 \
  --storage-class local-nvme \
  --image-reference "zinder-bench@sha256:<64-hex-digest>" \
  --report ./rocksdb-canonical-replay-storage-trial-01.json
```

The PostgreSQL arm reads its URL from an environment variable so credentials
never appear in command arguments or reports. It rejects a database where its
candidate schema already exists. The endpoint must be operator controlled; the
benchmark client does not treat a malicious PostgreSQL server as an input:

```bash
export ZINDER_BENCH_POSTGRES_DATABASE_URL='postgresql://...'
zinder-bench canonical-replay-storage postgres \
  --fixture ./fixtures/mainnet-150k-200k \
  --database-url-env ZINDER_BENCH_POSTGRES_DATABASE_URL \
  --block-prepare-concurrency 16 \
  --software-revision "$(git rev-parse HEAD)" \
  --trial-id trial-01 \
  --fixture-cache-policy warm \
  --runner-id linux-amd64-c8-m16-nvme-01 \
  --cpu-limit-cores 8 \
  --memory-limit-bytes 17179869184 \
  --client-cpu-limit-cores 2 \
  --client-memory-limit-bytes 8589934592 \
  --database-cpu-limit-cores 6 \
  --database-memory-limit-bytes 8589934592 \
  --database-image-reference "postgres@sha256:<64-hex-digest>" \
  --storage-class local-nvme \
  --image-reference "zinder-bench@sha256:<64-hex-digest>" \
  --report ./postgres-canonical-replay-storage-trial-01.json
```

The CPU and memory values in a storage report describe the complete benchmark
arm. PostgreSQL requires the client and database limits together, and their
exact sum must equal the aggregate arm limits. Its report also identifies the
database image independently from the benchmark-client image.

The concrete driver gate creates its own small fixture and requires only a
fresh disposable database URL:

```bash
ZINDER_TEST_POSTGRES_DATABASE_URL='postgresql://zinder_bench:zinder_bench_local_only@127.0.0.1:55432/zinder_bench' \
  cargo nextest run -p zinder-bench --profile=ci-postgres --run-ignored=all
```

Selection runs require a repeated, alternating-order campaign and an explicit
fixture page-cache policy. Fresh candidate volumes do not clear the host cache
for the bind-mounted fixture. The deployment runbook defines the minimum trial
count, unique trial IDs, warm/cold policy, and 5-artifact campaign ledger. Each
trial retains the RocksDB report and resource observation plus the PostgreSQL
report, client observation, and database observation. Run
`scripts/validate-storage-benchmark-campaign.sh` to reject inconsistent evidence
and compute candidate medians/minimums/maximums; a single report pair is
diagnostic only. The PostgreSQL database observation is complete only after the
server stops.

These commands prove a persisted canonical replay encoding round trip only. They
do not persist compact blocks, tree state, subtree roots, `ChainEpoch`, or
`ChainEvent`; exercise reorgs; build wallet projection or explorer materialized views; serve
queries; measure fresh canonical construction; or certify either deployment
topology.

End-to-end throughput compares the complete deployment arms under their stated
resource allocations; it is not an isolated database-engine score. The
PostgreSQL client and server have a fixed resource partition, while the RocksDB
process owns its whole arm budget. Use phase timings to explain work within each
arm, not as interchangeable engine microbenchmarks; cross-arm conclusions need
end-to-end time, storage bytes, digest equality, and resource evidence together.
The campaign summary's comparable high-water metrics are
`sampled_whole_arm_memory_peak_bytes` and
`sampled_whole_arm_storage_peak_bytes`. PostgreSQL memory is the maximum of
time-aligned client-plus-database `memory.current` samples, never the sum of
independent component peaks. The exact cgroup `memory.peak` values remain
component diagnostics in the external resource artifacts.

## Report fields

- `contract_identity`: exact benchmark report contract identity. The current identity is
  `benchmark-report`; missing or earlier identities are rejected.
- `report_format_version`: machine-readable report contract version. The
  closed measurement contract described here is version 4.
- `measurement_kind`: `rocksdb-canonical-fixture-replay`,
  `canonical-replay-storage`, or `rocksdb-storage-lifecycle`. The tagged shape
  prevents block-local replay evidence from acquiring placeholder lifecycle or
  canonical-construction fields.
- `provenance`: benchmark version, software revision, immutable image identity,
  build target OS/architecture, structured runner identity and resources, plus
  `run.trial_id`, `run.fixture_cache_policy`, and binary-generated start and
  completion Unix-millisecond timestamps.
- `storage_candidate`: identifies `rocksdb-canonical-fixture-replay`,
  `rocksdb-storage-lifecycle`, `rocksdb-canonical-replay-storage`, or
  `postgres-canonical-replay-storage`, including the logical model and the
  `rocksdb-single-host` or `postgres-scale-out` topology represented by that
  arm. This is candidate identity, not topology certification.
- `fixture.contract_identity`, `fixture.fixture_format_version`,
  `fixture.canonical_artifact_schema_version`,
  `fixture.canonical_block_facts_digest_evidence`, `fixture.tip_hash_hex`, and
  `fixture.digest_sha256`: fixture provenance required to compare candidates
  against identical source bytes and digest contracts. Each driver verifies
  every segment SHA-256 before writing its rows. Fixture format 3 requires the
  exact identity `canonical-fixture`; earlier numeric versions are refused.
- `fixture.workload_density`: the immutable workload totals and per-block
  maxima copied from the captured fixture manifest.
- Canonical-fixture `total_seconds` and `blocks_per_second`: the complete
  measured lifecycle and its end-to-end loaded-block rate.
- Canonical-fixture `resource_limits`: the CPU and memory envelope, the exact
  source-planner, admission, preparation, and deterministic delay settings, and
  the canonical `RocksDB` budget used by the run.
- Canonical-fixture `source_fetch_attribution`: completed segment requests, request and
  payload rates over replay wall time, cumulative concurrent request-task
  seconds, adaptive restart counts, and speculative discard totals. Total
  connected blocks and response payload bytes include completed responses that
  the adaptive planner later discarded and fetched again. The completed
  response bytes discarded on restart are exact for responses already held in
  the reorder buffer, but they are only a lower bound on wasted network work
  because canceled in-flight requests do not expose trustworthy actual bytes.
  The object is `null` when no metrics recorder covered completed segment
  requests.
- Canonical-fixture `prohibited_reads`: explicit counters for historical
  prevout and cross-block wallet reads. Both metric series must be present and
  zero; missing telemetry fails report validation.
- Canonical-fixture `publication_proof_provenance` records either
  `trusted-fresh-writer` or `cold-certification`. `publication_family_scans`
  must be empty for the trusted fresh-writer path and must contain successful
  cache-bypassing scans grouped by column family for cold certification.
- `benchmark_client_peak_rss`: peak resident bytes (Linux
  `/proc/self/status` `VmHWM`; reported as unavailable off Linux).
- `stage_durations`: cumulative task seconds and call counts for
  canonical-construction stages (`block_parse`, `identity_validation`, `compact_artifacts`,
  `transaction_facts`, `block_header_artifact`, `block_blob`, and `block_replay`).
- `round_trip`: block-local replay wall time plus shared initialization, preparation,
  persistence, index-construction, storage-optimization, validation,
  publication, fresh-reader-validation, and storage-measurement phase times. Any
  framework overhead between those timers is explicit as
  `unattributed_wall_clock_seconds`. The report also records range identity,
  block rate, logical and physical bytes, persisted sequence digest,
  digest-match result, `replay_format_version`, full semantic-replay validation,
  and process peak RSS. These names provide a common diagnostic vocabulary,
  not interchangeable engine microbenchmarks. In the fresh-reader phase
  RocksDB closes and reopens its files, while PostgreSQL closes the client
  connection and reconnects to the running server; neither result certifies a
  database-server restart.
- `round_trip.storage`: engine-specific evidence. The `RocksDB` variant records
  its schema, external-SST bytes, explicit compression, bounded resource budget,
  durability mode, the resolved database I/O mode, and the separately recorded
  buffered external-SST construction mode. The PostgreSQL variant records
  schema, explicit `lz4` replay-encoding compression, the queried comparison
  and durability server settings, table/index/WAL bytes, both component resource
  shares, and the immutable database image identity. Retain the rendered Compose
  model for initdb, shared-memory, and temporary-filesystem evidence.

Container resource observations are separate, versioned evidence artifacts
rather than fields inside the process-generated report. This keeps the report
honest about what the benchmark process can observe while allowing the campaign
validator to prove private component-cgroup scope, exact candidate storage
roots, and complete report-window coverage for every component.

## Scope and faithfulness

- Source transport differs from production (fixture files instead of JSON-RPC),
  so zero-delay source-fetch timing is not representative. The optional fixed
  delay supports controlled latency comparisons, but it does not reproduce
  Zebra response generation, network jitter, or JSON and hexadecimal decoding.
  Fixture replay parses only the block header before handing the payload to
  canonical preparation, matching the production batch-source boundary.
  Everything downstream runs the production canonical construction pipeline.
- Each shielded protocol's complete subtree-root prefix through the fixed tip is
  captured from index zero. Every root carries the exact block identity that
  completed it; identities inside the fixture range are checked against the
  captured block bytes before replay.
- Canonical replay storage runs use the production block parser and complete
  `CanonicalBlockFacts` semantic replay format, but their physical schemas are
  diagnostic vertical slices rather than production canonical stores.
- The PostgreSQL slice uses the exact production-intended `tokio-postgres`
  driver. The prescribed Compose and CI clusters require SCRAM-SHA-256 host
  authentication; an arbitrary operator-supplied database URL does not itself
  prove the session's authentication method. `NoTls` is limited to the isolated
  benchmark network; production remote transport still requires a
  certificate-validated TLS connector.
