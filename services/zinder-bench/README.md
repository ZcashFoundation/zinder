# zinder-bench

Fixed-range capture and storage benchmark harness for measuring Zinder against
identical source bytes. It replays captured fixtures into a fresh production
canonical `RocksDB` store and drives two backend-neutral canonical-replay round
trips: one sorted external SST ingestion for `RocksDB` and binary `COPY` with
deferred index construction for PostgreSQL. It also replays captured tip blocks
through the production canonical follower and wallet transition to measure
per-block cost, and replays a deep reorg at the tip to measure one wallet
transition against its logical-byte ceiling. Separately, it drives
wallet-shaped sync load at a running lightwalletd-compatible endpoint.

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
tip. Fixture format version 1 records the exact completing block identity for
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
are derived from the established 10 CPU, 10 GiB, 64 MiB-response profile, use
the canonical writer resource budget, and persist checkpoints at the predecessor,
every 100 blocks, and at the build tip. This matches fresh ingest with `explicit` coverage.
Pass `--tree-state-checkpoint-coverage every-block` to measure dense construction.
The report records the constructed coverage in
`resource_limits.tree_state_checkpoint_coverage` and verifies
`source_load.tree_state_checkpoint_count` against it. Every
`CanonicalPipelineLimits` field has an explicit positive override for controlled
source-admission experiments, including `--tree-hashing-workers` for the
loader's commitment-tree hashing threads, and `--source-segment-delay-millis`
injects a fixed delay into each outer fixture segment response. The report's
`stage_durations` include the ordered loader's stages
(`canonical_block_load`) and the store's per-block stages
(`canonical_block_store_load`), and the replay requires every subtree root the
loaded blocks complete to equal the fixture's node-observed root.

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
pipeline limits, tree-state checkpoint coverage (`cadence-and-tip` by default, or
`every-block` via `--tree-state-checkpoint-coverage`), reorg policy, wallet
workload, writer budget, and reader budget for both arms. It fails instead of
reporting if their authenticated fixture, replay plan, logical replay,
subtree-root load coverage and digest, fence, or effective-limit identities
differ. Report format version 1 records each retention
contract, `effective_limits.tree_state_checkpoint_coverage`, raw-blob counts,
physical canonical bytes, replay throughput, and a
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
independent pipeline tuning flags. Canonical construction defaults to
`--tree-state-checkpoint-coverage cadence-and-tip` to match fresh ingest with `explicit` coverage.
Pass `--tree-state-checkpoint-coverage every-block` to measure the cost of
writing dense checkpoints before READY, as wallet-serving ingest does by default. Report
format 1 records the constructed coverage in
`resource_limits.tree_state_checkpoint_coverage`, the canonical publication mode
in `resource_limits.canonical_publication_mode` (`trusted-fresh` or
`cold-certified`), and the persisted row count in
`canonical_storage_ready.tree_state_checkpoint_count`. The validator recomputes
the limits and checkpoint counts and requires wallet schema 1 and the
`lthash16-chacha12` UTXO commitment scheme.
Diagnostic parameter sweeps belong to `rocksdb-canonical-fixture-replay`.

By default, `--canonical-publication-mode trusted-fresh` uses the same
prepublication checks as fresh ingest. Use `--canonical-publication-mode
cold-certified` in a separate run to measure full certification. The report
identifies the mode in `resource_limits.canonical_publication_mode`.

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
bytes, and peak process RSS. Its wallet section, like the
`rocksdb-wallet-rebuild` report, lists `build_stages`: the wallet build's
non-overlapping stages in order, the same stages the projector records in
`zinder_wallet_projection_build_stage_duration_seconds`, so the stage seconds
add up to at most the build's total. It certifies storage readiness only; it does not
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

## 4. Measure per-block cost at the tip

`rocksdb-tip-replay` measures what each new block costs the canonical writer
and the wallet projector. It copies a closed canonical store and a closed
wallet store, both at a start height, into a scratch directory. For every
block of a fixture that starts at the next height, it runs one production
follow iteration of `zinder-ingest` (`follow_canonical_tip` stopping at that
height), then catches up a canonical secondary and applies the block's
retained event with the production wallet transition, as `zinder-projector`
does. The canonical and wallet stores stay separate databases, as they are in
production. The input stores and the fixture are never written.

Use it to measure one change at a time: replay the same inputs before and
after the change, then compare the two reports.

### Prepare the inputs

You need 3 inputs that line up at a start height `H`:

1. A READY canonical store and a READY wallet store whose tip is `H`. Build
   both with `rocksdb-storage-lifecycle --tip-height H`, or rebuild the wallet
   for an existing canonical store at `H` with `rocksdb-wallet-rebuild`.
2. A fixture of blocks `H+1` through `T`, captured with `capture`.
3. The node's tree-state checkpoint after every fixture block. Live following
   compares each derived checkpoint with the node's, so the fixture needs one
   per height:

   ```bash
   zinder-bench capture-tip-replay-checkpoints \
     --fixture ./fixtures/testnet-tip \
     --network zcash-testnet \
     --json-rpc-addr http://127.0.0.1:18232 \
     --node-auth-cookie /var/run/zebra-auth/.cookie
   ```

   The command admits the manifest, segment digests, and block linkage before
   it calls `z_gettreestate`, checks that each checkpoint names the captured
   block, and writes `tip-replay-checkpoints.json` beside the manifest. It
   refuses to replace an existing sidecar. `--fetch-concurrency` defaults to
   16.

### Run a replay

```bash
zinder-bench rocksdb-tip-replay \
  --fixture ./fixtures/testnet-tip \
  --canonical-store ./stores/canonical-at-h \
  --wallet-store ./stores/wallet-at-h \
  --scratch-dir ./scratch/tip-replay \
  --report ./reports/tip-replay-baseline.json \
  --software-revision "$(git rev-parse HEAD)" \
  --repeat 3
```

Both stores must be closed. `--scratch-dir` and `--report` must not exist, and
the command refuses to start when the scratch filesystem lacks room for one
copy of both stores plus 2 GiB. Each repeat replays from fresh copies, and
the command fails unless every repeat ends in the same canonical fence and
wallet digest. If a fixture read fails during the replay, for example because
a segment file was removed, the run ends with that source error and writes no
report. The replay does not retry it the way the writer retries a node outage.

| Option | Default | Effect |
| --- | --- | --- |
| `--repeat` | `1` | Replays, each from fresh scratch copies |
| `--raw-blob-retention` | `transactions` | Retention persisted in the canonical store |
| `--reorg-window-blocks` | `1000` | Canonical reorg window, as `zinder-ingest` passes it |
| `--max-transition-logical-bytes` | `536870912` | Wallet transition ceiling, the projector default |
| `--canonical-block-cache-bytes` | writer default | Canonical writer block cache |
| `--canonical-secondary-block-cache-bytes` | reader default | Canonical secondary block cache |
| `--wallet-block-cache-bytes` | wallet writer default | Wallet block cache |
| `--canonical-io-mode` | `direct` | I/O mode of the canonical writer and its secondary: `direct` or `buffered` |
| `--wallet-io-mode` | `direct` | I/O mode of the wallet store: `direct` or `buffered` |
| `--wallet-rocksdb-statistics` | off | Collects the wallet store's `RocksDB` statistics tickers and reports per-block deltas |
| `--keep-scratch` | off | Keeps the final repeat's scratch stores |
| `--label` | none | Free-form label copied into the report |

The budgets are resolved by the same functions the services use for their
configuration, with only the block cache, the I/O mode, and the wallet
statistics level overridden.

`direct` is the production policy: each store opens with direct reads and
falls back to buffered reads when the filesystem rejects direct I/O.
`buffered` skips the direct attempt, so reads, flushes, and compactions of
that store go through the page cache. The report records the mode each store
requested and the mode it resolved, so a `direct` request that fell back shows
as `buffered` in `resolved`. To measure the read path, run the same inputs once
per mode with every other option unchanged, and compare the reports.

Writing a scratch copy leaves its pages in the page cache, where a buffered
store would serve its first reads from memory while a direct store reads from
the device. On Linux, each repeat therefore evicts its copies from the page
cache after syncing them, so both modes open cold stores, and the report's
`method.scratch_page_cache` is `evicted-after-copy`. Other platforms record
`not-evicted`; do not compare I/O modes there. A buffered store still caches
what it reads during the replay, as much as the host's free memory allows, so
a buffered result on a host with spare memory describes an uncontended page
cache rather than a memory-limited container.

### Wallet statistics

`--wallet-rocksdb-statistics` reads the wallet store's `RocksDB` statistics
tickers around each block's wallet transition and reports what each ticker
counted. The reads sit outside the `wallet_apply` timer. Collection changes the
wallet store's options, so it is off by default, and an off run opens the
wallet store with `statistics_level` `off`. The production default is
`tickers`. A run with the option on opens it with `tickers`, so the difference
between an off run and an on run includes the cost of collection.

Tickers count DB-wide across every column family of the wallet store, and they
also count its background work. A flush or compaction that runs while a block's
transition runs adds its own block reads and cache misses to that block's
deltas, so a block's counts can reflect when a compaction ran rather than what
the transition read. A nonzero `rocksdb.compact.read.bytes` or
`rocksdb.flush.write.bytes` marks such a block. `RocksDB` records compaction
bytes periodically while the compaction runs and flush bytes when the flush
ends, so a block with zero in both can still overlap the first moments of a
job. Background work that runs between two transitions is not counted.

The report keeps these tickers, named as `RocksDB` names them:

| Ticker | Reading |
| --- | --- |
| `rocksdb.number.keys.read` | Point reads (`Get`) |
| `rocksdb.number.multiget.keys.read` | Keys read through `MultiGet` |
| `rocksdb.number.db.seek` | Iterator seeks |
| `rocksdb.bloom.filter.useful` | Reads a filter answered without touching a data block |
| `rocksdb.bloom.filter.full.positive` | Filter probes that answered "maybe present" |
| `rocksdb.bloom.filter.full.true.positive` | Filter probes whose key was present |
| `rocksdb.block.cache.data.miss` | Data blocks read from the store's files, by the transition or a concurrent compaction; `RocksDB` has no separate data-block-read ticker |
| `rocksdb.block.cache.data.hit` | Data blocks served from the block cache |
| `rocksdb.block.cache.filter.miss`, `.filter.hit` | Filter partitions read from files or the cache |
| `rocksdb.block.cache.index.miss`, `.index.hit` | Index partitions read from files or the cache |
| `rocksdb.bytes.read` | Bytes returned by point reads |
| `rocksdb.compact.read.bytes`, `.compact.write.bytes` | Bytes background compactions read and wrote |
| `rocksdb.flush.write.bytes` | Bytes background flushes wrote |

The canonical stores expose no statistics, so the report lists them under
`unavailable_measurements` when the option is on.

To confirm that the replay produced the production result, run with
`--keep-scratch` and rebuild a wallet from the kept canonical store with
`rocksdb-wallet-rebuild`. Its `wallet_projection_digest_hex` must equal the
report's `final_wallet.projection_digest_hex`.

### What each block measures

| Timer | Covers |
| --- | --- |
| `canonical_follow_step` | One follow iteration: fixture reads, settlement, preparation, subtree roots, commit, readiness |
| `canonical_prepare` | The follower's `zinder_ingest_canonical_live_prepare_seconds` sample |
| `canonical_commit` | The follower's `zinder_ingest_canonical_live_commit_seconds` sample, around `commit_live_append` |
| `canonical_secondary_catch_up` | Secondary catch-up to the new writer fence |
| `wallet_apply` | `apply_canonical_event_range`, including its replay-row reads from the secondary and the predecessor row that seeds linkage checking |
| `wallet_step` | Catch-up, the retained-event and epoch reads, and the wallet transition |
| `block_total` | `canonical_follow_step` plus `wallet_step` |

Each block also records its height, transactions, non-coinbase transparent
inputs, transparent outputs, and coinbase transparent outputs. The replay
excludes the gRPC control plane between the writer and the projector
(event pages, writer status, and retention leases), the mempool, query
serving, and the writer's age-based event pruning. Source reads come from
fixture files instead of JSON-RPC.

Two differences from a deployment shape every number. Blocks run one at a
time: each block's wallet transition finishes before the next canonical
append starts, while in production the projector trails the writer
concurrently. The writer, the secondary, and the wallet also share one
process, so process CPU time and peak resident memory cover all three.

### Read the report

The report's `contract_identity` is `rocksdb-tip-replay`. It records:

- `provenance`: the benchmark version, `--software-revision`, the label, the
  build (Cargo profile directory and kind, opt-level, debug info, debug
  assertions, every rustflag Cargo passed, the `rustc` version, and the target),
  the host, and start and completion times.
- `inputs`: the fixture identity, the checkpoint sidecar SHA-256, the
  fixture's workload density, and each input store's path, size, and start
  state.
- `configuration`: the settings above, every resolved `RocksDB` budget, the
  requested and resolved I/O mode of each store, and `wallet_statistics`
  (whether tickers were collected, and which).
- `method`: the percentile method, the stratum thresholds, and
  `scratch_page_cache`, the page-cache state of the scratch copies when each
  repeat opens them.
- `process`: whole-process wall time, CPU time across all threads, and peak
  resident memory.
- `aggregate`: timer summaries pooled over every block of every repeat.
- `repeats`: for each repeat, the copy and open times, the block loop's wall
  and CPU time, final canonical and wallet state, timer summaries, and every
  block's record.
- `unavailable_measurements`: measurements the run does not provide. Without
  `--wallet-rocksdb-statistics` that is point-read counts and `RocksDB`
  statistics deltas. With it, only the canonical stores' read statistics
  remain.

Every timer summary reports `count`, `mean_ms`, `p50_ms`, `p99_ms`, `p999_ms`,
`max_ms`, and `total_ms`. Percentiles use the nearest-rank method: the sample
at 1-based rank `ceil(n * p)` of the `n` ascending samples. The summaries are
repeated for each workload stratum. A block can be in more than one stratum,
and a stratum with no blocks has `timers: null`.

With `--wallet-rocksdb-statistics`, each block records `wallet_ticker_deltas`,
and each stratum records `wallet_tickers`: for every ticker, the `total` over
the stratum's blocks, `mean_per_block`, and the nearest-rank `p50`, `p99`, and
`max` of the per-block deltas. Without the option, `wallet_ticker_deltas` is
absent and `wallet_tickers` is `null`.

The report format is version 2. Version 2 replaced
`configuration.rocksdb_io_mode.selectable` with `requested`, added
`configuration.wallet_statistics` and `method.scratch_page_cache`, and added
the per-block and per-stratum ticker fields. `compare-tip-replay-reports`
refuses version 1 reports.

| Stratum | Blocks |
| --- | --- |
| `no_transparent_activity` | No non-coinbase transparent inputs or outputs |
| `near_empty_after_dense` | At most 10 non-coinbase transparent inputs plus outputs, within 1,000 heights after a block with more than 1,000 transparent inputs or outputs |
| `input_heavy` | At least 1,000 non-coinbase transparent inputs |
| `fan_out` | At least 1,000 transparent outputs and no non-coinbase transparent inputs |

Dense blocks among the 1,000 retained blocks before the fixture count for
`near_empty_after_dense`.

### Compare 2 reports

```bash
zinder-bench compare-tip-replay-reports \
  --baseline ./reports/tip-replay-baseline.json \
  --candidate ./reports/tip-replay-candidate.json \
  --report ./reports/tip-replay-comparison.json
```

The comparison refuses reports of different fixtures or start states, and
reports whose final canonical or wallet state differs, because a performance
change must not change the result. It also refuses reports with different
`--repeat` counts, because the pooled summaries count every block of every
repeat, so their totals and block counts grow with the repeat count. It
refuses a report that collected wallet statistics against one that did not,
because collection changes the wallet store's options and can change timings. It
refuses reports whose stratum rules differ, because those rules decide which
blocks each stratum holds, and reports whose percentile method is not
`nearest-rank`. It lists every build, host, configuration, and method setting
that differs, so you can confirm that only the intended variable changed. For each stratum and timer it gives
the baseline, the candidate, and the percent change of every summary
statistic, plus the mean block-loop wall and CPU time and the peak resident
memory. When both reports collected wallet statistics, it also gives each
stratum's mean per-block delta of every ticker. Without `--report`, it writes
to stdout.

## 5. Measure a deep reorg at the tip

`rocksdb-tip-reorg` tells you whether a reorg of the newest blocks fits the
wallet transition ceiling, `projector.follow.max_transition_logical_bytes`,
and how much memory the transition takes. It uses the same inputs as
`rocksdb-tip-replay` and runs these steps:

1. It copies both stores into a scratch directory and replays the fixture
   through `--reorg-tip-height`, one block at a time, as a tip replay does.
2. It builds a competing branch. The branch rewrites the `--depth` newest
   blocks and adds one block on top, so it is longer than the stored chain.
3. The production canonical follower switches to that branch in one suffix
   replacement, then appends the extra block. This records one `Reorged`
   event and one `Committed` event.
4. The wallet applies the `Reorged` event the way `zinder-projector` applies
   one retained event. It calls `apply_canonical_event_range_cancellable`
   with the replay rows of the event's committed range and the ceiling you
   set.

The fixture must hold every block from the store tip through one block past
`--reorg-tip-height`, with its tip replay checkpoints. The branch rewrites
fixture blocks, so the store tip must be at least `--depth` blocks below
`--reorg-tip-height`. The reorg must also start above the canonical settled
tip, so `--depth` can be at most `--reorg-window-blocks`.

The reorg window must be full when the branch arrives. The follower replaces
blocks up to the settled tip plus the window. When `--reorg-window-blocks`
raises the store's window, settlement stays where it was until the replay
passes it by the whole new window. Before that point the replacement would
also take in the branch's extra block, so the command refuses to build the
branch and names the lowest `--reorg-tip-height` that fills the window.

```bash
zinder-bench rocksdb-tip-reorg \
  --fixture ./fixtures/testnet-tip \
  --canonical-store ./stores/canonical-at-h \
  --wallet-store ./stores/wallet-at-h \
  --scratch-dir ./scratch/tip-reorg \
  --report ./reports/tip-reorg.json \
  --software-revision "$(git rev-parse HEAD)" \
  --reorg-tip-height 4618271 \
  --depth 1000 \
  --fork distinct-coinbases
```

| Option | Default | Effect |
| --- | --- | --- |
| `--reorg-tip-height` | required | Newest block the reorg replaces |
| `--depth` | `1000` | Blocks the reorg replaces, ending at `--reorg-tip-height` |
| `--fork` | `distinct-coinbases` | How the branch differs from the blocks it replaces |
| `--max-transition-logical-bytes` | `536870912` | Ceiling for every wallet transition, the reorg included |
| `--keep-scratch` | off | Keeps the scratch stores after the run |

The store options of `rocksdb-tip-replay` work the same way here:
`--raw-blob-retention`, `--reorg-window-blocks`, the three block-cache
settings, `--canonical-io-mode`, `--wallet-io-mode`, and
`--wallet-rocksdb-statistics`. The report's `configuration` records the
requested I/O modes in `requested_io_modes` and the statistics setting in
`wallet_statistics`. The top-level `io_modes` records the modes the stores
resolved, and is `null` after a refusal, which closes the stores early.

The two branch kinds bracket a real fork:

- `same-transactions` keeps every transaction and changes only block
  identities. The wallet plan then rewrites the same outpoints it rolls back.
- `distinct-coinbases` changes each coinbase's lock time, so every coinbase
  gets a new txid. A transaction that spends a renamed output gets the new
  txid in its input, and so a new txid of its own.

Both kinds leave shielded data unchanged, so every rewritten block keeps the
node's tree state. They do not update the block commitments field, and the
rewritten headers carry no valid proof of work. Zinder checks neither.

A refused transition is a result, not a failure: the command still writes its
report. The report's `wallet_transition` object records:

- `outcome`: `admitted` or `refused`.
- `refused_required_bytes`: the refusal's `required_bytes`, which is a lower
  bound on the whole plan, and `wallet_unchanged`, which checks that READY
  evidence did not move.
- `footprint`, for an admitted transition: the planner's
  `accounted_logical_bytes`, its distinct `planned_rows`, the published
  batch's `batch_records` and `batch_bytes`, its `durable_point_reads`, and
  the synchronous `write_seconds`. `plan_seconds` is the rest of the call.
- `elapsed_seconds`: the whole call, from the replay-row scan through the
  write or the refusal.
- `memory`: resident memory before the call, `VmHWM` reset just before it and
  read after it (`peak_rss_bytes`), a 20 ms `VmRSS` sampler's peak, and
  resident memory after it. The process also holds the canonical writer, the
  secondary, and the fixture, so these numbers cover all of them.
- `wallet_ticker_deltas`, with `--wallet-rocksdb-statistics`: what each
  ticker of [Wallet statistics](#wallet-statistics) counted across the call,
  admitted or refused, with the same background-work caveats. It is `null`
  without the option.

The report also records the catch-up time, the replaced range's transparent
workload, what the branch rewrote, and the follower's replacement timings.
An admitted run ends with the wallet at the branch tip. Check it against a
cold rebuild as you would a tip replay: run with `--keep-scratch`, then
rebuild a wallet from the kept canonical store with `rocksdb-wallet-rebuild`.

## 6. Measure tree-state reconstruction at wallet anchors

`TreeStateAtHeight` and lightwalletd `GetTreeState` answer a height without a
stored checkpoint by replaying stored compact blocks from the checkpoint below
it. Sparse construction (`--tree-state-checkpoint-coverage cadence-and-tip`)
checkpoints every 100 blocks, and a wallet that scans in 100-block rounds
anchors one block below each boundary, at `k * 100 - 1`, so every such request
replays the full 99-block stride. Measure that case on a READY canonical store,
such as one built with
`rocksdb-canonical-fixture-replay --tree-state-checkpoint-coverage cadence-and-tip`:

```bash
zinder-bench rocksdb-tree-state-replay \
  --fixture ./fixtures/mainnet-1710k-1850k \
  --canonical-store ./candidate/canonical \
  --secondary-root ./candidate/tree-state-secondary \
  --software-revision "$(git rev-parse --short HEAD)" \
  --from-height 1710000 \
  --to-height 1850000 \
  --max-anchors 128 \
  --report ./reports/tree-state-replay.json
```

The command opens the store through an admitted secondary, reconstructs the
tree state at up to `--max-anchors` anchors spread evenly over the window, and
writes a `rocksdb-tree-state-replay` report. Each anchor is reconstructed twice
in a row before the next one: first cold, then warm from the block cache the
first replay filled. Measuring the warm replay at once keeps later anchors from
evicting an earlier anchor's blocks when the sampled ranges exceed the
secondary's 128 MiB block cache. `--from-height` and `--to-height` default to
the retained range; the example window covers the 2022 Sapling output spam,
the densest history a reconstruction replays. Anchors that hold a stored
checkpoint, as every live-followed block does, need no reconstruction and are
counted in `stored_checkpoint_anchors_skipped`. Each anchor records its seed
checkpoint, the replayed blocks and note commitments, and its cold and warm
seconds. `cold_seconds` and `warm_seconds` summarize the anchors as
nearest-rank p50, p90, and p99 and the maximum.

To compare the same historical reads across sparse and dense stores, replace
`--from-height`, `--to-height`, and `--max-anchors` with
`--heights-file ./frontiers.txt`. The file contains one decimal u32 height per
line. Every listed height is measured in order, including duplicates and stored
checkpoints; no sampling limit applies. The explicit window includes the build
plan's history-predecessor checkpoint through the visible tip. For a full-history
store whose first retained block is 1, height 0 is a valid stored read. Empty
files, blank or malformed lines, and heights outside that served window fail
the command. This option conflicts
with the three sampling options; omitting it preserves the default sampling and
stored-checkpoint skipping behavior.

Report format version 3 adds `heights_file_sha256` (null for default sampling)
and `source` (`stored` or `replayed`) on each anchor. `read_path` distinguishes
`wallet-query-store` for explicit heights from the default
`validated-checkpoint` diagnostic. Explicit reads measure `tree_state_lookup`
and, when needed, `replay_tree_state` together, matching WalletQuery's storage
path without repeating final checkpoint root validation. Selection performs no
seed reads outside that measured region. These counters cover the store calls,
not RPC admission, scheduling, response encoding, or transport. Stored outcomes record zero
replayed blocks and commitments while retaining the same cold/warm timing and
RocksDB counters as replays. Logical key reads and thread-local block read
counts/bytes come from the existing read measurement primitive. The labels
cold/warm describe first and immediate repeat reads in this secondary; they do
not prove an empty operating-system page cache. Use identical height-file and
binary digests for paired sparse/dense measurements. These local read counters
do not measure a network request or wallet scan end to end.

### Attribute compact-block and subtree-root reads

Use the following release build to match the x86-64 CPU flags used in Zinder's
release images. Keep the executable and inputs fixed when comparing read paths.

```bash
CARGO_TARGET_DIR=.tmp/target-v3 \
CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq --cfg chacha20_avx512" \
  cargo build --release --locked -p zinder-bench

bench=.tmp/target-v3/release/zinder-bench
store=(--fixture ./fixtures/testnet --canonical-store ./candidate/canonical
       --software-revision "$(git rev-parse HEAD)")
mkdir -p .tmp/reads
"$bench" rocksdb-compact-block-range "${store[@]}" \
  --secondary-root .tmp/reads/ranges --rocksdb-statistics-level full \
  --report .tmp/reads/ranges.json
"$bench" rocksdb-subtree-roots "${store[@]}" --protocol orchard --max-entries 1024 \
  --secondary-root .tmp/reads/roots --report .tmp/reads/roots.json
"$bench" rocksdb-store-properties "${store[@]}" \
  --secondary-root .tmp/reads/properties --report .tmp/reads/properties.json
```

These commands and `rocksdb-tree-state-replay` admit the READY primary
read-only. Give each invocation an absent `--secondary-root` under an
existing directory outside the primary. `--report` writes a new JSON file
outside the primary. Omit it to print JSON to stdout. Set `--workload`
(`wallet` or `explorer`), `--raw-blob-retention` (`none`, `transactions`, or
`all`), and `--supported-reorg-depth` to the values persisted in the store.
The defaults are `wallet`, `transactions`, and 1,000 blocks. Pass
`--canonical-io-mode direct|buffered` (default `direct`) to compare direct
and buffered canonical reads, and `--evict-page-cache` on Linux to sync each
file in `--canonical-store` and drop its cached pages with
`posix_fadvise(POSIX_FADV_DONTNEED)` before each cold secondary open.
Reproducible cold measurements require a quiescent store because concurrent
reads or writes can repopulate the OS page cache.

The compact-block report (version 4) retains the 128-, 512-, and 1,024-block
unbounded calls in `cases`. Each entry in `bounded_cases` measures one
`compact_blocks_in_range_up_to` call over the same final 1,000 heights. The
256 KiB opening budget and the 2 MiB steady budget each run independently in
both directions. Descending reads select the high end of the range and return
their rows in height order. A single oversized block occupies a whole chunk.
Use `output.block_count`, `first_height`, `last_height`, and `stored_bytes` to
see what fit. Throughput uses the returned block count. The structured digest
checks that all four reads of a case agree.

The subtree command measures one page in `--protocol` (`sapling` by default,
`orchard`, or `ironwood`). Its default start is the first root completed in
retained history. Use `--start-index` to select another page.
`--max-entries` accepts 1 through 1,024 and defaults to 1,024. The visible
tree bounds the page. The command requires at least one available root and
checks the returned indices, root hashes, and completing blocks across reads.

Each range and subtree case opens a fresh secondary for the direct read and
another at the same READY fence for the query reader. **Cold** is the first
measured read after admission. **Warm** repeats it on the same handle. Admission
may already have populated the block cache, and the host page cache persists
across opens. Tree replay uses one secondary for the window. Its checkpoint
discovery and earlier anchors can warm later reads.

`--rocksdb-statistics-level` defaults to `tickers`. At `off`, ticker and
histogram deltas are null. At `tickers`, each read records DB-wide counter
deltas for Gets, MultiGets, seeks, iterator steps, bytes, data and index cache
hits and misses, file opens and errors, and prefetch. At `full`, each of the
six read histograms also reports the change in sample count and sum in
microseconds. **Full and Tickers runs are not directly comparable** because
collecting the histograms adds work.

All three read benchmarks measure thread-local PerfContext at every statistics
level. Times are nanoseconds. Block read bytes include separate data, index, and
filter counters. A cache hit can serve bytes without a file read. Query-reader
latency includes dispatch to and return from the blocking pool, with counters
sampled on the thread that performs the read. Timers exclude taking snapshots
and hashing the output. Tree replay report version 2 adds `cold_counters` and
`warm_counters` to each anchor. Each report records the cache budget, statistics
level, host CPU model when available, Cargo profile, compiler, target, Rust
flags, explicit target CPU, and compiled target features.

### Inspect column-family properties

`rocksdb-store-properties` reports the default family and every canonical
data family from one admitted secondary. Each row includes total SST bytes,
estimated live bytes, and the file count and estimated compression ratio at
each level from 0 to 6. A null ratio means RocksDB has no estimate. The
parsed aggregate table properties include index, filter, and data bytes,
raw key and value bytes, and entry and block counts. RocksDB omits format
versions from this aggregate, so `sst_files_by_format_version` counts the
version in each live block-based SST's read-only footer. Compression ratios
describe the existing files. Measuring savings from another codec requires a
separate recompression run on copies.

## 7. Drive wallet-shaped load at an endpoint

For a two-binary A/B comparison on a private fixture, follow [Wallet performance qualification](../../docs/runbooks/wallet-performance-qualification.md).

`lightwalletd-wallet-load` syncs a height range from a running
lightwalletd-compatible endpoint the way a real wallet engine does, for many
simulated wallets at once. The endpoint can be `zinder-compat-lightwalletd`, Go
lightwalletd, or any other `CompactTxStreamer` server, over `http://` or, through
a TLS proxy such as the Caddy front of the wallet image, over `https://` with
the host's trusted roots. To measure through a proxy that uses a private CA,
pass `--tls-ca-cert` with the CA's PEM file, which must hold at least one
usable CA certificate. When the endpoint is a loopback address or an IP, pass
`--tls-domain-name` with the proxy's site name. The connection dials the
endpoint's address and sends that name both as the TLS server name and as the
HTTP/2 `:authority`, with the endpoint's port when it is not 443, so a proxy
that routes by host, such as the Caddy site in
[`deploy/compat-lightwalletd.Caddyfile`](../../deploy/compat-lightwalletd.Caddyfile),
matches the requests. Both flags need an `https://` endpoint. With
`--tls-ca-cert`, the connection trusts only that CA, not the host's roots, so
it also works on a host whose platform trust store is empty. Run the same command against each
server to compare them under the same load.

```bash
zinder-bench lightwalletd-wallet-load \
  --endpoint https://wallet.example:443 \
  --workload slipstream \
  --wallets 20 \
  --from-height 3424811 \
  --target-label "zinder 0.18.0, 8 vCPU" \
  --report ./reports/slipstream-20.json
```

`--from-height` is the first block each wallet syncs. A restored wallet starts
one block above the checkpoint it was restored from, so `3424811` puts the
first tree-state request on the Android SDK checkpoint at 3,424,810, 10 blocks
off Zinder's 100-block checkpoint cadence. `--to-height` defaults to the
endpoint's latest block. `--wallet-start-interval-millis` staggers wallet
starts.

Each `--workload` follows one engine's request pattern, as read from its
source:

- `slipstream`: the Rust engine in the ZODL Android and iOS apps, with Android
  defaults and a direct connection. It asks for the tree state below the range,
  then runs 4 block workers, each on its own connection. The first request is
  a 64-block probe. Later requests are sized to 16 MiB at the density received
  so far, at most 10,000 blocks. Each 8 MiB piece of a stream (`--split-bytes`)
  except the range's last asks for the tree state at its last block. Pieces
  waiting on their tree states hold at most 256 MiB, Slipstream's chunk-queue
  budget, so a worker whose piece does not fit stops reading its stream until
  a tree state arrives. A failed request hands on the blocks it received and
  resumes after them on a fresh connection after 0.25, 0.5, and 1 seconds.
  Only a stream that ends cleanly after the request's last block completes
  it, so a stream that sends a block past the request or fails after its last
  block is retried from the start of that block's piece. A failed tree state
  is retried after 1 and 3 seconds. Once the blocks are in, the wallet fetches
  `--transactions-per-wallet` transactions from them, 8 at a time.
- `vizor`: the Vizor wallet. Each batch streams its blocks with a pool filter
  naming Sapling, Orchard, and Ironwood and asks for the tree state below its
  first block at the same time. After each batch arrives, up to 4 more download
  ahead. Batches are sized by `--vizor-profile`: `mobile` (11.3 MB, at most
  1,000 blocks) or `desktop` (31 MB, at most 10,000 blocks), or by
  `--batch-byte-budget`. A queued batch that fails fast is retried after 1 and
  4 seconds and then downloaded again in line; a failed in-line batch restarts
  the sync after 2, 4, and 8 seconds.
- `classic-sdk`: the classic Zcash Android SDK `CompactBlockProcessor`. One
  connection downloads serial 1,000-block batches (100 where a batch touches
  heights 1,710,000 to 2,050,000), each after the tree state below its first
  block, with an empty pool filter. Each call gets 5 attempts.
- `point-reads`: each wallet repeats `--rounds` rounds of `GetLightdInfo`,
  `GetLatestBlock`, and two tree-state reads at a 100-block cadence boundary
  and the preceding height. For each `--transparent-address` it also calls
  `GetTaddressTxids` over the range, then calls `GetAddressUtxosStream` once
  for all addresses. Each call gets one attempt.

Point-read report format 2 labels the requests `GetTreeState(on cadence)` and
`GetTreeState(off cadence)` and records `on_cadence_height` and
`off_cadence_height`. A dense store can serve both heights directly.
Format 1 called these requests `stored checkpoint` and `replay` based on
height alone. To measure how Zinder served them, use the `source` label on
`zinder_query_tree_state_reads_total`.

The `point-reads` settings below, and the setup they run, apply only to that
workload. `--workload` values that download blocks refuse them. Setup calls
have the same 30-second deadlines as the rounds' calls, and every setup
stream also ends at 120 seconds in total, so a server that stops answering,
or keeps sending messages without end, fails or finishes setup instead of
stalling it. Mempool discovery stops reading as soon as it has `<count>`
transaction IDs.

Settings that cycle through a list, such as distinct frontiers and transaction
IDs, share one position in each list across all wallets. A wallet takes the
next entry when it sends the read, so the run reads every entry once before it
reads any entry twice, even when `--wallet-start-interval-millis` lets one
wallet finish its rounds before the next starts. With 16 wallets, 2 rounds,
and 32 distinct frontiers, each frontier is read exactly once.

The lightwalletd protocol lets a client name pools in `poolTypes` only after
it checks that the server filters by pool, which lightwallet-protocol
`v0.4.0` added together with `GetLightdInfo.lightwalletProtocolVersion`. The
Ironwood pool exists from `v0.5.0`. Setup reads that version, and the report
shows it in `server.lightwallet_protocol_version`. A version must be
`vMAJOR.MINOR.PATCH` with an optional semantic-version pre-release or build
suffix, and a pre-release counts as below its release. Setup treats any
other value, such as `v0.4.0garbage`, as no version. Against a server that
advertises a version below `v0.4.0`, or none, `--distinct-frontiers` scans
with an empty `poolTypes`, which such a server reads as the shielded pools,
and `--discover-mined-txids` and `--discover-mempool-txids` are refused,
because the server may leave out transactions that touch only transparent
addresses. Name the transactions with `--mined-txid` and `--pending-txid`
instead. Against a server below `v0.5.0`, setup never names Ironwood in
`poolTypes` or `GetSubtreeRoots`.

- `--distinct-frontiers <count>`: before the run, scans the range from the
  tip down and keeps up to `<count>` heights whose tree states have different
  Sapling, Orchard, and, from `v0.5.0`, Ironwood frontiers. The candidates are the blocks
  with shielded outputs and the range's first height, whose frontier holds
  the outputs below the range. Each round then reads one of them
  as `GetTreeState(distinct frontier)`. The stored and replay heights repeat
  every round, so a server that caches recent tree states can answer them
  from the cache. Set `<count>` above the size of that cache to keep the
  requests spread over frontiers it does not hold.
- `--mined-txid <txid>`, `--pending-txid <txid>`: transaction IDs that each
  round reads with `GetTransaction`. Give each as the 64 hex characters a
  block explorer or `zcash-cli` shows, without a `0x` prefix; the load client
  reverses it into the byte order the protocol sends. A read is reported
  under the list it came from and where the answer placed the transaction:
  `GetTransaction(mined)` and `GetTransaction(pending)` when the answer
  matches, or, for example, `GetTransaction(pending, answered mined)` for a
  pending transaction mined before the read, and `answered fork` for one the
  server places on a fork. A failed read keeps its list's name.
- `--discover-mined-txids <count>`, `--discover-mempool-txids <count>`: before
  the run, find up to `<count>` mined transaction IDs from the range, newest
  first, or pending ones from `GetMempoolTx`, and add them to the lists above.
  Discovery selects every pool the server's version names, so it also finds
  transactions that touch only transparent addresses. Each list then keeps
  only the first occurrence of a transaction ID, so a transaction both named
  and discovered takes one place in the cycle, and the report's
  `parameters.mined_txids` and `parameters.pending_txids` count distinct
  transactions. An empty mempool leaves the pending list empty.
- `--missing-tx-checks <count>`: each round also asks `GetTransaction` for
  `<count>` transaction IDs that no chain contains, reported as
  `GetTransaction(missing)`. A `not_found` status is the expected answer, and
  its latency is in that method's `expected_miss_seconds`.
- `--include-taddress-transactions`, `--include-taddress-balance`,
  `--include-address-utxos-unary`: each round also reads
  `GetTaddressTransactions` per `--transparent-address`, and
  `GetTaddressBalance` and the unary `GetAddressUtxos` across all of them.
  Each needs at least one `--transparent-address`.

For example, this run reads through a Caddy proxy with a private CA, cycling
across 32 distinct frontiers with a mix of mined, pending, and missing
transactions:

```bash
zinder-bench lightwalletd-wallet-load \
  --endpoint https://127.0.0.1:443 \
  --tls-ca-cert ./caddy-root.crt \
  --tls-domain-name wallet.example \
  --workload point-reads \
  --wallets 16 \
  --rounds 50 \
  --from-height 3424811 \
  --distinct-frontiers 32 \
  --discover-mined-txids 8 \
  --discover-mempool-txids 4 \
  --missing-tx-checks 2 \
  --verify-subtree-roots \
  --report ./reports/point-reads-16.json
```

`--verify-subtree-roots` applies to every workload. Before the run it
observes what `GetSubtreeRoots` returns, and reports what it saw without
judging it. It reads the chain tip, then asks `GetBlock` for the block at the
tip's height, whose `chainMetadata` gives each pool's commitment tree size. A
subtree root covers 2^16 note commitments, so a tree of `size` commitments
has `size >> 16` completed subtrees. It then asks `GetSubtreeRoots` for every
Sapling, Orchard, and, from protocol `v0.5.0`, Ironwood root with
`max_entries` of 0, and asks for one more root at the index after the last
one returned, starting at index 0 when none was. Finally it reads the chain
tip again. When the tip changed, it repeats the probe, up to 3 times, because
a new block can complete a subtree during the probe, and it reports the last
attempt. A failed `GetBlock`, such as `NOT_FOUND` after a rollback, is
recorded rather than ending the run, and the tip read after it decides
whether the probe repeats. The result is `subtree_roots_verification`. Setup
requests are not counted in `rpcs`.

Read the observations together:

- A root at the index after the returned ones shows that the later request
  delivered a root beyond the first response. It indicates that the first
  response stopped early only when `tip_before` equals `tip_after`, so the
  tip did not move between the two requests. When the tip moved, a new
  subtree can have completed in between.
- Fewer roots than `completed_subtrees_at_tip` with no root after them can
  mean the server's subtree index lags its tip, or that it truncated the
  response and also drops the follow-up root.
- A comparison with `completed_subtrees_at_tip` holds only when
  `chain_metadata_block_is_tip` is true and `tip_before` equals `tip_after`.
  Otherwise the sizes and the roots may describe different blocks, and
  `completed_subtrees_at_tip` is `null` unless the block is the tip.
- A size of zero cannot be told apart from a size the server left unset,
  because the protocol encodes both the same way.
- Repeated roots inflate `returned_with_zero_max`; `distinct_roots` and
  `repeated_root_indexes` show them.

Zinder streams every remaining root for `max_entries` of 0, as the
[lightwalletd compatibility reference](../../docs/reference/lightwalletd-compatibility.md#getsubtreeroots)
describes. Against Zinder, a root at the index after the returned ones with
an unchanged tip is therefore worth reporting, and a root that appears only
when the tip moved is not evidence of early termination.

Each `GetSubtreeRoots` request has a total deadline of 120 seconds, so a
stream that keeps delivering roots cannot hold setup open. A request that
reaches it reports the deadline as its error, with the roots delivered before
it in `roots_before_error`.

The report, `lightwalletd-wallet-load`, records the endpoint's `GetLightdInfo`
identity, the workload's constants, and, over all wallets:

- `rpcs`: for each method, requests by client-observed status, latency of
  successful calls, latency of expected misses in `expected_miss_seconds`,
  each measured to the moment the call's outcome is known,
  and for `GetBlockRange` the time to first block. A
  `connect` entry counts every connection the run opened: one for the
  probe that reads the endpoint's identity and tip, and the rest by wallets.
  A status is a gRPC
  status name, `client_timeout`, `client_cancelled` for a call the wallet
  dropped, `connect_failed`, `out_of_sequence`, or `incomplete_stream`.
  `status_examples` keeps the first message received with each failed
  status. For streams, `first_message_seconds` and `last_message_seconds`
  are the time from opening the stream to its first and last message, the
  latter over successful calls. `messages` and `bytes` include a block a
  stream sent out of order, which the wallet then rejects. `incomplete_streams`
  counts streams that failed after delivering a message, that ended short or
  out of order, or that the wallet dropped after a message.
  `cancelled_after_messages` counts that last kind, which keeps the
  `client_cancelled` status.
- `time_to_first_block_seconds` and `wallet_sync_seconds` per wallet,
  `blocks_received`, `bytes_received`, `blocks_per_second`, and
  `bytes_per_second` over the run, `retries`, and the status that ended each
  failed wallet, or `client_task_failed` for a wallet whose task panicked.
  `blocks_received` and `bytes_received` equal the `GetBlockRange` entry's
  `messages` and `bytes`, so they count blocks received again on retry and
  blocks a wallet rejected as out of order or past its range, which the
  wallet does not process. Bytes are the protobuf-encoded size of the
  received blocks, not bytes on the wire. `total_rpc_bytes` is the same measure over
  every response the wallets received, without the probe's `GetLightdInfo`
  and `GetLatestBlock`, which `rpcs` still counts. `incomplete_streams` is the
  sum of the per-method counts.
- `subtree_roots_verification`: present with `--verify-subtree-roots`. It
  holds observations only. For `sapling`, `orchard`, and `ironwood`:
  - `returned_with_zero_max`: the roots the `max_entries` of 0 request
    delivered, repeats included.
  - `distinct_roots`: the different 32-byte `rootHash` values among them.
    `SubtreeRoot` carries no index, so a root's position in the response is
    the subtree index it stands for.
  - `malformed_root_hashes`: the roots whose `rootHash` is not 32 bytes. They
    count in `returned_with_zero_max` but are not kept.
  - `repeated_root_indexes`: the zero-based positions in the response whose
    root repeated one at a lower position. They equal subtree indexes only
    when the response starts at index 0 and skips nothing.
  - `root_at_returned_index`: whether a request for one root at index
    `returned_with_zero_max` delivered one, or `null` when the first request
    failed and the follow-up was not sent.
  - `completed_subtrees_at_tip`: `size >> 16` from the `chainMetadata` of
    `chain_metadata_block`, or `null` unless that block is the tip
    (`chain_metadata_block_is_tip`) or when it carried no `chainMetadata`.
  - `zero_max_error` and `follow_up_error`: present when that request failed,
    with the status in `error` and the roots delivered before it in
    `roots_before_error`. A request retains at most 1,048,576 distinct roots
    plus repeat positions, 16 times the protocol's 65,536 subtrees per pool.
    Past that it stops reading and reports a `resource_limit` error with the
    partial tally.

  `ironwood` is absent against a server below protocol `v0.5.0`. `attempts`
  is how many times the probe ran. `tip_before` and `tip_after` are the
  `GetLatestBlock` height and `hash_hex` read around the last attempt.
  `chain_metadata_block` is the height and `hash_hex` of the block `GetBlock`
  returned for `tip_before`'s height, absent when `GetBlock` failed, and
  `chain_metadata_error` then holds its status.
  `chain_metadata_block_is_tip` is true only when that block has
  `tip_before`'s height and the same 32-byte hash; it is false when a hash is
  missing or malformed. A complete `hash_hex` uses RPC byte order. Shorter
  hashes preserve the bytes as sent. A hash longer than 32 bytes fails the
  probe that returned it. An oversized `GetBlock` hash appears in
  `chain_metadata_error` and leaves the tree sizes unknown.
- `wallet_request_identity_sha256`: identifies how range, tree-state, and
  transaction requests were distributed among the simulated wallets. Each
  wallet contributes its own digest of full requests, counting retries and
  duplicates. Sorting the wallet digests allows wallets to run in any order
  while preserving the work each wallet performed. Compare this field, RPC
  statuses, and delivered bytes across runs.
- `tip_before` and `tip_after`: the height and RPC-display hash observed at
  the boundaries of the wallet workload. The closing probe is outside the
  measured interval. If it fails, `tip_after.error` records the failure and
  the report retains the wallet results. A frozen-snapshot comparison
  requires both observations to match the expected height and hash.
- `tree_state_heights`: how many requested heights sit on Zinder's 100-block
  checkpoint stride, and each other height's offset from it. Below the
  first-sync tip, that offset is the number of blocks Zinder replays. Above
  it, and on a store whose checkpoint pass has run, every settled block has a
  stored checkpoint and nothing is replayed, so the offsets are an upper
  bound. `distinct_heights` and `distinct_frontiers` count the different
  heights requested and the different frontiers the answers carried, so a
  run whose requests all hit one frontier is visible. With
  `--server-metrics-url` set to a Zinder server's `/metrics` endpoint,
  `server_observed` adds what that server counted during the run: tree
  states it read from a stored checkpoint or rebuilt, the blocks it replayed,
  the time requests waited for a rebuild or block-range slot, the tree
  states, block ranges, and requests it refused, stale-tip refusals, and the
  time block-range chunks spent in each stage. The stored and rebuilt tree
  state reads are also split by `height_band`, the first height of the
  250,000-block band that holds the requested height, in
  `tree_state_stored_reads_by_band` and `tree_state_replayed_reads_by_band`.
  `tree_state_reconstructions` counts replayed reads on servers that report
  reads by source. Older servers record the replayed-blocks histogram only
  for rebuilds, so the report uses its observation count on those servers.
  `receive_evictions` counts requests that lost their receive slot to a
  client holding fewer, and `receive_pools` describes each receive pool of
  the server's public gRPC listener: the requests it admitted, refused, and
  evicted, the slots it held on average when it admitted a request,
  `peak_slots_held_at_most`, the smallest histogram bucket that holds every
  admission, the mean time a request held its slot, and `mean_slots_held`,
  the mean number of slots held at once. The run reads the server's
  `zinder_grpc_request_receive_slots_held` gauge when it starts, when it ends,
  and every second between, leaving out reads that fail, and averages the
  reads over time, so `mean_slots_held` counts slots held across either end of
  the run and never exceeds the pool's size. The read after the run counts as
  taken when the wallets finished, however long its scrape takes, and reads
  that complete after that moment are left out, so the mean covers the run
  alone. Behind a proxy,
  `proxy_client_address_fallbacks` counts, by reason, the requests that
  counted against the proxy's own connection because they lacked one valid
  client address header.
  A metric the server does not export reads as zero. If the read after the
  run fails, or a counter series went down or disappeared because the server
  restarted during the run, the report keeps its client-side results, leaves
  `server_observed` out, and gives the reason in `server_metrics_error`. A
  scrape reads at most 64 MiB.

The simulated wallets do not scan, so each asks for its next blocks as soon as
the last ones arrive, or, for Slipstream, as soon as its chunk queue has room:
the load is the most an engine of that shape sends, not an average. They also
omit what a sync does besides blocks and tree states: `GetSubtreeRoots`,
mempool streams, and tip polls, and, outside `point-reads`, transparent
address queries.
Vizor sizes batches from compiled Mainnet tree sizes; the simulated wallet
uses the density it has received, so its first batch is 10 blocks. When no
block has arrived by the end of Slipstream's density wait, the simulated
wallet asks for another 64-block chunk, where Slipstream asks for the full
10,000 blocks.
Each connection gives up when the TCP connection and any TLS handshake have
not finished within 10 seconds, and sends no HTTP/2 keep-alive pings. Go lightwalletd's grpc-go server closes a connection whose client pings
more often than every five minutes, so pinging would end streams a real
wallet keeps.

### Verify PepperSync restore requests locally

Run the real PepperSync engine against a synthetic loopback gRPC fixture to
reproduce a fresh, unfunded restore on a fixed tip. Zingo Mobile 3.0.0-324
pins Zingolib 8.0.0 at `81d799775f11e882896e8710e8af12d6d4d29a41`. Build
the CLI from that revision in direct test mode, which lets it use the
loopback indexer.
[Zingo Mobile binding pin](https://github.com/zingolabs/zingo-mobile/blob/5fd3662f39dc20f953e4f93c2be738a4c03d3690/binding-layer.json)

From the Zinder repository, with Rust 1.99.1 and `protoc` installed:

```bash
zingolib_source="$PWD/.tmp/zingolib-81d7997"
mkdir -p "$zingolib_source"
curl -fsSL https://codeload.github.com/zingolabs/zingolib/tar.gz/81d799775f11e882896e8710e8af12d6d4d29a41 \
  | tar -xz --strip-components=1 -C "$zingolib_source"
cargo +1.99.1 build --manifest-path "$zingolib_source/Cargo.toml" --locked \
  -p zingo-cli --no-default-features --features nakednet-test-mode
ZINDER_TEST_PEPPER_SYNC_BIN="$zingolib_source/target/debug/zingo-cli" \
  cargo nextest run -p zinder-bench --profile=ci --run-ignored=all \
  -E 'test(pepper_sync_fresh_restore_requests)'
```

The test restores a disposable wallet over blocks 3,424,811 through
3,424,874. It checks that one connection carries the mempool subscription and
the sync requests, including the predecessor, tip, and birthday tree states.
The fixture has empty transparent history and one Sapling subtree completed
before the birthday. The client makes 21 `GetTaddressTxids` requests from
1,000 blocks below the birthday through the tip. It requests each shielded
pool's roots from index zero, then confirms the end of the Sapling roots
with an empty reply from index one. It requests a 10-block range, resumes
from the fifth block after the fixture interrupts the first stream with
`DEADLINE_EXCEEDED`, and requests the remaining 54 blocks. Every range
selects Sapling, Orchard, and Ironwood. The CLI reports all 64 blocks
scanned.

The fixture exercises the pinned client's direct gRPC path through sync
completion over a span with no new note commitments. On a populated chain,
subtree boundaries and discovered transactions change which ranges
PepperSync prioritizes. Wallets with history can trigger additional lookups.
[PepperSync sync loop](https://github.com/zingolabs/zingolib/blob/81d799775f11e882896e8710e8af12d6d4d29a41/pepper-sync/src/sync.rs)

### Verify raw block-range frames

`raw-range-probe` requests `GetBlockRange` from a running lightwalletd-compatible
server and checks the bytes it puts on the wire. Each response frame is hashed
as the raw protobuf payload, without decoding it. The digest is the SHA-256 of
every frame in stream order, each frame written as its payload length as a
big-endian `u32` followed by the payload. A server that reorders, drops, adds, or
changes a block fails the request even when a wallet would still parse it.

Every request is compared with an oracle of frame count, payload bytes, and
digest. The oracle comes from a file, or from a different reference server read
before the run. The probe rejects two URLs resolving to a common host and port. For listeners behind proxies, supply a known independent source or a trusted oracle file. A `START:END`
range with `START` above `END` streams descending.

```bash
# Record the oracle from a reference server, then probe the server under test.
zinder-bench raw-range-probe \
  --endpoint http://127.0.0.1:9067 \
  --oracle-endpoint http://127.0.0.1:9068 \
  --range 4598001:4600000 --both-directions \
  --write-oracle .tmp/oracle.json \
  --clients 4 --repetitions 32 --warmup-repetitions 1 \
  --server-pid "$READER_PID" --server-executable "$READER_BINARY" \
  --report .tmp/raw-range-probe.json

# Later runs reuse the recorded oracle.
zinder-bench raw-range-probe --endpoint http://127.0.0.1:9067 --oracle .tmp/oracle.json
```

An oracle file is `{"schema": "zinder-bench/raw-range-oracle/v1", "jobs": [...]}`,
and each job is `{"start", "end", "blocks", "bytes", "framed_sha256"}` with a
lowercase hex digest. Any tool can write one.

Each client opens its own HTTP/2 connection. After the connections open, the
warmup passes run untimed but verified, then every client's passes run together
as the timed interval. The probe exits non-zero when any request differs from
the oracle, after writing the report, so a campaign can require
`/verified` to be `true`.

The report (`schema` `zinder-bench/raw-range-probe/v1`) records:

- `verified`, `problems`, and `oracle` with its `source` and `sha256`.
- `chain`: the endpoint's `GetLightdInfo` identity and the `GetLatestBlock` tip
  before and after the timed interval, with hashes in display (RPC byte) order
  and `tip_unchanged`. Both reads sit outside the timed interval.
- `totals`: verified and failed request counts, blocks, bytes, `wall_ns`,
  `blocks_per_second`, and `mib_per_second`.
- `latency_ms`, overall and per range in `by_job`: nearest-rank minimum, `p50`,
  `p95`, and maximum for the first frame, completion, and largest frame gap.
- `client.cpu_ms`, and with `--server-pid` a `server` object: CPU over the timed
  interval and per MiB, and resident memory with the process's lifetime
  high-water mark before and after. The server is sampled read-only from
  `/proc`, in whole clock ticks, and its high-water mark is never reset.
- `counters`: with `--server-metrics-url`, `--server-freshness-counter`, and
  `--server-counter` or `--rocksdb-multiget-counters`, each series' increase
  over the timed interval in `increases`, summed over every label the selector
  leaves out. A selector is `[ALIAS=]NAME[{label=value,...}]`. Each selector
  needs a unique report key (its alias, or the whole selector if omitted).
  `--rocksdb-multiget-counters` reports the canonical secondary's
  `multiget_get`, `multiget_keys_read`, and `multiget_bytes_read` tickers. A
  server may publish these tickers on a timer, so each read waits, outside the
  timed interval, for the witness series to advance and for every counter to
  appear in that same scrape (up to `--server-counter-wait-secs`). When it does
  not, or a counter went backwards, `available` is `false` with a `reason` and
  no numbers, never zeros. An example witness for a full wallet serving pair is
  `zinder_wallet_serving_pair_publisher_refresh_total{status=ok}`.
- `warmup` and `observations`: every request as measured, including its digest.

Each request in `warmup` and `observations` reports `max_frame_gap_ns`,
the longest interval between two consecutive frames at the probe.
`max_frame_gap_after` is the **one-based ordinal of the frame ending that
interval**, in delivery order for either direction. A gap ending at frame 237
spans frames 236 and 237. The earliest maximum wins a tie. Both fields are
zero when fewer than two frames arrive or all observed offsets are equal.
Read these alongside `first_frame_ns` and `completion_ns`. The gap excludes the
wait for the first frame and the time from the last frame to stream completion.
Client scheduling and frame processing contribute to the observed interval.

To check the gap against the full sequence, run one request per job with
an oracle for the range and direction you want to inspect. Use a new report path.

```bash
zinder-bench raw-range-probe --endpoint "$ENDPOINT" --oracle "$ORACLE" \
  --record-frame-arrivals --report "$REPORT"
jq '.observations[] | {start, end, outcome, max_frame_gap_ns, max_frame_gap_after}' "$REPORT"
```

With `--record-frame-arrivals`, each request also records `frame_arrivals_ns`
as offsets from request start, including frames received before a stream error.
Subtract adjacent integer nanosecond offsets to reproduce the gap. To display
milliseconds, divide the integer gap by 1,000,000 and round once to the nearest
0.0001 ms. The default records the maximum with constant space per request.
The flag retains one offset per received frame, including warmups. Use matching
flag settings when comparing readers.

For a paired policy, use `/latency_ms/max_frame_gap/p95` in milliseconds with
`lower_is_better`. Each verified timed request with at least two frames
contributes one maximum, including a genuine zero when the offsets coincide.
Warmups, failed requests, and requests with fewer than two frames contribute
no samples. The summary uses nearest rank and is absent without eligible
requests. For the first oracle job, the pointer is
`/by_job/0/max_frame_gap_ms/p95`. Add `/config/record_frame_arrivals` to the
campaign case's `match` list when comparing reports that expose this flag.

## Report fields

- `contract_identity`: exact benchmark report contract identity. The current identity is
  `benchmark-report`; missing or earlier identities are rejected.
- `report_format_version`: machine-readable report contract version.
  `canonical-replay-storage` reports use version 4,
  `rocksdb-canonical-fixture-replay` reports version 5, and
  `rocksdb-storage-lifecycle` reports version 6.
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
  `rocksdb` or `postgres-diagnostic` topology represented by that
  arm. This is candidate identity, not topology certification.
- `fixture.contract_identity`, `fixture.fixture_format_version`,
  `fixture.canonical_artifact_schema_version`,
  `fixture.canonical_block_facts_digest_evidence`, `fixture.tip_hash_hex`, and
  `fixture.digest_sha256`: fixture provenance required to compare candidates
  against identical source bytes and digest contracts. Each driver verifies
  every segment SHA-256 before writing its rows. Fixture format 1 requires the
  exact identity `canonical-fixture`; earlier numeric versions are refused.
- `fixture.workload_density`: the immutable workload totals and per-block
  maxima copied from the captured fixture manifest.
- Canonical-fixture `total_seconds` and `blocks_per_second`: the complete
  measured lifecycle and its end-to-end loaded-block rate.
- Canonical-fixture `resource_limits`: the CPU and memory envelope, the exact
  source-planner, admission, preparation, deterministic delay, and
  `tree_state_checkpoint_coverage` (`every-block` or `cadence-and-tip`)
  settings, and the canonical `RocksDB` budget used by the run.
- Storage-lifecycle `resource_limits.tree_state_checkpoint_coverage`,
  `resource_limits.canonical_publication_mode`, and
  `canonical_storage_ready.tree_state_checkpoint_count` (versions 5 and 6): the actual
  construction checkpoint coverage (`every-block` or `cadence-and-tip`), the
  canonical publication mode (`trusted-fresh` or `cold-certified`), and the
  persisted checkpoint row count (including the predecessor checkpoint).
- Canonical-fixture `source_fetch_attribution` reports completed segment
  requests, request and payload rates over replay wall time, cumulative
  request-task seconds, density adjustments, and retained and discarded
  prefetch work. The planner applies size changes to future requests and keeps
  disjoint prefetched ranges. Both restart counts are zero. Discarded bytes
  count only completed responses. The object is `null` when no metrics
  recorder covered completed segment requests.
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

## Active-writer publication benchmark (`active-writer-publication`)

`zinder-bench active-writer-publication` drives four matched writer/publication
arms against an identical synthetic Regtest chain crossing at least one
4,096-block shielded catalog segment boundary while concurrent lightwalletd
compatibility readers measure live serving visibility and throughput:

```bash
zinder-bench active-writer-publication \
  --base-blocks 4092 \
  --live-blocks 16 \
  --reorg-window-blocks 4 \
  --segment-blocks 4096 \
  --report ./reports/active-writer-publication.json
```

The four matched modes are `quiescent_control`, `ingest_active`,
`ingest_and_projector`, and `ingest_projector_and_publisher`. Alongside the
unpublished `quiescent_control` baseline (used for `ingest_active` and
`ingest_and_projector`), the report records `published_quiescent_control` with a
sealed signed catalog at the settled boundary so `ingest_projector_and_publisher`
is compared against a matched catalog-backed baseline (`baseline_label` and
`baseline_catalog_published` on each `ModeDifferential`). The JSON report
records separate ingest prepare and commit latencies, commit-to-served-tip
publication delay (`p50`, `p95`, `max`), range `first_frame` and `completion`
latencies for settled catalog, cold-refill/warm live tail (verified with direct
`EncodedTail` cache fill/hit/eviction and peak-byte telemetry, plus cross-epoch
pair exclusion), and fixed-span slow boundary-crossing streams, mempool churn
(`50` and `500` tx batches), writer WAL fsyncs (`wal.synced`), `NodeSource` RPC
counts, per-role RocksDB flush/compaction and I/O modes (`direct` vs
`buffered`), matched stage differentials, and final-tip byte-level reply
parity.
