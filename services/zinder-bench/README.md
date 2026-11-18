# zinder-bench

Fixed-range capture and storage benchmark harness for measuring Zinder against
identical source bytes. It replays captured fixtures into a fresh production
canonical `RocksDB` store and drives two backend-neutral canonical-replay round
trips: one sorted external SST ingestion for `RocksDB` and binary `COPY` with
deferred index construction for PostgreSQL. It also replays captured tip blocks
through the production canonical follower and wallet transition to measure
per-block cost, and replays a deep reorg at the tip to measure one wallet
transition against its logical-byte ceiling.

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
it. Construction checkpoints every 100 blocks, and a wallet that scans in
100-block rounds anchors one block below each boundary, at `k * 100 - 1`, so
every such request replays the full 99-block stride. Measure that case on a
READY canonical store, such as one built with
`rocksdb-canonical-fixture-replay`:

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
