# Testing Runbook

Operational procedures for validating Zinder against its workspace, real Zebra
nodes, and external wallet applications. This document owns the test tiers,
runner profiles, live-node gates, and consumer-facing certification evidence.

## Test tier matrix

| Tier | Location | Profile | Trigger | Catches |
| ---- | -------- | ------- | ------- | ------- |
| T0 unit | `#[cfg(test)] mod tests in src/` | `default-filter` of `default`/`ci` | Every commit | Logic regressions in the unit under test |
| T1 integration | `tests/integration/` | `default-filter` of `default`/`ci` | Every commit | Cross-module wiring, gRPC adapter shape, store/proto round-trips |
| T1 PostgreSQL integration | `services/zinder-bench/tests/integration/postgres_canonical_replay.rs` | `ci-postgres` | Every pull request with disposable PostgreSQL | SCRAM connection, binary COPY, transaction, reconnect, and persisted read-back through the diagnostic driver |
| T2 perf | `tests/perf/` | `ci-perf` | Every commit | Latency budget regressions per the published budgets |
| Consumer parity | `crates/zinder-client/tests/parity/` | `ci-parity` | Consumer contract changes / release certification | Consumer-shaped request and error-shape regressions against production serving compositions |
| T3 live | `tests/live/` | `ci-live` | Manual / scheduled CI | Real upstream-node behavior (Zebra JSON-RPC, indexer gRPC) |
| External | n/a | n/a | Manual | Exploratory wallet runs (Zodl/Android SDK, public lightwalletd clients) |

`default-filter = "not test(/^live::/) and not test(/^perf::/) and not test(/^parity::/)"`
is the structural boundary. Every live test additionally carries
`#[ignore = LIVE_TEST_IGNORE_REASON]` and a runtime
`zinder_testkit::live::require_live()` check, so a stray `cargo test` cannot
talk to a node by accident.

`ci-parity` is fixture-backed and does not require `ZINDER_TEST_LIVE`. Treat it
as a consumer-contract gate for request and error shapes, not as a
replacement for live SDK, Zallet, or network validation.

The PostgreSQL integration test remains `#[ignore]` so the DB-free default gate
cannot connect to an accidental local service. Pull-request CI supplies a fresh
SCRAM-configured PostgreSQL service and runs `ci-postgres` with
`--run-ignored=all`; developers use the same profile with an explicit
`ZINDER_TEST_POSTGRES_DATABASE_URL`.

## Lightwalletd certification

The compatibility adapter is certified in layers: vendored-protocol coverage,
live parity against the pinned reference, an independent-client flow, then the
actual public deployment. The required boundaries and reference pins live in
[Lightwalletd compatibility](../reference/lightwalletd-compatibility.md).

Keep the evidence simple: retain the exact commands, image digests, client
version, network, wallet-serving coverage bounds, and command output with the release.
Do not generate a repository-specific report or manifest merely to restate
those results.

## Default validation gate (T0 + T1 + T2 + format/lint/docs)

Run before considering any change complete. This is the canonical gate; CI
mirrors it.

```bash
cargo fmt --all --check
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --profile=ci
cargo nextest run --profile=ci-perf
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps
cargo deny check
cargo machete
scripts/test-container-resource-evidence.sh
scripts/test-storage-benchmark-campaign-validator.sh
scripts/test-observability-smoke-safety.sh
scripts/runbook-lint.sh docs/runbooks/testing.md
git diff --check
```

Expected outcome: every command exits zero. The full suite runs in ~5 minutes
on a developer laptop. If `cargo machete` reports unused dependencies on a
crate you did not touch, treat it as a pre-existing finding, not a blocker.

The 2 storage-benchmark shell tests are hermetic contract tests. One exercises
resource observation, failure-status preservation, and exclusive artifact
publication against a fake cgroup; the other constructs a complete synthetic
campaign and verifies report, resource, alignment, and aggregation rejection
paths. Neither requires Docker or a live PostgreSQL server.

After changing captured fixture subtree roots, canonical subtree-root loading,
construction-manifest evidence, or fixture format admission, run the focused
complete-prefix proof:

```bash
cargo nextest run -p zinder-store --profile=ci \
  -E 'test(complete_prefix_loader_rejects_invalid_input_atomically_and_accepts_retry)'
cargo nextest run -p zinder-store --profile=ci \
  -E 'test(cold_certification_rejects_tampered_complete_prefix_rows)'
cargo nextest run -p zinder-bench --profile=ci \
  -E 'test(canonical_fixture_replay_cli_emits_self_validating_zero_prohibited_reads)'
cargo nextest run -p zinder-bench --profile=ci \
  -E 'test(canonical_fixture_replay_preserves_historical_subtree_prefix_after_cold_reopen_and_secondary)'
scripts/test-storage-benchmark-campaign-validator.sh
```

The first command proves that the public complete-prefix loader atomically
rejects missing, extra, misordered, and unauthenticated completion records and
then accepts a valid retry. The second proves cold certification rejects
post-load subtree-row tampering. The third executes the benchmark CLI in a
separate process and proves its self-validation observes both prohibited-read
series at zero. The fourth replays an index-zero historical prefix through
publication, independent cold reopen, and a fresh RocksDB secondary. The script
proves the hermetic campaign validator accepts fixture format v3 and refuses
obsolete fixture format v2.

`scripts/runbook-lint.sh` parses every fenced `bash` block in this runbook
through `bash -n` (syntax-only mode) so a typo or unclosed quote in an
operator recipe fails CI immediately rather than ambushing an on-call
engineer. It does not execute the blocks; running them still requires the
documented prerequisites. See [Runbook self-test](#runbook-self-test)
below for the full contract.

## Public SDK package gate

Run after changing `zinder-core`, `zinder-proto`, `zinder-client`, their
manifests, generated protocol artifacts, or release packaging:

```bash
scripts/verify-sdk-packages.sh
```

The gate permits exactly those three crates for crates.io, verifies their
lockstep dependency requirements and workspace MSRV, packages them in
dependency order, checks default and feature-extreme builds, and builds their
documentation with `PROTOC` unavailable. It then extracts the `.crate`
archives and compiles a standalone consumer against the extracted packages
using the workspace toolchain. This catches workspace path leakage, missing
package files, accidental test inclusion, generated-code dependencies, and
public protobuf vocabulary that a normal workspace build can hide.

## PostgreSQL driver integration gate

Run after changing the canonical-replay PostgreSQL path, its driver dependencies, or
the benchmark database configuration. The URL must identify a fresh disposable
database because the test deliberately leaves its completed schema in place and
then proves reuse is rejected.

```bash
ZINDER_TEST_POSTGRES_DATABASE_URL='postgresql://zinder_bench:zinder_bench_local_only@127.0.0.1:55432/zinder_bench' \
  cargo nextest run -p zinder-bench --profile=ci-postgres --run-ignored=all
```

The test generates its own one-block regtest fixture. A passing run proves a
real TCP password-authenticated connection, binary COPY, commit, deferred index
creation, complete read-back, completion publication, reconnect, WAL/storage
measurement, and safe existing-schema rejection. Start and clean the disposable
database with the commands in [Storage benchmark environment](../../deploy/storage-benchmark.md#run-the-postgresql-driver-integration-gate).

## Consumer parity gate (consumer-shaped fixtures)

Run before tagging a release, and after any change that touches a consumer-facing
wire contract or compatibility adapter:

```bash
cargo nextest run --profile=ci-parity
```

Expected outcome: every `parity::` module exits zero and the report is archived
with the release evidence. This profile is organized by consumer:

- `parity/lightwalletd_adapter_shapes.rs` covers isolated lightwalletd adapter
  shapes. Current ZODL proof comes from the bound ten-RPC contract and the real
  Android application certification run.
- `parity/zallet.rs` covers a Zallet-shaped retained chain-view contract,
  including the cloneable, `'static` owned snapshot surface. It is not a claim
  that a downstream Zallet integration has been certified.
- `parity/lightwalletd_operators.rs` covers public lightwalletd operator
  expectations.
- `parity/explorers.rs` covers explorer-facing transparent-address read shapes.

Do not use `ci-parity` as proof that a real consumer SDK or binary works. It
proves the fixture-backed request and error shapes that must stay stable before
the slower external runs start.

## T3: Live regtest sweep

The most valuable automated check that exercises a real Zebra node.

### Prerequisites

1. **z3 regtest sidecar** running. The default ZF z3 setup gives you:
   - `z3-regtest-zebra-1` (Zebra) listening JSON-RPC on host
     `127.0.0.1:29232` and indexer gRPC on `127.0.0.1:39155`.
   - Health endpoint at `127.0.0.1:28080`.
   - Mining target seed-derived address `tmDpFafuBHKGUYmuwLsrxWJrwcnSyzEEtYx`
     (configured via `ZEBRA_MINING__MINER_ADDRESS`).
2. **Container env contains** `ZEBRA_RPC__INDEXER_LISTEN_ADDR=0.0.0.0:8155`
   (mapped to host `39155`). Without it the indexer-gRPC tests skip.
3. **Auth method** matches what z3 publishes. The default sidecar uses basic
   auth `zebra:zebra` (`ZEBRA_RPC__ENABLE_COOKIE_AUTH=false`).

### Pre-flight checks

```bash
# Zebra JSON-RPC reachable
curl -s -u zebra:zebra -H "Content-Type: application/json" \
  -d '{"jsonrpc":"1.0","id":"1","method":"getblockchaininfo","params":[]}' \
  http://127.0.0.1:29232 | jq '.result | {chain, blocks, bestblockhash}'

# Indexer gRPC reachable (and feature compiled in)
grpcurl -plaintext 127.0.0.1:39155 list
# Expect: zebra.indexer.rpc.Indexer
```

If either probe fails, fix the sidecar before running tests. Don't use a
"--insecure" or "--ignore-failures" flag to paper over a missing service.

### Cleanup between runs

A persisted store from a previous run that bumped `STORE_SCHEMA_VERSION`
fails to reopen with `SchemaMismatch`. Clear the scratch directories before
reusing them across schema-changing commits:

```bash
rm -rf .tmp/regtest.zinder-store \
  .tmp/regtest.projector-canonical-secondary \
  .tmp/regtest.wallet-store \
  .tmp/regtest.compat-canonical-secondary \
  .tmp/regtest.compat-wallet-secondary
```

The `.tmp/` directory is `.gitignore`'d for exactly this purpose.

### Standalone native WalletQuery smoke

When a running `zinder-query` is available, set the test-only endpoint and run
the native T3 contract smoke. The endpoint is optional for the full live suite;
the test skips when it is absent. `require_live()` still supplies the network
identity and rejects mainnet, while `ZINDER_TEST_QUERY_GRPC_ADDR` names only the
already-running native gRPC surface under test.

```bash
ZINDER_TEST_LIVE=1 \
  ZINDER_NETWORK=zcash-regtest \
  ZINDER_NODE__JSON_RPC_ADDR=http://127.0.0.1:29232 \
  ZINDER_NODE__AUTH__METHOD=basic \
  ZINDER_NODE__AUTH__USERNAME=zebra \
  ZINDER_NODE__AUTH__PASSWORD=zebra \
  ZINDER_TEST_QUERY_GRPC_ADDR=http://127.0.0.1:29102 \
  cargo nextest run -p zinder-query --profile=ci-live --run-ignored=all \
    -E 'test(standalone_wallet_query_serves_native_contract)'
```

The smoke certifies reflection and service identity, the exact capability set
Zally requires, an epoch-pinned structured one-block compact range at the
settled tip, an epoch-pinned typed tree state at that height, and successful
chain-event stream admission.

### Run

```bash
ZINDER_TEST_LIVE=1 \
  ZINDER_NETWORK=zcash-regtest \
  ZINDER_NODE__JSON_RPC_ADDR=http://127.0.0.1:29232 \
  ZINDER_NODE__INDEXER_GRPC_ADDR=http://127.0.0.1:39155 \
  ZINDER_NODE__AUTH__METHOD=basic \
  ZINDER_NODE__AUTH__USERNAME=zebra \
  ZINDER_NODE__AUTH__PASSWORD=zebra \
  cargo nextest run --profile=ci-live --run-ignored=all
```

Expected outcome on a healthy regtest with the indexer gRPC port set: all
regtest-targeted tests pass in ~30 s. A full regtest `ci-live` invocation also
contains hosted-network checks that deliberately refuse to run on regtest:
the mainnet-only tests
(`fetch_chain_checkpoint_returns_advancing_tree_sizes_on_mainnet`,
`tip_id_advances_above_one_million`,
`bulk_catchup_last_1000_blocks_from_checkpoint`) and the testnet-or-mainnet tests
(`cli_constructs_complete_wallet_serving_history_from_config`,
`checkpoint_bounded_read_endpoint_latency_baseline`).

Without `ZINDER_NODE__INDEXER_GRPC_ADDR`, the two Zebra Indexer mempool-source
tests skip: `zebra_indexer_mempool_source_opens_stream_against_running_indexer`
and `streaming_source_recovers_after_zebra_indexer_restart`.

### Targeted reruns

```bash
# Single test
cargo nextest run --profile=ci-live -E 'test(read_endpoint_latency_baseline)' --run-ignored=all

# Whole suite of one file
cargo nextest run --profile=ci-live -E 'test(/^live::mempool_broadcast_cycle::/)' --run-ignored=all

# Re-run a specific failed test in isolation, with logs visible
cargo nextest run --profile=ci-live -E 'test(<failed_test_name>)' \
  --run-ignored=all --no-capture
```

Nextest does not have a built-in `--rerun-failures` flag; the `ci-live`
profile already retries each test up to twice on transient failure. For
deterministic reruns, copy the failed test name from the summary and pass it
through `-E 'test(<name>)'` as above.

The `node-mutating` test group in `.config/nextest.toml` serializes any test
that mutates regtest state (broadcast cycles, indexer-restart, deep-chain
bulk-catchup runs). Run them in isolation if you want to debug — parallel execution
will fight for the same node.

## T3: Live testnet (Zebra cookie auth)

Same surface as regtest but against real testnet. Useful for capability probes
and tip-fetch validation against a node with non-trivial chain weight.

```bash
cookie=$(docker exec <zebra_container> cat /var/run/auth/.cookie)
ZINDER_TEST_LIVE=1 \
  ZINDER_NETWORK=zcash-testnet \
  ZINDER_NODE__JSON_RPC_ADDR=http://127.0.0.1:18232 \
  ZINDER_NODE__INDEXER_GRPC_ADDR=http://127.0.0.1:18155 \
  ZINDER_NODE__AUTH__METHOD=basic \
  ZINDER_NODE__AUTH__USERNAME=${cookie%%:*} \
  ZINDER_NODE__AUTH__PASSWORD=${cookie#*:} \
  ZINDER_TEST_QUERY_GRPC_ADDR=http://127.0.0.1:19102 \
  cargo nextest run --profile=ci-live --run-ignored=all
```

`require_live()` accepts testnet by default; nothing further to opt in. Tests
that target only mainnet (see below) still skip. Tests that exercise
regtest-only RPCs (`generate`, `invalidateblock`, `reconsiderblock`) opt in
via `require_live_for(&[Network::ZcashRegtest])` and refuse to run here.
Omit `ZINDER_TEST_QUERY_GRPC_ADDR` only when no standalone native runtime is
available; that omission intentionally skips the native contract smoke.

The fixed-range canonical tracer runs on testnet and mainnet. It captures one
tip identity, retains the requested predecessor-to-tip range, and loads every
source-derived version-1 family retained by the Wallet workload through one
production source-to-SST pass. Construction fully decodes the persisted replay
sequence without filling the block cache. The tracer then reopens RocksDB
read-only, checks exact live-file entry counts, rejects tombstones and
overlapping or malformed SST key ranges, checksum-reads every SST boundary,
and cross-checks first, middle, and tip identities across the header, reverse
index, replay, compact-block, transaction-location, and raw transaction
families. It reports per-family prepared logical bytes and persisted SST
telemetry plus aggregate throughput. The store deliberately remains
`BUILDING`: this test does not claim canonical or wallet readiness before the
remaining source-observed families, publication records, and wallet projection
exist.
Run only this tracer with the same environment shown above:

```bash
cargo nextest run --profile=ci-live --run-ignored=all \
  -E 'test(canonical_blocks_load_requested_range_from_fixed_checkpoint)'
```

The tracer retains 1,000 blocks by default. Set
`ZINDER_TEST_CANONICAL_BLOCK_COUNT` to a positive integer for a larger
fixed-tip calibration range.

For the local Z3 testnet topology, the dedicated Docker setup builds the test
in release mode, joins the existing Zebra network, mounts its cookie volume
read-only, and creates an isolated project-scoped Zinder volume. It never
mounts Zebra's chain volume or the active Zinder data volume:

```bash
docker build -f deploy/Dockerfile \
  --target zinder-ingest-live-tests \
  -t zinder-ingest-live-tests:local .
docker compose -p zinder-canonical-live-test \
  -f deploy/docker-compose.canonical-live-test.yml \
  run --rm canonical-blocks
docker compose -p zinder-canonical-live-test \
  -f deploy/docker-compose.canonical-live-test.yml \
  down -v
```

### Complete RocksDB storage lifecycle

The canonical tracer above proves a bounded source-to-SST range but
deliberately leaves the store `BUILDING`. Use the dedicated
[RocksDB storage lifecycle harness](../../deploy/rocksdb-storage-lifecycle.md)
to measure a fresh complete-history canonical store through `READY`, then build
and cold-admit the wallet store at the same fixed Zebra tip. The
harness reports canonical and wallet times separately and captures exact peak
container memory plus sampled peak disk usage.

Run a small fixed-tip smoke before a current-tip measurement. Both runs reuse
Zebra's synchronized chain state but delete all project-scoped Zinder state:

```bash
docker build -f deploy/Dockerfile --target zinder-bench -t zinder-bench:local .

ZINDER_STORAGE_LIFECYCLE_TIP_HEIGHT=10000 \
ZINDER_STORAGE_LIFECYCLE_PROJECT_NAME=zinder-storage-lifecycle-smoke \
ZINDER_STORAGE_LIFECYCLE_EVIDENCE_PATH="$PWD/.tmp/rocksdb-storage-lifecycle-smoke" \
  scripts/run-rocksdb-storage-lifecycle.sh

scripts/run-rocksdb-storage-lifecycle.sh
```

This certifies only canonical and wallet storage readiness. Query serving,
continuous tip following, and executed reorg recovery remain separate gates.

### Canonical runtime tracer

Use the dedicated runtime topology after the storage lifecycle gate. It starts
the actual `zinder-ingest` binary, gives it one disposable Zinder volume, joins
the existing Zebra network, and mounts only Zebra's authentication cookie
read-only. It enforces the established 10-CPU, 10-GiB envelope. `/healthz` is
the container liveness check; the operator must poll `/readyz` separately and
record the authenticated canonical fence.

Choose a fixed source-authenticated predecessor below the current Zebra tip and
pin the locally reviewed image by immutable ID:

```bash
docker build -f deploy/Dockerfile \
  --target zinder-ingest \
  -t zinder-ingest:canonical-runtime-local .

image_id=$(docker image inspect \
  zinder-ingest:canonical-runtime-local \
  --format '{{.Id}}')

ZINDER_CANONICAL_RUNTIME_IMAGE="$image_id" \
ZINDER_CANONICAL_RUNTIME_CHECKPOINT_HEIGHT=<height> \
  docker compose \
    -f deploy/docker-compose.canonical-runtime-test.yml \
    up -d zinder-ingest
```

Acceptance requires a fresh `canonical.building` path to publish and cold-open
epoch 1 and event 1, then at least one natural Zebra advance to increment the
tip, epoch, event sequence, and digest together. Restart the service and require
the same fence to reopen before accepting further appends. A reversible source
outage may be tested by disconnecting only the ingest container from the source
network; readiness must become `node_unavailable`, the fence must remain
unchanged, and reconnecting must restore following. Never stop Zebra or mount,
delete, or modify its chain volume for this gate.

Record the image ID, checkpoint and fixed build fence, each appended fence,
readiness payloads, resource limit and observations, volume mounts, and these
metrics:

- `zinder_ingest_canonical_chain_epoch`;
- `zinder_ingest_canonical_chain_event_sequence`;
- `zinder_ingest_canonical_tip_height`;
- `zinder_ingest_canonical_lag_blocks`;
- `zinder_ingest_canonical_historical_prevout_reads_total`; and
- `zinder_ingest_canonical_cross_block_wallet_reads_total`.

This tracer certifies append-only service composition, not full-chain
construction performance, reorg replacement, wallet readiness, query serving,
client parity, or Railway behavior. Preserve its project-scoped volume until
the evidence has been reviewed. Remove it only with the Compose project name
and without `external` Zebra resources in the command scope.

## T3: Live mainnet (operator-hosted Zebra)

Local mainnet runs are supported against an operator-hosted Zebra:

1. Mainnet tests opt in via `require_live_for(&[Network::ZcashMainnet])` or
   `require_live_mainnet()`. They refuse to run on any other network.
2. `ZINDER_NETWORK=zcash-mainnet` plus the standard `ZINDER_NODE__*` schema
   pointed at a synced mainnet Zebra.
3. Same `cargo nextest` invocation as above:

```bash
cookie=$(docker exec <zebra_mainnet_container> cat /var/run/auth/.cookie)
ZINDER_TEST_LIVE=1 \
  ZINDER_NETWORK=zcash-mainnet \
  ZINDER_NODE__JSON_RPC_ADDR=http://127.0.0.1:8232 \
  ZINDER_NODE__AUTH__METHOD=basic \
  ZINDER_NODE__AUTH__USERNAME=${cookie%%:*} \
  ZINDER_NODE__AUTH__PASSWORD=${cookie#*:} \
  cargo nextest run --profile=ci-live --run-ignored=all
```

Currently-mainnet-only tests are:
`fetch_chain_checkpoint_returns_advancing_tree_sizes_on_mainnet`,
`tip_id_advances_above_one_million`,
`bulk_catchup_last_1000_blocks_from_checkpoint`, plus the transparent-address
balance read-only confirmations under `services/zinder-explorer/tests/live/`.

## T3: Parity against a reference lightwalletd

Compares `zinder-compat-lightwalletd` against
`electriccoinco/lightwalletd:latest`, both pointed at the same Zebra.
Catches drift in the wire-shape contract `LightdInfo`, `BlockId`,
`CompactBlock`, and `RawTransaction` use across the two shims.

These tests live in
[`services/zinder-compat-lightwalletd/tests/live/parity_against_lightwalletd.rs`](../../services/zinder-compat-lightwalletd/tests/live/parity_against_lightwalletd.rs)
and need both shims running on host-reachable ports. They skip when either
env var is absent so the default `ci-live` invocation does not require a
parity sidecar pair:

```bash
ZINDER_TEST_LIVE=1 \
  ZINDER_TEST_PARITY_ZINDER_ADDR=http://127.0.0.1:9087 \
  ZINDER_TEST_PARITY_LIGHTWALLETD_ADDR=http://127.0.0.1:9088 \
  cargo nextest run --profile=ci-live --run-ignored=all \
    -E 'test(/^live::parity_against_lightwalletd::/)'
```

Operator-divergent fields (build metadata, version strings, donation
address) are intentionally allow-listed; the assertions focus on the wire
shape both shims must agree on. The fixture-backed `ci-parity` profile
covers the same byte-shape contract without the running-binaries
dependency and is the gate enforced on every CI run.

## T3: Reorg sweep

Forces a bounded canonical-chain reorg on the regtest sidecar through
`invalidateblock`/`reconsiderblock`. The test starts the production
`RocksDbCanonicalStore` writer, reads its narrow
`IngestControl.VisibleChainEvents` stream, and verifies that the emitted
`ChainReorged` range covers the invalidated suffix. This catches drift between
Zebra rollback, current-writer replacement, and the ingest control stream.

```bash
ZINDER_TEST_LIVE=1 \
  ZINDER_NETWORK=zcash-regtest \
  ZINDER_NODE__JSON_RPC_ADDR=http://127.0.0.1:29232 \
  ZINDER_NODE__AUTH__METHOD=basic \
  ZINDER_NODE__AUTH__USERNAME=zebra \
  ZINDER_NODE__AUTH__PASSWORD=zebra \
  cargo nextest run --profile=ci-live -E 'test(/^live::reorg_sweep::/)' \
    --run-ignored=all
```

The test uses a current checkpoint plus one newly mined block for construction,
then replaces a three-block suffix. It joins the `node-mutating` nextest group;
only regtest is in scope because forced reorgs are destructive on shared or
public networks.

## T3: Network upgrade boundary crossing

Pins the per-height consensus-branch-id contract across a network
upgrade activation. The single-tip
[`mined_consensus_branch_id_parity`](../../services/zinder-ingest/tests/live/mined_consensus_branch_id_parity.rs)
test only samples the chain tip; this companion samples three heights
that straddle the latest reachable activation, so a regression in
[ADR-0008](../adrs/0008-network-parameter-discovery.md)'s discovery path
or in how `MinedTransactionChainContext.consensus_branch_id` is populated surfaces here
even when the tip happens to be in a "stable" upgrade window.

```bash
ZINDER_TEST_LIVE=1 \
  ZINDER_NETWORK=zcash-regtest \
  ZINDER_NODE__JSON_RPC_ADDR=http://127.0.0.1:29232 \
  ZINDER_NODE__AUTH__METHOD=basic \
  ZINDER_NODE__AUTH__USERNAME=zebra \
  ZINDER_NODE__AUTH__PASSWORD=zebra \
  cargo nextest run --profile=ci-live \
    -E 'test(/^live::network_upgrade_boundary::/)' --run-ignored=all
```

The same invocation works for testnet (`ZINDER_NETWORK=zcash-testnet`,
target activation = NU6.1 at height 3,536,500) and mainnet
(`ZINDER_NETWORK=zcash-mainnet`, target activation = NU6.1 at height
3,146,400). On testnet/mainnet the test anchors a checkpoint just below
the activation so the bulk catchup window stays at three blocks.

File:
[`services/zinder-ingest/tests/live/network_upgrade_boundary.rs`](../../services/zinder-ingest/tests/live/network_upgrade_boundary.rs).

## T3: Writer fencing and crash recovery

Multi-process storage access is the production deployment shape per
[ADR-0003](../adrs/0003-canonical-storage-access-boundary.md): one
`zinder-ingest` canonical primary, one `zinder-projector` wallet primary,
reader-local projector/compatibility secondaries, and no second writer for
either namespace. This section validates the structural RocksDB fencing and
the operationally critical "owner crashed, published readers survive"
semantic one store at a time; the wallet-serving admission tests cover the cross-store
boundary.

### Primary and secondary coverage

Three integration tests under
[`crates/zinder-store/tests/integration/primary_secondary.rs`](../../crates/zinder-store/tests/integration/primary_secondary.rs)
run with every `cargo nextest run --profile=ci`:

- `second_primary_open_returns_primary_already_open` — proves a second
  writer cannot silently take over an already-open primary store; the
  RocksDB lock surfaces as `StoreError::PrimaryAlreadyOpen`.
- `secondary_continues_serving_after_primary_drops` — opens a primary,
  commits two epochs, drops the primary handle, then asserts a fresh
  secondary opened against the same path serves the last-committed
  epoch without the primary coming back up. The restart stage of the same test
  asserts a restarted primary resumes from the durable state.
- `secondary_catches_up_after_primary_commits` — the live-writer
  catchup baseline; pre-existing.

### Manual reproduction: kill -9 mid-commit

Use this when changing storage commit paths or the
`PrimaryChainStore::commit_chain_epoch` body. The automated test cannot
exercise unclean shutdown mid-batch because dropping the handle in
process closes RocksDB cleanly.

```bash
# Terminal 1: start the phase-driven ingest loop against z3 regtest.
mkdir -p .tmp
rm -rf .tmp/regtest.zinder-store
cargo run --release --bin zinder-ingest -- \
  --config .tmp/regtest.ingest.toml &
INGEST_PID=$!

# Terminal 1 (after a few blocks commit): SIGKILL the writer mid-flight.
sleep 5
kill -9 "$INGEST_PID"
wait "$INGEST_PID" 2>/dev/null || true

# Reopen and assert state is consistent. The status command opens the
# canonical store through its normal fail-closed admission path.
cargo run --release --bin zinder-ingest -- \
  --config .tmp/regtest.ingest.toml status
```

Expected: status reports a durable store tip and exits successfully. RocksDB's
WAL replay restores any committed batch on reopen; partial commits are visible
only after their write batch is durably persisted, so the reopened tip is
always at or below the last batch the writer signaled durable.

### Manual reproduction: second-writer fencing across processes

```bash
cargo run --release --bin zinder-ingest -- \
  --config .tmp/regtest.ingest.toml &
sleep 2
# This second invocation must fail with PrimaryAlreadyOpen (LOCK file in
# .tmp/regtest.zinder-store/LOCK). If it succeeds, the fencing contract
# is broken.
cargo run --release --bin zinder-ingest -- \
  --config .tmp/regtest.ingest.toml
```

Expected: the second `zinder-ingest` process exits non-zero with an
error message naming the LOCK path. The first process continues
uninterrupted.

## Concurrent readers and stream cancellation

Production deployments fan many clients into each immutable-pair
`zinder-compat-lightwalletd` server. The gRPC adapter must serve concurrent
streamed reads without serializing them and clean up request-owned generation
handles when a client disconnects mid-stream.

### Stream cancellation coverage

Two integration tests under
[`services/zinder-query/tests/integration/stream_cancellation.rs`](../../services/zinder-query/tests/integration/stream_cancellation.rs)
run with every `cargo nextest run --profile=ci`:

- `dropping_compact_block_range_stream_does_not_break_subsequent_requests`
  — opens a `compact_block_range` stream, reads one chunk, drops it,
  then opens a second stream and drains it. Asserts the server stays
  responsive after a mid-stream cancellation.
- `parallel_compact_block_range_readers_all_drain_to_completion` —
  spawns 16 concurrent clients each draining the full committed range
  and asserts every reader finishes within a 20-second deadline.

### Manual reproduction: N concurrent compat readers

Operator-grade reproduction needs a real bulk-caught-up store plus a real
gRPC client. Two terminals:

```bash
# Terminal 1: serve over a populated store.
cargo run --release --bin zinder-compat-lightwalletd -- \
  --config .tmp/regtest.compat.toml

# Terminal 2: fan out N parallel GetBlockRange calls.
for reader_index in $(seq 1 10); do
  grpcurl -plaintext \
    -import-path crates/zinder-proto/proto/compat/lightwalletd \
    -proto crates/zinder-proto/proto/compat/lightwalletd/service.proto \
    -d '{"start":{"height":1},"end":{"height":100}}' \
    127.0.0.1:9067 cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlockRange \
    > ".tmp/reader-$reader_index.out" &
done
wait
wc -l .tmp/reader-*.out
```

Expected: every reader receives a non-zero number of lines, no
`-1`/transport errors, and no reader hangs past the others.

## Performance calibration (T2 + live latency)

Two layers, both useful.

### T2 perf budgets

Synthetic, fast, deterministic; runs every commit:

```bash
cargo nextest run --profile=ci-perf
```

The `services/zinder-query/tests/perf/perf_smoke.rs` library-contract fixture holds the budgets;
each test asserts an upper bound (`PERF_SMOKE_LATEST_BUDGET = 250 ms`,
`PERF_SMOKE_RANGE_BUDGET = 2 s`, and
`PERF_SMOKE_FULL_BLOCK_RANGE_BUDGET = 5 s` for 1,000 blocks). A budget
violation is a test failure.

### Live latency baseline

Real Zebra, real RocksDB; reports microsecond budgets per call:

```bash
ZINDER_TEST_LIVE=1 \
  ZINDER_NETWORK=zcash-regtest \
  ZINDER_NODE__JSON_RPC_ADDR=http://127.0.0.1:29232 \
  ZINDER_NODE__AUTH__METHOD=basic \
  ZINDER_NODE__AUTH__USERNAME=zebra \
  ZINDER_NODE__AUTH__PASSWORD=zebra \
  cargo nextest run --profile=ci-live -E 'test(read_endpoint_latency_baseline)' \
    --run-ignored=all --no-capture
```

Look for the `live_latency_baseline` line in the output:

```text
live_latency_baseline  network=zcash-regtest  tip=2361
  visible_tip_block          = 160 µs
  compact_block_at      = 168 µs
  compact_block_range_50 = 1175 µs
  tree_state_checkpoint_at_or_before         = 249 µs
```

These numbers are not asserted by the test; treat the printed values as
operator-facing telemetry. Compare across changes to spot regressions in
the read path. The baseline does not exercise `transaction()` — that path
goes through `mempool_broadcast_cycle` (broadcast then look up the mined
txid) and `deep_chain` (high-volume bulk catchup).

### Heavy probes (trust-sensitive code only)

Run on demand for changes to storage commit paths, parser code, or anything
under `chain_store::validation`:

```bash
cargo llvm-cov --workspace --all-features --no-report
cargo mutants --workspace --all-features \
  --file crates/zinder-store/src/chain_store.rs \
  --file crates/zinder-store/src/chain_store/validation.rs \
  --file crates/zinder-store/src/canonical_store/rocksdb.rs \
  --file crates/zinder-store/src/canonical_store/activation_schedule.rs \
  --file crates/zinder-store/src/canonical_store/control.rs \
  --file crates/zinder-core/src/network_upgrade_activations.rs \
  --file crates/zinder-source/src/source_block.rs \
  --re 'chain_event_history|settled_tip_only_commit_without_artifacts|validate_reorg_window_change|from_raw_block_bytes|matching_fingerprint_prefix_len|preserves_through|next_activation_schedule|validate_activation_expectation|read_bound|validate_open_store_control|raise_reorg_policy|decode_reorg_policy_trailer'
```

These are slow (mutants often >30 min). Scheduled CI runs them weekly; locally,
gate on whether you actually changed trust-sensitive code.

## External integration: lightwalletd-compatible wallets

### Conventions for the recipes below

- The production-shaped runtime set is `zinder-ingest`, `zinder-projector`,
  `zinder-query`, and `zinder-compat-lightwalletd`. Each accepts a TOML file,
  the `ZINDER_*` environment schema, and CLI overrides in that order.
- Keep every primary and secondary path distinct. Ingest owns the canonical
  primary, projector owns the wallet primary, and each reader owns exactly 2
  bounded generations beneath its distinct canonical and wallet secondary roots.
- Run `CanonicalControl` and `IngestControl` on loopback for same-host tests.
  A non-loopback writer requires one bearer-token file shared with projector
  and both serving readers.
- Put per-process configs under `.tmp/`:

  ```toml
  # .tmp/regtest.ingest.toml
  [network]
  name = "zcash-regtest"

  [node]
  json_rpc_addr = "http://127.0.0.1:29232"

  [node.auth]
  method = "basic"
  username = "zebra"
  password = "zebra"

  [storage]
  path = ".tmp/regtest.canonical"
  raw_blob_policy = "transactions"

  [ingest]
  source = "zebra-json-rpc"
  reorg_window_blocks = 1000

  [ingest.run_overrides]
  coverage = "wallet-serving"

  [ingest_control]
  listen_addr = "127.0.0.1:9100"
  ```

  ```toml
  # .tmp/regtest.projector.toml
  [network]
  name = "zcash-regtest"

  [node]
  json_rpc_addr = "http://127.0.0.1:29232"

  [node.auth]
  method = "basic"
  username = "zebra"
  password = "zebra"

  [storage]
  canonical_path = ".tmp/regtest.canonical"
  canonical_secondary_path = ".tmp/regtest.projector-canonical-secondary"

  [wallet]
  path = ".tmp/regtest.wallet"

  [projector]
  reorg_window_blocks = 1000
  build_owner_hex = "00112233445566778899aabbccddeeff"
  lease_duration_seconds = 14400

  [ingest_control]
  addr = "http://127.0.0.1:9100"
  ```

  ```toml
  # .tmp/regtest.query.toml
  [network]
  name = "zcash-regtest"

  [node]
  json_rpc_addr = "http://127.0.0.1:29232"

  [node.auth]
  method = "basic"
  username = "zebra"
  password = "zebra"

  [storage]
  path = ".tmp/regtest.canonical"
  secondary_path = ".tmp/regtest.query-canonical-secondary"

  [wallet]
  path = ".tmp/regtest.wallet"
  secondary_path = ".tmp/regtest.query-wallet-secondary"

  [ingest_control]
  addr = "http://127.0.0.1:9100"

  [query]
  listen_addr = "127.0.0.1:9102"
  reorg_window_blocks = 1000
  pair_convergence_attempts = 12
  ```

  ```toml
  # .tmp/regtest.compat.toml
  [network]
  name = "zcash-regtest"

  [node]
  json_rpc_addr = "http://127.0.0.1:29232"

  [node.auth]
  method = "basic"
  username = "zebra"
  password = "zebra"

  [storage]
  path = ".tmp/regtest.canonical"
  secondary_path = ".tmp/regtest.compat-canonical-secondary"

  [wallet]
  path = ".tmp/regtest.wallet"
  secondary_path = ".tmp/regtest.compat-wallet-secondary"

  [ingest_control]
  addr = "http://127.0.0.1:9100"

  [compat]
  listen_addr = "127.0.0.1:9067"
  reorg_window_blocks = 1000
  pair_convergence_attempts = 12
  ```

### One-shot deterministic test

Already covered by
`services/zinder-compat-lightwalletd/tests/integration/lightwalletd_scan_contract.rs`
in the default validation gate. It:

1. Stands up `LightwalletdGrpcAdapter` over a populated `PrimaryChainStore`.
2. Connects through the generated `CompactTxStreamerClient` to exercise the
   protocol transport directly.
3. Calls `GetBlockRange`, decodes every compact block, asserts `vtx`/`txid`
   alignment with stored transaction artifacts.
4. Asserts no key material appears in any client→server payload.

This is a deterministic generic lightwalletd scan contract, not current SDK or
application certification. Running it under `cargo nextest run --profile=ci`
is part of every commit.

### End-to-end with a real lightwalletd-compatible client

Use five terminals and keep all 4 Zinder processes alive. Stopping ingest before
reader bootstrap is invalid because each reader authenticates every candidate
pair through live `CanonicalControl`.

```bash
# Terminal 1: canonical writer and live control plane.
cargo run --release --bin zinder-ingest -- \
  --config .tmp/regtest.ingest.toml
```

```bash
# Terminal 2: current-format wallet construction and continuous following.
cargo run --release --bin zinder-projector -- \
  --config .tmp/regtest.projector.toml
```

```bash
# Terminal 3: immutable native WalletQuery reader.
cargo run --release --bin zinder-query -- \
  --config .tmp/regtest.query.toml
```

```bash
# Terminal 4: immutable wallet-serving lightwalletd compatibility reader.
cargo run --release --bin zinder-compat-lightwalletd -- \
  --config .tmp/regtest.compat.toml
```

```bash
# Terminal 5: probe native WalletQuery, then point the legacy wallet or SDK at
# http://127.0.0.1:9067.
grpcurl -plaintext \
  -import-path crates/zinder-proto/proto \
  -proto zinder/v1/wallet/wallet.proto \
  -d '{}' 127.0.0.1:9102 \
  zinder.v1.wallet.WalletQuery/ServerInfo

grpcurl -plaintext -d '{}' 127.0.0.1:9067 \
  cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo
```

Require ingest, projector, native query, and compatibility `/readyz` to be ready
before recording client evidence. Record both native `WalletQuery/ServerInfo`
and compatibility `GetLightdInfo`; a typed projection-behind or replica-behind
result is a failed admission for the current sample, not an empty wallet response.

For production Android-wallet claims

- Create-new-wallet bootstrap reaches the compat endpoint's advertised tip
  without protocol-shape errors.
- Restore/import and resync flows request tree state and subtree roots inside
  the complete wallet-serving retained range. Unknown tree-state or
  subtree-root gaps are release blockers, not acceptable follow-up notes.
- `GetAddressUtxosStream` is backed by stored transparent output artifacts.
  Synthetic empty responses, upstream-node fallbacks, and compact-block scans do
  not satisfy the wallet-serving contract.
- Pending-transaction UX requires `SendTransaction` and `GetMempoolStream`
  through the selected endpoint, followed by `GetTransaction` after mining.
  `GetMempoolTx` remains broader lightwalletd compatibility and is not part of
  the current Android SDK inventory.
- Send tests record writer-tip lag at submission time. A success when the writer
  is outside the wallet expiry window is not production evidence.

Reference comparison points: `zec.rocks` for public lightwalletd behavior and
`lightwalletd_scan_contract.rs` for the in-process protocol baseline.

## Public native `WalletQuery` contract

`zinder-query` owns the native wallet request types, query API, error mapping,
gRPC adapter, and standalone serving runtime. It is a first-class external
protocol surface for native consumers such as Zally and is deployed alongside
ingest and projector in the wallet-serving topology. Certify it directly
through its unit and integration tests plus the
[standalone native WalletQuery smoke](#standalone-native-walletquery-smoke).

`zinder-compat-lightwalletd` is a separate first-class external service that
translates the lightwalletd protocol. Its parity and wallet certification do
not replace native WalletQuery contract or live-smoke coverage, and native
coverage does not replace compatibility certification.

### Capability descriptor

Expected entries (this list is the public contract; treat any drift as a
breaking change requiring a capability rename). The
`zinder-proto::integration::capability_docs::testing_runbook_capability_list_mirrors_zinder_capabilities`
test asserts the list below equals the wallet and explorer rows of the
[`CAPABILITIES`](../../crates/zinder-proto/src/capabilities.rs) table:

<!-- capability-list:testing-runbook:start -->
```text
wallet.read.visible_tip_block_v1
wallet.read.settled_tip_block_v1
wallet.read.block_id_by_selector_v1
wallet.read.block_header_by_selector_v1
wallet.read.compact_block_at_v2
wallet.read.compact_block_range_v2
wallet.read.compact_block_ironwood_v2
wallet.read.full_block_at_v1
wallet.read.full_block_range_v1
wallet.read.tree_state_at_height_v2
wallet.read.latest_tree_state_checkpoint_v2
wallet.read.subtree_roots_in_range_v1
wallet.read.subtree_roots_ironwood_v1
wallet.read.transaction_by_id_v2
wallet.read.transaction_bytes_v1
wallet.read.server_info_v2
wallet.read.network_upgrade_activations_v1
wallet.broadcast.transaction_v1
wallet.events.chain_v1
wallet.snapshot.mempool_v3
wallet.events.mempool_v2
wallet.mempool.transparent_outputs_by_address_v1
wallet.mempool.transparent_spends_by_outpoint_v1
wallet.mempool.transparent_outputs_by_outpoint_v1
wallet.read.transparent_outputs_by_outpoint_v1
wallet.read.transparent_spends_by_outpoint_v1
wallet.read.transparent_unspent_outputs_by_outpoint_v1
wallet.read.chain_value_pools_at_tip_v1
wallet.read.transparent_utxo_set_summary_v1
wallet.read.transparent_utxo_set_commitment_v1
wallet.address.transparent_unspent_outputs_v1
wallet.address.transparent_history_v1
wallet.address.transparent_balance_v1
explorer.server_info_v1
explorer.transaction.detail_v4
explorer.block.summary_v1
explorer.block.production_series_v2
explorer.block.production_time_range_v1
explorer.block.detail_v1
explorer.block.transactions_v2
explorer.block.final_note_commitment_roots_v1
explorer.block.activity_distribution_v1
explorer.search_v1
explorer.commitment_root.search_v1
explorer.commitment_root.displaced_matches_v1
explorer.mempool.summary_v1
explorer.mempool.snapshot_v1
explorer.mempool.activity_v1
explorer.transparent_address.activity_v2
explorer.transparent_address.deltas_v1
explorer.fee.summary_v1
explorer.fee.conventional_distribution_v1
explorer.fee.paid_distribution_v1
explorer.value_pool.summary_v1
explorer.network_upgrade.status_v1
explorer.value_pool.flow_history_v1
explorer.value_pool.flow_events_in_range_v1
explorer.value_pool.flow_summary_v1
explorer.value_pool.flow_amount_threshold_summary_v1
explorer.value_pool.flow_rounded_amount_summary_v1
explorer.value_pool.balance_history_v1
explorer.utxo_set.summary_v1
explorer.utxo_set.commitment_v1
explorer.chain.reorg_history_v1
explorer.chain.displaced_block_history_v1
explorer.chain.displaced_block_detail_v1
explorer.mempool.event_counts_v1
explorer.transaction.fees_v1
explorer.transaction.history_v1
explorer.transaction.recent_v1
explorer.transaction.history_v2
explorer.transaction.intrinsic_value_balances_v1
explorer.transaction.component_summary_v2
explorer.transparent_address.ranking_v1
explorer.overview.snapshot_v1
explorer.migration.overview_v1
explorer.migration.cohorts_v1
explorer.migration.denominations_v1
```
<!-- capability-list:testing-runbook:end -->

The wallet rows above describe the public native contract advertised by the
standalone `zinder-query` runtime. The compatibility adapter may consume those
capabilities internally, but its lightwalletd protocol remains a separately
certified external surface. Explorer rows belong to the optional
`zinder-explorer` runtime and are not published as a release image.

## Failure interpretation reference

A live test failure is one of these classes. Read the error string before
debugging code.

| Error fragment | Class | Action |
| -------------- | ----- | ------ |
| `set ZINDER_TEST_LIVE=1 plus ZINDER_NETWORK and ZINDER_NODE__* env vars` | Opt-in missing | Add the env vars; not a real failure |
| `live test allowed only on [ZcashMainnet]` | Mainnet-only gate | Expected on regtest/testnet; refuse to bypass |
| `live test allowed only on [ZcashTestnet, ZcashMainnet]` | Hosted-network-only gate | Expected on regtest; rerun against testnet or mainnet |
| `requires ZINDER_NODE__INDEXER_GRPC_ADDR` | Indexer-feature missing | Restart Zebra with `ZEBRA_RPC__INDEXER_LISTEN_ADDR` and add the env var |
| `connection refused` / `transport error` | Sidecar not running | Restart z3 / verify ports |
| `chain_epoch.network does not match` | Wrong network | Check `ZINDER_NETWORK` matches the running Zebra |
| `schema mismatch` | Persisted store from a different schema version | Use a fresh `.tmp/<network>.zinder-store/` directory |
| `compact_block_range_too_large` / `invalid_block_range` | Real assertion | Treat as a real failure — investigate |
| `block_not_in_best_chain` from a hash lookup | Reorged out OR never indexed | Check the chain epoch you queried; not necessarily a bug |

If a test that previously passed starts failing, run with
`RUST_LOG=debug,zinder=trace` and the `--no-capture` flag for live tests:

```bash
ZINDER_TEST_LIVE=1 ... \
RUST_LOG=debug,zinder=trace \
cargo nextest run --profile=ci-live -E 'test(<failing_test>)' \
  --run-ignored=all --no-capture 2>&1 | tee .tmp/<failing_test>.log
```

The `.tmp/` directory is `.gitignore`'d for exactly this purpose.

## External certification procedures

Use this table when a change makes a production-readiness or consumer-support
claim after the default and live-node gates are already green. Each item points
at the owning detail instead of repeating the full procedure here. Keep evidence
artifacts in `.tmp/` unless the result changes a durable claim, in which case
update the referenced architecture or reference page.

| Procedure | Friction | Run shape | Pass condition | Evidence to keep | Owning detail |
| --- | --- | --- | --- | --- | --- |
| Full wallet-serving testnet bulk catchup | Medium | Against a synced testnet Zebra, start the checked-in ingest, projector, native query, and compatibility topology with fresh canonical and wallet stores. | Canonical construction reaches READY, the projector publishes the same authenticated fence, both readers open exact canonical/wallet secondary pairs, every `/readyz` endpoint is ready, native `WalletQuery/ServerInfo` reports the admitted tip, and `GetLightdInfo` reports the same non-zero height with `taddrSupport: true`. | Logs and `/readyz` JSON for all four runtimes; `WalletQuery/ServerInfo` and `GetLightdInfo` output; canonical floor/tip; wallet source event sequence and digest. | [Wallet data plane §External Wallet Compatibility Claims](../architecture/wallet-data-plane.md#external-wallet-compatibility-claims), [Chain ingestion §Operation Shape](../architecture/chain-ingestion.md#operation-shape) |
| Testnet upstream-readiness calibration | Medium | Configure Zebra with `ready_max_blocks_behind = 4`, `ready_max_tip_age = "5m"`, at least 1 live peer, and readiness enforcement on Testnet. Record the local time when each new `bestblockhash` first appears. At every sample, capture Zebra `/ready`, `getblockchaininfo`, and `getblockheader(bestblockhash)` including `time`, plus all Zinder `/readyz` endpoints and the trusted-TLS compatibility route. Cover an ordinary gap long enough to exercise the configured estimator limit, then introduce each upstream failure condition separately. | The header-time-to-local-observation offset accounts for every estimated-lag transition. Across the witnessed ordinary gap below 5 minutes, `blocks == headers`, estimated lag stays within the configured threshold, and the route remains available. Excessive local tip age, actual chain lag, peer loss, incomplete synchronization, and an unavailable health endpoint each drain readiness, and the route recovers after the upstream condition clears. If estimated lag exceeds the configured limit before local tip age while the chain is canonically current, the calibration is rejected rather than waived. | Exact Zebra config and image identity; timestamped `/ready`, `getblockchaininfo`, best-header `time`, first local height observation, Zinder readiness, and trusted-TLS samples; derived estimator inputs; consumer progress; intentional-failure start, recovery, and attribution. | [Deploying the wallet-serving container §Failure handling](deploying-wallet-serving.md#failure-handling), [Trusted TLS and ZODL compatibility admission](zodl-trusted-tls-certification.md) |
| Lightwalletd-compatible wallet bootstrap and restore | Medium | Point a real lightwalletd-compatible wallet or SDK at the testnet compat endpoint. Exercise create-new-wallet first, then restore/import or resync against the same serving store. | Create-new-wallet and restored/resync wallets reach the chain tip when requested tree-state and subtree-root heights are at or above the store floor. Below-floor requests fail only as the documented strict `NOT_FOUND` unsupported-floor case; unknown tree-state or subtree-root gaps are blockers. | Wallet logs, endpoint config diff, wallet-visible height, store floor, requested tree-state/subtree-root heights, `GetAddressUtxosStream` sample. | [External integration: lightwalletd-compatible wallets](#external-integration-lightwalletd-compatible-wallets), [Wallet data plane §External Wallet Compatibility Claims](../architecture/wallet-data-plane.md#external-wallet-compatibility-claims) |
| Current Zodl send plus mempool | Medium | With the unified `zinder-ingest` loop and mempool surface running, submit a self-send from the exact current app through its selected trusted-TLS compatibility endpoint. | `SendTransaction` returns the expected success mapping. The app's `GetMempoolStream` emits the exact submitted `RawTransaction` before mining, the bytes match the node transaction, wallet scan-back observes confirmation, and restart preserves the result. Traffic attribution shows no bundled-endpoint fallback. | App and compatibility logs, txid-correlated stream digest, matching node transaction digest, mined height, selected endpoint, no-fallback trace, and post-restart app state. | [Trusted TLS and ZODL compatibility admission](zodl-trusted-tls-certification.md), [Wallet data plane §External Wallet Compatibility Claims](../architecture/wallet-data-plane.md#external-wallet-compatibility-claims) |
| Broader lightwalletd mempool methods | Medium | Exercise every additional mempool RPC named by the compatibility claim, independently of the current Zodl method inventory. | `GetMempoolTx` returns the compact transaction while pending and empties after mining; stream lifecycle behavior matches the pinned lightwalletd contract. | Protocol-client transcript, transaction ID, compact transaction digest, mined height, and before/after responses. | [Wallet data plane §Mempool Snapshot and Subscription](../architecture/wallet-data-plane.md#mempool-snapshot-and-subscription) |
| Mainnet bounded live sweep | Low when local mainnet is synced | Run the mainnet-only `ci-live` tests against the local operator-hosted Zebra before attempting longer sessions. Prefer targeted filters first, then the full live profile if the node is healthy. | Mainnet-only tests pass on `ZINDER_NETWORK=zcash-mainnet`; non-mainnet tests either pass or skip for the documented reason. No test mutates mainnet state. | Nextest output, Zebra tip height, tested block range, node auth source used. | [T3: Live mainnet](#t3-live-mainnet-operator-hosted-zebra) |
| Lightwalletd compatibility parity | Medium | Run the same lightwalletd-compatible probes or wallet flow against Zinder and a reference lightwalletd endpoint. Include `GetBlockRange`, transaction lookup, transparent history, UTXO, mempool, and send shapes that the support claim names. | Zinder returns compatible response and error shapes where the lightwalletd contract applies. Any intentional difference is documented against the native contract or compatibility adapter. Hot-path timings stay inside the published budgets. | Per-method request/response summaries, error codes, timing samples, endpoint versions. | [T3: Parity against a reference lightwalletd](#t3-parity-against-a-reference-lightwalletd), [Wallet data plane §Performance and Pagination](../architecture/wallet-data-plane.md#performance-and-pagination) |
| Public deployment shape | Medium for internal CA, high for public cert | Put TLS in front of `zinder-compat-lightwalletd` with Caddy, nginx, or traefik, forwarding h2c to the local compat process. Use a private CA only for internal pilots; use a publicly trusted cert for public claims. | A real lightwalletd-compatible wallet validation succeeds through TLS, `GetLightdInfo` works through the proxy, public traffic cannot reach plaintext gRPC, `IngestControl`, or ops endpoints, proxy rate limiting is present for public exposure, and `--print-config` plus logs redact secrets. | Proxy config, cert source, endpoint validation logs, `grpcurl` result through TLS, bind-address audit, redacted `--print-config` output. | [Trusted TLS and ZODL compatibility admission](zodl-trusted-tls-certification.md), [Integration surfaces §Lightwalletd Compatibility](../reference/integration-surfaces.md#lightwalletd-compatibility) |
| Observability and mempool lifecycle semantics | Low | Run `scripts/observability-smoke.sh run` against regtest or a bounded public-network smoke. After it completes, manually query the documented `zinder_mempool_*` series because the smoke report does not yet assert the lifecycle and retention series. Inspect `/readyz`, Prometheus, and Grafana after mempool traffic and one retention pass. | Healthy traffic has no blocking readiness cause; exactly one mempool lifecycle state is active; retained count, floor sequence and age are present; examined-event and examined-encoded-byte histograms reflect bounded retention steps; retention duration uses only the documented closed step outcomes. Withdrawing the mempool hydration gate makes ingest non-ready until a newly certified snapshot at the exact canonical tip and a fresh canonical observation restore it. | `.tmp/observability/reports/latest-readiness.json`, latest readiness Markdown, manual Prometheus query output, Grafana screenshot or panel JSON, service logs. | [Service operations §Health and Readiness](../architecture/service-operations.md#health-and-readiness), [Observability smoke](../../observability/README.md) |
| Coherent backup, restore, and rolling restart | High until the coherent bundle exists | Produce one authenticated canonical/wallet checkpoint bundle, stop the stack, restore both stores into fresh paths, then start ingest, projector, native query, and compatibility in ownership order. Restart compatibility, query, projector, and ingest separately while the source advances. | The restored stores carry one matching canonical event fence and wallet digest; all owners fail closed on mismatches; both readers publish only an admitted wallet-serving pair; the 10,000-block tail reaches ready within 15 minutes; later restarts preserve continuous following. An ad hoc copy of independently timed RocksDB directories is not evidence. | Bundle manifest and digests, restore duration, tail range, source and restored fences, restart logs, all four `/readyz` responses, native `WalletQuery/ServerInfo` after every restart, compatibility `GetLightdInfo` after every restart, and second-owner failure output. | [ADR-0035 §Recovery boundary](../adrs/0035-canonical-storage-topologies.md#recovery-boundary), [Service operations §Recovery](../architecture/service-operations.md#recovery) |
| Long soak | High | Run ingest, projector, native query, and compatibility for several hours or longer while scraping every runtime and exercising native and compatibility wallet reads, mempool streams, and transaction submission. | No unexplained readiness flaps; canonical, projection, and wallet-serving lag stay bounded; cursor-retention and RocksDB alerts stay quiet; memory growth is explainable; and restart/shutdown is clean. | Metrics scrape and readiness samples for all four runtimes, native `WalletQuery/ServerInfo` samples, compatibility `GetLightdInfo` samples, process logs, memory/RSS samples, and final restart result. | [Service operations §Validation Tiers](../architecture/service-operations.md#validation-tiers), [Wallet data plane §Performance and Pagination](../architecture/wallet-data-plane.md#performance-and-pagination) |
| Certification evidence manifest | Low | Before declaring a production-ready support claim, collect the commands, versions, network, node tip, consumer versions, and artifact paths from the rows above into one file under `.tmp/production-readiness/<run-id>/manifest.md` or `.json`. | A reviewer can replay the certification story without chat history: every claimed consumer, network, command, binary version, and evidence artifact has a path and pass/fail result. | Manifest file, command transcript, commit SHA, binary versions, Zebra version and tip height, consumer versions. | This runbook |

## Production release certification checklist

Use this checklist only when the goal is a production-readiness or release claim.
For ordinary code changes, use the shorter pre-flight checklist below.

- [ ] Default validation gate is green.
- [ ] `cargo nextest run --profile=ci-parity` is green and its report is archived
      with the release evidence.
- [ ] T3 regtest is green with Zebra indexer gRPC enabled.
- [ ] Testnet live sweep is green for every wallet-serving claim in the release.
- [ ] Testnet upstream-readiness calibration records the best-header timestamp
      and first local height observation, then proves the configured estimator
      allowance prevents witnessed normal-gap flaps while peer loss,
      incomplete sync, actual lag, tips older than 5 minutes, and an unavailable
      Zebra health endpoint each fail closed and recover independently.
- [ ] Mainnet read-only targeted tests are green when the release makes a mainnet
      readiness claim.
- [ ] Full wallet-serving bulk-catchup and tip-follow phase evidence exists for the target
      network, including store floor, tip height, `/readyz`, and reader secondary
      state.
- [ ] Zodl or Android SDK bootstrap, restore/resync, transparent output, send,
      and mempool evidence is green when claiming lightwalletd-compatible wallet
      support.
- [ ] Public deployment audit is green: TLS on the public wallet endpoint, no
      public plaintext gRPC, no public ops endpoints, no public `IngestControl`,
      rate limiting present, filesystem permissions restricted, and secrets
      redacted in logs and `--print-config`.
- [ ] Observability smoke or calibration report exists, with zero traffic-blocking
      readiness causes and warning causes classified separately.
- [ ] Backup restore and restart evidence exists for the store used in the claim.
- [ ] Long soak evidence exists when the release touches ingestion, storage,
      mempool, compatibility serving, or public deployment shape.
- [ ] A release evidence manifest points to every command, version, network,
      artifact, and pass/fail result used for the claim.

## Pre-flight checklist (before declaring a change shipped)

- [ ] Default validation gate green (`cargo fmt`/`cargo clippy`/`cargo nextest run --profile=ci`/`cargo nextest run --profile=ci-perf`/`cargo doc`/`cargo deny`/`scripts/runbook-lint.sh`).
- [ ] If the change touched a consumer-facing wire contract or compatibility
      adapter: `cargo nextest run --profile=ci-parity`.
- [ ] Live regtest sweep green (`ci-live` profile against z3, with indexer
      gRPC env), including
      [Reorg sweep](#t3-reorg-sweep),
      [Network upgrade boundary crossing](#t3-network-upgrade-boundary-crossing),
      and [Writer fencing and crash recovery](#t3-writer-fencing-and-crash-recovery)
      where applicable. Hosted-network-only gates are expected to refuse regtest
      and should be rerun against testnet or mainnet.
- [ ] If the change touched storage commit, schema, or multi-process semantics:
      the [Writer fencing and crash recovery](#t3-writer-fencing-and-crash-recovery)
      `kill -9` and second-writer recipes have been replayed locally.
- [ ] If the change touched a streamed gRPC surface or the
      `zinder-compat-lightwalletd` adapter: the
      [Concurrent readers and stream cancellation](#concurrent-readers-and-stream-cancellation)
      tests have been replayed.
- [ ] If the change touched the lightwalletd wire surface: a manual end-to-end
      run with a lightwalletd-compatible wallet or the Android SDK against
      `zinder-compat-lightwalletd`.
- [ ] If the change touched the public native `WalletQuery` contract: its unit
      and integration tests plus the standalone live contract smoke are green,
      and the capability descriptor reflects every added, removed, or renamed
      capability. Run compatibility parity separately when the change also
      affects the lightwalletd adapter or shared behavior it exposes.
- [ ] If the change altered storage byte layout: `cargo mutants` against
      `chain_store.rs` and `chain_store/validation.rs` is a healthy plus
      coverage run via `cargo llvm-cov`.
- [ ] If the change altered readiness, metrics, backup, restart, or deployment
      configuration: run the relevant observability or deployment row from the
      ecosystem validation queue.
- [ ] If the change is mainnet-relevant: run the targeted mainnet live tests
      against an operator-hosted Zebra and record the evidence path.

## Runbook self-test

This runbook is operator-facing, dense, and changes often. Three classes
of drift are easy to introduce and expensive to discover during an
incident: a typo in a fenced shell command, a profile name that no
longer matches `.config/nextest.toml`, or a capability list that has
fallen behind the `CAPABILITIES` table. Each is caught by a piece of the
standard validation gate.

| Drift class | How it's caught | Where |
| ----------- | --------------- | ----- |
| Bash syntax in a fenced `bash` block | `scripts/runbook-lint.sh` runs every block through `bash -n`. Wired into the default validation gate above. | [`scripts/runbook-lint.sh`](../../scripts/runbook-lint.sh) |
| Profile-name drift between this runbook and `.config/nextest.toml` | `testing_runbook_profile_names_exist_in_nextest_toml` asserts every `--profile=<name>` quoted in this file resolves to a real `[profile.<name>]` section. | [`crates/zinder-proto/tests/integration/capability_docs.rs`](../../crates/zinder-proto/tests/integration/capability_docs.rs) |
| Default-filter drift between this runbook and `.config/nextest.toml` | `testing_runbook_default_filter_mirrors_nextest_toml` asserts the quoted expression matches the canonical one. | Same file. |
| Capability-list drift between this runbook and the `CAPABILITIES` table | `testing_runbook_capability_list_mirrors_zinder_capabilities` asserts the list under the `<!-- capability-list:testing-runbook -->` markers matches the wallet and explorer rows of the table. | Same file. |

All four checks ship with the default `cargo nextest run --profile=ci`
invocation; no separate runbook-self-test profile is needed. When you
add a new fenced command, profile, or capability, the corresponding
check fires on the next CI run if the runbook and code diverge.

To extend the self-test (for example, to gate a new profile name or a
new doc embedded constant), add an entry to
`RUNBOOK_REFERENCED_PROFILES` (or a peer constant) in
[`capability_docs.rs`](../../crates/zinder-proto/tests/integration/capability_docs.rs)
and let the existing assertion shape catch drift. The runbook-lint
script needs no changes for new blocks — it discovers them by markdown
fence syntax.

## Cross-references

- [ADR-0003: Epoch-bound storage access with RocksDB secondaries](../adrs/0003-canonical-storage-access-boundary.md) — secondary catchup and writer-status semantics that some live tests verify.
- [ADR-0006: IngestControl transport security](../adrs/0006-ingest-control-transport-security.md) — the bearer-token contract referenced in the external-integration conventions.
- [Service operations](../architecture/service-operations.md) — the operator-facing deployment story; the external-integration recipes here use its single-host, single-store conventions. For multi-host or multi-process deployments, follow that doc's recipes instead.
- [Wallet data plane](../architecture/wallet-data-plane.md) — the wire surface the external integration tests exercise.
- [Integration surfaces](../reference/integration-surfaces.md) — the consumer-facing support boundaries this runbook certifies.
- [Observability smoke](../../observability/README.md) — the local metrics and readiness harness; it records restore as blocked until a coherent canonical-plus-wallet bundle exists.
