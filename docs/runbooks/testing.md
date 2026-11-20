# Testing

This runbook tells contributors how to run Zinder's tests: the test tiers,
the validation gate every change must pass, what CI runs, and how to run the
live tests against a real Zebra or Zakura node.

## Test tiers

Tests are split into tiers by where they live. Each tier has a
[nextest](https://nexte.st) profile in `.config/nextest.toml`.

| Tier | Location | Profile | Needs | Runs in CI |
| --- | --- | --- | --- | --- |
| T0 unit | `#[cfg(test)]` modules in `src/` | `ci` | Nothing | Every pull request |
| T1 integration | `tests/integration/` | `ci` | Nothing | Every pull request |
| T1 PostgreSQL | `services/zinder-bench/tests/integration/postgres_canonical_replay.rs` | `ci-postgres` | A fresh, disposable PostgreSQL database | Every pull request |
| T2 perf | `tests/perf/` | `ci-perf` | Nothing | Every pull request |
| T3 live | `tests/live/` | `ci-live` | A running node, and for some tests more endpoints | Nightly, on node-facing pull requests, and weekly on Testnet |
| T4 parity | `tests/parity/` in `zinder-client` | `ci-parity` | Nothing; it uses fixtures | Every pull request |

The default profile's filter keeps the live, perf, and parity tiers out of an
ordinary run:

```toml
default-filter = "not test(/^live::/) and not test(/^perf::/) and not test(/^parity::/)"
```

Every live test is also `#[ignore]`d and checks `ZINDER_TEST_LIVE=1` at run
time. A stray `cargo test` therefore never contacts a node. The PostgreSQL
test is `#[ignore]`d for the same reason.

The T4 parity tier checks the request and error shapes that consumers depend
on: lightwalletd clients, Zallet, lightwalletd operators, and explorers. Real
wallets are checked as [External wallets](#external-wallets) describes. Live
parity against a reference lightwalletd is a T3 test, described in
[Parity against a reference lightwalletd](#parity-against-a-reference-lightwalletd).

## Default validation gate

Run this before you consider a change complete. Every command must exit zero.

```bash
cargo fmt --all --check
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --profile=ci
scripts/test-nextest-selectors.sh
cargo nextest run --profile=ci-parity
cargo nextest run --profile=ci-perf
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps
cargo deny check
cargo machete
scripts/test-changelog.sh
scripts/validate-changelog.sh fragments
scripts/runbook-lint.sh docs/runbooks/testing.md
scripts/test-install-ci-tool.sh
git diff --check
```

`scripts/test-nextest-selectors.sh` checks that every test-name predicate in
the nextest profiles and live workflow selects a compiled test, including
ignored tests. It checks exclusions individually and reuses the workspace
test binaries.

The `dependency hygiene` CI job also runs script tests for release, deployment,
storage-benchmark, and entrypoint policy. When you change one of those areas,
run the matching `scripts/test-*.sh` locally.

### Area-specific gates

Run these in addition to the default gate when your change touches the area.

| You changed | Run |
| --- | --- |
| `zinder-core`, `zinder-proto`, `zinder-client`, their manifests, or generated protocol files | `scripts/verify-sdk-packages.sh` |
| The PostgreSQL canonical-replay path or its driver dependencies | The [PostgreSQL gate](#postgresql-gate) |
| Storage commit paths, parsers, or the canonical live commit, replacement, or event lifecycle | The [heavy probes](#heavy-probes) |
| Node-facing code | The [live tests](#live-tests) on Regtest |

`scripts/verify-sdk-packages.sh` packages the three public crates, builds them
with `PROTOC` unavailable, and compiles a standalone consumer against the
extracted packages. It catches path leakage and missing package files that a
workspace build hides.

### PostgreSQL gate

The URL must name a fresh database, because the test leaves its schema in
place and then checks that reuse is rejected.
[Storage benchmark environment](../../deploy/storage-benchmark.md#run-the-postgresql-driver-integration-gate)
shows how to start and remove that database.

```bash
ZINDER_TEST_POSTGRES_DATABASE_URL='postgresql://zinder_bench:zinder_bench_local_only@127.0.0.1:55432/zinder_bench' \
  cargo nextest run -p zinder-bench --profile=ci-postgres --run-ignored=all
```

### Heavy probes

Coverage and mutation testing are slow. Scheduled CI runs them weekly. The
`mutation` job of `.github/workflows/ci-pr.yml` runs the same file list and
`--re` filter as the command below, so change all of them together.

`--cap-lints true` stops the workspace's denied warnings from failing a
mutant's build. Without it, a mutant that replaces a function body leaves the
parameters unused, fails to compile, and counts as unviable instead of being
tested.

`cargo mutants --workspace` selects the packages to mutate, but it runs only
the tests of the package that holds each mutant. A mutant in `zinder-store`
is therefore caught only by a `zinder-store` test, so put the assertion that
kills a mutant in the crate that owns the mutated file.

The file list and filter select about 295 mutants. A run with `--jobs 3` on
a GitHub-hosted runner took 56 minutes for 130 mutants, which puts one
unsharded run near two hours. The job splits the mutants into three shards
with `--shard`, about 100 mutants and under an hour each, and keeps a 90-minute
timeout per shard. Grow the shard count in the workflow matrix, and the
`--shard` denominator with it, before a shard approaches the timeout. A local
run can use the same flag to cover one slice, such as `--shard 0/3`.

```bash
cargo llvm-cov --workspace --all-features --no-report
cargo mutants --workspace --all-features --cap-lints true \
  --file crates/zinder-store/src/canonical_store/live_commit.rs \
  --file crates/zinder-store/src/canonical_store/live_replacement.rs \
  --file crates/zinder-store/src/canonical_store/event_lifecycle.rs \
  --file crates/zinder-store/src/format/stream_cursor.rs \
  --file crates/zinder-store/src/canonical_store/rocksdb.rs \
  --file crates/zinder-store/src/canonical_store/activation_schedule.rs \
  --file crates/zinder-store/src/canonical_store/control.rs \
  --file crates/zinder-store/src/canonical_store/served_tree_state.rs \
  --file crates/zinder-store/src/canonical_store/tree_state_replay.rs \
  --file crates/zinder-store/src/canonical_store/tree_state_densification.rs \
  --file crates/zinder-core/src/network_upgrade_activations.rs \
  --file crates/zinder-source/src/source_block.rs \
  --file services/zinder-ingest/src/artifact_builder.rs \
  --re 'canonical_event_history|validate_live_settled_tip|validate_live_block_extension|validate_replacement_blocks|verify_auth_tag|from_raw_block_bytes|compact_sapling_outputs|compact_orchard_actions|compact_note_ciphertext_prefix|matching_fingerprint_prefix_len|preserves_through|next_activation_schedule|validate_activation_expectation|read_bound|validate_open_store_control|raise_reorg_policy|decode_reorg_policy_trailer|framed_tree_size|skip_framed_node|read_tree_state_checkpoint_at|lookup_tree_state|find_tree_state|replay_tree_state|refuse_unadmitted_activations|reconstruct|required_seed_height|replay_compact_blocks|append_compact_block_commitments|requires_checkpoint|after_seed|scan_tree_state_strides|stride_ending_at_satisfied|next_tree_state_stride|first_seed_at_or_above|stride_satisfies|stored_heights_through_end|stored_tree_sizes|densify_tree_state_stride|replay_stride|append_stored_block|commitment_count|recorded_tree_sizes|commit_tree_state_stride|StoredStrideRows::read|StoredStrideRows::at|ReplayedTrees::append_block|ReplayedTrees::canonical_block_time|ReplayedTrees::checkpoint'
```

## Run one test or one tier

Filter with a nextest expression. Integration tests live in each crate's
`tests/{integration,live,perf}/` modules, and the per-crate test binary is
`tests/acceptance.rs`, so those test paths start with the tier name.

```bash
# One test in one crate.
cargo nextest run -p zinder-query -E 'test(parallel_compact_block_range_readers_all_drain_to_completion)'

# One module of a tier.
cargo nextest run -E 'test(/^integration::cli::/)'

# One live test, with its logs.
RUST_LOG=debug,zinder=trace \
  cargo nextest run --profile=ci-live --run-ignored=all --no-capture \
    -E 'test(read_endpoint_latency_baseline)'
```

The `ci` profile retries a failed test once and `ci-live` retries twice.
Nextest has no rerun-failures flag, so copy the failed name into `-E`.
`cargo test --workspace --all-features` works as a fallback runner, but it is
not the gate.

## Writing tests that CI can run

Tests can run from a `cargo nextest archive` on a machine that did not build
them, so they must not depend on build-machine paths:

- Start a service binary with `zinder_testkit::bin_exe!("zinder-query")`, not
  `env!("CARGO_BIN_EXE_<name>")`. The macro prefers the path nextest sets at
  run time.
- Resolve the source tree from the run-time `CARGO_MANIFEST_DIR`. A scan must
  fail when the tree is missing or nearly empty, never pass as clean.

Process-crash tests kill a real writer at a named point inside a commit, then
reopen the store and assert it holds exactly one complete batch. To cover a
new write path:

1. Add `crash_point("<store>.<write_path>.before_atomic_write")` and
   `...after_atomic_write` calls around its single atomic write. In the owning
   crate, `crash_point` resolves to `zinder_testkit::process_crash` only under
   `#[cfg(test)]` and to an empty function otherwise.
2. Add a child test that calls `crash_child()`, drives the write, and returns
   an error if it gets past the armed point.
3. Add a parent test, marked `#[cfg(unix)]` and named
   `<behavior>_across_process_abort`, that calls `run_crash_child` and checks
   the reopened store. The nextest override for that suffix allows 60 seconds.

`rg 'crash_point\("'` lists every existing crash point. These tests prove
atomic recovery to a complete batch. They do not prove durability across power
loss, because an aborted process leaves the page cache intact. That
durability rests on the synced (`set_sync(true)`) write points, and no CI tier
exercises it.

## Continuous integration

| Workflow | Runs on | What it checks |
| --- | --- | --- |
| `ci-pr` | Every pull request, pushes to `main`, and weekly | The `rust` job runs format, check, clippy, docs, and the `ci`, `ci-postgres`, `ci-parity`, and `ci-perf` profiles. `dependency hygiene` runs the changelog, runbook, alignment, and release-policy scripts, `cargo deny`, and `cargo machete`. Other jobs check the SDK packages and the vendored protocols. Coverage and mutation run only on the weekly schedule or a manual run. |
| `protocol-contracts` | Pull requests and `main` pushes that touch the protocol | Checked-in generated code, `proto breaking` (pull requests only), and capability coverage |
| `live-tests` | Node-facing pull requests, nightly Regtest, weekly Testnet, and manual Mainnet | The T3 suite, once against Zebra and once against Zakura on Regtest |
| `parity-compat` | Node-facing pull requests and nightly | Live parity against a reference lightwalletd, on both nodes |
| `binary-archives` | Pull requests that touch build inputs | Release binary archives for both platforms |
| `build-images` | Pull requests that touch images or deployment files | Deployment admission and runtime image smoke builds |

All workflows follow these rules:

- **A new push cancels the superseded run.** `ci-pr`, `protocol-contracts`,
  `live-tests`, and `parity-compat` group runs by workflow, event, and pull
  request number. Runs on `main`, scheduled runs, and manual runs are never
  cancelled. `binary-archives` and `build-images` cancel any older run for the
  same pull request or reference, including a manual one.
- **Every job has a timeout.** A hung job stops at its `timeout-minutes`.
- **Tools come from checksum-pinned release assets.** No job compiles
  `cargo-nextest`, `cargo-deny`, `cargo-machete`, `cargo-llvm-cov`, or
  `cargo-mutants`. `scripts/install-ci-tool.sh <name>` reads
  [`.github/ci-tools.lock`](../../.github/ci-tools.lock), verifies the
  archive's sha256 before extracting it, and installs the executable into
  `~/.cargo/bin`. To bump a tool, change its version, URL, and checksum
  together. The lock has x86_64 assets only, because no tool-installing job
  runs on arm64.
- **Rust caches are written from `main` only.** Every `Swatinem/rust-cache`
  step saves only when `github.ref` is `refs/heads/main`. The `ci-pr` `rust`
  job and all `live-tests` jobs share the `test` key, `parity-compat` uses
  `parity`, and other jobs use their own keys. In a two-node matrix, only the
  Zebra leg saves. The action hashes every `CARGO_*` and `RUST*` variable into
  the key, so jobs that share a key must keep identical workflow-level `env`
  blocks.
- **Live and parity share one path filter.** Keep the `paths` lists in
  `live-tests.yml` and `parity-compat.yml` identical.
- **Live and parity Cargo commands use `--locked`**, so a stale lockfile fails
  the job.

## Live tests

### How live tests are gated and configured

A live test runs only when `ZINDER_TEST_LIVE=1` is set. It reads the network
from `ZINDER_NETWORK` (`zcash-regtest`, `zcash-testnet`, or `zcash-mainnet`)
and the node endpoint from the same `ZINDER_NODE__*` variables the production
binaries use (see
[Configuration](../reference/configuration.md#environment-variables)).
`ZINDER_TEST_*` variables never reach production configuration.

Each test declares the networks it supports:

- `require_live()` accepts Regtest and Testnet.
- `require_live_for(&[...])` accepts only the listed networks.
  `require_live_mainnet()` accepts only Mainnet.

A test that does not support the selected network returns early, and nextest
reports it as passed. Some tests also return early unless their own endpoints
are set:

| Variable | Enables |
| --- | --- |
| `ZINDER_NODE__INDEXER_GRPC_ADDR` | The indexer mempool tests, including the node restart test. Add `ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH`, `ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH`, and `ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH` for a mutual-TLS indexer, and `ZINDER_NODE__INDEXER_TLS__SERVER_NAME` when the certificate does not name the URL host. |
| `ZINDER_TEST_INDEXER_CONTAINER_NAME` | The container the restart test restarts. It defaults to `z3_regtest_sidecar_zebra` and needs a Docker health check. |
| `ZINDER_TEST_QUERY_GRPC_ADDR` | The standalone native `WalletQuery` smoke test against a running `zinder-query`. `live::nu7_activation` also broadcasts through it when set. |
| `ZINDER_TEST_PARITY_ZINDER_ADDR`, `ZINDER_TEST_PARITY_LIGHTWALLETD_ADDR` | The `live::parity_against_lightwalletd` tests |
| `ZINDER_TEST_ZINGOLIB_BIN`, `ZINDER_TEST_ZINGOLIB_SERVER` | The Zingolib real-client flow |
| `ZINDER_TEST_COMPACT_*` | The two `live::compact_blocks_process` tests |
| `ZINDER_TEST_CANONICAL_BLOCK_COUNT` | The block count of the Testnet or Mainnet checkpoint tracer (default 1,000) |

When you record evidence, count only the tests that exercised the node.

Tests that mutate the node, or share its indexer port, join the
`node-mutating` test group, which runs one test at a time. The group does not
stop other tests from running beside its members. Run the node restart test,
`streaming_source_recovers_after_zebra_indexer_restart`, in its own invocation
after the rest of the suite.

Name live test functions after the behavior they check. Do not put `live`,
`regtest`, `testnet`, `mainnet`, or `z3` in the name.

A scratch store from an older schema version fails admission when it is
reopened. Delete scratch stores under `.tmp/` between runs on different
commits.

### Regtest through NU7

CI runs every Regtest job twice: once against Zebra with
[`deploy/zebra/regtest.toml`](../../deploy/zebra/regtest.toml), and once
against Zakura 1.8.6 with
[`deploy/zakura/regtest.toml`](../../deploy/zakura/regtest.toml). A Regtest
node started without such a file stays on Canopy and never produces
version-5 or version-6 transactions. Both files activate the upgrades on the
same schedule:

| Upgrade | Activation height |
| --- | --- |
| Overwinter, Sapling, Blossom, Heartwood, Canopy | 1 |
| NU5 | 2 |
| NU6 | 3 |
| NU6.1 | 4 |
| NU6.2 | 5 |
| NU6.3 | 6 |
| NU7 | 140 |

Environment variables cannot set these keys, because config-rs lowercases
them and `NU6.1` contains its path separator. Mount the file.

**1. Start Zebra.** `scripts/mine-live-regtest.sh` pays the first 10 blocks to
the default Regtest unified address, checks that the Sapling, Orchard, and
Ironwood trees all hold notes at height 10, and mines the rest to the
configured transparent miner:

```bash
docker run -d --name zebra-regtest \
  -p 127.0.0.1:39232:39232 -p 127.0.0.1:39155:8155 \
  --health-cmd "curl -s -o /dev/null http://127.0.0.1:39232" \
  --health-interval 5s --health-timeout 5s --health-retries 12 \
  -v "$PWD/deploy/zebra/regtest.toml:/etc/zebrad/zebrad.toml:ro" \
  -e CONFIG_FILE_PATH=/etc/zebrad/zebrad.toml \
  -e ZEBRA_RPC__LISTEN_ADDR=0.0.0.0:39232 \
  -e ZEBRA_RPC__INDEXER_LISTEN_ADDR=0.0.0.0:8155 \
  -e ZEBRA_RPC__ENABLE_COOKIE_AUTH=false \
  -e ZEBRA_MINING__MINER_ADDRESS=tmDpFafuBHKGUYmuwLsrxWJrwcnSyzEEtYx \
  docker.io/zfnd/zebra:7.1.0@sha256:4f9536c8061e407f9f2ec81b7e0dd2f1762f81deec316abb1530dca8796a33ba
scripts/mine-live-regtest.sh http://127.0.0.1:39232 120
```

**Or start Zakura.** Zakura differs from Zebra in three ways:

- It has no `generatetoaddress`, so every block pays the configured miner.
  The node starts twice on the same state: first with the unified address in
  the TOML for 10 blocks, then with the transparent test key.
- Its coinbase cannot pay Sapling or Orchard, so only the Ironwood tree holds
  notes on this schedule.
- It refuses a plaintext indexer listener on a non-loopback address. The TOML
  binds every listener to loopback, the container runs on the host network,
  and CI serves its indexer over mutual TLS.

`scripts/generate-live-indexer-tls.sh` writes throwaway TLS material that only
your user can read, so the node runs as your user. The certificates expire
after two days. Zakura backs up its non-finalized state at most every 5
seconds and does not flush it at shutdown, so wait before stopping it:

```bash
mkdir -p .tmp/zakura-regtest-state
scripts/generate-live-indexer-tls.sh .tmp/indexer-tls
start_zakura() {
  docker run -d --name zakura-regtest --network host \
    --user "$(id -u):$(id -g)" --entrypoint /usr/local/bin/zakurad \
    --health-cmd "curl -sf http://127.0.0.1:38080/healthy" \
    --health-interval 5s --health-timeout 5s --health-retries 12 \
    -v "$PWD/deploy/zakura/regtest.toml:/etc/zakura/zakurad.toml:ro" \
    -v "$PWD/.tmp/zakura-regtest-state:/var/lib/zakura/state" \
    -v "$PWD/.tmp/indexer-tls:/etc/zakura/indexer-tls:ro" \
    -e ZAKURA_RPC__INDEXER_TLS__CERT_FILE=/etc/zakura/indexer-tls/server.pem \
    -e ZAKURA_RPC__INDEXER_TLS__KEY_FILE=/etc/zakura/indexer-tls/server-key.pem \
    -e ZAKURA_RPC__INDEXER_TLS__CLIENT_CA_FILE=/etc/zakura/indexer-tls/ca.pem \
    "$@" \
    docker.io/zakuracore/zakura:1.8.6@sha256:e514c89b751b685686690098928507aaa994311e6b90c79373c55b0afaa45d50 \
    --config /etc/zakura/zakurad.toml start
}
start_zakura
scripts/mine-live-regtest.sh http://127.0.0.1:39232 10
sleep 6
docker stop zakura-regtest && docker rm zakura-regtest
start_zakura -e ZAKURA_MINING__MINER_ADDRESS=tmDpFafuBHKGUYmuwLsrxWJrwcnSyzEEtYx
until curl -sf -o /dev/null http://127.0.0.1:38080/healthy; do sleep 1; done
scripts/mine-live-regtest.sh http://127.0.0.1:39232 120
```

The restart must keep height 10. The Zakura TOML listens on JSON-RPC `39232`,
indexer `39230`, health `38080`, P2P `38233`, and QUIC `38234`. To run beside
another node, override `ZAKURA_RPC__LISTEN_ADDR`,
`ZAKURA_RPC__INDEXER_LISTEN_ADDR`, `ZAKURA_HEALTH__LISTEN_ADDR`,
`ZAKURA_NETWORK__LISTEN_ADDR`, and `ZAKURA_NETWORK__ZAKURA__LISTEN_ADDR` on
`127.0.0.1`, and point the health check at the new port.

**2. Check the node.** Each probe must succeed before you run tests:

```bash
curl -s -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getblockcount","params":[]}' \
  http://127.0.0.1:39232 | jq '.result'
# Expect: 120

# Zebra's plaintext indexer. Expect: zebra.indexer.rpc.Indexer
grpcurl -plaintext 127.0.0.1:39155 list

# Zakura's mutual-TLS indexer. It serves no reflection, so check the handshake.
# Expect: ALPN protocol: h2, Verify return code: 0 (ok)
openssl s_client -connect 127.0.0.1:39230 -alpn h2 \
  -CAfile .tmp/indexer-tls/ca.pem -verify_return_error \
  -cert .tmp/indexer-tls/client.pem -key .tmp/indexer-tls/client-key.pem \
  </dev/null 2>&1 | grep -E 'ALPN protocol|Verify return code'
```

**3. Run the NU7 activation test first.** It needs a tip below NU7, which the
rest of the suite mines past:

```bash
export ZINDER_TEST_LIVE=1 \
  ZINDER_NETWORK=zcash-regtest \
  ZINDER_NODE__JSON_RPC_ADDR=http://127.0.0.1:39232 \
  ZINDER_NODE__AUTH__METHOD=basic \
  ZINDER_NODE__AUTH__USERNAME=zebra \
  ZINDER_NODE__AUTH__PASSWORD=zebra
cargo nextest run --workspace --profile=ci-live --run-ignored=all \
  -E 'test(/^live::nu7_activation::/)'
```

The test broadcasts a funded spend on each side of NU7 and expects a
version-4 spend after NU7 to be rejected as `UnsupportedTransactionVersion`
(ZIP 2003). It then builds a canonical store and wallet projection from
genesis and checks, against the node, the consensus branch of each spend and
coinbase, every block's tree sizes, tree states at every height, and subtree
roots. On a chain already past NU7 it logs `nu7_activation_already_reached`
and passes without checking anything.

**4. Run the rest of the suite**, then the restart test alone. Both nodes run
with cookie authentication off and ignore the `zebra:zebra` pair. nextest runs
each test binary from its crate directory, so TLS paths must be absolute.

```bash
# Zebra:
export ZINDER_NODE__INDEXER_GRPC_ADDR=http://127.0.0.1:39155
export ZINDER_TEST_INDEXER_CONTAINER_NAME=zebra-regtest

# Zakura (use instead of the two Zebra lines):
# export ZINDER_NODE__INDEXER_GRPC_ADDR=https://127.0.0.1:39230
# export ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH="$PWD/.tmp/indexer-tls/ca.pem"
# export ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH="$PWD/.tmp/indexer-tls/client.pem"
# export ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH="$PWD/.tmp/indexer-tls/client-key.pem"
# export ZINDER_TEST_INDEXER_CONTAINER_NAME=zakura-regtest

cargo nextest run --workspace --profile=ci-live --run-ignored=all \
  -E 'test(/^live::/) and not test(/^live::nu7_activation::/) and not test(mainnet) and not test(canonical_blocks_load_requested_range_from_fixed_checkpoint) and not test(streaming_source_recovers_after_zebra_indexer_restart)'
sleep 6
cargo nextest run --workspace --profile=ci-live --run-ignored=all \
  -E 'test(streaming_source_recovers_after_zebra_indexer_restart)'
```

Every Regtest test should pass. The `sleep` lets the node back up its whole
chain before the restart. Tests limited to Testnet or Mainnet pass without
running.

This run covers parsing, canonical construction, wallet projection, and
serving reads for version-5 and version-6 blocks before, at, and after NU7.
It includes a transparent-to-Ironwood shielding spend with a real Halo2 proof
(`shielded_outputs_are_indexed_after_mining`). These areas are outside this
run:

- Zakura's Sapling and Orchard pools. A captured fixture covers a Zakura
  Orchard spend; see [Capturing node fixtures](capturing-node-fixtures.md).
- Spends of shielded notes, or a completed subtree. Both nodes report empty
  subtree-root lists.
- Indexer TLS verified against system roots, or rotated certificate files.
- NU7 on Testnet or Mainnet, production capacity, reorgs deeper than the
  reorg window, and restarts of the Zinder runtimes.

Integration tests in
`services/zinder-ingest/tests/integration/node_failover.rs` and
`services/zinder-query/tests/integration/broadcast_failover.rs` cover node
endpoint failover with simulated sources, because the local Regtest nodes do
not peer with each other. `live::broadcast_fallback` needs one Regtest node: it puts a
proxy to the node at `[node]` and the node itself at `[node_fallbacks.1]`,
stops the proxy, and checks that a broadcast moves to the fallback with cause
`node_unreachable`.

The Zingolib flow cannot run on this chain. Zingolib 5.0.0 supports upgrades
only through NU6.2, and the test refuses a tip at or past NU6.3.

### Light-wallet serving qualification

Use the pinned Regtest node and mined chain from [Regtest through NU7](#regtest-through-nu7).
The qualification build serves wallet RPCs with one canonical store and ingest
control. It uses the node for transparent balances, UTXOs, address history,
transaction submission, and the recommended fee. Before forwarding a request,
it checks that the node supports transaction broadcast, agrees with the
writer's activation schedule through the next block, and holds the served tip
on its best chain. The node can advance while answering. Compare forwarded
answers with wallet serving while the node stays at the same hash and height.

The node orders UTXOs within a block by position. Qualification needs every
matching output in the last block of a wallet's request to sort them by wallet
transaction ID. A request can match more than 100,000 outputs across multiple
blocks. Qualification uses bounded prefixes to finish the last requested
height. A tip change between successful reads returns `UNAVAILABLE`.

In the synthetic cutoff fixture, one height supplies 100,000 outputs and a
later height supplies one more. The node has no ending-height filter, so
qualification returns `RESOURCE_EXHAUSTED` when it cannot establish wallet
order. On Zebra 7.1.0 and Zakura 1.8.6, each indexed output takes at least
32 bytes. Those 100,000 outputs alone exceed the 2,000,000-byte block limit.

Each qualification instance admits one nonempty UTXO request and one
address-history request at a time. `GetTaddressTxids` and
`GetTaddressTransactions` share the history slot. Each request holds its slot
while it reads from the node and while the client drains the stream. A request
for an occupied slot receives `RESOURCE_EXHAUSTED` and can retry with backoff.

Build the two qualification binaries:

```bash
cargo build --locked -p zinder-ingest -p zinder-compat-lightwalletd \
  --features zinder-ingest/light-wallet-qualification,zinder-compat-lightwalletd/light-wallet-qualification
```

In a shell with the Regtest node authentication from the preceding section,
start ingest with a fresh store. The example uses the default ingest-control
listener on port 9100. Set the same storage policy on both processes.

```bash
unset ZINDER_NETWORK
export ZINDER_NODE__JSON_RPC_ADDR=http://127.0.0.1:39232
export ZINDER_STORAGE__RAW_BLOB_POLICY=none
mkdir -p .tmp
ZINDER_INGEST__RUN_OVERRIDES__COVERAGE=wallet-serving \
  target/debug/zinder-ingest --network zcash-regtest \
  --storage-path .tmp/qualification-canonical >.tmp/qualification-ingest.log 2>&1 &
```

Once ingest reaches the mined tip and its `/readyz` on port 9105 returns 200,
start compatibility from the same shell:

```bash
target/debug/zinder-compat-lightwalletd --network zcash-regtest \
  --serving light-wallet-qualification --listen-addr 127.0.0.1:9089 \
  --canonical-primary-path .tmp/qualification-canonical \
  --canonical-secondary-root .tmp/qualification-secondary
```

The canonical reader serves compact blocks, nullifiers, tree states, and subtree
roots. A subtree-root stream returns roots completed by its starting tip.
It can continue across an append and returns `UNAVAILABLE` if a later page
finds that tip has left the best chain. A writer halt observed by ingest control
closes admission until the publisher verifies the recovered writer's fence.

Both mempool methods use ingest control. If an address-history lookup
exceeds a row or byte limit, qualification splits it into disjoint height ranges.
Before looking up transaction IDs, the stream records the hash at the end of
the requested range. Each page is read in order. For each transaction, one
verbose node call obtains its bytes and mined height, then a block-hash
lookup checks the range before the wallet receives the row. The stream also
checks the hash for empty pages and before reporting completion. A replacement
returns `UNAVAILABLE`, and cancelling the stream stops further calls. If one
height exceeds the response limit, the stream ends with `RESOURCE_EXHAUSTED`.
With `none`, `GetTransaction` fetches the bytes from
the node. To exercise local mined-transaction reads, repeat with fresh store
paths and `ZINDER_STORAGE__RAW_BLOB_POLICY=transactions`. Neither run needs a
projector or wallet store. The feature is for qualification only. Release
builds accept `wallet` and `compact-blocks`.

### Testnet

Testnet uses the same command with a cookie-authenticated node. This example
reads the cookie from a Z3 stack's shared volume:

```bash
cookie=$(docker run --rm -v z3-testnet-cookie:/auth:ro alpine cat /auth/.cookie)
ZINDER_TEST_LIVE=1 \
  ZINDER_NETWORK=zcash-testnet \
  ZINDER_NODE__JSON_RPC_ADDR=http://127.0.0.1:18232 \
  ZINDER_NODE__INDEXER_GRPC_ADDR=http://127.0.0.1:18155 \
  ZINDER_NODE__AUTH__METHOD=basic \
  ZINDER_NODE__AUTH__USERNAME=${cookie%%:*} \
  ZINDER_NODE__AUTH__PASSWORD=${cookie#*:} \
  cargo nextest run --profile=ci-live --run-ignored=all
```

Tests that mine or reorg the chain are limited to Regtest and pass without
running. Set `ZINDER_TEST_QUERY_GRPC_ADDR` to also run the native
`WalletQuery` smoke against a running `zinder-query`.

Two tests matter mostly on Testnet and Mainnet:

- `latest_completed_subtree_roots_derive_from_their_completing_blocks` derives
  each pool's latest completed subtree root from the completing block and
  compares it byte for byte with `z_getsubtreesbyindex`. Run it against both
  certified nodes when either node pin or the commitment-tree code changes. It
  fails on a node where no pool has completed a subtree.
- `canonical_blocks_load_requested_range_from_fixed_checkpoint` loads a fixed
  range (1,000 blocks by default) into a canonical store and verifies the
  written files. It leaves the store in its building state, so it does not
  prove readiness. The live profile allows five minutes. The weekly Testnet
  lane skips this tracer, and the manually dispatched Mainnet lane includes
  it. You can also run it locally on Testnet.

To run that tracer in Docker beside a Z3 Testnet node, without touching the
node's chain volume:

```bash
docker build -f deploy/Dockerfile --target zinder-ingest-live-tests \
  -t zinder-ingest-live-tests:local .
docker compose -p zinder-canonical-live-test \
  -f deploy/docker-compose.canonical-live-test.yml run --rm canonical-blocks
docker compose -p zinder-canonical-live-test \
  -f deploy/docker-compose.canonical-live-test.yml down -v
```

### Mainnet

Mainnet tests run only against a synced node you operate. A test runs on
Mainnet only if it opts in with `require_live_mainnet()` or
`require_live_for(&[Network::ZcashMainnet, ...])`. Every other test passes
without running.

```bash
cookie=$(docker run --rm -v z3-mainnet-cookie:/auth:ro alpine cat /auth/.cookie)
ZINDER_TEST_LIVE=1 \
  ZINDER_NETWORK=zcash-mainnet \
  ZINDER_NODE__JSON_RPC_ADDR=http://127.0.0.1:8232 \
  ZINDER_NODE__AUTH__METHOD=basic \
  ZINDER_NODE__AUTH__USERNAME=${cookie%%:*} \
  ZINDER_NODE__AUTH__PASSWORD=${cookie#*:} \
  cargo nextest run --profile=ci-live --run-ignored=all
```

The Mainnet-only tests are
`fetch_chain_checkpoint_returns_advancing_tree_sizes_on_mainnet` and
`tip_id_advances_above_one_million`. No live test writes to Mainnet.

### Parity against a reference lightwalletd

These tests compare `zinder-compat-lightwalletd` with the pinned
`electriccoinco/lightwalletd` v0.6.0, both reading the same Regtest node. CI
mines the [Regtest through NU7](#regtest-through-nu7) chain to height 150, so
the compared range crosses NU7. The tests compare only the fields that the
[lightwalletd compatibility reference](../reference/lightwalletd-compatibility.md)
names as comparable; build metadata, version strings, and the donation
address are expected to differ.

Start a fresh node as in step 1 of [Regtest through NU7](#regtest-through-nu7),
but mine to 150 instead of 120. Then start the reference. Leave
`--ping-very-insecure` off, because the `Ping` test expects both servers to
refuse `Ping`.

```bash
scripts/mine-live-regtest.sh http://127.0.0.1:39232 150
docker run -d --name lightwalletd-reference --network host \
  docker.io/electriccoinco/lightwalletd:v0.6.2@sha256:75e5742df686094556abef66e69c37bc114de23a0706712aee324f332ca52951 \
  --grpc-bind-addr 0.0.0.0:9088 \
  --rpchost 127.0.0.1 --rpcport 39232 --rpcuser zebra --rpcpassword zebra \
  --no-tls-very-insecure --data-dir /tmp/lwd --log-file /dev/stdout
```

Next, start a fixed-height Zinder compatibility server on port 9087. Ingest
builds the canonical store, stops while `zinder-bench` builds the wallet store
from it, and restarts so the compatibility server can authenticate against a
live writer. Every process that opens the canonical store must state the same
raw-blob retention, here `all`. The binaries take the network from
`--network`, so unset the test-only `ZINDER_NETWORK` first:

```bash
cargo build --locked --release -p zinder-ingest -p zinder-compat-lightwalletd -p zinder-bench
unset ZINDER_NETWORK
export ZINDER_NODE__JSON_RPC_ADDR=http://127.0.0.1:39232 \
  ZINDER_NODE__AUTH__METHOD=basic \
  ZINDER_NODE__AUTH__USERNAME=zebra \
  ZINDER_NODE__AUTH__PASSWORD=zebra
mkdir -p .tmp
start_ingest() {
  ZINDER_STORAGE__RAW_BLOB_POLICY=all ./target/release/zinder-ingest --network zcash-regtest \
    --storage-path .tmp/parity-store > .tmp/ingest.log 2>&1 &
  ingest_pid=$!
  until grep -qE ' visible_tip_height=150( |$)' .tmp/ingest.log; do sleep 2; done
}
start_ingest
kill "$ingest_pid"
wait "$ingest_pid" || true
./target/release/zinder-bench rocksdb-wallet-rebuild \
  --network zcash-regtest --json-rpc-addr http://127.0.0.1:39232 \
  --raw-blob-retention all \
  --canonical-store .tmp/parity-store \
  --wallet-store .tmp/parity-wallet-store > .tmp/wallet-rebuild.json
start_ingest
ZINDER_STORAGE__RAW_BLOB_POLICY=all ./target/release/zinder-compat-lightwalletd --network zcash-regtest \
  --canonical-primary-path .tmp/parity-store \
  --canonical-secondary-root .tmp/parity-secondary-compat-canonical \
  --wallet-primary-path .tmp/parity-wallet-store \
  --wallet-secondary-root .tmp/parity-secondary-compat-wallet \
  --listen-addr 127.0.0.1:9087 \
  --node-json-rpc-addr http://127.0.0.1:39232 > .tmp/compat.log 2>&1 &
until curl -sf -o /dev/null http://127.0.0.1:9107/readyz; do sleep 1; done
```

Then run the parity tests:

```bash
ZINDER_TEST_LIVE=1 \
  ZINDER_NETWORK=zcash-regtest \
  ZINDER_TEST_PARITY_ZINDER_ADDR=http://127.0.0.1:9087 \
  ZINDER_TEST_PARITY_LIGHTWALLETD_ADDR=http://127.0.0.1:9088 \
  ZINDER_TEST_PARITY_TRANSPARENT_ADDRESS=tmDpFafuBHKGUYmuwLsrxWJrwcnSyzEEtYx \
  cargo nextest run -p zinder-compat-lightwalletd --profile=ci-live --run-ignored=all \
    -E 'test(/^live::parity_against_lightwalletd::/) and not test(/mempool/)'
```

CI excludes the mempool parity tests, because this fixed-height setup has no
live mempool composition.

## Specialized procedures

### Reorg sweep

`live::reorg_sweep` starts the production canonical writer, invalidates the
top three blocks on the Regtest node with `invalidateblock`, and checks that
the writer's `IngestControl.VisibleChainEvents` stream publishes a reorg
covering them. It runs in the Regtest suite. To run it alone, with the
Regtest variables from step 3 exported:

```bash
cargo nextest run --profile=ci-live --run-ignored=all -E 'test(/^live::reorg_sweep::/)'
```

Never run a forced reorg against a shared network. The test refuses any
network but Regtest.

### Network-upgrade boundary

`live::network_upgrade_boundary` checks the coinbase consensus branch ID at
the latest reachable activation height and one block on either side. On the
[Regtest through NU7](#regtest-through-nu7) chain it samples heights 139, 140,
and 141. On a node that activates every upgrade at height 1 there is no
boundary, and the test returns early. The reason for reading the schedule
from the node is in
[ADR-0008](../adrs/0008-consensus-parameters-from-the-node.md).

```bash
cargo nextest run --profile=ci-live --run-ignored=all -E 'test(/^live::network_upgrade_boundary::/)'
```

### Writer fencing and crash recovery

Only one process may write each store; readers open RocksDB secondaries (see
[ADR-0003](../adrs/0003-epoch-bound-reads-and-storage-roles.md)). The
canonical store tests in
`crates/zinder-store/src/canonical_store/secondary_tests.rs` run in the `ci`
profile and prove that a second primary is refused, that a secondary keeps
serving after the primary stops, and that a restarted primary resumes from
the same fence.

When you change a storage commit path, also reproduce it by hand with a real
process. Export the `ZINDER_NODE__*` variables from step 3 first, but not
`ZINDER_NETWORK`; the binary takes the network from `--network`.

```bash
cargo build --release -p zinder-ingest
mkdir -p .tmp
rm -rf .tmp/regtest.zinder-store
./target/release/zinder-ingest --network zcash-regtest \
  --storage-path .tmp/regtest.zinder-store > .tmp/ingest.log 2>&1 &
ingest_pid=$!
# Wait until the writer follows the tip, then kill it without warning.
until grep -q ' visible_tip_height=' .tmp/ingest.log; do sleep 2; done
kill -9 "$ingest_pid"
wait "$ingest_pid" 2>/dev/null || true
./target/release/zinder-ingest --network zcash-regtest \
  --storage-path .tmp/regtest.zinder-store probe
```

`probe` prints a JSON line with a numeric `store_tip` and exits zero. Kill the
writer only after it follows the tip. A kill during construction discards the
unpublished store, and `probe` then refuses to open it.

To check fencing across processes, start a second writer while the first is
running. Disable its ops endpoint, so it cannot fail on the port instead:

```bash
./target/release/zinder-ingest --network zcash-regtest \
  --storage-path .tmp/regtest.zinder-store > .tmp/ingest.log 2>&1 &
first_pid=$!
sleep 5
ZINDER_OPS__LISTEN_ADDR= ./target/release/zinder-ingest --network zcash-regtest \
  --storage-path .tmp/regtest.zinder-store
kill "$first_pid"
```

The second process must log `ingest_run_failed`, with a
`canonical store RocksDB open ready failed` cause, and exit non-zero. The first
keeps running.

### Concurrent readers

`services/zinder-query/tests/integration/stream_cancellation.rs` runs in the
`ci` profile. It checks that a cancelled stream does not break later
requests, and that 16 parallel readers each drain a full range within 20
seconds.

To load a real compatibility server, start one as in
[Parity against a reference lightwalletd](#parity-against-a-reference-lightwalletd)
and fan out range requests:

```bash
for reader_index in $(seq 1 10); do
  grpcurl -plaintext -d '{"start":{"height":1},"end":{"height":100}}' \
    127.0.0.1:9087 cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlockRange \
    > ".tmp/reader-$reader_index.out" &
done
wait
wc -l .tmp/reader-*.out
```

Every reader must receive blocks without a transport error, and none may hang
after the others finish.

### Performance

| Question | Run | Result |
| --- | --- | --- |
| Did a read path get slower? | `cargo nextest run --profile=ci-perf` | Fails when `services/zinder-query/tests/perf/perf_smoke.rs` exceeds its budgets: 250 ms for the visible tip, 2 s for a compact block range, and 5 s for a full block range of 1,000 blocks. |
| How fast are reads against a real node? | `read_endpoint_latency_baseline` on Regtest, with `--no-capture` | Prints one `live_latency_baseline` line with microsecond timings. Nothing is asserted; compare the line across changes. |
| What does each block cost the writer and projector at the tip? | `zinder-bench rocksdb-tip-replay` | Follow [section 4 of the zinder-bench README](../../services/zinder-bench/README.md#4-measure-per-block-cost-at-the-tip). |
| Does a deep reorg fit the wallet transition limit? | `zinder-bench rocksdb-tip-reorg` | Follow [section 5 of the zinder-bench README](../../services/zinder-bench/README.md#5-measure-a-deep-reorg-at-the-tip). |
| How does a wallet endpoint hold up while many wallets sync? | `zinder-bench lightwalletd-wallet-load` | Follow [section 7 of the zinder-bench README](../../services/zinder-bench/README.md#7-drive-wallet-shaped-load-at-an-endpoint). |
| How long, and how much memory and disk, does a full build take? | `scripts/run-rocksdb-storage-lifecycle.sh` | Follow [RocksDB storage lifecycle](../../deploy/rocksdb-storage-lifecycle.md). |

#### Run a paired benchmark campaign

Use `scripts/run-benchmark-campaign.py` to run the same cases against two
prepared targets. It runs the existing benchmark commands in alternating
before/after order and saves their raw reports. Build the clients first, and
start the targets with equivalent data, resource limits, and cache preparation.
Pin the workload to the same network, dataset, and height window. Record each
server's revision or image digest, serving settings, and cache preparation in
the campaign. The runner records client affinity and executable hashes.

For example, save this campaign as `wallet-campaign.json`. Set the binary
paths, endpoint addresses, revisions, and height window for your targets. This
example assumes both Testnet endpoints serve the same history through height
4,600,000. The two cases measure closed-loop block streaming and a classic SDK
sync. Both use four clients.

```json
{
  "environment": {"network": "zcash-testnet", "transport": "loopback h2c"},
  "arms": {
    "before": {"endpoint": "http://127.0.0.1:9067", "cbb_endpoint": "before=http://127.0.0.1:9067", "revision": "BASELINE_SHA"},
    "after": {"endpoint": "http://127.0.0.1:9167", "cbb_endpoint": "after=http://127.0.0.1:9167", "revision": "CANDIDATE_SHA"}
  },
  "cases": {
    "cbb-range": {
      "command": ["cbb", "--endpoint", "{cbb_endpoint}", "--concurrency", "4", "--steps", "10", "--chunk", "100", "--start-height", "4598000", "--tree-every", "0", "--warmup", "0", "--json"],
      "expect": {"/0/total_requests": 40, "/0/errors": 0}
    },
    "classic-sdk": {
      "command": ["zinder-bench", "lightwalletd-wallet-load", "--endpoint", "{endpoint}", "--workload", "classic-sdk", "--wallets", "4", "--from-height", "4580011", "--to-height", "4600000", "--report", "{report}"],
      "expect": {"/wallets_completed": 4, "/wallets_failed": 0, "/incomplete_streams": 0},
      "match": ["/server/chain_name", "/from_height", "/to_height", "/blocks_received", "/bytes_received"]
    }
  }
}
```

```sh
python3 scripts/run-benchmark-campaign.py wallet-campaign.json \
  --output .tmp/wallet-campaign
```

Each command runs in your current directory. A whole argument such as
`{endpoint}` takes its value from the current arm. Use `{report}` for tools
that write their own JSON file. Otherwise the runner reads JSON from standard
output. Pass `{scratch}` when the tool needs an unused path where it can
create a scratch or secondary directory for each run. `expect` checks values
against a known workload contract. `match` checks that values agree across
the two reports in each pair. Both use JSON pointers. For a store case, check
the fixture digest, read range, and output digest. For wallet comparisons
where the expected payload differs, use block counts and the window for
matching and compare payload sizes in the raw reports.

Add fixture manifests or an explicit CBB tree-height list to the optional
`inputs` array to record their hashes. Each case defaults to four repetitions
in `AB`, `BA`, `BA`, `AB` order. Set `repeats` or `timeout_seconds` at the top
level to override the defaults of four repetitions and 600 seconds per command.
Use credentials configured outside the campaign because its arguments and
reports are saved verbatim.

Read `summary.json` for failed commands, missing reports, and mismatched
fields. Failures preserve their logs and do not stop later pairs. Compare
per-method latency, errors, and work completed from the raw reports. Interpret
RPC, storage, and wallet scanner results in their own units. Use the
dedicated [storage campaign validator](../../deploy/storage-benchmark.md)
for the PostgreSQL qualification contract.

### Canonical runtime tracer

This runs the real `zinder-ingest` image beside an existing Zebra Testnet
node, in a 10-CPU, 10 GiB container with its own disposable volume. It mounts
only the node's cookie, never its chain volume.

```bash
docker build -f deploy/Dockerfile --target zinder-ingest \
  -t zinder-ingest:canonical-runtime-local .
image_id=$(docker image inspect zinder-ingest:canonical-runtime-local --format '{{.Id}}')
ZINDER_CANONICAL_RUNTIME_IMAGE="$image_id" \
ZINDER_CANONICAL_RUNTIME_CHECKPOINT_HEIGHT="${checkpoint_height:?set a height below the node tip}" \
  docker compose -f deploy/docker-compose.canonical-runtime-test.yml up -d zinder-ingest
```

Set `checkpoint_height` to a height below the node's tip first. Poll `/readyz` and the
`zinder_ingest_canonical_*` metrics. The store must publish, then advance its
tip, epoch, and event sequence together as the node mines. After a restart it
must reopen at the same fence. If you disconnect only the ingest container
from the node's network, readiness must report `node_unavailable` without
changing the fence, and following must resume when you reconnect it.

### External wallets

`services/zinder-compat-lightwalletd/tests/integration/lightwalletd_scan_contract.rs`
runs in the `ci` profile. It scans a populated store through the generated
`CompactTxStreamer` client and checks transaction alignment, tree sizes, and
that no key material reaches the server.

Before you claim support for a real lightwalletd-compatible wallet, deploy a
Testnet stack as in [Deploying Zinder](deploying.md), expose it as in
[Exposing Zinder over TLS](exposing-over-tls.md), and point the wallet at the
compatibility endpoint. Check each of these with every runtime's `/readyz`
ready:

1. A new wallet syncs to the advertised tip.
2. A restored wallet syncs. Tree-state and subtree-root requests at or above
   the store floor succeed.
3. `GetAddressUtxosStream` returns the wallet's transparent outputs.
4. A sent transaction appears in `GetMempoolStream`, then in
   `GetTransaction` after it is mined.

#### Measure a Zkool restore on a fixed fixture

Build the Zkool evaluator and prepare a loopback lightwalletd fixture using
the [pinned zcash-sync-evals workflow](https://github.com/craftsoldier/zcash-sync-evals/blob/f0928f9fcc2292ea6be6b406d67355108aa61177/.github/workflows/bench.yml).
The workflow records the Zkool revision, corpus checksum, and served height
for each window. The evaluator uses a public test seed and the Mainnet
consensus rules for the frozen corpus.

For the Sapling fixture, run the evaluator against the server capped at
height 1,499,999. Use a scratch path that does not exist and whose parent does.

```sh
python3 scripts/run-zkool-restore.py \
  --binary /path/to/zkool_sync_eval \
  --endpoint http://127.0.0.1:9067 \
  --birthday 1300000 \
  --expected-tip 1499999 \
  --scratch .tmp/zkool-restore
```

The command saves the wallet database and evaluator logs under the scratch
path. It prints JSON only after the evaluator reports the expected tip.
`sync_seconds` measures the restore and synchronization, with database
reporting outside the timed interval. The JSON records the evaluator SHA-256,
which you can require with `--expected-sha256`. When comparing endpoints,
verify that the final height, balances, note count, and history count agree
before comparing timings. Record the evaluator revision and corpus checksum
alongside the reports. This Sapling window exercises a restore with zero
wallet activity.

## Failure interpretation

| Message or symptom | Cause | Fix |
| --- | --- | --- |
| `set ZINDER_TEST_LIVE=1 plus ZINDER_NETWORK and ZINDER_NODE__* env vars to run live tests` | The live gate is not set. | Export the variables. |
| `resolving node target from environment` | A `ZINDER_NODE__*` value is missing or invalid. | Fix it as in [Configuration](../reference/configuration.md#environment-variables). |
| A live test passes in milliseconds | It returned early: the selected network is not one it supports, or its endpoint variable is unset. | Check [How live tests are gated and configured](#how-live-tests-are-gated-and-configured). |
| `connection refused` or `transport error` | The node is not running, or a port is wrong. | Run the probes in step 2. |
| `canonical store admission refused` with `store schema version` | A scratch store from an older schema. | Delete the scratch store. |
| `persisted network ... does not equal requested network` | A store reused across networks, or the wrong `ZINDER_NETWORK`. | Use a separate store per network. |
| `canonical store RocksDB open ready failed` | Another process holds the store's primary lock. | Stop the other writer. |
| `nu7_activation_already_reached` in the log | The chain is already past NU7. | Start a fresh node. |
| The restart test fails, or other tests fail during it | It ran alongside other live tests. | Run it alone, last, after a 6-second wait. |

## Native capability list

`testing_runbook_capability_list_mirrors_zinder_capabilities` in
`crates/zinder-proto/tests/integration/capability_docs.rs` checks that this
list equals the wallet and explorer rows of the
[`CAPABILITIES`](../../crates/zinder-proto/src/capabilities.rs) table. The
[capabilities reference](../reference/capabilities.md) explains each string.

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
wallet.read.chain_value_pools_at_tip_v1
wallet.address.transparent_unspent_outputs_v1
wallet.address.transparent_history_v1
wallet.read.transparent_outpoint_spend_status_v1
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

## Runbook self-test

`scripts/runbook-lint.sh docs/runbooks/testing.md` parses every fenced `bash`
block in this file with `bash -n`. It is part of the default gate and the
`dependency hygiene` CI job. It checks syntax only; it does not run the
blocks. Run it after you edit this file, and confirm it reports a non-zero
block count.

The tests in `crates/zinder-proto/tests/integration/capability_docs.rs` also
check this file on every `ci` run: the quoted `default-filter` must match
`.config/nextest.toml`, every profile in `RUNBOOK_REFERENCED_PROFILES` must
exist and appear here, and the capability list must match the code.
