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

Append `--list` to the mutation command to inspect the selected functions and
mutations. CI splits them across three shards with a 90-minute timeout per
shard. A GitHub-hosted run took 56 minutes for 130 mutants with `--jobs 3`.
If a shard approaches the timeout, increase the workflow matrix and the
`--shard` denominator together. Use `--shard 0/3` to run one shard locally.

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
  --file crates/zinder-store/src/canonical_store/block_load/codec.rs \
  --file crates/zinder-store/src/canonical_store/reader.rs \
  --file crates/zinder-store/src/canonical_store/served_tree_state.rs \
  --file crates/zinder-store/src/canonical_store/tree_state_replay.rs \
  --file crates/zinder-store/src/canonical_store/tree_state_densification.rs \
  --file crates/zinder-core/src/network_upgrade_activations.rs \
  --file crates/zinder-source/src/source_block.rs \
  --file services/zinder-compat-lightwalletd/src/sealed_history.rs \
  --file services/zinder-compat-lightwalletd/src/sealed_history/index.rs \
  --file services/zinder-ingest/src/artifact_builder.rs \
  --re 'sealed_history/index\.rs:.*\b(hash_frames|admit|decode_index)\b' \
  --re 'SegmentIdentity::check_block|validate_lengths|verify_payload|canonical_event_history|validate_live_settled_tip|validate_live_block_extension|validate_replacement_blocks|verify_auth_tag|from_raw_block_bytes|compact_sapling_outputs|compact_orchard_actions|compact_note_ciphertext_prefix|matching_fingerprint_prefix_len|preserves_through|next_activation_schedule|validate_activation_expectation|read_bound|validate_open_store_control|raise_reorg_policy|decode_reorg_policy_trailer|read_stored_final_note_commitment_roots_in_range|decode_stored_tree_state_roots|decode_checked_stored_roots|resolve_stored_roots_headers|framed_tree_size|skip_framed_node|read_tree_state_checkpoint_at|lookup_tree_state|find_tree_state|replay_tree_state|refuse_unadmitted_activations|reconstruct|required_seed_height|replay_compact_blocks|append_compact_block_commitments|requires_checkpoint|after_seed|scan_tree_state_strides|stride_ending_at_satisfied|next_tree_state_stride|first_seed_at_or_above|stride_satisfies|stored_heights_through_end|stored_tree_sizes|densify_tree_state_stride|replay_stride|append_stored_block|commitment_count|recorded_tree_sizes|commit_tree_state_stride|StoredStrideRows::read|StoredStrideRows::at|ReplayedTrees::append_block|ReplayedTrees::canonical_block_time|ReplayedTrees::checkpoint'
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
The [light-wallet profile](../reference/configuration.md#light-wallet-profile) serves wallet RPCs with one canonical store and ingest
control. Both `compact-blocks` and `light-wallet` check the
writer's canonical fence when admitting a reader. Qualification also requires
the writer to attest the reader's upgrade schedule. It uses the node for
transparent balances, UTXOs, address history, transaction submission, and the
recommended fee. Each forwarded read checks that the node holds, on its best
chain, the tip of the reader the request captured. A broadcast checks the
currently served reader instead, and returns `UNAVAILABLE` if the canonical
fence or schedule revision changes during verification.
Requests share evidence of broadcast support and agreement with the writer's
schedule through the next block for up to one minute. A failed tip check, a
new schedule revision, or a change to the upgrade active at the next block
requires fresh discovery. If discovery fails, concurrent callers waiting for it
return the refusal or a retryable error. The next request can retry immediately.

Forwarded transaction, balance, and UTXO answers describe the served tip
captured for the request, or the call fails. Address history is bounded to
that tip but is best-effort, as the next paragraph explains. A node on another
chain fails the tip check.
The node answers UTXO lookups at its own tip and reports that tip, so
qualification keeps an answer only when every page names the served tip. The
balance is the sum of the unspent outputs the node reports at the served tip,
because `getaddressbalance` does not report the tip it answered at. While the
node is ahead of the served tip, balance and UTXO calls fail, and a retry
succeeds once ingest serves the newer blocks. A balance over more than
100,000 unspent outputs returns `RESOURCE_EXHAUSTED`. Each reread of the last
height of a bounded UTXO request must repeat, in order, every output an
earlier read returned at that height, and must not name an output an earlier
read placed below it. Otherwise the call returns `UNAVAILABLE`.

Forwarded address history is best-effort and bounded to the served tip, not
an exact history. Two limits remain. First, neither node reports the chain a
`getaddresstxids` answer describes. Qualification reads the node's hash at
the served tip's height immediately before and after each request, and keeps
the answer only when both reads name the served tip. Each row must be a
transaction the served chain mines, in the block the node reports, after the
previous row in canonical position order, and its bytes must match the digest
ingest recorded. A history answer can still omit transactions if the node
leaves the served chain and returns to it during a single `getaddresstxids`
request, between the two hash reads. Second, the answer relies on the node's
address index being correct: the position and byte checks prove where each
returned transaction was mined, not that the list is complete for the
address. An exact history needs either a local address index, as wallet
serving keeps, or a node reply that binds the list to a chain tip, which
[zebra#12575](https://github.com/ZcashFoundation/zebra/issues/12575) requests.

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
address-history request at a time. Balance requests read the node's UTXOs and
share the UTXO slot. `GetTaddressTxids` and
`GetTaddressTransactions` share the history slot. Each request holds its slot
while it reads from the node and while the client drains the stream. A request
for an occupied slot receives `RESOURCE_EXHAUSTED` and can retry with backoff.

Build both standard binaries with Cargo.

```bash
cargo build --locked -p zinder-ingest -p zinder-compat-lightwalletd
```

In a shell with the Regtest node authentication from the preceding section,
start ingest with a fresh store. The example uses the default ingest-control
listener on port 9100.

```bash
unset ZINDER_NETWORK
export ZINDER_NODE__JSON_RPC_ADDR=http://127.0.0.1:39232
export ZINDER_PROFILE=light-wallet
mkdir -p .tmp
target/debug/zinder-ingest --network zcash-regtest \
  --storage-path .tmp/qualification-canonical >.tmp/qualification-ingest.log 2>&1 &
```

Once ingest reaches the mined tip and its `/readyz` on port 9105 returns 200,
start compatibility from the same shell:

```bash
target/debug/zinder-compat-lightwalletd --network zcash-regtest \
  --listen-addr 127.0.0.1:9089 \
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
Each page is read in order. For each transaction, one verbose node call
obtains its bytes and mined block, then a block-hash lookup checks that the
node still holds the served tip before the wallet receives the row. The stream
also checks the served tip before reporting completion. A replacement returns
`UNAVAILABLE`, bytes that differ from the recorded digest return `DATA_LOSS`,
and cancelling the stream stops further calls. If one
height exceeds the response limit, the stream ends with `RESOURCE_EXHAUSTED`.
With `none`, `GetTransaction` finds the transaction's block in the canonical
store and fetches only its bytes from the node. The node must report the same
block, and the bytes must match the digest ingest recorded for that
transaction, which every blob-retention policy keeps. Other bytes return
`DATA_LOSS`. A transaction the served chain does not mine is looked up in the
ingest mempool, as wallet serving does, even when the node has mined it in a
later block. To exercise local mined-transaction reads, repeat with fresh store
paths. Unset `ZINDER_PROFILE`, set `ZINDER_STORAGE__RAW_BLOB_POLICY=transactions`
for both binaries, set `ZINDER_INGEST__RUN_OVERRIDES__COVERAGE=wallet-serving`
for ingest, and pass `--serving light-wallet` to compatibility.

Use `GetAddressUtxosStream` when a wallet may receive a large set. The
generated Rust client's default unary decoding limit is 4 MiB. The
100,000-output transport fixture produces a 10,748,600-byte unary response.
The default client streams all rows, and a client configured with a 16 MiB
decoding limit also receives the complete unary response.

The parity workflow compares the light-wallet composition with wallet serving on
both pinned nodes. To repeat it, use the workflow's disposable height-150 chain
and its wallet-serving process on port 9087. The harness calls
`getblockchaininfo` without credentials at the host's loopback endpoint and at
`127.0.0.1:39232` inside the node container. Both compatibility processes use
`ZINDER_COMPAT__STALE_TIP_LIMIT_MS=0` because the fixture has historical
block timestamps and the outage removes the node witness for its tip.
With the Regtest node authentication exported and `ZINDER_PROFILE` unset, run
the following in place of the two manual processes
above. The harness creates fresh stores for both policies. It submits one
signed spend and compares pending `GetTransaction`, `GetMempoolTx`, and
`GetMempoolStream` on the ordinary wallet endpoint and both qualification
instances. It stops the named node for the outage checks and restarts it on exit.
Use `zakura-regtest` for the Zakura lane.

```bash
ZINDER_NODE__JSON_RPC_ADDR=http://127.0.0.1:39232 \
  scripts/test-light-wallet-qualification.sh target/debug \
  zebra-regtest http://127.0.0.1:9087
```

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
matching and compare payload sizes in the raw reports. To check the same
value in every case and both arms, map report pointers to keys from the
campaign’s `environment` object with `expect_from_environment`.

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

#### Compare first-block and whole-wallet latency

The [Slipstream and Vizor campaign](../../scripts/compact-range-campaign.example.json)
runs 16 simulated wallets per endpoint over Testnet heights 4,624,102 through
4,644,091. The benchmark reads the tip before and after the wallet workload.
Each observation must match the height and hash pinned in `environment`.
For a stacked change, build the baseline from the preceding PR head and the
candidate from the new head. Bind both listeners to the same four physical
cores and use separate cores for the client and writers.
Hold the store contents, serving settings (including the 256 KiB opening),
and build flags constant. Run each profile once on each endpoint to warm
the caches, and exclude those runs from the comparison.
Copy the example to `compact-range-campaign.json` and record the server
revisions, binary hashes, fixture identity, and CPU sets in its `environment`
object. Use the same `zinder-bench` binary with request-identity reporting
for both endpoints. Put it on `PATH` and run the command on
your reserved client cores (8–11 here).

```sh
taskset -c 8-11 python3 scripts/run-benchmark-campaign.py compact-range-campaign.json \
  --output .tmp/compact-range
```

A successful run has eight pairs per profile, 319,840 blocks per endpoint
in each pair, and matching delivered bytes. Both reports must also have the
same `wallet_request_identity_sha256`. Each wallet contributes the ranges,
transaction IDs, and tree states it requested, including repeated attempts.
Wallets can run in any order. Moving a request between wallets can change
their individual completion times, so the comparison checks how the work
was distributed. Slipstream
connection counts can vary between runs and must agree within each pair.
After the runner exits successfully,
calculate each pair's change in `GetBlockRange` first-message p99 and whole-wallet sync p99 separately.
A positive difference means the candidate took longer.

```python
import json
from collections import defaultdict
from pathlib import Path
from statistics import median

output = Path(".tmp/compact-range")


def read_report(run):
    return json.loads((output / run["path"] / "report.json").read_text())


changes = defaultdict(list)
for pair in json.loads((output / "summary.json").read_text()):
    before = read_report(pair["runs"]["baseline"])
    after = read_report(pair["runs"]["candidate"])
    first = [r["rpcs"]["GetBlockRange"]["first_message_seconds"]["p99"] for r in (before, after)]
    sync = [r["wallet_sync_seconds"]["p99"] for r in (before, after)]
    for metric, (old, new) in (("range first", first), ("wallet sync", sync)):
        changes[pair["case"], metric].append(1000 * (new - old))
for (profile, metric), samples in changes.items():
    print(f"{profile} {metric}: {median(samples):+.2f} ms")
```

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

## Disposable wallet Regtest fixtures

Select the fixture using the wallet binary's compiled or configured activation
heights. The node and the wallet must interpret the same block under the same
consensus rules.

| Wallet integration exercised | Zebra configuration under `deploy/zebra/` | Serving interface |
| --- | --- | --- |
| Zallet native backend | `regtest.toml` | Native WalletQuery with `--raw-blob-policy all` for full-block reads |
| Zally native SDK | `regtest.toml` | Native WalletQuery |
| Zingo CLI and zcash-devtool with the all-at-one schedule | `regtest-nu6-at-one.toml` | lightwalletd |
| Zkool v6.37.0, the Cake Wallet v6.5.4 engine fork, and Vizor v0.0.65 configured for NU6.3 at 250 | `regtest-ironwood-at-250.toml` | lightwalletd |

Give independent miners distinct fixture projects. After mining, wait until the
serving endpoint returns the target block before asserting what the wallet has
scanned. Rapid Regtest mining can fill the serving process's retained snapshots
and delay publication even after ingest and projection have reached the target.
Record the target height, the time it becomes readable, and the height the
wallet actually scans. When a wallet needs another confirmation, mine that
block explicitly and keep the original target in the assertion.


Use `scripts/regtest-fixture.py` to give a real wallet a fresh, owned node and
following single-host ingest, projector, native WalletQuery, and lightwalletd
endpoints. It uses `deploy/docker-compose.yml` and its configs. A local Linux
Docker host with bridge addresses reachable from the host, Python 3.11 or
later, Docker Compose, iproute2, curl, and jq are required. Start from the checkout matching
the candidate release. Pull the four runtime images first, or build them using
`deploy/Dockerfile` and its corresponding targets.

Create an image map with the exact release tag, digest, or built image reference
for each runtime. The command resolves references to immutable local image IDs
before starting anything:

```bash
cat > /tmp/fixture-images.json <<'JSON'
{
  "ingest": "ghcr.io/zcashfoundation/zinder-ingest:v0.21.0",
  "projector": "ghcr.io/zcashfoundation/zinder-projector:v0.21.0",
  "query": "ghcr.io/zcashfoundation/zinder-query:v0.21.0",
  "compat-lightwalletd": "ghcr.io/zcashfoundation/zinder-compat-lightwalletd:v0.21.0"
}
JSON
scripts/regtest-fixture.py up --project zinder-fixture-review-a \
  --port-base 42100 --images /tmp/fixture-images.json \
  --revision 830b426060169f5b2e94d84ff0f134bc1ad96004
scripts/regtest-fixture.py mine --project zinder-fixture-review-a --tip 152
scripts/regtest-fixture.py status --project zinder-fixture-review-a
scripts/regtest-fixture.py down --project zinder-fixture-review-a
```

Choose an unused project name and port range for each run. Ports are loopback
only: base + 32 is node RPC, + 2 is native WalletQuery, + 67 is lightwalletd,
and + 5, + 10, + 6, and + 7 are the ingest, projector, query, and compat ops
listeners. Each project owns distinct internal Compose networks and volumes. The node's
peer listener binds container loopback; it has no published peer port. An owned
Python TCP relay exposes only the selected loopback ports while containers
retain internal networks without an outbound default route. Readiness verifies
internal network isolation, absence of a default route, and an empty peer list.
Two unused /28 subnets are selected from `10.253.0.0/16`, excluding existing
Docker networks and host routes. Set `--subnet-pool` if that range conflicts
with your host. This avoids consuming Docker's large default address pools.
Do not attach unrelated containers to these fixture networks. The unauthenticated RPC can mine only this project's
fresh Regtest chain.

The default node config and mining helper exercise the CI schedule through NU7
and its shielded prefix. Use `--node-config <Regtest TOML>` for a client release's
consensus schedule. The checked-in `deploy/zebra/regtest-nu6-at-one.toml` activates
through NU6.3 at height 1, matching the demonstrated Zingo nakednet release.
`deploy/zebra/regtest-ironwood-at-250.toml` delays NU6.3 to 250, matching the
Zkool release evaluator; pass `--tip 260`. Both leave NU7 unset. Custom configs
use transparent `generate` mining; they do not assert the default helper's
three-pool shielded prefix. Schedule selection belongs to the fixture config,
not wallet names in server runtime. Client adapters still own their wallet's
funding and lifecycle transactions.

Pass `--raw-blob-policy all` for Zallet full-block serving. Every canonical
reader receives the same retention admission expectation. Use a new fixture
when changing retention. To test built or extracted release executables with
existing runtime images, add `--bin-dir /absolute/candidate/bin`; the command
mounts the four named executables and records their SHA256 values. All four
running `/healthz` revisions must equal `--revision`, including this mode.
This check establishes executable identity, not release signature provenance.
Verify downloaded archives or images through the release workflow separately.

The command prints the absolute `ready.json` path only after all four `/readyz`
responses reach the node tip. The receipt includes the actual node activation
map, complete health capabilities and versions, candidate commit, image IDs,
optional binary hashes, endpoints, retention, chain hash, and observation time.
`mine` and `status` refresh it; a failed refresh removes stale readiness.
Startup also saves `ready.initial.json` once. Pin that immutable receipt's SHA256
as a lifecycle harness input. Keep the changing `ready.json` outside pinned
inputs, because mining and status refresh it. Readiness does
not certify a wallet lifecycle.

Startup failure or interruption tears down this project's relay, containers,
networks, and volumes. A random ownership label prevents stale state directories
from controlling a newer instance that reuses the same Compose project name.
`down` removes those resources and readiness but keeps local
configuration and identity files for inspection. A stopped project directory
cannot be reused accidentally: select a new name, or remove that specific
stopped directory under `.tmp/regtest-fixtures/` before a fresh run. Never use
another campaign's project name, store, or ports.

Exercise setup, following, concurrent schedules, full-block capability, a fresh
repeat, failed-start cleanup, and project-name reuse protection with:

```bash
scripts/test-regtest-fixture.py --images /tmp/fixture-images.json \
  --revision 830b426060169f5b2e94d84ff0f134bc1ad96004 --port-base 43100
```

## Gate a real wallet lifecycle

Start the disposable fixture for the wallet's activation schedule, then use the
client's entrypoint with the fixture's immutable `ready.initial.json`. Allocate
a new wallet and output directory on each run.

| Wallet engine | Reproduction entrypoint |
| --- | --- |
| Zingo and standalone Zkool | [Build the pinned engine and generate the case manifest](../../scripts/wallet-clients/README.md). Run each Zkool internal-address setting as its own case. |
| Vizor | [Build the tagged Rust engine and run the funded wallet case](../../scripts/wallet-clients/vizor.md). |
| Cake Wallet | [Build and run the exact Zcash engine pinned by the app](../../scripts/wallet-clients/cake-regtest.md). |
| zcash-devtool | Use [run-devtool-regtest.py](../../scripts/wallet-clients/run-devtool-regtest.py) with the Regtest-enabled CLI, its source revision and binary SHA-256, the readiness receipt, and a fresh output directory. |

For native WalletQuery, select the lifecycle adapter from the exact Zallet
integration-tests or Zally revision under test. Zallet also requires canonical
full blocks. Retain them during ingest and configure the query service with the
same retention policy before starting the wallet.

To run the pinned zcash-devtool case, start the all-at-one Regtest fixture and
set `fixture_ready` to its `ready.initial.json`. From the Zinder checkout, build
the wallet in a private directory and pass its lockfile and binary digest to
the runner. Each step checks the node's block hash at the recorded fixture
height. To exercise a candidate service on the same node, keep the release
fixture as `--ready` and pass its service receipt with `--candidate`.
The runner creates fresh wallet and evidence directories.

```bash
case_dir=$(mktemp -d)
git clone https://github.com/zcash/zcash-devtool.git "$case_dir/client"
git -C "$case_dir/client" checkout --detach c8322f7e71ae46ab24801a721523ba83b27d911b
CARGO_TARGET_DIR="$case_dir/target" cargo +1.100.0 build --locked \
  --manifest-path "$case_dir/client/Cargo.toml" --features regtest_support
client="$case_dir/target/debug/zcash-devtool"
python3 scripts/wallet-clients/run-devtool-regtest.py \
  --client "$client" --client-sha256 "$(sha256sum "$client" | cut -d' ' -f1)" \
  --client-revision "$(git -C "$case_dir/client" rev-parse HEAD)" \
  --input "$case_dir/client/Cargo.lock" --ready "$fixture_ready" \
  --output "$case_dir/evidence"
```

Run a pinned wallet adapter against an isolated, funded fixture:

```sh
python3 scripts/run-wallet-lifecycle.py \
  --manifest /path/to/wallet-lifecycle.json \
  --output .tmp/wallet-lifecycle-evidence
```

Start from [the schema v2 manifest example](../../scripts/wallet-lifecycle.example.json).
Replace its paths, SHA-256 pins, endpoint, and adapter arguments. The example
cannot certify without real inputs and an adapter. Run
`python3 scripts/test-wallet-lifecycle.py` for isolated runner contract checks.
Those checks use a test adapter; they do not qualify a real wallet.

The manifest contains `schema_version: 2`, positive `timeout_seconds` per child,
and nonempty named `cases`. Each case identifies `wallet`, `version`, `variant`,
`network`, `implementation` (`app`, `engine`, or `simulated-profile`), `interface`
(`lightwalletd` or `native-wallet-query`), `endpoint`, and boolean
`require_no_fallback`. Use separate cases for Zkool `internal-enabled` and
`external-only`, and for each wallet/version/network/endpoint you intend to
qualify. The gate cannot infer a wallet omitted from the manifest.

`client` contains an absolute executable `path` and `sha256`. `inputs` maps
absolute file paths to required SHA-256 digests. Pin the client, adapter
executable, interpreted adapter source, configuration, activation schedule,
fixture identity, and patches. For a Python adapter, pin both interpreter and
script. Record exact service image digests, source commits, node schedule,
fixture height/hash, and configuration in the pinned fixture manifest. Pin an
immutable readiness snapshot, such as the fixture operator's `ready.initial.json`;
exclude mutable `ready.json` and wallet/node stores. The runner verifies inputs
before and after the case and before each command. Missing or mismatched pins
block execution. Hashes identify supplied inputs; reviewed adapter source and
its receipts must show that the real client used them.

`steps` maps these ordered names to commands: `bootstrap`, `restore`,
`transparent-discovery`, `shielded-discovery`, `send`, `pending`, `mined`,
`scanned`, `restart`, and `resume`. Commands are argv arrays. Whole arguments
`{report}`, `{scratch}`, and `{endpoint}` expand to a fresh JSON report path, a
shared private case directory, and the endpoint. Every command must receive
`{report}` and have a nonempty `expect` map of JSON pointers to domain results.
A counting-only command cannot qualify. Keep the persisted wallet in `{scratch}`
across the lifecycle, and use a separate new database for restore.

A single-process suite may execute once in `prepare` and save individual
observations in `{scratch}` for ordered readers to export. Pin its source and
record actual suite execution order in the fixture manifest. Reader order does
not establish the order of wallet operations. A funded restore may occur after
funding in that suite, provided it recovers existing activity into a genuinely
new, independent wallet database.

Every completed report contains `status: "completed"`, the exact `endpoint`,
boolean `fallback_used`, `assertions_enabled: true`, and
`assertion_mode: "always-on"`, plus domain fields. Always-on checks use explicit
failure paths, such as Python `if`/`raise`, that survive interpreter optimization.
Python `assert` alone is insufficient. An adapter may reject `-O`, but adapters
using explicit checks can run safely with it. Native adapters use checks that
are enabled in their measured build. These declarations are part of the pinned
adapter contract; the runner cannot establish real observations from an
unreviewed program that fabricates reports.

The following typed objects are required in v2 reports. All fingerprints are
64 lowercase hexadecimal SHA-256 characters; raw keys, seeds, transactions,
and authorization material must never appear in them or in report identifiers.

| Step | Required typed evidence |
| --- | --- |
| `send` | `transaction` with `fingerprint_sha256` and `observation: "submitted"` |
| `pending` | `transaction` with the same fingerprint and `observation: "wallet-pending"` |
| `mined` | `transaction` with the same fingerprint, `observation: "canonical-mined"`, and positive u32 `mined_height` |
| `scanned` | `transaction` with the same fingerprint, `observation: "wallet-scanned"`, and the same `mined_height` |
| `restore` | `restore` with the independent database, wallet, and activity fields below |
| `restart` | `restart_scope` matching the case; process evidence fields below when `process-restart` |
| `resume` | `progress` with u32 `before_height`, greater u32 `after_height`, and `new_block_mined_after_restart: true` |

Compute the transaction fingerprint as SHA-256 of the 32 bytes decoded from the
canonical RPC display transaction ID: `sha256(bytes.fromhex(display_txid))`.
Normalize a wallet's byte order to that display form first. This fingerprint is
not a hash of the full transaction. Obtain each stage independently from the
send outcome, selected wallet pending record, canonical mined response, and
selected scanned wallet record. Node mempool evidence alone cannot establish
wallet pending observation. All four fingerprints and both mined heights must
agree; sanitized linkage is retained in the receipts.

The `restore` object contains `fresh_database: true`, positive integer
`restored_activity_count`, and these fingerprint pairs:

- `source_database_fingerprint_sha256` and `restored_database_fingerprint_sha256`
  must differ. Hash canonical database paths or independent database identifiers.
- `source_wallet_fingerprint_sha256` and `restored_wallet_fingerprint_sha256`
  must agree. Use stable public account/address identity, never a seed or secret.
- `source_activity_fingerprint_sha256` and `restored_activity_fingerprint_sha256`
  must agree. Use the transaction fingerprint of known preexisting wallet activity.

An empty restore or reopening the existing database does not satisfy restore.
The known restored activity may precede the primary send tested later.

`restart_scope` is `process-restart`, `wallet-reopen`, or `unspecified` (default).
The completed restart report must repeat it. For `process-restart`, its `restart`
object contains distinct positive `before_process_id` and `after_process_id`,
and u32 `before_height` and `after_height` with no regression. These identify the
old wallet process and a different process reopening its persisted wallet.
Reopening at the same height is sufficient; a new block need not be scanned
until resume. `resume.progress.before_height` must be at least the reopened
height, and `after_height` must show actual progress on a block mined after
restart. Neither a same-process reopen nor resuming without new progress grants
process-restart lifecycle qualification.

Record endpoint observations in `endpoint_evidence` with `scope`
(`configured`, `process-trace`, or `local-only`), boolean `target_connected`, nonnegative integer
`unexpected_connections`, and, for a process trace, `trace_fingerprint_sha256`.
`local-only` means the operation made no Internet socket connection. Set
`target_connected: false` and `unexpected_connections: 0`; it never supplies a
positive connection observation. For example, an offline wallet list, local
confirmation lookup, or database reopen can use this scope. Missing endpoint
metadata also supplies no positive connection. A case needs at least one
explicit `target_connected: true` observation from a wallet lifecycle step to
qualify its target; fixture preparation or cleanup traffic cannot supply it.
Mixed online and local steps can qualify, while an entirely offline case cannot.

Retain a sanitized trace artifact matching that digest. Distinguish client
traffic from control-node mining connections in the reviewed adapter. Configured
endpoint evidence cannot establish the separate process-trace claim. That
claim requires a trace digest for every step, including a socketless trace for
local-only operations, and at least one observed target connection. A case
cannot qualify its target if fallback is allowed, observed, or contradicted by
unexpected trace connections. `require_no_fallback` must be true.

Other report statuses are `skipped`, `blocked`, `unsupported`, and `no-evidence`;
incomplete reports may add a non-sensitive identifier `reason_code`. Nonzero
exits, timeouts, missing reports, and failed checks become `failed`. Missing
steps become `no-evidence`, and later steps become `blocked`.

Optional `prepare` and `cleanup` use the same command/report/check contract.
Preparation can call the deployment runner for an isolated fixture. Cleanup runs
after partial preparation or execution; it is skipped if no pinned command was
entered. It must tolerate partial state and touch only case-owned resources.
By default cleanup verifies every case input. If teardown can run independently,
set `cleanup.input_paths` to the case input paths it needs, including any files
the teardown program reads indirectly. The runner also checks its executable
and pinned paths passed as command arguments. This lets teardown run if an
unrelated input changes. Qualification checks every case input before and after
cleanup, and an input change fails the case.
Reuse deployment runners and `scripts/mine-live-regtest.sh`; do not stop shared
processes by executable name. Process-group timeouts reuse the benchmark runner.
SIGINT and SIGTERM trigger bounded cleanup. Independently daemonized services
require explicit cleanup. Sends and mining are never retried automatically.

V2 `certified` requires all steps and cleanup to complete, a real app or engine,
linked primary transaction evidence, independent populated restore, target
endpoint policy, different-process reopen, and post-restart progress. Case
receipts expose `workflow_completed`, `transaction_linkage_certified`,
`restore_certified`, `process_restart_certified`, `wallet_reopen_certified`,
`resume_progress_certified`, `target_endpoint_certified`, and
`endpoint_trace_certified` separately. `target_connection_observed` records
whether any lifecycle step explicitly observed the target connection. A receipt
without this field set to true does not independently establish that connection
claim; retain its original runner digest and configured-versus-traced scope. `qualification_problems` names unmet flags.
`endpoint_evidence_scope` is `configured`, `process-trace`, or `unqualified`.

Schema v1 manifests remain accepted for workflow completeness, but cannot
produce a qualification pass in this runner. Historical v1 receipts retain their
original meaning and runner digest; do not reinterpret their `certified` field
as the stronger v2 claim. The gate returns 0 only for complete v2 qualification,
1 for incomplete qualification, 2 for invalid setup, and 130 on interruption.

`summary.json` contains the case matrix, individual `receipt.json` files contain
checks and sanitized typed observations, and `provenance.json` records schema,
manifest and runner digests, source revision, and tracked diff digest. The named
app or engine is the qualification scope; no mobile UI, platform build, or
external certification follows from an engine run. General workflow completeness
and simulated profiles remain separately visible.

Keep credentials outside argv and manifests. Endpoint URLs omit userinfo, query
strings, and fragments. The output directory is private. The runner discards
child stdout/stderr, deletes raw reports and wallet scratch after cleanup, and
retains only assertion names, pass/fail, hashes, heights, process IDs, and endpoint
metadata. Adapters must not put secrets in identifiers or write sensitive files
outside `{scratch}` and `{report}`. Review receipts before sharing them and retain
the manifest privately to reproduce its digest. Historical sync-only evaluations
and paired throughput campaigns remain separate evidence, using
`scripts/run-zkool-restore.py` and `scripts/run-benchmark-campaign.py` respectively.

## Compare wallet correctness across releases

The [wallet coverage matrix](../../scripts/wallet-clients/coverage-v0.22.0.md)
records each wallet build, interface, fixture, action, observed RPCs, and wallet
assertions. A successful restore can prove persisted history even when its
receipt identifies only the endpoint. Field handling requires a wallet
observation or an inspection of the pinned consumer. Keep native WalletQuery and
lightwalletd observations under their respective interfaces.

Build the pinned Zingo CLI and Zkool example and generate the Python protobuf
modules using the [client build instructions](../../scripts/wallet-clients/README.md#prerequisites).
Use Linux with `strace`. Install the Python `toml` package in the same venv
for the native ZIT driver and fixture commands. Set `release_tag` to the locally available selected
release tag and `release_sha` to its peeled commit. Set `evidence` to a new
private parent directory and `python_proto` to the generated modules.

For the Testnet checks, serve an isolated, peerless copy of the Zebra, canonical,
and wallet stores at the height and hash in the
[fixture descriptor](../../scripts/wallet-clients/subtree-testnet.json).
Use the selected release's ingest, projector, and lightwalletd binaries. Wait
until all three report the fixture tip. Set `local_wallet_url` to that compat
listener on IPv4 loopback. Each command verifies the running server commit and
chain fence before starting. The public recovery vector has known historical
activity in this window. Keep wallet databases and raw logs private.

The reference Zkool engines have pinned executable digests. For a different build, pass `--client-version VERSION` from its source checkout to the interrupted-stream and reorg commands. Each receipt records the version alongside the executable digest.

```bash
release_sha=$(git rev-parse "$release_tag^{commit}")
export PYTHONPATH="$python_proto"
"$client_venv/bin/python" scripts/wallet-clients/subtree-ab.py \
  --client "$zingo_cli" --endpoint "$local_wallet_url" \
  --release-commit "$release_sha" --output "$evidence/subtree" \
  --pagination --invalid-root-control
"$client_venv/bin/python" scripts/wallet-clients/interrupted-zkool.py \
  --client "$zkool_engine" --client-sha256 "$engine_sha256" \
  --endpoint "$local_wallet_url" --revision "$release_sha" \
  --output "$evidence/interrupted"
```

The first case restores the same wallet into fresh databases and changes only the
completion block hash byte order. Separate controls exercise one-row pagination
and the wallet's error path for an invalid Sapling root. The second case ends a
compact-block stream after 64 delivered blocks, reopens the same Zkool database
in a fresh process, and compares its final transaction history, notes, and pool
balances with an uninterrupted restore. Both cases observe the wallet process's
connections. The Zkool example exposes `testnet-read sync`, `resume`, and
`report` for this check.

To test wallet-visible rollback, use disposable Regtest images and matching
release binaries. The runner creates an isolated fixture, mines an owned
Ironwood coinbase on each side of the fork point, invalidates the later block,
and follows the replacement chain through another process restart. It verifies
the surviving transaction and the removal of the orphan's note and balance.

```bash
"$client_venv/bin/python" scripts/wallet-clients/reorg-zkool.py \
  --engine "$zkool_engine" --engine-sha256 "$engine_sha256" \
  --images "$fixture_images" --bin-dir "$release_bin_dir" \
  --revision "$release_sha" --proto-dir "$python_proto" \
  --port-base 48100 --output "$evidence/reorg"
```

For native stale-view recovery, build the ZIT external driver from
`zcash/integration-tests` at `485e7616f75b176ace08ac359cfd56d40dab951c`
against the selected Zallet checkout. The v0.22.0 baseline used Zallet
`32488c5554e5e0275c3313b2fc06c00e6780746a`. Pin the resulting test executable
and ordinary backend by SHA-256. The driver exposes a barrier before a native
full-block read. The scenario changes the query epoch on its own disposable
fixture, checks that the rejected read leaves scan work pending, then retries
the entire range and verifies persisted canonical block hashes. The test
harness triggers the retry.

```bash
zit_driver=$("$client_venv/bin/python" "$zit_root/qa/rpc-tests/build_zinder_driver.py" \
  --zallet-root "$zallet_root" --build-dir "$zit_build")
"$client_venv/bin/python" scripts/wallet-clients/native-expiry.py \
  --driver "$zit_driver" --driver-sha256 "$zit_driver_sha256" \
  --backend "$zallet_backend" --backend-sha256 "$zallet_backend_sha256" \
  --images "$fixture_images" --bin-dir "$release_bin_dir" \
  --revision "$release_sha" --port-base 48300 --output "$evidence/native-expiry"
```

Collect all eight schema-v2 lifecycle results using one `--lifecycle MANIFEST
SUMMARY READY` argument per run. Supply fresh scenario receipts for the selected
release. The comparator reports missing scenarios, lost calls, and lost wallet
assertions. A focused rerun may legitimately report uncovered scenarios; review
those lines before making a release-wide claim. To compare one family of
scenarios, add `--scenario-prefix zkool-reorg-` to `compare`. Supply existing
release-bound observations with repeated `--observations FILE` arguments.

```bash
python3 scripts/wallet-clients/coverage.py collect --release "$release_tag" \
  --revision "$release_sha" "${lifecycle_args[@]}" \
  --subtree-run "$evidence/subtree" --reorg-result "$evidence/reorg/receipt.json" \
  --interrupted-result "$evidence/interrupted/receipt.json" \
  --native-expiry-result "$evidence/native-expiry/receipt.json" \
  --output "$evidence/coverage.json"
python3 scripts/wallet-clients/coverage.py check "$evidence/coverage.json"
python3 scripts/wallet-clients/coverage.py compare "$evidence/coverage.json"
python3 scripts/wallet-clients/coverage.py render "$evidence/coverage.json" > "$evidence/coverage.md"
```

## Verify a two-wallet payment and return

For Vizor, zcash-devtool, and Zally, the [counterparty payment procedure](../../scripts/wallet-clients/payment-roles.md) checks the engine and Zingo in both payment roles, the return spend, and persisted balances. It also shows how to compare the eighteen role observations against a release baseline.

Use the [Zingo and Zallet round-trip procedure](../../scripts/wallet-clients/roundtrip.md) to verify a transfer through lightwalletd and native WalletQuery on the same chain. The disposable Regtest case controls mining and compares outcomes across independent runs. The live Testnet case can fund one payer through Fauzec and observe the payment through the selected release. Keep its private wallet state for interruption recovery. The [payment recovery cases](../../scripts/wallet-clients/payment-recovery.md) exercise a stopped wallet, a shallow rollback, and a running native wallet reconnecting to its endpoint.

For Zkool’s internal-enabled and external-only profiles and Cake Wallet’s software Zcash engine, run the [two-party payment campaign](../../scripts/wallet-clients/zkool-payment.md). Each profile uses its own NU6.3-at-250 chain and verifies a spend from a note received from Zallet.
