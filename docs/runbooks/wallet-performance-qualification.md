# Compare wallet reader performance

Run the private Regtest comparison for a quick PR decision. Use the frozen Testnet profiles for claims about warm wallet reads on real block data. Mainnet construction, cold startup, completed wallet restores, and reader replacement each need their own qualification.

## Compare two binaries locally

Use Linux with Python 3.11 or later and `taskset`. Choose a baseline and a candidate `zinder-compat-lightwalletd` (or `zinder` binary invoked via `zinder run compat-lightwalletd`) built for the same target with the same Rust version, profile, features, and allocator. Keep each release binary next to its original `BUILD-INFO.json` if available. Build the measurement tools once on the same harness revision for both arms.

```bash
cargo build --release --locked -p zinder-bench --bin zinder-bench
cargo build --release --locked -p zinder-compat-lightwalletd --example wallet-reader-fixture
scripts/compare-wallet-readers.py \
  --baseline "$BASELINE_READER" --candidate "$CANDIDATE_READER" \
  --bench target/release/zinder-bench --writer target/release/examples/wallet-reader-fixture \
  --output .tmp/reader-comparison
```

The command creates one deterministic primary and two private `compact-blocks` listeners with separate RocksDB secondaries. The fixture writer and reader processes ignore inherited `ZINDER_*` configuration. Each binary serves the same warm 16-block frontier and a disjoint 16-block history control, both ascending and descending. Four independent clients run against one arm at a time. Every stream must match the fixture's expected block count, byte count, and SHA-256 over the raw protobuf frames. Each frame contributes its four-byte big-endian length followed by the payload. The harness checks the tip before and after each period. Six pairs alternate baseline-first and candidate-first. Temporary stores and listeners are removed on exit.

Start with an A/A run using the same binary twice on the intended machine. Use the spread to choose a meaningful budget **before** testing a change. The supplied [example policy](../../scripts/benchmarks/wallet-reader/policy.json) uses a 5% relative tolerance on CPU per MiB, blocks/s, first-frame p95, and completion p95. It is a starting point for this fixture. Pass `--policy` to use your team's actual tolerances. Increase `--repetitions` if CPU is below sampling resolution. Use an idle machine for release comparisons. The harness records the CPU sets it gives the client and readers. Other processes can still use those CPUs.

`decision.json` lists paired changes for each metric and workload. Exit 0 covers improvement and no clear change. Exit 1 signals a regression, 3 signals mixed evidence, and 4 signals missing or invalid observations. Input errors exit 2. An improvement in one case alongside a regression in another needs a decision about that tradeoff. The evidence directory also contains the executed campaign, policy, binary digests, oracle files, raw request observations, logs, and per-run reports. Retain it with your PR's source SHAs and build receipts. For binaries without build metadata, the harness records their digests. You can reevaluate a different budget without running the listeners:

```bash
scripts/compare-benchmark-campaign.py .tmp/reader-comparison/trials \
  --policy scripts/benchmarks/wallet-reader/policy.json
```

## Repeat the frozen Testnet comparison

The [standalone](../../scripts/benchmarks/wallet-reader/standalone-testnet.json) and [managed](../../scripts/benchmarks/wallet-reader/managed-testnet.json) campaign files contain the range boundaries, two directional wire digests per case, clients, requests, and tip fences from the release comparisons. The standalone profile compares frontier and historical reads. The managed profile compares a fully signed range with one crossing the catalog boundary. Use matching full wallet listeners on copies of the same frozen Testnet state at height 4,644,091. For the managed profile the 15 signed segments cover 4,581,652 through 4,643,091. Verify you are serving the intended signed path by checking catalog adoption and the signed-route metrics. Both canonical and signed serving can return identical bytes.

Run from the checkout root with the same `zinder-bench` on PATH for both arms. Replace the two gRPC endpoints, metrics URLs, executable paths, and PIDs in a copy of the chosen campaign. Record the readers' build identities, listener configuration, storage device, host, CPU affinity, and catalog manifest digest with the reports. PIDs used for accounting must belong to the local listener processes. Run the command on the reader host or omit the process measurements and select an appropriate policy. The historical packets document the precise client, warmups, CPU placement, and sampling boundaries used for their published numbers.

```bash
python3 scripts/run-benchmark-campaign.py "$CAMPAIGN" --output .tmp/testnet-reader-trials
scripts/compare-benchmark-campaign.py .tmp/testnet-reader-trials \
  --policy scripts/benchmarks/wallet-reader/policy.json
```

To reuse the method on another snapshot, generate directional oracles from an independently verified canonical endpoint, pin its network, tip, and hash, and replace the expected counts, byte totals, and digests. The full-wallet profiles require a fresh publication of all three canonical MultiGet counters in each sample. A missing witness invalidates the comparison. Pin the request mix as well as the data. For Go lightwalletd and ztreamer, compare validated wallet outcomes and protocol fields when legitimate encoders produce different wire bytes; keep the byte-exact reader comparison for implementations claiming the same encoding. Server CPU per MiB and client CPU are distinct from whole-host utilization. A warm frontier measures reuse within a generation. Moving-tip observations need an advancing writer.

## Recompute the original results

The two original release packets contain per-request observations, plans, build identities, oracle digests, counters, and their original verifier. On a machine with the exported evidence, set `WALLET_EVIDENCE` to the directory containing `465-integrated-release/` and `466-integrated-release/`. Validate the packets against the repository pins before running their verifiers.

```bash
set -eu
pins="$PWD/scripts/benchmarks/wallet-reader/archives.sha256"
(cd "$WALLET_EVIDENCE" && sha256sum --check "$pins")
scratch=$(mktemp -d)
trap 'rm -rf -- "$scratch"' EXIT
for packet in 465-integrated-release 466-integrated-release; do
  tar xzf "$WALLET_EVIDENCE/$packet/evidence.tar.gz" -C "$scratch"
  (cd "$scratch/$packet" && sha256sum --quiet --check SHA256SUMS && python3 -I verify.py > "$scratch/replay.json")
  cmp "$scratch/replay.json" "$WALLET_EVIDENCE/$packet/paired.json"
done
```

A byte-identical replay establishes what the archived runs measured. Fresh release testing requires the fixture, binaries, and host. Private stores and raw operator evidence are outside Git.

## Qualify a broader claim

For restore performance, run the same wallet cohort on the same frozen window and transport for each reader. Count restores that return the expected tree and block responses. Record replay admissions, refusals, server CPU per completed restore, latency, and client headroom. [Wallet workloads](../../services/zinder-bench/README.md#7-drive-wallet-shaped-load-at-an-endpoint) · [Mainnet baseline](https://github.com/ZcashFoundation/zinder/issues/360)

For moving-tip behavior, append the same block on both arms and check the published height and hash. Measure the first request after publication separately from repeated requests. Record CPU and canonical MultiGet calls, keys, and bytes at several request rates. [Disposable wallet Regtest fixtures](testing.md#disposable-wallet-regtest-fixtures)

For startup and replacement, hold the binary and store fixed. Vary signed catalog coverage and filesystem protection. Record time to READY, resident memory, physical reads, open descriptors, and wire parity. Include overlapping reader generations when measuring peak memory. [Signed-history deployment](deploying-on-a-vm.md)

Construction and cross-host topology have separate qualification procedures. [Initial sync](initial-sync.md) · [Storage benchmark](../../deploy/storage-benchmark.md) · [Topology decision](../adrs/0004-storage-and-deployment-topologies.md)
