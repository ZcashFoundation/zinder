# Compare wallet reader performance

Run the private Regtest comparison for a quick PR decision. Use the frozen Testnet profiles for claims about warm wallet reads on real block data. Mainnet construction, cold startup, completed wallet restores, and reader replacement each need their own qualification.

## Compare two binaries locally

Use Linux with Python 3.11 or later and `taskset`. Choose a baseline and a candidate `zinder-compat-lightwalletd` built for the same target with the same Rust version, profile, features, and allocator. Keep each release binary next to its original `BUILD-INFO.json` if available. Build the measurement tools once on the same harness revision for both arms.

```bash
cargo build --release --locked -p zinder-bench --bin zinder-bench
cargo build --release --locked -p zinder-compat-lightwalletd --example wallet-reader-fixture
scripts/compare-wallet-readers.py \
  --baseline "$BASELINE_READER" --candidate "$CANDIDATE_READER" \
  --bench target/release/zinder-bench --writer target/release/examples/wallet-reader-fixture \
  --output .tmp/reader-comparison
```

The command creates one deterministic primary and two private `compact-blocks` listeners with separate RocksDB secondaries. Each binary serves the same warm 16-block frontier and a disjoint 16-block history control, both ascending and descending. Four independent clients run against one arm at a time. Every stream must match the fixture's expected block count, byte count, and SHA-256 over the raw protobuf frames. Each frame contributes its four-byte big-endian length followed by the payload. The harness checks the tip before and after each period. Six pairs alternate baseline-first and candidate-first. Temporary stores and listeners are removed on exit.

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

The two original release packets contain per-request observations, plans, build identities, oracle digests, counters, and their original verifier. On a machine with the exported evidence, set `WALLET_EVIDENCE` to the directory containing `465-integrated-release/` and `466-integrated-release/`. The local investigation uses `docs/investigations/indexer-performance/evidence/mainnet-wallet-defaults-20261122/reader-release-loop/` in the main checkout. Validate the packets against the repository pins before running their verifiers.

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

## Select the next measurement

| Question | Shortest useful measurement | Evidence to retain |
| --- | --- | --- |
| Do wallets complete a restore with the released replay fixes? | Run 16 Classic wallets over TLS on the original dense Mainnet window. Compare the same release settings with Go lightwalletd and ztreamer. The earlier prototype completed 20 of 80 restores, both references completed 80. Begin with one cohort. | Correct completed restores, validated wire/tree outputs, replay admissions and refusals by reason, server CPU per completion, latency, and client headroom. Use the [wallet-load workloads](../../services/zinder-bench/README.md#7-drive-wallet-shaped-load-at-an-endpoint) and the [matched campaign](testing.md#run-a-paired-benchmark-campaign). [Mainnet baseline issue](https://github.com/ZcashFoundation/zinder/issues/360) |
| Why does the managed reader use more memory at READY? | Hold the store and binary fixed. Cold-start canonical serving and signed catalogs at several row counts. Repeat on protected and portable filesystems. The 877-segment run used 537 MiB peak RSS versus 74 MiB canonical. The in-memory row layout takes 136 bytes on x86-64, or 465.9 MiB of rows at this coverage. Determine how much is resident before changing the layout. | RSS at READY and peak, private dirty and file-backed memory, physical I/O, FD count, time to first served request, catalog signature and wire parity. Portable full-catalog startup remains unmeasured. [Signed-history deployment](deploying-on-a-vm.md) |
| Does a wallet benefit when the chain moves? | On the [disposable wallet Regtest fixture](testing.md#disposable-wallet-regtest-fixtures), append identical blocks on both arms. At each new visible height, read the frontier once and then repeat it. Vary requests per generation. Separately time submission, ingest, projection, and reader visibility. A 49-second publication delay appeared on both historical builds at height 36. | Height/hash at each stage, first-after-advance and repeat latency/CPU, MultiGet calls, keys, and bytes. Keep the reader-generation fence and require byte parity across independently seeded replacement stores. |
| Can old and replacement generations fit in the reader's memory budget? | First run the deterministic `replacement_holds_two_full_tails_and_one_converted_chunk` test. For process memory, hold a 63.75 MiB tail, append one block, and measure the first replacement read in a fresh process. The archived single replacement reached 244.42 MiB. | Per-window RSS and reset high-water for the owned process, tail occupancy, in-flight readers, scope, and verified rows. The fixture writer accepts 256 blocks with 7,250 spends per block and `append.go`. The original Linux sampling procedure is in `466-hosted-full-window-memory/evidence.tar.gz`. |
| Where does mixed signed/live latency rise? | Run the signed boundary case with stage counters and the fully covered case as a control. The hosted #465 boundary first-frame p95 rose 9.96% at the paired median across six pairs. | Directional first-frame and completion p50/p95, per-request time after the first frame, CPU/MiB, retained byte count, and stage attribution. [Range profiling issue](https://github.com/ZcashFoundation/zinder/issues/372) |
| Does the Mainnet tail hit its byte cap? | Sum canonical encoded sizes from the catalog end through the tip at several real tips. Test reads that cross the measured cutoff before changing any limit. | Actual retained range, bytes, fallback read work, wire parity, and memory under overlapping requests. |
| Should dense construction or a new topology ship? | For dense construction compare fresh canonical stores through the same frozen Mainnet hash on matched isolated hardware. Then build the wallet projection and catalog, test cold and in-place adoption, and check native and compatibility outputs. For remote readers, verify cross-host replacement, restart and signed continuity separately. | First READY, closed disk bytes, writer CPU, publisher cost, startup I/O/RSS/FDs, and wallet parity. The dense build took 3 h 47 min versus 2 h 14 min sparse against a 3-hour budget. The 877-segment catalog took 519.71 s and 17.28 GiB to build. Use [initial sync](initial-sync.md), [the storage benchmark](../../deploy/storage-benchmark.md), and [the topology decision](../adrs/0004-storage-and-deployment-topologies.md). |

The detailed local findings and raw receipt links are under `docs/investigations/indexer-performance/analyses/wallet-reader-release-followups-20261122.md`. The two-host construction and catalog receipts are under `evidence/mainnet-wallet-defaults-20261122/controller/` in that collection. The five-way reference results and wallet completion receipts are under `evidence/z375-2026-11-21/`. The `repro-source/README.md` beside the two-host receipts indexes the archived runners and their pinned prerequisites for Go lightwalletd, ztreamer, native checks, and moving-tip analysis. These experiments qualify their measured host, workload, cache state, and transport. Public wallets, continuous reader pods, and fleet capacity need their respective operator and topology evidence.
