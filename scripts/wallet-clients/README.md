# Real wallet engine lifecycle adapters

Run Zingo and standalone Zkool against disposable local Regtest funds. These adapters create wallet databases, restore known activity into an independent database, discover transparent and shielded funds, sign a new transfer, observe it before mining, confirm the same transaction, reopen the persisted wallet in a new process, and scan a block mined after restart.

These are native engine checks. They do not qualify mobile applications, operating-system trust stores, physical devices, Cake's separate integration, or production capacity.

For other engines, use the [Vizor build and replay](vizor.md), [Cake build and replay](cake-regtest.md), or [zcash-devtool build and replay](../../docs/runbooks/testing.md#gate-a-real-wallet-lifecycle).

## Prerequisites

Provision the dedicated node and serving endpoint with the [Regtest fixture command](../../docs/runbooks/testing.md). Use its immutable `ready.initial.json`. The manifest derives the node and lightwalletd addresses from this receipt. Before funding, the adapter compares the live schedule, node block hash, and lightwalletd compact block hash against the pinned chain anchor. The adapters accept numeric loopback HTTP endpoints and mine only through the node identified by the receipt. Never supply an existing operator wallet or production node.

The unmodified engine schedules differ:

| Engine | Regtest activation schedule |
| --- | --- |
| Zingo 8.0.0 | Overwinter through NU6.3 at 1; NU7 disabled |
| Zkool v6.37.0 | Overwinter through NU6.2 at 1; NU6.3 at 250; NU7 disabled |

Use separate chains for Zkool's `internal-enabled` and `external-only` variants. Run candidate and release cases sequentially on each owned chain. Give every invocation a fresh harness output directory and evidence directory.

Build the Zingo CLI from Zingolib `81d799775f11e882896e8710e8af12d6d4d29a41`, using its own locked workspace and toolchain:

```bash
git clone --no-checkout https://github.com/zingolabs/zingolib.git "$zingo_source"
git -C "$zingo_source" checkout --detach 81d799775f11e882896e8710e8af12d6d4d29a41
CARGO_BUILD_JOBS=2 cargo +1.99.1 build --locked \
  --manifest-path "$zingo_source/Cargo.toml" \
  -p zingo-cli --no-default-features --features nakednet-test-mode
```

The `nakednet-test-mode` feature is an explicit measurement configuration. Record it in the evidence.

For Zkool, extract `hhanh00/zkool2` commit `d80892f163bc650e3b453f448da8c83eb5c32ddc` into a separate scratch workspace. Copy the private example into that workspace, then build against its lockfile:

```bash
git clone --no-checkout https://github.com/hhanh00/zkool2.git "$zkool_source"
git -C "$zkool_source" checkout --detach d80892f163bc650e3b453f448da8c83eb5c32ddc
mkdir -p "$zkool_source/rust/examples"
cp scripts/wallet-clients/zkool-regtest.rs "$zkool_source/rust/examples/sync_eval.rs"
CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR="$zkool_target" \
  cargo +1.99.0 build --release --locked \
  --manifest-path "$zkool_source/Cargo.toml" --example sync_eval
```

The example selects coin 2 and uses the application's account, sync, payment preparation, signing, and broadcast functions. The Python adapter sets `ZINDER_ZKOOL_USE_INTERNAL` for the selected scanning variant. It uses a public BIP-39 test vector exclusively for disposable Regtest wallets.

The fence probe needs generated Python lightwalletd modules plus `grpcio` and `protobuf`. Prepare them without modifying the vendored schemas:

```bash
python3 -m venv "$client_venv"
"$client_venv/bin/pip" install grpcio grpcio-tools protobuf
mkdir -p "$python_proto"
"$client_venv/bin/python" -m grpc_tools.protoc \
  -I crates/zinder-proto/proto/compat/lightwalletd \
  --python_out="$python_proto" --grpc_python_out="$python_proto" \
  crates/zinder-proto/proto/compat/lightwalletd/service.proto \
  crates/zinder-proto/proto/compat/lightwalletd/compact_formats.proto
```

Install Linux `strace` and use this checkout's `scripts/trace-wallet-endpoint.py` and schema v2 lifecycle runner. The manifest pins the client, adapters, trace wrapper, Python executables, generated modules, and readiness input. Keep Python package versions and the host inventory with the private evidence.

## Generate and run a case

Set each path below explicitly. Set `engine_sha256` to the executable digest retained with the pinned build. A digest mismatch rejects the manifest. The evidence directory retains client logs and publication waits independently of the harness, which removes its wallet scratch directory after the run.

```bash
python3 scripts/wallet-clients/make-manifest.py \
  --wallet zkool --variant internal-enabled \
  --binary "$zkool_target/release/examples/sync_eval" \
  --binary-sha256 "$engine_sha256" \
  --trace-tool "$trace_script" \
  --fence-python "$client_venv/bin/python" --proto-dir "$python_proto" \
  --ready "$immutable_ready" --evidence-dir "$new_evidence" \
  --output "$new_manifest"
python3 "$lifecycle_runner" --manifest "$new_manifest" --output "$new_output"
```

For Zingo, use `--wallet zingo`, omit `--variant`, and supply the CLI binary and its own chain. For the second Zkool case, use `--variant external-only` with that case's separate chain. For a candidate running beside the release on the same fixture, keep the release `--ready` and add `--candidate "$candidate_receipt"`. The candidate receipt identifies its `network`, `node_rpc`, distinct `client_endpoint`, `source_commit`, `binary_sha256`, and `shared_release_dependencies` (the release commit). A candidate deployed as a complete independent stack uses its own immutable `--ready`. Optional `--endpoint` and `--node` cross-check the selected receipt. Each run needs a fresh wallet and evidence directory.

## Read the receipts

Each step uses explicit checks that remain enforced under optimized Python. The adapters additionally refuse optimized startup, so a misconfigured invocation cannot silently discard a check. The `send`, `pending`, `mined`, and `scanned` records fingerprint the same canonical display transaction ID and match the canonical inclusion height to the wallet record. Restore compares independent database identities, public wallet identities, and known preexisting transaction activity. Zingo passes its generated recovery phrase through the restore process environment and excludes the recovery command's output from retained logs. Zkool passes its public Regtest vector as a driver argument.

Publisher retention can delay an accelerated Regtest chain. `wait-serving-fence.py` polls the actual serving endpoint for its tip, block, block range, and tree state, with a 180-second deadline. A block and range may differ in transparent fields because their default filters differ; the probe compares their canonical height and hash. Publication delay is retained separately from client execution.

The Zingo adapter mines a guard block before a scan and requires the wallet to scan the preceding target fence. Its receipt does not claim zero-block lag at the newest chain tip. This also prevents a partially scanned newest block from being mistaken for complete known-activity recovery. Pending observation occurs before any confirmation or guard mining for the submitted transfer.

Restart process IDs identify completed CLI invocation supervisors, each of which waits for its wallet process tree to exit. Zkool's offline report invocations directly identify native engine processes. Resume requires actual persisted wallet height growth after a new private block is mined. Endpoint connection traces are retained for online invocations; offline wallet reads do not create a new connection trace.

A partial gate remains partial. Preserve failed attempts, source identities, client logs, and the exact missing condition. Do not transfer evidence between engines, scanning variants, application platforms, transports, or server revisions.
