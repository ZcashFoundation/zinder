# Run Cake's Zcash engine on Regtest

Use this Linux fixture to qualify the wallet engine used by Cake Wallet 6.5.4 against a local lightwalletd endpoint. It generates a software wallet, restores its seed into a fresh database after funding, and checks its transparent and Ironwood balances. It sends an Ironwood payment, observes the transaction through the wallet's pending API, mines it, and matches its height to the wallet history. It then reopens the encrypted database in another wallet process and scans a new block.

Cake Wallet 6.5.4 at `ae2765659afce6b353d20504830b81198b2fc5e2` pins `cake-tech/zkool2` at `cb3e96f10e58a8fc8632b40636a0c64aa21fb653`. The fixture uses the fork's stock GraphQL server and the same software account setting for internal addresses. Flutter, the Dart bridge, application UI, mobile device behavior, and production TLS require their own tests. [Released dependency](https://github.com/cake-tech/cake_wallet/blob/ae2765659afce6b353d20504830b81198b2fc5e2/cw_zcash/pubspec.yaml) · [Cake wallet integration](https://github.com/cake-tech/cake_wallet/blob/ae2765659afce6b353d20504830b81198b2fc5e2/cw_zcash/lib/src/zcash_wallet.dart)

Build the pinned engine in a private checkout with Cargo, a C compiler, Protocol Buffers, pkg-config, and the libudev development files. The runner uses Linux `/proc`, `strace`, and Python 3 with `cryptography` for the ephemeral GraphQL credentials. Run this from a Zinder checkout containing the TLS and lifecycle harnesses.

```bash
umask 077
case_dir=$(mktemp -d)
git clone --no-checkout https://github.com/cake-tech/zkool2.git "$case_dir/engine"
git -C "$case_dir/engine" checkout --detach cb3e96f10e58a8fc8632b40636a0c64aa21fb653
cargo +1.100.0 build --locked --jobs 2 \
  --manifest-path "$case_dir/engine/rust/Cargo.toml" \
  --bin zkool_graphql --no-default-features \
  --features graphql,bundled-sapling-params,ledger
```

Start a disposable Regtest fixture with all upgrades through NU6.2 at height 1, NU6.3 at 250, NU7 disabled, and a tip above 250. The command below reads the endpoint and mining RPC from its readiness receipt. The fixture advances the chain by 104 blocks. Give each run exclusive control of mining.

```bash
fixture=/absolute/path/to/ready.json
python3 -O scripts/wallet-clients/cake-regtest.py \
  --engine "$case_dir/engine/target/debug/zkool_graphql" \
  --trace scripts/trace-wallet-endpoint.py \
  --fixture "$fixture" \
  --endpoint "$(jq -r '.client_endpoint // .lightwalletd' "$fixture")" \
  --node "$(jq -r .node_rpc "$fixture")" \
  --run "$case_dir/run"
```

Inspect `steps.json` for the observed heights, funded recovery, and transaction fingerprints. Each wallet process writes a connection-only trace and a summarized target check. The run directory also contains disposable wallet databases and credentials and must stay private.

## Run the shared lifecycle gate

Generate the ten-step manifest directly from the local engine, the released Cake dependency declaration, and the fixture receipt. The generator verifies the source lockfile and dependency declaration against the pinned release, then hashes the engine, interpreter, trace tools, adapter, and remaining inputs. Pass `--engine-sha256` to check a known binary digest, or repeat `--source-input` to retain additional source and build receipts.

```bash
curl -fsSL https://raw.githubusercontent.com/cake-tech/cake_wallet/ae2765659afce6b353d20504830b81198b2fc5e2/cw_zcash/pubspec.yaml \
  -o "$case_dir/released-cw-zcash.yaml"
python3 scripts/wallet-clients/cake-manifest.py \
  --engine "$case_dir/engine/target/debug/zkool_graphql" \
  --engine-lock "$case_dir/engine/Cargo.lock" \
  --released-pubspec "$case_dir/released-cw-zcash.yaml" \
  --fixture "$fixture" --output "$case_dir/manifest.json"
python3 scripts/run-wallet-lifecycle.py \
  --manifest "$case_dir/manifest.json" --output "$case_dir/qualification"
```

The shared gate executes a fresh funded run. Its `summary.json` records the restore, transaction linkage, endpoint tracing, restart, and subsequent block progress. [Shared test harness](../../docs/runbooks/testing.md)
