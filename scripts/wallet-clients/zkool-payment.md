# Pay and spend a received note with Zkool and Cake Wallet’s Zcash engine

Use this campaign to check both sides of a payment against a selected Zinder release. It starts three independent, peer-isolated Regtest chains, one each for Zkool’s internal-enabled profile, Zkool’s external-only profile, and Cake’s software Zcash engine. Each pays an independently keyed ordinary Zallet and receives funds back. The engine under test uses lightwalletd. Zallet uses native WalletQuery on the same chain.

## Prepare the wallet builds

Use Linux with Docker Compose, strace, and the wallet Python environment and generated descriptors described in the [two-wallet setup](roundtrip.md#prepare-the-clients). Cake also requires Python’s `cryptography` package to authenticate its private GraphQL connection. Install it in the wallet environment and install the clients’ proving parameters before starting. Set `wallet_python` to the prepared interpreter, `zkool_source` and `cake_source` to local upstream Git checkouts and `build_dir` to a new absolute scratch directory. The reference source revisions below select Zkool 6.37.0 and the Zcash engine used by Cake Wallet 6.5.4.

```bash
"$wallet_python" -m pip install cryptography==46.0.5
mkdir -p "$build_dir"/{zkool,cake}
git -C "$zkool_source" archive d80892f163bc650e3b453f448da8c83eb5c32ddc | tar -x -C "$build_dir/zkool"
git -C "$cake_source" archive cb3e96f10e58a8fc8632b40636a0c64aa21fb653 | tar -x -C "$build_dir/cake"
mkdir -p "$build_dir/zkool/rust/examples"
cp scripts/wallet-clients/zkool-payment.rs "$build_dir/zkool/rust/examples/zkool_payment.rs"
CARGO_TARGET_DIR="$build_dir/zkool-target" cargo +1.99.0 build --locked \
  --manifest-path "$build_dir/zkool/rust/Cargo.toml" --example zkool_payment
CARGO_TARGET_DIR="$build_dir/cake-target" cargo +1.99.0 build --locked \
  --manifest-path "$build_dir/cake/rust/Cargo.toml" --bin zkool_graphql \
  --no-default-features --features graphql,bundled-sapling-params
zkool_bin="$build_dir/zkool-target/debug/examples/zkool_payment"
cake_engine="$build_dir/cake-target/debug/zkool_graphql"
```

Set `zallet_backend` to the ordinary `zallet-zinder` executable. The reference build uses the `backends/zinder` lockfile at Zallet commit `32488c5554e5e0275c3313b2fc06c00e6780746a`, Rust 1.100.0, and `--no-default-features`. The campaign records the actual executables’ SHA-256 digests. A locally rebuilt executable with a different digest has its own identity for comparisons.

## Run the campaign

Set `wallet_python` to the prepared interpreter, `release_tag` to the release under test, `fixture_images` to the local fixture image map, and `evidence` to a new private directory. Supply the four release images tagged with that release, including its compat and native query services. Each chain starts at height 260, activates NU6.3 at height 250, and retains full blocks for Zallet. The runner checks the live serving identities, anchor, and upgrade heights before it funds either wallet.

```bash
umask 077
release_sha=$(git rev-parse --verify "refs/tags/${release_tag}^{commit}")
"$wallet_python" -B scripts/wallet-clients/zkool-payment-matrix.py campaign \
  --release "$release_tag" --revision "$release_sha" --state "$evidence" \
  --images "$fixture_images" --node-config deploy/zebra/regtest-ironwood-at-250.toml \
  --zkool-bin "$zkool_bin" --cake-engine "$cake_engine" --zallet-backend "$zallet_backend" \
  --trace-tool "$PWD/scripts/trace-wallet-endpoint.py" --port-bases 50600 50700 50800
```

The port bases are ordered by internal Zkool, external-only Zkool, and Cake. Use `--cases zkool-external`, for example, to run one configuration. Give a repeat its own directory and unused ports.

## Interpret a result

| Transfer | Wallet action | Evidence |
| --- | --- | --- |
| Outbound | The engine sends 10,000,000 zat to Zallet. | Canonical inclusion, sender debit and fee, recipient credit and decrypted memo. |
| Sweep | The engine sends its remaining funding change, apart from a small reserve, to Zallet. | Both ledgers reconcile, and the reserve is smaller than the later spendback. |
| Inbound | Zallet pays the engine 6,000,000 zat. | The engine reads its first incoming payment, decrypts the memo, and reports spendable funds. |
| Spendback | The engine pays Zallet 3,000,000 zat. | The spending transaction consumes a note from the inbound payment. Zkool exposes this link in SQLite and Cake exposes it through GraphQL. |

For every leg, the runner checks the signed transaction in the node mempool and the active chain. It records each wallet’s own pending observation separately. After restart, both wallets must recover all four confirmed transactions at their recorded heights and agree with their own pre-restart balances. The per-leg memo fields distinguish what was decrypted during confirmation from what the wallets expose after restart.

Read `summary.json` for case outcomes, actor observations, build identities, and cleanup. The SHA-256 index covers the per-case receipts and evidence files. Keep journals, wallet databases, credentials, and raw connection traces private. Zkool online commands and Cake engine sessions have endpoint traces. Zallet’s endpoint attribution comes from its persisted native configuration. Mobile UI, device keystores, hardware signing, and publicly trusted TLS require separate trials.

## Compare a repeat with the baseline

Import a complete three-case summary with its evidence files in place. Set `comparison` to a new output file. The comparison reports missing actor outcomes, different client or fixture identities, and changes in the recorded amounts and fees. Connection traces establish where the wallet connected. Individual RPC methods have no attribution in these scenarios.

```bash
"$wallet_python" -B scripts/wallet-clients/coverage.py collect --release "$release_tag" \
  --revision "$release_sha" --zkool-cake-result "$evidence/summary.json" --output "$comparison"
"$wallet_python" -B scripts/wallet-clients/coverage.py compare "$comparison" --scenario-prefix zkool-cake-
```
