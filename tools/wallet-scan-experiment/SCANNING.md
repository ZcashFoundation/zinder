# Shifted-key trial decryption

A wallet can use a second public point, `[2^64]EPK`, to shorten one step of Orchard and Ironwood trial decryption. This experiment measures a scanner that consumes the point, compares every recovered receipt with the shipping Orchard scanner, and exercises missing and incorrect hints.

## Observed results

On a shared Linux desktop with an AMD Ryzen 9 9950X, correct hints reduced elapsed scan time in both Rust and Chromium 159. The matched control uses the same viewing-key preparation, prefilter, and note confirmation as the hinted scanner. Comparing these two arms isolates the effect of supplying and using the extra point. Comparing against stock measures the combined effect of the scanner changes.

| Runtime | Archived actions | Stock median | Hinted median | Reduction vs. stock | Reduction vs. matched control |
| --- | ---: | ---: | ---: | ---: | ---: |
| Native Rust | 4,213 | 266 ms | 223 ms | 15.8% | 17.4% |
| Chromium / WebAssembly | 4,213 | 1,168 ms | 1,038 ms | 13.5% | 15.4% |
| Native Rust | 46,318 | 2,631 ms | 2,252 ms | 15.7% | 18.2% |
| Chromium / WebAssembly | 46,318 | 14,735 ms | 11,906 ms | 19.1% | 19.7% |

Each percentage is the median reduction within paired runs. The smaller range had three fresh processes per runtime and 18 timings per arm. The longer range had one process per runtime and six timings per arm. The longer range contains the smaller one, and Ironwood accounts for over 99% of the archived actions in both. The measurements describe these workloads on this desktop. Phone performance, energy use, network transfer, and full wallet restoration require separate measurements. The [raw samples](evidence/scans) and [timing summary](evidence/scan-summary.json) include CPU times and the spread between processes.

With correct hints, all arms returned identical receipts on both runtimes. The shorter range yielded all four known fixture receipts. The longer range yielded the same four plus eight archived receipts recognized by the stock scanner for the public fixture account. A valid but incorrect hint caused the hinted scanner to miss a known receipt. The [correctness and trust checks](#correctness-and-trust) show why point encoding alone cannot establish that a wallet found every payment.

## Reproduce a scan

The included Testnet corpus contains 4,213 actions. Each run adds six deterministic encrypted actions with known recipients. The scanner takes only public chain data and uses fixed, public fixture accounts. Use this variable-time implementation exclusively for experiments with those accounts.

Run the native comparison from the repository root.

```sh
corpus=tools/wallet-scan-experiment/evidence/testnet-4597953-4600000.scan
cargo run --locked --release -p wallet-scan-experiment --no-default-features --features runner \
  --bin compare-wallet-scans -- "$corpus" --batch-size 128 > native.json
```

For Chromium, install Node.js, Playwright, its browser, and the wasm-bindgen CLI version that matches `Cargo.lock`. These commands use an isolated output directory.

```sh
mkdir -p .tmp/scan-browser
rustup target add wasm32-unknown-unknown
cargo install --locked wasm-bindgen-cli --version 0.2.130 --root .tmp/scan-browser
npm install --prefix .tmp/scan-browser playwright@1.59.1
.tmp/scan-browser/node_modules/.bin/playwright install chromium
cargo build --locked --release -p wallet-scan-experiment --lib \
  --target wasm32-unknown-unknown --no-default-features
.tmp/scan-browser/bin/wasm-bindgen --target web --out-dir .tmp/scan-browser/web \
  target/wasm32-unknown-unknown/release/wallet_scan_experiment.wasm
PLAYWRIGHT_MODULE="$PWD/.tmp/scan-browser/node_modules/playwright/index.mjs" \
  node tools/wallet-scan-experiment/browser.mjs \
  .tmp/scan-browser/web/wallet_scan_experiment.js "$corpus" 128 1 > chromium.json
python3 tools/wallet-scan-experiment/summarize.py native.json chromium.json
```

Set `CHROMIUM_EXECUTABLE` to select an installed browser. To reproduce the longer corpus, use the [exporter](README.md#replaying-a-range) with Testnet heights 4,580,011 through 4,600,000. The [payload report](evidence/native-payloads.json) records the expected SHA-256 for each range.

The checked-in samples can also be summarized directly.

```sh
python3 tools/wallet-scan-experiment/summarize.py tools/wallet-scan-experiment/evidence/scans/*.json
```

## What the three arms measure

| Arm | Key agreement | Note discovery |
| --- | --- | --- |
| Stock | Orchard 0.16.0 and its batch key agreement | The upstream compact trial-decryption path |
| Ordinary control | Two signed-digit lanes and 128 doubling steps | Experimental plaintext-version prefilter and upstream confirmation |
| Hinted | Four lanes and 64 doubling steps, using the shifted point | The same prefilter and confirmation as the ordinary control |

The two experimental arms prepare the same incoming viewing keys once, use the same signed-digit recoding and odd-multiple windows, and batch-normalize the points. Their measured scans include ephemeral-point decoding, window construction, key agreement, the KDF, prefiltering, and full upstream compact-note decryption for each candidate. The hinted arm also decodes the extra point and constructs its additional windows. Both arms retain the original EPK for the KDF and confirmation. See the [Orchard implementation](https://docs.rs/crate/orchard/0.16.0/source/src/note_encryption.rs) and the [shifted-point proposal](https://github.com/zcashlabs/ztreamer/pull/382).

Every runner prepares the corpus, computes the stock reference answer, and warms all three arms before timing. Each cycle measures all six arm orderings. The timed scan compares the output to the reference by action position, pool, viewing-key index, amount, recipient, and note commitment. The reports include a digest of these fields for cross-platform comparison. Each trial uses one synthetic account with its external and internal incoming viewing keys and groups the actions by pool into batches of 128 in the published measurements. Native CPU time is process CPU on Linux. Chromium CPU time is renderer main-thread CPU between DevTools probes, including dispatch. Elapsed time covers the synchronous scan on an already loaded corpus. Download, parsing the corpus, account setup, wallet database and commitment-tree updates, and user-visible restore time are outside the timed interval.

## Correctness and trust

A deterministic fixture supplies one external receipt, one internal receipt, and one foreign note for each pool. All three arms recovered the four expected receipts and rejected the two foreign notes. Removing hints or providing an invalid 32-byte point encoding recovered all four through fallback. Replacing the first hint with another action’s valid point reduced the hinted result to three. The stock scanner continued to find four. A malformed corpus length fails at input decoding.

A validly encoded point carries no proof of its relationship to the on-chain EPK. A wallet can miss a payment if it trusts an incorrect hint, even when it verifies every note it does discover. This result matters when choosing a wallet trust model. The benchmark compares honest hints and uses known incorrect inputs to demonstrate the failure. Verification or a different trust mechanism would have its own cost.
