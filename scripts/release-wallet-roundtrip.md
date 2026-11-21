# Qualify a release candidate with two wallet payments

Run the x86_64-v3 candidate on two fresh, peer-isolated Regtest chains. Each chain gets an independent Zingo CLI payer and Zallet recipient. Both wallets and the node must see the payment pending. After mining, the recipient spends part of the credited note back to the payer. Both wallets must reopen with the expected history, fees, and balances.

The canonical audit checks four distinct transactions in the active blocks. The gate compares the four wallet outcomes with the selected versioned matrix, including client digests, fixture configuration, fees, change, and spendable balances. Each chain supplies its own canonical heights and transaction fingerprints.

## Run locally

Prepare the [wallet environment and protocol descriptors](wallet-clients/roundtrip.md#prepare-the-clients), the [client bundle](#build-the-client-bundle), and the selected release archive. Verify the archive against the release checksum independently. Use new directories for private state and shareable evidence, and choose two available loopback port bases.

```bash
umask 077
"$wallet_python" -B scripts/release-wallet-roundtrip.py \
  --candidate-archive "$candidate_archive" \
  --archive-sha256 "$candidate_archive_sha256" \
  --tag "$release_tag" --commit "$release_sha" \
  --client-bundle "$bundle" --client-bundle-sha256 "$bundle_sha256" \
  --images scripts/release-wallet-images.json \
  --baseline-matrix scripts/wallet-clients/coverage-v0.22.0.json \
  --state "$private_state" \
  --evidence "$evidence" \
  --port-bases 51500 51600
```

Inspect `summary.json`, `comparison.json`, and `MANIFEST.json` in the evidence directory. A passed result requires both fixtures to have been removed. `blocked` indicates a required artifact is unavailable. A changed fee, missing observation, or changed wallet build requires investigation and a reviewed baseline update. A port collision during the initial bind check or fixture startup triggers one retry. Retain the raw state and wallet databases privately.

## Build the client bundle

The reference pins are Zingolib `81d799775f11e882896e8710e8af12d6d4d29a41` and Zallet `32488c5554e5e0275c3313b2fc06c00e6780746a`. Build the Zingo CLI exactly as the [wallet engine prerequisites](wallet-clients/README.md#prerequisites) describe:

```bash
git clone --no-checkout https://github.com/zingolabs/zingolib.git "$zingo_source"
git -C "$zingo_source" checkout --detach 81d799775f11e882896e8710e8af12d6d4d29a41
CARGO_BUILD_JOBS=2 cargo +1.99.1 build --locked \
  --manifest-path "$zingo_source/Cargo.toml" \
  -p zingo-cli --no-default-features --features nakednet-test-mode
```

Build the ordinary Zallet native backend, which the runner launches directly:

```bash
git clone --no-checkout https://github.com/zcash/zallet.git "$zallet_source"
git -C "$zallet_source" checkout --detach 32488c5554e5e0275c3313b2fc06c00e6780746a
cargo +1.100.0 build --locked --no-default-features \
  --manifest-path "$zallet_source/backends/zinder/Cargo.toml" \
  --target-dir "$zallet_target"
```

Assemble the bundle around the two executables and one `inputs.json`:

```json
{
  "schema_version": 1,
  "zingo": {"binary": "zingo-cli", "source": "81d799775f11e882896e8710e8af12d6d4d29a41", "sha256": "<zingo digest>"},
  "zallet": {"binary": "zallet-zinder", "source": "32488c5554e5e0275c3313b2fc06c00e6780746a", "sha256": "<zallet digest>"}
}
```

```bash
mkdir -p "$bundle_root"
cp "$zingo_source/target/debug/zingo-cli" "$bundle_root/zingo-cli"
cp "$zallet_target/debug/zallet-zinder" "$bundle_root/zallet-zinder"
# Write inputs.json beside the executables, then:
tar -C "$bundle_root" -czf "$bundle" .
sha256sum "$bundle"
```

Record the full client source commits and binary digests in the bundle. The runner checks the digests against the actual executables and the selected baseline. Keep the bundle SHA-256 independently of the download location.


## Run on GitHub

Use [release-wallet-roundtrip](../.github/workflows/release-wallet-roundtrip.yml) with the tagged commit, the producing release workflow run ID, the x86_64-v3 build artifact, and the immutable HTTPS bundle URL and SHA-256. Enter `run-two-chain-gate` to dispatch. The selected tag must contain the gate. The workflow verifies the source workflow, repository, tag, and commit before downloading its artifact. Its read-only token needs `actions: read` to access that run.

The action provisions Python and generated descriptors, verifies the archive's four executable digests against `BUILD-INFO.json`, and mounts those executables in pinned runtime images. Only aggregate receipts, canonical transaction fingerprints, outcome comparisons, and a manifest of their hashes are uploaded. Local results do not establish hosted runner or arm64 execution.

Qualification evidence starts after the workflow verifies the candidate commit and downloads its release artifact. If the client bundle download or checksum check, Python setup, or descriptor generation then fails, the job exits unsuccessfully. The evidence artifact contains `summary.json`, `gate.json`, and their SHA-256 manifest. The summary identifies the failed stage. The candidate and wallets run only after setup succeeds.

## Activate the pre-publication gate

The wallet gate currently runs independently of release publication. Before making it a prerequisite, publish the checksummed wallet bundle and set `WALLET_ROUNDTRIP_CLIENT_BUNDLE_URL` and `WALLET_ROUNDTRIP_CLIENT_BUNDLE_SHA256`. Dispatch the workflow against a tagged candidate and verify a **passing** hosted receipt, four matched rows, and successful fixture cleanup.

Then add this job to [the release workflow](../.github/workflows/release.yml). Its timeout satisfies the release admission policy. The [composite action](release-wallet-action/action.yml) runs the same checks for dispatches and release jobs.

```yaml
  wallet-roundtrip:
    name: qualify candidate wallets on two fresh Regtest chains
    needs:
      - validate
      - compile
      - compare-binaries
    permissions:
      actions: read
      contents: read
    runs-on: ubuntu-latest
    timeout-minutes: 120
    steps:
      - name: Checkout validated commit
        uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        with:
          ref: ${{ needs.validate.outputs.commit }}
          persist-credentials: false

      - name: Download candidate release build
        uses: actions/download-artifact@9000827ccba6bdab643e8b6fd33ac0654aef8333 # v8.0.2
        with:
          name: release-build-x86_64-v3-unknown-linux-gnu
          path: candidate

      - name: Qualify the candidate
        uses: ./scripts/release-wallet-action
        with:
          tag: ${{ github.ref_name }}
          commit: ${{ needs.validate.outputs.commit }}
          version: ${{ needs.validate.outputs.version }}
          archive: candidate/zinder-${{ needs.validate.outputs.version }}-x86_64-v3-unknown-linux-gnu.tar.gz
          client-bundle-url: ${{ vars.WALLET_ROUNDTRIP_CLIENT_BUNDLE_URL }}
          client-bundle-sha256: ${{ vars.WALLET_ROUNDTRIP_CLIENT_BUNDLE_SHA256 }}
          state: ${{ runner.temp }}/release-wallet-state
          evidence: evidence

      - name: Upload allowlisted aggregate evidence
        if: always()
        uses: actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7.0.2
        with:
          name: release-wallet-roundtrip-evidence
          path: evidence/
          if-no-files-found: warn
          retention-days: 30
```

Add the gate to the existing publication dependencies.

```yaml
  publish-sdk-crates:
    needs:
      - validate
      - api-docs
      - sdk-packages
      - compare-binaries
      - wallet-roundtrip
```
