# Qualify the Vizor Rust wallet engine

The Linux lifecycle campaign runs the Vizor v0.0.65 Rust engine through its wallet
APIs. It uses a new mnemonic, funds transparent and Ironwood addresses, restores
the mnemonic into a second database, and tracks a signed payment across broadcast,
wallet-pending, mining, and wallet confirmation. A new engine process reopens the
database before the fixture mines a final block for resumed scanning.

Use a disposable, exclusively owned Zebra Regtest fixture with both JSON-RPC and
lightwalletd on IPv4 loopback. Activate Overwinter through NU6.2 at height 1,
NU6.3 at height 250, and leave NU7 inactive. Start past height 250. Keep the
fixture's readiness receipt. Each case mines more than 100 blocks for mature
coinbase outputs. The Linux tracer requires `strace`.

Set `vizor_archive` to the downloaded source archive and `fixture` to the directory
containing the ready receipt. The builder checks the archive SHA-256 before it
extracts the source. Its Rust build requires native development headers, including
`libudev-dev` on Ubuntu. [Pinned Vizor source archive](https://codeload.github.com/chainapsis/vizor-wallet/tar.gz/d0edb5b13890dd33d9bb93b5c4e50fd9973fa4e6)

```bash
python3 scripts/wallet-clients/build-vizor-regtest.py \
  --archive "$vizor_archive" --output .tmp/vizor-build
```

Generate a manifest and run the shared gate from the Zinder checkout. The
generator compares the engine binary and the checked-in Rust driver with
the digests recorded by the build. The build hashes the copy of the driver
passed to Cargo and checks it again before writing the receipt.
Rebuild engines with an unversioned receipt to generate a version 2 receipt.

```bash
python3 scripts/wallet-clients/vizor-manifest.py \
  --engine .tmp/vizor-build/vizor-regtest \
  --engine-provenance .tmp/vizor-build/engine-source.json \
  --fixture "$fixture/ready.initial.json" \
  --trace-tool scripts/trace-wallet-endpoint.py \
  --evidence .tmp/vizor-release-traces --output .tmp/vizor-release.json
python3 scripts/run-wallet-lifecycle.py \
  --manifest .tmp/vizor-release.json --output .tmp/vizor-release-result
```

For a candidate compatibility service on the same node, supply its readiness
receipt with `--candidate` and choose fresh manifest, trace, and output paths.
The receipt must identify the release revision, the candidate source and binary,
the shared node, and a distinct wallet endpoint. The live service must report
that revision through both `/healthz` and `GetLightdInfo`. Before opening a
wallet, the driver also reads the fixture's anchor block through `GetBlock`
and compares its height and hash with the immutable readiness receipt.
For a locally built Zinder candidate, set `ZINDER_BUILD_GIT_COMMIT` to its full
source commit when building the service.
Inspect the ten steps and the restore, linked transaction, process restart,
resume, and endpoint trace flags in `summary.json`. The process trace covers
the wallet driver and its children. The Python fixture controller uses a
separate loopback node connection to fund and mine. Flutter rendering, platform
secure storage, device TLS, and mobile background scheduling need app-level runs.
