# Counterparty payment roles for Vizor, zcash-devtool, and Zally

Run three signed payments between a wallet engine and independently keyed Zingo
wallets on disposable Regtest. First the engine pays Zingo. A different Zingo
wallet then pays a fresh engine wallet, which spends the received funds back.
The runner counts the inclusion block as confirmation one, then mines to the
ninth block after inclusion. It records each wallet’s height with its balance
and checks both heights against the canonical transaction. Zingo reopens
offline to read its persisted balance and history. Zally also reads back the
return change note from its reopened database.

The fixture operator publishes loopback endpoints. Choose an unused port base
with `--base`. The defaults are 51000 for Vizor, 51100 for devtool, and 51200
for Zally.

## Pins

| Input | Pin |
| --- | --- |
| Zinder release | `v0.22.0`, commit `999d51ab835037a2466763ca1a1e7739a26b72c1` |
| Runtime images | an explicit `--images` JSON map of the four runtime references |
| Release binaries | an explicit `--bin-dir` pointing at the release's binaries |
| Node | Zebra 7.1.0 `sha256:4f9536c8061e407f9f2ec81b7e0dd2f1762f81deec316abb1530dca8796a33ba` |
| Node config | `deploy/zebra/regtest-nu6-at-one.toml`, except Vizor's private NU6.3-at-2 copy |
| Zingo CLI | Zingolib `81d799775f11e882896e8710e8af12d6d4d29a41`; the binary reports its own build version |
| Vizor engine | `v0.0.65` source `d0edb5b13890dd33d9bb93b5c4e50fd9973fa4e6`, archive `0641e4f5…` |
| zcash-devtool | `c8322f7e71ae46ab24801a721523ba83b27d911b`, `--features regtest_support` |
| Zally | `31ee6c3ae09e109d189c6dcd691b192fb4c5b29e`, typed headless wallet, `zinder` feature |

The receipt pins the executed binaries, harness inputs, and node schedule by
SHA-256 before and after use. The matrix compares the binary digest and the
fixture recipe. A supplied binary must match a registered build to qualify.
Debug builds from a different directory can have a different digest even at
the same source commit. A different client build needs its own baseline.

## Build the engine harnesses

```bash
python3 scripts/wallet-clients/vizor-payment.py build \
  --source "$vizor_source/rust" --build "$build_root/vizor"
python3 scripts/wallet-clients/zally-payment.py build \
  --source "$zally_source" --build "$build_root/zally"
CARGO_TARGET_DIR="$build_root/devtool" \
  cargo build --locked --manifest-path "$devtool_source/Cargo.toml" --features regtest_support
```

The Zally build uses the source checkout’s toolchain and Cargo cache. Fetch its locked dependencies before an offline run. The first build derives a standalone lockfile from the workspace lock. Each later build uses `--locked`. The runner checks dependency versions, sources, and checksums against the selected workspace and uses the executable reported by Cargo.

Use a disposable Vizor source checkout. The builder makes its Rust crate a standalone Cargo workspace and installs the payment driver as an example. An updated driver triggers a rebuild even when the source has an older timestamp. The HID dependency requires `libudev-dev`.

## Run a case

Set `release_tag` to a locally available Zinder release tag. Set `images` to
the fixture image map, `release_bin` to its unpacked binaries, `zingo_cli` to
the pinned Zingo executable, and `output` to a private parent directory. For
Vizor, supply the payment driver, source directory, and the source archive's
provenance JSON. The runner resolves the tag under `refs/tags/`. The optional
`--revision` must match the tag's commit.

```bash
python3 scripts/wallet-clients/payment-roles.py --engine vizor --base 51000 \
  --release-tag "$release_tag" --images "$images" --bin-dir "$release_bin" \
  --zingo "$zingo_cli" --vizor-engine "$vizor_driver" \
  --vizor-source "$vizor_source" --vizor-provenance "$vizor_provenance" \
  --evidence "$output/vizor"
```

For Zally, generate the native protocol modules with a Python environment
containing `grpcio-tools` and `grpcio`. The probe reads `ServerInfo` and
`TreeStateAtHeight` from the native endpoint and checks the release and chain
anchor against the immutable fixture receipt.

```bash
mkdir -p "$python_proto"
"$wallet_python" -m grpc_tools.protoc -I crates/zinder-proto/proto \
  --python_out="$python_proto" \
  crates/zinder-proto/proto/zinder/v1/ops/server_info.proto \
  crates/zinder-proto/proto/zinder/v1/wallet/wallet.proto
python3 scripts/wallet-clients/payment-roles.py --engine zally --base 51200 \
  --release-tag "$release_tag" --images "$images" --bin-dir "$release_bin" \
  --zingo "$zingo_cli" --zally-source "$zally_source" --zally-binary "$zally_driver" \
  --native-probe-python "$wallet_python" --native-proto-dir "$python_proto" \
  --evidence "$output/zally"
```

To compile the Zally driver during the run, supply `--zally-build "$build_root/zally"`
in place of the binary argument.

For devtool, install `strace` and generate the lightwalletd modules in the
Python environment used for the live identity probe. The runner checks the
served release and the block hash at the fixture anchor before and after the
wallet commands. It records the endpoint that each online devtool process
connected to.

```bash
proto_source=crates/zinder-proto/proto/compat/lightwalletd
mkdir -p "$python_proto"
"$wallet_python" -m grpc_tools.protoc -I "$proto_source" --python_out="$python_proto" \
  "$proto_source/compact_formats.proto" "$proto_source/service.proto"
python3 scripts/wallet-clients/payment-roles.py --engine devtool --base 51100 \
  --release-tag "$release_tag" --images "$images" --bin-dir "$release_bin" \
  --zingo "$zingo_cli" --devtool "$devtool_cli" --devtool-source "$devtool_source" \
  --identity-probe-python "$wallet_python" --identity-proto-dir "$python_proto" \
  --evidence "$output/devtool"
```

Each run creates a new evidence directory, including a copy of its immutable
readiness receipt under `fixtures/`. Use `--fixture-state` to put the running
fixture's state on another local volume. The default is beside the receipt.
A qualifying run executes both directions and verifies removal of the owned
resources. A retained fixture (`--keep`), a borrowed fixture (`--ready`),
or a single direction (`--roles`) produces a diagnostic result.

## Compare the observed wallet behavior

`receipt.json`, wallet databases, and command logs are private. They can
contain raw transaction IDs, fixture memos, local paths, and wallet material.
The collector exports client and receipt fingerprints, assertions, and aggregate
accounting for each actor and leg.

```bash
python3 scripts/wallet-clients/coverage.py collect --release "$release_tag" \
  --counterparty-result "$output/devtool/receipt.json" \
  --counterparty-result "$output/vizor/receipt.json" \
  --counterparty-result "$output/zally/receipt.json" --output "$output/roles.json"
python3 scripts/wallet-clients/coverage.py check "$output/roles.json"
python3 scripts/wallet-clients/coverage.py compare "$output/roles.json" \
  --scenario-prefix counterparty-
```

The producer and collector check both the executable digest and its source
inputs before assigning a version. A supplied executable with an unregistered
digest produces an `unverified-inputs` receipt for a complete, disposable run. To register a new build, retain
the source revision, lockfile, driver, build log, and digest of the executable
reported by Cargo. Review those inputs before adding the digest and its source
record to the registry. For Vizor, the receipt also binds the selected Rust source
files, Cargo manifest, and lockfile to the release archive. [Registered wallet builds](counterparty_builds.py)

For a Zally driver compiled by the runner, keep `zally-build.json` beside the
receipt. It binds the executable selected by Cargo to the measured source
commit, lockfile, driver, and builder.

A change in the wallet build, fixture schedule, or counterpart build makes a
row incomparable. A lost wallet assertion or a change in recorded accounting
fails the comparison.

## What these runs establish

Vizor’s confirmed transaction supplies the network fee. The pinned driver also
reports its proposal fee. Both values must be present and agree. Devtool supplies its persisted transaction fee. A
missing, nonpositive, or inconsistent confirmed fee fails those payer cases.

Vizor also exposes its outgoing pending row. Its incoming
row was absent during the mempool observation. Its Regtest fixture activates
NU6.3 at height 2, before any tested wallet's birthday.

Zally's fee is the residual after subtracting the payment from the wallet's
balance decrease. Zally exposes neither a sent-history row nor a pending read
for these payments. Its payer restart check verifies the persisted balance.
The sender recorded the inbound memo, but Zally did not retain it in the
receive row.

The devtool Sapling receiver credited an incoming transfer and spent it back.
The incoming output had a SQL `NULL` memo. A retained memo with different
contents fails the case. Unified receiver use and inbound memo
decryption remain unexercised for this build. The connect trace identifies
the wallet's destination. The probe measures the server's revision and block
hash. Neither identifies the gRPC methods used by the wallet.

Pending assertions describe the snapshot taken before mining. Across repeated transfers from devtool and Zally to Zingo, the pending receive
row appeared in some pre-mining snapshots and was absent in others. The receipt identifies the wallet height at which each settled credit was measured.
The coverage rows describe tested wallet engines and interfaces. Mobile apps,
hardware signing, and production capacity require separate evidence.
