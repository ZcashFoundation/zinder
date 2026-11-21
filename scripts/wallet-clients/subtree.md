# Compare Sapling completion-hash encodings during funded recovery

Run a real Zingo CLI against a frozen, local Zinder Testnet service. Two fresh wallets recover the same public test-vector account, from height 4,624,102 through 4,644,091. The adapter serves each wallet the same protobuf responses. In the second restore, it reverses the 32 bytes of `SubtreeRoot.completingBlockHash` in each Sapling row. The original encoding agrees with the Go lightwalletd reference. The reversal reproduces the field encoding observed in ztreamer v0.2.0. Both arms use Zinder as their sole upstream.

## Run

Use Linux with `strace`, the pinned Testnet Zingo CLI, and Python with `grpcio`, `grpcio-tools`, and `protobuf`. Use the CLI built with `nakednet-test-mode`. The [wallet build and protobuf generation recipe](README.md#prerequisites) gives the source revision, features, and build commands.

Set `python_proto` to the generated Python modules directory, `wallet_python` to that environment's interpreter, `zingo_cli` to the verified executable, and `output` to a new private directory under an existing parent. Supply the actual selected release's full source commit and loopback HTTP endpoint. The gRPC service must report that revision and the fixture's frozen block hash.

```bash
PYTHONPATH="$python_proto" "$wallet_python" scripts/wallet-clients/subtree-ab.py \
  --endpoint "$wallet_endpoint" --release-commit "$release_commit" \
  --client "$zingo_cli" --output "$output" --pagination --invalid-root-control
```

The default [Testnet fixture](subtree-testnet.json) pins the baseline CLI executable, source revision, six rows shared by Zinder and Go v0.6.2, funded history digest, and expected spendable balance. When rebuilding the same CLI from a verified source checkout, record its checked-out revision, locked dependencies, toolchain, features, and executable SHA-256. Supply a separate copy of the fixture with the resulting executable digest via `--fixture`. Keep that fixture and the build receipt with the run so readers can identify the exact build. A wallet upgrade can change console output and the history digest. Confirm any new history baseline against an independent wallet readback.

## Read the result

A successful run writes `summary.json`. Each arm must independently match the fully scanned window, 143,049 outputs, all four pool balances, the known history digest, and 275,000 spendable Ironwood zatoshis. The CLI must reopen its persisted wallet in a separate process without the seed, sync to the same frozen fence, and reproduce those results. Separate processes also query the local wallet for balances, history, and spendability. The driver checks that the wallet files have distinct filesystem identities. Seed material, wallet files, history output, request fingerprints, and connect traces stay in the private output directory.

Before the wallet runs, the driver verifies the six roots and completion heights, then checks each completion hash against the corresponding compact block. During the wallet runs it records actual method calls separately from these preflight reads. Within each Sapling protobuf row it locates the unique completion hash, changes only those bytes, and decodes the result to verify all other fields. The receipt records the offsets, SHA-256 digests of both full row encodings, and hashes of the unchanged prefix and suffix.

Finite upstream replies are shared by method and exact request bytes. The A/B check compares request and framed response digests across the two wallet processes for restore and reopen. Only nonempty Sapling responses can change on the reversed route. The wallet's timer can issue another tip poll during a slower scan. The receipt records both poll counts, requires identical poll request and response payloads, and requires equal invocation counts for every other method. A mempool subscription must carry zero rows. Each online wallet process tree must connect exclusively to the loopback adapter.

With `--pagination`, a third fresh wallet receives one Sapling row per request, even when it requests all available rows. It must advance through indexes 0 to 5, confirm an empty reply at 6, and recover the same wallet state. With `--invalid-root-control`, a fourth fresh wallet receives an invalid field-element encoding in the first Sapling **rootHash**. It must report `server returned invalid subtree root` and scan zero blocks. The CLI can exit successfully with this sync failure, so the check requires both the wallet diagnostic and its reported scan state.

## What Zingo reads

At Zingolib revision `81d799775f11e882896e8710e8af12d6d4d29a41`, the decoder materializes the completion-hash bytes. The wallet sync path does not use them for validation, comparison, wallet state, or persistence. It consumes the streamed rows, advances its index, and requests an empty terminal page before finishing an unbounded read. [Pepper-sync RPC client](https://github.com/zingolabs/zingolib/blob/81d799775f11e882896e8710e8af12d6d4d29a41/pepper-sync/src/client.rs#L235-L294)

The sync path passes Sapling, Orchard, and Ironwood rows to the wallet. It uses `completing_block_height` to update shard ranges. It parses `root_hash` as a field element and can store the root in a shard or use it as an annotation for a partially populated shard. A shard whose existing leaves already compute its root can be kept as is. [Sync call sites](https://github.com/zingolabs/zingolib/blob/81d799775f11e882896e8710e8af12d6d4d29a41/pepper-sync/src/sync.rs#L2719-L2816) · [Shard ranges](https://github.com/zingolabs/zingolib/blob/81d799775f11e882896e8710e8af12d6d4d29a41/pepper-sync/src/sync/state.rs#L1347-L1416) · [Root validation and storage](https://github.com/zingolabs/zingolib/blob/81d799775f11e882896e8710e8af12d6d4d29a41/pepper-sync/src/witness.rs#L144-L190)

The recovered account holds Ironwood funds and has zero Sapling balance. The result establishes what this CLI does with populated Sapling rows while recovering that account. Sapling-funded spending, other engines, mobile applications, and production ztreamer end-to-end behavior need separate evidence.
