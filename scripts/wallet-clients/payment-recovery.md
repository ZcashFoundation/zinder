# Recover a pending payment on Regtest

Use these cases to check whether Zingo CLI and an independently keyed Zallet wallet still agree about the same shielded transfer after an interruption. Zingo connects through lightwalletd. Zallet uses native WalletQuery. Each case starts a peer-isolated chain, funds a new wallet pair, sends 0.10 TAZ, and spends 0.04 TAZ back to the payer.

| Case | Interruption | Wallet observation |
| --- | --- | --- |
| `disconnect` | Close Zallet and leave Zingo uninvoked while ten blocks are mined. | Both wallets had observed the pending transaction. After reopening, each records one confirmation at its canonical mining height. |
| `rollback` | Invalidate the block containing the transfer and extend a competing branch. If necessary, submit the original signed bytes to the fixture node again. | Both wallets follow the replacement branch, agree on where the payment was remined, and record it once. The receipt distinguishes automatic readmission from explicit rebroadcast. |
| [Running daemon loses its connection](transport-payment.md) | Stop the private relay between the running Zallet process and its native endpoint. | The same process reconnects and recovers the confirmed amount, memo, and spendable funds. |

## Run both recovery cases

Prepare the [wallet executables, Python descriptors, and isolated fixture inputs](roundtrip.md#prepare-the-clients). Set `evidence` to a private parent directory. Each invocation creates a new output directory and requires unused, non-overlapping loopback ports. Port bases below are examples.

```bash
"$wallet_python" -B scripts/wallet-clients/payment-recovery.py campaign \
  --output "$evidence/recovery" --port-bases 50000 50100 \
  --images "$fixture_images" --bin-dir "$release_bin_dir" --revision "$release_sha" \
  --zingo "$zingo" --zallet-backend "$zallet_backend"
```

To repeat one case, replace `campaign` with `disconnect` or `rollback`, supply one `--port-base`, and choose a new output directory. The runner retains a receipt for each case and a campaign summary.

Fixture startup has a 600-second deadline and teardown has a 180-second deadline. If either expires, the runner stops the operator and its Compose subprocesses before continuing cleanup. If Zallet fails to shut down, the runner verifies the daemon’s identity, sends it SIGTERM, and escalates to SIGKILL if it is still running. The case records a failure and finishes wallet cleanup before removing the fixture.

The campaign gives each case 7,200 seconds by default. Set `--case-timeout` and `--case-grace` in seconds to change the deadline and the time allowed for graceful shutdown. On timeout the supervisor stops the case, checks its wallet, relay, and Docker project, and records the outcome in the case’s `supervision.json`. It skips the remaining case after a timeout or interruption. Keep that file beside a supervised `receipt.json` for coverage import. A passing campaign case requires cooperative exit, a matching receipt digest, and verified teardown.

At each observed confirmation the case checks the active node block, membership of the signed transaction, and the wallet balances. Each payer debit includes its reported fee. Each payee records the expected credit and memo. The return spends the recovered credit. Reopening the wallets preserves the confirmed balances and transaction history.

The rollback receipt records the invalidated and replacement block hashes, the original and remined transaction fingerprints, both signed-byte digests, and each observed wallet mining height. A one-block fork can move the transaction or place it at its original height in a different block. The wallets follow the longer served branch. The test does not establish how a wallet displays the intermediate orphan or responds to a deep reorganization.

The receipt records fixture removal. Keep wallet state, node logs, and the signed transaction under the owner-only private directory. If a run is interrupted during cleanup, use the fixture project and state root recorded by that run with the [fixture operator](../../docs/runbooks/testing.md#disposable-wallet-regtest-fixtures).

## Compare release coverage

Import the three case receipts from the selected release. Each receipt must identify the wallet executables and harness files before the run and after teardown. Older experimental receipts without those checks remain available as raw observations. The collector projects a row for each affected wallet. For the transport outage it projects only Zallet, since Zingo kept its independent lightwalletd route.

```bash
"$wallet_python" -B scripts/wallet-clients/coverage.py collect --release "$release_tag" \
  --revision "$release_sha" --recovery-result "$evidence/recovery/disconnect/receipt.json" \
  --recovery-result "$evidence/recovery/rollback/receipt.json" \
  --recovery-result "$evidence/native-disconnect/receipt.json" --output "$evidence/recovery-matrix.json"
```

Compare these five observations independently from the other lifecycle cases. A differing client or fixture is reported as incomparable. The comparison checks both payment directions, including the return fee, the payer’s debit, the recipient’s credit, and each wallet’s ending balance. It also reports previously passing wallet assertions lost in the candidate.

```bash
"$wallet_python" -B scripts/wallet-clients/coverage.py compare "$evidence/recovery-matrix.json" \
  --baseline scripts/wallet-clients/coverage-v0.22.0.json --scenario-prefix payment-recovery-
```
