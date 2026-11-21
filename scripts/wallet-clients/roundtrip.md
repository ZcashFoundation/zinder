# Pay another wallet and spend the received funds

This case transfers 0.10 TAZ from Zingo CLI to an independently keyed Zallet wallet, then sends 0.04 TAZ back. Zingo uses lightwalletd and the ordinary Zallet daemon uses native WalletQuery. Both services must follow the same controlled node. The runner checks the recipient's amount and decrypted memo, reconciles each sender's debit with its reported fee, and reopens both wallets to check the confirmed ledger. Each run has a private wallet and operation journal.

## Prepare the clients

Use the [Zingo CLI build](README.md#prerequisites) with loopback transport support. Build or supply the ordinary `zallet-zinder` backend. The v0.22.0 reference client revisions are Zingolib `81d799775f11e882896e8710e8af12d6d4d29a41` and Zallet `32488c5554e5e0275c3313b2fc06c00e6780746a`. Set `zingo` and `zallet_backend` to their executables. The runner records both SHA-256 digests. Use Python 3.11 or newer on Linux with `grpcio`, `protobuf`, `toml`, and `mnemonic==0.21`. The Testnet native wallet uses a mnemonic import and Zallet's encrypted backup. Install the proving parameters required by the clients before the run.

Generate Python protocol descriptors with a compatible `protoc`. Place only the generated modules and trusted dependencies on the Python import path.

```bash
mkdir -p "$python_proto"
protoc -I crates/zinder-proto/proto/compat/lightwalletd --python_out="$python_proto" \
  crates/zinder-proto/proto/compat/lightwalletd/{compact_formats,service}.proto
protoc -I crates/zinder-proto/proto --python_out="$python_proto" \
  crates/zinder-proto/proto/zinder/v1/{ops/server_info,wallet/wallet}.proto
export PYTHONPATH="$python_proto"
release_sha=$(git rev-parse "$release_tag^{commit}")
```

## Run the repeatable Regtest case

Start the two-wallet repeat case with the fixture image map, release binaries, and client executables from the preceding section. Select a new state directory and two available local port bases. The command starts two independent chains in sequence, uses each base plus 82 for its wallet RPC, compares both payment receipts, audits their canonical blocks, and tears down the fixtures. The private directory retains each wallet run, startup and cleanup logs, `paired.json`, `canonical-audit.json`, and `cleanup.json`.

```bash
"$wallet_python" -B scripts/run-wallet-roundtrip-regtest.py --state "$roundtrip_state" \
  --port-bases 49500 49600 --images "$fixture_images" --bin-dir "$release_bin_dir" \
  --revision "$release_sha" --zingo "$zingo" --zallet-backend "$zallet_backend"
```

Each stage has a 5,400-second deadline. Set `--stage-timeout SECONDS` for a slower host or a shorter test. On timeout or cancellation, the runner signals the stage process group and escalates after `--stage-grace SECONDS` (default 30). Each fixture teardown has its own 600-second budget. Check `cleanup.json` for both projects and the named stage log for the original failure.

### Operate the fixtures separately

Choose a new private `state_root`, a unique `project` beginning with `zinder-fixture-`, an unused `port_base`, and a separate unused loopback `wallet_rpc_port`. Set `fixture_images` and `release_bin_dir` as described in the [fixture operator instructions](../../docs/runbooks/testing.md#disposable-wallet-regtest-fixtures). Start an empty chain for each independent run. The operator uses the same network upgrade schedule and all-block retention for both wallets.

```bash
umask 077
python3 scripts/regtest-fixture.py up --project "$project" --port-base "$port_base" \
  --state-root "$state_root" --images "$fixture_images" --bin-dir "$release_bin_dir" \
  --revision "$release_sha" --node-config deploy/zebra/regtest-nu6-at-one.toml \
  --raw-blob-policy all --tip 150
"$wallet_python" -B scripts/run-wallet-roundtrip.py regtest \
  --ready "$state_root/$project/ready.initial.json" --revision "$release_sha" \
  --zingo "$zingo" --zallet-backend "$zallet_backend" \
  --wallet-rpc-port "$wallet_rpc_port" --state "$private_run"
```

The runner mines one coinbase to the payer's transparent address, matures it, and shields it. Before mining either transfer, it requires the node mempool, payer wallet, and payee wallet to identify it as pending. It then checks the mined block against the active chain. After receiving spendable funds, Zallet pays Zingo back. On completion `receipt.json` contains the transaction fingerprints and the wallet-visible accounting.

Repeat those commands on a second new fixture with fresh wallets and identical inputs. Keep both fixtures running until the canonical audit finishes. Set `first_ready` and `second_ready` to their respective immutable `ready.initial.json` files. Set `audit_file` to a new path in an existing evidence directory.

```bash
"$wallet_python" scripts/run-wallet-roundtrip.py compare "$first_run/receipt.json" "$second_run/receipt.json"
python3 scripts/wallet-clients/audit-wallet-roundtrip.py --revision "$release_sha" \
  --run "$first_run" "$first_ready" --run "$second_run" "$second_ready" --output "$audit_file"
```

The comparison requires different wallet identities, fixture receipts, and transaction fingerprints. It checks the same release, client binaries, harness, fixture recipe, and wallet outcomes, including amounts, fees, pending observations, and balances. Wallet keys, transaction IDs, block hashes, and timing vary by design.

The audit reads both private journals, checks each fixture’s genesis and anchor against its loopback control node, and verifies the four signed transactions in the active-chain blocks. It matches the reported service binary digests to the immutable fixture record and the wallet harness history to each private journal. Each wallet run rechecks the client executables, harness files, and fixture receipt after wallet shutdown. The audit reconciles the payer and payee balances and records the audit tool digests alongside the payment outcomes. It makes read-only node calls. After the audit succeeds, run `scripts/regtest-fixture.py down` with each fixture’s own project and state root. The output can be supplied to the coverage collector’s `--roundtrip-audit` option.

## Run one live Fauzec-funded Testnet case

Use a peer-connected Zebra Testnet node, a following all-block Zinder store and wallet projection, and the selected release's native and lightwalletd listeners. Supply explicit IPv4 loopback URLs. The capture command verifies the network genesis, release, native full-block capability, block bytes, and chain agreement. It writes a new immutable readiness file. Keep that file for the whole trial. A frozen historical snapshot cannot receive a new payment.

```bash
umask 077
"$wallet_python" -B scripts/wallet-clients/roundtrip_fixture.py capture-testnet \
  --node-rpc "$local_node" --native-wallet-query "$local_native" --lightwalletd "$local_compat" \
  --query-ops "$local_query_ops" --compat-ops "$local_compat_ops" \
  --revision "$release_sha" --output "$testnet_ready"
"$wallet_python" -B scripts/run-wallet-roundtrip.py testnet \
  --ready "$testnet_ready" --revision "$release_sha" \
  --zingo "$zingo" --zallet-backend "$zallet_backend" \
  --wallet-rpc-port "$wallet_rpc_port" --state "$private_run"
```

The Testnet invocation requests one real 1 TAZ drip. It records the request ID before contacting Fauzec and checks the confirmed transaction, decrypted funding memo, and spendability in Zingo. The two subsequent transfers use the same amounts as Regtest. Public block arrival, confirmation policy, and faucet availability determine timing. The receipt distinguishes observed pending states from a transaction already mined by the time a wallet was polled. [Fauzec API](https://fauzec.com/.well-known/openapi.json)

## Resume and handle ambiguous submissions

Use the same state directory, readiness file, binary inputs, and RPC port when resuming. A lock serializes access. The journal persists a transaction intent before submission and the returned ID afterward. Pending faucet requests resume by their original ID. For inter-wallet transfers with a lost reply, the runner can adopt a transaction observed by both wallets that matches the intended recipient amount and memo. If submission remains ambiguous, it stops. Inspect the private journal, wallet histories, and node before taking further action. Starting another Testnet state directory would create another pair of wallets and initiate a new claim.

Keep the private state, encrypted seed backups, wallet encryption identities, wallet databases, and RPC credentials under the operator's control. Share the final receipt and input hashes as appropriate. The runner closes its wallet process after completion. Ownership of the Zinder and Zebra services stays with the fixture operator.
