# Wallet correctness coverage for v0.22.0

Zinder release `999d51ab835037a2466763ca1a1e7739a26b72c1` passed the funded Regtest lifecycle with eight client configurations. A frozen Testnet fixture and disposable Regtest forks supplied additional method and field evidence. The JSON records 82 action-level observations, their input identities, assertions, RPC counts where measured, and private receipt digests. [Machine-readable baseline](coverage-v0.22.0.json)

## What the funded lifecycle established

Each configuration recovered existing activity into an independent database, discovered transparent and shielded funds, and linked submission, wallet-pending state, canonical inclusion, and wallet scan to one transaction. Each reopened a persisted wallet in another process and advanced after a new block. All 80 required steps passed. The source receipts bind each case to its release and client binary. Their connection evidence identifies endpoints. It does not identify which RPC carried a particular discovery, send, or confirmation.

| Wallet configuration | Tested interface and client | Additional evidence or qualification |
|---|---|---|
| Zingo 8.0.0, nakednet CLI | lightwalletd, engine | Confirmed Ironwood note following shielding. The subtree experiment below also exercises this CLI on Testnet. |
| Zkool 6.37.0, internal enabled | lightwalletd, engine | A separate Regtest fork removed one owned Ironwood coinbase while preserving another. |
| Zkool 6.37.0, external only | lightwalletd, engine | A separate Testnet run compared recovery after stream interruption to an uninterrupted restore. |
| Vizor v0.0.65, Rust engine | lightwalletd, engine | The driver verified the fixture anchor over the wallet port. The Android and iOS applications were outside this run. |
| Cake 6.5.4 at `cb3e96f1`, internal enabled | lightwalletd, Linux GraphQL engine | The test exercised the forked engine through its GraphQL API. |
| zcash-devtool at `c8322f7e`, Regtest Sapling CLI | lightwalletd, engine | The wallet found each funding transaction at its mined height and had spendable transparent and Sapling balances. |
| Zallet at `32488c55`, ordinary backend | native WalletQuery, daemon application | The ordinary daemon used all-block retention. The separate native expiry case uses a feature-enabled driver. |
| Zally 0.2.2 stack upgrade at `31ee6c3a`, typed headless wallet | native WalletQuery, engine | The public pending-input row, mined Ironwood note, and shielding submission identified the same transaction. The additional shielded payment's pending evidence came from the node. |

## Signed payments between independent wallets

Two fresh Regtest chains each ran Zingo CLI 8.0.0 on lightwalletd and the ordinary Zallet daemon at `32488c55` on native WalletQuery. The four wallets had independent identities. Each chain started with spendable shielded funds in Zingo and an empty Zallet recipient. The matrix records the payer's and the payee's observations separately for both directions. The source receipts match the client binary hashes in the funded lifecycle rows.

| Payment observed on both chains | Payer state, in zatoshis | Recipient state, in zatoshis |
|---|---|---|
| Zingo sent 10,000,000 to Zallet | Zingo signed and paid a fee of 10,000. Its balance fell from 499,985,000 to 489,975,000. | Zallet decrypted the expected memo, credited 10,000,000, and reported the entire amount spendable. |
| Zallet returned 4,000,000 to Zingo | Zallet spent the newly received funds, paid a fee of 25,000, and held 5,975,000 in change. | Zingo decrypted the return memo and its balance rose to 493,975,000. |

For each payment the sender, recipient, and node all observed the same transaction pending before mining. Both wallets confirmed the transfers at heights 262 and 272 and retained the confirmed transactions and balances after reopening. An independent node audit checked active-chain block membership for four distinct signed transactions. Neither wallet invocation had a method-level trace, so the four new observations add wallet behavior coverage without attributing an RPC. [Round-trip procedure](roundtrip.md)

The private evidence files are `verified-r6/receipt.json` (SHA-256 `bdb21fdfc5e57498c5410371db98f773e42dc3a9203cba24377c2b6e4308420b`), `verified-r7/receipt.json` (`a9ef2384176bb5509073a6b4a4b740c4ef4e67201433f6167612faf95b1f0ef1`), the runner's comparison `paired-r6-r7.json` (`5ccf3b5dfe983340a7a6f2d486eef24de9cf05fd6437a7d8b1e02c4e3b5c7d78`), and `canonical-r6-r7.json` (`faaa579db3101aaf11edb191ea74aee4bdb332f6a0c9732cef2f5facfd4c4c50`). The independent audit binds both receipt hashes, both fixture hashes, and each leg's transaction fingerprint.

## Vizor, zcash-devtool, and Zally payments with Zingo

On three independent Regtest chains, each engine paid a Zingo wallet, received a
payment from a second Zingo wallet, and spent the received funds back. The 18
rows capture what the payer and recipient could observe for each of the nine
signed transfers. Both wallets recorded their balances at the ninth block after
inclusion (ten confirmations). Zingo read back its balance and history offline. [Reproduce and compare the payment roles](payment-roles.md)

| Engine | Evidence and limits |
|---|---|
| Vizor | The confirmed outbound and return transactions reported fees of 25,000 and 10,000 zatoshis, matching both proposals. Its Regtest fixture activated NU6.3 at height 2. |
| zcash-devtool | The Sapling receiver credited the payment and spent it back. Its inbound row retained no memo. Zingo observed the outbound payment pending in this run. Earlier snapshots missed that row. The server returned the release revision and anchor before and after use. All 16 online commands connected only to the selected endpoint. |
| Zally | The observed balance changes imply fees of 10,000 zatoshis for each engine payment. The wallet API exposes balances on reopen. Its sent history, pending state, and fee accessor remain unexercised. |

On one repeat, Zingo showed the incoming Zally payment before mining. On other
repeats it first appeared after mining. The settled credit agreed in every run.

## Spending a note received from another wallet

Two independent campaigns each exercised Zkool 6.37.0 with internal addresses enabled and disabled, and Cake Wallet’s 6.5.4 software Zcash engine. Each ran against ordinary Zallet on its own Regtest chain. The engine paid Zallet, swept its funding change, received 6,000,000 zatoshis from Zallet, and spent 3,000,000 from that incoming note. The histories, confirmed balances, and spendable balances of both parties matched after restart. Each campaign produced 12 distinct canonical payments. The matrix records 24 observations, one for each payer and recipient on each leg. [Repeat the three-chain campaign](zkool-payment.md)

The two Zkool configurations paid 2,000 zatoshis per sending transaction. Cake paid 10,000, and Zallet paid 20,000 on its return payment in each case. The node and Zallet observed all four pending transactions in every case. Zkool and Cake exposed each confirmed payment, but their own pending state stayed unobserved during the sampled intervals. The endpoint traces cover Zkool’s online commands and Cake’s engine sessions. The Zallet evidence records its configured native endpoint. These runs used rebuilt engine binaries, with separate digests from the earlier lifecycle clients. Signed mobile applications and hardware signing remain untested.

## Public Testnet payment

The same Zingo CLI and ordinary Zallet builds completed the [Fauzec-funded round trip](roundtrip.md#run-one-live-fauzec-funded-testnet-case) on v0.22.0. Zingo received 1 TAZ at block 4,647,718 and sent 0.10 TAZ to Zallet at 4,647,786. Zallet spent the received funds and returned 0.04 TAZ at 4,647,798. Both wallets and the local node observed both transactions pending. Each recipient decrypted the expected memo. After the wallets reopened, their confirmed balances were 0.9399 TAZ in Zingo and 0.05975 TAZ in Zallet. A later read after the local Zinder services restarted found the full 0.9399 TAZ spendable in Zingo at height 4,647,805.

The live run uses public confirmation timing. The two isolated chains above supply the repeatable release comparison. The privacy-safe evidence pointers are `testnet/receipt.json` (SHA-256 `8f5c2dcaa933308b59547c1ea1de0a9f8e3923ad539c7e66f4646f832e21f9cc`) and `return-maturity.json` (`3b73fe629f1512dbc64f26a8c6f9970491ff2fdf7917213fd79c2bb5d71ca2e2`).

## Actions with observed RPCs

| Real client action | Observed RPCs and behavior | Wallet assertion |
|---|---|---|
| Zingo Testnet restore, birthday 4,624,102 through block 4,644,091 | The reference and reversed completion-hash arms each made 89 calls. They included `GetSubtreeRoots` six times across pools, as well as block ranges, tree states, transparent history, mined transaction lookup, tip, identity, and an empty mempool subscription. The Sapling stream returned six roots and a terminal empty page. | Both arms scanned 19,990 blocks and 143,049 outputs, recovered the expected history digest and 275,000 spendable Ironwood zatoshis, and reproduced that state on reopening the wallet. The A/B and the pinned Pepper Sync consumer show that this build ignores the 32 bytes of `SubtreeRoot.completingBlockHash`. |
| Zingo limited Sapling pages and malformed root control | One-row pages caused the client to request Sapling indices 0 through 6. The final page was empty. In a separate restore the first populated page carried an invalid `rootHash`. | Pagination recovered the same funded wallet. The malformed root produced the wallet's invalid-root diagnostic before it scanned a block. The CLI exited zero in that failure case, so the assertion reads the diagnostic and scan counts. |
| Zkool internal-enabled rollback on a longer Regtest branch | Exclusive server counters during the first follow observed two `GetBlockRange`, two `GetTreeState`, two `GetTaddressTxids`, and one `GetLatestBlock`, all OK. | Owned notes and history entries both fell from two to one. Ironwood balance fell from 1,000,000,000 to 500,000,000 zatoshis. The first follow rewound the persisted wallet to 262. A second follow reached the replacement tip at 266. A fresh process scanned 267 and retained the corrected balance. |
| Zkool external-only interrupted compact-block recovery | The fixture delivered 64 rows through 4,624,166 and then terminated the real wallet's `GetBlockRange` with `UNAVAILABLE`. The process exited 1. A new process reopened the database and requested 4,624,167 through 4,644,091. | The resumed wallet matched the uninterrupted control at 4,644,091 on its ledger digest, two transactions, one received note, and zero balance. This case exercises a populated wallet with zero current funds. |
| Zallet bounded native scan with a query restart | `ServerInfo`, `TreeStateAtHeight`, and `VisibleTipBlock` were observed during initialization and view acquisition. The old `FullBlocksInRange` returned `FAILED_PRECONDITION / CHAIN_EPOCH_PIN_UNAVAILABLE`. The replacement full-range request succeeded. | The old attempt left block metadata unchanged with work pending. The driver retried the whole interval, persisted block hashes matching the private node at 149 through 151, and cleared the work. The ordinary daemon's lifecycle and the feature-enabled scan driver have separate identities. The row's client digest is the ordinary backend, and its variant names the driver build and feature flags. |

The historical frozen Testnet restore sample also recorded `GetBlockRange`, `GetLatestBlock`, `GetTreeState`, `GetTaddressTxids`, and `GetTransaction` for both Zkool variants. Stable wallet balances and history are separate assertions. Positive method counts alone do not show which response fields produced them. The earlier 18-period comparison that supplied the ztreamer wire witness had zero subtree calls. The witness identifies the byte ordering, while the funded Zingo run supplies the wallet result. [Frozen fixture and witness](subtree-testnet.json)

## Interpreting method coverage

The release protobufs declare 21 lightwalletd methods and 22 native WalletQuery methods. The selected scenarios directly attribute wallet or driver calls to nine lightwalletd and four native methods. The machine matrix lists every declared method for each interface. The formatted view includes real-client observations and direct probes in separate columns.

- **Wallet assertion** means a real client exhibited a measurable effect such as scan progress, pending state, recovered history, or balance. A field-level claim names its supporting check.
- **Method observed** means the scoped trace or counter saw a call. A cancelled empty mempool stream proves an attempted subscription. It provides no incoming-transaction evidence.
- **Contract-only** covers independent probes. For example, the frozen lightwalletd probe matched three-pool frontier digests at 32 heights and compared contiguous compact ranges. Native probes checked the visible tip against the control node and read back the release identity.
- **Uncalled** means the trace monitored the method throughout that action and recorded zero calls. The frozen read-only proxies monitored 18 methods. They did not register `SendTransaction`, `Ping`, or the client-streaming `GetTaddressBalanceStream`. The exclusive server-counter windows positively attribute completed methods, with unknown coverage for omitted or in-flight requests.
- **Unobserved** means this evidence cannot establish method use. For example, historical wallet-pending assertions establish substantive behavior, but their TCP connection traces cannot attribute it to `GetMempoolTx`, `GetMempoolStream`, or native `MempoolEvents`. **Inapplicable** means the method belongs to the other interface. Capability and configuration affect which methods can be served, including all-block retention for native full-block reads, a compatible node for `GetStandardFee`, and explicit enablement for `Ping`. [lightwalletd serving modes](../../docs/reference/lightwalletd-compatibility.md)

Important remaining gaps include mobile or hardware-wallet applications, a second wallet that consumes a populated Sapling subtree response, a field-sensitive wallet that checks completion-hash byte order, populated native subtree pagination, method traces for the eight funded sends, stream recovery in other clients, and longer or deeper wallet-visible forks. These results describe the named builds and fixture conditions.

## Rerun and compare

Run the selected release and client through the [wallet testing runbook](../../docs/runbooks/testing.md). The collector binds each lifecycle manifest and fixture to the gate's pinned inputs. Specialist imports read the private experiment receipts. The shared output contains aggregate counts, assertions, client hashes, and receipt hashes. Keep database contents, transaction IDs, request payloads, traces, and driver logs in the private evidence directory.

```bash
python3 scripts/wallet-clients/coverage.py check
python3 scripts/wallet-clients/coverage.py render > "$evidence/baseline-methods.md"
python3 scripts/wallet-clients/coverage.py collect --release "$release_tag" \
  --subtree-run "$evidence/subtree" --output "$evidence/new-subtree-coverage.json"
python3 scripts/wallet-clients/coverage.py compare \
  --scenario-prefix zingo-subtree- "$evidence/new-subtree-coverage.json"
```

To compare signed payments, import two independently executed Regtest runs. When the pair has a separate canonical block audit, supply it to retain that additional assertion. Omitting it leaves the other checks available and reports the missing independent audit when compared to this baseline.

```bash
python3 scripts/wallet-clients/coverage.py collect --release "$release_tag" \
  --roundtrip-pair "$first_run/receipt.json" "$second_run/receipt.json" \
  --roundtrip-audit "$canonical_audit" --output "$evidence/new-roundtrip-coverage.json"
python3 scripts/wallet-clients/coverage.py compare \
  --scenario-prefix roundtrip- "$evidence/new-roundtrip-coverage.json"
```

A zero exit means the selected baseline scenarios retained their passing assertions, method observations, field evidence, and recorded payment accounting. Exit 1 identifies a loss or incomparable client or fixture. Exit 2 identifies an invalid input. The comparison does not require identical RPC invocation counts across releases. Omit the prefix to compare the entire baseline. Missing scenarios remain visible, including historical measurements for which a candidate has no equivalent receipt.

Additional instruments can emit `--observations FILE` with `schema_version: 1`, the selected release tag and commit, and `scenarios` using the row format in the JSON baseline. Scope the observation to one action and one interface. Supply the SHA-256 of the raw evidence and a privacy-safe description. A positive count records an observed method, `observed_methods` names the methods monitored throughout a partial trace, and `fields` links a field and handling to a named assertion. A `none` method scope has no attributed calls. The three historical restore samples and three independent probes use this import form.

## Payment recovery observed on Regtest

Zingo CLI and the ordinary Zallet daemon agreed on the payer debit, recipient credit, fees, pending observations, confirmed history, and spendable funds after each interruption. Each case used an independent peer-isolated chain and both wallets signed a payment. The five additional matrix rows describe wallet outcomes. RPC method coverage was not measured for these cases.

| Case | What happened to the original payment | Wallet evidence |
| --- | --- | --- |
| Both wallets offline | The node mined it at 262 while Zallet was stopped and Zingo was uninvoked. | Both wallets reopened, caught up, and recorded it once. |
| Shallow rollback | The node dropped block 262. An empty replacement excluded the payment. The fixture explicitly resubmitted the same signed bytes, which were mined at 263. | Both wallets followed the replacement branch and recorded one debit and credit. |
| Native route interrupted | The node mined the payment while the private route to Zallet was down. | The same Zallet process reconnected, reconciled the 10,000,000-zatoshi credit and fixture memo, and signed a 4,000,000-zatoshi return. |

The return payment and a later wallet reopen verified spendability and persistence. The tests leave intermediate orphan display, deeper reorganizations, signed mobile apps, and public TLS behavior unexercised. [Recovery commands and evidence comparison](payment-recovery.md)
