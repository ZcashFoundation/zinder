# Capturing node fixtures

Use this runbook to record the Zakura Regtest blocks that Zinder's T1 tests
decode, for example when the certified Zakura release changes. A maintainer
runs it by hand; CI only reads the checked-in fixtures.

The capture produces three fixtures under
`services/zinder-ingest/tests/fixtures/`. Each block holds a shielding spend
that `zinder-testkit` builds with a real Halo2 proof:

| Fixture | Consensus branch | Transactions besides the coinbase |
| --- | --- | --- |
| `zakura-1.8.6-regtest-orchard-block-111.json` | NU6.2, `0x5437f330` | A version-5 transparent-to-Orchard shielding spend |
| `zakura-1.8.6-regtest-ironwood-block-132.json` | NU6.3, `0x37a5165b` | A version-6 transparent-to-Ironwood shielding spend and a version-6 transparent spend |
| `zakura-1.8.6-regtest-nu7-block-162.json` | NU7, `0x77190ad9` | The same two spends as the NU6.3 block |

The file names carry the Zakura version that the node reports, so a capture
from another release writes new files instead of overwriting these.

## The capture chain

The node runs [`deploy/zakura/regtest-fixture-capture.toml`](../../deploy/zakura/regtest-fixture-capture.toml),
not the live-lane schedule in `deploy/zakura/regtest.toml`. It activates NU6.3
at height 130 and NU7 at 160. Orchard accepts new value only before NU6.3, and
a coinbase output can be spent only after 100 blocks, so NU6.3 must come late
enough for an early coinbase output to be shielded into Orchard first.

The node mines every block to the unified address of the broadcast test seed
in `zinder-testkit`. That address has a P2PKH receiver and an Orchard
receiver:

- Before NU6.3, Zakura pays the coinbase to the P2PKH receiver. The capture
  spends these outputs.
- From NU6.3 onward, Zakura pays the miner reward as an Ironwood note to the
  Orchard receiver.

A `zinder-testkit` unit test fails if the TOML mines to any other address.

## Before you start

- Install Docker, `cargo`, `curl`, and `jq`.
- Use the Zakura image pinned in `scripts/check-zcash-upstream-alignment.sh`,
  by digest. The script records the image reference in each fixture.
- Make sure no other process listens on the TOML's loopback ports: JSON-RPC
  `39232`, legacy P2P `38233`, and QUIC `38234`. The live-lane Zakura node
  uses the same ports. To run beside it, override each listener with
  `ZAKURA_RPC__LISTEN_ADDR`, `ZAKURA_NETWORK__LISTEN_ADDR`, and
  `ZAKURA_NETWORK__ZAKURA__LISTEN_ADDR`, all on `127.0.0.1`.

## Capture the fixtures

From the repository root, start a fresh Zakura node with the capture
configuration. The state is ephemeral, so every start begins from an empty
chain. The container runs on the host network because every listener binds
loopback. Running `zakurad` as your own user skips the image entrypoint, which
would drop root to UID 10001.

```bash
zakura_image=docker.io/zakuracore/zakura:1.8.6@sha256:e514c89b751b685686690098928507aaa994311e6b90c79373c55b0afaa45d50
docker run -d --name zakura-fixture-capture --network host \
  --user "$(id -u):$(id -g)" --entrypoint /usr/local/bin/zakurad \
  -v "$PWD/deploy/zakura/regtest-fixture-capture.toml:/etc/zakura/zakurad.toml:ro" \
  "$zakura_image" --config /etc/zakura/zakurad.toml start
```

Run the capture script with the node's JSON-RPC URL and the same image
reference. To write somewhere other than the fixture directory first, pass an
output directory as a third argument.

```bash
scripts/capture-zakura-shielded-fixtures.sh http://127.0.0.1:39232 "$zakura_image"
```

The script refuses a node whose chain is not empty or whose NU6.3 and NU7
heights differ from the capture schedule. It then works through these steps:

1. Mines to height 110 and spends the oldest matured coinbase output into one
   Orchard note.
2. Mines block 111 and checks that the block holds exactly that spend.
3. Mines to height 131, broadcasts an Ironwood shielding spend and a
   transparent spend, and mines block 132.
4. Mines to height 161 and repeats the two spends in block 162.
5. Writes each block's fixture from `getblock(hash,0)`, `getblock(hash,1)`,
   and `getrawtransaction(txid,1)`, with a provenance paragraph that names the
   date, the node build, the image, and the schedule.

The `sign_test_spend` example in `zinder-testkit` signs each spend, and the
script broadcasts it with `sendrawtransaction`. Building one shielding proof
takes a few seconds. A successful run prints `wrote <path>` for each of the
three fixtures.

Then remove the node:

```bash
docker rm -f zakura-fixture-capture
```

## Check and commit

The shielding spends use fresh randomness, so every capture produces new
transaction IDs and block hashes, even on the same release.

Run the fixture tests:

```bash
cargo nextest run -p zinder-source -p zinder-ingest \
  -E 'test(/nu7_activation_boundary|fixture_block/)'
```

Then update what a new capture changes:

1. `historical_fixture_blocks_keep_known_facts_and_compact_block_bytes` pins
   the canonical-facts digest and compact-block hash of each fixture, so it
   fails after a new capture. Replace those known answers with the values the
   failure reports, after the other fixture tests pass.
2. The canonical-facts tests in
   `services/zinder-ingest/tests/integration/fixture_block.rs` list the
   Orchard and Ironwood actions of each transaction in block order. Zakura
   orders a block's non-coinbase transactions itself, so check that order
   against the new fixture.
3. If the Zakura version changed, point the `include_str!` paths in
   `crates/zinder-source/tests/integration/nu7_activation_boundary.rs` and
   `fixture_block.rs` at the new files, delete the old ones, and update the
   tested versions in [Upgrading the node](upgrading-the-node.md).

If Zakura rejects a shielding spend, the script stops and prints the node's
response. Do not adjust the transaction to get past it. A rejection of a
spend that the upstream transaction builder produced is a compatibility
finding to report.
