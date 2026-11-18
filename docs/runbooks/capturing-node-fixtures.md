# Capturing node fixtures

Maintainers use this procedure to record the Zakura Regtest blocks that
Zinder's integration tests decode, for example after the certified Zakura
release changes. CI only reads the checked-in fixtures.

The capture writes three fixtures to `services/zinder-ingest/tests/fixtures/`.
Each block holds a shielding spend built with a real Halo2 proof:

| Fixture | Consensus branch | Transactions besides the coinbase |
| --- | --- | --- |
| `zakura-1.8.6-regtest-orchard-block-111.json` | NU6.2, `0x5437f330` | A version-5 transparent-to-Orchard shielding spend |
| `zakura-1.8.6-regtest-ironwood-block-132.json` | NU6.3, `0x37a5165b` | A version-6 transparent-to-Ironwood shielding spend and a version-6 transparent spend |
| `zakura-1.8.6-regtest-nu7-block-162.json` | NU7, `0x77190ad9` | The same two spends as the NU6.3 block |

The file names carry the Zakura version the node reports, so a capture from
another release writes new files.

The node runs
[`deploy/zakura/regtest-fixture-capture.toml`](../../deploy/zakura/regtest-fixture-capture.toml),
not the live-test schedule. It activates NU6.3 at height 130 and NU7 at 160.
Orchard accepts new value only before NU6.3, and a coinbase output matures
after 100 blocks, so NU6.3 comes late enough to shield an early coinbase into
Orchard. Every block pays the unified address of the `zinder-testkit`
broadcast test seed.

## Before you start

- Install Docker, `cargo`, `curl`, and `jq`.
- Use the Zakura image pinned in `scripts/check-zcash-upstream-alignment.sh`,
  by digest.
- Free the loopback ports the TOML uses: JSON-RPC `39232`, P2P `38233`, and
  QUIC `38234`. The live-test Zakura node uses the same ports. To run beside
  it, override `ZAKURA_RPC__LISTEN_ADDR`, `ZAKURA_NETWORK__LISTEN_ADDR`, and
  `ZAKURA_NETWORK__ZAKURA__LISTEN_ADDR` on `127.0.0.1`.

## Capture the fixtures

1. From the repository root, start a fresh node. Its state is ephemeral, so
   each start begins from an empty chain. Running `zakurad` as your user
   skips the image entrypoint, which would drop root to UID 10001.

   ```bash
   zakura_image=docker.io/zakuracore/zakura:1.8.6@sha256:e514c89b751b685686690098928507aaa994311e6b90c79373c55b0afaa45d50
   docker run -d --name zakura-fixture-capture --network host \
     --user "$(id -u):$(id -g)" --entrypoint /usr/local/bin/zakurad \
     -v "$PWD/deploy/zakura/regtest-fixture-capture.toml:/etc/zakura/zakurad.toml:ro" \
     "$zakura_image" --config /etc/zakura/zakurad.toml start
   ```

2. Run the capture script with the node's URL and the same image reference.
   An optional third argument sets another output directory.

   ```bash
   scripts/capture-zakura-shielded-fixtures.sh http://127.0.0.1:39232 "$zakura_image"
   ```

   The script refuses a node whose chain is not empty or whose NU6.3 and NU7
   heights differ from the capture schedule. It mines the chain, signs each
   spend with the `sign_test_spend` example, broadcasts it, and checks each
   captured block's contents. A successful run prints `wrote <path>` for each
   of the three fixtures.

3. Remove the node:

   ```bash
   docker rm -f zakura-fixture-capture
   ```

If Zakura rejects a shielding spend, the script stops and prints the node's
response. Do not change the transaction to get past it. A rejected spend that
the upstream transaction builder produced is a compatibility finding to
report.

## Check and commit

Every capture produces new transaction IDs and block hashes, because the
shielding spends use fresh randomness.

1. Run the fixture tests:

   ```bash
   cargo nextest run -p zinder-source -p zinder-ingest \
     -E 'test(/nu7_activation_boundary|fixture_block/)'
   ```

2. `historical_fixture_blocks_keep_known_facts_and_compact_block_bytes` pins
   each fixture's canonical-facts digest and compact-block hash, so it fails
   after a capture. Once the other fixture tests pass, replace its known
   answers with the values the failure reports.
3. The canonical-facts tests in
   `services/zinder-ingest/tests/integration/fixture_block.rs` list each
   transaction's Orchard and Ironwood actions in block order. Zakura orders a
   block's non-coinbase transactions itself, so check that order against the
   new fixture.
4. If the Zakura version changed, point the `include_str!` paths in
   `crates/zinder-source/tests/integration/nu7_activation_boundary.rs` and
   `services/zinder-ingest/tests/integration/fixture_block.rs` at the new
   files, delete the old ones, and update the tested versions in
   [Upgrading the node](upgrading-the-node.md).
