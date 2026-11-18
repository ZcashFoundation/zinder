# Upgrading the node without rebuilding history

Use this runbook when a new node release advertises a network upgrade that
your Zinder stores were not built with. For example, Zebra 7.0.0 appends NU7
(branch `0x77190ad9`, Mainnet height 3,543,405, Testnet height 4,465,026) to
the table Zebra 6.2.2 advertises. Zinder keeps its canonical and wallet stores:
the canonical writer admits the new table as a schedule revision instead of
requiring a rebuild.

Schedule admission only lets existing stores open against the new table. Zinder
must also be able to parse blocks after the new upgrade activates. Check the
release notes of the Zinder release you are installing for that support.

For NU7, Zinder parses blocks with `zebra-chain` 16.0.0 and the matching
librustzcash family, the same parser release Zebra 7.1.0 uses. Zinder 0.9.0
and earlier parse with `zebra-chain` 11.2.0, which rejects branch `0x77190ad9`,
so their ingest stops at the first NU7 block. NU7 decoding is tested against
Regtest blocks captured from Zebra 7.0.0 and Zakura 1.8.6, including Zakura
blocks with Orchard and Ironwood shielding spends on both sides of NU7; see
[Capturing node fixtures](capturing-node-fixtures.md). The live suite also
crosses an NU7 activation on local Zebra 7.1.0 and Zakura 1.8.6 Regtest chains;
see [Regtest through NU7](testing.md#regtest-through-nu7). Zinder has not yet
run across an NU7 activation on Mainnet or on Testnet.

Zinder certifies the node releases that the live and parity lanes run: Zebra
7.1.0 and Zakura 1.8.6. Zebra 7.0.0 advertises the same protocol version and
network upgrades and is expected to keep working, but no lane tests it.
Upgrade Zebra 7.0.0 to 7.1.0 for its security fixes. The upgrade changes no
upgrade table, so it needs neither schedule admission nor a store rebuild.

## Before you start

- Install the same Zinder release for ingest, projector, query, compatibility,
  and Explorer. A reader from an earlier release cannot open a store after the
  writer's first admission.
- If you might need to roll back to an earlier Zinder release, capture a state
  bundle or keep a certified side-by-side lane first. Earlier releases refuse
  the upgraded store with an `unexpected default-column-family key` admission
  error.
- A release whose default reorg window is 1000 blocks also raises existing
  stores on first start unless `reorg_window_blocks` is set explicitly. The
  raise cannot be undone in place either; see
  [Reorg-window sizing](initial-sync.md#reorg-window-sizing).
- Check that the store's visible tip is below the new upgrade's activation
  height. The writer refuses a new activation at or below the tip, because
  blocks already stored were interpreted without it.

## Upgrade

1. Stop every Zinder service that uses the store: ingest, projector, query,
   compatibility, and Explorer.
2. Upgrade the node and wait until it serves RPC.
3. Compare the node's `getblockchaininfo` `upgrades` with the table the store
   uses. Every entry at or below the store tip must keep the same branch ID,
   height, name, and order. What may change above the tip depends on whether
   ingest has already admitted a schedule revision for this store:
   - If this is the first start of a Zinder release with schedule admission,
     the store has no revision yet. The node's table must begin with exactly
     the table the store was built with, in the same order, and may only
     append entries that activate above the store tip. An entry the store was
     built with cannot move yet, even if it has not activated.
   - Once ingest has recorded revision 1, the node's table may add, remove,
     or change any entry that activates above the store tip, including
     entries the store was built with.
4. Start `zinder-ingest`. When it reopens the store, it admits the node's table
   and logs `canonical_ready_store_reopened` with
   `network_upgrade_schedule_revision`. The first admission records revision 1;
   later admissions increment it only when the table changed.
5. Start the readers in dependency order: `zinder-projector`, `zinder-query`,
   then `zinder-compat-lightwalletd` and Explorer. Readers take the table from
   the store, not from the node, so they need no node to start. A reader
   started before a store's first admission waits for it until
   `storage.initial_catchup_timeout_ms` passes (for the projector,
   `projector.fence_convergence_timeout_seconds`); restart it if it gave up
   before ingest was ready.
6. Confirm that `WalletQuery.NetworkUpgradeActivations` returns the new table,
   including the new upgrade's branch ID and height.

Construction identity, canonical rows, the construction manifest, and the
wallet projection are unchanged by admission.

## With a fallback node

When ingest has `[node_fallbacks.N]` endpoints, every endpoint must advertise
exactly the table the store admitted, names included, to take over. While
ingest runs it admits no new table, so upgrade the nodes one at a time:

1. Upgrade a node ingest is not reading from. `/readyz` on ingest reports the
   active endpoint as `active_node.position`. Until the active node is upgraded
   too, the upgraded node advertises a different table, so the standby monitor
   logs `node_endpoint_skipped` with reason `schedule_conflict` for it and
   ingest cannot switch to it.
2. Upgrade the other nodes, then restart ingest. It starts on `[node]` again,
   admits the new table, and every endpoint qualifies again. Restart ingest
   while `[node]` answers: a fallback that would start ingest with a table
   other than the admitted one is refused at startup, so if `[node]` is
   unreachable then and no fallback advertises the admitted table, ingest exits
   and the restart tries again.

Ingest does not switch back on its own, so after a failover it keeps reading
from the fallback until the fallback fails or ingest restarts. Restarting the
node ingest reads from interrupts ingest like any node outage, and ingest
switches only to an endpoint whose table still equals the admitted one.

Query and wallet-mode compatibility keep broadcasting through every node
during the sequence. Their verification compares a node's table with the
admitted one only through the height after the served tip, the block a
broadcast targets, so a node that adds an upgrade further above the tip still
qualifies. When the writer admits the new table, they verify every verified
endpoint again. A node still on the old release once the upgrade activates refuses
transactions for the new consensus branch, so broadcasts move on to the next
endpoint, and the node is excluded as soon as the served tip reaches the block
before the activation height, until it is upgraded.

## If ingest refuses the table

Ingest leaves the store unchanged when it refuses a table.

| Store error contains | Meaning | Action |
| --- | --- | --- |
| `does not extend the table this store was built with as an exact prefix` | The store has no admitted revision yet, and the node's table changes, removes, or reorders an entry the store was built with. | Configure the node so its leading entries match the original table exactly, then restart ingest. |
| `adds an activation at or below canonical tip` | The new upgrade activates at or below the store's tip. For example, a Testnet store with blocks at 4,465,026 or above was built from a chain without NU7. | Rebuild the store or restore a bundle whose tip is below the activation height. |
| `differs from admitted schedule revision` | The node's table changes an activation the store has already crossed. | Restore the node's earlier activations for those heights, then restart ingest. |

A reader whose storage open ends with `has no writer-admitted network-upgrade
schedule yet` started against a store that ingest has never opened. Start
ingest, then restart the reader.

Query and compatibility check each broadcast endpoint's table against the
admitted schedule when they first reach it. An endpoint whose table disagrees
at or below the height after the served tip, for example a node left on
another network, is excluded and never receives a transaction. The reader logs
`wallet_node_excluded` with the endpoint `position` and `retry_after_secs`,
and verifies the endpoint again after that backoff, which starts at 30 seconds
and doubles to 5 minutes while the table keeps disagreeing. Broadcasts keep
going through the other verified endpoints. Only while no endpoint is
verified does `/readyz` list `node_source` under `degraded` with
`node_unavailable` and failure class `protocol_mismatch`, and every broadcast
fail with `NODE_UNAVAILABLE`. Point the endpoint at a node on the store's
chain; the reader verifies it again once the backoff passes, without a
restart.

## Related

- [ADR-0039: writer-admitted network-upgrade schedules](../adrs/0039-writer-admitted-network-upgrade-schedules.md)
- [Deploying on a VM](deploying-on-a-vm.md#restart-and-rollback)
