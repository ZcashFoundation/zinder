# Upgrading the node without rebuilding history

Use this runbook when a new node release advertises a network upgrade that
your Zinder stores were not built with. For example, Zebra 7.0.0 appends NU7
(branch `0x77190ad9`, Mainnet height 3,543,405, Testnet height 4,465,026) to
the table Zebra 6.2.2 advertises. Zinder keeps its canonical and wallet stores:
the canonical writer admits the new table as a schedule revision instead of
requiring a rebuild.

Schedule admission only lets existing stores open against the new table. Zinder
must also be able to parse blocks after the new upgrade activates; check the
release notes of the Zinder release you are installing for that support.

## Before you start

- Install the same Zinder release for ingest, projector, query, compatibility,
  and Explorer. A reader from an earlier release cannot open a store after the
  writer's first admission.
- If you might need to roll back to an earlier Zinder release, capture a state
  bundle or keep a certified side-by-side lane first. Earlier releases refuse
  the upgraded store with an `unexpected default-column-family key` admission
  error.
- Check that the store's visible tip is below the new upgrade's activation
  height. The writer refuses a new activation at or below the tip, because
  blocks already stored were interpreted without it.

## Upgrade

1. Stop every Zinder service that uses the store: ingest, projector, query,
   compatibility, and Explorer.
2. Upgrade the node and wait until it serves RPC.
3. Compare the node's `getblockchaininfo` `upgrades` with the table the store
   was built with. The new release may append entries or change entries above
   the store tip. Every earlier entry must keep the same branch ID, height,
   name, and order.
4. Start `zinder-ingest`. When it reopens the store, it admits the node's table
   and logs `canonical_ready_store_reopened` with
   `network_upgrade_schedule_revision`. The first admission records revision 1;
   later admissions increment it only when the table changed.
5. Start the readers in dependency order: `zinder-projector`, `zinder-query`,
   then `zinder-compat-lightwalletd` and Explorer. A reader started before
   ingest finishes admission refuses to open the store; restart it after
   ingest is ready.
6. Confirm that `WalletQuery.NetworkUpgradeActivations` returns the new table,
   including the new upgrade's branch ID and height.

Construction identity, canonical rows, the construction manifest, and the
wallet projection are unchanged by admission.

## If ingest refuses the table

Ingest leaves the store unchanged when it refuses a table.

| Store error contains | Meaning | Action |
| --- | --- | --- |
| `does not extend the table this store was built with as an exact prefix` | The store has no admitted revision yet, and the node's table changes, removes, or reorders an entry the store was built with. | Configure the node so its leading entries match the original table exactly, then restart ingest. |
| `adds an activation at or below canonical tip` | The new upgrade activates at or below the store's tip. For example, a Testnet store with blocks at 4,465,026 or above was built from a chain without NU7. | Rebuild the store or restore a bundle whose tip is below the activation height. |
| `differs from admitted schedule revision` | The node's table changes an activation the store has already crossed. | Restore the node's earlier activations for those heights, then restart ingest. |

A reader reports `restart the canonical writer so it admits the node's schedule`
when its node advertises a newer table than the store has admitted. Start or
restart ingest first, then restart the reader.

## Related

- [ADR-0008: network parameter discovery](../adrs/0008-network-parameter-discovery.md#schedule-admission)
- [Deploying on a VM](deploying-on-a-vm.md#restart-and-rollback)
