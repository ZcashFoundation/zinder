# Upgrading the node

Use this runbook to upgrade Zebra or Zakura under a running Zinder deployment, including node releases that add a network upgrade. Both stores stay intact with no rebuild.

Ingest admits network-upgrade table changes strictly above the store's tip (assigning a new revision number) and refuses changes at or below it ([ADR-0008](../adrs/0008-consensus-parameters-from-the-node.md)).

## Before you start

- Run the same Zinder release across ingest, projector, query, compat, and Explorer.
- Confirm in the release notes that your Zinder release decodes blocks for every upgrade the new node activates (certified runtimes: **Zebra 7.1.0**, **Zakura 1.8.6**).
- Confirm the store's tip (`current_height` on ingest `/readyz`) is below any newly activated upgrade's activation height.
- If you may roll Zinder back to an older release later, plan a separate store for rollback: an older release can refuse a store whose schedule revision was raised (`unexpected default-column-family key`).

## Upgrade the node

1. Stop every Zinder runtime sharing the store (`zinder-ingest`, `zinder-projector`, `zinder-query`, `zinder-compat-lightwalletd`, and `zinder-explorer` if enabled).
2. Upgrade the node and wait until JSON-RPC responds.
3. Verify `upgrades` in the node's `getblockchaininfo`: entries at or below the store's tip must keep the exact same name, branch ID, height, and order; entries above the tip may be added, moved, or removed.
4. Start `zinder-ingest` so it admits the updated schedule on reopen.
5. Start `zinder-projector`, `zinder-query`, `zinder-compat-lightwalletd`, and `zinder-explorer` in order (they read the admitted schedule from the store).

## Verify the upgrade

1. Confirm ingest logs `canonical_ready_store_reopened` (`network_upgrade_schedule_revision` increments if the table changed).
2. Confirm `NetworkUpgradeActivations` returns the new table (Mainnet port shown; Compose adds `+10000` Testnet, `+20000` Regtest):

   ```bash
   grpcurl -plaintext -d '{}' 127.0.0.1:9102 \
     zinder.v1.wallet.WalletQuery/NetworkUpgradeActivations
   ```

3. Confirm every `/readyz` endpoint returns `200`.

## With fallback nodes

Ingest admits a new table only at startup and fails over only to nodes advertising the exact admitted table (names included):

1. Upgrade standby `[node_fallbacks.N]` nodes first. Until ingest restarts, it logs `node_endpoint_skipped` (`schedule_conflict`) for upgraded standbys while continuing on the active node (`active_node.position` on `/readyz`).
2. Upgrade the remaining nodes.
3. Restart `zinder-ingest` while `[node]` is reachable so it admits the updated schedule; all nodes then qualify again.

Readers keep broadcasting through verified nodes during the rollout; once an upgrade activates, broadcasts skip un-upgraded nodes until they are upgraded.

## If ingest refuses the node's table

Ingest exits without modifying the store. Match the error in ingest's logs:

| Error contains | Meaning | Action |
| --- | --- | --- |
| `does not extend the table this store was built with as an exact prefix` | Store has no admitted revision yet and node altered/reordered leading entries | Align node's leading entries with original table, then restart ingest |
| `adds an activation at or below canonical tip` | New upgrade activates at or below the store's tip | Rebuild stores into empty paths ([Initial sync](initial-sync.md)) |
| `differs from admitted schedule revision` | Node altered an activation the store already passed | Restore earlier activations for passed heights, then restart ingest |

A reader exiting with `has no writer-admitted network-upgrade schedule yet` opened a store ingest has not yet opened; start `zinder-ingest` first, then restart the reader.

### Excluded broadcast node

If a reader excludes a broadcast node for a schedule mismatch (`wallet_node_excluded` with `position` and `retry_after_secs`), fix that node; the reader re-verifies it automatically without a restart ([Node source boundary](../architecture/node-source-boundary.md#capabilities)). If no verified broadcast node remains, `/readyz` lists `node_source` under `degraded` (`protocol_mismatch`) and broadcasts return `NODE_UNAVAILABLE`.
