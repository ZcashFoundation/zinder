# Upgrading the node

Use this runbook to upgrade the Zebra or Zakura node under a running Zinder
deployment, including a node release that adds a network upgrade. You keep
the canonical and wallet stores. Nothing is rebuilt.

Ingest accepts node-table changes above the store's tip and refuses changes
at or below it, and each accepted table gets a new revision number. This is
called admission; see
[ADR-0008](../adrs/0008-consensus-parameters-from-the-node.md).

## Before you start

- Run the same Zinder release for ingest, the projector, query,
  compatibility, and Explorer.
- Check that the Zinder release can decode blocks of every upgrade the new
  node release activates. Admission lets the store open, but ingest still
  stops at the first block it cannot decode. The release notes state which
  upgrades a release supports. Zinder certifies Zebra 7.1.0 and Zakura 1.8.6.
- Check that the store's tip is below the activation height of any new
  upgrade. The `/readyz` body of ingest reports the tip as `current_height`.
- If you might roll back Zinder to an earlier release, plan for a rebuild. An
  earlier release can refuse a store after admission with an
  `unexpected default-column-family key` error, and the remedy is a rebuild
  from empty paths, as
  [Service operations](../architecture/service-operations.md#recovery)
  describes.

## Upgrade the node

1. Stop every Zinder runtime that uses the store: ingest, the projector,
   query, compatibility, and Explorer.
2. Upgrade the node and wait until it answers JSON-RPC.
3. Compare the `upgrades` in the node's `getblockchaininfo` with the table
   the store uses. Entries at or below the store's tip must keep the same
   name, branch ID, height, and order. Entries above the tip may be added,
   removed, or changed.
4. Start `zinder-ingest`. It admits the node's table when it reopens the
   store.
5. Start the other runtimes in order: `zinder-projector`, `zinder-query`,
   then `zinder-compat-lightwalletd` and Explorer. They read the table from
   the store, not from the node.

## Verify the upgrade

1. Ingest logs `canonical_ready_store_reopened`. Its
   `network_upgrade_schedule_revision` field is higher than before when the
   table changed, and unchanged when it did not.
2. Query returns the new table, including the new upgrade's branch ID and
   activation height. The Mainnet port is shown. Under Compose, Testnet adds
   10000 and Regtest adds 20000:

   ```bash
   grpcurl -plaintext -d '{}' 127.0.0.1:9102 \
     zinder.v1.wallet.WalletQuery/NetworkUpgradeActivations
   ```

3. Every `/readyz` endpoint returns 200.

Admission changes only the recorded table. Canonical blocks, the wallet store,
and the store's construction identity stay the same.

## With fallback nodes

Ingest admits a new table only when it starts, and while it runs it switches
only to an endpoint that advertises exactly the admitted table, names
included. Upgrade the nodes one at a time:

1. Upgrade a node that ingest is not reading from. Ingest's `/readyz` names
   the active endpoint in `active_node.position`. Until ingest restarts, it
   logs `node_endpoint_skipped` with reason `schedule_conflict` for the
   upgraded node and does not switch to it.
2. Upgrade the remaining nodes.
3. Restart ingest while `[node]` answers. It starts on `[node]`, admits the
   new table, and every endpoint qualifies again. If `[node]` is unreachable
   at startup and no fallback advertises the admitted table, ingest exits,
   and its restart policy tries again.

Query and compatibility keep broadcasting through every verified node during
this sequence, because a node that adds an upgrade further ahead still passes
their check. Once the upgrade activates, broadcasts move past a node still on
the old release until you upgrade it.

## If ingest refuses the node's table

Ingest leaves the store unchanged when it refuses a table, and exits. Find the
store error in the ingest log:

| Error contains | Meaning | Action |
| --- | --- | --- |
| `does not extend the table this store was built with as an exact prefix` | The store has no admitted revision yet, and the node changes, removes, or reorders an entry the store was built with | Make the node's leading entries match the original table exactly, then restart ingest |
| `adds an activation at or below canonical tip` | A new upgrade activates at or below the store's tip | Rebuild the stores from empty paths |
| `differs from admitted schedule revision` | The node changes an activation the store has already passed | Restore the node's earlier activations for those heights, then restart ingest |

A reader that exits with `has no writer-admitted network-upgrade schedule
yet` opened a store that ingest has never opened. Start ingest, then restart
the reader.

### A broadcast node is excluded

A reader that excludes a broadcast node, for example because its table
disagrees with the admitted one, logs `wallet_node_excluded` with the endpoint
`position` and `retry_after_secs`. Fix the node; the reader verifies it again
on its own, as
[Node source boundary](../architecture/node-source-boundary.md#capabilities)
describes, so no restart is needed.

Broadcasts continue through the other verified nodes. If no node is verified,
`/readyz` lists `node_source` under `degraded` with `node_unavailable` and
failure class `protocol_mismatch`, and every broadcast fails with
`NODE_UNAVAILABLE`.
