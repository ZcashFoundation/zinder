# Verify the Cipherscan adapter

Use this procedure to confirm that an unchanged Cipherscan frontend works against `zinder-compat-cipherscan`, after you deploy the adapter or change it or the Explorer. The [coverage matrix](../architecture/cipherscan-adapter.md) says what each route should return.

## Prerequisites

- Run `zinder serve --preset explorer` (which enables `[ingest].explorer_views = true` and starts `zinder-explorer`, `zinder-query`, and `zinder-compat-cipherscan` together), or run `zinder-compat-cipherscan` (`zinder run compat-cipherscan`) pointing at running `zinder-explorer` and `zinder-query` instances ([Explorer plane](../architecture/explorer-plane.md)).
- The adapter listens for REST on `127.0.0.1:9070` and exposes `/healthz` and `/readyz` on `127.0.0.1:9108` (`zinder serve` aggregates `/healthz` and `/readyz` across all roles on `:9108`). Adjust the commands below if you changed them.
- A Cipherscan checkout whose API base URL points at the adapter.

## Steps

1. Check that the adapter is alive and connected:

   ```bash
   curl --fail http://127.0.0.1:9108/healthz
   curl --fail http://127.0.0.1:9108/readyz
   ```

   `/readyz` answers 200 once the adapter has connected to both gRPC endpoints. It does not prove that any view covers a requested range.

2. Probe one data-bearing route from each family the matrix lists as working:

   ```bash
   curl --fail http://127.0.0.1:9070/api/info
   curl --fail http://127.0.0.1:9070/api/mempool
   curl --fail http://127.0.0.1:9070/api/network/fees
   curl --fail http://127.0.0.1:9070/api/rich-list
   curl --fail http://127.0.0.1:9070/api/pools/flows
   ```

   Each must return 200 with Cipherscan's JSON shape. A body with `"degraded": true` is acceptable only where the matrix lists that route as degraded.

3. Probe one route from each family the matrix lists as failing, and confirm the documented status. For example, `/api/blocks` returns 502 with `"code": "upstream_error"`, and `/api/mining/pool-distribution` returns 503 with `"code": "upstream_field_unavailable"`. A failing route must never return 200 with zeroes or an empty list.

4. Open the Cipherscan frontend and load each page whose routes work: home and chain status, one transaction, the mempool, one transparent address and the rich list, supply, value pools, and privacy statistics. Each page must leave its loading state and show real content, with no new application error.

The adapter passes when every check matches the matrix. Panels the matrix marks as sidecar-owned, unavailable, or failing are outside this check.

## If a check fails

- **The adapter exits at startup.** It connects to both gRPC endpoints before it binds its listener. Start `zinder-explorer` and `zinder-query` first, then restart the adapter.
- **503 with `upstream_unavailable`.** The Explorer or `zinder-query` is down or unreachable. Check their `/readyz`.
- **503 with `upstream_field_unavailable` on a route the matrix lists as working.** The Explorer does not advertise the capability yet, usually because its views are still building. Wait for view replay to reach the tip, then retry.
- **502 with `upstream_error` on a route the matrix lists as working.** The Explorer rejected the call. Check the Explorer log for the method and its error reason in [Error vocabulary](../reference/error-vocabulary.md).
