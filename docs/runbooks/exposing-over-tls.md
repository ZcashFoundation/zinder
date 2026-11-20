# Exposing over TLS

Use this runbook to make the lightwalletd-compatible endpoint reachable by
wallets on the internet. You put Caddy in front of
`zinder-compat-lightwalletd`. Caddy terminates TLS on port 443, sends each
new request only while Zinder reports ready, and forwards it to Zinder's
plaintext gRPC listener on loopback.

TLS termination, client access rules, and rate limits belong in the proxy,
and Zinder serves plaintext gRPC behind it. Keep private everything that
[Service operations](../architecture/service-operations.md#security-boundary)
lists.

## Prerequisites

- A deployment whose `/readyz` endpoints all return 200, as described in
  [Initial sync](initial-sync.md).
- A DNS name you control, with A and, if you publish IPv6, AAAA records that
  point to the host running Caddy.
- A certificate and private key for that name, which you obtain and renew.
  The template reads them from the files you name. Mobile wallets check the
  certificate against the device's trust store, so use a publicly trusted
  certificate whose subject alternative names include the exact DNS name.
- Caddy v2 on the same host as Zinder, and `grpcurl` on a machine outside it.
- Inbound TCP port 443 open to the internet. Every other Zinder port stays
  closed to the internet.

The Compose deployment publishes the compatibility gRPC listener and its ops
endpoint on loopback, at the host ports in
[What you deploy](deploying.md#what-you-deploy).

## Configure Caddy

The template is `deploy/compat-lightwalletd.Caddyfile`. It reads these
environment variables when Caddy loads it, so set them in Caddy's service
environment:

| Variable | Value |
| --- | --- |
| `ZINDER_COMPAT_PUBLIC_FQDN` | The public DNS name, for example `lightwalletd.example.org` |
| `ZINDER_COMPAT_TLS_CERT_FILE` | Path to the certificate chain |
| `ZINDER_COMPAT_TLS_KEY_FILE` | Path to the private key |
| `ZINDER_COMPAT_HOST_PORT` | The compatibility gRPC host port, 9067 on Mainnet |
| `ZINDER_COMPAT_OPS_HOST_PORT` | The compatibility ops host port, 9107 on Mainnet |

Restrict the private key and its directory to the account that runs Caddy.

Do these steps in order. Caddy must replace the client address header before
Zinder trusts it, because until then any wallet can send its own.

1. Validate the configuration with the variables set:

   ```bash
   caddy adapt --config deploy/compat-lightwalletd.Caddyfile \
     --adapter caddyfile --validate >/dev/null
   ```

2. Load it into your Caddy service, for example with `caddy reload` or by
   restarting the service.

3. Check the requirements in
   [Keep wallets apart in the receive limit](#keep-wallets-apart-in-the-receive-limit),
   then tell Zinder to trust the client address Caddy forwards. Add this line
   to the env file you pass to Compose, then recreate the compatibility
   container with that env file and every `-f` file the deployment runs
   with, as in [Roll back the proxy](#roll-back-the-proxy):

   ```bash
   ZINDER_COMPAT__TRUST_PROXY_CLIENT_ADDRESS=true
   ```

   Outside Compose, set `compat.trust_proxy_client_address = true` in the
   compatibility config file instead. Running the binary with
   `--print-config` shows the resolved value.

The template does four things:

- It serves TLS on `:443` for the exact name, with your certificate, and
  offers only HTTP/2 (`alpn h2`), which gRPC needs. Its `auto_https off`
  setting turns off Caddy's own certificate management and HTTP redirects.
- Before every new gRPC call, `forward_auth` sends `GET /readyz` to the
  compatibility ops port. A 2xx response lets the call through. Any other
  response goes back to the client instead. A not-ready runtime answers 503,
  which a gRPC client reports as `UNAVAILABLE`.
- `reverse_proxy h2c://127.0.0.1:...` forwards admitted calls to the
  plaintext gRPC listener on loopback.
- `header_up X-Zinder-Client-Address {http.request.remote}` names, on every
  forwarded call, the wallet connection it came from, as an IP address and
  port. Caddy replaces any value the wallet sent.

When you renew the certificate, replace the files and reload Caddy.

## Keep wallets apart in the receive limit

Zinder admits at most 32 requests whose message is still arriving, as
[Configuration](../reference/configuration.md#public-grpc-listeners)
describes. When that pool is full, a new request takes the oldest slot of the
client holding the most slots, but only if that client holds strictly more
slots than the requester. Otherwise the new request is refused with
`UNAVAILABLE`. A client that holds no slot is therefore always admitted, and a
client is refused only while it holds at least as many slots as every other
client. When clients hold equal shares, a further request from any of them is
refused.

Caddy reaches Zinder over a few pooled connections. Without a client address,
every wallet behind Caddy counts as one of those few clients, so one wallet
that opens many slow requests gets the others refused.

With the header set and `ZINDER_COMPAT__TRUST_PROXY_CLIENT_ADDRESS=true`,
Zinder counts each wallet connection as its own client, as it does for a
direct connection. A wallet that opens many slow requests then loses its own
oldest slots to wallets holding fewer, and it cannot get a wallet with fewer
slots refused.

This guarantee depends on these settings:

- Caddy sets `X-Zinder-Client-Address` to `{http.request.remote}` on every
  forwarded call, replacing any value the client sent. Zinder reads the
  header only when a request carries it once, as one IP address and port,
  such as `203.0.113.7:51234` or `[2001:db8::7]:51234`. It counts a request
  against Caddy's own connection when the header is missing, appears more
  than once, holds a comma-separated list, or holds anything else.
  `zinder_grpc_proxy_client_address_fallbacks_total` counts these requests by
  reason, and stays at zero behind this template.
- Only Caddy can reach the gRPC listener, apart from peers you trust as much
  as Caddy. Zinder reads the header from every connection once the setting is
  on. A peer that reaches the listener without going through Caddy can name
  another wallet's connection, so that wallet loses its slots or is refused.
  The scan in [Verify from outside](#verify-from-outside) cannot show these
  paths. In the Compose deployment, the listener binds every address of the
  network namespace it shares with ingest, so besides Caddy it is reachable
  from:
  - every process on the host, through the loopback port or the container's
    own addresses;
  - every container on the Compose `node` network, the node's Docker network
    that `ZINDER_SOURCE_NETWORK_NAME` names, including the node;
  - every container on the Compose `node-fallback` network, the fallback
    node's Docker network that `ZINDER_FALLBACK_SOURCE_NETWORK_NAME` names,
    including the fallback node, when the deployment merges
    `deploy/docker-compose.node-fallback.yml`; and
  - every container on the Compose `observability` network, including
    Prometheus and Grafana.

  Before you turn the setting on, confirm that the host runs no untrusted
  users or workloads, and that you run and trust every container on those
  networks. This lists them. Set `env_file` and `compose_files` to the env
  file and the `-f` files the deployment runs with, in the same order:

  ```bash
  env_file=deploy/.env.mainnet
  compose_files=(-f deploy/docker-compose.yml -f deploy/docker-compose.node-fallback.yml)
  ingest="$(docker compose --env-file "$env_file" "${compose_files[@]}" ps -q zinder-ingest)"
  for network in $(docker inspect "$ingest" \
    --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}} {{end}}'); do
    docker network inspect "$network" \
      --format '{{.Name}}: {{range .Containers}}{{.Name}} {{end}}'
  done
  ```

- Zinder identifies wallets by connection, not by IP address, because wallets
  behind Tor share exit addresses. A client that opens many connections still
  counts as many clients, so keep the per-client connection limit described
  in [Add access control and rate limits](#add-access-control-and-rate-limits).

Leave the setting off when wallets connect to Zinder without a proxy. With it
off, Zinder ignores the header.

## How readiness reaches wallets

The readiness check runs once per call. It stops new calls while
compatibility is not ready, for example during startup. It does not end a
stream that has already started. Compatibility also checks its own readiness
on every call, so a call that passes Caddy just as readiness drops still
fails cleanly.

A node outage does not drain compatibility; see
[Readiness](../reference/readiness.md#during-a-node-outage).

## Verify from outside

Run these from a machine outside the host's network. Replace the name with
yours.

1. Check DNS, TLS, and HTTP/2 negotiation:

   ```bash
   name=lightwalletd.example.org
   dig +short A "$name"
   dig +short AAAA "$name"
   openssl s_client -connect "$name:443" -servername "$name" \
     -alpn h2 -verify_hostname "$name" -verify_return_error </dev/null
   ```

   The certificate verifies, and the output contains `ALPN protocol: h2`.

2. Call the endpoint through TLS:

   ```bash
   grpcurl -d '{}' "$name:443" \
     cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo
   ```

   The response names the expected network and reports
   `"taddrSupport": true`.

3. Check that nothing else is reachable. Scan every public address of the host
   from outside. Only port 443 answers. The plaintext gRPC ports, the ops
   ports (9105, 9106, 9107, and 9110 on Mainnet), the private ingest and
   projector control ports (9100 and 9101), Prometheus, and Grafana do not.
   `grpcurl -plaintext "$name:443" list` fails.

A loopback-only port mapping in Compose is not proof on its own. Confirm it
with a scan from outside.

## Add access control and rate limits

Add your client authentication and rate limits before you announce a public
endpoint. Put a policy in Caddy or in front of it that limits connections,
requests, and concurrent streams per client. Leave room for normal wallet
sync, which streams many blocks in a row. Zinder's receive limit tells
connections apart, not addresses, so only a per-client connection limit at
the proxy stops one client from spreading its requests over many
connections.

## Expose the native endpoint too

To expose the native `WalletQuery` endpoint as well, add a second site block
to the template, with its own DNS name and certificate. Point its `forward_auth` at the query ops port
(`ZINDER_QUERY_OPS_HOST_PORT`, 9106 on Mainnet) and its `reverse_proxy` at
`h2c://127.0.0.1:` plus the query gRPC port (`ZINDER_QUERY_HOST_PORT`, 9102 on
Mainnet). Keep the same `header_up X-Zinder-Client-Address` line. Once Caddy
serves the new site, set `ZINDER_QUERY__TRUST_PROXY_CLIENT_ADDRESS=true`
under the same requirements. Verify it the same way, calling
`zinder.v1.wallet.WalletQuery/ServerInfo`. How native clients connect is in
[Integration surfaces](../reference/integration-surfaces.md).

The `ExplorerQuery` endpoint, which runs when the deployment merges
`deploy/docker-compose.explorer.yml`, is exposed the same way. Its site
block points `forward_auth` at the explorer ops port
(`ZINDER_EXPLORER_OPS_HOST_PORT`, 9069 on Mainnet) and `reverse_proxy` at the
explorer gRPC port (`ZINDER_EXPLORER_HOST_PORT`, 9068 on Mainnet). The
explorer listener also runs in ingest's network namespace, so
`ZINDER_EXPLORER__TRUST_PROXY_CLIENT_ADDRESS=true` has the same requirements.
Verify it by calling `zinder.v1.explorer.ExplorerQuery/ServerInfo`.

## Supported wallets

Wallet apps are supported by name, each after end-to-end certification, as
[Lightwalletd compatibility](../reference/lightwalletd-compatibility.md#claim-boundary)
describes.
When you test a wallet, configure its endpoint as `https://<your name>:443`
and confirm that it does not fall back to a bundled server.

## Roll back the proxy

If a certificate rotation or a Caddy change breaks the endpoint, roll back in
this order. A Caddyfile without the `header_up X-Zinder-Client-Address` line
passes on whatever value a wallet sends, so Zinder must stop trusting the
header before Caddy loads such a file.

1. If the Caddyfile you restore does not set `X-Zinder-Client-Address`, turn
   the setting off first. In the env file you pass to Compose, set
   `ZINDER_COMPAT__TRUST_PROXY_CLIENT_ADDRESS=false`. Also set
   `ZINDER_QUERY__TRUST_PROXY_CLIENT_ADDRESS=false` if you expose the native
   endpoint, and `ZINDER_EXPLORER__TRUST_PROXY_CLIENT_ADDRESS=false` if you
   expose the explorer endpoint. Then recreate each container whose setting
   you changed. Give Compose the same env file and the same `-f` files, in
   the same order, that the deployment runs with. With fewer files, Compose
   recreates the container without what the missing overlays add, such as
   the fallback node's settings and cookie mount from
   `deploy/docker-compose.node-fallback.yml`. For a Mainnet deployment that
   merges that overlay:

   ```bash
   env_file=deploy/.env.mainnet
   compose_files=(-f deploy/docker-compose.yml -f deploy/docker-compose.node-fallback.yml)
   docker compose --env-file "$env_file" "${compose_files[@]}" \
     up -d zinder-compat-lightwalletd # and zinder-query or zinder-explorer, if you changed them
   ```

   Outside Compose, set `trust_proxy_client_address = false` and restart each
   runtime. Check the resolved value with `--print-config` before you go on.
2. Restore the previous certificate files and Caddyfile, and reload Caddy.
3. If no trusted certificate is available, stop serving the public name.
4. Repeat the checks in [Verify from outside](#verify-from-outside).

A proxy rollback does not change Zinder's stores.
