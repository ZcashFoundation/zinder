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

1. Validate the configuration with the variables set:

   ```bash
   caddy adapt --config deploy/compat-lightwalletd.Caddyfile \
     --adapter caddyfile --validate >/dev/null
   ```

2. Load it into your Caddy service, for example with `caddy reload` or by
   restarting the service.

3. Tell Zinder to trust the client address Caddy forwards. Add this line to
   the env file you pass to Compose, then recreate the compatibility
   container:

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
describes. When that pool is full, a request from a client holding fewer
slots takes a slot from the client holding the most. Caddy reaches Zinder over
a few pooled connections, so without a client address every wallet looks like
the same client, and one wallet that opens many slow requests gets the others
refused with `UNAVAILABLE`.

With the header set and `ZINDER_COMPAT__TRUST_PROXY_CLIENT_ADDRESS=true`,
Zinder counts each wallet connection as its own client, as it does for a
direct connection. One wallet connection that holds many slots then loses its
own oldest slots to other wallets, and refusals fall on it alone.

This guarantee depends on these proxy settings:

- Caddy sets `X-Zinder-Client-Address` to `{http.request.remote}` on every
  forwarded call, replacing any value the client sent. The value must be an
  IP address and port, such as `203.0.113.7:51234` or `[2001:db8::7]:51234`.
  Zinder counts a call without a valid value against Caddy's own connection.
- Only Caddy can reach the gRPC listener. Zinder trusts the header from any
  client once the setting is on, so a client that reaches the listener
  directly could name another wallet's connection. The Compose deployment
  publishes the listener on loopback only.
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
Mainnet). Keep the same `header_up X-Zinder-Client-Address` line, and set
`ZINDER_QUERY__TRUST_PROXY_CLIENT_ADDRESS=true`. Verify it the same way, calling
`zinder.v1.wallet.WalletQuery/ServerInfo`. How native clients connect is in
[Integration surfaces](../reference/integration-surfaces.md).

## Supported wallets

Wallet apps are supported by name, each after end-to-end certification, as
[Lightwalletd compatibility](../reference/lightwalletd-compatibility.md#claim-boundary)
describes.
When you test a wallet, configure its endpoint as `https://<your name>:443`
and confirm that it does not fall back to a bundled server.

## Roll back the proxy

If a certificate rotation or a Caddy change breaks the endpoint, restore the
previous certificate files and Caddyfile and reload Caddy. If no trusted
certificate is available, stop serving the public name. Then repeat the
checks in [Verify from outside](#verify-from-outside). A proxy rollback does
not change Zinder's stores.
