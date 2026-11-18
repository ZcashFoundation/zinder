# Exposing over TLS

Use this runbook to make the lightwalletd-compatible endpoint reachable by
wallets on the internet. You put Caddy in front of
`zinder-compat-lightwalletd`. Caddy terminates TLS on port 443, sends each
new request only while Zinder reports ready, and forwards it to Zinder's
plaintext gRPC listener on loopback.

Zinder has no built-in TLS and does not authenticate wallet clients. TLS,
client access rules, and rate limits belong in the proxy. What must stay
private is described in
[Service operations](../architecture/service-operations.md#security-boundary).

## Prerequisites

- A deployment whose `/readyz` endpoints all return 200, as described in
  [Initial sync](initial-sync.md).
- A DNS name you control, with A and, if you publish IPv6, AAAA records that
  point to the host running Caddy.
- A certificate and private key for that name. The template does not obtain
  or renew certificates. Mobile wallets check the certificate against the
  device's trust store, so use a publicly trusted certificate whose subject
  alternative names include the exact DNS name.
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

The template does three things:

- It serves TLS on `:443` for the exact name, with your certificate, and
  offers only HTTP/2 (`alpn h2`), which gRPC needs. Its `auto_https off`
  setting turns off Caddy's own certificate management and HTTP redirects.
- Before every new gRPC call, `forward_auth` sends `GET /readyz` to the
  compatibility ops port. A 2xx response lets the call through. Any other
  response goes back to the client instead. A not-ready runtime answers 503,
  which a gRPC client reports as `UNAVAILABLE`.
- `reverse_proxy h2c://127.0.0.1:...` forwards admitted calls to the
  plaintext gRPC listener on loopback.

When you renew the certificate, replace the files and reload Caddy.

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

The template enforces neither client authentication nor rate limits. Before
you announce a public endpoint, add a policy in Caddy or in front of it that
limits connections, requests, and concurrent streams per client. Leave room
for normal wallet sync, which streams many blocks in a row.

## Expose the native endpoint too

The repository ships a template only for the compatibility endpoint. To
expose the native `WalletQuery` endpoint, add a second site block with its
own DNS name and certificate. Point its `forward_auth` at the query ops port
(`ZINDER_QUERY_OPS_HOST_PORT`, 9106 on Mainnet) and its `reverse_proxy` at
`h2c://127.0.0.1:` plus the query gRPC port (`ZINDER_QUERY_HOST_PORT`, 9102 on
Mainnet). Verify it the same way, calling
`zinder.v1.wallet.WalletQuery/ServerInfo`. How native clients connect is in
[Integration surfaces](../reference/integration-surfaces.md).

## Supported wallets

A working endpoint does not mean that a particular wallet app works with it.
A named wallet is supported only after end-to-end certification, as described
in
[Lightwalletd compatibility](../reference/lightwalletd-compatibility.md#claim-boundary).
When you test a wallet, configure its endpoint as `https://<your name>:443`
and confirm that it does not fall back to a bundled server.

## Roll back the proxy

If a certificate rotation or a Caddy change breaks the endpoint, restore the
previous certificate files and Caddyfile and reload Caddy. If no trusted
certificate is available, stop serving the public name. Then repeat the
checks in [Verify from outside](#verify-from-outside). A proxy rollback does
not change Zinder's stores.
