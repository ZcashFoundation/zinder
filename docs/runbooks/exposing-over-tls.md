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
  The template reads them from the files you name. Use a publicly trusted
  certificate whose subject alternative names include the exact DNS name.
  Wallets may use platform roots or bundled WebPKI roots; installing a CA on
  the device does not establish that every wallet will trust it.
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
- Before every new gRPC call, `forward_auth` sends `HEAD /readyz` to the
  compatibility ops port. A 2xx status lets the call through. Caddy returns
  a refusing status to the client. A not-ready runtime answers 503, which a
  gRPC client reports as `UNAVAILABLE`.
- `reverse_proxy h2c://127.0.0.1:...` forwards admitted calls to the
  plaintext gRPC listener on loopback.
- `header_up X-Zinder-Client-Address {http.request.remote}` names, on every
  forwarded call, the wallet connection it came from, as an IP address and
  port. Caddy replaces any value the wallet sent.

When you renew the certificate, replace the files and reload Caddy.

## Verify a local TLS route

For an isolated fixture, prepare a short-lived private CA and a loopback Caddy
route without changing host trust, DNS, or certificate issuance. This reuses
the production readiness and proxy configuration. It does not start Zinder.
Use the fixture owner's gRPC and ops ports and reserve a separate TLS port.
The output directory must not exist; its keys are private to the invoking user.
Prerequisites are Python 3, OpenSSL, Caddy v2, GNU `timeout`, ripgrep, and grpcurl.

```bash
python3 scripts/prepare-wallet-tls-smoke.py --output .tmp/wallet-tls \
  --port 32543 --grpc-port 32567 --ops-port 32507
caddy adapt --config "$PWD/.tmp/wallet-tls/Caddyfile" --adapter caddyfile --validate >/dev/null
XDG_DATA_HOME="$PWD/.tmp/wallet-tls/data" \
XDG_CONFIG_HOME="$PWD/.tmp/wallet-tls/config" \
  caddy run --config "$PWD/.tmp/wallet-tls/Caddyfile" --adapter caddyfile
```

Run Caddy in its own terminal. It binds only `127.0.0.1`, disables its admin
listener and automatic HTTPS, and uses a certificate for `localhost` and
`127.0.0.1` that expires after two days. Stop that process after the test;
remove the scratch directory after retaining the public evidence. Do not
archive `*-key.pem`. The generator also produces an unused client leaf for
indexer tests; this wallet route does not require mutual TLS.

With the fixture frozen and ready, run in a second terminal:

```bash
scripts/verify-wallet-tls.sh .tmp/wallet-tls 32543 32567 .tmp/wallet-tls-receipt
```

Set `GRPCURL_BIN` to an installed binary's absolute path if needed. Exit zero
requires a verified HTTP/2 handshake, a successful `GetLightdInfo` with explicit
CA trust, matching pinned server identity and tip height/hash from direct
gRPC, rejection without CA trust,
rejection of `wrong.invalid` with the correct CA, plaintext rejection on the
TLS listener, and another successful trusted call. The output includes the
certificate identity, tool versions, individual logs, and `result.json`.
`estimatedHeight` is derived from wall-clock time and may differ even on a
frozen fixture. The verifier retains every raw reply and compares the vendor,
version, commit, network, activation height, consensus branch, protocol version,
and transparent support. It separately compares `GetLatestBlock` height and
hash before and after the controls and checks each `GetLightdInfo` block height
against that tip. Rerun on a frozen fixture if the actual tip changes.

The native benchmark client supports the same private CA without changing
platform roots. For example, on a fixture containing block 150:

```bash
zinder-bench lightwalletd-wallet-load --endpoint https://127.0.0.1:32543 \
  --tls-ca-cert .tmp/wallet-tls/ca.pem --tls-domain-name localhost \
  --workload classic-sdk --wallets 1 --from-height 150 --to-height 150
```

A private-CA success proves that route and that client's trust configuration.
It does not certify an unmodified wallet app. Zingolib 8.0.0 at
`81d799775f11e882896e8710e8af12d6d4d29a41` uses `with_webpki_roots()` outside
its unit tests. Zkool v6.37.0 at `d80892f163bc650e3b453f448da8c83eb5c32ddc`
uses tonic's enabled roots with `tls-webpki-roots`. Neither pin exposes a
private-CA override in the tested CLI path. Their `UnknownIssuer` failures
with a private CA, including Zkool with `SSL_CERT_FILE`, do not identify a
Zinder or Caddy defect. An existing endpoint with a certificate trusted by
those roots is needed to qualify their unmodified TLS paths. The operator
must supply its authorized hostname, routing, and certificate, or the wallet
maintainer must provide a supported per-client CA option upstream. Do not
patch out verification or substitute a test-only client build as app evidence.

For Android clients that actually use platform trust, app-scoped trust anchors
are available through [Network Security Configuration](https://developer.android.com/privacy-and-security/security-config).
This does not configure a Rust library's bundled roots. On Apple devices,
[manually installed certificate profiles need explicit SSL trust](https://support.apple.com/en-us/102390).
Testing those paths requires an operator-owned test device or managed profile,
the exact app build, and confirmation of its networking library. Browser
wallets likewise need trust in the selected browser profile and the exact
allowed origin. Leave device and browser trust changes to their owner; do not
modify system-wide roots for this check.

## Attribute wallet traffic and exclude fallback

A selected URL and a successful sync do not establish which endpoint served
the wallet. Record the exact app revision, endpoint selection, certificate
fingerprint, and fresh wallet state. During the whole run, capture connection
attempts from the app process and its children, including failed attempts.
On Linux, an isolated CLI can be observed without recording payloads:

```bash
python3 scripts/trace-wallet-endpoint.py --endpoint-port 32543 \
  --output .tmp/wallet-attribution -- "${wallet_command[@]}"
```

Set the `wallet_command` Bash array to the pinned client and its arguments.
The wrapper requires a new private output directory and writes `connect.log`
and `report.json`. It fails on non-loopback connections, other ports,
unparsed Internet connect calls, no observed connection, or a failed command.
Use a numeric loopback URL so DNS is unnecessary. Its report distinguishes
command failure from the traffic verdict. It observes ordinary Linux process
trees; it cannot attribute a separate pre-existing daemon or rule out traffic
through a local proxy. Review every
IPv4 and IPv6 destination against the configured endpoint and its DNS answers.
For a local fixture, require loopback destinations only, apart from explicitly
identified local resolver traffic. Record the parser or manual review that
produced that conclusion. Repeat with the candidate endpoint stopped: the
wallet must fail or remain pending without progress, and the trace must show
zero attempts to bundled servers. Trace mobile apps and browser workers with
their platform's process or network tooling; a Caddy access log alone cannot
exclude fallback. Starting Caddy and the fixture, stopping the selected endpoint,
and rerunning must be coordinated with their owners.

A lifecycle receipt may set `fallback_used: false` only after this attribution
is complete for that exact app run. The local verifier sets it only for its
single-target grpcurl calls and marks wallet-app attribution unassessed.
Missing device access, endpoint-selection support, an operator-controlled
trusted route, or a complete process trace is an explicit qualification gap.

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
  header only when a request carries it once, as one `ip:port` address or one
  bare IP address, such as `203.0.113.7:51234`, `[2001:db8::7]:51234`, or
  `203.0.113.7`. It counts a request against Caddy's own connection when the
  header is missing, appears more than once, holds a comma-separated list, or
  holds anything that is neither. `zinder_grpc_proxy_client_address_fallbacks_total`
  counts these requests by reason, and stays at zero behind this template. A
  local supervisor's headerless `grpc.health.v1.Health/Check` uses its
  loopback connection for the receive limit and leaves the counter unchanged.
- A proxy that names the client as an `ip:port` keeps Zinder keying the
  connection, so two wallets behind one address stay apart. A proxy that
  names only a bare IP, as GCP Global External Application Load Balancer does
  with `x-zinder-client-address: {client_ip_address}`, has no port to key, so
  Zinder groups every connection from that IP as one client. Set the custom
  header's replace semantics to `true` so the load balancer overwrites any
  value the client sent; if it appends instead, Zinder refuses the value with
  the `repeated` reason and counts the request against the proxy's own
  connection.
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
  The bare-IP proxy header is the one exception: when a proxy names only an
  IP address, every connection from it counts as one client.

Leave the setting off when wallets connect to Zinder without a proxy. With it
off, Zinder ignores the header.

## How readiness reaches wallets

Caddy checks readiness once per new call, using up to 16 connections to the
ops listener. Further checks wait for a connection. Each connection can
serve another check as soon as Caddy receives the status, even while the
admitted wallet call is streaming. This leaves 48 of the
listener's 64 connections available for other ops traffic. Caddy closes idle
connections after 5 seconds, before the listener's 10-second idle deadline.

The first check after readiness turns refusing stops the call at Caddy. If
readiness changes after the check, compatibility checks it again before
running the wallet RPC. An admitted block-range stream continues when
readiness changes.

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

## Enable browser wallets

On the compatibility runtime, set the origins of the wallet sites you serve. To get the exact value, open the wallet page and read `location.origin` in the browser console. For example:

```toml
[compat]
browser_origins = ["https://wallet.example.org"]
```

Restart compatibility and give the wallet developer your public lightwalletd URL. Use the default `wallet` serving mode for a wallet that syncs. The stock Caddy configuration forwards gRPC-Web to the same port as native gRPC. A local development wallet can use the loopback endpoint directly, with its exact development origin in the list, such as `http://localhost:5173`.

To verify, open the wallet at an allowed origin, select your endpoint, and start a sync. In the browser network panel, check that cross-origin preflight succeeds and the wallet sends `GetBlockRange` to your hostname. Confirm that its scanned height advances.

If preflight fails, check the private compatibility `/readyz` first. Caddy checks readiness even for `OPTIONS`, and a browser can report the proxy's refusal as a CORS error. Once the reader is ready, compare the request's `Origin` with the configured value, including its port. If only block reads work, check the serving mode.

The origin list controls browser access. Keep authentication and per-client limits at the public proxy. See [Configuration](../reference/configuration.md) for the environment setting and the explicit wildcard for public wallet endpoints.

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
