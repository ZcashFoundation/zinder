# Exposing over TLS

Use this runbook to expose Zinder's lightwalletd-compatible, native `WalletQuery`, or `ExplorerQuery` gRPC listeners behind Caddy on port `443`. Caddy terminates TLS, gates every new call on `/readyz`, forwards HTTP/2 (`h2c`) traffic to Zinder on host loopback, and replaces `X-Zinder-Client-Address` so Zinder's per-connection limits isolate individual wallets.

Zinder serves plaintext gRPC only; keep all gRPC, ops, control, and observability ports private as listed in the [security boundary](../architecture/service-operations.md#security-boundary).

## Production proxy setup

### Prerequisites

- All `/readyz` endpoints return `200` ([Initial sync](initial-sync.md)) on the host loopback ports in [Processes and canonical ports](deploying.md#what-you-deploy).
- A public DNS name (`A` and optional `AAAA` records) pointing to the Caddy host, and a certificate/key chain trusted by WebPKI/platform roots for that exact FQDN. (Installing a private CA on a device does not establish that mobile or bundled-root wallets will trust it.)
- Caddy v2 on the Zinder host and `grpcurl` on an external host. Allow inbound TCP `443` for HTTP/2 and UDP `443` for HTTP/3 through the host firewall and any network firewall, port forward, or container port mapping in front of Caddy. Keep all other Zinder ports closed externally.

### Configure Caddy

`deploy/compat-lightwalletd.Caddyfile` reads these environment variables from Caddy's service environment (restrict the private key and its directory to the Caddy account):

| Variable | Value |
| --- | --- |
| `ZINDER_COMPAT_PUBLIC_FQDN` | Public DNS name, e.g. `lightwalletd.example.org` |
| `ZINDER_COMPAT_TLS_CERT_FILE` | Path to certificate chain |
| `ZINDER_COMPAT_TLS_KEY_FILE` | Path to private key |
| `ZINDER_COMPAT_HOST_PORT` | Compatibility gRPC host port (`9067` Mainnet) |
| `ZINDER_COMPAT_OPS_HOST_PORT` | Reader ops `/readyz` host port (`9108` under `zinder-serve` / Compose, or `9107` standalone) |

Apply in this exact order so Caddy strips client-supplied headers **before** Zinder trusts `X-Zinder-Client-Address`:

1. Validate the Caddyfile:

   ```bash
   caddy adapt --config deploy/compat-lightwalletd.Caddyfile \
     --adapter caddyfile --validate >/dev/null
   ```

2. Load the config into Caddy (e.g. `caddy reload` or restart the Caddy service).
3. Verify the isolation requirements in [Keep wallets apart in the receive limit](#keep-wallets-apart-in-the-receive-limit), add `ZINDER_COMPAT__TRUST_PROXY_CLIENT_ADDRESS=true` to your Compose env file (or `compat.trust_proxy_client_address = true` outside Compose), and recreate `zinder-serve` (or standalone `zinder-compat-lightwalletd`) using the deployment's full env file and `-f` list.

The template sets `auto_https off` and uses the supplied certificate. When renewing certificates, replace the files and reload Caddy.

Caddy advertises HTTP/3 on responses over HTTP/2 with an `Alt-Svc` header containing `h3=":443"`. A client that supports QUIC connection migration can keep an active HTTP/3 stream open when a mobile device switches between Wi-Fi and cellular, provided the new path can reach the same Caddy instance. Clients that fall back to HTTP/2 can reach the endpoint over TCP `443` when UDP is blocked. Native gRPC clients that use HTTP/2 continue to negotiate `h2`. [HTTP/3 discovery](https://www.rfc-editor.org/rfc/rfc9114#section-3.1), [QUIC connection migration](https://www.rfc-editor.org/rfc/rfc9000#section-9).

## Keep wallets apart in the receive limit

Zinder admits at most 32 requests whose message is still arriving ([Public gRPC listeners](../reference/configuration.md#public-grpc-listeners)). When full, a new request evicts the oldest slot of the client holding the most slots (strictly more than the requester) or fails with `UNAVAILABLE`.

Because Caddy multiplexes wallets onto a few pooled `h2c` connections, Zinder would otherwise treat every wallet behind Caddy as one client. With `ZINDER_COMPAT__TRUST_PROXY_CLIENT_ADDRESS=true` and Caddy setting `X-Zinder-Client-Address` to `{http.request.remote}`, Zinder keys each wallet connection independently:

- Caddy must replace `X-Zinder-Client-Address` on every forwarded call as one `ip:port` or bare IP. Missing, repeated, comma-separated, or malformed values fall back to Caddy's own connection and increment `zinder_grpc_proxy_client_address_fallbacks_total` (headerless supervisor `grpc.health.v1.Health/Check` probes use loopback and do not increment it). A proxy forwarding only a bare IP (such as GCP Global External Application Load Balancer) groups all connections from that IP as one client; ensure its replace semantics are `true` so it never appends.
- Only Caddy (and equally trusted peers) may reach the listening port directly, because Zinder trusts the header from any caller once enabled. In the Compose deployment, every public listener shares `zinder-index`'s network namespace and is directly reachable from:
  - host processes via loopback or container addresses;
  - every container on the Compose `node` network (`ZINDER_SOURCE_NETWORK_NAME`, including the node and any external fallback node networks you attach); and
  - every container on the Compose `observability` network (including Prometheus and Grafana).

Before enabling `TRUST_PROXY_CLIENT_ADDRESS`, confirm the host runs no untrusted workloads and inspect every attached container with the exact env file and `-f` files your deployment runs:

```bash
env_file=deploy/.env.mainnet
compose_files=(-f deploy/docker-compose.yml)
ingest="$(docker compose --env-file "$env_file" "${compose_files[@]}" ps -q zinder-index)"
for network in $(docker inspect "$ingest" \
  --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}} {{end}}'); do
  docker network inspect "$network" \
    --format '{{.Name}}: {{range .Containers}}{{.Name}} {{end}}'
done
```

- Because wallets behind Tor share exit IPs, Zinder keys connections (`ip:port`), not IPs (except bare-IP proxy headers). Enforce per-client connection limits at your proxy as well.

Leave `TRUST_PROXY_CLIENT_ADDRESS` off (`false`) when wallets connect directly without a proxy.

## How readiness reaches wallets

Caddy checks `HEAD /readyz` once per new gRPC call (16 pooled connections, leaving 48 of the 64 ops connections free; 5s idle close before Zinder's 10s idle deadline). A `503` causes Caddy to reject the call (`UNAVAILABLE` to gRPC clients); once admitted, an active block-range stream continues if readiness later changes, while `zinder-compat-lightwalletd` re-checks readiness before starting the RPC handler. Node outages do not drain compatibility ([During a node outage](../reference/readiness.md#during-a-node-outage)).

## Add access control and rate limits

Before announcing a public endpoint, configure client authentication and per-client limits on connections, requests, and concurrent streams in Caddy or your edge gateway. Leave headroom for normal wallet sync bursts; per-client connection limits at the proxy prevent one client from opening many connections to bypass Zinder's per-connection receive limit.

## Enable browser wallets

Set `compat.browser_origins` (`ZINDER_COMPAT__BROWSER_ORIGINS`) to the exact wallet `location.origin` values (e.g. `["https://wallet.example.org"]`, or `http://localhost:5173` for local development) and restart compatibility. Caddy forwards gRPC-Web on the same port as native gRPC. Keep `compat.serving = "wallet"` (default) or `"light-wallet"` for syncing wallets, and keep edge rate limits in the proxy.

Browsers that negotiate `h3` can carry gRPC-Web calls to Caddy over HTTP/3. Caddy forwards the same gRPC-Web requests to Zinder over `h2c`. In the browser network panel, check that CORS preflight succeeds, the Protocol column shows `h3` after the browser discovers HTTP/3, and `GetBlockRange` advances scanned height. With UDP `443` blocked on the test client, repeat the call and confirm it succeeds over `h2`.

If preflight fails, check `/readyz` first (Caddy runs `forward_auth` on `OPTIONS` too, which browsers report as CORS failures), then compare the exact `Origin` header including port. See [Configuration](../reference/configuration.md) for the wildcard option.

## Expose the native endpoint too

To expose `WalletQuery` or `ExplorerQuery` (with `--preset explorer` / `ZINDER_PRESET=explorer`), add a site block per service to the Caddyfile with its own DNS name, certificate, `forward_auth` ops port, `reverse_proxy h2c://127.0.0.1:<port>`, and `header_up X-Zinder-Client-Address {http.request.remote}`:

- **Native `WalletQuery`:** `ZINDER_QUERY_OPS_HOST_PORT` (`9108` under `zinder-serve`, or `9106` standalone) and `ZINDER_QUERY_HOST_PORT` (`9102` Mainnet); once Caddy reloads, set `ZINDER_QUERY__TRUST_PROXY_CLIENT_ADDRESS=true` and verify `zinder.v1.wallet.WalletQuery/ServerInfo`. See [Integration surfaces](../reference/integration-surfaces.md).
- **`ExplorerQuery`:** `ZINDER_EXPLORER_OPS_HOST_PORT` (`9108` under `zinder-serve`, or `9069` standalone) and `ZINDER_EXPLORER_HOST_PORT` (`9068` Mainnet); once Caddy reloads, set `ZINDER_EXPLORER__TRUST_PROXY_CLIENT_ADDRESS=true` and verify `zinder.v1.explorer.ExplorerQuery/ServerInfo`.

## Verify from outside

Run from a machine outside the host's network:

1. Check DNS, TLS, and HTTP/2 ALPN (`ALPN protocol: h2`):

   ```bash
   name=lightwalletd.example.org
   dig +short A "$name"
   dig +short AAAA "$name"
   openssl s_client -connect "$name:443" -servername "$name" \
     -alpn h2 -verify_hostname "$name" -verify_return_error </dev/null
   ```

2. Call `GetLightdInfo` over TLS (expect your network and `"taddrSupport": true`):

   ```bash
   grpcurl -d '{}' "$name:443" \
     cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo
   ```

3. With a curl build that reports `HTTP2` and `HTTP3` in `curl --version`, check that the first command reports `HTTP/2` and an `Alt-Svc` header containing `h3=":443"`. The second must report `HTTP/3`. These probes check the edge transport. Use the RPC and browser checks to verify wallet calls.

   ```bash
   curl --http2 --max-time 10 -sS -D - -o /dev/null \
     -w 'HTTP/%{http_version}\n' "https://$name/"
   curl --http3-only --max-time 10 -sS -o /dev/null \
     -w 'HTTP/%{http_version}\n' "https://$name/"
   ```

   If HTTP/3 fails while HTTP/2 succeeds, check UDP `443` on each firewall, forwarder, and port mapping. To test fallback, block UDP `443` on the test client and repeat the second command with `--http3` in place of `--http3-only`. It should report `HTTP/2`.

4. Scan every public IP of the host. Confirm only port `443` answers on TCP and UDP and that `grpcurl -plaintext "$name:443" list` fails. All private gRPC, ops, control, and observability ports must be unreachable.

## Supported wallets

Wallet apps are supported by name after end-to-end certification ([Claim boundary](../reference/lightwalletd-compatibility.md#claim-boundary)). When testing an app, set its endpoint to `https://<your name>:443` and confirm it does not fall back to a bundled server.

## Roll back the proxy

If a Caddy change or certificate rotation breaks serving, roll back in this order so Zinder stops trusting `X-Zinder-Client-Address` **before** Caddy loads a config that omits `header_up`:

1. Set `ZINDER_COMPAT__TRUST_PROXY_CLIENT_ADDRESS=false` (and `ZINDER_QUERY__TRUST_PROXY_CLIENT_ADDRESS=false` / `ZINDER_EXPLORER__TRUST_PROXY_CLIENT_ADDRESS=false` if enabled) in your env file, then recreate the reader service using your full env file and `-f` list:

   ```bash
   env_file=deploy/.env.mainnet
   compose_files=(-f deploy/docker-compose.yml)
   docker compose --env-file "$env_file" "${compose_files[@]}" \
     up -d zinder-serve
   ```

   Outside Compose, set `trust_proxy_client_address = false`, restart each runtime, and confirm with `--print-config`.
2. Restore the previous certificate files and Caddyfile, and reload Caddy.
3. If no valid certificate is available, stop serving the public name.
4. Repeat [Verify from outside](#verify-from-outside).

A proxy rollback does not alter Zinder's stores.

---

## Local private-CA smoke test and traffic attribution

### Verify a local TLS route

For an isolated local fixture, generate a short-lived (2-day) private CA and loopback Caddy config for `localhost` / `127.0.0.1` without touching system trust roots. Prerequisites: Python 3, OpenSSL, Caddy v2, GNU `timeout`, `ripgrep`, and `grpcurl`.

```bash
python3 scripts/prepare-wallet-tls-smoke.py --output .tmp/wallet-tls \
  --port 32543 --grpc-port 32567 --ops-port 32507
caddy adapt --config "$PWD/.tmp/wallet-tls/Caddyfile" --adapter caddyfile --validate >/dev/null
XDG_DATA_HOME="$PWD/.tmp/wallet-tls/data" \
XDG_CONFIG_HOME="$PWD/.tmp/wallet-tls/config" \
  caddy run --config "$PWD/.tmp/wallet-tls/Caddyfile" --adapter caddyfile
```

In a second terminal against a frozen fixture, verify handshake, CA trust, SNI rejection, and plaintext rejection:

```bash
scripts/verify-wallet-tls.sh .tmp/wallet-tls 32543 32567 .tmp/wallet-tls-receipt
```

Test `zinder-bench` against the private CA directly:

```bash
zinder-bench lightwalletd-wallet-load --endpoint https://127.0.0.1:32543 \
  --tls-ca-cert .tmp/wallet-tls/ca.pem --tls-domain-name localhost \
  --workload classic-sdk --wallets 1 --from-height 150 --to-height 150
```

Stop Caddy and delete `.tmp/wallet-tls` (`*-key.pem`) once finished. Note that unmodified apps bundling `webpki-roots` (such as Zingolib 8.0.0 or Zkool v6.37.0 CLI paths) reject private CAs with `UnknownIssuer`; qualifying unmodified apps requires a publicly trusted cert or a client CA flag, while Android ([Network Security Configuration](https://developer.android.com/privacy-and-security/security-config)) and Apple ([manually installed certificate profiles](https://support.apple.com/en-us/102390)) platform-trust paths require device-owner trust configuration.

### Attribute wallet traffic and exclude fallback

Verify a Linux CLI wallet connects only to your fixture and does not fall back to bundled endpoints:

```bash
python3 scripts/trace-wallet-endpoint.py --endpoint-port 32543 \
  --output .tmp/wallet-attribution -- "${wallet_command[@]}"
```

The wrapper records `connect.log` and `report.json` and fails on non-loopback or unexpected port connections (trace mobile/browser apps with their platform's network tooling). Repeat with the candidate endpoint stopped to confirm zero fallback attempts before recording `fallback_used: false` on a lifecycle receipt.
