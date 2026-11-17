# Trusted TLS and ZODL compatibility admission

This runbook prepares and evaluates one TLS route for an isolated
`zinder-compat-lightwalletd` candidate. An Android-trusted internal route can
certify the client boundary, while a public operator claim additionally needs
public DNS and certificate trust, external reachability controls, rate limits,
and operational ownership. The runbook does not issue a certificate, start
Zinder or Caddy, change DNS, open a firewall, or certify ZODL by itself.

The route is intentionally narrow: Caddy terminates trusted TLS on `:443`,
checks compatibility readiness on a private loopback ops port, and forwards
admitted gRPC requests over private loopback h2c. It is not a native
`WalletQuery` route, an `IngestControl` route, or a general public-management
plane.

## Candidate boundary

Use an isolated Zinder wallet-serving lane whose canonical store authenticates
`raw_blob_policy = "transactions"` and whose compatibility reader has its own
canonical and wallet secondary roots. `transactions` is sufficient for the
lightwalletd transparent-history contract; it does not add the native
full-block retention that requires `all`. The checked-in Compose topology keeps
the compatibility gRPC and ops host publications on loopback, using
`ZINDER_COMPAT_HOST_PORT` and `ZINDER_COMPAT_OPS_HOST_PORT`, whose mainnet
defaults are 9067 and 9107.

Before considering the public route, require all of the following from the
isolated candidate:

- The four-runtime topology uses distinct secondary roots for native query and
  compatibility. Do not point the proxy at a lane that shares, nests, or
  reuses a drained compatibility secondary path.
- The compatibility process has completed construction admission from an
  immutable canonical and wallet-serving pair. With `transactions` or `all`,
  that constructor derives `GetLightdInfo.taddrSupport = true`; with `none`, it
  fails before the gRPC listener binds. `taddrSupport` is not an operator flag.
- `http://127.0.0.1:${ZINDER_COMPAT_OPS_HOST_PORT}/readyz` returns 200 before
  external traffic is admitted. `/healthz` only proves that the process is
  alive, so it is never a routing gate.
- A loopback h2c `GetLightdInfo` result identifies the expected network and
  reports `taddrSupport: true`. It proves the local candidate surface, not an
  Android or ZODL result.

Use the selected lane's values without substituting a public address for a
loopback listener:

```bash
: "${ZINDER_COMPAT_HOST_PORT:?set the selected lane's loopback gRPC port}"
: "${ZINDER_COMPAT_OPS_HOST_PORT:?set the selected lane's loopback ops port}"

curl -fsS "http://127.0.0.1:${ZINDER_COMPAT_OPS_HOST_PORT}/readyz"
grpcurl -plaintext -d '{}' "127.0.0.1:${ZINDER_COMPAT_HOST_PORT}" \
  cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo
```

Stop before configuring a public route if either command fails, the network is
wrong, or `taddrSupport` is false. Retain the failed output after redacting
addresses and credentials; do not change retention, flags, or storage paths in
place to make the probe pass.

## DNS and certificate authority

The operator supplies one exact fully qualified domain name (FQDN), owns its
DNS zone, and records the intended A and AAAA records before deployment. Both
records, when published, must reach the intended Caddy edge; an absent IPv6
route is not an AAAA proof. Do not use a literal hostname from an example,
another lane, or a bundled ZODL endpoint.

The operator also owns the certificate authority, issuance method, renewal
job, expiry alert, accountable owner, and emergency contact. The certificate
must be publicly trusted for a public Android or ZODL claim and contain the
exact FQDN in a subject alternative name. Keep the certificate chain readable
by the Caddy service account, but restrict the private key and its parent
directory to the account that runs Caddy and the approved renewal authority.
Do not place ACME, DNS-provider, bearer-token, wallet, or private-key material
in this repository, Caddyfile, shell history, or evidence bundle.

`deploy/compat-lightwalletd.Caddyfile` consumes a pre-provisioned certificate
and key through `ZINDER_COMPAT_TLS_CERT_FILE` and
`ZINDER_COMPAT_TLS_KEY_FILE`. Its global `auto_https off` setting disables
Caddy automatic certificate management and HTTP redirects, while the explicit
`https://...:443` site address still serves TLS. Renewal remains an
operator-owned action, followed by the organization-approved configuration
validation and reload process; the template never grants Caddy authority to
issue or renew a certificate.

## Caddy route and readiness semantics

Set the following Caddy service environment values from the isolated lane and
certificate authority, then validate the rendered configuration before any
approved deployment action:

```bash
: "${ZINDER_COMPAT_PUBLIC_FQDN:?set the operator-owned public FQDN}"
: "${ZINDER_COMPAT_HOST_PORT:?set the selected lane's loopback gRPC port}"
: "${ZINDER_COMPAT_OPS_HOST_PORT:?set the selected lane's loopback ops port}"
: "${ZINDER_COMPAT_TLS_CERT_FILE:?set the pre-provisioned certificate chain path}"
: "${ZINDER_COMPAT_TLS_KEY_FILE:?set the restricted private-key path}"

caddy adapt \
  --config deploy/compat-lightwalletd.Caddyfile \
  --adapter caddyfile \
  --validate \
  --pretty >/dev/null
```

The template has 3 independent properties:

1. The exact FQDN site address and manual certificate establish SNI-bound TLS
   on `:443`, and `alpn h2` requires the HTTP/2 application protocol needed by
   gRPC.
2. `forward_auth` sends a `GET /readyz` to
   `127.0.0.1:${ZINDER_COMPAT_OPS_HOST_PORT}` before every new gRPC RPC. A 2xx
   response continues to the data proxy; a 503 or other non-2xx response is
   returned to the client without calling compatibility gRPC. This adds one
   loopback readiness round trip per RPC.
3. `reverse_proxy h2c://127.0.0.1:${ZINDER_COMPAT_HOST_PORT}` carries only
   admitted requests to the plaintext local gRPC listener. It does not expose
   that listener beyond the host.

The pre-check stops new RPCs after readiness drains, but it cannot revoke an
RPC or stream that was already admitted. A node outage does not drain
compatibility readiness: `/readyz` stays 200, lists the node under `degraded`,
and only `SendTransaction` fails, with `UNAVAILABLE`; `GetTreeState` is served
from storage at every retained height. The gate therefore stays open for every
method the stored chain answers. The compatibility runtime also gates
new gRPC traffic with its own readiness interceptor, so a transition between
the Caddy pre-check and the upstream call fails closed. Existing streams drain
against the immutable canonical and wallet generation they already captured;
wait for the operator's stream-drain observation before stopping that lane.

Do not replace this pre-check with `/healthz`: `/healthz` remains 200 while a
healthy process is unable to serve a coherent pair. Do not add `health_uri`,
`health_port`, or `health_upstream` to this template as a substitute. Caddy
active health checks share the reverse-proxy transport and are a periodic
upstream-selection mechanism, not this per-RPC gate; the h2c gRPC transport
must not be assumed to validate a distinct ordinary-HTTP ops listener. A
separately designed and tested readiness bridge may use those features, but it
is outside this template.

This template supplies neither client authorization nor rate limiting. The
operator must retain an explicit access, connection, request, stream, and
rate-limit policy that preserves normal wallet synchronization. Do not claim a
public operator surface when those controls are absent.

## External verification and endpoint attribution

Run these probes from an external network after the approved deployment is
active. They are verification only; they do not issue certificates, modify DNS,
or change firewall state. Set the FQDN to the operator-owned candidate name,
not a placeholder.

```bash
: "${ZINDER_COMPAT_PUBLIC_FQDN:?set the operator-owned public FQDN}"

dig +short A "${ZINDER_COMPAT_PUBLIC_FQDN}"
dig +short AAAA "${ZINDER_COMPAT_PUBLIC_FQDN}"
openssl s_client \
  -connect "${ZINDER_COMPAT_PUBLIC_FQDN}:443" \
  -servername "${ZINDER_COMPAT_PUBLIC_FQDN}" \
  -alpn h2 \
  -verify_hostname "${ZINDER_COMPAT_PUBLIC_FQDN}" \
  -verify_return_error </dev/null
grpcurl -d '{}' "${ZINDER_COMPAT_PUBLIC_FQDN}:443" \
  cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo
```

Record the A and AAAA answers, certificate subject alternative names, issuer,
serial number, SHA-256 fingerprint, expiry, successful hostname verification,
and `ALPN protocol: h2`. Record the `GetLightdInfo` output with the expected
network and `taddrSupport: true`; redact any fields that identify a private
test wallet. A successful TLS connection without `h2`, an SNI mismatch, a
certificate verification failure, or a non-ready gRPC result blocks the route.

Use an external scanner against every public A and AAAA address to prove that
the selected compatibility plaintext port, every ops port, the private
`IngestControl` port, Prometheus, Grafana, and storage ports are unreachable.
The expected public result is TLS on 443 only. Capture the scanner location,
addresses, port list, time, and negative results; a local `ss` listing or a
loopback-only Compose declaration alone is not firewall evidence. Also prove
that `grpcurl -plaintext` cannot call the public FQDN on 443 and that the
plaintext compatibility port is unreachable externally.

Before Android onboarding, configure the current SDK or ZODL app with the exact
`https://<operator-owned-fqdn>:443` candidate endpoint through its documented
endpoint-selection path. Capture the selected endpoint and the app/SDK
revision, then attribute traffic during initial sync with device-side egress
logs, a controlled DNS or proxy observation, or an equivalent independent
network trace. The evidence must show connections only to the candidate route
and zero connection attempts to every bundled endpoint applicable to that app
revision. A Caddy access log, `grpcurl`, or a successful sync by itself does
not prove absence of fallback.

## Evidence, stop conditions, and rollback

Create the evidence bundle with restricted permissions, for example under
`.tmp/production-readiness/<run-id>/`, and retain a manifest that names the
Zinder revision and image digest, Caddy version and rendered-config digest,
network, lane ports, FQDN, DNS answers, certificate fingerprint and expiry,
readiness output, TLS and gRPC results, external reachability results,
rate-limit policy test, app/SDK revision, selected endpoint, and no-fallback
trace. Store secret values separately, if at all. The manifest must not contain
private keys, ACME or DNS credentials, bearer tokens, authorization headers,
wallet seeds, viewing keys, raw wallet databases, or unredacted wallet logs.

Stop and leave the candidate non-public when any of these conditions occurs:

- The isolated `transactions` or `all` composition does not derive the expected
  immutable `taddrSupport` claim, or a `none` composition can bind the
  compatibility listener.
- Compatibility `/readyz` is not 200, the Caddy pre-check does not return its
  non-2xx response to a new RPC, or a route used `/healthz` instead.
- The certificate, SNI, ALPN `h2`, or A/AAAA ownership proof is incomplete.
- Any plaintext, ops, control, observability, or storage port is reachable
  externally.
- Endpoint selection or traffic attribution is incomplete, or any bundled
  endpoint receives a connection attempt.
- Required rate-limit, redaction, permission, renewal, or rollback evidence is
  missing.

For a failed proxy or certificate rotation, first stop admitting new requests
through the approved edge control or by allowing the compatibility readiness
gate to drain, then retain the old lane until its existing streams have drained.
Restore the previous known-good manually loaded certificate and Caddy
configuration, or withdraw the public route if no trusted certificate remains.
Re-run the TLS, gRPC, external-port, and no-fallback checks before returning
traffic. Do not restart a Zinder binary on another lane's canonical or wallet
paths, copy secondary directories, or treat a proxy rollback as a storage
rollback.

## Gate C boundary

This runbook can supply trusted-TLS, private-plaintext, readiness-routing, and
endpoint-attribution evidence for Gate C. A public operator claim also requires
the firewall, public certificate, rate-limit, and ownership evidence described
above. The runbook does not pass Gate C or certify ZODL by itself: Gate C also
requires the exact current SDK RPC inventory through a bound server, positive
and negative retention matrices, reproducible SDK inputs, initial sync by the
current app, accurate separation of broader lightwalletd methods, and an
independent architecture and evidence audit.

Gate D, not Gate C, covers restore, transparent history, manual send, mempool,
mining, restart, and reorg behavior. Do not describe a successful route check,
endpoint validation, or initial sync as a complete ZODL certification.

## References

- [Deploying the wallet service on one VM](deploying-on-a-vm.md)
- [Testing](testing.md)
- [Lightwalletd compatibility](../reference/lightwalletd-compatibility.md)
- [Integration surfaces](../reference/integration-surfaces.md)
- [Wallet data plane](../architecture/wallet-data-plane.md)
