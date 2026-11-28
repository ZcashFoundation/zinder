#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
caddy_image='caddy:2.11-alpine@sha256:003a97808e781aa9f7e75cc2f43d34d461099031faf5e20faefb9f61126836f0'

mkdir -p -- "$repository_root/.tmp"
fixture="$(mktemp -d "$repository_root/.tmp/compat-caddyfile.XXXXXX")"
trap 'rm -rf -- "$fixture"' EXIT
"$repository_root/scripts/generate-live-indexer-tls.sh" "$fixture/tls"

docker run --rm --network none --user "$(id -u):$(id -g)" \
  --volume "$repository_root/deploy/compat-lightwalletd.Caddyfile:/etc/caddy/Caddyfile:ro" \
  --volume "$fixture/tls:/tls:ro" \
  --env XDG_DATA_HOME=/tmp \
  --env DEPLOY_COMPAT_PUBLIC_FQDN=localhost \
  --env DEPLOY_COMPAT_TLS_CERT_FILE=/tls/server.pem \
  --env DEPLOY_COMPAT_TLS_KEY_FILE=/tls/server-key.pem \
  --env DEPLOY_COMPAT_HOST_PORT=9067 \
  --env DEPLOY_SERVE_OPS_HOST_PORT=9108 \
  "$caddy_image" caddy adapt --config /etc/caddy/Caddyfile --adapter caddyfile --validate \
  > "$fixture/adapted.json"

jq -e '
  .apps.http.servers[]
  | select(.listen == [":443"])
  | if ((.protocols // []) | contains(["h2", "h3"])) and .automatic_https.disable
       and (.tls_connection_policies | length > 0 and all(.[]; .alpn == null))
    then .
    else error("expected HTTP/2 and HTTP/3 on the TLS listener with Caddy-managed ALPN")
    end
  | .routes[0].handle[0].routes[0].handle[0].routes[0].handle
  | .[0] as $readiness
  | .[1] as $wallet_proxy
  | if $readiness.handler == "reverse_proxy"
       and $readiness.rewrite == {"method": "HEAD", "uri": "/readyz"}
       and $readiness.upstreams == [{"dial": "127.0.0.1:9108"}]
       and [$readiness.handle_response[]?.match.status_code] == [[2]]
       and $readiness.transport.max_conns_per_host == 16
    then .
    else error("expected HEAD /readyz to admit only 2xx responses before forwarding")
    end
  | if $wallet_proxy.handler == "reverse_proxy"
       and $wallet_proxy.upstreams == [{"dial": "127.0.0.1:9067"}]
       and ($wallet_proxy.transport.versions | index("h2c") != null)
       and $wallet_proxy.headers.request.set["X-Zinder-Client-Address"] == ["{http.request.remote}"]
       and $wallet_proxy.headers.request.add["X-Zinder-Client-Address"] == null
    then true
    else error("expected loopback h2c forwarding with client-address replacement")
    end
' "$fixture/adapted.json" >/dev/null

echo "Compatibility Caddyfile checks passed"
