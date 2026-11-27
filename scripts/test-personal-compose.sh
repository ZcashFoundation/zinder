#!/usr/bin/env bash
set -euo pipefail

[[ $# == 1 ]] || { echo >&2 "usage: $0 LOCAL_ZINDER_IMAGE"; exit 2; }

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/zinder-personal-compose.XXXXXX")"
export ZINDER_IMAGE="$1"
export ZINDER_NETWORK__NAME=zcash-regtest
export ZEBRA_NETWORK__NETWORK=Regtest
export ZINDER_NODE__JSON_RPC_ADDR=http://zebra:8232
export ZINDER_SERVE_MEMORY_LIMIT=4g
compose=(
  docker compose --project-name "zinder-personal-smoke-$$"
  --file "$repository_root/deploy/personal/compose.yaml"
  --file "$scratch/regtest.yaml"
)
cleanup() {
  "${compose[@]}" down --volumes >/dev/null
  rm -rf -- "$scratch"
}
trap cleanup EXIT
cat > "$scratch/regtest.yaml" <<'YAML'
services:
  zebra:
    environment:
      CONFIG_FILE_PATH: /etc/zebrad/regtest.toml
      ZEBRA_MINING__MINER_ADDRESS: tmDpFafuBHKGUYmuwLsrxWJrwcnSyzEEtYx
    volumes:
      - ../zebra/regtest.toml:/etc/zebrad/regtest.toml:ro
YAML

"${compose[@]}" config --quiet
"${compose[@]}" up -d --wait --wait-timeout 120 zebra
# shellcheck disable=SC2016
"${compose[@]}" exec -T zebra sh -ec '
  printf "user = \"%s\"\n" "$(cat /var/run/zebra-auth/.cookie)" |
    curl --config - --silent --fail --header "Content-Type: application/json"       --data "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"generate\",\"params\":[12]}"       http://127.0.0.1:8232
' | jq -e '.result | length == 12' >/dev/null
"${compose[@]}" up -d --wait --wait-timeout 120 --pull never
curl --fail --silent --show-error --retry 25 --retry-delay 2 --retry-connrefused   http://127.0.0.1:9108/readyz | jq -e '.status == "ready"' >/dev/null
