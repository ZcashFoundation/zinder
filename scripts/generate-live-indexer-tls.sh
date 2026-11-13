#!/usr/bin/env bash
# Writes throwaway mutual-TLS material for a Regtest node's indexer listener
# and for Zinder as its client. The live workflow uses it for the Zakura leg.
#
# usage: scripts/generate-live-indexer-tls.sh <new-directory>
#
# The directory must not exist. One CA signs both leaves, and the CA key is
# deleted after signing:
#
#   ca.pem                       CA certificate (basicConstraints CA:TRUE)
#   server.pem, server-key.pem   node indexer certificate for localhost and
#                                127.0.0.1 (extendedKeyUsage serverAuth)
#   client.pem, client-key.pem   Zinder client certificate
#                                (extendedKeyUsage clientAuth)
#
# Every file is readable only by the invoking user, so run the node as that
# user. The certificates expire after two days.
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <new-directory>" >&2
  exit 2
fi

umask 077
mkdir -- "$1"
cd -- "$1"

new_key() {
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1"
}

new_serial() {
  printf '0x%s' "$(openssl rand -hex 16)"
}

new_key ca-key.pem
openssl req -new -key ca-key.pem -subj "/CN=Zinder live indexer CA" -out ca.csr
openssl x509 -req -in ca.csr -signkey ca-key.pem -days 2 -set_serial "$(new_serial)" \
  -extfile <(printf '%s\n' \
    'basicConstraints = critical, CA:TRUE' \
    'keyUsage = critical, keyCertSign, cRLSign' \
    'subjectKeyIdentifier = hash') \
  -out ca.pem

issue_leaf() {
  local name="$1" common_name="$2" extensions="$3"
  new_key "$name-key.pem"
  openssl req -new -key "$name-key.pem" -subj "/CN=$common_name" -out "$name.csr"
  openssl x509 -req -in "$name.csr" -CA ca.pem -CAkey ca-key.pem -days 2 \
    -set_serial "$(new_serial)" -extfile <(printf '%s\n' \
      'basicConstraints = critical, CA:FALSE' \
      'keyUsage = critical, digitalSignature' \
      'authorityKeyIdentifier = keyid' \
      "$extensions") \
    -out "$name.pem"
}

issue_leaf server localhost \
  "$(printf '%s\n' 'extendedKeyUsage = serverAuth' 'subjectAltName = DNS:localhost, IP:127.0.0.1')"
issue_leaf client zinder-live-test 'extendedKeyUsage = clientAuth'

rm -f -- ca-key.pem ./*.csr
openssl verify -CAfile ca.pem -purpose sslserver server.pem >/dev/null
openssl verify -CAfile ca.pem -purpose sslclient client.pem >/dev/null
