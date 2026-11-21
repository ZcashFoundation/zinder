#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 1 || $1 != /* ]]; then
  echo "Usage: $0 /absolute/path/to/new-generation" >&2
  exit 2
fi

# The parent is operator-owned. Publish all three mounts together and never
# replace the trust root of an existing generation, even on concurrent setup.
umask 077
generation=$1
staging=$(mktemp -d -- "${generation}.XXXXXXXX")
trap 'rm -rf -- "$staging"' EXIT
openssl genpkey -algorithm ED25519 -outform DER -out "$staging/publisher.pk8"
# The Ed25519 SubjectPublicKeyInfo ends in its 32 raw verification key bytes.
openssl pkey -inform DER -in "$staging/publisher.pk8" -pubout -outform DER \
  | tail -c 32 > "$staging/publisher.pub"
[[ $(wc -c < "$staging/publisher.pub") == 32 ]]
chown root:10001 "$staging/publisher.pk8"
chmod 0440 "$staging/publisher.pk8"
chmod 0444 "$staging/publisher.pub"
install -d -o 10001 -g 10001 -m 0700 "$staging/catalog"
sync "$staging/publisher.pk8" "$staging/publisher.pub" "$staging/catalog" "$staging"
mv -Tn -- "$staging" "$generation"
if [[ -e $staging ]]; then
  echo "History generation already exists: $generation" >&2
  exit 1
fi
sync "$(dirname -- "$generation")"
