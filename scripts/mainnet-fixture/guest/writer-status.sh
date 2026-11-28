#!/usr/bin/env bash
# writer-status.sh <ingest_control_addr> <token_file> <out.json>
set -euo pipefail
tok=$(cat "$2")
grpcurl -plaintext -import-path /opt/zfx/proto-all -proto zinder/v1/ingest/ingest.proto \
  -rpc-header "authorization: Bearer $tok" -format json -emit-defaults -d '{}' "$1" zinder.v1.ingest.CanonicalControl/WriterStatus > "$3"
cat "$3" | jq -c .
