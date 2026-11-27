#!/usr/bin/env bash
# Lints every fenced ```bash``` block in a Zinder runbook for syntactic
# correctness by piping each block through `bash -n` (parse-only mode).
#
# Catches typos and unclosed quotes/braces in operator-facing shell
# recipes before they hit production troubleshooting sessions. Does not
# execute the blocks; running them requires the same prerequisites the
# runbook documents (live node, TLS termination, ingest-control proxy,
# etc.).
#
# Usage:
#   scripts/runbook-lint.sh [path-to-runbook]
#
# Default target: docs/runbooks/testing.md.
#
# Exit codes:
#   0  every bash block parsed cleanly
#   1  one or more blocks failed `bash -n`
#   2  the target file was unreachable
#
# Wired into the runbook-self-test row of docs/runbooks/testing.md.

set -euo pipefail

RUNBOOK="${1:-docs/runbooks/testing.md}"

if [[ ! -r "$RUNBOOK" ]]; then
  echo "[runbook-lint] cannot read $RUNBOOK" >&2
  exit 2
fi

TMPDIR_LINT="$(mktemp -d -t zinder-runbook-lint.XXXXXX)"
trap 'rm -rf "$TMPDIR_LINT"' EXIT

awk -v lint_directory="$TMPDIR_LINT" '
  !in_block && /^[ \t]*```bash[ \t]*$/ {
    match($0, /^[ \t]*/)
    fence_indent = RLENGTH
    in_block = 1
    block_idx += 1
    out = sprintf("%s/block-%04d.sh", lint_directory, block_idx)
    next
  }
  in_block && /^[ \t]*```[ \t]*$/ {
    close(out)
    in_block = 0
    next
  }
  in_block {
    match($0, /^[ \t]*/)
    indent_to_strip = RLENGTH < fence_indent ? RLENGTH : fence_indent
    print substr($0, indent_to_strip + 1) > out
  }
  END {
    print block_idx + 0 > (lint_directory "/block-count")
  }
' "$RUNBOOK"

block_count="$(cat "$TMPDIR_LINT/block-count")"
fail_count=0
for block in "$TMPDIR_LINT"/block-*.sh; do
  [[ -e "$block" ]] || continue
  if ! bash -n "$block" 2>"$TMPDIR_LINT/error"; then
    block_label="$(basename "$block")"
    echo "[runbook-lint] $block_label failed:" >&2
    sed 's/^/  /' "$TMPDIR_LINT/error" >&2
    fail_count=$((fail_count + 1))
  fi
done

echo "[runbook-lint] checked $block_count bash blocks in $RUNBOOK; $fail_count failed"
if (( fail_count > 0 )); then
  exit 1
fi
