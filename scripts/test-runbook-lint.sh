#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
lint="$repository_root/scripts/runbook-lint.sh"
fixture="$(mktemp -t zinder-runbook-lint-test.XXXXXX)"
trap 'rm -f -- "$fixture"' EXIT

cat > "$fixture" <<'MARKDOWN'
```bash
true
```

1. Verify the endpoint.

   ```bash
   cat <<'END'
   Endpoint verification
   END
   if true; then
     true
   ```
MARKDOWN

# The unfinished if must reach bash -n after the indented heredoc ends.
if output="$("$lint" "$fixture" 2>&1)"; then
  echo >&2 "runbook lint accepted malformed Bash in a numbered step: $output"
  exit 1
else
  status=$?
fi
if [[ "$status" != 1 || "$output" != *'block-0002.sh failed:'* ||
      "$output" != *'checked 2 bash blocks'*'1 failed'* ]]; then
  echo >&2 "runbook lint did not isolate the malformed block: $output"
  exit 1
fi

sed -i '/^     true$/a\   fi' "$fixture"
output="$("$lint" "$fixture" 2>&1)"
if [[ "$output" != *'checked 2 bash blocks'*'0 failed'* ]]; then
  echo >&2 "runbook lint did not check both valid Bash blocks: $output"
  exit 1
fi

echo "runbook lint tests passed"
