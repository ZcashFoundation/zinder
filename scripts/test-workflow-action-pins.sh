#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

# Composite actions run with the same trust as the workflows that call them,
# so their external references are held to the same pinning rule.
search_roots=(.github/workflows)
for root in .github/actions scripts/release-wallet-action; do
  if [[ -d "${root}" ]]; then
    search_roots+=("${root}")
  fi
done

status=0
while IFS= read -r -d '' workflow; do
  while IFS=: read -r line_number line; do
    uses_value="${line#*uses:}"
    uses_value="${uses_value#"${uses_value%%[![:space:]]*}"}"
    action_ref="${uses_value%%[[:space:]#]*}"

    if [[ "${action_ref}" == ./* ]]; then
      continue
    fi

    if [[ ! "${action_ref}" =~ ^[^@[:space:]]+@[0-9a-f]{40}$ ]]; then
      printf '%s:%s: external action must use a full lowercase commit SHA: %s\n' \
        "${workflow}" "${line_number}" "${action_ref}" >&2
      status=1
    fi
  done < <(grep -nE '^[[:space:]-]*uses:[[:space:]]*' "${workflow}" || true)
done < <(
  find "${search_roots[@]}" -type f \( -name '*.yml' -o -name '*.yaml' \) -print0
)

exit "${status}"
