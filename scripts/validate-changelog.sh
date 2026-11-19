#!/usr/bin/env bash
set -euo pipefail

default_repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
changie_bin="${CHANGIE_BIN:-changie}"

fail() {
  echo >&2 "changelog rejected: $*"
  exit 1
}

usage() {
  cat >&2 <<'USAGE'
usage:
  validate-changelog.sh fragments [REPOSITORY]
  validate-changelog.sh pr BASE HEAD PR_BODY_FILE [REPOSITORY]
  validate-changelog.sh release v<semver> [REPOSITORY]
USAGE
  exit 2
}

pending_fragment() {
  local repository_root="$1"
  find "$repository_root/.changes/unreleased" \
    -maxdepth 1 \
    -type f \
    -name '*.yaml' \
    -print \
    -quit
}

validate_pending_fragments() {
  local repository_root="$1"
  [[ -z "$(pending_fragment "$repository_root")" ]] && return
  (
    cd "$repository_root"
    "$changie_bin" batch auto --dry-run --allow-no-changes=false >/dev/null
  ) || fail "pending fragments are not valid Changie v1.26.0 changes"
}

validate_repository() {
  local repository_root="$1"
  git -C "$repository_root" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
    || fail "repository is not a Git worktree: $repository_root"
  [[ -f "$repository_root/.changie.yaml" ]] || fail "missing .changie.yaml"
  [[ -d "$repository_root/.changes/unreleased" ]] \
    || fail "missing .changes/unreleased"
  [[ -f "$repository_root/CHANGELOG.md" ]] || fail "missing CHANGELOG.md"
}

validate_fragments() {
  local repository_root="${1:-$default_repository_root}"
  validate_repository "$repository_root"
  validate_pending_fragments "$repository_root"
}

validate_pr() {
  [[ $# -ge 3 && $# -le 4 ]] || usage
  local base_commit="$1"
  local head_commit="$2"
  local pr_body_file="$3"
  local repository_root="${4:-$default_repository_root}"
  validate_repository "$repository_root"
  [[ -f "$pr_body_file" ]] || fail "PR body file does not exist: $pr_body_file"
  git -C "$repository_root" cat-file -e "${base_commit}^{commit}" 2>/dev/null \
    || fail "base commit is unavailable: $base_commit"
  git -C "$repository_root" cat-file -e "${head_commit}^{commit}" 2>/dev/null \
    || fail "head commit is unavailable: $head_commit"

  validate_pending_fragments "$repository_root"
  local changed_fragment=false
  while IFS= read -r -d '' path; do
    if [[ "$path" =~ ^\.changes/unreleased/[^/]+\.yaml$ ]] \
      && git -C "$repository_root" cat-file -e "${head_commit}:${path}" 2>/dev/null
    then
      changed_fragment=true
      break
    fi
  done < <(
    git -C "$repository_root" diff \
      --name-only \
      -z \
      --diff-filter=ACMR \
      "${base_commit}...${head_commit}"
  )

  if [[ "$changed_fragment" != true ]] \
    && ! grep -Fqx -- '- [x] No release note required' "$pr_body_file"
  then
    fail "PR must change a present .changes/unreleased/*.yaml file or contain the exact checked declaration '- [x] No release note required'"
  fi
}

validate_release() {
  [[ $# -ge 1 && $# -le 2 ]] || usage
  local release_tag="$1"
  local repository_root="${2:-$default_repository_root}"
  validate_repository "$repository_root"
  [[ "$release_tag" == v* ]] || fail "release tag must start with v"
  local release_version="${release_tag#v}"
  local semver_pattern='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*))?$'
  [[ "$release_version" =~ $semver_pattern ]] \
    || fail "release tag must contain a complete SemVer version without build metadata"

  local stable_version="${release_version%%-*}"
  local escaped_release_version="${release_version//./\\.}"
  local section_heading_pattern="^## \\[${escaped_release_version}\\] - [0-9]{4}-[0-9]{2}-[0-9]{2}$"
  if [[ "$release_version" == "$stable_version" ]]; then
    [[ -z "$(pending_fragment "$repository_root")" ]] \
      || fail "stable release has pending .changes/unreleased/*.yaml fragments"
  else
    [[ -n "$(pending_fragment "$repository_root")" ]] \
      || fail "prerelease must retain its .changes/unreleased/*.yaml fragments"
    validate_pending_fragments "$repository_root"

    local archived_prerelease="$repository_root/.changes/v${release_version}.md"
    [[ -f "$archived_prerelease" ]] \
      || fail "prerelease is missing its archived Changie version"
    head -n 1 "$archived_prerelease" \
      | grep -Eq -- "$section_heading_pattern" \
      || fail "prerelease archive has an invalid version heading"

    local rendered_prerelease
    rendered_prerelease="$(
      cd "$repository_root"
      "$changie_bin" batch \
        "v${release_version}" \
        --dry-run \
        --keep \
        --allow-no-changes=false
    )" || fail "prerelease fragments could not be rendered"

    local rendered_prerelease_body="${rendered_prerelease#*$'\n'}"
    local archived_prerelease_body
    archived_prerelease_body="$(tail -n +2 "$archived_prerelease")"
    [[ "$rendered_prerelease_body" == "$archived_prerelease_body" ]] \
      || fail "prerelease archive does not match the retained fragments"
  fi

  local section_count
  section_count="$(grep -Ec -- "$section_heading_pattern" "$repository_root/CHANGELOG.md" || true)"
  [[ "$section_count" -eq 1 ]] \
    || fail "CHANGELOG.md must contain exactly one '## [${release_version}] - YYYY-MM-DD' section"

  local section_heading
  section_heading="$(
    grep -E -- "$section_heading_pattern" "$repository_root/CHANGELOG.md" \
      | head -n 1
  )"

  local release_note_line_count
  release_note_line_count="$(
    awk -v section_heading="$section_heading" '
      $0 == section_heading {
        in_section = 1
        next
      }
      in_section && /^## / {
        exit
      }
      in_section && NF {
        line_count++
      }
      END {
        print line_count + 0
      }
    ' "$repository_root/CHANGELOG.md"
  )"
  [[ "$release_note_line_count" -gt 0 ]] \
    || fail "CHANGELOG.md section '$section_heading' must contain release notes"
}

[[ $# -ge 1 ]] || usage
command="$1"
shift
case "$command" in
  fragments)
    [[ $# -le 1 ]] || usage
    validate_fragments "$@"
    ;;
  pr)
    validate_pr "$@"
    ;;
  release)
    validate_release "$@"
    ;;
  *)
    usage
    ;;
esac
