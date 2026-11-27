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
  validate-changelog.sh upgrade-line [REPOSITORY]
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

format_upgrade_class_line() {
  local upgrade_class="$1"
  # shellcheck disable=SC2016
  case "$upgrade_class" in
    compatible)
      printf '%s\n' \
        '**Upgrade class:** `compatible` — Reversible by redeploying the previous release against the same store and schema.'
      ;;
    additive-schema)
      printf '%s\n' \
        '**Upgrade class:** `additive-schema` — Reversible by redeploying the previous release against the same store and schema.'
      ;;
    projection-layout)
      printf '%s\n' \
        '**Upgrade class:** `projection-layout` — Requires a fresh rebuild of the affected projection; see release entries below for affected projections and rollback steps.'
      ;;
    canonical-layout)
      printf '%s\n' \
        '**Upgrade class:** `canonical-layout` — Requires a fresh rebuild of the canonical store and dependent projections; see release entries below for layout changes and rollback steps.'
      ;;
    *)
      fail "unknown Upgrade class: $upgrade_class"
      ;;
  esac
}

insert_upgrade_class_line() {
  local upgrade_line="$1"
  awk -v upgrade_line="$upgrade_line" '
    NR == 1 {
      print
      print ""
      print upgrade_line
      next
    }
    NR == 2 && $0 != "" {
      print ""
    }
    {
      print
    }
  '
}

version_requires_upgrade_class() {
  local stable_version="$1"
  local major minor
  IFS=. read -r major minor _ <<< "$stable_version"
  (( 10#$major > 0 || (10#$major == 0 && 10#$minor >= 28) ))
}

derive_pending_upgrade_class() {
  local repository_root="$1"
  local derived_class

  if ! derived_class="$(
    python3 - "$repository_root" "$changie_bin" 2>&1 <<'PY'
import glob
import os
import re
import subprocess
import sys
import tempfile

repository_root, changie_bin = sys.argv[1:3]
unreleased_dir = os.path.join(repository_root, ".changes", "unreleased")
PRECEDENCE = {
    "compatible": 0,
    "additive-schema": 1,
    "projection-layout": 2,
    "canonical-layout": 3,
}
ALLOWED_LIST = ", ".join(PRECEDENCE.keys())
PROJECTION_NAME_RE = re.compile(
    r"(?<![A-Za-z0-9])(wallet|explorer|materialized(?:[-_\s]+)views?)(?![A-Za-z0-9])",
    re.IGNORECASE,
)
REBUILD_ROLLBACK_RE = re.compile(
    r"(?<![A-Za-z0-9])(rebuilds?|rebuilding|rebuilt|roll(?:ed)?(?:-|\s+)?backs?|restor(?:e|ing|ed))(?![A-Za-z0-9])",
    re.IGNORECASE,
)

expected_files = sorted(
    os.path.basename(path)
    for path in glob.glob(os.path.join(unreleased_dir, "*.yaml"))
    if os.path.isfile(path)
)
if not expected_files:
    print("no pending .changes/unreleased/*.yaml fragments", file=sys.stderr)
    sys.exit(1)

with open(os.path.join(repository_root, ".changie.yaml"), "r", encoding="utf-8") as fh:
    cfg_lines = fh.read().splitlines()

strip_keys = {
    "versionFormat",
    "kindFormat",
    "changeFormat",
    "componentFormat",
    "headerFormat",
    "footerFormat",
    "newlines",
}
out_lines = []
skip = False
for line in cfg_lines:
    if line and not line[0].isspace() and not line.lstrip().startswith("#"):
        key = line.split(":", 1)[0].strip().strip("'\"")
        skip = key in strip_keys
    if not skip:
        out_lines.append(line)

out_lines.extend(
    [
        "versionFormat: ''",
        "kindFormat: ''",
        "componentFormat: ''",
        "headerFormat: ''",
        "footerFormat: ''",
        'changeFormat: \'ZINDER_FRAG|{{printf "%x" .Filename}}|{{printf "%x" .Body}}{{range $k, $v := .Custom}}|{{printf "%x" $k}}={{printf "%x" $v}}{{end}}\'',
        "newlines: {}",
    ]
)

with tempfile.NamedTemporaryFile(
    mode="w", suffix=".yaml", delete=False, encoding="utf-8"
) as tf:
    tf.write("\n".join(out_lines) + "\n")
    cfg_path = tf.name

try:
    env = dict(os.environ)
    env["CHANGIE_CONFIG_PATH"] = cfg_path
    proc = subprocess.run(
        [changie_bin, "batch", "auto", "--dry-run", "--allow-no-changes=false"],
        cwd=repository_root,
        env=env,
        capture_output=True,
        text=True,
    )
finally:
    if os.path.exists(cfg_path):
        os.unlink(cfg_path)

if proc.returncode != 0:
    msg = (proc.stderr or proc.stdout).strip() or f"{changie_bin} batch failed"
    print(msg, file=sys.stderr)
    sys.exit(1)

fragments = {}
for raw_line in proc.stdout.splitlines():
    if not raw_line:
        continue
    parts = raw_line.split("|")
    if len(parts) < 3 or parts[0] != "ZINDER_FRAG":
        print(f"unexpected changie inspection output: {raw_line!r}", file=sys.stderr)
        sys.exit(1)
    filename = os.path.basename(bytes.fromhex(parts[1]).decode("utf-8"))
    body_text = bytes.fromhex(parts[2]).decode("utf-8")
    custom = {}
    for item in parts[3:]:
        if "=" not in item:
            print(f"unexpected changie custom field output: {item!r}", file=sys.stderr)
            sys.exit(1)
        k_hex, v_hex = item.split("=", 1)
        custom[bytes.fromhex(k_hex).decode("utf-8")] = bytes.fromhex(v_hex).decode("utf-8")
    if filename in fragments:
        print(f"duplicate fragment reported by changie: {filename}", file=sys.stderr)
        sys.exit(1)
    fragments[filename] = (body_text, custom)

if sorted(fragments.keys()) != expected_files:
    print(
        f"changie fragment set mismatch: expected {expected_files}, got {sorted(fragments.keys())}",
        file=sys.stderr,
    )
    sys.exit(1)

highest_class = None
highest_rank = -1

for filename in expected_files:
    body_text, custom = fragments[filename]
    if "Upgrade" not in custom:
        print(
            f"{filename}: missing custom.Upgrade (expected one of: {ALLOWED_LIST})",
            file=sys.stderr,
        )
        sys.exit(1)

    upgrade_value = custom["Upgrade"]
    if upgrade_value == "":
        print(
            f"{filename}: empty custom.Upgrade (expected one of: {ALLOWED_LIST})",
            file=sys.stderr,
        )
        sys.exit(1)
    if upgrade_value not in PRECEDENCE:
        print(
            f"{filename}: unknown custom.Upgrade {upgrade_value!r} (expected one of: {ALLOWED_LIST})",
            file=sys.stderr,
        )
        sys.exit(1)

    if upgrade_value == "projection-layout":
        if not PROJECTION_NAME_RE.search(body_text):
            print(
                f"{filename}: projection-layout fragment must name an affected projection (wallet, explorer, or materialized-view) in body",
                file=sys.stderr,
            )
            sys.exit(1)
        if not REBUILD_ROLLBACK_RE.search(body_text):
            print(
                f"{filename}: projection-layout fragment must include rebuild or rollback guidance in body",
                file=sys.stderr,
            )
            sys.exit(1)

    rank = PRECEDENCE[upgrade_value]
    if rank > highest_rank:
        highest_rank = rank
        highest_class = upgrade_value

print(highest_class)
PY
  )"; then
    fail "$derived_class"
  fi
  printf '%s\n' "$derived_class"
}

inspect_release_section() {
  local changelog_path="$1"
  local section_heading="$2"
  local requires_upgrade_class="$3"
  local inspected

  if ! inspected="$(
    python3 - "$changelog_path" "$section_heading" "$requires_upgrade_class" 2>&1 <<'PY'
import re
import sys

changelog_path, section_heading, requires_flag = sys.argv[1:4]
requires_upgrade_class = requires_flag == "true"

CANONICAL_LINES = {
    "compatible": "**Upgrade class:** `compatible` — Reversible by redeploying the previous release against the same store and schema.",
    "additive-schema": "**Upgrade class:** `additive-schema` — Reversible by redeploying the previous release against the same store and schema.",
    "projection-layout": "**Upgrade class:** `projection-layout` — Requires a fresh rebuild of the affected projection; see release entries below for affected projections and rollback steps.",
    "canonical-layout": "**Upgrade class:** `canonical-layout` — Requires a fresh rebuild of the canonical store and dependent projections; see release entries below for layout changes and rollback steps.",
}
UPGRADE_LINE_RE = re.compile(r"^\s*(?:\*\*)?upgrade class(?:\*\*)?\s*:", re.IGNORECASE)
CANONICAL_SHAPE_RE = re.compile(r"^\*\*Upgrade class:\*\* `([^`]+)` — .+$")

with open(changelog_path, "r", encoding="utf-8") as fh:
    lines = fh.read().splitlines()

section_lines = []
in_section = False
for line in lines:
    if not in_section:
        if line == section_heading:
            in_section = True
        continue
    if line.startswith("## "):
        break
    section_lines.append(line)

non_empty_indices = [idx for idx, line in enumerate(section_lines) if line.strip() != ""]
upgrade_indices = [
    idx for idx in non_empty_indices if UPGRADE_LINE_RE.match(section_lines[idx])
]
content_count = len([idx for idx in non_empty_indices if idx not in upgrade_indices])

if not non_empty_indices or content_count == 0:
    print(
        f"CHANGELOG.md section '{section_heading}' must contain release notes",
        file=sys.stderr,
    )
    sys.exit(1)

if requires_upgrade_class or len(upgrade_indices) > 0:
    if len(upgrade_indices) == 0:
        print(
            f"CHANGELOG.md section '{section_heading}' is missing its Upgrade class declaration",
            file=sys.stderr,
        )
        sys.exit(1)
    if len(upgrade_indices) > 1:
        print(
            f"CHANGELOG.md section '{section_heading}' contains multiple Upgrade class declarations",
            file=sys.stderr,
        )
        sys.exit(1)

    upgrade_idx = upgrade_indices[0]
    if upgrade_idx != non_empty_indices[0]:
        print(
            f"CHANGELOG.md section '{section_heading}' must place Upgrade class immediately under the version heading",
            file=sys.stderr,
        )
        sys.exit(1)

    upgrade_line = section_lines[upgrade_idx]
    m = CANONICAL_SHAPE_RE.match(upgrade_line)
    if not m:
        print(
            f"CHANGELOG.md section '{section_heading}' has a malformed Upgrade class line",
            file=sys.stderr,
        )
        sys.exit(1)

    upgrade_class = m.group(1)
    if upgrade_class not in CANONICAL_LINES:
        print(
            f"CHANGELOG.md section '{section_heading}' declares unknown Upgrade class '{upgrade_class}'",
            file=sys.stderr,
        )
        sys.exit(1)

    if upgrade_line != CANONICAL_LINES[upgrade_class]:
        print(
            f"CHANGELOG.md section '{section_heading}' Upgrade class line does not match the canonical statement for '{upgrade_class}'",
            file=sys.stderr,
        )
        sys.exit(1)

    print(upgrade_line)
PY
  )"; then
    fail "$inspected"
  fi
  printf '%s' "$inspected"
}

validate_pending_fragments() {
  local repository_root="$1"
  [[ -z "$(pending_fragment "$repository_root")" ]] && return
  (
    cd "$repository_root"
    "$changie_bin" batch auto --dry-run --allow-no-changes=false >/dev/null
  ) || fail "pending fragments are not valid Changie v1.26.0 changes"
  derive_pending_upgrade_class "$repository_root" >/dev/null
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

validate_upgrade_line() {
  local repository_root="${1:-$default_repository_root}"
  validate_repository "$repository_root"
  [[ -n "$(pending_fragment "$repository_root")" ]] \
    || fail "no pending .changes/unreleased/*.yaml fragments"
  validate_pending_fragments "$repository_root"
  local derived_class
  derived_class="$(derive_pending_upgrade_class "$repository_root")"
  format_upgrade_class_line "$derived_class"
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
  local requires_upgrade_class=false
  if version_requires_upgrade_class "$stable_version"; then
    requires_upgrade_class=true
  fi

  local escaped_release_version="${release_version//./\\.}"
  local section_heading_pattern="^## \\[${escaped_release_version}\\] - [0-9]{4}-[0-9]{2}-[0-9]{2}$"
  if [[ "$release_version" == "$stable_version" ]]; then
    [[ -z "$(pending_fragment "$repository_root")" ]] \
      || fail "stable release has pending .changes/unreleased/*.yaml fragments"
  else
    [[ -n "$(pending_fragment "$repository_root")" ]] \
      || fail "prerelease must retain its .changes/unreleased/*.yaml fragments"
    validate_pending_fragments "$repository_root"
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

  local changelog_upgrade_line
  changelog_upgrade_line="$(
    inspect_release_section \
      "$repository_root/CHANGELOG.md" \
      "$section_heading" \
      "$requires_upgrade_class"
  )"

  if [[ "$release_version" != "$stable_version" ]]; then
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

    local expected_prerelease="$rendered_prerelease"
    if [[ "$requires_upgrade_class" == true ]] \
      || [[ -n "$changelog_upgrade_line" ]] \
      || grep -Eiq '^[[:space:]]*(\*\*)?upgrade class(\*\*)?:' "$archived_prerelease"
    then
      local derived_upgrade_class
      local expected_upgrade_line
      derived_upgrade_class="$(derive_pending_upgrade_class "$repository_root")"
      expected_upgrade_line="$(format_upgrade_class_line "$derived_upgrade_class")"
      [[ "$changelog_upgrade_line" == "$expected_upgrade_line" ]] \
        || fail "prerelease CHANGELOG.md Upgrade class does not match retained fragments"
      expected_prerelease="$(
        printf '%s\n' "$rendered_prerelease" \
          | insert_upgrade_class_line "$expected_upgrade_line"
      )"
    fi

    local expected_prerelease_body="${expected_prerelease#*$'\n'}"
    local archived_prerelease_body
    archived_prerelease_body="$(tail -n +2 "$archived_prerelease")"
    [[ "$expected_prerelease_body" == "$archived_prerelease_body" ]] \
      || fail "prerelease archive does not match the retained fragments"
  fi
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
  upgrade-line)
    [[ $# -le 1 ]] || usage
    validate_upgrade_line "$@"
    ;;
  *)
    usage
    ;;
esac
