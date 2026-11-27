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
    python3 - "$repository_root/.changes/unreleased" 2>&1 <<'PY'
import glob
import os
import re
import sys

unreleased_dir = sys.argv[1]
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
    r"(?<![A-Za-z0-9])(rebuilds?|rebuilding|rebuilt|roll(?:ed)?(?:[-\s])?backs?|restor(?:e|ing|ed))(?![A-Za-z0-9])",
    re.IGNORECASE,
)
TOP_KEY_RE = re.compile(r'^("[^"]+"|\'[^\']+\'|[A-Za-z0-9_.-]+)\s*:(.*)$')


def unquote_key(token):
    if len(token) >= 2 and token[0] == token[-1] and token[0] in ('"', "'"):
        return token[1:-1]
    return token


def strip_inline_comment(text):
    return re.sub(r"(?:^|\s+)#.*$", "", text).strip()


def parse_quoted_scalar(lines, start_idx, first_text, quote_char):
    buf = []
    pos = 1  # skip opening quote
    idx = start_idx
    segment = first_text
    while True:
        if quote_char == "'":
            while pos < len(segment):
                ch = segment[pos]
                if ch == "'":
                    if pos + 1 < len(segment) and segment[pos + 1] == "'":
                        buf.append("'")
                        pos += 2
                        continue
                    return "".join(buf), idx
                buf.append(ch)
                pos += 1
        else:
            while pos < len(segment):
                ch = segment[pos]
                if ch == "\\":
                    if pos + 1 < len(segment):
                        nxt = segment[pos + 1]
                        if nxt == "n":
                            buf.append("\n")
                        elif nxt == "t":
                            buf.append("\t")
                        elif nxt == "r":
                            buf.append("\r")
                        else:
                            buf.append(nxt)
                        pos += 2
                    else:
                        # line continuation
                        pos += 1
                    continue
                if ch == '"':
                    return "".join(buf), idx
                buf.append(ch)
                pos += 1
        idx += 1
        if idx >= len(lines):
            return "".join(buf), idx - 1
        buf.append("\n")
        segment = lines[idx]
        pos = 0


def parse_scalar_value(lines, idx, rest_text, key_indent):
    stripped = rest_text.lstrip(" \t")
    if stripped.startswith(("'", '"')):
        val, end_idx = parse_quoted_scalar(lines, idx, stripped, stripped[0])
        return val, end_idx, True

    inline_comment = bool(re.search(r"(?:^|\s+)#", stripped))
    parts = []
    first_val = strip_inline_comment(stripped)
    if first_val:
        parts.append(first_val)
    last_consumed = idx
    if not inline_comment:
        j = idx + 1
        while j < len(lines):
            pl = lines[j]
            ps = pl.strip()
            if ps == "":
                j += 1
                continue
            if ps.startswith("#"):
                break
            pi = len(pl) - len(pl.lstrip(" "))
            if pi <= key_indent:
                break
            if re.search(r"(?:^|\s+)#", ps):
                part = strip_inline_comment(ps)
                if part:
                    parts.append(part)
                last_consumed = j
                break
            parts.append(ps)
            last_consumed = j
            j += 1
    combined = " ".join(parts).strip()
    if combined in ("", "null", "Null", "NULL", "~"):
        return "", last_consumed, False
    return combined, last_consumed, False


def parse_flow_mapping(lines, start_idx, first_rest):
    buf = []
    idx = start_idx
    segment = first_rest
    in_quote = None
    escaped = False
    depth = 0
    found_end = False
    while idx < len(lines):
        pos = 0
        while pos < len(segment):
            ch = segment[pos]
            if in_quote == "'":
                buf.append(ch)
                if ch == "'":
                    if pos + 1 < len(segment) and segment[pos + 1] == "'":
                        buf.append("'")
                        pos += 2
                        continue
                    in_quote = None
            elif in_quote == '"':
                buf.append(ch)
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    in_quote = None
            else:
                if ch == "#" and (pos == 0 or segment[pos - 1].isspace()):
                    break
                if ch in ("'", '"'):
                    in_quote = ch
                    buf.append(ch)
                elif ch == "{":
                    depth += 1
                    if depth > 1:
                        buf.append(ch)
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        found_end = True
                        break
                    buf.append(ch)
                else:
                    buf.append(ch)
            pos += 1
        if found_end:
            break
        buf.append(" ")
        idx += 1
        if idx < len(lines):
            segment = lines[idx]

    inner = "".join(buf).strip()
    entries = []
    cur = []
    in_quote = None
    escaped = False
    pos = 0
    while pos < len(inner):
        ch = inner[pos]
        if in_quote == "'":
            cur.append(ch)
            if ch == "'":
                if pos + 1 < len(inner) and inner[pos + 1] == "'":
                    cur.append("'")
                    pos += 2
                    continue
                in_quote = None
        elif in_quote == '"':
            cur.append(ch)
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_quote = None
        else:
            if ch in ("'", '"'):
                in_quote = ch
                cur.append(ch)
            elif ch == ",":
                item_str = "".join(cur).strip()
                if item_str:
                    entries.append(item_str)
                cur = []
                pos += 1
                continue
            else:
                cur.append(ch)
        pos += 1
    tail = "".join(cur).strip()
    if tail:
        entries.append(tail)

    upgrade_values = []
    for entry in entries:
        m = TOP_KEY_RE.match(entry)
        if not m:
            continue
        raw_key = m.group(1)
        is_quoted_key = raw_key[0] in ("'", '"')
        rest = m.group(2)
        if not is_quoted_key and rest and rest[0] not in (" ", "\t"):
            continue
        k = unquote_key(raw_key)
        if k == "Upgrade":
            v_raw = rest.strip()
            if len(v_raw) >= 2 and v_raw[0] == "'" and v_raw[-1] == "'":
                upgrade_values.append((v_raw[1:-1].replace("''", "'"), True))
            elif len(v_raw) >= 2 and v_raw[0] == '"' and v_raw[-1] == '"':
                parsed, _ = parse_quoted_scalar([v_raw], 0, v_raw, '"')
                upgrade_values.append((parsed, True))
            else:
                clean = strip_inline_comment(v_raw)
                if clean in ("null", "Null", "NULL", "~"):
                    clean = ""
                upgrade_values.append((clean, False))
    return upgrade_values, idx


def parse_fragment(path):
    with open(path, "r", encoding="utf-8") as fh:
        raw = fh.read()
    text = raw.lstrip("\ufeff").replace("\r\n", "\n")
    lines = text.split("\n")

    body_text = ""
    upgrade_entries = []

    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or stripped in ("---", "..."):
            i += 1
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent != 0:
            i += 1
            continue
        m = TOP_KEY_RE.match(line)
        if not m:
            i += 1
            continue
        key = unquote_key(m.group(1))
        rest = m.group(2)
        if rest and rest[0] not in (" ", "\t"):
            i += 1
            continue

        if key == "body":
            rest_stripped = rest.lstrip(" \t")
            block_match = re.match(
                r"^([|>])([+-]?[1-9]?|[1-9]?[+-]?)(?:\s+#.*)?$", rest_stripped
            )
            if block_match:
                style = block_match.group(1)
                mod = block_match.group(2)
                explicit_indent = None
                for ch in mod:
                    if ch.isdigit():
                        explicit_indent = int(ch)
                j = i + 1
                block_lines = []
                while j < len(lines):
                    bl = lines[j]
                    if bl.strip() == "":
                        block_lines.append("")
                        j += 1
                        continue
                    bi = len(bl) - len(bl.lstrip(" "))
                    if bi == 0:
                        break
                    block_lines.append(bl)
                    j += 1
                while block_lines and block_lines[-1] == "":
                    block_lines.pop()
                if explicit_indent is not None:
                    base_indent = explicit_indent
                else:
                    base_indent = None
                    for bl in block_lines:
                        if bl.strip() != "":
                            base_indent = len(bl) - len(bl.lstrip(" "))
                            break
                    if base_indent is None:
                        base_indent = 0
                cleaned = [
                    bl[base_indent:] if len(bl) >= base_indent and bl.strip() != "" else ""
                    for bl in block_lines
                ]
                if style == ">":
                    paragraphs = []
                    current = []
                    for cl in cleaned:
                        if cl == "":
                            if current:
                                paragraphs.append(" ".join(current))
                                current = []
                        else:
                            current.append(cl)
                    if current:
                        paragraphs.append(" ".join(current))
                    body_text = "\n".join(paragraphs)
                else:
                    body_text = "\n".join(cleaned)
                i = j
                continue
            elif rest_stripped and rest_stripped[0] in ("'", '"'):
                body_text, end_idx = parse_quoted_scalar(
                    lines, i, rest_stripped, rest_stripped[0]
                )
                i = end_idx + 1
                continue
            else:
                parts = []
                if rest_stripped and not rest_stripped.startswith("#"):
                    parts.append(strip_inline_comment(rest_stripped))
                j = i + 1
                while j < len(lines):
                    pl = lines[j]
                    ps = pl.strip()
                    if ps == "":
                        j += 1
                        continue
                    pi = len(pl) - len(pl.lstrip(" "))
                    if pi == 0:
                        break
                    if not ps.startswith("#"):
                        parts.append(strip_inline_comment(ps))
                    j += 1
                body_text = " ".join(p for p in parts if p)
                i = j
                continue

        if key == "custom":
            rest_stripped = rest.lstrip(" \t")
            if rest_stripped.startswith("#"):
                rest_stripped = ""
            if rest_stripped.startswith("{"):
                entries, end_idx = parse_flow_mapping(lines, i, rest_stripped)
                upgrade_entries.extend(entries)
                i = end_idx + 1
                continue
            if rest_stripped == "":
                j = i + 1
                child_indent = None
                while j < len(lines):
                    cl = lines[j]
                    cs = cl.strip()
                    if not cs or cs.startswith("#"):
                        j += 1
                        continue
                    ci = len(cl) - len(cl.lstrip(" "))
                    if ci == 0:
                        break
                    if child_indent is None:
                        child_indent = ci
                    if ci == child_indent:
                        cm = TOP_KEY_RE.match(cl.lstrip(" "))
                        if cm:
                            ck = unquote_key(cm.group(1))
                            crest = cm.group(2)
                            if (not crest or crest[0] in (" ", "\t")) and ck == "Upgrade":
                                val, end_idx, was_quoted = parse_scalar_value(
                                    lines, j, crest, child_indent
                                )
                                upgrade_entries.append((val, was_quoted))
                                j = end_idx + 1
                                continue
                    j += 1
                i = j
                continue

        i += 1

    return body_text, upgrade_entries


fragment_paths = sorted(
    path
    for path in glob.glob(os.path.join(unreleased_dir, "*.yaml"))
    if os.path.isfile(path)
)
if not fragment_paths:
    print("no pending .changes/unreleased/*.yaml fragments", file=sys.stderr)
    sys.exit(1)

highest_class = None
highest_rank = -1

for fragment_path in fragment_paths:
    filename = os.path.basename(fragment_path)
    body_text, upgrade_entries = parse_fragment(fragment_path)

    if len(upgrade_entries) == 0:
        print(
            f"{filename}: missing custom.Upgrade (expected one of: {ALLOWED_LIST})",
            file=sys.stderr,
        )
        sys.exit(1)
    if len(upgrade_entries) > 1:
        print(
            f"{filename}: duplicate custom.Upgrade key",
            file=sys.stderr,
        )
        sys.exit(1)

    upgrade_value, _was_quoted = upgrade_entries[0]
    if upgrade_value == "":
        print(
            f"{filename}: empty custom.Upgrade (expected one of: {ALLOWED_LIST})",
            file=sys.stderr,
        )
        sys.exit(1)
    if upgrade_value not in PRECEDENCE:
        print(
            f"{filename}: unknown custom.Upgrade '{upgrade_value}' (expected one of: {ALLOWED_LIST})",
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
