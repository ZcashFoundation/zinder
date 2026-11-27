#!/usr/bin/env bash
# Lints fenced Bash blocks in a Zinder runbook for syntactic
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
  function indent_columns(whitespace, column,    position) {
    for (position = 1; position <= length(whitespace); position++)
      column += substr(whitespace, position, 1) == "\t" ? 4 - column % 4 : 1
    return column
  }

  function strip_indent(line, width,    position, columns, whitespace, leftover) {
    for (position = 1; columns < width && (whitespace = substr(line, position, 1)) ~ /[ \t]/; position++)
      columns = indent_columns(whitespace, columns)
    leftover = columns > width ? columns - width : 0
    return sprintf("%*s", leftover, "") substr(line, position)
  }

  function close_fence() {
    if (out != "") close(out)
    out = ""
    fence_len = 0
  }

  {
    match($0, /^[ \t]*/)
    line_indent = indent_columns(substr($0, 1, RLENGTH), 0)
    content = substr($0, RLENGTH + 1)

    if (fence_len && (content == "" || line_indent >= fence_margin)) {
      if (line_indent <= fence_margin + 3 && match(content, /^(```+|~~~+)/) &&
          RLENGTH >= fence_len && substr(content, 1, 1) == fence_char &&
          substr(content, RLENGTH + 1) ~ /^[ \t]*$/)
        close_fence()
      else if (out != "")
        print strip_indent($0, open_indent) > out
      next
    }
    if (fence_len) close_fence()
    if (content == "") next

    while (list_depth && line_indent < list_margin[list_depth]) list_depth--
    container_margin = list_depth ? list_margin[list_depth] : 0
    if (line_indent <= container_margin + 3 && match(content, /^([0-9]+[.)]|[-+*])[ \t]+/)) {
      list_prefix = substr(content, 1, RLENGTH)
      content = substr(content, RLENGTH + 1)
      padding_start = match(list_prefix, /[ \t]/)
      marker_end = line_indent + padding_start - 1
      line_indent = indent_columns(substr(list_prefix, padding_start), marker_end)
      container_margin = line_indent <= marker_end + 4 && content != "" ? line_indent : marker_end + 1
      list_margin[++list_depth] = container_margin
    }

    if (line_indent > container_margin + 3 || !match(content, /^(```+|~~~+)/)) next
    delimiter_len = RLENGTH
    delimiter_char = substr(content, 1, 1)
    info_string = substr(content, delimiter_len + 1)
    if (delimiter_char == "`" && info_string ~ /`/) next

    # The container determines where a fence can close. The opener determines how much to strip.
    fence_margin = container_margin
    open_indent = line_indent
    fence_len = delimiter_len
    fence_char = delimiter_char
    if (info_string ~ /^[ \t]*bash([ \t]|$)/) {
      block_idx += 1
      out = sprintf("%s/block-%04d.sh", lint_directory, block_idx)
    }
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
