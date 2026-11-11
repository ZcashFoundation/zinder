#!/usr/bin/env bash
# Every file a deploy Dockerfile copies from the build context must survive
# .dockerignore. The image workflow builds only deploy/Dockerfile targets, so
# without this check an excluded file in another Dockerfile surfaces only when
# an operator builds that image.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

# Prints the context sources of each COPY or ADD instruction, one per line.
# Instructions that copy from another stage, and whole-context copies, are
# skipped.
context_sources() {
    sed -e ':join' -e '/\\$/{N;s/\\\n//;b join' -e '}' "$1" |
        awk 'toupper($1) == "COPY" || toupper($1) == "ADD" {
            sources = ""
            from_stage = 0
            count = 0
            for (i = 2; i <= NF; i++) {
                if ($i ~ /^--from=/) { from_stage = 1 }
                if ($i ~ /^--/) { continue }
                args[++count] = $i
            }
            if (from_stage) { next }
            for (i = 1; i < count; i++) {
                if (args[i] != "." && args[i] != "./") { print args[i] }
            }
        }'
}

probe_dir="$(mktemp -d)"
trap 'rm -rf "$probe_dir"' EXIT

status=0
for dockerfile in deploy/Dockerfile deploy/Dockerfile.*; do
    mapfile -t sources < <(context_sources "$dockerfile" | sort -u)
    if [[ ${#sources[@]} -eq 0 ]]; then
        continue
    fi

    probe="$probe_dir/$(basename "$dockerfile").probe"
    {
        echo "FROM scratch"
        for source in "${sources[@]}"; do
            echo "COPY ${source} /context/${source}"
        done
    } > "$probe"

    if docker build --quiet --file "$probe" . > /dev/null; then
        echo "ok: ${dockerfile} (${#sources[@]} context sources)"
    else
        echo "error: ${dockerfile} copies a path that .dockerignore excludes" >&2
        status=1
    fi
done

exit "$status"
