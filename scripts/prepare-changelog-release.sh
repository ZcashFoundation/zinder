#!/usr/bin/env bash
set -euo pipefail

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
validator="$repository_root/scripts/validate-changelog.sh"
release_identity_validator="$repository_root/scripts/validate-release-tag.sh"
changie_bin="${CHANGIE_BIN:-changie}"

if [[ $# -ne 1 ]]; then
  echo >&2 "usage: prepare-changelog-release.sh VERSION"
  exit 2
fi

release_version="${1#v}"
semver_pattern='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*))?$'
if [[ ! "$release_version" =~ $semver_pattern ]]; then
  echo >&2 "release version must be complete SemVer without build metadata"
  exit 1
fi

normalize_changelog_end() {
  while [[ -z "$(tail -n 1 "$repository_root/CHANGELOG.md")" ]]; do
    sed -i '${/^$/d;}' "$repository_root/CHANGELOG.md"
  done
}

stable_version="${release_version%%-*}"
prerelease=false
[[ "$release_version" == "$stable_version" ]] || prerelease=true

bash "$release_identity_validator" "v${release_version}" >/dev/null

pending_fragment="$({
  find "$repository_root/.changes/unreleased" \
    -maxdepth 1 \
    -type f \
    -name '*.yaml' \
    -print \
    -quit
} || true)"
escaped_release_version="${release_version//./\\.}"
section_heading_pattern="^## \\[${escaped_release_version}\\] - [0-9]{4}-[0-9]{2}-[0-9]{2}$"
section_count="$(grep -Ec -- "$section_heading_pattern" "$repository_root/CHANGELOG.md" || true)"

if [[ "$section_count" -ne 0 ]]; then
  normalize_changelog_end
  CHANGIE_BIN="$changie_bin" bash "$validator" release "v${release_version}" "$repository_root"
  echo "changelog v${release_version} is already prepared"
  exit 0
fi
if [[ -z "$pending_fragment" ]]; then
  CHANGIE_BIN="$changie_bin" bash "$validator" release "v${release_version}" "$repository_root"
fi

upgrade_class_line="$(
  CHANGIE_BIN="$changie_bin" bash "$validator" upgrade-line "$repository_root"
)"

next_version="$(
  (
    cd "$repository_root"
    prerelease_staging_directory="$(
      mktemp -d "${TMPDIR:-/tmp}/zinder-changie-prereleases.XXXXXX"
    )"
    # shellcheck disable=SC2329
    restore_prerelease_versions() {
      while IFS= read -r -d '' staged_version; do
        mv "$staged_version" "$repository_root/.changes/"
      done < <(
        find "$prerelease_staging_directory" \
          -maxdepth 1 \
          -type f \
          -print0
      )
      rmdir "$prerelease_staging_directory"
    }
    trap restore_prerelease_versions EXIT

    while IFS= read -r -d '' prerelease_version; do
      mv "$prerelease_version" "$prerelease_staging_directory/"
    done < <(
      find "$repository_root/.changes" \
        -maxdepth 1 \
        -type f \
        -name 'v*-*.md' \
        -print0
    )

    "$changie_bin" next auto
  )
)"
if [[ "${next_version#v}" != "$stable_version" ]]; then
  echo >&2 \
    "requested v${release_version}, but pending fragment impacts require ${next_version}"
  exit 1
fi

(
  cd "$repository_root"
  if [[ "$prerelease" == true ]]; then
    earlier_prerelease="$(
      find "$repository_root/.changes" \
        -maxdepth 1 \
        -type f \
        -name "v${stable_version}-*.md" \
        -print \
        -quit
    )"
    if [[ -n "$earlier_prerelease" ]]; then
      "$changie_bin" batch \
        "v${release_version}" \
        --keep \
        --remove-prereleases \
        --allow-no-changes=false
    fi
    "$changie_bin" batch \
      "v${release_version}" \
      --keep \
      --allow-no-changes=false
  else
    "$changie_bin" batch \
      "v${release_version}" \
      --remove-prereleases \
      --allow-no-changes=false
  fi

  version_file="$repository_root/.changes/v${release_version}.md"
  tmp_version_file="$(mktemp "$repository_root/.changes/.v${release_version}.XXXXXX")"
  trap 'rm -f -- "$tmp_version_file"' EXIT
  awk -v upgrade_line="$upgrade_class_line" '
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
  ' "$version_file" > "$tmp_version_file"
  mv "$tmp_version_file" "$version_file"
  trap - EXIT

  "$changie_bin" merge --include-unreleased '## [Unreleased]'
)
normalize_changelog_end
CHANGIE_BIN="$changie_bin" bash "$validator" release "v${release_version}" "$repository_root"
echo "prepared changelog v${release_version}"
