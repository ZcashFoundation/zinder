#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage: check-release-binary-archive.sh ARCHIVE \
  --version VERSION --tag TAG --commit SHA --target RUST_TARGET \
  --source-date-epoch EPOCH [--cpu-baseline ISA] [--pasta-asm 0|1]
EOF
  exit 2
}

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
[[ $# -ge 1 ]] || usage
archive_path="$1"
shift
version=""
release_tag=""
commit=""
rust_target=""
source_date_epoch=""
pasta_asm=1
requested_cpu_baseline=""
while [[ $# -gt 0 ]]; do
  [[ $# -ge 2 ]] || usage
  case "$1" in
    --version) version="$2"; shift 2 ;;
    --tag) release_tag="$2"; shift 2 ;;
    --commit) commit="$2"; shift 2 ;;
    --target) rust_target="$2"; shift 2 ;;
    --source-date-epoch) source_date_epoch="$2"; shift 2 ;;
    --cpu-baseline) requested_cpu_baseline="$2"; shift 2 ;;
    --pasta-asm) pasta_asm="$2"; shift 2 ;;
    *) usage ;;
  esac
done

[[ -s "$archive_path" && -n "$version" && "$release_tag" == "v${version}" ]] || usage
[[ "$commit" =~ ^[0-9a-f]{40}$ && "$source_date_epoch" =~ ^[0-9]+$ ]] || usage
case "$pasta_asm" in
  0|1) ;;
  *)
    echo >&2 "unsupported --pasta-asm value (expected 0 or 1): $pasta_asm"
    exit 1
    ;;
esac
# shellcheck source=scripts/release-binary-platform.sh
source "$repository_root/scripts/release-binary-platform.sh"
resolve_release_binary_platform "$rust_target" "$requested_cpu_baseline" "$pasta_asm"

archive_root="zinder-${version}-${release_platform}"
member_list="$(tar -tzf "$archive_path")"
if grep -Eq '(^/|(^|/)\.\.(/|$))' <<< "$member_list"; then
  echo >&2 "release binary archive contains an unsafe path"
  exit 1
fi
if tar -tvzf "$archive_path" | awk '$1 ~ /^[lh]/ { found = 1 } END { exit !found }'; then
  echo >&2 "release binary archive contains a link"
  exit 1
fi
if [[ "$(wc -l <<< "$member_list")" -ne "$(LC_ALL=C sort -u <<< "$member_list" | wc -l)" ]]; then
  echo >&2 "release binary archive contains a duplicate member"
  exit 1
fi

file_catalog=(BUILD-INFO.json LICENSE README.md SHA256SUMS bin/zinder)
native_assets=()
if [[ "$rust_target" == *-linux-gnu ]]; then
  native_units="$repository_root/deploy/systemd/native"
  native_assets=("$native_units"/*.service "$native_units"/*.target "$native_units/zinder.env")
  for asset in "${native_assets[@]}"; do
    file_catalog+=("systemd/${asset##*/}")
  done
fi
expected_members="$(
  printf '%s\n' "$archive_root/" "$archive_root/bin/"
  if [[ "$rust_target" == *-linux-gnu ]]; then
    printf '%s/systemd/\n' "$archive_root"
  fi
  printf '%s\n' "${file_catalog[@]/#/$archive_root/}"
)"
actual_members="$(LC_ALL=C sort <<< "$member_list")"
expected_members="$(LC_ALL=C sort <<< "$expected_members")"
[[ "$actual_members" == "$expected_members" ]] || {
  echo >&2 "release binary archive has the wrong member catalog"
  diff -u <(printf '%s\n' "$expected_members") <(printf '%s\n' "$actual_members") || true
  exit 1
}

scratch_directory="$(mktemp -d "${TMPDIR:-/tmp}/zinder-release-check.XXXXXX")"
trap 'rm -rf -- "$scratch_directory"' EXIT
tar --extract --gzip --same-permissions --file "$archive_path" --directory "$scratch_directory"
staging_root="$scratch_directory/$archive_root"
[[ -d "$staging_root" ]] || {
  echo >&2 "release binary archive has the wrong root directory"
  exit 1
}
expected_files="$(printf '%s\n' "${file_catalog[@]}" | LC_ALL=C sort)"
actual_files="$(find "$staging_root" -type f -printf '%P\n' | LC_ALL=C sort)"
[[ "$actual_files" == "$expected_files" ]] || {
  echo >&2 "release binary archive has the wrong file catalog"
  diff -u <(printf '%s\n' "$expected_files") <(printf '%s\n' "$actual_files") || true
  exit 1
}

while IFS= read -r relative_path; do
  expected_mode=644
  [[ "$relative_path" == bin/* ]] && expected_mode=755
  actual_mode="$(stat -c '%a' "$staging_root/$relative_path")"
  [[ "$actual_mode" == "$expected_mode" ]] || {
    echo >&2 "release archive mode for $relative_path is $actual_mode, expected $expected_mode"
    exit 1
  }
done <<< "$expected_files"

while IFS= read -r directory_path; do
  [[ "$(stat -c '%a' "$directory_path")" == 755 ]] || {
    echo >&2 "release archive directory has a noncanonical mode: $directory_path"
    exit 1
  }
done < <(find "$staging_root" -type d -print)

while IFS= read -r archive_path_entry; do
  [[ "$(stat -c '%Y' "$archive_path_entry")" == "$source_date_epoch" ]] || {
    echo >&2 "release archive member has a noncanonical timestamp: $archive_path_entry"
    exit 1
  }
done < <(find "$staging_root" -print)

if tar --numeric-owner -tvzf "$archive_path" \
  | awk '$2 != "0/0" { exit 1 }'; then
  :
else
  echo >&2 "release binary archive members must use numeric owner 0/0"
  exit 1
fi

expected_checksum_files="$(grep -Fvx SHA256SUMS <<< "$expected_files")"
actual_checksum_files="$(awk '{print $2}' "$staging_root/SHA256SUMS" | LC_ALL=C sort)"
[[ "$actual_checksum_files" == "$expected_checksum_files" ]] || {
  echo >&2 "release archive checksum catalog differs from the file catalog"
  exit 1
}

(
  cd "$staging_root"
  sha256sum --check --strict SHA256SUMS >/dev/null
)

for asset in "${native_assets[@]}"; do
  cmp "$asset" "$staging_root/systemd/${asset##*/}"
done

build_info="$staging_root/BUILD-INFO.json"
jq -e \
  --arg version "$version" \
  --arg tag "$release_tag" \
  --arg commit "$commit" \
  --arg rust_target "$rust_target" \
  --arg release_platform "$release_platform" \
  --arg cpu_baseline "$cpu_baseline" \
  --argjson libc "$libc" \
  --argjson required_cpu_features "$required_cpu_features" \
  --argjson source_date_epoch "$source_date_epoch" '
    .schema_version == 1
    and .version == $version
    and .tag == $tag
    and .commit == $commit
    and .rust_target == $rust_target
    and .release_platform == $release_platform
    and .cpu_baseline == $cpu_baseline
    and .required_cpu_features == $required_cpu_features
    and .libc == $libc
    and .source_date_epoch == $source_date_epoch
    and ([.binaries[].name] | sort) == [
      "zinder"
    ]
  ' "$build_info" >/dev/null

while IFS= read -r binary_name; do
  binary_path="$staging_root/bin/$binary_name"
  expected_sha256="$(jq -r --arg name "$binary_name" '.binaries[] | select(.name == $name) | .sha256' "$build_info")"
  expected_size="$(jq -r --arg name "$binary_name" '.binaries[] | select(.name == $name) | .size' "$build_info")"
  [[ "$(sha256sum "$binary_path" | awk '{print $1}')" == "$expected_sha256" ]]
  [[ "$(stat -c '%s' "$binary_path")" == "$expected_size" ]]
  if [[ "$rust_target" == aarch64-apple-darwin ]]; then
    [[ "$(lipo -archs "$binary_path")" == arm64 ]]
    vtool -show-build "$binary_path" | awk '
      $1 == "platform" && $2 == "MACOS" { macos = 1 }
      $1 == "minos" && $2 == "11.0" { baseline = 1 }
      END { exit !(macos && baseline) }
    '
    otool -L "$binary_path" | awk '
      NR == 1 { next }
      $1 == "/usr/lib/libSystem.B.dylib" { has_libsystem = 1 }
      $1 == "/usr/lib/libc++.1.dylib" { cxx = 1 }
      $1 !~ /^\/(usr\/lib|System\/Library)\// { external = 1 }
      END { exit !(has_libsystem && cxx && !external) }
    '
  elif [[ "${ZINDER_RELEASE_SKIP_ELF_CHECKS:-false}" != true ]]; then
    grep -Fq 'ELF 64-bit' <<< "$(file "$binary_path")"
    actual_machine="$(
      readelf -h "$binary_path" \
        | sed -n 's/^[[:space:]]*Machine:[[:space:]]*//p'
    )"
    [[ "$actual_machine" == "$elf_machine" ]]
    ldd_output="$(ldd "$binary_path")"
    if grep -Fq 'not found' <<< "$ldd_output"; then
      echo >&2 "$binary_name requires a missing dynamic library"
      exit 1
    fi
    grep -Fq 'libstdc++.so.6' <<< "$ldd_output"
    glibc_ceiling="$(
      readelf --version-info "$binary_path" \
        | grep -Eo 'GLIBC_[0-9]+\.[0-9]+' \
        | sort -Vu \
        | tail -n 1
    )"
    [[ -n "$glibc_ceiling" ]]
    [[ "$(printf '%s\n' "$glibc_ceiling" GLIBC_2.34 | sort -V | tail -n 1)" == GLIBC_2.34 ]] || {
      echo >&2 "$binary_name requires $glibc_ceiling, above the GLIBC_2.34 release ceiling"
      exit 1
    }
    glibcxx_ceiling="$(
      readelf --version-info "$binary_path" \
        | grep -Eo 'GLIBCXX_[0-9]+\.[0-9]+(\.[0-9]+)?' \
        | sort -Vu \
        | tail -n 1
    )"
    [[ -n "$glibcxx_ceiling" ]]
    [[ "$(printf '%s\n' "$glibcxx_ceiling" GLIBCXX_3.4.30 | sort -V | tail -n 1)" == GLIBCXX_3.4.30 ]] || {
      echo >&2 "$binary_name requires $glibcxx_ceiling, above the GLIBCXX_3.4.30 release ceiling"
      exit 1
    }
  fi
  [[ "$("$binary_path" --version)" == "$binary_name $version" ]]
  "$binary_path" --help >/dev/null
  strings "$binary_path" | grep -F "$commit" >/dev/null
done < <(jq -r '.binaries[].name' "$build_info")
