#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage: check-release-binary-archive.sh ARCHIVE \
  --version VERSION --tag TAG --commit SHA --target RUST_TARGET \
  --source-date-epoch EPOCH
EOF
  exit 2
}

[[ $# -ge 1 ]] || usage
archive_path="$1"
shift
version=""
release_tag=""
commit=""
rust_target=""
source_date_epoch=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --version) version="$2"; shift 2 ;;
    --tag) release_tag="$2"; shift 2 ;;
    --commit) commit="$2"; shift 2 ;;
    --target) rust_target="$2"; shift 2 ;;
    --source-date-epoch) source_date_epoch="$2"; shift 2 ;;
    *) usage ;;
  esac
done

[[ -s "$archive_path" && -n "$version" && "$release_tag" == "v${version}" ]] || usage
[[ "$commit" =~ ^[0-9a-f]{40}$ && "$source_date_epoch" =~ ^[0-9]+$ ]] || usage
case "$rust_target" in
  x86_64-unknown-linux-gnu)
    release_platform=x86_64-v3-unknown-linux-gnu
    cpu_baseline=x86-64-v3
    elf_machine='Advanced Micro Devices X86-64'
    ;;
  aarch64-unknown-linux-gnu)
    release_platform=aarch64-unknown-linux-gnu
    cpu_baseline=armv8-a
    elf_machine=AArch64
    ;;
  *) usage ;;
esac

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

expected_members="$({
  printf '%s\n' \
    "$archive_root/" \
    "$archive_root/BUILD-INFO.json" \
    "$archive_root/LICENSE" \
    "$archive_root/README.md" \
    "$archive_root/SHA256SUMS" \
    "$archive_root/bin/" \
    "$archive_root/bin/zinder-compat-lightwalletd" \
    "$archive_root/bin/zinder-explorer" \
    "$archive_root/bin/zinder-ingest" \
    "$archive_root/bin/zinder-projector" \
    "$archive_root/bin/zinder-query"
})"
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
for release_directory in "$staging_root" "$staging_root/bin"; do
  [[ "$(stat -c '%a' "$release_directory")" == 755 ]] || {
    echo >&2 "release archive directory has a noncanonical mode: $release_directory"
    exit 1
  }
done

expected_files="$({
  printf '%s\n' \
    BUILD-INFO.json \
    LICENSE \
    README.md \
    SHA256SUMS \
    bin/zinder-compat-lightwalletd \
    bin/zinder-explorer \
    bin/zinder-ingest \
    bin/zinder-projector \
    bin/zinder-query
})"
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

build_info="$staging_root/BUILD-INFO.json"
jq -e \
  --arg version "$version" \
  --arg tag "$release_tag" \
  --arg commit "$commit" \
  --arg rust_target "$rust_target" \
  --arg release_platform "$release_platform" \
  --arg cpu_baseline "$cpu_baseline" \
  --argjson source_date_epoch "$source_date_epoch" '
    .schema_version == 1
    and .version == $version
    and .tag == $tag
    and .commit == $commit
    and .rust_target == $rust_target
    and .release_platform == $release_platform
    and .cpu_baseline == $cpu_baseline
    and .libc.family == "glibc"
    and .libc.minimum_runtime_version == "2.34"
    and .libc.dynamic_libstdcpp == true
    and .libc.minimum_libstdcpp_symbol == "GLIBCXX_3.4.30"
    and .source_date_epoch == $source_date_epoch
    and ([.binaries[].name] | sort) == [
      "zinder-compat-lightwalletd",
      "zinder-explorer",
      "zinder-ingest",
      "zinder-projector",
      "zinder-query"
    ]
  ' "$build_info" >/dev/null

while IFS= read -r binary_name; do
  binary_path="$staging_root/bin/$binary_name"
  expected_sha256="$(jq -r --arg name "$binary_name" '.binaries[] | select(.name == $name) | .sha256' "$build_info")"
  expected_size="$(jq -r --arg name "$binary_name" '.binaries[] | select(.name == $name) | .size' "$build_info")"
  [[ "$(sha256sum "$binary_path" | awk '{print $1}')" == "$expected_sha256" ]]
  [[ "$(stat -c '%s' "$binary_path")" == "$expected_size" ]]
  [[ "$("$binary_path" --version)" == "$binary_name $version" ]]
  "$binary_path" --help >/dev/null
  strings "$binary_path" | grep -F "$commit" >/dev/null

  if [[ "${ZINDER_RELEASE_SKIP_ELF_CHECKS:-false}" != true ]]; then
    grep -Fq 'ELF 64-bit' <<< "$(file "$binary_path")"
    actual_machine="$(
      readelf -h "$binary_path" \
        | sed -n 's/^[[:space:]]*Machine:[[:space:]]*//p'
    )"
    [[ "$actual_machine" == "$elf_machine" ]]
    ldd_output="$(ldd "$binary_path")"
    ! grep -Fq 'not found' <<< "$ldd_output"
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
done < <(jq -r '.binaries[].name' "$build_info")
