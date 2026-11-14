#!/usr/bin/env bash
set -euo pipefail

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
builder="$repository_root/scripts/build-release-binary-archive.sh"
checker="$repository_root/scripts/check-release-binary-archive.sh"
temporary_directory="$(mktemp -d "${TMPDIR:-/tmp}/zinder-binary-archive-test.XXXXXX")"
trap 'rm -rf -- "$temporary_directory"' EXIT

fail() {
  echo >&2 "release binary archive test failed: $*"
  exit 1
}

fixture_binaries="$temporary_directory/binaries"
mkdir -p "$fixture_binaries"
for binary_name in zinder-ingest zinder-projector zinder-query zinder-compat-lightwalletd zinder-explorer; do
  printf '#!/usr/bin/env bash\n# 0123456789abcdef0123456789abcdef01234567\nprintf "%%s 0.5.0-rc.1\\n" "$(basename "$0")"\n' \
    > "$fixture_binaries/$binary_name"
  chmod 755 "$fixture_binaries/$binary_name"
done

first_output="$temporary_directory/first"
second_output="$temporary_directory/second"
mkdir -p "$first_output" "$second_output"
for output_directory in "$first_output" "$second_output"; do
  "$builder" \
    --binaries "$fixture_binaries" \
    --output "$output_directory" \
    --version 0.5.0-rc.1 \
    --tag v0.5.0-rc.1 \
    --commit 0123456789abcdef0123456789abcdef01234567 \
    --target x86_64-unknown-linux-gnu \
    --source-date-epoch 1735689600
done

asset_name=zinder-0.5.0-rc.1-x86_64-v3-unknown-linux-gnu.tar.gz
first_asset="$first_output/$asset_name"
second_asset="$second_output/$asset_name"
[[ -s "$first_asset" && -s "$second_asset" ]] \
  || fail "the expected RC-preserving x86_64-v3 asset was not created"
[[ "$(sha256sum "$first_asset" | awk '{print $1}')" == \
   "$(sha256sum "$second_asset" | awk '{print $1}')" ]] \
  || fail "identical inputs did not produce an identical archive"

ZINDER_RELEASE_SKIP_ELF_CHECKS=true \
  "$checker" "$first_asset" \
    --version 0.5.0-rc.1 \
    --tag v0.5.0-rc.1 \
    --commit 0123456789abcdef0123456789abcdef01234567 \
    --target x86_64-unknown-linux-gnu \
    --source-date-epoch 1735689600

arm_output="$temporary_directory/arm"
mkdir -p "$arm_output"
"$builder" \
  --binaries "$fixture_binaries" \
  --output "$arm_output" \
  --version 0.5.0-rc.1 \
  --tag v0.5.0-rc.1 \
  --commit 0123456789abcdef0123456789abcdef01234567 \
  --target aarch64-unknown-linux-gnu \
  --source-date-epoch 1735689600
arm_asset="$arm_output/zinder-0.5.0-rc.1-aarch64-unknown-linux-gnu.tar.gz"
[[ -s "$arm_asset" ]] || fail "the aarch64 GNU asset was not created"
ZINDER_RELEASE_SKIP_ELF_CHECKS=true \
  "$checker" "$arm_asset" \
    --version 0.5.0-rc.1 \
    --tag v0.5.0-rc.1 \
    --commit 0123456789abcdef0123456789abcdef01234567 \
    --target aarch64-unknown-linux-gnu \
    --source-date-epoch 1735689600

cp "$fixture_binaries/zinder-ingest" "$fixture_binaries/zinder-bench"
if "$builder" \
  --binaries "$fixture_binaries" \
  --output "$temporary_directory/rejected-catalog" \
  --version 0.5.0-rc.1 \
  --tag v0.5.0-rc.1 \
  --commit 0123456789abcdef0123456789abcdef01234567 \
  --target x86_64-unknown-linux-gnu \
  --source-date-epoch 1735689600 >/dev/null 2>&1; then
  fail "an archive input containing zinder-bench was admitted"
fi
rm "$fixture_binaries/zinder-bench"

if ZINDER_RELEASE_SKIP_ELF_CHECKS=true \
  "$checker" "$first_asset" \
    --version 0.5.0-rc.1 \
    --tag v0.5.0-rc.1 \
    --commit fedcba9876543210fedcba9876543210fedcba98 \
    --target x86_64-unknown-linux-gnu \
    --source-date-epoch 1735689600 >/dev/null 2>&1; then
  fail "an archive with the wrong expected commit was admitted"
fi

cpu_baseline_root="$temporary_directory/cpu-baseline"
mkdir -p "$cpu_baseline_root"
tar --extract --gzip --same-permissions --file "$first_asset" --directory "$cpu_baseline_root"
root_name="$(find "$cpu_baseline_root" -mindepth 1 -maxdepth 1 -type d -printf '%f\n')"
jq '.cpu_baseline = "x86-64-v2"' \
  "$cpu_baseline_root/$root_name/BUILD-INFO.json" \
  > "$cpu_baseline_root/$root_name/BUILD-INFO.json.updated"
mv \
  "$cpu_baseline_root/$root_name/BUILD-INFO.json.updated" \
  "$cpu_baseline_root/$root_name/BUILD-INFO.json"
(
  cd "$cpu_baseline_root/$root_name"
  find bin -maxdepth 1 -type f -printf '%p\n' \
    | LC_ALL=C sort \
    | xargs sha256sum
  printf '%s\n' BUILD-INFO.json LICENSE README.md \
    | LC_ALL=C sort \
    | xargs sha256sum
) | LC_ALL=C sort -k2 > "$cpu_baseline_root/$root_name/SHA256SUMS"
chmod 0644 \
  "$cpu_baseline_root/$root_name/BUILD-INFO.json" \
  "$cpu_baseline_root/$root_name/SHA256SUMS"
find "$cpu_baseline_root/$root_name" -exec touch -h -d '@1735689600' {} +
wrong_cpu_baseline_asset="$temporary_directory/wrong-cpu-baseline.tar.gz"
LC_ALL=C tar \
  --directory "$cpu_baseline_root" \
  --sort=name \
  --format=gnu \
  --mtime='@1735689600' \
  --owner=0 \
  --group=0 \
  --numeric-owner \
  -cf - \
  "$root_name" \
  | gzip -n > "$wrong_cpu_baseline_asset"
if ZINDER_RELEASE_SKIP_ELF_CHECKS=true \
  "$checker" "$wrong_cpu_baseline_asset" \
    --version 0.5.0-rc.1 \
    --tag v0.5.0-rc.1 \
    --commit 0123456789abcdef0123456789abcdef01234567 \
    --target x86_64-unknown-linux-gnu \
    --source-date-epoch 1735689600 >/dev/null 2>&1; then
  fail "an archive with the wrong CPU baseline was admitted"
fi

tamper_root="$temporary_directory/tamper"
mkdir -p "$tamper_root"
tar --extract --gzip --same-permissions --file "$first_asset" --directory "$tamper_root"
root_name="$(find "$tamper_root" -mindepth 1 -maxdepth 1 -type d -printf '%f\n')"
printf 'tampered\n' >> "$tamper_root/$root_name/bin/zinder-query"
tampered_asset="$temporary_directory/tampered.tar.gz"
tar -C "$tamper_root" -czf "$tampered_asset" "$root_name"
if ZINDER_RELEASE_SKIP_ELF_CHECKS=true \
  "$checker" "$tampered_asset" \
    --version 0.5.0-rc.1 \
    --tag v0.5.0-rc.1 \
    --commit 0123456789abcdef0123456789abcdef01234567 \
    --target x86_64-unknown-linux-gnu \
    --source-date-epoch 1735689600 >/dev/null 2>&1; then
  fail "a checksum-tampered archive was admitted"
fi

mode_root="$temporary_directory/mode"
mkdir -p "$mode_root"
tar --extract --gzip --same-permissions --file "$first_asset" --directory "$mode_root"
chmod 0600 "$mode_root/$root_name/README.md"
mode_asset="$temporary_directory/wrong-mode.tar.gz"
tar -C "$mode_root" -czf "$mode_asset" "$root_name"
if ZINDER_RELEASE_SKIP_ELF_CHECKS=true \
  "$checker" "$mode_asset" \
    --version 0.5.0-rc.1 \
    --tag v0.5.0-rc.1 \
    --commit 0123456789abcdef0123456789abcdef01234567 \
    --target x86_64-unknown-linux-gnu \
    --source-date-epoch 1735689600 >/dev/null 2>&1; then
  fail "an archive with a noncanonical file mode was admitted"
fi

timestamp_root="$temporary_directory/timestamp"
mkdir -p "$timestamp_root"
tar --extract --gzip --same-permissions --file "$first_asset" --directory "$timestamp_root"
touch -d '@1735689601' "$timestamp_root/$root_name/README.md"
timestamp_asset="$temporary_directory/wrong-timestamp.tar.gz"
tar -C "$timestamp_root" -czf "$timestamp_asset" "$root_name"
if ZINDER_RELEASE_SKIP_ELF_CHECKS=true \
  "$checker" "$timestamp_asset" \
    --version 0.5.0-rc.1 \
    --tag v0.5.0-rc.1 \
    --commit 0123456789abcdef0123456789abcdef01234567 \
    --target x86_64-unknown-linux-gnu \
    --source-date-epoch 1735689600 >/dev/null 2>&1; then
  fail "an archive with a noncanonical timestamp was admitted"
fi

duplicate_asset="$temporary_directory/duplicate-member.tar.gz"
gzip -dc "$first_asset" > "$temporary_directory/duplicate-member.tar"
tar --append \
  --file "$temporary_directory/duplicate-member.tar" \
  --directory "$timestamp_root" \
  "$root_name/README.md"
gzip -n < "$temporary_directory/duplicate-member.tar" > "$duplicate_asset"
if ZINDER_RELEASE_SKIP_ELF_CHECKS=true \
  "$checker" "$duplicate_asset" \
    --version 0.5.0-rc.1 \
    --tag v0.5.0-rc.1 \
    --commit 0123456789abcdef0123456789abcdef01234567 \
    --target x86_64-unknown-linux-gnu \
    --source-date-epoch 1735689600 >/dev/null 2>&1; then
  fail "an archive containing a duplicate member was admitted"
fi

malicious_root="$temporary_directory/malicious"
mkdir -p "$malicious_root"
printf 'escape\n' > "$malicious_root/payload"
malicious_asset="$temporary_directory/traversal.tar.gz"
tar -C "$malicious_root" -czf "$malicious_asset" \
  --transform='s#^payload$#../payload#' payload
if "$checker" "$malicious_asset" \
  --version 0.5.0-rc.1 \
  --tag v0.5.0-rc.1 \
  --commit 0123456789abcdef0123456789abcdef01234567 \
  --target x86_64-unknown-linux-gnu \
  --source-date-epoch 1735689600 >/dev/null 2>&1; then
  fail "an archive containing a traversal path was admitted"
fi

echo "release binary archive tests passed"
