#!/usr/bin/env bash
# Run the same fixture against two separately compiled canonical-store revisions.
set -euo pipefail

repo=$(git rev-parse --show-toplevel)
baseline=00030603b10a1efc202246303af8816c098edc27
case $# in
  0) ;;
  2)
    if [[ $1 != --baseline ]]; then
      printf 'Usage: %s [--baseline <commit>]\n' "$0" >&2
      exit 2
    fi
    baseline=$(git -C "$repo" rev-parse --verify --end-of-options "${2}^{commit}")
    ;;
  *)
    printf 'Usage: %s [--baseline <commit>]\n' "$0" >&2
    exit 2
    ;;
esac
candidate=$(git -C "$repo" rev-parse HEAD)
scratch=$(mktemp -d)
probe=crates/zinder-store/src/canonical_store/v7_interop_probe.rs

cleanup() {
  status=$?
  trap - EXIT
  for checkout in "$scratch/candidate" "$scratch/baseline"; do
    if [[ -e "$checkout/.git" ]]; then
      git -C "$repo" worktree remove --force "$checkout" || status=1
    fi
  done
  rm -rf -- "$scratch"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

prepare() {
  local label=$1 revision=$2
  local checkout="$scratch/$label"
  git -C "$repo" worktree add --detach "$checkout" "$revision"
  git -C "$repo" show "$candidate:scripts/fixtures/canonical_v7_interop.rs" > "$checkout/$probe"
  printf '\n#[cfg(test)]\nmod v7_interop_probe;\n' >> "${checkout}/crates/zinder-store/src/canonical_store/mod.rs"
}

run() (
  local label=$1 create=$2 read=$3
  cd "$scratch/$label"
  printf 'v7 interoperability at %s, write=%s, read=%s\n' "$(git rev-parse HEAD)" "${create:-none}" "${read:-none}"
  CARGO_TARGET_DIR="$PWD/target" \
    CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-4}" \
    CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 CXXFLAGS=-g0 \
    ZINDER_V7_INTEROP_ROOT="$scratch" ZINDER_V7_INTEROP_CREATE="$create" ZINDER_V7_INTEROP_READ="$read" \
    cargo nextest run --locked --profile=ci -p zinder-store --lib -E 'test(=canonical_store::v7_interop_probe::v7_interop)'
)

prepare baseline "$baseline"
prepare candidate "$candidate"
run baseline baseline ""
run candidate candidate baseline
run baseline "" candidate
