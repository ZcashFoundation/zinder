#!/usr/bin/env bash
# Checks that every ChaCha backend this host can run produces the pinned
# LtHash16 expansion bytes.
#
# The wallet accumulators expand elements with the `chacha20` crate, whose
# backend is chosen at build time and at run time: AVX2 or SSE2 dispatch by
# default, AVX-512 dispatch with `--cfg chacha20_avx512` (the x86-64 release
# build), NEON on AArch64, and the portable code with
# `--cfg chacha20_backend="soft"`. Each build runs the same tests, which compare
# full expansions with independently computed vectors, so passing every build
# shows the backends agree byte for byte.
#
# Usage: scripts/test-lthash16-backends.sh
# RUSTFLAGS from the environment is kept and extended per backend. Each backend
# builds in its own target directory under target/lthash16-backends.
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

base_rustflags="${RUSTFLAGS:-}"
backends=(dispatch soft)
case "$(uname -m)" in
  x86_64) backends+=(avx512) ;;
esac

host_has_avx512=false
if grep -qw avx512f /proc/cpuinfo 2>/dev/null && grep -qw avx512vl /proc/cpuinfo 2>/dev/null; then
  host_has_avx512=true
fi

tests=(lthash16:: utxo_set_commitment::tests::display_digest_matches_reference_for_known_set)
for backend in "${backends[@]}"; do
  case "$backend" in
    dispatch) flags="$base_rustflags" ;;
    soft) flags="$base_rustflags --cfg chacha20_backend=\"soft\"" ;;
    avx512) flags="$base_rustflags --cfg chacha20_avx512" ;;
  esac
  if [[ "$backend" == avx512 && "$host_has_avx512" != true ]]; then
    echo "[lthash16-backends] host lacks AVX-512F/VL; the avx512 build exercises its AVX2 fallback"
  fi
  echo "[lthash16-backends] backend=$backend RUSTFLAGS=${flags# }"
  for test in "${tests[@]}"; do
    log="$(RUSTFLAGS="${flags# }" CARGO_TARGET_DIR="target/lthash16-backends/$backend" \
      cargo test --locked -p zinder-core --lib -- "$test" 2>&1)" || {
      printf '%s\n' "$log"
      exit 1
    }
    # A filter that matches nothing passes silently, so require a test to run.
    if ! grep -Eq '^test result: ok\. [1-9][0-9]* passed' <<< "$log"; then
      printf '%s\n' "$log"
      echo "[lthash16-backends] filter $test ran no tests" >&2
      exit 1
    fi
    grep -E '^test .* \.\.\. ok$' <<< "$log"
  done
done
echo "[lthash16-backends] all backends reproduce the pinned expansion vectors"
