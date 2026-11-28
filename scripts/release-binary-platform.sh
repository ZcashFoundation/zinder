#!/usr/bin/env bash
# shellcheck shell=bash
# shellcheck disable=SC2034

resolve_release_binary_platform() {
  local rust_target="$1" requested_cpu_baseline="$2" pasta_asm="$3"
  libc='{"family":"glibc","minimum_runtime_version":"2.34","dynamic_libstdcpp":true,"minimum_libstdcpp_symbol":"GLIBCXX_3.4.30"}'
  required_cpu_features='[]'
  case "${rust_target}:${requested_cpu_baseline}" in
    x86_64-unknown-linux-gnu:|x86_64-unknown-linux-gnu:x86-64-v3)
      release_platform=x86_64-v3-unknown-linux-gnu
      cpu_baseline=x86-64-v3
      elf_machine='Advanced Micro Devices X86-64'
      if [[ "$pasta_asm" == 1 ]]; then
        required_cpu_features='["adx","pclmulqdq"]'
      else
        required_cpu_features='["pclmulqdq"]'
      fi
      ;;
    x86_64-unknown-linux-gnu:x86-64)
      [[ "$pasta_asm" == 0 ]] || {
        echo >&2 'the x86-64 baseline requires --pasta-asm 0'
        return 1
      }
      release_platform="$rust_target"
      cpu_baseline=x86-64
      elf_machine='Advanced Micro Devices X86-64'
      ;;
    aarch64-unknown-linux-gnu:|aarch64-unknown-linux-gnu:armv8-a)
      release_platform="$rust_target"
      cpu_baseline=armv8-a
      elf_machine=AArch64
      ;;
    aarch64-apple-darwin:|aarch64-apple-darwin:armv8-a)
      release_platform="$rust_target"
      cpu_baseline=armv8-a
      libc='{"family":"libSystem","minimum_macos_version":"11.0","dynamic_libcxx":true}'
      ;;
    *)
      echo >&2 "unsupported release Rust target and CPU baseline: ${rust_target}:${requested_cpu_baseline}"
      return 1
      ;;
  esac
}
