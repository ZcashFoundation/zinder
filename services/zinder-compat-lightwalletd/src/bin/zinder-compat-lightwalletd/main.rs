//! Zinder lightwalletd-compatible gRPC server entry point.

use std::process::ExitCode;

#[global_allocator]
static GLOBAL_ALLOCATOR: zinder_runtime::RuntimeAllocator = zinder_runtime::RuntimeAllocator;

#[tokio::main]
async fn main() -> ExitCode {
    zinder_compat_lightwalletd::runtime::main_from_env(env!("CARGO_BIN_NAME")).await
}
