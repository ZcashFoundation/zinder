//! Cipherscan-compatible REST adapter binary entry point.

#[global_allocator]
static GLOBAL_ALLOCATOR: zinder_runtime::RuntimeAllocator = zinder_runtime::RuntimeAllocator;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    zinder_compat_cipherscan::runtime::main_from_env(env!("CARGO_BIN_NAME")).await
}
