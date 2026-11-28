//! Zinder native wallet-query gRPC server binary entry point.

#[global_allocator]
static GLOBAL_ALLOCATOR: zinder_runtime::RuntimeAllocator = zinder_runtime::RuntimeAllocator;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    zinder_query::runtime::main_from_env(env!("CARGO_BIN_NAME")).await
}
