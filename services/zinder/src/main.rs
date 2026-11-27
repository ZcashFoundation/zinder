//! Unified `zinder` binary entrypoint.

#[global_allocator]
static GLOBAL_ALLOCATOR: zinder_runtime::RuntimeAllocator = zinder_runtime::RuntimeAllocator;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    zinder::main_from_env().await
}
