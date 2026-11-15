//! Holds the `zinder-projector` binary to the allocator every service binary installs.

#[cfg(target_os = "linux")]
#[test]
fn binary_allocates_through_jemalloc() -> eyre::Result<()> {
    zinder_testkit::check_binary_allocates_through_jemalloc(env!("CARGO_BIN_EXE_zinder-projector"))
}
