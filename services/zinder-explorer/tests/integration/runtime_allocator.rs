//! Holds the `zinder-explorer` binary to the allocator every service binary installs.

#[cfg(target_os = "linux")]
#[test]
fn binary_allocates_through_jemalloc() -> eyre::Result<()> {
    zinder_testkit::check_binary_allocates_through_jemalloc(zinder_testkit::bin_exe!(
        "zinder-explorer"
    ))
}
