//! Holds the `zinder-explorer` binary to the container healthcheck contract.

#[test]
fn healthcheck_subcommand_probes_the_operational_endpoint() -> eyre::Result<()> {
    zinder_testkit::check_binary_healthcheck(zinder_testkit::bin_exe!("zinder-explorer"))
}
