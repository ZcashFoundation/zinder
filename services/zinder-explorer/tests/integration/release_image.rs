//! Holds the `zinder-explorer` binary to its release image contract: the container
//! healthcheck and the image's default configuration file.

#[test]
fn healthcheck_subcommand_probes_the_operational_endpoint() -> eyre::Result<()> {
    zinder_testkit::check_binary_healthcheck(zinder_testkit::bin_exe!("zinder-explorer"))
}

#[test]
fn image_default_config_holds_only_the_container_listeners() -> eyre::Result<()> {
    zinder_testkit::check_release_image_config(
        zinder_testkit::bin_exe!("zinder-explorer"),
        &[("ops", "[::]:9069"), ("explorer", "[::]:9068")],
        &[("ZINDER_NETWORK__NAME", "zcash-regtest")],
    )
}
