//! Holds the `zinder-projector` binary to its release image contract: the container
//! healthcheck and the image's default configuration file.

#[test]
fn healthcheck_subcommand_probes_the_operational_endpoint() -> eyre::Result<()> {
    zinder_testkit::check_binary_healthcheck(zinder_testkit::bin_exe!("zinder-projector"))
}

#[test]
fn image_default_config_holds_only_the_container_listeners() -> eyre::Result<()> {
    zinder_testkit::check_release_image_config(
        zinder_testkit::bin_exe!("zinder-projector"),
        &[("ops", "[::]:9110")],
        &[
            ("ZINDER_NETWORK__NAME", "zcash-regtest"),
            (
                "ZINDER_PROJECTOR__BUILD_OWNER_HEX",
                "00112233445566778899aabbccddeeff",
            ),
            ("ZINDER_PROJECTOR__LEASE_DURATION_SECONDS", "14400"),
        ],
    )
}
