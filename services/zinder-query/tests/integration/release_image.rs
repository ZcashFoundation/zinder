//! Holds the `zinder-query` binary to its release image contract: the container
//! healthcheck and the image's default configuration file.

#[test]
fn healthcheck_subcommand_probes_the_operational_endpoint() -> eyre::Result<()> {
    zinder_testkit::check_binary_healthcheck(zinder_testkit::bin_exe!("zinder-query"))
}

#[test]
fn image_default_config_holds_only_the_container_listeners() -> eyre::Result<()> {
    zinder_testkit::check_release_image_config(
        zinder_testkit::bin_exe!("zinder-query"),
        &[("ops", "[::]:9106"), ("query", "[::]:9102")],
        &[
            ("ZINDER_NETWORK__NAME", "zcash-regtest"),
            ("ZINDER_STORAGE__PATH", "/var/lib/zinder/canonical"),
            (
                "ZINDER_STORAGE__SECONDARY_PATH",
                "/var/lib/zinder/query/canonical-secondary",
            ),
            ("ZINDER_WALLET__PATH", "/var/lib/zinder/wallet"),
            (
                "ZINDER_WALLET__SECONDARY_PATH",
                "/var/lib/zinder/query/wallet-secondary",
            ),
        ],
    )
}
