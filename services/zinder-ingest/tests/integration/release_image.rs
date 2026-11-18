//! Holds the `zinder-ingest` binary to its release image contract: the container
//! healthcheck and the image's default configuration file.

#[test]
fn healthcheck_subcommand_probes_the_operational_endpoint() -> eyre::Result<()> {
    zinder_testkit::check_binary_healthcheck(zinder_testkit::bin_exe!("zinder-ingest"))
}

#[test]
fn image_default_config_holds_only_the_container_listeners() -> eyre::Result<()> {
    zinder_testkit::check_release_image_config(
        zinder_testkit::bin_exe!("zinder-ingest"),
        "ingest.toml",
        &[("ops", "[::]:9105")],
        &[
            ("ZINDER_NETWORK__NAME", "zcash-regtest"),
            ("ZINDER_NODE__JSON_RPC_ADDR", "http://127.0.0.1:18232"),
        ],
    )
}
