//! The reference proxy setup keeps the contract `trust_proxy_client_address`
//! relies on.
//!
//! A listener that trusts [`PROXY_CLIENT_ADDRESS_HEADER`] lets whoever sets
//! the header choose the client its requests count against. The reference
//! Caddy template must therefore replace the header, the TLS runbook must
//! stop the trust before restoring a proxy configuration that passes client
//! headers on, and it must name every network that reaches the listener
//! without the proxy.

#![allow(
    missing_docs,
    reason = "Integration test names describe the proxy contract under test."
)]

use eyre::{Result, ensure, eyre};
use zinder_runtime::PROXY_CLIENT_ADDRESS_HEADER;

const CADDYFILE: &str = include_str!("../../../../deploy/compat-lightwalletd.Caddyfile");
const COMPOSE: &str = include_str!("../../../../deploy/docker-compose.yml");
const TLS_RUNBOOK: &str = include_str!("../../../../docs/runbooks/exposing-over-tls.md");

/// The runbook text under `heading`, up to the next `## ` heading.
fn section<'doc>(document: &'doc str, heading: &str) -> Result<&'doc str> {
    let start = document
        .find(heading)
        .ok_or_else(|| eyre!("the TLS runbook has no {heading:?} section"))?;
    let body = &document[start + heading.len()..];
    Ok(body.find("\n## ").map_or(body, |end| &body[..end]))
}

/// The networks the Compose `zinder-ingest` service joins. Every public
/// listener runs in its network namespace.
fn ingest_networks(compose: &str) -> Vec<&str> {
    compose
        .lines()
        .skip_while(|line| *line != "  zinder-ingest:")
        .skip(1)
        .take_while(|line| line.is_empty() || line.starts_with("    "))
        .skip_while(|line| *line != "    networks:")
        .skip(1)
        .take_while(|line| line.starts_with("      "))
        .filter(|line| !line.starts_with("       "))
        .filter_map(|line| line.trim().strip_suffix(':'))
        .collect()
}

#[test]
fn the_caddy_template_replaces_the_header_the_listener_reads() -> Result<()> {
    let directives = CADDYFILE
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("header_up"))
        .collect::<Vec<_>>();
    let [directive] = directives.as_slice() else {
        return Err(eyre!(
            "the Caddy template sets {} upstream headers; expected one: {directives:?}",
            directives.len()
        ));
    };
    // `header_up <name> <value>` replaces what the client sent. A `+name`
    // would append to it, and the client's own value would reach Zinder.
    let fields = directive.split_whitespace().collect::<Vec<_>>();
    ensure!(
        matches!(fields.as_slice(), [_, name, "{http.request.remote}"]
            if name.eq_ignore_ascii_case(PROXY_CLIENT_ADDRESS_HEADER)),
        "the Caddy template must replace {PROXY_CLIENT_ADDRESS_HEADER} with the client \
         connection's address: {directive}"
    );
    Ok(())
}

#[test]
fn the_runbook_stops_trusting_the_header_before_restoring_an_older_proxy() -> Result<()> {
    let rollback = section(TLS_RUNBOOK, "## Roll back the proxy")?;
    let distrust = rollback
        .find("TRUST_PROXY_CLIENT_ADDRESS=false")
        .ok_or_else(|| eyre!("the rollback never turns trust_proxy_client_address off"))?;
    let restore = rollback
        .find("reload Caddy")
        .ok_or_else(|| eyre!("the rollback never reloads Caddy"))?;
    ensure!(
        distrust < restore,
        "the rollback must turn trust_proxy_client_address off before Caddy loads an older \
         configuration, which passes a client's own header on"
    );
    Ok(())
}

#[test]
fn the_runbook_names_every_network_that_reaches_a_trusting_listener() -> Result<()> {
    let requirements = section(TLS_RUNBOOK, "## Keep wallets apart in the receive limit")?;
    let networks = ingest_networks(COMPOSE);
    ensure!(
        !networks.is_empty(),
        "found no networks for zinder-ingest in deploy/docker-compose.yml"
    );
    for network in networks {
        ensure!(
            requirements.contains(&format!("Compose `{network}` network")),
            "the public listeners share zinder-ingest's namespace, which joins the {network} \
             network, but the runbook's requirements do not name it"
        );
    }
    ensure!(
        requirements.contains("docker network inspect"),
        "the runbook must show how to list the containers that reach the listener directly"
    );
    Ok(())
}

#[test]
fn ingest_networks_are_read_from_the_service_block() {
    let compose = [
        "services:",
        "  zinder-ingest:",
        "    image: zinder",
        "    networks:",
        "      node:",
        "        aliases:",
        "          - zinder-query",
        "      observability:",
        "    volumes:",
        "      - data:/var/lib/zinder",
        "  zinder-query:",
        "    networks:",
        "      other:",
    ]
    .join("\n");

    assert_eq!(ingest_networks(&compose), ["node", "observability"]);
}
