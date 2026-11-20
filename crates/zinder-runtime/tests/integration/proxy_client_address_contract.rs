//! The reference proxy setup keeps the contract `trust_proxy_client_address`
//! relies on.
//!
//! A listener that trusts [`PROXY_CLIENT_ADDRESS_HEADER`] lets whoever sets
//! the header choose the client its requests count against. The reference
//! Caddy template must therefore replace the header on the requests it
//! forwards to the gRPC listener, the TLS runbook must stop the trust before
//! restoring a proxy configuration that passes client headers on, and it
//! must name every network that reaches the listener without the proxy, in
//! the base Compose file and in every overlay merged with it.

#![allow(
    missing_docs,
    reason = "Integration test names describe the proxy contract under test."
)]

use std::fs;

use eyre::{Result, ensure, eyre};
use zinder_runtime::PROXY_CLIENT_ADDRESS_HEADER;

const CADDYFILE: &str = include_str!("../../../../deploy/compat-lightwalletd.Caddyfile");
const TLS_RUNBOOK: &str = include_str!("../../../../docs/runbooks/exposing-over-tls.md");
const DEPLOY_DIRECTORY: &str = "../../deploy";
const BASE_COMPOSE_FILE: &str = "docker-compose.yml";

/// The base Compose file and every overlay that adds to its `zinder-ingest`
/// service, as `(file name, contents)` pairs.
///
/// An overlay's `zinder-ingest` names no image: only the base file defines
/// how the service is built, so the overlay runs merged with it.
fn wallet_serving_compose_files() -> Result<Vec<(String, String)>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(DEPLOY_DIRECTORY)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let is_yaml = path.extension().is_some_and(|extension| extension == "yml");
        if !(name.starts_with("docker-compose") && is_yaml) {
            continue;
        }
        let contents = fs::read_to_string(&path)?;
        let overlays_ingest = ingest_service(&contents)
            .is_some_and(|service| !service.iter().any(|line| line.starts_with("    image:")));
        if name == BASE_COMPOSE_FILE || overlays_ingest {
            files.push((name.to_owned(), contents));
        }
    }
    files.sort();
    Ok(files)
}

/// The lines inside the Caddy block that forwards to the plaintext gRPC
/// listener, without its opening and closing lines.
fn grpc_upstream_block(caddyfile: &str) -> Result<Vec<&str>> {
    let mut lines = caddyfile
        .lines()
        .map(str::trim)
        .skip_while(|line| !(line.starts_with("reverse_proxy h2c://") && line.ends_with('{')));
    lines
        .next()
        .ok_or_else(|| eyre!("the Caddy template has no reverse_proxy h2c:// block"))?;
    let mut depth = 1_usize;
    let mut block = Vec::new();
    for line in lines {
        // Placeholders such as `{http.request.remote}` open and close on one
        // line, so only block braces change the depth.
        depth = (depth + line.matches('{').count())
            .checked_sub(line.matches('}').count())
            .ok_or_else(|| eyre!("unbalanced braces in the Caddy template at {line:?}"))?;
        if depth == 0 {
            return Ok(block);
        }
        block.push(line);
    }
    Err(eyre!(
        "the Caddy template's reverse_proxy h2c:// block never closes"
    ))
}

/// Checks that the gRPC upstream block replaces the client address header
/// with the address of the wallet's own connection.
fn replaces_the_client_address_header_upstream(caddyfile: &str) -> Result<()> {
    let directives = grpc_upstream_block(caddyfile)?
        .into_iter()
        .filter(|line| line.starts_with("header_up"))
        .collect::<Vec<_>>();
    let [directive] = directives.as_slice() else {
        return Err(eyre!(
            "the gRPC upstream block sets {} upstream headers; expected one: {directives:?}",
            directives.len()
        ));
    };
    // `header_up <name> <value>` replaces what the client sent. A `+name`
    // would append to it, and the client's own value would reach Zinder.
    let fields = directive.split_whitespace().collect::<Vec<_>>();
    ensure!(
        matches!(fields.as_slice(), [_, name, "{http.request.remote}"]
            if name.eq_ignore_ascii_case(PROXY_CLIENT_ADDRESS_HEADER)),
        "the gRPC upstream block must replace {PROXY_CLIENT_ADDRESS_HEADER} with the client \
         connection's address: {directive}"
    );
    Ok(())
}

/// The runbook text under `heading`, up to the next `## ` heading.
fn section<'doc>(document: &'doc str, heading: &str) -> Result<&'doc str> {
    let start = document
        .find(heading)
        .ok_or_else(|| eyre!("the TLS runbook has no {heading:?} section"))?;
    let body = &document[start + heading.len()..];
    Ok(body.find("\n## ").map_or(body, |end| &body[..end]))
}

/// The lines of the Compose `zinder-ingest` service, without its own line,
/// or `None` when the file does not define it.
fn ingest_service(compose: &str) -> Option<Vec<&str>> {
    let mut lines = compose
        .lines()
        .skip_while(|line| *line != "  zinder-ingest:");
    lines.next()?;
    Some(
        lines
            .take_while(|line| line.trim().is_empty() || line.starts_with("    "))
            .collect(),
    )
}

/// The networks the Compose `zinder-ingest` service joins. Every public
/// listener runs in its network namespace.
///
/// Reads the map form, the list form, and the flow form `[a, b]`, and fails
/// on a `networks` key it cannot read, so a new form is never read as no
/// networks.
fn ingest_networks(compose: &str) -> Result<Vec<&str>> {
    let service = ingest_service(compose).unwrap_or_default();
    let Some(key) = service
        .iter()
        .position(|line| line.starts_with("    networks:"))
    else {
        return Ok(Vec::new());
    };
    let inline = service[key]["    networks:".len()..].trim();
    let names = if inline.is_empty() {
        service[key + 1..]
            .iter()
            .map(|line| line.trim_end())
            .take_while(|line| line.is_empty() || line.starts_with("      "))
            .filter(|line| !(line.is_empty() || line.starts_with("       ")))
            .map(str::trim)
            .filter(|entry| !entry.starts_with('#'))
            .map(|entry| {
                entry
                    .strip_prefix("- ")
                    .or_else(|| entry.split_once(':').map(|(name, _)| name))
                    .ok_or_else(|| eyre!("unreadable zinder-ingest network entry {entry:?}"))
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        inline
            .strip_prefix('[')
            .and_then(|flow| flow.strip_suffix(']'))
            .ok_or_else(|| eyre!("unreadable zinder-ingest networks value {inline:?}"))?
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .collect()
    };
    let names = names
        .into_iter()
        .map(|name| name.trim().trim_matches(['"', '\'']))
        .collect::<Vec<_>>();
    ensure!(
        !names.is_empty(),
        "zinder-ingest has a networks key that names no network"
    );
    Ok(names)
}

#[test]
fn the_caddy_template_replaces_the_header_the_listener_reads() -> Result<()> {
    replaces_the_client_address_header_upstream(CADDYFILE)
}

#[test]
fn a_header_set_outside_the_grpc_upstream_block_does_not_count() {
    // `forward_auth` takes `header_up` too, but sets it only on the
    // readiness request, so the wallet's own header reaches Zinder.
    let caddyfile = "\
https://example.org:443 {
\troute {
\t\tforward_auth 127.0.0.1:9107 {
\t\t\turi /readyz
\t\t\theader_up X-Zinder-Client-Address {http.request.remote}
\t\t}

\t\treverse_proxy h2c://127.0.0.1:{$ZINDER_COMPAT_HOST_PORT} {
\t\t\tflush_interval -1
\t\t}
\t}
}
";

    assert_eq!(
        grpc_upstream_block(caddyfile).ok(),
        Some(vec!["flush_interval -1"])
    );
    assert!(replaces_the_client_address_header_upstream(caddyfile).is_err());
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
fn the_runbook_recreates_containers_with_every_compose_file_the_deployment_uses() -> Result<()> {
    let commands = TLS_RUNBOOK
        .lines()
        .filter(|line| line.contains("docker compose"))
        .collect::<Vec<_>>();

    ensure!(
        !commands.is_empty(),
        "the TLS runbook runs no Compose command"
    );
    for command in commands {
        ensure!(
            command.contains("--env-file \"$env_file\" \"${compose_files[@]}\""),
            "a Compose command must pass the env file and every -f file the deployment \
             runs with, or Compose drops what the missing overlays add: {command}"
        );
    }
    Ok(())
}

#[test]
fn the_runbook_names_every_network_that_reaches_a_trusting_listener() -> Result<()> {
    let requirements = section(TLS_RUNBOOK, "## Keep wallets apart in the receive limit")?;
    let compose_files = wallet_serving_compose_files()?;
    ensure!(
        compose_files.len() > 1,
        "found no overlay merged with deploy/{BASE_COMPOSE_FILE} in {DEPLOY_DIRECTORY}"
    );
    let mut checked = 0;
    for (file, contents) in &compose_files {
        let networks =
            ingest_networks(contents).map_err(|error| eyre!("deploy/{file}: {error}"))?;
        for network in networks {
            ensure!(
                requirements.contains(&format!("Compose `{network}` network")),
                "the public listeners share zinder-ingest's namespace, which deploy/{file} \
                 joins to the {network} network, but the runbook's requirements do not name it"
            );
            checked += 1;
        }
    }
    ensure!(
        checked > 0,
        "found no networks for zinder-ingest in the Compose files"
    );
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

    assert_eq!(
        ingest_networks(&compose).ok(),
        Some(vec!["node", "observability"])
    );
}

#[test]
fn ingest_networks_are_read_in_the_list_and_flow_forms() {
    let list = [
        "services:",
        "  zinder-ingest:",
        "    networks:",
        "      - node",
        "      # A wallet-facing network.",
        "      - \"node-fallback\"",
        "    volumes:",
        "      - data:/var/lib/zinder",
    ]
    .join("\n");
    let flow = "services:\n  zinder-ingest:\n    networks: [node, node-fallback]\n";

    for compose in [list.as_str(), flow] {
        assert_eq!(
            ingest_networks(compose).ok(),
            Some(vec!["node", "node-fallback"]),
            "{compose}"
        );
    }
}

#[test]
fn an_unreadable_ingest_networks_key_fails() {
    for compose in [
        "services:\n  zinder-ingest:\n    networks: node\n",
        "services:\n  zinder-ingest:\n    networks: {node: {}}\n",
        "services:\n  zinder-ingest:\n    networks: []\n",
        "services:\n  zinder-ingest:\n    networks:\n    volumes:\n",
    ] {
        assert!(ingest_networks(compose).is_err(), "{compose}");
    }
}
