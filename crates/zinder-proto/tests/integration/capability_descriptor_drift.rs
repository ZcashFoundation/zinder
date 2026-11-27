//! Capability-table-vs-descriptor drift guard.
//!
//! Cross-checks the single [`CAPABILITIES`] table against the compiled
//! `FileDescriptorSet` so the two cannot drift. Every served RPC on
//! `WalletQuery`, `ExplorerQuery`, and `IngestControl` must map to exactly one
//! capability row, except for explicitly versioned additive capability rows
//! on one RPC. Every capability that names a method must name one the
//! descriptor actually serves. Field-level capabilities (a capability that
//! gates a field on another RPC rather than a method of its own) carry
//! `method: None` and are exempt from the method-existence half.

#![allow(
    missing_docs,
    reason = "Integration test names describe the descriptor-drift contract under test."
)]

use std::collections::{BTreeMap, BTreeSet};

use eyre::{Result, eyre};
use prost::Message;
use prost_types::FileDescriptorSet;
use zinder_proto::ZINDER_V1_FILE_DESCRIPTOR_SET;
use zinder_proto::capabilities::{
    CAPABILITIES, CapabilitySurface, EXPLORER_TRANSACTION_HISTORY_V1,
    EXPLORER_TRANSACTION_HISTORY_V2,
};

/// Fully qualified methods the descriptor serves but no capability of their
/// own gates.
const UNCAPABILITIED_METHODS: &[&str] = &[];

/// RPCs whose additive protocol revision is intentionally advertised beside
/// its predecessor on the same method.
const VERSIONED_CAPABILITY_METHODS: &[&str] = &[
    "zinder.v1.explorer.ExplorerQuery.TransactionHistory",
    "zinder.v1.wallet.WalletQuery.CompactBlocksInRange",
];

/// Proto service names that back each capability surface.
const SURFACE_SERVICES: &[(CapabilitySurface, &str)] = &[
    (CapabilitySurface::Wallet, "zinder.v1.wallet.WalletQuery"),
    (
        CapabilitySurface::Explorer,
        "zinder.v1.explorer.ExplorerQuery",
    ),
    (CapabilitySurface::Ingest, "zinder.v1.ingest.IngestControl"),
];

#[test]
fn every_served_method_maps_to_exactly_one_capability() -> Result<()> {
    let served = served_methods()?;
    let mut method_to_capabilities: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for spec in CAPABILITIES {
        if let Some(method) = spec.method {
            method_to_capabilities
                .entry(method)
                .or_default()
                .push(spec.string);
        }
    }

    let mut unmapped: Vec<&str> = Vec::new();
    let mut over_mapped: Vec<(&str, Vec<&str>)> = Vec::new();
    for method in &served {
        if UNCAPABILITIED_METHODS.contains(&method.as_str()) {
            continue;
        }
        match method_to_capabilities.get(method.as_str()) {
            None => unmapped.push(method),
            Some(capabilities)
                if capabilities.len() > 1
                    && !VERSIONED_CAPABILITY_METHODS.contains(&method.as_str()) =>
            {
                over_mapped.push((method, capabilities.clone()));
            }
            Some(_) => {}
        }
    }

    assert!(
        unmapped.is_empty(),
        "served RPC methods have no capability in the CAPABILITIES table: {unmapped:?}. \
         Add a CapabilitySpec row (or list the method in UNCAPABILITIED_METHODS if it is \
         intentionally uncapability'd)."
    );
    assert!(
        over_mapped.is_empty(),
        "served RPC methods map to more than one capability: {over_mapped:?}. \
         Each method must bind to exactly one capability; field-level capabilities use \
         method: None."
    );
    Ok(())
}

#[test]
fn transaction_history_capability_versions_share_one_rpc() {
    let history_capabilities: BTreeSet<&str> = CAPABILITIES
        .iter()
        .filter(|spec| spec.method == Some("zinder.v1.explorer.ExplorerQuery.TransactionHistory"))
        .map(|spec| spec.string)
        .collect();

    assert_eq!(
        history_capabilities,
        BTreeSet::from([
            EXPLORER_TRANSACTION_HISTORY_V1,
            EXPLORER_TRANSACTION_HISTORY_V2,
        ])
    );
}

#[test]
fn every_capability_method_is_served() -> Result<()> {
    let served = served_methods()?;
    let mut missing: Vec<(&str, &str)> = Vec::new();
    for spec in CAPABILITIES {
        if let Some(method) = spec.method
            && !served.contains(method)
        {
            missing.push((spec.string, method));
        }
    }
    assert!(
        missing.is_empty(),
        "CAPABILITIES rows bind to proto methods the descriptor does not serve: {missing:?}. \
         Fix the method string or drop the row."
    );
    Ok(())
}

#[test]
fn uncapabilitied_methods_allowlist_is_served_and_unmapped() -> Result<()> {
    let served = served_methods()?;
    let table_methods: BTreeSet<&str> =
        CAPABILITIES.iter().filter_map(|spec| spec.method).collect();
    for method in UNCAPABILITIED_METHODS {
        assert!(
            served.contains(*method),
            "UNCAPABILITIED_METHODS lists {method} but the descriptor does not serve it; \
             remove the stale allowlist entry."
        );
        assert!(
            !table_methods.contains(method),
            "UNCAPABILITIED_METHODS lists {method} but a CapabilitySpec also binds it; \
             remove the allowlist entry or the duplicate capability."
        );
    }
    Ok(())
}

/// Returns the fully qualified names of every method on the three capability
/// surface services, decoded from the compiled descriptor.
fn served_methods() -> Result<BTreeSet<String>> {
    let descriptor = FileDescriptorSet::decode(ZINDER_V1_FILE_DESCRIPTOR_SET)?;
    let surface_service_names: BTreeSet<&str> = SURFACE_SERVICES
        .iter()
        .map(|(_, service)| *service)
        .collect();

    let mut methods = BTreeSet::new();
    for file in &descriptor.file {
        let package = file.package();
        for service in &file.service {
            let service_name = service.name();
            let qualified_service = format!("{package}.{service_name}");
            if !surface_service_names.contains(qualified_service.as_str()) {
                continue;
            }
            for method in &service.method {
                methods.insert(format!("{qualified_service}.{}", method.name()));
            }
        }
    }

    if methods.is_empty() {
        return Err(eyre!(
            "decoded zero methods on the capability surface services; the descriptor or \
             SURFACE_SERVICES list is wrong."
        ));
    }
    Ok(methods)
}
