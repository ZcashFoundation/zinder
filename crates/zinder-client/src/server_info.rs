//! Client-owned wallet endpoint metadata.

use zinder_core::{ArtifactSchemaVersion, Network};

use crate::{Capability, CapabilityDescriptor};

/// Upstream node metadata captured by the query endpoint.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct NodeServerInfo {
    /// Node-reported semantic version, when available.
    pub version: Option<String>,
    /// Exact upstream-node capability strings.
    pub capabilities: Vec<String>,
}

/// Wallet endpoint identity, capability, and schema metadata.
///
/// This is a client-owned representation. Generated protobuf messages remain
/// private to the remote transport conversion.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ServerInfo {
    /// Network served by the endpoint.
    pub network: Network,
    /// Stable service identifier, such as `zinder-query`.
    pub service_name: String,
    /// Service semantic version.
    pub service_version: String,
    /// Wallet capabilities, including unknown additive values.
    pub capabilities: Vec<Capability>,
    /// Monotonic native contract revision.
    pub contract_revision: u32,
    /// Full source commit embedded by the release build.
    pub build_git_commit: String,
    /// Canonical artifact schema version.
    pub schema_version: ArtifactSchemaVersion,
    /// Configured canonical reorg window depth in blocks.
    pub reorg_window_blocks: u32,
    /// Upstream node metadata, when the endpoint has a source snapshot.
    pub node: Option<NodeServerInfo>,
}

impl CapabilityDescriptor for ServerInfo {
    fn has(&self, capability: &str) -> bool {
        self.capabilities
            .iter()
            .any(|advertised| advertised.as_str() == capability)
    }
}
