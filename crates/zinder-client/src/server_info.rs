//! Client-owned wallet endpoint metadata.

use zinder_core::{ArtifactSchemaVersion, Network};

use crate::{Capability, CapabilityDescriptor};

/// Metadata of the node endpoint a query broadcasts transactions through.
///
/// The query starts without contacting its node and verifies the endpoint on
/// first contact. Until that verification passes, the server reports this
/// descriptor empty: no version and no capabilities. A query configured
/// without a node always reports it empty.
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
    /// Broadcast node metadata, empty until the endpoint's node passes
    /// first-contact verification. Read it on each `ServerInfo` call rather
    /// than caching the first answer.
    ///
    /// The release query always sets this field. Empty means a descriptor
    /// with no version and no capabilities, so check `capabilities.is_empty()`
    /// rather than `is_none()`; `None` means the server sent no descriptor.
    pub node: Option<NodeServerInfo>,
}

impl CapabilityDescriptor for ServerInfo {
    fn has(&self, capability: &str) -> bool {
        self.capabilities
            .iter()
            .any(|advertised| advertised.as_str() == capability)
    }
}
