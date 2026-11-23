//! Named deployment profiles shared by runtime configuration loaders.

use serde::{Deserialize, Serialize};

/// Selects a coordinated deployment configuration across runtimes.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum DeploymentProfile {
    /// Canonical wallet history with node-backed reads and no wallet projection.
    LightWallet,
}
