//! Shared trace-export configuration.

use http::Uri;
use serde::{Deserialize, Serialize, Serializer};

const DEFAULT_SAMPLING_RATE: f64 = 0.1;

/// Resolved `[observability]` settings. Debug and TOML output redact the collector address.
#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(try_from = "ObservabilitySection")]
pub struct ObservabilityConfig {
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "redact_endpoint"
    )]
    pub(crate) otlp_endpoint: Option<Uri>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) sampling_rate: Option<f64>,
}

impl std::fmt::Debug for ObservabilityConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObservabilityConfig")
            .field(
                "otlp_endpoint",
                &self.otlp_endpoint.as_ref().map(|_| "[REDACTED]"),
            )
            .field("sampling_rate", &self.sampling_rate)
            .finish()
    }
}

impl ObservabilityConfig {
    /// Whether the section has settings to include in redacted configuration output.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.otlp_endpoint.is_none() && self.sampling_rate.is_none()
    }
}

#[allow(
    clippy::ref_option,
    reason = "serde passes the endpoint field by reference"
)]
fn redact_endpoint<S: Serializer>(_: &Option<Uri>, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str("[REDACTED]")
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ObservabilitySection {
    otlp_endpoint: Option<String>,
    sampling_rate: Option<f64>,
}

impl TryFrom<ObservabilitySection> for ObservabilityConfig {
    type Error = &'static str;

    fn try_from(section: ObservabilitySection) -> Result<Self, Self::Error> {
        let otlp_endpoint = section
            .otlp_endpoint
            .as_deref()
            .map(str::trim)
            .filter(|endpoint| !endpoint.is_empty())
            .map(parse_collector_endpoint)
            .transpose()?;
        if section
            .sampling_rate
            .is_some_and(|rate| !(0.0..=1.0).contains(&rate))
        {
            return Err("observability.sampling_rate must be between 0 and 1");
        }
        let sampling_rate = section
            .sampling_rate
            .or_else(|| otlp_endpoint.as_ref().map(|_| DEFAULT_SAMPLING_RATE));
        Ok(Self {
            otlp_endpoint,
            sampling_rate,
        })
    }
}

fn parse_collector_endpoint(endpoint: &str) -> Result<Uri, &'static str> {
    const INVALID: &str = "observability.otlp_endpoint must be an http or https origin without credentials, a path, a query, or a fragment";
    if endpoint.contains('#') {
        return Err(INVALID);
    }
    let endpoint = endpoint.parse::<Uri>().map_err(|_| INVALID)?;
    if !matches!(endpoint.scheme_str(), Some("http" | "https"))
        || !endpoint.authority().is_some_and(|authority| {
            !authority.host().is_empty()
                && !authority.as_str().contains('@')
                && (authority.as_str() == authority.host() || authority.port_u16().is_some())
        })
        || !matches!(endpoint.path(), "" | "/")
        || endpoint.query().is_some()
    {
        return Err(INVALID);
    }
    Ok(endpoint)
}
