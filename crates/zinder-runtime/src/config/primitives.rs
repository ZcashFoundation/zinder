//! Typed configuration input primitives: strict toggles, redacting wrappers,
//! and origin list parsing.

use std::{fmt, str::FromStr};

use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

/// Sentinel string that explicitly opts out of a default-enabled toggleable
/// configuration setting.
pub const DISABLED_SENTINEL: &str = "none";

/// Placeholder emitted in redacted `Debug` and `Serialize` configuration
/// outputs and rejected as secret input.
pub const REDACTED_PLACEHOLDER: &str = "[REDACTED]";

/// Tri-state toggle configuration value separating an omitted field
/// ([`Toggle::Defaulted`]) from an explicit opt-out ([`Toggle::Disabled`],
/// parsed from `"none"`) and an explicit setting ([`Toggle::Enabled`]).
///
/// Empty or whitespace-only strings are rejected so accidentally blank
/// environment variables never silently alter runtime behavior.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Toggle<T> {
    /// Field was omitted; apply its section or runtime default.
    #[default]
    Defaulted,
    /// Operator explicitly opted out via `"none"`.
    Disabled,
    /// Operator supplied a parsed value of type `T`.
    Enabled(T),
}

impl<T> Toggle<T> {
    /// Returns whether the field was omitted.
    #[must_use]
    pub const fn is_defaulted(&self) -> bool {
        matches!(self, Self::Defaulted)
    }

    /// Returns whether the operator explicitly disabled the setting.
    #[must_use]
    pub const fn is_disabled(&self) -> bool {
        matches!(self, Self::Disabled)
    }

    /// Resolves this toggle using `default_val` when [`Toggle::Defaulted`].
    #[must_use]
    pub fn resolve_or(self, default_val: T) -> Option<T> {
        match self {
            Self::Defaulted => Some(default_val),
            Self::Disabled => None,
            Self::Enabled(inner) => Some(inner),
        }
    }

    /// Resolves this toggle without applying a fallback default.
    #[must_use]
    pub fn resolve_optional(self) -> Option<T> {
        match self {
            Self::Defaulted | Self::Disabled => None,
            Self::Enabled(inner) => Some(inner),
        }
    }

    /// Builds a [`Toggle`] for rendering a setting that defaults to enabled,
    /// mapping `None` to [`Toggle::Disabled`] (`"none"`).
    #[must_use]
    pub fn from_resolved_defaulted_on(resolved: Option<T>) -> Self {
        resolved.map_or(Self::Disabled, Self::Enabled)
    }

    /// Maps an `Enabled` inner value cleanly.
    #[must_use]
    pub fn map<U>(self, mapper: impl FnOnce(T) -> U) -> Toggle<U> {
        match self {
            Self::Defaulted => Toggle::Defaulted,
            Self::Disabled => Toggle::Disabled,
            Self::Enabled(inner) => Toggle::Enabled(mapper(inner)),
        }
    }

    /// Borrows a string-backed toggle as `Toggle<&str>`.
    #[must_use]
    pub fn as_deref(&self) -> Toggle<&str>
    where
        T: AsRef<str>,
    {
        match self {
            Self::Defaulted => Toggle::Defaulted,
            Self::Disabled => Toggle::Disabled,
            Self::Enabled(inner) => Toggle::Enabled(inner.as_ref()),
        }
    }
}

impl<T> From<T> for Toggle<T> {
    fn from(inner: T) -> Self {
        Self::Enabled(inner)
    }
}

impl<T: fmt::Display> Serialize for Toggle<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Defaulted => serializer.serialize_none(),
            Self::Disabled => serializer.serialize_str(DISABLED_SENTINEL),
            Self::Enabled(inner) => serializer.serialize_str(&inner.to_string()),
        }
    }
}

impl<'de, T> Deserialize<'de> for Toggle<T>
where
    T: FromStr,
    T::Err: fmt::Display,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(de::Error::custom(
                "empty value is not allowed; omit the setting to use the default or set it to \"none\" to disable",
            ));
        }
        if trimmed == DISABLED_SENTINEL {
            return Ok(Self::Disabled);
        }
        raw.parse::<T>()
            .map(Self::Enabled)
            .map_err(de::Error::custom)
    }
}

/// Secret string configuration input that preserves exact bytes while
/// redacting `Debug` and `Serialize` outputs and rejecting empty values or the
/// literal `[REDACTED]` placeholder on input.
#[derive(Clone)]
pub struct SecretInput(SecretString);

impl SecretInput {
    /// Validates and wraps a raw credential string.
    ///
    /// Rejects empty or whitespace-only values and the literal `[REDACTED]`
    /// placeholder so `--print-config` output is never re-accepted as a real
    /// secret.
    pub fn new(raw: impl Into<String>) -> Result<Self, &'static str> {
        let raw = raw.into();
        if raw.trim().is_empty() {
            return Err("empty secret value is not allowed");
        }
        if raw.trim() == REDACTED_PLACEHOLDER {
            return Err(
                "literal `[REDACTED]` is not a valid secret; supply the real credential instead of `--print-config` output",
            );
        }
        Ok(Self(SecretString::from(raw)))
    }

    /// Constructs a redacted placeholder for `--print-config` rendering.
    #[must_use]
    pub fn redacted_placeholder() -> Self {
        Self(SecretString::from(REDACTED_PLACEHOLDER.to_owned()))
    }

    /// Returns the underlying secret bytes for explicit resolution paths.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        self.0.expose_secret()
    }
}

impl fmt::Debug for SecretInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("\"[REDACTED]\"")
    }
}

impl PartialEq for SecretInput {
    fn eq(&self, other: &Self) -> bool {
        self.0.expose_secret() == other.0.expose_secret()
    }
}

impl Eq for SecretInput {}

impl Serialize for SecretInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(REDACTED_PLACEHOLDER)
    }
}

impl<'de> Deserialize<'de> for SecretInput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Self::new(raw).map_err(de::Error::custom)
    }
}

/// URL string wrapper that preserves the exact input URL for dialing while
/// redacting userinfo, query parameters, and fragments in `Debug` and
/// `Serialize` outputs.
#[derive(Clone, Eq, PartialEq)]
pub struct RedactedUrl(String);

impl RedactedUrl {
    /// Validates and wraps a non-empty endpoint URL.
    pub fn new(raw: impl Into<String>) -> Result<Self, &'static str> {
        let raw = raw.into();
        if raw.trim().is_empty() {
            return Err("empty URL value is not allowed");
        }
        if raw.contains(REDACTED_PLACEHOLDER) {
            return Err(
                "URL must not contain `[REDACTED]`; supply the real endpoint instead of `--print-config` output",
            );
        }
        Ok(Self(raw))
    }

    /// Wraps an already-redacted URL string for `--print-config` rendering.
    #[must_use]
    pub fn from_redacted_display(url: impl Into<String>) -> Self {
        Self(zinder_source::redact_endpoint_url(&url.into()).into_owned())
    }

    /// Returns the full underlying URL string for endpoint validation or dialing.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the wrapper and returns the underlying URL string.
    #[must_use]
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl FromStr for RedactedUrl {
    type Err = &'static str;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::new(raw)
    }
}

impl fmt::Debug for RedactedUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redacted = zinder_source::redact_endpoint_url(&self.0);
        fmt::Debug::fmt(redacted.as_ref(), formatter)
    }
}

impl fmt::Display for RedactedUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redacted = zinder_source::redact_endpoint_url(&self.0);
        formatter.write_str(redacted.as_ref())
    }
}

impl Serialize for RedactedUrl {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(zinder_source::redact_endpoint_url(&self.0).as_ref())
    }
}

impl<'de> Deserialize<'de> for RedactedUrl {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Self::new(raw).map_err(de::Error::custom)
    }
}

/// PROXY-protocol CIDR allowlist input that rejects empty values and
/// `[REDACTED]`, accepts `"none"` to opt out explicitly, and redacts any
/// configured allowlist in `Debug` and `Serialize` outputs.
#[derive(Clone, Default)]
pub enum RedactedCidrs {
    /// Field omitted; PROXY protocol is disabled by default.
    #[default]
    Defaulted,
    /// Explicitly disabled via `"none"`.
    Disabled,
    /// Operator-configured CIDR allowlist (redacted on `Debug` and `Serialize`).
    Configured(SecretString),
}

impl RedactedCidrs {
    /// Returns whether the field was omitted.
    #[must_use]
    pub const fn is_defaulted(&self) -> bool {
        matches!(self, Self::Defaulted)
    }

    /// Returns the raw allowlist string when enabled, or `None` when omitted or `"none"`.
    #[must_use]
    pub fn as_deref(&self) -> Option<&str> {
        match self {
            Self::Defaulted | Self::Disabled => None,
            Self::Configured(raw) => Some(raw.expose_secret()),
        }
    }

    /// Builds a [`RedactedCidrs`] projection for `--print-config`.
    #[must_use]
    pub fn from_resolved(enabled: bool) -> Self {
        if enabled {
            Self::Configured(SecretString::from(REDACTED_PLACEHOLDER.to_owned()))
        } else {
            Self::Defaulted
        }
    }
}

impl PartialEq for RedactedCidrs {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Defaulted, Self::Defaulted) | (Self::Disabled, Self::Disabled) => true,
            (Self::Configured(left), Self::Configured(right)) => {
                left.expose_secret() == right.expose_secret()
            }
            _ => false,
        }
    }
}

impl Eq for RedactedCidrs {}

impl fmt::Debug for RedactedCidrs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Defaulted => formatter.write_str("Defaulted"),
            Self::Disabled => formatter.write_str("Disabled"),
            Self::Configured(_) => formatter.write_str("Configured(\"[REDACTED]\")"),
        }
    }
}

impl Serialize for RedactedCidrs {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Defaulted => serializer.serialize_none(),
            Self::Disabled => serializer.serialize_str(DISABLED_SENTINEL),
            Self::Configured(_) => serializer.serialize_str(REDACTED_PLACEHOLDER),
        }
    }
}

impl<'de> Deserialize<'de> for RedactedCidrs {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(de::Error::custom(
                "empty value is not allowed; omit the setting or set it to \"none\" to disable",
            ));
        }
        if trimmed == REDACTED_PLACEHOLDER {
            return Err(de::Error::custom(
                "literal `[REDACTED]` is not a valid CIDR allowlist",
            ));
        }
        if trimmed == DISABLED_SENTINEL {
            return Ok(Self::Disabled);
        }
        Ok(Self::Configured(SecretString::from(raw)))
    }
}

#[cfg(test)]
mod tests {
    use super::RedactedUrl;

    #[test]
    fn redacted_url_rejects_redacted_placeholder_and_preserves_display_constructor() {
        assert!(RedactedUrl::new("https://prices.example/price?[REDACTED]").is_err());
        assert!(RedactedUrl::new("https://user:[REDACTED]@prices.example/price").is_err());
        let displayed =
            RedactedUrl::from_redacted_display("https://prices.example/price?api_key=secret");
        assert_eq!(
            displayed.as_str(),
            "https://prices.example/price?[REDACTED]"
        );
    }
}
