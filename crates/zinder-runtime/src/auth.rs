//! Shared-secret bearer-token authentication for private gRPC control planes.
//!
//! The `IngestControl` gRPC service exposes mempool state and chain events
//! to colocated projector and native query processes.
//! Operators that run those processes on a separate host (or any network
//! that is not strictly localhost / VPN-only) need authentication on the
//! control plane. This module provides the minimum viable surface:
//!
//! - [`BearerToken`] wraps the secret string in [`secrecy::SecretString`]
//!   so it never appears in logs, debug output, or panic messages.
//! - The token is loaded from a file path at startup; environment-variable
//!   sourcing is intentionally not supported because env vars leak into
//!   process listings and debugger snapshots.
//! - Server-side validation uses constant-time comparison so a remote
//!   attacker cannot extract the token through timing analysis.
//! - [`BearerTokenServerInterceptor`] and
//!   [`BearerTokenClientInterceptor`] implement
//!   [`tonic::service::Interceptor`] so the gRPC adapters can plug them
//!   into [`tonic::transport::Server::builder`] and
//!   [`tonic::service::interceptor::InterceptedService`] respectively.
//!
//! When no token is configured, the interceptor builders return an
//! identity interceptor that allows every request. This preserves the
//! localhost-default deployment model: an operator who has not configured
//! a token has explicitly opted into trusting the network boundary.

use std::{path::Path, str::FromStr};

use secrecy::{ExposeSecret, SecretString};
use subtle::ConstantTimeEq;
use thiserror::Error;
use tonic::{Request, Status, service::interceptor::InterceptedService, transport::Channel};

/// Authenticated bearer token used by the private `IngestControl` gRPC
/// channel.
#[derive(Clone)]
pub struct BearerToken {
    secret: SecretString,
}

impl std::fmt::Debug for BearerToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BearerToken")
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

impl BearerToken {
    /// Loads a token from a file. The file must contain a single
    /// non-empty line (trailing whitespace is trimmed). Operators rotate
    /// the token by replacing the file and restarting the process; this
    /// loader does not watch for changes.
    pub fn from_file(path: &Path) -> Result<Self, BearerTokenError> {
        let raw = std::fs::read_to_string(path).map_err(|source| BearerTokenError::ReadFailed {
            path: path.display().to_string(),
            source,
        })?;
        raw.parse()
    }

    /// Returns the token's exposed bytes for use in metadata. Marked
    /// `pub(crate)` so external callers cannot trivially log the secret.
    pub(crate) fn expose_for_metadata(&self) -> &str {
        self.secret.expose_secret()
    }

    /// Constant-time comparison against a candidate string.
    ///
    /// Uses [`subtle::ConstantTimeEq`] so the response time does not reveal
    /// how many leading bytes of the secret matched.
    fn matches(&self, candidate: &str) -> bool {
        self.secret
            .expose_secret()
            .as_bytes()
            .ct_eq(candidate.as_bytes())
            .into()
    }

    /// Verifies one `Bearer <token>` metadata value without exposing the
    /// configured secret. Private method-level capabilities use this when a
    /// service deliberately gives a more privileged RPC a separate token.
    pub fn verify_bearer_metadata(
        &self,
        metadata_value: Option<&tonic::metadata::AsciiMetadataValue>,
        header_name: &'static str,
    ) -> Result<(), Status> {
        let metadata_value = metadata_value
            .ok_or_else(|| Status::unauthenticated(format!("missing {header_name} header")))?;
        let header_string = metadata_value
            .to_str()
            .map_err(|_| Status::unauthenticated(format!("{header_name} header is not ASCII")))?;
        let claimed = header_string.strip_prefix("Bearer ").ok_or_else(|| {
            Status::unauthenticated(format!("{header_name} header missing Bearer scheme"))
        })?;
        if self.matches(claimed) {
            Ok(())
        } else {
            Err(Status::unauthenticated(format!(
                "invalid {header_name} token"
            )))
        }
    }
}

impl FromStr for BearerToken {
    type Err = BearerTokenError;

    /// Parses a token from a string slice. Trims leading/trailing
    /// whitespace, rejects empty values, and rejects non-ASCII strings
    /// (gRPC metadata cannot transport them without base64 encoding).
    fn from_str(token: &str) -> Result<Self, Self::Err> {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            return Err(BearerTokenError::Empty);
        }
        if !trimmed.is_ascii() {
            return Err(BearerTokenError::NonAscii);
        }
        Ok(Self {
            secret: SecretString::new(trimmed.to_owned().into()),
        })
    }
}

/// Errors raised while loading or validating a [`BearerToken`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BearerTokenError {
    /// The candidate token string was empty after trimming.
    #[error("bearer token must not be empty")]
    Empty,
    /// The candidate token string contains non-ASCII bytes (which gRPC
    /// metadata cannot transport without base64 encoding).
    #[error("bearer token must be ASCII")]
    NonAscii,
    /// The token file could not be read.
    #[error("could not read bearer token from {path}: {source}")]
    ReadFailed {
        /// Diagnostic path string.
        path: String,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
}

/// Tonic interceptor that validates `authorization: Bearer <token>` on
/// incoming gRPC requests. When `expected` is `None`, every request
/// passes (the localhost-default deployment story).
#[derive(Clone, Debug)]
pub struct BearerTokenServerInterceptor {
    expected: Option<BearerToken>,
}

impl BearerTokenServerInterceptor {
    /// Creates a server-side interceptor.
    #[must_use]
    pub const fn new(expected: Option<BearerToken>) -> Self {
        Self { expected }
    }
}

impl tonic::service::Interceptor for BearerTokenServerInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let Some(expected_token) = self.expected.as_ref() else {
            return Ok(request);
        };
        let metadata_value = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("missing authorization header"))?;
        let header_string = metadata_value
            .to_str()
            .map_err(|_| Status::unauthenticated("authorization header is not ASCII"))?;
        let claimed = header_string
            .strip_prefix("Bearer ")
            .ok_or_else(|| Status::unauthenticated("authorization header missing Bearer scheme"))?;
        if !expected_token.matches(claimed) {
            return Err(Status::unauthenticated("invalid bearer token"));
        }
        Ok(request)
    }
}

/// Tonic interceptor that attaches `authorization: Bearer <token>` to
/// outgoing gRPC requests. When `token` is `None`, no metadata is added.
#[derive(Clone)]
pub struct BearerTokenClientInterceptor {
    metadata_value: Option<tonic::metadata::AsciiMetadataValue>,
}

impl std::fmt::Debug for BearerTokenClientInterceptor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BearerTokenClientInterceptor")
            .field("token_attached", &self.metadata_value.is_some())
            .finish()
    }
}

impl BearerTokenClientInterceptor {
    /// Creates a client-side interceptor.
    pub fn new(token: Option<&BearerToken>) -> Result<Self, BearerTokenError> {
        let metadata_value = match token {
            Some(token) => Some(bearer_metadata(token)?),
            None => None,
        };
        Ok(Self { metadata_value })
    }
}

impl tonic::service::Interceptor for BearerTokenClientInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(metadata) = self.metadata_value.as_ref() {
            request
                .metadata_mut()
                .insert("authorization", metadata.clone());
        }
        Ok(request)
    }
}

/// Builds an opaque `Bearer` metadata value without exposing token text.
///
/// Used for a dedicated method-level capability header when a control plane
/// needs stronger authority than its ordinary authenticated RPCs.
pub fn bearer_metadata(
    token: &BearerToken,
) -> Result<tonic::metadata::AsciiMetadataValue, BearerTokenError> {
    let formatted = format!("Bearer {}", token.expose_for_metadata());
    formatted
        .parse::<tonic::metadata::AsciiMetadataValue>()
        .map_err(|_| BearerTokenError::NonAscii)
}

/// Tonic [`Channel`] with [`BearerTokenClientInterceptor`] attached.
///
/// Tonic's generated gRPC clients accept the intercepted service through
/// `Client::new(channel)`, so callers wrap the value returned by
/// [`crate::transport::connect_zinder_grpc`] in their service-specific
/// client type.
pub type AuthenticatedChannel = InterceptedService<Channel, BearerTokenClientInterceptor>;

/// Errors returned by [`crate::transport::connect_zinder_grpc`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BearerTokenConnectError {
    /// The endpoint URL could not be parsed.
    #[error("zinder gRPC endpoint URL is invalid: {0}")]
    InvalidEndpoint(#[source] tonic::transport::Error),
    /// The transport could not connect to the endpoint.
    #[error("zinder gRPC transport connect failed: {0}")]
    Transport(#[source] tonic::transport::Error),
    /// The dial did not complete within its connect timeout.
    #[error("zinder gRPC dial did not complete within {connect_timeout:?}")]
    ConnectTimeout {
        /// Bound the dial was given.
        connect_timeout: std::time::Duration,
    },
    /// TLS for an `https` endpoint could not be set up.
    #[error("zinder gRPC TLS setup failed: {reason}")]
    TlsSetup {
        /// Why the setup failed.
        reason: String,
    },
    /// The bearer token could not be encoded as gRPC metadata.
    #[error(transparent)]
    Token(#[from] BearerTokenError),
}

#[cfg(test)]
mod tests {
    #![allow(
        missing_docs,
        reason = "Unit test names describe the behavior under test."
    )]

    use super::{
        BearerToken, BearerTokenClientInterceptor, BearerTokenError, BearerTokenServerInterceptor,
    };
    use std::str::FromStr;
    use tonic::{Request, service::Interceptor};

    #[test]
    fn from_str_rejects_empty_token() {
        assert!(matches!(
            BearerToken::from_str(""),
            Err(BearerTokenError::Empty)
        ));
        assert!(matches!(
            BearerToken::from_str("   "),
            Err(BearerTokenError::Empty)
        ));
    }

    #[test]
    fn from_str_rejects_non_ascii_token() {
        assert!(matches!(
            BearerToken::from_str("tok\u{00e9}n"),
            Err(BearerTokenError::NonAscii)
        ));
    }

    #[test]
    fn debug_does_not_leak_secret() -> Result<(), BearerTokenError> {
        let token = BearerToken::from_str("super-secret-token")?;
        let formatted = format!("{token:?}");
        assert!(!formatted.contains("super-secret-token"));
        assert!(formatted.contains("REDACTED"));
        Ok(())
    }

    #[test]
    fn server_interceptor_passes_when_no_token_configured() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut interceptor = BearerTokenServerInterceptor::new(None);
        let request = Request::new(());
        interceptor
            .call(request)
            .map_err(|status| status.to_string())?;
        Ok(())
    }

    #[test]
    fn server_interceptor_rejects_missing_header() -> Result<(), Box<dyn std::error::Error>> {
        let token = BearerToken::from_str("expected")?;
        let mut interceptor = BearerTokenServerInterceptor::new(Some(token));
        let outcome = interceptor.call(Request::new(()));
        let status = outcome.err().ok_or("expected unauthenticated")?;
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        Ok(())
    }

    #[test]
    fn server_interceptor_accepts_valid_token() -> Result<(), Box<dyn std::error::Error>> {
        let token = BearerToken::from_str("expected")?;
        let mut server = BearerTokenServerInterceptor::new(Some(token.clone()));
        let mut client = BearerTokenClientInterceptor::new(Some(&token))?;
        let request = client
            .call(Request::new(()))
            .map_err(|status| status.to_string())?;
        server.call(request).map_err(|status| status.to_string())?;
        Ok(())
    }

    #[test]
    fn server_interceptor_rejects_wrong_token() -> Result<(), Box<dyn std::error::Error>> {
        let server_token = BearerToken::from_str("server-side")?;
        let client_token = BearerToken::from_str("client-side")?;
        let mut server = BearerTokenServerInterceptor::new(Some(server_token));
        let mut client = BearerTokenClientInterceptor::new(Some(&client_token))?;
        let request = client
            .call(Request::new(()))
            .map_err(|status| status.to_string())?;
        let outcome = server.call(request);
        let status = outcome.err().ok_or("expected unauthenticated")?;
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        Ok(())
    }

    #[test]
    fn from_file_loads_and_trims_token() -> Result<(), Box<dyn std::error::Error>> {
        let tempdir = tempfile::TempDir::new()?;
        let token_path = tempdir.path().join("token");
        std::fs::write(&token_path, "  expected-token\n")?;
        let token = BearerToken::from_file(&token_path)?;
        let mut server = BearerTokenServerInterceptor::new(Some(token.clone()));
        let mut client = BearerTokenClientInterceptor::new(Some(&token))?;
        let request = client
            .call(Request::new(()))
            .map_err(|status| status.to_string())?;
        server.call(request).map_err(|status| status.to_string())?;
        Ok(())
    }
}
