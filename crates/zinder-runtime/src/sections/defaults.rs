//! Shared configuration defaults for Zinder runtimes.

use crate::sections::RuntimeService;

/// Default base data directory from which Zinder services derive store paths
/// when `storage.data_dir` and explicit storage paths are unset.
pub const DEFAULT_DATA_DIR: &str = "/var/lib/zinder";

/// Default canonical store directory for ingest, Explorer, and the projector.
pub const DEFAULT_CANONICAL_PATH: &str = "/var/lib/zinder/canonical";

/// Default operational endpoint listen address per service.
///
/// Binds to `127.0.0.1` so the safe default works on bare metal without
/// exposing the endpoint. Container deployments override via
/// `ZINDER_OPS__LISTEN_ADDR=0.0.0.0:<port>` baked in each service's
/// Dockerfile.
#[must_use]
pub const fn default_ops_listen_addr(service: RuntimeService) -> &'static str {
    match service {
        RuntimeService::Ingest => "127.0.0.1:9105",
        RuntimeService::Projector => "127.0.0.1:9110",
        RuntimeService::Query => "127.0.0.1:9106",
        RuntimeService::CompatLightwalletd => "127.0.0.1:9107",
        RuntimeService::CompatCipherscan => "127.0.0.1:9108",
        RuntimeService::Explorer => "127.0.0.1:9069",
    }
}

/// Default gRPC listen address per service, when the service binds one.
///
/// [`RuntimeService::Ingest`] returns `None` because the ingest writer
/// does not expose a public gRPC endpoint; its private `IngestControl`
/// endpoint uses [`DEFAULT_INGEST_CONTROL_LISTEN_ADDR`].
#[must_use]
pub const fn default_grpc_listen_addr(service: RuntimeService) -> Option<&'static str> {
    match service {
        RuntimeService::Query => Some("127.0.0.1:9102"),
        RuntimeService::CompatLightwalletd => Some("127.0.0.1:9067"),
        RuntimeService::Explorer => Some("127.0.0.1:9068"),
        RuntimeService::CompatCipherscan | RuntimeService::Ingest | RuntimeService::Projector => {
            None
        }
    }
}

/// Default ingest-control writer listen address.
pub const DEFAULT_INGEST_CONTROL_LISTEN_ADDR: &str = "127.0.0.1:9100";

/// Default ingest-control reader URL.
pub const DEFAULT_INGEST_CONTROL_READER_URL: &str = "http://127.0.0.1:9100";

/// Default directory used by the canonical owner to stage checkpoint
/// candidates. Operators may override this with
/// `ingest_control.checkpoint_staging_root`.
pub const DEFAULT_INGEST_CONTROL_CHECKPOINT_STAGING_ROOT: &str = "/var/lib/zinder/checkpoints";

/// Default private projector-owner control listen address.
pub const DEFAULT_PROJECTOR_CONTROL_LISTEN_ADDR: &str = "127.0.0.1:9101";

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn ops_default_ports_are_distinct() {
        let ports: HashSet<&'static str> = [
            default_ops_listen_addr(RuntimeService::Ingest),
            default_ops_listen_addr(RuntimeService::Projector),
            default_ops_listen_addr(RuntimeService::Query),
            default_ops_listen_addr(RuntimeService::CompatLightwalletd),
            default_ops_listen_addr(RuntimeService::CompatCipherscan),
            default_ops_listen_addr(RuntimeService::Explorer),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            ports.len(),
            6,
            "per-service ops ports must be distinct so a single host can run \
             all six binaries without port conflicts",
        );
    }

    #[test]
    fn native_wallet_query_defaults_do_not_reuse_projector_control() {
        assert_eq!(
            default_grpc_listen_addr(RuntimeService::Query),
            Some("127.0.0.1:9102")
        );
        assert_eq!(
            default_ops_listen_addr(RuntimeService::Query),
            "127.0.0.1:9106"
        );
        assert_ne!(
            default_grpc_listen_addr(RuntimeService::Query),
            Some(DEFAULT_PROJECTOR_CONTROL_LISTEN_ADDR)
        );
    }
}
