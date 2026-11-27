//! CLI string-to-value parsers for the `zinder-ingest` runtime.

use zinder_runtime::ConfigError;

use crate::{IngestError, NodeSourceKind};

/// Parses the public `ingest.source` configuration name.
pub(crate) fn parse_node_source(node_source: &str) -> Result<NodeSourceKind, IngestError> {
    match node_source {
        "zebra-json-rpc" => Ok(NodeSourceKind::ZebraJsonRpc),
        "zebra-indexer-grpc" => Ok(NodeSourceKind::ZebraIndexerGrpc),
        _ => Err(IngestError::UnknownNodeSource {
            node_source: node_source.to_owned(),
        }),
    }
}

/// Parses the maximum replaceable reorg-window size.
pub(crate) fn parse_reorg_window_blocks(reorg_window_blocks: u32) -> Result<u32, ConfigError> {
    if reorg_window_blocks == 0 {
        return Err(ConfigError::invalid(
            "ingest.reorg_window_blocks must be greater than zero",
        ));
    }

    Ok(reorg_window_blocks)
}

/// Parses the canonical follower poll interval in milliseconds.
pub(crate) fn parse_poll_interval_ms(
    poll_interval_ms: u64,
) -> Result<std::time::Duration, ConfigError> {
    if poll_interval_ms == 0 {
        return Err(ConfigError::invalid(
            "ingest.follow.poll_interval_ms must be greater than zero",
        ));
    }

    Ok(std::time::Duration::from_millis(poll_interval_ms))
}
