//! Client-owned remote error-reason vocabulary.

macro_rules! define_error_reasons {
    ($( $variant:ident => $wire_name:literal, )+) => {
        /// Stable reason attached to a Zinder remote error.
        ///
        /// Known variants follow the current wire contract. [`Self::Unknown`]
        /// preserves an additive reason emitted by a newer server.
        #[derive(Clone, Debug, Eq, Hash, PartialEq)]
        #[non_exhaustive]
        pub enum ErrorReason {
            $(
                #[doc = concat!("Wire reason `", $wire_name, "`.")]
                $variant,
            )+
            /// Additive reason emitted by a newer server.
            Unknown(String),
        }

        impl ErrorReason {
            /// Converts an exact wire reason while preserving unknown values.
            #[must_use]
            pub fn from_wire_name(reason: &str) -> Self {
                match reason {
                    $( $wire_name => Self::$variant, )+
                    _ => Self::Unknown(reason.to_owned()),
                }
            }

            /// Returns the exact reason string carried on the wire.
            #[must_use]
            pub fn as_str(&self) -> &str {
                match self {
                    $( Self::$variant => $wire_name, )+
                    Self::Unknown(reason) => reason,
                }
            }
        }
    };
}

define_error_reasons! {
    Unspecified => "ERROR_REASON_UNSPECIFIED",
    InvalidBlockRange => "INVALID_BLOCK_RANGE",
    BlockRangeTooLarge => "BLOCK_RANGE_TOO_LARGE",
    SubtreeRootRangeTooLarge => "SUBTREE_ROOT_RANGE_TOO_LARGE",
    ChainEventCursorInvalid => "CHAIN_EVENT_CURSOR_INVALID",
    AddressOutputCursorInvalid => "ADDRESS_OUTPUT_CURSOR_INVALID",
    TransparentHistoryCursorInvalid => "TRANSPARENT_HISTORY_CURSOR_INVALID",
    InvalidAddress => "INVALID_ADDRESS",
    UnsupportedShieldedProtocol => "UNSUPPORTED_SHIELDED_PROTOCOL",
    InvalidChainStoreOptions => "INVALID_CHAIN_STORE_OPTIONS",
    ArtifactPayloadTooLarge => "ARTIFACT_PAYLOAD_TOO_LARGE",
    InvalidChainEpochArtifacts => "INVALID_CHAIN_EPOCH_ARTIFACTS",
    TransparentBalanceAddressCountExceeded => "TRANSPARENT_BALANCE_ADDRESS_COUNT_EXCEEDED",
    SnapshotPageCursorInvalid => "SNAPSHOT_PAGE_CURSOR_INVALID",
    BroadcastTransactionTooLarge => "BROADCAST_TRANSACTION_TOO_LARGE",
    BroadcastDisabled => "BROADCAST_DISABLED",
    ChainEventCursorExpired => "CHAIN_EVENT_CURSOR_EXPIRED",
    MempoolEventCursorExpired => "MEMPOOL_EVENT_CURSOR_EXPIRED",
    SnapshotPageCursorExpired => "SNAPSHOT_PAGE_CURSOR_EXPIRED",
    ChainEpochPinUnavailable => "CHAIN_EPOCH_PIN_UNAVAILABLE",
    SchemaMismatch => "SCHEMA_MISMATCH",
    SchemaTooNew => "SCHEMA_TOO_NEW",
    ReorgWindowExceeded => "REORG_WINDOW_EXCEEDED",
    ChainEpochConflict => "CHAIN_EPOCH_CONFLICT",
    ChainEpochNetworkMismatch => "CHAIN_EPOCH_NETWORK_MISMATCH",
    ArtifactUnavailable => "ARTIFACT_UNAVAILABLE",
    ChainEpochMissing => "CHAIN_EPOCH_MISSING",
    BlockNotInBestChain => "BLOCK_NOT_IN_BEST_CHAIN",
    CompactBlockPayloadMalformed => "COMPACT_BLOCK_PAYLOAD_MALFORMED",
    ArtifactCorrupt => "ARTIFACT_CORRUPT",
    UnsupportedChainEvent => "UNSUPPORTED_CHAIN_EVENT",
    UnsupportedBlockSelector => "UNSUPPORTED_BLOCK_SELECTOR",
    UnsupportedTransactionStatus => "UNSUPPORTED_TRANSACTION_STATUS",
    BlockingTaskFailed => "BLOCKING_TASK_FAILED",
    NodeUnavailable => "NODE_UNAVAILABLE",
    StorageUnavailable => "STORAGE_UNAVAILABLE",
    UnsupportedWalletEncoding => "UNSUPPORTED_WALLET_ENCODING",
    EntropyUnavailable => "ENTROPY_UNAVAILABLE",
    ExplorerInternal => "EXPLORER_INTERNAL",
    MaterializedViewUnavailable => "MATERIALIZED_VIEW_UNAVAILABLE",
    EndpointCapabilityUnavailable => "ENDPOINT_CAPABILITY_UNAVAILABLE",
    NodeCapabilityMissing => "NODE_CAPABILITY_MISSING",
    ExplorerPreconditionUnsatisfied => "EXPLORER_PRECONDITION_UNSATISFIED",
    NoVisibleChainEpoch => "NO_VISIBLE_CHAIN_EPOCH",
    ExplorerMethodDisabled => "EXPLORER_METHOD_DISABLED",
    DependencyNotConfigured => "DEPENDENCY_NOT_CONFIGURED",
    UpstreamUnreachable => "UPSTREAM_UNREACHABLE",
    ServiceNotReady => "SERVICE_NOT_READY",
    TransactionNotFound => "TRANSACTION_NOT_FOUND",
    CanonicalHistoryUnavailable => "CANONICAL_HISTORY_UNAVAILABLE",
}

#[cfg(all(test, feature = "remote"))]
mod tests {
    use zinder_proto::v1::ops::ErrorReason as WireErrorReason;

    use super::ErrorReason;

    #[test]
    fn every_generated_reason_converts_without_fallback() {
        let generated_reasons = [
            WireErrorReason::Unspecified,
            WireErrorReason::InvalidBlockRange,
            WireErrorReason::BlockRangeTooLarge,
            WireErrorReason::SubtreeRootRangeTooLarge,
            WireErrorReason::ChainEventCursorInvalid,
            WireErrorReason::AddressOutputCursorInvalid,
            WireErrorReason::TransparentHistoryCursorInvalid,
            WireErrorReason::InvalidAddress,
            WireErrorReason::UnsupportedShieldedProtocol,
            WireErrorReason::InvalidChainStoreOptions,
            WireErrorReason::ArtifactPayloadTooLarge,
            WireErrorReason::InvalidChainEpochArtifacts,
            WireErrorReason::TransparentBalanceAddressCountExceeded,
            WireErrorReason::SnapshotPageCursorInvalid,
            WireErrorReason::BroadcastTransactionTooLarge,
            WireErrorReason::BroadcastDisabled,
            WireErrorReason::ChainEventCursorExpired,
            WireErrorReason::MempoolEventCursorExpired,
            WireErrorReason::SnapshotPageCursorExpired,
            WireErrorReason::ChainEpochPinUnavailable,
            WireErrorReason::SchemaMismatch,
            WireErrorReason::SchemaTooNew,
            WireErrorReason::ReorgWindowExceeded,
            WireErrorReason::ChainEpochConflict,
            WireErrorReason::ChainEpochNetworkMismatch,
            WireErrorReason::ArtifactUnavailable,
            WireErrorReason::ChainEpochMissing,
            WireErrorReason::BlockNotInBestChain,
            WireErrorReason::CompactBlockPayloadMalformed,
            WireErrorReason::ArtifactCorrupt,
            WireErrorReason::UnsupportedChainEvent,
            WireErrorReason::UnsupportedBlockSelector,
            WireErrorReason::UnsupportedTransactionStatus,
            WireErrorReason::BlockingTaskFailed,
            WireErrorReason::NodeUnavailable,
            WireErrorReason::StorageUnavailable,
            WireErrorReason::UnsupportedWalletEncoding,
            WireErrorReason::EntropyUnavailable,
            WireErrorReason::ExplorerInternal,
            WireErrorReason::MaterializedViewUnavailable,
            WireErrorReason::EndpointCapabilityUnavailable,
            WireErrorReason::NodeCapabilityMissing,
            WireErrorReason::ExplorerPreconditionUnsatisfied,
            WireErrorReason::NoVisibleChainEpoch,
            WireErrorReason::ExplorerMethodDisabled,
            WireErrorReason::DependencyNotConfigured,
            WireErrorReason::UpstreamUnreachable,
            WireErrorReason::ServiceNotReady,
            WireErrorReason::TransactionNotFound,
            WireErrorReason::CanonicalHistoryUnavailable,
        ];

        for generated in generated_reasons {
            let client = ErrorReason::from_wire_name(generated.as_str_name());
            assert!(!matches!(client, ErrorReason::Unknown(_)));
            assert_eq!(client.as_str(), generated.as_str_name());
        }
    }
}
