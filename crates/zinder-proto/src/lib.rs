//! Zinder protocol ownership boundary.
//!
//! Service protocol schemas and generated modules live here so service crates
//! do not hand-write protocol-shaped payload structs.

pub mod capabilities;
pub mod error_policy;
pub mod wire;
pub use capabilities::{
    AdvertisePolicy, CAPABILITIES, Capability, CapabilityDescriptor, CapabilitySpec,
    CapabilitySurface, ExplorerReadiness, always_on_capability_strings, capabilities_for_surface,
};
pub use error_policy::{
    BoundaryError, ReasonPolicy, RetryDisposition, ZINDER_ERROR_DOMAIN, reason_policy,
    status_for_reason, status_with_reason, status_with_reason_and_metadata,
};

/// Wire-contract revision advertised in every `ops.ServerInfo` response.
///
/// Monotonically increasing marker, incremented whenever the semantics of an
/// existing wire surface are revised in place. Consumers require the minimum
/// revision they implement and may accept newer revisions when the server's
/// advertised capabilities satisfy their requirements.
pub const CONTRACT_REVISION: u32 = 6;

/// Encoded descriptor set for native Zinder v1 protobuf services.
///
/// Wire it into `tonic_reflection::server::Builder::configure().register_encoded_file_descriptor_set(...)`
/// to expose `grpc.reflection.v1.ServerReflection` on the native query server.
pub const ZINDER_V1_FILE_DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/generated/zinder_v1_descriptor.bin"
));

/// Encoded descriptor set for the vendored lightwalletd compatibility schemas.
pub const LIGHTWALLETD_COMPAT_FILE_DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/generated/lightwalletd_compat_descriptor.bin"
));

/// Native Zinder protocol modules.
pub mod v1 {
    /// Private ingest control-plane protocol messages.
    pub mod ingest {
        #![allow(
            clippy::allow_attributes_without_reason,
            clippy::default_trait_access,
            clippy::derive_partial_eq_without_eq,
            clippy::disallowed_names,
            clippy::doc_markdown,
            clippy::missing_fields_in_debug,
            clippy::must_use_candidate,
            clippy::too_long_first_doc_paragraph,
            clippy::too_many_lines,
            missing_docs,
            reason = "Generated protobuf code mirrors owned schemas."
        )]

        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/generated/zinder.v1.ingest.rs"
        ));
    }

    /// Operations-plane protocol messages (readiness, error vocabulary,
    /// shared ServerInfo) consumed by the ops endpoint and any future
    /// orchestrator-facing gRPC surface.
    pub mod ops {
        #![allow(
            clippy::allow_attributes_without_reason,
            clippy::default_trait_access,
            clippy::derive_partial_eq_without_eq,
            clippy::disallowed_names,
            clippy::doc_markdown,
            clippy::missing_fields_in_debug,
            clippy::must_use_candidate,
            clippy::too_long_first_doc_paragraph,
            clippy::too_many_lines,
            missing_docs,
            reason = "Generated protobuf code mirrors owned schemas."
        )]

        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/generated/zinder.v1.ops.rs"
        ));
    }

    /// Explorer-shaped read protocol messages served by zinder-explorer.
    pub mod explorer {
        #![allow(
            clippy::allow_attributes_without_reason,
            clippy::default_trait_access,
            clippy::derive_partial_eq_without_eq,
            clippy::disallowed_names,
            clippy::doc_markdown,
            clippy::missing_fields_in_debug,
            clippy::must_use_candidate,
            clippy::too_many_lines,
            missing_docs,
            reason = "Generated protobuf code mirrors owned schemas."
        )]

        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/generated/zinder.v1.explorer.rs"
        ));
    }

    /// Native wallet and wallet-like application protocol messages.
    pub mod wallet {
        #![allow(
            clippy::allow_attributes_without_reason,
            clippy::default_trait_access,
            clippy::derive_partial_eq_without_eq,
            clippy::disallowed_names,
            clippy::doc_markdown,
            clippy::large_enum_variant,
            clippy::missing_fields_in_debug,
            clippy::must_use_candidate,
            clippy::too_many_lines,
            missing_docs,
            reason = "Generated protobuf code mirrors owned schemas."
        )]

        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/generated/zinder.v1.wallet.rs"
        ));
    }
}

/// Compatibility protocol modules kept separate from native Zinder contracts.
pub mod compat {
    /// Vendored lightwalletd-compatible protocol messages.
    pub mod lightwalletd {
        #![allow(
            clippy::allow_attributes_without_reason,
            clippy::default_trait_access,
            clippy::derive_partial_eq_without_eq,
            clippy::doc_markdown,
            clippy::doc_overindented_list_items,
            clippy::disallowed_names,
            clippy::must_use_candidate,
            clippy::too_many_lines,
            clippy::too_long_first_doc_paragraph,
            missing_docs,
            reason = "Generated protobuf code mirrors vendored schemas."
        )]

        /// Upstream `lightwallet-protocol` commit served by this compatibility pin.
        pub const LIGHTWALLETD_PROTOCOL_COMMIT: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/generated/lightwalletd_protocol_commit.txt"
        ));

        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/generated/cash.z.wallet.sdk.rpc.rs"
        ));
    }
}

/// Vendored upstream protocol modules consumed by Zinder source adapters.
pub mod external {
    /// Vendored Zebra indexer client protocol.
    ///
    /// The mempool change stream and chain-tip notification stream from a
    /// Zebra deployment built with `--features indexer`. Zinder consumes
    /// these as a client; the adapter implementation lives in
    /// `zinder-source`.
    pub mod zebra_indexer_rpc {
        #![allow(
            clippy::allow_attributes_without_reason,
            clippy::default_trait_access,
            clippy::derive_partial_eq_without_eq,
            clippy::doc_markdown,
            clippy::disallowed_names,
            clippy::must_use_candidate,
            clippy::too_many_lines,
            missing_docs,
            reason = "Generated protobuf code mirrors vendored schemas."
        )]

        /// Upstream Zebra commit served by this vendored proto pin.
        pub const ZEBRA_INDEXER_PROTOCOL_COMMIT: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/generated/zebra_indexer_protocol_commit.txt"
        ));

        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/generated/zebra.indexer.rpc.rs"
        ));
    }
}
