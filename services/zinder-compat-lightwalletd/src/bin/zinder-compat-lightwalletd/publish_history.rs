use std::{num::NonZeroU32, path::PathBuf};

use clap::Args;
use zinder_compat_lightwalletd::ShieldedCatalogPublisher;
use zinder_core::BlockHeight;
use zinder_query::RocksDbCanonicalReader;
use zinder_store::{CanonicalStoreWorkload, RocksDbCanonicalSecondary};

use super::config::{LightwalletdConfig, LightwalletdConfigError};

#[derive(Args)]
pub(super) struct PublishHistory {
    /// Directory for the signed current.json, segment files, and publisher state.
    #[arg(long)]
    directory: PathBuf,
    /// File containing the publisher's DER-encoded Ed25519 PKCS#8 private key.
    #[arg(long)]
    signing_key: PathBuf,
    /// First covered height, including on subsequent runs.
    #[arg(long)]
    first_height: u32,
    /// Number of settled heights per segment.
    #[arg(long, default_value = "4096")]
    segment_blocks: NonZeroU32,
}

impl PublishHistory {
    pub(super) fn publish(
        &self,
        config: &LightwalletdConfig,
    ) -> Result<(), LightwalletdConfigError> {
        let scratch = tempfile::tempdir()?;
        let secondary = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
            &config.storage.path,
            scratch.path(),
            config.network,
            CanonicalStoreWorkload::Wallet,
            config.storage.expected_raw_blob_retention,
            config.canonical_reorg_policy,
            config.storage.canonical_rocksdb_budget,
        )?;
        let reader = RocksDbCanonicalReader::new(secondary).map_err(std::io::Error::other)?;
        let mut publisher = ShieldedCatalogPublisher::open(
            &self.directory,
            &self.signing_key,
            BlockHeight::new(self.first_height),
            self.segment_blocks,
        )?;
        let mut segments = 0;
        while publisher.publish_next(&reader)? {
            segments += 1_u32;
        }
        tracing::info!(segments, "settled shielded history publication complete");
        Ok(())
    }
}
