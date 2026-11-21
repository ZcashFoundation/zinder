use std::{io, num::NonZeroU32, path::PathBuf, time::Duration};

use clap::Args;
use tokio_util::sync::CancellationToken;
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
    /// First covered height. Defaults to the first retained height.
    #[arg(long)]
    first_height: Option<u32>,
    /// Number of settled heights per segment.
    #[arg(long, default_value = "4096")]
    segment_blocks: NonZeroU32,
    /// Keep publishing and check for newly settled blocks at this interval.
    #[arg(long, value_name = "SECONDS")]
    follow_interval_secs: Option<NonZeroU32>,
}

impl PublishHistory {
    pub(super) async fn publish(
        &self,
        config: &LightwalletdConfig,
    ) -> Result<(), LightwalletdConfigError> {
        let stopping = CancellationToken::new();
        let _signal = self
            .follow_interval_secs
            .map(|_| zinder_runtime::cancel_on_terminating_signal(stopping.clone()));
        let scratch = tempfile::tempdir()?;
        let mut secondary = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
            &config.storage.path,
            scratch.path(),
            config.network,
            CanonicalStoreWorkload::Wallet,
            config.storage.expected_raw_blob_retention,
            config.canonical_reorg_policy,
            config.storage.canonical_rocksdb_budget,
        )?;
        let first = self.first_height.map_or_else(
            || secondary.history_bounds().first_available_height(),
            BlockHeight::new,
        );
        let mut publisher = ShieldedCatalogPublisher::open(
            &self.directory,
            &self.signing_key,
            first,
            self.segment_blocks,
        )?;
        loop {
            let reader = RocksDbCanonicalReader::new(secondary).map_err(io::Error::other)?;
            let mut segments = 0_u32;
            while !stopping.is_cancelled() && publisher.publish_next(&reader)? {
                segments += 1;
            }
            if segments > 0 || self.follow_interval_secs.is_none() {
                tracing::info!(segments, "settled shielded history publication complete");
            }
            let Some(interval) = self.follow_interval_secs else {
                return Ok(());
            };
            tokio::select! {
                biased;
                () = stopping.cancelled() => return Ok(()),
                () = tokio::time::sleep(Duration::from_secs(u64::from(interval.get()))) => {}
            }
            secondary = reader
                .into_secondary()
                .map_err(|_| io::Error::other("publisher canonical reader is still shared"))?;
            secondary.try_catch_up()?;
        }
    }
}
