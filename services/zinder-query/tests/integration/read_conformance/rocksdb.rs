//! The conformance suite run against the `RocksDB` serving readers.

use std::time::Duration;

use eyre::eyre;
use tempfile::TempDir;
use zinder_core::{BlockHeight, BlockHeightRange, Network, NetworkUpgradeActivations};
use zinder_query::{
    CanonicalReader, RocksDbCanonicalReader, RocksDbWalletReader, StorageReadError,
    read_on_blocking_thread,
};
use zinder_store::{CanonicalStoreWorkload, RocksDbCanonicalSecondary, RocksDbResourceBudget};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, sample_regtest_upgrade_activations,
};

use super::{
    ReaderHarness, WalletReaderHarness, canonical_reader_conformance, wallet_reader_conformance,
};

/// A production canonical primary, with readers opened as `RocksDB`
/// secondaries beside it.
pub(crate) struct RocksDbReaderHarness {
    writer: CanonicalWriterFixture,
    secondaries: TempDir,
    readers_opened: u32,
}

impl ReaderHarness for RocksDbReaderHarness {
    type Canonical = RocksDbCanonicalReader;

    fn publish(
        baseline: &CanonicalBaseline<'_>,
        activations: &NetworkUpgradeActivations,
    ) -> eyre::Result<Self> {
        Ok(Self {
            writer: CanonicalWriterFixture::publish(baseline, activations)?,
            secondaries: TempDir::new()?,
            readers_opened: 0,
        })
    }

    fn append_live_block(&mut self, chain: &ChainFixture) -> eyre::Result<()> {
        self.writer.append_live_block(chain).map(drop)
    }

    fn replace_live_blocks(
        &mut self,
        replacement_chain: &ChainFixture,
        first_replaced_height: BlockHeight,
    ) -> eyre::Result<()> {
        self.writer
            .replace_live_blocks(replacement_chain, first_replaced_height)
            .map(drop)
    }

    fn prune_chain_events_before(&mut self, oldest_retained_sequence: u64) -> eyre::Result<()> {
        self.writer
            .prune_canonical_events_before(oldest_retained_sequence)
            .map(drop)
    }

    fn canonical_reader(&mut self) -> eyre::Result<RocksDbCanonicalReader> {
        Ok(RocksDbCanonicalReader::new(self.canonical_secondary()?)?)
    }
}

impl RocksDbReaderHarness {
    /// Opens a canonical secondary at the current fence.
    fn canonical_secondary(&mut self) -> eyre::Result<RocksDbCanonicalSecondary> {
        self.readers_opened = self
            .readers_opened
            .checked_add(1)
            .ok_or_else(|| eyre!("the harness opened too many readers"))?;
        let primary = self.writer.canonical_primary()?;
        Ok(
            RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
                self.writer.canonical_primary_path(),
                self.secondaries
                    .path()
                    .join(format!("canonical-{}", self.readers_opened)),
                primary.network(),
                CanonicalStoreWorkload::Wallet,
                primary.raw_blob_retention(),
                primary.reorg_policy(),
                RocksDbResourceBudget::for_local_tests(),
            )?,
        )
    }
}

/// Admitting a `RocksDB` canonical reader fails with the engine-neutral
/// [`StorageReadError`], so code that admits one names no `RocksDB` error.
#[test]
fn admitting_a_rocksdb_canonical_reader_fails_with_the_neutral_read_error() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let mut harness = RocksDbReaderHarness::publish(
        &CanonicalBaseline::new(&chain),
        &sample_regtest_upgrade_activations(),
    )?;
    let admitted: Result<RocksDbCanonicalReader, StorageReadError> =
        RocksDbCanonicalReader::new(harness.canonical_secondary()?);
    assert_eq!(
        admitted?.chain_epoch().visible_tip_height,
        BlockHeight::new(2)
    );
    Ok(())
}

/// A read that [`read_on_blocking_thread`] drives runs on the blocking
/// thread driving it, so it waits for no second blocking thread.
///
/// The runtime has one blocking thread, and the read's driver holds it.
#[test]
fn a_read_driven_on_a_blocking_thread_reads_on_that_thread() -> eyre::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()?;
    let outcome = runtime.block_on(async {
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
        let mut harness = RocksDbReaderHarness::publish(
            &CanonicalBaseline::new(&chain),
            &sample_regtest_upgrade_activations(),
        )?;
        let reader = harness.canonical_reader()?;
        let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(3));
        let driven = tokio::task::spawn_blocking(move || {
            read_on_blocking_thread(reader.compact_blocks_in_range(range))
        });
        let blocks = tokio::time::timeout(Duration::from_secs(5), driven)
            .await
            .map_err(|_| eyre!("the driven read waited for a second blocking thread"))???;
        assert_eq!(blocks.len(), 3);
        eyre::Ok(())
    });
    runtime.shutdown_timeout(Duration::from_secs(1));
    outcome
}

impl WalletReaderHarness for RocksDbReaderHarness {
    type Wallet = RocksDbWalletReader;

    fn wallet_reader(&mut self) -> eyre::Result<RocksDbWalletReader> {
        // The serving fixture shares the writer's directory, so its readers
        // stay valid after the fixture is dropped.
        let (_canonical, wallet) = self.writer.serving_store_fixture()?.take_readers()?;
        Ok(RocksDbWalletReader::new(wallet))
    }
}

mod canonical_cases {
    super::canonical_reader_conformance!(super::RocksDbReaderHarness);
}

mod wallet_cases {
    super::wallet_reader_conformance!(super::RocksDbReaderHarness);
}
