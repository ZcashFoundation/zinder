//! Production-store fixtures for the immutable wallet-serving boundary.

use std::{path::Path, sync::Arc};

use eyre::{ensure, eyre};
use tempfile::TempDir;
use zinder_core::{BlockHeight, NetworkUpgradeActivations};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalSecondary,
    RocksDbCanonicalStore, RocksDbResourceBudget,
};
use zinder_wallet_rocksdb::{
    RocksDbWalletBuildOptions, RocksDbWalletSecondary, build_wallet_from_canonical,
};

use crate::{CanonicalBaseline, CanonicalWriterFixture, ChainFixture};

/// Tempdir-backed production canonical and wallet secondaries at one READY fence.
///
/// This fixture deliberately builds both primaries through their production
/// publication paths before opening read-only secondaries. Tests can therefore
/// certify a `WalletServingReadPair` without substituting an
/// in-memory reader or a retired materialized-view adapter. Use
/// [`CanonicalWriterFixture`] for history that is checkpointed, followed,
/// reorged, or pruned before it is served.
pub struct WalletServingStoreFixture {
    canonical_reader: Option<RocksDbCanonicalSecondary>,
    wallet_reader: Option<RocksDbWalletSecondary>,
    _temporary_directory: Arc<TempDir>,
}

impl WalletServingStoreFixture {
    /// Builds READY primaries from `chain_fixture` and opens immutable secondaries.
    ///
    /// Checkpoints omit the frontier of every pool whose tree is still empty,
    /// so an empty-tree checkpoint serves an empty tree-state payload.
    ///
    /// # Errors
    ///
    /// Returns an error when the fixture is empty, omits a transaction blob,
    /// declares commitment-tree sizes its compact blocks cannot reach, or
    /// fails either production store's validation and publication contract.
    pub fn from_chain(
        chain_fixture: &ChainFixture,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> eyre::Result<Self> {
        CanonicalWriterFixture::publish(
            &CanonicalBaseline::new(chain_fixture).omitting_empty_frontiers(),
            network_upgrade_activations,
        )?
        .into_serving_store_fixture()
    }

    /// Builds a READY pair whose final block was published through the live
    /// append path, producing a second chain epoch.
    ///
    /// # Errors
    ///
    /// Returns an error when the fixture has fewer than two blocks or either
    /// production store rejects the baseline or live append.
    pub fn from_chain_after_live_append(
        chain_fixture: &ChainFixture,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> eyre::Result<Self> {
        Self::build_after_live_append(chain_fixture, network_upgrade_activations, None)
    }

    /// Builds a READY live-append pair whose canonical writer admitted
    /// `admitted_activations` over a store built with
    /// `construction_activations`.
    ///
    /// Before the live append, the writer admits the construction table as
    /// revision 1, as a writer's first start does, and then
    /// `admitted_activations` as the next revision. The live block is
    /// committed with the admitted table, and the canonical reader opens with
    /// it, as a serving publisher does after following writer status.
    ///
    /// # Errors
    ///
    /// Returns an error when the fixture has fewer than two blocks, the writer
    /// refuses either schedule revision, or either production store rejects
    /// the baseline or live append.
    pub fn from_chain_after_admitted_live_append(
        chain_fixture: &ChainFixture,
        construction_activations: &NetworkUpgradeActivations,
        admitted_activations: &NetworkUpgradeActivations,
    ) -> eyre::Result<Self> {
        Self::build_after_live_append(
            chain_fixture,
            construction_activations,
            Some(admitted_activations),
        )
    }

    fn build_after_live_append(
        chain_fixture: &ChainFixture,
        construction_activations: &NetworkUpgradeActivations,
        admitted_activations: Option<&NetworkUpgradeActivations>,
    ) -> eyre::Result<Self> {
        let baseline_tip = chain_fixture
            .tip_height()
            .and_then(|live_tip| live_tip.value().checked_sub(1))
            .filter(|baseline_tip| *baseline_tip > 0)
            .map(BlockHeight::new)
            .ok_or_else(|| {
                eyre!("wallet-serving live-append fixture requires at least two blocks")
            })?;
        let mut writer = CanonicalWriterFixture::publish(
            &CanonicalBaseline::new(chain_fixture).through(baseline_tip),
            construction_activations,
        )?;
        if let Some(admitted_activations) = admitted_activations {
            writer.admit_network_upgrade_schedule(construction_activations)?;
            writer.admit_network_upgrade_schedule(admitted_activations)?;
        }
        writer.append_live_block(chain_fixture)?;
        writer.into_serving_store_fixture()
    }

    /// Projects the wallet store from an already-published canonical primary,
    /// then opens immutable secondaries for both stores.
    ///
    /// `canonical_primary` must be the READY wallet-workload store at
    /// `canonical_primary_path`, inside `temporary_directory`. The fixture
    /// takes ownership of the directory, builds the wallet primary beside the
    /// canonical one, and drops both primaries before admitting readers. Use
    /// this to serve a canonical store built by production construction, such
    /// as one loaded from a live node.
    ///
    /// # Errors
    ///
    /// Returns an error when the wallet projection, either secondary
    /// admission, or the canonical primary's contract fails.
    pub fn from_published_canonical(
        temporary_directory: TempDir,
        canonical_primary_path: &Path,
        canonical_primary: RocksDbCanonicalStore,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> eyre::Result<Self> {
        Self::admit_closing_primary(
            Arc::new(temporary_directory),
            canonical_primary_path,
            canonical_primary,
            network_upgrade_activations,
        )
    }

    /// Projects the wallet store, closes `canonical_primary`, then admits
    /// readers named without a generation suffix.
    pub(crate) fn admit_closing_primary(
        temporary_directory: Arc<TempDir>,
        canonical_primary_path: &Path,
        canonical_primary: RocksDbCanonicalStore,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> eyre::Result<Self> {
        let reader_paths = ReaderPaths::new(temporary_directory.path(), "");
        let identity = build_wallet_primary(&canonical_primary, &reader_paths)?;
        drop(canonical_primary);
        Self::open_readers(
            temporary_directory,
            canonical_primary_path,
            &reader_paths,
            identity,
            network_upgrade_activations,
        )
    }

    /// Projects the wallet store and admits readers while `canonical_primary`
    /// stays open, naming every path after `generation`.
    pub(crate) fn admit_beside_open_primary(
        temporary_directory: Arc<TempDir>,
        generation: u32,
        canonical_primary_path: &Path,
        canonical_primary: &RocksDbCanonicalStore,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> eyre::Result<Self> {
        let reader_paths = ReaderPaths::new(temporary_directory.path(), &format!("-{generation}"));
        let identity = build_wallet_primary(canonical_primary, &reader_paths)?;
        Self::open_readers(
            temporary_directory,
            canonical_primary_path,
            &reader_paths,
            identity,
            network_upgrade_activations,
        )
    }

    fn open_readers(
        temporary_directory: Arc<TempDir>,
        canonical_primary_path: &Path,
        reader_paths: &ReaderPaths,
        identity: CanonicalServingIdentity,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> eyre::Result<Self> {
        ensure!(
            identity.network == network_upgrade_activations.network(),
            "canonical primary and network-upgrade activations must use the same network"
        );
        let canonical_reader = RocksDbCanonicalSecondary::open_ready(
            canonical_primary_path,
            &reader_paths.canonical_secondary,
            network_upgrade_activations,
            CanonicalStoreWorkload::Wallet,
            identity.raw_blob_retention,
            identity.reorg_policy,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let wallet_reader = RocksDbWalletSecondary::open_ready(
            &reader_paths.wallet_primary,
            &reader_paths.wallet_secondary,
            identity.network,
            RocksDbResourceBudget::for_local_tests(),
        )?;

        Ok(Self {
            canonical_reader: Some(canonical_reader),
            wallet_reader: Some(wallet_reader),
            _temporary_directory: temporary_directory,
        })
    }

    /// Takes the admitted readers while retaining their temporary primary paths.
    ///
    /// # Errors
    ///
    /// Returns an error if the readers were already taken.
    pub fn take_readers(
        &mut self,
    ) -> eyre::Result<(RocksDbCanonicalSecondary, RocksDbWalletSecondary)> {
        let canonical_reader = self
            .canonical_reader
            .take()
            .ok_or_else(|| eyre!("wallet-serving canonical reader was already taken"))?;
        let wallet_reader = self
            .wallet_reader
            .take()
            .ok_or_else(|| eyre!("wallet-serving wallet reader was already taken"))?;
        Ok((canonical_reader, wallet_reader))
    }
}

/// Wallet primary and reader paths for one admission.
struct ReaderPaths {
    wallet_primary: std::path::PathBuf,
    canonical_secondary: std::path::PathBuf,
    wallet_secondary: std::path::PathBuf,
}

impl ReaderPaths {
    fn new(root: &Path, suffix: &str) -> Self {
        Self {
            wallet_primary: root.join(format!("wallet-primary{suffix}")),
            canonical_secondary: root.join(format!("canonical-secondary{suffix}")),
            wallet_secondary: root.join(format!("wallet-secondary{suffix}")),
        }
    }
}

/// Immutable canonical identity a reader must be admitted against.
#[derive(Clone, Copy)]
struct CanonicalServingIdentity {
    network: zinder_core::Network,
    raw_blob_retention: RawBlobRetention,
    reorg_policy: CanonicalReorgPolicy,
}

/// Builds and closes the wallet primary at the canonical primary's fence.
fn build_wallet_primary(
    canonical_primary: &RocksDbCanonicalStore,
    reader_paths: &ReaderPaths,
) -> eyre::Result<CanonicalServingIdentity> {
    let reorg_policy = canonical_primary.reorg_policy();
    let wallet_outcome = build_wallet_from_canonical(
        canonical_primary,
        &reader_paths.wallet_primary,
        RocksDbWalletBuildOptions {
            supported_reorg_depth: reorg_policy.reorg_window_blocks(),
            ..RocksDbWalletBuildOptions::for_local_tests()
        },
    )?;
    drop(wallet_outcome.store);
    Ok(CanonicalServingIdentity {
        network: canonical_primary.network(),
        raw_blob_retention: canonical_primary.raw_blob_retention(),
        reorg_policy,
    })
}
