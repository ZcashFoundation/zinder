//! Process-local secondary access to an admitted wallet projection.

use std::{
    fs,
    num::NonZeroU16,
    path::{Path, PathBuf},
};

use rust_rocksdb::{DB, Options};
use zinder_core::{
    BlockHeight, BlockHeightRange, Network, TransparentAddressScriptHash, TransparentOutPoint,
};
use zinder_store::{RocksDbIoMode, RocksDbOpenRole, RocksDbResourceBudget, open_bounded_rocksdb};
use zinder_wallet_projection::{
    WalletAddressTransaction, WalletAddressTransactionKey, WalletAddressUnspentOutputKey,
    WalletCanonicalSourceIdentity, WalletProjectionBuildState, WalletProjectionReadyEvidence,
    WalletReorgUndo, WalletSpentOutput, WalletUnspentOutput, WalletUtxoSetSummary,
};

use crate::{
    RocksDbWalletError, RocksDbWalletStore, WalletAddressTransactionHistoryPage,
    WalletAddressUnspentOutputsPage,
    store::{
        decode_only_control, required_column_family_names, validate_resource_budget,
        wallet_column_family_descriptors,
    },
};

/// Result of one explicit wallet-secondary catch-up barrier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalletSecondaryCatchupOutcome {
    before: WalletCanonicalSourceIdentity,
    after: WalletCanonicalSourceIdentity,
}

impl WalletSecondaryCatchupOutcome {
    /// Returns the authenticated wallet source visible before catch-up.
    #[must_use]
    pub const fn before(self) -> WalletCanonicalSourceIdentity {
        self.before
    }

    /// Returns the authenticated wallet source visible after catch-up.
    #[must_use]
    pub const fn after(self) -> WalletCanonicalSourceIdentity {
        self.after
    }

    /// Reports whether catch-up advanced the durable event sequence.
    #[must_use]
    pub const fn advanced(self) -> bool {
        self.before.source_position().event_sequence < self.after.source_position().event_sequence
    }
}

/// One admitted read-only `RocksDB` secondary for a wallet-projection primary.
///
/// The reader owns a process-local secondary metadata path and exposes only
/// typed query reads. It never opens the wallet primary as a database handle,
/// writes a row, or follows a primary while it is held by a serving pair.
/// Callers catch up only inactive candidates, validate their exact canonical
/// source separately, and publish the immutable reader behind a new pair.
pub struct RocksDbWalletSecondary {
    store: RocksDbWalletStore,
    primary_path: PathBuf,
    expected_network: Network,
}

impl RocksDbWalletSecondary {
    /// Opens one READY wallet secondary and catches it up to the primary's
    /// current publication without opening the primary for reads or writes.
    pub fn open_ready(
        primary_path: impl AsRef<Path>,
        secondary_path: impl AsRef<Path>,
        expected_network: Network,
        resource_budget: RocksDbResourceBudget,
    ) -> Result<Self, RocksDbWalletError> {
        validate_resource_budget(resource_budget)?;
        let primary_path = fs::canonicalize(primary_path.as_ref()).map_err(|source| {
            RocksDbWalletError::PathUnavailable {
                path: primary_path.as_ref().to_path_buf(),
                source,
            }
        })?;
        let secondary_path = secondary_path.as_ref();
        if secondary_path == primary_path.as_path() {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "secondary metadata path must differ from the wallet primary path",
            });
        }
        require_exact_primary_column_family_metadata(&primary_path)?;
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::Secondary {
                primary_path: &primary_path,
                secondary_path,
            },
            resource_budget,
            wallet_column_family_descriptors,
        )
        .map_err(|source| RocksDbWalletError::rocksdb("open wallet secondary", source))?;
        bounded_open
            .db
            .try_catch_up_with_primary()
            .map_err(|source| {
                RocksDbWalletError::rocksdb("initial wallet secondary catch-up", source)
            })?;
        require_exact_secondary_column_families(&bounded_open.db)?;
        let control = decode_only_control(&bounded_open)?;
        let ready_evidence =
            ready_evidence_from_control(&control, &primary_path, expected_network)?;
        Ok(Self {
            store: RocksDbWalletStore {
                bounded_open,
                control,
                ready_evidence,
            },
            primary_path,
            expected_network,
        })
    }

    /// Catches up this inactive secondary and authenticates its new READY
    /// control. A source regression or same-sequence replacement is refused.
    pub fn try_catch_up(&mut self) -> Result<WalletSecondaryCatchupOutcome, RocksDbWalletError> {
        let before =
            WalletCanonicalSourceIdentity::from_ready_evidence(self.store.ready_evidence());
        self.store
            .bounded_open
            .db
            .try_catch_up_with_primary()
            .map_err(|source| RocksDbWalletError::rocksdb("wallet secondary catch-up", source))?;
        require_exact_secondary_column_families(&self.store.bounded_open.db)?;
        let control = decode_only_control(&self.store.bounded_open)?;
        let ready_evidence =
            ready_evidence_from_control(&control, &self.primary_path, self.expected_network)?;
        // The projector raises the depth in place when the canonical window
        // is raised; it never lowers it.
        if control.supported_reorg_depth < self.store.control.supported_reorg_depth {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "wallet supported reorg depth regressed during secondary catch-up",
            });
        }
        let after = WalletCanonicalSourceIdentity::from_ready_evidence(&ready_evidence);
        let before_sequence = before.source_position().event_sequence;
        let after_sequence = after.source_position().event_sequence;
        if after_sequence < before_sequence {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "wallet source event sequence regressed during secondary catch-up",
            });
        }
        if after_sequence == before_sequence
            && (after != before || ready_evidence != *self.store.ready_evidence())
        {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "wallet READY publication changed without advancing its event sequence",
            });
        }
        self.store.control = control;
        self.store.ready_evidence = ready_evidence;
        Ok(WalletSecondaryCatchupOutcome { before, after })
    }

    /// Returns the decoded READY evidence admitted from the secondary.
    #[must_use]
    pub const fn ready_evidence(&self) -> &WalletProjectionReadyEvidence {
        self.store.ready_evidence()
    }

    /// Returns the wallet network admitted from the secondary control record.
    #[must_use]
    pub const fn network(&self) -> Network {
        self.store.network()
    }

    /// Returns one current output by exact outpoint.
    pub fn find_unspent_output(
        &self,
        outpoint: TransparentOutPoint,
    ) -> Result<Option<WalletUnspentOutput>, RocksDbWalletError> {
        self.store.find_unspent_output(outpoint)
    }

    /// Returns one historical spent output by exact outpoint.
    pub fn find_spent_output(
        &self,
        outpoint: TransparentOutPoint,
    ) -> Result<Option<WalletSpentOutput>, RocksDbWalletError> {
        self.store.find_spent_output(outpoint)
    }

    /// Resolves one exact address-ordered unspent-output index key.
    pub fn find_unspent_output_by_address_key(
        &self,
        key: WalletAddressUnspentOutputKey,
    ) -> Result<Option<WalletUnspentOutput>, RocksDbWalletError> {
        self.store.find_unspent_output_by_address_key(key)
    }

    /// Returns one bounded page of current outputs for an address.
    pub fn address_unspent_outputs_page(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        after: Option<WalletAddressUnspentOutputKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressUnspentOutputsPage, RocksDbWalletError> {
        self.store
            .address_unspent_outputs_page(address_script_hash, after, page_size)
    }

    /// Returns one bounded page of current outputs for an address at or above
    /// `start_height`.
    pub fn address_unspent_outputs_page_from_height(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        start_height: BlockHeight,
        after: Option<WalletAddressUnspentOutputKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressUnspentOutputsPage, RocksDbWalletError> {
        self.store.address_unspent_outputs_page_from_height(
            address_script_hash,
            start_height,
            after,
            page_size,
        )
    }

    /// Returns one exact address-transaction row.
    pub fn find_address_transaction(
        &self,
        key: WalletAddressTransactionKey,
    ) -> Result<Option<WalletAddressTransaction>, RocksDbWalletError> {
        self.store.find_address_transaction(key)
    }

    /// Returns one bounded page of transaction history for an address.
    pub fn address_transaction_history_page(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        after: Option<WalletAddressTransactionKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressTransactionHistoryPage, RocksDbWalletError> {
        self.store
            .address_transaction_history_page(address_script_hash, after, page_size)
    }

    /// Returns one bounded page of address-touching transactions within an
    /// inclusive height range.
    pub fn address_transaction_history_range_page(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        height_range: BlockHeightRange,
        after: Option<WalletAddressTransactionKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressTransactionHistoryPage, RocksDbWalletError> {
        self.store.address_transaction_history_range_page(
            address_script_hash,
            height_range,
            after,
            page_size,
        )
    }

    /// Returns one retained reorg-undo record by block height.
    pub fn find_reorg_undo(
        &self,
        block_height: BlockHeight,
    ) -> Result<Option<WalletReorgUndo>, RocksDbWalletError> {
        self.store.find_reorg_undo(block_height)
    }

    /// Returns one address's current balance, with an absent row represented as zero.
    pub fn address_balance(
        &self,
        address_script_hash: TransparentAddressScriptHash,
    ) -> Result<u64, RocksDbWalletError> {
        self.store.address_balance(address_script_hash)
    }

    /// Returns the complete current UTXO aggregate committed by READY.
    #[must_use]
    pub const fn utxo_summary(&self) -> &WalletUtxoSetSummary {
        self.store.utxo_summary()
    }

    /// Returns the filesystem I/O mode selected for this secondary.
    #[must_use]
    pub const fn io_mode(&self) -> RocksDbIoMode {
        self.store.io_mode()
    }
}

fn ready_evidence_from_control(
    control: &zinder_wallet_projection::WalletStoreControlRecord,
    primary_path: &Path,
    expected_network: Network,
) -> Result<WalletProjectionReadyEvidence, RocksDbWalletError> {
    if control.network != expected_network {
        return Err(RocksDbWalletError::NetworkMismatch {
            expected: expected_network,
            observed: control.network,
        });
    }
    let WalletProjectionBuildState::Ready(ready_evidence) = &control.build_state else {
        return Err(RocksDbWalletError::StoreNotReady {
            path: primary_path.to_path_buf(),
        });
    };
    Ok(ready_evidence.clone())
}

fn require_exact_primary_column_family_metadata(path: &Path) -> Result<(), RocksDbWalletError> {
    let observed = DB::list_cf(&Options::default(), path).map_err(|source| {
        RocksDbWalletError::rocksdb("wallet primary column-family metadata", source)
    })?;
    require_exact_column_family_names(observed)
}

fn require_exact_secondary_column_families(db: &DB) -> Result<(), RocksDbWalletError> {
    require_exact_column_family_names(db.cf_names())
}

fn require_exact_column_family_names(mut observed: Vec<String>) -> Result<(), RocksDbWalletError> {
    let mut expected = required_column_family_names();
    expected.sort_unstable();
    observed.sort_unstable();
    if expected != observed {
        return Err(RocksDbWalletError::ColumnFamilyContractMismatch { expected, observed });
    }
    Ok(())
}
