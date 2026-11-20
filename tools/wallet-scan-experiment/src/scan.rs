//! Comparative compact note scanning with public, deterministic fixture keys.

#[path = "fixture.rs"]
mod fixture;

use blake2b_simd::{Hash, Params};
use chacha20::{
    ChaCha20, KeyIvInit as _,
    cipher::{StreamCipher as _, StreamCipherSeek as _},
};
use eyre::{OptionExt as _, Result, ensure};
use orchard::{
    Address, Note,
    keys::{FullViewingKey, PreparedIncomingViewingKey, Scope},
    note::{ExtractedNoteCommitment, Nullifier},
    note_encryption::{
        CompactAction, DomainVersion, IronwoodVersion, NoteEncryptionDomain, OrchardVersion,
    },
};
use zcash_note_encryption::{EphemeralKeyBytes, batch, try_compact_note_decryption};

use crate::{
    corpus::{Pool, ScanAction},
    shifted::{HintedBatch, HintedKey},
};

#[derive(Clone, Copy, Debug)]
pub(crate) enum Arm {
    Stock,
    OrdinaryPrefilter,
    Hinted,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Receipt {
    position: usize,
    pool: Pool,
    key_index: usize,
    amount: u64,
    address: [u8; 43],
    commitment: [u8; 32],
}

impl Receipt {
    fn from_note(
        position: usize,
        pool: Pool,
        key_index: usize,
        note: Note,
        address: Address,
    ) -> Self {
        Self {
            position,
            pool,
            key_index,
            amount: note.value().inner(),
            address: address.to_raw_address_bytes(),
            commitment: ExtractedNoteCommitment::from(note.commitment()).to_bytes(),
        }
    }
}

pub(crate) fn receipt_digest(receipts: &[Receipt]) -> Result<String> {
    let mut hash = Params::new().hash_length(32).to_state();
    for receipt in receipts {
        let pool = match receipt.pool {
            Pool::Orchard => b'O',
            Pool::Ironwood => b'I',
        };
        hash.update(&u64::try_from(receipt.position)?.to_le_bytes())
            .update(&[pool])
            .update(&u64::try_from(receipt.key_index)?.to_le_bytes())
            .update(&receipt.amount.to_le_bytes())
            .update(&receipt.address)
            .update(&receipt.commitment);
    }
    Ok(hash.finalize().to_hex().to_string())
}

struct Lane {
    position: usize,
    epk: [u8; 32],
    hint: Option<[u8; 32]>,
    first_byte: u8,
}

struct PoolInputs<V: DomainVersion> {
    outputs: Vec<(NoteEncryptionDomain<V>, CompactAction)>,
    lanes: Vec<Lane>,
}

impl<V: DomainVersion> PoolInputs<V> {
    fn new() -> Self {
        Self {
            outputs: Vec::new(),
            lanes: Vec::new(),
        }
    }

    fn push(&mut self, position: usize, action: &ScanAction) -> Result<()> {
        let nullifier = Option::<Nullifier>::from(Nullifier::from_bytes(&action.nullifier))
            .ok_or_eyre("noncanonical action nullifier")?;
        let cmx = Option::<ExtractedNoteCommitment>::from(ExtractedNoteCommitment::from_bytes(
            &action.cmx,
        ))
        .ok_or_eyre("noncanonical action commitment")?;
        let compact = CompactAction::from_parts(
            nullifier,
            cmx,
            EphemeralKeyBytes(action.epk),
            action.ciphertext,
        );
        self.outputs.push((
            NoteEncryptionDomain::<V>::for_compact_action(&compact),
            compact,
        ));
        self.lanes.push(Lane {
            position,
            epk: action.epk,
            hint: action.hint,
            first_byte: action.ciphertext[0],
        });
        Ok(())
    }
}

pub(crate) struct ScanInputs {
    orchard: PoolInputs<OrchardVersion>,
    ironwood: PoolInputs<IronwoodVersion>,
}

impl ScanInputs {
    pub(crate) fn new(actions: &[ScanAction]) -> Result<Self> {
        let mut inputs = Self {
            orchard: PoolInputs::new(),
            ironwood: PoolInputs::new(),
        };
        for (position, action) in actions.iter().enumerate() {
            match action.pool {
                Pool::Orchard => inputs.orchard.push(position, action)?,
                Pool::Ironwood => inputs.ironwood.push(position, action)?,
            }
        }
        Ok(inputs)
    }
}

pub(crate) struct Scanner {
    account: FullViewingKey,
    ordinary: Vec<PreparedIncomingViewingKey>,
    hinted: Vec<HintedKey>,
}

impl Scanner {
    pub(crate) fn new() -> Result<Self> {
        let account = fixture::wallet()?;
        let keys = [
            account.to_ivk(Scope::External),
            account.to_ivk(Scope::Internal),
        ];
        let hinted = keys
            .iter()
            .map(HintedKey::new)
            .collect::<Option<Vec<_>>>()
            .ok_or_eyre("invalid fixture viewing-key scalar")?;
        Ok(Self {
            account,
            ordinary: keys
                .iter()
                .map(orchard::keys::IncomingViewingKey::prepare)
                .collect(),
            hinted,
        })
    }

    pub(crate) fn scan(
        &self,
        inputs: &ScanInputs,
        arm: Arm,
        batch_size: usize,
    ) -> Result<Vec<Receipt>> {
        ensure!(batch_size > 0, "scan batch size must be positive");
        let mut receipts = self.scan_pool(&inputs.orchard, Pool::Orchard, arm, batch_size);
        receipts.extend(self.scan_pool(&inputs.ironwood, Pool::Ironwood, arm, batch_size));
        receipts.sort_unstable_by_key(|r| r.position);
        Ok(receipts)
    }

    fn scan_pool<V: DomainVersion>(
        &self,
        inputs: &PoolInputs<V>,
        pool: Pool,
        arm: Arm,
        batch_size: usize,
    ) -> Vec<Receipt> {
        let mut receipts = Vec::new();
        for (outputs, lanes) in inputs
            .outputs
            .chunks(batch_size)
            .zip(inputs.lanes.chunks(batch_size))
        {
            if matches!(arm, Arm::Stock) {
                let found = batch::try_compact_note_decryption(&self.ordinary, outputs);
                receipts.extend(found.into_iter().zip(lanes).filter_map(|(found, lane)| {
                    let ((note, address), key_index) = found?;
                    Some(Receipt::from_note(
                        lane.position,
                        pool,
                        key_index,
                        note,
                        address,
                    ))
                }));
                continue;
            }

            let keys = if matches!(arm, Arm::Hinted) {
                HintedBatch::new(lanes.iter().map(|lane| (lane.epk, lane.hint)))
            } else {
                HintedBatch::ordinary(lanes.iter().map(|lane| lane.epk))
            };
            let columns = self.symmetric_keys(&keys, lanes);
            let version = match pool {
                Pool::Orchard => 2,
                Pool::Ironwood => 3,
            };
            for (index, ((domain, output), lane)) in outputs.iter().zip(lanes).enumerate() {
                let receipt = columns.iter().enumerate().find_map(|(key_index, column)| {
                    let key = column[index].as_ref()?;
                    if !matches_note_version(key, lane.first_byte, version) {
                        return None;
                    }
                    // The stock scanner independently checks the note commitment and original EPK.
                    let (note, address) =
                        try_compact_note_decryption(domain, &self.ordinary[key_index], output)?;
                    Some(Receipt::from_note(
                        lane.position,
                        pool,
                        key_index,
                        note,
                        address,
                    ))
                });
                receipts.extend(receipt);
            }
        }
        receipts
    }

    fn symmetric_keys(&self, batch: &HintedBatch, lanes: &[Lane]) -> Vec<Vec<Option<Hash>>> {
        self.hinted
            .iter()
            .map(|ivk| {
                batch
                    .shared_secrets(ivk)
                    .into_iter()
                    .zip(lanes)
                    .map(|(secret, lane)| {
                        secret.map(|bytes| {
                            Params::new()
                                .hash_length(32)
                                .personal(b"Zcash_OrchardKDF")
                                .to_state()
                                .update(&bytes)
                                .update(&lane.epk)
                                .finalize()
                        })
                    })
                    .collect()
            })
            .collect()
    }
}

fn matches_note_version(key: &Hash, encrypted: u8, version: u8) -> bool {
    let Ok(mut cipher) = ChaCha20::new_from_slices(key.as_bytes(), &[0u8; 12]) else {
        return false;
    };
    let mut prefix = [encrypted];
    cipher.seek(64);
    cipher.apply_keystream(&mut prefix);
    prefix[0] == version
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receipt_digest_uses_fixed_width_fields_in_input_order() -> Result<()> {
        let mut receipts = [
            Receipt {
                position: 258,
                pool: Pool::Orchard,
                key_index: 0,
                amount: 65_537,
                address: [1; 43],
                commitment: [2; 32],
            },
            Receipt {
                position: 512,
                pool: Pool::Ironwood,
                key_index: 1,
                amount: 257,
                address: [3; 43],
                commitment: [4; 32],
            },
        ];
        let digest = receipt_digest(&receipts)?;
        assert_eq!(
            digest,
            "60f013c9361fce38d769d2b862093e330b8c39f049664af87238e88ccb2b6c44"
        );
        receipts.reverse();
        assert_ne!(receipt_digest(&receipts)?, digest);
        Ok(())
    }
}
