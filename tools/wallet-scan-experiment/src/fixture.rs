use eyre::{OptionExt as _, Result, ensure};
use orchard::{
    Address, Note,
    keys::{FullViewingKey, Scope, SpendingKey},
    note::{ExtractedNoteCommitment, RandomSeed, Rho},
    note_encryption::{DomainVersion, IronwoodVersion, NoteEncryptionDomain, OrchardVersion},
    value::NoteValue,
};
use serde::Serialize;
use zcash_note_encryption::{Domain as _, NoteEncryption};

use super::{Arm, Receipt, ScanInputs, Scanner};
use crate::corpus::{Pool, ScanAction, shifted_ephemeral_key};

pub(super) fn wallet() -> Result<FullViewingKey> {
    account(7)
}

fn account(fixture_byte: u8) -> Result<FullViewingKey> {
    let sk = Option::<SpendingKey>::from(SpendingKey::from_bytes([fixture_byte; 32]))
        .ok_or_eyre("invalid deterministic fixture account")?;
    Ok(FullViewingKey::from(&sk))
}

fn payment<V: DomainVersion>(
    pool: Pool,
    nonce: u8,
    address: Address,
) -> Result<(ScanAction, Note)> {
    let mut nullifier = [0; 32];
    nullifier[0] = nonce;
    let rho = Option::<Rho>::from(Rho::from_bytes(&nullifier)).ok_or_eyre("invalid fixture rho")?;
    let rseed = Option::<RandomSeed>::from(RandomSeed::from_bytes([nonce; 32], &rho))
        .ok_or_eyre("invalid fixture rseed")?;
    let note = Option::<Note>::from(Note::from_parts(
        address,
        NoteValue::from_raw(10_000 + u64::from(nonce)),
        rho,
        rseed,
        V::NOTE_VERSION,
    ))
    .ok_or_eyre("invalid fixture note")?;
    let encryption = NoteEncryption::<NoteEncryptionDomain<V>>::new(None, note, [0; 512]);
    let epk = NoteEncryptionDomain::<V>::epk_bytes(encryption.epk()).0;
    let encrypted = encryption.encrypt_note_plaintext();
    let ciphertext = encrypted.as_ref()[..52].try_into()?;
    Ok((
        ScanAction {
            pool,
            nullifier,
            cmx: ExtractedNoteCommitment::from(note.commitment()).to_bytes(),
            epk,
            ciphertext,
            hint: Some(shifted_ephemeral_key(&epk).ok_or_eyre("invalid generated EPK")?),
        },
        note,
    ))
}

#[derive(Debug, Serialize)]
pub(crate) struct FixtureReport {
    pub(crate) fixture_actions: usize,
    pub(crate) orchard_known_receipts: usize,
    pub(crate) ironwood_known_receipts: usize,
    pub(crate) foreign_notes: usize,
    pub(crate) receipts_without_hints: usize,
    pub(crate) receipts_with_malformed_points: usize,
    pub(crate) receipts_with_one_wrong_point: usize,
}

impl Scanner {
    pub(crate) fn append_fixture(&self, actions: &mut Vec<ScanAction>) -> Result<Vec<Receipt>> {
        let foreign = account(11)?;
        let mut expected = Vec::new();
        let mut nonce = 1;
        for pool in [Pool::Orchard, Pool::Ironwood] {
            for (address, key_index) in [
                (self.account.address_at(0u32, Scope::External), Some(0)),
                (self.account.address_at(0u32, Scope::Internal), Some(1)),
                (foreign.address_at(0u32, Scope::External), None),
            ] {
                let (action, note) = match pool {
                    Pool::Orchard => payment::<OrchardVersion>(pool, nonce, address)?,
                    Pool::Ironwood => payment::<IronwoodVersion>(pool, nonce, address)?,
                };
                if let Some(key_index) = key_index {
                    expected.push(Receipt::from_note(
                        actions.len(),
                        pool,
                        key_index,
                        note,
                        address,
                    ));
                }
                actions.push(action);
                nonce += 1;
            }
        }
        Ok(expected)
    }

    pub(crate) fn validate_fixture(&self) -> Result<FixtureReport> {
        let mut actions = Vec::new();
        let expected = self.append_fixture(&mut actions)?;
        let scan = |actions: &[ScanAction], arm| self.scan(&ScanInputs::new(actions)?, arm, 2);
        for arm in [Arm::Stock, Arm::OrdinaryPrefilter, Arm::Hinted] {
            ensure!(
                scan(&actions, arm)? == expected,
                "{arm:?} did not recover the expected notes"
            );
        }
        let wrong_point = actions[1].hint.ok_or_eyre("missing fixture hint")?;
        let mut wrong = actions.clone();
        wrong[0].hint = Some(wrong_point);
        let with_wrong = scan(&wrong, Arm::Hinted)?;
        ensure!(
            with_wrong == expected[1..],
            "incorrect hint did not reproduce the expected missed note"
        );
        ensure!(
            scan(&wrong, Arm::Stock)? == expected,
            "incorrect hint altered stock scan"
        );

        let mut negative = actions.clone();
        negative[0].cmx = negative[2].cmx;
        negative[1].ciphertext[0] ^= 0x80;
        negative[3].pool = Pool::Orchard;
        for arm in [Arm::Stock, Arm::OrdinaryPrefilter, Arm::Hinted] {
            ensure!(
                scan(&negative, arm)? == expected[3..],
                "{arm:?} accepted an invalid commitment, ciphertext, or domain"
            );
        }
        for action in &mut actions {
            action.hint = None;
        }
        let without = scan(&actions, Arm::Hinted)?;
        ensure!(without == expected, "missing hint lost a receipt");
        for action in &mut actions {
            action.hint = Some([0xff; 32]);
        }
        let malformed = scan(&actions, Arm::Hinted)?;
        ensure!(
            malformed == expected,
            "invalid point encoding lost a receipt"
        );
        Ok(FixtureReport {
            fixture_actions: actions.len(),
            orchard_known_receipts: expected.iter().filter(|r| r.pool == Pool::Orchard).count(),
            ironwood_known_receipts: expected.iter().filter(|r| r.pool == Pool::Ironwood).count(),
            foreign_notes: actions.len() - expected.len(),
            receipts_without_hints: without.len(),
            receipts_with_malformed_points: malformed.len(),
            receipts_with_one_wrong_point: with_wrong.len(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scanner_validates_the_known_receipts_and_fallbacks() -> Result<()> {
        let scanner = Scanner::new()?;
        let report = scanner.validate_fixture()?;
        assert_eq!(
            (
                report.orchard_known_receipts,
                report.ironwood_known_receipts
            ),
            (2, 2)
        );
        assert_eq!(report.receipts_with_one_wrong_point, 3);
        Ok(())
    }
}
