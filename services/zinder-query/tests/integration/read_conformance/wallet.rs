//! Conformance cases for [`WalletProjectionReader`].

use std::num::NonZeroU16;

use eyre::{ensure, eyre};
use zinder_core::{
    BlockHeight, BlockHeightRange, BlockId, Network, TransactionId, TransparentAddressScriptHash,
    TransparentOutPoint, TransparentOutpointSpendStatus, TransparentSpendFact,
    TransparentUnspentOutput,
};
use zinder_query::{CanonicalReader, SpendStatusReadEnd, WalletProjectionReader};
use zinder_store::RawBlobRetention;
use zinder_testkit::{
    CanonicalBaseline, ChainFixture, FixtureBlock, FixtureTransactionRows,
    sample_regtest_upgrade_activations,
};
use zinder_wallet_projection::{
    WalletAddressTransactionKey, WalletAddressUnspentOutputKey, WalletCanonicalSourceIdentity,
};

use super::WalletReaderHarness;

/// Address that holds every output and spend of the wallet chain.
const SCRIPT_PUB_KEY: &[u8] = &[0x51];
/// Address that holds the wallet chain's single output of another owner.
const OTHER_SCRIPT_PUB_KEY: &[u8] = &[0x52];
/// Value of every output the wallet chain creates.
const OUTPUT_ZAT: u64 = 1_000;
/// Outputs block 1 creates for the address, one per transaction.
const BLOCK_ONE_OUTPUTS: u32 = 4;
/// Outputs block 2 creates for the address, one per transaction.
const BLOCK_TWO_OUTPUTS: u32 = 3;

pub(crate) async fn wallet_source_matches_the_canonical_fence<H: WalletReaderHarness>()
-> eyre::Result<()> {
    let wallet_chain = wallet_chain()?;
    let mut harness = published::<H>(&wallet_chain.chain)?;
    let canonical = harness.canonical_reader()?;
    let wallet = harness.wallet_reader()?;

    assert_eq!(wallet.network(), Network::ZcashRegtest);
    let source = WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
    let position = source.source_position();
    let fence = canonical.event_fence();
    assert_eq!(position.chain_epoch_id, fence.chain_epoch_id());
    assert_eq!(position.tip, fence.visible_tip());
    assert_eq!(position.event_sequence, fence.chain_event_sequence());
    assert_eq!(source.source_sequence_digest(), fence.sequence_digest());
    let chain_epoch = canonical.chain_epoch();
    assert_eq!(
        source.settled_tip(),
        BlockId::new(chain_epoch.settled_tip_height, chain_epoch.settled_tip_hash)
    );
    Ok(())
}

pub(crate) async fn unspent_output_pages_are_bounded_and_resume<H: WalletReaderHarness>()
-> eyre::Result<()> {
    let wallet_chain = wallet_chain()?;
    let mut harness = published::<H>(&wallet_chain.chain)?;
    let wallet = harness.wallet_reader()?;
    let address = address();

    let expected = wallet_chain.unspent_outpoints();
    for page_size in [1, 2, 3, usize_of(BLOCK_ONE_OUTPUTS + BLOCK_TWO_OUTPUTS)] {
        let outputs = all_unspent_outputs(&wallet, address, BlockHeight::new(0), page_size).await?;
        assert_eq!(
            outputs
                .iter()
                .map(|output| output.outpoint)
                .collect::<Vec<_>>(),
            expected,
            "page size {page_size}"
        );
        for output in &outputs {
            assert_eq!(output.address_script_hash, address);
            assert_eq!(output.script_pub_key, SCRIPT_PUB_KEY);
            assert_eq!(output.value_zat, OUTPUT_ZAT);
        }
    }

    let unknown = TransparentAddressScriptHash::of_script_pub_key(&[0x53]);
    let page = wallet
        .address_unspent_outputs_page_from_height(
            unknown,
            BlockHeight::new(0),
            None,
            NonZeroU16::MAX,
        )
        .await?;
    ensure!(
        page.outputs.is_empty(),
        "an unknown address must hold no output"
    );
    assert_eq!(page.next_page_after, None);
    Ok(())
}

pub(crate) async fn unspent_output_pages_start_at_their_height<H: WalletReaderHarness>()
-> eyre::Result<()> {
    let wallet_chain = wallet_chain()?;
    let mut harness = published::<H>(&wallet_chain.chain)?;
    let wallet = harness.wallet_reader()?;

    let from_block_two = all_unspent_outputs(&wallet, address(), BlockHeight::new(2), 2).await?;
    assert_eq!(
        from_block_two
            .iter()
            .map(|output| output.outpoint)
            .collect::<Vec<_>>(),
        wallet_chain
            .unspent_outpoints()
            .into_iter()
            .filter(|outpoint| wallet_chain.block_two_creations.contains(outpoint))
            .collect::<Vec<_>>()
    );
    for output in &from_block_two {
        assert_eq!(output.created_at.block, wallet_chain.block_two);
    }
    ensure!(
        all_unspent_outputs(&wallet, address(), BlockHeight::new(3), 2)
            .await?
            .is_empty(),
        "no output is created above the tip"
    );
    Ok(())
}

pub(crate) async fn transaction_history_pages_stay_in_their_range<H: WalletReaderHarness>()
-> eyre::Result<()> {
    let wallet_chain = wallet_chain()?;
    let mut harness = published::<H>(&wallet_chain.chain)?;
    let wallet = harness.wallet_reader()?;
    let address = address();

    let whole = range(1, 2);
    let mut history = Vec::new();
    for page_size in [1, 2, 64] {
        let transactions = all_history(&wallet, address, whole, page_size).await?;
        ensure!(
            transactions
                .windows(2)
                .all(|pair| pair[0].key < pair[1].key),
            "history must come in ascending key order"
        );
        if history.is_empty() {
            history = transactions;
        } else {
            assert_eq!(transactions, history, "page size {page_size}");
        }
    }
    for transaction in &history {
        assert_eq!(transaction.key.address_script_hash(), address);
    }
    let touched = history
        .iter()
        .map(|transaction| transaction.transaction_id)
        .collect::<Vec<_>>();
    for transaction_id in wallet_chain
        .block_one_creations
        .iter()
        .chain(&wallet_chain.block_two_creations)
        .map(|outpoint| outpoint.transaction_id)
        .chain([wallet_chain.spender])
    {
        assert!(
            touched.contains(&transaction_id),
            "the history must name {transaction_id:?}"
        );
    }

    let block_two_only = all_history(&wallet, address, range(2, 2), 1).await?;
    ensure!(!block_two_only.is_empty(), "block 2 must touch the address");
    assert!(
        block_two_only
            .iter()
            .all(
                |transaction| transaction.key.block_height() == BlockHeight::new(2)
                    && transaction.block_hash == wallet_chain.block_two.hash
            )
    );
    assert_eq!(
        block_two_only,
        history
            .iter()
            .filter(|transaction| transaction.key.block_height() == BlockHeight::new(2))
            .copied()
            .collect::<Vec<_>>()
    );
    ensure!(
        all_history(&wallet, address, range(3, 9), 2)
            .await?
            .is_empty(),
        "no transaction touches the address above the tip"
    );
    Ok(())
}

pub(crate) async fn address_balances_sum_and_count_an_absent_address_as_zero<
    H: WalletReaderHarness,
>() -> eyre::Result<()> {
    let wallet_chain = wallet_chain()?;
    let mut harness = published::<H>(&wallet_chain.chain)?;
    let wallet = harness.wallet_reader()?;
    let address = address();
    let other = TransparentAddressScriptHash::of_script_pub_key(OTHER_SCRIPT_PUB_KEY);
    let absent = TransparentAddressScriptHash::of_script_pub_key(&[0x53]);
    let address_balance = OUTPUT_ZAT * u64::try_from(wallet_chain.unspent_outpoints().len())?;

    assert_eq!(
        wallet.total_address_balance(vec![address]).await?,
        address_balance
    );
    assert_eq!(wallet.total_address_balance(vec![other]).await?, OUTPUT_ZAT);
    assert_eq!(wallet.total_address_balance(vec![absent]).await?, 0);
    assert_eq!(
        wallet
            .total_address_balance(vec![absent, other, address])
            .await?,
        address_balance + OUTPUT_ZAT
    );
    assert_eq!(
        wallet.total_address_balance(vec![other, other]).await?,
        2 * OUTPUT_ZAT,
        "the caller removes duplicates"
    );
    assert_eq!(wallet.total_address_balance(Vec::new()).await?, 0);
    Ok(())
}

pub(crate) async fn outpoint_spend_statuses_follow_the_request_order<H: WalletReaderHarness>()
-> eyre::Result<()> {
    let wallet_chain = wallet_chain()?;
    let mut harness = published::<H>(&wallet_chain.chain)?;
    let wallet = harness.wallet_reader()?;
    let unspent = wallet_chain.unspent_outpoints()[0];
    let never_created = TransparentOutPoint::new(TransactionId::from_bytes([0x99; 32]), 0);

    let requested = vec![
        never_created,
        wallet_chain.spent,
        unspent,
        wallet_chain.spent,
        never_created,
    ];
    let statuses = wallet
        .outpoint_spend_statuses(requested.clone(), SpendStatusReadEnd::AllOutpoints)
        .await?;
    assert_eq!(
        statuses,
        vec![
            TransparentOutpointSpendStatus::NotFound,
            wallet_chain.spent_status(),
            TransparentOutpointSpendStatus::Unspent,
            wallet_chain.spent_status(),
            TransparentOutpointSpendStatus::NotFound,
        ]
    );
    ensure!(
        wallet
            .outpoint_spend_statuses(Vec::new(), SpendStatusReadEnd::AllOutpoints)
            .await?
            .is_empty(),
        "no outpoint has no status"
    );

    // A read that ends at the first `NotFound` keeps every status before it
    // and reads nothing after it.
    assert_eq!(
        wallet
            .outpoint_spend_statuses(requested, SpendStatusReadEnd::FirstNotFound)
            .await?,
        vec![TransparentOutpointSpendStatus::NotFound]
    );
    assert_eq!(
        wallet
            .outpoint_spend_statuses(
                vec![unspent, wallet_chain.spent, never_created, unspent],
                SpendStatusReadEnd::FirstNotFound,
            )
            .await?,
        vec![
            TransparentOutpointSpendStatus::Unspent,
            wallet_chain.spent_status(),
            TransparentOutpointSpendStatus::NotFound,
        ]
    );
    assert_eq!(
        wallet
            .outpoint_spend_statuses(
                vec![unspent, wallet_chain.spent],
                SpendStatusReadEnd::FirstNotFound,
            )
            .await?,
        vec![
            TransparentOutpointSpendStatus::Unspent,
            wallet_chain.spent_status(),
        ]
    );
    Ok(())
}

pub(crate) async fn a_wallet_reader_keeps_its_source_while_the_projection_advances<
    H: WalletReaderHarness,
>() -> eyre::Result<()> {
    let wallet_chain = wallet_chain()?;
    let mut harness = H::publish(
        &CanonicalBaseline::new(&wallet_chain.chain).through(BlockHeight::new(1)),
        &sample_regtest_upgrade_activations(),
    )?;
    let before = harness.wallet_reader()?;
    let before_source = WalletCanonicalSourceIdentity::from_ready_evidence(before.ready_evidence());
    harness.append_live_block(&wallet_chain.chain, before_source.settled_tip())?;
    let after = harness.wallet_reader()?;

    assert_eq!(
        WalletCanonicalSourceIdentity::from_ready_evidence(before.ready_evidence()),
        before_source
    );
    assert_eq!(
        before
            .outpoint_spend_statuses(vec![wallet_chain.spent], SpendStatusReadEnd::AllOutpoints)
            .await?,
        vec![TransparentOutpointSpendStatus::Unspent]
    );
    assert_eq!(
        after
            .outpoint_spend_statuses(vec![wallet_chain.spent], SpendStatusReadEnd::AllOutpoints)
            .await?,
        vec![wallet_chain.spent_status()]
    );
    let block_one_balance = OUTPUT_ZAT * u64::from(BLOCK_ONE_OUTPUTS);
    assert_eq!(
        before.total_address_balance(vec![address()]).await?,
        block_one_balance
    );
    assert_eq!(
        after.total_address_balance(vec![address()]).await?,
        OUTPUT_ZAT * u64::try_from(wallet_chain.unspent_outpoints().len())?
    );
    assert!(
        all_unspent_outputs(&before, address(), BlockHeight::new(2), 8)
            .await?
            .is_empty(),
        "a reader serves no output past its source"
    );
    Ok(())
}

pub(crate) async fn a_reorg_reverts_a_spend_only_for_readers_opened_after_it<
    H: WalletReaderHarness,
>() -> eyre::Result<()> {
    let wallet_chain = wallet_chain()?;
    let spend_height = BlockHeight::new(2);
    let replacement_chain = wallet_chain.chain.fork_at(spend_height)?.extend_blocks(1);
    let mut harness = H::publish(
        &CanonicalBaseline::new(&wallet_chain.chain).with_settled_tip(BlockHeight::new(1)),
        &sample_regtest_upgrade_activations(),
    )?;
    let before = harness.wallet_reader()?;
    harness.replace_live_blocks(&replacement_chain, spend_height)?;
    let after = harness.wallet_reader()?;

    assert_eq!(
        before
            .outpoint_spend_statuses(vec![wallet_chain.spent], SpendStatusReadEnd::AllOutpoints)
            .await?,
        vec![wallet_chain.spent_status()]
    );
    assert_eq!(
        after
            .outpoint_spend_statuses(vec![wallet_chain.spent], SpendStatusReadEnd::AllOutpoints)
            .await?,
        vec![TransparentOutpointSpendStatus::Unspent]
    );
    let replaced_creation = wallet_chain
        .block_two_creations
        .first()
        .copied()
        .ok_or_else(|| eyre!("block 2 must create an output"))?;
    assert_eq!(
        after
            .outpoint_spend_statuses(vec![replaced_creation], SpendStatusReadEnd::AllOutpoints)
            .await?,
        vec![TransparentOutpointSpendStatus::NotFound]
    );
    assert_eq!(
        after.total_address_balance(vec![address()]).await?,
        OUTPUT_ZAT * u64::from(BLOCK_ONE_OUTPUTS)
    );
    assert!(
        all_history(&after, address(), range(2, 2), 8)
            .await?
            .is_empty(),
        "the replacement block touches no address"
    );
    Ok(())
}

/// A two-block chain whose outputs and spend the wallet cases read.
struct WalletChain {
    chain: ChainFixture,
    block_two: BlockId,
    /// Outputs block 1 creates for the address, in address-index order.
    block_one_creations: Vec<TransparentOutPoint>,
    /// Outputs block 2 creates for the address, in address-index order.
    block_two_creations: Vec<TransparentOutPoint>,
    /// The block-1 output block 2 spends.
    spent: TransparentOutPoint,
    /// The block-2 transaction that spends it, at input 0.
    spender: TransactionId,
}

impl WalletChain {
    /// The address's unspent outputs after block 2, in address-index order.
    fn unspent_outpoints(&self) -> Vec<TransparentOutPoint> {
        self.block_one_creations
            .iter()
            .chain(&self.block_two_creations)
            .copied()
            .filter(|outpoint| *outpoint != self.spent)
            .collect()
    }

    fn spent_status(&self) -> TransparentOutpointSpendStatus {
        TransparentOutpointSpendStatus::SpentBy {
            transaction_id: self.spender,
            input_index: 0,
            block: self.block_two,
        }
    }
}

/// Builds the wallet chain.
///
/// Block 1 creates [`BLOCK_ONE_OUTPUTS`] outputs for the address and one for
/// another owner. Block 2 starts with a coinbase, then a transaction that
/// spends one block-1 output and creates the first of [`BLOCK_TWO_OUTPUTS`]
/// outputs.
fn wallet_chain() -> eyre::Result<WalletChain> {
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::None)
        .extend_blocks(2);
    let block_one = fixture_block(&chain, 1)?;
    let block_two = fixture_block(&chain, 2)?;
    let address = address();
    let other = TransparentAddressScriptHash::of_script_pub_key(OTHER_SCRIPT_PUB_KEY);
    let mut chain = chain.with_transaction_rows(FixtureTransactionRows::coinbase(
        transaction_id(0x2c, 0),
        block_two.height,
        block_two.hash,
        b"wallet-conformance-coinbase".to_vec(),
    ));

    let mut block_one_creations = Vec::new();
    for index in 0..BLOCK_ONE_OUTPUTS {
        let creator = transaction_id(0x10, index);
        chain = chain
            .with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
                creator,
                block_one.height,
                block_one.hash,
                index,
                format!("wallet-conformance-block-one-{index}").into_bytes(),
            ))
            .with_address_output_index(output(address, SCRIPT_PUB_KEY, creator, &block_one));
        block_one_creations.push(TransparentOutPoint::new(creator, 0));
    }
    let other_creator = transaction_id(0x30, 0);
    chain = chain
        .with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
            other_creator,
            block_one.height,
            block_one.hash,
            BLOCK_ONE_OUTPUTS,
            b"wallet-conformance-other-owner".to_vec(),
        ))
        .with_address_output_index(output(
            other,
            OTHER_SCRIPT_PUB_KEY,
            other_creator,
            &block_one,
        ));

    let spent = block_one_creations[1];
    let mut block_two_creations = Vec::new();
    for index in 0..BLOCK_TWO_OUTPUTS {
        let creator = transaction_id(0x20, index);
        chain = chain
            .with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
                creator,
                block_two.height,
                block_two.hash,
                index + 1,
                format!("wallet-conformance-block-two-{index}").into_bytes(),
            ))
            .with_address_output_index(output(address, SCRIPT_PUB_KEY, creator, &block_two));
        block_two_creations.push(TransparentOutPoint::new(creator, 0));
    }
    let spender = transaction_id(0x20, 0);
    chain = chain.with_transparent_spend_fact(TransparentSpendFact::new(
        spent,
        0,
        spender,
        1,
        block_two.height,
        block_two.hash,
        OUTPUT_ZAT,
        address,
        block_one.height,
        block_one.hash,
    ));

    Ok(WalletChain {
        chain,
        block_two: BlockId::new(block_two.height, block_two.hash),
        block_one_creations,
        block_two_creations,
        spent,
        spender,
    })
}

/// Publishes `chain` complete and settled at its tip.
fn published<H: WalletReaderHarness>(chain: &ChainFixture) -> eyre::Result<H> {
    H::publish(
        &CanonicalBaseline::new(chain),
        &sample_regtest_upgrade_activations(),
    )
}

/// Reads every unspent output of `address` from `start_height` up,
/// `page_size` at a time, checking each page's bound and continuation.
async fn all_unspent_outputs(
    wallet: &impl WalletProjectionReader,
    address: TransparentAddressScriptHash,
    start_height: BlockHeight,
    page_size: usize,
) -> eyre::Result<Vec<zinder_wallet_projection::WalletUnspentOutput>> {
    let page_size = NonZeroU16::new(u16::try_from(page_size)?)
        .ok_or_else(|| eyre!("a page holds at least one output"))?;
    let mut outputs = Vec::new();
    let mut after: Option<WalletAddressUnspentOutputKey> = None;
    loop {
        let page = wallet
            .address_unspent_outputs_page_from_height(address, start_height, after, page_size)
            .await?;
        ensure!(
            page.outputs.len() <= usize::from(page_size.get()),
            "a page must hold at most {page_size} outputs"
        );
        outputs.extend(page.outputs);
        match page.next_page_after {
            Some(next) => {
                ensure!(after < Some(next), "a continuation must move forward");
                after = Some(next);
            }
            None => return Ok(outputs),
        }
    }
}

/// Reads every transaction of `address` in `height_range`, `page_size` at
/// a time, checking each page's bound and continuation.
async fn all_history(
    wallet: &impl WalletProjectionReader,
    address: TransparentAddressScriptHash,
    height_range: BlockHeightRange,
    page_size: usize,
) -> eyre::Result<Vec<zinder_wallet_projection::WalletAddressTransaction>> {
    let page_size = NonZeroU16::new(u16::try_from(page_size)?)
        .ok_or_else(|| eyre!("a page holds at least one transaction"))?;
    let mut transactions = Vec::new();
    let mut after: Option<WalletAddressTransactionKey> = None;
    loop {
        let page = wallet
            .address_transaction_history_range_page(address, height_range, after, page_size)
            .await?;
        ensure!(
            page.transactions.len() <= usize::from(page_size.get()),
            "a page must hold at most {page_size} transactions"
        );
        transactions.extend(page.transactions);
        match page.next_page_after {
            Some(next) => {
                ensure!(after < Some(next), "a continuation must move forward");
                after = Some(next);
            }
            None => return Ok(transactions),
        }
    }
}

fn address() -> TransparentAddressScriptHash {
    TransparentAddressScriptHash::of_script_pub_key(SCRIPT_PUB_KEY)
}

fn output(
    address: TransparentAddressScriptHash,
    script_pub_key: &[u8],
    creator: TransactionId,
    block: &FixtureBlock,
) -> TransparentUnspentOutput {
    TransparentUnspentOutput::new(
        address,
        script_pub_key.to_vec(),
        TransparentOutPoint::new(creator, 0),
        OUTPUT_ZAT,
        block.height,
        block.hash,
    )
}

fn transaction_id(prefix: u8, index: u32) -> TransactionId {
    let mut bytes = [prefix; 32];
    bytes[28..].copy_from_slice(&index.to_be_bytes());
    TransactionId::from_bytes(bytes)
}

fn fixture_block(chain: &ChainFixture, height: u32) -> eyre::Result<FixtureBlock> {
    chain
        .block_at(BlockHeight::new(height))
        .cloned()
        .ok_or_else(|| eyre!("the fixture has no block at height {height}"))
}

const fn range(start: u32, end: u32) -> BlockHeightRange {
    BlockHeightRange::inclusive(BlockHeight::new(start), BlockHeight::new(end))
}

fn usize_of(count: u32) -> usize {
    usize::try_from(count).unwrap_or(usize::MAX)
}
