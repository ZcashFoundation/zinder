use std::{collections::BTreeSet, sync::Arc};

use futures_util::stream;
use tonic::{Request, Response, Status, Streaming};
use zinder_core::{
    BlockHash,
    wire::{
        decode_internal_block_hash, decode_internal_transaction_id, decode_rpc_block_hash_bytes,
        decode_rpc_block_hash_hex, encode_rpc_transaction_id_hex,
    },
};
use zinder_proto::compat::lightwalletd::{
    Address, AddressList, Balance, BlockId, BlockRange, ChainMetadata, ChainSpec, CompactBlock,
    CompactTx, Duration as PingDuration, Empty, GetAddressUtxosArg, GetAddressUtxosReply,
    GetAddressUtxosReplyList, GetMempoolTxRequest, GetSubtreeRootsArg,
    LIGHTWALLETD_PROTOCOL_VERSION, LightdInfo, PingResponse, PoolType, RawTransaction,
    SendResponse, ShieldedProtocol, StandardFee, SubtreeRoot, TransparentAddressBlockFilter,
    TreeState, TxFilter, compact_tx_streamer_server::CompactTxStreamer,
};

use super::{
    Darkside, MAX_POOL_BYTES, Rpc, Scenario, StreamReply,
    chain::{FixtureBlock, FixtureTx},
    stream_reply,
};

const MAX_ADDRESSES: usize = 128;
const MAX_SUFFIXES: usize = 128;

fn tip(scenario: &Scenario) -> Result<&FixtureBlock, Status> {
    scenario
        .chain
        .visible()
        .last()
        .map(Arc::as_ref)
        .ok_or_else(|| Status::failed_precondition("no visible blocks"))
}

fn find_block<'scenario>(
    scenario: &'scenario Scenario,
    id: &BlockId,
    decode_hash: impl FnOnce(&[u8]) -> Result<BlockHash, zinder_core::wire::WireDecodeError>,
) -> Result<&'scenario FixtureBlock, Status> {
    let visible = scenario.chain.visible();
    let found = if id.hash.is_empty() {
        visible
            .first()
            .and_then(|first| id.height.checked_sub(first.height))
            .and_then(|offset| usize::try_from(offset).ok())
            .and_then(|index| visible.get(index))
    } else {
        let hash =
            decode_hash(&id.hash).map_err(|_| Status::invalid_argument("invalid block hash"))?;
        visible
            .iter()
            .find(|block| block.hash == hash && (id.height == 0 || block.height == id.height))
    };
    found
        .map(Arc::as_ref)
        .ok_or_else(|| Status::not_found("block is not visible"))
}

fn range_heights(range: &BlockRange) -> Result<(u64, u64), Status> {
    let start = range
        .start
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing range start"))?;
    let end = range
        .end
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing range end"))?;
    if !start.hash.is_empty() || !end.hash.is_empty() {
        return Err(Status::invalid_argument("range endpoints must be heights"));
    }
    Ok((start.height, end.height))
}

#[derive(Clone, Copy)]
struct ScanShape {
    pools: [bool; 5],
    nullifiers: bool,
    mempool: bool,
}

impl ScanShape {
    fn select(pool_types: &[i32], nullifiers: bool, mempool: bool) -> Result<Self, Status> {
        let mut shape = Self {
            pools: [false; 5],
            nullifiers,
            mempool,
        };
        let mut use_default = true;
        for &pool in pool_types {
            match PoolType::try_from(pool) {
                Ok(PoolType::Invalid) => {
                    return Err(if mempool {
                        Status::invalid_argument("invalid pool type requested")
                    } else {
                        Status::unknown("GetBlockRange: invalid pool type requested")
                    });
                }
                Ok(PoolType::Transparent) if nullifiers => continue,
                Ok(
                    pool @ (PoolType::Transparent
                    | PoolType::Sapling
                    | PoolType::Orchard
                    | PoolType::Ironwood),
                ) => {
                    shape.pools[pool as usize] = true;
                }
                Err(_) => {}
            }
            use_default = false;
        }
        if use_default {
            shape.pools = [false, false, true, true, true];
        }
        Ok(shape)
    }

    fn block(self, block: &FixtureBlock, keep_empty: bool) -> CompactBlock {
        let mut compact = block.compact();
        compact.vtx.retain_mut(|tx| self.project(tx) || keep_empty);
        if self.nullifiers {
            compact.chain_metadata = Some(ChainMetadata::default());
        }
        compact
    }

    fn project(self, tx: &mut CompactTx) -> bool {
        let transparent = self.pools[PoolType::Transparent as usize] && !self.nullifiers;
        if !transparent || self.mempool {
            tx.vin.clear();
        }
        if !transparent {
            tx.vout.clear();
        }
        if !self.pools[PoolType::Sapling as usize] {
            tx.spends.clear();
            tx.outputs.clear();
        }
        if !self.pools[PoolType::Orchard as usize] {
            tx.actions.clear();
        }
        if !self.pools[PoolType::Ironwood as usize] {
            tx.ironwood_actions.clear();
        }
        let selected = !tx.vin.is_empty()
            || !tx.vout.is_empty()
            || !tx.spends.is_empty()
            || !tx.outputs.is_empty()
            || !tx.actions.is_empty()
            || !tx.ironwood_actions.is_empty();
        if self.nullifiers {
            tx.outputs.clear();
            for action in tx.actions.iter_mut().chain(&mut tx.ironwood_actions) {
                action.cmx.clear();
                action.ephemeral_key.clear();
                action.ciphertext.clear();
            }
        }
        selected
    }
}

impl Darkside {
    fn block_range(&self, range: &BlockRange, nullifiers: bool) -> Rpc<StreamReply<CompactBlock>> {
        let (start, end) = range_heights(range)?;
        let shape = ScanShape::select(&range.pool_types, nullifiers, false)?;
        let scenario = self.scenario.lock();
        find_block(
            &scenario,
            &BlockId {
                height: start,
                hash: Vec::new(),
            },
            decode_internal_block_hash,
        )?;
        find_block(
            &scenario,
            &BlockId {
                height: end,
                hash: Vec::new(),
            },
            decode_internal_block_hash,
        )?;
        let mut blocks: Vec<_> = scenario
            .chain
            .visible()
            .iter()
            .filter(|block| (start.min(end)..=start.max(end)).contains(&block.height))
            .cloned()
            .collect();
        drop(scenario);
        if start > end {
            blocks.reverse();
        }
        Ok(stream_reply(
            self.stream_slot()?,
            blocks
                .into_iter()
                .map(move |block| shape.block(&block, false)),
        ))
    }

    fn address_transactions(
        &self,
        filter: &TransparentAddressBlockFilter,
    ) -> Rpc<StreamReply<RawTransaction>> {
        check_address(&filter.address)?;
        let range = filter
            .range
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing address transaction range"))?;
        let (start, end) = range_heights(range)?;
        let scenario = self.scenario.lock();
        let tip = tip(&scenario)?.height;
        let mut transactions: Vec<_> = scenario
            .address_transactions
            .iter()
            .filter(|tx| {
                tx.address == filter.address
                    && (start.min(end)..=start.max(end).min(tip)).contains(&tx.height)
            })
            .map(|tx| RawTransaction {
                data: tx.data.clone(),
                height: tx.height,
            })
            .collect();
        drop(scenario);
        transactions.sort_by_key(|tx| tx.height);
        if start > end {
            transactions.reverse();
        }
        Ok(stream_reply(self.stream_slot()?, transactions.into_iter()))
    }

    fn balance(&self, addresses: Vec<String>) -> Rpc<Balance> {
        let utxos = self.address_utxos(&GetAddressUtxosArg {
            addresses,
            ..GetAddressUtxosArg::default()
        })?;
        let value_zat = utxos
            .into_iter()
            .try_fold(0_i64, |total, utxo| total.checked_add(utxo.value_zat))
            .ok_or_else(|| Status::out_of_range("address balance exceeds the wire range"))?;
        Ok(Response::new(Balance { value_zat }))
    }

    fn address_utxos(
        &self,
        request: &GetAddressUtxosArg,
    ) -> Result<Vec<GetAddressUtxosReply>, Status> {
        if request.addresses.len() > MAX_ADDRESSES {
            return Err(Status::resource_exhausted("too many requested addresses"));
        }
        for address in &request.addresses {
            check_address(address)?;
        }
        let addresses: BTreeSet<_> = request.addresses.iter().collect();
        let scenario = self.scenario.lock();
        let tip = tip(&scenario)?.height;
        let mut utxos: Vec<_> = scenario
            .utxos
            .iter()
            .filter(|utxo| {
                addresses.contains(&utxo.address)
                    && (request.start_height..=tip).contains(&utxo.height)
            })
            .cloned()
            .collect();
        drop(scenario);
        utxos.sort_by_key(|utxo| utxo.height);
        if request.max_entries != 0 {
            utxos.truncate(request.max_entries as usize);
        }
        Ok(utxos)
    }
}

fn check_address(address: &str) -> Result<(), Status> {
    if address.is_empty() || address.len() > 512 {
        return Err(Status::invalid_argument(
            "address must contain 1 to 512 bytes",
        ));
    }
    Ok(())
}

fn tree_state(scenario: &Scenario, block: &FixtureBlock) -> Result<TreeState, Status> {
    scenario
        .tree_states
        .get(&block.height)
        .filter(|tree| {
            tree.hash.is_empty() || decode_rpc_block_hash_hex(&tree.hash).ok() == Some(block.hash)
        })
        .cloned()
        .ok_or_else(|| Status::not_found("no tree state for the visible block"))
}

fn reject_send(reason: impl Into<String>) -> Response<SendResponse> {
    Response::new(SendResponse {
        error_code: -26,
        error_message: reason.into(),
    })
}

fn pool_bytes(pool: &[Arc<FixtureTx>]) -> usize {
    pool.iter().map(|tx| tx.retained_bytes).sum()
}

#[tonic::async_trait]
impl CompactTxStreamer for Darkside {
    async fn get_latest_block(&self, _: Request<ChainSpec>) -> Rpc<BlockId> {
        Ok(Response::new(tip(&self.scenario.lock())?.id()))
    }

    async fn get_block(&self, request: Request<BlockId>) -> Rpc<CompactBlock> {
        Ok(Response::new(
            find_block(
                &self.scenario.lock(),
                request.get_ref(),
                decode_internal_block_hash,
            )?
            .compact(),
        ))
    }

    async fn get_block_nullifiers(&self, request: Request<BlockId>) -> Rpc<CompactBlock> {
        let scenario = self.scenario.lock();
        let block = find_block(&scenario, request.get_ref(), decode_internal_block_hash)?;
        let compact = ScanShape::select(&[], true, false)?.block(block, true);
        drop(scenario);
        Ok(Response::new(compact))
    }

    type GetBlockRangeStream = StreamReply<CompactBlock>;

    async fn get_block_range(
        &self,
        request: Request<BlockRange>,
    ) -> Rpc<Self::GetBlockRangeStream> {
        self.block_range(request.get_ref(), false)
    }

    type GetBlockRangeNullifiersStream = StreamReply<CompactBlock>;

    async fn get_block_range_nullifiers(
        &self,
        request: Request<BlockRange>,
    ) -> Rpc<Self::GetBlockRangeNullifiersStream> {
        self.block_range(request.get_ref(), true)
    }

    async fn get_transaction(&self, request: Request<TxFilter>) -> Rpc<RawTransaction> {
        let request = request.into_inner();
        if request.block.is_some() || request.index != 0 {
            return Err(Status::invalid_argument("select transaction by hash"));
        }
        let id = decode_internal_transaction_id(&request.hash)
            .map_err(|_| Status::invalid_argument("invalid transaction ID"))?;
        let scenario = self.scenario.lock();
        for block in scenario.chain.visible() {
            if let Some(tx) = block.transactions.iter().find(|tx| tx.id == id) {
                return Ok(Response::new(tx.raw(block.height)));
            }
        }
        if let Some(tx) = scenario.mempool.iter().find(|tx| tx.id == id) {
            return Ok(Response::new(tx.raw(0)));
        }
        drop(scenario);
        Err(Status::not_found("transaction is not visible"))
    }

    async fn send_transaction(&self, request: Request<RawTransaction>) -> Rpc<SendResponse> {
        if self.reject_broadcasts {
            return Ok(reject_send("broadcast rejection configured for this test"));
        }
        let tx = match FixtureTx::parse(request.into_inner().data) {
            Ok(tx) if !tx.coinbase => Arc::new(tx),
            Ok(_) => return Ok(reject_send("cannot broadcast a coinbase transaction")),
            Err(error) => return Ok(reject_send(error.message())),
        };
        let mut scenario = self.scenario.lock();
        tip(&scenario)?;
        if scenario.mined(tx.id) || scenario.mempool.iter().any(|pending| pending.id == tx.id) {
            return Ok(reject_send("transaction is already known"));
        }
        if pool_bytes(&scenario.incoming) + tx.retained_bytes > MAX_POOL_BYTES
            || pool_bytes(&scenario.mempool) + tx.retained_bytes > MAX_POOL_BYTES
        {
            return Err(Status::resource_exhausted("broadcast pool is full"));
        }
        let txid_hex = encode_rpc_transaction_id_hex(tx.id);
        scenario.incoming.push(Arc::clone(&tx));
        scenario.mempool.push(tx);
        drop(scenario);
        self.changed.send_replace(());
        Ok(Response::new(SendResponse {
            error_code: 0,
            error_message: txid_hex,
        }))
    }

    type GetTaddressTxidsStream = StreamReply<RawTransaction>;

    async fn get_taddress_txids(
        &self,
        request: Request<TransparentAddressBlockFilter>,
    ) -> Rpc<Self::GetTaddressTxidsStream> {
        self.address_transactions(request.get_ref())
    }

    type GetTaddressTransactionsStream = StreamReply<RawTransaction>;

    async fn get_taddress_transactions(
        &self,
        request: Request<TransparentAddressBlockFilter>,
    ) -> Rpc<Self::GetTaddressTransactionsStream> {
        self.address_transactions(request.get_ref())
    }

    async fn get_taddress_balance(&self, request: Request<AddressList>) -> Rpc<Balance> {
        self.balance(request.into_inner().addresses)
    }

    async fn get_taddress_balance_stream(
        &self,
        request: Request<Streaming<Address>>,
    ) -> Rpc<Balance> {
        let mut stream = request.into_inner();
        let mut addresses = Vec::new();
        while let Some(address) = stream.message().await? {
            if addresses.len() == MAX_ADDRESSES {
                return Err(Status::resource_exhausted("too many requested addresses"));
            }
            check_address(&address.address)?;
            addresses.push(address.address);
        }
        self.balance(addresses)
    }

    type GetMempoolTxStream = StreamReply<CompactTx>;

    async fn get_mempool_tx(
        &self,
        request: Request<GetMempoolTxRequest>,
    ) -> Rpc<Self::GetMempoolTxStream> {
        let request = request.into_inner();
        let shape = ScanShape::select(&request.pool_types, false, true)?;
        let suffixes = request.exclude_txid_suffixes;
        if suffixes.len() > MAX_SUFFIXES
            || suffixes
                .iter()
                .any(|suffix| !(1..=32).contains(&suffix.len()))
        {
            return Err(Status::invalid_argument("invalid transaction ID suffixes"));
        }
        let pending = self.scenario.lock().mempool.clone();
        let mut excluded = BTreeSet::new();
        for suffix in &suffixes {
            let mut matching = pending
                .iter()
                .filter(|tx| tx.txid_bytes().ends_with(suffix));
            if let Some(tx) = matching.next()
                && matching.next().is_none()
            {
                excluded.insert(tx.id);
            }
        }
        Ok(stream_reply(
            self.stream_slot()?,
            pending.into_iter().filter_map(move |tx| {
                if excluded.contains(&tx.id) {
                    return None;
                }
                let mut compact = tx.compact();
                shape.project(&mut compact).then_some(compact)
            }),
        ))
    }

    type GetMempoolStreamStream = StreamReply<RawTransaction>;

    async fn get_mempool_stream(&self, _: Request<Empty>) -> Rpc<Self::GetMempoolStreamStream> {
        let permit = self.stream_slot()?;
        let scenario = self.scenario.lock();
        tip(&scenario)?;
        let generation = Arc::clone(&scenario.tip_generation);
        let changed = self.changed.subscribe();
        drop(scenario);
        let scenario = Arc::clone(&self.scenario);
        let stop = self.stop.clone();
        Ok(Response::new(Box::pin(stream::unfold(
            (changed, 0, permit),
            move |(mut changed, index, permit)| {
                let scenario = Arc::clone(&scenario);
                let generation = Arc::clone(&generation);
                let stop = stop.clone();
                async move {
                    loop {
                        changed.borrow_and_update();
                        {
                            let scenario = scenario.lock();
                            if !Arc::ptr_eq(&generation, &scenario.tip_generation) {
                                return None;
                            }
                            if let Some(tx) = scenario.mempool.get(index) {
                                return Some((Ok(tx.raw(0)), (changed, index + 1, permit)));
                            }
                        }
                        tokio::select! {
                            updated = changed.changed() => updated.ok()?,
                            () = stop.cancelled() => return None,
                        }
                    }
                }
            },
        ))))
    }

    async fn get_tree_state(&self, request: Request<BlockId>) -> Rpc<TreeState> {
        let scenario = self.scenario.lock();
        let block = find_block(&scenario, request.get_ref(), decode_rpc_block_hash_bytes)?;
        let tree = tree_state(&scenario, block)?;
        drop(scenario);
        Ok(Response::new(tree))
    }

    async fn get_latest_tree_state(&self, _: Request<Empty>) -> Rpc<TreeState> {
        let scenario = self.scenario.lock();
        Ok(Response::new(tree_state(&scenario, tip(&scenario)?)?))
    }

    type GetSubtreeRootsStream = StreamReply<SubtreeRoot>;

    async fn get_subtree_roots(
        &self,
        request: Request<GetSubtreeRootsArg>,
    ) -> Rpc<Self::GetSubtreeRootsStream> {
        let request = request.into_inner();
        ShieldedProtocol::try_from(request.shielded_protocol)
            .map_err(|_| Status::invalid_argument("unknown shielded protocol"))?;
        let scenario = self.scenario.lock();
        let tip = tip(&scenario)?.height;
        let max = if request.max_entries == 0 {
            usize::MAX
        } else {
            request.max_entries as usize
        };
        let roots =
            if let Some((start, roots)) = scenario.subtree_roots.get(&request.shielded_protocol) {
                let offset = request.start_index.checked_sub(*start).ok_or_else(|| {
                    Status::out_of_range("start index precedes the supplied roots")
                })?;
                roots
                    .iter()
                    .skip(offset as usize)
                    .take_while(|root| root.completing_block_height <= tip)
                    .take(max)
                    .cloned()
                    .collect()
            } else {
                Vec::new()
            };
        drop(scenario);
        Ok(stream_reply(self.stream_slot()?, roots.into_iter()))
    }

    async fn get_address_utxos(
        &self,
        request: Request<GetAddressUtxosArg>,
    ) -> Rpc<GetAddressUtxosReplyList> {
        Ok(Response::new(GetAddressUtxosReplyList {
            address_utxos: self.address_utxos(request.get_ref())?,
        }))
    }

    type GetAddressUtxosStreamStream = StreamReply<GetAddressUtxosReply>;

    async fn get_address_utxos_stream(
        &self,
        request: Request<GetAddressUtxosArg>,
    ) -> Rpc<Self::GetAddressUtxosStreamStream> {
        Ok(stream_reply(
            self.stream_slot()?,
            self.address_utxos(request.get_ref())?.into_iter(),
        ))
    }

    async fn get_lightd_info(&self, _: Request<Empty>) -> Rpc<LightdInfo> {
        let scenario = self.scenario.lock();
        let metadata = scenario
            .chain
            .metadata
            .clone()
            .ok_or_else(|| Status::failed_precondition("call Reset first"))?;
        let block_height = scenario.chain.tip.unwrap_or_default();
        drop(scenario);
        Ok(Response::new(LightdInfo {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            vendor: "Zinder Darkside".to_owned(),
            taddr_support: true,
            chain_name: metadata.chain_name,
            sapling_activation_height: u64::try_from(metadata.sapling_activation)
                .map_err(|_| Status::internal("invalid activation height"))?,
            consensus_branch_id: metadata.branch_id,
            block_height,
            estimated_height: block_height,
            lightwallet_protocol_version: LIGHTWALLETD_PROTOCOL_VERSION.to_owned(),
            ..LightdInfo::default()
        }))
    }

    async fn get_standard_fee(&self, _: Request<Empty>) -> Rpc<StandardFee> {
        Ok(Response::new(StandardFee {
            fee_zat: 5_000,
            estimator_version: 0,
        }))
    }

    async fn ping(&self, request: Request<PingDuration>) -> Rpc<PingResponse> {
        let request = request.into_inner();
        if !(0..=5_000_000).contains(&request.interval_us) {
            return Err(Status::invalid_argument(
                "ping interval must be between zero and five seconds",
            ));
        }
        self.ping.respond(request).await.map(Response::new)
    }
}

#[cfg(test)]
mod tests {
    use super::ScanShape;
    use zinder_proto::compat::lightwalletd::{CompactSaplingSpend, CompactTx, PoolType, TxOut};

    #[test]
    fn unknown_pool_numbers_keep_the_selection_explicit() -> eyre::Result<()> {
        let shape = ScanShape::select(&[i32::MAX], false, false)?;
        assert_eq!(shape.pools, [false; 5]);

        let shape = ScanShape::select(&[PoolType::Transparent.into(), i32::MAX], true, false)?;
        assert_eq!(shape.pools, [false; 5]);
        Ok(())
    }

    #[test]
    fn repeated_known_pools_still_select_the_same_projection() -> eyre::Result<()> {
        let sapling = PoolType::Sapling.into();
        let single = ScanShape::select(&[sapling], false, false)?;
        let repeated = ScanShape::select(&[sapling; 6], false, false)?;
        assert_eq!(repeated.pools, single.pools);
        Ok(())
    }

    #[test]
    fn nullifier_range_with_only_transparent_selection_uses_shielded_defaults() -> eyre::Result<()>
    {
        let spend = CompactSaplingSpend { nf: vec![17; 32] };
        let mut tx = CompactTx {
            spends: vec![spend.clone()],
            vout: vec![TxOut {
                value: 1,
                script_pub_key: vec![0x51],
            }],
            ..CompactTx::default()
        };
        let shape = ScanShape::select(&[PoolType::Transparent.into()], true, false)?;
        assert!(shape.project(&mut tx));
        assert_eq!(tx.spends, [spend]);
        assert_eq!(tx.vout, Vec::<TxOut>::new());
        Ok(())
    }
}
