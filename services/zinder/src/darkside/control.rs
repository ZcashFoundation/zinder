use std::{collections::BTreeSet, sync::Arc};

use prost::Message;
use tokio::sync::OwnedSemaphorePermit;
use tonic::{Request, Response, Status, Streaming};
use zinder_core::wire::{decode_internal_transaction_id, decode_rpc_block_hash_hex};
use zinder_proto::compat::lightwalletd::{
    BlockId, DarksideAddressTransaction, DarksideBlock, DarksideBlocksUrl, DarksideEmptyBlocks,
    DarksideHeight, DarksideMetaState, DarksideSubtreeRoots, DarksideTransactionsUrl, Empty,
    GetAddressUtxosReply, RawTransaction, ShieldedProtocol, TreeState,
    darkside_streamer_server::DarksideStreamer,
};

use super::{
    Darkside, FixtureOrigin, MAX_CACHE_ENTRIES, MAX_INJECTED_BYTES, Rpc, Scenario, StreamReply,
    chain::{FixtureBlock, FixtureTx, MAX_BLOCKS, MAX_CHAIN_BYTES, decode_fixture},
    stream_reply,
};

impl Darkside {
    fn begin_stage(&self) -> Result<(OwnedSemaphorePermit, Arc<()>), Status> {
        let permit = Arc::clone(&self.stage_slot)
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("another staging request is in progress"))?;
        let scenario = self.scenario.lock();
        if scenario.chain.metadata.is_none() {
            return Err(Status::failed_precondition("call Reset first"));
        }
        Ok((permit, Arc::clone(&scenario.generation)))
    }

    fn stage(&self, generation: &Arc<()>, batch: Batch) -> Rpc<Empty> {
        let mut scenario = self.scenario.lock();
        if !Arc::ptr_eq(generation, &scenario.generation) {
            return Err(Status::aborted("scenario was reset during staging"));
        }
        match batch {
            Batch::Blocks(blocks) => scenario.chain.stage_blocks(blocks)?,
            Batch::Transactions(transactions) => scenario.chain.stage_transactions(transactions)?,
        }
        drop(scenario);
        Ok(Response::new(Empty {}))
    }

    async fn download(&self, url: &str) -> Result<String, Status> {
        let fixtures = self
            .fixtures
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("URL staging requires --fixture-origin"))?;
        fixtures.download(url).await
    }
}

impl FixtureOrigin {
    async fn download(&self, url: &str) -> Result<String, Status> {
        let url = reqwest::Url::parse(url)
            .map_err(|_| Status::invalid_argument("invalid fixture URL"))?;
        if url.origin() != self.origin.origin()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(Status::permission_denied(
                "fixture URL must match --fixture-origin",
            ));
        }
        let mut reply = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|_| Status::unavailable("fixture download failed"))?;
        if !reply.status().is_success() {
            return Err(Status::unavailable(
                "fixture origin returned a non-success status",
            ));
        }
        let mut body = Vec::new();
        while let Some(chunk) = reply
            .chunk()
            .await
            .map_err(|_| Status::unavailable("fixture download interrupted"))?
        {
            if body.len() + chunk.len() > MAX_CHAIN_BYTES {
                return Err(Status::resource_exhausted(
                    "fixture download exceeds the size limit",
                ));
            }
            body.extend_from_slice(&chunk);
        }
        String::from_utf8(body)
            .map_err(|_| Status::invalid_argument("fixture must contain hexadecimal text"))
    }
}

enum Batch {
    Blocks(Vec<Arc<FixtureBlock>>),
    Transactions(Vec<(u64, Arc<FixtureTx>)>),
}

fn staged_size(bytes: &mut usize, added_bytes: usize) -> Result<(), Status> {
    *bytes += added_bytes;
    if *bytes > MAX_CHAIN_BYTES {
        return Err(Status::resource_exhausted(
            "staging batch exceeds the memory budget",
        ));
    }
    Ok(())
}

fn stage_block(
    blocks: &mut Vec<Arc<FixtureBlock>>,
    bytes: &mut usize,
    hex_block: &str,
) -> Result<(), Status> {
    if blocks.len() == MAX_BLOCKS {
        return Err(Status::resource_exhausted(
            "too many blocks in one staging batch",
        ));
    }
    let block = FixtureBlock::parse(hex_block)?;
    staged_size(bytes, block.retained_bytes())?;
    blocks.push(Arc::new(block));
    Ok(())
}

fn stage_transaction(
    transactions: &mut Vec<(u64, Arc<FixtureTx>)>,
    bytes: &mut usize,
    raw: RawTransaction,
) -> Result<(), Status> {
    check_height(raw.height)?;
    let tx = FixtureTx::parse(raw.data)?;
    if tx.coinbase {
        return Err(Status::invalid_argument(
            "a staged transaction cannot be a coinbase",
        ));
    }
    staged_size(bytes, tx.retained_bytes)?;
    transactions.push((raw.height, Arc::new(tx)));
    Ok(())
}

fn check_height(height: u64) -> Result<(), Status> {
    if i32::try_from(height).is_err() {
        return Err(Status::invalid_argument(
            "height is outside the Darkside height range",
        ));
    }
    Ok(())
}

fn height(height: i32) -> Result<u64, Status> {
    u64::try_from(height).map_err(|_| Status::invalid_argument("height must be non-negative"))
}

fn check_cache(entries: usize, message: &impl Message) -> Result<(), Status> {
    if entries >= MAX_CACHE_ENTRIES || message.encoded_len() > MAX_INJECTED_BYTES {
        return Err(Status::resource_exhausted(
            "injected fixture exceeds cache limits",
        ));
    }
    Ok(())
}

#[tonic::async_trait]
impl DarksideStreamer for Darkside {
    async fn reset(&self, request: Request<DarksideMetaState>) -> Rpc<Empty> {
        let metadata = request.into_inner();
        height(metadata.sapling_activation)?;
        if metadata.branch_id.len() != 8
            || !metadata
                .branch_id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(Status::invalid_argument(
                "branch ID must contain eight hexadecimal digits",
            ));
        }
        if metadata.chain_name.is_empty() || metadata.chain_name.len() > 64 {
            return Err(Status::invalid_argument(
                "chain name must contain 1 to 64 bytes",
            ));
        }
        let mut scenario = self.scenario.lock();
        *scenario = Scenario::default();
        scenario.chain.metadata = Some(metadata);
        drop(scenario);
        self.changed.send_replace(());
        Ok(Response::new(Empty {}))
    }

    async fn stage_blocks_stream(&self, request: Request<Streaming<DarksideBlock>>) -> Rpc<Empty> {
        let (_permit, generation) = self.begin_stage()?;
        let mut blocks = Vec::new();
        let mut bytes = 0;
        let mut input = request.into_inner();
        while let Some(block) = input.message().await? {
            stage_block(&mut blocks, &mut bytes, &block.block)?;
        }
        self.stage(&generation, Batch::Blocks(blocks))
    }

    async fn stage_blocks(&self, request: Request<DarksideBlocksUrl>) -> Rpc<Empty> {
        let (_permit, generation) = self.begin_stage()?;
        let body = self.download(&request.get_ref().url).await?;
        let mut blocks = Vec::new();
        let mut bytes = 0;
        for line in body.lines() {
            stage_block(&mut blocks, &mut bytes, line)?;
        }
        self.stage(&generation, Batch::Blocks(blocks))
    }

    async fn stage_blocks_create(&self, request: Request<DarksideEmptyBlocks>) -> Rpc<Empty> {
        let (_permit, generation) = self.begin_stage()?;
        let DarksideEmptyBlocks {
            height: start,
            nonce,
            count,
        } = request.into_inner();
        let start = height(start)?;
        let count = usize::try_from(count)
            .ok()
            .filter(|count| (1..=MAX_BLOCKS).contains(count))
            .ok_or_else(|| Status::invalid_argument("block count must be between 1 and 4096"))?;
        let end = start + count as u64;
        check_height(end - 1)?;
        let blocks = (start..end)
            .map(|height| Arc::new(FixtureBlock::synthetic(height, nonce)))
            .collect();
        self.stage(&generation, Batch::Blocks(blocks))
    }

    async fn stage_transactions_stream(
        &self,
        request: Request<Streaming<RawTransaction>>,
    ) -> Rpc<Empty> {
        let (_permit, generation) = self.begin_stage()?;
        let mut transactions = Vec::new();
        let mut bytes = 0;
        let mut input = request.into_inner();
        while let Some(raw) = input.message().await? {
            stage_transaction(&mut transactions, &mut bytes, raw)?;
        }
        self.stage(&generation, Batch::Transactions(transactions))
    }

    async fn stage_transactions(&self, request: Request<DarksideTransactionsUrl>) -> Rpc<Empty> {
        let (_permit, generation) = self.begin_stage()?;
        let request = request.into_inner();
        let height = height(request.height)?;
        let body = self.download(&request.url).await?;
        let mut transactions = Vec::new();
        let mut bytes = 0;
        for line in body.lines() {
            stage_transaction(
                &mut transactions,
                &mut bytes,
                RawTransaction {
                    data: decode_fixture(line)?,
                    height,
                },
            )?;
        }
        self.stage(&generation, Batch::Transactions(transactions))
    }

    async fn apply_staged(&self, request: Request<DarksideHeight>) -> Rpc<Empty> {
        let mut scenario = self.scenario.lock();
        let previous = scenario.chain.active.clone();
        let prior_tip = scenario.visible_tip();
        scenario.chain.apply(height(request.get_ref().height)?)?;
        if let Some(orphaned) = previous
            .iter()
            .zip(&scenario.chain.active)
            .find(|(old, new)| old.hash != new.hash)
            .map(|(old, _)| old)
            .or_else(|| previous.get(scenario.chain.active.len()))
        {
            let fork = orphaned.height;
            let end = previous.last().map_or(fork, |block| block.height);
            let remains = |height: u64| height < fork || height > end;
            scenario.tree_states.retain(|height, _| remains(*height));
            scenario.utxos.retain(|utxo| remains(utxo.height));
            scenario
                .address_transactions
                .retain(|tx| remains(tx.height));
            for (_, roots) in scenario.subtree_roots.values_mut() {
                if let Some(index) = roots
                    .iter()
                    .position(|root| !remains(root.completing_block_height))
                {
                    roots.truncate(index);
                }
            }
        }
        let Scenario { chain, mempool, .. } = &mut *scenario;
        if !mempool.is_empty() {
            let mined: BTreeSet<_> = chain
                .visible()
                .iter()
                .flat_map(|block| block.transactions.iter().map(|tx| tx.id))
                .collect();
            mempool.retain(|tx| !mined.contains(&tx.id));
        }
        let tip_changed = prior_tip != scenario.visible_tip();
        if tip_changed {
            scenario.tip_generation = Arc::new(());
        }
        drop(scenario);
        if tip_changed {
            self.changed.send_replace(());
        }
        Ok(Response::new(Empty {}))
    }

    type GetIncomingTransactionsStream = StreamReply<RawTransaction>;

    async fn get_incoming_transactions(
        &self,
        _: Request<Empty>,
    ) -> Rpc<Self::GetIncomingTransactionsStream> {
        let permit = self.stream_slot()?;
        let incoming = std::mem::take(&mut self.scenario.lock().incoming);
        Ok(stream_reply(
            permit,
            incoming.into_iter().map(|tx| tx.raw(u64::MAX)),
        ))
    }

    async fn clear_incoming_transactions(&self, _: Request<Empty>) -> Rpc<Empty> {
        self.scenario.lock().incoming.clear();
        Ok(Response::new(Empty {}))
    }

    async fn add_address_utxo(&self, request: Request<GetAddressUtxosReply>) -> Rpc<Empty> {
        let utxo = request.into_inner();
        check_height(utxo.height)?;
        if utxo.address.is_empty()
            || utxo.index < 0
            || utxo.value_zat < 0
            || decode_internal_transaction_id(&utxo.txid).is_err()
        {
            return Err(Status::invalid_argument("invalid address UTXO"));
        }
        let mut scenario = self.scenario.lock();
        check_cache(scenario.utxos.len(), &utxo)?;
        if scenario
            .utxos
            .iter()
            .any(|entry| entry.txid == utxo.txid && entry.index == utxo.index)
        {
            return Err(Status::already_exists("UTXO outpoint is already present"));
        }
        scenario.utxos.push(utxo);
        drop(scenario);
        Ok(Response::new(Empty {}))
    }

    async fn clear_address_utxo(&self, _: Request<Empty>) -> Rpc<Empty> {
        self.scenario.lock().utxos.clear();
        Ok(Response::new(Empty {}))
    }

    async fn add_address_transaction(
        &self,
        request: Request<DarksideAddressTransaction>,
    ) -> Rpc<Empty> {
        let tx = request.into_inner();
        check_height(tx.height)?;
        if tx.address.is_empty() || tx.data.is_empty() {
            return Err(Status::invalid_argument(
                "address and transaction bytes are required",
            ));
        }
        let mut scenario = self.scenario.lock();
        check_cache(scenario.address_transactions.len(), &tx)?;
        scenario.address_transactions.push(tx);
        drop(scenario);
        Ok(Response::new(Empty {}))
    }

    async fn clear_address_transactions(&self, _: Request<Empty>) -> Rpc<Empty> {
        self.scenario.lock().address_transactions.clear();
        Ok(Response::new(Empty {}))
    }

    async fn add_tree_state(&self, request: Request<TreeState>) -> Rpc<Empty> {
        let tree = request.into_inner();
        check_height(tree.height)?;
        if !tree.hash.is_empty() && decode_rpc_block_hash_hex(&tree.hash).is_err() {
            return Err(Status::invalid_argument("invalid tree-state block hash"));
        }
        let mut scenario = self.scenario.lock();
        if tree.encoded_len() > 2 * MAX_INJECTED_BYTES
            || (!scenario.tree_states.contains_key(&tree.height)
                && scenario.tree_states.len() == MAX_CACHE_ENTRIES)
        {
            return Err(Status::resource_exhausted(
                "tree-state fixture exceeds cache limits",
            ));
        }
        scenario.tree_states.insert(tree.height, tree);
        drop(scenario);
        Ok(Response::new(Empty {}))
    }

    async fn remove_tree_state(&self, request: Request<BlockId>) -> Rpc<Empty> {
        let request = request.into_inner();
        if !request.hash.is_empty() {
            return Err(Status::invalid_argument("remove tree state by height"));
        }
        self.scenario.lock().tree_states.remove(&request.height);
        Ok(Response::new(Empty {}))
    }

    async fn clear_all_tree_states(&self, _: Request<Empty>) -> Rpc<Empty> {
        self.scenario.lock().tree_states.clear();
        Ok(Response::new(Empty {}))
    }

    async fn set_subtree_roots(&self, request: Request<DarksideSubtreeRoots>) -> Rpc<Empty> {
        let request = request.into_inner();
        ShieldedProtocol::try_from(request.shielded_protocol)
            .map_err(|_| Status::invalid_argument("unknown shielded protocol"))?;
        if request.subtree_roots.len() > MAX_CACHE_ENTRIES
            || request
                .start_index
                .checked_add(u32::try_from(request.subtree_roots.len()).unwrap_or(u32::MAX))
                .is_none()
        {
            return Err(Status::resource_exhausted(
                "subtree roots exceed cache limits",
            ));
        }
        for root in &request.subtree_roots {
            if root.root_hash.len() != 32 || root.completing_block_hash.len() != 32 {
                return Err(Status::invalid_argument(
                    "subtree hashes must contain 32 bytes",
                ));
            }
            check_height(root.completing_block_height)?;
        }
        if !request
            .subtree_roots
            .windows(2)
            .all(|pair| pair[0].completing_block_height <= pair[1].completing_block_height)
        {
            return Err(Status::invalid_argument(
                "subtree completion heights must be ordered",
            ));
        }
        self.scenario.lock().subtree_roots.insert(
            request.shielded_protocol,
            (request.start_index, request.subtree_roots),
        );
        Ok(Response::new(Empty {}))
    }

    async fn stop(&self, _: Request<Empty>) -> Rpc<Empty> {
        self.stop.cancel();
        Ok(Response::new(Empty {}))
    }
}
