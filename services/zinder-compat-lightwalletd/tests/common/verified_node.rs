use serde_json::{Value, json};
use zinder_core::{
    BlockHash, BlockHeight, Network, NetworkUpgradeActivation, NetworkUpgradeActivations,
    wire::{encode_branch_id_hex, encode_rpc_block_hash_hex},
};
use zinder_testkit::{
    JsonRpcStub, JsonRpcTestServer, RpcReply, method, sample_regtest_upgrade_activations,
};

/// The sample regtest upgrades with one upgrade per height, so a node table
/// compares equal whatever order its `upgrades` object lists them in.
pub(crate) fn one_upgrade_per_height() -> eyre::Result<NetworkUpgradeActivations> {
    Ok(NetworkUpgradeActivations::new(
        Network::ZcashRegtest,
        sample_regtest_upgrade_activations()
            .activations()
            .iter()
            .zip(1..)
            .map(|(activation, height)| NetworkUpgradeActivation {
                activation_height: BlockHeight::new(height),
                ..activation.clone()
            })
            .collect(),
    )?)
}

/// Starts a node double that passes first-contact verification once for a
/// pair serving `schedule` with `tip_hash` at its tip, then serves the additional RPC replies.
pub(crate) fn verifiable_node(
    schedule: &NetworkUpgradeActivations,
    tip_hash: BlockHash,
    replies: impl IntoIterator<Item = JsonRpcStub>,
) -> eyre::Result<JsonRpcTestServer> {
    let upgrades: serde_json::Map<String, Value> = schedule
        .activations()
        .iter()
        .map(|activation| {
            (
                encode_branch_id_hex(activation.branch_id),
                json!({
                    "name": activation.name,
                    "activationheight": activation.activation_height.value(),
                    "status": "active",
                }),
            )
        })
        .collect();
    let admission = [
        method("rpc.discover").reply(RpcReply::result(json!({
            "openrpc": "1.3.2",
            "methods": [
                {"name": "rpc.discover"},
                {"name": "sendrawtransaction"},
                {"name": "getstandardfee"},
            ],
        }))),
        method("getblockchaininfo").reply(RpcReply::result(json!({ "upgrades": upgrades }))),
        method("getblockhash").reply(RpcReply::result(json!(encode_rpc_block_hash_hex(tip_hash)))),
    ];
    JsonRpcTestServer::start(admission.into_iter().chain(replies))
}
