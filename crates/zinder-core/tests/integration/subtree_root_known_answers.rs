//! Subtree roots derived from real Testnet blocks match the node's roots.
//!
//! Each case is one pool's tree state before a block that completes a subtree
//! partway through its note commitments, the commitments that block adds to
//! the pool, and the node's tree-state root and subtree root for the block.
//! The vectors were read from a Zakura 1.8.6 Testnet node with
//! `z_gettreestate`, `getblock` at verbosity 2, and `z_getsubtreesbyindex`.
//! Sapling commitments are in wire order, the reverse of `getblock`'s `cmu`
//! hex.

use eyre::{Result, eyre};
use zinder_core::{
    BlockHeight, CommitmentTreeAccumulator, CommitmentTreeFrontier, CommitmentTreeFrontiers,
    CompletedSubtreeRoot, ConsensusBranchId, FinalNoteCommitmentRoot, Network,
    NetworkUpgradeActivation, NetworkUpgradeActivations, ShieldedProtocol, SubtreeRootHash,
    SubtreeRootIndex,
};

#[test]
fn accumulator_derives_the_subtree_roots_testnet_blocks_complete() -> Result<()> {
    let activations = testnet_pool_activations()?;
    for completion in KNOWN_SUBTREE_COMPLETIONS {
        let label = format!(
            "{:?} subtree {}",
            completion.protocol, completion.subtree_index
        );
        let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
            completion.predecessor_height()?,
            &completion.predecessor_frontiers(&activations)?,
            &activations,
        )?;
        let commitments = completion
            .commitments
            .iter()
            .map(|commitment| decode_32(commitment))
            .collect::<Result<Vec<_>>>()?;
        let (sapling, orchard, ironwood) = match completion.protocol {
            ShieldedProtocol::Sapling => (commitments.as_slice(), &[][..], &[][..]),
            ShieldedProtocol::Orchard => (&[][..], commitments.as_slice(), &[][..]),
            ShieldedProtocol::Ironwood => (&[][..], &[][..], commitments.as_slice()),
            _ => return Err(eyre!("{label}: unknown pool")),
        };
        let completing_height = BlockHeight::new(completion.completing_height);
        accumulator.append_block_commitments(completing_height, sapling, orchard, ironwood)?;

        // The node's root for the block's tree state authenticates the
        // commitments and their byte order.
        let frontiers = accumulator.validated_frontiers()?;
        let frontier = pool_frontier(&frontiers, completion.protocol)
            .ok_or_else(|| eyre!("{label}: the pool has no frontier"))?;
        assert_eq!(
            frontier.final_root(),
            FinalNoteCommitmentRoot::from_bytes(decode_32(completion.final_root)?),
            "{label}: block tree-state root"
        );
        assert_eq!(
            accumulator.completed_subtree_roots(),
            [CompletedSubtreeRoot {
                protocol: completion.protocol,
                subtree_index: SubtreeRootIndex::new(completion.subtree_index),
                root_hash: SubtreeRootHash::from_bytes(decode_32(completion.subtree_root)?),
                completing_block_height: completing_height,
            }],
            "{label}: subtree root"
        );
    }
    Ok(())
}

/// One subtree that a Testnet block completes, with the node's answers.
struct KnownSubtreeCompletion {
    protocol: ShieldedProtocol,
    subtree_index: u32,
    completing_height: u32,
    /// The pool's `finalRoot` at the block before the completing block.
    predecessor_final_root: &'static str,
    /// The pool's `finalState` at the block before the completing block.
    predecessor_final_state: &'static str,
    /// Note commitments the completing block adds to the pool, in wire order.
    commitments: &'static [&'static str],
    /// The pool's `finalRoot` at the completing block.
    final_root: &'static str,
    /// The node's `z_getsubtreesbyindex` root.
    subtree_root: &'static str,
}

impl KnownSubtreeCompletion {
    fn predecessor_height(&self) -> Result<BlockHeight> {
        self.completing_height
            .checked_sub(1)
            .map(BlockHeight::new)
            .ok_or_else(|| eyre!("no block precedes genesis"))
    }

    /// Returns the case's frontier for its pool and an empty frontier for
    /// every other pool active at the predecessor, which receives no
    /// commitments here.
    fn predecessor_frontiers(
        &self,
        activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeFrontiers> {
        let predecessor_height = self.predecessor_height()?;
        let frontier = |protocol: ShieldedProtocol| -> Result<Option<CommitmentTreeFrontier>> {
            if protocol == self.protocol {
                return Ok(Some(CommitmentTreeFrontier::from_canonical_final_state(
                    protocol,
                    FinalNoteCommitmentRoot::from_bytes(decode_32(self.predecessor_final_root)?),
                    hex::decode(self.predecessor_final_state)?,
                )?));
            }
            let active = activations
                .activation_height_by_name(protocol.activation_upgrade_name())
                .is_some_and(|activation_height| activation_height <= predecessor_height);
            Ok(active.then(|| CommitmentTreeFrontier::empty(protocol)))
        };
        Ok(CommitmentTreeFrontiers::from_validated_parts(
            frontier(ShieldedProtocol::Sapling)?,
            frontier(ShieldedProtocol::Orchard)?,
            frontier(ShieldedProtocol::Ironwood)?,
        ))
    }
}

fn pool_frontier(
    frontiers: &CommitmentTreeFrontiers,
    protocol: ShieldedProtocol,
) -> Option<&CommitmentTreeFrontier> {
    match protocol {
        ShieldedProtocol::Sapling => frontiers.sapling(),
        ShieldedProtocol::Orchard => frontiers.orchard(),
        ShieldedProtocol::Ironwood => frontiers.ironwood(),
        _ => None,
    }
}

/// Testnet's activations for the upgrades that activate a shielded pool.
fn testnet_pool_activations() -> Result<NetworkUpgradeActivations> {
    let activation = |branch_id, height, name: &str| NetworkUpgradeActivation {
        branch_id: ConsensusBranchId::new(branch_id),
        activation_height: BlockHeight::new(height),
        name: name.to_owned(),
    };
    Ok(NetworkUpgradeActivations::new(
        Network::ZcashTestnet,
        vec![
            activation(0x76b8_09bb, 280_000, "Sapling"),
            activation(0xc2d6_d0b4, 1_842_420, "NU5"),
            activation(0x37a5_165b, 4_134_000, "NU6.3"),
        ],
    )?)
}

fn decode_32(hex_bytes: &str) -> Result<[u8; 32]> {
    let bytes = hex::decode(hex_bytes)?;
    <[u8; 32]>::try_from(bytes.as_slice())
        .map_err(|_| eyre!("expected 32 bytes, found {}", bytes.len()))
}

const KNOWN_SUBTREE_COMPLETIONS: &[KnownSubtreeCompletion] = &[
    // Block 00148c6cb01f1f76182297e3464dd8d0d1edc9fb7c63e3aef4940d59c32c2bea.
    KnownSubtreeCompletion {
        protocol: ShieldedProtocol::Sapling,
        subtree_index: 0,
        completing_height: 490_074,
        predecessor_final_root: "6cc967d0793aad55e94bcad1d257c969d324d5de7eceaa87193653b42c7ae564",
        predecessor_final_state: concat!(
            "01223a0bb4f88cb80147223c660bdf09f7664f19062a41526fdd23de4edce5b9",
            "1f001f00000000018ba45043f5c349d36993d984379f9f6123f1a5ff1c589b50",
            "75ea79be17733e010107d7254b330c1c7e6ee4e37dc398bd8cb20b24dd49f5d1",
            "fd24fe027759c4ab3c01fd26199df5ef5589bec85d95ab0b1590ec4acdd85dd6",
            "1aaf77cdf4b7c1e7d46101393f0551dc5cab104a6d94c45c562563b3b163e583",
            "05203ab1c6d2b0dd0b550e015ca9b43e5a9b72d5058a42de9da0dd0a0f4222a7",
            "0cd10cb25b4ec7b2943e314b017f18ced50f2f06bf5e3f5cec4c35c98b3356d7",
            "fc607d10f09d628f430e62b6540192cfaeee553ceb3eedba413ea31f1a00201d",
            "54c5c31a201380cc713828e05c6001739fe8a76af395571d8c39734022178ba3",
            "33485eb3d959e56e404219c2266a0c01dd960b6c11b157d1626f0768ec099af9",
            "385aea3f31c91111a8c5b899ffb99e6b0192acd61b1853311b0bf166057ca433",
            "e231c93ab5988844a09a91c113ebc58e18019fbfd76ad6d98cafa0174391546e",
            "7022afe62e870e20e16d57c4c419a5c2bb690000000000000000000000000000",
            "0000",
        ),
        commitments: &[
            "945914d4845b0b69f59d30354fff85e8e8f245f6d5a82586e9b65311172b4267",
            "8ac9ef65dcf68f8826f8458337bb5345c04e291cfa794bf33f758477fbf6c73c",
            "6b9db84370f9f1c1c30908005c423f7bf7720e9d7c9f91bbfaa2595d4cfb5f6c",
            "3ac46aa34a1baa284258eb289dabb0a0771b02212f6526ebb39b526d888a7700",
            "f76da6191eaac5cce8ae6a5465bcefed7513ad17c9deb4fe2f1a0c06b21af118",
            "b3150ec2d652e140d6f5e0d43cc06147eb9be95c6c2eca2eced967520c37572f",
            "8d2d2b18ae3b92166d5bc94c84bec06aaf07347fa2263c8532ef003212d45748",
            "e9e22eeab8d6fe9ac715a0627d3eef225fdeae0b1ea66d581cd25cb1e0bb0631",
            "040faec5ea16545bb05d7e8b71ede6cf2a6dd707e3550eec6757d022dd2a4c0e",
            "a474c3e317c013717480b3f148e225dd21dea4a01a77c6842112f2a57d189d42",
            "de191024e3511a45c32a530fe83740b4116328a98bb75aeddd67060f47eb9336",
            "f4fa9ce2e12821ae66912c262ac64e2c1d5ce3c0aa09017cde8cad288e241266",
            "7a0fd006e43d73c320a5b0a5d4a696d82dc58118c60b98a28a24be20407f2854",
            "29371656ce9c8c9ff527c2d1bec5991bf19a390df29dd808579847d3d710f116",
            "c76c927e1af05765d71683631ed894aea1b0f95d08861a2e571aff4e8504642a",
            "0e830c2cf550a7e273d4d3e94baae433963615ec6260a9689a3996a06cea0c62",
            "c66a083e6769c83e5322e1e2586ccc38c12a7f030dde3ebdcca728cfa755bc22",
            "aa848767f01e4f8f35cc7d23481de69f5b9e980824abb22db6d81ead634cd331",
            "7f9d68f91eb89aa774714c0d3f9bcd8f1f3f8f190a210ca3529bff6e909dec60",
            "6b6d62eaa90609a996f5ca626b13cc2368a8f56f477e2decaec5d5b460386a3a",
            "7d95f1d8d3a354392fe43fea29f46e39b5e92bb692ea80d407c1b84fb15e0133",
            "fe1e7bf17db6d8d3739a7ce81a3b0dbbbdd613f89e9008375818a5bb61702d06",
            "1f6e6f97bfafeb1f625f02063909bf5257eed7d3f0f91ebd7ae2873e38ba0d18",
            "4fac8ba42affb1e56dcb625c0d426662f798910c9c6c03cbbc774e976f58586c",
            "f131c41a0e693116a7ce732f907e5d054335ea33c7f285292e649538125fe952",
            "10fcf7706f7afc6383d1c3217004c93695824a5cf9b69203cae4cedc45647017",
            "f5b719072a1c93019fd00455cb48adc2240406492702217c97ea58be37962b0c",
            "e80e74f08b307db42e1539fb5a77b1af5ea6b19555979a8f55ec861fd9203c16",
            "2bcfb56a286fd2e5bb750f8a71d3aa709ac654adc8367d2af0316de56a18e525",
            "af10a63332be9ff33fdb4d6ebeee3ead71a98f90d39b62edfe73c5b16dbae354",
            "0c225cedc8dd164d2ebea8dcb420f78f380ddf202eec308bc7f29616069d1503",
            "dcb1610dde7e50eaef17965120aaecfe5509e0a53bb7fc2d0d5ea331d5a38a6b",
            "5074d2fd347950fb55b0680d832199632caf37faa3826520403dff05eb78a315",
            "60adcea01a696484611f2d9dc0ec3d9eab337d22760806c213a3a95b8944b74c",
        ],
        final_root: "65fc28b0762801e5fa0d28036fe375cde21ea09968606047f133154c0985d3fd",
        subtree_root: "1f8322ef806eb2430dc4a7a41c1b344bea5be946efc7b4349c1c9edb14ff9d39",
    },
    // Block 001286284b8ff1b67b3445825171ad4e6f2906941a68cfb417ae80c03e71fd56.
    KnownSubtreeCompletion {
        protocol: ShieldedProtocol::Orchard,
        subtree_index: 2,
        completing_height: 4_094_022,
        predecessor_final_root: "70513b5a7ecc39871f960e7b43e9397e56323db6d4baf18bad862cd47ba22120",
        predecessor_final_state: concat!(
            "011a17c8d7daedc0e60041350444807bf7532838e9db7f6be3b298684f7f004d",
            "27018beb1cbb327e319bc1ae3cff5bd01f76c2042ed9c644c6c7639bc903b900",
            "38091f0000010ed27f98402390f150d7f4250444e9f9fb1c1e95e25e4ef18fea",
            "58ab99da911e017d7bb80d1241eb94349aeb152fe3de885b613b7e986ee38ef2",
            "288654f312a634011d8f22d0247be4297cd73f88ec10731cd002b91071ae7489",
            "b7f666d3bb227a0d0150bb0b109ffb7f9c981af08102963e088130c478ab170d",
            "13259e2887d2e05231011fd44e0fc866bb3d8e36e20fa6b050b359b04715ab76",
            "40c7816e6b79b932f71601456a5452b9b01401fc0d01fcd7487c429c8f47f25b",
            "20da13dc66ef0b72e32d3501736e4f0a66f97d255e4e227a52fecc32b52ad97c",
            "ed87fc740f1534c5acccda0f01e3a5773f1167a92c6f64793bdb1e233122b8df",
            "b9f48f68550b6ab23c26164d0901cf744206851afba2e94e7f084e8ed9282809",
            "54194758b1d163a6b77f2684e33001572eb95a1bf011d08ece1d953cc35d4057",
            "84afb37f36013f79fd0627bc2e853f01a6d6574f4f46dba55e06e54611d3d724",
            "81769650ec4631f0b6538874aa06da2c01c63a66bce49e365e0a705c99d61b12",
            "e30dd66d2216eab6198bc63a875498911a01d7bf49bd778859971a7297f24129",
            "a1a12c5b3da28b43a766c5ad9f6accc30f07000165702b4337a80dbc279d6b6d",
            "2fb16d177975f9f5de0a05851769e91c0815d921000000000000000000000000",
            "0000",
        ),
        commitments: &[
            "6e576678f004840e5d985c821616671a1c636ac4a8487cac528bd6c3b2a63a3b",
            "b775b24a3500a4c034d40b6b6c11a63caa3153c4ec3ab44e76295161e2542c03",
            "ea97afc55706fbb506b90c0702e50f7eb5a99a696baa365087cd193ba5052c24",
            "8c9b6bfaf5a544f97b21463e838587a387a3fab93ff5ec9f300d71a988f67b2f",
            "379356a06da730685a223bb8dac4259c71dc29c32c63be089e6a4de72aab3313",
            "8be69cc00f79499b8d6f71d0bf4e10f821524525114e9c06e0dcefeb5e6d7823",
            "543aa8bccd8137bd2dbe0727e4402fd4114dae92ec8db3560eedd7c050459e3d",
            "4344e9eb2033d228da0dc78991d910861723f347cd1fb739452ad35b94be6e1c",
        ],
        final_root: "be52e7fc531c373879d5211658e6f2b777571ac4b318b3b6e32ee635e439ac2a",
        subtree_root: "78fccfdeba6dcd684fa879326e6822f59cd0a22c0aaa6df1e5596a877f870128",
    },
    // Block 000020d5cf6083d2554ef7dcae0fadecb7472c4be66d3fe55209da487d89f58b.
    KnownSubtreeCompletion {
        protocol: ShieldedProtocol::Ironwood,
        subtree_index: 11,
        completing_height: 4_555_302,
        predecessor_final_root: "91f2ed8d741417e50e55409efae4551c9d06fd791f4b9dfbea2f977666121d15",
        predecessor_final_state: concat!(
            "014e589d7441da262fab120df006dea0574df2baec97f79a30c4f22f47d1fb61",
            "200122238adc9445a34884d2ac96feea55b946a056594c968c531b4220f71d95",
            "5c281f000158fa024b9156fbbbfa0f7d9c783ad0cea511f134132b48fce1ea35",
            "64da1deb350001eb63c13fd923d4699427e2d61acfd534b92e3be18a06bc3398",
            "1a62d0adc8e23a019302dbd7f61798bd96b723110327361a3f4630d3a3cf884f",
            "f1d56b13955d772d01da4c9a9edbd26b26aa5026f5b16bbea7fe45fa7a2fdb02",
            "ce22ee6d8e111e130c01aaca5740bee969734daffb31f48b6f97922e94e55cc0",
            "8292d598394fcbc7370b016a413cc9941bf3e1e5008c2a893ff45ed1580946dc",
            "fb6320eb1bd4d9c44d6f14018174d558033df6128abbd1648821f6cb558b0e22",
            "fc86792c69beffcde1148913013e464e2bdecfe42304c5cf17ccc08d201e5e79",
            "0c9016064374a7c64852bf400d013faa7ccdde784805efdfc9352f7bfc938f9d",
            "ab3f090d930de9d5671caa967d1b013ca2b3839a3dcd61d9bd0c3578cc58d8c2",
            "cf450051382f756a9c5101de93b704016cd438e32662077d917d4baa3cc7a162",
            "3cb59330880be14bb955c72b8bd1b43401a304db20121dee5ae9f4d29fdba5e3",
            "e500950c3d6ca5a01c4423c0c837448b07018fba562e46f9789e7912109de0a3",
            "889777ae9328e2e4b97880ef44b2ea19191b0177ca8152914f31c4eeb61c4a47",
            "33424bf16325dd67ee7fb08e0d5b85722c631a01a4d6a8741fb1a14cb2b0c924",
            "a1fc37cc30e91c7f75207627078c5b41d99878380001578f59ce0221fc193049",
            "78e07ee85b33d9d6e9b1c269db185985472b85b1813700000000000000000000",
            "0000",
        ),
        commitments: &[
            "532562d7296ddd1efcd1d0f476a926021466f684290655ca078de16ddec6ce3d",
            "c5caf141a58d0225b4f422226e8f9978fa4263a36977932ed9cbd3b6e7d30a33",
            "7707aed6c4fcb196d3a0a4e15f72d405659ab272c3f2469d1cf1332414bd6109",
            "7e3af5f7c40a587e81833f8feebd2fd41e986042f5b7040ff2af648ff0cbb901",
            "9cfb0f7da30d3da7507ffcca01d69899f2ca99aab24d613f2ce7c2b482315f05",
            "cd1050cd6487679c7aec983d243bc87c726d32494fa407135cdbb886a168ff1c",
            "b8ceb9f7e2b487cdeb90b27852d6c2cc5aa921744f1e6582724bbf655664763b",
            "8563af972ac1bcfe9bcdf66fc41600f1e6e458c0d539b40495d6b57af8eb162e",
            "af0938d634fe9bf005432ef2391673a1b14c8314d5068b2982d7caf41aecf529",
            "fc544ae2b3e98b041a5aad49f45a7fc43f45b6c0ba58e73f6d558c3812feac29",
            "b06b6c41836799cf5d901d876fe7656253810e961ddd02ab9b3cc00f72f04d10",
            "bbba0d6abcc20aeb5d3e8ed0ef619f996b70b32e3dc86ec51b092ecc9adf5a18",
            "be1c96df1008f3c070c321177742471c41c88705ddb61b11ed4173f061175801",
        ],
        final_root: "77efaaa7c781892f963d99659f88621f52acf4eb6ca0f8a5e5ffee9ef3b6552d",
        subtree_root: "266b1f775a182dd712fac4eab0a2926a57506b316010a2043ca22141e5ba7607",
    },
];
