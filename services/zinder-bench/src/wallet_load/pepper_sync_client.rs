//! Request evidence from the Zingolib revision shipped by Zingo Mobile.

use std::{
    error::Error,
    path::{Path, PathBuf},
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use tokio::process::Command;
use zinder_core::{SUBTREE_LEAF_COUNT, ShieldedProtocol as Pool};
use zinder_proto::compat::lightwalletd::{PoolType, ShieldedProtocol, SubtreeRoot};
use zinder_testkit::{FIXTURE_NOTE_COMMITMENT, frontier_of_size};

use super::mock_streamer::{self, MockStreamer, RangeEnding};

const BIRTHDAY: u64 = 3_424_811;
const TIP: u64 = BIRTHDAY + 63;
const PUBLIC_TEST_VECTOR_SEED: &str = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon art";

#[tokio::test]
#[ignore = "requires the pinned Zingo CLI build described in the zinder-bench README"]
async fn pepper_sync_fresh_restore_requests() -> Result<(), Box<dyn Error>> {
    let binary = std::env::var_os("ZINDER_TEST_PEPPER_SYNC_BIN")
        .map(PathBuf::from)
        .ok_or("set ZINDER_TEST_PEPPER_SYNC_BIN to the pinned Zingo CLI executable")?;
    let mut fixture = MockStreamer::new(RangeEnding::Clean);
    fixture.first_range_deadline_after = Some(4);
    fixture.tip_height = TIP;
    fixture.mainnet_sapling_frontier =
        Some(frontier_of_size(Pool::Sapling, SUBTREE_LEAF_COUNT + 1)?);
    fixture.subtree_roots.insert(
        ShieldedProtocol::Sapling as i32,
        vec![SubtreeRoot {
            root_hash: FIXTURE_NOTE_COMMITMENT.to_vec(),
            completing_block_hash: mock_streamer::tip_hash(BIRTHDAY - 2),
            completing_block_height: BIRTHDAY - 2,
        }],
    );
    let requests = Arc::clone(&fixture.requests);
    let server = mock_streamer::serve(fixture).await?;
    restore_wallet(&binary, &server.url).await?;

    assert_eq!(requests.connections.lock().len(), 1);
    assert_eq!(requests.mempool_streams.load(Ordering::Relaxed), 1);
    let heights = requests.tree_state_heights.lock().clone();
    assert_eq!(
        heights.get(..3),
        Some([BIRTHDAY - 1, TIP, BIRTHDAY].as_slice())
    );
    assert!(heights[3..].iter().all(|&height| height == BIRTHDAY));
    assert_eq!(
        *requests.transparent_history_ranges.lock(),
        [(BIRTHDAY - 1_000, TIP); 21]
    );
    let mut roots = requests.subtree_root_requests.lock().clone();
    roots.sort_unstable();
    assert_eq!(
        roots,
        [
            (ShieldedProtocol::Sapling as i32, 0, 0),
            (ShieldedProtocol::Sapling as i32, 1, 0),
            (ShieldedProtocol::Orchard as i32, 0, 0),
            (ShieldedProtocol::Ironwood as i32, 0, 0),
        ]
    );
    assert_eq!(
        *requests.block_ranges.lock(),
        [
            (BIRTHDAY, BIRTHDAY + 9),
            (BIRTHDAY + 4, BIRTHDAY + 9),
            (BIRTHDAY + 10, TIP),
        ]
    );
    let shielded = [
        PoolType::Sapling as i32,
        PoolType::Orchard as i32,
        PoolType::Ironwood as i32,
    ];
    assert_eq!(*requests.block_range_pool_types.lock(), [shielded; 3]);
    Ok(())
}

async fn restore_wallet(binary: &Path, endpoint: &str) -> Result<(), Box<dyn Error>> {
    let wallet = tempfile::tempdir()?;
    let output = tokio::time::timeout(
        Duration::from_secs(45),
        Command::new(binary)
            .kill_on_drop(true)
            .env("ZINGO_SEED", PUBLIC_TEST_VECTOR_SEED)
            .args(["--chain", "mainnet", "--server", endpoint, "--data-dir"])
            .arg(wallet.path())
            .args([
                "--birthday",
                &BIRTHDAY.to_string(),
                "--waitsync",
                "sync",
                "status",
            ])
            .output(),
    )
    .await??;
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        output.status.success(),
        "Zingo CLI exited with {}",
        output.status
    );
    assert!(!stderr.to_ascii_lowercase().contains("error:"));
    assert!(stderr.contains("Zingo CLI quit successfully."));
    let status: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(status["session_blocks_scanned"], TIP - BIRTHDAY + 1);
    Ok(())
}
