use std::{
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::Relaxed},
    },
    task::{Context, Waker},
};

use eyre::OptionExt as _;
use prost::bytes::Bytes;

use super::{SealedBlock, ShieldedTailBuffer, build_source, fill, quarter_chunk_rows, span};

tokio::task_local! {
    static LIVE: Arc<LivePayloadBytes>;
}

#[derive(Default)]
struct LivePayloadBytes {
    current: AtomicUsize,
    peak: AtomicUsize,
}

struct TrackedPayload {
    bytes: Bytes,
    live: Arc<LivePayloadBytes>,
}

impl AsRef<[u8]> for TrackedPayload {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for TrackedPayload {
    fn drop(&mut self) {
        self.live.current.fetch_sub(self.bytes.len(), Relaxed);
    }
}

// Each allocation is counted once until the final Bytes clone releases its owner.
pub(in crate::published_range::tail) fn track_chunk(
    mut rows: Vec<SealedBlock>,
) -> Vec<SealedBlock> {
    let _ = LIVE.try_with(|live| {
        for row in &mut rows {
            let bytes = std::mem::take(&mut row.payload);
            let held = live.current.fetch_add(bytes.len(), Relaxed) + bytes.len();
            live.peak.fetch_max(held, Relaxed);
            row.payload = Bytes::from_owner(TrackedPayload {
                bytes,
                live: Arc::clone(live),
            });
        }
    });
    rows
}

#[tokio::test]
async fn replacement_holds_two_full_tails_and_one_converted_chunk() -> eyre::Result<()> {
    let live = Arc::<LivePayloadBytes>::default();
    LIVE.scope(Arc::clone(&live), async {
        let buffer = ShieldedTailBuffer::default();
        let initial = quarter_chunk_rows(64);
        let mut next = build_source(64, 2);
        next.native_row_bytes = initial.native_row_bytes;
        let row = initial.largest_payload()?;
        let budget = 8 * row;
        let range = span(33, 64);
        let old = fill(&buffer, &initial, range, budget).await?;
        assert_eq!(old.bytes, budget);
        assert_eq!(live.current.load(Relaxed), budget);

        let replacement = {
            let lock = buffer.0.refresh_lock.lock().await;
            let owners = Arc::strong_count(&old);
            let mut request = pin!(fill(&buffer, &next, range, budget));
            assert!(
                request
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            assert_eq!(Arc::strong_count(&old), owners);
            drop(lock);

            request.await?
        };
        assert!(
            buffer
                .load_full()
                .is_some_and(|tail| Arc::ptr_eq(&tail, &replacement))
        );
        assert_eq!(replacement.bytes, budget);
        assert_eq!(live.current.load(Relaxed), 2 * budget);
        assert_eq!(live.peak.load(Relaxed), 2 * budget + 4 * row);
        assert_eq!(old.rows.len(), replacement.rows.len());
        for (old_row, new_row) in old.rows.iter().zip(&replacement.rows) {
            assert_eq!(old_row.id.height, new_row.id.height);
            assert_ne!(old_row.payload, new_row.payload);
            assert_ne!(old_row.payload.as_ptr(), new_row.payload.as_ptr());
        }

        let pinned = old
            .rows
            .front()
            .ok_or_eyre("missing old row")?
            .payload
            .clone();
        drop(old);
        assert_eq!(live.current.load(Relaxed), budget + pinned.len());
        drop(pinned);
        assert_eq!(live.current.load(Relaxed), budget);
        drop(replacement);
        drop(buffer);
        assert_eq!(live.current.load(Relaxed), 0);
        Ok(())
    })
    .await
}
