//! Overlapped canonical point reads for hydration.
//!
//! Canonical prevout resolution and prevout-cache warm-up issue many
//! independent reads against the canonical secondary. On network or
//! virtualized storage each read waits on device latency, so issuing them one
//! at a time leaves replay bound by that latency rather than by bandwidth or
//! CPU. Splitting the work across a few scoped threads overlaps the waits and
//! the row decoding without changing which rows are read.

use std::{num::NonZeroUsize, sync::OnceLock, thread};

use crate::IngestError;

/// Most threads one hydration step reads canonical storage with.
const MAX_CANONICAL_READ_WORKERS: usize = 16;

/// Fewest independent reads worth splitting across threads.
const MIN_PARALLEL_READS: usize = 32;

/// Returns how many threads hydration reads canonical storage with.
///
/// The count follows the process's available parallelism, which honors a
/// cgroup CPU quota, capped at [`MAX_CANONICAL_READ_WORKERS`].
pub(super) fn canonical_read_workers() -> usize {
    static WORKERS: OnceLock<usize> = OnceLock::new();
    *WORKERS.get_or_init(|| {
        thread::available_parallelism()
            .map_or(1, NonZeroUsize::get)
            .clamp(1, MAX_CANONICAL_READ_WORKERS)
    })
}

/// Reads `items` in contiguous chunks on up to [`canonical_read_workers`]
/// scoped threads and returns the chunk results in item order.
///
/// Small inputs run on the calling thread. The first failing chunk's error is
/// returned; the other chunks still run to completion first.
pub(super) fn read_in_parallel<T, R>(
    items: &[T],
    read_chunk: impl Fn(&[T]) -> Result<Vec<R>, IngestError> + Sync,
) -> Result<Vec<R>, IngestError>
where
    T: Sync,
    R: Send,
{
    let workers = canonical_read_workers();
    if workers <= 1 || items.len() < MIN_PARALLEL_READS {
        return read_chunk(items);
    }
    let chunk_len = items.len().div_ceil(workers);
    let read_chunk = &read_chunk;
    thread::scope(|scope| {
        let handles = items
            .chunks(chunk_len)
            .map(|chunk| scope.spawn(move || read_chunk(chunk)))
            .collect::<Vec<_>>();
        let mut combined = Vec::with_capacity(items.len());
        let mut first_error = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok(chunk_results)) => combined.extend(chunk_results),
                Ok(Err(error)) => {
                    first_error.get_or_insert(error);
                }
                Err(_) => {
                    first_error.get_or_insert_with(|| IngestError::BlockingTaskFailed {
                        reason: "a canonical read worker panicked".to_owned(),
                    });
                }
            }
        }
        first_error.map_or(Ok(combined), Err)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_results_keep_item_order() -> Result<(), IngestError> {
        let items = (0..1_000_u32).collect::<Vec<_>>();
        let doubled = read_in_parallel(&items, |chunk| {
            Ok(chunk.iter().map(|item| item * 2).collect())
        })?;
        assert_eq!(
            doubled,
            items.iter().map(|item| item * 2).collect::<Vec<_>>()
        );
        Ok(())
    }

    #[test]
    fn a_failing_chunk_fails_the_read() {
        let items = (0..1_000_u32).collect::<Vec<_>>();
        let outcome = read_in_parallel(&items, |chunk| {
            if chunk.contains(&777) {
                return Err(IngestError::MaterializedViewDispatch(
                    "chunk failed".to_owned(),
                ));
            }
            Ok(chunk.to_vec())
        });
        assert!(matches!(
            outcome,
            Err(IngestError::MaterializedViewDispatch(message)) if message == "chunk failed"
        ));
    }
}
