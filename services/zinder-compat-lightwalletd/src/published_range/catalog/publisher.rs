use std::{
    collections::BTreeMap,
    fs::File,
    io,
    num::{NonZeroU32, NonZeroUsize},
    path::{Path, PathBuf},
    sync::mpsc,
};

use parking_lot::{Condvar, Mutex};
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use zinder_core::{BlockHeight, BlockHeightRange};
use zinder_query::{CanonicalReader, RocksDbCanonicalReader};

use super::{
    super::manifest::{
        BuiltSegment, install_signed, invalid, lock_directory, publish_segment, signed_contents,
        signing_key_from, stage_segment, sync_directory,
    },
    CatalogManifest, FORMAT, MAX_SEGMENTS, ShieldedRangeIdentity, check_parent,
    check_parent_hashes, load_entries, open_segment,
};

/// Concurrency and manifest-commit batching bounds for [`ShieldedCatalogPublisher::publish_available`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublicationConcurrency {
    /// Maximum concurrent segment-builder threads (`1` runs sequentially).
    pub workers: NonZeroUsize,
    /// Maximum contiguous segments staged before syncing and re-signing the catalog.
    pub commit_batch: NonZeroUsize,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::published_range) struct PublisherHead {
    count: usize,
    digest: [u8; 32],
}

impl PublisherHead {
    pub(in crate::published_range) fn read(directory: &Path, key: &[u8]) -> io::Result<Self> {
        let path = directory.join(".publisher-head.json");
        Ok(serde_json::from_str(&signed_contents(&path, key, 2048)?)?)
    }

    pub(in crate::published_range) fn verify(
        &self,
        entries: &[ShieldedRangeIdentity],
    ) -> io::Result<()> {
        let prefix = entries.get(..self.count).ok_or_else(|| {
            invalid("signed shielded catalog rolled back below publisher checkpoint")
        })?;
        if Self::of(prefix)?.digest != self.digest {
            return Err(invalid(
                "signed shielded catalog changed the publisher checkpoint",
            ));
        }
        Ok(())
    }

    fn of(entries: &[ShieldedRangeIdentity]) -> io::Result<Self> {
        Ok(Self {
            count: entries.len(),
            digest: Sha256::digest(serde_json::to_vec(entries)?).into(),
        })
    }
}

/// Publishes complete newly settled segments from successive captured canonical readers.
pub struct ShieldedCatalogPublisher {
    _lock: File,
    directory: PathBuf,
    key: Ed25519KeyPair,
    first: BlockHeight,
    segment_blocks: NonZeroU32,
    pub(super) entries: Vec<ShieldedRangeIdentity>,
    needs_reopen: bool,
}

impl ShieldedCatalogPublisher {
    /// Validates that `signing_key` is a readable, size-bounded Ed25519
    /// PKCS#8 signing key file without opening or locking a catalog directory.
    ///
    /// # Errors
    ///
    /// Rejects unreadable paths, non-regular files (such as directories or
    /// FIFOs), keys exceeding the bounded read limit, and malformed PKCS#8
    /// key material.
    pub fn validate_signing_key(signing_key: &Path) -> io::Result<()> {
        let _key = signing_key_from(signing_key)?;
        Ok(())
    }

    /// Resumes the signed catalog in `directory`, or starts at `first` when empty.
    /// Both operations hold one exclusive publisher lease for this directory.
    /// `segment_blocks` sets the number of newly settled heights per publication.
    ///
    /// # Errors
    ///
    /// Rejects another active publisher, invalid keys, rollback below the last durable
    /// publisher checkpoint, missing segments, or gaps in the resumed publication.
    pub fn open(
        directory: &Path,
        signing_key: &Path,
        first: BlockHeight,
        segment_blocks: NonZeroU32,
    ) -> io::Result<Self> {
        let key = signing_key_from(signing_key)?;
        let lock = lock_directory(directory)?;
        let current = directory.join("current.json");
        let entries = if current.try_exists()? {
            load_entries(&current, key.public_key().as_ref())?
        } else {
            Vec::new()
        };
        if entries
            .first()
            .is_some_and(|entry| entry.first != first.value())
        {
            return Err(invalid(
                "configured first height differs from resumed shielded catalog",
            ));
        }
        let head_path = directory.join(".publisher-head.json");
        if head_path.try_exists()? {
            PublisherHead::read(directory, key.public_key().as_ref())?.verify(&entries)?;
        } else if !entries.is_empty() {
            return Err(invalid(
                "missing durable checkpoint for existing shielded catalog",
            ));
        }
        let mut previous = None;
        for entry in &entries {
            let segment = open_segment(directory, entry, previous)?;
            previous = Some(segment.identity());
        }
        let resumed = Self {
            _lock: lock,
            directory: directory.to_owned(),
            key,
            first,
            segment_blocks,
            entries,
            needs_reopen: false,
        };
        resumed.checkpoint()?;
        Ok(resumed)
    }

    fn checkpoint(&self) -> io::Result<()> {
        let head = PublisherHead::of(&self.entries)?;
        install_signed(
            &self.directory.join(".publisher-head.json"),
            serde_json::to_string(&head)?,
            &self.key,
        )
    }

    /// Publishes the next full settled segment, returning false when the remaining suffix is shorter.
    /// Reads only previously unpublished canonical block payloads. This performs blocking file I/O.
    ///
    /// # Errors
    ///
    /// Returns an error on a displaced publication, missing canonical data, or a storage failure.
    pub fn publish_next(&mut self, reader: &RocksDbCanonicalReader) -> io::Result<bool> {
        if self.needs_reopen {
            return Err(invalid(
                "reopen the publisher after a failed catalog installation",
            ));
        }
        let Some(start) = self.next_segment_start(reader)? else {
            return Ok(false);
        };
        let Some(end) = start
            .value()
            .checked_add(self.segment_blocks.get() - 1)
            .map(BlockHeight::new)
        else {
            return Ok(false);
        };
        if end > reader.chain_epoch().settled_tip_height {
            return Ok(false);
        }
        if self.entries.len() >= MAX_SEGMENTS {
            return Err(invalid("shielded catalog segment limit exceeded"));
        }
        let range = BlockHeightRange::inclusive(start, end);
        let segment = publish_segment(reader, &self.directory, range)?;
        check_parent(self.entries.last().copied(), &segment)?;
        self.entries.push(segment.identity());
        self.commit_manifest()?;
        Ok(true)
    }

    /// Publishes every complete newly settled segment reachable from `reader`.
    ///
    /// Returns the number of newly committed segments in ascending contiguous order. When a
    /// prefix succeeds before a later failure, the contiguous prefix commits durably and the
    /// failing error is returned.
    ///
    /// # Errors
    ///
    /// Returns an error on a displaced publication, catalog segment limit overflow, missing
    /// canonical data, or a storage failure.
    pub fn publish_available(
        &mut self,
        reader: &RocksDbCanonicalReader,
        concurrency: PublicationConcurrency,
        stopping: &CancellationToken,
    ) -> io::Result<usize> {
        if self.needs_reopen {
            return Err(invalid(
                "reopen the publisher after a failed catalog installation",
            ));
        }
        if stopping.is_cancelled() {
            return Ok(0);
        }
        if concurrency.workers.get() == 1 && concurrency.commit_batch.get() == 1 {
            let mut published = 0_usize;
            while !stopping.is_cancelled() && self.publish_next(reader)? {
                published += 1;
            }
            return Ok(published);
        }
        self.publish_available_with(reader, concurrency, stopping, &stage_segment)
    }

    pub(super) fn publish_available_with<F>(
        &mut self,
        reader: &RocksDbCanonicalReader,
        concurrency: PublicationConcurrency,
        stopping: &CancellationToken,
        stage: &F,
    ) -> io::Result<usize>
    where
        F: Fn(&RocksDbCanonicalReader, &Path, BlockHeightRange, usize) -> io::Result<BuiltSegment>
            + Sync,
    {
        if self.needs_reopen {
            return Err(invalid(
                "reopen the publisher after a failed catalog installation",
            ));
        }
        if stopping.is_cancelled() {
            return Ok(0);
        }
        let plan = self.plan_ranges(reader)?;
        if plan.ranges.is_empty() {
            if plan.limit_exceeded {
                return Err(invalid("shielded catalog segment limit exceeded"));
            }
            return Ok(0);
        }
        let context = BatchContext {
            reader,
            concurrency,
            stopping,
            stage,
        };
        let committed = if concurrency.workers.get() == 1 || plan.ranges.len() == 1 {
            self.publish_sequential_batched(&plan.ranges, &context)?
        } else {
            self.publish_parallel_batched(&plan.ranges, &context)?
        };
        if plan.limit_exceeded && committed == plan.ranges.len() && !stopping.is_cancelled() {
            return Err(invalid("shielded catalog segment limit exceeded"));
        }
        Ok(committed)
    }

    fn next_segment_start(
        &self,
        reader: &RocksDbCanonicalReader,
    ) -> io::Result<Option<BlockHeight>> {
        let chain = reader.chain_epoch();
        let Some(previous) = self.entries.last().copied() else {
            return Ok(Some(self.first));
        };
        let end = BlockHeight::new(previous.last);
        if previous.network_id != chain.network.id()
            || previous.artifact_schema != chain.artifact_schema_version.value()
            || end > chain.settled_tip_height
            || reader
                .secondary()
                .block_header_at(end)
                .map_err(io::Error::other)?
                .is_none_or(|header| header.block_hash.as_bytes() != previous.last_hash)
        {
            return Err(invalid(
                "previous shielded catalog does not belong to the writer's settled chain",
            ));
        }
        Ok(end.next())
    }

    fn plan_ranges(&self, reader: &RocksDbCanonicalReader) -> io::Result<PlannedRanges> {
        let settled_tip = reader.chain_epoch().settled_tip_height;
        let Some(mut start) = self.next_segment_start(reader)? else {
            return Ok(PlannedRanges {
                ranges: Vec::new(),
                limit_exceeded: false,
            });
        };
        let remaining_capacity = MAX_SEGMENTS.saturating_sub(self.entries.len());
        let mut ranges = Vec::new();
        let mut limit_exceeded = false;
        while let Some(end) = start
            .value()
            .checked_add(self.segment_blocks.get() - 1)
            .map(BlockHeight::new)
        {
            if end > settled_tip {
                break;
            }
            if ranges.len() == remaining_capacity {
                limit_exceeded = true;
                break;
            }
            ranges.push(BlockHeightRange::inclusive(start, end));
            let Some(next_start) = end.next() else {
                break;
            };
            start = next_start;
        }
        Ok(PlannedRanges {
            ranges,
            limit_exceeded,
        })
    }

    fn commit_manifest(&mut self) -> io::Result<()> {
        let catalog = CatalogManifest {
            format: FORMAT.to_owned(),
            segments: &self.entries,
        };
        let current = self.directory.join("current.json");
        // A rename may be visible even if the subsequent directory fsync fails.
        // Resume from the signed files before attempting another publication.
        self.needs_reopen = true;
        install_signed(&current, serde_json::to_string(&catalog)?, &self.key)?;
        self.checkpoint()?;
        self.needs_reopen = false;
        Ok(())
    }

    fn commit_batch(&mut self, batch: &mut Vec<BuiltSegment>) -> io::Result<usize> {
        let count = batch.len();
        if count == 0 {
            return Ok(0);
        }
        sync_directory(&self.directory)?;
        for built in batch.drain(..) {
            self.entries.push(built.identity);
        }
        self.commit_manifest()?;
        Ok(count)
    }

    fn publish_sequential_batched<F>(
        &mut self,
        ranges: &[BlockHeightRange],
        context: &BatchContext<'_, F>,
    ) -> io::Result<usize>
    where
        F: Fn(&RocksDbCanonicalReader, &Path, BlockHeightRange, usize) -> io::Result<BuiltSegment>,
    {
        let directory = self.directory.clone();
        let commit_limit = context.concurrency.commit_batch.get();
        let mut batch = Vec::with_capacity(commit_limit.min(ranges.len()));
        let mut committed = 0_usize;
        for &range in ranges {
            if context.stopping.is_cancelled() {
                break;
            }
            let built = match (context.stage)(context.reader, &directory, range, 0) {
                Ok(built) => built,
                Err(error) => {
                    self.commit_batch(&mut batch)?;
                    return Err(error);
                }
            };
            let prior = batch
                .last()
                .map(|previous| previous.identity)
                .or_else(|| self.entries.last().copied());
            if let Err(error) = check_parent_hashes(prior, built.first_parent) {
                self.commit_batch(&mut batch)?;
                return Err(error);
            }
            batch.push(built);
            if batch.len() >= commit_limit {
                committed += self.commit_batch(&mut batch)?;
            }
        }
        committed += self.commit_batch(&mut batch)?;
        Ok(committed)
    }

    fn publish_parallel_batched<F>(
        &mut self,
        ranges: &[BlockHeightRange],
        context: &BatchContext<'_, F>,
    ) -> io::Result<usize>
    where
        F: Fn(&RocksDbCanonicalReader, &Path, BlockHeightRange, usize) -> io::Result<BuiltSegment>
            + Sync,
    {
        let worker_count = context.concurrency.workers.get().min(ranges.len());
        let commit_limit = context.concurrency.commit_batch.get();
        let max_in_flight = commit_limit.max(worker_count.saturating_mul(2));
        let queue = JobQueue::new(ranges.len(), max_in_flight);
        let directory = self.directory.clone();
        let (sender, receiver) = mpsc::channel();
        std::thread::scope(|scope| {
            let _stop_on_exit = StopOnDrop(&queue);
            for slot in 0..worker_count {
                let worker_sender = sender.clone();
                let worker_reader = context.reader.clone();
                let worker_dir = directory.as_path();
                let queue_ref = &queue;
                let stage = context.stage;
                let stopping = context.stopping;
                scope.spawn(move || {
                    let _panic_guard = PanicStopOnDrop(queue_ref);
                    while let Some(index) = queue_ref.take_next(stopping) {
                        let built = stage(&worker_reader, worker_dir, ranges[index], slot);
                        let failed = built.is_err();
                        if worker_sender.send((index, built)).is_err() || failed {
                            break;
                        }
                    }
                });
            }
            drop(sender);
            let mut committer = BatchCommitter::new(self, commit_limit, &queue);
            while let Ok((index, built)) = receiver.recv() {
                if let Err(error) = committer.record_outcome(index, built) {
                    queue.stop();
                    return Err(error);
                }
                if context.stopping.is_cancelled() {
                    queue.stop();
                }
            }
            committer.finish()
        })
    }
}

struct PlannedRanges {
    ranges: Vec<BlockHeightRange>,
    limit_exceeded: bool,
}

struct BatchContext<'a, F> {
    reader: &'a RocksDbCanonicalReader,
    concurrency: PublicationConcurrency,
    stopping: &'a CancellationToken,
    stage: &'a F,
}

struct JobQueueState {
    next: usize,
    total: usize,
    committed_floor: usize,
    max_in_flight: usize,
    aborted_from: Option<usize>,
    stopped: bool,
}

struct JobQueue {
    state: Mutex<JobQueueState>,
    changed: Condvar,
}

impl JobQueue {
    fn new(total: usize, max_in_flight: usize) -> Self {
        Self {
            state: Mutex::new(JobQueueState {
                next: 0,
                total,
                committed_floor: 0,
                max_in_flight,
                aborted_from: None,
                stopped: false,
            }),
            changed: Condvar::new(),
        }
    }

    fn take_next(&self, stopping: &CancellationToken) -> Option<usize> {
        let mut guard = self.state.lock();
        loop {
            if stopping.is_cancelled() || guard.stopped {
                let transitioned = !guard.stopped;
                guard.stopped = true;
                drop(guard);
                if transitioned {
                    self.changed.notify_all();
                }
                return None;
            }
            if guard.aborted_from.is_some_and(|limit| guard.next >= limit)
                || guard.next >= guard.total
            {
                drop(guard);
                return None;
            }
            if guard.next < guard.committed_floor.saturating_add(guard.max_in_flight) {
                let index = guard.next;
                guard.next += 1;
                drop(guard);
                return Some(index);
            }
            self.changed.wait(&mut guard);
        }
    }

    fn advance_floor(&self, floor: usize) {
        let mut guard = self.state.lock();
        if floor > guard.committed_floor {
            guard.committed_floor = floor;
            drop(guard);
            self.changed.notify_all();
        }
    }

    fn abort_from(&self, index: usize) {
        let mut guard = self.state.lock();
        if guard.aborted_from.is_none_or(|existing| index < existing) {
            guard.aborted_from = Some(index);
            drop(guard);
            self.changed.notify_all();
        }
    }

    fn stop(&self) {
        let mut guard = self.state.lock();
        guard.stopped = true;
        drop(guard);
        self.changed.notify_all();
    }
}

struct StopOnDrop<'a>(&'a JobQueue);

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        self.0.stop();
    }
}

struct PanicStopOnDrop<'a>(&'a JobQueue);

impl Drop for PanicStopOnDrop<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.stop();
        }
    }
}

struct BatchCommitter<'a> {
    publisher: &'a mut ShieldedCatalogPublisher,
    commit_batch: usize,
    queue: &'a JobQueue,
    next_to_commit: usize,
    completed: BTreeMap<usize, BuiltSegment>,
    batch: Vec<BuiltSegment>,
    committed: usize,
    earliest_error: Option<(usize, io::Error)>,
}

impl<'a> BatchCommitter<'a> {
    fn new(
        publisher: &'a mut ShieldedCatalogPublisher,
        commit_batch: usize,
        queue: &'a JobQueue,
    ) -> Self {
        Self {
            publisher,
            commit_batch,
            queue,
            next_to_commit: 0,
            completed: BTreeMap::new(),
            batch: Vec::new(),
            committed: 0,
            earliest_error: None,
        }
    }

    fn record_outcome(
        &mut self,
        index: usize,
        outcome: io::Result<BuiltSegment>,
    ) -> io::Result<()> {
        match outcome {
            Ok(built) => {
                if self
                    .earliest_error
                    .as_ref()
                    .is_none_or(|(err_index, _)| index < *err_index)
                {
                    self.completed.insert(index, built);
                }
            }
            Err(error) => self.record_error(index, error),
        }
        self.drain_ready()
    }

    fn record_error(&mut self, index: usize, error: io::Error) {
        if self
            .earliest_error
            .as_ref()
            .is_none_or(|(existing, _)| index < *existing)
        {
            self.earliest_error = Some((index, error));
        }
        self.queue.abort_from(index);
    }

    fn drain_ready(&mut self) -> io::Result<()> {
        while self
            .earliest_error
            .as_ref()
            .is_none_or(|(err_index, _)| self.next_to_commit < *err_index)
        {
            let Some(built) = self.completed.remove(&self.next_to_commit) else {
                break;
            };
            let prior = self
                .batch
                .last()
                .map(|previous| previous.identity)
                .or_else(|| self.publisher.entries.last().copied());
            if let Err(error) = check_parent_hashes(prior, built.first_parent) {
                self.record_error(self.next_to_commit, error);
                break;
            }
            self.batch.push(built);
            self.next_to_commit += 1;
            self.queue.advance_floor(self.next_to_commit);
            if self.batch.len() >= self.commit_batch {
                self.committed += self.publisher.commit_batch(&mut self.batch)?;
            }
        }
        Ok(())
    }

    fn finish(mut self) -> io::Result<usize> {
        self.committed += self.publisher.commit_batch(&mut self.batch)?;
        if let Some((_, error)) = self.earliest_error.take() {
            return Err(error);
        }
        Ok(self.committed)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::{Arc, mpsc},
        time::Duration,
    };

    use tokio_util::sync::CancellationToken;

    use super::JobQueue;

    #[test]
    fn job_queue_wakes_window_waiters_when_stopping_transitions() -> io::Result<()> {
        let queue = Arc::new(JobQueue::new(4, 2));
        let stopping = CancellationToken::new();
        assert_eq!(queue.take_next(&stopping), Some(0));
        assert_eq!(queue.take_next(&stopping), Some(1));

        let (entered_tx, entered_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let waiter_queue = Arc::clone(&queue);
        let waiter_stopping = stopping.clone();
        let waiter = std::thread::spawn(move || {
            let _ = entered_tx.send(());
            let next = waiter_queue.take_next(&waiter_stopping);
            let _ = done_tx.send(next);
        });

        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .map_err(io::Error::other)?;
        // Let the waiter enter `Condvar::wait` before cancelling.
        std::thread::sleep(Duration::from_millis(20));

        stopping.cancel();
        // Calling `take_next` first transitions `guard.stopped` before `stop()` runs.
        assert_eq!(queue.take_next(&stopping), None);
        queue.stop();

        let waiter_next = done_rx
            .recv_timeout(Duration::from_secs(2))
            .map_err(io::Error::other)?;
        assert_eq!(waiter_next, None);
        waiter
            .join()
            .map_err(|_| io::Error::other("job queue waiter thread panicked"))
    }
}
