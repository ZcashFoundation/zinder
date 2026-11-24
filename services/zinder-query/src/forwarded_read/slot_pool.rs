//! Slots shared by several kinds of forwarded read, each with a reserved
//! floor, a cap, and its own first-in, first-out queue of waiting requests.

use std::{collections::VecDeque, sync::Arc, time::Instant as StdInstant};

use parking_lot::Mutex;
use tokio::{sync::oneshot, time::Instant};

use super::{ACTIVE_GAUGE, QUEUED_GAUGE, REFUSALS_COUNTER, RefusalReason, WAIT_HISTOGRAM};

/// The share of a pool one kind of read may use.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ClassShare {
    /// The `kind` metric label.
    pub(super) label: &'static str,
    /// Slots no other kind can take.
    pub(super) reserved: u32,
    /// Most slots the kind holds at once, reserved and shared together.
    pub(super) cap: u32,
    /// Most requests of the kind that wait for a slot at once.
    pub(super) max_waiters: usize,
}

/// A pool of slots split into per-kind reserved floors and one shared
/// elastic remainder.
///
/// A request takes a free slot at once only when no request of its kind
/// waits, so each kind is served in arrival order. A returned slot goes to the
/// oldest waiter that may use it: a reserved slot to its own kind, a shared
/// slot to the kinds in turn.
pub(super) struct SlotPool {
    state: Mutex<PoolState>,
}

struct PoolState {
    classes: Vec<ClassState>,
    elastic_free: u32,
    next_waiter: u64,
    /// The kind a freed shared slot is offered to first, so a shared slot
    /// that several kinds wait for does not always go to the same kind.
    rotation: usize,
}

struct ClassState {
    share: ClassShare,
    active: u32,
    waiters: VecDeque<(u64, oneshot::Sender<PoolSlot>)>,
}

impl ClassState {
    fn record_gauges(&self) {
        metrics::gauge!(ACTIVE_GAUGE, "kind" => self.share.label).set(f64::from(self.active));
        metrics::gauge!(QUEUED_GAUGE, "kind" => self.share.label).set(f64::from(
            u32::try_from(self.waiters.len()).unwrap_or(u32::MAX),
        ));
    }
}

impl PoolState {
    fn can_admit(&self, class: usize) -> bool {
        self.classes.get(class).is_some_and(|state| {
            state.active < state.share.cap
                && (state.active < state.share.reserved || self.elastic_free > 0)
        })
    }

    fn allocate(&mut self, class: usize) {
        let elastic_free = &mut self.elastic_free;
        if let Some(state) = self.classes.get_mut(class) {
            if state.active >= state.share.reserved {
                *elastic_free -= 1;
            }
            state.active += 1;
        }
    }

    fn release(&mut self, class: usize) {
        let elastic_free = &mut self.elastic_free;
        if let Some(state) = self.classes.get_mut(class) {
            state.active -= 1;
            if state.active >= state.share.reserved {
                *elastic_free += 1;
            }
        }
    }

    /// Hands free slots to the oldest waiters that may use them, starting with
    /// `first`, whose slot was just returned.
    fn dispatch(&mut self, pool: &Arc<SlotPool>, first: usize) {
        let count = self.classes.len();
        let mut order = Vec::with_capacity(count + 1);
        order.push(first);
        order.extend((0..count).map(|offset| (self.rotation + offset) % count));
        for class in order {
            while self.can_admit(class) {
                let Some((_, waiter)) = self
                    .classes
                    .get_mut(class)
                    .and_then(|state| state.waiters.pop_front())
                else {
                    break;
                };
                self.allocate(class);
                if let Err(mut unclaimed) = waiter.send(PoolSlot::new(pool, class)) {
                    // The waiter left after it was queued; the slot is still
                    // free, and releasing it here keeps the pool lock.
                    unclaimed.pool = None;
                    self.release(class);
                } else {
                    self.rotation = (class + 1) % count;
                }
            }
            if let Some(state) = self.classes.get(class) {
                state.record_gauges();
            }
        }
    }
}

impl SlotPool {
    /// Builds a pool whose kinds hold `shares`, with `elastic` further slots
    /// any kind may take up to its cap.
    pub(super) fn new(shares: &[ClassShare], elastic: u32) -> Arc<Self> {
        let classes: Vec<_> = shares
            .iter()
            .map(|share| ClassState {
                share: *share,
                active: 0,
                waiters: VecDeque::new(),
            })
            .collect();
        for class in &classes {
            class.record_gauges();
            for reason in RefusalReason::ALL {
                metrics::counter!(
                    REFUSALS_COUNTER,
                    "kind" => class.share.label,
                    "reason" => reason.label(),
                )
                .increment(0);
            }
        }
        Arc::new(Self {
            state: Mutex::new(PoolState {
                classes,
                elastic_free: elastic,
                next_waiter: 0,
                rotation: 0,
            }),
        })
    }

    /// Returns the share of `class`.
    pub(super) fn share(&self, class: usize) -> Option<ClassShare> {
        self.state
            .lock()
            .classes
            .get(class)
            .map(|state| state.share)
    }

    /// Takes a slot for `class`, waiting in arrival order until `deadline`
    /// when none is free.
    ///
    /// Dropping the returned future gives up its place in the queue, or the
    /// slot it was just handed, and records the wait as cancelled.
    pub(super) async fn acquire(
        self: &Arc<Self>,
        class: usize,
        deadline: Instant,
    ) -> Result<PoolSlot, RefusalReason> {
        let started_at = StdInstant::now();
        let label = self.share(class).map_or("unknown", |share| share.label);
        let receiver = match self.enter(class, deadline) {
            Entry::Admitted(slot) => {
                record_wait(label, "admitted", started_at);
                return Ok(slot);
            }
            Entry::Refused(reason) => {
                record_refusal(label, reason, started_at);
                return Err(reason);
            }
            Entry::Queued(id, receiver) => QueuedRequest {
                pool: Arc::clone(self),
                class,
                id,
                receiver: Some(receiver),
                label,
                started_at,
                resolved: false,
            },
        };
        receiver.wait(deadline).await
    }

    fn enter(self: &Arc<Self>, class: usize, deadline: Instant) -> Entry {
        let mut state = self.state.lock();
        let Some(waiting) = state.classes.get(class).map(|state| state.waiters.len()) else {
            return Entry::Refused(RefusalReason::WaitersFull);
        };
        if waiting == 0 && state.can_admit(class) {
            state.allocate(class);
            if let Some(state) = state.classes.get(class) {
                state.record_gauges();
            }
            drop(state);
            return Entry::Admitted(PoolSlot::new(self, class));
        }
        if Instant::now() >= deadline {
            drop(state);
            return Entry::Refused(RefusalReason::WaitTimeout);
        }
        let id = state.next_waiter;
        state.next_waiter += 1;
        let Some(class_state) = state.classes.get_mut(class) else {
            return Entry::Refused(RefusalReason::WaitersFull);
        };
        if waiting >= class_state.share.max_waiters {
            return Entry::Refused(RefusalReason::WaitersFull);
        }
        let (sender, receiver) = oneshot::channel();
        class_state.waiters.push_back((id, sender));
        class_state.record_gauges();
        drop(state);
        Entry::Queued(id, receiver)
    }

    fn leave(&self, class: usize, id: u64) {
        let mut state = self.state.lock();
        if let Some(class_state) = state.classes.get_mut(class) {
            class_state.waiters.retain(|(waiter, _)| *waiter != id);
            class_state.record_gauges();
        }
    }

    fn give_back(self: &Arc<Self>, class: usize) {
        let mut state = self.state.lock();
        state.release(class);
        state.dispatch(self, class);
    }

    #[cfg(test)]
    pub(super) fn counts(&self, class: usize) -> (u32, usize) {
        self.state
            .lock()
            .classes
            .get(class)
            .map_or((0, 0), |state| (state.active, state.waiters.len()))
    }
}

enum Entry {
    Admitted(PoolSlot),
    Refused(RefusalReason),
    Queued(u64, oneshot::Receiver<PoolSlot>),
}

/// A request's place in a kind's queue, given back when it is dropped.
struct QueuedRequest {
    pool: Arc<SlotPool>,
    class: usize,
    id: u64,
    receiver: Option<oneshot::Receiver<PoolSlot>>,
    label: &'static str,
    started_at: StdInstant,
    resolved: bool,
}

impl QueuedRequest {
    async fn wait(mut self, deadline: Instant) -> Result<PoolSlot, RefusalReason> {
        let Some(receiver) = self.receiver.as_mut() else {
            return Err(RefusalReason::WaitTimeout);
        };
        let granted = tokio::time::timeout_at(deadline, receiver).await;
        self.resolved = true;
        if let Ok(Ok(slot)) = granted {
            record_wait(self.label, "admitted", self.started_at);
            return Ok(slot);
        }
        record_refusal(self.label, RefusalReason::WaitTimeout, self.started_at);
        Err(RefusalReason::WaitTimeout)
    }
}

impl Drop for QueuedRequest {
    fn drop(&mut self) {
        // Leave the queue before dropping the receiver, so a slot handed to
        // this request in the meantime is returned outside the pool lock.
        self.pool.leave(self.class, self.id);
        drop(self.receiver.take());
        if !self.resolved {
            record_wait(self.label, "cancelled", self.started_at);
        }
    }
}

/// One taken slot, returned to its pool when dropped.
pub(super) struct PoolSlot {
    pool: Option<Arc<SlotPool>>,
    class: usize,
}

impl PoolSlot {
    fn new(pool: &Arc<SlotPool>, class: usize) -> Self {
        Self {
            pool: Some(Arc::clone(pool)),
            class,
        }
    }
}

impl std::fmt::Debug for PoolSlot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PoolSlot")
            .field("class", &self.class)
            .finish_non_exhaustive()
    }
}

impl Drop for PoolSlot {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            pool.give_back(self.class);
        }
    }
}

fn record_wait(label: &'static str, outcome: &'static str, started_at: StdInstant) {
    metrics::histogram!(WAIT_HISTOGRAM, "kind" => label, "outcome" => outcome)
        .record(started_at.elapsed());
}

fn record_refusal(label: &'static str, reason: RefusalReason, started_at: StdInstant) {
    record_wait(label, "refused", started_at);
    metrics::counter!(REFUSALS_COUNTER, "kind" => label, "reason" => reason.label()).increment(1);
}
