//! The slots of one receive pool, and which slot a new request may take when
//! every slot is held.
//!
//! A full pool admits a new request by evicting the oldest slot of the client
//! holding the most, as long as that client holds strictly more slots than
//! the requester. One client that opens streams and sends nothing therefore
//! cannot lock other clients out; it only loses its own oldest slots to them.
//! A request whose client already holds as many slots as any other is
//! refused, so when clients hold equal shares, a further request from any of
//! them is refused.
//!
//! Each pool reports how many slots it holds, how many it held once it
//! admitted a request, and how long each request held its slot.

use std::{
    net::SocketAddr,
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

use super::{
    RECEIVE_SLOT_HOLD_DURATION_METRIC, RECEIVE_SLOTS_HELD_AT_ADMISSION_METRIC,
    RECEIVE_SLOTS_HELD_METRIC, ReceivePool,
};

/// The slots of one receive pool, shared by every connection.
#[derive(Debug)]
pub(super) struct ReceiveSlots {
    capacity: NonZeroUsize,
    pool: ReceivePool,
    table: Mutex<SlotTable>,
}

#[derive(Debug, Default)]
struct SlotTable {
    next_slot_id: u64,
    /// Held slots, oldest first.
    held: Vec<HeldSlot>,
}

#[derive(Debug)]
struct HeldSlot {
    id: u64,
    holder: Holder,
    eviction: CancellationToken,
    claimed_at: Instant,
}

/// Whose slots count together when the pool picks one to evict.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Holder {
    /// A client, identified by its connection's remote address or by the
    /// client connection a trusted proxy names.
    Client(SocketAddr),
    /// A request whose transport reported no remote address. It counts as a
    /// client of its own.
    Unidentified { slot_id: u64 },
}

/// A request admitted into a pool.
#[derive(Debug)]
pub(super) struct Admission {
    /// The slot the request holds until it is dropped.
    pub(super) slot: ReceiveSlot,
    /// Whether another client's request lost its slot to admit this one.
    pub(super) evicted_another: bool,
}

/// A held receive slot, returned to its pool when dropped.
#[derive(Debug)]
pub(super) struct ReceiveSlot {
    slots: Arc<ReceiveSlots>,
    id: u64,
    eviction: CancellationToken,
}

impl ReceiveSlots {
    pub(super) fn new(capacity: NonZeroUsize, pool: ReceivePool) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            pool,
            table: Mutex::new(SlotTable::default()),
        })
    }

    /// Claims a slot for a request from `client`, evicting another client's
    /// oldest slot when the pool is full and the fairness rule allows it.
    /// Returns `None` when the request is refused.
    pub(super) fn claim(self: &Arc<Self>, client: Option<SocketAddr>) -> Option<Admission> {
        let mut table = self.table.lock();
        let id = table.next_slot_id;
        let holder = client.map_or(Holder::Unidentified { slot_id: id }, Holder::Client);
        let claimed_at = Instant::now();
        let evicted_hold = if table.held.len() >= self.capacity.get() {
            let index = table.eviction_candidate(holder)?;
            let evicted = table.held.remove(index);
            evicted.eviction.cancel();
            Some(claimed_at.saturating_duration_since(evicted.claimed_at))
        } else {
            None
        };
        table.next_slot_id = id.wrapping_add(1);
        let eviction = CancellationToken::new();
        table.held.push(HeldSlot {
            id,
            holder,
            eviction: eviction.clone(),
            claimed_at,
        });
        let held = table.held.len();
        drop(table);
        metrics::histogram!(RECEIVE_SLOTS_HELD_AT_ADMISSION_METRIC, "pool" => self.pool.label())
            .record(f64::from(u32::try_from(held).unwrap_or(u32::MAX)));
        if let Some(hold) = evicted_hold {
            // The evicted request's slot passed to this one, so the pool
            // holds as many slots as before.
            self.record_hold(hold);
        } else {
            metrics::gauge!(RECEIVE_SLOTS_HELD_METRIC, "pool" => self.pool.label()).increment(1.0);
        }
        Some(Admission {
            slot: ReceiveSlot {
                slots: Arc::clone(self),
                id,
                eviction,
            },
            evicted_another: evicted_hold.is_some(),
        })
    }

    /// Frees slot `id`, unless another client's request already took it.
    fn release(&self, id: u64) {
        let mut table = self.table.lock();
        let released = table
            .held
            .iter()
            .position(|held| held.id == id)
            .map(|index| table.held.remove(index));
        drop(table);
        if let Some(released) = released {
            metrics::gauge!(RECEIVE_SLOTS_HELD_METRIC, "pool" => self.pool.label()).decrement(1.0);
            self.record_hold(released.claimed_at.elapsed());
        }
    }

    fn record_hold(&self, hold: Duration) {
        metrics::histogram!(RECEIVE_SLOT_HOLD_DURATION_METRIC, "pool" => self.pool.label())
            .record(hold.as_secs_f64());
    }

    #[cfg(test)]
    fn held(&self) -> usize {
        self.table.lock().held.len()
    }
}

impl SlotTable {
    /// The oldest slot of the holder with the most slots, ties going to the
    /// holder whose oldest slot is older, provided it holds more slots than
    /// `requester` does.
    fn eviction_candidate(&self, requester: Holder) -> Option<usize> {
        let requester_holds = self.slots_held_by(requester);
        let mut busiest: Option<(usize, usize)> = None;
        for (index, held) in self.held.iter().enumerate() {
            let seen_before = self.held[..index]
                .iter()
                .any(|earlier| earlier.holder == held.holder);
            if seen_before {
                continue;
            }
            let holds = self.slots_held_by(held.holder);
            if busiest.is_none_or(|(_, most)| holds > most) {
                busiest = Some((index, holds));
            }
        }
        busiest
            .filter(|&(_, most)| most > requester_holds)
            .map(|(index, _)| index)
    }

    fn slots_held_by(&self, holder: Holder) -> usize {
        self.held
            .iter()
            .filter(|held| held.holder == holder)
            .count()
    }
}

impl ReceiveSlot {
    /// Completes once another client's request has taken this slot.
    pub(super) fn evicted(&self) -> WaitForCancellationFutureOwned {
        self.eviction.clone().cancelled_owned()
    }

    /// The token cancelled when another client's request takes this
    /// slot.
    pub(super) fn eviction_token(&self) -> CancellationToken {
        self.eviction.clone()
    }
}

impl Drop for ReceiveSlot {
    fn drop(&mut self) {
        self.slots.release(self.id);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{Ipv4Addr, SocketAddr},
        num::NonZeroUsize,
        sync::Arc,
    };

    use metrics_exporter_prometheus::PrometheusBuilder;

    use super::{super::ReceivePool, Admission, ReceiveSlots};

    fn connection(port: u16) -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, port))
    }

    fn pool(capacity: usize) -> Result<Arc<ReceiveSlots>, &'static str> {
        NonZeroUsize::new(capacity)
            .map(|capacity| ReceiveSlots::new(capacity, ReceivePool::Request))
            .ok_or("capacity must be non-zero")
    }

    fn admitted(admission: Option<Admission>) -> Result<Admission, &'static str> {
        admission.ok_or("the request was refused")
    }

    #[test]
    fn a_full_pool_takes_the_oldest_slot_of_the_busiest_connection() -> Result<(), &'static str> {
        let slots = pool(3)?;
        let quiet = admitted(slots.claim(Some(connection(1))))?;
        let busy_oldest = admitted(slots.claim(Some(connection(2))))?;
        let busy_newest = admitted(slots.claim(Some(connection(2))))?;

        let newcomer = admitted(slots.claim(Some(connection(3))))?;

        assert!(newcomer.evicted_another);
        assert!(busy_oldest.slot.eviction.is_cancelled());
        assert!(!busy_newest.slot.eviction.is_cancelled());
        assert!(!quiet.slot.eviction.is_cancelled());
        assert_eq!(slots.held(), 3);
        Ok(())
    }

    #[test]
    fn a_connection_holding_the_most_slots_is_refused() -> Result<(), &'static str> {
        let slots = pool(2)?;
        let _first = admitted(slots.claim(Some(connection(1))))?;
        let _second = admitted(slots.claim(Some(connection(1))))?;

        assert!(slots.claim(Some(connection(1))).is_none());
        Ok(())
    }

    #[test]
    fn connections_holding_equal_shares_refuse_each_other() -> Result<(), &'static str> {
        let slots = pool(2)?;
        let _first = admitted(slots.claim(Some(connection(1))))?;
        let _second = admitted(slots.claim(Some(connection(2))))?;

        assert!(slots.claim(Some(connection(2))).is_none());
        assert!(slots.claim(Some(connection(1))).is_none());
        Ok(())
    }

    #[test]
    fn a_request_takes_a_slot_only_from_a_client_holding_strictly_more() -> Result<(), &'static str>
    {
        let slots = pool(4)?;
        let busy_oldest = admitted(slots.claim(Some(connection(1))))?;
        let _busy = admitted(slots.claim(Some(connection(1))))?;
        let _quiet = admitted(slots.claim(Some(connection(2))))?;
        let _other = admitted(slots.claim(Some(connection(3))))?;

        // The quiet client holds one slot against the busy client's two.
        let quiet_again = admitted(slots.claim(Some(connection(2))))?;
        assert!(quiet_again.evicted_another);
        assert!(busy_oldest.slot.eviction.is_cancelled());
        // The quiet client now holds the most slots, so its next request is
        // refused, although the busy client opened more requests.
        assert!(slots.claim(Some(connection(2))).is_none());
        // A client holding no slot is always admitted while the pool is full.
        assert!(admitted(slots.claim(Some(connection(4))))?.evicted_another);
        Ok(())
    }

    #[test]
    fn a_pool_records_its_occupancy_at_admission_and_each_slot_hold() -> Result<(), &'static str> {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || -> Result<(), &'static str> {
            let slots = pool(2)?;
            let first = admitted(slots.claim(Some(connection(1))))?;
            let _second = admitted(slots.claim(Some(connection(1))))?;
            // A full pool: the newcomer evicts the first slot, which then
            // holds nothing when its request finally drops it.
            let _third = admitted(slots.claim(Some(connection(2))))?;
            drop(first);
            // Refused requests are not admitted, so they record nothing.
            assert!(slots.claim(Some(connection(1))).is_none());
            Ok(())
        })?;

        let exposition = handle.render();
        for expected in [
            "zinder_grpc_request_receive_slots_held_at_admission_count{pool=\"request\"} 3",
            "zinder_grpc_request_receive_slots_held_at_admission_sum{pool=\"request\"} 5",
            // The evicted slot, then the two dropped at the end of the
            // closure.
            "zinder_grpc_request_receive_slot_hold_duration_seconds_count{pool=\"request\"} 3",
        ] {
            assert!(exposition.contains(expected), "{expected}\n{exposition}");
        }
        Ok(())
    }

    #[test]
    fn a_pool_reports_the_slots_it_holds_through_evictions_and_releases() -> Result<(), &'static str>
    {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let held_now = || {
            handle.render().lines().find_map(|line| {
                line.strip_prefix("zinder_grpc_request_receive_slots_held{pool=\"request\"} ")
                    .map(str::to_owned)
            })
        };

        metrics::with_local_recorder(&recorder, || -> Result<(), &'static str> {
            let slots = pool(2)?;
            let first = admitted(slots.claim(Some(connection(1))))?;
            let second = admitted(slots.claim(Some(connection(1))))?;
            assert_eq!(held_now().as_deref(), Some("2"));
            // The newcomer takes the first request's slot, which then frees
            // nothing when that request drops it.
            let newcomer = admitted(slots.claim(Some(connection(2))))?;
            drop(first);
            assert!(slots.claim(Some(connection(1))).is_none());
            assert_eq!(held_now().as_deref(), Some("2"));
            drop(second);
            assert_eq!(held_now().as_deref(), Some("1"));
            drop(newcomer);
            assert_eq!(held_now().as_deref(), Some("0"));
            Ok(())
        })
    }

    #[test]
    fn ties_between_busiest_connections_evict_the_older_slot() -> Result<(), &'static str> {
        let slots = pool(2)?;
        let older = admitted(slots.claim(Some(connection(1))))?;
        let newer = admitted(slots.claim(Some(connection(2))))?;

        let _newcomer = admitted(slots.claim(Some(connection(3))))?;

        assert!(older.slot.eviction.is_cancelled());
        assert!(!newer.slot.eviction.is_cancelled());
        Ok(())
    }

    #[test]
    fn requests_without_a_remote_address_count_as_their_own_connection() -> Result<(), &'static str>
    {
        let slots = pool(2)?;
        let first = admitted(slots.claim(None))?;
        let _second = admitted(slots.claim(None))?;

        let third = admitted(slots.claim(None))?;

        assert!(third.evicted_another);
        assert!(first.slot.eviction.is_cancelled());
        Ok(())
    }

    #[test]
    fn dropping_a_slot_frees_it_and_an_evicted_slot_frees_nothing_twice() -> Result<(), &'static str>
    {
        let slots = pool(1)?;
        let evicted = admitted(slots.claim(Some(connection(1))))?;
        let holder = admitted(slots.claim(Some(connection(2))))?;
        drop(evicted);
        assert_eq!(slots.held(), 1);

        drop(holder);
        assert_eq!(slots.held(), 0);
        let reclaimed = admitted(slots.claim(Some(connection(1))))?;
        assert!(!reclaimed.evicted_another);
        Ok(())
    }
}
