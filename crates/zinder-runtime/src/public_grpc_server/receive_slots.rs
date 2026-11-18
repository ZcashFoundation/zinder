//! The slots of one receive pool, and which slot a new request may take when
//! every slot is held.
//!
//! A full pool admits a new request by evicting the oldest slot of the client
//! connection holding the most, as long as that connection holds more slots
//! than the requester's. One connection that opens streams and sends nothing
//! therefore cannot lock other connections out; it only loses its own oldest
//! slots to them. A request whose connection already holds as many slots as
//! any other is refused.

use std::{net::SocketAddr, num::NonZeroUsize, sync::Arc};

use parking_lot::Mutex;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

/// The slots of one receive pool, shared by every connection.
#[derive(Debug)]
pub(super) struct ReceiveSlots {
    capacity: NonZeroUsize,
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
}

/// Whose slots count together when the pool picks one to evict.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Holder {
    /// A client connection, identified by its remote address.
    Connection(SocketAddr),
    /// A request whose transport reported no remote address. It counts as a
    /// connection of its own.
    Unidentified { slot_id: u64 },
}

/// A request admitted into a pool.
#[derive(Debug)]
pub(super) struct Admission {
    /// The slot the request holds until it is dropped.
    pub(super) slot: ReceiveSlot,
    /// Whether another connection's request lost its slot to admit this one.
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
    pub(super) fn new(capacity: NonZeroUsize) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            table: Mutex::new(SlotTable::default()),
        })
    }

    /// Claims a slot for a request from `remote_address`, evicting another
    /// connection's oldest slot when the pool is full and the fairness rule
    /// allows it. Returns `None` when the request is refused.
    pub(super) fn claim(self: &Arc<Self>, remote_address: Option<SocketAddr>) -> Option<Admission> {
        let mut table = self.table.lock();
        let id = table.next_slot_id;
        let holder =
            remote_address.map_or(Holder::Unidentified { slot_id: id }, Holder::Connection);
        let evicted_another = table.held.len() >= self.capacity.get();
        if evicted_another {
            let index = table.eviction_candidate(holder)?;
            table.held.remove(index).eviction.cancel();
        }
        table.next_slot_id = id.wrapping_add(1);
        let eviction = CancellationToken::new();
        table.held.push(HeldSlot {
            id,
            holder,
            eviction: eviction.clone(),
        });
        drop(table);
        Some(Admission {
            slot: ReceiveSlot {
                slots: Arc::clone(self),
                id,
                eviction,
            },
            evicted_another,
        })
    }

    fn release(&self, id: u64) {
        self.table.lock().held.retain(|held| held.id != id);
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
    /// Completes once another connection's request has taken this slot.
    pub(super) fn evicted(&self) -> WaitForCancellationFutureOwned {
        self.eviction.clone().cancelled_owned()
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

    use super::{Admission, ReceiveSlots};

    fn connection(port: u16) -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, port))
    }

    fn pool(capacity: usize) -> Result<Arc<ReceiveSlots>, &'static str> {
        NonZeroUsize::new(capacity)
            .map(ReceiveSlots::new)
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
