//! Appends a batch of leaves to a frontier, hashing on rayon's current pool.
//!
//! The leaves before the new tip are cut into aligned complete subtrees, whose
//! roots are hashed independently and then merged, in leaf order, into the
//! roots of the complete subtrees that already hold the tree's leaves. Every
//! tree node is hashed exactly once, as in a serial append, so the frontier and
//! the subtree roots it completes are those of appending the leaves one at a
//! time.

use incrementalmerkletree::{Hashable, Level, Position, frontier::Frontier};
use rayon::prelude::*;

use super::{NOTE_COMMITMENT_TREE_DEPTH, SUBTREE_ROOT_LEVEL};

/// Complete subtrees at or below this level are hashed on one thread, because
/// splitting them costs more than the few hashes they hold.
const SEQUENTIAL_SUBTREE_LEVEL: u8 = 4;

/// The frontier after a batch, with the roots of the `2^16`-leaf subtrees the
/// batch completed, by subtree index in completion order.
pub(super) struct BatchAppend<Node> {
    pub(super) frontier: Frontier<Node, NOTE_COMMITMENT_TREE_DEPTH>,
    pub(super) completed_subtree_roots: Vec<(u32, Node)>,
}

/// Appends `leaves` after `frontier`, or returns `None` if the merged roots do
/// not form a frontier, which would mean this module is wrong.
///
/// The caller has already checked that the tree has room for every leaf.
pub(super) fn append_batch<Node>(
    frontier: &Frontier<Node, NOTE_COMMITMENT_TREE_DEPTH>,
    leaves: &[Node],
) -> Option<BatchAppend<Node>>
where
    Node: Hashable + Clone + Send + Sync,
{
    let Some((tip_leaf, before_tip)) = leaves.split_last() else {
        return Some(BatchAppend {
            frontier: frontier.clone(),
            completed_subtree_roots: Vec::new(),
        });
    };
    let first_position = frontier.tree_size();
    let tip_position = first_position + u64::try_from(before_tip.len()).ok()?;
    let mut roots = CompleteSubtreeRoots::of(frontier);
    let chunks = aligned_chunks(first_position, tip_position);
    let hashed = chunks
        .par_iter()
        .map(|chunk| {
            let offset = usize::try_from(chunk.position - first_position).ok()?;
            let leaves = before_tip.get(offset..offset + (1_usize << chunk.level))?;
            Some(subtree_root(leaves, chunk.level))
        })
        .collect::<Option<Vec<_>>>()?;

    let mut completed_subtree_roots = Vec::new();
    for (chunk, (root, inner_subtree_roots)) in chunks.iter().zip(hashed) {
        let first_subtree = u32::try_from(chunk.position >> SUBTREE_ROOT_LEVEL).ok()?;
        for (subtree, inner_root) in (first_subtree..).zip(inner_subtree_roots) {
            completed_subtree_roots.push((subtree, inner_root));
        }
        let last_position = chunk.position + (1_u64 << chunk.level) - 1;
        if let Some(completed) = roots.merge(chunk.level, root) {
            let subtree = u32::try_from(last_position >> SUBTREE_ROOT_LEVEL).ok()?;
            completed_subtree_roots.push((subtree, completed));
        }
    }

    // The leaves before the tip are exactly the ommers' subtrees.
    let tip = Position::from(tip_position);
    let frontier = Frontier::from_parts(tip, tip_leaf.clone(), roots.into_ommers()).ok()?;
    let subtree_level = Level::from(SUBTREE_ROOT_LEVEL);
    if tip.is_complete_subtree(subtree_level) {
        let subtree = u32::try_from(tip_position >> SUBTREE_ROOT_LEVEL).ok()?;
        completed_subtree_roots.push((subtree, frontier.value()?.root(Some(subtree_level))));
    }
    Some(BatchAppend {
        frontier,
        completed_subtree_roots,
    })
}

/// One aligned complete subtree of a batch: `2^level` leaves from `position`.
struct AlignedChunk {
    position: u64,
    level: u8,
}

/// Cuts the positions `start..end` into the largest aligned complete
/// subtrees, in leaf order.
fn aligned_chunks(start: u64, end: u64) -> Vec<AlignedChunk> {
    let mut chunks = Vec::new();
    let mut position = start;
    while position < end {
        let alignment = position
            .trailing_zeros()
            .min(u32::from(NOTE_COMMITMENT_TREE_DEPTH));
        let fitting = (end - position).ilog2();
        let level = alignment.min(fitting);
        chunks.push(AlignedChunk {
            position,
            // A level below the tree depth always fits.
            level: u8::try_from(level).unwrap_or(NOTE_COMMITMENT_TREE_DEPTH),
        });
        position += 1_u64 << level;
    }
    chunks
}

/// Hashes `2^level` leaves to their subtree root, and returns it with the
/// roots of the `2^16`-leaf subtrees inside it in leaf order.
fn subtree_root<Node>(leaves: &[Node], level: u8) -> (Node, Vec<Node>)
where
    Node: Hashable + Clone + Send + Sync,
{
    if level <= SEQUENTIAL_SUBTREE_LEVEL {
        return (sequential_subtree_root(leaves), Vec::new());
    }
    let (left_leaves, right_leaves) = leaves.split_at(leaves.len() / 2);
    let ((left, mut subtree_roots), (right, right_subtree_roots)) = rayon::join(
        || subtree_root(left_leaves, level - 1),
        || subtree_root(right_leaves, level - 1),
    );
    let root = Node::combine(Level::from(level - 1), &left, &right);
    if level == SUBTREE_ROOT_LEVEL {
        return (root.clone(), vec![root]);
    }
    subtree_roots.extend(right_subtree_roots);
    (root, subtree_roots)
}

fn sequential_subtree_root<Node>(leaves: &[Node]) -> Node
where
    Node: Hashable + Clone,
{
    let mut level = Level::from(0);
    let mut nodes = leaves.to_vec();
    while nodes.len() > 1 {
        nodes = nodes
            .as_chunks::<2>()
            .0
            .iter()
            .map(|[left, right]| Node::combine(level, left, right))
            .collect();
        level = level + 1;
    }
    nodes.pop().unwrap_or_else(Node::empty_leaf)
}

/// Roots of the complete subtrees that together hold a tree's leaves:
/// `slots[level]` is present exactly when bit `level` of the leaf count is set.
struct CompleteSubtreeRoots<Node> {
    slots: Vec<Option<Node>>,
}

impl<Node> CompleteSubtreeRoots<Node>
where
    Node: Hashable + Clone,
{
    /// Splits `frontier` into the complete subtrees of its leaves.
    ///
    /// Folding the tip leaf in repeats the hashes a serial append performs
    /// when the next leaf arrives, so no hash is added. A subtree this
    /// completes was completed by an earlier batch, so it is not reported.
    fn of(frontier: &Frontier<Node, NOTE_COMMITMENT_TREE_DEPTH>) -> Self {
        let mut roots = Self {
            slots: vec![None; usize::from(NOTE_COMMITMENT_TREE_DEPTH) + 1],
        };
        let Some(frontier) = frontier.value() else {
            return roots;
        };
        let position = u64::from(frontier.position());
        let mut ommers = frontier.ommers().iter();
        for (level, slot) in roots.slots.iter_mut().enumerate() {
            if position >> level & 1 == 1 {
                *slot = ommers.next().cloned();
            }
        }
        let _already_reported = roots.merge(0, frontier.leaf().clone());
        roots
    }

    /// Adds the root of the next complete subtree in leaf order, at `level`,
    /// carrying into higher levels, and returns the `2^16`-leaf subtree root
    /// that the carry completes, if any.
    fn merge(&mut self, level: u8, root: Node) -> Option<Node> {
        let mut level = usize::from(level);
        let mut carry = root;
        let mut completed = None;
        while let Some(left) = self.slots.get_mut(level).and_then(Option::take) {
            carry = Node::combine(Level::from(u8::try_from(level).ok()?), &left, &carry);
            level += 1;
            if level == usize::from(SUBTREE_ROOT_LEVEL) {
                completed = Some(carry.clone());
            }
        }
        if let Some(slot) = self.slots.get_mut(level) {
            *slot = Some(carry);
        }
        completed
    }

    /// Returns the roots in level order, which are the ommers of a frontier
    /// whose tip position equals this leaf count.
    fn into_ommers(self) -> Vec<Node> {
        self.slots.into_iter().flatten().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A cheap, order-sensitive stand-in for a note-commitment hash, so the
    /// batch can be checked against serial appends across many shapes and
    /// across `2^16`-leaf subtree boundaries.
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct MixNode(u64);

    impl Hashable for MixNode {
        fn empty_leaf() -> Self {
            Self(0x9e37_79b9_7f4a_7c15)
        }

        fn combine(level: Level, a: &Self, b: &Self) -> Self {
            let mixed = a.0.rotate_left(17).wrapping_mul(0xbf58_476d_1ce4_e5b9)
                ^ b.0.wrapping_mul(0x94d0_49bb_1331_11eb)
                ^ u64::from(u8::from(level)).wrapping_mul(0x2545_f491_4f6c_dd1d);
            Self(mixed ^ (mixed >> 29))
        }
    }

    fn leaf(position: u64) -> MixNode {
        MixNode(position.wrapping_mul(0x632b_e59b_d9b4_e019) ^ 0x5851_f42d_4c95_7f2d)
    }

    /// Serial reference: the official append, with the subtree roots the
    /// accumulator records after each leaf.
    fn serial(
        frontier: &Frontier<MixNode, NOTE_COMMITMENT_TREE_DEPTH>,
        leaves: &[MixNode],
    ) -> (
        Frontier<MixNode, NOTE_COMMITMENT_TREE_DEPTH>,
        Vec<(u32, MixNode)>,
    ) {
        let mut frontier = frontier.clone();
        let mut completed = Vec::new();
        for leaf in leaves {
            assert!(frontier.append(leaf.clone()));
            if let Some((index, root)) = super::super::completed_subtree_root(&frontier) {
                completed.push((index.value(), root));
            }
        }
        (frontier, completed)
    }

    /// Builds the frontier after `size` leaves without hashing them all, from
    /// arbitrary ommers, since the batch only reads the frontier's parts.
    fn frontier_of_size(size: u64) -> Frontier<MixNode, NOTE_COMMITMENT_TREE_DEPTH> {
        if size == 0 {
            return Frontier::empty();
        }
        let position = Position::from(size - 1);
        let ommers = (0..u64::from(position.past_ommer_count()))
            .map(|level| MixNode(level ^ size.rotate_left(7)))
            .collect();
        let frontier = Frontier::from_parts(position, leaf(size - 1), ommers);
        assert!(frontier.is_ok());
        frontier.unwrap_or_else(|_| Frontier::empty())
    }

    fn assert_batch_matches_serial(start: u64, count: u64) {
        let frontier = frontier_of_size(start);
        let leaves = (start..start + count).map(leaf).collect::<Vec<_>>();
        let batch = append_batch(&frontier, &leaves);
        assert!(batch.is_some(), "batch from {start} with {count} leaves");
        let Some(batch) = batch else { return };
        let (expected_frontier, expected_roots) = serial(&frontier, &leaves);
        assert_eq!(
            batch.frontier, expected_frontier,
            "frontier from {start} + {count}"
        );
        assert_eq!(
            batch.completed_subtree_roots, expected_roots,
            "subtree roots from {start} + {count}"
        );
    }

    #[test]
    fn batches_match_serial_appends_for_every_small_shape() {
        for start in 0..70 {
            for count in 0..70 {
                assert_batch_matches_serial(start, count);
            }
        }
    }

    #[test]
    fn batches_match_serial_appends_around_subtree_boundaries() {
        let subtree = 1_u64 << SUBTREE_ROOT_LEVEL;
        for start in [
            subtree - 1,
            subtree,
            subtree + 1,
            3 * subtree - 40,
            5 * subtree - 1,
        ] {
            for count in [1, 2, 39, 40, 41, 64, 1_000] {
                assert_batch_matches_serial(start, count);
            }
        }
        // Batches that contain whole subtrees, aligned and unaligned.
        assert_batch_matches_serial(0, subtree);
        assert_batch_matches_serial(0, subtree + 1);
        assert_batch_matches_serial(subtree - 3, 2 * subtree + 7);
        assert_batch_matches_serial(4 * subtree, 4 * subtree + 1);
        assert_batch_matches_serial(7 * subtree + 12_345, 3 * subtree);
    }

    #[test]
    fn batches_match_serial_appends_on_a_bounded_pool() {
        let pool = rayon::ThreadPoolBuilder::new().num_threads(3).build();
        assert!(pool.is_ok());
        let Ok(pool) = pool else { return };
        pool.install(|| {
            assert_batch_matches_serial(1_u64 << 20, 5_000);
            assert_batch_matches_serial((1_u64 << 31) - 17, 2_000);
        });
    }

    #[test]
    fn chunks_are_aligned_maximal_and_cover_the_range() {
        let chunks = aligned_chunks(5, 37);
        let shape = chunks
            .iter()
            .map(|chunk| (chunk.position, chunk.level))
            .collect::<Vec<_>>();
        assert_eq!(
            shape,
            vec![(5, 0), (6, 1), (8, 3), (16, 4), (32, 2), (36, 0)]
        );
        assert!(aligned_chunks(9, 9).is_empty());
    }
}
