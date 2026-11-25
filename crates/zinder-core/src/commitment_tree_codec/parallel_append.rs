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

type OmmerKey = (u8, u64);

/// The frontier after a batch, with the roots of the `2^16`-leaf subtrees the
/// batch completed, by subtree index in completion order.
pub(super) struct BatchAppend<Node> {
    pub(super) frontier: Frontier<Node, NOTE_COMMITMENT_TREE_DEPTH>,
    pub(super) completed_subtree_roots: Vec<(u32, Node)>,
}

/// The frontiers at one or more cumulative leaf boundaries across a batch,
/// with the roots of every `2^16`-leaf subtree the batch completed.
pub(super) struct MultiBlockBatchAppend<Node> {
    pub(super) frontiers: Vec<Frontier<Node, NOTE_COMMITMENT_TREE_DEPTH>>,
    pub(super) completed_subtree_roots: Vec<(u32, Node)>,
}

struct ChunkHashOutput<Node> {
    root: Node,
    inner_subtree_roots: Vec<Node>,
    internal_ommers: Vec<(OmmerKey, Node)>,
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
    if leaves.is_empty() {
        return Some(BatchAppend {
            frontier: frontier.clone(),
            completed_subtree_roots: Vec::new(),
        });
    }
    let mut batch = append_batch_with_frontiers(frontier, leaves, &[leaves.len()])?;
    let frontier = batch.frontiers.pop()?;
    Some(BatchAppend {
        frontier,
        completed_subtree_roots: batch.completed_subtree_roots,
    })
}

/// Appends `leaves` after `frontier` and derives the intermediate frontier at
/// each strictly increasing cumulative leaf count in `end_offsets`.
///
/// Every tree node is hashed at most once across the full batch; chunks that
/// contain intermediate block tips additionally retain the left-child ommers
/// needed to reconstruct each requested frontier.
pub(super) fn append_batch_with_frontiers<Node>(
    frontier: &Frontier<Node, NOTE_COMMITMENT_TREE_DEPTH>,
    leaves: &[Node],
    end_offsets: &[usize],
) -> Option<MultiBlockBatchAppend<Node>>
where
    Node: Hashable + Clone + Send + Sync,
{
    if leaves.is_empty() {
        return end_offsets.is_empty().then(|| MultiBlockBatchAppend {
            frontiers: Vec::new(),
            completed_subtree_roots: Vec::new(),
        });
    }
    validate_end_offsets(end_offsets, leaves.len())?;
    let (tip_leaf, before_tip) = leaves.split_last()?;
    let first_position = frontier.tree_size();
    let tip_position = first_position + u64::try_from(before_tip.len()).ok()?;
    let intermediate_tips = intermediate_tip_positions(first_position, end_offsets)?;
    let collect_ommers = !intermediate_tips.is_empty();
    let (mut roots, mut ommers) = CompleteSubtreeRoots::of_with_ommers(frontier, collect_ommers);
    let chunks = aligned_chunks(first_position, tip_position);
    let hashed = hash_aligned_chunks(first_position, before_tip, &chunks, &intermediate_tips)?;

    let mut completed_subtree_roots =
        merge_hashed_chunks(&chunks, hashed, &mut roots, &mut ommers, collect_ommers)?;
    let final_tip = Position::from(tip_position);
    let final_frontier =
        Frontier::from_parts(final_tip, tip_leaf.clone(), roots.into_ommers()).ok()?;
    let subtree_level = Level::from(SUBTREE_ROOT_LEVEL);
    if final_tip.is_complete_subtree(subtree_level) {
        let subtree = u32::try_from(tip_position >> SUBTREE_ROOT_LEVEL).ok()?;
        completed_subtree_roots.push((subtree, final_frontier.value()?.root(Some(subtree_level))));
    }
    let frontiers = if intermediate_tips.is_empty() {
        vec![final_frontier]
    } else {
        build_checkpoint_frontiers(first_position, leaves, end_offsets, ommers, final_frontier)?
    };
    Some(MultiBlockBatchAppend {
        frontiers,
        completed_subtree_roots,
    })
}

fn validate_end_offsets(end_offsets: &[usize], total_leaves: usize) -> Option<()> {
    let mut previous = 0_usize;
    for &offset in end_offsets {
        if offset <= previous || offset > total_leaves {
            return None;
        }
        previous = offset;
    }
    (previous == total_leaves).then_some(())
}

fn intermediate_tip_positions(first_position: u64, end_offsets: &[usize]) -> Option<Vec<u64>> {
    let mut tips = Vec::with_capacity(end_offsets.len().saturating_sub(1));
    for &offset in &end_offsets[..end_offsets.len().saturating_sub(1)] {
        let leaf_offset = u64::try_from(offset.checked_sub(1)?).ok()?;
        tips.push(first_position + leaf_offset);
    }
    Some(tips)
}

fn hash_aligned_chunks<Node>(
    first_position: u64,
    before_tip: &[Node],
    chunks: &[AlignedChunk],
    intermediate_tips: &[u64],
) -> Option<Vec<ChunkHashOutput<Node>>>
where
    Node: Hashable + Clone + Send + Sync,
{
    chunks
        .par_iter()
        .map(|chunk| {
            let offset = usize::try_from(chunk.position - first_position).ok()?;
            let chunk_size = 1_usize << chunk.level;
            let leaves = before_tip.get(offset..offset + chunk_size)?;
            let chunk_end = chunk.position + u64::try_from(chunk_size).ok()?;
            let tips_inside = tips_strictly_inside(intermediate_tips, chunk.position, chunk_end);
            Some(subtree_root_with_tips(
                chunk.position,
                leaves,
                chunk.level,
                tips_inside,
            ))
        })
        .collect::<Option<Vec<_>>>()
}

fn merge_hashed_chunks<Node>(
    chunks: &[AlignedChunk],
    hashed: Vec<ChunkHashOutput<Node>>,
    roots: &mut CompleteSubtreeRoots<Node>,
    ommers: &mut Vec<(OmmerKey, Node)>,
    collect_ommers: bool,
) -> Option<Vec<(u32, Node)>>
where
    Node: Hashable + Clone,
{
    let mut completed_subtree_roots = Vec::new();
    for (chunk, chunk_out) in chunks.iter().zip(hashed) {
        let first_subtree = u32::try_from(chunk.position >> SUBTREE_ROOT_LEVEL).ok()?;
        for (subtree, inner_root) in (first_subtree..).zip(chunk_out.inner_subtree_roots) {
            completed_subtree_roots.push((subtree, inner_root));
        }
        if collect_ommers {
            ommers.extend(chunk_out.internal_ommers);
        }
        let last_position = chunk.position + (1_u64 << chunk.level) - 1;
        let recorded = collect_ommers.then_some(&mut *ommers);
        if let Some(completed) = roots.merge(chunk.level, chunk.position, chunk_out.root, recorded)
        {
            let subtree = u32::try_from(last_position >> SUBTREE_ROOT_LEVEL).ok()?;
            completed_subtree_roots.push((subtree, completed));
        }
    }
    Some(completed_subtree_roots)
}

fn build_checkpoint_frontiers<Node>(
    first_position: u64,
    leaves: &[Node],
    end_offsets: &[usize],
    mut ommers: Vec<(OmmerKey, Node)>,
    final_frontier: Frontier<Node, NOTE_COMMITMENT_TREE_DEPTH>,
) -> Option<Vec<Frontier<Node, NOTE_COMMITMENT_TREE_DEPTH>>>
where
    Node: Hashable + Clone,
{
    ommers.sort_unstable_by_key(|&(key, _)| key);
    let mut frontiers = Vec::with_capacity(end_offsets.len());
    for &end_offset in &end_offsets[..end_offsets.len().saturating_sub(1)] {
        let leaf_offset = end_offset.checked_sub(1)?;
        let tip_position = first_position + u64::try_from(leaf_offset).ok()?;
        let tip_leaf = leaves.get(leaf_offset)?.clone();
        let tip_ommers = collect_frontier_ommers(tip_position, &ommers)?;
        frontiers
            .push(Frontier::from_parts(Position::from(tip_position), tip_leaf, tip_ommers).ok()?);
    }
    frontiers.push(final_frontier);
    Some(frontiers)
}

fn collect_frontier_ommers<Node: Clone>(
    tip_position: u64,
    sorted_ommers: &[(OmmerKey, Node)],
) -> Option<Vec<Node>> {
    let mut ommers = Vec::new();
    for level in 0..NOTE_COMMITMENT_TREE_DEPTH {
        if (tip_position >> level) & 1 == 1 {
            let start_pos = tip_position & !((1_u64 << (level + 1)) - 1);
            let index = sorted_ommers
                .binary_search_by_key(&(level, start_pos), |&(key, _)| key)
                .ok()?;
            ommers.push(sorted_ommers.get(index)?.1.clone());
        }
    }
    Some(ommers)
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

fn tips_strictly_inside(sorted_tips: &[u64], start: u64, end: u64) -> &[u64] {
    let from = sorted_tips.partition_point(|&tip| tip <= start);
    let to = sorted_tips.partition_point(|&tip| tip < end);
    sorted_tips.get(from..to).unwrap_or(&[])
}

fn has_tip_in_range(sorted_tips: &[u64], start: u64, end: u64) -> bool {
    let index = sorted_tips.partition_point(|&tip| tip < start);
    sorted_tips.get(index).is_some_and(|&tip| tip < end)
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

fn subtree_root_with_tips<Node>(
    base_pos: u64,
    leaves: &[Node],
    level: u8,
    intermediate_tips: &[u64],
) -> ChunkHashOutput<Node>
where
    Node: Hashable + Clone + Send + Sync,
{
    if intermediate_tips.is_empty() {
        let (root, inner_subtree_roots) = subtree_root(leaves, level);
        return ChunkHashOutput {
            root,
            inner_subtree_roots,
            internal_ommers: Vec::new(),
        };
    }
    if level <= SEQUENTIAL_SUBTREE_LEVEL {
        let (root, internal_ommers) =
            sequential_subtree_root_with_tips(base_pos, leaves, level, intermediate_tips);
        return ChunkHashOutput {
            root,
            inner_subtree_roots: Vec::new(),
            internal_ommers,
        };
    }
    let child_level = level - 1;
    let mid_pos = base_pos + (1_u64 << child_level);
    let mid_tip_index = intermediate_tips.partition_point(|&tip| tip < mid_pos);
    let left_tips = &intermediate_tips[..mid_tip_index];
    let right_needs_left_root = mid_tip_index < intermediate_tips.len();
    let right_start = if intermediate_tips.get(mid_tip_index) == Some(&mid_pos) {
        mid_tip_index + 1
    } else {
        mid_tip_index
    };
    let right_tips = &intermediate_tips[right_start..];
    let (left_leaves, right_leaves) = leaves.split_at(leaves.len() / 2);
    let (left_out, right_out) = rayon::join(
        || subtree_root_with_tips(base_pos, left_leaves, child_level, left_tips),
        || subtree_root_with_tips(mid_pos, right_leaves, child_level, right_tips),
    );
    let root = Node::combine(Level::from(child_level), &left_out.root, &right_out.root);
    let mut internal_ommers = left_out.internal_ommers;
    internal_ommers.extend(right_out.internal_ommers);
    if right_needs_left_root {
        internal_ommers.push(((child_level, base_pos), left_out.root));
    }
    if level == SUBTREE_ROOT_LEVEL {
        return ChunkHashOutput {
            root: root.clone(),
            inner_subtree_roots: vec![root],
            internal_ommers,
        };
    }
    let mut inner_subtree_roots = left_out.inner_subtree_roots;
    inner_subtree_roots.extend(right_out.inner_subtree_roots);
    ChunkHashOutput {
        root,
        inner_subtree_roots,
        internal_ommers,
    }
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

fn sequential_subtree_root_with_tips<Node>(
    base_pos: u64,
    leaves: &[Node],
    level: u8,
    intermediate_tips: &[u64],
) -> (Node, Vec<(OmmerKey, Node)>)
where
    Node: Hashable + Clone,
{
    let mut nodes = leaves.to_vec();
    let mut internal_ommers = Vec::new();
    for lvl in 0..level {
        let step = 1_u64 << lvl;
        let mut next_nodes = Vec::with_capacity(nodes.len() / 2);
        for (pair_index, [left, right]) in nodes.as_chunks::<2>().0.iter().enumerate() {
            let left_pos = base_pos + (u64::try_from(pair_index).unwrap_or(u64::MAX) << (lvl + 1));
            let mid_pos = left_pos + step;
            let right_end = mid_pos + step;
            if has_tip_in_range(intermediate_tips, mid_pos, right_end) {
                internal_ommers.push(((lvl, left_pos), left.clone()));
            }
            next_nodes.push(Node::combine(Level::from(lvl), left, right));
        }
        nodes = next_nodes;
    }
    (
        nodes.pop().unwrap_or_else(Node::empty_leaf),
        internal_ommers,
    )
}

/// Roots of the complete subtrees that together hold a tree's leaves:
/// `slots[level]` is present exactly when bit `level` of the leaf count is set.
struct CompleteSubtreeRoots<Node> {
    slots: Vec<Option<(u64, Node)>>,
}

impl<Node> CompleteSubtreeRoots<Node>
where
    Node: Hashable + Clone,
{
    /// Splits `frontier` into the complete subtrees of its leaves, optionally
    /// collecting every left-child ommer covering `[0..frontier.tree_size())`.
    fn of_with_ommers(
        frontier: &Frontier<Node, NOTE_COMMITMENT_TREE_DEPTH>,
        collect_ommers: bool,
    ) -> (Self, Vec<(OmmerKey, Node)>) {
        let mut roots = Self {
            slots: vec![None; usize::from(NOTE_COMMITMENT_TREE_DEPTH) + 1],
        };
        let Some(frontier) = frontier.value() else {
            return (roots, Vec::new());
        };
        let position = u64::from(frontier.position());
        let mut ommers = frontier.ommers().iter();
        for (level, slot) in roots.slots.iter_mut().enumerate() {
            if position >> level & 1 == 1 {
                let start_pos = position & !((1_u64 << (level + 1)) - 1);
                *slot = ommers.next().cloned().map(|node| (start_pos, node));
            }
        }
        let _already_reported = roots.merge(0, position, frontier.leaf().clone(), None);
        let initial_ommers = if collect_ommers {
            roots
                .slots
                .iter()
                .enumerate()
                .filter_map(|(level, slot)| {
                    let (start_pos, node) = slot.as_ref()?;
                    Some(((u8::try_from(level).ok()?, *start_pos), node.clone()))
                })
                .collect()
        } else {
            Vec::new()
        };
        (roots, initial_ommers)
    }

    /// Adds the root of the next complete subtree in leaf order, at `level`,
    /// carrying into higher levels, and returns the `2^16`-leaf subtree root
    /// that the carry completes, if any.
    fn merge(
        &mut self,
        level: u8,
        start_pos: u64,
        root: Node,
        ommers: Option<&mut Vec<(OmmerKey, Node)>>,
    ) -> Option<Node> {
        let mut level = usize::from(level);
        let mut carry_pos = start_pos;
        let mut carry = root;
        let mut completed = None;
        while let Some((left_pos, left)) = self.slots.get_mut(level).and_then(Option::take) {
            carry = Node::combine(Level::from(u8::try_from(level).ok()?), &left, &carry);
            carry_pos = left_pos;
            level += 1;
            if level == usize::from(SUBTREE_ROOT_LEVEL) {
                completed = Some(carry.clone());
            }
        }
        if let Some(slot) = self.slots.get_mut(level) {
            if let (Some(ommers), Ok(lvl)) = (ommers, u8::try_from(level)) {
                ommers.push(((lvl, carry_pos), carry.clone()));
            }
            *slot = Some((carry_pos, carry));
        }
        completed
    }

    /// Returns the roots in level order, which are the ommers of a frontier
    /// whose tip position equals this leaf count.
    fn into_ommers(self) -> Vec<Node> {
        self.slots
            .into_iter()
            .flatten()
            .map(|(_, node)| node)
            .collect()
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

    fn assert_multi_block_matches_serial(start: u64, block_counts: &[u64]) {
        let frontier = frontier_of_size(start);
        let total: u64 = block_counts.iter().sum();
        let leaves = (start..start + total).map(leaf).collect::<Vec<_>>();
        let mut end_offsets = Vec::new();
        let mut running = 0_usize;
        for &count in block_counts {
            let count_usize = usize::try_from(count).unwrap_or(usize::MAX);
            if count_usize > 0 {
                running += count_usize;
                end_offsets.push(running);
            }
        }
        let batch = append_batch_with_frontiers(&frontier, &leaves, &end_offsets);
        assert!(
            batch.is_some(),
            "multi-block batch from {start} with {block_counts:?}"
        );
        let Some(batch) = batch else { return };

        let mut serial_frontier = frontier;
        let mut serial_roots = Vec::new();
        let mut expected_frontiers = Vec::new();
        let mut cursor = 0_usize;
        for &count in block_counts {
            let count_usize = usize::try_from(count).unwrap_or(usize::MAX);
            if count_usize == 0 {
                continue;
            }
            for item_leaf in &leaves[cursor..cursor + count_usize] {
                assert!(serial_frontier.append(item_leaf.clone()));
                if let Some((index, root)) = super::super::completed_subtree_root(&serial_frontier)
                {
                    serial_roots.push((index.value(), root));
                }
            }
            cursor += count_usize;
            expected_frontiers.push(serial_frontier.clone());
        }
        assert_eq!(
            batch.frontiers, expected_frontiers,
            "intermediate frontiers from {start} with {block_counts:?}"
        );
        assert_eq!(
            batch.completed_subtree_roots, serial_roots,
            "completed subtree roots from {start} with {block_counts:?}"
        );
    }

    #[test]
    fn multi_block_batches_match_serial_frontiers_across_small_shapes() {
        for start in [0_u64, 1, 2, 5, 7, 15, 16, 31, 63] {
            for split in [
                &[1_u64][..],
                &[1, 1, 1, 1, 1],
                &[1, 0, 2, 0, 3, 5, 8],
                &[7, 1, 9, 2, 13, 1],
                &[31, 1, 1, 1],
                &[1, 32, 1, 16, 1],
            ] {
                assert_multi_block_matches_serial(start, split);
            }
        }
        assert!(
            append_batch_with_frontiers(&frontier_of_size(0), &[leaf(0), leaf(1)], &[1, 1])
                .is_none()
        );
    }

    #[test]
    fn multi_block_batches_match_serial_frontiers_across_subtree_boundaries() {
        let subtree = 1_u64 << SUBTREE_ROOT_LEVEL;
        assert_multi_block_matches_serial(subtree - 10, &[5, 5, 1, 19, 2_000, 1]);
        assert_multi_block_matches_serial(subtree - 1, &[1, 0, 1, subtree, 3]);
        assert_multi_block_matches_serial(3 * subtree - 500, &[400, 100, 250, subtree, 700]);
        assert_multi_block_matches_serial(1_u64 << 20, &[17, 1_024, 3, 4_096, 1, 255]);
    }
}
