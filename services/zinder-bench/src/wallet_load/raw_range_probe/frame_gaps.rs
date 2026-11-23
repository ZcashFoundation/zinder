//! Client-observed intervals between successive response frames.

#[derive(Default)]
pub(super) struct FrameGaps {
    previous_ns: Option<u64>,
    pub(super) max_ns: u64,
    pub(super) ending_frame: u64,
    pub(super) arrivals_ns: Option<Vec<u64>>,
}

impl FrameGaps {
    pub(super) fn new(record_arrivals: bool) -> Self {
        Self {
            arrivals_ns: record_arrivals.then(Vec::new),
            ..Self::default()
        }
    }

    pub(super) fn observe(&mut self, offset_ns: u64, ordinal: u64) {
        if let Some(previous_ns) = self.previous_ns.replace(offset_ns) {
            let gap = offset_ns.saturating_sub(previous_ns);
            if gap > self.max_ns {
                self.max_ns = gap;
                self.ending_frame = ordinal;
            }
        }
        if let Some(arrivals) = &mut self.arrivals_ns {
            arrivals.push(offset_ns);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FrameGaps;

    #[test]
    fn tracks_the_first_largest_positive_gap_in_delivery_order() {
        for record in [false, true] {
            let mut gaps = FrameGaps::new(record);
            let offsets = [500, 500, 503, 510, 517, 518];
            assert_eq!((gaps.max_ns, gaps.ending_frame), (0, 0));
            for ((ordinal, offset), expected) in
                (1..)
                    .zip(offsets)
                    .zip([(0, 0), (0, 0), (3, 3), (7, 4), (7, 4), (7, 4)])
            {
                gaps.observe(offset, ordinal);
                assert_eq!((gaps.max_ns, gaps.ending_frame), expected);
            }
            assert_eq!(
                gaps.arrivals_ns.as_deref(),
                record.then_some(offsets.as_slice())
            );
        }
    }
}
