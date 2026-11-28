//! Authenticated wallet chain-event projection from canonical retained events.

use std::num::NonZeroU32;

use zinder_core::{BlockHeight, BlockHeightRange, ChainEpoch, ChainEpochId};

use crate::{
    ChainEpochCommitted, ChainEvent, ChainEventEnvelope, ChainEventHistoryRequest,
    ChainEventStreamFamily, ChainEventStreamResume, ChainRangeReverted, EventStreamStartPosition,
    StreamCursorTokenV1,
    format::{
        CHAIN_EVENT_LOCATOR_MAX, ChainEventCursorAnchor, ChainEventCursorPayload, ChainEventLocator,
    },
};

use super::{
    CanonicalEventKind, CanonicalRetainedEvent, CanonicalStoreError, RocksDbCanonicalSecondary,
    displaced_archive::first_displaced_block_hash_after_event,
    event_lifecycle::read_retained_event_from_db,
};

impl RocksDbCanonicalSecondary {
    /// Projects retained canonical transitions into authenticated wallet events.
    pub fn wallet_chain_event_history(
        &self,
        request: ChainEventHistoryRequest<'_>,
    ) -> Result<Vec<ChainEventEnvelope>, CanonicalStoreError> {
        let current_event_sequence = self.ready_evidence.visible_event_sequence;
        let oldest_retained_sequence = self.canonical_event_retention_floor()?;
        let Some(cursor) = request.from_cursor else {
            return self.retained_wallet_events(
                oldest_retained_sequence,
                request.family,
                request.max_events,
            );
        };
        let payload = cursor
            .decode_chain_event(self.network(), self.cursor_auth_key)
            .map_err(|_| CanonicalStoreError::CanonicalEventCursorMalformed {
                reason: "cursor token failed validation",
            })?;
        if payload.event_sequence == 0 || payload.event_sequence > current_event_sequence {
            return Err(CanonicalStoreError::CanonicalEventCursorMalformed {
                reason: "cursor sequence is outside canonical history",
            });
        }
        let start_sequence = payload.event_sequence.checked_add(1).ok_or(
            CanonicalStoreError::CanonicalEventCursorMalformed {
                reason: "cursor sequence cannot advance",
            },
        )?;
        let expired = || CanonicalStoreError::CanonicalEventCursorExpired {
            event_sequence: payload.event_sequence,
            oldest_retained_sequence,
        };
        let tip = payload.locator.tip();
        if payload.family == ChainEventStreamFamily::Settled && !self.anchor_is_canonical(tip)? {
            return Err(expired());
        }
        if start_sequence >= oldest_retained_sequence {
            return self.retained_wallet_events(start_sequence, payload.family, request.max_events);
        }
        if payload.family == ChainEventStreamFamily::Settled || self.anchor_is_canonical(tip)? {
            return Err(expired());
        }
        for &anchor in &payload.locator.entries()[1..] {
            if self.anchor_is_canonical(anchor)? {
                let reorg =
                    self.synthesize_wallet_reorg(&payload, anchor, oldest_retained_sequence)?;
                return Ok(vec![reorg]);
            }
        }
        Err(expired())
    }

    fn retained_wallet_events(
        &self,
        start_sequence: u64,
        family: ChainEventStreamFamily,
        max_events: NonZeroU32,
    ) -> Result<Vec<ChainEventEnvelope>, CanonicalStoreError> {
        let mut envelopes = Vec::new();
        for sequence in start_sequence..=self.ready_evidence.visible_event_sequence {
            let event = read_retained_event_from_db(&self.bounded_open.db, sequence)?;
            if let Some(envelope) = self.wallet_event_envelope(event, family)? {
                envelopes.push(envelope);
                if envelopes.len() == max_events.get() as usize {
                    break;
                }
            }
        }
        Ok(envelopes)
    }

    /// Resolves and authenticates one wallet subscription start position.
    pub fn resolve_wallet_chain_event_stream_start(
        &self,
        start: &EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, CanonicalStoreError> {
        match start {
            EventStreamStartPosition::AfterCursor(cursor) => {
                let payload = cursor
                    .decode_chain_event(self.network(), self.cursor_auth_key)
                    .map_err(|_| CanonicalStoreError::CanonicalEventCursorMalformed {
                        reason: "cursor token failed validation",
                    })?;
                if requested_family != ChainEventStreamFamily::Visible
                    && requested_family != payload.family
                {
                    return Err(CanonicalStoreError::CanonicalEventCursorMalformed {
                        reason: "request family does not match the cursor's encoded family",
                    });
                }
                Ok(ChainEventStreamResume {
                    cursor: Some(cursor.clone()),
                    family: payload.family,
                })
            }
            EventStreamStartPosition::EarliestRetained => Ok(ChainEventStreamResume {
                cursor: None,
                family: requested_family,
            }),
            EventStreamStartPosition::LiveTail => {
                let event_sequence = self.ready_evidence.visible_event_sequence;
                if event_sequence == 0 {
                    return Ok(ChainEventStreamResume {
                        cursor: None,
                        family: requested_family,
                    });
                }
                let chain_epoch = self.chain_epoch()?;
                Ok(ChainEventStreamResume {
                    cursor: Some(self.mint_wallet_event_cursor(
                        chain_epoch,
                        requested_family,
                        event_sequence,
                    )?),
                    family: requested_family,
                })
            }
        }
    }

    fn anchor_is_canonical(
        &self,
        anchor: ChainEventCursorAnchor,
    ) -> Result<bool, CanonicalStoreError> {
        Ok(self
            .block_header_at(anchor.height)?
            .is_some_and(|header| header.block_hash == anchor.hash))
    }

    fn synthesize_wallet_reorg(
        &self,
        payload: &ChainEventCursorPayload,
        mut fork_point: ChainEventCursorAnchor,
        oldest_retained_sequence: u64,
    ) -> Result<ChainEventEnvelope, CanonicalStoreError> {
        let reverted_epoch = self.chain_epoch_at(ChainEpochId::new(payload.event_sequence))?;
        if payload.locator.tip()
            != (ChainEventCursorAnchor {
                height: reverted_epoch.visible_tip_height,
                hash: reverted_epoch.visible_tip_hash,
            })
        {
            return Err(CanonicalStoreError::CanonicalEventCursorMalformed {
                reason: "cursor tip does not match its historical chain epoch",
            });
        }
        let expired = || CanonicalStoreError::CanonicalEventCursorExpired {
            event_sequence: payload.event_sequence,
            oldest_retained_sequence,
        };
        if fork_point.height < reverted_epoch.settled_tip_height {
            let settled_tip = ChainEventCursorAnchor {
                height: reverted_epoch.settled_tip_height,
                hash: reverted_epoch.settled_tip_hash,
            };
            if !self.anchor_is_canonical(settled_tip)? {
                return Err(expired());
            }
            fork_point = settled_tip;
        }
        let reverted_start = fork_point.height.next().ok_or_else(expired)?;
        let current_epoch = self.chain_epoch()?;
        if current_epoch.visible_tip_height < reverted_start {
            return Err(expired());
        }
        let current_sequence = self.ready_evidence.visible_event_sequence;
        let cursor =
            self.mint_wallet_event_cursor(current_epoch, payload.family, current_sequence)?;
        let event = ChainEvent::ChainReorged {
            reverted: ChainRangeReverted::new(
                reverted_epoch,
                BlockHeightRange::inclusive(reverted_start, payload.locator.tip().height),
            ),
            committed: ChainEpochCommitted::new(
                current_epoch,
                BlockHeightRange::inclusive(reverted_start, current_epoch.visible_tip_height),
            ),
        };
        Ok(ChainEventEnvelope::new(
            cursor,
            current_sequence,
            current_epoch,
            current_epoch.settled_tip_height,
            event,
        ))
    }

    fn wallet_event_envelope(
        &self,
        event: CanonicalRetainedEvent,
        family: ChainEventStreamFamily,
    ) -> Result<Option<ChainEventEnvelope>, CanonicalStoreError> {
        let chain_epoch = self.chain_epoch_at(event.resulting_epoch_id())?;
        let wallet_event = match family {
            ChainEventStreamFamily::Visible => {
                let reverted_epoch = match event.kind() {
                    CanonicalEventKind::Committed => None,
                    CanonicalEventKind::Reorged => event
                        .previous_epoch_id()
                        .map(|id| self.chain_epoch_at(id))
                        .transpose()?,
                };
                ChainEvent::from_canonical_retained(event, chain_epoch, reverted_epoch)?
            }
            ChainEventStreamFamily::Settled => {
                let start = match event.previous_epoch_id() {
                    Some(id) => self.chain_epoch_at(id)?.settled_tip_height.next(),
                    None => Some(event.committed_range().start),
                };
                let Some(start) = start.filter(|height| *height <= chain_epoch.settled_tip_height)
                else {
                    return Ok(None);
                };
                ChainEvent::ChainCommitted {
                    committed: ChainEpochCommitted::new(
                        chain_epoch,
                        BlockHeightRange::inclusive(start, chain_epoch.settled_tip_height),
                    ),
                }
            }
        };
        let event_sequence = event.cursor().event_sequence();
        let cursor = self.mint_wallet_event_cursor(chain_epoch, family, event_sequence)?;
        Ok(Some(ChainEventEnvelope::new(
            cursor,
            event_sequence,
            chain_epoch,
            chain_epoch.settled_tip_height,
            wallet_event,
        )))
    }

    fn mint_wallet_event_cursor(
        &self,
        chain_epoch: ChainEpoch,
        family: ChainEventStreamFamily,
        event_sequence: u64,
    ) -> Result<StreamCursorTokenV1, CanonicalStoreError> {
        let tip = match family {
            ChainEventStreamFamily::Visible => ChainEventCursorAnchor {
                height: chain_epoch.visible_tip_height,
                hash: chain_epoch.visible_tip_hash,
            },
            ChainEventStreamFamily::Settled => ChainEventCursorAnchor {
                height: chain_epoch.settled_tip_height,
                hash: chain_epoch.settled_tip_hash,
            },
        };
        let mut entries = vec![tip];
        for height in locator_heights(tip.height).into_iter().skip(1) {
            if family == ChainEventStreamFamily::Visible
                && let Some(hash) = first_displaced_block_hash_after_event(
                    &self.bounded_open.db,
                    event_sequence,
                    height,
                )?
            {
                entries.push(ChainEventCursorAnchor { height, hash });
            } else if let Some(header) = self.block_header_at(height)? {
                entries.push(ChainEventCursorAnchor {
                    height,
                    hash: header.block_hash,
                });
            }
        }
        let locator = ChainEventLocator::new(entries).map_err(|_| {
            CanonicalStoreError::CanonicalEventCursorMalformed {
                reason: "chain-event locator is outside its bound",
            }
        })?;
        StreamCursorTokenV1::chain_event(
            chain_epoch.network,
            family,
            event_sequence,
            &locator,
            self.cursor_auth_key,
        )
        .map_err(|_| CanonicalStoreError::CanonicalEventCursorMalformed {
            reason: "cursor authentication failed",
        })
    }
}

fn locator_heights(tip_height: BlockHeight) -> Vec<BlockHeight> {
    let mut heights = Vec::with_capacity(CHAIN_EVENT_LOCATOR_MAX);
    let mut current = tip_height.value();
    let mut step = 1_u32;
    loop {
        heights.push(BlockHeight::new(current));
        if heights.len() >= CHAIN_EVENT_LOCATOR_MAX || current == 0 {
            break;
        }
        current = current.saturating_sub(step);
        step = step.saturating_mul(2);
    }
    heights
}
