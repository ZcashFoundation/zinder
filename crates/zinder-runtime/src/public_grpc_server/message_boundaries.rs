//! Finds where gRPC messages end in a request body that arrives in chunks.
//!
//! Each gRPC message on the wire is a 1-byte compression flag, a 4-byte
//! big-endian length, and that many bytes. A client stream counts as active
//! only when a whole message arrives, so a client that drips bytes without
//! completing a message still goes idle.

const PREFIX_BYTES: usize = 5;

/// Position within the gRPC message currently arriving.
#[derive(Debug, Default)]
pub(super) struct MessageBoundaries {
    prefix: [u8; PREFIX_BYTES],
    prefix_received: usize,
    payload_remaining: usize,
}

impl MessageBoundaries {
    /// Consumes the next chunk of the body and returns whether at least one
    /// message finished arriving in it.
    pub(super) fn completes_a_message(&mut self, mut chunk: &[u8]) -> bool {
        let mut completed = false;
        while !chunk.is_empty() {
            if self.payload_remaining > 0 {
                let taken = self.payload_remaining.min(chunk.len());
                self.payload_remaining -= taken;
                completed |= self.payload_remaining == 0;
                chunk = &chunk[taken..];
                continue;
            }
            let wanted = PREFIX_BYTES - self.prefix_received;
            let taken = wanted.min(chunk.len());
            let (head, rest) = chunk.split_at(taken);
            self.prefix[self.prefix_received..self.prefix_received + taken].copy_from_slice(head);
            self.prefix_received += taken;
            chunk = rest;
            if self.prefix_received == PREFIX_BYTES {
                self.prefix_received = 0;
                let [_, length @ ..] = self.prefix;
                self.payload_remaining =
                    usize::try_from(u32::from_be_bytes(length)).unwrap_or(usize::MAX);
                completed |= self.payload_remaining == 0;
            }
        }
        completed
    }
}

#[cfg(test)]
mod tests {
    use eyre::{Result, eyre};

    use super::MessageBoundaries;

    fn message(payload: &[u8]) -> Result<Vec<u8>> {
        let mut framed = vec![0];
        framed.extend_from_slice(&u32::try_from(payload.len())?.to_be_bytes());
        framed.extend_from_slice(payload);
        Ok(framed)
    }

    #[test]
    fn a_whole_message_in_one_chunk_completes() -> Result<()> {
        let mut boundaries = MessageBoundaries::default();
        assert!(boundaries.completes_a_message(&message(b"hello")?));
        Ok(())
    }

    #[test]
    fn an_empty_message_completes_at_its_prefix() -> Result<()> {
        let mut boundaries = MessageBoundaries::default();
        assert!(boundaries.completes_a_message(&message(b"")?));
        Ok(())
    }

    #[test]
    fn a_message_split_byte_by_byte_completes_only_at_its_last_byte() -> Result<()> {
        let framed = message(b"abc")?;
        let (last, earlier) = framed
            .split_last()
            .ok_or_else(|| eyre!("a framed message is never empty"))?;
        let mut boundaries = MessageBoundaries::default();
        for byte in earlier {
            assert!(!boundaries.completes_a_message(std::slice::from_ref(byte)));
        }
        assert!(boundaries.completes_a_message(std::slice::from_ref(last)));
        Ok(())
    }

    #[test]
    fn a_chunk_ending_one_message_and_starting_another_completes() -> Result<()> {
        let mut stream = message(b"first")?;
        stream.extend(message(b"second")?);
        let (head, tail) = stream.split_at(8);
        let mut boundaries = MessageBoundaries::default();
        assert!(!boundaries.completes_a_message(head));
        assert!(boundaries.completes_a_message(tail));
        assert!(!boundaries.completes_a_message(&[0, 0, 0]));
        Ok(())
    }

    #[test]
    fn a_prefix_announcing_a_payload_does_not_complete() {
        let mut boundaries = MessageBoundaries::default();
        assert!(!boundaries.completes_a_message(&[0, 0, 0, 0, 100, 1, 2, 3]));
    }
}
