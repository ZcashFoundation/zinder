use std::cmp::Ordering;
use std::collections::HashMap;

use jsonrpsee::core::ClientError;
use jsonrpsee::core::middleware::{Batch, BatchEntry, Notification, Request, RpcServiceT};
use jsonrpsee::http_client::{HttpBackend, RpcLogger, RpcService};
use jsonrpsee::types::InvalidRequestId;

use crate::SourceError;

type Inner = RpcLogger<RpcService<HttpBackend>>;

/// Checks the reply IDs before jsonrpsee reorders the replies and discards their IDs.
#[derive(Clone, Debug)]
pub(crate) struct CheckBatchIds(pub(super) Inner);

impl RpcServiceT for CheckBatchIds {
    type MethodResponse = <Inner as RpcServiceT>::MethodResponse;
    type BatchResponse = <Inner as RpcServiceT>::BatchResponse;
    type NotificationResponse = <Inner as RpcServiceT>::NotificationResponse;

    fn call<'a>(
        &self,
        request: Request<'a>,
    ) -> impl Future<Output = Self::MethodResponse> + Send + 'a {
        self.0.call(request)
    }

    fn notification<'a>(
        &self,
        notification: Notification<'a>,
    ) -> impl Future<Output = Self::NotificationResponse> + Send + 'a {
        self.0.notification(notification)
    }

    fn batch<'a>(
        &self,
        requests: Batch<'a>,
    ) -> impl Future<Output = Self::BatchResponse> + Send + 'a {
        let mut requested_ids: HashMap<_, _> = requests
            .iter()
            .filter_map(|entry| match entry {
                Ok(BatchEntry::Call(request)) => Some((request.id(), false)),
                _ => None,
            })
            .collect();
        let response = self.0.batch(requests);
        async move {
            let replies = response.await?;
            match replies.len().cmp(&requested_ids.len()) {
                Ordering::Less => {
                    return Err(batch_mismatch(
                        "batched block response contained fewer entries than requested",
                    ));
                }
                Ordering::Greater => {
                    return Err(batch_mismatch(
                        "batched block response contained more entries than requested",
                    ));
                }
                Ordering::Equal => {}
            }
            for reply in &replies {
                let seen = requested_ids
                    .get_mut(reply.id())
                    .ok_or_else(|| InvalidRequestId::NotPendingRequest(reply.id().to_string()))?;
                if *seen {
                    return Err(batch_mismatch(
                        "batched getblock response repeated a request id",
                    ));
                }
                *seen = true;
            }
            Ok(replies)
        }
    }
}

fn batch_mismatch(reason: &'static str) -> ClientError {
    ClientError::Transport(Box::new(SourceError::SourceProtocolMismatch { reason }))
}
