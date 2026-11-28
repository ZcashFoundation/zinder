//! Authenticated client for canonical retained-event lease ownership.

use thiserror::Error;
use tokio_util::sync::CancellationToken;
use zinder_core::UnixTimestampMillis;
use zinder_proto::v1::ingest::{
    AcquireCanonicalProjectionBuildLeaseRequest, CanonicalEventPageRequest,
    CanonicalEventPageResponse, CanonicalProjectionBuildLease,
    CanonicalProjectionBuildLeaseResponse, CanonicalWriterStatusRequest,
    CanonicalWriterStatusResponse, CreateCanonicalOwnerCheckpointRequest,
    CreateCanonicalOwnerCheckpointResponse, ReadmitCanonicalOwnerCheckpointRequest,
    ReleaseCanonicalProjectionBuildLeaseRequest, RenewCanonicalProjectionBuildLeaseRequest,
    canonical_control_client::CanonicalControlClient,
};
use zinder_runtime::{
    AuthenticatedChannel, BearerToken, BearerTokenConnectError, bearer_metadata,
    connect_zinder_grpc, connect_zinder_grpc_at_startup,
};

/// Exact lease identity and anchor owned by one projector process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CanonicalRetentionLease {
    lease_id: [u8; 16],
    generation: u64,
    anchor_chain_epoch_id: u64,
    anchor_event_cursor: Vec<u8>,
    expires_at: UnixTimestampMillis,
}

impl CanonicalRetentionLease {
    /// Creates a lease around one authenticated canonical event fence.
    pub(crate) fn new(
        lease_id: [u8; 16],
        anchor_chain_epoch_id: u64,
        anchor_event_cursor: Vec<u8>,
        expires_at: UnixTimestampMillis,
    ) -> Self {
        Self {
            lease_id,
            generation: 0,
            anchor_chain_epoch_id,
            anchor_event_cursor,
            expires_at,
        }
    }

    /// Returns a renewal preserving the lease identity and canonical anchor.
    pub(crate) fn renewed(&self, expires_at: UnixTimestampMillis) -> Self {
        Self {
            expires_at,
            ..self.clone()
        }
    }

    /// Returns the current exclusive expiry bound.
    pub(crate) const fn expires_at(&self) -> UnixTimestampMillis {
        self.expires_at
    }

    /// Returns the exact canonical epoch protected against event pruning.
    pub(crate) const fn anchor_chain_epoch_id(&self) -> u64 {
        self.anchor_chain_epoch_id
    }

    /// Returns the exact retained-event cursor protected against pruning.
    pub(crate) fn anchor_event_cursor(&self) -> &[u8] {
        &self.anchor_event_cursor
    }

    fn into_proto(self) -> CanonicalProjectionBuildLease {
        CanonicalProjectionBuildLease {
            lease_id: self.lease_id.to_vec(),
            generation: self.generation,
            anchor_chain_epoch_id: self.anchor_chain_epoch_id,
            anchor_event_cursor: self.anchor_event_cursor,
            expires_at_unix_millis: self.expires_at.value(),
        }
    }

    fn from_proto(
        proto: CanonicalProjectionBuildLease,
    ) -> Result<Self, CanonicalWriterControlError> {
        let lease_id = <[u8; 16]>::try_from(proto.lease_id).map_err(|bytes| {
            CanonicalWriterControlError::InvalidResponse(format!(
                "canonical lease id has {} bytes, expected 16",
                bytes.len()
            ))
        })?;
        if proto.anchor_event_cursor.is_empty() {
            return Err(CanonicalWriterControlError::InvalidResponse(
                "canonical lease response omitted its event cursor".to_owned(),
            ));
        }
        if proto.generation == 0 {
            return Err(CanonicalWriterControlError::InvalidResponse(
                "canonical lease response omitted its generation".to_owned(),
            ));
        }
        Ok(Self {
            lease_id,
            generation: proto.generation,
            anchor_chain_epoch_id: proto.anchor_chain_epoch_id,
            anchor_event_cursor: proto.anchor_event_cursor,
            expires_at: UnixTimestampMillis::new(proto.expires_at_unix_millis),
        })
    }
}

/// Narrow client that is the projector's only canonical-primary mutation path.
#[derive(Clone)]
pub(crate) struct CanonicalWriterControlClient {
    client: CanonicalControlClient<AuthenticatedChannel>,
    checkpoint_bearer_token: Option<BearerToken>,
}

impl CanonicalWriterControlClient {
    /// Connects through the common authenticated Zinder transport.
    pub(crate) async fn connect(
        endpoint: &str,
        bearer_token: Option<&BearerToken>,
        checkpoint_bearer_token: Option<&BearerToken>,
    ) -> Result<Self, CanonicalWriterControlError> {
        let channel = connect_zinder_grpc(endpoint, bearer_token).await?;
        Ok(Self::from_channel(channel, checkpoint_bearer_token))
    }

    /// Connects while the projector starts, dialing again after each dial
    /// that times out, so a writer that is slow to come up delays startup
    /// instead of ending it. Returns `None` when `cancel` fires first.
    pub(crate) async fn connect_at_startup(
        endpoint: &str,
        bearer_token: Option<&BearerToken>,
        checkpoint_bearer_token: Option<&BearerToken>,
        cancel: &CancellationToken,
    ) -> Result<Option<Self>, CanonicalWriterControlError> {
        Ok(
            connect_zinder_grpc_at_startup(endpoint, bearer_token, cancel)
                .await?
                .map(|channel| Self::from_channel(channel, checkpoint_bearer_token)),
        )
    }

    fn from_channel(
        channel: AuthenticatedChannel,
        checkpoint_bearer_token: Option<&BearerToken>,
    ) -> Self {
        Self {
            client: CanonicalControlClient::new(channel),
            checkpoint_bearer_token: checkpoint_bearer_token.cloned(),
        }
    }

    /// Reads the writer fence used to authenticate the canonical secondary.
    pub(crate) async fn writer_status(
        &mut self,
    ) -> Result<CanonicalWriterStatusResponse, CanonicalWriterControlError> {
        self.client
            .writer_status(CanonicalWriterStatusRequest {})
            .await
            .map(tonic::Response::into_inner)
            .map_err(CanonicalWriterControlError::Status)
    }

    /// Reads one bounded retained-event page after an exact durable cursor.
    ///
    /// An empty cursor is deliberately not accepted here: the production
    /// projector always resumes from a READY wallet cursor or constructs a
    /// wallet at an explicitly retained canonical anchor. Starting from the
    /// writer's retention floor would silently skip a persisted wallet fence.
    pub(crate) async fn event_page(
        &mut self,
        from_cursor: &[u8],
        max_events: u32,
    ) -> Result<CanonicalEventPageResponse, CanonicalWriterControlError> {
        if from_cursor.is_empty() {
            return Err(CanonicalWriterControlError::InvalidRequest(
                "retained-event page requires a nonempty persisted cursor".to_owned(),
            ));
        }
        if max_events == 0 {
            return Err(CanonicalWriterControlError::InvalidRequest(
                "retained-event page limit must be nonzero".to_owned(),
            ));
        }
        self.client
            .event_page(CanonicalEventPageRequest {
                from_cursor: from_cursor.to_vec(),
                max_events,
            })
            .await
            .map(tonic::Response::into_inner)
            .map_err(CanonicalWriterControlError::Status)
    }

    /// Requests one canonical owner checkpoint under the writer's configured
    /// shared staging root. The endpoint's dedicated checkpoint capability is
    /// enforced by the canonical owner; this ordinary retained-event client is
    /// not a substitute for that authorization boundary.
    pub(crate) async fn create_owner_checkpoint(
        &mut self,
        candidate_id: String,
        staging_root_binding: Vec<u8>,
        expected_fence: zinder_proto::v1::ingest::CanonicalWriterFence,
    ) -> Result<CreateCanonicalOwnerCheckpointResponse, CanonicalWriterControlError> {
        let checkpoint_bearer_token = self.checkpoint_bearer_token.as_ref().ok_or_else(|| {
            CanonicalWriterControlError::InvalidRequest(
                "canonical checkpoint capability is not configured".to_owned(),
            )
        })?;
        let mut request = tonic::Request::new(CreateCanonicalOwnerCheckpointRequest {
            candidate_id,
            staging_root_binding,
            expected_fence: Some(expected_fence),
        });
        let metadata = bearer_metadata(checkpoint_bearer_token).map_err(|error| {
            CanonicalWriterControlError::InvalidRequest(format!(
                "could not encode canonical checkpoint capability: {error}"
            ))
        })?;
        request
            .metadata_mut()
            .insert("x-zinder-checkpoint-authorization", metadata);
        self.client
            .create_owner_checkpoint(request)
            .await
            .map(tonic::Response::into_inner)
            .map_err(CanonicalWriterControlError::Status)
    }

    /// Requires the canonical owner to cold-re-admit the exact physical
    /// checkpoint immediately before the wallet owner captures its matching
    /// checkpoint. The dedicated method capability remains mandatory.
    pub(crate) async fn readmit_owner_checkpoint(
        &mut self,
        candidate_id: String,
        staging_root_binding: Vec<u8>,
        expected_fence: zinder_proto::v1::ingest::CanonicalWriterFence,
        expected_database_identity: Vec<u8>,
    ) -> Result<CreateCanonicalOwnerCheckpointResponse, CanonicalWriterControlError> {
        let checkpoint_bearer_token = self.checkpoint_bearer_token.as_ref().ok_or_else(|| {
            CanonicalWriterControlError::InvalidRequest(
                "canonical checkpoint capability is not configured".to_owned(),
            )
        })?;
        let mut request = tonic::Request::new(ReadmitCanonicalOwnerCheckpointRequest {
            candidate_id,
            staging_root_binding,
            expected_fence: Some(expected_fence),
            expected_database_identity,
        });
        let metadata = bearer_metadata(checkpoint_bearer_token).map_err(|error| {
            CanonicalWriterControlError::InvalidRequest(format!(
                "could not encode canonical checkpoint capability: {error}"
            ))
        })?;
        request
            .metadata_mut()
            .insert("x-zinder-checkpoint-authorization", metadata);
        self.client
            .readmit_owner_checkpoint(request)
            .await
            .map(tonic::Response::into_inner)
            .map_err(CanonicalWriterControlError::Status)
    }

    /// Acquires pruning protection for the pinned construction anchor.
    pub(crate) async fn acquire(
        &mut self,
        lease: CanonicalRetentionLease,
    ) -> Result<CanonicalRetentionLease, CanonicalWriterControlError> {
        let expected = lease.clone();
        let response = self
            .client
            .acquire_projection_build_lease(AcquireCanonicalProjectionBuildLeaseRequest {
                lease: Some(lease.into_proto()),
            })
            .await
            .map(tonic::Response::into_inner)
            .map_err(CanonicalWriterControlError::Status)?;
        require_acquired_lease_response(response, &expected)
    }

    /// Renews pruning protection without changing its identity or anchor.
    pub(crate) async fn renew(
        &mut self,
        lease: CanonicalRetentionLease,
    ) -> Result<CanonicalRetentionLease, CanonicalWriterControlError> {
        let expected = lease.clone();
        let response = self
            .client
            .renew_projection_build_lease(RenewCanonicalProjectionBuildLeaseRequest {
                lease: Some(lease.into_proto()),
            })
            .await
            .map(tonic::Response::into_inner)
            .map_err(CanonicalWriterControlError::Status)?;
        require_exact_lease_response(response, &expected)
    }

    /// Releases the exact lease identity after promotion or abandoned work.
    pub(crate) async fn release(
        &mut self,
        lease: &CanonicalRetentionLease,
    ) -> Result<(), CanonicalWriterControlError> {
        self.client
            .release_projection_build_lease(ReleaseCanonicalProjectionBuildLeaseRequest {
                lease: Some(lease.clone().into_proto()),
            })
            .await
            .map(|_| ())
            .map_err(CanonicalWriterControlError::Status)
    }
}

fn require_exact_lease_response(
    response: CanonicalProjectionBuildLeaseResponse,
    expected: &CanonicalRetentionLease,
) -> Result<CanonicalRetentionLease, CanonicalWriterControlError> {
    let observed = response.lease.ok_or_else(|| {
        CanonicalWriterControlError::InvalidResponse(
            "canonical lease response omitted the acquired lease".to_owned(),
        )
    })?;
    let observed = CanonicalRetentionLease::from_proto(observed)?;
    if &observed != expected {
        return Err(CanonicalWriterControlError::InvalidResponse(
            "canonical lease response differs from the requested identity, anchor, or expiry"
                .to_owned(),
        ));
    }
    Ok(observed)
}

fn require_acquired_lease_response(
    response: CanonicalProjectionBuildLeaseResponse,
    expected: &CanonicalRetentionLease,
) -> Result<CanonicalRetentionLease, CanonicalWriterControlError> {
    let observed = response.lease.ok_or_else(|| {
        CanonicalWriterControlError::InvalidResponse(
            "canonical lease response omitted the acquired lease".to_owned(),
        )
    })?;
    let observed = CanonicalRetentionLease::from_proto(observed)?;
    if observed.lease_id != expected.lease_id
        || observed.anchor_chain_epoch_id != expected.anchor_chain_epoch_id
        || observed.anchor_event_cursor != expected.anchor_event_cursor
        || observed.expires_at != expected.expires_at
    {
        return Err(CanonicalWriterControlError::InvalidResponse(
            "canonical acquired lease response differs from the requested identity, anchor, or expiry"
                .to_owned(),
        ));
    }
    Ok(observed)
}

/// Authenticated canonical-control failure.
#[derive(Debug, Error)]
pub enum CanonicalWriterControlError {
    /// Connecting to canonical control failed.
    #[error(transparent)]
    Connect(#[from] BearerTokenConnectError),

    /// Canonical control RPC returned an error status.
    #[error("canonical control RPC failed: {0}")]
    Status(tonic::Status),

    /// Canonical control response contained invalid evidence or lease fields.
    #[error("canonical control returned invalid evidence: {0}")]
    InvalidResponse(String),

    /// Caller constructed an invalid canonical-control request.
    #[error("canonical control request is invalid: {0}")]
    InvalidRequest(String),
}

#[cfg(test)]
mod tests {
    use super::{
        CanonicalProjectionBuildLease, CanonicalProjectionBuildLeaseResponse,
        CanonicalRetentionLease, CanonicalWriterControlError, require_acquired_lease_response,
        require_exact_lease_response,
    };
    use zinder_core::UnixTimestampMillis;

    #[test]
    fn acquisition_accepts_only_the_writer_assigned_generation_change()
    -> Result<(), CanonicalWriterControlError> {
        let requested = requested_lease();
        let acquired = require_acquired_lease_response(
            lease_response(7, UnixTimestampMillis::new(20)),
            &requested,
        )?;

        assert_eq!(acquired.generation, 7);
        assert!(matches!(
            require_acquired_lease_response(
                lease_response(7, UnixTimestampMillis::new(21)),
                &requested,
            ),
            Err(CanonicalWriterControlError::InvalidResponse(_))
        ));
        Ok(())
    }

    #[test]
    fn renewal_requires_the_exact_generation_bearing_lease() {
        let expected = CanonicalRetentionLease {
            generation: 7,
            ..requested_lease()
        };

        assert!(
            require_exact_lease_response(
                lease_response(7, UnixTimestampMillis::new(20)),
                &expected,
            )
            .is_ok()
        );
        assert!(matches!(
            require_exact_lease_response(
                lease_response(8, UnixTimestampMillis::new(20)),
                &expected,
            ),
            Err(CanonicalWriterControlError::InvalidResponse(_))
        ));
    }

    fn requested_lease() -> CanonicalRetentionLease {
        CanonicalRetentionLease::new(
            [0x11; 16],
            3,
            vec![1, 0, 0, 0, 0, 0, 0, 0, 4],
            UnixTimestampMillis::new(20),
        )
    }

    fn lease_response(
        generation: u64,
        expires_at: UnixTimestampMillis,
    ) -> CanonicalProjectionBuildLeaseResponse {
        CanonicalProjectionBuildLeaseResponse {
            lease: Some(CanonicalProjectionBuildLease {
                lease_id: vec![0x11; 16],
                anchor_chain_epoch_id: 3,
                anchor_event_cursor: vec![1, 0, 0, 0, 0, 0, 0, 0, 4],
                expires_at_unix_millis: expires_at.value(),
                generation,
            }),
        }
    }
}
