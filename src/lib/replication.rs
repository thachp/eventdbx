pub mod proto {
    tonic::include_proto!("eventdbx.replication");
}

use std::sync::Arc;

use proto::{
    EventBatch, HeartbeatRequest, HeartbeatResponse, ReplicationAck, SnapshotChunk,
    SnapshotRequest, replication_server::Replication,
};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

#[derive(Clone, Default)]
pub struct ReplicationState {
    pub last_sequence: Arc<Mutex<u64>>,
}

#[derive(Clone, Default)]
pub struct ReplicationService {
    state: ReplicationState,
}

impl ReplicationService {
    pub fn new() -> Self {
        Self {
            state: ReplicationState::default(),
        }
    }

    pub fn state(&self) -> ReplicationState {
        self.state.clone()
    }
}

#[tonic::async_trait]
impl Replication for ReplicationService {
    type BootstrapSnapshotStream =
        tokio_stream::wrappers::ReceiverStream<Result<SnapshotChunk, Status>>;

    async fn bootstrap_snapshot(
        &self,
        _request: Request<SnapshotRequest>,
    ) -> Result<Response<Self::BootstrapSnapshotStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(tx);
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn apply_events(
        &self,
        request: Request<tonic::Streaming<EventBatch>>,
    ) -> Result<Response<ReplicationAck>, Status> {
        let mut stream = request.into_inner();
        let mut last_sequence = 0u64;
        while let Some(batch) = stream.message().await? {
            last_sequence = batch.sequence;
        }

        let mut guard = self.state.last_sequence.lock().await;
        if last_sequence > *guard {
            *guard = last_sequence;
        }

        Ok(Response::new(ReplicationAck {
            applied_sequence: *guard,
        }))
    }

    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let applied = *self.state.last_sequence.lock().await;
        Ok(Response::new(HeartbeatResponse {
            applied_sequence: applied,
            pending_events: 0,
        }))
    }
}
