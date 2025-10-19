pub mod proto {
    tonic::include_proto!("eventdbx.replication");
}

use std::{sync::Arc, time::Duration};

use proto::{
    AggregatePosition, EventBatch, EventRecord as ProtoEventRecord, HeartbeatRequest,
    HeartbeatResponse, ListPositionsRequest, ListPositionsResponse, PullEventsRequest,
    PullEventsResponse, ReplicationAck, SnapshotChunk, SnapshotRequest,
    replication_client::ReplicationClient, replication_server::Replication,
};
use tokio::{sync::mpsc, time::sleep};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, transport::Channel};
use tracing::{error, info, warn};

use crate::{
    config::{Config, RemoteConfig},
    error::{EventError, Result},
    store::{EventMetadata, EventRecord, EventStore},
};

#[derive(Clone, Default)]
pub struct ReplicationState {
    pub last_sequence: Arc<tokio::sync::Mutex<u64>>,
}

#[derive(Clone)]
pub struct ReplicationService {
    state: ReplicationState,
    store: Arc<EventStore>,
}

impl ReplicationService {
    pub fn new(store: Arc<EventStore>) -> Self {
        Self {
            state: ReplicationState::default(),
            store,
        }
    }

    pub fn state(&self) -> ReplicationState {
        self.state.clone()
    }
}

#[tonic::async_trait]
impl Replication for ReplicationService {
    type BootstrapSnapshotStream = ReceiverStream<std::result::Result<SnapshotChunk, Status>>;

    async fn bootstrap_snapshot(
        &self,
        _request: Request<SnapshotRequest>,
    ) -> std::result::Result<Response<Self::BootstrapSnapshotStream>, Status> {
        let (tx, rx) = mpsc::channel(1);
        drop(tx);
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn apply_events(
        &self,
        request: Request<tonic::Streaming<EventBatch>>,
    ) -> std::result::Result<Response<ReplicationAck>, Status> {
        let mut stream = request.into_inner();
        let mut last_sequence = 0u64;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            for event in batch.events.iter() {
                let record =
                    decode_event(event).map_err(|err| Status::internal(err.to_string()))?;
                self.store
                    .append_replica(record)
                    .map_err(|err| Status::internal(err.to_string()))?;
            }
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
    ) -> std::result::Result<Response<HeartbeatResponse>, Status> {
        let applied = *self.state.last_sequence.lock().await;
        Ok(Response::new(HeartbeatResponse {
            applied_sequence: applied,
            pending_events: 0,
        }))
    }

    async fn list_positions(
        &self,
        _request: Request<ListPositionsRequest>,
    ) -> std::result::Result<Response<ListPositionsResponse>, Status> {
        let positions = self
            .store
            .aggregate_positions()
            .map_err(|err| Status::internal(err.to_string()))?;

        let proto_positions = positions
            .into_iter()
            .map(|entry| AggregatePosition {
                aggregate_type: entry.aggregate_type,
                aggregate_id: entry.aggregate_id,
                version: entry.version,
            })
            .collect();

        Ok(Response::new(ListPositionsResponse {
            positions: proto_positions,
        }))
    }

    async fn pull_events(
        &self,
        request: Request<PullEventsRequest>,
    ) -> std::result::Result<Response<PullEventsResponse>, Status> {
        let req = request.into_inner();
        let limit = if req.limit == 0 {
            None
        } else {
            Some(req.limit as usize)
        };

        let events = self
            .store
            .events_after(
                &req.aggregate_type,
                &req.aggregate_id,
                req.from_version,
                limit,
            )
            .map_err(|err| Status::internal(err.to_string()))?;

        let proto_events = events
            .into_iter()
            .map(|event| convert_event(&event).map_err(|err| Status::internal(err.to_string())))
            .collect::<std::result::Result<Vec<_>, Status>>()?;

        Ok(Response::new(PullEventsResponse {
            events: proto_events,
        }))
    }
}

#[derive(Clone)]
pub struct ReplicationManager {
    remotes: Arc<Vec<RemoteHandle>>,
}

struct RemoteHandle {
    name: String,
    sender: mpsc::Sender<Vec<EventRecord>>,
}

impl ReplicationManager {
    pub fn from_config(config: &Config) -> Self {
        let mut handles = Vec::new();
        for (name, remote_config) in &config.remotes {
            let (tx, rx) = mpsc::channel(64);
            let worker = RemoteWorker::new(name.clone(), remote_config.clone(), rx);
            tokio::spawn(async move { worker.run().await });
            handles.push(RemoteHandle {
                name: name.clone(),
                sender: tx,
            });
        }

        Self {
            remotes: Arc::new(handles),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.remotes.is_empty()
    }

    pub fn enqueue(&self, events: &[EventRecord]) {
        if events.is_empty() {
            return;
        }

        for handle in self.remotes.iter() {
            let mut batch = Vec::with_capacity(events.len());
            for event in events {
                batch.push(event.clone());
            }

            let sender = handle.sender.clone();
            let remote_name = handle.name.clone();
            tokio::spawn(async move {
                if let Err(err) = sender.send(batch).await {
                    warn!(
                        "failed to queue replication batch for {}: {}",
                        remote_name, err
                    );
                }
            });
        }
    }
}

struct RemoteWorker {
    name: String,
    config: RemoteConfig,
    receiver: mpsc::Receiver<Vec<EventRecord>>,
    sequence: u64,
}

impl RemoteWorker {
    fn new(name: String, config: RemoteConfig, receiver: mpsc::Receiver<Vec<EventRecord>>) -> Self {
        Self {
            name,
            config,
            receiver,
            sequence: 0,
        }
    }

    async fn run(mut self) {
        while let Some(events) = self.receiver.recv().await {
            if events.is_empty() {
                continue;
            }

            let mut attempt: u32 = 0;
            loop {
                match self.send_batch(&events).await {
                    Ok(_) => break,
                    Err(err) => {
                        attempt += 1;
                        warn!(
                            "replication to remote '{}' failed on attempt {}: {}",
                            self.name, attempt, err
                        );
                        let delay = Self::backoff_delay(attempt);
                        sleep(delay).await;
                    }
                }
            }
        }
    }

    async fn send_batch(&mut self, events: &[EventRecord]) -> Result<()> {
        let mut client = self.connect().await?;
        self.sequence = self.sequence.wrapping_add(1);
        let message = EventBatch {
            sequence: self.sequence,
            events: events
                .iter()
                .filter_map(|record| match convert_event(record) {
                    Ok(event) => Some(event),
                    Err(err) => {
                        error!(
                            "failed to encode event {}::{} version {} for replication: {}",
                            record.aggregate_type, record.aggregate_id, record.version, err
                        );
                        None
                    }
                })
                .collect(),
        };

        if message.events.is_empty() {
            return Ok(());
        }

        let stream = tokio_stream::iter(vec![message]);

        let response = client
            .apply_events(Request::new(stream))
            .await
            .map_err(|err| EventError::Storage(err.to_string()))?;
        let ack = response.into_inner();
        info!(
            "remote '{}' acknowledged sequence {} (applied {})",
            self.name, self.sequence, ack.applied_sequence
        );
        Ok(())
    }

    async fn connect(&self) -> Result<ReplicationClient<Channel>> {
        let endpoint = normalize_endpoint(&self.config.endpoint)?;
        let endpoint = tonic::transport::Endpoint::try_from(endpoint)
            .map_err(|err| EventError::Config(err.to_string()))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|err| EventError::Storage(err.to_string()))?;
        Ok(ReplicationClient::new(channel))
    }

    fn backoff_delay(attempt: u32) -> Duration {
        match attempt {
            0 | 1 => Duration::from_secs(1),
            2 => Duration::from_secs(2),
            3 => Duration::from_secs(4),
            _ => Duration::from_secs(10),
        }
    }
}

pub fn convert_event(record: &EventRecord) -> Result<ProtoEventRecord> {
    let payload = serde_json::to_vec(&record.payload)?;
    let metadata = serde_json::to_vec(&record.metadata)?;

    Ok(ProtoEventRecord {
        aggregate_type: record.aggregate_type.clone(),
        aggregate_id: record.aggregate_id.clone(),
        event_type: record.event_type.clone(),
        version: record.version,
        merkle_root: record.merkle_root.clone(),
        payload,
        metadata,
        hash: record.hash.clone(),
    })
}

pub fn decode_event(proto: &ProtoEventRecord) -> Result<EventRecord> {
    let payload: serde_json::Value = serde_json::from_slice(&proto.payload)?;
    let metadata: EventMetadata = serde_json::from_slice(&proto.metadata)?;

    Ok(EventRecord {
        aggregate_type: proto.aggregate_type.clone(),
        aggregate_id: proto.aggregate_id.clone(),
        event_type: proto.event_type.clone(),
        payload,
        metadata,
        version: proto.version,
        hash: proto.hash.clone(),
        merkle_root: proto.merkle_root.clone(),
    })
}

pub fn normalize_endpoint(endpoint: &str) -> Result<String> {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        return Ok(endpoint.to_string());
    }
    if let Some(rest) = endpoint.strip_prefix("grpc://") {
        return Ok(format!("http://{}", rest));
    }
    if let Some(rest) = endpoint.strip_prefix("grpcs://") {
        return Ok(format!("https://{}", rest));
    }
    Err(EventError::Config(format!(
        "unsupported replication endpoint scheme: {}",
        endpoint
    )))
}
