use std::{sync::Arc, time::Duration};

use tokio::{sync::mpsc, task, time::sleep};
use tracing::{debug, info, warn};

use crate::{
    config::{Config, RemoteConfig},
    encryption::Encryptor,
    error::{EventError, Result},
    replication_capnp_client::{CapnpReplicationClient, normalize_capnp_endpoint},
    replication_planner::{
        build_push_plans, positions_to_map, summarize_plans, validate_fast_forward,
    },
    store::{AggregatePositionEntry, EventRecord, EventStore},
};

const REPLICATION_CHANNEL_CAPACITY: usize = 128;
const CATCH_UP_BATCH_SIZE: usize = 512;

#[derive(Clone)]
pub struct ReplicationManager {
    remotes: Arc<Vec<RemoteHandle>>,
}

struct RemoteHandle {
    name: String,
    sender: mpsc::Sender<Vec<EventRecord>>,
}

impl ReplicationManager {
    pub fn from_config(
        config: &Config,
        store: Arc<EventStore>,
        encryptor: Option<Encryptor>,
    ) -> Option<Self> {
        if config.remotes.is_empty() {
            return None;
        }

        let mut handles = Vec::new();
        for (name, remote_config) in &config.remotes {
            let (tx, rx) = mpsc::channel(REPLICATION_CHANNEL_CAPACITY);
            let worker = RemoteWorker::new(
                name.clone(),
                remote_config.clone(),
                Arc::clone(&store),
                rx,
                encryptor.clone(),
            );
            tokio::spawn(async move { worker.run().await });
            handles.push(RemoteHandle {
                name: name.clone(),
                sender: tx,
            });
        }

        if handles.is_empty() {
            None
        } else {
            Some(Self {
                remotes: Arc::new(handles),
            })
        }
    }

    pub fn enqueue(&self, events: &[EventRecord]) {
        if events.is_empty() {
            return;
        }

        for handle in self.remotes.iter() {
            let batch: Vec<EventRecord> = events.iter().cloned().collect();
            let sender = handle.sender.clone();
            let remote_name = handle.name.clone();
            tokio::spawn(async move {
                if let Err(err) = sender.send(batch).await {
                    warn!(
                        target: "replication",
                        remote = %remote_name,
                        "failed to queue replication batch: {}",
                        err
                    );
                }
            });
        }
    }
}

struct RemoteWorker {
    name: String,
    config: RemoteConfig,
    store: Arc<EventStore>,
    receiver: mpsc::Receiver<Vec<EventRecord>>,
    pending: Option<Vec<EventRecord>>,
    sequence: u64,
    encryptor: Option<Encryptor>,
}

impl RemoteWorker {
    fn new(
        name: String,
        config: RemoteConfig,
        store: Arc<EventStore>,
        receiver: mpsc::Receiver<Vec<EventRecord>>,
        encryptor: Option<Encryptor>,
    ) -> Self {
        Self {
            name,
            config,
            store,
            receiver,
            pending: None,
            sequence: 0,
            encryptor,
        }
    }

    async fn run(mut self) {
        let mut attempt: u32 = 0;
        loop {
            match self.connect_and_process().await {
                Ok(should_continue) => {
                    attempt = 0;
                    if !should_continue {
                        info!(
                            target: "replication",
                            remote = %self.name,
                            "replication worker stopped (channel closed)"
                        );
                        break;
                    }
                }
                Err(err) => {
                    attempt = attempt.saturating_add(1);
                    warn!(
                        target: "replication",
                        remote = %self.name,
                        attempt,
                        "replication error: {}",
                        err
                    );
                    sleep(Self::backoff_delay(attempt)).await;
                }
            }
        }
    }

    async fn connect_and_process(&mut self) -> Result<bool> {
        let mut client = self.connect().await?;
        self.bootstrap(&mut client).await?;
        self.flush_pending(&mut client).await?;

        while let Some(events) = self.receiver.recv().await {
            if events.is_empty() {
                continue;
            }

            if let Err(err) = self.send_batch(&mut client, events.as_slice()).await {
                self.pending = Some(events);
                return Err(err);
            }
        }

        // Channel closed; attempt to flush any buffered work one last time.
        self.flush_pending(&mut client).await?;
        Ok(false)
    }

    async fn bootstrap(&mut self, client: &mut CapnpReplicationClient) -> Result<()> {
        let remote_positions = client
            .list_positions()
            .await
            .map_err(|err| EventError::Storage(err.to_string()))?;
        let local_positions = self.load_local_positions().await?;

        let remote_map = positions_to_map(&remote_positions, None);
        let local_map = positions_to_map(&local_positions, None);

        if let Err(err) = validate_fast_forward(&self.name, &local_map, &remote_map) {
            return Err(EventError::Storage(err.to_string()));
        }

        let plans = build_push_plans(&local_map, &remote_map);
        if plans.is_empty() {
            debug!(
                target: "replication",
                remote = %self.name,
                "remote already synchronized"
            );
            return Ok(());
        }

        let stats = summarize_plans(&plans);
        debug!(
            target: "replication",
            remote = %self.name,
            aggregates = stats.len(),
            "starting replication catch-up"
        );

        for plan in &plans {
            let mut current = plan.from_version;
            while current < plan.to_version {
                let events = self
                    .load_events(&plan.aggregate_type, &plan.aggregate_id, current)
                    .await?;
                if events.is_empty() {
                    break;
                }
                current = events
                    .last()
                    .map(|record| record.version)
                    .unwrap_or(current);
                self.send_batch(client, events.as_slice()).await?;
            }
        }

        info!(
            target: "replication",
            remote = %self.name,
            aggregates = stats.len(),
            "completed replication catch-up"
        );

        Ok(())
    }

    async fn flush_pending(&mut self, client: &mut CapnpReplicationClient) -> Result<()> {
        if let Some(events) = self.pending.take() {
            if let Err(err) = self.send_batch(client, events.as_slice()).await {
                self.pending = Some(events);
                return Err(err);
            }
        }
        Ok(())
    }

    async fn load_local_positions(&self) -> Result<Vec<AggregatePositionEntry>> {
        let store = Arc::clone(&self.store);
        Ok(task::spawn_blocking(move || store.aggregate_positions())
            .await
            .map_err(|err| {
                EventError::Storage(format!("replication positions task failed: {err}"))
            })??)
    }

    async fn load_events(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
        from_version: u64,
    ) -> Result<Vec<EventRecord>> {
        let store = Arc::clone(&self.store);
        let aggregate_type = aggregate_type.to_string();
        let aggregate_id = aggregate_id.to_string();
        Ok(task::spawn_blocking(move || {
            store.events_after(
                &aggregate_type,
                &aggregate_id,
                from_version,
                Some(CATCH_UP_BATCH_SIZE),
            )
        })
        .await
        .map_err(|err| EventError::Storage(format!("replication events task failed: {err}")))??)
    }

    async fn send_batch(
        &mut self,
        client: &mut CapnpReplicationClient,
        events: &[EventRecord],
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        self.sequence = self.sequence.wrapping_add(1);
        let applied = client
            .apply_events(self.sequence, events)
            .await
            .map_err(|err| EventError::Storage(err.to_string()))?;
        if applied != self.sequence {
            warn!(
                target: "replication",
                remote = %self.name,
                expected = self.sequence,
                acknowledged = applied,
                "replication ack sequence mismatch"
            );
        } else {
            debug!(
                target: "replication",
                remote = %self.name,
                sequence = self.sequence,
                count = events.len(),
                "replicated batch"
            );
        }
        Ok(())
    }

    async fn connect(&self) -> Result<CapnpReplicationClient> {
        let endpoint = normalize_capnp_endpoint(&self.config.endpoint)
            .map_err(|err| EventError::Config(err.to_string()))?;
        let token = self.config.resolved_token(self.encryptor.as_ref())?;
        CapnpReplicationClient::connect(&endpoint, &token)
            .await
            .map_err(|err| EventError::Storage(err.to_string()))
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
