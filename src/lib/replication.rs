use std::{sync::Arc, time::Duration};

use tokio::{sync::mpsc, time::sleep};
use tracing::{info, warn};

use crate::{
    config::{Config, RemoteConfig},
    error::{EventError, Result},
    replication_capnp_client::{
        decode_public_key_bytes, normalize_capnp_endpoint, CapnpReplicationClient,
    },
    store::EventRecord,
};

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
                match self.send_batch(events.as_slice()).await {
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

        let applied = client
            .apply_events(self.sequence, events)
            .await
            .map_err(|err| EventError::Storage(err.to_string()))?;
        info!(
            "remote '{}' acknowledged sequence {} (applied {})",
            self.name, self.sequence, applied
        );
        Ok(())
    }

    async fn connect(&self) -> Result<CapnpReplicationClient> {
        let endpoint = normalize_capnp_endpoint(&self.config.endpoint)
            .map_err(|err| EventError::Config(err.to_string()))?;
        let expected_key = decode_public_key_bytes(&self.config.public_key)
            .map_err(|err| EventError::Config(err.to_string()))?;
        CapnpReplicationClient::connect(&endpoint, &expected_key)
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
