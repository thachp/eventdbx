use parking_lot::Mutex;
use tokio::runtime::Builder;
use tokio_stream::iter;
use tonic::{Request, transport::Channel};

use crate::{
    config::GrpcPluginConfig,
    error::{EventError, Result},
    replication::{
        convert_event, normalize_endpoint,
        proto::{EventBatch, replication_client::ReplicationClient},
    },
    schema::AggregateSchema,
    store::{AggregateState, EventRecord},
};

use super::Plugin;

pub struct GrpcPlugin {
    config: GrpcPluginConfig,
    runtime: tokio::runtime::Runtime,
    client: Mutex<Option<ReplicationClient<Channel>>>,
}

impl GrpcPlugin {
    pub fn new(config: GrpcPluginConfig) -> Self {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build gRPC runtime");
        Self {
            config,
            runtime,
            client: Mutex::new(None),
        }
    }

    pub fn ensure_ready(&self) -> Result<()> {
        let endpoint = self.config.endpoint.clone();
        self.runtime.block_on(async {
            let _ = connect(&endpoint).await?;
            Ok(())
        })
    }
}

impl Plugin for GrpcPlugin {
    fn name(&self) -> &'static str {
        "grpc"
    }

    fn notify_event(
        &self,
        record: &EventRecord,
        _state: &AggregateState,
        _schema: Option<&AggregateSchema>,
    ) -> Result<()> {
        let batch = EventBatch {
            sequence: record.version,
            events: vec![convert_event(record)?],
        };
        let endpoint = self.config.endpoint.clone();

        self.runtime.block_on(async {
            let mut guard = self.client.lock();
            if guard.is_none() {
                let client = connect(&endpoint).await?;
                *guard = Some(client);
            }

            if let Some(client) = guard.as_mut() {
                if let Err(err) = client
                    .apply_events(Request::new(iter(vec![batch.clone()])))
                    .await
                {
                    *guard = None;
                    return Err(EventError::Storage(err.to_string()));
                }
                Ok(())
            } else {
                Err(EventError::Storage(
                    "grpc client unavailable after initialization".into(),
                ))
            }
        })
    }
}

async fn connect(endpoint: &str) -> Result<ReplicationClient<Channel>> {
    let endpoint = normalize_endpoint(endpoint)?;
    let channel = tonic::transport::Endpoint::try_from(endpoint.clone())
        .map_err(|err| EventError::Config(err.to_string()))?
        .connect()
        .await
        .map_err(|err| EventError::Storage(err.to_string()))?;
    Ok(ReplicationClient::new(channel))
}
