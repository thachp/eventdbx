use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use json_patch::Patch;
use once_cell::sync::Lazy;
use rand::{Rng, SeedableRng, distributions::Alphanumeric, rngs::StdRng};
use serde_json::{Value, json};

const AGGREGATE_TYPE: &str = "orders";
const EVENT_APPLY_TYPE: &str = "order_created";
const EVENT_PATCH_TYPE: &str = "order_patched";
const APPLY_PAYLOAD_SIZES: &[usize] = &[256, 1024, 4096];
const SEED_COUNT: usize = 1024;
const LIST_EVENT_COUNT: usize = 512;
const LIST_LIMIT: usize = 128;
const LIST_AGGREGATE_ID: &str = "list-benchmark";

static PATCH_TEMPLATE: Lazy<Value> = Lazy::new(|| {
    json!([
        { "op": "replace", "path": "/description", "value": "patched description" },
        { "op": "add", "path": "/status", "value": "patched" }
    ])
});

static PATCH_DOC: Lazy<Patch> =
    Lazy::new(|| serde_json::from_value(PATCH_TEMPLATE.clone()).expect("patch template is valid"));

fn criterion_benches() -> Criterion {
    Criterion::default().warm_up_time(std::time::Duration::from_secs(3))
}

fn bench_apply(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply");
    let payloads: Vec<Value> = APPLY_PAYLOAD_SIZES
        .iter()
        .map(|&size| build_payload(size))
        .collect();

    let mut eventdbx = EventdbxBackend::new().expect("eventdbx backend");
    for (idx, &size) in APPLY_PAYLOAD_SIZES.iter().enumerate() {
        let payload = &payloads[idx];
        group.bench_with_input(BenchmarkId::new("eventdbx", size), payload, |b, payload| {
            b.iter(|| {
                let key = eventdbx.apply_new(payload).expect("eventdbx apply");
                black_box(key.event_id);
            });
        });
    }

    #[cfg(feature = "bench-postgres")]
    {
        if let Some(mut backend) = postgres::PostgresBackend::from_env().expect("postgres init") {
            for (idx, &size) in APPLY_PAYLOAD_SIZES.iter().enumerate() {
                let payload = &payloads[idx];
                group.bench_with_input(
                    BenchmarkId::new("postgres", size),
                    payload,
                    |b, payload| {
                        b.iter(|| {
                            let key = backend.apply_new(payload).expect("postgres apply");
                            black_box(key.id);
                        });
                    },
                );
            }
        } else {
            eprintln!("Skipping postgres apply benchmark: set EVENTDBX_PG_DSN");
        }
    }

    #[cfg(feature = "bench-mongodb")]
    {
        if let Some(mut backend) = mongodb_backend::MongoBackend::from_env().expect("mongodb init")
        {
            for (idx, &size) in APPLY_PAYLOAD_SIZES.iter().enumerate() {
                let payload = &payloads[idx];
                group.bench_with_input(BenchmarkId::new("mongodb", size), payload, |b, payload| {
                    b.iter(|| {
                        let key = backend.apply_new(payload).expect("mongodb apply");
                        black_box(key.id.clone());
                    });
                });
            }
        } else {
            eprintln!("Skipping mongodb apply benchmark: set EVENTDBX_MONGO_URI");
        }
    }

    #[cfg(feature = "bench-sqlite")]
    {
        if let Some(mut backend) = sqlite::SqliteBackend::from_env().expect("sqlite init") {
            for (idx, &size) in APPLY_PAYLOAD_SIZES.iter().enumerate() {
                let payload = &payloads[idx];
                group.bench_with_input(BenchmarkId::new("sqlite", size), payload, |b, payload| {
                    b.iter(|| {
                        let key = backend.apply_new(payload).expect("sqlite apply");
                        black_box(key.id);
                    });
                });
            }
        } else {
            eprintln!(
                "Skipping sqlite apply benchmark: set EVENTDBX_SQLITE_PATH or allow temp file"
            );
        }
    }

    #[cfg(feature = "bench-mssql")]
    {
        if let Some(mut backend) = mssql::MssqlBackend::from_env().expect("mssql init") {
            for (idx, &size) in APPLY_PAYLOAD_SIZES.iter().enumerate() {
                let payload = &payloads[idx];
                group.bench_with_input(BenchmarkId::new("mssql", size), payload, |b, payload| {
                    b.iter(|| {
                        let key = backend.apply_new(payload).expect("mssql apply");
                        black_box(key.id);
                    });
                });
            }
        } else {
            eprintln!("Skipping SQL Server apply benchmark: set EVENTDBX_MSSQL_DSN");
        }
    }

    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");
    let seed_payload = build_payload(512);

    let mut eventdbx = EventdbxBackend::new().expect("eventdbx backend");
    let eventdbx_keys = eventdbx
        .seed(&seed_payload, SEED_COUNT)
        .expect("seed eventdbx get dataset");
    let mut eventdbx_cycle = KeyCycle::new(eventdbx_keys);
    group.bench_function("eventdbx", |b| {
        b.iter(|| {
            let key = eventdbx_cycle.next();
            let payload = eventdbx.get(&key).expect("eventdbx get");
            black_box(payload);
        });
    });

    #[cfg(feature = "bench-postgres")]
    {
        if let Some(mut backend) = postgres::PostgresBackend::from_env().expect("postgres init") {
            let keys = backend
                .seed(&seed_payload, SEED_COUNT)
                .expect("seed postgres get dataset");
            let mut cycle = KeyCycle::new(keys);
            group.bench_function("postgres", |b| {
                b.iter(|| {
                    let key = cycle.next();
                    let payload = backend.get(&key).expect("postgres get");
                    black_box(payload);
                });
            });
        }
    }

    #[cfg(feature = "bench-mongodb")]
    {
        if let Some(mut backend) = mongodb_backend::MongoBackend::from_env().expect("mongodb init")
        {
            let keys = backend
                .seed(&seed_payload, SEED_COUNT)
                .expect("seed mongodb get dataset");
            let mut cycle = KeyCycle::new(keys);
            group.bench_function("mongodb", |b| {
                b.iter(|| {
                    let key = cycle.next();
                    let payload = backend.get(&key).expect("mongodb get");
                    black_box(payload);
                });
            });
        }
    }

    #[cfg(feature = "bench-sqlite")]
    {
        if let Some(mut backend) = sqlite::SqliteBackend::from_env().expect("sqlite init") {
            let keys = backend
                .seed(&seed_payload, SEED_COUNT)
                .expect("seed sqlite get dataset");
            let mut cycle = KeyCycle::new(keys);
            group.bench_function("sqlite", |b| {
                b.iter(|| {
                    let key = cycle.next();
                    let payload = backend.get(&key).expect("sqlite get");
                    black_box(payload);
                });
            });
        }
    }

    #[cfg(feature = "bench-mssql")]
    {
        if let Some(mut backend) = mssql::MssqlBackend::from_env().expect("mssql init") {
            let keys = backend
                .seed(&seed_payload, SEED_COUNT)
                .expect("seed mssql get dataset");
            let mut cycle = KeyCycle::new(keys);
            group.bench_function("mssql", |b| {
                b.iter(|| {
                    let key = cycle.next();
                    let payload = backend.get(&key).expect("mssql get");
                    black_box(payload);
                });
            });
        }
    }

    group.finish();
}

fn bench_patch(c: &mut Criterion) {
    let mut group = c.benchmark_group("patch");
    let seed_payload = build_payload(512);

    let mut eventdbx = EventdbxBackend::new().expect("eventdbx backend");
    let eventdbx_keys = eventdbx
        .seed(&seed_payload, SEED_COUNT / 2)
        .expect("seed eventdbx patch dataset");
    let mut eventdbx_cycle = KeyCycle::new(eventdbx_keys);
    group.bench_function("eventdbx", |b| {
        b.iter(|| {
            let key = eventdbx_cycle.next();
            eventdbx
                .patch(&key, &PATCH_TEMPLATE, &PATCH_DOC)
                .expect("eventdbx patch");
        });
    });

    #[cfg(feature = "bench-postgres")]
    {
        if let Some(mut backend) = postgres::PostgresBackend::from_env().expect("postgres init") {
            let keys = backend
                .seed(&seed_payload, SEED_COUNT / 2)
                .expect("seed postgres patch dataset");
            let mut cycle = KeyCycle::new(keys);
            group.bench_function("postgres", |b| {
                b.iter(|| {
                    let key = cycle.next();
                    backend
                        .patch(&key, &PATCH_TEMPLATE, &PATCH_DOC)
                        .expect("postgres patch");
                });
            });
        }
    }

    #[cfg(feature = "bench-mongodb")]
    {
        if let Some(mut backend) = mongodb_backend::MongoBackend::from_env().expect("mongodb init")
        {
            let keys = backend
                .seed(&seed_payload, SEED_COUNT / 2)
                .expect("seed mongodb patch dataset");
            let mut cycle = KeyCycle::new(keys);
            group.bench_function("mongodb", |b| {
                b.iter(|| {
                    let key = cycle.next();
                    backend
                        .patch(&key, &PATCH_TEMPLATE, &PATCH_DOC)
                        .expect("mongodb patch");
                });
            });
        }
    }

    #[cfg(feature = "bench-sqlite")]
    {
        if let Some(mut backend) = sqlite::SqliteBackend::from_env().expect("sqlite init") {
            let keys = backend
                .seed(&seed_payload, SEED_COUNT / 2)
                .expect("seed sqlite patch dataset");
            let mut cycle = KeyCycle::new(keys);
            group.bench_function("sqlite", |b| {
                b.iter(|| {
                    let key = cycle.next();
                    backend
                        .patch(&key, &PATCH_TEMPLATE, &PATCH_DOC)
                        .expect("sqlite patch");
                });
            });
        }
    }

    #[cfg(feature = "bench-mssql")]
    {
        if let Some(mut backend) = mssql::MssqlBackend::from_env().expect("mssql init") {
            let keys = backend
                .seed(&seed_payload, SEED_COUNT / 2)
                .expect("seed mssql patch dataset");
            let mut cycle = KeyCycle::new(keys);
            group.bench_function("mssql", |b| {
                b.iter(|| {
                    let key = cycle.next();
                    backend
                        .patch(&key, &PATCH_TEMPLATE, &PATCH_DOC)
                        .expect("mssql patch");
                });
            });
        }
    }

    group.finish();
}

fn bench_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("list");
    let seed_payload = build_payload(512);

    let mut eventdbx = EventdbxBackend::new().expect("eventdbx backend");
    eventdbx
        .seed_list_dataset(LIST_AGGREGATE_ID, &seed_payload, LIST_EVENT_COUNT)
        .expect("seed eventdbx list dataset");
    group.bench_function("eventdbx", |b| {
        b.iter(|| {
            let events = eventdbx
                .list(LIST_AGGREGATE_ID, LIST_LIMIT)
                .expect("eventdbx list");
            black_box(events);
        });
    });

    #[cfg(feature = "bench-postgres")]
    {
        if let Some(mut backend) = postgres::PostgresBackend::from_env().expect("postgres init") {
            backend
                .seed_list_dataset(LIST_AGGREGATE_ID, &seed_payload, LIST_EVENT_COUNT)
                .expect("seed postgres list dataset");
            group.bench_function("postgres", |b| {
                b.iter(|| {
                    let events = backend
                        .list(LIST_AGGREGATE_ID, LIST_LIMIT)
                        .expect("postgres list");
                    black_box(events);
                });
            });
        }
    }

    #[cfg(feature = "bench-mongodb")]
    {
        if let Some(mut backend) = mongodb_backend::MongoBackend::from_env().expect("mongodb init")
        {
            backend
                .seed_list_dataset(LIST_AGGREGATE_ID, &seed_payload, LIST_EVENT_COUNT)
                .expect("seed mongodb list dataset");
            group.bench_function("mongodb", |b| {
                b.iter(|| {
                    let events = backend
                        .list(LIST_AGGREGATE_ID, LIST_LIMIT)
                        .expect("mongodb list");
                    black_box(events);
                });
            });
        }
    }

    #[cfg(feature = "bench-sqlite")]
    {
        if let Some(mut backend) = sqlite::SqliteBackend::from_env().expect("sqlite init") {
            backend
                .seed_list_dataset(LIST_AGGREGATE_ID, &seed_payload, LIST_EVENT_COUNT)
                .expect("seed sqlite list dataset");
            group.bench_function("sqlite", |b| {
                b.iter(|| {
                    let events = backend
                        .list(LIST_AGGREGATE_ID, LIST_LIMIT)
                        .expect("sqlite list");
                    black_box(events);
                });
            });
        }
    }

    #[cfg(feature = "bench-mssql")]
    {
        if let Some(mut backend) = mssql::MssqlBackend::from_env().expect("mssql init") {
            backend
                .seed_list_dataset(LIST_AGGREGATE_ID, &seed_payload, LIST_EVENT_COUNT)
                .expect("seed mssql list dataset");
            group.bench_function("mssql", |b| {
                b.iter(|| {
                    let events = backend
                        .list(LIST_AGGREGATE_ID, LIST_LIMIT)
                        .expect("mssql list");
                    black_box(events);
                });
            });
        }
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_benches();
    targets = bench_apply, bench_get, bench_patch, bench_list
}
criterion_main!(benches);

fn build_payload(size: usize) -> Value {
    let mut rng = StdRng::from_entropy();
    let text: String = (0..size)
        .map(|_| rng.sample(Alphanumeric) as char)
        .collect();
    json!({
        "amount": 42,
        "currency": "USD",
        "description": text,
    })
}

struct KeyCycle<K> {
    keys: Arc<Vec<K>>,
    index: usize,
}

impl<K: Clone> KeyCycle<K> {
    fn new(keys: Vec<K>) -> Self {
        assert!(!keys.is_empty(), "key cycle requires at least one key");
        Self {
            keys: Arc::new(keys),
            index: 0,
        }
    }

    fn next(&mut self) -> K {
        let key = self.keys[self.index].clone();
        self.index = (self.index + 1) % self.keys.len();
        key
    }
}

fn next_aggregate_id(counter: &mut u64) -> String {
    *counter += 1;
    format!("agg-{}", counter)
}

#[derive(Clone)]
struct EventdbxKey {
    aggregate_id: String,
    event_id: u64,
}

struct EventdbxBackend {
    store: eventdbx::store::EventStore,
    _temp_dir: tempfile::TempDir,
    write_counter: u64,
}

impl EventdbxBackend {
    fn new() -> Result<Self> {
        use eventdbx::store::EventStore;

        let temp_dir = tempfile::tempdir().context("create temp dir for eventdbx store")?;
        let store = EventStore::open(temp_dir.path().to_path_buf(), None, 0)?;

        Ok(Self {
            store,
            _temp_dir: temp_dir,
            write_counter: 0,
        })
    }

    fn apply_new(&mut self, payload: &Value) -> Result<EventdbxKey> {
        let aggregate_id = next_aggregate_id(&mut self.write_counter);
        self.apply_with(&aggregate_id, EVENT_APPLY_TYPE, payload)
    }

    fn apply_with(
        &self,
        aggregate_id: &str,
        event_type: &str,
        payload: &Value,
    ) -> Result<EventdbxKey> {
        use eventdbx::store::AppendEvent;

        let record = self.store.append(AppendEvent {
            aggregate_type: AGGREGATE_TYPE.to_string(),
            aggregate_id: aggregate_id.to_string(),
            event_type: event_type.to_string(),
            payload: payload.clone(),
            metadata: None,
            issued_by: None,
            note: None,
        })?;

        Ok(EventdbxKey {
            aggregate_id: aggregate_id.to_string(),
            event_id: record.metadata.event_id.as_u64(),
        })
    }

    fn seed(&mut self, payload: &Value, count: usize) -> Result<Vec<EventdbxKey>> {
        let mut keys = Vec::with_capacity(count);
        for _ in 0..count {
            keys.push(self.apply_new(payload)?);
        }
        Ok(keys)
    }

    fn get(&self, key: &EventdbxKey) -> Result<Value> {
        use eventdbx::snowflake::SnowflakeId;

        let record = self
            .store
            .find_event_by_id(SnowflakeId::from_u64(key.event_id))?
            .ok_or_else(|| anyhow!("event {} not found", key.event_id))?;
        Ok(record.payload)
    }

    fn patch(&mut self, key: &EventdbxKey, patch_value: &Value, _patch_ops: &Patch) -> Result<()> {
        let payload = self.store.prepare_payload_from_patch(
            AGGREGATE_TYPE,
            &key.aggregate_id,
            patch_value,
        )?;
        let _ = self.apply_with(&key.aggregate_id, EVENT_PATCH_TYPE, &payload)?;
        Ok(())
    }

    fn list(&self, aggregate_id: &str, limit: usize) -> Result<Vec<Value>> {
        let events =
            self.store
                .list_events_paginated(AGGREGATE_TYPE, aggregate_id, 0, Some(limit))?;
        Ok(events.into_iter().map(|record| record.payload).collect())
    }

    fn seed_list_dataset(
        &mut self,
        aggregate_id: &str,
        payload: &Value,
        count: usize,
    ) -> Result<()> {
        for _ in 0..count {
            let _ = self.apply_with(aggregate_id, EVENT_APPLY_TYPE, payload)?;
        }
        Ok(())
    }
}

#[cfg(feature = "bench-postgres")]
mod postgres {
    use super::*;
    use tokio::runtime::Runtime;
    use tokio_postgres::{Client, NoTls, types::Json};

    #[derive(Clone)]
    pub struct PostgresKey {
        pub id: i64,
        pub aggregate_id: String,
    }

    pub struct PostgresBackend {
        runtime: Runtime,
        client: Client,
        table: String,
        insert_sql: String,
        select_sql: String,
        update_sql: String,
        list_sql: String,
        write_counter: u64,
    }

    impl PostgresBackend {
        pub fn from_env() -> Result<Option<Self>> {
            let url = match std::env::var("EVENTDBX_PG_DSN") {
                Ok(url) => url,
                Err(_) => return Ok(None),
            };
            let table = std::env::var("EVENTDBX_PG_TABLE")
                .unwrap_or_else(|_| "eventdbx_bench_events".into());

            let runtime = Runtime::new().context("create tokio runtime for postgres")?;
            let (client, connection) = runtime
                .block_on(tokio_postgres::connect(&url, NoTls))
                .context("connect to postgres")?;

            runtime.spawn(async move {
                if let Err(err) = connection.await {
                    eprintln!("postgres connection error: {err}");
                }
            });

            let insert_sql = format!(
                "INSERT INTO {table} (aggregate_type, aggregate_id, event_type, payload) \
                 VALUES ($1, $2, $3, $4::jsonb) RETURNING id",
            );
            let select_sql = format!("SELECT payload FROM {table} WHERE id = $1");
            let update_sql = format!("UPDATE {table} SET payload = $1::jsonb WHERE id = $2");
            let list_sql = format!(
                "SELECT payload FROM {table} WHERE aggregate_id = $1 \
                 ORDER BY id DESC LIMIT $2"
            );

            let mut backend = Self {
                runtime,
                client,
                table,
                insert_sql,
                select_sql,
                update_sql,
                list_sql,
                write_counter: 0,
            };
            backend.init_schema()?;

            Ok(Some(backend))
        }

        fn init_schema(&mut self) -> Result<()> {
            self.runtime.block_on(self.client.batch_execute(&format!(
                "CREATE TABLE IF NOT EXISTS {table} (
                    id BIGSERIAL PRIMARY KEY,
                    aggregate_type TEXT NOT NULL,
                    aggregate_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload JSONB NOT NULL
                );",
                table = self.table
            )))?;
            self.runtime.block_on(
                self.client
                    .batch_execute(&format!("TRUNCATE TABLE {};", self.table)),
            )?;
            Ok(())
        }

        pub fn apply_new(&mut self, payload: &Value) -> Result<PostgresKey> {
            let aggregate_id = super::next_aggregate_id(&mut self.write_counter);
            self.apply_with(&aggregate_id, EVENT_APPLY_TYPE, payload)
        }

        fn apply_with(
            &mut self,
            aggregate_id: &str,
            event_type: &str,
            payload: &Value,
        ) -> Result<PostgresKey> {
            let json_payload = Json(payload.clone());
            let row = self.runtime.block_on(self.client.query_one(
                &self.insert_sql,
                &[&AGGREGATE_TYPE, &aggregate_id, &event_type, &json_payload],
            ))?;
            Ok(PostgresKey {
                id: row.get(0),
                aggregate_id: aggregate_id.to_string(),
            })
        }

        pub fn seed(&mut self, payload: &Value, count: usize) -> Result<Vec<PostgresKey>> {
            let mut keys = Vec::with_capacity(count);
            for _ in 0..count {
                keys.push(self.apply_new(payload)?);
            }
            Ok(keys)
        }

        pub fn get(&mut self, key: &PostgresKey) -> Result<Value> {
            let row = self
                .runtime
                .block_on(self.client.query_opt(&self.select_sql, &[&key.id]))?
                .ok_or_else(|| anyhow!("postgres row {} missing", key.id))?;
            let payload: Json<Value> = row.get(0);
            Ok(payload.0)
        }

        pub fn patch(
            &mut self,
            key: &PostgresKey,
            _patch_value: &Value,
            patch_ops: &Patch,
        ) -> Result<()> {
            let row = self
                .runtime
                .block_on(self.client.query_opt(&self.select_sql, &[&key.id]))?
                .ok_or_else(|| anyhow!("postgres row {} missing", key.id))?;
            let payload: Json<Value> = row.get(0);
            let mut document = payload.0;
            json_patch::patch(&mut document, patch_ops)
                .context("apply patch to postgres payload")?;
            let updated = Json(document);
            self.runtime
                .block_on(self.client.execute(&self.update_sql, &[&updated, &key.id]))?;
            Ok(())
        }

        pub fn list(&mut self, aggregate_id: &str, limit: usize) -> Result<Vec<Value>> {
            let rows = self.runtime.block_on(
                self.client
                    .query(&self.list_sql, &[&aggregate_id, &(limit as i64)]),
            )?;
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                let payload: Json<Value> = row.get(0);
                values.push(payload.0);
            }
            Ok(values)
        }

        pub fn seed_list_dataset(
            &mut self,
            aggregate_id: &str,
            payload: &Value,
            count: usize,
        ) -> Result<()> {
            for _ in 0..count {
                let _ = self.apply_with(aggregate_id, EVENT_APPLY_TYPE, payload)?;
            }
            Ok(())
        }
    }
}

#[cfg(feature = "bench-mongodb")]
mod mongodb_backend {
    use super::*;
    use futures_util::TryStreamExt;
    use mongodb::{
        Client, Collection,
        bson::{Bson, Document, doc, to_bson},
        options::{ClientOptions, FindOptions},
    };
    use tokio::runtime::Runtime;

    #[derive(Clone)]
    pub struct MongoKey {
        pub id: Bson,
        pub aggregate_id: String,
    }

    pub struct MongoBackend {
        runtime: Runtime,
        collection: Collection<Document>,
        write_counter: u64,
    }

    impl MongoBackend {
        pub fn from_env() -> Result<Option<Self>> {
            let uri = match std::env::var("EVENTDBX_MONGO_URI") {
                Ok(uri) => uri,
                Err(_) => return Ok(None),
            };
            let database =
                std::env::var("EVENTDBX_MONGO_DB").unwrap_or_else(|_| "eventdbx_bench".into());
            let collection =
                std::env::var("EVENTDBX_MONGO_COLLECTION").unwrap_or_else(|_| "events".into());

            let runtime = Runtime::new().context("create tokio runtime for mongodb")?;
            let options = runtime
                .block_on(ClientOptions::parse(uri))
                .context("parse mongodb uri")?;
            let client = Client::with_options(options).context("create mongodb client")?;
            let collection = client
                .database(&database)
                .collection::<Document>(&collection);

            runtime
                .block_on(collection.delete_many(doc! {}, None))
                .context("clean mongodb collection")?;

            Ok(Some(Self {
                runtime,
                collection,
                write_counter: 0,
            }))
        }

        pub fn apply_new(&mut self, payload: &Value) -> Result<MongoKey> {
            let aggregate_id = super::next_aggregate_id(&mut self.write_counter);
            self.apply_with(&aggregate_id, EVENT_APPLY_TYPE, payload)
        }

        fn apply_with(
            &mut self,
            aggregate_id: &str,
            event_type: &str,
            payload: &Value,
        ) -> Result<MongoKey> {
            let payload_bson = to_bson(payload).context("payload to bson")?;
            let document = doc! {
                "aggregate_type": AGGREGATE_TYPE,
                "aggregate_id": aggregate_id,
                "event_type": event_type,
                "payload": payload_bson,
            };
            let result = self
                .runtime
                .block_on(self.collection.insert_one(document, None))?;
            let id = result
                .inserted_id
                .ok_or_else(|| anyhow!("mongodb missing inserted id"))?;
            Ok(MongoKey {
                id,
                aggregate_id: aggregate_id.to_string(),
            })
        }

        pub fn seed(&mut self, payload: &Value, count: usize) -> Result<Vec<MongoKey>> {
            let mut keys = Vec::with_capacity(count);
            for _ in 0..count {
                keys.push(self.apply_new(payload)?);
            }
            Ok(keys)
        }

        pub fn get(&mut self, key: &MongoKey) -> Result<Value> {
            let filter = doc! { "_id": key.id.clone() };
            let document = self
                .runtime
                .block_on(self.collection.find_one(filter, None))?
                .ok_or_else(|| anyhow!("mongodb document not found"))?;
            let payload = document
                .get("payload")
                .ok_or_else(|| anyhow!("mongodb payload missing"))?;
            mongodb::bson::from_bson::<Value>(payload.clone()).context("decode mongodb payload")
        }

        pub fn patch(
            &mut self,
            key: &MongoKey,
            _patch_value: &Value,
            patch_ops: &Patch,
        ) -> Result<()> {
            let filter = doc! { "_id": key.id.clone() };
            let document = self
                .runtime
                .block_on(self.collection.find_one(filter.clone(), None))?
                .ok_or_else(|| anyhow!("mongodb document not found"))?;
            let payload = document
                .get("payload")
                .ok_or_else(|| anyhow!("mongodb payload missing"))?;
            let mut value = mongodb::bson::from_bson::<Value>(payload.clone())
                .context("decode mongodb payload")?;
            json_patch::patch(&mut value, patch_ops).context("apply patch to mongodb payload")?;
            let updated = to_bson(&value).context("encode patched payload to bson")?;
            self.runtime.block_on(self.collection.update_one(
                filter,
                doc! { "$set": { "payload": updated } },
                None,
            ))?;
            Ok(())
        }

        pub fn list(&mut self, aggregate_id: &str, limit: usize) -> Result<Vec<Value>> {
            let options = FindOptions::builder()
                .limit(limit as i64)
                .sort(doc! { "_id": -1 })
                .build();
            let mut cursor = self.runtime.block_on(
                self.collection
                    .find(doc! { "aggregate_id": aggregate_id }, options),
            )?;
            let mut values = Vec::new();
            while let Some(doc) = self
                .runtime
                .block_on(cursor.try_next())
                .context("iterate mongodb cursor")?
            {
                let payload = doc
                    .get("payload")
                    .ok_or_else(|| anyhow!("mongodb payload missing"))?;
                values.push(
                    mongodb::bson::from_bson::<Value>(payload.clone())
                        .context("decode mongodb payload")?,
                );
            }
            Ok(values)
        }

        pub fn seed_list_dataset(
            &mut self,
            aggregate_id: &str,
            payload: &Value,
            count: usize,
        ) -> Result<()> {
            for _ in 0..count {
                let _ = self.apply_with(aggregate_id, EVENT_APPLY_TYPE, payload)?;
            }
            Ok(())
        }
    }
}

#[cfg(feature = "bench-sqlite")]
mod sqlite {
    use super::*;
    use rusqlite::{Connection, params};
    use tempfile::TempDir;

    #[derive(Clone)]
    pub struct SqliteKey {
        pub id: i64,
        pub aggregate_id: String,
    }

    pub struct SqliteBackend {
        conn: Connection,
        _temp_dir: Option<TempDir>,
        write_counter: u64,
    }

    impl SqliteBackend {
        pub fn from_env() -> Result<Option<Self>> {
            let (conn, temp_dir) = match std::env::var("EVENTDBX_SQLITE_PATH") {
                Ok(path) => (
                    Connection::open(path).context("open sqlite database from path")?,
                    None,
                ),
                Err(_) => {
                    let temp_dir = TempDir::new().context("create temp dir for sqlite")?;
                    let path = temp_dir.path().join("bench.db");
                    (
                        Connection::open(&path).context("open sqlite temp database")?,
                        Some(temp_dir),
                    )
                }
            };

            let mut backend = Self {
                conn,
                _temp_dir: temp_dir,
                write_counter: 0,
            };
            backend.init_schema()?;
            Ok(Some(backend))
        }

        fn init_schema(&mut self) -> Result<()> {
            self.conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    aggregate_type TEXT NOT NULL,
                    aggregate_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                DELETE FROM events;",
            )?;
            Ok(())
        }

        pub fn apply_new(&mut self, payload: &Value) -> Result<SqliteKey> {
            let aggregate_id = super::next_aggregate_id(&mut self.write_counter);
            self.apply_with(&aggregate_id, EVENT_APPLY_TYPE, payload)
        }

        fn apply_with(
            &mut self,
            aggregate_id: &str,
            event_type: &str,
            payload: &Value,
        ) -> Result<SqliteKey> {
            self.conn.execute(
                "INSERT INTO events (aggregate_type, aggregate_id, event_type, payload) \
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    AGGREGATE_TYPE,
                    aggregate_id,
                    event_type,
                    payload.to_string(),
                ],
            )?;
            Ok(SqliteKey {
                id: self.conn.last_insert_rowid(),
                aggregate_id: aggregate_id.to_string(),
            })
        }

        pub fn seed(&mut self, payload: &Value, count: usize) -> Result<Vec<SqliteKey>> {
            let mut keys = Vec::with_capacity(count);
            for _ in 0..count {
                keys.push(self.apply_new(payload)?);
            }
            Ok(keys)
        }

        pub fn get(&self, key: &SqliteKey) -> Result<Value> {
            let payload: String = self.conn.query_row(
                "SELECT payload FROM events WHERE id = ?1",
                params![key.id],
                |row| row.get(0),
            )?;
            serde_json::from_str(&payload).context("parse sqlite payload")
        }

        pub fn patch(
            &mut self,
            key: &SqliteKey,
            _patch_value: &Value,
            patch_ops: &Patch,
        ) -> Result<()> {
            let payload: String = self.conn.query_row(
                "SELECT payload FROM events WHERE id = ?1",
                params![key.id],
                |row| row.get(0),
            )?;
            let mut value: Value =
                serde_json::from_str(&payload).context("parse sqlite payload")?;
            json_patch::patch(&mut value, patch_ops).context("apply patch to sqlite payload")?;
            self.conn.execute(
                "UPDATE events SET payload = ?1 WHERE id = ?2",
                params![value.to_string(), key.id],
            )?;
            Ok(())
        }

        pub fn list(&self, aggregate_id: &str, limit: usize) -> Result<Vec<Value>> {
            let mut stmt = self.conn.prepare(
                "SELECT payload FROM events WHERE aggregate_id = ?1 \
                 ORDER BY id DESC LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![aggregate_id, limit as i64], |row| {
                let payload: String = row.get(0)?;
                let value: Value = serde_json::from_str(&payload).map_err(|err| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(err),
                    )
                })?;
                Ok(value)
            })?;
            let mut values = Vec::new();
            for row in rows {
                values.push(row?);
            }
            Ok(values)
        }

        pub fn seed_list_dataset(
            &mut self,
            aggregate_id: &str,
            payload: &Value,
            count: usize,
        ) -> Result<()> {
            for _ in 0..count {
                let _ = self.apply_with(aggregate_id, EVENT_APPLY_TYPE, payload)?;
            }
            Ok(())
        }
    }
}

#[cfg(feature = "bench-mssql")]
mod mssql {
    use super::*;
    use futures_util::TryStreamExt;
    use tiberius::{Client, Config, Query};
    use tokio::{net::TcpStream, runtime::Runtime, sync::Mutex};
    use tokio_util::compat::TokioAsyncWriteCompatExt;

    #[derive(Clone)]
    pub struct MssqlKey {
        pub id: i64,
        pub aggregate_id: String,
    }

    pub struct MssqlBackend {
        runtime: Runtime,
        client: Arc<Mutex<Client<tokio_util::compat::Compat<TcpStream>>>>,
        table: String,
        write_counter: u64,
    }

    impl MssqlBackend {
        pub fn from_env() -> Result<Option<Self>> {
            let dsn = match std::env::var("EVENTDBX_MSSQL_DSN") {
                Ok(value) => value,
                Err(_) => return Ok(None),
            };
            let table = std::env::var("EVENTDBX_MSSQL_TABLE")
                .unwrap_or_else(|_| "eventdbx_bench_events".into());

            let runtime = Runtime::new().context("create tokio runtime for mssql")?;
            let client = runtime
                .block_on(Self::connect(&dsn))
                .context("connect to mssql")?;
            let client = Arc::new(Mutex::new(client));

            let mut backend = Self {
                runtime,
                client,
                table,
                write_counter: 0,
            };
            backend.init_schema()?;

            Ok(Some(backend))
        }

        async fn connect(dsn: &str) -> Result<Client<tokio_util::compat::Compat<TcpStream>>> {
            let mut config = Config::from_ado_string(dsn).context("parse mssql dsn")?;
            config.trust_cert();
            let addr = config.get_addr();
            let tcp = TcpStream::connect(addr).await.context("connect tcp")?;
            tcp.set_nodelay(true).context("set nodelay")?;
            Client::connect(config, tcp.compat_write())
                .await
                .context("connect client")
        }

        fn init_schema(&mut self) -> Result<()> {
            let table = self.table.clone();
            self.runtime.block_on(async {
                let mut client = self.client.lock().await;
                client
                    .simple_query(&format!(
                        "IF OBJECT_ID('{table}', 'U') IS NULL BEGIN
                            CREATE TABLE {table} (
                                id BIGINT IDENTITY(1,1) PRIMARY KEY,
                                aggregate_type NVARCHAR(255) NOT NULL,
                                aggregate_id NVARCHAR(255) NOT NULL,
                                event_type NVARCHAR(255) NOT NULL,
                                payload NVARCHAR(MAX) NOT NULL
                            );
                        END;",
                        table = table
                    ))
                    .await
                    .context("create table")?;
                client
                    .simple_query(&format!("TRUNCATE TABLE {table};", table = table))
                    .await
                    .context("truncate table")?;
                Ok::<_, anyhow::Error>(())
            })?;
            Ok(())
        }

        pub fn apply_new(&mut self, payload: &Value) -> Result<MssqlKey> {
            let aggregate_id = super::next_aggregate_id(&mut self.write_counter);
            self.apply_with(&aggregate_id, EVENT_APPLY_TYPE, payload)
        }

        fn apply_with(
            &mut self,
            aggregate_id: &str,
            event_type: &str,
            payload: &Value,
        ) -> Result<MssqlKey> {
            let payload_string = payload.to_string();
            let table = self.table.clone();
            self.runtime.block_on(async {
                let mut client = self.client.lock().await;
                let mut stream = client
                    .query(
                        &format!(
                            "INSERT INTO {table} (aggregate_type, aggregate_id, event_type, payload)
                             OUTPUT INSERTED.id
                             VALUES (@P1, @P2, @P3, @P4);",
                            table = table
                        ),
                        &[&AGGREGATE_TYPE, &aggregate_id, &event_type, &payload_string],
                    )
                    .await
                    .context("mssql insert")?;

                let row = stream.try_next().await.context("fetch inserted id")?;
                let row = row.ok_or_else(|| anyhow!("mssql missing output row"))?;
                let id: i64 = row.try_get(0).context("read inserted id")?;
                Ok(MssqlKey {
                    id,
                    aggregate_id: aggregate_id.to_string(),
                })
            })
        }

        pub fn seed(&mut self, payload: &Value, count: usize) -> Result<Vec<MssqlKey>> {
            let mut keys = Vec::with_capacity(count);
            for _ in 0..count {
                keys.push(self.apply_new(payload)?);
            }
            Ok(keys)
        }

        pub fn get(&mut self, key: &MssqlKey) -> Result<Value> {
            let table = self.table.clone();
            self.runtime.block_on(async {
                let mut client = self.client.lock().await;
                let mut stream = client
                    .query(
                        &format!("SELECT payload FROM {table} WHERE id = @P1;", table = table),
                        &[&key.id],
                    )
                    .await
                    .context("mssql select")?;

                let row = stream.try_next().await.context("fetch row")?;
                let row = row.ok_or_else(|| anyhow!("mssql row {} missing", key.id))?;
                let payload: &str = row.try_get(0).context("get payload")?;
                serde_json::from_str(payload).context("decode mssql payload")
            })
        }

        pub fn patch(
            &mut self,
            key: &MssqlKey,
            _patch_value: &Value,
            patch_ops: &Patch,
        ) -> Result<()> {
            let table = self.table.clone();
            self.runtime.block_on(async {
                let mut client = self.client.lock().await;
                let mut stream = client
                    .query(
                        &format!("SELECT payload FROM {table} WHERE id = @P1;", table = table),
                        &[&key.id],
                    )
                    .await
                    .context("mssql select for patch")?;
                let row = stream.try_next().await.context("fetch row")?;
                let row = row.ok_or_else(|| anyhow!("mssql row {} missing", key.id))?;
                let payload: &str = row.try_get(0).context("get payload")?;
                let mut value: Value =
                    serde_json::from_str(payload).context("decode mssql payload")?;
                json_patch::patch(&mut value, patch_ops).context("apply patch to mssql payload")?;
                let updated = value.to_string();
                client
                    .execute(
                        &format!(
                            "UPDATE {table} SET payload = @P1 WHERE id = @P2;",
                            table = table
                        ),
                        &[&updated, &key.id],
                    )
                    .await
                    .context("update mssql payload")?;
                Ok(())
            })
        }

        pub fn list(&mut self, aggregate_id: &str, limit: usize) -> Result<Vec<Value>> {
            let table = self.table.clone();
            self.runtime.block_on(async {
                let mut client = self.client.lock().await;
                let mut stream = client
                    .query(
                        &format!(
                            "SELECT payload FROM {table} WHERE aggregate_id = @P1 \
                             ORDER BY id DESC OFFSET 0 ROWS FETCH NEXT @P2 ROWS ONLY;",
                            table = table
                        ),
                        &[&aggregate_id, &(limit as i32)],
                    )
                    .await
                    .context("mssql list query")?;

                let mut values = Vec::new();
                while let Some(row) = stream.try_next().await.context("fetch row")? {
                    let payload: &str = row.try_get(0).context("get payload")?;
                    values.push(serde_json::from_str(payload).context("decode mssql payload")?);
                }
                Ok(values)
            })
        }

        pub fn seed_list_dataset(
            &mut self,
            aggregate_id: &str,
            payload: &Value,
            count: usize,
        ) -> Result<()> {
            for _ in 0..count {
                let _ = self.apply_with(aggregate_id, EVENT_APPLY_TYPE, payload)?;
            }
            Ok(())
        }
    }
}
