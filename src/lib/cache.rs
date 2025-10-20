use std::num::NonZeroUsize;

use lru::LruCache;
use parking_lot::Mutex;

use crate::store::AggregateState;

const KEY_SEPARATOR: &str = "\u{1F}";

pub struct AggregateCache {
    inner: Mutex<LruCache<String, AggregateState>>,
}

impl AggregateCache {
    pub fn new(capacity: usize) -> Option<Self> {
        NonZeroUsize::new(capacity).map(|size| Self {
            inner: Mutex::new(LruCache::new(size)),
        })
    }

    pub fn get(&self, aggregate_type: &str, aggregate_id: &str) -> Option<AggregateState> {
        let key = make_key(aggregate_type, aggregate_id);
        let mut guard = self.inner.lock();
        guard.get(&key).cloned()
    }

    pub fn put(&self, aggregate: AggregateState) {
        let key = make_key(&aggregate.aggregate_type, &aggregate.aggregate_id);
        let mut guard = self.inner.lock();
        guard.put(key, aggregate);
    }

    pub fn remove(&self, aggregate_type: &str, aggregate_id: &str) {
        let key = make_key(aggregate_type, aggregate_id);
        let mut guard = self.inner.lock();
        guard.pop(&key);
    }

    pub fn clear(&self) {
        let mut guard = self.inner.lock();
        guard.clear();
    }
}

fn make_key(aggregate_type: &str, aggregate_id: &str) -> String {
    format!("{aggregate_type}{KEY_SEPARATOR}{aggregate_id}")
}
