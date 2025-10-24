use std::{
    fmt,
    str::FromStr,
    thread::sleep,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Custom epoch (2025-01-01T00:00:00Z) expressed in milliseconds.
const EPOCH_MILLIS: u64 = 1_735_689_600_000;
const WORKER_ID_BITS: u8 = 10;
const SEQUENCE_BITS: u8 = 12;
const MAX_SEQUENCE: u16 = (1 << SEQUENCE_BITS) - 1;

pub const MAX_WORKER_ID: u16 = (1 << WORKER_ID_BITS) - 1;

#[derive(Debug)]
pub struct SnowflakeGenerator {
    worker_id: u16,
    last_timestamp: u64,
    sequence: u16,
}

impl SnowflakeGenerator {
    pub fn new(worker_id: u16) -> Self {
        Self {
            worker_id,
            last_timestamp: 0,
            sequence: 0,
        }
    }

    pub fn next_id(&mut self) -> SnowflakeId {
        loop {
            let mut timestamp = current_millis();
            if timestamp < self.last_timestamp {
                let wait = self.last_timestamp - timestamp;
                sleep(Duration::from_millis(wait));
                continue;
            }

            if timestamp == self.last_timestamp {
                self.sequence = (self.sequence + 1) & MAX_SEQUENCE;
                if self.sequence == 0 {
                    timestamp = wait_next_millis(self.last_timestamp);
                }
            } else {
                self.sequence = 0;
            }

            self.last_timestamp = timestamp;
            let elapsed = timestamp - EPOCH_MILLIS;
            let id = (elapsed << (WORKER_ID_BITS + SEQUENCE_BITS))
                | ((self.worker_id as u64) << SEQUENCE_BITS)
                | self.sequence as u64;
            return SnowflakeId(id);
        }
    }
}

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_millis() as u64
}

fn wait_next_millis(last_timestamp: u64) -> u64 {
    loop {
        let timestamp = current_millis();
        if timestamp > last_timestamp {
            return timestamp;
        }
        sleep(Duration::from_micros(100));
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SnowflakeId(u64);

impl SnowflakeId {
    pub fn as_u64(self) -> u64 {
        self.0
    }

    pub fn from_u64(value: u64) -> Self {
        Self(value)
    }
}

impl fmt::Display for SnowflakeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for SnowflakeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SnowflakeId").field(&self.0).finish()
    }
}

impl FromStr for SnowflakeId {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u64>().map(SnowflakeId)
    }
}

impl Serialize for SnowflakeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for SnowflakeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value
            .parse::<SnowflakeId>()
            .map_err(|err| serde::de::Error::custom(err.to_string()))
    }
}

impl From<SnowflakeId> for u64 {
    fn from(value: SnowflakeId) -> Self {
        value.0
    }
}
