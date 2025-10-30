use std::collections::{BTreeMap, HashMap};

use anyhow::{Result, bail};

use crate::store::AggregatePositionEntry;

/// Filters aggregate positions when building replication plans.
pub trait PositionFilter {
    fn includes(&self, aggregate_type: &str, aggregate_id: &str) -> bool;
}

#[derive(Debug, Clone)]
pub struct SyncPlan {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub from_version: u64,
    pub to_version: u64,
    pub event_count: u64,
}

pub fn positions_to_map(
    entries: &[AggregatePositionEntry],
    filter: Option<&dyn PositionFilter>,
) -> HashMap<(String, String), u64> {
    entries
        .iter()
        .filter(|entry| match filter {
            Some(filter) => filter.includes(&entry.aggregate_type, &entry.aggregate_id),
            None => true,
        })
        .map(|entry| {
            (
                (entry.aggregate_type.clone(), entry.aggregate_id.clone()),
                entry.version,
            )
        })
        .collect()
}

pub fn validate_fast_forward(
    remote_name: &str,
    local: &HashMap<(String, String), u64>,
    remote: &HashMap<(String, String), u64>,
) -> Result<()> {
    for (key, remote_version) in remote {
        if *remote_version == 0 {
            continue;
        }
        match local.get(key) {
            Some(local_version) => {
                if remote_version > local_version {
                    bail!(
                        "remote '{}' is ahead for aggregate {}::{} (remote version {}, local version {})",
                        remote_name,
                        key.0,
                        key.1,
                        remote_version,
                        local_version
                    );
                }
            }
            None => {
                bail!(
                    "remote '{}' contains aggregate {}::{} that does not exist locally; aborting push",
                    remote_name,
                    key.0,
                    key.1
                );
            }
        }
    }
    Ok(())
}

pub fn build_push_plans(
    local: &HashMap<(String, String), u64>,
    remote: &HashMap<(String, String), u64>,
) -> Vec<SyncPlan> {
    let mut plans = Vec::new();
    for (key, local_version) in local {
        let remote_version = remote.get(key).copied().unwrap_or(0);
        if remote_version > *local_version {
            continue;
        }
        if local_version > &remote_version {
            plans.push(SyncPlan {
                aggregate_type: key.0.clone(),
                aggregate_id: key.1.clone(),
                from_version: remote_version,
                to_version: *local_version,
                event_count: local_version - remote_version,
            });
        }
    }
    plans
}

pub fn build_pull_plans(
    local: &HashMap<(String, String), u64>,
    remote: &HashMap<(String, String), u64>,
) -> Vec<SyncPlan> {
    let mut plans = Vec::new();
    for (key, remote_version) in remote {
        let local_version = local.get(key).copied().unwrap_or(0);
        if remote_version > &local_version {
            plans.push(SyncPlan {
                aggregate_type: key.0.clone(),
                aggregate_id: key.1.clone(),
                from_version: local_version,
                to_version: *remote_version,
                event_count: remote_version - local_version,
            });
        }
    }
    plans
}

pub fn summarize_plans(plans: &[SyncPlan]) -> BTreeMap<String, u64> {
    let mut map = BTreeMap::new();
    for plan in plans {
        *map.entry(plan.aggregate_type.clone()).or_default() += plan.event_count;
    }
    map
}
