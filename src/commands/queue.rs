use std::{io::Write, path::PathBuf};

use anyhow::{Context, Result};
use chrono::{Duration, TimeZone, Utc};
use clap::{Args, Subcommand};

use eventdbx::{
    config::{Config, load_or_default},
    plugin::{
        PluginManager,
        queue::{JobRecord, PluginQueueStore},
    },
    snowflake::SnowflakeId,
};

use crate::commands::plugin::{dedupe_plugins_by_name, normalize_plugin_names};

#[derive(Args)]
pub struct QueueArgs {
    #[command(subcommand)]
    pub action: Option<QueueAction>,
}

#[derive(Subcommand, Clone, Copy)]
pub enum QueueAction {
    /// Remove all dead entries from the queue
    #[command(name = "clear")]
    Clear,
    /// Remove completed entries from the queue
    #[command(name = "clear-done")]
    ClearDone(QueueClearDoneArgs),
    /// Retry dead entries, optionally filtering by job id
    #[command(name = "retry")]
    Retry(QueueRetryArgs),
}

#[derive(Args, Clone, Copy)]
pub struct QueueRetryArgs {
    /// Retry only the dead entry with this job id
    #[arg(long)]
    pub event_id: Option<SnowflakeId>,
}

#[derive(Args, Clone, Copy)]
pub struct QueueClearDoneArgs {
    /// Only clear done jobs created more than this many hours ago
    #[arg(long, value_name = "HOURS")]
    pub older_than_hours: Option<u64>,
}

pub fn execute(config_path: Option<PathBuf>, args: QueueArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;

    let mut plugins = config.load_plugins()?;
    let mut plugins_dirty = normalize_plugin_names(&mut plugins);
    if dedupe_plugins_by_name(&mut plugins) {
        plugins_dirty = true;
    }

    if plugins_dirty {
        config.ensure_data_dir()?;
        config.save_plugins(&plugins)?;
    }

    config.migrate_plugin_queue_to_root()?;
    config.migrate_plugin_runtime_to_root()?;

    let queue_store = PluginQueueStore::open_with_legacy(
        config.plugin_queue_db_path().as_path(),
        config.plugin_queue_path().as_path(),
    )
    .map_err(anyhow::Error::from)?;

    match args.action {
        None => {
            print_plugin_queue_status(&queue_store)?;
        }
        Some(QueueAction::Clear) => {
            let status = queue_store.status().map_err(anyhow::Error::from)?;
            let dead_count = status.dead.len();

            if dead_count == 0 {
                println!("no dead plugin jobs to clear");
                return Ok(());
            }

            if !confirm_clear("dead", dead_count)? {
                println!("clear cancelled");
                return Ok(());
            }

            let cleared = queue_store.clear_dead().map_err(anyhow::Error::from)?;
            println!("cleared {} dead job(s) from the plugin queue", cleared);
        }
        Some(QueueAction::ClearDone(opts)) => {
            let status = queue_store.status().map_err(anyhow::Error::from)?;
            let cutoff_millis = opts.older_than_hours.map(|hours| {
                let bounded = std::cmp::min(hours, i64::MAX as u64) as i64;
                Utc::now()
                    .checked_sub_signed(Duration::hours(bounded))
                    .unwrap_or_else(|| Utc.timestamp_millis_opt(0).single().unwrap())
                    .timestamp_millis()
            });

            let done: Vec<_> = status
                .done
                .into_iter()
                .filter(|job| match cutoff_millis {
                    Some(cutoff) => job.created_at < cutoff,
                    None => true,
                })
                .collect();

            if done.is_empty() {
                println!("no done plugin jobs match the provided criteria");
                return Ok(());
            }

            if !confirm_clear("done", done.len())? {
                println!("clear cancelled");
                return Ok(());
            }

            let cleared = queue_store
                .clear_done(cutoff_millis)
                .map_err(anyhow::Error::from)?;
            println!("cleared {} done job(s) from the plugin queue", cleared);
        }
        Some(QueueAction::Retry(retry_args)) => {
            retry_dead_jobs(&config, &queue_store, retry_args.event_id)?;
        }
    }
    Ok(())
}

fn print_plugin_queue_status(store: &PluginQueueStore) -> Result<()> {
    let status = store.status().map_err(anyhow::Error::from)?;

    println!(
        "pending={} processing={} done={} dead={}",
        status.pending.len(),
        status.processing.len(),
        status.done.len(),
        status.dead.len()
    );

    if !status.pending.is_empty() {
        println!("\nPending jobs:");
        for job in &status.pending {
            print_job(job);
        }
    }

    if !status.processing.is_empty() {
        println!("\nProcessing jobs:");
        for job in &status.processing {
            print_job(job);
        }
    }

    if !status.dead.is_empty() {
        println!("\nDead jobs:");
        for job in &status.dead {
            print_job(job);
        }
    }

    Ok(())
}

fn print_job(job: &JobRecord) {
    let retry_at = job
        .next_retry_at
        .map(|ts| format_timestamp(ts))
        .unwrap_or_else(|| "-".into());
    let last_error = job.last_error.as_deref().unwrap_or("-");
    println!(
        "  id={} plugin=\"{}\" status={:?} attempts={} next_retry={} last_error={}",
        job.id,
        job.plugin,
        job.status.as_str(),
        job.attempts,
        retry_at,
        last_error
    );
}

fn retry_dead_jobs(
    config: &Config,
    store: &PluginQueueStore,
    filter_job: Option<SnowflakeId>,
) -> Result<()> {
    let status = store.status().map_err(anyhow::Error::from)?;
    let targets: Vec<JobRecord> = status
        .dead
        .into_iter()
        .filter(|job| match filter_job {
            Some(id) => job.id == id.as_u64(),
            None => true,
        })
        .collect();

    if targets.is_empty() {
        if filter_job.is_some() {
            println!("no dead plugin jobs match the provided id");
        } else {
            println!("no dead plugin jobs to retry");
        }
        return Ok(());
    }

    let now = Utc::now().timestamp_millis();
    for job in &targets {
        store
            .retry_dead_job(job.id, now)
            .map_err(anyhow::Error::from)?;
    }

    let manager = PluginManager::from_config(config)?;
    if manager.is_empty() {
        println!("no enabled plugins available to process retries");
        return Ok(());
    }

    manager.dispatch_pending().map_err(anyhow::Error::from)?;

    let refreshed = store.status().map_err(anyhow::Error::from)?;
    println!(
        "retry summary: retried {} job(s), {} still dead",
        targets.len(),
        refreshed.dead.len()
    );

    Ok(())
}

fn confirm_clear(kind: &str, count: usize) -> Result<bool> {
    print!(
        "This will remove {} {} plugin job(s). Type 'clear' to confirm: ",
        count, kind
    );
    std::io::stdout().flush()?;
    let mut input = String::new();
    std::io::stdin()
        .read_line(&mut input)
        .with_context(|| "failed to read confirmation input")?;
    Ok(input.trim().eq_ignore_ascii_case("clear"))
}

fn format_timestamp(timestamp_millis: i64) -> String {
    Utc.timestamp_millis_opt(timestamp_millis)
        .single()
        .unwrap_or_else(|| Utc.timestamp_millis_opt(0).single().unwrap())
        .to_rfc3339()
}
