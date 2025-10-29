use std::{collections::BTreeSet, fs, path::PathBuf};

use anyhow::{Context, Result};
use clap::Args;
use serde::Serialize;

use eventdbx::{
    config::{Config, DEFAULT_DOMAIN_NAME, load_or_default},
    encryption::Encryptor,
    store::{EventStore, StoreCounts},
};

#[derive(Args)]
pub struct ListArgs {
    /// Emit the results as JSON
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Debug, Serialize)]
struct DomainSummary {
    domain: String,
    active: bool,
    active_aggregates: usize,
    archived_aggregates: usize,
    total_aggregates: usize,
    events: u64,
}

pub fn execute(config_path: Option<PathBuf>, args: ListArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let encryptor = config.encryption_key()?;

    let mut domains = BTreeSet::new();
    domains.insert(DEFAULT_DOMAIN_NAME.to_string());
    domains.insert(config.active_domain().to_string());

    if let Ok(entries) = fs::read_dir(config.domains_root()) {
        for entry in entries.flatten() {
            if !is_directory(&entry) {
                continue;
            }
            if let Some(name) = entry.file_name().to_str() {
                if !name.trim().is_empty() {
                    domains.insert(name.to_string());
                }
            }
        }
    }

    let summaries = domains
        .into_iter()
        .map(|domain| domain_summary(&config, &domain, encryptor.clone()))
        .collect::<Result<Vec<_>>>()?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&summaries)?);
        return Ok(());
    }

    if summaries.is_empty() {
        println!("no domains found");
        return Ok(());
    }

    print_table(&summaries);
    Ok(())
}

fn domain_summary(
    config: &Config,
    domain: &str,
    encryptor: Option<Encryptor>,
) -> Result<DomainSummary> {
    let domain_root = domain_data_dir(config, domain);
    let event_store_path = domain_root.join("event_store");

    let counts = if event_store_path.exists() {
        let store =
            EventStore::open_read_only(event_store_path.clone(), encryptor).with_context(|| {
                format!(
                    "failed to open event store for domain '{}' at {}",
                    domain,
                    event_store_path.display()
                )
            })?;
        Some(
            store
                .counts()
                .with_context(|| format!("failed to compute statistics for domain '{domain}'"))?,
        )
    } else {
        None
    };

    Ok(DomainSummary {
        domain: domain.to_string(),
        active: domain.eq_ignore_ascii_case(config.active_domain()),
        active_aggregates: counts
            .as_ref()
            .map(|c| c.active_aggregates)
            .unwrap_or_default(),
        archived_aggregates: counts
            .as_ref()
            .map(|c| c.archived_aggregates)
            .unwrap_or_default(),
        total_aggregates: counts
            .as_ref()
            .map(StoreCounts::total_aggregates)
            .unwrap_or_default(),
        events: counts
            .as_ref()
            .map(StoreCounts::total_events)
            .unwrap_or_default(),
    })
}

fn print_table(summaries: &[DomainSummary]) {
    let mut domain_width = "domain".len();
    let mut active_width = "active".len();
    let mut archived_width = "archived".len();
    let mut total_width = "total".len();
    let mut events_width = "events".len();

    for summary in summaries {
        let label_len = table_domain_label(summary).len();
        domain_width = domain_width.max(label_len);
        active_width = active_width.max(summary.active_aggregates.to_string().len());
        archived_width = archived_width.max(summary.archived_aggregates.to_string().len());
        total_width = total_width.max(summary.total_aggregates.to_string().len());
        events_width = events_width.max(summary.events.to_string().len());
    }

    println!(
        "{:domain_width$}  {:>active_width$}  {:>archived_width$}  {:>total_width$}  {:>events_width$}",
        "domain",
        "active",
        "archived",
        "total",
        "events",
        domain_width = domain_width,
        active_width = active_width,
        archived_width = archived_width,
        total_width = total_width,
        events_width = events_width,
    );
    for summary in summaries {
        println!(
            "{:domain_width$}  {:>active_width$}  {:>archived_width$}  {:>total_width$}  {:>events_width$}",
            table_domain_label(summary),
            summary.active_aggregates,
            summary.archived_aggregates,
            summary.total_aggregates,
            summary.events,
            domain_width = domain_width,
            active_width = active_width,
            archived_width = archived_width,
            total_width = total_width,
            events_width = events_width,
        );
    }
}

fn table_domain_label(summary: &DomainSummary) -> String {
    if summary.active {
        format!("{} *", summary.domain)
    } else {
        summary.domain.clone()
    }
}

fn domain_data_dir(config: &Config, domain: &str) -> PathBuf {
    if domain.eq_ignore_ascii_case(DEFAULT_DOMAIN_NAME) {
        config.data_dir.clone()
    } else {
        config.domains_root().join(domain)
    }
}

fn is_directory(entry: &fs::DirEntry) -> bool {
    entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false)
}
