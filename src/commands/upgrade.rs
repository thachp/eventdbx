use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration, Utc};
use clap::{Args, Subcommand};
use reqwest::{Client, StatusCode, header::ACCEPT};
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json;
use tracing::{debug, warn};

use eventdbx::config::default_config_path;

const USER_AGENT: &str = concat!("eventdbx-cli/", env!("CARGO_PKG_VERSION"));
const REPO_BASE: &str = "https://github.com/thachp/eventdbx";
const INSTALLER_SH: &str = "eventdbx-installer.sh";
const INSTALLER_PS1: &str = "eventdbx-installer.ps1";
const RELEASES_API: &str = "https://api.github.com/repos/thachp/eventdbx/releases";
const MIN_UPGRADE_VERSION: &str = "1.13.2";
const UPDATE_CHECK_INTERVAL_HOURS: i64 = 6;

#[derive(Debug, Default, Serialize, Deserialize)]
struct UpgradeNoticeState {
    #[serde(default)]
    ignored_version: Option<String>,
    #[serde(default)]
    latest_known_version: Option<String>,
    #[serde(default, with = "chrono::serde::ts_seconds_option")]
    last_checked: Option<DateTime<Utc>>,
}

#[derive(Args, Debug)]
pub struct UpgradeArgs {
    /// Upgrade subcommand
    #[command(subcommand)]
    pub command: Option<UpgradeCommand>,
    /// Release tag (e.g. v1.12.4) or 'latest'
    #[arg(value_name = "VERSION")]
    pub version: Option<String>,
    /// Print the installer command without executing it
    #[arg(long, default_value_t = false)]
    pub print_only: bool,
    /// Suppress upgrade notices for the provided release tag
    #[arg(
        long = "suppress",
        value_name = "VERSION",
        conflicts_with_all = ["version", "print_only", "clear_suppress"],
        help = "Suppress upgrade reminders for the specified release (e.g. v1.15.3)"
    )]
    pub suppress_version: Option<String>,
    /// Clear any suppressed release tag so future notices appear
    #[arg(
        long = "clear-suppress",
        conflicts_with_all = ["version", "print_only", "suppress_version"],
        default_value_t = false
    )]
    pub clear_suppress: bool,
}

#[derive(Subcommand, Debug)]
pub enum UpgradeCommand {
    /// List available EventDBX releases
    List(UpgradeListArgs),
}

#[derive(Args, Debug, Clone)]
pub struct UpgradeListArgs {
    /// Maximum number of releases to display
    #[arg(long, short = 'n', default_value_t = 20)]
    pub limit: usize,
    /// Emit the release list as JSON
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

fn upgrade_state_path() -> Result<PathBuf> {
    let config_path = default_config_path()?;
    let dir = config_path
        .parent()
        .context("configuration path has no parent directory")?
        .to_path_buf();
    Ok(dir.join("upgrade-state.json"))
}

fn load_upgrade_state(path: &Path) -> Result<UpgradeNoticeState> {
    if !path.exists() {
        return Ok(UpgradeNoticeState::default());
    }

    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read upgrade state from {}", path.display()))?;
    match serde_json::from_str(&contents) {
        Ok(state) => Ok(state),
        Err(err) => {
            warn!(
                "failed to parse upgrade notice state in {}: {err}",
                path.display()
            );
            Ok(UpgradeNoticeState::default())
        }
    }
}

fn save_upgrade_state(path: &Path, state: &UpgradeNoticeState) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let contents = serde_json::to_string_pretty(state)?;
    fs::write(path, contents)
        .with_context(|| format!("failed to write upgrade state to {}", path.display()))?;
    Ok(())
}

fn is_check_stale(last_checked: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
    match last_checked {
        None => true,
        Some(ts) if ts > now => true,
        Some(ts) => now - ts >= Duration::hours(UPDATE_CHECK_INTERVAL_HOURS),
    }
}

fn parse_release_tag(tag: &str) -> Result<Version> {
    let normalized = tag.trim().trim_start_matches('v');
    Version::parse(normalized).with_context(|| format!("invalid release tag '{}'", tag))
}

fn normalize_release_spec(spec: &str) -> Result<String> {
    let trimmed = spec.trim();
    if trimmed.is_empty() {
        bail!("release spec cannot be empty");
    }

    if trimmed.eq_ignore_ascii_case("latest") {
        Ok("latest".to_string())
    } else {
        let tag = if trimmed.starts_with('v') {
            trimmed.to_string()
        } else {
            format!("v{}", trimmed)
        };
        validate_supported_version(&tag)?;
        Ok(tag)
    }
}

fn update_suppressed_version(value: Option<String>) -> Result<()> {
    let path = upgrade_state_path()?;
    let mut state = load_upgrade_state(&path)?;
    state.ignored_version = value;
    save_upgrade_state(&path, &state)
}

pub async fn execute(args: UpgradeArgs) -> Result<()> {
    if args.command.is_some() && (args.suppress_version.is_some() || args.clear_suppress) {
        bail!("--suppress and --clear-suppress cannot be used with subcommands");
    }

    if args.clear_suppress {
        update_suppressed_version(None)?;
        println!("Upgrade reminders have been re-enabled.");
        return Ok(());
    }

    let client = build_client()?;

    if let Some(spec) = args.suppress_version.as_deref() {
        let normalized = normalize_release_spec(spec)?;
        let tag = if normalized.eq_ignore_ascii_case("latest") {
            resolve_release(&client, "latest").await?
        } else {
            normalized
        };
        update_suppressed_version(Some(tag.clone()))?;
        println!("Suppressing upgrade reminders for {tag}");
        return Ok(());
    }

    if let Some(subcommand) = &args.command {
        match subcommand {
            UpgradeCommand::List(list_args) => {
                list_releases(&client, list_args).await?;
                return Ok(());
            }
        }
    }

    let version = args.version.as_deref().unwrap_or("latest");
    let tag = resolve_release(&client, version).await?;
    let path_segment = format!("download/{}", tag);
    let display_version = tag.clone();

    let os = std::env::consts::OS;
    match os {
        "macos" | "linux" => {
            let url = format!("{}/releases/{}/{}", REPO_BASE, path_segment, INSTALLER_SH);
            let command = format!("curl --proto '=https' --tlsv1.2 -LsSf {} | sh", url);
            println!("Installing EventDBX CLI ({display_version}) via: {command}");
            if args.print_only {
                return Ok(());
            }
            let status = Command::new("sh")
                .arg("-c")
                .arg(&command)
                .status()
                .with_context(|| "failed to invoke shell for installer")?;
            if !status.success() {
                bail!("upgrade command exited with status {}", status);
            }
        }
        "windows" => {
            let url = format!("{}/releases/{}/{}", REPO_BASE, path_segment, INSTALLER_PS1);
            let ps_command = format!("irm {} | iex", url);
            println!(
                "Installing EventDBX CLI ({display_version}) via PowerShell: {}",
                ps_command
            );
            if args.print_only {
                return Ok(());
            }
            let status = Command::new("powershell")
                .args([
                    "-ExecutionPolicy",
                    "Bypass",
                    "-NoLogo",
                    "-NoProfile",
                    "-Command",
                    &ps_command,
                ])
                .status()
                .with_context(|| "failed to invoke PowerShell for installer")?;
            if !status.success() {
                bail!("upgrade command exited with status {}", status);
            }
        }
        other => bail!("upgrades are not supported on this platform ({other})"),
    }

    Ok(())
}

pub async fn try_handle_shortcut(raw: &[String]) -> Result<bool> {
    if raw.is_empty() {
        return Ok(false);
    }

    let command = &raw[0];
    if let Some(version) = command.strip_prefix("upgrade@") {
        if raw.len() > 1 {
            bail!("unexpected arguments after '{command}'");
        }
        let trimmed = version.trim();
        if trimmed.eq_ignore_ascii_case("list") {
            let client = build_client()?;
            let default_args = UpgradeListArgs::default();
            list_releases(&client, &default_args).await?;
        } else {
            execute(UpgradeArgs {
                command: None,
                version: Some(if trimmed.is_empty() {
                    "latest".to_string()
                } else {
                    trimmed.to_string()
                }),
                print_only: false,
                suppress_version: None,
                clear_suppress: false,
            })
            .await?;
        }
        return Ok(true);
    }

    Ok(false)
}

pub async fn maybe_print_upgrade_notice() -> Result<()> {
    if std::env::var_os("DBX_NO_UPGRADE_CHECK").is_some() {
        return Ok(());
    }

    let path = upgrade_state_path()?;
    let mut state = load_upgrade_state(&path)?;
    let now = Utc::now();
    let mut latest_tag = state.latest_known_version.clone();
    let mut state_changed = false;

    if latest_tag.is_none() || is_check_stale(state.last_checked, now) {
        match build_client() {
            Ok(client) => match fetch_latest(&client).await {
                Ok(tag) => {
                    if state.latest_known_version.as_deref() != Some(tag.as_str()) {
                        state.latest_known_version = Some(tag.clone());
                        state_changed = true;
                    }
                    if state.last_checked.map_or(true, |ts| ts != now) {
                        state.last_checked = Some(now);
                        state_changed = true;
                    }
                    latest_tag = Some(tag);
                }
                Err(err) => {
                    debug!("skipping upgrade notice; failed to query latest release: {err}");
                }
            },
            Err(err) => {
                debug!("skipping upgrade notice; failed to build HTTP client: {err}");
            }
        }
    }

    if state_changed {
        if let Err(err) = save_upgrade_state(&path, &state) {
            debug!("failed to persist upgrade state: {err}");
        }
    }

    let Some(latest_tag) = latest_tag else {
        return Ok(());
    };

    if state.ignored_version.as_deref() == Some(latest_tag.as_str()) {
        return Ok(());
    }

    let current_version =
        Version::parse(env!("CARGO_PKG_VERSION")).expect("CARGO_PKG_VERSION is valid semver");
    let latest_version = match parse_release_tag(&latest_tag) {
        Ok(version) => version,
        Err(err) => {
            debug!("cannot parse latest release tag '{latest_tag}': {err}");
            return Ok(());
        }
    };

    if latest_version > current_version {
        println!(
            "A newer EventDBX CLI release ({latest_tag}) is available.\n  Upgrade:  dbx upgrade {latest_tag}\n  Suppress: dbx upgrade --suppress {latest_tag}"
        );
    }

    Ok(())
}

fn build_client() -> Result<Client> {
    Client::builder()
        .user_agent(USER_AGENT)
        .build()
        .context("failed to build HTTP client")
}

async fn resolve_release(client: &Client, spec: &str) -> Result<String> {
    let trimmed = spec.trim();
    let normalized = trimmed.strip_prefix('@').unwrap_or(trimmed).trim();
    if normalized.is_empty() || normalized.eq_ignore_ascii_case("latest") {
        return fetch_latest(client).await;
    }

    fetch_tag(client, normalized).await
}

async fn list_releases(client: &Client, args: &UpgradeListArgs) -> Result<()> {
    let limit = args.limit.max(1);
    let mut releases: Vec<GithubRelease> = Vec::new();
    let mut page: usize = 1;

    while releases.len() < limit {
        let remaining = limit - releases.len();
        let per_page = remaining.min(100).max(1);
        let response = client
            .get(RELEASES_API)
            .query(&[
                ("per_page", per_page.to_string()),
                ("page", page.to_string()),
            ])
            .header(ACCEPT, "application/vnd.github+json")
            .send()
            .await
            .with_context(|| format!("failed to list GitHub releases (page {})", page))?;

        if response.status() == StatusCode::NOT_FOUND {
            break;
        }

        let batch: Vec<GithubRelease> = response
            .error_for_status()
            .with_context(|| format!("GitHub returned an error listing releases (page {})", page))?
            .json()
            .await
            .with_context(|| format!("failed to parse GitHub releases response (page {})", page))?;

        if batch.is_empty() {
            break;
        }

        releases.extend(batch.into_iter());
        page += 1;
    }

    if releases.is_empty() {
        println!("(no releases found)");
        return Ok(());
    }

    releases.truncate(limit);

    if args.json {
        let listed: Vec<ListedRelease> = releases.iter().map(ListedRelease::from).collect();
        println!("{}", serde_json::to_string_pretty(&listed)?);
    } else {
        for release in &releases {
            let mut line = release.tag_name.clone();
            if let Some(published_at) = release.published_at.as_deref() {
                line.push_str(&format!("  {}", published_at));
            }
            let mut flags: Vec<&str> = Vec::new();
            if release.prerelease {
                flags.push("prerelease");
            }
            if release.draft {
                flags.push("draft");
            }
            if !flags.is_empty() {
                line.push_str(&format!("  [{}]", flags.join(", ")));
            }
            println!("{}", line);
        }
    }

    Ok(())
}

async fn fetch_latest(client: &Client) -> Result<String> {
    let url = format!("{}/latest", RELEASES_API);
    let response = client
        .get(&url)
        .header(ACCEPT, "application/vnd.github+json")
        .send()
        .await
        .with_context(|| "failed to contact GitHub releases API for latest tag")?;
    if response.status() == StatusCode::NOT_FOUND {
        bail!("no releases found for EventDBX");
    }
    let release: GithubRelease = response
        .error_for_status()
        .with_context(|| "GitHub returned an error when fetching the latest release")?
        .json()
        .await
        .with_context(|| "failed to parse GitHub latest release response")?;
    validate_supported_version(&release.tag_name)?;
    Ok(release.tag_name)
}

async fn fetch_tag(client: &Client, spec: &str) -> Result<String> {
    let tag = if spec.starts_with('v') {
        spec.to_string()
    } else {
        format!("v{}", spec)
    };
    validate_supported_version(&tag)?;
    let url = format!("{}/tags/{}", RELEASES_API, tag);
    let response = client
        .get(&url)
        .header(ACCEPT, "application/vnd.github+json")
        .send()
        .await
        .with_context(|| format!("failed to contact GitHub releases API for tag {}", tag))?;
    if response.status() == StatusCode::NOT_FOUND {
        bail!("release '{}' was not found on GitHub", tag);
    }
    let release: GithubRelease = response
        .error_for_status()
        .with_context(|| format!("GitHub returned an error when fetching tag {}", tag))?
        .json()
        .await
        .with_context(|| format!("failed to parse GitHub response for tag {}", tag))?;
    Ok(release.tag_name)
}

#[derive(Clone, Debug, Deserialize)]
struct GithubRelease {
    tag_name: String,
    name: Option<String>,
    published_at: Option<String>,
    #[serde(default)]
    draft: bool,
    #[serde(default)]
    prerelease: bool,
    html_url: Option<String>,
}

#[derive(Debug, Serialize)]
struct ListedRelease {
    tag: String,
    name: Option<String>,
    published_at: Option<String>,
    draft: bool,
    prerelease: bool,
    html_url: Option<String>,
}

impl From<&GithubRelease> for ListedRelease {
    fn from(release: &GithubRelease) -> Self {
        Self {
            tag: release.tag_name.clone(),
            name: release.name.clone(),
            published_at: release.published_at.clone(),
            draft: release.draft,
            prerelease: release.prerelease,
            html_url: release.html_url.clone(),
        }
    }
}

impl Default for UpgradeListArgs {
    fn default() -> Self {
        Self {
            limit: 20,
            json: false,
        }
    }
}

fn validate_supported_version(tag: &str) -> Result<()> {
    let normalized = tag.trim().trim_start_matches('v');
    let parsed =
        Version::parse(normalized).with_context(|| format!("invalid version tag '{}'", tag))?;
    let min =
        Version::parse(MIN_UPGRADE_VERSION).expect("MIN_UPGRADE_VERSION is a valid semver string");
    if parsed < min {
        bail!(
            "upgrading to '{}' is not supported; minimum supported version is v{}",
            tag,
            MIN_UPGRADE_VERSION
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use tempfile::tempdir;

    #[test]
    fn normalize_release_spec_adds_prefix() {
        let result = normalize_release_spec("1.13.3").expect("spec should normalize");
        assert_eq!(result, "v1.13.3");
    }

    #[test]
    fn normalize_release_spec_accepts_prefixed_value() {
        let result = normalize_release_spec("v1.14.0").expect("spec should normalize");
        assert_eq!(result, "v1.14.0");
    }

    #[test]
    fn normalize_release_spec_supports_latest() {
        let result = normalize_release_spec("latest").expect("latest should be accepted");
        assert_eq!(result, "latest");
    }

    #[test]
    fn normalize_release_spec_rejects_empty() {
        assert!(normalize_release_spec(" ").is_err());
    }

    #[test]
    fn check_interval_detection() {
        let now = Utc
            .with_ymd_and_hms(2024, 7, 1, 12, 0, 0)
            .single()
            .expect("valid date");

        assert!(is_check_stale(None, now));

        let old = now - Duration::hours(UPDATE_CHECK_INTERVAL_HOURS + 1);
        assert!(is_check_stale(Some(old), now));

        let fresh = now - Duration::hours(UPDATE_CHECK_INTERVAL_HOURS - 1);
        assert!(!is_check_stale(Some(fresh), now));

        let future = now + Duration::hours(1);
        assert!(is_check_stale(Some(future), now));
    }

    #[test]
    fn upgrade_state_round_trip() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("state.json");
        let mut state = UpgradeNoticeState::default();
        state.ignored_version = Some("v1.15.1".to_string());
        state.latest_known_version = Some("v1.15.3".to_string());
        state.last_checked = Some(
            Utc.with_ymd_and_hms(2024, 6, 30, 8, 0, 0)
                .single()
                .expect("valid timestamp"),
        );

        save_upgrade_state(&path, &state).expect("state should persist");
        let loaded = load_upgrade_state(&path).expect("state should load");
        assert_eq!(loaded.ignored_version, state.ignored_version);
        assert_eq!(loaded.latest_known_version, state.latest_known_version);
        assert_eq!(loaded.last_checked, state.last_checked);
    }
}
