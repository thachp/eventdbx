use std::process::Command;

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};
use reqwest::{Client, StatusCode, header::ACCEPT};
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json;

const USER_AGENT: &str = concat!("eventdbx-cli/", env!("CARGO_PKG_VERSION"));
const REPO_BASE: &str = "https://github.com/thachp/eventdbx";
const INSTALLER_SH: &str = "eventdbx-installer.sh";
const INSTALLER_PS1: &str = "eventdbx-installer.ps1";
const RELEASES_API: &str = "https://api.github.com/repos/thachp/eventdbx/releases";
const MIN_UPGRADE_VERSION: &str = "1.13.2";

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

pub async fn execute(args: UpgradeArgs) -> Result<()> {
    let client = build_client()?;

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
            })
            .await?;
        }
        return Ok(true);
    }

    Ok(false)
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
