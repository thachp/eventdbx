use std::{
    collections::BTreeSet,
    fs,
    io::Cursor,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration, Utc};
use clap::{Args, Subcommand};
use reqwest::{Client, StatusCode, header::ACCEPT};
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json;
use tempfile::tempdir;
use tracing::{debug, warn};
use xz2::read::XzDecoder;
use zip::ZipArchive;

use eventdbx::config::default_config_path;

const USER_AGENT: &str = concat!("eventdbx-cli/", env!("CARGO_PKG_VERSION"));
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
    /// Show the download and activation steps without performing them
    #[arg(long, default_value_t = false)]
    pub print_only: bool,
    /// Download and store the release without switching the active binary
    #[arg(long, default_value_t = false)]
    pub no_switch: bool,
    /// Suppress upgrade notices for the provided release tag
    #[arg(
        long = "suppress",
        value_name = "VERSION",
        conflicts_with_all = ["version", "print_only", "clear_suppress", "no_switch"],
        help = "Suppress upgrade reminders for the specified release (e.g. v1.15.3)"
    )]
    pub suppress_version: Option<String>,
    /// Clear any suppressed release tag so future notices appear
    #[arg(
        long = "clear-suppress",
        conflicts_with_all = ["version", "print_only", "suppress_version", "no_switch"],
        default_value_t = false
    )]
    pub clear_suppress: bool,
}

#[derive(Subcommand, Debug)]
pub enum UpgradeCommand {
    /// List available EventDBX releases
    List(UpgradeListArgs),
    /// Activate an installed EventDBX release
    Use(UpgradeUseArgs),
    /// Show locally installed EventDBX releases
    Installed(UpgradeInstalledArgs),
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

#[derive(Args, Debug, Clone)]
pub struct UpgradeUseArgs {
    /// Installed release tag to activate (e.g. v3.9.13)
    #[arg(value_name = "VERSION")]
    pub version: String,
}

#[derive(Args, Debug, Clone, Default)]
pub struct UpgradeInstalledArgs {
    /// Emit installed versions as JSON
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArchiveFormat {
    TarXz,
    Zip,
}

impl ArchiveFormat {
    fn extension(self) -> &'static str {
        match self {
            ArchiveFormat::TarXz => ".tar.xz",
            ArchiveFormat::Zip => ".zip",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TargetInfo {
    triple: &'static str,
    binary_name: &'static str,
    format: ArchiveFormat,
}

impl TargetInfo {
    fn asset_name(&self) -> String {
        format!("eventdbx-{}{}", self.triple, self.format.extension())
    }
}

fn current_target_info() -> Result<TargetInfo> {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    match (os, arch) {
        ("macos", "aarch64") => Ok(TargetInfo {
            triple: "aarch64-apple-darwin",
            binary_name: "dbx",
            format: ArchiveFormat::TarXz,
        }),
        ("macos", "x86_64") => Ok(TargetInfo {
            triple: "x86_64-apple-darwin",
            binary_name: "dbx",
            format: ArchiveFormat::TarXz,
        }),
        ("linux", "x86_64") => Ok(TargetInfo {
            triple: "x86_64-unknown-linux-gnu",
            binary_name: "dbx",
            format: ArchiveFormat::TarXz,
        }),
        ("linux", "aarch64") => Ok(TargetInfo {
            triple: "aarch64-unknown-linux-gnu",
            binary_name: "dbx",
            format: ArchiveFormat::TarXz,
        }),
        ("windows", "x86_64") => Ok(TargetInfo {
            triple: "x86_64-pc-windows-msvc",
            binary_name: "dbx.exe",
            format: ArchiveFormat::Zip,
        }),
        (other_os, other_arch) => bail!(
            "upgrades are not supported on this platform ({}-{})",
            other_os,
            other_arch
        ),
    }
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

fn versions_root() -> Result<PathBuf> {
    let config_path = default_config_path()?;
    let root = config_path
        .parent()
        .context("configuration path has no parent directory")?
        .to_path_buf();
    Ok(root.join("versions"))
}

fn target_versions_dir(target: &TargetInfo) -> Result<PathBuf> {
    Ok(versions_root()?.join(target.triple))
}

fn version_dir(target: &TargetInfo, version: &str) -> Result<PathBuf> {
    Ok(target_versions_dir(target)?.join(version))
}

fn current_version_tag() -> String {
    format!("v{}", env!("CARGO_PKG_VERSION"))
}

fn set_executable_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let metadata = fs::metadata(path)
            .with_context(|| format!("failed to read permissions for {}", path.display()))?;
        let mut perms = metadata.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms)
            .with_context(|| format!("failed to update permissions for {}", path.display()))?;
    }
    Ok(())
}

fn ensure_current_version_snapshot(target: &TargetInfo) -> Result<()> {
    let current_tag = current_version_tag();
    let dir = version_dir(target, &current_tag)?;
    let binary_path = dir.join(target.binary_name);
    if binary_path.exists() {
        if read_active_marker(target)?.is_none() {
            write_active_marker(target, &current_tag)?;
        }
        return Ok(());
    }

    let active_path = std::env::current_exe()
        .with_context(|| "failed to resolve current executable path for snapshot")?;

    fs::create_dir_all(&dir).with_context(|| format!("failed to create {}", dir.display()))?;
    fs::copy(&active_path, &binary_path).with_context(|| {
        format!(
            "failed to copy existing binary to {}",
            binary_path.display()
        )
    })?;
    set_executable_permissions(&binary_path)?;
    write_active_marker(target, &current_tag)?;
    Ok(())
}

fn installed_versions_set(target: &TargetInfo) -> Result<BTreeSet<String>> {
    let mut versions = BTreeSet::new();
    let dir = target_versions_dir(target)?;
    if dir.is_dir() {
        for entry in
            fs::read_dir(&dir).with_context(|| format!("failed to read {}", dir.display()))?
        {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    versions.insert(name.to_string());
                }
            }
        }
    }
    Ok(versions)
}

fn read_active_marker(target: &TargetInfo) -> Result<Option<String>> {
    let marker_path = target_versions_dir(target)?.join("active");
    if !marker_path.exists() {
        return Ok(None);
    }
    let contents = fs::read_to_string(&marker_path).with_context(|| {
        format!(
            "failed to read active version marker at {}",
            marker_path.display()
        )
    })?;
    Ok(Some(contents.trim().to_string()))
}

fn write_active_marker(target: &TargetInfo, version: &str) -> Result<()> {
    let dir = target_versions_dir(target)?;
    fs::create_dir_all(&dir).with_context(|| format!("failed to create {}", dir.display()))?;
    let marker_path = dir.join("active");
    fs::write(&marker_path, version).with_context(|| {
        format!(
            "failed to write active version marker at {}",
            marker_path.display()
        )
    })
}

fn version_binary_path(target: &TargetInfo, version: &str) -> Result<PathBuf> {
    Ok(version_dir(target, version)?.join(target.binary_name))
}

fn select_asset<'a>(release: &'a GithubRelease, target: &TargetInfo) -> Result<&'a GithubAsset> {
    let expected = target.asset_name();
    release
        .assets
        .iter()
        .find(|asset| asset.name == expected)
        .with_context(|| {
            format!(
                "release '{}' does not include asset '{}'",
                release.tag_name, expected
            )
        })
}

async fn download_asset(client: &Client, asset: &GithubAsset) -> Result<Vec<u8>> {
    let response = client
        .get(&asset.browser_download_url)
        .send()
        .await
        .with_context(|| {
            format!(
                "failed to download release asset from {}",
                asset.browser_download_url
            )
        })?
        .error_for_status()
        .with_context(|| {
            format!(
                "GitHub returned an error when downloading {}",
                asset.browser_download_url
            )
        })?;
    let bytes = response
        .bytes()
        .await
        .with_context(|| "failed to read release asset bytes")?;
    Ok(bytes.to_vec())
}

fn unpack_archive(bytes: &[u8], format: ArchiveFormat, output_dir: &Path) -> Result<()> {
    match format {
        ArchiveFormat::TarXz => {
            let decoder = XzDecoder::new(Cursor::new(bytes));
            let mut archive = tar::Archive::new(decoder);
            archive.unpack(output_dir).with_context(|| {
                format!("failed to unpack archive into {}", output_dir.display())
            })?;
        }
        ArchiveFormat::Zip => {
            let cursor = Cursor::new(bytes);
            let mut archive =
                ZipArchive::new(cursor).with_context(|| "failed to parse zip archive")?;
            archive.extract(output_dir).with_context(|| {
                format!("failed to extract archive into {}", output_dir.display())
            })?;
        }
    }
    Ok(())
}

fn find_binary(root: &Path, binary_name: &str) -> Result<PathBuf> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)
            .with_context(|| format!("failed to inspect extracted archive at {}", dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            let metadata = entry
                .metadata()
                .with_context(|| format!("failed to inspect {}", path.display()))?;
            if metadata.is_dir() {
                stack.push(path);
            } else if metadata.is_file() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name == binary_name {
                        return Ok(path);
                    }
                }
            }
        }
    }
    bail!(
        "failed to locate binary '{}' in extracted archive",
        binary_name
    );
}

fn copy_binary_to_version(source: &Path, target_dir: &Path, binary_name: &str) -> Result<PathBuf> {
    fs::create_dir_all(target_dir)
        .with_context(|| format!("failed to create {}", target_dir.display()))?;
    let destination = target_dir.join(binary_name);
    fs::copy(source, &destination)
        .with_context(|| format!("failed to copy binary to {}", destination.display()))?;
    set_executable_permissions(&destination)?;
    Ok(destination)
}

async fn ensure_release_installed(
    client: &Client,
    release: &GithubRelease,
    target: &TargetInfo,
    print_only: bool,
) -> Result<PathBuf> {
    let version = &release.tag_name;
    let version_dir = version_dir(target, version)?;
    let binary_path = version_binary_path(target, version)?;
    if binary_path.exists() {
        if print_only {
            println!(
                "EventDBX CLI ({}) is already installed at {}",
                version,
                version_dir.display()
            );
        } else {
            println!(
                "EventDBX CLI ({}) is already installed at {}",
                version,
                version_dir.display()
            );
        }
        return Ok(version_dir);
    }

    let asset = select_asset(release, target)?;
    if print_only {
        println!(
            "Would download EventDBX CLI ({}) from {}",
            version, asset.browser_download_url
        );
        println!("Install location: {}", version_dir.display());
        return Ok(version_dir);
    }

    println!(
        "Downloading EventDBX CLI ({}) for {}",
        version, target.triple
    );
    let bytes = download_asset(client, asset).await?;
    let temp = tempdir().context("failed to create temporary directory for upgrade")?;
    unpack_archive(&bytes, target.format, temp.path())?;
    let extracted = find_binary(temp.path(), target.binary_name)?;
    copy_binary_to_version(&extracted, &version_dir, target.binary_name)?;
    println!(
        "Stored EventDBX CLI ({}) in {}",
        version,
        version_dir.display()
    );
    Ok(version_dir)
}

#[cfg(not(windows))]
fn replace_active_binary(source: &Path, active_path: &Path) -> Result<()> {
    let staged = active_path.with_file_name(format!(".dbx-tmp-{}", std::process::id()));
    fs::copy(source, &staged)
        .with_context(|| format!("failed to stage new binary at {}", staged.display()))?;
    set_executable_permissions(&staged)?;
    fs::rename(&staged, active_path).with_context(|| {
        format!(
            "failed to replace active binary at {}",
            active_path.display()
        )
    })?;
    Ok(())
}

#[cfg(windows)]
fn replace_active_binary(_source: &Path, _active_path: &Path) -> Result<()> {
    bail!(
        "switching installed versions is not supported on Windows yet; copy the binary from the versions directory manually or rerun the published installer"
    );
}

fn activate_installed_version(target: &TargetInfo, version: &str, print_only: bool) -> Result<()> {
    let source = version_binary_path(target, version)?;
    if !source.exists() {
        bail!(
            "version '{}' is not installed; run 'dbx upgrade {}' first",
            version,
            version
        );
    }

    let active_path =
        std::env::current_exe().with_context(|| "failed to resolve current executable path")?;
    if print_only {
        println!(
            "Would replace active binary at {} with {}",
            active_path.display(),
            source.display()
        );
        return Ok(());
    }

    replace_active_binary(&source, &active_path)?;
    write_active_marker(target, version)?;
    println!("Active EventDBX CLI switched to {}.", version);
    println!("Re-run your command to use the new version.");
    Ok(())
}

fn print_installed_versions(target: &TargetInfo, args: &UpgradeInstalledArgs) -> Result<()> {
    let versions = installed_versions_set(target)?;
    let active = read_active_marker(target)?;

    if args.json {
        let payload = InstalledVersionsList {
            versions: versions.iter().cloned().collect::<Vec<_>>(),
            active,
            target: target.triple.to_string(),
        };
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else if versions.is_empty() {
        println!("(no versions installed)");
    } else {
        for version in versions {
            if active.as_deref() == Some(version.as_str()) {
                println!("{}  [active]", version);
            } else {
                println!("{}", version);
            }
        }
    }

    Ok(())
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
    if args.command.is_some() && args.no_switch {
        bail!("--no-switch cannot be used with subcommands");
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
            resolve_release(&client, "latest").await?.tag_name
        } else {
            normalized
        };
        update_suppressed_version(Some(tag.clone()))?;
        println!("Suppressing upgrade reminders for {tag}");
        return Ok(());
    }

    let target = current_target_info()?;
    ensure_current_version_snapshot(&target)?;

    if let Some(subcommand) = args.command {
        match subcommand {
            UpgradeCommand::List(list_args) => {
                list_releases(&client, &list_args, &target).await?;
            }
            UpgradeCommand::Use(use_args) => {
                let normalized = normalize_release_spec(&use_args.version)?;
                if normalized.eq_ignore_ascii_case("latest") {
                    bail!("please specify a concrete release tag when switching versions");
                }
                activate_installed_version(&target, &normalized, args.print_only)?;
            }
            UpgradeCommand::Installed(installed_args) => {
                print_installed_versions(&target, &installed_args)?;
            }
        }
        return Ok(());
    }

    let version_spec = args.version.as_deref().unwrap_or("latest");
    let release = resolve_release(&client, version_spec).await?;
    let tag = release.tag_name.clone();

    if args.print_only {
        let version_directory = version_dir(&target, &tag)?;
        let binary_path = version_binary_path(&target, &tag)?;
        if binary_path.exists() {
            println!(
                "EventDBX CLI ({}) is already installed at {}",
                tag,
                version_directory.display()
            );
        } else {
            let asset = select_asset(&release, &target)?;
            println!(
                "Would download EventDBX CLI ({}) from {}",
                tag, asset.browser_download_url
            );
            println!("Install location: {}", version_directory.display());
        }
        let active_path =
            std::env::current_exe().with_context(|| "failed to resolve current executable path")?;
        if args.no_switch {
            println!("Active binary would remain at {}.", active_path.display());
        } else {
            println!(
                "Active binary at {} would be replaced with {} after download.",
                active_path.display(),
                tag
            );
        }
        return Ok(());
    }

    ensure_release_installed(&client, &release, &target, false).await?;
    if args.no_switch {
        println!(
            "Installation complete. Activate this version with `dbx upgrade use {}`.",
            tag
        );
        return Ok(());
    }

    activate_installed_version(&target, &tag, false)?;
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
            let target = current_target_info()?;
            ensure_current_version_snapshot(&target)?;
            list_releases(&client, &default_args, &target).await?;
        } else {
            execute(UpgradeArgs {
                command: None,
                version: Some(if trimmed.is_empty() {
                    "latest".to_string()
                } else {
                    trimmed.to_string()
                }),
                print_only: false,
                no_switch: false,
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
            Ok(client) => match fetch_latest_release(&client).await {
                Ok(release) => {
                    let tag = release.tag_name.clone();
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

async fn resolve_release(client: &Client, spec: &str) -> Result<GithubRelease> {
    let trimmed = spec.trim();
    let normalized = trimmed.strip_prefix('@').unwrap_or(trimmed).trim();
    if normalized.is_empty() || normalized.eq_ignore_ascii_case("latest") {
        return fetch_latest_release(client).await;
    }

    fetch_tag_release(client, normalized).await
}

async fn list_releases(client: &Client, args: &UpgradeListArgs, target: &TargetInfo) -> Result<()> {
    let limit = args.limit.max(1);
    let mut releases: Vec<GithubRelease> = Vec::new();
    let mut page: usize = 1;
    let installed = installed_versions_set(target)?;
    let active = read_active_marker(target)?;

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
        let listed: Vec<ListedRelease> = releases
            .iter()
            .map(|release| {
                ListedRelease::from_release(
                    release,
                    installed.contains(&release.tag_name),
                    active.as_deref() == Some(release.tag_name.as_str()),
                )
            })
            .collect();
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
            if active.as_deref() == Some(release.tag_name.as_str()) {
                flags.push("active");
            } else if installed.contains(&release.tag_name) {
                flags.push("installed");
            }
            if !flags.is_empty() {
                line.push_str(&format!("  [{}]", flags.join(", ")));
            }
            println!("{}", line);
        }
    }

    Ok(())
}

async fn fetch_latest_release(client: &Client) -> Result<GithubRelease> {
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
    Ok(release)
}

async fn fetch_tag_release(client: &Client, spec: &str) -> Result<GithubRelease> {
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
    Ok(release)
}

#[derive(Clone, Debug, Deserialize)]
struct GithubAsset {
    name: String,
    browser_download_url: String,
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
    #[serde(default)]
    assets: Vec<GithubAsset>,
}

#[derive(Debug, Serialize)]
struct ListedRelease {
    tag: String,
    name: Option<String>,
    published_at: Option<String>,
    draft: bool,
    prerelease: bool,
    html_url: Option<String>,
    installed: bool,
    active: bool,
}

impl ListedRelease {
    fn from_release(release: &GithubRelease, installed: bool, active: bool) -> Self {
        Self {
            tag: release.tag_name.clone(),
            name: release.name.clone(),
            published_at: release.published_at.clone(),
            draft: release.draft,
            prerelease: release.prerelease,
            html_url: release.html_url.clone(),
            installed,
            active,
        }
    }
}

#[derive(Debug, Serialize)]
struct InstalledVersionsList {
    versions: Vec<String>,
    active: Option<String>,
    target: String,
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
    use std::fs;
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
    fn find_binary_locates_nested_target() {
        let dir = tempdir().expect("create temp dir");
        let nested = dir.path().join("deep").join("bin");
        fs::create_dir_all(&nested).expect("create nested dir");
        let binary = nested.join("dbx");
        fs::write(&binary, b"fake-binary").expect("write binary stub");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&binary).expect("metadata").permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&binary, perms).expect("set permissions");
        }

        let found = find_binary(dir.path(), "dbx").expect("binary should be discovered");
        assert_eq!(found, binary);
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
