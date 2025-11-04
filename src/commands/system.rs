use std::{
    fs,
    fs::File,
    io::{self, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use clap::Args;
use eventdbx::config::{Config, default_config_path, load_or_default};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use serde::{Deserialize, Serialize};
use tar::{Archive, Builder};
use tempfile::tempdir;

#[derive(Args)]
pub struct BackupArgs {
    /// Destination path for the backup archive (.tar.gz)
    #[arg(long)]
    pub output: PathBuf,

    /// Overwrite the output file if it already exists
    #[arg(long, default_value_t = false)]
    pub force: bool,
}

#[derive(Args)]
pub struct RestoreArgs {
    /// Path to the backup archive (.tar.gz)
    #[arg(long)]
    pub input: PathBuf,

    /// Override the data directory recorded in the backup
    #[arg(long)]
    pub data_dir: Option<PathBuf>,

    /// Replace existing data; required if the target directory is not empty
    #[arg(long, default_value_t = false)]
    pub force: bool,
}

#[derive(Serialize, Deserialize)]
struct BackupManifest {
    version: u32,
    created_at: DateTime<Utc>,
    config: Config,
    config_path: String,
}

fn capture_effective_config(config: &Config) -> Config {
    let mut resolved = config.clone();
    resolved.data_encryption_key = config.resolved_data_encryption_key();
    resolved.auth.private_key = config.auth.resolved_private_key();
    resolved.auth.public_key = config.auth.resolved_public_key();
    resolved.auth.key_id = config.auth.resolved_key_id();
    resolved
}

#[derive(Deserialize)]
struct PidRecord {
    pid: u32,
}

pub fn backup(config_path: Option<PathBuf>, args: BackupArgs) -> Result<()> {
    let (config, resolved_config_path) = load_or_default(config_path)?;

    ensure_server_stopped(&config)?;
    config.ensure_data_dir()?;

    let BackupArgs { output, force } = args;
    let output_path = output;
    if output_path.starts_with(&config.data_dir) {
        bail!(
            "backup output path {} resides inside the data directory {}; choose a different location",
            output_path.display(),
            config.data_dir.display(),
        );
    }

    if output_path.exists() {
        if force {
            fs::remove_file(&output_path).with_context(|| {
                format!(
                    "failed to remove existing backup at {}",
                    output_path.display()
                )
            })?;
        } else {
            bail!(
                "backup target {} already exists (use --force to overwrite)",
                output_path.display()
            );
        }
    } else if let Some(parent) = output_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create parent directory for {}",
                    output_path.display()
                )
            })?;
        }
    }

    let file = File::create(&output_path)
        .with_context(|| format!("failed to create {}", output_path.display()))?;
    let encoder = GzEncoder::new(file, Compression::default());
    let mut builder = Builder::new(encoder);

    let manifest = BackupManifest {
        version: 1,
        created_at: Utc::now(),
        config: capture_effective_config(&config),
        config_path: resolved_config_path.to_string_lossy().into_owned(),
    };
    let manifest_bytes =
        serde_json::to_vec_pretty(&manifest).context("failed to encode backup manifest")?;
    append_bytes(&mut builder, "manifest.json", &manifest_bytes)?;

    builder
        .append_dir_all("data", &config.data_dir)
        .with_context(|| format!("failed to archive {}", config.data_dir.display()))?;

    let encoder = builder
        .into_inner()
        .context("failed to finalize archive stream")?;
    encoder
        .finish()
        .context("failed to finish writing backup archive")?;

    println!(
        "Backup created at {} (captured data directory {})",
        output_path.display(),
        config.data_dir.display()
    );
    Ok(())
}

pub fn restore(config_path: Option<PathBuf>, args: RestoreArgs) -> Result<()> {
    let RestoreArgs {
        input,
        data_dir,
        force,
    } = args;

    if !input.exists() {
        bail!("backup archive {} does not exist", input.display());
    }

    let target_config_path = match config_path {
        Some(path) => path,
        None => default_config_path().context("failed to resolve default config path")?,
    };

    let archive_file =
        File::open(&input).with_context(|| format!("failed to open {}", input.display()))?;
    let decoder = GzDecoder::new(archive_file);
    let mut archive = Archive::new(decoder);
    let temp_dir = tempdir().context("failed to create temporary working directory")?;

    archive
        .unpack(temp_dir.path())
        .context("failed to unpack backup archive")?;

    let manifest_path = temp_dir.path().join("manifest.json");
    if !manifest_path.exists() {
        bail!("backup archive does not contain a manifest.json");
    }
    let manifest: BackupManifest = {
        let contents =
            fs::read(&manifest_path).with_context(|| "failed to read manifest from backup")?;
        serde_json::from_slice(&contents).with_context(|| "failed to decode manifest.json")?
    };

    let mut restored_config = manifest.config;
    if let Some(data_dir) = data_dir {
        restored_config.data_dir = data_dir;
    }
    restored_config.updated_at = Utc::now();
    restored_config = capture_effective_config(&restored_config);

    ensure_server_stopped(&restored_config)?;

    let backup_data_root = temp_dir.path().join("data");
    if !backup_data_root.exists() {
        bail!("backup archive is missing the data directory");
    }

    prepare_target_data_dir(&restored_config.data_dir, force)?;

    copy_dir_all(&backup_data_root, &restored_config.data_dir).with_context(|| {
        format!(
            "failed to restore into {}",
            restored_config.data_dir.display()
        )
    })?;

    if let Some(parent) = target_config_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create directory for config at {}",
                    parent.display()
                )
            })?;
        }
    }
    restored_config.save(&target_config_path).with_context(|| {
        format!(
            "failed to write restored config to {}",
            target_config_path.display()
        )
    })?;

    println!(
        "Restored backup from {} into {} (config written to {})",
        input.display(),
        restored_config.data_dir.display(),
        target_config_path.display()
    );
    Ok(())
}

fn append_bytes<W: Write>(builder: &mut Builder<W>, path: &str, data: &[u8]) -> Result<()> {
    let mut header = tar::Header::new_gnu();
    header.set_size(data.len() as u64);
    header.set_mode(0o644);
    let mtime = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    header.set_mtime(mtime);
    header.set_cksum();
    builder.append_data(&mut header, path, data)?;
    Ok(())
}

fn prepare_target_data_dir(target: &Path, force: bool) -> Result<()> {
    if target.exists() {
        if force {
            fs::remove_dir_all(target).with_context(|| {
                format!(
                    "failed to remove existing data directory {}",
                    target.display()
                )
            })?;
        } else if dir_contains_entries(target)? {
            bail!(
                "data directory {} is not empty (use --force to overwrite)",
                target.display()
            );
        }
    }
    fs::create_dir_all(target).with_context(|| format!("failed to create {}", target.display()))?;
    Ok(())
}

fn dir_contains_entries(path: &Path) -> Result<bool> {
    if !path.is_dir() {
        return Ok(false);
    }
    Ok(fs::read_dir(path)?.next().is_some())
}

fn copy_dir_all(src: &Path, dst: &Path) -> Result<()> {
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let target_path = dst.join(entry.file_name());
        if file_type.is_dir() {
            fs::create_dir_all(&target_path)?;
            copy_dir_all(&entry.path(), &target_path)?;
        } else if file_type.is_file() {
            if let Some(parent) = target_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(entry.path(), &target_path).with_context(|| {
                format!(
                    "failed to copy {} to {}",
                    entry.path().display(),
                    target_path.display()
                )
            })?;
        } else if file_type.is_symlink() {
            let source_path = entry.path();
            let target = fs::read_link(&source_path)
                .with_context(|| format!("failed to read link {}", source_path.display()))?;
            create_symlink(&source_path, &target, &target_path).with_context(|| {
                format!(
                    "failed to recreate symlink {} -> {}",
                    target_path.display(),
                    target.display()
                )
            })?;
        }
    }
    Ok(())
}

fn create_symlink(source: &Path, original: &Path, link: &Path) -> io::Result<()> {
    #[cfg(unix)]
    let _ = source;
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(original, link)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::{symlink_dir, symlink_file};
        let metadata = fs::metadata(source);
        match metadata {
            Ok(meta) if meta.is_dir() => symlink_dir(original, link),
            Ok(_) => symlink_file(original, link),
            Err(_) => {
                // Fallback to file symlink if metadata is unavailable.
                match symlink_file(original, link) {
                    Ok(()) => Ok(()),
                    Err(err) => {
                        // Attempt directory symlink as a final fallback.
                        symlink_dir(original, link).or(Err(err))
                    }
                }
            }
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = source;
        let _ = original;
        let _ = link;
        Err(io::Error::new(
            io::ErrorKind::Other,
            "symlink restoration is not supported on this platform",
        ))
    }
}

fn ensure_server_stopped(config: &Config) -> Result<()> {
    let pid_path = config.pid_file_path();
    if !pid_path.exists() {
        return Ok(());
    }
    match active_pid(&pid_path)? {
        Some(pid) if process_is_running(pid) => bail!(
            "EventDBX server appears to be running (pid {}); stop it before continuing",
            pid
        ),
        _ => Ok(()),
    }
}

fn active_pid(path: &Path) -> Result<Option<u32>> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read pid file at {}", path.display()))?;
    let trimmed = contents.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if let Ok(record) = serde_json::from_str::<PidRecord>(trimmed) {
        return Ok(Some(record.pid));
    }
    trimmed
        .parse::<u32>()
        .map(Some)
        .map_err(|err| anyhow!("invalid pid contents at {}: {}", path.display(), err))
}

fn process_is_running(pid: u32) -> bool {
    #[cfg(unix)]
    {
        unsafe {
            if libc::kill(pid as libc::pid_t, 0) == 0 {
                true
            } else {
                let err = io::Error::last_os_error();
                !matches!(err.raw_os_error(), Some(libc::ESRCH))
            }
        }
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::{
            Foundation::CloseHandle,
            System::Threading::{OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION},
        };
        unsafe {
            let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
            if handle == 0 {
                false
            } else {
                CloseHandle(handle);
                true
            }
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = pid;
        false
    }
}
