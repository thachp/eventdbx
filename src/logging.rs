use std::{
    cmp::Reverse,
    fs,
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::SystemTime,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Local, NaiveDate};
use flate2::{Compression, write::GzEncoder};
use parking_lot::Mutex;
use std::ffi::OsStr;
use tracing::warn;
use tracing_appender::non_blocking::{self, WorkerGuard};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

const LOG_DIR_ENV: &str = "EVENTDBX_LOG_DIR";
const LOG_PREFIX: &str = "eventdbx";
const ACTIVE_FILE_NAME: &str = "eventdbx.log";
const MAX_RETAINED_LOGS: usize = 14;

static FILE_GUARD: OnceLock<WorkerGuard> = OnceLock::new();
static PANIC_HOOK: OnceLock<()> = OnceLock::new();

#[derive(Clone)]
struct DailyRotatingWriter {
    inner: Arc<WriterInner>,
}

struct WriterInner {
    state: Mutex<WriterState>,
    log_dir: PathBuf,
}

struct WriterState {
    file: Option<BufWriter<fs::File>>,
    current_day: NaiveDate,
    period_start: DateTime<Local>,
}

pub fn init() -> Result<()> {
    if FILE_GUARD.get().is_some() {
        return Ok(());
    }

    let log_dir = resolve_log_dir()?;
    let writer = DailyRotatingWriter::new(log_dir)?;
    let (file_writer, guard) = non_blocking::NonBlockingBuilder::default()
        .lossy(false)
        .finish(writer);

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let stdout_layer = fmt::layer().with_target(false);
    let file_layer = fmt::layer()
        .with_writer(file_writer.clone())
        .with_target(true)
        .with_ansi(false);

    let subscriber = tracing_subscriber::registry()
        .with(env_filter)
        .with(stdout_layer)
        .with(file_layer);

    match subscriber.try_init() {
        Ok(_) => {
            let _ = FILE_GUARD.set(guard);
            install_panic_hook();
        }
        Err(_) => {
            // Subscriber already installed elsewhere; drop guard so the worker thread exits.
            drop(guard);
        }
    }

    Ok(())
}

impl DailyRotatingWriter {
    fn new<P: Into<PathBuf>>(dir: P) -> Result<Self> {
        let log_dir = dir.into();
        fs::create_dir_all(&log_dir)
            .with_context(|| format!("failed to create log directory {}", log_dir.display()))?;

        let active_path = log_dir.join(ACTIVE_FILE_NAME);
        let now = Local::now();

        Self::rotate_stale_file(&log_dir, &active_path, now)?;

        let file = Some(Self::open_writer(&active_path)?);
        let state = WriterState {
            file,
            current_day: now.date_naive(),
            period_start: now,
        };

        Ok(Self {
            inner: Arc::new(WriterInner {
                state: Mutex::new(state),
                log_dir,
            }),
        })
    }

    fn rotate_stale_file(log_dir: &Path, active_path: &Path, now: DateTime<Local>) -> Result<()> {
        let metadata = match fs::metadata(active_path) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "unable to inspect existing log file {}",
                        active_path.display()
                    )
                });
            }
        };

        let modified = metadata.modified().ok().map(DateTime::<Local>::from);
        if let Some(modified_at) = modified {
            if modified_at.date_naive() == now.date_naive() {
                return Ok(());
            }
            let rotated_path = Self::unique_rotated_path(log_dir, modified_at);
            fs::rename(active_path, &rotated_path).with_context(|| {
                format!(
                    "failed to rotate stale log {} -> {}",
                    active_path.display(),
                    rotated_path.display()
                )
            })?;
            Self::finalize_rotated_file(log_dir, &rotated_path);
            return Ok(());
        }

        let rotated_path = Self::unique_rotated_path(log_dir, now);
        fs::rename(active_path, &rotated_path).with_context(|| {
            format!(
                "failed to rotate stale log {} -> {}",
                active_path.display(),
                rotated_path.display()
            )
        })?;
        Self::finalize_rotated_file(log_dir, &rotated_path);
        Ok(())
    }

    fn open_writer(path: &Path) -> Result<BufWriter<fs::File>> {
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .open(path)
            .with_context(|| format!("failed to open log file {}", path.display()))?;
        Ok(BufWriter::new(file))
    }

    fn rotate(&self, state: &mut WriterState, now: DateTime<Local>) -> Result<()> {
        if let Some(mut writer) = state.file.take() {
            if let Err(err) = writer.flush() {
                eprintln!("failed to flush log file before rotation: {err}");
            }
            drop(writer);
        }

        let active_path = self.active_path();
        if active_path.exists() {
            let rotated_path = Self::unique_rotated_path(&self.inner.log_dir, state.period_start);
            fs::rename(&active_path, &rotated_path).with_context(|| {
                format!(
                    "failed to rotate log {} -> {}",
                    active_path.display(),
                    rotated_path.display()
                )
            })?;
            Self::finalize_rotated_file(&self.inner.log_dir, &rotated_path);
        }

        state.file = Some(Self::open_writer(&active_path)?);
        state.current_day = now.date_naive();
        state.period_start = now;

        Ok(())
    }

    fn unique_rotated_path(dir: &Path, timestamp: DateTime<Local>) -> PathBuf {
        let base = format!("{}_{}", LOG_PREFIX, timestamp.format("%Y-%m-%d_%H-%M-%S"));
        let mut candidate = dir.join(format!("{}.log", base));
        let mut counter = 1;
        while candidate.exists() {
            candidate = dir.join(format!("{}-{}.log", base, counter));
            counter += 1;
        }
        candidate
    }

    fn finalize_rotated_file(log_dir: &Path, rotated_path: &Path) {
        if let Err(err) = Self::compress_file(rotated_path) {
            warn!(
                "failed to compress rotated log {}: {}",
                rotated_path.display(),
                err
            );
        }
        if let Err(err) = Self::enforce_retention(log_dir) {
            warn!(
                "failed to enforce log retention in {}: {}",
                log_dir.display(),
                err
            );
        }
    }

    fn compress_file(path: &Path) -> Result<PathBuf> {
        if path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("gz"))
            .unwrap_or(false)
        {
            return Ok(path.to_path_buf());
        }

        let new_extension = match path.extension().and_then(|ext| ext.to_str()) {
            Some(ext) if !ext.is_empty() => format!("{ext}.gz"),
            _ => "log.gz".to_string(),
        };
        let gz_path = path.with_extension(new_extension);

        let mut input = fs::File::open(path)
            .with_context(|| format!("failed to open {} for compression", path.display()))?;
        let output = fs::File::create(&gz_path)
            .with_context(|| format!("failed to create compressed log {}", gz_path.display()))?;
        let mut encoder = GzEncoder::new(output, Compression::default());
        io::copy(&mut input, &mut encoder)
            .with_context(|| format!("failed to compress {}", path.display()))?;
        let _ = encoder
            .finish()
            .with_context(|| format!("failed to finish compression for {}", gz_path.display()))?;
        drop(input);
        fs::remove_file(path)
            .with_context(|| format!("failed to remove uncompressed log {}", path.display()))?;

        Ok(gz_path)
    }

    fn enforce_retention(log_dir: &Path) -> Result<()> {
        let mut entries: Vec<(SystemTime, PathBuf)> = Vec::new();
        for entry in fs::read_dir(log_dir)
            .with_context(|| format!("failed to inspect log directory {}", log_dir.display()))?
        {
            let entry = entry.with_context(|| {
                format!(
                    "failed to inspect log directory entry in {}",
                    log_dir.display()
                )
            })?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            let file_name = match path.file_name().and_then(OsStr::to_str) {
                Some(name) => name,
                None => continue,
            };

            if file_name == ACTIVE_FILE_NAME || !file_name.starts_with(LOG_PREFIX) {
                continue;
            }

            let metadata = entry
                .metadata()
                .with_context(|| format!("failed to read metadata for {}", path.display()))?;
            let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            entries.push((modified, path));
        }

        entries.sort_by_key(|(modified, _)| Reverse(*modified));
        while entries.len() > MAX_RETAINED_LOGS {
            if let Some((_, path)) = entries.pop() {
                if let Err(err) = fs::remove_file(&path) {
                    warn!("failed to remove expired log {}: {}", path.display(), err);
                }
            }
        }

        Ok(())
    }

    fn active_path(&self) -> PathBuf {
        self.inner.log_dir.join(ACTIVE_FILE_NAME)
    }
}

impl Write for DailyRotatingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let now = Local::now();
        let mut state = self.inner.state.lock();

        if now.date_naive() != state.current_day {
            if let Err(err) = self.rotate(&mut state, now) {
                eprintln!("failed to rotate logs: {err:?}");
            }
        }

        if state.file.is_none() {
            state.file = Some(
                Self::open_writer(&self.active_path())
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?,
            );
            state.current_day = now.date_naive();
            state.period_start = now;
        }

        let writer = state
            .file
            .as_mut()
            .expect("log writer must be available after rotation");
        writer.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut state = self.inner.state.lock();
        if let Some(writer) = state.file.as_mut() {
            writer.flush()
        } else {
            Ok(())
        }
    }
}

fn resolve_log_dir() -> Result<PathBuf> {
    if let Ok(dir) = std::env::var(LOG_DIR_ENV) {
        let path = PathBuf::from(dir);
        if path.is_absolute() {
            return Ok(path);
        }
        let base =
            std::env::current_dir().context("failed to resolve current working directory")?;
        return Ok(base.join(path));
    }

    let home = dirs::home_dir().context("unable to locate user home directory")?;
    Ok(home.join(".eventdbx").join("logs"))
}

fn install_panic_hook() {
    PANIC_HOOK.get_or_init(|| {
        let default_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if let Some(location) = info.location() {
                tracing::error!(
                    target: "panic",
                    file = location.file(),
                    line = location.line(),
                    message = %info
                );
            } else {
                tracing::error!(target: "panic", message = %info);
            }
            default_hook(info);
        }));
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Days;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn rotates_when_forced() {
        let temp = tempdir().unwrap();
        let dir = temp.path().to_path_buf();
        let mut writer = DailyRotatingWriter::new(dir.clone()).unwrap();

        writer.write_all(b"first line\n").unwrap();
        writer.flush().unwrap();

        {
            let mut state = writer.inner.state.lock();
            state.current_day = state.current_day - Days::new(1);
        }

        writer.write_all(b"second line\n").unwrap();
        writer.flush().unwrap();

        let entries: Vec<_> = fs::read_dir(dir).unwrap().collect();
        assert!(entries.len() >= 2, "expected rotated log file to exist");
    }
}
