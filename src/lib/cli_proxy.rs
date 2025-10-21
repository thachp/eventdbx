use std::{
    env,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
};

use anyhow::{Context, Result};
use capnp::message::ReaderOptions;
use capnp::serialize::{OwnedSegments, write_message_to_words};
use capnp_futures::serialize::{read_message, try_read_message};
use futures::AsyncWriteExt;
use tokio::{
    net::{TcpListener, TcpStream},
    process::Command,
    task::JoinHandle,
};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::{debug, info, warn};

use crate::cli_capnp::{cli_request, cli_response};

const CLI_SERVER_ADDR: &str = "0.0.0.0:6393";

#[derive(Debug, Clone)]
pub struct CliCommandResult {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
}

pub async fn start(config_path: Arc<PathBuf>) -> Result<JoinHandle<()>> {
    let listener = TcpListener::bind(CLI_SERVER_ADDR)
        .await
        .context("failed to bind CLI Cap'n Proto listener")?;
    info!("CLI Cap'n Proto server listening on {}", CLI_SERVER_ADDR);

    let handle = tokio::spawn(async move {
        if let Err(err) = serve(listener, config_path).await {
            warn!("CLI proxy server terminated: {err:?}");
        }
    });
    Ok(handle)
}

async fn serve(listener: TcpListener, config_path: Arc<PathBuf>) -> Result<()> {
    loop {
        let (stream, peer) = listener
            .accept()
            .await
            .context("failed to accept CLI proxy connection")?;
        let config_path = Arc::clone(&config_path);
        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, config_path).await {
                warn!(target: "cli_proxy", peer = %peer, "CLI proxy connection error: {err:?}");
            }
        });
    }
}

async fn handle_connection(stream: TcpStream, config_path: Arc<PathBuf>) -> Result<()> {
    let (reader, writer) = stream.into_split();
    let mut reader = reader.compat();
    let mut writer = writer.compat_write();

    loop {
        let message = match try_read_message(&mut reader, ReaderOptions::new()).await {
            Ok(Some(message)) => message,
            Ok(None) => break,
            Err(err) => {
                return Err(anyhow::Error::new(err).context("failed to read CLI request"));
            }
        };

        let response_result = process_request(message, Arc::clone(&config_path)).await;

        let response_bytes = {
            let mut response_message = capnp::message::Builder::new_default();
            {
                let mut response = response_message.init_root::<cli_response::Builder>();
                match response_result {
                    Ok(result) => {
                        response.set_exit_code(result.exit_code);
                        response.set_stdout(&result.stdout);
                        response.set_stderr(&result.stderr);
                    }
                    Err(err) => {
                        response.set_exit_code(-1);
                        response.set_stdout("");
                        response.set_stderr(&err.to_string());
                    }
                }
            }
            write_message_to_words(&response_message)
        };

        writer
            .write_all(&response_bytes)
            .await
            .context("failed to write CLI response")?;
        writer
            .flush()
            .await
            .context("failed to flush CLI response")?;
    }

    Ok(())
}

async fn process_request(
    message: capnp::message::Reader<OwnedSegments>,
    config_path: Arc<PathBuf>,
) -> Result<CliCommandResult> {
    let request = message
        .get_root::<cli_request::Reader>()
        .context("failed to decode CLI request")?;
    let args = {
        let args_reader = request
            .get_args()
            .context("failed to read CLI request arguments")?;

        let mut collected = Vec::with_capacity(args_reader.len() as usize);
        for arg in args_reader.iter() {
            let value = arg.context("failed to read CLI argument")?;
            collected.push(value.to_string()?);
        }
        collected
    };

    execute_cli_command(args, &config_path).await
}

async fn execute_cli_command(args: Vec<String>, config_path: &PathBuf) -> Result<CliCommandResult> {
    let exe = resolve_cli_executable().context("failed to resolve CLI executable")?;

    let final_args = augment_args_with_config(args, config_path);

    let mut command = Command::new(exe);
    command.args(&final_args);
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());

    let output = command
        .output()
        .await
        .context("failed to execute CLI command")?;

    let exit_code = output
        .status
        .code()
        .unwrap_or_else(|| if output.status.success() { 0 } else { -1 });

    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();

    debug!(
        target: "cli_proxy",
        args = ?final_args,
        exit_code,
        "CLI command executed"
    );

    Ok(CliCommandResult {
        exit_code,
        stdout,
        stderr,
    })
}

fn resolve_cli_executable() -> Result<PathBuf> {
    if let Ok(path) = env::var("EVENTDBX_CLI") {
        return Ok(PathBuf::from(path));
    }
    if let Ok(path) = env::var("CARGO_BIN_EXE_eventdbx") {
        return Ok(PathBuf::from(path));
    }
    let current = std::env::current_exe().context("failed to resolve current executable")?;
    if let Some(dir) = current.parent() {
        if let Some(candidate) = probe_dir_for_cli(dir) {
            return Ok(candidate);
        }
        if let Some(parent) = dir.parent() {
            if let Some(candidate) = probe_dir_for_cli(parent) {
                return Ok(candidate);
            }
        }
    }
    Ok(current)
}

fn probe_dir_for_cli(dir: &Path) -> Option<PathBuf> {
    let unix_path = dir.join("eventdbx");
    if unix_path.exists() {
        return Some(unix_path);
    }
    #[cfg(windows)]
    {
        let windows_path = dir.join("eventdbx.exe");
        if windows_path.exists() {
            return Some(windows_path);
        }
    }
    None
}

fn augment_args_with_config(mut args: Vec<String>, config_path: &PathBuf) -> Vec<String> {
    if has_config_arg(&args) {
        return args;
    }

    let mut final_args = Vec::with_capacity(args.len() + 2);
    final_args.push("--config".to_string());
    final_args.push(config_path.to_string_lossy().into_owned());
    final_args.extend(args.drain(..));
    final_args
}

fn has_config_arg(args: &[String]) -> bool {
    args.iter()
        .any(|arg| arg == "--config" || arg.starts_with("--config="))
}

pub async fn invoke(args: &[String]) -> Result<CliCommandResult> {
    let stream = TcpStream::connect(CLI_SERVER_ADDR)
        .await
        .context("failed to connect to CLI proxy")?;
    let (reader, writer) = stream.into_split();
    let mut writer = writer.compat_write();
    let message_bytes = {
        let mut message = capnp::message::Builder::new_default();
        {
            let mut request = message.init_root::<cli_request::Builder>();
            let mut list = request.init_args(args.len() as u32);
            for (idx, arg) in args.iter().enumerate() {
                list.set(idx as u32, arg);
            }
        }
        write_message_to_words(&message)
    };

    writer
        .write_all(&message_bytes)
        .await
        .context("failed to send CLI request")?;
    writer
        .flush()
        .await
        .context("failed to flush CLI request")?;

    let mut reader = reader.compat();
    let response_message = read_message(&mut reader, ReaderOptions::new())
        .await
        .context("failed to read CLI response")?;
    let response = response_message
        .get_root::<cli_response::Reader>()
        .context("failed to decode CLI response")?;

    let stdout = response
        .get_stdout()
        .context("missing stdout field in CLI response")?
        .to_string()
        .map_err(|err| anyhow::Error::new(err).context("invalid UTF-8 in CLI stdout"))?;
    let stderr = response
        .get_stderr()
        .context("missing stderr field in CLI response")?
        .to_string()
        .map_err(|err| anyhow::Error::new(err).context("invalid UTF-8 in CLI stderr"))?;

    Ok(CliCommandResult {
        exit_code: response.get_exit_code(),
        stdout,
        stderr,
    })
}
