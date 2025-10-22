use std::process::Command;

use anyhow::{Context, Result, bail};
use clap::Args;

const REPO_BASE: &str = "https://github.com/thachp/eventdbx";
const INSTALLER_SH: &str = "eventdbx-installer.sh";
const INSTALLER_PS1: &str = "eventdbx-installer.ps1";

#[derive(Args, Debug)]
pub struct UpgradeArgs {
    /// Release tag (e.g. v1.12.4) or 'latest'
    #[arg(value_name = "VERSION", default_value = "latest")]
    pub version: String,
    /// Print the installer command without executing it
    #[arg(long, default_value_t = false)]
    pub print_only: bool,
}

pub fn execute(args: UpgradeArgs) -> Result<()> {
    let target = normalize_target(&args.version);
    let (path_segment, display_version) = match &target {
        Target::Latest => ("latest/download".to_string(), "latest".to_string()),
        Target::Tag(tag) => (format!("download/{}", tag), tag.clone()),
    };

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

pub fn try_handle_shortcut(raw: &[String]) -> Result<bool> {
    if raw.is_empty() {
        return Ok(false);
    }

    let command = &raw[0];
    if let Some(version) = command.strip_prefix("upgrade@") {
        if raw.len() > 1 {
            bail!("unexpected arguments after '{command}'");
        }
        execute(UpgradeArgs {
            version: version.to_string(),
            print_only: false,
        })?;
        return Ok(true);
    }

    Ok(false)
}

enum Target {
    Latest,
    Tag(String),
}

fn normalize_target(input: &str) -> Target {
    let trimmed = input.trim();
    let without_prefix = trimmed.strip_prefix('@').unwrap_or(trimmed).trim();
    if without_prefix.is_empty() || without_prefix.eq_ignore_ascii_case("latest") {
        Target::Latest
    } else {
        let tag = if without_prefix.starts_with('v') {
            without_prefix.to_string()
        } else {
            format!("v{}", without_prefix)
        };
        Target::Tag(tag)
    }
}
