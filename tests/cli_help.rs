use std::fmt::Write as _;

use anyhow::{Context, Result};
use assert_cmd::Command;
use tempfile::tempdir;

struct HelpCase {
    path: &'static [&'static str],
    expected_snippet: &'static str,
}

const HELP_CASES: &[HelpCase] = &[
    HelpCase {
        path: &[],
        expected_snippet: "EventDBX server CLI",
    },
    HelpCase {
        path: &["start"],
        expected_snippet: "Start the EventDBX server",
    },
    HelpCase {
        path: &["stop"],
        expected_snippet: "Stop the EventDBX server",
    },
    HelpCase {
        path: &["status"],
        expected_snippet: "Display EventDBX server status",
    },
    HelpCase {
        path: &["restart"],
        expected_snippet: "Restart the EventDBX server",
    },
    HelpCase {
        path: &["destroy"],
        expected_snippet: "Destroy all EventDBX data and configuration",
    },
    HelpCase {
        path: &["config"],
        expected_snippet: "Update system configuration",
    },
    HelpCase {
        path: &["token"],
        expected_snippet: "Manage access tokens",
    },
    HelpCase {
        path: &["token", "generate"],
        expected_snippet: "Generate a new token",
    },
    HelpCase {
        path: &["token", "list"],
        expected_snippet: "List configured tokens",
    },
    HelpCase {
        path: &["token", "revoke"],
        expected_snippet: "Revoke an active token",
    },
    HelpCase {
        path: &["token", "refresh"],
        expected_snippet: "Refresh an existing token",
    },
    HelpCase {
        path: &["schema"],
        expected_snippet: "Manage schemas",
    },
    HelpCase {
        path: &["schema", "create"],
        expected_snippet: "Create a new schema definition",
    },
    HelpCase {
        path: &["schema", "add"],
        expected_snippet: "Add events to an existing schema definition",
    },
    HelpCase {
        path: &["schema", "remove"],
        expected_snippet: "Remove an event definition from an aggregate",
    },
    HelpCase {
        path: &["schema", "annotate"],
        expected_snippet: "Set or clear the default note for an event",
    },
    HelpCase {
        path: &["schema", "list"],
        expected_snippet: "List available schemas",
    },
    HelpCase {
        path: &["schema", "hide"],
        expected_snippet: "Hide a field from aggregate detail responses",
    },
    HelpCase {
        path: &["schema", "validate"],
        expected_snippet: "Validate payload against a schema definition",
    },
    HelpCase {
        path: &["plugin"],
        expected_snippet: "Manage plugins",
    },
    HelpCase {
        path: &["plugin", "install"],
        expected_snippet: "Install a plugin binary into the local registry",
    },
    HelpCase {
        path: &["plugin", "config"],
        expected_snippet: "Configure plugins",
    },
    HelpCase {
        path: &["plugin", "config", "tcp"],
        expected_snippet: "Configure the TCP plugin",
    },
    HelpCase {
        path: &["plugin", "config", "http"],
        expected_snippet: "Configure the HTTP plugin",
    },
    HelpCase {
        path: &["plugin", "config", "log"],
        expected_snippet: "Configure the logging plugin",
    },
    HelpCase {
        path: &["plugin", "config", "process"],
        expected_snippet: "Configure a subprocess plugin installed via the registry",
    },
    HelpCase {
        path: &["plugin", "enable"],
        expected_snippet: "Enable a configured plugin",
    },
    HelpCase {
        path: &["plugin", "disable"],
        expected_snippet: "Disable a configured plugin",
    },
    HelpCase {
        path: &["plugin", "remove"],
        expected_snippet: "Remove a configured plugin",
    },
    HelpCase {
        path: &["plugin", "replay"],
        expected_snippet: "Replay stored events through a plugin",
    },
    HelpCase {
        path: &["plugin", "test"],
        expected_snippet: "Send a sample event to all enabled plugins",
    },
    HelpCase {
        path: &["plugin", "list"],
        expected_snippet: "List enabled plugins",
    },
    HelpCase {
        path: &["queue"],
        expected_snippet: "Show or manage the plugin retry queue",
    },
    HelpCase {
        path: &["queue", "clear"],
        expected_snippet: "Remove all dead entries from the queue",
    },
    HelpCase {
        path: &["queue", "retry"],
        expected_snippet: "Retry dead entries, optionally filtering by job id",
    },
    HelpCase {
        path: &["events"],
        expected_snippet: "List events in the store or inspect a specific event",
    },
    HelpCase {
        path: &["events"],
        expected_snippet: "-e, --event <EVENT_ID>",
    },
    HelpCase {
        path: &["aggregate"],
        expected_snippet: "Manage aggregates",
    },
    HelpCase {
        path: &["aggregate", "create"],
        expected_snippet: "Create a new aggregate instance",
    },
    HelpCase {
        path: &["aggregate", "apply"],
        expected_snippet: "Apply an event to an aggregate instance",
    },
    HelpCase {
        path: &["aggregate", "list"],
        expected_snippet: "List aggregates in the store",
    },
    HelpCase {
        path: &["aggregate", "get"],
        expected_snippet: "Retrieve the state of an aggregate",
    },
    HelpCase {
        path: &["aggregate", "replay"],
        expected_snippet: "Replay events for an aggregate instance",
    },
    HelpCase {
        path: &["aggregate", "verify"],
        expected_snippet: "Verify an aggregate's Merkle root",
    },
    HelpCase {
        path: &["aggregate", "snapshot"],
        expected_snippet: "Create a snapshot of the aggregate state",
    },
    HelpCase {
        path: &["aggregate", "archive"],
        expected_snippet: "Archive an aggregate instance",
    },
    HelpCase {
        path: &["aggregate", "restore"],
        expected_snippet: "Restore an archived aggregate instance",
    },
    HelpCase {
        path: &["aggregate", "remove"],
        expected_snippet: "Remove an aggregate that has no events",
    },
    HelpCase {
        path: &["aggregate", "commit"],
        expected_snippet: "Commit events previously staged with `aggregate apply --stage`",
    },
    HelpCase {
        path: &["aggregate", "export"],
        expected_snippet: "Export aggregate state to CSV or JSON",
    },
    HelpCase {
        path: &["upgrade"],
        expected_snippet: "Upgrade or switch the EventDBX CLI binary",
    },
    HelpCase {
        path: &["backup"],
        expected_snippet: "Create a backup archive containing all EventDBX data",
    },
    HelpCase {
        path: &["restore"],
        expected_snippet: "Restore EventDBX data from a backup archive",
    },
];

#[test]
fn cli_help_regressions() -> Result<()> {
    for case in HELP_CASES {
        let stdout = run_help(case.path)
            .with_context(|| format!("command {:?} --help failed", case.path))?;
        assert!(
            stdout.contains(case.expected_snippet),
            "expected help for {:?} to contain {:?}\nstdout:\n{}",
            case.path,
            case.expected_snippet,
            indent_output(&stdout)
        );
    }
    Ok(())
}

fn run_help(path: &[&str]) -> Result<String> {
    let temp_log = tempdir()?;
    let mut cmd = Command::cargo_bin("dbx")?;
    cmd.args(path);
    cmd.arg("--help");
    cmd.env("EVENTDBX_LOG_DIR", temp_log.path());
    cmd.env("DBX_NO_UPGRADE_CHECK", "1");
    let output = cmd.output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "dbx {:?} --help exited with {}: {}",
            path,
            output.status,
            stderr
        );
    }
    let stdout = String::from_utf8(output.stdout)?.replace("\r\n", "\n");
    Ok(stdout)
}

fn indent_output(output: &str) -> String {
    let mut indented = String::new();
    for line in output.lines() {
        let _ = writeln!(&mut indented, "    {}", line);
    }
    indented
}
