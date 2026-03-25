use std::fmt::Write as _;

use anyhow::{Context, Result};
use assert_cmd::cargo::cargo_bin_cmd;
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
        path: &["token", "bootstrap"],
        expected_snippet: "Issue a CLI bootstrap token",
    },
    HelpCase {
        path: &["schema"],
        expected_snippet: "Manage schemas",
    },
    HelpCase {
        path: &["schema", "validate"],
        expected_snippet: "Validate the local schema.dbx file",
    },
    HelpCase {
        path: &["schema", "show"],
        expected_snippet: "Show compiled runtime JSON from the local schema.dbx file",
    },
    HelpCase {
        path: &["schema", "apply"],
        expected_snippet: "Apply the local schema.dbx file to the active runtime schema store",
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
        path: &["aggregate", "verify"],
        expected_snippet: "Verify an aggregate's Merkle root",
    },
];

#[test]
fn help_output_matches_supported_surface() -> Result<()> {
    let temp = tempdir().context("failed to create temp dir")?;
    let home = temp.path().join("home");
    std::fs::create_dir_all(&home).context("failed to create HOME")?;

    let mut failures = String::new();

    for case in HELP_CASES {
        let mut cmd = cargo_bin_cmd!("dbx");
        cmd.env("HOME", &home);
        cmd.env("DBX_NO_UPGRADE_CHECK", "1");
        cmd.args(case.path);
        cmd.arg("--help");

        let output = cmd
            .output()
            .with_context(|| format!("failed to execute dbx {} --help", case.path.join(" ")))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        if !output.status.success() || !stdout.contains(case.expected_snippet) {
            let _ = writeln!(
                failures,
                "case {:?} missing {:?}\nstatus={}\nstdout:\n{}\nstderr:\n{}\n",
                case.path,
                case.expected_snippet,
                output.status,
                stdout,
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    if failures.is_empty() {
        return Ok(());
    }

    anyhow::bail!("help regressions detected:\n{}", failures);
}

#[test]
fn removed_commands_no_longer_appear_in_root_help() -> Result<()> {
    let temp = tempdir().context("failed to create temp dir")?;
    let home = temp.path().join("home");
    std::fs::create_dir_all(&home).context("failed to create HOME")?;

    let mut cmd = cargo_bin_cmd!("dbx");
    cmd.env("HOME", &home);
    cmd.env("DBX_NO_UPGRADE_CHECK", "1");
    cmd.arg("--help");
    let output = cmd.output().context("failed to execute dbx --help")?;
    let stdout = String::from_utf8_lossy(&output.stdout);

    for removed in [
        "checkout",
        "merge",
        "push",
        "pull",
        "watch",
        "plugin",
        "queue",
        "tenant",
        "snapshots",
        "upgrade",
        "backup",
        "restore",
    ] {
        assert!(
            !stdout.contains(removed),
            "root help unexpectedly contains removed command {removed}:\n{stdout}"
        );
    }

    Ok(())
}
