use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use assert_cmd::Command;
use serde_json::{Value, json};
use tempfile::TempDir;

use chrono::Utc;
use eventdbx::{plugin::queue::PluginQueueStore, snowflake::SnowflakeId};

struct CliTest {
    _tmp: TempDir,
    home: PathBuf,
    log_dir: PathBuf,
    config_home: PathBuf,
}

struct FailureOutput {
    stdout: String,
    stderr: String,
}

macro_rules! cli_failure_test {
    ($name:ident, [$($arg:expr),+ $(,)?], $pattern:expr) => {
        #[test]
        fn $name() -> Result<()> {
            let cli = CliTest::new()?;
            let failure = cli.run_failure(&[$($arg),+])?;
            assert!(
                failure.stderr.contains($pattern),
                "expected stderr to contain {:?}\nstdout:\n{}\nstderr:\n{}",
                $pattern,
                failure.stdout,
                failure.stderr
            );
            Ok(())
        }
    };
}

macro_rules! cli_success_contains_test {
    ($name:ident, [$($arg:expr),+ $(,)?], $pattern:expr) => {
        #[test]
        fn $name() -> Result<()> {
            let cli = CliTest::new()?;
            let stdout = cli.run(&[$($arg),+])?;
            assert!(
                stdout.contains($pattern),
                "expected stdout to contain {:?}, got:\n{}",
                $pattern,
                stdout
            );
            Ok(())
        }
    };
}

impl CliTest {
    fn new() -> Result<Self> {
        let tmp = tempfile::tempdir().context("failed to create temp dir")?;
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).context("failed to create temporary home directory")?;
        let log_dir = home.join("logs");
        fs::create_dir_all(&log_dir).context("failed to create temporary log directory")?;
        let config_home = home.join(".config");
        fs::create_dir_all(&config_home).context("failed to create temporary config directory")?;
        Ok(Self {
            _tmp: tmp,
            home,
            log_dir,
            config_home,
        })
    }

    fn command(&self) -> Result<Command> {
        let mut cmd = Command::cargo_bin("dbx")?;
        cmd.env("HOME", &self.home);
        cmd.env("EVENTDBX_LOG_DIR", &self.log_dir);
        cmd.env("DBX_NO_UPGRADE_CHECK", "1");
        cmd.env("XDG_CONFIG_HOME", &self.config_home);
        Ok(cmd)
    }

    fn run(&self, args: &[&str]) -> Result<String> {
        let output = self.exec(args)?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("dbx {:?} exited with {}: {}", args, output.status, stderr);
        }
        Ok(normalize_output(output.stdout)?)
    }

    fn run_json(&self, args: &[&str]) -> Result<Value> {
        let stdout = self.run(args)?;
        let parsed = serde_json::from_str(stdout.trim())
            .with_context(|| format!("failed to parse JSON output from dbx {:?}", args))?;
        Ok(parsed)
    }

    fn create_aggregate(&self, aggregate: &str, aggregate_id: &str) -> Result<()> {
        self.run(&["aggregate", "create", aggregate, aggregate_id])?;
        Ok(())
    }

    fn run_failure(&self, args: &[&str]) -> Result<FailureOutput> {
        let output = self.exec(args)?;
        if output.status.success() {
            anyhow::bail!("expected dbx {:?} to fail but it succeeded", args);
        }
        Ok(FailureOutput {
            stdout: normalize_output(output.stdout)?,
            stderr: normalize_output(output.stderr)?,
        })
    }

    fn exec(&self, args: &[&str]) -> Result<std::process::Output> {
        let mut cmd = self.command()?;
        cmd.args(args);
        cmd.output()
            .with_context(|| format!("failed to run dbx {:?}", args))
    }

    fn run_with_input(&self, args: &[&str], input: &str) -> Result<String> {
        let mut cmd = self.command()?;
        cmd.args(args);
        let output = cmd
            .write_stdin(input)
            .output()
            .with_context(|| format!("failed to run dbx {:?}", args))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("dbx {:?} exited with {}: {}", args, output.status, stderr);
        }
        Ok(normalize_output(output.stdout)?)
    }

    fn data_dir(&self) -> PathBuf {
        self.home.join(".eventdbx")
    }
}

#[test]
fn token_list_empty_prints_hint() -> Result<()> {
    let cli = CliTest::new()?;
    let stdout = cli.run(&["token", "list"])?;
    assert!(
        stdout.contains("no issued tokens"),
        "unexpected token list output:\n{}",
        stdout
    );
    Ok(())
}

#[test]
fn token_generate_and_list_json_round_trip() -> Result<()> {
    let cli = CliTest::new()?;
    let stdout = cli.run(&[
        "token", "generate", "--group", "ops", "--user", "alice", "--root", "--json",
    ])?;
    let record: Value =
        serde_json::from_str(stdout.trim()).context("failed to parse token generate output")?;
    assert_eq!(record["group"], "ops");
    assert_eq!(record["user"], "alice");
    assert_eq!(record["root"], json!(true));

    let stdout = cli.run(&["token", "list", "--json"])?;
    let records: Value =
        serde_json::from_str(stdout.trim()).context("failed to parse token list output")?;
    let Some(array) = records.as_array() else {
        anyhow::bail!("expected token list JSON to be an array, got {}", records);
    };
    assert_eq!(
        array.len(),
        1,
        "expected one token record, got {}",
        array.len()
    );
    assert_eq!(array[0]["group"], "ops");
    assert_eq!(array[0]["user"], "alice");

    Ok(())
}

#[test]
fn token_generate_requires_action_for_non_root() -> Result<()> {
    let cli = CliTest::new()?;
    let failure = cli.run_failure(&["token", "generate", "--group", "ops", "--user", "alice"])?;
    assert!(
        failure
            .stderr
            .contains("at least one --action must be provided for non-root tokens"),
        "unexpected validation message:\n{}",
        failure.stderr
    );
    Ok(())
}

#[test]
fn token_refresh_replaces_token() -> Result<()> {
    let cli = CliTest::new()?;
    let original = cli.run_json(&[
        "token", "generate", "--group", "ops", "--user", "alice", "--root", "--json",
    ])?;
    let original_token = original["token"]
        .as_str()
        .context("missing token field from token generate")?
        .to_string();

    let refreshed = cli.run_json(&["token", "refresh", "--token", &original_token, "--json"])?;
    let new_token = refreshed["token"]
        .as_str()
        .context("missing token field from refresh response")?;
    assert_ne!(
        new_token, original_token,
        "refresh should issue a replacement token"
    );

    let list = cli.run_json(&["token", "list", "--json"])?;
    let array = list
        .as_array()
        .context("token list did not produce array after refresh")?;
    assert_eq!(
        array.len(),
        2,
        "expected refreshed token plus revoked original"
    );
    let mut active_found = false;
    let mut revoked_found = false;
    for record in array {
        match record["status"].as_str() {
            Some("active") => {
                active_found = true;
                assert_eq!(record["token"], json!(new_token));
            }
            Some("revoked") => {
                revoked_found = true;
                assert_eq!(record["token"], json!(original_token));
            }
            _ => {}
        }
    }
    assert!(active_found, "missing active refreshed token entry");
    assert!(revoked_found, "missing revoked original token entry");

    Ok(())
}

cli_failure_test!(
    token_refresh_unknown_token_fails,
    ["token", "refresh", "--token", "unknown"],
    "token not found or inactive"
);

cli_failure_test!(
    token_refresh_missing_token_argument,
    ["token", "refresh", "--json"],
    "the following required arguments were not provided"
);

#[test]
fn token_revoke_unknown_token_fails() -> Result<()> {
    let cli = CliTest::new()?;
    let failure = cli.run_failure(&["token", "revoke", "missing-token"])?;
    assert!(
        failure.stderr.contains("token not found or inactive"),
        "unexpected revoke failure message:\n{}",
        failure.stderr
    );
    Ok(())
}

#[test]
fn plugin_list_empty_reports_none() -> Result<()> {
    let cli = CliTest::new()?;
    let stdout = cli.run(&["plugin", "list"])?;
    assert_eq!(stdout.trim(), "(no plugins configured)");

    let stdout = cli.run(&["plugin", "list", "--json"])?;
    let plugins: Value =
        serde_json::from_str(stdout.trim()).context("failed to parse plugin list JSON")?;
    assert_eq!(plugins, json!([]));

    Ok(())
}

#[test]
fn queue_status_empty() -> Result<()> {
    let cli = CliTest::new()?;
    let stdout = cli.run(&["queue"])?;
    assert_eq!(stdout.trim(), "pending=0 processing=0 done=0 dead=0");
    Ok(())
}

#[test]
fn queue_retry_without_dead_events() -> Result<()> {
    let cli = CliTest::new()?;
    let stdout = cli.run(&["queue", "retry"])?;
    assert!(
        stdout.contains("no dead plugin jobs to retry"),
        "unexpected queue retry output:\n{}",
        stdout
    );
    Ok(())
}

cli_success_contains_test!(
    queue_clear_no_dead_events,
    ["queue", "clear"],
    "no dead plugin jobs to clear"
);

cli_success_contains_test!(
    queue_retry_specific_event_no_match,
    ["queue", "retry", "--event-id", "1"],
    "no dead plugin jobs match the provided id"
);

#[test]
fn queue_clear_prompts_and_respects_confirmation() -> Result<()> {
    let cli = CliTest::new()?;
    {
        let queue_path = cli.data_dir().join("plugin_queue.db");
        let store = PluginQueueStore::open(queue_path.as_path()).map_err(anyhow::Error::from)?;
        let job = store
            .enqueue_job(
                "test",
                serde_json::json!({"event_id": SnowflakeId::from_u64(42).to_string()}),
                Utc::now().timestamp_millis(),
            )
            .map_err(anyhow::Error::from)?;
        let dispatched = store
            .dispatch_jobs("test", 1, Utc::now().timestamp_millis())
            .map_err(anyhow::Error::from)?;
        assert_eq!(dispatched.len(), 1);
        store
            .fail_job(job.id, "boom".into(), Utc::now().timestamp_millis(), 0)
            .map_err(anyhow::Error::from)?;
    }

    let cancelled = cli.run_with_input(&["queue", "clear"], "\n")?;
    assert!(
        cancelled.contains("clear cancelled"),
        "expected clear cancellation message, got:\n{}",
        cancelled
    );

    let cleared = cli.run_with_input(&["queue", "clear"], "clear\n")?;
    assert!(
        cleared.contains("cleared 1 dead job(s) from the plugin queue"),
        "expected clear confirmation, got:\n{}",
        cleared
    );
    Ok(())
}

#[test]
fn queue_status_displays_dead_events() -> Result<()> {
    let cli = CliTest::new()?;
    {
        let queue_path = cli.data_dir().join("plugin_queue.db");
        let store = PluginQueueStore::open(queue_path.as_path()).map_err(anyhow::Error::from)?;
        store
            .enqueue_job(
                "alpha",
                serde_json::json!({"event_id": SnowflakeId::from_u64(99).to_string()}),
                Utc::now().timestamp_millis(),
            )
            .map_err(anyhow::Error::from)?;
        let job = store
            .enqueue_job(
                "alpha",
                serde_json::json!({"event_id": SnowflakeId::from_u64(100).to_string()}),
                Utc::now().timestamp_millis(),
            )
            .map_err(anyhow::Error::from)?;
        let dispatched = store
            .dispatch_jobs("alpha", 1, Utc::now().timestamp_millis())
            .map_err(anyhow::Error::from)?;
        assert_eq!(dispatched.len(), 1);
        store
            .fail_job(job.id, "error".into(), Utc::now().timestamp_millis(), 0)
            .map_err(anyhow::Error::from)?;
    }

    let status_output = cli.run(&["queue"])?;
    assert!(
        status_output.contains("dead=1"),
        "unexpected queue status:\n{}",
        status_output
    );
    assert!(
        status_output.contains("status=\"dead\""),
        "expected dead job details in output:\n{}",
        status_output
    );
    Ok(())
}

#[test]
fn schema_list_empty_json() -> Result<()> {
    let cli = CliTest::new()?;
    let stdout = cli.run(&["schema", "list", "--json"])?;
    let schemas: Value =
        serde_json::from_str(stdout.trim()).context("failed to parse schema list JSON")?;
    assert_eq!(schemas, json!([]));
    Ok(())
}

#[test]
fn schema_add_remove_annotate_flow() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run_json(&[
        "schema",
        "create",
        "inventory",
        "--events",
        "item_created",
        "--json",
    ])?;

    let add_out = cli.run(&["schema", "add", "inventory", "item_restocked"])?;
    assert!(
        add_out.contains("schema=inventory added_events=item_restocked total_events=2"),
        "unexpected schema add output:\n{}",
        add_out
    );

    let schema_after_add = cli.run_json(&["schema", "inventory"])?;
    assert!(
        schema_after_add["events"]
            .get("item_restocked")
            .and_then(|event| event.get("fields"))
            .is_some(),
        "expected schema to contain item_restocked event: {}",
        schema_after_add
    );

    let annotate_out = cli.run(&[
        "schema",
        "annotate",
        "inventory",
        "item_created",
        "--note",
        "Initial load event",
    ])?;
    assert!(
        annotate_out.contains("note_set=\"Initial load event\""),
        "unexpected schema annotate output:\n{}",
        annotate_out
    );

    let schema_with_note = cli.run_json(&["schema", "inventory"])?;
    assert_eq!(
        schema_with_note["events"]["item_created"]["notes"],
        json!("Initial load event")
    );

    let clear_out = cli.run(&["schema", "annotate", "inventory", "item_created", "--clear"])?;
    assert!(
        clear_out.contains("note_cleared"),
        "unexpected schema annotate clear output:\n{}",
        clear_out
    );

    let schema_cleared = cli.run_json(&["schema", "inventory"])?;
    assert!(
        schema_cleared["events"]["item_created"]
            .get("notes")
            .is_none(),
        "expected note to be cleared from schema: {}",
        schema_cleared
    );

    let remove_out = cli.run(&["schema", "remove", "inventory", "item_restocked"])?;
    assert!(
        remove_out.contains("schema=inventory removed_event=item_restocked remaining_events=1"),
        "unexpected schema remove output:\n{}",
        remove_out
    );

    Ok(())
}

#[test]
fn schema_hide_field() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run_json(&[
        "schema",
        "create",
        "orders",
        "--events",
        "order_created",
        "--json",
    ])?;

    let hide_out = cli.run(&[
        "schema",
        "hide",
        "--aggregate",
        "orders",
        "--field",
        "internal_notes",
    ])?;
    assert!(
        hide_out.contains("aggregate=orders field=internal_notes hidden"),
        "unexpected schema hide output:\n{}",
        hide_out
    );

    let schema_after_hide = cli.run_json(&["schema", "orders"])?;
    let hidden = schema_after_hide["hidden_fields"]
        .as_array()
        .context("hidden_fields not present in schema json")?;
    assert!(
        hidden.iter().any(|value| value == "internal_notes"),
        "expected hidden_fields to include internal_notes: {}",
        schema_after_hide
    );

    Ok(())
}

#[test]
fn schema_create_duplicate_fails() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run(&["schema", "create", "dupe", "--events", "event_a"])?;
    let failure = cli.run_failure(&["schema", "create", "dupe", "--events", "event_b"])?;
    assert!(
        failure.stderr.contains("schema already exists"),
        "unexpected duplicate schema message:\n{}",
        failure.stderr
    );
    Ok(())
}

#[test]
fn schema_list_after_create_contains_entry() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run(&[
        "schema",
        "create",
        "customers",
        "--events",
        "customer_created",
    ])?;
    let list = cli.run_json(&["schema", "list", "--json"])?;
    let array = list
        .as_array()
        .context("schema list did not return an array")?;
    assert_eq!(array.len(), 1);
    assert_eq!(array[0]["aggregate"], json!("customers"));
    Ok(())
}

#[test]
fn schema_remove_unknown_event_fails() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run(&["schema", "create", "inventory", "--events", "item_created"])?;
    let failure = cli.run_failure(&["schema", "remove", "inventory", "item_deleted"])?;
    assert!(
        failure
            .stderr
            .contains("event item_deleted is not defined for aggregate inventory"),
        "unexpected remove failure:\n{}",
        failure.stderr
    );
    Ok(())
}

#[test]
fn schema_validate_unknown_event_fails() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run(&["schema", "create", "prices", "--events", "price_created"])?;
    let failure = cli.run_failure(&[
        "schema",
        "validate",
        "--aggregate",
        "prices",
        "--event",
        "price_updated",
        "--payload",
        "{}",
    ])?;
    assert!(
        failure
            .stderr
            .contains("event price_updated is not defined for aggregate prices"),
        "unexpected schema validate failure:\n{}",
        failure.stderr
    );
    Ok(())
}

#[test]
fn schema_validate_invalid_json_fails() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run(&["schema", "create", "audit", "--events", "log_created"])?;
    let failure = cli.run_failure(&[
        "schema",
        "validate",
        "--aggregate",
        "audit",
        "--event",
        "log_created",
        "--payload",
        "{invalid",
    ])?;
    assert!(
        failure.stderr.contains("key must be a string")
            || failure.stderr.contains("expected value"),
        "unexpected invalid JSON message:\n{}",
        failure.stderr
    );
    Ok(())
}

#[test]
fn schema_hide_empty_field_fails() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run(&["schema", "create", "orders", "--events", "order_created"])?;
    let failure = cli.run_failure(&["schema", "hide", "--aggregate", "orders", "--field", ""])?;
    assert!(
        failure.stderr.contains("field name cannot be empty"),
        "unexpected hide validation message:\n{}",
        failure.stderr
    );
    Ok(())
}

#[test]
fn schema_create_empty_aggregate_fails() -> Result<()> {
    let cli = CliTest::new()?;
    let failure = cli.run_failure(&["schema", "create", "", "--events", "evt"])?;
    assert!(
        failure.stderr.contains("aggregate name must be provided"),
        "unexpected empty aggregate message:\n{}",
        failure.stderr
    );
    Ok(())
}

#[test]
fn schema_annotate_requires_note_or_clear() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run_json(&[
        "schema",
        "create",
        "ledger",
        "--events",
        "entry_logged",
        "--json",
    ])?;

    let failure = cli.run_failure(&["schema", "annotate", "ledger", "entry_logged"])?;
    assert!(
        failure
            .stderr
            .contains("either --note must be provided or use --clear"),
        "unexpected annotate validation message:\n{}",
        failure.stderr
    );

    Ok(())
}

#[test]
fn schema_annotate_clear_with_note_fails() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run_json(&[
        "schema",
        "create",
        "ledger",
        "--events",
        "entry_logged",
        "--json",
    ])?;
    let failure = cli.run_failure(&[
        "schema",
        "annotate",
        "ledger",
        "entry_logged",
        "--note",
        "example",
        "--clear",
    ])?;
    assert!(
        failure
            .stderr
            .contains("--note cannot be combined with --clear"),
        "unexpected annotate clear message:\n{}",
        failure.stderr
    );
    Ok(())
}

#[test]
fn schema_hide_field_idempotent_and_error_cases() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run_json(&[
        "schema",
        "create",
        "billing",
        "--events",
        "invoice_created",
        "--json",
    ])?;

    let hide_again = cli.run(&[
        "schema",
        "hide",
        "--aggregate",
        "billing",
        "--field",
        "secret_code",
    ])?;
    assert!(
        hide_again.contains("aggregate=billing field=secret_code hidden"),
        "unexpected schema hide output:\n{}",
        hide_again
    );

    let schema_hidden = cli.run_json(&["schema", "billing"])?;
    let hidden = schema_hidden["hidden_fields"]
        .as_array()
        .context("hidden_fields not present after hide")?;
    assert!(
        hidden.iter().any(|value| value == "secret_code"),
        "expected hidden_fields to contain secret_code: {}",
        schema_hidden
    );

    let hide_duplicate = cli.run(&[
        "schema",
        "hide",
        "--aggregate",
        "billing",
        "--field",
        "secret_code",
    ])?;
    assert!(
        hide_duplicate.contains("aggregate=billing field=secret_code hidden"),
        "duplicate hide should still report hidden:\n{}",
        hide_duplicate
    );

    let failure = cli.run_failure(&[
        "schema",
        "hide",
        "--aggregate",
        "",
        "--field",
        "secret_code",
    ])?;
    assert!(
        failure.stderr.contains("aggregate name cannot be empty"),
        "expected empty aggregate error, got:\n{}",
        failure.stderr
    );

    Ok(())
}

#[test]
fn schema_show_outputs_json() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run_json(&[
        "schema",
        "create",
        "customers",
        "--events",
        "customer_created",
        "--json",
    ])?;
    let schema = cli.run_json(&["schema", "customers"])?;
    assert_eq!(schema["aggregate"], json!("customers"));
    assert!(schema["events"].get("customer_created").is_some());
    Ok(())
}

#[test]
fn remote_list_empty_json() -> Result<()> {
    let cli = CliTest::new()?;
    let stdout = cli.run(&["remote", "ls", "--json"])?;
    let remotes: Value =
        serde_json::from_str(stdout.trim()).context("failed to parse remote ls JSON")?;
    assert!(
        remotes
            .as_object()
            .map(|map| map.is_empty())
            .unwrap_or(false),
        "expected empty JSON object for remotes, got {}",
        remotes
    );
    Ok(())
}

cli_success_contains_test!(
    remote_list_empty_text,
    ["remote", "ls"],
    "(no remotes configured)"
);

cli_failure_test!(
    remote_show_unknown_fails,
    ["remote", "show", "ghost"],
    "no remote named 'ghost' is configured"
);

cli_failure_test!(
    remote_remove_unknown_fails,
    ["remote", "rm", "ghost"],
    "no remote named 'ghost' is configured"
);

#[test]
fn remote_key_shows_path() -> Result<()> {
    let cli = CliTest::new()?;
    let output = cli.run(&["remote", "key", "--show-path"])?;
    assert!(
        output.contains("path:"),
        "expected path in remote key output: {}",
        output
    );
    Ok(())
}

cli_failure_test!(
    remote_push_unknown_remote_fails,
    ["push", "ghost"],
    "no remote named 'ghost' is configured"
);

cli_failure_test!(
    remote_pull_unknown_remote_fails,
    ["pull", "ghost"],
    "no remote named 'ghost' is configured"
);

cli_failure_test!(
    remote_push_schema_only_filter_conflict,
    ["push", "ghost", "--schema-only", "--aggregate", "order"],
    "--schema-only cannot be combined with aggregate filters"
);

cli_failure_test!(
    remote_pull_schema_only_filter_conflict,
    ["pull", "ghost", "--schema-only", "--aggregate", "order"],
    "--schema-only cannot be combined with aggregate filters"
);

#[test]
fn backup_output_inside_data_dir_fails() -> Result<()> {
    let cli = CliTest::new()?;
    let output_path = cli.data_dir().join("backup.tar.gz");
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let failure =
        cli.run_failure(&["backup", "--output", output_path.to_string_lossy().as_ref()])?;
    assert!(
        failure.stderr.contains("backup output path")
            && failure.stderr.contains("resides inside the data directory"),
        "unexpected backup path failure:\n{}",
        failure.stderr
    );
    Ok(())
}

#[test]
fn backup_creates_archive() -> Result<()> {
    let cli = CliTest::new()?;
    let output_path = cli.home.join("backup.tar.gz");
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let stdout = cli.run(&["backup", "--output", output_path.to_string_lossy().as_ref()])?;
    assert!(
        stdout.contains("Backup created at"),
        "unexpected backup output:\n{}",
        stdout
    );
    assert!(output_path.exists(), "backup archive was not created");
    Ok(())
}

#[test]
fn restore_missing_archive_fails() -> Result<()> {
    let cli = CliTest::new()?;
    let missing = cli.home.join("missing.tar.gz");
    let failure = cli.run_failure(&["restore", "--input", missing.to_string_lossy().as_ref()])?;
    assert!(
        failure.stderr.contains("backup archive") && failure.stderr.contains("does not exist"),
        "unexpected restore failure:\n{}",
        failure.stderr
    );
    Ok(())
}

#[test]
fn config_prints_toml_when_no_updates() -> Result<()> {
    let cli = CliTest::new()?;
    let stdout = cli.run(&["config"])?;
    assert!(
        stdout.contains("data_dir") && stdout.contains("port"),
        "unexpected config output:\n{}",
        stdout
    );
    Ok(())
}

#[test]
fn config_updates_port_writes_file() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run(&["config", "--port", "8080"])?;
    let config_path = cli.data_dir().join("config.toml");
    let contents = fs::read_to_string(config_path)?;
    assert!(
        contents.contains("port = 8080"),
        "expected config to include updated port: {}",
        contents
    );
    Ok(())
}

#[test]
fn token_revoke_updates_status() -> Result<()> {
    let cli = CliTest::new()?;
    let record = cli.run_json(&[
        "token", "generate", "--group", "ops", "--user", "alice", "--root", "--json",
    ])?;
    let token = record["token"]
        .as_str()
        .context("token generate output missing token field")?
        .to_string();

    let revoke = cli.run_json(&["token", "revoke", &token, "--reason", "cleanup", "--json"])?;
    assert_eq!(revoke["token"], json!(token));
    assert_eq!(revoke["revoked"], json!(true));
    assert_eq!(revoke["reason"], json!("cleanup"));

    let list = cli.run_json(&["token", "list", "--json"])?;
    let array = list
        .as_array()
        .context("token list json did not return array")?;
    assert_eq!(array.len(), 1);
    assert_eq!(array[0]["status"], json!("revoked"));

    Ok(())
}

#[test]
fn plugin_http_config_enable_disable() -> Result<()> {
    let cli = CliTest::new()?;

    let output = cli.run(&[
        "plugin",
        "config",
        "http",
        "--name",
        "hook",
        "--endpoint",
        "http://127.0.0.1:9",
        "--header",
        "X-Test=ok",
        "--disable",
    ])?;
    assert!(
        output.contains("HTTP plugin 'hook' disabled"),
        "unexpected config output:\n{}",
        output
    );

    let list = cli.run(&["plugin", "list"])?;
    assert!(
        list.contains("http (hook) - disabled (enable with plugin enable hook)"),
        "unexpected plugin list output:\n{}",
        list
    );

    let failure = cli.run_failure(&["plugin", "enable", "hook"])?;
    assert!(
        failure
            .stderr
            .contains("failed to establish connection for 'hook'"),
        "unexpected enable failure message:\n{}",
        failure.stderr
    );

    let plugins = cli.run_json(&["plugin", "list", "--json"])?;
    let array = plugins
        .as_array()
        .context("plugin list json did not return array")?;
    assert_eq!(array.len(), 1);
    let plugin = &array[0];
    assert_eq!(plugin["enabled"], json!(false));
    assert_eq!(plugin["name"], json!("hook"));
    assert_eq!(plugin["config"]["type"], json!("http"));
    assert_eq!(plugin["config"]["endpoint"], json!("http://127.0.0.1:9"));
    assert_eq!(plugin["config"]["headers"], json!({ "X-Test": "ok" }));

    Ok(())
}

cli_failure_test!(
    plugin_disable_unknown_fails,
    ["plugin", "disable", "ghost"],
    "no plugin named 'ghost' is configured"
);

cli_failure_test!(
    plugin_config_process_requires_installation,
    [
        "plugin",
        "config",
        "process",
        "--name",
        "worker",
        "--plugin",
        "search",
        "--version",
        "1.2.3"
    ],
    "plugin 'search' version 1.2.3 is not installed"
);

cli_success_contains_test!(
    plugin_test_reports_empty,
    ["plugin", "test"],
    "(no plugins enabled)"
);

#[test]
fn plugin_enable_unknown_fails() -> Result<()> {
    let cli = CliTest::new()?;
    let failure = cli.run_failure(&["plugin", "enable", "ghost"])?;
    assert!(
        failure
            .stderr
            .contains("no plugin named 'ghost' is configured"),
        "unexpected enable failure message:\n{}",
        failure.stderr
    );
    Ok(())
}

#[test]
fn plugin_remove_unknown_fails() -> Result<()> {
    let cli = CliTest::new()?;
    let failure = cli.run_failure(&["plugin", "remove", "ghost"])?;
    assert!(
        failure
            .stderr
            .contains("no plugin named 'ghost' is configured"),
        "unexpected remove failure message:\n{}",
        failure.stderr
    );
    Ok(())
}

#[test]
fn plugin_config_log_sets_template() -> Result<()> {
    let cli = CliTest::new()?;
    let out = cli.run(&[
        "plugin",
        "config",
        "log",
        "--name",
        "audit",
        "--level",
        "debug",
        "--template",
        "[{aggregate}] {event}",
    ])?;
    assert!(
        out.contains("Log plugin 'audit' configured"),
        "unexpected log config output:\n{}",
        out
    );

    let list = cli.run_json(&["plugin", "list", "--json"])?;
    let array = list
        .as_array()
        .context("log plugin list did not return array")?;
    assert_eq!(array.len(), 1);
    let plugin = &array[0];
    assert_eq!(plugin["config"]["type"], json!("log"));
    assert_eq!(plugin["config"]["level"], json!("debug"));
    assert_eq!(plugin["config"]["template"], json!("[{aggregate}] {event}"));
    Ok(())
}

#[test]
fn plugin_config_tcp_sets_values() -> Result<()> {
    let cli = CliTest::new()?;
    let out = cli.run(&[
        "plugin",
        "config",
        "tcp",
        "--name",
        "sink",
        "--host",
        "127.0.0.1",
        "--port",
        "7000",
        "--disable",
    ])?;
    assert!(
        out.contains("TCP plugin 'sink' disabled"),
        "unexpected tcp config output:\n{}",
        out
    );

    let list = cli.run_json(&["plugin", "list", "--json"])?;
    let array = list
        .as_array()
        .context("tcp plugin list did not return array")?;
    assert_eq!(array.len(), 1);
    let plugin = &array[0];
    assert_eq!(plugin["config"]["type"], json!("tcp"));
    assert_eq!(plugin["config"]["host"], json!("127.0.0.1"));
    assert_eq!(plugin["config"]["port"], json!(7000));
    Ok(())
}

#[test]
fn plugin_disable_already_disabled() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run(&[
        "plugin",
        "config",
        "http",
        "--name",
        "hook",
        "--endpoint",
        "http://127.0.0.1:9",
        "--disable",
    ])?;
    let output = cli.run(&["plugin", "disable", "hook"])?;
    assert!(
        output.contains("Plugin 'hook' is already disabled"),
        "unexpected disable repeat message:\n{}",
        output
    );
    Ok(())
}

#[test]
fn plugin_config_http_enables_plugin() -> Result<()> {
    let cli = CliTest::new()?;
    let output = cli.run(&[
        "plugin",
        "config",
        "http",
        "--name",
        "hooks",
        "--endpoint",
        "http://127.0.0.1:8080",
        "--header",
        "X-App=test",
    ])?;
    assert!(
        output.contains("HTTP plugin 'hooks' configured"),
        "unexpected http config output:\n{}",
        output
    );
    let list = cli.run(&["plugin", "list"])?;
    assert!(
        list.contains("http (hooks) - enabled"),
        "unexpected plugin list output:\n{}",
        list
    );
    Ok(())
}

#[test]
fn plugin_list_multiple_entries() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run(&[
        "plugin", "config", "log", "--name", "audit", "--level", "info",
    ])?;
    cli.run(&[
        "plugin",
        "config",
        "http",
        "--name",
        "hooks",
        "--endpoint",
        "http://127.0.0.1:8080",
        "--disable",
    ])?;
    let list = cli.run_json(&["plugin", "list", "--json"])?;
    let array = list
        .as_array()
        .context("plugin list did not return array")?;
    assert_eq!(array.len(), 2);
    Ok(())
}

#[test]
fn aggregate_get_unknown_fails() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run(&[
        "schema",
        "create",
        "existing",
        "--events",
        "existing_created",
    ])?;
    cli.create_aggregate("existing", "existing-1")?;
    cli.run(&[
        "aggregate",
        "apply",
        "existing",
        "existing-1",
        "existing_created",
        "--field",
        "status=ok",
    ])?;
    let failure = cli.run_failure(&["aggregate", "get", "existing", "missing-id"])?;
    assert!(
        failure.stderr.contains("aggregate not found"),
        "unexpected aggregate get error:\n{}",
        failure.stderr
    );
    Ok(())
}

#[test]
fn aggregate_replay_unknown_fails() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run(&[
        "schema",
        "create",
        "existing",
        "--events",
        "existing_created",
    ])?;
    cli.create_aggregate("existing", "existing-1")?;
    cli.run(&[
        "aggregate",
        "apply",
        "existing",
        "existing-1",
        "existing_created",
        "--field",
        "status=ok",
    ])?;
    let failure = cli.run_failure(&["aggregate", "replay", "existing", "missing-id"])?;
    assert!(
        failure.stderr.contains("aggregate not found"),
        "unexpected aggregate replay error:\n{}",
        failure.stderr
    );
    Ok(())
}

#[test]
fn aggregate_remove_unknown_fails() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run(&[
        "schema",
        "create",
        "existing",
        "--events",
        "existing_created",
    ])?;
    cli.create_aggregate("existing", "existing-1")?;
    cli.run(&[
        "aggregate",
        "apply",
        "existing",
        "existing-1",
        "existing_created",
        "--field",
        "status=ok",
    ])?;
    let failure = cli.run_failure(&["aggregate", "remove", "existing", "missing-id"])?;
    assert!(
        failure.stderr.contains("aggregate not found"),
        "unexpected aggregate remove error:\n{}",
        failure.stderr
    );
    Ok(())
}

#[test]
fn aggregate_verify_unknown_fails() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run(&[
        "schema",
        "create",
        "existing",
        "--events",
        "existing_created",
    ])?;
    cli.create_aggregate("existing", "existing-1")?;
    cli.run(&[
        "aggregate",
        "apply",
        "existing",
        "existing-1",
        "existing_created",
        "--field",
        "status=ok",
    ])?;
    let failure = cli.run_failure(&["aggregate", "verify", "existing", "missing-id"])?;
    assert!(
        failure.stderr.contains("aggregate not found"),
        "unexpected aggregate verify error:\n{}",
        failure.stderr
    );
    Ok(())
}

#[test]
fn aggregate_apply_requires_schema_in_strict_mode() -> Result<()> {
    let cli = CliTest::new()?;
    let _ = cli.run(&["config"])?;
    let config_path = cli.home.join(".eventdbx").join("config.toml");
    let mut config_contents =
        fs::read_to_string(&config_path).context("failed to read config.toml")?;
    config_contents = config_contents.replace("restrict = \"default\"", "restrict = \"strict\"");
    fs::write(&config_path, config_contents).context("failed to update restrict mode")?;
    let failure = cli.run_failure(&[
        "aggregate",
        "apply",
        "order",
        "order-1",
        "order_created",
        "--field",
        "status=created",
    ])?;
    assert!(
        failure
            .stderr
            .contains("aggregate order requires a schema before events can be appended"),
        "missing schema error not reported\nstdout:\n{}\nstderr:\n{}",
        failure.stdout,
        failure.stderr
    );
    Ok(())
}

#[test]
fn aggregate_apply_allows_missing_schema_in_default_mode() -> Result<()> {
    let cli = CliTest::new()?;
    cli.create_aggregate("order", "order-1")?;
    cli.run(&[
        "aggregate",
        "apply",
        "order",
        "order-1",
        "order_created",
        "--field",
        "status=created",
    ])?;
    Ok(())
}

#[test]
fn aggregate_apply_without_create_requires_flag() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run_json(&[
        "schema",
        "create",
        "legacy",
        "--events",
        "legacy_created",
        "--json",
    ])?;

    let failure = cli.run_failure(&[
        "aggregate",
        "apply",
        "legacy",
        "legacy-1",
        "legacy_created",
        "--field",
        "status=pending",
    ])?;
    assert!(
        failure.stderr.contains("does not exist"),
        "unexpected error when applying without create:\n{}",
        failure.stderr
    );

    let record = cli.run_json(&[
        "aggregate",
        "apply",
        "legacy",
        "legacy-1",
        "legacy_created",
        "--field",
        "status=pending",
        "--allow-create",
    ])?;
    assert_eq!(record["aggregate_id"], json!("legacy-1"));
    assert_eq!(record["version"], json!(1));
    Ok(())
}

#[test]
fn aggregate_create_outputs_state() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run_json(&[
        "schema",
        "create",
        "patient",
        "--events",
        "patient_created",
        "--json",
    ])?;

    let state = cli.run_json(&["aggregate", "create", "patient", "patient-1", "--json"])?;
    assert_eq!(state["aggregate_type"], json!("patient"));
    assert_eq!(state["aggregate_id"], json!("patient-1"));
    assert_eq!(state["version"], json!(0));
    assert!(state["state"].as_object().unwrap().is_empty());
    Ok(())
}

#[test]
fn aggregate_list_json_empty() -> Result<()> {
    let cli = CliTest::new()?;
    let failure = cli.run_failure(&["aggregate", "list", "--json"])?;
    assert!(
        failure.stderr.contains("storage error"),
        "unexpected aggregate list failure:\n{}",
        failure.stderr
    );
    Ok(())
}

#[test]
fn aggregate_commit_no_staged_events() -> Result<()> {
    let cli = CliTest::new()?;
    let output = cli.run(&["aggregate", "commit"])?;
    assert!(
        output.contains("no staged events to commit"),
        "unexpected commit output:\n{}",
        output
    );
    Ok(())
}

#[test]
fn aggregate_apply_metadata_invalid_key_fails() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run(&[
        "schema",
        "create",
        "metadata",
        "--events",
        "metadata_created",
    ])?;
    cli.create_aggregate("metadata", "id-1")?;
    let failure = cli.run_failure(&[
        "aggregate",
        "apply",
        "metadata",
        "id-1",
        "metadata_created",
        "--payload",
        "{}",
        "--metadata",
        "{\"foo\":1}",
    ])?;
    assert!(
        failure
            .stderr
            .contains("metadata key 'foo' must start with '@'"),
        "unexpected metadata validation message:\n{}",
        failure.stderr
    );
    Ok(())
}

#[test]
fn aggregate_apply_note_too_long_fails() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run(&["schema", "create", "notes", "--events", "note_created"])?;
    cli.create_aggregate("notes", "id-1")?;
    let long_note = "n".repeat(129);
    let failure = cli.run_failure(&[
        "aggregate",
        "apply",
        "notes",
        "id-1",
        "note_created",
        "--field",
        "status=ok",
        "--note",
        &long_note,
    ])?;
    assert!(
        failure.stderr.contains("note cannot exceed"),
        "unexpected note length message:\n{}",
        failure.stderr
    );
    Ok(())
}

#[test]
fn aggregate_list_stage_empty_shows_message() -> Result<()> {
    let cli = CliTest::new()?;
    let stdout = cli.run(&["aggregate", "list", "--stage"])?;
    assert_eq!(stdout.trim(), "no staged events");
    Ok(())
}

#[test]
fn aggregate_stage_enforces_first_event_rule() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run_json(&[
        "schema",
        "create",
        "order",
        "--events",
        "order_created,order_updated",
        "--json",
    ])?;

    cli.create_aggregate("order", "order-1")?;
    let failure = cli.run_failure(&[
        "aggregate",
        "apply",
        "order",
        "order-1",
        "order_updated",
        "--field",
        "status=processing",
        "--stage",
    ])?;
    assert!(
        failure
            .stderr
            .contains("first event for a new aggregate must end with '_created'"),
        "first-event rule error not reported\nstdout:\n{}\nstderr:\n{}",
        failure.stdout,
        failure.stderr
    );

    let staged_list = cli.run(&["aggregate", "list", "--stage"])?;
    assert_eq!(
        staged_list.trim(),
        "no staged events",
        "staging queue should remain empty after failed apply: {}",
        staged_list
    );
    Ok(())
}

#[test]
fn aggregate_apply_payload_and_field_conflict() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run_json(&[
        "schema",
        "create",
        "conflict",
        "--events",
        "conflict_created",
        "--json",
    ])?;
    cli.create_aggregate("conflict", "id-1")?;
    let failure = cli.run_failure(&[
        "aggregate",
        "apply",
        "conflict",
        "id-1",
        "conflict_created",
        "--payload",
        r#"{"status":"ok"}"#,
        "--field",
        "status=ok",
    ])?;
    assert!(
        failure
            .stderr
            .contains("--payload cannot be used together with --field"),
        "unexpected aggregate apply validation message:\n{}",
        failure.stderr
    );
    Ok(())
}

#[test]
fn aggregate_apply_and_get_flow() -> Result<()> {
    let cli = CliTest::new()?;
    let schema = cli.run_json(&[
        "schema",
        "create",
        "order",
        "--events",
        "order_created",
        "--json",
    ])?;
    assert_eq!(schema["aggregate"], "order");
    assert!(
        schema["events"].get("order_created").is_some(),
        "expected schema to include order_created event"
    );

    cli.create_aggregate("order", "order-1")?;
    let record = cli.run_json(&[
        "aggregate",
        "apply",
        "order",
        "order-1",
        "order_created",
        "--field",
        "status=created",
    ])?;
    assert_eq!(record["aggregate_type"], "order");
    assert_eq!(record["aggregate_id"], "order-1");
    assert_eq!(record["event_type"], "order_created");
    assert_eq!(record["payload"]["status"], "created");
    assert_eq!(record["version"], 1);

    let detail = cli.run_json(&["aggregate", "get", "order", "order-1", "--include-events"])?;
    assert_eq!(detail["aggregate_type"], "order");
    assert_eq!(detail["aggregate_id"], "order-1");
    assert_eq!(detail["version"], 1);
    assert_eq!(detail["state"]["status"], "created");
    let events = detail["events"]
        .as_array()
        .context("aggregate get response missing events array")?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["event_type"], "order_created");
    assert_eq!(events[0]["version"], 1);

    let aggregates = cli.run_json(&["aggregate", "list", "--json"])?;
    let list = aggregates
        .as_array()
        .context("aggregate list did not return an array")?;
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["aggregate_type"], "order");
    assert_eq!(list[0]["aggregate_id"], "order-1");
    assert_eq!(list[0]["version"], 1);

    Ok(())
}

#[test]
fn aggregate_patch_and_select_flow() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run(&[
        "schema",
        "create",
        "person",
        "--events",
        "person_created,person_updated",
    ])?;

    cli.create_aggregate("person", "person-1")?;
    cli.run(&[
        "aggregate",
        "apply",
        "person",
        "person-1",
        "person_created",
        "--payload",
        r#"{"status":"active","profile":{"name":{"first":"Ada"}}}"#,
    ])?;

    let patch_ops = r#"[{"op":"replace","path":"/status","value":"inactive"},{"op":"replace","path":"/profile/name/first","value":"Grace"}]"#;
    let record = cli.run_json(&[
        "aggregate",
        "patch",
        "person",
        "person-1",
        "person_updated",
        "--patch",
        patch_ops,
    ])?;
    assert_eq!(record["aggregate_type"], "person");
    assert_eq!(record["aggregate_id"], "person-1");
    assert_eq!(record["event_type"], "person_updated");
    assert_eq!(record["version"], 2);
    assert_eq!(record["payload"]["status"], "inactive");
    assert_eq!(record["payload"]["profile"]["name"]["first"], "Grace");

    let selection = cli.run_json(&[
        "aggregate",
        "select",
        "person",
        "person-1",
        "status",
        "profile.name.first",
    ])?;
    assert_eq!(selection["aggregate_type"], "person");
    assert_eq!(selection["aggregate_id"], "person-1");
    assert_eq!(selection["version"], 2);
    let map = selection["selection"]
        .as_object()
        .context("selection output missing selection map")?;
    assert_eq!(map["status"], json!("inactive"));
    assert_eq!(map["profile.name.first"], json!("Grace"));

    Ok(())
}

#[test]
fn aggregate_list_supports_sorting() -> Result<()> {
    let cli = CliTest::new()?;

    cli.create_aggregate("order", "order-1")?;
    cli.create_aggregate("order", "order-2")?;
    cli.create_aggregate("invoice", "invoice-1")?;

    cli.run(&[
        "aggregate",
        "apply",
        "order",
        "order-1",
        "order_created",
        "--field",
        "status=created",
    ])?;
    cli.run(&[
        "aggregate",
        "apply",
        "order",
        "order-2",
        "order_created",
        "--field",
        "status=created",
    ])?;
    cli.run(&[
        "aggregate",
        "apply",
        "invoice",
        "invoice-1",
        "invoice_created",
        "--field",
        "total=100",
    ])?;
    cli.run(&[
        "aggregate",
        "apply",
        "order",
        "order-1",
        "order_updated",
        "--field",
        "status=confirmed",
    ])?;

    let aggregates = cli.run_json(&[
        "aggregate",
        "list",
        "--json",
        "--sort",
        "aggregate_type:asc,aggregate_id:desc",
    ])?;
    let list = aggregates
        .as_array()
        .context("sorted aggregate list did not return an array")?;
    assert_eq!(list.len(), 3, "expected three aggregates in list");
    assert_eq!(list[0]["aggregate_type"], "invoice");
    assert_eq!(list[0]["aggregate_id"], "invoice-1");
    assert_eq!(list[1]["aggregate_type"], "order");
    assert_eq!(list[1]["aggregate_id"], "order-2");
    assert_eq!(list[2]["aggregate_id"], "order-1");

    let version_sorted = cli.run_json(&[
        "aggregate",
        "list",
        "--json",
        "--sort",
        "version:desc,aggregate_id:asc",
    ])?;
    let version_list = version_sorted
        .as_array()
        .context("version-sorted list did not return an array")?;
    assert_eq!(version_list.len(), 3);
    assert_eq!(version_list[0]["aggregate_id"], "order-1");
    assert_eq!(version_list[0]["version"], 2);
    assert_eq!(version_list[1]["version"], 1);
    assert_eq!(version_list[2]["version"], 1);

    let paged = cli.run_json(&[
        "aggregate",
        "list",
        "--json",
        "--take",
        "1",
        "--skip",
        "1",
        "--sort",
        "version:desc,aggregate_id:asc",
    ])?;
    let paged_list = paged
        .as_array()
        .context("paged aggregate list did not return an array")?;
    assert_eq!(paged_list.len(), 1);
    assert_eq!(paged_list[0]["aggregate_id"], "invoice-1");

    Ok(())
}

#[test]
fn aggregate_list_hides_archived_by_default() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run(&[
        "schema",
        "create",
        "archive_demo",
        "--events",
        "archive_created",
    ])?;

    cli.create_aggregate("archive_demo", "demo-1")?;
    cli.run(&[
        "aggregate",
        "apply",
        "archive_demo",
        "demo-1",
        "archive_created",
        "--field",
        "status=active",
    ])?;

    cli.run(&["aggregate", "archive", "archive_demo", "demo-1"])?;

    let active_only = cli.run_json(&["aggregate", "list", "--json"])?;
    let active_list = active_only
        .as_array()
        .context("active list did not return an array")?;
    assert!(
        active_list.is_empty(),
        "archived aggregate should be hidden by default"
    );

    let include_archived = cli.run_json(&["aggregate", "list", "--json", "--include-archived"])?;
    let include_list = include_archived
        .as_array()
        .context("include archived list did not return an array")?;
    assert_eq!(include_list.len(), 1);
    assert_eq!(include_list[0]["aggregate_type"], json!("archive_demo"));
    assert_eq!(include_list[0]["aggregate_id"], json!("demo-1"));
    assert_eq!(include_list[0]["archived"], json!(true));

    let archived_only = cli.run_json(&["aggregate", "list", "--json", "--archived-only"])?;
    let archived_list = archived_only
        .as_array()
        .context("archived-only list did not return an array")?;
    assert_eq!(archived_list.len(), 1);
    assert_eq!(archived_list[0]["aggregate_id"], json!("demo-1"));

    Ok(())
}

#[test]
fn aggregate_replay_after_apply_returns_events() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run_json(&[
        "schema",
        "create",
        "replay_order",
        "--events",
        "order_created",
        "--json",
    ])?;
    cli.create_aggregate("replay_order", "order-1")?;
    cli.run(&[
        "aggregate",
        "apply",
        "replay_order",
        "order-1",
        "order_created",
        "--field",
        "status=pending",
    ])?;

    let events = cli.run_json(&["aggregate", "replay", "replay_order", "order-1", "--json"])?;
    let array = events
        .as_array()
        .context("aggregate replay did not return an array")?;
    assert_eq!(array.len(), 1);
    assert_eq!(array[0]["event_type"], json!("order_created"));
    assert_eq!(array[0]["payload"]["status"], json!("pending"));
    Ok(())
}

#[test]
fn aggregate_snapshot_creates_record() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run_json(&[
        "schema",
        "create",
        "snap",
        "--events",
        "snap_created",
        "--json",
    ])?;
    cli.create_aggregate("snap", "snap-1")?;
    cli.run(&[
        "aggregate",
        "apply",
        "snap",
        "snap-1",
        "snap_created",
        "--field",
        "status=ready",
    ])?;
    let output = cli.run(&["aggregate", "snapshot", "snap", "snap-1"])?;
    let snapshot: Value =
        serde_json::from_str(output.trim()).context("failed to parse snapshot output")?;
    assert_eq!(snapshot["aggregate_id"], json!("snap-1"));
    assert_eq!(snapshot["state"]["status"], json!("ready"));
    Ok(())
}

#[test]
fn aggregate_verify_success_json() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run_json(&[
        "schema",
        "create",
        "verify",
        "--events",
        "verify_created",
        "--json",
    ])?;
    cli.create_aggregate("verify", "verify-1")?;
    cli.run(&[
        "aggregate",
        "apply",
        "verify",
        "verify-1",
        "verify_created",
        "--field",
        "status=ok",
    ])?;
    let output = cli.run(&["aggregate", "verify", "verify", "verify-1", "--json"])?;
    let body: Value =
        serde_json::from_str(output.trim()).context("failed to parse verify output")?;
    assert_eq!(body["aggregate_id"], json!("verify-1"));
    assert!(
        body["merkle_root"]
            .as_str()
            .map(|s| !s.is_empty())
            .unwrap_or(false)
    );
    Ok(())
}

#[test]
fn aggregate_archive_and_restore_flow() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run_json(&[
        "schema",
        "create",
        "archive",
        "--events",
        "archive_created",
        "--json",
    ])?;
    cli.create_aggregate("archive", "archive-1")?;
    cli.run(&[
        "aggregate",
        "apply",
        "archive",
        "archive-1",
        "archive_created",
        "--field",
        "status=active",
    ])?;

    let archived = cli.run(&[
        "aggregate",
        "archive",
        "archive",
        "archive-1",
        "--comment",
        "archived",
    ])?;
    assert!(
        archived.contains("archived=true"),
        "unexpected archive output:\n{}",
        archived
    );

    let restored = cli.run(&[
        "aggregate",
        "restore",
        "archive",
        "archive-1",
        "--comment",
        "restored",
    ])?;
    assert!(
        restored.contains("archived=false"),
        "unexpected restore output:\n{}",
        restored
    );
    Ok(())
}

#[test]
fn aggregate_replay_with_skip_take() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run_json(&[
        "schema",
        "create",
        "timeline",
        "--events",
        "order_created,order_updated",
        "--json",
    ])?;
    cli.create_aggregate("timeline", "order-1")?;
    cli.run(&[
        "aggregate",
        "apply",
        "timeline",
        "order-1",
        "order_created",
        "--field",
        "status=pending",
    ])?;
    cli.run(&[
        "aggregate",
        "apply",
        "timeline",
        "order-1",
        "order_updated",
        "--field",
        "status=complete",
    ])?;
    let events = cli.run_json(&[
        "aggregate",
        "replay",
        "timeline",
        "order-1",
        "--skip",
        "1",
        "--json",
    ])?;
    let array = events
        .as_array()
        .context("replay with skip did not return array")?;
    assert_eq!(array.len(), 1);
    assert_eq!(array[0]["event_type"], json!("order_updated"));
    Ok(())
}

#[test]
fn aggregate_export_json_creates_file() -> Result<()> {
    let cli = CliTest::new()?;
    cli.run_json(&[
        "schema",
        "create",
        "export",
        "--events",
        "export_created",
        "--json",
    ])?;
    cli.create_aggregate("export", "export-1")?;
    cli.run(&[
        "aggregate",
        "apply",
        "export",
        "export-1",
        "export_created",
        "--field",
        "status=ready",
    ])?;
    let output_path = cli.home.join("export.json");
    let stdout = cli.run(&[
        "aggregate",
        "export",
        "export",
        "--format",
        "json",
        "--output",
        output_path.to_string_lossy().as_ref(),
        "--pretty",
    ])?;
    assert!(
        stdout.contains("Exported"),
        "unexpected export output:\n{}",
        stdout
    );
    assert!(output_path.exists(), "export file not created");
    let contents = fs::read_to_string(&output_path)?;
    assert!(
        contents.contains("export-1"),
        "export file missing aggregate id: {}",
        contents
    );
    Ok(())
}

#[test]
fn aggregate_stage_and_commit_flow() -> Result<()> {
    let cli = CliTest::new()?;
    let schema = cli.run_json(&[
        "schema",
        "create",
        "order",
        "--events",
        "order_created,order_updated",
        "--json",
    ])?;
    assert_eq!(schema["aggregate"], "order");

    cli.create_aggregate("order", "order-1")?;

    let staged = cli.run(&[
        "aggregate",
        "apply",
        "order",
        "order-1",
        "order_created",
        "--field",
        "status=created",
        "--stage",
    ])?;
    assert!(
        staged.contains("event staged for later commit"),
        "unexpected staging output:\n{}",
        staged
    );

    let staged_list = cli.run(&["aggregate", "list", "--stage"])?;
    assert!(
        staged_list.contains("\"event\": \"order_created\""),
        "expected staged list to include order_created event:\n{}",
        staged_list
    );

    let commit_output = cli.run(&["aggregate", "commit"])?;
    assert!(
        commit_output.contains("committed 1 event(s)"),
        "unexpected commit output:\n{}",
        commit_output
    );

    let staged_list_after = cli.run(&["aggregate", "list", "--stage"])?;
    assert_eq!(
        staged_list_after.trim(),
        "no staged events",
        "expected staging queue to be empty after commit:\n{}",
        staged_list_after
    );

    let staged_update = cli.run(&[
        "aggregate",
        "apply",
        "order",
        "order-1",
        "order_updated",
        "--field",
        "status=processing",
        "--stage",
    ])?;
    assert!(
        staged_update.contains("event staged for later commit"),
        "unexpected staging output for update:\n{}",
        staged_update
    );

    let commit_update = cli.run(&["aggregate", "commit"])?;
    assert!(
        commit_update.contains("committed 1 event(s)"),
        "unexpected commit output for update:\n{}",
        commit_update
    );

    let detail = cli.run_json(&["aggregate", "get", "order", "order-1", "--include-events"])?;
    assert_eq!(detail["version"], 2);
    assert_eq!(detail["state"]["status"], "processing");
    let events = detail["events"]
        .as_array()
        .context("aggregate get response missing events array")?;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["event_type"], "order_created");
    assert_eq!(events[1]["event_type"], "order_updated");
    assert_eq!(events[1]["payload"]["status"], "processing");

    Ok(())
}

fn normalize_output(bytes: Vec<u8>) -> Result<String> {
    Ok(String::from_utf8(bytes)?.replace("\r\n", "\n"))
}
