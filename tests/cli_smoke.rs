use std::path::Path;
use std::path::PathBuf;
use std::{fs, process::Output};

use anyhow::{Context, Result};
use assert_cmd::{Command, cargo::cargo_bin_cmd};
use serde_json::{Value, json};
use tempfile::TempDir;

use eventdbx::{config::Config, store::EventStore, token::TokenManager};

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
        let project_dir = self.project_dir()?;
        self.command_in(project_dir.as_path())
    }

    fn command_in(&self, cwd: &Path) -> Result<Command> {
        let mut cmd = cargo_bin_cmd!("dbx");
        cmd.env("HOME", &self.home);
        cmd.env("EVENTDBX_LOG_DIR", &self.log_dir);
        cmd.env("DBX_NO_UPGRADE_CHECK", "1");
        cmd.env("XDG_CONFIG_HOME", &self.config_home);
        cmd.current_dir(cwd);
        Ok(cmd)
    }

    fn run(&self, args: &[&str]) -> Result<String> {
        let output = self.exec(args)?;
        if !output.status.success() {
            anyhow::bail!(
                "dbx {:?} exited with {}: {}",
                args,
                output.status,
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(normalize_output(output.stdout)?)
    }

    fn run_json(&self, args: &[&str]) -> Result<Value> {
        let stdout = self.run(args)?;
        serde_json::from_str(stdout.trim())
            .with_context(|| format!("failed to parse JSON output from dbx {:?}", args))
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

    fn exec(&self, args: &[&str]) -> Result<Output> {
        let mut cmd = self.command()?;
        cmd.args(args);
        cmd.output()
            .with_context(|| format!("failed to run dbx {:?}", args))
    }

    fn exec_in(&self, cwd: &Path, args: &[&str]) -> Result<Output> {
        let mut cmd = self.command_in(cwd)?;
        cmd.args(args);
        cmd.output()
            .with_context(|| format!("failed to run dbx {:?} in {}", args, cwd.display()))
    }

    fn run_in(&self, cwd: &Path, args: &[&str]) -> Result<String> {
        let output = self.exec_in(cwd, args)?;
        if !output.status.success() {
            anyhow::bail!(
                "dbx {:?} exited with {}: {}",
                args,
                output.status,
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(normalize_output(output.stdout)?)
    }

    fn run_failure_in(&self, cwd: &Path, args: &[&str]) -> Result<FailureOutput> {
        let output = self.exec_in(cwd, args)?;
        if output.status.success() {
            anyhow::bail!("expected dbx {:?} to fail but it succeeded", args);
        }
        Ok(FailureOutput {
            stdout: normalize_output(output.stdout)?,
            stderr: normalize_output(output.stderr)?,
        })
    }

    fn workspace_dir(&self) -> Result<PathBuf> {
        Ok(self.project_dir()?.join(".dbx"))
    }

    fn data_dir(&self) -> Result<PathBuf> {
        self.workspace_dir()
    }

    fn config_path(&self) -> Result<PathBuf> {
        Ok(self.workspace_dir()?.join("config.toml"))
    }

    fn init(&self) -> Result<String> {
        self.init_with_args(&[])
    }

    fn init_with_args(&self, args: &[&str]) -> Result<String> {
        let mut argv = vec!["init"];
        argv.extend_from_slice(args);
        self.run(&argv)
    }

    fn load_config(&self) -> Result<Config> {
        let config_path = self.config_path()?;
        if !config_path.exists() {
            let _ = self.init()?;
        }
        let contents = fs::read_to_string(&config_path).context("failed to read config")?;
        toml::from_str(&contents).context("failed to parse config.toml")
    }

    fn project_dir(&self) -> Result<PathBuf> {
        let dir = self.home.join("project");
        fs::create_dir_all(&dir).context("failed to create project directory")?;
        Ok(dir)
    }

    fn write_project_file(&self, relative: &str, contents: &str) -> Result<PathBuf> {
        let path = self.project_dir()?.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        fs::write(&path, contents)
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(path)
    }

    fn write_schema_source(&self, contents: &str) -> Result<PathBuf> {
        self.write_project_file("schema.dbx", contents)
    }

    fn apply_schema_source(&self, contents: &str) -> Result<()> {
        let _ = self.init()?;
        let path = self.write_schema_source(contents)?;
        self.run(&[
            "schema",
            "apply",
            "--file",
            path.to_string_lossy().as_ref(),
            "--no-reload",
        ])?;
        Ok(())
    }
}

fn normalize_output(bytes: Vec<u8>) -> Result<String> {
    Ok(String::from_utf8(bytes)
        .context("output was not utf-8")?
        .replace("\r\n", "\n"))
}

fn core_schema() -> &'static str {
    r#"
    aggregate order {
      snapshot_threshold = 2

      field status {
        type = "text"
      }

      event order_created {
        fields = ["status"]
      }

      event order_paid {
        fields = ["status"]
      }
    }
    "#
}

#[test]
fn token_list_includes_init_bootstrap_token() -> Result<()> {
    let cli = CliTest::new()?;
    let _ = cli.init()?;
    let stdout = cli.run(&["token", "list"])?;
    assert!(
        stdout.contains("subject=cli:bootstrap"),
        "unexpected output:\n{stdout}"
    );
    Ok(())
}

#[test]
fn token_generate_and_list_json_round_trip() -> Result<()> {
    let cli = CliTest::new()?;
    let _ = cli.init()?;
    let generated = cli.run_json(&[
        "token",
        "generate",
        "--group",
        "ops",
        "--user",
        "alice",
        "--action",
        "aggregate.read",
        "--json",
    ])?;
    assert_eq!(generated["group"], json!("ops"));
    assert_eq!(generated["user"], json!("alice"));
    assert_eq!(generated["actions"], json!(["aggregate.read"]));
    assert_eq!(generated["tenants"], json!([]));

    let listed = cli.run_json(&["token", "list", "--json"])?;
    let records = listed
        .as_array()
        .context("token list did not return an array")?;
    let generated_record = records
        .iter()
        .find(|record| {
            record.get("group") == Some(&json!("ops"))
                && record.get("user") == Some(&json!("alice"))
        })
        .context("generated token missing from token list")?;
    assert_eq!(generated_record["actions"], json!(["aggregate.read"]));
    Ok(())
}

#[test]
fn schema_validate_reports_compiled_summary() -> Result<()> {
    let cli = CliTest::new()?;
    let schema_path = cli.write_schema_source(core_schema())?;
    let output = cli.run_json(&[
        "schema",
        "validate",
        "--file",
        schema_path.to_string_lossy().as_ref(),
        "--json",
    ])?;
    assert_eq!(output["valid"], json!(true));
    assert_eq!(output["aggregate_count"], json!(1));
    assert_eq!(output["aggregates"], json!(["order"]));
    Ok(())
}

#[test]
fn schema_show_is_deterministic_for_unchanged_input() -> Result<()> {
    let cli = CliTest::new()?;
    let schema_path = cli.write_schema_source(core_schema())?;
    let first = cli.run(&[
        "schema",
        "show",
        "--file",
        schema_path.to_string_lossy().as_ref(),
    ])?;
    let second = cli.run(&[
        "schema",
        "show",
        "--file",
        schema_path.to_string_lossy().as_ref(),
    ])?;
    assert_eq!(
        first, second,
        "schema show output changed across identical runs"
    );
    Ok(())
}

#[test]
fn schema_apply_writes_runtime_schema_store() -> Result<()> {
    let cli = CliTest::new()?;
    cli.apply_schema_source(core_schema())?;
    let config = cli.load_config()?;
    let contents = fs::read_to_string(config.schema_store_path())
        .context("failed to read compiled schemas.json")?;
    let parsed: Value = serde_json::from_str(&contents).context("failed to parse schemas.json")?;
    assert!(
        parsed.get("order").is_some(),
        "compiled schema missing order aggregate"
    );
    Ok(())
}

#[test]
fn automatic_snapshot_triggers_from_schema_threshold() -> Result<()> {
    let cli = CliTest::new()?;
    cli.apply_schema_source(core_schema())?;

    let _ = cli.run_json(&[
        "aggregate",
        "create",
        "order",
        "order-1",
        "--event",
        "order_created",
        "--field",
        "status=open",
        "--json",
    ])?;
    let _ = cli.run_json(&[
        "aggregate",
        "apply",
        "order",
        "order-1",
        "order_paid",
        "--field",
        "status=paid",
    ])?;

    let config = cli.load_config()?;
    let store = EventStore::open_read_only(config.event_store_path(), config.encryption_key()?)?;
    let snapshots = store.list_snapshots(Some("order"), Some("order-1"), None)?;
    assert_eq!(
        snapshots.len(),
        1,
        "expected one auto snapshot at version 2"
    );
    assert_eq!(snapshots[0].version, 2);
    Ok(())
}

#[test]
fn aggregate_and_event_history_flow() -> Result<()> {
    let cli = CliTest::new()?;
    cli.apply_schema_source(core_schema())?;

    let created = cli.run_json(&[
        "aggregate",
        "create",
        "order",
        "order-42",
        "--event",
        "order_created",
        "--field",
        "status=open",
        "--json",
    ])?;
    assert_eq!(created["aggregate_type"], json!("order"));
    assert_eq!(created["aggregate_id"], json!("order-42"));
    assert_eq!(created["version"], json!(1));

    let appended = cli.run_json(&[
        "aggregate",
        "apply",
        "order",
        "order-42",
        "order_paid",
        "--field",
        "status=paid",
    ])?;
    let event_id = appended["metadata"]["event_id"]
        .as_str()
        .context("append output missing event_id")?
        .to_string();

    let listed = cli.run_json(&["aggregate", "list", "order", "--json"])?;
    let items = listed["items"]
        .as_array()
        .context("aggregate list items missing")?;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["aggregate_id"], json!("order-42"));

    let aggregate = cli.run_json(&["aggregate", "get", "order", "order-42", "--include-events"])?;
    assert_eq!(aggregate["state"]["status"], json!("paid"));
    assert_eq!(aggregate["version"], json!(2));
    assert_eq!(
        aggregate["events"]
            .as_array()
            .context("events missing")?
            .len(),
        2
    );

    let events = cli.run_json(&["events", "order", "order-42", "--json"])?;
    let event_items = events
        .as_array()
        .context("events did not return an array")?;
    assert_eq!(event_items.len(), 2);

    let event = cli.run_json(&["events", "--event", &event_id, "--json"])?;
    assert_eq!(event["event_type"], json!("order_paid"));

    let verify = cli.run_json(&["aggregate", "verify", "order", "order-42", "--json"])?;
    assert!(verify["merkle_root"].as_str().is_some());
    Ok(())
}

#[test]
fn status_rejects_non_default_domain_config() -> Result<()> {
    let cli = CliTest::new()?;
    let _ = cli.init()?;
    let mut contents =
        fs::read_to_string(cli.config_path()?).context("failed to read generated config.toml")?;
    contents = contents.replace("domain = \"default\"\n", "domain = \"legacy\"\n");
    fs::write(cli.config_path()?, contents).context("failed to write legacy config")?;

    let failure = cli.run_failure(&["status"])?;
    assert!(
        failure
            .stderr
            .contains("single-tenant core only supports domain 'default'"),
        "unexpected stderr:\n{}\nstdout:\n{}",
        failure.stderr,
        failure.stdout
    );
    Ok(())
}

#[test]
fn status_rejects_legacy_tenant_layout() -> Result<()> {
    let cli = CliTest::new()?;
    let _ = cli.init()?;

    let rogue_dir = cli
        .data_dir()?
        .join("shards")
        .join("shard-0000")
        .join("tenants")
        .join("beta");
    fs::create_dir_all(&rogue_dir).context("failed to create legacy tenant dir")?;
    fs::write(rogue_dir.join("README"), "legacy").context("failed to seed legacy tenant dir")?;

    let failure = cli.run_failure(&["status"])?;
    assert!(
        failure
            .stderr
            .contains("single-tenant core only supports one local domain"),
        "unexpected stderr:\n{}\nstdout:\n{}",
        failure.stderr,
        failure.stdout
    );
    Ok(())
}

#[test]
fn config_prints_lean_surface() -> Result<()> {
    let cli = CliTest::new()?;
    let _ = cli.init()?;
    let stdout = cli.run(&["config"])?;
    assert!(stdout.contains("socket"));
    assert!(stdout.contains("auth"));
    assert!(!stdout.contains("multi_tenant"));
    assert!(!stdout.contains("snapshot_threshold"));
    assert!(!stdout.contains("plugin_max_attempts"));
    Ok(())
}

#[test]
fn init_creates_local_workspace() -> Result<()> {
    let cli = CliTest::new()?;

    let stdout = cli.init()?;
    let config = cli.load_config()?;
    let token = stdout
        .lines()
        .find_map(|line| line.strip_prefix("Bootstrap token (ttl=86400s): "))
        .context("init output missing bootstrap token")?
        .to_string();
    let manager = TokenManager::load(
        config.jwt_manager_config()?,
        config.tokens_path(),
        config.jwt_revocations_path(),
        config.encryption_key()?,
    )?;
    let claims = manager.verify(&token)?;
    let ttl_secs = claims
        .exp
        .map(|exp| exp - claims.iat)
        .context("bootstrap token should expire")?;

    assert!(stdout.contains("Initialized empty EventDBX workspace"));
    assert!(cli.config_path()?.exists());
    assert!(cli.data_dir()?.exists());
    assert!(config.cli_token_path().exists());
    assert!(config.event_store_path().join("CURRENT").exists());
    assert!(
        (ttl_secs - 86_400).abs() <= 1,
        "expected bootstrap TTL close to 86400, got {ttl_secs}"
    );
    Ok(())
}

#[test]
fn init_is_idempotent() -> Result<()> {
    let cli = CliTest::new()?;

    let _ = cli.init()?;
    let stdout = cli.init()?;

    assert!(stdout.contains("already initialized"));
    Ok(())
}

#[test]
fn destroy_removes_workspace_directory() -> Result<()> {
    let cli = CliTest::new()?;
    let _ = cli.init()?;

    let stdout = cli.run(&["destroy", "--yes"])?;

    assert!(stdout.contains("Removed EventDBX workspace"));
    assert!(!cli.workspace_dir()?.exists());
    Ok(())
}

#[test]
fn runtime_commands_fail_after_destroy_removes_workspace() -> Result<()> {
    let cli = CliTest::new()?;
    let _ = cli.init()?;

    let _ = cli.run(&["destroy", "--yes"])?;
    let failure = cli.run_failure(&["status"])?;

    assert!(failure.stderr.contains("Run `dbx init`"));
    Ok(())
}

#[test]
fn destroy_preserves_external_data_dir() -> Result<()> {
    let cli = CliTest::new()?;
    let _ = cli.init()?;

    let external_data_dir = cli.home.join("external-data");
    fs::create_dir_all(&external_data_dir).context("failed to create external data dir")?;
    fs::write(external_data_dir.join("sentinel.txt"), "keep me")
        .context("failed to write sentinel file")?;

    let mut config = cli.load_config()?;
    config.data_dir = external_data_dir.clone();
    let contents = toml::to_string_pretty(&config).context("failed to serialize config")?;
    fs::write(cli.config_path()?, contents).context("failed to update config.toml")?;

    let _ = cli.run(&["destroy", "--yes"])?;

    assert!(!cli.workspace_dir()?.exists());
    assert!(external_data_dir.exists());
    assert!(external_data_dir.join("sentinel.txt").exists());
    Ok(())
}

#[test]
fn init_ttl_accepts_human_duration_suffixes() -> Result<()> {
    let cli = CliTest::new()?;

    let stdout = cli.init_with_args(&["--ttl", "10d"])?;
    let config = cli.load_config()?;
    let token = stdout
        .lines()
        .find_map(|line| line.strip_prefix("Bootstrap token (ttl=864000s): "))
        .context("init output missing bootstrap token")?
        .to_string();
    let manager = TokenManager::load(
        config.jwt_manager_config()?,
        config.tokens_path(),
        config.jwt_revocations_path(),
        config.encryption_key()?,
    )?;
    let claims = manager.verify(&token)?;
    let ttl_secs = claims
        .exp
        .map(|exp| exp - claims.iat)
        .context("bootstrap token should expire")?;

    assert!(
        (ttl_secs - 864_000).abs() <= 1,
        "expected bootstrap TTL close to 864000, got {ttl_secs}"
    );
    Ok(())
}

#[test]
fn runtime_commands_fail_without_workspace() -> Result<()> {
    let cli = CliTest::new()?;
    let outside = cli.home.join("outside");
    fs::create_dir_all(&outside).context("failed to create outside dir")?;

    let failure = cli.run_failure_in(&outside, &["status"])?;
    assert!(failure.stderr.contains("Run `dbx init`"));
    Ok(())
}

#[test]
fn runtime_commands_discover_workspace_from_nested_directory() -> Result<()> {
    let cli = CliTest::new()?;
    let _ = cli.init()?;
    let nested = cli.project_dir()?.join("nested").join("deeper");
    fs::create_dir_all(&nested).context("failed to create nested dir")?;

    let stdout = cli.run_in(&nested, &["config"])?;

    assert!(stdout.contains("data_dir"));
    assert!(stdout.contains(".dbx"));
    Ok(())
}
