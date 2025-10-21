use std::{io, net::TcpListener, path::PathBuf, time::Duration};

use base64::{Engine, engine::general_purpose::STANDARD};
use eventdbx::{config::Config, server};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{task::JoinHandle, time::sleep};

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread")]
async fn admin_rest_endpoints() -> TestResult<()> {
    let temp = TempDir::new()?;
    let mut config = Config::default();
    config.data_dir = temp.path().join("data");

    let http_port = match allocate_port() {
        Ok(port) => port,
        Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("skipping admin API test: port binding not permitted ({err})");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };
    let admin_port = match allocate_port() {
        Ok(port) => port,
        Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("skipping admin API test: port binding not permitted ({err})");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };

    config.port = http_port;
    config.restrict = false;
    config.data_encryption_key = Some(STANDARD.encode([2u8; 32]));
    config.admin.enabled = true;
    config.admin.bind_addr = "127.0.0.1".to_string();
    config.admin.port = Some(admin_port);
    config.set_admin_master_key("super-secret")?;
    config.ensure_data_dir()?;

    let config_path = temp.path().join("config.toml");
    config.save(&config_path)?;

    let server_handle = spawn_server(config.clone(), config_path.clone())?;

    let base_url = format!("http://127.0.0.1:{http_port}");
    wait_for_health(&base_url).await?;

    let admin_base = format!("http://127.0.0.1:{admin_port}/admin");
    let admin_key = "super-secret";
    wait_for_admin(&admin_base, admin_key).await?;

    let client = Client::new();

    // Unauthorized requests should be rejected.
    let unauthorized = client.get(format!("{admin_base}/tokens")).send().await?;
    assert_eq!(
        unauthorized.status(),
        StatusCode::UNAUTHORIZED,
        "admin endpoints must require authentication"
    );

    // Issue a new token.
    let issue_payload = json!({
        "group": "ops",
        "user": "scheduler",
        "expiration_secs": 600
    });
    let issued_token: Value = client
        .post(format!("{admin_base}/tokens"))
        .header("X-Admin-Key", admin_key)
        .json(&issue_payload)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let token_str = issued_token["token"]
        .as_str()
        .expect("issued token response should include token value")
        .to_string();

    // List tokens should include the newly issued token.
    let tokens: Value = client
        .get(format!("{admin_base}/tokens"))
        .header("X-Admin-Key", admin_key)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(
        tokens
            .as_array()
            .expect("token list should be an array")
            .iter()
            .any(|item| item["token"] == token_str),
        "token list must include the issued token"
    );

    // Create a schema through the admin API.
    let schema_payload = json!({
        "aggregate": "admin-test",
        "events": ["created"],
        "snapshot_threshold": 10
    });
    let schema_response: Value = client
        .post(format!("{admin_base}/schemas"))
        .header("X-Admin-Key", admin_key)
        .json(&schema_payload)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(schema_response["aggregate"], "admin-test");

    // Retrieve the newly created schema.
    let schema_lookup: Value = client
        .get(format!("{admin_base}/schemas/admin-test"))
        .header("X-Admin-Key", admin_key)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(schema_lookup["aggregate"], "admin-test");

    // Configure a remote endpoint.
    let remote_payload = json!({
        "endpoint": "grpc://127.0.0.1:7443",
        "public_key": "ZmFrZS1rZXk="
    });
    client
        .put(format!("{admin_base}/remotes/primary"))
        .header("X-Admin-Key", admin_key)
        .json(&remote_payload)
        .send()
        .await?
        .error_for_status()?;

    let remotes: Value = client
        .get(format!("{admin_base}/remotes"))
        .header("X-Admin-Key", admin_key)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(
        remotes
            .get("primary")
            .and_then(|remote| remote.get("endpoint"))
            .map(|endpoint| endpoint == "grpc://127.0.0.1:7443")
            .unwrap_or(false),
        "remote configuration should be persisted"
    );

    // Configure a log plugin.
    let plugin_payload = json!({
        "enabled": true,
        "config": {
            "type": "log",
            "level": "debug",
            "template": null
        }
    });
    client
        .put(format!("{admin_base}/plugins/logger"))
        .header("X-Admin-Key", admin_key)
        .json(&plugin_payload)
        .send()
        .await?
        .error_for_status()?;

    let plugins_response: Value = client
        .get(format!("{admin_base}/plugins"))
        .header("X-Admin-Key", admin_key)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    assert!(
        plugins_response
            .as_array()
            .expect("plugins list should be an array")
            .iter()
            .any(|plugin| plugin["name"] == "logger"),
        "plugin list should include the configured logger plugin"
    );

    server_handle.abort();
    let _ = server_handle.await;

    Ok(())
}

fn allocate_port() -> io::Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn spawn_server(
    config: Config,
    config_path: PathBuf,
) -> TestResult<JoinHandle<eventdbx::error::Result<()>>> {
    Ok(tokio::spawn(async move {
        server::run(config, config_path).await
    }))
}

async fn wait_for_health(base_url: &str) -> TestResult<()> {
    for _ in 0..40 {
        if let Ok(resp) = reqwest::get(format!("{base_url}/health")).await {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    Err("server did not become healthy in time".into())
}

async fn wait_for_admin(base_url: &str, admin_key: &str) -> TestResult<()> {
    let client = Client::new();
    for _ in 0..40 {
        let resp = client
            .get(format!("{base_url}/tokens"))
            .header("X-Admin-Key", admin_key)
            .send()
            .await;
        if let Ok(resp) = resp {
            if resp.status() != StatusCode::UNAUTHORIZED {
                return Ok(());
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    Err("admin API did not become ready in time".into())
}
