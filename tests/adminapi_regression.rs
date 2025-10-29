use std::{io, net::TcpListener, path::PathBuf, time::Duration};

use base64::{Engine, engine::general_purpose::STANDARD};
use eventdbx::{
    config::Config,
    restrict::RestrictMode,
    schema::{CreateSchemaInput, SchemaManager},
    server,
    token::{IssueTokenInput, JwtLimits, ROOT_ACTION, ROOT_RESOURCE, TokenManager},
};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{task::JoinHandle, time::sleep};

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread")]
async fn adminapi_regression() -> TestResult<()> {
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
    let socket_port = match allocate_port() {
        Ok(port) => port,
        Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("skipping admin API test: port binding not permitted ({err})");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };

    config.port = http_port;
    config.restrict = RestrictMode::Off;
    config.data_encryption_key = Some(STANDARD.encode([2u8; 32]));
    config.socket.bind_addr = format!("127.0.0.1:{socket_port}");
    config.admin.enabled = true;
    config.admin.bind_addr = "127.0.0.1".to_string();
    config.admin.port = Some(admin_port);
    config.ensure_data_dir()?;

    let token_manager = TokenManager::load(
        config.jwt_manager_config()?,
        config.tokens_path(),
        config.jwt_revocations_path(),
        config.encryption_key()?,
    )?;
    let admin_token = token_manager
        .issue(IssueTokenInput {
            subject: "ops:admin".to_string(),
            group: "ops".to_string(),
            user: "admin".to_string(),
            actions: vec![ROOT_ACTION.to_string()],
            resources: vec![ROOT_RESOURCE.to_string()],
            ttl_secs: Some(3600),
            not_before: None,
            issued_by: "adminapi-regression".to_string(),
            limits: JwtLimits::default(),
        })?
        .token
        .expect("admin token should include value");
    drop(token_manager);

    // Pre-seed a schema so the in-memory schema manager contains an aggregate
    // before the server starts. Update endpoints operate on this stateful cache.
    let managed_aggregate = "admin-managed";
    let schema_manager = SchemaManager::load(config.schema_store_path())?;
    schema_manager.create(CreateSchemaInput {
        aggregate: managed_aggregate.to_string(),
        events: vec!["created".to_string(), "updated".to_string()],
        snapshot_threshold: Some(5),
    })?;
    drop(schema_manager);

    let config_path = temp.path().join("config.toml");
    config.save(&config_path)?;

    let server_handle = spawn_server(config.clone(), config_path.clone())?;

    let base_url = format!("http://127.0.0.1:{http_port}");
    wait_for_health(&base_url).await?;

    let admin_base = format!("http://127.0.0.1:{admin_port}/admin");
    wait_for_admin(&admin_base, &admin_token).await?;

    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;

    // Unauthorized requests should be rejected.
    let unauthorized = client.get(format!("{admin_base}/tokens")).send().await?;
    assert_eq!(
        unauthorized.status(),
        StatusCode::UNAUTHORIZED,
        "admin endpoints must require authentication"
    );

    // Issue a new token with explicit actions and TTL.
    let issue_payload = json!({
        "group": "ops",
        "user": "scheduler",
        "actions": ["aggregate.append", "schema.read"],
        "resources": ["aggregate:admin-managed:*"],
        "ttl_secs": 600,
        "issued_by": "admin-test",
        "keep_alive": false
    });
    let issued_token: Value = client
        .post(format!("{admin_base}/tokens"))
        .bearer_auth(&admin_token)
        .json(&issue_payload)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        issued_token["group"], "ops",
        "issued token should reflect group"
    );
    assert_eq!(
        issued_token["status"], "active",
        "issued token should be active initially"
    );
    assert_eq!(
        issued_token["actions"],
        json!(["aggregate.append", "schema.read"]),
        "issued token should retain requested actions"
    );
    assert_eq!(
        issued_token["limits"]["keep_alive"], false,
        "issued token should honor keep-alive flag"
    );
    let mut token_str = issued_token["token"]
        .as_str()
        .expect("issued token response should include token value")
        .to_string();
    let token_jti = issued_token["jti"]
        .as_str()
        .expect("issued token response should include jti")
        .to_string();

    // List tokens should include the newly issued token.
    let tokens_list: Value = client
        .get(format!("{admin_base}/tokens"))
        .bearer_auth(&admin_token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let tokens = tokens_list
        .as_array()
        .expect("token list should be an array");
    let issued_entry = tokens
        .iter()
        .find(|item| item["jti"] == token_jti)
        .expect("issued token must be in list");
    assert!(
        issued_entry.get("token").is_none(),
        "token list should not expose raw token values"
    );
    assert_eq!(
        issued_entry["status"], "active",
        "token list entry should reflect active status"
    );
    assert_eq!(
        issued_entry["actions"], issued_token["actions"],
        "token list should surface action scopes"
    );

    // Refresh the token to update expiration and increase write limit.
    let refresh_payload = json!({
        "ttl_secs": 900
    });
    let refreshed_token: Value = client
        .post(format!("{admin_base}/tokens/{token_str}/refresh"))
        .bearer_auth(&admin_token)
        .json(&refresh_payload)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let refreshed_token_str = refreshed_token["token"]
        .as_str()
        .expect("refresh response should include token")
        .to_string();
    assert_ne!(
        refreshed_token_str, token_str,
        "refresh should issue a replacement token"
    );
    let refreshed_jti = refreshed_token["jti"]
        .as_str()
        .expect("refresh response should include jti")
        .to_string();
    assert_eq!(
        refreshed_token["status"], "active",
        "refreshed token should remain active"
    );
    assert_eq!(
        refreshed_token["actions"], issued_token["actions"],
        "refresh should preserve action scopes"
    );
    token_str = refreshed_token_str;

    // Revoke the token and verify it is marked as revoked.
    let revoke_response = client
        .post(format!("{admin_base}/tokens/{token_str}/revoke"))
        .bearer_auth(&admin_token)
        .send()
        .await?;
    assert_eq!(
        revoke_response.status(),
        StatusCode::NO_CONTENT,
        "token revoke should return 204"
    );
    let tokens_after_revoke: Value = client
        .get(format!("{admin_base}/tokens"))
        .bearer_auth(&admin_token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let tokens_after = tokens_after_revoke
        .as_array()
        .expect("token list should be an array");
    let revoked_entry = tokens_after
        .iter()
        .find(|item| item["jti"] == refreshed_jti)
        .expect("revoked token should still appear in listing");
    assert!(
        revoked_entry.get("token").is_none(),
        "revoked token listing should not expose raw token"
    );
    assert_eq!(
        revoked_entry["status"], "revoked",
        "revoked token must display revoked status"
    );

    // Create a schema through the admin API.
    let created_aggregate = "admin-created";
    let schema_payload = json!({
        "aggregate": created_aggregate,
        "events": ["created", "updated"],
        "snapshot_threshold": 10
    });
    let schema_response: Value = client
        .post(format!("{admin_base}/schemas"))
        .bearer_auth(&admin_token)
        .json(&schema_payload)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(schema_response["aggregate"], created_aggregate);
    assert!(
        schema_response["events"]["created"].is_object(),
        "created event should be present in schema"
    );

    // Retrieve the newly created schema.
    let created_lookup: Value = client
        .get(format!("{admin_base}/schemas/{created_aggregate}"))
        .bearer_auth(&admin_token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        created_lookup["aggregate"], created_aggregate,
        "created schema lookup should succeed"
    );

    // Update the pre-seeded schema to add fields, lock it, and adjust snapshot threshold.
    let schema_update_payload = json!({
        "snapshot_threshold": 25,
        "locked": true,
        "field_lock": {
            "field": "secret",
            "lock": true
        },
        "add_fields": {
            "updated": ["status"]
        }
    });
    let updated_schema: Value = client
        .patch(format!("{admin_base}/schemas/{managed_aggregate}"))
        .bearer_auth(&admin_token)
        .json(&schema_update_payload)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        updated_schema["snapshot_threshold"].as_u64(),
        Some(25),
        "schema snapshot threshold should reflect update"
    );
    assert_eq!(
        updated_schema["locked"], true,
        "schema should be marked as locked after update"
    );
    assert!(
        updated_schema["field_locks"]
            .as_array()
            .expect("field locks should be array")
            .iter()
            .any(|field| field == "secret"),
        "schema should lock the specified field"
    );
    assert!(
        updated_schema["events"]["updated"]["fields"]
            .as_array()
            .expect("updated event should include fields list")
            .iter()
            .any(|field| field == "status"),
        "schema should include newly added field on updated event"
    );

    // Remove an event from the schema and ensure it is no longer present.
    let schema_after_removal: Value = client
        .delete(format!(
            "{admin_base}/schemas/{managed_aggregate}/events/created"
        ))
        .bearer_auth(&admin_token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(
        !schema_after_removal["events"]
            .as_object()
            .expect("schema events should be object")
            .contains_key("created"),
        "removed event should not appear in schema"
    );

    // Configure a remote endpoint.
    let remote_public_key = STANDARD.encode([3u8; 32]);
    let remote_payload = json!({
        "endpoint": "tcp://0.0.0.0:6363",
        "public_key": remote_public_key
    });
    let remote_response = client
        .put(format!("{admin_base}/remotes/primary"))
        .bearer_auth(&admin_token)
        .json(&remote_payload)
        .send()
        .await?;
    if remote_response.status().is_success() {
        assert!(
            matches!(
                remote_response.status(),
                StatusCode::CREATED | StatusCode::OK
            ),
            "remote upsert should return 200 or 201"
        );
        let remote_created: Value = remote_response.json().await.unwrap_or_else(|err| {
            panic!("remote upsert response was not valid JSON: {err}");
        });
        match remote_created {
            Value::Object(_) => {
                assert_eq!(
                    remote_created["endpoint"], "tcp://0.0.0.0:6363",
                    "remote endpoint should match configuration"
                );
            }
            Value::Null => {
                // Some environments may serialize no body on success; the follow-up fetch will validate.
            }
            other => panic!("unexpected remote upsert payload: {other:?}"),
        }
    } else {
        let status = remote_response.status();
        let body = remote_response.text().await.unwrap_or_default();
        panic!("remote upsert failed: status={status} body={body}");
    }

    let remotes: Value = client
        .get(format!("{admin_base}/remotes"))
        .bearer_auth(&admin_token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(
        remotes
            .get("primary")
            .and_then(|remote| remote.get("endpoint"))
            .map(|endpoint| endpoint == "tcp://0.0.0.0:6363")
            .unwrap_or(false),
        "remote configuration should be persisted"
    );

    // Delete the remote configuration and ensure it no longer exists.
    let delete_remote = client
        .delete(format!("{admin_base}/remotes/primary"))
        .bearer_auth(&admin_token)
        .send()
        .await?;
    assert_eq!(
        delete_remote.status(),
        StatusCode::NO_CONTENT,
        "remote delete should return NO_CONTENT"
    );
    let remotes_after_delete: Value = client
        .get(format!("{admin_base}/remotes"))
        .bearer_auth(&admin_token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(
        remotes_after_delete.get("primary").is_none(),
        "remote should be removed after deletion"
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
    let plugin_response = client
        .put(format!("{admin_base}/plugins/logger"))
        .bearer_auth(&admin_token)
        .json(&plugin_payload)
        .send()
        .await?;
    assert_eq!(
        plugin_response.status(),
        StatusCode::CREATED,
        "new plugin should return CREATED"
    );
    let plugin_created: Value = plugin_response.json().await?;
    assert_eq!(
        plugin_created["enabled"], true,
        "plugin should start enabled when configured as such"
    );
    assert_eq!(
        plugin_created["config"]["type"], "log",
        "plugin config should identify as log"
    );

    let plugins_response: Value = client
        .get(format!("{admin_base}/plugins"))
        .bearer_auth(&admin_token)
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

    // Disable and then enable the plugin via dedicated endpoints.
    let disabled_plugin: Value = client
        .post(format!("{admin_base}/plugins/logger/disable"))
        .bearer_auth(&admin_token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        disabled_plugin["enabled"], false,
        "plugin disable endpoint should mark plugin as disabled"
    );
    let enabled_plugin: Value = client
        .post(format!("{admin_base}/plugins/logger/enable"))
        .bearer_auth(&admin_token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        enabled_plugin["enabled"], true,
        "plugin enable endpoint should mark plugin as enabled"
    );

    // Disable once more so removal is allowed.
    let disabled_before_delete: Value = client
        .post(format!("{admin_base}/plugins/logger/disable"))
        .bearer_auth(&admin_token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        disabled_before_delete["enabled"], false,
        "plugin should report disabled prior to deletion"
    );

    // Remove the plugin entirely and confirm it no longer appears in listings.
    let delete_plugin_response = client
        .delete(format!("{admin_base}/plugins/logger"))
        .bearer_auth(&admin_token)
        .send()
        .await?;
    assert_eq!(
        delete_plugin_response.status(),
        StatusCode::NO_CONTENT,
        "plugin delete should return NO_CONTENT"
    );
    let plugins_after_delete: Value = client
        .get(format!("{admin_base}/plugins"))
        .bearer_auth(&admin_token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(
        !plugins_after_delete
            .as_array()
            .expect("plugins list should be an array")
            .iter()
            .any(|plugin| plugin["name"] == "logger"),
        "plugin should not appear in list after deletion"
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

async fn wait_for_admin(base_url: &str, token: &str) -> TestResult<()> {
    let client = Client::new();
    for _ in 0..40 {
        let resp = client
            .get(format!("{base_url}/tokens"))
            .bearer_auth(token)
            .send()
            .await;
        if let Ok(resp) = resp {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    Err("admin API did not become ready in time".into())
}
