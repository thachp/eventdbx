use std::time::Instant;

use anyhow::{Result, anyhow};
use axum::{
    body::Body,
    extract::MatchedPath,
    http::{HeaderValue, Request, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, histogram};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use once_cell::sync::OnceCell;

static PROMETHEUS_HANDLE: OnceCell<PrometheusHandle> = OnceCell::new();
static START_TIME: OnceCell<Instant> = OnceCell::new();

pub fn init() -> Result<()> {
    if PROMETHEUS_HANDLE.get().is_some() {
        return Ok(());
    }

    let builder = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full("eventdbx_http_request_duration_seconds".into()),
            &[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
        )
        .map_err(|err| anyhow!("failed to configure prometheus exporter: {err}"))?;
    let handle = builder
        .install_recorder()
        .map_err(|err| anyhow!("failed to initialise prometheus recorder: {err}"))?;

    describe_counter!(
        "eventdbx_http_requests_total",
        "Total number of HTTP requests processed by the EventDBX server."
    );
    describe_histogram!(
        "eventdbx_http_request_duration_seconds",
        "HTTP request latency observed by the EventDBX server."
    );
    describe_counter!(
        "eventdbx_plugin_jobs_enqueued_total",
        "Total number of plugin jobs enqueued for delivery."
    );
    describe_counter!(
        "eventdbx_plugin_jobs_completed_total",
        "Total number of plugin jobs completed successfully."
    );
    describe_counter!(
        "eventdbx_plugin_jobs_failed_total",
        "Total number of plugin jobs that failed."
    );
    describe_gauge!(
        "eventdbx_plugin_queue_jobs",
        "Current number of plugin jobs by state."
    );
    describe_counter!(
        "eventdbx_store_operations_total",
        "Total number of operations executed by the event store."
    );
    describe_histogram!(
        "eventdbx_store_operation_duration_seconds",
        "Duration of operations executed by the event store."
    );
    describe_counter!(
        "eventdbx_capnp_control_requests_total",
        "Total number of Cap'n Proto control requests handled over the TCP socket."
    );
    describe_histogram!(
        "eventdbx_capnp_control_request_duration_seconds",
        "Latency of Cap'n Proto control requests handled over the TCP socket."
    );
    describe_counter!(
        "eventdbx_capnp_replication_requests_total",
        "Total number of replication socket requests handled."
    );
    describe_histogram!(
        "eventdbx_capnp_replication_request_duration_seconds",
        "Latency of replication socket requests handled."
    );
    describe_counter!(
        "eventdbx_cli_proxy_commands_total",
        "Total number of CLI proxy shell-out commands executed."
    );
    describe_histogram!(
        "eventdbx_cli_proxy_command_duration_seconds",
        "Latency of CLI proxy shell-out command execution."
    );

    let _ = PROMETHEUS_HANDLE.set(handle);
    let _ = START_TIME.set(Instant::now());
    Ok(())
}

pub async fn metrics_handler() -> Response {
    if PROMETHEUS_HANDLE.get().is_none() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "metrics recorder not initialised",
        )
            .into_response();
    }

    let body = render_metrics();
    let headers = [(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4"),
    )];
    (StatusCode::OK, headers, body).into_response()
}

pub async fn track_http_metrics(req: Request<Body>, next: Next) -> Response {
    let method = req.method().clone();
    let matched_path = req
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_string());
    let path = matched_path.unwrap_or_else(|| req.uri().path().to_string());
    let method_label = method.as_str().to_owned();

    let start = Instant::now();
    let response = next.run(req).await;
    let latency = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();

    counter!(
        "eventdbx_http_requests_total",
        1,
        "method" => method_label.clone(),
        "path" => path.clone(),
        "status" => status.clone()
    );
    histogram!(
        "eventdbx_http_request_duration_seconds",
        latency,
        "method" => method_label,
        "path" => path,
        "status" => status
    );

    response
}

pub fn record_capnp_control_request(operation: &str, status: &str, duration: f64) {
    counter!(
        "eventdbx_capnp_control_requests_total",
        1,
        "operation" => operation.to_owned(),
        "status" => status.to_owned()
    );
    histogram!(
        "eventdbx_capnp_control_request_duration_seconds",
        duration,
        "operation" => operation.to_owned(),
        "status" => status.to_owned()
    );
}

pub fn record_capnp_replication_request(operation: &str, status: &str, duration: f64) {
    counter!(
        "eventdbx_capnp_replication_requests_total",
        1,
        "operation" => operation.to_owned(),
        "status" => status.to_owned()
    );
    histogram!(
        "eventdbx_capnp_replication_request_duration_seconds",
        duration,
        "operation" => operation.to_owned(),
        "status" => status.to_owned()
    );
}

pub fn record_cli_proxy_command(
    command: &str,
    status: &str,
    exit_code: Option<i32>,
    duration: f64,
) {
    let exit_code_label = exit_code
        .map(|code| code.to_string())
        .unwrap_or_else(|| "n/a".to_string());
    counter!(
        "eventdbx_cli_proxy_commands_total",
        1,
        "command" => command.to_owned(),
        "status" => status.to_owned(),
        "exit_code" => exit_code_label.clone()
    );
    histogram!(
        "eventdbx_cli_proxy_command_duration_seconds",
        duration,
        "command" => command.to_owned(),
        "status" => status.to_owned(),
        "exit_code" => exit_code_label
    );
}

pub fn render_metrics() -> String {
    if let Some(handle) = PROMETHEUS_HANDLE.get() {
        let mut body = handle.render();
        if let Some(start) = START_TIME.get() {
            let uptime = start.elapsed().as_secs_f64();
            body.push_str(&format!("eventdbx_uptime_seconds{{}} {}\n", uptime));
        }
        body
    } else {
        String::new()
    }
}
