use std::time::Instant;

use anyhow::{Result, anyhow};
use axum::{
    extract::{MatchedPath, Request as AxumRequest},
    http::{HeaderValue, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, histogram};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use once_cell::sync::OnceCell;

static PROMETHEUS_HANDLE: OnceCell<PrometheusHandle> = OnceCell::new();
static START_TIME: OnceCell<Instant> = OnceCell::new();

pub fn init() -> Result<()> {
    PROMETHEUS_HANDLE
        .get_or_try_init(|| {
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
                "eventdbx_cli_proxy_commands_total",
                "Total number of CLI proxy shell-out commands executed."
            );
            describe_histogram!(
                "eventdbx_cli_proxy_command_duration_seconds",
                "Latency of CLI proxy shell-out command execution."
            );

            let _ = START_TIME.set(Instant::now());
            Ok(handle)
        })
        .map(|_| ())
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

pub async fn track_http_metrics(req: AxumRequest, next: Next) -> Response {
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
        "method" => method_label.clone(),
        "path" => path.clone(),
        "status" => status.clone()
    )
    .increment(1);
    histogram!(
        "eventdbx_http_request_duration_seconds",
        "method" => method_label,
        "path" => path,
        "status" => status
    )
    .record(latency);

    response
}

pub fn record_capnp_control_request(operation: &str, status: &str, duration: f64) {
    let operation_label = operation.to_owned();
    let status_label = status.to_owned();
    counter!(
        "eventdbx_capnp_control_requests_total",
        "operation" => operation_label.clone(),
        "status" => status_label.clone()
    )
    .increment(1);
    histogram!(
        "eventdbx_capnp_control_request_duration_seconds",
        "operation" => operation_label,
        "status" => status_label
    )
    .record(duration);
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
    let command_label = command.to_owned();
    let status_label = status.to_owned();
    counter!(
        "eventdbx_cli_proxy_commands_total",
        "command" => command_label.clone(),
        "status" => status_label.clone(),
        "exit_code" => exit_code_label.clone()
    )
    .increment(1);
    histogram!(
        "eventdbx_cli_proxy_command_duration_seconds",
        "command" => command_label,
        "status" => status_label,
        "exit_code" => exit_code_label
    )
    .record(duration);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_includes_uptime_once_initialised() {
        init().expect("observability init should succeed");
        let metrics = render_metrics();
        assert!(
            metrics.contains("eventdbx_uptime_seconds"),
            "expected uptime metric in output, got: {metrics:?}"
        );
    }

    #[test]
    fn counter_metrics_are_recorded() {
        init().expect("observability init should succeed");
        metrics::counter!("test_observability_counter_total").increment(1);
        let metrics = render_metrics();
        assert!(
            metrics.contains("test_observability_counter_total"),
            "expected counter metric in output, got: {metrics:?}"
        );
    }

    #[tokio::test]
    async fn metrics_handler_renders_text_plain() {
        init().expect("observability init should succeed");
        let response = metrics_handler().await;
        assert_eq!(response.status(), StatusCode::OK);

        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .expect("content type header missing");
        assert_eq!(content_type, "text/plain; version=0.0.4");

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("metrics body to bytes");
        let body = String::from_utf8_lossy(&body_bytes);
        assert!(
            body.contains("eventdbx_uptime_seconds"),
            "expected uptime metric in response body, got: {body:?}"
        );
    }

    #[tokio::test]
    async fn router_layer_records_http_metrics() {
        use axum::{Router, body::Body, http::Request, middleware::from_fn, routing::get};
        use tower::util::ServiceExt;

        init().expect("observability init should succeed");

        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .layer(from_fn(track_http_metrics));

        let request = Request::builder()
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let second = Request::builder()
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(second).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("metrics body to bytes");
        let body = String::from_utf8_lossy(&body_bytes);
        metrics::counter!("test_snapshot_total").increment(1);
        let snapshot = render_metrics();
        assert!(
            snapshot.contains("test_snapshot_total"),
            "expected manual counter to appear in snapshot, got: {snapshot:?}"
        );
        assert!(
            body.contains("eventdbx_http_requests_total"),
            "expected http metrics in response body, got: {body:?}"
        );
    }
}
