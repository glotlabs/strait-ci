use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use axum::{
    Router,
    body::Body,
    extract::{Path as AxumPath, State},
    http::{HeaderMap, HeaderValue, Request, StatusCode},
    routing::{get, post},
};
use janus_lib::{HEADER_IDEMPOTENCY_KEY, RunnerRoute, RunnerRouteTemplate};
use serde_json::{Map, json};
use sha2::Digest;
use tokio::time::{Duration, sleep};
use tower::util::ServiceExt;
use uuid::Uuid;

use super::{
    JobCreatedResponse, JobDefinitionResponse, JobLogsResponse, JobMetadata, JobStatus,
    JobStatusResponse, JobStore, cancel_job, create_job as create_job_impl, get_job, get_job_logs,
    list_jobs,
    models::{FailureCategory, JobOutput, TerminalReason},
};
use crate::{
    AppState,
    artifacts::ArtifactStore,
    auth::AuthStore,
    config::{ArtifactsConfig, AuthConfig, Config, JobsConfig, ServerConfig},
    manifest::ManifestStore,
};

static TEST_IDEMPOTENCY_COUNTER: AtomicU64 = AtomicU64::new(1);

async fn create_job(
    auth: crate::auth::Authorized<crate::auth::JobsRun>,
    state: State<crate::AppState>,
    path: AxumPath<String>,
    mut headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, axum::Json<JobCreatedResponse>), super::JobError> {
    headers.entry(HEADER_IDEMPOTENCY_KEY).or_insert_with(|| {
        let value = TEST_IDEMPOTENCY_COUNTER.fetch_add(1, Ordering::Relaxed);
        HeaderValue::from_str(&format!("test_dispatch_{value:016x}"))
            .expect("idempotency header should be valid")
    });
    create_job_impl(auth, state, path, headers, body).await
}

#[tokio::test]
async fn creates_job_metadata_for_valid_request() {
    let temp = temp_dir("job_create");
    let state = test_state(&temp);
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "commit": "abc123",
                        "branch": "main"
                    })
                    .to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let created: JobCreatedResponse = serde_json::from_slice(&body).expect("created job body");
    let metadata_path = temp
        .join("jobs")
        .join(&created.job_id)
        .join("metadata.json");
    let metadata: JobMetadata =
        serde_json::from_slice(&fs::read(metadata_path).expect("metadata should be written"))
            .expect("metadata should parse");

    assert_eq!(metadata.name, "build-app");
    assert_eq!(metadata.inputs["commit"], "abc123");
}

#[tokio::test]
async fn redacts_sensitive_inputs_in_persisted_metadata() {
    let temp = temp_dir("job_sensitive_redaction");
    let state = test_state_from_manifest(
        &temp,
        "build-app",
        r#"
[inputs.token]
type = "string"
required = true
sensitive = true
"#,
        "",
        "#!/bin/sh\nexit 0\n",
        600,
        50,
        64,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "token": "super-secret-value" }).to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let created: JobCreatedResponse = serde_json::from_slice(&body).expect("created job body");
    let metadata_path = temp
        .join("jobs")
        .join(&created.job_id)
        .join("metadata.json");
    let metadata: JobMetadata =
        serde_json::from_slice(&fs::read(metadata_path).expect("metadata should be written"))
            .expect("metadata should parse");

    assert_eq!(metadata.inputs["token"], "[REDACTED]");
}

#[tokio::test]
async fn reuses_existing_job_for_same_idempotency_key() {
    let temp = temp_dir("job_idempotent_replay");
    let state = test_state(&temp);
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job_impl))
        .with_state(state);

    let request_body = json!({
        "commit": "abc123",
        "branch": "main"
    })
    .to_string();

    let first = app
        .clone()
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("x-idempotency-key", "dispatch_replay_1")
                .header("content-type", "application/json")
                .body(Body::from(request_body.clone()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let second = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("x-idempotency-key", "dispatch_replay_1")
                .header("content-type", "application/json")
                .body(Body::from(request_body))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(first.status(), StatusCode::CREATED);
    assert_eq!(second.status(), StatusCode::CREATED);

    let first = read_created_job(first).await;
    let second = read_created_job(second).await;
    assert_eq!(first.job_id, second.job_id);
}

#[tokio::test]
async fn rejects_reusing_idempotency_key_for_different_request() {
    let temp = temp_dir("job_idempotent_conflict");
    let state = test_state(&temp);
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job_impl))
        .with_state(state);

    let first = app
        .clone()
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("x-idempotency-key", "dispatch_conflict_1")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "commit": "abc123",
                        "branch": "main"
                    })
                    .to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let second = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("x-idempotency-key", "dispatch_conflict_1")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "commit": "def456",
                        "branch": "main"
                    })
                    .to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(first.status(), StatusCode::CREATED);
    assert_eq!(second.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn rejects_missing_idempotency_key() {
    let temp = temp_dir("job_missing_idempotency_key");
    let state = test_state(&temp);
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job_impl))
        .with_state(state);

    let response = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "commit": "abc123",
                        "branch": "main"
                    })
                    .to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rejects_missing_required_input() {
    let temp = temp_dir("job_missing_input");
    let state = test_state(&temp);
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rejects_unknown_input() {
    let temp = temp_dir("job_unknown_input");
    let state = test_state(&temp);
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "commit": "abc123",
                        "branch": "main",
                        "extra": "nope"
                    })
                    .to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn accepts_job_request_with_body_within_limit() {
    let temp = temp_dir("job_request_within_limit");
    let state = test_state_with_request_body_limit(&temp, 1);
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let branch = "b".repeat(900);
    let response = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "commit": "abc123",
                        "branch": branch
                    })
                    .to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn rejects_job_request_body_over_limit() {
    let temp = temp_dir("job_request_over_limit");
    let state = test_state_with_request_body_limit(&temp, 1);
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let branch = "b".repeat(1500);
    let response = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "commit": "abc123",
                        "branch": branch
                    })
                    .to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn rate_limits_job_creation_per_token() {
    let temp = temp_dir("job_rate_limit");
    let state = test_state_with_job_run_rate_limit(&temp, 1);
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let first = app
        .clone()
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "commit": "abc123",
                        "branch": "main"
                    })
                    .to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    let second = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "commit": "abc123",
                        "branch": "main"
                    })
                    .to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(first.status(), StatusCode::CREATED);
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        second
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok()),
        Some("60")
    );
}

#[tokio::test]
async fn rejects_string_input_over_max_length() {
    let temp = temp_dir("job_string_max_length");
    let state = test_state_from_manifest(
        &temp,
        "build-app",
        r#"
[inputs.commit]
type = "string"
required = true
max_length = 6
"#,
        "",
        "#!/bin/sh\nexit 0\n",
        600,
        50,
        64,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "abcdefg" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rejects_string_input_that_fails_pattern() {
    let temp = temp_dir("job_string_pattern");
    let state = test_state_from_manifest(
        &temp,
        "build-app",
        r#"
[inputs.commit]
type = "string"
required = true
pattern = "^[a-f0-9]+$"
"#,
        "",
        "#!/bin/sh\nexit 0\n",
        600,
        50,
        64,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "not-a-sha" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rejects_json_input_over_max_json_bytes() {
    let temp = temp_dir("job_json_max_bytes");
    let state = test_state_from_manifest(
        &temp,
        "build-app",
        r#"
[inputs.payload]
type = "json"
required = true
max_json_bytes = 16
"#,
        "",
        "#!/bin/sh\nexit 0\n",
        600,
        50,
        64,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "payload": { "long": "value that exceeds the limit" } }).to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn accepts_structured_json_input() {
    let temp = temp_dir("job_json_accepts_structured_value");
    let state = test_state_from_manifest(
        &temp,
        "build-app",
        r#"
[inputs.payload]
type = "json"
required = true
"#,
        "",
        "#!/bin/sh\nexit 0\n",
        600,
        50,
        64,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "payload": {
                            "commit": "abc123",
                            "flags": ["fast", "clean"]
                        }
                    })
                    .to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn rejects_null_json_input() {
    let temp = temp_dir("job_json_rejects_null");
    let state = test_state_from_manifest(
        &temp,
        "build-app",
        r#"
[inputs.payload]
type = "json"
required = true
"#,
        "",
        "#!/bin/sh\nexit 0\n",
        600,
        50,
        64,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "payload": null }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn resolves_artifact_inputs() {
    let temp = temp_dir("job_artifact_input");
    let state = test_state_with_artifact_manifest(&temp);
    let artifact_id = store_artifact(&state.artifacts, b"src");
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::post("/jobs/build-with-artifact/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "source": artifact_id
                    })
                    .to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let created: JobCreatedResponse = serde_json::from_slice(&body).expect("created job body");
    let metadata_path = temp
        .join("jobs")
        .join(&created.job_id)
        .join("metadata.json");
    let metadata: JobMetadata =
        serde_json::from_slice(&fs::read(metadata_path).expect("metadata should be written"))
            .expect("metadata should parse");

    assert!(metadata.resolved_artifacts["source"].ends_with("/blob"));
}

#[tokio::test]
async fn executes_successful_script_and_cleans_workdir() {
    let temp = temp_dir("job_execute_success");
    let state = test_state_with_script(
        &temp,
        "build-app",
        r#"#!/bin/sh
printf '%s' "$INPUT_COMMIT"
printf '%s' "$INPUT_SOURCE" >&2
exit 0
"#,
        600,
    );
    let artifact_id = store_artifact(&state.artifacts, b"src");
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "commit": "abc123",
                        "source": artifact_id
                    })
                    .to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    let created = read_created_job(response).await;
    let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;

    assert_eq!(metadata.status, JobStatus::Success);
    assert_eq!(metadata.terminal_reason, Some(TerminalReason::Success));
    assert_eq!(metadata.failure_category, None);
    assert_eq!(metadata.exit_code, Some(0));
    assert!(metadata.duration_ms.is_some());
    assert_eq!(metadata.output_metadata.stdout.bytes, 6);
    assert!(!metadata.output_metadata.stdout.truncated);
    assert_eq!(
        fs::read_to_string(temp.join("jobs").join(&created.job_id).join("stdout.log"))
            .expect("stdout log"),
        "abc123"
    );
    assert!(
        fs::read_to_string(temp.join("jobs").join(&created.job_id).join("stderr.log"))
            .expect("stderr log")
            .ends_with("/blob")
    );
    assert!(
        !temp
            .join("jobs")
            .join(&created.job_id)
            .join("work")
            .exists()
    );
}

#[tokio::test]
async fn marks_failed_script_as_failed() {
    let temp = temp_dir("job_execute_failed");
    let state = test_state_with_script(
        &temp,
        "build-app",
        "#!/bin/sh\nprintf 'boom' >&2\nexit 7\n",
        600,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    let created = read_created_job(response).await;
    let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;

    assert_eq!(metadata.status, JobStatus::Failed);
    assert_eq!(metadata.terminal_reason, Some(TerminalReason::ExitCode));
    assert_eq!(metadata.failure_category, Some(FailureCategory::Job));
    assert_eq!(metadata.exit_code, Some(7));
    assert!(
        temp.join("jobs")
            .join(&created.job_id)
            .join("work")
            .exists()
    );
}

#[tokio::test]
async fn times_out_long_running_script() {
    let temp = temp_dir("job_execute_timeout");
    let state = test_state_with_script(&temp, "build-app", "#!/bin/sh\nsleep 2\n", 1);
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    let created = read_created_job(response).await;
    let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;

    assert_eq!(metadata.status, JobStatus::TimedOut);
    assert_eq!(metadata.terminal_reason, Some(TerminalReason::Timeout));
    assert_eq!(metadata.failure_category, Some(FailureCategory::Timeout));
    assert_eq!(metadata.exit_code, None);
}

#[tokio::test]
async fn registers_declared_outputs_as_artifacts() {
    let temp = temp_dir("job_output_artifact");
    let state = test_state_from_manifest(
        &temp,
        "build-app",
        r#"
[inputs.commit]
type = "string"
required = false
"#,
        r#"
[outputs.app]
type = "artifact"
path = "app.tar.gz"
required = true
"#,
        "#!/bin/sh\nprintf 'bundle' > \"$JANUS_OUTPUT_DIR/app.tar.gz\"\n",
        600,
        50,
        64,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let created = read_created_job(
        app.oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed"),
    )
    .await;
    let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;

    assert_eq!(metadata.status, JobStatus::Success);
    let output = &metadata.outputs["app"];
    let JobOutput::Artifact {
        artifact_id, size, ..
    } = output
    else {
        panic!("expected artifact output");
    };
    assert_eq!(*size, 6);
    let stored = fs::read_to_string(temp.join("artifacts").join(artifact_id).join("blob"))
        .expect("stored output artifact");
    assert_eq!(stored, "bundle");
}

#[tokio::test]
async fn registers_typed_outputs() {
    let temp = temp_dir("job_output_typed");
    let state = test_state_from_manifest(
        &temp,
        "build-app",
        "",
        r#"
[outputs.version]
type = "string"
path = "version.txt"
required = true

[outputs.build_number]
type = "integer"
path = "build-number.txt"
required = true

[outputs.published]
type = "boolean"
path = "published.txt"
required = true

[outputs.metadata]
type = "json"
path = "metadata.json"
required = true
"#,
        "#!/bin/sh\nprintf 'v1.2.3' > \"$JANUS_OUTPUT_DIR/version.txt\"\nprintf '42' > \"$JANUS_OUTPUT_DIR/build-number.txt\"\nprintf 'true' > \"$JANUS_OUTPUT_DIR/published.txt\"\nprintf '{\"commit\":\"abc123\"}' > \"$JANUS_OUTPUT_DIR/metadata.json\"\n",
        600,
        50,
        64,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let created = read_created_job(
        app.oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from("{}".to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed"),
    )
    .await;
    let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;

    assert_eq!(
        metadata.outputs["version"],
        JobOutput::String {
            value: "v1.2.3".to_string()
        }
    );
    assert_eq!(
        metadata.outputs["build_number"],
        JobOutput::Integer { value: 42 }
    );
    assert_eq!(
        metadata.outputs["published"],
        JobOutput::Boolean { value: true }
    );
    assert_eq!(
        metadata.outputs["metadata"],
        JobOutput::Json {
            value: json!({ "commit": "abc123" }),
        }
    );
}

#[tokio::test]
async fn fails_successful_script_when_required_output_is_missing() {
    let temp = temp_dir("job_output_missing");
    let state = test_state_from_manifest(
        &temp,
        "build-app",
        r#"
[inputs.commit]
type = "string"
required = false
"#,
        r#"
[outputs.app]
type = "artifact"
path = "app.tar.gz"
required = true
"#,
        "#!/bin/sh\nexit 0\n",
        600,
        50,
        64,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let created = read_created_job(
        app.oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed"),
    )
    .await;
    let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;

    assert_eq!(metadata.status, JobStatus::Failed);
    assert!(
        fs::read_to_string(temp.join("jobs").join(&created.job_id).join("stderr.log"))
            .expect("stderr log")
            .contains("required output app is missing")
    );
}

#[tokio::test]
async fn reads_job_status_over_http() {
    let temp = temp_dir("job_status_http");
    let state = test_state_with_script(&temp, "build-app", "#!/bin/sh\nexit 0\n", 600);
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .route("/runs/{job_id}", get(get_job))
        .with_state(state);

    let created = read_created_job(
        app.clone()
            .oneshot(
                Request::post("/jobs/build-app/runs")
                    .header("x-janus-key-id", "runner")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed"),
    )
    .await;

    let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;

    let response = app
        .oneshot(
            Request::get(format!("/runs/{}", created.job_id))
                .header("x-janus-key-id", "runner")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let fetched: JobStatusResponse = serde_json::from_slice(&body).expect("job metadata body");

    assert_eq!(fetched.job_id, created.job_id);
    assert_eq!(fetched.status, metadata.status);
    assert_eq!(fetched.finished_at, metadata.finished_at);
}

#[tokio::test]
async fn reads_job_logs_over_http() {
    let temp = temp_dir("job_logs_http");
    let state = test_state_with_script(
        &temp,
        "build-app",
        "#!/bin/sh\nprintf 'out'\nprintf 'err' >&2\n",
        600,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .route("/runs/{job_id}/logs", get(get_job_logs))
        .with_state(state);

    let created = read_created_job(
        app.clone()
            .oneshot(
                Request::post("/jobs/build-app/runs")
                    .header("x-janus-key-id", "runner")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed"),
    )
    .await;

    let _ = wait_for_terminal_metadata(&temp, &created.job_id).await;

    let response = app
        .oneshot(
            Request::get(format!("/runs/{}/logs", created.job_id))
                .header("x-janus-key-id", "runner")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let logs: JobLogsResponse = serde_json::from_slice(&body).expect("job logs body");

    assert_eq!(logs.stdout, "out");
    assert_eq!(logs.stderr, "err");
}

#[tokio::test]
async fn rejects_invalid_job_id_in_status_route() {
    let temp = temp_dir("job_status_invalid_id");
    let state = test_state(&temp);
    let app = Router::new()
        .route("/runs/{job_id}", get(get_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::get("/runs/not-a-job-id")
                .header("x-janus-key-id", "runner")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rejects_invalid_job_id_in_logs_route() {
    let temp = temp_dir("job_logs_invalid_id");
    let state = test_state(&temp);
    let app = Router::new()
        .route("/runs/{job_id}/logs", get(get_job_logs))
        .with_state(state);

    let response = app
        .oneshot(
            Request::get("/runs/not-a-job-id/logs")
                .header("x-janus-key-id", "runner")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn lists_job_definitions_over_http() {
    let temp = temp_dir("job_list_http");
    let state = test_state(&temp);
    let app = Router::new()
        .route("/jobs", get(list_jobs))
        .with_state(state);

    let response = app
        .oneshot(
            Request::get("/jobs")
                .header("x-janus-key-id", "runner")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let jobs: Vec<JobDefinitionResponse> =
        serde_json::from_slice(&body).expect("job definitions body");

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].name, "build-app");
    assert_eq!(jobs[0].timeout_seconds, 600);
    assert!(jobs[0].inputs.contains_key("commit"));
    assert!(jobs[0].inputs["commit"].required);
}

#[tokio::test]
async fn runner_http_contract_serializes_shared_dtos() {
    let temp = temp_dir("runner_http_contract");
    let state = test_state(&temp);
    let app = Router::new()
        .route(
            RunnerRouteTemplate::Capabilities.path(),
            get(|| async { axum::Json(janus_lib::RunnerCapabilitiesResponse::current()) }),
        )
        .route(RunnerRouteTemplate::Jobs.path(), get(list_jobs))
        .route(RunnerRouteTemplate::JobRuns.path(), post(create_job_impl))
        .route(RunnerRouteTemplate::Run.path(), get(get_job))
        .route(RunnerRouteTemplate::RunLogs.path(), get(get_job_logs))
        .with_state(state);

    let response = app
        .clone()
        .oneshot(
            Request::get(RunnerRoute::Jobs.path())
                .header("x-janus-key-id", "runner")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let definitions: Vec<janus_lib::JobDefinitionResponse> =
        serde_json::from_slice(&body).expect("shared job definitions");
    assert_eq!(definitions[0].name, "build-app");

    let response = app
        .clone()
        .oneshot(
            Request::post(
                RunnerRoute::JobRuns {
                    job_name: "build-app",
                }
                .path(),
            )
            .header("x-janus-key-id", "runner")
            .header("content-type", "application/json")
            .header(HEADER_IDEMPOTENCY_KEY, "contract_dispatch_1")
            .body(Body::from(
                json!({"commit":"abc123","branch":"main"}).to_string(),
            ))
            .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let created: janus_lib::JobCreatedResponse =
        serde_json::from_slice(&body).expect("shared create response");
    assert_eq!(created.status, JobStatus::Running);

    let response = app
        .clone()
        .oneshot(
            Request::get(
                RunnerRoute::Run {
                    job_id: &created.job_id,
                }
                .path(),
            )
            .header("x-janus-key-id", "runner")
            .body(Body::empty())
            .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let status: janus_lib::JobStatusResponse =
        serde_json::from_slice(&body).expect("shared status response");
    assert_eq!(status.job_id, created.job_id);

    let response = app
        .oneshot(
            Request::get(
                RunnerRoute::RunLogs {
                    job_id: &created.job_id,
                }
                .path(),
            )
            .header("x-janus-key-id", "runner")
            .body(Body::empty())
            .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let _logs: janus_lib::JobLogsResponse =
        serde_json::from_slice(&body).expect("shared logs response");
}

#[tokio::test]
async fn lists_sensitive_input_metadata_over_http() {
    let temp = temp_dir("job_list_sensitive_input");
    let state = test_state_from_manifest(
        &temp,
        "build-app",
        r#"
[inputs.token]
type = "string"
required = true
sensitive = true
"#,
        "",
        "#!/bin/sh\nexit 0\n",
        600,
        50,
        64,
    );
    let app = Router::new()
        .route("/jobs", get(list_jobs))
        .with_state(state);

    let response = app
        .oneshot(
            Request::get("/jobs")
                .header("x-janus-key-id", "runner")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let jobs: Vec<JobDefinitionResponse> =
        serde_json::from_slice(&body).expect("job definitions body");

    assert!(jobs[0].inputs["token"].sensitive);
}

#[tokio::test]
async fn cancels_running_job_over_http() {
    let temp = temp_dir("job_cancel_http");
    let state = test_state_with_script(&temp, "build-app", "#!/bin/sh\nsleep 5\n", 600);
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .route("/runs/{job_id}", get(get_job).delete(cancel_job))
        .with_state(state);

    let created = read_created_job(
        app.clone()
            .oneshot(
                Request::post("/jobs/build-app/runs")
                    .header("x-janus-key-id", "runner")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed"),
    )
    .await;

    let cancel = app
        .oneshot(
            Request::delete(format!("/runs/{}", created.job_id))
                .header("x-janus-key-id", "runner")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(cancel.status(), StatusCode::ACCEPTED);
    let metadata = read_metadata(&temp, &created.job_id);
    assert!(matches!(
        metadata.status,
        JobStatus::CancelRequested | JobStatus::Canceling | JobStatus::Canceled
    ));

    let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;
    assert_eq!(metadata.status, JobStatus::Canceled);
    assert_eq!(metadata.exit_code, None);
}

#[tokio::test]
async fn rejects_invalid_job_id_in_cancel_route() {
    let temp = temp_dir("job_cancel_invalid_id");
    let state = test_state(&temp);
    let app = Router::new()
        .route("/runs/{job_id}", get(get_job).delete(cancel_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::delete("/runs/not-a-job-id")
                .header("x-janus-key-id", "runner")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[cfg(unix)]
#[tokio::test]
async fn cancel_kills_child_process_tree() {
    let temp = temp_dir("job_cancel_process_tree");
    let state = test_state_with_script(
        &temp,
        "build-app",
        "#!/bin/sh\nsleep 30 &\necho $! > \"$JANUS_WORKDIR/child.pid\"\nwait\n",
        600,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .route("/runs/{job_id}", get(get_job).delete(cancel_job))
        .with_state(state);

    let created = read_created_job(
        app.clone()
            .oneshot(
                Request::post("/jobs/build-app/runs")
                    .header("x-janus-key-id", "runner")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed"),
    )
    .await;

    let child_pid_path = temp
        .join("jobs")
        .join(&created.job_id)
        .join("work")
        .join("child.pid");
    let child_pid = wait_for_file(&child_pid_path).await;

    let cancel = app
        .oneshot(
            Request::delete(format!("/runs/{}", created.job_id))
                .header("x-janus-key-id", "runner")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(cancel.status(), StatusCode::ACCEPTED);

    let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;
    assert_eq!(metadata.status, JobStatus::Canceled);
    assert!(!unix_process_exists(
        child_pid.trim().parse().expect("child pid should parse")
    ));
}

#[tokio::test]
async fn fails_job_when_log_limit_is_exceeded() {
    let temp = temp_dir("job_log_limit");
    let state = test_state_with_script_and_log_limit(
        &temp,
        "build-app",
        "#!/bin/sh\nprintf 'a'\n",
        600,
        0,
        64,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let created = read_created_job(
        app.oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed"),
    )
    .await;
    let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;

    assert_eq!(metadata.status, JobStatus::Failed);
    assert_eq!(metadata.exit_code, None);
    assert_eq!(
        fs::read(temp.join("jobs").join(&created.job_id).join("stdout.log"))
            .expect("stdout should read")
            .len(),
        0
    );
    assert!(
        fs::read_to_string(temp.join("jobs").join(&created.job_id).join("stderr.log"))
            .expect("stderr should read")
            .contains("job stdout log exceeded configured limit")
    );
}

#[tokio::test]
async fn job_process_sees_only_deliberate_environment() {
    let temp = temp_dir("job_env_isolation");
    let state = test_state_with_script(&temp, "build-app", "#!/bin/sh\nenv | sort\n", 600);
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let created = read_created_job(
        app.oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed"),
    )
    .await;
    let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;

    assert_eq!(metadata.status, JobStatus::Success);
    let stdout = fs::read_to_string(temp.join("jobs").join(&created.job_id).join("stdout.log"))
        .expect("stdout log");
    assert!(stdout.contains("JANUS_JOB_NAME=build-app"));
    assert!(stdout.contains("INPUT_COMMIT=abc123"));
    assert!(stdout.contains("PATH=/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"));
    assert!(!stdout.contains("HOME="));
}

#[tokio::test]
async fn reserved_system_input_is_available_without_manifest_declaration() {
    let temp = temp_dir("job_system_input");
    let state = test_state_with_script(
        &temp,
        "build-app",
        "#!/bin/sh\nprintf '%s\\n%s\\n%s' \"$INPUT_JANUS_JOB_RUN_ID\" \"$INPUT_OUTPUT_DIR\" \"$INPUT_METADATA_PATH\"\n",
        600,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let created = read_created_job(
        app.oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "commit": "abc123",
                        "janus_job_run_id": "server-job-1",
                        "output_dir": "server-output-dir",
                        "metadata_path": "server-metadata-path"
                    })
                    .to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed"),
    )
    .await;
    let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;

    assert_eq!(metadata.status, JobStatus::Success);
    assert_eq!(metadata.inputs["janus_job_run_id"], "server-job-1");
    assert_eq!(metadata.inputs["output_dir"], "server-output-dir");
    assert_eq!(metadata.inputs["metadata_path"], "server-metadata-path");
    assert_eq!(
        fs::read_to_string(temp.join("jobs").join(&created.job_id).join("stdout.log"))
            .expect("stdout should read"),
        "server-job-1\nserver-output-dir\nserver-metadata-path"
    );
}

#[tokio::test]
async fn sensitive_input_is_available_to_job_env_but_redacted_on_disk() {
    let temp = temp_dir("job_sensitive_env");
    let state = test_state_from_manifest(
        &temp,
        "build-app",
        r#"
[inputs.token]
type = "string"
required = true
sensitive = true
"#,
        "",
        "#!/bin/sh\nprintf '%s' \"$INPUT_TOKEN\"\n",
        600,
        50,
        64,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let created = read_created_job(
        app.oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "token": "super-secret-value" }).to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed"),
    )
    .await;
    let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;

    assert_eq!(metadata.status, JobStatus::Success);
    assert_eq!(metadata.inputs["token"], "[REDACTED]");
    assert_eq!(
        fs::read_to_string(temp.join("jobs").join(&created.job_id).join("stdout.log"))
            .expect("stdout should read"),
        "super-secret-value"
    );
}

#[tokio::test]
async fn begin_shutdown_cancels_active_jobs() {
    let temp = temp_dir("job_begin_shutdown");
    let state = test_state_with_script(&temp, "build-app", "#!/bin/sh\nsleep 5\n", 600);
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state.clone());

    let created = read_created_job(
        app.oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed"),
    )
    .await;

    let canceled = state.jobs.begin_shutdown();
    assert_eq!(canceled, 1);

    let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;
    assert_eq!(metadata.status, JobStatus::Canceled);
}

#[tokio::test]
async fn rejects_new_jobs_after_shutdown_starts() {
    let temp = temp_dir("job_reject_shutdown");
    let state = test_state(&temp);
    state.jobs.begin_shutdown();

    let error = state
        .jobs
        .create_job(
            "build-app",
            "test_shutdown_key",
            br#"{"commit":"abc123","branch":"main"}"#,
            json!({
                "commit": "abc123",
                "branch": "main"
            })
            .as_object()
            .expect("body should be object")
            .clone(),
            &state.manifests,
            &state.artifacts,
            state.config.jobs.default_log_limit_mb,
            state.config.jobs.cleanup_successful_workdirs,
            state.config.jobs.keep_failed_workdirs,
        )
        .expect_err("create_job should reject while shutting down");

    assert!(matches!(error, super::JobError::ShuttingDown));
}

#[test]
fn recovers_running_jobs_on_startup() {
    let temp = temp_dir("job_recovery_startup");
    let job_id = "job_00000000000000000000000000000001";
    let jobs_dir = temp.join("jobs").join(job_id);
    fs::create_dir_all(&jobs_dir).expect("job dir should be created");
    fs::write(jobs_dir.join("stderr.log"), "").expect("stderr should exist");
    fs::write(
        jobs_dir.join("metadata.json"),
        serde_json::to_vec_pretty(&JobMetadata {
            job_id: job_id.to_string(),
            name: "build-app".to_string(),
            idempotency_key: "dispatch_recovery".to_string(),
            request_hash: "abc123".to_string(),
            status: JobStatus::Running,
            started_at: "2026-01-01T00:00:00Z".to_string(),
            finished_at: None,
            duration_ms: None,
            exit_code: None,
            terminal_reason: None,
            failure_category: None,
            inputs: Map::new(),
            resolved_artifacts: BTreeMap::new(),
            outputs: BTreeMap::new(),
            output_metadata: Default::default(),
        })
        .expect("metadata"),
    )
    .expect("metadata written");

    let store = JobStore::new(&temp).expect("job store should init");
    let recovered = store
        .recover_interrupted_jobs()
        .expect("recovery should succeed");

    assert_eq!(recovered, 1);
    let metadata = store.read_job(job_id).expect("job should load");
    assert_eq!(metadata.status, JobStatus::Failed);
    assert!(metadata.finished_at.is_some());
    assert!(
        fs::read_to_string(jobs_dir.join("stderr.log"))
            .expect("stderr should read")
            .contains("runner restarted before job completion")
    );
}

#[test]
fn recovers_cancel_requested_jobs_on_startup_as_canceled() {
    let temp = temp_dir("job_recovery_cancel_requested");
    let job_id = "job_00000000000000000000000000000002";
    let jobs_dir = temp.join("jobs").join(job_id);
    fs::create_dir_all(&jobs_dir).expect("job dir should be created");
    fs::write(jobs_dir.join("stderr.log"), "").expect("stderr should exist");
    fs::write(
        jobs_dir.join("metadata.json"),
        serde_json::to_vec_pretty(&JobMetadata {
            job_id: job_id.to_string(),
            name: "build-app".to_string(),
            idempotency_key: "dispatch_recovery_cancel_requested".to_string(),
            request_hash: "abc123".to_string(),
            status: JobStatus::CancelRequested,
            started_at: "2026-01-01T00:00:00Z".to_string(),
            finished_at: None,
            duration_ms: None,
            exit_code: None,
            terminal_reason: None,
            failure_category: None,
            inputs: Map::new(),
            resolved_artifacts: BTreeMap::new(),
            outputs: BTreeMap::new(),
            output_metadata: Default::default(),
        })
        .expect("metadata"),
    )
    .expect("metadata written");

    let store = JobStore::new(&temp).expect("job store should init");
    let recovered = store
        .recover_interrupted_jobs()
        .expect("recovery should succeed");

    assert_eq!(recovered, 1);
    let metadata = store.read_job(job_id).expect("job should load");
    assert_eq!(metadata.status, JobStatus::Canceled);
    assert!(metadata.finished_at.is_some());
    assert!(
        fs::read_to_string(jobs_dir.join("stderr.log"))
            .expect("stderr should read")
            .contains("runner restarted while canceling the job")
    );
}

#[test]
fn recovers_canceling_jobs_on_startup_as_canceled() {
    let temp = temp_dir("job_recovery_canceling");
    let job_id = "job_00000000000000000000000000000003";
    let jobs_dir = temp.join("jobs").join(job_id);
    fs::create_dir_all(&jobs_dir).expect("job dir should be created");
    fs::write(jobs_dir.join("stderr.log"), "").expect("stderr should exist");
    fs::write(
        jobs_dir.join("metadata.json"),
        serde_json::to_vec_pretty(&JobMetadata {
            job_id: job_id.to_string(),
            name: "build-app".to_string(),
            idempotency_key: "dispatch_recovery_canceling".to_string(),
            request_hash: "abc123".to_string(),
            status: JobStatus::Canceling,
            started_at: "2026-01-01T00:00:00Z".to_string(),
            finished_at: None,
            duration_ms: None,
            exit_code: None,
            terminal_reason: None,
            failure_category: None,
            inputs: Map::new(),
            resolved_artifacts: BTreeMap::new(),
            outputs: BTreeMap::new(),
            output_metadata: Default::default(),
        })
        .expect("metadata"),
    )
    .expect("metadata written");

    let store = JobStore::new(&temp).expect("job store should init");
    let recovered = store
        .recover_interrupted_jobs()
        .expect("recovery should succeed");

    assert_eq!(recovered, 1);
    let metadata = store.read_job(job_id).expect("job should load");
    assert_eq!(metadata.status, JobStatus::Canceled);
    assert!(metadata.finished_at.is_some());
    assert!(
        fs::read_to_string(jobs_dir.join("stderr.log"))
            .expect("stderr should read")
            .contains("runner restarted while canceling the job")
    );
}

#[test]
fn removes_job_dir_missing_metadata_on_startup() {
    let temp = temp_dir("job_recovery_missing_metadata");
    let jobs_dir = temp.join("jobs").join("job_incomplete");
    fs::create_dir_all(jobs_dir.join("work")).expect("work dir should be created");
    fs::write(jobs_dir.join("stdout.log"), "").expect("stdout should exist");

    let store = JobStore::new(&temp).expect("job store should init");
    let recovered = store
        .recover_interrupted_jobs()
        .expect("recovery should succeed");

    assert_eq!(recovered, 1);
    assert!(!jobs_dir.exists());
}

#[test]
fn removes_job_dir_with_invalid_metadata_on_startup() {
    let temp = temp_dir("job_recovery_invalid_metadata");
    let jobs_dir = temp.join("jobs").join("job_invalid");
    fs::create_dir_all(&jobs_dir).expect("job dir should be created");
    fs::write(jobs_dir.join("metadata.json"), b"{not-json").expect("metadata should exist");

    let store = JobStore::new(&temp).expect("job store should init");
    let recovered = store
        .recover_interrupted_jobs()
        .expect("recovery should succeed");

    assert_eq!(recovered, 1);
    assert!(!jobs_dir.exists());
}

#[tokio::test]
async fn parallel_jobs_can_run_together() {
    let temp = temp_dir("job_parallel_concurrency");
    let state = test_state_with_manifests(
        &temp,
        vec![
            TestManifest {
                job_name: "job-a",
                concurrency: "parallel",
                inputs_toml: OPTIONAL_COMMIT_INPUT,
                outputs_toml: "",
            },
            TestManifest {
                job_name: "job-b",
                concurrency: "parallel",
                inputs_toml: OPTIONAL_COMMIT_INPUT,
                outputs_toml: "",
            },
        ],
        "#!/bin/sh\nsleep 1\n",
        600,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let first = app
        .clone()
        .oneshot(
            Request::post("/jobs/job-a/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "a" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let second = app
        .oneshot(
            Request::post("/jobs/job-b/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "b" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(first.status(), StatusCode::CREATED);
    assert_eq!(second.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn job_exclusive_rejects_second_instance_while_running() {
    let temp = temp_dir("job_exclusive_concurrency");
    let state = test_state_with_manifests(
        &temp,
        vec![TestManifest {
            job_name: "build-app",
            concurrency: "job_exclusive",
            inputs_toml: OPTIONAL_COMMIT_INPUT,
            outputs_toml: "",
        }],
        "#!/bin/sh\nsleep 1\n",
        600,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let first = app
        .clone()
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "a" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let second = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "b" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(first.status(), StatusCode::CREATED);
    assert_eq!(second.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn global_exclusive_rejects_when_other_job_is_running() {
    let temp = temp_dir("job_global_exclusive_conflict");
    let state = test_state_with_manifests(
        &temp,
        vec![
            TestManifest {
                job_name: "job-a",
                concurrency: "parallel",
                inputs_toml: OPTIONAL_COMMIT_INPUT,
                outputs_toml: "",
            },
            TestManifest {
                job_name: "job-b",
                concurrency: "global_exclusive",
                inputs_toml: OPTIONAL_COMMIT_INPUT,
                outputs_toml: "",
            },
        ],
        "#!/bin/sh\nsleep 1\n",
        600,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let first = app
        .clone()
        .oneshot(
            Request::post("/jobs/job-a/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "a" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let second = app
        .oneshot(
            Request::post("/jobs/job-b/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "b" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(first.status(), StatusCode::CREATED);
    assert_eq!(second.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn parallel_job_rejects_while_global_exclusive_is_running() {
    let temp = temp_dir("job_global_exclusive_running");
    let state = test_state_with_manifests(
        &temp,
        vec![
            TestManifest {
                job_name: "job-a",
                concurrency: "global_exclusive",
                inputs_toml: OPTIONAL_COMMIT_INPUT,
                outputs_toml: "",
            },
            TestManifest {
                job_name: "job-b",
                concurrency: "parallel",
                inputs_toml: OPTIONAL_COMMIT_INPUT,
                outputs_toml: "",
            },
        ],
        "#!/bin/sh\nsleep 1\n",
        600,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let first = app
        .clone()
        .oneshot(
            Request::post("/jobs/job-a/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "a" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let second = app
        .oneshot(
            Request::post("/jobs/job-b/runs")
                .header("x-janus-key-id", "runner")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "b" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(first.status(), StatusCode::CREATED);
    assert_eq!(second.status(), StatusCode::CONFLICT);
}

fn test_state(temp: &Path) -> AppState {
    test_state_from_manifest(
        temp,
        "build-app",
        REQUIRED_COMMIT_AND_BRANCH_INPUTS,
        "",
        "#!/bin/sh\nexit 0\n",
        600,
        50,
        64,
    )
}

fn test_state_with_artifact_manifest(temp: &Path) -> AppState {
    test_state_from_manifest(
        temp,
        "build-with-artifact",
        REQUIRED_SOURCE_INPUT,
        "",
        "#!/bin/sh\nexit 0\n",
        600,
        50,
        64,
    )
}

fn test_state_with_request_body_limit(temp: &Path, max_request_body_kb: u64) -> AppState {
    test_state_from_manifest(
        temp,
        "build-app",
        REQUIRED_COMMIT_AND_BRANCH_INPUTS,
        "",
        "#!/bin/sh\nexit 0\n",
        600,
        50,
        max_request_body_kb,
    )
}

fn test_state_with_job_run_rate_limit(temp: &Path, max_run_requests_per_minute: u32) -> AppState {
    let manifests_dir = temp.join("manifests");
    let scripts_dir = temp.join("scripts");
    fs::create_dir_all(&manifests_dir).expect("manifests dir should be created");
    fs::create_dir_all(&scripts_dir).expect("scripts dir should be created");
    let script = write_executable_script(&scripts_dir, "build.sh", "#!/bin/sh\nexit 0\n");
    fs::write(
        manifests_dir.join("build-app.toml"),
        format!(
            r#"
name = "build-app"
script = "{}"
timeout_seconds = 600
concurrency = "parallel"

[inputs.commit]
type = "string"
required = true

[inputs.branch]
type = "string"
required = true
"#,
            script.display()
        ),
    )
    .expect("manifest should be written");

    let config = Config {
        data_dir: temp.display().to_string(),
        manifests_dir: manifests_dir.display().to_string(),
        server: ServerConfig {
            listen: "127.0.0.1:0".to_string(),
        },
        auth: AuthConfig {
            mode: "signed".to_string(),
            servers: Vec::new(),
        },
        artifacts: ArtifactsConfig {
            max_size_mb: 1,
            ttl_seconds: 3600,
            cleanup_interval_seconds: 600,
            require_checksum_on_upload: true,
            max_upload_requests_per_minute: 60,
        },
        jobs: JobsConfig {
            default_log_limit_mb: 50,
            max_request_body_kb: 64,
            max_run_requests_per_minute,
            cleanup_successful_workdirs: true,
            keep_failed_workdirs: true,
        },
    };

    build_state(config)
}

fn test_state_with_script(
    temp: &Path,
    job_name: &str,
    script_body: &str,
    timeout_seconds: u64,
) -> AppState {
    test_state_with_script_and_log_limit(temp, job_name, script_body, timeout_seconds, 50, 64)
}

fn test_state_with_script_and_log_limit(
    temp: &Path,
    job_name: &str,
    script_body: &str,
    timeout_seconds: u64,
    default_log_limit_mb: u64,
    max_request_body_kb: u64,
) -> AppState {
    test_state_from_manifest(
        temp,
        job_name,
        OPTIONAL_COMMIT_AND_SOURCE_INPUTS,
        "",
        script_body,
        timeout_seconds,
        default_log_limit_mb,
        max_request_body_kb,
    )
}

fn test_state_from_manifest(
    temp: &Path,
    job_name: &str,
    inputs_toml: &str,
    outputs_toml: &str,
    script_body: &str,
    timeout_seconds: u64,
    default_log_limit_mb: u64,
    max_request_body_kb: u64,
) -> AppState {
    let manifests_dir = temp.join("manifests");
    let scripts_dir = temp.join("scripts");
    fs::create_dir_all(&manifests_dir).expect("manifests dir should be created");
    fs::create_dir_all(&scripts_dir).expect("scripts dir should be created");
    let script = write_executable_script(&scripts_dir, "build.sh", script_body);
    fs::write(
        manifests_dir.join(format!("{job_name}.toml")),
        format!(
            r#"
name = "{job_name}"
script = "{}"
timeout_seconds = {timeout_seconds}
concurrency = "parallel"

{inputs_toml}
{outputs_toml}
"#,
            script.display()
        ),
    )
    .expect("manifest should be written");

    let config = Config {
        data_dir: temp.display().to_string(),
        manifests_dir: manifests_dir.display().to_string(),
        server: ServerConfig {
            listen: "127.0.0.1:0".to_string(),
        },
        auth: AuthConfig {
            mode: "signed".to_string(),
            servers: Vec::new(),
        },
        artifacts: ArtifactsConfig {
            max_size_mb: 1,
            ttl_seconds: 3600,
            cleanup_interval_seconds: 600,
            require_checksum_on_upload: true,
            max_upload_requests_per_minute: 60,
        },
        jobs: JobsConfig {
            default_log_limit_mb,
            max_request_body_kb,
            max_run_requests_per_minute: 60,
            cleanup_successful_workdirs: true,
            keep_failed_workdirs: true,
        },
    };

    build_state(config)
}

fn test_state_with_manifests(
    temp: &Path,
    manifests: Vec<TestManifest<'_>>,
    script_body: &str,
    timeout_seconds: u64,
) -> AppState {
    let manifests_dir = temp.join("manifests");
    let scripts_dir = temp.join("scripts");
    fs::create_dir_all(&manifests_dir).expect("manifests dir should be created");
    fs::create_dir_all(&scripts_dir).expect("scripts dir should be created");
    let script = write_executable_script(&scripts_dir, "build.sh", script_body);

    for manifest in manifests {
        fs::write(
            manifests_dir.join(format!("{}.toml", manifest.job_name)),
            format!(
                r#"
name = "{}"
script = "{}"
timeout_seconds = {}
concurrency = "{}"

{}
{}
"#,
                manifest.job_name,
                script.display(),
                timeout_seconds,
                manifest.concurrency,
                manifest.inputs_toml,
                manifest.outputs_toml
            ),
        )
        .expect("manifest should be written");
    }

    let config = Config {
        data_dir: temp.display().to_string(),
        manifests_dir: manifests_dir.display().to_string(),
        server: ServerConfig {
            listen: "127.0.0.1:0".to_string(),
        },
        auth: AuthConfig {
            mode: "signed".to_string(),
            servers: Vec::new(),
        },
        artifacts: ArtifactsConfig {
            max_size_mb: 1,
            ttl_seconds: 3600,
            cleanup_interval_seconds: 600,
            require_checksum_on_upload: true,
            max_upload_requests_per_minute: 60,
        },
        jobs: JobsConfig {
            default_log_limit_mb: 50,
            max_request_body_kb: 64,
            max_run_requests_per_minute: 60,
            cleanup_successful_workdirs: true,
            keep_failed_workdirs: true,
        },
    };

    build_state(config)
}

fn build_state(config: Config) -> AppState {
    AppState {
        config: Arc::new(config.clone()),
        auth: Arc::new(AuthStore::test_with_signed_servers(&[(
            "runner",
            "runner",
            &[
                "jobs:run",
                "jobs:read",
                "logs:read",
                "artifacts:read",
                "artifacts:write",
            ],
        )])),
        manifests: Arc::new(
            ManifestStore::load_from_dir(&config.manifests_dir).expect("manifests should load"),
        ),
        artifacts: Arc::new(
            ArtifactStore::new(
                &config.data_dir,
                config.artifacts.ttl_seconds,
                config.artifacts.max_size_mb,
                config.artifacts.require_checksum_on_upload,
            )
            .expect("artifact store should init"),
        ),
        jobs: Arc::new(JobStore::new(&config.data_dir).expect("job store should init")),
        rate_limiter: Arc::new(crate::rate_limit::RateLimiter::new()),
        runtime_status: Arc::new(crate::RuntimeStatus::new(0, 0)),
    }
}

async fn read_created_job(response: axum::response::Response) -> JobCreatedResponse {
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    serde_json::from_slice(&body).expect("created job body")
}

async fn wait_for_terminal_metadata(temp: &Path, job_id: &str) -> JobMetadata {
    let metadata_path = temp.join("jobs").join(job_id).join("metadata.json");

    for _ in 0..100 {
        let metadata: JobMetadata =
            serde_json::from_slice(&fs::read(&metadata_path).expect("metadata should be readable"))
                .expect("metadata should parse");

        if metadata.finished_at.is_some() {
            return metadata;
        }

        sleep(Duration::from_millis(25)).await;
    }

    panic!("job did not reach a terminal state");
}

fn read_metadata(temp: &Path, job_id: &str) -> JobMetadata {
    let metadata_path = temp.join("jobs").join(job_id).join("metadata.json");
    serde_json::from_slice(&fs::read(&metadata_path).expect("metadata should be readable"))
        .expect("metadata should parse")
}

async fn wait_for_file(path: &Path) -> String {
    for _ in 0..100 {
        if let Ok(contents) = fs::read_to_string(path) {
            if !contents.trim().is_empty() {
                return contents;
            }
        }

        sleep(Duration::from_millis(25)).await;
    }

    panic!("file was not populated: {}", path.display());
}

#[cfg(unix)]
fn unix_process_exists(pid: i32) -> bool {
    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }

    let result = unsafe { kill(pid, 0) };
    if result == 0 {
        true
    } else {
        std::io::Error::last_os_error().raw_os_error() != Some(3)
    }
}

fn store_artifact(store: &ArtifactStore, bytes: &[u8]) -> String {
    let checksum = hex::encode(sha2::Sha256::digest(bytes));
    store
        .store_bytes(bytes, Some(&checksum))
        .expect("artifact should store")
        .artifact_id
}

fn write_executable_script(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, body).expect("script should be written");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(&path).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&path, permissions).expect("permissions should be set");
    }

    path
}

fn temp_dir(label: &str) -> PathBuf {
    let unique = Uuid::now_v7().simple().to_string();
    let path = std::env::temp_dir().join(format!("janus-runner-{label}-{unique}"));
    fs::create_dir_all(&path).expect("temp dir should be created");
    path
}

const REQUIRED_COMMIT_AND_BRANCH_INPUTS: &str = r#"
[inputs.commit]
type = "string"
required = true

[inputs.branch]
type = "string"
required = true
"#;

const REQUIRED_SOURCE_INPUT: &str = r#"
[inputs.source]
type = "artifact"
required = true
"#;

const OPTIONAL_COMMIT_AND_SOURCE_INPUTS: &str = r#"
[inputs.commit]
type = "string"
required = false

[inputs.source]
type = "artifact"
required = false
"#;

const OPTIONAL_COMMIT_INPUT: &str = r#"
[inputs.commit]
type = "string"
required = false
"#;

struct TestManifest<'a> {
    job_name: &'a str,
    concurrency: &'a str,
    inputs_toml: &'a str,
    outputs_toml: &'a str,
}
