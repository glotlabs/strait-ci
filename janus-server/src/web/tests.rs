use super::routes::{build_router, csrf_token};
use crate::{
    app::{bootstrap_admin, build_state},
    auth::{hash_password, session_cookie},
    git,
    models::{
        JobRunOutput, Repo, RunnerJobDefinition, User, UserRole, WorkflowDefinition,
        WorkflowInputBinding, WorkflowJobDefinition, WorkflowJobOutcomePolicy, WorkflowTrigger,
    },
    scheduler,
};
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Path as AxumPath, State},
    http::{Request, StatusCode},
    routing::{get, post},
};
use chrono::{Duration, Utc};
use janus_lib::{HEADER_IDEMPOTENCY_KEY, JobOutputMetadata, RunnerRouteTemplate};
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::time::sleep;
use tower::util::ServiceExt;
use url::form_urlencoded;
use uuid::Uuid;

#[tokio::test]
async fn repo_creation_installs_hook() {
    let fixture = test_fixture().await;
    let user = fixture.user.clone();
    let token = csrf_token(&fixture.state, &user);
    let cookie = session_cookie_value(&fixture.state, &user.id);
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/repos")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(format!(
                    "csrf_token={}&name=demo&default_branch=main",
                    token
                )))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let repos = fixture.state.db.list_repos().expect("repos");
    let repo = repos.iter().find(|repo| repo.name == "demo").expect("repo");
    let hook = fs::read_to_string(PathBuf::from(&repo.bare_path).join("hooks/post-receive"))
        .expect("hook");
    assert!(hook.contains("git post-receive"));
    assert!(hook.contains("--socket-path"));
    assert!(hook.contains(&repo.id));
}

#[tokio::test]
async fn workflows_page_renders_runner_job_builder() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    fixture
        .state
        .db
        .replace_runner_jobs(
            &fixture.runner_id,
            &[
                runner_job_definition(
                    r#"{"name":"build-app","timeout_seconds":60,"inputs":{"commit":{"type":"string","required":true},"branch":{"type":"string","required":true},"source":{"type":"artifact","required":true}},"outputs":{"app":{"type":"artifact","required":true}}}"#,
                ),
                runner_job_definition(
                    r#"{"name":"test-app","timeout_seconds":60,"inputs":{"commit":{"type":"string","required":true}},"outputs":{}}"#,
                ),
            ],
        )
        .expect("runner jobs");
    fixture
        .state
        .db
        .update_runner_health(&fixture.runner_id, "healthy")
        .expect("health");

    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::get("/workflows")
                .header("cookie", cookie)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let html = String::from_utf8(body.to_vec()).expect("html");
    assert!(html.contains("workflow-runner-catalog"));
    assert!(html.contains("workflow-job-list"));
    assert!(html.contains("Job"));
    assert!(html.contains("build-app"));
    assert!(html.contains("test-app"));
    assert!(html.contains("\"commit\""));

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::get("/assets/form_validation.js")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::get("/assets/workflow_builder_state.js")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::get("/assets/workflow_builder_bindings.js")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::get("/assets/workflow_builder_tables.js")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::get("/assets/workflow_builder_rows.js")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn runners_page_renders_name_edit_form() {
    let fixture = test_fixture().await;
    let admin = admin_user(&fixture.state);
    let cookie = session_cookie_value(&fixture.state, &admin.id);
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::get("/runners")
                .header("cookie", cookie)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let html = String::from_utf8(body.to_vec()).expect("html");
    assert!(html.contains(&format!("/runners/{}/update", fixture.runner_id)));
    assert!(html.contains("Runner name"));
    assert!(html.contains("Save name"));
}

#[tokio::test]
async fn runner_name_can_be_updated_from_frontend() {
    let fixture = test_fixture().await;
    let admin = admin_user(&fixture.state);
    let token = csrf_token(&fixture.state, &admin);
    let cookie = session_cookie_value(&fixture.state, &admin.id);
    let body = form_urlencoded::Serializer::new(String::new())
        .append_pair("csrf_token", &token)
        .append_pair("name", "renamed-runner")
        .finish();

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!("/runners/{}/update", fixture.runner_id))
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::SEE_OTHER);

    let runner = fixture
        .state
        .db
        .get_runner(&fixture.runner_id)
        .expect("runner")
        .expect("runner");
    assert_eq!(runner.name, "renamed-runner");
}

#[tokio::test]
async fn workflows_page_marks_stale_workflow_schemas() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "stale-workflow-page");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .replace_runner_jobs(
            &fixture.runner_id,
            &[runner_job_definition(
                r#"{"name":"build-app","timeout_seconds":60,"inputs":{"commit":{"type":"string","required":true},"branch":{"type":"string","required":false},"source":{"type":"artifact","required":true}},"outputs":{"app":{"type":"artifact","required":true}}}"#,
            )],
        )
        .expect("runner jobs");

    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::get("/workflows")
                .header("cookie", cookie)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let html = String::from_utf8(body.to_vec()).expect("html");
    assert!(html.contains("stale"));
    assert!(html.contains("input branch changed requiredness from required to optional"));
}

#[tokio::test]
async fn manual_workflow_can_be_run_from_frontend() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "manual-workflow");
    create_manual_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    let workflow = fixture
        .state
        .db
        .workflows_for_repo(&repo.id)
        .expect("workflows")
        .into_iter()
        .find(|workflow| workflow.name == "wf")
        .expect("workflow");

    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::get("/workflows")
                .header("cookie", cookie.clone())
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let html = String::from_utf8(body.to_vec()).expect("html");
    assert!(html.contains(&format!("/workflows/{}/run", workflow.id)));
    assert!(html.contains("Run workflow"));

    let token = csrf_token(&fixture.state, &fixture.user);
    let body = form_urlencoded::Serializer::new(String::new())
        .append_pair("csrf_token", &token)
        .append_pair("branch", "release")
        .append_pair("commit", "abc123")
        .finish();
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!("/workflows/{}/run", workflow.id))
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::SEE_OTHER);

    let pipeline = fixture
        .state
        .db
        .list_pipeline_runs()
        .expect("pipelines")
        .into_iter()
        .find(|pipeline| pipeline.workflow_id == workflow.id)
        .expect("pipeline");
    assert_eq!(pipeline.trigger_type, "manual");
    assert_eq!(pipeline.trigger_ref.as_deref(), Some("release"));
    assert_eq!(pipeline.commit_sha.as_deref(), Some("abc123"));
}

#[tokio::test]
async fn manual_workflow_promotes_a_recent_build_artifact_to_another_runner() {
    let mock = spawn_mock_runner().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    fixture
        .state
        .db
        .update_runner_name(&fixture.runner_id, "glot-build")
        .expect("build runner name");
    let repo = create_repo_direct(&fixture.state, &fixture.user, "artifact-promotion");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    let build_workflow = fixture
        .state
        .db
        .workflows_for_repo(&repo.id)
        .expect("workflows")
        .into_iter()
        .find(|workflow| workflow.name == "wf")
        .expect("build workflow");
    let build_pipeline_id = scheduler::enqueue_workflow_run(
        Arc::clone(&fixture.state),
        &build_workflow,
        "push",
        Some("refs/heads/main"),
        Some("abc123def456"),
    )
    .expect("build pipeline");
    let build_job_id = fixture
        .state
        .db
        .pipeline_snapshot(&build_pipeline_id)
        .expect("snapshot")
        .expect("pipeline")
        .jobs[0]
        .run
        .id
        .clone();
    let bytes = b"immutable-release-bits";
    let pending = fixture
        .state
        .artifacts
        .store_bytes("job_output", &build_job_id, "app", bytes)
        .expect("store build artifact");
    let server_artifact_id = fixture
        .state
        .db
        .insert_server_artifact(&pending)
        .expect("artifact row");
    fixture
        .state
        .db
        .finish_job_run(
            &build_job_id,
            "success",
            Some(10),
            Some(0),
            None,
            None,
            &JobOutputMetadata::default(),
            "",
            "",
            &[JobRunOutput {
                output_name: "app".to_string(),
                kind: "artifact".to_string(),
                runner_artifact_id: Some("build-runner-artifact".to_string()),
                server_artifact_id: Some(server_artifact_id.clone()),
                value: None,
                sha256: Some(pending.sha256.clone()),
                size_bytes: Some(bytes.len() as i64),
            }],
        )
        .expect("finish build");
    fixture
        .state
        .db
        .finalize_pipeline_status(&build_pipeline_id)
        .expect("finish pipeline");

    let prod_runner_id = fixture
        .state
        .db
        .create_runner("glot-prod", &mock.base_url)
        .expect("prod runner");
    fixture
        .state
        .db
        .replace_runner_jobs(
            &prod_runner_id,
            &[runner_job_definition(
                r#"{"name":"deploy-app","timeout_seconds":60,"inputs":{"app":{"type":"artifact","required":true}},"outputs":{}}"#,
            )],
        )
        .expect("prod jobs");
    let prod_jobs = vec![WorkflowJobDefinition {
        runner_id: prod_runner_id,
        runner_job_name: "deploy-app".to_string(),
        inputs: BTreeMap::from([(
            "app".to_string(),
            WorkflowInputBinding::PromotedArtifact {
                source_runner_id: fixture.runner_id.clone(),
                source_job_name: "build-app".to_string(),
                output_name: "app".to_string(),
            },
        )]),
        outcome_policy: WorkflowJobOutcomePolicy::Required,
    }];
    let prod_workflow_id = fixture
        .state
        .db
        .create_workflow(
            &repo.id,
            "deploy-prod",
            true,
            &serde_json::to_string(&WorkflowTrigger {
                kind: "manual".to_string(),
                branches: vec!["main".to_string()],
            })
            .expect("trigger"),
            &serde_json::to_string(&WorkflowDefinition {
                jobs: prod_jobs.clone(),
            })
            .expect("definition"),
            &workflow_job_schemas(&fixture.state, &prod_jobs),
        )
        .expect("prod workflow");

    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::get(format!("/workflows/{prod_workflow_id}"))
                .header("cookie", cookie.clone())
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let html = String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body")
            .to_vec(),
    )
    .expect("html");
    assert!(html.contains(&server_artifact_id));
    assert!(html.contains("glot-build / build-app / app"));
    assert!(html.contains("abc123def456"));

    let token = csrf_token(&fixture.state, &fixture.user);
    let invalid_body = form_urlencoded::Serializer::new(String::new())
        .append_pair("csrf_token", &token)
        .append_pair("artifact:0:app", "srvart_not_eligible")
        .finish();
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!("/workflows/{prod_workflow_id}/run"))
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie.clone())
                .body(Body::from(invalid_body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = form_urlencoded::Serializer::new(String::new())
        .append_pair("csrf_token", &token)
        .append_pair("branch", "main")
        .append_pair("commit", "abc123def456")
        .append_pair("artifact:0:app", &server_artifact_id)
        .finish();
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!("/workflows/{prod_workflow_id}/run"))
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let prod_pipeline = fixture
        .state
        .db
        .list_pipeline_runs()
        .expect("pipelines")
        .into_iter()
        .find(|pipeline| pipeline.workflow_id == prod_workflow_id)
        .expect("prod pipeline");
    let selections = fixture
        .state
        .db
        .workflow_run_artifact_selections(&prod_pipeline.id)
        .expect("selections");
    assert_eq!(selections.len(), 1);
    assert_eq!(selections[0].server_artifact_id, server_artifact_id);

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch production deploy");
    let requests = mock.requests_for("deploy-app");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["app"], json!("artifact-1"));
}

#[tokio::test]
async fn api_workflow_includes_schema_status() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "stale-workflow-api");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .replace_runner_jobs(
            &fixture.runner_id,
            &[runner_job_definition(
                r#"{"name":"build-app","timeout_seconds":60,"inputs":{"commit":{"type":"string","required":true},"branch":{"type":"integer","required":true},"source":{"type":"artifact","required":true}},"outputs":{"app":{"type":"artifact","required":true}}}"#,
            )],
        )
        .expect("runner jobs");

    let workflow = fixture
        .state
        .db
        .workflows_for_repo(&repo.id)
        .expect("workflows")
        .into_iter()
        .find(|workflow| workflow.name == "wf")
        .expect("workflow");
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::get(format!("/api/workflows/{}", workflow.id))
                .header("cookie", cookie)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let payload: JsonValue = serde_json::from_slice(&body).expect("json");
    assert_eq!(payload["schema_status"], json!("incompatible"));
    assert_eq!(
        payload["schema_diff"][0]["kind"],
        json!("input_type_changed")
    );
    assert_eq!(
        payload["schema_diff"][0]["message"],
        json!("job-1 input branch changed type from string to integer")
    );
    assert_eq!(payload["schema_diff"][0]["incompatible"], json!(true));
    assert_eq!(payload["id"], json!(workflow.id));
}

#[tokio::test]
async fn api_workflow_reports_additive_schema_diff_without_stale_status() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "additive-workflow-api");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .replace_runner_jobs(
            &fixture.runner_id,
            &[runner_job_definition(
                r#"{"name":"build-app","timeout_seconds":60,"inputs":{"commit":{"type":"string","required":true},"branch":{"type":"string","required":true},"source":{"type":"artifact","required":true},"published":{"type":"boolean","required":false}},"outputs":{"app":{"type":"artifact","required":true},"image_tag":{"type":"string","required":false}}}"#,
            )],
        )
        .expect("runner jobs");

    let workflow = fixture
        .state
        .db
        .workflows_for_repo(&repo.id)
        .expect("workflows")
        .into_iter()
        .find(|workflow| workflow.name == "wf")
        .expect("workflow");
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::get(format!("/api/workflows/{}", workflow.id))
                .header("cookie", cookie)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let payload: JsonValue = serde_json::from_slice(&body).expect("json");
    assert_eq!(payload["schema_status"], json!("current"));
    assert_eq!(payload["schema_diff"][0]["kind"], json!("input_added"));
    assert_eq!(payload["schema_diff"][0]["incompatible"], json!(false));
    assert_eq!(payload["schema_diff"][1]["kind"], json!("output_added"));
}

#[tokio::test]
async fn api_create_runner_rejects_invalid_base_url() {
    let fixture = test_fixture().await;
    let admin = admin_user(&fixture.state);
    let token = csrf_token(&fixture.state, &admin);
    let cookie = session_cookie_value(&fixture.state, &admin.id);
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/runners")
                .header("content-type", "application/json")
                .header("cookie", cookie)
                .body(Body::from(
                    json!({
                        "csrf_token": token,
                        "name": "runner",
                        "base_url": "not a url"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let payload: JsonValue = serde_json::from_slice(&body).expect("json");
    assert_eq!(payload["error"]["code"], json!("bad_request"));
    assert_eq!(
        payload["error"]["message"],
        json!("base_url must be a valid URL")
    );
}

#[tokio::test]
async fn api_create_runner_rejects_local_runner_url() {
    let fixture = test_fixture().await;
    let admin = admin_user(&fixture.state);
    let token = csrf_token(&fixture.state, &admin);
    let cookie = session_cookie_value(&fixture.state, &admin.id);
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/runners")
                .header("content-type", "application/json")
                .header("cookie", cookie)
                .body(Body::from(
                    json!({
                        "csrf_token": token,
                        "name": "runner",
                        "base_url": "https://127.0.0.1"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let payload: JsonValue = serde_json::from_slice(&body).expect("json");
    assert_eq!(payload["error"]["code"], json!("bad_request"));
    assert_eq!(
        payload["error"]["message"],
        json!("base_url host must not be localhost")
    );
}

#[tokio::test]
async fn api_create_runner_can_allow_local_http_by_policy() {
    let fixture = test_fixture_with_runner_url_policy(
        r#"require_https = false
allow_credentials = false
allow_query = false
allow_fragment = false
allow_path = false
allow_localhost = true
allow_private_ips = true
allow_link_local_ips = false
allow_documentation_ips = false
allow_multicast_ips = false"#,
    )
    .await;
    let admin = admin_user(&fixture.state);
    let token = csrf_token(&fixture.state, &admin);
    let cookie = session_cookie_value(&fixture.state, &admin.id);
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/runners")
                .header("content-type", "application/json")
                .header("cookie", cookie)
                .body(Body::from(
                    json!({
                        "csrf_token": token,
                        "name": "runner",
                        "base_url": "http://127.0.0.1:9"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn responses_include_security_headers() {
    let fixture = test_fixture().await;
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::get("/health")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-content-type-options").unwrap(),
        "nosniff"
    );
    assert_eq!(response.headers().get("x-frame-options").unwrap(), "DENY");
    assert!(
        response
            .headers()
            .get("content-security-policy")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("frame-ancestors 'none'")
    );
}

#[tokio::test]
async fn api_internal_errors_do_not_expose_details() {
    let fixture = test_fixture().await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "duplicate-api-repo");
    let repo_dir_count_before = fs::read_dir(&fixture.state.config.repos_dir)
        .expect("repo dir")
        .count();
    let token = csrf_token(&fixture.state, &fixture.user);
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/repos")
                .header("content-type", "application/json")
                .header("cookie", cookie)
                .body(Body::from(
                    json!({
                        "csrf_token": token,
                        "name": repo.name,
                        "default_branch": "main"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let payload: JsonValue = serde_json::from_slice(&body).expect("json");
    assert_eq!(payload["error"]["code"], json!("internal_error"));
    assert_eq!(payload["error"]["message"], json!("internal server error"));
    assert!(!String::from_utf8_lossy(&body).contains("UNIQUE"));
    let repo_dir_count_after = fs::read_dir(&fixture.state.config.repos_dir)
        .expect("repo dir")
        .count();
    assert_eq!(repo_dir_count_after, repo_dir_count_before);
}

#[tokio::test]
async fn admin_mutations_record_audit_events() {
    let fixture = test_fixture().await;
    let token = csrf_token(&fixture.state, &fixture.user);
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/repos")
                .header("content-type", "application/json")
                .header("cookie", cookie)
                .body(Body::from(
                    json!({
                        "csrf_token": token,
                        "name": "audit-repo",
                        "default_branch": "main"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let events = fixture.state.db.list_audit_events().expect("audit events");
    let event = events
        .iter()
        .find(|event| event.action == "repo.create")
        .expect("repo create audit event");
    assert_eq!(
        event.actor_user_id.as_deref(),
        Some(fixture.user.id.as_str())
    );
    assert_eq!(
        event.actor_username.as_deref(),
        Some(fixture.user.username.as_str())
    );
    assert_eq!(event.target_type, "repo");
    assert_eq!(event.target_name.as_deref(), Some("audit-repo"));
    let metadata: JsonValue = serde_json::from_str(&event.metadata_json).expect("metadata");
    assert_eq!(metadata["default_branch"], json!("main"));
}

#[tokio::test]
async fn artifact_download_uses_attachment_headers_and_safe_filename() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "artifact-download");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    let workflow = fixture
        .state
        .db
        .workflows_for_repo(&repo.id)
        .expect("workflows")
        .into_iter()
        .next()
        .expect("workflow");
    let pipeline_id = fixture
        .state
        .db
        .create_pipeline_run(
            &repo.id,
            &workflow.id,
            &workflow.version_id,
            "manual",
            Some("main"),
            Some("HEAD"),
        )
        .expect("pipeline");
    let pending = fixture
        .state
        .artifacts
        .store_bytes(
            "pipeline_source",
            &pipeline_id,
            "evil\r\n../<script>.html",
            b"<script>alert(1)</script>",
        )
        .expect("artifact");
    let artifact_id = fixture
        .state
        .db
        .insert_server_artifact(&pending)
        .expect("artifact record");
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::get(format!("/artifacts/{artifact_id}"))
                .header("cookie", cookie)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/octet-stream"
    );
    assert_eq!(
        response.headers().get("x-content-type-options").unwrap(),
        "nosniff"
    );
    assert_eq!(
        response.headers().get("cache-control").unwrap(),
        "private, no-store"
    );
    let disposition = response
        .headers()
        .get("content-disposition")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(disposition.starts_with("attachment;"));
    assert!(disposition.contains("filename=\""));
    assert!(!disposition.contains('\n'));
    assert!(!disposition.contains('\r'));
    assert!(!disposition.contains("../"));
    assert!(!disposition.contains('<'));
    assert!(!disposition.contains('>'));
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], b"<script>alert(1)</script>");
}

#[tokio::test]
async fn artifacts_page_lists_recent_job_outputs_with_download_links() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "artifact-list");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    let workflow = fixture
        .state
        .db
        .workflows_for_repo(&repo.id)
        .expect("workflows")
        .into_iter()
        .next()
        .expect("workflow");
    let pipeline_id = scheduler::enqueue_workflow_run(
        Arc::clone(&fixture.state),
        &workflow,
        "manual",
        Some("refs/heads/main"),
        Some("abc123"),
    )
    .expect("pipeline");
    let job_id = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline_id)
        .expect("snapshot")
        .expect("pipeline")
        .jobs[0]
        .run
        .id
        .clone();
    let pending = fixture
        .state
        .artifacts
        .store_bytes("job_output", &job_id, "release.tar.gz", b"release")
        .expect("artifact");
    let artifact_id = fixture
        .state
        .db
        .insert_server_artifact(&pending)
        .expect("artifact record");
    fixture
        .state
        .db
        .finish_job_run(
            &job_id,
            "success",
            Some(10),
            Some(0),
            None,
            None,
            &JobOutputMetadata::default(),
            "",
            "",
            &[JobRunOutput {
                output_name: "release.tar.gz".to_string(),
                kind: "artifact".to_string(),
                runner_artifact_id: Some("runner-artifact".to_string()),
                server_artifact_id: Some(artifact_id.clone()),
                value: None,
                sha256: Some(pending.sha256.clone()),
                size_bytes: Some(pending.size_bytes),
            }],
        )
        .expect("finish job");
    let source = fixture
        .state
        .artifacts
        .store_bytes("pipeline_source", &pipeline_id, "source.tar.gz", b"source")
        .expect("source artifact");
    fixture
        .state
        .db
        .insert_server_artifact(&source)
        .expect("source artifact record");

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::get("/artifacts")
                .header(
                    "cookie",
                    session_cookie_value(&fixture.state, &fixture.user.id),
                )
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let html = String::from_utf8(body.to_vec()).expect("html");
    assert!(html.contains("Latest artifacts"));
    assert!(html.contains("release.tar.gz"));
    assert!(html.contains("artifact-list"));
    assert!(html.contains(&format!("href=\"/artifacts/{artifact_id}\"")));
    assert!(html.contains(&format!("href=\"/pipelines/{pipeline_id}\"")));
    assert!(!html.contains("source.tar.gz"));
}

#[tokio::test]
async fn scheduler_tasks_stop_on_shutdown_signal() {
    let fixture = test_fixture().await;
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handles = scheduler::spawn(Arc::clone(&fixture.state), shutdown_rx);
    shutdown_tx.send(true).expect("shutdown signal");
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        scheduler::shutdown(handles),
    )
    .await
    .expect("scheduler shutdown");
}

#[tokio::test]
async fn api_create_workflow_rejects_empty_branch_entry() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "api-empty-workflow-branch");
    let token = csrf_token(&fixture.state, &fixture.user);
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/workflows")
                .header("content-type", "application/json")
                .header("cookie", cookie)
                .body(Body::from(
                    json!({
                        "csrf_token": token,
                        "repo_id": repo.id,
                        "name": "wf",
                        "enabled": true,
                        "trigger_kind": "push",
                        "branches": [" "],
                        "jobs": []
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let payload: JsonValue = serde_json::from_slice(&body).expect("json");
    assert_eq!(payload["error"]["code"], json!("bad_request"));
    assert_eq!(
        payload["error"]["message"],
        json!("workflow branch cannot be empty")
    );
}

#[tokio::test]
async fn repo_manual_trigger_rejects_whitespace_commit() {
    let fixture = test_fixture().await;
    let repo = create_repo_direct(
        &fixture.state,
        &fixture.user,
        "manual-trigger-invalid-commit",
    );
    let token = csrf_token(&fixture.state, &fixture.user);
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let body = form_urlencoded::Serializer::new(String::new())
        .append_pair("csrf_token", &token)
        .append_pair("branch", "refs/heads/main")
        .append_pair("commit", "bad ref")
        .finish();

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!("/repos/{}/trigger", repo.id))
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let text = String::from_utf8(body.to_vec()).expect("text");
    assert!(text.contains("commit must not contain whitespace"));
}

#[tokio::test]
async fn repo_form_validation_rerenders_form_with_submitted_values() {
    let fixture = test_fixture().await;
    let token = csrf_token(&fixture.state, &fixture.user);
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let body = form_urlencoded::Serializer::new(String::new())
        .append_pair("csrf_token", &token)
        .append_pair("name", "demo-invalid")
        .append_pair("default_branch", "bad branch")
        .finish();

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/repos")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let html = String::from_utf8(body.to_vec()).expect("html");
    assert!(html.contains("default branch is invalid"));
    assert!(html.contains("value=\"demo-invalid\""));
    assert!(html.contains("Create repository"));
}

#[tokio::test]
async fn pipeline_events_requires_authorized_pipeline() {
    let fixture = test_fixture().await;
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::get("/pipelines/missing/events")
                .header("cookie", cookie)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn workflow_form_submission_accepts_structured_jobs_json() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    let token = csrf_token(&fixture.state, &fixture.user);
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let jobs_json = serde_json::to_string(&vec![json!({
        "runner_id": fixture.runner_id,
        "runner_job_name": "build-app",
        "inputs": {
            "commit": { "kind": "commit" },
            "branch": { "kind": "branch" },
            "source": { "kind": "source_artifact" }
        },
        "outcome_policy": "allowed_to_fail"
    })])
    .expect("jobs json");
    let body = form_urlencoded::Serializer::new(String::new())
        .append_pair("csrf_token", &token)
        .append_pair("repo_id", &repo.id)
        .append_pair("name", "wf")
        .append_pair("trigger_kind", "push")
        .append_pair("branch_name", "main")
        .append_pair("jobs_json", &jobs_json)
        .finish();

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/workflows")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::SEE_OTHER);

    let workflow = fixture
        .state
        .db
        .workflows_for_repo(&repo.id)
        .expect("workflows")
        .into_iter()
        .find(|workflow| workflow.name == "wf")
        .expect("workflow");
    let definition: WorkflowDefinition =
        serde_json::from_str(&workflow.definition_json).expect("definition");
    assert_eq!(definition.jobs.len(), 1);
    assert_eq!(definition.jobs[0].runner_job_name, "build-app");
    assert_eq!(
        definition.jobs[0].outcome_policy,
        WorkflowJobOutcomePolicy::AllowedToFail
    );
    assert_eq!(
        definition.jobs[0].inputs.get("source"),
        Some(&WorkflowInputBinding::SourceArtifact)
    );
}

#[tokio::test]
async fn workflow_form_rejects_missing_required_runner_input() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo-missing-required-input");
    let token = csrf_token(&fixture.state, &fixture.user);
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let jobs_json = serde_json::to_string(&vec![json!({
        "runner_id": fixture.runner_id,
        "runner_job_name": "build-app",
        "inputs": {
            "commit": { "kind": "commit" },
            "branch": { "kind": "branch" }
        },
        "outcome_policy": "required"
    })])
    .expect("jobs json");
    let body = form_urlencoded::Serializer::new(String::new())
        .append_pair("csrf_token", &token)
        .append_pair("repo_id", &repo.id)
        .append_pair("name", "wf")
        .append_pair("trigger_kind", "push")
        .append_pair("branch_name", "main")
        .append_pair("jobs_json", &jobs_json)
        .finish();

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/workflows")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let text = String::from_utf8(body.to_vec()).expect("text");
    assert!(text.contains("workflow job 1 missing required input source for runner job build-app"));
}

#[tokio::test]
async fn workflow_form_rejects_empty_required_literal_input() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    fixture
        .state
        .db
        .replace_runner_jobs(
            &fixture.runner_id,
            &[runner_job_definition(
                r#"{"name":"release","timeout_seconds":60,"inputs":{"version":{"type":"string","required":true}},"outputs":{}}"#,
            )],
        )
        .expect("runner jobs");
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo-empty-required-input");
    let token = csrf_token(&fixture.state, &fixture.user);
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let jobs_json = serde_json::to_string(&vec![json!({
        "runner_id": fixture.runner_id,
        "runner_job_name": "release",
        "inputs": {
            "version": { "kind": "literal", "value": "  " }
        },
        "outcome_policy": "required"
    })])
    .expect("jobs json");
    let body = form_urlencoded::Serializer::new(String::new())
        .append_pair("csrf_token", &token)
        .append_pair("repo_id", &repo.id)
        .append_pair("name", "wf")
        .append_pair("trigger_kind", "push")
        .append_pair("branch_name", "main")
        .append_pair("jobs_json", &jobs_json)
        .finish();

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/workflows")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let text = String::from_utf8(body.to_vec()).expect("text");
    assert!(text.contains("workflow job 1 missing required input version for runner job release"));
}

#[tokio::test]
async fn workflow_form_rejects_missing_job_output_reference() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    fixture
        .state
        .db
        .replace_runner_jobs(
            &fixture.runner_id,
            &[
                runner_job_definition(
                    r#"{"name":"produce","timeout_seconds":60,"inputs":{},"outputs":{"version":{"type":"string","required":true}}}"#,
                ),
                runner_job_definition(
                    r#"{"name":"consume","timeout_seconds":60,"inputs":{"version":{"type":"string","required":true}},"outputs":{}}"#,
                ),
            ],
        )
        .expect("runner jobs");
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo-missing-output");
    let token = csrf_token(&fixture.state, &fixture.user);
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let jobs_json = serde_json::to_string(&vec![
        json!({
            "runner_id": fixture.runner_id,
            "runner_job_name": "produce",
            "inputs": {},
            "outcome_policy": "required"
        }),
        json!({
            "runner_id": fixture.runner_id,
            "runner_job_name": "consume",
            "inputs": {
                "version": { "kind": "job_output", "job_index": 0, "output_name": "missing" }
            },
            "outcome_policy": "required"
        }),
    ])
    .expect("jobs json");
    let body = form_urlencoded::Serializer::new(String::new())
        .append_pair("csrf_token", &token)
        .append_pair("repo_id", &repo.id)
        .append_pair("name", "wf")
        .append_pair("trigger_kind", "push")
        .append_pair("branch_name", "main")
        .append_pair("jobs_json", &jobs_json)
        .finish();

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/workflows")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let text = String::from_utf8(body.to_vec()).expect("text");
    assert!(text.contains("workflow input version references missing output job-1.missing"));
}

#[tokio::test]
async fn workflow_form_rejects_typed_output_input_mismatch() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    fixture
        .state
        .db
        .replace_runner_jobs(
            &fixture.runner_id,
            &[
                runner_job_definition(
                    r#"{"name":"produce","timeout_seconds":60,"inputs":{},"outputs":{"build_number":{"type":"integer","required":true}}}"#,
                ),
                runner_job_definition(
                    r#"{"name":"consume","timeout_seconds":60,"inputs":{"build_number":{"type":"string","required":true}},"outputs":{}}"#,
                ),
            ],
        )
        .expect("runner jobs");
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo-mismatch");
    let token = csrf_token(&fixture.state, &fixture.user);
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let jobs_json = serde_json::to_string(&vec![
        json!({
            "runner_id": fixture.runner_id,
            "runner_job_name": "produce",
            "inputs": {},
            "outcome_policy": "required"
        }),
        json!({
            "runner_id": fixture.runner_id,
            "runner_job_name": "consume",
            "inputs": {
                "build_number": { "kind": "job_output", "job_index": 0, "output_name": "build_number" }
            },
            "outcome_policy": "required"
        }),
    ])
    .expect("jobs json");
    let body = form_urlencoded::Serializer::new(String::new())
        .append_pair("csrf_token", &token)
        .append_pair("repo_id", &repo.id)
        .append_pair("name", "wf")
        .append_pair("trigger_kind", "push")
        .append_pair("branch_name", "main")
        .append_pair("jobs_json", &jobs_json)
        .finish();

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/workflows")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let text = String::from_utf8(body.to_vec()).expect("text");
    assert!(
        text.contains(
            "workflow input build_number expects string but job-1.build_number is integer"
        )
    );
}

#[tokio::test]
async fn workflow_form_rejects_literal_input_type_mismatch() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    fixture
        .state
        .db
        .replace_runner_jobs(
            &fixture.runner_id,
            &[runner_job_definition(
                r#"{"name":"build-app","timeout_seconds":60,"inputs":{"published":{"type":"boolean","required":true}},"outputs":{}}"#,
            )],
        )
        .expect("runner jobs");
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo-literal-mismatch");
    let token = csrf_token(&fixture.state, &fixture.user);
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let jobs_json = serde_json::to_string(&vec![json!({
        "runner_id": fixture.runner_id,
        "runner_job_name": "build-app",
        "inputs": {
            "published": { "kind": "literal", "value": "true" }
        },
        "outcome_policy": "required"
    })])
    .expect("jobs json");
    let body = form_urlencoded::Serializer::new(String::new())
        .append_pair("csrf_token", &token)
        .append_pair("repo_id", &repo.id)
        .append_pair("name", "wf")
        .append_pair("trigger_kind", "push")
        .append_pair("branch_name", "main")
        .append_pair("jobs_json", &jobs_json)
        .finish();

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/workflows")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let text = String::from_utf8(body.to_vec()).expect("text");
    assert!(text.contains("workflow input published expects boolean but got string"));
}

#[tokio::test]
async fn workflow_form_rejects_unknown_literal_input_name() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo-unknown-input");
    let token = csrf_token(&fixture.state, &fixture.user);
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let jobs_json = serde_json::to_string(&vec![json!({
        "runner_id": fixture.runner_id,
        "runner_job_name": "build-app",
        "inputs": {
            "bogus": { "kind": "literal", "value": 123 }
        },
        "outcome_policy": "required"
    })])
    .expect("jobs json");
    let body = form_urlencoded::Serializer::new(String::new())
        .append_pair("csrf_token", &token)
        .append_pair("repo_id", &repo.id)
        .append_pair("name", "wf")
        .append_pair("trigger_kind", "push")
        .append_pair("branch_name", "main")
        .append_pair("jobs_json", &jobs_json)
        .finish();

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/workflows")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let text = String::from_utf8(body.to_vec()).expect("text");
    assert!(text.contains("workflow job 1 provides unknown input bogus for runner job build-app"));
}

#[test]
fn hook_ingestion_is_idempotent() {
    let dir = temp_dir("hook_idempotent");
    let config_path = write_test_config(&dir);
    let state = build_state(config_path, PathBuf::from("/bin/janus-server")).expect("state");
    let hash = hash_password("password123").expect("hash");
    state
        .db
        .create_user("alice", &hash, UserRole::Admin)
        .expect("user");
    let _user = state
        .db
        .get_user_credentials("alice")
        .expect("user")
        .unwrap()
        .0;
    let repo_id = state
        .db
        .create_repo(
            "demo",
            "demo",
            &dir.join("repos/demo.git").display().to_string(),
            "main",
        )
        .expect("repo");
    let refs = vec![crate::models::PushEventRef {
        old_rev: "0".repeat(40),
        new_rev: "1".repeat(40),
        ref_name: "refs/heads/main".to_string(),
    }];
    let key = git::event_key(&repo_id, &refs);
    state
        .db
        .create_push_event(&repo_id, &key, &refs)
        .expect("push1");
    state
        .db
        .create_push_event(&repo_id, &key, &refs)
        .expect("push2");
    let events = state.db.list_unprocessed_push_events().expect("events");
    assert_eq!(events.len(), 1);
}

#[test]
fn bootstrap_admin_only_works_when_no_users_exist() {
    let dir = temp_dir("bootstrap_admin_once");
    let config_path = write_test_config(&dir);

    bootstrap_admin(&config_path, "admin", "password123").expect("bootstrap should create admin");
    let state =
        build_state(config_path.clone(), PathBuf::from("/bin/janus-server")).expect("state");
    let users = state.db.list_users().expect("users");
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].username, "admin");
    assert_eq!(users[0].role, UserRole::Admin);

    let error = bootstrap_admin(&config_path, "other-admin", "password123")
        .expect_err("second bootstrap should fail");
    assert!(
        error
            .to_string()
            .contains("bootstrap-admin can only run when no users exist")
    );
}

#[tokio::test]
async fn push_event_creates_pipeline_and_dispatches_job() {
    let mock = spawn_mock_runner().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "event-1",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        )
        .expect("push event");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("reconcile");
    let pipelines = fixture.state.db.list_pipeline_runs().expect("pipelines");
    assert_eq!(pipelines.len(), 1);
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipelines[0].id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.jobs.len(), 1);
    assert_eq!(snapshot.jobs[0].run.status, "running");
}

#[tokio::test]
async fn runner_health_refresh_marks_supported_protocol_healthy() {
    let mock = spawn_mock_runner().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;

    scheduler::refresh_runner_health(Arc::clone(&fixture.state))
        .await
        .expect("refresh");

    let runner = fixture
        .state
        .db
        .get_runner(&fixture.runner_id)
        .expect("runner")
        .expect("runner");
    assert_eq!(runner.last_health_state, "healthy");
    let jobs = fixture
        .state
        .db
        .list_runner_jobs(&fixture.runner_id)
        .expect("jobs");
    assert!(jobs.iter().any(|job| job.name == "build-app"));
}

#[tokio::test]
async fn runner_health_refresh_marks_unsupported_protocol_incompatible() {
    let mock = spawn_mock_runner_with_unsupported_protocol().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;

    scheduler::refresh_runner_health(Arc::clone(&fixture.state))
        .await
        .expect("refresh");

    let runner = fixture
        .state
        .db
        .get_runner(&fixture.runner_id)
        .expect("runner")
        .expect("runner");
    assert_eq!(runner.last_health_state, "incompatible");
}

#[tokio::test]
async fn scheduler_passes_typed_outputs_to_downstream_job_inputs() {
    let mock = spawn_mock_runner().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let producer_runner_id = fixture.runner_id.clone();
    fixture
        .state
        .db
        .replace_runner_jobs(
            &producer_runner_id,
            &[runner_job_definition(
                r#"{"name":"produce-typed","timeout_seconds":60,"inputs":{},"outputs":{"version":{"type":"string","required":true},"build_number":{"type":"integer","required":true},"published":{"type":"boolean","required":true},"metadata":{"type":"json","required":true}}}"#,
            )],
        )
        .expect("producer jobs");
    let consumer_runner_id = fixture
        .state
        .db
        .create_runner("consumer-runner", &mock.base_url)
        .expect("consumer runner");
    fixture
        .state
        .db
        .replace_runner_jobs(
            &consumer_runner_id,
            &[runner_job_definition(
                r#"{"name":"consume-typed","timeout_seconds":60,"inputs":{"version":{"type":"string","required":true},"build_number":{"type":"integer","required":true},"published":{"type":"boolean","required":true},"metadata":{"type":"json","required":true}},"outputs":{}}"#,
            )],
        )
        .expect("consumer jobs");
    fixture
        .state
        .db
        .update_runner_health(&consumer_runner_id, "healthy")
        .expect("consumer health");

    let repo = create_repo_direct(&fixture.state, &fixture.user, "typed-chain");
    create_workflow_with_jobs_direct(
        &fixture.state,
        &repo.id,
        vec![
            WorkflowJobDefinition {
                runner_id: producer_runner_id,
                runner_job_name: "produce-typed".to_string(),
                inputs: BTreeMap::new(),
                outcome_policy: WorkflowJobOutcomePolicy::Required,
            },
            WorkflowJobDefinition {
                runner_id: consumer_runner_id,
                runner_job_name: "consume-typed".to_string(),
                inputs: BTreeMap::from([
                    (
                        "version".to_string(),
                        binding(
                            json!({ "kind": "job_output", "job_index": 0, "output_name": "version" }),
                        ),
                    ),
                    (
                        "build_number".to_string(),
                        binding(
                            json!({ "kind": "job_output", "job_index": 0, "output_name": "build_number" }),
                        ),
                    ),
                    (
                        "published".to_string(),
                        binding(
                            json!({ "kind": "job_output", "job_index": 0, "output_name": "published" }),
                        ),
                    ),
                    (
                        "metadata".to_string(),
                        binding(
                            json!({ "kind": "job_output", "job_index": 0, "output_name": "metadata" }),
                        ),
                    ),
                ]),
                outcome_policy: WorkflowJobOutcomePolicy::Required,
            },
        ],
    );
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "typed-event-1",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        )
        .expect("push event");

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch producer");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll producer 1");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll producer 2");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch consumer");

    let requests = mock.requests_for("consume-typed");
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request["version"], json!("v1.2.3"));
    assert_eq!(request["build_number"], json!(42));
    assert_eq!(request["published"], json!(true));
    assert_eq!(request["metadata"], json!({ "commit": "abc123" }));
    assert!(request["janus_job_run_id"].is_string());
}

#[tokio::test]
async fn scheduler_rejects_mismatched_typed_output_binding() {
    let mock = spawn_mock_runner().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let producer_runner_id = fixture.runner_id.clone();
    fixture
        .state
        .db
        .replace_runner_jobs(
            &producer_runner_id,
            &[runner_job_definition(
                r#"{"name":"produce-int","timeout_seconds":60,"inputs":{},"outputs":{"build_number":{"type":"integer","required":true}}}"#,
            )],
        )
        .expect("producer jobs");
    let consumer_runner_id = fixture
        .state
        .db
        .create_runner("consumer-mismatch", &mock.base_url)
        .expect("consumer runner");
    fixture
        .state
        .db
        .replace_runner_jobs(
            &consumer_runner_id,
            &[runner_job_definition(
                r#"{"name":"consume-string","timeout_seconds":60,"inputs":{"build_number":{"type":"string","required":true}},"outputs":{}}"#,
            )],
        )
        .expect("consumer jobs");
    fixture
        .state
        .db
        .update_runner_health(&consumer_runner_id, "healthy")
        .expect("consumer health");

    let repo = create_repo_direct(&fixture.state, &fixture.user, "typed-mismatch");
    create_workflow_with_jobs_direct(
        &fixture.state,
        &repo.id,
        vec![
            WorkflowJobDefinition {
                runner_id: producer_runner_id,
                runner_job_name: "produce-int".to_string(),
                inputs: BTreeMap::new(),
                outcome_policy: WorkflowJobOutcomePolicy::Required,
            },
            WorkflowJobDefinition {
                runner_id: consumer_runner_id,
                runner_job_name: "consume-string".to_string(),
                inputs: BTreeMap::from([(
                    "build_number".to_string(),
                    binding(
                        json!({ "kind": "job_output", "job_index": 0, "output_name": "build_number" }),
                    ),
                )]),
                outcome_policy: WorkflowJobOutcomePolicy::Required,
            },
        ],
    );
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "typed-event-2",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        )
        .expect("push event");

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch producer");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll producer 1");
    let error = scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect_err("mismatch should fail before dispatch");
    assert!(
        error.to_string().contains(
            "workflow input build_number expects string but job-1.build_number is integer"
        )
    );
    assert!(mock.requests_for("consume-string").is_empty());
}

#[tokio::test]
async fn scheduler_persists_terminal_runner_state() {
    let mock = spawn_mock_runner().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "event-2",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        )
        .expect("push event");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("reconcile1");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("reconcile2");
    let pipeline = fixture
        .state
        .db
        .list_pipeline_runs()
        .expect("pipelines")
        .remove(0);
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "success");
    assert_eq!(snapshot.jobs[0].run.status, "success");
    assert_eq!(snapshot.jobs[0].run.exit_code, Some(0));
    assert_eq!(
        snapshot.jobs[0].run.terminal_reason.as_deref(),
        Some("success")
    );
    assert_eq!(snapshot.jobs[0].run.failure_category, None);
    assert_eq!(snapshot.jobs[0].run.output_metadata.artifacts.count, 0);
    assert!(snapshot.jobs[0].stdout.contains("ok"));
}

#[tokio::test]
async fn scheduler_persists_timeout_runner_state() {
    let mock = spawn_mock_runner_with_timeout().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "event-timeout",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        )
        .expect("push event");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("reconcile1");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("reconcile2");
    let pipeline = fixture
        .state
        .db
        .list_pipeline_runs()
        .expect("pipelines")
        .remove(0);
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "failed");
    assert_eq!(snapshot.jobs[0].run.status, "failed");
    assert_eq!(
        snapshot.jobs[0].run.terminal_reason.as_deref(),
        Some("timeout")
    );
    assert_eq!(
        snapshot.jobs[0].run.failure_category.as_deref(),
        Some("timeout")
    );
    assert_eq!(snapshot.jobs[0].run.exit_code, None);
}

#[tokio::test]
async fn scheduler_persists_job_failure_runner_state() {
    let mock = spawn_mock_runner_with_job_failure().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "event-job-failed",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        )
        .expect("push event");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("reconcile1");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("reconcile2");
    let pipeline = fixture
        .state
        .db
        .list_pipeline_runs()
        .expect("pipelines")
        .remove(0);
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "failed");
    assert_eq!(snapshot.jobs[0].run.status, "failed");
    assert_eq!(
        snapshot.jobs[0].run.terminal_reason.as_deref(),
        Some("exit_code")
    );
    assert_eq!(
        snapshot.jobs[0].run.failure_category.as_deref(),
        Some("job")
    );
    assert_eq!(snapshot.jobs[0].run.exit_code, Some(7));
}

#[tokio::test]
async fn scheduler_reuses_dispatch_key_after_ambiguous_runner_create_failure() {
    let mock = spawn_mock_runner_with_fail_first_dispatch().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    let workflow = fixture
        .state
        .db
        .workflows_for_repo(&repo.id)
        .expect("workflows")
        .remove(0);
    let commit_sha = "1".repeat(40);
    let pipeline_id = scheduler::enqueue_workflow_run(
        Arc::clone(&fixture.state),
        &workflow,
        "push",
        Some("refs/heads/main"),
        Some(&commit_sha),
    )
    .expect("enqueue");

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect_err("first dispatch should fail ambiguously");
    let pipeline = fixture
        .state
        .db
        .get_pipeline_run(&pipeline_id)
        .expect("pipeline")
        .expect("pipeline should exist");
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.jobs[0].run.status, "pending");
    assert_eq!(mock.dispatch_count(), 1);

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("reconcile2");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("reconcile3");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("reconcile4");

    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "success");
    assert_eq!(snapshot.jobs[0].run.status, "success");
    assert_eq!(mock.dispatch_count(), 1);
}

#[tokio::test]
async fn scheduler_marks_runner_create_bad_request_failed() {
    let mock = spawn_mock_runner_with_create_bad_request().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    let workflow = fixture
        .state
        .db
        .workflows_for_repo(&repo.id)
        .expect("workflows")
        .remove(0);
    let pipeline_id = scheduler::enqueue_workflow_run(
        Arc::clone(&fixture.state),
        &workflow,
        "push",
        Some("refs/heads/main"),
        Some(&"1".repeat(40)),
    )
    .expect("enqueue");

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch should be handled");
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline_id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "failed");
    assert_eq!(snapshot.jobs[0].run.status, "failed");
    assert_eq!(
        snapshot.jobs[0].run.terminal_reason.as_deref(),
        Some("spawn_error")
    );
    assert_eq!(
        snapshot.jobs[0].run.failure_category.as_deref(),
        Some("job")
    );
    assert!(snapshot.jobs[0].stderr.contains("runner_input_invalid"));
    assert_eq!(mock.dispatch_count(), 1);

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("extra reconcile");
    assert_eq!(mock.dispatch_count(), 1);
}

#[tokio::test]
async fn enqueue_workflow_run_allows_stale_schema() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "stale-enqueue");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .replace_runner_jobs(
            &fixture.runner_id,
            &[runner_job_definition(
                r#"{"name":"build-app","timeout_seconds":60,"inputs":{"commit":{"type":"string","required":true},"branch":{"type":"string","required":false},"source":{"type":"artifact","required":true}},"outputs":{"app":{"type":"artifact","required":true}}}"#,
            )],
        )
        .expect("runner jobs");
    let workflow = fixture
        .state
        .db
        .workflows_for_repo(&repo.id)
        .expect("workflows")
        .remove(0);

    let pipeline_id = scheduler::enqueue_workflow_run(
        Arc::clone(&fixture.state),
        &workflow,
        "push",
        Some("refs/heads/main"),
        Some(&"1".repeat(40)),
    )
    .expect("stale workflow should still enqueue");

    assert!(!pipeline_id.is_empty());
}

#[tokio::test]
async fn enqueue_workflow_run_blocks_incompatible_schema() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "incompatible-enqueue");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .replace_runner_jobs(&fixture.runner_id, &[])
        .expect("runner jobs");
    let workflow = fixture
        .state
        .db
        .workflows_for_repo(&repo.id)
        .expect("workflows")
        .remove(0);

    let error = scheduler::enqueue_workflow_run(
        Arc::clone(&fixture.state),
        &workflow,
        "push",
        Some("refs/heads/main"),
        Some(&"1".repeat(40)),
    )
    .expect_err("incompatible workflow should be blocked");

    assert!(
        error
            .to_string()
            .contains("incompatible workflow schema for workflow")
    );
}

#[tokio::test]
async fn cancel_pipeline_tracks_runner_cancel_progress() {
    let mock = spawn_mock_runner().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "event-cancel",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        )
        .expect("push event");

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch");
    let pipeline = fixture
        .state
        .db
        .list_pipeline_runs()
        .expect("pipelines")
        .remove(0);

    scheduler::cancel_pipeline(Arc::clone(&fixture.state), &pipeline.id)
        .await
        .expect("cancel");
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "cancel_requested");
    assert_eq!(
        snapshot.pipeline.cancel_reason.as_deref(),
        Some("user_requested")
    );
    assert_eq!(snapshot.jobs[0].run.status, "cancel_requested");
    assert_eq!(
        snapshot.jobs[0].run.cancel_reason.as_deref(),
        Some("user_requested")
    );

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll cancel requested");
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "cancel_requested");
    assert_eq!(snapshot.jobs[0].run.status, "cancel_requested");

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll canceling");
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "canceling");
    assert_eq!(snapshot.jobs[0].run.status, "canceling");

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll canceled");
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "canceled");
    assert_eq!(snapshot.jobs[0].run.status, "canceled");
    assert_eq!(
        snapshot.jobs[0].run.terminal_reason.as_deref(),
        Some("canceled")
    );
    assert_eq!(
        snapshot.jobs[0].run.failure_category.as_deref(),
        Some("canceled")
    );
    assert_eq!(snapshot.jobs[0].run.duration_ms, Some(1000));
}

#[tokio::test]
async fn scheduler_retries_infra_failure_and_recovers() {
    let mock = spawn_mock_runner_with_infra_failure_once().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "event-infra-retry",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        )
        .expect("push event");

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch1");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll infra failure");
    let pipeline = fixture
        .state
        .db
        .list_pipeline_runs()
        .expect("pipelines")
        .remove(0);
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "running");
    assert_eq!(snapshot.jobs[0].run.status, "pending");
    assert_eq!(snapshot.jobs[0].run.infra_retry_count, 1);
    assert!(snapshot.jobs[0].run.last_infra_retry_at.is_some());

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch2");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll success");
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "success");
    assert_eq!(snapshot.jobs[0].run.status, "success");
    assert_eq!(snapshot.jobs[0].run.infra_retry_count, 1);
    assert_eq!(mock.dispatch_count(), 2);
}

#[tokio::test]
async fn scheduler_fails_job_after_infra_retry_budget_exhausted() {
    let mock = spawn_mock_runner_with_infra_failure().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "event-infra-exhaust",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        )
        .expect("push event");

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch1");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll1");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch2");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll2");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch3");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll3");

    let pipeline = fixture
        .state
        .db
        .list_pipeline_runs()
        .expect("pipelines")
        .remove(0);
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "failed");
    assert_eq!(snapshot.jobs[0].run.status, "failed");
    assert_eq!(snapshot.jobs[0].run.infra_retry_count, 2);
    assert_eq!(
        snapshot.jobs[0].run.failure_category.as_deref(),
        Some("infra")
    );
    assert_eq!(
        snapshot.jobs[0].run.terminal_reason.as_deref(),
        Some("spawn_error")
    );
    assert_eq!(mock.dispatch_count(), 3);
}

#[tokio::test]
async fn scheduler_stops_retrying_when_output_artifact_persist_fails() {
    let mock = spawn_mock_runner_with_output_artifact_download_failure().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "event-output-artifact-fail",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        )
        .expect("push event");

    for _ in 0..10 {
        scheduler::reconcile_once(Arc::clone(&fixture.state))
            .await
            .expect("reconcile");
    }

    let pipeline = fixture
        .state
        .db
        .list_pipeline_runs()
        .expect("pipelines")
        .remove(0);
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "failed");
    assert_eq!(snapshot.jobs[0].run.status, "failed");
    assert_eq!(snapshot.jobs[0].run.infra_retry_count, 2);
    assert_eq!(
        snapshot.jobs[0].run.terminal_reason.as_deref(),
        Some("output_registration_failed")
    );
    assert_eq!(
        snapshot.jobs[0].run.failure_category.as_deref(),
        Some("infra")
    );
    assert_eq!(mock.artifact_download_count(), 3);

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("extra reconcile");
    assert_eq!(mock.artifact_download_count(), 3);
}

#[tokio::test]
async fn scheduler_retries_stuck_cancellation() {
    let mock = spawn_mock_runner_with_stuck_cancellation().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "event-stuck-cancel",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        )
        .expect("push event");

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch");
    let pipeline = fixture
        .state
        .db
        .list_pipeline_runs()
        .expect("pipelines")
        .remove(0);

    scheduler::cancel_pipeline(Arc::clone(&fixture.state), &pipeline.id)
        .await
        .expect("cancel");
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    let runner_run_id = snapshot.jobs[0]
        .run
        .runner_run_id
        .clone()
        .expect("runner run id");
    assert_eq!(mock.cancel_count(&runner_run_id), 1);
    assert_eq!(snapshot.jobs[0].run.cancel_retry_count, 0);

    sleep(std::time::Duration::from_millis(1100)).await;
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("reconcile stuck cancel");

    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "cancel_requested");
    assert_eq!(snapshot.jobs[0].run.status, "cancel_requested");
    assert_eq!(snapshot.jobs[0].run.cancel_retry_count, 1);
    assert!(snapshot.jobs[0].run.last_cancel_retry_at.is_some());
    assert!(mock.cancel_count(&runner_run_id) >= 2);
}

#[tokio::test]
async fn scheduler_fails_job_after_cancel_retry_budget_exhausted() {
    let mock = spawn_mock_runner_with_stuck_cancellation().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "event-exhaust-cancel",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        )
        .expect("push event");

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch");
    let pipeline = fixture
        .state
        .db
        .list_pipeline_runs()
        .expect("pipelines")
        .remove(0);

    scheduler::cancel_pipeline(Arc::clone(&fixture.state), &pipeline.id)
        .await
        .expect("cancel");

    sleep(std::time::Duration::from_millis(1100)).await;
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("retry 1");
    sleep(std::time::Duration::from_millis(1100)).await;
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("retry 2");
    sleep(std::time::Duration::from_millis(1100)).await;
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("exhaust");

    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "failed");
    assert_eq!(snapshot.jobs[0].run.status, "failed");
    assert_eq!(
        snapshot.jobs[0].run.cancel_reason.as_deref(),
        Some("stuck_retry_exhausted")
    );
    assert_eq!(snapshot.jobs[0].run.cancel_retry_count, 2);
}

struct TestFixture {
    state: Arc<crate::app::AppState>,
    app: Router,
    user: User,
    runner_id: String,
}

async fn test_fixture() -> TestFixture {
    test_fixture_with_runner("http://127.0.0.1:9").await
}

async fn test_fixture_with_runner(base_url: &str) -> TestFixture {
    test_fixture_with_runner_url_policy_and_runner(base_url, "").await
}

async fn test_fixture_with_runner_url_policy(policy_overrides: &str) -> TestFixture {
    test_fixture_with_runner_url_policy_and_runner("http://127.0.0.1:9", policy_overrides).await
}

async fn test_fixture_with_runner_url_policy_and_runner(
    base_url: &str,
    policy_overrides: &str,
) -> TestFixture {
    let dir = temp_dir("fixture");
    let config_path = write_test_config_with_runner_url_policy(&dir, policy_overrides);
    let state = build_state(config_path, PathBuf::from("/bin/janus-server")).expect("state");
    let hash = hash_password("password123").expect("hash");
    let username = format!("alice-{}", Uuid::now_v7());
    state
        .db
        .create_user(&username, &hash, UserRole::Admin)
        .expect("user");
    let user = state
        .db
        .get_user_credentials(&username)
        .expect("creds")
        .unwrap()
        .0;
    let runner_name = format!("runner-{}", Uuid::now_v7());
    let runner_id = state
        .db
        .create_runner(&runner_name, base_url)
        .expect("runner");
    if base_url != "http://127.0.0.1:9" {
        state
            .db
            .replace_runner_jobs(
                &runner_id,
                &[runner_job_definition(
                    r#"{"name":"build-app","timeout_seconds":60,"inputs":{"commit":{"type":"string","required":true},"branch":{"type":"string","required":true},"source":{"type":"artifact","required":true}},"outputs":{"app":{"type":"artifact","required":true}}}"#,
                )],
            )
            .expect("runner jobs");
        state
            .db
            .update_runner_health(&runner_id, "healthy")
            .expect("health");
    }
    let session_id = state
        .db
        .create_session(&user.id, &(Utc::now() + Duration::days(1)).to_rfc3339())
        .expect("session");
    let app = build_router(Arc::clone(&state));
    let _cookie = session_cookie(
        &state.config.auth.session_secret,
        &session_id,
        state.config.auth.session_cookie_secure,
    );
    TestFixture {
        state,
        app,
        user,
        runner_id,
    }
}

fn create_repo_direct(state: &Arc<crate::app::AppState>, _user: &User, name: &str) -> Repo {
    let path = PathBuf::from(&state.config.repos_dir).join(format!("{name}.git"));
    let repo_id = state
        .db
        .create_repo(name, name, &path.display().to_string(), "main")
        .expect("repo");
    state.db.get_repo(&repo_id).expect("repo").unwrap()
}

fn create_workflow_direct(state: &Arc<crate::app::AppState>, repo_id: &str, runner_id: &str) {
    let trigger = serde_json::to_string(&WorkflowTrigger {
        kind: "push".to_string(),
        branches: vec!["main".to_string()],
    })
    .expect("trigger");
    let jobs = vec![WorkflowJobDefinition {
        runner_id: runner_id.to_string(),
        runner_job_name: "build-app".to_string(),
        inputs: BTreeMap::from([
            ("commit".to_string(), WorkflowInputBinding::Commit),
            ("branch".to_string(), WorkflowInputBinding::Branch),
        ]),
        outcome_policy: WorkflowJobOutcomePolicy::Required,
    }];
    let definition =
        serde_json::to_string(&WorkflowDefinition { jobs: jobs.clone() }).expect("definition");
    let job_schemas = workflow_job_schemas(state, &jobs);
    state
        .db
        .create_workflow(repo_id, "wf", true, &trigger, &definition, &job_schemas)
        .expect("workflow");
}

fn create_manual_workflow_direct(
    state: &Arc<crate::app::AppState>,
    repo_id: &str,
    runner_id: &str,
) {
    let trigger = serde_json::to_string(&WorkflowTrigger {
        kind: "manual".to_string(),
        branches: vec!["main".to_string()],
    })
    .expect("trigger");
    let jobs = vec![WorkflowJobDefinition {
        runner_id: runner_id.to_string(),
        runner_job_name: "build-app".to_string(),
        inputs: BTreeMap::from([
            ("commit".to_string(), WorkflowInputBinding::Commit),
            ("branch".to_string(), WorkflowInputBinding::Branch),
        ]),
        outcome_policy: WorkflowJobOutcomePolicy::Required,
    }];
    let definition =
        serde_json::to_string(&WorkflowDefinition { jobs: jobs.clone() }).expect("definition");
    let job_schemas = workflow_job_schemas(state, &jobs);
    state
        .db
        .create_workflow(repo_id, "wf", true, &trigger, &definition, &job_schemas)
        .expect("workflow");
}

fn create_workflow_with_jobs_direct(
    state: &Arc<crate::app::AppState>,
    repo_id: &str,
    jobs: Vec<WorkflowJobDefinition>,
) {
    let trigger = serde_json::to_string(&WorkflowTrigger {
        kind: "push".to_string(),
        branches: vec!["main".to_string()],
    })
    .expect("trigger");
    let definition =
        serde_json::to_string(&WorkflowDefinition { jobs: jobs.clone() }).expect("definition");
    let job_schemas = workflow_job_schemas(state, &jobs);
    state
        .db
        .create_workflow(repo_id, "wf", true, &trigger, &definition, &job_schemas)
        .expect("workflow");
}

fn binding(value: JsonValue) -> WorkflowInputBinding {
    serde_json::from_value(value).expect("workflow input binding")
}

fn workflow_job_schemas(
    state: &Arc<crate::app::AppState>,
    jobs: &[WorkflowJobDefinition],
) -> Vec<RunnerJobDefinition> {
    jobs.iter()
        .map(|job| {
            state
                .db
                .list_runner_jobs(&job.runner_id)
                .expect("runner jobs")
                .into_iter()
                .find(|schema| schema.name == job.runner_job_name)
                .expect("runner job schema for workflow job")
        })
        .collect::<Vec<_>>()
}

fn runner_job_definition(schema: &str) -> RunnerJobDefinition {
    serde_json::from_str(schema).expect("runner job schema")
}

fn admin_user(state: &Arc<crate::app::AppState>) -> User {
    state
        .db
        .list_users()
        .expect("users")
        .into_iter()
        .find(|user| user.role.is_admin())
        .expect("admin user")
}

fn write_test_config(dir: &Path) -> PathBuf {
    write_test_config_with_runner_url_policy(dir, "")
}

fn write_test_config_with_runner_url_policy(dir: &Path, policy_overrides: &str) -> PathBuf {
    let config_path = dir.join("server.toml");
    fs::create_dir_all(dir.join("data")).expect("data dir");
    fs::create_dir_all(dir.join("repos")).expect("repos dir");
    let runner_url_policy = if policy_overrides.trim().is_empty() {
        r#"require_https = true
allow_credentials = false
allow_query = false
allow_fragment = false
allow_path = false
allow_localhost = false
allow_private_ips = false
allow_link_local_ips = false
allow_documentation_ips = false
allow_multicast_ips = false"#
            .to_string()
    } else {
        policy_overrides.to_string()
    };
    fs::write(
        &config_path,
        format!(
            r#"data_dir = "{}"
repos_dir = "{}"

[database]
path = "{}"

[server]
listen = "127.0.0.1:0"
public_base_url = "ci.test"

[auth]
session_secret = "test-secret"
session_ttl_days = 1
session_cookie_secure = false
login_rate_limit_per_minute = 100

[runner_auth]
key_id = "test-server"
private_key_path = "{}/runner-signing.key"
public_key_path = "{}/runner-signing.pub"

[scheduler]
poll_interval_ms = 50
cancel_stuck_timeout_seconds = 1
max_cancel_retries = 2
max_infra_retries = 2

[runners]
healthcheck_interval_seconds = 60
connect_timeout_seconds = 5
request_timeout_seconds = 120

[runner_url_policy]
{}

[limits]
request_body_bytes = 1048576
runner_json_bytes = 4194304
runner_logs_bytes = 8388608
runner_artifact_bytes = 268435456
runner_error_bytes = 16384
server_artifact_bytes = 268435456
"#,
            dir.join("data").display(),
            dir.join("repos").display(),
            dir.join("data/server.sqlite3").display(),
            dir.join("data").display(),
            dir.join("data").display(),
            runner_url_policy,
        ),
    )
    .expect("config");
    config_path
}

fn temp_dir(label: &str) -> PathBuf {
    let suffix = Uuid::now_v7().simple().to_string();
    let dir = std::env::temp_dir().join(format!("janus-server-{label}-{suffix}"));
    fs::create_dir_all(&dir).expect("temp dir");
    dir
}

fn session_cookie_value(state: &Arc<crate::app::AppState>, user_id: &str) -> String {
    let session_id = state
        .db
        .create_session(user_id, &(Utc::now() + Duration::days(1)).to_rfc3339())
        .expect("session");
    session_cookie(
        &state.config.auth.session_secret,
        &session_id,
        state.config.auth.session_cookie_secure,
    )
    .to_string()
}

struct MockRunnerState {
    runs: Mutex<BTreeMap<String, MockRun>>,
    dispatches: Mutex<BTreeMap<String, String>>,
    requests: Mutex<Vec<MockRequest>>,
    cancel_requests: Mutex<BTreeMap<String, usize>>,
    artifact_downloads: AtomicUsize,
    fail_first_dispatch: AtomicBool,
    stall_cancellation: AtomicBool,
    terminal_outcome: MockTerminalOutcome,
    capabilities: janus_lib::RunnerCapabilitiesResponse,
}

#[derive(Debug, Clone)]
struct MockRun {
    polls: usize,
    cancel_stage: Option<u8>,
    job_name: String,
}

#[derive(Debug, Clone)]
struct MockRequest {
    job_name: String,
    body: JsonValue,
}

#[derive(Debug, Clone, Copy)]
enum MockTerminalOutcome {
    Success,
    Timeout,
    JobFailed,
    InfraFailed,
    InfraFailOnceThenSuccess,
    OutputArtifactDownloadFailed,
    CreateBadRequest,
}

struct MockRunner {
    base_url: String,
    state: Arc<MockRunnerState>,
}

async fn spawn_mock_runner() -> MockRunner {
    spawn_mock_runner_with_options(false, false, MockTerminalOutcome::Success).await
}

async fn spawn_mock_runner_with_fail_first_dispatch() -> MockRunner {
    spawn_mock_runner_with_options(true, false, MockTerminalOutcome::Success).await
}

async fn spawn_mock_runner_with_create_bad_request() -> MockRunner {
    spawn_mock_runner_with_options(false, false, MockTerminalOutcome::CreateBadRequest).await
}

async fn spawn_mock_runner_with_stuck_cancellation() -> MockRunner {
    spawn_mock_runner_with_options(false, true, MockTerminalOutcome::Success).await
}

async fn spawn_mock_runner_with_timeout() -> MockRunner {
    spawn_mock_runner_with_options(false, false, MockTerminalOutcome::Timeout).await
}

async fn spawn_mock_runner_with_output_artifact_download_failure() -> MockRunner {
    spawn_mock_runner_with_options(
        false,
        false,
        MockTerminalOutcome::OutputArtifactDownloadFailed,
    )
    .await
}

async fn spawn_mock_runner_with_job_failure() -> MockRunner {
    spawn_mock_runner_with_options(false, false, MockTerminalOutcome::JobFailed).await
}

async fn spawn_mock_runner_with_infra_failure() -> MockRunner {
    spawn_mock_runner_with_options(false, false, MockTerminalOutcome::InfraFailed).await
}

async fn spawn_mock_runner_with_infra_failure_once() -> MockRunner {
    spawn_mock_runner_with_options(false, false, MockTerminalOutcome::InfraFailOnceThenSuccess)
        .await
}

async fn spawn_mock_runner_with_unsupported_protocol() -> MockRunner {
    let mut capabilities = janus_lib::RunnerCapabilitiesResponse::current();
    capabilities.protocol_version = janus_lib::RUNNER_PROTOCOL_VERSION + 1;
    capabilities.supported_protocol_versions = vec![janus_lib::RUNNER_PROTOCOL_VERSION + 1];
    spawn_mock_runner_with_options_and_capabilities(
        false,
        false,
        MockTerminalOutcome::Success,
        capabilities,
    )
    .await
}

async fn spawn_mock_runner_with_options(
    fail_first_dispatch: bool,
    stall_cancellation: bool,
    terminal_outcome: MockTerminalOutcome,
) -> MockRunner {
    spawn_mock_runner_with_options_and_capabilities(
        fail_first_dispatch,
        stall_cancellation,
        terminal_outcome,
        janus_lib::RunnerCapabilitiesResponse::current(),
    )
    .await
}

async fn spawn_mock_runner_with_options_and_capabilities(
    fail_first_dispatch: bool,
    stall_cancellation: bool,
    terminal_outcome: MockTerminalOutcome,
    capabilities: janus_lib::RunnerCapabilitiesResponse,
) -> MockRunner {
    let state = Arc::new(MockRunnerState {
        runs: Mutex::new(BTreeMap::new()),
        dispatches: Mutex::new(BTreeMap::new()),
        requests: Mutex::new(Vec::new()),
        cancel_requests: Mutex::new(BTreeMap::new()),
        artifact_downloads: AtomicUsize::new(0),
        fail_first_dispatch: AtomicBool::new(fail_first_dispatch),
        stall_cancellation: AtomicBool::new(stall_cancellation),
        terminal_outcome,
        capabilities,
    });
    let app = Router::new()
        .route(
            RunnerRouteTemplate::Capabilities.path(),
            get(mock_capabilities),
        )
        .route(RunnerRouteTemplate::Jobs.path(), get(mock_list_jobs))
        .route(RunnerRouteTemplate::JobRuns.path(), post(mock_create_run))
        .route(
            RunnerRouteTemplate::Run.path(),
            get(mock_get_run).delete(mock_cancel_run),
        )
        .route(RunnerRouteTemplate::RunLogs.path(), get(mock_logs))
        .route(
            RunnerRouteTemplate::Artifacts.path(),
            post(mock_artifact_upload),
        )
        .route(
            RunnerRouteTemplate::Artifact.path(),
            get(mock_artifact_download),
        )
        .with_state(Arc::clone(&state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    MockRunner {
        base_url: format!("http://{}", addr),
        state,
    }
}

async fn mock_capabilities(
    State(state): State<Arc<MockRunnerState>>,
) -> Json<janus_lib::RunnerCapabilitiesResponse> {
    Json(state.capabilities.clone())
}

impl MockRunner {
    fn dispatch_count(&self) -> usize {
        self.state.dispatches.lock().expect("dispatches").len()
    }

    fn cancel_count(&self, job_id: &str) -> usize {
        self.state
            .cancel_requests
            .lock()
            .expect("cancel requests")
            .get(job_id)
            .copied()
            .unwrap_or(0)
    }

    fn requests_for(&self, job_name: &str) -> Vec<JsonValue> {
        self.state
            .requests
            .lock()
            .expect("requests")
            .iter()
            .filter(|request| request.job_name == job_name)
            .map(|request| request.body.clone())
            .collect()
    }

    fn artifact_download_count(&self) -> usize {
        self.state.artifact_downloads.load(Ordering::SeqCst)
    }
}

async fn mock_list_jobs() -> Json<JsonValue> {
    Json(json!([{"name":"build-app","timeout_seconds":60}]))
}

async fn mock_create_run(
    State(state): State<Arc<MockRunnerState>>,
    AxumPath(name): AxumPath<String>,
    headers: axum::http::HeaderMap,
    body: Body,
) -> (StatusCode, Json<JsonValue>) {
    let body = to_bytes(body, usize::MAX).await.expect("bytes");
    let request_body: JsonValue = serde_json::from_slice(&body).expect("json request body");
    let key = headers
        .get(HEADER_IDEMPOTENCY_KEY)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("missing")
        .to_string();
    let job_id = {
        let mut dispatches = state.dispatches.lock().expect("dispatches");
        dispatches
            .entry(key)
            .or_insert_with(|| Uuid::now_v7().to_string())
            .clone()
    };
    state
        .runs
        .lock()
        .expect("runs")
        .entry(job_id.clone())
        .or_insert(MockRun {
            polls: 0,
            cancel_stage: None,
            job_name: name.clone(),
        });
    state.requests.lock().expect("requests").push(MockRequest {
        job_name: name,
        body: request_body,
    });
    if matches!(
        state.terminal_outcome,
        MockTerminalOutcome::CreateBadRequest
    ) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "code": "runner_input_invalid",
                "message": "input source expected artifact id"
            })),
        );
    }
    if state.fail_first_dispatch.swap(false, Ordering::SeqCst) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error":"simulated ambiguous create failure"})),
        );
    }
    (
        StatusCode::CREATED,
        Json(json!({"job_id":job_id,"status":"running","started_at":Utc::now().to_rfc3339()})),
    )
}

async fn mock_get_run(
    State(state): State<Arc<MockRunnerState>>,
    AxumPath(job_id): AxumPath<String>,
) -> Json<JsonValue> {
    let mut runs = state.runs.lock().expect("runs");
    let run = runs.entry(job_id.clone()).or_insert(MockRun {
        polls: 0,
        cancel_stage: None,
        job_name: "build-app".to_string(),
    });
    if let Some(stage) = run.cancel_stage {
        if state.stall_cancellation.load(Ordering::SeqCst) {
            return Json(json!({
                "job_id": job_id,
                "name": "build-app",
                "status": "running",
                "started_at": Utc::now().to_rfc3339(),
                "finished_at": null,
                "duration_ms": null,
                "exit_code": null,
                "terminal_reason": null,
                "failure_category": null,
                "outputs": {},
                "output_metadata": {
                    "stdout": {"bytes": 0, "truncated": false},
                    "stderr": {"bytes": 0, "truncated": false},
                    "artifacts": {"count": 0, "bytes": 0}
                }
            }));
        }
        run.cancel_stage = Some(stage.saturating_add(1));
        let status = match stage {
            0 => "cancel_requested",
            1 => "canceling",
            _ => "canceled",
        };
        return Json(json!({
            "job_id": job_id,
            "name": "build-app",
            "status": status,
            "started_at": Utc::now().to_rfc3339(),
            "finished_at": if status == "canceled" { json!(Utc::now().to_rfc3339()) } else { JsonValue::Null },
            "duration_ms": if status == "canceled" { json!(1000) } else { JsonValue::Null },
            "exit_code": null,
            "terminal_reason": if status == "canceled" { json!("canceled") } else { JsonValue::Null },
            "failure_category": if status == "canceled" { json!("canceled") } else { JsonValue::Null },
            "outputs": {},
            "output_metadata": {
                "stdout": {"bytes": 0, "truncated": false},
                "stderr": {"bytes": 0, "truncated": false},
                "artifacts": {"count": 0, "bytes": 0}
            }
        }));
    }
    run.polls += 1;
    if run.polls >= 2 {
        let dispatch_count = state.dispatches.lock().expect("dispatches").len();
        let (status, exit_code, terminal_reason, failure_category, stdout_bytes) = match state
            .terminal_outcome
        {
            MockTerminalOutcome::Success => ("success", Some(0), Some("success"), None, 3),
            MockTerminalOutcome::Timeout => ("failed", None, Some("timeout"), Some("timeout"), 0),
            MockTerminalOutcome::JobFailed => {
                ("failed", Some(7), Some("exit_code"), Some("job"), 0)
            }
            MockTerminalOutcome::InfraFailed => {
                ("failed", None, Some("spawn_error"), Some("infra"), 0)
            }
            MockTerminalOutcome::InfraFailOnceThenSuccess => {
                if dispatch_count >= 2 {
                    ("success", Some(0), Some("success"), None, 3)
                } else {
                    ("failed", None, Some("spawn_error"), Some("infra"), 0)
                }
            }
            MockTerminalOutcome::OutputArtifactDownloadFailed => {
                ("success", Some(0), Some("success"), None, 3)
            }
            MockTerminalOutcome::CreateBadRequest => ("success", Some(0), Some("success"), None, 3),
        };
        let outputs = match run.job_name.as_str() {
            "produce-typed" => json!({
                "version": { "type": "string", "value": "v1.2.3" },
                "build_number": { "type": "integer", "value": 42 },
                "published": { "type": "boolean", "value": true },
                "metadata": { "type": "json", "value": { "commit": "abc123" } }
            }),
            "produce-int" => json!({
                "build_number": { "type": "integer", "value": 42 }
            }),
            _ if matches!(
                state.terminal_outcome,
                MockTerminalOutcome::OutputArtifactDownloadFailed
            ) =>
            {
                json!({
                    "app": {
                        "type": "artifact",
                        "artifact_id": "artifact-1",
                        "sha256": format!("{:x}", Sha256::digest(b"artifact")),
                        "size": 8
                    }
                })
            }
            _ => json!({}),
        };
        Json(json!({
            "job_id": job_id,
            "name": run.job_name,
            "status": status,
            "started_at": Utc::now().to_rfc3339(),
            "finished_at": Utc::now().to_rfc3339(),
            "duration_ms": 1000,
            "exit_code": exit_code,
            "terminal_reason": terminal_reason,
            "failure_category": failure_category,
            "outputs": outputs,
            "output_metadata": {
                "stdout": {"bytes": stdout_bytes, "truncated": false},
                "stderr": {"bytes": 0, "truncated": false},
                "artifacts": {"count": 0, "bytes": 0}
            }
        }))
    } else {
        Json(json!({
            "job_id": job_id,
            "name": "build-app",
            "status": "running",
            "started_at": Utc::now().to_rfc3339(),
            "finished_at": null,
            "duration_ms": null,
            "exit_code": null,
            "terminal_reason": null,
            "failure_category": null,
            "outputs": {},
            "output_metadata": {
                "stdout": {"bytes": 0, "truncated": false},
                "stderr": {"bytes": 0, "truncated": false},
                "artifacts": {"count": 0, "bytes": 0}
            }
        }))
    }
}

async fn mock_logs() -> Json<JsonValue> {
    Json(json!({"stdout":"ok\n","stderr":""}))
}

async fn mock_cancel_run(
    State(state): State<Arc<MockRunnerState>>,
    AxumPath(job_id): AxumPath<String>,
) -> StatusCode {
    {
        let mut cancel_requests = state.cancel_requests.lock().expect("cancel requests");
        *cancel_requests.entry(job_id.clone()).or_insert(0) += 1;
    }
    if let Some(run) = state.runs.lock().expect("runs").get_mut(&job_id) {
        run.cancel_stage = Some(0);
    }
    StatusCode::ACCEPTED
}

async fn mock_artifact_upload(body: Body) -> (StatusCode, Json<JsonValue>) {
    let bytes = to_bytes(body, usize::MAX).await.expect("bytes");
    (
        StatusCode::CREATED,
        Json(json!({
            "artifact_id":"artifact-1",
            "sha256":format!("{:x}", Sha256::digest(&bytes)),
            "size": bytes.len(),
            "expires_at": Utc::now().to_rfc3339()
        })),
    )
}

async fn mock_artifact_download(State(state): State<Arc<MockRunnerState>>) -> (StatusCode, Body) {
    state.artifact_downloads.fetch_add(1, Ordering::SeqCst);
    if matches!(
        state.terminal_outcome,
        MockTerminalOutcome::OutputArtifactDownloadFailed
    ) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Body::from(
                r#"{"code":"artifact_rate_limit_exceeded","message":"token exceeded artifact rate limit"}"#,
            ),
        );
    }
    (StatusCode::OK, Body::from("artifact"))
}
