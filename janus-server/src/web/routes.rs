use std::{
    collections::BTreeMap,
    fs,
    net::{Ipv4Addr, Ipv6Addr},
    path::{Path, PathBuf},
    sync::Arc,
};

use axum::{
    Form, Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path as AxumPath, State},
    http::{
        HeaderMap, Request, StatusCode,
        header::{
            CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_SECURITY_POLICY,
            CONTENT_TYPE, HeaderName, HeaderValue, REFERRER_POLICY, SET_COOKIE,
            X_CONTENT_TYPE_OPTIONS, X_FRAME_OPTIONS,
        },
    },
    middleware::{Next, from_fn},
    response::{IntoResponse, Redirect, Response, Sse},
    routing::{get, post},
};
use chrono::{Duration, Utc};
use hmac::{Hmac, Mac};
use janus_lib::SUPPORTED_RUNNER_PROTOCOL_VERSIONS;
use maud::{Markup, html};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Sha256;
use tokio::time;
use tracing::{error, info, warn};
use url::Url;
use uuid::Uuid;

const CONTENT_SECURITY_POLICY_VALUE: &str = concat!(
    "default-src 'self'; ",
    "script-src 'self'; ",
    "style-src 'self'; ",
    "connect-src 'self'; ",
    "img-src 'self' data:; ",
    "object-src 'none'; ",
    "base-uri 'none'; ",
    "frame-ancestors 'none'; ",
    "form-action 'self'"
);
const PERMISSIONS_POLICY: HeaderName = HeaderName::from_static("permissions-policy");
const ARTIFACT_PAGE_LIMIT: usize = 100;

use crate::{
    app::AppState,
    auth::{
        AdminUser, CurrentUser, clear_session_cookie, hash_password, parse_session_cookie,
        session_cookie, verify_password,
    },
    config::RunnerUrlPolicyConfig,
    git,
    models::{
        self, PipelineRun, Repo, RunnerJobDefinition, RunnerJobInputDefinition, User, UserRole,
        Workflow, WorkflowDefinition, WorkflowInputBinding, WorkflowJobDefinition,
        WorkflowJobOutcomePolicy, WorkflowRunArtifactSelection, WorkflowTrigger,
        parse_job_output_binding,
    },
    scheduler,
    schema_diff::{WorkflowSchemaDiff, workflow_schema_report},
};

use super::views::{
    artifact as artifact_view, pipeline as pipeline_view, repo as repo_view, runner as runner_view,
    user as user_view, workflow as workflow_view,
};

type HmacSha256 = Hmac<Sha256>;

pub(crate) fn build_router(state: Arc<AppState>) -> Router {
    let max_request_body_bytes = state.config.limits.request_body_bytes;
    Router::new()
        .route("/", get(index))
        .route("/login", get(login_form).post(login))
        .route("/logout", post(logout))
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/assets/app.css", get(asset_app_css))
        .route("/assets/form_validation.js", get(asset_form_validation_js))
        .route(
            "/assets/workflow_builder.js",
            get(asset_workflow_builder_js),
        )
        .route(
            "/assets/workflow_builder_dom.js",
            get(asset_workflow_builder_dom_js),
        )
        .route(
            "/assets/workflow_builder_bindings.js",
            get(asset_workflow_builder_bindings_js),
        )
        .route(
            "/assets/workflow_builder_tables.js",
            get(asset_workflow_builder_tables_js),
        )
        .route(
            "/assets/workflow_builder_rows.js",
            get(asset_workflow_builder_rows_js),
        )
        .route(
            "/assets/workflow_builder_state.js",
            get(asset_workflow_builder_state_js),
        )
        .route("/assets/pipeline_events.js", get(asset_pipeline_events_js))
        .route("/users", get(users_page).post(create_user))
        .route("/repos", get(repos_page).post(create_repo))
        .route("/repos/{repo_id}/trigger", post(trigger_repo))
        .route("/runners", get(runners_page).post(create_runner))
        .route("/runners/{runner_id}/update", post(update_runner))
        .route("/runners/{runner_id}/toggle", post(toggle_runner))
        .route("/runners/{runner_id}/test", post(test_runner))
        .route("/workflows", get(workflows_page).post(create_workflow))
        .route("/workflows/{workflow_id}", get(workflow_detail_page))
        .route("/workflows/{workflow_id}/run", post(run_workflow))
        .route("/workflows/{workflow_id}/update", post(update_workflow))
        .route("/pipelines", get(pipelines_page))
        .route("/pipelines/{pipeline_id}", get(pipeline_detail))
        .route("/pipelines/{pipeline_id}/events", get(pipeline_events))
        .route("/pipelines/{pipeline_id}/rerun", post(rerun_pipeline))
        .route(
            "/pipelines/{pipeline_id}/cancel",
            post(cancel_pipeline_route),
        )
        .route("/artifacts", get(artifacts_page))
        .route("/artifacts/{artifact_id}", get(download_artifact))
        .route("/api/me", get(api_me))
        .route("/api/repos", get(api_list_repos).post(api_create_repo))
        .route(
            "/api/runners",
            get(api_list_runners).post(api_create_runner),
        )
        .route(
            "/api/workflows",
            get(api_list_workflows).post(api_create_workflow),
        )
        .route(
            "/api/workflows/{workflow_id}",
            get(api_get_workflow).put(api_update_workflow),
        )
        .route("/api/pipelines", get(api_list_pipelines))
        .route("/api/pipelines/{pipeline_id}", get(api_get_pipeline))
        .layer(from_fn(security_headers))
        .layer(DefaultBodyLimit::max(max_request_body_bytes))
        .with_state(state)
}

async fn security_headers(request: Request<Body>, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    headers.insert(X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(REFERRER_POLICY, HeaderValue::from_static("no-referrer"));
    headers.insert(
        CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CONTENT_SECURITY_POLICY_VALUE),
    );
    headers.insert(
        PERMISSIONS_POLICY,
        HeaderValue::from_static("camera=(), microphone=(), geolocation=()"),
    );
    response
}

async fn health() -> Markup {
    html! { pre { "ok" } }
}
async fn ready() -> Markup {
    html! { pre { "ready" } }
}
struct EmbeddedAsset {
    content_type: &'static str,
    body: &'static str,
}

impl IntoResponse for EmbeddedAsset {
    fn into_response(self) -> Response {
        (
            [
                (CONTENT_TYPE, self.content_type),
                (CACHE_CONTROL, "public, max-age=300"),
            ],
            self.body,
        )
            .into_response()
    }
}

async fn asset_app_css() -> EmbeddedAsset {
    embedded_asset("text/css; charset=utf-8", include_str!("assets/app.css"))
}
async fn asset_form_validation_js() -> EmbeddedAsset {
    embedded_asset(
        "text/javascript; charset=utf-8",
        include_str!("assets/form_validation.js"),
    )
}
async fn asset_workflow_builder_js() -> EmbeddedAsset {
    embedded_asset(
        "text/javascript; charset=utf-8",
        include_str!("assets/workflow_builder.js"),
    )
}
async fn asset_workflow_builder_dom_js() -> EmbeddedAsset {
    embedded_asset(
        "text/javascript; charset=utf-8",
        include_str!("assets/workflow_builder_dom.js"),
    )
}
async fn asset_workflow_builder_bindings_js() -> EmbeddedAsset {
    embedded_asset(
        "text/javascript; charset=utf-8",
        include_str!("assets/workflow_builder_bindings.js"),
    )
}
async fn asset_workflow_builder_tables_js() -> EmbeddedAsset {
    embedded_asset(
        "text/javascript; charset=utf-8",
        include_str!("assets/workflow_builder_tables.js"),
    )
}
async fn asset_workflow_builder_rows_js() -> EmbeddedAsset {
    embedded_asset(
        "text/javascript; charset=utf-8",
        include_str!("assets/workflow_builder_rows.js"),
    )
}
async fn asset_workflow_builder_state_js() -> EmbeddedAsset {
    embedded_asset(
        "text/javascript; charset=utf-8",
        include_str!("assets/workflow_builder_state.js"),
    )
}
async fn asset_pipeline_events_js() -> EmbeddedAsset {
    embedded_asset(
        "text/javascript; charset=utf-8",
        include_str!("assets/pipeline_events.js"),
    )
}
fn embedded_asset(content_type: &'static str, body: &'static str) -> EmbeddedAsset {
    EmbeddedAsset { content_type, body }
}
async fn index() -> impl IntoResponse {
    Redirect::to("/repos")
}

async fn login_form() -> Markup {
    user_view::login_page(None, "")
}

#[derive(Deserialize)]
struct LoginForm {
    username: String,
    password: String,
}

async fn login(State(state): State<Arc<AppState>>, Form(form): Form<LoginForm>) -> Response {
    if form.username.trim().is_empty() || form.password.is_empty() {
        return html_bad_request(user_view::login_page(
            Some("username and password are required"),
            form.username.trim(),
        ));
    }
    if !state.allow_login_attempt(&form.username) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "too many login attempts, try again later",
        )
            .into_response();
    }
    let Ok(Some((user, hash))) = state.db.get_user_credentials(&form.username) else {
        return crate::auth::unauthorized();
    };
    if !verify_password(&form.password, &hash) {
        return crate::auth::unauthorized();
    }
    let _ = state.db.cleanup_expired_sessions();
    let _ = state.db.delete_sessions_for_user(&user.id);
    let expires_at =
        (Utc::now() + Duration::days(state.config.auth.session_ttl_days as i64)).to_rfc3339();
    let Ok(session_id) = state.db.create_session(&user.id, &expires_at) else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create session",
        )
            .into_response();
    };
    let mut response = Redirect::to("/repos").into_response();
    response.headers_mut().append(
        SET_COOKIE,
        session_cookie(
            &state.config.auth.session_secret,
            &session_id,
            state.config.auth.session_cookie_secure,
        )
        .to_string()
        .parse()
        .expect("cookie header"),
    );
    response
}

async fn logout(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Some(cookie) = headers.get("cookie").and_then(|value| value.to_str().ok())
        && let Some(session_id) = parse_session_cookie(&state.config.auth.session_secret, cookie)
    {
        let _ = state.db.delete_session(&session_id);
    }
    let mut response = Redirect::to("/login").into_response();
    response.headers_mut().append(
        SET_COOKIE,
        clear_session_cookie()
            .to_string()
            .parse()
            .expect("cookie header"),
    );
    response
}

#[derive(Deserialize)]
struct CsrfOnlyForm {
    csrf_token: String,
}
#[derive(Deserialize)]
struct CreateUserForm {
    csrf_token: String,
    username: String,
    password: String,
    role: String,
}
#[derive(Deserialize)]
struct CreateRepoForm {
    csrf_token: String,
    name: String,
    default_branch: String,
}
#[derive(Deserialize)]
struct CreateRunnerForm {
    csrf_token: String,
    name: String,
    base_url: String,
}
#[derive(Deserialize)]
struct UpdateRunnerForm {
    csrf_token: String,
    name: String,
}
#[derive(Deserialize)]
struct WorkflowForm {
    csrf_token: String,
    repo_id: String,
    name: String,
    trigger_kind: String,
    branch_name: String,
    jobs_json: String,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowApiView {
    #[serde(flatten)]
    workflow: Workflow,
    schema_status: crate::schema_diff::WorkflowSchemaStatus,
    schema_diff: Vec<WorkflowSchemaDiff>,
}
#[derive(Deserialize)]
struct ManualTriggerForm {
    csrf_token: String,
    branch: Option<String>,
    commit: Option<String>,
    #[serde(flatten)]
    artifact_selections: BTreeMap<String, String>,
}

async fn users_page(
    _: AdminUser,
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Markup, Response> {
    let csrf = csrf_token(&state, &user);
    let users = state.db.list_users().map_err(internal_error)?;
    Ok(user_view::users_page(
        users,
        &csrf,
        None,
        user_view::CreateUserFormView::default(),
    ))
}

async fn create_user(
    _: AdminUser,
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    Form(form): Form<CreateUserForm>,
) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    validate_username(&form.username)
        .map_err(|error| render_users_form_error(&state, &user, &form, error))?;
    validate_password(&form.password)
        .map_err(|error| render_users_form_error(&state, &user, &form, error))?;
    let role = validate_role(&form.role)
        .map_err(|error| render_users_form_error(&state, &user, &form, error))?;
    let hash = hash_password(&form.password).map_err(internal_error_text)?;
    state
        .db
        .create_user(form.username.trim(), &hash, role)
        .map_err(internal_error)?;
    record_audit_event(
        &state,
        &user,
        "user.create",
        "user",
        None,
        Some(form.username.trim()),
        json!({ "role": role.as_str() }),
    )?;
    Ok(Redirect::to("/users"))
}

async fn repos_page(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Markup, Response> {
    let repos = state.db.list_repos().map_err(internal_error)?;
    let csrf = csrf_token(&state, &user);
    let repo_cards = repos
        .into_iter()
        .map(|repo| repo_view::RepoCard {
            clone_url: repo_clone_url(&state, &repo),
            repo,
        })
        .collect::<Vec<_>>();
    Ok(repo_view::repos_page(
        repo_cards,
        &csrf,
        None,
        repo_form_view(None),
    ))
}

async fn create_repo(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    Form(form): Form<CreateRepoForm>,
) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    if form.name.trim().chars().count() > 80 {
        return Err(render_repos_form_error(
            &state,
            &user,
            &form,
            "repository name must be 80 characters or fewer",
        ));
    }
    let normalized = git::validate_repo_name(&form.name)
        .map_err(|error| render_repos_form_error(&state, &user, &form, error.to_string()))?;
    validate_branch_name(&form.default_branch)
        .map_err(|error| render_repos_form_error(&state, &user, &form, error))?;
    let repo = provision_repo(
        &state,
        form.name.trim(),
        &normalized,
        form.default_branch.trim(),
    )?;
    record_audit_event(
        &state,
        &user,
        "repo.create",
        "repo",
        Some(&repo.id),
        Some(&repo.name),
        json!({ "default_branch": repo.default_branch, "normalized_name": repo.normalized_name }),
    )?;
    Ok(Redirect::to("/repos"))
}

fn provision_repo(
    state: &Arc<AppState>,
    name: &str,
    normalized: &str,
    default_branch: &str,
) -> Result<Repo, Response> {
    let bare_path = PathBuf::from(&state.config.repos_dir).join(format!("{}.git", Uuid::now_v7()));
    git::init_bare_repo(&bare_path).map_err(api_internal_error)?;

    let repo_id = match state.db.create_repo(
        name,
        normalized,
        &bare_path.display().to_string(),
        default_branch,
    ) {
        Ok(repo_id) => repo_id,
        Err(error) => {
            cleanup_bare_repo(&bare_path);
            return Err(api_internal_error(error));
        }
    };

    let repo = match state.db.get_repo(&repo_id) {
        Ok(Some(repo)) => repo,
        Ok(None) => {
            rollback_repo_provisioning(state, &repo_id, &bare_path);
            return Err(api_internal_error("missing repo after create"));
        }
        Err(error) => {
            rollback_repo_provisioning(state, &repo_id, &bare_path);
            return Err(api_internal_error(error));
        }
    };

    if let Err(error) = git::install_post_receive_hook(
        Path::new(&repo.bare_path),
        state.server_bin.as_path(),
        Path::new(&state.config.control.socket_path),
        &repo.id,
    ) {
        rollback_repo_provisioning(state, &repo.id, Path::new(&repo.bare_path));
        return Err(api_internal_error(error));
    }

    Ok(repo)
}

fn rollback_repo_provisioning(state: &Arc<AppState>, repo_id: &str, bare_path: &Path) {
    if let Err(error) = state.db.delete_repo(repo_id) {
        warn!(repo_id, error = %error, "failed to roll back repository database row");
    }
    cleanup_bare_repo(bare_path);
}

fn cleanup_bare_repo(bare_path: &Path) {
    if bare_path.exists() {
        if let Err(error) = fs::remove_dir_all(bare_path) {
            warn!(
                bare_path = %bare_path.display(),
                error = %error,
                "failed to clean up repository directory after provisioning failure"
            );
        }
    }
}

async fn trigger_repo(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(repo_id): AxumPath<String>,
    Form(form): Form<ManualTriggerForm>,
) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    let repo = authorized_repo(&state, &user, &repo_id)?;
    let branch = optional_trimmed_value(form.branch.as_deref())
        .unwrap_or_else(|| format!("refs/heads/{}", repo.default_branch));
    validate_manual_ref("branch", &branch).map_err(bad_request)?;
    let commit =
        optional_trimmed_value(form.commit.as_deref()).unwrap_or_else(|| "HEAD".to_string());
    validate_manual_ref("commit", &commit).map_err(bad_request)?;
    let refs = vec![models::PushEventRef {
        old_rev: "0000000000000000000000000000000000000000".to_string(),
        new_rev: commit,
        ref_name: branch,
    }];
    let key = git::event_key(&repo_id, &refs);
    state
        .db
        .create_push_event(&repo_id, &key, &refs)
        .map_err(internal_error)?;
    Ok(Redirect::to("/pipelines"))
}

async fn runners_page(
    _: AdminUser,
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Markup, Response> {
    let runners = state.db.list_runners().map_err(internal_error)?;
    let csrf = csrf_token(&state, &user);
    let runners_with_jobs = runners
        .into_iter()
        .map(|runner| {
            let jobs = state
                .db
                .list_runner_jobs(&runner.id)
                .map_err(internal_error)?;
            Ok((runner, jobs))
        })
        .collect::<Result<Vec<_>, Response>>()?;
    Ok(runner_view::runners_page(
        runners_with_jobs,
        runner_auth_view(&state),
        &csrf,
        None,
        runner_view::RunnerFormView::default(),
    ))
}

async fn create_runner(
    _: AdminUser,
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    Form(form): Form<CreateRunnerForm>,
) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    validate_runner_name(&form.name)
        .map_err(|error| render_runners_form_error(&state, &user, &form, error))?;
    validate_base_url(&form.base_url, &state.config.runner_url_policy)
        .map_err(|error| render_runners_form_error(&state, &user, &form, error))?;
    let runner_id = state
        .db
        .create_runner(form.name.trim(), form.base_url.trim())
        .map_err(internal_error)?;
    record_audit_event(
        &state,
        &user,
        "runner.create",
        "runner",
        Some(&runner_id),
        Some(form.name.trim()),
        json!({ "base_url": form.base_url.trim() }),
    )?;
    refresh_single_runner(&state, &runner_id)
        .await
        .map_err(internal_error_text)?;
    Ok(Redirect::to("/runners"))
}

async fn toggle_runner(
    _: AdminUser,
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(runner_id): AxumPath<String>,
    Form(form): Form<CsrfOnlyForm>,
) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    let runner = state
        .db
        .get_runner(&runner_id)
        .map_err(internal_error)?
        .ok_or_else(|| not_found("runner"))?;
    state
        .db
        .set_runner_enabled(&runner_id, !runner.enabled)
        .map_err(internal_error)?;
    record_audit_event(
        &state,
        &user,
        "runner.update",
        "runner",
        Some(&runner.id),
        Some(&runner.name),
        json!({ "field": "enabled", "old": runner.enabled, "new": !runner.enabled }),
    )?;
    Ok(Redirect::to("/runners"))
}

async fn update_runner(
    _: AdminUser,
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(runner_id): AxumPath<String>,
    Form(form): Form<UpdateRunnerForm>,
) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    validate_runner_name(&form.name).map_err(bad_request)?;
    state
        .db
        .get_runner(&runner_id)
        .map_err(internal_error)?
        .ok_or_else(|| not_found("runner"))?;
    state
        .db
        .update_runner_name(&runner_id, form.name.trim())
        .map_err(internal_error)?;
    record_audit_event(
        &state,
        &user,
        "runner.update",
        "runner",
        Some(&runner_id),
        Some(form.name.trim()),
        json!({ "field": "name" }),
    )?;
    Ok(Redirect::to("/runners"))
}

async fn test_runner(
    _: AdminUser,
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(runner_id): AxumPath<String>,
    Form(form): Form<CsrfOnlyForm>,
) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    refresh_single_runner(&state, &runner_id)
        .await
        .map_err(internal_error_text)?;
    Ok(Redirect::to("/runners"))
}

async fn workflows_page(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Markup, Response> {
    let repos = visible_repos_for_user(&state, &user)?;
    let workflows = state.db.list_workflows().map_err(internal_error)?;
    let runner_catalog = workflow_runner_catalog(&state)?;
    let csrf = csrf_token(&state, &user);
    let repo_select = workflow_view::repo_selector(&repos);
    let form = workflow_form_view(None, &runner_catalog, repo_select);
    let workflow_cards = workflow_cards_for_repos(&state, &repos, workflows)?;
    Ok(workflow_view::workflows_page(form, workflow_cards, &csrf))
}

async fn workflow_detail_page(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(workflow_id): AxumPath<String>,
) -> Result<Markup, Response> {
    let workflow = authorized_workflow(&state, &user, &workflow_id)?;
    let csrf = csrf_token(&state, &user);
    let repo = state
        .db
        .get_repo(&workflow.repo_id)
        .map_err(internal_error)?
        .ok_or_else(|| not_found("repo"))?;
    let runner_catalog = workflow_runner_catalog(&state)?;
    let repo_field = workflow_view::fixed_repo_field(&workflow, &repo);
    let schema_report = workflow_schema_report(&state, &workflow).map_err(internal_error)?;
    let form = workflow_form_view(Some(&workflow), &runner_catalog, repo_field);
    let manual_artifacts = manual_artifact_fields(&state, &workflow)?;
    Ok(workflow_view::workflow_detail_page(
        &workflow,
        &repo,
        schema_report,
        form,
        &csrf,
        manual_artifacts,
    ))
}

async fn create_workflow(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    Form(form): Form<WorkflowForm>,
) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    let repo = authorized_repo(&state, &user, &form.repo_id)?;
    let parsed = parse_workflow_form(&state, &form)
        .map_err(|error| render_create_workflow_form_error(&state, &user, &form, error))?;
    let workflow_id = state
        .db
        .create_workflow(
            &repo.id,
            &form.name.trim(),
            true,
            &parsed.trigger_json,
            &parsed.definition_json,
            &parsed.job_schemas,
        )
        .map_err(internal_error)?;
    record_audit_event(
        &state,
        &user,
        "workflow.create",
        "workflow",
        Some(&workflow_id),
        Some(form.name.trim()),
        json!({ "repo_id": repo.id, "enabled": true }),
    )?;
    Ok(Redirect::to("/workflows"))
}

async fn update_workflow(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(workflow_id): AxumPath<String>,
    Form(form): Form<WorkflowForm>,
) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    let workflow = authorized_workflow(&state, &user, &workflow_id)?;
    if workflow.repo_id != form.repo_id {
        return Err(bad_request("workflow repo cannot be changed"));
    }
    let parsed = parse_workflow_form(&state, &form).map_err(|error| {
        render_update_workflow_form_error(&state, &user, &workflow, &form, error)
    })?;
    let version_id = state
        .db
        .update_workflow(
            &workflow.id,
            form.name.trim(),
            true,
            &parsed.trigger_json,
            &parsed.definition_json,
            &parsed.job_schemas,
        )
        .map_err(internal_error)?;
    record_audit_event(
        &state,
        &user,
        "workflow.update",
        "workflow",
        Some(&workflow.id),
        Some(form.name.trim()),
        json!({ "repo_id": workflow.repo_id, "version_id": version_id, "enabled": true }),
    )?;
    Ok(Redirect::to(&format!("/workflows/{}", workflow.id)))
}

async fn run_workflow(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(workflow_id): AxumPath<String>,
    Form(form): Form<ManualTriggerForm>,
) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    let workflow = authorized_workflow(&state, &user, &workflow_id)?;
    let trigger: WorkflowTrigger =
        serde_json::from_str(&workflow.trigger_json).map_err(internal_error_text)?;
    if trigger.kind != "manual" {
        return Err(bad_request("workflow trigger is not manual"));
    }
    let repo = state
        .db
        .get_repo(&workflow.repo_id)
        .map_err(internal_error)?
        .ok_or_else(|| not_found("repo"))?;
    let branch = form
        .branch
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| trigger.branches.first().cloned())
        .unwrap_or(repo.default_branch);
    let commit = form
        .commit
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("HEAD");
    let artifact_selections =
        validate_manual_artifact_selections(&state, &workflow, &form.artifact_selections)?;
    let pipeline_id = scheduler::enqueue_workflow_run_with_artifacts(
        Arc::clone(&state),
        &workflow,
        "manual",
        Some(&branch),
        Some(commit),
        &artifact_selections,
    )
    .map_err(internal_error)?;
    record_audit_event(
        &state,
        &user,
        "workflow.run",
        "pipeline",
        Some(&pipeline_id),
        Some(&workflow.name),
        json!({
            "workflow_id": workflow.id,
            "artifact_selections": artifact_selections,
        }),
    )?;
    Ok(Redirect::to(&format!("/pipelines/{pipeline_id}")))
}

async fn pipelines_page(
    CurrentUser(_user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Markup, Response> {
    let pipelines = state.db.list_pipeline_runs().map_err(internal_error)?;
    let repos = state.db.list_repos().map_err(internal_error)?;
    let repo_by_id = repos
        .into_iter()
        .map(|repo| (repo.id.clone(), repo))
        .collect::<BTreeMap<_, _>>();
    let visible_pipelines = pipelines
        .into_iter()
        .filter_map(|pipeline| {
            let repo = repo_by_id.get(&pipeline.repo_id)?;
            Some((pipeline, repo.clone()))
        })
        .collect::<Vec<_>>();
    Ok(pipeline_view::pipelines_page(visible_pipelines))
}

async fn artifacts_page(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Markup, Response> {
    require_admin(&user)?;
    let artifacts = state
        .db
        .list_recent_produced_artifacts(ARTIFACT_PAGE_LIMIT)
        .map_err(internal_error)?;
    Ok(artifact_view::artifacts_page(&artifacts))
}

async fn pipeline_detail(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(pipeline_id): AxumPath<String>,
) -> Result<Markup, Response> {
    let pipeline = authorized_pipeline(&state, &user, &pipeline_id)?;
    let snapshot = state
        .db
        .pipeline_snapshot(&pipeline.id)
        .map_err(internal_error)?
        .ok_or_else(|| not_found("pipeline"))?;
    let csrf = csrf_token(&state, &user);
    Ok(pipeline_view::pipeline_detail_page(
        &pipeline, &snapshot, &csrf,
    ))
}

async fn pipeline_events(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(pipeline_id): AxumPath<String>,
) -> Result<
    Sse<
        impl futures_util::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>,
    >,
    Response,
> {
    let pipeline = authorized_pipeline(&state, &user, &pipeline_id)?;
    let pipeline_id = pipeline.id;
    let stream = async_stream::stream! {
        loop {
            let payload = match state.db.pipeline_snapshot(&pipeline_id) {
                Ok(Some(snapshot)) => serde_json::to_string(&snapshot).unwrap_or_else(|_| "{}".to_string()),
                Ok(None) => "{}".to_string(),
                Err(error) => json!({ "error": error.to_string() }).to_string(),
            };
            if payload != "{}" { yield Ok(axum::response::sse::Event::default().data(payload)); }
            time::sleep(std::time::Duration::from_secs(2)).await;
        }
    };
    Ok(Sse::new(stream))
}

async fn rerun_pipeline(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(pipeline_id): AxumPath<String>,
    Form(form): Form<CsrfOnlyForm>,
) -> Response {
    let result: Result<Redirect, Response> = (|| {
        verify_csrf(&state, &user, &form.csrf_token)?;
        let pipeline = authorized_pipeline(&state, &user, &pipeline_id)?;
        let workflow = state
            .db
            .get_workflow_by_version_id(&pipeline.workflow_version_id)
            .map_err(internal_error)?
            .ok_or_else(|| not_found("workflow"))?;
        let original_selections = state
            .db
            .workflow_run_artifact_selections(&pipeline.id)
            .map_err(internal_error)?;
        let new_pipeline_id = scheduler::enqueue_workflow_run_with_artifacts(
            Arc::clone(&state),
            &workflow,
            "rerun",
            pipeline.trigger_ref.as_deref(),
            pipeline.commit_sha.as_deref(),
            &original_selections,
        )
        .map_err(internal_error)?;
        Ok(Redirect::to(&format!("/pipelines/{new_pipeline_id}")))
    })();
    match result {
        Ok(redirect) => redirect.into_response(),
        Err(response) => response,
    }
}

async fn cancel_pipeline_route(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(pipeline_id): AxumPath<String>,
    Form(form): Form<CsrfOnlyForm>,
) -> Response {
    match verify_csrf(&state, &user, &form.csrf_token)
        .and_then(|_| authorized_pipeline(&state, &user, &pipeline_id))
    {
        Ok(pipeline) => match scheduler::cancel_pipeline(Arc::clone(&state), &pipeline.id).await {
            Ok(()) => Redirect::to(&format!("/pipelines/{}", pipeline.id)).into_response(),
            Err(error) => internal_error(error),
        },
        Err(response) => response,
    }
}

#[derive(Serialize)]
struct SessionInfo {
    user: User,
    csrf_token: String,
}
#[derive(Serialize)]
struct ApiErrorEnvelope {
    error: ApiErrorBody,
}
#[derive(Serialize)]
struct ApiErrorBody {
    code: &'static str,
    message: String,
}
#[derive(Deserialize)]
struct ApiRepoCreateRequest {
    name: String,
    default_branch: String,
    csrf_token: String,
}
#[derive(Deserialize)]
struct ApiRunnerCreateRequest {
    name: String,
    base_url: String,
    csrf_token: String,
}
#[derive(Deserialize)]
struct ApiWorkflowRequest {
    csrf_token: String,
    repo_id: String,
    name: String,
    enabled: bool,
    trigger_kind: String,
    branches: Vec<String>,
    jobs: Vec<ApiWorkflowJob>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApiWorkflowJob {
    runner_id: String,
    runner_job_name: String,
    #[serde(default)]
    inputs: BTreeMap<String, WorkflowInputBinding>,
    #[serde(default)]
    outcome_policy: WorkflowJobOutcomePolicy,
}

async fn api_me(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Json<SessionInfo> {
    Json(SessionInfo {
        csrf_token: csrf_token(&state, &user),
        user,
    })
}
async fn api_list_repos(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<Repo>>, Response> {
    Ok(Json(visible_repos_for_user(&state, &user)?))
}
async fn api_create_repo(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    Json(request): Json<ApiRepoCreateRequest>,
) -> Result<Json<Repo>, Response> {
    verify_csrf(&state, &user, &request.csrf_token)
        .map_err(|_| api_forbidden("csrf validation failed"))?;
    if request.name.trim().chars().count() > 80 {
        return Err(api_bad_request(
            "repository name must be 80 characters or fewer",
        ));
    }
    let normalized = git::validate_repo_name(&request.name).map_err(api_bad_request)?;
    validate_branch_name(&request.default_branch).map_err(api_bad_request)?;
    let repo = provision_repo(
        &state,
        request.name.trim(),
        &normalized,
        request.default_branch.trim(),
    )?;
    record_audit_event(
        &state,
        &user,
        "repo.create",
        "repo",
        Some(&repo.id),
        Some(&repo.name),
        json!({ "default_branch": repo.default_branch, "normalized_name": repo.normalized_name }),
    )?;
    Ok(Json(repo))
}
async fn api_list_runners(
    _: AdminUser,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<models::Runner>>, Response> {
    Ok(Json(state.db.list_runners().map_err(api_internal_error)?))
}
async fn api_create_runner(
    _: AdminUser,
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    Json(request): Json<ApiRunnerCreateRequest>,
) -> Result<Json<models::Runner>, Response> {
    verify_csrf(&state, &user, &request.csrf_token)
        .map_err(|_| api_forbidden("csrf validation failed"))?;
    validate_runner_name(&request.name).map_err(api_bad_request)?;
    validate_base_url(&request.base_url, &state.config.runner_url_policy)
        .map_err(api_bad_request)?;
    let runner_id = state
        .db
        .create_runner(request.name.trim(), request.base_url.trim())
        .map_err(api_internal_error)?;
    record_audit_event(
        &state,
        &user,
        "runner.create",
        "runner",
        Some(&runner_id),
        Some(request.name.trim()),
        json!({ "base_url": request.base_url.trim() }),
    )?;
    refresh_single_runner(&state, &runner_id)
        .await
        .map_err(api_internal_error)?;
    let runner = state
        .db
        .get_runner(&runner_id)
        .map_err(api_internal_error)?
        .ok_or_else(|| not_found("runner"))?;
    Ok(Json(runner))
}
async fn api_list_workflows(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<WorkflowApiView>>, Response> {
    let repos = visible_repos_for_user(&state, &user)?;
    let repo_ids = repos.into_iter().map(|repo| repo.id).collect::<Vec<_>>();
    let workflows = state
        .db
        .list_workflows()
        .map_err(api_internal_error)?
        .into_iter()
        .filter(|workflow| repo_ids.iter().any(|id| id == &workflow.repo_id))
        .map(|workflow| {
            let schema_report =
                workflow_schema_report(&state, &workflow).map_err(api_internal_error)?;
            Ok(WorkflowApiView {
                schema_status: schema_report.status,
                schema_diff: schema_report.diff,
                workflow,
            })
        })
        .collect::<Result<Vec<_>, Response>>()?;
    Ok(Json(workflows))
}
async fn api_create_workflow(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    Json(request): Json<ApiWorkflowRequest>,
) -> Result<Json<WorkflowApiView>, Response> {
    verify_csrf(&state, &user, &request.csrf_token)
        .map_err(|_| api_forbidden("csrf validation failed"))?;
    let repo = authorized_repo(&state, &user, &request.repo_id)?;
    let parsed = parse_api_workflow_request(&state, &request).map_err(api_bad_request)?;
    let workflow_id = state
        .db
        .create_workflow(
            &repo.id,
            request.name.trim(),
            request.enabled,
            &parsed.trigger_json,
            &parsed.definition_json,
            &parsed.job_schemas,
        )
        .map_err(api_internal_error)?;
    let workflow = state
        .db
        .get_workflow(&workflow_id)
        .map_err(api_internal_error)?
        .ok_or_else(|| api_internal_error("workflow missing after create"))?;
    record_audit_event(
        &state,
        &user,
        "workflow.create",
        "workflow",
        Some(&workflow.id),
        Some(&workflow.name),
        json!({ "repo_id": repo.id, "enabled": request.enabled }),
    )?;
    let schema_report = workflow_schema_report(&state, &workflow).map_err(api_internal_error)?;
    Ok(Json(WorkflowApiView {
        schema_status: schema_report.status,
        schema_diff: schema_report.diff,
        workflow,
    }))
}
async fn api_get_workflow(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(workflow_id): AxumPath<String>,
) -> Result<Json<WorkflowApiView>, Response> {
    let workflow = authorized_workflow(&state, &user, &workflow_id)?;
    let schema_report = workflow_schema_report(&state, &workflow).map_err(api_internal_error)?;
    Ok(Json(WorkflowApiView {
        schema_status: schema_report.status,
        schema_diff: schema_report.diff,
        workflow,
    }))
}
async fn api_update_workflow(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(workflow_id): AxumPath<String>,
    Json(request): Json<ApiWorkflowRequest>,
) -> Result<Json<WorkflowApiView>, Response> {
    verify_csrf(&state, &user, &request.csrf_token)
        .map_err(|_| api_forbidden("csrf validation failed"))?;
    let workflow = authorized_workflow(&state, &user, &workflow_id)?;
    if workflow.repo_id != request.repo_id {
        return Err(api_bad_request("workflow repo cannot be changed"));
    }
    let parsed = parse_api_workflow_request(&state, &request).map_err(api_bad_request)?;
    let version_id = state
        .db
        .update_workflow(
            &workflow.id,
            request.name.trim(),
            request.enabled,
            &parsed.trigger_json,
            &parsed.definition_json,
            &parsed.job_schemas,
        )
        .map_err(api_internal_error)?;
    let workflow = state
        .db
        .get_workflow(&workflow.id)
        .map_err(api_internal_error)?
        .ok_or_else(|| not_found("workflow"))?;
    record_audit_event(
        &state,
        &user,
        "workflow.update",
        "workflow",
        Some(&workflow.id),
        Some(&workflow.name),
        json!({ "repo_id": workflow.repo_id, "version_id": version_id, "enabled": request.enabled }),
    )?;
    let schema_report = workflow_schema_report(&state, &workflow).map_err(api_internal_error)?;
    Ok(Json(WorkflowApiView {
        schema_status: schema_report.status,
        schema_diff: schema_report.diff,
        workflow,
    }))
}
async fn api_list_pipelines(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<PipelineRun>>, Response> {
    let repos = visible_repos_for_user(&state, &user)?;
    let repo_ids = repos.into_iter().map(|repo| repo.id).collect::<Vec<_>>();
    let pipelines = state
        .db
        .list_pipeline_runs()
        .map_err(api_internal_error)?
        .into_iter()
        .filter(|pipeline| repo_ids.iter().any(|id| id == &pipeline.repo_id))
        .collect::<Vec<_>>();
    Ok(Json(pipelines))
}
async fn api_get_pipeline(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(pipeline_id): AxumPath<String>,
) -> Result<Json<models::PipelineSnapshot>, Response> {
    let pipeline = authorized_pipeline(&state, &user, &pipeline_id)?;
    Ok(Json(
        state
            .db
            .pipeline_snapshot(&pipeline.id)
            .map_err(api_internal_error)?
            .ok_or_else(|| not_found("pipeline"))?,
    ))
}

async fn download_artifact(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(artifact_id): AxumPath<String>,
) -> Result<Response, Response> {
    let artifact = state
        .db
        .get_server_artifact_by_id(&artifact_id)
        .map_err(internal_error)?
        .ok_or_else(|| not_found("artifact"))?;
    authorize_artifact(&state, &user, &artifact)?;
    let bytes = state
        .artifacts
        .read_bytes(&artifact)
        .map_err(internal_error)?;
    let filename = safe_download_filename(&artifact.artifact_name);
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/octet-stream")
        .header(
            CONTENT_DISPOSITION,
            format!("attachment; filename=\"{filename}\""),
        )
        .header(X_CONTENT_TYPE_OPTIONS, "nosniff")
        .header(CACHE_CONTROL, "private, no-store")
        .header(CONTENT_LENGTH, bytes.len().to_string())
        .body(Body::from(bytes))
        .map_err(internal_error)
}

fn authorize_artifact(
    state: &Arc<AppState>,
    user: &User,
    artifact: &models::ServerArtifact,
) -> Result<(), Response> {
    match artifact.scope_type.as_str() {
        "pipeline_source" => {
            authorized_pipeline(state, user, &artifact.scope_id)?;
        }
        "job_output" => {
            let pipeline = state
                .db
                .pipeline_for_job_run(&artifact.scope_id)
                .map_err(internal_error)?
                .ok_or_else(|| not_found("pipeline"))?;
            authorized_pipeline(state, user, &pipeline.id)?;
        }
        _ => return Err(not_found("artifact")),
    }
    Ok(())
}

fn safe_download_filename(name: &str) -> String {
    let sanitized = name
        .chars()
        .map(|ch| match ch {
            '"' | '\\' | '/' | '<' | '>' | ':' | '|' | '?' | '*' | '\r' | '\n' | '\t' => '_',
            ch if ch.is_control() => '_',
            ch if ch.is_ascii() => ch,
            _ => '_',
        })
        .collect::<String>()
        .trim_matches(['.', ' '])
        .to_string();
    let mut sanitized = sanitized;
    while sanitized.contains("..") {
        sanitized = sanitized.replace("..", "_");
    }
    if sanitized.is_empty() {
        "artifact.bin".to_string()
    } else {
        sanitized
    }
}

fn record_audit_event(
    state: &Arc<AppState>,
    actor: &User,
    action: &'static str,
    target_type: &'static str,
    target_id: Option<&str>,
    target_name: Option<&str>,
    metadata: Value,
) -> Result<(), Response> {
    let audit_id = state
        .db
        .create_audit_event(
            Some(actor),
            action,
            target_type,
            target_id,
            target_name,
            metadata.clone(),
        )
        .map_err(internal_error)?;
    info!(
        audit_id = %audit_id,
        actor_user_id = %actor.id,
        actor_username = %actor.username,
        action,
        target_type,
        target_id = target_id.unwrap_or(""),
        target_name = target_name.unwrap_or(""),
        metadata = %metadata,
        "audit event recorded"
    );
    Ok(())
}

async fn refresh_single_runner(state: &Arc<AppState>, runner_id: &str) -> Result<(), String> {
    let runner = state
        .db
        .get_runner(runner_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "runner not found".to_string())?;
    let capabilities = state
        .runner_client
        .capabilities(&runner)
        .await
        .map_err(|error| error.to_string())?;
    if !capabilities.is_compatible_with_supported_versions(SUPPORTED_RUNNER_PROTOCOL_VERSIONS) {
        state
            .db
            .update_runner_health(runner_id, "incompatible")
            .map_err(|error| error.to_string())?;
        return Err(format!(
            "runner protocol incompatible: runner supports {:?}, server supports {:?}",
            capabilities.supported_protocol_versions, SUPPORTED_RUNNER_PROTOCOL_VERSIONS
        ));
    }
    let jobs = state
        .runner_client
        .list_jobs(&runner)
        .await
        .map_err(|error| error.to_string())?;
    state
        .db
        .replace_runner_jobs(runner_id, &jobs)
        .map_err(|error| error.to_string())?;
    state
        .db
        .update_runner_health(runner_id, "healthy")
        .map_err(|error| error.to_string())?;
    Ok(())
}

struct ParsedWorkflow {
    trigger_json: String,
    definition_json: String,
    job_schemas: Vec<RunnerJobDefinition>,
}

fn parse_workflow_form(
    state: &Arc<AppState>,
    form: &WorkflowForm,
) -> Result<ParsedWorkflow, String> {
    if form.name.trim().is_empty() {
        return Err("workflow name cannot be empty".to_string());
    }
    if form.name.trim().chars().count() > 120 {
        return Err("workflow name must be 120 characters or fewer".to_string());
    }
    let trigger_kind = form.trigger_kind.trim();
    if !matches!(trigger_kind, "push" | "manual") {
        return Err("trigger kind must be push or manual".to_string());
    }
    let branch_name = form.branch_name.trim();
    validate_workflow_branch_filter(branch_name)?;
    let trigger = WorkflowTrigger {
        kind: trigger_kind.to_string(),
        branches: if branch_name.is_empty() {
            Vec::new()
        } else {
            vec![branch_name.to_string()]
        },
    };
    let jobs = parse_workflow_form_jobs(&form.jobs_json)?;
    let definition = WorkflowDefinition { jobs };
    definition.validate()?;
    validate_promoted_artifact_trigger(trigger_kind, &definition)?;
    let job_schemas = validate_workflow_runners(state, &definition)?;
    Ok(ParsedWorkflow {
        trigger_json: serde_json::to_string(&trigger).map_err(|error| error.to_string())?,
        definition_json: serde_json::to_string(&definition).map_err(|error| error.to_string())?,
        job_schemas,
    })
}

fn parse_api_workflow_request(
    state: &Arc<AppState>,
    request: &ApiWorkflowRequest,
) -> Result<ParsedWorkflow, String> {
    if request.name.trim().is_empty() {
        return Err("workflow name cannot be empty".to_string());
    }
    if request.name.trim().chars().count() > 120 {
        return Err("workflow name must be 120 characters or fewer".to_string());
    }
    let trigger_kind = request.trigger_kind.trim();
    if !matches!(trigger_kind, "push" | "manual") {
        return Err("trigger kind must be push or manual".to_string());
    }
    let branches = request
        .branches
        .iter()
        .map(|branch| {
            let branch = branch.trim().to_string();
            if branch.is_empty() {
                return Err("workflow branch cannot be empty".to_string());
            }
            validate_workflow_branch_filter(&branch)?;
            Ok(branch)
        })
        .collect::<Result<Vec<_>, String>>()?;
    let trigger = WorkflowTrigger {
        kind: trigger_kind.to_string(),
        branches,
    };
    let jobs = request
        .jobs
        .iter()
        .map(|job| WorkflowJobDefinition {
            runner_id: job.runner_id.trim().to_string(),
            runner_job_name: job.runner_job_name.trim().to_string(),
            inputs: job.inputs.clone(),
            outcome_policy: job.outcome_policy.clone(),
        })
        .collect::<Vec<_>>();
    let definition = WorkflowDefinition { jobs };
    definition.validate()?;
    validate_promoted_artifact_trigger(trigger_kind, &definition)?;
    let job_schemas = validate_workflow_runners(state, &definition)?;
    Ok(ParsedWorkflow {
        trigger_json: serde_json::to_string(&trigger).map_err(|error| error.to_string())?,
        definition_json: serde_json::to_string(&definition).map_err(|error| error.to_string())?,
        job_schemas,
    })
}

#[derive(Debug, Clone, Deserialize)]
struct SubmittedWorkflowJob {
    runner_id: String,
    runner_job_name: String,
    #[serde(default)]
    inputs: BTreeMap<String, WorkflowInputBinding>,
    #[serde(default)]
    outcome_policy: WorkflowJobOutcomePolicy,
}

fn parse_workflow_form_jobs(input: &str) -> Result<Vec<WorkflowJobDefinition>, String> {
    let jobs = serde_json::from_str::<Vec<SubmittedWorkflowJob>>(input)
        .map_err(|error| format!("invalid workflow jobs payload: {error}"))?;
    if jobs.is_empty() {
        return Err("workflow must contain at least one job".to_string());
    }
    Ok(jobs
        .into_iter()
        .map(|job| WorkflowJobDefinition {
            runner_id: job.runner_id.trim().to_string(),
            runner_job_name: job.runner_job_name.trim().to_string(),
            inputs: job.inputs,
            outcome_policy: job.outcome_policy,
        })
        .collect())
}

fn validate_workflow_runners(
    state: &Arc<AppState>,
    definition: &WorkflowDefinition,
) -> Result<Vec<RunnerJobDefinition>, String> {
    let mut runner_job_defs = BTreeMap::new();
    for (job_index, job) in definition.jobs.iter().enumerate() {
        let runner = state
            .db
            .get_runner(&job.runner_id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("unknown runner {}", job.runner_id))?;
        let jobs = state
            .db
            .list_runner_jobs(&runner.id)
            .map_err(|error| error.to_string())?;
        let Some(runner_job) = jobs
            .into_iter()
            .find(|schema| schema.name == job.runner_job_name)
        else {
            return Err(format!(
                "runner {} does not advertise job {}",
                runner.name, job.runner_job_name
            ));
        };
        runner_job_defs.insert(job_index, runner_job);
    }

    for (job_index, job) in definition.jobs.iter().enumerate() {
        let runner_job = runner_job_defs
            .get(&job_index)
            .ok_or_else(|| "missing parsed runner job definition".to_string())?;
        for (input_name, value) in &job.inputs {
            let input_definition = runner_job.inputs.get(input_name).ok_or_else(|| {
                format!(
                    "workflow job {} provides unknown input {} for runner job {}",
                    job_index + 1,
                    input_name,
                    job.runner_job_name
                )
            })?;
            let expected_kind = input_definition.kind.as_str();
            match value {
                WorkflowInputBinding::Commit | WorkflowInputBinding::Branch => {
                    if expected_kind != "string" {
                        return Err(format!(
                            "workflow input {input_name} expects {expected_kind} but built-in binding is string"
                        ));
                    }
                    continue;
                }
                WorkflowInputBinding::SourceArtifact => {
                    if expected_kind != "artifact" {
                        return Err(format!(
                            "workflow input {input_name} expects {expected_kind} but source binding is artifact"
                        ));
                    }
                    continue;
                }
                WorkflowInputBinding::PromotedArtifact {
                    source_runner_id,
                    source_job_name,
                    output_name,
                } => {
                    if expected_kind != "artifact" {
                        return Err(format!(
                            "workflow input {input_name} expects {expected_kind} but promoted binding is artifact"
                        ));
                    }
                    let source_runner = state
                        .db
                        .get_runner(source_runner_id)
                        .map_err(|error| error.to_string())?
                        .ok_or_else(|| {
                            format!(
                                "promoted artifact references unknown runner {source_runner_id}"
                            )
                        })?;
                    let source_job = state
                        .db
                        .list_runner_jobs(source_runner_id)
                        .map_err(|error| error.to_string())?
                        .into_iter()
                        .find(|candidate| candidate.name == *source_job_name)
                        .ok_or_else(|| {
                            format!(
                                "runner {} does not advertise artifact source job {}",
                                source_runner.name, source_job_name
                            )
                        })?;
                    let source_output = source_job.outputs.get(output_name).ok_or_else(|| {
                        format!(
                            "runner {} job {} does not advertise output {}",
                            source_runner.name, source_job_name, output_name
                        )
                    })?;
                    if source_output.kind.as_str() != "artifact" {
                        return Err(format!(
                            "promoted source {} / {} / {} is {} instead of artifact",
                            source_runner.name,
                            source_job_name,
                            output_name,
                            source_output.kind.as_str()
                        ));
                    }
                    continue;
                }
                WorkflowInputBinding::Literal { value } => {
                    if !value_matches_input_kind(value, expected_kind) {
                        return Err(format!(
                            "workflow input {input_name} expects {expected_kind} but got {}",
                            describe_json_value_kind(value)
                        ));
                    }
                    validate_literal_input_constraints(input_name, input_definition, value)?;
                    continue;
                }
                WorkflowInputBinding::JobOutput { .. } => {}
            }
            if let Some(binding) = parse_job_output_binding(value) {
                if binding.job_index >= definition.jobs.len() {
                    return Err(format!(
                        "workflow input {input_name} references unknown job job-{}",
                        binding.job_index + 1
                    ));
                }
                if binding.job_index >= job_index {
                    return Err(format!(
                        "workflow input {input_name} references job-{}.{} but only earlier jobs can be referenced",
                        binding.job_index + 1,
                        binding.output_name
                    ));
                }
                let upstream_runner_job = runner_job_defs
                    .get(&binding.job_index)
                    .ok_or_else(|| "missing upstream runner job definition".to_string())?;
                let output = upstream_runner_job
                    .outputs
                    .get(&binding.output_name)
                    .ok_or_else(|| {
                        format!(
                            "workflow input {input_name} references missing output job-{}.{}",
                            binding.job_index + 1,
                            binding.output_name
                        )
                    })?;
                if output.kind.as_str() != expected_kind {
                    return Err(format!(
                        "workflow input {input_name} expects {expected_kind} but job-{}.{} is {}",
                        binding.job_index + 1,
                        binding.output_name,
                        output.kind.as_str()
                    ));
                }
            }
        }
        validate_required_workflow_inputs(job_index, job, runner_job)?;
    }
    Ok(definition
        .jobs
        .iter()
        .enumerate()
        .filter_map(|(job_index, _)| runner_job_defs.get(&job_index).cloned())
        .collect())
}

fn validate_required_workflow_inputs(
    job_index: usize,
    job: &WorkflowJobDefinition,
    runner_job: &RunnerJobDefinition,
) -> Result<(), String> {
    for (input_name, input_definition) in &runner_job.inputs {
        if !input_definition.required {
            continue;
        }
        let Some(binding) = job.inputs.get(input_name) else {
            return Err(format!(
                "workflow job {} missing required input {} for runner job {}",
                job_index + 1,
                input_name,
                job.runner_job_name
            ));
        };
        if workflow_input_binding_is_empty(binding) {
            return Err(format!(
                "workflow job {} missing required input {} for runner job {}",
                job_index + 1,
                input_name,
                job.runner_job_name
            ));
        }
    }
    Ok(())
}

fn workflow_input_binding_is_empty(binding: &WorkflowInputBinding) -> bool {
    match binding {
        WorkflowInputBinding::Literal { value } => match value {
            Value::Null => true,
            Value::String(value) => value.trim().is_empty(),
            _ => false,
        },
        WorkflowInputBinding::JobOutput { output_name, .. } => output_name.trim().is_empty(),
        WorkflowInputBinding::Commit
        | WorkflowInputBinding::Branch
        | WorkflowInputBinding::SourceArtifact
        | WorkflowInputBinding::PromotedArtifact { .. } => false,
    }
}

fn validate_promoted_artifact_trigger(
    trigger_kind: &str,
    definition: &WorkflowDefinition,
) -> Result<(), String> {
    let has_promoted_binding = definition.jobs.iter().any(|job| {
        job.inputs
            .values()
            .any(|binding| matches!(binding, WorkflowInputBinding::PromotedArtifact { .. }))
    });
    if has_promoted_binding && trigger_kind != "manual" {
        return Err("artifact selected at run time requires a manual workflow trigger".to_string());
    }
    Ok(())
}

fn validate_literal_input_constraints(
    input_name: &str,
    input_definition: &RunnerJobInputDefinition,
    value: &Value,
) -> Result<(), String> {
    if let Some(max_length) = input_definition.max_length {
        if let Some(value) = value.as_str()
            && value.chars().count() > max_length
        {
            return Err(format!(
                "workflow input {input_name} exceeds max length {max_length}"
            ));
        }
    }
    if let Some(max_json_bytes) = input_definition.max_json_bytes {
        let byte_len = serde_json::to_vec(value)
            .map_err(|error| error.to_string())?
            .len();
        if byte_len > max_json_bytes {
            return Err(format!(
                "workflow input {input_name} exceeds max JSON size {max_json_bytes} bytes"
            ));
        }
    }
    Ok(())
}

fn value_matches_input_kind(value: &Value, expected_kind: &str) -> bool {
    match expected_kind {
        "string" | "artifact" => value.is_string(),
        "integer" => value.as_i64().is_some(),
        "boolean" => value.is_boolean(),
        "json" => !value.is_null(),
        _ => false,
    }
}

fn describe_json_value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(number) => {
            if number.as_i64().is_some() {
                "integer"
            } else {
                "number"
            }
        }
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowRunnerCatalogEntry {
    id: String,
    name: String,
    jobs: Vec<RunnerJobDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkflowEditorJob {
    runner_id: String,
    runner_job_name: String,
    outcome_policy: WorkflowJobOutcomePolicy,
    #[serde(default)]
    inputs: BTreeMap<String, WorkflowInputBinding>,
}

fn workflow_runner_catalog(
    state: &Arc<AppState>,
) -> Result<Vec<WorkflowRunnerCatalogEntry>, Response> {
    let runners = state.db.list_runners().map_err(internal_error)?;
    let mut catalog = Vec::with_capacity(runners.len());
    for runner in runners {
        let jobs = state
            .db
            .list_runner_jobs(&runner.id)
            .map_err(internal_error)?;
        catalog.push(WorkflowRunnerCatalogEntry {
            id: runner.id,
            name: runner.name,
            jobs,
        });
    }
    Ok(catalog)
}

fn workflow_form_view(
    workflow: Option<&Workflow>,
    runner_catalog: &[WorkflowRunnerCatalogEntry],
    repo_field: Markup,
) -> workflow_view::WorkflowFormView {
    let (trigger_kind, branch_name, jobs_json, definition) = if let Some(item) = workflow {
        let trigger: WorkflowTrigger =
            serde_json::from_str(&item.trigger_json).unwrap_or(WorkflowTrigger {
                kind: "push".to_string(),
                branches: vec!["main".to_string()],
            });
        let definition: WorkflowDefinition = serde_json::from_str(&item.definition_json)
            .unwrap_or(WorkflowDefinition { jobs: Vec::new() });
        let jobs_json =
            serde_json::to_string(&definition.jobs).unwrap_or_else(|_| "[]".to_string());
        (
            trigger.kind,
            trigger.branches.first().cloned().unwrap_or_default(),
            jobs_json,
            definition,
        )
    } else {
        (
            "push".to_string(),
            "main".to_string(),
            "[]".to_string(),
            WorkflowDefinition { jobs: Vec::new() },
        )
    };

    let mut catalog = runner_catalog.to_vec();
    for job in &definition.jobs {
        if let Some(runner) = catalog.iter_mut().find(|runner| runner.id == job.runner_id) {
            if !runner
                .jobs
                .iter()
                .any(|item| item.name == job.runner_job_name)
            {
                runner.jobs.push(RunnerJobDefinition {
                    name: job.runner_job_name.clone(),
                    ..RunnerJobDefinition::default()
                });
                runner
                    .jobs
                    .sort_by(|left, right| left.name.cmp(&right.name));
            }
        } else {
            catalog.push(WorkflowRunnerCatalogEntry {
                id: job.runner_id.clone(),
                name: job.runner_id.clone(),
                jobs: vec![RunnerJobDefinition {
                    name: job.runner_job_name.clone(),
                    ..RunnerJobDefinition::default()
                }],
            });
        }
    }

    let mut initial_jobs = definition
        .jobs
        .iter()
        .map(|job| WorkflowEditorJob {
            runner_id: job.runner_id.clone(),
            runner_job_name: job.runner_job_name.clone(),
            outcome_policy: job.outcome_policy.clone(),
            inputs: job.inputs.clone(),
        })
        .collect::<Vec<_>>();
    if initial_jobs.is_empty() {
        let (runner_id, runner_job_name) = catalog
            .first()
            .map(|runner| {
                (
                    if catalog.len() == 1 {
                        runner.id.clone()
                    } else {
                        String::new()
                    },
                    if catalog.len() == 1 && runner.jobs.len() == 1 {
                        runner
                            .jobs
                            .first()
                            .map(|job| job.name.clone())
                            .unwrap_or_default()
                    } else {
                        String::new()
                    },
                )
            })
            .unwrap_or_default();
        initial_jobs.push(WorkflowEditorJob {
            runner_id,
            runner_job_name,
            outcome_policy: WorkflowJobOutcomePolicy::Required,
            inputs: BTreeMap::new(),
        });
    }

    workflow_view::WorkflowFormView {
        name: workflow.map(|item| item.name.clone()).unwrap_or_default(),
        trigger_kind,
        branch_name,
        jobs_json,
        repo_field,
        runner_catalog_json: workflow_view::script_json(
            &serde_json::to_string(&catalog).unwrap_or_else(|_| "[]".to_string()),
        ),
        initial_jobs_json: workflow_view::script_json(
            &serde_json::to_string(&initial_jobs).unwrap_or_else(|_| "[]".to_string()),
        ),
        is_edit: workflow.is_some(),
        error: None,
    }
}

fn workflow_form_view_from_submission(
    form: &WorkflowForm,
    runner_catalog: &[WorkflowRunnerCatalogEntry],
    repo_field: Markup,
    is_edit: bool,
    error: String,
) -> workflow_view::WorkflowFormView {
    let jobs = serde_json::from_str::<Vec<WorkflowEditorJob>>(&form.jobs_json).unwrap_or_default();
    workflow_view::WorkflowFormView {
        name: form.name.clone(),
        trigger_kind: form.trigger_kind.clone(),
        branch_name: form.branch_name.clone(),
        jobs_json: form.jobs_json.clone(),
        repo_field,
        runner_catalog_json: workflow_view::script_json(
            &serde_json::to_string(runner_catalog).unwrap_or_else(|_| "[]".to_string()),
        ),
        initial_jobs_json: workflow_view::script_json(
            &serde_json::to_string(&jobs).unwrap_or_else(|_| "[]".to_string()),
        ),
        is_edit,
        error: Some(error),
    }
}

fn repo_form_view(form: Option<&CreateRepoForm>) -> repo_view::RepoFormView {
    repo_view::RepoFormView {
        name: form.map(|form| form.name.clone()).unwrap_or_default(),
        default_branch: form
            .map(|form| form.default_branch.clone())
            .unwrap_or_else(|| "main".to_string()),
    }
}

fn runner_form_view(form: &CreateRunnerForm) -> runner_view::RunnerFormView {
    runner_view::RunnerFormView {
        name: form.name.clone(),
        base_url: form.base_url.clone(),
    }
}

fn runner_auth_view(state: &Arc<AppState>) -> runner_view::RunnerAuthView {
    runner_view::RunnerAuthView {
        key_id: state.runner_signer.key_id().to_string(),
        public_key: state.runner_signer.public_key_base64(),
    }
}

fn render_users_form_error(
    state: &Arc<AppState>,
    user: &User,
    form: &CreateUserForm,
    error: String,
) -> Response {
    match state.db.list_users() {
        Ok(users) => html_bad_request(user_view::users_page(
            users,
            &csrf_token(state, user),
            Some(&error),
            user_view::CreateUserFormView {
                username: form.username.clone(),
            },
        )),
        Err(error) => internal_error(error),
    }
}

fn render_repos_form_error(
    state: &Arc<AppState>,
    user: &User,
    form: &CreateRepoForm,
    error: impl std::fmt::Display,
) -> Response {
    let result = (|| {
        let repos = state.db.list_repos().map_err(internal_error)?;
        let repo_cards = repos
            .into_iter()
            .map(|repo| repo_view::RepoCard {
                clone_url: repo_clone_url(state, &repo),
                repo,
            })
            .collect::<Vec<_>>();
        Ok::<_, Response>(repo_view::repos_page(
            repo_cards,
            &csrf_token(state, user),
            Some(&error.to_string()),
            repo_form_view(Some(form)),
        ))
    })();
    match result {
        Ok(markup) => html_bad_request(markup),
        Err(response) => response,
    }
}

fn render_runners_form_error(
    state: &Arc<AppState>,
    user: &User,
    form: &CreateRunnerForm,
    error: String,
) -> Response {
    let result = (|| {
        let runners_with_jobs = runners_with_jobs(state)?;
        Ok::<_, Response>(runner_view::runners_page(
            runners_with_jobs,
            runner_auth_view(state),
            &csrf_token(state, user),
            Some(&error),
            runner_form_view(form),
        ))
    })();
    match result {
        Ok(markup) => html_bad_request(markup),
        Err(response) => response,
    }
}

fn workflow_cards_for_repos(
    state: &Arc<AppState>,
    repos: &[Repo],
    workflows: Vec<Workflow>,
) -> Result<Vec<workflow_view::WorkflowCard>, Response> {
    workflows
        .into_iter()
        .filter_map(|workflow| {
            let repo = repos
                .iter()
                .find(|repo| repo.id == workflow.repo_id)?
                .clone();
            let schema_report = workflow_schema_report(state, &workflow).map_err(internal_error);
            Some((workflow, repo, schema_report))
        })
        .map(|(workflow, repo, schema_report)| {
            let schema_report = schema_report?;
            let trigger: WorkflowTrigger =
                serde_json::from_str(&workflow.trigger_json).unwrap_or(WorkflowTrigger {
                    kind: "push".to_string(),
                    branches: Vec::new(),
                });
            let definition: WorkflowDefinition = serde_json::from_str(&workflow.definition_json)
                .unwrap_or(WorkflowDefinition { jobs: Vec::new() });
            let manual_artifacts = manual_artifact_fields(state, &workflow)?;
            Ok(workflow_view::WorkflowCard {
                workflow,
                repo,
                schema_report,
                trigger,
                job_count: definition.jobs.len(),
                manual_artifacts,
            })
        })
        .collect::<Result<Vec<_>, Response>>()
}

#[derive(Clone)]
struct ManualArtifactRequirement {
    job_index: usize,
    input_name: String,
    source_runner_id: String,
    source_job_name: String,
    output_name: String,
}

fn manual_artifact_requirements(
    workflow: &Workflow,
) -> Result<Vec<ManualArtifactRequirement>, Response> {
    let definition: WorkflowDefinition =
        serde_json::from_str(&workflow.definition_json).map_err(internal_error_text)?;
    let mut requirements = Vec::new();
    for (job_index, job) in definition.jobs.iter().enumerate() {
        for (input_name, binding) in &job.inputs {
            if let WorkflowInputBinding::PromotedArtifact {
                source_runner_id,
                source_job_name,
                output_name,
            } = binding
            {
                requirements.push(ManualArtifactRequirement {
                    job_index,
                    input_name: input_name.clone(),
                    source_runner_id: source_runner_id.clone(),
                    source_job_name: source_job_name.clone(),
                    output_name: output_name.clone(),
                });
            }
        }
    }
    Ok(requirements)
}

fn manual_artifact_field_name(requirement: &ManualArtifactRequirement) -> String {
    format!(
        "artifact:{}:{}",
        requirement.job_index, requirement.input_name
    )
}

fn manual_artifact_fields(
    state: &Arc<AppState>,
    workflow: &Workflow,
) -> Result<Vec<workflow_view::ManualArtifactField>, Response> {
    manual_artifact_requirements(workflow)?
        .into_iter()
        .map(|requirement| {
            let source_runner = state
                .db
                .get_runner(&requirement.source_runner_id)
                .map_err(internal_error)?;
            let source_label = format!(
                "{} / {} / {}",
                source_runner
                    .as_ref()
                    .map(|runner| runner.name.as_str())
                    .unwrap_or(requirement.source_runner_id.as_str()),
                requirement.source_job_name,
                requirement.output_name
            );
            let artifacts = state
                .db
                .list_recent_promotable_artifacts(
                    &workflow.repo_id,
                    &requirement.source_runner_id,
                    &requirement.source_job_name,
                    &requirement.output_name,
                    10,
                )
                .map_err(internal_error)?;
            let options = artifacts
                .into_iter()
                .map(|artifact| {
                    let label = promotable_artifact_label(&artifact);
                    workflow_view::ManualArtifactOption {
                        value: artifact.server_artifact_id,
                        label,
                    }
                })
                .collect();
            Ok(workflow_view::ManualArtifactField {
                field_name: manual_artifact_field_name(&requirement),
                label: format!(
                    "Artifact for job-{}.{}",
                    requirement.job_index + 1,
                    requirement.input_name
                ),
                source_label,
                options,
            })
        })
        .collect()
}

fn promotable_artifact_label(artifact: &models::PromotableArtifact) -> String {
    let revision = artifact
        .commit_sha
        .as_deref()
        .map(|commit| commit.chars().take(12).collect::<String>())
        .unwrap_or_else(|| "unknown commit".to_string());
    let branch = artifact.trigger_ref.as_deref().unwrap_or("unknown ref");
    let digest = artifact.sha256.chars().take(12).collect::<String>();
    format!(
        "{} @ {} · {} · {} · {} bytes · sha256:{}",
        revision, branch, artifact.workflow_name, artifact.created_at, artifact.size_bytes, digest
    )
}

fn validate_manual_artifact_selections(
    state: &Arc<AppState>,
    workflow: &Workflow,
    submitted: &BTreeMap<String, String>,
) -> Result<Vec<WorkflowRunArtifactSelection>, Response> {
    let mut selections = Vec::new();
    for requirement in manual_artifact_requirements(workflow)? {
        let field_name = manual_artifact_field_name(&requirement);
        let selected_id = submitted
            .get(&field_name)
            .map(String::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                bad_request(format!(
                    "select an artifact for job-{}.{}",
                    requirement.job_index + 1,
                    requirement.input_name
                ))
            })?;
        let eligible = state
            .db
            .list_recent_promotable_artifacts(
                &workflow.repo_id,
                &requirement.source_runner_id,
                &requirement.source_job_name,
                &requirement.output_name,
                10,
            )
            .map_err(internal_error)?;
        if !eligible
            .iter()
            .any(|artifact| artifact.server_artifact_id == selected_id)
        {
            return Err(bad_request(format!(
                "selected artifact for job-{}.{} is not among the 10 newest eligible artifacts",
                requirement.job_index + 1,
                requirement.input_name
            )));
        }
        selections.push(WorkflowRunArtifactSelection {
            job_index: requirement.job_index,
            input_name: requirement.input_name,
            server_artifact_id: selected_id.to_string(),
        });
    }
    Ok(selections)
}

fn render_create_workflow_form_error(
    state: &Arc<AppState>,
    user: &User,
    form: &WorkflowForm,
    error: String,
) -> Response {
    let result = (|| {
        let repos = visible_repos_for_user(state, user)?;
        let workflows = state.db.list_workflows().map_err(internal_error)?;
        let runner_catalog = workflow_runner_catalog(state)?;
        let workflow_cards = workflow_cards_for_repos(state, &repos, workflows)?;
        let repo_field = workflow_view::repo_selector(&repos);
        let form_view =
            workflow_form_view_from_submission(form, &runner_catalog, repo_field, false, error);
        Ok::<_, Response>(workflow_view::workflows_page(
            form_view,
            workflow_cards,
            &csrf_token(state, user),
        ))
    })();
    match result {
        Ok(markup) => html_bad_request(markup),
        Err(response) => response,
    }
}

fn render_update_workflow_form_error(
    state: &Arc<AppState>,
    user: &User,
    workflow: &Workflow,
    form: &WorkflowForm,
    error: String,
) -> Response {
    let result = (|| {
        let repo = state
            .db
            .get_repo(&workflow.repo_id)
            .map_err(internal_error)?
            .ok_or_else(|| not_found("repo"))?;
        let runner_catalog = workflow_runner_catalog(state)?;
        let repo_field = workflow_view::fixed_repo_field(workflow, &repo);
        let schema_report = workflow_schema_report(state, workflow).map_err(internal_error)?;
        let form_view =
            workflow_form_view_from_submission(form, &runner_catalog, repo_field, true, error);
        let manual_artifacts = manual_artifact_fields(state, workflow)?;
        Ok::<_, Response>(workflow_view::workflow_detail_page(
            workflow,
            &repo,
            schema_report,
            form_view,
            &csrf_token(state, user),
            manual_artifacts,
        ))
    })();
    match result {
        Ok(markup) => html_bad_request(markup),
        Err(response) => response,
    }
}

fn runners_with_jobs(
    state: &Arc<AppState>,
) -> Result<Vec<(models::Runner, Vec<RunnerJobDefinition>)>, Response> {
    state
        .db
        .list_runners()
        .map_err(internal_error)?
        .into_iter()
        .map(|runner| {
            let jobs = state
                .db
                .list_runner_jobs(&runner.id)
                .map_err(internal_error)?;
            Ok((runner, jobs))
        })
        .collect::<Result<Vec<_>, Response>>()
}

fn visible_repos_for_user(state: &Arc<AppState>, user: &User) -> Result<Vec<Repo>, Response> {
    require_admin(user)?;
    state.db.list_repos().map_err(internal_error)
}
fn require_admin(user: &User) -> Result<(), Response> {
    if user.role.is_admin() {
        Ok(())
    } else {
        Err(forbidden("admin role required"))
    }
}
fn authorized_repo(state: &Arc<AppState>, user: &User, repo_id: &str) -> Result<Repo, Response> {
    require_admin(user)?;
    let repo = state
        .db
        .get_repo(repo_id)
        .map_err(internal_error)?
        .ok_or_else(|| not_found("repo"))?;
    Ok(repo)
}
fn authorized_workflow(
    state: &Arc<AppState>,
    user: &User,
    workflow_id: &str,
) -> Result<Workflow, Response> {
    let workflow = state
        .db
        .get_workflow(workflow_id)
        .map_err(internal_error)?
        .ok_or_else(|| not_found("workflow"))?;
    let repo = authorized_repo(state, user, &workflow.repo_id)?;
    if repo.id != workflow.repo_id {
        return Err(forbidden("workflow access denied"));
    }
    Ok(workflow)
}
fn authorized_pipeline(
    state: &Arc<AppState>,
    user: &User,
    pipeline_id: &str,
) -> Result<PipelineRun, Response> {
    let pipeline = state
        .db
        .get_pipeline_run(pipeline_id)
        .map_err(internal_error)?
        .ok_or_else(|| not_found("pipeline"))?;
    let repo = authorized_repo(state, user, &pipeline.repo_id)?;
    if repo.id != pipeline.repo_id {
        return Err(forbidden("pipeline access denied"));
    }
    Ok(pipeline)
}
fn repo_clone_url(state: &Arc<AppState>, repo: &Repo) -> String {
    format!(
        "ssh://git@{}/{}",
        state.config.server.public_base_url.trim_end_matches('/'),
        repo.name
    )
}
fn validate_username(username: &str) -> Result<(), String> {
    let trimmed = username.trim();
    if trimmed.len() < 3 {
        return Err("username must be at least 3 characters".to_string());
    }
    if trimmed.chars().count() > 64 {
        return Err("username must be 64 characters or fewer".to_string());
    }
    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
    {
        return Err("username contains invalid characters".to_string());
    }
    Ok(())
}
fn validate_password(password: &str) -> Result<(), String> {
    if password.len() < 8 {
        Err("password must be at least 8 characters".to_string())
    } else {
        Ok(())
    }
}
fn validate_role(role: &str) -> Result<UserRole, String> {
    UserRole::parse(role).ok_or_else(|| "role must be admin".to_string())
}
fn validate_branch_name(branch: &str) -> Result<(), String> {
    if branch.trim().is_empty() || branch.contains(' ') {
        Err("default branch is invalid".to_string())
    } else if branch.trim().chars().count() > 255 {
        Err("default branch must be 255 characters or fewer".to_string())
    } else {
        Ok(())
    }
}
fn validate_workflow_branch_filter(branch: &str) -> Result<(), String> {
    if branch.is_empty() {
        return Ok(());
    }
    if branch.contains(',') {
        return Err("branch name must contain only one branch".to_string());
    }
    if branch.chars().any(char::is_whitespace) {
        return Err("branch name must not contain whitespace".to_string());
    }
    if branch.chars().count() > 255 {
        return Err("branch name must be 255 characters or fewer".to_string());
    }
    Ok(())
}
fn validate_manual_ref(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{field} cannot be empty"));
    }
    if value.chars().any(char::is_whitespace) {
        return Err(format!("{field} must not contain whitespace"));
    }
    Ok(())
}
fn validate_runner_name(name: &str) -> Result<(), String> {
    if name.trim().is_empty() {
        Err("runner name cannot be empty".to_string())
    } else if name.trim().chars().count() > 120 {
        Err("runner name must be 120 characters or fewer".to_string())
    } else {
        Ok(())
    }
}
fn validate_base_url(url: &str, policy: &RunnerUrlPolicyConfig) -> Result<(), String> {
    if url.trim().chars().count() > 2048 {
        return Err("base_url must be 2048 characters or fewer".to_string());
    }
    let parsed = Url::parse(url.trim()).map_err(|_| "base_url must be a valid URL".to_string())?;
    if policy.require_https && parsed.scheme() != "https" {
        return Err("base_url must use https".to_string());
    }
    if parsed.host_str().is_none() {
        return Err("base_url must include a host".to_string());
    }
    if !policy.allow_credentials && (!parsed.username().is_empty() || parsed.password().is_some()) {
        return Err("base_url must not include credentials".to_string());
    }
    if !policy.allow_query && parsed.query().is_some() {
        return Err("base_url must not include query".to_string());
    }
    if !policy.allow_fragment && parsed.fragment().is_some() {
        return Err("base_url must not include fragment".to_string());
    }
    if !policy.allow_path && !matches!(parsed.path(), "" | "/") {
        return Err("base_url must not include a path".to_string());
    }
    if let Some(host) = parsed.host() {
        match host {
            url::Host::Domain(domain) => {
                let domain = domain.trim_end_matches('.').to_ascii_lowercase();
                if !policy.allow_localhost
                    && (matches!(domain.as_str(), "localhost" | "localhost.localdomain")
                        || domain.ends_with(".localhost"))
                {
                    return Err("base_url host must not be localhost".to_string());
                }
            }
            url::Host::Ipv4(ip) => {
                validate_ipv4_runner_host(ip, policy)?;
            }
            url::Host::Ipv6(ip) => {
                validate_ipv6_runner_host(ip, policy)?;
            }
        }
    }
    Ok(())
}

fn validate_ipv4_runner_host(ip: Ipv4Addr, policy: &RunnerUrlPolicyConfig) -> Result<(), String> {
    if !policy.allow_localhost && ip.is_loopback() {
        return Err("base_url host must not be localhost".to_string());
    }
    if !policy.allow_private_ips && (ip.is_private() || ip.is_unspecified() || ip.octets()[0] == 0)
    {
        return Err("base_url host must not be private or unspecified".to_string());
    }
    if !policy.allow_link_local_ips && ip.is_link_local() {
        return Err("base_url host must not be link-local".to_string());
    }
    if !policy.allow_documentation_ips && ip.is_documentation() {
        return Err("base_url host must not be documentation-only".to_string());
    }
    if !policy.allow_multicast_ips && (ip.octets()[0] >= 224 || ip.is_broadcast()) {
        return Err("base_url host must not be multicast or broadcast".to_string());
    }
    Ok(())
}

fn validate_ipv6_runner_host(ip: Ipv6Addr, policy: &RunnerUrlPolicyConfig) -> Result<(), String> {
    if !policy.allow_localhost && ip.is_loopback() {
        return Err("base_url host must not be localhost".to_string());
    }
    if !policy.allow_private_ips && (ip.is_unique_local() || ip.is_unspecified()) {
        return Err("base_url host must not be private or unspecified".to_string());
    }
    if !policy.allow_link_local_ips && ip.is_unicast_link_local() {
        return Err("base_url host must not be link-local".to_string());
    }
    if !policy.allow_documentation_ips
        && matches!(ip.segments()[0], 0x2001 if ip.segments()[1] == 0x0db8)
    {
        return Err("base_url host must not be documentation-only".to_string());
    }
    if !policy.allow_multicast_ips && ip.is_multicast() {
        return Err("base_url host must not be multicast or broadcast".to_string());
    }
    Ok(())
}
fn optional_trimmed_value(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}
pub(super) fn csrf_token(state: &Arc<AppState>, user: &User) -> String {
    let mut mac = HmacSha256::new_from_slice(state.config.auth.session_secret.as_bytes())
        .expect("valid hmac key");
    mac.update(b"csrf:");
    mac.update(user.id.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}
fn verify_csrf(state: &Arc<AppState>, user: &User, token: &str) -> Result<(), Response> {
    if token == csrf_token(state, user) {
        Ok(())
    } else {
        Err(forbidden("csrf validation failed"))
    }
}
fn internal_error(error: impl std::fmt::Display) -> Response {
    error!(error = %error, "request failed with internal error");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal server error").into_response()
}
fn internal_error_text(error: impl std::fmt::Display) -> Response {
    internal_error(error)
}
fn bad_request(error: impl std::fmt::Display) -> Response {
    (StatusCode::BAD_REQUEST, error.to_string()).into_response()
}
fn html_bad_request(markup: Markup) -> Response {
    (StatusCode::BAD_REQUEST, markup).into_response()
}
fn api_error(status: StatusCode, code: &'static str, message: impl std::fmt::Display) -> Response {
    (
        status,
        Json(ApiErrorEnvelope {
            error: ApiErrorBody {
                code,
                message: message.to_string(),
            },
        }),
    )
        .into_response()
}
fn api_bad_request(error: impl std::fmt::Display) -> Response {
    api_error(StatusCode::BAD_REQUEST, "bad_request", error)
}
fn api_forbidden(error: impl std::fmt::Display) -> Response {
    api_error(StatusCode::FORBIDDEN, "forbidden", error)
}
fn api_internal_error(error: impl std::fmt::Display) -> Response {
    error!(error = %error, "api request failed with internal error");
    api_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
        "internal server error",
    )
}
fn forbidden(error: impl std::fmt::Display) -> Response {
    (StatusCode::FORBIDDEN, error.to_string()).into_response()
}
fn not_found(entity: &str) -> Response {
    (StatusCode::NOT_FOUND, format!("{entity} not found")).into_response()
}
