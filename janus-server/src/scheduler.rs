use std::sync::Arc;

use chrono::{DateTime, Utc};
use glob::Pattern;
use janus_lib::{
    FailureCategory, JobOutput, JobStatus as RunnerJobStatus, SUPPORTED_RUNNER_PROTOCOL_VERSIONS,
};
use serde_json::{Map, Value, json};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{self, Duration, MissedTickBehavior},
};
use tracing::{error, info, warn};

use crate::{
    app::AppState,
    git,
    models::{
        JobRun, RunnerJobDefinition, Workflow, WorkflowDefinition, WorkflowInputBinding,
        WorkflowRunArtifactSelection, WorkflowTrigger,
    },
    runner::{JobLogsResponse, JobOutputMetadata},
    schema_diff::{WorkflowSchemaStatus, workflow_schema_report},
    state_machine::{self, JobStatus, PipelineStatus},
};

pub struct SchedulerHandles {
    reconcile: JoinHandle<()>,
    health: JoinHandle<()>,
}

pub fn spawn(state: Arc<AppState>, shutdown: watch::Receiver<bool>) -> SchedulerHandles {
    let scheduler_state = Arc::clone(&state);
    let mut reconcile_shutdown = shutdown.clone();
    let reconcile = tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_millis(
            scheduler_state.config.scheduler.poll_interval_ms.max(200),
        ));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                changed = reconcile_shutdown.changed() => {
                    if changed.is_ok() && *reconcile_shutdown.borrow() {
                        info!("scheduler reconcile loop shutting down");
                        break;
                    }
                    if changed.is_err() {
                        warn!("scheduler reconcile shutdown channel closed");
                        break;
                    }
                }
                _ = interval.tick() => {
                    if let Err(error) = reconcile(Arc::clone(&scheduler_state)).await {
                        warn!(%error, "scheduler loop failed");
                    }
                }
            }
        }
    });

    let health_state = Arc::clone(&state);
    let mut health_shutdown = shutdown;
    let health = tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(
            health_state
                .config
                .runners
                .healthcheck_interval_seconds
                .max(5),
        ));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                changed = health_shutdown.changed() => {
                    if changed.is_ok() && *health_shutdown.borrow() {
                        info!("runner health loop shutting down");
                        break;
                    }
                    if changed.is_err() {
                        warn!("runner health shutdown channel closed");
                        break;
                    }
                }
                _ = interval.tick() => {
                    if let Err(error) = refresh_runner_health(Arc::clone(&health_state)).await {
                        warn!(%error, "runner health refresh failed");
                    }
                }
            }
        }
    });

    SchedulerHandles { reconcile, health }
}

pub async fn shutdown(handles: SchedulerHandles) {
    let results = tokio::join!(handles.reconcile, handles.health);
    if let Err(error) = results.0 {
        warn!(%error, "scheduler reconcile task failed during shutdown");
    }
    if let Err(error) = results.1 {
        warn!(%error, "runner health task failed during shutdown");
    }
}

pub(crate) async fn reconcile_once(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    recover_incomplete_pipelines(Arc::clone(&state))?;
    process_push_events(Arc::clone(&state)).await?;
    dispatch_pending_jobs(Arc::clone(&state)).await?;
    poll_running_jobs(state).await?;
    Ok(())
}

async fn reconcile(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    reconcile_once(state).await
}

pub(crate) fn enqueue_workflow_run(
    state: Arc<AppState>,
    workflow: &Workflow,
    trigger_type: &str,
    trigger_ref: Option<&str>,
    commit_sha: Option<&str>,
) -> Result<String, Box<dyn std::error::Error>> {
    enqueue_workflow_run_with_artifacts(state, workflow, trigger_type, trigger_ref, commit_sha, &[])
}

pub(crate) fn enqueue_workflow_run_with_artifacts(
    state: Arc<AppState>,
    workflow: &Workflow,
    trigger_type: &str,
    trigger_ref: Option<&str>,
    commit_sha: Option<&str>,
    artifact_selections: &[WorkflowRunArtifactSelection],
) -> Result<String, Box<dyn std::error::Error>> {
    match workflow_schema_report(&state, workflow)?.status {
        WorkflowSchemaStatus::Current => {}
        WorkflowSchemaStatus::Stale => {
            warn!(
                workflow_id = %workflow.id,
                workflow_version_id = %workflow.version_id,
                workflow = %workflow.name,
                "workflow schema is stale; creating pipeline from saved schema snapshot"
            );
        }
        WorkflowSchemaStatus::Incompatible => {
            return Err(std::io::Error::other(format!(
                "incompatible workflow schema for workflow {}",
                workflow.name
            ))
            .into());
        }
    }
    let definition: WorkflowDefinition = serde_json::from_str(&workflow.definition_json)?;
    definition.validate().map_err(std::io::Error::other)?;
    let pipeline_id = state.db.create_pipeline_run(
        &workflow.repo_id,
        &workflow.id,
        &workflow.version_id,
        trigger_type,
        trigger_ref,
        commit_sha,
    )?;
    state
        .db
        .add_workflow_run_artifact_selections(&pipeline_id, artifact_selections)?;
    let mut previous_run_id: Option<String> = None;
    for (job_index, job) in definition.jobs.iter().enumerate() {
        let run_id = state.db.create_job_run(
            &pipeline_id,
            job_index as i64,
            &job.runner_id,
            &job.runner_job_name,
            &job.outcome_policy,
        )?;
        if let Some(previous) = previous_run_id.as_deref() {
            state.db.add_previous_job(&run_id, previous)?;
        }
        previous_run_id = Some(run_id);
    }
    Ok(pipeline_id)
}

pub(crate) async fn cancel_pipeline(
    state: Arc<AppState>,
    pipeline_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let jobs = state
        .db
        .list_job_runs_for_pipeline(pipeline_id)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let mut requested_remote_cancel = false;
    for job in jobs {
        let runner = state
            .db
            .get_runner(&job.runner_id)
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        if job.status == JobStatus::Running.as_str()
            && let (Some(runner), Some(runner_run_id)) = (runner, job.runner_run_id.clone())
        {
            state
                .runner_client
                .cancel_job_run(&runner, &runner_run_id)
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            state
                .db
                .mark_job_run_cancel_requested(&job.id, "user_requested")?;
            requested_remote_cancel = true;
        }
        if job.status == JobStatus::Pending.as_str() {
            state.db.set_job_run_status(&job.id, "canceled")?;
        }
    }
    if requested_remote_cancel {
        state
            .db
            .mark_pipeline_cancel_requested(pipeline_id, "user_requested")?;
    } else {
        state.db.set_pipeline_status(pipeline_id, "canceled")?;
    }
    Ok(())
}

async fn process_push_events(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    let events = state.db.list_unprocessed_push_events()?;
    for event in events {
        let workflows = state.db.workflows_for_repo(&event.repo_id)?;
        for workflow in workflows.into_iter().filter(|item| item.enabled) {
            let trigger: WorkflowTrigger = serde_json::from_str(&workflow.trigger_json)?;
            if trigger.kind != "push" {
                continue;
            }
            for ref_update in &event.refs {
                if !matches_branch(&trigger.branches, &ref_update.ref_name) {
                    continue;
                }
                match enqueue_workflow_run(
                    Arc::clone(&state),
                    &workflow,
                    "push",
                    Some(&ref_update.ref_name),
                    Some(&ref_update.new_rev),
                ) {
                    Ok(pipeline_id) => {
                        info!(pipeline_id, workflow = %workflow.name, "pipeline created from push event");
                    }
                    Err(error)
                        if error
                            .to_string()
                            .contains("incompatible workflow schema for workflow") =>
                    {
                        warn!(
                            workflow_id = %workflow.id,
                            workflow_version_id = %workflow.version_id,
                            workflow = %workflow.name,
                            %error,
                            "skipping push-triggered pipeline because workflow schema is incompatible"
                        );
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        state.db.mark_push_event_processed(&event.id)?;
    }
    Ok(())
}

fn recover_incomplete_pipelines(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    for pipeline in state.db.pipelines_requiring_recovery()? {
        let next_status = match pipeline.status.as_str() {
            "cancel_requested" => PipelineStatus::CancelRequested,
            "canceling" => PipelineStatus::Canceling,
            _ => PipelineStatus::Running,
        };
        state
            .db
            .set_pipeline_status(&pipeline.id, next_status.as_str())?;
    }
    Ok(())
}

async fn dispatch_pending_jobs(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    let jobs = state.db.list_job_runs_by_status(&["pending"])?;
    for job in jobs {
        let previous_jobs = state.db.previous_jobs_for_job_run(&job.id)?;
        let previous_job_state =
            state_machine::next_ready_job_status(previous_jobs.iter().filter_map(|item| {
                JobStatus::parse(&item.status)
                    .map(|status| (status, item.outcome_policy.allows_failure()))
            }));
        if previous_job_state == Some(JobStatus::Blocked) {
            state
                .db
                .set_job_run_status(&job.id, JobStatus::Blocked.as_str())?;
            state.db.finalize_pipeline_status(&job.pipeline_run_id)?;
            continue;
        }
        if previous_job_state.is_none() {
            continue;
        }
        let Some(runner) = state.db.get_runner(&job.runner_id)? else {
            state
                .db
                .set_job_run_status(&job.id, JobStatus::Failed.as_str())?;
            continue;
        };
        if !runner.enabled {
            continue;
        }
        let Some(pipeline) = state.db.pipeline_for_job_run(&job.id)? else {
            continue;
        };
        let definition_json = state
            .db
            .workflow_definition_json(&pipeline.workflow_version_id)?;
        let definition: WorkflowDefinition = serde_json::from_str(&definition_json)?;
        let job_schemas = state
            .db
            .workflow_job_schemas(&pipeline.workflow_version_id)?;
        let Some(job_definition) = definition.jobs.get(job.job_index as usize) else {
            continue;
        };
        let Some(runner_job_definition) = job_schemas.get(job.job_index as usize) else {
            continue;
        };
        let payload = match resolve_job_inputs(
            Arc::clone(&state),
            &pipeline,
            &job.id,
            job.job_index as usize,
            job_definition,
            runner_job_definition,
        )
        .await
        {
            Ok(payload) => payload,
            Err(error) => {
                let message = error.to_string();
                if !is_scheduler_artifact_transfer_error(&message) {
                    return Err(error);
                }
                retry_or_fail_infra_error(
                    Arc::clone(&state),
                    &job,
                    "spawn_error",
                    &format!("failed to prepare job inputs: {message}"),
                    &JobOutputMetadata::default(),
                    "",
                )?;
                continue;
            }
        };
        state
            .db
            .set_job_run_input_payload(&job.id, &Value::Object(payload.clone()))?;
        let created = match state
            .runner_client
            .create_job_run(
                &runner,
                &job.runner_job_name,
                &job.dispatch_idempotency_key,
                Value::Object(payload),
            )
            .await
        {
            Ok(created) => created,
            Err(error) => {
                let message = error.to_string();
                if is_runner_create_terminal_rejection(&message) {
                    fail_job_after_runner_create_rejection(
                        Arc::clone(&state),
                        &job,
                        &format!("failed to create runner job: {message}"),
                    )?;
                    continue;
                }
                return Err(std::io::Error::other(message).into());
            }
        };
        state
            .db
            .set_job_run_started(&job.id, &created.job_id, &created.started_at)?;
    }
    Ok(())
}

async fn poll_running_jobs(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    let jobs = state.db.list_job_runs_by_status(&[
        JobStatus::Running.as_str(),
        JobStatus::CancelRequested.as_str(),
        JobStatus::Canceling.as_str(),
    ])?;
    for job in jobs {
        let Some(runner) = state.db.get_runner(&job.runner_id)? else {
            continue;
        };
        let Some(runner_run_id) = &job.runner_run_id else {
            continue;
        };
        if retry_stuck_cancellation(Arc::clone(&state), &job, &runner, runner_run_id).await? {
            state.db.finalize_pipeline_status(&job.pipeline_run_id)?;
            continue;
        }
        match state
            .runner_client
            .get_job_run(&runner, runner_run_id)
            .await
        {
            Ok(status) => match &status.status {
                RunnerJobStatus::Running
                | RunnerJobStatus::CancelRequested
                | RunnerJobStatus::Canceling => {
                    let next_status = match &status.status {
                        RunnerJobStatus::Canceling => JobStatus::Canceling,
                        RunnerJobStatus::CancelRequested => {
                            if job.status == JobStatus::Canceling.as_str() {
                                JobStatus::Canceling
                            } else {
                                JobStatus::CancelRequested
                            }
                        }
                        RunnerJobStatus::Running => {
                            if matches!(job.status.as_str(), "cancel_requested" | "canceling") {
                                JobStatus::parse(&job.status).unwrap_or(JobStatus::Running)
                            } else {
                                JobStatus::Running
                            }
                        }
                        _ => unreachable!(),
                    };
                    if job.status != next_status.as_str() {
                        state.db.set_job_run_status(&job.id, next_status.as_str())?;
                        state.db.finalize_pipeline_status(&job.pipeline_run_id)?;
                    }
                }
                _ => {
                    let logs = state
                        .runner_client
                        .get_job_logs(&runner, runner_run_id)
                        .await
                        .unwrap_or_else(|error| {
                            error!(%error, "failed to fetch runner logs");
                            JobLogsResponse {
                                stdout: String::new(),
                                stderr: String::new(),
                            }
                        });
                    if should_retry_infra_failure(
                        &job,
                        &status,
                        state.config.scheduler.max_infra_retries,
                    ) {
                        let retry_count = state.db.requeue_job_run_for_infra_retry(&job.id)?;
                        info!(
                            job_run_id = %job.id,
                            terminal_reason = ?status.terminal_reason.as_ref().map(|reason| reason.as_str()),
                            retry_count,
                            "requeued job after infra-classified terminal failure"
                        );
                        state.db.finalize_pipeline_status(&job.pipeline_run_id)?;
                        continue;
                    }
                    let outputs = status.outputs.into_iter().collect::<Vec<_>>();
                    let terminal = match &status.status {
                        RunnerJobStatus::Success => JobStatus::Success,
                        RunnerJobStatus::Canceled => JobStatus::Canceled,
                        _ => JobStatus::Failed,
                    };
                    let outputs =
                        match persist_job_outputs(Arc::clone(&state), &runner, &job.id, outputs)
                            .await
                        {
                            Ok(outputs) => outputs,
                            Err(error) => {
                                retry_or_fail_infra_error(
                                    Arc::clone(&state),
                                    &job,
                                    "output_registration_failed",
                                    &format!("failed to persist job outputs: {error}"),
                                    &status.output_metadata,
                                    &logs.stdout,
                                )?;
                                continue;
                            }
                        };
                    state.db.finish_job_run(
                        &job.id,
                        terminal.as_str(),
                        status.duration_ms,
                        status.exit_code,
                        status
                            .terminal_reason
                            .as_ref()
                            .map(|reason| reason.as_str()),
                        status
                            .failure_category
                            .as_ref()
                            .map(|category| category.as_str()),
                        &status.output_metadata,
                        &logs.stdout,
                        &logs.stderr,
                        &outputs,
                    )?;
                    state.db.finalize_pipeline_status(&job.pipeline_run_id)?;
                }
            },
            Err(error) => {
                warn!(%error, job_run_id = %job.id, "runner status polling failed");
            }
        }
    }
    Ok(())
}

fn is_scheduler_artifact_transfer_error(message: &str) -> bool {
    message.contains("runner artifact upload failed")
        || message.contains("runner artifact download failed")
        || message.contains("pipeline missing commit sha")
        || message.contains("git archive failed")
}

fn is_runner_create_terminal_rejection(message: &str) -> bool {
    [
        "400 Bad Request",
        "401 Unauthorized",
        "403 Forbidden",
        "404 Not Found",
        "422 Unprocessable Entity",
    ]
    .iter()
    .any(|status| message.contains(status))
}

fn fail_job_after_runner_create_rejection(
    state: Arc<AppState>,
    job: &JobRun,
    stderr: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    warn!(
        job_run_id = %job.id,
        %stderr,
        "runner rejected job create request; marking job failed"
    );
    state.db.finish_job_run(
        &job.id,
        JobStatus::Failed.as_str(),
        None,
        None,
        Some("spawn_error"),
        Some("job"),
        &JobOutputMetadata::default(),
        "",
        stderr,
        &[],
    )?;
    state.db.finalize_pipeline_status(&job.pipeline_run_id)?;
    Ok(())
}

fn retry_or_fail_infra_error(
    state: Arc<AppState>,
    job: &JobRun,
    terminal_reason: &str,
    stderr: &str,
    output_metadata: &JobOutputMetadata,
    stdout: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if job.infra_retry_count < i64::from(state.config.scheduler.max_infra_retries) {
        let retry_count = state.db.requeue_job_run_for_infra_retry(&job.id)?;
        warn!(
            job_run_id = %job.id,
            retry_count,
            %stderr,
            "requeued job after scheduler-side infra error"
        );
    } else {
        warn!(
            job_run_id = %job.id,
            infra_retry_count = job.infra_retry_count,
            %stderr,
            "scheduler-side infra retry budget exhausted; marking job failed"
        );
        state.db.finish_job_run(
            &job.id,
            JobStatus::Failed.as_str(),
            None,
            None,
            Some(terminal_reason),
            Some("infra"),
            output_metadata,
            stdout,
            stderr,
            &[],
        )?;
    }
    state.db.finalize_pipeline_status(&job.pipeline_run_id)?;
    Ok(())
}

fn should_retry_infra_failure(
    job: &crate::models::JobRun,
    status: &crate::runner::JobStatusResponse,
    max_infra_retries: u32,
) -> bool {
    status.status == RunnerJobStatus::Failed
        && matches!(status.failure_category, Some(FailureCategory::Infra))
        && job.infra_retry_count < i64::from(max_infra_retries)
}

async fn retry_stuck_cancellation(
    state: Arc<AppState>,
    job: &crate::models::JobRun,
    runner: &crate::models::Runner,
    runner_run_id: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let local_status = JobStatus::parse(&job.status);
    if !matches!(
        local_status,
        Some(JobStatus::CancelRequested | JobStatus::Canceling)
    ) {
        return Ok(false);
    }
    let Some(started_at) = job
        .cancel_started_at
        .as_deref()
        .or(job.cancel_requested_at.as_deref())
    else {
        return Ok(false);
    };
    let elapsed = Utc::now()
        .signed_duration_since(DateTime::parse_from_rfc3339(started_at)?.with_timezone(&Utc));
    if elapsed.num_seconds() < state.config.scheduler.cancel_stuck_timeout_seconds as i64 {
        return Ok(false);
    }
    if job.cancel_retry_count >= i64::from(state.config.scheduler.max_cancel_retries) {
        warn!(
            job_run_id = %job.id,
            runner_run_id = %runner_run_id,
            cancel_retry_count = job.cancel_retry_count,
            max_cancel_retries = state.config.scheduler.max_cancel_retries,
            "job cancellation retry budget exhausted; marking job failed"
        );
        state.db.mark_job_run_cancel_retry_exhausted(&job.id)?;
        return Ok(true);
    }

    warn!(
        job_run_id = %job.id,
        runner_run_id = %runner_run_id,
        local_status = %job.status,
        elapsed_seconds = elapsed.num_seconds(),
        "job cancellation appears stuck; retrying cancel request"
    );
    state
        .runner_client
        .cancel_job_run(runner, runner_run_id)
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let retry_count = state.db.record_cancel_retry(&job.id)?;
    warn!(
        job_run_id = %job.id,
        runner_run_id = %runner_run_id,
        cancel_retry_count = retry_count,
        "job cancellation retried"
    );
    Ok(false)
}

async fn resolve_job_inputs(
    state: Arc<AppState>,
    pipeline: &crate::models::PipelineRun,
    job_run_id: &str,
    job_index: usize,
    job_definition: &crate::models::WorkflowJobDefinition,
    runner_job_definition: &RunnerJobDefinition,
) -> Result<Map<String, Value>, Box<dyn std::error::Error>> {
    let mut resolved = Map::new();
    for (key, value) in &job_definition.inputs {
        let expected_input_kind = runner_job_definition
            .inputs
            .get(key)
            .map(|item| item.kind.as_str());
        match value {
            WorkflowInputBinding::Commit => {
                resolved.insert(
                    key.clone(),
                    json!(pipeline.commit_sha.clone().unwrap_or_default()),
                );
            }
            WorkflowInputBinding::Branch => {
                resolved.insert(
                    key.clone(),
                    json!(pipeline.trigger_ref.clone().unwrap_or_default()),
                );
            }
            WorkflowInputBinding::SourceArtifact => {
                let artifact_id =
                    ensure_source_artifact(Arc::clone(&state), pipeline, &job_definition.runner_id)
                        .await?;
                resolved.insert(key.clone(), json!(artifact_id));
            }
            WorkflowInputBinding::PromotedArtifact { .. } => {
                let server_artifact_id = state
                    .db
                    .workflow_run_artifact_selection(&pipeline.id, job_index, key)?
                    .ok_or_else(|| {
                        format!(
                            "missing promoted artifact selection for job-{}.{}",
                            job_index + 1,
                            key
                        )
                    })?;
                let artifact = state
                    .db
                    .get_server_artifact_by_id(&server_artifact_id)?
                    .ok_or_else(|| {
                        format!("missing selected server artifact {server_artifact_id}")
                    })?;
                let bytes = state.artifacts.read_bytes(&artifact)?;
                let target_runner = state
                    .db
                    .get_runner(&job_definition.runner_id)?
                    .ok_or_else(|| format!("missing runner {}", job_definition.runner_id))?;
                let upload = state
                    .runner_client
                    .upload_artifact(&target_runner, bytes, &artifact.sha256)
                    .await
                    .map_err(|error| std::io::Error::other(error.to_string()))?;
                resolved.insert(key.clone(), json!(upload.artifact_id));
            }
            WorkflowInputBinding::JobOutput {
                job_index,
                output_name,
            } => {
                let binding = crate::models::JobOutputBinding {
                    job_index: *job_index,
                    output_name: output_name.clone(),
                };
                let upstream_run_id =
                    find_job_run_id(Arc::clone(&state), &pipeline.id, binding.job_index)?;
                let output = state
                    .db
                    .job_outputs(&upstream_run_id)?
                    .into_iter()
                    .find(|item| item.output_name == binding.output_name)
                    .ok_or_else(|| {
                        format!(
                            "missing output {} from job-{}",
                            binding.output_name,
                            binding.job_index + 1
                        )
                    })?;
                if let Some(expected_input_kind) = expected_input_kind
                    && output.kind != expected_input_kind
                {
                    return Err(format!(
                        "workflow input {key} expects {expected_input_kind} but job-{}.{} is {}",
                        binding.job_index + 1,
                        binding.output_name,
                        output.kind,
                    )
                    .into());
                }
                if output.kind == "artifact" {
                    let source_artifact_id =
                        output.runner_artifact_id.as_deref().ok_or_else(|| {
                            format!(
                                "artifact output {} missing artifact id",
                                binding.output_name
                            )
                        })?;
                    let artifact_id = ensure_runner_has_artifact(
                        Arc::clone(&state),
                        &job_definition.runner_id,
                        source_artifact_id,
                        &upstream_run_id,
                    )
                    .await?;
                    resolved.insert(key.clone(), json!(artifact_id));
                } else {
                    resolved.insert(
                        key.clone(),
                        output.value.ok_or_else(|| {
                            format!("typed output {} missing value", binding.output_name)
                        })?,
                    );
                }
            }
            WorkflowInputBinding::Literal { value } => {
                resolved.insert(key.clone(), value.clone());
            }
        }
    }

    if !resolved.contains_key("janus_job_run_id") {
        resolved.insert("janus_job_run_id".to_string(), json!(job_run_id));
    }
    Ok(resolved)
}

fn find_job_run_id(
    state: Arc<AppState>,
    pipeline_id: &str,
    job_index: usize,
) -> Result<String, Box<dyn std::error::Error>> {
    let snapshot = state
        .db
        .pipeline_snapshot(pipeline_id)?
        .ok_or_else(|| format!("missing pipeline {pipeline_id}"))?;
    snapshot
        .jobs
        .into_iter()
        .find(|item| item.run.job_index == job_index as i64)
        .map(|item| item.run.id)
        .ok_or_else(|| format!("missing upstream job-{}", job_index + 1).into())
}

async fn ensure_source_artifact(
    state: Arc<AppState>,
    pipeline: &crate::models::PipelineRun,
    runner_id: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let server_artifact = ensure_server_source_artifact(Arc::clone(&state), pipeline)?;
    let bytes = state.artifacts.read_bytes(&server_artifact)?;
    let Some(runner) = state.db.get_runner(runner_id)? else {
        return Err(format!("missing runner {runner_id}").into());
    };
    let upload = state
        .runner_client
        .upload_artifact(&runner, bytes, &server_artifact.sha256)
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    Ok(upload.artifact_id)
}

async fn ensure_runner_has_artifact(
    state: Arc<AppState>,
    target_runner_id: &str,
    source_artifact_id: &str,
    upstream_job_run_id: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let snapshot = state
        .db
        .pipeline_snapshot(
            &state
                .db
                .pipeline_for_job_run(upstream_job_run_id)?
                .ok_or("missing pipeline for job")?
                .id,
        )?
        .ok_or("missing pipeline snapshot")?;
    let upstream = snapshot
        .jobs
        .into_iter()
        .find(|item| item.run.id == upstream_job_run_id)
        .ok_or("missing upstream run")?;
    if upstream.run.runner_id == target_runner_id {
        return Ok(source_artifact_id.to_string());
    }
    let Some(target_runner) = state.db.get_runner(target_runner_id)? else {
        return Err("missing target runner".into());
    };
    let server_artifact_id = upstream
        .outputs
        .iter()
        .find(|item| item.runner_artifact_id.as_deref() == Some(source_artifact_id))
        .and_then(|item| item.server_artifact_id.clone())
        .ok_or("missing server-managed artifact mirror")?;
    let server_artifact = state
        .db
        .get_server_artifact_by_id(&server_artifact_id)?
        .ok_or("missing server artifact record")?;
    let bytes = state.artifacts.read_bytes(&server_artifact)?;
    let upload = state
        .runner_client
        .upload_artifact(&target_runner, bytes, &server_artifact.sha256)
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    Ok(upload.artifact_id)
}

fn ensure_server_source_artifact(
    state: Arc<AppState>,
    pipeline: &crate::models::PipelineRun,
) -> Result<crate::models::ServerArtifact, Box<dyn std::error::Error>> {
    if let Some(existing) =
        state
            .db
            .get_server_artifact("pipeline_source", &pipeline.id, "source")?
    {
        return Ok(existing);
    }
    let Some(repo) = state.db.get_repo(&pipeline.repo_id)? else {
        return Err(format!("missing repo {}", pipeline.repo_id).into());
    };
    let commit_sha = pipeline
        .commit_sha
        .clone()
        .ok_or_else(|| "pipeline missing commit sha".to_string())?;
    let archive_path = std::path::PathBuf::from(&state.config.data_dir)
        .join("source-archives")
        .join(format!("{}-{}.tar.gz", pipeline.id, commit_sha));
    if !archive_path.exists() {
        git::create_source_archive(
            std::path::PathBuf::from(repo.bare_path).as_path(),
            &commit_sha,
            &archive_path,
        )?;
    }
    let pending =
        state
            .artifacts
            .store_file("pipeline_source", &pipeline.id, "source", &archive_path)?;
    state.db.insert_server_artifact(&pending)?;
    state
        .db
        .get_server_artifact_by_id(&pending.id)?
        .ok_or_else(|| "missing stored source artifact".into())
}

async fn persist_job_outputs(
    state: Arc<AppState>,
    runner: &crate::models::Runner,
    job_run_id: &str,
    outputs: Vec<(String, JobOutput)>,
) -> Result<Vec<crate::models::JobRunOutput>, Box<dyn std::error::Error>> {
    let mut persisted = Vec::new();
    for (name, output) in outputs {
        match output {
            JobOutput::Artifact {
                artifact_id,
                sha256,
                size,
            } => {
                let bytes = state
                    .runner_client
                    .download_artifact(runner, &artifact_id)
                    .await
                    .map_err(|error| std::io::Error::other(error.to_string()))?;
                let pending =
                    state
                        .artifacts
                        .store_bytes("job_output", job_run_id, &name, &bytes)?;
                let server_artifact_id = state.db.insert_server_artifact(&pending)?;
                persisted.push(crate::models::JobRunOutput {
                    output_name: name,
                    kind: "artifact".to_string(),
                    runner_artifact_id: Some(artifact_id),
                    server_artifact_id: Some(server_artifact_id),
                    value: None,
                    sha256: Some(sha256),
                    size_bytes: Some(size as i64),
                });
            }
            JobOutput::String { value } => {
                persisted.push(crate::models::JobRunOutput {
                    output_name: name,
                    kind: "string".to_string(),
                    runner_artifact_id: None,
                    server_artifact_id: None,
                    value: Some(json!(value)),
                    sha256: None,
                    size_bytes: None,
                });
            }
            JobOutput::Integer { value } => {
                persisted.push(crate::models::JobRunOutput {
                    output_name: name,
                    kind: "integer".to_string(),
                    runner_artifact_id: None,
                    server_artifact_id: None,
                    value: Some(json!(value)),
                    sha256: None,
                    size_bytes: None,
                });
            }
            JobOutput::Boolean { value } => {
                persisted.push(crate::models::JobRunOutput {
                    output_name: name,
                    kind: "boolean".to_string(),
                    runner_artifact_id: None,
                    server_artifact_id: None,
                    value: Some(json!(value)),
                    sha256: None,
                    size_bytes: None,
                });
            }
            JobOutput::Json { value } => {
                persisted.push(crate::models::JobRunOutput {
                    output_name: name,
                    kind: "json".to_string(),
                    runner_artifact_id: None,
                    server_artifact_id: None,
                    value: Some(value),
                    sha256: None,
                    size_bytes: None,
                });
            }
        }
    }
    Ok(persisted)
}

fn matches_branch(patterns: &[String], ref_name: &str) -> bool {
    if patterns.is_empty() {
        return true;
    }
    let branch = ref_name.trim_start_matches("refs/heads/");
    patterns.iter().any(|pattern| {
        Pattern::new(pattern)
            .map(|compiled| compiled.matches(branch) || compiled.matches(ref_name))
            .unwrap_or(false)
    })
}

pub(crate) async fn refresh_runner_health(
    state: Arc<AppState>,
) -> Result<(), Box<dyn std::error::Error>> {
    let runners = state.db.list_runners()?;
    for runner in runners.into_iter().filter(|item| item.enabled) {
        match state.runner_client.capabilities(&runner).await {
            Ok(capabilities) => {
                if !capabilities
                    .is_compatible_with_supported_versions(SUPPORTED_RUNNER_PROTOCOL_VERSIONS)
                {
                    warn!(
                        runner = %runner.name,
                        protocol_version = capabilities.protocol_version,
                        supported_protocol_versions = ?capabilities.supported_protocol_versions,
                        server_supported_protocol_versions = ?SUPPORTED_RUNNER_PROTOCOL_VERSIONS,
                        "runner protocol is incompatible"
                    );
                    state.db.update_runner_health(&runner.id, "incompatible")?;
                    continue;
                }
            }
            Err(error) => {
                warn!(runner = %runner.name, %error, "runner capabilities check failed");
                state.db.update_runner_health(&runner.id, "unreachable")?;
                continue;
            }
        }
        match state.runner_client.list_jobs(&runner).await {
            Ok(jobs) => {
                state.db.replace_runner_jobs(&runner.id, &jobs)?;
                state.db.update_runner_health(&runner.id, "healthy")?;
            }
            Err(error) => {
                warn!(runner = %runner.name, %error, "runner health check failed");
                state.db.update_runner_health(&runner.id, "unreachable")?;
            }
        }
    }
    Ok(())
}
