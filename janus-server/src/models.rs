use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};
use serde_json::Value;
pub type RunnerJobInputDefinition = janus_lib::JobInputDefinitionResponse;
pub type RunnerJobOutputDefinition = janus_lib::JobOutputDefinitionResponse;
pub type RunnerJobDefinition = janus_lib::JobDefinitionResponse;
use janus_lib::JobOutputMetadata;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub username: String,
    pub role: UserRole,
    pub created_at: String,
}

#[cfg(test)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub id: String,
    pub actor_user_id: Option<String>,
    pub actor_username: Option<String>,
    pub action: String,
    pub target_type: String,
    pub target_id: Option<String>,
    pub target_name: Option<String>,
    pub metadata_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserRole {
    Admin,
}

impl UserRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Admin => "admin",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "admin" => Some(Self::Admin),
            _ => None,
        }
    }

    pub fn is_admin(self) -> bool {
        matches!(self, Self::Admin)
    }
}

impl fmt::Display for UserRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Repo {
    pub id: String,
    pub name: String,
    pub normalized_name: String,
    pub bare_path: String,
    pub default_branch: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Runner {
    pub id: String,
    pub name: String,
    pub base_url: String,
    pub enabled: bool,
    pub last_health_state: String,
    pub last_seen_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    pub id: String,
    pub repo_id: String,
    pub name: String,
    pub enabled: bool,
    pub created_at: String,
    pub version: i64,
    pub version_id: String,
    pub trigger_json: String,
    pub definition_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushEvent {
    pub id: String,
    pub repo_id: String,
    pub received_at: String,
    pub event_key: String,
    pub processed_at: Option<String>,
    pub refs: Vec<PushEventRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushEventRef {
    pub old_rev: String,
    pub new_rev: String,
    pub ref_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineRun {
    pub id: String,
    pub repo_id: String,
    pub workflow_id: String,
    pub workflow_version_id: String,
    pub trigger_type: String,
    pub trigger_ref: Option<String>,
    pub commit_sha: Option<String>,
    pub status: String,
    pub started_at: String,
    pub cancel_reason: Option<String>,
    pub cancel_requested_at: Option<String>,
    pub cancel_started_at: Option<String>,
    pub finished_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRun {
    pub id: String,
    pub pipeline_run_id: String,
    pub job_index: i64,
    pub runner_id: String,
    pub runner_job_name: String,
    pub dispatch_idempotency_key: String,
    pub runner_run_id: Option<String>,
    pub status: String,
    pub outcome_policy: WorkflowJobOutcomePolicy,
    pub started_at: Option<String>,
    pub duration_ms: Option<i64>,
    pub exit_code: Option<i32>,
    pub terminal_reason: Option<String>,
    pub failure_category: Option<String>,
    pub cancel_reason: Option<String>,
    pub cancel_requested_at: Option<String>,
    pub cancel_started_at: Option<String>,
    pub cancel_retry_count: i64,
    pub last_cancel_retry_at: Option<String>,
    pub infra_retry_count: i64,
    pub last_infra_retry_at: Option<String>,
    pub finished_at: Option<String>,
    pub output_metadata: JobOutputMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowTrigger {
    pub kind: String,
    #[serde(default)]
    pub branches: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDefinition {
    pub jobs: Vec<WorkflowJobDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowJobDefinition {
    pub runner_id: String,
    pub runner_job_name: String,
    #[serde(default)]
    pub inputs: BTreeMap<String, WorkflowInputBinding>,
    #[serde(default)]
    pub outcome_policy: WorkflowJobOutcomePolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowJobOutcomePolicy {
    #[default]
    Required,
    AllowedToFail,
}

impl WorkflowJobOutcomePolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Required => "required",
            Self::AllowedToFail => "allowed_to_fail",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "required" => Some(Self::Required),
            "allowed_to_fail" => Some(Self::AllowedToFail),
            _ => None,
        }
    }

    pub fn allows_failure(&self) -> bool {
        matches!(self, Self::AllowedToFail)
    }
}

impl WorkflowDefinition {
    pub fn validate(&self) -> Result<(), String> {
        if self.jobs.is_empty() {
            return Err("workflow must contain at least one job".to_string());
        }
        for (index, job) in self.jobs.iter().enumerate() {
            if job.runner_id.trim().is_empty() {
                return Err(format!("workflow job {} must choose a runner", index + 1));
            }
            if job.runner_job_name.trim().is_empty() {
                return Err(format!(
                    "workflow job {} must choose a runner job",
                    index + 1
                ));
            }
        }
        Ok(())
    }
}

impl JobRun {
    pub fn display_name(&self) -> String {
        format!("job-{} / {}", self.job_index + 1, self.runner_job_name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkflowInputBinding {
    Literal {
        value: Value,
    },
    Commit,
    Branch,
    SourceArtifact,
    PromotedArtifact {
        source_runner_id: String,
        source_job_name: String,
        output_name: String,
    },
    JobOutput {
        job_index: usize,
        output_name: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowRunArtifactSelection {
    pub job_index: usize,
    pub input_name: String,
    pub server_artifact_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromotableArtifact {
    pub server_artifact_id: String,
    pub sha256: String,
    pub size_bytes: i64,
    pub created_at: String,
    pub pipeline_run_id: String,
    pub workflow_name: String,
    pub job_run_id: String,
    pub runner_id: String,
    pub runner_name: String,
    pub runner_job_name: String,
    pub output_name: String,
    pub commit_sha: Option<String>,
    pub trigger_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobOutputBinding {
    pub job_index: usize,
    pub output_name: String,
}

pub fn parse_job_output_binding(value: &WorkflowInputBinding) -> Option<JobOutputBinding> {
    let WorkflowInputBinding::JobOutput {
        job_index,
        output_name,
    } = value
    else {
        return None;
    };
    if output_name.is_empty() {
        return None;
    }
    Some(JobOutputBinding {
        job_index: *job_index,
        output_name: output_name.clone(),
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSnapshot {
    pub pipeline: PipelineRun,
    pub jobs: Vec<JobRunDetail>,
    pub artifact_selections: Vec<WorkflowRunArtifactSelection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRunDetail {
    pub run: JobRun,
    pub stdout: String,
    pub stderr: String,
    pub outputs: Vec<JobRunOutput>,
    pub previous_jobs: Vec<PreviousJobSummary>,
    pub resolved_inputs: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreviousJobSummary {
    pub job_run_id: String,
    pub job_index: i64,
    pub runner_job_name: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRunOutput {
    pub output_name: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub runner_artifact_id: Option<String>,
    pub server_artifact_id: Option<String>,
    pub value: Option<Value>,
    pub sha256: Option<String>,
    pub size_bytes: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerArtifact {
    pub id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub artifact_name: String,
    pub sha256: String,
    pub size_bytes: i64,
    pub storage_path: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducedArtifact {
    pub id: String,
    pub artifact_name: String,
    pub sha256: String,
    pub size_bytes: i64,
    pub created_at: String,
    pub repo_id: String,
    pub repo_name: String,
    pub workflow_name: String,
    pub pipeline_run_id: String,
    pub job_run_id: String,
    pub runner_name: String,
    pub runner_job_name: String,
    pub commit_sha: Option<String>,
    pub trigger_ref: Option<String>,
}
