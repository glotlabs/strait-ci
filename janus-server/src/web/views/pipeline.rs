use maud::{Markup, html};

use crate::models::{self, PipelineRun, Repo};

use super::components::{
    badge, csrf_input, display_status, layout, page_intro, render_optional, status_tone,
};

pub(crate) fn pipelines_page(pipelines: Vec<(PipelineRun, Repo)>) -> Markup {
    layout(
        "Pipelines",
        html! {
            (page_intro(
                "Pipelines",
                "Track workflow executions across repositories and inspect current run state.",
            ))
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Runs" }
                        h2 { "Recent pipelines" }
                    }
                }
                div class="stack-md" {
                    @for (pipeline, repo) in &pipelines {
                        article class="list-row" {
                            div {
                                h3 {
                                    a href=(format!("/pipelines/{}", pipeline.id)) { (pipeline.id) }
                                }
                                p class="muted" { (repo.name) }
                            }
                            div class="list-row-meta" {
                                (badge(&display_status(&pipeline.status), status_tone(&pipeline.status)))
                                span { (pipeline.trigger_ref.clone().unwrap_or_default()) }
                            }
                        }
                    }
                }
            }
        },
    )
}

pub(crate) fn pipeline_detail_page(
    pipeline: &PipelineRun,
    snapshot: &models::PipelineSnapshot,
    csrf: &str,
) -> Markup {
    layout(
        "Pipeline",
        html! {
            (page_intro(
                "Pipeline Detail",
                "Inspect execution state, retry behavior, logs, and artifact/output metadata per job.",
            ))
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Execution" }
                        h2 { (snapshot.pipeline.id) }
                    }
                    div class="badge-row" {
                        (badge(&display_status(&snapshot.pipeline.status), status_tone(&snapshot.pipeline.status)))
                    }
                }
                div class="meta-grid" {
                    div class="meta-pair" { span { "Trigger ref" } strong { (snapshot.pipeline.trigger_ref.clone().unwrap_or_default()) } }
                    div class="meta-pair" { span { "Cancel reason" } strong { (snapshot.pipeline.cancel_reason.clone().unwrap_or_default()) } }
                    div class="meta-pair" { span { "Cancel requested" } strong { (snapshot.pipeline.cancel_requested_at.clone().unwrap_or_default()) } }
                    div class="meta-pair" { span { "Cancel started" } strong { (snapshot.pipeline.cancel_started_at.clone().unwrap_or_default()) } }
                }
                @if !snapshot.artifact_selections.is_empty() {
                    div class="stack-md" {
                        span class="subsection-title" { "Promoted artifacts" }
                        @for selection in &snapshot.artifact_selections {
                            div class="list-row" {
                                strong { "job-" (selection.job_index + 1) "." (selection.input_name) }
                                a href=(format!("/artifacts/{}", selection.server_artifact_id)) {
                                    (selection.server_artifact_id)
                                }
                            }
                        }
                    }
                }
                div class="actions" {
                    form method="post" action=(format!("/pipelines/{}/rerun", snapshot.pipeline.id)) {
                        (csrf_input(csrf))
                        button type="submit" class="secondary" { "Rerun pipeline" }
                    }
                    form method="post" action=(format!("/pipelines/{}/cancel", snapshot.pipeline.id)) {
                        (csrf_input(csrf))
                        button type="submit" class="ghost" { "Cancel pipeline" }
                    }
                }
            }
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Jobs" }
                        h2 { "Job runs" }
                    }
                }
                div class="stack-lg" {
                    @for job in &snapshot.jobs {
                        @let resolved_inputs_json =
                            serde_json::to_string_pretty(&job.resolved_inputs)
                                .unwrap_or_else(|_| "{}".to_string());
                        article class="job-card" {
                            div class="entity-head" {
                                div {
                                    h3 { (job.run.display_name()) }
                                    p class="muted" { "Runner job: " code { (job.run.runner_job_name) } }
                                }
                                div class="badge-row" {
                                    (badge(&display_status(&job.run.status), status_tone(&job.run.status)))
                                }
                            }
                            div class="meta-grid" {
                                div class="meta-pair" {
                                    span { "Previous jobs" }
                                    strong {
                                        @if job.previous_jobs.is_empty() {
                                            span class="muted" { "None" }
                                        } @else {
                                            @for previous in &job.previous_jobs {
                                                span class="chip" {
                                                    "job-" (previous.job_index + 1) " / "
                                                    (previous.runner_job_name) " ("
                                                    (display_status(&previous.status)) ")"
                                                }
                                            }
                                        }
                                    }
                                }
                                div class="meta-pair" { span { "Failure category" } strong { (render_optional(job.run.failure_category.as_deref())) } }
                                div class="meta-pair" { span { "Terminal reason" } strong { (render_optional(job.run.terminal_reason.as_deref())) } }
                                div class="meta-pair" { span { "Exit code" } strong { (render_optional(job.run.exit_code.map(|value| value.to_string()).as_deref())) } }
                                div class="meta-pair" { span { "Duration ms" } strong { (render_optional(job.run.duration_ms.map(|value| value.to_string()).as_deref())) } }
                                div class="meta-pair" { span { "Cancel reason" } strong { (job.run.cancel_reason.clone().unwrap_or_default()) } }
                                div class="meta-pair" { span { "Cancel requested" } strong { (job.run.cancel_requested_at.clone().unwrap_or_default()) } }
                                div class="meta-pair" { span { "Cancel started" } strong { (job.run.cancel_started_at.clone().unwrap_or_default()) } }
                                div class="meta-pair" { span { "Cancel retries" } strong { (job.run.cancel_retry_count) } }
                                div class="meta-pair" { span { "Last cancel retry" } strong { (job.run.last_cancel_retry_at.clone().unwrap_or_default()) } }
                                div class="meta-pair" { span { "Infra retries" } strong { (job.run.infra_retry_count) } }
                                div class="meta-pair" { span { "Last infra retry" } strong { (job.run.last_infra_retry_at.clone().unwrap_or_default()) } }
                                div class="meta-pair" {
                                    span { "Stdout" }
                                    strong { (job.run.output_metadata.stdout.bytes) "B · truncated=" (job.run.output_metadata.stdout.truncated) }
                                }
                                div class="meta-pair" {
                                    span { "Stderr" }
                                    strong { (job.run.output_metadata.stderr.bytes) "B · truncated=" (job.run.output_metadata.stderr.truncated) }
                                }
                                div class="meta-pair" {
                                    span { "Artifacts" }
                                    strong { (job.run.output_metadata.artifacts.count) " files · " (job.run.output_metadata.artifacts.bytes) "B" }
                                }
                            }
                            div class="log-grid" {
                                div class="log-panel" { span class="subsection-title" { "Resolved inputs" } pre { (resolved_inputs_json) } }
                                div class="log-panel" { span class="subsection-title" { "Stdout" } pre { (job.stdout) } }
                                div class="log-panel" { span class="subsection-title" { "Stderr" } pre { (job.stderr) } }
                            }
                        }
                    }
                }
            }
            script src="/assets/pipeline_events.js" data-pipeline-id=(pipeline.id) defer {}
        },
    )
}
