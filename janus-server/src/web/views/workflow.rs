use maud::{Markup, PreEscaped, html};

use crate::models::{Repo, Workflow, WorkflowTrigger};

use super::components::{badge, csrf_input, form_error, layout, page_intro, x_mark};
use crate::schema_diff::{WorkflowSchemaDiff, WorkflowSchemaReport};

pub(crate) struct WorkflowCard {
    pub workflow: Workflow,
    pub repo: Repo,
    pub schema_report: WorkflowSchemaReport,
    pub trigger: WorkflowTrigger,
    pub job_count: usize,
    pub manual_artifacts: Vec<ManualArtifactField>,
}

#[derive(Clone)]
pub(crate) struct ManualArtifactField {
    pub field_name: String,
    pub label: String,
    pub source_label: String,
    pub options: Vec<ManualArtifactOption>,
}

#[derive(Clone)]
pub(crate) struct ManualArtifactOption {
    pub value: String,
    pub label: String,
}

pub(crate) struct WorkflowFormView {
    pub name: String,
    pub trigger_kind: String,
    pub branch_name: String,
    pub jobs_json: String,
    pub repo_field: Markup,
    pub runner_catalog_json: PreEscaped<String>,
    pub initial_jobs_json: PreEscaped<String>,
    pub is_edit: bool,
    pub error: Option<String>,
}

pub(crate) fn workflows_page(
    form: WorkflowFormView,
    workflows: Vec<WorkflowCard>,
    csrf: &str,
) -> Markup {
    layout(
        "Workflows",
        html! {
            (page_intro(
                "Workflows",
                "Define reusable CI pipelines with structured jobs, serial execution order, and runner bindings.",
            ))
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Automation" }
                        h2 { "Create workflow" }
                    }
                }
                form method="post" action="/workflows" class="stack-lg" {
                    (csrf_input(csrf))
                    (workflow_form_fields(form))
                }
            }
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Catalog" }
                        h2 { "Existing workflows" }
                    }
                }
                div class="card-grid" {
                    @for card in &workflows {
                        article class="entity-card" {
                            div class="entity-head" {
                                div {
                                    h3 {
                                        a href=(format!("/workflows/{}", card.workflow.id)) { (card.workflow.name) }
                                    }
                                }
                                div class="badge-row" {
                                    (badge(card.schema_report.status.as_str(), card.schema_report.status.tone()))
                                    (badge(&format!("v{}", card.workflow.version), "neutral"))
                                }
                            }
                            div class="meta-grid" {
                                div class="meta-pair" { span { "Repo" } strong { (card.repo.name) } }
                                div class="meta-pair" { span { "Branches" } strong { (card.trigger.branches.join(", ")) } }
                                div class="meta-pair" { span { "Trigger" } strong { (card.trigger.kind) } }
                                div class="meta-pair" { span { "Jobs" } strong { (card.job_count) } }
                            }
                            @if card.trigger.kind == "manual" {
                                (manual_run_form(
                                    &format!("/workflows/{}/run", card.workflow.id),
                                    csrf,
                                    manual_default_branch(&card.trigger, &card.repo),
                                    &card.manual_artifacts,
                                ))
                            }
                            (schema_diff_summary(&card.schema_report.diff))
                        }
                    }
                }
            }
        },
    )
}

pub(crate) fn workflow_detail_page(
    workflow: &Workflow,
    repo: &Repo,
    schema_report: WorkflowSchemaReport,
    form: WorkflowFormView,
    csrf: &str,
    manual_artifacts: Vec<ManualArtifactField>,
) -> Markup {
    let is_manual = form.trigger_kind == "manual";
    let default_branch = if form.branch_name.trim().is_empty() {
        repo.default_branch.clone()
    } else {
        form.branch_name.clone()
    };
    layout(
        "Workflow",
        html! {
            (page_intro(
                "Workflow Detail",
                "Adjust trigger behavior, job order, and runner/job bindings.",
            ))
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Editing" }
                        h2 { (workflow.name) }
                        p class="muted" { "Repository: " (repo.name) }
                    }
                    div class="badge-row" {
                        (badge(schema_report.status.as_str(), schema_report.status.tone()))
                    }
                }
                (schema_diff_details(&schema_report.diff))
                form method="post" action=(format!("/workflows/{}/update", workflow.id)) class="stack-lg" {
                    (csrf_input(csrf))
                    (workflow_form_fields(form))
                }
            }
            @if is_manual {
                section class="card" {
                    div class="section-head" {
                        div {
                            div class="eyebrow" { "Manual run" }
                            h2 { "Run workflow" }
                        }
                    }
                    (manual_run_form(
                        &format!("/workflows/{}/run", workflow.id),
                        csrf,
                        default_branch,
                        &manual_artifacts,
                    ))
                }
            }
        },
    )
}

fn schema_diff_summary(diff: &[WorkflowSchemaDiff]) -> Markup {
    html! {
        @if !diff.is_empty() {
            div class="schema-diff-summary" {
                @for item in diff.iter().take(3) {
                    p class="muted" { (item.message) }
                }
                @if diff.len() > 3 {
                    p class="muted" { (diff.len() - 3) " more schema changes" }
                }
            }
        }
    }
}

fn schema_diff_details(diff: &[WorkflowSchemaDiff]) -> Markup {
    html! {
        @if !diff.is_empty() {
            div class="callout" {
                div class="section-head compact" {
                    div {
                        div class="eyebrow" { "Schema diff" }
                        h3 { "Runner schema changes" }
                    }
                }
                ul class="schema-diff-list" {
                    @for item in diff {
                        li {
                            strong {
                                @if item.incompatible { "Blocking" } @else { "Notice" }
                            }
                            span { (item.message) }
                        }
                    }
                }
            }
        }
    }
}

pub(crate) fn repo_selector(repos: &[Repo]) -> Markup {
    html! {
        select name="repo_id" required {
            @for repo in repos {
                option value=(repo.id) { (repo.name) }
            }
        }
    }
}

pub(crate) fn fixed_repo_field(workflow: &Workflow, repo: &Repo) -> Markup {
    html! {
        input type="hidden" name="repo_id" value=(workflow.repo_id);
        input value=(repo.name) disabled;
    }
}

pub(crate) fn script_json(input: &str) -> PreEscaped<String> {
    PreEscaped(input.replace("</script", "<\\/script"))
}

fn workflow_form_fields(form: WorkflowFormView) -> Markup {
    html! {
        div class="form-grid form-grid-2" {
            label {
                span { "Workflow name" }
                input name="name" value=(form.name) maxlength="120" required data-validate data-trim-required="true";
            }
            label {
                span { "Trigger" }
                select name="trigger_kind" required {
                    option value="push" selected[form.trigger_kind == "push"] { "push" }
                    option value="manual" selected[form.trigger_kind == "manual"] { "manual" }
                }
            }
        }
        div class="form-grid form-grid-2" {
            label {
                span { "Repository" }
                (form.repo_field)
            }
            label {
                span { "Branch" }
                input name="branch_name" value=(form.branch_name) maxlength="255" data-validate data-no-whitespace="true" data-single-branch="true";
            }
        }
        (form_error(form.error.as_deref()))
        div class="card soft-card" {
            div class="section-head" {
                div {
                    div class="eyebrow" { "Jobs" }
                    h3 { "Workflow builder" }
                    p class="muted" {
                        "Jobs run one by one in the order shown below. Inputs are rendered from the selected runner job manifest."
                    }
                }
            }
            div id="workflow-builder" class="stack-md" {
                template id="workflow-job-row-template" {
                    fieldset class="job-builder-row" data-workflow-job-row="true" {
                        label {
                            span { "Runner" }
                            select data-field="runner_id" {}
                        }
                        label {
                            span { "Job" }
                            select data-field="runner_job_name" {}
                        }
                        label {
                            span { "Outcome" }
                            select data-field="outcome_policy" {}
                        }
                        label {
                            span { "Inputs" }
                            button type="button" class="input-summary-trigger ghost" data-input-summary="true" {}
                        }
                        label {
                            span { "Outputs" }
                            button type="button" class="input-summary-trigger ghost" data-output-summary="true" {}
                        }
                        dialog class="inputs-dialog" data-inputs-dialog="true" {
                            div class="dialog-card" {
                                div class="section-head" {
                                    div {
                                        div class="eyebrow" { "Inputs" }
                                        h3 { "Configure job inputs" }
                                        p class="muted" {
                                            "Bindings are saved back into the workflow when you submit the form."
                                        }
                                    }
                                    button type="button" class="ghost" data-dialog-close="inputs" { "Close" }
                                }
                                div class="inputs-wrap" data-inputs-wrap="true" {}
                                div class="actions" {
                                    button type="button" data-dialog-close="inputs" { "Done" }
                                }
                            }
                        }
                        dialog class="inputs-dialog" data-outputs-dialog="true" {
                            div class="dialog-card" {
                                div class="section-head" {
                                    div {
                                        div class="eyebrow" { "Outputs" }
                                        h3 { "Declared job outputs" }
                                        p class="muted" {
                                            "Runner jobs can expose artifact or typed outputs, which downstream jobs can consume."
                                        }
                                    }
                                    button type="button" class="ghost" data-dialog-close="outputs" { "Close" }
                                }
                                div class="inputs-wrap" data-outputs-wrap="true" {}
                                div class="actions" {
                                    button type="button" data-dialog-close="outputs" { "Done" }
                                }
                            }
                        }
                        div class="job-row-remove" {
                            button type="button" class="job-remove-button ghost" data-remove-job="true" aria-label="Remove job" title="Remove job" { (x_mark()) }
                        }
                    }
                }
                template id="workflow-inputs-empty-template" {
                    div class="muted" { "None" }
                }
                template id="workflow-outputs-empty-template" {
                    div class="muted" { "None" }
                }
                template id="workflow-inputs-table-template" {
                    table class="inputs-table" {
                        thead {
                            tr {
                                th { "Input" }
                                th { "Type" }
                                th { "Binding" }
                                th { "Value" }
                            }
                        }
                        tbody data-table-body="true" {}
                    }
                }
                template id="workflow-outputs-table-template" {
                    table class="inputs-table" {
                        thead {
                            tr {
                                th { "Output" }
                                th { "Type" }
                                th { "Required" }
                            }
                        }
                        tbody data-table-body="true" {}
                    }
                }
                div id="workflow-job-list" class="stack-md" {}
                div id="workflow-validation-errors" class="form-error" tabindex="-1" hidden {}
                div class="actions" {
                    button type="button" id="workflow-add-job" class="secondary" { "Add job" }
                }
            }
        }
        textarea id="workflow-jobs-json" name="jobs_json" hidden {
            (form.jobs_json)
        }
        script type="application/json" id="workflow-runner-catalog" {
            (form.runner_catalog_json)
        }
        script type="application/json" id="workflow-initial-jobs" {
            (form.initial_jobs_json)
        }
        script type="module" src="/assets/workflow_builder.js" {}
        div class="actions" {
            button type="submit" {
                @if form.is_edit { "Save workflow" } @else { "Create workflow" }
            }
        }
    }
}

fn manual_default_branch(trigger: &WorkflowTrigger, repo: &Repo) -> String {
    trigger
        .branches
        .first()
        .cloned()
        .unwrap_or_else(|| repo.default_branch.clone())
}

fn manual_run_form(
    action: &str,
    csrf: &str,
    default_branch: String,
    artifact_fields: &[ManualArtifactField],
) -> Markup {
    let unavailable = artifact_fields.iter().any(|field| field.options.is_empty());
    html! {
        form method="post" action=(action) class="stack-md inset-panel" {
            (csrf_input(csrf))
            div class="inline-fields" {
                label {
                    span { "Branch ref" }
                    input name="branch" value=(default_branch);
                }
                label {
                    span { "Commit" }
                    input name="commit" value="HEAD";
                }
            }
            @for field in artifact_fields {
                label {
                    span { (field.label) }
                    select name=(field.field_name) required {
                        option value="" selected disabled { "Select an artifact" }
                        @for option in &field.options {
                            option value=(option.value) { (option.label) }
                        }
                    }
                    small class="muted" {
                        @if field.options.is_empty() {
                            "No successful artifacts are available from " (field.source_label) "."
                        } @else {
                            "Showing the 10 newest successful artifacts from " (field.source_label) "."
                        }
                    }
                }
            }
            div class="actions" {
                button type="submit" disabled[unavailable] { "Run workflow" }
            }
        }
    }
}
