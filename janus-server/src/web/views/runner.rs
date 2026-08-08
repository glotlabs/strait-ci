use maud::{Markup, html};

use crate::models::{Runner, RunnerJobDefinition};

use super::components::{badge, csrf_input, form_error, layout, page_intro, runner_state_tone};

#[derive(Default)]
pub(crate) struct RunnerFormView {
    pub name: String,
    pub base_url: String,
}

pub(crate) struct RunnerAuthView {
    pub key_id: String,
    pub public_key: String,
}

pub(crate) struct RunnerEditFormView {
    pub name: String,
    pub base_url: String,
}

impl RunnerEditFormView {
    pub(crate) fn from_runner(runner: &Runner) -> Self {
        Self {
            name: runner.name.clone(),
            base_url: runner.base_url.clone(),
        }
    }
}

pub(crate) fn runners_page(
    runners: Vec<Runner>,
    auth: RunnerAuthView,
    csrf: &str,
    error: Option<&str>,
    form: RunnerFormView,
) -> Markup {
    layout(
        "Runners",
        html! {
            (page_intro(
                "Runners",
                "Register execution backends, refresh their advertised jobs, and control availability.",
            ))
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Execution" }
                        h2 { "Add runner" }
                    }
                }
                form method="post" action="/runners" class="stack-lg" {
                    (csrf_input(csrf))
                    (form_error(error))
                    div class="form-grid form-grid-2" {
                        label { span { "Name" } input name="name" value=(form.name) maxlength="120" required data-validate data-trim-required="true"; }
                        label {
                            span { "Base URL" }
                            input name="base_url" type="url" value=(form.base_url) placeholder="http://127.0.0.1:8080" maxlength="2048" required data-validate data-trim-required="true";
                        }
                    }
                    div class="actions" {
                        button type="submit" { "Add runner" }
                    }
                }
            }
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Server identity" }
                        h2 { "Runner trust key" }
                    }
                }
                div class="meta-pair" {
                    span { "Key ID" }
                    code { (auth.key_id) }
                }
                div class="meta-pair" {
                    span { "Public key" }
                    code { (auth.public_key) }
                }
            }
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Fleet" }
                        h2 { "Connected runners" }
                    }
                }
                div class="card-grid" {
                    @for runner in &runners {
                        article class="entity-card" {
                            div class="entity-head" {
                                div {
                                    h3 {
                                        a href=(format!("/runners/{}", runner.id)) { (runner.name) }
                                    }
                                    p class="muted" { (runner.base_url) }
                                }
                                div class="badge-row" {
                                    (badge(&runner.last_health_state, runner_state_tone(&runner.last_health_state)))
                                    (badge(
                                        if runner.enabled { "enabled" } else { "disabled" },
                                        if runner.enabled { "success" } else { "danger" },
                                    ))
                                }
                            }
                            div class="meta-pair" {
                                span { "Runner ID" }
                                code { (runner.id) }
                            }
                        }
                    }
                }
            }
        },
    )
}

pub(crate) fn runner_detail_page(
    runner: &Runner,
    jobs: &[RunnerJobDefinition],
    csrf: &str,
    error: Option<&str>,
    form: RunnerEditFormView,
) -> Markup {
    layout(
        "Runner",
        html! {
            (page_intro(
                "Runner Detail",
                "Edit connection settings, control availability, and inspect advertised jobs.",
            ))
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Execution backend" }
                        h2 { (runner.name) }
                        p class="muted" { (runner.base_url) }
                    }
                    div class="badge-row" {
                        (badge(&runner.last_health_state, runner_state_tone(&runner.last_health_state)))
                        (badge(
                            if runner.enabled { "enabled" } else { "disabled" },
                            if runner.enabled { "success" } else { "danger" },
                        ))
                    }
                }
                div class="meta-grid" {
                    div class="meta-pair" { span { "Runner ID" } code { (runner.id) } }
                    div class="meta-pair" {
                        span { "Last seen" }
                        strong { (runner.last_seen_at.as_deref().unwrap_or("Never")) }
                    }
                    div class="meta-pair" { span { "Created" } strong { (runner.created_at) } }
                }
            }
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Configuration" }
                        h2 { "Edit runner" }
                    }
                }
                form method="post" action=(format!("/runners/{}/update", runner.id)) class="stack-lg" {
                    (csrf_input(csrf))
                    (form_error(error))
                    div class="form-grid form-grid-2" {
                        label {
                            span { "Name" }
                            input name="name" value=(form.name) maxlength="120" required data-validate data-trim-required="true";
                        }
                        label {
                            span { "Base URL" }
                            input name="base_url" type="url" value=(form.base_url) maxlength="2048" required data-validate data-trim-required="true";
                        }
                    }
                    div class="actions" {
                        button type="submit" { "Save changes" }
                        a href="/runners" { "Back to runners" }
                    }
                }
            }
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Operations" }
                        h2 { "Runner controls" }
                    }
                }
                div class="actions" {
                    form method="post" action=(format!("/runners/{}/test", runner.id)) {
                        (csrf_input(csrf))
                        button type="submit" class="secondary" { "Refresh jobs" }
                    }
                    form method="post" action=(format!("/runners/{}/toggle", runner.id)) {
                        (csrf_input(csrf))
                        button type="submit" class="ghost" {
                            @if runner.enabled { "Disable runner" } @else { "Enable runner" }
                        }
                    }
                }
            }
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Capabilities" }
                        h2 { "Advertised jobs" }
                    }
                    (badge(&jobs.len().to_string(), "neutral"))
                }
                @if jobs.is_empty() {
                    p class="muted" { "No jobs have been advertised. Refresh the runner to fetch its current job catalog." }
                } @else {
                    div class="card-grid" {
                        @for job in jobs {
                            article class="job-card soft-card" {
                                div class="entity-head" {
                                    div {
                                        h3 { (job.name) }
                                        p class="muted" { (job.concurrency.as_str()) }
                                    }
                                    @if job.timeout_seconds == 0 {
                                        (badge("no timeout", "neutral"))
                                    } @else {
                                        (badge(&format!("{}s timeout", job.timeout_seconds), "neutral"))
                                    }
                                }
                                div class="meta-grid" {
                                    div class="meta-pair" { span { "Inputs" } strong { (job.inputs.len()) } }
                                    div class="meta-pair" { span { "Outputs" } strong { (job.outputs.len()) } }
                                }
                            }
                        }
                    }
                }
            }
        },
    )
}
