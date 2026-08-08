use maud::{DOCTYPE, Markup, html};

pub(super) fn layout(title: &str, body: Markup) -> Markup {
    html! {
        (DOCTYPE)
        html {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { (title) }
                link rel="stylesheet" href="/assets/app.css";
                script type="module" src="/assets/form_validation.js" {}
            }
            body {
                div class="app-shell" {
                    nav class="topbar" {
                        a class="brand" href="/repos" {
                            span class="brand-mark" { "S" }
                            span { "Janus" }
                        }
                        div class="nav-links" {
                            a href="/repos" { "Repos" }
                            a href="/runners" { "Runners" }
                            a href="/workflows" { "Workflows" }
                            a href="/pipelines" { "Pipelines" }
                            a href="/artifacts" { "Artifacts" }
                            a href="/users" { "Users" }
                        }
                        form method="post" action="/logout" {
                            button type="submit" class="ghost" { "Logout" }
                        }
                    }
                    main class="page-shell" { (body) }
                }
            }
        }
    }
}

pub(super) fn layout_public(title: &str, body: Markup) -> Markup {
    html! {
        (DOCTYPE)
        html {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { (title) }
                link rel="stylesheet" href="/assets/app.css";
                script type="module" src="/assets/form_validation.js" {}
            }
            body {
                main class="public-shell" { (body) }
            }
        }
    }
}

pub(super) fn csrf_input(token: &str) -> Markup {
    html! {
        input type="hidden" name="csrf_token" value=(token);
    }
}

pub(super) fn form_error(error: Option<&str>) -> Markup {
    html! {
        @if let Some(message) = error {
            div class="form-error" role="alert" { (message) }
        }
    }
}

pub(super) fn page_intro(title: &str, subtitle: &str) -> Markup {
    html! {
        section class="hero-card" {
            div class="eyebrow" { "Janus" }
            h1 { (title) }
            p class="muted" { (subtitle) }
        }
    }
}

pub(super) fn badge(label: &str, tone: &str) -> Markup {
    html! {
        span class=(format!("badge badge-{tone}")) { (label) }
    }
}

pub(super) fn x_mark() -> Markup {
    html! {
        svg class="size-6" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" {
            path stroke-linecap="round" stroke-linejoin="round" d="M6 18 18 6M6 6l12 12" {}
        }
    }
}

pub(super) fn display_status(status: &str) -> String {
    match status {
        "cancel_requested" => "cancel requested".to_string(),
        "canceling" => "stopping".to_string(),
        "failed" => "failed".to_string(),
        _ => status.to_string(),
    }
}

pub(super) fn render_optional(value: Option<&str>) -> String {
    value.unwrap_or_default().to_string()
}

pub(super) fn status_tone(status: &str) -> &'static str {
    match status {
        "success" => "success",
        "running" | "pending" => "warning",
        "failed" | "canceled" => "danger",
        "cancel_requested" | "canceling" => "neutral",
        _ => "neutral",
    }
}

pub(super) fn runner_state_tone(state: &str) -> &'static str {
    match state {
        "healthy" => "success",
        "unknown" | "incompatible" => "warning",
        _ => "danger",
    }
}
