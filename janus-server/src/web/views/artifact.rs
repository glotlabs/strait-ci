use maud::{Markup, html};

use crate::models::ProducedArtifact;

use super::components::{layout, page_intro};

pub(crate) fn artifacts_page(artifacts: &[ProducedArtifact]) -> Markup {
    layout(
        "Artifacts",
        html! {
            (page_intro(
                "Artifacts",
                "Browse and download the latest artifacts produced by workflow jobs.",
            ))
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Outputs" }
                        h2 { "Latest artifacts" }
                    }
                    span class="muted" { (artifacts.len()) " shown" }
                }
                @if artifacts.is_empty() {
                    div class="empty-state" {
                        h3 { "No artifacts yet" }
                        p class="muted" { "Artifacts produced by completed jobs will appear here." }
                    }
                } @else {
                    div class="table-wrap" {
                        table {
                            thead {
                                tr {
                                    th { "Artifact" }
                                    th { "Source" }
                                    th { "Created" }
                                    th { "Size" }
                                    th { "Digest" }
                                    th { "" }
                                }
                            }
                            tbody {
                                @for artifact in artifacts {
                                    tr {
                                        td {
                                            strong { (artifact.artifact_name) }
                                            div class="muted artifact-detail" { (artifact.repo_name) }
                                        }
                                        td {
                                            a href=(format!("/pipelines/{}", artifact.pipeline_run_id)) {
                                                (artifact.workflow_name) " / " (artifact.runner_job_name)
                                            }
                                            div class="muted artifact-detail" {
                                                (artifact.trigger_ref.as_deref().unwrap_or(""))
                                            }
                                        }
                                        td { time datetime=(artifact.created_at) { (artifact.created_at) } }
                                        td class="nowrap" { (format_bytes(artifact.size_bytes)) }
                                        td { code class="artifact-digest" title=(artifact.sha256) { (short_digest(&artifact.sha256)) } }
                                        td {
                                            a class="download-link" href=(format!("/artifacts/{}", artifact.id)) {
                                                "Download"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
    )
}

fn short_digest(digest: &str) -> &str {
    digest.get(..12).unwrap_or(digest)
}

fn format_bytes(bytes: i64) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut value = bytes.max(0) as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", bytes.max(0), UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_sizes_are_human_readable() {
        assert_eq!(format_bytes(42), "42 B");
        assert_eq!(format_bytes(1536), "1.5 KiB");
    }

    #[test]
    fn short_digest_handles_unexpected_values() {
        assert_eq!(short_digest("1234567890abcdef"), "1234567890ab");
        assert_eq!(short_digest("short"), "short");
    }
}
