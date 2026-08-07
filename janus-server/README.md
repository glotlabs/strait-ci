# janus-server

`janus-server` is the orchestration layer for managed repositories, workflows, and pipeline runs.

Current implementation:

- SQLite-backed user, session, repo, runner, workflow, push-event, pipeline, and job-run storage
- Server-rendered admin UI for login, users, repos, runners, workflows, and pipeline detail
- Bare repository provisioning with deterministic `post-receive` hook installation
- CLI hook ingestion via `janus-server hook post-receive --repo-id <repo_id>`
- Background scheduler that turns push events into pipeline runs and dispatches runnable jobs to `janus-runner`
- Runner health refresh and cached runner-job metadata
- Signed session cookies and local username/password auth

Commands:

```bash
cargo run -p janus-server -- serve
cargo run -p janus-server -- admin bootstrap-admin --username admin --config server.toml
printf '%s\n' "$ADMIN_PASSWORD" | cargo run -p janus-server -- admin bootstrap-admin --username admin --password-stdin --config server.toml
cargo run -p janus-server -- admin seed-user --username alice --password secret --role admin
cargo run -p janus-server -- admin reconcile-hooks
cargo run -p janus-server -- admin runner-key init --config server.toml
cargo run -p janus-server -- admin runner-key show --config server.toml
cargo run -p janus-server -- admin runner-key show --format toml --config server.toml
cargo run -p janus-server -- admin runner-key rotate --config server.toml
```

`admin bootstrap-admin` only works before any users exist. Without `--password-stdin`, it prompts twice with terminal echo disabled.

Initialize runner signing keys once before first `serve`; this generates `[runner_auth]`, creates the Ed25519 keypair, and writes the generated key ID into the server config.

Runner signing key rotation generates a new key ID, writes a new Ed25519 keypair, updates `[runner_auth]` in the server config, and leaves existing public key files in place for rollout. Add the new public key to every runner as another `[[auth.servers]]` entry before restarting the server with the new key; remove the old runner entry after old in-flight requests have drained.

JavaScript asset tests use Node's built-in test runner and do not require a package manager:

```bash
./test_js.sh
```

Configuration:

- Start from [`server.example.toml`](/Users/petter/dev/Projects/janus/janus-server/server.example.toml)
- Default config path is `server.toml`
- `hook post-receive` also accepts `--config <path>`

Workflow JSON shape:

```json
{
  "jobs": [
    {
      "runner_id": "runner-uuid",
      "runner_job_name": "build-app",
      "inputs": {
        "commit": { "kind": "commit" },
        "branch": { "kind": "branch" },
        "source": { "kind": "source_artifact" }
      },
      "outcome_policy": "required"
    }
  ]
}
```

Supported input bindings:

- `commit` and `branch` values from the pipeline trigger
- a generated source archive
- an artifact or typed value output from an earlier job in the same pipeline
- a promoted artifact selected when a manual workflow is started

## Artifact promotion

A manual deployment workflow can consume an immutable artifact produced by a
different, successful pipeline in the same repository. In the workflow editor,
set the deployment job's artifact input to **Artifact selected at run time** and
choose the source runner, job, and output (for example,
`glot-build / build-app / app`).

The manual Run form lists the ten newest matching artifacts. Each option shows
its commit, source workflow, creation time, size, and digest. Janus validates the
selection server-side, records it with the new pipeline, and uploads the
server-mirrored bytes to the deployment runner. Production reruns retain the
exact original artifact selection rather than resolving the newest artifact
again.

Only artifacts from successful jobs in successful pipelines for the same
repository are eligible. Runtime-selected artifact bindings are accepted only
on manual workflows.
