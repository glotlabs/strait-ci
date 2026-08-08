# janus-runner

`janus-runner` is a small HTTP service for running predefined jobs on a host.

It is designed around a simple model:

- Job definitions live on disk as TOML manifests.
- Clients upload artifacts the jobs need.
- Clients start a named job by POSTing JSON inputs.
- The runner executes a local script with a tightly controlled environment.
- The script can emit declared output files, which the runner stores as artifacts.
- Clients poll for status and read logs over HTTP.

This makes it useful as a lightweight execution backend for CI orchestration, build workers, release pipelines, or other systems that need remote job execution without exposing arbitrary shell access.

## What The Runner Does

- Loads job manifests from a configured directory at startup.
- Validates manifests before serving traffic.
- Authenticates every protected route with signed server requests and explicit permissions.
- Stores uploaded artifacts on disk with checksum validation and TTL-based expiry.
- Runs job scripts in isolated per-run working directories.
- Passes runtime metadata and declared inputs to scripts through environment variables.
- Persists job metadata, logs, and output artifact references on disk.
- Recovers interrupted jobs and incomplete artifacts on startup.
- Cleans up expired artifacts in a background loop.

## Repository Layout

This repository currently contains one Rust service:

- [`janus-runner/`](/Users/petter/dev/Projects/janus/janus-runner) - the runner binary and its source code

Important files:

- [`janus-runner/Cargo.toml`](/Users/petter/dev/Projects/janus/janus-runner/Cargo.toml) - Rust package definition
- [`janus-runner/runner.example.toml`](/Users/petter/dev/Projects/janus/janus-runner/runner.example.toml) - example runner config
- [`janus-runner/manifests/build-app.example.toml`](/Users/petter/dev/Projects/janus/janus-runner/manifests/build-app.example.toml) - example job manifest
- [`janus-runner/manifests/build-freebsd-packages.example.toml`](/Users/petter/dev/Projects/janus/janus-runner/manifests/build-freebsd-packages.example.toml) - FreeBSD package build manifest

## Quickstart

### 1. Requirements

- Rust toolchain with `cargo`
- A Unix-like environment is the intended target for job scripts
- A directory for manifests
- A directory for runtime data

### 2. Build

```bash
cd janus-runner
cargo build
```

### 3. Create a config

Start from the example:

```bash
cp runner.example.toml runner.toml
```

Example config:

```toml
data_dir = "/var/lib/janus-runner"
manifests_dir = "/etc/janus-runner/jobs"

[server]
listen = "127.0.0.1:8080"

[auth]
mode = "signed"

[[auth.servers]]
name = "main-janus"
key_id = "main-janus-2026-05"
public_key = "paste-server-public-key-base64-here"
permissions = [
  "artifacts:write",
  "artifacts:read",
  "jobs:run",
  "jobs:read",
  "logs:read",
]

[artifacts]
max_size_mb = 500
ttl_seconds = 86400
cleanup_interval_seconds = 600
require_checksum_on_upload = true
max_upload_requests_per_minute = 60

[jobs]
default_log_limit_mb = 50
max_request_body_kb = 64
max_run_requests_per_minute = 60
cleanup_successful_workdirs = true
keep_failed_workdirs = true
```

### 4. Trust the server public key

Start `janus-server` once and copy its runner trust key from the Runners page, or from the configured `runner_auth.public_key_path`, into `[[auth.servers]]`.

### 5. Add a manifest

Put a manifest file in `manifests_dir`. Example:

```toml
name = "build-app"
script = "/opt/janus-runner/jobs/build-app.sh"
timeout_seconds = 600
concurrency = "job_exclusive"

[inputs.commit]
type = "string"
required = true

[inputs.branch]
type = "string"
required = true

[inputs.source]
type = "artifact"
required = true

[outputs.app]
type = "artifact"
path = "app.tar.gz"
required = true
```

### 6. Write the script

Example script:

```sh
#!/bin/sh
set -eu

mkdir -p "$JANUS_WORKDIR/src"
cp "$INPUT_SOURCE" "$JANUS_WORKDIR/src/source.tar.gz"

printf 'building commit %s on branch %s\n' "$INPUT_COMMIT" "$INPUT_BRANCH"

# Produce the declared output inside JANUS_OUTPUT_DIR.
printf 'bundle' > "$JANUS_OUTPUT_DIR/app.tar.gz"
```

Make the script executable:

```bash
chmod +x /opt/janus-runner/jobs/build-app.sh
```

### FreeBSD package build job

`build-freebsd-packages.example.toml` accepts the generated repository source
archive as its `source` input. On a FreeBSD runner with Rust and `pkg` installed,
the job builds both workspace binaries, creates their native packages, and
registers `janus-server.pkg` and `janus-runner.pkg` as output artifacts.

Install the sample manifest and script from the `janus-runner` package with:

```sh
cp /opt/janus-runner/share/examples/janus-runner/build-freebsd-packages.toml.sample \
  /opt/janus-runner/manifests/build-freebsd-packages.toml
cp /opt/janus-runner/share/examples/janus-runner/build-freebsd-packages.sh.sample \
  /opt/janus-runner/jobs/build-freebsd-packages.sh
chmod 0755 /opt/janus-runner/jobs/build-freebsd-packages.sh
```

### 7. Run the server

```bash
cd janus-runner
cargo run -- runner.toml
```

The config path can also be provided through `JANUS_RUNNER_CONFIG`.

## Configuration

The runner loads a single TOML config file with these top-level sections:

- `data_dir`: root directory for persisted artifacts, jobs, logs, and metadata
- `manifests_dir`: directory containing job manifest `.toml` files
- `[server]`: HTTP bind address
- `[auth]`: signed request auth and trusted server public keys
- `[artifacts]`: artifact retention, size limits, checksum policy, and upload rate limits
- `[jobs]`: job log limits, body size limits, job rate limits, and workdir cleanup behavior

### Auth

Only signed request auth is supported.

Each trusted server has:

- `name`: human-readable label used in logs
- `key_id`: server signing key id
- `public_key`: base64-encoded Ed25519 public key
- `permissions`: a list of allowed capabilities

During server key rotation, keep both the old and new public keys configured as separate `[[auth.servers]]` entries. Remove the old entry only after the server is signing with the new key everywhere and old in-flight requests have drained.

Supported permissions:

- `artifacts:write`
- `artifacts:read`
- `jobs:run`
- `jobs:read`
- `logs:read`

## Job Manifests

Each job is defined by a TOML manifest.

Required fields:

- `name`
- `script`
- `timeout_seconds`
- `concurrency`

Optional sections:

- `[inputs.<name>]`
- `[outputs.<name>]`

### Concurrency modes

Supported values:

- `parallel`
- `job_exclusive`
- `global_exclusive`

Behavior:

- `parallel`: can run unless a `global_exclusive` job is active
- `job_exclusive`: only one instance of that job can run at a time, and it also blocks on a running `global_exclusive` job
- `global_exclusive`: can only run when no other jobs are active

### Input types

Supported input types:

- `string`
- `integer`
- `boolean`
- `artifact`
- `json`

Input fields:

- `type`
- `required`
- `sensitive` (optional, default `false`)
- `max_length` for `string`
- `pattern` for `string`
- `max_json_bytes` for `json`

Notes:

- Input names must be safe identifiers and cannot use reserved runtime names.
- `artifact` inputs must be artifact IDs returned by the upload API.
- `json` inputs can be structured JSON objects or arrays, but not `null`.
- Sensitive inputs are available to the script but redacted in persisted metadata.

### Outputs

Each output declares:

- `type`: one of `artifact`, `string`, `integer`, `boolean`, `json`
- `path`: relative path inside the job output directory
- `required`: whether the file must exist for the job to remain successful

Output behavior:

- `artifact`: the file is uploaded and exposed as an artifact reference
- `string`: the file is read as UTF-8 text
- `integer`: the file must contain a valid signed integer
- `boolean`: the file must contain `true` or `false`
- `json`: the file must contain valid non-`null` JSON

Output paths must be relative and must not escape the output directory.

## Runtime Environment For Scripts

The runner executes each script with a minimal environment.

### Runner-provided context

- `JANUS_JOB_ID`
- `JANUS_JOB_NAME`
- `JANUS_WORKDIR`
- `JANUS_OUTPUT_DIR`
- `JANUS_METADATA_PATH`
- `PATH`

`PATH` preserves the entries configured for the `janus-runner` service, then
appends any missing standard system paths used by jobs.

### Declared inputs

For each manifest input, the runner exports:

- `INPUT_<NAME>`

Examples:

- `INPUT_COMMIT`
- `INPUT_BRANCH`
- `INPUT_SOURCE`

Name normalization:

- Input names are uppercased
- `-` becomes `_`

Artifact input values are resolved to local blob paths before they are exposed to the script. That means `INPUT_SOURCE` points to a file on disk, not the original artifact ID string.

## HTTP API

All protected endpoints use:

```http
X-Janus-Key-Id: <key-id>
X-Janus-Timestamp: <rfc3339 timestamp>
X-Janus-Nonce: <unique nonce>
X-Janus-Content-SHA256: <hex sha256 of body>
X-Janus-Signature: ed25519:<base64 signature>
```

### Health

`GET /health`

Returns basic liveness information.

Example response:

```json
{
  "status": "ok",
  "listen": "127.0.0.1:8080",
  "manifest_count": 1
}
```

Possible `status` values:

- `ok`
- `shutting_down`

### Readiness

`GET /ready`

Alias:

- `GET /readiness`

Returns readiness information, startup recovery results, and background task health.

Example response:

```json
{
  "status": "ready",
  "listen": "127.0.0.1:8080",
  "manifest_count": 1,
  "startup": {
    "completed": true,
    "recovered_artifacts": 0,
    "recovered_jobs": 0
  },
  "background_tasks": [
    {
      "name": "artifact_cleanup",
      "running": true,
      "last_success_at": "2026-05-30T12:00:00Z",
      "last_error": null
    }
  ]
}
```

### Upload artifact

`POST /artifacts`

Required permission:

- `artifacts:write`

Body:

- Raw bytes

Headers:

- `Content-Type: application/octet-stream`
- `X-SHA256: <hex digest>` when checksums are required

Example:

```bash
checksum="$(shasum -a 256 dist/app.tar.gz | awk '{print $1}')"

curl -X POST http://127.0.0.1:8080/artifacts \
  -H "Content-Type: application/octet-stream" \
  -H "X-SHA256: $checksum" \
  --data-binary @dist/app.tar.gz
```

Direct protected API calls also need the `X-Janus-*` signed request headers normally generated by `janus-server`.

Example response:

```json
{
  "artifact_id": "art_018f7d0fd88e7dc7a4d0d4a0c0f47f50",
  "sha256": "8e9c158c3f1f83a3a0c92c6b2d18d6f4c8570d9c3b9cb5cf1ce9e54e9f8dd0db",
  "size": 12345,
  "expires_at": "2026-05-31T12:00:00Z"
}
```

### Download artifact

`GET /artifacts/{artifact_id}`

Required permission:

- `artifacts:read`

Returns:

- `200 OK`
- `Content-Type: application/octet-stream`
- `X-SHA256: <hex digest>`

### List jobs

`GET /jobs`

Required permission:

- `jobs:read`

Returns all loaded job definitions.

Example response:

```json
[
  {
    "name": "build-app",
    "concurrency": "job_exclusive",
    "timeout_seconds": 600,
    "inputs": {
      "commit": {
        "type": "string",
        "required": true
      },
      "branch": {
        "type": "string",
        "required": true
      },
      "source": {
        "type": "artifact",
        "required": true
      }
    },
    "outputs": {
      "app": {
        "path": "app.tar.gz",
        "required": true
      }
    }
  }
]
```

### Start a job

`POST /jobs/{name}/runs`

Required permission:

- `jobs:run`

Body:

- A JSON object whose keys match the manifest input names

Example:

```bash
curl -X POST http://127.0.0.1:8080/jobs/build-app/runs \
  -H "Content-Type: application/json" \
  -d '{
    "commit": "abc123",
    "branch": "main",
    "source": "art_018f7d0fd88e7dc7a4d0d4a0c0f47f50"
  }'
```

Example response:

```json
{
  "job_id": "job_018f7d10ab8978d4b7f041ab4fa6fc6b",
  "status": "running",
  "started_at": "2026-05-30T12:00:00Z"
}
```

### Get job status

`GET /runs/{job_id}`

Required permission:

- `jobs:read`

Example response:

```json
{
  "job_id": "job_018f7d10ab8978d4b7f041ab4fa6fc6b",
  "name": "build-app",
  "status": "success",
  "started_at": "2026-05-30T12:00:00Z",
  "finished_at": "2026-05-30T12:00:03Z",
  "exit_code": 0,
  "outputs": {
    "app": {
      "artifact_id": "art_018f7d1188e07f1a88ec68eb8566bb96",
      "sha256": "2f0f3b5f1a2e7ea3b9f3fb0ad7f75774e0e0e3d229ef7d4a93d69f8b8f17d83f",
      "size": 6
    }
  }
}
```

Possible job statuses:

- `running`
- `success`
- `failed`
- `timed_out`
- `canceled`
- `rejected`

### Get logs

`GET /runs/{job_id}/logs`

Required permission:

- `logs:read`

Example response:

```json
{
  "stdout": "build output",
  "stderr": ""
}
```

### Cancel a job

`DELETE /runs/{job_id}`

Required permission:

- `jobs:run`

Returns `202 Accepted` when cancellation has been requested.

## End-To-End Example

A typical flow looks like this:

1. Upload an artifact, such as a source tarball.
2. Start a job that references that artifact ID in an `artifact` input.
3. Poll `GET /runs/{job_id}` until the job reaches a terminal status.
4. Read `GET /runs/{job_id}/logs` if needed.
5. Download any output artifacts returned in the job status response.

## Persistence Model

The runner stores state under `data_dir`.

High-level layout:

- `data_dir/artifacts/` - uploaded artifacts and output artifacts
- `data_dir/jobs/` - per-run metadata, stdout, stderr, work directory, output directory

Each job run gets its own directory containing:

- `metadata.json`
- `stdout.log`
- `stderr.log`
- `work/`
- `output/`

## Failure And Recovery Behavior

- Incomplete artifact directories are removed on startup.
- Jobs that were still `running` when the runner stopped are marked `failed` on startup.
- Expired artifacts are deleted by the background cleanup task.
- Job creation is rejected while the runner is shutting down.
- On shutdown, active jobs are canceled and the runner waits briefly for them to drain.

## Limits And Validation

Artifact upload validation:

- Optional or required SHA-256 header depending on config
- Maximum upload size
- Artifact ID validation on download
- Per-server upload rate limiting

Job run validation:

- Request body must be a JSON object
- Maximum request body size
- Inputs must match the manifest exactly
- Input types and constraints are validated before execution
- Per-server job-run rate limiting
- Concurrency rules are enforced before the job starts

## Logging

The runner uses structured JSON logs via `tracing`.

You can control verbosity with `RUST_LOG`, for example:

```bash
RUST_LOG=janus_runner=debug,axum=info cargo run -- runner.toml
```

## Development

Run the test suite:

```bash
cd janus-runner
cargo test
```

Useful local files:

- [`janus-runner/src/main.rs`](/Users/petter/dev/Projects/janus/janus-runner/src/main.rs:1) - server setup and routes
- [`janus-runner/src/manifest.rs`](/Users/petter/dev/Projects/janus/janus-runner/src/manifest.rs:1) - manifest schema and validation
- [`janus-runner/src/artifacts.rs`](/Users/petter/dev/Projects/janus/janus-runner/src/artifacts.rs:1) - artifact storage and HTTP handlers
- [`janus-runner/src/jobs/`](/Users/petter/dev/Projects/janus/janus-runner/src/jobs) - job API, execution, storage, and models

## Design Boundaries

This project is intentionally constrained:

- Jobs must be declared ahead of time in manifests.
- Scripts are executed locally on the runner host.
- The API is small and explicit.
- The runner is not a general-purpose remote shell.

That constraint is the point: callers can trigger a controlled set of known jobs with typed inputs and artifact handoff, while the host retains ownership of what can actually run.
