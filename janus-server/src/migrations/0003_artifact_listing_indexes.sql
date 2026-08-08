CREATE INDEX idx_server_artifacts_created_at
    ON server_artifacts(created_at DESC, id DESC);

CREATE INDEX idx_job_run_artifacts_server_artifact
    ON job_run_artifacts(server_artifact_id);
