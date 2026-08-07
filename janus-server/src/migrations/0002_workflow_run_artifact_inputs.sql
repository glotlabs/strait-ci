CREATE TABLE workflow_run_artifact_inputs (
    pipeline_run_id TEXT NOT NULL,
    job_index INTEGER NOT NULL,
    input_name TEXT NOT NULL,
    server_artifact_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(pipeline_run_id, job_index, input_name),
    FOREIGN KEY(pipeline_run_id) REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    FOREIGN KEY(server_artifact_id) REFERENCES server_artifacts(id) ON DELETE RESTRICT
);

CREATE INDEX idx_workflow_run_artifact_inputs_artifact
    ON workflow_run_artifact_inputs(server_artifact_id);
