use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, Row, params, types::Type};
use serde_json::Value;
use uuid::Uuid;

#[cfg(test)]
use crate::models::AuditEvent;
use crate::models::{
    JobRun, JobRunDetail, JobRunOutput, PipelineRun, PipelineSnapshot, PreviousJobSummary,
    ProducedArtifact, PromotableArtifact, PushEvent, PushEventRef, Repo, Runner,
    RunnerJobDefinition, RunnerJobInputDefinition, RunnerJobOutputDefinition, ServerArtifact, User,
    UserRole, Workflow, WorkflowJobOutcomePolicy, WorkflowRunArtifactSelection,
};
use crate::state_machine::{self, JobStatus};
use janus_lib::{Concurrency, InputType, JobOutputMetadata, OutputType};

struct Migration {
    version: i64,
    sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        sql: include_str!("migrations/0001_init.sql"),
    },
    Migration {
        version: 2,
        sql: include_str!("migrations/0002_workflow_run_artifact_inputs.sql"),
    },
    Migration {
        version: 3,
        sql: include_str!("migrations/0003_artifact_listing_indexes.sql"),
    },
];

#[derive(Clone)]
pub struct Database {
    conn: Arc<Mutex<Connection>>,
}

impl Database {
    pub fn open(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA foreign_keys = ON;")?;
        apply_migrations(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn create_user(
        &self,
        username: &str,
        password_hash: &str,
        role: UserRole,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let id = Uuid::now_v7().to_string();
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO users (id, username, password_hash, role, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![id, username, password_hash, role.as_str(), now()],
        )?;
        Ok(id)
    }

    pub fn create_audit_event(
        &self,
        actor: Option<&User>,
        action: &str,
        target_type: &str,
        target_id: Option<&str>,
        target_name: Option<&str>,
        metadata: Value,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let id = Uuid::now_v7().to_string();
        let metadata_json = serde_json::to_string(&metadata)?;
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO audit_events (id, actor_user_id, actor_username, action, target_type, target_id, target_name, metadata_json, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                id,
                actor.map(|user| user.id.as_str()),
                actor.map(|user| user.username.as_str()),
                action,
                target_type,
                target_id,
                target_name,
                metadata_json,
                now()
            ],
        )?;
        Ok(id)
    }

    #[cfg(test)]
    pub fn list_audit_events(&self) -> Result<Vec<AuditEvent>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT id, actor_user_id, actor_username, action, target_type, target_id, target_name, metadata_json, created_at
             FROM audit_events
             ORDER BY created_at ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(AuditEvent {
                id: row.get(0)?,
                actor_user_id: row.get(1)?,
                actor_username: row.get(2)?,
                action: row.get(3)?,
                target_type: row.get(4)?,
                target_id: row.get(5)?,
                target_name: row.get(6)?,
                metadata_json: row.get(7)?,
                created_at: row.get(8)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn list_users(&self) -> Result<Vec<User>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt =
            conn.prepare("SELECT id, username, role, created_at FROM users ORDER BY username ASC")?;
        let rows = stmt.query_map([], |row| {
            Ok(User {
                id: row.get(0)?,
                username: row.get(1)?,
                role: user_role_from_row(row, 2)?,
                created_at: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn user_count(&self) -> Result<i64, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Ok(conn.query_row("SELECT COUNT(*) FROM users", [], |row| row.get(0))?)
    }

    pub fn get_user_credentials(
        &self,
        username: &str,
    ) -> Result<Option<(User, String)>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Ok(conn
            .query_row(
                "SELECT id, username, role, created_at, password_hash FROM users WHERE username = ?1",
                [username],
                |row| {
                    Ok((
                        User {
                            id: row.get(0)?,
                            username: row.get(1)?,
                            role: user_role_from_row(row, 2)?,
                            created_at: row.get(3)?,
                        },
                        row.get(4)?,
                    ))
                },
            )
            .optional()?)
    }

    pub fn create_session(
        &self,
        user_id: &str,
        expires_at: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let session_id = Uuid::new_v4().to_string();
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO sessions (id, user_id, expires_at, created_at) VALUES (?1, ?2, ?3, ?4)",
            params![session_id, user_id, expires_at, now()],
        )?;
        Ok(session_id)
    }

    pub fn delete_sessions_for_user(
        &self,
        user_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute("DELETE FROM sessions WHERE user_id = ?1", [user_id])?;
        Ok(())
    }

    pub fn cleanup_expired_sessions(&self) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute("DELETE FROM sessions WHERE expires_at <= ?1", [now()])?;
        Ok(())
    }

    pub fn delete_session(&self, session_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute("DELETE FROM sessions WHERE id = ?1", [session_id])?;
        Ok(())
    }

    pub fn user_for_session(
        &self,
        session_id: &str,
    ) -> Result<Option<User>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Ok(conn
            .query_row(
                "SELECT u.id, u.username, u.role, u.created_at
                 FROM sessions s
                 JOIN users u ON u.id = s.user_id
                 WHERE s.id = ?1 AND s.expires_at > ?2",
                params![session_id, now()],
                |row| {
                    Ok(User {
                        id: row.get(0)?,
                        username: row.get(1)?,
                        role: user_role_from_row(row, 2)?,
                        created_at: row.get(3)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn create_repo(
        &self,
        name: &str,
        normalized_name: &str,
        bare_path: &str,
        default_branch: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let id = Uuid::now_v7().to_string();
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO repos (id, name, normalized_name, bare_path, default_branch, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![id, name, normalized_name, bare_path, default_branch, now()],
        )?;
        Ok(id)
    }

    pub fn delete_repo(&self, repo_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute("DELETE FROM repos WHERE id = ?1", [repo_id])?;
        Ok(())
    }

    pub fn list_repos(&self) -> Result<Vec<Repo>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT id, name, normalized_name, bare_path, default_branch, created_at
             FROM repos
             ORDER BY normalized_name",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(Repo {
                id: row.get(0)?,
                name: row.get(1)?,
                normalized_name: row.get(2)?,
                bare_path: row.get(3)?,
                default_branch: row.get(4)?,
                created_at: row.get(5)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn get_repo(&self, repo_id: &str) -> Result<Option<Repo>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Ok(conn
            .query_row(
                "SELECT id, name, normalized_name, bare_path, default_branch, created_at
             FROM repos WHERE id = ?1",
                [repo_id],
                |row| {
                    Ok(Repo {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        normalized_name: row.get(2)?,
                        bare_path: row.get(3)?,
                        default_branch: row.get(4)?,
                        created_at: row.get(5)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn get_repo_by_normalized_name(
        &self,
        normalized_name: &str,
    ) -> Result<Option<Repo>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Ok(conn
            .query_row(
                "SELECT id, name, normalized_name, bare_path, default_branch, created_at
             FROM repos WHERE normalized_name = ?1",
                [normalized_name],
                |row| {
                    Ok(Repo {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        normalized_name: row.get(2)?,
                        bare_path: row.get(3)?,
                        default_branch: row.get(4)?,
                        created_at: row.get(5)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn create_push_event(
        &self,
        repo_id: &str,
        event_key: &str,
        refs: &[PushEventRef],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = self.conn.lock().expect("db mutex poisoned");
        let tx = conn.transaction()?;
        let push_event_id = Uuid::now_v7().to_string();
        tx.execute(
            "INSERT OR IGNORE INTO push_events (id, repo_id, received_at, event_key) VALUES (?1, ?2, ?3, ?4)",
            params![push_event_id, repo_id, now(), event_key],
        )?;
        let created = tx.changes() > 0;
        if created {
            for item in refs {
                tx.execute(
                    "INSERT INTO push_event_refs (id, push_event_id, old_rev, new_rev, ref_name) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![Uuid::now_v7().to_string(), push_event_id, item.old_rev, item.new_rev, item.ref_name],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn list_unprocessed_push_events(
        &self,
    ) -> Result<Vec<PushEvent>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT id, repo_id, received_at, event_key, processed_at
             FROM push_events WHERE processed_at IS NULL ORDER BY received_at ASC",
        )?;
        let events = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(stmt);

        let mut result = Vec::new();
        for (id, repo_id, received_at, event_key, processed_at) in events {
            let mut refs_stmt = conn.prepare(
                "SELECT old_rev, new_rev, ref_name FROM push_event_refs WHERE push_event_id = ?1 ORDER BY ref_name",
            )?;
            let refs = refs_stmt
                .query_map([id.clone()], |row| {
                    Ok(PushEventRef {
                        old_rev: row.get(0)?,
                        new_rev: row.get(1)?,
                        ref_name: row.get(2)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;
            result.push(PushEvent {
                id,
                repo_id,
                received_at,
                event_key,
                processed_at,
                refs,
            });
        }
        Ok(result)
    }

    pub fn mark_push_event_processed(
        &self,
        push_event_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "UPDATE push_events SET processed_at = ?2 WHERE id = ?1",
            params![push_event_id, now()],
        )?;
        Ok(())
    }

    pub fn create_runner(
        &self,
        name: &str,
        base_url: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let id = Uuid::now_v7().to_string();
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO runners (id, name, base_url, enabled, last_health_state, created_at)
             VALUES (?1, ?2, ?3, 1, 'unknown', ?4)",
            params![id, name, base_url, now()],
        )?;
        Ok(id)
    }

    pub fn list_runners(&self) -> Result<Vec<Runner>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT id, name, base_url, enabled, last_health_state, last_seen_at, created_at FROM runners ORDER BY name ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(Runner {
                id: row.get(0)?,
                name: row.get(1)?,
                base_url: row.get(2)?,
                enabled: row.get::<_, i64>(3)? != 0,
                last_health_state: row.get(4)?,
                last_seen_at: row.get(5)?,
                created_at: row.get(6)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn get_runner(
        &self,
        runner_id: &str,
    ) -> Result<Option<Runner>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Ok(conn.query_row(
            "SELECT id, name, base_url, enabled, last_health_state, last_seen_at, created_at FROM runners WHERE id = ?1",
            [runner_id],
            |row| {
                Ok(Runner {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    base_url: row.get(2)?,
                    enabled: row.get::<_, i64>(3)? != 0,
                    last_health_state: row.get(4)?,
                    last_seen_at: row.get(5)?,
                    created_at: row.get(6)?,
                })
            },
        ).optional()?)
    }

    pub fn set_runner_enabled(
        &self,
        runner_id: &str,
        enabled: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "UPDATE runners SET enabled = ?2 WHERE id = ?1",
            params![runner_id, enabled as i64],
        )?;
        Ok(())
    }

    pub fn update_runner_name(
        &self,
        runner_id: &str,
        name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "UPDATE runners SET name = ?2 WHERE id = ?1",
            params![runner_id, name],
        )?;
        Ok(())
    }

    pub fn update_runner_health(
        &self,
        runner_id: &str,
        state: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "UPDATE runners SET last_health_state = ?2, last_seen_at = ?3 WHERE id = ?1",
            params![runner_id, state, now()],
        )?;
        Ok(())
    }

    pub fn replace_runner_jobs(
        &self,
        runner_id: &str,
        jobs: &[RunnerJobDefinition],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = self.conn.lock().expect("db mutex poisoned");
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM runner_jobs WHERE runner_id = ?1", [runner_id])?;
        for job in jobs {
            let runner_job_id = Uuid::now_v7().to_string();
            tx.execute(
                "INSERT INTO runner_jobs (id, runner_id, job_name, concurrency, timeout_seconds, last_refreshed_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    runner_job_id,
                    runner_id,
                    job.name,
                    job.concurrency.as_str(),
                    job.timeout_seconds as i64,
                    now()
                ],
            )?;
            insert_runner_job_definition(&tx, &runner_job_id, job)?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn list_runner_jobs(
        &self,
        runner_id: &str,
    ) -> Result<Vec<RunnerJobDefinition>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT id, job_name, concurrency, timeout_seconds FROM runner_jobs WHERE runner_id = ?1 ORDER BY job_name",
        )?;
        let rows = stmt.query_map([runner_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        let rows = rows.collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|(job_id, job_name, concurrency, timeout_seconds)| {
                load_runner_job_definition(&conn, &job_id, &job_name, &concurrency, timeout_seconds)
            })
            .collect()
    }

    pub fn create_workflow(
        &self,
        repo_id: &str,
        name: &str,
        enabled: bool,
        trigger_json: &str,
        definition_json: &str,
        job_schemas: &[RunnerJobDefinition],
    ) -> Result<String, Box<dyn std::error::Error>> {
        let workflow_id = Uuid::now_v7().to_string();
        let version_id = Uuid::now_v7().to_string();
        let now = now();
        let mut conn = self.conn.lock().expect("db mutex poisoned");
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO workflows (id, repo_id, name, enabled, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![workflow_id, repo_id, name, enabled as i64, now,],
        )?;
        tx.execute(
            "INSERT INTO workflow_versions (id, workflow_id, version, trigger_json, definition_json, created_at)
             VALUES (?1, ?2, 1, ?3, ?4, ?5)",
            params![
                version_id,
                workflow_id,
                trigger_json,
                definition_json,
                now
            ],
        )?;
        insert_workflow_version_job_schemas(&tx, &version_id, job_schemas)?;
        tx.commit()?;
        Ok(workflow_id)
    }

    pub fn list_workflows(&self) -> Result<Vec<Workflow>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT w.id, w.repo_id, w.name, w.enabled, w.created_at, v.version, v.id, v.trigger_json, v.definition_json
             FROM workflows w
             JOIN workflow_versions v ON v.workflow_id = w.id
             WHERE v.version = (SELECT MAX(version) FROM workflow_versions WHERE workflow_id = w.id)
             ORDER BY w.created_at DESC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(Workflow {
                id: row.get(0)?,
                repo_id: row.get(1)?,
                name: row.get(2)?,
                enabled: row.get::<_, i64>(3)? != 0,
                created_at: row.get(4)?,
                version: row.get(5)?,
                version_id: row.get(6)?,
                trigger_json: row.get(7)?,
                definition_json: row.get(8)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn workflows_for_repo(
        &self,
        repo_id: &str,
    ) -> Result<Vec<Workflow>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT w.id, w.repo_id, w.name, w.enabled, w.created_at, v.version, v.id, v.trigger_json, v.definition_json
             FROM workflows w
             JOIN workflow_versions v ON v.workflow_id = w.id
             WHERE w.repo_id = ?1
               AND v.version = (SELECT MAX(version) FROM workflow_versions WHERE workflow_id = w.id)
             ORDER BY w.name ASC",
        )?;
        let rows = stmt.query_map([repo_id], |row| {
            Ok(Workflow {
                id: row.get(0)?,
                repo_id: row.get(1)?,
                name: row.get(2)?,
                enabled: row.get::<_, i64>(3)? != 0,
                created_at: row.get(4)?,
                version: row.get(5)?,
                version_id: row.get(6)?,
                trigger_json: row.get(7)?,
                definition_json: row.get(8)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn get_workflow(
        &self,
        workflow_id: &str,
    ) -> Result<Option<Workflow>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Ok(conn.query_row(
            "SELECT w.id, w.repo_id, w.name, w.enabled, w.created_at, v.version, v.id, v.trigger_json, v.definition_json
             FROM workflows w
             JOIN workflow_versions v ON v.workflow_id = w.id
             WHERE w.id = ?1
               AND v.version = (SELECT MAX(version) FROM workflow_versions WHERE workflow_id = w.id)",
            [workflow_id],
            |row| {
                Ok(Workflow {
                    id: row.get(0)?,
                    repo_id: row.get(1)?,
                    name: row.get(2)?,
                    enabled: row.get::<_, i64>(3)? != 0,
                    created_at: row.get(4)?,
                    version: row.get(5)?,
                    version_id: row.get(6)?,
                    trigger_json: row.get(7)?,
                    definition_json: row.get(8)?,
                })
            },
        ).optional()?)
    }

    pub fn get_workflow_by_version_id(
        &self,
        workflow_version_id: &str,
    ) -> Result<Option<Workflow>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Ok(conn.query_row(
            "SELECT w.id, w.repo_id, w.name, w.enabled, w.created_at, v.version, v.id, v.trigger_json, v.definition_json
             FROM workflows w
             JOIN workflow_versions v ON v.workflow_id = w.id
             WHERE v.id = ?1",
            [workflow_version_id],
            |row| {
                Ok(Workflow {
                    id: row.get(0)?,
                    repo_id: row.get(1)?,
                    name: row.get(2)?,
                    enabled: row.get::<_, i64>(3)? != 0,
                    created_at: row.get(4)?,
                    version: row.get(5)?,
                    version_id: row.get(6)?,
                    trigger_json: row.get(7)?,
                    definition_json: row.get(8)?,
                })
            },
        ).optional()?)
    }

    pub fn update_workflow(
        &self,
        workflow_id: &str,
        name: &str,
        enabled: bool,
        trigger_json: &str,
        definition_json: &str,
        job_schemas: &[RunnerJobDefinition],
    ) -> Result<String, Box<dyn std::error::Error>> {
        let mut conn = self.conn.lock().expect("db mutex poisoned");
        let tx = conn.transaction()?;
        let next_version: i64 = tx.query_row(
            "SELECT COALESCE(MAX(version), 0) + 1 FROM workflow_versions WHERE workflow_id = ?1",
            [workflow_id],
            |row| row.get(0),
        )?;
        let version_id = Uuid::now_v7().to_string();
        tx.execute(
            "UPDATE workflows SET name = ?2, enabled = ?3 WHERE id = ?1",
            params![workflow_id, name, enabled as i64],
        )?;
        tx.execute(
            "INSERT INTO workflow_versions (id, workflow_id, version, trigger_json, definition_json, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                version_id,
                workflow_id,
                next_version,
                trigger_json,
                definition_json,
                now()
            ],
        )?;
        insert_workflow_version_job_schemas(&tx, &version_id, job_schemas)?;
        tx.commit()?;
        Ok(version_id)
    }

    pub fn create_pipeline_run(
        &self,
        repo_id: &str,
        workflow_id: &str,
        workflow_version_id: &str,
        trigger_type: &str,
        trigger_ref: Option<&str>,
        commit_sha: Option<&str>,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let id = Uuid::now_v7().to_string();
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO pipeline_runs (id, repo_id, workflow_id, workflow_version_id, trigger_type, trigger_ref, commit_sha, status, started_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'running', ?8)",
            params![id, repo_id, workflow_id, workflow_version_id, trigger_type, trigger_ref, commit_sha, now()],
        )?;
        Ok(id)
    }

    pub fn add_workflow_run_artifact_selections(
        &self,
        pipeline_run_id: &str,
        selections: &[WorkflowRunArtifactSelection],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = self.conn.lock().expect("db mutex poisoned");
        let tx = conn.transaction()?;
        for selection in selections {
            tx.execute(
                "INSERT INTO workflow_run_artifact_inputs
                 (pipeline_run_id, job_index, input_name, server_artifact_id, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    pipeline_run_id,
                    selection.job_index as i64,
                    selection.input_name,
                    selection.server_artifact_id,
                    now()
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn workflow_run_artifact_selections(
        &self,
        pipeline_run_id: &str,
    ) -> Result<Vec<WorkflowRunArtifactSelection>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT job_index, input_name, server_artifact_id
             FROM workflow_run_artifact_inputs
             WHERE pipeline_run_id = ?1
             ORDER BY job_index, input_name",
        )?;
        let rows = stmt.query_map([pipeline_run_id], |row| {
            Ok(WorkflowRunArtifactSelection {
                job_index: row.get::<_, i64>(0)? as usize,
                input_name: row.get(1)?,
                server_artifact_id: row.get(2)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn workflow_run_artifact_selection(
        &self,
        pipeline_run_id: &str,
        job_index: usize,
        input_name: &str,
    ) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Ok(conn
            .query_row(
                "SELECT server_artifact_id
                 FROM workflow_run_artifact_inputs
                 WHERE pipeline_run_id = ?1 AND job_index = ?2 AND input_name = ?3",
                params![pipeline_run_id, job_index as i64, input_name],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub fn create_job_run(
        &self,
        pipeline_run_id: &str,
        job_index: i64,
        runner_id: &str,
        runner_job_name: &str,
        outcome_policy: &WorkflowJobOutcomePolicy,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let id = Uuid::now_v7().to_string();
        let dispatch_idempotency_key = next_dispatch_idempotency_key();
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO job_runs (id, pipeline_run_id, job_index, runner_id, runner_job_name, dispatch_idempotency_key, status, outcome_policy)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7)",
            params![
                id,
                pipeline_run_id,
                job_index,
                runner_id,
                runner_job_name,
                dispatch_idempotency_key,
                outcome_policy.as_str()
            ],
        )?;
        conn.execute(
            "INSERT INTO job_run_logs (job_run_id, stdout, stderr, updated_at) VALUES (?1, '', '', ?2)",
            params![id, now()],
        )?;
        Ok(id)
    }

    pub fn add_previous_job(
        &self,
        job_run_id: &str,
        previous_job_run_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO job_run_previous (job_run_id, previous_job_run_id) VALUES (?1, ?2)",
            params![job_run_id, previous_job_run_id],
        )?;
        Ok(())
    }

    pub fn list_pipeline_runs(&self) -> Result<Vec<PipelineRun>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT id, repo_id, workflow_id, workflow_version_id, trigger_type, trigger_ref, commit_sha, status, started_at, cancel_reason, cancel_requested_at, cancel_started_at, finished_at
             FROM pipeline_runs ORDER BY started_at DESC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(PipelineRun {
                id: row.get(0)?,
                repo_id: row.get(1)?,
                workflow_id: row.get(2)?,
                workflow_version_id: row.get(3)?,
                trigger_type: row.get(4)?,
                trigger_ref: row.get(5)?,
                commit_sha: row.get(6)?,
                status: row.get(7)?,
                started_at: row.get(8)?,
                cancel_reason: row.get(9)?,
                cancel_requested_at: row.get(10)?,
                cancel_started_at: row.get(11)?,
                finished_at: row.get(12)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn get_pipeline_run(
        &self,
        pipeline_id: &str,
    ) -> Result<Option<PipelineRun>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Ok(conn.query_row(
            "SELECT id, repo_id, workflow_id, workflow_version_id, trigger_type, trigger_ref, commit_sha, status, started_at, cancel_reason, cancel_requested_at, cancel_started_at, finished_at
             FROM pipeline_runs WHERE id = ?1",
            [pipeline_id],
            |row| {
                Ok(PipelineRun {
                    id: row.get(0)?,
                    repo_id: row.get(1)?,
                    workflow_id: row.get(2)?,
                    workflow_version_id: row.get(3)?,
                    trigger_type: row.get(4)?,
                    trigger_ref: row.get(5)?,
                    commit_sha: row.get(6)?,
                    status: row.get(7)?,
                    started_at: row.get(8)?,
                    cancel_reason: row.get(9)?,
                    cancel_requested_at: row.get(10)?,
                    cancel_started_at: row.get(11)?,
                    finished_at: row.get(12)?,
                })
            },
        ).optional()?)
    }

    pub fn pipeline_snapshot(
        &self,
        pipeline_id: &str,
    ) -> Result<Option<PipelineSnapshot>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let pipeline = conn
            .query_row(
                "SELECT id, repo_id, workflow_id, workflow_version_id, trigger_type, trigger_ref, commit_sha, status, started_at, cancel_reason, cancel_requested_at, cancel_started_at, finished_at
                 FROM pipeline_runs WHERE id = ?1",
                [pipeline_id],
                |row| {
                    Ok(PipelineRun {
                        id: row.get(0)?,
                        repo_id: row.get(1)?,
                        workflow_id: row.get(2)?,
                        workflow_version_id: row.get(3)?,
                        trigger_type: row.get(4)?,
                        trigger_ref: row.get(5)?,
                        commit_sha: row.get(6)?,
                        status: row.get(7)?,
                        started_at: row.get(8)?,
                        cancel_reason: row.get(9)?,
                        cancel_requested_at: row.get(10)?,
                        cancel_started_at: row.get(11)?,
                        finished_at: row.get(12)?,
                    })
                },
            )
            .optional()?;
        let Some(pipeline) = pipeline else {
            return Ok(None);
        };
        let mut stmt = conn.prepare(
            "SELECT id, pipeline_run_id, job_index, runner_id, runner_job_name, dispatch_idempotency_key, runner_run_id, status, outcome_policy, started_at, duration_ms, exit_code, terminal_reason, failure_category, cancel_reason, cancel_requested_at, cancel_started_at, cancel_retry_count, last_cancel_retry_at, infra_retry_count, last_infra_retry_at, finished_at, output_metadata_json
             FROM job_runs WHERE pipeline_run_id = ?1 ORDER BY job_index",
        )?;
        let job_rows = stmt
            .query_map([pipeline_id], |row| {
                Ok(JobRun {
                    id: row.get(0)?,
                    pipeline_run_id: row.get(1)?,
                    job_index: row.get(2)?,
                    runner_id: row.get(3)?,
                    runner_job_name: row.get(4)?,
                    dispatch_idempotency_key: row.get(5)?,
                    runner_run_id: row.get(6)?,
                    status: row.get(7)?,
                    outcome_policy: outcome_policy_from_row(row, 8)?,
                    started_at: row.get(9)?,
                    duration_ms: row.get(10)?,
                    exit_code: row.get(11)?,
                    terminal_reason: row.get(12)?,
                    failure_category: row.get(13)?,
                    cancel_reason: row.get(14)?,
                    cancel_requested_at: row.get(15)?,
                    cancel_started_at: row.get(16)?,
                    cancel_retry_count: row.get(17)?,
                    last_cancel_retry_at: row.get(18)?,
                    infra_retry_count: row.get(19)?,
                    last_infra_retry_at: row.get(20)?,
                    finished_at: row.get(21)?,
                    output_metadata: parse_output_metadata(row.get(22)?),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(stmt);
        let mut jobs = Vec::new();
        for run in job_rows {
            let (stdout, stderr): (String, String) = conn.query_row(
                "SELECT stdout, stderr FROM job_run_logs WHERE job_run_id = ?1",
                [run.id.clone()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let resolved_inputs = conn
                .query_row(
                    "SELECT input_payload_json FROM job_runs WHERE id = ?1",
                    [run.id.clone()],
                    |row| row.get::<_, Option<String>>(0),
                )?
                .and_then(|json| serde_json::from_str::<Value>(&json).ok())
                .and_then(|value| match value {
                    Value::Object(map) => Some(map.into_iter().collect()),
                    _ => None,
                })
                .unwrap_or_default();
            let mut dep_stmt = conn.prepare(
                "SELECT jr.id, jr.job_index, jr.runner_job_name, jr.status
                 FROM job_run_previous p
                 JOIN job_runs jr ON jr.id = p.previous_job_run_id
                 WHERE p.job_run_id = ?1
                 ORDER BY jr.job_index",
            )?;
            let previous_jobs = dep_stmt
                .query_map([run.id.clone()], |row| {
                    Ok(PreviousJobSummary {
                        job_run_id: row.get(0)?,
                        job_index: row.get(1)?,
                        runner_job_name: row.get(2)?,
                        status: row.get(3)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;
            let mut artifact_stmt = conn.prepare(
                "SELECT artifact_name, output_type, runner_artifact_id, server_artifact_id, value_json, sha256, size_bytes FROM job_run_artifacts WHERE job_run_id = ?1 ORDER BY artifact_name",
            )?;
            let outputs = artifact_stmt
                .query_map([run.id.clone()], parse_job_run_output_row)?
                .collect::<Result<Vec<_>, _>>()?;
            jobs.push(JobRunDetail {
                run,
                stdout,
                stderr,
                outputs,
                previous_jobs,
                resolved_inputs,
            });
        }
        let mut selection_stmt = conn.prepare(
            "SELECT job_index, input_name, server_artifact_id
             FROM workflow_run_artifact_inputs
             WHERE pipeline_run_id = ?1
             ORDER BY job_index, input_name",
        )?;
        let artifact_selections = selection_stmt
            .query_map([pipeline_id], |row| {
                Ok(WorkflowRunArtifactSelection {
                    job_index: row.get::<_, i64>(0)? as usize,
                    input_name: row.get(1)?,
                    server_artifact_id: row.get(2)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(PipelineSnapshot {
            pipeline,
            jobs,
            artifact_selections,
        }))
    }

    pub fn list_job_runs_by_status(
        &self,
        statuses: &[&str],
    ) -> Result<Vec<JobRun>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut query = String::from(
            "SELECT id, pipeline_run_id, job_index, runner_id, runner_job_name, dispatch_idempotency_key, runner_run_id, status, outcome_policy, started_at, duration_ms, exit_code, terminal_reason, failure_category, cancel_reason, cancel_requested_at, cancel_started_at, cancel_retry_count, last_cancel_retry_at, infra_retry_count, last_infra_retry_at, finished_at, output_metadata_json FROM job_runs WHERE status IN (",
        );
        for idx in 0..statuses.len() {
            if idx > 0 {
                query.push(',');
            }
            query.push('?');
            query.push_str(&(idx + 1).to_string());
        }
        query.push(')');
        let mut stmt = conn.prepare(&query)?;
        let rows = stmt.query_map(
            rusqlite::params_from_iter(statuses.iter().copied()),
            |row| {
                Ok(JobRun {
                    id: row.get(0)?,
                    pipeline_run_id: row.get(1)?,
                    job_index: row.get(2)?,
                    runner_id: row.get(3)?,
                    runner_job_name: row.get(4)?,
                    dispatch_idempotency_key: row.get(5)?,
                    runner_run_id: row.get(6)?,
                    status: row.get(7)?,
                    outcome_policy: outcome_policy_from_row(row, 8)?,
                    started_at: row.get(9)?,
                    duration_ms: row.get(10)?,
                    exit_code: row.get(11)?,
                    terminal_reason: row.get(12)?,
                    failure_category: row.get(13)?,
                    cancel_reason: row.get(14)?,
                    cancel_requested_at: row.get(15)?,
                    cancel_started_at: row.get(16)?,
                    cancel_retry_count: row.get(17)?,
                    last_cancel_retry_at: row.get(18)?,
                    infra_retry_count: row.get(19)?,
                    last_infra_retry_at: row.get(20)?,
                    finished_at: row.get(21)?,
                    output_metadata: parse_output_metadata(row.get(22)?),
                })
            },
        )?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn previous_jobs_for_job_run(
        &self,
        job_run_id: &str,
    ) -> Result<Vec<JobRun>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT jr.id, jr.pipeline_run_id, jr.job_index, jr.runner_id, jr.runner_job_name, jr.dispatch_idempotency_key, jr.runner_run_id, jr.status, jr.outcome_policy, jr.started_at, jr.duration_ms, jr.exit_code, jr.terminal_reason, jr.failure_category, jr.cancel_reason, jr.cancel_requested_at, jr.cancel_started_at, jr.cancel_retry_count, jr.last_cancel_retry_at, jr.infra_retry_count, jr.last_infra_retry_at, jr.finished_at, jr.output_metadata_json
             FROM job_run_previous d
             JOIN job_runs jr ON jr.id = d.previous_job_run_id
             WHERE d.job_run_id = ?1
             ORDER BY jr.job_index",
        )?;
        let rows = stmt.query_map([job_run_id], |row| {
            Ok(JobRun {
                id: row.get(0)?,
                pipeline_run_id: row.get(1)?,
                job_index: row.get(2)?,
                runner_id: row.get(3)?,
                runner_job_name: row.get(4)?,
                dispatch_idempotency_key: row.get(5)?,
                runner_run_id: row.get(6)?,
                status: row.get(7)?,
                outcome_policy: outcome_policy_from_row(row, 8)?,
                started_at: row.get(9)?,
                duration_ms: row.get(10)?,
                exit_code: row.get(11)?,
                terminal_reason: row.get(12)?,
                failure_category: row.get(13)?,
                cancel_reason: row.get(14)?,
                cancel_requested_at: row.get(15)?,
                cancel_started_at: row.get(16)?,
                cancel_retry_count: row.get(17)?,
                last_cancel_retry_at: row.get(18)?,
                infra_retry_count: row.get(19)?,
                last_infra_retry_at: row.get(20)?,
                finished_at: row.get(21)?,
                output_metadata: parse_output_metadata(row.get(22)?),
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn set_job_run_started(
        &self,
        job_run_id: &str,
        runner_run_id: &str,
        started_at: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "UPDATE job_runs
             SET status = 'running',
                 runner_run_id = ?2,
                 started_at = ?3,
                 cancel_reason = NULL,
                 cancel_requested_at = NULL,
                 cancel_started_at = NULL,
                 cancel_retry_count = 0,
                 last_cancel_retry_at = NULL
             WHERE id = ?1",
            params![job_run_id, runner_run_id, started_at],
        )?;
        Ok(())
    }

    pub fn set_job_run_input_payload(
        &self,
        job_run_id: &str,
        payload: &Value,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "UPDATE job_runs SET input_payload_json = ?2 WHERE id = ?1",
            params![job_run_id, serde_json::to_string(payload)?],
        )?;
        Ok(())
    }

    pub fn finish_job_run(
        &self,
        job_run_id: &str,
        status: &str,
        duration_ms: Option<u64>,
        exit_code: Option<i32>,
        terminal_reason: Option<&str>,
        failure_category: Option<&str>,
        output_metadata: &JobOutputMetadata,
        stdout: &str,
        stderr: &str,
        outputs: &[JobRunOutput],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = self.conn.lock().expect("db mutex poisoned");
        let tx = conn.transaction()?;
        tx.execute(
            "UPDATE job_runs
             SET status = ?2,
                 duration_ms = ?3,
                 exit_code = ?4,
                 terminal_reason = ?5,
                 failure_category = ?6,
                 finished_at = ?7,
                 output_metadata_json = ?8,
                 cancel_requested_at = CASE WHEN ?2 = 'canceled' THEN cancel_requested_at ELSE NULL END,
                 cancel_started_at = CASE WHEN ?2 = 'canceled' THEN cancel_started_at ELSE NULL END,
                 cancel_retry_count = CASE WHEN ?2 = 'canceled' THEN cancel_retry_count ELSE 0 END,
                 last_cancel_retry_at = CASE WHEN ?2 = 'canceled' THEN last_cancel_retry_at ELSE NULL END
             WHERE id = ?1",
            params![
                job_run_id,
                status,
                duration_ms.map(|value| value as i64),
                exit_code,
                terminal_reason,
                failure_category,
                now(),
                serde_json::to_string(output_metadata)?
            ],
        )?;
        tx.execute(
            "UPDATE job_run_logs SET stdout = ?2, stderr = ?3, updated_at = ?4 WHERE job_run_id = ?1",
            params![job_run_id, stdout, stderr, now()],
        )?;
        tx.execute(
            "DELETE FROM job_run_artifacts WHERE job_run_id = ?1",
            [job_run_id],
        )?;
        for output in outputs {
            tx.execute(
                "INSERT INTO job_run_artifacts (id, job_run_id, artifact_name, artifact_role, output_type, runner_artifact_id, server_artifact_id, value_json, sha256, size_bytes)
                 VALUES (?1, ?2, ?3, 'output', ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    Uuid::now_v7().to_string(),
                    job_run_id,
                    output.output_name,
                    output.kind,
                    output.runner_artifact_id,
                    output.server_artifact_id,
                    output.value.as_ref().map(serde_json::to_string).transpose()?,
                    output.sha256,
                    output.size_bytes,
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn set_job_run_status(
        &self,
        job_run_id: &str,
        status: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "UPDATE job_runs
             SET status = ?2,
                 duration_ms = NULL,
                 exit_code = NULL,
                 terminal_reason = NULL,
                 failure_category = NULL,
                 output_metadata_json = NULL,
                 cancel_reason = CASE
                     WHEN ?2 IN ('cancel_requested', 'canceling', 'canceled') THEN cancel_reason
                     ELSE NULL
                 END,
                 cancel_requested_at = CASE
                     WHEN ?2 = 'cancel_requested' AND cancel_requested_at IS NULL THEN ?3
                     WHEN ?2 NOT IN ('cancel_requested', 'canceling', 'canceled') THEN NULL
                     ELSE cancel_requested_at
                 END,
                 cancel_started_at = CASE
                     WHEN ?2 = 'canceling' AND cancel_started_at IS NULL THEN ?3
                     WHEN ?2 NOT IN ('canceling', 'canceled') THEN NULL
                     ELSE cancel_started_at
                 END,
                 cancel_retry_count = CASE
                     WHEN ?2 IN ('cancel_requested', 'canceling', 'canceled') THEN cancel_retry_count
                     ELSE 0
                 END,
                 last_cancel_retry_at = CASE
                     WHEN ?2 IN ('cancel_requested', 'canceling', 'canceled') THEN last_cancel_retry_at
                     ELSE NULL
                 END,
                 finished_at = CASE
                     WHEN ?2 IN ('success', 'failed', 'canceled', 'blocked', 'skipped') THEN ?3
                     ELSE NULL
                 END
             WHERE id = ?1",
            params![job_run_id, status, now()],
        )?;
        Ok(())
    }

    pub fn mark_job_run_cancel_requested(
        &self,
        job_run_id: &str,
        reason: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "UPDATE job_runs
             SET status = 'cancel_requested',
                 cancel_reason = ?2,
                 cancel_requested_at = COALESCE(cancel_requested_at, ?3),
                 finished_at = NULL
             WHERE id = ?1",
            params![job_run_id, reason, now()],
        )?;
        Ok(())
    }

    pub fn record_cancel_retry(&self, job_run_id: &str) -> Result<i64, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "UPDATE job_runs
             SET cancel_retry_count = cancel_retry_count + 1,
                 last_cancel_retry_at = ?2
             WHERE id = ?1",
            params![job_run_id, now()],
        )?;
        let count = conn.query_row(
            "SELECT cancel_retry_count FROM job_runs WHERE id = ?1",
            [job_run_id],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    pub fn requeue_job_run_for_infra_retry(
        &self,
        job_run_id: &str,
    ) -> Result<i64, Box<dyn std::error::Error>> {
        let mut conn = self.conn.lock().expect("db mutex poisoned");
        let tx = conn.transaction()?;
        tx.execute(
            "UPDATE job_runs
             SET status = 'pending',
                 runner_run_id = NULL,
                 started_at = NULL,
                 duration_ms = NULL,
                 exit_code = NULL,
                 terminal_reason = NULL,
                 failure_category = NULL,
                 cancel_reason = NULL,
                 cancel_requested_at = NULL,
                 cancel_started_at = NULL,
                 cancel_retry_count = 0,
                 last_cancel_retry_at = NULL,
                 dispatch_idempotency_key = ?3,
                 infra_retry_count = infra_retry_count + 1,
                 last_infra_retry_at = ?2,
                 finished_at = NULL,
                 output_metadata_json = NULL
             WHERE id = ?1",
            params![job_run_id, now(), next_dispatch_idempotency_key()],
        )?;
        tx.execute(
            "UPDATE job_run_logs SET stdout = '', stderr = '', updated_at = ?2 WHERE job_run_id = ?1",
            params![job_run_id, now()],
        )?;
        tx.execute(
            "DELETE FROM job_run_artifacts WHERE job_run_id = ?1",
            [job_run_id],
        )?;
        let count = tx.query_row(
            "SELECT infra_retry_count FROM job_runs WHERE id = ?1",
            [job_run_id],
            |row| row.get(0),
        )?;
        tx.commit()?;
        Ok(count)
    }

    pub fn mark_job_run_cancel_retry_exhausted(
        &self,
        job_run_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "UPDATE job_runs
             SET status = 'failed',
                 cancel_reason = 'stuck_retry_exhausted',
                 terminal_reason = 'canceled',
                 failure_category = 'infra',
                 finished_at = ?2
             WHERE id = ?1",
            params![job_run_id, now()],
        )?;
        Ok(())
    }

    pub fn job_outputs(
        &self,
        job_run_id: &str,
    ) -> Result<Vec<JobRunOutput>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT artifact_name, output_type, runner_artifact_id, server_artifact_id, value_json, sha256, size_bytes FROM job_run_artifacts WHERE job_run_id = ?1 ORDER BY artifact_name",
        )?;
        let rows = stmt.query_map([job_run_id], parse_job_run_output_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn pipeline_for_job_run(
        &self,
        job_run_id: &str,
    ) -> Result<Option<PipelineRun>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Ok(conn.query_row(
            "SELECT p.id, p.repo_id, p.workflow_id, p.workflow_version_id, p.trigger_type, p.trigger_ref, p.commit_sha, p.status, p.started_at, p.cancel_reason, p.cancel_requested_at, p.cancel_started_at, p.finished_at
             FROM job_runs j JOIN pipeline_runs p ON p.id = j.pipeline_run_id WHERE j.id = ?1",
            [job_run_id],
            |row| {
                Ok(PipelineRun {
                    id: row.get(0)?,
                    repo_id: row.get(1)?,
                    workflow_id: row.get(2)?,
                    workflow_version_id: row.get(3)?,
                    trigger_type: row.get(4)?,
                    trigger_ref: row.get(5)?,
                    commit_sha: row.get(6)?,
                    status: row.get(7)?,
                    started_at: row.get(8)?,
                    cancel_reason: row.get(9)?,
                    cancel_requested_at: row.get(10)?,
                    cancel_started_at: row.get(11)?,
                    finished_at: row.get(12)?,
                })
            },
        ).optional()?)
    }

    pub fn list_job_runs_for_pipeline(
        &self,
        pipeline_id: &str,
    ) -> Result<Vec<JobRun>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT id, pipeline_run_id, job_index, runner_id, runner_job_name, dispatch_idempotency_key, runner_run_id, status, outcome_policy, started_at, duration_ms, exit_code, terminal_reason, failure_category, cancel_reason, cancel_requested_at, cancel_started_at, cancel_retry_count, last_cancel_retry_at, infra_retry_count, last_infra_retry_at, finished_at, output_metadata_json
             FROM job_runs WHERE pipeline_run_id = ?1 ORDER BY job_index",
        )?;
        let rows = stmt.query_map([pipeline_id], |row| {
            Ok(JobRun {
                id: row.get(0)?,
                pipeline_run_id: row.get(1)?,
                job_index: row.get(2)?,
                runner_id: row.get(3)?,
                runner_job_name: row.get(4)?,
                dispatch_idempotency_key: row.get(5)?,
                runner_run_id: row.get(6)?,
                status: row.get(7)?,
                outcome_policy: outcome_policy_from_row(row, 8)?,
                started_at: row.get(9)?,
                duration_ms: row.get(10)?,
                exit_code: row.get(11)?,
                terminal_reason: row.get(12)?,
                failure_category: row.get(13)?,
                cancel_reason: row.get(14)?,
                cancel_requested_at: row.get(15)?,
                cancel_started_at: row.get(16)?,
                cancel_retry_count: row.get(17)?,
                last_cancel_retry_at: row.get(18)?,
                infra_retry_count: row.get(19)?,
                last_infra_retry_at: row.get(20)?,
                finished_at: row.get(21)?,
                output_metadata: parse_output_metadata(row.get(22)?),
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn workflow_definition_json(
        &self,
        workflow_version_id: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Ok(conn.query_row(
            "SELECT definition_json FROM workflow_versions WHERE id = ?1",
            [workflow_version_id],
            |row| row.get(0),
        )?)
    }

    pub fn workflow_job_schemas(
        &self,
        workflow_version_id: &str,
    ) -> Result<Vec<RunnerJobDefinition>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT id, job_index, job_name, concurrency, timeout_seconds FROM workflow_version_job_schemas WHERE workflow_version_id = ?1 ORDER BY job_index",
        )?;
        let rows = stmt
            .query_map([workflow_version_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(
                |(schema_id, _job_index, job_name, concurrency, timeout_seconds)| {
                    load_workflow_version_job_schema(
                        &conn,
                        &schema_id,
                        &job_name,
                        &concurrency,
                        timeout_seconds,
                    )
                },
            )
            .collect()
    }

    pub fn finalize_pipeline_status(
        &self,
        pipeline_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt =
            conn.prepare("SELECT status, outcome_policy FROM job_runs WHERE pipeline_run_id = ?1")?;
        let rows = stmt
            .query_map([pipeline_id], |row| {
                Ok((row.get::<_, String>(0)?, outcome_policy_from_row(row, 1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let pipeline_status = state_machine::terminal_pipeline_status(rows.iter().filter_map(
            |(status, outcome_policy)| {
                JobStatus::parse(status).map(|parsed| (parsed, outcome_policy.allows_failure()))
            },
        ));

        conn.execute(
            "UPDATE pipeline_runs
             SET status = ?2,
                 cancel_reason = CASE
                     WHEN ?2 IN ('cancel_requested', 'canceling', 'canceled') THEN cancel_reason
                     ELSE NULL
                 END,
                 cancel_requested_at = CASE
                     WHEN ?2 IN ('cancel_requested', 'canceling', 'canceled') THEN cancel_requested_at
                     ELSE NULL
                 END,
                 cancel_started_at = CASE
                     WHEN ?2 IN ('canceling', 'canceled') THEN cancel_started_at
                     ELSE NULL
                 END,
                 finished_at = CASE WHEN ?2 IN ('success', 'failed', 'canceled', 'blocked') THEN ?3 ELSE NULL END
             WHERE id = ?1",
            params![pipeline_id, pipeline_status.as_str(), now()],
        )?;
        Ok(())
    }

    pub fn set_pipeline_status(
        &self,
        pipeline_id: &str,
        status: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "UPDATE pipeline_runs
             SET status = ?2,
                 cancel_reason = CASE
                     WHEN ?2 IN ('cancel_requested', 'canceling', 'canceled') THEN cancel_reason
                     ELSE NULL
                 END,
                 cancel_requested_at = CASE
                     WHEN ?2 = 'cancel_requested' AND cancel_requested_at IS NULL THEN ?3
                     WHEN ?2 NOT IN ('cancel_requested', 'canceling', 'canceled') THEN NULL
                     ELSE cancel_requested_at
                 END,
                 cancel_started_at = CASE
                     WHEN ?2 = 'canceling' AND cancel_started_at IS NULL THEN ?3
                     WHEN ?2 NOT IN ('canceling', 'canceled') THEN NULL
                     ELSE cancel_started_at
                 END,
                 finished_at = CASE WHEN ?2 IN ('success', 'failed', 'canceled', 'blocked') THEN ?3 ELSE NULL END
             WHERE id = ?1",
            params![pipeline_id, status, now()],
        )?;
        Ok(())
    }

    pub fn mark_pipeline_cancel_requested(
        &self,
        pipeline_id: &str,
        reason: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "UPDATE pipeline_runs
             SET status = 'cancel_requested',
                 cancel_reason = ?2,
                 cancel_requested_at = COALESCE(cancel_requested_at, ?3),
                 finished_at = NULL
             WHERE id = ?1",
            params![pipeline_id, reason, now()],
        )?;
        Ok(())
    }

    pub fn pipelines_requiring_recovery(
        &self,
    ) -> Result<Vec<PipelineRun>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT DISTINCT p.id, p.repo_id, p.workflow_id, p.workflow_version_id, p.trigger_type, p.trigger_ref, p.commit_sha, p.status, p.started_at, p.cancel_reason, p.cancel_requested_at, p.cancel_started_at, p.finished_at
             FROM pipeline_runs p
             LEFT JOIN job_runs j ON j.pipeline_run_id = p.id
             WHERE p.status IN ('running', 'cancel_requested', 'canceling')
             OR j.status IN ('pending', 'running', 'cancel_requested', 'canceling')",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(PipelineRun {
                id: row.get(0)?,
                repo_id: row.get(1)?,
                workflow_id: row.get(2)?,
                workflow_version_id: row.get(3)?,
                trigger_type: row.get(4)?,
                trigger_ref: row.get(5)?,
                commit_sha: row.get(6)?,
                status: row.get(7)?,
                started_at: row.get(8)?,
                cancel_reason: row.get(9)?,
                cancel_requested_at: row.get(10)?,
                cancel_started_at: row.get(11)?,
                finished_at: row.get(12)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn insert_server_artifact(
        &self,
        artifact: &crate::artifacts::PendingArtifact,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO server_artifacts (id, scope_type, scope_id, artifact_name, sha256, size_bytes, storage_path, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(scope_type, scope_id, artifact_name) DO UPDATE SET
                sha256 = excluded.sha256,
                size_bytes = excluded.size_bytes,
                storage_path = excluded.storage_path,
                created_at = excluded.created_at",
            params![
                artifact.id,
                artifact.scope_type,
                artifact.scope_id,
                artifact.artifact_name,
                artifact.sha256,
                artifact.size_bytes,
                artifact.storage_path,
                now()
            ],
        )?;
        Ok(artifact.id.clone())
    }

    pub fn get_server_artifact(
        &self,
        scope_type: &str,
        scope_id: &str,
        artifact_name: &str,
    ) -> Result<Option<ServerArtifact>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Ok(conn.query_row(
            "SELECT id, scope_type, scope_id, artifact_name, sha256, size_bytes, storage_path, created_at
             FROM server_artifacts WHERE scope_type = ?1 AND scope_id = ?2 AND artifact_name = ?3",
            params![scope_type, scope_id, artifact_name],
            |row| {
                Ok(ServerArtifact {
                    id: row.get(0)?,
                    scope_type: row.get(1)?,
                    scope_id: row.get(2)?,
                    artifact_name: row.get(3)?,
                    sha256: row.get(4)?,
                    size_bytes: row.get(5)?,
                    storage_path: row.get(6)?,
                    created_at: row.get(7)?,
                })
            },
        ).optional()?)
    }

    pub fn get_server_artifact_by_id(
        &self,
        artifact_id: &str,
    ) -> Result<Option<ServerArtifact>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Ok(conn.query_row(
            "SELECT id, scope_type, scope_id, artifact_name, sha256, size_bytes, storage_path, created_at
             FROM server_artifacts WHERE id = ?1",
            [artifact_id],
            |row| {
                Ok(ServerArtifact {
                    id: row.get(0)?,
                    scope_type: row.get(1)?,
                    scope_id: row.get(2)?,
                    artifact_name: row.get(3)?,
                    sha256: row.get(4)?,
                    size_bytes: row.get(5)?,
                    storage_path: row.get(6)?,
                    created_at: row.get(7)?,
                })
            },
        ).optional()?)
    }

    pub fn list_recent_produced_artifacts(
        &self,
        limit: usize,
    ) -> Result<Vec<ProducedArtifact>, Box<dyn std::error::Error>> {
        let limit = i64::try_from(limit.min(500))?;
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT sa.id, sa.artifact_name, sa.sha256, sa.size_bytes, sa.created_at,
                    repo.id, repo.name, w.name, p.id, jr.id, r.name,
                    jr.runner_job_name, p.commit_sha, p.trigger_ref
             FROM server_artifacts sa
             JOIN job_run_artifacts jra ON jra.server_artifact_id = sa.id
             JOIN job_runs jr ON jr.id = jra.job_run_id
             JOIN pipeline_runs p ON p.id = jr.pipeline_run_id
             JOIN repos repo ON repo.id = p.repo_id
             JOIN workflows w ON w.id = p.workflow_id
             JOIN runners r ON r.id = jr.runner_id
             WHERE sa.scope_type = 'job_output'
               AND jra.artifact_role = 'output'
               AND jra.output_type = 'artifact'
             ORDER BY sa.created_at DESC, sa.id DESC
             LIMIT ?1",
        )?;
        let rows = stmt.query_map([limit], |row| {
            Ok(ProducedArtifact {
                id: row.get(0)?,
                artifact_name: row.get(1)?,
                sha256: row.get(2)?,
                size_bytes: row.get(3)?,
                created_at: row.get(4)?,
                repo_id: row.get(5)?,
                repo_name: row.get(6)?,
                workflow_name: row.get(7)?,
                pipeline_run_id: row.get(8)?,
                job_run_id: row.get(9)?,
                runner_name: row.get(10)?,
                runner_job_name: row.get(11)?,
                commit_sha: row.get(12)?,
                trigger_ref: row.get(13)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn list_recent_promotable_artifacts(
        &self,
        repo_id: &str,
        source_runner_id: &str,
        source_job_name: &str,
        output_name: &str,
        limit: usize,
    ) -> Result<Vec<PromotableArtifact>, Box<dyn std::error::Error>> {
        let limit = i64::try_from(limit.min(100))?;
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT sa.id, sa.sha256, sa.size_bytes, sa.created_at,
                    p.id, w.name, jr.id, jr.runner_id, r.name,
                    jr.runner_job_name, jra.artifact_name, p.commit_sha, p.trigger_ref
             FROM job_run_artifacts jra
             JOIN server_artifacts sa ON sa.id = jra.server_artifact_id
             JOIN job_runs jr ON jr.id = jra.job_run_id
             JOIN pipeline_runs p ON p.id = jr.pipeline_run_id
             JOIN workflows w ON w.id = p.workflow_id
             JOIN runners r ON r.id = jr.runner_id
             WHERE p.repo_id = ?1
               AND p.status = 'success'
               AND jr.status = 'success'
               AND jr.runner_id = ?2
               AND jr.runner_job_name = ?3
               AND jra.artifact_name = ?4
               AND jra.artifact_role = 'output'
               AND jra.output_type = 'artifact'
             ORDER BY p.finished_at DESC, jr.finished_at DESC, sa.id DESC
             LIMIT ?5",
        )?;
        let rows = stmt.query_map(
            params![
                repo_id,
                source_runner_id,
                source_job_name,
                output_name,
                limit
            ],
            |row| {
                Ok(PromotableArtifact {
                    server_artifact_id: row.get(0)?,
                    sha256: row.get(1)?,
                    size_bytes: row.get(2)?,
                    created_at: row.get(3)?,
                    pipeline_run_id: row.get(4)?,
                    workflow_name: row.get(5)?,
                    job_run_id: row.get(6)?,
                    runner_id: row.get(7)?,
                    runner_name: row.get(8)?,
                    runner_job_name: row.get(9)?,
                    output_name: row.get(10)?,
                    commit_sha: row.get(11)?,
                    trigger_ref: row.get(12)?,
                })
            },
        )?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }
}

fn now() -> String {
    Utc::now().to_rfc3339()
}

fn parse_output_metadata(raw: Option<String>) -> JobOutputMetadata {
    raw.and_then(|value| serde_json::from_str(&value).ok())
        .unwrap_or_default()
}

fn outcome_policy_from_row(
    row: &rusqlite::Row<'_>,
    index: usize,
) -> rusqlite::Result<WorkflowJobOutcomePolicy> {
    let value = row.get::<_, String>(index)?;
    WorkflowJobOutcomePolicy::parse(&value).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid job outcome policy {value}"),
            )),
        )
    })
}

fn parse_job_run_output_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<JobRunOutput> {
    let value_json: Option<String> = row.get(4)?;
    let value = value_json
        .map(|json| {
            serde_json::from_str::<Value>(&json).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    4,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })
        })
        .transpose()?;

    Ok(JobRunOutput {
        output_name: row.get(0)?,
        kind: row.get(1)?,
        runner_artifact_id: row.get(2)?,
        server_artifact_id: row.get(3)?,
        value,
        sha256: row.get(5)?,
        size_bytes: row.get(6)?,
    })
}

fn next_dispatch_idempotency_key() -> String {
    format!("dispatch_{}", Uuid::now_v7().simple())
}

fn parse_runner_input_type(value: String) -> rusqlite::Result<InputType> {
    InputType::parse(&value).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            format!("invalid runner input type {value}").into(),
        )
    })
}

fn parse_runner_output_type(value: String) -> rusqlite::Result<OutputType> {
    OutputType::parse(&value).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            format!("invalid runner output type {value}").into(),
        )
    })
}

fn parse_runner_concurrency(value: String) -> rusqlite::Result<Concurrency> {
    Concurrency::parse(&value).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            format!("invalid runner concurrency {value}").into(),
        )
    })
}

fn insert_runner_job_definition(
    tx: &rusqlite::Transaction<'_>,
    runner_job_id: &str,
    schema: &RunnerJobDefinition,
) -> Result<(), Box<dyn std::error::Error>> {
    for (input_name, input) in &schema.inputs {
        tx.execute(
            "INSERT INTO runner_job_inputs (id, runner_job_id, input_name, input_type, required, sensitive, max_length, pattern, max_json_bytes)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                Uuid::now_v7().to_string(),
                runner_job_id,
                input_name,
                input.kind.as_str(),
                input.required as i64,
                input.sensitive as i64,
                input.max_length.map(|value| value as i64),
                input.pattern.as_deref(),
                input.max_json_bytes.map(|value| value as i64),
            ],
        )?;
    }
    for (output_name, output) in &schema.outputs {
        tx.execute(
            "INSERT INTO runner_job_outputs (id, runner_job_id, output_name, output_type, required, output_path)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                Uuid::now_v7().to_string(),
                runner_job_id,
                output_name,
                output.kind.as_str(),
                output.required as i64,
                output.path.as_str(),
            ],
        )?;
    }
    Ok(())
}

fn load_runner_job_definition(
    conn: &Connection,
    runner_job_id: &str,
    job_name: &str,
    concurrency: &str,
    timeout_seconds: i64,
) -> Result<RunnerJobDefinition, Box<dyn std::error::Error>> {
    let mut input_stmt = conn.prepare(
        "SELECT input_name, input_type, required, sensitive, max_length, pattern, max_json_bytes FROM runner_job_inputs WHERE runner_job_id = ?1 ORDER BY input_name",
    )?;
    let inputs = input_stmt
        .query_map([runner_job_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                RunnerJobInputDefinition {
                    kind: parse_runner_input_type(row.get(1)?)?,
                    required: row.get::<_, i64>(2)? != 0,
                    sensitive: row.get::<_, i64>(3)? != 0,
                    max_length: row.get::<_, Option<i64>>(4)?.map(|value| value as usize),
                    pattern: row.get(5)?,
                    max_json_bytes: row.get::<_, Option<i64>>(6)?.map(|value| value as usize),
                },
            ))
        })?
        .collect::<Result<BTreeMap<_, _>, _>>()?;

    let mut output_stmt = conn.prepare(
        "SELECT output_name, output_type, required, output_path FROM runner_job_outputs WHERE runner_job_id = ?1 ORDER BY output_name",
    )?;
    let outputs = output_stmt
        .query_map([runner_job_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                RunnerJobOutputDefinition {
                    kind: parse_runner_output_type(row.get(1)?)?,
                    required: row.get::<_, i64>(2)? != 0,
                    path: row.get(3)?,
                },
            ))
        })?
        .collect::<Result<BTreeMap<_, _>, _>>()?;

    Ok(RunnerJobDefinition {
        name: job_name.to_string(),
        concurrency: parse_runner_concurrency(concurrency.to_string())?,
        timeout_seconds: timeout_seconds as u64,
        inputs,
        outputs,
    })
}

fn insert_workflow_version_job_schemas(
    tx: &rusqlite::Transaction<'_>,
    workflow_version_id: &str,
    schemas: &[RunnerJobDefinition],
) -> Result<(), Box<dyn std::error::Error>> {
    for (job_index, schema) in schemas.iter().enumerate() {
        let schema_id = Uuid::now_v7().to_string();
        tx.execute(
            "INSERT INTO workflow_version_job_schemas (id, workflow_version_id, job_index, job_name, concurrency, timeout_seconds)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                schema_id,
                workflow_version_id,
                job_index as i64,
                schema.name,
                schema.concurrency.as_str(),
                schema.timeout_seconds as i64
            ],
        )?;
        for (input_name, input) in &schema.inputs {
            tx.execute(
                "INSERT INTO workflow_version_job_schema_inputs (id, workflow_version_job_schema_id, input_name, input_type, required, sensitive, max_length, pattern, max_json_bytes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    Uuid::now_v7().to_string(),
                    schema_id,
                    input_name,
                    input.kind.as_str(),
                    input.required as i64,
                    input.sensitive as i64,
                    input.max_length.map(|value| value as i64),
                    input.pattern.as_deref(),
                    input.max_json_bytes.map(|value| value as i64),
                ],
            )?;
        }
        for (output_name, output) in &schema.outputs {
            tx.execute(
                "INSERT INTO workflow_version_job_schema_outputs (id, workflow_version_job_schema_id, output_name, output_type, required, output_path)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    Uuid::now_v7().to_string(),
                    schema_id,
                    output_name,
                    output.kind.as_str(),
                    output.required as i64,
                    output.path.as_str(),
                ],
            )?;
        }
    }
    Ok(())
}

fn load_workflow_version_job_schema(
    conn: &Connection,
    schema_id: &str,
    job_name: &str,
    concurrency: &str,
    timeout_seconds: i64,
) -> Result<RunnerJobDefinition, Box<dyn std::error::Error>> {
    let mut input_stmt = conn.prepare(
        "SELECT input_name, input_type, required, sensitive, max_length, pattern, max_json_bytes FROM workflow_version_job_schema_inputs WHERE workflow_version_job_schema_id = ?1 ORDER BY input_name",
    )?;
    let inputs = input_stmt
        .query_map([schema_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                RunnerJobInputDefinition {
                    kind: parse_runner_input_type(row.get(1)?)?,
                    required: row.get::<_, i64>(2)? != 0,
                    sensitive: row.get::<_, i64>(3)? != 0,
                    max_length: row.get::<_, Option<i64>>(4)?.map(|value| value as usize),
                    pattern: row.get(5)?,
                    max_json_bytes: row.get::<_, Option<i64>>(6)?.map(|value| value as usize),
                },
            ))
        })?
        .collect::<Result<BTreeMap<_, _>, _>>()?;

    let mut output_stmt = conn.prepare(
        "SELECT output_name, output_type, required, output_path FROM workflow_version_job_schema_outputs WHERE workflow_version_job_schema_id = ?1 ORDER BY output_name",
    )?;
    let outputs = output_stmt
        .query_map([schema_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                RunnerJobOutputDefinition {
                    kind: parse_runner_output_type(row.get(1)?)?,
                    required: row.get::<_, i64>(2)? != 0,
                    path: row.get(3)?,
                },
            ))
        })?
        .collect::<Result<BTreeMap<_, _>, _>>()?;

    Ok(RunnerJobDefinition {
        name: job_name.to_string(),
        concurrency: parse_runner_concurrency(concurrency.to_string())?,
        timeout_seconds: timeout_seconds as u64,
        inputs,
        outputs,
    })
}

fn user_role_from_row(row: &Row<'_>, index: usize) -> rusqlite::Result<UserRole> {
    let raw = row.get::<_, String>(index)?;
    UserRole::parse(&raw).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unknown user role: {raw}"),
            )),
        )
    })
}

fn apply_migrations(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL
        );",
    )?;

    for migration in MIGRATIONS {
        let applied = conn
            .query_row(
                "SELECT 1 FROM schema_migrations WHERE version = ?1",
                [migration.version],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if applied {
            continue;
        }

        let tx = conn.unchecked_transaction()?;
        tx.execute_batch(migration.sql)?;
        tx.execute(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (?1, ?2)",
            (migration.version, now()),
        )?;
        tx.commit()?;
    }

    Ok(())
}
