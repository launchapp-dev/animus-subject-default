//! CRUD primitives over the `tasks` table.
//!
//! Task rows are stored as `(id, status, title, priority, task_type,
//! updated_at, completed_at, blocked_reason, blocked_at, json)` — the
//! same layout the in-tree daemon's `save_task` writes. The `json` column
//! is the canonical source of truth; the other columns are denormalized
//! lookups the daemon indexes on.
//!
//! We intentionally work with `serde_json::Value` task payloads instead
//! of importing the in-tree `OrchestratorTask` struct so this plugin
//! stays standalone. The shape we read and write is dictated by the
//! daemon's `OrchestratorTask` serde derive — see
//! `crates/protocol/src/orchestrator.rs` in `launchapp-dev/animus-cli`.

use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use parking_lot::Mutex;
use rusqlite::{params, Connection};
use serde_json::{json, Map, Value};

use super::file_layout::open;
use super::id_gen::next_sequential_id;

/// Process-wide SQLite store for task rows. Internally serialized via a
/// single `Mutex<Connection>` because rusqlite connections are not
/// `Sync`. The connection is opened with WAL + `busy_timeout=5000`, so
/// the lock is brief and tolerates concurrent readers via WAL semantics.
#[derive(Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
    id_prefix: String,
    id_pad: usize,
}

impl Store {
    /// Open the database at `path` and prepare it for use.
    pub fn open(path: &Path, id_prefix: impl Into<String>, id_pad: usize) -> Result<Self> {
        let conn = open(path)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            id_prefix: id_prefix.into(),
            id_pad,
        })
    }

    /// Build a store on top of an already-opened in-memory connection.
    /// Used by the test suite to keep tests hermetic.
    pub fn from_conn(conn: Connection, id_prefix: impl Into<String>, id_pad: usize) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn)),
            id_prefix: id_prefix.into(),
            id_pad,
        }
    }

    /// Return every task in the store, in stable id order.
    pub fn list_all(&self) -> Result<Vec<Value>> {
        let guard = self.conn.lock();
        let mut stmt = guard.prepare("SELECT json FROM tasks ORDER BY id")?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .filter_map(|r| r.ok())
            .filter_map(|raw| serde_json::from_str::<Value>(&raw).ok())
            .collect();
        Ok(rows)
    }

    /// Fetch one task by id.
    pub fn get(&self, id: &str) -> Result<Value> {
        let guard = self.conn.lock();
        let raw: String = guard
            .query_row("SELECT json FROM tasks WHERE id = ?1", params![id], |row| {
                row.get(0)
            })
            .map_err(|_| anyhow!("task not found: {id}"))?;
        Ok(serde_json::from_str(&raw)?)
    }

    /// Insert or replace a task row. The caller is responsible for the
    /// JSON shape — typically built from `build_new_task` /
    /// `apply_patch`.
    pub fn upsert(&self, task: &Value) -> Result<()> {
        let id = task
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("task json missing id"))?;
        let status = task
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("backlog");
        let title = task.get("title").and_then(Value::as_str);
        let priority = task.get("priority").and_then(Value::as_str);
        let task_type = task.get("task_type").and_then(Value::as_str);
        let updated_at = task.pointer("/metadata/updated_at").and_then(Value::as_str);
        let completed_at = task
            .pointer("/metadata/completed_at")
            .and_then(Value::as_str);
        let blocked_reason = task.get("blocked_reason").and_then(Value::as_str);
        let blocked_at = task.get("blocked_at").and_then(Value::as_str);
        let payload = serde_json::to_string(task)?;

        let guard = self.conn.lock();
        guard
            .execute(
                "INSERT OR REPLACE INTO tasks (
                    id, status, title, priority, task_type,
                    updated_at, completed_at, blocked_reason, blocked_at, json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    id,
                    status,
                    title,
                    priority,
                    task_type,
                    updated_at,
                    completed_at,
                    blocked_reason,
                    blocked_at,
                    payload
                ],
            )
            .with_context(|| format!("failed to upsert task {id}"))?;
        Ok(())
    }

    /// Delete the row with `id`. Returns true when a row was removed.
    pub fn delete(&self, id: &str) -> Result<bool> {
        let guard = self.conn.lock();
        let removed = guard.execute("DELETE FROM tasks WHERE id = ?1", params![id])?;
        Ok(removed > 0)
    }

    /// Return all existing task ids. Used by id generation.
    pub fn ids(&self) -> Result<Vec<String>> {
        let guard = self.conn.lock();
        let mut stmt = guard.prepare("SELECT id FROM tasks")?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Generate the next sequential id according to this store's prefix
    /// and pad width. Mirrors the in-tree `next_task_id` helper.
    pub fn next_id(&self) -> Result<String> {
        let existing = self.ids()?;
        Ok(next_sequential_id(
            existing.iter().map(String::as_str),
            &self.id_prefix,
            self.id_pad,
        ))
    }
}

/// Build a fresh task JSON payload shaped like the in-tree
/// `OrchestratorTask`. Only the fields necessary for an `Insert` are
/// populated; callers can layer additional fields via `apply_patch`
/// before the first `upsert`.
///
/// Status defaults to `"backlog"`, priority to `"medium"`, task_type to
/// `"feature"`, scope to `"medium"`, risk to `"medium"` — matching the
/// in-tree `TaskCreateInput` defaults observed in
/// `BuiltinTaskProvider::create`.
#[allow(clippy::too_many_arguments)]
pub fn build_new_task(
    id: String,
    title: String,
    description: String,
    priority: Option<String>,
    task_type: Option<String>,
    tags: Vec<String>,
    created_by: Option<String>,
    linked_requirements: Vec<String>,
    linked_architecture_entities: Vec<String>,
    now: &str,
) -> Value {
    let mut task = Map::new();
    task.insert("id".into(), json!(id));
    task.insert("title".into(), json!(title));
    task.insert("description".into(), json!(description));
    task.insert("status".into(), json!("backlog"));
    task.insert(
        "priority".into(),
        json!(priority.unwrap_or_else(|| "medium".into())),
    );
    task.insert(
        "task_type".into(),
        json!(task_type.unwrap_or_else(|| "feature".into())),
    );
    task.insert("scope".into(), json!("medium"));
    task.insert("risk".into(), json!("medium"));
    task.insert("tags".into(), json!(tags));
    task.insert("linked_requirements".into(), json!(linked_requirements));
    task.insert(
        "linked_architecture_entities".into(),
        json!(linked_architecture_entities),
    );
    task.insert("checklist".into(), json!([]));
    task.insert("dependencies".into(), json!([]));
    task.insert("assignee".into(), Value::Null);
    task.insert("paused".into(), json!(false));
    task.insert("cancelled".into(), json!(false));
    task.insert("blocked_at".into(), Value::Null);
    task.insert("blocked_reason".into(), Value::Null);
    task.insert("blocked_by".into(), Value::Null);
    task.insert("deadline".into(), Value::Null);

    let mut metadata = Map::new();
    metadata.insert("created_at".into(), json!(now));
    metadata.insert("updated_at".into(), json!(now));
    metadata.insert(
        "created_by".into(),
        json!(created_by.unwrap_or_else(|| "animus.subject.default".into())),
    );
    metadata.insert("updated_by".into(), json!("animus.subject.default"));
    metadata.insert("completed_at".into(), Value::Null);
    task.insert("metadata".into(), Value::Object(metadata));

    Value::Object(task)
}

/// Stamp the standard `metadata.updated_at` / `metadata.updated_by`
/// fields after a mutation.
pub fn stamp_update(task: &mut Value, now: &str, updated_by: &str) {
    if let Some(obj) = task.as_object_mut() {
        let metadata = obj
            .entry("metadata".to_string())
            .or_insert_with(|| Value::Object(Map::new()));
        if let Some(m) = metadata.as_object_mut() {
            m.insert("updated_at".into(), json!(now));
            m.insert("updated_by".into(), json!(updated_by));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn fresh_store() -> Store {
        let conn = Connection::open_in_memory().unwrap();
        // Mirror the file_layout schema so tests exercise the real shape.
        conn.execute_batch(
            "CREATE TABLE tasks (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                title TEXT,
                priority TEXT,
                task_type TEXT,
                updated_at TEXT,
                completed_at TEXT,
                blocked_reason TEXT,
                blocked_at TEXT,
                json TEXT NOT NULL
            );",
        )
        .unwrap();
        Store::from_conn(conn, "TASK-", 3)
    }

    #[test]
    fn upsert_and_get_round_trip() {
        let store = fresh_store();
        let task = build_new_task(
            "TASK-001".into(),
            "Ship".into(),
            "Body".into(),
            None,
            None,
            vec![],
            None,
            vec![],
            vec![],
            "2026-05-23T00:00:00Z",
        );
        store.upsert(&task).unwrap();
        let back = store.get("TASK-001").unwrap();
        assert_eq!(back.get("title").and_then(Value::as_str), Some("Ship"));
        assert_eq!(back.get("status").and_then(Value::as_str), Some("backlog"));
    }

    #[test]
    fn next_id_increments() {
        let store = fresh_store();
        let id1 = store.next_id().unwrap();
        assert_eq!(id1, "TASK-001");
        let t1 = build_new_task(
            id1.clone(),
            "a".into(),
            String::new(),
            None,
            None,
            vec![],
            None,
            vec![],
            vec![],
            "n",
        );
        store.upsert(&t1).unwrap();
        let id2 = store.next_id().unwrap();
        assert_eq!(id2, "TASK-002");
    }

    #[test]
    fn delete_returns_false_when_missing() {
        let store = fresh_store();
        assert!(!store.delete("TASK-999").unwrap());
    }
}
