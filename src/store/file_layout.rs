//! Database open + schema bootstrap.
//!
//! The `tasks` table columns mirror the in-tree daemon's
//! `crates/orchestrator-core/src/workflow/state_manager.rs` definition
//! exactly: same column names, same types, same indexes. The `json`
//! column carries the full task payload, which the daemon's loader
//! deserializes via `serde_json::from_str::<OrchestratorTask>`.

use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::Connection;

/// Open (or create) the tasks database at `path`. Applies pragmas, creates
/// the `tasks` table if missing, and returns a ready connection.
pub fn open(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("failed to create parent directory for {}", path.display())
            })?;
        }
    }

    let conn = Connection::open(path)
        .with_context(|| format!("failed to open database at {}", path.display()))?;

    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         PRAGMA busy_timeout=5000;",
    )
    .context("failed to set SQLite pragmas")?;

    // Schema mirrors the in-tree daemon's `tasks` table exactly so the
    // daemon's existing `load_all_tasks`, `load_task`, `query_task_ids`
    // helpers can be re-pointed at this file unchanged.
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS tasks (
            id             TEXT PRIMARY KEY,
            status         TEXT NOT NULL,
            title          TEXT,
            priority       TEXT,
            task_type      TEXT,
            updated_at     TEXT,
            completed_at   TEXT,
            blocked_reason TEXT,
            blocked_at     TEXT,
            json           TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_task_status ON tasks(status);",
    )
    .context("failed to create tasks table")?;

    Ok(conn)
}
