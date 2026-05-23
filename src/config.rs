//! Environment-driven configuration for `animus-subject-default`.

use std::path::PathBuf;

use anyhow::{anyhow, Result};

/// Path to the SQLite database backing the task store.
pub const ENV_DB_PATH: &str = "ANIMUS_DEFAULT_TASK_DB_PATH";

/// Override the sequential id prefix (default `TASK-`).
pub const ENV_ID_PREFIX: &str = "ANIMUS_DEFAULT_TASK_ID_PREFIX";

/// Override the sequential id zero-padding width (default `3`).
pub const ENV_ID_PAD: &str = "ANIMUS_DEFAULT_TASK_ID_PAD";

/// Default location relative to the daemon-supplied cwd (the project root).
/// Matches `<project_root>/.animus/subjects/tasks.db`.
pub const DEFAULT_DB_PATH: &str = ".animus/subjects/tasks.db";

/// Default sequential id prefix, matching the in-tree
/// `BuiltinTaskProvider`.
pub const DEFAULT_ID_PREFIX: &str = "TASK-";

/// Default zero-pad width for sequential ids, matching the in-tree
/// `next_sequential_id` helper.
pub const DEFAULT_ID_PAD: usize = 3;

/// Subject kind this backend claims.
pub const SUBJECT_KIND: &str = "task";

/// Runtime configuration for the default task backend.
#[derive(Debug, Clone)]
pub struct DefaultTaskConfig {
    /// Filesystem path to the SQLite database. Created on first use.
    pub db_path: PathBuf,
    /// Sequential id prefix (`TASK-` by default).
    pub id_prefix: String,
    /// Zero-padding width for sequential ids (3 by default → `TASK-007`).
    pub id_pad: usize,
}

impl DefaultTaskConfig {
    /// Load configuration from environment variables, falling back to
    /// defaults that match the in-tree behavior.
    pub fn from_env() -> Result<Self> {
        let db_path = std::env::var(ENV_DB_PATH)
            .ok()
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DB_PATH));

        let id_prefix = std::env::var(ENV_ID_PREFIX)
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_ID_PREFIX.to_string());

        let id_pad = std::env::var(ENV_ID_PAD)
            .ok()
            .filter(|s| !s.is_empty())
            .map(|raw| {
                raw.parse::<usize>()
                    .map_err(|e| anyhow!("{ENV_ID_PAD} must be a non-negative integer: {e}"))
            })
            .transpose()?
            .unwrap_or(DEFAULT_ID_PAD);

        Ok(Self {
            db_path,
            id_prefix,
            id_pad,
        })
    }

    /// In-process builder for tests.
    pub fn new(db_path: impl Into<PathBuf>) -> Self {
        Self {
            db_path: db_path.into(),
            id_prefix: DEFAULT_ID_PREFIX.to_string(),
            id_pad: DEFAULT_ID_PAD,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_in_tree_convention() {
        let cfg = DefaultTaskConfig::new("/tmp/tasks.db");
        assert_eq!(cfg.id_prefix, "TASK-");
        assert_eq!(cfg.id_pad, 3);
    }
}
