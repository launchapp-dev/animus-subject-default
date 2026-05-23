//! SQLite-backed task store mirroring the in-tree `BuiltinTaskProvider`
//! data layout.
//!
//! The store opens a single SQLite database (default
//! `<cwd>/.animus/subjects/tasks.db`) with a `tasks` table whose columns
//! are identical to the daemon's `workflow.db` `tasks` table. Each row
//! carries a `json` blob holding the full task payload — the same shape
//! the in-tree `OrchestratorTask` produces — so existing daemon readers
//! can be re-pointed at this database without code changes.

pub mod crud;
pub mod file_layout;
pub mod id_gen;

pub use crud::Store;
pub use file_layout::open;
