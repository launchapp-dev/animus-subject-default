//! Default task subject backend for Animus.
//!
//! This plugin implements the full TaskProvider trait surface as a stdio
//! JSON-RPC subject backend so the in-tree InTreeTaskSubjectBackend (defined
//! in `orchestrator-daemon-runtime/src/inproc_subject_backend.rs`) can be
//! removed in favor of a discoverable, swappable plugin.
//!
//! # Wire surface
//!
//! Beyond the standard `subject/{list,get,update,watch,schema}` verbs that
//! `animus-plugin-runtime` dispatches via [`backend::DefaultTaskBackend`],
//! the plugin's [`main`](../animus_subject_default/index.html) binary
//! intercepts these task-only verbs on the raw RPC stream:
//!
//! - `task/list` / `task/list_filtered` / `task/list_prioritized`
//! - `task/get`
//! - `task/create` / `task/update` / `task/replace` / `task/delete`
//! - `task/statistics`
//! - `task/assign`
//! - `task/status`
//! - `task/next`
//! - `task/add_checklist_item` / `task/update_checklist_item`
//! - `task/add_dependency` / `task/remove_dependency`
//!
//! All other methods fall through to the standard `subject_backend_main`
//! handler.
//!
//! # Data layout
//!
//! Tasks are persisted in a SQLite database whose `tasks` table mirrors the
//! schema the in-tree daemon uses (`id`, `status`, `title`, `priority`,
//! `task_type`, `updated_at`, `completed_at`, `blocked_reason`, `blocked_at`,
//! `json`), so existing daemon readers can be re-pointed at this database
//! without code changes. By default the file lives at
//! `<cwd>/.animus/subjects/tasks.db`; daemons launch plugins with the
//! project root as cwd, so the default resolves to
//! `<project_root>/.animus/subjects/tasks.db`. Override with
//! `ANIMUS_DEFAULT_TASK_DB_PATH`.
//!
//! Task ids are sequential and match the in-tree `BuiltinTaskProvider`
//! convention: `TASK-001`, `TASK-002`, … (zero-padded to width 3 by default).

pub mod backend;
pub mod config;
pub mod methods;
pub mod store;
