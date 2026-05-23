# animus-subject-default

Default task subject backend for Animus — a stdio JSON-RPC plugin that
replaces the in-tree `InTreeTaskSubjectBackend` defined in
`crates/orchestrator-daemon-runtime/src/inproc_subject_backend.rs` in the
`launchapp-dev/animus-cli` repo.

## What it does

- Implements the full **TaskProvider** trait surface on the subject_backend
  wire (17 methods, listed below).
- Persists tasks in a SQLite database whose `tasks` table mirrors the
  daemon's `workflow.db` schema so existing readers continue to work
  unchanged.
- Generates sequential `TASK-NNN` ids that match the in-tree
  `next_sequential_id` helper (zero-padded to width 3 by default).
- Claims `subject_kind = "task"` so the daemon's `SubjectRouter` picks it
  up automatically and skips the in-tree adapter when installed.

## Wire surface (17 methods)

The plugin runs its own JSON-RPC dispatcher (mirroring the in-tree
`InTreeTaskSubjectBackend` pattern) so every TaskProvider trait method is
addressable on the wire, even the verbs that aren't part of the standard
`SubjectBackend` trait:

| Verb                                | Status |
|-------------------------------------|--------|
| `task/list`                         | covered |
| `task/list_filtered`                | covered |
| `task/list_prioritized`             | covered |
| `task/get`                          | covered |
| `task/create`                       | covered |
| `task/update`                       | covered (patch shape) |
| `task/replace`                      | covered (full overwrite) |
| `task/delete`                       | covered |
| `task/statistics`                   | covered |
| `task/assign`                       | covered |
| `task/status`                       | covered |
| `task/next`                         | covered |
| `task/add_checklist_item`           | covered |
| `task/update_checklist_item`        | covered |
| `task/add_dependency`               | covered |
| `task/remove_dependency`            | covered |
| `task/schema`                       | covered (delegates to SubjectBackend) |
| `task/watch`                        | covered via SubjectBackend stream |
| `health/check`                      | covered |

`subject/list`, `subject/get`, `subject/update`, `subject/schema` are
also accepted as legacy aliases.

## Patchable fields

`SubjectPatch.custom` is honored for the dependency-aware fields the
audit flagged as one-way in the in-tree adapter:

- `paused`, `cancelled`
- `scope`, `risk`
- `deadline`
- `assignee`
- `blocked_at`, `blocked_reason`, `blocked_by`
- `linked_requirements`, `linked_architecture_entities`

Top-level patch fields (`title`, `description`/`body`, `status`,
`priority`, `labels`, `task_type`) are also supported. `status` writes
flip the standard side-effects: blocked sets `blocked_at`; in-progress /
ready clears `paused` and `blocked_*`; completed stamps
`metadata.completed_at`; cancelled flips the `cancelled` boolean.

## Data layout

- Default DB path: `<project_root>/.animus/subjects/tasks.db` (override
  with `ANIMUS_DEFAULT_TASK_DB_PATH`).
- Table schema mirrors the in-tree `tasks` table:
  `(id, status, title, priority, task_type, updated_at, completed_at,
  blocked_reason, blocked_at, json)`.
- The `json` column is the canonical task payload; other columns are
  denormalized lookups the daemon's existing SQL indexes on.

### Migration from in-tree storage

The in-tree daemon writes the `tasks` table inside its primary
`workflow.db` at `~/.animus/<repo-scope>/workflow.db`. This plugin writes
to a separate database file at `<project_root>/.animus/subjects/tasks.db`
to avoid cross-process SQLite lock contention with the daemon's
long-lived connection.

To migrate existing tasks one-time:

```bash
sqlite3 ~/.animus/<repo-scope>/workflow.db ".dump tasks" \
  | sqlite3 <project_root>/.animus/subjects/tasks.db
```

After installing this plugin and confirming the daemon routes through
it, you can drop the `tasks` rows from `workflow.db`.

## Installation

```bash
animus plugin install launchapp-dev/animus-subject-default
```

The plugin claims `subject_kind = "task"`. The daemon's `SubjectRouter`
rejects duplicate-kind registrations at startup; the in-tree
`InTreeTaskSubjectBackend` checks for an external plugin claiming `task`
and steps aside, so installation is the only step required to switch
over.

## Configuration

| Env var                            | Default                                | Description |
|------------------------------------|----------------------------------------|-------------|
| `ANIMUS_DEFAULT_TASK_DB_PATH`      | `.animus/subjects/tasks.db`            | Database path (relative to cwd, which is the project root) |
| `ANIMUS_DEFAULT_TASK_ID_PREFIX`    | `TASK-`                                | Sequential id prefix |
| `ANIMUS_DEFAULT_TASK_ID_PAD`       | `3`                                    | Zero-pad width (e.g. `3` → `TASK-007`) |

## Build

```bash
cargo build --release
```

The release binary is `target/release/animus-subject-default`. The plugin
discovers configuration from the environment at startup.

## License

Elastic License 2.0. See `LICENSE`.
