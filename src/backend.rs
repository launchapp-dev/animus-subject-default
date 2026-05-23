//! `DefaultTaskBackend` — `SubjectBackend` impl for the standard wire
//! verbs (`list`, `get`, `update`, `watch`, `schema`).
//!
//! The full TaskProvider surface (`create`, `next`, `status`,
//! `add_checklist_item`, etc.) is handled by [`crate::methods`] which
//! the binary's stdio loop intercepts before falling back to this
//! `SubjectBackend` impl via the standard runtime helpers. This split
//! lets future runtime versions surface the extra verbs through a
//! trait extension without code churn in the dispatcher.

use std::pin::Pin;
use std::sync::Arc;

use animus_plugin_protocol::{HealthCheckResult, HealthStatus};
use animus_subject_protocol::{
    BackendError, ChangeKind, CustomFieldKind, CustomFieldSpec, EventStream, Subject,
    SubjectBackend, SubjectChangedEvent, SubjectFilter, SubjectId, SubjectList, SubjectPatch,
    SubjectSchema, SubjectStatus,
};
use async_trait::async_trait;
use chrono::Utc;
use futures_core::Stream;
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::config::{DefaultTaskConfig, SUBJECT_KIND};
use crate::methods::{add_kind_prefix, apply_patch, strip_kind_prefix, subject_view};
use crate::store::crud::{stamp_update, Store};

/// Watch channel capacity. Subscribers more than this many events behind
/// are dropped and the daemon re-lists on the next tick.
const WATCH_CAPACITY: usize = 256;

/// `SubjectBackend` adapter wrapping a `Store`.
#[derive(Clone)]
pub struct DefaultTaskBackend {
    store: Store,
    events: Arc<broadcast::Sender<SubjectChangedEvent>>,
}

impl DefaultTaskBackend {
    /// Build a backend from a [`DefaultTaskConfig`]. The SQLite database
    /// is opened (creating it and its parent directories if needed) and
    /// the watch channel is primed.
    pub fn from_config(config: &DefaultTaskConfig) -> anyhow::Result<Self> {
        let store = Store::open(&config.db_path, &config.id_prefix, config.id_pad)?;
        let (tx, _rx) = broadcast::channel(WATCH_CAPACITY);
        Ok(Self {
            store,
            events: Arc::new(tx),
        })
    }

    /// Build a backend from a prebuilt store. Used by tests.
    pub fn from_store(store: Store) -> Self {
        let (tx, _rx) = broadcast::channel(WATCH_CAPACITY);
        Self {
            store,
            events: Arc::new(tx),
        }
    }

    /// Borrow the underlying store. Exposed for [`crate::methods`] dispatch.
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Broadcast a change event. Returns silently if no subscribers.
    pub fn broadcast(&self, change_kind: ChangeKind, subject: Subject) {
        let event = SubjectChangedEvent {
            id: subject.id.clone(),
            change_kind,
            subject,
            previous_native_status: None,
            previous_dispatch_label: None,
        };
        let _ = self.events.send(event);
    }
}

#[async_trait]
impl SubjectBackend for DefaultTaskBackend {
    async fn list(&self, filter: SubjectFilter) -> Result<SubjectList, BackendError> {
        let raw = self.store.list_all().map_err(BackendError::Other)?;
        let mut subjects = Vec::new();
        for task in raw {
            let subject_value = subject_view(&task);
            let subject = parse_subject(&subject_value)?;
            if !filter.status.is_empty() && !filter.status.contains(&subject.status) {
                continue;
            }
            if !filter.kind.is_empty() && !filter.kind.iter().any(|k| k == &subject.kind) {
                continue;
            }
            if !filter.labels_any.is_empty()
                && !subject.labels.iter().any(|l| filter.labels_any.contains(l))
            {
                continue;
            }
            if !filter.labels_all.is_empty()
                && !filter
                    .labels_all
                    .iter()
                    .all(|need| subject.labels.contains(need))
            {
                continue;
            }
            subjects.push(subject);
        }
        let limit = filter.limit.unwrap_or(u32::MAX) as usize;
        subjects.truncate(limit);
        Ok(SubjectList {
            subjects,
            next_cursor: None,
            fetched_at: Utc::now(),
        })
    }

    async fn get(&self, id: &SubjectId) -> Result<Subject, BackendError> {
        let bare = strip_kind_prefix(id.as_str());
        let task = self
            .store
            .get(&bare)
            .map_err(|_| BackendError::NotFound(id.to_string()))?;
        parse_subject(&subject_view(&task))
    }

    async fn update(&self, id: &SubjectId, patch: SubjectPatch) -> Result<Subject, BackendError> {
        let bare = strip_kind_prefix(id.as_str());
        let mut task = self
            .store
            .get(&bare)
            .map_err(|_| BackendError::NotFound(id.to_string()))?;
        let patch_json = subject_patch_to_value(&patch);
        apply_patch(&mut task, &patch_json);
        let now = Utc::now().to_rfc3339();
        stamp_update(&mut task, &now, "animus.subject.default");
        self.store.upsert(&task).map_err(BackendError::Other)?;
        let subject_value = subject_view(&task);
        let subject = parse_subject(&subject_value)?;
        let change_kind = if patch.status.is_some() {
            ChangeKind::StatusChanged
        } else {
            ChangeKind::Updated
        };
        self.broadcast(change_kind, subject.clone());
        Ok(subject)
    }

    async fn watch(&self) -> Option<EventStream> {
        let rx = self.events.subscribe();
        let stream = BroadcastStream::new(rx).filter_map(|item| item.ok());
        Some(Box::pin(stream) as Pin<Box<dyn Stream<Item = SubjectChangedEvent> + Send>>)
    }

    fn schema(&self) -> SubjectSchema {
        SubjectSchema {
            kinds: vec![SUBJECT_KIND.to_string()],
            status_values: vec![
                SubjectStatus::Ready,
                SubjectStatus::InProgress,
                SubjectStatus::Blocked,
                SubjectStatus::Done,
                SubjectStatus::Cancelled,
            ],
            supports_watch: true,
            supports_create: true,
            supports_pagination: false,
            native_status_values: vec![
                "backlog".into(),
                "ready".into(),
                "in-progress".into(),
                "blocked".into(),
                "completed".into(),
                "cancelled".into(),
            ],
            status_dispatch_hints: vec![],
            custom_fields: vec![
                CustomFieldSpec {
                    key: "type".into(),
                    kind: CustomFieldKind::String,
                    values: None,
                },
                CustomFieldSpec {
                    key: "risk".into(),
                    kind: CustomFieldKind::Enum,
                    values: Some(vec!["low".into(), "medium".into(), "high".into()]),
                },
                CustomFieldSpec {
                    key: "scope".into(),
                    kind: CustomFieldKind::Enum,
                    values: Some(vec!["small".into(), "medium".into(), "large".into()]),
                },
                CustomFieldSpec {
                    key: "paused".into(),
                    kind: CustomFieldKind::Bool,
                    values: None,
                },
                CustomFieldSpec {
                    key: "cancelled".into(),
                    kind: CustomFieldKind::Bool,
                    values: None,
                },
                CustomFieldSpec {
                    key: "deadline".into(),
                    kind: CustomFieldKind::Date,
                    values: None,
                },
                CustomFieldSpec {
                    key: "assignee".into(),
                    kind: CustomFieldKind::String,
                    values: None,
                },
                CustomFieldSpec {
                    key: "checklist".into(),
                    kind: CustomFieldKind::String,
                    values: None,
                },
                CustomFieldSpec {
                    key: "dependencies".into(),
                    kind: CustomFieldKind::String,
                    values: None,
                },
                CustomFieldSpec {
                    key: "blocked_at".into(),
                    kind: CustomFieldKind::Date,
                    values: None,
                },
                CustomFieldSpec {
                    key: "blocked_reason".into(),
                    kind: CustomFieldKind::String,
                    values: None,
                },
                CustomFieldSpec {
                    key: "blocked_by".into(),
                    kind: CustomFieldKind::String,
                    values: None,
                },
                CustomFieldSpec {
                    key: "linked_requirements".into(),
                    kind: CustomFieldKind::String,
                    values: None,
                },
                CustomFieldSpec {
                    key: "linked_architecture_entities".into(),
                    kind: CustomFieldKind::String,
                    values: None,
                },
            ],
        }
    }

    async fn health(&self) -> Result<HealthCheckResult, BackendError> {
        // A trivial select smoke-tests the database connection.
        match self.store.list_all() {
            Ok(_) => Ok(HealthCheckResult {
                status: HealthStatus::Healthy,
                uptime_ms: None,
                memory_usage_bytes: None,
                last_error: None,
            }),
            Err(e) => Ok(HealthCheckResult {
                status: HealthStatus::Unhealthy,
                uptime_ms: None,
                memory_usage_bytes: None,
                last_error: Some(e.to_string()),
            }),
        }
    }
}

fn parse_subject(value: &Value) -> Result<Subject, BackendError> {
    serde_json::from_value(value.clone())
        .map_err(|e| BackendError::Other(anyhow::anyhow!("subject decode failed: {e}")))
}

fn subject_patch_to_value(patch: &SubjectPatch) -> Value {
    let mut out = serde_json::Map::new();
    if let Some(status) = patch.status {
        let s = match status {
            SubjectStatus::Ready => "ready",
            SubjectStatus::InProgress => "in-progress",
            SubjectStatus::Blocked => "blocked",
            SubjectStatus::Done => "completed",
            SubjectStatus::Cancelled => "cancelled",
        };
        out.insert("status".into(), json!(s));
    }
    if let Some(assignee) = &patch.assignee {
        out.insert("assignee".into(), json!(assignee));
    }
    if !patch.labels_add.is_empty() || !patch.labels_remove.is_empty() {
        // SubjectPatch uses add/remove semantics; the in-tree dispatcher
        // accepts a flat `labels` overwrite. Surface both via custom so
        // downstream patch logic can layer in the merge.
        out.insert("labels_add".into(), json!(patch.labels_add.clone()));
        out.insert("labels_remove".into(), json!(patch.labels_remove.clone()));
    }
    if let Some(comment) = &patch.comment {
        out.insert("comment".into(), json!(comment));
    }
    if !patch.custom.is_empty() {
        out.insert(
            "custom".into(),
            Value::Object(patch.custom.clone().into_iter().collect()),
        );
    }
    // Daemon also addresses the wire id with `task:` prefix; the dispatcher
    // strips it when needed, so no transform here.
    let _ = add_kind_prefix; // keep import live across feature flag flips.
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::crud::build_new_task;
    use rusqlite::Connection;

    fn fresh_backend() -> DefaultTaskBackend {
        let conn = Connection::open_in_memory().unwrap();
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
        let store = Store::from_conn(conn, "TASK-", 3);
        let task = build_new_task(
            "TASK-001".into(),
            "seed".into(),
            String::new(),
            None,
            None,
            vec![],
            None,
            vec![],
            vec![],
            "2026-05-23T00:00:00Z",
        );
        store.upsert(&task).unwrap();
        DefaultTaskBackend::from_store(store)
    }

    #[tokio::test]
    async fn list_returns_seeded_task() {
        let backend = fresh_backend();
        let list = backend.list(SubjectFilter::default()).await.unwrap();
        assert_eq!(list.subjects.len(), 1);
        assert_eq!(list.subjects[0].kind, "task");
        assert_eq!(list.subjects[0].title, "seed");
    }

    #[tokio::test]
    async fn get_strips_kind_prefix() {
        let backend = fresh_backend();
        let subject = backend.get(&SubjectId::new("task:TASK-001")).await.unwrap();
        assert_eq!(subject.title, "seed");
    }

    #[tokio::test]
    async fn schema_claims_task_kind_and_create_support() {
        let backend = fresh_backend();
        let schema = backend.schema();
        assert_eq!(schema.kinds, vec!["task".to_string()]);
        assert!(schema.supports_create);
        assert!(schema.supports_watch);
    }
}
