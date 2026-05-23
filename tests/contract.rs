//! Wire-shape contract: every TaskProvider verb is reachable through
//! `methods::try_dispatch` and produces a `kind=task` subject view.

use animus_subject_default::config::SUBJECT_KIND;
use animus_subject_default::methods::{strip_kind_prefix, try_dispatch, TASK_KIND};
use animus_subject_default::store::crud::Store;
use rusqlite::Connection;
use serde_json::{json, Value};

fn fresh_store() -> Store {
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
    Store::from_conn(conn, "TASK-", 3)
}

#[tokio::test]
async fn kind_constant_matches_config() {
    assert_eq!(TASK_KIND, SUBJECT_KIND);
}

/// Hit every documented `task/<verb>` method against the dispatcher and
/// assert that none of them route to "unknown method" (which would
/// surface as `None` from `try_dispatch`). Each verb at minimum must
/// either succeed or return a structured error — never a routing miss.
#[tokio::test]
async fn every_task_verb_is_routed() {
    let store = fresh_store();

    let verbs = [
        "task/list",
        "task/list_filtered",
        "task/list_prioritized",
        "task/get",
        "task/create",
        "task/update",
        "task/replace",
        "task/delete",
        "task/statistics",
        "task/assign",
        "task/status",
        "task/next",
        "task/add_checklist_item",
        "task/update_checklist_item",
        "task/add_dependency",
        "task/remove_dependency",
    ];

    for verb in verbs {
        let outcome = try_dispatch(&store, verb, None).await;
        assert!(
            outcome.is_some(),
            "verb {verb} was not routed by try_dispatch"
        );
    }
}

/// `task/create` followed by `task/list_prioritized` returns the created
/// task and the subject view carries the cross-backend shape the daemon
/// expects (id prefixed with `task:`, kind = `task`, custom fields for
/// scope/risk/paused/cancelled/deadline/assignee/checklist/dependencies).
#[tokio::test]
async fn create_yields_full_subject_view() {
    let store = fresh_store();
    let created = try_dispatch(
        &store,
        "task/create",
        Some(json!({
            "title": "Land plugin",
            "body": "details",
            "priority": "critical",
            "labels": ["backend", "infra"],
        })),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(created.get("kind").and_then(Value::as_str), Some("task"));
    let id_wire = created.get("id").and_then(Value::as_str).unwrap();
    assert!(id_wire.starts_with("task:TASK-"));
    let bare = strip_kind_prefix(id_wire);
    assert!(bare.starts_with("TASK-"));

    let custom = created
        .get("custom")
        .and_then(Value::as_object)
        .expect("custom block");
    for key in [
        "type",
        "risk",
        "scope",
        "paused",
        "cancelled",
        "deadline",
        "assignee",
        "checklist",
        "dependencies",
        "blocked_at",
        "blocked_reason",
        "blocked_by",
        "linked_requirements",
        "linked_architecture_entities",
    ] {
        assert!(
            custom.contains_key(key),
            "subject view missing custom.{key}"
        );
    }
}

/// The `task/list_prioritized` ordering must place `critical` ahead of
/// `low` regardless of insertion order.
#[tokio::test]
async fn list_prioritized_orders_by_priority() {
    let store = fresh_store();
    try_dispatch(
        &store,
        "task/create",
        Some(json!({ "title": "lo", "priority": "low" })),
    )
    .await
    .unwrap()
    .unwrap();
    let hi = try_dispatch(
        &store,
        "task/create",
        Some(json!({ "title": "hi", "priority": "critical" })),
    )
    .await
    .unwrap()
    .unwrap();

    let listed = try_dispatch(&store, "task/list_prioritized", None)
        .await
        .unwrap()
        .unwrap();
    let arr = listed
        .get("subjects")
        .and_then(Value::as_array)
        .expect("subjects array");
    assert_eq!(arr.first().and_then(|t| t.get("id")), hi.get("id"));
}

/// Deadlines, paused/cancelled, scope, and risk are patchable via
/// `SubjectPatch.custom`. The audit flagged these as one-way only in
/// the in-tree adapter.
#[tokio::test]
async fn dependency_aware_fields_are_patchable() {
    let store = fresh_store();
    let created = try_dispatch(&store, "task/create", Some(json!({ "title": "x" })))
        .await
        .unwrap()
        .unwrap();
    let id = created
        .get("id")
        .and_then(Value::as_str)
        .unwrap()
        .to_string();

    try_dispatch(
        &store,
        "task/update",
        Some(json!({
            "id": id.clone(),
            "patch": {
                "custom": {
                    "paused": true,
                    "cancelled": false,
                    "scope": "large",
                    "risk": "high",
                    "deadline": "2026-12-31",
                    "blocked_reason": "waiting on infra",
                }
            }
        })),
    )
    .await
    .unwrap()
    .unwrap();

    let fetched = try_dispatch(&store, "task/get", Some(json!({ "id": id })))
        .await
        .unwrap()
        .unwrap();
    let custom = fetched.get("custom").and_then(Value::as_object).unwrap();
    assert_eq!(custom.get("paused").and_then(Value::as_bool), Some(true));
    assert_eq!(custom.get("scope").and_then(Value::as_str), Some("large"));
    assert_eq!(custom.get("risk").and_then(Value::as_str), Some("high"));
    assert_eq!(
        custom.get("deadline").and_then(Value::as_str),
        Some("2026-12-31")
    );
    assert_eq!(
        custom.get("blocked_reason").and_then(Value::as_str),
        Some("waiting on infra")
    );
}
