//! Dispatch for the full TaskProvider surface — all 17 trait methods.
//!
//! `animus-plugin-runtime`'s `subject_backend_main` loop only handles the
//! standard `subject/{list,get,update,watch,schema}` verbs. The in-tree
//! `InTreeTaskSubjectBackend` that this plugin replaces sidesteps that
//! limitation by running its own JSON-RPC dispatcher; we do the same so the
//! daemon can drive `task/create`, `task/next`, `task/status`, etc. without
//! a protocol extension.
//!
//! The dispatcher returns `None` for any method outside the task surface so
//! the caller (the stdio loop in `main.rs`) can fall back to the standard
//! `subject_backend_main` handling.

use animus_plugin_protocol::{error_codes, RpcError};
use chrono::Utc;
use serde_json::{json, Map, Value};

use crate::store::crud::{build_new_task, stamp_update, Store};

/// Subject kind prefix this dispatcher claims.
pub const TASK_KIND: &str = "task";

/// Build the wire id (`task:TASK-001`) from a bare id (`TASK-001`).
pub fn add_kind_prefix(bare: &str) -> String {
    if bare.starts_with("task:") {
        bare.to_string()
    } else {
        format!("task:{bare}")
    }
}

/// Strip the `task:` prefix from a wire id, returning the bare id.
/// Inputs without the prefix are returned unchanged so callers can pass
/// either form.
pub fn strip_kind_prefix(wire: &str) -> String {
    wire.strip_prefix("task:")
        .map(str::to_string)
        .unwrap_or_else(|| wire.to_string())
}

/// Try to dispatch `method` against the task surface. Returns
/// `Some(Result)` when the method is recognized (success or error), or
/// `None` when it should fall through to the standard handler.
pub async fn try_dispatch(
    store: &Store,
    method: &str,
    params: Option<Value>,
) -> Option<Result<Value, RpcError>> {
    let verb = method.strip_prefix("task/")?;
    let result = match verb {
        "list" => list(store, params),
        "list_filtered" => list_filtered(store, params),
        "list_prioritized" => list_prioritized(store, params),
        "get" => get(store, params),
        "create" => create(store, params),
        "update" => update(store, params),
        "replace" => replace(store, params),
        "delete" => delete(store, params),
        "statistics" => statistics(store, params),
        "assign" => assign(store, params),
        "status" => status(store, params),
        "next" => next(store, params),
        "add_checklist_item" => add_checklist_item(store, params),
        "update_checklist_item" => update_checklist_item(store, params),
        "add_dependency" => add_dependency(store, params),
        "remove_dependency" => remove_dependency(store, params),
        // schema + watch are handled by the standard SubjectBackend
        // surface in `backend.rs`; fall through.
        _ => return None,
    };
    Some(result)
}

// =====================================================================
// List / get
// =====================================================================

fn list(store: &Store, params: Option<Value>) -> Result<Value, RpcError> {
    let limit = params
        .as_ref()
        .and_then(|p| p.get("limit"))
        .and_then(Value::as_u64)
        .map(|n| n as usize);
    let status_filter = first_status(&params);
    let tasks = store
        .list_all()
        .map_err(|e| internal(format!("task list failed: {e}")))?;
    let filtered: Vec<Value> = tasks
        .into_iter()
        .filter(|t| match status_filter.as_deref() {
            Some(want) => t.get("status").and_then(Value::as_str) == Some(want),
            None => true,
        })
        .take(limit.unwrap_or(usize::MAX))
        .map(|t| subject_view(&t))
        .collect();
    Ok(json!({ "subjects": filtered, "next_cursor": Value::Null }))
}

fn list_filtered(store: &Store, params: Option<Value>) -> Result<Value, RpcError> {
    // Same surface as `list` — kept distinct on the wire so callers can
    // be explicit about filter intent, matching the TaskProvider trait.
    list(store, params)
}

fn list_prioritized(store: &Store, _params: Option<Value>) -> Result<Value, RpcError> {
    let mut tasks = store
        .list_all()
        .map_err(|e| internal(format!("task list failed: {e}")))?;
    tasks.sort_by_key(|task| std::cmp::Reverse(priority_rank(task)));
    let view: Vec<Value> = tasks.iter().map(subject_view).collect();
    Ok(json!({ "subjects": view, "next_cursor": Value::Null }))
}

fn get(store: &Store, params: Option<Value>) -> Result<Value, RpcError> {
    let id = require_id(&params)?;
    let task = store
        .get(&id)
        .map_err(|e| not_found(format!("task {id} not found: {e}")))?;
    Ok(subject_view(&task))
}

fn next(store: &Store, _params: Option<Value>) -> Result<Value, RpcError> {
    let mut tasks = store
        .list_all()
        .map_err(|e| internal(format!("task list failed: {e}")))?;
    tasks.retain(|t| {
        matches!(
            t.get("status").and_then(Value::as_str),
            Some("ready" | "backlog" | "in-progress")
        )
    });
    tasks.sort_by_key(|task| std::cmp::Reverse(priority_rank(task)));
    Ok(tasks.first().map(subject_view).unwrap_or(Value::Null))
}

fn statistics(store: &Store, _params: Option<Value>) -> Result<Value, RpcError> {
    let tasks = store
        .list_all()
        .map_err(|e| internal(format!("task list failed: {e}")))?;
    let total = tasks.len();
    let mut by_status: Map<String, Value> = Map::new();
    let mut by_priority: Map<String, Value> = Map::new();
    let mut in_progress = 0usize;
    let mut blocked = 0usize;
    let mut completed = 0usize;
    for task in &tasks {
        let status = task
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("backlog");
        *by_status.entry(status.to_string()).or_insert(json!(0)) =
            json!(by_status.get(status).and_then(Value::as_u64).unwrap_or(0) + 1);
        let priority = task
            .get("priority")
            .and_then(Value::as_str)
            .unwrap_or("medium");
        *by_priority.entry(priority.to_string()).or_insert(json!(0)) = json!(
            by_priority
                .get(priority)
                .and_then(Value::as_u64)
                .unwrap_or(0)
                + 1
        );
        match status {
            "in-progress" | "in_progress" => in_progress += 1,
            "blocked" => blocked += 1,
            "completed" | "done" | "cancelled" => completed += 1,
            _ => {}
        }
    }
    Ok(json!({
        "total": total,
        "by_status": by_status,
        "by_priority": by_priority,
        "in_progress": in_progress,
        "blocked": blocked,
        "completed": completed,
    }))
}

// =====================================================================
// Create / update / replace / delete
// =====================================================================

fn create(store: &Store, params: Option<Value>) -> Result<Value, RpcError> {
    let params = params.unwrap_or_else(|| Value::Object(Map::new()));
    let title = params
        .get("title")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("task create requires --title"))?
        .to_string();
    let description = params
        .get("body")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let priority = params
        .get("priority")
        .and_then(Value::as_str)
        .map(canonical_priority);
    let task_type = params
        .get("task_type")
        .and_then(Value::as_str)
        .map(str::to_string);
    let tags = string_array(&params, "labels");
    let linked_requirements = string_array(&params, "linked_requirements");
    let linked_architecture_entities = string_array(&params, "linked_architecture_entities");
    let created_by = params
        .get("created_by")
        .and_then(Value::as_str)
        .map(str::to_string);

    let id = store
        .next_id()
        .map_err(|e| internal(format!("id generation failed: {e}")))?;
    let now = Utc::now().to_rfc3339();
    let mut task = build_new_task(
        id,
        title,
        description,
        priority,
        task_type,
        tags,
        created_by,
        linked_requirements,
        linked_architecture_entities,
        &now,
    );

    if let Some(status) = params.get("status").and_then(Value::as_str) {
        set_status_field(&mut task, status, &now);
    }

    store
        .upsert(&task)
        .map_err(|e| internal(format!("task create failed: {e}")))?;
    Ok(subject_view(&task))
}

fn update(store: &Store, params: Option<Value>) -> Result<Value, RpcError> {
    let id = require_id(&params)?;
    let patch = patch_value(&params);
    let mut task = store
        .get(&id)
        .map_err(|e| not_found(format!("task {id} not found: {e}")))?;
    apply_patch(&mut task, &patch);
    let now = Utc::now().to_rfc3339();
    stamp_update(&mut task, &now, "animus.subject.default");
    store
        .upsert(&task)
        .map_err(|e| internal(format!("task update failed: {e}")))?;
    Ok(subject_view(&task))
}

fn replace(store: &Store, params: Option<Value>) -> Result<Value, RpcError> {
    let params = params.unwrap_or(Value::Null);
    let task = params
        .get("task")
        .cloned()
        .ok_or_else(|| invalid("task replace requires --task <json>"))?;
    let id = task
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("task replace payload missing id"))?
        .to_string();
    let _ = id; // upsert already enforces id presence.
    store
        .upsert(&task)
        .map_err(|e| internal(format!("task replace failed: {e}")))?;
    Ok(subject_view(&task))
}

fn delete(store: &Store, params: Option<Value>) -> Result<Value, RpcError> {
    let id = require_id(&params)?;
    let removed = store
        .delete(&id)
        .map_err(|e| internal(format!("task delete failed: {e}")))?;
    Ok(json!({ "id": add_kind_prefix(&id), "deleted": removed }))
}

// =====================================================================
// Assign / status
// =====================================================================

fn assign(store: &Store, params: Option<Value>) -> Result<Value, RpcError> {
    let id = require_id(&params)?;
    let assignee = params
        .as_ref()
        .and_then(|p| p.get("assignee"))
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("task assign requires --assignee"))?
        .to_string();
    let mut task = store
        .get(&id)
        .map_err(|e| not_found(format!("task {id} not found: {e}")))?;
    if let Some(obj) = task.as_object_mut() {
        obj.insert("assignee".into(), json!(assignee));
    }
    let now = Utc::now().to_rfc3339();
    stamp_update(&mut task, &now, "animus.subject.default");
    store
        .upsert(&task)
        .map_err(|e| internal(format!("task assign failed: {e}")))?;
    Ok(subject_view(&task))
}

fn status(store: &Store, params: Option<Value>) -> Result<Value, RpcError> {
    let id = require_id(&params)?;
    let status_str = params
        .as_ref()
        .and_then(|p| p.get("status"))
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("task status requires --status"))?
        .to_string();
    let mut task = store
        .get(&id)
        .map_err(|e| not_found(format!("task {id} not found: {e}")))?;
    let now = Utc::now().to_rfc3339();
    set_status_field(&mut task, &status_str, &now);
    stamp_update(&mut task, &now, "animus.subject.default");
    store
        .upsert(&task)
        .map_err(|e| internal(format!("task set_status failed: {e}")))?;
    Ok(subject_view(&task))
}

// =====================================================================
// Checklist
// =====================================================================

fn add_checklist_item(store: &Store, params: Option<Value>) -> Result<Value, RpcError> {
    let id = require_id(&params)?;
    let description = params
        .as_ref()
        .and_then(|p| p.get("description"))
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("task add_checklist_item requires --description"))?
        .to_string();
    let updated_by = params
        .as_ref()
        .and_then(|p| p.get("updated_by"))
        .and_then(Value::as_str)
        .unwrap_or("animus.subject.default")
        .to_string();

    let mut task = store
        .get(&id)
        .map_err(|e| not_found(format!("task {id} not found: {e}")))?;
    let now = Utc::now().to_rfc3339();
    {
        let obj = task
            .as_object_mut()
            .ok_or_else(|| internal("task payload not an object"))?;
        let checklist = obj
            .entry("checklist".to_string())
            .or_insert_with(|| json!([]));
        let arr = checklist
            .as_array_mut()
            .ok_or_else(|| internal("task checklist field is not an array"))?;
        let next_idx = arr.len() + 1;
        let item_id = format!("CL-{next_idx:03}");
        arr.push(json!({
            "id": item_id,
            "description": description,
            "completed": false,
            "created_at": now,
            "updated_at": now,
        }));
    }
    stamp_update(&mut task, &now, &updated_by);
    store
        .upsert(&task)
        .map_err(|e| internal(format!("task add_checklist_item failed: {e}")))?;
    Ok(subject_view(&task))
}

fn update_checklist_item(store: &Store, params: Option<Value>) -> Result<Value, RpcError> {
    let id = require_id(&params)?;
    let item_id = params
        .as_ref()
        .and_then(|p| p.get("item_id"))
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("task update_checklist_item requires --item-id"))?
        .to_string();
    let completed = params
        .as_ref()
        .and_then(|p| p.get("completed"))
        .and_then(Value::as_bool)
        .ok_or_else(|| invalid("task update_checklist_item requires --completed"))?;
    let updated_by = params
        .as_ref()
        .and_then(|p| p.get("updated_by"))
        .and_then(Value::as_str)
        .unwrap_or("animus.subject.default")
        .to_string();

    let mut task = store
        .get(&id)
        .map_err(|e| not_found(format!("task {id} not found: {e}")))?;
    let now = Utc::now().to_rfc3339();
    {
        let arr = task
            .pointer_mut("/checklist")
            .and_then(Value::as_array_mut)
            .ok_or_else(|| invalid("task has no checklist"))?;
        let item = arr
            .iter_mut()
            .find(|i| i.get("id").and_then(Value::as_str) == Some(item_id.as_str()))
            .ok_or_else(|| not_found(format!("checklist item {item_id} not found")))?;
        if let Some(obj) = item.as_object_mut() {
            obj.insert("completed".into(), json!(completed));
            obj.insert("updated_at".into(), json!(now));
        }
    }
    stamp_update(&mut task, &now, &updated_by);
    store
        .upsert(&task)
        .map_err(|e| internal(format!("task update_checklist_item failed: {e}")))?;
    Ok(subject_view(&task))
}

// =====================================================================
// Dependencies
// =====================================================================

fn add_dependency(store: &Store, params: Option<Value>) -> Result<Value, RpcError> {
    let id = require_id(&params)?;
    let dependency_id = params
        .as_ref()
        .and_then(|p| p.get("dependency_id"))
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("task add_dependency requires --dependency-id"))?
        .to_string();
    let dependency_type = params
        .as_ref()
        .and_then(|p| p.get("dependency_type"))
        .and_then(Value::as_str)
        .unwrap_or("blocks")
        .to_string();
    let updated_by = params
        .as_ref()
        .and_then(|p| p.get("updated_by"))
        .and_then(Value::as_str)
        .unwrap_or("animus.subject.default")
        .to_string();

    let mut task = store
        .get(&id)
        .map_err(|e| not_found(format!("task {id} not found: {e}")))?;
    let now = Utc::now().to_rfc3339();
    {
        let obj = task
            .as_object_mut()
            .ok_or_else(|| internal("task payload not an object"))?;
        let deps = obj
            .entry("dependencies".to_string())
            .or_insert_with(|| json!([]));
        let arr = deps
            .as_array_mut()
            .ok_or_else(|| internal("dependencies field is not an array"))?;
        let bare = strip_kind_prefix(&dependency_id);
        let already = arr
            .iter()
            .any(|d| d.get("dependency_id").and_then(Value::as_str) == Some(bare.as_str()));
        if !already {
            arr.push(json!({
                "dependency_id": bare,
                "dependency_type": dependency_type,
                "added_at": now,
            }));
        }
    }
    stamp_update(&mut task, &now, &updated_by);
    store
        .upsert(&task)
        .map_err(|e| internal(format!("task add_dependency failed: {e}")))?;
    Ok(subject_view(&task))
}

fn remove_dependency(store: &Store, params: Option<Value>) -> Result<Value, RpcError> {
    let id = require_id(&params)?;
    let dependency_id = params
        .as_ref()
        .and_then(|p| p.get("dependency_id"))
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("task remove_dependency requires --dependency-id"))?
        .to_string();
    let updated_by = params
        .as_ref()
        .and_then(|p| p.get("updated_by"))
        .and_then(Value::as_str)
        .unwrap_or("animus.subject.default")
        .to_string();

    let mut task = store
        .get(&id)
        .map_err(|e| not_found(format!("task {id} not found: {e}")))?;
    let now = Utc::now().to_rfc3339();
    if let Some(arr) = task
        .pointer_mut("/dependencies")
        .and_then(Value::as_array_mut)
    {
        let bare = strip_kind_prefix(&dependency_id);
        arr.retain(|d| d.get("dependency_id").and_then(Value::as_str) != Some(bare.as_str()));
    }
    stamp_update(&mut task, &now, &updated_by);
    store
        .upsert(&task)
        .map_err(|e| internal(format!("task remove_dependency failed: {e}")))?;
    Ok(subject_view(&task))
}

// =====================================================================
// Subject view + patch translation
// =====================================================================

/// Project a task payload onto the cross-backend `Subject` JSON shape the
/// daemon reads. Mirrors the in-tree `task_to_subject_json` exactly so
/// the daemon's downstream consumers see the same wire form.
pub fn subject_view(task: &Value) -> Value {
    let id_bare = task.get("id").and_then(Value::as_str).unwrap_or("");
    let labels = task.get("tags").cloned().unwrap_or_else(|| json!([]));
    let native_status = task
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("backlog");
    let status_label = normalize_status(native_status);
    let priority_bucket = match task
        .get("priority")
        .and_then(Value::as_str)
        .unwrap_or("medium")
    {
        "critical" => 4,
        "high" => 3,
        "medium" => 2,
        "low" => 1,
        _ => 2,
    };
    let risk = task.get("risk").and_then(Value::as_str).unwrap_or("medium");
    let scope = task
        .get("scope")
        .and_then(Value::as_str)
        .unwrap_or("medium");
    let paused = task.get("paused").and_then(Value::as_bool).unwrap_or(false);
    let cancelled = task
        .get("cancelled")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let task_type = task
        .get("task_type")
        .and_then(Value::as_str)
        .unwrap_or("feature");
    let created_at = task
        .pointer("/metadata/created_at")
        .cloned()
        .unwrap_or(Value::Null);
    let updated_at = task
        .pointer("/metadata/updated_at")
        .cloned()
        .unwrap_or(Value::Null);

    json!({
        "id": add_kind_prefix(id_bare),
        "kind": TASK_KIND,
        "title": task.get("title").cloned().unwrap_or_else(|| json!("")),
        "description": task.get("description").cloned().unwrap_or_else(|| json!("")),
        "status": status_label,
        "native_status": native_status,
        "priority": priority_bucket,
        "labels": labels,
        "custom": {
            "type": task_type,
            "risk": risk,
            "scope": scope,
            "paused": paused,
            "cancelled": cancelled,
            "deadline": task.get("deadline").cloned().unwrap_or(Value::Null),
            "assignee": task.get("assignee").cloned().unwrap_or(Value::Null),
            "checklist": task.get("checklist").cloned().unwrap_or_else(|| json!([])),
            "dependencies": task.get("dependencies").cloned().unwrap_or_else(|| json!([])),
            "blocked_at": task.get("blocked_at").cloned().unwrap_or(Value::Null),
            "blocked_reason": task.get("blocked_reason").cloned().unwrap_or(Value::Null),
            "blocked_by": task.get("blocked_by").cloned().unwrap_or(Value::Null),
            "linked_requirements": task.get("linked_requirements").cloned().unwrap_or_else(|| json!([])),
            "linked_architecture_entities": task.get("linked_architecture_entities").cloned().unwrap_or_else(|| json!([])),
            "raw_status": native_status,
        },
        "created_at": created_at,
        "updated_at": updated_at,
    })
}

/// Map a native task status to a normalized `SubjectStatus` string. Keeps
/// the original value reachable via `subject.native_status` /
/// `subject.custom.raw_status` for clients that need the precise bucket.
pub fn normalize_status(native: &str) -> &'static str {
    match native {
        "ready" => "ready",
        "in-progress" | "in_progress" => "in-progress",
        "blocked" => "blocked",
        "done" | "completed" => "done",
        "cancelled" => "cancelled",
        // Backlog / draft / triage / unknown native states are surfaced
        // as `ready` so downstream dispatchers see them as eligible for
        // pickup. The original string is preserved in native_status.
        _ => "ready",
    }
}

/// Apply a `SubjectPatch` (or the in-tree shorthand `{patch: {...}}` /
/// raw top-level fields) to the in-place task payload. Supports the full
/// set of in-tree fields including dependency-aware metadata, deadlines,
/// paused/cancelled flags, and scope/risk.
pub fn apply_patch(task: &mut Value, patch: &Value) {
    if let Some(status) = patch.get("status").and_then(Value::as_str) {
        let now = Utc::now().to_rfc3339();
        set_status_field(task, status, &now);
    }
    if let Some(priority) = patch.get("priority").and_then(Value::as_str) {
        if let Some(obj) = task.as_object_mut() {
            obj.insert("priority".into(), json!(canonical_priority(priority)));
        }
    }
    if let Some(labels) = patch.get("labels").and_then(Value::as_array) {
        if let Some(obj) = task.as_object_mut() {
            obj.insert(
                "tags".into(),
                json!(labels
                    .iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect::<Vec<_>>()),
            );
        }
    }
    let labels_add: Vec<String> = patch
        .get("labels_add")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let labels_remove: Vec<String> = patch
        .get("labels_remove")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    if !labels_add.is_empty() || !labels_remove.is_empty() {
        if let Some(obj) = task.as_object_mut() {
            let mut tags: Vec<String> = obj
                .get("tags")
                .and_then(Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            tags.retain(|t| !labels_remove.contains(t));
            for label in labels_add {
                if !tags.contains(&label) {
                    tags.push(label);
                }
            }
            obj.insert("tags".into(), json!(tags));
        }
    }
    if let Some(title) = patch.get("title").and_then(Value::as_str) {
        if let Some(obj) = task.as_object_mut() {
            obj.insert("title".into(), json!(title));
        }
    }
    if let Some(body) = patch.get("body").and_then(Value::as_str) {
        if let Some(obj) = task.as_object_mut() {
            obj.insert("description".into(), json!(body));
        }
    }
    if let Some(description) = patch.get("description").and_then(Value::as_str) {
        if let Some(obj) = task.as_object_mut() {
            obj.insert("description".into(), json!(description));
        }
    }
    if let Some(assignee) = patch.get("assignee") {
        if let Some(obj) = task.as_object_mut() {
            // null clears, value sets.
            obj.insert("assignee".into(), assignee.clone());
        }
    }

    // SubjectPatch.custom carries the dependency-aware fields the audit
    // flagged as one-way only. Each is patchable here.
    if let Some(custom) = patch.get("custom").and_then(Value::as_object) {
        for (key, value) in custom {
            apply_custom_field(task, key, value);
        }
    }

    // Tolerate the flat shape too (`{paused: true}`, `{deadline: "..."}`
    // posted at the top level by older clients).
    for key in [
        "paused",
        "cancelled",
        "scope",
        "risk",
        "deadline",
        "task_type",
        "blocked_reason",
        "blocked_at",
        "blocked_by",
    ] {
        if let Some(value) = patch.get(key) {
            apply_custom_field(task, key, value);
        }
    }
}

fn apply_custom_field(task: &mut Value, key: &str, value: &Value) {
    let Some(obj) = task.as_object_mut() else {
        return;
    };
    match key {
        // Direct passthroughs to the task root.
        "paused" | "cancelled" | "scope" | "risk" | "deadline" | "task_type" | "blocked_reason"
        | "blocked_at" | "blocked_by" | "assignee" => {
            obj.insert(key.to_string(), value.clone());
        }
        "linked_requirements" | "linked_architecture_entities" => {
            obj.insert(key.to_string(), value.clone());
        }
        // null clears, anything else sets verbatim. Lets workflow YAML
        // patch arbitrary custom fields that downstream readers know
        // about even when this plugin doesn't.
        _ => {
            if value.is_null() {
                obj.remove(key);
            } else {
                obj.insert(key.to_string(), value.clone());
            }
        }
    }
}

fn set_status_field(task: &mut Value, status: &str, now: &str) {
    let Some(obj) = task.as_object_mut() else {
        return;
    };
    obj.insert("status".into(), json!(status));
    match status {
        "blocked" => {
            obj.insert("blocked_at".into(), json!(now));
        }
        "in-progress" | "in_progress" => {
            obj.insert("paused".into(), json!(false));
            obj.insert("blocked_at".into(), Value::Null);
            obj.insert("blocked_reason".into(), Value::Null);
            obj.insert("blocked_by".into(), Value::Null);
        }
        "ready" => {
            obj.insert("paused".into(), json!(false));
            obj.insert("blocked_at".into(), Value::Null);
            obj.insert("blocked_reason".into(), Value::Null);
            obj.insert("blocked_by".into(), Value::Null);
        }
        "completed" | "done" => {
            if let Some(metadata) = obj.get_mut("metadata").and_then(Value::as_object_mut) {
                metadata.insert("completed_at".into(), json!(now));
            }
        }
        "cancelled" => {
            obj.insert("cancelled".into(), json!(true));
        }
        _ => {}
    }
}

// =====================================================================
// Helpers
// =====================================================================

fn patch_value(params: &Option<Value>) -> Value {
    params
        .as_ref()
        .and_then(|p| p.get("patch"))
        .cloned()
        .unwrap_or_else(|| params.clone().unwrap_or(Value::Null))
}

fn require_id(params: &Option<Value>) -> Result<String, RpcError> {
    let wire_id = params
        .as_ref()
        .and_then(|p| p.get("id"))
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("task call requires --id"))?
        .trim()
        .to_string();
    if wire_id.is_empty() {
        return Err(invalid("task --id must not be empty"));
    }
    Ok(strip_kind_prefix(&wire_id))
}

fn string_array(params: &Value, key: &str) -> Vec<String> {
    params
        .get(key)
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn first_status(params: &Option<Value>) -> Option<String> {
    let p = params.as_ref()?;
    match p.get("status")? {
        Value::Array(arr) => arr.first().and_then(Value::as_str).map(str::to_string),
        Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

fn priority_rank(task: &Value) -> u8 {
    match task
        .get("priority")
        .and_then(Value::as_str)
        .unwrap_or("medium")
    {
        "critical" => 4,
        "high" => 3,
        "medium" => 2,
        "low" => 1,
        _ => 0,
    }
}

fn canonical_priority(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().as_str() {
        "critical" | "p0" => "critical".into(),
        "high" | "p1" => "high".into(),
        "medium" | "p2" => "medium".into(),
        "low" | "p3" => "low".into(),
        other => other.to_string(),
    }
}

fn invalid(msg: impl Into<String>) -> RpcError {
    RpcError {
        code: error_codes::INVALID_PARAMS,
        message: msg.into(),
        data: None,
    }
}

fn not_found(msg: impl Into<String>) -> RpcError {
    RpcError {
        code: error_codes::INVALID_PARAMS,
        message: msg.into(),
        data: Some(json!({ "category": "not_found" })),
    }
}

fn internal(msg: impl Into<String>) -> RpcError {
    RpcError {
        code: error_codes::INTERNAL_ERROR,
        message: msg.into(),
        data: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::crud::Store;
    use rusqlite::Connection;

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
    async fn create_then_get_round_trip() {
        let store = fresh_store();
        let created = try_dispatch(
            &store,
            "task/create",
            Some(json!({ "title": "Ship it", "body": "details", "priority": "high" })),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(created.get("kind").and_then(Value::as_str), Some("task"));
        let id = created
            .get("id")
            .and_then(Value::as_str)
            .unwrap()
            .to_string();
        assert!(id.starts_with("task:TASK-"));

        let fetched = try_dispatch(&store, "task/get", Some(json!({ "id": id })))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            fetched.get("title").and_then(Value::as_str),
            Some("Ship it")
        );
    }

    #[tokio::test]
    async fn status_transitions_clear_blocked_fields() {
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
            "task/status",
            Some(json!({ "id": id.clone(), "status": "blocked" })),
        )
        .await
        .unwrap()
        .unwrap();
        try_dispatch(
            &store,
            "task/status",
            Some(json!({ "id": id.clone(), "status": "in-progress" })),
        )
        .await
        .unwrap()
        .unwrap();
        let task = store.get(&strip_kind_prefix(&id)).unwrap();
        assert_eq!(
            task.get("blocked_at").cloned().unwrap_or(Value::Null),
            Value::Null
        );
    }

    #[tokio::test]
    async fn checklist_add_then_update_marks_completed() {
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
            "task/add_checklist_item",
            Some(json!({ "id": id.clone(), "description": "step 1" })),
        )
        .await
        .unwrap()
        .unwrap();
        try_dispatch(
            &store,
            "task/update_checklist_item",
            Some(json!({ "id": id.clone(), "item_id": "CL-001", "completed": true })),
        )
        .await
        .unwrap()
        .unwrap();
        let task = store.get(&strip_kind_prefix(&id)).unwrap();
        let completed = task
            .pointer("/checklist/0/completed")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        assert!(completed);
    }

    #[tokio::test]
    async fn dependency_add_and_remove_round_trip() {
        let store = fresh_store();
        let a = try_dispatch(&store, "task/create", Some(json!({ "title": "a" })))
            .await
            .unwrap()
            .unwrap();
        let b = try_dispatch(&store, "task/create", Some(json!({ "title": "b" })))
            .await
            .unwrap()
            .unwrap();
        let a_id = a.get("id").and_then(Value::as_str).unwrap().to_string();
        let b_id = b.get("id").and_then(Value::as_str).unwrap().to_string();
        try_dispatch(
            &store,
            "task/add_dependency",
            Some(json!({ "id": a_id.clone(), "dependency_id": b_id.clone(), "dependency_type": "blocks" })),
        )
        .await
        .unwrap()
        .unwrap();
        let after_add = store.get(&strip_kind_prefix(&a_id)).unwrap();
        let deps_len = after_add
            .pointer("/dependencies")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or(0);
        assert_eq!(deps_len, 1);

        try_dispatch(
            &store,
            "task/remove_dependency",
            Some(json!({ "id": a_id.clone(), "dependency_id": b_id })),
        )
        .await
        .unwrap()
        .unwrap();
        let after_remove = store.get(&strip_kind_prefix(&a_id)).unwrap();
        let after_len = after_remove
            .pointer("/dependencies")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or(0);
        assert_eq!(after_len, 0);
    }

    #[tokio::test]
    async fn update_patch_custom_fields() {
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
                "patch": { "custom": { "paused": true, "scope": "large", "risk": "high", "deadline": "2026-12-31" } }
            })),
        )
        .await
        .unwrap()
        .unwrap();
        let task = store.get(&strip_kind_prefix(&id)).unwrap();
        assert_eq!(task.get("paused").and_then(Value::as_bool), Some(true));
        assert_eq!(task.get("scope").and_then(Value::as_str), Some("large"));
        assert_eq!(task.get("risk").and_then(Value::as_str), Some("high"));
        assert_eq!(
            task.get("deadline").and_then(Value::as_str),
            Some("2026-12-31")
        );
    }

    #[tokio::test]
    async fn next_returns_highest_priority_open_task() {
        let store = fresh_store();
        try_dispatch(
            &store,
            "task/create",
            Some(json!({ "title": "low", "priority": "low" })),
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
        let next = try_dispatch(&store, "task/next", None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(next.get("id"), hi.get("id"));
    }

    #[tokio::test]
    async fn delete_removes_row() {
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
        try_dispatch(&store, "task/delete", Some(json!({ "id": id.clone() })))
            .await
            .unwrap()
            .unwrap();
        let missing = store.get(&strip_kind_prefix(&id));
        assert!(missing.is_err());
    }

    #[tokio::test]
    async fn unknown_method_returns_none() {
        let store = fresh_store();
        assert!(try_dispatch(&store, "subject/list", None).await.is_none());
        assert!(try_dispatch(&store, "issue/list", None).await.is_none());
    }
}
