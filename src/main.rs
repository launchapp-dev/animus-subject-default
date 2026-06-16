//! `animus-subject-default` binary: stdio JSON-RPC subject backend that
//! covers the full TaskProvider surface.
//!
//! `animus-plugin-runtime::subject_backend_main` only dispatches the
//! standard `list/get/update/watch/schema` verbs. The in-tree
//! `InTreeTaskSubjectBackend` (which this plugin replaces) sidesteps that
//! by running its own RPC loop with `task/create`, `task/next`,
//! `task/status`, and the rest of the TaskProvider trait surface. We do
//! the same here so the daemon can drive every TaskProvider method over
//! the plugin wire without protocol extensions.
//!
//! The loop intercepts `task/<verb>` methods via [`methods::try_dispatch`]
//! and falls back to the standard SubjectBackend handler (`subject_view`
//! semantics) for everything else.

use std::collections::HashMap;
use std::io::{self, IsTerminal, Write};
use std::sync::Arc;

use animus_plugin_protocol::{
    error_codes, InitializeResult, PluginCapabilities, PluginInfo, RpcError, RpcRequest,
    RpcResponse, PLUGIN_KIND_SUBJECT_BACKEND, PROTOCOL_VERSION,
};
use animus_subject_default::backend::DefaultTaskBackend;
use animus_subject_default::config::DefaultTaskConfig;
use animus_subject_default::methods;
use animus_subject_protocol::{
    SubjectBackend, METHOD_SUBJECT_UNWATCH, METHOD_SUBJECT_WATCH, NOTIFICATION_SUBJECT_CHANGED,
};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Stdout};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// Per-watch task registry: maps a `subject/watch` request id (stringified
/// JSON-RPC id) to the spawned task draining `backend.watch()` into
/// `subject/changed` notifications. `subject/unwatch { watch_id }` aborts and
/// removes the matching task so the backend's watch subscription is dropped
/// instead of leaking until the plugin process exits.
type WatchRegistry = Arc<Mutex<HashMap<String, JoinHandle<()>>>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let info = PluginInfo {
        name: env!("CARGO_PKG_NAME").into(),
        version: env!("CARGO_PKG_VERSION").into(),
        plugin_kind: PLUGIN_KIND_SUBJECT_BACKEND.into(),
        description: Some(env!("CARGO_PKG_DESCRIPTION").into()),
    };

    let capabilities = capabilities();

    if parse_manifest_flag() {
        print_manifest_and_exit(&info, &capabilities);
    }
    refuse_terminal_stdin(&info.name);

    let config = DefaultTaskConfig::from_env()?;
    let backend = Arc::new(DefaultTaskBackend::from_config(&config)?);
    let stdout = Arc::new(Mutex::new(tokio::io::stdout()));
    let watches: WatchRegistry = Arc::new(Mutex::new(HashMap::new()));
    let mut reader = BufReader::new(tokio::io::stdin());

    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let request: RpcRequest = match serde_json::from_str(trimmed) {
            Ok(req) => req,
            Err(_) => continue,
        };

        let info = info.clone();
        let capabilities = capabilities.clone();
        let backend = backend.clone();
        let stdout = stdout.clone();
        let watches = watches.clone();
        tokio::spawn(async move {
            handle_request(request, info, capabilities, backend, stdout, watches).await;
        });
    }

    Ok(())
}

async fn handle_request(
    request: RpcRequest,
    info: PluginInfo,
    capabilities: PluginCapabilities,
    backend: Arc<DefaultTaskBackend>,
    stdout: Arc<Mutex<Stdout>>,
    watches: WatchRegistry,
) {
    let id = request.id.clone();
    let method = request.method.clone();
    let params = request.params.clone();

    // Lifecycle methods always take precedence.
    let lifecycle = match method.as_str() {
        "initialize" => Some(Some(initialize_response(id.clone(), &info, &capabilities))),
        "initialized" => Some(None),
        "$/ping" => Some(Some(RpcResponse::ok(id.clone(), json!({})))),
        "shutdown" => Some(Some(RpcResponse::ok(id.clone(), json!({})))),
        "health/check" => {
            let health = backend.health().await;
            Some(Some(health_response(id.clone(), health)))
        }
        other if other.starts_with("$/") => Some(None),
        _ => None,
    };
    if let Some(maybe) = lifecycle {
        if let Some(resp) = maybe {
            write_frame(&stdout, &resp).await;
        }
        return;
    }

    // `task/delete` and `subject/delete` route through SubjectBackend::delete
    // so the response shape is the protocol DeleteSubjectResponse `{ok}` and
    // the watch broadcaster sees a Deleted event. We intercept BEFORE the
    // legacy task dispatcher so the kind-prefixed verb returns the protocol
    // shape v0.5.7 hosts expect when SubjectSchema.supports_delete is true.
    if method == "task/delete" || method == "subject/delete" {
        let resp = handle_subject_delete(id.clone(), params, backend.clone()).await;
        write_frame(&stdout, &resp).await;
        return;
    }

    // `subject/watch` (and the kind-prefixed `task/watch`): ack the request,
    // then spawn a task that drains `backend.watch()` into `subject/changed`
    // notifications, echoing the watch request id so the daemon can correlate
    // events to this subscription. The spawned task is tracked in `watches`
    // keyed by the stringified request id so `subject/unwatch` can cancel it.
    if method == METHOD_SUBJECT_WATCH || method == "task/watch" {
        let resp = handle_subject_watch(id.clone(), backend.clone(), stdout.clone(), watches).await;
        write_frame(&stdout, &resp).await;
        return;
    }

    // `subject/unwatch` (and `task/unwatch`): abort and drop the watch task
    // for the given `watch_id` so the backend's broadcast subscription is
    // released. Best-effort and idempotent — an unknown / already-gone
    // watch_id is a no-op success.
    if method == METHOD_SUBJECT_UNWATCH || method == "task/unwatch" {
        let resp = handle_subject_unwatch(id.clone(), params, watches).await;
        write_frame(&stdout, &resp).await;
        return;
    }
    // `subject/list` is the protocol-canonical verb: route through
    // `SubjectBackend::list` so the v0.5.7 `SubjectFilter` additions
    // (`native_status`, `dispatch_label`, `has_attachment_kind`) are
    // honored. `task/list` stays on the legacy `methods::list` path
    // (intercepted later by `methods::try_dispatch`) because that path
    // accepts the historical `{"status": "ready"}` shorthand and raw
    // native-status strings like `"backlog"` / `"completed"` that don't
    // match the `SubjectFilter` shape.
    //
    // TODO(codex-p2): unify task/list and subject/list filter semantics —
    // either teach `methods::list` to honor the new SubjectFilter
    // dimensions, or migrate the daemon's `SubjectRouter` to call
    // `subject/list` exclusively. Carrying two filter dialects on the
    // same backend is fragile.
    if method == "subject/list" {
        let resp = handle_standard_subject(id.clone(), &method, params, backend.clone())
            .await
            .unwrap_or_else(|| RpcResponse::ok(None, Value::Null));
        write_frame(&stdout, &resp).await;
        return;
    }

    // Try the extended TaskProvider surface first.
    if let Some(dispatch) = methods::try_dispatch(backend.store(), &method, params.clone()).await {
        let resp = match dispatch {
            Ok(value) => RpcResponse::ok(id, value),
            Err(error) => RpcResponse::err(id, error),
        };
        write_frame(&stdout, &resp).await;
        return;
    }

    // Fall back to the standard `subject_backend_main`-style dispatch
    // for the protocol-defined verbs (`<kind>/{list,get,update,watch,schema}`)
    // hosted by any other subject kind (or `subject/<verb>` legacy
    // callers). We re-implement the small subset we need inline rather
    // than calling subject_backend_main directly because that helper owns
    // its own loop.
    let resp = handle_standard_subject(id, &method, params, backend).await;
    if let Some(resp) = resp {
        write_frame(&stdout, &resp).await;
    }
}

async fn handle_subject_delete(
    id: Option<Value>,
    params: Option<Value>,
    backend: Arc<DefaultTaskBackend>,
) -> RpcResponse {
    let id_str = params
        .as_ref()
        .and_then(|p| p.get("id"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let Some(id_str) = id_str else {
        return RpcResponse::err(
            id,
            RpcError {
                code: error_codes::INVALID_PARAMS,
                message: "subject/delete requires id".into(),
                data: None,
            },
        );
    };
    let subject_id = animus_subject_protocol::SubjectId::new(id_str);
    match backend.delete(&subject_id).await {
        Ok(value) => RpcResponse::ok(id, serde_json::to_value(value).unwrap_or(Value::Null)),
        Err(error) => RpcResponse::err(id, error.into()),
    }
}

/// Stringify a JSON-RPC id for use as the watch-registry key. The daemon
/// allocates numeric ids and sends `subject/unwatch { watch_id: "<n>" }`
/// where the string is the decimal form of that number, so we normalize both
/// the watch id and the unwatch `watch_id` through the same representation.
fn watch_key(id: &Option<Value>) -> String {
    match id {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        Some(other) => other.to_string(),
        None => "null".to_string(),
    }
}

/// Start a `subject/watch` subscription: subscribe to the backend's change
/// broadcast, then spawn a task that forwards each event as a
/// `subject/changed` notification carrying `{ id: <watch req id>, event }`.
/// The task handle is stored in `watches` so `subject/unwatch` can abort it.
async fn handle_subject_watch(
    id: Option<Value>,
    backend: Arc<DefaultTaskBackend>,
    stdout: Arc<Mutex<Stdout>>,
    watches: WatchRegistry,
) -> RpcResponse {
    use tokio_stream::StreamExt;

    let key = watch_key(&id);
    let Some(mut stream) = backend.watch().await else {
        return RpcResponse::err(
            id,
            RpcError {
                code: error_codes::METHOD_NOT_SUPPORTED,
                message: "this backend does not support subject/watch".into(),
                data: None,
            },
        );
    };

    let echo_id = id.clone();
    let task = tokio::spawn(async move {
        while let Some(event) = stream.next().await {
            let notification = json!({
                "jsonrpc": "2.0",
                "method": NOTIFICATION_SUBJECT_CHANGED,
                "params": {
                    "id": echo_id,
                    "event": event,
                },
            });
            write_frame(&stdout, &notification).await;
        }
    });

    // Replace any prior task under the same key (a re-issued watch with the
    // same id), aborting the stale one so it cannot leak.
    if let Some(previous) = watches.lock().await.insert(key, task) {
        previous.abort();
    }

    RpcResponse::ok(id, json!({}))
}

/// Cancel a `subject/watch` subscription. Aborts and removes the registered
/// task for `params.watch_id`. Unknown ids are a no-op success so the daemon's
/// best-effort, fire-and-forget unwatch never errors on a stale subscription.
async fn handle_subject_unwatch(
    id: Option<Value>,
    params: Option<Value>,
    watches: WatchRegistry,
) -> RpcResponse {
    let watch_id = params
        .as_ref()
        .and_then(|p| p.get("watch_id"))
        .and_then(Value::as_str)
        .map(str::to_string);
    if let Some(watch_id) = watch_id {
        if let Some(task) = watches.lock().await.remove(&watch_id) {
            task.abort();
        }
    }
    RpcResponse::ok(id, json!({ "ok": true }))
}

async fn handle_standard_subject(
    id: Option<Value>,
    method: &str,
    params: Option<Value>,
    backend: Arc<DefaultTaskBackend>,
) -> Option<RpcResponse> {
    let (_kind, verb) = subject_method_parts(method);
    match verb {
        "list" => {
            // ao-cli's daemon control wraps the filter in a `{"filter": {...}}`
            // envelope (see crates/orchestrator-daemon-runtime/src/control/
            // dispatch.rs `params = json!({"filter": request.filter})`). Other
            // callers (and the protocol-export schemas) send the filter flat.
            // Accept both: prefer the unwrapped form when present, otherwise
            // deserialize the params directly.
            let filter = match params.as_ref() {
                Some(value) if value.get("filter").is_some() => value
                    .get("filter")
                    .and_then(|f| serde_json::from_value(f.clone()).ok())
                    .unwrap_or_default(),
                Some(_) => parse_or_default(params).unwrap_or_default(),
                None => Default::default(),
            };
            let res = backend.list(filter).await;
            Some(match res {
                Ok(value) => {
                    RpcResponse::ok(id, serde_json::to_value(value).unwrap_or(Value::Null))
                }
                Err(error) => RpcResponse::err(id, error.into()),
            })
        }
        "get" => {
            let id_str = params
                .as_ref()
                .and_then(|p| p.get("id"))
                .and_then(Value::as_str)
                .map(str::to_string);
            let Some(id_str) = id_str else {
                return Some(RpcResponse::err(
                    id,
                    RpcError {
                        code: error_codes::INVALID_PARAMS,
                        message: "subject/get requires id".into(),
                        data: None,
                    },
                ));
            };
            let subject_id = animus_subject_protocol::SubjectId::new(id_str);
            Some(match backend.get(&subject_id).await {
                Ok(value) => {
                    RpcResponse::ok(id, serde_json::to_value(value).unwrap_or(Value::Null))
                }
                Err(error) => RpcResponse::err(id, error.into()),
            })
        }
        "update" => {
            let id_str = params
                .as_ref()
                .and_then(|p| p.get("id"))
                .and_then(Value::as_str)
                .map(str::to_string);
            let Some(id_str) = id_str else {
                return Some(RpcResponse::err(
                    id,
                    RpcError {
                        code: error_codes::INVALID_PARAMS,
                        message: "subject/update requires id".into(),
                        data: None,
                    },
                ));
            };
            let subject_id = animus_subject_protocol::SubjectId::new(id_str);
            let patch: animus_subject_protocol::SubjectPatch = params
                .as_ref()
                .and_then(|p| p.get("patch"))
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            Some(match backend.update(&subject_id, patch).await {
                Ok(value) => {
                    RpcResponse::ok(id, serde_json::to_value(value).unwrap_or(Value::Null))
                }
                Err(error) => RpcResponse::err(id, error.into()),
            })
        }
        "schema" => {
            let schema = backend.schema();
            Some(RpcResponse::ok(
                id,
                serde_json::to_value(schema).unwrap_or(Value::Null),
            ))
        }
        // `delete` is intercepted earlier in `handle_request` for the
        // `task` / `subject` prefixes and routed through
        // `SubjectBackend::delete`. Other prefixes (e.g. `requirement/delete`)
        // fall through to the `METHOD_NOT_FOUND` arm below because this
        // plugin only advertises the `task` kind.
        //
        // watch is intentionally not implemented in this minimal stdio
        // loop; the broadcast stream is exposed via the SubjectBackend
        // trait for embedders that wrap us in a higher-level host.
        _ => Some(RpcResponse::err(
            id,
            RpcError {
                code: error_codes::METHOD_NOT_FOUND,
                message: format!("method '{method}' not implemented by animus-subject-default"),
                data: None,
            },
        )),
    }
}

fn parse_or_default<T: Default + serde::de::DeserializeOwned>(params: Option<Value>) -> Option<T> {
    match params {
        None => Some(T::default()),
        Some(value) => serde_json::from_value(value).ok(),
    }
}

fn subject_method_parts(method: &str) -> (&str, &str) {
    match method.split_once('/') {
        Some(("subject", verb)) => ("", verb),
        Some((prefix, verb)) => (prefix, verb),
        None => ("", method),
    }
}

fn capabilities() -> PluginCapabilities {
    PluginCapabilities {
        methods: vec![
            "task/list".into(),
            "task/list_filtered".into(),
            "task/list_prioritized".into(),
            "task/get".into(),
            "task/create".into(),
            "task/update".into(),
            "task/replace".into(),
            "task/delete".into(),
            "task/statistics".into(),
            "task/assign".into(),
            "task/status".into(),
            "task/next".into(),
            "task/add_checklist_item".into(),
            "task/update_checklist_item".into(),
            "task/add_dependency".into(),
            "task/remove_dependency".into(),
            "task/schema".into(),
            "task/watch".into(),
            "task/unwatch".into(),
            "subject/list".into(),
            "subject/get".into(),
            "subject/update".into(),
            "subject/delete".into(),
            "subject/schema".into(),
            "subject/watch".into(),
            "subject/unwatch".into(),
            "health/check".into(),
            SUBJECT_KIND_TASK_CAPABILITY.into(),
        ],
        streaming: true,
        progress: false,
        cancellation: false,
        projections: Vec::new(),
        subject_kinds: vec!["task".into()],
        mcp_tools: Vec::new(),
    }
}

/// Capability marker advertised so the daemon's plugin preflight at
/// `crates/orchestrator-core/src/plugin_preflight/mod.rs` recognizes this
/// plugin as covering the `kind=task` subject role. The preflight derives
/// subject_kinds by scanning the plugin's `capabilities` for entries
/// prefixed `subject_kind:`. Mirrors the `$ui/web` pattern used by
/// `launchapp-dev/animus-web-ui` to advertise extra capability markers.
const SUBJECT_KIND_TASK_CAPABILITY: &str = "subject_kind:task";

fn initialize_response(
    id: Option<Value>,
    info: &PluginInfo,
    capabilities: &PluginCapabilities,
) -> RpcResponse {
    let result = InitializeResult {
        protocol_version: PROTOCOL_VERSION.to_string(),
        plugin_info: info.clone(),
        capabilities: capabilities.clone(),
        kind_capabilities: std::collections::HashMap::new(),
    };
    match serde_json::to_value(result) {
        Ok(value) => RpcResponse::ok(id, value),
        Err(error) => RpcResponse::err(
            id,
            RpcError {
                code: error_codes::INTERNAL_ERROR,
                message: format!("encode initialize result: {error}"),
                data: None,
            },
        ),
    }
}

fn health_response(
    id: Option<Value>,
    result: Result<
        animus_plugin_protocol::HealthCheckResult,
        animus_subject_protocol::BackendError,
    >,
) -> RpcResponse {
    match result {
        Ok(health) => RpcResponse::ok(id, serde_json::to_value(health).unwrap_or(Value::Null)),
        Err(error) => RpcResponse::err(id, error.into()),
    }
}

async fn write_frame<T: serde::Serialize>(stdout: &Arc<Mutex<Stdout>>, frame: &T) {
    if let Ok(mut payload) = serde_json::to_string(frame) {
        payload.push('\n');
        let mut guard = stdout.lock().await;
        let _ = guard.write_all(payload.as_bytes()).await;
        let _ = guard.flush().await;
    }
}

fn parse_manifest_flag() -> bool {
    std::env::args()
        .skip(1)
        .any(|arg| arg == "--manifest" || arg == "-m")
}

fn print_manifest_and_exit(info: &PluginInfo, capabilities: &PluginCapabilities) -> ! {
    let mut advertised = capabilities.methods.clone();
    if !advertised.iter().any(|m| m == SUBJECT_KIND_TASK_CAPABILITY) {
        advertised.push(SUBJECT_KIND_TASK_CAPABILITY.to_string());
    }
    let manifest = json!({
        "name": info.name.clone(),
        "version": info.version.clone(),
        "plugin_kind": info.plugin_kind.clone(),
        "description": info.description.clone().unwrap_or_default(),
        "protocol_version": PROTOCOL_VERSION,
        "capabilities": advertised,
        "env_required": [
            {
                "name": "ANIMUS_DEFAULT_TASK_DB_PATH",
                "description": "Path to the tasks SQLite database.",
                "required": false
            },
            {
                "name": "ANIMUS_DEFAULT_TASK_ID_PREFIX",
                "description": "Sequential task id prefix.",
                "required": false
            },
            {
                "name": "ANIMUS_DEFAULT_TASK_ID_PAD",
                "description": "Zero-padding width for sequential task ids.",
                "required": false
            }
        ]
    });
    let mut stdout = io::stdout().lock();
    let _ = writeln!(
        stdout,
        "{}",
        serde_json::to_string(&manifest).expect("serialize manifest")
    );
    let _ = stdout.flush();
    std::process::exit(0);
}

fn refuse_terminal_stdin(plugin_name: &str) {
    if io::stdin().is_terminal() {
        eprintln!("{plugin_name} is a STDIO plugin; pipe JSON-RPC on stdin or pass --manifest");
        std::process::exit(2);
    }
}
