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

use std::io::{self, IsTerminal, Write};
use std::sync::Arc;

use animus_plugin_protocol::{
    error_codes, InitializeResult, PluginCapabilities, PluginInfo, RpcError, RpcRequest,
    RpcResponse, PLUGIN_KIND_SUBJECT_BACKEND, PROTOCOL_VERSION,
};
use animus_subject_default::backend::DefaultTaskBackend;
use animus_subject_default::config::DefaultTaskConfig;
use animus_subject_default::methods;
use animus_subject_protocol::SubjectBackend;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Stdout};
use tokio::sync::Mutex;

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
        tokio::spawn(async move {
            handle_request(request, info, capabilities, backend, stdout).await;
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

async fn handle_standard_subject(
    id: Option<Value>,
    method: &str,
    params: Option<Value>,
    backend: Arc<DefaultTaskBackend>,
) -> Option<RpcResponse> {
    let (_kind, verb) = subject_method_parts(method);
    match verb {
        "list" => {
            let filter = parse_or_default(params).unwrap_or_default();
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
            "health/check".into(),
            SUBJECT_KIND_TASK_CAPABILITY.into(),
        ],
        streaming: true,
        progress: false,
        cancellation: false,
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
