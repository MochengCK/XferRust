//! MCP（Model Context Protocol）服务：把引擎能力暴露给任意 AI Agent。
//!
//! 设计要点：
//! - **一层薄映射**：MCP 工具直接落到原生 JSON-RPC（`task.*` / `engine.*`），
//!   与界面走同一条协议，不另写一份下载逻辑；
//! - **传输**：Streamable HTTP（`POST /mcp`），JSON-RPC 2.0 单发请求；
//!   另给一个 `/health` 供界面判断服务是否在跑；
//! - **鉴权**：与 JSON-RPC 服务共用 `rpc-secret`，客户端用
//!   `Authorization: Bearer <secret>`；未配置 secret 时只监听回环地址，等同本机放行；
//! - **stdio**：由 `xferrust mcp` 子命令把 stdin/stdout 转发到本端点
//!   （见 `src/main.rs`），供只支持 stdio 的客户端使用。

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use serde_json::{json, Map, Value};
use tokio_util::sync::CancellationToken;

use xfer_rpc::Router;
use xfer_types::{ENGINE_NAME, ENGINE_VERSION};

/// 实现的 MCP 协议版本。客户端请求同年份的版本时按其请求回，便于新客户端接入。
pub const MCP_PROTOCOL_VERSION: &str = "2025-06-18";

const ERR_PARSE: i64 = -32700;
const ERR_INVALID_REQUEST: i64 = -32600;
const ERR_METHOD_NOT_FOUND: i64 = -32601;
const ERR_INVALID_PARAMS: i64 = -32602;

/// MCP 服务本体：协议处理与工具映射，不含传输。
pub struct McpServer {
    router: Arc<Router>,
    /// 与 JSON-RPC 服务共用的密钥（`--rpc-secret`）。None = 不校验。
    token: Option<String>,
}

impl McpServer {
    pub fn new(router: Arc<Router>, token: Option<String>) -> Self {
        Self { router, token }
    }

    /// 校验 HTTP 请求的 Bearer 令牌。
    pub fn authorized(&self, headers: &HeaderMap) -> bool {
        let Some(want) = &self.token else {
            return true;
        };
        headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(|got| got.trim() == want)
            .unwrap_or(false)
    }

    /// 处理一帧 JSON-RPC 请求。返回 None 表示这是通知（无需响应）。
    pub fn handle(&self, body: &str) -> Option<Value> {
        let parsed: Value = match serde_json::from_str(body) {
            Ok(v) => v,
            Err(_) => return Some(error_frame(Value::Null, ERR_PARSE, "Parse error")),
        };
        // MCP 不支持 JSON-RPC batch
        if parsed.is_array() {
            return Some(error_frame(
                Value::Null,
                ERR_INVALID_REQUEST,
                "Batch requests are not supported",
            ));
        }
        self.handle_one(&parsed)
    }

    fn handle_one(&self, req: &Value) -> Option<Value> {
        let id = req.get("id").cloned();
        let Some(method) = req.get("method").and_then(Value::as_str) else {
            return id.map(|id| error_frame(id, ERR_INVALID_REQUEST, "Invalid request"));
        };
        let params = req.get("params").cloned().unwrap_or(Value::Null);

        // 通知：`notifications/initialized`、`notifications/cancelled` 等，静默接受
        let Some(id) = id else {
            tracing::debug!(method, "MCP 通知");
            return None;
        };

        let result: Result<Value, (i64, String)> = match method {
            "initialize" => Ok(self.initialize_result(&params)),
            "ping" => Ok(json!({})),
            "tools/list" => Ok(json!({ "tools": tools() })),
            "tools/call" => self.call_tool(&params),
            other => Err((ERR_METHOD_NOT_FOUND, format!("Method not found: {other}"))),
        };

        Some(match result {
            Ok(v) => json!({"jsonrpc": "2.0", "id": id, "result": v}),
            Err((code, message)) => error_frame(id, code, &message),
        })
    }

    fn initialize_result(&self, params: &Value) -> Value {
        let requested = params
            .get("protocolVersion")
            .and_then(Value::as_str)
            .unwrap_or("");
        let version = if requested.starts_with("2025-") {
            requested
        } else {
            MCP_PROTOCOL_VERSION
        };
        json!({
            "protocolVersion": version,
            "capabilities": { "tools": { "listChanged": false } },
            "serverInfo": {
                "name": "xferrust",
                "title": ENGINE_NAME,
                "version": ENGINE_VERSION,
            },
            "instructions": "XferRust download engine. Use the tools to add and manage \
HTTP(S), BitTorrent, magnet and HLS downloads on the local machine.",
        })
    }

    /// `tools/call`：工具级失败用 `isError: true` 回在结果里（协议要求），
    /// 只有"工具不存在/参数形状不对"才走 JSON-RPC 错误。
    fn call_tool(&self, params: &Value) -> Result<Value, (i64, String)> {
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .ok_or((ERR_INVALID_PARAMS, "参数 name 缺失".to_string()))?;
        let args = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
        if !args.is_object() && !args.is_null() {
            return Err((ERR_INVALID_PARAMS, "参数 arguments 必须是对象".to_string()));
        }
        if !is_known_tool(name) {
            return Err((ERR_INVALID_PARAMS, format!("Unknown tool: {name}")));
        }
        Ok(match self.dispatch(name, &args) {
            Ok(v) => json!({
                "content": [{ "type": "text", "text": render(&v) }],
                "isError": false,
            }),
            Err(e) => json!({
                "content": [{ "type": "text", "text": e }],
                "isError": true,
            }),
        })
    }

    /// 工具名 → 原生 RPC。
    fn dispatch(&self, name: &str, args: &Value) -> Result<Value, String> {
        let obj = args.as_object().cloned().unwrap_or_default();
        let arg = |k: &str| obj.get(k).and_then(Value::as_str).map(str::to_string);
        let gid = || arg("gid").ok_or_else(|| "参数 gid 缺失".to_string());

        match name {
            "add_url" => {
                let url = arg("url").ok_or("参数 url 缺失")?;
                let mut p = Map::new();
                p.insert("uris".into(), json!([url]));
                for (from, to) in [
                    ("dir", "dir"),
                    ("out", "out"),
                    ("referer", "referer"),
                    ("user_agent", "user-agent"),
                ] {
                    if let Some(v) = arg(from) {
                        p.insert(to.into(), json!(v));
                    }
                }
                self.native("task.add", Value::Object(p))
            }
            "add_magnet" => {
                let magnet = arg("magnet").ok_or("参数 magnet 缺失")?;
                let mut p = Map::new();
                p.insert("magnet".into(), json!(magnet));
                if let Some(dir) = arg("dir") {
                    p.insert("dir".into(), json!(dir));
                }
                self.native("task.add", Value::Object(p))
            }
            "add_torrent" => {
                let payload = match arg("torrent_base64") {
                    Some(v) => v,
                    None => {
                        let path = arg("path").ok_or("参数 path 或 torrent_base64 缺失")?;
                        let bytes = std::fs::read(&path)
                            .map_err(|e| format!("读取种子文件失败: {e}"))?;
                        base64::engine::general_purpose::STANDARD.encode(bytes)
                    }
                };
                let mut p = Map::new();
                p.insert("torrent".into(), json!(payload));
                if let Some(dir) = arg("dir") {
                    p.insert("dir".into(), json!(dir));
                }
                self.native("task.add", Value::Object(p))
            }
            "list_tasks" => {
                let scope = arg("scope").unwrap_or_else(|| "all".into());
                let num = obj.get("limit").and_then(Value::as_i64).unwrap_or(100);
                let offset = obj.get("offset").and_then(Value::as_i64).unwrap_or(0);
                self.native(
                    "task.list",
                    json!({ "scope": scope, "num": num, "offset": offset }),
                )
            }
            "get_task" => self.native("task.tell", json!({ "gid": gid()? })),
            "get_task_files" => self.native("task.getFiles", json!({ "gid": gid()? })),
            "pause_task" => self.native("task.pause", json!({ "gid": gid()? })),
            "resume_task" => self.native("task.resume", json!({ "gid": gid()? })),
            "remove_task" => {
                let delete_files = obj
                    .get("delete_files")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                self.native(
                    "task.remove",
                    json!({ "gid": gid()?, "deleteFiles": delete_files }),
                )
            }
            "get_stats" => self.native("engine.globalStat", json!({})),
            "get_engine_info" => self.native("engine.getVersion", json!({})),
            "get_options" => self.native("engine.getOptions", json!({})),
            "set_options" => {
                let options = obj
                    .get("options")
                    .and_then(Value::as_object)
                    .cloned()
                    .ok_or("参数 options（对象）缺失")?;
                self.native("engine.changeOptions", Value::Object(options))
            }
            other => Err(format!("Unknown tool: {other}")),
        }
    }

    /// 调一条原生 RPC，自动注入 token。
    fn native(&self, method: &str, mut params: Value) -> Result<Value, String> {
        if let Value::Object(ref mut map) = params {
            if let Some(token) = &self.token {
                map.insert("token".into(), json!(token));
            }
        }
        let req = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });
        let resp = self
            .router
            .handle(&req.to_string())
            .response
            .ok_or_else(|| "引擎无响应".to_string())?;
        if let Some(err) = resp.get("error") {
            let msg = err
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("引擎返回错误");
            return Err(msg.to_string());
        }
        Ok(resp.get("result").cloned().unwrap_or(Value::Null))
    }
}

fn error_frame(id: Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message },
    })
}

/// 结果文本化：结构化数据用两空格缩进，模型读起来更稳。
fn render(v: &Value) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
}

fn is_known_tool(name: &str) -> bool {
    TOOL_NAMES.contains(&name)
}

/// 工具名清单（与 [`tools`] 保持同步）。
pub const TOOL_NAMES: [&str; 13] = [
    "add_url",
    "add_magnet",
    "add_torrent",
    "list_tasks",
    "get_task",
    "get_task_files",
    "pause_task",
    "resume_task",
    "remove_task",
    "get_stats",
    "get_engine_info",
    "get_options",
    "set_options",
];

/// `tools/list` 的返回体。
pub fn tools() -> Value {
    json!([
        {
            "name": "add_url",
            "description": "Add an HTTP(S) or HLS (M3U8) download task. 按 URL 新建下载任务。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "url": { "type": "string", "description": "Download URL. 下载地址。" },
                    "dir": { "type": "string", "description": "Save directory. 保存目录。" },
                    "out": { "type": "string", "description": "Output file name. 保存文件名。" },
                    "referer": { "type": "string", "description": "HTTP Referer." },
                    "user_agent": { "type": "string", "description": "HTTP User-Agent." }
                },
                "required": ["url"]
            }
        },
        {
            "name": "add_magnet",
            "description": "Add a BitTorrent magnet link. 添加磁力链接任务。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "magnet": { "type": "string", "description": "magnet: URI." },
                    "dir": { "type": "string", "description": "Save directory. 保存目录。" }
                },
                "required": ["magnet"]
            }
        },
        {
            "name": "add_torrent",
            "description": "Add a .torrent file by local path or base64 payload. 按本地种子文件或 base64 添加 BT 任务。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Absolute path to a .torrent file. 本地种子文件路径。" },
                    "torrent_base64": { "type": "string", "description": "Base64 of the .torrent content. 种子内容 base64。" },
                    "dir": { "type": "string", "description": "Save directory. 保存目录。" }
                }
            }
        },
        {
            "name": "list_tasks",
            "description": "List download tasks with status, progress and speed. 列出任务。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "scope": {
                        "type": "string",
                        "enum": ["all", "active", "waiting", "stopped"],
                        "description": "Task scope, default all. 任务范围。"
                    },
                    "limit": { "type": "integer", "description": "Max items, default 100. 数量上限。" },
                    "offset": { "type": "integer", "description": "Skip N items. 偏移。" }
                }
            }
        },
        {
            "name": "get_task",
            "description": "Get one task by gid. 按 gid 查看任务详情。",
            "inputSchema": {
                "type": "object",
                "properties": { "gid": { "type": "string", "description": "Task gid." } },
                "required": ["gid"]
            }
        },
        {
            "name": "get_task_files",
            "description": "List the files of a task. 查看任务包含的文件。",
            "inputSchema": {
                "type": "object",
                "properties": { "gid": { "type": "string", "description": "Task gid." } },
                "required": ["gid"]
            }
        },
        {
            "name": "pause_task",
            "description": "Pause a task. 暂停任务。",
            "inputSchema": {
                "type": "object",
                "properties": { "gid": { "type": "string", "description": "Task gid." } },
                "required": ["gid"]
            }
        },
        {
            "name": "resume_task",
            "description": "Resume a paused task. 继续任务。",
            "inputSchema": {
                "type": "object",
                "properties": { "gid": { "type": "string", "description": "Task gid." } },
                "required": ["gid"]
            }
        },
        {
            "name": "remove_task",
            "description": "Remove a task, optionally deleting downloaded files. 移除任务（可选删除文件）。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "gid": { "type": "string", "description": "Task gid." },
                    "delete_files": { "type": "boolean", "description": "Also delete files on disk. 同时删除文件。" }
                },
                "required": ["gid"]
            }
        },
        {
            "name": "get_stats",
            "description": "Global download/upload speed and task counters. 全局速度与任务计数。",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "get_engine_info",
            "description": "Engine name, version and capability list. 引擎版本与能力。",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "get_options",
            "description": "Read global engine options (speed limits, proxy, concurrency …). 读取全局选项。",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "set_options",
            "description": "Update global engine options, e.g. {\"max-overall-download-limit\":\"1M\"}. 修改全局选项。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "options": {
                        "type": "object",
                        "description": "Key/value pairs to change. 要修改的键值对。"
                    }
                },
                "required": ["options"]
            }
        }
    ])
}

/// 启动 MCP 服务（Streamable HTTP），阻塞直到出错或 shutdown 令牌触发。
pub async fn serve(
    bind: SocketAddr,
    server: Arc<McpServer>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let app = axum::Router::new()
        .route(
            "/mcp",
            axum::routing::post(post_mcp).get(method_not_allowed),
        )
        .route("/health", axum::routing::get(health))
        .with_state(server);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    let local = listener.local_addr()?;
    tracing::info!("MCP 监听于 http://{local}/mcp（Streamable HTTP）");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        })
        .await
        .map_err(std::io::Error::other)
}

async fn post_mcp(
    State(server): State<Arc<McpServer>>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Response {
    if !server.authorized(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
            "Unauthorized",
        )
            .into_response();
    }
    let Ok(text) = std::str::from_utf8(&body) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    match server.handle(text) {
        Some(v) => {
            let payload = serde_json::to_string(&v).unwrap_or_else(|_| "{}".into());
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(payload))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
        // 通知：202 且无响应体
        None => StatusCode::ACCEPTED.into_response(),
    }
}

/// 不提供服务端主动推送（SSE），按 Streamable HTTP 规范回 405。
async fn method_not_allowed() -> Response {
    (StatusCode::METHOD_NOT_ALLOWED, [(header::ALLOW, "POST")]).into_response()
}

async fn health() -> Response {
    axum::Json(json!({
        "ok": true,
        "name": "xferrust-mcp",
        "engine": ENGINE_NAME,
        "version": ENGINE_VERSION,
        "protocol": MCP_PROTOCOL_VERSION,
    }))
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server(secret: Option<&str>, token: Option<&str>) -> McpServer {
        let dir = std::env::temp_dir().join(format!(
            "xfer-mcp-test-{}-{}",
            std::process::id(),
            token.unwrap_or("none")
        ));
        let _ = std::fs::create_dir_all(&dir);
        let manager = xfer_engine::TaskManager::start(dir, 1);
        let router = Arc::new(Router::new(
            secret.map(str::to_string),
            manager.clone(),
            manager.events(),
        ));
        McpServer::new(router, token.map(str::to_string))
    }

    fn call(server: &McpServer, method: &str, params: Value) -> Value {
        let req = json!({"jsonrpc": "2.0", "id": 7, "method": method, "params": params});
        server
            .handle(&req.to_string())
            .expect("请求都应带 id，返回响应")
    }

    #[tokio::test]
    async fn initialize_negotiates_protocol_and_capabilities() {
        let s = server(None, None);
        let resp = call(&s, "initialize", json!({"protocolVersion": "2025-06-18"}));
        assert_eq!(resp["result"]["protocolVersion"], MCP_PROTOCOL_VERSION);
        assert!(resp["result"]["capabilities"]["tools"].is_object());
        assert_eq!(resp["result"]["serverInfo"]["name"], "xferrust");
    }

    #[tokio::test]
    async fn tools_list_covers_core_actions() {
        let s = server(None, None);
        let resp = call(&s, "tools/list", json!({}));
        let tools = resp["result"]["tools"].as_array().expect("tools 数组");
        assert_eq!(tools.len(), TOOL_NAMES.len());
        for name in ["add_url", "add_magnet", "list_tasks", "pause_task"] {
            assert!(
                tools.iter().any(|t| t["name"] == name),
                "缺少工具 {name}"
            );
        }
        // 每个工具都必须带输入 schema，客户端据此生成参数
        assert!(tools.iter().all(|t| t["inputSchema"].is_object()));
    }

    #[tokio::test]
    async fn notification_has_no_response() {
        let s = server(None, None);
        let req = json!({"jsonrpc": "2.0", "method": "notifications/initialized"});
        assert!(s.handle(&req.to_string()).is_none());
    }

    #[tokio::test]
    async fn tool_call_injects_token_and_hits_engine() {
        // 引擎侧的 secret 与 MCP 侧 token 一致 → 鉴权应通过
        let s = server(Some("s3cr3t"), Some("s3cr3t"));
        let resp = call(
            &s,
            "tools/call",
            json!({"name": "get_engine_info", "arguments": {}}),
        );
        assert_eq!(resp["result"]["isError"], false);
        let text = resp["result"]["content"][0]["text"]
            .as_str()
            .expect("文本内容");
        assert!(text.contains(ENGINE_NAME), "应返回引擎信息，实际: {text}");
    }

    #[tokio::test]
    async fn tool_call_reports_auth_failure_in_band() {
        // MCP 侧 token 与引擎 secret 不一致 → 工具级错误，而不是 JSON-RPC 错误
        let s = server(Some("engine-secret"), Some("other"));
        let resp = call(&s, "tools/call", json!({"name": "get_stats", "arguments": {}}));
        assert_eq!(resp["result"]["isError"], true);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("Unauthorized"), "实际: {text}");
    }

    #[tokio::test]
    async fn unknown_tool_is_invalid_params() {
        let s = server(None, None);
        let resp = call(&s, "tools/call", json!({"name": "nope", "arguments": {}}));
        assert_eq!(resp["error"]["code"], ERR_INVALID_PARAMS);
    }

    #[tokio::test]
    async fn batch_and_parse_errors_are_rejected() {
        let s = server(None, None);
        let batch = s.handle("[{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}]");
        assert_eq!(batch.unwrap()["error"]["code"], ERR_INVALID_REQUEST);
        let broken = s.handle("{not json");
        assert_eq!(broken.unwrap()["error"]["code"], ERR_PARSE);
    }

    #[tokio::test]
    async fn bearer_auth_is_enforced() {
        let s = server(None, Some("tok"));
        let mut headers = HeaderMap::new();
        assert!(!s.authorized(&headers));
        headers.insert(header::AUTHORIZATION, "Bearer tok".parse().unwrap());
        assert!(s.authorized(&headers));
        headers.insert(header::AUTHORIZATION, "Bearer nope".parse().unwrap());
        assert!(!s.authorized(&headers));
    }
}
