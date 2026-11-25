//! JSON-RPC 2.0 传输层：HTTP POST + aria2 风格的 **GET/JSONP** + WebSocket。
//!
//! 三种入口共用同一个 [`Router`]：
//! - `POST /jsonrpc`：标准 JSON-RPC；
//! - `GET /jsonrpc?method=…&id=…&params=[…][&jsonp=cb]`：aria2 的 GET 形式，
//!   浏览器端插件（Bilibili-Evolved 的「aria2 输出支持」等）靠它绕开 CORS；
//!   带 `jsonp` 时按脚本回调包装返回；
//! - `GET /jsonrpc` + `Upgrade: websocket`：单连接复用（请求/响应 + 事件推送）。
//!
//! 设计目标：一条 WebSocket 连接同时承载请求/响应与事件推送，
//! 客户端无需轮询；每个连接按首条请求自动识别协议族（原生/前端兼容），
//! 只推送对应协议的事件帧，避免串扰与冗余流量。

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{FromRequestParts, Query, Request, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::router::{Proto, Router};
use xfer_engine::EngineEvent;

struct AppState {
    router: Arc<Router>,
}

/// aria2 GET 形式的查询参数。
///
/// `params` 是**URL 编码的 JSON 数组**（aria2 约定），`jsonp` 是回调名。
/// 全部用 `Option` 接收：不带 `params` 的调用（如 `aria2.getVersion`）很常见。
#[derive(Debug, Default, Deserialize)]
pub(crate) struct GetRpcQuery {
    pub(crate) method: Option<String>,
    pub(crate) id: Option<String>,
    pub(crate) params: Option<String>,
    pub(crate) jsonp: Option<String>,
}

/// JSONP 回调名白名单：`[A-Za-z_$][A-Za-z0-9_$.]*`。
///
/// **必须校验**：JSONP 的响应体会被浏览器当脚本执行，回调名直接拼进响应体
/// 就等于把任意 JS 注入调用方页面（经典的 JSONP XSS）。
pub(crate) fn is_valid_jsonp_callback(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' || c == '$' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$' || c == '.')
}

/// 把 aria2 的 GET 查询参数拼成一条 JSON-RPC 请求文本（交给 Router 解析）。
///
/// 单独抽出来是为了能直接单测：拼错一个字段名，浏览器端插件那边只会看到
/// 「发送失败」，服务端日志里看不出任何异常。
pub(crate) fn get_query_to_request(q: &GetRpcQuery) -> Result<String, String> {
    let method = q
        .method
        .as_deref()
        .map(str::trim)
        .filter(|m| !m.is_empty())
        .ok_or_else(|| "缺少 method 参数".to_string())?;
    // params 缺省 = 无参数（aria2 同语义）；给了但解析不出来是调用方的错，
    // 必须回错误而不是悄悄当空数组执行（那会变成"参数丢了还能跑"）
    let params: Value = match q.params.as_deref().map(str::trim) {
        Some(s) if !s.is_empty() => {
            serde_json::from_str(s).map_err(|e| format!("params 不是合法 JSON：{e}"))?
        }
        _ => Value::Array(Vec::new()),
    };
    // id 允许数字或字符串，原样透传；没给就按通知处理
    let id: Value = match q.id.as_deref().map(str::trim) {
        Some(s) if !s.is_empty() => {
            serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.to_string()))
        }
        _ => Value::Null,
    };
    Ok(json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    })
    .to_string())
}

async fn post_jsonrpc(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    body: bytes::Bytes,
) -> axum::response::Response {
    let text = match std::str::from_utf8(&body) {
        Ok(t) => t,
        Err(_) => return axum::http::StatusCode::BAD_REQUEST.into_response(),
    };
    match state.router.handle(text).response {
        Some(v) => axum::response::Json(v).into_response(),
        None => axum::http::StatusCode::BAD_REQUEST.into_response(),
    }
}

fn is_websocket_upgrade(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(',')
                .any(|t| t.trim().eq_ignore_ascii_case("websocket"))
        })
}

/// GET 形式的响应：带 `jsonp` 时包装成脚本回调，否则回 JSON。
fn get_rpc_response(router: &Router, q: &GetRpcQuery) -> Response {
    let text = match get_query_to_request(q) {
        Ok(t) => t,
        Err(msg) => {
            let body = json!({
                "jsonrpc": "2.0",
                "id": Value::Null,
                "error": { "code": crate::compat::RPC_ERROR_CODE, "message": msg },
            });
            return (StatusCode::BAD_REQUEST, axum::response::Json(body)).into_response();
        }
    };
    let Some(resp) = router.handle(&text).response else {
        return (StatusCode::BAD_REQUEST, "请求没有产生响应").into_response();
    };
    match q.jsonp.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(cb) => {
            if !is_valid_jsonp_callback(cb) {
                return (StatusCode::BAD_REQUEST, "非法的 jsonp 回调名").into_response();
            }
            let payload = serde_json::to_string(&resp).expect("响应序列化不会失败");
            // aria2 的格式就是 `cb(<json>)`：不加分号，与调用方的解析保持一致
            (
                [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
                format!("{cb}({payload})"),
            )
                .into_response()
        }
        None => axum::response::Json(resp).into_response(),
    }
}

/// `GET /jsonrpc`：WebSocket 升级优先，其余按 aria2 的 GET 形式处理。
///
/// 两条路径共用同一个方法/路径（宿主用 WS，浏览器插件用 GET），所以不能在
/// 路由层区分 —— 只能先看 `Upgrade` 头。
async fn get_jsonrpc(State(state): State<Arc<AppState>>, req: Request) -> Response {
    if is_websocket_upgrade(req.headers()) {
        let (mut parts, _body) = req.into_parts();
        return match axum::extract::ws::WebSocketUpgrade::from_request_parts(&mut parts, &()).await
        {
            Ok(ws) => ws_jsonrpc(State(state), ws).await.into_response(),
            Err(rej) => rej.into_response(),
        };
    }
    let query = match Query::<GetRpcQuery>::try_from_uri(req.uri()) {
        Ok(Query(q)) => q,
        Err(rej) => return rej.into_response(),
    };
    get_rpc_response(&state.router, &query)
}

async fn ws_jsonrpc(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    ws: axum::extract::ws::WebSocketUpgrade,
) -> impl axum::response::IntoResponse {
    ws.on_upgrade(move |mut socket| async move {
        use axum::extract::ws::Message;
        let mut events = state.router.events();
        let mut proto = Proto::Unknown;
        let mut native_events = false;
        loop {
            tokio::select! {
                msg = socket.recv() => {
                    let text = match msg {
                        Some(Ok(Message::Text(t))) => t,
                        Some(Ok(Message::Close(_))) | None => break,
                        Some(Ok(_)) => continue,
                        Some(Err(_)) => break,
                    };
                    let out = state.router.handle(&text);
                    // 首条请求确定连接的协议族，之后只推送该族事件
                    if proto == Proto::Unknown && out.proto_used != Proto::Unknown {
                        proto = out.proto_used;
                    }
                    if out.native_subscribed {
                        native_events = true;
                    }
                    if let Some(resp) = out.response {
                        let payload = serde_json::to_string(&resp).expect("响应序列化不会失败");
                        if socket.send(Message::Text(payload.into())).await.is_err() {
                            break;
                        }
                    }
                }
                ev = events.recv() => {
                    let ev: EngineEvent = match ev {
                        Ok(e) => e,
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    };
                    let frame: Option<Value> = match proto {
                        Proto::Compat => state.router.compat_event_frame(&ev),
                        Proto::Native if native_events => state.router.native_event_frame(&ev),
                        _ => None,
                    };
                    if let Some(f) = frame {
                        let payload = serde_json::to_string(&f).expect("通知序列化不会失败");
                        if socket.send(Message::Text(payload.into())).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }
    })
}

/// 启动 RPC 服务（阻塞直到出错或 shutdown 令牌触发）。
pub async fn serve(
    bind: SocketAddr,
    router: Arc<Router>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let state = Arc::new(AppState { router });
    let app = axum::Router::new()
        .route(
            "/jsonrpc",
            axum::routing::post(post_jsonrpc).get(get_jsonrpc),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let local = listener.local_addr()?;
    tracing::info!("RPC 监听于 http://{local}/jsonrpc（HTTP POST + aria2 GET/JSONP + WebSocket）");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        })
        .await
        .map_err(std::io::Error::other)
}
