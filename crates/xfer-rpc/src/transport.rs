//! JSON-RPC 2.0 传输层：HTTP POST 单发 + WebSocket 单连接复用。
//!
//! 设计目标：一条 WebSocket 连接同时承载请求/响应与事件推送，
//! 客户端无需轮询；每个连接按首条请求自动识别协议族（原生/前端兼容），
//! 只推送对应协议的事件帧，避免串扰与冗余流量。

use std::net::SocketAddr;
use std::sync::Arc;

use axum::response::IntoResponse;
use serde_json::Value;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::router::{Proto, Router};
use xfer_engine::EngineEvent;

struct AppState {
    router: Arc<Router>,
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
            axum::routing::post(post_jsonrpc).get(ws_jsonrpc),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let local = listener.local_addr()?;
    tracing::info!("RPC 监听于 http://{local}/jsonrpc（HTTP POST + WebSocket 复用）");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        })
        .await
        .map_err(std::io::Error::other)
}
