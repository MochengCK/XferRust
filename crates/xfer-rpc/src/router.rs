//! 协议路由器：按方法命名空间把请求分发给原生/前端兼容分发器。
//!
//! - 原生方法（`task.*`/`engine.*`/`events.*`）→ NativeDispatcher；
//! - 其余（`aria2.*`、无前缀旧名、`system.*`）→ CompatDispatcher；
//! - 支持 JSON-RPC batch（数组请求 → 数组响应）；
//! - 每个请求封装统一响应帧（result / error）。

use std::sync::Arc;

use serde_json::{json, Value};
use tokio::sync::broadcast;
use xfer_engine::EngineEvent;

use crate::compat::{CompatDispatcher, CompatError, RPC_ERROR_CODE};
use crate::native::NativeDispatcher;

/// 连接协议族（传输层据此选择事件推送格式）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Proto {
    Unknown,
    Native,
    Compat,
}

/// 一次请求处理的结果。
pub struct HandleOutcome {
    /// 响应帧（None = 通知类请求或解析失败）。
    pub response: Option<Value>,
    /// 本次请求命中的协议族。
    pub proto_used: Proto,
    /// 本次请求是否为原生事件订阅。
    pub native_subscribed: bool,
}

/// 顶层路由器（无状态，可全局共享）。
pub struct Router {
    native: NativeDispatcher,
    compat: CompatDispatcher,
    events: broadcast::Sender<EngineEvent>,
}

impl Router {
    pub fn new(
        secret: Option<String>,
        engine: Arc<xfer_engine::TaskManager>,
        events: broadcast::Sender<EngineEvent>,
    ) -> Self {
        Self {
            native: NativeDispatcher::new(secret.clone(), engine.clone()),
            compat: CompatDispatcher::new(secret, engine),
            events,
        }
    }

    pub fn events(&self) -> broadcast::Receiver<EngineEvent> {
        self.events.subscribe()
    }

    pub fn native_event_frame(&self, ev: &EngineEvent) -> Option<Value> {
        self.native.event_frame(ev)
    }

    pub fn compat_event_frame(&self, ev: &EngineEvent) -> Option<Value> {
        self.compat.event_frame(ev)
    }

    /// 处理一帧请求体（单个或 batch 数组）。
    pub fn handle(&self, body: &str) -> HandleOutcome {
        let parsed: Value = match serde_json::from_str(body) {
            Ok(v) => v,
            Err(_) => {
                // 非法 JSON：回一个标准 parse error 帧（无 id），
                // 避免 WS 客户端对畸形帧空等（HTTP 端也返回结构化错误而非空 400）。
                return HandleOutcome {
                    response: Some(frame(
                        Value::Null,
                        Err((RPC_ERROR_CODE, "Parse error".to_string())),
                    )),
                    proto_used: Proto::Unknown,
                    native_subscribed: false,
                };
            }
        };
        if let Value::Array(items) = parsed {
            // JSON-RPC batch：逐条处理，只回带 id 的条目
            let mut proto = Proto::Unknown;
            let mut subscribed = false;
            let mut responses = Vec::new();
            for item in &items {
                let out = self.handle_one(item);
                if proto == Proto::Unknown {
                    proto = out.proto_used;
                }
                subscribed |= out.native_subscribed;
                if let Some(r) = out.response {
                    responses.push(r);
                }
            }
            return HandleOutcome {
                response: if responses.is_empty() {
                    None
                } else {
                    Some(Value::Array(responses))
                },
                proto_used: proto,
                native_subscribed: subscribed,
            };
        }
        self.handle_one(&parsed)
    }

    fn handle_one(&self, req: &Value) -> HandleOutcome {
        let Some(method) = req.get("method").and_then(Value::as_str) else {
            // 无效请求（缺 method / method 非字符串）：带 id 则回错误帧，
            // 不带 id（通知）则按 JSON-RPC 约定静默丢弃。
            let id = req.get("id").cloned();
            return HandleOutcome {
                response: id.map(|id| frame(id, Err((RPC_ERROR_CODE, "Invalid request".into())))),
                proto_used: Proto::Unknown,
                native_subscribed: false,
            };
        };
        let id = req.get("id").cloned();
        let params = req.get("params").cloned().unwrap_or(Value::Null);

        if NativeDispatcher::owns(method) {
            let result = self.native.dispatch(method, &params);
            let subscribed = method == "events.subscribe" && result.is_ok();
            HandleOutcome {
                response: id.map(|id| frame(id, result.map_err(|e| (RPC_ERROR_CODE, e)))),
                proto_used: Proto::Native,
                native_subscribed: subscribed,
            }
        } else {
            let params_arr = params.as_array().cloned().unwrap_or_default();
            let result = self.compat.dispatch(method, &params_arr);
            HandleOutcome {
                response: id.map(|id| {
                    frame(
                        id,
                        result.map_err(|e| {
                            let msg = match &e {
                                CompatError::Unauthorized => "Unauthorized".to_string(),
                                CompatError::MethodNotFound => "Method not found".to_string(),
                                CompatError::Method(m) => m.clone(),
                            };
                            (RPC_ERROR_CODE, msg)
                        }),
                    )
                }),
                proto_used: Proto::Compat,
                native_subscribed: false,
            }
        }
    }
}

/// 组装 JSON-RPC 响应帧。
fn frame(id: Value, result: Result<Value, (i64, String)>) -> Value {
    match result {
        Ok(v) => json!({"jsonrpc": "2.0", "id": id, "result": v}),
        Err((code, message)) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": code, "message": message},
        }),
    }
}
