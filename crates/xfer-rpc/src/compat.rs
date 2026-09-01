//! 前端客户端线上协议适配层。
//!
//! 仅做协议翻译，不含任何引擎逻辑：把前端统一的命名空间前缀方法名、
//! 位置参数、字符串数值、token 鉴权语义翻译到引擎公开 API。
//! 新增能力请走原生协议（native.rs），本层保持冻结。

use std::sync::Arc;

use serde_json::{json, Value};
use xfer_engine::{EngineEvent, TaskManager};
use xfer_types::{Gid, ENGINE_NAME, ENGINE_VERSION};

/// 协议约定的业务错误码。
pub const RPC_ERROR_CODE: i64 = 1;

#[derive(Debug, thiserror::Error)]
pub enum CompatError {
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Method not found")]
    MethodNotFound,
    #[error("{0}")]
    Method(String),
}

/// 前端线上协议分发器。
pub struct CompatDispatcher {
    secret: Option<String>,
    engine: Arc<TaskManager>,
}

impl CompatDispatcher {
    pub fn new(secret: Option<String>, engine: Arc<TaskManager>) -> Self {
        Self { secret, engine }
    }

    /// 分发一条前端请求（位置参数；密钥时首元素为 `"token:<secret>"`）。
    pub fn dispatch(&self, method: &str, params: &[Value]) -> Result<Value, CompatError> {
        // 剥离前端统一附加的方法名命名空间前缀（线上协议兼容）。
        let method = method.strip_prefix("aria2.").unwrap_or(method);

        // system.* 方法不做顶层鉴权（multicall 的每个子调用各自带凭据）
        let params_rest: &[Value] = if method.starts_with("system.") {
            params
        } else if let Some(want) = &self.secret {
            let ok = params
                .first()
                .and_then(Value::as_str)
                .map(|s| s.strip_prefix("token:") == Some(want.as_str()))
                .unwrap_or(false);
            if !ok {
                return Err(CompatError::Unauthorized);
            }
            &params[1..]
        } else {
            params
        };

        let gid_at = |idx: usize| -> Result<Gid, CompatError> {
            let s = params_rest
                .get(idx)
                .and_then(Value::as_str)
                .ok_or_else(|| CompatError::Method(format!("参数 {idx}（gid）缺失")))?;
            Gid::parse(s).ok_or_else(|| CompatError::Method(format!("GID {s} 非法")))
        };
        let keys_at = |idx: usize| -> Option<Vec<String>> {
            params_rest.get(idx).and_then(|v| {
                v.as_array().map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(|s| s.to_string())
                        .collect()
                })
            })
        };

        let e = &self.engine;
        match method {
            "getVersion" => Ok(json!({
                "version": ENGINE_VERSION,
                "enabledFeatures": [
                    format!("{ENGINE_NAME} (Rust)"),
                ],
            })),
            "addUri" => {
                let uris: Vec<String> = params_rest
                    .first()
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .map(|s| s.to_string())
                            .collect()
                    })
                    .ok_or_else(|| CompatError::Method("参数 0（uris 数组）缺失".into()))?;
                let options = params_rest.get(1).cloned().unwrap_or_else(|| json!({}));
                let position = params_rest.get(2).and_then(Value::as_i64);
                e.add_uri(uris, &options, position)
                    .map(|g| json!(g.0))
                    .map_err(CompatError::Method)
            }
            "addTorrent" => {
                let tb64 = params_rest
                    .first()
                    .and_then(Value::as_str)
                    .ok_or_else(|| CompatError::Method("参数 0（torrent base64）缺失".into()))?;
                let options = params_rest.get(1).cloned().unwrap_or_else(|| json!({}));
                let position = params_rest.get(2).and_then(Value::as_i64);
                e.add_torrent(tb64, &options, position)
                    .map(|g| json!(g.0))
                    .map_err(CompatError::Method)
            }
            "getPeers" => e.get_peers(&gid_at(0)?).map_err(CompatError::Method),
            "remove" | "forceRemove" => e
                .remove(&gid_at(0)?)
                .map(|_| json!("OK"))
                .map_err(CompatError::Method),
            "pause" | "forcePause" => e
                .pause(&gid_at(0)?)
                .map(|_| json!("OK"))
                .map_err(CompatError::Method),
            "unpause" => e
                .unpause(&gid_at(0)?)
                .map(|_| json!("OK"))
                .map_err(CompatError::Method),
            "tellStatus" => {
                let keys = keys_at(1);
                e.tell_status(&gid_at(0)?, keys.as_deref())
                    .map_err(CompatError::Method)
            }
            "tellActive" => {
                let keys = keys_at(0);
                Ok(e.tell_active(keys.as_deref()))
            }
            "tellWaiting" | "tellStopped" => {
                let offset = params_rest.first().and_then(Value::as_i64).unwrap_or(0);
                let num = params_rest.get(1).and_then(Value::as_i64).unwrap_or(-1);
                let keys = keys_at(2);
                Ok(if method == "tellWaiting" {
                    e.tell_waiting(offset, num, keys.as_deref())
                } else {
                    e.tell_stopped(offset, num, keys.as_deref())
                })
            }
            "getGlobalStat" => Ok(e.global_stat()),
            "getFiles" => e.get_files(&gid_at(0)?).map_err(CompatError::Method),
            "getURIs" => e.get_uris(&gid_at(0)?).map_err(CompatError::Method),
            "getOption" => e.get_option(&gid_at(0)?).map_err(CompatError::Method),
            "changeOption" => {
                let options = params_rest.get(1).cloned().unwrap_or_else(|| json!({}));
                e.change_option(&gid_at(0)?, &options)
                    .map(|_| json!("OK"))
                    .map_err(CompatError::Method)
            }
            "changeGlobalOption" => {
                let options = params_rest.first().cloned().unwrap_or_else(|| json!({}));
                e.change_global_option(&options)
                    .map(|_| json!("OK"))
                    .map_err(CompatError::Method)
            }
            "getGlobalOption" => Ok(e.get_global_option()),
            "purgeDownloadResult" => e
                .purge_download_result()
                .map(|_| json!("OK"))
                .map_err(CompatError::Method),
            "removeDownloadResult" => e
                .remove_download_result(&gid_at(0)?)
                .map(|_| json!("OK"))
                .map_err(CompatError::Method),
            "saveSession" => e
                .save_session()
                .map(|_| json!("OK"))
                .map_err(CompatError::Method),
            "shutdown" | "forceShutdown" => {
                e.shutdown();
                Ok(json!("OK"))
            }
            "system.multicall" => {
                let calls = params_rest
                    .first()
                    .and_then(Value::as_array)
                    .ok_or_else(|| CompatError::Method("参数 0（calls 数组）缺失".into()))?;
                let mut results = Vec::with_capacity(calls.len());
                for call in calls {
                    let (name, call_params) = parse_multicall_entry(call)
                        .ok_or_else(|| CompatError::Method("multicall 条目格式非法".into()))?;
                    match self.dispatch(&name, &call_params) {
                        Ok(v) => results.push(json!([v])),
                        Err(e) => results.push(json!({
                            "code": RPC_ERROR_CODE,
                            "message": e.to_string(),
                        })),
                    }
                }
                Ok(Value::Array(results))
            }
            "system.listMethods" => Ok(json!(self.supported_methods())),
            "system.listNotifications" => Ok(json!(NOTIFICATIONS.map(|n| format!("aria2.{n}")))),
            _ => Err(CompatError::MethodNotFound),
        }
    }

    fn supported_methods(&self) -> Vec<String> {
        let mut m: Vec<String> = METHODS.map(|m| format!("aria2.{m}")).to_vec();
        m.extend(SYSTEM_METHODS.map(|m| m.to_string()));
        m.extend(NOTIFICATIONS.map(|n| format!("aria2.{n}")));
        m
    }

    /// 引擎事件 → 前端通知帧（无 id）。
    pub fn event_frame(&self, ev: &EngineEvent) -> Option<Value> {
        let name = match ev.0.as_str() {
            "start" => "onDownloadStart",
            "pause" => "onDownloadPause",
            "stop" => "onDownloadStop",
            "complete" => {
                // BT 任务完成用 onBtDownloadComplete，HTTP 用 onDownloadComplete，
                // 与 aria2 通知语义一致（避免错误地广播不存在的 BT 完成事件）。
                let gid = Gid::from(ev.1.as_str());
                if self.engine.is_bt_task(&gid) {
                    "onBtDownloadComplete"
                } else {
                    "onDownloadComplete"
                }
            }
            "error" => "onDownloadError",
            _ => return None,
        };
        Some(json!({
            "jsonrpc": "2.0",
            "method": format!("aria2.{name}"),
            "params": [{"gid": ev.1}],
        }))
    }
}

/// 支持的业务方法（不含命名空间前缀）。
const METHODS: [&str; 25] = [
    "addUri",
    "addTorrent",
    "getPeers",
    "remove",
    "forceRemove",
    "pause",
    "forcePause",
    "unpause",
    "tellStatus",
    "tellActive",
    "tellWaiting",
    "tellStopped",
    "getGlobalStat",
    "getVersion",
    "getFiles",
    "getURIs",
    "getOption",
    "changeOption",
    "getGlobalOption",
    "changeGlobalOption",
    "purgeDownloadResult",
    "removeDownloadResult",
    "saveSession",
    "shutdown",
    "forceShutdown",
];

const SYSTEM_METHODS: [&str; 3] = [
    "system.multicall",
    "system.listMethods",
    "system.listNotifications",
];

const NOTIFICATIONS: [&str; 6] = [
    "onDownloadStart",
    "onDownloadPause",
    "onDownloadStop",
    "onDownloadComplete",
    "onDownloadError",
    "onBtDownloadComplete",
];

/// 解析 multicall 条目：`{"methodName": "...", "params": [...]}`。
fn parse_multicall_entry(call: &Value) -> Option<(String, Vec<Value>)> {
    let name = call.get("methodName")?.as_str()?.to_string();
    let params = call
        .get("params")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    Some((name, params))
}
