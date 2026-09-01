//! 原生协议：引擎的第一公民 RPC 接口。
//!
//! 设计原则：
//! - 命名空间方法名（`task.*` / `engine.*` / `events.*`），语义直白；
//! - 命名参数（JSON 对象），字段自描述、可扩展；
//! - 数值字段为真实 JSON 数值（不再字符串承载）；
//! - `events.subscribe` 订阅后由服务端推送进度/生命周期事件，
//!   客户端免轮询（配合传输层的单连接复用，流量与 CPU 开销最低）。

use std::sync::Arc;

use serde_json::{json, Value};
use xfer_engine::{EngineEvent, TaskManager};
use xfer_types::{Gid, ENGINE_NAME, ENGINE_VERSION};

/// 原生方法命名空间前缀。
pub const NATIVE_PREFIXES: [&str; 3] = ["task.", "engine.", "events."];

/// 原生协议分发器。
pub struct NativeDispatcher {
    secret: Option<String>,
    engine: Arc<TaskManager>,
}

impl NativeDispatcher {
    pub fn new(secret: Option<String>, engine: Arc<TaskManager>) -> Self {
        Self { secret, engine }
    }

    /// 方法名是否属于原生命名空间。
    pub fn owns(method: &str) -> bool {
        NATIVE_PREFIXES.iter().any(|p| method.starts_with(p))
    }

    /// 分发一条原生请求。`params` 为命名参数对象。
    pub fn dispatch(&self, method: &str, params: &Value) -> Result<Value, String> {
        let obj = params.as_object().cloned().unwrap_or_default();
        // 密钥鉴权：设置 secret 时每个请求必须带 "token" 字段
        if let Some(want) = &self.secret {
            let ok = obj
                .get("token")
                .and_then(Value::as_str)
                .map(|t| t == want)
                .unwrap_or(false);
            if !ok {
                return Err("Unauthorized".into());
            }
        }
        let gid = |key: &str| -> Result<Gid, String> {
            let s = obj
                .get(key)
                .and_then(Value::as_str)
                .ok_or_else(|| format!("参数 {key}（gid）缺失"))?;
            Gid::parse(s).ok_or_else(|| format!("GID {s} 非法"))
        };
        let keys = || -> Option<Vec<String>> {
            obj.get("keys").and_then(Value::as_array).map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(|s| s.to_string())
                    .collect()
            })
        };
        let e = &self.engine;
        match method {
            "task.add" => {
                // BT 磁力链接（BEP 9：先获取元数据再下载）
                if let Some(m) = obj.get("magnet").and_then(Value::as_str) {
                    let mut options = serde_json::Map::new();
                    for k in ["dir", "out"] {
                        if let Some(v) = obj.get(k) {
                            options.insert(k.to_string(), v.clone());
                        }
                    }
                    let position = obj.get("position").and_then(Value::as_i64);
                    return e
                        .add_magnet(m, &Value::Object(options), position)
                        .map(|g| json!({"gid": g.0}));
                }
                // BT 形式：torrent 字段（.torrent base64）
                if let Some(tb64) = obj.get("torrent").and_then(Value::as_str) {
                    let mut options = serde_json::Map::new();
                    for k in ["dir", "out"] {
                        if let Some(v) = obj.get(k) {
                            options.insert(k.to_string(), v.clone());
                        }
                    }
                    let position = obj.get("position").and_then(Value::as_i64);
                    return e
                        .add_torrent(tb64, &Value::Object(options), position)
                        .map(|g| json!({"gid": g.0}));
                }
                let uris: Vec<String> = obj
                    .get("uris")
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .map(|s| s.to_string())
                            .collect()
                    })
                    .ok_or_else(|| "参数 uris（数组）缺失".to_string())?;
                let mut options = serde_json::Map::new();
                for k in ["dir", "out", "checksum"] {
                    if let Some(v) = obj.get(k) {
                        options.insert(k.to_string(), v.clone());
                    }
                }
                let position = obj.get("position").and_then(Value::as_i64);
                e.add_uri(uris, &Value::Object(options), position)
                    .map(|g| json!({"gid": g.0}))
            }
            "task.tell" => e.tell_status_native(&gid("gid")?, keys().as_deref()),
            "task.list" => {
                let scope = obj.get("scope").and_then(Value::as_str).unwrap_or("all");
                let offset = obj.get("offset").and_then(Value::as_i64).unwrap_or(0);
                let num = obj.get("num").and_then(Value::as_i64).unwrap_or(-1);
                Ok(e.list_native(scope, offset, num, keys().as_deref()))
            }
            "task.pause" => e.pause(&gid("gid")?).map(|_| json!({"ok": true})),
            "task.resume" => e.unpause(&gid("gid")?).map(|_| json!({"ok": true})),
            "task.remove" => e.remove(&gid("gid")?).map(|_| json!({"ok": true})),
            "task.purgeResults" => e.purge_download_result().map(|_| json!({"ok": true})),
            "task.removeResult" => e
                .remove_download_result(&gid("gid")?)
                .map(|_| json!({"ok": true})),
            "task.getFiles" => e.get_files(&gid("gid")?),
            "task.getUris" => e.get_uris(&gid("gid")?),
            "task.getPeers" => e.get_peers(&gid("gid")?),
            "task.getTrackers" => e.get_trackers(&gid("gid")?),
            "task.addTrackers" => {
                let gid = gid("gid")?;
                let trackers: Vec<String> = obj
                    .get("trackers")
                    .and_then(Value::as_array)
                    .ok_or_else(|| "参数 trackers（数组）缺失".to_string())?
                    .iter()
                    .filter_map(Value::as_str)
                    .map(|s| s.to_string())
                    .collect();
                if trackers.is_empty() {
                    return Err("trackers 不能为空".into());
                }
                e.add_trackers(&gid, trackers).map(|_| json!({"ok": true}))
            }
            "task.getOption" => e.get_option(&gid("gid")?),
            "task.changeOption" => {
                let gid = gid("gid")?;
                let mut options = obj.clone();
                options.remove("gid");
                options.remove("token");
                e.change_option(&gid, &Value::Object(options))
                    .map(|_| json!({"ok": true}))
            }
            "engine.saveSession" => e.save_session().map(|_| json!({"ok": true})),
            "engine.getVersion" => Ok(json!({
                "name": ENGINE_NAME,
                "version": ENGINE_VERSION,
                "features": ["http", "resume", "checksum", "bt", "events"],
            })),
            "engine.globalStat" => Ok(e.global_stat_native()),
            "engine.getOptions" => Ok(e.get_global_option()),
            "engine.changeOptions" => {
                let mut options = obj.clone();
                options.remove("token");
                e.change_global_option(&Value::Object(options))
                    .map(|_| json!({"ok": true}))
            }
            "engine.getTrackers" => Ok(json!({
                "trackers": e.get_global_trackers(),
            })),
            "engine.addTracker" => {
                let url = obj
                    .get("tracker")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "参数 tracker 缺失".to_string())?;
                e.add_global_tracker(url).map(|_| json!({"ok": true}))
            }
            "engine.removeTracker" => {
                let url = obj
                    .get("tracker")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "参数 tracker 缺失".to_string())?;
                e.remove_global_tracker(url).map(|_| json!({"ok": true}))
            }
            "engine.getSubscriptions" => {
                let subs = e.get_subscriptions();
                Ok(serde_json::to_value(&subs).unwrap_or(Value::Array(vec![])))
            }
            "engine.addSubscription" => {
                let name = obj.get("name").and_then(Value::as_str).unwrap_or("");
                let url = obj
                    .get("url")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "参数 url 缺失".to_string())?;
                let enabled = obj.get("enabled").and_then(Value::as_bool).unwrap_or(true);
                e.add_subscription(name, url, enabled)
                    .map(|sub| serde_json::to_value(&sub).unwrap_or(json!({"ok": true})))
            }
            "engine.removeSubscription" => {
                let id = obj
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "参数 id 缺失".to_string())?;
                e.remove_subscription(id).map(|_| json!({"ok": true}))
            }
            "engine.toggleSubscription" => {
                let id = obj
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "参数 id 缺失".to_string())?;
                e.toggle_subscription(id).map(|_| json!({"ok": true}))
            }
            "engine.refreshSubscription" => {
                let id = obj
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "参数 id 缺失".to_string())?;
                let mgr = e.clone();
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(mgr.refresh_subscription(id))
                })
                .map(|n| json!({"count": n}))
            }
            "engine.refreshAllSubscriptions" => {
                let mgr = e.clone();
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(mgr.refresh_all_subscriptions())
                })
                .map(|n| json!({"count": n}))
            }
            "engine.getAutoUpdateTrackers" => Ok(json!({"enabled": e.get_auto_update_trackers()})),
            "engine.setAutoUpdateTrackers" => {
                let enabled = obj
                    .get("enabled")
                    .and_then(Value::as_bool)
                    .ok_or_else(|| "参数 enabled 缺失".to_string())?;
                e.set_auto_update_trackers(enabled);
                Ok(json!({"ok": true}))
            }
            "engine.shutdown" | "engine.forceShutdown" => {
                e.shutdown();
                Ok(json!({"ok": true}))
            }
            "events.subscribe" => Ok(json!({"ok": true})),
            _ => Err("Method not found".into()),
        }
    }

    /// 引擎事件 → 原生通知帧（payload 为实时状态摘要）。
    pub fn event_frame(&self, ev: &EngineEvent) -> Option<Value> {
        let (event, gid) = ev;
        let gid_parsed = Gid::parse(gid)?;
        match event.as_str() {
            "progress" => {
                let (status, completed, total, speed) =
                    self.engine.progress_snapshot(&gid_parsed)?;
                Some(json!({
                    "jsonrpc": "2.0",
                    "method": "task.progress",
                    "params": {
                        "gid": gid,
                        "status": status,
                        "completedLength": completed,
                        "totalLength": total,
                        "downloadSpeed": speed,
                    },
                }))
            }
            "start" | "pause" | "stop" | "complete" => Some(json!({
                "jsonrpc": "2.0",
                "method": format!("task.{event}"),
                "params": {"gid": gid},
            })),
            "error" => {
                let st = self.engine.tell_status_native(&gid_parsed, None).ok()?;
                Some(json!({
                    "jsonrpc": "2.0",
                    "method": "task.error",
                    "params": {
                        "gid": gid,
                        "errorCode": st["errorCode"],
                        "errorMessage": st["errorMessage"],
                    },
                }))
            }
            _ => None,
        }
    }
}
