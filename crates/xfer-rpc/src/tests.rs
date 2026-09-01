//! 协议层测试：原生/前端兼容分发、batch、鉴权、事件帧、WS 端到端。

use super::*;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use xfer_engine::{EngineEvent, TaskManager};
use xfer_types::{ENGINE_NAME, ENGINE_VERSION};

fn setup() -> (
    Arc<Router>,
    Arc<TaskManager>,
    broadcast::Sender<EngineEvent>,
) {
    let mgr = TaskManager::new(std::env::temp_dir(), 2);
    let events = mgr.events();
    let router = Arc::new(Router::new(None, mgr.clone(), events.clone()));
    (router, mgr, events)
}

fn handle(router: &Router, body: &str) -> Value {
    router.handle(body).response.expect("应有响应")
}

// ------------------------------------------------------------------
// 原生协议
// ------------------------------------------------------------------

#[tokio::test]
async fn native_get_version_and_stat() {
    let (router, _mgr, _events) = setup();
    let v = handle(
        &router,
        r#"{"jsonrpc":"2.0","id":1,"method":"engine.getVersion","params":{}}"#,
    );
    assert_eq!(v["result"]["version"], ENGINE_VERSION);
    assert_eq!(v["result"]["name"], ENGINE_NAME);

    let s = handle(
        &router,
        r#"{"jsonrpc":"2.0","id":2,"method":"engine.globalStat","params":{}}"#,
    );
    // 数值类型（不是字符串）
    assert_eq!(s["result"]["numActive"], 0);
    assert_eq!(s["result"]["downloadSpeed"], 0);
}

/// 畸形请求必须回错误帧而非静默丢弃（否则 WS 客户端空等、HTTP 端拿不到结构化错误）。
#[test]
fn malformed_request_returns_error_not_hang() {
    let (router, _mgr, _events) = setup();

    // 非法 JSON → parse error 帧（id 为 null）
    let out = router.handle("{ not json");
    let resp = out.response.expect("parse error 应有响应");
    assert_eq!(resp["error"]["code"], crate::compat::RPC_ERROR_CODE);
    assert!(resp["error"]["message"].as_str().unwrap().contains("Parse"));

    // 缺 method 但带 id → invalid request 错误帧（不再丢弃）
    let out = router.handle(r#"{"jsonrpc":"2.0","id":7}"#);
    let resp = out.response.expect("invalid request 应有响应");
    assert_eq!(resp["id"], 7);
    assert_eq!(resp["error"]["code"], crate::compat::RPC_ERROR_CODE);

    // 缺 method 且无 id（通知）→ 按 JSON-RPC 约定无响应
    let out = router.handle(r#"{"jsonrpc":"2.0"}"#);
    assert!(out.response.is_none());
}

#[tokio::test]
async fn native_task_lifecycle_schema() {
    let (router, mgr, _events) = setup();
    let add = handle(
        &router,
        r#"{"jsonrpc":"2.0","id":1,"method":"task.add","params":{"uris":["http://127.0.0.1:9/x.zip"],"dir":"/tmp"}}"#,
    );
    let gid = add["result"]["gid"].as_str().unwrap().to_string();

    // task.tell：数值字段为 JSON 数值
    let tell = handle(
        &router,
        &format!(r#"{{"jsonrpc":"2.0","id":2,"method":"task.tell","params":{{"gid":"{gid}"}}}}"#),
    );
    assert_eq!(tell["result"]["gid"], gid);
    assert!(tell["result"]["totalLength"].is_number());
    assert!(tell["result"]["completedLength"].is_number());
    assert_eq!(tell["result"]["errorCode"], 0);

    // keys 过滤
    let tell = handle(
        &router,
        &format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"task.tell","params":{{"gid":"{gid}","keys":["gid","status"]}}}}"#
        ),
    );
    assert_eq!(tell["result"].as_object().unwrap().len(), 2);

    // task.list（下载会失败，但最终应出现在 stopped 或仍 active/waiting）
    let list = handle(
        &router,
        r#"{"jsonrpc":"2.0","id":4,"method":"task.list","params":{"scope":"all"}}"#,
    );
    assert!(list["result"]
        .as_array()
        .unwrap()
        .iter()
        .any(|t| t["gid"] == gid));

    // removeResult 前任务未结束 → 业务错误帧
    let r = handle(
        &router,
        &format!(
            r#"{{"jsonrpc":"2.0","id":5,"method":"task.removeResult","params":{{"gid":"{gid}"}}}}"#
        ),
    );
    assert!(r.get("error").is_some());

    // 等任务失败落停后可清理
    let _ = mgr;
}

#[tokio::test]
async fn native_auth_and_batch() {
    let mgr = TaskManager::new(std::env::temp_dir(), 2);
    let router = Arc::new(Router::new(
        Some("s3cret".into()),
        mgr.clone(),
        mgr.events(),
    ));

    // 无 token 拒绝
    let r = handle(
        &router,
        r#"{"jsonrpc":"2.0","id":1,"method":"engine.getVersion","params":{}}"#,
    );
    assert_eq!(r["error"]["message"], "Unauthorized");
    // token 通过
    let r = handle(
        &router,
        r#"{"jsonrpc":"2.0","id":2,"method":"engine.getVersion","params":{"token":"s3cret"}}"#,
    );
    assert_eq!(r["result"]["version"], ENGINE_VERSION);

    // batch：原生 + 兼容混合
    let r = handle(
        &router,
        r#"[
            {"jsonrpc":"2.0","id":10,"method":"engine.getVersion","params":{"token":"s3cret"}},
            {"jsonrpc":"2.0","id":11,"method":"aria2.getVersion","params":["token:s3cret"]}
        ]"#,
    );
    let arr = r.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["result"]["version"], ENGINE_VERSION);
    assert_eq!(arr[1]["result"]["version"], ENGINE_VERSION);
}

#[tokio::test]
async fn native_events_subscribe_flag() {
    let (router, _mgr, _events) = setup();
    let out = router.handle(r#"{"jsonrpc":"2.0","id":1,"method":"events.subscribe","params":{}}"#);
    assert!(out.native_subscribed);
    assert_eq!(out.proto_used, Proto::Native);
}

// ------------------------------------------------------------------
// 前端兼容协议
// ------------------------------------------------------------------

#[tokio::test]
async fn compat_get_version_and_auth() {
    let (router, _mgr, _events) = setup();
    let v = handle(
        &router,
        r#"{"jsonrpc":"2.0","id":1,"method":"aria2.getVersion","params":[]}"#,
    );
    assert_eq!(v["result"]["version"], ENGINE_VERSION);
    // 无前缀形式
    let v = handle(
        &router,
        r#"{"jsonrpc":"2.0","id":2,"method":"getVersion","params":[]}"#,
    );
    assert_eq!(v["result"]["version"], ENGINE_VERSION);

    let mgr = TaskManager::new(std::env::temp_dir(), 2);
    let router = Arc::new(Router::new(
        Some("s3cret".into()),
        mgr.clone(),
        mgr.events(),
    ));
    let r = handle(
        &router,
        r#"{"jsonrpc":"2.0","id":1,"method":"aria2.getVersion","params":[]}"#,
    );
    assert_eq!(r["error"]["message"], "Unauthorized");
    let r = handle(
        &router,
        r#"{"jsonrpc":"2.0","id":2,"method":"aria2.getVersion","params":["token:s3cret"]}"#,
    );
    assert_eq!(r["result"]["version"], ENGINE_VERSION);
}

#[tokio::test]
async fn compat_add_uri_status_and_multicall() {
    let (router, _mgr, _events) = setup();
    let add = handle(
        &router,
        r#"{"jsonrpc":"2.0","id":1,"method":"aria2.addUri","params":[["http://127.0.0.1:9/f.zip"],{"dir":"/tmp","out":"f.zip"}]}"#,
    );
    let gid = add["result"].as_str().unwrap().to_string();
    assert_eq!(gid.len(), 16);

    // tellStatus 字符串数值（兼容语义）
    let st = handle(
        &router,
        &format!(r#"{{"jsonrpc":"2.0","id":2,"method":"aria2.tellStatus","params":["{gid}"]}}"#),
    );
    assert!(st["result"]["totalLength"].is_string());
    assert_eq!(st["result"]["errorCode"], "0");

    // multicall：成功 [result] / 业务错误 {code,message} / 未知方法
    let mc = handle(
        &router,
        r#"{"jsonrpc":"2.0","id":3,"method":"system.multicall","params":[[
            {"methodName":"aria2.getVersion","params":[]},
            {"methodName":"aria2.tellStatus","params":["bbbbbbbbbbbbbbbb"]},
            {"methodName":"aria2.nope","params":[]}
        ]]}"#,
    );
    let arr = mc["result"].as_array().unwrap();
    assert_eq!(arr[0][0]["version"], ENGINE_VERSION);
    assert_eq!(arr[1]["code"], 1);
    assert_eq!(arr[2]["message"], "Method not found");

    // addTorrent 现为支持方法：参数缺失/非法时返回业务错误而非 Method not found
    let r = handle(
        &router,
        r#"{"jsonrpc":"2.0","id":4,"method":"aria2.addTorrent","params":[]}"#,
    );
    assert_eq!(r["error"]["message"], "参数 0（torrent base64）缺失");
}

#[tokio::test]
async fn event_frames_both_protocols() {
    let (router, _mgr, _events) = setup();
    let ev: EngineEvent = ("start".into(), "aaaaaaaaaaaaaaaa".into());
    let compat = router.compat_event_frame(&ev).unwrap();
    assert_eq!(compat["method"], "aria2.onDownloadStart");
    assert_eq!(compat["params"][0]["gid"], "aaaaaaaaaaaaaaaa");
    assert!(compat.get("id").is_none());

    // 原生事件帧：未知 gid 时无 payload（progress/error 需要查状态）
    let native = router.native_event_frame(&ev).unwrap();
    assert_eq!(native["method"], "task.start");
    assert_eq!(native["params"]["gid"], "aaaaaaaaaaaaaaaa");

    let progress: EngineEvent = ("progress".into(), "bbbbbbbbbbbbbbbb".into());
    assert!(router.native_event_frame(&progress).is_none());
    assert!(router.compat_event_frame(&progress).is_none());
}

// ------------------------------------------------------------------
// WS 端到端：单连接复用 + 协议族自动识别 + 事件推送
// ------------------------------------------------------------------

#[tokio::test]
async fn ws_e2e_native_and_compat_channels() {
    use futures_util::{SinkExt, StreamExt};

    let mgr = TaskManager::start(std::env::temp_dir(), 2);
    let events = mgr.events();
    let router = Arc::new(Router::new(None, mgr.clone(), events.clone()));
    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();

    let local = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = l.local_addr().unwrap();
        drop(l); // 释放后交给 serve 绑定
        a
    };
    // 用真实 serve 起服务
    let r3 = router.clone();
    let serve_task = tokio::spawn(async move {
        crate::serve(local, r3, sd).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // 连接 A：原生客户端（首条原生请求 → 只收原生帧）
    let (ws_a, _) = tokio_tungstenite::connect_async(format!("ws://{local}/jsonrpc"))
        .await
        .unwrap();
    let (mut tx_a, mut rx_a) = ws_a.split();
    tx_a.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"jsonrpc":"2.0","id":1,"method":"events.subscribe","params":{}}"#.into(),
    ))
    .await
    .unwrap();
    let msg = rx_a.next().await.unwrap().unwrap().into_text().unwrap();
    let v: Value = serde_json::from_str(&msg).unwrap();
    assert_eq!(v["id"], 1);
    assert_eq!(v["result"]["ok"], true);

    // 连接 B：前端客户端（首条 aria2 请求 → 只收 aria2 帧）
    let (ws_b, _) = tokio_tungstenite::connect_async(format!("ws://{local}/jsonrpc"))
        .await
        .unwrap();
    let (mut tx_b, mut rx_b) = ws_b.split();

    // 通过 B 添加任务 → 触发 start/error 事件（本地 127.0.0.1:9 拒绝连接）
    tx_b
        .send(tokio_tungstenite::tungstenite::Message::Text(
            r#"{"jsonrpc":"2.0","id":1,"method":"aria2.addUri","params":[["http://127.0.0.1:9/x.zip"],{"dir":"/tmp"}]}"#
                .into(),
        ))
        .await
        .unwrap();
    let msg = rx_b.next().await.unwrap().unwrap().into_text().unwrap();
    let v: Value = serde_json::from_str(&msg).unwrap();
    let gid = v["result"].as_str().unwrap().to_string();

    // B 应收到 aria2.onDownloadStart（自动推送，无需订阅）
    let mut got_start_b = false;
    let mut got_native_leak = false;
    for _ in 0..10 {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(3), rx_b.next()).await;
        let Ok(Some(Ok(m))) = msg else { break };
        let v: Value = serde_json::from_str(&m.into_text().unwrap()).unwrap();
        if v["method"] == "aria2.onDownloadStart" {
            got_start_b = true;
        }
        if v["method"]
            .as_str()
            .map(|s| s.starts_with("task."))
            .unwrap_or(false)
        {
            got_native_leak = true;
        }
        if v["method"] == "aria2.onDownloadError" {
            break;
        }
    }
    assert!(got_start_b, "B 应收到 aria2.onDownloadStart");
    assert!(!got_native_leak, "B 不应收到原生事件帧");

    // A 应收到 task.start（已订阅）
    let mut got_start_a = false;
    let mut got_compat_leak = false;
    for _ in 0..10 {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(3), rx_a.next()).await;
        let Ok(Some(Ok(m))) = msg else { break };
        let v: Value = serde_json::from_str(&m.into_text().unwrap()).unwrap();
        if v["method"] == "task.start" && v["params"]["gid"] == gid {
            got_start_a = true;
        }
        if v["method"]
            .as_str()
            .map(|s| s.starts_with("aria2."))
            .unwrap_or(false)
        {
            got_compat_leak = true;
        }
        if v["method"] == "task.error" {
            break;
        }
    }
    assert!(got_start_a, "A 应收到 task.start");
    assert!(!got_compat_leak, "A 不应收到前端事件帧");

    shutdown.cancel();
    let _ = serve_task.await;
    let _ = events;
}
