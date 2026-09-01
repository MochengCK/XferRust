//! M1 验收级集成测试：真实 HTTP 服务 + 真实任务管理器 + 真实落盘。
//!
//! 覆盖：完整下载 + 内容校验、checksum 正确/错误、暂停→恢复（Range 续传）、
//! URI 镜像故障转移、并发槽排队、事件通知、停止列表与全局统计。
//! 全部通过引擎公开 API 直调（不经过 RPC 协议层）。

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::http::{header, HeaderValue, StatusCode};
use serde_json::{json, Value};
use sha2::Digest;
use xfer_engine::TaskManager;
use xfer_types::Gid;

/// 测试数据生成：确定性伪随机。
fn make_data(len: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 31) % 251) as u8).collect()
}

fn temp_dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("xfer-m1-{tag}-{}", std::process::id()));
    // 控制文件目录隔离（不污染真实 ~/.xfer/ctrl；沙箱内写用户目录会被拒）
    let ctrl = std::env::temp_dir().join(format!("xfer-m1-ctrl-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&ctrl);
    std::env::set_var("XFER_CTRL_DIR", &ctrl);
    d
}

fn parse_gid(s: &str) -> Gid {
    Gid::parse(s).expect("gid 格式")
}

struct TestServer {
    base: String,
    /// 每个请求收到的 Range 头（验证续传证据）。
    range_log: Arc<Mutex<Vec<String>>>,
}

/// 启动测试服务器：
/// - `/file.bin`   Range 支持 + Content-Disposition 文件名
/// - `/slow.bin`   Range 支持 + 分块延迟（供暂停测试）
/// - `/missing`    404
async fn start_server(data: Vec<u8>, slow_delay: Duration) -> TestServer {
    let data = Arc::new(data);
    let range_log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    fn range_of(headers: &axum::http::HeaderMap) -> String {
        headers
            .get(header::RANGE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string()
    }

    fn partial(data: &[u8], start: usize) -> axum::response::Response {
        let mut r = axum::response::Response::new(axum::body::Body::from(data[start..].to_vec()));
        *r.status_mut() = StatusCode::PARTIAL_CONTENT;
        r.headers_mut().insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!(
                "bytes {}-{}/{}",
                start,
                data.len() - 1,
                data.len()
            ))
            .unwrap(),
        );
        r
    }

    let file_data = data.clone();
    let file_log = range_log.clone();
    let file_route = axum::routing::get(move |headers: axum::http::HeaderMap| {
        let data = file_data.clone();
        let log = file_log.clone();
        async move {
            let range = range_of(&headers);
            log.lock().unwrap().push(range.clone());
            let mut r = if range == "bytes=0-0" {
                partial(&data, 0)
            } else if let Some(start) = parse_range_start(&range) {
                if start < data.len() {
                    partial(&data, start)
                } else {
                    let mut r = axum::response::Response::new(axum::body::Body::empty());
                    *r.status_mut() = StatusCode::RANGE_NOT_SATISFIABLE;
                    r
                }
            } else {
                axum::response::Response::new(axum::body::Body::from(data.as_ref().clone()))
            };
            r.headers_mut().insert(
                header::CONTENT_DISPOSITION,
                HeaderValue::from_static("attachment; filename=\"cdn-name.bin\""),
            );
            r
        }
    });

    let slow_data = data.clone();
    let slow_log = range_log.clone();
    let slow_route = axum::routing::get(move |headers: axum::http::HeaderMap| {
        let data = slow_data.clone();
        let log = slow_log.clone();
        async move {
            let range = range_of(&headers);
            log.lock().unwrap().push(range.clone());
            if range == "bytes=0-0" {
                return partial(&data, 0);
            }
            let start = parse_range_start(&range).unwrap_or(0);
            if start >= data.len() {
                let mut r = axum::response::Response::new(axum::body::Body::empty());
                *r.status_mut() = StatusCode::RANGE_NOT_SATISFIABLE;
                return r;
            }
            // 分块流式输出（带延迟）
            const CHUNK: usize = 16 * 1024;
            let rest = data[start..].to_vec();
            let total = data.len();
            let stream = futures_util::stream::unfold(0usize, move |i| {
                let rest = rest.clone();
                async move {
                    if i >= rest.len() {
                        return None;
                    }
                    tokio::time::sleep(slow_delay).await;
                    let end = (i + CHUNK).min(rest.len());
                    let item: Result<axum::body::Bytes, std::convert::Infallible> =
                        Ok(axum::body::Bytes::copy_from_slice(&rest[i..end]));
                    Some((item, end))
                }
            });
            let mut r = axum::response::Response::new(axum::body::Body::from_stream(stream));
            *r.status_mut() = StatusCode::PARTIAL_CONTENT;
            r.headers_mut().insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes {}-{}/{}", start, total - 1, total)).unwrap(),
            );
            r
        }
    });

    let missing_route = axum::routing::get(|| async { StatusCode::NOT_FOUND });

    let app = axum::Router::new()
        .route("/file.bin", file_route)
        .route("/slow.bin", slow_route)
        .route("/missing", missing_route);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    TestServer { base, range_log }
}

fn parse_range_start(range: &str) -> Option<usize> {
    // 兼容开放式 `bytes=N-` 与闭合区间 `bytes=N-M`（分片下载）
    let s = range.strip_prefix("bytes=")?;
    s.split('-').next()?.parse().ok()
}

fn add(mgr: &Arc<TaskManager>, uris: Vec<String>, opts: Value) -> String {
    mgr.add_uri(uris, &opts, None).expect("add_uri").0
}

fn status(mgr: &Arc<TaskManager>, gid: &str) -> Value {
    mgr.tell_status(&parse_gid(gid), None).expect("tell_status")
}

/// 轮询任务状态直到期望值或超时；返回最后状态快照。
async fn wait_status(mgr: &Arc<TaskManager>, gid: &str, want: &str, timeout: Duration) -> Value {
    let deadline = Instant::now() + timeout;
    loop {
        let st = status(mgr, gid);
        if st["status"] == want {
            return st;
        }
        assert!(
            Instant::now() < deadline,
            "等待状态超时: {want}，当前快照: {st}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// 收集事件直到满足条件或超时。
async fn wait_event(
    rx: &mut tokio::sync::broadcast::Receiver<(String, String)>,
    pred: impl Fn(&(String, String)) -> bool,
    timeout: Duration,
) -> Vec<(String, String)> {
    let mut got = Vec::new();
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Ok(ev)) => {
                let hit = pred(&ev);
                got.push(ev);
                if hit {
                    return got;
                }
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => continue,
            _ => break,
        }
    }
    got
}

const DATA_LEN: usize = 256 * 1024;

#[tokio::test]
async fn download_complete_and_checksum_flow() {
    let data = make_data(DATA_LEN);
    let server = start_server(data.clone(), Duration::ZERO).await;
    let dir = temp_dir("basic");
    let _ = std::fs::remove_dir_all(&dir);

    let mgr = TaskManager::start(dir.clone(), 2);
    let mut events = mgr.events().subscribe();

    // 1) 常规下载：out 指定文件名
    let gid = add(
        &mgr,
        vec![format!("{}/file.bin", server.base)],
        json!({"dir": dir.to_string_lossy(), "out": "out.bin"}),
    );
    let st = wait_status(&mgr, &gid, "complete", Duration::from_secs(15)).await;
    assert_eq!(st["totalLength"], DATA_LEN.to_string());
    assert_eq!(st["completedLength"], DATA_LEN.to_string());
    assert_eq!(st["errorCode"], "0");
    let saved = std::fs::read(dir.join("out.bin")).unwrap();
    assert_eq!(saved, data);

    // 事件：start + complete
    let got = wait_event(
        &mut events,
        |ev| ev.0 == "complete" && ev.1 == gid,
        Duration::from_secs(5),
    )
    .await;
    assert!(
        got.iter().any(|ev| ev.0 == "start" && ev.1 == gid),
        "应收到 start 事件: {got:?}"
    );

    // 2) checksum 正确 → complete（未指定 out：用服务器文件名）
    let sha256 = hex::encode(sha2::Sha256::digest(&data));
    let gid2 = add(
        &mgr,
        vec![format!("{}/file.bin", server.base)],
        json!({"dir": dir.to_string_lossy(), "checksum": format!("sha-256={sha256}")}),
    );
    let st = wait_status(&mgr, &gid2, "complete", Duration::from_secs(15)).await;
    let path2 = st["files"][0]["path"].as_str().unwrap().to_string();
    assert!(
        path2.ends_with("cdn-name.bin"),
        "应使用服务器文件名: {path2}"
    );

    // 3) checksum 错误 → error code 9（文件自动改名避免冲突）
    let gid3 = add(
        &mgr,
        vec![format!("{}/file.bin", server.base)],
        json!({
            "dir": dir.to_string_lossy(),
            "checksum": format!("sha-256={}", "00".repeat(32)),
        }),
    );
    let st = wait_status(&mgr, &gid3, "error", Duration::from_secs(15)).await;
    assert_eq!(st["errorCode"], "9", "checksum 不符应为错误码 9");

    // 4) 停止列表与全局统计
    let stopped = mgr.tell_stopped(0, 10, None);
    assert!(stopped.as_array().unwrap().len() >= 2);
    let stat = mgr.global_stat();
    assert_eq!(stat["numStoppedTotal"], "3");
    assert_eq!(stat["numActive"], "0");
    assert_eq!(stat["downloadSpeed"], "0");

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn pause_then_resume_via_range() {
    let data = make_data(DATA_LEN);
    let server = start_server(data.clone(), Duration::from_millis(15)).await;
    let dir = temp_dir("pause");
    let _ = std::fs::remove_dir_all(&dir);

    let mgr = TaskManager::start(dir.clone(), 1);
    let mut events = mgr.events().subscribe();

    let gid = add(
        &mgr,
        vec![format!("{}/slow.bin", server.base)],
        json!({"dir": dir.to_string_lossy(), "out": "slow.bin"}),
    );

    // 等到有进度后暂停
    let mut completed_at_pause = 0u64;
    for _ in 0..200 {
        let st = status(&mgr, &gid);
        let c: u64 = st["completedLength"].as_str().unwrap().parse().unwrap();
        if c > 0 && st["status"] == "active" {
            completed_at_pause = c;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(completed_at_pause > 0, "下载应已开始推进");

    mgr.pause(&parse_gid(&gid)).unwrap();
    let st = wait_status(&mgr, &gid, "paused", Duration::from_secs(5)).await;
    let paused_len: u64 = st["completedLength"].as_str().unwrap().parse().unwrap();
    assert!(paused_len >= completed_at_pause);

    let got = wait_event(
        &mut events,
        |ev| ev.0 == "pause" && ev.1 == gid,
        Duration::from_secs(2),
    )
    .await;
    assert!(
        got.iter().any(|ev| ev.0 == "pause"),
        "应收到 pause 事件: {got:?}"
    );

    // 恢复下载 → 完成
    mgr.unpause(&parse_gid(&gid)).unwrap();
    wait_status(&mgr, &gid, "complete", Duration::from_secs(15)).await;

    // 续传证据：存在 bytes=N- 且 N>0 的请求
    let ranges = server.range_log.lock().unwrap().clone();
    assert!(
        ranges
            .iter()
            .any(|r| parse_range_start(r).map(|n| n > 0).unwrap_or(false)),
        "应存在断点续传请求: {ranges:?}"
    );

    let saved = std::fs::read(dir.join("slow.bin")).unwrap();
    assert_eq!(saved, data, "续传结果内容必须与源一致");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn mirror_failover_and_concurrency() {
    let data = make_data(DATA_LEN);
    let server = start_server(data.clone(), Duration::from_millis(10)).await;
    let dir = temp_dir("queue");
    let _ = std::fs::remove_dir_all(&dir);

    let mgr = TaskManager::start(dir.clone(), 1);

    // 镜像故障转移：第一个 URI 404，第二个可用
    let gid = add(
        &mgr,
        vec![
            format!("{}/missing", server.base),
            format!("{}/file.bin", server.base),
        ],
        json!({"dir": dir.to_string_lossy(), "out": "mirror.bin"}),
    );
    wait_status(&mgr, &gid, "complete", Duration::from_secs(15)).await;
    assert_eq!(std::fs::read(dir.join("mirror.bin")).unwrap(), data);

    // 并发槽 = 1：第二个任务排队等待
    let gid_a = add(
        &mgr,
        vec![format!("{}/slow.bin", server.base)],
        json!({"dir": dir.to_string_lossy(), "out": "a.bin"}),
    );
    let gid_b = add(
        &mgr,
        vec![format!("{}/slow.bin", server.base)],
        json!({"dir": dir.to_string_lossy(), "out": "b.bin"}),
    );

    // 等 a 被调度起来后：b 应为 waiting（a 占用唯一并发槽）
    wait_status(&mgr, &gid_a, "active", Duration::from_secs(5)).await;
    let st_b = status(&mgr, &gid_b);
    assert_eq!(st_b["status"], "waiting");
    let waiting = mgr.tell_waiting(0, 10, None);
    assert_eq!(waiting.as_array().unwrap().len(), 1);

    wait_status(&mgr, &gid_a, "complete", Duration::from_secs(30)).await;
    wait_status(&mgr, &gid_b, "complete", Duration::from_secs(30)).await;
    assert_eq!(std::fs::read(dir.join("a.bin")).unwrap(), data);
    assert_eq!(std::fs::read(dir.join("b.bin")).unwrap(), data);

    // remove 已完成任务 → 彻底移除（含历史记录），无需再调 removeDownloadResult
    assert!(mgr.remove(&parse_gid(&gid_b)).is_ok());
    assert!(mgr.tell_status(&parse_gid(&gid_b), None).is_err());
    // 移除后任务不在停止列表中
    let stopped = mgr.tell_stopped(0, 1000, None);
    assert!(!stopped
        .as_array()
        .unwrap()
        .iter()
        .any(|t| t["gid"].as_str() == Some(gid_b.as_str())));
    // 移除后数据文件仍在（未勾选删除文件）
    assert!(dir.join("b.bin").exists());

    let _ = std::fs::remove_dir_all(&dir);
}

/// 会话持久化：设置项 + 任务历史（完成/暂停）跨管理器实例恢复，
/// 暂停任务恢复后可继续断点续传。
#[tokio::test]
async fn session_persistence_and_restore() {
    let data = make_data(DATA_LEN);
    let server = start_server(data.clone(), Duration::from_millis(15)).await;
    let root = temp_dir("session");
    let _ = std::fs::remove_dir_all(&root);
    let dl_dir = root.join("dl");
    let _ = std::fs::create_dir_all(&dl_dir);
    let session = root.join("session.json");
    let _ = std::fs::remove_file(&session);

    // 第一段实例：完成任务 + 修改设置 + 留一个暂停中的任务
    let mgr = TaskManager::start_with_session(Some(dl_dir.clone()), Some(1), session.clone());
    let gid_done = add(
        &mgr,
        vec![format!("{}/file.bin", server.base)],
        json!({"dir": dl_dir.to_string_lossy(), "out": "done.bin"}),
    );
    wait_status(&mgr, &gid_done, "complete", Duration::from_secs(15)).await;

    let gid_paused = add(
        &mgr,
        vec![format!("{}/slow.bin", server.base)],
        json!({"dir": dl_dir.to_string_lossy(), "out": "paused.bin"}),
    );
    // 等有进度后暂停（走 active→paused 异步转移）
    let mut advanced = false;
    for _ in 0..200 {
        let st = status(&mgr, &gid_paused);
        let c: u64 = st["completedLength"].as_str().unwrap().parse().unwrap();
        if c > 0 && st["status"] == "active" {
            advanced = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(advanced, "下载应已开始推进");
    mgr.pause(&parse_gid(&gid_paused)).unwrap();
    wait_status(&mgr, &gid_paused, "paused", Duration::from_secs(5)).await;

    mgr.change_global_option(&json!({"max-concurrent-downloads": "7"}))
        .unwrap();
    drop(mgr);

    // 会话文件已生成（终态/暂停/设置变更触发自动保存）
    let text = std::fs::read_to_string(&session).expect("会话文件应已生成");
    assert!(text.contains(&gid_done), "会话应包含完成任务: {text}");

    // 第二段实例：显式参数缺省 → 采用会话设置恢复
    let mgr2 = TaskManager::start_with_session(None, None, session.clone());
    let opts = mgr2.get_global_option();
    assert_eq!(opts["max-concurrent-downloads"], "7");
    assert_eq!(opts["dir"].as_str().unwrap(), dl_dir.to_string_lossy());

    // 完成历史恢复（含进度快照）
    let st = status(&mgr2, &gid_done);
    assert_eq!(st["status"], "complete");
    assert_eq!(st["completedLength"], DATA_LEN.to_string());
    assert_eq!(
        st["files"][0]["path"],
        dl_dir.join("done.bin").to_string_lossy().as_ref(),
        "恢复任务应还原 path 字段"
    );
    let stopped = mgr2.tell_stopped(0, 10, None);
    assert!(stopped
        .as_array()
        .unwrap()
        .iter()
        .any(|t| t["gid"] == gid_done.as_str()));

    // 暂停任务恢复为 paused → 继续下载完成（断点续传）
    assert_eq!(status(&mgr2, &gid_paused)["status"], "paused");
    mgr2.unpause(&parse_gid(&gid_paused)).unwrap();
    wait_status(&mgr2, &gid_paused, "complete", Duration::from_secs(30)).await;
    assert_eq!(
        std::fs::read(dl_dir.join("paused.bin")).unwrap(),
        data,
        "续传结果内容必须与源一致"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// 设置持久化回归：命令行参数（-j/-d）只做本次会话的临时覆盖，
/// 退出保存后再次无参数启动应恢复用户在界面里保存的设置，
/// 而不是被上次的临时参数改写。
#[tokio::test]
async fn settings_survive_cli_override() {
    let root = temp_dir("session-cli-override");
    let _ = std::fs::remove_dir_all(&root);
    let dl_dir = root.join("dl");
    let _ = std::fs::create_dir_all(&dl_dir);
    let session = root.join("session.json");
    let _ = std::fs::remove_file(&session);
    let saved_dir = root.join("saved");

    // 实例 A：用户在界面里保存设置（并发 8 / 目录 saved_dir）
    let mgr = TaskManager::start_with_session(Some(dl_dir.clone()), Some(3), session.clone());
    mgr.change_global_option(&json!({
        "max-concurrent-downloads": "8",
        "dir": saved_dir.to_string_lossy(),
    }))
    .unwrap();
    drop(mgr);

    // 实例 B：带命令行参数启动（临时覆盖为 1）并正常退出保存
    let mgr = TaskManager::start_with_session(Some(dl_dir.clone()), Some(1), session.clone());
    assert_eq!(
        mgr.get_global_option()["max-concurrent-downloads"],
        "1",
        "命令行参数应在本次会话内生效"
    );
    mgr.save_session().unwrap();
    drop(mgr);

    // 实例 C：无参数启动 → 恢复用户保存的设置（8 / saved_dir），而非临时值
    let mgr = TaskManager::start_with_session(None, None, session.clone());
    let opts = mgr.get_global_option();
    assert_eq!(
        opts["max-concurrent-downloads"], "8",
        "无参数启动应恢复用户保存的并发设置，而不是被上次命令行临时值改写"
    );
    assert_eq!(
        opts["dir"].as_str().unwrap(),
        saved_dir.to_string_lossy().as_ref(),
        "无参数启动应恢复用户保存的下载目录"
    );

    let _ = std::fs::remove_dir_all(&root);
}
