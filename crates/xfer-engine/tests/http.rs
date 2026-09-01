//! xfer-engine HTTP 集成：多连接分片下载全链路（TaskManager::add_uri）。
//!
//! 验证：并发连接数（split 选项生效）、文件正确性、暂停/续传
//! （控制文件）、崩溃重启续传、单连接回退（服务器谎报 Range 支持）。

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use futures_util::StreamExt;
use tokio::net::TcpListener;
use xfer_engine::TaskManager;
use xfer_http::ctrl_path;
use xfer_types::Gid;

/// 位置敏感的确定性数据：任何错位写都会被校验出来。
fn sample(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// Drop 时递减的在途请求计数流（真实并发峰值）。
struct GuardedStream<S> {
    inner: std::pin::Pin<Box<S>>,
    inflight: Arc<AtomicUsize>,
}
impl<S: futures_util::Stream> futures_util::Stream for GuardedStream<S> {
    type Item = S::Item;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}
impl<S> Drop for GuardedStream<S> {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, Ordering::SeqCst);
    }
}

struct HttpServer {
    addr: SocketAddr,
    peak: Arc<AtomicUsize>,
    served: Arc<AtomicU64>,
}

impl HttpServer {
    fn url(&self) -> String {
        format!("http://{}/file.bin", self.addr)
    }
}

/// 支持 Range 的测试服务。`chunk_delay` 每块发送间隔；
/// `lie_200` = 对 from>0 的请求谎报 200（测回退）。
async fn start_server(data: Arc<Vec<u8>>, chunk_delay: Duration, lie_200: bool) -> HttpServer {
    let peak = Arc::new(AtomicUsize::new(0));
    let inflight = Arc::new(AtomicUsize::new(0));
    let served = Arc::new(AtomicU64::new(0));
    let peak_ret = peak.clone();
    let served_ret = served.clone();
    let app = Router::new().route(
        "/file.bin",
        get(move |headers: axum::http::HeaderMap| {
            let data = data.clone();
            let peak = peak.clone();
            let inflight = inflight.clone();
            let served = served.clone();
            let delay = chunk_delay;
            let lie_200 = lie_200;
            async move {
                let range = headers
                    .get(header::RANGE)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_string();
                let total = data.len();
                let (from, to) = match range.strip_prefix("bytes=").and_then(|r| r.split_once('-'))
                {
                    Some((f, t)) => (
                        f.parse::<usize>().unwrap_or(0),
                        t.parse::<usize>().unwrap_or(total),
                    ),
                    None => (0, total),
                };
                let from = from.min(total);
                let to = (to + 1).min(total).max(from);
                // 谎报 200：对 from>0 的 Range 请求返回 200 + 完整文件
                // （符合 HTTP 语义：200 必须携带完整资源）。
                let lied = lie_200 && from > 0;
                let (from, to) = if lied { (0, total) } else { (from, to) };
                let mut chunks: Vec<Result<Vec<u8>, std::io::Error>> = Vec::new();
                let mut off = from;
                while off < to {
                    let end = (off + 8192).min(to);
                    chunks.push(Ok(data[off..end].to_vec()));
                    off = end;
                }
                served.fetch_add((to - from) as u64, Ordering::Relaxed);
                // inflight 随请求生命周期增减；peak 只单调记录历史峰值。
                let n = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(n, Ordering::SeqCst);
                let stream = futures_util::stream::iter(chunks).then(move |c| {
                    let delay = delay;
                    async move {
                        if !delay.is_zero() {
                            tokio::time::sleep(delay).await;
                        }
                        c
                    }
                });
                let guarded = GuardedStream {
                    inner: Box::pin(stream),
                    inflight: inflight.clone(),
                };
                let mut resp = Response::new(axum::body::Body::from_stream(guarded));
                if lied {
                    // 谎报 200：测单连接回退（body 已是完整文件）
                } else if from > 0 || to < total {
                    *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
                    resp.headers_mut().insert(
                        header::CONTENT_RANGE,
                        HeaderValue::from_str(&format!(
                            "bytes {}-{}/{}",
                            from,
                            to.saturating_sub(1),
                            total
                        ))
                        .unwrap(),
                    );
                }
                resp
            }
        }),
    );
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(l, app).await;
    });
    HttpServer {
        addr,
        peak: peak_ret,
        served: served_ret,
    }
}

fn tmpdir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("xfer-engine-http-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    // 控制文件目录隔离（不污染真实 ~/.xfer/ctrl；同进程路径哈希互不冲突）
    let ctrl = std::env::temp_dir().join(format!("xfer-engine-http-ctrl-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&ctrl);
    std::env::set_var("XFER_CTRL_DIR", &ctrl);
    d
}

/// 任务目标文件对应的控制文件路径（存于隔离的数据目录，不在下载目录）。
fn ctrl_of(dir: &Path, file: &str) -> PathBuf {
    ctrl_path(&dir.join(file))
}

async fn wait_status(
    mgr: &TaskManager,
    gid: &Gid,
    want: &str,
    limit_ms: u64,
) -> Option<serde_json::Value> {
    let mut waited = 0u64;
    loop {
        tokio::time::sleep(Duration::from_millis(50)).await;
        waited += 50;
        let st = mgr.tell_status_native(gid, None).unwrap();
        let cur = st["status"].as_str().unwrap_or_default();
        if cur == want {
            return Some(st);
        }
        if cur == "error" {
            panic!("任务进入 error: {st}");
        }
        if waited >= limit_ms {
            return None;
        }
    }
}

/// 等待任务完成度超过 `min_bytes`，返回当时进度。
async fn wait_progress(mgr: &TaskManager, gid: &Gid, min_bytes: u64, limit_ms: u64) -> Option<u64> {
    let mut waited = 0u64;
    loop {
        tokio::time::sleep(Duration::from_millis(50)).await;
        waited += 50;
        let st = mgr.tell_status_native(gid, None).unwrap();
        let c = st["completedLength"].as_u64().unwrap_or(0);
        if c >= min_bytes {
            return Some(c);
        }
        if waited >= limit_ms {
            return None;
        }
    }
}

/// 前 `fail_first` 个请求返回 500（模拟服务端瞬时故障），之后正常。
/// 验证 drive_download 的同 URI 瞬态重试：探测 500 → 重试 → 成功。
async fn start_flaky_server(data: Arc<Vec<u8>>, fail_first: usize) -> SocketAddr {
    let fails = Arc::new(AtomicUsize::new(fail_first));
    let app = Router::new().route(
        "/file.bin",
        get(move |headers: axum::http::HeaderMap| {
            let data = data.clone();
            let fails = fails.clone();
            async move {
                // 原子递减故障余额（用 fetch_update 防 usize 下溢：
                // 下溢成 MAX 会让后续所有请求永久 500）
                let failed = fails
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                        if n > 0 {
                            Some(n - 1)
                        } else {
                            None
                        }
                    })
                    .is_ok();
                if failed {
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
                let total = data.len();
                let range = headers
                    .get(header::RANGE)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_string();
                let (from, to) = match range.strip_prefix("bytes=").and_then(|r| r.split_once('-')) {
                    Some((f, t)) => (
                        f.parse::<usize>().unwrap_or(0),
                        (t.parse::<usize>().unwrap_or(total - 1) + 1).min(total),
                    ),
                    None => (0, total),
                };
                let from = from.min(total);
                let to = to.max(from);
                let mut resp = Response::new(axum::body::Body::from(data[from..to].to_vec()));
                if from > 0 || to < total {
                    *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
                    resp.headers_mut().insert(
                        header::CONTENT_RANGE,
                        HeaderValue::from_str(&format!("bytes {}-{}/{}", from, to - 1, total))
                            .unwrap(),
                    );
                }
                resp
            }
        }),
    );
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(l, app).await;
    });
    addr
}

/// 服务端瞬时故障（500）后同 URI 重试应恢复并完成下载，
/// 而不是把任务打入 error。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transient_server_error_retries_same_uri() {
    let dir = tmpdir("retry");
    let data = Arc::new(sample(512 * 1024));
    // 前 2 个请求 500（探测 3 次：两次失败 + 一次成功）
    let addr = start_flaky_server(data.clone(), 2).await;
    let url = format!("http://{addr}/file.bin");

    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![url],
            &serde_json::json!({"dir": dir, "split": "4", "min-split-size": "64K"}),
            None,
        )
        .expect("addUri 应成功");

    // 重试含 1s + 3s 退避，放宽超时
    let st = wait_status(&mgr, &gid, "complete", 30_000)
        .await
        .expect("瞬态 500 重试后 30s 内未完成");
    assert_eq!(st["completedLength"], (512 * 1024) as u64);
    let out = std::fs::read(dir.join("file.bin")).unwrap();
    assert_eq!(out, *data, "重试下载文件与源数据不一致");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_connection_split_download() {
    let dir = tmpdir("split");
    let data = Arc::new(sample(2 * 1024 * 1024));
    let srv = start_server(data.clone(), Duration::from_millis(2), false).await;

    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![srv.url()],
            &serde_json::json!({
                "dir": dir,
                "split": "8",
                "min-split-size": "64K",
            }),
            None,
        )
        .expect("addUri 应成功");

    let st = wait_status(&mgr, &gid, "complete", 30_000)
        .await
        .expect("30s 内未完成");
    assert_eq!(st["totalLength"], (2 * 1024 * 1024) as u64);
    assert_eq!(st["completedLength"], (2 * 1024 * 1024) as u64);
    // 完成后连接数归零
    assert_eq!(st["connections"], 0);

    let out = std::fs::read(dir.join("file.bin")).unwrap();
    assert_eq!(out, *data, "多连接下载文件与源数据不一致");

    // 分片并发真实发生（峰值 ≥ 2 个在途请求）
    assert!(
        srv.peak.load(Ordering::SeqCst) >= 2,
        "未观察到并发请求: peak={}",
        srv.peak.load(Ordering::SeqCst)
    );
    // 控制文件已清理
    assert!(!ctrl_of(&dir, "file.bin").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_option_limits_connections() {
    // 任务级 split=2 应限制并发（服务器峰值 ≤ 2 + 偶发重叠的探测请求）
    let dir = tmpdir("limit");
    let data = Arc::new(sample(512 * 1024));
    let srv = start_server(data.clone(), Duration::from_millis(3), false).await;

    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![srv.url()],
            &serde_json::json!({
                "dir": dir,
                "split": "2",
                "max-connection-per-server": "2",
                "min-split-size": "32K",
            }),
            None,
        )
        .expect("addUri 应成功");

    wait_status(&mgr, &gid, "complete", 30_000)
        .await
        .expect("30s 内未完成");
    let out = std::fs::read(dir.join("file.bin")).unwrap();
    assert_eq!(out, *data);
    assert!(
        srv.peak.load(Ordering::SeqCst) <= 3,
        "split=2 时并发超出: peak={}",
        srv.peak.load(Ordering::SeqCst)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_resume_with_ctrl_file() {
    let dir = tmpdir("pause");
    let data = Arc::new(sample(2 * 1024 * 1024));
    // 慢速服务保证有充足时间暂停
    let srv = start_server(data.clone(), Duration::from_millis(5), false).await;

    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![srv.url()],
            &serde_json::json!({
                "dir": dir,
                "split": "4",
                "min-split-size": "64K",
            }),
            None,
        )
        .expect("addUri 应成功");

    // 等下载有进度后暂停
    let progressed = wait_progress(&mgr, &gid, 100 * 1024, 15_000).await;
    assert!(progressed.is_some(), "未观察到下载进度");
    mgr.pause(&gid).expect("pause 应成功");
    let st = wait_status(&mgr, &gid, "paused", 10_000)
        .await
        .expect("暂停超时");
    assert!(st["completedLength"].as_u64().unwrap() > 0);
    // 暂停后保留控制文件（断点续传）
    assert!(ctrl_of(&dir, "file.bin").exists(), "暂停后应保留控制文件");

    // 继续下载直至完成
    mgr.unpause(&gid).expect("unpause 应成功");
    let st = wait_status(&mgr, &gid, "complete", 60_000)
        .await
        .expect("续传 60s 内未完成");
    assert_eq!(st["completedLength"], (2 * 1024 * 1024) as u64);
    let out = std::fs::read(dir.join("file.bin")).unwrap();
    assert_eq!(out, *data, "续传后文件与源数据不一致");
    assert!(!ctrl_of(&dir, "file.bin").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn not_splittable_server_falls_back_to_single_connection() {
    let dir = tmpdir("fallback");
    let data = Arc::new(sample(256 * 1024));
    let srv = start_server(data.clone(), Duration::ZERO, true).await;

    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![srv.url()],
            &serde_json::json!({
                "dir": dir,
                "split": "4",
                "min-split-size": "32K",
            }),
            None,
        )
        .expect("addUri 应成功");

    let st = wait_status(&mgr, &gid, "complete", 30_000)
        .await
        .expect("回退单连接后 30s 内未完成");
    assert_eq!(st["completedLength"], (256 * 1024) as u64);
    let out = std::fs::read(dir.join("file.bin")).unwrap();
    assert_eq!(out, *data, "回退下载文件与源数据不一致");
    // 回退后控制文件不应残留
    assert!(!ctrl_of(&dir, "file.bin").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_engine_resumes_from_ctrl_file() {
    // 第一台引擎：跑到一半"崩溃"（直接丢弃，不调 save_session）
    let dir = tmpdir("restart");
    let data = Arc::new(sample(2 * 1024 * 1024));
    let srv = start_server(data.clone(), Duration::from_millis(4), false).await;

    let first = TaskManager::start(dir.clone(), 2);
    let gid = first
        .add_uri(
            vec![srv.url()],
            &serde_json::json!({
                "dir": dir,
                "split": "4",
                "min-split-size": "64K",
            }),
            None,
        )
        .expect("addUri 应成功");
    let progressed = wait_progress(&first, &gid, 150 * 1024, 15_000).await;
    assert!(progressed.is_some(), "未观察到下载进度");
    // 先停掉任务（避免 zombie 任务与第二台引擎同时写同一文件），
    // 再丢弃管理器模拟进程退出（不走 save_session）
    first.pause(&gid).expect("pause 应成功");
    wait_status(&first, &gid, "paused", 10_000)
        .await
        .expect("暂停超时");
    let first_served = srv.served.load(Ordering::Relaxed);
    drop(first);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(ctrl_of(&dir, "file.bin").exists(), "崩溃后应有控制文件");

    // 第二台引擎：新 gid，靠控制文件 + 文件长度续传
    let second = TaskManager::start(dir.clone(), 2);
    let gid2 = second
        .add_uri(
            vec![srv.url()],
            &serde_json::json!({
                "dir": dir,
                "split": "4",
                "min-split-size": "64K",
            }),
            None,
        )
        .expect("addUri 应成功");
    let st = wait_status(&second, &gid2, "complete", 60_000)
        .await
        .expect("重启续传 60s 内未完成");
    assert_eq!(st["completedLength"], (2 * 1024 * 1024) as u64);
    let out = std::fs::read(dir.join("file.bin")).unwrap();
    assert_eq!(out, *data, "重启续传后文件与源数据不一致");
    assert!(!ctrl_of(&dir, "file.bin").exists());
    // 第二轮确实跳过了已完成部分
    let second_served = srv.served.load(Ordering::Relaxed) - first_served;
    assert!(
        second_served < 2 * 1024 * 1024,
        "续传应少下字节: second_served={second_served}"
    );
}

/// 移除任务：控制文件必须清理；勾选删除时数据文件一并删除，
/// 未勾选时数据文件保留。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remove_cleans_ctrl_and_optional_files() {
    let dir = tmpdir("remove-clean");
    // 20ms/块制造足够慢的下载，保证 pause/remove 时任务仍在 active
    let data = Arc::new(sample(1024 * 1024));
    let srv = start_server(data.clone(), Duration::from_millis(20), false).await;

    // 场景一：暂停任务移除（保留文件）→ 数据文件在、控制文件删
    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![srv.url()],
            &serde_json::json!({"dir": dir, "split": "2", "min-split-size": "64K"}),
            None,
        )
        .expect("addUri 应成功");
    let progressed = wait_progress(&mgr, &gid, 100 * 1024, 15_000).await;
    assert!(progressed.is_some(), "未观察到下载进度");
    mgr.pause(&gid).expect("pause 应成功");
    wait_status(&mgr, &gid, "paused", 10_000)
        .await
        .expect("暂停超时");
    assert!(ctrl_of(&dir, "file.bin").exists(), "移除前应有控制文件");

    mgr.remove_with_files(&gid, false).expect("移除应成功");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !ctrl_of(&dir, "file.bin").exists(),
        "任务移除后控制文件应删除"
    );
    assert!(dir.join("file.bin").exists(), "未勾选删除时数据文件应保留");

    // 场景二：active 任务移除（勾选删除）→ 数据文件与控制文件都删
    let gid2 = mgr
        .add_uri(
            vec![srv.url()],
            &serde_json::json!({"dir": dir, "split": "2", "min-split-size": "64K", "out": "del.bin"}),
            None,
        )
        .expect("addUri 应成功");
    let progressed = wait_progress(&mgr, &gid2, 50 * 1024, 15_000).await;
    assert!(progressed.is_some(), "未观察到下载进度");
    mgr.remove_with_files(&gid2, true)
        .expect("active 移除应成功");
    // active 移除是异步的：等 worker 退出、文件删除完成
    let mut deleted = false;
    for _ in 0..200 {
        if !dir.join("del.bin").exists() {
            deleted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(deleted, "勾选删除时数据文件应删除");
    assert!(!ctrl_of(&dir, "del.bin").exists(), "移除后控制文件应删除");

    // 移除后任务不应出现在历史记录中
    let stopped = mgr.tell_stopped(0, 1000, None);
    let in_stopped = stopped
        .as_array()
        .unwrap()
        .iter()
        .any(|t| t["gid"].as_str() == Some(gid.0.as_str()));
    assert!(!in_stopped, "移除任务不应出现在停止列表中");
    let in_stopped2 = stopped
        .as_array()
        .unwrap()
        .iter()
        .any(|t| t["gid"].as_str() == Some(gid2.0.as_str()));
    assert!(!in_stopped2, "移除任务不应出现在停止列表中");
}

/// 暂停后已用时冻结：paused 状态下 elapsedMs 不再增长，恢复后继续累计。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_elapsed_freezes() {
    let dir = tmpdir("elapsed");
    // 20ms/块 + 2 连接：active 期持续约 2.5s，足够观察到 elapsed 累计
    let data = Arc::new(sample(2 * 1024 * 1024));
    let srv = start_server(data.clone(), Duration::from_millis(20), false).await;

    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![srv.url()],
            &serde_json::json!({"dir": dir, "split": "2", "min-split-size": "64K"}),
            None,
        )
        .expect("addUri 应成功");

    // 等任务 active 且累计一点时长
    let mut active_ms = 0u64;
    for _ in 0..200 {
        let st = mgr.tell_status_native(&gid, None).unwrap();
        if st["status"] == "active" && st["elapsedMs"].as_u64().unwrap_or(0) > 1200 {
            active_ms = st["elapsedMs"].as_u64().unwrap();
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(active_ms > 1200, "active 期间 elapsed 应累计: {active_ms}");

    // 暂停：elapsed 冻结
    mgr.pause(&gid).expect("pause 应成功");
    wait_status(&mgr, &gid, "paused", 10_000)
        .await
        .expect("暂停超时");
    tokio::time::sleep(Duration::from_millis(800)).await;
    let e1 = mgr.tell_status_native(&gid, None).unwrap()["elapsedMs"]
        .as_u64()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    let e2 = mgr.tell_status_native(&gid, None).unwrap()["elapsedMs"]
        .as_u64()
        .unwrap();
    assert!(
        e2.saturating_sub(e1) < 200,
        "暂停期间 elapsed 不应增长: e1={e1} e2={e2}"
    );

    // 恢复并完成：elapsed 继续累计超过暂停前的值
    mgr.unpause(&gid).expect("unpause 应成功");
    wait_status(&mgr, &gid, "complete", 30_000)
        .await
        .expect("续传超时");
    let final_e = mgr.tell_status_native(&gid, None).unwrap()["elapsedMs"]
        .as_u64()
        .unwrap();
    assert!(
        final_e > e2,
        "恢复后 elapsed 应继续累计: final={final_e} frozen={e2}"
    );
}
