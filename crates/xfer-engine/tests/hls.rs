//! xfer-engine HLS（M3U8）集成：`.m3u8` 任务的全链路。
//!
//! 验证：主清单选流、分片顺序拼接、产物命名与总长/分片位图回填、
//! 暂停续传（控制文件连续前缀）、内容嗅探回退普通 HTTP、以及
//! "扩展名像清单、内容是错误页"时的诚实报错。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::routing::get;
use axum::Router;
use tokio::net::TcpListener;
use xfer_engine::TaskManager;
use xfer_http::ctrl_path;
use xfer_types::Gid;

/// 位置敏感数据：任何错位/重复写都会被校验出来。
fn sample(tag: u8, len: usize) -> Vec<u8> {
    (0..len).map(|i| (i as u8).wrapping_add(tag)).collect()
}

fn tmpdir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("xfer-engine-hls-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let ctrl = std::env::temp_dir().join(format!("xfer-engine-hls-ctrl-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&ctrl);
    std::env::set_var("XFER_CTRL_DIR", &ctrl);
    d
}

/// 媒体清单文本（`EXTINF` + 相对分片地址）。
fn media_playlist(paths: &[&str], endlist: bool) -> String {
    let mut s = String::from("#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-MEDIA-SEQUENCE:0\n");
    for p in paths {
        s.push_str(&format!("#EXTINF:4.0,\n{p}\n"));
    }
    if endlist {
        s.push_str("#EXT-X-ENDLIST\n");
    }
    s
}

struct HlsServer {
    addr: SocketAddr,
    hits: HashMap<String, Arc<AtomicUsize>>,
    /// 每个请求记一条：(路径, `Range` 头)
    requests: Arc<std::sync::Mutex<Vec<(String, Option<String>)>>>,
}

impl HlsServer {
    fn url(&self, p: &str) -> String {
        format!("http://{}{}", self.addr, p)
    }
    fn hits(&self, p: &str) -> usize {
        self.hits.get(p).map(|c| c.load(Ordering::SeqCst)).unwrap_or(0)
    }
    /// 某个路径收到过的全部 `Range` 头（按时间顺序）。
    fn ranges(&self, p: &str) -> Vec<Option<String>> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _)| path == p)
            .map(|(_, r)| r.clone())
            .collect()
    }
}

/// 静态文件服务：支持 Range（分片大小预探测依赖），可对指定路径
/// 注入 `Content-Type`，并对 `/show/seg` 下的响应加延迟（测暂停续传）。
async fn start_hls_server(
    files: HashMap<String, Vec<u8>>,
    content_types: HashMap<String, String>,
    seg_delay_ms: u64,
) -> HlsServer {
    start_hls_server_slow(files, content_types, seg_delay_ms, HashMap::new()).await
}

/// 分块慢发 `(块字节, 块间隔毫秒)`：制造"段内只下到一半就被暂停"的现场。
type Trickle = HashMap<String, (usize, u64)>;

/// 同上，另可对**指定路径**追加延迟（毫秒）：模拟 CDN 抖动 —— 队头慢、
/// 后面的分片先下完等在内存里，是"HLS 进度虚高"的现场。
async fn start_hls_server_slow(
    files: HashMap<String, Vec<u8>>,
    content_types: HashMap<String, String>,
    seg_delay_ms: u64,
    slow: HashMap<String, u64>,
) -> HlsServer {
    start_hls_server_trickle(files, content_types, seg_delay_ms, slow, HashMap::new()).await
}

async fn start_hls_server_trickle(
    files: HashMap<String, Vec<u8>>,
    content_types: HashMap<String, String>,
    seg_delay_ms: u64,
    slow: HashMap<String, u64>,
    trickle: Trickle,
) -> HlsServer {
    let mut hits: HashMap<String, Arc<AtomicUsize>> = HashMap::new();
    let requests: Arc<std::sync::Mutex<Vec<(String, Option<String>)>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut app = Router::new();
    for (path, body) in files {
        let data = Arc::new(body);
        let hit = Arc::new(AtomicUsize::new(0));
        let ct = content_types.get(&path).cloned();
        let delayed = path.starts_with("/show/seg");
        let extra = slow.get(&path).copied().unwrap_or(0);
        let trickle_cfg = trickle.get(&path).copied();
        let requests = requests.clone();
        let route = path.clone();
        hits.insert(path.clone(), hit.clone());
        app = app.route(
            &path,
            get(move |headers: HeaderMap| {
                let data = data.clone();
                let hit = hit.clone();
                let ct = ct.clone();
                let requests = requests.clone();
                let route = route.clone();
                async move {
                    hit.fetch_add(1, Ordering::SeqCst);
                    let range_hdr = headers
                        .get(header::RANGE)
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string());
                    requests.lock().unwrap().push((route.clone(), range_hdr.clone()));
                    let wait = if delayed { seg_delay_ms } else { 0 } + extra;
                    if wait > 0 {
                        tokio::time::sleep(Duration::from_millis(wait)).await;
                    }
                    let total = data.len();
                    let range = headers
                        .get(header::RANGE)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or_default()
                        .to_string();
                    let (from, to) = match range.strip_prefix("bytes=").and_then(|r| r.split_once('-'))
                    {
                        Some((f, t)) => (
                            f.trim().parse::<usize>().unwrap_or(0),
                            t.trim().parse::<usize>().unwrap_or(total),
                        ),
                        None => (0, total),
                    };
                    let from = from.min(total);
                    let to = (to + 1).min(total).max(from);
                    let body = data[from..to].to_vec();
                    let mut resp = match trickle_cfg {
                        // 分块慢发：制造"段内只收到一半"的现场
                        Some((chunk, gap)) => {
                            let stream = futures_util::stream::unfold(
                                (body, 0usize),
                                move |(b, off)| async move {
                                    if off >= b.len() {
                                        return None;
                                    }
                                    let end = (off + chunk).min(b.len());
                                    let piece = b[off..end].to_vec();
                                    tokio::time::sleep(Duration::from_millis(gap)).await;
                                    Some((Ok::<_, std::io::Error>(piece), (b, end)))
                                },
                            );
                            axum::response::Response::new(axum::body::Body::from_stream(stream))
                        }
                        None => axum::response::Response::new(axum::body::Body::from(body)),
                    };
                    if let Some(ct) = &ct {
                        resp.headers_mut().insert(
                            header::CONTENT_TYPE,
                            HeaderValue::from_str(ct).unwrap(),
                        );
                    }
                    if from > 0 || to < total {
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
    }
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(l, app).await;
    });
    HlsServer {
        addr,
        hits,
        requests,
    }
}

async fn tell(mgr: &TaskManager, gid: &Gid) -> serde_json::Value {
    mgr.tell_status_native(gid, None).unwrap()
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
        let st = tell(mgr, gid).await;
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

async fn wait_progress(mgr: &TaskManager, gid: &Gid, min_bytes: u64, limit_ms: u64) -> bool {
    let mut waited = 0u64;
    loop {
        tokio::time::sleep(Duration::from_millis(50)).await;
        waited += 50;
        if tell(mgr, gid).await["completedLength"].as_u64().unwrap_or(0) >= min_bytes {
            return true;
        }
        if waited >= limit_ms {
            return false;
        }
    }
}

fn file_of(st: &serde_json::Value) -> PathBuf {
    PathBuf::from(st["files"][0]["path"].as_str().unwrap_or_default())
}

/// 主清单选流 + 顺序拼接 + 命名/总长/分片位图回填 + 控制文件清理。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hls_task_downloads_best_variant() {
    let dir = tmpdir("best");
    let lo: Vec<Vec<u8>> = (1..=2).map(|i| sample(100 + i, 16 * 1024)).collect();
    let hi: Vec<Vec<u8>> = (1..=3).map(|i| sample(i, 20 * 1024)).collect();
    let expected: Vec<u8> = hi.iter().flatten().copied().collect();

    let mut files = HashMap::new();
    files.insert(
        "/show/master.m3u8".to_string(),
        b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=400000,RESOLUTION=640x360\nlo/index.m3u8\n\
          #EXT-X-STREAM-INF:BANDWIDTH=2400000,RESOLUTION=1920x1080\nhi/index.m3u8\n"
            .to_vec(),
    );
    files.insert(
        "/show/lo/index.m3u8".to_string(),
        media_playlist(&["seg1.ts", "seg2.ts"], true).into_bytes(),
    );
    for (i, s) in lo.iter().enumerate() {
        files.insert(format!("/show/lo/seg{}.ts", i + 1), s.clone());
    }
    files.insert(
        "/show/hi/index.m3u8".to_string(),
        media_playlist(&["seg1.ts", "seg2.ts", "seg3.ts"], true).into_bytes(),
    );
    for (i, s) in hi.iter().enumerate() {
        files.insert(format!("/show/hi/seg{}.ts", i + 1), s.clone());
    }
    let srv = start_hls_server(files, HashMap::new(), 0).await;

    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![srv.url("/show/master.m3u8")],
            &serde_json::json!({"dir": dir, "split": "3"}),
            None,
        )
        .expect("addUri 应成功");

    let st = wait_status(&mgr, &gid, "complete", 30_000)
        .await
        .expect("30s 内未完成");
    // 命名：清单末段是通用名 index → 用父目录名（show）
    assert_eq!(
        file_of(&st),
        dir.join("show.ts"),
        "产物命名应由清单地址推导"
    );
    assert_eq!(
        st["totalLength"].as_u64().unwrap(),
        expected.len() as u64,
        "分片预探测应给出精确总长"
    );
    assert_eq!(st["completedLength"].as_u64().unwrap(), expected.len() as u64);
    assert!(st["numPieces"].as_u64().unwrap() > 0, "分片位图应可用");
    assert!(st["bitfield"].as_str().unwrap().len() > 0);
    assert_eq!(std::fs::read(file_of(&st)).unwrap(), expected, "必须是高码率变体且顺序正确");
    assert_eq!(srv.hits("/show/lo/seg1.ts"), 0, "不应下载低码率变体");
    assert!(
        !ctrl_path(&file_of(&st)).exists(),
        "完成后控制文件应删除"
    );
}

/// 暂停 → 恢复：控制文件里的连续前缀必须被复用，产物完整。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hls_pause_resume_keeps_prefix() {
    let dir = tmpdir("resume");
    let segs: Vec<Vec<u8>> = (1..=6).map(|i| sample(i, 24 * 1024)).collect();
    let expected: Vec<u8> = segs.iter().flatten().copied().collect();
    let mut files = HashMap::new();
    files.insert(
        "/show/index.m3u8".to_string(),
        media_playlist(
            &["seg1.ts", "seg2.ts", "seg3.ts", "seg4.ts", "seg5.ts", "seg6.ts"],
            true,
        )
        .into_bytes(),
    );
    for (i, s) in segs.iter().enumerate() {
        files.insert(format!("/show/seg{}.ts", i + 1), s.clone());
    }
    let srv = start_hls_server(files, HashMap::new(), 120).await;

    let mgr = TaskManager::start(dir.clone(), 2);
    // 并发 1：分片严格按序在飞，暂停点之后的字节一定来自连续前缀。
    // 关闭大小预探测：探测请求会让"每个分片被请求几次"无法反映续传行为
    let gid = mgr
        .add_uri(
            vec![srv.url("/show/index.m3u8")],
            &serde_json::json!({"dir": dir, "split": "1", "hls-probe-size": "false"}),
            None,
        )
        .expect("addUri 应成功");

    assert!(
        wait_progress(&mgr, &gid, segs[0].len() as u64, 20_000).await,
        "20s 内未看到首段落盘"
    );
    mgr.pause(&gid).expect("pause 应成功");
    let st = wait_status(&mgr, &gid, "paused", 15_000)
        .await
        .expect("暂停未生效");
    let partial = std::fs::metadata(file_of(&st)).map(|m| m.len()).unwrap_or(0);
    assert!(partial > 0, "暂停时应已落盘部分数据");
    assert_eq!(
        st["completedLength"].as_u64().unwrap_or(0),
        partial,
        "暂停后的进度必须等于磁盘上的真实字节（含段内续传留住的半截）"
    );

    mgr.unpause(&gid).expect("unpause 应成功");
    let st = wait_status(&mgr, &gid, "complete", 40_000)
        .await
        .expect("恢复后 40s 内未完成");
    assert_eq!(std::fs::read(file_of(&st)).unwrap(), expected, "续传后产物必须完整且无重复段");
    assert_eq!(
        srv.hits("/show/seg1.ts"),
        1,
        "已持久化的首段不应在恢复后重下"
    );
    assert!(!ctrl_path(&file_of(&st)).exists());
}

/// 进度 = **已交给文件的字节**，且**绝不倒退**（暂停也不倒退）。
///
/// 现场：分片按清单顺序整段拼接，队头慢时后面的分片会先下完等在内存里。
/// 旧实现把这些字节按"已接收"上报，界面能显示 40MB、一暂停回落成磁盘上的
/// 1.4MB（那些字节还在内存，取消即丢弃）。这条用例钉两件事：
///   1. 进度不得超前文件（含 `FileSink` 那 512KB 写回缓冲）；
///   2. 暂停后读到的进度不得小于暂停前看到的峰值。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hls_progress_is_committed_bytes_and_never_regresses() {
    use std::time::Instant;

    let dir = tmpdir("progress");
    // 分片要足够大：`FileSink` 自带 512KB 写回缓冲（进度含它，取消时会被
    // flush 落盘，不算虚报），分片太小就全被缓冲吃掉、磁盘恒为 0
    let seg_bytes = 512 * 1024usize;
    let count = 6usize;
    const SINK_BUF: i64 = 512 * 1024;
    let names: Vec<String> = (1..=count).map(|i| format!("seg{i}.ts")).collect();
    let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    let mut files = HashMap::new();
    files.insert(
        "/show/index.m3u8".to_string(),
        media_playlist(&refs, true).into_bytes(),
    );
    for (i, n) in names.iter().enumerate() {
        files.insert(format!("/show/{n}"), sample(i as u8 + 1, seg_bytes));
    }
    // 队头（首片）慢 2.5s，其余分片正常：这段时间里后面的分片会先下完等内存
    let mut slow = HashMap::new();
    slow.insert("/show/seg1.ts".to_string(), 2_500u64);
    let srv = start_hls_server_slow(files, HashMap::new(), 0, slow).await;
    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![srv.url("/show/index.m3u8")],
            &serde_json::json!({"dir": dir, "split": "3", "hls-probe-size": "false"}),
            None,
        )
        .expect("addUri 应成功");

    let started = Instant::now();
    let mut worst: i64 = 0;
    let mut peak: i64 = 0;
    let mut paused: Option<(i64, i64)> = None;
    loop {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let st = tell(&mgr, &gid).await;
        let disk = std::fs::metadata(file_of(&st)).map(|m| m.len()).unwrap_or(0) as i64;
        let done = st["completedLength"].as_u64().unwrap_or(0) as i64;
        worst = worst.max(done - disk);
        peak = peak.max(done);

        // 队头还在下的时候就暂停：验证"暂停不倒退"
        if paused.is_none() && started.elapsed() >= Duration::from_millis(700) {
            mgr.pause(&gid).expect("pause 应成功");
            let st = wait_status(&mgr, &gid, "paused", 15_000)
                .await
                .expect("暂停未生效");
            paused = Some((
                st["completedLength"].as_u64().unwrap_or(0) as i64,
                std::fs::metadata(file_of(&st)).map(|m| m.len()).unwrap_or(0) as i64,
            ));
            break;
        }
        match st["status"].as_str().unwrap_or_default() {
            "complete" | "error" => break,
            _ => {}
        }
    }

    let (paused_done, paused_disk) = paused.expect("用例没走到暂停点（下载比预期快？）");
    assert!(
        paused_done >= peak,
        "暂停后进度倒退了：暂停前峰值 {peak} → 暂停后 {paused_done}"
    );
    assert!(
        paused_done <= paused_disk,
        "暂停后的进度（{paused_done}）比磁盘上的字节（{paused_disk}）还多——虚报"
    );
    assert!(
        worst <= SINK_BUF,
        "进度比磁盘超前 {worst} 字节（上限 {SINK_BUF} = 写回缓冲）——\
         在飞/待写的分片被当成已下载了（那些字节暂停即丢弃）"
    );
}

/// 内容嗅探：地址不像清单、但响应声明 mpegurl —— 内容是清单就按 HLS
/// 下载，不是清单则回退普通 HTTP（整段存下来），两者都不该失败。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hls_sniffs_content_type_then_falls_back_to_http() {
    let dir = tmpdir("sniff");
    let segs: Vec<Vec<u8>> = (1..=2).map(|i| sample(i, 12 * 1024)).collect();
    let expected: Vec<u8> = segs.iter().flatten().copied().collect();
    let html = b"<html><body>not a playlist</body></html>".to_vec();

    let mut files = HashMap::new();
    // 声明 mpegurl 且内容真的是清单 → 走 HLS（地址本身不像清单）
    files.insert(
        "/api/stream/one".to_string(),
        media_playlist(&["seg1.ts", "seg2.ts"], true).into_bytes(),
    );    // 声明 mpegurl 但内容是错误页 → 嗅探失败后回退普通 HTTP
    files.insert("/api/stream/two".to_string(), html.clone());
    for (i, s) in segs.iter().enumerate() {
        // 分片地址相对清单解析：清单在 /api/stream/one，故分片在 /api/stream/
        files.insert(format!("/api/stream/seg{}.ts", i + 1), s.clone());
    }
    let mut cts = HashMap::new();
    cts.insert(
        "/api/stream/one".to_string(),
        "application/vnd.apple.mpegurl".to_string(),
    );
    cts.insert(
        "/api/stream/two".to_string(),
        "application/vnd.apple.mpegurl".to_string(),
    );
    let srv = start_hls_server(files, cts, 0).await;

    let mgr = TaskManager::start(dir.clone(), 2);
    let gid_hls = mgr
        .add_uri(
            vec![srv.url("/api/stream/one?id=1")],
            &serde_json::json!({"dir": dir, "out": "sniffed.ts"}),
            None,
        )
        .expect("addUri 应成功");
    let _st = wait_status(&mgr, &gid_hls, "complete", 30_000)
        .await
        .expect("嗅探命中的任务应完成");
    assert_eq!(std::fs::read(dir.join("sniffed.ts")).unwrap(), expected);

    let gid_html = mgr
        .add_uri(
            vec![srv.url("/api/stream/two?id=2")],
            &serde_json::json!({"dir": dir, "out": "not-playlist.html"}),
            None,
        )
        .expect("addUri 应成功");
    let st = wait_status(&mgr, &gid_html, "complete", 30_000)
        .await
        .expect("内容不是清单时应回退普通 HTTP 并完成");
    assert_eq!(std::fs::read(file_of(&st)).unwrap(), html);
}

/// 扩展名声明是清单（`.m3u8`）、内容却不是：必须**报错**而不是把错误页
/// 存成视频（静默产出错误产物比失败更糟）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hls_task_errors_on_non_playlist_body() {
    let dir = tmpdir("broken");
    let mut files = HashMap::new();
    files.insert(
        "/broken.m3u8".to_string(),
        b"<html><body>403 Forbidden</body></html>".to_vec(),
    );
    let srv = start_hls_server(files, HashMap::new(), 0).await;

    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![srv.url("/broken.m3u8")],
            &serde_json::json!({"dir": dir}),
            None,
        )
        .expect("addUri 应成功");
    let st = wait_status(&mgr, &gid, "error", 30_000)
        .await
        .expect("应进入 error");
    assert_eq!(st["errorCode"].as_u64().unwrap(), 5);
    assert!(
        st["errorMessage"].as_str().unwrap().contains("M3U8"),
        "错误信息应点明不是播放列表: {}",
        st["errorMessage"]
    );
    assert!(dir.read_dir().unwrap().next().is_none(), "不应留下半成品文件");
}

/// 直播清单（无 `#EXT-X-ENDLIST`）：下载当前窗口即完成，产物为窗口快照。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hls_live_playlist_records_current_window() {
    let dir = tmpdir("live");
    let segs: Vec<Vec<u8>> = (1..=2).map(|i| sample(i, 8 * 1024)).collect();
    let expected: Vec<u8> = segs.iter().flatten().copied().collect();
    let mut files = HashMap::new();
    files.insert(
        "/live/index.m3u8".to_string(),
        media_playlist(&["seg1.ts", "seg2.ts"], false).into_bytes(),
    );
    for (i, s) in segs.iter().enumerate() {
        files.insert(format!("/live/seg{}.ts", i + 1), s.clone());
    }
    let srv = start_hls_server(files, HashMap::new(), 0).await;

    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![srv.url("/live/index.m3u8")],
            &serde_json::json!({"dir": dir}),
            None,
        )
        .expect("addUri 应成功");
    let st = wait_status(&mgr, &gid, "complete", 30_000)
        .await
        .expect("直播窗口快照也应完成");
    assert_eq!(std::fs::read(file_of(&st)).unwrap(), expected);
    assert_eq!(file_of(&st).file_name().unwrap(), "live.ts");
}

/// 显式 `hls=false`：扩展名像清单也按普通 HTTP 整段下载。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hls_can_be_disabled_per_task() {
    let dir = tmpdir("disable");
    let body = media_playlist(&["seg1.ts"], true).into_bytes();
    let mut files = HashMap::new();
    files.insert("/x/index.m3u8".to_string(), body.clone());
    let srv = start_hls_server(files, HashMap::new(), 0).await;

    let mgr = TaskManager::start(dir.clone(), 1);
    let gid = mgr
        .add_uri(
            vec![srv.url("/x/index.m3u8")],
            &serde_json::json!({"dir": dir, "hls": "false", "out": "raw.m3u8"}),
            None,
        )
        .expect("addUri 应成功");
    wait_status(&mgr, &gid, "complete", 20_000)
        .await
        .expect("应完成");
    assert_eq!(std::fs::read(dir.join("raw.m3u8")).unwrap(), body);
    assert_eq!(srv.hits("/x/seg1.ts"), 0, "不得请求任何分片");
}

/// **段内续传（引擎级）**：暂停时"下一待写段"已经收到的部分留在文件里，
/// 恢复时对这一段发 `Range` 接着下 —— 而不是整段重下（一次暂停能省几十 MB）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hls_pause_resume_continues_within_segment() {
    let dir = tmpdir("partial");
    let seg_len = 256 * 1024usize;
    let names = ["seg1.ts", "seg2.ts", "seg3.ts", "seg4.ts"];
    let segs: Vec<Vec<u8>> = (1..=4).map(|i| sample(i, seg_len)).collect();
    let mut files = HashMap::new();
    files.insert(
        "/show/index.m3u8".to_string(),
        media_playlist(&names, true).into_bytes(),
    );
    for (i, s) in segs.iter().enumerate() {
        files.insert(format!("/show/seg{}.ts", i + 1), s.clone());
    }
    // seg2 慢发：32KB 一块、每块 60ms → 整段约 480ms，暂停点落在中途
    let mut trickle = HashMap::new();
    trickle.insert("/show/seg2.ts".to_string(), (32 * 1024usize, 60u64));
    let srv = start_hls_server_trickle(files, HashMap::new(), 0, HashMap::new(), trickle).await;

    let mgr = TaskManager::start(dir.clone(), 2);
    // 并发 1：严格按序，暂停时"下一待写段"就是 seg2
    let gid = mgr
        .add_uri(
            vec![srv.url("/show/index.m3u8")],
            &serde_json::json!({"dir": dir, "split": "1", "hls-probe-size": "false"}),
            None,
        )
        .expect("addUri 应成功");

    assert!(
        wait_progress(&mgr, &gid, seg_len as u64, 20_000).await,
        "20s 内未看到首段落盘"
    );
    let mut waited = 0u64;
    while srv.hits("/show/seg2.ts") == 0 && waited < 5_000 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        waited += 20;
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    mgr.pause(&gid).expect("pause 应成功");
    let st = wait_status(&mgr, &gid, "paused", 15_000)
        .await
        .expect("暂停未生效");
    let file = file_of(&st);
    let paused_len = std::fs::metadata(&file).map(|m| m.len()).unwrap_or(0);
    assert!(
        paused_len > seg_len as u64 && paused_len < (seg_len * 2) as u64,
        "暂停时应留住第 2 段的半截（实际 {paused_len} 字节）"
    );
    assert_eq!(
        st["completedLength"].as_u64().unwrap_or(0),
        paused_len,
        "暂停后的进度必须等于磁盘上的真实字节"
    );
    let part = paused_len - seg_len as u64;

    mgr.unpause(&gid).expect("unpause 应成功");
    let st = wait_status(&mgr, &gid, "complete", 40_000)
        .await
        .expect("恢复后 40s 内未完成");
    let expected: Vec<u8> = segs.iter().flatten().copied().collect();
    assert_eq!(
        std::fs::read(file_of(&st)).unwrap(),
        expected,
        "段内续传产物必须严丝合缝（多写一遍开头就会错位）"
    );
    let rs = srv.ranges("/show/seg2.ts");
    assert_eq!(rs.len(), 2, "seg2 只应被请求两次：首次 + 续传，不该整段重下");
    assert_eq!(
        rs[1].as_deref(),
        Some(format!("bytes={part}-").as_str()),
        "续传请求必须从段内续传位置接着下"
    );
}
