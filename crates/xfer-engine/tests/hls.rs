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
}

impl HlsServer {
    fn url(&self, p: &str) -> String {
        format!("http://{}{}", self.addr, p)
    }
    fn hits(&self, p: &str) -> usize {
        self.hits.get(p).map(|c| c.load(Ordering::SeqCst)).unwrap_or(0)
    }
}

/// 静态文件服务：支持 Range（分片大小预探测依赖），可对指定路径
/// 注入 `Content-Type`，并对 `/show/seg` 下的响应加延迟（测暂停续传）。
async fn start_hls_server(
    files: HashMap<String, Vec<u8>>,
    content_types: HashMap<String, String>,
    seg_delay_ms: u64,
) -> HlsServer {
    let mut hits: HashMap<String, Arc<AtomicUsize>> = HashMap::new();
    let mut app = Router::new();
    for (path, body) in files {
        let data = Arc::new(body);
        let hit = Arc::new(AtomicUsize::new(0));
        let ct = content_types.get(&path).cloned();
        let delayed = path.starts_with("/show/seg");
        hits.insert(path.clone(), hit.clone());
        app = app.route(
            &path,
            get(move |headers: HeaderMap| {
                let data = data.clone();
                let hit = hit.clone();
                let ct = ct.clone();
                async move {
                    hit.fetch_add(1, Ordering::SeqCst);
                    if delayed && seg_delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(seg_delay_ms)).await;
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
                    let mut resp = axum::response::Response::new(axum::body::Body::from(body));
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
    HlsServer { addr, hits }
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
