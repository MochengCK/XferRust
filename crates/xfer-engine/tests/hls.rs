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


/// 段文件目录（乱序落盘：`<产物>.hlseg/`）。
fn seg_dir_of(out: &std::path::Path) -> std::path::PathBuf {
    let name = out
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "out".to_string());
    out.with_file_name(format!("{name}.hlseg"))
}

/// 第 `i` 段的段文件路径。
fn seg_file_of(out: &std::path::Path, i: usize) -> std::path::PathBuf {
    seg_dir_of(out).join(format!("{i:06}"))
}

/// **磁盘上的真实字节**：产物 + 段目录里还没拼进去的那些段文件。
///
/// 乱序落盘下这才是"已经下到磁盘上的量" —— 进度必须等于它（而不是只等于产物
/// 长度，产物只有连续前缀才长）。
fn disk_bytes(out: &std::path::Path) -> u64 {
    let mut total = std::fs::metadata(out).map(|m| m.len()).unwrap_or(0);
    if let Ok(rd) = std::fs::read_dir(seg_dir_of(out)) {
        for e in rd.flatten() {
            total += e.metadata().map(|m| m.len()).unwrap_or(0);
        }
    }
    total
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

/// 进度必须**一路往前走**，同时不虚报超过一个分片。
///
/// 现场（真实站点）：单连接被限速到几十 KB/s，一个 1.5MB 的分片要 20 秒才
/// 下完 —— 只按「分片整段落盘」跳的口径下，界面 20 秒不动一格，用户看到的
/// 就是「进度不是实时的」（磁盘与网络其实一直在走）。现在把**光标所在那一个
/// 分片**的在飞字节算进进度（它取消时会被段内续传落盘，所以不会白算）。
/// 这条用例钉三件事：
///   1. 队头分片下载期间进度**连续变化**（不是长时间静止）；
///   2. 进度**绝不倒退**（暂停也不倒退）；
///   3. 最多只领先磁盘「一个分片 + 写回缓冲」—— 后面那些在飞分片取消即丢，
///      不能算进进度。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hls_progress_advances_while_head_segment_is_slow() {
    use std::collections::BTreeSet;
    use std::time::Instant;

    const SINK_BUF: i64 = 512 * 1024;

    let dir = tmpdir("progress-advance");
    let seg_bytes = 512 * 1024usize;
    let count = 6usize;
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
    // 队头片慢发：16KB / 120ms → 512KB 要约 4 秒（模拟「单连接被限速」）
    let mut trickle = Trickle::new();
    trickle.insert("/show/seg1.ts".to_string(), (16 * 1024usize, 120u64));
    let srv = start_hls_server_trickle(files, HashMap::new(), 0, HashMap::new(), trickle).await;
    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![srv.url("/show/index.m3u8")],
            &serde_json::json!({"dir": dir, "split": "3", "hls-probe-size": "false"}),
            None,
        )
        .expect("addUri 应成功");

    let started = Instant::now();
    let mut seen: BTreeSet<u64> = BTreeSet::new();
    let mut last: u64 = 0;
    let mut worst_ahead: i64 = 0;
    let mut peak: u64 = 0;
    let mut paused: Option<(u64, u64)> = None;
    loop {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let st = tell(&mgr, &gid).await;
        // 乱序落盘下"磁盘"= 产物 + 还没拼进去的段文件
        let disk = disk_bytes(&file_of(&st));
        let done = st["completedLength"].as_u64().unwrap_or(0);
        assert!(done >= last, "进度倒退了：{last} → {done}");
        last = done;
        if done > 0 {
            seen.insert(done);
        }
        peak = peak.max(done);
        worst_ahead = worst_ahead.max(done as i64 - disk as i64);

        // 队头还在下的时候暂停：验证「暂停不倒退」
        if started.elapsed() >= Duration::from_millis(1_600) {
            mgr.pause(&gid).expect("pause 应成功");
            let st = wait_status(&mgr, &gid, "paused", 15_000)
                .await
                .expect("暂停未生效");
            paused = Some((
                st["completedLength"].as_u64().unwrap_or(0),
                disk_bytes(&file_of(&st)),
            ));
            break;
        }
        match st["status"].as_str().unwrap_or_default() {
            "complete" | "error" => break,
            _ => {}
        }
    }

    assert!(
        seen.len() >= 5,
        "队头分片下载期间进度只出现过 {} 个不同值（{seen:?}）—— \
         说明进度在等整段落盘、没跟着在飞字节走",
        seen.len()
    );
    let (paused_done, paused_disk) = paused.expect("用例没走到暂停点");
    assert!(
        paused_done >= peak,
        "暂停后进度倒退了：暂停前峰值 {peak} → 暂停后 {paused_done}"
    );
    assert!(
        paused_done <= paused_disk,
        "暂停后进度（{paused_done}）比磁盘（{paused_disk}）还多——\
         段内续传没把光标那段的半截落盘"
    );
    assert!(
        worst_ahead <= SINK_BUF,
        "进度比磁盘超前 {worst_ahead} 字节（上限 {SINK_BUF} = 产物的写回缓冲）——\
         乱序落盘下进度就该等于磁盘上的字节，多出来说明算了还没落盘的数据"
    );
    assert!(
        paused_done + SINK_BUF as u64 >= paused_disk,
        "暂停后进度（{paused_done}）比磁盘（{paused_disk}）少了一大截 —— \
         乱序落盘必须把段文件里的字节也算进已下载（用户报的「速度几 MB、\
         进度只涨几 KB」就是这个）"
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
    // 乱序落盘：第 2 段下到一半就暂停，那半截留在**它自己的段文件**里
    let part = std::fs::metadata(seg_file_of(&file, 1))
        .map(|m| m.len())
        .unwrap_or(0);
    assert!(
        part > 0 && part < seg_len as u64,
        "暂停时段文件里应留住第 2 段的半截（实际 {part} 字节）"
    );
    assert_eq!(
        st["completedLength"].as_u64().unwrap_or(0),
        disk_bytes(&file),
        "暂停后的进度必须等于磁盘上的真实字节（产物 + 段文件）"
    );

    mgr.unpause(&gid).expect("unpause 应成功");
    let st = wait_status(&mgr, &gid, "complete", 40_000)
        .await
        .expect("恢复后 40s 内未完成");
    let expected: Vec<u8> = segs.iter().flatten().copied().collect();
    let got = std::fs::read(file_of(&st)).unwrap();
    assert!(
        got == expected,
        "段内续传产物必须严丝合缝（多写一遍开头就会错位）：产物 {} 字节 / 期望 {} 字节，\
         首个不一致位置 {:?}",
        got.len(),
        expected.len(),
        got.iter().zip(expected.iter()).position(|(a, b)| a != b)
    );
    let rs = srv.ranges("/show/seg2.ts");
    assert_eq!(rs.len(), 2, "seg2 只应被请求两次：首次 + 续传，不该整段重下");
    assert_eq!(
        rs[1].as_deref(),
        Some(format!("bytes={part}-").as_str()),
        "续传请求必须从段内续传位置接着下"
    );
}

/// HLS 任务也要有**分片位图**（任务列表里的那排分片格子）。
///
/// 大清单不做分片预探测（`plan.total = None`），此前直接把 `http_pieces` 置空，
/// 用户看到的就是「M3U8 任务没有分片显示」。现在总长先取主清单的
/// `BANDWIDTH × 时长`（下载第一秒位图就建好），再按**已落盘**字节增量点亮；
/// 单层清单那种连码率都没有的情况，等实测估算一出现也补建。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hls_exposes_piece_bitfield_for_unknown_total() {
    let dir = tmpdir("pieces");
    // 40 段 > SIZE_PROBE_SAMPLE(32) → 不预探测，`plan.total` 为 None
    let count = 40usize;
    let seg_bytes = 64 * 1024usize;
    let names: Vec<String> = (1..=count).map(|i| format!("s{i}.ts")).collect();
    let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    let mut files = HashMap::new();
    files.insert(
        "/m/master.m3u8".to_string(),
        "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=800000\nv1.m3u8\n"
            .as_bytes()
            .to_vec(),
    );
    files.insert("/m/v1.m3u8".to_string(), media_playlist(&refs, true).into_bytes());
    for (i, n) in names.iter().enumerate() {
        files.insert(format!("/m/{n}"), sample(i as u8 + 1, seg_bytes));
    }
    // 首片慢发：好观察「一个字节都还没落盘时位图就已经在了」
    let mut trickle = Trickle::new();
    trickle.insert("/m/s1.ts".to_string(), (8 * 1024usize, 120u64));
    let srv = start_hls_server_trickle(files, HashMap::new(), 0, HashMap::new(), trickle).await;

    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![srv.url("/m/master.m3u8")],
            // 片长调小（默认 4MB）才看得出「一格一格点亮」
            &serde_json::json!({"dir": dir, "split": "4", "min-split-size": "65536"}),
            None,
        )
        .expect("addUri 应成功");

    let mut before_progress = false;
    let mut lit = false;
    let mut max_pieces = 0usize;
    let st = loop {
        tokio::time::sleep(Duration::from_millis(30)).await;
        let st = tell(&mgr, &gid).await;
        let bf = st["bitfield"].as_str().unwrap_or("").to_string();
        let done = st["completedLength"].as_u64().unwrap_or(0);
        let n = st["numPieces"].as_u64().unwrap_or(0) as usize;
        if !bf.is_empty() && n > 0 {
            max_pieces = max_pieces.max(n);
            if done == 0 {
                before_progress = true;
            }
            if bf.chars().any(|c| c != '0') {
                lit = true;
            }
        }
        match st["status"].as_str().unwrap_or_default() {
            "complete" | "error" => break st,
            _ => {}
        }
    };
    assert_eq!(st["status"], "complete", "任务应下载完成：{st}");
    assert!(
        before_progress,
        "一个字节都还没落盘时就要有位图（HLS 的总长靠主清单码率推算），\
         否则界面就是「没有分片显示」"
    );
    assert!(lit, "下载过程中位图应随落盘字节一格一格点亮");
    assert!(max_pieces > 1, "64KB 片长 / 2.5MB 产物应有多个分片格");
    let bf = st["bitfield"].as_str().unwrap_or("");
    assert!(
        bf.starts_with("ff"),
        "完成后位图应全亮（并定格为真实总长），实际 {bf:?}"
    );
}

/// **暂停 → 恢复，进度不得倒退**（用户报："暂停再开始进度还是会倒退"）。
///
/// 两个数必须对齐：
/// - 引擎在启动下载前用 `playlist_resume_point()` 回填进度基线（恢复后第一帧
///   就显示它）= **产物长度 + 段目录里全部段文件的字节**；
/// - 下载器内部再用 `PlaylistStats::progress()` 接管 = **产物已拼字节 + `spilled`**。
///
/// 一旦下载器初始化 `spilled` 时漏掉"**已下完但还没轮到拼**"的整段（它们既不在
/// 产物里、又不算进 `spilled`），第一帧就会从基线掉下来 —— 那就是"倒退"。
/// 这条用例在高并发下暂停（此时必然有一批段已完成却还没拼），逐帧钉住进度
/// **单调不减**，并要求恢复的第一帧不低于暂停时的水位。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hls_resume_does_not_rewind_progress() {
    let dir = tmpdir("no-rewind");
    let seg_len = 96 * 1024usize;
    let count = 12usize;
    let names: Vec<String> = (1..=count).map(|i| format!("seg{i}.ts")).collect();
    let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    let mut files = HashMap::new();
    files.insert("/show/index.m3u8".to_string(), media_playlist(&refs, true).into_bytes());
    for (i, n) in names.iter().enumerate() {
        files.insert(format!("/show/{n}"), sample(i as u8 + 1, seg_len));
    }
    // 首片慢发把队头顶住：后面若干片会"先下完、等在段目录里"（正是漏算的那批）
    let mut trickle = Trickle::new();
    trickle.insert("/show/seg1.ts".to_string(), (8 * 1024usize, 90u64));
    let srv = start_hls_server_trickle(files, HashMap::new(), 0, HashMap::new(), trickle).await;

    let mgr = TaskManager::start(dir.clone(), 2);
    // 高并发 + 关预探测：段大小未知、多个段同时在飞
    let gid = mgr
        .add_uri(
            vec![srv.url("/show/index.m3u8")],
            &serde_json::json!({"dir": dir, "split": "8", "hls-probe-size": "false"}),
            None,
        )
        .expect("addUri 应成功");

    // 等队头之外的段攒起来（磁盘字节明显超过产物长度）
    let mut waited = 0u64;
    loop {
        tokio::time::sleep(Duration::from_millis(30)).await;
        waited += 30;
        let st = tell(&mgr, &gid).await;
        let out_len = std::fs::metadata(file_of(&st)).map(|m| m.len()).unwrap_or(0);
        if disk_bytes(&file_of(&st)) > out_len + seg_len as u64 || waited >= 8_000 {
            break;
        }
    }
    mgr.pause(&gid).expect("pause 应成功");
    let st = wait_status(&mgr, &gid, "paused", 15_000)
        .await
        .expect("暂停未生效");
    let paused_done = st["completedLength"].as_u64().unwrap_or(0);
    let paused_disk = disk_bytes(&file_of(&st));
    assert!(
        paused_done >= paused_disk.saturating_sub(512 * 1024),
        "暂停后进度（{paused_done}）不该比磁盘（{paused_disk}）少一大截"
    );

    mgr.unpause(&gid).expect("unpause 应成功");
    // 恢复后逐帧盯进度：允许不涨，但**绝不许低于暂停时的水位**
    let mut last = paused_done;
    let mut first: Option<u64> = None;
    let mut floor_violation: Option<(u64, u64)> = None;
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_millis(2_000) {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let st = tell(&mgr, &gid).await;
        let done = st["completedLength"].as_u64().unwrap_or(0);
        if first.is_none() {
            first = Some(done);
        }
        if done < last {
            floor_violation = Some((last, done));
            break;
        }
        last = done;
        if st["status"].as_str() == Some("complete") {
            break;
        }
    }
    assert!(
        floor_violation.is_none(),
        "恢复后进度倒退了：{:?}（暂停时水位 {paused_done}）——\
         引擎的续传基线是「产物 + 全部段文件」，下载器初始化 `spilled` 时\
         必须用同一个口径，否则第一帧就掉下来",
        floor_violation
    );
    let st = wait_status(&mgr, &gid, "complete", 40_000)
        .await
        .expect("恢复后 40s 内未完成");
    let expected: Vec<u8> = (1..=count)
        .flat_map(|i| sample(i as u8, seg_len))
        .collect();
    assert_eq!(
        std::fs::read(file_of(&st)).unwrap(),
        expected,
        "续传后产物必须完整且顺序正确"
    );
}
