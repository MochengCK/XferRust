//! xfer-engine HTTP-FLV 直播录制集成：`.flv` 长连接（无 Content-Length）的全链路。
//!
//! 验证：直播识别（嗅探）与持续录制、`isLive` / `liveRecordedMs` 上报、
//! 产物时间轴归零与断口闭合、断线重连续录、暂停/恢复保留前缀、停播收尾、
//! 重播（服务端从头重发同一内容）收尾、带总长的点播 FLV 走普通下载、
//! `flv=false` 关闭嗅探、`flv=true` 强制录制。

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use tokio::net::TcpListener;
use xfer_engine::TaskManager;
use xfer_types::Gid;

// ---------------------------------------------------------------------------
// FLV 生成
// ---------------------------------------------------------------------------

/// FLV 文件头（9 字节）+ `PreviousTagSize0`（4 字节）。
fn flv_header() -> Vec<u8> {
    vec![b'F', b'L', b'V', 1, 5, 0, 0, 0, 9, 0, 0, 0, 0]
}

/// 一个 FLV tag（11 字节头 + 数据 + 4 字节 PreviousTagSize）。
///
/// 数据字节取 ≥ 0x80 的确定序列：既**永远不会**凑出 `FLV` 三个字符
/// （内容里出现 "FLV" 不会误导任何断言），又对每个偏移有差异。
fn tag(kind: u8, ts: u32, flavor: u8, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(15 + len);
    out.push(kind);
    out.extend_from_slice(&[(len >> 16) as u8, (len >> 8) as u8, len as u8]);
    out.push((ts >> 16) as u8);
    out.push((ts >> 8) as u8);
    out.push(ts as u8);
    out.push((ts >> 24) as u8);
    out.extend_from_slice(&[0, 0, 0]); // streamId
    for i in 0..len {
        out.push(((i as u8).wrapping_mul(7)).wrapping_add(flavor) | 0x80);
    }
    out.extend_from_slice(&((11 + len) as u32).to_be_bytes());
    out
}

/// 一段 FLV 直播内容：文件头 + onMetaData 脚本 tag + `n` 个媒体 tag
/// （每 `step_ms` 一个，音视频交替）。
fn flv_segment(base_ms: u32, n: usize, step_ms: u32, flavor: u8, payload: usize) -> Vec<u8> {
    let mut out = flv_header();
    out.extend_from_slice(&tag(18, 0, flavor, 16));
    for i in 0..n {
        let kind = if i % 2 == 0 { 9 } else { 8 };
        out.extend_from_slice(&tag(kind, base_ms + i as u32 * step_ms, flavor, payload));
    }
    out
}

/// 逐 tag 走一遍产物（结构严格自洽校验），返回 `(类型, 时间戳, 数据长度)`。
fn product_tags(data: &[u8]) -> Vec<(u8, u32, usize)> {
    assert_eq!(&data[..3], b"FLV", "产物必须以 FLV 文件头开头");
    let off = u32::from_be_bytes([data[5], data[6], data[7], data[8]]) as usize;
    let mut pos = off + 4;
    let mut out = Vec::new();
    while pos < data.len() {
        let kind = data[pos];
        let size = ((data[pos + 1] as usize) << 16)
            | ((data[pos + 2] as usize) << 8)
            | (data[pos + 3] as usize);
        let ts = ((data[pos + 4] as u32) << 16)
            | ((data[pos + 5] as u32) << 8)
            | (data[pos + 6] as u32)
            | ((data[pos + 7] as u32) << 24);
        out.push((kind, ts, size));
        pos += 11 + size + 4;
    }
    assert_eq!(pos, data.len(), "产物结构必须严格自洽（长度对得上）");
    out
}

/// 媒体 tag 的时间戳序列（跳过脚本 tag）。
fn media_ts(tags: &[(u8, u32, usize)]) -> Vec<u32> {
    tags.iter()
        .filter(|(k, _, _)| *k == 8 || *k == 9)
        .map(|(_, ts, _)| *ts)
        .collect()
}

// ---------------------------------------------------------------------------
// 脚本化直播服务器
// ---------------------------------------------------------------------------

/// 直播流的媒体 tag 数据长度（脚本化服务器与断言共用同一常量）。
const PAYLOAD: usize = 8192;
/// 媒体 tag 的推送间隔（服务器侧真实时间）。
const STEP_MS: u64 = 20;
/// 两个 tag 之间的时间戳步进（40ms ≈ 25fps 的常见步长）。
const TS_STEP: u32 = 40;

/// 一次请求的行为（每次都从脚本取一条；越界一直用最后一条）。
#[derive(Clone)]
enum Step {
    /// 从 `base` 起发 `n` 个媒体 tag（每 20ms 一个）后 EOF。
    Eof {
        base: u32,
        n: usize,
        flavor: u8,
    },
    /// 发 `n` 个 tag 后**挂住**（不结束、也不再发数据）。
    Hang {
        base: u32,
        n: usize,
        flavor: u8,
    },
    /// 永远发（每 20ms 一个 tag）；第 `k` 次请求的基准时间戳 +10s
    /// （模拟真实直播里"重连后流已经往前走了"）。
    Forever { base: u32, flavor: u8 },
    /// 404（直播下线的典型形态）。
    Gone,
}

impl Step {
    /// 探测请求（引擎的 `Range: bytes=0-0` 探测）用的一小段：快速结束。
    fn probe() -> Self {
        Step::Eof {
            base: 0,
            n: 1,
            flavor: 0,
        }
    }
}

struct FlvServer {
    addr: SocketAddr,
    hits: Arc<AtomicUsize>,
}

impl FlvServer {
    fn url(&self, p: &str) -> String {
        format!("http://{}{}", self.addr, p)
    }
    fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }
}

/// 无限直播流（懒生成，不预分配）：文件头之后每 `step_ms` 一个 tag。
fn forever_body(base: u32, flavor: u8, step_ms: u64, payload: usize) -> Body {
    let stream = futures_util::stream::unfold(0usize, move |i| async move {
        tokio::time::sleep(Duration::from_millis(step_ms)).await;
        let chunk = if i == 0 {
            flv_header()
        } else {
            let n = i - 1;
            let kind = if n % 2 == 0 { 9 } else { 8 };
            tag(kind, base + n as u32 * TS_STEP, flavor, payload)
        };
        Some((Ok::<_, std::io::Error>(chunk), i + 1))
    });
    Body::from_stream(stream)
}

/// 逐块推送的流式响应体（无 Content-Length → chunked）。
fn paced_body(chunks: Vec<Vec<u8>>, step_ms: u64, hang: bool) -> Body {
    let stream = futures_util::stream::unfold(
        (chunks.into_iter(), step_ms, hang),
        |(mut it, step, hang)| async move {
            match it.next() {
                Some(c) => {
                    if step > 0 {
                        tokio::time::sleep(Duration::from_millis(step)).await;
                    }
                    Some((Ok::<_, std::io::Error>(c), (it, step, hang)))
                }
                None => {
                    if hang {
                        futures_util::future::pending::<()>().await;
                    }
                    None
                }
            }
        },
    );
    Body::from_stream(stream)
}

/// 一段 FLV 内容拆成逐 tag 的块（第一块是文件头 + 脚本 tag）。
fn chunks_of(seg: &[u8], n_tags: usize, payload: usize) -> Vec<Vec<u8>> {
    let head_len = 13 + 11 + 16 + 4;
    let tag_len = 11 + payload + 4;
    let mut chunks = vec![seg[..head_len].to_vec()];
    for i in 0..n_tags {
        let s = head_len + i * tag_len;
        chunks.push(seg[s..s + tag_len].to_vec());
    }
    assert_eq!(head_len + n_tags * tag_len, seg.len());
    chunks
}

/// 起一个脚本化直播服务器（默认路由 `/live/stream.flv`）。
async fn start_flv_server(script: Vec<Step>) -> FlvServer {
    start_flv_server_at("/live/stream.flv", script).await
}

/// 同上，路由可指定（`flv=true` 的强制用例要用一个不带 `.flv` 的地址）。
async fn start_flv_server_at(path: &'static str, script: Vec<Step>) -> FlvServer {
    let hits = Arc::new(AtomicUsize::new(0));
    let script = Arc::new(Mutex::new(script));
    let router = {
        let hits = hits.clone();
        let script = script.clone();
        Router::new().route(
            path,
            get(move || {
                let hits = hits.clone();
                let script = script.clone();
                async move {
                    let n = hits.fetch_add(1, Ordering::SeqCst);
                    let step = {
                        let g = script.lock().unwrap();
                        g.get(n).or_else(|| g.last()).cloned()
                    };
                    match step {
                        None | Some(Step::Gone) => {
                            StatusCode::NOT_FOUND.into_response()
                        }
                        Some(Step::Eof { base, n: tags, flavor }) => {
                            let seg = flv_segment(base, tags, TS_STEP, flavor, PAYLOAD);
                            Response::new(paced_body(
                                chunks_of(&seg, tags, PAYLOAD),
                                STEP_MS,
                                false,
                            ))
                        }
                        Some(Step::Hang { base, n: tags, flavor }) => {
                            let seg = flv_segment(base, tags, TS_STEP, flavor, PAYLOAD);
                            Response::new(paced_body(
                                chunks_of(&seg, tags, PAYLOAD),
                                STEP_MS,
                                true,
                            ))
                        }
                        Some(Step::Forever { base, flavor }) => {
                            // 第 n 次请求（含探测）：基准 +n*10s
                            let b = base.wrapping_add(n as u32 * 10_000);
                            Response::new(forever_body(b, flavor, STEP_MS, PAYLOAD))
                        }
                    }
                }
            }),
        )
    };
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(l, router).await;
    });
    FlvServer { addr, hits }
}

/// 静态路由：原样返回一段 FLV（分块 or 带 Content-Length 由 body 决定）。
async fn start_static_server(path: &'static str, doc: Vec<u8>, chunked: bool) -> SocketAddr {
    let doc = Arc::new(doc);
    let router = Router::new().route(
        path,
        get(move || {
            let doc = doc.clone();
            async move {
                if chunked {
                    let chunks: Vec<Vec<u8>> = doc.chunks(4096).map(|c| c.to_vec()).collect();
                    Response::new(paced_body(chunks, 0, false))
                } else {
                    Response::new(Body::from(doc.as_ref().clone()))
                }
            }
        }),
    );
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(l, router).await;
    });
    addr
}

// ---------------------------------------------------------------------------
// 任务助手
// ---------------------------------------------------------------------------

fn tmpdir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("xfer-engine-flv-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let ctrl = std::env::temp_dir().join(format!("xfer-engine-flv-ctrl-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&ctrl);
    std::env::set_var("XFER_CTRL_DIR", &ctrl);
    d
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

/// 等到"已落盘字节 ≥ min"且任务仍是 active（直播看的是字节数，没有百分比）。
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

// ---------------------------------------------------------------------------
// 用例
// ---------------------------------------------------------------------------

/// 直播识别 + 持续录制 + 字段上报 + 暂停保留现场（产物时间轴从 0 开始）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flv_live_records_reports_fields_and_pause_keeps_scene() {
    let dir = tmpdir("flv-record");
    let srv = start_flv_server(vec![
        Step::probe(),
        Step::Forever {
            base: 3_600_000,
            flavor: 1,
        },
    ])
    .await;
    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![srv.url("/live/stream.flv")],
            &serde_json::json!({"dir": dir, "flv-live-stall-timeout": "20"}),
            None,
        )
        .expect("addUri 应成功");

    assert!(
        wait_progress(&mgr, &gid, 40_000, 20_000).await,
        "20s 内应录进数据"
    );
    let st = tell(&mgr, &gid).await;
    assert_eq!(st["isLive"], true, "FLV 直播录制任务必须带 isLive 标记");
    let ms_live = st["liveRecordedMs"].as_u64().unwrap_or(0);
    assert!(ms_live > 0, "录制期间应有已录制时长，实际 {st:?}");
    assert_eq!(
        st["totalLength"].as_u64().unwrap_or(0),
        0,
        "录制期间没有总长（流在生长），界面不该有百分比分母"
    );

    // 暂停：现场保留（控制文件）、产物前缀保住
    mgr.pause(&gid).expect("pause 应成功");
    let st = wait_status(&mgr, &gid, "paused", 15_000)
        .await
        .expect("暂停未生效");
    assert_eq!(st["isLive"], true, "暂停后仍是录制任务（形态不回落）");
    let file = file_of(&st);
    assert!(
        xfer_http::flv_has_live_ctrl(&file),
        "暂停后应留下直播现场（控制文件）"
    );
    let data = std::fs::read(&file).unwrap();
    assert!(data.len() >= 40_000, "产物应保留已录字节");
    let tags = product_tags(&data);
    let ts = media_ts(&tags);
    assert!(!ts.is_empty());
    assert_eq!(ts[0], 0, "产物时间轴必须归零（开播以来的时间戳被清零）");
    assert!(
        ts.windows(2).all(|w| w[0] <= w[1]),
        "产物时间轴不得倒退: {ts:?}"
    );
    assert!(
        st["liveRecordedMs"].as_u64().unwrap_or(0) >= ms_live,
        "暂停后的已录制时长不得回退"
    );
}

/// 断线重连续录 + 断口闭合 + 停播（持续 404）收尾：产物连续可播、时长正确。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flv_live_reconnect_closes_gap_and_finalizes_on_gone() {
    let dir = tmpdir("flv-gap");
    let srv = start_flv_server(vec![
        Step::probe(),
        Step::Eof {
            base: 1_000_000,
            n: 10,
            flavor: 2,
        },
        Step::Eof {
            base: 1_003_000,
            n: 10,
            flavor: 3,
        },
        Step::Gone,
    ])
    .await;
    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![srv.url("/live/stream.flv")],
            &serde_json::json!({"dir": dir, "flv-live-stall-timeout": "20"}),
            None,
        )
        .expect("addUri 应成功");
    let st = wait_status(&mgr, &gid, "complete", 40_000)
        .await
        .expect("地址持续 404 后应按录制结束收尾");
    assert_eq!(st["isLive"], true, "收尾后仍是录制形态");
    assert_eq!(
        st["liveRecordedMs"].as_u64().unwrap_or(0),
        720,
        "已录时长 = 两段各 360ms，断线期间的 3 秒不进产物"
    );
    let file = file_of(&st);
    assert!(
        !xfer_http::flv_has_live_ctrl(&file),
        "录到干净后现场应被收掉"
    );
    let data = std::fs::read(&file).unwrap();
    assert_eq!(
        st["totalLength"].as_u64().unwrap_or(0),
        data.len() as u64,
        "完成后总长定格为产物长度"
    );
    let tags = product_tags(&data);
    assert_eq!(
        tags,
        vec![
            (18, 0, 16),
            (9, 0, PAYLOAD),
            (8, 40, PAYLOAD),
            (9, 80, PAYLOAD),
            (8, 120, PAYLOAD),
            (9, 160, PAYLOAD),
            (8, 200, PAYLOAD),
            (9, 240, PAYLOAD),
            (8, 280, PAYLOAD),
            (9, 320, PAYLOAD),
            (8, 360, PAYLOAD),
            (18, 360, 16),
            (9, 360, PAYLOAD),
            (8, 400, PAYLOAD),
            (9, 440, PAYLOAD),
            (8, 480, PAYLOAD),
            (9, 520, PAYLOAD),
            (8, 560, PAYLOAD),
            (9, 600, PAYLOAD),
            (8, 640, PAYLOAD),
            (9, 680, PAYLOAD),
            (8, 720, PAYLOAD),
        ],
        "两段必须首尾相接：文件头只出现一次、重连带的头被跳过、断口闭合"
    );
    assert_eq!(srv.hits(), 9, "1 次探测 + 2 次录制连接 + 6 次 404 收尾探测");
}

/// 停滞收尾：连接还挂着但不再有数据 → 按录制结束收尾，产物完好。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flv_live_stall_ends_recording() {
    let dir = tmpdir("flv-stall");
    let srv = start_flv_server(vec![
        Step::probe(),
        Step::Hang {
            base: 500_000,
            n: 6,
            flavor: 4,
        },
    ])
    .await;
    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![srv.url("/live/stream.flv")],
            // 停滞 2 秒：发完 6 个 tag（120ms）后挂住，很快就该收尾
            &serde_json::json!({"dir": dir, "flv-live-stall-timeout": "2"}),
            None,
        )
        .expect("addUri 应成功");
    let st = wait_status(&mgr, &gid, "complete", 30_000)
        .await
        .expect("停滞超时后应按录制结束收尾");
    assert_eq!(st["isLive"], true);
    assert_eq!(st["liveRecordedMs"].as_u64().unwrap_or(0), 200);
    let data = std::fs::read(file_of(&st)).unwrap();
    let ts = media_ts(&product_tags(&data));
    assert_eq!(ts, vec![0, 40, 80, 120, 160, 200], "发到的 tag 全都要在产物里");
}

/// 暂停 → 恢复：已录前缀保住、不重录；恢复后时间轴接着走。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flv_pause_resume_keeps_prefix_and_continues_timeline() {
    let dir = tmpdir("flv-pause");
    let srv = start_flv_server(vec![
        Step::probe(),
        Step::Forever {
            base: 2_000_000,
            flavor: 5,
        },
    ])
    .await;
    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![srv.url("/live/stream.flv")],
            &serde_json::json!({"dir": dir, "flv-live-stall-timeout": "20"}),
            None,
        )
        .expect("addUri 应成功");
    assert!(wait_progress(&mgr, &gid, 40_000, 20_000).await, "应录进数据");
    mgr.pause(&gid).unwrap();
    let st = wait_status(&mgr, &gid, "paused", 15_000).await.expect("暂停未生效");
    let file = file_of(&st);
    let len1 = std::fs::metadata(&file).unwrap().len();
    let ms1 = st["liveRecordedMs"].as_u64().unwrap_or(0);
    assert!(xfer_http::flv_has_live_ctrl(&file));
    assert!(len1 >= 40_000 && ms1 > 0);

    // 恢复：服务端的流已经往前走了（Forever 的第 3 次请求基准 +30s）
    mgr.unpause(&gid).expect("unpause 应成功");
    let mut waited = 0u64;
    loop {
        tokio::time::sleep(Duration::from_millis(50)).await;
        waited += 50;
        if tell(&mgr, &gid).await["completedLength"].as_u64().unwrap_or(0) > len1 + 16_000
            || waited >= 20_000
        {
            break;
        }
    }
    mgr.pause(&gid).unwrap();
    let st = wait_status(&mgr, &gid, "paused", 15_000).await.expect("再次暂停未生效");
    let len2 = std::fs::metadata(file_of(&st)).unwrap().len();
    assert!(len2 > len1, "恢复后应继续增长（{len1} → {len2}）");
    assert!(
        st["liveRecordedMs"].as_u64().unwrap_or(0) > ms1,
        "已录时长从现场继续累计"
    );
    let data = std::fs::read(file_of(&st)).unwrap();
    let ts = media_ts(&product_tags(&data));
    assert_eq!(ts[0], 0, "产物仍从 0 开始");
    assert!(
        ts.windows(2).all(|w| w[0] <= w[1]),
        "恢复后的时间轴必须接着现场走（不回退）"
    );
    assert!(
        (len2 - len1) as usize >= 16_000,
        "恢复后的追加不能把旧内容重写一遍"
    );
}

/// 服务端从头发同一份内容（重播）→ 收尾，不能把内容重复追加。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flv_replay_from_start_ends_recording() {
    let dir = tmpdir("flv-replay");
    let doc = flv_segment(0, 5, TS_STEP, 6, PAYLOAD);
    let srv = start_flv_server(vec![
        Step::probe(),
        Step::Eof {
            base: 0,
            n: 5,
            flavor: 6,
        },
    ])
    .await;
    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![srv.url("/live/stream.flv")],
            &serde_json::json!({"dir": dir, "flv-live-stall-timeout": "20"}),
            None,
        )
        .expect("addUri 应成功");
    let st = wait_status(&mgr, &gid, "complete", 30_000)
        .await
        .expect("重播应被识别并收尾");
    assert_eq!(st["isLive"], true);
    assert_eq!(st["liveRecordedMs"].as_u64().unwrap_or(0), 160);
    let data = std::fs::read(file_of(&st)).unwrap();
    assert_eq!(
        data, doc,
        "产物恰好一遍内容：重播的整遍都不能写进去"
    );
}

/// 带 Content-Length 的 FLV 是点播文件：普通下载、逐字节原样、不标直播。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flv_vod_with_content_length_is_plain_download() {
    let dir = tmpdir("flv-vod");
    let doc = flv_segment(1_000_000, 8, 40, 7, 512);
    let addr = start_static_server("/vod/movie.flv", doc.clone(), false).await;
    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![format!("http://{addr}/vod/movie.flv")],
            &serde_json::json!({"dir": dir}),
            None,
        )
        .expect("addUri 应成功");
    let st = wait_status(&mgr, &gid, "complete", 30_000)
        .await
        .expect("点播 FLV 应完成");
    assert_eq!(st["isLive"], false, "点播文件不是直播任务");
    assert_eq!(st["liveRecordedMs"].as_u64().unwrap_or(0), 0);
    assert_eq!(
        std::fs::read(file_of(&st)).unwrap(),
        doc,
        "点播 FLV 必须逐字节原样下载（不做时间轴改写）"
    );
}

/// `flv=false`：关闭嗅探，chunked 的 FLV 也按普通文件下载（内容原样）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flv_can_be_disabled_per_task() {
    let dir = tmpdir("flv-off");
    let doc = flv_segment(1_000_000, 6, 40, 8, 512);
    let addr = start_static_server("/vod/chunked.flv", doc.clone(), true).await;
    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![format!("http://{addr}/vod/chunked.flv")],
            &serde_json::json!({"dir": dir, "flv": "false"}),
            None,
        )
        .expect("addUri 应成功");
    let st = wait_status(&mgr, &gid, "complete", 30_000)
        .await
        .expect("关闭 FLV 后应普通下载完成");
    assert_eq!(st["isLive"], false);
    assert_eq!(
        std::fs::read(file_of(&st)).unwrap(),
        doc,
        "关闭嗅探后内容必须原样（时间戳不动）"
    );
}

/// 内容不是 FLV（扩展名像、chunked 长响应）：嗅探落空 → 回退普通下载。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flv_non_flv_body_falls_back_to_http() {
    let dir = tmpdir("flv-fallback");
    let text = b"<html><body>not an flv at all</body></html>".to_vec();
    let addr = start_static_server("/live/error.flv", text.clone(), true).await;
    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![format!("http://{addr}/live/error.flv")],
            &serde_json::json!({"dir": dir}),
            None,
        )
        .expect("addUri 应成功");
    let st = wait_status(&mgr, &gid, "complete", 30_000)
        .await
        .expect("回退普通下载后应完成");
    assert_eq!(st["isLive"], false, "没嗅探成功就不是直播任务");
    assert_eq!(std::fs::read(file_of(&st)).unwrap(), text);
}

/// `flv=true`：地址既不带 `.flv` 也不报 FLV 类型（chunked 长连接）也强制录制。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flv_forced_option_records_unknown_endpoint() {
    let dir = tmpdir("flv-force");
    // 地址不带 `.flv`、响应也不报类型：只有 `flv=true` 才会走录制
    let srv = start_flv_server_at(
        "/live/stream.bin",
        vec![
            Step::probe(),
            Step::Forever {
                base: 1_500_000,
                flavor: 9,
            },
        ],
    )
    .await;
    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_uri(
            vec![srv.url("/live/stream.bin")],
            &serde_json::json!({
                "dir": dir,
                "flv": "true",
                "flv-live-stall-timeout": "20",
            }),
            None,
        )
        .expect("addUri 应成功");
    assert!(
        wait_progress(&mgr, &gid, 40_000, 20_000).await,
        "强制 FLV 后应按直播录制"
    );
    let st = tell(&mgr, &gid).await;
    assert_eq!(st["isLive"], true);
    assert!(st["liveRecordedMs"].as_u64().unwrap_or(0) > 0);
    mgr.pause(&gid).unwrap();
    let st = wait_status(&mgr, &gid, "paused", 15_000)
        .await
        .expect("暂停未生效");
    assert!(xfer_http::flv_has_live_ctrl(&file_of(&st)));
}
