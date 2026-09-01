//! xfer-engine BT 集成：TaskManager::add_torrent 全链路（tracker + seed）。

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use base64::Engine;
use sha1::{Digest, Sha1};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use xfer_bencode::{bytes, dict, encode, int, parse_torrent};
use xfer_bt::message::{encode_handshake, Message, PeerReader};
use xfer_engine::TaskManager;
use xfer_types::{InfoHash, PeerId};

const PIECE_LEN: usize = 64 * 1024;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("xfer_bt=debug,xfer_engine=info,warn")
        .with_test_writer()
        .try_init();
}

fn sha1_of(b: &[u8]) -> [u8; 20] {
    let mut h = Sha1::new();
    h.update(b);
    h.finalize().into()
}

async fn bind_random() -> (TcpListener, SocketAddr) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    (l, addr)
}

async fn serve_seed(
    listener: TcpListener,
    data: Arc<Vec<u8>>,
    info_hash: InfoHash,
    peer_id: PeerId,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let data = data.clone();
        tokio::spawn(async move {
            let _ = handle_seed_peer(stream, &data, info_hash, peer_id).await;
        });
    }
}

async fn handle_seed_peer(
    mut stream: TcpStream,
    data: &[u8],
    info_hash: InfoHash,
    peer_id: PeerId,
) -> std::io::Result<()> {
    let mut reader = PeerReader::new();
    let hs = match reader.read_handshake(&mut stream).await? {
        Some(h) => h,
        None => return Ok(()), // 对端关闭连接
    };
    if hs.info_hash != info_hash {
        return Ok(());
    }
    stream
        .write_all(&encode_handshake(&info_hash, &peer_id))
        .await?;
    let n_pieces = data.len().div_ceil(PIECE_LEN);
    let mut bf = vec![0u8; n_pieces.div_ceil(8)];
    for i in 0..n_pieces {
        bf[i / 8] |= 0x80 >> (i % 8);
    }
    stream.write_all(&Message::Bitfield(bf).encode()).await?;
    stream.write_all(&Message::Unchoke.encode()).await?;
    loop {
        match reader.read_message(&mut stream).await? {
            None => break,
            Some(Message::Request {
                index,
                begin,
                length,
            }) => {
                let off = index as usize * PIECE_LEN + begin as usize;
                let end = (off + length as usize).min(data.len());
                if off >= data.len() {
                    continue;
                }
                stream
                    .write_all(
                        &Message::Piece {
                            index,
                            begin,
                            block: data[off..end].to_vec(),
                        }
                        .encode(),
                    )
                    .await?;
            }
            Some(Message::Interested) | Some(Message::KeepAlive) | Some(Message::Cancel { .. }) => {
            }
            Some(_) => {}
        }
    }
    Ok(())
}

async fn tracker_announce(
    Query(_q): Query<HashMap<String, String>>,
    State(seed): State<Arc<RwLock<Option<SocketAddr>>>>,
) -> Response {
    let Some(addr) = *seed.read().unwrap() else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "no seed").into_response();
    };
    let mut peers = Vec::with_capacity(6);
    if let std::net::IpAddr::V4(v4) = addr.ip() {
        peers.extend_from_slice(&v4.octets());
    } else {
        peers.extend_from_slice(&[127, 0, 0, 1]);
    }
    peers.extend_from_slice(&addr.port().to_be_bytes());
    let resp = dict(BTreeMap::from([
        (b"interval".to_vec(), int(60)),
        (b"peers".to_vec(), bytes(peers)),
    ]));
    ([(header::CONTENT_TYPE, "text/plain")], encode(&resp)).into_response()
}

async fn start_tracker() -> (SocketAddr, Arc<RwLock<Option<SocketAddr>>>) {
    let state: Arc<RwLock<Option<SocketAddr>>> = Arc::new(RwLock::new(None));
    let app = Router::new()
        .route("/announce", get(tracker_announce))
        .with_state(state.clone());
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(l, app).await;
    });
    (addr, state)
}

fn make_torrent_b64(data: &[u8], tracker_url: &str) -> String {
    let pieces: Vec<u8> = data.chunks(PIECE_LEN).flat_map(sha1_of).collect();
    let info = dict(BTreeMap::from([
        (b"name".to_vec(), bytes("bt-data.bin")),
        (b"piece length".to_vec(), int(PIECE_LEN as i64)),
        (b"length".to_vec(), int(data.len() as i64)),
        (b"pieces".to_vec(), bytes(pieces)),
    ]));
    let top = dict(BTreeMap::from([
        (b"announce".to_vec(), bytes(tracker_url)),
        (b"info".to_vec(), info),
    ]));
    base64::engine::general_purpose::STANDARD.encode(encode(&top))
}

#[tokio::test]
async fn add_torrent_downloads_and_completes() {
    let dir = std::env::temp_dir().join(format!("xfer-engine-bt-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..(3 * PIECE_LEN + 777))
        .map(|i| (i.wrapping_mul(73) % 239) as u8)
        .collect();
    init_tracing();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb64 = make_torrent_b64(&data, &tracker_url);
    // info_hash 从种子解析（seed 需要）
    let meta = parse_torrent(
        &base64::engine::general_purpose::STANDARD
            .decode(&tb64)
            .unwrap(),
    )
    .unwrap();
    let ih = InfoHash::from_bytes(&meta.info_hash);

    let data_arc = Arc::new(data.clone());
    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = Some(saddr);
    let ih2 = ih;
    let pid = PeerId::azureus_prefix(&[0x42; 12]);
    tokio::spawn(serve_seed(sl, data_arc, ih2, pid));

    let mgr = TaskManager::start(dir.clone(), 2);
    let options = serde_json::json!({});
    let gid = mgr
        .add_torrent(&tb64, &options, None)
        .expect("addTorrent 应成功");

    // 轮询直到完成（30s 上限）
    let mut done = false;
    let mut last = String::new();
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let st = mgr.tell_status_native(&gid, None).unwrap();
        last = st["status"].to_string();
        if st["status"] == "complete" {
            done = true;
            break;
        }
        if st["status"] == "error" {
            panic!("任务进入 error: {:?}", st);
        }
    }
    if !done {
        let st = mgr.tell_status_native(&gid, None).unwrap();
        let peers = mgr.get_peers(&gid).unwrap();
        panic!(
            "BT 任务 30s 内未完成: status={last} completed={} peers={peers} err={}",
            st["completedLength"], st["errorMessage"]
        );
    }

    let out = std::fs::read(dir.join("bt-data.bin")).unwrap();
    assert_eq!(out, data, "下载文件与源数据不一致");

    // getPeers 应有记录
    let peers = mgr.get_peers(&gid).unwrap();
    assert!(!peers.as_array().unwrap().is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn add_torrent_rejects_invalid_base64() {
    let dir = std::env::temp_dir().join(format!("xfer-engine-bt2-{}", std::process::id()));
    let mgr = TaskManager::start(dir.clone(), 2);
    assert!(mgr
        .add_torrent("not-base64!!!", &serde_json::json!({}), None)
        .is_err());
    // 合法 base64 但不是 torrent
    let junk = base64::engine::general_purpose::STANDARD.encode(b"hello");
    assert!(mgr
        .add_torrent(&junk, &serde_json::json!({}), None)
        .is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

// ----------------------------------------------------------------------
// 暂停 → 恢复（断点续传）
// ----------------------------------------------------------------------

/// 可控可用片 + 每片请求计数 + 活动连接计数的 seed。
struct ResumeSeed {
    data: Arc<Vec<u8>>,
    avail: Arc<RwLock<Vec<bool>>>,
    counts: Arc<Mutex<Vec<u32>>>,
    /// 当前活跃的 peer 连接数（暂停后应归零 → 引擎后台任务已全停）。
    active: Arc<AtomicUsize>,
}

async fn serve_resume_seed(
    listener: TcpListener,
    seed: Arc<ResumeSeed>,
    info_hash: InfoHash,
    peer_id: PeerId,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let seed = seed.clone();
        tokio::spawn(async move {
            let _ = handle_resume_seed(stream, &seed, info_hash, peer_id).await;
        });
    }
}

async fn handle_resume_seed(
    mut stream: TcpStream,
    seed: &ResumeSeed,
    info_hash: InfoHash,
    peer_id: PeerId,
) -> std::io::Result<()> {
    struct ConnGuard(Arc<AtomicUsize>);
    impl Drop for ConnGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    seed.active.fetch_add(1, Ordering::SeqCst);
    let _guard = ConnGuard(seed.active.clone());

    let mut reader = PeerReader::new();
    let hs = match reader.read_handshake(&mut stream).await? {
        Some(h) => h,
        None => return Ok(()),
    };
    if hs.info_hash != info_hash {
        return Ok(());
    }
    stream
        .write_all(&encode_handshake(&info_hash, &peer_id))
        .await?;
    // 连接建立时按当前 avail 生成 bitfield（暂停/恢复是不同引擎、不同连接）
    let n_pieces = seed.data.len().div_ceil(PIECE_LEN);
    let avail_now = seed.avail.read().unwrap().clone();
    let mut bf = vec![0u8; n_pieces.div_ceil(8)];
    for i in 0..n_pieces {
        if avail_now.get(i).copied().unwrap_or(false) {
            bf[i / 8] |= 0x80 >> (i % 8);
        }
    }
    stream.write_all(&Message::Bitfield(bf).encode()).await?;
    stream.write_all(&Message::Unchoke.encode()).await?;
    loop {
        match reader.read_message(&mut stream).await? {
            None => break,
            Some(Message::Request {
                index,
                begin,
                length,
            }) => {
                if !seed
                    .avail
                    .read()
                    .unwrap()
                    .get(index as usize)
                    .copied()
                    .unwrap_or(false)
                {
                    continue;
                }
                seed.counts.lock().unwrap()[index as usize] += 1;
                let off = index as usize * PIECE_LEN + begin as usize;
                let end = (off + length as usize).min(seed.data.len());
                if off >= seed.data.len() {
                    continue;
                }
                stream
                    .write_all(
                        &Message::Piece {
                            index,
                            begin,
                            block: seed.data[off..end].to_vec(),
                        }
                        .encode(),
                    )
                    .await?;
            }
            Some(Message::Interested) | Some(Message::KeepAlive) | Some(Message::Cancel { .. }) => {
            }
            Some(_) => {}
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_then_unpause_resumes_from_ctrl_file() {
    // 隔离控制文件目录（与引擎级 resume 测试分目录，避免跨测试串扰）
    std::env::set_var(
        "XFER_CTRL_DIR",
        std::env::temp_dir()
            .join(format!("xfer-engine-bt-ctrl-{}", std::process::id()))
            .to_string_lossy()
            .to_string(),
    );

    let dir = std::env::temp_dir().join(format!("xfer-engine-bt3-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    init_tracing();

    const N: usize = 8;
    let data: Vec<u8> = (0..(N * PIECE_LEN))
        .map(|i| (i.wrapping_mul(73) % 239) as u8)
        .collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb64 = make_torrent_b64(&data, &tracker_url);
    let meta = parse_torrent(
        &base64::engine::general_purpose::STANDARD
            .decode(&tb64)
            .unwrap(),
    )
    .unwrap();
    let ih = InfoHash::from_bytes(&meta.info_hash);

    // 阶段一：仅前 4 片可用
    let mut avail = vec![false; N];
    for i in 0..4 {
        avail[i] = true;
    }
    let avail = Arc::new(RwLock::new(avail));
    let counts = Arc::new(Mutex::new(vec![0u32; N]));
    let active = Arc::new(AtomicUsize::new(0));
    let seed = Arc::new(ResumeSeed {
        data: Arc::new(data.clone()),
        avail: avail.clone(),
        counts: counts.clone(),
        active: active.clone(),
    });
    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = Some(saddr);
    tokio::spawn(serve_resume_seed(
        sl,
        seed,
        ih,
        PeerId::azureus_prefix(&[0x77; 12]),
    ));

    let mgr = TaskManager::start(dir.clone(), 2);
    let gid = mgr
        .add_torrent(&tb64, &serde_json::json!({}), None)
        .expect("addTorrent 应成功");

    // 等待前 4 片落盘（seed 不提供后 4 片，进度会停在这里）
    let mut got4 = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let st = mgr.tell_status_native(&gid, None).unwrap();
        if st["status"] == "error" {
            panic!("任务进入 error: {st:?}");
        }
        let completed: u64 = st["completedLength"].as_u64().unwrap_or(0);
        if completed >= (4 * PIECE_LEN) as u64 {
            got4 = true;
            break;
        }
    }
    assert!(got4, "阶段一未能下载前 4 片");

    // 暂停 → 引擎走取消分支保存控制文件
    mgr.pause(&gid).expect("暂停应成功");
    let mut paused = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if mgr.tell_status_native(&gid, None).unwrap()["status"] == "paused" {
            paused = true;
            break;
        }
    }
    assert!(paused, "任务未进入 paused");

    // 静默验证（回归：暂停后后台任务必须全停）——
    // 旧缺陷：暂停只停了主循环，peer 会话/监听器继续后台下载并
    // 更新续传文件，恢复时进度凭空跳变（如 10% → 50%）。
    // 判据：① 种子侧活动连接归零（全部会话已退出）；
    //       ② 静默窗口内无新请求、进度冻结。
    let mut conns_zero = false;
    for _ in 0..20 {
        if active.load(Ordering::SeqCst) == 0 {
            conns_zero = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        conns_zero,
        "暂停后仍有 {} 条 peer 连接存活：后台任务未停止",
        active.load(Ordering::SeqCst)
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    let quiet_counts = counts.lock().unwrap().clone();
    let quiet_completed = mgr
        .tell_status_native(&gid, None)
        .unwrap()["completedLength"]
        .as_u64()
        .unwrap_or(0);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        counts.lock().unwrap().clone(),
        quiet_counts,
        "暂停后不应再有下载请求（后台任务应已全停）"
    );
    let now_completed = mgr
        .tell_status_native(&gid, None)
        .unwrap()["completedLength"]
        .as_u64()
        .unwrap_or(0);
    assert_eq!(
        now_completed, quiet_completed,
        "暂停后进度应冻结：{quiet_completed} → {now_completed}"
    );

    let ctrl = xfer_storage::ctrl_path(&dir.join("bt-data.bin"));
    assert!(ctrl.exists(), "暂停后应保留续传控制文件: {ctrl:?}");
    let at_pause = counts.lock().unwrap().clone();
    assert!(at_pause[..4].iter().sum::<u32>() > 0, "阶段一应有下载请求");
    assert_eq!(at_pause[4..].iter().sum::<u32>(), 0, "阶段一不提供后 4 片");

    // 阶段二：放开全部片段，恢复任务
    *avail.write().unwrap() = vec![true; N];
    mgr.unpause(&gid).expect("恢复应成功");

    let mut done = false;
    for _ in 0..80 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let st = mgr.tell_status_native(&gid, None).unwrap();
        if st["status"] == "complete" {
            done = true;
            break;
        }
        if st["status"] == "error" {
            panic!("恢复后任务进入 error: {st:?}");
        }
    }
    assert!(done, "恢复后任务未完成");

    let out = std::fs::read(dir.join("bt-data.bin")).unwrap();
    assert_eq!(out, data, "续传后文件与源数据不一致");

    let final_counts = counts.lock().unwrap().clone();
    assert_eq!(
        final_counts[..4], at_pause[..4],
        "暂停前已完成的前 4 片在恢复后不应再被请求: pause={at_pause:?} final={final_counts:?}"
    );
    assert!(
        final_counts[4..].iter().all(|&n| n > 0),
        "后 4 片应逐片请求: {final_counts:?}"
    );
    assert!(!ctrl.exists(), "完成后控制文件应被清理: {ctrl:?}");

    let _ = std::fs::remove_dir_all(&dir);
}
