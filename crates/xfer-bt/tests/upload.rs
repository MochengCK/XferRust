//! 上传统计与上传限速端到端：seed 引擎（TorrentEngine seed 模式）
//! 向 leech 引擎供块，验证 `uploaded()` 计数与 `upload_limit` 限速。

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use sha1::{Digest, Sha1};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use xfer_bencode::{bytes, dict, encode, int, parse_torrent, TorrentMeta};
use xfer_bt::{TorrentConfig, TorrentEngine};
use xfer_types::PeerId;

const PIECE_LEN: usize = 64 * 1024;

fn sha1_of(b: &[u8]) -> [u8; 20] {
    let mut h = Sha1::new();
    h.update(b);
    h.finalize().into()
}

/// 简易 HTTP tracker：单 peer 槽（测试手工写入 seed 监听地址）。
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
        (b"complete".to_vec(), int(1)),
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

fn make_torrent_bytes(data: &[u8], tracker_url: &str) -> Vec<u8> {
    let pieces: Vec<u8> = data.chunks(PIECE_LEN).flat_map(sha1_of).collect();
    let info = dict(BTreeMap::from([
        (b"name".to_vec(), bytes("data.bin")),
        (b"piece length".to_vec(), int(PIECE_LEN as i64)),
        (b"length".to_vec(), int(data.len() as i64)),
        (b"pieces".to_vec(), bytes(pieces)),
    ]));
    let top = dict(BTreeMap::from([
        (b"announce".to_vec(), bytes(tracker_url)),
        (b"info".to_vec(), info),
    ]));
    encode(&top)
}

fn meta_of(tb: &[u8]) -> TorrentMeta {
    parse_torrent(tb).unwrap()
}

fn make_config(
    meta: &TorrentMeta,
    dir: &std::path::Path,
    tag: u8,
    upload_limit: u64,
    seed_mode: bool,
    encryption: xfer_bt::EncryptionMode,
    bt_protocol: xfer_bt::BtProtocol,
) -> TorrentConfig {
    TorrentConfig {
        dir: dir.to_path_buf(),
        peer_id: PeerId::azureus_prefix(&[tag; 12]),
        listen_port: 0,
        max_peers: 8,
        adaptive: false,
        numwant: 50,
        announce_urls: meta
            .announce
            .iter()
            .cloned()
            .chain(meta.announce_list.iter().flat_map(|t| t.iter().cloned()))
            .collect(),
        udp_announce_urls: Vec::new(),
        pipeline: 0,
        enable_dht: false,
        dht_port: 0,
        encryption,
        bt_protocol,
        download_limit: 0,
        upload_limit,
        seed_mode,
        seed_duration: 0,
    }
}

/// 启动 seed 引擎（目录内预写完整数据），等监听就绪后把地址写入 tracker 槽。
async fn spawn_seed(
    meta: TorrentMeta,
    data: &[u8],
    dir: &std::path::Path,
    tracker_state: &Arc<RwLock<Option<SocketAddr>>>,
    upload_limit: u64,
    encryption: xfer_bt::EncryptionMode,
    bt_protocol: xfer_bt::BtProtocol,
) -> (Arc<TorrentEngine>, CancellationToken) {
    std::fs::write(dir.join("data.bin"), data).unwrap();
    let cfg = make_config(&meta, dir, 1, upload_limit, true, encryption, bt_protocol);
    let engine = TorrentEngine::new(meta, cfg).unwrap();
    let cancel = CancellationToken::new();
    let c2 = cancel.clone();
    let e2 = engine.clone();
    tokio::spawn(async move {
        // seed 模式永久做种，测试结束时经 cancel 停止
        let _ = e2.run(c2).await;
    });
    // 等监听端口就绪（run 首先绑定监听器）
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let port = engine.listen_port();
        if port != 0 {
            *tracker_state.write().unwrap() = Some(SocketAddr::from(([127, 0, 0, 1], port)));
            break;
        }
        assert!(Instant::now() < deadline, "seed 监听端口未在 5s 内就绪");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    (engine, cancel)
}

/// 运行 leech 下载，返回 (耗时, 下载目录内文件内容)。
async fn run_leech(
    meta: TorrentMeta,
    dir: &std::path::Path,
    encryption: xfer_bt::EncryptionMode,
    bt_protocol: xfer_bt::BtProtocol,
) -> (Duration, Vec<u8>) {
    let cfg = make_config(&meta, dir, 2, 0, false, encryption, bt_protocol);
    let engine = TorrentEngine::new(meta, cfg).unwrap();
    let start = Instant::now();
    tokio::time::timeout(
        Duration::from_secs(60),
        engine.run(CancellationToken::new()),
    )
    .await
    .expect("leech 下载超时")
    .expect("leech 下载失败");
    let content = std::fs::read(dir.join("data.bin")).unwrap();
    (start.elapsed(), content)
}

fn fresh_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("xfer-bt-upload-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// 上传计数：leech 完成后，seed 的 uploaded() 应不小于数据总量
/// （全部块均经 serve_block 发出并计数）。
#[tokio::test]
async fn uploaded_counter_counts_served_bytes() {
    let data: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    let (tracker_addr, state) = start_tracker().await;
    let tracker_url = format!("http://{tracker_addr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    let seed_dir = fresh_dir("seed1");
    let leech_dir = fresh_dir("leech1");
    let (seed, cancel) = spawn_seed(meta.clone(), &data, &seed_dir, &state, 0, xfer_bt::EncryptionMode::PlaintextOnly, xfer_bt::BtProtocol::TcpOnly).await;
    let (_, content) = run_leech(meta, &leech_dir, xfer_bt::EncryptionMode::PlaintextOnly, xfer_bt::BtProtocol::TcpOnly).await;
    assert_eq!(content, data, "下载内容与源数据不一致");
    // 允许少量重复请求导致的多余计数，但绝不能少于数据总量
    assert!(
        seed.uploaded() >= data.len() as u64,
        "uploaded() = {}，应 >= 数据总量 {}",
        seed.uploaded(),
        data.len()
    );
    cancel.cancel();
    let _ = std::fs::remove_dir_all(&seed_dir);
    let _ = std::fs::remove_dir_all(&leech_dir);
}

/// 上传限速：64 KiB/s 上限下传输 256 KiB 至少需 2s（不限速时 < 1s），
/// 验证 serve_block 前的令牌桶等待真实生效。
#[tokio::test]
async fn upload_limit_throttles_serving() {
    let data: Vec<u8> = (0..256 * 1024).map(|i| (i % 199) as u8).collect();
    let (tracker_addr, state) = start_tracker().await;
    let tracker_url = format!("http://{tracker_addr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    let seed_dir = fresh_dir("seed2");
    let leech_dir = fresh_dir("leech2");
    let (seed, cancel) = spawn_seed(meta.clone(), &data, &seed_dir, &state, 64 * 1024, xfer_bt::EncryptionMode::PlaintextOnly, xfer_bt::BtProtocol::TcpOnly).await;
    let (elapsed, content) = run_leech(meta, &leech_dir, xfer_bt::EncryptionMode::PlaintextOnly, xfer_bt::BtProtocol::TcpOnly).await;
    assert_eq!(content, data, "下载内容与源数据不一致");
    assert!(
        elapsed >= Duration::from_secs(2),
        "上传限速未生效：256 KiB @ 64 KiB/s 应 >= 2s，实际 {:?}",
        elapsed
    );
    assert!(seed.uploaded() >= data.len() as u64);
    cancel.cancel();
    let _ = std::fs::remove_dir_all(&seed_dir);
    let _ = std::fs::remove_dir_all(&leech_dir);
}

/// MSE 端到端：双引擎全开 MSE，完整下载必须走加密流完成。
/// 出站 MSE 失败会明文重拨（§7.19④），因此必须断言下载过程中
/// 确实出现过 `encrypted == true` 的连接，否则测试会静默退化为明文。
#[tokio::test]
async fn mse_encrypted_engine_to_engine_transfer() {
    let data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 251) as u8).collect();
    let (tracker_addr, state) = start_tracker().await;
    let tracker_url = format!("http://{tracker_addr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    let seed_dir = fresh_dir("mse-seed");
    let leech_dir = fresh_dir("mse-leech");
    let (seed, cancel) = spawn_seed(meta.clone(), &data, &seed_dir, &state, 0, xfer_bt::EncryptionMode::PreferEncryption, xfer_bt::BtProtocol::TcpOnly).await;

    let cfg = make_config(
        &meta,
        &leech_dir,
        2,
        0,
        false,
        xfer_bt::EncryptionMode::PreferEncryption,
        xfer_bt::BtProtocol::TcpOnly,
    );
    let leech = TorrentEngine::new(meta, cfg).unwrap();
    let leech2 = leech.clone();
    let mut run_fut = Box::pin(tokio::spawn(async move {
        leech2.run(CancellationToken::new()).await
    }));

    // 下载过程中轮询连接加密标志
    let mut saw_encrypted = false;
    loop {
        tokio::select! {
            r = &mut run_fut => {
                r.unwrap().expect("leech 下载失败");
                break;
            }
            _ = tokio::time::sleep(Duration::from_millis(5)) => {
                if leech.peers_info().iter().any(|p| p.encrypted) {
                    saw_encrypted = true;
                }
            }
        }
    }
    assert!(saw_encrypted, "下载全程未出现加密连接——MSE 未真正生效");

    let content = std::fs::read(leech_dir.join("data.bin")).unwrap();
    assert_eq!(content, data, "加密传输后内容不一致");
    assert!(seed.uploaded() >= data.len() as u64);

    // 新字段回归：种子侧每 peer 上传计数、进度位图已知、传输协议。
    // 注：不要求进度=100%——Have 在 10s choking 轮次才批量发出，
    // 环回秒级传输在对端视角来不及收到（真实客户端即时发 Have，不受影响）。
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut detail = String::new();
    let mut ok = false;
    while Instant::now() < deadline {
        if let Some(p) = seed.peers_info().first() {
            detail = format!(
                "protocol={} uploaded={} progress={:?}",
                p.protocol, p.uploaded, p.progress
            );
            if p.protocol == "tcp" && p.progress.is_some() && p.uploaded >= data.len() as u64 {
                ok = true;
                break;
            }
        } else {
            detail = "no peers".to_string();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(ok, "种子侧每 peer 新字段回归失败: {detail}");

    cancel.cancel();
    let _ = std::fs::remove_dir_all(&seed_dir);
    let _ = std::fs::remove_dir_all(&leech_dir);
}

/// 用指定加密模式起一对 seed/leech，尝试在 `timeout` 内完成下载。
/// 成功返回内容，失败（超时/拒绝）返回 Err。每个用例独立 tracker 与目录。
async fn run_matrix_case(
    seed_enc: xfer_bt::EncryptionMode,
    leech_enc: xfer_bt::EncryptionMode,
    tag: &str,
    timeout_s: u64,
) -> Result<Vec<u8>, String> {
    let data: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
    let (tracker_addr, state) = start_tracker().await;
    let tracker_url = format!("http://{tracker_addr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    let seed_dir = fresh_dir(&format!("mx-seed-{tag}"));
    let leech_dir = fresh_dir(&format!("mx-leech-{tag}"));
    let (seed, cancel) = spawn_seed(meta.clone(), &data, &seed_dir, &state, 0, seed_enc, xfer_bt::BtProtocol::TcpOnly).await;

    let cfg = make_config(&meta, &leech_dir, 2, 0, false, leech_enc, xfer_bt::BtProtocol::TcpOnly);
    let engine = TorrentEngine::new(meta, cfg).unwrap();
    let res = tokio::time::timeout(
        Duration::from_secs(timeout_s),
        engine.run(CancellationToken::new()),
    )
    .await;

    let outcome = match res {
        Ok(Ok(())) => std::fs::read(leech_dir.join("data.bin"))
            .ok()
            .filter(|c| *c == data)
            .ok_or_else(|| "下载内容与源数据不一致".to_string()),
        Ok(Err(e)) => Err(format!("leech 运行失败: {e}")),
        Err(_) => Err("下载未在时限内完成（连接被拒绝/无可用对端）".to_string()),
    };

    cancel.cancel();
    drop(seed);
    let _ = std::fs::remove_dir_all(&seed_dir);
    let _ = std::fs::remove_dir_all(&leech_dir);
    outcome
}

/// 加密模式矩阵（P2 三档）：
/// ① 强制×强制：双方仅加密，传输成功；
/// ② 强制种子 × 仅明文 leech：种子拒绝明文连接，下载失败；
/// ③ 优先加密种子 × 仅明文 leech：回退明文协商，传输成功。
#[tokio::test]
async fn encryption_mode_matrix() {
    use xfer_bt::EncryptionMode::{ForceEncryption, PlaintextOnly, PreferEncryption};

    // ① 强制 × 强制
    let ok = run_matrix_case(ForceEncryption, ForceEncryption, "ff", 30).await;
    assert!(ok.is_ok(), "强制×强制应成功: {:?}", ok.err());

    // ② 强制种子 × 仅明文 leech → 必失败
    let rej = run_matrix_case(ForceEncryption, PlaintextOnly, "fp", 8).await;
    assert!(
        rej.is_err(),
        "强制种子应拒绝仅明文 leech，但却成功了"
    );

    // ③ 优先加密种子 × 仅明文 leech → 回退明文成功
    let fb = run_matrix_case(PreferEncryption, PlaintextOnly, "pb", 30).await;
    assert!(fb.is_ok(), "优先加密种子应能回退明文: {:?}", fb.err());
}

/// uTP 引擎对引擎传输（P3）：双方固定 `UtpOnly`，强制走 uTP（无 TCP 回退），
/// 验证 uTP 拨号/入站 + MSE/明文握手 + 数据完整性；并断言对端上报
/// `protocol == "utp"`。
#[tokio::test]
async fn utp_engine_to_engine_transfer() {
    use xfer_bt::{BtProtocol, EncryptionMode};

    let data: Vec<u8> = (0..512 * 1024).map(|i| (i % 251) as u8).collect();
    let (tracker_addr, state) = start_tracker().await;
    let tracker_url = format!("http://{tracker_addr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    let seed_dir = fresh_dir("utp-seed");
    let leech_dir = fresh_dir("utp-leech");
    let (seed, cancel) = spawn_seed(
        meta.clone(),
        &data,
        &seed_dir,
        &state,
        0,
        EncryptionMode::PreferEncryption,
        BtProtocol::UtpOnly,
    )
    .await;

    let cfg = make_config(
        &meta,
        &leech_dir,
        2,
        0,
        false,
        EncryptionMode::PreferEncryption,
        BtProtocol::UtpOnly,
    );
    let leech = TorrentEngine::new(meta, cfg).unwrap();
    let leech2 = leech.clone();
    let mut run_fut = Box::pin(tokio::spawn(async move {
        leech2.run(CancellationToken::new()).await
    }));

    // 下载过程中轮询：确认出现 uTP 对端（protocol == "utp"）
    let mut saw_utp = false;
    loop {
        tokio::select! {
            r = &mut run_fut => {
                r.unwrap().expect("uTP leech 下载失败");
                break;
            }
            _ = tokio::time::sleep(Duration::from_millis(5)) => {
                if leech.peers_info().iter().any(|p| p.protocol == "utp") {
                    saw_utp = true;
                }
            }
        }
    }
    assert!(saw_utp, "下载全程未出现 uTP 对端——uTP 拨号未生效");

    let content = std::fs::read(leech_dir.join("data.bin")).unwrap();
    assert_eq!(content, data, "uTP 传输后内容不一致");
    // 种子侧也应看到 uTP 对端
    assert!(
        seed.peers_info().iter().any(|p| p.protocol == "utp"),
        "种子侧未见 uTP 对端"
    );
    assert!(seed.uploaded() >= data.len() as u64);
    cancel.cancel();
    let _ = std::fs::remove_dir_all(&seed_dir);
    let _ = std::fs::remove_dir_all(&leech_dir);
}
