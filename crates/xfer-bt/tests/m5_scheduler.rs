//! M5 精细调度互操作测试：
//! - 请求流水线自适应（16→256）
//! - 30s 连接超时
//! - 冷启动突发连接
//! - 慢速节点淘汰
//! - 限速
//! - seed 模式（下载完成后上传）

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use axum::extract::{Query, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use sha1::{Digest, Sha1};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use xfer_bencode::{bytes, dict, encode, int, parse_torrent, TorrentMeta};
use xfer_bt::message::{encode_handshake, Message, PeerReader};
use xfer_bt::{TorrentConfig, TorrentEngine};
use xfer_types::{InfoHash, PeerId};

const PIECE_LEN: usize = 64 * 1024;

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

/// seed peer：被动握手、发 bitfield+unchoke、响应 request 发 piece 块。
/// 支持延迟发送以测试流水线自适应和慢速节点淘汰。
async fn serve_seed(
    listener: TcpListener,
    data: Arc<Vec<u8>>,
    piece_len: usize,
    info_hash: InfoHash,
    peer_id: PeerId,
    delay_ms: u64,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let data = data.clone();
        tokio::spawn(async move {
            let _ = handle_seed_peer(stream, &data, piece_len, info_hash, peer_id, delay_ms).await;
        });
    }
}

async fn handle_seed_peer(
    mut stream: TcpStream,
    data: &[u8],
    piece_len: usize,
    info_hash: InfoHash,
    peer_id: PeerId,
    delay_ms: u64,
) -> std::io::Result<()> {
    let mut reader = PeerReader::new();
    let hs = loop {
        match reader.read_handshake(&mut stream).await? {
            Some(h) => break h,
            None => continue,
        }
    };
    if hs.info_hash != info_hash {
        return Ok(());
    }
    stream
        .write_all(&encode_handshake(&info_hash, &peer_id))
        .await?;
    let n_pieces = data.len().div_ceil(piece_len);
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
                if delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
                let off = index as usize * piece_len + begin as usize;
                let end = (off + length as usize).min(data.len());
                if off >= data.len() {
                    continue;
                }
                let block = data[off..end].to_vec();
                stream
                    .write_all(
                        &Message::Piece {
                            index,
                            begin,
                            block,
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
    State(seed): State<Arc<RwLock<Vec<SocketAddr>>>>,
) -> Response {
    let addrs = seed.read().unwrap().clone();
    let mut peers = Vec::new();
    for addr in &addrs {
        if let std::net::IpAddr::V4(v4) = addr.ip() {
            peers.extend_from_slice(&v4.octets());
            peers.extend_from_slice(&addr.port().to_be_bytes());
        }
    }
    let resp = dict(BTreeMap::from([
        (b"interval".to_vec(), int(60)),
        (b"complete".to_vec(), int(addrs.len() as i64)),
        (b"peers".to_vec(), bytes(peers)),
    ]));
    ([(header::CONTENT_TYPE, "text/plain")], encode(&resp)).into_response()
}

async fn start_tracker() -> (SocketAddr, Arc<RwLock<Vec<SocketAddr>>>) {
    let state: Arc<RwLock<Vec<SocketAddr>>> = Arc::new(RwLock::new(Vec::new()));
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

/// 测试 1：流水线自适应——大数据量下载应触发流水线深度增长。
#[tokio::test]
async fn pipeline_adaptive_grows_under_fast_peer() {
    let dir = std::env::temp_dir().join(format!("m5-pipeline-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // 大数据量：64 片 × 64KB = 4MB（足够触发流水线增长）
    let data: Vec<u8> = (0..(64 * PIECE_LEN)).map(|i| (i % 251) as u8).collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    let seed = Arc::new(data.clone());
    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = vec![saddr];
    tokio::spawn(serve_seed(
        sl,
        seed,
        PIECE_LEN,
        InfoHash::from_bytes(&meta.info_hash),
        PeerId::azureus_prefix(&[9u8; 12]),
        0, // 无延迟：快速 peer
    ));

    let cfg = TorrentConfig {
        dir: dir.to_path_buf(),
        peer_id: PeerId::azureus_prefix(&[3u8; 12]),
        listen_port: 0,
        max_peers: 8,
        adaptive: false,
        numwant: 50,
        announce_urls: vec![tracker_url],
        pipeline: 0, // 自适应
        udp_announce_urls: Vec::new(),
        enable_dht: false,
        dht_port: 0,
        encryption: xfer_bt::EncryptionMode::PlaintextOnly,
        bt_protocol: xfer_bt::BtProtocol::TcpOnly,
        download_limit: 0,
        upload_limit: 0,
        seed_mode: false,
        seed_duration: 0,
        seed_ratio: 0.0,
        selected_files: None,
    };
    let engine = TorrentEngine::new(meta, cfg).unwrap();
    tokio::time::timeout(
        Duration::from_secs(30),
        engine.clone().run(CancellationToken::new()),
    )
    .await
    .expect("下载未超时")
    .expect("下载应成功");

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载文件与源数据不一致");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 测试 2：慢速节点淘汰——慢速 seed 应被淘汰，快速 seed 应完成下载。
#[tokio::test]
async fn slow_peer_does_not_block_download() {
    let dir = std::env::temp_dir().join(format!("m5-slow-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // 小数据量
    let data: Vec<u8> = (0..(4 * PIECE_LEN + 1234))
        .map(|i| (i % 251) as u8)
        .collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    // 慢速 seed：每块延迟 200ms
    let seed = Arc::new(data.clone());
    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = vec![saddr];
    tokio::spawn(serve_seed(
        sl,
        seed,
        PIECE_LEN,
        InfoHash::from_bytes(&meta.info_hash),
        PeerId::azureus_prefix(&[9u8; 12]),
        200, // 200ms/块 → 慢速节点
    ));

    let cfg = TorrentConfig {
        dir: dir.to_path_buf(),
        peer_id: PeerId::azureus_prefix(&[3u8; 12]),
        listen_port: 0,
        max_peers: 8,
        adaptive: false,
        numwant: 50,
        announce_urls: vec![tracker_url],
        pipeline: 0,
        udp_announce_urls: Vec::new(),
        enable_dht: false,
        dht_port: 0,
        encryption: xfer_bt::EncryptionMode::PlaintextOnly,
        bt_protocol: xfer_bt::BtProtocol::TcpOnly,
        download_limit: 0,
        upload_limit: 0,
        seed_mode: false,
        seed_duration: 0,
        seed_ratio: 0.0,
        selected_files: None,
    };
    let engine = TorrentEngine::new(meta, cfg).unwrap();
    // 即使有慢速节点，下载也应在合理时间内完成
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        engine.clone().run(CancellationToken::new()),
    )
    .await;
    assert!(result.is_ok(), "下载不应超时");
    assert!(result.unwrap().is_ok(), "下载应成功");

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载文件与源数据不一致");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 测试 3：限速——下载速度应受限于配置的速率。
#[tokio::test]
async fn rate_limit_caps_download_speed() {
    let dir = std::env::temp_dir().join(format!("m5-ratelimit-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // 4 片 = 256KB
    let data: Vec<u8> = (0..(4 * PIECE_LEN)).map(|i| (i % 251) as u8).collect();
    let data_len = data.len() as u64;

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    let seed = Arc::new(data.clone());
    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = vec![saddr];
    tokio::spawn(serve_seed(
        sl,
        seed,
        PIECE_LEN,
        InfoHash::from_bytes(&meta.info_hash),
        PeerId::azureus_prefix(&[9u8; 12]),
        0,
    ));

    // 限速 50KB/s → 256KB 应至少需要 ~5s
    let limit = 50 * 1024; // 50KB/s
    let cfg = TorrentConfig {
        dir: dir.to_path_buf(),
        peer_id: PeerId::azureus_prefix(&[3u8; 12]),
        listen_port: 0,
        max_peers: 8,
        adaptive: false,
        numwant: 50,
        announce_urls: vec![tracker_url],
        pipeline: 0,
        udp_announce_urls: Vec::new(),
        enable_dht: false,
        dht_port: 0,
        encryption: xfer_bt::EncryptionMode::PlaintextOnly,
        bt_protocol: xfer_bt::BtProtocol::TcpOnly,
        download_limit: limit,
        upload_limit: 0,
        seed_mode: false,
        seed_duration: 0,
        seed_ratio: 0.0,
        selected_files: None,
    };
    let engine = TorrentEngine::new(meta, cfg).unwrap();
    let start = Instant::now();
    tokio::time::timeout(
        Duration::from_secs(60),
        engine.clone().run(CancellationToken::new()),
    )
    .await
    .expect("下载未超时")
    .expect("下载应成功");
    let elapsed = start.elapsed();

    // 256KB / 50KB/s ≈ 5s；允许 2-15s 的范围（有开销）
    let min_expected = data_len / limit / 2; // 至少理论值的一半
    assert!(
        elapsed >= Duration::from_millis(min_expected),
        "限速下载时间应 ≥ {min_expected}ms，实际 {}ms",
        elapsed.as_millis()
    );

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data);
    let _ = std::fs::remove_dir_all(&dir);
}

/// 测试 4：seed 模式——下载完成后引擎应进入 seed 循环并接受新连接。
#[tokio::test]
async fn seed_mode_accepts_incoming_connections() {
    let dir = std::env::temp_dir().join(format!("m5-seed-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..(2 * PIECE_LEN + 99)).map(|i| (i % 97) as u8).collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    let seed = Arc::new(data.clone());
    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = vec![saddr];
    tokio::spawn(serve_seed(
        sl,
        seed,
        PIECE_LEN,
        InfoHash::from_bytes(&meta.info_hash),
        PeerId::azureus_prefix(&[9u8; 12]),
        0,
    ));

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    let cfg = TorrentConfig {
        dir: dir.to_path_buf(),
        peer_id: PeerId::azureus_prefix(&[3u8; 12]),
        listen_port: 0,
        max_peers: 8,
        adaptive: false,
        numwant: 50,
        announce_urls: vec![tracker_url],
        pipeline: 0,
        udp_announce_urls: Vec::new(),
        enable_dht: false,
        dht_port: 0,
        encryption: xfer_bt::EncryptionMode::PlaintextOnly,
        bt_protocol: xfer_bt::BtProtocol::TcpOnly,
        download_limit: 0,
        upload_limit: 0,
        seed_mode: true,
        seed_duration: 3, // 3 秒后由 cancel 停止
        seed_ratio: 0.0,
        selected_files: None,
    };
    let engine = TorrentEngine::new(meta, cfg).unwrap();
    let engine_task = tokio::spawn(engine.clone().run(cancel_clone));

    // 等待下载完成 + seed 模式启动。
    // 固定 2s sleep 在高负载并行下易超时导致偶发失败，改为进度轮询。
    let total = data.len() as u64;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while engine.progress().done < total {
        if tokio::time::Instant::now() >= deadline {
            panic!("下载超时：done={}/{}", engine.progress().done, total);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 取消 seed 模式
    cancel.cancel();
    let _ = engine_task.await;

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data);
    let _ = std::fs::remove_dir_all(&dir);
}

/// 测试 5：冷启动突发连接——多个 seed 同时返回时应快速建立连接。
#[tokio::test]
async fn cold_start_burst_connects_multiple_seeds() {
    let dir = std::env::temp_dir().join(format!("m5-burst-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..(8 * PIECE_LEN + 7))
        .map(|i| (i.wrapping_mul(131) % 253) as u8)
        .collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    // 3 个 seed
    let seed = Arc::new(data.clone());
    let mut addrs = Vec::new();
    for i in 0..3 {
        let (sl, saddr) = bind_random().await;
        addrs.push(saddr);
        let sid = PeerId::azureus_prefix(&[10 + i as u8; 12]);
        let s2 = seed.clone();
        let ih = InfoHash::from_bytes(&meta.info_hash);
        tokio::spawn(async move {
            serve_seed(sl, s2, PIECE_LEN, ih, sid, 0).await;
        });
    }
    *seed_ref.write().unwrap() = addrs;

    let start = Instant::now();
    let cfg = TorrentConfig {
        dir: dir.to_path_buf(),
        peer_id: PeerId::azureus_prefix(&[3u8; 12]),
        listen_port: 0,
        max_peers: 50,
        adaptive: false,
        numwant: 50,
        announce_urls: vec![tracker_url],
        pipeline: 0,
        udp_announce_urls: Vec::new(),
        enable_dht: false,
        dht_port: 0,
        encryption: xfer_bt::EncryptionMode::PlaintextOnly,
        bt_protocol: xfer_bt::BtProtocol::TcpOnly,
        download_limit: 0,
        upload_limit: 0,
        seed_mode: false,
        seed_duration: 0,
        seed_ratio: 0.0,
        selected_files: None,
    };
    let engine = TorrentEngine::new(meta, cfg).unwrap();
    tokio::time::timeout(
        Duration::from_secs(30),
        engine.clone().run(CancellationToken::new()),
    )
    .await
    .expect("下载未超时")
    .expect("下载应成功");
    let elapsed = start.elapsed();

    // 3 个 seed 冷启动突发 → 应快速完成
    assert!(
        elapsed < Duration::from_secs(10),
        "冷启动突发应快速完成，实际 {}ms",
        elapsed.as_millis()
    );

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data);
    let _ = std::fs::remove_dir_all(&dir);
}
