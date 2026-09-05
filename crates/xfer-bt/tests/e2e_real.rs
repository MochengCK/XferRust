//! 端到端测试（真实网络场景诊断）：
//! - tracker 返回不可达 peer + 可达 peer 混合
//! - peer 连接超时后继续可用 peer
//! - 验证 is_unroutable 不会错误过滤回环地址
//! - 验证 announce 的 compact peer 解析正确

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

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

/// seed peer：被动握手、发 Bitfield+Unchoke、响应 request 发 piece 块。
async fn serve_seed(
    listener: TcpListener,
    data: Arc<Vec<u8>>,
    piece_len: usize,
    info_hash: InfoHash,
    peer_id: PeerId,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let data = data.clone();
        tokio::spawn(async move {
            let _ = handle_seed_peer(stream, &data, piece_len, info_hash, peer_id).await;
        });
    }
}

async fn handle_seed_peer(
    mut stream: TcpStream,
    data: &[u8],
    piece_len: usize,
    info_hash: InfoHash,
    peer_id: PeerId,
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

/// tracker：返回包含不可达 peer + 可达 peer 的混合列表。
async fn tracker_announce(
    Query(_q): Query<HashMap<String, String>>,
    State(state): State<Arc<RwLock<Vec<SocketAddr>>>>,
) -> Response {
    let addrs = state.read().unwrap().clone();
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

async fn run_download(meta: TorrentMeta, dir: &std::path::Path) -> Result<(), String> {
    let cfg = TorrentConfig {
        dir: dir.to_path_buf(),
        peer_id: PeerId::azureus_prefix(&[3u8; 12]),
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
    let engine = TorrentEngine::new(meta, cfg).map_err(|e| e.to_string())?;
    tokio::time::timeout(
        Duration::from_secs(30),
        engine.clone().run(CancellationToken::new()),
    )
    .await
    .map_err(|_| "下载超时".to_string())??;
    Ok(())
}

/// 测试 1：tracker 返回不可达 peer + 可达 peer 混合。
/// 不可达 peer 应被跳过（连接超时/失败），下载通过可达 peer 完成。
#[tokio::test]
async fn download_with_unreachable_and_reachable_peers() {
    let dir = std::env::temp_dir().join(format!("e2e-mixed-peers-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..(4 * PIECE_LEN + 1234))
        .map(|i| (i % 251) as u8)
        .collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    // 可达 seed
    let seed = Arc::new(data.clone());
    let (sl, saddr) = bind_random().await;

    // 不可达 peer：用随机端口（几乎肯定没人在监听）
    let unreachable1: SocketAddr = "127.0.0.1:39999".parse().unwrap();
    let unreachable2: SocketAddr = "127.0.0.1:48888".parse().unwrap();

    // tracker 返回混合列表：不可达 + 可达
    *seed_ref.write().unwrap() = vec![unreachable1, saddr, unreachable2];

    tokio::spawn(serve_seed(
        sl,
        seed,
        PIECE_LEN,
        InfoHash::from_bytes(&meta.info_hash),
        PeerId::azureus_prefix(&[9u8; 12]),
    ));

    run_download(meta, &dir).await.expect("下载应成功");

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载文件与源数据不一致");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 测试 2：tracker 返回非 compact 格式 peers（dictionary 列表）。
/// 验证非 compact peer 解析也能正确下载。
#[tokio::test]
async fn download_with_non_compact_peers() {
    let dir = std::env::temp_dir().join(format!("e2e-noncompact-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..(4 * PIECE_LEN + 99)).map(|i| (i % 97) as u8).collect();

    // 自定义 tracker 返回非 compact 格式
    let (sl_tracker, taddr) = bind_random().await;
    let (sl_seed, saddr) = bind_random().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    // 启动自定义 tracker
    let saddr_for_tracker = saddr;
    tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut buf = [0u8; 4096];
        while let Ok((mut stream, _)) = sl_tracker.accept().await {
            let saddr = saddr_for_tracker;
            let _ = stream.read(&mut buf).await;
            // 构造非 compact peers 响应
            let peer_dict = dict(BTreeMap::from([
                (b"ip".to_vec(), bytes("127.0.0.1")),
                (b"port".to_vec(), int(saddr.port() as i64)),
            ]));
            let resp = dict(BTreeMap::from([
                (b"interval".to_vec(), int(60)),
                (b"complete".to_vec(), int(1)),
                (
                    b"peers".to_vec(),
                    xfer_bencode::Value::List(vec![peer_dict]),
                ),
            ]));
            let body = encode(&resp);
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(header.as_bytes()).await;
            let _ = stream.write_all(&body).await;
        }
    });

    // 启动 seed
    let seed = Arc::new(data.clone());
    tokio::spawn(serve_seed(
        sl_seed,
        seed,
        PIECE_LEN,
        InfoHash::from_bytes(&meta.info_hash),
        PeerId::azureus_prefix(&[9u8; 12]),
    ));

    run_download(meta, &dir).await.expect("下载应成功");

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载文件与源数据不一致");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 测试 3：验证 is_unroutable 不会过滤回环地址。
/// 回环地址（127.0.0.1）的 peer 必须能被连接和下载。
#[tokio::test]
async fn loopback_peer_is_not_filtered() {
    let dir = std::env::temp_dir().join(format!("e2e-loopback-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..(2 * PIECE_LEN + 11)).map(|i| (i % 97) as u8).collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    let seed = Arc::new(data.clone());
    let (sl, saddr) = bind_random().await;
    // tracker 只返回一个 127.0.0.1 地址
    *seed_ref.write().unwrap() = vec![saddr];

    tokio::spawn(serve_seed(
        sl,
        seed,
        PIECE_LEN,
        InfoHash::from_bytes(&meta.info_hash),
        PeerId::azureus_prefix(&[9u8; 12]),
    ));

    run_download(meta, &dir).await.expect("下载应成功");

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载文件与源数据不一致");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 测试 4：peer 中途断开后下载继续（验证断线重连/降级处理）。
/// seed 在传输一半时断开，另一个 seed 接手完成下载。
#[tokio::test]
async fn download_continues_after_peer_disconnect() {
    let dir = std::env::temp_dir().join(format!("e2e-disconnect-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // 8 片 = 512KB
    let data: Vec<u8> = (0..(8 * PIECE_LEN)).map(|i| (i % 251) as u8).collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    // seed 1: 在发送 2 片后断开
    let data1 = Arc::new(data.clone());
    let ih1 = InfoHash::from_bytes(&meta.info_hash);
    let pid1 = PeerId::azureus_prefix(&[10u8; 12]);
    let (sl1, saddr1) = bind_random().await;
    tokio::spawn(async move {
        serve_seed_with_disconnect(sl1, data1, PIECE_LEN, ih1, pid1, 2).await;
    });

    // seed 2: 完整 seed
    let seed2 = Arc::new(data.clone());
    let ih2 = InfoHash::from_bytes(&meta.info_hash);
    let pid2 = PeerId::azureus_prefix(&[20u8; 12]);
    let (sl2, saddr2) = bind_random().await;
    tokio::spawn(async move {
        serve_seed(sl2, seed2, PIECE_LEN, ih2, pid2).await;
    });

    *seed_ref.write().unwrap() = vec![saddr1, saddr2];

    run_download(meta, &dir).await.expect("下载应成功");

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载文件与源数据不一致");
    let _ = std::fs::remove_dir_all(&dir);
}

/// seed peer：发送指定片数后主动断开连接。
async fn serve_seed_with_disconnect(
    listener: TcpListener,
    data: Arc<Vec<u8>>,
    piece_len: usize,
    info_hash: InfoHash,
    peer_id: PeerId,
    max_pieces_before_disconnect: u32,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let data = data.clone();
        let ih = info_hash;
        let pid = peer_id;
        tokio::spawn(async move {
            let _ = handle_seed_disconnect(
                stream,
                &data,
                piece_len,
                ih,
                pid,
                max_pieces_before_disconnect,
            )
            .await;
        });
    }
}

async fn handle_seed_disconnect(
    mut stream: TcpStream,
    data: &[u8],
    piece_len: usize,
    info_hash: InfoHash,
    peer_id: PeerId,
    max_pieces: u32,
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

    let mut pieces_sent = 0u32;
    loop {
        match reader.read_message(&mut stream).await? {
            None => break,
            Some(Message::Request {
                index,
                begin,
                length,
            }) => {
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
                // 简单计算：每片可能有多个 block，我们按 piece index 计数
                if begin == 0 {
                    pieces_sent += 1;
                }
                if pieces_sent >= max_pieces {
                    // 主动断开
                    tracing::debug!(pieces_sent, "seed 1 主动断开");
                    return Ok(());
                }
            }
            Some(Message::Interested) | Some(Message::KeepAlive) | Some(Message::Cancel { .. }) => {
            }
            Some(_) => {}
        }
    }
    Ok(())
}
